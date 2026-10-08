# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the PostgreSQL CDC slot/publication source-write path (cdc_pg_slot.py).

The ONE place the tool writes to a source. These tests use a statement-dispatch fake
connection (no real DB): they assert the fixed allowlist is enforced, the publication is
FOR TABLE (never FOR ALL TABLES), the slot returns its consistent LSN, create/drop are
idempotent, provision drops a stale slot before creating a fresh one, and the write engine
is PostgreSQL-only. No AWS, no live PostgreSQL.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.cdc_pg_slot import (
    PgReplicationError,
    _assert_allowed,
    build_pg_source_write_engine,
    create_publication,
    create_replication_slot,
    deprovision_pg_replication,
    drop_publication,
    drop_replication_slot,
    pg_publication_name,
    pg_slot_name,
    provision_pg_replication,
    rekeyed_tables_needing_full_identity,
    set_replica_identity_full,
)
from dsql_migrator.core.models import SourceConnectionConfig, SourceType


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    """Records executed statements; answers existence/reconcile reads + the slot LSN.

    ``pubs`` maps an existing publication name -> its member tables (for the reconcile
    read); ``replident`` maps a qualified table -> its pg_class.relreplident code (default
    'd', usable). ``slots`` are pre-existing slot names. G17: ``all_tables`` names FOR ALL
    TABLES publications; an ``ADD TABLE`` joins the table to ``pubs`` (or raises
    ``add_errors[table]``); ``xid`` answers ``pg_current_xact_id()`` and ``xmin_ready`` the
    polls of :func:`wait_for_transactions_before`, one per poll (the last one repeats).
    The one-table ADD's identity read (:func:`read_replica_identity_usability`) answers
    ``usability[table]`` -- ``(relkind, relreplident, usable, unusable partitions, sample)``
    -- else a plain table usable unless its ``replident`` is 'n'; ``usability_error`` makes
    that read raise, and ``usability_absent`` leaves the named tables out of its answer (not
    found on the source as a table, G20).
    """

    def __init__(
        self, *, pubs=None, slots=(), lsn="3/AF012B8", replident=None,
        fail_slot_create=False, all_tables=(), add_errors=None, xid="7001",
        xmin_ready=(True,), poll_error=None, usability=None, usability_error=None,
        usability_absent=(),
    ):
        self.statements: list[tuple[str, dict]] = []
        self._pubs = {k: list(v) for k, v in dict(pubs or {}).items()}
        self._slots = set(slots)
        self._lsn = lsn
        self._replident = dict(replident or {})
        self._fail_slot_create = fail_slot_create
        self._all_tables = set(all_tables)
        self._add_errors = dict(add_errors or {})
        self._xid = xid
        self._xmin_ready = list(xmin_ready)
        self._poll_error = poll_error
        self._usability = dict(usability or {})
        self._usability_error = usability_error
        self._usability_absent = set(usability_absent)
        self.polls = 0

    def execute(self, statement, params=None):
        import re

        sql = str(statement)
        params = params or {}
        self.statements.append((sql, params))
        up = " ".join(sql.upper().split())
        if "AS IDENTITY_USABLE" in up:  # the one-table ADD's fail-closed identity read
            if self._usability_error is not None:
                raise self._usability_error
            rows = []
            for name in params.get("names") or []:
                if name in self._usability_absent:
                    continue
                if name in self._usability:
                    rows.append((name, *self._usability[name]))
                else:
                    code = self._replident.get(name, "d")
                    rows.append((name, "r", code, code != "n", 0, []))
            return _Result(rows)
        if "PG_SNAPSHOT_XMIN" in up:
            self.polls += 1
            if self._poll_error is not None:
                raise self._poll_error
            ready = self._xmin_ready[min(self.polls - 1, len(self._xmin_ready) - 1)]
            return _Result([(ready,)])
        if up.startswith("SELECT PG_CURRENT_XACT_ID"):
            return _Result([(self._xid,)])
        if up.startswith("ALTER PUBLICATION") and " ADD TABLE " in up:
            name = re.search(r'ALTER PUBLICATION "([^"]+)"', sql).group(1)
            schema, table = re.search(r'ADD TABLE "([^"]+)"\."([^"]+)"', sql).groups()
            qualified = f"{schema}.{table}"
            if qualified in self._add_errors:
                raise self._add_errors[qualified]
            self._pubs.setdefault(name, []).append(qualified)
            return _Result([])
        # PG_PUBLICATION_TABLES must be checked before PG_PUBLICATION (substring).
        if "FROM PG_PUBLICATION_TABLES" in up:
            tables = self._pubs.get(params.get("name"), [])
            return _Result([(t,) for t in tables])
        if "PUBALLTABLES" in up:
            name = params.get("name")
            return _Result([(name in self._all_tables, True)] if name in self._pubs else [])
        if "FROM PG_PUBLICATION" in up:
            return _Result([(1,)] if params.get("name") in self._pubs else [])
        if "FROM PG_CLASS" in up:  # _verify_tables_replicable relreplident read
            names = params.get("names") or []
            return _Result([(n, self._replident.get(n, "d")) for n in names])
        if "FROM PG_REPLICATION_SLOTS" in up:
            return _Result([(1,)] if params.get("name") in self._slots else [])
        if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in up:
            if self._fail_slot_create:
                raise RuntimeError("all replication slots are in use")
            self._slots.add(params.get("name"))
            return _Result([(self._lsn,)])
        if up.startswith("SELECT PG_DROP_REPLICATION_SLOT"):
            self._slots.discard(params.get("name"))
            return _Result([])
        return _Result([])  # CREATE/DROP PUBLICATION

    def writes(self) -> list[str]:
        """The mutating statements issued (excludes the existence-check reads)."""
        out = []
        for sql, _ in self.statements:
            up = " ".join(sql.upper().split())
            if up.startswith(("CREATE PUBLICATION", "DROP PUBLICATION", "ALTER PUBLICATION",
                              "SELECT PG_CURRENT_XACT_ID")) or (
                up.startswith("SELECT")
                and ("PG_CREATE_LOGICAL_REPLICATION_SLOT" in up
                     or up.startswith("SELECT PG_DROP_REPLICATION_SLOT"))
            ):
                out.append(sql)
        return out


# ---------------------------------------------------------------------------
# Deterministic naming
# ---------------------------------------------------------------------------


def test_slot_and_publication_names_are_deterministic_sanitized_and_collision_resistant() -> None:
    import re

    stack = "mysql-dsql-cdc-Prod.1"
    slot = pg_slot_name(stack)
    pub = pg_publication_name(stack)
    # Slot charset [a-z0-9_]; sanitized base + prefix + a hash discriminator.
    assert re.fullmatch(r"[a-z0-9_]+", slot)
    assert slot.startswith("dsqlmig_mysql_dsql_cdc_prod_1_")
    assert pub.startswith("dsqlmig_pub_mysql_dsql_cdc_prod_1_")
    # Deterministic.
    assert pg_slot_name(stack) == slot
    # 63-char slot-name limit is respected even for a very long stack name.
    assert len(pg_slot_name("x" * 200)) <= 63
    # Collision resistance: case-only differences and long suffixes that would sanitize/
    # truncate to the same base still yield DISTINCT slot names (the hash of the original
    # name differs), so two migrations of one source never fight over one slot.
    assert pg_slot_name("mysql-dsql-cdc-Orders") != pg_slot_name("mysql-dsql-cdc-orders")
    long_a = "mysql-dsql-cdc-" + "a" * 60 + "-one"
    long_b = "mysql-dsql-cdc-" + "a" * 60 + "-two"
    assert pg_slot_name(long_a) != pg_slot_name(long_b)


# ---------------------------------------------------------------------------
# Allowlist
# ---------------------------------------------------------------------------


def test_allowlist_rejects_arbitrary_sql() -> None:
    _assert_allowed("CREATE PUBLICATION \"p\" FOR TABLE a.b")  # ok
    _assert_allowed("SELECT lsn FROM pg_create_logical_replication_slot(:n,'pgoutput')")
    with pytest.raises(PgReplicationError):
        _assert_allowed("DROP TABLE customers")
    with pytest.raises(PgReplicationError):
        _assert_allowed("UPDATE orders SET x = 1")
    with pytest.raises(PgReplicationError):
        _assert_allowed("SELECT * FROM orders")  # a plain read is not a sanctioned write


def test_allowlist_permits_replica_identity_full_only_for_alter_table() -> None:
    # The re-keyed-table before-image widening is the ONLY ALTER TABLE allowed...
    _assert_allowed('ALTER TABLE "app"."orders" REPLICA IDENTITY FULL')
    # ...any other ALTER TABLE (drop/rename/type change) is refused so the broad prefix
    # never becomes an arbitrary-DDL surface.
    with pytest.raises(PgReplicationError):
        _assert_allowed('ALTER TABLE "app"."orders" DROP COLUMN secret')
    with pytest.raises(PgReplicationError):
        _assert_allowed('ALTER TABLE "app"."orders" REPLICA IDENTITY NOTHING')


def test_allowlist_permits_only_two_exact_alter_publication_shapes() -> None:
    """The un-guarded write surface gains exactly two ALTER PUBLICATION shapes.

    The partition-root repair has to exist: a publication DEBEZIUM created
    (publication.autocreate.mode=filtered, which is derived from snapshot.mode=initial) lands
    at PostgreSQL's default publish_via_partition_root=false, and the connector offers no
    property to change it -- under which pgoutput names changes after the LEAF partition
    while table.include.list names the collapsed PARENT, so the connector silently discards
    them. G17 deliberately adds ONE more: ``ADD TABLE`` of exactly one double-quoted
    ``schema.table`` (a table selected after a Full load + CDC Full Load joins the run's
    publication before its copy).

    Constrained the same way ALTER TABLE is: both shapes are pinned, so the prefix can never
    unpublish a table, add an unquoted / several tables, hand the publication to another
    owner, or rename it.
    """
    _assert_allowed('ALTER PUBLICATION "p" SET (publish_via_partition_root = true)')
    # Case-insensitive and run-of-whitespace tolerant (the check normalizes both), which is
    # all the latitude the module needs since it composes the statement itself...
    _assert_allowed('alter publication "p"   SET (publish_via_partition_root = TRUE)')
    _assert_allowed('ALTER PUBLICATION "dsqlmig_pub_x" ADD TABLE "app"."orders"')
    # A quoted name may carry a comma or a doubled quote: it is still ONE identifier.
    _assert_allowed('ALTER PUBLICATION "p" ADD TABLE "app"."a,b"')
    _assert_allowed('ALTER PUBLICATION "p" ADD TABLE "app"."a""b"')
    # ...and both shapes are otherwise EXACT, deliberately: a looser match is what would let
    # this prefix drift into an arbitrary-DDL surface. Anything else is refused.
    for refused in (
        'ALTER PUBLICATION "p" SET TABLE app.orders',
        'ALTER PUBLICATION "p" DROP TABLE app.orders',
        'ALTER PUBLICATION "p" DROP TABLE "app"."orders"',
        'ALTER PUBLICATION "p" ADD TABLE app.orders',
        'ALTER PUBLICATION "p" ADD TABLE ONLY "app"."orders"',
        'ALTER PUBLICATION "p" ADD TABLE "app"."orders" *',
        'ALTER PUBLICATION "p" ADD TABLE "app"."orders", "app"."items"',
        'ALTER PUBLICATION "p" ADD TABLE "app"."orders" (id)',
        'ALTER PUBLICATION "p" ADD TABLE "app"."orders" WHERE (id > 1)',
        'ALTER PUBLICATION "p" ADD TABLES IN SCHEMA "app"',
        'ALTER PUBLICATION "p" ADD TABLE "app"."orders"; DROP TABLE "app"."x"',
        'ALTER PUBLICATION "p-1" ADD TABLE "app"."orders"',
        'ALTER PUBLICATION "p" OWNER TO bob',
        'ALTER PUBLICATION "p" RENAME TO q',
        'ALTER PUBLICATION "p" SET (publish = \'insert\')',
        'ALTER PUBLICATION "p" SET (publish_via_partition_root = false)',
    ):
        with pytest.raises(PgReplicationError):
            _assert_allowed(refused)


def test_allowlist_permits_only_the_exact_transaction_id_read() -> None:
    # G17: taking a transaction ID assigns one (a write in PostgreSQL's terms), so it rides
    # the allowlisted path -- in exactly one form.
    _assert_allowed("SELECT pg_current_xact_id()::text")
    for refused in (
        "SELECT pg_current_xact_id()",
        "SELECT pg_current_xact_id()::text, 1",
        "SELECT pg_current_xact_id()::text; DROP TABLE x",
        "SELECT pg_current_xact_id_if_assigned()::text",
    ):
        with pytest.raises(PgReplicationError):
            _assert_allowed(refused)


# ---------------------------------------------------------------------------
# FIX 1: re-keyed (composite-PK) tables need REPLICA IDENTITY FULL
# ---------------------------------------------------------------------------


def test_rekeyed_tables_needing_full_identity_flags_added_leading_column_only() -> None:
    # orders: re-key (tenant_id, id) adds tenant_id OUTSIDE the source PK [id] -> needs FULL.
    # events: re-key just REORDERS the composite source PK [a, b] -> DEFAULT identity covers it.
    # items: not re-keyed at all (absent from the key map) -> not returned.
    message_key_columns = {
        "app.orders": ["tenant_id", "id"],
        "app.events": ["b", "a"],
    }
    table_pks = {
        "app.orders": ["id"],
        "app.events": ["a", "b"],
        "app.items": ["id"],
    }
    assert rekeyed_tables_needing_full_identity(message_key_columns, table_pks) == [
        "app.orders"
    ]
    # A normal (non-re-keyed) selection needs nothing.
    assert rekeyed_tables_needing_full_identity({}, table_pks) == []


def test_set_replica_identity_full_issues_alter_per_table() -> None:
    conn = _FakeConn()
    logs: list[str] = []
    set_replica_identity_full(conn, ["app.orders", "app.customers"], on_log=logs.append)
    alters = [
        sql for sql, _ in conn.statements if sql.upper().startswith("ALTER TABLE")
    ]
    assert len(alters) == 2
    assert all(a.upper().endswith("REPLICA IDENTITY FULL") for a in alters)
    assert '"app"."orders"' in alters[0]
    assert any("REPLICA IDENTITY FULL" in m for m in logs)
    # Empty input is a no-op (no writes).
    conn2 = _FakeConn()
    set_replica_identity_full(conn2, [])
    assert conn2.statements == []


def test_provision_sets_replica_identity_full_before_slot_for_rekeyed_tables() -> None:
    conn = _FakeConn(lsn="7/CAFE")
    provision_pg_replication(
        conn,
        slot_name="dsqlmig_s",
        publication_name="dsqlmig_pub_s",
        tables=["app.orders", "app.items"],
        full_identity_tables=["app.orders"],
    )
    ops = [" ".join(sql.upper().split()) for sql, _ in conn.statements]
    alter_idx = next(
        i for i, s in enumerate(ops)
        if s.startswith("ALTER TABLE") and s.endswith("REPLICA IDENTITY FULL")
    )
    slot_idx = next(
        i for i, s in enumerate(ops) if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in s
    )
    # FULL identity is set BEFORE the slot's LSN so the streamed before-image is wide.
    assert alter_idx < slot_idx
    # Only the re-keyed table is altered (app.items is not).
    alters = [s for s in ops if s.startswith("ALTER TABLE")]
    assert len(alters) == 1
    assert '"APP"."ORDERS"' in alters[0]


def test_provision_without_rekeyed_tables_issues_no_alter() -> None:
    # A normal (non-re-keyed) migration never touches REPLICA IDENTITY on the source.
    conn = _FakeConn()
    provision_pg_replication(
        conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s",
        tables=["app.orders"],
    )
    assert not any(sql.upper().startswith("ALTER TABLE") for sql, _ in conn.statements)


# ---------------------------------------------------------------------------
# create_publication -- FOR TABLE, idempotent, never FOR ALL TABLES
# ---------------------------------------------------------------------------


def test_create_publication_is_for_exact_tables_not_all_tables() -> None:
    conn = _FakeConn()
    logs: list[str] = []
    create_publication(
        conn, name="dsqlmig_pub_s", tables=["app.orders", "app.customers"],
        on_log=logs.append,
    )
    writes = conn.writes()
    assert len(writes) == 1
    ddl = writes[0]
    assert ddl.startswith('CREATE PUBLICATION "dsqlmig_pub_s" FOR TABLE ')
    assert "FOR ALL TABLES" not in ddl.upper()
    # Tables are double-quoted schema.table.
    assert '"app"."orders"' in ddl and '"app"."customers"' in ddl
    assert any("creating publication" in m for m in logs)


def test_create_publication_reuses_when_it_exists_with_the_same_tables() -> None:
    conn = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders"]})
    logs: list[str] = []
    created = create_publication(
        conn, name="dsqlmig_pub_s", tables=["app.orders"], on_log=logs.append
    )
    assert created is False  # reused, not created
    assert conn.writes() == []  # no CREATE issued
    assert any("reusing" in m for m in logs)


def test_create_publication_adds_a_requested_table_the_reused_one_lacks() -> None:
    # A re-run that added a table must NOT silently reuse the stale, narrower publication
    # (pgoutput only streams a publication's members -> the new table would be unreplicated).
    # G17 (deliberately changed from a refusal, which made "Re-run all tables" fail before
    # any table loaded on a grown set): the missing table is ADDED -- one audited ADD TABLE,
    # no CREATE, no DROP -- and the publication counts as reused.
    conn = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders"]})
    logs: list[str] = []
    created = create_publication(
        conn, name="dsqlmig_pub_s", tables=["app.orders", "app.customers"], on_log=logs.append
    )
    assert created is False
    assert conn.writes() == ['ALTER PUBLICATION "dsqlmig_pub_s" ADD TABLE "app"."customers"']
    assert any('added app.customers to publication "dsqlmig_pub_s"' in m for m in logs)
    # ...and provisioning over it adds the table, then creates the fresh slot.
    prov = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders"]}, lsn="8/2")
    handles = provision_pg_replication(
        prov, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s",
        tables=["app.orders", "app.customers"],
    )
    assert handles.consistent_lsn == "8/2"
    writes = [" ".join(w.upper().split()) for w in prov.writes()]
    assert not any(w.startswith(("CREATE PUBLICATION", "DROP PUBLICATION")) for w in writes)
    add = next(i for i, w in enumerate(writes) if w.startswith("ALTER PUBLICATION"))
    slot = next(i for i, w in enumerate(writes) if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in w)
    assert add < slot  # the slot's consistent point decodes the added table's changes


def test_create_publication_reuses_a_publication_that_covers_the_set() -> None:
    # G16 (FAILS WITHOUT THE FIX: the reconcile refused any difference): a Full load + CDC
    # re-run over a set NARROWED after its first load requests fewer tables than the
    # publication that load created. Reused as is -- no write on the source -- because the
    # connector's table.include.list, not the publication, defines what CDC captures.
    conn = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders", "app.customers"]})
    logs: list[str] = []
    created = create_publication(
        conn, name="dsqlmig_pub_s", tables=["app.orders"], on_log=logs.append
    )
    assert created is False and conn.writes() == []
    assert logs == [
        "publication dsqlmig_pub_s already exists and covers every requested table; reusing "
        "it (it also lists app.customers, which this run does not load: CDC's "
        "table.include.list filters them out)"
    ]
    # ...and provisioning over it creates the fresh slot, with no CREATE PUBLICATION.
    prov = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders", "app.customers"]}, lsn="7/1")
    handles = provision_pg_replication(
        prov, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s", tables=["app.orders"],
    )
    assert handles.consistent_lsn == "7/1"
    writes = [w.upper() for w in prov.writes()]
    assert not any(w.startswith("CREATE PUBLICATION") for w in writes)
    assert any("PG_CREATE_LOGICAL_REPLICATION_SLOT" in w for w in writes)
    # A publication MISSING a requested table gets it ADDED (G17, deliberately changed from a
    # refusal): never left unreplicated, never by dropping the publication.
    missing = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders"]})
    assert create_publication(missing, name="dsqlmig_pub_s", tables=["app.customers"]) is False
    assert missing.writes() == [
        'ALTER PUBLICATION "dsqlmig_pub_s" ADD TABLE "app"."customers"']


def test_create_publication_rejects_zero_tables() -> None:
    with pytest.raises(PgReplicationError):
        create_publication(_FakeConn(), name="p", tables=[])


# ---------------------------------------------------------------------------
# slot create / drop
# ---------------------------------------------------------------------------


def test_create_replication_slot_returns_consistent_lsn() -> None:
    conn = _FakeConn(lsn="9/AABBCC")
    lsn = create_replication_slot(conn, name="dsqlmig_s")
    assert lsn == "9/AABBCC"
    assert any("pg_create_logical_replication_slot" in s.lower() for s in conn.writes())


def test_create_replication_slot_refuses_to_reuse_an_existing_slot() -> None:
    # A pre-existing slot has an OLD LSN; reusing it would break the gapless guarantee.
    conn = _FakeConn(slots={"dsqlmig_s"})
    with pytest.raises(PgReplicationError):
        create_replication_slot(conn, name="dsqlmig_s")


def test_drop_replication_slot_is_idempotent() -> None:
    absent = _FakeConn()
    drop_replication_slot(absent, name="dsqlmig_s")
    assert absent.writes() == []  # nothing to drop -> no write

    present = _FakeConn(slots={"dsqlmig_s"})
    drop_replication_slot(present, name="dsqlmig_s")
    assert any(s.upper().startswith("SELECT PG_DROP_REPLICATION_SLOT") for s in present.writes())


def test_drop_publication_uses_if_exists() -> None:
    conn = _FakeConn()
    drop_publication(conn, name="dsqlmig_pub_s")
    writes = conn.writes()
    assert len(writes) == 1
    assert writes[0].upper().startswith("DROP PUBLICATION IF EXISTS")


# ---------------------------------------------------------------------------
# provision / deprovision orchestration + ordering
# ---------------------------------------------------------------------------


def test_provision_creates_publication_then_fresh_slot_and_returns_lsn() -> None:
    conn = _FakeConn(lsn="3/AF012B8")
    handles = provision_pg_replication(
        conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s",
        tables=["app.orders"],
    )
    assert handles.consistent_lsn == "3/AF012B8"
    assert handles.slot_name == "dsqlmig_s"
    assert handles.publication_name == "dsqlmig_pub_s"
    writes = conn.writes()
    # Publication is created before the slot.
    assert writes[0].upper().startswith("CREATE PUBLICATION")
    assert "pg_create_logical_replication_slot" in writes[-1].lower()


def test_provision_drops_a_stale_slot_before_creating_a_fresh_one() -> None:
    # A slot left from a prior run must be dropped so the new slot's LSN matches THIS
    # Full Load's snapshot (fresh consistency point).
    conn = _FakeConn(slots={"dsqlmig_s"}, lsn="5/1234")
    logs: list[str] = []
    handles = provision_pg_replication(
        conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s",
        tables=["app.orders"], on_log=logs.append,
    )
    assert handles.consistent_lsn == "5/1234"
    ops = [s.upper() for s in conn.writes()]
    drop_idx = next(i for i, s in enumerate(ops) if s.startswith("SELECT PG_DROP_REPLICATION_SLOT"))
    create_idx = next(i for i, s in enumerate(ops) if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in s)
    assert drop_idx < create_idx  # stale slot dropped before the fresh create
    assert any("stale" in m for m in logs)


def test_provision_refuses_a_table_with_no_replica_identity() -> None:
    # Source-safety guard: publishing a REPLICA IDENTITY NOTHING table would break its
    # UPDATE/DELETE on the source, so provision must refuse before creating anything.
    conn = _FakeConn(replident={"app.orders": "n"})
    with pytest.raises(PgReplicationError) as ei:
        provision_pg_replication(
            conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s",
            tables=["app.orders"],
        )
    assert "REPLICA IDENTITY" in str(ei.value)
    assert conn.writes() == []  # nothing created


def test_provision_drops_the_publication_it_created_if_slot_creation_fails() -> None:
    # AUTOCOMMIT commits CREATE PUBLICATION immediately; if slot creation then fails,
    # provision must drop the publication it just created (no orphan arming a source
    # write outage).
    conn = _FakeConn(fail_slot_create=True)
    with pytest.raises(RuntimeError):
        provision_pg_replication(
            conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s",
            tables=["app.orders"],
        )
    ops = [s.upper() for s in conn.writes()]
    assert any(s.startswith("CREATE PUBLICATION") for s in ops)
    # ...and it was compensated with a DROP PUBLICATION.
    assert any(s.startswith("DROP PUBLICATION") for s in ops)


def test_deprovision_drops_slot_then_publication() -> None:
    conn = _FakeConn(slots={"dsqlmig_s"})
    deprovision_pg_replication(conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s")
    ops = [s.upper() for s in conn.writes()]
    slot_drop = next(i for i, s in enumerate(ops) if s.startswith("SELECT PG_DROP_REPLICATION_SLOT"))
    pub_drop = next(i for i, s in enumerate(ops) if s.startswith("DROP PUBLICATION"))
    assert slot_drop < pub_drop  # slot first (frees pinned WAL), then publication


# ---------------------------------------------------------------------------
# write engine is PostgreSQL-only
# ---------------------------------------------------------------------------


def test_write_engine_refuses_a_mysql_source() -> None:
    mysql = SourceConnectionConfig(
        source_type=SourceType.MYSQL, host="db", database="app", username="u"
    )
    with pytest.raises(PgReplicationError):
        build_pg_source_write_engine(mysql, None)


def test_write_engine_for_postgres_is_autocommit_and_unguarded() -> None:
    pg = SourceConnectionConfig(
        source_type=SourceType.POSTGRES, host="pg", port=5432, database="app",
        username="u",
    )
    engine = build_pg_source_write_engine(pg, None)
    try:
        assert engine.url.drivername.startswith("postgresql")
        # AUTOCOMMIT set via execution_options (pg_create_logical_replication_slot cannot
        # run in a wrapping transaction).
        assert engine.get_execution_options().get("isolation_level") == "AUTOCOMMIT"
    finally:
        engine.dispose()


def test_provision_does_not_drop_a_reused_publication_when_slot_creation_fails() -> None:
    # Tier-3 #24: when provision REUSES an existing publication (created=False) and the slot
    # create then fails, the compensating cleanup must NOT drop that publication -- we did not
    # create it, and dropping an operator's/other pipeline's publication is a destructive,
    # unaudited source write. Assert: no CREATE PUBLICATION and no DROP PUBLICATION issued.
    conn = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders"]}, fail_slot_create=True)
    with pytest.raises(RuntimeError):
        provision_pg_replication(
            conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s", tables=["app.orders"]
        )
    ops = [s.upper() for s in conn.writes()]
    assert not any(s.startswith("CREATE PUBLICATION") for s in ops)  # reused, not created
    assert not any(s.startswith("DROP PUBLICATION") for s in ops)   # so never compensated-dropped


def test_slot_health_read_prefers_the_deployed_slot_name() -> None:
    """D-2: the monitor re-derived the slot name from the MUTABLE stack name, while every
    other PostgreSQL path (dispatch_source_config, _drop_pg_source_replication) prefers the
    recorded/deployed name -- their comments say why. Since pg_slot_name embeds a hash of
    the stack name, a drifted name pointed the health read at a slot that never existed, so
    the WAL-pressure panel silently vanished for a live, healthy slot."""
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_status import _refresh_pg_slot_health
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    # The hash makes the derived names genuinely different, which is the whole hazard.
    assert pg_slot_name("cdc-a") != pg_slot_name("cdc-b")

    class _Config:
        source_type = SourceType.POSTGRES

    read: list[str] = []

    class _Dialect:
        def read_replication_slot_health(self, _connection, slot_name):
            read.append(slot_name)
            return "health"

    import dsql_migrator.core.source_dialect as source_dialect

    original = source_dialect.dialect_for
    source_dialect.dialect_for = lambda _st: _Dialect()
    try:
        # Deployed name present -> used verbatim, even though the stack name differs.
        state = DataMigrationState()
        state.cdc_stack_name = "cdc-renamed"
        state.set_cdc_deployed_slot_name("dsql_cdc_slot_deadbeef")
        _refresh_pg_slot_health(state, _Config(), object())
        assert read == ["dsql_cdc_slot_deadbeef"]
        assert state.cdc_slot_health == "health"

        # No deployed name -> fall back to the name derived from this session's stack.
        read.clear()
        state2 = DataMigrationState()
        state2.cdc_stack_name = "cdc-renamed"
        _refresh_pg_slot_health(state2, _Config(), object())
        assert read == [pg_slot_name("cdc-renamed")]

        # An empty name resolves to nothing, so the health is left "not known" rather than
        # probing a guessed name whose absence would look like a lost slot. Defensive
        # only -- cdc_stack_name defaults to CDC_DEFAULT_STACK_NAME in the running app.
        read.clear()
        state3 = DataMigrationState()
        state3.cdc_stack_name = ""
        _refresh_pg_slot_health(state3, _Config(), object())
        assert read == []
        assert state3.cdc_slot_health is None
    finally:
        source_dialect.dialect_for = original


# --- CDC-start pre-flight: what the source actually has ------------------------
# A CDC-only start deploys connectors against objects only a Full-Load-+-CDC run creates.
# The probe reads the state; the blocker turns it into a verdict. Both must distinguish
# "absent" from "unreadable", and must NOT block the legitimate re-snapshot start.


class _ObjectsConn:
    """Answers read_pg_replication_objects' statements from scripted facts.

    ``via_root`` / ``partition_roots`` matter because the roll-up they drive is the thing
    that stops a REPAIRABLE publication from reading as a missing table. A double that
    answered the pubviaroot probe with the generic facts row reported True unconditionally,
    which is exactly how the whole defect stayed invisible to a green unit suite.
    """

    def __init__(
        self,
        *,
        pub_n=1,
        all_dml=True,
        slot_any=1,
        slot_ok=1,
        covered=(),
        via_root=True,
        partition_roots=None,
    ):
        self._row = (pub_n, all_dml, slot_any, slot_ok)
        self._covered = list(covered)
        self._via_root = via_root
        self._partition_roots = dict(partition_roots or {})
        self.statements: list[str] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        upper = sql.upper()
        if "PUBVIAROOT" in upper:
            return _Result([] if self._via_root is None else [(self._via_root,)])
        if "PG_PARTITION_ROOT" in upper:
            return _Result(list(self._partition_roots.items()))
        if "PG_PUBLICATION_TABLES" in upper:
            return _Result([(t,) for t in self._covered])
        return _Result([self._row])


def test_read_pg_replication_objects_reads_the_catalog_shape() -> None:
    from dsql_migrator.core.cdc_pg_slot import read_pg_replication_objects

    conn = _ObjectsConn(covered=["app.orders", "app.items"])
    got = read_pg_replication_objects(conn, publication_name="p1", slot_name="s1")
    assert got.publication_present is True
    assert got.publication_publishes_all_dml is True
    assert got.slot_present_any_database is True and got.slot_usable is True
    assert got.publication_tables == frozenset({"app.orders", "app.items"})
    # The slot query must be scoped to a LOGICAL pgoutput slot in THIS database:
    # pg_replication_slots is cluster-wide, so a bare name match can hit a slot bound
    # elsewhere that the connector cannot use.
    joined = " ".join(s.upper() for s in conn.statements)
    assert "CURRENT_DATABASE()" in joined
    assert "'PGOUTPUT'" in joined.replace('"', "'")
    assert "SLOT_TYPE = 'LOGICAL'" in joined


def test_a_repairable_publication_does_not_read_as_a_missing_partitioned_table() -> None:
    """THE defect, and the half that made a retry a dead end.

    Live on Aurora PostgreSQL 17.7: a `Full load only` run then Start CDC. Debezium creates
    the publication (publication.autocreate.mode=filtered, derived from
    snapshot.mode=initial) and it lands at PostgreSQL's default
    publish_via_partition_root=false, which the connector cannot set. Under that setting
    `pg_publication_tables` EXPANDS the registered partitioned parent into its LEAVES --
    reproduced on PostgreSQL 17.11: `pg_publication_rel` holds `ecommerce.order_events`
    (relkind 'p') while the view reports `order_events_2026q3` / `_2026q4` and no parent row.

    Two consequences, both fixed here. pgoutput names every change after the leaf while
    `table.include.list` names the collapsed parent, so the connector DISCARDS them (7 rows
    lost with the connector RUNNING and the DLQ empty). And the coverage check compared the
    captured parent against the leaf-expanded view, so it called the parent MISSING -- which
    turns the next Start into a hard block on a state the Start itself repairs.
    """
    from dsql_migrator.core.cdc_pg_slot import (
        pg_replication_objects_blocker,
        read_pg_replication_objects,
    )

    broken = _ObjectsConn(
        covered=["ecommerce.order_events_2026q3", "ecommerce.order_events_2026q4",
                 "ecommerce.users"],
        via_root=False,
        partition_roots={
            "ecommerce.order_events_2026q3": "ecommerce.order_events",
            "ecommerce.order_events_2026q4": "ecommerce.order_events",
        },
    )
    got = read_pg_replication_objects(broken, publication_name="p1", slot_name="s1")
    assert got.publication_via_partition_root is False
    # The leaves are rolled up to the table the TARGET actually has, so coverage is judged
    # against what the publication will effectively cover once the Start repairs it.
    assert got.publication_tables == frozenset({"ecommerce.order_events", "ecommerce.users"})
    assert pg_replication_objects_blocker(
        got,
        ["ecommerce.order_events", "ecommerce.users"],
        resumes_from_slot=False,
    ) is None, "a repairable publication must not block the Start"

    # A table that is genuinely absent in ANY form still blocks -- the relaxation is scoped
    # to the partition roll-up, not to coverage in general.
    assert pg_replication_objects_blocker(
        got,
        ["ecommerce.order_events", "ecommerce.nowhere"],
        resumes_from_slot=False,
    ) is not None

    # And with the property already true the view reports the parent, so nothing is rolled
    # up and no partition query is even issued -- the gapless route is untouched.
    healthy = _ObjectsConn(covered=["ecommerce.order_events"], via_root=True)
    ok = read_pg_replication_objects(healthy, publication_name="p1", slot_name="s1")
    assert ok.publication_via_partition_root is True
    assert ok.publication_tables == frozenset({"ecommerce.order_events"})
    assert not [s for s in healthy.statements if "PG_PARTITION_ROOT" in s.upper()]


def test_ensure_publication_via_partition_root_creates_repairs_or_does_nothing() -> None:
    """Three idempotent outcomes, and the repair must be an in-place ALTER.

    A DROP would destroy the slot with it (the publication and slot are created together and
    the slot pins the source WAL), so "repaired" has to be an ALTER: measured on PostgreSQL
    17.11 that flips f->t in place, keeps restart_lsn, and the next commit decodes under the
    PARENT relid on the SAME slot with no connector restart.
    """
    from dsql_migrator.core.cdc_pg_slot import ensure_publication_via_partition_root

    class _Conn:
        def __init__(self, via_root):
            self._via_root = via_root
            self.writes: list[str] = []

        def execute(self, statement, params=None):
            sql = str(statement)
            if "PUBVIAROOT" in sql.upper():
                return _Result([] if self._via_root is None else [(self._via_root,)])
            self.writes.append(sql)
            return _Result([])

    logs: list[str] = []
    already = _Conn(True)
    assert ensure_publication_via_partition_root(
        already, name="p1", on_log=logs.append
    ) == "unchanged"
    assert already.writes == [], "a correct publication must not be written to"
    assert logs == []

    repaired = _Conn(False)
    assert ensure_publication_via_partition_root(
        repaired, name="p1", on_log=logs.append
    ) == "repaired"
    assert len(repaired.writes) == 1
    assert repaired.writes[0] == (
        'ALTER PUBLICATION "p1" SET (publish_via_partition_root = true)'
    )
    assert "DROP" not in repaired.writes[0].upper(), "a DROP would take the slot with it"
    # The operator must learn that the repair does not recover what was already missed.
    assert any("not recovered" in m for m in logs)

    created = _Conn(None)
    assert ensure_publication_via_partition_root(created, name="p1") == "created"
    assert created.writes == [
        'CREATE PUBLICATION "p1" WITH (publish_via_partition_root = true)'
    ]
    # No table list: on this route Debezium owns membership and sets it itself, so
    # reconciling here would turn a changed selection into a hard failure.
    assert "TABLE" not in created.writes[0].upper()


def test_no_publication_means_unknown_dml_not_false() -> None:
    # bool_and over zero rows is NULL. Reporting that as False would claim the
    # publication restricts its change types, which is a different (and wrong) remedy.
    from dsql_migrator.core.cdc_pg_slot import read_pg_replication_objects

    got = read_pg_replication_objects(
        _ObjectsConn(pub_n=0, all_dml=None, slot_any=0, slot_ok=0),
        publication_name="p1",
        slot_name="s1",
    )
    assert got.publication_present is False
    assert got.publication_publishes_all_dml is None  # not False
    assert got.publication_tables == frozenset()


def _objects(**kw):
    from dsql_migrator.core.cdc_pg_slot import PgReplicationObjects

    base = dict(
        publication_name="p1",
        slot_name="s1",
        publication_present=True,
        publication_publishes_all_dml=True,
        publication_tables=frozenset({"app.orders", "app.items"}),
        slot_present_any_database=True,
        slot_usable=True,
    )
    base.update(kw)
    return PgReplicationObjects(**base)


def test_the_blocker_grades_coverage_not_just_existence() -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker

    selected = ["app.orders", "app.items"]

    # Everything present -> no block, for BOTH start shapes.
    assert blocker(_objects(), selected, resumes_from_slot=True) is None
    assert blocker(_objects(), selected, resumes_from_slot=False) is None

    # Absent publication: the reported crash. The message must name the remedy AND warn
    # off the one that silently loses data.
    absent = blocker(
        _objects(publication_present=False, publication_tables=frozenset(),
                 publication_publishes_all_dml=None),
        selected, resumes_from_slot=False,
    )
    assert absent is not None
    assert "Publication autocreation is disabled" in absent[1]
    assert "Full load + CDC" in absent[1]

    # A publication that omits a selected table is WORSE than a crash: RUNNING while
    # replicating nothing for it. It must block even though the publication "exists".
    gap = blocker(
        _objects(publication_tables=frozenset({"app.orders"})),
        selected, resumes_from_slot=False,
    )
    assert gap is not None and "app.items" in gap[1]

    # A publication restricted to a subset of INSERT/UPDATE/DELETE: also silent.
    narrowed = blocker(
        _objects(publication_publishes_all_dml=False), selected, resumes_from_slot=False
    )
    assert narrowed is not None and "INSERT/UPDATE/DELETE" in narrowed[1]


def test_a_missing_slot_blocks_a_resume_but_never_a_re_snapshot() -> None:
    # THE line that keeps this from over-blocking. With snapshot.mode=initial Debezium
    # creates the slot itself and snapshots every captured table, so there is no window
    # and nothing to lose -- blocking that start would leave the operator no path at all.
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker

    no_slot = _objects(slot_usable=False, slot_present_any_database=False)
    resume = blocker(no_slot, ["app.orders", "app.items"], resumes_from_slot=True)
    assert resume is not None
    assert "cannot be created at a past position" in resume[1]
    assert blocker(no_slot, ["app.orders", "app.items"], resumes_from_slot=False) is None

    # A name collision with an UNUSABLE slot blocks either way: the connector can neither
    # use it nor create its own under that name.
    collision = _objects(slot_usable=False, slot_present_any_database=True)
    for resumes in (True, False):
        verdict = blocker(collision, ["app.orders", "app.items"], resumes_from_slot=resumes)
        assert verdict is not None, resumes


def test_the_blocker_honours_the_connectors_own_publication_creation() -> None:
    """The v0.1.507 escape hatch shipped DEAD, and this is the assertion that proves it.

    The Start-CDC worker backstop calls this function right before creating anything. It
    blocked an absent publication unconditionally, so clicking "Re-snapshot every table
    instead" un-disabled Start and the job then raised "Start CDC blocked". The two keywords
    must mirror build_pg_source_config exactly: autocreate <-> filtered, resumes <-> never.
    """
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker

    nothing = _objects(
        publication_present=False,
        publication_publishes_all_dml=None,
        publication_tables=frozenset(),
        slot_present_any_database=False,
        slot_usable=False,
    )
    selected = ["app.orders", "app.items"]

    # The accepted re-snapshot: the connector makes both, so neither absence blocks.
    assert (
        blocker(
            nothing,
            selected,
            resumes_from_slot=False,
            connector_autocreates_publication=True,
        )
        is None
    )
    # Without the recorded intent it still blocks -- the default must not over-permit.
    assert blocker(nothing, selected, resumes_from_slot=False) is not None

    # The coverage branch must be SKIPPED too, not just the first one: an absent
    # publication has no tables, so grading coverage would report the whole selection
    # "missing" and block on the very next branch instead.
    verdict = blocker(
        nothing, selected, resumes_from_slot=False, connector_autocreates_publication=True
    )
    assert verdict is None

    # ...but a publication that EXISTS and is incomplete still blocks even with autocreate:
    # Debezium's `filtered` mode does not ALTER an existing publication.
    incomplete = _objects(publication_tables=frozenset({"app.orders"}))
    assert (
        blocker(
            incomplete,
            selected,
            resumes_from_slot=False,
            connector_autocreates_publication=True,
        )
        is not None
    )
    narrowed = _objects(publication_publishes_all_dml=False)
    assert (
        blocker(
            narrowed,
            selected,
            resumes_from_slot=False,
            connector_autocreates_publication=True,
        )
        is not None
    )


def test_an_invalidated_slot_is_not_usable() -> None:
    """An over-run slot is INVALIDATED (wal_status='lost'), and grading it usable turns a
    recoverable re-snapshot into a dead Debezium task. Live-verified on PostgreSQL 16.15:
    with max_slot_wal_keep_size=0 and enough churn the slot reached wal_status=lost while
    the source kept committing -- so this is a lost OPTION, not a source outage."""
    from dsql_migrator.core.cdc_pg_slot import read_pg_replication_objects

    conn = _ObjectsConn(covered=["app.orders"])
    read_pg_replication_objects(conn, publication_name="p", slot_name="s")
    joined = " ".join(s.upper() for s in conn.statements)
    assert "WAL_STATUS" in joined
    assert "'LOST'" in joined.replace('"', "'")
    # COALESCE keeps it NULL-safe: wal_status is PG13+ and the tool targets PG13-18.
    assert "COALESCE" in joined
    # ...and only on the USABLE subquery -- an invalidated slot still EXISTS in the
    # cluster, which is what slot_present_any_database means, and the name collision it
    # causes still has to be reported.
    usable_q = joined[joined.index("SLOT_OK_N") - 400 : joined.index("SLOT_OK_N")]
    assert "COALESCE(WAL_STATUS" in usable_q
    any_q = joined[: joined.index("SLOT_ANY_N")]
    assert "WAL_STATUS" not in any_q.split("SLOT_NAME = :SLOT")[-1]


# ---------------------------------------------------------------------------
# Audit honesty: a source write is recorded AFTER it lands, never before
# ---------------------------------------------------------------------------


def test_a_failed_source_write_is_logged_as_failed_not_as_done() -> None:
    """These writes are on the CUSTOMER's production source, so the log must not lie.

    The audit line used to be emitted before ``connection.execute``, so a publication
    that was never created, a REPLICA IDENTITY that was never widened, or a WAL-pinning
    slot that was never dropped were all recorded as facts. That is the worst kind of
    false reassurance: the operator stops looking for the thing that is still there.
    """
    from dsql_migrator.core.cdc_pg_slot import _run_write

    class _Boom:
        def execute(self, _statement, _params=None):
            raise RuntimeError("permission denied for database app\nCONTEXT: ignored")

    seen: list[str] = []
    with pytest.raises(RuntimeError):
        _run_write(
            _Boom(),
            'CREATE PUBLICATION "p" FOR TABLE "public"."t"',
            action="creating publication p",
            on_log=seen.append,
        )
    assert len(seen) == 1, seen
    assert seen[0].startswith("FAILED: creating publication p"), seen
    assert "permission denied" in seen[0]
    # First line only -- the server message is not ours to trust for length or content.
    assert "CONTEXT" not in seen[0], seen


def test_a_successful_source_write_is_logged_once_after_it_lands() -> None:
    from dsql_migrator.core.cdc_pg_slot import _run_write

    order: list[str] = []

    class _Conn:
        def execute(self, _statement, _params=None):
            order.append("executed")
            return _Result([])

    _run_write(
        _Conn(),
        'DROP PUBLICATION "p"',
        action="dropping publication p",
        on_log=lambda m: order.append(f"logged:{m}"),
    )
    assert order == ["executed", "logged:dropping publication p"], order


def test_a_swallowed_compensating_drop_still_says_the_publication_survived() -> None:
    """Compensation is best-effort, but its failure must not be silent.

    The publication this call created is still on the source arming the write outage the
    compensation exists to prevent, and the original slot error is what propagates -- so
    without a line the log claims the cleanup happened.
    """
    from dsql_migrator.core import cdc_pg_slot as mod

    conn = _FakeConn(fail_slot_create=True)
    real_execute = conn.execute

    def _execute(statement, params=None):
        if " ".join(str(statement).upper().split()).startswith("DROP PUBLICATION"):
            raise RuntimeError("must be owner of publication")
        return real_execute(statement, params)

    conn.execute = _execute  # type: ignore[assignment]
    seen: list[str] = []
    with pytest.raises(Exception):
        mod.provision_pg_replication(
            conn,
            tables=["public.t"],
            slot_name="dsqlmig_s",
            publication_name="dsqlmig_pub_s",
            on_log=seen.append,
        )
    assert any("STILL ON THE SOURCE" in m for m in seen), seen
    assert any("DROP PUBLICATION" in m for m in seen), seen


# ---------------------------------------------------------------------------
# A partitioned source table must actually replicate
# ---------------------------------------------------------------------------


def test_the_publication_publishes_via_the_partition_root() -> None:
    """Without this, CDC replicated NOTHING for a partitioned table — silently.

    PostgreSQL's default is ``publish_via_partition_root = false``: a publication naming a
    partitioned PARENT expands to its LEAVES and publishes every change under the LEAF's
    relation name. This tool collapses partitions into the parent, so Debezium's
    ``table.include.list`` names the PARENT — and every leaf-named change was filtered out
    and discarded. The connector ran, the slot advanced, and no row ever arrived. Verified
    on a live PostgreSQL 17: ``pubviaroot`` was ``f`` and ``pg_publication_tables`` returned
    both leaves; with the option on it returns the parent, which ALSO makes the reuse
    reconcile agree.
    """
    conn = _FakeConn()
    provision_pg_replication(
        conn,
        tables=["ecommerce.order_events"],
        slot_name="dsqlmig_s",
        publication_name="dsqlmig_pub_s",
    )
    created = [s for s in conn.writes() if s.upper().startswith("CREATE PUBLICATION")]
    assert len(created) == 1, conn.writes()
    sql = created[0]
    assert "publish_via_partition_root = true" in sql.lower(), sql
    # Still FOR TABLE <exact tables>, never FOR ALL TABLES (a write-outage guard).
    assert "FOR ALL TABLES" not in sql.upper(), sql
    assert '"ecommerce"."order_events"' in sql, sql


def test_the_option_is_a_statement_suffix_not_a_table_name() -> None:
    # Guard against the WITH clause landing inside the table list, which would name a
    # table called "WITH" and fail at the server.
    conn = _FakeConn()
    provision_pg_replication(
        conn, tables=["s.a", "s.b"], slot_name="sl", publication_name="pb"
    )
    sql = next(s for s in conn.writes() if s.upper().startswith("CREATE PUBLICATION"))
    assert sql.lower().rstrip().endswith("with (publish_via_partition_root = true)"), sql
    assert sql.index('"s"."b"') < sql.lower().index("with ("), sql


# ---------------------------------------------------------------------------
# G2: the runtime REPLICA IDENTITY re-read for re-keyed tables (read-only)
# ---------------------------------------------------------------------------


class _IdentityConn:
    """Records every execute; answers the one re-keyed identity statement with ``rows``."""

    def __init__(self, rows=(), *, fail=None):
        self.calls: list[tuple[str, dict]] = []
        self._rows = list(rows)
        self._fail = fail

    def execute(self, statement, params=None):
        self.calls.append((str(statement), dict(params or {})))
        if self._fail is not None:
            raise self._fail
        return _Result(self._rows)


def test_read_rekeyed_replica_identity_is_one_bounded_statement() -> None:
    from dsql_migrator.core.cdc_pg_slot import RekeyedIdentityRead, read_rekeyed_replica_identity

    sample = [f"app.events_p{i}" for i in range(5)]
    conn = _IdentityConn([
        ("app.orders", "d", ["id"], 0, []),
        ("app.events", "f", ["id"], 37, sample),
        ("app.items", "f", ["order_id", "id"], 0, None),
    ])
    reads = read_rekeyed_replica_identity(conn, ["app.orders", "app.events", "app.items", "app.orders"])
    # ONE execute for every table, the names bound (never interpolated), deduplicated.
    assert len(conn.calls) == 1
    sql, params = conn.calls[0]
    assert params == {"names": ["app.events", "app.items", "app.orders"]}
    assert "= ANY(:names)" in sql
    # Leaves aggregated server-side: a count and at most five names, never a row per leaf.
    assert "pg_partition_tree" in sql and "count(*)" in sql and "LIMIT 5" in sql
    assert "relkind IN ('r','p')" in sql
    # The grading predicates themselves (the live PG 13-17 run checks their semantics): a
    # LEAF is weak when it is NOT FULL, in both the count and the sample, and the live key is
    # the PRIMARY KEY, never a unique index.
    assert sql.count("t.isleaf AND l.relreplident <> 'f'") == 2, sql
    assert "i.indisprimary" in sql, sql
    # Catalog only: nothing that could write or lock a user table.
    upper = sql.upper()
    for word in ("INSERT", "UPDATE ", "DELETE", "ALTER", "LOCK", "FOR UPDATE"):
        assert word not in upper, word
    assert reads["app.orders"] == RekeyedIdentityRead("d", ("id",), 0, ())
    assert reads["app.events"] == RekeyedIdentityRead("f", ("id",), 37, tuple(sample))
    assert reads["app.items"].weak_partition_names == ()  # a NULL sample is empty
    assert reads["app.items"].primary_key == ("order_id", "id")
    # No names: no statement at all.
    idle = _IdentityConn()
    assert read_rekeyed_replica_identity(idle, []) == {} and idle.calls == []


def test_read_rekeyed_replica_identity_raises_on_failure() -> None:
    """Unreadable is not evidence: the failure propagates, never mapped to FULL."""
    from dsql_migrator.core.cdc_pg_slot import read_rekeyed_replica_identity

    with pytest.raises(RuntimeError):
        read_rekeyed_replica_identity(_IdentityConn(fail=RuntimeError("denied")), ["app.t"])


_IDENTITY_KEYS = {
    "adds-a-column": ["tenant_id", "id"],
    "pure-reorder": ["b", "a"],
    "omits-a-source-pk-column": ["tenant_id", "a"],
}


@pytest.mark.parametrize("identity", ["d", "n", "i", "f"])
@pytest.mark.parametrize("weak_partitions", [0, 3, 37])
@pytest.mark.parametrize("key_kind", sorted(_IDENTITY_KEYS))
def test_runtime_grade_matches_start_prerequisite(identity, weak_partitions, key_kind) -> None:
    """Parity: a state that passes Start never raises a runtime finding, and the reverse."""
    from dsql_migrator.core.cdc_pg_slot import RekeyedIdentityRead, grade_rekeyed_replica_identity
    from dsql_migrator.core.models import PrerequisiteStatus, TableDef
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_replica_identity_covers_key,
    )

    pk = ["id"] if key_kind == "adds-a-column" else ["a", "b"]
    key = _IDENTITY_KEYS[key_kind]
    leaves = {f"app.t_p{i}": "d" for i in range(weak_partitions)}
    leaves.update({f"app.t_ok{i}": "f" for i in range(2)})
    start = check_replica_identity_covers_key(
        TableDef(name="app.t", primary_key=pk),
        PostgresCdcFacts(replica_identity={"app.t": identity},
                         leaf_replica_identity={"app.t": leaves}),
        key,
    )
    expected = None if start is None else {
        PrerequisiteStatus.FAIL: "weak",
        PrerequisiteStatus.PASS: "full",
        PrerequisiteStatus.INFO: "unknown",
    }[start.status]
    weak_names = tuple(sorted(n for n, c in leaves.items() if c != "f"))[:5]
    read = RekeyedIdentityRead(identity, tuple(pk), weak_partitions, weak_names)
    assert grade_rekeyed_replica_identity("app.t", key, read) == expected
    # A missing row is always unknown -- never "not re-keyed", never FULL.
    assert grade_rekeyed_replica_identity("app.t", key, None) == "unknown"


# ---------------------------------------------------------------------------
# G20: the Full Load's fresh publication refuses -- BEFORE any write -- a table whose
# UPDATE/DELETE publishing would break on the source, with G17's strict rule
# ---------------------------------------------------------------------------


def _provision(conn, tables, *, full=(), logs=None):
    return provision_pg_replication(
        conn, slot_name="dsqlmig_s", publication_name="dsqlmig_pub_s", tables=tables,
        full_identity_tables=list(full), on_log=(logs.append if logs is not None else None),
    )


def _normalized(conn):
    return [" ".join(sql.upper().split()) for sql, _ in conn.statements]


def _assert_nothing_written(conn):
    # No publication, no slot, no ADD -- and no ALTER TABLE (writes() does not list it).
    assert conn.writes() == []
    assert not any(s.startswith("ALTER TABLE") for s in _normalized(conn))


def test_g20_a_default_identity_without_a_primary_key_is_refused_before_any_write() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    conn = _FakeConn(usability={"app.orders": ("r", "d", False, 0, [])})
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.orders", "app.items"], logs=logs)
    err = ei.value
    assert err.cause == "REPLICA IDENTITY DEFAULT, but no valid primary key"
    assert err.unreadable is False
    msg = str(err)
    assert msg.startswith(
        "app.orders: REPLICA IDENTITY DEFAULT, but no valid primary key. Published for CDC"
    )
    assert "Nothing was changed there" in msg and len(msg) <= 300 and "\n" not in msg
    assert (
        "refused before any write: app.orders has no usable REPLICA IDENTITY (REPLICA "
        "IDENTITY DEFAULT, but no valid primary key), so it is not published"
    ) in logs
    _assert_nothing_written(conn)
    # The strict read was the very first statement after the membership read.
    ups = _normalized(conn)
    assert "AS IDENTITY_USABLE" in ups[1] and "PUBALLTABLES" in ups[0]


def test_g20_an_unreadable_identity_is_refused_before_any_write() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    class OperationalError(Exception):
        pass

    conn = _FakeConn(usability_error=OperationalError("permission denied for pg_class"))
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.orders"], logs=logs)
    assert ei.value.unreadable is True and ei.value.cause is None
    msg = str(ei.value)
    assert msg.startswith("Whether each table to publish for CDC")
    assert "(OperationalError)" in msg and "permission denied" not in msg
    assert logs == [
        "refused before any write: the REPLICA IDENTITY read failed (OperationalError), so "
        "nothing is published"
    ]
    _assert_nothing_written(conn)


def test_g20_a_table_not_found_is_refused_before_any_write() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    conn = _FakeConn(usability_absent={"app.gone"})
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.orders", "app.gone"], logs=logs)
    assert ei.value.unreadable is True
    assert str(ei.value).startswith("app.gone: not found on the source as a table, so its")
    assert logs == [
        "refused before any write: app.gone was not found on the source as a table, so it is "
        "not published"
    ]
    _assert_nothing_written(conn)


def test_g20_a_partitioned_table_is_graded_by_its_partitions() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    usability = {"app.p": ("p", "d", True, 2, ["app.p_1", "app.p_2"])}
    for full in ((), ("app.p",)):  # widening the parent does not reach its partitions
        conn = _FakeConn(usability=usability)
        logs: list[str] = []
        with pytest.raises(PgReplicaIdentityError) as ei:
            _provision(conn, ["app.p"], full=full, logs=logs)
        msg = str(ei.value)
        assert msg.startswith("app.p: 2 of its partitions have none. Published for CDC")
        assert "REPLICA IDENTITY FULL on each partition" in msg
        assert "app.p_1" not in msg  # partition names go to the audit line
        assert any("app.p_1, app.p_2" in m for m in logs)
        _assert_nothing_written(conn)


def test_g20_a_rekeyed_plain_table_is_graded_as_the_full_it_gets_first() -> None:
    # Provisioning sets REPLICA IDENTITY FULL on a re-keyed table before the publication,
    # so a plain one without a primary key passes -- and the order is unchanged.
    conn = _FakeConn(usability={"app.orders": ("r", "d", False, 0, [])}, lsn="7/1")
    handles = _provision(conn, ["app.orders"], full=["app.orders"])
    assert handles.consistent_lsn == "7/1"
    ups = _normalized(conn)
    alter = next(i for i, s in enumerate(ups) if s.startswith("ALTER TABLE"))
    create = next(i for i, s in enumerate(ups) if s.startswith("CREATE PUBLICATION"))
    slot = next(i for i, s in enumerate(ups) if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in s)
    strict = next(i for i, s in enumerate(ups) if "AS IDENTITY_USABLE" in s)
    assert strict < alter < create < slot


def test_g20_a_rekeyed_plain_table_with_nothing_is_still_refused() -> None:
    # No weakening: graded as FULL by the strict check, it is still REPLICA IDENTITY
    # NOTHING to the lenient one -- refused before any write, as before G20.
    conn = _FakeConn(replident={"app.orders": "n"})
    with pytest.raises(PgReplicationError) as ei:
        _provision(conn, ["app.orders"], full=["app.orders"])
    assert "REPLICA IDENTITY NOTHING" in str(ei.value)
    _assert_nothing_written(conn)


def test_g20_a_reused_publication_refuses_before_its_first_add() -> None:
    # Before G20 the ADDs ran table by table, so app.b was added and only then app.c was
    # refused. Now every table the reuse would add is graded first.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    conn = _FakeConn(
        pubs={"dsqlmig_pub_s": ["app.a"]},
        usability={"app.c": ("r", "i", False, 0, [])},
    )
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.a", "app.b", "app.c"])
    assert str(ei.value).startswith(
        "app.c: REPLICA IDENTITY USING INDEX, but that index is gone or not valid."
    )
    assert conn.writes() == []  # no ALTER PUBLICATION ... ADD TABLE app.b
    # Only the tables it would add were read (app.a is already a member).
    strict = [p for sql, p in conn.statements if "AS IDENTITY_USABLE" in sql.upper()]
    assert strict == [{"names": ["app.b", "app.c"]}]


def test_g20_members_of_a_reused_publication_are_not_graded_strictly() -> None:
    # Already published: a Re-run that passed before still passes (its identity is not this
    # run's change)...
    conn = _FakeConn(
        pubs={"dsqlmig_pub_s": ["app.a"]},
        usability={"app.a": ("r", "d", False, 0, [])}, lsn="9/9",
    )
    assert _provision(conn, ["app.a"]).consistent_lsn == "9/9"
    assert not any("AS IDENTITY_USABLE" in s for s in _normalized(conn))
    # ...while a member with REPLICA IDENTITY NOTHING is still refused, as before.
    nothing = _FakeConn(pubs={"dsqlmig_pub_s": ["app.a"]}, replident={"app.a": "n"})
    with pytest.raises(PgReplicationError):
        _provision(nothing, ["app.a"])
    assert nothing.writes() == []


def test_g20_a_for_all_tables_publication_adds_nothing_so_grades_nothing() -> None:
    conn = _FakeConn(pubs={"dsqlmig_pub_s": []}, all_tables={"dsqlmig_pub_s"}, lsn="4/4")
    assert _provision(conn, ["app.orders"]).consistent_lsn == "4/4"
    assert not any("AS IDENTITY_USABLE" in s for s in _normalized(conn))


def test_g20_safe_tables_still_publish() -> None:
    usability = {
        "app.pk": ("r", "d", True, 0, []),
        "app.full": ("r", "f", True, 0, []),
        "app.idx": ("r", "i", True, 0, []),
        "app.part": ("p", "d", True, 0, []),
    }
    conn = _FakeConn(usability=usability, lsn="5/5")
    assert _provision(conn, sorted(usability)).consistent_lsn == "5/5"
    assert any(s.startswith("CREATE PUBLICATION") for s in _normalized(conn))


def test_g20_many_refused_tables_make_one_bounded_deterministic_line() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    names = [f"{chr(97 + i) * 63}.{'t' * 63}" for i in range(12)]
    usability = {n: ("r", "d", False, 0, []) for n in names}
    usability[names[0]] = ("r", "i", False, 0, [])  # the longest cause leads (sorted first)
    messages = set()
    for order in (names, list(reversed(names)), names[5:] + names[:5]):
        conn = _FakeConn(usability=usability)
        logs: list[str] = []
        with pytest.raises(PgReplicaIdentityError) as ei:
            _provision(conn, order, logs=logs)
        msg = str(ei.value)
        messages.add(msg)
        assert len(msg) <= 300 and "\n" not in msg
        assert "(+11 more tables)" in msg
        # The leading name is clipped at 32 characters (the review's per-cause remedies
        # need the room; every name is in full in its audit line).
        assert msg.startswith("a" * 29 + "...: REPLICA IDENTITY USING INDEX")
        assert len(logs) == 12 and logs == sorted(logs)
        _assert_nothing_written(conn)
    assert len(messages) == 1


@pytest.mark.parametrize(
    "usability, cause, remedy",
    [
        # A new primary key repairs DEFAULT only: PostgreSQL keeps NOTHING and USING INDEX
        # (relreplident) when a key is added, and a DEFERRABLE key is no replica identity.
        (("r", "d", False, 0, []), "REPLICA IDENTITY DEFAULT, but no valid primary key",
         "give it a primary key that is not DEFERRABLE, or REPLICA IDENTITY FULL"),
        (("r", "n", False, 0, []), "REPLICA IDENTITY NOTHING",
         "set REPLICA IDENTITY FULL, or DEFAULT with a primary key"),
        (("r", "i", False, 0, []),
         "REPLICA IDENTITY USING INDEX, but that index is gone or not valid",
         "set REPLICA IDENTITY FULL, or DEFAULT with a primary key"),
        # A partitioned table: the leaves carry the identity; its own NOTHING is its own.
        (("p", "d", True, 2, ["app.t_1", "app.t_2"]), "2 of its partitions have none",
         "set REPLICA IDENTITY FULL on each partition"),
        (("p", "n", False, 1, ["app.t_1"]), "REPLICA IDENTITY NOTHING",
         "set REPLICA IDENTITY FULL on it and on each partition"),
        (("p", "n", False, 0, []), "REPLICA IDENTITY NOTHING",
         "set REPLICA IDENTITY FULL on it, or DEFAULT with a primary key"),
    ],
)
def test_g20_review_each_refusal_names_the_remedy_that_fixes_its_cause(
    usability, cause, remedy
) -> None:
    # Review: every cause got "give it a primary key or REPLICA IDENTITY FULL", and the
    # first half cannot clear NOTHING, a gone identity index, or a DEFERRABLE key -- the
    # operator followed it and got the same refusal back.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    conn = _FakeConn(usability={"app.t": usability})
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.t"])
    msg = str(ei.value)
    assert msg == (
        f"app.t: {cause}. Published for CDC, its UPDATE/DELETE would fail on the source. "
        f"Nothing was changed there: {remedy}, then re-run the Full Load."
    )
    assert "give it a primary key or" not in msg
    assert ei.value.remedy == remedy  # G21: the remedy travels on the error too
    _assert_nothing_written(conn)


def test_g20_review_the_longest_refusal_still_fits_one_bounded_line() -> None:
    # The longest cause and remedy, a 127-character name and a six-digit count of other
    # refused tables: still one line the JobManager keeps whole (300 characters).
    from dsql_migrator.core.cdc_pg_slot import (
        ReplicaIdentityUsability,
        _publish_refusal_message,
        replica_identity_problem,
    )

    name = "s" * 63 + "." + "t" * 63
    for kind, code in (("r", "i"), ("r", "d"), ("r", "x"), ("p", "n")):
        read = ReplicaIdentityUsability(kind, code, False, 3, ("a", "b", "c"))
        problem = replica_identity_problem(read)
        msg = _publish_refusal_message([(name, problem, read)] * 100_000, ["app.gone"])
        assert "(+100000 more tables)" in msg and "\n" not in msg
        assert len(msg) <= 300, (kind, code, len(msg))
        assert msg.endswith(", then re-run the Full Load."), (kind, code)


# ---------------------------------------------------------------------------
# G21: every REPLICA IDENTITY refusal names the fix for its cause, in one bounded line
# ---------------------------------------------------------------------------

_NOTHING_REMEDY = "set REPLICA IDENTITY FULL, or DEFAULT with a primary key"


def test_g21_the_lenient_nothing_refusal_names_its_fix() -> None:
    # FAILS WITHOUT THE FIX: "Set a primary key or ALTER TABLE ... REPLICA IDENTITY FULL"
    # -- a new primary key does not clear NOTHING (PostgreSQL keeps relreplident) -- after a
    # list of every refused name, which the JobManager's 300-character cap could cut off.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    # A member of a reused publication: the strict check grades only what it would add.
    conn = _FakeConn(pubs={"dsqlmig_pub_s": ["app.a"]}, replident={"app.a": "n"})
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, ["app.a"], logs=logs)
    err = ei.value
    assert str(err) == (
        "app.a: REPLICA IDENTITY NOTHING. Published for CDC, its UPDATE/DELETE would fail on "
        "the source. Nothing was changed there: set REPLICA IDENTITY FULL, or DEFAULT with a "
        "primary key, then re-run the Full Load."
    )
    assert (err.cause, err.remedy, err.unreadable) == (
        "REPLICA IDENTITY NOTHING", _NOTHING_REMEDY, False
    )
    assert logs == ["refused before any write: app.a has REPLICA IDENTITY NOTHING"]
    _assert_nothing_written(conn)


def test_g21_many_nothing_tables_make_one_bounded_line_and_one_audit_line_each() -> None:
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError

    names = [f"{chr(97 + i) * 63}.{'t' * 63}" for i in range(12)]
    conn = _FakeConn(
        pubs={"dsqlmig_pub_s": list(names)}, replident={n: "n" for n in names}
    )
    logs: list[str] = []
    with pytest.raises(PgReplicaIdentityError) as ei:
        _provision(conn, list(reversed(names)), logs=logs)
    msg = str(ei.value)
    assert len(msg) <= 300 and "\n" not in msg
    assert "(+11 more tables)" in msg
    assert msg.startswith("a" * 29 + "...: REPLICA IDENTITY NOTHING")
    assert msg.endswith(f"{_NOTHING_REMEDY}, then re-run the Full Load.")
    assert logs == [
        f"refused before any write: {n} has REPLICA IDENTITY NOTHING" for n in sorted(names)
    ]
    _assert_nothing_written(conn)


@pytest.mark.parametrize(
    "usability, head, remedy",
    [
        (("r", "d", False, 0, []), "REPLICA IDENTITY DEFAULT, but no valid primary key",
         "give it a primary key that is not DEFERRABLE, or REPLICA IDENTITY FULL"),
        (("r", "n", False, 0, []), "REPLICA IDENTITY NOTHING", _NOTHING_REMEDY),
        (("r", "i", False, 0, []),
         "REPLICA IDENTITY USING INDEX, but that index is gone or not valid", _NOTHING_REMEDY),
        (("p", "d", True, 2, ["app.t_1", "app.t_2"]), "2 of its partitions have none",
         "set REPLICA IDENTITY FULL on each partition"),
        (("p", "n", False, 1, ["app.t_1"]), "REPLICA IDENTITY NOTHING",
         "set REPLICA IDENTITY FULL on it and on each partition"),
    ],
    ids=["no-pk", "nothing", "dropped-identity-index", "partitions", "nothing-and-a-leaf"],
)
def test_g21_a_one_table_add_names_the_fix_for_its_cause(usability, head, remedy) -> None:
    # FAILS WITHOUT THE FIX: every cause got "Give it a primary key, or set REPLICA IDENTITY
    # FULL (on each partition of a partitioned table)", which cannot clear NOTHING, a gone
    # identity index or a DEFERRABLE key.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError, add_table_to_publication

    conn = _FakeConn(pubs={"p": ["app.a"]}, usability={"app.t": usability})
    with pytest.raises(PgReplicaIdentityError) as ei:
        add_table_to_publication(conn, name="p", table="app.t")
    assert str(ei.value) == (
        f"app.t: {head}. Once published, every UPDATE and DELETE on it would fail on the "
        f"source, so it was not added: {remedy}, then re-run the Full Load."
    )
    assert ei.value.remedy == remedy
    assert ei.value.cause.startswith(head) and not ei.value.unreadable
    assert conn.writes() == []


def test_g21_the_longest_one_table_refusal_still_fits_one_bounded_line() -> None:
    # Its only displayed channel is the Full Load's error (JobManager: the first line, 300
    # characters): a 127-character name, a 63-character publication and the longest causes
    # and remedies still fit, and partition names stay out of the line.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError, add_table_to_publication

    table = "s" * 63 + "." + "t" * 63
    publication = "p" * 63
    leaf = "l" * 63 + "." + "m" * 63
    for usability in (
        ("r", "i", False, 0, []),
        ("r", "d", False, 0, []),
        ("p", "d", True, 999_999, [leaf] * 5),
    ):
        conn = _FakeConn(pubs={publication: ["app.a"]}, usability={table: usability})
        with pytest.raises(PgReplicaIdentityError) as ei:
            add_table_to_publication(conn, name=publication, table=table)
        msg = str(ei.value)
        assert len(msg) <= 300 and "\n" not in msg, (usability[:2], len(msg))
        assert msg.endswith(", then re-run the Full Load."), usability[:2]
        assert leaf not in msg
        assert conn.writes() == []
