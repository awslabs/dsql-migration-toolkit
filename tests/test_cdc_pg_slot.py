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
    'd', usable). ``slots`` are pre-existing slot names.
    """

    def __init__(
        self, *, pubs=None, slots=(), lsn="3/AF012B8", replident=None,
        fail_slot_create=False,
    ):
        self.statements: list[tuple[str, dict]] = []
        self._pubs = dict(pubs or {})
        self._slots = set(slots)
        self._lsn = lsn
        self._replident = dict(replident or {})
        self._fail_slot_create = fail_slot_create

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.statements.append((sql, params))
        up = " ".join(sql.upper().split())
        # PG_PUBLICATION_TABLES must be checked before PG_PUBLICATION (substring).
        if "FROM PG_PUBLICATION_TABLES" in up:
            tables = self._pubs.get(params.get("name"), [])
            return _Result([(t,) for t in tables])
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
            if up.startswith(("CREATE PUBLICATION", "DROP PUBLICATION")) or (
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


def test_create_publication_refuses_reuse_when_the_table_set_differs() -> None:
    # A re-run that added a table must NOT silently reuse the stale, narrower publication
    # (pgoutput only streams a publication's members -> the new table would be unreplicated).
    conn = _FakeConn(pubs={"dsqlmig_pub_s": ["app.orders"]})
    with pytest.raises(PgReplicationError) as ei:
        create_publication(conn, name="dsqlmig_pub_s", tables=["app.orders", "app.customers"])
    assert "app.customers" in str(ei.value) or "covers" in str(ei.value)


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
