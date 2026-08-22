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
)
from dsql_migrator.core.models import SourceConnectionConfig, SourceType


class _Result:
    def __init__(self, rows):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None


class _FakeConn:
    """Records executed statements; answers existence checks + the slot LSN from state."""

    def __init__(self, *, pubs=(), slots=(), lsn="3/AF012B8"):
        self.statements: list[tuple[str, dict]] = []
        self._pubs = set(pubs)
        self._slots = set(slots)
        self._lsn = lsn

    def execute(self, statement, params=None):
        sql = str(statement)
        params = params or {}
        self.statements.append((sql, params))
        up = " ".join(sql.upper().split())
        if "FROM PG_PUBLICATION" in up:
            return _Result([(1,)] if params.get("name") in self._pubs else [])
        if "FROM PG_REPLICATION_SLOTS" in up:
            return _Result([(1,)] if params.get("name") in self._slots else [])
        if "PG_CREATE_LOGICAL_REPLICATION_SLOT" in up:
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


def test_slot_and_publication_names_are_deterministic_and_sanitized() -> None:
    stack = "mysql-dsql-cdc-Prod.1"
    slot = pg_slot_name(stack)
    pub = pg_publication_name(stack)
    # Hyphens/dots/uppercase are reduced to the PostgreSQL slot charset [a-z0-9_].
    assert slot == "dsqlmig_mysql_dsql_cdc_prod_1"
    assert pub == "dsqlmig_pub_mysql_dsql_cdc_prod_1"
    import re

    assert re.fullmatch(r"[a-z0-9_]+", slot)
    # 63-char slot-name limit is respected.
    assert len(pg_slot_name("x" * 200)) <= 63


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


def test_create_publication_skips_when_it_already_exists() -> None:
    conn = _FakeConn(pubs={"dsqlmig_pub_s"})
    logs: list[str] = []
    create_publication(conn, name="dsqlmig_pub_s", tables=["app.orders"], on_log=logs.append)
    assert conn.writes() == []  # no CREATE issued
    assert any("already exists" in m for m in logs)


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
