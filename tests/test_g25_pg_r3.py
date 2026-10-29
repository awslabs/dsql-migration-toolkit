# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 repair round 3: the PostgreSQL findings still open after two rounds.

* an inheritance parent: the Full Load copied its children's rows (and Validation compared
  them) while CDC -- a ``FOR TABLE ONLY`` publication -- captures only the parent's own, so
  every later change of a child row was lost from the parent's copy (the descendant check
  this round reworded was removed in its second repair: nothing publishes the children);
* the slot-position Start check judged by a window recorded when a Start was SUBMITTED, so a
  Start any check refused exempted the next one -- after a restart too, and also when a
  check after the slot check refused it (round 4 removed every window: the check judges a
  stack's first Start by what the stack records, tests/test_g25_pg_r4.py);
* PostgreSQL 18, a re-keyed table with a generated column: the Full load + CDC Retry guard
  and the CDC monitor still offered REPLICA IDENTITY FULL, never "Keep source PK";
* the slot-position block on a resumed stack recommended a re-snapshot and then said a
  re-snapshot cannot be relied on there;
* PostgreSQL 18 remedies built from the parent-level read were wrong for a partitioned
  parent's own NOTHING (and for an inheritance child with its own generated column, a case
  the second repair removed with the descendant check).

Every test FAILS on the code before this round. Fakes only: no real database (the
inheritance reads were also run live on PostgreSQL 16.10, see the round-3 notes).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import ColumnDef, SourceConnectionConfig, SourceType, TableDef
from tests.test_g25_pg import AT_START, CAPTURED, MOVED, SLOT_REFUSAL, _dialog_gate, _start
from tests.test_g25_pg_publication import _Conn, _provision


def _par(name="app.par"):
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="integer"),
                 ColumnDef(name="v", mysql_type="text")],
        primary_key=["id"],
    )


# ---------------------------------------------------------------------------
# remaining[4]: an inheritance parent is read FROM ONLY -- its own rows, the rows CDC captures
# ---------------------------------------------------------------------------


class _SqlaResult:
    def __init__(self, rows=(), scalar=None):
        self._rows, self._scalar = list(rows), scalar

    def scalar(self):
        return self._scalar

    def mappings(self):
        rows = self._rows

        class _M:
            def __iter__(self):
                return iter(rows)

            def first(self):
                return rows[0] if rows else None

        return _M()


class _PgReadConn:
    """A guarded-engine double for the Full Load's read: it answers the live inheritance
    read with ``parent`` and serves ``rows`` to the first keyset page."""

    def __init__(self, parent, rows=({"id": 1, "v": "own"},)):
        self.parent, self.rows, self.sql = parent, list(rows), []

    def execution_options(self, **_k):
        return self

    def execute(self, statement, params=None, execution_options=None):
        sql = " ".join(str(statement).split())
        self.sql.append(sql)
        if "AS inheritance_parent" in sql:
            assert params == {"schema": "app", "name": "par"}
            return _SqlaResult(scalar=self.parent)
        if "MIN(" in sql:
            return _SqlaResult([{"lo": 1, "hi": 100}])
        if sql.startswith("SELECT") and "ORDER BY" in sql:
            rows, self.rows = self.rows, []
            return _SqlaResult(rows)
        return _SqlaResult()


class _Engine:
    def __init__(self, conn):
        self.conn = conn

    def connect(self):
        conn = self.conn

        class _Ctx:
            def __enter__(self):
                return conn

            def __exit__(self, *exc):
                return False

        return _Ctx()

    def dispose(self):
        pass


_PG = SourceConnectionConfig(source_type=SourceType.POSTGRES, host="h", port=5432,
                             database="app", username="u")


@pytest.mark.parametrize("parent", [True, False], ids=["inheritance-parent", "other"])
def test_the_full_load_reads_an_inheritance_parents_own_rows_only(parent) -> None:
    # FAILS WITHOUT THE FIX: "FROM "app"."par"" also returned every INHERITS child's rows --
    # live on PostgreSQL 16 the parent's copy held its child's rows, whose later changes CDC
    # (FOR TABLE ONLY) never captures. A partitioned table or a plain one reads as before.
    from dsql_migrator.core.exporter import TableExporter

    conn = _PgReadConn(parent)
    rows = list(TableExporter(engine_factory=lambda _c: _Engine(conn)).stream_converted_rows(
        _PG, _par()))
    assert [r["id"] for r in rows] == [1]
    inheritance = [s for s in conn.sql if "AS inheritance_parent" in s]
    page = next(s for s in conn.sql if "ORDER BY" in s)
    start = next(i for i, s in enumerate(conn.sql) if s.startswith("START TRANSACTION"))
    assert len(inheritance) == 1  # once per read, inside its snapshot, before the first page
    assert start < conn.sql.index(inheritance[0]) < conn.sql.index(page)
    if parent:
        assert ' FROM ONLY "app"."par" ORDER BY "app"."par"."id" ' in f"{page} "
    else:
        assert ' FROM "app"."par" ORDER BY "app"."par"."id" ' in f"{page} "


def test_the_file_export_reads_an_inheritance_parents_own_rows_only() -> None:
    from dsql_migrator.core.exporter import export_rows
    from dsql_migrator.core.source_dialect import dialect_for

    class _Writer:
        def __init__(self):
            self.rows = []

        def write_header(self, _cols):
            pass

        def write_row(self, row):
            self.rows.append(row)

    conn, writer = _PgReadConn(True), _Writer()
    assert export_rows(conn, _par(), writer, dialect=dialect_for(SourceType.POSTGRES)) == 1
    assert any(' FROM ONLY "app"."par" ORDER BY' in s for s in conn.sql)


def test_the_shard_plan_bands_an_inheritance_parents_own_rows() -> None:
    from dsql_migrator.core.exporter import TableExporter

    conn = _PgReadConn(True)
    ranges = TableExporter(engine_factory=lambda _c: _Engine(conn)).plan_pk_shard_ranges(
        _PG, _par(), 2)
    assert len(ranges) == 2
    assert any('FROM ONLY "app"."par"' in s and "MIN(" in s for s in conn.sql)


def test_the_inheritance_read_is_live_catalog_only_and_partition_safe() -> None:
    from dsql_migrator.core.source_dialect.postgres import (
        _PG_INHERITANCE_PARENT_SQL,
        pg_reads_own_rows_only,
    )

    # A plain table with children only: never a partitioned one, whose rows live in its
    # partitions (FROM ONLY would read none of them).
    assert "c.relkind = 'r' AND c.relhassubclass" in _PG_INHERITANCE_PARENT_SQL
    for word in ("INSERT", "UPDATE", "DELETE", "LOCK"):
        assert word not in _PG_INHERITANCE_PARENT_SQL.upper()

    class _Never:
        def execute(self, *_a, **_k):
            raise AssertionError("an unqualified name has no schema to read it in")

    assert pg_reads_own_rows_only(_Never(), "customers") is False
    assert pg_reads_own_rows_only(_PgReadConn(None), "app.par") is False  # not found


def _pg_validator_conn(statements):
    class _Res:
        def __init__(self, rows):
            self._rows = rows

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class _Sqla:
        connection = SimpleNamespace(driver_connection=None)  # renders Composed offline

        def exec_driver_sql(self, sql, params=None, execution_options=None):
            statements.append(sql)
            if "SUM(page_tok)" in sql:
                return _Res([(42, 2, 2)])
            if "SUM(ck_tok)" in sql:  # a composite key's page (G138): 2 rows, last key
                return _Res([(7, 2, "2", "b")])
            if sql.startswith("SELECT COALESCE(SUM("):  # the whole-table checksum
                return _Res([(7,)])
            if " AS tok " in sql:
                return _Res([(1, "t1"), (2, "t2")])
            if "COUNT(*)" in sql:
                return _Res([(2,)])
            return _Res([(1,), (2,)])

    return _Sqla()


def test_validation_reads_an_inheritance_parents_own_rows_on_every_source_read() -> None:
    # FAILS WITHOUT THE FIX: every PostgreSQL source read of the parent counted and summed its
    # children's rows -- live, a MISMATCH against the target once a child row changed (or, had
    # the Full Load been fixed alone, against a target that rightly lacks them).
    from dsql_migrator.core import validator as v
    from dsql_migrator.core.source_dialect import dialect_for

    pg = dialect_for(SourceType.POSTGRES)
    composite = TableDef(name="app.par", columns=_par().columns, primary_key=["id", "v"])
    statements: list[str] = []
    conn = _pg_validator_conn(statements)
    assert v._source_row_count_live(pg, conn, _par(), 5000, from_only=True) == 2
    assert v._source_row_count_live(pg, conn, composite, 5000, from_only=True) == 2
    assert v._source_checksum_for(pg, conn, _par(), 5000, from_only=True) == "42"
    assert v._source_checksum_for(pg, conn, composite, 5000, from_only=True) == "7"
    assert list(v._iter_source_pks_for(pg, conn, _par(), "id", 5000, from_only=True)) == [1, 2]
    assert v._source_pk_tokens_for(pg, conn, _par(), "id", 5, from_only=True) == {
        "1": "t1", "2": "t2"}
    # The composite checksum is paged and held to one COUNT(*): two statements (G138).
    assert len(statements) == 7
    for sql in statements:
        assert 'FROM ONLY "app"."par"' in sql, sql
        assert 'FROM "app"."par"' not in sql, sql
    # Not an inheritance parent: the statements are the target's, byte for byte.
    plain: list[str] = []
    v._source_row_count_live(pg, _pg_validator_conn(plain), _par(), 5000)
    assert plain and all("ONLY" not in s for s in plain)


def test_a_reader_whose_statement_has_no_from_table_fails_closed() -> None:
    from psycopg import sql

    from dsql_migrator.core.validator import _from_only, _pg_table_identifier

    with pytest.raises(RuntimeError, match="FROM ONLY"):
        _from_only(sql.SQL("SELECT 1"), _pg_table_identifier("app.par"))


def test_the_compare_decides_from_only_live_for_each_postgresql_table(monkeypatch) -> None:
    from dsql_migrator.core import validator as v
    from dsql_migrator.core.models import ValidationMode
    from dsql_migrator.core.source_dialect import dialect_for

    seen: dict = {}

    def _capture(name, value):
        def _reader(*_a, **kw):
            seen[name] = kw.get("from_only")
            return value
        return _reader

    monkeypatch.setattr(v, "_source_row_count_live", _capture("count", 2))
    monkeypatch.setattr(v, "_source_checksum_for", _capture("checksum", "42"))
    monkeypatch.setattr(v, "_bounded_target_count", lambda *_a, **_k: 2)
    monkeypatch.setattr(v, "_target_checksum", lambda *_a, **_k: "42")
    monkeypatch.setattr(v, "read_target_primary_key", lambda *_a: ("id",))  # G138

    class _Source:
        def __init__(self, parent):
            self.parent = parent

        def execute(self, statement, params=None):
            if "AS inheritance_parent" in str(statement):
                return SimpleNamespace(scalar=lambda: self.parent)
            return []  # the stale-inventory catalog read: nothing to compare

    for parent in (True, False):
        result = v.Validator()._compare_table(
            _Source(parent), object(), _par(), ValidationMode.CHECKSUM, None, False,
            lambda: False, dialect_for(SourceType.POSTGRES))
        assert result.matched is True
        assert seen == {"count": parent, "checksum": parent}


def test_a_parent_is_never_refused_for_its_child_tables(monkeypatch) -> None:
    # Round 3 refused a parent whose INHERITS children had no usable identity -- in the
    # provisioning, the one-table ADD, the Retry guard and the prerequisite -- although the
    # tool publishes it FOR TABLE ONLY / ADD TABLE ONLY and refuses the snapshot that would
    # let Debezium publish the children. Round 3's second repair grades a parent by its own
    # identity alone: none of the four refuses it.
    from dataclasses import replace

    from dsql_migrator.core.cdc_pg_slot import add_table_to_publication
    from dsql_migrator.core.models import PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect
    from tests.test_g17_full_load_and_cdc_additions import (
        _guard,
        _pg_migrator,
        _pg_watermark,
        _PgSource,
    )
    from tests.test_prerequisites import _G20ProbeConnection

    row = ("r", "d", True, 0, [])
    _provision(_Conn(usability={"app.par": row}), ["app.par"])
    assert add_table_to_publication(
        _Conn(pubs={"p": ["app.a"]}, usability={"app.par": row}), name="p", table="app.par")
    facts = PostgresSourceDialect().probe_cdc_prerequisites(
        _G20ProbeConnection(usable={"app.par": row}), ["app.par"])
    result = check_replica_identity(_par(), replace(facts, replica_identity={"app.par": "d"}))
    assert result.status is PrerequisiteStatus.PASS
    source = _PgSource(usability={"app.c": row})
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": None}


# ---------------------------------------------------------------------------
# remaining[5]: the slot-position check counts only a Start that went ahead -- after a restart
# (since round 4: only a Start the stack records, tests/test_g25_pg_r4.py)
# ---------------------------------------------------------------------------


def _restarted(state):
    """A real restart: the session snapshot the app saves (capture, then JSON, as every
    store writes it), applied to a fresh state."""
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    snapshot = capture_session_snapshot(
        "s1", SessionConnectionState(), EvaluationState(), SchemaConversionState(), state)
    fresh = DataMigrationState()
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(snapshot.model_dump_json()),
        SessionConnectionState(), EvaluationState(), SchemaConversionState(), fresh)
    return fresh


def test_a_refused_start_still_blocks_after_a_restart(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: only the submit-time window survived the restart, so the refused
    # Start's window exempted the next one -- which streamed from the moved slot.
    refused = _start(monkeypatch, confirmed=MOVED)
    assert SLOT_REFUSAL in str(refused["error"])
    restored = _restarted(refused["state"])
    blocked = _dialog_gate(monkeypatch, restored, confirmed=MOVED)
    assert blocked is not None and blocked[0] == SLOT_REFUSAL
    again = _start(monkeypatch, confirmed=MOVED, state=restored)
    assert SLOT_REFUSAL in str(again["error"]) and again["kwargs"] == {}


def test_a_start_that_went_ahead_still_counts_after_a_restart(monkeypatch) -> None:
    went = _start(monkeypatch, confirmed=AT_START)
    assert went["error"] is None and went["kwargs"]
    restored = _restarted(went["state"])
    # Saved as recorded: since round 4 no window decides the check, the stack's record does.
    assert restored.cdc_ops_window_start == CAPTURED
    # Stop -> Start: after the restart the stack-phase probe reads the stopped stack
    # (DeploySink=true, MskBootstrapServers blank -- tests/test_g25_pg_r4.py runs the probe).
    restored.set_cdc_has_committed_offset(True)
    assert _dialog_gate(monkeypatch, restored, confirmed=MOVED) is None
    # A Start window alone (what an older build saved, or a Start whose connectors were never
    # created) exempts nothing: the stack records no Start.
    from dsql_migrator.ui.data_migration import DataMigrationState

    old = DataMigrationState()
    old.set_cdc_ops_window_start(CAPTURED)
    blocked = _dialog_gate(monkeypatch, old, confirmed=MOVED)
    assert blocked is not None and blocked[0] == SLOT_REFUSAL


def test_a_start_refused_after_the_slot_check_does_not_count_for_it(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the slot check marked the Start as gone ahead, so a check after
    # it (here the column-exclusion overlay) refused it -- no connector -- and the next Start,
    # once the slot moved, skipped the check.
    from dsql_migrator.ui.data_migration import _cdc_ui

    def _overlay_fails(*_a, **_k):
        raise RuntimeError("Start CDC blocked: the overlay failed")

    real = _cdc_ui._apply_start_cdc_overlay
    monkeypatch.setattr(_cdc_ui, "_apply_start_cdc_overlay", _overlay_fails)
    first = _start(monkeypatch, confirmed=AT_START)
    assert "the overlay failed" in str(first["error"]) and first["kwargs"] == {}
    monkeypatch.setattr(_cdc_ui, "_apply_start_cdc_overlay", real)
    later = _start(monkeypatch, confirmed=MOVED, state=first["state"])
    assert SLOT_REFUSAL in str(later["error"]) and later["kwargs"] == {}


# ---------------------------------------------------------------------------
# remaining[6]: PostgreSQL 18, a re-keyed table with a generated column: Keep source PK
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("usability", [
    ("r", "d", True, 0, [], False, True, False),   # DEFAULT: the re-key needs FULL
    ("r", "f", False, 0, [], False, True, False),  # FULL: PostgreSQL 18 refuses it
    ("p", "d", True, 0, [], False, True, False),   # partitioned: its leaves would need FULL
], ids=["default", "full", "partitioned"])
def test_the_retry_guard_names_keep_source_pk_for_a_postgresql_18_rekey(
        monkeypatch, usability) -> None:
    # FAILS WITHOUT THE FIX: "Set REPLICA IDENTITY FULL on the source" while DEFAULT, then
    # "give it a primary key and REPLICA IDENTITY DEFAULT" once FULL -- a loop that never
    # named the fix the prerequisite and provisioning give.
    from dsql_migrator.ui.data_migration._full_load_engine import PG_REKEYED_FULL_REFUSED
    from tests.test_g17_full_load_and_cdc_additions import (
        _guard,
        _pg_migrator,
        _pg_watermark,
        _PgSource,
    )

    source = _PgSource(usability={"app.c": usability},
                       rekey_reads=[("app.c", usability[1], ["id"], 0, [])])
    out, _b = _guard(_pg_migrator(monkeypatch, source, rekey={"app.c": ["tenant", "id"]}),
                     ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_REKEYED_FULL_REFUSED.format(
        remedy='set it back to "Keep source PK" in Schema Conversion')}
    assert "Set REPLICA IDENTITY FULL" not in out["app.c"]
    assert source.writes() == []


def test_the_cdc_monitor_names_keep_source_pk_for_a_postgresql_18_rekey() -> None:
    from dsql_migrator.core.cdc_pg_slot import RekeyedIdentityRead, read_rekeyed_replica_identity
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        _transition_line,
        fold_replica_identity_probe,
        monitor_weak_notice,
    )
    from tests.test_cdc_pg_slot import _IdentityConn

    # The re-read carries PostgreSQL 18's generated-column fact...
    conn = _IdentityConn([("app.t", "d", ["id"], 0, [], True)])
    read = read_rekeyed_replica_identity(conn, ["app.t"])["app.t"]
    assert read == RekeyedIdentityRead("d", ("id",), 0, (), True)
    assert "a.attgenerated <> ''" in conn.calls[0][0]
    # ...into the FAILURE line, which no longer tells it to set FULL...
    probe = SimpleNamespace(ok=True, started_at=None, finished_at=None,
                            scope={"app.t": ("tenant", "id")}, reads={"app.t": read})
    findings, transitions = fold_replica_identity_probe({}, probe)
    (transition,) = transitions
    assert transition.full_refused is True
    _status, _action, detail = _transition_line(transition)
    assert 'Fix: set it back to "Keep source PK" in Schema Conversion' in detail
    assert "REPLICA IDENTITY FULL on the source (each partition too)" not in detail
    # ...and into the monitor's notice -- with every open table refused, its only fix
    # (tests/test_g25_pg_r3_repair.py covers the mixed case and CDC only).
    _header, body = monitor_weak_notice(findings, has_password=True, last_probe=probe)
    assert 'FULL is no fix here: on PostgreSQL 18 its generated columns' in body
    # PostgreSQL 13-17 (no flag): unchanged.
    plain = probe.reads["app.t"]._replace(full_refused=False)
    _f, (t2,) = fold_replica_identity_probe({}, SimpleNamespace(**{**vars(probe),
                                                                   "reads": {"app.t": plain}}))
    assert 'Fix: ALTER TABLE "app"."t" REPLICA IDENTITY FULL' in _transition_line(t2)[2]


# ---------------------------------------------------------------------------
# remaining[7]: the slot-position block on a resumed stack does not contradict itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("resumed", [True, False], ids=["resumed-stack", "never-started"])
def test_the_slot_position_block_names_one_way_out(monkeypatch, resumed) -> None:
    # FAILS WITHOUT THE FIX: on a stack started before, "Re-snapshot every table, or run the
    # Full Load again" was followed by "...so a re-snapshot cannot be relied on here". Since
    # round 4 such a stack is not judged on the slot at all (its own connector moved it), so
    # the one block left is a never-started stack's, whose re-snapshot does run.
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    state.set_cdc_has_committed_offset(resumed)
    blocked = _dialog_gate(monkeypatch, state, confirmed=MOVED)
    if resumed:
        assert blocked is None
        return
    assert blocked is not None and blocked[0] == SLOT_REFUSAL
    body = blocked[1]
    assert "a re-snapshot cannot be relied on here" not in body
    # G25 round 5: the gate names no route for a stack never started -- the route rule does,
    # where the re-snapshot remedy's standing is known (the Start CDC dialog, the job).
    assert body.endswith("silently skip the changes in between.")
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    assert _pg_block_route(state, SLOT_REFUSAL).startswith(
        "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
        "re-snapshots passes its own checks), or Re-run Full Load")


# ---------------------------------------------------------------------------
# remaining[8]: PostgreSQL 18 remedies built from the parent-level read
# ---------------------------------------------------------------------------


def test_a_partitioned_parents_own_nothing_keeps_full_as_its_fix() -> None:
    # FAILS WITHOUT THE FIX: "on PostgreSQL 18, FULL fails every UPDATE and DELETE" -- never
    # true of a partitioned parent's own identity (only its partitions' are checked), and the
    # Full Load's refusal for the same table says "set REPLICA IDENTITY FULL on it".
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts, check_replica_identity

    for partitioned, leaves in ((True, {}), (False, {"app.p": {"app.p1": "d"}})):
        table = TableDef(name="app.p", primary_key=["id"], partitioned=partitioned)
        facts = PostgresCdcFacts(replica_identity={"app.p": "n"}, leaf_replica_identity=leaves,
                                 full_identity_refused={"app.p": True})
        result = check_replica_identity(table, facts)
        assert "FULL fails" not in result.remediation
        assert 'ALTER TABLE "app"."p" REPLICA IDENTITY FULL' in result.remediation
    # A plain table keeps the PostgreSQL 18 wording.
    plain = check_replica_identity(
        TableDef(name="app.t", primary_key=["id"]),
        PostgresCdcFacts(replica_identity={"app.t": "n"}, full_identity_refused={"app.t": True}))
    assert "on PostgreSQL 18, FULL fails every UPDATE and DELETE" in plain.remediation
