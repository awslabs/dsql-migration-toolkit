# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G137: a large MySQL table is read in parallel reader shards on Full load + CDC.

Each MySQL shard reads its own snapshot, so a row whose key changes during the read can be
copied twice or not at all, and a transaction across two ranges half applied. CDC, started
after the load from the start point taken before any read, replays every change since then
and corrects them. So the multi-process planner splits a MySQL table only in a Full load + CDC
run whose watermark has a binlog file and position, only when every key Aurora DSQL enforces as
unique on it includes the leading key column, and (both engines) an append table with a changed
key only while the live table's key equals the applied one. A MySQL shard never re-reads in
place. The split is recorded per table (``ChunkState.reader_shards``), and every route that
would take such a table out of CDC's replay refuses it with the route. Doubles only: no
MySQL, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import dataclasses
import inspect
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.converter import (
    PrimaryKeyStrategy,
    SchemaConverter,
    SchemaConvertOptions,
    TableConversion,
    parse_target_unique_keys,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import (
    ChunkState,
    ColumnDef,
    IndexDef,
    MigrationJob,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TableSelection,
    TargetConnectionConfig,
    Watermark,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    DataMigrationInputs,
    _TableWorkerResult,
)
from dsql_migrator.ui.data_migration._models import (
    SPLIT_READ_HEADER,
    build_full_load_table_rows,
    split_read_refusal,
    split_read_tables,
)

from tests.test_full_load_overlay import _InlineExecutor
from tests.test_ui_data_migration import _FakeImporter, _RecordingUi, _lost_connection

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
_TS = datetime(2026, 10, 19, tzinfo=timezone.utc)
EVENTS = "app.events"
_DDL = (
    'CREATE TABLE "app"."events" (\n  "id" BIGINT NOT NULL,\n  "email" VARCHAR(64) NOT NULL,\n'
    '  PRIMARY KEY ("id")\n)'
)
_UQ_EMAIL = 'CREATE UNIQUE INDEX ASYNC "uq_email" ON "app"."events" ("email")'


def _table(name=EVENTS, *, lead="bigint", pk=("id",), extra=()):
    columns = [ColumnDef(name="id", mysql_type=lead, nullable=False),
               ColumnDef(name="email", mysql_type="varchar(64)", nullable=False),
               *extra]
    return TableDef(name=name, columns=columns, primary_key=list(pk))


def _conv(name=EVENTS, *, ddl=_DDL, indexes=()):
    return TableConversion(table=name, target_ddl=ddl, index_ddls=list(indexes))


def _wm(*, binlog=True, counts=None):
    return Watermark(
        binlog_file="mysql-bin.000009" if binlog else None,
        binlog_position=4 if binlog else None,
        snapshot_timestamp=_TS,
        table_row_counts=dict(counts or {}),
    )


def _inputs(tables, *, cdc=True, source=SourceType.MYSQL, conversions=None, replace=()):
    return DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app", source_type=source),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=list(tables)),
        is_cdc_migration=cdc,
        replace_tables=frozenset(replace),
        table_conversions=dict(conversions or {t.name: _conv(t.name) for t in tables}),
    )


# ---------------------------------------------------------------------------
# 1. The rule (pure)
# ---------------------------------------------------------------------------


def test_cdc_replays_the_load_only_for_mysql_full_load_and_cdc_with_a_binlog_point() -> None:
    f = engine._cdc_replays_load
    tables = [_table()]
    assert f(_inputs(tables), _wm()) is True
    assert f(_inputs(tables, cdc=False), _wm()) is False                     # Full load only
    assert f(_inputs(tables), None) is False
    assert f(_inputs(tables), _wm(binlog=False)) is False                    # no file:pos
    assert f(_inputs(tables), _wm().model_copy(update={"binlog_position": None})) is False
    assert f(_inputs(tables), _wm().model_copy(update={"binlog_file": None})) is False
    assert f(_inputs(tables, source=SourceType.POSTGRES), _wm()) is False    # shared snapshot
    # The flag is the UI's migration type at the load: CDC only runs no load at all.


def test_the_sharding_rule_with_cdc_replaying_the_load() -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _sharding_is_consistent,
        _table_may_shard,
    )

    assert _sharding_is_consistent(cdc_coexisting=False, shared_snapshot=False,
                                   cdc_replays_load=True)
    assert not _sharding_is_consistent(cdc_coexisting=False, shared_snapshot=False)
    for replace in (False, True):
        assert _table_may_shard(cdc_coexisting=False, shared_snapshot=False,
                                table_is_replace=replace, cdc_replays_load=True)
        assert not _table_may_shard(cdc_coexisting=False, shared_snapshot=False,
                                    table_is_replace=replace)
    # Unchanged: a coexisting stream does not let a REPLACE split.
    assert not _table_may_shard(cdc_coexisting=True, shared_snapshot=False,
                                table_is_replace=True)


def test_the_dead_shardable_ok_is_gone_and_the_in_process_gate_is_unchanged() -> None:
    src = inspect.getsource(engine)
    assert "_shardable_ok" not in src
    gate = inspect.getsource(BatchedTableMigrator.migrate_table)
    assert "if not is_replace and self._inputs.cdc_coexisting:" in gate


def test_the_in_process_path_still_reads_a_mysql_full_load_and_cdc_table_with_one_stream():
    # Tables in parallel 1 (the thread path): migrate_table keeps its coexisting-only rule.
    from tests.test_ui_data_migration import (
        _FakeExporter,
        _FakeWatermarkCapturer,
        _inputs as _base_inputs,
        _watermark,
    )

    exporter = _FakeExporter(rows_by_table={"orders": [{"id": i} for i in range(1, 7)]})
    exporter.shard_ranges_by_table["orders"] = [(None, 3), (3, 5), (5, None)]
    importer = _FakeImporter()
    inputs = dataclasses.replace(_base_inputs(), is_cdc_migration=True)
    migrator = BatchedTableMigrator(
        inputs, exporter=exporter, watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
    )
    migrator.migrate_table(inputs.inventory.tables[0])
    assert exporter.streamed == ["orders"]


# ---------------------------------------------------------------------------
# 2. The unique keys Aurora DSQL enforces, per the applied DDL
# ---------------------------------------------------------------------------


def _convert(table, **options):
    return SchemaConverter().convert_table(table, SchemaConvertOptions(**options))


def test_parse_target_unique_keys_reads_the_converters_own_output() -> None:
    users = TableDef(name="app.users", columns=[
        ColumnDef(name="id", mysql_type="bigint", nullable=False),
        ColumnDef(name="email", mysql_type="varchar(120)", nullable=False),
        ColumnDef(name="tenant", mysql_type="int", nullable=False)],
        primary_key=["id"],
        indexes=[IndexDef(name="uq_email", columns=["email"], unique=True),
                 IndexDef(name="ix_t", columns=["tenant"])])
    plain = _convert(users)
    assert parse_target_unique_keys(plain.target_ddl, plain.index_ddls) == [["id"], ["email"]]
    no_unique = users.model_copy(update={"indexes": [IndexDef(name="ix_t", columns=["tenant"])]})
    composite = _convert(no_unique, primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY,
                         composite_leading_column="tenant")
    assert parse_target_unique_keys(composite.target_ddl, composite.index_ddls) == [
        ["tenant", "id"], ["id"]]
    # A plain CREATE INDEX is no key.
    assert parse_target_unique_keys(_convert(no_unique).target_ddl,
                                    _convert(no_unique).index_ddls) == [["id"]]


@pytest.mark.parametrize("ddl, indexes, keys", [
    # Column-level UNIQUE, folded as PostgreSQL stores it; a quoted name kept as written.
    ('CREATE TABLE t (id BIGINT PRIMARY KEY, Email TEXT UNIQUE)', [], [["id"], ["email"]]),
    # Table-level, unnamed and named; a named primary key.
    ('CREATE TABLE t (id BIGINT, t INT, e TEXT, CONSTRAINT pk PRIMARY KEY (ID), '
     'UNIQUE (t, e), CONSTRAINT uq UNIQUE ("E"))', [], [["id"], ["t", "e"], ["E"]]),
    # An identity clause (sqlglot reads it only stripped).
    ('CREATE TABLE t (id BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 65536), '
     'PRIMARY KEY (id))', ['CREATE UNIQUE INDEX ASYNC u ON t (id, x)'], [["id"], ["id", "x"]]),
    # A partial unique index is unique within its predicate: its columns count.
    ('CREATE TABLE t (id BIGINT PRIMARY KEY)',
     ['CREATE UNIQUE INDEX ASYNC "u" ON t ("x") WHERE "x" > 0'], [["id"], ["x"]]),
    # A plain index, even one whose text names UNIQUE in a literal, is no key.
    ("CREATE TABLE t (id BIGINT PRIMARY KEY, n TEXT DEFAULT 'UNIQUE')",
     ["CREATE INDEX ASYNC i ON t (n) WHERE n <> 'UNIQUE'"], [["id"]]),
])
def test_parse_target_unique_keys_shapes(ddl, indexes, keys) -> None:
    assert parse_target_unique_keys(ddl, indexes) == keys


@pytest.mark.parametrize("ddl, indexes", [
    ('CREATE TABLE t (id BIGINT PRIMARY KEY)', ['CREATE UNIQUE INDEX ASYNC u ON t ((lower(x)))']),
    ('CREATE TABLE t (id BIGINT PRIMARY KEY)', ['CREATE UNIQUE ASYNC nonsense']),
    ('not sql at all', []),
    ('CREATE TABLE t (id BIGINT, x INT)', []),                     # no primary key
])
def test_parse_target_unique_keys_refuses_what_it_cannot_read(ddl, indexes) -> None:
    assert parse_target_unique_keys(ddl, indexes) is None


class _Cursor:
    def __init__(self, rows=None, error=None):
        self.rows, self.error, self.executed = rows or [], error, []

    def execute(self, statement, params=None):
        self.executed.append((statement, params))
        if self.error:
            raise self.error

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _Conn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def test_target_unique_keys_reads_every_unique_index_of_the_live_table() -> None:
    from dsql_migrator.core.target_introspector import target_unique_keys

    cursor = _Cursor(rows=[(1, ["id"]), (2, ["tenant_id", "email"])])
    assert target_unique_keys("app.users", connection_factory=lambda: _Conn(cursor)) == [
        ["id"], ["tenant_id", "email"]]
    statement, params = cursor.executed[0]
    assert "ix.indisunique" in statement and params == {"schema": "app", "table": "users"}
    # Unknown, never "no unique key": an expression column, no row (the table is missing),
    # an unreadable catalog or no connection.
    expression = _Cursor(rows=[(1, ["id"]), (2, [])])
    assert target_unique_keys("app.users", connection_factory=lambda: _Conn(expression)) is None
    short = _Cursor(rows=[(2, ["a"])])
    assert target_unique_keys("app.users", connection_factory=lambda: _Conn(short)) is None
    assert target_unique_keys("app.users", connection_factory=lambda: _Conn(_Cursor())) is None
    failing = _Cursor(error=RuntimeError("boom"))
    assert target_unique_keys("app.users", connection_factory=lambda: _Conn(failing)) is None

    def _no_connection():
        raise OSError("down")

    assert target_unique_keys("app.users", connection_factory=_no_connection) is None


# ---------------------------------------------------------------------------
# 3. The process planner (workers inline, their work recorded)
# ---------------------------------------------------------------------------


class _Handle:
    """JobHandle stand-in: one job the planner and the result loop write to."""

    job_id = "j"

    def __init__(self, names):
        self.job = MigrationJob(job_id="j", chunks=[ChunkState(chunk_id=n) for n in names])
        self.cancelled = False
        self.heartbeats = 0

    def update(self, mutator):
        mutator(self.job)

    def heartbeat(self):
        self.heartbeats += 1

    def snapshot(self):
        return self.job


class _Planner:
    """The exporter's planning read only: the ranges per table, and every call recorded."""

    def __init__(self, ranges):
        self.ranges = ranges
        self.planned: list = []

    def plan_pk_shard_ranges(self, conn, table, shards, *, min_rows=0):
        self.planned.append(table.name)
        return list(self.ranges.get(table.name, [(None, None)]))


_FOUR = [(None, 250), (250, 500), (500, 750), (750, None)]


class _IndexImporter:
    def __init__(self):
        self.created: list = []

    def create_indexes(self, ddls):
        self.created.append(list(ddls))
        return len(ddls), []


@pytest.fixture
def rig(monkeypatch):
    """Run ``_migrate_tables_in_parallel`` on the process path with the workers inline."""
    activity: list = []
    shards: list = []
    wholes: list = []
    fail_shards: set = set()      # FAILED before its source read handed the importer a row
    partial_shards: set = set()   # FAILED after committing rows its count cannot see
    fail_wholes: set = set()

    def _shard(args):
        shards.append(args)
        failed = args.shard_index in fail_shards | partial_shards
        return _TableWorkerResult(table_name=args.table.name,
                                  status="FAILED" if failed else "DONE",
                                  rows_loaded=0 if failed else 10,
                                  shard_index=args.shard_index,
                                  wrote_nothing=args.shard_index in fail_shards)

    def _whole(args):
        wholes.append(args)
        if args.table.name in fail_wholes:
            return _TableWorkerResult(table_name=args.table.name, status="FAILED",
                                      error_message="boom")
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=40)

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", lambda m, d: (None, None, "snap-1"))
    monkeypatch.setattr(engine, "log_activity",
                        lambda cat, action, **kw: activity.append((action, kw.get("target"),
                                                                   kw.get("detail"))))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")
    _InlineExecutor.submitted = []

    def run(tables, *, cdc=True, source=SourceType.MYSQL, watermark=None, fresh=True,
            conversions=None, replace=(), ranges=None, live_pk=None, live_unique=None,
            handle=None, recreator=None):
        inputs = _inputs(tables, cdc=cdc, source=source, conversions=conversions,
                         replace=replace)
        planner = _Planner({t.name: _FOUR for t in tables} if ranges is None else ranges)
        index_importer = _IndexImporter()
        pk_reads: list = []
        unique_reads: list = []

        def _pk(table):
            pk_reads.append(table.name)
            return live_pk(table) if live_pk else list(table.primary_key)

        def _unique(table):
            unique_reads.append(table.name)
            return live_unique(table) if live_unique else [list(table.primary_key)]

        migrator = BatchedTableMigrator(
            inputs, exporter=planner, watermark_capturer=object(),
            importer_factory=lambda _i: index_importer,
            table_recreator=recreator or (
                lambda t: [_UQ_EMAIL.replace("UNIQUE INDEX", "INDEX")]
            ),
            target_counter=lambda t: None, target_pk_reader=_pk,
            target_generated_reader=lambda t: set(), target_unique_keys_reader=_unique,
        )
        handle = handle or _Handle([t.name for t in tables])
        log = ErrorLogStore()
        counts = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, log,
            watermark=_wm() if watermark is None else watermark, fresh_watermark=fresh,
        )
        return SimpleNamespace(
            errors=[r.message for r in log.records("j")],
            counts=counts, handle=handle, planned=planner.planned, shards=list(shards),
            wholes=list(wholes), activity=list(activity), created=index_importer.created,
            pk_reads=pk_reads, unique_reads=unique_reads,
            parts={c.chunk_id: c.reader_shards for c in handle.job.chunks},
            status={c.chunk_id: c.status for c in handle.job.chunks},
        )

    run.fail_shards = fail_shards
    run.partial_shards = partial_shards
    run.fail_wholes = fail_wholes
    run.reset = lambda: (activity.clear(), shards.clear(), wholes.clear(), fail_shards.clear(),
                         partial_shards.clear(), fail_wholes.clear())
    return run


def _split_lines(r):
    return [(t, d) for a, t, d in r.activity if a == "reader shards"]


def test_a_mysql_full_load_and_cdc_append_table_is_split_in_four_own_snapshots(rig) -> None:
    r = rig([_table()])
    assert [(a.pk_lower, a.pk_upper, a.shard_index) for a in r.shards] == [
        (None, 250, 0), (250, 500, 1), (500, 750, 2), (750, None, 3)]
    assert {a.shared_snapshot_id for a in r.shards} == {None}   # MySQL: each its own
    assert r.wholes == [] and r.status == {EVENTS: "DONE"}
    assert r.parts == {EVENTS: 4}                                 # recorded on the job
    assert _split_lines(r) == [(EVENTS, engine._SPLIT_OWN_SNAPSHOTS.format(k=4))]
    assert "CDC, from the start point taken before any read, corrects rows that changed " \
           "between them" in _split_lines(r)[0][1]
    # The append's unique guard read the live table once; its key is unchanged, so no key read.
    assert r.unique_reads == [EVENTS] and r.pk_reads == []


def test_a_mysql_drop_and_reload_table_splits_and_builds_its_indexes_once(rig) -> None:
    r = rig([_table()], replace=[EVENTS])
    assert len(r.shards) == 4 and r.status == {EVENTS: "DONE"}
    assert r.created == [[_UQ_EMAIL.replace("UNIQUE INDEX", "INDEX")]]  # once, after all four
    assert r.unique_reads == []          # recreated from the applied DDL: no live read
    rig.reset()
    rig.fail_shards.add(2)
    failed = rig([_table()], replace=[EVENTS])
    assert failed.status == {EVENTS: "FAILED"} and failed.created == []


def test_full_load_only_reads_every_mysql_table_with_one_reader_and_says_so(rig) -> None:
    r = rig([_table()], cdc=False)
    assert r.planned == [] and r.shards == []
    assert [a.table.name for a in r.wholes] == [EVENTS]
    assert r.parts == {EVENTS: None}
    assert _split_lines(r) == [(None, engine._RUN_FULL_LOAD_ONLY)]


def test_a_run_whose_start_point_has_no_binlog_position_reads_with_one_reader(rig) -> None:
    r = rig([_table()], watermark=_wm(binlog=False))
    assert r.planned == [] and r.shards == [] and len(r.wholes) == 1
    assert _split_lines(r) == [(None, engine._RUN_NO_BINLOG_POINT)]


def test_reader_shards_one_says_nothing_new(rig, monkeypatch) -> None:
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
    for cdc in (True, False):
        r = rig([_table()], cdc=cdc)
        assert r.shards == [] and _split_lines(r) == [] and r.parts == {EVENTS: None}


def test_a_unique_key_without_the_leading_column_keeps_one_reader(rig) -> None:
    r = rig([_table()], replace=[EVENTS],
            conversions={EVENTS: _conv(indexes=[_UQ_EMAIL])})
    assert r.shards == [] and len(r.wholes) == 1 and r.parts == {EVENTS: None}
    ((target, detail),) = _split_lines(r)
    assert target == EVENTS and detail.startswith(
        "read with one reader: its unique key (email) does not include id, the column its "
        "parts would be split on")
    # A unique key that includes the leading column splits.
    rig.reset()
    with_id = 'CREATE UNIQUE INDEX ASYNC "uq" ON "app"."events" ("email", "id")'
    ok = rig([_table()], replace=[EVENTS], conversions={EVENTS: _conv(indexes=[with_id])})
    assert len(ok.shards) == 4
    # So does one declared in the CREATE TABLE.
    rig.reset()
    inline = _DDL.replace('"email" VARCHAR(64) NOT NULL', '"email" VARCHAR(64) NOT NULL UNIQUE')
    refused = rig([_table()], replace=[EVENTS], conversions={EVENTS: _conv(ddl=inline)})
    assert refused.shards == [] and "unique key (email)" in _split_lines(refused)[0][1]


def test_an_unreadable_converted_ddl_refuses_the_split(rig) -> None:
    r = rig([_table()], replace=[EVENTS], conversions={EVENTS: _conv(
        indexes=['CREATE UNIQUE INDEX ASYNC u ON "app"."events" ((lower("email")))'])})
    assert r.shards == []
    assert _split_lines(r)[0][1] == (
        "read with one reader: its unique keys could not be read from its converted DDL")


def test_an_append_table_is_judged_on_the_live_tables_unique_indexes(rig) -> None:
    # The applied DDL has no unique index; the live table does (added by hand).
    r = rig([_table()], live_unique=lambda t: [["id"], ["email"]])
    assert r.shards == [] and "unique key (email) does not include id" in _split_lines(r)[0][1]
    rig.reset()
    unknown = rig([_table()], live_unique=lambda t: None)
    assert unknown.shards == []
    assert _split_lines(unknown)[0][1] == (
        "read with one reader: its unique indexes on Aurora DSQL could not be read")


def test_a_guard_that_raises_keeps_one_reader_and_never_fails_the_run(rig) -> None:
    def _boom(_table):
        raise RuntimeError("catalog down")

    r = rig([_table()], live_unique=_boom)
    assert r.shards == [] and len(r.wholes) == 1 and r.status == {EVENTS: "DONE"}
    assert _split_lines(r)[0][1] == "read with one reader: its keys could not be read"


def _composite():
    return _conv(ddl='CREATE TABLE "app"."events" ("id" BIGINT NOT NULL, "email" VARCHAR(64) '
                     'NOT NULL, "tenant" INT NOT NULL, PRIMARY KEY ("tenant", "id"))',
                 indexes=['CREATE UNIQUE INDEX ASYNC "ux" ON "app"."events" ("id")'])


@pytest.mark.parametrize("source", [SourceType.MYSQL, SourceType.POSTGRES])
def test_a_changed_key_append_table_splits_only_on_the_applied_live_key(rig, source) -> None:
    table = _table(extra=[ColumnDef(name="tenant", mysql_type="int", nullable=False)])
    same = rig([table], source=source, conversions={EVENTS: _composite()},
               live_pk=lambda t: ["tenant", "id"],
               live_unique=lambda t: [["tenant", "id"], ["id"]])
    assert len(same.shards) == 4 and same.pk_reads == [EVENTS]
    rig.reset()
    old = rig([table], source=source, conversions={EVENTS: _composite()},
              live_pk=lambda t: ["id"])
    assert old.shards == [] and len(old.wholes) == 1
    assert _split_lines(old)[0][1] == (
        "read with one reader: its applied primary key (tenant, id) differs from the source's, "
        "and the Aurora DSQL table's key is (id), so it loads as one unit, which recreates an "
        "empty table with the applied key or refuses the load")
    rig.reset()
    unread = rig([table], source=source, conversions={EVENTS: _composite()},
                 live_pk=lambda t: None)
    assert unread.shards == [] and "key could not be read" in _split_lines(unread)[0][1]


def test_postgresql_splits_in_any_run_on_the_shared_snapshot_with_no_unique_guard(rig) -> None:
    for cdc in (True, False):
        r = rig([_table()], cdc=cdc, source=SourceType.POSTGRES, watermark=_wm(binlog=False),
                replace=[EVENTS], conversions={EVENTS: _conv(indexes=[_UQ_EMAIL])})
        assert len(r.shards) == 4 and {a.shared_snapshot_id for a in r.shards} == {"snap-1"}
        assert _split_lines(r) == [(EVENTS, engine._SPLIT_SHARED_SNAPSHOT.format(k=4))]
        assert r.parts == {EVENTS: 4}
        rig.reset()


def test_the_watermark_estimate_spares_the_planning_read_of_a_small_table(rig) -> None:
    small = rig([_table()], watermark=_wm(counts={EVENTS: 10}))
    assert small.planned == [] and small.shards == [] and len(small.wholes) == 1
    rig.reset()
    unknown = rig([_table()], watermark=_wm(counts={EVENTS: 0}))     # 0 is unknown there
    assert unknown.planned == [EVENTS] and len(unknown.shards) == 4
    rig.reset()
    retry = rig([_table()], watermark=_wm(counts={EVENTS: 10}), fresh=False)
    assert retry.planned == [EVENTS]                     # a retry's estimates may be stale
    rig.reset()
    big = rig([_table()], watermark=_wm(counts={EVENTS: 5000}))
    assert big.planned == [EVENTS] and len(big.shards) == 4


def test_the_planning_pre_pass_heartbeats_per_candidate(rig) -> None:
    tables = [_table(), _table("app.more"), _table("app.text_key", lead="varchar(10)")]
    r = rig(tables, conversions={t.name: _conv(t.name) for t in tables})
    assert r.planned == [EVENTS, "app.more"]   # a non-integer leading column is no candidate
    assert r.handle.heartbeats == 2            # one per planned candidate (no recreate here)


def _marked(*names, parts=4):
    """A job whose ``names`` an earlier load read in ``parts`` parts (their rows still there)."""
    handle = _Handle(list(names or (EVENTS,)))
    for chunk in handle.job.chunks:
        chunk.reader_shards = parts
    return handle


def test_an_append_reload_keeps_the_record_of_the_rows_read_in_parts(rig, monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the reload the route led to -- Reader shards 1, the dialog's
    # default Append -- cleared the record while the rows read in parts stayed on Aurora DSQL
    # (ON CONFLICT DO NOTHING keeps them), and every refusal lifted with it.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
    append = rig([_table()], handle=_marked())
    assert append.shards == [] and append.status == {EVENTS: "DONE"}
    assert append.parts == {EVENTS: 4}
    rig.reset()
    flo = rig([_table()], cdc=False, handle=_marked())         # Full load only appends too
    assert flo.parts == {EVENTS: 4}
    rig.reset()
    # A split append keeps the larger count: the earlier parts' rows are still there.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    two = rig([_table()], ranges={EVENTS: [(None, 500), (500, None)]}, handle=_marked())
    assert len(two.shards) == 2 and two.parts == {EVENTS: 4}
    rig.reset()
    fresh = rig([_table()], ranges={EVENTS: [(None, 500), (500, None)]})
    assert fresh.parts == {EVENTS: 2}


def test_a_one_unit_or_failed_retry_keeps_the_record(rig) -> None:
    # Reader shards still 4, but the guard could not read the live unique indexes: the table
    # loads as one unit, appending over the rows read in parts -- kept.
    one_unit = rig([_table()], live_unique=lambda t: None, handle=_marked())
    assert one_unit.shards == [] and len(one_unit.wholes) == 1
    assert one_unit.parts == {EVENTS: 4}
    rig.reset()
    # A retry that fails (or stops) changes nothing the earlier parts wrote.
    rig.fail_wholes.add(EVENTS)
    failed = rig([_table()], live_unique=lambda t: None, handle=_marked())
    assert failed.status == {EVENTS: "FAILED"} and failed.parts == {EVENTS: 4}
    rig.reset()
    rig.fail_shards.update({0, 1, 2, 3})       # split again; every part failed at once
    failed_split = rig([_table()], handle=_marked())
    assert failed_split.status == {EVENTS: "FAILED"} and failed_split.parts == {EVENTS: 4}
    # A retry seeds its tables with the record they had (the engine's own seed, below).
    job = MigrationJob(job_id="j2")
    engine._seed_retry_chunks(job, [ChunkState(chunk_id=EVENTS, status="FAILED",
                                               reader_shards=4)], {EVENTS})
    assert job.chunks[0].status == "PENDING" and job.chunks[0].reader_shards == 4


def test_only_a_load_that_recreates_the_table_clears_the_record(rig, monkeypatch) -> None:
    # A Drop & reload with one reader recreates the table (empty, from the applied DDL) in the
    # pre-pass before any read: the rows read in parts are gone, and so is their record.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
    drop = rig([_table()], replace=[EVENTS], handle=_marked())
    assert drop.status == {EVENTS: "DONE"} and drop.parts == {EVENTS: None}
    rig.reset()
    # ...even when the load after the recreate fails: the earlier copy is gone either way.
    rig.fail_wholes.add(EVENTS)
    gone = rig([_table()], replace=[EVENTS], handle=_marked())
    assert gone.status == {EVENTS: "FAILED"} and gone.parts == {EVENTS: None}
    rig.reset()
    # A Drop & reload that reads it in parts again records its own plan, not the larger one.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    again = rig([_table()], replace=[EVENTS], ranges={EVENTS: [(None, 500), (500, None)]},
                handle=_marked())
    assert len(again.shards) == 2 and again.parts == {EVENTS: 2}


def test_a_recreate_that_aborts_the_run_clears_only_the_tables_it_recreated(rig) -> None:
    # No overlay pre-pass: a recreate failure aborts the run (as before). The table recreated
    # before it is empty -- cleared; the one whose recreate failed keeps its rows and record.
    other = "app.other"

    def _recreate(table):
        if table.name == other:
            raise RuntimeError("DDL refused")
        return []

    tables = [_table(), _table(other)]
    handle = _marked(EVENTS, other)
    with pytest.raises(RuntimeError, match="DDL refused"):
        rig(tables, replace=[EVENTS, other], conversions={t.name: _conv(t.name) for t in tables},
            handle=handle, recreator=_recreate,
            ranges={EVENTS: [(None, None)], other: [(None, None)]})
    assert {c.chunk_id: c.reader_shards for c in handle.job.chunks} == {EVENTS: None, other: 4}


def test_a_split_whose_parts_wrote_no_row_is_not_recorded(rig) -> None:
    # FAILS WITHOUT THE FIX: the plan was recorded before any part ran, so a table no part of
    # which wrote a row was refused with "copied twice or not at all" -- nothing was copied.
    rig.fail_shards.update({0, 1, 2, 3})       # each part failed before its first batch
    none = rig([_table()])
    assert none.status == {EVENTS: "FAILED"} and none.parts == {EVENTS: None}
    rig.reset()
    # Only provably: parts that failed with no row counted, after a batch committed split
    # halves and then failed (the count misses those rows), keep the record.
    rig.partial_shards.update({0, 1, 2, 3})
    partial = rig([_table()])
    assert partial.status == {EVENTS: "FAILED"} and partial.parts == {EVENTS: 4}
    rig.reset()
    handle = _Handle([EVENTS])
    handle.cancelled = True                    # a Stop before any part was submitted
    stopped = rig([_table()], handle=handle)
    assert stopped.shards == [] and stopped.parts == {EVENTS: None}
    rig.reset()
    rig.fail_shards.update({0, 1, 2})          # one part wrote rows: that is enough
    some = rig([_table()])
    assert some.status == {EVENTS: "FAILED"} and some.parts == {EVENTS: 4}
    # Each failed part's record counts its range from 1, of the table's ranges.
    assert sorted(e for e in some.errors if " FAILED (rows_loaded=" in e) == [
        f"range {i} of 4 FAILED (rows_loaded=0): (no error message)" for i in (1, 2, 3)]
    rig.reset()
    # PostgreSQL records its shared-snapshot parts the same way.
    pg = rig([_table()], source=SourceType.POSTGRES, watermark=_wm(binlog=False))
    assert pg.parts == {EVENTS: 4}


def test_a_thread_path_load_clears_the_record_only_when_it_recreated_the_table(
        monkeypatch) -> None:
    # Tables in parallel 1 (or a migrator double): one reader per table, in this thread.
    class _ThreadMigrator:
        def __init__(self, inputs, fail=()):
            self._inputs = inputs
            self.fail = set(fail)

        def migrate_table(self, table, *, on_rows=None, should_cancel=None, **_k):
            if table.name in self.fail:
                raise ValueError("bad")
            return 5

    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    tables = [_table(), _table("app.kept"), _table("app.failed")]
    inputs = _inputs(tables, replace=[EVENTS, "app.failed"])
    handle = _marked(EVENTS, "app.kept", "app.failed")
    engine._migrate_tables_in_parallel(
        handle, "j", tables, _ThreadMigrator(inputs, fail=["app.failed"]), ErrorLogStore(),
        watermark=_wm())
    assert {c.chunk_id: (c.status, c.reader_shards) for c in handle.job.chunks} == {
        EVENTS: ("DONE", None),               # a finished Drop & reload: recreated
        "app.kept": ("DONE", 4),              # an append: its rows read in parts stay
        "app.failed": ("FAILED", 4),          # may not have reached its recreate: kept
    }


# ---------------------------------------------------------------------------
# 4. A MySQL shard never re-reads in place
# ---------------------------------------------------------------------------


class _Queue:
    def __init__(self):
        self.sent: list = []

    def put_nowait(self, msg):
        self.sent.append(msg)


def test_a_mysql_shard_fails_on_a_lost_source_connection_without_re_reading(monkeypatch):
    queue = _Queue()
    monkeypatch.setattr(engine, "_worker_progress_queue", queue)
    calls = {"n": 0}

    def _work():
        calls["n"] += 1
        raise _lost_connection(2013)

    with pytest.raises(engine._SplitRangeNotReread) as caught:
        engine._retry_source_drops_in_process(
            _work, cancelled=lambda: False, table_name=EVENTS, shard_index=1, reread=False,
            shard_count=4)
    assert calls["n"] == 1
    assert not [m for m in queue.sent if isinstance(m, tuple) and m[0] == engine._SOURCE_REREAD]
    message = str(caught.value)
    # The range as the operator counts it: from 1, of the table's total.
    assert message.startswith(
        "The source connection was lost while primary-key range 2 of 4 of this table was read "
        "(OperationalError: ")
    assert engine._range_label(0, None) == "1" and engine._range_label(3, 4) == "4 of 4"
    assert message.endswith(
        "A MySQL table read in parts is not re-read in place: each part reads its own "
        "snapshot, and a second snapshot in one range could copy a row twice under one of its "
        "unique keys. So the table did not finish: Retry unfinished tables loads it again.")
    # Its own words reach the parent: no connection-lost hint promising a re-read.
    inputs = _inputs([_table()])
    assert engine._worker_error_message(caught.value, inputs) == message
    # A data error is raised as it is.
    with pytest.raises(ValueError):
        engine._retry_source_drops_in_process(
            lambda: (_ for _ in ()).throw(ValueError("bad")), cancelled=lambda: False,
            table_name=EVENTS, shard_index=1, reread=False)


class _PgConnectionLost(Exception):
    sqlstate = "08006"


class _FlakyExporter:
    """A shard stream whose first read loses the source connection (as the engine says it)."""

    def __init__(self, error):
        self.reads = 0
        self.error = error

    def stream_converted_rows(self, conn, table, **kw):
        self.reads += 1
        attempt = self.reads

        def _rows():
            if attempt == 1:
                raise self.error
            yield {"id": 1, "email": "a"}

        return _rows()


def _run_shard(monkeypatch, *, shared, importer=None, exporter=None, max_quarantine=None):
    exporter = exporter or _FlakyExporter(
        _PgConnectionLost("server closed the connection") if shared
        else _lost_connection(2013))
    importer = importer or _FakeImporter()

    class _ShardMigrator(BatchedTableMigrator):  # the real class, imported before any patch
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=exporter, watermark_capturer=object(),
                importer_factory=lambda _i: importer, table_recreator=lambda t: [],
                target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
            )

    monkeypatch.setattr(engine, "BatchedTableMigrator", _ShardMigrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS", "0")
    inputs = _inputs([_table()], source=SourceType.POSTGRES if shared else SourceType.MYSQL)
    result = engine._migrate_shard_in_process(engine._ShardWorkerArgs(
        job_id="j", table=_table(), inputs=inputs, pk_lower=None, pk_upper=250,
        shard_index=0, shared_snapshot_id="snap-1" if shared else None, shard_count=4,
        max_quarantine_records=max_quarantine))
    return result, exporter


def test_the_shard_worker_fails_a_mysql_range_and_re_reads_a_shared_snapshot(monkeypatch):
    mysql, exporter = _run_shard(monkeypatch, shared=False)
    assert mysql.status == "FAILED" and exporter.reads == 1
    assert mysql.error_message.startswith(
        "The source connection was lost while primary-key range 1 of 4 of this table was read")
    assert "re-running is safe" not in mysql.error_message
    assert mysql.rows_loaded == 0                       # it failed before any batch
    assert mysql.wrote_nothing is True                  # no row ever reached its importer
    pg, exporter = _run_shard(monkeypatch, shared=True)
    assert pg.status == "DONE" and exporter.reads == 2
    assert pg.wrote_nothing is False


def test_a_parts_result_counts_the_rows_it_wrote_when_it_fails_or_stops(monkeypatch):
    # The parent judges by these whether a split table's parts wrote any row at all.
    from dsql_migrator.core.batched_import import BatchedImportResult

    class _OneBatchThen:
        """Reads a row, commits one batch of 3 rows, then loses the source connection or
        stops."""

        def __init__(self, end):
            self.end = end

        def import_rows(self, rows, table, *, on_batch_loaded=None, **_k):
            next(iter(rows))
            on_batch_loaded(3, 0)
            if self.end == "stop":
                return BatchedImportResult(rows_loaded=3, cancelled=True)
            raise _lost_connection(2013)

    class _Exporter:
        def stream_converted_rows(self, conn, table, **kw):
            return iter([{"id": 1, "email": "a"}])

    failed, _ = _run_shard(monkeypatch, shared=False, importer=_OneBatchThen("lost"),
                           exporter=_Exporter())
    assert failed.status == "FAILED" and failed.rows_loaded == 3
    assert failed.wrote_nothing is False
    assert failed.error_message.startswith("The source connection was lost while primary-key "
                                           "range 1 of 4")
    stopped, _ = _run_shard(monkeypatch, shared=False, importer=_OneBatchThen("stop"),
                            exporter=_Exporter())
    assert stopped.status == "STOPPED" and stopped.rows_loaded == 3
    assert stopped.wrote_nothing is False


class _Poison(Exception):
    sqlstate = "54000"          # a permanent per-row error, as a value over 1 MiB is


class _PoisonTarget:
    """An Aurora DSQL double for the real importer: commits a batch unless it holds a
    poison row, which it rejects with a permanent per-row error."""

    def __init__(self):
        self.committed: set = set()

    def connect(self):
        target = self

        class _Cursor:
            rowcount = -1

            def execute(self, _statement, params=None):
                params = list(params or [])
                rows = [params[i:i + 2] for i in range(0, len(params), 2)]
                if any(r[1] == "POISON" for r in rows):
                    raise _Poison("value too long")
                target.committed.update(r[0] for r in rows)
                self.rowcount = len(rows)

            def fetchall(self):
                return []

            def close(self):
                return None

        class _Conn:
            def cursor(self):
                return _Cursor()

            def close(self):
                return None

        return _Conn()


def test_a_part_that_committed_rows_inside_a_failed_batch_did_not_write_nothing(monkeypatch):
    # FAILS WITHOUT THE FIX: every second row poisoned, so the first batch binary-splits,
    # commits its good halves and then hits the part's quarantine cap mid-batch: every batch is
    # FAILED and the part reports 0 rows, though rows are on Aurora DSQL -- the parent restored
    # the split table's earlier (empty) record from that count. (One batch in flight, so the
    # cap falls inside the first batch every time.)
    from dsql_migrator.core.batched_import import BatchedImporter, BatchedImportOptions

    class _Exporter:
        def stream_converted_rows(self, conn, table, **kw):
            return iter([{"id": i, "email": "POISON" if i % 2 == 0 else f"u{i}"}
                         for i in range(1, 2001)])

    target = _PoisonTarget()
    importer = BatchedImporter(BatchedImportOptions(batch_size=500, parallelism=1),
                               connection_factory=target.connect, sleep=lambda _s: None,
                               jitter=lambda: 0.0)
    result, _ = _run_shard(monkeypatch, shared=False, importer=importer, exporter=_Exporter(),
                           max_quarantine=100)
    assert result.status == "FAILED" and result.rows_loaded == 0
    assert len(target.committed) == 100                  # ...yet its rows are there
    assert result.wrote_nothing is False                 # so the parent keeps the record


def test_the_shard_worker_passes_reread_only_with_a_shared_snapshot() -> None:
    src = inspect.getsource(engine._migrate_shard_in_process)
    call = src[src.index("_retry_source_drops_in_process("):]
    assert "reread=bool(args.shared_snapshot_id)" in call[: call.index("\n        )\n")]


# ---------------------------------------------------------------------------
# 5. The recorded split: ChunkState.reader_shards
# ---------------------------------------------------------------------------


def test_reader_shards_round_trips_and_is_left_out_while_unset() -> None:
    from pydantic import BaseModel, ConfigDict

    split = ChunkState(chunk_id=EVENTS, status="DONE", reader_shards=4)
    assert json.loads(split.model_dump_json())["reader_shards"] == 4
    assert ChunkState.model_validate_json(split.model_dump_json()).reader_shards == 4
    whole = ChunkState(chunk_id=EVENTS, status="DONE")
    assert "reader_shards" not in whole.model_dump() and whole.reader_shards is None
    # An older snapshot (no field) loads; an older build (extra="forbid", no field) still loads
    # a one-reader chunk this build saved.
    assert ChunkState.model_validate({"chunk_id": EVENTS}).reader_shards is None

    class _OlderChunk(BaseModel):
        model_config = ConfigDict(extra="forbid")
        chunk_id: str
        status: str = "PENDING"
        rows_loaded: int = 0
        rows_skipped: int = 0
        rows_quarantined: int = 0
        attempts: int = 0
        started_at: object = None
        finished_at: object = None

    assert _OlderChunk.model_validate_json(whole.model_dump_json()).chunk_id == EVENTS
    job = MigrationJob(job_id="j", chunks=[split, whole])
    assert MigrationJob.model_validate_json(job.model_dump_json()).chunks[0].reader_shards == 4


def test_a_retry_seeds_every_table_with_its_record() -> None:
    # FAILS WITHOUT THE FIX: a retried table was re-seeded without its parts, whatever the
    # retry then did -- its rows read in parts stay until a load recreates the table.
    prior = [ChunkState(chunk_id="a", status="FAILED", reader_shards=4),
             ChunkState(chunk_id="b", status="DONE", reader_shards=4),
             ChunkState(chunk_id="c", status="FAILED")]
    job = MigrationJob(job_id="j2")
    engine._seed_retry_chunks(job, prior, {"a", "c", "new"})
    assert {c.chunk_id: (c.status, c.reader_shards) for c in job.chunks} == {
        "a": ("PENDING", 4), "b": ("DONE", 4), "c": ("PENDING", None), "new": ("PENDING", None)}


def test_record_reader_shards_takes_the_plan_only_for_a_recreated_table() -> None:
    job = MigrationJob(job_id="j", chunks=[ChunkState(chunk_id="a", reader_shards=4),
                                           ChunkState(chunk_id="b"),
                                           ChunkState(chunk_id="c", reader_shards=2),
                                           ChunkState(chunk_id="d", reader_shards=4),
                                           ChunkState(chunk_id="e", reader_shards=4)])
    without_plan = engine._record_reader_shards(
        job, ["a", "b", "d", "e"], {"b": 3, "d": 2, "e": 2}, recreated=["d", "a"])
    # a: recreated, one reader -> cleared; b: planned 3; c: not this run's; d: recreated,
    # planned 2; e: kept rows, planned 2 -> the larger 4.
    assert [c.reader_shards for c in job.chunks] == [None, 3, 2, 2, 4]
    assert without_plan == {"a": None, "b": None, "d": None, "e": 4}
    engine._restore_unwritten_parts(job, {"b": None, "e": 4})
    assert [c.reader_shards for c in job.chunks] == [None, None, 2, 2, 4]


def _split_job(*, binlog=True, status="DONE"):
    return MigrationJob(
        job_id="job-1", status=status,
        chunks=[ChunkState(chunk_id=EVENTS, status=status, attempts=1, reader_shards=4),
                ChunkState(chunk_id="app.users", status="DONE", attempts=1)],
        watermark=_wm(binlog=binlog, counts={EVENTS: 1_000_000, "app.users": 5}),
    )


def test_the_full_load_table_captions_a_table_read_in_parts() -> None:
    from dsql_migrator.ui.data_migration._full_load_ui import (
        _reader_shards_caption,
        _reader_shards_tooltip,
        _render_full_load_progress,
    )

    rows = build_full_load_table_rows(_split_job())
    by = {r.table: r for r in rows}
    assert by[EVENTS].reader_shards == 4 and by["app.users"].reader_shards is None
    assert _reader_shards_caption(by[EVENTS]) == "read in 4 parts"
    assert _reader_shards_caption(by["app.users"]) == ""
    mysql_tip = _reader_shards_tooltip(by[EVENTS], own_snapshots=True)
    assert "each from its own snapshot" in mysql_tip and "Reload specific tables" in mysql_tip
    assert _reader_shards_tooltip(by[EVENTS], own_snapshots=False) == (
        "The Full Load read this table in 4 primary-key ranges sharing one snapshot (Reader "
        "shards per large table in Settings).")

    class _Ui(_RecordingUi):
        def __init__(self):
            super().__init__()
            self.slots: list = []

        def table(self, *a, rows=None, columns=None, **k):
            el = super().table(*a, rows=rows, columns=columns, **k)
            el.add_slot = lambda name, template=None: (self.slots.append((name, template)), el)[1]
            return el

        def linear_progress(self, *a, **k):
            return self._El(self)

    ui = _Ui()
    _render_full_load_progress(ui, _split_job(), rows)
    (table,) = ui.tables
    parts = {row["table"]: row["parts"] for row in table["rows"]}
    assert parts == {EVENTS: "read in 4 parts", "app.users": ""}
    tips = {row["table"]: row["parts_tooltip"] for row in table["rows"]}
    assert "each from its own snapshot" in tips[EVENTS]
    (slot,) = [t for n, t in ui.slots if n == "body-cell-table"]
    assert "props.row.parts" in slot and 'name="call_split"' in slot


# ---------------------------------------------------------------------------
# 6. Every route out of CDC's replay says so and stops
# ---------------------------------------------------------------------------


def test_split_read_tables_names_only_a_mysql_records_split_tables() -> None:
    assert split_read_tables(_split_job()) == [EVENTS]
    assert split_read_tables(_split_job(binlog=False)) == []        # PostgreSQL: one snapshot
    assert split_read_tables(None) == []


def test_the_refusal_names_the_tables_and_the_route() -> None:
    text = split_read_refusal([EVENTS], operation="Unticking it", then="untick it",
                              job=_split_job())
    assert text == (
        "The Full Load read app.events in parts, each part from its own snapshot (Reader "
        "shards per large table), so its copy on Aurora DSQL is consistent only once CDC "
        "replays it from the start point taken before the load. Unticking it would leave it "
        "as loaded: a row whose key changed during the read copied twice or not at all, a "
        "transaction across two parts half applied. Set Reader shards per large table to 1 in "
        "Settings and reload it (Reload specific tables, which drops and recreates it), then "
        "untick it. Or, once CDC has replayed it, run Validation in CHECKSUM mode (Fast sweep "
        "off): a match proves the copy and ends this refusal.")
    two = split_read_refusal(["a", "b"], operation="Unticking them", then="untick them")
    assert "their copy" in two and "reload them" in two
    assert two.endswith("once CDC has replayed them, run Validation in CHECKSUM mode (Fast "
                        "sweep off): a match proves the copy and ends this refusal.")


def test_the_route_names_the_control_the_full_load_step_shows() -> None:
    # FAILS WITHOUT THE FIX: every route said "Reload specific tables", which the Full Load
    # step does not show while a table is unfinished -- only Retry unfinished tables.
    from dsql_migrator.ui.data_migration._models import split_reload_route

    # FAILS WITHOUT THE FIX (round 2): "choosing Drop & reload" -- with one reader the dialog
    # drops and recreates a table read in parts by itself, and the only Drop & reload choice
    # it shows is for the OTHER tables.
    head = "set Reader shards per large table to 1 in Settings and reload"
    assert split_reload_route([EVENTS], _split_job()) == (
        f"{head} it (Reload specific tables, which drops and recreates it)")
    assert split_reload_route([EVENTS], _split_job(status="FAILED")) == (
        f"{head} it (Retry unfinished tables, which drops and recreates it)")
    other_open = _split_job()
    other_open.chunks[1].status = "FAILED"          # app.users did not finish
    assert split_reload_route([EVENTS], other_open) == (
        f"{head} it (once no table is left unfinished: Reload specific tables, which drops "
        "and recreates it)")
    both = MigrationJob(job_id="job-1", status="FAILED", watermark=_wm(),
                        chunks=[ChunkState(chunk_id="app.a", status="FAILED", reader_shards=4),
                                ChunkState(chunk_id="app.b", status="DONE", reader_shards=4)])
    assert split_reload_route(["app.a", "app.b"], both) == (
        f"{head} them (Retry unfinished tables for app.a, then Reload specific tables for "
        "app.b, which drop and recreate them)")
    # CDC only has no Full Load step: through Full load + CDC (Full load only refuses them).
    assert split_reload_route([EVENTS], _split_job(), cdc_only=True) == (
        "switch the migration type to Full load + CDC, set Reader shards per large table to 1 "
        "in Settings and reload it on its Full Load step (Reload specific tables, which drops "
        "and recreates it)")
    # Every surface words it so: the tree's refusal, the Full load only refusal, the lock tip.
    from dsql_migrator.ui.data_migration import (
        LOCK_TIP_SPLIT,
        full_load_only_switch_refusal,
        table_set_add_lock,
    )

    from tests.test_g12_table_set_after_load import _Jobs

    failed = _split_job(status="FAILED")
    jobs = _Jobs({"job-1": failed})
    untick = table_set_add_lock(_fc_state(), jobs).removal_refusal([EVENTS], tree=True)
    assert ("reload it (Retry unfinished tables, which drops and recreates it), then untick "
            "it.") in untick
    switch = full_load_only_switch_refusal(_fc_state(), jobs, failed)
    assert ("reload it (Retry unfinished tables, which drops and recreates it), then switch the "
            "migration type.") in switch
    cdc_only = _fc_state()
    cdc_only.migration_type = MigrationType.CDC_ONLY
    assert "half applied. Switch the migration type to Full load + CDC, set Reader shards" in (
        full_load_only_switch_refusal(cdc_only, jobs, failed))
    assert "Retry unfinished tables while it is unfinished, else Reload specific tables" in (
        LOCK_TIP_SPLIT)


def test_the_caption_tooltip_names_the_control_for_its_row() -> None:
    from dsql_migrator.ui.data_migration._full_load_ui import _reader_shards_tooltip

    def _tip(status, others_open=False):
        (row,) = [r for r in build_full_load_table_rows(_split_job(status=status))
                  if r.table == EVENTS]
        return _reader_shards_tooltip(row, own_snapshots=True, others_open=others_open)

    assert "reload it (Reload specific tables, which drops and recreates it) first." in (
        _tip("DONE"))
    assert "reload it (Retry unfinished tables, which drops and recreates it) first." in (
        _tip("FAILED"))
    assert ("reload it (once no table is left unfinished: Reload specific tables, which drops "
            "and recreates it) first.") in _tip("DONE", others_open=True)
    # The other end, a CHECKSUM match, is named for Full load only and a later start point;
    # an untick has only the reload, before CDC starts (round 4).
    assert ("Until a match in Validation in CHECKSUM mode (Fast sweep off) proves that replay, "
            "switching to Full load only and starting CDC from a later start point are refused. "
            "Unticking it is refused until it is reloaded. Once CDC starts, the table set is "
            "fixed (Start over to change it).") in _tip("DONE")


def _fc_state(selected=(EVENTS, "app.users")):
    state = DataMigrationState()
    state.migration_type = FLC
    state.job_id = "job-1"
    state.selection = TableSelection(selected_tables=list(selected))
    state.selection_touched = True
    state.set_cdc_stack_phase("absent", status=None)
    return state


def test_the_add_lock_holds_a_split_table_selected() -> None:
    from dsql_migrator.ui.data_migration import TableSetAddLock, table_set_add_lock

    from tests.test_g12_table_set_after_load import _Jobs

    lock = table_set_add_lock(_fc_state(), _Jobs({"job-1": _split_job()}))
    assert lock.split_held == frozenset({EVENTS})
    assert lock.removal_blocked(["app.users", EVENTS, EVENTS]) == [EVENTS]
    assert lock.removal_refusal(["app.users"], tree=True) is None
    # Only the reload, then that the set is fixed once CDC starts (round 4: no CHECKSUM end).
    assert lock.removal_refusal([EVENTS], tree=True).endswith(
        "then untick it. Once CDC starts, the table set is fixed (Start over to change it).")
    assert lock.removal_refusal([EVENTS], tree=False).endswith(
        "then unselect it. Once CDC starts, the table set is fixed (Start over to change it).")
    # Its ticked leaf is flagged (locked, with the reason); an unticked one never is.
    assert lock.node_flags([EVENTS, "app.users"], [EVENTS, "app.users"]) == {EVENTS: "split"}
    assert lock.node_flags([EVENTS], []) == {}
    assert hash(lock) == hash(TableSetAddLock(
        covered=lock.covered, rule_held=lock.rule_held, split_held=frozenset({EVENTS})))
    # A PostgreSQL record holds nothing.
    pg = table_set_add_lock(_fc_state(), _Jobs({"job-1": _split_job(binlog=False)}))
    assert pg.split_held == frozenset()


def _tree(state, jobs, listed):
    from dsql_migrator.ui.data_migration import _render_table_selection, table_set_add_lock

    from tests.test_g16_full_load_and_cdc_narrowing import _TreeUi
    from tests.test_g12_table_set_after_load import _inv

    ui = _TreeUi()
    changes, refreshed = [], []
    _render_table_selection(
        ui, _inv(*listed), state, listed, target_existing=listed, default_selection=listed,
        lock_check=lambda: None, on_locked=lambda: refreshed.append(1),
        on_selection_change=changes.append,
        add_lock_fn=lambda: table_set_add_lock(state, jobs),
    )
    return ui, changes, refreshed


def test_the_tree_refuses_to_untick_a_split_table_and_says_why() -> None:
    from dsql_migrator.ui.data_migration import LOCK_TIP_SPLIT

    from tests.test_g12_table_set_after_load import _Jobs

    state = _fc_state()
    jobs = _Jobs({"job-1": _split_job()})
    listed = [EVENTS, "app.users"]
    ui, changes, refreshed = _tree(state, jobs, listed)
    leaves = {leaf["id"]: leaf for leaf in ui.nodes[0][0]["children"][0]["children"]}
    assert leaves[f"table:{EVENTS}"]["tickable"] is False
    assert leaves[f"table:{EVENTS}"]["add_locked"] == "split"
    (template,) = [t for name, t in ui.slots if name == "header-table"]
    assert "props.node.add_locked === 'split'" in template and LOCK_TIP_SPLIT in template
    (on_tick,) = ui.trees
    on_tick(SimpleNamespace(value=["table:app.users"]))          # untick app.events
    ((note, tone),) = ui.notes
    assert tone == "negative" and note.startswith("The Full Load read app.events in parts")
    assert "then untick it. " in note
    assert note.endswith("the table set is fixed (Start over to change it). This change was not "
                         "applied.")
    assert state.selection.selected_tables == [EVENTS, "app.users"] and refreshed == [1]
    on_tick(SimpleNamespace(value=[f"table:{EVENTS}"]))           # untick app.users: free
    assert state.selection.selected_tables == [EVENTS] and changes == [[EVENTS]]
    # Unselect all keeps the split table and says so.
    ui.clicks["Unselect all"]()
    assert ui.unticks[-1] == ["table:app.users"]
    assert state.selection.selected_tables == [EVENTS]
    note, tone = ui.notes[-1]
    assert tone == "negative" and "It stays selected; every other table was unselected." in note
    assert "CHECKSUM" not in note and "the table set is fixed (Start over to change it)." in note


def test_the_tree_hint_names_the_lock_on_a_split_table() -> None:
    from dsql_migrator.ui.data_migration import TREE_HINT_FL_CDC, TREE_HINT_SPLIT, _table_tree_hint

    from tests.test_g12_table_set_after_load import _Jobs

    jobs = _Jobs({"job-1": _split_job()})
    assert _table_tree_hint(_fc_state(), jobs, job=_split_job(), locked=False,
                            migration_type=FLC) == TREE_HINT_FL_CDC + TREE_HINT_SPLIT
    whole = _split_job()
    whole.chunks[0].reader_shards = None
    assert _table_tree_hint(_fc_state(), jobs, job=whole, locked=False,
                            migration_type=FLC) == TREE_HINT_FL_CDC
    # The reload that lifts the lock is a Drop & reload, as the refusals and the manual say.
    assert "reloaded with one reader (Reader shards per large table set to 1, then Drop & " \
           "reload)" in TREE_HINT_SPLIT


def test_a_table_rule_that_unselects_a_split_table_is_an_e3() -> None:
    from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules

    tables = [_table(), _table("app.users")]

    def _lock(names):
        blocked = [n for n in names if n == EVENTS]
        return (blocked, "Kept: read in parts.") if blocked else None

    refused = resolve_exclusion_rules(
        "exclude table app.events", migratable=tables, source_type=SourceType.MYSQL,
        effective_selection=[EVENTS, "app.users"], unselect_lock=_lock)
    ((line, errors),) = refused.errors.items()
    assert line == 1 and [e.code for e in errors] == ["E3"]
    assert errors[0].message == "This rule would unselect app.events. Kept: read in parts."
    free = resolve_exclusion_rules(
        "exclude table app.users", migratable=tables, source_type=SourceType.MYSQL,
        effective_selection=[EVENTS, "app.users"], unselect_lock=_lock)
    assert free.errors == {} and free.tables_to_unselect == ("app.users",)


def test_the_change_and_reload_popup_refuses_unselecting_a_split_table() -> None:
    from dsql_migrator.ui.data_migration import TableSetAddLock
    from dsql_migrator.ui.data_migration._models import ExclusionInputs, popup_resolution

    state = _fc_state()
    inv = SourceInventory(tables=[_table(), _table("app.users")])
    inputs = ExclusionInputs(selected=[EVENTS, "app.users"], migratable=[EVENTS, "app.users"],
                             applied={}, live_columns=None, view_ddls={},
                             preserve_foreign_keys=True, signature=())
    lock = TableSetAddLock(covered=frozenset({EVENTS, "app.users"}),
                           split_held=frozenset({EVENTS}))
    resolution = popup_resolution(state, inv, "exclude table app.events", inputs=inputs,
                                  source_type=SourceType.MYSQL, add_lock=lock)
    ((_line, errors),) = resolution.errors.items()
    assert errors[0].code == "E3" and errors[0].message.startswith(
        "This rule would unselect app.events. The Full Load read app.events in parts")
    assert "then unselect it. " in errors[0].message
    # Round 4: the reload only (no CHECKSUM end), and the set is fixed once CDC starts.
    assert "CHECKSUM" not in errors[0].message and errors[0].message.endswith(
        "then unselect it. Once CDC starts, the table set is fixed (Start over to change it).")


def test_full_load_only_is_refused_while_a_split_table_waits_for_cdc() -> None:
    from dsql_migrator.ui.data_migration import full_load_only_switch_refusal

    from tests.test_g12_table_set_after_load import _Jobs

    jobs = _Jobs({"job-1": _split_job()})
    state = _fc_state()
    refusal = full_load_only_switch_refusal(state, jobs, _split_job())
    assert refusal == split_read_refusal(
        [EVENTS], operation="Switching to Full load only", then="switch the migration type")
    assert full_load_only_switch_refusal(state, jobs, _split_job(binlog=False)) is None
    assert full_load_only_switch_refusal(state, jobs, None) is None
    flo = _fc_state()
    flo.migration_type = FLO
    assert full_load_only_switch_refusal(flo, jobs, _split_job()) is None
    # FAILS WITHOUT THE FIX (round 2): a submitted Start CDC ended the refusal -- also one that
    # never streamed. Only the record decides: a Start, or a stream, leaves the mark.
    started = _fc_state()
    started.set_cdc_ops_window_start(_TS)
    assert full_load_only_switch_refusal(started, jobs, _split_job()) == refusal
    streamed = _fc_state()
    streamed.set_cdc_ops_window_start(_TS)
    streamed.set_cdc_has_committed_offset(True)
    assert full_load_only_switch_refusal(streamed, jobs, _split_job()) == refusal
    proven = _split_job()
    proven.chunks[0].reader_shards = None          # a CHECKSUM match or a recreate cleared it
    assert full_load_only_switch_refusal(started, jobs, proven) is None


def test_the_type_selector_refuses_the_switch_and_marks_the_tile() -> None:
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import _render_migration_type_selector

    class _Ui(_RecordingUi):
        def __init__(self):
            super().__init__()
            self.notes: list = []
            self.clicks: list = []

        def notify(self, text="", *_a, **k):
            self.notes.append((str(text), k.get("type")))

        def card(self, *a, **k):
            el = super().card(*a, **k)
            el.on = lambda _evt, handler, *_a, **_k: (self.clicks.append(handler), el)[1]
            return el

    state = _fc_state()
    ui = _Ui()
    _render_migration_type_selector(
        ui, state, status=StepStatus.DONE, refresh=lambda: None, locked=False,
        type_refusals={FLO: "Refused: read in parts."})
    assert "Not available now: tables the Full Load read in parts need CDC (click for the " \
           "route)." in ui.texts
    assert "Refused: read in parts." in ui.tooltips
    # FAILS WITHOUT THE FIX: the tile looked disabled (muted, a not-allowed cursor) while its
    # line asked for the click, and the line was amber while the click's refusal is an error.
    assert not any("cursor-not-allowed" in c for c in ui.classes_applied)
    assert "text-xs text-red-700" in ui.classes_applied          # the lock line: error tone
    assert any(c.startswith("text-red-600") for c in ui.classes_applied)     # its icon
    assert not any("amber" in c for c in ui.classes_applied)
    ui.clicks[0]()                                   # Full load only
    assert state.migration_type is FLC
    assert ui.notes == [("Refused: read in parts. The migration type was not changed.",
                         "negative")]
    ui.clicks[1]()                                   # CDC only stays free
    assert state.migration_type is MigrationType.CDC_ONLY


def test_the_screen_marks_full_load_only_and_withholds_the_untick_hint_for_a_split_table(
        monkeypatch) -> None:
    # The real Data Migration screen after a failed Full load + CDC load whose failed table
    # was read in parts: the Continue hint does not offer the untick its lock refuses, and the
    # Full load only tile says why it is not available.
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import CONTINUE_HINT_FL_CDC
    from dsql_migrator.ui.workflow import WorkflowStep, with_status

    from tests.test_g16_full_load_and_cdc_narrowing import _fc_screen

    def _render(split):
        job = MigrationJob(
            job_id="job-1", status="FAILED",
            chunks=[ChunkState(chunk_id="app.a", status="DONE", attempts=1),
                    ChunkState(chunk_id="app.b", status="FAILED", attempts=1,
                               reader_shards=4 if split else None)],
            watermark=_wm(counts={"app.a": 1, "app.b": 1}),
        )
        s = _fc_screen(monkeypatch, job=job, selected=["app.a", "app.b"], render=False)
        s.session.set_workflow(
            with_status(s.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
        s.content(lambda: None)
        return s

    tile = "Not available now: tables the Full Load read in parts need CDC (click for the route)."
    plain = _render(False)
    assert any(t.endswith(CONTINUE_HINT_FL_CDC) for t in plain.ui.texts)
    assert tile not in plain.ui.texts
    split = _render(True)
    assert not any(t.endswith(CONTINUE_HINT_FL_CDC) for t in split.ui.texts)
    assert any(t.startswith("Retry the failed tables before starting CDC")
               for t in split.ui.texts)
    assert tile in split.ui.texts
    # The route names a control this screen shows (FAILS WITHOUT THE FIX: "Reload specific
    # tables", which the step offers only once every table has finished).
    labels = [getattr(b, "text", "") for b in split.ui.buttons]
    assert "Retry unfinished tables (1)" in labels
    assert not any(str(label).startswith("Reload specific tables") for label in labels)
    (route,) = [t for t in split.ui.tooltips if t.startswith("The Full Load read app.b in parts")]
    assert "reload it (Retry unfinished tables, which drops and recreates it), then switch" in (
        route)


def _start_state(*, manual=None):
    from dsql_migrator.core.models import MigrationMode

    state = _fc_state()
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    if manual is not None:
        state.set_cdc_start_mode("manual")
        state.set_cdc_start_position(binlog_file=manual[0], binlog_pos=manual[1])
    return state


def _seed(state, job):
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_seed, _cdc_watermark

    return _cdc_start_seed(state, _cdc_watermark(job, state), SourceType.MYSQL)


def test_a_manual_start_point_later_than_the_full_load_is_refused() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import split_start_point_refusal

    job = _split_job()
    later = _start_state(manual=("mysql-bin.000012", 4))
    seed, origin = _seed(later, job)
    refusal = split_start_point_refusal(later, job, seed, SourceType.MYSQL, origin=origin)
    assert refusal == split_read_refusal(
        [EVENTS],
        operation="Starting CDC from mysql-bin.000012:4, later than the Full Load's start point "
        "(mysql-bin.000009:4),",
        then="start CDC from that point",
        job=job,
        alternative=(
            "Set the CDC start point back to Automatic (the Full Load's start point), or "
            "enter a Manual point at or before mysql-bin.000009:4: CDC then replays every "
            "read, with no reload."
        ),
    )
    # The no-cost way first, then the reload, then the CHECKSUM match.
    assert refusal.endswith(
        "half applied. Set the CDC start point back to Automatic (the Full Load's start point), "
        "or enter a Manual point at or before mysql-bin.000009:4: CDC then replays every read, "
        "with no reload. Or set Reader shards per large table to 1 in Settings and reload it "
        "(Reload specific tables, which drops and recreates it), then start CDC from that "
        "point. Or, once CDC has replayed it, run Validation in CHECKSUM mode (Fast sweep off): "
        "a match proves the copy and ends this refusal.")
    # FAILS WITHOUT THE FIX (round 2): a submitted Start CDC ended the refusal -- one the worker
    # refused or that never streamed too -- and a later point then left the tear. Only the
    # record decides (a CHECKSUM match or a recreate clears it).
    from tests.test_g12_table_set_after_load import _Jobs

    started = _start_state(manual=("mysql-bin.000012", 4))
    started.set_cdc_ops_window_start(_TS)
    seed, origin = _seed(started, job)
    assert split_start_point_refusal(started, job, seed, SourceType.MYSQL, origin=origin,
                                     job_manager=_Jobs({"job-1": job})) == refusal
    proven = _split_job()
    proven.chunks[0].reader_shards = None
    assert split_start_point_refusal(started, proven, seed, SourceType.MYSQL, origin=origin,
                                     job_manager=_Jobs({"job-1": proven})) is None
    # At or before the Full Load's start point: every read is replayed.
    for point in (("mysql-bin.000009", 4), ("mysql-bin.000008", 900)):
        earlier = _start_state(manual=point)
        seed, origin = _seed(earlier, job)
        assert split_start_point_refusal(earlier, job, seed, SourceType.MYSQL,
                                         origin=origin) is None
    # Automatic, a resume, PostgreSQL, or no split table captured: nothing to refuse.
    auto = _start_state()
    seed, origin = _seed(auto, job)
    assert split_start_point_refusal(auto, job, seed, SourceType.MYSQL, origin=origin) is None
    seed, origin = _seed(later, job)
    assert split_start_point_refusal(later, job, seed, SourceType.MYSQL, origin=origin,
                                     resumes=True) is None
    assert split_start_point_refusal(later, job, seed, SourceType.POSTGRES,
                                     origin=origin) is None
    unticked = _start_state(manual=("mysql-bin.000012", 4))
    unticked.selection = TableSelection(selected_tables=["app.users"])
    seed, origin = _seed(unticked, job)
    assert split_start_point_refusal(unticked, job, seed, SourceType.MYSQL,
                                     origin=origin) is None


def test_start_cdc_shows_the_refusal_disables_the_button_and_its_backstop_refuses(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_g12_table_set_after_load import _Jobs

    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda ui, **kw: notices.append(
        (kw.get("tone"), kw.get("header"), kw.get("body"))) or real(ui, **kw))
    inv = SourceInventory(tables=[_table(), _table("app.users")])
    state = _start_state(manual=("mysql-bin.000012", 4))
    jobs = _Jobs({"job-1": _split_job()})
    ui = _RecordingUi()
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL),
                              restored_source_type=None)
    _cdc_ui._render_cdc_start_button(ui, state, jobs, lambda: None, inventory=inv,
                                     session=session)
    refusal = [n for n in notices if n[1] == SPLIT_READ_HEADER]
    assert refusal and refusal[0][0] == "error"
    assert refusal[0][2].startswith("The Full Load read app.events in parts")
    assert refusal[0][2] in ui.tooltips                # the disabled Start's tooltip
    notices.clear()
    _cdc_ui._start_cdc_deploy(ui, state, jobs, lambda: None, inventory=inv, session=session)
    assert [n[0] for n in notices if n[1] == SPLIT_READ_HEADER] == ["error"]
    assert jobs.submitted == []


def test_the_purged_binlog_remedy_names_no_manual_route_for_split_tables() -> None:
    from dsql_migrator.core.watermark import binlog_resume_gap_reason

    plain = binlog_resume_gap_reason("mysql-bin.000001", ["mysql-bin.000005"])
    assert plain.endswith("Or start CDC from a manual position and accept the gap.")
    route = "set Reader shards per large table to 1 in Settings and reload it (Retry X)"
    split = binlog_resume_gap_reason("mysql-bin.000001", ["mysql-bin.000005"],
                                     split_tables=[EVENTS], split_route=route)
    assert "Or start CDC from a manual position and accept the gap." not in split
    assert split.endswith(
        "A manual position is no way forward for app.events: the Full Load read it in parts, "
        "each from its own snapshot, and only CDC from its watermark makes that copy "
        "consistent, so Start CDC refuses a later start point while it is captured. To start "
        f"from a manual position and accept the gap, first {route}.")
    generic = binlog_resume_gap_reason("mysql-bin.000001", ["mysql-bin.000005"],
                                       split_tables=[EVENTS])
    assert generic.endswith("first set Reader shards per large table to 1 in Settings and "
                            "reload it with Drop & reload.")
    # A purged Manual point: Fetch current position's point is refused too while the table is
    # captured, so the remedy names the ways that remain (FAILS WITHOUT THE FIX).
    manual = binlog_resume_gap_reason("mysql-bin.000001", ["mysql-bin.000005"], manual=True,
                                      split_tables=[EVENTS], split_route=route)
    assert "no way forward" not in manual
    assert manual.endswith(
        "changes before that position are not replicated. While CDC captures app.events, which "
        "the Full Load read in parts, Start CDC refuses a point later than the Full Load's "
        "start point, as a fetched one is: choose Automatic (the Full Load's start point) or a "
        f"retained position no later than it, or first {route}.")
    assert binlog_resume_gap_reason("mysql-bin.000001", ["mysql-bin.000005"],
                                    manual=True).endswith("are not replicated.")


def test_the_load_then_untick_route_carves_out_tables_read_in_parts(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._models import (
        PRE_LOAD_SPLIT_CARVE_OUT,
        cdc_pre_load_table_list_routes,
        mysql_reads_may_split,
    )

    plain = cdc_pre_load_table_list_routes(None)
    assert PRE_LOAD_SPLIT_CARVE_OUT not in plain
    carved = cdc_pre_load_table_list_routes(None, split=True)
    assert carved == plain.replace(" Or capture fewer", PRE_LOAD_SPLIT_CARVE_OUT + " Or capture fewer")
    assert "such a table cannot be unticked until it is reloaded with one reader" in carved
    assert ("Nor can it be taken to Full load only until that reload, or a match in Validation "
            "in CHECKSUM mode once CDC has replayed it.") in carved
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    assert mysql_reads_may_split(SourceType.MYSQL) is True
    assert mysql_reads_may_split(SourceType.POSTGRES) is False
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    assert mysql_reads_may_split(SourceType.MYSQL) is False     # one slot never splits
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
    assert mysql_reads_may_split(SourceType.MYSQL) is False


def test_the_full_load_card_and_the_early_deploy_carry_the_carve_out(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import pre_load_table_list_warning
    from dsql_migrator.ui.data_migration._cdc_ui import _deploy_block_notice
    from dsql_migrator.ui.data_migration._models import PRE_LOAD_SPLIT_CARVE_OUT

    from tests.test_g12_table_set_after_load import _Jobs
    from tests.test_g128_deploy_review import _band, _inventory, _state
    from tests.test_g131_table_list_warning import _deploy_ready

    monkeypatch.delenv("DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS", raising=False)
    names = _band(300)
    inv = _inventory(names)
    mysql = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL),
                            restored_source_type=None)
    _header, before = pre_load_table_list_warning(_state(names), _Jobs({}), mysql, inv)
    assert PRE_LOAD_SPLIT_CARVE_OUT not in before            # Reader shards 1 (default)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    _header, card = pre_load_table_list_warning(_state(names), _Jobs({}), mysql, inv)
    assert PRE_LOAD_SPLIT_CARVE_OUT in card
    _header, deploy = _deploy_block_notice(_deploy_ready(names, FLC), _Jobs({}), inv, mysql)
    assert PRE_LOAD_SPLIT_CARVE_OUT in deploy
    pg = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES),
                         restored_source_type=None)
    _header, pg_card = pre_load_table_list_warning(_state(names), _Jobs({}), pg, inv)
    assert PRE_LOAD_SPLIT_CARVE_OUT not in pg_card


def _split_record(names, *, split, status="DONE"):
    """A MySQL Full load + CDC record over ``names`` whose table ``split`` was read in parts
    (and ended ``status``); the others finished."""
    return MigrationJob(
        job_id="job-1", status=status, watermark=_wm(counts={n: 10 for n in names}),
        chunks=[ChunkState(chunk_id=n, status=status if n == split else "DONE", attempts=1,
                           reader_shards=4 if n == split else None) for n in names],
    )


_SPLIT_FORCED = (
    "app.b: read in parts by the Full Load, each part from its own snapshot, so its copy waits "
    "for CDC's replay from the Full Load's start point: an append would keep any rows read in "
    "parts as they are (CDC from a re-run's new start point never replays the changes before "
    "it, and after a reload with one reader the table stays marked), so it is dropped and "
    "reloaded.")


def test_a_retry_with_one_reader_drops_and_reloads_a_split_table(monkeypatch):
    # FAILS WITHOUT THE FIX: the dialog offered (and preselected) Append, which keeps the rows
    # read in parts -- the reload the route asks for did not take the table out of the replay.
    from tests.test_g17_full_load_and_cdc_additions import _open_retry_dialog
    from tests.test_start_dialog_overlay import _body, _confirm

    names = ["app.a", "app.b"]
    state = _fc_state(selected=names)
    job = _split_record(names, split="app.b", status="FAILED")
    ui, picked = _open_retry_dialog(monkeypatch, state=state, job=job, rows=("app.b",),
                                    tables=names)
    assert state.exclusion_forced_replace.keys() == {"app.b"}
    assert ui.start().text == "Drop 1 table, recreate and load" and ui.radios == []
    assert _SPLIT_FORCED in _body(ui)
    _confirm(ui)
    ((retried, replace),) = picked
    assert retried == ["app.b"] and "app.b" in replace
    # FAILS WITHOUT THE FIX (round 2): a marked table with no rows was appended (no choice was
    # shown), which kept its mark and every refusal, while the route said the retry drops and
    # reloads it. Dropping an empty table loses nothing: it is forced too.
    empty = _fc_state(selected=names)
    ui, picked = _open_retry_dialog(monkeypatch, state=empty, job=job, rows=(), tables=names)
    assert empty.exclusion_forced_replace.keys() == {"app.b"}
    assert ui.start().text == "Drop 1 table, recreate and load"
    assert _SPLIT_FORCED in _body(ui)
    _confirm(ui)
    ((retried, replace),) = picked
    assert retried == ["app.b"] and "app.b" in replace


def test_the_split_retry_is_forced_until_the_record_clears_unless_it_reads_in_parts(
        monkeypatch):
    from tests.test_g17_full_load_and_cdc_additions import _open_retry_dialog

    names = ["app.a", "app.b"]
    job = _split_record(names, split="app.b", status="FAILED")
    # Reader shards still above 1: the retry reads it in parts again, and an Append keeps
    # CDC's replay correct -- the operator chooses.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    parts = _fc_state(selected=names)
    ui, _picked = _open_retry_dialog(monkeypatch, state=parts, job=job, rows=("app.b",),
                                     tables=names)
    assert parts.exclusion_forced_replace == {} and ui.radios
    # A Start CDC submitted (or streamed) since does not end the wait: the record decides.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
    started = _fc_state(selected=names)
    started.set_cdc_ops_window_start(_TS)
    _open_retry_dialog(monkeypatch, state=started, job=job, rows=("app.b",), tables=names)
    assert started.exclusion_forced_replace.keys() == {"app.b"}
    # Once a CHECKSUM match (or a recreate) cleared the mark, the operator chooses again.
    proven = _split_record(names, split="none", status="FAILED")
    proven.chunks[1].status = "FAILED"
    cleared = _fc_state(selected=names)
    ui, _picked = _open_retry_dialog(monkeypatch, state=cleared, job=proven, rows=("app.b",),
                                     tables=names)
    assert cleared.exclusion_forced_replace == {} and ui.radios


def test_the_post_load_table_list_hold_carves_out_a_split_table(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: after the load, Deploy, Start CDC and Retry CDC said "Capture fewer
    # tables in this CDC pipeline, or use Full load only. Deselect tables in the table picker."
    # -- the table read in parts can be neither deselected nor taken to Full load only.
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._models import CDC_TABLE_LIST_HEADER

    from tests.test_g12_table_set_after_load import _Jobs
    from tests.test_g128_deploy_review import _inventory, _names, _state

    names = _names(300)
    split = names[0]
    record = _split_record(names, split=split)
    inv = _inventory(names)
    tail = (
        f"Capture fewer tables in this CDC pipeline. Deselect tables in the table picker. The "
        f"Full Load read {split} in parts, so it cannot be deselected: to deselect it, set "
        "Reader shards per large table to 1 in Settings and reload it (Reload specific tables, "
        "which drops and recreates it) first. Once CDC starts, the table set is fixed (Start "
        "over to change it). Nor can the migration switch to Full load only until that reload, "
        "or until a match in Validation in CHECKSUM mode proves CDC's replay of it."
    )

    def _state_for(phase="absent"):
        state = _state(names, phase=phase)
        state.job_id = "job-1"
        state.set_prereq_gated_mode(MigrationMode.CDC)
        return state

    jobs = _Jobs({"job-1": record})
    header, body = _cdc_ui._deploy_block_notice(_state_for(), jobs, inv, None)
    assert header == CDC_TABLE_LIST_HEADER and body.endswith(tail)
    assert "Full load only." not in body and "or use Full load only" not in body
    # The Start button and the Retry CDC backstop say the same.
    notices: list = []
    real = _cdc_ui.render_notice
    monkeypatch.setattr(_cdc_ui, "render_notice",
                        lambda ui, **kw: notices.append(kw) or real(ui, **kw))
    _cdc_ui._render_cdc_start_button(_RecordingUi(), _state_for("infra"), jobs, lambda: None,
                                     inventory=inv, session=None)
    (start,) = [n for n in notices if n.get("header") == CDC_TABLE_LIST_HEADER]
    assert start["body"].endswith(tail)
    notices.clear()
    monkeypatch.setattr(_cdc_ui, "render_notice", lambda ui, **kw: notices.append(kw))
    jobs.submit = lambda work: pytest.fail("submitted")
    _cdc_ui._start_cdc_deploy(
        SimpleNamespace(notify=lambda *a, **k: None), _state_for("infra"), jobs, lambda: None,
        inventory=inv,
        session=SimpleNamespace(source_config=None, target_config=None, aws_profile=None,
                                restored_source_type=None, source_password=None),
    )
    (retry,) = [n for n in notices if n.get("header") == CDC_TABLE_LIST_HEADER]
    assert retry["body"].endswith(tail)
    # No table read in parts: today's hold.
    plain = _split_record(names, split="none")
    _h, plain_body = _cdc_ui._deploy_block_notice(_state_for(), _Jobs({"job-1": plain}), inv,
                                                  None)
    assert plain_body.endswith(
        "Capture fewer tables in this CDC pipeline, or use Full load only. Deselect tables in "
        "the table picker.")
    # A Start CDC submitted since does not end it (round 2: it did): the record decides.
    started = _state_for()
    started.set_cdc_ops_window_start(_TS)
    _h, started_body = _cdc_ui._deploy_block_notice(started, jobs, inv, None)
    assert f"The Full Load read {split} in parts, so it cannot be deselected" in started_body
    assert "or use Full load only" not in started_body


def test_the_cdc_only_remedies_go_through_full_load_and_cdc_for_a_split_table(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: CDC only's "switch the migration type to Full load only for that
    # run" -- a switch refused while a table read in parts waits for CDC's replay. Since G147
    # every MySQL CDC only re-run goes through Full load + CDC (its Full Load makes the start
    # point exact), so the split table needs no route of its own.
    from dsql_migrator.ui.cdc_start_point_gate import drop_and_reload_rerun, reload_condition
    from dsql_migrator.ui.data_migration._cdc_ui import _reload_condition_notice

    from tests.test_g12_table_set_after_load import _Jobs

    plain_rerun, plain_note = drop_and_reload_rerun(MigrationType.CDC_ONLY)
    rerun, note = drop_and_reload_rerun(MigrationType.CDC_ONLY, split=True)
    assert (rerun, note) == (plain_rerun, plain_note)
    assert note == (" (CDC only has no Full Load step: switch the migration type to Full load + "
                    "CDC for that run, whose Full Load makes the CDC start point exact)")
    assert drop_and_reload_rerun(FLC, split=True) == drop_and_reload_rerun(FLC)
    assert note in reload_condition(MigrationType.CDC_ONLY, split=True)
    assert "Full load only for that run" not in reload_condition(MigrationType.CDC_ONLY,
                                                                 split=True)
    # The Start CDC dialog's notice judges it from the record (the card from the same facts).
    job = _split_record([EVENTS, "app.users"], split=EVENTS)
    state = _start_state()
    state.migration_type = MigrationType.CDC_ONLY
    state.set_cdc_has_committed_offset(False)
    jobs = _Jobs({"job-1": job})
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL),
                              restored_source_type=None)
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    monkeypatch.setattr(cdc_ui, "drop_reload_condition_due", lambda *_a, **_k: True)
    _header, body = _reload_condition_notice(state, job, session)
    assert note in body
    state.set_cdc_ops_window_start(_TS)          # a Start submitted since: still the record's
    _header, body = _reload_condition_notice(state, job, session)
    assert note in body
    job.chunks[0].reader_shards = None           # cleared (a CHECKSUM match): the plain note
    _header, body = _reload_condition_notice(state, job, session)
    assert plain_note in body


def test_a_fetch_current_position_remedy_says_a_split_table_comes_first() -> None:
    # FAILS WITHOUT THE FIX: the start-point and GTID refusals' "choose Manual, then Fetch current
    # position" led straight into the split refusal of a later point.
    from dsql_migrator.ui.cdc_gtid_gate import (
        KIND_NO_GTID,
        GtidStartVerdict,
        gtid_start_notice,
    )
    from dsql_migrator.ui.cdc_start_point_gate import (
        KIND_CHANGED,
        StartPointVerdict,
        start_point_notice,
        with_split_qualifier,
    )
    from dsql_migrator.ui.data_migration._cdc_ui import _split_later_qualifier

    from tests.test_g12_table_set_after_load import _Jobs

    job = _split_record([EVENTS, "app.users"], split=EVENTS)
    qualifier = _split_later_qualifier(_start_state(), _Jobs({"job-1": job}), job)
    assert qualifier == (
        "While CDC captures app.events, which the Full Load read in parts, Start CDC refuses a "
        "start point later than the Full Load's (Fetch current position gives one): first set "
        "Reader shards per large table to 1 in Settings and reload it (Reload specific tables, "
        "which drops and recreates it).")
    # The refusal it announces stays after a Start was submitted, and so does the qualifier.
    started = _start_state()
    started.set_cdc_ops_window_start(_TS)
    assert _split_later_qualifier(started, _Jobs({"job-1": job}), job) == qualifier
    changed = start_point_notice(
        StartPointVerdict(kind=KIND_CHANGED, changed=((EVENTS, "a column was added"),)),
        point_label="the Full Load watermark (binlog mysql-bin.000009:4)",
        coord="binlog mysql-bin.000009:4", migration_type=FLC, split_qualifier=qualifier)
    assert "Fetch current position" in changed[3] and changed[3].endswith(qualifier)
    gtid = gtid_start_notice(
        GtidStartVerdict(kind=KIND_NO_GTID), point_label="your Manual start point",
        coord="binlog mysql-bin.000012:4", origin="manual", migration_type=FLC,
        split_qualifier=qualifier)
    assert "Fetch current position" in gtid[3] and gtid[3].endswith(qualifier)
    # A remedy that names no fetched point is left as it is; no qualifier, nothing added.
    plain = ("error", "x", "Header", "Check the source and Start CDC again.")
    assert with_split_qualifier(plain, qualifier) == plain
    assert with_split_qualifier(changed, "") == changed
    assert with_split_qualifier(None, qualifier) is None


def test_the_purged_binlog_probe_routes_a_split_table_by_its_state(monkeypatch) -> None:
    # The probe hands the reason the route by the control the Full Load step shows, and CDC
    # only's re-run note goes through Full load + CDC.
    import dsql_migrator.core.watermark as wm_mod
    import dsql_migrator.ui.connect as connect
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_g12_table_set_after_load import _Jobs

    class _Engine:
        def connect(self):
            class _C:
                def __enter__(self):
                    return object()

                def __exit__(self, *_a):
                    return False

            return _C()

        def dispose(self):
            return None

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine())
    )
    monkeypatch.setattr(wm_mod, "list_binary_logs", lambda _c: ["mysql-bin.000095"])
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL),
                              source_password="pw", restored_source_type=None)
    job = _split_record([EVENTS, "app.users"], split=EVENTS, status="FAILED")
    state = _start_state()
    state.set_cdc_has_committed_offset(False)
    reason = _cdc_ui._probe_binlog_resume_gap(state, _Jobs({"job-1": job}), session)
    assert reason.endswith(
        "To start from a manual position and accept the gap, first set Reader shards per large "
        "table to 1 in Settings and reload it (Retry unfinished tables, which drops and "
        "recreates it).")
    state.migration_type = MigrationType.CDC_ONLY
    cdc_only = _cdc_ui._probe_binlog_resume_gap(state, _Jobs({"job-1": job}), session)
    assert "switch the migration type to Full load + CDC" in cdc_only
    assert "Full load only for that run" not in cdc_only


# ---------------------------------------------------------------------------
# 7. The texts: Settings and the manual say the same thing
# ---------------------------------------------------------------------------


def test_the_settings_texts_state_the_g137_rule_and_the_trade_off() -> None:
    from dsql_migrator.config import AppConfig, TUNABLE_KNOBS

    fields = AppConfig.model_fields
    shards = " ".join(fields["full_load_reader_shards"].description.split())
    assert "single-integer-PK" not in shards
    assert "MySQL: Full Load + CDC with a binlog start point only, and only a table whose " \
           "every unique key on Aurora DSQL includes that leading column" in shards
    assert "PostgreSQL: any run, the ranges sharing one snapshot" in shards
    assert "rise up to full_load_table_parallelism-fold" in shards
    retry = " ".join(fields["full_load_source_retry_attempts"].description.split())
    assert "single point in time" not in retry
    assert "A MySQL table read in reader shards is not re-read" in retry
    throttle = " ".join(fields["full_load_max_source_threads_running"].description.split())
    assert "x full_load_reader_shards x" not in throttle
    (knob,) = [k for k in TUNABLE_KNOBS if k.field == "full_load_max_source_threads_running"]
    assert "reader shards × batches-per-table" not in knob.help_text
    assert "whatever the reader shards" in " ".join(knob.help_text.split())
    # The reload that lifts the refusals is a Drop & reload, as every refusal says.
    (shards_knob,) = [k for k in TUNABLE_KNOBS if k.field == "full_load_reader_shards"]
    help_text = " ".join(shards_knob.help_text.split())
    assert "it is reloaded with this set to 1 and Drop & reload" in help_text
    # ...and a CHECKSUM match ends them too -- not an untick (round 4).
    assert "refused until a match in Validation in CHECKSUM mode proves that replay" in (
        help_text)
    assert ("Unticking it needs that reload, before CDC starts: once CDC starts, the table set "
            "is fixed (manual §3.8).") in help_text


_RULE = {
    "en": "**MySQL**: only in **Full load + CDC** whose start point has a binlog file and position",
    "ko": "**MySQL**: 시작 지점에 binlog 파일과 위치가 있는 **Full load + CDC**에서만",
    "ja": "**MySQL**: 開始位置に binlog ファイルと位置がある **Full load + CDC** でのみ",
}
_ROUTES = {
    "en": "### A table read in parts needs CDC's replay (MySQL)",
    "ko": "### 나누어 읽은 테이블에는 CDC의 재생이 필요합니다(MySQL)",
    "ja": "### 分割して読んだテーブルには CDC の再生が必要です（MySQL）",
}
_TRADE_OFF = {
    "en": "K > 1 turns idle slots into concurrent reads of the same table for its whole read",
    "ko": "K > 1은 놀던 slot을 같은 테이블에 대한 동시 읽기로 바꾸며",
    "ja": "K > 1 は空いていた slot を同じテーブルへの同時読み取りに変え",
}
_LIMIT = {
    "en": "**MySQL: a table read in parts is consistent only once CDC replays it.**",
    "ko": "**MySQL: 나누어 읽은 테이블은 CDC가 재생해야 일관됨.**",
    "ja": "**MySQL: 分割して読んだテーブルは CDC が再生して初めて一貫します。**",
}
_STALE = {
    "en": ("A MySQL table always uses one reader", "rarely worth turning on",
           "depends on each table being consistent as of a single point in time",
           "(a future enhancement)"),
    "ko": ("MySQL 테이블은 항상", "켤 가치가 드묾", "**단일 시점**으로 일관되다는 전제", "(향후 개선)"),
    "ja": ("MySQL のテーブルは常に 1 つのリーダー", "有効化する価値はまれ",
           "**単一時点** で一貫していることに依存", "（将来の拡張）"),
}


# The review's fixes: the route by the control the step shows, the two ends of the mark (a
# recreate, a CHECKSUM match), the activity-log scope, the failed range counted from 1, the
# recovery after a Start that never streamed.
_REVIEW = {
    "en": ("**Retry unfinished tables** while it is unfinished, **Reload specific tables** once "
           "every table has loaded",
           "The table stays marked until one of two things clears the mark: a load that "
           "recreates it (a **Drop & reload**), or a Validation whose **CHECKSUM** comparison "
           "matches it",
           "for each table large enough to split",
           "with that reason (*range 2 of 4*)",
           "A Start CDC, even one that streamed, does not end the refusals",
           "Retry unfinished tables while it is unfinished, else Reload specific tables: with one "
           "reader they drop and reload it"),
    "ko": ("테이블이 끝나지 않았으면 **Retry unfinished tables**, 모든 테이블이 적재된 뒤에는 "
           "**Reload specific tables**",
           "테이블의 표시는 두 가지 중 하나로 지워집니다: 그 테이블을 다시 만드는 적재(**Drop & "
           "reload**), 또는 그 테이블의 마지막 적재가 끝난 뒤 시작된 Validation의 **CHECKSUM** "
           "비교가 일치하는 경우입니다",
           "나눌 만큼 큰 테이블",
           "그 이유(*range 2 of 4*)와 함께",
           "Start CDC는 스트리밍했더라도 거부를 끝내지 않습니다",
           "끝나지 않았으면 Retry unfinished tables, 그 밖에는 Reload specific tables"),
    "ja": ("テーブルが終わっていなければ **Retry unfinished tables**、すべてのテーブルがロードされた後は "
           "**Reload specific tables**",
           "テーブルの印は、次の 2 つのどちらかで消えます: そのテーブルを作り直すロード（**Drop & "
           "reload**）、またはそのテーブルの最後のロードが終わった後に始まった Validation の "
           "**CHECKSUM** 比較が一致した場合です",
           "分割できる大きさのテーブル",
           "その理由（*range 2 of 4*）とともに",
           "Start CDC は、ストリーミングした場合でも拒否を終わらせません",
           "終わっていなければ Retry unfinished tables、それ以外は Reload specific tables"),
}
_REVIEW_STALE = {
    "en": ("With Reader shards above 1, the activity log says per table",
           "reload it (Reload specific tables, Drop & reload), then untick it",
           "The table stays marked until a load recreates it.",
           "A Start that never streamed ends them too",
           "The refusals last until CDC first starts for this migration"),
    "ko": ("Reader shards가 1보다 크면, 활동 로그가 테이블마다",
           "그 테이블을 다시 적재한 뒤(Reload specific tables, Drop & reload), 체크를 해제하세요",
           "테이블은 적재가 그 테이블을 다시 만들 때까지 표시된 채로 남습니다.",
           "스트리밍하지 못한 Start도 거부를 끝내지만",
           "거부는 이 마이그레이션에서 CDC가 처음 시작될 때까지"),
    "ja": ("Reader shards が 1 より大きいと、アクティビティログがテーブルごとに",
           "そのテーブルをロードし直してから(Reload specific tables、Drop & reload)、チェックを外して",
           "テーブルは、ロードがそのテーブルを作り直すまで印が付いたままです。",
           "ストリーミングしなかった Start も拒否を終わらせます",
           "拒否は、この移行で CDC が初めて開始される"),
}
# Round 3: what ends the wait, and the recovery after a Start that never streamed, in 3.8; the
# lock's ends in 13.7; the refusals' ends in 6.2 (round 4: an untick ends only by the reload,
# before CDC starts -- a CHECKSUM match ends only Full load only's and the later point's).
_ENDS = {
    "en": ("start CDC again from **Automatic** (or a Manual point at or before the Full Load's "
           "watermark): that is the replay, and it is never refused",
           "run Validation in **CHECKSUM** mode with *Fast sweep* off: a match proves the copy "
           "and ends the refusal",
           "Once CDC starts, the table set is fixed (Start over to change it), so a Validation "
           "in CHECKSUM mode that matches the table after CDC's replay ends the Full load only "
           "and later-start-point refusals but never lets it be unticked.",
           "is refused until the table is reloaded with one reader (Drop & reload: an Append "
           "keeps those rows); Full load only and the later start point are also allowed once a "
           "Validation in CHECKSUM mode matches it. Unticking needs the reload before CDC "
           "starts: from CDC's first Start the table set is fixed."),
    "ko": ("**Automatic**(또는 Full Load 워터마크와 같거나 그보다 이른 Manual 지점)에서 CDC를 다시 "
           "시작하세요: 그것이 재생이며 거부되지 않습니다",
           "*Fast sweep*을 끄고 **CHECKSUM** 모드로 Validation을 실행합니다: 일치하면 사본이 "
           "증명되어 거부가 끝납니다",
           "CDC가 시작되면 테이블 집합은 고정되므로(바꾸려면 Start over), CDC의 재생 뒤 CHECKSUM "
           "모드의 Validation이 그 테이블의 일치를 확인하면 Full load only와 더 나중의 시작 지점에 대한 "
           "거부는 끝나지만, 체크 해제는 허용되지 않습니다.",
           "모두 거부됩니다. Full load only와 더 나중의 시작 지점은 CHECKSUM 모드의 Validation이 그 "
           "테이블의 일치를 확인해도 허용됩니다. 선택 해제에는 CDC를 시작하기 전의 다시 적재가 "
           "필요합니다: CDC의 첫 Start부터 테이블 집합은 고정됩니다."),
    "ja": ("**Automatic**（または Full Load のウォーターマークと同じかそれより前の Manual 位置）から "
           "CDC をもう一度開始してください: それが再生であり、拒否されません",
           "*Fast sweep* をオフにして **CHECKSUM** モードで Validation を実行します: 一致すれば"
           "コピーが証明され、拒否は終わります",
           "CDC を開始するとテーブルの集合は固定されるので(変更するには Start over)、CDC の再生の後に "
           "CHECKSUM モードの Validation がそのテーブルの一致を確かめると Full load only とより後の"
           "開始位置への拒否は終わりますが、チェックを外すことはできません。",
           "まですべて拒否されます。Full load only とより後の開始位置は、CHECKSUM モードの Validation "
           "がそのテーブルの一致を確かめた場合にも許可されます。選択を外すには CDC を開始する前の"
           "ロードし直しが必要です: CDC の最初の Start からテーブルの集合は固定されます。"),
}


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_tells_the_review_fixes_in_every_language(lang) -> None:
    from tests.test_g25_docs import _all_pages, _has

    pages = _all_pages(lang)
    full_load = pages["03-full-load.md"]
    tables = pages["13-excluding-tables-and-columns.md"]
    for phrase in _REVIEW[lang][:5]:
        assert _has(lang, full_load, phrase), (lang, phrase)
    assert _has(lang, tables, _REVIEW[lang][5]), lang
    for phrase in _REVIEW_STALE[lang]:
        assert not any(_has(lang, text, phrase) for text in pages.values()), (lang, phrase)


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_says_what_ends_the_wait_in_every_language(lang) -> None:
    from tests.test_g25_docs import _all_pages, _has

    pages = _all_pages(lang)
    restart, checksum, lock, limit = _ENDS[lang]
    assert _has(lang, pages["03-full-load.md"], restart), lang
    assert _has(lang, pages["03-full-load.md"], checksum), lang
    assert _has(lang, pages["13-excluding-tables-and-columns.md"], lock), lang
    assert _has(lang, pages["06-limitations.md"], limit), lang


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_tells_the_g137_story_in_every_language(lang) -> None:
    from tests.test_g25_docs import _all_pages, _has, _page

    assert _has(lang, _page(lang, "03-full-load.md"), _RULE[lang])
    assert _has(lang, _page(lang, "03-full-load.md"), _ROUTES[lang])
    assert _has(lang, _page(lang, "07-performance-and-tuning.md"), _TRADE_OFF[lang])
    assert _has(lang, _page(lang, "06-limitations.md"), _LIMIT[lang])
    pages = _all_pages(lang)
    for phrase in _STALE[lang]:
        assert not any(_has(lang, text, phrase) for text in pages.values()), (lang, phrase)


# ---------------------------------------------------------------------------
# 8. The record decides when the wait ends (a CHECKSUM match or a recreate), and the routes
#    name the control the Full Load step shows over the view it renders
# ---------------------------------------------------------------------------


# Long before any comparison the tests below start on the real clock.
_PAST = datetime(2020, 1, 1, tzinfo=timezone.utc)


def _finished(at):
    """A Full load + CDC record whose app.events (read in 4 parts) and app.users finished at
    ``at``."""
    job = _split_job()
    for chunk in job.chunks:
        chunk.finished_at = at
    return job


def test_a_checksum_match_clears_the_record_only_after_the_load_ended() -> None:
    from datetime import timedelta

    from dsql_migrator.ui.data_migration._models import clear_proven_split_records

    later = _TS + timedelta(minutes=5)
    job = _finished(_TS)
    job.chunks.append(ChunkState(chunk_id="app.stopped", status="FAILED", reader_shards=4))
    job.chunks.append(ChunkState(chunk_id="app.again", status="DONE", reader_shards=4,
                                 finished_at=later + timedelta(seconds=1)))
    cleared = clear_proven_split_records(job, {
        EVENTS: later,              # compared after its load ended: proven
        "app.users": later,         # never read in parts: nothing to clear
        "app.stopped": later,       # a stop stamps no end: unknown, kept
        "app.again": later,         # loaded again after the comparison started: kept
    })
    assert cleared == [EVENTS]
    assert {c.chunk_id: c.reader_shards for c in job.chunks} == {
        EVENTS: None, "app.users": None, "app.stopped": 4, "app.again": 4}
    # A load running now may still write the table: nothing is cleared.
    running = _finished(_TS)
    running.status = "RUNNING"
    assert clear_proven_split_records(running, {EVENTS: later}) == []
    assert running.chunks[0].reader_shards == 4
    # A PostgreSQL record waits for nothing (one shared snapshot): its parts stay recorded.
    pg = _finished(_TS)
    pg.watermark = _wm(binlog=False)
    assert clear_proven_split_records(pg, {EVENTS: later}) == []
    assert pg.chunks[0].reader_shards == 4
    # A naive time (an older store) compares as UTC.
    naive = _finished(_TS)
    assert clear_proven_split_records(naive, {EVENTS: later.replace(tzinfo=None)}) == [EVENTS]


class _Store:
    """A JobStore double that keeps what the manager persisted."""

    def __init__(self):
        self.saved: dict = {}

    def load_all(self):
        return []

    def save(self, job, error):
        self.saved[job.job_id] = job.model_copy(deep=True)

    def prune_terminal(self, keep_most_recent):
        return []


def _manager_with(record):
    """A real JobManager holding ``record`` (a DONE Full Load job), persisted to a store."""
    from dsql_migrator.core.job_manager import JobManager

    store = _Store()
    manager = JobManager(store=store, stall_timeout_seconds=None)

    def _work(handle):
        def _copy(job):
            job.chunks = [c.model_copy(deep=True) for c in record.chunks]
            job.watermark = record.watermark

        handle.update(_copy)

    job_id = manager.submit(_work, job_id="fl-1")
    assert manager.wait(job_id, timeout=5.0) is True
    return manager, store


def _item(table, *, checksum=True, matched=True, skipped=False):
    from dsql_migrator.core.models import TableValidationResult

    return TableValidationResult(
        table=table, source_row_count=5, target_row_count=5, row_count_match=True,
        matched=matched, checksum_match=(matched if checksum else None),
        deep_checks_skipped=skipped,
    )


def test_a_validation_checksum_match_ends_the_wait_and_persists_it(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (round 3): nothing but a recreate cleared the record, so a table
    # CDC had replayed -- proven equal to the source -- stayed refused for good.
    from dsql_migrator.core.models import ValidationMode, ValidationReport
    from dsql_migrator.ui import validation as v

    logged: list = []
    monkeypatch.setattr(v, "log_activity",
                        lambda cat, action, **kw: logged.append((action, kw)))

    def _validated(items):
        manager, store = _manager_with(_finished(_PAST))
        state = DataMigrationState()
        state.job_id = "fl-1"
        vstate = v.ValidationState()
        vstate.mark_run_started()
        vstate.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=items))
        cleared = v.clear_proven_split_tables(state, manager, vstate, items)
        record = manager.get_status("fl-1")
        return cleared, record.chunks[0].reader_shards, store.saved["fl-1"].chunks[0]

    cleared, now, saved = _validated([_item(EVENTS), _item("app.users")])
    assert cleared == [EVENTS] and now is None and saved.reader_shards is None
    ((action, kw),) = logged
    assert action == "tables read in parts proven" and kw["status"].value == "success"
    assert kw["detail"] == (
        "app.events matched by CHECKSUM: the copy the Full Load read in parts equals the source, "
        "so Full load only and a CDC start point later than the Full Load's are no longer "
        "refused")
    # A ROW_COUNT match, a CHECKSUM skipped by Fast sweep, a mismatch, or a table this run did
    # not compare: the mark stays.
    for items in ([_item(EVENTS, checksum=False)], [_item(EVENTS, skipped=True)],
                  [_item(EVENTS, matched=False)], [_item("app.users")]):
        logged.clear()
        cleared, now, saved = _validated(items)
        assert (cleared, now, saved.reader_shards, logged) == ([], 4, 4, [])
    # No Full Load record (or it is gone): nothing to clear, nothing raised.
    vstate = v.ValidationState()
    vstate.mark_run_started()
    assert v.clear_proven_split_tables(DataMigrationState(), None, vstate, [_item(EVENTS)]) == []
    gone = DataMigrationState()
    gone.job_id = "pruned"
    manager, _store = _manager_with(_finished(_PAST))
    assert v.clear_proven_split_tables(gone, manager, vstate, [_item(EVENTS)]) == []


def test_the_validation_run_and_its_recheck_clear_a_table_they_match_by_checksum(
        monkeypatch) -> None:
    # The runner itself (and the per-table re-check) end the wait for a table they match by
    # CHECKSUM -- not by row count.
    from dsql_migrator.core.models import (
        AssessmentReport,
        TargetInventory,
        ValidationMode,
        ValidationReport,
    )
    from dsql_migrator.ui import validation as v
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore

    from tests.test_ui_validation import (
        _FakeValidator,
        _inventory,
        _no_identity_columns,
        _recheck_closure,
    )

    def _screen(mode):
        record = MigrationJob(
            job_id="x", status="DONE", watermark=_wm(counts={"orders": 10}),
            chunks=[ChunkState(chunk_id="orders", status="DONE", reader_shards=4,
                               finished_at=_PAST)])
        manager, _store = _manager_with(record)
        store, evals, migrations, validations = (SessionStore(), EvaluationStore(),
                                                 DataMigrationStore(), v.ValidationStore())
        session = store.get_or_create("s")
        session.set_source(SourceConnectionConfig(host="db", database="app"), v.SecretValue("pw"))
        session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                                  region="us-east-1"))
        session.set_source_verified(True)
        session.set_target_verified(True)
        evals.get_or_create("s").set_result(EvaluationResult(
            inventory=_inventory(), assessment=AssessmentReport.from_items([]),
            target_inventory=TargetInventory(), target_conflicts=[]))
        migrations.get_or_create("s").job_id = "fl-1"
        report = ValidationReport.build(mode=mode, items=[
            _item("orders", checksum=mode is ValidationMode.CHECKSUM)])
        content, runner = v.build_validation_screen(
            store, "s", job_manager=manager, eval_store=evals, migration_store=migrations,
            validation_store=validations, validator_factory=lambda _i: _FakeValidator(report),
            sync_sequences=_no_identity_columns)
        return content, runner, validations.get_or_create("s"), manager

    def _shards(manager):
        return manager.get_status("fl-1").chunks[0].reader_shards

    content, runner, vstate, manager = _screen(ValidationMode.CHECKSUM)
    runner()
    assert manager.wait(vstate.job_id, timeout=5.0) is True
    assert _shards(manager) is None
    content, runner, vstate, manager = _screen(ValidationMode.ROW_COUNT)
    runner()
    assert manager.wait(vstate.job_id, timeout=5.0) is True
    assert _shards(manager) == 4
    # A re-check that matches it by CHECKSUM ends the wait too.
    content, runner, vstate, manager = _screen(ValidationMode.CHECKSUM)
    vstate.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[
        _item("orders", matched=False)]))
    _recheck_closure(content)(["orders"])
    assert manager.wait(vstate.job_id, timeout=5.0) is True
    assert _shards(manager) is None


def _never_streamed(state):
    """A Start CDC (Automatic) was submitted and never streamed: the worker refused it, or its
    connector failed. Submission stamps the CDC operations window; nothing else is recorded."""
    state.set_cdc_ops_window_start(_TS)
    state.set_cdc_has_committed_offset(False)
    return state


def test_every_refusal_holds_after_a_start_that_never_streamed(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (round 3): a submitted Start CDC ended every refusal -- one that
    # never streamed too -- and a later Manual point or Full load only then left the tear. A
    # split recorded by a load AFTER an earlier Start never armed them at all.
    from dsql_migrator.ui.data_migration import _cdc_ui, full_load_only_switch_refusal
    from dsql_migrator.ui.data_migration._cdc_ui import (
        _captured_split_tables,
        _split_later_qualifier,
        split_start_point_refusal,
    )

    from tests.test_g12_table_set_after_load import _Jobs

    job = _split_job()
    jobs = _Jobs({"job-1": job})
    later = _never_streamed(_start_state(manual=("mysql-bin.000012", 4)))
    seed, origin = _seed(later, job)
    assert split_start_point_refusal(later, job, seed, SourceType.MYSQL, origin=origin,
                                     job_manager=jobs).startswith(
        "The Full Load read app.events in parts")
    assert full_load_only_switch_refusal(later, jobs, job) is not None
    assert _captured_split_tables(later, job) == [EVENTS]
    assert _split_later_qualifier(later, jobs, job).startswith("While CDC captures app.events")
    assert "cannot be deselected" in _cdc_ui._table_list_hold_remedy(
        "Too many tables. Capture fewer tables in this CDC pipeline, or use Full load only.",
        later, jobs, None)
    # That replay is what recovers it, so it is never refused: Automatic, a Manual point at or
    # before the Full Load's start point, or a resume of a stopped stream.
    auto = _never_streamed(_start_state())
    seed, origin = _seed(auto, job)
    assert split_start_point_refusal(auto, job, seed, SourceType.MYSQL, origin=origin,
                                     job_manager=jobs) is None
    at = _never_streamed(_start_state(manual=("mysql-bin.000009", 4)))
    seed, origin = _seed(at, job)
    assert split_start_point_refusal(at, job, seed, SourceType.MYSQL, origin=origin,
                                     job_manager=jobs) is None
    stopped = _start_state(manual=("mysql-bin.000012", 4))
    stopped.set_cdc_ops_window_start(_TS)
    stopped.set_cdc_has_committed_offset(True)              # it streamed, then stopped
    stopped.set_cdc_deployed_watermark(("mysql-bin.000012", "4"))
    seed, origin = _seed(stopped, job)
    resumes = _cdc_ui.cdc_start_resumes(stopped, seed, SourceType.MYSQL)
    assert resumes is True
    assert split_start_point_refusal(stopped, job, seed, SourceType.MYSQL, origin=origin,
                                     resumes=resumes, job_manager=jobs) is None


def test_the_routes_follow_the_view_the_full_load_step_renders(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (round 3): the routes were worded off the record while the step
    # chose its buttons off the scoped view -- after the table set changed since the load,
    # a route named a control the step did not show.
    from dsql_migrator.ui.data_migration import (
        _cdc_ui,
        full_load_only_switch_refusal,
        table_set_add_lock,
    )
    from dsql_migrator.ui.data_migration._cdc_ui import (
        _split_later_qualifier,
        split_start_point_refusal,
    )

    from tests.test_g12_table_set_after_load import _Jobs

    def _state(selected):
        state = _start_state(manual=("mysql-bin.000012", 4))
        state.selection = TableSelection(selected_tables=list(selected))
        state.set_resolved_selection(list(selected))
        return state

    def _routes(state, job):
        jobs = _Jobs({"job-1": job})
        seed, origin = _seed(state, job)
        return [
            table_set_add_lock(state, jobs).removal_refusal([EVENTS], tree=True),
            full_load_only_switch_refusal(state, jobs, job),
            split_start_point_refusal(state, job, seed, SourceType.MYSQL, origin=origin,
                                      job_manager=jobs),
            _split_later_qualifier(state, jobs, job),
            _cdc_ui._table_list_hold_remedy(
                "Too many tables. Capture fewer tables in this CDC pipeline, or use Full load "
                "only.", state, jobs, None),
        ]

    reload_now = "(Reload specific tables, which drops and recreates it)"
    reload_later = ("(once no table is left unfinished: Reload specific tables, which drops "
                    "and recreates it)")
    # A table ticked after the load (G17) is unfinished in the step's view: the step shows
    # Retry unfinished tables, and Reload specific tables only once it has loaded.
    added = _routes(_state([EVENTS, "app.users", "app.new"]), _split_job())
    assert all(reload_later in text for text in added), added
    # Another table that did not finish was unticked since: the step's view holds only
    # finished tables and shows Reload specific tables.
    failed_other = _split_job()
    failed_other.chunks[1].status = "FAILED"
    failed_other.status = "FAILED"
    unticked = _routes(_state([EVENTS]), failed_other)
    assert all(reload_now in text for text in unticked), unticked
    # Nothing changed since the load: the record is the view.
    same = _routes(_state([EVENTS, "app.users"]), _split_job())
    assert all(reload_now in text for text in same), same


def test_the_routes_on_the_real_screen_name_the_button_it_shows(monkeypatch) -> None:
    # The verifier's two screens: a table ticked after the load (S3) and an unfinished table
    # unticked since (S2). Every route names the button the step shows, as the caption does.
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import full_load_only_switch_refusal, table_set_add_lock
    from dsql_migrator.ui.workflow import WorkflowStep, with_status

    from tests.test_g16_full_load_and_cdc_narrowing import _fc_screen

    def _render(job, selected, step):
        s = _fc_screen(monkeypatch, job=job, selected=selected, render=False)
        s.session.set_workflow(with_status(s.session.workflow, WorkflowStep.FULL_LOAD, step))
        s.content(lambda: None)
        return s

    def _job(status_b):
        return MigrationJob(
            job_id="job-1", status="DONE" if status_b == "DONE" else "FAILED",
            chunks=[ChunkState(chunk_id="app.a", status="DONE", attempts=1, rows_loaded=5,
                               reader_shards=4),
                    ChunkState(chunk_id="app.b", status=status_b, attempts=1, rows_loaded=5)],
            watermark=_wm(counts={"app.a": 5, "app.b": 5}),
        )

    for job, selected, step, button, route in (
        (_job("DONE"), ["app.a", "app.b", "app.c"], StepStatus.DONE,
         "Retry unfinished tables (1)",
         "once no table is left unfinished: Reload specific tables, which drops and recreates "
         "it"),
        (_job("FAILED"), ["app.a"], StepStatus.FAILED, "Reload specific tables…",
         "Reload specific tables, which drops and recreates it"),
    ):
        s = _render(job, selected, step)
        labels = [str(getattr(b, "text", "")) for b in s.ui.buttons]
        assert button in labels, labels
        rec = s.jobs.jobs["job-1"]
        texts = [
            table_set_add_lock(s.state, s.jobs).removal_refusal(["app.a"], tree=True),
            full_load_only_switch_refusal(s.state, s.jobs, rec),
            *[t for t in s.ui.tooltips if t.startswith("The Full Load read app.a")],
        ]
        assert len(texts) >= 3 and all(f"({route})" in t for t in texts), texts
        (caption,) = [row["parts_tooltip"] for tbl in s.ui.tables for row in tbl["rows"]
                      if isinstance(row, dict) and row.get("table") == "app.a"]
        assert f"({route}) first." in caption


def test_a_locked_type_selector_shows_no_route_on_the_full_load_only_tile(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (round 3): every tile is muted and ignores a click while a load
    # runs or the CDC infrastructure is created, yet the Full load only tile still asked for
    # the click in the error tone.
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import _render_migration_type_selector

    line = "Not available now: tables the Full Load read in parts need CDC (click for the route)."

    class _Ui(_RecordingUi):
        def __init__(self):
            super().__init__()
            self.notes: list = []
            self.clicks: list = []

        def notify(self, text="", *_a, **k):
            self.notes.append((str(text), k.get("type")))

        def card(self, *a, **k):
            el = super().card(*a, **k)
            el.on = lambda _evt, handler, *_a, **_k: (self.clicks.append(handler), el)[1]
            return el

    state = _fc_state()
    ui = _Ui()
    _render_migration_type_selector(
        ui, state, status=StepStatus.IN_PROGRESS, refresh=lambda: None, locked=True,
        lock_reason="Locked while a migration is in progress — finish or cancel it to change "
        "the type.", type_refusals={FLO: "Refused: read in parts."})
    assert line not in ui.texts and "Refused: read in parts." not in ui.tooltips
    assert not any("text-red-700" in c for c in ui.classes_applied)
    ui.clicks[0]()
    assert ui.notes == [] and state.migration_type is FLC
    # The real screen while a split load runs: the selector's lock reason, no route.
    from dsql_migrator.ui.workflow import WorkflowStep, with_status

    from tests.test_g16_full_load_and_cdc_narrowing import _fc_screen

    job = MigrationJob(
        job_id="job-1", status="RUNNING", watermark=_wm(counts={"app.a": 5, "app.b": 5}),
        chunks=[ChunkState(chunk_id="app.a", status="IN_PROGRESS", attempts=1, reader_shards=4),
                ChunkState(chunk_id="app.b", status="DONE", attempts=1, rows_loaded=5)])
    s = _fc_screen(monkeypatch, job=job, selected=["app.a", "app.b"], render=False)
    s.session.set_workflow(with_status(s.session.workflow, WorkflowStep.FULL_LOAD,
                                       StepStatus.IN_PROGRESS))
    s.content(lambda: None)
    assert line not in s.ui.texts
    assert any(t.startswith("Locked while") for t in s.ui.texts)


# ---------------------------------------------------------------------------
# 9. Round 4: an untick ends only by the reload, before CDC starts; a re-run of every table
#    drops and reloads a table read in parts
# ---------------------------------------------------------------------------


def _ties_an_untick_to_checksum(text) -> list:
    """The sentences of ``text`` that tie an untick, an unselect or a deselect to a CHECKSUM
    match -- a way out CDC's first Start closes for good."""
    import re

    return [s for s in re.split(r"(?<=\.)\s+", text or "")
            if "CHECKSUM" in s and re.search(r"(?i)untick|unselect|deselect", s)]


def test_no_untick_text_promises_an_untick_after_a_checksum_match(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (round 4): the untick, unselect and deselect refusals, the lock tip,
    # the tree hint, the caption, the knob help, the pre-load carve-out and the activity line
    # offered a CHECKSUM match "once CDC has replayed it" as a way to untick the table. That
    # replay needs CDC started, and CDC's first Start fixes the table set for good (G16), so
    # the match could never let the user untick it.
    from dsql_migrator.config import TUNABLE_KNOBS
    from dsql_migrator.core.models import StepStatus, ValidationMode, ValidationReport
    from dsql_migrator.ui import validation as v
    from dsql_migrator.ui.data_migration import (
        LOCK_TIP_SPLIT,
        TREE_HINT_SPLIT,
        _cdc_ui,
        full_load_only_switch_refusal,
        selection_lock_reason,
        table_set_add_lock,
        table_set_editable_after_load,
    )
    from dsql_migrator.ui.data_migration._cdc_ui import split_start_point_refusal
    from dsql_migrator.ui.data_migration._full_load_ui import _reader_shards_tooltip
    from dsql_migrator.ui.data_migration._models import (
        PRE_LOAD_SPLIT_CARVE_OUT,
        SPLIT_SET_FIXED,
        split_checksum_route,
    )

    logged: list = []
    monkeypatch.setattr(v, "log_activity",
                        lambda cat, action, **kw: logged.append((action, kw["detail"])))
    manager, _store = _manager_with(_finished(_PAST))
    state = _start_state(manual=("mysql-bin.000012", 4))
    state.job_id = "fl-1"

    def _texts():
        record = manager.get_status("fl-1")
        seed, origin = _seed(state, record)
        return record, full_load_only_switch_refusal(state, manager, record), (
            split_start_point_refusal(state, record, seed, SourceType.MYSQL, origin=origin,
                                      job_manager=manager))

    # Before any Start (G16's narrowing is open): the untick, unselect and deselect texts name
    # only the reload, and that the set is fixed once CDC starts.
    record, switch, later = _texts()
    lock = table_set_add_lock(state, manager)
    untick = lock.removal_refusal([EVENTS], tree=True)
    unselect = lock.removal_refusal([EVENTS], tree=False)
    deselect = _cdc_ui._table_list_hold_remedy(
        "Too many tables. Capture fewer tables in this CDC pipeline, or use Full load only.",
        state, manager, None)
    (row,) = [r for r in build_full_load_table_rows(record) if r.table == EVENTS]
    caption = _reader_shards_tooltip(row, own_snapshots=True)
    (knob,) = [k for k in TUNABLE_KNOBS if k.field == "full_load_reader_shards"]
    for text in (untick, unselect, LOCK_TIP_SPLIT, TREE_HINT_SPLIT, deselect, caption,
                 " ".join(knob.help_text.split()), PRE_LOAD_SPLIT_CARVE_OUT):
        assert _ties_an_untick_to_checksum(text) == [], text
    for text in (untick, unselect, LOCK_TIP_SPLIT, TREE_HINT_SPLIT, deselect, caption):
        assert SPLIT_SET_FIXED in text, text
    # The removal refusal has no CHECKSUM sentence at all.
    assert "CHECKSUM" not in untick and "CHECKSUM" not in unselect
    assert untick.endswith(f"then untick it. {SPLIT_SET_FIXED}")
    assert unselect.endswith(f"then unselect it. {SPLIT_SET_FIXED}")
    # Full load only and a later start point keep the CHECKSUM end: it works for them.
    assert switch.endswith(split_checksum_route([EVENTS]))
    assert later.endswith(split_checksum_route([EVENTS]))
    assert deselect.endswith(
        "Nor can the migration switch to Full load only until that reload, or until a match in "
        "Validation in CHECKSUM mode proves CDC's replay of it.")

    # A Start CDC, then (once CDC has replayed the table) a CHECKSUM match clears the mark.
    state.set_cdc_ops_window_start(_TS)
    vstate = v.ValidationState()
    vstate.mark_run_started()
    items = [_item(EVENTS)]
    vstate.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=items))
    assert v.clear_proven_split_tables(state, manager, vstate, items) == [EVENTS]
    record, switch, later = _texts()
    assert split_read_tables(record) == []
    # The table set is fixed for good: no untick is possible -- and none was promised.
    assert table_set_editable_after_load(state, manager) is False
    assert table_set_add_lock(state, manager) is None
    assert selection_lock_reason(state, manager, status=StepStatus.DONE, migration_type=FLC,
                                 has_job=True).startswith(
        "Locked — a Full Load has already run for this table set. Use 'Start over'")
    ((action, line),) = logged
    assert action == "tables read in parts proven"
    assert "untick" not in line and _ties_an_untick_to_checksum(line) == []
    # ...while the match did end the Full load only and the later-point refusals.
    assert switch is None and later is None


def test_a_rerun_of_every_table_drops_and_reloads_a_split_table(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (round 4): Re-run Full Load and Re-run all tables (a new start point
    # and a new record) offered Append for a table read in parts: CDC from the new start point
    # never replays the changes before it, so the rows read in parts stayed as they were for
    # good, and the new record no longer marked them (no refusal held them either).
    from tests.test_g17_full_load_and_cdc_additions import _open_retry_dialog
    from tests.test_start_dialog_overlay import _body, _confirm

    names = ["app.a", "app.b"]
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    # Whether the re-run reads the table in parts again (Reader shards 4) or not (1).
    for shards in ("1", "4"):
        monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", shards)
        for status, click in (("FAILED", "Re-run all tables"), ("DONE", "Re-run Full Load")):
            state = _fc_state(selected=names)
            job = _split_record(names, split="app.b", status=status)
            ui, _picked = _open_retry_dialog(monkeypatch, state=state, job=job, rows=names,
                                             tables=names, click=click)
            assert state.exclusion_forced_replace.keys() == {"app.b"}, (shards, click)
            assert ui.start().text == "Drop 1 table, recreate and load"
            assert _SPLIT_FORCED in _body(ui)
            # app.a holds rows too and stays the operator's choice (Append by default).
            assert ui.radios, (shards, click)
            _confirm(ui)
            assert state.take_run_choice()[0] == frozenset({"app.b"}), (shards, click)
    # A record that marks no table forces nothing.
    plain = _fc_state(selected=names)
    ui, _picked = _open_retry_dialog(
        monkeypatch, state=plain, job=_split_record(names, split="none"), rows=names,
        tables=names, click="Re-run Full Load")
    assert plain.exclusion_forced_replace == {} and ui.radios


_ROUND4 = {
    "en": ("Unticking the table has only the reload, before CDC starts: the replay needs CDC "
           "started, and from CDC's first Start the table set is fixed",
           "A re-run of every table (**Re-run Full Load**, or **Re-run all tables**) drops and "
           "reloads such a table too, whatever the choice and the Reader shards setting",
           "the CHECKSUM match ends the Full load only and later-start-point refusals",
           "is the exception: it cannot be unticked until it is reloaded with one reader (Drop & "
           "reload)."),
    "ko": ("테이블의 체크 해제에는 CDC를 시작하기 전에 다시 적재하는 방법만 있습니다: 재생에는 CDC가 "
           "시작되어야 하고, CDC의 첫 Start부터 테이블 집합이 고정되기 때문입니다",
           "모든 테이블의 재실행(**Re-run Full Load** 또는 **Re-run all tables**)도 선택이나 Reader "
           "shards 설정과 관계없이 그런 테이블을 삭제 후 다시 적재합니다",
           "CHECKSUM 일치가 Full load only와 더 나중의 시작 지점에 대한 거부를 끝냅니다",
           "은 예외입니다: 리더 하나로 다시 적재하기(Drop & reload) 전에는 선택을 해제할 수 없습니다."),
    "ja": ("テーブルのチェックを外すには、CDC を開始する前にロードし直すしかありません: 再生には CDC の"
           "開始が必要で、CDC の最初の Start からテーブルの集合は固定されるからです",
           "すべてのテーブルの再実行（**Re-run Full Load** または **Re-run all tables**）も、選択や "
           "Reader shards の設定にかかわらず、そのようなテーブルを削除してロードし直します",
           "CHECKSUM の一致が Full load only とより後の開始位置への拒否を終わらせます",
           "は例外です: リーダー 1 つでロードし直す(Drop & reload)まで選択を外せません。"),
}
_ROUND4_STALE = {
    "en": ("the two ways that end the refusal",
           "The lock ends only when the table is reloaded with one reader or a Validation in "
           "CHECKSUM mode matches it",
           "it cannot be unticked until it is reloaded with one reader (Drop & reload) or a "
           "Validation in CHECKSUM mode matches it"),
    "ko": ("거부를 끝내는 두 가지 방법을 안내합니다",
           "잠금은 CDC가 시작될 때가 아니라",
           "CHECKSUM 모드의 Validation이 그 테이블의 일치를 확인하기 전에는 선택을 해제할 수 없습니다"),
    "ja": ("拒否を終わらせる 2 つの方法が示されます",
           "ロックが解けるのは、CDC の開始時ではなく",
           "CHECKSUM モードの Validation がそのテーブルの一致を確かめるまで選択を外せません"),
}


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_says_an_untick_ends_only_by_the_reload_in_every_language(lang) -> None:
    from tests.test_g25_docs import _all_pages, _has

    pages = _all_pages(lang)
    untick, rerun, checksum, exception = _ROUND4[lang]
    assert _has(lang, pages["03-full-load.md"], untick), lang
    assert _has(lang, pages["03-full-load.md"], rerun), lang
    assert _has(lang, pages["03-full-load.md"], checksum), lang
    assert _has(lang, pages["06-limitations.md"], exception), lang
    for phrase in _ROUND4_STALE[lang]:
        assert not any(_has(lang, text, phrase) for text in pages.values()), (lang, phrase)
