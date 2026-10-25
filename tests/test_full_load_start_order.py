# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The multi-process Full Load starts its largest tables first (R2).

The process pool starts its units first in, first out, and a table that is not split loads
on one process, so tables started in inventory (or creation) order left the largest table
that happened to start late running alone at the end while the other slots sat idle. The
units are now submitted largest first by the scan-free row estimate the start point already
holds (MySQL ``information_schema.tables.table_rows``, PostgreSQL ``pg_class.reltuples``); a
PostgreSQL table whose ``reltuples`` is -1 (never analysed) is estimated from its size on
disk in one catalog read; a table with no estimate at all starts first. These pin:

* the order itself (unknown first, then the largest unit first -- a split table by its share
  per shard --, a tie in the order given, a table's reader shards together and in range
  order);
* where the estimates come from, the one size read, and that a failed read changes only the
  order;
* on the process path, that only the start order changed: the chunks, the recreate pre-pass
  and the units themselves are as before;
* the PostgreSQL size estimate's SQL, and the exporter's short read-only connection.

Doubles only, except the last test: it runs the size estimate on a real PostgreSQL when
``RUN_INTEGRATION_TESTS=1`` and ``DSQL_MIGRATOR_TEST_PG_DSN`` are set.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from types import SimpleNamespace

import pytest

from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.exporter import TableExporter
from dsql_migrator.core.introspector import ReadOnlySourceError
from dsql_migrator.core.models import SourceConnectionConfig, SourceType, TableDef
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    _largest_first,
    _load_order_estimates,
    _TableWorkerResult,
)

from tests.test_full_load_overlay import _InlineExecutor
from tests.test_g137_mysql_reader_shards import _FOUR, _Handle, _inputs, _table, _wm
from tests.test_source_dialect import _FakeEstimateConnection

_PG = SourceConnectionConfig(host="db", database="app", source_type=SourceType.POSTGRES)


def _units(*names, shards=None):
    """Work units as the planner builds them: ("table", t), or one ("shard", t, lo, hi, i)
    per range of a table named in ``shards`` (name -> ranges)."""
    units = []
    for name in names:
        table = _table(name)
        ranges = (shards or {}).get(name)
        if ranges:
            units += [("shard", table, lo, hi, i) for i, (lo, hi) in enumerate(ranges)]
        else:
            units.append(("table", table))
    return units


def _order(units):
    return [(u[1].name, u[4] if u[0] == "shard" else None) for u in units]


# ---------------------------------------------------------------------------
# 1. The order (pure)
# ---------------------------------------------------------------------------


def test_unknown_tables_start_first_then_the_largest() -> None:
    units = _units("a", "b", "c", "d", "e")
    ordered = _largest_first(units, {"a": 10, "b": 5000, "d": 300, "e": 0})
    assert _order(ordered) == [("c", None), ("b", None), ("d", None), ("a", None), ("e", None)]
    # A new list: the planner's own list (the pre-pass walks it) is left as it was.
    assert _order(units) == [(n, None) for n in "abcde"]


def test_a_tie_and_every_unknown_keep_the_order_the_tables_came_in() -> None:
    units = _units("x", "y", "z", "u", "v")
    ordered = _largest_first(units, {"x": 7, "y": 9, "z": 7})
    assert [n for n, _ in _order(ordered)] == ["u", "v", "y", "x", "z"]
    # No estimate at all: exactly the order given (today's order).
    assert _largest_first(units, {}) == units
    assert _largest_first([], {"x": 1}) == []


def test_a_tables_shards_stay_together_in_range_order_at_their_share() -> None:
    # "big" is split in four: 5,000 rows, 1,250 a shard. "huge" (9,000) and "mid" (2,000) are
    # one unit each: each is read by one process, longer than any of big's shards, so they
    # start before them -- by its whole size, big's four shards would take the slots first
    # and leave "mid" or "huge" to run alone at the end.
    units = _units("tiny", "big", "mid", "huge", shards={"big": _FOUR})
    ordered = _largest_first(units, {"tiny": 1, "big": 5000, "mid": 2000, "huge": 9000})
    assert _order(ordered) == [
        ("huge", None), ("mid", None), ("big", 0), ("big", 1), ("big", 2), ("big", 3),
        ("tiny", None)]
    # A share larger than any whole table: the shards first, still together and in order.
    larger = _largest_first(units, {"tiny": 1, "big": 40_000, "mid": 2000, "huge": 9000})
    assert _order(larger) == [
        ("big", 0), ("big", 1), ("big", 2), ("big", 3), ("huge", None), ("mid", None),
        ("tiny", None)]
    # A share equal to a whole table's size: a tie, in the order the tables came in.
    tie = _largest_first(units, {"tiny": 1, "big": 8000, "mid": 2000, "huge": 9000})
    assert _order(tie) == [
        ("huge", None), ("big", 0), ("big", 1), ("big", 2), ("big", 3), ("mid", None),
        ("tiny", None)]
    # Unknown, a split table still starts first, its ranges in order.
    unknown = _largest_first(units, {"tiny": 1, "mid": 2000, "huge": 9000})
    assert _order(unknown)[:4] == [("big", 0), ("big", 1), ("big", 2), ("big", 3)]


def test_what_counts_as_a_row_estimate() -> None:
    f = engine._is_row_estimate
    assert f(0) and f(5) and f(10**12)
    assert not any(f(v) for v in (None, -1, True, False, 1.5, "5"))


# ---------------------------------------------------------------------------
# 2. Where the estimates come from
# ---------------------------------------------------------------------------


class _Exporter:
    """The parent's planning reads: shard ranges per table, and the size read recorded."""

    def __init__(self, sizes=None, *, ranges=None, error=None):
        self.sizes = dict(sizes or {})
        self.ranges = dict(ranges or {})
        self.error = error
        self.size_reads: list = []

    def plan_pk_shard_ranges(self, conn, table, shards, *, min_rows=0):
        return list(self.ranges.get(table.name, [(None, None)]))

    def estimate_rows_from_size(self, conn, tables):
        self.size_reads.append((conn.source_type, list(tables)))
        if self.error is not None:
            raise self.error
        return {name: self.sizes.get(name) for name in tables}


def _migrator(*, source=SourceType.MYSQL, exporter=None):
    return SimpleNamespace(
        _inputs=SimpleNamespace(source_config=SourceConnectionConfig(
            host="db", database="app", source_type=source)),
        _exporter=exporter,
    )


def _tables(*names):
    return [TableDef(name=n, primary_key=["id"]) for n in names]


def test_mysql_orders_by_the_start_points_estimates_and_reads_nothing() -> None:
    exporter = _Exporter({"c": 99})
    beats: list = []
    estimates = _load_order_estimates(
        _migrator(exporter=exporter), _tables("a", "b", "c"),
        _wm(counts={"a": 10, "b": 0}), heartbeat=lambda: beats.append(1),
    )
    # A MySQL 0 is 0 rows; a table not in the start point has none. No size read.
    assert estimates == {"a": 10, "b": 0}
    assert exporter.size_reads == [] and beats == []
    # No start point at all: no estimate (the table starts first), and still no read.
    assert _load_order_estimates(_migrator(exporter=exporter), _tables("a"), None) == {}
    assert exporter.size_reads == []


def test_a_postgres_table_without_an_estimate_is_sized_in_one_read() -> None:
    exporter = _Exporter({"fresh": 7000, "added": 20, "gone": None})
    beats: list = []
    estimates = _load_order_estimates(
        _migrator(source=SourceType.POSTGRES, exporter=exporter),
        _tables("known", "fresh", "added", "gone", "clash"),
        # "fresh" was never analysed (reltuples -1 -> None); "added" and "gone" are not in
        # the start point; "clash" never starts (withheld).
        _wm(counts={"known": 100, "fresh": None}),
        skip={"clash"}, heartbeat=lambda: beats.append(1),
    )
    assert estimates == {"known": 100, "fresh": 7000, "added": 20}
    assert exporter.size_reads == [(SourceType.POSTGRES, ["fresh", "added", "gone"])]
    assert beats == [1]                       # the read is stamped, once
    # Every table estimated: nothing to read, nothing stamped.
    exporter.size_reads.clear()
    beats.clear()
    assert _load_order_estimates(
        _migrator(source=SourceType.POSTGRES, exporter=exporter), _tables("known"),
        _wm(counts={"known": 100}), heartbeat=lambda: beats.append(1),
    ) == {"known": 100}
    assert exporter.size_reads == [] and beats == []


def test_a_size_read_returning_junk_keeps_only_real_estimates() -> None:
    class _Junk(_Exporter):
        def estimate_rows_from_size(self, conn, tables):
            return {"a": -5, "b": True, "c": 3, "stranger": 9}

    assert _load_order_estimates(
        _migrator(source=SourceType.POSTGRES, exporter=_Junk()), _tables("a", "b", "c"),
        _wm(),
    ) == {"c": 3}


def test_a_failed_size_read_changes_only_the_order(caplog) -> None:
    exporter = _Exporter(error=RuntimeError("could not connect"))
    beats: list = []
    with caplog.at_level(logging.WARNING, logger=engine.__name__):
        estimates = _load_order_estimates(
            _migrator(source=SourceType.POSTGRES, exporter=exporter),
            _tables("known", "fresh"), _wm(counts={"known": 5, "fresh": None}),
            heartbeat=lambda: beats.append(1),
        )
    assert estimates == {"known": 5}          # "fresh" starts first, the run goes on
    assert beats == [1]
    assert "Could not read the size of 1 table(s) for the load order" in caplog.text
    # An exporter without the read (a test double): nothing read, nothing stamped.
    assert _load_order_estimates(
        _migrator(source=SourceType.POSTGRES, exporter=object()), _tables("fresh"),
        _wm(), heartbeat=lambda: beats.append(1),
    ) == {}
    assert beats == [1]


def test_a_read_only_guard_refusal_is_never_swallowed() -> None:
    exporter = _Exporter(error=ReadOnlySourceError("refused non-read statement"))
    with pytest.raises(ReadOnlySourceError):
        _load_order_estimates(
            _migrator(source=SourceType.POSTGRES, exporter=exporter), _tables("t"), _wm()
        )


# ---------------------------------------------------------------------------
# 3. The process path (workers inline, in the order they were submitted)
# ---------------------------------------------------------------------------


@pytest.fixture
def run(monkeypatch):
    """Run ``_migrate_tables_in_parallel`` on the process path with the workers inline."""
    started: list = []
    activity: list = []

    def _whole(args):
        started.append((args.table.name, None))
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=1)

    def _shard(args):
        started.append((args.table.name, args.shard_index))
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=1,
                                  shard_index=args.shard_index)

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", lambda m, d: (None, None, "snap-1"))
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity",
                        lambda cat, action, **kw: activity.append((action, kw.get("target"))))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")

    def _run(tables, *, counts=None, source=SourceType.MYSQL, cdc=False, exporter=None,
             replace=(), clash=(), fresh=True):
        started.clear()
        activity.clear()
        recreated: list = []
        exporter = exporter or _Exporter()
        migrator = BatchedTableMigrator(
            _inputs(tables, cdc=cdc, source=source, replace=replace),
            exporter=exporter, watermark_capturer=object(),
            importer_factory=lambda _i: SimpleNamespace(create_indexes=lambda d: (0, [])),
            table_recreator=lambda t: recreated.append(t.name) or [],
            target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(),
            target_unique_keys_reader=lambda t: [list(t.primary_key)],
        )
        if clash:
            migrator.bind_run_inputs(dataclasses.replace(
                migrator._inputs, name_clash_tables=frozenset(clash)))
        handle = _Handle([t.name for t in tables])
        counts_ = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, ErrorLogStore(),
            watermark=_wm(counts=counts), fresh_watermark=fresh,
        )
        return SimpleNamespace(
            started=list(started), recreated=recreated, exporter=exporter, counts=counts_,
            handle=handle, chunks=[c.chunk_id for c in handle.job.chunks],
            status={c.chunk_id: c.status for c in handle.job.chunks},
        )

    return _run


def test_the_pool_starts_the_largest_table_first_and_nothing_else_moves(run) -> None:
    names = ["app.categories", "app.order_items", "app.added", "app.orders", "app.empty"]
    tables = [_table(n) for n in names]
    r = run(tables, replace=names, counts={
        "app.categories": 1408, "app.order_items": 3_010_557, "app.orders": 1_005_382,
        "app.empty": 0})
    assert r.started == [("app.added", None), ("app.order_items", None),
                         ("app.orders", None), ("app.categories", None), ("app.empty", None)]
    # Only the start order changed: the job lists its tables as given, the serial recreate
    # pre-pass (before any unit starts) still walks them in that order, and every table loads.
    assert r.chunks == names and r.recreated == names
    assert set(r.status.values()) == {"DONE"} and r.counts == (0, 0, 0)
    assert r.exporter.size_reads == []        # MySQL: the start point's estimates only


def test_a_retry_orders_by_its_older_start_point_too(run) -> None:
    tables = [_table("app.small"), _table("app.large")]
    r = run(tables, counts={"app.small": 5, "app.large": 50_000}, fresh=False)
    assert r.started == [("app.large", None), ("app.small", None)]


def test_a_split_table_starts_its_ranges_together_at_their_share(run, monkeypatch) -> None:
    # MySQL Full load + CDC from a binlog start point, Reader shards 4: "app.events" (20,000)
    # is split in four ranges of 5,000; "app.text_key" (9,000) cannot be split, so one
    # process reads all of it -- it starts first, then the four ranges together.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    tables = [_table("app.small"), _table("app.events"),
              _table("app.text_key", lead="varchar(10)")]
    r = run(tables, cdc=True, exporter=_Exporter(ranges={"app.events": _FOUR}),
            counts={"app.small": 10, "app.events": 20_000, "app.text_key": 9000})
    assert r.started == [("app.text_key", None), ("app.events", 0), ("app.events", 1),
                         ("app.events", 2), ("app.events", 3), ("app.small", None)]
    assert r.status == {"app.small": "DONE", "app.events": "DONE", "app.text_key": "DONE"}


def test_a_never_analysed_postgres_table_is_ordered_by_its_size(run) -> None:
    tables = [_table("app.lookup"), _table("app.events"), _table("app.audit"),
              _table("app.unread")]
    exporter = _Exporter({"app.events": 2_000_000, "app.lookup": 12})
    r = run(tables, source=SourceType.POSTGRES, exporter=exporter, counts={
        "app.lookup": None, "app.events": None, "app.audit": 300_000})
    # One read, for the tables without an estimate; "app.unread" stays unknown and starts
    # first, the small never-analysed lookup table last.
    assert exporter.size_reads == [
        (SourceType.POSTGRES, ["app.lookup", "app.events", "app.unread"])]
    assert r.started == [("app.unread", None), ("app.events", None), ("app.audit", None),
                         ("app.lookup", None)]
    assert r.handle.heartbeats == 1           # the size read, stamped


def test_a_withheld_table_is_neither_sized_nor_started(run) -> None:
    tables = [_table("app.a"), _table("app.b"), _table("app.c")]
    exporter = _Exporter({"app.a": 1, "app.b": 2})
    r = run(tables, source=SourceType.POSTGRES, exporter=exporter, clash={"app.c"})
    assert exporter.size_reads == [(SourceType.POSTGRES, ["app.a", "app.b"])]
    assert r.started == [("app.b", None), ("app.a", None)]
    assert r.status["app.c"] == "FAILED"


def test_a_failed_size_read_still_loads_every_table(run) -> None:
    tables = [_table("app.a"), _table("app.b"), _table("app.c")]
    r = run(tables, source=SourceType.POSTGRES, counts={"app.a": 5, "app.b": None},
            exporter=_Exporter(error=RuntimeError("connection refused")))
    assert r.started == [("app.b", None), ("app.c", None), ("app.a", None)]
    assert set(r.status.values()) == {"DONE"}


# ---------------------------------------------------------------------------
# 4. The size estimate's SQL, and the exporter's read
# ---------------------------------------------------------------------------


def test_mysql_has_no_size_fallback_and_reads_nothing() -> None:
    conn = _FakeEstimateConnection("shop", [("shop", "orders", 5)])
    assert dialect_for(SourceType.MYSQL).estimate_rows_from_size(conn, ["orders"]) == {}
    assert conn.estimate_sql == ""


def test_postgres_estimates_rows_from_the_relation_size_and_column_widths() -> None:
    conn = _FakeEstimateConnection("public", [
        ("public", "fresh", 7000), ("public", "negative", -1), ("public", "nully", None),
        ("sales", "big", 10**10)])
    out = dialect_for(SourceType.POSTGRES).estimate_rows_from_size(
        conn, ["fresh", "negative", "nully", "absent", "sales.big"])
    assert out == {"fresh": 7000, "negative": None, "nully": None, "absent": None,
                   "sales.big": 10**10}
    sql = " ".join(conn.estimate_sql.split())
    # The table's size now (a partitioned parent: its leaves, none = 0) over a row width
    # from its column types: 28 bytes of overhead, a fixed width at its length, else 32.
    assert "pg_catalog.pg_relation_size(c.oid)" in sql
    assert ("coalesce(sum(pg_catalog.pg_relation_size(t.relid)), 0) FROM "
            "pg_catalog.pg_partition_tree(c.oid) t WHERE t.isleaf") in sql
    assert ("/ (28 + ( SELECT coalesce(sum(CASE WHEN a.attlen > 0 THEN a.attlen ELSE 32 END)"
            in sql)
    assert "a.attnum > 0 AND NOT a.attisdropped" in sql
    assert "c.relkind IN ('r', 'p')" in sql and "pg_namespace" in sql
    # Catalog only: no row of the table is read, and it is no reltuples read.
    assert "count(" not in sql.lower() and "reltuples" not in sql
    assert dialect_for(SourceType.POSTGRES).estimate_rows_from_size(conn, []) == {}


class _Conn:
    def __init__(self, log):
        self.log = log

    def __enter__(self):
        self.log.append("connect")
        return self

    def __exit__(self, *exc):
        self.log.append("close")
        return False

    def execution_options(self, **options):
        self.log.append(("options", options))
        return self


class _Engine:
    def __init__(self, log):
        self.log = log

    def connect(self):
        return _Conn(self.log)

    def dispose(self):
        self.log.append("dispose")


def test_the_exporter_reads_sizes_over_one_short_autocommit_connection(monkeypatch) -> None:
    log: list = []
    seen: list = []
    exporter = TableExporter(engine_factory=lambda conf: log.append(conf) or _Engine(log))
    pg = type(dialect_for(SourceType.POSTGRES))
    monkeypatch.setattr(pg, "estimate_rows_from_size",
                        lambda self, conn, names: seen.append((conn, names)) or {"t": 3})
    assert exporter.estimate_rows_from_size(_PG, ["t", "u"]) == {"t": 3}
    assert log[0] is _PG and log[1:] == [
        "connect", ("options", {"isolation_level": "AUTOCOMMIT"}), "close", "dispose"]
    assert seen[0][1] == ["t", "u"]
    # Nothing to size: no engine, no connection.
    log.clear()
    assert exporter.estimate_rows_from_size(_PG, []) == {}
    assert log == []


def test_the_exporter_bounds_the_reads_lock_waits_before_it_reads(monkeypatch) -> None:
    # Reading a table's size takes the ACCESS SHARE lock a SELECT takes: live, a table
    # another session held exclusively kept the read waiting for the whole Full Load read
    # timeout. The session's lock waits are bounded first, as the evaluation's catalog
    # reads are.
    from dsql_migrator.core.introspector import INTROSPECTION_LOCK_TIMEOUT_SECONDS

    log: list = []

    class _PgConn(_Conn):
        dialect = SimpleNamespace(name="postgresql")

        def execute(self, statement, *args):
            self.log.append(("execute", str(statement)))

    class _PgEngine(_Engine):
        def connect(self):
            return _PgConn(self.log)

    monkeypatch.setattr(type(dialect_for(SourceType.POSTGRES)), "estimate_rows_from_size",
                        lambda self, conn, names: log.append("read") or {"t": 3})
    exporter = TableExporter(engine_factory=lambda conf: _PgEngine(log))
    assert exporter.estimate_rows_from_size(_PG, ["t"]) == {"t": 3}
    assert log == [
        "connect", ("options", {"isolation_level": "AUTOCOMMIT"}),
        ("execute", f"SET lock_timeout = '{INTROSPECTION_LOCK_TIMEOUT_SECONDS}s'"),
        "read", "close", "dispose"]


def test_the_exporter_disposes_its_engine_when_the_read_fails(monkeypatch) -> None:
    log: list = []
    exporter = TableExporter(engine_factory=lambda conf: _Engine(log))

    def _boom(self, conn, names):
        raise RuntimeError("catalog read failed")

    monkeypatch.setattr(type(dialect_for(SourceType.POSTGRES)), "estimate_rows_from_size", _boom)
    with pytest.raises(RuntimeError):
        exporter.estimate_rows_from_size(_PG, ["t"])
    assert log[-1] == "dispose"


# ---------------------------------------------------------------------------
# 5. On a real PostgreSQL (opt-in)
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_INTEGRATION_TESTS") != "1",
    reason="set RUN_INTEGRATION_TESTS=1 (and provide infra) to run integration tests",
)
def test_the_size_estimate_runs_on_postgres_and_orders_never_analysed_tables() -> None:
    """The size estimate's SQL RUNS on PostgreSQL (the unit tests only string-match it, and
    the estimate helper swallows an execution error), through the read-only guard and the
    exporter, and orders tables that were never analysed by their real size.

    Requires ``RUN_INTEGRATION_TESTS=1`` and ``DSQL_MIGRATOR_TEST_PG_DSN``.
    """
    dsn = os.environ.get("DSQL_MIGRATOR_TEST_PG_DSN")
    if not dsn:
        pytest.skip("set DSQL_MIGRATOR_TEST_PG_DSN to a reachable PostgreSQL DSN")
    from sqlalchemy import create_engine, text

    from dsql_migrator.core.introspector import install_read_only_guard

    url = dsn if dsn.startswith("postgresql") else f"postgresql+psycopg://{dsn}"
    url = url.replace("postgresql://", "postgresql+psycopg://", 1)
    try:
        admin_engine = create_engine(url)
        admin = admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT")
    except Exception as exc:  # noqa: BLE001 - environment not ready -> skip
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    def _guarded(_conf):
        built = create_engine(url)
        install_read_only_guard(built)
        return built

    s = "r2_size_it"
    try:
        admin.execute(text(f"DROP SCHEMA IF EXISTS {s} CASCADE"))
        admin.execute(text(f"CREATE SCHEMA {s}"))
        for ddl in (
            f"CREATE TABLE {s}.big (id bigint PRIMARY KEY, a int, b text, c timestamptz)",
            f"INSERT INTO {s}.big SELECT g, g % 7, 'v' || g, now() "
            "FROM generate_series(1, 300000) g",
            f"CREATE TABLE {s}.mid (id bigint PRIMARY KEY, gone text, v int)",
            f"ALTER TABLE {s}.mid DROP COLUMN gone",
            f"INSERT INTO {s}.mid SELECT g, g FROM generate_series(1, 30000) g",
            f'CREATE TABLE {s}."Small Lookup" (id int PRIMARY KEY, name text)',
            f'INSERT INTO {s}."Small Lookup" SELECT g, \'n\' || g FROM generate_series(1, 10) g',
            f"CREATE TABLE {s}.empty (id int PRIMARY KEY)",
            f"CREATE TABLE {s}.parted (id bigint, v text) PARTITION BY RANGE (id)",
            f"CREATE TABLE {s}.p1 PARTITION OF {s}.parted FOR VALUES FROM (0) TO (75001)",
            f"CREATE TABLE {s}.p2 PARTITION OF {s}.parted FOR VALUES FROM (75001) TO (MAXVALUE)",
            f"INSERT INTO {s}.parted SELECT g, 'v' || g FROM generate_series(1, 150000) g",
            f"CREATE TABLE {s}.no_parts (id bigint) PARTITION BY RANGE (id)",
        ):
            admin.execute(text(ddl))
        names = [f"{s}.big", f"{s}.mid", f"{s}.Small Lookup", f"{s}.empty", f"{s}.parted",
                 f"{s}.no_parts", f"{s}.missing"]

        sizes = TableExporter(engine_factory=_guarded).estimate_rows_from_size(_PG, names)
        exact = {n: admin.execute(text(f'SELECT count(*) FROM {s}."{n.split(".", 1)[1]}"'))
                 .scalar() for n in names[:5]}
        for name in (f"{s}.big", f"{s}.mid", f"{s}.parted"):
            assert 0.4 * exact[name] <= sizes[name] <= 2.5 * exact[name], (name, sizes, exact)
        assert sizes[f"{s}.Small Lookup"] < 1000          # one page, not EXPLAIN's 10
        assert sizes[f"{s}.empty"] == 0 and sizes[f"{s}.no_parts"] == 0
        assert sizes[f"{s}.missing"] is None

        # The whole estimate path as a run orders these tables: the start point's reltuples
        # (-1 for a table never analysed, unless autovacuum got there first; "mid" is
        # analysed here, so row counts from both sources are ordered together), the size
        # read for the rest, and the one table the catalog does not have first.
        admin.execute(text(f"ANALYZE {s}.mid"))
        estimates = dialect_for(SourceType.POSTGRES).estimate_row_counts(admin, names)
        assert estimates[f"{s}.mid"] == exact[f"{s}.mid"]
        tables = [TableDef(name=n, primary_key=["id"]) for n in names]
        migrator = SimpleNamespace(
            _inputs=SimpleNamespace(source_config=_PG),
            _exporter=TableExporter(engine_factory=_guarded),
        )
        order = _load_order_estimates(migrator, tables, _wm(counts=estimates))
        ordered = [u[1].name for u in _largest_first([("table", t) for t in tables], order)]
        assert ordered == [f"{s}.missing", f"{s}.big", f"{s}.parted", f"{s}.mid",
                           f"{s}.Small Lookup", f"{s}.empty", f"{s}.no_parts"], (ordered, order)

        # A table another session holds exclusively (a running ALTER TABLE or VACUUM FULL):
        # the read waits at most the lock bound, then estimates nothing (those tables start
        # first). The engine's own 30 s statement timeout stands in for the Full Load read
        # timeout, which the wait otherwise ran into.
        import time

        from dsql_migrator.core.introspector import INTROSPECTION_LOCK_TIMEOUT_SECONDS

        def _guarded_with_timeout(_conf):
            built = create_engine(url, connect_args={"options": "-c statement_timeout=30000"})
            install_read_only_guard(built)
            return built

        holder_engine = create_engine(url)
        holder = holder_engine.connect()
        try:
            holder.execute(text(f"LOCK TABLE {s}.mid IN ACCESS EXCLUSIVE MODE"))
            started = time.monotonic()
            locked = TableExporter(engine_factory=_guarded_with_timeout).estimate_rows_from_size(
                _PG, names)
            waited = time.monotonic() - started
        finally:
            holder.rollback()
            holder.close()
            holder_engine.dispose()
        assert waited < INTROSPECTION_LOCK_TIMEOUT_SECONDS + 10, waited
        assert set(locked.values()) == {None}, locked
    finally:
        admin.execute(text(f"DROP SCHEMA IF EXISTS {s} CASCADE"))
        admin.close()
        admin_engine.dispose()
