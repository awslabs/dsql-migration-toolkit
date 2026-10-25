# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P1: a large table is read in key ranges by default, and a big table's ranges run side by side.

Measured live (Seoul, 2026-10-25): one key range of an Aurora DSQL table takes about 20K rows/s
on a clean cluster, and a 10M-row table read in 4 ranges reached 39.6K rows/s overall (54K at
its peak). So Reader shards per large table is now 0 = automatic by default (a table of at
least 5M estimated rows is read in one range per 2.5M of them, at most 32), 1 = off and 2-32 =
exactly that many; the process path no longer clamps it to ``32 // Tables in parallel`` (each
running unit holds one source connection, so the pool bounds the source readers). A MySQL
automatic split is always ONE reader session (P2), never parts from separate snapshots (G137);
a PostgreSQL table sharing the run's snapshot is read by as many shards as its share of the
run's estimated rows (``_unit_counts``), each a contiguous block of its ranges in turn, and the
anchor that holds the snapshot is pinged within the source's idle-in-transaction limit and
closed once the last shard reading it returned. Pinned here:

* the rules (pure): ``_auto_reader_ranges`` and ``_unit_counts``, an exhaustive grid;
* the planner on the inline process pool, with no Reader shards set (automatic);
* the shard worker reading a block of ranges through one session from the shared snapshot,
  with table-wide range ids, and keeping its resume point across a re-read;
* the anchor's keepalive interval and its early close;
* the perf harness's ``--reader-shards`` and report, and the Fargate script's variants.

Doubles only: no MySQL, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import concurrent.futures
import dataclasses
import itertools
import json
import os
import shutil
import subprocess
import sys
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.load_stats import LoadStats, LoadStatsBoard
from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TargetConnectionConfig,
)
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    DataMigrationInputs,
    _TableWorkerResult,
)
from dsql_migrator.ui.data_migration._models import mysql_reads_may_split, split_read_tables

from tests.test_full_load_overlay import _InlineExecutor
from tests.test_g137_mysql_reader_shards import (
    _Handle,
    _IndexImporter,
    _PgConnectionLost,
    _inputs,
    _table,
    _wm,
)
from tests.test_measure_performance import _args, _command_doubles, _job, mp  # noqa: F401
from tests.test_p2_one_session_ranges import _PagesImporter, _events, _range_turns, _THREE

_READER_SHARDS = "DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS"
_M = 1_000_000


@pytest.fixture(autouse=True)
def _automatic(monkeypatch):
    """Every test starts with Reader shards unset (the default, automatic) and gets back
    whatever was set before it -- also when the harness's command sets it (os.environ)."""
    monkeypatch.setenv(_READER_SHARDS, "")  # records the value before the test ...
    monkeypatch.delenv(_READER_SHARDS)      # ... which the test then runs without


# ---------------------------------------------------------------------------
# 1. The rules (pure)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("estimate, ranges", [
    (None, 1), (0, 1), (2 * _M, 1), (4_900_000, 1), (4_999_999, 1),
    (5 * _M, 2), (7_499_999, 2), (10 * _M, 4), (20 * _M, 8),
    (80 * _M, 32), (1_000 * _M, 32),
    (True, 1), (-1, 1), ("10000000", 1),      # not a row estimate
])
def test_automatic_ranges_follow_the_estimate(estimate, ranges) -> None:
    assert engine._auto_reader_ranges(estimate, 1 * _M) == ranges


def test_automatic_ranges_need_the_shard_minimum_too() -> None:
    assert engine._auto_reader_ranges(10 * _M, 20 * _M) == 1
    assert engine._auto_reader_ranges(20 * _M, 20 * _M) == 8
    # A minimum below 5M does not lower the automatic floor.
    assert engine._auto_reader_ranges(4 * _M, 1) == 1


def _names(prefix: str, n: int) -> list:
    return [f"{prefix}{i:03d}" for i in range(n)]


def test_unit_counts_spread_over_tables_first() -> None:
    f = engine._unit_counts
    # One 100M table alone at 16 processes: all 16 (its K is 32).
    assert f({"big": 32}, {"big": 100 * _M}, 16, 0) == {"big": 16}
    # Sixteen 20M tables at 16 processes: one each, side by side.
    sixteen = _names("t", 16)
    assert f({n: 8 for n in sixteen}, {n: 20 * _M for n in sixteen}, 16, 0) == {
        n: 1 for n in sixteen}
    # Two 100M tables at 4: two each.
    assert f({"a": 32, "b": 32}, {"a": 100 * _M, "b": 100 * _M}, 4, 0) == {"a": 2, "b": 2}
    # Three 10M tables at 4: 4/3 each rounds to 1, the idle slot goes to the first by name.
    assert f({"c": 4, "a": 4, "b": 4}, {"a": 10 * _M, "b": 10 * _M, "c": 10 * _M}, 4, 0) == {
        "a": 2, "b": 1, "c": 1}
    # K caps it: a 10M table (4 ranges) alone at 16 gets 4, not 16.
    assert f({"t": 4}, {"t": 10 * _M}, 16, 0) == {"t": 4}
    # No estimate in the run: one each.
    assert f({"a": 4, "b": 8}, {}, 16, 0) == {"a": 1, "b": 1}
    # One 100M table among 200 other units of 1M at 4: its share (4/3) and no idle slot.
    others = {n: 1 * _M for n in _names("o", 200)}
    assert f({"big": 32}, {"big": 100 * _M, **others}, 4, 200) == {"big": 1}


def test_unit_counts_the_largest_share_takes_the_idle_slots() -> None:
    f = engine._unit_counts
    # 60M and 20M at 4: 3 and 1 by their shares, nothing idle.
    assert f({"a": 24, "b": 8}, {"a": 60 * _M, "b": 20 * _M}, 4, 0) == {"a": 3, "b": 1}
    # 60M and 20M at 8 beside two other units: 6 + 2 = 8 already, the others queue.
    assert f({"a": 24, "b": 8}, {"a": 60 * _M, "b": 20 * _M, "x": 1}, 8, 2) == {
        "a": 6, "b": 2}
    # 30M (12 ranges) and 10M (4 ranges) at 16, nothing else: 12 and 4, the rest idle.
    assert f({"a": 12, "b": 4}, {"a": 30 * _M, "b": 10 * _M}, 16, 0) == {"a": 12, "b": 4}


def test_unit_counts_hold_their_bounds_over_a_grid() -> None:
    # Exhaustive (no hypothesis in the venv): 1 <= G <= min(K, tp) always, and an idle slot is
    # left only once every table has as many shards as it may take.
    sizes = (5 * _M, 10 * _M, 100 * _M)
    for tp in (1, 2, 3, 4, 7, 16):
        for count in (1, 2, 3):
            for picked in itertools.product(sizes, repeat=count):
                for other in (0, 1, 5):
                    names = [f"t{i}" for i in range(count)]
                    estimates = {n: s for n, s in zip(names, picked)}
                    if other:
                        estimates["other"] = 1 * _M
                    k = {n: engine._auto_reader_ranges(estimates[n], 1) for n in names}
                    got = engine._unit_counts(k, estimates, tp, other)
                    assert set(got) == set(names)
                    for n in names:
                        assert 1 <= got[n] <= min(k[n], tp), (tp, picked, other, got)
                    if sum(got.values()) + other < tp:
                        assert all(got[n] == min(k[n], tp) for n in names), (tp, picked, got)


# ---------------------------------------------------------------------------
# 2. The planner, on the inline process pool, with Reader shards automatic
# ---------------------------------------------------------------------------


class _KPlanner:
    """The planning read: ``shards`` equal ranges of the leading key over [0, 1,000,000),
    every call recorded as (table, shards, min_rows)."""

    def __init__(self) -> None:
        self.calls: list = []

    def plan_pk_shard_ranges(self, conn, table, shards, *, min_rows=0):
        self.calls.append((table.name, shards, min_rows))
        bounds = [None] + [i * _M // shards for i in range(1, shards)] + [None]
        return list(zip(bounds, bounds[1:]))


@pytest.fixture
def spread(monkeypatch):
    """``_migrate_tables_in_parallel`` on the process path, the workers inline and recorded;
    Tables in parallel 4, Reader shards unset."""
    seen = SimpleNamespace(shards=[], wholes=[], activity=[], anchors=0, unique=[])

    def _shard(args):
        seen.shards.append(args)
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=10,
                                  shard_index=args.shard_index)

    def _whole(args):
        seen.wholes.append(args)
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=40)

    def _open(migrator, dialect):
        seen.anchors += 1
        return None, None, "snap-1"

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(_InlineExecutor, "submitted", [])
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", _open)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity", lambda cat, action, **kw: seen.activity.append(
        (action, kw.get("target"), kw.get("detail"))))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")

    def run(tables, counts, *, source=SourceType.MYSQL, cdc=False, binlog=True,
            coexisting=False, replace=()):
        for bucket in (seen.shards, seen.wholes, seen.activity, seen.unique):
            bucket.clear()
        seen.anchors = 0
        inputs = _inputs(tables, cdc=cdc, source=source, replace=replace)
        if coexisting:
            inputs = dataclasses.replace(inputs, cdc_coexisting=True)
        planner = _KPlanner()

        def _unique(table):
            seen.unique.append(table.name)
            return [list(table.primary_key)]

        migrator = BatchedTableMigrator(
            inputs, exporter=planner, watermark_capturer=object(),
            importer_factory=lambda _i: _IndexImporter(), table_recreator=lambda t: [],
            target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(), target_unique_keys_reader=_unique,
        )
        handle = _Handle([t.name for t in tables])
        watermark = _wm(binlog=binlog, counts=counts)
        handle.job.watermark = watermark       # what split_read_tables reads the record by
        counts_ = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, ErrorLogStore(), watermark=watermark,
            fresh_watermark=True,
        )
        return SimpleNamespace(
            counts=counts_, planned=list(planner.calls), shards=list(seen.shards),
            wholes={a.table.name: a for a in seen.wholes}, anchors=seen.anchors,
            lines=[(t, d) for a, t, d in seen.activity if a == "reader shards"],
            unique=list(seen.unique), job=handle.job,
            parts={c.chunk_id: c.reader_shards for c in handle.job.chunks},
            status={c.chunk_id: c.status for c in handle.job.chunks},
        )

    return run


BIG = "app.events"


def test_a_mysql_full_load_only_table_of_10m_rows_is_read_in_4_ranges_by_one_session(
        spread) -> None:
    r = spread([_table(BIG)], {BIG: 10 * _M})
    # The estimate decided: the planning read is the leading key's MIN and MAX only.
    assert r.planned == [(BIG, 4, 0)]
    assert r.shards == [] and len(r.wholes[BIG].read_ranges) == 4
    assert r.parts == {BIG: None}                     # one snapshot: no G137 record
    assert r.lines == [(BIG, engine._SPLIT_ONE_SNAPSHOT.format(k=4))]
    assert r.unique == []                             # no unique-key guard for it
    assert r.wholes[BIG].in_flight == engine._unit_in_flight(8, 4, 128) == 8
    assert r.status == {BIG: "DONE"}


def test_a_mysql_full_load_and_cdc_table_split_automatically_keeps_every_route_open(
        spread) -> None:
    # Full load + CDC from a binlog start point: an automatic split is still one session
    # and one snapshot (P2), never parts from separate snapshots (G137).
    r = spread([_table(BIG)], {BIG: 10 * _M}, cdc=True)
    assert r.planned == [(BIG, 4, 0)] and r.shards == []
    assert len(r.wholes[BIG].read_ranges) == 4
    assert r.parts == {BIG: None} and split_read_tables(r.job) == []
    assert r.lines == [(BIG, engine._SPLIT_ONE_SNAPSHOT.format(k=4))]
    assert r.unique == []
    # ... and with the default setting the load-then-untick route carves nothing out.
    assert mysql_reads_may_split(SourceType.MYSQL) is False


def test_a_table_below_5m_rows_or_with_no_estimate_is_read_whole_with_no_planning_read(
        spread) -> None:
    r = spread([_table(BIG), _table("app.unknown"), _table("app.empty")],
               {BIG: 3 * _M, "app.empty": 0})
    assert r.planned == [] and r.shards == [] and r.lines == []
    assert {name: unit.read_ranges for name, unit in r.wholes.items()} == {
        BIG: (), "app.unknown": (), "app.empty": ()}


def test_the_shard_minimum_raises_the_automatic_floor(spread, monkeypatch) -> None:
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", str(20 * _M))
    r = spread([_table(BIG)], {BIG: 10 * _M})
    assert r.planned == [] and r.wholes[BIG].read_ranges == ()


def test_an_explicit_value_keeps_the_g137_parts(spread, monkeypatch) -> None:
    monkeypatch.setenv(_READER_SHARDS, "4")
    r = spread([_table(BIG)], {BIG: 10 * _M}, cdc=True)
    assert r.planned == [(BIG, 4, 1 * _M)]           # an explicit value: the shard minimum
    assert [(a.shard_index, a.shared_snapshot_id, a.ranges) for a in r.shards] == [
        (i, None, ()) for i in range(4)]
    assert r.parts == {BIG: 4} and split_read_tables(r.job) == [BIG]
    assert r.lines == [(BIG, engine._SPLIT_OWN_SNAPSHOTS.format(k=4))]
    assert r.unique == [BIG]                          # the append's unique-key guard
    assert mysql_reads_may_split(SourceType.MYSQL) is True
    # Reader shards 1: off.
    monkeypatch.setenv(_READER_SHARDS, "1")
    off = spread([_table(BIG)], {BIG: 100 * _M}, cdc=True)
    assert off.planned == [] and off.wholes[BIG].read_ranges == ()


def test_a_load_while_cdc_streams_splits_only_with_an_explicit_value(spread, monkeypatch) -> None:
    r = spread([_table(BIG)], {BIG: 100 * _M}, coexisting=True)
    assert r.planned == [] and r.wholes[BIG].read_ranges == ()
    # PostgreSQL with an explicit value: one range per shard on the shared snapshot, as
    # before (no share of the run: a load while CDC streams keeps its shape).
    monkeypatch.setenv(_READER_SHARDS, "6")
    pg = spread([_table(BIG)], {BIG: 100 * _M}, source=SourceType.POSTGRES, binlog=False,
                coexisting=True)
    assert [(a.shard_index, len(a.ranges)) for a in pg.shards] == [(i, 0) for i in range(6)]
    assert {a.shared_snapshot_id for a in pg.shards} == {"snap-1"}


def test_a_postgresql_table_of_10m_rows_alone_gets_four_shards_of_one_range(spread) -> None:
    r = spread([_table(BIG)], {BIG: 10 * _M}, source=SourceType.POSTGRES, binlog=False)
    assert r.planned == [(BIG, 4, 0)] and r.wholes == {}
    bounds = [None, 250_000, 500_000, 750_000, None]
    assert [(a.shard_index, a.ranges, a.range_base, a.pk_lower, a.pk_upper)
            for a in r.shards] == [
        (i, ((bounds[i], bounds[i + 1]),), i, bounds[i], bounds[i + 1]) for i in range(4)]
    assert {a.shared_snapshot_id for a in r.shards} == {"snap-1"} and r.anchors == 1
    assert {a.shard_count for a in r.shards} == {4}
    assert {a.in_flight for a in r.shards} == {engine._unit_in_flight(8, 1, 32)} == {4}
    assert r.lines == [(BIG, engine._SPLIT_SHARED_SNAPSHOT.format(k=4))]
    assert r.parts == {BIG: 4} and r.status == {BIG: "DONE"}


def test_a_postgresql_table_of_100m_rows_alone_gets_four_shards_of_eight_ranges(spread) -> None:
    r = spread([_table(BIG)], {BIG: 100 * _M}, source=SourceType.POSTGRES, binlog=False)
    assert r.planned == [(BIG, 32, 0)]
    assert [(a.shard_index, a.range_base, len(a.ranges)) for a in r.shards] == [
        (0, 0, 8), (1, 8, 8), (2, 16, 8), (3, 24, 8)]
    # Contiguous blocks that cover the table once, each unit spanning its block.
    ranges = [rng for a in r.shards for rng in a.ranges]
    assert ranges == _KPlanner().plan_pk_shard_ranges(None, _table(BIG), 32)
    assert all((a.pk_lower, a.pk_upper) == (a.ranges[0][0], a.ranges[-1][1]) for a in r.shards)
    # At most 4 batches in flight per range of its block, Batches per table (8) binding.
    assert {a.in_flight for a in r.shards} == {engine._unit_in_flight(8, 8, 32)} == {8}
    assert {a.max_quarantine_records for a in r.shards} == {engine.MAX_QUARANTINE_RECORDS // 4}
    assert r.lines == [(BIG, engine._SPLIT_SHARED_SNAPSHOT_BLOCKS.format(k=32, g=4))]
    assert r.lines[0][1] == "read in 32 primary-key ranges by 4 readers sharing one snapshot"
    assert r.parts == {BIG: 4} and r.anchors == 1


def test_two_postgresql_tables_of_100m_rows_share_the_pool(spread) -> None:
    tables = [_table("app.a"), _table("app.b")]
    r = spread(tables, {"app.a": 100 * _M, "app.b": 100 * _M}, source=SourceType.POSTGRES,
               binlog=False)
    assert [(a.table.name, a.shard_index, a.range_base, len(a.ranges)) for a in r.shards] == [
        ("app.a", 0, 0, 16), ("app.a", 1, 16, 16), ("app.b", 0, 0, 16), ("app.b", 1, 16, 16)]
    assert r.parts == {"app.a": 2, "app.b": 2} and r.anchors == 1


def test_three_postgresql_tables_of_10m_rows_take_two_one_and_one(spread) -> None:
    tables = [_table("app.c"), _table("app.a"), _table("app.b")]
    r = spread(tables, {n: 10 * _M for n in ("app.a", "app.b", "app.c")},
               source=SourceType.POSTGRES, binlog=False)
    # The idle slot goes to the first by name: two shards of two ranges each.
    assert [(a.table.name, a.range_base, len(a.ranges)) for a in r.shards] == [
        ("app.a", 0, 2), ("app.a", 2, 2)]
    # The other two: one reader session each, its own snapshot, its four ranges in turn.
    assert {name: len(unit.read_ranges) for name, unit in r.wholes.items()} == {
        "app.b": 4, "app.c": 4}
    assert sorted(r.lines) == sorted([
        ("app.a", engine._SPLIT_SHARED_SNAPSHOT_BLOCKS.format(k=4, g=2)),
        ("app.b", engine._SPLIT_ONE_SNAPSHOT.format(k=4)),
        ("app.c", engine._SPLIT_ONE_SNAPSHOT.format(k=4)),
    ])
    assert r.parts == {"app.a": 2, "app.b": None, "app.c": None}


def test_a_postgresql_table_with_a_small_share_is_one_session_and_opens_no_anchor(spread) -> None:
    text_key = [_table(f"app.t{i}", lead="varchar(10)") for i in range(3)]
    r = spread([_table(BIG), *text_key], {BIG: 10 * _M, **{t.name: 20 * _M for t in text_key}},
               source=SourceType.POSTGRES, binlog=False)
    # 10M of 70M at 4 processes beside 3 other units: one shard, read as one session.
    assert r.shards == [] and r.anchors == 0
    assert len(r.wholes[BIG].read_ranges) == 4
    assert r.lines == [(BIG, engine._SPLIT_ONE_SNAPSHOT.format(k=4))]
    assert r.parts[BIG] is None


def test_the_largest_shards_start_first(spread) -> None:
    # R2 is unchanged: a split table goes by its share per shard, its shards together.
    tables = [_table("app.small", lead="varchar(10)"), _table(BIG)]
    r = spread(tables, {"app.small": 30 * _M, BIG: 100 * _M}, source=SourceType.POSTGRES,
               binlog=False)
    # 100M of 130M at 4 beside one other unit: 3 shards (33M each), then the 30M table.
    assert _InlineExecutor.submitted == [BIG, BIG, BIG, "app.small"]
    assert [len(a.ranges) for a in r.shards] == [10, 11, 11]


def test_the_thread_path_reads_every_table_with_one_reader(spread, monkeypatch) -> None:
    # Tables in parallel 1: the load runs in the app's own process; no planning read.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    loaded: list = []
    monkeypatch.setattr(engine, "_migrate_one_table", lambda handle, job_id, table, *a, **k: (
        loaded.append(table.name) or engine._TableLoadOutcome.LOADED))
    r = spread([_table(BIG)], {BIG: 100 * _M})
    assert loaded == [BIG] and r.planned == [] and r.wholes == {} and r.shards == []


# ---------------------------------------------------------------------------
# 3. The shard worker: a block of ranges, one session, the shared snapshot
# ---------------------------------------------------------------------------


class _BlockExporter:
    """A PostgreSQL shard's exporter: 3-row pages of ``rows`` per range, a page of each in
    turn; ``lose_on`` (call number -> page number) loses the source connection there."""

    def __init__(self, rows, *, lose_on=None) -> None:
        self.rows = rows
        self.lose_on = dict(lose_on or {})
        self.calls: list = []

    def stream_converted_ranges(self, conn, table, ranges, *, page_rows, **kwargs):
        self.calls.append(SimpleNamespace(ranges=list(ranges), page_rows=page_rows, **kwargs))
        call = len(self.calls)

        def _pages():
            for number, turn in enumerate(_range_turns(self.rows, list(ranges), 3), start=1):
                if self.lose_on.get(call) == number:
                    raise _PgConnectionLost("server closed the connection")
                yield turn

        return _pages()

    def stream_converted_rows(self, *_a, **_k):
        raise AssertionError("a shard's block of ranges is read by one session, in turn")


class _Queue:
    def __init__(self) -> None:
        self.sent: list = []

    def put_nowait(self, message) -> None:
        self.sent.append(message)


_PG = SourceConnectionConfig(host="db", database="app", source_type=SourceType.POSTGRES)
# The table's last two of four ranges: the block of shard 1 of 2.
_BLOCK = ((100, 200), (200, None))


def _run_block_shard(monkeypatch, exporter, importer, *, ranges=_BLOCK, base=2, in_flight=8):
    class _ShardMigrator(BatchedTableMigrator):  # the real class, imported before any patch
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=exporter, watermark_capturer=object(),
                importer_factory=lambda _i: importer, table_recreator=lambda t: [],
                target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
            )

    queue = _Queue()
    monkeypatch.setattr(engine, "BatchedTableMigrator", _ShardMigrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", queue)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS", "0")
    table = _events()
    inputs = DataMigrationInputs(
        source_config=_PG, source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=[table]),
    )
    result = engine._migrate_shard_in_process(engine._ShardWorkerArgs(
        job_id="j", table=table, inputs=inputs, pk_lower=ranges[0][0],
        pk_upper=ranges[-1][1], shard_index=1, shared_snapshot_id="snap-1", shard_count=2,
        in_flight=in_flight, ranges=ranges, range_base=base,
    ))
    return result, queue


def test_a_shard_reads_its_block_through_one_session_on_the_shared_snapshot(monkeypatch):
    exporter, importer = _BlockExporter(_THREE), _PagesImporter()
    result, _queue = _run_block_shard(monkeypatch, exporter, importer)
    assert result.status == "DONE" and result.wrote_nothing is False
    (call,) = exporter.calls
    assert call.ranges == list(_BLOCK) and call.shared_snapshot_id == "snap-1"
    # Pages of whole batches, at most a range's share of the 8 in flight (4 each).
    assert call.page_rows == engine._range_page_rows(2000, 2, 8 // 2) == 4000
    assert callable(call.on_zero_dates) and callable(call.should_cancel)
    # Table-wide range ids: the block's first range is the table's third (base 2).
    (load,) = importer.calls
    assert [index for index, _ids in load.pages] == [2, 3, 3, 3, 3]
    assert [ids for _index, ids in load.pages][0] == [100, 101, 102]
    assert result.rows_loaded == 13
    assert (result.load_stats["ranges"], result.load_stats["in_flight_cap"]) == (2, 8)


def test_a_blocks_page_holds_at_most_a_ranges_share_of_the_batches_in_flight(monkeypatch):
    # Two batches in flight over the block's two ranges: one batch (2,000 rows) a page, so the
    # batches in flight come from both ranges -- not the 4,000-row page of two batches the
    # reader's 5,000-row page alone would give (as migrate_table sizes a one-session table's).
    exporter = _BlockExporter(_THREE)
    result, _queue = _run_block_shard(monkeypatch, exporter, _PagesImporter(), in_flight=2)
    assert result.status == "DONE"
    (call,) = exporter.calls
    assert call.page_rows == engine._range_page_rows(2000, 2, 2 // 2) == 2000
    assert result.load_stats["in_flight_cap"] == 2


def test_a_shard_re_reading_its_block_imports_the_snapshot_again_and_keeps_its_resume_point(
        monkeypatch):
    # The read loses its source connection when its second page is asked for.
    exporter, importer = _BlockExporter(_THREE, lose_on={1: 2}), _PagesImporter()
    result, queue = _run_block_shard(monkeypatch, exporter, importer)
    assert result.status == "DONE"
    assert [(c.ranges, c.shared_snapshot_id) for c in exporter.calls] == [
        (list(_BLOCK), "snap-1")] * 2
    # The first attempt committed range 2's first page; the re-read of the SAME snapshot
    # starts from it (the ordinals still name the same rows), keyed table-wide.
    assert [c.start for c in importer.calls] == [{}, {"2": 1}]
    rereads = [m for m in queue.sent if isinstance(m, tuple) and m[0] == engine._SOURCE_REREAD]
    assert len(rereads) == 1 and rereads[0][5] == 1 and rereads[0][6] is True


def test_a_shard_of_one_range_reads_as_before(monkeypatch):
    class _RowsExporter:
        def __init__(self) -> None:
            self.calls: list = []

        def stream_converted_rows(self, conn, table, **kwargs):
            self.calls.append(kwargs)
            return iter([{"id": 150, "v": "a"}])

        def stream_converted_ranges(self, *_a, **_k):
            raise AssertionError("one range is one row stream")

    from tests.test_ui_data_migration import _FakeImporter

    exporter, importer = _RowsExporter(), _FakeImporter()
    result, _queue = _run_block_shard(monkeypatch, exporter, importer,
                                      ranges=((100, 200),), base=1)
    assert result.status == "DONE"
    ((kwargs),) = exporter.calls
    assert (kwargs["pk_lower"], kwargs["pk_upper"]) == (100, 200)
    assert kwargs["shared_snapshot_id"] == "snap-1"
    assert result.load_stats["ranges"] == 1


# ---------------------------------------------------------------------------
# 4. The anchor: its keepalive interval and its early close
# ---------------------------------------------------------------------------


class _Anchor:
    """The anchor session: answers the idle-in-transaction read with ``setting`` (or raises
    ``error``) and records every statement."""

    def __init__(self, setting="0", error=None) -> None:
        self.setting = setting
        self.error = error
        self.sql: list = []

    def exec_driver_sql(self, sql):
        self.sql.append(sql)
        if self.error is not None and sql != "SELECT 1":
            raise self.error
        return SimpleNamespace(scalar=lambda: self.setting)


@pytest.mark.parametrize("setting, seconds", [
    ("30000", 10.0),        # a 30 s limit: every 10 s
    ("0", 60.0),            # no limit
    ("600000", 60.0),       # a 10 min limit: still every 60 s
    ("6000", 5.0),          # a 6 s limit: no more often than every 5 s
    (None, 60.0),           # no row
    ("junk", 60.0),
])
def test_the_keepalive_interval_is_a_third_of_the_idle_limit(setting, seconds) -> None:
    anchor = _Anchor(setting)
    assert engine._anchor_keepalive_seconds(anchor) == seconds
    assert anchor.sql == [engine._ANCHOR_IDLE_TIMEOUT_SQL]
    assert "pg_settings" in anchor.sql[0] and "idle_in_transaction_session_timeout" in anchor.sql[0]


def test_an_unreadable_idle_limit_or_no_anchor_keeps_the_60_s_ping() -> None:
    assert engine._anchor_keepalive_seconds(_Anchor(error=RuntimeError("down"))) == 60.0
    assert engine._anchor_keepalive_seconds(None) == 60.0


class _HeldWholeExecutor(_InlineExecutor):
    """Runs a shard at once; holds a whole-table unit until :meth:`release` (at the latest
    after a few seconds, so a regression fails instead of hanging)."""

    held: list = []

    def submit(self, fn, *args):
        if isinstance(args[0], engine._TableWorkerArgs):
            future = concurrent.futures.Future()
            _HeldWholeExecutor.held.append((future, fn, args))
            return future
        return super().submit(fn, *args)

    @classmethod
    def release(cls) -> None:
        for future, fn, args in cls.held:
            if not future.done():
                future.set_result(fn(*args))


def _anchor_run(monkeypatch, *, interval=None, close_error=None):
    """A PostgreSQL run: a 10M table read in 4 shards on the shared snapshot, and a text-key
    table one process reads whole, which is still loading when the shards are done.
    ``close_error``: what closing the anchor raises."""
    anchor = _Anchor("30000")
    closes: list = []

    def _close(engine_, conn):
        held_pending = any(not f.done() for f, _fn, _a in _HeldWholeExecutor.held)
        closes.append((conn, held_pending, list(anchor.sql)))
        if conn is not None:
            _HeldWholeExecutor.release()
            if close_error is not None:
                raise close_error

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _HeldWholeExecutor)
    monkeypatch.setattr(_InlineExecutor, "submitted", [])
    monkeypatch.setattr(_HeldWholeExecutor, "held", [])
    monkeypatch.setattr(engine, "_migrate_shard_in_process", lambda args: _TableWorkerResult(
        table_name=args.table.name, status="DONE", rows_loaded=10,
        shard_index=args.shard_index))
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", lambda args: (
        _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=40)))
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", lambda m, d: ("engine", anchor, "s-1"))
    monkeypatch.setattr(engine, "_close_shared_snapshot", _close)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    if interval is not None:
        monkeypatch.setattr(engine, "_anchor_keepalive_seconds", lambda conn: interval)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    tables = [_table(BIG), _table("app.notes", lead="varchar(10)")]
    migrator = BatchedTableMigrator(
        _inputs(tables, cdc=False, source=SourceType.POSTGRES), exporter=_KPlanner(),
        watermark_capturer=object(), importer_factory=lambda _i: _IndexImporter(),
        table_recreator=lambda t: [], target_counter=lambda t: None,
        target_pk_reader=lambda t: list(t.primary_key), target_generated_reader=lambda t: set(),
        target_unique_keys_reader=lambda t: [list(t.primary_key)],
    )
    handle = _Handle([t.name for t in tables])
    safety = threading.Timer(5.0, _HeldWholeExecutor.release)
    safety.start()
    try:
        counts = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, ErrorLogStore(),
            watermark=_wm(binlog=False, counts={BIG: 10 * _M, "app.notes": 1 * _M}),
            fresh_watermark=True,
        )
    finally:
        safety.cancel()
    return SimpleNamespace(anchor=anchor, closes=closes, counts=counts,
                           status={c.chunk_id: c.status for c in handle.job.chunks})


def test_the_anchor_closes_once_its_last_shard_returned_while_another_table_loads(monkeypatch):
    r = _anchor_run(monkeypatch)
    assert r.status == {BIG: "DONE", "app.notes": "DONE"} and r.counts == (0, 0, 0)
    # Closed early -- while the text-key table was still loading -- then only the no-op of
    # the finally (nothing left open).
    assert [(conn is r.anchor, pending) for conn, pending, _sql in r.closes] == [
        (True, True), (False, False)]
    # Its idle-in-transaction limit was read once, right after the export.
    assert r.closes[0][2][0] == engine._ANCHOR_IDLE_TIMEOUT_SQL
    # No ping after the close: the session is gone.
    assert r.anchor.sql == r.closes[0][2]


def test_an_anchor_that_fails_to_close_early_never_fails_the_run(monkeypatch):
    r = _anchor_run(monkeypatch, close_error=RuntimeError("close failed"))
    assert r.status == {BIG: "DONE", "app.notes": "DONE"} and r.counts == (0, 0, 0)
    # Closed once, early; the finally finds nothing left open.
    assert [conn is r.anchor for conn, _pending, _sql in r.closes] == [True, False]


class _HeldShardExecutor(_InlineExecutor):
    """Runs every unit at once but the shards :attr:`hold` names (table, shard index), which
    wait until :meth:`release`."""

    hold: set = set()
    held: list = []

    def submit(self, fn, *args):
        unit = args[0]
        if isinstance(unit, engine._ShardWorkerArgs) and (
                unit.table.name, unit.shard_index) in _HeldShardExecutor.hold:
            _InlineExecutor.submitted.append(unit.table.name)
            future = concurrent.futures.Future()
            _HeldShardExecutor.held.append((future, fn, args))
            return future
        return super().submit(fn, *args)

    @classmethod
    def release(cls) -> None:
        for future, fn, args in cls.held:
            if not future.done():
                future.set_result(fn(*args))


def test_the_anchor_stays_open_until_every_shard_reading_its_snapshot_returned(monkeypatch):
    # FAILS WITHOUT THE COUNT OF THE SHARDS THAT IMPORT THE SNAPSHOT: closed on the first
    # shard's result, the anchor's transaction -- and the exported snapshot with it -- ended
    # while another shard had still to import it. Two 100M-row tables at Tables in parallel 4:
    # two shards each on the shared snapshot; the second table's last shard is still running
    # when the three others have returned (the first table is done).
    anchor = _Anchor("0")
    closes: list = []
    returned: list = []

    def _close(engine_, conn):
        closes.append((conn, [future.done() for future, _fn, _a in _HeldShardExecutor.held]))

    real_record = engine._record_load_stats

    def _record(board, name, unit, snapshot, *, final=False):
        if final:  # a unit's result
            returned.append((name, unit))
            if len(returned) == 3:  # every other shard has returned: the held one may end
                _HeldShardExecutor.release()
        return real_record(board, name, unit, snapshot, final=final)

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _HeldShardExecutor)
    monkeypatch.setattr(_InlineExecutor, "submitted", [])
    monkeypatch.setattr(_HeldShardExecutor, "held", [])
    monkeypatch.setattr(_HeldShardExecutor, "hold", {("app.b", 1)})
    monkeypatch.setattr(engine, "_record_load_stats", _record)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", lambda args: _TableWorkerResult(
        table_name=args.table.name, status="DONE", rows_loaded=10,
        shard_index=args.shard_index))
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", lambda m, d: ("engine", anchor, "s-1"))
    monkeypatch.setattr(engine, "_close_shared_snapshot", _close)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "4")
    tables = [_table("app.a"), _table("app.b")]
    migrator = BatchedTableMigrator(
        _inputs(tables, cdc=False, source=SourceType.POSTGRES), exporter=_KPlanner(),
        watermark_capturer=object(), importer_factory=lambda _i: _IndexImporter(),
        table_recreator=lambda t: [], target_counter=lambda t: None,
        target_pk_reader=lambda t: list(t.primary_key), target_generated_reader=lambda t: set(),
        target_unique_keys_reader=lambda t: [list(t.primary_key)],
    )
    handle = _Handle([t.name for t in tables])
    safety = threading.Timer(5.0, _HeldShardExecutor.release)
    safety.start()
    try:
        counts = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, ErrorLogStore(),
            watermark=_wm(binlog=False, counts={"app.a": 100 * _M, "app.b": 100 * _M}),
            fresh_watermark=True,
        )
    finally:
        safety.cancel()
    assert {c.chunk_id: c.status for c in handle.job.chunks} == {
        "app.a": "DONE", "app.b": "DONE"} and counts == (0, 0, 0)
    assert sorted(returned) == [("app.a", 0), ("app.a", 1), ("app.b", 0), ("app.b", 1)]
    assert returned[-1] == ("app.b", 1)
    # Closed when the held shard -- the snapshot's last reader -- had returned; then only the
    # finally's no-op.
    assert closes == [(anchor, [True]), (None, [True])]


def test_the_keepalive_pings_at_the_interval_read_from_the_anchor(monkeypatch):
    # An interval of 0 pings on every pass of the result loop until the close; 10 s (a 30 s
    # limit) does not ping within this short run.
    eager = _anchor_run(monkeypatch, interval=0.0)
    assert eager.anchor.sql.count("SELECT 1") >= 1
    assert eager.closes[0][0] is eager.anchor
    monkeypatch.undo()
    calm = _anchor_run(monkeypatch)
    assert calm.anchor.sql.count("SELECT 1") == 0


# ---------------------------------------------------------------------------
# 5. The perf harness and the Fargate script
# ---------------------------------------------------------------------------


def test_the_reader_shards_flag_parses_unset_zero_one_and_n(mp, monkeypatch) -> None:  # noqa: F811
    captured: dict = {}
    monkeypatch.setattr(mp, "cmd_full_load", lambda args: captured.setdefault("args", args) and 0)
    monkeypatch.setenv("DB_HOST", "db.invalid")
    monkeypatch.setenv("DB_PASSWORD", "unused-by-this-test")
    for argv, expected in (([], None), (["--reader-shards", "0"], 0),
                           (["--reader-shards", "1"], 1), (["--reader-shards", "8"], 8)):
        captured.clear()
        monkeypatch.setattr("sys.argv", ["measure_performance.py", "full-load", *argv])
        mp.main()
        assert captured["args"].reader_shards == expected


def test_the_flag_sets_the_loaders_setting_for_any_value(mp, monkeypatch, tmp_path):  # noqa: F811
    args, _seen, report_path = _command_doubles(mp, monkeypatch, tmp_path)
    assert args.reader_shards is None and mp.cmd_full_load(args) == 0
    assert _READER_SHARDS not in os.environ                      # unset: left alone
    assert json.loads(report_path.read_text())["reader_shards"] == "auto"
    for value, reported in ((1, 1), (0, "auto"), (6, 6)):
        args.reader_shards = value
        assert mp.cmd_full_load(args) == 0
        assert os.environ[_READER_SHARDS] == str(value)          # 0 and 1 are not dropped
        assert json.loads(report_path.read_text())["reader_shards"] == reported


def test_the_report_names_the_units_and_the_loaders_value(mp, monkeypatch) -> None:  # noqa: F811
    from tests.test_measure_performance import _unit

    board = LoadStatsBoard()
    board.record("perf.a", None, _unit(batches=2), final=True)
    board.record("perf.b", 0, _unit(batches=1), final=True)
    board.record("perf.b", 1, _unit(batches=1), final=True)
    # The .env value the loader never reads does not count: os.environ does.
    monkeypatch.setattr(mp, "ENV", {_READER_SHARDS: "4"})
    report = mp._full_load_report(
        _args(), _job(), board, elapsed=13.0, started_at=0.0, finished_at=13.0, monitor={},
        cdc_mode=False,
    )
    assert report["reader_shards"] == "auto"
    per = {row["table"]: row for row in report["per_table"]}
    assert (per["perf.a"]["units"], per["perf.b"]["units"]) == (1, 2)
    assert per["perf.b"]["reader_shards"] == 2                   # the chunk's G137 record
    monkeypatch.setenv(_READER_SHARDS, "16")
    assert mp._full_load_report(
        _args(), _job(), board, elapsed=13.0, started_at=0.0, finished_at=13.0, monitor={},
        cdc_mode=False,
    )["reader_shards"] == 16
    # A value the loader refused (its run failed on it) is reported as it was set.
    monkeypatch.setenv(_READER_SHARDS, "33")
    assert mp._full_load_report(
        _args(), _job(), board, elapsed=13.0, started_at=0.0, finished_at=13.0, monitor={},
        cdc_mode=False,
    )["reader_shards"] == "33"
    json.dumps(report)


_FARGATE = Path(__file__).resolve().parents[1] / "deploy" / "run_measure_on_fargate.sh"


def _shell_function(text: str, name: str) -> str:
    start = text.index(f"{name}() {{")
    return text[start:text.index("\n}\n", start) + 3]


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_the_fargate_variants_pass_reader_shards_explicitly(tmp_path) -> None:
    text = _FARGATE.read_text()
    mapping = text[text.index("  local no_prefetch prefetch_env reader_shards="):]
    mapping = mapping[:mapping.index("  esac\n") + len("  esac\n")]
    script = "\n".join([
        _shell_function(text, "build_cmd_json"),
        "map_variant() {",
        '  local variant="$1"',
        mapping,
        '  build_cmd_json /tmp/r.json "$no_prefetch" "$reader_shards" "$composite_leading"',
        "}",
        'for v in off on keep composite shard4 shard0 auto custom; do map_variant "$v"; done',
    ])
    # The heredoc runs python3: this interpreter, first on PATH.
    (tmp_path / "python3").symlink_to(sys.executable)
    env = {**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}",
           "MEASURE_SCHEMA": "perf", "MEASURE_TABLES": "", "TABLE_PARALLELISM": "4",
           "BATCH_PARALLELISM": "8", "PROGRESS_INTERVAL": "30", "COMPOSITE_LEADING": "cid"}
    out = subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True,
                         timeout=60)
    assert out.returncode == 0, out.stderr
    commands = [json.loads(line)[0] for line in out.stdout.splitlines() if line.strip()]

    def _flag(command: str):
        words = command.split(" ; ")[0].split()
        return words[words.index("--reader-shards") + 1] if "--reader-shards" in words else None

    assert [_flag(c) for c in commands] == ["1", "1", "1", "1", "4", "0", "0", "1"]
    assert "--no-prefetch" in commands[0] and "--no-prefetch" not in commands[1]
    assert "--composite-leading cid" in commands[3]
