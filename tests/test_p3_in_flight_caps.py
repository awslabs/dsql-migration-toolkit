# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P3: the INSERT batches a Full Load keeps in flight are capped, statically.

Measured live (Seoul, 2026-10-25): one primary-key-ordered stream into one key range of an
Aurora DSQL table takes about 10 commits a second whatever is in flight; more only queues
there (CommitLatency 26 ms -> 0.7 s with 8 in flight -> 3 s with 32, ComputeDPU four times
as high at the same rows/s), and Tables in parallel 12 x Batches per table 32 (352 in
flight) loaded slower, at ten times the commit latency. So each load unit keeps
``max(1, min(bp, R * ranges, B // total_workers))`` batches in flight -- R = 4 per key range,
B = 128 per run, bp = Batches per table -- and the run is at most B by construction: at most
``total_workers`` units run at once. No IPC, no tokens. Pinned here:

* the rule and its measurement seams (``DSQL_MIGRATOR_FULL_LOAD_RANGE_IN_FLIGHT`` /
  ``DSQL_MIGRATOR_FULL_LOAD_IN_FLIGHT_BUDGET``: an integer >= 1, anything else keeps the
  default);
* an exhaustive grid (no hypothesis in the venv): the ``total_workers`` largest caps add up
  to at most B, and every cap is between 1 and bp and at most R per key range;
* the process path hands every unit its cap; ``migrate_table`` caps the importer it builds
  (a double's ``cap_parallelism``; a real importer never opens nor runs more than the cap);
  a whole-table worker hands ``migrate_table`` the parent's cap and a shard worker caps its
  own; ``BatchedImporter.cap_parallelism`` never raises the pool;
* the units' shape on the write-path counters (set, summed by a merge, kept by a window);
* the perf harness's ``--range-in-flight`` / ``--in-flight-budget`` and its report;
* the texts that name the caps.

Doubles only: no MySQL, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import dataclasses
import json
import os
import threading
import time
from types import SimpleNamespace

import pytest

import dsql_migrator.core.batched_import as bi
from dsql_migrator.config import TUNABLE_KNOBS, AppConfig
from dsql_migrator.core.batched_import import BatchedImporter, BatchedImportOptions
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.load_stats import (
    LoadStats,
    LoadStatsBoard,
    diff_load_stats,
    merge_load_stats,
    summarize_load_stats,
)
from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    SourceInventory,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    DataMigrationInputs,
    _TableWorkerResult,
)

from tests.test_batched_import import _FakeStore, _connection_factory, _rows
from tests.test_full_load_overlay import _InlineExecutor
from tests.test_g137_mysql_reader_shards import (
    _FOUR,
    _Handle,
    _IndexImporter,
    _Planner,
    _inputs as _shard_inputs,
    _table as _shard_table,
    _wm,
)
from tests.test_measure_performance import _args, _command_doubles, _job, mp  # noqa: F401
from tests.test_ui_data_migration import (
    _FakeExporter,
    _FakeImporter,
    _FakeWatermarkCapturer,
    _inputs as _ui_inputs,
    _watermark,
)
from tests.test_ui_design import _FormUi, _render_tuning_tab

_RANGE_SEAM = "DSQL_MIGRATOR_FULL_LOAD_RANGE_IN_FLIGHT"
_BUDGET_SEAM = "DSQL_MIGRATOR_FULL_LOAD_IN_FLIGHT_BUDGET"


@pytest.fixture(autouse=True)
def _no_seams(monkeypatch):
    """Every test starts without the seams, and gets back whatever was set before it --
    also when the harness's command sets them (os.environ, not monkeypatch)."""
    for key in (_RANGE_SEAM, _BUDGET_SEAM):
        monkeypatch.setenv(key, "")  # records the value before the test ...
        monkeypatch.delenv(key)      # ... which the test then runs without


# ---------------------------------------------------------------------------
# 1. The rule (pure)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bp, ranges, share, cap", [
    (8, 1, None, 4),       # an unsplit table: 4 in flight (it was Batches per table, 8)
    (8, 4, None, 8),       # four key ranges: Batches per table binds
    (2, 1, None, 2),       # Batches per table below 4 binds
    (32, 8, 6, 6),         # the unit's share of the run's budget binds
    (8, 1, 1, 1),          # a share of one
    (8, 0, None, 4),       # no range is read as one
    (1, 32, 128, 1),
    (32, 32, 128, 32),
])
def test_a_units_cap_is_the_smallest_of_its_three_bounds(bp, ranges, share, cap) -> None:
    assert engine._unit_in_flight(bp, ranges, share) == cap


def test_the_defaults_and_the_designs_examples() -> None:
    assert (engine._RANGE_IN_FLIGHT, engine._RUN_IN_FLIGHT_BUDGET) == (4, 128)
    assert (engine._range_in_flight(), engine._run_in_flight_budget()) == (4, 128)
    # Tables in parallel 4 x Batches per table 8 (the defaults), unsplit tables: 4 each,
    # 16 in flight for the run (it was 32). The budget does not bind at Tables in parallel
    # <= 16 with Batches per table 8.
    assert engine._unit_in_flight(8, 1, engine._in_flight_share(4)) == 4
    assert engine._in_flight_share(16) == 8
    # perf_ecom at Tables in parallel 12 x Batches per table 32: 12 x 4 = 48 (it was 352).
    assert 12 * engine._unit_in_flight(32, 1, engine._in_flight_share(12)) == 48
    # Tables in parallel 16, two key ranges a unit: 16 x min(8, 8, 8) = 128, the budget.
    assert 16 * engine._unit_in_flight(8, 2, engine._in_flight_share(16)) == 128


def test_the_seams_replace_r_and_b(monkeypatch) -> None:
    monkeypatch.setenv(_RANGE_SEAM, "8")
    monkeypatch.setenv(_BUDGET_SEAM, " 32 ")
    assert (engine._range_in_flight(), engine._run_in_flight_budget()) == (8, 32)
    assert engine._unit_in_flight(16, 1, None) == 8
    assert engine._in_flight_share(4) == 8 and engine._in_flight_share(5) == 6
    assert engine._unit_in_flight(16, 4, engine._in_flight_share(4)) == 8
    # Read per call: unset again, the constants are back.
    monkeypatch.delenv(_RANGE_SEAM)
    monkeypatch.delenv(_BUDGET_SEAM)
    assert (engine._range_in_flight(), engine._run_in_flight_budget()) == (4, 128)


@pytest.mark.parametrize("raw", ["0", "-2", "", "   ", "four", "2.5", "1e3", "true"])
def test_a_seam_that_is_not_an_integer_of_at_least_one_keeps_the_default(monkeypatch, raw):
    monkeypatch.setenv(_RANGE_SEAM, raw)
    monkeypatch.setenv(_BUDGET_SEAM, raw)
    assert (engine._range_in_flight(), engine._run_in_flight_budget()) == (4, 128)


def test_a_budget_below_the_pool_size_keeps_one_batch_per_running_unit(monkeypatch) -> None:
    # Measurement only: the floor of one per unit wins over a budget smaller than the pool.
    monkeypatch.setenv(_BUDGET_SEAM, "2")
    assert engine._in_flight_share(4) == 1
    assert engine._unit_in_flight(8, 1, engine._in_flight_share(4)) == 1
    assert engine._in_flight_share(0) == 2  # no worker at all is read as one


_RANGES = (1, 2, 4, 8, 32)


def test_a_runs_in_flight_never_exceeds_the_budget_over_the_whole_grid() -> None:
    # Exhaustive: Tables in parallel 1..16 x Batches per table x key ranges per unit x unit
    # counts, every unit the same shape and a mix. At most total_workers units run at once,
    # so the run's in flight is at most the sum of the total_workers largest caps.
    r, b = engine._RANGE_IN_FLIGHT, engine._RUN_IN_FLIGHT_BUDGET
    checked = 0
    for tp in range(1, 17):
        for bp in (1, 2, 4, 8, 16, 32):
            for units in sorted({1, tp, 3 * tp}):
                total_workers = min(tp, units)
                share = engine._in_flight_share(total_workers)
                shapes = [[k] * units for k in _RANGES]
                shapes.append([_RANGES[i % len(_RANGES)] for i in range(units)])
                for ranges in shapes:
                    caps = [engine._unit_in_flight(bp, k, share) for k in ranges]
                    for cap, k in zip(caps, ranges):
                        assert 1 <= cap <= bp and cap <= r * k, (tp, bp, k, cap)
                    largest = sorted(caps, reverse=True)[:total_workers]
                    assert sum(largest) <= b, (tp, bp, units, ranges)
                    checked += 1
    # tp 1 has two unit counts ({1, 3}), every other tp three; 6 values of bp, 6 shapes.
    assert checked == (2 + 15 * 3) * 6 * 6


# ---------------------------------------------------------------------------
# 2. The process path hands every unit its cap
# ---------------------------------------------------------------------------


@pytest.fixture
def pool(monkeypatch):
    """``_migrate_tables_in_parallel`` on the process path with the workers inline: a MySQL
    Full load + CDC run from a binlog start point, Reader shards 4, so "app.events" is read
    in four ranges and "app.small" whole (five units)."""
    seen: dict = {"whole": [], "shard": []}

    def _whole(args):
        seen["whole"].append(args)
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=1)

    def _shard(args):
        seen["shard"].append(args)
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=1,
                                  shard_index=args.shard_index)

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(_InlineExecutor, "submitted", [])
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", lambda m, d: (None, None, "snap-1"))
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")

    def run(*, tp: int, bp: int):
        monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", str(tp))
        monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM", str(bp))
        seen["whole"].clear()
        seen["shard"].clear()
        tables = [_shard_table("app.small"), _shard_table("app.events")]
        migrator = BatchedTableMigrator(
            _shard_inputs(tables), exporter=_Planner({"app.events": _FOUR}),
            watermark_capturer=object(), importer_factory=lambda _i: _IndexImporter(),
            table_recreator=lambda t: [], target_counter=lambda t: None,
            target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(),
            target_unique_keys_reader=lambda t: [list(t.primary_key)],
        )
        engine._migrate_tables_in_parallel(
            _Handle([t.name for t in tables]), "j", tables, migrator, ErrorLogStore(),
            watermark=_wm(), fresh_watermark=True,
        )
        return SimpleNamespace(
            whole={a.table.name: a.in_flight for a in seen["whole"]},
            shards=[a.in_flight for a in seen["shard"]],
        )

    return run


def test_every_unit_of_the_pool_gets_its_cap(pool) -> None:
    # The defaults: the whole table 4 (it loaded with 8), each of the four ranges 4.
    r = pool(tp=4, bp=8)
    assert r.whole == {"app.small": 4} and r.shards == [4, 4, 4, 4]
    # Batches per table below 4 binds.
    r = pool(tp=2, bp=2)
    assert r.whole == {"app.small": 2} and r.shards == [2, 2, 2, 2]
    # Batches per table 32 at Tables in parallel 16: still 4 per key range.
    r = pool(tp=16, bp=32)
    assert r.whole == {"app.small": 4} and r.shards == [4, 4, 4, 4]


def test_the_pool_divides_the_budget_among_its_workers(pool, monkeypatch) -> None:
    # Five units at Tables in parallel 16: five workers, 128 // 5 = 25 each.
    monkeypatch.setenv(_RANGE_SEAM, "16")
    r = pool(tp=16, bp=32)
    assert r.whole == {"app.small": 16} and r.shards == [16] * 4
    monkeypatch.setenv(_RANGE_SEAM, "64")
    r = pool(tp=16, bp=32)
    assert r.whole == {"app.small": 25} and r.shards == [25] * 4   # the share binds
    # A budget of 8 over four workers: 2 each.
    monkeypatch.delenv(_RANGE_SEAM)
    monkeypatch.setenv(_BUDGET_SEAM, "8")
    r = pool(tp=4, bp=8)
    assert r.whole == {"app.small": 2} and r.shards == [2, 2, 2, 2]


def test_the_worker_args_carry_the_cap_and_default_to_none() -> None:
    table = _shard_table()
    inputs = _shard_inputs([table])
    assert engine._TableWorkerArgs(job_id="j", table=table, inputs=inputs).in_flight is None
    assert engine._ShardWorkerArgs(
        job_id="j", table=table, inputs=inputs, pk_lower=None, pk_upper=None, shard_index=0,
    ).in_flight is None
    args = engine._TableWorkerArgs(job_id="j", table=table, inputs=inputs, in_flight=4)
    assert args.in_flight == 4


# ---------------------------------------------------------------------------
# 3. migrate_table caps the importer it builds; the shard worker caps its own
# ---------------------------------------------------------------------------


class _CappedImporter(_FakeImporter):
    """The fake importer, recording every cap the engine sets on it."""

    def __init__(self, **kw) -> None:
        super().__init__(**kw)
        self.caps: list = []

    def cap_parallelism(self, n: int) -> None:
        self.caps.append(n)


def _table_migrator(importer, *, ranges=None, coexisting=False):
    exporter = _FakeExporter(rows_by_table={"orders": [{"id": i} for i in range(1, 7)]})
    if ranges:
        exporter.shard_ranges_by_table["orders"] = ranges
    inputs = dataclasses.replace(_ui_inputs(), cdc_coexisting=coexisting)
    migrator = BatchedTableMigrator(
        inputs, exporter=exporter, watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
    )
    return migrator, inputs.inventory.tables[0]


def test_migrate_table_caps_the_importer_before_it_loads(monkeypatch) -> None:
    importer = _CappedImporter()
    migrator, orders = _table_migrator(importer)
    result = migrator.migrate_table(orders)
    # One key range, Batches per table 8: 4.
    assert importer.caps == [4] and importer.received[0][0] == "orders"
    assert (result.load_stats["ranges"], result.load_stats["in_flight_cap"]) == (1, 4)

    # A process worker's unit: the parent's cap wins.
    importer = _CappedImporter()
    migrator, orders = _table_migrator(importer)
    result = migrator.migrate_table(orders, in_flight_cap=2)
    assert importer.caps == [2] and result.load_stats["in_flight_cap"] == 2

    # Batches per table below 4 binds; the seam replaces R.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM", "2")
    importer = _CappedImporter()
    migrator, orders = _table_migrator(importer)
    migrator.migrate_table(orders)
    assert importer.caps == [2]
    monkeypatch.delenv("DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM")
    monkeypatch.setenv(_RANGE_SEAM, "1")
    importer = _CappedImporter()
    migrator, orders = _table_migrator(importer)
    migrator.migrate_table(orders)
    assert importer.caps == [1]


def test_a_coexisting_load_read_in_ranges_gets_four_per_range(monkeypatch) -> None:
    # The in-process K readers of a load while CDC streams (the only one migrate_table
    # splits): three ranges, at most min(8, 3 x 4) = 8 in flight -- as before P3.
    importer = _CappedImporter()
    migrator, orders = _table_migrator(
        importer, ranges=[(None, 3), (3, 5), (5, None)], coexisting=True
    )
    result = migrator.migrate_table(orders)
    assert importer.caps == [8]
    assert (result.load_stats["ranges"], result.load_stats["in_flight_cap"]) == (3, 8)
    assert sorted(r["id"] for r in importer.received[0][1]) == [1, 2, 3, 4, 5, 6]


def test_an_importer_without_the_cap_still_loads() -> None:
    importer = _FakeImporter()
    migrator, orders = _table_migrator(importer)
    result = migrator.migrate_table(orders)
    assert result.rows_loaded == 6 and result.load_stats["in_flight_cap"] == 4


def test_a_unit_re_read_sets_its_shape_again_never_adds_it() -> None:
    importer = _CappedImporter()
    migrator, orders = _table_migrator(importer)
    stats = LoadStats()
    migrator.migrate_table(orders, load_stats=stats)
    migrator.migrate_table(orders, load_stats=stats)   # a source-drop re-read, same unit
    snapshot = stats.snapshot()
    assert (snapshot["ranges"], snapshot["in_flight_cap"]) == (1, 4)


def _customers() -> TableDef:
    return TableDef(
        name="customers",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="name", mysql_type="varchar(50)")],
        primary_key=["id"],
    )


def test_a_real_importer_never_opens_nor_runs_more_than_the_cap() -> None:
    # Batches per table 8, one key range: the cap is 4. Every statement on the fake target
    # takes a few ms, so the batches overlap; at no point are more than 4 running, and no
    # more than 4 connections are ever opened.
    store = _FakeStore()
    base = _connection_factory(store, ["id", "name"], ["id"])
    lock = threading.Lock()
    running = {"now": 0, "max": 0}

    def factory():
        connection = base()
        original = connection.handle_execute

        def handle_execute(query, params, cursor):
            with lock:
                running["now"] += 1
                running["max"] = max(running["max"], running["now"])
            try:
                time.sleep(0.004)
                return original(query, params, cursor)
            finally:
                with lock:
                    running["now"] -= 1

        connection.handle_execute = handle_execute
        return connection

    importer = BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=8), connection_factory=factory,
        occ_max_attempts=3, occ_base_delay=0.0, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    table = _customers()
    inputs = DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=[table]),
    )
    migrator = BatchedTableMigrator(
        inputs, exporter=_FakeExporter(rows_by_table={"customers": _rows(120)}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
    )
    result = migrator.migrate_table(table)

    assert result.rows_loaded == 120 and len(store.rows) == 120
    assert importer._options.parallelism == 4
    assert store.connections_created <= 4 and result.load_stats["connects"] <= 4
    assert 2 <= running["max"] <= 4, running
    assert result.load_stats["in_flight_cap"] == 4


def _run_shard_worker(monkeypatch, importer, *, in_flight=None):
    class _Exporter:
        def stream_converted_rows(self, conn, table, **_kw):
            return iter([{"id": i, "email": f"e{i}"} for i in range(1, 6)])

    class _ShardMigrator(BatchedTableMigrator):  # the real class, imported before any patch
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=_Exporter(), watermark_capturer=object(),
                importer_factory=lambda _i: importer, table_recreator=lambda t: [],
                target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
            )

    monkeypatch.setattr(engine, "BatchedTableMigrator", _ShardMigrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    table = _shard_table()
    return engine._migrate_shard_in_process(engine._ShardWorkerArgs(
        job_id="j", table=table, inputs=_shard_inputs([table]), pk_lower=None, pk_upper=250,
        shard_index=1, shard_count=4, in_flight=in_flight,
    ))


def test_the_shard_worker_caps_its_importer(monkeypatch) -> None:
    importer = _CappedImporter()
    result = _run_shard_worker(monkeypatch, importer, in_flight=3)
    assert result.status == "DONE" and importer.caps == [3]
    assert (result.load_stats["ranges"], result.load_stats["in_flight_cap"]) == (1, 3)
    # Sized without a share when the parent sent none: one key range, 4.
    importer = _CappedImporter()
    result = _run_shard_worker(monkeypatch, importer)
    assert importer.caps == [4] and result.load_stats["in_flight_cap"] == 4
    # A real importer: the pool follows.
    store = _FakeStore()
    real = BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=8),
        connection_factory=_connection_factory(store, ["id", "email"], ["id"]),
        occ_max_attempts=3, occ_base_delay=0.0, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    result = _run_shard_worker(monkeypatch, real, in_flight=3)
    assert result.status == "DONE" and len(store.rows) == 5
    assert real._options.parallelism == 3 and store.connections_created <= 3


def _run_table_worker(monkeypatch, importer, *, in_flight=None):
    table = _shard_table()
    rows = [{"id": i, "email": f"e{i}"} for i in range(1, 6)]

    class _TableMigrator(BatchedTableMigrator):  # the real class, imported before any patch
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=_FakeExporter(rows_by_table={table.name: rows}),
                watermark_capturer=object(), importer_factory=lambda _i: importer,
                table_recreator=lambda t: [], target_counter=lambda t: None,
                target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
            )

    monkeypatch.setattr(engine, "BatchedTableMigrator", _TableMigrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    return engine._migrate_one_table_in_process(engine._TableWorkerArgs(
        job_id="j", table=table, inputs=_shard_inputs([table]), in_flight=in_flight,
    ))


def test_the_whole_table_worker_hands_the_parents_cap_to_migrate_table(monkeypatch) -> None:
    # The parent sized this unit with its share of the run's budget (2 here, where the
    # worker on its own would size 4): migrate_table keeps that one.
    importer = _CappedImporter()
    result = _run_table_worker(monkeypatch, importer, in_flight=2)
    assert result.status == "DONE" and importer.caps == [2]
    assert (result.load_stats["ranges"], result.load_stats["in_flight_cap"]) == (1, 2)
    # Sized without a share when the parent sent none: one key range, 4.
    importer = _CappedImporter()
    result = _run_table_worker(monkeypatch, importer)
    assert result.status == "DONE" and importer.caps == [4]
    assert result.load_stats["in_flight_cap"] == 4


# ---------------------------------------------------------------------------
# 4. BatchedImporter.cap_parallelism
# ---------------------------------------------------------------------------


def test_cap_parallelism_lowers_never_raises_and_the_pool_and_prefetch_follow(monkeypatch):
    importer = BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=8),
        connection_factory=_connection_factory(_FakeStore(), ["id", "name"], ["id"]),
        occ_max_attempts=3, occ_base_delay=0.0, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    importer.cap_parallelism(16)
    assert importer._options.parallelism == 8        # never raised
    importer.cap_parallelism(0)
    assert importer._options.parallelism == 1        # never below one
    importer = BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=8),
        connection_factory=_connection_factory(_FakeStore(), ["id", "name"], ["id"]),
        occ_max_attempts=3, occ_base_delay=0.0, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    importer.cap_parallelism(3)
    assert importer._options.parallelism == 3
    assert importer._options.batch_size == 2          # the other options are kept

    sizes: list = []
    depths: list = []

    class _Pool(bi._ConnectionPool):
        def __init__(self, factory, size, stats=None):
            sizes.append(size)
            super().__init__(factory, size, stats=stats)

    real_depth = bi._prefetch_depth
    monkeypatch.setattr(bi, "_ConnectionPool", _Pool)
    monkeypatch.setattr(bi, "_prefetch_depth", lambda n: depths.append(n) or real_depth(n))
    result = importer.import_rows(_rows(10), _customers())
    assert result.rows_loaded == 10
    assert sizes == [3] and depths == [3]


# ---------------------------------------------------------------------------
# 5. The units' shape on the write-path counters
# ---------------------------------------------------------------------------


def _shaped(ranges: int, cap: int, *, batches: int = 0) -> dict:
    stats = LoadStats()
    stats.set_unit_shape(ranges=ranges, in_flight_cap=cap)
    for _ in range(batches):
        stats.record_batch(0.1, failed=False)
    return stats.snapshot()


def test_a_units_shape_is_set_summed_by_a_merge_and_kept_by_a_window() -> None:
    stats = LoadStats()
    assert (stats.snapshot()["ranges"], stats.snapshot()["in_flight_cap"]) == (0, 0)
    stats.set_unit_shape(ranges=1, in_flight_cap=4)
    stats.set_unit_shape(ranges=1, in_flight_cap=4)   # set again (a re-read), not added
    before = stats.snapshot()
    assert (before["ranges"], before["in_flight_cap"]) == (1, 4)
    stats.record_batch(0.2, failed=False)
    window = diff_load_stats(stats.snapshot(), before)
    assert window["batches"] == 1
    assert (window["ranges"], window["in_flight_cap"]) == (1, 4)     # current, not 0
    stats.set_unit_shape(ranges=-1, in_flight_cap=-5)
    assert (stats.snapshot()["ranges"], stats.snapshot()["in_flight_cap"]) == (0, 0)

    # A table's shards: its K ranges and its total cap; a run's totals.
    merged = merge_load_stats([_shaped(1, 4), _shaped(1, 4), _shaped(3, 8)])
    assert (merged["ranges"], merged["in_flight_cap"], merged["units"]) == (5, 16, 3)
    summary = summarize_load_stats(merged)
    assert (summary["ranges"], summary["in_flight_cap"]) == (5, 16)
    json.dumps(summary)

    # A snapshot from before the shape existed merges as 0.
    older = _shaped(1, 4, batches=2)
    del older["ranges"], older["in_flight_cap"]
    assert (merge_load_stats([older])["ranges"], merge_load_stats([older])["in_flight_cap"]) \
        == (0, 0)
    assert merge_load_stats([older, _shaped(2, 8)])["in_flight_cap"] == 8
    assert diff_load_stats(_shaped(1, 4, batches=3), older)["in_flight_cap"] == 4
    assert summarize_load_stats(older)["in_flight_cap"] == 0
    # The parent's index pass reads and writes no row: no shape.
    assert LoadStats(units=0).snapshot()["in_flight_cap"] == 0


def test_the_board_merges_the_shapes_per_table_and_per_run() -> None:
    board = LoadStatsBoard()
    for shard in range(4):
        board.record("app.events", shard, _shaped(1, 4))
    board.record("app.small", None, _shaped(1, 4))
    board.record("app.small", "index", LoadStats(units=0).snapshot())
    assert (board.table("app.events")["ranges"], board.table("app.events")["in_flight_cap"]) \
        == (4, 16)
    assert board.table("app.small")["in_flight_cap"] == 4
    assert (board.run()["ranges"], board.run()["in_flight_cap"]) == (5, 20)


# ---------------------------------------------------------------------------
# 6. The perf harness
# ---------------------------------------------------------------------------


def test_the_in_flight_flags_parse(mp, monkeypatch) -> None:  # noqa: F811 - the fixture
    captured: dict = {}
    monkeypatch.setattr(mp, "cmd_full_load", lambda args: captured.setdefault("args", args) and 0)
    monkeypatch.setenv("DB_HOST", "db.invalid")
    monkeypatch.setenv("DB_PASSWORD", "unused-by-this-test")
    for argv, expected in (
        ([], (None, None)),
        (["--range-in-flight", "8", "--in-flight-budget", "64"], (8, 64)),
    ):
        captured.clear()
        monkeypatch.setattr("sys.argv", ["measure_performance.py", "full-load", *argv])
        mp.main()
        assert (captured["args"].range_in_flight, captured["args"].in_flight_budget) == expected


def test_the_flags_set_the_loaders_seams_for_the_run(mp, monkeypatch, tmp_path):  # noqa: F811
    args, seen, report_path = _command_doubles(mp, monkeypatch, tmp_path)
    assert mp.cmd_full_load(args) == 0                       # no flag: the product's caps
    assert _RANGE_SEAM not in os.environ and _BUDGET_SEAM not in os.environ
    assert json.loads(report_path.read_text())["in_flight"] == {
        "per_range": 4, "run_budget": 128}
    assert any("(in flight <= 4 per key range, 128 per run)" in line for line in seen["log"])

    args.range_in_flight, args.in_flight_budget = 8, 64
    assert mp.cmd_full_load(args) == 0
    assert (os.environ[_RANGE_SEAM], os.environ[_BUDGET_SEAM]) == ("8", "64")
    assert json.loads(report_path.read_text())["in_flight"] == {
        "per_range": 8, "run_budget": 64}
    assert any("(in flight <= 8 per key range, 64 per run)" in line for line in seen["log"])

    # A value the loader does not take is reported as what applied: its default.
    args.range_in_flight, args.in_flight_budget = 0, None
    assert mp.cmd_full_load(args) == 0
    assert json.loads(report_path.read_text())["in_flight"] == {
        "per_range": 4, "run_budget": 64}


def test_the_report_carries_each_tables_ranges_and_cap(mp) -> None:  # noqa: F811
    board = LoadStatsBoard()
    board.record("perf.a", None, _shaped(1, 4, batches=2), final=True)
    board.record("perf.b", 0, _shaped(1, 4, batches=1), final=True)
    board.record("perf.b", 1, _shaped(1, 4, batches=1), final=True)
    report = mp._full_load_report(
        _args(), _job(), board, elapsed=13.0, started_at=0.0, finished_at=13.0, monitor={},
        cdc_mode=False,
    )
    assert report["in_flight"] == {"per_range": 4, "run_budget": 128}
    per = {row["table"]: row for row in report["per_table"]}
    assert (per["perf.a"]["ranges"], per["perf.a"]["in_flight_cap"]) == (1, 4)
    assert (per["perf.b"]["ranges"], per["perf.b"]["in_flight_cap"]) == (2, 8)
    assert report["load_stats"]["in_flight_cap"] == 12
    json.dumps(report)
    lines: list = []
    mp.log = lines.append
    mp._log_full_load_summary(report)
    assert any("in flight: at most 4 per key range, 8 per table or part, 128 per run" in line
               for line in lines), lines
    assert any("perf.b" in line and "in flight <= 8 over 2 range(s)" in line
               for line in lines), lines

    # A table with no counters has no shape to report.
    report = mp._full_load_report(
        _args(), _job(), LoadStatsBoard(), elapsed=13.0, started_at=0.0, finished_at=13.0,
        monitor={}, cdc_mode=False,
    )
    assert {(row["ranges"], row["in_flight_cap"]) for row in report["per_table"]} == {
        (None, None)}
    mp._log_full_load_summary(report)


# ---------------------------------------------------------------------------
# 7. The texts that name the caps
# ---------------------------------------------------------------------------


def test_the_settings_notice_names_the_caps_the_loader_applies(monkeypatch) -> None:
    ui = _FormUi()
    _render_tuning_tab("Full Load", ui)
    blob = " ".join(ui.texts)
    assert ("In flight: at most 4 batches per key range, Batches per table per table, "
            "128 per run") in blob
    assert "A key range of an Aurora DSQL table takes about 10 commits a second" in blob
    # The seams (one-image A/Bs) are what the loader applies, so the notice follows them.
    monkeypatch.setenv(_RANGE_SEAM, "8")
    monkeypatch.setenv(_BUDGET_SEAM, "64")
    ui = _FormUi()
    _render_tuning_tab("Full Load", ui)
    assert ("In flight: at most 8 batches per key range, Batches per table per table, "
            "64 per run") in " ".join(ui.texts)


def test_the_knob_descriptions_name_the_caps_not_the_product() -> None:
    fields = AppConfig.model_fields
    batches = fields["full_load_batch_parallelism"].description
    tables = fields["full_load_table_parallelism"].description
    assert "at most this many" in batches and "each key range keeps at most 4" in batches
    assert "at most 128" in batches
    assert "parts of a large table" in tables and "at most 128 per run" in tables
    assert "x full_load_batch_parallelism" not in tables
    knobs = {k.field: k for k in TUNABLE_KNOBS}
    assert "at most this many per table (each key range keeps at most 4)" in (
        knobs["full_load_batch_parallelism"].description)
    assert "parts of a large table" in knobs["full_load_table_parallelism"].description
    # The OCC-exhaustion hint names the two Settings knobs.
    from dsql_migrator.core.target_connection import target_error_hint

    error = Exception("x")
    error.sqlstate = "40001"  # type: ignore[attr-defined]
    hint = target_error_hint(error)
    assert "Lower Tables in parallel or Batches per table and re-run" in hint
