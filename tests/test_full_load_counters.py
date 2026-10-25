# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The Full Load's write-path counters, end to end on the existing doubles (R0).

The table workers are spawned processes that set up no logging, so the perf harness's
parent-logger OCC counter saw no batch at all. These pin the replacement:

* the importer counts batches, attempts, retries by code (OC000 / OC001 apart), 23505
  recoveries, connects / reconnects, read-ahead queue waits and its phases, into the
  collector the load unit binds;
* each worker returns its counters (with its process's CPU seconds and peak RSS) on every
  outcome and sends them every couple of seconds while it loads -- also while no batch
  completes (an OCC storm), so a window's OC000 / OC001 land in the window they happened in;
* the in-process (tp=1) path counts across a table's source-drop re-reads and reports a
  failed or stopped table too;
* the parent's drain and result loop merge them per table and per run on a board, never
  through the job (no persist, no liveness stamp).
"""

from __future__ import annotations

import dataclasses
import threading
import time
from types import SimpleNamespace

import pytest

from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
    _ConnectionPool,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.load_stats import LoadStats, LoadStatsBoard
from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    TableLoadResult,
    _TableWorkerResult,
)

from tests.test_batched_import import (
    _FakeStore,
    _connection_factory,
    _index_ddls,
    _no_sleep,
    _rows,
    _table,
    _zero_jitter,
)


class _DsqlError(Exception):
    """A psycopg-like error: ``sqlstate`` plus Aurora DSQL's message."""

    def __init__(self, message: str, sqlstate: str) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


_OC000 = "change conflicts with another transaction, please retry: (OC000)"
_OC001 = "schema has been updated by another transaction, please retry: (OC001)"


def _failing_factory(store, columns, keys, errors, *, delay=0.0):
    """The existing fake target, raising ``errors`` in order on INSERTs before storing,
    and taking ``delay`` seconds per statement."""
    base = _connection_factory(store, columns, keys)
    lock = threading.Lock()

    def factory():
        connection = base()
        original = connection.handle_execute

        def handle_execute(query, params, cursor):
            text = query if isinstance(query, str) else query.as_string(None)
            if delay:
                time.sleep(delay)
            if text.lstrip().upper().startswith("INSERT"):
                with lock:
                    error = errors.pop(0) if errors else None
                if error is not None:
                    raise error
            return original(query, params, cursor)

        connection.handle_execute = handle_execute
        return connection

    return factory


def _importer(factory, *, batch_size=2, parallelism=1, attempts=5) -> BatchedImporter:
    return BatchedImporter(
        BatchedImportOptions(batch_size=batch_size, parallelism=parallelism),
        connection_factory=factory, occ_max_attempts=attempts, occ_base_delay=0.0,
        sleep=_no_sleep, jitter=_zero_jitter,
    )


# --------------------------------------------------------------------------- importer


def test_the_importer_counts_attempts_and_retries_by_cause() -> None:
    store = _FakeStore()
    errors = [_DsqlError(_OC001, "40001"), _DsqlError(_OC000, "40001"),
              _DsqlError("serialization failure", "40001")]
    importer = _importer(_failing_factory(store, ["id", "name"], ["id"], errors))
    stats = LoadStats()
    importer.bind_load_stats(stats)

    result = importer.import_rows(_rows(6), _table(), on_conflict=OnConflictMode.NONE)

    assert result.rows_loaded == 6 and result.failures == 0
    s = stats.snapshot()
    assert (s["batches"], s["batches_failed"]) == (3, 0)
    # The first batch took four attempts, the other two one each.
    assert (s["statements"], s["attempts"]) == (3, 6)
    assert s["attempts_per_statement"] == {4: 1, 1: 2}
    assert s["retries"] == {"40001/OC001": 1, "40001/OC000": 1, "40001": 1}
    assert s["retries_exhausted"] == 0
    # Every attempt is one INSERT round trip; every batch one sample.
    assert s["execute_ms"]["n"] == 6 and s["batch_ms"]["n"] == 3
    # A failed attempt discards its connection, so each retry reconnected.
    assert (s["connects"], s["reconnects"], s["connect_failures"]) == (4, 3, 0)
    assert s["load_s"] > 0 and s["load_started_at"] <= s["load_finished_at"]
    assert s["index_s"] == 0.0 and s["count_s"] == 0.0


def test_a_spent_retry_budget_is_counted_and_fails_the_batch() -> None:
    store = _FakeStore()
    errors = [_DsqlError(_OC001, "40001"), _DsqlError(_OC001, "40001")]
    importer = _importer(_failing_factory(store, ["id", "name"], ["id"], errors), attempts=2)

    result = importer.import_rows(_rows(2), _table(), on_conflict=OnConflictMode.NONE)

    assert result.failures == 1
    s = importer.load_stats.snapshot()  # unbound: the importer's own collector
    assert s["retries"] == {"40001/OC001": 2} and s["retries_exhausted"] == 1
    assert (s["batches"], s["batches_failed"], s["attempts"]) == (1, 1, 2)


def test_a_23505_recovery_after_an_uncertain_commit_is_counted() -> None:
    # Aurora DSQL's XX000 at commit: the rows landed, the client got an error. The retry's
    # plain INSERT then meets 23505 and recovers by reading the keys already present.
    store = _FakeStore()
    store.landed_failures.append("XX000")
    importer = _importer(_connection_factory(store, ["id", "name"], ["id"]), batch_size=10)

    result = importer.import_rows(_rows(4), _table(), on_conflict=OnConflictMode.NONE)

    assert result.failures == 0 and len(store.rows) == 4
    s = importer.load_stats.snapshot()
    assert s["retries"] == {"XX000": 1}
    assert s["unique_recoveries"] == 1
    # The pool discards a connection on ANY error raised while it is leased, so both the
    # XX000 and the 23505 cost a reconnect -- what the counter is there to show.
    assert s["reconnects"] == 2


def test_a_skip_existing_overlap_is_counted_as_a_recovery() -> None:
    store = _FakeStore()
    store.seed(_rows(1), ["id"])
    importer = _importer(_connection_factory(store, ["id", "name"], ["id"]), batch_size=10)

    result = importer.import_rows(_rows(3), _table(), on_conflict=OnConflictMode.SKIP_EXISTING)

    assert (result.rows_loaded, result.conflicts) == (2, 1)
    s = importer.load_stats.snapshot()
    assert s["unique_recoveries"] == 1 and s["retries"] == {}
    # The optimistic INSERT and the select-filtered one are both write round trips.
    assert s["execute_ms"]["n"] == 2


def test_the_index_pass_is_timed_and_its_retries_counted_apart() -> None:
    store = _FakeStore()
    store.ddl_kill_connections_remaining = 1  # the first connection dies on its DDL
    importer = _importer(_connection_factory(store, ["id", "name"], ["id"]), batch_size=10)

    result = importer.import_rows(_rows(3), _table(), index_ddls=_index_ddls())

    assert result.indexes_created == 2
    s = importer.load_stats.snapshot()
    assert s["index_retries"] == {"conn": 1} and s["retries"] == {}
    assert s["index_s"] >= 0.0 and s["load_s"] > 0.0


def test_a_slow_target_blocks_the_reader_and_a_slow_source_starves_the_writers() -> None:
    # Target-bound: one writer, 30 ms per INSERT, a reader that never waits for rows.
    store = _FakeStore()
    importer = _importer(
        _failing_factory(store, ["id", "name"], ["id"], [], delay=0.03), batch_size=1
    )
    importer.import_rows(_rows(10), _table(), on_conflict=OnConflictMode.NONE)
    target_bound = importer.load_stats.snapshot()
    assert target_bound["reader_blocked_s"] > 0.05
    assert target_bound["reader_blocked_s"] > target_bound["writer_wait_s"]

    # Source-bound: rows trickle in every 30 ms, the target answers at once.
    def _slow_rows():
        for row in _rows(6):
            time.sleep(0.03)
            yield row

    store = _FakeStore()
    importer = _importer(_connection_factory(store, ["id", "name"], ["id"]), batch_size=1)
    importer.import_rows(_slow_rows(), _table(), on_conflict=OnConflictMode.NONE)
    source_bound = importer.load_stats.snapshot()
    assert source_bound["writer_wait_s"] > 0.05
    assert source_bound["writer_wait_s"] > source_bound["reader_blocked_s"]


def test_a_bound_collector_adds_up_across_the_units_attempts() -> None:
    # A source-drop re-read calls import_rows again on the same unit: one cumulative count.
    stats = LoadStats()
    store = _FakeStore()
    importer = _importer(_connection_factory(store, ["id", "name"], ["id"]), batch_size=2)
    importer.bind_load_stats(stats)
    importer.import_rows(_rows(4), _table(), on_conflict=OnConflictMode.SKIP_EXISTING)
    importer.import_rows(_rows(4), _table(), on_conflict=OnConflictMode.SKIP_EXISTING)
    assert stats.snapshot()["batches"] == 4


def test_the_pool_counts_connects_reconnects_and_failed_connects() -> None:
    stats = LoadStats()
    calls = {"n": 0}

    class _Conn:
        def close(self) -> None:
            pass

    def factory():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("connection timeout expired")
        return _Conn()

    pool = _ConnectionPool(factory, 1, stats=stats)
    with pytest.raises(RuntimeError):
        with pool.lease():
            pass
    with pytest.raises(ValueError):
        with pool.lease():
            raise ValueError("dropped while in use")
    with pool.lease():
        pass
    with pool.lease():
        pass  # reused, no new connect
    s = stats.snapshot()
    assert (s["connect_failures"], s["connects"], s["reconnects"]) == (1, 2, 1)


def test_an_importer_built_without_init_still_counts() -> None:
    # Several tests build a bare importer (BatchedImporter.__new__) to drive one method.
    bare = BatchedImporter.__new__(BatchedImporter)
    assert isinstance(bare.load_stats, LoadStats)
    assert bare.load_stats is bare.load_stats


# --------------------------------------------------------------------------- workers


class _Queue:
    """multiprocessing.Queue stand-in recording what a worker sends."""

    def __init__(self) -> None:
        self.sent: list = []

    def put_nowait(self, message) -> None:
        self.sent.append(message)


def _stats_messages(sent) -> list:
    return [m for m in sent if isinstance(m, tuple) and m and m[0] == engine._LOAD_STATS]


@pytest.mark.parametrize("ending", ["done", "failed", "stopped"])
def test_the_whole_table_worker_returns_its_counters_on_every_outcome(monkeypatch, ending):
    from tests.test_g150_zero_dates import _dated_table, _ui_inputs

    class _Migrator:
        def __init__(self, inputs, **_k):
            pass

        def migrate_table(self, table, *, on_rows, load_stats, **_kw):
            load_stats.record_batch(0.4, failed=False)
            load_stats.record_retry(Exception("no sqlstate: a dropped socket"), exhausted=False)
            on_rows(engine.PROGRESS_FLUSH_ROWS, 0)  # one progress flush
            if ending == "failed":
                raise RuntimeError("boom")
            if ending == "stopped":
                raise engine._FullLoadStopped(table.name)
            return TableLoadResult(rows_loaded=engine.PROGRESS_FLUSH_ROWS)

    queue = _Queue()
    monkeypatch.setattr(engine, "BatchedTableMigrator", _Migrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", queue)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    result = engine._migrate_one_table_in_process(
        engine._TableWorkerArgs(job_id="j", table=_dated_table("orders"), inputs=_ui_inputs())
    )
    assert result.status == {"done": "DONE", "failed": "FAILED", "stopped": "STOPPED"}[ending]
    assert result.load_stats["batches"] == 1
    assert result.load_stats["retries"] == {"conn": 1}
    # Its process to itself: the worker measured the process too.
    assert result.load_stats["cpu_s"] is not None and result.load_stats["wall_s"] >= 0
    # The progress flush carries rows only: the counters travel on their own clock.
    assert ("orders", engine.PROGRESS_FLUSH_ROWS, 0) in queue.sent


class _LiveQueue(_Queue):
    """A worker's queue that notes when a live counter message matching ``want`` arrives
    (the ticker sends from its own thread)."""

    def __init__(self, want) -> None:
        super().__init__()
        self.want = want
        self.seen = threading.Event()

    def put_nowait(self, message) -> None:
        super().put_nowait(message)
        if isinstance(message, tuple) and message and message[0] == engine._LOAD_STATS:
            if self.want(message):
                self.seen.set()


def test_a_worker_sends_its_counters_while_no_batch_completes(monkeypatch) -> None:
    # An OCC storm completes no batch, so no progress flush: counters sent only beside a
    # flush reached the parent when the storm ended, and its OC000s landed in a later window
    # than the minute they happened in -- the reading held against CloudWatch.
    from tests.test_g150_zero_dates import _dated_table, _ui_inputs

    queue = _LiveQueue(lambda m: m[3]["retries"] == {"40001/OC000": 1} and m[3]["batches"] == 0)

    class _Migrator:
        def __init__(self, inputs, **_k):
            pass

        def migrate_table(self, table, *, on_rows, load_stats, **_kw):
            load_stats.record_retry(_DsqlError(_OC000, "40001"), exhausted=False)
            # The storm lasts until the parent has seen it (bounded, so a regression fails).
            assert queue.seen.wait(3.0), "no counters reached the parent during the storm"
            load_stats.record_batch(0.4, failed=False)
            on_rows(2000, 0)
            return TableLoadResult(rows_loaded=2000)

    monkeypatch.setattr(engine, "_LOAD_STATS_SECONDS", 0.01)
    monkeypatch.setattr(engine, "BatchedTableMigrator", _Migrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", queue)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    result = engine._migrate_one_table_in_process(
        engine._TableWorkerArgs(job_id="j", table=_dated_table("orders"), inputs=_ui_inputs())
    )
    assert result.status == "DONE"
    live = _stats_messages(queue.sent)
    assert live and {m[1:3] for m in live} == {("orders", None)}  # a whole-table unit
    # The ticker ends with the unit: nothing more is sent once the worker returned.
    sent = len(queue.sent)
    time.sleep(0.05)
    assert len(queue.sent) == sent


def test_the_ticker_sends_until_stopped_and_never_raises(monkeypatch) -> None:
    monkeypatch.setattr(engine, "_LOAD_STATS_SECONDS", 0.005)
    stats = LoadStats()
    stats.record_batch(0.1, failed=False)
    got: list = []
    ticked = threading.Event()

    def send(snapshot) -> None:
        got.append(snapshot)
        if len(got) >= 3:
            ticked.set()
        raise RuntimeError("a closed pipe")  # telemetry: ignored, the ticker carries on

    stop = engine._start_load_stats_ticker(stats, send)
    assert ticked.wait(2.0)
    stop()
    assert all(s["batches"] == 1 for s in got)
    sent = len(got)
    time.sleep(0.03)
    assert len(got) == sent  # stopped and joined
    assert not [t for t in threading.enumerate() if t.name == "fullload-load-stats"]
    # Nobody keeps the counters: no thread at all.
    engine._start_load_stats_ticker(stats, None)()


def test_the_shard_worker_sends_its_counters_under_its_shard_index(monkeypatch) -> None:
    from tests.test_g137_mysql_reader_shards import _inputs as _shard_inputs, _table as _events

    queue = _LiveQueue(lambda m: True)

    class _Exporter:
        def stream_converted_rows(self, conn, table, **_kw):
            def rows():
                assert queue.seen.wait(3.0), "no counters while the range was read"
                yield from ({"id": i, "email": f"e{i}"} for i in range(1, 4))
            return rows()

    store = _FakeStore()
    importer = _importer(_connection_factory(store, ["id", "email"], ["id"]), batch_size=2)

    class _ShardMigrator(BatchedTableMigrator):
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=_Exporter(), watermark_capturer=object(),
                importer_factory=lambda _i: importer, table_recreator=lambda t: [],
                target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
            )

    monkeypatch.setattr(engine, "_LOAD_STATS_SECONDS", 0.01)
    monkeypatch.setattr(engine, "BatchedTableMigrator", _ShardMigrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", queue)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    result = engine._migrate_shard_in_process(engine._ShardWorkerArgs(
        job_id="j", table=_events(), inputs=_shard_inputs([_events()]), pk_lower=None,
        pk_upper=250, shard_index=2, shared_snapshot_id=None, shard_count=4))

    assert result.status == "DONE" and len(store.rows) == 3
    assert {m[1:3] for m in _stats_messages(queue.sent)} == {(_events().name, 2)}
    assert result.load_stats["batches"] == 2


def test_the_shard_worker_counts_its_range_into_its_own_collector(monkeypatch) -> None:
    from tests.test_g137_mysql_reader_shards import _run_shard

    class _Exporter:
        def stream_converted_rows(self, conn, table, **_kw):
            return iter([{"id": i, "email": f"e{i}"} for i in range(1, 6)])

    store = _FakeStore()
    importer = _importer(_connection_factory(store, ["id", "email"], ["id"]), batch_size=2)
    result, _ = _run_shard(monkeypatch, shared=False, importer=importer, exporter=_Exporter())

    assert result.status == "DONE" and len(store.rows) == 5
    s = result.load_stats
    assert s["batches"] == 3 and s["statements"] == 3 and s["execute_ms"]["n"] == 3
    assert s["connects"] == 1 and s["cpu_s"] is not None


@pytest.mark.parametrize("stopped_first", [False, True])
def test_the_drain_hands_counters_to_the_board_not_the_job(monkeypatch, stopped_first, caplog):
    import queue as queue_module

    updates: list = []

    class _Handle:
        cancelled = False

        def update(self, fn):  # a job write: persists, and stamps the stall watchdog
            updates.append(fn)

    class _Event:
        def __init__(self, s=False):
            self._set = s

        def is_set(self):
            return self._set

        def set(self):
            self._set = True

    snapshot = LoadStats().snapshot()
    snapshot["batches"] = 7
    q: "queue_module.Queue[object]" = queue_module.Queue()
    q.put((engine._LOAD_STATS, "app.orders", 2, snapshot))
    q.put(engine._PROGRESS_SENTINEL)
    board = LoadStatsBoard()
    engine._drain_progress_queue(q, _Handle(), _Event(), _Event(stopped_first), board)
    assert board.table("app.orders")["batches"] == 7
    assert not updates
    assert "could not apply a message" not in caplog.text

    # Without a board the counters are dropped quietly -- never a drain warning.
    q.put((engine._LOAD_STATS, "app.orders", 2, snapshot))
    q.put(engine._PROGRESS_SENTINEL)
    engine._drain_progress_queue(q, _Handle(), _Event(), _Event(stopped_first))
    assert not updates and "could not apply a message" not in caplog.text


def _unit_snapshot(batches: int, *, cpu: float = 1.0) -> dict:
    stats = LoadStats()
    for _ in range(batches):
        stats.record_batch(0.5, failed=False)
        stats.record_statement(1)
    snapshot = stats.snapshot()
    snapshot.update(cpu_s=cpu, wall_s=2.0, peak_rss_bytes=1 << 20)
    return snapshot


def test_a_parallel_run_merges_its_shards_and_tables_on_the_board(monkeypatch) -> None:
    from tests.test_full_load_overlay import _InlineExecutor
    from tests.test_g137_mysql_reader_shards import (
        _FOUR, _UQ_EMAIL, _Handle, _IndexImporter, _Planner, _inputs, _table as _events, _wm,
    )

    def _shard(args):
        # A live snapshot first (as a flush sends it), then the final with the result.
        engine._report_progress(
            engine._worker_progress_queue,
            (engine._LOAD_STATS, args.table.name, args.shard_index, _unit_snapshot(1)),
        )
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=10,
                                  shard_index=args.shard_index,
                                  load_stats=_unit_snapshot(5 + args.shard_index))

    def _whole(args):
        return _TableWorkerResult(table_name=args.table.name, status="DONE", rows_loaded=40,
                                  load_stats=_unit_snapshot(2, cpu=3.0))

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")

    big, small = _events("app.events"), _events("app.small")
    inputs = _inputs([big, small], replace=("app.events",))
    migrator = BatchedTableMigrator(
        inputs, exporter=_Planner({"app.events": _FOUR, "app.small": [(None, None)]}),
        watermark_capturer=object(), importer_factory=lambda _i: _IndexImporter(),
        # A recreated table hands back its index DDLs; the parent builds them after the
        # shards ("index", which loads no rows itself).
        table_recreator=lambda t: [_UQ_EMAIL.replace("UNIQUE INDEX", "INDEX")],
        target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
        target_generated_reader=lambda t: set(),
        target_unique_keys_reader=lambda t: [list(t.primary_key)],
    )
    handle = _Handle(["app.events", "app.small"])
    board = LoadStatsBoard()
    engine._migrate_tables_in_parallel(
        handle, "j", [big, small], migrator, ErrorLogStore(),
        watermark=_wm(), fresh_watermark=True, load_stats=board,
    )

    events = board.table("app.events")
    # Four shards, each its FINAL snapshot (5, 6, 7, 8 batches) -- never the live one --
    # and the parent's index pass, which adds no unit.
    assert events["units"] == 4 and events["batches"] == 5 + 6 + 7 + 8
    assert events["cpu_s"] == 4.0
    assert board.table("app.small")["batches"] == 2
    run = board.run()
    assert run["units"] == 5 and run["batches"] == 28 and run["cpu_s"] == 7.0
    assert {c.chunk_id: c.status for c in handle.job.chunks} == {
        "app.events": "DONE", "app.small": "DONE"}


def test_the_thread_path_records_a_loaded_tables_counters() -> None:
    from tests.test_g150_zero_dates import _dated_table, _Handle, _ResultMigrator

    board = LoadStatsBoard()
    outcome = engine._migrate_one_table(
        _Handle(), "job-1", _dated_table("orders"),
        _ResultMigrator(TableLoadResult(rows_loaded=6, load_stats=_unit_snapshot(3))),
        ErrorLogStore(), load_stats=board,
    )
    assert outcome is engine._TableLoadOutcome.LOADED
    assert board.table("orders")["batches"] == 3


class _ThreadPathMigrator:
    """A DataMigrator double that takes the table's collector, as BatchedTableMigrator
    does: one batch per attempt, the first attempt losing its source connection."""

    def __init__(self, ending: str) -> None:
        self.ending, self.attempts = ending, 0

    def migrate_table(self, table, *, on_rows=None, should_cancel=None, load_stats=None):
        from tests.test_ui_data_migration import _lost_connection

        self.attempts += 1
        load_stats.record_batch(0.2, failed=False)
        if self.attempts == 1:
            raise _lost_connection()  # the source dropped: the table is re-read
        if self.ending == "failed":
            raise RuntimeError("boom")
        if self.ending == "stopped":
            raise engine._FullLoadStopped(table.name)
        return TableLoadResult(rows_loaded=2, load_stats=load_stats.snapshot())


@pytest.mark.parametrize("ending, expected", [
    ("done", "LOADED"), ("failed", "FAILED"), ("stopped", "STOPPED"),
])
def test_the_thread_path_counts_across_a_re_read_and_on_every_outcome(
    monkeypatch, ending, expected
) -> None:
    # tp=1 loads in the run's own process. Its counters were recorded only for a table
    # that loaded, and only from the last attempt: a failed or stopped table reported none,
    # and a source-drop re-read lost the first attempt's retries and connects.
    from tests.test_g150_zero_dates import _dated_table, _Handle

    monkeypatch.setattr(engine._time, "sleep", lambda _s: None)  # no re-read backoff
    monkeypatch.setattr(engine, "log_activity", lambda *a, **k: None)
    board = LoadStatsBoard()
    migrator = _ThreadPathMigrator(ending)
    outcome = engine._migrate_one_table(
        _Handle(), "job-1", _dated_table("orders"), migrator, ErrorLogStore(), load_stats=board,
    )
    assert outcome is getattr(engine._TableLoadOutcome, expected)
    assert migrator.attempts == 2
    table = board.table("orders")
    assert table["units"] == 1 and table["batches"] == 2  # both attempts, one collector
    assert "cpu_s" not in table  # the run's own process: no CPU of its own to report


def test_the_thread_path_puts_live_counters_on_the_board(monkeypatch) -> None:
    from tests.test_g150_zero_dates import _dated_table, _Handle

    monkeypatch.setattr(engine, "_LOAD_STATS_SECONDS", 0.01)
    board = LoadStatsBoard()

    class _Migrator:
        def migrate_table(self, table, *, on_rows=None, should_cancel=None, load_stats=None):
            load_stats.record_retry(_DsqlError(_OC001, "40001"), exhausted=False)
            deadline = time.monotonic() + 3.0
            while not board.table("orders").get("retries") and time.monotonic() < deadline:
                time.sleep(0.005)
            assert board.table("orders")["retries"] == {"40001/OC001": 1}, "not live"
            return TableLoadResult(rows_loaded=1, load_stats=load_stats.snapshot())

    outcome = engine._migrate_one_table(
        _Handle(), "job-1", _dated_table("orders"), _Migrator(), ErrorLogStore(),
        load_stats=board,
    )
    assert outcome is engine._TableLoadOutcome.LOADED
    # A double of the protocol that takes no collector is never handed one.
    assert engine._takes_load_stats(_Migrator()) is True
    assert engine._takes_load_stats(SimpleNamespace(migrate_table=lambda table, **kw: 0)) is False
    assert engine._takes_load_stats(object()) is False


def test_migrate_table_times_the_post_load_count_apart_from_the_load() -> None:
    from tests.test_ui_data_migration import (
        _FakeExporter, _FakeWatermarkCapturer, _inputs as _base_inputs, _watermark,
    )

    counted: list = []

    def _counter(table):
        counted.append(table.name)
        time.sleep(0.02)
        return 6

    store = _FakeStore()
    importer = _importer(_connection_factory(store, ["id"], ["id"]), batch_size=2)
    inputs = dataclasses.replace(_base_inputs(), replace_tables=frozenset({"orders"}))
    migrator = BatchedTableMigrator(
        inputs,
        exporter=_FakeExporter(rows_by_table={"orders": [{"id": i} for i in range(1, 7)]}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
        table_recreator=lambda t: [],
        target_counter=_counter,
        target_generated_reader=lambda t: set(),
    )
    unit = LoadStats()
    result = migrator.migrate_table(inputs.inventory.tables[0], load_stats=unit)

    assert result.rows_loaded == 6 and counted == ["orders"]
    s = result.load_stats
    assert s == unit.snapshot()
    assert s["batches"] == 3 and s["count_s"] >= 0.02 and s["load_s"] > 0.0


def test_run_full_load_hands_its_board_to_the_parallel_runner() -> None:
    import inspect

    assert "load_stats" in inspect.signature(engine.run_full_load).parameters
    src = inspect.getsource(engine.run_full_load)
    assert "load_stats=load_stats" in src.split("_migrate_tables_in_parallel(")[1][:300]


def test_a_failed_shard_returns_its_counters_too(monkeypatch) -> None:
    from tests.test_g137_mysql_reader_shards import _run_shard

    result, _ = _run_shard(monkeypatch, shared=False)  # a lost source connection
    assert result.status == "FAILED"
    assert result.load_stats["units"] == 1 and result.load_stats["cpu_s"] is not None


def test_the_counters_cross_the_process_boundary() -> None:
    # Spawned workers pickle both the live message and the result.
    import pickle

    snapshot = LoadStats(process_clock=True).snapshot()
    message = (engine._LOAD_STATS, "app.t", 3, snapshot)
    assert pickle.loads(pickle.dumps(message)) == message
    result = _TableWorkerResult(table_name="app.t", status="DONE", load_stats=snapshot)
    assert pickle.loads(pickle.dumps(result)).load_stats == snapshot
    assert _TableWorkerResult(table_name="t", status="DONE").load_stats == {}


def test_worker_results_without_counters_are_ignored(monkeypatch) -> None:
    # A double's result carries no load_stats: recording it must be a quiet no-op.
    board = LoadStatsBoard()
    engine._record_load_stats(board, "t", None, {}, final=True)
    engine._record_load_stats(board, "t", None, None, final=True)
    engine._record_load_stats(None, "t", None, _unit_snapshot(1))
    assert board.run() == {}
    engine._bind_load_stats(SimpleNamespace(), LoadStats())  # no bind method: nothing
