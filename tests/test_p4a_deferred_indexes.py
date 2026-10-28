# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P4a: a run's secondary-index DDL waits until every table's data has loaded.

A ``CREATE INDEX ASYNC`` changes the schema, so each one a finished table submitted while
others still loaded made their open transactions fail OC001 (40001 "schema has been updated
by another transaction"): live, 118-265 retries a perf_ecom run, each discarding a 2000-row
commit. Pinned here, on doubles only (no MySQL, no Aurora DSQL, no AWS):

* both paths (the process pool and the in-process thread path) leave a recreated table's
  index DDLs to ONE serial pass after every unit has returned -- a whole table's from its
  worker, a sharded table's from the recreate pre-pass -- largest table first, one table at a
  time, on one connection (``BatchedImporter.index_pass``);
* only a DONE table gets its indexes (quarantine-only and promoted tables included), a
  failed index never fails the table, and the pass runs also after a Stop, a raising pool and
  with nothing to load (a retry's carried flags);
* a Stop requested during the pass, or Aurora DSQL not answering
  (:class:`IndexTargetUnreachable`, also when the pass cannot connect at all), leaves the
  rest flagged ``indexes_pending``: the run is then incomplete (never DONE) with no data
  missing, and Retry unfinished tables (``index_only``) submits only the indexes not on the
  table yet, without reloading it, recording any failure where the screen reads that
  table's records (its owner), and never replacing it;
* after a table-set change a retry leaves a table no longer selected as is (G12);
* a reload, and a fresh run, carry the flag; the payload stays loadable by older builds;
* the step's verdict, the recovery row and the notice follow the flag.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import dataclasses
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    IndexTargetUnreachable,
)
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import ChunkState, MigrationJob, StepStatus
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    FullLoadIncompleteError,
    TableLoadResult,
    _PERSISTED_ERROR_CAP,
    _RunCounts,
    _TableWorkerResult,
    _finalize_run,
    _seed_retry_chunks,
    _stored_index_name,
    full_load_progress_caption,
    index_pending_load_message,
    run_full_load,
    run_full_load_retry,
)

import tests.test_g137_mysql_reader_shards as g137
from tests.test_full_load_overlay import _InlineExecutor
from tests.test_ui_data_migration import (
    _FakeExporter,
    _FakeImporter,
    _FakeWatermarkCapturer,
    _watermark,
)

A, B, C = "app.a", "app.b", "app.c"


class _Error(Exception):
    """A psycopg-like error: a message and its SQLSTATE."""

    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.sqlstate = sqlstate


@pytest.fixture(autouse=True)
def _indexes_after_load(monkeypatch):
    """P4d puts a Drop & reload table's secondary indexes on the EMPTY table before its rows
    (tests/test_p4d_indexes_before_load.py); the pass after every table's data pinned here
    stays as its fallback, and is P4a's whole order again under P4d's measurement seam."""
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_INDEXES_AFTER_LOAD", "1")


def _ddl(name: str, table: str, *, unique: bool = False) -> str:
    return (
        f'CREATE {"UNIQUE " if unique else ""}INDEX ASYNC "{name}" ON "app"."{table}" '
        '("email")'
    )


# ---------------------------------------------------------------------------
# The rig: the process path with its workers inline, every event recorded
# ---------------------------------------------------------------------------


class _PassImporter:
    """The run's index pass on Aurora DSQL, recorded: one pass context, one create per
    table. ``fail_on`` maps a create call (from 1) to an exception it raises; ``on_call``
    runs before each create's answer."""

    def __init__(self, events: list) -> None:
        self.events = events
        self.fail_on: dict = {}
        self.on_call = None
        self.calls = 0

    @contextlib.contextmanager
    def index_pass(self):
        self.events.append(("pass-open",))
        try:
            yield self._create
        finally:
            self.events.append(("pass-close",))

    def _create(self, ddls):
        self.calls += 1
        self.events.append(("index", list(ddls)))
        if self.on_call is not None:
            self.on_call(self.calls)
        error = self.fail_on.get(self.calls)
        if error is not None:
            raise error
        return len(ddls), []


@pytest.fixture
def prig(monkeypatch):
    """``_migrate_tables_in_parallel`` on the process path, workers inline; ``events``
    records every unit run, chunk completion and index submission in order."""
    events: list = []
    whole: dict = {}          # table -> {"status", "ddls", "quarantine"}
    shard_status: dict = {}   # (table, shard index) -> status
    hooks: dict = {}          # table -> called while its whole-table unit runs
    importer = _PassImporter(events)
    activity: list = []

    def _shard(args):
        events.append(("unit", args.table.name, args.shard_index))
        status = shard_status.get((args.table.name, args.shard_index), "DONE")
        return _TableWorkerResult(table_name=args.table.name, status=status, rows_loaded=10,
                                  shard_index=args.shard_index)

    def _whole(args):
        name = args.table.name
        events.append(("unit", name, None))
        if name in hooks:
            hooks[name]()
        spec = whole.get(name, {})
        status = spec.get("status", "DONE")
        return _TableWorkerResult(
            table_name=name, status=status, rows_loaded=40,
            error_message=None if status == "DONE" else "boom",
            quarantine_records=tuple(spec.get("quarantine", ())),
            index_ddls=tuple(spec.get("ddls", ())) if status == "DONE" else (),
        )

    real_complete = engine._complete_chunk

    def _complete(job, chunk_id, *a, **kw):
        events.append(("complete", chunk_id, kw.get("indexes_pending")))
        return real_complete(job, chunk_id, *a, **kw)

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(engine, "_migrate_shard_in_process", _shard)
    monkeypatch.setattr(engine, "_migrate_one_table_in_process", _whole)
    monkeypatch.setattr(engine, "_complete_chunk", _complete)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "_open_shared_snapshot", lambda m, d: (None, None, "snap-1"))
    monkeypatch.setattr(engine, "log_activity", lambda _cat, action, **kw: activity.append(
        (action, getattr(kw.get("status"), "value", None), kw.get("detail"))))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")
    _InlineExecutor.submitted = []

    def run(tables, *, replace=(), counts=None, ranges=None, recreate=None, handle=None,
            index_reader=None, conversions=None, inventory=None):
        migrator = BatchedTableMigrator(
            g137._inputs(inventory or tables, replace=replace, conversions=conversions),
            exporter=g137._Planner(ranges or {}), watermark_capturer=object(),
            importer_factory=lambda _i: importer,
            table_recreator=lambda t: list((recreate or {}).get(t.name, ())),
            target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(),
            target_unique_keys_reader=lambda t: [list(t.primary_key)],
            target_index_reader=index_reader or (lambda t: set()),
        )
        handle = handle or g137._Handle([t.name for t in tables])
        log = ErrorLogStore()
        counts = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, log, watermark=g137._wm(counts=counts or {}),
            fresh_watermark=True,
        )
        return SimpleNamespace(
            counts=counts, handle=handle, log=log, migrator=migrator,
            chunks={c.chunk_id: c for c in handle.job.chunks},
            errors=[r.message for r in log.records("j")],
        )

    run.events, run.whole, run.shard_status, run.hooks = events, whole, shard_status, hooks
    run.importer, run.activity = importer, activity
    return run


def _first(events, kind) -> int:
    return next(i for i, e in enumerate(events) if e[0] == kind)


def _last(events, kind) -> int:
    return max(i for i, e in enumerate(events) if e[0] == kind)


def _submitted(events) -> list:
    return [e[1] for e in events if e[0] == "index"]


def _pass_lines(activity) -> list:
    return [(status, detail) for action, status, detail in activity if action == "index pass"]


# ---------------------------------------------------------------------------
# 1. The process path
# ---------------------------------------------------------------------------


def test_no_index_ddl_is_submitted_before_the_last_unit_result(prig) -> None:
    ddl_a, ddl_b = _ddl("ix_a", "a"), _ddl("ix_b", "b")
    prig.whole.update({A: {"ddls": [ddl_a]}, B: {"ddls": [ddl_b]}})
    r = prig([g137._table(A), g137._table(B), g137._table(C)], replace=[A, B],
             counts={A: 10, B: 500, C: 20}, recreate={A: ["pre-pass a"], B: ["pre-pass b"]})
    events = prig.events
    # Every unit ran and every chunk completed before the pass opened.
    assert _last(events, "unit") < _first(events, "pass-open")
    assert _last(events, "complete") < _first(events, "pass-open")
    # A whole-table unit's own DDLs (its worker may have recreated it), once each, largest
    # estimate first, on one pass; a table with none (C, an append) is not submitted.
    assert _submitted(events) == [[ddl_b], [ddl_a]]
    assert [e[0] for e in events].count("pass-open") == 1
    assert ("complete", A, True) in events and ("complete", C, False) in events
    assert all(r.chunks[n].status == "DONE" and not r.chunks[n].indexes_pending
               for n in (A, B, C))
    assert r.counts == (0, 0, 0)
    ((status, detail),) = _pass_lines(prig.activity)
    assert status == "info" and detail == (
        "every table's data has loaded: submitting 2 secondary index(es) of 2 table(s), one "
        "at a time")


def test_a_sharded_replace_table_gets_its_pre_pass_ddls_once_after_its_shards(prig) -> None:
    ddl = _ddl("ix_events", "events")
    r = prig([g137._table(g137.EVENTS)], replace=[g137.EVENTS],
             ranges={g137.EVENTS: g137._FOUR}, recreate={g137.EVENTS: [ddl]})
    units = [e for e in prig.events if e[0] == "unit"]
    assert units == [("unit", g137.EVENTS, i) for i in range(4)]
    assert _submitted(prig.events) == [[ddl]]
    assert _last(prig.events, "unit") < _first(prig.events, "pass-open")
    assert r.chunks[g137.EVENTS].status == "DONE" and not r.chunks[g137.EVENTS].indexes_pending


@pytest.mark.parametrize("status", ["FAILED", "STOPPED"])
def test_a_table_that_did_not_load_gets_no_index_and_no_flag(prig, status) -> None:
    prig.whole.update({A: {"status": status, "ddls": [_ddl("ix_a", "a")]},
                       B: {"ddls": [_ddl("ix_b", "b")]}})
    r = prig([g137._table(A), g137._table(B)], replace=[A, B])
    assert _submitted(prig.events) == [[_ddl("ix_b", "b")]]
    assert r.chunks[A].status == "FAILED" and not r.chunks[A].indexes_pending
    # A sharded table with one shard that did not finish: none either.
    prig.events.clear()
    prig.shard_status[(g137.EVENTS, 2)] = status
    r = prig([g137._table(g137.EVENTS)], replace=[g137.EVENTS],
             ranges={g137.EVENTS: g137._FOUR}, recreate={g137.EVENTS: ["ix"]})
    assert _submitted(prig.events) == [] and not r.chunks[g137.EVENTS].indexes_pending
    assert r.counts.index_pending == 0


def test_a_quarantine_only_table_and_a_promoted_table_get_their_indexes(prig) -> None:
    quarantined = ({"primary_key": "id=7", "message": "value too large", "error_code": "54000"},)
    prig.whole.update({
        A: {"ddls": [_ddl("ix_a", "a")], "quarantine": quarantined},
        # B is no replace table: its worker promoted it (an empty target, a changed key)
        # and recreated it itself, so the DDLs come back with its result.
        B: {"ddls": [_ddl("ix_b", "b", unique=True)]},
    })
    r = prig([g137._table(A), g137._table(B)], replace=[A], counts={A: 50, B: 5})
    assert _submitted(prig.events) == [[_ddl("ix_a", "a")], [_ddl("ix_b", "b", unique=True)]]
    assert r.counts == (0, 1, 0)  # the quarantine still counts; the indexes do not
    assert r.chunks[A].rows_quarantined == 1 and not r.chunks[A].indexes_pending


def test_the_pass_goes_largest_first_unknown_last_then_by_name(prig) -> None:
    names = ["app.x", "app.w", "app.v", "app.u"]
    prig.whole.update({n: {"ddls": [f"ddl {n}"]} for n in names})
    prig([g137._table(n) for n in names], replace=names,
         counts={"app.x": 5, "app.w": 900, "app.u": 5})
    # w (900) first, then u and x (5 each, by name); v has no estimate: last.
    assert _submitted(prig.events) == [["ddl app.w"], ["ddl app.u"], ["ddl app.x"],
                                       ["ddl app.v"]]


def test_a_stop_before_the_pass_still_submits_the_loaded_tables_indexes(prig) -> None:
    handle = g137._Handle([A, B, C])
    # The operator stops while A loads: B and C are never submitted.
    prig.hooks[A] = lambda: setattr(handle, "cancelled", True)
    prig.whole.update({n: {"ddls": [f"ddl {n}"]} for n in (A, B, C)})
    r = prig([g137._table(n) for n in (A, B, C)], replace=[A, B, C], handle=handle,
             counts={A: 3, B: 2, C: 1})
    assert [e[1] for e in prig.events if e[0] == "unit"] == [A]
    assert _submitted(prig.events) == [["ddl app.a"]]
    assert r.chunks[A].status == "DONE" and not r.chunks[A].indexes_pending
    assert {r.chunks[B].status, r.chunks[C].status} == {"FAILED"}  # retryable
    assert r.counts.index_pending == 0
    assert not [line for line in _pass_lines(prig.activity) if "stopped" in line[1]]


def test_a_stop_during_the_pass_leaves_the_rest_flagged(prig) -> None:
    handle = g137._Handle([A, B, C])
    prig.whole.update({n: {"ddls": [f"ddl {n}"]} for n in (A, B, C)})
    prig.importer.on_call = lambda n: setattr(handle, "cancelled", True) if n == 1 else None
    r = prig([g137._table(n) for n in (A, B, C)], replace=[A, B, C], handle=handle,
             counts={A: 3, B: 2, C: 1})
    assert _submitted(prig.events) == [["ddl app.a"]]
    assert not r.chunks[A].indexes_pending
    assert r.chunks[B].indexes_pending and r.chunks[C].indexes_pending
    assert all(c.status == "DONE" for c in r.chunks.values())  # their data is loaded
    assert r.counts.index_pending == 2
    assert _pass_lines(prig.activity)[-1] == ("info", (
        "stopped by the operator before the secondary indexes of 2 table(s) were submitted "
        "(their data is loaded): Retry unfinished tables submits them without reloading "
        "data"))
    # The run ends as a Stop, not as an incomplete run.
    prig.activity.clear()
    _finalize_run(handle, "j", [A, B, C], r.counts, ErrorLogStore(),
                  accept_quarantined_rows=False)
    assert [action for action, _s, _d in prig.activity] == ["run stopped"]


def test_a_raising_pool_still_runs_the_pass_and_keeps_the_exception(prig, monkeypatch) -> None:
    from concurrent.futures.process import BrokenProcessPool

    later = concurrent.futures.Future()

    class _ThenFuture(concurrent.futures.Future):
        """Its result breaks the pool for the next one (deterministic order)."""

        def result(self, timeout=None):
            value = super().result(timeout)
            if not later.done():
                later.set_exception(BrokenProcessPool("a worker died"))
            return value

    class _Executor(_InlineExecutor):
        def submit(self, fn, *args):
            if args[0].table.name == B:
                return later
            future = _ThenFuture()
            future.set_result(fn(*args))
            return future

    monkeypatch.setattr(engine, "ProcessPoolExecutor", _Executor)
    prig.whole.update({A: {"ddls": ["ddl a"]}, B: {"ddls": ["ddl b"]}})
    with pytest.raises(BrokenProcessPool):
        prig([g137._table(A), g137._table(B)], replace=[A, B], counts={A: 9, B: 1})
    assert _submitted(prig.events) == [["ddl a"]]  # the loaded table got its indexes


def test_a_raising_teardown_still_runs_the_pass_and_keeps_its_exception(
        prig, monkeypatch) -> None:
    def _broken(*_a):
        raise RuntimeError("the anchor could not be closed")

    monkeypatch.setattr(engine, "_close_shared_snapshot", _broken)
    prig.whole.update({A: {"ddls": ["ddl a"]}})
    with pytest.raises(RuntimeError, match="anchor could not be closed"):
        prig([g137._table(A)], replace=[A])
    assert _submitted(prig.events) == [["ddl a"]]


def test_an_unreachable_target_leaves_the_table_and_the_rest_flagged(prig) -> None:
    prig.whole.update({n: {"ddls": [f"ddl {n}"]} for n in (A, B, C)})
    unreachable = IndexTargetUnreachable(1, ["UNIQUE ix_b0: 54000 too many indexes"])
    unreachable.__cause__ = _Error("server closed the connection unexpectedly", "08006")
    prig.importer.fail_on[2] = unreachable
    r = prig([g137._table(n) for n in (A, B, C)], replace=[A, B, C], counts={A: 3, B: 2, C: 1})
    assert _submitted(prig.events) == [["ddl app.a"], ["ddl app.b"]]  # C never tried
    assert not r.chunks[A].indexes_pending
    assert r.chunks[B].indexes_pending and r.chunks[C].indexes_pending
    assert r.counts == (0, 0, 2)
    # The permanent failure it carried is recorded, as any index failure is.
    assert any("UNIQUE ix_b0" in e and e.startswith("index not created") for e in r.errors)
    assert _pass_lines(prig.activity)[-1] == ("warning", (
        "Aurora DSQL did not answer while secondary indexes were submitted: 2 table(s) still "
        "need them (their data is loaded); Retry unfinished tables submits them without "
        "reloading data (last attempt: 08006)"))


def test_a_failed_index_never_fails_the_table(prig) -> None:
    class _Failing(_PassImporter):
        def _create(self, ddls):
            super()._create(ddls)
            return 0, [f"UNIQUE {d}: 23505 could not create unique index" for d in ddls]

    prig.importer.__class__ = _Failing
    prig.whole.update({A: {"ddls": ["ix_a"]}})
    r = prig([g137._table(A)], replace=[A])
    assert r.chunks[A].status == "DONE" and not r.chunks[A].indexes_pending
    assert r.counts == (0, 0, 0)
    assert r.errors == [
        "index not created: UNIQUE ix_a: 23505 could not create unique index — the table's "
        "DATA loaded completely; add the index later or reduce the table's index count "
        "(Aurora DSQL allows 24 per table, including the primary key)"]


def test_with_nothing_to_load_the_pass_completes_the_carried_flags(prig) -> None:
    conv = TableConversion(table=A, target_ddl=g137._DDL,
                           index_ddls=[_ddl("ix_old", "a"), _ddl("ix_new", "a")])
    handle = g137._Handle([A, B])
    handle.job.chunks = [ChunkState(chunk_id=A, status="DONE", indexes_pending=True),
                         ChunkState(chunk_id=B, status="DONE", indexes_pending=True)]
    reads: list = []
    # B was left flagged by an earlier run but is not in this run's inventory: it stays.
    r = prig([], handle=handle, conversions={A: conv}, inventory=[g137._table(A)],
             index_reader=lambda t: reads.append(t.name) or {"ix_old"})
    # One catalog read by relation; only the index not on it yet is submitted.
    assert reads == [A]
    assert _submitted(prig.events) == [[_ddl("ix_new", "a")]]
    assert not r.chunks[A].indexes_pending and r.chunks[B].indexes_pending
    assert r.counts == (0, 0, 1)
    # Every index already there: no submission at all, the flag cleared.
    prig.events.clear()
    handle.job.chunks[0].indexes_pending = True
    r = prig([], handle=handle, conversions={A: conv}, inventory=[g137._table(A)],
             index_reader=lambda t: {"ix_old", "ix_new"})
    assert _submitted(prig.events) == [] and not r.chunks[A].indexes_pending


# ---------------------------------------------------------------------------
# 2. The thread path (tp=1): a real BatchedTableMigrator over fakes
# ---------------------------------------------------------------------------


class _RecordingImporter(_FakeImporter):
    """The load's importer and the run's index pass, both into one event list."""

    def __init__(self, events: list, **kw) -> None:
        super().__init__(**kw)
        self.events = events

    def import_rows(self, rows, table, **kw):
        self.events.append(("rows", table.name, kw.get("index_ddls")))
        return super().import_rows(rows, table, **kw)

    @contextlib.contextmanager
    def index_pass(self):
        self.events.append(("pass-open",))
        yield lambda ddls: (self.events.append(("index", list(ddls))), (len(ddls), []))[1]
        self.events.append(("pass-close",))


def _thread_migrator(events, *, replace=(), recreate=None, index_reader=None,
                     conversions=None):
    """A real BatchedTableMigrator over orders and customers, its importer recorded."""
    from tests.test_ui_data_migration import _inputs, _tables

    inputs = dataclasses.replace(
        _inputs(), replace_tables=frozenset(replace),
        table_conversions=dict(conversions or {}),
    )
    importer = _RecordingImporter(events)
    return BatchedTableMigrator(
        inputs,
        exporter=_FakeExporter({"orders": [{"id": 1}, {"id": 2}], "customers": [{"id": 3}]}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
        table_recreator=lambda t: list((recreate or {}).get(t.name, ())),
        target_counter=lambda t: None,
        target_index_reader=index_reader or (lambda t: set()),
    ), _tables()


def _run(job_fn) -> MigrationJob:
    manager = JobManager()
    job_id = manager.submit(job_fn)
    assert manager.wait(job_id, timeout=10.0)
    job = manager.get_status(job_id)
    job._error = manager.get_error(job_id)  # type: ignore[attr-defined]
    return job


def test_the_thread_path_defers_the_ddls_until_every_table_has_loaded(monkeypatch) -> None:
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    events: list = []
    migrator, tables = _thread_migrator(
        events, replace=["orders", "customers"],
        recreate={"orders": ["ix orders"], "customers": ["ix customers"]})
    job = _run(lambda h: run_full_load(h, tables, migrator=migrator, error_log=ErrorLogStore()))
    assert job.status == "DONE", job._error
    # The loads got no index DDL; the pass submitted both after them, largest first.
    assert events == [
        ("rows", "orders", None), ("rows", "customers", None),
        ("pass-open",), ("index", ["ix orders"]), ("index", ["ix customers"]), ("pass-close",),
    ]
    assert not any(c.indexes_pending for c in job.chunks)


def test_the_thread_path_runs_the_pass_when_its_loop_raises(monkeypatch) -> None:
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    events: list = []
    migrator, tables = _thread_migrator(
        events, replace=["orders", "customers"],
        recreate={"orders": ["ix orders"], "customers": ["ix customers"]})
    real = engine._migrate_one_table

    def _second_raises(handle, job_id, table, *a, **kw):
        if table.name == "customers":
            raise RuntimeError("the job store is gone")
        return real(handle, job_id, table, *a, **kw)

    monkeypatch.setattr(engine, "_migrate_one_table", _second_raises)
    handle = g137._Handle(["orders", "customers"])
    with pytest.raises(RuntimeError, match="job store is gone"):
        engine._migrate_tables_in_parallel(handle, "j", tables, migrator, ErrorLogStore(),
                                           watermark=_watermark())
    assert events[-3:] == [("pass-open",), ("index", ["ix orders"]), ("pass-close",)]
    assert not handle.job.chunks[0].indexes_pending


def test_a_library_caller_keeps_the_inline_index_pass() -> None:
    events: list = []
    migrator, tables = _thread_migrator(events, replace=["orders"], recreate={"orders": ["ix"]})
    result = migrator.migrate_table(tables[0])
    assert events == [("rows", "orders", ["ix"])] and result.deferred_index_ddls == ()
    events.clear()
    deferred = migrator.migrate_table(tables[0], defer_indexes=True)
    assert events == [("rows", "orders", None)] and deferred.deferred_index_ddls == ("ix",)


def test_a_replace_count_shortfall_raises_and_owes_no_index() -> None:
    events: list = []
    migrator, tables = _thread_migrator(events, replace=["orders"], recreate={"orders": ["ix"]})
    migrator._target_counter = lambda t: 1  # 2 rows loaded, 1 on the target
    with pytest.raises(RuntimeError, match="silent row loss"):
        migrator.migrate_table(tables[0], defer_indexes=True)


def test_a_whole_table_worker_defers_and_returns_its_ddls(monkeypatch) -> None:
    seen: dict = {}

    class _Migrator:
        def __init__(self, inputs, **_k):
            pass

        def migrate_table(self, table, **kwargs):
            seen.update(kwargs)
            return TableLoadResult(rows_loaded=2, deferred_index_ddls=("ix 1", "ix 2"))

    monkeypatch.setattr(engine, "BatchedTableMigrator", _Migrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    from tests.test_g150_zero_dates import _dated_table, _ui_inputs

    result = engine._migrate_one_table_in_process(engine._TableWorkerArgs(
        job_id="j", table=_dated_table("orders"), inputs=_ui_inputs()))
    assert seen["defer_indexes"] is True
    assert result.status == "DONE" and result.index_ddls == ("ix 1", "ix 2")


# ---------------------------------------------------------------------------
# 3. The run's verdict, the retry that completes it, and the carried flags
# ---------------------------------------------------------------------------


def test_the_index_pending_message_fits_the_persisted_cap() -> None:
    message = index_pending_load_message(pending=12345)
    assert len(message) <= _PERSISTED_ERROR_CAP, len(message)
    assert "No data is missing" in message and "'Retry unfinished tables'" in message


class _CancelHandle:
    def __init__(self, cancelled=False, chunks=()):
        self.cancelled = cancelled
        self.job = MigrationJob(job_id="j", chunks=list(chunks))

    def snapshot(self):
        return self.job

    def update(self, fn):
        fn(self.job)

    def heartbeat(self):
        pass


def _finalize(counts, *, cancelled=False, accepted=False, log=None):
    lines: list = []
    engine_log = engine.log_activity
    engine.log_activity = lambda _c, action, **kw: lines.append((action, kw.get("detail")))
    try:
        _finalize_run(_CancelHandle(cancelled), "j", [A, B, C], counts, log or ErrorLogStore(),
                      accept_quarantined_rows=accepted)
        return lines, None
    except FullLoadIncompleteError as exc:
        return lines, str(exc)
    finally:
        engine.log_activity = engine_log


def test_pending_indexes_alone_make_the_run_incomplete() -> None:
    lines, error = _finalize(_RunCounts(0, 0, index_pending=2))
    assert error == index_pending_load_message(pending=2)
    assert [a for a, _d in lines] == ["run incomplete"]
    assert lines[0][1].startswith("2 loaded table(s) still need their secondary indexes")
    # A Stop keeps the stop path; nothing pending: complete.
    assert [a for a, _d in _finalize(_RunCounts(0, 0, 2), cancelled=True)[0]] == ["run stopped"]
    assert _finalize(_RunCounts(0, 0, 0))[1] is None


def test_an_accepted_gap_does_not_wave_pending_indexes_through() -> None:
    lines, error = _finalize(_RunCounts(0, 1, index_pending=1), accepted=True)
    assert error == index_pending_load_message(pending=1)
    assert "run completed (quarantine accepted)" not in [a for a, _d in lines]
    # Without pending indexes the accepted gap completes, as before.
    lines, error = _finalize(_RunCounts(0, 1, 0), accepted=True)
    assert error is None and lines[0][0] == "run completed (quarantine accepted)"


def test_a_real_failure_keeps_its_message_and_names_the_pending_indexes() -> None:
    lines, error = _finalize(_RunCounts(1, 0, index_pending=2))
    assert error.startswith("Full Load incomplete: 1 of 3 table(s) did not fully load")
    assert "'Retry unfinished tables'" in error
    ((action, detail),) = lines
    assert action == "run incomplete"
    assert "2 loaded table(s) still need their secondary indexes" in detail


def _index_only_rig(monkeypatch, *, reader):
    """A real BatchedTableMigrator over a and b (thread path), b loaded with its indexes
    pending in ``prior``; the conversion gives a one index and b two."""
    from dsql_migrator.core.models import SourceInventory

    from tests.test_ui_data_migration import _inputs

    events: list = []
    tables = [g137._table(A), g137._table(B)]
    conversions = {
        A: TableConversion(table=A, target_ddl=g137._DDL, index_ddls=[_ddl("ix_a", "a")]),
        B: TableConversion(table=B, target_ddl=g137._DDL,
                           index_ddls=[_ddl("ix_old", "b"), _ddl("IX_New", "b", unique=True)]),
    }
    inputs = dataclasses.replace(
        _inputs(), inventory=SourceInventory(tables=tables), table_conversions=conversions)
    loads: list = []

    class _Importer(_RecordingImporter):
        def import_rows(self, rows, table, **kw):
            loads.append(table.name)
            return super().import_rows(rows, table, **kw)

    importer = _Importer(events)
    migrator = BatchedTableMigrator(
        inputs, exporter=_FakeExporter({A: [{"id": 1}], B: [{"id": 2}]}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
        # As the default recreator does: the table's conversion's index DDLs.
        table_recreator=lambda t: list(engine._effective_conversion(inputs, t).index_ddls),
        target_counter=lambda t: None, target_generated_reader=lambda t: set(),
        target_index_reader=reader,
    )
    prior = [ChunkState(chunk_id=A, status="DONE", rows_loaded=1),
             ChunkState(chunk_id=B, status="DONE", rows_loaded=1, indexes_pending=True)]
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    return migrator, tables, prior, events, loads


def test_an_index_only_retry_submits_what_is_missing_without_reloading(monkeypatch) -> None:
    reads: list = []
    migrator, tables, prior, events, loads = _index_only_rig(
        monkeypatch, reader=lambda t: reads.append(t.name) or {"ix_old", "orders_pkey"})
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[B]))
    assert job.status == "DONE", job._error
    assert loads == []                     # no row read or written
    assert reads == [B]                    # one catalog read, by relation
    assert events == [("pass-open",), ("index", [_ddl("IX_New", "b", unique=True)]),
                      ("pass-close",)]
    assert {c.chunk_id: (c.status, c.indexes_pending) for c in job.chunks} == {
        A: ("DONE", False), B: ("DONE", False)}


def test_an_index_only_retry_with_an_unreadable_catalog_submits_every_ddl(monkeypatch) -> None:
    migrator, tables, prior, events, _loads = _index_only_rig(monkeypatch, reader=lambda t: None)
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[B]))
    assert job.status == "DONE"
    assert _submitted(events) == [[_ddl("ix_old", "b"), _ddl("IX_New", "b", unique=True)]]


def test_a_reload_without_index_only_reloads_and_still_submits_the_carried_ones(
        monkeypatch) -> None:
    migrator, tables, prior, events, loads = _index_only_rig(
        monkeypatch, reader=lambda t: {"ix_old"})
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark()))
    assert job.status == "DONE", job._error
    assert loads == [B]                    # reloaded (an append) ...
    assert _submitted(events) == [[_ddl("IX_New", "b", unique=True)]]  # ... then indexed
    assert not any(c.indexes_pending for c in job.chunks)


def test_an_index_only_name_that_owes_nothing_is_reloaded(monkeypatch) -> None:
    migrator, tables, prior, events, loads = _index_only_rig(monkeypatch, reader=lambda t: set())
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[0]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[A]))  # A is DONE but owes no index
    assert loads == [A] and job.status == "DONE"


def test_an_index_only_table_that_dropped_rows_still_needs_its_gap_accepted(
        monkeypatch) -> None:
    from datetime import datetime, timezone

    from dsql_migrator.core.models import DataErrorRecord

    migrator, tables, prior, events, _loads = _index_only_rig(monkeypatch, reader=lambda t: set())
    prior[1] = prior[1].model_copy(update={"rows_quarantined": 3})
    # The rows the earlier load dropped stay recorded under that load's job id: the retry
    # does not reload the table, so it keeps that owner and counts them from there.
    log = ErrorLogStore()
    for pk in (1, 2, 3):
        log.record("job-0", DataErrorRecord(
            table=B, chunk_id=B, message=f"quarantined row pk[{pk}]: too large",
            occurred_at=datetime.now(timezone.utc)))

    def _retry(**kw):
        return _run(lambda h: run_full_load_retry(
            h, prior, [tables[1]], migrator=migrator, error_log=log,
            watermark=_watermark(), index_only=[B], prior_job_id="job-0", **kw))

    job = _retry()
    assert job.status == "FAILED" and "3 row(s) QUARANTINED" in job._error, job._error
    assert not any(c.indexes_pending for c in job.chunks)  # its indexes are in
    # An acceptance covers that gap only up to the rows it was given.
    assert _retry(accept_quarantined_rows=True, accepted_quarantine_rows=2).status == "FAILED"
    accepted = _retry(accept_quarantined_rows=True, accepted_quarantine_rows=3)
    assert accepted.status == "DONE", accepted._error


def test_an_unreachable_target_then_the_retry_completes_the_run(monkeypatch) -> None:
    migrator, tables, _prior, events, loads = _index_only_rig(
        monkeypatch, reader=lambda t: {"ix_old"})
    migrator.bind_run_inputs(dataclasses.replace(
        migrator._inputs, replace_tables=frozenset({A, B})))
    importer = migrator._importer_factory(None)
    fail = {"on": 2, "calls": 0}

    @contextlib.contextmanager
    def _pass():
        def create(ddls):
            fail["calls"] += 1
            events.append(("index", list(ddls)))
            if fail["calls"] == fail["on"]:
                raise IndexTargetUnreachable(0, [])
            return len(ddls), []
        yield create

    importer.index_pass = _pass
    first = _run(lambda h: run_full_load(h, tables, migrator=migrator, error_log=ErrorLogStore()))
    # a's indexes went in; Aurora DSQL did not answer b's: no data is missing, the run is not
    # complete, its message fits the persisted cap.
    assert first.status == "FAILED"
    message = index_pending_load_message(pending=1)
    assert first._error == f"FullLoadIncompleteError: {message}"  # whole: within the cap
    assert len(message) <= _PERSISTED_ERROR_CAP
    assert {c.chunk_id: (c.status, c.indexes_pending) for c in first.chunks} == {
        A: ("DONE", False), B: ("DONE", True)}
    assert loads == [A, B]
    # Retry unfinished tables: b only gets the indexes not on it yet; nothing is reloaded.
    loads.clear()
    events.clear()
    fail["on"] = None
    retried = _run(lambda h: run_full_load_retry(
        h, first.chunks, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=first.watermark, index_only=[B]))
    assert retried.status == "DONE", retried._error
    assert loads == [] and _submitted(events) == [[_ddl("IX_New", "b", unique=True)]]
    assert not any(c.indexes_pending for c in retried.chunks)


def test_a_retry_after_a_set_change_leaves_a_deselected_tables_indexes_alone(
        monkeypatch) -> None:
    # G12: a table no longer selected is left as is on Aurora DSQL -- no DDL on it -- and the
    # indexes it owes do not make the retry incomplete; its chunk keeps the flag for the
    # record. With the set unchanged (run_table_names None) the retry completes every
    # flagged chunk of the record.
    migrator, tables, prior, events, loads = _index_only_rig(monkeypatch, reader=lambda t: set())
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[0]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), run_table_names=[A]))
    assert job.status == "DONE", job._error
    assert loads == [A] and _submitted(events) == []
    assert {c.chunk_id: (c.status, c.indexes_pending) for c in job.chunks} == {
        A: ("DONE", False), B: ("DONE", True)}
    events.clear()
    loads.clear()
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[0]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark()))
    assert job.status == "DONE", job._error
    assert loads == [A]
    assert _submitted(events) == [[_ddl("ix_old", "b"), _ddl("IX_New", "b", unique=True)]]
    assert not any(c.indexes_pending for c in job.chunks)


def test_an_index_only_retrys_failures_are_recorded_under_the_tables_owner(
        monkeypatch) -> None:
    # A retry keeps the earlier owner of the error records of a table it does not reload
    # (table_error_job_ids), and the screen reads that table's records from its owner only:
    # recorded under the retry's own id, a UNIQUE index that failed in an index-only retry
    # never showed as a missing constraint.
    from dsql_migrator.ui.data_migration._cdc_status import full_load_error_records_for

    migrator, tables, prior, _events, _loads = _index_only_rig(
        monkeypatch, reader=lambda t: set())

    @contextlib.contextmanager
    def _pass():
        yield lambda ddls: (0, [
            f"UNIQUE {_stored_index_name(d)}: 54000 too many indexes" for d in ddls])

    migrator._importer_factory(None).index_pass = _pass
    log = ErrorLogStore()
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=log, watermark=_watermark(),
        index_only=[B], prior_job_id="job-0"))
    assert job.status == "DONE", job._error  # a failed index never fails the table
    assert job.table_error_job_ids[B] == "job-0"
    shown = [r.message for r in full_load_error_records_for(log, job) if r.table == B]
    assert [m.split(":")[1].strip() for m in shown] == ["UNIQUE ix_old", "UNIQUE IX_New"]


def test_an_index_only_table_is_never_in_the_retrys_replace_set(monkeypatch) -> None:
    # A replace table's run-level pre-drops DROP the foreign keys that reference it and the
    # views over it; an index-only table is never recreated, so it must not be one.
    migrator, tables, prior, _events, loads = _index_only_rig(
        monkeypatch, reader=lambda t: set())
    migrator.bind_run_inputs(dataclasses.replace(
        migrator._inputs, replace_tables=frozenset({A, B})))
    seen: list = []
    migrator.predrop_blocking_foreign_keys = lambda: seen.append(
        set(migrator._inputs.replace_tables))
    job = _run(lambda h: run_full_load_retry(
        h, prior, tables, migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[B]))
    assert job.status == "DONE", job._error
    assert seen == [{A}] and loads == [A]


def test_a_fresh_append_run_carries_the_pending_indexes(monkeypatch) -> None:
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    events: list = []
    conv = TableConversion(table="orders", target_ddl='CREATE TABLE "orders" ("id" INT, '
                           'PRIMARY KEY ("id"))', index_ddls=[_ddl("ix_o", "orders")])
    migrator, tables = _thread_migrator(events, conversions={"orders": conv},
                                        index_reader=lambda t: set())
    job = _run(lambda h: run_full_load(h, tables, migrator=migrator, error_log=ErrorLogStore(),
                                       carried_index_pending={"orders", "not.in.run"}))
    assert job.status == "DONE", job._error
    assert events == [("rows", "orders", None), ("rows", "customers", None),
                      ("pass-open",), ("index", [_ddl("ix_o", "orders")]), ("pass-close",)]
    assert [c.chunk_id for c in job.chunks] == ["orders", "customers"]
    assert not any(c.indexes_pending for c in job.chunks)


# ---------------------------------------------------------------------------
# 4. The persisted flag
# ---------------------------------------------------------------------------


def test_the_flag_is_left_out_of_the_payload_while_false() -> None:
    off = ChunkState(chunk_id="t", status="DONE")
    assert "indexes_pending" not in off.model_dump_json()
    on = ChunkState(chunk_id="t", status="DONE", indexes_pending=True)
    assert ChunkState.model_validate_json(on.model_dump_json()).indexes_pending is True
    old = '{"chunk_id": "t", "status": "DONE", "rows_loaded": 4}'
    assert ChunkState.model_validate_json(old).indexes_pending is False
    job = MigrationJob(job_id="j", chunks=[on])
    assert MigrationJob.model_validate_json(job.model_dump_json()).chunks[0].indexes_pending


def test_retry_seeding_keeps_the_flag_on_reset_and_copied_chunks() -> None:
    prior = [ChunkState(chunk_id="a", status="DONE", indexes_pending=True),
             ChunkState(chunk_id="b", status="FAILED", indexes_pending=True),
             ChunkState(chunk_id="c", status="DONE")]
    job = MigrationJob(job_id="r")
    _seed_retry_chunks(job, prior, {"b"})
    flags = {c.chunk_id: (c.status, c.indexes_pending) for c in job.chunks}
    assert flags == {"a": ("DONE", True), "b": ("PENDING", True), "c": ("DONE", False)}


def test_complete_chunk_sets_the_flag_and_never_clears_it() -> None:
    job = MigrationJob(job_id="j", chunks=[ChunkState(chunk_id="a", indexes_pending=True),
                                           ChunkState(chunk_id="b")])
    engine._complete_chunk(job, "a", 1)                       # an append keeps it
    engine._complete_chunk(job, "b", 1, indexes_pending=True)
    assert [c.indexes_pending for c in job.chunks] == [True, True]
    engine._complete_chunk(job, "b", 1, indexes_pending=False)  # never cleared here
    assert job.chunks[1].indexes_pending is True
    engine._clear_indexes_pending(job, "b")
    assert job.chunks[1].indexes_pending is False


def test_the_caption_says_the_run_submits_its_indexes_once_every_table_settled() -> None:
    from tests.test_ui_data_migration import _full_load_job

    job = _full_load_job([{"chunk_id": "a", "status": "DONE", "indexes_pending": True},
                          {"chunk_id": "b", "status": "DONE"}], counts={"a": 1, "b": 1})
    assert full_load_progress_caption(job) == (
        "All 2 tables loaded — submitting their secondary indexes and finishing up…")
    job.chunks[0].indexes_pending = False
    assert full_load_progress_caption(job) == "All 2 tables loaded — finishing up…"
    job.chunks[1].status = "FAILED"
    assert full_load_progress_caption(job) == "All 2 tables settled (1 loaded) — finishing up…"
    job.chunks[1].status = "IN_PROGRESS"
    assert "Exporting and loading b" in full_load_progress_caption(job)


def test_the_index_name_is_read_as_the_catalog_stores_it() -> None:
    assert _stored_index_name('CREATE INDEX ASYNC "Ix_A" ON "s"."t" ("c")') == "Ix_A"
    assert _stored_index_name("create unique index async Ix_A on t (c)") == "ix_a"
    assert _stored_index_name('CREATE INDEX ASYNC "a""b" ON t (c)') == 'a"b'
    assert _stored_index_name(f'CREATE INDEX ASYNC "{"x" * 70}" ON t (c)') == "x" * 63
    assert _stored_index_name("CREATE INDEX ASYNC ON t (c)") is None   # unnamed: submitted
    assert _stored_index_name("not an index") is None


# ---------------------------------------------------------------------------
# 5. The importer's index pass and the catalog read
# ---------------------------------------------------------------------------


class _IdxConnection:
    def __init__(self, log, faults):
        self.log, self.faults = log, faults

    def cursor(self):
        connection = self

        class _Cursor:
            def execute(self, ddl, params=None):
                connection.log.append((id(connection), ddl))
                errors = connection.faults.get(ddl) or []
                if errors:
                    raise errors.pop(0)

            def close(self):
                pass

        return _Cursor()

    def close(self):
        pass


def _idx_importer(faults=None, attempts=3):
    log: list = []
    made: list = []

    def factory():
        made.append(_IdxConnection(log, faults if faults is not None else {}))
        return made[-1]

    importer = BatchedImporter(
        BatchedImportOptions(parallelism=8), connection_factory=factory,
        occ_max_attempts=attempts, occ_base_delay=0.0, sleep=lambda _s: None,
        jitter=lambda: 0.0,
    )
    return importer, log, made


def test_the_index_pass_runs_every_table_on_one_connection() -> None:
    importer, log, made = _idx_importer()
    with importer.index_pass() as create:
        assert create(["d1", "d2"]) == (2, [])
        assert create([]) == (0, [])
        assert create(["d3"]) == (1, [])
    assert [ddl for _c, ddl in log] == ["d1", "d2", "d3"] and len(made) == 1


@pytest.mark.parametrize("unanswered", [
    _Error("server closed the connection unexpectedly", "08006"),
    _Error("server unavailable", "XX000"),
    _Error("schema has been updated by another transaction (OC001)", "40001"),
])
def test_a_target_that_does_not_answer_raises_with_what_came_before(unanswered) -> None:
    importer, log, _made = _idx_importer({
        "d1": [_Error("relation already exists", "42P07")], "d2": [unanswered] * 3})
    with importer.index_pass() as create:
        with pytest.raises(IndexTargetUnreachable) as raised:
            create(["d0", "d1", "d2", "d3"])
    assert raised.value.created == 1 and raised.value.__cause__ is unanswered
    (failure,) = raised.value.failures
    assert failure.startswith("d1: ") and "relation already exists" in failure
    assert "d3" not in [ddl for _c, ddl in log]
    assert importer.load_stats.snapshot()["index_s"] >= 0.0
    # create_indexes keeps recording it as a missing index (its own callers).
    importer, _log, _made = _idx_importer({"d2": [unanswered] * 3})
    created, failures = importer.create_indexes(["d2", "d3"])
    assert created == 1 and len(failures) == 1 and failures[0].startswith("d2")


def test_a_pass_that_cannot_connect_leaves_the_indexes_pending_not_failed() -> None:
    # The pass opens ONE fresh connection at the end of the run (the inline pass reused the
    # load's open ones): a connect that fails for a reason no retry clears -- the AWS
    # credentials that mint its IAM token expired during a long run -- ran no CREATE, so it
    # is no index's failure. Recorded per DDL, every flag was cleared with "add the index
    # later" as the remedy; the pass raises instead and Retry unfinished tables submits them.
    def _no_credentials():
        raise RuntimeError("Unable to locate credentials")

    importer = BatchedImporter(
        BatchedImportOptions(parallelism=8), connection_factory=_no_credentials,
        occ_max_attempts=3, occ_base_delay=0.0, sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    with importer.index_pass() as create:
        with pytest.raises(IndexTargetUnreachable) as raised:
            create(["d1", "d2"])
    assert raised.value.created == 0 and raised.value.failures == []
    # create_indexes keeps recording it per DDL (its own callers, unchanged).
    created, failures = importer.create_indexes(["d1", "d2"])
    assert created == 0 and [f.split(":")[0] for f in failures] == ["d1", "d2"]
    # A statement the server rejected is that index's failure, as before: not unreachable.
    importer, log, _made = _idx_importer({"d1": [_Error("relation already exists", "42P07")]})
    with importer.index_pass() as create:
        created, failures = create(["d1", "d2"])
    assert created == 1 and len(failures) == 1 and failures[0].startswith("d1: ")


def test_target_index_names_reads_the_relations_indexes() -> None:
    from dsql_migrator.core.target_introspector import target_index_names

    cursor = g137._Cursor(rows=[("app", "users_pkey"), ("app", "Uq_Email")])
    assert target_index_names("app.users", connection_factory=lambda: g137._Conn(cursor)) == {
        "users_pkey", "Uq_Email"}
    statement, params = cursor.executed[0]
    assert "pg_index" in statement and params == {"schema": "app", "table": "users"}
    # A bare name across user schemas: found in two -> unknown, never a guess.
    two = g137._Cursor(rows=[("a", "i1"), ("b", "i2")])
    assert target_index_names("users", connection_factory=lambda: g137._Conn(two)) is None
    failing = g137._Cursor(error=RuntimeError("boom"))
    assert target_index_names("app.users", connection_factory=lambda: g137._Conn(failing)) is None
    assert target_index_names("app.users", connection_factory=lambda: g137._Conn(
        g137._Cursor())) == set()


def test_target_index_names_reads_again_on_the_same_connection_after_a_conflict() -> None:
    from dsql_migrator.core.target_introspector import target_index_names

    class _Flaky(g137._Cursor):
        def execute(self, statement, params=None):
            self.executed.append((statement, params))
            if len(self.executed) == 1:
                raise _Error("schema has been updated by another transaction (OC001)", "40001")

    cursor = _Flaky(rows=[("app", "ix")])
    connections: list = []

    def factory():
        connections.append(g137._Conn(cursor))
        return connections[-1]

    assert target_index_names("app.t", connection_factory=factory, sleep=lambda _s: None) == {
        "ix"}
    assert len(connections) == 1 and len(cursor.executed) == 2


# ---------------------------------------------------------------------------
# 6. The step: its verdict, the recovery row and the notice follow the flag
# ---------------------------------------------------------------------------


def _ui_job(status, chunks):
    from tests.test_g12_table_set_after_load import _job

    return _job(status, chunks, counts={c.chunk_id: 1 for c in chunks})


def _chunk(name, status="DONE", **kw):
    from tests.test_g12_table_set_after_load import _c

    return _c(name, status, **kw)


def test_index_pending_tables_are_the_done_chunks_still_owing_their_indexes() -> None:
    from dsql_migrator.ui.data_migration import index_pending_tables

    job = _ui_job("FAILED", [_chunk(A, indexes_pending=True), _chunk(B),
                             _chunk(C, "FAILED", indexes_pending=True)])
    assert index_pending_tables(job) == [A]  # C reloads: it is unsettled, not index-only
    assert index_pending_tables(None) == []


def test_the_step_verdict_counts_them_as_unfinished_work() -> None:
    from dsql_migrator.ui.data_migration import untick_can_settle
    from dsql_migrator.ui.data_migration._full_load_ui import (
        _incomplete_is_quarantine_only,
        full_load_step_for_scope,
    )
    from dsql_migrator.ui.data_migration._models import scope_full_load_job

    log = ErrorLogStore()
    job = _ui_job("FAILED", [_chunk(A, indexes_pending=True), _chunk(B, rows_quarantined=2)])
    assert untick_can_settle(job, [A]) is True and untick_can_settle(job, [B]) is True
    assert untick_can_settle(_ui_job("FAILED", [_chunk(A)]), None) is False
    # A quarantine-only run with an index still owed: not acceptable as it is.
    assert _incomplete_is_quarantine_only(job, log) is False
    assert _incomplete_is_quarantine_only(
        _ui_job("FAILED", [_chunk(A), _chunk(B, rows_quarantined=2)]), log) is True
    # The record itself: never DONE with a pending table, even accepted.
    assert full_load_step_for_scope(job, job, accepted=True, error_log=log) is StepStatus.FAILED
    # A set that still selects it: FAILED; one that no longer does: settled as unfinished work
    # outside the selection, like an unsettled table.
    pending_in_view = scope_full_load_job(job, [A])
    assert full_load_step_for_scope(job, pending_in_view, accepted=True,
                                    error_log=log) is StepStatus.FAILED
    record = _ui_job("FAILED", [_chunk(A, indexes_pending=True), _chunk(B)])
    view = scope_full_load_job(record, [B])
    assert [c.chunk_id for c in view.chunks] == [B]
    assert full_load_step_for_scope(record, view, accepted=False,
                                    error_log=log) is StepStatus.DONE


def test_the_progress_section_shows_the_notice_once_the_run_ended(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _full_load_ui as fl
    from dsql_migrator.ui.data_migration._models import build_full_load_table_rows

    notices: list = []
    monkeypatch.setattr(fl, "render_notice", lambda ui, **kw: notices.append(kw))

    class _Ui(g137._RecordingUi):
        def linear_progress(self, *a, **k):
            return self._El(self)

    names = [f"app.t{i:02d}" for i in range(10)]
    job = _ui_job("FAILED", [_chunk(n, indexes_pending=True) for n in names] + [_chunk(A)])
    fl._render_full_load_progress(_Ui(), job, build_full_load_table_rows(job))
    (notice,) = [n for n in notices if n["header"].startswith("Secondary indexes")]
    assert notice["tone"] == "warning"
    assert notice["header"] == "Secondary indexes not created yet (10 table(s))"
    assert notice["body"].startswith(fl.INDEX_PENDING_BODY)
    assert notice["body"].endswith(
        "Tables: app.t00, app.t01, app.t02, app.t03, app.t04, app.t05, app.t06, app.t07 and "
        "2 more.")
    # While the run runs, the flags are the pass's to clear: no notice.
    notices.clear()
    job.status = "RUNNING"
    fl._render_full_load_progress(_Ui(), job, build_full_load_table_rows(job))
    assert not [n for n in notices if n["header"].startswith("Secondary indexes")]


def _render_recovery(monkeypatch, job, retry_tables, *, error=None):
    """The real Full Load step over ``job`` (Full load only), its progress section stubbed;
    returns the double, the tooltips and the notices ``(tone, header)``."""
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_g12_table_set_after_load import _Jobs, _VerifiedSession, _flo
    from tests.test_ui_data_migration import _ConfirmDialogUi

    import nicegui.context as ctxobj
    import nicegui.run as nrun

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    # The confirm dialog opens in the top-level client, off any page here.
    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()),
                        raising=False)
    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(fl, "_render_full_load_progress", lambda *a, **k: None)
    monkeypatch.setattr(fl, "full_load_error_records_for", lambda _log, _job: [])
    notices: list = []
    real_notice = fl.render_notice
    monkeypatch.setattr(fl, "render_notice", lambda ui, **kw: (
        notices.append((kw.get("tone"), kw.get("header"))), real_notice(ui, **kw))[1])
    tips: list = []
    monkeypatch.setattr(_ConfirmDialogUi._El, "tooltip",
                        lambda self, text="", *a, **k: tips.append(str(text)) or self,
                        raising=False)
    names = [c.chunk_id for c in job.chunks]
    ui = _ConfirmDialogUi()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, _flo(names), _Jobs({"job-1": job}, {"job-1": error} if error else {}),
        _VerifiedSession(), job=job,
        status=StepStatus.FAILED, selected_names=names, guard_reason=None,
        start_full_load=noop, retry_failed_load=noop, reload_table=noop,
        accept_quarantine_and_continue=noop, stop_full_load=noop, refresh=noop,
        retry_tables=retry_tables, schema_recreate_candidates=lambda: [],
    )
    ui.notices = notices
    return ui, tips


def test_retry_is_offered_when_only_pending_indexes_remain(monkeypatch) -> None:
    calls: list = []
    job = _ui_job("FAILED", [_chunk(A), _chunk(B, indexes_pending=True)])
    error = f"FullLoadIncompleteError: {index_pending_load_message(pending=1)}"
    ui, tips = _render_recovery(
        monkeypatch, job, lambda names, **kw: calls.append((list(names), kw)), error=error)
    # No red "Load failed" over a run whose data loaded: the notice says what is owed.
    assert not [n for n in ui.notices if n[1] == "Load failed"]
    assert "Retry unfinished tables (1)" in ui.buttons
    assert "Re-run Full Load" not in ui.buttons  # the recovery row, not a lone re-run
    assert any(t.startswith("Recommended: submit the secondary indexes of the 1 loaded")
               for t in tips), tips
    asyncio.run(dict(ui.handlers)["Retry unfinished tables (1)"]())
    assert "Data loaded; secondary indexes not created yet." in ui.texts
    # The summary says what the confirm does: no row is read or written for it.
    assert any("1 loaded table only gets its secondary indexes: no row is read or written "
               "for it." in t for t in ui.texts), ui.texts
    confirm = [h for label, h in ui.handlers if label == "Confirm and start"]
    confirm[-1]()
    assert calls == [([B], {"index_only": [B]})]


def test_a_mixed_retry_reloads_the_failed_table_and_only_indexes_the_loaded_one(
        monkeypatch) -> None:
    calls: list = []
    job = _ui_job("FAILED", [_chunk(A, "FAILED"), _chunk(B, indexes_pending=True)])
    ui, _tips = _render_recovery(
        monkeypatch, job, lambda names, **kw: calls.append((list(names), kw)),
        error="FullLoadIncompleteError: Full Load incomplete: 1 of 2 table(s) did not fully load.")
    assert ("error", "Load failed") in ui.notices  # a real failure keeps its red box
    assert "Retry unfinished tables (2)" in ui.buttons
    asyncio.run(dict(ui.handlers)["Retry unfinished tables (2)"]())
    [h for label, h in ui.handlers if label == "Confirm and start"][-1]()
    assert calls == [([A, B], {"index_only": [B]})]


def test_the_retry_route_passes_index_only_and_never_drops_those_tables(monkeypatch) -> None:
    from tests.test_g12_table_set_after_load import _c, _job, _retry_route

    job = _job("FAILED", [_c("app.a", "FAILED"), _c("app.b", indexes_pending=True)],
               counts={"app.a": 1, "app.b": 1})
    s, retry, calls = _retry_route(monkeypatch, job=job, selected=["app.a", "app.b"])
    # The dialog's Drop & reload choice covers every table holding rows ...
    s.state.set_tables_with_data(frozenset({"app.a", "app.b"}))
    s.state.set_reload_mode("drop")
    retry(["app.a", "app.b"], index_only=["app.b", "app.c"])
    ((_prior, tables, kw),) = calls
    assert tables == ["app.a", "app.b"]
    assert kw["index_only"] == ["app.b"]                       # only a retried name
    assert kw["inputs"].replace_tables == frozenset({"app.a"})  # ... never an index-only one
    # A per-table Reload passes none: a reload reloads.
    calls.clear()
    s.state.job_id = "job-1"  # the record again (the retry above submitted job-2)
    retry(["app.b"])
    ((_prior, tables, kw),) = calls
    assert tables == ["app.b"] and kw["index_only"] == []


def test_a_fresh_run_carries_the_previous_runs_pending_indexes(monkeypatch) -> None:
    from dsql_migrator.ui import data_migration as dm

    from tests.test_g12_table_set_after_load import _c, _job, _screen

    job = _job("FAILED", [_c("app.a", indexes_pending=True), _c("app.b"),
                          _c("app.c", "FAILED", indexes_pending=True)],
               counts={"app.a": 1, "app.b": 1, "app.c": 1})
    s = _screen(monkeypatch, job=job, selected=["app.a", "app.b"])
    captured: dict = {}
    monkeypatch.setattr(dm, "_render_full_load_step", lambda *_a, **kw: captured.update(kw))
    runs: list = []
    monkeypatch.setattr(dm, "run_full_load", lambda _h, tables, **kw: runs.append(kw))
    s.content(lambda: None)
    captured["start_full_load"]()
    (work,) = s.jobs.submitted
    work(SimpleNamespace(job_id="job-2"))
    # app.c owes them too (its reload failed with the flag) but is not in this run.
    assert runs[0]["carried_index_pending"] == {"app.a"}


def test_the_completeness_verdict_does_not_call_the_load_complete_while_indexes_are_owed(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _full_load_ui as fl
    from dsql_migrator.ui.data_migration._models import (
        build_full_load_table_rows,
        full_load_completeness,
    )

    notices: list = []
    monkeypatch.setattr(fl, "_render_notice", lambda ui, **kw: notices.append(kw))
    job = _ui_job("FAILED", [_chunk(A, rows_loaded=1, indexes_pending=True),
                             _chunk(B, rows_loaded=1)])
    completeness = full_load_completeness(build_full_load_table_rows(job))
    fl._render_completeness_banner(object(), completeness, indexes_pending=1)
    ((notice),) = notices
    assert notice["tone"] == "info"
    assert notice["header"] == "Every row loaded — secondary indexes not created yet"
    assert notice["body"].endswith("the secondary indexes of 1 of them are not created yet.")
    notices.clear()
    fl._render_completeness_banner(object(), completeness)
    assert notices[0]["tone"] == "success" and notices[0]["header"] == "Full Load complete"


def test_a_table_cdc_does_not_capture_yet_is_never_index_only(monkeypatch) -> None:
    # G17: a Full load + CDC table selected after the load reloads under its guards; owing
    # indexes does not let it skip that load (its guards pass here, so it is reloaded, then
    # its owed indexes submitted, and CDC's coverage extended).
    migrator, tables, prior, events, loads = _index_only_rig(monkeypatch, reader=lambda t: set())
    migrator.guard_cdc_additions = lambda names, **_kw: {n: None for n in names}
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[B], cdc_additions=[B],
        cdc_coverage_open=lambda: True))
    assert job.status == "DONE", job._error
    assert loads == [B]
    assert _submitted(events) == [[_ddl("ix_old", "b"), _ddl("IX_New", "b", unique=True)]]
    assert B in job.watermark.cdc_added_tables
