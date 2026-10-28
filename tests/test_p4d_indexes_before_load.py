# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P4d: a Drop & reload table's secondary indexes go on the EMPTY recreated table before any row.

Live (Seoul, 2026-10-26, perf_ecom 8.5M rows / 25 secondary indexes): built after every
table's data (P4a), the builds ran up to 680 s after the load; on the empty tables they took
13-41 s and the load into the indexed tables had no OC001 retry. Pinned here, on doubles only
(no MySQL, no Aurora DSQL, no AWS):

* the process path submits every Drop & reload table's index DDLs after the recreate pre-pass
  and before any unit (whole and sharded tables), records each build with its job id, and
  waits -- bounded, honouring Stop, a heartbeat per read, only those builds by their job ids
  -- before its first unit; a build that failed is an index not created, recorded once its
  table loaded; one still running at the bound stays "building" and the load starts anyway;
* the run's index pass after every table's data covers only what a loaded table still owes:
  a table its worker recreated, one the submissions did not reach, and the rest of one Aurora
  DSQL stopped answering for (checked against the table first);
* the thread path (one table at a time) indexes each table right after its own recreate -- a
  re-read's recreate too -- and keeps the pass after the load with several threads;
* a 23505 on a secondary UNIQUE index is a data error -- the row is quarantined, named by its
  index, value-free -- in NONE and SKIP_EXISTING, while a 23505 on the primary key keeps its
  recovery, and an unknown index keeps the old retry;
* the seams, the caption, the P4b record and notice, the harness flag and report, the W4 text.
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import os
import threading
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import psycopg
import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
import dsql_migrator.ui.data_migration._index_builds as ib
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    IndexTargetUnreachable,
    OnConflictMode,
    violated_constraint_name,
)
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.load_stats import LoadStatsBoard
from dsql_migrator.core.models import (
    ChunkState,
    ColumnDef,
    IndexBuild,
    IndexBuilds,
    MigrationJob,
    TableDef,
)
from dsql_migrator.core.target_introspector import (
    INDEX_BUILD_UNLISTED_DETAILS,
    IndexBuildRead,
    read_primary_key_index_name,
)
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    _TableWorkerResult,
    full_load_progress_caption,
    run_full_load,
)

import tests.test_g137_mysql_reader_shards as g137
from tests.test_full_load_overlay import _InlineExecutor
from tests.test_measure_performance import _command_doubles, mp  # noqa: F401 - fixture
from tests.test_ui_data_migration import (
    _FakeExporter,
    _FakeImporter,
    _FakeWatermarkCapturer,
    _inputs as _ui_inputs,
    _lost_connection,
    _watermark,
)
from tests.test_ui_design import _RecordingUi

A, B, C = "app.a", "app.b", "app.c"
_SEAM = "DSQL_MIGRATOR_FULL_LOAD_INDEXES_AFTER_LOAD"
_WAIT_SEAM = "DSQL_MIGRATOR_FULL_LOAD_INDEX_WAIT_SECONDS"
_KEY_VALUE = "alice@example.com"


def _ddl(name: str, table: str, *, unique: bool = False) -> str:
    return (
        f'CREATE {"UNIQUE " if unique else ""}INDEX ASYNC "{name}" ON "app"."{table}" '
        '("email")'
    )


def _conv(table: str, *ddls: str) -> TableConversion:
    return g137._conv(table, indexes=list(ddls))


class _Err(Exception):
    """A psycopg-like error: a message and its SQLSTATE."""

    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.sqlstate = sqlstate


class _Clock:
    """The wait's clock: a sleep advances it."""

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _Builds:
    """Aurora DSQL's index builds as the migrator's ``_index_build_reader`` reads them,
    scripted: ``script[index]`` is its status at the 1st, 2nd, ... read (the last repeats;
    none: completed at once), ``details[index]`` a failed build's reason; ``unreadable``: every
    read answers nothing. Every read is an event."""

    def __init__(self, events: list) -> None:
        self.events = events
        self.script: dict = {}
        self.details: dict = {}
        self.unreadable = False
        self.on_read = None
        self.reads = 0

    def __call__(self, builds):
        self.reads += 1
        self.events.append(("read", sorted(b.index for b in builds)))
        if self.on_read is not None:
            self.on_read(self.reads)
        if self.unreadable:
            return {(b.table, b.index): None for b in builds}
        answer = {}
        for build in builds:
            steps = self.script.get(build.index, ["completed"])
            status = steps[min(self.reads, len(steps)) - 1]
            answer[(build.table, build.index)] = (
                # Its job id not listed and its index not valid (P4b's inference).
                IndexBuildRead("failed", INDEX_BUILD_UNLISTED_DETAILS, unlisted=True)
                if status == "unlisted"
                else IndexBuildRead(
                    status, self.details.get(build.index) if status == "failed" else None
                )
            )
        return answer


class _Pass:
    """The run's index submissions on Aurora DSQL, recorded: every pass and every table's
    create. The DDLs go in order, as the importer's: one in ``fail`` fails (its failure
    text), every other answers the build job id ``job-N`` in submission order;
    ``unreachable_at = (call, i)``: that create's ``i``-th DDL (from 0) is where Aurora DSQL
    stops answering (IndexTargetUnreachable with what went before it)."""

    def __init__(self, events: list) -> None:
        self.events = events
        self.jobs = 0
        self.calls = 0
        self.fail: dict = {}
        self.unreachable_at = None

    @contextlib.contextmanager
    def index_pass(self):
        self.events.append(("pass-open",))
        try:
            yield self._create
        finally:
            self.events.append(("pass-close",))

    def _create(self, ddls, on_job=None):
        self.calls += 1
        self.events.append(("index", list(ddls)))
        created, failures = 0, []
        for position, ddl in enumerate(ddls):
            if self.unreachable_at == (self.calls, position):
                error = IndexTargetUnreachable(created, failures)
                error.__cause__ = _Err("server closed the connection unexpectedly", "08006")
                raise error
            if ddl in self.fail:
                failures.append(self.fail[ddl])
                continue
            created += 1
            self.jobs += 1
            if on_job is not None:
                on_job(ddl, f"job-{self.jobs}")
        return created, failures


def _first(events, kind) -> int:
    return next(i for i, e in enumerate(events) if e[0] == kind)


def _last(events, kind) -> int:
    return max(i for i, e in enumerate(events) if e[0] == kind)


def _submitted(events) -> list:
    return [e[1] for e in events if e[0] == "index"]


def _pass_lines(activity) -> list:
    return [(status, detail) for action, status, detail, _t in activity if action == "index pass"]


@pytest.fixture(autouse=True)
def _p4d_order(monkeypatch):
    """The product's order (the seams unset) unless a test sets one."""
    monkeypatch.delenv(_SEAM, raising=False)
    monkeypatch.delenv(_WAIT_SEAM, raising=False)


# ---------------------------------------------------------------------------
# The rig: the process path with its workers inline, every event recorded
# ---------------------------------------------------------------------------


@pytest.fixture
def p4d(monkeypatch):
    """``_migrate_tables_in_parallel`` on the process path, workers inline, the wait on a fake
    clock. The whole-table worker answers as the real one: no DDL for a table the parent
    indexed (``pre_indexed``), its conversion's for one it pre-recreated and did not, and
    ``recreated_ddls`` when its own re-read recreated the table."""
    events: list = []
    workers: list = []
    whole: dict = {}
    shard_status: dict = {}
    activity: list = []
    importer = _Pass(events)
    builds = _Builds(events)
    clock = _Clock()
    real_wait = engine._wait_for_index_builds_before_load

    def _shard(args):
        events.append(("unit", args.table.name, args.shard_index))
        status = shard_status.get((args.table.name, args.shard_index), "DONE")
        return _TableWorkerResult(table_name=args.table.name, status=status, rows_loaded=10,
                                  shard_index=args.shard_index)

    def _whole(args):
        name = args.table.name
        events.append(("unit", name, None))
        workers.append(args)
        spec = whole.get(name, {})
        status = spec.get("status", "DONE")
        if "recreated_ddls" in spec:
            ddls = spec["recreated_ddls"]
        elif args.pre_recreated and not args.pre_indexed:
            ddls = spec.get("ddls", ())
        else:
            ddls = ()
        return _TableWorkerResult(
            table_name=name, status=status, rows_loaded=40,
            error_message=None if status == "DONE" else "boom",
            index_ddls=tuple(ddls) if status == "DONE" else (),
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
    monkeypatch.setattr(
        engine, "_wait_for_index_builds_before_load",
        lambda handle, migrator, outcome, **kw: real_wait(
            handle, migrator, outcome, sleep=clock.sleep, monotonic=clock.monotonic, **kw
        ),
    )
    monkeypatch.setattr(engine, "log_activity", lambda _cat, action, **kw: activity.append(
        (action, getattr(kw.get("status"), "value", None), kw.get("detail"), kw.get("target"))))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")
    _InlineExecutor.submitted = []

    def run(tables, *, replace=(), counts=None, ranges=None, recreate=None, handle=None,
            conversions=None, index_reader=None, inventory=None, load_stats=None):
        def _recreator(table):
            events.append(("recreate", table.name))
            return list((recreate or {}).get(table.name, ()))

        migrator = BatchedTableMigrator(
            g137._inputs(inventory or tables, replace=replace, conversions=conversions),
            exporter=g137._Planner(ranges or {}), watermark_capturer=object(),
            importer_factory=lambda _i: importer, table_recreator=_recreator,
            target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
            target_generated_reader=lambda t: set(),
            target_unique_keys_reader=lambda t: [list(t.primary_key)],
            target_index_reader=index_reader or (lambda t: set()),
            index_build_reader=builds,
        )
        handle = handle or g137._Handle([t.name for t in tables])
        log = ErrorLogStore()
        counts = engine._migrate_tables_in_parallel(
            handle, "j", tables, migrator, log, watermark=g137._wm(counts=counts or {}),
            fresh_watermark=True, load_stats=load_stats,
        )
        return SimpleNamespace(
            counts=counts, handle=handle, record=handle.job.index_builds,
            chunks={c.chunk_id: c for c in handle.job.chunks},
            errors=[r.message for r in log.records("j")],
        )

    run.events, run.workers, run.whole, run.shard_status = events, workers, whole, shard_status
    run.importer, run.builds, run.clock, run.activity = importer, builds, clock, activity
    return run


# ---------------------------------------------------------------------------
# 1. The process path: before any unit
# ---------------------------------------------------------------------------


def test_every_recreated_tables_indexes_go_on_after_the_recreates_and_before_any_unit(
        p4d) -> None:
    ix_a, ux_b = _ddl("ix_a", "a"), _ddl("ux_b", "b", unique=True)
    r = p4d([g137._table(A), g137._table(B), g137._table(C)], replace=[A, B],
            conversions={A: _conv(A, ix_a), B: _conv(B, ux_b), C: _conv(C, _ddl("ix_c", "c"))},
            counts={A: 10, B: 500, C: 20})
    events = p4d.events
    # Both recreated, their DDLs one at a time on one connection, one read of exactly their
    # builds -- and only then the units. C is appended to: Step 2 made its indexes.
    assert events[: _first(events, "unit")] == [
        ("recreate", A), ("recreate", B),
        ("pass-open",), ("index", [ix_a]), ("index", [ux_b]), ("pass-close",),
        ("read", ["ix_a", "ux_b"]),
    ]
    assert _submitted(events) == [[ix_a], [ux_b]]  # no pass after the load
    assert [(w.table.name, w.pre_recreated, w.pre_indexed) for w in p4d.workers] == [
        (B, True, True), (C, False, False), (A, True, True)]  # largest first (R2)
    assert all(c.status == "DONE" and not c.indexes_pending for c in r.chunks.values())
    assert ("complete", A, False) in events and r.counts == (0, 0, 0)
    record = r.record
    assert (record.submitted, record.completed, record.building, record.failures) == (
        2, 2, [], [])
    assert record.completed_by_table == {A: 1, B: 1}
    # The builds settled before the rows; "data loaded" is stamped once every table had.
    assert record.finished_at is not None and record.data_loaded_at is not None
    assert record.finished_at <= record.data_loaded_at
    assert _pass_lines(p4d.activity) == [
        ("info", "before any row loads: submitting 2 secondary index(es) of 2 recreated "
                 "table(s) on the empty table(s), one at a time"),
        ("info", "Aurora DSQL built the 2 secondary index(es) on the empty table(s) in 0 s; "
                 "the load starts"),
    ]


def test_a_sharded_table_gets_its_recreate_ddls_and_a_whole_one_its_conversions(p4d) -> None:
    ix_e, ix_a = _ddl("ix_events", "events"), _ddl("ix_a", "a")
    r = p4d([g137._table(g137.EVENTS), g137._table(A)], replace=[g137.EVENTS, A],
            ranges={g137.EVENTS: g137._FOUR}, recreate={g137.EVENTS: [ix_e], A: ["stale"]},
            conversions={g137.EVENTS: _conv(g137.EVENTS), A: _conv(A, ix_a)})
    events = p4d.events
    # The sharded table: the recreate's DDLs (P4a's pre-pass ones); the whole table: its
    # effective conversion's, which its worker derived before P4d.
    assert _submitted(events) == [[ix_e], [ix_a]]
    assert _last(events, "index") < _first(events, "unit")
    assert [e[2] for e in events if e[0] == "unit" and e[1] == g137.EVENTS] == [0, 1, 2, 3]
    assert all(c.status == "DONE" and not c.indexes_pending for c in r.chunks.values())


def test_a_table_with_no_secondary_index_gets_no_submission(p4d) -> None:
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A)})
    assert [e for e in p4d.events if e[0] in ("pass-open", "index", "read")] == []
    assert p4d.workers[0].pre_indexed is True  # nothing owed either
    assert r.record is None and _pass_lines(p4d.activity) == []


# ---------------------------------------------------------------------------
# 2. The wait
# ---------------------------------------------------------------------------


def test_the_wait_reads_only_those_builds_with_backoff_and_a_heartbeat_per_read(p4d) -> None:
    ix_a, ix_b = _ddl("ix_a", "a"), _ddl("ix_b", "a")
    p4d.builds.script = {"ix_a": ["building", "building", "completed"],
                         "ix_b": ["building", "completed"]}
    handle = g137._Handle([A])
    beats: list = []
    handle.heartbeat = lambda: beats.append(p4d.builds.reads)
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, ix_a, ix_b)},
            handle=handle)
    events = p4d.events
    # Right after the submissions, then 1 s later, then 2 s: ix_b settled at the 2nd read.
    assert [e for e in events if e[0] == "read"] == [
        ("read", ["ix_a", "ix_b"]), ("read", ["ix_a", "ix_b"]), ("read", ["ix_a"])]
    assert p4d.clock.sleeps == [1.0, 1.0, 1.0]
    assert _last(events, "read") < _first(events, "unit")
    assert [1, 2, 3] == [n for n in beats if n][:3]  # one heartbeat after each read
    assert r.record.completed == 2 and r.record.building == []
    assert _pass_lines(p4d.activity)[-1] == (
        "info", "Aurora DSQL built the 2 secondary index(es) on the empty table(s) in 3 s; "
                "the load starts")


def test_a_build_still_running_at_the_bound_stays_building_and_the_load_starts(p4d) -> None:
    ix_a = _ddl("ix_a", "a")
    p4d.builds.script = {"ix_a": ["building"]}
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, ix_a)})
    events = p4d.events
    assert p4d.clock.now == pytest.approx(600.0)  # the bound, never past it
    assert max(p4d.clock.sleeps) <= 1.0           # slept in slices: a Stop is seen at once
    assert _last(events, "read") < _first(events, "unit")  # the load started anyway
    assert r.chunks[A].status == "DONE" and not r.chunks[A].indexes_pending
    record = r.record
    assert [(b.index, b.job_id) for b in record.building] == [("ix_a", "job-1")]
    assert record.data_loaded_at is not None  # the watch reports it from there (P4b)
    assert ib.index_builds_pending(handle_job(r))
    assert _pass_lines(p4d.activity)[-1] == (
        "info", "1 of the 1 secondary index(es) still building after 600 s: the load starts "
                "anyway (Aurora DSQL keeps building them while the rows load, and the Data "
                "Migration step shows when they finish)")


def handle_job(r) -> MigrationJob:
    return r.handle.job.model_copy(update={"status": "DONE"})


def test_the_wait_bound_follows_its_seam(p4d, monkeypatch) -> None:
    monkeypatch.setenv(_WAIT_SEAM, "12")
    p4d.builds.script = {"ix_a": ["building"]}
    p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, _ddl("ix_a", "a"))})
    assert p4d.clock.now == pytest.approx(12.0)
    # 0: no wait, no read -- the load starts at once with the build left to the watch.
    p4d.events.clear()
    p4d.builds.reads = 0
    monkeypatch.setenv(_WAIT_SEAM, "0")
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, _ddl("ix_a", "a"))})
    assert p4d.builds.reads == 0 and [e for e in p4d.events if e[0] == "read"] == []
    assert len(r.record.building) == 1 and r.chunks[A].status == "DONE"


def test_a_stop_during_the_wait_ends_it_and_no_unit_starts(p4d) -> None:
    handle = g137._Handle([A, B])
    p4d.builds.script = {"ix_a": ["building"]}
    p4d.builds.on_read = lambda n: setattr(handle, "cancelled", True) if n == 2 else None
    r = p4d([g137._table(A), g137._table(B)], replace=[A],
            conversions={A: _conv(A, _ddl("ix_a", "a"))}, handle=handle)
    assert p4d.builds.reads == 2 and p4d.clock.now == pytest.approx(1.0)
    assert [e for e in p4d.events if e[0] == "unit"] == []
    assert {c.status for c in r.chunks.values()} == {"FAILED"}  # retryable, nothing loaded
    assert not any(c.indexes_pending for c in r.chunks.values())
    assert _pass_lines(p4d.activity)[-1] == (
        "info", "stopped by the operator after 1 s, with 1 of the 1 secondary index(es) on "
                "the empty table(s) still building")


def test_a_failed_build_is_an_index_not_created_once_its_table_has_loaded(p4d) -> None:
    ux_a, ix_a, ux_b = (_ddl("ux_a", "a", unique=True), _ddl("ix_a", "a"),
                        _ddl("ux_b", "b", unique=True))
    p4d.builds.script = {"ux_a": ["failed"], "ux_b": ["building", "failed"]}
    detail = f'could not create unique index "ux_a": Key (email)=({_KEY_VALUE}) is duplicated.'
    p4d.builds.details = {"ux_a": detail, "ux_b": detail.replace("ux_a", "ux_b")}
    p4d.whole[B] = {"status": "FAILED"}
    r = p4d([g137._table(A), g137._table(B)], replace=[A, B],
            conversions={A: _conv(A, ux_a, ix_a), B: _conv(B, ux_b)})
    # A loaded: its failed UNIQUE build is a missing constraint, reported as an index the run
    # could not create (value-free). B did not load: nothing (a retry recreates it).
    assert [e for e in r.errors if e.startswith("index not created")] == [
        "index not created: UNIQUE ux_a: Aurora DSQL's build of the index failed: could not "
        'create unique index "ux_a" — the table\'s DATA loaded completely; add the index '
        "later or reduce the table's index count (Aurora DSQL allows 24 per table, including "
        "the primary key)"]
    assert "boom" in r.errors  # B's own failure, as before
    assert _KEY_VALUE not in json.dumps(r.errors)
    assert r.chunks[A].status == "DONE" and not r.chunks[A].indexes_pending
    # The failed builds leave the record (they are the error log's now): no double report.
    record = r.record
    assert (record.submitted, record.completed, record.failed, record.failures) == (
        1, 1, 0, [])
    assert _pass_lines(p4d.activity)[-1] == (
        "warning", "Aurora DSQL built 1 of the 3 secondary index(es) on the empty table(s) "
                   "in 1 s, and 2 failed (each is listed with its table's index failures "
                   "once the table's data has loaded); the load starts")


def test_a_job_id_not_listed_yet_is_still_building_never_a_failure(p4d) -> None:
    # Aurora DSQL may not list a just-submitted job yet: within P4b's grace from the
    # submission (15 min, longer than the wait) it is a build still running.
    p4d.builds.script = {"ix_a": ["unlisted", "completed"]}
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, _ddl("ix_a", "a"))})
    assert p4d.builds.reads == 2 and r.errors == []
    assert (r.record.submitted, r.record.completed, r.record.failed) == (1, 1, 0)


def test_a_sharded_tables_failed_build_is_recorded_once_every_part_loaded(p4d) -> None:
    ux_e = _ddl("ux_events", "events", unique=True)
    p4d.builds.script = {"ux_events": ["failed"]}
    p4d.builds.details = {"ux_events": 'could not create unique index "ux_events"'}
    r = p4d([g137._table(g137.EVENTS)], replace=[g137.EVENTS],
            ranges={g137.EVENTS: g137._FOUR}, recreate={g137.EVENTS: [ux_e]})
    assert r.errors == [
        "index not created: UNIQUE ux_events: Aurora DSQL's build of the index failed: could "
        'not create unique index "ux_events" — the table\'s DATA loaded completely; add the '
        "index later or reduce the table's index count (Aurora DSQL allows 24 per table, "
        "including the primary key)"]
    # A part that failed: the table did not load, so nothing is reported for it.
    p4d.shard_status[(g137.EVENTS, 1)] = "FAILED"
    r = p4d([g137._table(g137.EVENTS)], replace=[g137.EVENTS],
            ranges={g137.EVENTS: g137._FOUR}, recreate={g137.EVENTS: [ux_e]})
    assert not [e for e in r.errors if e.startswith("index not created")]


def test_an_unreadable_target_lets_the_load_start_after_three_reads(p4d) -> None:
    p4d.builds.unreadable = True
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, _ddl("ix_a", "a"))})
    assert p4d.builds.reads == 3 and p4d.clock.sleeps == [1.0, 1.0, 1.0]
    assert r.chunks[A].status == "DONE" and len(r.record.building) == 1
    assert _pass_lines(p4d.activity)[-1] == (
        "info", "the builds of 1 of the 1 secondary index(es) could not be read from Aurora "
                "DSQL after 3 s: the load starts, and the Data Migration step reads them "
                "afterwards")


@pytest.mark.parametrize(("raw", "expected"), [
    (None, 600.0), ("0", 0.0), ("12.5", 12.5), (" 30 ", 30.0), ("-1", 600.0), ("x", 600.0),
    ("inf", 600.0), ("nan", 600.0)])
def test_the_wait_seam_parses(monkeypatch, raw, expected) -> None:
    if raw is not None:
        monkeypatch.setenv(_WAIT_SEAM, raw)
    assert engine._index_wait_before_load_seconds() == expected


@pytest.mark.parametrize(("raw", "expected"), [
    (None, False), ("1", True), ("true", True), (" YES ", True), ("on", True), ("0", False),
    ("", False), ("off", False)])
def test_the_order_seam_parses(monkeypatch, raw, expected) -> None:
    if raw is not None:
        monkeypatch.setenv(_SEAM, raw)
    assert engine._indexes_after_load() is expected


def test_the_order_seam_restores_p4as_pass_after_every_table(p4d, monkeypatch) -> None:
    monkeypatch.setenv(_SEAM, "1")
    ix_a = _ddl("ix_a", "a")
    p4d.whole[A] = {"ddls": [ix_a]}
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, ix_a)})
    events = p4d.events
    assert _last(events, "unit") < _first(events, "pass-open")
    assert _submitted(events) == [[ix_a]] and p4d.builds.reads == 0
    assert p4d.workers[0].pre_indexed is False
    assert r.record.data_loaded_at is not None and not r.chunks[A].indexes_pending


def test_the_wait_never_raises_into_the_run(p4d) -> None:
    def _broken(builds):
        raise RuntimeError("the reader itself broke")

    p4d.builds.__class__ = type("_B", (_Builds,), {"__call__": lambda self, b: _broken(b)})
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, _ddl("ix_a", "a"))})
    assert r.chunks[A].status == "DONE"  # three unanswered reads, then the load
    assert _pass_lines(p4d.activity)[-1][1].startswith(
        "the builds of 1 of the 1 secondary index(es) could not be read from Aurora DSQL")


# ---------------------------------------------------------------------------
# 3. What the pass after the load still covers
# ---------------------------------------------------------------------------


def test_a_table_its_worker_recreated_gets_its_indexes_after_every_table(p4d) -> None:
    ix_a = _ddl("ix_a", "a")
    p4d.whole[A] = {"recreated_ddls": [ix_a]}  # its re-read recreated it: the indexes went
    r = p4d([g137._table(A), g137._table(B)], replace=[A],
            conversions={A: _conv(A, ix_a)}, counts={A: 9, B: 1})
    events = p4d.events
    assert _submitted(events) == [[ix_a], [ix_a]]  # before its rows, and after every table
    assert _last(events, "unit") < _last(events, "pass-open")
    assert ("complete", A, True) in events
    assert not r.chunks[A].indexes_pending and r.counts.index_pending == 0
    # The builds of the dropped copy leave the record; the new one is watched.
    record = r.record
    assert (record.submitted, record.completed, record.completed_by_table) == (1, 0, {})
    assert [(b.index, b.job_id) for b in record.building] == [("ix_a", "job-2")]


def test_aurora_dsql_not_answering_leaves_the_rest_to_the_pass_after_the_load(p4d) -> None:
    ix_a1, ix_a2, ix_b = _ddl("ix_a1", "a"), _ddl("ix_a2", "a"), _ddl("ix_b", "b")
    p4d.importer.unreachable_at = (1, 1)  # A's first went in, then nothing answered
    p4d.whole[B] = {"ddls": [ix_b]}       # never reached: its worker derives them, as before
    reads: list = []
    r = p4d([g137._table(A), g137._table(B)], replace=[A, B],
            conversions={A: _conv(A, ix_a1, ix_a2), B: _conv(B, ix_b)}, counts={A: 9, B: 1},
            index_reader=lambda t: reads.append(t.name) or {"ix_a1", "ix_a2"})
    events = p4d.events
    # Before the units: A cut off at ix_a2, B not reached (its worker got no pre_indexed).
    assert events[: _first(events, "unit")][:4] == [
        ("recreate", A), ("recreate", B), ("pass-open",), ("index", [ix_a1, ix_a2])]
    assert {w.table.name: w.pre_indexed for w in p4d.workers} == {A: True, B: False}
    # After them: A's rest is checked against the table first (ix_a2 may have landed: it
    # did) -- one catalog read, nothing submitted -- and B gets its own.
    assert reads == [A]
    assert _submitted(events)[1:] == [[ix_b]]
    assert not any(c.indexes_pending for c in r.chunks.values()) and r.counts == (0, 0, 0)
    assert ("warning", "Aurora DSQL did not answer while secondary indexes were submitted "
                       "before the load: 2 table(s) get the rest of theirs once every "
                       "table's data has loaded (last attempt: 08006)") in _pass_lines(
        p4d.activity)
    # A's build that went in (job-1, built during the wait) is of this very relation: it
    # stays counted; B's, submitted after the load, is watched.
    record = r.record
    assert record.completed_by_table == {A: 1}
    assert [(b.table, b.index, b.job_id) for b in record.building] == [(B, "ix_b", "job-2")]
    assert (record.submitted, record.completed) == (2, 1)


def test_the_rest_of_a_cut_off_table_is_submitted_when_it_is_not_there(p4d) -> None:
    ux_bad, ix_a1, ix_a2 = (_ddl("ux_bad", "a", unique=True), _ddl("ix_a1", "a"),
                            _ddl("ix_a2", "a"))
    # ux_bad failed for good (recorded), ix_a1 went in, ix_a2 is where Aurora DSQL stopped.
    p4d.importer.fail[ux_bad] = "UNIQUE ux_bad: 54000 more than 24 indexes"
    p4d.importer.unreachable_at = (1, 2)
    r = p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, ux_bad, ix_a1, ix_a2)},
            index_reader=lambda t: {"a_pkey", "ix_a1"})
    # Only the rest -- from the one not answered on -- and of it only what is not there yet:
    # the failed one is not submitted again (its failure was recorded once).
    assert _submitted(p4d.events) == [[ux_bad, ix_a1, ix_a2], [ix_a2]]
    assert [e for e in r.errors if "ux_bad" in e] == [
        "index not created: UNIQUE ux_bad: 54000 more than 24 indexes — the table's DATA "
        "loaded completely; add the index later or reduce the table's index count (Aurora "
        "DSQL allows 24 per table, including the primary key)"]
    assert not r.chunks[A].indexes_pending
    # The pass did not void the cut-off table's earlier build: no recreate since.
    record = r.record
    assert record.completed_by_table == {A: 1}
    assert [(b.index, b.job_id) for b in record.building] == [("ix_a2", "job-2")]


def test_with_nothing_to_load_the_pass_still_completes_carried_flags(p4d) -> None:
    ix_old, ix_new = _ddl("ix_old", "a"), _ddl("ix_new", "a")
    handle = g137._Handle([A])
    handle.job.chunks = [ChunkState(chunk_id=A, status="DONE", indexes_pending=True)]
    r = p4d([], handle=handle, conversions={A: _conv(A, ix_old, ix_new)},
            inventory=[g137._table(A)], index_reader=lambda t: {"ix_old"})
    assert _submitted(p4d.events) == [[ix_new]] and not r.chunks[A].indexes_pending


# ---------------------------------------------------------------------------
# 4. The thread path: one table at a time
# ---------------------------------------------------------------------------


class _ThreadImporter(_FakeImporter):
    """The load's importer and the index submissions, into one event list."""

    def __init__(self, events: list, jobs: _Pass) -> None:
        super().__init__()
        self.events = events
        self.jobs = jobs

    def import_rows(self, rows, table, **kw):
        self.events.append(("rows", table.name, kw.get("index_ddls")))
        return super().import_rows(rows, table, **kw)

    def index_pass(self):
        return self.jobs.index_pass()


class _FlakyExporter(_FakeExporter):
    """Loses the source connection on the first read of each table in ``flaky``."""

    def __init__(self, rows, flaky=()) -> None:
        super().__init__(rows)
        self.flaky = set(flaky)

    def stream_converted_rows(self, conn, table, **kw):
        rows = super().stream_converted_rows(conn, table, **kw)
        if table.name not in self.flaky:
            return rows
        self.flaky.discard(table.name)

        def _dropped():
            raise _lost_connection()
            yield  # pragma: no cover - a generator that fails on its first row

        return _dropped()


def _thread_rig(monkeypatch, *, recreate, flaky=(), replace=("orders", "customers")):
    events: list = []
    jobs = _Pass(events)
    builds = _Builds(events)
    clock = _Clock()
    real_wait = engine._wait_for_index_builds_before_load
    monkeypatch.setattr(
        engine, "_wait_for_index_builds_before_load",
        lambda handle, migrator, outcome, **kw: real_wait(
            handle, migrator, outcome, sleep=clock.sleep, monotonic=clock.monotonic, **kw
        ),
    )
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS", "0")

    def _recreator(table):
        events.append(("recreate", table.name))
        return list(recreate.get(table.name, ()))

    importer = _ThreadImporter(events, jobs)
    inputs = dataclasses.replace(_ui_inputs(), replace_tables=frozenset(replace))
    migrator = BatchedTableMigrator(
        inputs,
        exporter=_FlakyExporter({"orders": [{"id": 1}, {"id": 2}], "customers": [{"id": 3}]},
                                flaky),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer,
        table_recreator=_recreator,
        target_counter=lambda t: None,
        index_build_reader=builds,
    )
    return SimpleNamespace(events=events, builds=builds, clock=clock, migrator=migrator,
                           tables=list(inputs.inventory.tables))


def _run(job_fn) -> MigrationJob:
    manager = JobManager()
    job_id = manager.submit(job_fn)
    assert manager.wait(job_id, timeout=10.0)
    job = manager.get_status(job_id)
    job._error = manager.get_error(job_id)  # type: ignore[attr-defined]
    return job


def test_the_thread_path_indexes_each_table_right_after_its_own_recreate(monkeypatch) -> None:
    ix_o, ix_c = _ddl("ix_orders", "orders"), _ddl("ix_customers", "customers")
    rig = _thread_rig(monkeypatch, recreate={"orders": [ix_o], "customers": [ix_c]})
    rig.builds.script = {"ix_orders": ["building", "completed"]}
    job = _run(lambda h: run_full_load(h, rig.tables, migrator=rig.migrator,
                                       error_log=ErrorLogStore()))
    assert job.status == "DONE", job._error
    # Each one: recreated, indexed on the empty table, its builds waited for, then loaded --
    # nothing else loads meanwhile -- and no pass after the last one.
    assert rig.events == [
        ("recreate", "orders"), ("pass-open",), ("index", [ix_o]), ("pass-close",),
        ("read", ["ix_orders"]), ("read", ["ix_orders"]), ("rows", "orders", None),
        ("recreate", "customers"), ("pass-open",), ("index", [ix_c]), ("pass-close",),
        ("read", ["ix_customers"]), ("rows", "customers", None),
    ]
    assert rig.clock.sleeps == [1.0]
    assert not any(c.indexes_pending for c in job.chunks)
    record = job.index_builds
    assert (record.submitted, record.completed, record.building) == (2, 2, [])
    assert record.data_loaded_at is not None


def test_a_thread_path_reread_indexes_the_recreated_table_again(monkeypatch) -> None:
    ix_o = _ddl("ix_orders", "orders")
    rig = _thread_rig(monkeypatch, recreate={"orders": [ix_o]}, flaky={"orders"},
                      replace=("orders",))
    rig.tables = rig.tables[:1]
    job = _run(lambda h: run_full_load(h, rig.tables, migrator=rig.migrator,
                                       error_log=ErrorLogStore()))
    assert job.status == "DONE", job._error
    # The re-read recreated the table: its indexes went with the old copy, so they go on the
    # new one before its rows, and the old build leaves the record.
    assert [e for e in rig.events if e[0] in ("recreate", "index")] == [
        ("recreate", "orders"), ("index", [ix_o]), ("recreate", "orders"), ("index", [ix_o])]
    record = job.index_builds
    assert (record.submitted, record.completed, record.building) == (1, 1, [])
    assert record.completed_by_table == {"orders": 1}


def test_a_failed_build_on_the_thread_path_is_recorded_once_the_table_loaded(
        monkeypatch) -> None:
    ux_o = _ddl("ux_orders", "orders", unique=True)
    rig = _thread_rig(monkeypatch, recreate={"orders": [ux_o]}, replace=("orders",))
    rig.tables = rig.tables[:1]
    rig.builds.script = {"ux_orders": ["failed"]}
    rig.builds.details = {"ux_orders": f"Key (email)=({_KEY_VALUE}) is duplicated"}
    log = ErrorLogStore()
    job = _run(lambda h: run_full_load(h, rig.tables, migrator=rig.migrator, error_log=log))
    # A missing UNIQUE constraint after the load is a warning on a loaded table, not a failure.
    assert job.status == "DONE", job._error
    (message,) = [r.message for r in log.records(job.job_id)]
    assert message.startswith("index not created: UNIQUE ux_orders: Aurora DSQL's build of the "
                              "index failed:")
    assert _KEY_VALUE not in message
    assert job.index_builds.submitted == 0  # it left the record: the error log reports it


def test_a_thread_path_table_that_fails_after_its_recreate_drops_its_old_builds(
        monkeypatch) -> None:
    ix_o = _ddl("ix_orders", "orders")
    rig = _thread_rig(monkeypatch, recreate={"orders": [ix_o]}, replace=("orders",))
    rig.tables = rig.tables[:1]
    rig.migrator._importer_factory(None)._failures = 1  # every batch fails
    earlier = IndexBuilds(submitted=2, completed=1, failed=1, completed_by_table={"orders": 1},
                          failures=[IndexBuild(table="orders", index="ix_old", job_id="j0",
                                               details="boom")])

    def _work(handle):
        handle.update(lambda job: setattr(job, "index_builds", earlier))
        run_full_load(handle, rig.tables, migrator=rig.migrator, error_log=ErrorLogStore())

    job = _run(_work)
    assert job.status == "FAILED" and job.chunks[0].status == "FAILED"
    # Its old relation went at the recreate: its records go then, though the load failed;
    # the build of the new copy is recorded (the table has it until a retry recreates it).
    record = job.index_builds
    assert record.failures == [] and record.completed_by_table == {"orders": 1}
    assert (record.submitted, record.completed, record.failed) == (1, 1, 0)


def test_the_thread_path_voids_a_recreated_tables_old_builds_at_its_recreate(
        monkeypatch) -> None:
    # A table with no secondary index left: nothing to submit, but its old relation's
    # records go at its recreate -- the pass after the load has nothing of it to void.
    rig = _thread_rig(monkeypatch, recreate={}, replace=("orders",))
    rig.tables = rig.tables[:1]
    earlier = IndexBuilds(submitted=1, completed=1, completed_by_table={"orders": 1})
    voided_at: list = []
    real_void = engine._void_index_builds

    def _void(job, tables):
        voided_at.append((sorted(tables), [e[0] for e in rig.events]))
        return real_void(job, tables)

    monkeypatch.setattr(engine, "_void_index_builds", _void)

    def _work(handle):
        handle.update(lambda job: setattr(job, "index_builds", earlier))
        run_full_load(handle, rig.tables, migrator=rig.migrator, error_log=ErrorLogStore())

    job = _run(_work)
    assert job.status == "DONE", job._error
    assert job.index_builds.completed_by_table == {} and job.index_builds.submitted == 0
    # Voided once, right after the recreate and before its rows.
    assert voided_at == [(["orders"], ["recreate"])]


def test_the_thread_path_with_several_threads_keeps_the_pass_after_the_load(
        monkeypatch) -> None:
    seen: list = []

    class _Migrator:
        _inputs = SimpleNamespace(cdc_coexisting=False, replace_tables=frozenset({"t"}))

        def migrate_table(self, table, *, on_rows=None, should_cancel=None,
                          defer_indexes=False, before_load_indexes=None):
            seen.append(before_load_indexes)
            return engine.TableLoadResult(rows_loaded=1)

    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    handle = g137._Handle(["t", "u"])
    tables = [TableDef(name=n, columns=[ColumnDef(name="id", mysql_type="int")],
                       primary_key=["id"]) for n in ("t", "u")]
    engine._migrate_tables_in_parallel(handle, "j", tables, _Migrator(), ErrorLogStore())
    assert seen == [None, None]
    # One thread: the hook is handed down.
    seen.clear()
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    engine._migrate_tables_in_parallel(g137._Handle(["t"]), "j", tables[:1], _Migrator(),
                                       ErrorLogStore())
    assert len(seen) == 1 and callable(seen[0])


def test_the_hook_returns_what_the_load_still_owes(monkeypatch) -> None:
    calls: list = []
    outcome = engine._IndexesBeforeLoad(reached={"t"}, cut_off={"t"}, owed={"t": ["rest"]},
                                        failures={"t": ["ix: boom"]})
    monkeypatch.setattr(engine, "_submit_indexes_before_load",
                        lambda h, m, ddls, **kw: calls.append(dict(ddls)) or outcome)
    monkeypatch.setattr(engine, "_wait_for_index_builds_before_load", lambda *a, **kw: None)
    voided, cut_off = set(), set()
    handle = g137._Handle(["t"])
    hook = engine._indexes_before_load_hook(handle, object(), load_stats=None,
                                            voided=voided, cut_off=cut_off)
    assert hook("t", ["ix", "rest"]) == (["rest"], ["ix: boom"])
    assert calls == [{"t": ["ix", "rest"]}] and cut_off == {"t"} and voided == {"t"}
    # Not reached (a Stop): every DDL is owed, nothing failed.
    outcome = engine._IndexesBeforeLoad()
    assert hook("t", ["ix"]) == (["ix"], []) and cut_off == set()  # a fresh recreate
    # None to submit: nothing owed, no submission.
    calls.clear()
    assert hook("t", []) == ([], []) and calls == []


# ---------------------------------------------------------------------------
# 5. The record, the caption, the notice
# ---------------------------------------------------------------------------


def _build(index, table=A, job_id="j-1") -> IndexBuild:
    return IndexBuild(table=table, index=index, job_id=job_id)


def test_builds_before_the_load_have_no_data_loaded_time_until_every_table_loaded() -> None:
    job = MigrationJob(job_id="j", index_builds=IndexBuilds(
        submitted=1, completed=1, data_loaded_at=datetime(2026, 10, 25, tzinfo=timezone.utc)))
    engine._merge_index_builds(job, A, [_build("ix_a")], recreated=True, before_load=True)
    record = job.index_builds
    assert record.data_loaded_at is None and record.submitted_at is not None
    assert [(b.table, b.index) for b in record.building] == [(A, "ix_a")]
    when = datetime(2026, 10, 26, 9, tzinfo=timezone.utc)
    engine._stamp_data_loaded(job, when)
    assert job.index_builds.data_loaded_at == when
    engine._stamp_data_loaded(job, when + timedelta(hours=1))  # set once
    assert job.index_builds.data_loaded_at == when
    # Nothing recorded: nothing to stamp.
    empty = MigrationJob(job_id="e")
    engine._stamp_data_loaded(empty, when)
    assert empty.index_builds is None
    # The payload leaves the unset time out, so the snapshot still loads.
    job.index_builds = record
    again = MigrationJob.model_validate_json(job.model_dump_json())
    assert again.index_builds == record and "data_loaded_at" not in job.model_dump_json()


def test_a_fold_before_the_load_counts_completions_and_drops_failures() -> None:
    at = datetime(2026, 10, 26, tzinfo=timezone.utc)
    job = MigrationJob(job_id="j", index_builds=IndexBuilds(
        submitted=3, building=[_build("ix_a"), _build("ux_a"), _build("ix_b", B)],
        submitted_at=at))
    reads = {(A, "ix_a"): IndexBuildRead("completed"),
             (A, "ux_a"): IndexBuildRead("failed", "could not create unique index"),
             (B, "ix_b"): None}
    engine._fold_builds_before_load(job, reads, at + timedelta(seconds=5), {(A, "ux_a")})
    record = job.index_builds
    assert (record.submitted, record.completed, record.failed) == (2, 1, 0)
    assert [b.index for b in record.building] == ["ix_b"] and record.failures == []
    assert record.finished_at is None
    engine._fold_builds_before_load(job, {(B, "ix_b"): IndexBuildRead("completed")},
                                    at + timedelta(seconds=9), set())
    assert job.index_builds.finished_at == at + timedelta(seconds=9)


def test_the_caption_says_the_run_waits_for_the_builds_before_loading() -> None:
    job = MigrationJob(
        job_id="j", status="RUNNING", watermark=_watermark(),
        chunks=[ChunkState(chunk_id=A), ChunkState(chunk_id=B)],
        index_builds=IndexBuilds(submitted=3, completed=1,
                                 building=[_build("ix_a"), _build("ix_a2"), _build("x", "z")]),
    )
    assert full_load_progress_caption(job) == (
        "Waiting for Aurora DSQL to build the secondary indexes of 1 recreated table(s) "
        "before loading (2 still building)…")
    # A table has started: the load's caption. The data has loaded: never the wait's.
    job.chunks[0].status = "IN_PROGRESS"
    assert full_load_progress_caption(job).startswith("Exporting and loading app.a")
    job.chunks[0].status = "PENDING"
    job.index_builds = job.index_builds.model_copy(
        update={"data_loaded_at": datetime(2026, 10, 26, tzinfo=timezone.utc)})
    assert full_load_progress_caption(job).startswith("Exporting and loading tables")


def test_the_notice_says_the_indexes_were_built_before_the_data_finished_loading() -> None:
    loaded = datetime(2026, 10, 26, 3, 20, tzinfo=timezone.utc)
    record = IndexBuilds(submitted=25, completed=25, data_loaded_at=loaded,
                         finished_at=loaded - timedelta(minutes=6))
    ui = _RecordingUi()
    ib.render_index_builds(ui, MigrationJob(job_id="j", status="DONE", index_builds=record))
    assert ui.texts == [
        "Secondary indexes built: all 25, the last by 03:14 UTC (before the data finished "
        "loading)"]


def test_the_failed_build_texts_no_longer_say_afterwards(monkeypatch) -> None:
    lines: list = []
    monkeypatch.setattr(ib, "log_activity", lambda *_a, **kw: lines.append(kw.get("detail")))
    failed = IndexBuild(table=A, index="ix_a", job_id="j", details="boom")
    ib._log_failed_build(failed)
    assert lines[0].startswith("Aurora DSQL's build of the index failed: boom. ")
    ui = _RecordingUi()
    ib.render_index_builds(ui, MigrationJob(job_id="j", status="DONE", index_builds=IndexBuilds(
        submitted=1, failed=1, failures=[failed])))
    assert ui.texts[1].startswith("The data loaded, but Aurora DSQL could not build this "
                                  "index. app.a.ix_a: boom.")


def test_the_index_ddls_before_the_load_follow_each_tables_unit() -> None:
    a, events = g137._table(A), g137._table(g137.EVENTS)

    class _Migrator:
        _inputs = g137._inputs([a, events], conversions={
            A: _conv(A, "conv a"), g137.EVENTS: _conv(g137.EVENTS, "conv events")})

    units = [engine._Unit("table", a), engine._Unit("shard", events, None, 250, 0)]
    plan = engine._before_load_index_ddls(
        _Migrator(), [a, events], units, {A: ["recreate a"], g137.EVENTS: ["recreate e"]},
        recreated={A, g137.EVENTS},
    )
    assert plan == {A: ["conv a"], g137.EVENTS: ["recreate e"]}
    # A table not recreated (a failed recreate, a withheld one) is left out.
    assert engine._before_load_index_ddls(
        _Migrator(), [a], units[:1], {A: ["recreate a"]}, recreated=set()) == {}


def test_a_worker_given_pre_indexed_owes_no_index_and_a_reread_owes_them_again(
        monkeypatch) -> None:
    from tests.test_ui_data_migration import _inputs, _tables

    importer = _FakeImporter()
    table = _tables()[0]
    conv = TableConversion(
        table="orders",
        target_ddl='CREATE TABLE "orders" ("id" INT NOT NULL, PRIMARY KEY ("id"))',
        index_ddls=['CREATE INDEX ASYNC "i" ON "orders" ("id")'],
    )
    migrator = BatchedTableMigrator(
        dataclasses.replace(_inputs(), replace_tables=frozenset({"orders"}),
                            table_conversions={"orders": conv}),
        exporter=_FakeExporter({"orders": [{"id": 1}]}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i: importer, table_recreator=lambda t: ["recreated"],
        target_counter=lambda t: None,
    )
    pre = migrator.migrate_table(table, pre_recreated=True, pre_indexed=True,
                                 defer_indexes=True)
    assert pre.deferred_index_ddls == () and importer.index_ddls_by_table["orders"] is None
    # A pre-recreated table the parent did not index derives its conversion's, as before.
    plain = migrator.migrate_table(table, pre_recreated=True, defer_indexes=True)
    assert plain.deferred_index_ddls == ('CREATE INDEX ASYNC "i" ON "orders" ("id")',)
    # A re-read recreates it (pre_recreated False): the recreate's, whatever pre_indexed says.
    again = migrator.migrate_table(table, pre_indexed=True, defer_indexes=True)
    assert again.deferred_index_ddls == ("recreated",)


# ---------------------------------------------------------------------------
# 6. A secondary UNIQUE 23505 is a data error; a primary-key 23505 keeps its recovery
# ---------------------------------------------------------------------------


class UniqueViolation(Exception):
    """psycopg's UniqueViolation as the loader meets it: SQLSTATE 23505, the violated index
    in its diagnostic (``named``) and the primary message, and the values in DETAIL."""

    def __init__(self, index: str, value: object, *, named: bool = True) -> None:
        primary = (
            f'duplicate key value violates unique constraint "{index}"' if named
            else "duplicate key value violates unique constraint"
        )
        super().__init__(f"{primary}\nDETAIL:  Key (email)=({value}) already exists.")
        self.sqlstate = "23505"
        self.diag = SimpleNamespace(constraint_name=index if named else None)


class _Users:
    """Aurora DSQL's "app"."users": ``id`` its primary key (index ``users_pkey``), ``email``
    UNIQUE (``users_email_key``). The rows, the catalog's answer for the primary key's index
    (``catalog``: a name, None, or an Exception the read raises), every statement's kind.
    ``secondary_first``: a row whose key AND email exist is reported on the email index.
    ``race``: called before each INSERT (a concurrent writer)."""

    def __init__(self, rows=(), *, catalog="users_pkey", named=True,
                 secondary_first=False) -> None:
        self.rows = dict(rows)
        self.catalog = catalog
        self.named = named
        self.secondary_first = secondary_first
        self.race = None
        self.kinds: list = []
        self.catalog_reads = 0
        self.lock = threading.Lock()

    def connect(self):
        return _UsersConnection(self)


class _UsersConnection:
    def __init__(self, store) -> None:
        self.store = store
        self.closed = False
        self.broken = False
        self.info = SimpleNamespace(transaction_status=psycopg.pq.TransactionStatus.IDLE)

    def cursor(self):
        return _UsersCursor(self.store)

    def close(self) -> None:
        self.closed = True


class _UsersCursor:
    def __init__(self, store) -> None:
        self.store = store
        self.rowcount = -1
        self.description = None
        self._rows: list = []

    def execute(self, statement, params=None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        store = self.store
        with store.lock:
            if "pg_catalog.pg_index" in text:
                store.kinds.append("catalog")
                store.catalog_reads += 1
                assert "indisprimary" in text and params == {"schema": "app", "table": "users"}
                if isinstance(store.catalog, Exception):
                    raise store.catalog
                self._rows = [(store.catalog,)] if store.catalog else []
                return
            if text.lstrip().startswith("SELECT"):
                store.kinds.append("select")
                self._rows = [(key,) for key in (params or []) if key in store.rows]
                return
            store.kinds.append("insert")
            if store.race is not None:
                store.race(store)
            values = list(params or [])
            batch = [(values[i], values[i + 1]) for i in range(0, len(values), 2)]
            emails = set(store.rows.values())
            seen: set = set()
            for key, email in batch:
                key_taken = key in store.rows
                email_taken = email in emails or email in seen
                if key_taken and not (store.secondary_first and email_taken):
                    raise UniqueViolation("users_pkey", key, named=store.named)
                if email_taken:
                    raise UniqueViolation("users_email_key", email, named=store.named)
                seen.add(email)
            for key, email in batch:
                store.rows[key] = email
            self.rowcount = len(batch)

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        pass


_USERS = TableDef(name="app.users", columns=[
    ColumnDef(name="id", mysql_type="int", nullable=False),
    ColumnDef(name="email", mysql_type="varchar(64)", nullable=False)], primary_key=["id"])


def _load(store, rows, mode, *, attempts: int = 3):
    importer = BatchedImporter(
        BatchedImportOptions(batch_size=4, parallelism=1),
        connection_factory=store.connect, occ_max_attempts=attempts, occ_base_delay=0.0,
        sleep=lambda _s: None, jitter=lambda: 0.0,
    )
    return importer.import_rows(iter(rows), _USERS, on_conflict=mode, key_columns=["id"])


def _users(*pairs) -> list:
    return [{"id": key, "email": email} for key, email in pairs]


_DUPLICATED = _users((1, _KEY_VALUE), (2, "bob@example.com"), (3, _KEY_VALUE),
                     (4, "carol@example.com"))


@pytest.mark.parametrize("mode", [OnConflictMode.NONE, OnConflictMode.SKIP_EXISTING])
def test_a_row_a_secondary_unique_index_rejects_is_quarantined(mode) -> None:
    store = _Users()
    result = _load(store, _DUPLICATED, mode)
    # The rest of the table loads; the duplicate is ONE dropped row, never a failed batch.
    assert (result.rows_loaded, result.failures, result.quarantined) == (3, 0, 1)
    assert sorted(store.rows) == [1, 2, 4]
    (record,) = result.quarantine_records
    assert record.primary_key == "id=3" and record.error_code == "23505"
    assert record.message == (
        'UniqueViolation: duplicate key value violates unique constraint "users_email_key"')
    assert _KEY_VALUE not in record.model_dump_json()
    assert store.catalog_reads == 1  # the primary key's index, read once and kept


@pytest.mark.parametrize("mode", [OnConflictMode.NONE, OnConflictMode.SKIP_EXISTING])
def test_a_primary_key_race_keeps_the_recovery(mode) -> None:
    # A concurrent writer takes key 4 between the SELECT of the present keys and the INSERT
    # of the rest: a duplicate of the KEY, so the rows are re-derived and the rest loaded.
    store = _Users(rows={2: "bob@example.com"} if mode is OnConflictMode.SKIP_EXISTING else {})
    taken = {"done": False}

    def _race(s):
        if s.kinds[-2:] == ["select", "insert"] and not taken["done"]:
            taken["done"] = True
            s.rows[4] = "carol@example.com"

    store.race = _race
    rows = _users((1, "a@example.com"), (2, "bob@example.com"), (3, "c@example.com"),
                  (4, "carol@example.com"))
    if mode is OnConflictMode.NONE:
        # NONE meets 23505 only on a retry of a batch whose first attempt landed: seed it.
        store.rows[2] = "bob@example.com"
    result = _load(store, rows, mode)
    assert (result.failures, result.quarantined) == (0, 0)
    assert result.rows_loaded + result.conflicts == 4 and sorted(store.rows) == [1, 2, 3, 4]
    assert store.catalog_reads == 1  # it named the primary key: told apart, then retried


def test_a_landed_batch_run_again_is_skipped_whichever_index_is_named() -> None:
    # NONE's retry of a batch that had landed: every key is there, and the server names the
    # email index -- the select-filter still finds every key present and skips them all.
    pairs = ((1, "a@example.com"), (2, "b@example.com"))
    store = _Users(rows=dict(pairs), secondary_first=True)
    result = _load(store, _users(*pairs), OnConflictMode.NONE)
    assert (result.rows_loaded, result.conflicts, result.quarantined, result.failures) == (
        0, 2, 0, 0)
    assert store.catalog_reads == 0  # nothing to tell apart: no row was inserted


@pytest.mark.parametrize("unknown", ["no-name", "no-catalog", "catalog-fails"])
def test_an_index_that_cannot_be_told_apart_keeps_the_old_retry(unknown) -> None:
    store = _Users(
        named=unknown != "no-name",
        catalog=None if unknown == "no-catalog" else (
            RuntimeError("catalog down") if unknown == "catalog-fails" else "users_pkey"),
    )
    result = _load(store, _DUPLICATED, OnConflictMode.SKIP_EXISTING)
    # Unknown is not a data error: the batch retries, then fails as before (no row dropped).
    assert result.failures == 1 and result.quarantined == 0
    assert "exhausted 3 attempts due to concurrent unique violations" in result.first_error
    assert store.catalog_reads == (0 if unknown == "no-name" else 1)


def test_violated_constraint_name_reads_the_diagnostic_then_the_primary_message() -> None:
    assert violated_constraint_name(UniqueViolation("ux", _KEY_VALUE)) == "ux"
    by_text = _Err('duplicate key value violates unique constraint "ux two"\nDETAIL:  Key '
                   f"(email)=({_KEY_VALUE}) already exists.", "23505")
    assert violated_constraint_name(by_text) == "ux two"
    assert violated_constraint_name(_Err("duplicate key value violates unique constraint",
                                         "23505")) is None
    assert violated_constraint_name(_Err("", "23505")) is None

    class _Unreadable(Exception):
        @property
        def diag(self):
            raise RuntimeError("no diagnostic")

    assert violated_constraint_name(_Unreadable('violates unique constraint "u"')) == "u"


def test_the_primary_key_index_is_read_by_relation() -> None:
    class _Cursor:
        def __init__(self, rows):
            self.rows, self.seen = rows, []

        def execute(self, statement, params=None):
            self.seen.append((statement, params))

        def fetchall(self):
            return self.rows

        def close(self):
            pass

    for rows, expected in (([("users_pkey",)], "users_pkey"), ([], None),
                           ([("a",), ("b",)], None), ([(None,)], None)):
        cursor = _Cursor(rows)
        connection = SimpleNamespace(cursor=lambda c=cursor: c)
        assert read_primary_key_index_name(connection, "app.users") == expected
        ((statement, params),) = cursor.seen
        assert "ix.indisprimary" in statement and params == {"schema": "app", "table": "users"}


def test_a_quarantined_rows_reason_names_its_unique_index() -> None:
    from dsql_migrator.core.batched_import import _BatchWork

    importer = BatchedImporter(BatchedImportOptions(), connection_factory=lambda: None)
    work = _BatchWork(chunk_id="c", table_name="app.users", columns=("id", "email"),
                      key_columns=("id",), on_conflict=OnConflictMode.NONE,
                      rows=({"id": 3, "email": _KEY_VALUE},))
    unnamed = _Err(f"duplicate key value\nDETAIL:  Key (email)=({_KEY_VALUE})", "23505")
    unnamed.diag = SimpleNamespace(constraint_name="users_email_key")
    importer._quarantine_one(work, unnamed)
    (record,) = importer._quarantine
    assert record.message == "_Err: duplicate key value [unique index: users_email_key]"
    assert _KEY_VALUE not in record.message
    # Named already: not twice.
    named = UniqueViolation("users_email_key", _KEY_VALUE)
    importer._quarantine_one(dataclasses.replace(work, rows=({"id": 4, "email": "x"},)), named)
    assert importer._quarantine[1].message.count("users_email_key") == 1


def test_the_quarantine_advice_for_a_secondary_unique_row() -> None:
    advice = engine._quarantine_recovery_advice("23505")
    assert advice.startswith("The rest of the table loaded. A UNIQUE index on Aurora DSQL")
    assert "remove the duplicate at the source" in advice and "Schema Conversion (Step 2)" in (
        advice)
    assert advice != engine._quarantine_recovery_advice("23514")


# ---------------------------------------------------------------------------
# 7. The perf harness, and the exclusion warning
# ---------------------------------------------------------------------------


def test_the_index_order_flag_parses(mp, monkeypatch) -> None:  # noqa: F811 - the fixture
    captured: dict = {}
    monkeypatch.setattr(mp, "cmd_full_load", lambda args: captured.setdefault("args", args) and 0)
    monkeypatch.setenv("DB_HOST", "db.invalid")
    monkeypatch.setenv("DB_PASSWORD", "unused-by-this-test")
    for argv, expected in (([], False), (["--indexes-after-load"], True)):
        captured.clear()
        monkeypatch.setattr("sys.argv", ["measure_performance.py", "full-load", *argv])
        mp.main()
        assert captured["args"].indexes_after_load is expected


def test_the_index_order_flag_sets_the_seam_and_the_report_says_so(
        mp, monkeypatch, tmp_path) -> None:  # noqa: F811
    monkeypatch.setenv(_SEAM, "")  # restored after the test, whatever the command sets
    monkeypatch.delenv(_SEAM)
    args, seen, report_path = _command_doubles(mp, monkeypatch, tmp_path)
    assert mp.cmd_full_load(args) == 0
    assert _SEAM not in os.environ
    assert json.loads(report_path.read_text())["index_order"] == "before-load"
    assert not any("secondary indexes after the load" in line for line in seen["log"])
    args.indexes_after_load = True
    assert mp.cmd_full_load(args) == 0
    assert os.environ[_SEAM] == "1"
    assert json.loads(report_path.read_text())["index_order"] == "after-load"
    assert any("with the secondary indexes after the load" in line for line in seen["log"])


def test_the_end_to_end_is_bounded_by_the_run_when_the_builds_finished_first(
        mp) -> None:  # noqa: F811 - the fixture
    loaded = datetime(2026, 10, 26, 3, 20, tzinfo=timezone.utc)
    started_at = (loaded - timedelta(seconds=300)).timestamp()
    early = IndexBuilds(submitted=25, completed=25, data_loaded_at=loaded,
                        submitted_at=loaded - timedelta(seconds=290),
                        finished_at=loaded - timedelta(seconds=250))
    builds, end_to_end = mp._index_builds_report(early, waited=0.0, read_errors=0,
                                                 started_at=started_at, elapsed=310.0)
    assert builds["seconds_after_load"] == 0.0 and end_to_end == 310.0
    late = early.model_copy(update={"finished_at": loaded + timedelta(seconds=40)})
    _builds, end_to_end = mp._index_builds_report(late, waited=40.0, read_errors=0,
                                                  started_at=started_at, elapsed=310.0)
    assert end_to_end == 340.0


def test_the_nulls_not_distinct_warning_says_a_drop_and_reload_rejects_them_too() -> None:
    from tests.test_g170_exclusion_nnd_match_full import _applied, _inventory, _warnings
    from dsql_migrator.core.column_exclusion import W4, build_exclusion_context

    ctx = build_exclusion_context(_inventory(), _applied(), warnings=True)
    (shared,) = _warnings(ctx, "public.users", "email", W4)
    assert ("rejected after the first (23505), on a Drop & reload too (the index is created "
            "before the rows load).") in shared
    assert "cannot be built after a Drop & reload" not in shared


def test_the_board_keeps_a_tables_submissions_before_and_after_its_rows_apart(p4d) -> None:
    board = LoadStatsBoard()
    ix_a = _ddl("ix_a", "a")
    p4d.whole[A] = {"recreated_ddls": [ix_a]}  # submitted before its rows, and after
    p4d([g137._table(A)], replace=[A], conversions={A: _conv(A, ix_a)}, load_stats=board)
    # Each is the table's own unit on the run's board: neither final snapshot replaces the
    # other, and both count into the table.
    units = {unit for (table, unit) in board._units if table == A}
    assert {"index before load", "index"} <= units
