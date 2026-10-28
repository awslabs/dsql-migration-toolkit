# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P4b: when Aurora DSQL finished building the secondary indexes a Full Load submitted.

The run's index pass submits ``CREATE INDEX ASYNC`` once every table's data has loaded
(P4a) and does not wait for the builds (7-12 minutes after an 8.5M-row load, live). Pinned
here, on doubles only (no MySQL, no Aurora DSQL, no AWS):

* the importer hands each submitted DDL's build job id to ``on_job`` (none for a failed
  DDL, once after an OCC retry, never failing the DDL);
* the pass records every build it submitted on the job (``MigrationJob.index_builds``) in
  the update that clears the table's flag -- a table that stays flagged records what it did
  submit -- voiding the records a recreate or a resubmission replaces, and a retry carries
  the earlier record; the payload stays loadable by an older build;
* ``index_build_states`` reads ``sys.jobs`` BY the recorded ids, falls back to
  ``pg_index`` for a build no longer listed (or with no id), and never calls
  ``sys.wait_for_job``;
* the fold, the watch (backoff, heartbeat, budget, cancel, one line per failure and one at
  the end, one live watch per job, a rest after it ended, the give-up of id-less builds),
  and the notice on the Data Migration step;
* the perf harness waits (bounded) and reports index-ready and end-to-end times;
* no key value reaches an activity line, the persisted record or the report (Property 7).
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._full_load_engine as engine
import dsql_migrator.ui.data_migration._index_builds as ib
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    IndexTargetUnreachable,
    _answered_job_id,
)
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobManager, JobNotFoundError
from dsql_migrator.core.models import ChunkState, IndexBuild, IndexBuilds, MigrationJob
from dsql_migrator.core.target_introspector import (
    INDEX_BUILD_UNLISTED_DETAILS,
    IndexBuildRead,
    index_build_states,
)
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    _merge_index_builds,
    run_full_load_retry,
)
from dsql_migrator.ui.design import NOTICE_STYLE

import tests.test_g137_mysql_reader_shards as g137
from tests.test_measure_performance import _command_doubles, mp  # noqa: F401 - fixture
from tests.test_p4a_deferred_indexes import (  # noqa: F401 - prig is a fixture
    A,
    B,
    C,
    _PassImporter,
    _RecordingImporter,
    _ddl,
    _pass_lines,
    _run,
    _thread_migrator,
    prig,
)
from tests.test_ui_data_migration import _FakeExporter, _FakeWatermarkCapturer, _watermark
from tests.test_ui_design import _RecordingUi

T0 = datetime(2026, 10, 25, 3, 0, 0, tzinfo=timezone.utc)
_OC001 = "schema has been updated by another transaction, please retry: (OC001)"
_KEY_VALUE = "alice@example.com"
_FAILED_UNIQUE_DETAILS = (
    f'could not create unique index "ux_email": Key (email)=({_KEY_VALUE}) is duplicated.'
)


class _Err(Exception):
    """A psycopg-like error: a message and its SQLSTATE."""

    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.sqlstate = sqlstate


@pytest.fixture(autouse=True)
def _no_watches():
    ib._WATCHES.clear()
    yield
    ib._WATCHES.clear()


@pytest.fixture(autouse=True)
def _indexes_after_load(monkeypatch):
    """The builds of the pass after every table's data (P4a's order, under P4d's measurement
    seam); the builds submitted before the rows: tests/test_p4d_indexes_before_load.py."""
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_INDEXES_AFTER_LOAD", "1")


def _build(index, *, table=A, job_id="j-1", unique=False, details=None) -> IndexBuild:
    return IndexBuild(table=table, index=index, job_id=job_id, unique=unique, details=details)


def _record(*building, completed=0, failures=(), submitted=None, at=T0) -> IndexBuilds:
    failures = list(failures)
    return IndexBuilds(
        submitted=(completed + len(failures) + len(building)) if submitted is None
        else submitted,
        completed=completed, failed=len(failures), building=list(building),
        failures=failures, data_loaded_at=at, submitted_at=at + timedelta(seconds=4),
    )


# ---------------------------------------------------------------------------
# 1. The importer: each submitted DDL's build job id
# ---------------------------------------------------------------------------

_NO_RESULT = object()  # the statement returned no result set (PostgreSQL, an old double)
_NO_ROW = object()     # a result set with no row


class _Target:
    """One fake Aurora DSQL: each CREATE answers the next of ``answers`` (a job id, None for
    a NULL id, _NO_ROW, _NO_RESULT) unless the next of ``errors`` is an exception."""

    def __init__(self, answers=(), errors=()):
        self.answers = list(answers)
        self.errors = list(errors)
        self.executed: list = []
        self.connects = 0

    def connect(self):
        self.connects += 1
        return _Conn(self)


class _Conn:
    def __init__(self, target):
        self.target = target

    def cursor(self):
        return _Cur(self.target)

    def close(self):
        pass


class _Cur:
    def __init__(self, target):
        self.target = target
        self.description = None
        self._row = None

    def execute(self, statement, params=None):
        self.target.executed.append(str(statement))
        error = self.target.errors.pop(0) if self.target.errors else None
        if error is not None:
            raise error
        answer = self.target.answers.pop(0) if self.target.answers else _NO_RESULT
        if answer is _NO_RESULT:
            return
        self.description = [("job_id",)]
        self._row = None if answer is _NO_ROW else (answer,)

    def fetchone(self):
        return self._row

    def close(self):
        pass


def _importer(target) -> BatchedImporter:
    return BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=1),
        connection_factory=target.connect, occ_max_attempts=5, occ_base_delay=0.0,
        sleep=lambda _s: None, jitter=lambda: 0.0,
    )


_IX = 'CREATE INDEX ASYNC "ix_a" ON "app"."a" ("b")'
_UX = 'CREATE UNIQUE INDEX ASYNC "ux_email" ON "app"."a" ("email")'


def test_each_submitted_ddl_hands_on_job_the_build_job_id_it_answered_with() -> None:
    target = _Target(answers=["4f0c1a2b-job", _NO_RESULT])
    calls: list = []
    created, failures = _importer(target).create_indexes(
        [_IX, _UX], on_job=lambda ddl, job: calls.append((ddl, job)))
    assert (created, failures) == (2, [])
    # A statement with no result set (PostgreSQL) answers no job id.
    assert calls == [(_IX, "4f0c1a2b-job"), (_UX, None)]


def test_a_null_id_or_no_row_answers_none_and_an_integer_id_is_kept_as_text() -> None:
    target = _Target(answers=[None, _NO_ROW, 42])
    calls: list = []
    _importer(target).create_indexes([_IX, _UX, _IX], on_job=lambda d, j: calls.append(j))
    assert calls == [None, None, "42"]


def test_a_failed_ddl_gets_no_on_job_call() -> None:
    target = _Target(answers=["job-2"], errors=[_Err('relation "ix_a" already exists', "42P07")])
    calls: list = []
    created, failures = _importer(target).create_indexes(
        [_IX, _UX], on_job=lambda ddl, job: calls.append((ddl, job)))
    assert created == 1 and len(failures) == 1 and failures[0].startswith("ix_a: ")
    assert calls == [(_UX, "job-2")]


def test_a_40001_then_success_reports_the_id_once() -> None:
    target = _Target(answers=["job-after-retry"], errors=[_Err(_OC001, "40001")])
    calls: list = []
    importer = _importer(target)
    created, failures = importer.create_indexes([_UX], on_job=lambda d, j: calls.append((d, j)))
    assert (created, failures) == (1, [])
    assert target.executed == [_UX, _UX]  # retried once
    assert calls == [(_UX, "job-after-retry")]
    assert importer.load_stats.snapshot()["index_retries"] == {"40001/OC001": 1}


def test_an_on_job_that_raises_never_fails_the_ddl() -> None:
    target = _Target(answers=["job-1", "job-2"])

    def _broken(_ddl, _job):
        raise RuntimeError("the job store is gone")

    created, failures = _importer(target).create_indexes([_IX, _UX], on_job=_broken)
    assert (created, failures) == (2, [])
    assert target.executed == [_IX, _UX]  # each submitted exactly once


def test_the_run_level_pass_passes_on_job_through_and_works_without_it() -> None:
    target = _Target(answers=["job-1", "job-2", "job-3"])
    calls: list = []
    with _importer(target).index_pass() as create:
        assert create([_IX], on_job=lambda d, j: calls.append(j)) == (1, [])
        assert create([_UX]) == (1, [])  # no on_job: submitted as before
        assert create([_IX], on_job=lambda d, j: calls.append(j)) == (1, [])
    assert calls == ["job-1", "job-3"]
    assert target.connects == 1  # one connection for the whole pass (P4a)


def test_reading_the_answer_never_raises() -> None:
    class _Cursor:
        description = [("job_id",)]

        def fetchone(self):
            raise RuntimeError("no result to fetch")

    class _Row:
        def __getitem__(self, _i):
            raise IndexError("no column")

    class _Odd:
        description = [("job_id",)]

        def fetchone(self):
            return _Row()

    assert _answered_job_id(_Cursor()) is None
    assert _answered_job_id(_Odd()) is None
    assert _answered_job_id(SimpleNamespace()) is None  # no description: no result set


# ---------------------------------------------------------------------------
# 2. The record on the job
# ---------------------------------------------------------------------------


def test_the_record_round_trips_and_leaves_unset_fields_out() -> None:
    record = _record(_build("ix_a"), _build("ux_email", job_id=None, unique=True))
    job = MigrationJob(job_id="j", index_builds=record)
    payload = json.loads(job.model_dump_json())
    assert payload["index_builds"] == {
        "submitted": 2, "completed": 0, "failed": 0,
        "data_loaded_at": "2026-10-25T03:00:00Z", "submitted_at": "2026-10-25T03:00:04Z",
        "building": [{"table": A, "index": "ix_a", "job_id": "j-1"},
                     {"table": A, "index": "ux_email", "unique": True}],
    }
    assert MigrationJob.model_validate_json(job.model_dump_json()) == job
    counted = MigrationJob(job_id="j", index_builds=_record(completed=2).model_copy(
        update={"completed_by_table": {A: 2}}))
    assert json.loads(counted.model_dump_json())["index_builds"]["completed_by_table"] == {A: 2}
    assert MigrationJob.model_validate_json(counted.model_dump_json()) == counted
    # None and empty lists are left out, so an older build still loads the snapshot.
    assert "index_builds" not in json.loads(MigrationJob(job_id="j").model_dump_json())
    assert json.loads(IndexBuilds().model_dump_json()) == {
        "submitted": 0, "completed": 0, "failed": 0}


def test_an_older_snapshot_without_the_record_loads() -> None:
    old = '{"job_id": "j", "status": "DONE", "chunks": [{"chunk_id": "a", "status": "DONE"}]}'
    job = MigrationJob.model_validate_json(old)
    assert job.index_builds is None and job.chunks[0].status == "DONE"


def test_a_build_record_is_frozen_and_forbids_unknown_fields() -> None:
    build = _build("ix_a")
    with pytest.raises(Exception):
        build.index = "other"  # type: ignore[misc]
    with pytest.raises(Exception):
        IndexBuild(table=A, index="ix", status="completed")  # type: ignore[call-arg]
    with pytest.raises(Exception):
        IndexBuilds(running=1)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# 3. The run's index pass records what it submitted
# ---------------------------------------------------------------------------


class _JobPass(_PassImporter):
    """The pass double answering a build job id per DDL (``job-1``, ``job-2``, ... in
    submission order; ``None`` for a DDL in ``no_id``)."""

    no_id: frozenset = frozenset()

    def _create(self, ddls, on_job=None):
        answer = super()._create(ddls)
        for ddl in ddls:
            self.jobs = getattr(self, "jobs", 0) + 1
            if on_job is not None:
                on_job(ddl, None if ddl in self.no_id else f"job-{self.jobs}")
        return answer


def test_the_pass_records_every_build_it_submitted(prig) -> None:
    prig.importer.__class__ = _JobPass
    ix_a, ux_b, ix_b = _ddl("ix_a", "a"), _ddl("ux_b", "b", unique=True), _ddl("ix_b", "b")
    prig.importer.no_id = frozenset({ix_b})
    prig.whole.update({A: {"ddls": [ix_a]}, B: {"ddls": [ux_b, ix_b]}})
    before = datetime.now(timezone.utc)
    r = prig([g137._table(A), g137._table(B)], replace=[A, B], counts={A: 10, B: 500})
    record = r.handle.job.index_builds
    assert (record.submitted, record.completed, record.failed) == (3, 0, 0)
    # Largest first (b), each with its job id, the UNIQUE one marked; no id: None.
    assert [(b.table, b.index, b.unique, b.job_id) for b in record.building] == [
        (B, "ux_b", True, "job-1"), (B, "ix_b", False, None), (A, "ix_a", False, "job-3")]
    # The data had loaded before the pass started; the submission came after.
    assert all(c.finished_at <= record.data_loaded_at for c in r.handle.job.chunks)
    assert before <= record.data_loaded_at <= record.submitted_at
    assert record.finished_at is None and not record.failures
    assert not any(c.indexes_pending for c in r.handle.job.chunks)
    # The pass's last line: the run does not wait for them.
    assert _pass_lines(prig.activity)[-1] == ("info", (
        "submitted 3 secondary index(es) of 2 table(s); Aurora DSQL builds them in the "
        "background, and the Data Migration step shows when they finish"))


def test_the_record_lands_in_the_update_that_clears_the_flag(prig) -> None:
    prig.importer.__class__ = _JobPass
    prig.whole.update({A: {"ddls": [_ddl("ix_a", "a")]}})
    handle = g137._Handle([A])
    states: list = []
    update = handle.update

    def _update(mutator):
        update(mutator)
        chunk = handle.job.chunks[0]
        record = handle.job.index_builds
        states.append((chunk.indexes_pending, len(record.building) if record else 0))

    handle.update = _update
    prig([g137._table(A)], replace=[A], handle=handle)
    # Never a moment with the flag cleared and its build unrecorded.
    assert (False, 0) not in states[states.index((True, 0)):]
    assert states[-1] == (False, 1)


def test_an_importer_without_on_job_records_nothing(prig) -> None:
    # The P4a double: its create takes no on_job, so it submits as before.
    prig.whole.update({A: {"ddls": [_ddl("ix_a", "a")]}})
    r = prig([g137._table(A)], replace=[A])
    assert r.handle.job.index_builds is None
    assert [e for e in prig.events if e[0] == "index"] == [("index", [_ddl("ix_a", "a")])]
    assert not [line for line in _pass_lines(prig.activity) if "submitted" in line[1]]


def test_a_stop_during_the_pass_records_the_builds_it_did_submit(prig) -> None:
    prig.importer.__class__ = _JobPass
    handle = g137._Handle([A, B])
    prig.whole.update({A: {"ddls": [_ddl("ix_a", "a")]}, B: {"ddls": [_ddl("ix_b", "b")]}})
    prig.importer.on_call = lambda n: setattr(handle, "cancelled", True) if n == 1 else None
    r = prig([g137._table(A), g137._table(B)], replace=[A, B], handle=handle,
             counts={A: 9, B: 1})
    assert [(b.table, b.index) for b in r.handle.job.index_builds.building] == [(A, "ix_a")]
    assert r.chunks[B].indexes_pending and not r.chunks[A].indexes_pending
    assert _pass_lines(prig.activity)[-1][1].startswith("submitted 1 secondary index(es) of 1")


def test_an_unreachable_target_records_what_the_table_submitted_and_keeps_its_flag(
        prig) -> None:
    ix_1, ix_2 = _ddl("ix_1", "a"), _ddl("ix_2", "a")

    class _Unreachable(_PassImporter):
        def _create(self, ddls, on_job=None):
            on_job(ddls[0], "job-1")  # the first one went in, then Aurora DSQL stopped answering
            error = IndexTargetUnreachable(1, [])
            error.__cause__ = _Err("server closed the connection unexpectedly", "08006")
            raise error

    prig.importer.__class__ = _Unreachable
    prig.whole.update({A: {"ddls": [ix_1, ix_2]}})
    r = prig([g137._table(A)], replace=[A])
    assert r.chunks[A].indexes_pending  # still owed (Retry unfinished tables)
    record = r.handle.job.index_builds
    assert [(b.index, b.job_id) for b in record.building] == [("ix_1", "job-1")]
    assert record.submitted == 1


def test_a_ddl_without_a_readable_name_is_recorded_by_its_label() -> None:
    build = engine._index_build_of(A, "CREATE UNIQUE INDEX ASYNC ix_Lower ON app.a (b)", "j")
    assert (build.index, build.unique, build.job_id) == ("ix_lower", True, "j")  # folded
    long = "x" * 70
    assert engine._index_build_of(A, f'CREATE INDEX ASYNC "{long}" ON app.a (b)', None).index \
        == long[:63]  # as the catalog stores it


def test_a_recreated_table_voids_its_earlier_records() -> None:
    job = MigrationJob(job_id="j", index_builds=_record(
        _build("ix_old", job_id="old-1"), _build("ix_other", table=B, job_id="old-2"),
        completed=3, failures=[_build("ux_email", job_id="old-3", unique=True,
                                      details="could not create unique index")]))
    new = [_build("ix_new", job_id="new-1"), _build("ux_email", job_id="new-2", unique=True)]
    loaded = T0 + timedelta(hours=2)
    _merge_index_builds(job, A, new, recreated=True, data_loaded_at=loaded)
    record = job.index_builds
    # A's old relation went with its indexes: its building AND failed records are void.
    assert [(b.table, b.index) for b in record.building] == [
        (B, "ix_other"), (A, "ix_new"), (A, "ux_email")]
    assert record.failures == [] and record.failed == 0
    assert record.submitted == record.completed + record.failed + len(record.building) == 6
    assert record.data_loaded_at == loaded and record.submitted_at > loaded


def test_a_resubmitted_index_voids_only_its_own_record() -> None:
    job = MigrationJob(job_id="j", index_builds=_record(
        _build("ix_old", job_id="old-1"),
        failures=[_build("ux_email", job_id="old-2", unique=True, details="dup"),
                  _build("ix_b", table=B, job_id="old-3", details="too many indexes")]))
    _merge_index_builds(job, A, [_build("ux_email", job_id="new-1", unique=True)],
                        recreated=False, data_loaded_at=T0)
    record = job.index_builds
    assert [(b.index, b.job_id) for b in record.building] == [("ix_old", "old-1"),
                                                               ("ux_email", "new-1")]
    assert [(f.table, f.index) for f in record.failures] == [(B, "ix_b")]
    assert (record.submitted, record.failed) == (3, 1)


def test_nothing_to_record_creates_no_record_and_a_new_build_reopens_a_finished_one() -> None:
    job = MigrationJob(job_id="j")
    _merge_index_builds(job, A, [], recreated=True, data_loaded_at=T0)
    assert job.index_builds is None
    finished = _record(completed=2)
    finished = finished.model_copy(update={"finished_at": T0 + timedelta(minutes=9)})
    job.index_builds = finished
    _merge_index_builds(job, A, [], recreated=False, data_loaded_at=T0)
    assert job.index_builds is finished  # nothing added, nothing void: untouched
    _merge_index_builds(job, B, [_build("ix_b", table=B)], recreated=True,
                        data_loaded_at=T0 + timedelta(hours=1))
    assert job.index_builds.finished_at is None and job.index_builds.submitted == 3


def test_voiding_the_last_building_record_finishes_the_record() -> None:
    job = MigrationJob(job_id="j", index_builds=_record(_build("ix_a"), completed=1))
    _merge_index_builds(job, A, [], recreated=True, data_loaded_at=T0)
    record = job.index_builds
    assert record.building == [] and record.finished_at is not None
    assert (record.submitted, record.completed) == (1, 1)


def test_a_recreated_table_takes_its_completed_builds_out_of_the_counts() -> None:
    job = MigrationJob(job_id="j", index_builds=IndexBuilds(
        submitted=5, completed=4, completed_by_table={A: 3, B: 1},
        building=[_build("ix_b2", table=B)], data_loaded_at=T0, submitted_at=T0))
    _merge_index_builds(job, A, [_build("ix_a", job_id="new-1")], recreated=True,
                        data_loaded_at=T0)
    record = job.index_builds
    # A's old relation went with its 3 built indexes: counted once, as the new build.
    assert record.completed_by_table == {B: 1} and record.completed == 1
    assert record.submitted == record.completed + record.failed + len(record.building) == 3
    # An index-only submission (no recreate) keeps the table's completed builds counted.
    _merge_index_builds(job, B, [_build("ix_b3", table=B)], recreated=False, data_loaded_at=T0)
    assert job.index_builds.completed_by_table == {B: 1} and job.index_builds.submitted == 4


def test_voiding_recreated_tables_keeps_the_counts_and_leaves_the_others() -> None:
    job = MigrationJob(job_id="j", index_builds=IndexBuilds(
        submitted=7, completed=3, failed=1, completed_by_table={A: 1, B: 1, C: 1},
        building=[_build("ix_a"), _build("ix_b", table=B), _build("ix_c", table=C)],
        failures=[_build("ux_b", table=B, unique=True, details="dup")],
        data_loaded_at=T0, submitted_at=T0))
    engine._void_index_builds(job, [A, B])
    record = job.index_builds
    assert [(b.table, b.index) for b in record.building] == [(C, "ix_c")]
    assert record.failures == [] and record.completed_by_table == {C: 1}
    assert (record.submitted, record.completed, record.failed) == (2, 1, 0)
    assert record.finished_at is None  # C still builds
    engine._void_index_builds(job, [C])
    assert job.index_builds.submitted == 0 and job.index_builds.finished_at is not None
    before = job.index_builds
    engine._void_index_builds(job, [A])  # nothing recorded for it: untouched
    assert job.index_builds is before
    nothing = MigrationJob(job_id="k")
    engine._void_index_builds(nothing, [A])
    assert nothing.index_builds is None


def test_a_recreate_voids_the_old_relations_builds_even_when_its_load_fails(prig) -> None:
    # A Drop & reload of B whose load fails: the pre-pass recreated B, so the builds an
    # earlier run recorded for its old relation -- a failed UNIQUE one, one building, and its
    # built ones -- are void although no index pass reaches B (a stale failure would name a
    # missing constraint of a relation that is gone).
    prig.importer.__class__ = _JobPass
    handle = g137._Handle([A, B])
    handle.job.index_builds = IndexBuilds(
        submitted=6, completed=3, failed=1, completed_by_table={A: 1, B: 2},
        building=[_build("ix_old", table=B, job_id="old-1"), _build("ix_a1", job_id="old-2")],
        failures=[_build("ux_b", table=B, job_id="old-3", unique=True, details="dup")],
        data_loaded_at=T0, submitted_at=T0)
    prig.whole.update({A: {"ddls": [_ddl("ix_new", "a")]}, B: {"status": "FAILED"}})
    r = prig([g137._table(A), g137._table(B)], replace=[A, B], handle=handle,
             counts={A: 9, B: 1})
    assert r.chunks[B].status == "FAILED"
    record = r.handle.job.index_builds
    assert [(b.table, b.index, b.job_id) for b in record.building] == [(A, "ix_new", "job-1")]
    assert record.failures == [] and record.failed == 0
    assert record.completed_by_table == {} and record.completed == 0
    assert record.submitted == 1


def test_the_thread_path_voids_a_recreated_tables_old_builds_before_its_pass(
        monkeypatch) -> None:
    # The pass never gets to submit orders' index (Aurora DSQL stops answering): orders stays
    # flagged, and the builds an earlier run recorded for its old relation are void all the
    # same -- the table was recreated before its rows.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    events: list = []
    migrator, tables = _thread_migrator(events, replace=["orders"],
                                        recreate={"orders": ["ix orders"]})

    class _Unanswered(_RecordingImporter):
        @contextlib.contextmanager
        def index_pass(self):
            def create(_ddls):
                raise IndexTargetUnreachable(0, [])

            yield create

    migrator._importer_factory(None).__class__ = _Unanswered
    handle = g137._Handle(["orders", "customers"])
    handle.job.index_builds = IndexBuilds(
        submitted=3, completed=2, completed_by_table={"orders": 1, "customers": 1},
        building=[_build("ix_o", table="orders")], data_loaded_at=T0, submitted_at=T0)
    engine._migrate_tables_in_parallel(handle, "j", tables, migrator, ErrorLogStore(),
                                       watermark=_watermark())
    chunks = {c.chunk_id: c for c in handle.job.chunks}
    assert chunks["orders"].indexes_pending  # still owed (Retry unfinished tables)
    record = handle.job.index_builds
    # orders was recreated: its old build and its built one are gone; customers appended.
    assert record.building == [] and record.completed_by_table == {"customers": 1}
    assert (record.submitted, record.completed) == (1, 1)


def _job_rig(monkeypatch, *, reader):
    """The P4a index-only rig, its pass answering a job id per DDL."""
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

    class _Importer(_RecordingImporter):
        jobs = 0

        @contextlib.contextmanager
        def index_pass(self):
            def create(ddls, on_job=None):
                self.events.append(("index", list(ddls)))
                for ddl in ddls:
                    type(self).jobs += 1
                    on_job(ddl, f"new-{type(self).jobs}")
                return len(ddls), []

            yield create

    migrator = BatchedTableMigrator(
        inputs, exporter=_FakeExporter({A: [{"id": 1}], B: [{"id": 2}]}),
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),
        importer_factory=lambda _i, imp=_Importer(events): imp,
        table_recreator=lambda t: list(engine._effective_conversion(inputs, t).index_ddls),
        target_counter=lambda t: None, target_generated_reader=lambda t: set(),
        target_index_reader=reader,
    )
    prior = [ChunkState(chunk_id=A, status="DONE", rows_loaded=1),
             ChunkState(chunk_id=B, status="DONE", rows_loaded=1, indexes_pending=True)]
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "1")
    return migrator, tables, prior, events


def test_a_retry_carries_the_earlier_record_and_adds_its_own(monkeypatch) -> None:
    migrator, tables, prior, events = _job_rig(monkeypatch, reader=lambda t: {"ix_old"})
    earlier = _record(_build("ix_a", job_id="old-1"), completed=1)
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[B], prior_index_builds=earlier))
    assert job.status == "DONE", job._error
    record = job.index_builds
    # The earlier run's build still watched beside the one this retry submitted.
    assert [(b.table, b.index, b.job_id, b.unique) for b in record.building] == [
        (A, "ix_a", "old-1", False), (B, "IX_New", "new-1", True)]
    assert (record.submitted, record.completed) == (3, 1)
    assert record.data_loaded_at > earlier.data_loaded_at  # this run's data
    assert len(earlier.building) == 1  # the prior record is copied, never changed
    # Without one, the retry starts with only its own.
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), index_only=[B]))
    assert [b.index for b in job.index_builds.building] == ["IX_New"]


def test_a_reload_that_recreates_the_table_replaces_its_failed_build(monkeypatch) -> None:
    # The remedy for a failed UNIQUE build after fixing the source: Reload with Drop & reload.
    migrator, tables, prior, events = _job_rig(monkeypatch, reader=lambda t: set())
    migrator.bind_run_inputs(dataclasses.replace(
        migrator._inputs, replace_tables=frozenset({B})))
    prior[1] = prior[1].model_copy(update={"indexes_pending": False})
    # Also a build of an index the reload's conversion no longer has: it went with the
    # old relation, so it is void too (not watched until it "fails").
    earlier = _record(_build("ix_gone", table=B, job_id="old-8"),
                      failures=[_build("IX_New", table=B, job_id="old-9", unique=True,
                                       details="could not create unique index")],
                      completed=1)
    job = _run(lambda h: run_full_load_retry(
        h, prior, [tables[1]], migrator=migrator, error_log=ErrorLogStore(),
        watermark=_watermark(), prior_index_builds=earlier))
    assert job.status == "DONE", job._error
    record = job.index_builds
    assert record.failures == [] and record.failed == 0
    assert "ix_gone" not in [b.index for b in record.building]
    assert [(b.index, b.job_id) for b in record.building] == [
        ("ix_old", f"new-{type(migrator._importer_factory(None)).jobs - 1}"),
        ("IX_New", f"new-{type(migrator._importer_factory(None)).jobs}")]
    assert record.submitted == record.completed + len(record.building)


# ---------------------------------------------------------------------------
# 4. index_build_states: sys.jobs by id, pg_index once a job is gone
# ---------------------------------------------------------------------------


class _Catalog:
    """sys.jobs rows by id and pg_index rows by relation; ``errors`` raise on the next
    statements in order (None: no error), ``unreadable`` relations always raise."""

    def __init__(self, jobs=None, indexes=None, *, errors=(), unreadable=(), down=False):
        self.jobs = dict(jobs or {})
        self.indexes = dict(indexes or {})
        self.errors = list(errors)
        self.unreadable = set(unreadable)
        self.down = down
        self.statements: list = []
        self.connects = 0
        self.closed = 0

    def connect(self):
        self.connects += 1
        if self.down:
            raise OSError("could not connect")
        return _CatalogConn(self)


class _CatalogConn:
    def __init__(self, db):
        self.db = db

    def cursor(self):
        return _CatalogCur(self.db)

    def rollback(self):
        pass

    def close(self):
        self.db.closed += 1


class _CatalogCur:
    def __init__(self, db):
        self.db = db
        self.rows: list = []

    def execute(self, statement, params=None):
        self.db.statements.append((statement, dict(params or {})))
        error = self.db.errors.pop(0) if self.db.errors else None
        if error is not None:
            raise error
        if "sys.jobs" in statement:
            self.rows = [(i, *self.db.jobs[i]) for i in params["ids"] if i in self.db.jobs]
            return
        relation = (f"{params['schema']}.{params['table']}" if "schema" in params
                    else params["table"])
        if relation in self.db.unreadable:
            raise _Err("permission denied for relation", "42501")
        self.rows = list(self.db.indexes.get(relation, ()))

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def _states(db, *builds):
    return index_build_states(list(builds), connection_factory=db.connect, sleep=lambda _s: None)


def test_sys_jobs_is_read_by_the_recorded_ids_only() -> None:
    db = _Catalog(jobs={"j-1": ("processing", None), "j-2": ("SUBMITTED", None)})
    reads = _states(db, _build("ix_a", job_id="j-2"), _build("ix_b", job_id="j-1"))
    assert reads == {(A, "ix_a"): IndexBuildRead("building"),
                     (A, "ix_b"): IndexBuildRead("building")}
    ((statement, params),) = db.statements
    assert "FROM sys.jobs WHERE job_id = ANY(%(ids)s)" in statement
    assert params == {"ids": ["j-1", "j-2"]}
    assert "object_name" not in statement and "job_type" not in statement
    assert db.connects == 1 and db.closed == 1


@pytest.mark.parametrize("status, details, expected", [
    ("submitted", None, IndexBuildRead("building")),
    ("processing", None, IndexBuildRead("building")),
    ("completed", None, IndexBuildRead("completed")),
    ("Completed", None, IndexBuildRead("completed")),
    ("failed", _FAILED_UNIQUE_DETAILS, IndexBuildRead("failed", _FAILED_UNIQUE_DETAILS)),
    ("cancelled", None, IndexBuildRead("failed", None)),
    ("canceled", None, IndexBuildRead("failed", None)),
    ("queued", None, IndexBuildRead("building")),  # still listed, not ended
])
def test_the_job_status_maps_to_a_build_state(status, details, expected) -> None:
    db = _Catalog(jobs={"j-1": (status, details)})
    assert _states(db, _build("ix_a"))[(A, "ix_a")] == expected
    assert len(db.statements) == 1  # no catalog read while the job is listed


def test_a_build_no_longer_listed_or_without_an_id_falls_back_to_pg_index() -> None:
    db = _Catalog(indexes={A: [("app", "ix_valid", True), ("app", "ix_invalid", False)]})
    reads = _states(
        db,
        _build("ix_valid", job_id="gone-1"),            # its job aged out, index valid
        _build("ix_invalid", job_id="gone-2"),          # its job aged out, not valid
        _build("ix_absent", job_id="gone-3"),           # its job aged out, no index
        _build("ix_invalid", table=A, job_id=None),     # (same key: the first id wins)
        _build("ix_noid_valid", table=B, job_id=None),
        _build("ix_noid", table=B, job_id=None),
    )
    assert reads[(A, "ix_valid")] == IndexBuildRead("completed")
    # Marked unlisted: an inference the fold holds back while the job cannot have aged out.
    unlisted = IndexBuildRead("failed", INDEX_BUILD_UNLISTED_DETAILS, unlisted=True)
    assert reads[(A, "ix_invalid")] == unlisted
    assert reads[(A, "ix_absent")] == unlisted
    # No id: an index not valid (or not there yet) is still building.
    db = _Catalog(indexes={B: [("app", "ix_noid_valid", True), ("app", "ix_noid", False)]})
    reads = _states(db, _build("ix_noid_valid", table=B, job_id=None),
                    _build("ix_noid", table=B, job_id=None),
                    _build("ix_never", table=B, job_id=None))
    assert reads == {(B, "ix_noid_valid"): IndexBuildRead("completed"),
                     (B, "ix_noid"): IndexBuildRead("building"),
                     (B, "ix_never"): IndexBuildRead("building")}
    # Without any id, sys.jobs is not read at all; one catalog read per relation.
    assert [s for s, _p in db.statements if "sys.jobs" in s] == []
    assert len(db.statements) == 1


def test_a_40001_or_an_xx000_is_read_again_on_the_same_connection() -> None:
    db = _Catalog(jobs={"j-1": ("completed", None)},
                  errors=[_Err(_OC001, "40001"), _Err("server unavailable", "XX000")])
    assert _states(db, _build("ix_a"))[(A, "ix_a")] == IndexBuildRead("completed")
    assert len(db.statements) == 3 and db.connects == 1


def test_an_unreadable_relation_or_no_connection_leaves_its_builds_unknown() -> None:
    db = _Catalog(indexes={B: [("app", "ix_b", True)]}, unreadable={A})
    reads = _states(db, _build("ix_a", job_id=None), _build("ix_b", table=B, job_id=None))
    assert reads == {(A, "ix_a"): None, (B, "ix_b"): IndexBuildRead("completed")}
    down = _Catalog(down=True)
    assert _states(down, _build("ix_a")) == {(A, "ix_a"): None}
    assert _states(_Catalog(), ) == {}


def test_an_ambiguous_bare_name_is_unknown() -> None:
    db = _Catalog(indexes={"orders": [("s1", "ix", True), ("s2", "ix", False)]})
    assert _states(db, _build("ix", table="orders", job_id=None)) == {("orders", "ix"): None}


def test_an_unreadable_sys_jobs_trusts_only_a_valid_index() -> None:
    db = _Catalog(indexes={A: [("app", "ix_done", True), ("app", "ix_running", False)]},
                  errors=[_Err("permission denied for view jobs", "42501")])
    reads = _states(db, _build("ix_done", job_id="j-1"), _build("ix_running", job_id="j-2"))
    # The job of the invalid one may still run: unknown, never "failed".
    assert reads == {(A, "ix_done"): IndexBuildRead("completed"), (A, "ix_running"): None}


def test_no_code_path_calls_wait_for_job() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
    for path in root.rglob("*.py"):
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if "wait_for_job" in stripped:
                # Only prose (a comment or a docstring) may name it, to forbid it.
                assert "never" in stripped or "blocks" in stripped, (path, stripped)
                assert not re.search(r"CALL\s+sys\.wait_for_job\s*\(", stripped), (path, stripped)


# ---------------------------------------------------------------------------
# 5. The fold
# ---------------------------------------------------------------------------


def test_the_fold_counts_completed_drops_them_and_keeps_failures_value_free() -> None:
    record = _record(_build("ix_a"), _build("ux_email", unique=True), _build("ix_c"),
                     completed=2)
    now = T0 + timedelta(minutes=8)
    folded, failed, finished = ib.fold_index_build_reads(record, {
        (A, "ix_a"): IndexBuildRead("completed"),
        (A, "ux_email"): IndexBuildRead("failed", _FAILED_UNIQUE_DETAILS + "\nDETAIL: more"),
        (A, "ix_c"): None,
    }, now)
    assert [b.index for b in folded.building] == ["ix_c"]
    assert (folded.completed, folded.failed, folded.submitted) == (3, 1, 5)
    assert folded.completed_by_table == {A: 1}  # counted by table (a recreate voids them)
    (failure,) = failed
    assert folded.failures == [failure] and failure.unique
    assert failure.details == 'could not create unique index "ux_email"'  # no key value
    assert folded.checked_at == now and folded.finished_at is None and not finished
    assert record.building and len(record.building) == 3  # pure: the input is unchanged
    # The last one settles: finished now.
    later = now + timedelta(minutes=1)
    done, failed, finished = ib.fold_index_build_reads(
        folded, {(A, "ix_c"): IndexBuildRead("completed")}, later)
    assert finished and done.finished_at == later and done.building == [] and failed == []
    assert done.submitted == done.completed + done.failed


def test_an_unlisted_job_reads_as_failed_only_once_it_can_have_aged_out() -> None:
    record = _record(_build("ux_email", unique=True))
    unlisted = {(A, "ux_email"): IndexBuildRead(
        "failed", INDEX_BUILD_UNLISTED_DETAILS, unlisted=True)}
    # Aurora DSQL keeps a finished job about 30 minutes: an id it does not list this soon
    # after the submission is unexplained, never a failure (its build stays watched).
    soon = record.submitted_at + timedelta(minutes=14, seconds=59)
    folded, failed, finished = ib.fold_index_build_reads(record, unlisted, soon)
    assert failed == [] and not finished
    assert [b.index for b in folded.building] == ["ux_email"] and folded.checked_at == soon
    later = record.submitted_at + timedelta(minutes=15)
    folded, failed, finished = ib.fold_index_build_reads(record, unlisted, later)
    assert [f.details for f in failed] == [INDEX_BUILD_UNLISTED_DETAILS] and finished
    assert folded.failed == 1 and folded.building == []
    # A failure sys.jobs itself reports is decided at once.
    listed = {(A, "ux_email"): IndexBuildRead("failed", "could not create unique index")}
    _record_now, failed, _finished = ib.fold_index_build_reads(record, listed, soon)
    assert len(failed) == 1


def test_a_fold_with_no_answer_changes_nothing() -> None:
    record = _record(_build("ix_a"))
    folded, failed, finished = ib.fold_index_build_reads(record, {(A, "ix_a"): None}, T0)
    assert folded is record and failed == [] and not finished
    folded, _f, _d = ib.fold_index_build_reads(
        record, {(A, "ix_a"): IndexBuildRead("building")}, T0 + timedelta(seconds=5))
    assert folded.building == record.building and folded.checked_at == T0 + timedelta(seconds=5)


# ---------------------------------------------------------------------------
# 6. The watch
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self):
        self.t = 0.0
        self.sleeps: list = []

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.t += seconds
        # A watch or a wait past every budget here would spin forever: fail instead.
        assert self.t < 10_000, "the poll loop did not stop at its budget"


class _Reads:
    """index_build_states as scripted: one {index: IndexBuildRead | None} per poll, then
    every build still building."""

    def __init__(self, *script, on_call=None):
        self.script = list(script)
        self.calls: list = []
        self.on_call = on_call

    def __call__(self, builds, *, connection_factory):
        self.calls.append([(b.table, b.index, b.job_id) for b in builds])
        if self.on_call is not None:
            self.on_call(len(self.calls))
        states = self.script.pop(0) if self.script else {}
        return {(b.table, b.index): states.get(b.index, IndexBuildRead("building"))
                for b in builds}


class _Beats:
    def __init__(self):
        self.beats = 0
        self.cancelled = False

    def heartbeat(self):
        self.beats += 1


def _fl_job(manager, record, status="DONE"):
    def work(handle):
        handle.update(lambda job: setattr(job, "index_builds", record))
        if status == "FAILED":
            raise RuntimeError("Full Load incomplete")

    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5)
    return job_id


def _watch(monkeypatch, record, reads, *, handle=None, refreshes=None, activity=None,
           budget=600.0):
    manager = JobManager(stall_timeout_seconds=None)
    job_id = _fl_job(manager, record)
    monkeypatch.setattr(ib, "index_build_states", reads)
    lines = activity if activity is not None else []
    monkeypatch.setattr(ib, "log_activity", lambda _cat, action, **kw: lines.append(
        (action, getattr(kw.get("status"), "value", None), kw.get("target"), kw.get("detail"))))
    clock = _Clock()
    beats = handle or _Beats()
    last = ib.run_index_build_watch(
        manager, job_id, connection_factory=lambda: None, handle=beats,
        refresh=(lambda: refreshes.append(1)) if refreshes is not None else None,
        budget_seconds=budget, sleep=clock.sleep, monotonic=clock.monotonic,
        now=lambda: T0 + timedelta(seconds=clock.t + 600),
    )
    return SimpleNamespace(manager=manager, job_id=job_id, clock=clock, beats=beats,
                           last=last, lines=lines, reads=reads)


def test_the_watch_backs_off_from_2_to_30_seconds_with_a_heartbeat_per_poll(monkeypatch):
    w = _watch(monkeypatch, _record(_build("ix_a")), _Reads())
    assert w.clock.sleeps[:6] == [2.0, 4.0, 8.0, 16.0, 30.0, 30.0]
    # Bounded: it stops at the budget, Aurora DSQL's build runs on regardless.
    assert w.clock.t >= 600.0 and w.clock.t - w.clock.sleeps[-1] < 600.0
    assert w.beats.beats == len(w.reads.calls) == len(w.clock.sleeps) + 1
    assert w.manager.get_status(w.job_id).index_builds.building  # still building
    assert w.lines == []  # nothing settled: no line


def test_the_watch_stops_when_cancelled_or_the_job_is_gone(monkeypatch):
    handle = _Beats()
    reads = _Reads(on_call=lambda n: setattr(handle, "cancelled", n >= 2))
    w = _watch(monkeypatch, _record(_build("ix_a")), reads, handle=handle)
    assert len(reads.calls) == 2 and w.clock.sleeps == [2.0]

    class _Gone:
        def get_status(self, job_id):
            raise JobNotFoundError(job_id)

    never = _Reads()
    monkeypatch.setattr(ib, "index_build_states", never)
    assert ib.run_index_build_watch(_Gone(), "pruned", connection_factory=lambda: None,
                                    sleep=lambda _s: None) is None
    assert never.calls == []


def test_the_watch_logs_one_line_per_failed_build_and_one_at_the_end(monkeypatch):
    record = _record(_build("ux_email", unique=True, job_id="j-1"),
                     _build("ix_b", job_id="j-2"), completed=1)
    refreshes: list = []
    w = _watch(monkeypatch, record, _Reads(
        {"ux_email": IndexBuildRead("failed", _FAILED_UNIQUE_DETAILS)},
        {},
        {"ix_b": IndexBuildRead("completed")},
    ), refreshes=refreshes)
    stored = w.manager.get_status(w.job_id).index_builds
    assert stored.building == [] and (stored.completed, stored.failed) == (2, 1)
    assert stored.finished_at is not None and stored.checked_at == stored.finished_at
    (failure,) = stored.failures
    assert failure.details == 'could not create unique index "ux_email"'
    assert [line[:3] for line in w.lines] == [
        ("index build failed", "warning", f"{A}.ux_email"),
        ("index builds finished", "warning", None),
    ]
    # P4d: no "after the data loaded" -- a build submitted before the rows can fail while
    # they load.
    assert w.lines[0][3] == (
        "Aurora DSQL's build of the UNIQUE index failed: could not "
        'create unique index "ux_email". The loaded rows most likely hold a duplicate key '
        "for it, so the target does not enforce that constraint: remove the duplicates, "
        "drop the index if the table still lists it, and create it again before cutting "
        "over.")
    finished = T0 + timedelta(seconds=6 + 600)
    assert w.lines[1][3] == (
        f"Aurora DSQL built 2 secondary index(es); the last finished by "
        f"{finished:%H:%M} UTC, 10 min after the data loaded; 1 failed")
    assert len(refreshes) == 2  # a change each time something settled, not every poll
    assert w.clock.sleeps == [2.0, 4.0] and w.last == stored


def test_the_end_line_is_a_success_when_every_build_completed(monkeypatch):
    w = _watch(monkeypatch, _record(_build("ix_a")),
               _Reads({"ix_a": IndexBuildRead("completed")}))
    ((action, status, _target, detail),) = w.lines
    assert (action, status) == ("index builds finished", "success")
    assert detail.startswith("Aurora DSQL built 1 secondary index(es); the last finished by ")
    assert detail.endswith(" UTC, 10 min after the data loaded")


def test_a_failed_build_created_again_by_hand_reads_as_built(monkeypatch):
    record = _record(completed=1, failures=[
        _build("ux_email", unique=True, job_id="j-1", details="could not create unique index")])
    record = record.model_copy(update={"finished_at": T0 + timedelta(minutes=5)})
    refreshes: list = []
    reads = _Reads({"ux_email": IndexBuildRead("completed")})
    w = _watch(monkeypatch, record, reads, refreshes=refreshes)
    # Read once, by its index -- never by its old job id, whose row would still say failed --
    # and nothing builds, so the watch ends at once.
    assert reads.calls == [[(A, "ux_email", None)]] and w.clock.sleeps == []
    stored = w.manager.get_status(w.job_id).index_builds
    assert stored.failures == [] and (stored.completed, stored.failed) == (2, 0)
    assert stored.completed_by_table == {A: 1}
    assert stored.finished_at == stored.checked_at  # when it was seen built
    assert w.lines == [("index valid", "success", f"{A}.ux_email",
                        "The UNIQUE index whose build had failed is valid on Aurora DSQL now.")]
    assert refreshes == [1]
    assert _render(w.manager.get_status(w.job_id)).texts[0].startswith(
        "Secondary indexes built: all 2")


def test_a_failed_build_still_not_valid_stays_failed_after_one_read(monkeypatch):
    record = _record(failures=[_build("ix_b", details="index row size exceeds the maximum")])
    reads = _Reads()  # every build: not valid
    w = _watch(monkeypatch, record, reads)
    assert len(reads.calls) == 1 and w.clock.sleeps == []
    stored = w.manager.get_status(w.job_id).index_builds
    assert [f.index for f in stored.failures] == ["ix_b"] and stored.failed == 1
    assert stored.failures[0].details == "index row size exceeds the maximum"
    assert w.lines == []


def test_a_failed_build_is_due_a_read_at_most_once_a_minute() -> None:
    manager = JobManager(stall_timeout_seconds=None)
    record = _record(failures=[_build("ix_b", details="x")]).model_copy(
        update={"checked_at": T0})
    job = MigrationJob(job_id="j", status="DONE", index_builds=record)
    assert not ib.index_build_watch_due(job, manager, now=T0 + timedelta(seconds=59))
    assert ib.index_build_watch_due(job, manager, now=T0 + timedelta(seconds=60))
    never_read = record.model_copy(update={"checked_at": None})
    assert ib.index_build_watch_due(job.model_copy(update={"index_builds": never_read}),
                                    manager, now=T0)
    # The 30 s re-render stays for builds still running only.
    assert not ib.index_builds_pending(job)


def test_one_live_watch_per_job_and_a_rest_after_it_ended(monkeypatch):
    manager = JobManager(stall_timeout_seconds=None)
    job_id = _fl_job(manager, _record(_build("ix_a")))
    job = manager.get_status(job_id)
    release, started = threading.Event(), threading.Event()

    def _blocking(_jm, fl_job_id, **_kw):
        assert fl_job_id == job_id
        started.set()
        release.wait(5)

    monkeypatch.setattr(ib, "run_index_build_watch", _blocking)
    clock = _Clock()

    def start():
        return ib.start_index_build_watch(manager, job, connection_factory=lambda: None,
                                          monotonic=clock.monotonic)

    first = start()
    assert first and started.wait(5)
    assert start() is None  # one live watch per job
    assert not ib.index_build_watch_due(job, manager, monotonic=clock.monotonic)
    release.set()
    assert manager.wait(first, timeout=5)
    assert not ib.index_build_watch_due(job, manager, monotonic=clock.monotonic)
    clock.t = 29.9
    assert not ib.index_build_watch_due(job, manager, monotonic=clock.monotonic)
    clock.t = 30.0
    assert ib.index_build_watch_due(job, manager, monotonic=clock.monotonic)
    second = start()
    assert second and second != first
    assert manager.wait(second, timeout=5)


def test_a_render_meanwhile_waits_for_the_submission_and_sees_the_live_watch(monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(ib, "run_index_build_watch", lambda *_a, **_k: release.wait(5))
    job = MigrationJob(job_id="j", status="DONE", index_builds=_record(_build("ix")))
    answers: list = []

    class _Manager(JobManager):
        def submit(self, work, **kw):
            # Another render's due check while this one submits: it waits on the registry.
            self.other = threading.Thread(
                target=lambda: answers.append(ib.index_build_watch_due(job, self)))
            self.other.start()
            self.other.join(0.2)
            answers.append("waited" if self.other.is_alive() else "ran")
            return super().submit(work, **kw)

    manager = _Manager(stall_timeout_seconds=None)
    watch = ib.start_index_build_watch(manager, job, connection_factory=lambda: None)
    manager.other.join(5)
    assert watch and answers == ["waited", False]  # it then saw the live watch: no second
    release.set()
    assert manager.wait(watch, timeout=5)


def test_a_watch_that_could_not_start_rests_like_one_that_ended() -> None:
    class _Broken(JobManager):
        def submit(self, *_a, **_k):
            raise RuntimeError("cannot start a thread")

    manager = _Broken(stall_timeout_seconds=None)
    job = MigrationJob(job_id="j", status="DONE", index_builds=_record(_build("ix")))
    clock = _Clock()
    assert ib.start_index_build_watch(manager, job, connection_factory=lambda: None,
                                      monotonic=clock.monotonic) is None
    assert not ib.index_build_watch_due(job, manager, monotonic=clock.monotonic)
    clock.t = 30.0
    assert ib.index_build_watch_due(job, manager, monotonic=clock.monotonic)


def test_a_watch_is_due_only_for_a_finished_load_with_a_pending_build() -> None:
    manager = JobManager(stall_timeout_seconds=None)
    building = MigrationJob(job_id="j", status="DONE", index_builds=_record(_build("ix")))
    assert ib.index_build_watch_due(building, manager)
    for status in ("FAILED", "CANCELLED"):
        assert ib.index_build_watch_due(building.model_copy(update={"status": status}), manager)
    assert not ib.index_build_watch_due(building.model_copy(update={"status": "RUNNING"}),
                                        manager)
    done = building.model_copy(update={"index_builds": _record(completed=1)})
    assert not ib.index_build_watch_due(done, manager)
    assert not ib.index_build_watch_due(MigrationJob(job_id="j", status="DONE"), manager)
    assert not ib.index_build_watch_due(building, None)
    assert ib.start_index_build_watch(manager, building, connection_factory=None) is None


def test_id_less_builds_are_given_up_six_hours_after_their_submission() -> None:
    manager = JobManager(stall_timeout_seconds=None)
    record = _record(_build("ix", job_id=None))
    job = MigrationJob(job_id="j", status="DONE", index_builds=record)
    submitted = record.submitted_at
    assert ib.index_build_watch_due(job, manager, now=submitted + timedelta(hours=5, minutes=59))
    assert not ib.index_builds_pending(job, now=submitted + timedelta(hours=6, seconds=1))
    assert not ib.index_build_watch_due(job, manager, now=submitted + timedelta(hours=7))
    # A build with a job id is never given up (Aurora DSQL lists it until it ends).
    with_id = job.model_copy(update={"index_builds": _record(_build("ix", job_id=None),
                                                             _build("ix2", job_id="j-9"))})
    assert ib.index_build_watch_due(with_id, manager, now=submitted + timedelta(days=2))


# ---------------------------------------------------------------------------
# 7. The notice
# ---------------------------------------------------------------------------

_GLYPHS = ("✓", "⚠", "ℹ", "…", "—")  # check, warning, info, ..., em dash


def _render(job, *, now=None):
    ui = _RecordingUi()
    ib.render_index_builds(ui, job, now=now)
    for text in ui.texts:
        assert not any(glyph in text for glyph in _GLYPHS), text
    return ui


def _tone_of(ui):
    blob = " ".join(ui.classes)
    (tone,) = [tone for tone, style in NOTICE_STYLE.items() if style[0] in blob]
    return tone


def _job(record, status="DONE"):
    return MigrationJob(job_id="job-1", status=status, index_builds=record)


def test_the_notice_while_aurora_dsql_builds() -> None:
    ui = _render(_job(_record(_build("ix_a"), _build("ix_b"), completed=1)))
    assert _tone_of(ui) == "info" and ui.spinner_colors == ["primary"]  # live
    assert ui.texts[0] == "Aurora DSQL is building secondary indexes: 1 of 3 built"
    assert ui.texts[2] == (
        "Every table's data loaded at 03:00 UTC. Aurora DSQL builds the indexes in the "
        "background; queries that need one are slower until it is built. You can continue "
        "with the next step meanwhile.")


def test_the_notice_once_every_index_is_built() -> None:
    record = _record(completed=3).model_copy(update={"finished_at": T0 + timedelta(minutes=12)})
    ui = _render(_job(record))
    assert _tone_of(ui) == "success" and NOTICE_STYLE["success"][3] in ui.icons
    assert ui.texts == [
        "Secondary indexes built: all 3, the last by 03:12 UTC (12 min after the data loaded)"]
    quick = record.model_copy(update={"finished_at": T0 + timedelta(seconds=40)})
    assert "(less than a minute after the data loaded)" in _render(_job(quick)).texts[0]


def test_a_failed_unique_build_is_a_missing_constraint() -> None:
    record = _record(completed=2, failures=[_build(
        "ux_email", unique=True, details='could not create unique index "ux_email"')])
    ui = _render(_job(record))
    assert _tone_of(ui) == "error" and NOTICE_STYLE["error"][3] in ui.icons
    assert ui.texts[0] == "UNIQUE index build failed: a constraint is missing (1 of 3 failed)"
    assert ui.texts[1] == (
        "The data loaded, but Aurora DSQL could not build this index. "
        f'{A}.ux_email (UNIQUE): could not create unique index "ux_email". The loaded rows '
        "most likely hold a duplicate key for the UNIQUE index, so the target does not "
        "enforce that constraint: remove the duplicates, drop the index if the table still "
        "lists it, and create it again before cutting over. Once an index is valid on Aurora "
        "DSQL, this step shows it as built.")


def test_failed_builds_beside_running_ones_name_both_counts_and_cap_the_list() -> None:
    failures = [_build(f"ix_{i:02d}", details="index row size exceeds the maximum")
                for i in range(10)]
    ui = _render(_job(_record(_build("ix_running"), failures=failures)))
    assert _tone_of(ui) == "warning"
    assert ui.texts[0] == "Secondary index builds failed (10 of 11, 1 still building)"
    assert f"{A}.ix_07: index row size exceeds the maximum; and 2 more." in ui.texts[1]
    assert f"{A}.ix_08" not in ui.texts[1]
    assert ui.texts[1].endswith(
        "Fix the cause Aurora DSQL reports, drop each index the table still lists, and "
        "create it again. Once an index is valid on Aurora DSQL, this step shows it as built.")


def test_a_mixed_failure_names_the_unique_one_and_gives_both_remedies() -> None:
    record = _record(_build("ix_run"), completed=5, failures=[
        _build("ix_size", details="index row size exceeds the maximum"),
        _build("ux_email", unique=True, details='could not create unique index "ux_email"')])
    ui = _render(_job(record))
    assert _tone_of(ui) == "error"
    assert ui.texts[0] == (
        "UNIQUE index build failed: a constraint is missing (2 of 8 failed, 1 still building)")
    body = ui.texts[1]
    assert f"{A}.ix_size: index row size exceeds the maximum; " in body
    assert f'{A}.ux_email (UNIQUE): could not create unique index "ux_email". ' in body
    assert "hold a duplicate key for the UNIQUE index" in body
    assert "For the other one, fix the cause Aurora DSQL reports" in body
    two = _record(failures=[_build("ux_a", unique=True), _build("ux_b", unique=True)])
    ui = _render(_job(two))
    assert ui.texts[0] == "UNIQUE index builds failed: 2 constraints are missing (2 of 2 failed)"
    assert "duplicate keys for the UNIQUE indexes" in ui.texts[1]
    assert "For the other" not in ui.texts[1]


def test_id_less_builds_past_the_give_up_are_named_not_confirmed() -> None:
    record = _record(_build("ux_x", job_id=None, unique=True), completed=1)
    later = record.submitted_at + timedelta(hours=7)
    ui = _render(_job(record), now=later)
    assert _tone_of(ui) == "warning" and ui.spinner_colors == []
    assert ui.texts[0] == "Secondary index builds not confirmed: 1 of 2 built"
    assert ui.texts[1] == (
        f"Not confirmed: {A}.ux_x. Aurora DSQL answered no job id for this build, and its "
        "index was still not valid 6 hours after the submission, so the tool no longer "
        "checks: check the table's indexes on Aurora DSQL; a UNIQUE one is a missing "
        "constraint until it is valid.")
    # Beside a build still watched, the same sentence closes the live notice.
    mixed = _render(_job(_record(_build("ix_x", job_id=None), _build("ix_y"))), now=later)
    assert _tone_of(mixed) == "info" and mixed.texts[2].endswith(
        "check the table's indexes on Aurora DSQL.")
    # Before the give-up an id-less build is just building.
    assert "Not confirmed" not in " ".join(_render(_job(record), now=record.submitted_at).texts)


def test_no_record_renders_nothing() -> None:
    assert _render(_job(None)).texts == []
    assert _render(_job(IndexBuilds())).texts == []


# ---------------------------------------------------------------------------
# 8. The Data Migration step
# ---------------------------------------------------------------------------


def test_the_progress_section_shows_the_builds_after_the_index_notice(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _full_load_ui as fl
    from dsql_migrator.ui.data_migration._models import build_full_load_table_rows

    from tests.test_p4a_deferred_indexes import _chunk, _ui_job

    headers: list = []

    def _notice(ui, **kw):
        headers.append(kw["header"])

    monkeypatch.setattr(fl, "render_notice", _notice)
    monkeypatch.setattr(ib, "render_notice", _notice)

    class _Ui(g137._RecordingUi):
        def linear_progress(self, *a, **k):
            return self._El(self)

    job = _ui_job("DONE", [_chunk(A), _chunk(B)])
    job.index_builds = _record(_build("ix_a"), completed=1)
    records = [(B, "index not created: ix_b: too many indexes")]
    fl._render_full_load_progress(_Ui(), job, build_full_load_table_rows(job),
                                  quarantine_records=records)
    index_notice = next(i for i, h in enumerate(headers) if h.startswith("Indexes not created"))
    assert headers[index_notice + 1] == (
        "Aurora DSQL is building secondary indexes: 1 of 2 built")
    # While the run runs, the record is the pass's: no notice yet.
    headers.clear()
    job.status = "RUNNING"
    fl._render_full_load_progress(_Ui(), job, build_full_load_table_rows(job))
    assert not [h for h in headers if "secondary indexes" in h.lower()]


def _render_step(monkeypatch, job, watch):
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_g12_table_set_after_load import _Jobs, _VerifiedSession, _flo
    from tests.test_ui_data_migration import _ConfirmDialogUi

    timers: list = []

    class _Ui(_ConfirmDialogUi):
        def refreshable(self, fn):
            wrapper = super().refreshable(fn)
            wrapper.refresh = lambda *_a, **_k: None
            wrapper.refresh.region = fn.__name__  # which region a timer re-renders
            return wrapper

        def timer(self, interval, callback=None, *a, **k):
            timers.append((interval, getattr(callback, "region", None)))
            return _ConfirmDialogUi._El(self)

    monkeypatch.setattr(fl, "_render_full_load_progress", lambda *a, **k: None)
    monkeypatch.setattr(fl, "full_load_error_records_for", lambda _log, _job: [])
    noop = lambda *_a, **_k: None  # noqa: E731
    names = [c.chunk_id for c in job.chunks]
    dm._render_full_load_step(
        _Ui(), _flo(names), _Jobs({"job-1": job}), _VerifiedSession(), job=job,
        status=engine.StepStatus.DONE, selected_names=names, guard_reason=None,
        start_full_load=noop, retry_failed_load=noop, reload_table=noop,
        accept_quarantine_and_continue=noop, stop_full_load=noop, refresh=noop,
        retry_tables=noop, schema_recreate_candidates=lambda: [],
        watch_index_builds=watch,
    )
    return timers


def test_the_live_region_starts_the_watch_and_rereads_while_a_build_is_pending(
        monkeypatch) -> None:
    from tests.test_p4a_deferred_indexes import _chunk, _ui_job

    job = _ui_job("DONE", [_chunk(A)])
    job.index_builds = _record(_build("ix_a"))
    started: list = []
    timers = _render_step(monkeypatch, job, lambda j: started.append(j.job_id))
    assert started == ["job-1"]
    # The live region re-renders now and then, so a watch that ran out of time restarts.
    assert timers == [(30.0, "_live_detail")]
    # Every build settled: no re-render; the step still offers the watch (its own due check
    # says no -- or, with a failed build, reads it again at most once a minute).
    started.clear()
    job.index_builds = _record(completed=1)
    assert _render_step(monkeypatch, job, lambda j: started.append(j.job_id)) == []
    assert started == ["job-1"]
    assert not ib.index_build_watch_due(job, JobManager(stall_timeout_seconds=None))
    # A job with no record is offered nothing.
    started.clear()
    job.index_builds = None
    assert _render_step(monkeypatch, job, lambda j: started.append(j.job_id)) == []
    assert started == []


def test_the_screen_wires_the_watch_and_the_retry_carry() -> None:
    import inspect

    import dsql_migrator.ui.data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    assert "watch_index_builds=watch_index_builds," in src
    assert "prior_index_builds=prior_index_builds," in src
    watch = src[src.index("def watch_index_builds("):]
    watch = watch[:watch.index("def _fk_revalidating(")]
    assert "index_build_watch_due(job, job_manager)" in watch
    assert "connection_factory=_fk_target_connect()" in watch and "refresh=refresh" in watch


# ---------------------------------------------------------------------------
# 9. The perf harness: index-ready and end-to-end
# ---------------------------------------------------------------------------


def test_the_flag_parses(mp, monkeypatch) -> None:  # noqa: F811 - the fixture
    captured: dict = {}
    monkeypatch.setattr(mp, "cmd_full_load", lambda args: captured.setdefault("args", args) and 0)
    monkeypatch.setenv("DB_HOST", "db.invalid")
    monkeypatch.setenv("DB_PASSWORD", "unused-by-this-test")
    for argv, expected in (([], 1800.0), (["--wait-index-builds", "300"], 300.0),
                           (["--wait-index-builds", "0"], 0.0)):
        captured.clear()
        monkeypatch.setattr("sys.argv", ["measure_performance.py", "full-load", *argv])
        mp.main()
        assert captured["args"].wait_index_builds == expected
        assert captured["args"].wait_sys_jobs is None  # the start gate is unchanged


def test_the_harness_waits_for_the_builds_and_reports_index_ready_and_end_to_end(
        mp, monkeypatch) -> None:  # noqa: F811
    lines: list = []
    monkeypatch.setattr(mp, "log", lines.append)
    clock = _Clock()
    reads = _Reads({"ix_a": IndexBuildRead("completed")}, {},
                   {"ix_b": IndexBuildRead("failed", _FAILED_UNIQUE_DETAILS)})
    record = _record(_build("ix_a"), _build("ix_b"))
    done, waited, errors = mp._wait_for_index_builds(
        lambda: None, record, 600.0, sleep=clock.sleep, clock=clock.monotonic,
        now=lambda: T0 + timedelta(seconds=300 + clock.t), read=reads)
    assert clock.sleeps == [15.0, 15.0] and (waited, errors) == (30.0, 0)
    assert done.building == [] and (done.completed, done.failed) == (1, 1)
    started_at = (T0 - timedelta(seconds=120)).timestamp()
    builds, end_to_end = mp._index_builds_report(done, waited=waited, read_errors=errors,
                                                 started_at=started_at, elapsed=125.0)
    assert builds == {
        "submitted": 2, "completed": 1, "failed": 1, "still_building": 0,
        "data_loaded_at": "2026-10-25T03:00:00.000+00:00",
        "submitted_at": "2026-10-25T03:00:04.000+00:00",
        "finished_at": "2026-10-25T03:05:30.000+00:00",
        "seconds_after_load": 330.0, "waited_seconds": 30.0, "read_errors": 0,
    }
    assert end_to_end == 450.0  # from the run's start, not from its end
    text = json.dumps(builds)
    assert _KEY_VALUE not in text and "ix_b" not in text  # counts and times only


def test_the_wait_is_bounded_and_zero_means_no_read(mp, monkeypatch) -> None:  # noqa: F811
    monkeypatch.setattr(mp, "log", lambda _line: None)
    clock = _Clock()
    reads = _Reads()
    record = _record(_build("ix_a"))
    still, waited, _errors = mp._wait_for_index_builds(
        lambda: None, record, 40.0, sleep=clock.sleep, clock=clock.monotonic, read=reads)
    assert clock.sleeps == [15.0, 15.0, 10.0] and waited == 40.0 and len(reads.calls) == 4
    builds, end_to_end = mp._index_builds_report(still, waited=waited, read_errors=0,
                                                 started_at=T0.timestamp(), elapsed=80.0)
    assert builds["still_building"] == 1 and builds["finished_at"] is None
    assert builds["seconds_after_load"] is None and end_to_end is None
    none = _Reads()
    assert mp._wait_for_index_builds(lambda: None, record, 0.0, read=none) == (record, 0.0, 0)
    assert none.calls == []
    # Reads that answer nothing are counted and tried again at the next poll.
    clock = _Clock()
    _still, _waited, errors = mp._wait_for_index_builds(
        lambda: None, record, 30.0, sleep=clock.sleep, clock=clock.monotonic,
        read=lambda builds, connection_factory: {(b.table, b.index): None for b in builds})
    assert errors == 3
    # A run that submitted no build: nothing to wait for, the end to end is its wall time.
    assert mp._index_builds_report(None, waited=0.0, read_errors=0, started_at=0.0,
                                   elapsed=12.345) == (None, 12.35)


def test_the_command_reports_the_builds_it_waited_for(
        mp, monkeypatch, tmp_path) -> None:  # noqa: F811
    import dsql_migrator.core.target_introspector as ti
    import dsql_migrator.ui.data_migration as data_migration

    args, seen, report_path = _command_doubles(mp, monkeypatch, tmp_path)
    inner = data_migration.run_full_load

    def _run_with_builds(handle, tables, **kw):
        inner(handle, tables, **kw)
        handle.update(lambda job: setattr(job, "index_builds", _record(
            _build("ux_email", table="perf.t1", unique=True),
            at=datetime.now(timezone.utc))))

    monkeypatch.setattr(data_migration, "run_full_load", _run_with_builds)
    monkeypatch.setattr(mp, "_target_conn", lambda: None)
    monkeypatch.setattr(ti, "index_build_states", lambda builds, connection_factory: {
        (b.table, b.index): IndexBuildRead("completed") for b in builds})
    args.wait_index_builds = 60.0
    assert mp.cmd_full_load(args) == 0
    report = json.loads(report_path.read_text())
    assert report["index_builds"]["completed"] == 1
    assert report["index_builds"]["still_building"] == 0
    assert report["end_to_end_seconds"] is not None and report["end_to_end_seconds"] >= 0
    assert any(line.startswith("index builds: 1 submitted, 1 built, 0 failed, 0 still "
                               "building | ready ") for line in seen["log"]), seen["log"]
    # --wait-index-builds 0: reported as the run left them, still building.
    args.wait_index_builds = 0.0
    assert mp.cmd_full_load(args) == 0
    report = json.loads(report_path.read_text())
    assert report["index_builds"]["still_building"] == 1
    assert report["end_to_end_seconds"] is None


# ---------------------------------------------------------------------------
# 10. Property 7: names, counts and times only
# ---------------------------------------------------------------------------


def test_no_key_value_reaches_a_line_the_record_or_the_report(
        monkeypatch, mp) -> None:  # noqa: F811
    record = _record(_build("ux_email", unique=True))
    activity: list = []
    w = _watch(monkeypatch, record, _Reads(
        {"ux_email": IndexBuildRead("failed", _FAILED_UNIQUE_DETAILS)}), activity=activity)
    stored = w.manager.get_status(w.job_id)
    assert _KEY_VALUE not in stored.model_dump_json()
    assert all(_KEY_VALUE not in str(line) for line in activity)
    ui = _render(stored)
    assert all(_KEY_VALUE not in text for text in ui.texts)
    builds, _ = mp._index_builds_report(stored.index_builds, waited=0.0, read_errors=0,
                                        started_at=0.0, elapsed=1.0)
    assert _KEY_VALUE not in json.dumps(builds)
