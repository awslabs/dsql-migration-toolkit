# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the background :class:`JobManager` (Requirement 9.3).

Covers:
- ``submit`` returns a ``job_id`` immediately without waiting for the work
  (non-blocking background execution);
- background work records progress and the job reaches ``DONE``;
- an unhandled exception transitions the job to ``FAILED`` and is captured
  (``error_count`` and a human-readable message);
- status lookup by ``job_id`` returns an independent, consistent snapshot;
- unknown / duplicate ``job_id`` handling and ``list_jobs`` / ``wait``;
- thread-safety: concurrent progress updates while polling never corrupt or
  crash the status read;
- credential safety: the manager exposes only the worker's (redacted) error and
  never introduces a plaintext secret into job state (Property 7).
"""

from __future__ import annotations

import threading

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.job_manager import JobManager, JobNotFoundError
from dsql_migrator.core.models import ChunkState, MigrationJob


def _new_manager() -> JobManager:
    return JobManager()


def test_submit_returns_job_id_immediately_without_blocking() -> None:
    """``submit`` must return before the work completes (non-blocking)."""
    release = threading.Event()

    def work(_handle) -> None:  # noqa: ANN001 - JobHandle, not needed here
        release.wait(timeout=5.0)

    manager = _new_manager()
    job_id = manager.submit(work)

    # If submit blocked on the work, we would deadlock before this line because
    # the work waits on an event we only set below.
    assert isinstance(job_id, str) and job_id
    status = manager.get_status(job_id)
    assert status.status in {"PENDING", "RUNNING"}

    release.set()
    assert manager.wait(job_id, timeout=5.0) is True
    assert manager.get_status(job_id).status == "DONE"


def test_background_work_records_progress_and_reaches_done() -> None:
    """Progress recorded via the handle is visible and the job ends ``DONE``."""

    def work(handle) -> None:  # noqa: ANN001
        def advance(job: MigrationJob) -> None:
            job.chunks = [
                ChunkState(chunk_id="orders#0", status="DONE", rows_loaded=10),
                ChunkState(chunk_id="orders#1", status="DONE", rows_loaded=5),
            ]
            job.progress_pct = 100.0

        handle.update(advance)

    manager = _new_manager()
    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    status = manager.get_status(job_id)
    assert status.status == "DONE"
    assert status.progress_pct == 100.0
    assert [chunk.chunk_id for chunk in status.chunks] == ["orders#0", "orders#1"]
    assert manager.get_error(job_id) is None


def test_cooperative_cancel_marks_job_cancelled() -> None:
    """A worker that returns after a stop request ends as ``CANCELLED``."""
    started = threading.Event()
    release = threading.Event()
    saw_cancel: list[bool] = []

    def work(handle) -> None:  # noqa: ANN001
        started.set()
        release.wait(timeout=5.0)
        saw_cancel.append(handle.cancelled)  # the worker observes the request

    manager = _new_manager()
    job_id = manager.submit(work)
    assert started.wait(timeout=5.0)

    assert manager.request_cancel(job_id) is True
    release.set()
    assert manager.wait(job_id, timeout=5.0) is True

    assert saw_cancel == [True]
    status = manager.get_status(job_id)
    assert status.status == "CANCELLED"
    # A user-facing stop message is captured for the UI.
    assert "Stopped by user" in (manager.get_error(job_id) or "")


def test_request_cancel_is_false_for_unknown_or_terminal_job() -> None:
    manager = _new_manager()
    assert manager.request_cancel("does-not-exist") is False

    job_id = manager.submit(lambda _handle: None)
    assert manager.wait(job_id, timeout=5.0) is True
    # Already DONE -> cannot be cancelled.
    assert manager.request_cancel(job_id) is False
    assert manager.get_status(job_id).status == "DONE"


def test_failure_sets_failed_status_and_captures_error() -> None:
    """An unhandled exception marks the job ``FAILED`` and is captured."""

    def work(_handle) -> None:  # noqa: ANN001
        raise RuntimeError("introspection failed")

    manager = _new_manager()
    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    status = manager.get_status(job_id)
    assert status.status == "FAILED"
    assert status.error_count >= 1
    error = manager.get_error(job_id)
    assert error is not None
    assert "introspection failed" in error


def test_failure_preserves_worker_recorded_error_count() -> None:
    """A worker-recorded ``error_count`` is not clobbered on failure."""

    def work(handle) -> None:  # noqa: ANN001
        handle.update(lambda job: setattr(job, "error_count", 3))
        raise RuntimeError("partial failure")

    manager = _new_manager()
    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    status = manager.get_status(job_id)
    assert status.status == "FAILED"
    assert status.error_count == 3


def test_get_status_returns_independent_snapshot() -> None:
    """A returned snapshot is a copy: mutating it never affects the store."""

    def work(handle) -> None:  # noqa: ANN001
        handle.update(lambda job: setattr(job, "progress_pct", 42.0))

    manager = _new_manager()
    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    snapshot = manager.get_status(job_id)
    snapshot.progress_pct = 0.0
    snapshot.chunks.append(ChunkState(chunk_id="injected"))

    fresh = manager.get_status(job_id)
    assert fresh.progress_pct == 42.0
    assert fresh.chunks == []


def test_get_status_unknown_job_raises() -> None:
    manager = _new_manager()
    with pytest.raises(JobNotFoundError):
        manager.get_status("does-not-exist")


def test_get_error_unknown_job_raises() -> None:
    manager = _new_manager()
    with pytest.raises(JobNotFoundError):
        manager.get_error("does-not-exist")


def test_wait_unknown_job_raises() -> None:
    manager = _new_manager()
    with pytest.raises(JobNotFoundError):
        manager.wait("does-not-exist")


def test_submit_with_explicit_job_id_is_used() -> None:
    manager = _new_manager()
    job_id = manager.submit(lambda _handle: None, job_id="job-123")
    assert job_id == "job-123"
    assert manager.wait("job-123", timeout=5.0) is True
    assert manager.get_status("job-123").job_id == "job-123"


def test_submit_duplicate_job_id_raises() -> None:
    release = threading.Event()
    manager = _new_manager()
    manager.submit(lambda _handle: release.wait(timeout=5.0), job_id="dup")
    try:
        with pytest.raises(ValueError):
            manager.submit(lambda _handle: None, job_id="dup")
    finally:
        release.set()
        manager.wait("dup", timeout=5.0)


def test_list_jobs_returns_all_jobs() -> None:
    manager = _new_manager()
    first = manager.submit(lambda _handle: None)
    second = manager.submit(lambda _handle: None)
    assert manager.wait(first, timeout=5.0) is True
    assert manager.wait(second, timeout=5.0) is True

    job_ids = {job.job_id for job in manager.list_jobs()}
    assert job_ids == {first, second}


def test_wait_times_out_while_job_running() -> None:
    """``wait`` returns ``False`` when the worker is still running."""
    release = threading.Event()
    manager = _new_manager()
    job_id = manager.submit(lambda _handle: release.wait(timeout=5.0))
    try:
        assert manager.wait(job_id, timeout=0.05) is False
    finally:
        release.set()
        assert manager.wait(job_id, timeout=5.0) is True


def test_concurrent_updates_are_thread_safe_under_polling() -> None:
    """Concurrent progress updates and status polling never corrupt state."""
    total = 200

    def work(handle) -> None:  # noqa: ANN001
        for index in range(total):
            handle.update(
                lambda job, i=index: job.chunks.append(
                    ChunkState(chunk_id=f"chunk-{i}", status="DONE")
                )
            )

    manager = _new_manager()
    job_id = manager.submit(work)

    # Poll aggressively from the main thread while the worker mutates the job.
    # A non-thread-safe read would raise (e.g. list mutated during iteration).
    poll_errors: list[Exception] = []
    for _ in range(500):
        try:
            snapshot = manager.get_status(job_id)
            assert len(snapshot.chunks) <= total
        except Exception as exc:  # noqa: BLE001 - record any read error
            poll_errors.append(exc)

    assert manager.wait(job_id, timeout=5.0) is True
    assert poll_errors == []

    final = manager.get_status(job_id)
    assert final.status == "DONE"
    assert len(final.chunks) == total


def test_manager_does_not_leak_credentials_in_job_state() -> None:
    """The manager exposes only the worker's redacted error (Property 7)."""
    secret_value = "super-secret-token"

    def work(_handle) -> None:  # noqa: ANN001
        secret = SecretValue(secret_value)
        # A well-behaved component redacts its own secret before raising.
        message = f"connect failed using token {secret.reveal()}".replace(
            secret.reveal(), "***"
        )
        raise RuntimeError(message)

    manager = _new_manager()
    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=5.0) is True

    error = manager.get_error(job_id)
    assert error is not None
    assert secret_value not in error
    assert "***" in error

    dumped = manager.get_status(job_id).model_dump_json()
    assert secret_value not in dumped


# ---------------------------------------------------------------------------
# Durable job state: persistence + restore on restart (resumability, Property 4)
# ---------------------------------------------------------------------------


def test_sqlite_job_store_round_trips_a_job(tmp_path) -> None:  # noqa: ANN001
    from dsql_migrator.core.job_store import SqliteJobStore

    store = SqliteJobStore(str(tmp_path / "jobs.sqlite"))
    job = MigrationJob(
        job_id="J1",
        status="DONE",
        chunks=[ChunkState(chunk_id="t1", status="DONE", rows_loaded=42, attempts=1)],
    )
    store.save(job, error=None)

    # A fresh store over the same file sees the persisted snapshot.
    reopened = SqliteJobStore(str(tmp_path / "jobs.sqlite"))
    persisted = reopened.load_all()
    assert len(persisted) == 1
    assert persisted[0].job.job_id == "J1"
    assert persisted[0].job.chunks[0].rows_loaded == 42

    reopened.delete("J1")
    assert reopened.load_all() == []


def test_job_manager_persists_job_lifecycle_to_store() -> None:
    from dsql_migrator.core.job_store import InMemoryJobStore

    store = InMemoryJobStore()
    manager = JobManager(store=store)
    job_id = manager.submit(lambda handle: None)
    assert manager.wait(job_id, timeout=5.0)

    persisted = {p.job.job_id: p.job for p in store.load_all()}
    assert job_id in persisted
    assert persisted[job_id].status == "DONE"


def test_job_manager_restore_reconciles_interrupted_run() -> None:
    from dsql_migrator.core.job_store import InMemoryJobStore

    store = InMemoryJobStore()
    # A snapshot left RUNNING by a killed worker: one table done, one in flight.
    store.save(
        MigrationJob(
            job_id="J9",
            status="RUNNING",
            chunks=[
                ChunkState(chunk_id="done", status="DONE", rows_loaded=10, attempts=1),
                ChunkState(chunk_id="busy", status="IN_PROGRESS", attempts=1),
            ],
        ),
        error=None,
    )

    manager = JobManager(store=store)  # restores on construction
    job = manager.get_status("J9")

    # Interrupted job + its in-flight chunk are marked FAILED; the finished
    # chunk is preserved so a retry resumes only the unfinished work.
    assert job.status == "FAILED"
    states = {c.chunk_id: c.status for c in job.chunks}
    assert states == {"done": "DONE", "busy": "FAILED"}
    assert "Interrupted" in (manager.get_error("J9") or "")
    # The reconciled status is persisted back.
    assert {p.job.job_id: p.job.status for p in store.load_all()}["J9"] == "FAILED"


def test_is_interrupted_by_restart_marker() -> None:
    # The restart-interruption marker is detectable so the UI can tell it apart
    # from a real in-job failure (and re-check live AWS state instead of showing
    # a misleading "failed").
    from dsql_migrator.core.job_manager import (
        INTERRUPTED_BY_RESTART_MESSAGE,
        is_interrupted_by_restart,
    )

    assert is_interrupted_by_restart(INTERRUPTED_BY_RESTART_MESSAGE) is True
    # A reconciled interrupted job carries exactly this marker.
    from dsql_migrator.core.job_store import InMemoryJobStore

    store = InMemoryJobStore()
    store.save(MigrationJob(job_id="J1", status="RUNNING"), error=None)
    manager = JobManager(store=store)
    assert is_interrupted_by_restart(manager.get_error("J1")) is True

    # A genuine in-job error is NOT flagged as a restart interruption.
    assert is_interrupted_by_restart("OC000: target rejected the DDL") is False
    assert is_interrupted_by_restart(None) is False
    assert is_interrupted_by_restart("") is False


def test_job_manager_restore_keeps_terminal_jobs_unchanged() -> None:
    from dsql_migrator.core.job_store import InMemoryJobStore

    store = InMemoryJobStore()
    store.save(
        MigrationJob(
            job_id="done-job",
            status="DONE",
            chunks=[ChunkState(chunk_id="t", status="DONE", rows_loaded=5, attempts=1)],
        ),
        error=None,
    )

    manager = JobManager(store=store)
    job = manager.get_status("done-job")
    assert job.status == "DONE"  # terminal jobs are not reconciled
    assert job.chunks[0].rows_loaded == 5


def test_attach_store_restores_into_existing_manager() -> None:
    from dsql_migrator.core.job_store import InMemoryJobStore

    store = InMemoryJobStore()
    store.save(MigrationJob(job_id="X", status="DONE"), error=None)

    manager = JobManager()  # starts empty / in-memory
    manager.attach_store(store)

    assert manager.get_status("X").status == "DONE"


def test_sqlite_job_store_prune_keeps_newest_done_and_spares_active(tmp_path) -> None:  # noqa: ANN001
    from dsql_migrator.core.job_store import SqliteJobStore

    store = SqliteJobStore(str(tmp_path / "jobs.sqlite"))
    store.save(MigrationJob(job_id="d1", status="DONE"), None)
    store.save(MigrationJob(job_id="d2", status="DONE"), None)
    store.save(MigrationJob(job_id="f1", status="FAILED"), "boom")
    store.save(MigrationJob(job_id="r1", status="RUNNING"), None)

    deleted = store.prune_terminal(keep_most_recent=1)

    assert deleted == ["d1"]  # oldest DONE pruned; newest DONE (d2) kept
    remaining = {p.job.job_id for p in store.load_all()}
    assert remaining == {"d2", "f1", "r1"}  # FAILED/RUNNING never pruned


def test_job_manager_prune_terminal_drops_from_memory_and_store() -> None:
    from dsql_migrator.core.job_store import InMemoryJobStore

    store = InMemoryJobStore()
    manager = JobManager(store=store)
    first = manager.submit(lambda handle: None)
    assert manager.wait(first, timeout=5.0)
    second = manager.submit(lambda handle: None)
    assert manager.wait(second, timeout=5.0)

    pruned = manager.prune_terminal(keep_most_recent=1)
    assert pruned == 1
    remaining = {job.job_id for job in manager.list_jobs()}
    assert remaining == {second}  # newest DONE kept
    assert {p.job.job_id for p in store.load_all()} == {second}


# ---------------------------------------------------------------------------
# Stall watchdog: a RUNNING job that stops making progress is reaped to FAILED
# ---------------------------------------------------------------------------


class _FakeClock:
    """A manually advanced monotonic clock for deterministic watchdog tests."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_reap_stalled_jobs_fails_a_running_job_with_no_progress() -> None:
    clock = _FakeClock()
    # Watchdog thread disabled (timeout passed but we drive reap manually via a
    # short window); use a real timeout and advance the fake clock past it.
    manager = JobManager(stall_timeout_seconds=300.0, clock=clock)

    release = threading.Event()

    def work(handle) -> None:  # noqa: ANN001
        handle.update(lambda job: setattr(job, "progress_pct", 10.0))
        release.wait(timeout=5.0)  # simulate a wedged worker

    job_id = manager.submit(work)
    # Let the worker reach RUNNING and record its one progress update.
    deadline = clock.now
    for _ in range(100):
        if manager.get_status(job_id).status == "RUNNING":
            break
        threading.Event().wait(0.01)
    assert manager.get_status(job_id).status == "RUNNING"

    # Not yet stalled: within the window, reap is a no-op.
    clock.advance(299.0)
    assert manager.reap_stalled_jobs() == []
    assert manager.get_status(job_id).status == "RUNNING"

    # Past the window with no further progress -> reaped to FAILED.
    clock.advance(2.0)
    reaped = manager.reap_stalled_jobs()
    assert reaped == [job_id]
    status = manager.get_status(job_id)
    assert status.status == "FAILED"
    assert status.error_count >= 1
    assert "stalled" in (manager.get_error(job_id) or "").lower()

    release.set()
    manager.shutdown()


def test_reap_stalled_jobs_ignores_progressing_job() -> None:
    clock = _FakeClock()
    manager = JobManager(stall_timeout_seconds=300.0, clock=clock)

    release = threading.Event()

    def work(handle) -> None:  # noqa: ANN001
        release.wait(timeout=5.0)

    job_id = manager.submit(work)
    for _ in range(100):
        if manager.get_status(job_id).status == "RUNNING":
            break
        threading.Event().wait(0.01)

    # Advance most of the window, then record progress (resets the liveness clock).
    clock.advance(290.0)
    manager.apply_update(job_id, lambda job: setattr(job, "progress_pct", 50.0))
    clock.advance(290.0)  # 290s since the LAST progress, still < 300
    assert manager.reap_stalled_jobs() == []
    assert manager.get_status(job_id).status == "RUNNING"

    release.set()
    manager.shutdown()


def test_reap_stalled_jobs_skips_terminal_jobs() -> None:
    clock = _FakeClock()
    manager = JobManager(stall_timeout_seconds=300.0, clock=clock)
    done = manager.submit(lambda handle: None)
    assert manager.wait(done, timeout=5.0)
    clock.advance(10_000.0)
    # A DONE job is never reaped, regardless of how old its last update is.
    assert manager.reap_stalled_jobs() == []
    assert manager.get_status(done).status == "DONE"
    manager.shutdown()


def test_watchdog_disabled_when_timeout_none() -> None:
    manager = JobManager(stall_timeout_seconds=None)
    assert manager.reap_stalled_jobs() == []
    manager.shutdown()
