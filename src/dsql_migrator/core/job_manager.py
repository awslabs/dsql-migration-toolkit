"""Background job manager for long-running migration operations.

Long-running steps (schema introspection, data migration) must not block the
web UI's event loop (Requirement 9.3 / design.md "8. App Orchestrator & Job
Manager"). This module provides a small, thread-safe :class:`JobManager` that:

1. starts a caller-supplied unit of work on a background daemon thread and
   returns a ``job_id`` immediately (non-blocking), creating the job record in
   the ``PENDING`` state and transitioning it to ``RUNNING`` once the worker
   begins;
2. records progress (``progress_pct``, ``error_count``, per-chunk
   :class:`~dsql_migrator.core.models.ChunkState`, status) into a thread-safe
   state store keyed by ``job_id``, so the UI can poll it periodically (e.g.
   NiceGUI ``ui.timer``) without ever sharing mutable state across threads;
3. transitions the job to ``DONE`` on success and ``FAILED`` on an unhandled
   exception, capturing a human-readable error message for actionable UI
   feedback; and
4. exposes a status-query interface (:meth:`JobManager.get_status`,
   :meth:`JobManager.list_jobs`) that returns deep copies so a polling caller
   can never observe a torn write or mutate the live job state.

Reused data contracts: the job state is the existing
:class:`~dsql_migrator.core.models.MigrationJob` (with ``status`` /
``progress_pct`` / ``error_count`` / ``chunks``); no parallel model is
introduced.

Thread-safety model: every read and every mutation goes through a single
re-entrant lock. The worker never touches the live :class:`MigrationJob`
directly; instead it records progress through :meth:`JobHandle.update`, whose
mutator runs under the same lock that :meth:`JobManager.get_status` uses to take
its snapshot. Reads and writes are therefore fully serialized.

Credential safety (Property 7 / Requirement 9.2): the manager neither receives
nor resolves credentials, and performs no logging, so it cannot introduce
plaintext secrets into job state or logs. A captured failure message is stored
verbatim from the worker's exception; components that handle secrets (e.g. the
importers) are responsible for redacting their own messages before raising.

Threads (not asyncio tasks) are used because the core engine is synchronous and
I/O-bound (``psycopg`` / ``subprocess`` / ``ThreadPoolExecutor``); a background
thread keeps the NiceGUI event loop responsive without rewriting the engine as
coroutines.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Callable, Optional

from dsql_migrator.core.models import MigrationJob

if TYPE_CHECKING:
    from dsql_migrator.core.job_store import JobStore

# Default no-progress window (seconds) after which the stall watchdog declares a
# RUNNING job hung and marks it FAILED. A healthy Full Load flushes progress
# every PROGRESS_FLUSH_ROWS (each <=10k rows) and the source stream has its own
# per-socket read timeout (SOURCE_READ_TIMEOUT_SECONDS, 300s); this is set well
# above both so it only ever fires on a genuine wedge the read timeout did not
# catch (e.g. a target/connection hang with no socket deadline), never on a slow
# but progressing load.
DEFAULT_STALL_TIMEOUT_SECONDS = 900.0

# How often the watchdog scans for stalled jobs (seconds).
DEFAULT_STALL_POLL_SECONDS = 30.0

# Error message stamped on a PENDING/RUNNING job that was killed by an app
# restart (reconciled to FAILED on the next :meth:`JobManager.restore`). It is a
# stable marker (matched by :func:`is_interrupted_by_restart`) so the UI can tell
# "the worker was interrupted by a restart" apart from a real in-job failure --
# the underlying AWS/CloudFormation work (e.g. a CDC connector still CREATING)
# often kept running, so the UI should re-check live state instead of declaring a
# hard failure.
INTERRUPTED_BY_RESTART_MESSAGE = (
    "Interrupted: the app restarted while this job was running. "
    "Retry the failed tables to resume."
)


def is_interrupted_by_restart(error: Optional[str]) -> bool:
    """Return whether ``error`` is the restart-interruption marker (not a real failure).

    Lets the UI distinguish a job the JobManager reconciled to FAILED purely
    because the process restarted mid-run -- where the real AWS work may have
    continued -- from a genuine in-job error, so it can re-check live state rather
    than show a misleading "failed".
    """
    return bool(error) and INTERRUPTED_BY_RESTART_MESSAGE in error

# A monotonic clock (injectable for deterministic tests; never wall-clock so it
# is immune to system time jumps).
Clock = Callable[[], float]

# A unit of background work. It receives a :class:`JobHandle` to record progress
# and may run for a long time; returning normally marks the job ``DONE`` and
# raising marks it ``FAILED``.
WorkFn = Callable[["JobHandle"], None]

# A mutation applied to the live :class:`MigrationJob` under the manager lock.
Mutator = Callable[[MigrationJob], None]


class JobNotFoundError(KeyError):
    """Raised when a ``job_id`` is not known to the :class:`JobManager`."""


@dataclass
class _JobRecord:
    """Internal per-job state: the job, its worker thread, and any error.

    ``error`` holds a human-readable failure message (the worker's redacted
    exception text) for UI feedback; it is kept off the :class:`MigrationJob`
    so the shared model contract is unchanged. ``last_progress_at`` is the
    monotonic timestamp of the last status change or progress update, used by the
    stall watchdog to detect a hung worker that has stopped making progress.
    """

    job: MigrationJob
    thread: Optional[threading.Thread] = None
    error: Optional[str] = None
    cancel_event: threading.Event = field(default_factory=threading.Event)
    last_progress_at: Optional[float] = None


class JobHandle:
    """A worker's thread-safe handle to record progress for its job.

    The worker mutates job state only through :meth:`update`, whose mutator runs
    under the manager lock. This keeps all writes serialized with the snapshot
    reads performed by :meth:`JobManager.get_status`.
    """

    def __init__(self, manager: "JobManager", job_id: str) -> None:
        """Bind the handle to ``manager`` and the job it manages."""
        self._manager = manager
        self._job_id = job_id

    @property
    def job_id(self) -> str:
        """The ``job_id`` this handle reports progress for."""
        return self._job_id

    @property
    def cancelled(self) -> bool:
        """Whether a cooperative stop has been requested for this job.

        Long-running work (e.g. Full Load) should poll this between units (tables
        / batches) and stop promptly when it becomes ``True``; the manager then
        marks the job ``CANCELLED`` once the worker returns.
        """
        return self._manager.is_cancel_requested(self._job_id)

    def update(self, mutator: Mutator) -> None:
        """Apply ``mutator`` to the live :class:`MigrationJob` under the lock.

        Use this to record progress, e.g.
        ``handle.update(lambda job: setattr(job, "progress_pct", 50.0))`` or to
        append/refresh :class:`~dsql_migrator.core.models.ChunkState` entries.
        The mutation is atomic with respect to status reads.
        """
        self._manager.apply_update(self._job_id, mutator)


class JobManager:
    """Runs background jobs and exposes their status by ``job_id`` (Req 9.3).

    All state access is guarded by a single re-entrant lock, so the UI may poll
    :meth:`get_status` from one thread while a worker updates the same job from
    another without races. Worker threads are daemons so they never block
    process exit.
    """

    def __init__(
        self,
        *,
        store: "Optional[JobStore]" = None,
        stall_timeout_seconds: Optional[float] = DEFAULT_STALL_TIMEOUT_SECONDS,
        clock: Optional[Clock] = None,
    ) -> None:
        """Create a job manager, optionally backed by a durable ``store``.

        When a ``store`` is supplied, job snapshots are persisted on every state
        change and any previously persisted jobs are reloaded immediately, so an
        interrupted run survives an app restart (resumability, Property 4).

        ``stall_timeout_seconds`` arms the stall watchdog: a ``RUNNING`` job that
        records no status change or progress update for this many seconds is
        marked ``FAILED`` (a backstop for a worker wedged in a blocking call that
        cannot mark itself failed). Pass ``None`` to disable the watchdog (tests
        drive :meth:`reap_stalled_jobs` directly). ``clock`` is an injectable
        monotonic clock so tests advance time deterministically; it defaults to
        :func:`time.monotonic`.
        """
        self._records: dict[str, _JobRecord] = {}
        self._lock = threading.RLock()
        self._store: "Optional[JobStore]" = store
        self._clock: Clock = clock or time.monotonic
        self._stall_timeout = stall_timeout_seconds
        self._watchdog: Optional[threading.Thread] = None
        self._watchdog_stop = threading.Event()
        if store is not None:
            self.restore()
        if stall_timeout_seconds is not None:
            self._start_watchdog()

    def attach_store(self, store: "JobStore") -> None:
        """Attach a durable ``store`` and reload any persisted jobs.

        Used by the app to wire the configured SQLite store onto the module-level
        manager after configuration is loaded. Restores persisted jobs into this
        (typically empty) manager so a reconnecting UI can find them.
        """
        with self._lock:
            self._store = store
        self.restore()

    def restore(self) -> None:
        """Reload persisted jobs, reconciling ones interrupted by a restart.

        A job whose snapshot is still ``PENDING``/``RUNNING`` had its worker
        killed with the previous process, so it is marked ``FAILED`` and any
        ``IN_PROGRESS`` chunk is marked ``FAILED`` too. Already-``DONE`` chunks
        and the captured watermark are preserved, so the existing "retry failed
        tables" path resumes exactly the unfinished work (idempotent re-load).
        """
        store = self._store
        if store is None:
            return
        with self._lock:
            for persisted in store.load_all():
                job = persisted.job
                error = persisted.error
                if job.status in ("PENDING", "RUNNING"):
                    job.status = "FAILED"  # type: ignore[assignment]
                    for chunk in job.chunks:
                        if chunk.status == "IN_PROGRESS":
                            chunk.status = "FAILED"  # type: ignore[assignment]
                    failed = sum(1 for c in job.chunks if c.status == "FAILED")
                    job.error_count = max(job.error_count, failed, 1)
                    if not error:
                        error = INTERRUPTED_BY_RESTART_MESSAGE
                self._records[job.job_id] = _JobRecord(
                    job=job, thread=None, error=error
                )
            # Persist the reconciled snapshots so the interrupted status sticks.
            for record in self._records.values():
                self._persist_locked(record)

    def _persist_locked(self, record: _JobRecord) -> None:
        """Persist ``record`` to the store, if any. Must hold the lock."""
        if self._store is not None:
            self._store.save(record.job, record.error)

    def prune_terminal(self, keep_most_recent: int) -> int:
        """Bound growth by keeping only the newest ``keep_most_recent`` DONE jobs.

        Non-terminal (resumable/active) jobs are never pruned. Returns how many
        DONE jobs were dropped from both the durable store and memory. Typically
        called once at startup so completed migrations do not accumulate.
        """
        if self._store is not None:
            deleted = self._store.prune_terminal(keep_most_recent)
            with self._lock:
                for job_id in deleted:
                    self._records.pop(job_id, None)
            return len(deleted)
        with self._lock:
            done = [
                job_id
                for job_id, record in self._records.items()
                if record.job.status == "DONE"
            ]
            to_delete = done if keep_most_recent <= 0 else done[:-keep_most_recent]
            for job_id in to_delete:
                self._records.pop(job_id, None)
            return len(to_delete)

    def submit(self, work: WorkFn, *, job_id: Optional[str] = None) -> str:
        """Start ``work`` on a background thread and return its ``job_id`` now.

        Creates the job in ``PENDING`` state and launches a daemon thread that
        transitions it to ``RUNNING``, runs ``work(handle)``, then marks it
        ``DONE`` (normal return) or ``FAILED`` (unhandled exception). Returns
        immediately without waiting for the work to finish (non-blocking).

        ``job_id`` may be supplied to correlate the job with an external id;
        otherwise a random hex id is generated. Raises :class:`ValueError` if the
        id is already in use.
        """
        new_id = job_id or uuid.uuid4().hex
        thread = threading.Thread(
            target=self._run,
            args=(new_id, work),
            name=f"job-{new_id}",
            daemon=True,
        )
        with self._lock:
            if new_id in self._records:
                raise ValueError(f"job_id already exists: {new_id}")
            record = _JobRecord(
                job=MigrationJob(job_id=new_id),
                thread=thread,
                last_progress_at=self._clock(),
            )
            self._records[new_id] = record
            self._persist_locked(record)
        thread.start()
        return new_id

    def get_status(self, job_id: str) -> MigrationJob:
        """Return a deep-copied snapshot of the job's state (for UI polling).

        The copy is taken under the lock, so it is internally consistent and the
        caller can read or mutate it freely without affecting the live job.
        Raises :class:`JobNotFoundError` for an unknown ``job_id``.
        """
        with self._lock:
            return self._require(job_id).job.model_copy(deep=True)

    def get_error(self, job_id: str) -> Optional[str]:
        """Return the captured failure message, or ``None`` if not failed.

        The message is the worker's (already redacted) exception text; the
        manager never adds credentials to it. Raises :class:`JobNotFoundError`
        for an unknown ``job_id``.
        """
        with self._lock:
            return self._require(job_id).error

    def list_jobs(self) -> list[MigrationJob]:
        """Return deep-copied snapshots of every known job."""
        with self._lock:
            return [record.job.model_copy(deep=True) for record in self._records.values()]

    def wait(self, job_id: str, timeout: Optional[float] = None) -> bool:
        """Block until the job's worker finishes (or ``timeout`` elapses).

        Returns ``True`` if the worker has finished, ``False`` if it is still
        running after ``timeout``. Mainly useful for orchestrated shutdown and
        deterministic tests; the UI itself polls :meth:`get_status` instead.
        Raises :class:`JobNotFoundError` for an unknown ``job_id``.
        """
        with self._lock:
            thread = self._require(job_id).thread
        if thread is None:
            return True
        thread.join(timeout)
        return not thread.is_alive()

    def apply_update(self, job_id: str, mutator: Mutator) -> None:
        """Apply ``mutator`` to the live job under the lock (used by handles).

        Prefer :meth:`JobHandle.update` from within a worker. Raises
        :class:`JobNotFoundError` for an unknown ``job_id``.
        """
        with self._lock:
            record = self._require(job_id)
            mutator(record.job)
            # Any progress update (chunk completion, row-count flush) refreshes
            # the watchdog's liveness clock, so a slow-but-progressing job is
            # never reaped -- only one that has gone silent for the whole window.
            record.last_progress_at = self._clock()
            self._persist_locked(record)

    def request_cancel(self, job_id: str) -> bool:
        """Request a cooperative stop of the job's running work.

        Sets the job's cancel flag (which the worker polls via
        :attr:`JobHandle.cancelled`) so the worker can finish the current unit,
        leave already-done work intact, and return; the manager then records the
        job as ``CANCELLED``. This never force-kills the thread, so no partial or
        torn write can occur. Returns ``True`` if a running job's cancel was
        requested, ``False`` for an unknown or already-terminal job.
        """
        with self._lock:
            record = self._records.get(job_id)
            if record is None or record.job.status in (
                "DONE",
                "FAILED",
                "CANCELLED",
            ):
                return False
            record.cancel_event.set()
            return True

    def is_cancel_requested(self, job_id: str) -> bool:
        """Return whether a cooperative stop has been requested for ``job_id``."""
        with self._lock:
            record = self._records.get(job_id)
            return record is not None and record.cancel_event.is_set()

    # -- stall watchdog -----------------------------------------------------

    def _start_watchdog(self) -> None:
        """Launch the background thread that reaps stalled jobs (daemon)."""
        thread = threading.Thread(
            target=self._watchdog_loop, name="job-stall-watchdog", daemon=True
        )
        self._watchdog = thread
        thread.start()

    def _watchdog_loop(self) -> None:
        """Periodically reap stalled jobs until the manager is shut down."""
        while not self._watchdog_stop.wait(DEFAULT_STALL_POLL_SECONDS):
            try:
                self.reap_stalled_jobs()
            except Exception:  # noqa: BLE001 - the watchdog must never die
                pass

    def reap_stalled_jobs(self) -> list[str]:
        """Mark every ``RUNNING`` job with no recent progress ``FAILED``.

        A job is "stalled" when its monotonic ``last_progress_at`` is older than
        the configured stall timeout: its worker is wedged in a blocking call and
        can neither complete nor report its own failure, so it would otherwise
        sit in ``RUNNING`` forever (the UI shows "in progress" with no terminal
        affordances). Marking it ``FAILED`` -- with any ``IN_PROGRESS`` chunk also
        failed -- releases the UI to its terminal path (failure reason + "Retry
        failed tables"); the already-``DONE`` chunks are preserved so the retry
        resumes only the unfinished work (idempotent). The wedged thread is never
        force-killed (no torn write); it is abandoned as a daemon. Returns the
        ids of the jobs reaped. Idempotent and safe to call from a test.
        """
        if self._stall_timeout is None:
            return []
        now = self._clock()
        reaped: list[str] = []
        with self._lock:
            for record in self._records.values():
                if record.job.status != "RUNNING":
                    continue
                last = record.last_progress_at
                if last is None or (now - last) < self._stall_timeout:
                    continue
                record.job.status = "FAILED"  # type: ignore[assignment]
                failed_chunks = 0
                for chunk in record.job.chunks:
                    if chunk.status == "IN_PROGRESS":
                        chunk.status = "FAILED"  # type: ignore[assignment]
                    if chunk.status == "FAILED":
                        failed_chunks += 1
                record.job.error_count = max(
                    record.job.error_count, failed_chunks, 1
                )
                if not record.error:
                    record.error = (
                        "No progress for "
                        f"{int(self._stall_timeout)}s — the load appears to have "
                        "stalled (an unresponsive source/target connection) and "
                        "was stopped. Retry the failed tables to resume; the "
                        "already-loaded tables are kept."
                    )
                record.last_progress_at = now
                self._persist_locked(record)
                reaped.append(record.job.job_id)
        return reaped

    def shutdown(self) -> None:
        """Stop the stall watchdog thread (best effort). Safe to call repeatedly.

        Worker threads are daemons and are not joined here; this only quiesces the
        watchdog so a test or a clean process exit does not leave it spinning.
        """
        self._watchdog_stop.set()
        thread = self._watchdog
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=1.0)

    def _run(self, job_id: str, work: WorkFn) -> None:
        """Worker thread body: RUNNING -> work -> DONE/CANCELLED/FAILED."""
        self._set_status(job_id, "RUNNING")
        handle = JobHandle(self, job_id)
        try:
            work(handle)
        except Exception as exc:  # noqa: BLE001 - failure is recorded as job state
            self._mark_failed(job_id, exc)
        else:
            if self.is_cancel_requested(job_id):
                self._mark_cancelled(job_id)
            else:
                self._mark_done(job_id)

    def _set_status(
        self, job_id: str, status: str
    ) -> None:
        """Set the job's status under the lock (no-op for an unknown id)."""
        with self._lock:
            record = self._records.get(job_id)
            if record is not None:
                record.job.status = status  # type: ignore[assignment]
                record.last_progress_at = self._clock()
                self._persist_locked(record)

    def _mark_done(self, job_id: str) -> None:
        """Mark the job ``DONE`` unless a failure was already recorded."""
        with self._lock:
            record = self._records.get(job_id)
            if record is not None and record.job.status != "FAILED":
                record.job.status = "DONE"
                self._persist_locked(record)

    def _mark_cancelled(self, job_id: str) -> None:
        """Mark the job ``CANCELLED`` (cooperative stop), preserving any error.

        Records a user-facing stop message only when none was already captured,
        so a failure that happened to coincide with a stop keeps its own reason.
        """
        with self._lock:
            record = self._records.get(job_id)
            if record is None or record.job.status == "FAILED":
                return
            record.job.status = "CANCELLED"  # type: ignore[assignment]
            if not record.error:
                record.error = (
                    "Stopped by user. Already-loaded tables are kept; retry the "
                    "remaining (failed) tables to resume."
                )
            self._persist_locked(record)

    def _mark_failed(self, job_id: str, exc: BaseException) -> None:
        """Mark the job ``FAILED`` and capture a redacted error message.

        ``error_count`` is bumped to at least one so the failure is visible via
        the status snapshot even when the worker did not count it itself.
        """
        with self._lock:
            record = self._records.get(job_id)
            if record is None:
                return
            record.job.status = "FAILED"
            if record.job.error_count == 0:
                record.job.error_count = 1
            record.error = f"{type(exc).__name__}: {exc}"
            self._persist_locked(record)

    def _require(self, job_id: str) -> _JobRecord:
        """Return the record for ``job_id`` or raise :class:`JobNotFoundError`.

        Must be called while holding the lock.
        """
        record = self._records.get(job_id)
        if record is None:
            raise JobNotFoundError(job_id)
        return record


__all__ = [
    "WorkFn",
    "Mutator",
    "JobNotFoundError",
    "JobHandle",
    "JobManager",
    "INTERRUPTED_BY_RESTART_MESSAGE",
    "is_interrupted_by_restart",
]
