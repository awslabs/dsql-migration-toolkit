# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""When Aurora DSQL finished building the secondary indexes a Full Load submitted (P4b).

A Drop & reload table's ``CREATE INDEX ASYNC`` statements go on the empty recreated table
before its rows (P4d: the run waits, bounded, for those builds before its first row); a table
recreated while others loaded gets them once every table's data has loaded (P4a). Each build
is recorded on the job (``MigrationJob.index_builds``): its table, its index, whether it is
UNIQUE, and the job id Aurora DSQL answered with. The run does not wait for the builds after
the load -- Aurora DSQL builds them in the background (7-12 minutes after an 8.5M-row load,
live) and Validation and Start CDC need none of them. This module says when they are done:

* :func:`fold_index_build_reads` folds one read of Aurora DSQL
  (:func:`~dsql_migrator.core.target_introspector.index_build_states`: ``sys.jobs`` BY the
  recorded job ids, ``pg_index`` once a job is no longer listed -- an inference held back
  while the job cannot have aged out yet) into the record: a build that completed is
  counted and dropped, one that failed is kept with its value-free reason until its index
  is valid (created again by hand);
* a background watch (:func:`run_index_build_watch`; one per Full Load job, started by the
  Data Migration step's render while a build is pending -- or, at most once a minute, while
  one failed -- :func:`start_index_build_watch`) polls with backoff, heartbeats, and logs
  one line per failed build and one when the last one ended -- never ``CALL
  sys.wait_for_job``, which blocks for as long as an id is unknown to it;
* :func:`render_index_builds`, the step's notice: building, built (when, and how long after
  the data loaded), or failed -- a failed UNIQUE build is a missing constraint.
"""

from __future__ import annotations

import logging
import threading
import time as _time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus, log_activity
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import IndexBuild, IndexBuilds
from dsql_migrator.core.target_introspector import index_build_states
from dsql_migrator.ui.data_migration._fk_validation import (
    _call_safely,
    _job_live,
    sanitize_job_details,
)
from dsql_migrator.ui.design import render_notice

_LOGGER = logging.getLogger(__name__)

# One watch polls for at most this long; the step starts another while a build is pending
# (Aurora DSQL's build runs on whether or not anyone watches it).
_WATCH_BUDGET_SECONDS = 600.0
_POLL_FIRST_SECONDS = 2.0
_POLL_MAX_SECONDS = 30.0
# No new watch for a job until this long after its last one ended.
_WATCH_REST_SECONDS = 30.0
# A build Aurora DSQL answered no job id for is confirmed only by its index turning valid;
# this long after the record's last submission it is shown "not confirmed" and no longer
# watched.
_INDEX_BUILD_GIVE_UP_SECONDS = 6 * 3600
# A job id ``sys.jobs`` does not list while its index is not valid means "ended without a
# usable index" only once the job can have aged out -- Aurora DSQL keeps a finished job about
# 30 minutes after it ends -- so this soon after the record's last submission such an id is
# unexplained (not listed yet, or not a sys.jobs id) and its build is left building.
_UNLISTED_GRACE_SECONDS = 15 * 60
# A failed build is read again (its index valid now, created again by hand: built) when the
# step renders, at most this often.
_FAILURE_RECHECK_SECONDS = 60.0
# At most this many builds are named in a notice.
_LISTED_MAX = 8
_TERMINAL = ("DONE", "FAILED", "CANCELLED")

# The remedy on one failed build's activity line (the line names the index).
_REMEDY_UNIQUE = (
    "The loaded rows most likely hold a duplicate key for it, so the target does not "
    "enforce that constraint: remove the duplicates, drop the index if the table still "
    "lists it, and create it again before cutting over."
)
_REMEDY = (
    "Fix the cause Aurora DSQL reports, drop the index if the table still lists it, and "
    "create it again."
)
_RECHECK = "Once an index is valid on Aurora DSQL, this step shows it as built."


# ---------------------------------------------------------------------------
# The record
# ---------------------------------------------------------------------------


def fold_index_build_reads(
    builds: IndexBuilds, reads: Mapping, now: datetime
) -> "tuple[IndexBuilds, list[IndexBuild], bool]":
    """Fold one read of Aurora DSQL into the record. Pure.

    ``reads`` maps ``(table, index)`` to an ``IndexBuildRead`` (``None``: not read this
    time, unchanged). A completed build is counted (by table) and dropped from
    ``building``; a failed one moves to ``failures`` with its details sanitized (the first
    line, no ``Key (...)=(...)`` part, at most 300 characters: value-free, Property 7) --
    except an ``unlisted`` failure within :data:`_UNLISTED_GRACE_SECONDS` of the record's
    last submission, which stays building (its job cannot have aged out yet). A failure read
    as completed (the step's re-check: its index is valid now) is counted as built and
    leaves ``failures``. ``checked_at`` is ``now`` when anything answered, and
    ``finished_at`` ``now`` when the last build settled (or the last failure turned out
    built). Returns ``(record, the builds that failed now, whether the last build settled
    now)``; the record itself when nothing answered.
    """
    still: list[IndexBuild] = []
    failed: list[IndexBuild] = []
    by_table = dict(builds.completed_by_table)
    completed = 0
    answered = False
    decidable = _unlisted_decidable(builds, now)
    for build in builds.building:
        read = reads.get((build.table, build.index))
        if read is None:
            still.append(build)
            continue
        answered = True
        status = getattr(read, "status", None)
        if status == "completed":
            completed += 1
            by_table[build.table] = by_table.get(build.table, 0) + 1
        elif status == "failed" and (decidable or not getattr(read, "unlisted", False)):
            failed.append(
                build.model_copy(
                    update={"details": sanitize_job_details(getattr(read, "details", None))}
                )
            )
        else:
            still.append(build)
    kept: list[IndexBuild] = []
    resolved = 0
    for failure in builds.failures:
        read = reads.get((failure.table, failure.index))
        if read is None:
            kept.append(failure)
            continue
        answered = True
        if getattr(read, "status", None) == "completed":
            resolved += 1
            by_table[failure.table] = by_table.get(failure.table, 0) + 1
        else:
            kept.append(failure)
    if not answered:
        return builds, [], False
    finished = bool(builds.building) and not still
    record = builds.model_copy(update={
        "completed": builds.completed + completed + resolved,
        "failed": max(0, builds.failed - resolved) + len(failed),
        "completed_by_table": by_table,
        "building": still,
        "failures": kept + failed,
        "checked_at": now,
        "finished_at": now if finished or (resolved and not still) else builds.finished_at,
    })
    return record, failed, finished


def _unlisted_decidable(record: IndexBuilds, now: datetime) -> bool:
    """Whether an unlisted job id can mean its build ended: the record's last submission is
    at least :data:`_UNLISTED_GRACE_SECONDS` old (no time: decidable)."""
    if record.submitted_at is None:
        return True
    try:
        return (now - record.submitted_at).total_seconds() >= _UNLISTED_GRACE_SECONDS
    except TypeError:  # a naive and an aware time: not comparable, decide as before
        return True


def _recheck_due(record: object, now: datetime) -> bool:
    """Whether the record's failed builds are due a re-read (at most once a minute)."""
    if record is None or not getattr(record, "failures", None):
        return False
    checked = getattr(record, "checked_at", None)
    if checked is None:
        return True
    try:
        return (now - checked).total_seconds() >= _FAILURE_RECHECK_SECONDS
    except TypeError:
        return True


def _by_name(failure: IndexBuild) -> IndexBuild:
    """A failed build read again by its index only (``pg_index``: valid or not), never by its
    old job id, whose ``sys.jobs`` row would still say failed."""
    return failure.model_copy(update={"job_id": None, "details": None})


def _given_up(build: IndexBuild, record: IndexBuilds, now: datetime) -> bool:
    """An id-less build this long after the record's last submission: not watched any more."""
    if build.job_id or record.submitted_at is None:
        return False
    try:
        return (now - record.submitted_at).total_seconds() > _INDEX_BUILD_GIVE_UP_SECONDS
    except TypeError:  # a naive and an aware time: not decidable, keep watching
        return False


def _now(now: Optional[datetime]) -> datetime:
    return now if now is not None else datetime.now(timezone.utc)


def index_builds_pending(job: object, *, now: Optional[datetime] = None) -> bool:
    """Whether ``job`` records a build a watch still reads (one Aurora DSQL is building, not
    an id-less one past the give-up). No I/O."""
    record = getattr(job, "index_builds", None)
    if record is None or not record.building:
        return False
    moment = _now(now)
    return any(not _given_up(build, record, moment) for build in record.building)


# ---------------------------------------------------------------------------
# The watch
# ---------------------------------------------------------------------------


def _hhmm(moment: Optional[datetime]) -> str:
    """``HH:MM`` in UTC ("" for no time)."""
    if moment is None:
        return ""
    aware = moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)
    return aware.astimezone(timezone.utc).strftime("%H:%M")


def _after_load(record: IndexBuilds) -> str:
    """How long after the data loaded the last build settled ("" when not known) -- or that it
    settled before: P4d builds a Drop & reload table's indexes on the empty table, before its
    rows."""
    start, end = record.data_loaded_at, record.finished_at
    if start is None or end is None:
        return ""
    try:
        seconds = (end - start).total_seconds()
    except TypeError:
        return ""
    if seconds <= 0:
        return "before the data finished loading"
    if seconds < 60:
        return "less than a minute after the data loaded"
    return f"{round(seconds / 60)} min after the data loaded"


def _log_failed_build(build: IndexBuild) -> None:
    log_activity(
        ActivityCategory.FULL_LOAD,
        "index build failed",
        status=ActivityStatus.WARNING,
        target=f"{build.table}.{build.index}",
        detail=(
            f"Aurora DSQL's build of the {'UNIQUE ' if build.unique else ''}index failed: "
            f"{build.details or 'no details reported'}. "
            + (_REMEDY_UNIQUE if build.unique else _REMEDY)
        ),
    )


def _log_resolved_build(build: IndexBuild) -> None:
    log_activity(
        ActivityCategory.FULL_LOAD,
        "index valid",
        status=ActivityStatus.SUCCESS,
        target=f"{build.table}.{build.index}",
        detail=(
            f"The {'UNIQUE ' if build.unique else ''}index whose build had failed is valid on "
            "Aurora DSQL now."
        ),
    )


def _log_builds_finished(record: IndexBuilds) -> None:
    after = _after_load(record)
    log_activity(
        ActivityCategory.FULL_LOAD,
        "index builds finished",
        status=ActivityStatus.WARNING if record.failed else ActivityStatus.SUCCESS,
        detail=(
            f"Aurora DSQL built {record.completed} secondary index(es); the last finished "
            f"by {_hhmm(record.finished_at)} UTC"
            + (f", {after}" if after else "")
            + (f"; {record.failed} failed" if record.failed else "")
        ),
    )


def run_index_build_watch(
    job_manager: Any,
    fl_job_id: str,
    *,
    connection_factory: Callable[[], Any],
    refresh: Optional[Callable[[], None]] = None,
    handle: object = None,
    budget_seconds: float = _WATCH_BUDGET_SECONDS,
    first_delay: float = _POLL_FIRST_SECONDS,
    max_delay: float = _POLL_MAX_SECONDS,
    sleep: Callable[[float], None] = _time.sleep,
    monotonic: Callable[[], float] = _time.monotonic,
    now: Optional[Callable[[], datetime]] = None,
) -> Optional[IndexBuilds]:
    """The watch job's work: poll Aurora DSQL for the Full Load job's pending builds.

    Each poll: a heartbeat, the job's record (a job gone from the manager ends the watch),
    ONE read of its building records by job id and of its failed ones by index name
    (:func:`~dsql_migrator.core.target_introspector.index_build_states`; a failed index
    created again since and valid now is built), folded into the live job under the
    manager's lock (``apply_update``, persisted; skipped when nothing answered). One
    WARNING line per build that failed, one SUCCESS line per failed one valid now, one line
    when the last one settled (SUCCESS, or WARNING with the failed count), and
    ``refresh()`` after a change. It ends when nothing is building (a record with failed
    builds only is read once), when the handle is cancelled, or after ``budget_seconds``
    (the step starts another while one is pending); between polls the wait doubles from
    ``first_delay`` to ``max_delay``. Returns the last record it saw (``None``: the job is
    gone). Read-only on Aurora DSQL.
    """
    clock = now if callable(now) else (lambda: datetime.now(timezone.utc))
    beat = getattr(handle, "heartbeat", None)
    delay = max(0.0, first_delay)
    deadline = monotonic() + max(0.0, budget_seconds)
    record: Optional[IndexBuilds] = None
    while True:
        if callable(beat):
            beat()
        try:
            job = job_manager.get_status(fl_job_id)
        except JobNotFoundError:
            return None
        record = getattr(job, "index_builds", None)
        if record is None or not (record.building or record.failures):
            return record
        read_at = clock()
        reads = index_build_states(
            list(record.building) + [_by_name(failure) for failure in record.failures],
            connection_factory=connection_factory,
        )
        if any(read is not None for read in reads.values()):
            outcome: dict = {}

            def _fold(live: Any, reads: Mapping = reads, read_at: datetime = read_at) -> None:
                current = getattr(live, "index_builds", None)
                if current is None or not (current.building or current.failures):
                    return
                folded, failed, finished = fold_index_build_reads(current, reads, read_at)
                live.index_builds = folded
                still_failed = {(f.table, f.index) for f in folded.failures}
                resolved = [
                    f for f in current.failures if (f.table, f.index) not in still_failed
                ]
                outcome.update(
                    record=folded, failed=failed, finished=finished, resolved=resolved,
                    changed=len(folded.building) != len(current.building) or bool(resolved),
                )

            try:
                job_manager.apply_update(fl_job_id, _fold)
            except JobNotFoundError:
                return None
            record = outcome.get("record", record)
            for build in outcome.get("failed", ()):
                _log_failed_build(build)
            for build in outcome.get("resolved", ()):
                _log_resolved_build(build)
            if outcome.get("finished"):
                _log_builds_finished(record)
            if outcome.get("changed"):
                _call_safely(refresh)
        if record is None or not record.building:
            return record
        if getattr(handle, "cancelled", False) or monotonic() >= deadline:
            return record
        sleep(delay)
        delay = min(max(max_delay, 0.0), max(delay * 2, first_delay))


# {Full Load job id: (its watch's job id, monotonic time it ended or None while it runs)}
_WATCHES: "dict[str, tuple[str, Optional[float]]]" = {}
_WATCHES_LOCK = threading.RLock()


def index_build_watch_due(
    job: object,
    job_manager: object,
    *,
    now: Optional[datetime] = None,
    monotonic: Callable[[], float] = _time.monotonic,
) -> bool:
    """Whether the step should start a watch for the Full Load ``job`` now: the job is
    terminal, it records a build still pending (:func:`index_builds_pending`) or failed
    builds not read again for a minute, no watch for it runs, and the last one ended at
    least 30 s ago. No I/O."""
    if job is None or job_manager is None:
        return False
    if getattr(job, "status", None) not in _TERMINAL:
        return False
    moment = _now(now)
    if not (
        index_builds_pending(job, now=moment)
        or _recheck_due(getattr(job, "index_builds", None), moment)
    ):
        return False
    with _WATCHES_LOCK:
        entry = _WATCHES.get(getattr(job, "job_id", ""))
    if entry is None:
        return True
    watch_id, ended = entry
    if _job_live(job_manager, watch_id):
        return False
    return ended is None or monotonic() - ended >= _WATCH_REST_SECONDS


def start_index_build_watch(
    job_manager: Any,
    job: object,
    *,
    connection_factory: Optional[Callable[[], Any]],
    refresh: Optional[Callable[[], None]] = None,
    now: Optional[datetime] = None,
    monotonic: Callable[[], float] = _time.monotonic,
) -> Optional[str]:
    """Start the background watch of the Full Load ``job``'s pending builds when one is due
    (:func:`index_build_watch_due`) -- one per job at a time: the due check, the registry
    entry and the submission are one step under the registry's lock. Returns the watch's job
    id, or ``None`` (not due, no job manager or connection). A non-durable job: nothing
    resumes it after a restart, the next render starts another. Best effort: never raises
    (a submission that fails counts as a watch that just ended)."""
    if job_manager is None or connection_factory is None or job is None:
        return None
    fl_job_id = getattr(job, "job_id", None)
    if not fl_job_id:
        return None
    try:
        with _WATCHES_LOCK:
            if not index_build_watch_due(job, job_manager, now=now, monotonic=monotonic):
                return None
            watch_id = f"index-builds-{uuid.uuid4().hex}"

            def _work(handle: object = None) -> None:
                try:
                    run_index_build_watch(
                        job_manager, fl_job_id, connection_factory=connection_factory,
                        refresh=refresh, handle=handle,
                    )
                finally:
                    with _WATCHES_LOCK:
                        if _WATCHES.get(fl_job_id, ("", None))[0] == watch_id:
                            _WATCHES[fl_job_id] = (watch_id, monotonic())

            _WATCHES[fl_job_id] = (watch_id, None)
            try:
                return job_manager.submit(_work, job_id=watch_id, durable=False)
            except Exception:
                _WATCHES[fl_job_id] = (watch_id, monotonic())
                raise
    except Exception:  # noqa: BLE001 - the watch is a report; never break the render
        _LOGGER.warning("Could not start the index build watch", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# The notice
# ---------------------------------------------------------------------------


def _notice_remedy(unique: int, others: int) -> str:
    """The failed-builds notice's remedy, for ``unique`` UNIQUE and ``others`` other ones."""
    if not unique:
        return (
            _REMEDY
            if others == 1
            else "Fix the cause Aurora DSQL reports, drop each index the table still lists, "
            "and create it again."
        )
    text = (
        "The loaded rows most likely hold a duplicate key for the UNIQUE index, so the "
        "target does not enforce that constraint: remove the duplicates, drop the index if "
        "the table still lists it, and create it again before cutting over."
        if unique == 1
        else "The loaded rows most likely hold duplicate keys for the UNIQUE indexes, so the "
        "target does not enforce those constraints: remove the duplicates, drop each index "
        "the table still lists, and create it again before cutting over."
    )
    if others:
        text += (
            " For the other one, fix the cause Aurora DSQL reports, drop it if the table "
            "still lists it, and create it again."
            if others == 1
            else " For the others, fix the cause Aurora DSQL reports, drop each one the table "
            "still lists, and create it again."
        )
    return text


def _names(builds: "Sequence[IndexBuild]") -> str:
    shown = ", ".join(f"{b.table}.{b.index}" for b in builds[:_LISTED_MAX])
    more = len(builds) - _LISTED_MAX
    return shown + (f" and {more} more" if more > 0 else "")


def _not_confirmed(builds: "Sequence[IndexBuild]") -> str:
    """The sentence on the id-less builds past the give-up ("" without any)."""
    if not builds:
        return ""
    one = len(builds) == 1
    return (
        f"Not confirmed: {_names(builds)}. Aurora DSQL answered no job id for "
        f"{'this build' if one else 'these builds'}, and "
        f"{'its index was' if one else 'their indexes were'} still not valid 6 hours after "
        "the submission, so the tool no longer checks: check the table's indexes on Aurora "
        "DSQL"
        + (
            "; a UNIQUE one is a missing constraint until it is valid."
            if any(b.unique for b in builds)
            else "."
        )
    )


def render_index_builds(ui, job: object, *, now: Optional[datetime] = None) -> None:
    """The Data Migration step's notice on Aurora DSQL's builds of the secondary indexes the
    Full Load submitted (``MigrationJob.index_builds``). No I/O.

    Building: info, live ("n of m built", when the data loaded, and that nothing waits for
    them). All built: success (when the last one finished, and how long after the data
    loaded). Failed builds: error when a UNIQUE one is among them (a missing constraint),
    else warning, naming each one (a UNIQUE one marked) with Aurora DSQL's reason and the
    remedy, the builds still running counted in the header; the step reads a failed one
    again, so one created again by hand reads as built once it is valid. An id-less build
    past the give-up is named "not confirmed".
    """
    record = getattr(job, "index_builds", None)
    if record is None or not record.submitted:
        return
    moment = _now(now)
    building = list(record.building)
    unconfirmed = [b for b in building if _given_up(b, record, moment)]
    watched = len(building) - len(unconfirmed)
    loaded = _hhmm(record.data_loaded_at)
    not_confirmed = _not_confirmed(unconfirmed)
    failures = list(record.failures)
    if failures:
        unique = sum(1 for f in failures if f.unique)
        others = len(failures) - unique
        counts = f"{len(failures)} of {record.submitted}"
        running = f", {watched} still building" if watched else ""
        # Each UNIQUE one is named as such: it is the missing constraint the header names.
        listed = "; ".join(
            f"{f.table}.{f.index}{' (UNIQUE)' if f.unique else ''}: "
            f"{f.details or 'no details reported'}"
            for f in failures[:_LISTED_MAX]
        ) + (
            f"; and {len(failures) - _LISTED_MAX} more" if len(failures) > _LISTED_MAX else ""
        )
        if unique:
            header = (
                "UNIQUE index build failed: a constraint is missing"
                if unique == 1
                else f"UNIQUE index builds failed: {unique} constraints are missing"
            ) + f" ({counts} failed{running})"
        else:
            header = f"Secondary index builds failed ({counts}{running})"
        render_notice(
            ui,
            tone="error" if unique else "warning",
            header=header,
            body=(
                "The data loaded, but Aurora DSQL could not build "
                f"{'this index' if len(failures) == 1 else 'these indexes'}. "
                f"{listed}. {_notice_remedy(unique, others)} {_RECHECK}"
                + (f" {not_confirmed}" if not_confirmed else "")
            ),
        )
        return
    if watched:
        render_notice(
            ui,
            tone="info",
            busy=True,
            header=(
                "Aurora DSQL is building secondary indexes: "
                f"{record.completed} of {record.submitted} built"
            ),
            body=(
                (f"Every table's data loaded at {loaded} UTC. " if loaded else "")
                + "Aurora DSQL builds the indexes in the background; queries that need one "
                "are slower until it is built. You can continue with the next step "
                "meanwhile."
                + (f" {not_confirmed}" if not_confirmed else "")
            ),
        )
        return
    if unconfirmed:
        render_notice(
            ui,
            tone="warning" if any(b.unique for b in unconfirmed) else "info",
            header=(
                "Secondary index builds not confirmed: "
                f"{record.completed} of {record.submitted} built"
            ),
            body=not_confirmed,
        )
        return
    after = _after_load(record)
    finished = _hhmm(record.finished_at)
    render_notice(
        ui,
        tone="success",
        header=(
            f"Secondary indexes built: all {record.submitted}"
            + (f", the last by {finished} UTC" if finished else "")
            + (f" ({after})" if after else "")
        ),
    )


__all__ = [
    "fold_index_build_reads",
    "index_build_watch_due",
    "index_builds_pending",
    "render_index_builds",
    "run_index_build_watch",
    "start_index_build_watch",
]
