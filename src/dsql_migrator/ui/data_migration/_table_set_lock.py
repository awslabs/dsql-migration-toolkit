# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The column-exclusion and table-set locks (extracted from ``_cdc_monitoring.py``).

What a migration that already holds data, or a CDC stack, may still change, and the remedy
that is true when it may not: the exclusion lock (:class:`ExclusionLockState`,
:func:`exclusion_lock_state`) and the gates that read it (:func:`capture_error_with_remedy`,
:func:`change_and_reload_block_reason`); whether the table set may still change after a
Full Load (:func:`table_set_editable_after_load`) and the waits that hold it
(:func:`table_set_wait_reason`); in Full load + CDC until CDC first starts, the per-table
add-lock (:class:`TableSetAddLock`) and the tables selected since the load
(:func:`cdc_uncovered_selection`, :func:`fl_cdc_pending_additions`,
:func:`cdc_addition_block_reason`); the CDC lifecycle reads these share
(:func:`cdc_stack_being_deleted`, :func:`cdc_operation_in_flight`, ...); and the texts the
table tree, the exclusion popup and the Full Load step show for them.

No NiceGUI call and no AWS call: these read the migration state and the job manager's
records only. Its top-level imports are ``_cdc_state``, ``_models``, ``_cdc_status`` and
``core.models``. ``_models`` imports from this module only inside functions: this module
imports ``_models`` at its top, so a top-level import back would be a cycle.
``_cdc_monitoring``, where this block lived, re-exports every name defined here, private
ones included, so an import from it resolves unchanged.

A re-export does not carry a monkeypatch, nor does any module-level import: a test patches
a name on the module whose code reads it. A patch here reaches the calls made in this
module and the readers that import the name inside a function, at call time: ``_models``,
``_full_load_ui`` and ``ui.validation``. It does not reach the readers that bind it with a
module-level import: ``_exclusion_dialog``, ``_cdc_ui``, the package ``__init__``, and
``_cdc_monitoring``, whose ``_exclusion_state_facts`` reads ``exclusion_lock_state``
through the re-export. A test of one of those patches the name there.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from typing import Optional

from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.data_migration._cdc_state import (
    cdc_delete_in_flight,
    cdc_infra_deploy_in_flight,
    cdc_streaming_started,
)
from dsql_migrator.ui.data_migration._models import (
    SPLIT_SET_FIXED,
    session_source_type,
)
from dsql_migrator.ui.data_migration._cdc_status import (
    _current_job,
    is_infra_create_stack_status,
    is_stack_teardown_status,
)


# CDC-stack phases in which the excluded-column set has already been handed to the
# pipeline, so changing it now would diverge what Full Load writes from what CDC captures.
# NB "unstable" is included here even though ``lob_exclusion_lock``'s own tuple omits it:
# ``cdc_infra_prep_state`` treats an unstable stack as ready, so leaving it out would let
# the bundled action through on a stack that already carries the old list.
_EXCLUSION_BAKED_PHASES = (
    "infra",
    "running",
    "provisioning",
    "partial",
    "unstable",
)


def cdc_stack_being_deleted(migration_state, job_manager) -> bool:
    """True while this session's cdc-stack is being DELETED (an expected wait, not a lock).

    Either an in-session delete job is PENDING/RUNNING (:func:`cdc_delete_in_flight`), or
    the probed stack reports ``DELETE_IN_PROGRESS`` (a restored session sees it that way).
    Every exclusion lock checks this BEFORE its streaming / stack clauses: a delete job
    makes ``cdc_streaming_started`` True and a deleting stack is phase "unstable", so those
    clauses would otherwise tell the operator to start the Delete that is already running.
    Not this: a FAILED delete (retrying the Delete is right) or a Stop (the stack stays).
    """
    return cdc_delete_in_flight(migration_state, job_manager) or is_stack_teardown_status(
        getattr(migration_state, "cdc_stack_phase_status", None)
    )


def cdc_delete_eta(migration_state) -> "tuple[str, str]":
    """``(estimate, why)`` for a cdc-stack delete, engine-aware (see core.cdc).

    A PostgreSQL session (live or restored) has no seeder Lambda; a MySQL one is assumed to
    (the conservative default -- the host seed mode is not read on a render path); an
    unknown engine gets the covering range.
    """
    from dsql_migrator.core.cdc import (
        cdc_teardown_estimate,
        cdc_teardown_reason,
        stack_has_seeder_lambda,
    )

    session = getattr(migration_state, "_session", None)
    seeder: "Optional[bool]" = None
    if session is not None:
        engine = session_source_type(session)
        seeder = None if engine is None else stack_has_seeder_lambda(engine, None)
    return (
        cdc_teardown_estimate(has_seeder_lambda=seeder),
        cdc_teardown_reason(has_seeder_lambda=seeder),
    )


def cdc_stop_in_flight(migration_state, job_manager) -> bool:
    """True while this session's Stop CDC job is PENDING/RUNNING (a wait, not a remedy)."""
    if getattr(migration_state, "cdc_action_kind", None) != "stop":
        return False
    job = _current_job(job_manager, getattr(migration_state, "cdc_deploy_job_id", None))
    return job is not None and job.status in ("PENDING", "RUNNING")


def cdc_operation_in_flight(migration_state, job_manager) -> "Optional[str]":
    """This session's CDC lifecycle operation still running -- ``"delete"`` (a stack the probe
    reads as being deleted counts too), ``"stop"`` or ``"start"`` -- else ``None`` (G132).

    Each makes :func:`cdc_streaming_started` True, so a gate keyed on it holds while one runs;
    this names the operation, so the gate can say to wait for it rather than to Stop CDC
    first. Pure apart from the job reads."""
    if cdc_stack_being_deleted(migration_state, job_manager):
        return "delete"
    if cdc_stop_in_flight(migration_state, job_manager):
        return "stop"
    if getattr(migration_state, "cdc_action_kind", None) != "start":
        return None
    job = _current_job(job_manager, getattr(migration_state, "cdc_deploy_job_id", None))
    return "start" if job is not None and job.status in ("PENDING", "RUNNING") else None


def _stop_then(migration_state, job_manager) -> str:
    """How a streaming-state remedy begins: stop CDC -- or wait for the running Stop."""
    if cdc_stop_in_flight(migration_state, job_manager):
        return "wait for the Stop CDC that is running to finish, then "
    return "Stop CDC, then "


def capture_error_with_remedy(
    migration_state, job_manager, capture, *, table_lock: "Optional[str]" = None
) -> str:
    """A capture-size refusal plus the remedy that is TRUE for its cause and the state.

    If the generated columns Aurora DSQL computes -- added automatically, never the
    operator's choice -- alone exceed the limit, fewer exclusions cannot fix it: capture
    fewer tables per CDC pipeline, or use Full load only. How to capture fewer tables
    depends on the table picker: ``table_lock`` is its own lock reason (a Full Load ran,
    CDC streams, a stack holds the set, ...), whose remedy is appended verbatim; with no
    lock, deselect tables. Otherwise, while the exclusion card is editable: exclude fewer
    columns there; once it is locked, the lock's own cause-and-remedy text.
    """
    from dsql_migrator.core.cdc import CDC_PARAMETER_BYTE_LIMIT
    from dsql_migrator.ui.data_migration._models import (
        CAPTURE_SIZE_REMEDY_UNLOCKED,
        table_picker_remedy,
    )

    if capture.generated_bytes > CDC_PARAMETER_BYTE_LIMIT:
        return (
            f"{capture.error} The generated columns Aurora DSQL computes -- which CDC "
            f"must not stream -- alone need {capture.generated_bytes:,} bytes, so "
            "excluding fewer columns cannot fix it: capture fewer tables in this CDC "
            "pipeline, or use Full load only. "
            + table_picker_remedy(table_lock)
        )
    lock = exclusion_lock_state(migration_state, job_manager)
    return f"{capture.error} {lock.reason if lock.locked else CAPTURE_SIZE_REMEDY_UNLOCKED}"


@dataclass(frozen=True)
class ExclusionLockState:
    """Why the column exclusions are fixed right now, in the ONE strict order.

    ``tier``: 0 the CDC infrastructure is being deleted (info: an expected wait, no
    remedy), 1 it is being created, 2 CDC is streaming or a stack holds the set, 3 a Full
    Load committed data with it and no stack exists; ``None`` = not locked. ``reason`` is the
    whole user-facing text, remedy included; ``tone`` its severity.
    """

    tier: "Optional[int]"
    reason: "Optional[str]" = None
    tone: str = "neutral"
    # The remedy cell (plan LOCK): "R1" a stack + a recorded Full Load, "R1b" a stack + a
    # Full Load whose record is gone, "R2" a stack, no Full Load, CDC only, "R3" a stack,
    # no Full Load, a Full Load type, "R4" no stack + a Full Load; None for tiers 0-1 and
    # unlocked. Which remedy buttons the read-only popup shows follow from it.
    cell: "Optional[str]" = None
    offers_change_and_reload: bool = False
    offers_change_type: bool = False

    @property
    def locked(self) -> bool:
        return self.tier is not None


def exclusion_lock_state(migration_state, job_manager) -> ExclusionLockState:
    """The exclusion lock's cause and its TRUE remedy, checked in strict order.

    Order matters because the naive OR answered with the wrong remedy: the Full Load clause
    (Start over) used to win over a stack that must be deleted first, and a running Delete
    reached the streaming / stack clauses, which then offered that very Delete. So:
    0 deleting > 1 creating > 2 streaming or stack > 3 Full Load committed.

    The remedy then depends on whether a Full Load committed data (``job_id`` set: it
    survives a pruned job record and a type switch; the FULL_LOAD step is not used, since
    CDC alone promotes it to DONE) and on the migration type and engine. It never offers
    "Stop CDC" as a complete remedy (a stopped stack keeps these exclusions and its
    offsets), never a Manual start point as a way around a gap, and names only actions this
    version has. Pure apart from job-status reads; no AWS I/O.
    """
    committed = getattr(migration_state, "job_id", None) is not None
    if cdc_stack_being_deleted(migration_state, job_manager):
        estimate, why = cdc_delete_eta(migration_state)
        return ExclusionLockState(
            0,
            "The CDC infrastructure is being deleted. This is in progress, not stuck: a "
            f"cdc-stack delete takes {estimate} because {why}. Wait for the deletion to "
            "finish before changing these exclusions.",
            tone="info",
        )
    if cdc_infra_deploy_in_flight(migration_state, job_manager):
        from dsql_migrator.ui.data_migration._models import MigrationType as _Type

        _type = getattr(migration_state, "migration_type", None)
        _found = committed and _current_job(
            job_manager, getattr(migration_state, "job_id", None)) is not None
        if _found:
            # The same remedy R1 gives once the create finishes: Change & reload, not
            # Start over (the load's record exists).
            then = (
                "switch the migration type to Full load + CDC and use Change & reload there"
                if _type is _Type.CDC_ONLY
                else "use Change & reload for the affected tables"
            )
            where = (
                "switch the migration type to Full load + CDC and delete it on its CDC step"
                if _type is _Type.FULL_LOAD_ONLY
                else "delete the CDC infrastructure on the CDC step"
            )
            tail = (f"A Full Load also loaded data with them: to change them, wait for the "
                    f"create to finish, {where}, then {then}.")
        elif committed:
            tail = ("A Full Load also loaded data with them, and the record of that run is "
                    "no longer available: to migrate a different column set, wait for the "
                    "create to finish, delete the CDC infrastructure on the CDC step, then "
                    "use Start over.")
        else:
            tail = "Wait for it to finish, then delete it on the CDC step to change them."
        return ExclusionLockState(
            1,
            "Locked while the CDC infrastructure is being created — the excluded columns "
            f"were submitted with it. {tail}",
        )
    from dsql_migrator.ui.data_migration._models import MigrationType

    migration_type = getattr(migration_state, "migration_type", None)
    job_id = getattr(migration_state, "job_id", None)
    job_found = _current_job(job_manager, job_id) is not None if job_id else False
    streaming = cdc_streaming_started(migration_state, job_manager)
    has_streamed = streaming or bool(getattr(migration_state, "cdc_has_committed_offset", False))
    stack = streaming or (
        getattr(migration_state, "cdc_stack_phase", None) in _EXCLUSION_BAKED_PHASES
    )
    session = getattr(migration_state, "_session", None)
    engine = session_source_type(session) if session is not None else None
    # Change & reload re-runs the recorded Full Load for the changed tables, so it is named
    # only while that job record exists (a pruned record leaves Start over alone).
    reseed = _delete_reseed_note(engine)
    if stack:
        held = (
            "Locked — CDC is being stopped, and its source connector was started with "
            "these excluded columns."
            if streaming and cdc_stop_in_flight(migration_state, job_manager)
            else "Locked — CDC is streaming, and its source connector was started with "
            "these excluded columns."
            if streaming
            else "Locked — the CDC infrastructure is deployed with these excluded columns."
        )
        delete = (
            "delete the CDC infrastructure on the CDC step (Stop CDC alone is not enough: a "
            "stopped stack keeps these exclusions and its offsets)"
            if migration_type is not MigrationType.FULL_LOAD_ONLY
            else "switch the migration type to Full load + CDC and delete the CDC "
            "infrastructure on its CDC step (Full load only has no CDC step to delete it "
            "from)"
        )
        if streaming:
            delete = _stop_then(migration_state, job_manager) + delete
        full_load_only = migration_type is MigrationType.FULL_LOAD_ONLY
        if job_id is not None and not job_found:
            # R1b: the load's record is gone, so nothing can be reloaded: Start over.
            return ExclusionLockState(
                2,
                f"{held} A Full Load also loaded data with them, so they are fixed for this "
                f"migration. To migrate a different column set, {delete}, then use Start "
                "over.",
                cell="R1b",
                offers_change_type=full_load_only,
            )
        if job_id is not None:
            # R1: delete, then Change & reload the tables whose excluded columns change.
            if migration_type is MigrationType.CDC_ONLY:
                reload = ("switch the migration type to Full load + CDC, use Change & reload "
                          "there, then continue with CDC (switch back to CDC only, or stay "
                          "in Full load + CDC)")
                then = "deploy it again and Start CDC"
            elif full_load_only:
                reload = "use Change & reload for the affected tables"
                then = ("only if you continue with CDC: deploy it again and Start CDC "
                        "(otherwise switch back to Full load only)")
            else:
                reload = "use Change & reload for the affected tables"
                then = "deploy it again and Start CDC"
            # G16/G17: a Full load + CDC stack that never streamed still lets the table list
            # change the set (the popup is read-only here, so the list is the way to do it).
            narrowing = (
                R1_FL_CDC_SENTENCE
                if migration_type is MigrationType.FULL_LOAD_AND_CDC
                and getattr(migration_state, "cdc_stack_phase", None) == "infra"
                and table_set_editable_after_load(migration_state, job_manager)
                else ""
            )
            return ExclusionLockState(
                2,
                f"{held} A Full Load also loaded data with them. To change them: 1 {delete}, "
                f"2 {reload}, 3 {then}.{reseed}{narrowing}",
                cell="R1",
                offers_change_type=full_load_only,
            )
        if migration_type is MigrationType.CDC_ONLY:
            # R2: no Full Load; the delete takes the stream position with it.
            if engine is SourceType.POSTGRES:
                cost = (
                    " The next Start CDC re-creates the publication and snapshots the "
                    "captured tables again; a row already on Aurora DSQL that the source "
                    "has since deleted stays there."
                )
            elif has_streamed:
                cost = (
                    " Deleting it also deletes the stream position and the MSK topics: the "
                    "next Start CDC on Automatic begins at the source's current position, so changes to "
                    "the captured tables after the last one Aurora DSQL applied are NOT "
                    "replicated. If they were written in that window, re-copy them: after "
                    "the delete, switch the migration type to Full load + CDC and run its "
                    "Full Load (choosing to drop and recreate the tables that hold rows) "
                    "before the new stack's first Start CDC: once that stack has streamed, a "
                    "new Full Load start point needs another Delete and redeploy."
                )
            else:
                cost = (
                    " Start CDC on Automatic then begins at the source's current position, "
                    "as the first Start would have."
                )
            # CDC promotes the FULL_LOAD step to DONE while its pipeline is live, and that
            # keeps the table set locked after the Delete: only the column rules can
            # change then. A stopped stack found after a Start over did not promote it.
            from dsql_migrator.core.models import StepStatus
            from dsql_migrator.ui.workflow import WorkflowStep, get_status

            workflow = getattr(session, "workflow", None)
            table_set_locked = workflow is not None and get_status(
                workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
            change = (
                "change the column rules (changing the table set needs Start over)"
                if table_set_locked
                else "change the excluded columns here"
            )
            return ExclusionLockState(
                2,
                f"{held} To change them, {delete}, {change}, then deploy it again and Start "
                f"CDC.{cost} Rows already on Aurora DSQL keep their values in newly excluded "
                "columns.",
                cell="R2",
            )
        # R3: a Full Load type, no Full Load has run yet.
        again = (
            "deploy it again (it can overlap the load), then Start Full Load"
            if migration_type is not MigrationType.FULL_LOAD_ONLY
            else "switch back to Full load only, then Start Full Load"
        )
        rows = (
            " The captured tables already hold rows: choose to drop and recreate them in "
            "the Start Full Load dialog."
            if has_streamed
            else ""
        )
        return ExclusionLockState(
            2,
            f"{held} To change them, {delete}, change the excluded columns, then {again}. "
            f"No Full Load has run, so nothing needs reloading.{rows}",
            cell="R3",
            offers_change_type=full_load_only,
        )
    if job_id is not None:
        # R4: no stack, a Full Load committed data with this set.
        if not job_found:
            return ExclusionLockState(
                3,
                "Locked — a Full Load has already loaded data using this exclusion set, and "
                "the record of that run is no longer available. To migrate a different "
                "column set, use Start over.",
                cell="R4",
            )
        cdc_only = migration_type is MigrationType.CDC_ONLY
        if cdc_only:
            return ExclusionLockState(
                3,
                "Locked — a Full Load already loaded these tables with these exclusions, "
                "before this migration was switched to CDC only, and CDC only has no Full "
                "Load step to reload them on. To change them: 1 switch the migration type to "
                "Full load + CDC, 2 open this window there and use Change & reload, "
                "3 continue with CDC (switch back to CDC only, or stay in Full load + CDC).",
                cell="R4",
                offers_change_type=True,
            )
        if migration_type is MigrationType.FULL_LOAD_AND_CDC and table_set_editable_after_load(
            migration_state, job_manager
        ):
            # G16/G17: Full load + CDC, before CDC first starts, can still drop and add tables
            # (an added one loads under the guards first); the column set stays fixed
            # (Change & reload). Once CDC has started, the Start over text at the end applies
            # again.
            return ExclusionLockState(
                3, R4_FL_CDC_NARROWING, cell="R4", offers_change_and_reload=True
            )
        if migration_type is MigrationType.FULL_LOAD_ONLY and table_set_editable_after_load(
            migration_state, job_manager
        ):
            # G12: Full load only keeps the column set fixed (Change & reload) but lets the
            # TABLE set change in both directions.
            return ExclusionLockState(
                3,
                "Locked — a Full Load has already loaded data using this exclusion set. To "
                "change the excluded columns, use Change & reload: it drops and reloads "
                "exactly the tables whose excluded columns change, so no row keeps a value "
                "loaded under the previous set. In Full load only, which tables migrate can "
                "still change: tick or untick them in the table list, or use a table line in "
                "Change & reload. A table you unselect is left as is on Aurora DSQL (not "
                "dropped); a table you select that is not loaded yet is loaded by Retry "
                "unfinished tables on the Full Load step (one already loaded is kept as "
                "is).",
                cell="R4",
                offers_change_and_reload=True,
            )
        return ExclusionLockState(
            3,
            "Locked — a Full Load has already loaded data using this exclusion set. To "
            "change the excluded columns, use Change & reload: it drops and reloads exactly "
            "the tables whose excluded columns change, so no row keeps a value loaded under "
            "the previous set. Changing the table set needs Start over.",
            cell="R4",
            offers_change_and_reload=True,
        )
    return ExclusionLockState(None)


def fk_apply_running(migration_state, job_manager) -> bool:
    """Is the foreign-key pass's OWN job still running?

    Reads ``fk_apply_job_id``, never ``job_id`` -- that slot belongs to the Full Load, and
    writing this job into it is what erased the load's per-table rows and export watermark.
    """
    jid = getattr(migration_state, "fk_apply_job_id", None)
    if not jid or getattr(migration_state, "fk_apply_result", None) is not None:
        return False
    job = _current_job(job_manager, jid)
    return job is not None and job.status in ("PENDING", "RUNNING")


RECONNECT_REASON = (
    "Reconnect first: re-verify the source and target on the Connect step (credentials "
    "are not restored after a restart)."
)

# Full load only lifts the table-set lock after a Full Load (G12), and Full load + CDC lets it
# change its table set (drop tables, or add one under the guards) until CDC first starts
# (G16, G17) -- but never while one of these runs: each is a WAIT (it ends by itself), so
# none of them says "Start over".
TABLE_SET_WAIT_LOAD = (
    "Locked while a Full Load runs — wait for it to finish (or stop it), then change the "
    "table set."
)
TABLE_SET_WAIT_FK = (
    "Locked while foreign keys are being applied — wait for that pass to finish (or stop "
    "it), then change the table set."
)
TABLE_SET_WAIT_RESTORE = (
    "Locked while Cut over restores the excluded columns' DEFAULTs — wait for it to "
    "finish, then change the table set."
)
# Full load + CDC only (G16): the CDC side of the same waits.
TABLE_SET_WAIT_INFRA = (
    "Locked while the CDC infrastructure is being created — wait for it to finish (its "
    "progress is on the CDC step), then change the table set."
)
TABLE_SET_WAIT_CDC_CHECK = (
    "Locked until the CDC state is read — Full load + CDC can change its table set only "
    "while CDC has not started for this migration, and this session has not confirmed that "
    "yet. This usually clears within seconds; if it does not, open the CDC step and use "
    "Re-check CDC state."
)
TABLE_SET_WAIT_CDC_UNREAD = (
    "Locked — the CDC state could not be read, so the tool cannot confirm that CDC never "
    "started for this migration. The CDC step shows the error: fix the AWS access, use "
    "Re-check CDC state there, then change the table set."
)

# A cdc-stack whose CREATE failed (or is rolling back) never streamed: no connector, no
# committed offset, no started table list. Kept apart from the other non-stable statuses
# (an UPDATE_* or DELETE_FAILED stack may carry a started set).
_FAILED_CREATE_STATUSES = frozenset(
    {"CREATE_FAILED", "ROLLBACK_IN_PROGRESS", "ROLLBACK_COMPLETE", "ROLLBACK_FAILED"}
)


def table_set_failed_create_wait(status) -> str:
    """The table-set wait while a failed CDC infrastructure create is still in the account."""
    return (
        f"Locked — the CDC infrastructure's create failed ({status}), so it never streamed. "
        "Delete it on the CDC step (once any rollback has finished), then change the table "
        "set."
    )


def table_set_delete_wait(migration_state) -> str:
    """The table-set wait while the CDC infrastructure is being deleted (an expected wait,
    never a remedy): the same text the streaming clause of ``selection_lock_reason`` gives."""
    estimate, _why = cdc_delete_eta(migration_state)
    return (
        f"Locked while the CDC infrastructure is being deleted (a cdc-stack delete takes "
        f"{estimate}). Wait for the deletion to finish before changing the table set."
    )


def _cdc_started_for_migration(migration_state, job_manager) -> bool:
    """Has CDC started for this migration -- now or at any time before (G16)?

    Any of: a Start CDC was submitted (``cdc_ops_window_start``: set at every submission,
    restored with the session and never cleared by Stop or Delete -- the durable marker);
    connectors are recorded; the probed stack has a committed offset (it streamed, then
    stopped); or a connector-level lifecycle job (start / stop) is in flight or the pipeline
    is live. A Delete in flight alone is not evidence (it removes CDC). Conservative: a
    Start the worker refused still counts. Pure apart from job reads.
    """
    if getattr(migration_state, "cdc_ops_window_start", None) is not None:
        return True
    if getattr(migration_state, "cdc_connector_names", None):
        return True
    if getattr(migration_state, "cdc_has_committed_offset", False):
        return True
    return cdc_streaming_started(migration_state, job_manager) and not cdc_stack_being_deleted(
        migration_state, job_manager
    )


# ``job=`` default of the helpers below: read the Full Load record here.
_JOB_UNSET = object()


def _fl_cdc_narrowing_job(migration_state, job_manager, *, job=_JOB_UNSET):
    """The Full Load record while Full load + CDC's narrowing is open (G16), else ``None``.

    Open when CDC never started for this migration (:func:`_cdc_started_for_migration`), the
    probed stack is absent, never-started infra, or a create / delete / failed create (each
    a wait) -- never one that carries a started set (running, unstable UPDATE_*,
    DELETE_FAILED, ...) -- and the record is found and either still runs (the load wait) or
    carries per-table watermark counts (the covered set the narrowing and the per-table
    add-lock are judged against). An unprobed stack (phase ``None``) is open here and
    reported as a wait by :func:`table_set_wait_reason`. The CDC state is checked before
    the record is read: a read is a deep copy of the whole record, and a started migration
    (every render once CDC runs) needs none. ``job`` is the record when the caller already
    holds it (``None`` = no record); omitted, it is read here.
    """
    if _cdc_started_for_migration(migration_state, job_manager):
        return None
    phase = getattr(migration_state, "cdc_stack_phase", None)
    if phase not in (None, "absent", "infra"):
        if phase != "unstable":
            return None
        status = str(getattr(migration_state, "cdc_stack_phase_status", None) or "").upper()
        if not (
            cdc_infra_deploy_in_flight(migration_state, job_manager)
            or is_infra_create_stack_status(status)
            or cdc_stack_being_deleted(migration_state, job_manager)
            or status in _FAILED_CREATE_STATUSES
        ):
            return None
    if job is _JOB_UNSET:
        job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    if job is None:
        return None
    if getattr(job, "status", None) in ("PENDING", "RUNNING"):
        return job
    if not getattr(getattr(job, "watermark", None), "table_row_counts", None):
        return None
    return job


def _fl_cdc_narrowing_open(migration_state, job_manager) -> bool:
    """Full load + CDC's half of :func:`table_set_editable_after_load` (G16): see
    :func:`_fl_cdc_narrowing_job`."""
    return _fl_cdc_narrowing_job(migration_state, job_manager) is not None


def table_set_editable_after_load(migration_state, job_manager) -> bool:
    """Can the table set still change although a Full Load has run?

    The ONE predicate behind the tree, the popup's E3 and the R4 text, so they cannot
    disagree. Per migration type:

    * **Full load only** (G12, both directions) -- True when the Full Load's job record is
      found (a pruned record cannot be reconciled with a changed set: its chunk list is
      what Retry / Reload / the step verdict read) and there is no CDC evidence: nothing
      streaming, starting, being created or deleted, no stack phase that carries the set,
      and no connector recorded for this migration.
    * **Full load + CDC** (G16, G17) -- True until CDC first starts for this migration
      (:func:`_fl_cdc_narrowing_open`): tables can then be dropped, and any migratable
      table ticked except one an exclude table rule holds (:func:`table_set_add_lock`); a
      table the Full Load did not load is loaded by Retry unfinished tables under its
      watermark (a MySQL one with no start-point definition recorded, only by Re-run all
      tables) and only then captured (:func:`fl_cdc_pending_additions`). Once CDC has
      started, even if stopped since, the set is fixed again.
    * **CDC only** -- always False: it has no Full Load step to scope; switching to Full
      load + CDC is the way to change the set there.

    A running load, foreign-key pass or DEFAULT restore -- and in Full load + CDC an infra
    create or delete, a failed create, or a CDC state not read yet -- does NOT make this
    False: those are waits, reported by :func:`table_set_wait_reason`. Pure apart from job
    reads.
    """
    from dsql_migrator.ui.data_migration._models import MigrationType

    migration_type = getattr(migration_state, "migration_type", None)
    if migration_type is MigrationType.FULL_LOAD_AND_CDC:
        return _fl_cdc_narrowing_open(migration_state, job_manager)
    if migration_type is not MigrationType.FULL_LOAD_ONLY:
        return False
    if _current_job(job_manager, getattr(migration_state, "job_id", None)) is None:
        return False
    if cdc_streaming_started(migration_state, job_manager):
        return False
    if cdc_infra_deploy_in_flight(migration_state, job_manager):
        return False
    if cdc_stack_being_deleted(migration_state, job_manager):
        return False
    if getattr(migration_state, "cdc_stack_phase", None) in _EXCLUSION_BAKED_PHASES:
        return False
    return not (getattr(migration_state, "cdc_connector_names", None) or [])


def table_set_wait_reason(migration_state, job_manager, *, status) -> "Optional[str]":
    """While :func:`table_set_editable_after_load` holds, why the set must wait, or None.

    First match: a Full Load (or retry / reload) running, the foreign-key pass running,
    cut over restoring the excluded columns' DEFAULTs. Then, in Full load + CDC only (G16):
    the CDC infrastructure being deleted, being created, or left behind by a failed create,
    and -- until the probe reports -- the CDC state not read yet (the committed offset and
    the deployed table list are only known then), or unreadable.
    """
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration._models import (
        MigrationType,
        cutover_restore_block_reason,
    )

    job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    if status is StepStatus.IN_PROGRESS or (
        job is not None and job.status in ("PENDING", "RUNNING")
    ):
        return TABLE_SET_WAIT_LOAD
    if fk_apply_running(migration_state, job_manager):
        return TABLE_SET_WAIT_FK
    if cutover_restore_block_reason(migration_state):
        return TABLE_SET_WAIT_RESTORE
    if getattr(migration_state, "migration_type", None) is not MigrationType.FULL_LOAD_AND_CDC:
        return None
    if cdc_stack_being_deleted(migration_state, job_manager):
        return table_set_delete_wait(migration_state)
    phase_status = getattr(migration_state, "cdc_stack_phase_status", None)
    if cdc_infra_deploy_in_flight(migration_state, job_manager) or is_infra_create_stack_status(
        phase_status
    ):
        return TABLE_SET_WAIT_INFRA
    if (
        getattr(migration_state, "cdc_stack_phase", None) == "unstable"
        and str(phase_status or "").upper() in _FAILED_CREATE_STATUSES
    ):
        return table_set_failed_create_wait(phase_status)
    if not getattr(migration_state, "cdc_stack_phase_checked", False):
        return (
            TABLE_SET_WAIT_CDC_UNREAD
            if getattr(migration_state, "cdc_probe_error", None)
            else TABLE_SET_WAIT_CDC_CHECK
        )
    return None


# --- G16/G17: the per-table add-lock of a Full load + CDC set changed after its Full Load --

# The lock-icon tooltip on a tree leaf that cannot be ticked now (static: a client-side
# slot). Since G17 an exclude table rule is the only per-table reason: a table the Full Load
# did not load may be ticked, and its guarded load decides whether CDC can capture it.
LOCK_TIP_RULE = (
    "Unselected by an exclude table rule — delete the rule in Change & reload to select it "
    "again."
)
# G137: the lock-icon tooltip on a TICKED leaf that cannot be unticked now -- a table the Full
# Load read in parts (MySQL), whose copy is consistent only once CDC replays it: only a reload
# with one reader that recreates it lets it be unticked (a CHECKSUM match proves the replay,
# but CDC's first Start fixes the table set for good: SPLIT_SET_FIXED).
LOCK_TIP_SPLIT = (
    "Read in parts by the Full Load (Reader shards per large table), each part from its own "
    "snapshot: its copy is consistent only once CDC replays it from the Full Load's start "
    "point, so it stays selected until it is reloaded with one reader. To untick it, set "
    "Reader shards per large table to 1 in Settings and reload it first (Retry unfinished "
    "tables while it is unfinished, else Reload specific tables: with one reader either drops "
    "and recreates it). " + SPLIT_SET_FIXED
)
# How a table added after the Full Load gets loaded (G17). Retry unfinished tables loads it
# under the Full Load's start point -- except a MySQL table that Full Load recorded no
# start-point definition for (a Full Load from an earlier version, a table converted after
# the load), which only Re-run all tables loads: the Full Load step says which, per table.
ADDED_TABLE_LOAD_ROUTE = (
    "Retry unfinished tables, or Re-run all tables where that step says Retry cannot add it"
)
TREE_HINT_FL_CDC = (
    "A Full Load has run. Until CDC starts, a table you untick is left as is on Aurora DSQL "
    "(not dropped) and CDC does not capture it; a table you tick that this Full Load did not "
    f"load is loaded on the Full Load step first ({ADDED_TABLE_LOAD_ROUTE}), and CDC then "
    "captures it with the other tables. A lock marks a table an exclude table rule "
    "unselected."
)
# Appended while the Full Load read a table in parts (G137, LOCK_TIP_SPLIT).
TREE_HINT_SPLIT = (
    " A lock on a ticked table marks one the Full Load read in parts: it stays selected "
    "until it is reloaded with one reader (Reader shards per large table set to 1, then Drop "
    "& reload). " + SPLIT_SET_FIXED
)
# Appended to CDC only's table-set lock while Full load + CDC could still change the set
# (CDC never started for this migration and the type may change): the non-destructive
# route.
CDC_ONLY_NARROWING_SENTENCE = (
    " To change the table set before CDC starts, switch the migration type to Full load + "
    "CDC and change it there (a table you add is loaded there first)."
)
# The Exclude tables & columns texts of the same state.
R4_FL_CDC_NARROWING = (
    "Locked — a Full Load has already loaded data using this exclusion set. To change the "
    "excluded columns, use Change & reload: it drops and reloads exactly the tables whose "
    "excluded columns change, so no row keeps a value loaded under the previous set. In Full "
    "load + CDC, until CDC starts, tables can still be dropped from or added to this "
    "migration: untick or tick them in the table list, or use a table line in Change & "
    "reload. A table you unselect is left as is on Aurora DSQL (not dropped) and CDC does "
    "not capture it; a table the Full Load did not load is loaded on the Full Load step "
    f"first ({ADDED_TABLE_LOAD_ROUTE}), and CDC then captures it with the other tables."
)
R1_FL_CDC_SENTENCE = (
    " Until CDC starts, the table list can still change this migration's tables: CDC does "
    "not capture a table you untick (it is left as is on Aurora DSQL), and captures a table "
    f"you tick once it is loaded on the Full Load step ({ADDED_TABLE_LOAD_ROUTE})."
)
# Appended to the Full Load step's "Retry the failed tables before starting CDC" hint.
CONTINUE_HINT_FL_CDC = (
    " Or untick them in the table list: a table you untick is left as is on Aurora DSQL "
    "(not dropped) and CDC does not capture it."
)


def _delete_reseed_note(engine) -> str:
    """What deleting the CDC infrastructure costs the next Start, per engine ("" unknown)."""
    if engine is SourceType.POSTGRES:
        return (" PostgreSQL: deleting the stack drops the replication slot and the "
                "publication, so Start CDC cannot resume from the load's slot: choose "
                "\"Re-snapshot every table instead\" in its dialog (it reads every "
                "captured table again).")
    if engine is SourceType.MYSQL:
        return (" MySQL: Start CDC on Automatic re-seeds from the Full Load watermark (it "
                "needs the binlog kept since that load; Start CDC warns if it was purged).")
    return ""


def _refuse_rule(names: "list[str]") -> str:
    one = len(names) == 1
    its = "its" if one else "their"
    them = "it" if one else "them"
    return (
        f"An exclude table rule unselected {_bounded_names(names)}, and that Save dropped "
        f"{its} excluded-column entries: ticked here, CDC would stream values for columns "
        f"{its} loaded rows hold as NULL. To select {them} again, delete that rule in "
        f"Exclude tables & columns (Change & reload), which applies {its} column rules "
        "again."
    )


def _refuse_rule_unloaded(names: "list[str]") -> str:
    """The rule refusal for a table the Full Load did not load (G17): it has no loaded rows
    holding NULL -- only its column rules would not apply."""
    one = len(names) == 1
    its = "its" if one else "their"
    them = "it" if one else "them"
    return (
        f"An exclude table rule unselected {_bounded_names(names)}, and that Save dropped "
        f"{its} excluded-column entries: ticked here, {'it' if one else 'they'} would load "
        f"and stream every column, including the ones {its} column rules exclude. To select "
        f"{them}, delete that rule in Exclude tables & columns (Change & reload), which "
        f"applies {its} column rules."
    )


def _bounded_names(names: "list[str]") -> str:
    from dsql_migrator.ui.cdc_dlq_gate import bounded_table_names

    return bounded_table_names(names)


# The per-table codes (since G17 only an exclude table rule refuses a tick); a refusal
# names the codes in this order.
_ADD_LOCK_REFUSALS = (("rule", _refuse_rule),)


@dataclass(frozen=True)
class TableSetAddLock:
    """Which tables a Full load + CDC set changed after its Full Load may be ticked (G16,
    G17) -- ONE helper for the tree (lock icons, a tick, Select all), the popup's E3 and the
    Save, so they cannot disagree.

    ``covered`` -- the tables CDC may capture from the watermark's start point
    (:func:`~dsql_migrator.ui.data_migration._models.cdc_covered_names`: the run's tables
    plus the guarded additions). Any migratable table may be ticked (G17): one the Full Load
    did not load is pending until Retry unfinished tables loads it under the guards, and
    only then joins this set (a MySQL one with no start-point definition recorded can never
    join it: only Re-run all tables, a fresh watermark, loads it). A covered table may be
    ticked again even once a never-started
    CDC stack is deployed without it: the stack has no topics yet, and Start CDC re-sends
    the table list and creates each captured table's topic with the partition count the
    stack was deployed with. ``rule_held`` -- tables an exclude table rule unselected: that
    Save dropped their excluded-column entries, so a TREE tick would let a covered table's
    CDC stream columns its loaded rows hold as NULL, or load and stream an uncovered one
    with every column (deleting the rule in Change & reload applies the column rules, so the
    popup is not refused for it). ``covered`` also keeps the stored tables a write cannot
    list, and splits the rule refusal. ``split_held`` (G137) -- tables the Full Load read in
    parts, each from its own snapshot (MySQL, ``_models.split_read_tables``: until a CHECKSUM
    match or a recreate clears the record): their copy is consistent only once CDC replays
    them, so neither the tree nor a table rule may UNSELECT one. ``split_unfinished`` (the
    held tables that did not finish) and ``others_open`` (another table did not finish), both
    over the view the Full Load step renders (``_models.split_route_view``), word the
    refusal's route by the control that step shows (``_models.split_reload_route``).
    Hashable (a popup memo key).
    """

    covered: frozenset
    rule_held: frozenset = frozenset()
    split_held: frozenset = frozenset()
    split_unfinished: frozenset = frozenset()
    others_open: bool = False

    def code(self, name: str, *, tree: bool) -> "Optional[str]":
        """Why ``name`` cannot be ticked now -- ``"rule"`` (tree only) -- or ``None`` when
        it can."""
        if tree and name in self.rule_held:
            return "rule"
        return None

    def removal_blocked(self, names: "Iterable[str]") -> "list[str]":
        """The ``names`` this lock keeps from being unselected (G137: read in parts), in
        input order (once each)."""
        return [n for n in dict.fromkeys(names or ()) if n in self.split_held]

    def removal_refusal(self, names: "Iterable[str]", *, tree: bool) -> "Optional[str]":
        """The refusal of unselecting the refused ``names`` (``tree``: an untick, else a table
        rule's unselect), or ``None`` when none is refused. Only the reload route: a CHECKSUM
        match waits for CDC's replay, and CDC's first Start fixes the table set for good
        (``checksum_way_out=False``, ``_models.SPLIT_SET_FIXED``)."""
        from dsql_migrator.ui.data_migration._models import split_read_refusal

        blocked = self.removal_blocked(names)
        if not blocked:
            return None
        it = "it" if len(blocked) == 1 else "them"
        verb = "untick" if tree else "unselect"
        mine = frozenset(blocked)
        return split_read_refusal(
            blocked, operation=f"{verb.capitalize()}ing {it}", then=f"{verb} {it}",
            # Another table unfinished: a held one outside this refusal, or any other.
            states=(
                self.split_unfinished & mine,
                self.others_open or bool(self.split_unfinished - mine),
            ),
            checksum_way_out=False,
        )

    def blocked(self, names: "Iterable[str]", *, tree: bool) -> "list[str]":
        """The ``names`` this lock refuses, in input order (once each)."""
        return [n for n in dict.fromkeys(names or ()) if self.code(n, tree=tree) is not None]

    def refusal(self, names: "Iterable[str]", *, tree: bool) -> "Optional[str]":
        """The per-reason refusal for the refused ``names`` (their names bounded), or
        ``None`` when none is refused."""
        by_code: "dict[str, list[str]]" = {}
        for name in dict.fromkeys(names or ()):
            code = self.code(name, tree=tree)
            if code is not None:
                by_code.setdefault(code, []).append(name)
        if not by_code:
            return None
        parts = []
        for code, build in _ADD_LOCK_REFUSALS:
            if code not in by_code:
                continue
            covered = [n for n in by_code[code] if n in self.covered]
            uncovered = [n for n in by_code[code] if n not in self.covered]
            if covered:
                parts.append(build(covered))
            if uncovered:
                parts.append(_refuse_rule_unloaded(uncovered))
        return " ".join(parts)

    def node_flags(self, listed: "Iterable[str]", ticked: "Iterable[str]") -> "dict[str, str]":
        """``{table: code}`` for the ``listed`` tables whose checkbox cannot change now: an
        UNTICKED one that cannot be ticked (its add-lock code), and a TICKED one read in
        parts (``"split"``, G137), which cannot be unticked -- any other ticked table can."""
        ticked_set = set(ticked or ())
        flags: "dict[str, str]" = {}
        for name in listed or ():
            if name in ticked_set:
                if name in self.split_held:
                    flags[name] = "split"
                continue
            code = self.code(name, tree=True)
            if code is not None:
                flags[name] = code
        return flags


def table_set_add_lock(
    migration_state, job_manager, *, job=_JOB_UNSET
) -> "Optional[TableSetAddLock]":
    """The per-table add-lock while a Full load + CDC set may change after its Full Load
    (G16, G17), or ``None`` (every other type and state: Full load only adds freely, and a
    locked set needs no per-table answer). Requires per-table watermark counts -- the
    covered set (:func:`~dsql_migrator.ui.data_migration._models.cdc_covered_names`) the
    lock keeps and splits by. ``job`` -- see :func:`_fl_cdc_narrowing_job`. Pure apart from
    one job read; no AWS I/O."""
    from dsql_migrator.ui.data_migration._models import (
        MigrationType,
        cdc_covered_names,
        split_read_tables,
        split_route_states,
        split_route_view,
    )

    if getattr(migration_state, "migration_type", None) is not MigrationType.FULL_LOAD_AND_CDC:
        return None
    record = _fl_cdc_narrowing_job(migration_state, job_manager, job=job)
    covered = cdc_covered_names(getattr(record, "watermark", None))
    if covered is None:
        return None
    getter = getattr(migration_state, "rule_excluded_tables", None)
    rule_held = frozenset(getter()) if callable(getter) else frozenset()
    # G137: the tables read in parts off the record; their route by the view the Full Load
    # step renders (its buttons).
    split = split_read_tables(record)
    unfinished, others_open = (
        split_route_states(split, split_route_view(migration_state, job_manager, record))
        if split
        else (frozenset(), False)
    )
    return TableSetAddLock(
        covered=covered,
        rule_held=rule_held,
        split_held=frozenset(split),
        split_unfinished=unfinished,
        others_open=others_open,
    )


# --- G17: the tables a Full load + CDC set gained after its Full Load ---------------------


def _uncovered(migration_state, selected, job) -> "list[str]":
    """``selected`` names -- also STORED in the touched selection -- the record's watermark
    does not cover (selection order), or [] without a touched selection or per-table
    counts. Pure, O(tables)."""
    from dsql_migrator.ui.data_migration._models import cdc_covered_names

    if not getattr(migration_state, "selection_touched", False):
        return []
    covered = cdc_covered_names(getattr(job, "watermark", None))
    if covered is None:
        return []
    stored = set(
        getattr(getattr(migration_state, "selection", None), "selected_tables", None) or ()
    )
    return [
        n for n in dict.fromkeys(selected or ()) if n and n in stored and n not in covered
    ]


def cdc_uncovered_selection(migration_state, job_manager, selected, *, job) -> "list[str]":
    """The selected tables CDC would NOT capture although they are selected, because the
    watermark does not cover them yet (G17) -- in Full load + CDC and CDC only, with a
    touched selection, per-table counts on the record ``job`` and CDC never started for the
    migration; otherwise []. ``selected`` is the resolved selection (stored and migratable).
    Start CDC refuses while it is non-empty. Pure apart from CDC-state job reads."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    if getattr(migration_state, "migration_type", None) not in (
        MigrationType.FULL_LOAD_AND_CDC,
        MigrationType.CDC_ONLY,
    ):
        return []
    names = _uncovered(migration_state, selected, job)
    if not names or _cdc_started_for_migration(migration_state, job_manager):
        return []
    return names


def fl_cdc_pending_additions(migration_state, job_manager, selected, *, job) -> "list[str]":
    """The tables a Full load + CDC set gained after its Full Load that are still to load
    under its watermark (G17): :func:`cdc_uncovered_selection` while the narrowing is open
    (:func:`_fl_cdc_narrowing_job`, the record ``job`` in hand); [] in every other type and
    once CDC has started (the view is removal-only again). Each is unfinished on the Full
    Load step -- whatever its record chunk says -- until its guarded load extends the
    coverage. Pure apart from CDC-state job reads."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    if getattr(migration_state, "migration_type", None) is not MigrationType.FULL_LOAD_AND_CDC:
        return []
    names = _uncovered(migration_state, selected, job)
    if not names or _fl_cdc_narrowing_job(migration_state, job_manager, job=job) is None:
        return []
    return names


def cdc_addition_block_reason(
    migration_state, job_manager, names: "Sequence[str]", *, job=_JOB_UNSET
) -> "Optional[str]":
    """Why Retry unfinished tables cannot load ``names`` -- tables the Full load + CDC
    watermark does not cover yet -- now, or ``None`` (G17; at the click, no AWS I/O).

    CDC started for the migration (even if stopped since) closes the route; a CDC
    infrastructure delete in flight, and a CDC state not read yet (or unreadable), are
    waits -- the guard needs to know CDC never started. An infrastructure create in flight
    and a failed create do not block: they touch no source object."""
    names = [n for n in dict.fromkeys(names or ()) if n]
    if not names:
        return None
    listed = _bounded_names(names)
    one = len(names) == 1
    them = "it" if one else "them"
    closed = (
        f"{listed} {'was' if one else 'were'} selected after the Full Load, and CDC has "
        f"started for this migration since, so CDC cannot capture {them} from the Full "
        f"Load's start point and {'it is' if one else 'they are'} not loaded. To migrate "
        f"{them} too, use Start over."
    )
    if _cdc_started_for_migration(migration_state, job_manager):
        return closed
    if cdc_stack_being_deleted(migration_state, job_manager):
        return (
            f"Retry unfinished tables cannot load {listed} while the CDC infrastructure is "
            "being deleted. Wait for the deletion to finish, then retry."
        )
    if not getattr(migration_state, "cdc_stack_phase_checked", False):
        if getattr(migration_state, "cdc_probe_error", None):
            return (
                f"Retry unfinished tables cannot load {listed}: the CDC state could not be "
                "read, so the tool cannot confirm that CDC never started for this "
                "migration. The CDC step shows the error: fix the AWS access, use Re-check "
                "CDC state there, then retry."
            )
        return (
            f"Retry unfinished tables cannot load {listed} yet: the CDC state has not been "
            "read, so the tool cannot confirm that CDC never started for this migration. "
            "This usually clears within seconds; if it does not, open the CDC step and use "
            "Re-check CDC state."
        )
    if _fl_cdc_narrowing_job(migration_state, job_manager, job=job) is None:
        return closed
    return None


def change_and_reload_block_reason(
    migration_state, job_manager, *, connections_ready: bool
) -> "Optional[str]":
    """Why Change & reload cannot run now, or ``None``: the ONE gate for every entry point
    (the read-only popup's remedy, the quarantine route, the re-check at Save), evaluated
    again at CLICK time. The first match of:

    a. the lock's tiers 0-2 (its own reason, so a tier above always wins);
    b. CDC only with a recorded Full Load (no Full Load step to reload on);
    c. no job record: none submitted (``job_id`` None), or the record is gone;
    d. that Full Load is still pending or running;
    e. the foreign-key pass is running;
    f. cut over is restoring the excluded columns' DEFAULTs;
    g. not reconnected (the reload needs the source).
    """
    from dsql_migrator.ui.data_migration._models import MigrationType

    lock = exclusion_lock_state(migration_state, job_manager)
    if lock.tier in (0, 1, 2):
        return lock.reason
    job_id = getattr(migration_state, "job_id", None)
    if job_id is not None and getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY:
        return (
            "A CDC only migration has no Full Load step to reload on. Switch the migration "
            "type to Full load + CDC, use Change & reload there, then continue with CDC."
        )
    if job_id is None:
        return (
            "No Full Load run is recorded in this session, so there is nothing to reload. "
            "Close this dialog: with no CDC stack, the column rules can be edited directly."
        )
    job = _current_job(job_manager, job_id)
    if job is None:
        return (
            "The record of the last Full Load run is no longer available, so there is "
            "nothing to reload from. Use Start over to change exclusions."
        )
    if job.status in ("PENDING", "RUNNING"):
        return (
            "A Full Load is running with the current exclusions. Wait for it to finish (or "
            "Stop it), then use Change & reload."
        )
    if fk_apply_running(migration_state, job_manager):
        return (
            "Foreign keys are being applied to these tables. Wait for that pass to finish "
            "(or stop it), then use Change & reload."
        )
    restoring = getattr(migration_state, "cutover_restore_in_flight", None)
    if callable(restoring) and restoring():
        return (
            "Cut over is restoring the excluded columns' DEFAULTs. Wait for it to finish, "
            "then use Change & reload."
        )
    if not connections_ready:
        return RECONNECT_REASON
    return None
