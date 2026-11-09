# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Step 5 (Cut over) screen of the five-step migration workflow.

Cut over is the one step the tool does not perform: repointing the application from the
source database to Aurora DSQL is the operator's act. :func:`build_cutover_screen` shows the
runbook the last Validation verdict releases (or what holds it), records the operator's
acknowledgement, and runs the runbook's actions in the background: the identity-sequence sync
(:func:`_run_cutover_identity_sync`), the foreign-key pass a CDC migration defers to cut over
(:func:`_run_cutover_foreign_keys`), and the restore of the DEFAULTs the column exclusions
dropped (:func:`_run_cutover_default_restore`). The three CDC gates (dead-letter, REPLICA
IDENTITY, source schema) hold the runbook until their check passes or is waived
(:func:`_render_cdc_dlq_gate`, :func:`_render_probe_gate`); on a CDC migration, Finish also
waits for the preserved foreign keys to be in place, and for Aurora DSQL to validate the ones
a MySQL CDC stream can break, each hold with its own waiver (:func:`cutover_finish_fk_gate`,
:func:`cutover_fk_held`).

It was split out of :mod:`dsql_migrator.ui.validation`, which re-exports every name defined
here (private ones too), so ``from dsql_migrator.ui.validation import ...`` of any of them
keeps resolving. A re-export does not carry a monkeypatch: a test patches a name Cut over
reads in this module. The per-session :class:`~dsql_migrator.ui.validation.ValidationState`
and the helpers both steps read (the verdict summary, the CDC gate builder, the identity-sync
scope) stay in validation. Validation imports this module at its end, and this module imports
them back at its own end, after its definitions, so either module can be imported first.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable, Collection, Iterable, Mapping, Optional, Sequence

from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import (
    CdcCatalogDriftWaiver,
    CdcDlqWaiver,
    CdcReplicaIdentityWaiver,
    SourceType,
    StepStatus,
    ValidationReport,
)
from dsql_migrator.ui.cdc_dlq_gate import (
    READ_FAILED,
    READ_INCOMPLETE,
    ack_log_clause,
    bounded_table_names,
    cdc_dlq_gate_for,
    issue_lines,
    issue_notice,
    read_state_notice,
    schedule_cdc_dlq_check,
    unverifiable_notice,
    validation_evidence_at,
    waived_notice,
    waiver_log_detail,
    way_forward_lines,
)
from dsql_migrator.ui.cdc_catalog_drift_gate import (
    PROBE_STATE_NO_BASELINE as G4_PROBE_NO_BASELINE,
    catalog_drift_ack_log_clause,
    catalog_drift_gate_for,
    catalog_drift_waiver_log_detail,
    cutover_must_recheck_notice as g4_must_recheck_notice,
    cutover_open_notice as g4_open_notice,
    cutover_pending_notice as g4_pending_notice,
    cutover_unchecked_notice as g4_unchecked_notice,
    cutover_waivable_notice as g4_waivable_notice,
    cutover_waived_notice as g4_waived_notice,
    schedule_catalog_drift_probe,
)
from dsql_migrator.ui.cdc_replica_identity_gate import (
    PROBE_STATE_FAILED as RI_PROBE_FAILED,
    cutover_must_recheck_notice as ri_must_recheck_notice,
    cutover_pending_notice as ri_pending_notice,
    cutover_unchecked_notice as ri_unchecked_notice,
    cutover_waivable_notice as ri_waivable_notice,
    cutover_waived_notice as ri_waived_notice,
    cutover_weak_notice as ri_weak_notice,
    has_source_connection,
    replica_identity_ack_log_clause,
    replica_identity_gate_for,
    replica_identity_waiver_log_detail,
    schedule_replica_identity_probe,
)
from dsql_migrator.ui.cutover_defaults import (
    CutoverDefaultsSnapshot,
    CutoverRestoreOutcome,
)
from dsql_migrator.ui.data_migration import DataMigrationStore
from dsql_migrator.ui.data_migration._foreign_keys import (
    apply_preserved_foreign_keys,
    foreign_keys_counted_before_add,
    foreign_keys_in_scope,
    foreign_keys_pointing_in,
    foreign_keys_to_apply,
)
from dsql_migrator.ui.data_migration._models import (
    effective_lob_exclusions,
    session_applied_conversions,
    session_source_type,
)
from dsql_migrator.ui.design import (
    inline_hint,
    render_notice,
)
from dsql_migrator.ui.evaluation import EvaluationStore
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    WorkflowStep,
    _dev_unlock_steps,
    get_status,
    with_status,
)


def cutover_finish_fk_gate(
    *,
    cdc_in_use: bool,
    fk_pending_count: int,
    fk_apply_result: "Optional[tuple[int, int, int]]",
    proceed_without_fks: bool,
    inputs_unknown: bool = False,
    fks_in_place: "Optional[bool]" = None,
    fks_reading: bool = False,
) -> "Optional[str]":
    """Whether "I've cut over" is soft-blocked on preserved foreign keys NOT IN PLACE, and
    why. Pure.

    For a CDC migration the deferred FK ``ADD CONSTRAINT``s are created ONLY by the
    cut-over "Apply foreign keys" button -- Full Load's post-load pass is a no-op for a
    CDC run -- so acknowledging cut over with them never applied would silently mark the
    target live with ZERO enforced foreign keys. Since G142 this hold waits only for the
    keys to be IN PLACE (added ``NOT VALID``, seconds; each enforces every new write), not
    for Aurora DSQL's validation of the rows already loaded. The keys a MySQL CDC stream can
    break are a second, independent hold on the keys that ARE in place
    (:func:`cutover_fk_held`), with its own waiver. Returns:

    * ``None``        -- not gated (Full-Load-only, no preserved FKs, every FK in place,
      or the operator explicitly opted to proceed without them).
    * ``"pending"``   -- preserved FKs exist but the cut-over apply has NOT been run (or a
      catalog read shows a key missing since).
    * ``"checking"``  -- the apply has not run in this session, and the catalog read that
      would show the keys in place is still being taken (``fks_reading``).
    * ``"incomplete"`` -- the apply ran but some FKs were skipped (orphans) or failed,
      so referential integrity is not fully enforced.
    * ``"unknown"``    -- the schema inputs the FK count is derived from are not loaded
      in this session, so the tool cannot say whether the deferred ``ADD CONSTRAINT``s
      are still outstanding. Checked BEFORE the ``fk_pending_count <= 0`` clear, because
      that count is itself 0 in exactly this case: an indeterminate denominator must not
      paint the finish button green (the same rule Full Load applies to a 0 denominator).

    ``fk_pending_count`` counts the keys the cut-over pass adds (:func:`_cutover_fk_targets`:
    never a key it leaves out by design). ``fks_in_place`` is a catalog read taken after the
    latest Validation verdict (the validation watch's): ``True`` when it shows EVERY pending
    key on the target (so the keys are in place whatever this session's apply result says
    -- after a Validation re-run or a restart cleared it), ``False`` when it shows one
    missing (dropped since, or never added), ``None`` when there is no such read. It never
    masks this session's own apply result: skipped or failed keys are ``"incomplete"``
    whatever a read says. ``proceed_without_fks`` is the operator's explicit opt-out of
    this hold; when set the gate clears, so this is a soft-block with a stated escape
    hatch, never a trap. Full-Load-only runs are never gated here (their FKs are an
    explicit action on the Data Migration step, and the runbook re-offers the action itself
    for any not in place).
    """
    if not cdc_in_use:
        return None
    if proceed_without_fks:
        return None
    if inputs_unknown:
        return "unknown"
    if fk_pending_count <= 0:
        return None
    if fk_apply_result is not None and (fk_apply_result[1] or fk_apply_result[2]):
        return "incomplete"
    if fks_in_place is False:
        return "pending"
    if fks_in_place is None and fk_apply_result is None:
        return "checking" if fks_reading else "pending"
    return None


# How often the Cut over screen comes back while Aurora DSQL still validates a foreign key
# and no background watch reads it (G142); the watch itself refreshes on a change.
_FK_VALIDATION_REFRESH_SECONDS = 30.0
# How often the Cut over screen comes back while its foreign-key pass runs, so the
# "Adding foreign keys: X of N" count moves and the result shows (as the Full load card).
_FK_APPLY_POLL_SECONDS = 1.5


def _cutover_summary_for_preview() -> "ValidationSummary":
    """A synthetic 'ready' summary used only for the dev-unlock cut-over preview.

    Never used in real flows (the real summary comes from a validation run); it
    exists so a developer reviewing the UI with DSQL_MIGRATOR_DEV_UNLOCK_STEPS on
    can see the go-path runbook without a verdict.
    """
    return ValidationSummary(
        total_tables=0,
        matched_tables=0,
        mismatched_tables=0,
        orphan_count=0,
        is_match=True,
        mode="checksum",
        as_of="preview",
        reconcile_performed=True,
        reconciled_tables=0,
        inconsistent_tables=0,
        missing_on_target=0,
        extra_on_target=0,
        errored_tables=0,
        ready_for_cutover=True,
    )


def _run_cutover_identity_sync(
    session: object,
    validation_state: "ValidationState",
    report: "Optional[ValidationReport]",
    *,
    job_manager: "Optional[JobManager]" = None,
    sync: "Optional[Callable[..., dict]]" = None,
    refresh: "Optional[Callable[[], None]]" = None,
    extras: "Optional[Callable[[Sequence[str]], Mapping[str, object]]]" = None,
    identity_tables: "Optional[Callable[[], Sequence[str]]]" = None,
) -> None:
    """Advance identity sequences past the current target ``MAX(pk)`` (operator action).

    Invoked by the explicit "Sync identity sequences" button in the cut-over runbook --
    NOT on render, so viewing the screen never writes to the target. Keyed off the
    CURRENT ``MAX(pk)`` over the validated tables and every migrated identity table
    (``identity_tables``, read in the job: a table left out of the last Validation is
    synced too), so any rows CDC delivered after the last Validation are covered before
    the operator repoints the app (the app's first insert would otherwise collide,
    23505). Runs in the background (a target write) so the click never blocks the UI;
    the outcome and the tables it covered are stored on ``validation_state`` and the
    screen refreshes to show it. Best-effort -- ``resync_identity_sequences`` never
    raises; the ``is_identity`` catalog filter skips non-identity tables.
    """
    target_config = getattr(session, "target_config", None)
    report_names = [item.table for item in report.items] if report is not None else []
    if target_config is None or (not report_names and identity_tables is None):
        # Nothing to sync (no target / no tables): record an empty result so the button
        # reflects "ran, nothing to do" rather than staying in the un-run state.
        validation_state.set_cutover_identity_sync({}, covered=())
        if refresh is not None:
            refresh()
        return
    aws_profile = getattr(session, "aws_profile", None)

    def _work(_handle: object = None) -> None:
        # Read in the background job, like the sync: it reads the source.
        table_names = _with_identity_tables(report_names, identity_tables)
        advanced, failed = resync_identity_sequences(
            target_config,
            table_names,
            aws_profile=aws_profile,
            sync=sync,
            extras=extras(table_names) if extras is not None else None,
            heartbeat=getattr(_handle, "heartbeat", None),
        )
        if advanced:
            detail = ", ".join(
                f"{name} -> RESTART WITH {value}"
                for name, value in sorted(advanced.items())
            )
            log_activity(
                ActivityCategory.VALIDATION,
                "identity sequences re-synced (cut-over)",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{len(advanced)} identity primary key(s) advanced past the current "
                    "target rows before cut-over, so the application's first insert "
                    f"after cut-over cannot collide: {detail}"
                ),
            )
        if failed:
            # Surface a failed RESTART WITH as an ERROR — the cut-over runbook renders it
            # so the operator cannot repoint the app believing the sequences are safe
            # (audit finding D2). Reasons are value-free (Property 7).
            real_failed, not_identity = _split_not_identity(failed)
            if real_failed:
                fdetail = ", ".join(
                    f"{name}: {reason}" for name, reason in sorted(real_failed.items())
                )
                log_activity(
                    ActivityCategory.VALIDATION,
                    "identity sequence sync failed (cut-over)",
                    status=ActivityStatus.FAILURE,
                    detail=(
                        f"{len(real_failed)} identity sequence(s) could NOT be advanced "
                        f"before cut-over; the application's first insert may collide — "
                        f"retry the sync or advance them manually before repointing: {fdetail}"
                    ),
                )
            if not_identity:
                from dsql_migrator.core.target_introspector import not_identity_detail

                log_activity(
                    ActivityCategory.VALIDATION,
                    "no identity on the target (cut-over)",
                    status=ActivityStatus.FAILURE,
                    detail=not_identity_detail(not_identity),
                )
        validation_state.set_cutover_identity_sync(advanced, failed, covered=table_names)
        if refresh is not None:
            refresh()

    if job_manager is not None:
        job_manager.submit(_work)
    else:
        _work()


def _job_live(job_manager: "Optional[JobManager]", job_id: Optional[str]) -> bool:
    """Whether ``job_id`` is PENDING or RUNNING (unknown / no manager: not live)."""
    if not job_id or job_manager is None:
        return False
    try:
        return getattr(job_manager.get_status(job_id), "status", None) in ("PENDING", "RUNNING")
    except Exception:  # noqa: BLE001 - an unknown id is not a live job
        return False


def cutover_job_block(migration_state, job_manager: "Optional[JobManager]") -> Optional[str]:
    """Why a cut-over DEFAULT restore (or, under CDC, the FK apply) must wait: a Full Load
    or a Start CDC job is pending or running -- it (re)drops the DEFAULTs. ``None`` else."""
    if migration_state is None:
        return None
    if _job_live(job_manager, getattr(migration_state, "job_id", None)):
        return "off while a Full Load runs"
    if getattr(migration_state, "cdc_action_kind", None) == "start" and _job_live(
        job_manager, getattr(migration_state, "cdc_deploy_job_id", None)
    ):
        return "off while Start CDC runs"
    return None


def _fill_cutover_defaults(
    session: object,
    migration_state,
    conv_state,
    inventory,
    overlay_ledger,
    job_manager: "Optional[JobManager]",
    *,
    fill_key: object,
    generation: int,
    facts_reader: "Optional[Callable[..., Mapping[str, Any]]]" = None,
) -> "CutoverDefaultsSnapshot":
    """The background cache fill (worker thread): ledger x ONE bulk catalog read x the
    current effective DDL and the still-excluded set. Never raises."""
    from dsql_migrator.core.overlay_ledger import overlay_ledger_key
    from dsql_migrator.ui.cutover_defaults import read_cutover_defaults
    from dsql_migrator.ui.data_migration._models import (
        effective_target_ddls,
        session_source_type,
    )

    target = getattr(session, "target_config", None)
    aws_profile = getattr(session, "aws_profile", None)

    def _facts(names, columns):
        if facts_reader is not None:
            return facts_reader(names, columns)
        from dsql_migrator.core.target_connection import DsqlConnector
        from dsql_migrator.core.target_introspector import target_overlay_facts

        return target_overlay_facts(
            names, columns,
            connection_factory=DsqlConnector(target, aws_profile=aws_profile).connect,
            probe_rows=False,
        )

    mark = getattr(migration_state, "mark_overlay_ledger_in_use", None)
    return read_cutover_defaults(
        ledger=overlay_ledger,
        ledger_key=overlay_ledger_key(target),
        read_facts=_facts,
        current_ddls=lambda: effective_target_ddls(
            inventory, conv_state, session_source_type(session)
        ),
        still_excluded=lambda: session_exclusion_split(
            migration_state, session, inventory, conv_state, job_manager
        )[1],
        on_record_found=mark if callable(mark) else None,
        # A ledger table no longer selected (Full load only can unselect a loaded table,
        # G12; Full load + CDC too, before CDC starts, G16) is the operator's decision,
        # never an automatic restore.
        selected_tables=lambda: _migrated_tables(migration_state),
        fill_key=fill_key,
        generation=generation,
    )


def _cutover_fill_key(
    session: object, migration_state, validation_state: "ValidationState", conv_state,
    job_manager: "Optional[JobManager]",
) -> object:
    """What the cut-over DEFAULT status depends on: a change refills the cache."""
    from dsql_migrator.core.overlay_ledger import overlay_ledger_key

    def _term(job_id: Optional[str]) -> tuple:
        return (job_id, bool(job_id) and not _job_live(job_manager, job_id))

    getter = getattr(migration_state, "lob_exclusions", None)
    stored = getter() if callable(getter) else {}
    return (
        overlay_ledger_key(session.target_config),  # type: ignore[attr-defined]
        _term(getattr(migration_state, "job_id", None)),
        _term(
            getattr(migration_state, "cdc_deploy_job_id", None)
            if getattr(migration_state, "cdc_action_kind", None) == "start"
            else None
        ),
        _term(validation_state.cutover_default_restore_job_id),
        tuple(sorted((getattr(conv_state, "edited_target_ddls", None) or {}).items())),
        getattr(conv_state, "preserve_foreign_keys", None),
        tuple(sorted((t, tuple(sorted(c))) for t, c in (stored or {}).items())),
        # The selection decides which pending DEFAULTs restore automatically (G12).
        tuple(sorted(_migrated_tables(migration_state) or ())),
    )


def _log_cutover_restore(outcome: "CutoverRestoreOutcome") -> None:
    """The activity lines of one restore job (value-free: statements are schema text)."""
    if outcome.refused:
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULTs not restored (cut-over)",
            status=ActivityStatus.WARNING,
            detail=outcome.refused,
        )
        return
    if outcome.skipped:
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULTs not restored (cut-over)",
            status=ActivityStatus.WARNING,
            detail=(
                f"{len(outcome.skipped)} skipped: their record changed since the status "
                "was read; check again and retry: " + ", ".join(outcome.skipped)
            ),
        )
    if outcome.restored or outcome.already_done or outcome.gone:
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULTs restored (cut-over)",
            status=ActivityStatus.SUCCESS,
            detail=(
                f"{len(outcome.restored)} restored"
                + (f", {len(outcome.already_done)} already in place" if outcome.already_done else "")
                + (f", {len(outcome.gone)} no longer on the target" if outcome.gone else "")
                # Each statement ends in its own ';': drop the last so the clause separator
                # below does not double it.
                + (
                    f"; ran: {' '.join(outcome.statements).rstrip().rstrip(';')}"
                    if outcome.statements
                    else ""
                )
                + "; migrated rows stay NULL (a DEFAULT applies to new inserts only)"
            ),
        )
    if outcome.diverged:
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULTs left as they are (cut-over)",
            status=ActivityStatus.INFO,
            detail=(
                "a different DEFAULT is set on Aurora DSQL, never overwritten: "
                + ", ".join(outcome.diverged)
            ),
        )
    if outcome.failed:
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULT restore failed (cut-over)",
            status=ActivityStatus.FAILURE,
            detail="; ".join(f"{name}: {reason}" for name, reason in sorted(outcome.failed.items())),
        )
    if outcome.ledger_warning:
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULT record not updated (cut-over)",
            status=ActivityStatus.WARNING,
            detail=outcome.ledger_warning,
        )


def _run_cutover_default_restore(
    session: object,
    validation_state: "ValidationState",
    migration_state,
    *,
    request: "Optional[tuple[str, str]]",
    overlay_ledger,
    job_manager: "Optional[JobManager]",
    refresh: Callable[[], None],
    notify: Callable[..., None],
    settle_probe: "Optional[Callable[[], Optional[str]]]" = None,
    restore: "Optional[Callable[..., Any]]" = None,
    connection_factory: "Optional[Callable[[], Any]]" = None,
    fill_key_now: "Optional[Callable[[], object]]" = None,
) -> None:
    """[Restore DEFAULTs] (``request`` None: the automatic list) or one row's [Restore].

    Every gate is re-read HERE, at click time, never taken from the render (a second tab
    with an old render must not submit after a CDC start cleared the attestation): no
    Full Load / Start CDC job pending or running, no restore running, under CDC the drained
    attestation, and a loaded status. Then ONE background job (see
    :func:`~dsql_migrator.ui.cutover_defaults.run_cutover_default_restore`).
    """
    from dsql_migrator.core.overlay_ledger import overlay_ledger_key
    from dsql_migrator.core.schema_applier import restore_column_default
    from dsql_migrator.ui.cutover_defaults import (
        CutoverRestoreOutcome,
        cutover_settle_probe,
        run_cutover_default_restore,
    )

    def _refuse(message: str) -> None:
        notify(message, type="warning")
        refresh()

    block = cutover_job_block(migration_state, job_manager)
    if block:
        return _refuse(f"Restoring DEFAULTs is {block}; try again when it finishes.")
    if validation_state.cutover_default_restore_in_flight(job_manager):
        return _refuse("The DEFAULTs are already being restored.")
    cdc = _cdc_in_use(session)
    if cdc and not validation_state.cutover_cdc_drained_confirmed:
        return _refuse(
            "Confirm first that source writes are frozen and CDC has drained to zero lag."
        )
    snap = validation_state.cutover_defaults
    target = getattr(session, "target_config", None)
    if snap is None or snap.state != "loaded" or target is None or overlay_ledger is None:
        return _refuse("The DEFAULT status is not loaded; check again.")
    # The list is only as good as the inputs it was classified with: a DDL edit or an
    # exclusion change in another tab since then re-classifies it (auto -> manual).
    if fill_key_now is not None:
        try:
            current = fill_key_now()
        except Exception:  # noqa: BLE001 - unknown inputs: re-read
            current = object()
        if current != snap.fill_key:
            validation_state.invalidate_cutover_defaults()
            return _refuse(
                "The DEFAULT status changed since this screen was drawn; check again."
            )
    if request is None:
        items = list(snap.auto_items) + [i for i in snap.gone_items if i.executable]
        if not snap.auto_items:
            return _refuse("No DEFAULT can be restored automatically.")
    else:
        items = [
            i for i in snap.items
            if (i.table, i.column) == tuple(request) and i.status == "pending" and i.executable
        ]
        if not items:
            return _refuse("That DEFAULT is no longer pending; check again.")
    aws_profile = getattr(session, "aws_profile", None)

    def _connect():
        if connection_factory is not None:
            return connection_factory()
        from dsql_migrator.core.target_connection import DsqlConnector

        return DsqlConnector(target, aws_profile=aws_profile).connect()

    probe = settle_probe
    if probe is None and cdc:
        def probe() -> Optional[str]:
            return cutover_settle_probe(
                region=getattr(target, "region", ""),
                stack_name=getattr(migration_state, "cdc_stack_name", ""),
                aws_profile=aws_profile,
                assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
            )

    key = overlay_ledger_key(target)

    def _work(handle: object = None) -> None:
        try:
            outcome = run_cutover_default_restore(
                handle, items=items, ledger=overlay_ledger, ledger_key=key,
                restore=restore or restore_column_default, connection_factory=_connect,
                settle_probe=probe if cdc else None,
            )
        except Exception as exc:  # noqa: BLE001 - reported, never a silent success
            outcome = CutoverRestoreOutcome(
                refused=f"not restored: {_safe_restore_error(exc)}"
            )
        if outcome.refused_by_settle:
            validation_state.set_cutover_cdc_drained_confirmed(False)
        _log_cutover_restore(outcome)
        validation_state.set_cutover_default_restore_outcome(outcome)
        validation_state.invalidate_cutover_defaults()
        refresh()

    if job_manager is None:
        _work()
        return
    validation_state.set_cutover_default_restore_job(job_manager.submit(_work))


def _safe_restore_error(exc: BaseException) -> str:
    text = str(exc).strip().splitlines()
    return text[0] if text else type(exc).__name__


def _cutover_pending_foreign_keys(
    session: object,
    eval_store: "Optional[EvaluationStore]",
    conversion_store: "Optional[Any]",
    session_id: str,
    *,
    migration_state: object = None,
) -> int:
    """Count foreign keys still to apply at cut over.

    Zero when FK preservation is off, no schema-conversion / evaluation state exists,
    or the source has no foreign keys. Used to decide whether the cut-over runbook
    shows the "Apply foreign keys" action.

    Counts the SAME rendered ``foreign_key_ddls`` that ``apply_preserved_foreign_keys``
    will iterate at cut over (:func:`_cutover_fk_targets`, the pass's own selection: no
    key it leaves out by design) -- rebuilding the applied conversions exactly as
    :func:`_run_cutover_foreign_keys` does (a pure, side-effect-free schema
    re-conversion + Schema-Conversion edits/toggle; no DB). This excludes FKs on a
    table that converts UNSUPPORTED (its ``foreign_key_ddls`` is empty even though the
    source FK metadata survives on ``preserved_foreign_keys``), so the count never
    over-states what apply actually does. The rebuild is heavier than a raw metadata
    tally, but cut over is not a hot path.
    """
    conv_state = conversion_store.get(session_id) if conversion_store is not None else None
    if conv_state is None or not conv_state.preserve_foreign_keys:
        return 0
    targets = _cutover_fk_targets(
        session, eval_store, conversion_store, session_id, migration_state=migration_state
    )
    if targets is None:
        return 0
    return sum(len(conv.foreign_key_ddls) for conv in targets[1].values())


def _cutover_fk_targets(
    session: object,
    eval_store: "Optional[EvaluationStore]",
    conversion_store: "Optional[Any]",
    session_id: str,
    *,
    migration_state: object = None,
) -> "Optional[tuple[list, dict]]":
    """``(inventory tables, the conversions whose foreign keys the cut-over pass adds)``,
    ``None`` without the schema inputs. Pure, no DB.

    The pass's own selection (``foreign_keys_to_apply``): the migration's scope less each
    unselected child pointing in that the last pass found absent from the target and left
    out (the record's ``fk_absent_children``) -- so Finish never waits for a key the pass
    leaves out by design, and waits for every key it adds (one the operator added or
    renamed in Schema Conversion too)."""
    context = _identity_sync_context(
        session, eval_store, conversion_store, session_id, migration_state=migration_state
    )
    if context is None:
        return None

    tables, scoped = context
    absent = getattr(migration_state, "fk_absent_children", None)
    return tables, foreign_keys_to_apply(
        scoped,
        foreign_keys_pointing_in(scoped, _migrated_tables(migration_state)),
        absent() if callable(absent) else (),
    )


def _cutover_fk_keys(
    session: object,
    eval_store: "Optional[EvaluationStore]",
    conversion_store: "Optional[Any]",
    session_id: str,
    *,
    migration_state: object = None,
    cdc_in_use: bool = False,
) -> "tuple[list[tuple[str, str]], frozenset, dict]":
    """``(pending keys, keys a MySQL CDC stream can break, key -> ForeignKeyDef)`` of the
    foreign keys the cut-over pass adds (G142, :func:`_cutover_fk_targets`). Pure, no DB;
    empty without the schema inputs.

    The pending keys are EVERY rendered foreign-key DDL's ``(child, constraint)`` -- the
    same DDLs :func:`_cutover_pending_foreign_keys` counts, so a key the operator added or
    renamed in Schema Conversion counts too, and a DDL whose name cannot be read stays as
    ``(child, None)`` (no catalog read can confirm it). The definitions are the keys with
    source metadata only (they explain a failed validation). The second set is MySQL +
    CDC only (``_fk_validation.foreign_keys_cdc_can_break``: a cascade action InnoDB never
    logs, or a text key MySQL matches more loosely): Finish waits for Aurora DSQL to
    validate those.
    """
    context = _cutover_fk_targets(
        session, eval_store, conversion_store, session_id, migration_state=migration_state
    )
    if context is None:
        return [], frozenset(), {}
    from dsql_migrator.ui.data_migration._fk_validation import (
        foreign_key_defs,
        foreign_keys_cdc_can_break,
        pending_foreign_keys,
    )

    tables, applied = context
    conv_state = conversion_store.get(session_id) if conversion_store is not None else None
    if conv_state is None or not getattr(conv_state, "preserve_foreign_keys", True):
        return [], frozenset(), {}
    defs = foreign_key_defs(applied)
    # The session's engine, a restored one too (no source_config): a PostgreSQL source has
    # no MySQL cascade / collation gap to hold Finish on.
    source = session_source_type(session)
    breakable = (
        foreign_keys_cdc_can_break(applied, tables)
        if cdc_in_use and source is SourceType.MYSQL
        else frozenset()
    )
    return pending_foreign_keys(applied), breakable, defs


def _fk_validation_view_of(migration_state: object) -> object:
    """The session's foreign-key validation view (``_fk_validation.FkValidationView``):
    the persisted job ids under the last read of Aurora DSQL, or ``None``."""
    view = getattr(migration_state, "fk_validation_view", None)
    if not callable(view):
        return None
    try:
        return view()
    except Exception:  # noqa: BLE001 - a report; never break the screen
        return None


def cutover_fk_in_place(
    keys: "Sequence[tuple[str, Optional[str]]]", pending_count: int, view: object
) -> "tuple[Optional[bool], bool, list[str]]":
    """``(fks_in_place, fks_reading, missing keys)`` for the Finish gate (G142). Pure.

    ``keys`` are EVERY pending foreign-key DDL's ``(child, constraint)``
    (:func:`_cutover_fk_keys`), ``view`` the session's ``FkValidationView`` -- read after the
    latest Validation verdict: Cut over drops an older read before it asks. ``True`` only
    when that read finds every pending key in place, the keys being exactly the pending
    set; ``False`` when it finds a named key missing (gone from the target, or not in the
    record of the pass that last ran: never added); ``None`` when it cannot tell -- no
    record, a key whose name cannot be read, or a read still to come (``fks_reading``).
    """
    if not keys or view is None or not getattr(view, "items", ()):
        return None, False, []
    if getattr(view, "checking", True):
        return None, True, []
    named = [key for key in keys if key[1]]
    states = {key: view.state_of(key) for key in named}  # type: ignore[attr-defined]
    missing = [f"{t}.{c}" for (t, c), state in states.items() if state in (None, "absent")]
    if missing:
        return False, False, missing
    if any(state == "unknown" for state in states.values()):
        return None, True, []
    if len(named) == len(keys) == int(pending_count):
        return True, False, []
    return None, False, []


def cutover_fk_held(
    breakable: "Iterable[tuple[str, str]]", view: object
) -> "dict[tuple[str, str], str]":
    """The keys a MySQL CDC stream can break that are IN PLACE and that Finish still waits
    for Aurora DSQL to validate, each with its state (``_fk_validation``'s: failed,
    not-validated, validating, unknown). Pure.

    Finish's second hold, independent of the first (:func:`cutover_finish_fk_gate`, keys
    not in place) and with its own waiver: a key the record does not hold, or one a read
    found gone, is not in place -- the first hold's -- and never held here. Only a
    validated key is released, by a read taken after the latest Validation verdict (Cut
    over drops older ones): before that read every recorded key is ``unknown``, being read.
    """
    held: "dict[tuple[str, str], str]" = {}
    if view is None:
        return held
    for key in sorted(breakable or ()):
        state = view.state_of(key)  # type: ignore[attr-defined]
        if state not in (None, "absent", "validated"):
            held[key] = state
    return held


def _fk_keys_phrase(count: int, *, preserved: bool = False) -> str:
    """``"1 foreign key"`` / ``"3 preserved foreign keys"``."""
    return f"{count} {'preserved ' if preserved else ''}foreign key" + ("" if count == 1 else "s")


def _cutover_orphans_hold_copy(held: "Mapping[tuple[str, str], str]") -> "tuple[str, str]":
    """The validation hold's header and body, from each held key's state (G142): what will
    release it -- nothing to do while Aurora DSQL's check runs or is being read, fixing the
    rows and "Validate again" after a failure, "Validate again" when no job runs. Every
    held key is in place (:func:`cutover_fk_held`). Pure."""
    by_state: "dict[str, list[str]]" = {}
    for (table, constraint), state in held.items():
        by_state.setdefault(state, []).append(f"{table}.{constraint}")
    names = [f"{table}.{constraint}" for table, constraint in held]
    failed = by_state.get("failed", [])
    no_job = by_state.get("not-validated", [])
    validating = by_state.get("validating", [])
    reading = by_state.get("unknown", [])
    if failed:
        header = "Foreign keys CDC can break failed Aurora DSQL's validation"
    elif no_job:
        header = "Foreign keys CDC can break are not validated"
    elif validating:
        header = "Waiting for Aurora DSQL to validate the foreign keys CDC can break"
    else:
        header = "Reading Aurora DSQL's validation of the foreign keys CDC can break"
    clauses = [
        f"Aurora DSQL's validation failed for {bounded_table_names(failed)}: fix the rows "
        "(see the details above), then click Validate again."
        if failed
        else "",
        f"No validation job runs for {bounded_table_names(no_job)} "
        + (
            "(it could not be submitted, or Aurora DSQL no longer reports it)"
            if len(no_job) == 1
            else "(they could not be submitted, or Aurora DSQL no longer reports them)"
        )
        + ": click Validate again."
        if no_job
        else "",
        f"Aurora DSQL's check of {bounded_table_names(validating)} is still running."
        if validating
        else "",
        f"Aurora DSQL's verdict on {bounded_table_names(reading)} is being read now."
        if reading
        else "",
    ]
    body = (
        f"{_fk_keys_phrase(len(names))} ({bounded_table_names(names)}) can be left with "
        "orphan rows by a MySQL CDC stream: an ON DELETE / ON UPDATE action runs inside "
        "MySQL and never reaches the binary log, and a key MySQL matches more loosely "
        "(ignoring case or trailing spaces) can match no parent on Aurora DSQL. The final "
        "Validation's row counts do not see such rows, so Finish waits for Aurora DSQL's own "
        "check of the rows already loaded. "
        + " ".join(clause for clause in clauses if clause)
        + (" It is" if len(names) == 1 else " Each is")
        + " in place and refuses every new write that would break it. Or explicitly choose "
        "to cut over without this check below."
    )
    return header, body


def _cutover_foreign_keys_stripped(
    conversion_store: "Optional[Any]", session_id: str
) -> bool:
    """Whether the operator turned FK preservation OFF for this migration.

    ``preserve_foreign_keys=False`` is the documented opt-out: the target goes live with
    NO foreign keys and referential integrity becomes the application's job. That is
    exactly the fact an auditor most needs on the cut-over line, yet it reaches
    ``_cutover_pending_foreign_keys`` as a plain ``0`` -- indistinguishable from a schema
    that simply had none. Returns ``False`` when the state is absent, so a missing
    snapshot is never reported as a deliberate strip.

    Pure: reads recorded state, no DB.
    """
    if conversion_store is None:
        return False
    conv_state = conversion_store.get(session_id)
    return conv_state is not None and not conv_state.preserve_foreign_keys


def _cutover_integrity_detail(
    validation_state: "ValidationState",
    pending: int,
    *,
    inputs_missing: bool = False,
    fks_stripped: bool = False,
    identity_missed: "Sequence[str]" = (),
    standalone_sequences: int = 0,
    fk_view: object = None,
    fk_validation_waived: "Optional[bool]" = None,
) -> str:
    """One clause stating whether the target went live with its foreign keys enforced.

    The absence of an "apply foreign keys" line is ambiguous across FOUR situations -- the
    schema had none, they were applied, the action was never run, or the operator
    explicitly waived them -- so an auditor reading only the log could not answer the
    question this screen exists to settle. Says which, plus whether the identity sequences
    were advanced (an un-advanced sequence is a post-cut-over duplicate-key risk).

    ``pending <= 0`` is itself ambiguous across three MORE situations, and the reassuring
    one was being printed for all of them: the schema genuinely has no foreign keys, the
    schema state could not be read (a partial session snapshot loses it), or the operator
    STRIPPED every foreign key via ``preserve_foreign_keys=False``. Only the first is
    "none in scope"; the render already distinguishes them with
    :func:`_cutover_gate_inputs_missing`, so the log now takes the same two predicates
    rather than collapsing a deliberate integrity trade-off into "nothing to do".

    ``identity_missed`` names the migrated identity tables the sync did not reach
    (:func:`_identity_sync_missed`): the clause then never reads "synced".
    ``standalone_sequences`` counts the source sequences no column owns, which the tool
    neither creates nor syncs (Evaluation's ``PG_STANDALONE_SEQUENCE``).

    ``fk_view`` (``_fk_validation.FkValidationView``, the last read of Aurora DSQL) adds
    how many keys in place Aurora DSQL VALIDATED and names each one it has NOT (G142): a
    key in place enforces every new write, but only a validated one proves the rows
    already loaded; that read, not the pass's count, says how many are in place. The two
    waivers are said apart: "WAIVED (N not applied)" for keys not in place, "validation
    check WAIVED" for keys in place Aurora DSQL has not validated.
    ``fk_validation_waived``: whether the second waiver covers the keys held now (``None``
    reads whether it was given).

    Pure: reads the already-recorded outcomes, no DB. Counts and states only, never a row
    value (Property 7).
    """
    waived = validation_state.proceed_without_foreign_keys
    validation_waived = (
        bool(validation_state.proceed_without_fk_validation)
        if fk_validation_waived is None
        else bool(fk_validation_waived)
    )
    result = validation_state.cutover_fk_apply
    read = fk_view is not None and not getattr(fk_view, "checking", True)
    # After a read: the recorded keys it did not find gone (one it could not read counts --
    # the pass put it in place); a key dropped since the pass is not in place.
    in_place = (
        int(getattr(fk_view, "in_place", 0) or 0)
        if read
        else int(result[0] or 0)
        if result is not None
        else 0
    )
    not_validated = (
        []
        if fk_view is None or getattr(fk_view, "checking", True)
        else [
            item.label
            for item in getattr(fk_view, "items", ())
            if item.state not in ("validated", "absent")
        ]
    )
    validated_clause = (
        ""
        if fk_view is None
        or not getattr(fk_view, "items", ())
        or (not getattr(fk_view, "checking", True) and not getattr(fk_view, "in_place", 0))
        else (
            "; validated by Aurora DSQL: NOT KNOWN (not read back from Aurora DSQL yet)"
            if getattr(fk_view, "checking", True)
            else f"; validated by Aurora DSQL: {fk_view.validated} of {fk_view.in_place}"
            + (
                f" ({len(not_validated)} NOT validated: {_bounded_names(not_validated)})"
                if not_validated
                else ""
            )
        )
    )
    counted = pending > 0
    if waived and not (counted and in_place >= pending):
        fk = f"foreign keys WAIVED ({max(0, pending - in_place)} not applied)"
    elif fks_stripped:
        fk = (
            "foreign keys STRIPPED by the schema conversion -- the target is live "
            "WITHOUT them and referential integrity is the application's job"
        )
    elif inputs_missing:
        fk = (
            "foreign keys: NOT KNOWN (the schema-conversion / evaluation state this "
            "count reads is unavailable, so this is not an assertion that there are none)"
        )
    elif pending <= 0:
        fk = "foreign keys: none in scope"
    else:
        if result is None and in_place < pending:
            fk = f"foreign keys NOT applied ({pending - in_place} outstanding)"
        elif result is None:
            fk = f"foreign keys {in_place}/{pending} in place"
        else:
            applied, skipped, failed = result
            fk = f"foreign keys {applied}/{pending} applied"
            if skipped or failed:
                fk += f" ({skipped} skipped on orphan rows, {failed} failed)"
    if counted and not fks_stripped and not inputs_missing:
        fk += (", validation check WAIVED" if validation_waived else "") + validated_clause
    synced = validation_state.cutover_identity_sync
    # A failed or impossible sync is said, never folded into "synced" -- the clause is
    # built from the counts, so a failure never reads "synced (nothing to advance), 1 FAILED".
    failed = dict(getattr(validation_state, "cutover_identity_sync_failed", None) or {})
    real, not_identity = _split_not_identity(failed) if synced is not None else ({}, [])
    missed = list(identity_missed) if synced is not None else []
    if synced is None:
        identity = "identity sequences not synced"
    elif not real and not not_identity and not missed:
        identity = (
            f"identity sequences synced ({len(synced)} advanced)"
            if synced
            else "identity sequences synced (nothing to advance)"
        )
    else:
        counts = [f"{len(synced)} advanced"] if synced else []
        if real:
            counts.append(f"{len(real)} FAILED")
        if not_identity:
            counts.append(f"{len(not_identity)} with no identity on the target")
        if missed:
            counts.append(f"{len(missed)} NOT synced ({_bounded_names(missed)})")
        identity = "identity sequences: " + ", ".join(counts)
    if standalone_sequences:
        identity += f"; standalone sequences left to the operator: {standalone_sequences}"
    return f"{fk}; {identity}"


def _cutover_gate_inputs_missing(
    eval_store: "Optional[EvaluationStore]",
    conversion_store: "Optional[Any]",
    session_id: str,
) -> bool:
    """Whether the cut-over gates have no schema inputs to count from.

    ``_cutover_pending_foreign_keys`` / ``_cutover_identity_tables`` answer 0 / ``[]``
    both when there is genuinely nothing to apply AND when the evaluation / conversion
    state they read is absent -- and that hides the corresponding runbook action. Those
    semantics are correct and pinned, so this is a SEPARATE predicate: it tells the
    render which of the two zeros it is looking at, so "nothing to do" and "can't tell"
    stop rendering identically (an empty runbook that reads as done).

    Pure: no DB, no target-catalog read. The cut-over screen re-renders on every
    refresh, so it must never scan the target -- the foreign-key pass is behind a button,
    and Aurora DSQL's verdict on it is read by a background watch, for the same reason.
    """
    if conversion_store is None or eval_store is None:
        return True
    if conversion_store.get(session_id) is None:
        return True
    result = eval_store.get_or_create(session_id).result
    return result is None or result.inventory is None


def _cutover_standalone_sequences(
    eval_store: "Optional[EvaluationStore]", session_id: str
) -> "list[str]":
    """The source sequences no column owns (Evaluation's ``PG_STANDALONE_SEQUENCE``
    items), ``[]`` without an Evaluation report. No sync reaches them -- an identity sync
    moves a column's own sequence only -- so the runbook names each one for the operator,
    who moves it past the source's final value before repointing. Pure, no DB."""
    if eval_store is None:
        return []
    from dsql_migrator.core.assessor import PG_STANDALONE_SEQUENCE_RULE_ID

    result = eval_store.get_or_create(session_id).result
    assessment = getattr(result, "assessment", None) if result is not None else None
    return sorted(
        {
            item.object_name
            for item in getattr(assessment, "items", None) or ()
            if item.rule_id == PG_STANDALONE_SEQUENCE_RULE_ID
        }
    )


def _identity_sync_missed(
    validation_state: "ValidationState", identity_tables: "Sequence[str]"
) -> "list[str]":
    """The migrated identity tables the last cut-over sync was not given (``[]`` before it
    ran): neither its result line nor the acknowledgement may read "synced" over them."""
    covered = validation_state.cutover_identity_sync_covered
    if validation_state.cutover_identity_sync is None or covered is None:
        return []
    return sorted(set(identity_tables) - set(covered))


def _run_cutover_foreign_keys(
    session: object,
    validation_state: "ValidationState",
    *,
    eval_store: "Optional[EvaluationStore]" = None,
    conversion_store: "Optional[Any]" = None,
    session_id: str = "",
    job_manager: "Optional[JobManager]" = None,
    refresh: "Optional[Callable[[], None]]" = None,
    migration_state: object = None,
) -> None:
    """Apply preserved foreign keys on Aurora DSQL at cut over (operator action).

    For a CDC migration the FK ``ADD CONSTRAINT``s are deferred (they must not exist
    while the sink streams out-of-order rows -- it dead-letters an FK violation). This
    runs them once, AFTER the final drain and BEFORE repointing: it rebuilds the applied
    conversions from the source inventory + Schema-Conversion edits/toggle, then adds each
    FK via ``apply_preserved_foreign_keys`` -- G142: ``NOT VALID`` at once, seconds inside
    the write freeze (a key whose parent side is incomplete by design is counted first),
    and an async VALIDATE per key whose job id is recorded on ``migration_state`` (the
    persisted record) for the background watch that reads Aurora DSQL's verdict. Runs in
    the background so the click never blocks the UI; the ``(applied, skipped, failed)``
    result is stored on ``validation_state`` and the screen refreshes. Best-effort.
    """
    target_config = getattr(session, "target_config", None)
    conv_state = (
        conversion_store.get(session_id) if conversion_store is not None else None
    )
    eval_result = (
        eval_store.get_or_create(session_id).result if eval_store is not None else None
    )
    inventory = eval_result.inventory if eval_result is not None else None
    if target_config is None or conv_state is None or inventory is None:
        # Nothing to apply (missing state): record an empty result so the button reads
        # "ran, nothing to do" rather than staying un-run.
        validation_state.set_cutover_fk_apply((0, 0, 0))
        if refresh is not None:
            refresh()
        return
    aws_profile = getattr(session, "aws_profile", None)
    # Read at submit time: only the migrated tables' keys are added (a table the migration
    # never loaded is not altered, nor counted as pending).
    scope = _migrated_tables(migration_state)

    def _work(_handle: object = None) -> None:
        from dsql_migrator.core.target_connection import DsqlConnector

        # The session's engine (a restored one too) and a COPY of the edits: this runs
        # on a job thread while the UI thread may be editing them.
        _conversion, applied = session_applied_conversions(session, inventory, conv_state)

        _unscoped = applied
        applied = foreign_keys_in_scope(applied, scope)
        connect = DsqlConnector(target_config, aws_profile=aws_profile).connect
        # The keys whose parent side is incomplete by design keep the count before their
        # ADD (G142): an excluded referenced column, a parent or child outside the set.
        _excluded: dict = {}
        if callable(getattr(migration_state, "lob_exclusions", None)):
            try:
                _excluded = effective_lob_exclusions(
                    migration_state, inventory,
                    applied_conversions=_unscoped,
                    preserve_foreign_keys=conv_state.preserve_foreign_keys,
                )
            except Exception:  # noqa: BLE001 - refines which keys are counted first
                import logging

                # Validation's logger, by name: this warning keeps the logger it had before
                # Cut over moved out of ui/validation.py (the app's log line prints it).
                logging.getLogger("dsql_migrator.ui.validation").warning(
                    "Could not read the column exclusions", exc_info=True
                )
        _record = getattr(migration_state, "fk_validation_records", None)
        result = apply_preserved_foreign_keys(
            applied, connect,
            skip_if_absent=foreign_keys_pointing_in(applied, scope),
            check_before_add=foreign_keys_counted_before_add(applied, scope, _excluded),
            # The recorded VALIDATE jobs in (a running one is kept, never submitted twice)
            # and the submitted ones out, onto the persisted record the watch reads.
            validation_jobs=(
                {key: rec.job_id for key, rec in _record().items() if rec.job_id}
                if callable(_record)
                else None
            ),
            on_validation=getattr(migration_state, "record_fk_validation", None),
            # Publish per-FK progress so a render during the pass reports how far it has
            # got. The pass said nothing at all before.
            on_progress=validation_state.set_cutover_fk_apply_progress,
            # Per FK and per poll of a still-building parent index (up to 300 s per
            # distinct parent index), and per page of a key counted before its ADD; at
            # scale a silent pass ran past the watchdog's window and left a spurious
            # FAILED job record behind (the id is not polled, so the reap was invisible
            # while still being persisted).
            heartbeat=getattr(_handle, "heartbeat", None),
            # Audit only: the pass is identical, but it must not read as something that
            # happened during the load -- for a CDC migration this is the ONLY pass that
            # creates these constraints, and it runs here, right before the repoint. Puts
            # it in the same category as the identity-sequence sync beside it.
            origin="cut-over",
        )
        validation_state.set_cutover_fk_apply(result)
        if refresh is not None:
            refresh()
        # Aurora DSQL's verdict on the rows already loaded is read back in the background
        # (its own job and slot), so this job -- and the second-click guard -- ends now.
        if migration_state is not None and job_manager is not None:
            from dsql_migrator.ui.data_migration._fk_validation import (
                foreign_key_defs,
                start_fk_validation_watch,
            )

            start_fk_validation_watch(
                migration_state, job_manager, connection_factory=connect,
                fk_defs=lambda: foreign_key_defs(applied),
                category=ActivityCategory.VALIDATION, refresh=refresh,
            )

    if job_manager is not None:
        # RE-ENTRANCY GUARD: refuse to start a second pass while one is live. Without it a
        # second click submitted a second CONCURRENT pass over every foreign key, and the
        # newer job id displaced the older, so the first could no longer be tracked at all.
        running = validation_state.cutover_fk_apply_job_id
        if running:
            try:
                live = job_manager.get_status(running)
            except Exception:  # noqa: BLE001 - an unknown id is not a live job
                live = None
            if live is not None and getattr(live, "status", None) in ("PENDING", "RUNNING"):
                if refresh is not None:
                    refresh()
                return
        # The screen reads the pass as running while its job is live and no result is
        # recorded, so the previous result goes BEFORE the submit: the pass's own last
        # refresh runs inside its job and must already show its result, never "Adding
        # foreign keys" next to it with Apply disabled.
        validation_state.set_cutover_fk_apply(None)
        validation_state.set_cutover_fk_apply_progress(0, 0)
        validation_state.set_cutover_fk_apply_job(job_manager.submit(_work))
        # Show the pass running at once (the click handler does not re-render).
        if refresh is not None:
            refresh()
    else:
        _work()


def build_cutover_screen(
    store: SessionStore,
    session_id: str,
    *,
    validation_store: ValidationStore,
    job_manager: Optional[JobManager] = None,
    sync_sequences: "Optional[Callable[..., dict]]" = None,
    eval_store: "Optional[EvaluationStore]" = None,
    conversion_store: "Optional[Any]" = None,
    # No AI params beyond the activity feed: the "Ask AI DBA about cut over" section was
    # removed because its advice did not read as grounded in the actual migration -- and the
    # one fact that decides GO/HOLD on this screen, the foreign-key state, was never in the
    # grounding at all. Keeping a chat that sounds authoritative about a cut over while
    # missing that is worse than not offering one.
    ai_post_event: "Optional[Callable[..., object]]" = None,
    # The column-exclusion DEFAULT restore: the Data Migration state (jobs, CDC stack,
    # exclusions) and the overlay ledger handle. None: no restore block (as before).
    migration_store: "Optional[DataMigrationStore]" = None,
    overlay_ledger: "Optional[object]" = None,
    overlay_facts_reader: "Optional[Callable[..., Mapping[str, Any]]]" = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Cut over step (step 5), returning ``(content_builder, runner)``.

    Cut-over is the one step the tool does NOT perform: repointing the application
    from the source database to DSQL is an operational act only the operator can do. So this step
    has no job to run — the ``runner`` simply marks the step ``DONE`` when the user
    acknowledges they have cut over (the workflow shell's Run button is hidden for
    this step; a dedicated in-content button drives the acknowledgement).

    The content reflects the *last validation verdict* so the guidance is honest:
    on a clean MATCH it shows the cut-over runbook (tailored to whether CDC is in
    use); otherwise it tells the user to get a clean Validation first and offers no
    "I've cut over" affordance. This keeps cut-over gated on evidence, not vibes.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    validation_state = validation_store.get_or_create(session_id)
    migration_state = (
        migration_store.get_or_create(session_id) if migration_store is not None else None
    )
    # The migration's foreign keys for the FK block and the Finish gate (G142), derived
    # again only when what they come from changes: the screen re-renders while Aurora DSQL
    # validates, and each derivation converts the schema again.
    _fk_keys_memo: dict = {}

    def _fk_keys_now(cdc_in_use: bool) -> "tuple[list, frozenset, dict]":
        conv_state = conversion_store.get(session_id) if conversion_store is not None else None
        eval_result = (
            eval_store.get_or_create(session_id).result if eval_store is not None else None
        )
        inventory = getattr(eval_result, "inventory", None)
        absent = getattr(migration_state, "fk_absent_children", None)
        key = (
            tuple(sorted((getattr(conv_state, "edited_target_ddls", None) or {}).items())),
            getattr(conv_state, "preserve_foreign_keys", None),
            tuple(_migrated_tables(migration_state) or ()),
            session_source_type(session),
            bool(cdc_in_use),
            # The children the last pass left out as absent: not waited for.
            absent() if callable(absent) else frozenset(),
        )
        if _fk_keys_memo.get("inventory") is not inventory or _fk_keys_memo.get("key") != key:
            _fk_keys_memo.update(
                inventory=inventory,
                key=key,
                value=_cutover_fk_keys(
                    session, eval_store, conversion_store, session_id,
                    migration_state=migration_state, cdc_in_use=cdc_in_use,
                ),
            )
        return _fk_keys_memo["value"]

    def _fk_gate_now(pending: int, *, drop_stale_reads: bool = False) -> SimpleNamespace:
        """The Finish gate's foreign-key inputs and verdict from the CURRENT state (no I/O;
        G142), for the render, the acknowledgement and its line.

        Two independent holds, each with its own waiver: ``gate``, keys not in place
        (:func:`cutover_finish_fk_gate`; lifted by "Cut over without enforced foreign
        keys"), and ``validation``, the keys in place a MySQL CDC stream can break that
        Aurora DSQL has not validated (:func:`cutover_fk_held`; lifted by "Cut over without
        this check" while it names every key held). Going live without the keys not in
        place is not the decision to go live with keys not validated, nor the reverse.

        ``drop_stale_reads`` (the render): a catalog read that started before the latest
        Validation verdict, or before this screen was opened (unless a watch is reading the
        keys right now), is dropped first, so an old settled read -- taken before a CDC
        start removed the keys, say -- never opens Finish; the screen then says it is
        reading, and ``reread`` asks it to start a watch at once.
        """
        cdc = _cdc_in_use(session)
        keys, breakable, _defs = _fk_keys_now(cdc)
        reread = False
        invalidate = getattr(migration_state, "invalidate_fk_validation_reads", None)
        if drop_stale_reads and callable(invalidate):
            evidence = validation_evidence_at(validation_state)
            if evidence is not None:
                reread = bool(invalidate(before=evidence))
            opened = (
                getattr(session, "active_view_since", None)
                if getattr(session, "active_view", None) == WorkflowStep.CUT_OVER.value
                else None
            )
            if opened is not None and not migration_state.fk_validation_watch_active():
                reread = bool(invalidate(before=opened)) or reread
        view = _fk_validation_view_of(migration_state)
        in_place, reading, missing = cutover_fk_in_place(keys, pending, view)
        # A read can only come when a watch can run (a job manager, the session's record
        # and a target to read): otherwise "reading" would never end.
        reading = reading and (
            job_manager is not None
            and migration_state is not None
            and getattr(session, "target_config", None) is not None
        )
        gate = cutover_finish_fk_gate(
            cdc_in_use=cdc,
            fk_pending_count=pending,
            fk_apply_result=validation_state.cutover_fk_apply,
            proceed_without_fks=validation_state.proceed_without_foreign_keys,
            inputs_unknown=_cutover_gate_inputs_missing(eval_store, conversion_store, session_id),
            fks_in_place=in_place,
            fks_reading=reading,
        )
        held = cutover_fk_held(breakable, view)
        held_waived = bool(held) and set(held) <= set(
            validation_state.proceed_without_fk_validation
        )
        return SimpleNamespace(
            keys=keys, view=view, in_place=in_place, missing=missing, held=held,
            gate=gate, validation=bool(held) and not held_waived, held_waived=held_waived,
            blocked=gate is not None or (bool(held) and not held_waived),
            reread=reread,
        )

    def _schedule_cutover_defaults_fill(refresh: Callable[[], None]) -> None:
        """Refill the DEFAULT status in the background when its inputs changed (or it
        aged past the TTL). The render itself does no store I/O."""
        if (
            overlay_ledger is None
            or migration_state is None
            or getattr(session, "target_config", None) is None
        ):
            return
        import time as _time

        conv_state = conversion_store.get(session_id) if conversion_store is not None else None
        eval_result = (
            eval_store.get_or_create(session_id).result if eval_store is not None else None
        )
        inventory = eval_result.inventory if eval_result is not None else None
        try:
            key = _cutover_fill_key(
                session, migration_state, validation_state, conv_state, job_manager
            )
        except Exception:  # noqa: BLE001 - no key, no fill (the block says "checking")
            return
        if not validation_state.cutover_defaults_stale(key, _time.monotonic()):
            return
        if validation_state.cutover_defaults_filling:
            return  # a fill for these inputs is already running

        async def _fill() -> None:
            from nicegui import run

            # Claimed HERE, when the fill runs: a timer deleted before it fires (a
            # refresh, a reload) then never holds the claim, and every render that
            # finds the cache stale can arm a replacement.
            if not validation_state.cutover_defaults_stale(key, _time.monotonic()):
                return  # an earlier timer's fill already landed
            generation = validation_state.begin_cutover_defaults_fill()
            if generation is None:
                return
            snap = None
            try:
                snap = await run.io_bound(
                    _fill_cutover_defaults, session, migration_state, conv_state,
                    inventory, overlay_ledger, job_manager,
                    fill_key=key, generation=generation, facts_reader=overlay_facts_reader,
                )
            finally:
                stored = validation_state.finish_cutover_defaults_fill(snap, generation)
            # A result dropped because the inputs moved meanwhile: re-render, so the
            # render arms a fill for the new inputs (it stores, so this cannot loop).
            if stored or (snap is not None and validation_state.cutover_defaults is None):
                try:
                    refresh()
                except Exception:  # noqa: BLE001 - the page went away
                    pass

        ui.timer(0.05, _fill, once=True)

    def _log_cut_over_with_pending_defaults() -> None:
        """At the acknowledgement: DEFAULTs still owed are a WARNING, never a block.
        Reads the cached status only (no I/O)."""
        if overlay_ledger is None or migration_state is None:
            return
        snap = validation_state.cutover_defaults
        uses_ledger = bool(getattr(migration_state, "overlay_ledger_in_use", False)) or bool(
            effective_lob_exclusions(migration_state, None)
        )
        if snap is None or snap.state != "loaded":
            if not uses_ledger:
                return  # the session never used the record: nothing can be owed
            log_activity(
                ActivityCategory.VALIDATION,
                "excluded-column DEFAULTs not restored at cut over",
                status=ActivityStatus.WARNING,
                detail=(
                    "cut over before the tool could confirm the excluded columns' "
                    f"DEFAULTs ({snap.state if snap is not None else 'not checked'})"
                ),
            )
            return
        owed = [i for i in snap.items if i.status in ("pending", "unknown")]
        if not owed:
            return
        shown = ", ".join(
            f"{i.table}.{i.column} DEFAULT {i.original_default}" for i in owed[:8]
        ) + (f" (+{len(owed) - 8} more)" if len(owed) > 8 else "")
        log_activity(
            ActivityCategory.VALIDATION,
            "excluded-column DEFAULTs not restored at cut over",
            status=ActivityStatus.WARNING,
            detail=f"cut over with {len(owed)} DEFAULTs not restored: {shown}",
        )

    def _cutover_scope_missing() -> list[str]:
        """Tables the migration now includes that the last validation never compared (after
        a table-set change: Full load only, G12; a Full load + CDC addition, G17; empty
        otherwise). No I/O."""
        eval_result = (
            eval_store.get_or_create(session_id).result if eval_store is not None else None
        )
        return validation_scope_gap(
            validation_state.result,
            eval_result.inventory if eval_result is not None else None,
            migration_state,
            validation_state.table_exclude,
            # Only tables selected SINCE the load hold: a chip turned off for a loaded
            # table is not kept across a restart, and must not hold it then.
            covered=full_load_covered_tables(migration_state, job_manager),
        )[0]

    def _cutover_load_pending() -> bool:
        """Validation's own prerequisite (Data Migration) is not met: the scope hold then
        points at the step that can be opened. Reads the workflow only."""
        from dsql_migrator.ui.workflow import is_prerequisite_met

        return not is_prerequisite_met(session.workflow, WorkflowStep.VALIDATION)

    def _current_gate(kind: str):
        """One CDC gate (``"dlq"``, ``"ri"`` or ``"g4"``) from the CURRENT state (no I/O),
        for click handlers: each re-check builds only its own gate, when it is reached."""
        _report = validation_state.result
        _summary = summarize_validation(_report) if _report is not None else None
        (gate,) = _cdc_gates(
            migration_state, validation_state, session, summary=_summary, kinds=(kind,),
        )
        return gate

    def _current_dlq_gate():
        """The CDC dead-letter gate from the CURRENT state (no I/O), for click handlers."""
        return _current_gate("dlq")

    def _current_ri_gate():
        """The REPLICA IDENTITY gate from the CURRENT state (no I/O), for click handlers."""
        return _current_gate("ri")

    def _current_g4_gate():
        """The source-schema gate from the CURRENT state (no I/O), for click handlers."""
        return _current_gate("g4")

    def runner() -> None:
        # Re-checked at the acknowledgement (another tab may have ticked a table since the
        # page was drawn): never sign off a table the validation never compared.
        if _cutover_scope_missing():
            ui.notify(  # type: ignore[attr-defined]
                "Validation does not cover the current table set — finish Data Migration "
                "(load the newly selected tables), then re-run Validation."
                if _cutover_load_pending()
                else "Validation does not cover the current table set — re-run Validation "
                "first.",
                type="warning",
            )
            return
        # No job: mark DONE as the user's acknowledgement that they've cut over.
        session.set_workflow(
            with_status(session.workflow, WorkflowStep.CUT_OVER, StepStatus.DONE)
        )
        # Record the cut-over decision -- the journey's conclusion. Which release state
        # the operator acknowledged (a clean match vs an explicitly ACCEPTED gap of
        # permanently-dropped rows) is the single most important audit fact, yet nothing
        # was logged when "I've cut over" was clicked. Compute the same release state the
        # runbook gated on so the log records exactly what was signed off.
        _report = validation_state.result
        _summary = summarize_validation(_report) if _report is not None else None
        _release = cutover_release_state(
            _summary, gap_accepted=validation_state.accept_explained_gap
        )
        _accepted_gap = _release == "accepted"
        _ack_fk_pending = _cutover_pending_foreign_keys(
            session, eval_store, conversion_store, session_id,
            migration_state=migration_state,
        )
        _ack_fk = _fk_gate_now(_ack_fk_pending)
        log_activity(
            ActivityCategory.VALIDATION,
            "cut over acknowledged",
            status=ActivityStatus.SUCCESS,
            detail=(
                "operator acknowledged cut-over to Aurora DSQL ("
                + (
                    "clean match" if _release == "clean"
                    else "ACCEPTED gap: some rows were permanently dropped and the "
                         "operator signed off on migrating without them"
                    if _accepted_gap
                    else f"release state: {_release}"
                )
                + ")"
                # The integrity state, on the line that closes the journey. Without it an
                # auditor reading only the log could not tell whether the target went live
                # with its foreign keys enforced -- the absence of an apply line covers
                # four different situations.
                + "; "
                + _cutover_integrity_detail(
                    validation_state,
                    _ack_fk_pending,
                    inputs_missing=_cutover_gate_inputs_missing(
                        eval_store, conversion_store, session_id
                    ),
                    fks_stripped=_cutover_foreign_keys_stripped(
                        conversion_store, session_id
                    ),
                    identity_missed=_identity_sync_missed(
                        validation_state,
                        _cutover_identity_tables(
                            session, eval_store, conversion_store, session_id,
                            migration_state=migration_state,
                        ),
                    ),
                    standalone_sequences=len(
                        _cutover_standalone_sequences(eval_store, session_id)
                    ),
                    # G142: which keys Aurora DSQL validated, by the last catalog read
                    # (taken after the latest verdict), and whether the waiver of that check
                    # covers the keys it holds on.
                    fk_view=_ack_fk.view,
                    fk_validation_waived=_ack_fk.held_waived,
                )
                # The go-live line must carry the same disclosure as the verdict it
                # signs off; an auditor who reads only the closing line would otherwise
                # see a clean cut-over over an unvalidated column or a count-only sweep.
                + (
                    f"; NOT covered by validation: {_ack_caveats}"
                    if _summary is not None
                    and _report is not None
                    and (
                        _ack_caveats := verdict_evidence_caveats(_summary, _report)
                    )
                    else ""
                )
                # What the CDC dead-letter gate rested on, then the re-keyed tables'
                # REPLICA IDENTITY re-check (inert: MySQL, no re-key, Full load only), then
                # the captured tables' source-schema re-check (inert: Full load only, or no
                # baseline handle) -- nothing for a gate that is inert.
                + "".join(
                    _ack_of(_gate)
                    for _ack_of, _gate in zip(
                        (ack_log_clause, replica_identity_ack_log_clause,
                         catalog_drift_ack_log_clause),
                        _cdc_gates(
                            migration_state, validation_state, session,
                            summary=_summary, release=_release,
                        ),
                    )
                )
            ),
        )
        _log_cut_over_with_pending_defaults()
        if ai_post_event is not None:
            ai_post_event(
                text=(
                    "Cut over acknowledged: live on Aurora DSQL ("
                    + (
                        "clean match"
                        if _release == "clean"
                        else "accepted gap: migrating without permanently-dropped rows"
                        if _accepted_gap
                        else f"release state: {_release}"
                    )
                    + ")"
                ),
                status="success",
            )

    def content(refresh: Callable[[], None]) -> None:
        engine = _source_engine_word(session)
        with _section(ui, icon="rocket_launch", title="Cut over to Aurora DSQL"):
            render_notice(
                ui,
                tone="info",
                header="The final step is yours to perform",
                body=(
                    f"Cut-over is the moment your application stops writing to {engine} "
                    "and starts using Aurora DSQL. The tool has done its job — read "
                    "the source, converted the schema, loaded the data, and proven "
                    "consistency — but repointing your application is an operational "
                    "act only you can do. This step is the runbook for doing it "
                    "safely, with a rollback path."
                ),
            )

        report = validation_state.result
        summary = summarize_validation(report) if report is not None else None
        drift = (
            format_drift(report, postgres_source=engine == "PostgreSQL")
            if report is not None
            else None
        )

        # The runbook's "Sync identity sequences" button calls this: advance identity
        # keys past the current target MAX(pk), in the background, then refresh to show
        # the outcome. Defined here so it closes over this render's report/refresh.
        def _identity_sync_provider() -> None:
            _run_cutover_identity_sync(
                session, validation_state, report,
                job_manager=job_manager, sync=sync_sequences, refresh=refresh,
                extras=lambda names: _identity_sync_extras(
                    session, eval_store, conversion_store, session_id, names,
                    migration_state=migration_state,
                ),
                identity_tables=lambda: _cutover_identity_tables(
                    session, eval_store, conversion_store, session_id,
                    migration_state=migration_state,
                ),
            )

        # For a CDC migration, foreign keys were deferred during the stream; the runbook's
        # "Apply foreign keys" button runs them at cut over (after the final drain).
        def _fk_apply_provider() -> None:
            # Re-read at click, never trusted from the render: a second tab drawn before
            # a CDC start cleared the attestation must not apply foreign keys.
            if _cdc_in_use(session):
                _block = cutover_job_block(migration_state, job_manager)
                if _block or not validation_state.cutover_cdc_drained_confirmed:
                    ui.notify(  # type: ignore[attr-defined]
                        f"Applying foreign keys is {_block}; try again when it finishes."
                        if _block
                        else "Confirm first that source writes are frozen and CDC has "
                        "drained to zero lag.",
                        type="warning",
                    )
                    refresh()
                    return
            _run_cutover_foreign_keys(
                session, validation_state,
                eval_store=eval_store, conversion_store=conversion_store,
                session_id=session_id, job_manager=job_manager, refresh=refresh,
                migration_state=migration_state,
            )

        _fk_pending = _cutover_pending_foreign_keys(
            session, eval_store, conversion_store, session_id,
            migration_state=migration_state,
        )
        _identity_tables = _cutover_identity_tables(
            session, eval_store, conversion_store, session_id,
            migration_state=migration_state,
        )
        _identity_pending = len(_identity_tables)
        # Both counts above are 0 when their schema inputs are missing, and 0 hides the
        # action. Distinguish "nothing to apply" from "can't tell" so a session without
        # those inputs is told so instead of shown a silently complete runbook.
        _gate_inputs_unknown = _cutover_gate_inputs_missing(
            eval_store, conversion_store, session_id
        )

        # Dev-only UI review: with no clean verdict, synthesize a ready summary so
        # the runbook itself can be reviewed without running the whole workflow.
        # Gated on the same dev flag as the nav unlock; never reached in real use.
        if (summary is None or not summary.ready_for_cutover) and _dev_unlock_steps():
            render_notice(
                ui,
                tone="warning",
                header="Developer preview (no real validation verdict)",
                body=(
                    "DSQL_MIGRATOR_DEV_UNLOCK_STEPS is on, so the cut-over runbook "
                    "below is shown for UI review only. In real use it appears only "
                    "after Validation reports a clean MATCH."
                ),
            )
            summary = summary or _cutover_summary_for_preview()
            _render_cutover_section(
                ui, summary, drift,
                cdc_in_use=_cdc_in_use(session), source_engine=engine,
            )
            return

        # After a table-set change (Full load only, G12; a Full load + CDC addition, G17): a
        # result that has no row for a table the migration now includes cannot release cut
        # over, whatever it says about the tables it did compare. An extra hold -- every
        # release gate below is unchanged.
        _scope_missing = _cutover_scope_missing()
        if _scope_missing:
            _listed = bounded_table_names(list(_scope_missing))
            _one = len(_scope_missing) == 1
            # Validation cannot be opened while Data Migration is not complete (a table
            # ticked since the load turns it Failed until loaded): lead with that step.
            _body = (
                (
                    "Data Migration is not complete for the current table set, which "
                    f"includes {_listed} (selected after the last validation ran). Finish "
                    "it on the Data Migration step (Retry unfinished tables loads what is "
                    f"left, or untick {'it' if _one else 'them'} there), then re-run "
                    "Validation; the cut-over runbook appears here once the result covers "
                    "every migrated table."
                )
                if _cutover_load_pending()
                else (
                    f"The last validation has no result for {_listed}, part of this "
                    "migration's table set (selected after it ran, or left out of it). "
                    f"Re-run Validation (or turn {'its' if _one else 'their'} chip off "
                    f"under Objects to validate to leave {'it' if _one else 'them'} "
                    "out); the cut-over runbook appears here once the result covers "
                    "every migrated table."
                )
            )
            with _section(ui, icon="fact_check", title="Validate the current table set"):
                render_notice(
                    ui,
                    tone="warning",
                    header="Validation does not cover the current table set",
                    body=_body,
                )
            return

        release = cutover_release_state(
            summary, gap_accepted=validation_state.accept_explained_gap
        )

        # The gate's own copy has always promised cut-over is reachable when "every
        # difference is explained" -- but the gate tested ``ready_for_cutover``, which is
        # a bare match. So a run whose ONLY finding was a permanently quarantined row
        # could never reach cut-over, and no amount of reloading would change that: the
        # value is one DSQL cannot store. The step was unreachable by design rather than
        # by any decision the operator made. Offer the acknowledgement instead.
        if release == "acceptable":
            rows = summary.quarantine_explained_rows  # type: ignore[union-attr]
            tables = summary.quarantine_explained_tables  # type: ignore[union-attr]
            row_noun = "row" if rows == 1 else "rows"
            table_noun = "table" if len(tables) == 1 else "tables"
            with _section(ui, icon="fact_check", title="Acknowledge the known gap"):
                render_notice(
                    ui,
                    tone="warning",
                    header="Every difference is explained — cut-over needs your sign-off",
                    body=(
                        f"Validation found no unexplained difference. The {rows} "
                        f"{row_noun} missing from {len(tables)} {table_noun} "
                        f"({', '.join(tables)}) are exactly the rows the migration "
                        "already reported dropping, because DSQL could not store those "
                        "values — reloading will not change that. Cutting over means "
                        f"accepting that those {row_noun} will not exist on the target. "
                        "Confirm below to unlock the runbook, or fix the source value(s) "
                        "and re-run Validation for a full match instead."
                    ),
                )

                def _accept() -> None:
                    validation_state.accept_explained_gap = True
                    refresh()

                ui.button(  # type: ignore[attr-defined]
                    f"Accept the {rows}-{row_noun} gap and continue to cut-over",
                    icon="check",
                    on_click=_accept,
                ).props("no-caps color=primary")
            return

        # Row counts match, but record-level reconciliation ran on NO table (all
        # composite/non-integer PK) in a non-CHECKSUM mode. A row-count match does NOT
        # prove the record sets match, so this is a soft-block, not a go: show the
        # runbook only once the records are actually verified (re-run in CHECKSUM).
        if release == "unverified":
            with _section(
                ui, icon="fact_check", title="Verify records before cutting over"
            ):
                _inapplicable = summary.reconcile_inapplicable_tables  # type: ignore[union-attr]
                render_notice(
                    ui,
                    tone="warning",
                    header="Row counts match, but records were never reconciled",
                    body=(
                        f"All {summary.total_tables} table(s) have matching row counts, "  # type: ignore[union-attr]
                        "but record-level reconciliation could not run for any table "
                        "(none has a single-column integer primary key), so a "
                        "same-count-but-different-rows divergence would go undetected. "
                        "Re-run Validation in CHECKSUM mode (it value-compares every row) "
                        "before cutting over — the cut-over runbook appears here once the "
                        "records are verified. Reconcile inapplicable for "
                        f"{len(_inapplicable)} table(s): {', '.join(_inapplicable)}."
                    ),
                )
            return

        if release == "blocked":
            with _section(ui, icon="fact_check", title="Validate first"):
                render_notice(
                    ui,
                    tone="warning",
                    header="Get a clean validation before you cut over",
                    body=(
                        "Cut over only when Validation reports a clean MATCH, or when "
                        "every difference is explained by rows the migration already "
                        "reported dropping (you can then acknowledge that gap here). Go "
                        "to the Validation step and run it; the cut-over runbook appears "
                        "here once there is nothing unexplained left."
                        if summary is not None
                        else
                        "No validation result yet. Run the Validation step first — "
                        "the cut-over runbook appears here once it reports a clean "
                        "MATCH."
                    ),
                )
            return

        # The CDC dead-letter gate (ui/cdc_dlq_gate.py), evaluated only AFTER the release
        # gate above has released -- so nothing here can ever override a mismatch, an
        # unverified record set or an unaccepted gap. Inert (nothing drawn, nothing armed)
        # when the session has no CDC evidence, so Full Load only is unchanged.
        _dlq_gate = cdc_dlq_gate_for(
            migration_state, validation_state,
            cdc_in_use=_cdc_in_use(session), release=release, summary=summary,
        )
        if _dlq_gate.read_required:
            # One bounded background read (never on this render), TTL-throttled.
            schedule_cdc_dlq_check(
                ui, session, migration_state, refresh,
                evidence_at=validation_evidence_at(validation_state),
            )
        # The re-keyed tables' REPLICA IDENTITY gate (ui/cdc_replica_identity_gate.py), also
        # only after the release gate: inert for MySQL, no re-key and Full load only.
        _ri_gate = replica_identity_gate_for(
            migration_state, validation_state, session,
            cdc_in_use=_cdc_in_use(session), release=release, summary=summary,
        )
        if _ri_gate.probe_state is not None and has_source_connection(session):
            # One bounded background read (never on this render), TTL-throttled; Cut over
            # waits for one that started after the latest validation.
            schedule_replica_identity_probe(
                ui, session, migration_state, refresh,
                evidence_at=validation_evidence_at(validation_state),
            )
        _held = False
        if _dlq_gate.held:
            def _waive_dlq_check() -> None:
                current = _current_dlq_gate()
                if not current.can_waive:
                    refresh()  # the state moved since this was drawn: show it
                    return
                log_activity(
                    ActivityCategory.VALIDATION,
                    "CDC dead-letter check waived at cut over",
                    status=ActivityStatus.WARNING,
                    detail=waiver_log_detail(current),
                )
                validation_state.cutover_dlq_waiver = CdcDlqWaiver(
                    tables={i.table: i.count for i in current.waivable_issues},
                    read_problem=current.read_problem,
                    waived_at=datetime.now(timezone.utc),
                )
                refresh()

            _check_btn: dict = {}

            def _check_dlq_again() -> None:
                # No refresh here: it would delete the one-shot timer before it fires. The
                # read's own completion refreshes the screen.
                schedule_cdc_dlq_check(
                    ui, session, migration_state, refresh,
                    evidence_at=validation_evidence_at(validation_state), force=True,
                )
                btn = _check_btn.get("button")
                if btn is not None:
                    btn.props("disable")

            with _section(ui, icon="fact_check", title="CDC dead-letter check"):
                _render_cdc_dlq_gate(
                    ui, _dlq_gate,
                    on_waive=_waive_dlq_check,
                    on_check_again=_check_dlq_again,
                    check_running=bool(
                        getattr(migration_state, "cdc_dlq_check_running", False)
                    ),
                    check_button=_check_btn,
                )
            _held = True
        _ri_check_btn: dict = {}

        def _check_ri_again() -> None:
            # No refresh here (it would delete the one-shot timer before it fires); the
            # read's own completion refreshes the screen.
            schedule_replica_identity_probe(
                ui, session, migration_state, refresh,
                evidence_at=validation_evidence_at(validation_state), force=True,
            )
            btn = _ri_check_btn.get("button")
            if btn is not None:
                btn.props("disable")

        _ri_running = bool(
            getattr(migration_state, "cdc_replica_identity_probe_running", False)
        )
        if _ri_gate.held:
            def _waive_ri_check() -> None:
                current = _current_ri_gate()
                if not current.can_waive:
                    refresh()  # the state moved since this was drawn: show it
                    return
                log_activity(
                    ActivityCategory.VALIDATION,
                    "REPLICA IDENTITY CHECKSUM proof waived at cut over",
                    status=ActivityStatus.WARNING,
                    detail=replica_identity_waiver_log_detail(current),
                )
                validation_state.cutover_replica_identity_waiver = CdcReplicaIdentityWaiver(
                    tables={
                        i.table: i.finding.restored_at  # type: ignore[attr-defined]
                        for i in current.waivable
                    },
                    waived_at=datetime.now(timezone.utc),
                )
                refresh()

            with _section(ui, icon="fact_check", title="Source REPLICA IDENTITY check"):
                _render_replica_identity_gate(
                    ui, _ri_gate,
                    on_waive=_waive_ri_check,
                    on_check_again=_check_ri_again,
                    check_running=_ri_running,
                    check_button=_ri_check_btn,
                )
            _held = True
        # The captured tables' source-schema gate (ui/cdc_catalog_drift_gate.py), also only
        # after the release gate: inert for Full load only and without the baseline handle.
        _g4_gate = catalog_drift_gate_for(
            migration_state, validation_state, session,
            cdc_in_use=_cdc_in_use(session), release=release, summary=summary,
        )
        if _g4_gate.probe_state not in (None, G4_PROBE_NO_BASELINE) and has_source_connection(
            session
        ):
            # One bounded background read (never on this render), TTL-throttled; Cut over
            # waits for one that started after the latest validation or baseline change.
            schedule_catalog_drift_probe(
                ui, session, migration_state, refresh,
                evidence_at=validation_evidence_at(validation_state),
            )
        _g4_check_btn: dict = {}

        def _check_g4_again() -> None:
            # No refresh here (it would delete the one-shot timer before it fires); the
            # read's own completion refreshes the screen.
            schedule_catalog_drift_probe(
                ui, session, migration_state, refresh,
                evidence_at=validation_evidence_at(validation_state), force=True,
            )
            btn = _g4_check_btn.get("button")
            if btn is not None:
                btn.props("disable")

        _g4_running = bool(
            getattr(migration_state, "cdc_catalog_drift_probe_running", False)
        )
        if _g4_gate.held:
            def _waive_g4_check() -> None:
                current = _current_g4_gate()
                if not current.can_waive:
                    refresh()  # the state moved since this was drawn: show it
                    return
                log_activity(
                    ActivityCategory.VALIDATION,
                    "source schema CHECKSUM proof waived at cut over",
                    status=ActivityStatus.WARNING,
                    detail=catalog_drift_waiver_log_detail(current),
                )
                validation_state.cutover_catalog_drift_waiver = CdcCatalogDriftWaiver(
                    tables={
                        i.table: i.finding.resolved_at  # type: ignore[attr-defined]
                        for i in current.waivable
                    },
                    waived_at=datetime.now(timezone.utc),
                )
                refresh()

            with _section(ui, icon="fact_check", title="Source schema check"):
                _render_catalog_drift_gate(
                    ui, _g4_gate,
                    on_waive=_waive_g4_check,
                    on_check_again=_check_g4_again,
                    check_running=_g4_running,
                    check_button=_g4_check_btn,
                )
            _held = True
        if _held:
            return
        if _dlq_gate.waived:
            _w_header, _w_body = waived_notice(_dlq_gate)
            render_notice(ui, tone="warning", header=_w_header, body=_w_body)
        _unverifiable = unverifiable_notice(_dlq_gate)
        if _unverifiable is not None:
            render_notice(
                ui, tone="warning", header=_unverifiable[0], body=_unverifiable[1]
            )
        _ri_waived = ri_waived_notice(_ri_gate)
        if _ri_waived is not None:
            render_notice(ui, tone="warning", header=_ri_waived[0], body=_ri_waived[1])
        # FAIL-OPEN, by design, when the source could not be re-read (no password after a
        # restart, a failed read, a table not found): warned here and recorded in the
        # acknowledgement line, never a dead end.
        _ri_unchecked = ri_unchecked_notice(_ri_gate)
        if _ri_unchecked is not None:
            render_notice(ui, tone="warning", header=_ri_unchecked[0], body=_ri_unchecked[1])
            if _ri_gate.probe_state == RI_PROBE_FAILED and has_source_connection(session):
                with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                    _ri_btn = ui.button(  # type: ignore[attr-defined]
                        "Check again", icon="refresh", on_click=_check_ri_again,
                    ).props("no-caps color=primary")
                    if _ri_running:
                        _ri_btn.props("disable")
                    _ri_check_btn["button"] = _ri_btn
        _g4_waived = g4_waived_notice(_g4_gate)
        if _g4_waived is not None:
            render_notice(ui, tone="warning", header=_g4_waived[0], body=_g4_waived[1])
        # FAIL-OPEN, by design, when the source schema could not be re-read (no password
        # after a restart, no Step 1 inventory, a failed read): warned here and recorded in
        # the acknowledgement line, never a dead end.
        _g4_unchecked = g4_unchecked_notice(_g4_gate)
        if _g4_unchecked is not None:
            render_notice(ui, tone="warning", header=_g4_unchecked[0], body=_g4_unchecked[1])
            if _g4_gate.probe_state == RI_PROBE_FAILED and has_source_connection(session):
                with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                    _g4_btn = ui.button(  # type: ignore[attr-defined]
                        "Check again", icon="refresh", on_click=_check_g4_again,
                    ).props("no-caps color=primary")
                    if _g4_running:
                        _g4_btn.props("disable")
                    _g4_check_btn["button"] = _g4_btn

        # Released with an accepted gap: the runbook follows, but it must not read as a
        # clean match -- the operator is cutting over to a target that is knowingly short.
        if release == "accepted":
            rows = summary.quarantine_explained_rows  # type: ignore[union-attr]
            tables = summary.quarantine_explained_tables  # type: ignore[union-attr]
            row_noun = "row" if rows == 1 else "rows"
            render_notice(
                ui,
                tone="warning",
                header="Cutting over with an accepted gap",
                body=(
                    f"You accepted that {rows} {row_noun} could not be migrated "
                    f"({', '.join(tables)}). Everything else matched. Those {row_noun} "
                    "will be absent on DSQL after cut-over — keep the source available "
                    "if you may still need them."
                ),
            )

        # The excluded columns' DEFAULT status for the runbook below (background read).
        _schedule_cutover_defaults_fill(refresh)
        _defaults_on = (
            overlay_ledger is not None
            and migration_state is not None
            and getattr(session, "target_config", None) is not None
        )
        # Only a session that uses the record sees its state (as the Full Load: an
        # unreadable ledger matters only to a session that uses it). A readable fill
        # that finds records marks the session, so owed DEFAULTs still surface.
        _uses_ledger = _defaults_on and (
            bool(getattr(migration_state, "overlay_ledger_in_use", False))
            or bool(effective_lob_exclusions(migration_state, None))
        )
        _defaults = validation_state.cutover_defaults if _defaults_on else None
        if _defaults is not None and not _uses_ledger and _defaults.state != "loaded":
            _defaults = None
        _restore_block = cutover_job_block(migration_state, job_manager) if _defaults_on else None
        _restore_running = (
            validation_state.cutover_default_restore_in_flight(job_manager)
            if _defaults_on
            else False
        )
        # One registry of the drained-gated buttons, shared by the runbook and the Finish
        # area below, so the one checkbox enables every one of them.
        _drained_gated: list = []

        def _fill_key_now() -> object:
            return _cutover_fill_key(
                session, migration_state, validation_state,
                conversion_store.get(session_id) if conversion_store is not None else None,
                job_manager,
            )

        def _restore_provider(request: "Optional[tuple[str, str]]" = None) -> None:
            _run_cutover_default_restore(
                session, validation_state, migration_state, request=request,
                overlay_ledger=overlay_ledger, job_manager=job_manager, refresh=refresh,
                notify=ui.notify,  # type: ignore[attr-defined]
                fill_key_now=_fill_key_now,
            )

        def _recheck_defaults() -> None:
            validation_state.invalidate_cutover_defaults()
            refresh()

        # G142: Aurora DSQL's verdict on the foreign keys in place -- the persisted VALIDATE
        # job ids under the last catalog read, which a background watch takes (never this
        # render, and never from session memory alone: the record outlives a re-render, a
        # Validation re-run and a restart). Only a read taken after the latest Validation
        # verdict counts here: an older one is dropped now and read again. The Finish gate
        # waits only for the keys to be in place, except the keys a MySQL CDC stream can
        # break.
        from dsql_migrator.ui.data_migration._fk_validation import (
            _job_live as _fk_job_live,
            fk_watch_due,
            start_fk_validation_watch,
            submit_fk_revalidation,
        )

        _fk_now = _fk_gate_now(_fk_pending, drop_stale_reads=True)
        _fk_view = _fk_now.view

        def _fk_connect():
            target = getattr(session, "target_config", None)
            if target is None:
                return None
            from dsql_migrator.core.target_connection import DsqlConnector

            return DsqlConnector(target, aws_profile=getattr(session, "aws_profile", None)).connect

        def _fk_defs_now() -> dict:
            return _fk_keys_now(_cdc_in_use(session))[2]

        def _fk_validate_again(keys: "Sequence[tuple[str, str]]") -> None:
            # No drained attestation: VALIDATE only reads, on Aurora DSQL, and every key in
            # place already enforces each new write.
            submit_fk_revalidation(
                migration_state, job_manager, connection_factory=_fk_connect(),
                keys=list(keys), fk_defs=_fk_defs_now,
                category=ActivityCategory.VALIDATION, refresh=refresh,
            )
            refresh()

        # A read just dropped as older than the latest verdict or this screen's opening is
        # taken again at once (the watch's rest after its last run does not apply: the
        # next read is newer, so this cannot repeat).
        if _fk_now.reread or fk_watch_due(migration_state, _fk_view, job_manager):
            start_fk_validation_watch(
                migration_state, job_manager, connection_factory=_fk_connect(),
                fk_defs=_fk_defs_now, category=ActivityCategory.VALIDATION, refresh=refresh,
            )
        # The cut-over pass is running when its job is live and it has not recorded its
        # result yet: its own last refresh runs inside the job, and must show the result.
        _fk_apply_running = validation_state.cutover_fk_apply is None and _fk_job_live(
            job_manager, validation_state.cutover_fk_apply_job_id
        )
        if _fk_apply_running:
            # Come back while it runs, so "Adding foreign keys: X of N" moves and the result
            # shows even when the pass's own refresh is lost (as the Full load card does).
            ui.timer(_FK_APPLY_POLL_SECONDS, refresh, once=True)  # type: ignore[attr-defined]
        elif (
            _fk_view is not None
            and not _fk_view.settled
            and not migration_state.fk_validation_watch_active()
        ):
            # Nothing is reading it right now (the last watch ran out of time, or a read is
            # due): come back, so a watch starts again and the result shows when it lands.
            ui.timer(_FK_VALIDATION_REFRESH_SECONDS, refresh, once=True)  # type: ignore[attr-defined]

        # Go path: the verdict is clean — show the tailored runbook + an explicit
        # acknowledgement that marks the step (and the whole journey) Done. The runbook
        # includes an explicit "Sync identity sequences" action (an operator step, not a
        # render side-effect): identity keys must be advanced past the current target
        # MAX(pk) BEFORE the app repoints, or its first insert collides (23505).
        _render_cutover_section(
            ui, summary, drift,
            cdc_in_use=_cdc_in_use(session),
            source_engine=engine,
            identity_sync_provider=_identity_sync_provider,
            identity_sync_result=validation_state.cutover_identity_sync,
            identity_sync_failed=validation_state.cutover_identity_sync_failed,
            identity_sync_missed=_identity_sync_missed(validation_state, _identity_tables),
            standalone_sequences=_cutover_standalone_sequences(eval_store, session_id),
            fk_apply_provider=_fk_apply_provider,
            fk_apply_result=validation_state.cutover_fk_apply,
            fk_pending_count=_fk_pending,
            fk_apply_running=_fk_apply_running,
            fk_apply_progress=validation_state.cutover_fk_apply_progress,
            fk_validation=_fk_view,
            # The keys Finish waits for (MySQL CDC, breakable, in place, not validated): the
            # block says so for each instead of "does not hold the next step" -- not once
            # that hold's own waiver lifted it or the cut over is done.
            fk_holding=(
                frozenset()
                if _fk_now.held_waived
                or get_status(session.workflow, WorkflowStep.CUT_OVER) is StepStatus.DONE
                else frozenset(_fk_now.held)
            ),
            on_validate_again=(
                _fk_validate_again
                if migration_state is not None and getattr(session, "target_config", None)
                else None
            ),
            fk_revalidating=_fk_job_live(
                job_manager, getattr(migration_state, "fk_revalidation_job_id", None)
            ),
            identity_pending_count=_identity_pending,
            cdc_drained_confirmed=validation_state.cutover_cdc_drained_confirmed,
            on_cdc_drained_change=validation_state.set_cutover_cdc_drained_confirmed,
            inputs_unknown=_gate_inputs_unknown,
            default_restore=_defaults,
            defaults_checking=_uses_ledger and _defaults is None,
            restore_provider=_restore_provider if _defaults_on else None,
            restore_job_block=_restore_block,
            restore_running=_restore_running,
            restore_outcome=validation_state.cutover_default_restore_outcome,
            on_recheck_defaults=_recheck_defaults,
            drained_gated=_drained_gated,
        )

        done = get_status(session.workflow, WorkflowStep.CUT_OVER) is StepStatus.DONE
        with _section(ui, icon="flag", title="Finish"):
            if done:
                render_notice(
                    ui,
                    tone="success",
                    header="Cut-over complete",
                    body=(
                        "You've marked the cut-over done — your application is live "
                        "on Aurora DSQL. Keep the source as a read-only rollback "
                        "anchor until you've signed off."
                    ),
                )
            else:
                # For a CDC migration the deferred foreign keys are created ONLY by the
                # "Apply foreign keys" button above (Full Load's post-pass is a no-op for
                # CDC), so acknowledging cut over with them never applied would silently
                # leave the target with zero (or partial) enforced FKs. Two soft-blocks,
                # each with its own explicit opt-out (FIX 4, G142): the keys not in place,
                # and the keys in place a MySQL CDC stream can break that Aurora DSQL has
                # not validated -- shown side by side, so lifting one never hides the other.
                _fk_gate = _fk_now.gate
                if _fk_gate is not None:
                    _fk_result = validation_state.cutover_fk_apply
                    _fk_not_in_place = (
                        max(0, _fk_pending - int(_fk_result[0]))
                        if _fk_gate == "incomplete" and _fk_result is not None
                        else len(_fk_now.missing) or _fk_pending
                    )
                    if _fk_gate == "unknown":
                        _gate_header = "Can't confirm whether foreign keys are applied"
                        _gate_body = (
                            "The schema inputs this check reads (the Step 1 evaluation "
                            "and your Step 2 conversion choices) are not loaded in this "
                            "session, so the tool cannot say whether the foreign keys "
                            "deferred during CDC still need applying. Re-run Evaluation "
                            "(or reload the page to resume the session) and come back, "
                            "or explicitly choose to cut over without enforced foreign "
                            "keys below."
                        )
                    elif _fk_gate == "checking":
                        _gate_header = "Confirming the foreign keys are in place"
                        _gate_body = (
                            f"This schema has {_fk_keys_phrase(_fk_pending)} to be in place "
                            "before you finish. Aurora DSQL's catalog is being read now (a "
                            "read taken after the latest validation); this updates when it "
                            "lands. You can also apply them again above (keys already in "
                            "place are left as they are), or explicitly choose to cut over "
                            "without them below."
                        )
                    elif _fk_gate == "pending" and _fk_now.missing:
                        _gate_header = "Foreign keys are not applied yet"
                        _one = len(_fk_now.missing) == 1
                        _gate_body = (
                            f"{_fk_keys_phrase(len(_fk_now.missing))} of this schema's "
                            f"{_fk_pending} {'is' if _one else 'are'} not on Aurora DSQL "
                            f"({bounded_table_names(_fk_now.missing)}). Use \"Apply foreign "
                            "keys\" above before finishing (keys already in place are left "
                            "as they are), or explicitly choose to cut over without them "
                            "below."
                        )
                    elif _fk_gate == "pending":
                        # Never applied, or removed since (Start CDC's "Remove foreign keys",
                        # a reload): the record holds none of them.
                        _it = "it" if _fk_pending == 1 else "them"
                        _gate_header = "Foreign keys are not applied yet"
                        _gate_body = (
                            f"This schema's {_fk_keys_phrase(_fk_pending, preserved=True)} "
                            f"{'is' if _fk_pending == 1 else 'are'} not in place on the "
                            f"target. CDC streams without {_it}, so only \"Apply foreign "
                            f"keys\" above adds {_it}: after the final drain, and again "
                            f"whenever Start CDC or a reload removed {_it}. Apply {_it} "
                            "before finishing, or explicitly choose to cut over without "
                            f"{_it} below."
                        )
                    else:
                        _gate_header = "Foreign keys are not applied yet"
                        _gate_body = (
                            "Some preserved foreign keys were skipped (orphan rows) or "
                            "failed to apply, so referential integrity is not fully "
                            "enforced. Use \"Apply foreign keys\" above before finishing, "
                            "or explicitly choose to cut over without them below."
                        )
                    render_notice(
                        ui,
                        tone="warning",
                        header=_gate_header,
                        body=_gate_body,
                    )

                    def _proceed_without_fks() -> None:
                        # The one decision on this screen that CANNOT be reconstructed
                        # afterwards: nothing else records that the operator chose to go
                        # live with referential integrity unenforced (or unproven), and the
                        # absence of an "apply foreign keys" line is ambiguous between "no
                        # FKs in the schema", "applied", "never run" and "waived".
                        log_activity(
                            ActivityCategory.VALIDATION,
                            "foreign keys waived at cut over",
                            status=ActivityStatus.WARNING,
                            detail=(
                                "operator chose to cut over before the tool confirmed "
                                f"that the {_fk_pending} preserved foreign key(s) are in "
                                "place — referential integrity is the application's "
                                "responsibility on the target"
                                if _fk_gate == "checking"
                                else "operator chose to cut over WITHOUT applying "
                                f"{_fk_not_in_place} preserved foreign key(s) — referential "
                                "integrity is the application's responsibility on the target"
                            ),
                        )
                        # This hold only: never the validation hold beside it.
                        validation_state.waive_foreign_keys()
                        refresh()

                    ui.button(  # type: ignore[attr-defined]
                        "Cut over without enforced foreign keys",
                        icon="link_off",
                        on_click=_proceed_without_fks,
                    ).props("no-caps color=warning outline")
                if _fk_now.validation:
                    # G142: these keys are in place, but a MySQL CDC stream can break them
                    # without a trace the final Validation sees.
                    _fk_held = dict(_fk_now.held)
                    _fk_held_names = [f"{table}.{constraint}" for table, constraint in _fk_held]
                    _check_header, _check_body = _cutover_orphans_hold_copy(_fk_held)
                    render_notice(ui, tone="warning", header=_check_header, body=_check_body)

                    def _proceed_without_validation() -> None:
                        log_activity(
                            ActivityCategory.VALIDATION,
                            "foreign keys waived at cut over",
                            status=ActivityStatus.WARNING,
                            detail=(
                                f"operator chose to cut over with {len(_fk_held_names)} "
                                "foreign key(s) Aurora DSQL has NOT validated "
                                f"({_bounded_names(_fk_held_names)}) — they are in place "
                                "and refuse every new orphan write, but the rows already "
                                "on the target are not proven to satisfy them"
                            ),
                        )
                        # These keys only: a key held later holds Finish again.
                        validation_state.waive_fk_validation(_fk_held)
                        refresh()

                    ui.button(  # type: ignore[attr-defined]
                        "Cut over without this check",
                        icon="link_off",
                        on_click=_proceed_without_validation,
                    ).props("no-caps color=warning outline")
                # DEFAULTs still owed: a warning with the action, never a block (the
                # acknowledgement logs it). Applications inserting without the column
                # would store NULL where the source had a DEFAULT.
                if _defaults is not None and _defaults.state == "loaded" and _defaults.outstanding:
                    render_notice(
                        ui,
                        tone="warning",
                        header=(
                            f"{_defaults.pending} excluded column(s) still have no DEFAULT "
                            "on Aurora DSQL"
                            if _defaults.pending
                            else f"{_defaults.unknown} excluded column(s) could not be "
                            "checked on Aurora DSQL"
                        ),
                        body=" ".join(
                            part
                            for part in (
                                "Your application's inserts will store NULL there where "
                                "the source would have used a DEFAULT."
                                if _defaults.pending
                                else "",
                                "Restore them now." if _defaults.auto_items else "",
                                "Decide on the columns listed under \"Needs your "
                                "decision\" above."
                                if _defaults.manual_items
                                else "",
                                f"{_defaults.unknown} column(s) could not be read on Aurora "
                                "DSQL, so the tool cannot tell whether their DEFAULT is "
                                "back; use \"Check again\" above before you repoint."
                                if _defaults.unknown
                                else "",
                            )
                            if part
                        ),
                    )
                    if _defaults.auto_items:
                        _fin_btn = ui.button(  # type: ignore[attr-defined]
                            "Restore DEFAULTs",
                            icon="settings_backup_restore",
                            on_click=lambda: _restore_provider(None),
                        ).props("color=primary outline no-caps")
                        _fin_other = bool(_restore_block) or _restore_running
                        if _fin_other or (
                            _cdc_in_use(session)
                            and not validation_state.cutover_cdc_drained_confirmed
                        ):
                            _fin_btn.props("disable")
                        _drained_gated.append((_fin_btn, _fin_other))
                elif _uses_ledger and _defaults is None:
                    inline_hint(
                        ui,
                        "Checking whether the excluded columns' DEFAULTs are back on "
                        "Aurora DSQL.",
                        tone="info",
                    )
                ui.label(  # type: ignore[attr-defined]
                    "When your application is live on DSQL and smoke-tested, mark "
                    "the cut-over complete to finish the migration journey."
                ).classes("text-sm text-gray-600")
                with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                    def _acknowledge() -> None:
                        if _fk_now.blocked:
                            return  # gated: a foreign-key hold stands without its opt-out
                        if _fk_gate_now(_fk_pending).blocked:
                            # Re-read from state at the click (no I/O): a read or a CDC
                            # start since this page was drawn moved the foreign keys.
                            refresh()
                            return
                        # Re-evaluated from state at the click (no I/O): a dead letter folded
                        # after this page was drawn must not be signed off unseen.
                        _now = _current_dlq_gate()
                        if _now.applies and bool(
                            getattr(migration_state, "cdc_dlq_check_running", False)
                        ):
                            ui.notify(  # type: ignore[attr-defined]
                                "Checking the CDC dead-letter queue — try again in a few "
                                "seconds.",
                                type="info",
                            )
                            return
                        if _now.held:
                            ui.notify(  # type: ignore[attr-defined]
                                "New CDC dead-letter information arrived since this page "
                                "was drawn — review the check above.",
                                type="warning",
                            )
                            refresh()
                            return
                        _ri_now = _current_ri_gate()
                        if _ri_now.applies and bool(
                            getattr(
                                migration_state, "cdc_replica_identity_probe_running", False
                            )
                        ):
                            ui.notify(  # type: ignore[attr-defined]
                                "Re-reading REPLICA IDENTITY on the source — try again in "
                                "a few seconds.",
                                type="info",
                            )
                            return
                        if _ri_now.held:
                            ui.notify(  # type: ignore[attr-defined]
                                "New REPLICA IDENTITY information arrived since this page "
                                "was drawn — review the check above.",
                                type="warning",
                            )
                            refresh()
                            return
                        _g4_now = _current_g4_gate()
                        if _g4_now.applies and bool(
                            getattr(migration_state, "cdc_catalog_drift_probe_running", False)
                        ):
                            ui.notify(  # type: ignore[attr-defined]
                                "Re-reading the source schema — try again in a few seconds.",
                                type="info",
                            )
                            return
                        if _g4_now.held:
                            ui.notify(  # type: ignore[attr-defined]
                                "New source schema information arrived since this page was "
                                "drawn — review the check above.",
                                type="warning",
                            )
                            refresh()
                            return
                        runner()
                        refresh()

                    _ack_btn = ui.button(  # type: ignore[attr-defined]
                        "I've cut over to DSQL",
                        icon="check_circle",
                        on_click=_acknowledge,
                    ).props("color=primary")
                    # Disabled (not hidden) while a foreign-key hold stands, so the operator
                    # sees the path forward is "apply / validate the FKs or opt out", not a
                    # missing button. The handler is a no-op belt-and-suspenders if reached.
                    if _fk_now.blocked:
                        _ack_btn.props("disable")

    return content, runner


def _render_cdc_dlq_gate(
    ui: object,
    gate: object,
    *,
    on_waive: Callable[[], None],
    on_check_again: Callable[[], None],
    check_running: bool,
    check_button: "Optional[dict]" = None,
) -> None:
    """Render the held CDC dead-letter gate: why, which tables, what to do, the actions.

    Copy comes from :mod:`dsql_migrator.ui.cdc_dlq_gate` (value-free: names, counts,
    times, error classes). The read-state notice is ``error`` for a failed or page-capped
    read, ``warning`` for a history that can't be confirmed (its way forward is one full
    CHECKSUM run that clears it by itself, like the per-table re-check) and ``info`` while
    the fresh read is in flight; the per-table notice is ``warning`` (a re-check, the
    normal way forward). "Continue without this check" appears only when nothing
    re-checkable is left; "Check again" only for a failed or partial read, disabled while
    a read runs.
    """
    read = read_state_notice(gate)  # type: ignore[arg-type]
    if read is not None:
        render_notice(ui, tone=read[0], header=read[1], body=read[2])
    issue = issue_notice(gate)  # type: ignore[arg-type]
    if issue is not None:
        render_notice(ui, tone=issue[0], header=issue[1], body=issue[2])
        with ui.column().classes("w-full gap-1"):  # type: ignore[attr-defined]
            for line in issue_lines(gate):  # type: ignore[arg-type]
                ui.label(line).classes("text-sm text-gray-700")  # type: ignore[attr-defined]
    steps = way_forward_lines(gate)  # type: ignore[arg-type]
    if steps:
        ui.label("What to do").classes(  # type: ignore[attr-defined]
            "text-sm font-medium text-gray-900 mt-1"
        )
        with ui.column().classes("w-full gap-1.5"):  # type: ignore[attr-defined]
            for num, text in enumerate(steps, start=1):
                with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                    ui.label(str(num)).classes(  # type: ignore[attr-defined]
                        "shrink-0 w-5 h-5 rounded-full bg-blue-100 text-blue-700 "
                        "text-[11px] font-semibold flex items-center justify-center"
                    )
                    ui.label(text).classes("text-sm text-gray-700 leading-snug")  # type: ignore[attr-defined]
    show_check = bool(getattr(gate, "read_required", False)) and getattr(
        gate, "read_state", None
    ) in (READ_FAILED, READ_INCOMPLETE)
    can_waive = bool(getattr(gate, "can_waive", False))
    if not (show_check or can_waive):
        return
    with ui.row().classes("w-full justify-end gap-2"):  # type: ignore[attr-defined]
        if can_waive:
            ui.button(  # type: ignore[attr-defined]
                "Continue without this check", icon="rule", on_click=on_waive,
            ).props("no-caps color=warning outline")
        if show_check:
            button = ui.button(  # type: ignore[attr-defined]
                "Check again", icon="refresh", on_click=on_check_again,
            ).props("no-caps color=primary")
            if check_running:
                button.props("disable")
            if check_button is not None:
                check_button["button"] = button


def _render_probe_gate(
    ui: object,
    gate: object,
    *,
    notices: "tuple[Callable, Callable, Callable, Callable]",
    on_waive: Callable[[], None],
    on_check_again: Callable[[], None],
    check_running: bool,
    check_button: "Optional[dict]" = None,
    needs_baseline: bool = False,
) -> None:
    """Render a held gate the tool re-reads the source for -- REPLICA IDENTITY (re-keyed
    PostgreSQL tables) or the source schema (G4): why, what to do, the actions. ``notices``
    are the gate module's ``(open, pending, must re-check, waivable)`` notice functions
    (value-free copy).

    ``error`` while a table is still open (with "Check again", disabled while a read runs or
    without the source password -- and, with ``needs_baseline``, without the baseline a read
    is compared with: the button could only do nothing, the notice says when the check
    resumes), ``info`` while the fresh read Cut over waits for is in flight, ``warning`` for
    a table that needs a CHECKSUM after the fix, and ``warning`` with "Continue without this
    check" only for tables Validation cannot prove and only when nothing else is open.
    """
    open_notice, pending_notice, must_notice, waivable_notice = notices
    opened = open_notice(gate)
    if opened is not None:
        render_notice(ui, tone="error", header=opened[0], body=opened[1])
    pending = pending_notice(gate)
    if pending is not None:
        render_notice(ui, tone="info", header=pending[0], body=pending[1])
    must = must_notice(gate)
    if must is not None:
        render_notice(ui, tone="warning", header=must[0], body=must[1])
    waivable = waivable_notice(gate)
    if waivable is not None:
        render_notice(ui, tone="warning", header=waivable[0], body=waivable[1])
    can_waive = bool(getattr(gate, "can_waive", False))
    show_check = opened is not None
    if not (show_check or can_waive):
        return
    with ui.row().classes("w-full justify-end gap-2"):  # type: ignore[attr-defined]
        if can_waive:
            ui.button(  # type: ignore[attr-defined]
                "Continue without this check", icon="rule", on_click=on_waive,
            ).props("no-caps color=warning outline")
        if show_check:
            button = ui.button(  # type: ignore[attr-defined]
                "Check again", icon="refresh", on_click=on_check_again,
            ).props("no-caps color=primary")
            if (
                check_running
                or not bool(getattr(gate, "has_source_connection", False))
                or (
                    needs_baseline
                    and getattr(gate, "probe_state", None) == G4_PROBE_NO_BASELINE
                )
            ):
                button.props("disable")
            if check_button is not None:
                check_button["button"] = button


def _render_replica_identity_gate(
    ui: object,
    gate: object,
    *,
    on_waive: Callable[[], None],
    on_check_again: Callable[[], None],
    check_running: bool,
    check_button: "Optional[dict]" = None,
) -> None:
    """Render the held REPLICA IDENTITY gate (re-keyed PostgreSQL tables): copy from
    :mod:`dsql_migrator.ui.cdc_replica_identity_gate`, rendered by :func:`_render_probe_gate`
    (``error`` while a table is still below FULL)."""
    _render_probe_gate(
        ui, gate,
        notices=(ri_weak_notice, ri_pending_notice, ri_must_recheck_notice,
                 ri_waivable_notice),
        on_waive=on_waive, on_check_again=on_check_again, check_running=check_running,
        check_button=check_button,
    )


def _render_catalog_drift_gate(
    ui: object,
    gate: object,
    *,
    on_waive: Callable[[], None],
    on_check_again: Callable[[], None],
    check_running: bool,
    check_button: "Optional[dict]" = None,
) -> None:
    """Render the held source-schema gate: copy from
    :mod:`dsql_migrator.ui.cdc_catalog_drift_gate`, rendered by :func:`_render_probe_gate`
    (``error`` while a table's source schema still differs from the Step 1 inventory; "Check
    again" also disabled without a baseline)."""
    _render_probe_gate(
        ui, gate,
        notices=(g4_open_notice, g4_pending_notice, g4_must_recheck_notice,
                 g4_waivable_notice),
        on_waive=on_waive, on_check_again=on_check_again, check_running=check_running,
        check_button=check_button, needs_baseline=True,
    )


def _render_cutover_section(
    ui: object, summary: ValidationSummary, drift: DriftDisplay, *,
    cdc_in_use: bool = False,
    source_engine: str = "MySQL",
    identity_sync_provider: "Optional[Callable[[], None]]" = None,
    identity_sync_result: "Optional[dict[str, int]]" = None,
    identity_sync_failed: "Optional[dict[str, str]]" = None,
    identity_sync_missed: "Sequence[str]" = (),
    standalone_sequences: "Sequence[str]" = (),
    fk_apply_provider: "Optional[Callable[[], None]]" = None,
    fk_apply_result: "Optional[tuple[int, int, int]]" = None,
    fk_pending_count: int = 0,
    fk_apply_running: bool = False,
    fk_apply_progress: "Optional[tuple[int, int]]" = None,
    fk_validation: object = None,
    on_validate_again: "Optional[Callable[[Sequence[tuple[str, str]]], None]]" = None,
    fk_revalidating: bool = False,
    fk_holding: "Collection[tuple[str, str]]" = (),
    identity_pending_count: int = 0,
    cdc_drained_confirmed: bool = False,
    on_cdc_drained_change: "Optional[Callable[[bool], None]]" = None,
    inputs_unknown: bool = False,
    default_restore: "Optional[CutoverDefaultsSnapshot]" = None,
    defaults_checking: bool = False,
    restore_provider: "Optional[Callable[[Optional[tuple[str, str]]], None]]" = None,
    restore_job_block: Optional[str] = None,
    restore_running: bool = False,
    restore_outcome: "Optional[CutoverRestoreOutcome]" = None,
    on_recheck_defaults: "Optional[Callable[[], None]]" = None,
    drained_gated: "Optional[list]" = None,
) -> None:
    """Render the go-path cut-over runbook as its OWN titled section card.

    The verdict says the target is consistent; this tells the user how to actually
    finish — repoint the application to DSQL — which is the one step the tool does
    not do for them. Mirrors :func:`_render_recovery_section` (same `_section`
    container, info notice, numbered steps, closing caveat) so go and no-go feel
    symmetric. The steps branch on ``cdc_in_use``: a CDC path drains the stream to
    zero lag and tears the cdc-stack down afterwards; a Full-Load-only path just
    needs a brief source write-freeze. Rollback is called out either way, because
    once the application writes to DSQL those rows live only on DSQL (this tool
    replicates MySQL -> DSQL, not the reverse).

    ``identity_sync_provider`` (when given) wires an explicit "Sync identity
    sequences" button, shown just before the repoint step: it advances identity keys
    past the current target MAX(pk) so the app's first insert after cut-over cannot
    collide. It is a deliberate operator action (a target write), never a render
    side-effect. ``identity_sync_result`` is the last outcome to display ({} == ran,
    nothing to advance; a dict names the advanced sequences; None == not run yet). The
    sync block is shown only when ``identity_pending_count > 0`` (>=1 table converted to a
    server-generated IDENTITY key), symmetric with the FK-apply block's ``fk_pending_count``
    gate — a schema with no identity key has nothing to sync. ``identity_sync_missed``
    names the migrated identity tables the last sync was not given: an error, never "done".

    ``standalone_sequences`` (Evaluation's ``PG_STANDALONE_SEQUENCE`` items) adds "Before
    you repoint: move standalone sequences past the source": no sync reaches a sequence no
    column owns, so each is named with the source read and the Aurora DSQL statement.

    The FK-apply block is shown whenever ``fk_pending_count > 0`` (regardless of CDC): a
    CDC migration applies the deferred FKs here, and a Full-Load-only migration re-offers
    the idempotent apply to complete any FK not in place (FIX 9). For a CDC migration the
    Apply button is gated behind an explicit "CDC has drained" confirmation checkbox
    (FIX 2) — applying mid-stream would dead-letter 23503 violations. G142: the block says
    what happens before (each key added at once, then validated by Aurora DSQL in the
    background), during (``fk_apply_running``: "Adding foreign keys: X of N" from
    ``fk_apply_progress``, the button disabled) and after: the keys in place, then
    ``fk_validation`` (``_fk_validation.render_fk_validation``: N of M validated, failed
    keys with Aurora DSQL's details, the orphan count, the listing query and
    ``on_validate_again``; ``fk_holding`` names the keys Finish waits for).

    That checkbox is ONE for the whole section: it renders under CDC whenever the FK
    block renders or DEFAULT restores are pending, and its toggle enables every
    drained-gated button registered in ``drained_gated`` (``(button, other_blocked)``:
    Apply foreign keys, [Restore DEFAULTs], each per-row [Restore], and the finish-area
    [Restore DEFAULTs]) -- never one disabled for another reason -- without a refresh.
    ``default_restore`` is the background-read DEFAULT status (``defaults_checking``
    while it loads); see :func:`_render_cutover_defaults_block`.
    """
    with _section(ui, icon="rocket_launch", title="How to cut over"):  # type: ignore[misc]
        render_notice(
            ui,
            tone="success",
            header="Validation passed — you're ready to switch your app to DSQL",
            body=(
                f"Cut-over is the moment your application stops writing to {source_engine} "
                "and starts using Aurora DSQL. The tool has proven the target is "
                "consistent; repointing the application is the final operational "
                "step, and only you can do it. Follow the runbook below for a safe "
                "switch with a rollback path."
            ),
        )
        if cdc_in_use:
            steps = (
                ("1", "Let CDC catch up: on the Data Migration step, watch the CDC "
                 "status until replication lag is at (or near) zero — DSQL is "
                 "tracking the source's live writes."),
                ("2", "Freeze writes on the source: briefly put your application in "
                 f"read-only / maintenance mode so {source_engine} stops taking new "
                 "writes."),
                ("3", "Wait for the final drain: let CDC apply the last in-flight "
                 "change events until lag is zero again, and at least 5 minutes after "
                 "the last source write — a change waiting on a unique index is held "
                 "that long before it is applied or dead-lettered, and its lag can "
                 f"take a minute or two to show. {source_engine} and DSQL now hold the "
                 "same rows."),
                ("4", "Re-run validation here for the final go/no-go — do this AFTER "
                 "the drain (step 3). Cut over only on a clean MATCH (or differences "
                 "you can fully explain). This final run also advances the sequence "
                 "of each identity key (only sequences a column owns) past the rows "
                 "CDC delivered, so the app's first insert after cut-over cannot "
                 "collide with a migrated id. Tables with dead-lettered CDC changes "
                 "are compared by CHECKSUM automatically, and Cut over re-reads the "
                 "CDC dead-letter queue before this runbook opens; a dead letter found "
                 "by a later read (each time you reopen this step, at most once a "
                 "minute) locks it again until that table is re-checked."),
                ("5", "Repoint your application to the Aurora DSQL endpoint "
                 "(PostgreSQL wire, IAM-token auth — no password) and smoke-test "
                 "the critical read/write paths."),
                ("6", "Once you're confident, tear the CDC pipeline down: click "
                 "Start over (top right) and choose \"Delete all CDC "
                 "infrastructure\". Do this LAST — it ends replication. It stops "
                 "MSK / MSK Connect / NAT cost AND clears the old stack, which a "
                 "future fresh Full Load or CDC needs removed before it can deploy. "
                 "(If that option isn't shown, CDC is already torn down — nothing "
                 "left to remove.)"),
            )
        else:
            steps = (
                ("1", "Freeze writes on the source: put your application in "
                 "read-only / maintenance mode so no new rows are written to "
                 f"{source_engine} while you switch."),
                # G25 (rule E): a fresh copy is a Drop & reload re-run. The default Append
                # applies no update or delete, and a row-count MATCH cannot see a stale
                # value; Retry unfinished tables only fills unfinished work.
                ("2", "If the source took writes since the snapshot, Re-run Full Load "
                 "choosing 'Drop & reload' (the default Append only adds missing rows "
                 "and applies no update or delete made since), then re-run validation "
                 "here in CHECKSUM mode."),
                ("3", "Confirm a clean MATCH on this screen — that's the go/no-go "
                 "gate."),
                ("4", "Repoint your application to the Aurora DSQL endpoint "
                 "(PostgreSQL wire, IAM-token auth — no password) and smoke-test "
                 "the critical read/write paths."),
                ("5", "Lift the freeze — your application is now live on DSQL."),
            )
        steps_heading = (
            "Steps to cut over (with CDC)"
            if cdc_in_use
            else "Steps to cut over (Full Load)"
        )
        ui.label(steps_heading).classes(  # type: ignore[attr-defined]
            "text-base font-semibold text-gray-900 mt-1"
        )
        with ui.column().classes("w-full gap-2"):  # type: ignore[attr-defined]
            for num, text in steps:
                with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                    ui.label(num).classes(  # type: ignore[attr-defined]
                        "shrink-0 w-6 h-6 rounded-full bg-green-100 text-green-700 "
                        "text-xs font-semibold flex items-center justify-center"
                    )
                    ui.label(text).classes("text-sm text-gray-700 leading-snug")  # type: ignore[attr-defined]

        # Neither action below can be offered without the Step 1/2 schema inputs, and both
        # gates read 0 when those are missing -- which renders as a runbook with nothing
        # outstanding. Say so instead: an empty runbook must not be mistaken for a complete
        # one on the screen that decides whether integrity is enforced.
        if inputs_unknown:
            render_notice(
                ui,
                tone="warning",
                header="Can't confirm foreign keys / identity sequences",
                body=(
                    "The Step 1 evaluation and Step 2 conversion choices this runbook "
                    "reads are not loaded in this session, so the tool cannot tell you "
                    "whether foreign keys still need applying or identity sequences still "
                    "need advancing — the two actions that normally appear here. Reload "
                    "the page to resume the session, or re-run Evaluation and Schema "
                    "Conversion, before you repoint. Treat this as \"unknown\", not "
                    "\"nothing to do\"."
                ),
            )

        # ONE drained attestation for every action below that must wait for CDC to drain
        # (the FK ADD CONSTRAINT pass, the DEFAULT restores). Latched on ValidationState;
        # each gated button registers with its OWN other-block flag, so a tick never
        # enables a button that is disabled for another reason.
        _fk_block = fk_apply_provider is not None and fk_pending_count > 0
        _restores_pending = (
            default_restore is not None
            and default_restore.state == "loaded"
            and default_restore.pending > 0
        )
        _drained = {"ok": (not cdc_in_use) or bool(cdc_drained_confirmed)}
        _gated: list = drained_gated if drained_gated is not None else []

        def _apply_drained_gate() -> None:
            for _btn, _other_blocked in list(_gated):
                if _drained["ok"] and not _other_blocked:
                    _btn.props(remove="disable")
                else:
                    _btn.props("disable")

        if cdc_in_use and (_fk_block or _restores_pending):
            def _drained_toggle(event: object) -> None:
                _drained["ok"] = bool(getattr(event, "value", False))
                # Latch it so the next render starts from the operator's answer.
                # Deliberately no refresh() here: the gated buttons are flipped directly,
                # and refreshing would rebuild the whole section on every tick.
                if on_cdc_drained_change is not None:
                    on_cdc_drained_change(_drained["ok"])
                _apply_drained_gate()

            ui.checkbox(  # type: ignore[attr-defined]
                "I have frozen source writes, both CDC connectors are RUNNING, and CDC "
                "has drained to zero lag",
                value=bool(cdc_drained_confirmed),
                on_change=_drained_toggle,
            ).props("dense")

        # Explicit foreign-key apply. For a CDC migration the FK ADD CONSTRAINTs were
        # DEFERRED during the stream (they must not exist while the sink applies rows out
        # of parent-before-child order — an FK violation would be dead-lettered), so this
        # is the ONLY path that creates them; apply here, AFTER the final drain and BEFORE
        # repointing. For a Full-Load-only migration they are the explicit action on the
        # Data Migration step, and any key not in place is NOT enforced, so the same
        # (idempotent) action is re-offered to complete them and surface the result
        # (FIX 9). G142: each key is added NOT VALID at once (seconds), and Aurora DSQL
        # validates the rows already loaded in the background -- its verdict is read back
        # from the target and shown under the button. Shown whenever the schema has foreign
        # keys to (re)apply.
        if _fk_block:
            with ui.column().classes(  # type: ignore[attr-defined]
                "w-full gap-1 mt-1 pt-2 border-t border-gray-100"
            ):
                _fk_noun = "foreign key" if fk_pending_count == 1 else "foreign keys"
                if cdc_in_use:
                    ui.label(  # type: ignore[attr-defined]
                        "Before you repoint: apply foreign keys"
                    ).classes("text-sm font-semibold text-gray-900")
                    ui.label(  # type: ignore[attr-defined]
                        f"Aurora DSQL enforces foreign keys, but this schema's "
                        f"{fk_pending_count} {_fk_noun} were not created during CDC (they "
                        "must not exist while the stream applies rows out of "
                        "parent-before-child order). Apply them now — after the final "
                        "drain and before repointing. Each key is added at once, NOT "
                        "VALID, so this takes seconds: from then on Aurora DSQL refuses "
                        "every new write that would break it. Aurora DSQL then validates "
                        "the rows already loaded, in the background. Safe to run more than "
                        "once."
                    ).classes("text-xs text-gray-600 leading-snug")
                else:
                    ui.label(  # type: ignore[attr-defined]
                        "Verify foreign keys are enforced"
                    ).classes("text-sm font-semibold text-gray-900")
                    ui.label(  # type: ignore[attr-defined]
                        # Do NOT assert they were applied. They have not been applied at
                        # the end of the load since v0.1.461 -- they are an EXPLICIT action
                        # ("Apply foreign keys" on Data Migration, or here), so an operator
                        # who never clicked it, or who clicked "Stop applying", was told by
                        # this screen that referential integrity was already in place.
                        f"Aurora DSQL enforces foreign keys, and this schema has "
                        f"{fk_pending_count} preserved {_fk_noun}. They are applied by the "
                        "explicit \"Apply foreign keys\" action — on the Data Migration "
                        "step, or here — not automatically at the end of the load. If that "
                        "action was never run, was stopped part-way, or some keys could not "
                        "be added, referential integrity is NOT fully enforced. Apply here "
                        "to complete them and see the result: keys already in place are "
                        "left as they are, and each missing one is added at once (safe to "
                        "run more than once)."
                    ).classes("text-xs text-gray-600 leading-snug")

                # The ADD CONSTRAINT pass must run ONLY after CDC has drained: applying it
                # while the sink still streams out-of-order rows dead-letters 23503
                # violations. This screen does not poll MSK, so the CDC apply is gated on
                # the shared attestation above (FIX 2) -- latched on ValidationState, so a
                # refresh (the apply's own completion refresh included) keeps the answer.
                # A Full-Load-only re-apply has no live stream, so it is never gated.
                def _fk_apply_clicked() -> None:
                    if cdc_in_use and not _drained["ok"]:
                        return  # gated: CDC has not been confirmed drained
                    if fk_apply_running:
                        return  # the pass already runs (its job guard refuses a second)
                    fk_apply_provider()

                if fk_apply_running:
                    _done, _total = (tuple(fk_apply_progress or ()) + (0, 0))[:2]
                    render_notice(
                        ui,
                        tone="info",
                        busy=True,
                        header=(
                            f"Adding foreign keys: {_done} of {_total}"
                            if _total
                            else "Adding foreign keys"
                        ),
                        body=(
                            "Each key is added at once and enforces every new write from "
                            "then on; only a key whose parent table or referenced column "
                            "is outside this migration reads its child table first."
                        ),
                    )
                with ui.row().classes("items-center gap-3 mt-1"):  # type: ignore[attr-defined]
                    _fk_apply_btn = ui.button(  # type: ignore[attr-defined]
                        "Apply foreign keys",
                        icon="link",
                        on_click=_fk_apply_clicked,
                    ).props("color=primary outline no-caps")
                    if fk_apply_running:
                        _fk_apply_btn.props("disable")
                    if cdc_in_use:
                        # Enabled only once the operator confirms CDC has drained -- but a
                        # latched confirmation must not be re-disabled on re-render, or the
                        # tick would show ticked next to a dead button. Registered either
                        # way so a LATER un-tick still disables it.
                        if not _drained["ok"]:
                            _fk_apply_btn.props("disable")
                        _gated.append((_fk_apply_btn, bool(fk_apply_running)))
                    if fk_apply_result is not None and not fk_apply_running:
                        _applied, _skipped, _failed = fk_apply_result
                        if _failed == 0 and _skipped == 0:
                            inline_hint(
                                ui,
                                f"Applied {_applied} foreign key(s): in place, enforcing "
                                "every new write.",
                                tone="success",
                            )
                        else:
                            inline_hint(
                                ui,
                                f"Applied {_applied}, skipped {_skipped} (orphan rows), "
                                f"failed {_failed}.",
                                tone="warning",
                            )
                # Skipped / failed FKs must not read as done: surface a warning so the
                # operator resolves them before repointing onto a schema whose referential
                # integrity is not fully enforced. Since G142 a key is skipped only when its
                # parent side is incomplete on Aurora DSQL by design and its count found a
                # child row naming a missing parent.
                if fk_apply_result is not None and (
                    fk_apply_result[1] or fk_apply_result[2]
                ):
                    _a, _s, _f = fk_apply_result
                    render_notice(
                        ui,
                        tone="warning",
                        header="Some foreign keys were not applied",
                        body=(
                            f"{_s} skipped (child rows reference a parent row that is not "
                            "on Aurora DSQL, and the key's parent table or referenced "
                            f"column is outside this migration) and {_f} failed. The "
                            "activity log names each table / foreign key and why. Resolve "
                            "it, then click Apply foreign keys again — it is idempotent. "
                            "Referential integrity is not fully enforced on the target "
                            "until every foreign key is applied."
                        ),
                    )
                if not fk_apply_running and fk_validation is not None:
                    from dsql_migrator.ui.data_migration._fk_validation import (
                        render_fk_validation,
                    )

                    render_fk_validation(
                        ui, fk_validation,
                        on_validate_again=on_validate_again,
                        revalidating=fk_revalidating,
                        holding=fk_holding,
                    )

        if default_restore is not None or defaults_checking:
            _render_cutover_defaults_block(
                ui, default_restore, checking=defaults_checking, cdc_in_use=cdc_in_use,
                drained=_drained, gated=_gated, restore_provider=restore_provider,
                restore_job_block=restore_job_block, restore_running=restore_running,
                restore_outcome=restore_outcome, on_recheck=on_recheck_defaults,
            )

        # Explicit identity-sequence sync: an OPERATOR ACTION, not a render side-effect.
        # Identity (AUTO_INCREMENT) keys are loaded/replicated with explicit ids, which do
        # not advance a GENERATED BY DEFAULT sequence, so before repointing the app the
        # sequence must be moved past the current target MAX(pk) or the app's first insert
        # collides (23505). The button does that on demand (idempotent, safe to re-click);
        # do it after the final drain (CDC) / reload (Full Load) and before repointing.
        if identity_sync_provider is not None and identity_pending_count > 0:
            with ui.column().classes(  # type: ignore[attr-defined]
                "w-full gap-1 mt-1 pt-2 border-t border-gray-100"
            ):
                _id_noun = "table" if identity_pending_count == 1 else "tables"
                ui.label(  # type: ignore[attr-defined]
                    "Before you repoint: sync identity sequences"
                ).classes("text-sm font-semibold text-gray-900")
                ui.label(  # type: ignore[attr-defined]
                    f"{identity_pending_count} {_id_noun} use a server-generated "
                    "(AUTO_INCREMENT / IDENTITY) key; advance each sequence past the "
                    "migrated rows so your application's first insert after cut-over does "
                    "not hit a duplicate key. Run this after the final drain/reload and "
                    "before repointing. Safe to run more than once."
                ).classes("text-xs text-gray-600 leading-snug")
                with ui.row().classes("items-center gap-3 mt-1"):  # type: ignore[attr-defined]
                    ui.button(  # type: ignore[attr-defined]
                        "Sync identity sequences",
                        icon="sync",
                        on_click=lambda: identity_sync_provider(),
                    ).props("color=primary outline no-caps")
                    if identity_sync_result is not None:
                        if identity_sync_result:
                            advanced = ", ".join(
                                f"{name} → {value}"
                                for name, value in sorted(identity_sync_result.items())
                            )
                            inline_hint(
                                ui,
                                f"Advanced {len(identity_sync_result)} sequence(s): "
                                f"{advanced}.",
                                tone="success",
                            )
                        elif not identity_sync_failed and not identity_sync_missed:
                            # Genuinely nothing to advance (no server-generated keys, or
                            # all already correct) AND nothing failed or left out -> the
                            # reassuring success line.
                            inline_hint(
                                ui,
                                "Done — no server-generated key needed advancing.",
                                tone="success",
                            )
                # A FAILED RESTART WITH must never read as done: surface it as an ERROR
                # so the operator does not repoint the app onto lagging sequences (the
                # app's first insert would collide, 23505). Audit finding D2 — this used
                # to be swallowed and painted as the success line above.
                real_failed, not_identity = _split_not_identity(identity_sync_failed or {})
                _render_not_identity_notice(ui, not_identity)
                if real_failed:
                    failed_detail = "; ".join(
                        f"{name} ({reason})"
                        for name, reason in sorted(real_failed.items())
                    )
                    render_notice(
                        ui,
                        tone="error",
                        header="Identity sequence sync failed — do not cut over yet",
                        body=(
                            f"{len(real_failed)} identity sequence(s) could "
                            "NOT be advanced past the migrated rows, so your "
                            "application's first insert after cut-over may hit a "
                            "duplicate key. Retry the sync (it is idempotent); if it "
                            "keeps failing, advance the sequence(s) manually with "
                            "ALTER TABLE … ALTER COLUMN … RESTART WITH before "
                            f"repointing. Affected: {failed_detail}."
                        ),
                    )
                # A migrated identity table the last sync was not given (the schema it is
                # scoped from changed since, or could not be read in the job) is no "done"
                # either: its sequence may still be behind the rows CDC delivered.
                if identity_sync_missed:
                    render_notice(
                        ui,
                        tone="error",
                        header="Identity sequences not synced — do not cut over yet",
                        body=(
                            f"The last sync did not reach {len(identity_sync_missed)} "
                            "table(s) this migration gives an identity key "
                            f"({bounded_table_names(list(identity_sync_missed))}), so "
                            "their sequences may still be behind the migrated rows and "
                            "your application's first insert after cut-over may hit a "
                            "duplicate key. Run Sync identity sequences again: it covers "
                            "every migrated identity table and never moves a sequence "
                            "backwards."
                        ),
                    )

        if standalone_sequences:
            _render_standalone_sequences_block(
                ui, standalone_sequences, source_engine=source_engine
            )

        # Rollback caveat: once the app writes to DSQL, those rows exist only there
        # (this tool does not replicate DSQL -> MySQL), so keep the source as a
        # read-only rollback anchor until sign-off.
        render_notice(
            ui,
            tone="info",
            header="Keep the source as your rollback anchor",
            body=(
                f"Until you've signed off on DSQL, keep the {source_engine} source "
                "frozen (read-only) rather than dropping it. Before you repoint, "
                "rollback is trivial — the source is untouched and still authoritative. "
                "After the application writes to DSQL, those new rows live only on "
                f"DSQL (this tool replicates {source_engine} -> DSQL, not the reverse), "
                "so rolling back then means reconciling them yourself first."
            ),
        )


def _render_standalone_sequences_block(
    ui: object, sequences: "Sequence[str]", *, source_engine: str
) -> None:
    """The "Before you repoint: move standalone sequences past the source" block (no I/O).

    Evaluation lists each source sequence no column owns (``PG_STANDALONE_SEQUENCE``) and
    leaves moving it at cut over to the operator: no sync reaches it, so the application's
    nextval() after the repoint would hand out values the source already used (23505 on a
    key, or a number issued twice). Each one gets the source read and the Aurora DSQL
    statement: ``setval`` with both values read sets exactly the source's next value.
    """
    from dsql_migrator.core.source_dialect import dialect_for
    from dsql_migrator.ui.design import CODE_SURFACE_CLASSES

    pg = dialect_for(SourceType.POSTGRES)
    one = len(sequences) == 1
    with ui.column().classes(  # type: ignore[attr-defined]
        "w-full gap-1 mt-1 pt-2 border-t border-gray-100"
    ):
        ui.label(  # type: ignore[attr-defined]
            "Before you repoint: move standalone sequences past the source"
        ).classes("text-sm font-semibold text-gray-900")
        ui.label(  # type: ignore[attr-defined]
            f"{len(sequences)} {source_engine} "
            f"{'sequence is' if one else 'sequences are'} owned by no column, so no sync "
            f"here moves {'it' if one else 'them'}: after cut over, the application's "
            "nextval() would hand out values the source already used. After the final "
            "drain/reload and before repointing, run the first line of each on "
            f"{source_engine} and the second on Aurora DSQL with the two values it returns "
            "(is_called as true or false). Create "
            f"{'it' if one else 'them'} first if you have not: each one's Evaluation item "
            "(PG_STANDALONE_SEQUENCE) gives the CREATE SEQUENCE."
        ).classes("text-xs text-gray-600 leading-snug")
        with ui.column().classes(  # type: ignore[attr-defined]
            f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"
        ):
            for name in sequences:
                quoted = pg.quote_table(name)
                literal = "'" + quoted.replace("'", "''") + "'"
                for statement in (
                    f"SELECT last_value, is_called FROM {quoted};",
                    f"SELECT setval({literal}, <last_value>, <is_called>);",
                ):
                    ui.label(statement).classes(  # type: ignore[attr-defined]
                        "text-xs font-mono wrap-anywhere text-gray-800"
                    )


def _restore_hint(
    *, cdc_in_use: bool, restore_job_block: Optional[str], restore_running: bool,
    has_auto: bool,
) -> str:
    """What disables (or, under CDC, enables) [Restore DEFAULTs], in one line; empty when
    nothing does. A load or Start CDC starting later is refused at click time."""
    if restore_running:
        return "Restoring the DEFAULTs…"
    if restore_job_block:
        return f"Restore is {restore_job_block}."
    if not has_auto:
        return "Nothing to restore automatically; decide on the columns below."
    if cdc_in_use:
        # The drained tick enables the button client-side without a re-render, so this is
        # phrased as the rule, true before and after the tick.
        return "Enabled once you confirm above that CDC has drained."
    return ""


def _render_cutover_defaults_block(
    ui: object,
    snap: "Optional[CutoverDefaultsSnapshot]",
    *,
    checking: bool,
    cdc_in_use: bool,
    drained: "dict[str, bool]",
    gated: list,
    restore_provider: "Optional[Callable[[Optional[tuple[str, str]]], None]]",
    restore_job_block: Optional[str],
    restore_running: bool,
    restore_outcome: "Optional[CutoverRestoreOutcome]",
    on_recheck: "Optional[Callable[[], None]]",
) -> None:
    """The excluded columns' DEFAULTs to put back before repointing (no I/O here).

    Modelled on "Before you repoint: sync identity sequences": the check row, the exact
    statements, [Restore DEFAULTs], the columns that need a decision (each with its
    reason, statement and its own [Restore]), what is left as it is, and which columns
    stay nullable. A DEFAULT affects new inserts only, so migrated rows stay NULL.
    """
    from dsql_migrator.ui.design import CODE_SURFACE_CLASSES

    def _recheck_button() -> None:
        if on_recheck is not None:
            ui.button(  # type: ignore[attr-defined]
                "Check again", icon="refresh", on_click=lambda: on_recheck(),
            ).props("flat dense no-caps color=primary")

    with ui.column().classes(  # type: ignore[attr-defined]
        "w-full gap-1 mt-1 pt-2 border-t border-gray-100"
    ):
        if snap is None:
            if checking:
                with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                    ui.spinner(size="sm")  # type: ignore[attr-defined]
                    ui.label(  # type: ignore[attr-defined]
                        "Excluded-column DEFAULTs: checking Aurora DSQL"
                    ).classes("text-sm text-gray-700")
                    _recheck_button()
            return
        if snap.state == "ledger_unreadable":
            _render_check_row(
                ui, passed=False, warn_on_fail=True,
                label="Excluded-column DEFAULTs",
                detail=(
                    "The record of dropped DEFAULTs could not be read, so the tool cannot "
                    "say which excluded columns still need their DEFAULT back."
                ),
            )
            _recheck_button()
            return
        if snap.state == "target_unreadable":
            _render_check_row(
                ui, passed=False, warn_on_fail=True,
                label="Excluded-column DEFAULTs",
                detail="Could not read Aurora DSQL's catalog to check them.",
            )
            _recheck_button()
            return
        if not snap.items and not snap.nullable:
            return
        stamp = snap.checked_at.strftime("%H:%M UTC") if snap.checked_at else "just now"
        if snap.items:
            _render_check_row(
                ui, passed=snap.outstanding == 0, warn_on_fail=True,
                label=f"Excluded-column DEFAULTs restored: {snap.restored} of {snap.total}",
                detail=f"checked {stamp}",
            )
        pending = snap.pending
        if pending:
            ui.label(  # type: ignore[attr-defined]
                "Before you repoint: restore DEFAULTs on "
                f"{pending} excluded column{'s' if pending != 1 else ''}"
            ).classes("text-sm font-semibold text-gray-900")
            ui.label(  # type: ignore[attr-defined]
                "The migration dropped these so excluded columns load as NULL. Restoring "
                "affects new inserts only; migrated rows stay NULL."
            ).classes("text-xs text-gray-600 leading-snug")
            if cdc_in_use:
                ui.label(  # type: ignore[attr-defined]
                    "Tick the confirmation above only once both connectors are RUNNING and "
                    "caught up. A DEFAULT restored before a connector finishes its snapshot "
                    "or replay gives those rows the DEFAULT."
                ).classes("text-xs text-gray-600 leading-snug")
        auto = snap.auto_items
        blocked_elsewhere = bool(restore_job_block) or restore_running
        if auto:
            with ui.column().classes(  # type: ignore[attr-defined]
                f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"
            ):
                for item in auto:
                    ui.label(item.statement).classes(  # type: ignore[attr-defined]
                        "text-xs font-mono wrap-anywhere text-gray-800"
                    )
        if pending:
            with ui.row().classes("items-center gap-3 mt-1"):  # type: ignore[attr-defined]
                _btn = ui.button(  # type: ignore[attr-defined]
                    "Restore DEFAULTs",
                    icon="settings_backup_restore",
                    on_click=lambda: restore_provider(None) if restore_provider else None,
                ).props("color=primary outline no-caps")
                other = blocked_elsewhere or not auto or restore_provider is None
                if other or not drained["ok"]:
                    _btn.props("disable")
                gated.append((_btn, other))
                _hint = _restore_hint(
                    cdc_in_use=cdc_in_use, restore_job_block=restore_job_block,
                    restore_running=restore_running, has_auto=bool(auto),
                )
                if _hint:
                    inline_hint(ui, _hint, tone="info")
        if restore_outcome is not None:
            if restore_outcome.refused:
                render_notice(
                    ui, tone="warning", header="DEFAULTs not restored",
                    body=restore_outcome.refused,
                )
            if restore_outcome.failed:
                render_notice(
                    ui,
                    tone="warning",
                    header=f"{len(restore_outcome.failed)} DEFAULT(s) could not be restored",
                    body=(
                        "Run these statements yourself, or try again: "
                        + " ".join(
                            f"{name} ({reason}): "
                            f"{restore_outcome.failed_statements.get(name, '')}"
                            for name, reason in sorted(restore_outcome.failed.items())
                        )
                    ),
                )
            if restore_outcome.skipped:
                render_notice(
                    ui,
                    tone="warning",
                    header=f"{len(restore_outcome.skipped)} DEFAULT(s) not restored",
                    body=(
                        "Their record changed since this screen was drawn (for example a "
                        "reload recorded them again). Check the list below and restore "
                        "again: " + ", ".join(restore_outcome.skipped) + "."
                    ),
                )
            if restore_outcome.ledger_warning:
                inline_hint(ui, restore_outcome.ledger_warning + ".", tone="warning")
        manual = snap.manual_items
        if manual:
            ui.label(  # type: ignore[attr-defined]
                f"Needs your decision ({len(manual)}):"
            ).classes("text-sm font-semibold text-gray-900 mt-1")
            for item in manual:
                with ui.column().classes("w-full gap-0 pl-2"):  # type: ignore[attr-defined]
                    ui.label(  # type: ignore[attr-defined]
                        f"{item.table}.{item.column}  DEFAULT {item.original_default}"
                    ).classes("text-xs font-mono text-gray-800 wrap-anywhere")
                    ui.label(item.reason or "").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-600 leading-snug"
                    )
                    ui.label(item.statement).classes(  # type: ignore[attr-defined]
                        "text-xs font-mono text-gray-500 wrap-anywhere"
                    )
                    if item.executable:
                        _row_btn = ui.button(  # type: ignore[attr-defined]
                            "Restore",
                            on_click=lambda _e=None, _k=(item.table, item.column): (
                                restore_provider(_k) if restore_provider else None
                            ),
                        ).props("outline dense no-caps size=sm color=primary")
                        _row_other = blocked_elsewhere or restore_provider is None
                        if _row_other or not drained["ok"]:
                            _row_btn.props("disable")
                        gated.append((_row_btn, _row_other))
                    else:
                        inline_hint(ui, "Run this statement yourself.", tone="info")
        unknown = [i for i in snap.items if i.status == "unknown"]
        if unknown:
            inline_hint(
                ui,
                "Could not check "
                + ", ".join(f"{i.table}.{i.column}" for i in unknown)
                + " on Aurora DSQL.",
                tone="warning",
            )
            _recheck_button()
        diverged = snap.diverged_items
        if diverged:
            inline_hint(
                ui,
                "Left as they are (a different DEFAULT is set on Aurora DSQL): "
                + ", ".join(
                    f"{i.table}.{i.column} ({i.live_default})" for i in diverged
                )
                + ".",
                tone="info",
            )
        if snap.nullable:
            inline_hint(
                ui,
                f"{len(snap.nullable)} excluded column(s) stay nullable (Aurora DSQL "
                "cannot add NOT NULL back in place): " + ", ".join(snap.nullable) + ".",
                tone="info",
            )


# The Validation step's per-session state and the helpers both steps share. The functions
# above read them only when called (annotations name a few), never at import. Imported at
# the END, after this module's own definitions, because validation imports this module at
# ITS end (to re-export the names above): either module can then be imported first.
from dsql_migrator.ui.validation import (  # noqa: E402
    DriftDisplay,
    ValidationState,
    ValidationStore,
    ValidationSummary,
    _bounded_names,
    _cdc_gates,
    _cdc_in_use,
    _cutover_identity_tables,
    _identity_sync_context,
    _identity_sync_extras,
    _migrated_tables,
    _render_check_row,
    _render_not_identity_notice,
    _section,
    _source_engine_word,
    _split_not_identity,
    _with_identity_tables,
    cutover_release_state,
    format_drift,
    full_load_covered_tables,
    resync_identity_sequences,
    session_exclusion_split,
    summarize_validation,
    validation_scope_gap,
    verdict_evidence_caveats,
)
