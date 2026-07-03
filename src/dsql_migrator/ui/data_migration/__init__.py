"""Step 3 (Data Migration) screen of the four-step migration workflow.

The Data Migration screen drives the export -> import pipeline the design maps to
this step (design.md "Data Migration Design"). From the source inventory produced
by Step 1 (Evaluation) and the configured source/target connections it:

1. starts the data migration job (Requirement 8.2),
2. shows migration progress with auto-refresh -- per-table progress and failure
   counts -- while the job runs (Requirement 8.3), and
3. displays the export watermark: the exact consistency point the data was
   exported as-of (binlog file:position and/or GTID set, plus the snapshot
   timestamp and per-table snapshot row counts) (Requirement 8.5 / Property 11).

Because the migration is long-running, the run executes on a background job via
:class:`~dsql_migrator.core.job_manager.JobManager` so the NiceGUI event loop is
never blocked (Requirement 9.3); the screen polls the job with a ``ui.timer`` and
updates the Data Migration step status in the per-session
:class:`~dsql_migrator.core.models.WorkflowState` (NOT_STARTED -> IN_PROGRESS ->
DONE/FAILED) through the workflow helpers.

Engine wiring. The actual export and import are the implemented Task 8 components:
the read-only :class:`~dsql_migrator.core.watermark.WatermarkCapturer`, the
:class:`~dsql_migrator.core.exporter.TableExporter`, and the in-process
:class:`~dsql_migrator.core.batched_import.BatchedImporter` (batched
``INSERT ... ON CONFLICT`` with OCC retry, over the same boto3 IAM connection the
tool uses elsewhere -- no external binary). They are reached through a small,
injectable :class:`DataMigrator` seam so the run orchestration and the UI can be
unit tested with fakes (no real MySQL / DSQL); the reference
:class:`BatchedTableMigrator` wires the real components and is the default. The
watermark captured at export start is persisted on the job record's
:attr:`~dsql_migrator.core.models.MigrationJob.watermark` field (Requirement 5.7),
so the UI reads it straight from the job snapshot.

As with the sibling step screens, the run orchestration, progress aggregation,
and watermark formatting below are independent of NiceGUI so they can be unit
tested directly; only :func:`build_data_migration_screen` and its render helpers
touch NiceGUI.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.cdc import (
    CdcPipelineOrchestrator,
)
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import (
    JobHandle,
    JobManager,
    JobNotFoundError,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.target_introspector import tables_with_rows
from dsql_migrator.core.models import (
    ChunkState,
    ErrorLogSummary,
    LoadKind,
    LoadStatusView,
    MigrationJob,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteCheckRequest,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
    SourceInventory,
    StepStatus,
    TableSelection,
    TargetInventory,
    Watermark,
)
from dsql_migrator.core.table_selection import TableSelector
from dsql_migrator.core.watermark import WatermarkCapturer
from dsql_migrator.ui.design import NOTICE_STYLE, inline_hint, render_notice
from dsql_migrator.ui.evaluation import EvaluationStore
from dsql_migrator.ui.prerequisite_probes import build_prerequisite_checker
from dsql_migrator.ui.schema_conversion import (
    TABLE_PREFIX,
    SchemaConversionStore,
    applied_table_conversions,
    selected_object_names,
)
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

# Full Load backend run engine (NiceGUI-free); re-exported so the package's
# public import surface is unchanged.
from dsql_migrator.ui.data_migration._engine import (
    MigratorFactory,
    DataMigrationInputs,
    TableLoadResult,
    _as_load_result,
    DataMigrator,
    _seed_chunks,
    _start_chunk,
    _complete_chunk,
    _fail_chunk,
    _advance_chunk_rows,
    _find_chunk,
    _recompute_progress,
    _error_code,
    full_load_progress_caption,
    FULL_LOAD_TABLE_PARALLELISM,
    PROGRESS_FLUSH_ROWS,
    _FullLoadStopped,
    FullLoadIncompleteError,
    _fail_unfinished_chunks,
    _migrate_one_table,
    _migrate_tables_in_parallel,
    run_full_load,
    _seed_retry_chunks,
    run_full_load_retry,
    run_data_migration,
    job_status_to_step_status,
    data_migration_step_after_cdc,
    reconcile_full_load_step,
    ImporterFactory,
    _default_importer_factory,
    TableRecreator,
    _default_table_recreator,
    BatchedTableMigrator,
    default_migrator_factory,
)

# Pure view-models, formatters, and enums (NiceGUI-free); re-exported so the
# package's public import surface is unchanged.
from dsql_migrator.ui.data_migration._models import (
    MigrationType,
    _SUBSTEPS,
    prereq_mode_for_type,
    substeps_for_type,
    resolve_active_substep_for_type,
    resolve_active_substep,
    _MigrationTypeMeta,
    _MIGRATION_TYPE_META,
    MigrationProgress,
    summarize_progress,
    build_full_load_status_view,
    FullLoadTableRow,
    build_full_load_table_rows,
    failed_table_names,
    format_duration,
    format_table_timing,
    FullLoadCompleteness,
    full_load_completeness,
    MigrationTableStatus,
    build_migration_table_status,
    _LOAD_STATE_ORDER,
    summarize_table_states,
    prerequisite_block_reason,
    PrereqCategory,
    _PREREQ_CATEGORY_BY_CHECK,
    _PREREQ_CATEGORY_ORDER,
    PrereqCategoryGroup,
    _rollup_category_status,
    _category_summary,
    group_prereq_results,
    format_error_summary,
    WatermarkDisplay,
    format_binlog_coordinate,
    format_watermark,
    LobExclusionCandidate,
    lob_exclusion_candidates,
    format_column_exclude_list,
    _DSQL_VALUE_LIMIT_MIB,
    _BROKER_MESSAGE_LIMIT_MIB,
    DlqHealth,
    assess_dlq_health,
    ConnectorHealthRow,
    _HEALTHY_CONNECTOR_STATES,
    _BAD_CONNECTOR_STATES,
    connector_role_label,
    connector_health_rows,
    CdcHandlingFact,
    cdc_handling_facts,
)

# CDC status / controller / deploy-formatting logic (NiceGUI-free); re-exported so
# the package's public import surface is unchanged and the render code below
# resolves the pure helpers/constants it consumes.
from dsql_migrator.ui.data_migration._status import (
    _current_job,
    _read_cdc_template_body,
    _CDC_ACTION_NOUN,
    _CDC_ACTION_TERMINAL,
    _CDC_STAGE_LABELS,
    _CDC_ACTION_TITLE,
    _CDC_STAGE_ETA_SECONDS,
    _format_eta_hint,
    _CDC_DEPLOY_STAGE_STYLE,
    _deploy_total_duration,
    _LOG_GLYPH_FALLBACKS,
    _ascii_log,
    _migration_status_tables,
    _fetch_migration_row_counts,
    _cdc_status_view,
    _filter_mine,
    _is_inflight_stack_status,
    _classify_cdc_stack_phase,
    _probe_cdc_stack_phase,
    _ensure_cdc_controller,
    _CDC_DISCOVERY_THROTTLE_SECONDS,
    _CDC_IDLE_RATE_THRESHOLD,
    CdcActivitySummary,
    cdc_activity_summary,
    cdc_error_log_key,
    _fetch_cdc_status,
    _apply_cdc_status,
    _refresh_cdc_status,
    _CDC_TONE_STYLE,
)

# Text shown when a watermark field could not be captured (binary logging
# disabled, or SHOW MASTER STATUS restricted on RDS/Aurora). The export is still
# valid; only the optional coordinate is missing (Requirement 5.7).
_UNAVAILABLE = "unavailable"


_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-session data-migration state (NiceGUI-free); re-exported so the package's
# public import surface is unchanged.
# ---------------------------------------------------------------------------
from dsql_migrator.ui.data_migration._state import (
    DataMigrationState,
    DataMigrationStore,
)


# ---------------------------------------------------------------------------
# NiceGUI screen
# ---------------------------------------------------------------------------

# Quasar color names reused for the inline status badge.
_STATUS_COLORS: dict[StepStatus, str] = {
    StepStatus.NOT_STARTED: "grey",
    StepStatus.IN_PROGRESS: "primary",
    StepStatus.DONE: "positive",
    StepStatus.FAILED: "negative",
}

# Quasar color names for each per-table chunk status badge.
_CHUNK_STATUS_COLORS: dict[str, str] = {
    "PENDING": "grey",
    "IN_PROGRESS": "primary",
    "DONE": "positive",
    "FAILED": "negative",
}

# How often the screen polls the background migration job (seconds). Each tick
# rebuilds the live per-table progress table (one row per migrating table, with
# custom Quasar slots), so an overly-fast poll makes the browser laggy on a wide
# table set. 1.5s is still clearly "live" for a bulk load while cutting the
# per-tick DOM churn to a third of the old 0.5s.
_POLL_INTERVAL_SECONDS = 1.5



def build_data_migration_screen(
    store: SessionStore,
    session_id: str,
    *,
    job_manager: JobManager,
    eval_store: EvaluationStore,
    migration_store: DataMigrationStore,
    conv_store: SchemaConversionStore,
    migrator_factory: MigratorFactory = default_migrator_factory,
    staging_bucket: Optional[str] = None,
    cdc_deploy_role_arn: Optional[str] = None,
    cdc_secret_kms_key_id: Optional[str] = None,
    validation_store: Optional[object] = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Data Migration screen, returning ``(content_builder, runner)``.

    ``validation_store`` (optional ``ValidationStore``) lets the CDC step surface
    the Validation result as cutover information; ``None`` simply omits it.

    ``content_builder`` renders the screen (status, watermark, per-table progress)
    and is given the workflow shell's refresh callback so it can reflect
    background-job progress. ``runner`` is invoked by the step's Run/Re-run
    button: it validates the source/target connections and the Step 1 inventory,
    marks the step ``IN_PROGRESS``, and submits the migration to ``job_manager``
    (returning immediately so the UI never blocks). Both plug into
    :func:`~dsql_migrator.ui.workflow.build_workflow_sidebar`.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    migration_state = migration_store.get_or_create(session_id)
    # Bind the session so migration_type / cdc_infra_inputs read-through to it
    # (the Migration plan step chose the mode early and stored it on the session).
    migration_state.bind_session(session)
    # Thread the process-config CDC deploy-role ARN onto the per-session state so
    # the (module-level) deploy handlers can pass it to build_cdc_stack_deployer.
    migration_state.cdc_deploy_role_arn = cdc_deploy_role_arn
    # Optional CMK for the tool-managed source-credentials secret (default: the
    # account's aws/secretsmanager managed key when None).
    migration_state.cdc_secret_kms_key_id = cdc_secret_kms_key_id
    eval_state = eval_store.get_or_create(session_id)
    conv_state = conv_store.get_or_create(session_id)

    def _inventory() -> Optional[SourceInventory]:
        result = eval_state.result
        return result.inventory if result is not None else None

    def _target_inventory() -> Optional[TargetInventory]:
        # The target DSQL catalog from Step 1 (Evaluation). Used to treat tables
        # that already exist on the target as migratable, so Data Migration can
        # proceed when Schema Conversion was applied in a prior session.
        result = eval_state.result
        return result.target_inventory if result is not None else None

    def runner() -> None:
        inventory = _inventory()
        if inventory is None:
            migration_state.set_error(
                "Run Step 1 (Evaluation) first to introspect the source schema, "
                "then start the data migration."
            )
            return
        if not session.has_source():
            migration_state.set_error(
                "Configure and test the source connection first, then start the "
                "data migration."
            )
            return
        if not session.has_target():
            migration_state.set_error(
                "Configure and test the target connection first, then start the "
                "data migration."
            )
            return
        if not inventory.tables:
            migration_state.set_error(
                "The source inventory has no tables to migrate."
            )
            return

        source_config = session.source_config
        target_config = session.target_config
        assert source_config is not None  # guaranteed by has_source()
        assert target_config is not None  # guaranteed by has_target()
        inputs = DataMigrationInputs(
            source_config=source_config,
            source_password=session.source_password,
            target_config=target_config,
            inventory=inventory,
            aws_profile=session.aws_profile,
            staging_bucket=staging_bucket,
            replace_tables=migration_state.replace_targets,
            # Only suppress DROP+recreate when CDC is ACTIVELY STREAMING into the
            # target (a DROP would race the live sink). Before CDC starts -- even in
            # the Full load + CDC pattern -- a re-run still drops+recreates the
            # confirmed tables for a clean reload (no leftover rows). Once CDC is
            # live the load falls back to idempotent SKIP_EXISTING (no DROP).
            cdc_coexisting=cdc_streaming_started(migration_state, job_manager),
            table_conversions=applied_table_conversions(
                SchemaConverter().convert(inventory), conv_state.edited_target_ddls
            ),
        )
        # Full Load runs over the tables the user selected in the picker. A
        # table is migratable when it has a target table to load into: either its
        # DDL was generated in this session's Step 2, or it already exists on the
        # target DSQL (Schema Conversion applied earlier). The picker defaults to
        # all of them (Property 16).
        generated = migratable_table_names(
            inventory, conv_state.generated_node_ids, _target_inventory()
        )
        if not generated:
            migration_state.set_error(
                "No tables are ready to migrate. Generate schema DDL in Step 2 "
                "(Schema Conversion), or point at a target where the tables "
                "already exist; only tables with a target table can be migrated."
            )
            return
        names = effective_migration_selection(
            generated,
            migration_state.selection,
            touched=migration_state.selection_touched,
            default=target_existing_table_names(inventory, _target_inventory()),
        )
        if not names:
            migration_state.set_error("Select at least one table to migrate.")
            return
        tables = TableSelector().resolve(
            inventory, TableSelection(selected_tables=names)
        )
        migrator = migrator_factory(inputs)
        error_log = migration_state.error_log

        migration_state.clear_outputs()
        session.set_workflow(
            with_status(
                session.workflow, WorkflowStep.FULL_LOAD, StepStatus.IN_PROGRESS
            )
        )

        def work(handle: JobHandle) -> None:
            run_full_load(
                handle,
                tables,
                migrator=migrator,
                error_log=error_log,
                accept_quarantined_rows=migration_state.accept_quarantined_rows,
            )

        migration_state.job_id = job_manager.submit(work)

    def content(refresh: Callable[[], None]) -> None:
        migration_type = migration_state.migration_type
        prereq_mode = prereq_mode_for_type(migration_type)
        steps = substeps_for_type(migration_type)
        # The unified Data Migration step is backed by WorkflowStep.FULL_LOAD for
        # all migration types (its status gates Validation).
        status = get_status(session.workflow, WorkflowStep.FULL_LOAD)
        # Reconcile a stale IN_PROGRESS step against the real job status. The live
        # poll only advances the step while the job is RUNNING; if the job already
        # reached a terminal state without the poll running (e.g. an app restart
        # reconciled a hung/interrupted job to FAILED, or the watchdog reaped it),
        # the saved step would otherwise stay IN_PROGRESS forever -- the screen
        # would show "Full Load in progress…" with no terminal affordances. Sync
        # it once on render so the failure reason and "Retry failed tables" appear.
        _current = _current_job(job_manager, migration_state.job_id)
        reconciled = reconcile_full_load_step(
            status, _current.status if _current is not None else None
        )
        if reconciled is not None:
            if reconciled is StepStatus.FAILED:
                migration_state.set_error(
                    job_manager.get_error(migration_state.job_id)
                    or "Data migration failed."
                )
            session.set_workflow(  # type: ignore[attr-defined]
                with_status(session.workflow, WorkflowStep.FULL_LOAD, reconciled)
            )
            status = reconciled
        # Once CDC is actually streaming, the Data Migration step is effectively
        # complete for downstream gating even if no Full Load ran (a CDC-only plan,
        # or a reconnected session with no local watermark): data is flowing to the
        # target, so Validation must be reachable. Without this the step only ever
        # reaches DONE via a finished Full Load, leaving CDC-only runs stuck at
        # "Complete Data Migration first before opening Validation".
        promoted = data_migration_step_after_cdc(
            status, cdc_streaming=cdc_streaming_started(migration_state, job_manager)
        )
        if promoted is not None:
            session.set_workflow(  # type: ignore[attr-defined]
                with_status(session.workflow, WorkflowStep.FULL_LOAD, promoted)
            )
            status = promoted
        inventory = _inventory()

        async def run_checks(mode: MigrationMode) -> None:
            """Run read-only prerequisite checks for ``mode`` off the event loop."""
            if inventory is None or not inventory.tables:
                migration_state.set_error(
                    "Run Step 1 (Evaluation) first to introspect the source schema."
                )
                refresh()
                return
            if not session.has_source() or not session.has_target():
                migration_state.set_error(
                    "Configure and test the source and target connections first."
                )
                refresh()
                return
            generated = migratable_table_names(
                inventory, conv_state.generated_node_ids, _target_inventory()
            )
            if not generated:
                migration_state.set_error(
                    "No tables are ready to migrate. Generate schema DDL in Step 2 "
                    "(Schema Conversion), or point at a target where the tables "
                    "already exist; only tables with a target table can be migrated."
                )
                refresh()
                return
            names = effective_migration_selection(
                generated,
                migration_state.selection,
                touched=migration_state.selection_touched,
                default=target_existing_table_names(inventory, _target_inventory()),
            )
            if not names:
                migration_state.set_error("Select at least one table to check.")
                refresh()
                return
            tables = TableSelector().resolve(
                inventory, TableSelection(selected_tables=names)
            )
            source_config = session.source_config
            target_config = session.target_config
            assert source_config is not None and target_config is not None
            checker = build_prerequisite_checker(
                source_config=source_config,
                source_password=session.source_password,
                target_config=target_config,
                aws_profile=session.aws_profile,
            )
            request = PrerequisiteCheckRequest(
                mode=mode, tables=[table.name for table in tables]
            )
            from nicegui import run

            # Acknowledge the click immediately: clear any prior error, flag the
            # mode as running, and re-render so a spinner/"checking..." appears
            # right below the button before the (slow) read-only checks start.
            migration_state.clear_outputs()
            migration_state.set_prereq_running(mode)
            refresh()
            try:
                report = await run.io_bound(
                    lambda: checker.check(request, tables=tables)
                )
            except Exception as exc:  # noqa: BLE001 - surface as inline feedback
                migration_state.clear_prereq_running(mode)
                migration_state.set_error(
                    f"Prerequisite checks could not complete: {exc}"
                )
                refresh()
                return
            migration_state.clear_prereq_running(mode)
            migration_state.set_prereq_report(mode, report)
            # Record the prerequisite-check outcome on the activity log so the
            # timeline shows readiness was assessed and whether it passed. Both
            # modes run the same prerequisite checker, so the action label is
            # unified ("prerequisite check"); the category distinguishes them.
            category = (
                ActivityCategory.CDC
                if mode is MigrationMode.CDC
                else ActivityCategory.FULL_LOAD
            )
            blockers = sum(
                1
                for r in report.results
                if r.required
                and str(getattr(r.status, "value", r.status)).upper() == "FAIL"
            )
            log_activity(
                category,
                "prerequisite check",
                status=(
                    ActivityStatus.SUCCESS
                    if report.can_proceed
                    else ActivityStatus.INFO
                ),
                detail=(
                    "ready"
                    if report.can_proceed
                    else f"not ready: {blockers} required check(s) failed"
                ),
            )
            refresh()

        with ui.column().classes("w-full gap-3"):
            # Migration-type selector (AWS DMS-style): Full load only / CDC only /
            # Full load + CDC. The combined type runs Full Load then auto-advances
            # to the CDC step (gapless handoff from the watermark).
            _render_migration_type_selector(
                ui,
                migration_state,
                status=status,
                refresh=refresh,
                locked=migration_type_locked(
                    migration_state, job_manager, status=status
                ),
            )

            with ui.row().classes("items-center gap-2"):
                ui.label("Data Migration status:").classes(
                    "text-sm text-gray-500"
                )
                ui.badge(status.value).props(f"color={_STATUS_COLORS[status]}")

            if inventory is None:
                render_notice(
                    ui,
                    tone="warning",
                    header="Run Step 1 first",
                    body="No source inventory yet. Run Step 1 (Evaluation) to introspect "
                    "the source schema.",
                )
                return
            if not session.has_target():
                render_notice(
                    ui,
                    tone="warning",
                    header="No target connection",
                    body="No target connection configured. Set it up in the Connect "
                    "section above before starting the migration.",
                )
                return
            if not inventory.tables:
                render_notice(
                    ui,
                    tone="warning",
                    header="Nothing to migrate",
                    body="The source inventory has no tables to migrate.",
                )
                return

            error = migration_state.error
            if error and status is not StepStatus.IN_PROGRESS:
                render_notice(
                    ui,
                    tone="error",
                    header="Migration failed",
                    body=error,
                )

            async def refresh_browser() -> None:
                """Re-introspect this session's source + target for the picker.

                Session-scoped: uses only this session's connections/state so a
                refresh in one browser session never affects another. Updates the
                evaluation result so the migratable set (generated DDL + tables
                now present on the target) reflects the latest schema -- e.g.
                tables just created on the target in Step 2.
                """
                from nicegui import run as _run

                from dsql_migrator.ui.evaluation import (
                    EvaluationResult as _ER,
                )
                from dsql_migrator.ui.evaluation import (
                    _default_introspector_factory,
                    _default_target_browser_factory,
                    _find_target_conflicts,
                )

                current = eval_state.result
                if current is None:
                    ui.notify(
                        "Run Step 1 (Evaluation) first to introspect the schema.",
                        type="warning",
                    )
                    return
                if not session.has_source() or not session.has_target():
                    ui.notify(
                        "Configure and verify the source and target connections "
                        "first, then refresh.",
                        type="warning",
                    )
                    return
                ui.notify("Refreshing source/target objects...", type="info")
                try:
                    introspector = _default_introspector_factory(
                        session.source_password
                    )
                    new_inventory = await _run.io_bound(
                        introspector.introspect, session.source_config
                    )
                    browser = _default_target_browser_factory(session.aws_profile)
                    new_target = await _run.io_bound(
                        browser.browse, session.target_config
                    )
                except Exception as exc:  # noqa: BLE001
                    ui.notify(f"Could not refresh objects: {exc}", type="negative")
                    return
                eval_state.set_result(
                    _ER(
                        inventory=new_inventory,
                        assessment=current.assessment,
                        target_inventory=new_target,
                        target_conflicts=_find_target_conflicts(
                            new_inventory, new_target
                        ),
                    )
                )
                ui.notify("Object browser refreshed.", type="positive")
                refresh()

            # Lock the table picker once prerequisite checks have run for the
            # selected mode: the checks (and any CDC start offset / connector
            # config) are scoped to that exact table set, so changing it
            # afterwards would silently invalidate them. The user re-runs checks
            # to change the selection. Also lock once CDC has started: the running
            # source connector's table set is fixed (changing the picker cannot
            # add/remove streamed tables on the live pipeline).
            selection_locked = (
                migration_state.get_prereq_report(prereq_mode) is not None
                or cdc_streaming_started(migration_state, job_manager)
            )

            job = _current_job(job_manager, migration_state.job_id)

            # When locked because CDC is actually streaming, reflect the REAL set of
            # tables the connectors were deployed with (watermark-covered / confirmed
            # selection), not the generic "everything on the target" default -- so a
            # reconnect shows what CDC is truly replicating instead of every table
            # ticked-and-frozen. Only for the live-CDC lock; a prereq-only lock keeps
            # the normal selection view.
            locked_selection: Optional[list[str]] = None
            if selection_locked and cdc_streaming_started(migration_state, job_manager):
                cdc_wm = getattr(job, "watermark", None) if job is not None else None
                real = [
                    t.name for t in _cdc_tables_for_config(migration_state, inventory, cdc_wm)
                ]
                # Only override the default when we can actually resolve the streamed
                # set. If it comes back empty (no watermark row-counts and no confirmed
                # selection), fall back to the normal selection view rather than
                # showing "0 tables" while CDC is clearly running -- an empty override
                # would misrepresent just as much as "everything selected" did.
                locked_selection = real or None

            # Set the object browser apart from the rest of the page with a
            # tinted, bordered panel so it reads as a distinct browser region.
            with ui.column().classes(
                "w-full gap-2 p-3 rounded-lg border border-gray-200 bg-gray-50"
            ):
                _render_table_selection(
                    ui,
                    inventory,
                    migration_state,
                    migratable_table_names(
                        inventory, conv_state.generated_node_ids, _target_inventory()
                    ),
                    target_existing=target_existing_table_names(
                        inventory, _target_inventory()
                    ),
                    on_refresh=refresh_browser,
                    locked=selection_locked,
                    locked_selection=locked_selection,
                )

            # The tables a Full Load will migrate (same logic the run uses), so
            # the Full Load step can re-surface them for an explicit confirmation.
            migratable = migratable_table_names(
                inventory, conv_state.generated_node_ids, _target_inventory()
            )
            selected_names = effective_migration_selection(
                migratable,
                migration_state.selection,
                touched=migration_state.selection_touched,
                default=target_existing_table_names(inventory, _target_inventory()),
            )
            # A Full Load that already ran (live job, or the step reached DONE --
            # both survive a session restore) is proof its prerequisites passed at
            # start time, so a missing in-memory report (not persisted across a
            # reconnect) must not re-lock the re-run button.
            guard_reason = full_load_run_guard_reason(
                migration_state,
                inventory,
                prereq_mode=prereq_mode,
                has_run=job is not None or status is StepStatus.DONE,
            )
            active = resolve_active_substep_for_type(
                migration_state.active_substep,
                migration_type=migration_type,
                has_job=job is not None,
                full_load_done=status is StepStatus.DONE,
            )
            # Pin the view to CDC once connectors have actually been deployed. The
            # sub-step resolver falls back to full_load/prerequisites when no explicit
            # active sub-step is stored, and nothing persisted "cdc" -- so a re-render
            # after a CDC Retry (or a reconnect) would collapse the CDC section and
            # snap the user back to Prerequisites even though CDC is live. If the type
            # includes CDC and connectors exist, CDC is unambiguously the active step;
            # persist it so retries/re-renders stay put. (Only when it would otherwise
            # drift off "cdc", to avoid needless writes on steady-state polls.)
            if (
                "cdc" in substeps_for_type(migration_type)
                and getattr(migration_state, "cdc_connector_names", None)
                and active != "cdc"
            ):
                active = "cdc"
                migration_state.set_active_substep("cdc")
            # Keep the stepper in view across a *real* sub-step transition (e.g.
            # Full Load finishing auto-opens CDC, which collapses the sections
            # above it and would otherwise leave the retained scroll position
            # stranded near the page bottom). Only fire on an actual change of the
            # active sub-step -- never on the routine progress-poll re-renders that
            # keep the same active sub-step -- so the page is never yanked while
            # the user reads a running Full Load.
            if (
                migration_state.last_rendered_substep is not None
                and migration_state.last_rendered_substep != active
            ):
                ui.timer(
                    0.05,
                    lambda: ui.run_javascript(
                        "const el=document.querySelector('.dm-substeps-anchor');"
                        "if(el){el.scrollIntoView("
                        "{behavior:'smooth',block:'start'});}"
                    ),
                    once=True,
                )
            migration_state.last_rendered_substep = active
            # Per-step "done" state for the vertical stepper headers: a completed
            # step collapses to a header with a ✓ (Quasar `done` prop) so the
            # downward flow shows progress at a glance. Prerequisites are done once
            # their checks pass (guard cleared); Full Load once its workflow step is
            # DONE; CDC once the stream has started.
            prereq_done = guard_reason is None
            full_load_done = status is StepStatus.DONE
            cdc_started = cdc_streaming_started(migration_state, job_manager)

            def go(substep: str) -> None:
                migration_state.set_active_substep(substep)
                refresh()

            def start_full_load() -> None:
                migration_state.set_active_substep("full_load")
                runner()
                refresh()

            def stop_full_load() -> None:
                # Cooperatively stop the running Full Load: already-loaded tables
                # are kept; in-flight/queued tables become retryable. The worker
                # finishes its current batch and the job ends as CANCELLED.
                job_id = migration_state.job_id
                if job_id is not None:
                    job_manager.request_cancel(job_id)
                refresh()

            def _run_retry_for(names_to_retry: Sequence[str]) -> None:
                # Shared scoped re-run: carries succeeded tables forward and re-runs
                # ONLY ``names_to_retry`` (the failed set, or a single table picked
                # via per-table Reload), reusing the original watermark. Works for a
                # DONE table too (e.g. reloading a quarantine table after fixing the
                # source value), since it re-seeds exactly those chunks.
                current = _current_job(job_manager, migration_state.job_id)
                if current is None:
                    return
                names = [n for n in names_to_retry]
                if not names:
                    return
                if not session.has_source() or not session.has_target():
                    migration_state.set_error(
                        "Configure and test the source and target connections "
                        "first, then reload the table(s)."
                    )
                    refresh()
                    return
                source_config = session.source_config
                target_config = session.target_config
                assert source_config is not None and target_config is not None
                retry_inputs = DataMigrationInputs(
                    source_config=source_config,
                    source_password=session.source_password,
                    target_config=target_config,
                    inventory=inventory,
                    aws_profile=session.aws_profile,
                    staging_bucket=staging_bucket,
                    cdc_coexisting=cdc_streaming_started(
                        migration_state, job_manager
                    ),
                    table_conversions=applied_table_conversions(
                        SchemaConverter().convert(inventory),
                        conv_state.edited_target_ddls,
                    ),
                )
                retry_tables = TableSelector().resolve(
                    inventory, TableSelection(selected_tables=names)
                )
                retry_migrator = migrator_factory(retry_inputs)
                error_log = migration_state.error_log
                prior_chunks = current.chunks
                watermark = current.watermark
                accept_quarantined = migration_state.accept_quarantined_rows
                migration_state.set_active_substep("full_load")
                session.set_workflow(
                    with_status(
                        session.workflow,
                        WorkflowStep.FULL_LOAD,
                        StepStatus.IN_PROGRESS,
                    )
                )

                def work(handle: JobHandle) -> None:
                    run_full_load_retry(
                        handle,
                        prior_chunks,
                        retry_tables,
                        migrator=retry_migrator,
                        error_log=error_log,
                        watermark=watermark,
                        accept_quarantined_rows=accept_quarantined,
                    )

                migration_state.job_id = job_manager.submit(work)
                refresh()

            def retry_failed_load() -> None:
                # Re-run Full Load for only the previously failed tables, carrying
                # the succeeded tables forward so the view stays unified.
                current = _current_job(job_manager, migration_state.job_id)
                if current is None:
                    return
                _run_retry_for(failed_table_names(current))

            def reload_table(table_name: str) -> None:
                # Per-table Reload: re-run Full Load for exactly one table (even a
                # DONE one), e.g. after fixing an oversized source value so a
                # previously-quarantined row now loads. Reuses the scoped retry path.
                _run_retry_for([table_name])

            def accept_quarantine_and_continue() -> None:
                # Accept permanently-quarantined rows (>1 MiB values, etc.) as an
                # acknowledged gap and unblock CDC WITHOUT re-running: the loadable
                # rows are already on the target and the dropped rows can never load.
                # Only valid when the incompleteness is quarantine-only (no retryable
                # failures). Marks the step DONE, records the acceptance, and keeps
                # the flag so any later re-run also completes.
                current = _current_job(job_manager, migration_state.job_id)
                if current is None or not _incomplete_is_quarantine_only(
                    current, migration_state.error_log
                ):
                    return
                migration_state.set_accept_quarantined_rows(True)
                quarantined = _quarantined_row_count(
                    current, migration_state.error_log
                )
                log_activity(
                    ActivityCategory.FULL_LOAD,
                    "quarantine accepted",
                    status=ActivityStatus.SUCCESS,
                    detail=(
                        f"{quarantined} permanently-quarantined row(s) accepted as an "
                        "acknowledged gap; Full Load marked complete and CDC unblocked."
                    ),
                )
                session.set_workflow(
                    with_status(
                        session.workflow,
                        WorkflowStep.FULL_LOAD,
                        StepStatus.DONE,
                    )
                )
                refresh()

            has_full_load = "full_load" in steps
            has_cdc = "cdc" in steps
            # The Prerequisites "Continue" button targets the first phase step.
            # For the combined type the immediate next phase is Full Load, but the
            # label says "(then CDC)" so the user knows the whole Full load + CDC
            # flow starts here (CDC opens automatically once the snapshot finishes),
            # not just a stand-alone Full Load.
            first_phase = "full_load" if has_full_load else "cdc"
            if has_full_load and has_cdc:
                first_phase_label = "Continue to Full Load (then CDC)"
            elif has_full_load:
                first_phase_label = "Continue to Full Load"
            else:
                first_phase_label = "Continue to CDC"

            # Vertical stepper: the sub-steps stack top-to-bottom (Prerequisites
            # -> Full Load -> CDC) and advancing expands the next step inline below
            # the current one instead of swapping the whole panel, so the work reads
            # as one continuous downward flow. Done/upcoming steps stay as collapsed
            # headers (with their status icon) for context. Navigation stays via the
            # explicit Back/Continue buttons (no header-nav) so the user cannot jump
            # forward into a step whose prerequisites are not yet met.
            # Accordion (not a q-stepper): each sub-step is an independently
            # collapsible section, so the user can expand ANY step -- including ones
            # already completed -- to review everything done there, and several at
            # once. The active sub-step opens by default; done steps collapse to a
            # ✓ header. This is the vertical "review the whole journey" flow a
            # q-stepper cannot give (it renders only the single active panel).
            with ui.column().classes("w-full gap-0 dm-substeps-anchor"):
                # Render one accordion section. ``state`` drives the header icon/color
                # (done ✓ green / active ● primary / upcoming ○ grey); the section is
                # open when it is the active sub-step. Returns nothing; the body is
                # built by ``render_body`` inside the expansion.
                def _substep(
                    name, title, *, state, render_body, first=False, expanded=None
                ):
                    icon, color = {
                        "done": ("check_circle", "positive"),
                        "active": ("radio_button_checked", "primary"),
                        "upcoming": ("radio_button_unchecked", "grey"),
                    }.get(state, ("radio_button_unchecked", "grey"))
                    # Connector line between sections (skipped before the first), so
                    # the column still reads as an ordered Prereq → Full Load → CDC flow.
                    if not first:
                        with ui.row().classes("items-center w-full pl-3 -my-1"):
                            ui.element("div").classes(
                                "w-px h-4 bg-gray-300 ml-[10px]"
                            )
                    # Open the section that is the active sub-step by default; an
                    # explicit ``expanded`` overrides that (e.g. keep Prerequisites
                    # open while its checks run / are still required, even when the
                    # persisted active sub-step is a later one after a reconnect --
                    # otherwise the Check button's re-render would collapse it).
                    open_now = (name == active) if expanded is None else expanded
                    exp = ui.expansion(value=open_now).classes(
                        "w-full border border-gray-200 rounded-md"
                    )
                    with exp.add_slot("header"):
                        with ui.row().classes("items-center gap-2 no-wrap w-full"):
                            ui.icon(icon, color=color).classes("text-lg")
                            ui.label(title).classes("text-sm font-semibold")
                    with exp:
                        with ui.column().classes("w-full gap-2 p-1"):
                            render_body()
                    return exp

                # --- Prerequisites ---------------------------------------------
                def _prereq_body():
                    _render_prerequisites_panel(
                        ui,
                        migration_state,
                        run_checks,
                        mode=prereq_mode,
                        combined=(
                            migration_type is MigrationType.FULL_LOAD_AND_CDC
                        ),
                    )
                    with ui.row().classes("!flex w-full justify-end items-center"):
                        if guard_reason is None:
                            ui.button(
                                first_phase_label,
                                on_click=lambda t=first_phase: go(t),
                                icon="arrow_forward",
                            ).props("color=primary")
                        else:
                            inline_hint(
                                ui, guard_reason, tone="warning", classes="text-sm"
                            )

                # Keep Prerequisites expanded while it is the actionable section:
                # its checks are running, or it still blocks (guard not cleared).
                # Without this a reconnected session (whose persisted active
                # sub-step is a later one) would collapse this section the instant
                # the "Check" button triggers a re-render, hiding the running
                # spinner and results the user just asked for.
                prereq_expanded = prerequisites_section_expanded(
                    active_substep=active,
                    running=migration_state.is_prereq_running(prereq_mode),
                    done=prereq_done,
                )
                _substep(
                    "prerequisites",
                    "Prerequisites",
                    state=(
                        "done" if prereq_done
                        else "active" if active == "prerequisites"
                        else "upcoming"
                    ),
                    render_body=_prereq_body,
                    first=True,
                    expanded=prereq_expanded,
                )

                # --- Full Load -------------------------------------------------
                if has_full_load:
                    def _full_load_body():
                        _render_full_load_step(
                            ui,
                            migration_state,
                            job_manager,
                            session,
                            job=job,
                            status=status,
                            selected_names=selected_names,
                            guard_reason=guard_reason,
                            start_full_load=start_full_load,
                            retry_failed_load=retry_failed_load,
                            reload_table=reload_table,
                            accept_quarantine_and_continue=accept_quarantine_and_continue,
                            stop_full_load=stop_full_load,
                            refresh=refresh,
                        )
                        with ui.row().classes(
                            "!flex w-full justify-between items-center"
                        ):
                            ui.button(
                                "Back: Prerequisites",
                                on_click=lambda: go("prerequisites"),
                                icon="arrow_back",
                            ).props("color=primary outline")
                            if has_cdc:
                                with ui.row().classes("items-center gap-2"):
                                    if status is StepStatus.DONE:
                                        inline_hint(
                                            ui,
                                            "Full Load complete -- review the results "
                                            "above, then continue when ready.",
                                            tone="success",
                                        )
                                        ui.button(
                                            "Continue to CDC",
                                            on_click=lambda: go("cdc"),
                                            icon="arrow_forward",
                                        ).props("color=primary")
                                    elif status is StepStatus.FAILED:
                                        inline_hint(
                                            ui,
                                            "Retry the failed tables before starting "
                                            "CDC -- streaming resumes from the "
                                            "snapshot, so it must not start on "
                                            "partial data.",
                                            tone="warning",
                                        )
                                        ui.button(
                                            "Continue to CDC", icon="arrow_forward"
                                        ).props("color=primary").props("disable")
                                    else:
                                        ui.label(
                                            "Continue to CDC unlocks when the Full "
                                            "Load finishes."
                                        ).classes("text-xs text-gray-500")
                                        ui.button(
                                            "Continue to CDC", icon="arrow_forward"
                                        ).props("color=primary").props("disable")
                            elif status is StepStatus.DONE:
                                # Full-load-ONLY finished: the type has no CDC phase,
                                # so there is no "Continue to CDC" button. A user who
                                # now wants to keep the target in sync must switch the
                                # type to "CDC only" (NOT "Full load + CDC", which
                                # would re-run the snapshot) -- it attaches streaming
                                # to the already-loaded target from the Full Load
                                # watermark. Point them at the migration-type selector
                                # above, and note the infra may need deploying first
                                # (a Full-load-only plan typically has no CDC stack).
                                infra_ready = getattr(
                                    migration_state, "cdc_stack_phase", None
                                ) in ("infra", "running", "unstable")
                                body = (
                                    "To keep the target in sync with ongoing source "
                                    "changes, change the migration type above to "
                                    "\"CDC only\" — it streams from this Full Load's "
                                    "watermark onto the already-loaded target (no "
                                    "re-snapshot)."
                                )
                                if not infra_ready:
                                    body += (
                                        " CDC streaming infrastructure isn't deployed "
                                        "yet, so you'll deploy it first (~15–20 min) "
                                        "on the CDC step."
                                    )
                                render_notice(
                                    ui,
                                    tone="info",
                                    header="Full Load complete — want continuous "
                                    "replication (CDC) next?",
                                    body=body,
                                )

                    _substep(
                        "full_load",
                        "Full Load",
                        state=(
                            "done" if full_load_done
                            else "active" if active == "full_load"
                            else "upcoming"
                        ),
                        render_body=_full_load_body,
                    )

                # --- CDC -------------------------------------------------------
                if has_cdc:
                    # Discover the deployed CDC connectors/stack OFF the event loop.
                    # The describe_stacks + list_connectors calls are BLOCKING network
                    # I/O; running them during render (as before) starved the NiceGUI
                    # WebSocket. Render now only READS state — a throttled one-shot
                    # timer runs the AWS reads on a worker thread and refreshes when
                    # done. The throttle timestamp (set inside _ensure_cdc_controller)
                    # gates re-arming, so the refresh does not loop.
                    import time as _disc_time
                    from nicegui import run as _disc_run

                    _last_disc = getattr(
                        migration_state, "_cdc_discovery_monotonic", None
                    )
                    if (
                        _last_disc is None
                        or (_disc_time.monotonic() - _last_disc)
                        >= _CDC_DISCOVERY_THROTTLE_SECONDS
                    ):

                        async def _discover_cdc() -> None:
                            try:
                                await _disc_run.io_bound(
                                    _ensure_cdc_controller, migration_state, session
                                )
                            except Exception:  # noqa: BLE001 - best-effort discovery
                                pass
                            # Log any connector RUNNING/FAILED transition (on change).
                            _log_cdc_connector_transitions(migration_state, job_manager)
                            refresh()

                        ui.timer(0.05, _discover_cdc, once=True)  # type: ignore[attr-defined]

                    def _cdc_body():
                        _render_cdc_step(
                            ui,
                            migration_state,
                            job_manager,
                            refresh,
                            inventory=inventory,
                            migration_type=migration_type,
                            run_checks=run_checks,
                            session=session,
                        )
                        with ui.row().classes(
                            "!flex w-full justify-start items-center"
                        ):
                            back_target = (
                                "full_load" if has_full_load else "prerequisites"
                            )
                            back_label = (
                                "Back: Full Load" if has_full_load
                                else "Back: Prerequisites"
                            )
                            ui.button(
                                back_label,
                                on_click=lambda t=back_target: go(t),
                                icon="arrow_back",
                            ).props("color=primary outline")

                    _substep(
                        "cdc",
                        "CDC",
                        state=(
                            "done" if cdc_started
                            else "active" if active == "cdc"
                            else "upcoming"
                        ),
                        render_body=_cdc_body,
                    )

    return content, runner


def prerequisites_section_expanded(
    *, active_substep: Optional[str], running: bool, done: bool
) -> bool:
    """Whether the Prerequisites sub-step section should render expanded.

    The section normally follows the active sub-step (open only when it is the
    active one). But it must ALSO stay open while it is the actionable section --
    its checks are ``running``, or it is not yet ``done`` (the run guard still
    blocks). This is what keeps it expanded through the "Check" button's
    re-render in a reconnected session, whose persisted ``active_substep`` may be
    a later step (e.g. ``"full_load"``): without it, clicking Check would collapse
    the section and hide the running spinner / results. NiceGUI-agnostic for tests.
    """
    return active_substep == "prerequisites" or running or not done


def full_load_run_guard_reason(
    state: DataMigrationState,
    inventory: Optional[SourceInventory],
    *,
    prereq_mode: MigrationMode = MigrationMode.FULL_LOAD,
    has_run: bool = False,
) -> Optional[str]:
    """Return a disable reason for the Data Migration Run button, or ``None``.

    Gates the step's Run button on the prerequisite report for ``prereq_mode``
    (Property 14): runnable only once the user has run the checks for that mode
    and every required check passed. ``prereq_mode`` is the mode the selected
    migration type checks (Full load only -> FULL_LOAD; CDC only / combined ->
    CDC, whose checks are a superset). Returns guidance otherwise (no inventory,
    checks not yet run, or a failed required check). NiceGUI-agnostic for tests.

    ``has_run`` says a Full Load already ran for this migration (a job exists or
    the workflow's Full Load step reached DONE). A run can only have started once
    the prerequisite checks passed (the Prerequisites "Continue" gate), so when
    ``has_run`` is set and the in-memory report is simply absent -- e.g. after a
    session restore, which does NOT persist the report -- we do not re-block the
    (re-)run button. This is what lets a reconnected user navigate Back to a
    finished Full Load and re-run it. A report that is present but failing still
    blocks, since that is a live signal worth surfacing.

    The message distinguishes a *reconnected* user who had already cleared the
    prerequisites (but hadn't started the load yet) from a first-time user who
    never ran them. The persisted ``active_substep`` is the tell: the "Continue
    to Full Load/CDC" button that advances it past ``"prerequisites"`` is only
    reachable once the checks passed, and it survives a restart -- yet the report
    itself does not. So ``report is None`` together with an advanced
    ``active_substep`` (and no started run) means "reconnected, checks just need a
    quick re-run", which we word accordingly instead of the blunt first-run
    prompt. Either way the run stays gated until the read-only checks are re-run
    (the connection was re-established on reconnect, so the old result can't be
    trusted).
    """
    if inventory is None or not inventory.tables:
        return "Run Step 1 (Evaluation) first to introspect the source schema."
    report = state.get_prereq_report(prereq_mode)
    if report is None:
        if has_run:
            return None
        if getattr(state, "active_substep", None) in ("full_load", "cdc"):
            return (
                "Reconnected — re-run the prerequisite checks (Prerequisites "
                "step) to resume. They're read-only and quick; your progress "
                "wasn't lost, but the results aren't kept across an app restart."
            )
        return "Run the prerequisite checks first (Prerequisites tab)."
    return prerequisite_block_reason(report)


def _render_watermark(ui: object, job: MigrationJob) -> None:
    """Render the export watermark for ``job`` (Requirement 8.5 / Property 11)."""
    ui.label("Export watermark").classes("text-lg font-semibold")  # type: ignore[attr-defined]
    if job.watermark is None:
        ui.label(  # type: ignore[attr-defined]
            "The export consistency point is captured when the migration starts."
        ).classes("text-sm text-gray-500")
        return

    display = format_watermark(job.watermark)
    ui.label(display.summary).classes("text-sm text-gray-700")  # type: ignore[attr-defined]

    columns = [
        {"name": "field", "label": "Field", "field": "field", "align": "left"},
        {"name": "value", "label": "Value", "field": "value", "align": "left"},
    ]
    rows = [
        {"field": "Binlog coordinate (file:position)", "value": display.coordinate},
        {"field": "GTID set", "value": display.gtid},
        {"field": "Server UUID", "value": display.server_uuid},
        {"field": "Snapshot timestamp (UTC)", "value": display.snapshot_timestamp},
    ]
    ui.table(columns=columns, rows=rows).classes("w-full")  # type: ignore[attr-defined]

    if display.table_row_counts:
        approximate = getattr(job.watermark, "row_counts_approximate", False)
        heading = (
            f"Snapshot row counts (estimated, {len(display.table_row_counts)})"
            if approximate
            else f"Snapshot row counts ({len(display.table_row_counts)})"
        )
        count_columns = [
            {"name": "table", "label": "Table", "field": "table", "align": "left"},
            {
                "name": "rows",
                "label": "Snapshot rows (est.)" if approximate else "Snapshot rows",
                "field": "rows",
            },
        ]
        count_rows = [
            {"table": table, "rows": count}
            for table, count in display.table_row_counts.items()
        ]
        # Collapsed by default so the watermark stays compact; the per-table
        # counts are detail the user can expand on demand.
        with ui.expansion(heading, icon="numbers").classes("w-full"):  # type: ignore[attr-defined]
            if approximate:
                ui.label(  # type: ignore[attr-defined]
                    "Estimates from the source catalog (no COUNT(*) scan) to "
                    "minimize source load; exact counts are verified in Validation."
                ).classes("text-xs text-gray-400")
            ui.table(  # type: ignore[attr-defined]
                columns=count_columns, rows=count_rows, row_key="table"
            ).classes("w-full")


def _render_load_status(ui, view: LoadStatusView) -> None:
    """Render the unified load status (Full Load or CDC) -- Req 13.1.

    One component for both modes: Full Load shows the progress bar + terminal
    summary; CDC shows continuous lag / "caught up to" / DLQ depth / connector
    states. Both show the same per-table table (with an errors column sourced
    from the single error log).
    """
    heading = "Migration progress" if view.kind == LoadKind.FULL_LOAD else "CDC status"
    ui.label(heading).classes("text-lg font-semibold")

    if not view.tables and view.kind == LoadKind.FULL_LOAD:
        ui.label("No tables to migrate.").classes("text-sm text-gray-500")
        return

    if view.progress_pct is not None:
        ui.linear_progress(value=view.progress_pct / 100.0, show_value=False).props(
            "instant-feedback"
        ).classes("w-full")
        ui.label(
            f"Summary — {view.progress_pct:.1f}% complete, "
            f"{view.tables_done}/{len(view.tables)} tables done, "
            f"{view.tables_failed} failed"
        ).classes("text-sm text-gray-600")

    if view.kind == LoadKind.CDC:
        bits: list[str] = []
        if view.lag_seconds is not None:
            bits.append(f"lag {view.lag_seconds:.1f}s")
        if view.caught_up_to is not None:
            bits.append(f"caught up to {view.caught_up_to.isoformat()}")
        if view.dlq_depth is not None:
            bits.append(f"DLQ depth {view.dlq_depth}")
        if bits:
            ui.label("  ·  ".join(bits)).classes("text-sm text-gray-600")
        if view.connector_states:
            states = ", ".join(
                f"{name}={state}" for name, state in view.connector_states.items()
            )
            ui.label(f"Connectors: {states}").classes("text-xs text-gray-500")

    columns = [
        {"name": "table", "label": "Table", "field": "table", "align": "left"},
        {"name": "state", "label": "Status", "field": "state"},
        {"name": "rows_loaded", "label": "Rows loaded", "field": "rows_loaded"},
        {"name": "errors", "label": "Errors", "field": "errors"},
    ]
    rows = [
        {
            "table": row.table,
            "state": row.state,
            "rows_loaded": row.rows_loaded if row.rows_loaded is not None else "",
            "errors": row.errors,
        }
        for row in view.tables
    ]
    ui.table(columns=columns, rows=rows, row_key="table").classes("w-full")


def _split_migration_schema(name: str) -> tuple[str, str]:
    """Split a possibly-qualified ``schema.object`` name into (schema, object).

    Mirrors the Step 2 Object browser grouping: a qualified ``database.object``
    name splits on the dot; a plain name falls under the ``"source"`` schema.
    """
    if "." in name:
        schema, _, obj = name.partition(".")
        return schema, obj
    return "source", name


def generated_table_names(
    inventory: SourceInventory, generated_node_ids: Optional[Sequence[str]]
) -> list[str]:
    """Return inventory table names whose schema DDL was generated in Step 2.

    The Data Migration table picker is scoped to the tables the user generated
    schema DDL for in Step 2 (Schema Conversion): only those have a target table
    to load into. ``generated_node_ids`` is the Schema Conversion
    ``generated_node_ids`` snapshot (object-leaf node ids); this maps it back to
    the table names present in ``inventory`` (inventory order). ``None`` /empty
    (nothing generated yet) yields an empty list.
    """
    if not generated_node_ids:
        return []
    names = selected_object_names(generated_node_ids)
    return [table.name for table in inventory.tables if table.name in names]


def target_existing_table_names(
    inventory: SourceInventory, target: Optional[TargetInventory]
) -> list[str]:
    """Return source table names that already exist on the target DSQL.

    A table whose schema DDL was applied to DSQL in an earlier Schema Conversion
    run (a prior session or out of band) already has a target table to load into,
    so it is migratable even without a current-session DDL generation.

    Matching is **qualified**: the converter creates a PostgreSQL schema named
    after each source MySQL database and qualifies tables into it, so a source
    ``db.table`` exists on the target only when a target table of that name lives
    in the matching schema -- NOT merely because some other schema happens to
    have a same-named table (which would otherwise wrongly pre-select a table
    whose schema is absent from the target). An unqualified source table
    (single-database introspection, schema ``"source"``) falls back to matching
    by unqualified name. Comparison is case-insensitive (PostgreSQL identifier
    folding); target views are ignored. Returns inventory order; empty when no
    target inventory is available (Step 1 not run / no target catalog).
    """
    if target is None:
        return []
    target_qualified = {
        (schema.name.lower(), relation.name.lower())
        for schema in target.schemas
        for relation in schema.tables
    }
    target_unqualified = {name for _schema, name in target_qualified}
    existing: list[str] = []
    for table in inventory.tables:
        schema, obj = _split_migration_schema(table.name)
        obj_lower = obj.lower()
        if schema == "source":
            if obj_lower in target_unqualified:
                existing.append(table.name)
        elif (schema.lower(), obj_lower) in target_qualified:
            existing.append(table.name)
    return existing


def migratable_table_names(
    inventory: SourceInventory,
    generated_node_ids: Optional[Sequence[str]],
    target: Optional[TargetInventory],
) -> list[str]:
    """Return the tables that can be migrated, in inventory order (Req 5.9).

    A table is migratable when it has a target table to load into, which is true
    when EITHER its schema DDL was generated in this session's Step 2
    (``generated_node_ids``) OR it already exists on the target DSQL (``target``)
    -- e.g. Schema Conversion was applied in a prior session/out of band. Taking
    the union lets Data Migration proceed without re-running Schema Conversion in
    the current session when the target schema is already in place.
    """
    migratable = set(generated_table_names(inventory, generated_node_ids))
    migratable |= set(target_existing_table_names(inventory, target))
    return [table.name for table in inventory.tables if table.name in migratable]


# MigrationType, its resolvers (prereq_mode_for_type / substeps_for_type /
# resolve_active_substep[_for_type]) and the selector metadata are pure (no
# NiceGUI), so they live in ._models and are re-imported below with the other
# view-models. _migration_resumed_committed / migration_type_locked stay here
# (they read migration_state / job_manager).


def migration_type_locked(migration_state, job_manager, *, status) -> bool:
    """True only once CDC streaming has STARTED, so the type must not change.

    Thin boolean over :func:`migration_type_lock_reason` (the single source of
    truth for *why* the type is locked). ``job_manager`` is accepted for caller
    signature compatibility but unused — the decision is a pure function of the
    workflow status and the migration state.
    """
    return migration_type_lock_reason(migration_state, status=status) is not None


def migration_type_lock_reason(migration_state, *, status) -> Optional[str]:
    """Why the migration type is locked, or ``None`` if it can still be changed.

    Separates two distinct sources so the UI can explain the lock clearly:

    * **Owned** — a migration this session is actively running
      (``StepStatus.IN_PROGRESS``). Changing the type mid-run is incoherent.
    * **Discovered** — CDC connectors / a running cdc-stack were found on AWS
      (possibly deployed in a previous session; ``cdc_connector_names`` survives a
      restore). Switching the type would orphan/break that live pipeline.

    Both legitimately freeze the choice, but the reason (and the remedy) differ.
    Pure: reads only ``status`` and already-populated state — no AWS I/O — so it is
    safe to call during render and is unit-testable.
    """
    if status is StepStatus.IN_PROGRESS:
        return (
            "Locked while a migration is in progress — finish or cancel it to "
            "change the type."
        )
    if getattr(migration_state, "cdc_stack_phase", None) == "running" or getattr(
        migration_state, "cdc_connector_names", None
    ):
        return (
            "Locked because CDC connectors from a previous run are still deployed "
            "(Start over does not delete them). To change the type, use 'Delete "
            "CDC infrastructure' on the CDC step first."
        )
    return None


def _render_migration_type_selector(
    ui, migration_state, *, status, refresh, locked: Optional[bool] = None
) -> None:
    """Render the migration type as AWS-console-style radio tiles.

    Each type is a selectable card (radio + icon + title + description), matching
    the Cloudscape "tiles" pattern AWS uses for migration-type choices. The
    selected tile is highlighted (primary border + tint); changing the type
    resets the active sub-step and re-renders. ``locked`` (computed by the caller
    via :func:`migration_type_locked`) disables the whole group so the type
    cannot change once a migration has started; when ``None`` it falls back to
    locking only while this step is ``IN_PROGRESS``.
    """
    running = locked if locked is not None else (status is StepStatus.IN_PROGRESS)
    selected = migration_state.migration_type

    def _select(new_type: MigrationType) -> None:
        if running or new_type is selected:
            return
        migration_state.set_migration_type(new_type)
        migration_state.set_active_substep(None)  # default for the new type
        refresh()

    ui.label("Migration type").classes("text-sm font-semibold")  # type: ignore[attr-defined]
    # Explain WHY the choice is locked (a dead, silently-disabled control looked
    # like a bug). The reason comes from the single pure source so the message and
    # the lock can never drift apart.
    lock_reason = migration_type_lock_reason(migration_state, status=status)
    if running and lock_reason:
        ui.label(lock_reason).classes(  # type: ignore[attr-defined]
            "text-xs text-amber-700 mb-1"
        )
    with ui.row().classes("w-full gap-3 items-stretch no-wrap"):  # type: ignore[attr-defined]
        for mt in MigrationType:
            meta = _MIGRATION_TYPE_META[mt]
            is_selected = mt is selected
            # Cloudscape-tile look: bordered card, primary border + tint when
            # selected, muted + non-interactive while a job runs.
            border = "border-blue-500" if is_selected else "border-gray-300"
            bg = "bg-blue-50" if is_selected else "bg-white"
            interactivity = (
                "opacity-60 cursor-not-allowed"
                if running
                else "cursor-pointer hover:border-blue-400"
            )
            tile = ui.card().classes(  # type: ignore[attr-defined]
                f"flex-1 p-3 rounded-lg border {border} {bg} {interactivity} "
                "transition-colors gap-1"
            )
            tile.on("click", lambda _e=None, _mt=mt: _select(_mt))
            with tile:
                with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon(  # type: ignore[attr-defined]
                        "radio_button_checked"
                        if is_selected
                        else "radio_button_unchecked",
                        color="primary" if is_selected else "grey-6",
                    ).classes("text-lg")
                    ui.icon(  # type: ignore[attr-defined]
                        meta.icon,
                        color="primary" if is_selected else "grey-7",
                    ).classes("text-lg")
                    ui.label(meta.label).classes(  # type: ignore[attr-defined]
                        "text-sm font-semibold"
                    )
                ui.label(meta.blurb).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-600"
                )
                if meta.when:
                    ui.label(meta.when).classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-700 font-medium mt-1"
                    )
                if meta.requirements:
                    # Purely informational: what the mode needs (verified later by
                    # the Prerequisites step). Kept a calm neutral gray -- NOT a
                    # warning/error color -- so it reads as a heads-up at decision
                    # time, not an alarm before anything has run. The icon still
                    # distinguishes "needs infra" (info) from "no extra infra"
                    # (check_circle), but both use the same quiet tone.
                    needs_infra = "MSK" in meta.requirements
                    with ui.row().classes("items-start gap-1 no-wrap mt-1"):  # type: ignore[attr-defined]
                        ui.icon(  # type: ignore[attr-defined]
                            "info" if needs_infra else "check_circle",
                        ).classes("text-gray-500 text-xs mt-0.5")
                        ui.label(meta.requirements).classes("text-xs text-gray-500")
    if running:
        ui.label(  # type: ignore[attr-defined]
            "Migration type is locked once the migration has started."
        ).classes("text-xs text-gray-500")




def format_selected_workloads(names: Sequence[str]) -> str:
    """Return a short headline for the tables a Full Load will migrate."""
    count = len(names)
    if count == 0:
        return "No tables selected"
    noun = "table" if count == 1 else "tables"
    return f"{count} {noun} selected for Full Load"


def effective_migration_selection(
    migratable: Sequence[str],
    selection: TableSelection,
    *,
    touched: bool,
    default: Optional[Sequence[str]] = None,
) -> list[str]:
    """Resolve the effective migration selection within the migratable set.

    The migratable universe is ``migratable`` (tables that have a target table to
    load into -- DDL generated this session or already present on the target).
    Until the user changes the picker (``touched`` is ``False``) the pre-selected
    default is ``default`` intersected with ``migratable`` (in ``migratable``
    order); when ``default`` is ``None`` every migratable table is pre-selected.
    The default is the tables whose DDL exists on the target DSQL, so only tables
    that actually have a destination table are checked out of the box. Once
    changed, the effective selection is the user's chosen names intersected with
    the still-migratable set; an explicit empty selection stays empty so the run
    guard can require at least one table.
    """
    migratable_order = list(migratable)
    if not touched:
        if default is None:
            return migratable_order
        default_set = set(default)
        return [name for name in migratable_order if name in default_set]
    chosen = set(selection.selected_tables)
    return [name for name in migratable_order if name in chosen]


def build_migration_table_tree(
    inventory: SourceInventory, migratable: Sequence[str]
) -> list[dict]:
    """Assemble the hierarchical table picker for ``ui.tree`` (Schema/Tables).

    Mirrors the Step 2 Object browser layout (schema -> Tables -> table leaves)
    but is tables-only: data migration loads table rows, so views/triggers/
    routines are not offered. Only migratable tables (in ``migratable`` -- DDL
    generated this session or already present on the target) are listed and
    tickable; tables with no target table to load into are omitted entirely, so
    a source-only schema absent from the target does not appear (and never shows
    a misleading indeterminate parent checkbox). Leaf node ids reuse
    ``TABLE_PREFIX`` so ticks map back to table
    names via :func:`~dsql_migrator.ui.schema_conversion.selected_object_names`.

    Each table leaf also carries ``has_pk`` (whether the table has a primary key)
    and ``header: "table"`` so the renderer's ``header-table`` Quasar slot can
    show a small PK indicator beside the leaf label -- Aurora DSQL requires a
    primary key, so a missing one is worth flagging in the picker.
    """
    migratable_set = set(migratable)
    order: list[str] = []
    by_schema: dict[str, list[dict]] = {}

    for table in inventory.tables:
        # Scoped to migratable tables (Req 5.9): a table with no target table to
        # load into is omitted entirely (not shown disabled), so the picker
        # lists only what can actually be migrated. This also means a schema
        # whose tables are all non-migratable never appears -- avoiding a
        # misleading indeterminate parent checkbox for a schema nothing can be
        # selected under (e.g. a source-only schema absent from the target).
        if table.name not in migratable_set:
            continue
        schema, obj = _split_migration_schema(table.name)
        if schema not in by_schema:
            by_schema[schema] = []
            order.append(schema)
        by_schema[schema].append(
            {
                "id": f"{TABLE_PREFIX}{table.name}",
                "label": obj,
                "has_pk": bool(table.primary_key),
                "header": "table",  # -> the "header-table" leaf slot (PK badge)
            }
        )

    schema_nodes: list[dict] = []
    for schema in order:
        tables = by_schema[schema]
        category = {
            "id": f"category:tables:{schema}",
            "label": f"Tables ({len(tables)})",
            "children": tables,
        }
        schema_nodes.append(
            {
                "id": f"schema:{schema}",
                "label": f"Schema: {schema}",
                "children": [category],
            }
        )
    return schema_nodes


def _render_table_selection(
    ui,
    inventory: SourceInventory,
    migration_state,
    migratable: Sequence[str],
    *,
    target_existing: Sequence[str] = (),
    on_refresh: Optional[Callable[[], object]] = None,
    locked: bool = False,
    locked_selection: Optional[Sequence[str]] = None,
) -> None:
    """Render the hierarchical table picker scoped to migratable tables (Req 5.9).

    Mirrors the Step 2 Object browser (schema -> Tables -> table leaves):
    only migratable tables -- those with a target table to load into, whether the
    DDL was generated this session or already exists on the target (Schema
    Conversion run earlier) -- are listed and tickable; tables with no target
    table are omitted entirely (and a schema with none does not appear).
    By default only ``target_existing`` (tables whose DDL has actually been
    created on the target DSQL) are pre-ticked, since only those have a
    destination table to load into; any other migratable table stays available
    but unticked. Pre-selected tables can be unticked. The ticked set is
    persisted to the session selection so Full Load / CDC / prerequisite checks
    run over exactly the chosen tables (Property 16). ``on_refresh``, when given,
    re-introspects this session's source/target so the browser reflects the
    latest schema (e.g. tables just created on the target in Step 2).
    """
    with ui.row().classes("items-center gap-1"):
        ui.label("Tables to migrate").classes("text-sm font-semibold")
        # The refresh button is hidden once locked: re-introspecting could change
        # the migratable set out from under the prerequisite checks/config.
        if on_refresh is not None and not locked:
            ui.button(on_click=on_refresh).props(
                "flat dense round size=sm icon=refresh"
            ).tooltip("Refresh source/target objects")
        if locked:
            ui.icon("lock", color="grey").classes("text-sm").tooltip(
                "Locked after prerequisite checks ran for this selection"
            )
    ui.label(
        "Only tables are listed here — they hold the row data to migrate. Views, "
        "triggers and routines have no data of their own and are not migrated in "
        "this step; they are created in Schema Conversion (a view reflects its "
        "tables' data once loaded)."
    ).classes("text-xs text-gray-500")
    if locked:
        ui.label(
            "Locked — prerequisite checks ran for this table selection. "
            "Re-run the checks (Prerequisites step) to change which tables "
            "are migrated."
        ).classes("text-xs text-gray-500")
    if not migratable:
        render_notice(
            ui,
            tone="warning",
            header="No tables ready to migrate",
            body="No tables are ready to migrate yet. In Step 2 (Schema Conversion), "
            'tick tables and click "Generate DDL for selected" -- or connect to a '
            "target where the tables already exist. Only tables with a target "
            "table can be migrated.",
        )
        return

    pre_selected_count = sum(1 for n in migratable if n in set(target_existing))
    ui.label(
        f"Pre-selected by default: {pre_selected_count} table(s) that already "
        "exist on the target DSQL (schema applied earlier — this session or a "
        "prior one). Only tables with a target table to load into are listed; "
        "untick any to skip, or use Select all / Unselect all below."
    ).classes("text-xs text-gray-400")

    migratable_order = list(migratable)
    if locked and locked_selection is not None:
        # Locked because CDC is live (or prereqs ran): the connectors stream a
        # FIXED table set, so the browser must reflect THAT set -- not the generic
        # "everything on the target" default. Without this, a reconnect (which
        # resets selection_touched paths) shows every migratable table ticked and
        # frozen, misrepresenting what CDC is actually replicating. Intersect with
        # the migratable universe so only real, tickable leaves are marked.
        locked_set = set(locked_selection)
        effective = [name for name in migratable_order if name in locked_set]
    else:
        effective = effective_migration_selection(
            migratable_order,
            migration_state.selection,
            touched=migration_state.selection_touched,
            default=list(target_existing),
        )
    # Object-browser tree (schema -> Tables -> leaf, leaf checkboxes), styled to
    # match the app's AWS/Cloudscape look: a live name filter + selection counter
    # above a white, bordered scroll panel. Each leaf shows a small PK indicator
    # via the "header-table" Quasar slot (Aurora DSQL requires a primary key, so a
    # missing one is worth flagging). The slot is a client-side Vue template, so
    # it adds no per-node Python work.
    nodes = build_migration_table_tree(inventory, migratable_order)
    effective_set = set(effective)
    total_leaves = sum(len(n["children"][0]["children"]) for n in nodes)

    def on_tick(event: object) -> None:
        value = getattr(event, "value", None) or []
        names = selected_object_names(value)
        chosen = [name for name in migratable_order if name in names]
        migration_state.set_selection(TableSelection(selected_tables=chosen))
        count_label.text = f"{len(chosen)} of {total_leaves} selected"

    # Filter + bulk actions + selection counter (Cloudscape table header band).
    filter_input = None
    count_label = None
    if not locked:
        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            filter_input = (
                ui.input(placeholder="Filter tables by name")
                .props("dense clearable outlined")
                .classes("flex-1 min-w-0 text-sm")
            )

            def _dm_select_all() -> None:
                tree.tick()
                migration_state.set_selection(
                    TableSelection(selected_tables=list(migratable_order))
                )
                count_label.text = f"{len(migratable_order)} of {total_leaves} selected"

            def _dm_unselect_all() -> None:
                tree.untick()
                migration_state.set_selection(TableSelection(selected_tables=[]))
                count_label.text = f"0 of {total_leaves} selected"

            ui.button("Select all", on_click=_dm_select_all).props(
                "flat dense no-caps size=sm color=primary icon=done_all"
            )
            ui.button("Unselect all", on_click=_dm_unselect_all).props(
                "flat dense no-caps size=sm color=grey-7 icon=remove_done"
            )
            count_label = ui.label(
                f"{len(effective_set)} of {total_leaves} selected"
            ).classes("text-xs text-gray-500 whitespace-nowrap")

    # Legend for the per-table primary-key indicator, so the check / warning icons
    # beside each leaf are self-explanatory (they also carry a hover tooltip).
    with ui.row().classes("items-center gap-3 w-full text-xs text-gray-500"):
        with ui.row().classes("items-center gap-1 no-wrap"):
            ui.icon("check_circle", color="green-6").classes("text-sm")
            ui.label("Has a primary key")
        with ui.row().classes("items-center gap-1 no-wrap"):
            ui.icon("warning", color="amber-7").classes("text-sm")
            ui.label("No primary key (required to migrate to Aurora DSQL)")

    with ui.scroll_area().classes(
        "w-full bg-white rounded-md border border-gray-200"
    ).style("height: 280px"):
        # When locked, omit on_tick so ticks can't change the selection; grey the
        # checkboxes (vs primary) so it reads as non-interactive, not just inert.
        tree = ui.tree(
            nodes,
            label_key="label",
            node_key="id",
            tick_strategy="leaf",
            on_tick=None if locked else on_tick,
        )
        tree.props(f"tick-color={'grey' if locked else 'primary'} no-connectors")
        # Wire the name filter to the tree (Quasar QTree ``filter`` prop): typing
        # narrows the visible nodes to matching table leaves. Without this bind
        # the input renders but does nothing.
        if filter_input is not None:
            filter_input.bind_value_to(tree, "filter")
        # A small PK indicator beside each table leaf (client-side Vue template):
        # a green check when the table has a primary key, an amber warning when it
        # does not. Non-leaf nodes (schema / "Tables (N)") have no "header" key, so
        # this slot only renders on table leaves.
        tree.add_slot(
            "header-table",
            r"""
            <div class="row items-center no-wrap">
              <span class="text-body2">{{ props.node.label }}</span>
              <q-icon v-if="props.node.has_pk" name="check_circle"
                      color="green-6" size="16px" class="q-ml-xs">
                <q-tooltip>Has a primary key</q-tooltip>
              </q-icon>
              <q-icon v-else name="warning" color="amber-7" size="16px" class="q-ml-xs">
                <q-tooltip>No primary key — required to migrate to Aurora DSQL</q-tooltip>
              </q-icon>
            </div>
            """,
        )
        # Load fully expanded so every schema's tables are visible without manual
        # drilling (the migratable-only tree keeps this bounded).
        tree.expand()
        ticked_ids = [f"{TABLE_PREFIX}{name}" for name in effective]
        if ticked_ids:
            tree.tick(ticked_ids)
        if locked:
            # pointer-events-none blocks clicks; the greyed checkboxes + dimmed
            # tree communicate "selection is locked" at a glance (not just dead).
            tree.props("no-selection-unset").classes(
                "pointer-events-none opacity-70"
            )


def _render_full_load_step(
    ui,
    migration_state,
    job_manager,
    session,
    *,
    job,
    status,
    selected_names,
    guard_reason,
    start_full_load,
    retry_failed_load,
    reload_table,
    accept_quarantine_and_continue,
    stop_full_load,
    refresh,
) -> None:
    """Render the Full Load step: confirm the selected workloads, then run it.

    Re-surfaces the tables chosen in the object browser as an explicit
    confirmation (Usability-first) and gates the start on the Full Load
    prerequisite verdict. Starting opens a confirmation dialog listing exactly
    what will be migrated before the snapshot export/load begins. While a job
    runs (or after it finishes) the watermark, live per-table progress (rows and
    percent vs the source snapshot count), retry of only the failed tables, a
    source-vs-loaded completeness verdict, and the downloadable error log are
    shown here.
    """
    ui.label("Workloads to migrate").classes("text-sm font-semibold")
    ui.label(format_selected_workloads(selected_names)).classes(
        "text-sm text-gray-600"
    )
    if selected_names:
        with ui.row().classes("items-center gap-1 flex-wrap"):
            for name in selected_names:
                ui.badge(name).props("color=blue-grey-6 outline")
    ui.label(
        "These tables (ticked in the object browser above) are exported from the "
        "source as of one consistency watermark and bulk-loaded into the target. "
        "The source is read only."
    ).classes("text-xs text-gray-400")

    # Explicit confirmation before the (target-writing) Full Load begins. Tables
    # that already hold data are listed as a destructive DROP+recreate warning.
    replace_targets = sorted(migration_state.replace_targets)
    # Re-running Full Load while CDC is streaming is dangerous: the snapshot writes
    # (and any DROP+recreate) collide with the live CDC sink, which resumes from
    # the OLD watermark -- creating gaps/overlap or losing rows the stream wrote.
    # We surface a hard warning (not a silent block) so the operator decides.
    cdc_live = cdc_streaming_started(migration_state, job_manager)

    def _open_confirm_dialog_now() -> None:
        """Build + open the Start-Full-Load confirm dialog in the TOP-LEVEL client.

        Created in the client context (not the per-render content slot) and opened
        on demand, so the periodic progress-poll re-render does NOT tear it down a
        couple of seconds after it appears.
        """
        from nicegui import context as _ctx

        client = _ctx.client
        replace_targets_now = sorted(migration_state.replace_targets)
        cdc_live_now = cdc_streaming_started(migration_state, job_manager)

        def _build() -> None:
            with ui.dialog() as confirm_dialog, ui.card().classes("min-w-[360px]"):
                ui.label("Start Full Load?").classes("text-lg font-semibold")
                ui.label(
                    f"{format_selected_workloads(selected_names)}. The target "
                    "tables will receive the snapshot rows; the source is accessed "
                    "read only."
                ).classes("text-sm")
                if cdc_live_now:
                    with ui.card().classes(
                        "w-full bg-red-50 border border-red-200 gap-1"
                    ):
                        ui.label(
                            "⚠ CDC is currently streaming. Re-running Full Load now "
                            "will collide with the live pipeline -- the snapshot (and "
                            "any DROP+recreate) writes to tables the CDC sink is "
                            "actively applying changes to, which can drop streamed "
                            "rows or create a gap/overlap (CDC resumes from the "
                            "ORIGINAL watermark, not this new one)."
                        ).classes("text-sm text-red-700")
                        ui.label(
                            "Stop CDC first (CDC step → Stop CDC), re-run the Full "
                            "Load, then start CDC again so it resumes from the new "
                            "snapshot."
                        ).classes("text-xs text-red-700")
                if selected_names:
                    with ui.row().classes("items-center gap-1 flex-wrap"):
                        for name in selected_names:
                            ui.badge(name).props("color=blue-grey-6 outline")
                if replace_targets_now and not cdc_live_now:
                    with ui.card().classes(
                        "w-full bg-red-50 border border-red-200 gap-1"
                    ):
                        ui.label(
                            f"⚠ {len(replace_targets_now)} table(s) already contain "
                            "data and will be DROPPED and recreated before loading "
                            "(DSQL has no TRUNCATE). Existing rows in these tables "
                            "will be permanently lost. This cannot be undone."
                        ).classes("text-sm text-red-700")
                        with ui.row().classes("items-center gap-1 flex-wrap"):
                            for name in replace_targets_now:
                                ui.badge(name).props("color=red-6 outline")

                def _confirm() -> None:
                    confirm_dialog.close()
                    start_full_load()

                with ui.row().classes("justify-end w-full gap-2"):
                    ui.button("Cancel", on_click=confirm_dialog.close).props("flat")
                    if cdc_live_now:
                        confirm_label = "Re-run anyway (CDC is live)"
                    elif replace_targets_now:
                        confirm_label = "Drop, recreate and load"
                    else:
                        confirm_label = "Confirm and start"
                    confirm_color = (
                        "negative" if (replace_targets_now or cdc_live_now) else "primary"
                    )
                    ui.button(confirm_label, on_click=_confirm).props(
                        f"color={confirm_color}"
                    )
            confirm_dialog.open()

        with client:  # type: ignore[attr-defined]
            _build()

    # Re-entrancy guard: the confirm handler runs a ~1-2s off-loop probe before
    # opening the dialog, so a double-click would open TWO dialogs. This flag drops
    # a second click while the first is still resolving; the clicked button is also
    # disabled + relabeled "Checking…" for a visible cue (restored on dialog open).
    _confirm_busy = {"value": False}

    async def _open_full_load_confirm(event: object = None) -> None:
        """Check which selected target tables hold data, then open the dialog.

        Runs the read-only non-empty probe off the event loop so the UI stays
        responsive, records the result (drives the destructive warning + the
        run's replace set), then opens the confirm dialog in the top-level client
        context (so the progress poll re-render cannot close it).
        """
        if _confirm_busy["value"]:
            return  # a probe is already in flight -> ignore the extra click
        _confirm_busy["value"] = True
        btn = getattr(event, "sender", None)
        original_text = getattr(btn, "text", None) if btn is not None else None
        # Preserve the button's own icon (play_arrow for Start, restart_alt for the
        # terminal Re-run) so restore does not swap it for the wrong one.
        original_icon = None
        if btn is not None:
            original_icon = getattr(btn, "_props", {}).get("icon")
        if btn is not None:
            try:
                btn.disable()
                btn.set_text("Checking…")
                btn.props("icon=hourglass_top")
            except Exception:  # noqa: BLE001 - cue is best-effort
                pass
        try:
            migration_state.set_replace_targets(frozenset())
            target_config = getattr(session, "target_config", None)
            if target_config is not None and selected_names:
                from nicegui import run

                def _probe() -> frozenset[str]:
                    connector = DsqlConnector(
                        target_config, aws_profile=session.aws_profile
                    )
                    return frozenset(
                        tables_with_rows(
                            list(selected_names), connection_factory=connector.connect
                        )
                    )

                try:
                    found = await run.io_bound(_probe)
                    migration_state.set_replace_targets(found)
                except Exception:  # noqa: BLE001 - on probe failure, warn-less confirm
                    migration_state.set_replace_targets(frozenset())
            _open_confirm_dialog_now()
        finally:
            _confirm_busy["value"] = False
            if btn is not None and not getattr(btn, "is_deleted", False):
                try:
                    if original_icon:
                        btn.props(f"icon={original_icon}")
                    if original_text is not None:
                        btn.set_text(original_text)
                    btn.enable()
                except Exception:  # noqa: BLE001
                    pass

    # The watermark, object browser, prerequisites, and buttons are STATIC: they
    # are rendered once per full render and must NOT be rebuilt by the 0.5s poll,
    # or user-expanded sections (snapshot row counts, prerequisite categories)
    # would keep collapsing. Only this live region -- the per-table progress,
    # caption, completeness verdict, and error-log summary -- is refreshed on each
    # poll tick; the region re-arms its own one-shot timer while the job runs.
    @ui.refreshable
    def _live_detail() -> None:
        current = _current_job(job_manager, migration_state.job_id)
        if current is None:
            return
        running = current.status in ("PENDING", "RUNNING")
        if running:
            ui.label(full_load_progress_caption(current)).classes(
                "text-sm text-gray-500"
            )
        summary = migration_state.error_log.summary(current.job_id)
        rows = build_full_load_table_rows(
            current,
            summary,
            migration_state.error_log.latest_messages(current.job_id),
        )
        _render_full_load_progress(
            ui,
            current,
            rows,
            reload_table=reload_table,
            accept_quarantine_and_continue=accept_quarantine_and_continue,
            quarantine_only=_incomplete_is_quarantine_only(
                current, migration_state.error_log
            ),
        )
        # The completeness baseline (expected_rows) comes from the watermark's
        # per-table counts, which are scan-free information_schema ESTIMATES
        # (row_counts_approximate) -- not exact. So a "row-count mismatch" against
        # them is expected noise, not a failure, and must not read as a red alert.
        approximate = (
            getattr(current.watermark, "row_counts_approximate", False)
            if current.watermark is not None
            else False
        )
        _render_completeness_banner(
            ui, full_load_completeness(rows), approximate=approximate
        )
        _render_error_log(ui, migration_state, current)
        if running:
            # Re-arm a single-shot poll: it fires once, refreshes only this live
            # region (not the whole page), and the refresh renders a fresh timer.
            # once=True avoids the "parent slot deleted" crash a repeating timer
            # hits when its slot is rebuilt.
            ui.timer(_POLL_INTERVAL_SECONDS, _poll_live, once=True)

    def _poll_live() -> None:
        """Poll the job: refresh only the live region while running; on a terminal
        state, finalize the step status and do one full refresh (to flip buttons
        and reveal retry)."""
        try:
            current = job_manager.get_status(migration_state.job_id)
        except JobNotFoundError:
            return
        mapped = job_status_to_step_status(current.status)
        if mapped is None:
            _live_detail.refresh()
            return
        if mapped is StepStatus.FAILED:
            migration_state.set_error(
                job_manager.get_error(migration_state.job_id)
                or "Data migration failed."
            )
        session.set_workflow(  # type: ignore[attr-defined]
            with_status(
                session.workflow,  # type: ignore[attr-defined]
                WorkflowStep.FULL_LOAD,
                mapped,
            )
        )
        # Combined mode: do NOT auto-advance to CDC when the Full Load completes.
        # The operator should be able to review the finished snapshot's stats
        # (per-table rows, completeness, watermark) before moving on, so the view
        # stays on the Full Load step; the gapless hand-off to CDC happens when
        # they click "Continue to CDC". refresh() flips the buttons (Re-run /
        # enable Continue) without changing the active sub-step.
        refresh()

    if status is StepStatus.IN_PROGRESS:
        stopping = False
        try:
            stopping = job_manager.is_cancel_requested(migration_state.job_id)
        except Exception:  # noqa: BLE001 - best-effort UI hint
            stopping = False
        with ui.row().classes("items-center gap-2"):
            ui.spinner(size="sm")
            ui.label(
                "Stopping… finishing the current batch."
                if stopping
                else "Full Load in progress…"
            ).classes("text-sm text-gray-500")
            stop_btn = ui.button(
                "Stop Full Load", on_click=stop_full_load, icon="stop"
            ).props("color=negative outline")
            if stopping:
                stop_btn.disable()
                stop_btn.tooltip("Stop already requested; finishing the current batch.")
            else:
                stop_btn.tooltip(
                    "Stop after the current batch. Loaded tables are kept; the "
                    "rest become retryable."
                )
    else:
        # After a finished run that left failed tables, the recovery actions
        # (Retry failed tables = recommended, Re-run all tables = secondary) are
        # grouped together in the terminal section below, next to the failure
        # reason -- so we do NOT also draw a standalone primary "Re-run" button
        # here, which would compete with the recommended Retry and read as the
        # main action. The standalone button is only for the pre-run Start and
        # for a clean finished run (DONE, no failures) where re-running all is the
        # natural single action.
        terminal_with_failures = (
            job is not None
            and status is not StepStatus.IN_PROGRESS
            and bool(failed_table_names(job))
        )
        if not terminal_with_failures:
            start_label = "Re-run Full Load" if job is not None else "Start Full Load"
            start_btn = ui.button(
                start_label, on_click=_open_full_load_confirm, icon="play_arrow"
            ).props("color=primary")
            if not selected_names:
                start_btn.disable()
                start_btn.tooltip(
                    "Select at least one table in the object browser above."
                )
            elif guard_reason:
                start_btn.disable()
                start_btn.tooltip(guard_reason)
            elif cdc_live:
                # Not blocked, but warn up-front (the dialog reiterates the risk).
                start_btn.tooltip(
                    "CDC is streaming -- re-running can break the live pipeline. "
                    "Stop CDC first."
                )
                inline_hint(
                    ui,
                    "⚠ CDC is live. Re-running Full Load can collide with the "
                    "running stream -- stop CDC first (CDC step → Stop CDC).",
                    tone="warning",
                )

    if job is not None:
        ui.separator()
        # Static: captured once at run start, so its snapshot-row-counts expansion
        # is not collapsed by the poll.
        _render_watermark(ui, job)
        # Live: per-table progress, caption, completeness, error log.
        _live_detail()

        # Terminal-only affordances (shown after the job finishes, on the full
        # refresh the poll triggers): the job-level failure reason and retry.
        if status is not StepStatus.IN_PROGRESS:
            job_error = None
            try:
                job_error = job_manager.get_error(job.job_id)
            except JobNotFoundError:
                job_error = None
            if job_error:
                render_notice(
                    ui,
                    tone="error",
                    header="Load failed",
                    body=job_error,
                )

            failed = failed_table_names(job)
            if failed:
                # Recovery needs a live source AND target. After an app restart the
                # connections (and the in-memory source password) are NOT restored
                # (Property 7), so both recovery actions would silently no-op. Tell
                # the user exactly why and disable the buttons instead of letting a
                # click do nothing.
                missing: list[str] = []
                if not session.has_source():
                    missing.append("source")
                if not session.has_target():
                    missing.append("target")
                connections_missing = bool(missing)
                if connections_missing:
                    render_notice(
                        ui,
                        tone="warning",
                        header="Reconnect to retry",
                        body=(
                            f"The {' and '.join(missing)} connection is not active "
                            "(connections and credentials are not restored after a "
                            "restart for security). Re-open the Connect step, test "
                            f"the {' and '.join(missing)} connection, then return "
                            "here to retry the failed tables — the succeeded tables "
                            "are kept."
                        ),
                    )
                # Two recovery actions, grouped and ranked so the recommended one
                # is unambiguous: "Retry failed tables" (resume only the unfinished
                # work, keep succeeded tables) is the primary action on the right;
                # "Re-run all tables" (fresh snapshot over everything) is the
                # secondary/destructive-leaning option on the left. Putting them in
                # one row -- instead of a lone primary "Re-run" up top competing
                # with the retry down here -- makes the choice clear at a glance.
                done_count = sum(1 for c in job.chunks if c.status == "DONE")
                reconnect_tip = (
                    f"Reconnect the {' and '.join(missing)} connection first "
                    "(Connect step), then retry."
                )
                with ui.row().classes("items-center gap-2 w-full"):
                    rerun_btn = ui.button(
                        "Re-run all tables",
                        on_click=_open_full_load_confirm,
                        icon="restart_alt",
                    ).props("color=primary outline")
                    if connections_missing:
                        rerun_btn.disable()
                        rerun_btn.tooltip(reconnect_tip)
                    elif not selected_names:
                        rerun_btn.disable()
                        rerun_btn.tooltip(
                            "Select at least one table in the object browser above."
                        )
                    elif guard_reason:
                        rerun_btn.disable()
                        rerun_btn.tooltip(guard_reason)
                    else:
                        rerun_btn.tooltip(
                            "Re-runs ALL selected tables from a fresh snapshot (new "
                            "watermark). Use 'Retry failed tables' to resume only "
                            "the unfinished ones instead."
                        )
                    # Spacer pushes the recommended action to the right edge.
                    ui.space()
                    retry_btn = ui.button(
                        f"Retry failed tables ({len(failed)})",
                        on_click=retry_failed_load,
                        icon="replay",
                    ).props("color=primary")
                    if connections_missing:
                        retry_btn.disable()
                        retry_btn.tooltip(reconnect_tip)
                    else:
                        retry_btn.tooltip(
                            f"Recommended: re-run Full Load for only the "
                            f"{len(failed)} failed table(s); the {done_count} "
                            "succeeded table(s) are kept as-is (no re-load, no new "
                            "snapshot)."
                        )


# Quasar color names for each per-table Full Load state badge.
_LOAD_STATE_COLORS: dict[str, str] = {
    "PENDING": "grey",
    "IN_PROGRESS": "primary",
    "DONE": "positive",
    "FAILED": "negative",
}


def _log_cdc_event(
    action: str,
    *,
    detail: "Optional[str]" = None,
    status: ActivityStatus = ActivityStatus.STARTED,
) -> None:
    """Append a discrete CDC lifecycle milestone to the activity log.

    Records control-plane actions (deploy / start / stop / teardown) and connector
    state transitions as one-line events -- the audit trail of WHAT happened to the
    CDC pipeline and WHEN. Continuous progress (replication lag / applied counts)
    stays out of the log (it would flood the rotated file); it lives in the live
    monitoring panel.
    """
    log_activity(ActivityCategory.CDC, action, status=status, detail=detail)


def _log_cdc_connector_transitions(migration_state, job_manager) -> None:
    """Log connector RUNNING/FAILED state transitions to the activity log.

    Discrete + de-duplicated: only a CHANGE from the last-seen state is logged
    (RUNNING -> SUCCESS event, FAILED -> FAILURE event), so the rotated log gets a
    milestone per transition, never a per-poll stream. Other intermediate states
    (PROVISIONING, etc.) are tracked but not logged. Advisory: never raises.
    """
    try:
        view = _cdc_status_view(migration_state, job_manager)
        states = dict(getattr(view, "connector_states", None) or {})
    except Exception:  # noqa: BLE001 - advisory; never break discovery
        return
    if not states:
        return
    last = getattr(migration_state, "_last_logged_connector_states", {}) or {}
    for name, state in states.items():
        norm = str(state).upper()
        if last.get(name) == norm:
            continue
        if norm in ("RUNNING", "FAILED"):
            _log_cdc_event(
                f"connector {name} {norm.lower()}",
                status=(
                    ActivityStatus.SUCCESS
                    if norm == "RUNNING"
                    else ActivityStatus.FAILURE
                ),
            )
    migration_state._last_logged_connector_states = {
        n: str(s).upper() for n, s in states.items()
    }


def _quarantined_row_count(job: "MigrationJob", error_log: "ErrorLogStore") -> int:
    """Count permanently-quarantined rows recorded for ``job`` in the error log."""
    if job is None:
        return 0
    return sum(
        1
        for record in error_log.records(job.job_id)
        if str(getattr(record, "message", "")).startswith("quarantined row pk[")
    )


def _incomplete_is_quarantine_only(
    job: "MigrationJob", error_log: "ErrorLogStore"
) -> bool:
    """True when a run's only incompleteness is quarantined rows (overridable).

    A quarantine table's chunk completes DONE (its loadable rows are committed),
    so there are NO failed chunks; the run is incomplete only because rows were
    permanently dropped. That is the one case the accept-and-continue override may
    unblock -- a retryable table failure (a FAILED chunk) must still block.
    """
    if job is None:
        return False
    if failed_table_names(job):  # a retryable real failure is present
        return False
    return _quarantined_row_count(job, error_log) > 0


def _format_progress_cell(row: "FullLoadTableRow") -> str:
    """Render a per-table progress cell (percent, ``loading...``, or a dash)."""
    pct = row.progress_pct
    if pct is not None:
        return f"{pct:.0f}%"
    if row.state == "IN_PROGRESS":
        return "loading..."
    return "—"


def _format_rows_on_target_cell(row: "FullLoadTableRow") -> str:
    """Render the "Rows on target" cell: total present, with a new/existing split.

    Shows the total rows now on the target for this table (``rows_present`` =
    newly inserted + already present). When some rows already existed and were
    skipped by the idempotent re-load, the total is followed by a plain-language
    breakdown -- ``"1,067,310  ·  455,319 new + 611,991 already there"`` -- so the
    operator understands a re-run that mostly skips is not "stuck at zero" but is
    re-using rows a prior run loaded. With no skips it is just the loaded count.
    Thousands separators make large tables readable.
    """
    if row.rows_skipped:
        return (
            f"{row.rows_present:,}  ·  {row.rows_loaded:,} new + "
            f"{row.rows_skipped:,} already there"
        )
    return f"{row.rows_loaded:,}"


def _format_complete_cell(row: "FullLoadTableRow") -> str:
    """Render the per-table completeness cell comparing loaded vs source rows."""
    complete = row.complete
    if complete is True:
        return "✓ match"
    if complete is False:
        return f"✗ {row.rows_loaded}/{row.expected_rows}"
    return ""


# Friendly labels for each load state, used by the status-distribution chips.
_LOAD_STATE_LABELS: dict[str, str] = {
    "DONE": "Done",
    "IN_PROGRESS": "In progress",
    "FAILED": "Failed",
    "PENDING": "Pending",
}


def _render_table_state_summary(ui, rows: "Sequence[FullLoadTableRow]") -> None:
    """Render colored chips with the table count in each load state (O(tables))."""
    counts = summarize_table_states(rows)
    with ui.row().classes("items-center gap-2 flex-wrap"):
        for state in _LOAD_STATE_ORDER:
            count = counts.get(state, 0)
            if count == 0:
                continue
            color = _LOAD_STATE_COLORS.get(state, "grey")
            ui.badge(f"{_LOAD_STATE_LABELS[state]}: {count}").props(
                f"color={color}"
            ).classes("text-sm q-px-sm q-py-xs")


def _render_full_load_progress(
    ui, job: MigrationJob, rows: "Sequence[FullLoadTableRow]",
    *,
    reload_table=None,
    accept_quarantine_and_continue=None,
    quarantine_only: bool = False,
) -> None:
    """Render the overall progress, a status distribution, and a live per-table
    table with colored status badges and per-row progress bars."""
    total = len(rows)
    settled = sum(1 for row in rows if row.state in ("DONE", "FAILED"))
    pct = job.progress_pct or 0.0
    if total:
        ui.linear_progress(value=pct / 100.0, show_value=False).props(
            "instant-feedback"
        ).classes("w-full")
    ui.label(
        f"Overall — {pct:.1f}% complete, {settled}/{total} tables settled"
    ).classes("text-sm text-gray-600")

    # At-a-glance status distribution (scales to any table count).
    _render_table_state_summary(ui, rows)

    columns = [
        {"name": "table", "label": "Table", "field": "table", "align": "left"},
        {"name": "state", "label": "Status", "field": "state", "align": "left"},
        {
            "name": "rows_loaded",
            "label": "Rows on target",
            "field": "rows_loaded",
            "align": "left",
        },
        {"name": "expected", "label": "Source rows", "field": "expected"},
        {"name": "progress", "label": "Progress", "field": "progress", "align": "left"},
        {
            "name": "time",
            "label": "Time (ETA / total)",
            "field": "time",
            "align": "left",
        },
        {"name": "attempts", "label": "Attempts", "field": "attempts"},
        {"name": "errors", "label": "Errors", "field": "errors"},
        {"name": "complete", "label": "Complete", "field": "complete", "align": "left"},
    ]
    now = datetime.now(timezone.utc)
    table_rows = [
        {
            "table": row.table,
            "state": row.state,
            "state_label": _LOAD_STATE_LABELS.get(row.state, row.state),
            "state_color": _LOAD_STATE_COLORS.get(row.state, "grey"),
            "rows_loaded": _format_rows_on_target_cell(row),
            "expected": row.expected_rows if row.expected_rows is not None else "—",
            "progress": _format_progress_cell(row),
            "progress_value": (
                None if row.progress_pct is None else round(row.progress_pct / 100.0, 4)
            ),
            "time": format_table_timing(row, now),
            "attempts": row.attempts,
            "errors": row.errors,
            "complete": _format_complete_cell(row),
        }
        for row in rows
    ]
    # `wrap-cells` lets long cells (headers like "Time (ETA / total)") wrap to a
    # second line instead of forcing the row wider than the card, and `dense`
    # tightens padding -- together the 9 columns fit the card width so the table
    # never shows a bottom horizontal scrollbar.
    table = ui.table(
        columns=columns,
        rows=table_rows,
        row_key="table",
        pagination=10,
    ).props("wrap-cells dense").classes("w-full")
    # Colored status badge per row (visualizes each table's load state).
    table.add_slot(
        "body-cell-state",
        r"""
        <q-td :props="props">
          <q-badge :color="props.row.state_color" :label="props.row.state_label" />
        </q-td>
        """,
    )
    # Per-row progress bar (rows loaded vs source snapshot count) with a label;
    # falls back to the text cell ("loading..."/"—") when the percent is unknown.
    table.add_slot(
        "body-cell-progress",
        r"""
        <q-td :props="props">
          <div v-if="props.row.progress_value !== null"
               class="row items-center no-wrap" style="gap:8px; min-width:140px">
            <q-linear-progress :value="props.row.progress_value" size="10px"
                 color="primary" track-color="grey-3" rounded style="flex:1" />
            <span class="text-caption">{{ props.value }}</span>
          </div>
          <span v-else>{{ props.value }}</span>
        </q-td>
        """,
    )

    # Surface the failure cause inline: list each failed table with its latest
    # error message so the user can diagnose without downloading the log. Always
    # shown (not collapsible) so the live poll re-render never hides it.
    failures = [row for row in rows if row.error_message]
    quar_prefix = "quarantined row pk["
    real_failures = [
        r for r in failures if not str(r.error_message).startswith(quar_prefix)
    ]
    quarantined = [
        r for r in failures if str(r.error_message).startswith(quar_prefix)
    ]
    terminal = job.status in ("DONE", "FAILED", "CANCELLED")

    def _reload_btn(table_name: str) -> None:
        # Per-table Reload: only offered once the job has settled, so it can't
        # collide with an in-flight run.
        if reload_table is not None and terminal:
            ui.button(
                "Reload", on_click=lambda n=table_name: reload_table(n)
            ).props("flat dense no-caps size=sm color=primary icon=replay").tooltip(
                "Re-run Full Load for just this table (e.g. after fixing the "
                "source value), keeping the others as-is."
            )

    # Real, retryable table failures (red) -- distinct from quarantined rows.
    if real_failures:
        with ui.column().classes("w-full gap-1"):
            render_notice(
                ui,
                tone="error",
                header=f"Failure details ({len(real_failures)})",
            )
            for row in real_failures:
                with ui.row().classes("items-center gap-2 w-full no-wrap"):
                    ui.badge(row.table).props("color=negative outline")
                    inline_hint(
                        ui,
                        row.error_message,
                        tone="error",
                        classes="text-xs break-all",
                    )
                    _reload_btn(row.table)

    # Quarantined rows (amber): the table loaded -- these rows were permanently
    # dropped (e.g. a value over DSQL's ~1 MiB per-value limit), NOT a failure.
    if quarantined:
        with ui.column().classes("w-full gap-1"):
            render_notice(
                ui,
                tone="warning",
                header=(
                    f"Quarantined rows ({len(quarantined)}) — the table loaded; "
                    "these rows were permanently dropped (e.g. a value over DSQL's "
                    "~1 MiB per-value limit)"
                ),
            )
            for row in quarantined:
                with ui.row().classes("items-center gap-2 w-full no-wrap"):
                    ui.badge(f"{row.table} · Done — quarantined").props(
                        "color=warning outline"
                    )
                    inline_hint(
                        ui,
                        row.error_message,
                        tone="warning",
                        classes="text-xs break-all",
                    )
                    _reload_btn(row.table)
            # When the ONLY incompleteness is quarantine, offer to accept the gap
            # and unblock CDC (Validation still reports it). Real failures suppress
            # this -- they must be retried/reloaded first.
            if (
                quarantine_only
                and terminal
                and accept_quarantine_and_continue is not None
            ):
                with ui.row().classes("items-center gap-2 w-full"):
                    ui.button(
                        "Accept quarantined rows & continue",
                        on_click=accept_quarantine_and_continue,
                        icon="check",
                    ).props("unelevated no-caps color=warning")
                    ui.label(
                        "Fix the source value(s) and Reload to load them, or accept "
                        "the gap to proceed to CDC (the gap is reported in Validation)."
                    ).classes("text-xs text-gray-500")


def _render_completeness_banner(
    ui, completeness: "FullLoadCompleteness", *, approximate: bool = False
) -> None:
    """Render the source-vs-loaded completeness verdict once the load settles.

    ``approximate`` says the per-table baseline (``expected_rows``) came from the
    watermark's scan-free ``information_schema`` ESTIMATES rather than exact
    counts. When the only discrepancies are row-count gaps (no actual FAILED
    table) and the baseline was approximate, a "mismatch" is expected noise --
    the estimate is off, or rows were inserted after the snapshot -- so the
    verdict is shown as a calm INFO note pointing at Validation for an exact
    check, NOT a red "finished with issues" alert. A genuinely FAILED table is
    always surfaced as a warning regardless of the baseline's precision.
    """
    if completeness.total == 0 or completeness.settled != completeness.total:
        return  # still running -- the verdict is only meaningful when settled
    if completeness.all_complete:
        _render_notice(
            ui,
            tone="success",
            header="Full Load complete",
            body=(
                f"All {completeness.total} tables loaded every source row "
                "(loaded rows match the source snapshot count)."
            ),
        )
        return

    # An estimate-only discrepancy (no real failure, approximate baseline) is an
    # informational note, not a failure: the counts simply differ from the
    # scan-free estimate. Surface it calmly (AWS-style info box) and defer to
    # Validation for the exact truth.
    estimate_only = approximate and completeness.failed == 0
    if estimate_only:
        notes: list[str] = []
        if completeness.mismatched:
            notes.append(
                f"{len(completeness.mismatched)} table(s) differ from the estimate "
                f"({', '.join(completeness.mismatched)})"
            )
        if completeness.unknown:
            notes.append(
                f"{completeness.unknown} without an estimate to compare"
            )
        _render_notice(
            ui,
            tone="info",
            header="Full Load finished — counts differ from the pre-load estimate",
            body=(
                "The pre-load row counts are approximate (scan-free "
                "information_schema figures, not exact) and drift when rows change "
                "during the load: "
                + "; ".join(notes)
                + ". This is expected — run Validation (Step 4) for an exact "
                "row-count/checksum check to confirm."
            ),
        )
        return

    problems: list[str] = []
    if completeness.failed:
        problems.append(f"{completeness.failed} failed")
    if completeness.mismatched:
        problems.append(
            f"{len(completeness.mismatched)} row-count mismatch "
            f"({', '.join(completeness.mismatched)})"
        )
    if completeness.unknown:
        problems.append(
            f"{completeness.unknown} without a source count to compare"
        )
    _render_notice(
        ui,
        tone="warning",
        header="Full Load finished with issues",
        body=(
            "; ".join(problems)
            + ". Retry the failed tables, or run Validation (Step 4) for a full "
            "row-count/checksum check."
        ),
    )


# Checks that only CDC requires (binlog/GTID/MSK). Everything else is common to
# Full Load and CDC. Used to tag each result row with the phase that needs it so
# the combined "Full load + CDC" panel shows it covers both (the CDC run is a
# superset of the Full Load checks -- see core.prerequisites).
_CDC_ONLY_CHECK_IDS = frozenset(
    {
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.GTID_MODE,
        PrerequisiteCheckId.MSK_AVAILABLE,
        PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
    }
)


def prereq_phase_tag(check_id: PrerequisiteCheckId, *, combined: bool) -> str:
    """Return the phase tag for a prerequisite check row.

    In the combined "Full load + CDC" panel, a common check is tagged
    ``"Full Load + CDC"`` (it gates both phases) and a CDC-only check ``"CDC"``,
    so the single check run visibly covers both phases. Off the combined panel
    the tag is empty (the panel title already names the single phase).
    """
    if not combined:
        return ""
    return "CDC" if check_id in _CDC_ONLY_CHECK_IDS else "Full Load + CDC"


@dataclass(frozen=True)
class PrereqPhaseVerdict:
    """Per-phase pass/block verdict within the combined Full-load-+-CDC report.

    ``phase`` is "Full Load" or "CDC"; ``can_proceed`` is True when no *required*
    check that gates THAT phase failed. ``blocking_titles`` lists the failing
    required checks for the phase (for a precise message).
    """

    phase: str
    can_proceed: bool
    blocking_titles: tuple[str, ...]


def prereq_phase_verdicts(report: object) -> list[PrereqPhaseVerdict]:
    """Split a combined report's gating verdict into a Full Load and a CDC verdict.

    The combined panel runs CDC-mode checks (a superset of Full Load's), so a
    single "Blocked" headline can't tell the operator WHICH phase is blocked. This
    derives two verdicts from the same results: the **Full Load** phase is gated
    only by the common (non-CDC-only) checks, while the **CDC** phase is gated by
    every check. So a binlog/replication failure correctly reads as "Full Load can
    proceed, CDC is blocked" instead of implying the Full Load itself is blocked.
    Pure: no UI, no AWS. ``report`` is duck-typed (needs ``.results``).
    """
    results = list(getattr(report, "results", []) or [])

    def _blocking(for_full_load: bool) -> tuple[str, ...]:
        titles: list[str] = []
        for r in results:
            if not getattr(r, "required", False):
                continue
            if r.status is not PrerequisiteStatus.FAIL:
                continue
            # The Full Load phase ignores CDC-only checks; CDC considers all.
            if for_full_load and r.check_id in _CDC_ONLY_CHECK_IDS:
                continue
            titles.append(r.title)
        return tuple(titles)

    full_blocking = _blocking(for_full_load=True)
    cdc_blocking = _blocking(for_full_load=False)
    return [
        PrereqPhaseVerdict("Full Load", not full_blocking, full_blocking),
        PrereqPhaseVerdict("CDC", not cdc_blocking, cdc_blocking),
    ]


def _render_prerequisites_panel(
    ui,
    migration_state,
    run_checks,
    mode: MigrationMode = MigrationMode.FULL_LOAD,
    *,
    combined: bool = False,
) -> None:
    """Render a mode's prerequisite check button and result panel (Req 5.10).

    Shared by Full Load, CDC, and the combined "Full load + CDC" type so every
    step presents an identical panel (same intro line, outline button, spinner,
    grouped results table); only the wording and the ``mode`` differ. A failed
    required check blocks that mode.

    ``combined`` is set for the Full-load-+-CDC type: the checks still run in CDC
    ``mode`` (a superset of the Full Load checks), but the wording says "Full
    Load and CDC" and each result row is tagged with the phase that needs it, so
    the single run visibly covers both phases (no separate Full Load panel).
    """
    is_cdc = mode is MigrationMode.CDC
    if combined:
        intro = (
            "Run read-only checks before migrating. These cover both phases -- "
            "the Full Load and the CDC stream; a failed required check blocks the "
            "migration."
        )
        button_label = "Check Full load + CDC prerequisites"
        running_text = "Running Full load + CDC prerequisite checks… (read-only)"
    elif is_cdc:
        intro = (
            "Run read-only checks before streaming. A failed required check "
            "blocks CDC."
        )
        button_label = "Check CDC prerequisites"
        running_text = "Running CDC prerequisite checks… (read-only)"
    else:
        intro = (
            "Run read-only checks before loading. A failed required check "
            "blocks the Full Load."
        )
        button_label = "Check Full Load prerequisites"
        running_text = "Running Full Load prerequisite checks… (read-only)"
    ui.label(intro).classes("text-sm text-gray-500")
    running = migration_state.is_prereq_running(mode)
    with ui.row().classes("items-center gap-2 flex-wrap"):
        # Keep the label visible while running (just disabled) -- Quasar's
        # `loading` prop would swap the label for a spinner, leaving an empty
        # outlined button with a lone spinning ring. The progress spinner lives
        # in the status line below instead, so there is exactly one spinner.
        check_btn = ui.button(
            "Checking…" if running else button_label,
            on_click=lambda: run_checks(mode),
        ).props("outline")
        if running:
            check_btn.props("disable")
    # Immediate feedback directly below the button while a check runs, so the
    # click is acknowledged at once instead of seeming unresponsive.
    if running:
        with ui.row().classes("items-center gap-2"):
            ui.spinner(size="sm")
            ui.label(running_text).classes("text-sm text-gray-600")
    report = migration_state.get_prereq_report(mode)
    if report is not None:
        _render_prereq_results(ui, mode, report, combined=combined)


# Quasar color names for each prerequisite check status badge. INFO is a calm
# blue (an optional recommendation / expected state), distinct from the amber
# WARN (something is off).
_PREREQ_STATUS_COLORS: dict[PrerequisiteStatus, str] = {
    PrerequisiteStatus.PASS: "positive",
    PrerequisiteStatus.FAIL: "negative",
    PrerequisiteStatus.WARN: "warning",
    PrerequisiteStatus.INFO: "info",
    PrerequisiteStatus.SKIP: "grey",
}

# Material icon name for each prerequisite status, shown on the category header.
_PREREQ_STATUS_ICONS: dict[PrerequisiteStatus, str] = {
    PrerequisiteStatus.PASS: "check_circle",
    PrerequisiteStatus.FAIL: "error",
    PrerequisiteStatus.WARN: "warning",
    PrerequisiteStatus.INFO: "info",
    PrerequisiteStatus.SKIP: "remove_circle_outline",
}


def _render_prereq_results(
    ui, mode: MigrationMode, report: PrerequisiteReport, *, combined: bool = False
) -> None:
    """Render one mode's prerequisite results grouped by category (Req 5.10).

    Shows the gating verdict, then one collapsible section per category
    (Connectivity / Source Configuration / Schema & Tables / Streaming). A
    section that needs attention (a blocking failure or a warning) starts
    expanded; passing or not-applicable sections start collapsed to keep the
    panel scannable. When ``combined``, the verdict label names both phases and
    each row carries a phase tag.
    """
    if combined:
        label = "Full load + CDC"
    elif mode == MigrationMode.FULL_LOAD:
        label = "Full Load"
    else:
        label = "CDC"
    verdict = "Can proceed" if report.can_proceed else "Blocked"
    color = "positive" if report.can_proceed else "negative"
    with ui.row().classes("items-center gap-2"):
        ui.label(f"{label} prerequisites").classes("text-sm font-semibold")
        ui.badge(verdict).props(f"color={color}")
    # In the combined panel, also show a PER-PHASE verdict so a CDC-only failure
    # (e.g. binlog not ROW) reads as "Full Load can proceed, CDC blocked" instead
    # of a single ambiguous "Blocked" that looks like the Full Load itself failed.
    if combined:
        _render_prereq_phase_verdicts(ui, report)
    with ui.column().classes("w-full gap-1"):
        for group in group_prereq_results(report.results):
            _render_prereq_category(ui, group, combined=combined)


def _render_prereq_phase_verdicts(ui, report) -> None:
    """Render the Full Load / CDC per-phase verdict badges + a clarifying note."""
    verdicts = prereq_phase_verdicts(report)
    with ui.row().classes("items-center gap-3 flex-wrap"):
        for v in verdicts:
            color = "positive" if v.can_proceed else "negative"
            text = f"{v.phase}: {'Can proceed' if v.can_proceed else 'Blocked'}"
            ui.badge(text).props(f"color={color} outline")
    full = next((v for v in verdicts if v.phase == "Full Load"), None)
    cdc = next((v for v in verdicts if v.phase == "CDC"), None)
    # When only the CDC phase is blocked, spell out that the Full Load can still
    # run -- this is the common "binlog/replication not ready" case.
    if full is not None and cdc is not None and full.can_proceed and not cdc.can_proceed:
        render_notice(
            ui,
            tone="warning",
            header="CDC stream blocked",
            body="The Full Load can run now; only the CDC stream is blocked "
            f"({', '.join(cdc.blocking_titles)}). Fix these to enable CDC, or "
            "run Full load only.",
        )


def _render_prereq_category(
    ui, group: PrereqCategoryGroup, *, combined: bool = False
) -> None:
    """Render one category as a collapsible section with a status header."""
    color = _PREREQ_STATUS_COLORS[group.status]
    icon = _PREREQ_STATUS_ICONS[group.status]
    # Draw attention to sections that need action; collapse the rest.
    expanded = group.status in (PrerequisiteStatus.FAIL, PrerequisiteStatus.WARN)
    with ui.expansion(value=expanded).classes("w-full").props(
        "expand-separator"
    ) as exp:
        with exp.add_slot("header"):
            with ui.row().classes("items-center gap-2 w-full no-wrap"):
                ui.icon(icon).props(f"color={color}")
                ui.label(group.category.value).classes("text-sm font-semibold")
                ui.badge(group.summary).props(f"color={color} outline")
        _render_prereq_table(ui, group.results, combined=combined)
        # NOTE: the cdc-stack deploy guide is intentionally NOT shown here. In this
        # tool's model MSK/MSK Connect are not prerequisites -- they are created
        # when the customer deploys the cdc-stack AFTER the CDC step produces the
        # connector config. The deploy guide therefore lives in the CDC step's
        # "Start CDC" section, not in prerequisites (where MSK is only an
        # informational INFO -- expected, no action needed -- not a blocking FAIL).




def _render_prereq_table(
    ui, results: Sequence[PrerequisiteResult], *, combined: bool = False
) -> None:
    """Render one category's checks as a compact results table.

    In the combined "Full load + CDC" panel an extra "Phase" column tags each
    check with the phase that needs it (Full Load + CDC vs CDC), so the single
    run visibly covers both phases.
    """
    columns = [
        {"name": "check", "label": "Check", "field": "check", "align": "left"},
        {"name": "status", "label": "Status", "field": "status"},
        {
            "name": "detail",
            "label": "Detail / remediation",
            "field": "detail",
            "align": "left",
        },
    ]
    if combined:
        # Insert the Phase tag column right after the check name.
        columns.insert(
            1, {"name": "phase", "label": "Phase", "field": "phase", "align": "left"}
        )
    rows = [
        {
            "check": result.title
            + (f" ({result.target})" if result.target else ""),
            "status": result.status.value,
            "detail": result.remediation or result.detail,
            **(
                {"phase": prereq_phase_tag(result.check_id, combined=True)}
                if combined
                else {}
            ),
        }
        for result in results
    ]
    ui.table(columns=columns, rows=rows, row_key="check").classes("w-full")


def _render_error_log(ui, migration_state, job: MigrationJob) -> None:
    """Render the data-error summary and a download button when errors exist.

    The summary count equals the rows in the downloadable log (Property 15); the
    button is hidden when there are no errors.
    """
    job_id = job.job_id
    summary = migration_state.error_log.summary(job_id)
    ui.label("Data errors").classes("text-sm font-semibold")
    ui.label(format_error_summary(summary)).classes("text-sm text-gray-600")
    if summary.total_errors > 0:
        def _download_log() -> None:
            try:
                payload = migration_state.error_log.render_log(job_id)
                ui.download.content(  # type: ignore[attr-defined]
                    payload, f"error_log_{job_id}.ndjson", "application/x-ndjson"
                )
            except Exception as exc:  # noqa: BLE001 - surface instead of silent
                _LOGGER.exception("Failed to render/download error log")
                ui.notify(  # type: ignore[attr-defined]
                    f"Could not generate the error log: {exc}", type="negative"
                )

        ui.button(
            "Download error log (NDJSON)", on_click=_download_log
        ).props("outline")










































# -- Infrastructure input form (BYO-VPC) ------------------------------------







# AWS Console (Cloudscape) "Alert" notice: the tone palette and renderer live in
# the shared design module (single source of truth so every page reads the same).
# These module-level aliases preserve the existing in-file call sites/imports.
_NOTICE_STYLE = NOTICE_STYLE
_render_notice = render_notice




































# Quasar badge color per Full Load chunk state, for the per-table status table.
_MIGRATION_FL_BADGE: dict[str, str] = {
    "DONE": "positive",
    "IN_PROGRESS": "primary",
    "FAILED": "negative",
    "PENDING": "grey",
    "": "grey",
}




























__all__ = [
    "DataMigrationInputs",
    "DataMigrator",
    "MigratorFactory",
    "run_data_migration",
    "run_full_load",
    "job_status_to_step_status",
    "reconcile_full_load_step",
    "MigrationProgress",
    "summarize_progress",
    "build_full_load_status_view",
    "build_full_load_table_rows",
    "FullLoadTableRow",
    "failed_table_names",
    "full_load_completeness",
    "FullLoadCompleteness",
    "build_migration_table_status",
    "MigrationTableStatus",
    "summarize_table_states",
    "run_full_load_retry",
    "prerequisite_block_reason",
    "group_prereq_results",
    "PrereqCategory",
    "PrereqCategoryGroup",
    "format_error_summary",
    "full_load_run_guard_reason",
    "full_load_progress_caption",
    "generated_table_names",
    "target_existing_table_names",
    "migratable_table_names",
    "resolve_active_substep",
    "MigrationType",
    "prereq_mode_for_type",
    "substeps_for_type",
    "resolve_active_substep_for_type",
    "prerequisites_section_expanded",
    "prereq_phase_tag",
    "PrereqPhaseVerdict",
    "prereq_phase_verdicts",
    "format_selected_workloads",
    "effective_migration_selection",
    "build_migration_table_tree",
    "WatermarkDisplay",
    "format_binlog_coordinate",
    "format_watermark",
    "LobExclusionCandidate",
    "lob_exclusion_candidates",
    "format_column_exclude_list",
    "DlqHealth",
    "assess_dlq_health",
    "ConnectorHealthRow",
    "connector_health_rows",
    "classify_cdc_card_phase",
    "cdc_unstable_message",
    "CdcActivitySummary",
    "cdc_activity_summary",
    "CdcHandlingFact",
    "cdc_handling_facts",
    "ImporterFactory",
    "BatchedTableMigrator",
    "default_migrator_factory",
    "DataMigrationState",
    "DataMigrationStore",
    "build_data_migration_screen",
]


# ---------------------------------------------------------------------------
# CDC data-plane UI (moved to ``_cdc_ui`` for file size; see that module's
# docstring). Imported HERE, at the bottom of the module, so that the four
# names ``_cdc_ui`` imports back from this package (``_log_cdc_event``,
# ``migration_type_lock_reason``, ``_LOGGER``, ``_render_notice``) are already
# defined above -- this ordering is what breaks the import cycle. Re-exported
# so ``dm.<name>`` and existing monkeypatch targets keep resolving unchanged.
# ---------------------------------------------------------------------------
from dsql_migrator.ui.data_migration._cdc_ui import (  # noqa: E402
    # CDC-domain constants re-exported from ``core.cdc`` via ``_cdc_ui`` so the
    # package's import surface is unchanged (e.g. ``from ...data_migration import
    # CDC_DEFAULT_STACK_NAME`` still resolves after the CDC UI move).
    CDC_DEFAULT_STACK_NAME,
    _CDC_INFRA_FIELDS,
    _CDC_POLL_INTERVAL_SECONDS,
    _CdcSourceSecret,
    _DLQ_LEVEL_TONE,
    _DLQ_RECORD_LIST_LIMIT,
    _DLQ_RECORD_PAGE_SIZE,
    _cdc_infra_prefill,
    _cdc_tables_for_config,
    _cdc_target_region,
    _diagnose_for_dialog,
    _dlq_panel_tone,
    _open_cdc_delete_dialog,
    _open_cdc_infra_dialog,
    _open_cdc_start_dialog,
    _open_cdc_stop_dialog,
    _render_cdc_cost_estimate,
    _render_cdc_decision,
    _render_cdc_delete_action,
    _render_cdc_deploy_live,
    _render_cdc_dlq_breakdown,
    _render_cdc_dlq_panel,
    _render_cdc_dlq_records,
    _render_cdc_error_download,
    _render_cdc_handling_panel,
    _render_cdc_infra_deploy_action,
    _render_cdc_infra_form,
    _render_cdc_least_privilege_note,
    _render_cdc_live_monitoring,
    _render_cdc_lob_exclusion_panel,
    _render_cdc_manual_inputs,
    _render_cdc_params_file,
    _render_cdc_partial_actions,
    _render_cdc_pipeline_health,
    _render_cdc_running_actions,
    _render_cdc_runs_on_banner,
    _render_cdc_source_config_card,
    _render_cdc_start_action,
    _render_cdc_start_button,
    _render_cdc_start_point_card,
    _render_cdc_start_summary,
    _render_cdc_step,
    _render_change_flow_status,
    _render_deploy_log,
    _render_deploy_stages,
    _render_migration_table_status,
    _resolve_cdc_source_secret,
    _sentinel_watermark,
    _start_cdc_delete,
    _start_cdc_deploy,
    _start_cdc_infra_deploy,
    _start_cdc_stop,
    cdc_deploy_card_superseded,
    cdc_deploy_connection_blocker,
    cdc_live_running_names,
    cdc_streaming_started,
    cdc_unstable_message,
    classify_cdc_card_phase,
)

