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

import inspect
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.cdc import (
    CDC_DEFAULT_DLQ_TOPIC,
    CDC_DEFAULT_STACK_NAME,
    CDC_DEFAULT_TOPIC_PREFIX,
    CDC_PLACEHOLDER_PREFIX,
    CDC_STACK_NAME_PREFIX,
    CdcPipelineOrchestrator,
    CdcResumePoint,
    build_cdc_infra_params,
    build_cdc_stack_params,
    cdc_expected_connector_names,
    cdc_stack_params_to_json,
)
from dsql_migrator.core.cdc_coords import parse_binlog_coordinate, validate_gtid
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import (
    JobHandle,
    JobManager,
    JobNotFoundError,
    is_interrupted_by_restart,
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
    TableStatusRow,
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

# How often the CDC step polls MSK Connect + the DSQL target for live status.
# Slower than the Full Load poll: these are network round-trips to AWS/DSQL, and
# connector state / replication lag change on the order of seconds, not 0.5s.
_CDC_POLL_INTERVAL_SECONDS = 5.0


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
                def _substep(name, title, *, state, render_body, first=False):
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
                    exp = ui.expansion(value=(name == active)).classes(
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
    """
    if inventory is None or not inventory.tables:
        return "Run Step 1 (Evaluation) first to introspect the source schema."
    report = state.get_prereq_report(prereq_mode)
    if report is None:
        if has_run:
            return None
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


def _render_cdc_decision(
    ui, migration_state, *, status, refresh, locked: Optional[bool] = None
) -> None:
    """Render the Migration Plan's single real decision: include CDC or not.

    The Migration Plan step's only durable effect is whether CDC streaming
    infrastructure (MSK, ~15-20 min) is provisioned early, so it asks exactly that
    -- "Include CDC?" Yes/No -- instead of the full three-way type tiles (which
    overstate the commitment: the type is freely changeable on the Data Migration
    step, and Full Load vs the CDC-only variant is decided there when tables are
    picked). Mapping: **No -> FULL_LOAD_ONLY**, **Yes -> FULL_LOAD_AND_CDC** (the
    common snapshot-then-stream default; the rarer CDC-only variant is selected on
    the Data Migration step). Internally this still writes the same
    ``migration_type`` enum, so substeps / prerequisites / the journey banner /
    session snapshots are unchanged. Locking mirrors the type selector: once a
    migration is running or CDC infrastructure is deployed, the choice is frozen
    (with the reason shown) so it cannot change out from under billable resources.
    """
    running = locked if locked is not None else (status is StepStatus.IN_PROGRESS)
    includes_cdc = migration_state.migration_type in (
        MigrationType.CDC_ONLY,
        MigrationType.FULL_LOAD_AND_CDC,
    )

    def _choose(include_cdc: bool) -> None:
        if running:
            return
        # No-op if the answer already matches: any CDC mode (FULL_LOAD_AND_CDC or
        # CDC_ONLY) already "includes CDC", so re-selecting Yes must NOT clobber a
        # CDC_ONLY choice the user made on the Data Migration step. Only flip when
        # the include-CDC answer actually changes.
        if include_cdc == includes_cdc:
            return
        new_type = (
            MigrationType.FULL_LOAD_AND_CDC
            if include_cdc
            else MigrationType.FULL_LOAD_ONLY
        )
        migration_state.set_migration_type(new_type)
        migration_state.set_active_substep(None)  # default for the new type
        refresh()

    ui.label("Include CDC (continuous replication)?").classes(  # type: ignore[attr-defined]
        "text-sm font-semibold"
    )
    lock_reason = migration_type_lock_reason(migration_state, status=status)
    if running and lock_reason:
        ui.label(lock_reason).classes(  # type: ignore[attr-defined]
            "text-xs text-amber-700 mb-1"
        )
    # Two Cloudscape-style tiles: No (Full Load only) / Yes (Full Load + CDC).
    options = (
        (
            False,
            "sync_disabled",
            "No — one-time load",
            "Full Load only: a one-shot snapshot copy. No streaming infrastructure.",
        ),
        (
            True,
            "sync",
            "Yes — keep in sync",
            "Provision CDC (MSK) so the target stays in sync after the load — for a "
            "near-zero-downtime cutover. You choose the exact pattern (Full Load + "
            "CDC, or CDC only) on the Data Migration step.",
        ),
    )
    with ui.row().classes("w-full gap-3 items-stretch no-wrap"):  # type: ignore[attr-defined]
        for value, icon, title, blurb in options:
            is_selected = value == includes_cdc
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
            tile.on("click", lambda _e=None, _v=value: _choose(_v))
            with tile:
                with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon(  # type: ignore[attr-defined]
                        "radio_button_checked"
                        if is_selected
                        else "radio_button_unchecked",
                        color="primary" if is_selected else "grey-6",
                    ).classes("text-lg")
                    ui.icon(  # type: ignore[attr-defined]
                        icon, color="primary" if is_selected else "grey-7"
                    ).classes("text-lg")
                    ui.label(title).classes("text-sm font-semibold")  # type: ignore[attr-defined]
                ui.label(blurb).classes("text-xs text-gray-600")  # type: ignore[attr-defined]
    if running:
        ui.label(  # type: ignore[attr-defined]
            "Locked once the migration has started or CDC infrastructure is deployed."
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
            {"id": f"{TABLE_PREFIX}{table.name}", "label": obj}
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
    nodes = build_migration_table_tree(inventory, migratable_order)

    def on_tick(event: object) -> None:
        value = getattr(event, "value", None) or []
        names = selected_object_names(value)
        chosen = [name for name in migratable_order if name in names]
        migration_state.set_selection(TableSelection(selected_tables=chosen))

    if not locked:
        # Bulk selection over the migratable set. Programmatic tick/untick does not
        # fire on_tick, so update the session selection directly to stay in sync.
        with ui.row().classes("items-center gap-1 w-full no-wrap"):

            def _dm_select_all() -> None:
                tree.tick()
                migration_state.set_selection(
                    TableSelection(selected_tables=list(migratable_order))
                )

            def _dm_unselect_all() -> None:
                tree.untick()
                migration_state.set_selection(TableSelection(selected_tables=[]))

            ui.button("Select all", on_click=_dm_select_all).props(
                "flat dense no-caps size=sm color=primary icon=done_all"
            )
            ui.button("Unselect all", on_click=_dm_unselect_all).props(
                "flat dense no-caps size=sm color=grey-7 icon=remove_done"
            )

    with ui.scroll_area().classes(
        "w-full bg-white rounded border border-gray-200"
    ).style("height: 280px"):
        # When locked, omit the on_tick handler so ticks can't change the
        # selection, and disable the tree so it reads as non-interactive. The
        # tick checkboxes are rendered grey (vs the primary color when active) so
        # it is visually obvious the boxes cannot be changed -- not just inert.
        tree = ui.tree(
            nodes,
            label_key="label",
            node_key="id",
            tick_strategy="leaf",
            on_tick=None if locked else on_tick,
        )
        # Grey, non-interactive checkboxes when locked; the normal primary color
        # (a live, changeable selection) otherwise.
        tree.props(f"tick-color={'grey' if locked else 'primary'}")
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

    async def _open_full_load_confirm() -> None:
        """Check which selected target tables hold data, then open the dialog.

        Runs the read-only non-empty probe off the event loop so the UI stays
        responsive, records the result (drives the destructive warning + the
        run's replace set), then opens the confirm dialog in the top-level client
        context (so the progress poll re-render cannot close it).
        """
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


def _render_cdc_stack_deploy_help(ui) -> None:
    """Render a contextual guide for provisioning the CDC infrastructure.

    The MSK cluster and MSK Connect are created by the optional ``cdc-stack``
    CloudFormation template; until it is deployed these checks fail by design
    (the control-plane app does not depend on it). This panel gives the concrete
    deploy command + key parameters so the user can stand it up.
    """
    with ui.expansion(  # type: ignore[attr-defined]
        "How to provision the CDC infrastructure (cdc-stack)", value=True
    ).classes("w-full").props("expand-separator"):
        ui.label(  # type: ignore[attr-defined]
            "MSK and MSK Connect are created by the optional cdc-stack "
            "CloudFormation template -- deploy it once with CloudFormation; "
            "the control-plane app does not provision it automatically."
        ).classes("text-sm text-gray-600")
        ui.code(  # type: ignore[attr-defined]
            "aws cloudformation validate-template \\\n"
            "  --template-body file://deploy/cdc-stack/cdc-stack.yaml\n\n"
            "aws cloudformation deploy \\\n"
            "  --template-file deploy/cdc-stack/cdc-stack.yaml \\\n"
            "  --stack-name mysql-dsql-cdc-stack \\\n"
            "  --capabilities CAPABILITY_NAMED_IAM \\\n"
            "  --parameter-overrides ...   # see the template Parameters",
            language="bash",
        ).classes("w-full text-xs")
        ui.label(  # type: ignore[attr-defined]
            "Key parameters: VPC + private subnets, the S3 location of both "
            "plugin artifacts, the DSQL cluster endpoint/ARN, the source-credential "
            "Secrets Manager ARN, and the source DB security group for egress."
        ).classes("text-xs text-gray-500")
        ui.label(  # type: ignore[attr-defined]
            "MSK Serverless does not expose its bootstrap brokers via Ref/GetAtt, "
            "so this is a two-pass deploy: pass 1 creates the cluster, then supply "
            "its bootstrap string as MskBootstrapServers on pass 2 to create the "
            "connectors. See deploy/cdc-stack/README.md."
        ).classes("text-xs text-gray-500")
        ui.label(  # type: ignore[attr-defined]
            "After it is deployed, re-run these prerequisite checks."
        ).classes("text-xs text-gray-600")

    # Teardown guidance, symmetric with deploy. The cdc-stack (MSK Serverless +
    # MSK Connect + NAT) bills hourly, so remind the operator to delete it after
    # cutover. Informational only -- the control plane never calls CloudFormation
    # (decision-change 8); collapsed by default so it is not mistaken for an action.
    with ui.expansion(  # type: ignore[attr-defined]
        "How to tear down the CDC infrastructure (stop the hourly bill)",
        value=False,
    ).classes("w-full").props("expand-separator"):
        inline_hint(  # type: ignore[attr-defined]
            ui,
            "The cdc-stack (MSK Serverless + MSK Connect + NAT gateway) bills by "
            "the hour while deployed. Delete it once you have cut over and no "
            "longer need CDC streaming.",
            tone="neutral",
            classes="text-sm",
        )
        ui.code(  # type: ignore[attr-defined]
            "aws cloudformation delete-stack \\\n"
            "  --stack-name mysql-dsql-cdc-stack\n\n"
            "aws cloudformation wait stack-delete-complete \\\n"
            "  --stack-name mysql-dsql-cdc-stack",
            language="bash",
        ).classes("w-full text-xs")
        ui.label(  # type: ignore[attr-defined]
            "This permanently removes the MSK cluster, both connectors, and any "
            "unprocessed messages (including the dead-letter queue). The DSQL "
            "cluster and your migrated data are NOT part of this stack and "
            "survive deletion."
        ).classes("text-xs text-gray-500")
        ui.label(  # type: ignore[attr-defined]
            "Note: if you connected the source with a username/password, the tool "
            "created a Secrets Manager secret for CDC. The in-app 'Delete CDC "
            "infrastructure' button removes it for you; the CLI delete-stack above "
            "does NOT — delete it manually so your database credentials do not "
            "linger:"
        ).classes("text-xs text-gray-500")
        ui.code(  # type: ignore[attr-defined]
            "aws secretsmanager delete-secret \\\n"
            "  --secret-id mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source \\\n"
            "  --recovery-window-in-days 7",
            language="bash",
        ).classes("w-full text-xs")


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


def _render_cdc_step(
    ui,
    migration_state,
    job_manager,
    refresh,
    *,
    inventory: Optional[SourceInventory] = None,
    migration_type: "MigrationType" = MigrationType.CDC_ONLY,
    run_checks=None,
    session: object = None,
) -> None:
    """Render the CDC step in user-journey order: decide -> prepare -> start ->
    monitor -> reference.

    The order follows how an operator actually thinks about CDC, not the code's
    convenience: (1) where it runs, (2) DECIDE the start point, (3) tune what is
    captured (oversized LOBs), (4) the explicit "start streaming" next action,
    (5) live monitoring once running (Pipeline health + per-table status + DLQ),
    (6) collapsed reference (CDC behavior & limits). Data parity and change-flow
    idle -- the cutover signals -- are shown by those panels and the Validation
    step, so there is no separate cutover panel.
    ``run_checks`` is accepted for signature parity; the prerequisites step owns it.
    """
    # 1. Where CDC runs (orientation banner).
    _render_cdc_runs_on_banner(ui)

    # 2. DECIDE: the CDC start point (Automatic/Manual). This is the central
    #    decision, so it comes first -- the source/sink connector config it
    #    produces is rendered (collapsed) inside this card.
    _render_cdc_source_config_card(
        ui,
        migration_state,
        job_manager,
        refresh,
        inventory=inventory,
        migration_type=migration_type,
        session=session,
    )

    # 3. PREPARE: oversized-LOB exclusion influences the captured columns, so it
    #    sits with the start-point decision (it feeds column.exclude.list).
    _render_cdc_lob_exclusion_panel(ui, migration_state, inventory, refresh)

    # 4. START: actually deploy the connectors (cloudformation update_stack) and
    #    show step-by-step progress.
    _render_cdc_start_action(
        ui, migration_state, job_manager, refresh,
        inventory=inventory, session=session,
    )

    # 5. MONITOR: live connector health + DLQ, meaningful only once streaming.
    _render_cdc_live_monitoring(ui, migration_state, job_manager)

    # 5b. PER-TABLE: Full Load outcome + live source/target row counts per selected
    #     table, so the operator can see Full Load completion and CDC replication
    #     converge table by table (MSK has no per-table metric; this counts directly).
    _render_migration_table_status(
        ui, migration_state, job_manager, session, inventory=inventory
    )

    # 6. REFERENCE: what CDC handles vs. what to watch (collapsed, educational).
    #    (A separate "Cutover readiness" panel was removed: its signals are already
    #    shown -- data parity in the per-table Consistency column / Validation step,
    #    and change-flow idle in the Pipeline health card -- so it only duplicated.)
    _render_cdc_handling_panel(ui)


def cdc_streaming_started(migration_state, job_manager) -> bool:
    """True once CDC has been started, so its inputs must no longer change.

    "Started" means the connectors are deployed/streaming (cdc-stack phase
    ``running`` -- detected connectors), or a CDC lifecycle job (start/stop/delete)
    is in flight. After this point the start position is already seeded into the
    MSK connect-offsets topic and the table set is fixed by the running source
    connector, so editing the CDC start point or the table selection would have no
    effect on the live pipeline and only mislead the operator. Mirrors the
    "running" detection in :func:`_render_cdc_start_action`. Read-only/best-effort.
    """
    controller = getattr(migration_state, "cdc_controller", None)
    names = getattr(migration_state, "cdc_connector_names", []) or []
    if controller is not None and names:
        return True  # connectors detected -> streaming
    if getattr(migration_state, "cdc_stack_phase", None) == "running":
        return True
    deploy_job = _current_job(
        job_manager, getattr(migration_state, "cdc_deploy_job_id", None)
    )
    return deploy_job is not None and deploy_job.status in ("PENDING", "RUNNING")


def _render_cdc_source_config_card(
    ui,
    migration_state,
    job_manager,
    refresh,
    *,
    inventory: Optional[SourceInventory] = None,
    migration_type: "MigrationType" = MigrationType.CDC_ONLY,
    session: object = None,
) -> None:
    """Render the Debezium source config, seeded from the watermark OR a manual
    start position.

    Builds :class:`~dsql_migrator.core.cdc.DebeziumSourceConfig` via
    :class:`CdcPipelineOrchestrator` (no AWS calls). The start offset is taken
    from the Full Load watermark when present (gapless -- Property 11), otherwise
    from a manual GTID / binlog file:position the operator enters here. The
    user's opt-in oversized-LOB exclusions flow into ``column.exclude.list``.
    Always shows the manual-entry form so a custom offset can override or stand
    in for a missing watermark; ``refresh`` re-renders the config preview when an
    override is applied.
    """
    job = _current_job(job_manager, migration_state.job_id)
    watermark = getattr(job, "watermark", None) if job is not None else None
    override = migration_state.cdc_start_override()
    mode = migration_state.cdc_start_mode()
    # The watermark resume (gapless) is the Automatic option's source.
    wm_resume = (
        CdcResumePoint.from_watermark(watermark) if watermark is not None else None
    )
    wm_usable = wm_resume is not None and wm_resume.has_coordinates()
    # Effective start position: in manual mode an entered override wins; in auto
    # mode the watermark is used.
    if mode == "manual" and override is not None and override.has_coordinates():
        effective_resume: Optional[CdcResumePoint] = override
    elif mode == "auto" and wm_usable:
        effective_resume = wm_resume
    else:
        effective_resume = None

    # PRIMARY card: the CDC start point. This is the central decision of the CDC
    # step, so it is a top-level card with an explicit Automatic/Manual choice --
    # not buried inside the raw connector config (which is now a collapsed
    # "advanced" expansion below).
    # Once CDC is streaming (or a lifecycle job is in flight) the start point is
    # already seeded into connect-offsets and cannot change the live pipeline, so
    # the radio + manual inputs are locked (read-only) to avoid misleading edits.
    started = cdc_streaming_started(migration_state, job_manager)
    _render_cdc_start_point_card(
        ui,
        migration_state,
        refresh,
        wm_resume=wm_resume,
        wm_usable=wm_usable,
        effective_resume=effective_resume,
        mode=mode,
        locked=started,
    )

    if effective_resume is None:
        return

    # Build the connector config (pure -- no AWS calls). Restrict the table list
    # to what the watermark covered when inventory + watermark exist; otherwise
    # fall back to the user's confirmed selection (manual seed).
    exclusions = migration_state.cdc_lob_exclusions()
    exclude_value = format_column_exclude_list(
        {table: sorted(cols) for table, cols in exclusions.items()}
    )
    exclude_list = exclude_value.split(",") if exclude_value else None
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)

    # A CDC sink needs at least one table: with none selected the sink topic list
    # would be empty and the deploy would later fail at connector create (opaque
    # HTTP 400). Surface that here as a calm, actionable notice instead of building
    # a config preview from an empty selection (build_sink_config would raise).
    if not tables_for_config:
        render_notice(
            ui,
            tone="warning",
            header="Select at least one table before starting CDC",
            body=(
                "No tables are selected for replication yet, so there is nothing "
                "for the CDC sink to write. Choose the tables to migrate (Schema "
                "Conversion / the table picker), then return here to start CDC."
            ),
        )
        return

    config = CdcPipelineOrchestrator().build_source_config(
        "mysql-source",
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        column_exclude_list=exclude_list,
        resume_override=override if (mode == "manual" and override is not None and override.has_coordinates()) else None,
    )

    # The sink config (topics, keying, DLQ). DLQ name is the cdc-stack default --
    # NOT derived from a connector that may not exist yet (pre-deploy).
    sink = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC
    )

    # Build the deployable cdc-stack parameter set: tool-known values filled,
    # customer-environment values as <FILL_ME> placeholders.
    target = getattr(session, "target_config", None)
    params = build_cdc_stack_params(
        config,
        sink,
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME),
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
    )

    # SECONDARY (collapsed): connector configuration to hand to the cdc-stack.
    # Human-readable summary + the actual deployable parameter file. Advanced
    # output, so progressive disclosure keeps the start-point decision primary.
    source_lines = [
        f"snapshot.mode = {config.snapshot_mode}",
        f"table.include.list = {', '.join(config.table_include_list) or '(all selected)'}",
    ]
    if config.start_gtid:
        source_lines.append(f"gtid (start) = {config.start_gtid}")
    if config.start_binlog_file:
        source_lines.append(
            f"binlog (start) = {config.start_binlog_file}:{config.start_binlog_pos}"
        )
    if config.column_exclude_list:
        source_lines.append(
            f"column.exclude.list = {', '.join(config.column_exclude_list)}"
        )
    sink_lines = [
        f"name = {sink.name}",
        f"topics (prefixed) = {dict(params.filled).get('SinkTopics') or '(all selected)'}",
        f"pk.mode = {sink.pk_mode}",
        f"insert.mode = {sink.insert_mode}",
        f"delete.enabled = {str(sink.delete_enabled).lower()}",
        f"errors.deadletterqueue.topic.name = {sink.dlq_topic}",
    ]
    with ui.expansion(  # type: ignore[attr-defined]
        "Connector configuration (hand to cdc-stack)", icon="settings"
    ).classes("w-full").props("expand-separator"):
        ui.label("Debezium source connector").classes(  # type: ignore[attr-defined]
            "text-xs font-semibold text-gray-600"
        )
        ui.code("\n".join(source_lines)).classes("w-full text-xs")  # type: ignore[attr-defined]
        ui.label("Custom DSQL sink connector").classes(  # type: ignore[attr-defined]
            "text-xs font-semibold text-gray-600 mt-2"
        )
        ui.code("\n".join(sink_lines)).classes("w-full text-xs")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Poison rows are quarantined to the dead-letter queue, not dropped."
        ).classes("text-xs text-gray-500")
        _render_cdc_params_file(ui, params)


def _render_cdc_params_file(ui, params) -> None:
    """Render the deployable cdc-stack parameter file (JSON) + a copy button.

    Tool-known values are filled; customer-environment values carry
    ``<FILL_ME:`` placeholders that MUST be replaced before deploying. A bold
    warning makes that unmistakable. The CDC start position is intentionally not
    a parameter (it is seeded via the connect-offsets topic), so a note explains
    that separately.
    """
    params_json = cdc_stack_params_to_json(params)
    n_placeholder = len(params.placeholders)
    with ui.expansion(  # type: ignore[attr-defined]
        "cdc-stack parameter file (JSON)", icon="description"
    ).classes("w-full mt-2").props("expand-separator"):
        inline_hint(  # type: ignore[attr-defined]
            ui,
            f"⚠ Replace every value starting with {CDC_PLACEHOLDER_PREFIX} "
            f"({n_placeholder} customer-environment value(s)) before deploying — "
            "CloudFormation will reject or fail on an unfilled placeholder.",
            tone="warning",
            classes="text-xs font-semibold",
        )
        ui.code(params_json, language="json").classes("w-full text-xs")  # type: ignore[attr-defined]

        def _copy() -> None:
            try:
                ui.clipboard.write(params_json)  # type: ignore[attr-defined]
                ui.notify("Parameter file copied.", type="positive", position="top")  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - clipboard may be unavailable
                ui.notify(  # type: ignore[attr-defined]
                    "Copy the parameter file from the block above.", type="info",
                    position="top",
                )

        ui.button("Copy parameter file", on_click=_copy).props(  # type: ignore[attr-defined]
            "size=sm outline icon=content_copy"
        )
        ui.label(  # type: ignore[attr-defined]
            "Note: the CDC start position (GTID / binlog) is NOT a parameter here "
            "— it is seeded via the connect-offsets topic after deploy. See the "
            "start point shown above."
        ).classes("text-xs text-gray-500")


def _sentinel_watermark() -> "Watermark":
    """A placeholder watermark for the manual-override path (coords unused).

    ``build_source_config`` requires a ``Watermark`` argument, but when a manual
    ``resume_override`` is supplied its coordinates are ignored. Pass a minimal
    valid watermark stamped with the current time so the type contract holds.
    """
    return Watermark(snapshot_timestamp=datetime.now(timezone.utc))


def _cdc_tables_for_config(
    migration_state, inventory: Optional[SourceInventory], watermark
) -> list:
    """Resolve which tables the CDC source config should include.

    Prefers the watermark's covered tables (the snapshot's exact set) when both
    inventory and watermark are present; otherwise falls back to the user's
    confirmed table selection (the manual-seed case where no Full Load ran).
    Returns an empty list when neither is available (config shows "all selected").
    """
    if inventory is None:
        return []
    if watermark is not None and watermark.table_row_counts:
        covered = set(watermark.table_row_counts)
        return [t for t in inventory.tables if t.name in covered]
    selection = migration_state.selection
    if selection is not None and selection.selected_tables:
        return TableSelector().resolve(inventory, selection)
    return []


def _render_cdc_start_point_card(
    ui,
    migration_state,
    refresh,
    *,
    wm_resume,
    wm_usable: bool,
    effective_resume,
    mode: str,
    locked: bool = False,
) -> None:
    """Render the PRIMARY 'CDC start point' card with an Automatic/Manual choice.

    This is the central decision of the CDC step -- where streaming begins -- so
    it is a top-level card (not buried in the raw connector config). An AWS
    console-style radio chooses between Automatic (gapless from the Full Load
    watermark) and Manual (an explicit GTID / binlog position); the manual inputs
    appear only when Manual is selected. A status line confirms the resolved start
    point. Validation is advisory (orange hint, never blocks): MSK Connect is the
    final authority at connector start.

    ``locked`` (CDC already started) makes the radio + manual inputs read-only:
    the start position is already committed to connect-offsets, so changing it
    here cannot affect the live pipeline.
    """
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon(  # type: ignore[attr-defined]
                "play_circle",
                color="primary" if effective_resume is not None else "grey",
            ).classes("text-xl")
            ui.label("CDC start point").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            ui.space()  # type: ignore[attr-defined]
            if locked:
                ui.badge("Locked", color="grey").props("outline")  # type: ignore[attr-defined]
            elif effective_resume is not None:
                ui.badge("Ready", color="positive").props("outline")  # type: ignore[attr-defined]
            else:
                ui.badge("Action needed", color="warning").props("outline")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Where change streaming begins. Automatic resumes exactly where the "
            "Full Load snapshot ended (no gap, no overlap)."
        ).classes("text-xs text-gray-500")

        if locked:
            # CDC has started: the start point is fixed (seeded into connect-offsets).
            ui.label(  # type: ignore[attr-defined]
                "CDC has started — the start point is locked. To change it, stop "
                "CDC first."
            ).classes("text-xs text-gray-500")

        auto_label = (
            "Automatic — gapless from Full Load (recommended)"
            if wm_usable
            else "Automatic — needs a Full Load watermark (unavailable)"
        )

        def _on_mode(value: str) -> None:
            migration_state.set_cdc_start_mode(value)
            refresh()

        # AWS-console-style radio choice. Disable Automatic when no usable
        # watermark exists so the user is steered to Manual rather than a dead end.
        # When CDC has started the whole choice is disabled (read-only).
        radio = ui.radio(  # type: ignore[attr-defined]
            {"auto": auto_label, "manual": "Manual — enter a GTID or binlog position"},
            value=mode,
            on_change=lambda e: _on_mode(e.value),
        ).props("inline=false")
        if locked:
            radio.props("disable")
        if not wm_usable and mode == "auto" and not locked:
            # Steer to manual: auto is not usable here.
            inline_hint(  # type: ignore[attr-defined]
                ui,
                "No usable Full Load watermark in this session "
                "(run a Full Load first, or choose Manual).",
                tone="warning",
            )

        if mode == "manual":
            _render_cdc_manual_inputs(ui, migration_state, refresh, locked=locked)
        elif wm_usable and wm_resume is not None:
            _render_cdc_start_summary(
                ui, wm_resume.gtid_executed, wm_resume.binlog_file,
                wm_resume.binlog_position,
            )

        # Resolved start-point confirmation (the single source of truth).
        if effective_resume is not None:
            with ui.row().classes("items-center gap-2 no-wrap mt-1"):  # type: ignore[attr-defined]
                ui.icon("check_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
                coord = (
                    f"GTID {effective_resume.gtid_executed}"
                    if effective_resume.gtid_executed
                    else f"binlog {effective_resume.binlog_file}:"
                    f"{effective_resume.binlog_position}"
                )
                ui.label(f"Start point set — {coord}").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-700 font-mono"
                )


def _render_cdc_start_summary(ui, gtid, binlog_file, binlog_pos) -> None:
    """Show the resolved coordinates for the Automatic (watermark) choice."""
    with ui.row().classes("items-center gap-x-6 gap-y-1 flex-wrap mt-1"):  # type: ignore[attr-defined]
        for label_text, value in (
            ("GTID", gtid or "(none)"),
            (
                "Binlog",
                f"{binlog_file}:{binlog_pos}" if binlog_file else "(none)",
            ),
        ):
            with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                ui.label(f"{label_text}:").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
                ui.label(str(value)).classes("text-xs font-mono")  # type: ignore[attr-defined]


def _render_cdc_manual_inputs(ui, migration_state, refresh, *, locked: bool = False) -> None:
    """Render the Manual start-position inputs (GTID / binlog file:pos) + Apply.

    Shown only when the Manual radio is selected. Advisory validation: an
    unrecognized-but-valid GTID must not be rejected, so a bad-looking value shows
    an orange hint but is still stored; MSK Connect validates at connector start.
    ``locked`` (CDC already started) renders the inputs + button read-only.
    """
    gtid_error = {"msg": None}  # mutable cell so the handler can show a hint

    def _apply() -> None:
        gtid_raw = (gtid_input.value or "").strip()
        binlog_raw = (binlog_input.value or "").strip()
        gtid_error["msg"] = validate_gtid(gtid_raw) if gtid_raw else None
        binlog_file = None
        binlog_pos = None
        binlog_unparsed = False
        if binlog_raw:
            parsed = parse_binlog_coordinate(binlog_raw)
            if parsed is not None:
                binlog_file, binlog_pos = parsed
            else:
                binlog_unparsed = True
        migration_state.set_cdc_start_position(
            gtid=gtid_raw or None,
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
        )
        # Explicit feedback so the click is never silent. Anchored to the top so
        # it appears near this card (which sits high on the page), not at the
        # bottom-center default where it is easy to miss.
        if not gtid_raw and not binlog_raw:
            ui.notify(  # type: ignore[attr-defined]
                "Enter a GTID set or a binlog file:position first.",
                type="warning", position="top",
            )
        elif binlog_unparsed and not gtid_raw:
            ui.notify(  # type: ignore[attr-defined]
                "Binlog must be 'file:position' (e.g. mysql-bin.000123:45678).",
                type="negative", position="top",
            )
        elif gtid_error["msg"]:
            # Stored anyway (advisory), but tell the user it looks off.
            ui.notify(  # type: ignore[attr-defined]
                "Start point saved, but the GTID format looks unusual — "
                "double-check it.",
                type="warning", position="top",
            )
        else:
            coord = gtid_raw if gtid_raw else f"{binlog_file}:{binlog_pos}"
            ui.notify(  # type: ignore[attr-defined]
                f"Start point saved (not streaming yet) — {coord}",
                type="positive", position="top",
            )
        refresh()

    ui.label(  # type: ignore[attr-defined]
        "Enter a GTID set, or a binlog file:position. Use a GTID when the source "
        "has GTIDs enabled (preferred)."
    ).classes("text-xs text-gray-500 mt-1")
    gtid_input = ui.input(  # type: ignore[attr-defined]
        "GTID set",
        value=migration_state._cdc_start_gtid or "",
        placeholder="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100",
    ).classes("w-full text-xs")
    binlog_default = ""
    if migration_state._cdc_start_binlog_file and migration_state._cdc_start_binlog_pos is not None:
        binlog_default = (
            f"{migration_state._cdc_start_binlog_file}:"
            f"{migration_state._cdc_start_binlog_pos}"
        )
    binlog_input = ui.input(  # type: ignore[attr-defined]
        "Binlog file:position",
        value=binlog_default,
        placeholder="mysql-bin.000123:45678",
    ).classes("w-full text-xs")
    if locked:
        # CDC started: the coordinate is already committed -- show it read-only.
        gtid_input.props("readonly")
        binlog_input.props("readonly")

    if gtid_error["msg"]:
        inline_hint(ui, gtid_error["msg"], tone="warning")  # type: ignore[attr-defined]

    # "Use this start point" (not "Apply"/"Start") — this only records the
    # coordinate into the connector config; it does NOT begin streaming. Actual
    # streaming starts when the config is deployed to the cdc-stack. Disabled once
    # CDC has started (the start point is already seeded and cannot change).
    apply_btn = ui.button("Use this start point", on_click=_apply).props(  # type: ignore[attr-defined]
        "size=sm color=primary"
    )
    if locked:
        apply_btn.props("disable")


def _render_cdc_runs_on_banner(ui) -> None:
    """Orientation banner: where CDC runs (source -> MSK -> DSQL sink)."""
    with ui.row().classes(  # type: ignore[attr-defined]
        "w-full items-start gap-2 p-3 rounded-lg border border-blue-200 "
        "bg-blue-50 no-wrap"
    ):
        ui.icon("hub", color="primary").classes("text-xl")  # type: ignore[attr-defined]
        with ui.column().classes("gap-0"):  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "Runs on the optional cdc-stack: Debezium → MSK → custom DSQL "
                "sink."
            ).classes("text-sm text-gray-700")
            ui.label(  # type: ignore[attr-defined]
                "The control plane configures and monitors CDC; the cdc-stack "
                "(deployed separately) does the streaming."
            ).classes("text-xs text-gray-500")


def classify_cdc_card_phase(
    detected_names,
    stack_name: str,
    probed_phase: Optional[str],
    running_names=None,
    failed_names=None,
) -> Optional[str]:
    """Resolve the CDC lifecycle card's phase from detected connectors + probe.

    Connector detection is authoritative when it finds connectors; otherwise the
    probed CloudFormation phase (``absent`` / ``infra`` / ``unstable``) is used.

    Both expected connectors (source AND sink) present:

    - any of them ``FAILED`` (per ``failed_names`` -- task stopped/errored, needs a
      restart) -> ``"partial"`` (NOT streaming, recovery required) -- this is
      checked first so a dead task is never mistaken for "still provisioning";
    - else all ``RUNNING`` (per ``running_names``) -> ``"running"`` (streaming);
    - else some still coming up (``CREATING``/``UPDATING``) -> ``"provisioning"`` --
      MSK takes ~10-20 min to bring a connector up, so this avoids the misleading
      "Streaming" label while a sink is still being created.

    A non-empty but incomplete set is ``"partial"`` -- the post-failed-Start /
    post-rollback state where (typically) only the source survived: changes are
    captured into Kafka but never written to DSQL, so it is NOT streaming.

    ``running_names`` (optional) is the subset of detected connectors that are
    genuinely RUNNING; ``failed_names`` (optional) is the subset the live status
    view reports FAILED (task dead). When both are omitted the legacy behavior
    holds -- both present is treated as ``"running"``. NiceGUI-agnostic so the
    classification is unit-testable. Pure.
    """
    names = set(detected_names or [])
    if not names:
        return probed_phase
    expected = set(cdc_expected_connector_names(stack_name))
    if not expected.issubset(names):
        return "partial"
    # Both expected connectors exist. A FAILED connector (task stopped/errored)
    # needs a restart, NOT more waiting -- treat it as partial (recovery), never
    # provisioning. Checked before the running/provisioning split below.
    if failed_names and (set(failed_names) & expected):
        return "partial"
    # If we know which are RUNNING, require ALL of them to be RUNNING for
    # "streaming"; otherwise it is still provisioning (coming up).
    if running_names is not None and not expected.issubset(set(running_names)):
        return "provisioning"
    return "running"


def cdc_live_running_names(discovery_running, connector_states) -> list:
    """Narrow the discovery "running" set to connectors LIVE-reported RUNNING.

    ``discovery_running`` is the set of connectors whose MSK ``connectorState`` was
    RUNNING at the last stack/connector discovery -- but a connector can report
    ``connectorState=RUNNING`` while its TASK has stopped/errored (not actually
    streaming). The live status view (``connector_states``, built from CloudWatch
    task health) flips such a connector to a non-RUNNING state. Intersecting the
    two yields the connectors that are genuinely streaming, so the lifecycle card's
    phase matches the Pipeline-health rows (a task-dead source → "Incomplete", not
    a misleading "Streaming").

    When no live states are available yet (no poll has run), the discovery set is
    used as-is (best signal we have). Pure.
    """
    running = [n for n in (discovery_running or [])]
    if not connector_states:
        return running
    return [
        n for n in running
        if str(connector_states.get(n, "")).upper() == "RUNNING"
    ]


def cdc_deploy_card_superseded(
    has_deploy_job: bool, deploying: bool, deploy_job_error, phase
) -> bool:
    """Whether a finished deploy job's stage card should be SUPPRESSED.

    A deploy job the JobManager reconciled to FAILED only because the app
    restarted mid-run carries stale stages (e.g. ``stack_sink=FAILED`` with later
    steps still PENDING) -- a red mark + "not done yet" rows that contradict the
    real, AWS-derived verdict. When live connector discovery already has a
    definitive phase (``running`` / ``provisioning`` / ``partial``), that is the
    truth, so the stale interrupted-job stage card is hidden and discovery drives
    the display. Only applies to a restart-interrupted job that is no longer
    in-flight; a genuinely-failed job or one still running keeps its card. Pure.
    """
    return (
        has_deploy_job
        and not deploying
        and is_interrupted_by_restart(deploy_job_error)
        and phase in ("running", "provisioning", "partial")
    )


def _render_cdc_start_action(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Render the CDC lifecycle card: Deploy infra → Start → Stop → Delete.

    The UI owns the whole cdc-stack lifecycle as CloudFormation operations. The
    card branches on the cached stack phase (probed by ``_ensure_cdc_controller``
    / ``_probe_cdc_stack_phase``):

    * **absent** — no stack yet → show the BYO-VPC infrastructure form + a
      "Deploy CDC infrastructure" button (create_stack, ~15-20 min).
    * **infra** — stack up, no connectors → "Start CDC" (after the start point is
      set): a two-pass update that creates the connectors.
    * **running** — connectors deployed → "Stop CDC" (delete connectors only).
    * **unstable** — an operation is mid-flight or rolled back → guidance.

    A guarded "Delete CDC infrastructure" action is always offered (type-to-
    confirm) for full teardown / rollback recovery. While any lifecycle job runs,
    its ordered stages + stack-event log stream in via ``_render_cdc_deploy_live``.
    """
    deploy_job = _current_job(
        job_manager, getattr(migration_state, "cdc_deploy_job_id", None)
    )
    deploying = deploy_job is not None and deploy_job.status in ("PENDING", "RUNNING")

    controller = getattr(migration_state, "cdc_controller", None)
    names = getattr(migration_state, "cdc_connector_names", []) or []
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    expected = set(cdc_expected_connector_names(stack_name))
    detected_names = names if controller is not None else []
    # "Running" must mean a connector whose TASK is actually streaming, not merely
    # that the connector resource exists. The live status view folds CloudWatch
    # task health into per-connector state (a connector MSK reports RUNNING but
    # whose task stopped/errored becomes FAILED), so intersect the discovery
    # running-set with the status view's RUNNING connectors. This keeps the top
    # card's phase in lockstep with the Pipeline-health rows -- so a source whose
    # task died shows "Incomplete", not a misleading "Streaming".
    live_states = (
        getattr(
            _cdc_status_view(migration_state, job_manager), "connector_states", None
        )
        if controller is not None
        else None
    )
    running_names = cdc_live_running_names(
        getattr(migration_state, "cdc_connector_running_names", []) or [],
        live_states,
    ) if controller is not None else []
    # Connectors the live status view reports FAILED (task stopped/errored). A
    # FAILED connector needs a RESTART, not more waiting -- so it must classify as
    # "partial" (recovery), never "provisioning". Without this a dead source task
    # is mislabeled "Provisioning… (streaming begins once both reach RUNNING)",
    # telling the user to wait when the fix is to restart.
    failed_names = (
        [n for n, s in (live_states or {}).items() if str(s).upper() == "FAILED"]
    )
    phase = classify_cdc_card_phase(
        detected_names,
        stack_name,
        getattr(migration_state, "cdc_stack_phase", None),
        running_names=running_names,
        failed_names=failed_names,
    )

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            badge_text, badge_color, icon_color = (
                ("Streaming", "positive", "positive") if phase == "running"
                else ("Provisioning…", "primary", "primary") if phase == "provisioning"
                else ("Working…", "primary", "primary") if deploying
                else ("Incomplete", "warning", "warning") if phase == "partial"
                else ("Infra ready", "primary", "primary") if phase == "infra"
                else ("Busy", "warning", "warning") if phase == "unstable"
                else ("Not deployed", "grey", "grey")
            )
            ui.icon("rocket_launch", color=icon_color).classes("text-xl")  # type: ignore[attr-defined]
            ui.label("CDC pipeline").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            ui.space()  # type: ignore[attr-defined]
            ui.badge(badge_text, color=badge_color).props("outline")  # type: ignore[attr-defined]

        # Live progress for whichever lifecycle job is in flight (or just done).
        # EXCEPT: a deploy job that the JobManager reconciled to FAILED only
        # because the app restarted mid-run is NOT the truth -- its stale stages
        # (e.g. stack_sink=FAILED) would show a red mark and "not done yet" steps
        # even though AWS finished. When live connector discovery already has a
        # definitive verdict (Streaming / Provisioning / Incomplete), let THAT
        # drive the card and suppress the stale interrupted job's stage card.
        deploy_job_error = None
        if deploy_job is not None:
            try:
                deploy_job_error = job_manager.get_error(
                    migration_state.cdc_deploy_job_id
                )
            except JobNotFoundError:
                deploy_job_error = None
        interrupted_superseded = cdc_deploy_card_superseded(
            deploy_job is not None, deploying, deploy_job_error, phase
        )
        if deploy_job is not None and not interrupted_superseded:
            _render_cdc_deploy_live(ui, migration_state, job_manager, refresh)
            if deploying:
                return  # all action buttons hidden while an operation runs

        if phase == "provisioning":
            # Both connectors exist but at least one is still CREATING/UPDATING on
            # MSK (~10-20 min). Show a clear "still coming up" notice instead of a
            # misleading "Streaming"/Stop affordance, and re-poll so the card flips
            # to "running" on its own once the sink reaches RUNNING.
            still = [n for n in names if n not in set(running_names)]
            render_notice(
                ui,
                tone="info",
                header="CDC pipeline provisioning",
                body=(
                    "The connectors are deploying on MSK Connect (this takes about "
                    "10–20 minutes). "
                    + (
                        f"Still coming up: {', '.join(still)}. "
                        if still
                        else ""
                    )
                    + "Streaming begins once both connectors reach RUNNING; this "
                    "view refreshes automatically."
                ),
            )
            ui.timer(  # type: ignore[attr-defined]
                _CDC_POLL_INTERVAL_SECONDS, refresh, once=True
            )
        elif phase == "running":
            _render_cdc_running_actions(
                ui, migration_state, job_manager, refresh, session=session
            )
        elif phase == "partial":
            _render_cdc_partial_actions(
                ui, migration_state, job_manager, refresh,
                names=names, expected=expected,
                inventory=inventory, session=session,
            )
        elif phase == "infra":
            _render_cdc_start_button(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            )
        elif phase == "unstable":
            status = getattr(migration_state, "cdc_stack_phase_status", None) or "busy"
            if _is_inflight_stack_status(status):
                # A real in-progress operation -> waiting is the right action.
                message = (
                    f"The cdc-stack is '{status}'. Wait for the current operation to "
                    "finish (progress refreshes), then the next action appears."
                )
            else:
                # A terminal failed/rolled-back state (e.g. ROLLBACK_FAILED,
                # ROLLBACK_COMPLETE, UPDATE_ROLLBACK_FAILED, DELETE_FAILED). The
                # stack is stuck, NOT busy -- waiting never clears it. The user must
                # delete it before redeploying.
                message = (
                    f"The cdc-stack is stuck in '{status}' from a failed operation — "
                    "it will not clear on its own. Use Delete CDC infrastructure "
                    "below, then deploy again."
                )
            render_notice(
                ui,
                tone="warning",
                header="cdc-stack needs cleanup",
                body=message,
            )
        else:  # absent / not yet probed
            _render_cdc_infra_deploy_action(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            )

        # Full teardown (guarded) once a stack might exist. When a connector is up
        # (running OR partial) the inline primary action is Stop CDC (connectors
        # only, keeps MSK/VPC) -- a full Delete would tear down the whole pipeline,
        # so it is NOT shown inline beside Stop; it is tucked behind a collapsed
        # "Danger zone" so it stays reachable for an intentional teardown but can't
        # be hit by mistake beside Stop. For infra / unstable phases (no connector
        # to stop) Delete IS the recovery action, so it stays inline.
        if phase in ("running", "partial"):
            if phase == "running":
                danger_header = "Stop CDC first for a normal pause"
                danger_body = (
                    "Deleting tears down the whole cdc-stack (MSK, VPC wiring, "
                    "plugins, connectors). To pause streaming, use Stop CDC above "
                    "instead — it keeps the infrastructure so you can restart "
                    "quickly. Delete only for a full teardown."
                )
            else:  # partial
                danger_header = "Delete only if Stop + Start can't recover"
                danger_body = (
                    "Try Stop CDC above, then Start CDC again first. Delete only if "
                    "Start keeps failing (e.g. the MSK partition quota is exhausted) "
                    "— it tears down the whole cdc-stack (MSK, VPC wiring, plugins, "
                    "connectors) and a redeploy reclaims the quota from a clean slate."
                )
            with ui.expansion("Danger zone — delete all CDC infrastructure").props(
                "dense"
            ).classes("w-full mt-2 text-red-700"):
                render_notice(ui, tone="warning", header=danger_header, body=danger_body)
                _render_cdc_delete_action(
                    ui, migration_state, job_manager, refresh, session=session
                )
        elif phase in ("infra", "unstable"):
            _render_cdc_delete_action(ui, migration_state, job_manager, refresh, session=session)


def _render_cdc_partial_actions(
    ui, migration_state, job_manager, refresh, *, names, expected,
    inventory=None, session=None,
) -> None:
    """Actions for a PARTIAL pipeline: some but not all connectors are present.

    This is the post-failed-Start / post-rollback state — e.g. the source
    survived but the sink connector failed to create (commonly the MSK Serverless
    partition quota being exhausted) and CloudFormation rolled it back. With only
    one connector, changes are captured into Kafka but never written to DSQL (if
    the sink is missing) or nothing is captured at all (if the source is missing),
    so this is NOT "streaming". Be honest about it and steer the user to recover.

    Two recovery actions, ranked:

    - **Retry CDC** (primary): re-run Start CDC. ``run_cdc_start`` is config-aware
      idempotent, so a source that is already RUNNING is kept (no binlog re-read,
      no wasted MSK churn) and only the MISSING connector is (re)created. The
      events the source already streamed sit buffered in the Kafka topics, so the
      new sink consumes them and catches DSQL up -- no gap. This is the fast,
      non-destructive path for a sink that failed to create.
    - **Clean up leftover connector** (secondary): remove the surviving connector
      (same backend path as Stop CDC) to return to a clean infra-only state --
      useful when Retry keeps failing (e.g. MSK partition quota) and a from-scratch
      Start (or Delete + redeploy via the Danger zone) is wanted.
    """
    present = set(names)
    missing = [n for n in expected if n not in present]
    missing_label = ", ".join(connector_role_label(n) for n in missing) or "a connector"
    present_label = ", ".join(connector_role_label(n) for n in sorted(present))
    render_notice(
        ui,
        tone="warning",
        header="CDC pipeline is incomplete — not streaming",
        body=(
            f"Only {present_label} is present; {missing_label} is missing (a failed "
            "Start or a rolled-back deploy). Until both the source and sink "
            "connectors are running, changes are not being written to DSQL. Retry "
            "CDC to re-create the missing connector (the running one is kept). If "
            "Retry keeps failing (e.g. the MSK partition quota), clean up the "
            "leftover connector, or use the Danger zone below to delete the "
            "infrastructure and redeploy."
        ),
    )

    def _cleanup() -> None:
        _open_cdc_stop_dialog(
            ui, migration_state,
            lambda: _start_cdc_stop(ui, migration_state, job_manager, refresh, session=session),
            partial=True,
        )

    def _retry() -> None:
        # Re-run Start CDC: idempotent, so the RUNNING connector is left alone and
        # the missing one is re-created (the source's buffered events let the new
        # sink catch DSQL up with no gap).
        _start_cdc_deploy(
            ui, migration_state, job_manager, refresh,
            inventory=inventory, session=session,
        )

    with ui.row().classes("items-center gap-2 w-full"):  # type: ignore[attr-defined]
        ui.button(  # type: ignore[attr-defined]
            "Clean up leftover connector", on_click=_cleanup,
            icon="cleaning_services",
        ).props("color=grey-7 outline")
        ui.space()  # type: ignore[attr-defined]
        retry_btn = ui.button(  # type: ignore[attr-defined]
            "Retry CDC", on_click=_retry, icon="replay"
        ).props("color=primary")
        retry_btn.tooltip(
            f"Re-create the missing connector ({missing_label}); the running "
            "connector is kept. Buffered changes let the new sink catch up — no gap."
        )


def _render_cdc_start_button(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """The 'Start CDC' button shown when infra is deployed but no connectors run."""
    job = _current_job(job_manager, migration_state.job_id)
    watermark = getattr(job, "watermark", None) if job is not None else None
    override = migration_state.cdc_start_override()
    wm_resume = (
        CdcResumePoint.from_watermark(watermark) if watermark is not None else None
    )
    ready = (override is not None and override.has_coordinates()) or (
        wm_resume is not None and wm_resume.has_coordinates()
    )
    render_notice(
        ui,
        tone="info",
        icon="rocket_launch",
        header="Ready to start CDC",
        body=(
            "Infrastructure is deployed. Start CDC creates the connectors for your "
            "selected tables (source first, then sink) and begins streaming. It "
            "takes a few minutes; progress appears below."
        ),
    )

    # Show WHICH tables will stream right here, so the "pick your tables first"
    # advice is verifiable at a glance instead of asking the user to scroll up.
    cdc_tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    table_names = [t.name for t in cdc_tables]
    if table_names:
        preview = ", ".join(table_names[:6]) + (
            f" +{len(table_names) - 6} more" if len(table_names) > 6 else ""
        )
        noun = "table" if len(table_names) == 1 else "tables"
        selection_line = f"Will stream {len(table_names)} {noun}: {preview}"
    else:
        selection_line = (
            "Will stream all tables covered by the Full Load snapshot / your "
            "current selection."
        )

    # Escalate the "pick your tables first" guidance ONLY once connectors have
    # actually existed before (a prior Start/Stop this session or a restored run):
    # that is when repeated create/delete has begun consuming MSK's non-reclaimed
    # partition capacity, so the caution is real. On the FIRST start after a fresh
    # deploy it is just a calm heads-up (info) -- not an alarm on the happy path.
    started_before = bool(getattr(migration_state, "cdc_connector_names", None))
    if started_before:
        render_notice(
            ui,
            tone="warning",
            icon="warning",
            header="Re-starting CDC uses more MSK capacity each time",
            body=(
                f"{selection_line} Each restart re-creates connectors, and MSK's "
                "limited capacity isn't freed up between runs — so repeated "
                "start/stop cycles can eventually require deleting and redeploying "
                "the CDC infrastructure. Confirm the table set is final before "
                "starting again."
            ),
        )
    else:
        render_notice(
            ui,
            tone="info",
            icon="tips_and_updates",
            header="Pick all your tables before you start",
            body=(
                f"{selection_line} Changing the set later means re-creating "
                "connectors, and each connector uses some of MSK's limited "
                "capacity that isn't freed up again — so many restarts can "
                "eventually require redeploying the CDC infrastructure. Choosing "
                "everything you need up front keeps this smooth."
            ),
        )

    def _confirm() -> None:
        _open_cdc_start_dialog(
            ui, migration_state,
            lambda: _start_cdc_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
        )

    start_btn = ui.button(  # type: ignore[attr-defined]
        "Start CDC", on_click=_confirm, icon="play_arrow"
    ).props("color=primary")
    if not ready:
        start_btn.props("disable")
        ui.label(  # type: ignore[attr-defined]
            "Set the CDC start point above first."
        ).classes("text-xs text-gray-500")


def _render_cdc_running_actions(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """The 'Stop CDC' action shown while connectors are running."""
    ui.label(  # type: ignore[attr-defined]
        "The cdc-stack connectors are deployed and streaming — see live status "
        "below. Stop CDC removes just the connectors; MSK, the VPC wiring, and "
        "the plugins are kept so you can restart quickly."
    ).classes("text-xs text-gray-600")

    def _confirm() -> None:
        _open_cdc_stop_dialog(
            ui, migration_state,
            lambda: _start_cdc_stop(ui, migration_state, job_manager, refresh, session=session),
        )

    ui.button(  # type: ignore[attr-defined]
        "Stop CDC", on_click=_confirm, icon="stop_circle"
    ).props("color=orange outline")


def _render_cdc_infra_deploy_action(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """The BYO-VPC infrastructure form + 'Deploy CDC infrastructure' button."""
    ui.label(  # type: ignore[attr-defined]
        "No cdc-stack is deployed yet. Provide your VPC and the plugin/source "
        "details below, then deploy the infrastructure (MSK Serverless, the "
        "connector networking, plugins and IAM role). This takes ~15-20 minutes "
        "and creates billable AWS resources; connectors are created later by "
        "Start CDC."
    ).classes("text-xs text-gray-600")

    _render_cdc_least_privilege_note(ui, session=session)
    _render_cdc_infra_form(ui, migration_state, session=session)

    async def _confirm() -> None:
        await _open_cdc_infra_dialog(
            ui, migration_state,
            lambda: _start_cdc_infra_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
        )

    ui.button(  # type: ignore[attr-defined]
        "Deploy CDC infrastructure", on_click=_confirm, icon="cloud_upload"
    ).props("color=primary")


def _render_cdc_least_privilege_note(ui, *, session=None) -> None:
    """Recommend a dedicated least-privilege CDC MySQL user before deploy.

    When the source was connected with a username/password, those exact
    credentials are stored in Secrets Manager and injected into the live Debezium
    connector. If the operator connected as root/admin, that over-privileged
    credential becomes long-lived. This collapsible note recommends a dedicated
    CDC user with only the grants Debezium needs and shows a copyable snippet.
    Shown only for password auth (an SM-auth source already chose its own secret).
    """
    if getattr(session, "source_secret_id", None):
        return  # SM auth -- the customer manages their own credential.
    with ui.expansion(  # type: ignore[attr-defined]
        "Recommended: use a dedicated least-privilege CDC user",
        icon="security",
        value=False,
    ).classes("w-full").props("expand-separator"):
        ui.label(  # type: ignore[attr-defined]
            "The username/password you connected with will be stored in AWS "
            "Secrets Manager and used by the CDC connector. Avoid using an "
            "admin/root account: create a dedicated MySQL user granted only what "
            "Debezium needs, reconnect on the Connect step as that user, then "
            "deploy. This limits blast radius if the secret is ever exposed."
        ).classes("text-xs text-gray-600")
        ui.code(  # type: ignore[attr-defined]
            "CREATE USER 'dsql_cdc'@'%' IDENTIFIED BY '<strong-password>';\n"
            "GRANT SELECT, RELOAD, SHOW DATABASES,\n"
            "      REPLICATION SLAVE, REPLICATION CLIENT,\n"
            "      LOCK TABLES\n"
            "  ON *.* TO 'dsql_cdc'@'%';\n"
            "FLUSH PRIVILEGES;",
            language="sql",
        ).classes("w-full text-xs")
        ui.label(  # type: ignore[attr-defined]
            "RELOAD + LOCK TABLES are used only for the consistent initial "
            "snapshot; SELECT reads table data; REPLICATION SLAVE/CLIENT read the "
            "binlog. Scope the grant to specific schemas instead of *.* if your "
            "policy requires it."
        ).classes("text-xs text-gray-500")


def _render_cdc_delete_action(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """A guarded 'Delete CDC infrastructure' (full stack teardown) action."""
    with ui.expansion("Delete CDC infrastructure", icon="delete_forever").classes("w-full"):  # type: ignore[attr-defined]
        inline_hint(  # type: ignore[attr-defined]
            ui,
            "Deletes the entire cdc-stack — MSK, VPC wiring, plugins, IAM role and "
            "any connectors. Use this when you are completely done with CDC, or to "
            "recover from a failed creation. This cannot be undone.",
            tone="neutral",
        )

        def _confirm() -> None:
            _open_cdc_delete_dialog(
                ui, migration_state,
                lambda: _start_cdc_delete(ui, migration_state, job_manager, refresh, session=session),
                session=session,
            )

        ui.button(  # type: ignore[attr-defined]
            "Delete CDC infrastructure", on_click=_confirm, icon="delete_forever"
        ).props("color=negative outline")


# -- Infrastructure input form (BYO-VPC) ------------------------------------

# (label, state-key, placeholder/help, required?) for the Deploy-infra form.
# The form asks for ONLY the VPC id; from it the tool diagnoses egress and either
# reuses existing NAT subnets, has the stack create its own NAT, or (blocked) asks
# for the subnet override below. Everything else is auto-discovered/derived.
# DsqlClusterArn + source host are auto-prefilled silently.
_CDC_INFRA_FIELDS: tuple[tuple[str, str, str, bool], ...] = (
    ("VPC ID", "vpc_id", "vpc-0123456789abcdef0", True),
    (
        "Advanced — connector subnet IDs (leave blank to auto-configure)",
        "connector_subnet_ids",
        "subnet-aaa,subnet-bbb",
        False,
    ),
)


def _cdc_infra_prefill(
    migration_state, session, *, lookup_arn: bool = True
) -> dict[str, str]:
    """Seed the infra form from prior input + the known source/target config.

    Previously-entered values win; otherwise we fill what the tool already knows:
    the source host from the source config, and the DSQL cluster ARN looked up
    (best effort, read-only) from the target endpoint. The VPC id is the only
    thing the customer must type; subnets, plugin bucket/keys, and (for SM auth)
    the secret are resolved at deploy time, not prefilled here.

    ``lookup_arn`` gates the one network call here (DSQL ``GetCluster`` for the
    cluster ARN). The ARN is not a displayed field -- it's only needed at submit
    -- so render passes ``lookup_arn=False`` to stay off the event loop, and the
    submit path resolves it (offloaded via ``run.io_bound``) where a brief wait
    is acceptable. This keeps a render of the CDC step from blocking every
    connected browser session on a control-plane round-trip (Fargate: one loop).
    """
    values = migration_state.cdc_infra_inputs()
    source = getattr(session, "source_config", None) if session else None
    if source is not None and not values.get("source_db_hostname"):
        host = getattr(source, "host", "")
        if host:
            values["source_db_hostname"] = host
    # DsqlClusterArn is NOT derivable from the endpoint hostname; fetch it once
    # (read-only GetCluster) and cache it so the form never asks for it.
    target = getattr(session, "target_config", None) if session else None
    if lookup_arn and target is not None and not values.get("dsql_cluster_arn"):
        endpoint = getattr(target, "cluster_endpoint", "")
        region = getattr(target, "region", None)
        aws_profile = getattr(session, "aws_profile", None)
        if endpoint:
            try:
                from dsql_migrator.core.dsql_metadata import (
                    build_dsql_client,
                    fetch_dsql_cluster_arn,
                )

                client = build_dsql_client(aws_profile, region)
                arn = fetch_dsql_cluster_arn(client, endpoint)
                if arn:
                    values["dsql_cluster_arn"] = arn
            except Exception:  # noqa: BLE001 - fall back to manual entry
                pass
    return values


def _render_cdc_infra_form(ui, migration_state, *, session=None) -> None:
    """Render the minimal infra inputs (just VpcId + an advanced subnet override).

    Everything else is resolved at deploy time: subnets/NAT from the VPC, the
    plugin bucket/keys (uploaded), the DSQL cluster ARN + source host (prefilled),
    and the source-credentials secret (reused from the Connect Secrets-Manager
    reference, or created by the tool from the username/password used on Connect).
    """
    # Render must not block the event loop on a control-plane round-trip (one
    # asyncio loop serves every browser session on Fargate), so skip the DSQL
    # GetCluster ARN lookup here -- it's not a displayed field and the submit
    # path resolves it off-loop just before deploy.
    values = _cdc_infra_prefill(migration_state, session, lookup_arn=False)
    # Persist the prefill so a deploy submitted without edits still has the host.
    migration_state.set_cdc_infra_inputs(values)

    with ui.expansion("Infrastructure inputs", icon="lan", value=True).classes("w-full"):  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Enter your VPC ID. Subnets, the plugin S3 bucket, the DSQL cluster "
            "ARN, the source host, and the source-credentials secret are all "
            "configured automatically."
        ).classes("text-xs text-gray-500")
        for label, key, placeholder, required in _CDC_INFRA_FIELDS:
            field = ui.input(  # type: ignore[attr-defined]
                label=label + (" *" if required else ""),
                value=values.get(key, ""),
                placeholder=placeholder,
            ).classes("w-full text-sm")

            # The connector must reach the source MySQL privately, so the source's
            # own VPC (or one with private connectivity to it) is the safe default.
            if key == "vpc_id":
                ui.label(  # type: ignore[attr-defined]
                    "Recommended: the same VPC as your source MySQL (or one with "
                    "private connectivity to it) — the connector must reach the "
                    "source privately."
                ).classes("w-full text-xs text-gray-500")

            def _save(_e, k=key, f=field) -> None:
                current = migration_state.cdc_infra_inputs()
                current[k] = (f.value or "").strip()
                migration_state.set_cdc_infra_inputs(current)

            field.on("blur", _save)

        # Advanced: the cdc-stack name. Defaults to mysql-dsql-cdc-stack; change it (e.g.
        # mysql-dsql-cdc-orders) to run a SECOND migration's CDC alongside an existing one.
        # Must stay in the mysql-dsql-cdc-* family the deploy role authorizes.
        name_field = ui.input(  # type: ignore[attr-defined]
            label="Advanced — CDC stack name (one per source DB)",
            value=getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME),
            placeholder="mysql-dsql-cdc-stack",
        ).classes("w-full text-sm")

        def _save_stack_name(_e, f=name_field) -> None:
            candidate = (f.value or "").strip()
            if not candidate:
                # Empty -> keep the default; reflect it back in the field.
                f.value = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
                return
            if migration_state.set_cdc_stack_name(candidate):
                return
            # Reject: revert to the current name and explain the rule.
            f.value = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
            ui.notify(  # type: ignore[attr-defined]
                f"CDC stack name must start with '{CDC_STACK_NAME_PREFIX}' and use "
                "only letters, digits and hyphens (e.g. mysql-dsql-cdc-orders).",
                type="warning", position="top",
            )

        name_field.on("blur", _save_stack_name)


# AWS Console (Cloudscape) "Alert" notice: the tone palette and renderer live in
# the shared design module (single source of truth so every page reads the same).
# These module-level aliases preserve the existing in-file call sites/imports.
_NOTICE_STYLE = NOTICE_STYLE
_render_notice = render_notice


def _render_cdc_cost_estimate(ui, *, includes_nat: bool) -> None:
    """Show a ballpark hourly cost line in the deploy dialog (not a quote)."""
    from dsql_migrator.core.cdc import estimate_cdc_hourly_cost

    est = estimate_cdc_hourly_cost(includes_nat=includes_nat)
    _render_notice(
        ui,
        tone="info",
        icon="payments",
        header="Estimated cost",
        body=(
            f"~${est.hourly_low_usd:.2f}–${est.hourly_high_usd:.2f}/hour while deployed "
            f"({'incl.' if includes_nat else 'no'} NAT gateway). {est.caveat}"
        ),
    )


def cdc_deploy_connection_blocker(session) -> Optional[str]:
    """Return why the source/target connections are not ready for a CDC deploy.

    The infra deploy needs a live TARGET (DSQL ARN is derived from it) and, for a
    username/password source, the in-memory source credentials (to create the CDC
    source secret -- the connector cannot read an in-memory password). These are
    NOT restored after an app restart (Property 7), so without this pre-check the
    user clicks Deploy, fills the dialog, hits Deploy again, and only THEN gets a
    "test the source connection first" error. Surfacing it on the dialog (with the
    Deploy button disabled) tells them up front to reconnect before starting.

    Returns ``None`` when ready; otherwise a short actionable reason. A Secrets-
    Manager-auth source needs no in-memory password (the connector reads the
    customer's secret), so only the target is required there. Pure/read-only.
    """
    if session is None:
        return "No session — open the Connect step and connect the source and target."
    if not getattr(session, "has_target", lambda: False)():
        return (
            "The target (Aurora DSQL) connection is not active. Open the Connect "
            "step and test the target connection, then return here to deploy."
        )
    # A Secrets-Manager-auth source supplies its own secret; no in-memory password.
    if getattr(session, "source_secret_id", None):
        return None
    if not getattr(session, "has_source", lambda: False)():
        return (
            "The source (MySQL) connection is not active. Open the Connect step "
            "and test the source connection, then return here to deploy."
        )
    if getattr(session, "source_password", None) is None:
        return (
            "Re-enter the source credentials: the in-memory password is not kept "
            "after a restart (for security), and the CDC source secret is created "
            "from it. Test the source connection on the Connect step, then deploy."
        )
    return None


async def _open_cdc_infra_dialog(ui, migration_state, on_confirm, *, session=None) -> None:
    """Confirm dialog before the (~15-20 min, billable) infrastructure create.

    When ``session`` is given and no manual subnet override is set, runs the
    read-only VPC network diagnosis and shows its outcome (reuse existing subnets,
    create a NAT gateway with the hourly-cost notice, or blocked) so the user
    consents with the network plan + cost in front of them. A pre-flight
    connection check (:func:`cdc_deploy_connection_blocker`) also runs first: if
    the source/target connections are not ready (e.g. after a restart), the dialog
    says so and disables Deploy, so the user reconnects BEFORE starting rather than
    hitting a failure mid-submit.

    The network diagnosis makes EC2 ``Describe*`` calls, so it runs off the event
    loop (``run.io_bound``) -- otherwise opening this dialog would freeze every
    browser session on Fargate (one asyncio loop) for the round-trip.
    """
    from nicegui import run

    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    conn_blocker = cdc_deploy_connection_blocker(session)
    net_message, net_kind, routed_warning = await run.io_bound(
        _diagnose_for_dialog, migration_state, session
    )
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 460px"):  # type: ignore[attr-defined]
        ui.label("Deploy CDC infrastructure").classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            f"This creates the cdc-stack '{stack_name}' (CloudFormation): an MSK "
            "Serverless cluster, connector networking in your VPC, the plugins and "
            "an IAM role. It takes about 15-20 minutes and creates billable AWS "
            "resources. No connectors are created yet — run Start CDC afterwards."
        ).classes("text-sm text-gray-700")
        # Pre-flight connection check: surface a not-ready source/target up front
        # (Deploy is disabled below) so the user reconnects before starting instead
        # of hitting a "test the source connection first" failure mid-submit.
        if conn_blocker:
            _render_notice(
                ui,
                tone="error",
                icon="link_off",
                header="Reconnect before deploying",
                body=conn_blocker,
            )
        # NAT base is only incurred when the stack creates its own NAT ("create");
        # reused existing subnets ("discovered") have no new NAT charge.
        _render_cdc_cost_estimate(ui, includes_nat=(net_kind == "create"))
        if net_message:
            # Only "blocked" is an error (Deploy is disabled below); creating a NAT
            # or reusing subnets is just an FYI, so it reads as a calm info notice.
            net_tone, net_icon = {
                "create": ("info", "lan"),
                "blocked": ("error", "error"),
            }.get(net_kind, ("info", "lan"))
            _render_notice(
                ui, tone=net_tone, icon=net_icon, header="Network", body=net_message
            )
        if routed_warning:
            # A complex-VPC caution (TGW/peering/VPN) for the auto-carved subnets:
            # something to be aware of, not a blocker -> amber "warning" notice.
            _render_notice(
                ui,
                tone="warning",
                header="Check subnet overlap",
                body=routed_warning,
            )

        async def _go() -> None:
            dialog.close()
            # on_confirm is the async _start_cdc_infra_deploy (it offloads its AWS
            # round-trips); await it so failures surface and it isn't a no-op.
            result = on_confirm()
            if inspect.isawaitable(result):
                await result

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
            deploy_btn = ui.button("Deploy", on_click=_go).props("color=primary")  # type: ignore[attr-defined]
            if conn_blocker:
                # Not connected -> the deploy would fail mid-submit; block it and
                # point the user back to Connect.
                deploy_btn.props("disable")
                deploy_btn.tooltip(conn_blocker)
            elif net_kind == "blocked":
                # Cannot auto-resolve egress; block the deploy until fixed/overridden.
                deploy_btn.props("disable")
    dialog.open()


def _diagnose_for_dialog(migration_state, session):
    """Return (message, kind, routed_warning) for the deploy dialog, best-effort.

    kind ∈ {"discovered","create","blocked",""}. ``routed_warning`` is a non-empty
    caution only when the VPC routes off-VPC (TGW/peering/VPN) and the stack will
    auto-carve subnets ("create" mode), else "". Empty message+kind when a manual
    subnet override is set (no diagnosis needed) or context is missing. Read-only.
    """
    if session is None:
        return "", "", ""
    fields = migration_state.cdc_infra_inputs()
    if (fields.get("connector_subnet_ids") or "").strip():
        return "using the connector subnets you provided.", "discovered", ""
    vpc_id = (fields.get("vpc_id") or "").strip()
    target = getattr(session, "target_config", None)
    region = getattr(target, "region", None) if target else None
    if not vpc_id or not region:
        return "", "", ""
    try:
        from dsql_migrator.core.ec2_metadata import (
            build_ec2_client,
            diagnose_cdc_network,
        )

        ec2 = build_ec2_client(getattr(session, "aws_profile", None), region)
        diagnosis = diagnose_cdc_network(ec2, vpc_id)
    except Exception:  # noqa: BLE001 - dialog still works without the preview
        return "", "", ""
    return diagnosis.reason, diagnosis.mode, (diagnosis.routed_cidr_warning or "")


def _open_cdc_start_dialog(ui, migration_state, on_confirm, *, session=None) -> None:
    """Confirm dialog before the (billable, partition-quota-using) Start.

    A pre-flight connection check (:func:`cdc_deploy_connection_blocker`) runs
    first: Start CDC creates the source connector's credentials secret from the
    in-memory source password (not restored after a restart) and needs a live
    target, so if either is missing the dialog says so and disables Start CDC --
    the user reconnects BEFORE starting instead of hitting a failure mid-submit
    (and wasting MSK partition quota on a half-created connector).
    """
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    conn_blocker = cdc_deploy_connection_blocker(session)
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 460px"):  # type: ignore[attr-defined]
        ui.label("Start CDC — create connectors").classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            f"This updates the cdc-stack '{stack_name}' to create the source and "
            "sink connectors for your selected tables and begin streaming. The MSK "
            "cluster, plugins and IAM role are reused (not changed). Each connector "
            "uses MSK Serverless partition quota, so avoid unnecessary retries."
        ).classes("text-sm text-gray-700")
        if conn_blocker:
            _render_notice(
                ui,
                tone="error",
                icon="link_off",
                header="Reconnect before starting CDC",
                body=conn_blocker,
            )

        def _go() -> None:
            dialog.close()
            on_confirm()

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
            start_btn = ui.button("Start CDC", on_click=_go).props("color=primary")  # type: ignore[attr-defined]
            if conn_blocker:
                start_btn.props("disable")
                start_btn.tooltip(conn_blocker)
    dialog.open()


def _open_cdc_stop_dialog(ui, migration_state, on_confirm, *, partial: bool = False) -> None:
    """Confirm dialog before removing the CDC connectors.

    The same backend action (delete the connectors) serves two situations, so the
    wording adapts: while streaming it is a "Stop CDC" (pause); after a failed
    Start that left only one connector (``partial=True``) nothing is streaming, so
    it is framed as cleaning up the leftover connector, not stopping a pipeline.
    """
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    if partial:
        title = "Clean up leftover connector"
        body = (
            f"This updates the cdc-stack '{stack_name}' to remove the leftover CDC "
            "connector from the failed Start (nothing is streaming yet). MSK, the "
            "VPC wiring and the plugins are kept, so you can Start CDC again "
            "afterwards. MSK Connect has no pause, so cleanup means deleting the "
            "connector."
        )
        confirm_label = "Clean up connector"
    else:
        title = "Stop CDC — remove connectors"
        body = (
            f"This updates the cdc-stack '{stack_name}' to delete the two CDC "
            "connectors and stop streaming. MSK, the VPC wiring and the plugins "
            "are kept, so you can restart with Start CDC. MSK Connect has no pause, "
            "so stopping means deleting the connectors."
        )
        confirm_label = "Stop CDC"
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 460px"):  # type: ignore[attr-defined]
        ui.label(title).classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(body).classes("text-sm text-gray-700")  # type: ignore[attr-defined]

        def _go() -> None:
            dialog.close()
            on_confirm()

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
            ui.button(confirm_label, on_click=_go).props("color=orange")  # type: ignore[attr-defined]
    dialog.open()


def _open_cdc_delete_dialog(ui, migration_state, on_confirm, *, session=None) -> None:
    """Type-to-confirm dialog before deleting the whole cdc-stack."""
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    # Was a tool-managed source secret created (password auth), or is the source
    # secret the customer's own Secrets Manager reference (SM auth)?
    uses_sm_auth = bool(getattr(session, "source_secret_id", None))
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 480px"):  # type: ignore[attr-defined]
        ui.label("Delete CDC infrastructure").classes("text-lg font-semibold text-red-700")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "This deletes the entire cdc-stack — MSK, the VPC wiring, plugins, IAM "
            "role and any connectors. This cannot be undone."
        ).classes("text-sm text-gray-700")
        # Show the exact stack name in a copyable mono box so the operator can see
        # (and copy) precisely what to type -- relying on the input placeholder
        # alone is a trap (it looks pre-filled but is empty).
        ui.label("Stack name to confirm:").classes("text-xs text-gray-500 mt-1")  # type: ignore[attr-defined]
        ui.code(stack_name).classes("w-full text-sm")  # type: ignore[attr-defined]
        # Disclose what happens to the source-credentials secret (Property 7: the
        # operator must know whether their DB credentials are removed or retained).
        if uses_sm_auth:
            ui.label(  # type: ignore[attr-defined]
                "Your Secrets Manager source secret is left untouched (the tool "
                "never created it)."
            ).classes("text-xs text-gray-500")
        else:
            from dsql_migrator.core.secrets import cdc_source_secret_name

            secret_name = cdc_source_secret_name(stack_name)
            ui.label(  # type: ignore[attr-defined]
                f"The source-credentials secret the tool created for CDC "
                f"('{secret_name}') will also be scheduled for deletion with a "
                "7-day recovery window, so your database credentials do not linger "
                "in Secrets Manager."
            ).classes("text-xs text-amber-700")
        # Create the widgets first, then wire the live check -- so the handler can
        # safely reference all three even if on_change fires immediately.
        confirm_input = ui.input(  # type: ignore[attr-defined]
            label="Type the stack name above to enable Delete",
            placeholder=stack_name,
        ).classes("w-full")
        mismatch_hint = inline_hint(ui, "", tone="warning")  # type: ignore[attr-defined]
        del_btn = ui.button("Delete", icon="delete_forever").props("color=negative")  # type: ignore[attr-defined]
        del_btn.disable()

        # Gate the Delete button on an exact match, with a live near-miss hint.
        # NiceGUI's on_value_change delivers the current value to Python (the raw
        # DOM "input" event does not), so the gate reacts as the user types.
        def _check(_e=None) -> None:
            typed = (confirm_input.value or "").strip()
            if typed == stack_name:
                del_btn.enable()
                mismatch_hint.set_text("")
            else:
                del_btn.disable()
                mismatch_hint.set_text(
                    f"Doesn't match '{stack_name}' yet." if typed else ""
                )

        confirm_input.on_value_change(_check)

        def _go() -> None:
            if (confirm_input.value or "").strip() != stack_name:
                return
            dialog.close()
            on_confirm()

        del_btn.on("click", _go)
        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
    dialog.open()


def _cdc_target_region(ui, session):
    """Return (target_config, region) or notify + return (None, None) if missing."""
    target = getattr(session, "target_config", None)
    region = getattr(target, "region", None) if target else None
    if not region:
        ui.notify(  # type: ignore[attr-defined]
            "Configure the target connection first.", type="warning", position="top"
        )
        return None, None
    return target, region


def _start_cdc_deploy(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Build connector params and submit the two-pass Start CDC as a background job."""
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_start

    job = _current_job(job_manager, migration_state.job_id)
    watermark = getattr(job, "watermark", None) if job is not None else None
    override = migration_state.cdc_start_override()
    exclusions = migration_state.cdc_lob_exclusions()
    exclude_value = format_column_exclude_list(
        {table: sorted(cols) for table, cols in exclusions.items()}
    )
    exclude_list = exclude_value.split(",") if exclude_value else None
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    mode = migration_state.cdc_start_mode()
    # Guard: a CDC sink requires at least one table (empty -> SinkTopics="" ->
    # connector create fails later with an opaque HTTP 400, ~minutes into a
    # billable deploy). Stop here with an actionable message before submitting.
    if not tables_for_config:
        render_notice(
            ui,
            tone="warning",
            header="Select at least one table before starting CDC",
            body=(
                "No tables are selected for replication, so the CDC sink would have "
                "no topics to write. Choose the tables to migrate, then start CDC."
            ),
        )
        return
    source_config = CdcPipelineOrchestrator().build_source_config(
        "mysql-source",
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        column_exclude_list=exclude_list,
        resume_override=override if (mode == "manual" and override is not None and override.has_coordinates()) else None,
    )
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC
    )
    target, region = _cdc_target_region(ui, session)
    if region is None:
        return
    params = build_cdc_stack_params(
        source_config, sink_config,
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=migration_state.cdc_stack_name,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
    )
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=getattr(session, "aws_profile", None),
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name

    def work(handle) -> None:
        run_cdc_start(
            handle,
            stack_name=stack_name,
            params=params,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
            # Drives the automatic gapless offset seed (Property 11). When the job
            # has no watermark (or it lacks binlog coords) the seeder is not
            # deployed and the source connector starts from the current binlog.
            watermark=watermark,
        )

    job_id = job_manager.submit(work)
    migration_state.set_cdc_deploy_job_id(job_id, kind="start")
    _log_cdc_event("start CDC connectors", detail=f"stack {stack_name}")
    ui.notify("Start CDC submitted — watch the progress below.", type="positive", position="top")  # type: ignore[attr-defined]
    refresh()


@dataclass(frozen=True)
class _CdcSourceSecret:
    """Outcome of resolving the CDC source-credentials secret for a deploy.

    On success ``ok`` is True and ``arn``/``name`` carry the secret coordinates.
    On failure ``ok`` is False and ``error``/``error_type`` carry a credential-free
    notify message (the caller surfaces it). The plaintext password never appears
    on this object or in ``error`` (Property 7).
    """

    ok: bool
    arn: str = ""
    name: str = ""
    error: str = ""
    error_type: str = "warning"


def _resolve_cdc_source_secret(
    session, *, stack_name: str, aws_profile, region, kms_key_id=None
) -> _CdcSourceSecret:
    """Resolve the source-credentials secret the CDC connector reads from.

    - Source connected with Secrets Manager auth -> reuse that secret's ARN/name.
    - Source connected with username/password -> create (or upsert) a tool-managed
      secret from the in-memory credentials so the user never re-enters them. When
      ``kms_key_id`` is set, the created secret is encrypted with that customer-
      managed key instead of the default aws/secretsmanager key.

    Pure of any UI: returns a :class:`_CdcSourceSecret` the caller turns into a
    notify + return on failure.
    """
    source_secret_id = getattr(session, "source_secret_id", None)
    if source_secret_id:
        from dsql_migrator.core.s3_provision import extract_secret_name

        return _CdcSourceSecret(
            ok=True,
            arn=source_secret_id,
            name=extract_secret_name(source_secret_id),
        )

    pw = getattr(session, "source_password", None)
    src_cfg = getattr(session, "source_config", None)
    username = (getattr(src_cfg, "username", None) or "").strip()
    if pw is None or not username:
        return _CdcSourceSecret(
            ok=False,
            error=(
                "Test the source connection first (username/password) so the tool "
                "can create the CDC source secret."
            ),
            error_type="warning",
        )
    try:
        from dsql_migrator.core.secrets import (
            cdc_source_secret_name,
            ensure_source_secret,
        )

        arn = ensure_source_secret(
            stack_name=stack_name,
            username=username,
            password=pw.reveal(),
            aws_profile=aws_profile,
            region=region,
            kms_key_id=kms_key_id,
        )
        return _CdcSourceSecret(
            ok=True, arn=arn, name=cdc_source_secret_name(stack_name)
        )
    except Exception as exc:  # noqa: BLE001
        return _CdcSourceSecret(
            ok=False,
            error=f"Could not create the CDC source secret: {exc}",
            error_type="negative",
        )


async def _start_cdc_infra_deploy(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Validate the BYO-VPC inputs and submit create_stack as a background job.

    The pre-submit resolution makes several blocking AWS round-trips (DSQL
    ``GetCluster`` for the cluster ARN, EC2 ``Describe*`` for the network
    diagnosis, and a Secrets Manager read/create for the source credentials).
    On Fargate a single asyncio loop serves every browser session, so these run
    off the loop via ``run.io_bound`` -- otherwise clicking Deploy would freeze
    the UI for all connected users for the duration of the round-trips.
    """
    from nicegui import run
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_infra_deploy

    target, region = _cdc_target_region(ui, session)
    if region is None:
        return
    aws_profile = getattr(session, "aws_profile", None)

    # Resolve the cluster ARN off the loop (render skipped this GetCluster).
    fields = await run.io_bound(_cdc_infra_prefill, migration_state, session)

    # VpcId is the one always-required field.
    if not (fields.get("vpc_id") or "").strip():
        ui.notify(  # type: ignore[attr-defined]
            "Enter your VPC ID.", type="warning", position="top"
        )
        return

    # DsqlClusterArn is auto-derived (GetCluster) in the prefill; if that lookup
    # failed (e.g. missing dsql:GetCluster permission) we cannot proceed.
    if not (fields.get("dsql_cluster_arn") or "").strip():
        ui.notify(  # type: ignore[attr-defined]
            "Could not resolve the DSQL cluster ARN automatically. Check the "
            "target connection and dsql:GetCluster permission, then retry.",
            type="negative", position="top",
        )
        return

    # --- Click-time network resolution (read-only, off the event loop) ----------
    # (a) An explicit subnet override (Advanced) is used as-is. Otherwise diagnose
    #     the VPC: reuse existing NAT-egress subnets (discovered), let the stack
    #     create its own NAT (create), or fail with guidance (blocked).
    connector_subnet_ids = (fields.get("connector_subnet_ids") or "").strip()
    nat_public_subnet_id = ""
    private_subnet_cidr_a = ""
    private_subnet_cidr_b = ""
    private_subnet_az_a = ""
    private_subnet_az_b = ""
    if not connector_subnet_ids:
        def _diagnose():
            from dsql_migrator.core.ec2_metadata import (
                build_ec2_client,
                diagnose_cdc_network,
            )

            ec2 = build_ec2_client(aws_profile, region)
            return diagnose_cdc_network(ec2, fields["vpc_id"])

        try:
            diagnosis = await run.io_bound(_diagnose)
        except Exception as exc:  # noqa: BLE001
            ui.notify(  # type: ignore[attr-defined]
                f"Could not inspect the VPC network: {exc}",
                type="negative", position="top",
            )
            return
        if diagnosis.mode == "blocked":
            ui.notify(  # type: ignore[attr-defined]
                diagnosis.reason, type="warning", position="top"
            )
            return
        if diagnosis.mode == "discovered":
            connector_subnet_ids = diagnosis.connector_subnet_ids or ""
        else:  # "create" — the stack will make its own subnets + NAT
            nat_public_subnet_id = diagnosis.nat_public_subnet_id or ""
            private_subnet_cidr_a = diagnosis.private_subnet_cidrs[0]
            private_subnet_cidr_b = diagnosis.private_subnet_cidrs[1]
            private_subnet_az_a = diagnosis.availability_zones[0]
            private_subnet_az_b = diagnosis.availability_zones[1]

    # (b) source-credentials secret. The CDC connector can only read source
    #     credentials from Secrets Manager (never an in-memory password), so:
    #     - SM auth on Connect -> reuse that secret's ARN/name.
    #     - username/password auth -> create (or upsert) a tool-managed secret
    #       from the in-memory credentials, so the user never re-enters them.
    #     This does a Secrets Manager round-trip, so it runs off the loop too.
    secret = await run.io_bound(
        _resolve_cdc_source_secret,
        session,
        stack_name=migration_state.cdc_stack_name,
        aws_profile=aws_profile,
        region=region,
        kms_key_id=getattr(migration_state, "cdc_secret_kms_key_id", None),
    )
    if not secret.ok:
        ui.notify(  # type: ignore[attr-defined]
            secret.error, type=secret.error_type, position="top"
        )
        return
    source_secret_arn = secret.arn
    source_secret_name = secret.name

    template_body = _read_cdc_template_body()
    if template_body is None:
        ui.notify(  # type: ignore[attr-defined]
            "Could not read the cdc-stack template (deploy/cdc-stack/cdc-stack.yaml).",
            type="negative", position="top",
        )
        return

    job = _current_job(job_manager, migration_state.job_id)
    watermark = getattr(job, "watermark", None) if job is not None else None
    override = migration_state.cdc_start_override()
    exclusions = migration_state.cdc_lob_exclusions()
    exclude_value = format_column_exclude_list(
        {table: sorted(cols) for table, cols in exclusions.items()}
    )
    exclude_list = exclude_value.split(",") if exclude_value else None
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    mode = migration_state.cdc_start_mode()
    source_config = CdcPipelineOrchestrator().build_source_config(
        "mysql-source",
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        column_exclude_list=exclude_list,
        resume_override=override if (mode == "manual" and override is not None and override.has_coordinates()) else None,
    )
    # Infra-only deploy (DeploySink=false, no connector yet): an empty table
    # selection is allowed here -- SinkTopics is populated later at Start CDC.
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC, allow_empty=True
    )
    # Plugin bucket + keys are left EMPTY here; the deploy job ensures the managed
    # bucket, uploads the bundled artifacts, and patches these in before create.
    params = build_cdc_infra_params(
        source_config, sink_config,
        vpc_id=fields["vpc_id"],
        connector_subnet_ids=connector_subnet_ids,
        nat_public_subnet_id=nat_public_subnet_id,
        private_subnet_cidr_a=private_subnet_cidr_a,
        private_subnet_cidr_b=private_subnet_cidr_b,
        private_subnet_az_a=private_subnet_az_a,
        private_subnet_az_b=private_subnet_az_b,
        source_db_security_group_id=fields.get("source_db_security_group_id", ""),
        plugin_bucket_arn="",
        debezium_plugin_s3_key="",
        dsql_sink_plugin_s3_key="",
        source_db_hostname=fields.get("source_db_hostname", ""),
        source_secret_arn=source_secret_arn,
        source_secret_name=source_secret_name,
        dsql_cluster_arn=fields["dsql_cluster_arn"],
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=migration_state.cdc_stack_name,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
    )
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=aws_profile,
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name

    def work(handle) -> None:
        run_cdc_infra_deploy(
            handle,
            stack_name=stack_name,
            template_body=template_body,
            params=params,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
            region=region,
            aws_profile=aws_profile,
        )

    job_id = job_manager.submit(work)
    migration_state.set_cdc_deploy_job_id(job_id, kind="infra")
    _log_cdc_event("deploy CDC infrastructure", detail=f"stack {stack_name}")
    ui.notify("Infrastructure deploy started (~15-20 min).", type="positive", position="top")  # type: ignore[attr-defined]
    refresh()


def _start_cdc_stop(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """Submit Stop CDC (blank MskBootstrapServers → delete connectors) as a job."""
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_stop

    _, region = _cdc_target_region(ui, session)
    if region is None:
        return
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=getattr(session, "aws_profile", None),
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name

    def work(handle) -> None:
        run_cdc_stop(
            handle,
            stack_name=stack_name,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
        )

    job_id = job_manager.submit(work)
    migration_state.set_cdc_deploy_job_id(job_id, kind="stop")
    _log_cdc_event("stop CDC connectors", detail=f"stack {stack_name}")
    ui.notify("Stop CDC submitted — removing connectors.", type="positive", position="top")  # type: ignore[attr-defined]
    refresh()


def _start_cdc_delete(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """Submit Delete CDC infrastructure (full stack delete) as a job."""
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_delete

    _, region = _cdc_target_region(ui, session)
    if region is None:
        return
    aws_profile = getattr(session, "aws_profile", None)
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=aws_profile,
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name
    # Only the tool-managed (password-auth) secret should be cleaned up; if the
    # source used Secrets Manager auth, the tool never created one and must NOT
    # delete the customer's secret. delete_source_secret targets only the
    # deterministic mysql-dsql-migrator/cdc/<stack>/source name, but gating here keeps
    # the customer's own SM-auth secret entirely out of scope.
    cleanup_secret = not getattr(session, "source_secret_id", None)

    def work(handle) -> None:
        run_cdc_delete(
            handle,
            stack_name=stack_name,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
            region=region,
            aws_profile=aws_profile,
            cleanup_source_secret=cleanup_secret,
        )

    job_id = job_manager.submit(work)
    migration_state.set_cdc_deploy_job_id(job_id, kind="delete")
    _log_cdc_event("delete CDC infrastructure", detail=f"stack {stack_name}")
    ui.notify("Delete CDC infrastructure submitted.", type="warning", position="top")  # type: ignore[attr-defined]
    refresh()


def _render_cdc_deploy_live(ui, migration_state, job_manager, refresh) -> None:
    """Render the active lifecycle job's stages + event log; poll while running.

    The displayed stage labels + terminal messages adapt to which operation
    (``cdc_action_kind``) is running. When the job finishes it re-probes the
    stack phase and triggers a full ``refresh`` so the card flips to the next
    action (e.g. infra-deploy DONE → Start button appears).
    """
    # The deploy log expansion is rebuilt on every 5s poll (it lives inside the
    # refreshable region). Hoist its open/closed state here -- outside the
    # refreshable -- so a user who expands it stays expanded across polls instead
    # of having it snap shut every refresh.
    log_state = {"open": False}

    @ui.refreshable
    def _deploy_live() -> None:  # type: ignore[misc]
        job = _current_job(job_manager, migration_state.cdc_deploy_job_id)
        if job is None:
            return
        kind = getattr(migration_state, "cdc_action_kind", None) or "start"
        # "Refresh now" forces an immediate poll of the same live region (the 5s
        # timer keeps running too); reuses _poll_deploy so a finished job also
        # advances the card, identical to the automatic tick.
        _render_deploy_stages(ui, job, kind, on_refresh=_poll_deploy)
        _render_deploy_log(ui, migration_state.get_cdc_deploy_log(), log_state)
        if job.status in ("PENDING", "RUNNING"):
            ui.timer(_CDC_POLL_INTERVAL_SECONDS, _poll_deploy, once=True)  # type: ignore[attr-defined]
            return
        done_msg, fail_msg = _CDC_ACTION_TERMINAL.get(
            kind, _CDC_ACTION_TERMINAL["start"]
        )
        done_noun, fail_noun = _CDC_ACTION_NOUN.get(
            kind, _CDC_ACTION_NOUN["start"]
        )
        total = _deploy_total_duration(job)
        if job.status == "DONE":
            render_notice(
                ui,
                tone="success",
                header=f"{done_noun} complete" + (f" — took {total}" if total else ""),
                body=done_msg,
            )
        elif job.status == "FAILED":
            # A job reconciled to FAILED purely because the app restarted mid-run is
            # NOT a real failure: the AWS/CloudFormation work (e.g. a connector
            # still CREATING) usually kept going. Show a calm "re-checking live
            # state" notice (not a red "failed"), and let the connector discovery /
            # stack-phase probe -- which runs on every CDC-step render -- surface the
            # true state (Provisioning / Streaming / Incomplete) instead. The whole
            # card refreshes off that, so this interrupted job-card stops driving the
            # verdict.
            job_error = None
            try:
                job_error = job_manager.get_error(migration_state.cdc_deploy_job_id)
            except JobNotFoundError:
                job_error = None
            if is_interrupted_by_restart(job_error):
                render_notice(
                    ui,
                    tone="info",
                    icon="autorenew",
                    header="Re-checking CDC status after a restart",
                    body=(
                        "The app restarted while this operation was running. The "
                        "work continues on AWS — re-reading the live connector and "
                        "stack state now; the pipeline status below updates "
                        "automatically."
                    ),
                )
            else:
                render_notice(
                    ui,
                    tone="error",
                    header=f"{fail_noun} failed"
                    + (f" (after {total})" if total else ""),
                    body=fail_msg,
                )

    def _poll_deploy() -> None:
        job = _current_job(job_manager, migration_state.cdc_deploy_job_id)
        # When the operation finishes, re-probe the stack so the lifecycle card
        # advances to the next action; otherwise just refresh the live region.
        if job is not None and job.status not in ("PENDING", "RUNNING"):
            refresh()
        else:
            _deploy_live.refresh()

    _deploy_live()


def _render_deploy_stages(ui, job, kind: str = "start", on_refresh=None) -> None:
    """Render each stage (job chunk) of the running operation as an icon row.

    Each not-yet-finished stage shows a rough ETA hint (``~3 min``) so the user
    knows what to expect; the in-progress stage also shows live elapsed time. A
    total estimate for the whole operation is shown under the title.

    While the operation runs, the title row also shows an "Auto-refreshing"
    caption and (when ``on_refresh`` is given) a manual Refresh button on the
    right: connector creation takes 10-20 min, so without a visible live signal a
    user can mistake the long-running stage for a frozen UI. The button lets them
    force an immediate poll; the caption reassures them it updates on its own.
    """
    labels = _CDC_STAGE_LABELS.get(kind, _CDC_STAGE_LABELS["start"])
    etas = _CDC_STAGE_ETA_SECONDS.get(kind, {})
    now = datetime.now(timezone.utc)
    running = job.status in ("PENDING", "RUNNING")
    with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        ui.label(_CDC_ACTION_TITLE.get(kind, "Progress")).classes(  # type: ignore[attr-defined]
            "text-sm font-semibold"
        )
        # Sum the estimates of stages not yet DONE for a whole-operation hint.
        remaining_total = sum(
            etas.get(c.chunk_id, 0) for c in job.chunks if c.status != "DONE"
        )
        total_hint = _format_eta_hint(remaining_total)
        if total_hint and running:
            ui.label(f"est. {total_hint} remaining").classes(  # type: ignore[attr-defined]
                "text-xs text-gray-400"
            )
        if running:
            # Right-aligned live signal + manual refresh: a long connector-create
            # stage looks frozen otherwise. The caption proves it polls on its own;
            # the button forces an immediate update for the impatient.
            ui.space()  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                ui.spinner(size="xs").props("color=primary")  # type: ignore[attr-defined]
                ui.label("Auto-refreshing…").classes("text-xs text-gray-400")  # type: ignore[attr-defined]
                if on_refresh is not None:
                    ui.button(on_click=on_refresh).props(  # type: ignore[attr-defined]
                        "flat dense round size=sm icon=refresh"
                    ).tooltip("Refresh now")
    for chunk in job.chunks:
        icon, color = _CDC_DEPLOY_STAGE_STYLE.get(chunk.status, _CDC_DEPLOY_STAGE_STYLE["PENDING"])
        label = labels.get(chunk.chunk_id, chunk.chunk_id)
        in_progress = chunk.status == "IN_PROGRESS"
        # Emphasize the running stage: a live animated hourglass spinner (instead of
        # the static icon) plus a bold, pulsing primary label, so the eye is drawn
        # to exactly which step is happening now.
        with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            if in_progress:
                ui.spinner("hourglass", size="sm", color="primary")  # type: ignore[attr-defined]
            else:
                ui.icon(icon, color=color).classes("text-base")  # type: ignore[attr-defined]
            label_classes = (
                "text-sm font-semibold text-primary animate-pulse"
                if in_progress
                else "text-xs text-gray-700"
            )
            ui.label(label).classes(label_classes)  # type: ignore[attr-defined]
            # The running stage shows live elapsed; pending stages show their ETA.
            if in_progress and chunk.started_at is not None:
                elapsed = (now - chunk.started_at).total_seconds()
                eta = etas.get(chunk.chunk_id, 0)
                suffix = f" / {_format_eta_hint(eta)}" if _format_eta_hint(eta) else ""
                ui.label(  # type: ignore[attr-defined]
                    f"{format_duration(max(0.0, elapsed))} elapsed{suffix}"
                ).classes("text-xs text-primary font-medium")
            elif chunk.status == "PENDING":
                hint = _format_eta_hint(etas.get(chunk.chunk_id, 0))
                if hint:
                    ui.label(hint).classes("text-xs text-gray-400")  # type: ignore[attr-defined]


def _render_deploy_log(ui, log_lines, log_state=None) -> None:
    """Render the timestamped deploy log lines, newest last (logging style).

    ``log_state`` is an optional ``{"open": bool}`` dict owned by the caller (it
    must outlive this render). The expansion opens to that remembered state and
    writes its toggles back, so the live poll's 5s rebuild does not collapse a log
    the user opened.
    """
    if not log_lines:
        return
    if log_state is None:
        log_state = {"open": False}

    def _remember(e) -> None:
        log_state["open"] = bool(e.value)

    with ui.expansion(  # type: ignore[attr-defined]
        f"Deploy log ({len(log_lines)} lines)",
        icon="terminal",
        value=log_state["open"],
        on_value_change=_remember,
    ).classes("w-full"):
        # ASCII-only separator ("-"), and sanitize each message, so the monospace
        # ui.code font never renders a missing-glyph box (tofu) for punctuation
        # like the em-dash / ellipsis some deploy messages contained.
        text = "\n".join(
            f"{ts.strftime('%H:%M:%S')} - {_ascii_log(msg)}" for ts, msg in log_lines
        )
        ui.code(text).classes("w-full text-xs")  # type: ignore[attr-defined]


# Quasar badge color per Full Load chunk state, for the per-table status table.
_MIGRATION_FL_BADGE: dict[str, str] = {
    "DONE": "positive",
    "IN_PROGRESS": "primary",
    "FAILED": "negative",
    "PENDING": "grey",
    "": "grey",
}


def _render_migration_table_status(
    ui, migration_state, job_manager, session, *, inventory=None
) -> None:
    """Per-table consistency view: Full Load vs CDC, source-vs-target, DLQ.

    Answers the operator's real question -- "did CDC replicate everything, is
    anything missing?". It separates the one-shot Full Load row count from the net
    rows CDC has applied since (``target − Full Load``), shows the source-vs-target
    consistency verdict, and surfaces per-table quarantined (DLQ) events -- changes
    that did NOT reach the target. MSK Connect publishes no per-table replicated-row
    metric, so the source/target counts come from a direct COUNT(*) on each side;
    that scans the source, so it is an explicit "Refresh counts" action (not an
    auto-poll). Shown once a Full Load job exists (the CDC step is reached only
    after Full Load in the combined flow).
    """
    table_names = _migration_status_tables(migration_state, job_manager)
    if not table_names:
        return

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("table_rows", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Per-table migration status").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold"
            )
            ui.space()  # type: ignore[attr-defined]
            fetched_at = getattr(migration_state, "row_counts_fetched_at", None)
            if fetched_at is not None:
                ui.label(  # type: ignore[attr-defined]
                    f"counts as of {fetched_at.strftime('%H:%M:%S')} UTC"
                ).classes("text-xs text-gray-400")

        @ui.refreshable
        def _status_table() -> None:  # type: ignore[misc]
            job = _current_job(job_manager, migration_state.job_id)
            # Per-table quarantined (DLQ) counts from the single error log: change
            # events that did NOT reach the target -- the "missing" the customer
            # cares about for consistency.
            dlq_counts: dict[str, int] = {}
            if job is not None:
                try:
                    dlq_counts = dict(
                        migration_state.error_log.summary(job.job_id).errors_by_table
                    )
                except Exception:  # noqa: BLE001 - best-effort, never break the table
                    dlq_counts = {}
            rows_model = build_migration_table_status(
                table_names,
                full_load_job=job,
                target_counts=getattr(migration_state, "row_count_target", {}),
                source_counts=getattr(migration_state, "row_count_source", {}),
                dlq_counts=dlq_counts,
                source_max_pk=getattr(migration_state, "row_max_pk_source", {}),
                target_max_pk=getattr(migration_state, "row_max_pk_target", {}),
            )
            # Columns separate the one-shot Full Load contribution from the ongoing
            # CDC contribution, then show the source-vs-target consistency verdict
            # and anything quarantined (not applied).
            columns = [
                {"name": "table", "label": "Table", "field": "table", "align": "left"},
                {"name": "fl", "label": "Full Load", "field": "fl", "align": "left"},
                {"name": "fl_rows", "label": "Full Load rows", "field": "fl_rows"},
                {
                    "name": "cdc_net",
                    "label": "Net rows since Full Load",
                    "field": "cdc_net",
                },
                {"name": "source", "label": "Source rows", "field": "source"},
                {"name": "target", "label": "Target rows", "field": "target"},
                {"name": "stream", "label": "Stream lag (newest)", "field": "stream"},
                {"name": "dlq", "label": "Quarantined", "field": "dlq"},
                {
                    "name": "consistency",
                    "label": "Consistency",
                    "field": "consistency",
                    "align": "left",
                },
            ]

            def _fmt(n: "Optional[int]") -> str:
                return "—" if n is None else f"{n:,}"

            def _fmt_signed(n: "Optional[int]") -> str:
                if n is None:
                    return "—"
                return f"+{n:,}" if n > 0 else f"{n:,}"

            # User-facing consistency label + the verdict key (drives the badge color).
            _CONSISTENCY_LABEL = {
                "consistent": "✓ consistent",
                "quarantined": "⚠ data quarantined",
                "behind": "replicating…",
                "gap": "⚠ rows missing",
                "ahead": "target ahead",
                "unknown": "refresh to check",
            }
            table_rows = []
            for r in rows_model:
                source_label = _fmt(r.source_rows)
                if r.source_estimate and r.source_rows is not None:
                    source_label += " (est.)"
                # Stream lag: whether the newest source row (high-water PK) has
                # landed on the target. Distinguishes a lagging stream from a
                # caught-up-but-gappy one.
                if r.stream_caught_up is True:
                    stream = "✓ caught up"
                elif r.stream_caught_up is False:
                    stream = f"{r.pk_gap:,} behind"
                else:
                    stream = "—"
                verdict = r.consistency
                table_rows.append(
                    {
                        "table": r.table,
                        "fl": r.full_load_state or "—",
                        "fl_state": r.full_load_state or "",
                        "fl_rows": _fmt(r.full_load_rows),
                        "cdc_net": _fmt_signed(r.cdc_applied_net),
                        "source": source_label,
                        "target": _fmt(r.target_rows),
                        "stream": stream,
                        "dlq": _fmt(r.dlq_count) if r.dlq_count else "0",
                        "consistency": _CONSISTENCY_LABEL.get(verdict, verdict),
                        "verdict": verdict,
                    }
                )
            # `dense` + a high rows-per-page (no footer pager) + `flat` keep the
            # table compact and render every table inline, so it grows with content
            # instead of showing a bottom pagination bar / inner scroll. The columns
            # carry short labels so the row fits the card width without a horizontal
            # scrollbar at the bottom.
            table = ui.table(  # type: ignore[attr-defined]
                columns=columns,
                rows=table_rows,
                row_key="table",
                pagination={"rowsPerPage": 0},
            ).props("dense flat").classes("w-full")
            # Color the Full Load state as a badge.
            table.add_slot(
                "body-cell-fl",
                r"""
                <q-td :props="props">
                  <q-badge v-if="props.row.fl_state"
                    :color="{'DONE':'positive','IN_PROGRESS':'primary','FAILED':'negative','PENDING':'grey'}[props.row.fl_state] || 'grey'"
                    :label="props.value" outline />
                  <span v-else>—</span>
                </q-td>
                """,
            )
            # Color the consistency verdict (green=consistent, red=quarantined,
            # amber=behind/ahead, grey=unknown) so a problem is obvious at a glance.
            table.add_slot(
                "body-cell-consistency",
                r"""
                <q-td :props="props">
                  <q-badge
                    :color="{'consistent':'positive','quarantined':'negative','gap':'negative','behind':'warning','ahead':'warning','unknown':'grey'}[props.row.verdict] || 'grey'"
                    :label="props.value" outline />
                </q-td>
                """,
            )

        async def _refresh_counts() -> None:
            # The source/target counts are a direct COUNT(*)/MAX(pk) on each side and
            # can take seconds on a large source, so give clear in-progress feedback:
            # the button is disabled and relabelled "Refreshing…" (its text stays
            # legible -- we do NOT use Quasar `loading`, which replaces the label
            # with a bare spinner), and an ongoing top notification carries the
            # spinner. A toast confirms when the counts land. Without this the click
            # looked like a no-op (only a few cells changed at the end).
            from nicegui import run

            refresh_btn.set_text("Refreshing…")
            refresh_btn.disable()
            # An "ongoing"-type ui.notify has timeout=0 and returns no handle, so it
            # never auto-dismisses -- it would linger forever. Use ui.notification,
            # which returns a handle we explicitly .dismiss() when the fetch ends.
            progress = ui.notification(  # type: ignore[attr-defined]
                "Counting source and target rows…",
                type="ongoing",
                position="top",
                spinner=True,
                timeout=None,
            )
            try:
                fetched = await run.io_bound(
                    _fetch_migration_row_counts,
                    migration_state,
                    session,
                    table_names,
                    inventory,
                )
                if fetched is not None:
                    (
                        source_counts,
                        target_counts,
                        source_max_pk,
                        target_max_pk,
                        at,
                        source_available,
                    ) = fetched
                    migration_state.set_row_counts(
                        source=source_counts,
                        target=target_counts,
                        source_max_pk=source_max_pk,
                        target_max_pk=target_max_pk,
                        fetched_at=at,
                    )
                    if source_available:
                        ui.notify("Source/target counts updated.", type="positive", position="top")  # type: ignore[attr-defined]
                    else:
                        # Target read OK, but the source couldn't be read -- almost
                        # always a restored session with no source password (creds
                        # are never persisted). Tell the user how to get source counts.
                        ui.notify(  # type: ignore[attr-defined]
                            "Target counts updated. Source counts need the source "
                            "connection — re-enter it on the Connect step to compare "
                            "source vs target.",
                            type="warning",
                            position="top",
                            multi_line=True,
                        )
                else:
                    ui.notify(  # type: ignore[attr-defined]
                        "Could not read the counts — check the source/target "
                        "connections.",
                        type="warning",
                        position="top",
                    )
            finally:
                # Always dismiss the ongoing notification so it does not linger.
                try:
                    progress.dismiss()  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001 - already gone / page rebuilt
                    pass
                # The page may have been rebuilt while awaiting (NiceGUI #3028);
                # only touch the button/table if they still exist.
                if not getattr(refresh_btn, "is_deleted", False):
                    refresh_btn.set_text("Refresh source/target counts")
                    refresh_btn.enable()
                    _status_table.refresh()

        # Top: the one thing to know before reading the numbers -- the source side
        # is a scan-free estimate, so it adds no load on a TB-scale source but is
        # approximate (Validation does the exact reconciliation).
        render_notice(
            ui,
            tone="info",
            header="Source rows are an estimate (no load on the source)",
            body=(
                "Refresh reads the source row counts from information_schema "
                "(table_rows) — a scan-free estimate, so it adds no load even on a "
                "TB-scale source, but it can drift from the exact count under heavy "
                "writes. The target counts are exact. For an authoritative row/"
                "checksum reconciliation, run Validation (Step 4)."
            ),
        )
        refresh_btn = ui.button(  # type: ignore[attr-defined]
            "Refresh source/target counts",
            on_click=_refresh_counts,
            icon="sync",
        ).props("color=primary outline size=sm")
        _status_table()
        # Below the table (lower importance): the column legend. A plain transparent
        # block (not a tinted notice box) of simple bullet points -- it is reference
        # help, not a status to act on, so it stays visually quiet.
        with ui.column().classes("gap-1 w-full mt-1"):  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                ui.icon("info").classes("text-sky-600 text-base")  # type: ignore[attr-defined]
                ui.label("How to read this table").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-gray-900"
                )
            _legend = [
                "Full Load rows — rows the one-shot snapshot loaded.",
                "Net rows since Full Load — the NET change in target row count "
                "since Full Load (target − Full Load), not a count of CDC events: "
                "inserts add, deletes subtract, updates don't change it. So it is "
                "negative when the stream net-deleted rows (e.g. more deletes than "
                "inserts) — that is expected, not an error.",
                "Source rows — scan-free estimate. Target rows — exact count.",
                "Stream lag — newest row (PK) on each side: “✓ caught up” vs “N behind”.",
                "Consistency — ✓ consistent = counts match · replicating… = catching "
                "up · ⚠ rows missing = newest landed but rows gone mid-stream · "
                "⚠ data quarantined = DLQ has un-applied events.",
                "Anything other than ✓ means investigate.",
            ]
            for _line in _legend:
                with ui.row().classes("items-start gap-2 no-wrap w-full pl-1"):  # type: ignore[attr-defined]
                    ui.label("•").classes("text-xs text-gray-400")  # type: ignore[attr-defined]
                    ui.label(_line).classes("text-xs text-gray-600 flex-1")  # type: ignore[attr-defined]


def _render_cdc_live_monitoring(ui, migration_state, job_manager) -> None:
    """Live connector health + DLQ, polled read-only from MSK Connect.

    Mirrors the Full Load poll chain: a refreshable region arms a one-shot timer
    at the END of its render, and the poll re-renders + re-arms -- a
    self-perpetuating single-shot chain that avoids the "parent slot deleted"
    crash a repeating timer causes. Meaningful only once streaming, so it is
    placed after the start action.
    """
    ui.label("Live status").classes("text-sm font-semibold")  # type: ignore[attr-defined]

    @ui.refreshable
    def _cdc_live() -> None:  # type: ignore[misc]
        view = _cdc_status_view(migration_state, job_manager)
        if view is not None:
            _render_cdc_pipeline_health(
                ui, view, getattr(migration_state, "cdc_activity", None)
            )
            _render_cdc_dlq_panel(
                ui, migration_state, job_manager, view, on_refresh=_poll_cdc
            )
        else:
            controller = getattr(migration_state, "cdc_controller", None)
            names = getattr(migration_state, "cdc_connector_names", [])
            if controller is None or not names:
                ui.label(  # type: ignore[attr-defined]
                    "Live connector health and replication lag appear here once "
                    "the cdc-stack connectors are detected."
                ).classes("text-xs text-gray-500")
        if getattr(migration_state, "cdc_controller", None) is not None and getattr(
            migration_state, "cdc_connector_names", []
        ):
            ui.timer(_CDC_POLL_INTERVAL_SECONDS, _poll_cdc, once=True)  # type: ignore[attr-defined]

    async def _poll_cdc() -> None:
        # The MSK Connect + CloudWatch reads are blocking network I/O; run them on
        # a worker thread (run.io_bound) so they never block the NiceGUI event loop.
        # Blocking the loop here previously starved the WebSocket keep-alive and
        # made the browser drop the connection. The pure view-build + state write
        # happens back on the loop after the fetch returns.
        from nicegui import run

        try:
            fetched = await run.io_bound(_fetch_cdc_status, migration_state)
        except Exception:  # noqa: BLE001 - keep the last good view on any error
            fetched = None
        if fetched is not None:
            _apply_cdc_status(migration_state, fetched)
        _cdc_live.refresh()

    _cdc_live()


def _render_cdc_pipeline_health(
    ui, status_view: LoadStatusView, activity: "Optional[CdcActivitySummary]"
) -> None:
    """Render the combined "Pipeline health" card: connector health + change flow.

    Connector state/lag and the change-flow throughput both answer "is the pipeline
    running and moving data right now?", so they share one card (a labelled
    connector-health group, then a change-flow group), rather than two adjacent
    cards the operator has to mentally join. The DLQ stays its own card (it is about
    data set aside, a different concern). Renders nothing when there is no connector
    health to show (pre-streaming).
    """
    rows = connector_health_rows(
        status_view.connector_states, lag_seconds=status_view.lag_seconds
    )
    if not rows:
        return
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("monitor_heart", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Pipeline health").classes("text-sm font-semibold")  # type: ignore[attr-defined]

        # --- Connectors -------------------------------------------------------
        ui.label("Connectors").classes(  # type: ignore[attr-defined]
            "text-xs font-semibold text-gray-500 uppercase tracking-wide"
        )
        if status_view.caught_up_to is not None:
            ui.label(  # type: ignore[attr-defined]
                f"Caught up to {status_view.caught_up_to.isoformat()}"
            ).classes("text-xs text-gray-500")
        _badge_color = {"ok": "positive", "warn": "warning", "bad": "negative"}
        for row in rows:
            _border, _bg, icon_color, icon = _CDC_TONE_STYLE.get(
                row.tone, _CDC_TONE_STYLE["warn"]
            )
            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.icon(icon, color=icon_color).classes("text-base")  # type: ignore[attr-defined]
                with ui.column().classes("gap-0"):  # type: ignore[attr-defined]
                    # Friendly role label is primary; raw connector id is a small
                    # secondary line for reference/debugging.
                    ui.label(row.label or row.name).classes(  # type: ignore[attr-defined]
                        "text-sm font-medium"
                    )
                    ui.label(row.name).classes("text-xs text-gray-400 font-mono")  # type: ignore[attr-defined]
                ui.space()  # type: ignore[attr-defined]
                ui.badge(  # type: ignore[attr-defined]
                    row.state, color=_badge_color.get(row.tone, "grey")
                ).props("outline")
            ui.label(row.detail).classes("text-xs text-gray-500 ml-6")  # type: ignore[attr-defined]

        # --- Change flow ------------------------------------------------------
        if activity is not None:
            ui.separator().classes("my-1")  # type: ignore[attr-defined]
            ui.label("Change flow").classes(  # type: ignore[attr-defined]
                "text-xs font-semibold text-gray-500 uppercase tracking-wide"
            )
            ui.label(  # type: ignore[attr-defined]
                "Whether changes are still streaming from the source to the target. "
                "When you quiesce the source for cutover, watch this drop to idle — "
                "it means the pipeline has drained."
            ).classes("text-xs text-gray-500")
            _render_change_flow_status(ui, activity)


def _render_change_flow_status(ui, activity: "CdcActivitySummary") -> None:
    """The change-flow state line + CloudWatch throughput (inner block, no card).

    Pure information for the operator's cutover judgement — NOT a gate or a
    recommendation. Honest about "unknown": when a rate is unavailable it is shown
    as such, never as 0/idle.
    """
    def _fmt(rate: "Optional[float]") -> str:
        return f"{rate:.2f}/s" if rate is not None else "unknown"

    with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        if activity.idle is True:
            ui.icon("pause_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
            ui.label("No changes flowing — pipeline idle").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-700"
            )
        elif activity.idle is False:
            ui.icon("sync", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Streaming — changes are flowing").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-700"
            )
        else:
            ui.icon("help_outline", color="grey").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Activity unknown").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
    ui.label(  # type: ignore[attr-defined]
        f"source poll: {_fmt(activity.source_poll_rate)} · "
        f"sink send: {_fmt(activity.sink_send_rate)}  (CloudWatch, ~last few min)"
    ).classes("text-xs text-gray-500 ml-6")


# Health level (assess_dlq_health) -> notice tone + status badge (label, quasar
# color) for the DLQ panel, so the panel speaks the same severity language as the
# rest of the app (NOTICE_STYLE / status badges) instead of a bespoke palette:
# clean = success, sporadic poison = info, approaching threshold = warning,
# systematic = error.
_DLQ_LEVEL_TONE: dict[str, str] = {
    "ok": "success",
    "warn": "warning",
    "alarm": "error",
}


def _dlq_panel_tone(health) -> str:
    """Map a :class:`DlqHealth` to a notice tone.

    ``ok`` with a non-zero depth is downgraded to ``info`` (sporadic, isolated
    poison is an FYI, not a success), while a truly clean stream (depth 0) stays
    ``success``. Calibrated to the project's severity rules (info/warning/error).
    """
    if health.level == "ok":
        return "success" if health.depth == 0 else "info"
    return _DLQ_LEVEL_TONE.get(health.level, "info")


def _render_cdc_dlq_panel(
    ui, migration_state, job_manager, status_view: LoadStatusView, on_refresh=None
) -> None:
    """Render the dead-letter queue as one cohesive, AWS-console-style card.

    A tinted notice band (tone from DLQ health -- :func:`_dlq_panel_tone`) carries
    a leading status icon, the "Dead-letter queue (poison records)" title, a depth
    badge, an optional refresh control, and the health message; below it sit the
    per-table breakdown, the scrollable record table (visible even at high row
    counts via paging), and the download. ``on_refresh`` (optional) re-polls the
    sink's CloudWatch dead-letter log on demand instead of waiting for the ~5s
    auto-poll.
    """
    health = assess_dlq_health(status_view.dlq_depth)
    if health is None:
        return
    tone = _dlq_panel_tone(health)
    bg, border, icon_color, default_icon = NOTICE_STYLE.get(tone, NOTICE_STYLE["info"])
    with ui.column().classes(  # type: ignore[attr-defined]
        f"w-full gap-2 rounded-md border {border} {bg} p-3"
    ):
        # Header band: icon + title + depth badge (left), refresh (right).
        with ui.row().classes("w-full items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            ui.icon(default_icon).classes(f"{icon_color} text-lg")  # type: ignore[attr-defined]
            ui.label("Dead-letter queue (poison records)").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-900"
            )
            _badge_color = (
                "negative"
                if tone == "error"
                else "warning"
                if tone == "warning"
                else "grey-6"
                if health.depth == 0
                else "primary"
            )
            ui.badge(  # type: ignore[attr-defined]
                f"{health.depth} quarantined"
            ).props(f"color={_badge_color}")
            ui.space()  # type: ignore[attr-defined]
            if on_refresh is not None:
                ui.button(on_click=on_refresh).props(  # type: ignore[attr-defined]
                    "flat dense round size=sm icon=refresh"
                ).tooltip("Refresh dead-letter records from CloudWatch")
        ui.label(health.message).classes("text-xs text-gray-700")  # type: ignore[attr-defined]
        _render_cdc_dlq_breakdown(ui, status_view)
        # CDC commonly has no Full Load job_id this session, so key the record list /
        # download off the same stable CDC key the fold used (cdc_error_log_key) --
        # not _current_job, which would be None and hide everything.
        log_key = cdc_error_log_key(migration_state)
        _render_cdc_dlq_records(ui, migration_state, log_key)
        if health.depth > 0:
            _render_cdc_error_download(ui, migration_state, log_key)


def _render_cdc_dlq_breakdown(ui, status_view: LoadStatusView) -> None:
    """Show which tables produced DLQ/error records, so a poison source is found.

    Reads the per-table counts already on the view's
    :class:`~dsql_migrator.core.models.ErrorLogSummary` (no new query) and renders
    them as compact, always-visible per-table chips (table xN), sorted by count.
    Chips (not a collapsible sub-table) keep the at-a-glance "where is the poison
    coming from" answer visible without a click and survive the ~5s re-render
    without snapping shut. Nothing shown when there are no per-table counts (a
    clean stream or errors not yet attributed to a table).
    """
    summary = status_view.error_summary
    by_table = summary.errors_by_table if summary is not None else {}
    if not by_table:
        return
    ordered = sorted(by_table.items(), key=lambda kv: (-kv[1], kv[0]))
    with ui.row().classes("w-full items-center gap-1 flex-wrap"):  # type: ignore[attr-defined]
        ui.label("By table:").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
        for table, count in ordered:
            ui.badge(f"{table} ×{count}").props("color=grey-7 outline")  # type: ignore[attr-defined]


# Cap the inline DLQ record list so a flood of poison rows never renders an
# unbounded table in the browser; the full set is always in the downloadable log.
_DLQ_RECORD_LIST_LIMIT = 200
# Rows shown per page in the record table; paging keeps a large DLQ readable
# (a fixed, scrollable viewport) instead of an ever-growing wall of rows.
_DLQ_RECORD_PAGE_SIZE = 10


def _render_cdc_dlq_records(ui, migration_state, log_key: str) -> None:
    """List the individual quarantined records (time, table, SQLSTATE, reason).

    Reads the per-record rows already in the single
    :class:`~dsql_migrator.core.error_log.ErrorLogStore` under ``log_key`` (the CDC
    error-log key -- the Full Load job id, or the stable per-stack fallback when no
    Full Load ran this session; the same rows the download renders, no new query),
    newest first, so the operator sees WHAT was dead-lettered, not just a count.
    Rendered as a paged, bounded-height table so a high record count stays readable
    (a fixed viewport with paging) rather than an unbounded wall of rows.
    Credential-free by construction (table / SQLSTATE / reason + SQL TEMPLATE with
    ``?`` placeholders, never row values -- Property 7). Nothing is shown when there
    are no records yet (clean stream).
    """
    if not log_key:
        return
    try:
        records = migration_state.error_log.records(log_key)
    except Exception:  # noqa: BLE001 - advisory list; never break the panel
        records = []
    if not records:
        return
    ordered = sorted(
        records,
        key=lambda r: r.occurred_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    shown = ordered[:_DLQ_RECORD_LIST_LIMIT]
    rows = [
        {
            "table": r.table,
            "code": r.error_code or "—",
            "message": r.message,
            "when": r.occurred_at.strftime("%Y-%m-%d %H:%M:%S")
            if r.occurred_at
            else "—",
        }
        for r in shown
    ]
    extra = len(ordered) - len(shown)
    caption = (
        f"Quarantined records ({len(ordered)}"
        + (f", showing latest {len(shown)}" if extra > 0 else "")
        + ")"
    )
    # Always-visible (NOT a ui.expansion): the CDC panel re-renders on every ~5s
    # poll, which would re-create an expansion in its default (closed) state and
    # snap a just-opened list shut. A paged ui.table keeps a fixed, scrollable
    # viewport so even hundreds of rows stay readable and visible.
    ui.label(caption).classes("text-xs font-medium text-gray-700 mt-1")  # type: ignore[attr-defined]
    table = ui.table(  # type: ignore[attr-defined]
        columns=[
            {"name": "when", "label": "Time", "field": "when", "align": "left",
             "sortable": True},
            {"name": "table", "label": "Table", "field": "table", "align": "left",
             "sortable": True},
            {"name": "code", "label": "SQLSTATE", "field": "code", "align": "left",
             "sortable": True},
            {"name": "message", "label": "Reason", "field": "message", "align": "left"},
        ],
        rows=rows,
        row_key="message",
        pagination={"rowsPerPage": _DLQ_RECORD_PAGE_SIZE, "sortBy": "when",
                    "descending": True},
    ).classes("w-full text-xs bg-white rounded-md")
    # A search box so a specific table/SQLSTATE/reason is findable in a large DLQ.
    table.props('flat bordered dense')
    with table.add_slot("top-left"):  # type: ignore[attr-defined]
        ui.input(placeholder="Filter records").props(  # type: ignore[attr-defined]
            "dense clearable borderless"
        ).bind_value(table, "filter")


def _render_cdc_error_download(ui, migration_state, log_key: str) -> None:
    """Offer the single error log (DLQ-sourced rows included) as a download."""
    summary = migration_state.error_log.summary(log_key)
    if summary.total_errors <= 0:
        return
    # A filesystem-safe slug for the filename (the CDC fallback key is "cdc:<stack>").
    safe = log_key.replace(":", "_").replace("/", "_")

    def _download_log() -> None:
        try:
            payload = migration_state.error_log.render_log(log_key)
            ui.download.content(  # type: ignore[attr-defined]
                payload, f"cdc_error_log_{safe}.ndjson", "application/x-ndjson"
            )
        except Exception as exc:  # noqa: BLE001 - surface instead of silent
            _LOGGER.exception("Failed to render/download CDC error log")
            ui.notify(  # type: ignore[attr-defined]
                f"Could not generate the error log: {exc}", type="negative"
            )

    ui.button(  # type: ignore[attr-defined]
        "Download error log (NDJSON)", on_click=_download_log
    ).props("outline dense")


def _render_cdc_lob_exclusion_panel(
    ui, migration_state, inventory: Optional[SourceInventory], refresh
) -> None:
    """Render the explicit, opt-in oversized-LOB column exclusion (H13).

    Lists the columns the evaluation flagged as able to exceed the DSQL 1 MiB
    per-value limit and lets the user exclude them from capture (Debezium
    ``column.exclude.list``). Excluding is the only safe handling for values that
    can also exceed the broker limit -- runtime isolation can't recover those
    (cdc-handling-design.md §4-b). No silent loss: nothing is excluded unless the
    user ticks it, and the resulting list is shown verbatim.

    When no oversized-LOB column qualifies (the common case), there is nothing to
    configure -- so instead of a full section the panel collapses to a single calm
    INFO notice (AWS-style), keeping the result discoverable without the visual
    weight of a settings card. The full opt-in card is shown only when there are
    actual candidates to exclude.
    """
    candidates = lob_exclusion_candidates(inventory)
    selection = migration_state.cdc_lob_exclusions()
    if not candidates:
        # Nothing to exclude -> a lightweight info notice, not a heavy settings card.
        render_notice(
            ui,
            tone="info",
            icon="data_object",
            header="No oversized LOB columns",
            body=(
                "No MySQL LOB/TEXT columns in the selected tables can exceed the "
                f"Aurora DSQL {_DSQL_VALUE_LIMIT_MIB} MiB value limit — nothing "
                "needs excluding from CDC capture."
            ),
        )
        return
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            ui.icon("data_object", color="warning").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Oversized LOB columns (optional exclusion)").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold"
            )
        ui.label(  # type: ignore[attr-defined]
            "These MySQL LOB/TEXT columns can hold values over the Aurora DSQL "
            f"{_DSQL_VALUE_LIMIT_MIB} MiB limit. A value over the "
            f"{_BROKER_MESSAGE_LIMIT_MIB} MiB broker limit can't be streamed at "
            "all, so exclude such columns here to keep CDC from stalling. "
            "Nothing is excluded unless you tick it."
        ).classes("text-xs text-gray-500")
        for candidate in candidates:
            excluded = selection.get(candidate.table, set())
            for column in candidate.columns:
                def _toggle(
                    event, _table=candidate.table, _column=column
                ) -> None:
                    migration_state.set_cdc_lob_exclusion(
                        _table, _column, bool(event.value)
                    )
                    if callable(refresh):
                        refresh()

                ui.checkbox(  # type: ignore[attr-defined]
                    f"{candidate.table}.{column}",
                    value=column in excluded,
                    on_change=_toggle,
                ).props("dense")
        exclude_value = format_column_exclude_list(
            {table: sorted(cols) for table, cols in selection.items()}
        )
        if exclude_value:
            with ui.row().classes("items-center gap-1 no-wrap flex-wrap"):  # type: ignore[attr-defined]
                ui.label("column.exclude.list:").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500"
                )
                ui.label(exclude_value).classes("text-xs font-mono")  # type: ignore[attr-defined]


def _render_cdc_handling_panel(ui) -> None:
    """Render the CDC behavior & limits section: what's handled vs. what to watch.

    A collapsed section (reference material) split into two clearly-labeled groups
    -- "Handled automatically" (the guarantees) and "Limits to watch" (the caveats
    the operator must plan around, e.g. DDL is not replicated) -- so the two read
    distinctly instead of as one mixed list. Customer-facing copy only (the
    internal spike hypothesis codes on each fact are not shown).
    """
    facts = cdc_handling_facts()
    handled = [f for f in facts if f.handled]
    limits = [f for f in facts if not f.handled]

    def _fact_rows(group, *, icon, color):
        for fact in group:
            with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.icon(icon, color=color).classes("text-base")  # type: ignore[attr-defined]
                with ui.column().classes("gap-0 flex-1 min-w-0"):  # type: ignore[attr-defined]
                    ui.label(fact.title).classes("text-sm")  # type: ignore[attr-defined]
                    ui.label(fact.detail).classes("text-xs text-gray-500")  # type: ignore[attr-defined]

    with ui.expansion("CDC behavior & limits", icon="info").classes(  # type: ignore[attr-defined]
        "w-full"
    ).props("expand-separator"):
        with ui.column().classes("w-full gap-3 p-1"):  # type: ignore[attr-defined]
            if handled:
                ui.label("Handled automatically").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold text-gray-500 uppercase tracking-wide"
                )
                _fact_rows(handled, icon="check_circle", color="positive")
            if limits:
                ui.separator().classes("my-1")  # type: ignore[attr-defined]
                ui.label("Limits to watch").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold text-gray-500 uppercase tracking-wide"
                )
                _fact_rows(limits, icon="warning", color="warning")


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
