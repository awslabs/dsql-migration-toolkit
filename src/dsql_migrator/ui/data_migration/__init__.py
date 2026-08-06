# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
    composite_key_columns_for_cdc,
)
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import (
    JobHandle,
    JobManager,
    JobNotFoundError,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.target_introspector import (
    target_primary_key_columns,
    tables_with_rows,
)
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
    applied_view_ddls,
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
    sync_identity_sequences_for_tables,
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
    migration_status_label,
    migration_status_badge,
    stale_error_notice,
    substeps_for_type,
    resolve_active_substep_for_type,
    resolve_active_substep,
    should_pin_cdc_substep,
    _MigrationTypeMeta,
    _MIGRATION_TYPE_META,
    MigrationProgress,
    summarize_progress,
    build_full_load_status_view,
    FullLoadTableRow,
    build_full_load_table_rows,
    failed_table_names,
    quarantined_rows_by_table,
    unsettled_table_names,
    format_duration,
    format_table_timing,
    FullLoadCompleteness,
    full_load_completeness,
    MigrationTableStatus,
    build_migration_table_status,
    _LOAD_STATE_ORDER,
    summarize_table_states,
    prereq_scope_gap,
    schema_recreate_tables,
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
    cdc_cascade_gap_tables,
    cdc_handling_facts,
    cdc_prerequisite_block_reason,
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
    is_infra_create_stack_status,
    cdc_attach_scope_mismatch,
    split_attachable_stacks,
    _classify_cdc_stack_phase,
    _probe_cdc_stack_phase,
    _ensure_cdc_controller,
    cdc_discovery_fingerprint,
    _CDC_DISCOVERY_THROTTLE_SECONDS,
    _CDC_IDLE_RATE_THRESHOLD,
    CdcActivitySummary,
    cdc_activity_summary,
    cdc_error_log_key,
    full_load_error_records,
    full_load_error_summary,
    full_load_latest_messages,
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
    # (the session is the authoritative store for the mode + CDC infra inputs).
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

    def _assessment():
        """The Step 1 (Evaluation) compatibility report, or None if it hasn't run."""
        result = eval_state.result
        return getattr(result, "assessment", None) if result is not None else None

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
        _conversion = SchemaConverter().convert(inventory)
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
                _conversion, conv_state.edited_target_ddls
            ),
            # Converted view DDLs so a "drop & reload" run can pre-drop / recreate
            # views that depend on a replaced table (else the DROP is blocked).
            dependent_view_ddls=applied_view_ddls(
                _conversion, conv_state.edited_target_ddls
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
            default=default_migration_selection(
                inventory,
                conv_state.generated_node_ids,
                _target_inventory(),
                conv_state.ticked_node_ids,
            ),
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
        # Record WHICH prerequisite mode cleared the gate for this run. The reports
        # are not persisted, so the run-guard excuses an absent report once a run
        # exists; scoping that excuse to this mode is what stops a later switch to a
        # CDC type from inheriting a Full-load-only pass (the CDC checks -- binlog
        # ROW/FULL, replication grants -- would never have run).
        migration_state.set_prereq_gated_mode(
            prereq_mode_for_type(migration_state.migration_type)
        )
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
                inputs=inputs,
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
        #
        # Gate this on cdc_pipeline_live (connectors detected / phase running), NOT
        # cdc_streaming_started: the latter latches the instant Start is pressed, so
        # promoting on it flipped the Data Migration step (and its "Success" badge)
        # to DONE while the connectors were still coming up and no row had reached
        # the target. Promotion means "data has actually arrived", so it must wait
        # for the pipeline to be genuinely live.
        promoted = data_migration_step_after_cdc(
            status, cdc_streaming=cdc_pipeline_live(migration_state)
        )
        if promoted is not None:
            session.set_workflow(  # type: ignore[attr-defined]
                with_status(session.workflow, WorkflowStep.FULL_LOAD, promoted)
            )
            status = promoted
        inventory = _inventory()

        # Composite-PK CDC re-key: when a table's applied Schema Conversion gave it
        # a composite target key, Debezium must key its change record on those same
        # columns (message.key.columns) so the sink's record-key ON CONFLICT/DELETE
        # match the target -- no sink change. Recompute from the applied conversion
        # each render and store it on the state for the CDC start path to read.
        if inventory is not None and inventory.tables:
            _applied = applied_table_conversions(
                SchemaConverter().convert(inventory), conv_state.edited_target_ddls
            )
            migration_state.set_cdc_message_key_columns(
                composite_key_columns_for_cdc(inventory.tables, _applied)
            )

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
                default=default_migration_selection(
                    inventory,
                    conv_state.generated_node_ids,
                    _target_inventory(),
                    conv_state.ticked_node_ids,
                ),
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
            # Pin the exact table set the checks covered. The picker locks the moment
            # a report exists (``selection_locked`` below), so from here on this set
            # IS the migration scope -- but until now it was only implied by the
            # default when the user never touched the picker, leaving
            # ``migration_state.selection`` empty. Anything downstream that reads the
            # selection (the CDC connector's TableIncludeList / SinkTopics and the
            # topic partition plan) then resolved to "no tables". Recording it makes
            # the confirmed scope explicit, so a CDC deploy fired right after the
            # checks -- before any Full Load watermark exists -- gets the real set.
            migration_state.set_selection(TableSelection(selected_tables=list(names)))
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
            #
            # job_manager is required: without it an IN-FLIGHT connector start is
            # invisible (no connectors yet, phase not "running", and on a CDC-only
            # plan the full_load step is not IN_PROGRESS), which left the tiles
            # switchable while Start CDC was already running.
            _type_lock_reason = migration_type_lock_reason(
                migration_state, status=status, job_manager=job_manager
            )
            _render_migration_type_selector(
                ui,
                migration_state,
                status=status,
                refresh=refresh,
                # One evaluation drives BOTH the disabled state and the explanation,
                # so a lock can never appear without its reason.
                locked=_type_lock_reason is not None,
                lock_reason=_type_lock_reason,
            )
            # Plan-level CDC discovery surfacing: the moment the plan includes CDC,
            # the discovery (armed below on has_cdc) populates cdc_other_stacks. Show
            # any existing CDC infrastructure right here -- where the user selects the
            # type -- so they can ATTACH instead of navigating to the deep CDC substep
            # only to find a duplicate-deploy risk (a second, costly MSK cluster).
            if "cdc" in substeps_for_type(migration_type):
                _render_cdc_existing_infra_banner(ui, migration_state, refresh)
                # Surface the CDC-specific Evaluation finding right where CDC is
                # chosen. The assessment already detects FK cascades that CDC cannot
                # replicate (they never reach the binary log), but that finding lived
                # only in the Evaluation report -- read BEFORE the user knew whether
                # CDC was in scope. Its own guidance starts with "Before starting
                # CDC", so this is the moment it is actionable.
                _cascade_tables = cdc_cascade_gap_tables(_assessment())
                if _cascade_tables:
                    _listed = ", ".join(_cascade_tables[:6]) + (
                        f" +{len(_cascade_tables) - 6} more"
                        if len(_cascade_tables) > 6
                        else ""
                    )
                    _noun = "table" if len(_cascade_tables) == 1 else "tables"
                    render_notice(
                        ui,
                        tone="warning",
                        header=(
                            "CDC cannot replicate this schema's cascading foreign keys"
                        ),
                        body=(
                            f"{len(_cascade_tables)} {_noun} use foreign keys with "
                            f"automatic ON DELETE/UPDATE actions ({_listed}). MySQL "
                            "applies those to child rows inside InnoDB, so they never "
                            "reach the binary log — CDC cannot see them, and DSQL has "
                            "no foreign keys to re-perform them. The child rows are "
                            "left behind on the target with no error. Replace the "
                            "automatic actions with explicit child-row statements in "
                            "your application before starting CDC (you need that on "
                            "DSQL anyway), and enable the orphan-record check in "
                            "Validation. See Evaluation for the full list."
                        ),
                    )

            with ui.row().classes("items-center gap-2"):
                ui.label("Data Migration status:").classes(
                    "text-sm text-gray-500"
                )
                # Outline chip to match the CDC table's status badges (Full Load /
                # Consistency / DLQ are all outline) — one consistent status-chip style
                # across both stats tables, per the design system.
                # Name the phase AND read that phase's own status. A bare "DONE" was
                # ambiguous once the type selector moved; labelling it while still
                # showing the shared full_load value was worse -- a restored session
                # that had once run a Full Load came back reading "CDC: DONE" with CDC
                # never having run, because the whole workflow is persisted.
                _badge_label, _badge_status = migration_status_badge(
                    migration_type,
                    full_load_status=status,
                    cdc_status=get_status(session.workflow, WorkflowStep.CDC),
                    cdc_streaming=cdc_pipeline_live(migration_state),
                )
                ui.badge(
                    "{}: {}".format(_badge_label, _badge_status.value)
                ).props(f"color={_STATUS_COLORS[_badge_status]} outline")

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
                # An error recorded under a DIFFERENT migration type is carried-over
                # context, not a live failure of the current selection: it stays (the
                # gap it reports is real, and CDC does not backfill a Full Load gap) but
                # is demoted from error to warning, so the screen no longer shows
                # "Migration failed" beside a "Success" header and a DONE status.
                notice = stale_error_notice(
                    error,
                    migration_type=migration_type,
                    error_migration_type=migration_state.error_migration_type,
                    # Accepting the quarantine RESOLVES this error; the step's own
                    # "complete -- with an accepted gap" notice carries the facts.
                    quarantine_accepted=migration_state.accept_quarantined_rows,
                )
                if notice is not None:
                    _tone, _header, _body = notice
                    render_notice(ui, tone=_tone, header=_header, body=_body)

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

            job = _current_job(job_manager, migration_state.job_id)

            # Why the table picker is frozen (None = still editable). Single source for
            # both the boolean and the lock tooltip's wording, so the control and its
            # explanation can never drift apart.
            selection_lock = selection_lock_reason(
                migration_state,
                job_manager,
                status=status,
                migration_type=migration_type,
                has_job=job is not None,
            )
            selection_locked = selection_lock is not None

            # When locked because CDC is actually streaming, reflect the REAL set of
            # tables the connectors were deployed with (watermark-covered / confirmed
            # selection), not the generic "everything on the target" default -- so a
            # reconnect shows what CDC is truly replicating instead of every table
            # ticked-and-frozen. Only for the live-CDC lock -- the other lock causes
            # (a finished Full Load, deployed-but-not-started CDC infra) have no
            # connector table list to reflect, so they keep the normal selection view.
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
                    # Pre-tick this session's Schema Conversion selection (falling back
                    # to the target-existing set only when nothing was generated here),
                    # so picking 3 tables in Step 2 does not arrive with 11 ticked.
                    default_selection=default_migration_selection(
                        inventory,
                        conv_state.generated_node_ids,
                        _target_inventory(),
                        conv_state.ticked_node_ids,
                    ),
                    on_refresh=refresh_browser,
                    locked=selection_locked,
                    lock_reason=selection_lock,
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
                default=default_migration_selection(
                    inventory,
                    conv_state.generated_node_ids,
                    _target_inventory(),
                    conv_state.ticked_node_ids,
                ),
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
            # Widened beyond "connectors exist": an infra create/teardown in flight, and
            # a CDC-only session whose infrastructure is ready but not yet started, are
            # both CDC work with no connectors yet -- and both used to collapse the CDC
            # section right after the operator acted on it.
            _infra_job = _current_job(
                job_manager, getattr(migration_state, "cdc_deploy_job_id", None)
            )
            if (
                should_pin_cdc_substep(
                    migration_type=migration_type,
                    has_connectors=bool(
                        getattr(migration_state, "cdc_connector_names", None)
                    ),
                    infra_prep_state=cdc_infra_prep_state(
                        migration_state, job_manager
                    ),
                    infra_action_kind=getattr(
                        migration_state, "cdc_action_kind", None
                    ),
                    infra_action_running=(
                        _infra_job is not None
                        and _infra_job.status in ("PENDING", "RUNNING")
                    ),
                )
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
                    ui.notify(
                        "No previous load run found to retry from.",
                        type="warning",
                        position="top",
                    )
                    return
                names = [n for n in names_to_retry]
                if not names:
                    ui.notify(
                        "No tables selected for retry.",
                        type="info",
                        position="top",
                    )
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
                retry_cdc_coexisting = cdc_streaming_started(
                    migration_state, job_manager
                )
                # Carry the run-wide reload choice onto the retry: if the user chose
                # "Drop & reload", a table being retried must be recreated on the
                # RE-run too, not left to skip-existing over the stale rows --
                # otherwise a retry reports "0 new + N already there" over data that
                # was never refreshed (e.g. after a first-run DROP failed on a
                # dependent view). ``replace_targets`` already encodes the choice
                # (drop set when mode=="drop", empty for append); scope it to the
                # tables being retried and suppress when CDC is live (DROP races the
                # sink).
                retry_replace = (
                    frozenset()
                    if retry_cdc_coexisting
                    else frozenset(migration_state.replace_targets) & set(names)
                )
                _retry_conversion = SchemaConverter().convert(inventory)
                retry_inputs = DataMigrationInputs(
                    source_config=source_config,
                    source_password=session.source_password,
                    target_config=target_config,
                    inventory=inventory,
                    aws_profile=session.aws_profile,
                    staging_bucket=staging_bucket,
                    replace_tables=retry_replace,
                    cdc_coexisting=retry_cdc_coexisting,
                    table_conversions=applied_table_conversions(
                        _retry_conversion,
                        conv_state.edited_target_ddls,
                    ),
                    dependent_view_ddls=applied_view_ddls(
                        _retry_conversion,
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
                        inputs=retry_inputs,
                    )

                migration_state.job_id = job_manager.submit(work)
                ui.notify(
                    f"Retrying {len(names)} table(s) — progress below.",
                    type="positive",
                    position="top",
                )
                refresh()

            def retry_failed_load() -> None:
                # Re-run Full Load for the UNFINISHED tables (FAILED *or* still
                # PENDING), carrying the succeeded tables forward so the view stays
                # unified. PENDING matters when a fatal/aborted run left tables it
                # never got to attempt -- they must be resumable, not stranded.
                current = _current_job(job_manager, migration_state.job_id)
                if current is None:
                    return
                _run_retry_for(unsettled_table_names(current))

            def reload_table(table_name: str) -> None:
                # Per-table Reload: re-run Full Load for exactly one table (even a
                # DONE one), e.g. after fixing an oversized source value so a
                # previously-quarantined row now loads. Reuses the scoped retry path.
                _run_retry_for([table_name])

            def retry_tables(names: Sequence[str]) -> None:
                # Retry an explicit SUBSET of tables (the failed-table checklist's
                # ticked set). Same scoped retry path as Reload, for many tables.
                _run_retry_for(list(names))

            # On-demand AI diagnosis of a FAILED Full Load table (opt-in: AI Assist
            # on). Reuses the shared chat drawer + a strategist bound to the Full
            # Load error grounding, so a reply explains the specific failure
            # (schema/DDL vs data vs transient) and the recovery step. Built lazily
            # and ONCE (the drawer is a top-level dialog; the per-poll progress
            # re-render must not create a new drawer each time). ``None`` when AI is
            # off -> the renderer shows a disabled affordance instead.
            ai_error_opener = None
            if session.ai_assist.enabled:
                _ai_drawer = {"open_chat": None}

                def ai_error_opener(table_name: str, error_message: str) -> None:
                    from dsql_migrator.core.assessment_strategist import (
                        AssessmentStrategist,
                    )
                    from dsql_migrator.ui.ai_chat_drawer import build_chat_drawer

                    if _ai_drawer["open_chat"] is None:
                        _ai_drawer["open_chat"] = build_chat_drawer(ui)
                    strategist = AssessmentStrategist(
                        session.ai_assist, aws_profile=session.aws_profile
                    )
                    # Ground the reply in THIS migration's situation (type, CDC
                    # status, DROP+recreate) so it isn't generic.
                    migration_context = full_load_error_migration_context(
                        migration_state,
                        table_name=table_name,
                        cdc_live=cdc_streaming_started(migration_state, job_manager),
                    )
                    _ai_drawer["open_chat"](
                        title="AI Assist — Full Load failure",
                        subtitle=table_name,
                        first_question=(
                            f"Why did loading {table_name} fail, and how do I "
                            "fix it?"
                        ),
                        streamer=lambda messages, on_delta: (
                            strategist.stream_full_load_error_chat(
                                table_name,
                                error_message,
                                messages,
                                on_delta,
                                migration_context=migration_context,
                            )
                        ),
                    )

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
                # Accepting the gap is the moment this quarantined load becomes COMPLETE
                # -- but it happens AFTER _finalize_run, which saw the run as incomplete
                # and skipped the identity-sequence sync. Without this, a GENERATED BY
                # DEFAULT identity key stays at nextval=1 while migrated ids are already
                # present, and the app's first insert after cut-over collides (23505).
                # Quarantined rows are permanently dropped, so MAX(pk) is final now.
                # Run it in the background (a target write) over the migration scope; the
                # catalog's is_identity filter skips non-identity tables.
                target_config = session.target_config
                if target_config is not None and selected_names:
                    _sync_names = list(selected_names)
                    _sync_profile = session.aws_profile

                    def _sync_after_accept(_handle: JobHandle) -> None:
                        sync_identity_sequences_for_tables(
                            target_config, _sync_names, aws_profile=_sync_profile
                        )

                    job_manager.submit(_sync_after_accept)
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

            # Discover the deployed CDC connectors/stack OFF the event loop, armed
            # BEFORE any sub-step renders. The describe_stacks + list_connectors calls
            # are BLOCKING network I/O; running them during render starved the NiceGUI
            # WebSocket. Render only READS state -- a throttled one-shot timer runs the
            # AWS reads on a worker thread and refreshes when done. The throttle
            # timestamp (set inside _ensure_cdc_controller) gates re-arming, so the
            # refresh does not loop.
            #
            # It is armed here (plan level) rather than inside the CDC sub-step block
            # because the Prerequisites sub-step now renders the first-deploy
            # affordance, and it must not offer a fresh deploy before the account-wide
            # discovery has reported -- otherwise the duplicate-MSK guard (attach to an
            # existing pipeline) is gone and the user can pay for a second cluster.
            # Sub-steps render in order (Prerequisites -> Full Load -> CDC), so arming
            # after them would leave the first pass with an unprobed state.
            if has_cdc:
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
                        # Refresh only when discovery actually changed something. This
                        # used to refresh unconditionally, which rebuilt every widget
                        # ~0.05s+ after the screen appeared -- so a click on Start /
                        # Re-run Full Load in that window hit an element that no longer
                        # existed and was dropped, and the button appeared to need a
                        # second press. On a revisit discovery usually finds the same
                        # stack and connectors, so the rebuild bought nothing.
                        _before = cdc_discovery_fingerprint(migration_state)
                        try:
                            await _disc_run.io_bound(
                                _ensure_cdc_controller, migration_state, session
                            )
                        except Exception:  # noqa: BLE001 - best-effort discovery
                            pass
                        # Log any connector RUNNING/FAILED transition (on change).
                        _log_cdc_connector_transitions(migration_state, job_manager)
                        # A real change (first probe reporting, a new stack found, a
                        # connector appearing/going away) still refreshes, so the
                        # duplicate-MSK adopt guard shows up as soon as it is known.
                        if cdc_discovery_fingerprint(migration_state) != _before:
                            refresh()

                    ui.timer(0.05, _discover_cdc, once=True)  # type: ignore[attr-defined]

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
                    # A live CDC *connector* operation (Start/Stop -> update_stack)
                    # or an actually-streaming pipeline means re-checking here is
                    # inert, so the Check button is disabled. The ~15-20 min
                    # infrastructure create (create_stack) is explicitly NOT counted:
                    # nothing streams and no load is running during it, so the
                    # prerequisite checks are precisely what the user should run --
                    # and running them then is what lets the MSK create overlap the
                    # Full Load instead of serializing after it.
                    _stack_status = getattr(
                        migration_state, "cdc_stack_phase_status", None
                    )
                    _cdc_deploying = (
                        _is_inflight_stack_status(_stack_status)
                        and not is_infra_create_stack_status(_stack_status)
                    ) or cdc_streaming_started(migration_state, job_manager)
                    _render_prerequisites_panel(
                        ui,
                        migration_state,
                        run_checks,
                        mode=prereq_mode,
                        combined=(
                            migration_type is MigrationType.FULL_LOAD_AND_CDC
                        ),
                        load_running=(
                            status is StepStatus.IN_PROGRESS or _cdc_deploying
                        ),
                    )
                    # Right-align ONLY when this row holds the primary action (design
                    # system: primary actions sit on the right of a button row). The
                    # guard message that replaces it is a full sentence, and inheriting
                    # justify-end ragged it against the right edge, away from the
                    # content it explains -- prose reads left.
                    _nav_justify = (
                        "justify-end" if guard_reason is None else "justify-start"
                    )
                    with ui.row().classes(
                        f"!flex w-full {_nav_justify} items-center"
                    ):
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
                    # CDC infrastructure prep, offered HERE (bottom of Prerequisites)
                    # rather than only deep inside the CDC sub-step: the ~15-20 min MSK
                    # create should overlap the Full Load, so it must be reachable
                    # before the load starts -- and by this point the checks have
                    # pinned a real table set for the connector/partition plan.
                    #
                    # EXCEPT for CDC only, where the CDC step renders it instead: with no
                    # Full Load there is nothing to overlap, and splitting one continuous
                    # task across two sections meant the operator deployed here and then
                    # had to hunt for Start CDC in a different section. Rendering it in
                    # both places would duplicate a billable deploy form.
                    if has_cdc and migration_type is not MigrationType.CDC_ONLY:
                        _render_cdc_infra_prep_section(
                            ui,
                            migration_state,
                            job_manager,
                            refresh,
                            inventory=inventory,
                            session=session,
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
                            retry_tables=retry_tables,
                            reload_table=reload_table,
                            accept_quarantine_and_continue=accept_quarantine_and_continue,
                            stop_full_load=stop_full_load,
                            refresh=refresh,
                            ai_error_opener=ai_error_opener,
                            # Tables the load will recreate to apply a primary key that
                            # differs from the source, so the confirm dialog can disclose
                            # the DDL step before it runs. Passed as a THUNK, not a list:
                            # the inventory and applied conversions live in this scope,
                            # but the answer also depends on the targets' REAL primary
                            # keys, which are only known after the dialog's pre-open
                            # probe. Calling it there (not here) is what stops the dialog
                            # announcing a recreate for a table that already carries the
                            # applied key.
                            schema_recreate_candidates=lambda: schema_recreate_tables(
                                selected_names,
                                table_conversions=(
                                    applied_table_conversions(
                                        SchemaConverter().convert(inventory),
                                        conv_state.edited_target_ddls,
                                    )
                                    if inventory is not None
                                    else {}
                                ),
                                inventory=inventory,
                                tables_with_data=migration_state.tables_with_data,
                                # Cached by the pre-dialog probe, so no target I/O here.
                                target_keys=migration_state.target_primary_keys,
                            ),
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
                                    "changes, set the migration type to "
                                    "\"CDC only\" — it streams from this Full Load's "
                                    "watermark onto the already-loaded target (no "
                                    "re-snapshot). Use the link below to jump to that "
                                    "setting."
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
                                # The selector this refers to is at the top of the
                                # page; after a Full Load the notice can sit well below
                                # the fold, so "change the migration type above" asked
                                # the user to go find a control they could not see.
                                # Jump to it instead of only naming it.
                                _scroll_to_migration_type_button(ui)

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


def full_load_error_migration_context(
    migration_state,
    *,
    table_name: str,
    cdc_live: bool,
) -> str:
    """Assemble a credential-free description of the CURRENT migration situation.

    Fed to the Full Load "AI Assist" grounding so the reply is specific to THIS
    migration (its type, whether CDC is part of the plan / already streaming,
    whether the failed table was a DROP+recreate of an existing target) rather than
    generic. Contains only tool state -- migration type, CDC status, and whether
    the target table pre-existed -- never any connection/credential detail
    (Property 7). NiceGUI-agnostic for tests.
    """
    mt = getattr(migration_state, "migration_type", None)
    mt_value = getattr(mt, "value", mt)
    type_label = {
        "full_load_only": "Full Load only (no CDC in this plan)",
        "cdc_only": "CDC only",
        "full_load_and_cdc": "Full Load + CDC (change data capture will follow)",
    }.get(str(mt_value), str(mt_value))
    replace_targets = set(getattr(migration_state, "replace_targets", set()) or set())
    lines = [
        f"Migration type: {type_label}",
        (
            f"Target table '{table_name}' already existed and was being "
            "DROP+recreated"
            if table_name in replace_targets
            else f"Target table '{table_name}' was being created fresh "
            "(no pre-existing target table)"
        ),
    ]
    if "cdc" in str(mt_value):
        lines.append(
            "CDC is currently streaming (a re-run of Full Load can collide with "
            "the live sink)."
            if cdc_live
            else "CDC has not started streaming yet."
        )
    return "\n".join(lines)


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

    That excuse is scoped to the mode that ACTUALLY cleared the gate
    (``state.prereq_gated_mode``). A Full-load-only run passed only the
    ``FULL_LOAD`` checks, so a user who finishes it and then switches the type to
    add CDC must not inherit that pass for ``CDC`` mode -- the CDC-only checks
    (binlog ``ROW``/``FULL`` row image, the replication grants) were never run. This
    is the "add CDC later" path the docs advertise as reversible, and it previously
    reached the CDC sub-step with the binlog format unverified, so a source on
    ``STATEMENT``/``MIXED`` was only discovered as an undiagnosed connector failure
    ~26 min into a billable create. When the modes differ the checks are required
    again, worded for the mode that is now missing.

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
        # Only excuse the absent report when the completed run was gated by THIS
        # mode. An unknown gated mode (older session, or a snapshot written before
        # the field existed) keeps the previous lenient behavior so a reconnect is
        # never hard-blocked; a KNOWN but different mode means these checks have
        # genuinely never run.
        gated_mode = getattr(state, "prereq_gated_mode", None)
        if has_run and (gated_mode is None or gated_mode is prereq_mode):
            return None
        if has_run:
            return (
                "Run the prerequisite checks for this migration type first — "
                "adding CDC needs checks the completed Full Load never ran "
                "(binary-log format and replication grants)."
            )
        if getattr(state, "active_substep", None) in ("full_load", "cdc"):
            return (
                "Reconnected — re-run the prerequisite checks (Prerequisites "
                "step) to resume. They're read-only and quick; your progress "
                "wasn't lost, but the results aren't kept across an app restart."
            )
        return "Run the prerequisite checks first (Prerequisites tab)."
    # A report can outlive the selection it covered: nothing clears it, and (since the lock
    # only holds once inputs are committed) the picker is editable while the checks are just
    # a preview. Block on tables ADDED since -- they never saw TARGET_SCHEMA_READY, and
    # run_full_load raises FullLoadIncompleteError on any per-table failure, so one unchecked
    # table fails the whole job. Removing tables is NOT a gap: the report is then a superset,
    # so everything still selected was checked and passed.
    added = prereq_scope_gap(report, state.selection.selected_tables)
    if added:
        listed = ", ".join(added[:3]) + (" and more" if len(added) > 3 else "")
        return (
            f"Re-run the prerequisite checks — {listed} "
            f"{'were' if len(added) > 1 else 'was'} added to the selection after the "
            "checks ran, so they were never checked."
        )
    return prerequisite_block_reason(report)


def _render_watermark(ui: object, job: MigrationJob) -> None:
    """Render the export watermark for ``job`` (Requirement 8.5 / Property 11).

    Compact by design: this is provenance the operator reads once (or copies into a
    runbook), not something to watch, so it must not occupy the height a 4-row
    two-column ``ui.table`` did -- complete with sortable "Field"/"Value" headers for
    four fixed rows. Each coordinate is now a labelled monospace line inside one bordered
    panel: the label identifies the field, the monospace value is scannable and
    copy-pasteable, and unavailable fields are muted so the eye skips them instead of
    reading "unavailable" as an error.
    """
    if job.watermark is None:
        ui.label("Export watermark").classes(  # type: ignore[attr-defined]
            "text-sm font-semibold text-gray-700"
        )
        ui.label(  # type: ignore[attr-defined]
            "The export consistency point is captured when the migration starts."
        ).classes("text-sm text-gray-500")
        return

    display = format_watermark(job.watermark)
    with ui.element("div").classes(  # type: ignore[attr-defined]
        "w-full rounded-md border border-gray-200 bg-gray-50 p-3"
    ):
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("bookmark").classes("text-gray-500 text-base")  # type: ignore[attr-defined]
            ui.label("Export watermark").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-800"
            )
            # The summary IS the identifying line (snapshot time + coordinate), so it
            # sits on the header row rather than as a separate paragraph.
            ui.label(display.summary).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-500 truncate"
            )
        for label, value in (
            ("Snapshot (UTC)", display.snapshot_timestamp),
            ("Binlog file:pos", display.coordinate),
            ("GTID set", display.gtid),
            ("Server UUID", display.server_uuid),
        ):
            # An unavailable coordinate is normal (binary logging off, or SHOW MASTER
            # STATUS restricted on RDS/Aurora), so it is muted -- not styled like a
            # missing required value.
            unavailable = str(value).strip().lower() == _UNAVAILABLE
            with ui.row().classes("items-baseline gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.label(label).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500 shrink-0 w-32"
                )
                ui.label(value).classes(  # type: ignore[attr-defined]
                    "text-xs font-mono break-all "
                    + ("text-gray-400 italic" if unavailable else "text-gray-800")
                )

        if display.table_row_counts:
            approximate = getattr(job.watermark, "row_counts_approximate", False)
            count = len(display.table_row_counts)
            heading = (
                f"Per-table snapshot rows ({count}, estimated)"
                if approximate
                else f"Per-table snapshot rows ({count})"
            )
            # INSIDE the watermark panel and styled like its other fields. It used to sit
            # outside as a full-width Quasar expansion wrapping a bordered ``ui.table``
            # with its own sortable headers -- a second visual container hanging below the
            # panel it belongs to, in a style nothing else on the screen uses. The counts
            # are one value per table, so labelled monospace rows (the same shape as the
            # coordinates above) read as more of this panel's detail rather than a
            # separate data grid.
            # Quasar's default expansion header is a grey full-bleed bar with a large
            # leading glyph -- a heavy band across an otherwise flat panel, in the one
            # place that should read as a quiet "more detail" affordance. Strip the fill
            # and the icon and size the header like the field labels above it, so opening
            # it feels like unfolding more of the same panel.
            with ui.expansion(heading).classes(  # type: ignore[attr-defined]
                "w-full"
            ).props(
                "dense dense-toggle expand-separator=false "
                "header-class='text-xs text-gray-500 px-0'"
            ):
                if approximate:
                    ui.label(  # type: ignore[attr-defined]
                        "Estimated from the source catalog (no COUNT(*) scan) to spare "
                        "the source; exact counts are verified in Validation."
                    ).classes("text-xs text-gray-400 pb-1")
                for table, rows_count in display.table_row_counts.items():
                    # SAME two-column shape as the coordinates above: a fixed-width label
                    # column then the value, so the table names and the counts each line
                    # up with the fields they sit under instead of forming a second,
                    # differently-aligned list.
                    with ui.row().classes(  # type: ignore[attr-defined]
                        "items-baseline gap-2 no-wrap w-full"
                    ):
                        ui.label(table).classes(  # type: ignore[attr-defined]
                            "text-xs text-gray-500 shrink-0 w-64 truncate"
                        )
                        # Monospace, matching the coordinate values -- a column of counts
                        # then lines up on the digits and is comparable at a glance.
                        ui.label(f"{rows_count:,}").classes(  # type: ignore[attr-defined]
                            "text-xs font-mono text-gray-800"
                        )


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

# CSS marker on the "Migration type" heading, used as a scroll target. The
# post-Full-Load "want CDC next?" notice renders far below the selector it refers to
# ("change the migration type above"), which on a long Data Migration page can be off
# screen -- so the notice offers a jump link instead of leaving the user to scroll and
# find it. Kept as a named constant so the anchor and the querySelector cannot drift.
MIGRATION_TYPE_ANCHOR = "dm-migration-type-anchor"


def _scroll_to_migration_type_button(
    ui, *, label: str = "Change migration type"
) -> None:
    """Render a link-style button that scrolls to the migration-type selector.

    Mirrors the sub-step scroll already used on this screen (querySelector on a CSS
    marker + ``scrollIntoView``) so the behaviour is consistent. A brief ring is drawn
    around the heading after the scroll: landing at the top of a long page without it,
    the user still has to work out WHICH control the notice meant.

    Purely navigational -- it changes no migration state, so the type is still chosen by
    deliberately clicking a tile.
    """
    ui.button(  # type: ignore[attr-defined]
        label,
        icon="arrow_upward",
        on_click=lambda: ui.run_javascript(  # type: ignore[attr-defined]
            f"const el=document.querySelector('.{MIGRATION_TYPE_ANCHOR}');"
            "if(el){el.scrollIntoView({behavior:'smooth',block:'center'});"
            # Tailwind ring utilities, added then removed, so the highlight needs no
            # stylesheet and leaves no residue on the element.
            "el.classList.add('ring-2','ring-blue-400','rounded');"
            "setTimeout(()=>el.classList.remove("
            "'ring-2','ring-blue-400','rounded'),2000);}"
        ),
    ).props("flat dense no-caps color=primary")


def migration_type_locked(migration_state, job_manager, *, status) -> bool:
    """True once CDC has been committed/started, so the type must not change.

    Thin boolean over :func:`migration_type_lock_reason` (the single source of
    truth for *why* the type is locked). ``job_manager`` is needed to see an
    IN-FLIGHT connector start: the connectors do not exist yet and the stack phase
    is not "running", so state alone cannot tell that CDC is already committed.
    """
    return (
        migration_type_lock_reason(
            migration_state, status=status, job_manager=job_manager
        )
        is not None
    )


def migration_type_lock_reason(
    migration_state, *, status, job_manager=None
) -> Optional[str]:
    """Why the migration type is locked, or ``None`` if it can still be changed.

    Separates two distinct sources so the UI can explain the lock clearly:

    * **Owned** — a migration this session is actively running
      (``StepStatus.IN_PROGRESS``). Changing the type mid-run is incoherent.
    * **Discovered** — CDC connectors / a running cdc-stack were found on AWS
      (possibly deployed in a previous session; ``cdc_connector_names`` survives a
      restore). Switching the type would orphan/break that live pipeline.
    * **Starting** — a connector start is IN FLIGHT (``kind="start"``). This is the
      gap the first two miss: the connectors do not exist yet (so
      ``cdc_connector_names`` is empty and the phase is not yet "running"), and on a
      CDC-only plan the ``full_load`` step is not IN_PROGRESS either -- so the type
      selector stayed live while Start CDC was already running. The user could switch
      to Full load + CDC and watch it lock immediately afterwards, once the connectors
      appeared. The start point and table set are committed the moment Start is
      pressed, so the choice must freeze then, not when the connectors finish.

    * **Provisioning** — a cdc-stack CREATE is IN FLIGHT (``kind="infra"``). This one
      used to be deliberately excluded, on the grounds that the create makes no
      connectors and streams nothing (see :func:`cdc_streaming_started`, which still
      excludes it for exactly that reason -- it answers "are the inputs committed?").
      But "nothing is streaming" is not the same as "the choice is free". The create is
      a ~15-20 min CloudFormation run that provisions a BILLABLE MSK Serverless cluster,
      and every way to watch or undo it (the stage progress, the event log, Delete CDC
      infrastructure) lives on the CDC sub-step. Switching to Full load only removes
      that sub-step outright (:func:`substeps_for_type`), so the user is left with an
      MSK cluster building in their account with no progress, no completion signal and
      no teardown control on screen. The tool also already treats this moment as
      committed elsewhere: the oversized-LOB exclusions lock during an ``infra`` job
      because ``ColumnExcludeList`` is baked into the stack at create time -- and those
      exclusions are a CDC-only setting, so freezing them while leaving the type itself
      switchable was internally inconsistent.

    Each legitimately freezes the choice, but the reason (and the remedy) differ.
    Reads ``status`` and already-populated state; ``job_manager`` (optional) is only
    consulted to see the in-flight job. No AWS I/O, so it is safe to call during render
    and is unit-testable.
    """
    if status is StepStatus.IN_PROGRESS:
        return (
            "Locked while a migration is in progress — finish or cancel it to "
            "change the type."
        )
    if job_manager is not None and cdc_streaming_started(migration_state, job_manager):
        return (
            "Locked because CDC is starting — the start point and table set are "
            "already committed. Stop CDC on the CDC step to change the type."
        )
    if getattr(migration_state, "cdc_stack_phase", None) == "running" or getattr(
        migration_state, "cdc_connector_names", None
    ):
        return (
            "Locked because CDC connectors from a previous run are still deployed "
            "(Start over does not delete them). To change the type, use 'Delete "
            "CDC infrastructure' on the CDC step first."
        )
    # A cdc-stack CREATE in flight: billable MSK is provisioning, and the only progress
    # view / teardown control lives on the CDC sub-step that a type switch would remove.
    if job_manager is not None and cdc_infra_deploy_in_flight(
        migration_state, job_manager
    ):
        return (
            "Locked while the CDC streaming infrastructure is being created — it is "
            "provisioning a billable Amazon MSK cluster, and its progress and 'Delete "
            "CDC infrastructure' control live on the CDC step. Wait for it to finish "
            "(or delete it there) to change the type."
        )
    return None


def selection_lock_reason(
    migration_state,
    job_manager,
    *,
    status,
    migration_type: MigrationType,
    has_job: bool,
) -> Optional[str]:
    """Why the table picker is frozen, or ``None`` while the selection can still change.

    The picker locks once the selection has been COMMITTED to something irreversible --
    not merely once the read-only prerequisite checks have run. It used to lock on "a
    prerequisite report exists", which was both too early and a dead end. Too early: the
    checks are a preview, so locking on them froze the scope before any migration began.
    A dead end: the lock's tooltip told the user to "re-run the checks to change which
    tables are migrated", but ``run_checks`` pins the checked set via ``set_selection``
    and then re-reads that pinned set (``touched=True``), so a re-run yields the SAME
    set -- and nothing clears a report. The only real exits were Start over or an
    undocumented migration-type flip (``prereq_mode`` is type-derived, so switching the
    type unlocked the picker and left a stale-scoped report satisfying the run guard).
    A late edit is instead caught by :func:`prereq_scope_gap` on the run guard.

    The three commits that DO fix the table set, each with its own remedy:

    * **A Full Load exists or finished** -- the export ran against this set (the job
      survives as ``status DONE`` even after the job record is pruned).
    * **CDC is streaming** -- the running source connector's table list is fixed;
      ticking the picker cannot add or remove a streamed table.
    * **CDC infrastructure is deployed or deploying** -- the subtle one. The immutable
      topic-partition plan is baked at infra create (``core/cdc.py``), and the deploy
      button lives on the PREREQUISITES step so the ~15-20 min MSK create can overlap
      the load. A table added after that gets its topic created with 1 partition,
      permanently. :func:`cdc_streaming_started` deliberately excludes an in-flight
      ``kind="infra"`` job (counting it once froze this picker for 20 minutes), so
      without this clause that whole window would be unguarded. Scoped to CDC-bearing
      types so a Full-load-only run is not frozen by a CDC stack that merely exists in
      the account.

    Pure: reads ``status`` plus already-populated state (``job_manager`` is only used for
    the job-id lookups the CDC helpers already do) -- no AWS I/O, so it is safe during
    render and unit-testable.
    """
    if has_job or status is StepStatus.DONE:
        return (
            "Locked — a Full Load has already run for this table set. Use 'Start over' "
            "to migrate a different set of tables."
        )
    if cdc_streaming_started(migration_state, job_manager):
        return (
            "Locked — CDC is running and its source connector streams a fixed table "
            "set. To change it, stop CDC on the CDC step first."
        )
    if "cdc" in substeps_for_type(migration_type) and cdc_infra_prep_state(
        migration_state, job_manager
    ) in ("ready", "deploying"):
        return (
            "Locked — CDC infrastructure is deployed for this table set, and each "
            "table's Kafka topic partitions are fixed when it is created. A table "
            "added now would stream on a single partition. To change the set, delete "
            "the CDC infrastructure on the CDC step first."
        )
    return None


def _render_cdc_existing_infra_banner(ui, migration_state, refresh) -> None:
    """Plan-level banner: existing CDC infrastructure was found in the account under a
    name this (reset) session does not target -- offer to ATTACH here, next to the
    migration-type choice, so the user need not reach the deep CDC substep to avoid a
    duplicate deploy.

    Driven by ``cdc_other_stacks`` (populated by the CDC discovery that runs whenever
    the plan includes CDC), so it appears as soon as the user picks a CDC-inclusive
    type. Renders nothing when no other stacks exist. Attaching is read/attach-only:
    it points the session at the stack and forces a fresh probe, so a running pipeline
    opens straight to its monitoring view in the CDC step; starting fresh stays the
    explicit Stop/Delete path. Deploying a deliberate SECOND pipeline (a different
    suffix) is still possible from the CDC step, so this surfaces a CHOICE, never a
    hard block.
    """
    others = getattr(migration_state, "cdc_other_stacks", []) or []
    if not others:
        return
    # A stack in a failed / rolled-back / deleting state must NOT be offered for
    # attach: its resources are partly gone, so it can never stream -- attaching only
    # produces a dead session. Worse, "Attach to <stack> (DELETE_FAILED)" buried the
    # fact that actually matters: a teardown did not finish, so its MSK / NAT may
    # still be BILLING with nothing tracking it.
    attachable, needs_cleanup = split_attachable_stacks(others)

    if needs_cleanup:
        listed = ", ".join(f"{name} ({status})" for name, status in needs_cleanup)
        _render_notice(
            ui,
            tone="error",
            header="Leftover CDC infrastructure needs cleanup — it may still be billing",
            body=(
                f"{listed}. A previous teardown did not finish, so this stack cannot "
                "be used (it is partly deleted) and cannot be attached to — but its "
                "Amazon MSK / NAT resources may still be incurring cost. Delete it "
                "from the CloudFormation console (a DELETE_FAILED stack usually needs "
                "'Retain resources' on the leftovers), or retry the delete from the "
                "CDC step, before deploying anything new."
            ),
        )

    # Withhold attach for a pipeline that does not cover THIS session's tables. Attaching
    # promotes Data Migration to DONE and unlocks Validation, so attaching a stack that
    # streams a different table set would report the migration complete while every table
    # this session loaded had no CDC at all -- silently losing each source change after the
    # watermark, and letting the operator proceed to cut over on that. Scope is checked
    # against the loaded/selected set; an unknown set on either side does not block (see
    # cdc_attach_scope_mismatch).
    loaded_tables = list(migration_state.selection.selected_tables)
    tables_by_stack = getattr(migration_state, "cdc_other_stack_tables", None) or {}
    in_scope: "list[tuple[str, str]]" = []
    out_of_scope: "list[tuple[str, str, list[str]]]" = []
    for name, status in attachable:
        missing = cdc_attach_scope_mismatch(
            tables_by_stack.get(name, ()), loaded_tables
        )
        if missing:
            out_of_scope.append((name, status, missing))
        else:
            in_scope.append((name, status))

    if out_of_scope:
        for name, status, missing in out_of_scope:
            listed = ", ".join(missing[:6]) + (
                f" +{len(missing) - 6} more" if len(missing) > 6 else ""
            )
            noun = "table" if len(missing) == 1 else "tables"
            _render_notice(
                ui,
                tone="warning",
                header=(
                    f"{name} streams a different set of tables — not safe to attach"
                ),
                body=(
                    f"That pipeline does not replicate {len(missing)} {noun} this "
                    f"session loaded ({listed}). Attaching would mark the migration "
                    "complete while those tables received no ongoing changes at all. "
                    "Either deploy CDC for this table set, or (if that pipeline is the "
                    "one you want) change the table selection to match it. Its "
                    "infrastructure is still billing meanwhile — delete it from the CDC "
                    "step if it is no longer needed."
                ),
            )

    if in_scope:
        listed = ", ".join(f"{name} ({status})" for name, status in in_scope)
        _render_notice(
            ui,
            tone="warning",
            header="Existing CDC infrastructure found",
            body=(
                f"This account already has CDC infrastructure: {listed}. Attach to it "
                "instead of deploying a new one — a second Amazon MSK cluster is "
                "costly. Attaching re-reads its live state; a running pipeline opens "
                "straight to monitoring in the CDC step."
            ),
        )
        with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
            for name, _status in in_scope:
                def _adopt(_name=name) -> None:
                    if migration_state.adopt_cdc_stack(_name):
                        refresh()
                ui.button(  # type: ignore[attr-defined]
                    f"Attach to {name}", on_click=_adopt, icon="link"
                ).props("color=primary")


def _render_migration_type_selector(
    ui,
    migration_state,
    *,
    status,
    refresh,
    locked: Optional[bool] = None,
    lock_reason: Optional[str] = None,
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
        if running:
            return
        # Re-selecting the SAME tile still has to record the choice: the type has a
        # default (Full load only), so clicking that tile is how a user confirms it --
        # and the journey header keeps its migration-type banner hidden until an
        # explicit choice exists. Bailing out on "no change" left that user with no
        # banner at all. The substep reset / re-render stay scoped to a real change,
        # so confirming the current type does not disturb the screen.
        changed = new_type is not selected
        migration_state.set_migration_type(new_type)
        if changed:
            migration_state.set_active_substep(None)  # default for the new type
        refresh()

    # Scroll anchor: the post-Full-Load "want CDC next?" notice sits far below this
    # selector and tells the user to change the type "above". Its jump-link targets this
    # class (see MIGRATION_TYPE_ANCHOR) so they do not have to hunt for the control.
    ui.label("Migration type").classes(  # type: ignore[attr-defined]
        f"text-sm font-semibold {MIGRATION_TYPE_ANCHOR}"
    )
    # Explain WHY the choice is locked (a dead, silently-disabled control looked
    # like a bug). Prefer the reason the CALLER computed alongside ``locked``: it is
    # the same evaluation that produced the lock, so the message and the disabled
    # state cannot drift. Recomputing here without the caller's job_manager would
    # miss an in-flight connector start and leave the tiles locked with no
    # explanation. The local fallback keeps older callers working.
    if lock_reason is None:
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


def default_migration_selection(
    inventory: SourceInventory,
    generated_node_ids: Optional[Sequence[str]],
    target: Optional[TargetInventory],
    ticked_node_ids: Optional[Sequence[str]] = None,
) -> list[str]:
    """Return the tables to pre-tick in the picker before the user touches it.

    **The Schema Conversion selection wins whenever one is known.** Picking three tables
    in Step 2 and finding all eleven ticked in Step 3 is wrong: the default used to be
    "every table that exists on the target", so a target carrying tables from earlier
    runs (or a full E2E reset) silently re-selected them all and discarded the deliberate
    Step 2 selection. Worse, it defaults to migrating MORE than asked -- the wrong
    direction for a long-running load.

    Resolves the Step 2 scope exactly the way Schema Conversion's own apply does
    (``_selected_apply_names``): the committed ``generated_node_ids`` when DDL was
    generated, **else the live ``ticked_node_ids``**. Both are persisted, so this holds
    across a restart. Using only the generated ids (the first cut of this fix) still
    over-ticked every real flow that applies without pressing "Generate DDL for
    selected", or that pressed Clear afterwards -- ``generated_node_ids`` is empty there
    while the user's tick set is right there in state.

    Falls back to the target-existing set only when NEITHER is known -- a reconnect into
    a session that never ticked anything, or a schema applied out of band -- where the
    Step 2 choice is genuinely unknown and an empty default would leave the user staring
    at zero ticked tables with no explanation. Intersected with the migratable universe
    by the caller.
    """
    generated = generated_table_names(inventory, generated_node_ids)
    if generated:
        return generated
    ticked = generated_table_names(inventory, ticked_node_ids)
    if ticked:
        return ticked
    return target_existing_table_names(inventory, target)


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
    default_selection: Optional[Sequence[str]] = None,
    on_refresh: Optional[Callable[[], object]] = None,
    locked: bool = False,
    lock_reason: Optional[str] = None,
    locked_selection: Optional[Sequence[str]] = None,
) -> None:
    """Render the hierarchical table picker scoped to migratable tables (Req 5.9).

    Mirrors the Step 2 Object browser (schema -> Tables -> table leaves):
    only migratable tables -- those with a target table to load into, whether the
    DDL was generated this session or already exists on the target (Schema
    Conversion run earlier) -- are listed and tickable; tables with no target
    table are omitted entirely (and a schema with none does not appear).
    ``default_selection`` (from :func:`default_migration_selection`) is the set to
    pre-tick before the user touches the picker: this session's Schema Conversion
    choice when there is one, else the tables already on the target. When it is
    ``None`` the renderer falls back to ``target_existing`` (the pre-existing
    behaviour, kept so a caller that only knows the target set still works).
    ``target_existing`` remains the "has a destination table" universe. Pre-selected
    tables can be unticked. The ticked set is
    persisted to the session selection so Full Load / CDC / prerequisite checks
    run over exactly the chosen tables (Property 16). ``on_refresh``, when given,
    re-introspects this session's source/target so the browser reflects the
    latest schema (e.g. tables just created on the target in Step 2).

    ``locked`` freezes the picker; ``lock_reason`` (from
    :func:`selection_lock_reason`) is the cause + remedy shown on the lock icon's
    tooltip. The causes differ materially -- a finished Full Load, live CDC, and
    deployed CDC infrastructure each need a different remedy -- so the caller passes
    the reason rather than this renderer guessing one.
    """
    with ui.row().classes("items-center gap-1"):
        ui.label("Tables to migrate").classes("text-sm font-semibold")
        # "Why only tables?" is useful context but reads as clutter when always on --
        # move it to a hover ⓘ next to the title (matches the app's other header
        # tooltips) instead of a standing paragraph.
        ui.icon("info").classes("text-gray-400 text-sm cursor-help").tooltip(
            "Only tables are listed — they hold the row data. Views, triggers and "
            "routines have no data of their own; they are created in Schema "
            "Conversion, not migrated in this step."
        )
        # The refresh button is hidden once locked: re-introspecting could change the
        # migratable set out from under a committed migration (a running/finished load,
        # or the connector + partition plan CDC was deployed with).
        if on_refresh is not None and not locked:
            ui.button(on_click=on_refresh).props(
                "flat dense round size=sm icon=refresh"
            ).tooltip("Refresh source/target objects")
        if locked:
            # The lock icon's tooltip carries the reason + how to change it, so no
            # separate standing "Locked — …" paragraph is needed below. The reason is
            # per-cause (see selection_lock_reason); the fallback only covers a caller
            # that passes locked=True without one.
            ui.icon("lock", color="grey").classes("text-sm").tooltip(
                lock_reason
                or "Locked — this table set is already committed to a migration."
            )
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

    # Describe the ACTUAL pre-tick set, and say where it came from. This used to count
    # ``target_existing`` and always claim "already on the target", which stopped being
    # true once the default became this session's Schema Conversion selection -- and was
    # misleading even before, since it described the default rather than what was ticked.
    default_set = set(
        default_selection if default_selection is not None else target_existing
    )
    pre_selected = [n for n in migratable if n in default_set]
    # Name the real origin. ``default_migration_selection`` FALLS BACK to the
    # target-existing set when this session generated nothing (a reconnect / schema
    # applied out of band), so "came from Schema Conversion" cannot be inferred from
    # the parameter merely being present -- it has to differ from the target set.
    # Claiming otherwise told a reconnected user their 11 ticked tables were a Step 2
    # choice they never made in this session.
    origin = (
        "selected in Schema Conversion"
        if default_selection is not None
        and set(default_selection) != set(target_existing)
        else "already on the target"
    )
    ui.label(
        f"Pre-selected: {len(pre_selected)} of {len(migratable)} table(s) "
        f"{origin} — tick or untick to change."
    ).classes("text-xs text-gray-400")

    migratable_order = list(migratable)
    if locked and locked_selection is not None:
        # Locked because CDC is live: the connectors stream a FIXED table set, so the
        # browser must reflect THAT set -- not the generic "everything on the target"
        # default. Without this, a reconnect (which resets selection_touched paths)
        # shows every migratable table ticked and frozen, misrepresenting what CDC is
        # actually replicating. Intersect with the migratable universe so only real,
        # tickable leaves are marked. (Only the caller's live-CDC branch passes this;
        # the other lock causes keep the normal selection view.)
        locked_set = set(locked_selection)
        effective = [name for name in migratable_order if name in locked_set]
    else:
        effective = effective_migration_selection(
            migratable_order,
            migration_state.selection,
            touched=migration_state.selection_touched,
            default=list(default_set),
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
    retry_tables=None,
    ai_error_opener=None,
    schema_recreate_candidates: Optional[Sequence[str]] = None,
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

    ``schema_recreate_candidates`` names the tables whose target the load will DROP and
    recreate to apply a primary key that differs from the source (see
    :func:`schema_recreate_tables`). Passed in because resolving it needs the inventory
    and the applied conversions, which the caller owns; the confirm dialog discloses
    whichever of them the current action covers.
    """
    ui.label("Workloads to migrate").classes("text-sm font-semibold")
    ui.label(format_selected_workloads(selected_names)).classes(
        "text-sm text-gray-600"
    )
    if selected_names:
        with ui.row().classes("items-center gap-1 flex-wrap"):
            for name in selected_names:
                ui.badge(name).props("color=blue-grey-6 outline")
    # No explanatory caption here. Its three claims are each already stated somewhere the
    # reader is better served:
    #   * "ticked in the object browser above" -- the picker's own caption says where the
    #     selection came from, and the badges listing it sit directly above this line;
    #   * "as of one consistency watermark" -- the Export watermark panel shows the actual
    #     coordinate, not just the promise of one;
    #   * "the source is read only" -- the confirm dialog says it at the moment the user
    #     commits, which is where a reassurance about writes actually matters.
    # Restating all three as standing grey text taught nothing on the second read.

    # Explicit confirmation before the (target-writing) Full Load begins. Tables
    # that already hold data offer a Drop-vs-Append choice; CDC-live is a hard
    # warning. Both are computed fresh inside the dialog builder below (so the
    # periodic poll re-render can't tear a stale value into the dialog).

    def _open_confirm_dialog_now(
        *, action_tables, on_confirm, title, table_reasons=None
    ) -> None:
        """Build + open the Full-Load confirm dialog in the TOP-LEVEL client.

        Shared by Start, Re-run-all, Retry-failed and per-table Reload:
        ``action_tables`` is the set being (re)loaded (drives the summary/badges),
        ``on_confirm`` is the action to run when confirmed, and ``title`` is the
        dialog heading. Built in the client context (not the per-render content
        slot) and opened on demand, so the periodic progress-poll re-render does
        NOT tear it down a couple of seconds after it appears. The Drop-vs-Append
        choice (when the target already holds data) applies to whichever action
        this dialog wraps, so a retry/Reload honors the SAME choice as Start.

        When ``table_reasons`` (a ``{table: failure_reason}`` map) is given -- the
        Retry-failed case -- the dialog renders ``action_tables`` as a **checklist**
        (all pre-checked) with each table's failure reason, so the user can uncheck
        tables they aren't ready to retry (e.g. a not-yet-fixed source value) and
        retry only the rest. ``on_confirm`` is then called with the checked subset;
        otherwise it is called with no arguments.
        """
        from nicegui import context as _ctx

        client = _ctx.client
        tables_with_data_now = sorted(migration_state.tables_with_data)
        cdc_live_now = cdc_streaming_started(migration_state, job_manager)
        # Empty targets whose primary key the load will RECREATE from the applied DDL.
        # A changed key is a schema change, so appending cannot deliver it; recreating an
        # empty table destroys nothing, which is why the load does it without asking --
        # but it must still be disclosed here, because a target DDL the user edited by
        # hand after Schema Conversion is replaced. Never includes a populated table:
        # those go through the explicit Drop & reload choice below. Resolved by the
        # caller (which owns the inventory + applied conversions) and narrowed to the
        # tables THIS dialog is about; a live sink forbids recreating anything.
        #
        # ``schema_recreate_candidates`` MUST be a callable, resolved here -- after the
        # pre-dialog probe has read the targets' real primary keys -- because a target
        # that already carries the applied key needs no recreate, and announcing one
        # contradicts what the user just did in Schema Conversion. Evaluating it at
        # render time (as a plain list) can only compare the applied DDL against the
        # SOURCE key, which is exactly what produced the false disclosure.
        #
        # Rejected rather than tolerated: accepting a list too would let a caller
        # silently regress to render-time evaluation, and the dialog would still look
        # correct while disclosing a recreate that does not happen.
        if not callable(schema_recreate_candidates):
            raise TypeError(
                "schema_recreate_candidates must be a callable resolved after the "
                "target probe, not a pre-computed list -- a list can only have been "
                "built before any target primary key was read."
            )
        _candidates = schema_recreate_candidates() or ()
        recreate_now = (
            [] if cdc_live_now else [n for n in _candidates if n in set(action_tables)]
        )
        selectable = table_reasons is not None
        # Live set of checked tables (mutated by the per-row checkboxes). Starts as
        # the full action set (all pre-checked) -- the common "retry everything".
        checked: set = set(action_tables)

        def _build() -> None:
            with ui.dialog() as confirm_dialog, ui.card().classes("min-w-[360px]"):
                ui.label(title).classes("text-lg font-semibold")
                ui.label(
                    f"{format_selected_workloads(action_tables)}. The target "
                    "tables will receive the snapshot rows; the source is accessed "
                    "read only."
                ).classes("text-sm")
                if cdc_live_now:
                    render_notice(
                        ui,
                        tone="error",
                        header="CDC is currently streaming",
                        body=(
                            "Re-running Full Load now will collide with the live "
                            "pipeline -- the snapshot (and any DROP+recreate) writes "
                            "to tables the CDC sink is actively applying changes to, "
                            "which can drop streamed rows or create a gap/overlap "
                            "(CDC resumes from the ORIGINAL watermark, not this new "
                            "one). Stop CDC first (CDC step → Stop CDC), re-run the "
                            "Full Load, then start CDC again so it resumes from the "
                            "new snapshot."
                        ),
                    )
                # Retry-failed: a checklist (all pre-checked) with each table's
                # failure reason, so the user can uncheck tables not ready to retry
                # and retry only the rest. Other actions just show badges.
                if selectable and action_tables:
                    ui.label(
                        "Uncheck any table you're not ready to retry yet (e.g. a "
                        "source value you haven't fixed):"
                    ).classes("text-xs text-gray-500")
                    with ui.column().classes(
                        "w-full gap-1 max-h-60 overflow-auto"
                    ):
                        for name in action_tables:
                            reason = (table_reasons or {}).get(name, "")

                            def _toggle(e: object, n=name) -> None:
                                if bool(getattr(e, "value", False)):
                                    checked.add(n)
                                else:
                                    checked.discard(n)
                                _sync_confirm_enabled()

                            with ui.row().classes("items-start gap-2 no-wrap w-full"):
                                ui.checkbox(value=True, on_change=_toggle).props(
                                    "dense"
                                )
                                with ui.column().classes("gap-0 min-w-0"):
                                    ui.label(name).classes(
                                        "text-sm font-medium text-gray-900"
                                    )
                                    if reason:
                                        ui.label(reason).classes(
                                            "text-xs text-red-700 break-all"
                                        )
                elif action_tables:
                    with ui.row().classes("items-center gap-1 flex-wrap"):
                        for name in action_tables:
                            ui.badge(name).props("color=blue-grey-6 outline")
                # Disclose the empty targets whose schema will be recreated to deliver
                # the primary key chosen in Schema Conversion. No choice is offered --
                # the table is empty, so there is nothing to lose and nothing to decide,
                # and appending simply cannot apply a new key -- but doing it silently
                # would hide that a hand-edited target DDL is about to be replaced.
                if recreate_now:
                    listed = ", ".join(recreate_now[:6]) + (
                        f" +{len(recreate_now) - 6} more"
                        if len(recreate_now) > 6
                        else ""
                    )
                    _noun = "table" if len(recreate_now) == 1 else "tables"
                    _verb = "uses" if len(recreate_now) == 1 else "use"
                    render_notice(
                        ui,
                        tone="info",
                        header=(
                            f"{len(recreate_now)} empty {_noun} will be recreated to "
                            "apply the chosen primary key"
                        ),
                        body=(
                            f"{listed} {_verb} a primary key that differs from the "
                            "source (for example the composite key chosen to avoid hot "
                            "partitions). A primary key cannot be applied by appending, "
                            "so each of these tables is dropped and recreated from your "
                            "applied Schema Conversion before loading. They hold no rows "
                            "on the target, so no data is lost — but any manual change "
                            "made to the target table outside Schema Conversion is "
                            "replaced."
                        ),
                    )
                # When selected targets already hold data (and CDC is not live),
                # let the user choose the run-wide behavior: append (keep existing
                # rows, load only the missing ones -- the non-destructive default)
                # or drop & reload (DROP+recreate each first, for a clean load).
                if tables_with_data_now and not cdc_live_now:
                    with ui.card().classes(
                        "w-full bg-amber-50 border border-amber-200 gap-2"
                    ):
                        ui.label(
                            f"{len(tables_with_data_now)} selected table(s) already "
                            "contain data on the target. Choose how to load them:"
                        ).classes("text-sm text-gray-800")
                        with ui.row().classes("items-center gap-1 flex-wrap"):
                            for name in tables_with_data_now:
                                ui.badge(name).props("color=amber-8 outline")
                        reload_choice = ui.radio(
                            {
                                "append": "Append — keep existing rows, load only "
                                "the missing ones (idempotent; recommended)",
                                "drop": "Drop & reload — DROP and recreate these "
                                "tables first for a clean load (existing rows are "
                                "permanently lost; DSQL has no TRUNCATE)",
                            },
                            value=migration_state.reload_mode,
                            on_change=lambda e: migration_state.set_reload_mode(
                                str(getattr(e, "value", "append"))
                            ),
                        ).props("dense").classes("text-sm")
                        reload_choice.classes("w-full")
                        # Reassure the user that a Drop & reload recreates the target
                        # from the APPLIED schema conversion -- including any edits
                        # they made in Schema Conversion -- not a stale/original DDL.
                        ui.label(
                            "Drop & reload recreates each table from your applied "
                            "Schema Conversion (including any edits you made there), "
                            "then rebuilds its secondary indexes after loading."
                        ).classes("text-xs text-gray-500")

                def _confirm() -> None:
                    if selectable and not checked:
                        return  # guard: nothing selected (button is also disabled)
                    confirm_dialog.close()
                    # Retry-failed passes the checked subset (in the original order);
                    # every other action calls on_confirm with no arguments.
                    if selectable:
                        chosen = [n for n in action_tables if n in checked]
                        on_confirm(chosen)
                    else:
                        on_confirm()

                with ui.row().classes("justify-end w-full gap-2"):
                    ui.button("Cancel", on_click=confirm_dialog.close).props("flat")
                    if cdc_live_now:
                        confirm_label = "Re-run anyway (CDC is live)"
                    elif tables_with_data_now:
                        # The button label + color follow the current choice so a
                        # destructive drop reads as destructive.
                        drop_chosen = migration_state.reload_mode == "drop"
                        confirm_label = (
                            "Drop, recreate and load" if drop_chosen
                            else "Append and load"
                        )
                    elif recreate_now:
                        # Name the DDL step in the button too, so the action is visible
                        # without relying on the notice above being read. Stays
                        # primary-coloured: these targets are empty, so nothing is lost.
                        confirm_label = "Recreate and load"
                    else:
                        confirm_label = "Confirm and start"
                    confirm_color = (
                        "negative"
                        if (cdc_live_now or (tables_with_data_now
                                             and migration_state.reload_mode == "drop"))
                        else "primary"
                    )
                    start_btn = ui.button(confirm_label, on_click=_confirm).props(
                        f"color={confirm_color}"
                    )

                    def _sync_confirm_enabled() -> None:
                        # Disable confirm when the retry checklist has nothing ticked.
                        if selectable and not checked:
                            start_btn.disable()
                        else:
                            start_btn.enable()

                    _sync_confirm_enabled()
                    # The label/color depend on the radio; re-render them on change
                    # so switching Drop<->Append updates the button immediately.
                    def _sync_btn(e: object) -> None:
                        drop_now = str(getattr(e, "value", "append")) == "drop"
                        start_btn.set_text(
                            "Drop, recreate and load" if drop_now
                            else "Append and load"
                        )
                        start_btn.props(
                            f"color={'negative' if drop_now else 'primary'}"
                        )
                    if tables_with_data_now:
                        reload_choice.on_value_change(_sync_btn)
            confirm_dialog.open()

        with client:  # type: ignore[attr-defined]
            _build()

    # Re-entrancy guard: the confirm handler runs a ~1-2s off-loop probe before
    # opening the dialog, so a double-click would open TWO dialogs. This flag drops
    # a second click while the first is still resolving; the clicked button is also
    # disabled + relabeled "Checking…" for a visible cue (restored on dialog open).
    _confirm_busy = {"value": False}

    async def _open_full_load_confirm(
        event: object = None,
        *,
        probe_tables=None,
        on_confirm=None,
        title="Start Full Load?",
        table_reasons=None,
    ) -> None:
        """Probe which target tables hold data, then open the confirm dialog.

        Shared by Start, Re-run-all, Retry-failed and per-table Reload. Runs the
        read-only non-empty probe off the event loop (over ``probe_tables`` -- the
        tables this action will touch, defaulting to the full selection for
        Start/Re-run), records the result (drives the Drop-vs-Append choice + the
        run's replace set), then opens the confirm dialog in the top-level client
        context. ``on_confirm`` is the action to run on confirm (defaults to
        ``start_full_load``); ``title`` is the dialog heading. ``table_reasons``
        (Retry-failed) turns the dialog into a per-table checklist and makes
        ``on_confirm`` receive the checked subset.
        """
        if _confirm_busy["value"]:
            return  # a probe is already in flight -> ignore the extra click
        _confirm_busy["value"] = True
        action_tables = (
            list(probe_tables) if probe_tables is not None else list(selected_names)
        )
        confirm_action = on_confirm if on_confirm is not None else start_full_load
        btn = getattr(event, "sender", None)
        original_text = getattr(btn, "text", None) if btn is not None else None
        # Preserve the button's own icon (play_arrow for Start, restart_alt for the
        # terminal Re-run, replay for Reload) so restore does not swap it wrongly.
        original_icon = None
        if btn is not None:
            original_icon = getattr(btn, "_props", {}).get("icon")
        if btn is not None:
            try:
                btn.disable()
                btn.set_text("Checking target…")
                btn.props("icon=hourglass_top")
                btn.update()
            except Exception:  # noqa: BLE001 - cue is best-effort
                pass
        # Yield to flush the button's busy-state to the client BEFORE the
        # potentially slow target probe. Without this, NiceGUI batches the prop
        # changes and the user sees no feedback until the await below returns.
        import asyncio
        await asyncio.sleep(0)
        try:
            # Probe which action tables already hold rows. Default the run-wide
            # choice to "append" (non-destructive); the confirm dialog lets the
            # user switch to "drop" for a clean reload.
            migration_state.set_tables_with_data(frozenset())
            migration_state.set_target_primary_keys({})
            migration_state.set_reload_mode("append")
            target_config = getattr(session, "target_config", None)
            if target_config is not None and action_tables:
                from nicegui import run

                def _probe() -> tuple[frozenset[str], dict]:
                    connector = DsqlConnector(
                        target_config, aws_profile=session.aws_profile
                    )
                    names = list(action_tables)
                    found = frozenset(
                        tables_with_rows(names, connection_factory=connector.connect)
                    )
                    # Read each target's REAL primary key on the same trip. The dialog
                    # needs it to avoid announcing a recreate for a table that already
                    # carries the applied key (the state right after "Apply all to
                    # target"), and doing it here keeps the render path I/O-free.
                    keys = {
                        name: target_primary_key_columns(
                            name, connection_factory=connector.connect
                        )
                        for name in names
                    }
                    return found, keys

                try:
                    found, keys = await run.io_bound(_probe)
                    migration_state.set_tables_with_data(found)
                    migration_state.set_target_primary_keys(keys)
                except Exception:  # noqa: BLE001 - on probe failure, warn-less confirm
                    migration_state.set_tables_with_data(frozenset())
                    # Leave the key map EMPTY, not partially filled: an unprobed table
                    # is treated as unknown, so the disclosure stays conservative.
                    migration_state.set_target_primary_keys({})
            _open_confirm_dialog_now(
                action_tables=action_tables,
                on_confirm=confirm_action,
                title=title,
                table_reasons=table_reasons,
            )
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

    async def _open_reload_confirm(table_name: str, event: object = None) -> None:
        """Per-table Reload -> the same probe + Drop-vs-Append confirm dialog.

        Scopes the probe/choice to the single table being reloaded, so the user
        chooses append vs drop for THAT table (and a schema-edited target is
        recreated from the applied conversion on drop).
        """
        await _open_full_load_confirm(
            event,
            probe_tables=[table_name],
            on_confirm=lambda n=table_name: reload_table(n),
            title=f"Reload {table_name}?",
        )

    # The watermark, object browser, prerequisites, and buttons are STATIC: they
    # are rendered once per full render and must NOT be rebuilt by the 0.5s poll,
    # or user-expanded sections (snapshot row counts, prerequisite categories)
    # would keep collapsing. Only this live region -- the per-table progress,
    # caption, completeness verdict, and error-log summary -- is refreshed on each
    # poll tick; the region re-arms its own one-shot timer while the job runs.
    # The per-table progress table is rebuilt on every ~1.5s poll (it lives inside
    # the refreshable _live_detail). A freshly-built ui.table resets to page 1, so
    # without persisting the page a user browsing page 2+ is yanked back on each
    # tick. This holder lives in _render_full_load_step (NOT rebuilt by the poll --
    # only _live_detail is), so the chosen page survives the rebuild; the table
    # seeds its pagination from it and writes back via on_pagination_change.
    # It carries rowsPerPage too: without that, raising "Records per page" was undone
    # by the very next poll tick, so the setting looked broken.
    _progress_page = {"page": 1, "rowsPerPage": 10}

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
        # Full Load records ONLY: CDC writes under this same job_id (cdc_error_log_key),
        # so an unfiltered read counts dead-lettered rows as Full Load failures.
        summary = full_load_error_summary(migration_state.error_log, current.job_id)
        rows = build_full_load_table_rows(
            current,
            summary,
            full_load_latest_messages(migration_state.error_log, current.job_id),
        )
        _render_full_load_progress(
            ui,
            current,
            rows,
            reload_table=reload_table,
            reload_confirm=_open_reload_confirm,
            quarantine_only=_incomplete_is_quarantine_only(
                current, migration_state.error_log
            ),
            # EVERY dropped row, not one per table: ``latest_messages()`` keeps only the
            # last message per table, so a table that dropped 3 rows listed exactly 1 --
            # and the count above it disagreed with the list below.
            quarantine_records=[
                (str(record.table), str(record.message))
                for record in full_load_error_records(
                    migration_state.error_log, current.job_id
                )
            ],
            ai_error_opener=ai_error_opener,
            page_state=_progress_page,
        )
        # The error-log download belongs with the DETAIL it serializes, not under the
        # accept button: sitting immediately below "Accept quarantined rows & continue" it
        # read as that decision's secondary option, when it is just a way to take the same
        # per-row information away with you.
        _render_error_log(ui, migration_state, current)
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
            ui,
            full_load_completeness(rows),
            approximate=approximate,
            quarantine_accepted=migration_state.accept_quarantined_rows,
        )
        # The action the banner just described, directly beneath its verdict.
        _render_accept_quarantine_action(
            ui,
            quarantine_only=_incomplete_is_quarantine_only(
                current, migration_state.error_log
            ),
            terminal=current.status in ("DONE", "FAILED", "CANCELLED"),
            quarantine_accepted=migration_state.accept_quarantined_rows,
            accept_quarantine_and_continue=accept_quarantine_and_continue,
        )
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
            # Say what is actually guaranteed. "finishing the current batch" read as a
            # promise the tool could not keep: a wedged worker once left this label up
            # indefinitely with nothing progressing, so it looked like normal shutdown
            # when the job was in fact stuck. The stop is COOPERATIVE (each worker
            # finishes its batch, then returns) and now has a bounded grace period, so
            # the honest wording is "waiting for the workers", not "almost done".
            ui.label(
                "Stopping… waiting for the in-flight batches to finish."
                if stopping
                else "Full Load in progress…"
            ).classes("text-sm text-gray-500")
            stop_btn = ui.button(
                "Stop Full Load", on_click=stop_full_load, icon="stop"
            ).props("color=negative outline")
            if stopping:
                stop_btn.disable()
                stop_btn.tooltip(
                    "Stop already requested — waiting for the in-flight batches. If a "
                    "worker does not respond, the run is torn down after a grace "
                    "period and the unfinished tables become retryable."
                )
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
        # A terminated run with ANY unfinished table (FAILED or still PENDING)
        # shows the recovery row (Retry unfinished / Re-run all) instead of a lone
        # "Re-run Full Load" -- so a crash that left tables PENDING still offers a
        # scoped retry, not just a full re-run.
        terminal_with_failures = (
            job is not None
            and status is not StepStatus.IN_PROGRESS
            and bool(unsettled_table_names(job))
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
            elif cdc_streaming_started(migration_state, job_manager):
                # CDC is live: starting/re-running Full Load would collide with the
                # running stream, so DISABLE (grey out) the button rather than let it
                # look clickable. The tooltip + hint say how to re-enable it (Stop CDC),
                # so it is a clear "not now", not a dead end.
                start_btn.disable()
                start_btn.tooltip(
                    "CDC is streaming -- starting Full Load would collide with the "
                    "live pipeline. Stop CDC first (CDC step → Stop CDC) to run it."
                )
                inline_hint(
                    ui,
                    "CDC is live, so Full Load is disabled. Stop CDC first "
                    "(CDC step → Stop CDC) to run it.",
                    tone="warning",
                )

    if job is not None:
        ui.separator()
        # Live FIRST: per-table progress, caption, completeness, error log. This is what
        # the operator is watching, so it leads -- the watermark used to sit above it and
        # pushed the progress table (and, on a finished run, the completeness verdict and
        # any quarantine detail) below a block of static reference data.
        _live_detail()
        # Static reference AFTER: the watermark is captured once at run start and never
        # changes, so it reads as provenance/audit detail rather than something to watch.
        # It stays OUTSIDE the refreshable region (only _live_detail re-renders on each
        # ~1.5s poll tick), so its snapshot-row-counts expansion is still never collapsed
        # by the poll -- the reason it was hoisted out in the first place holds either
        # way, because order within the parent does not affect what the poll rebuilds.
        _render_watermark(ui, job)

        # Terminal-only affordances (shown after the job finishes, on the full
        # refresh the poll triggers): the job-level failure reason and retry.
        if status is not StepStatus.IN_PROGRESS:
            job_error = None
            try:
                job_error = job_manager.get_error(job.job_id)
            except JobNotFoundError:
                job_error = None
            # Suppress the raw job error when the ONLY problem is quarantine: the
            # completeness banner above already gives the same facts and remedy in plain
            # language, so this box added a third telling of "3 rows were dropped" -- in
            # red, with an exception class name, contradicting the amber "the rest
            # loaded" framing. A red "Load failed" also overstates a run whose only
            # incompleteness is rows that can never load. Any REAL failure still shows
            # it: that is a retryable error whose exact text matters.
            _quarantine_only_error = _incomplete_is_quarantine_only(
                job, migration_state.error_log
            )
            if job_error and not _quarantine_only_error:
                render_notice(
                    ui,
                    tone="error",
                    header="Load failed",
                    body=job_error,
                )

            # Recovery set = every table that did NOT finish (FAILED *or* still
            # PENDING). A fatal/aborted run (e.g. a run-level crash before the big
            # tables were attempted) leaves them PENDING, not FAILED -- so keying
            # recovery off FAILED alone would strand them with only a full "Re-run"
            # as escape. ``unsettled`` resumes exactly the unfinished tables.
            failed = unsettled_table_names(job)
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

                    # Per-table reason (cause) for the retry checklist, so the user
                    # can see WHY each table is unfinished before deciding whether to
                    # retry it now. FAILED tables carry their error-log message; a
                    # still-PENDING table was never attempted (the run ended first),
                    # so it gets a plain "not yet loaded" note.
                    _failure_reasons = full_load_latest_messages(
                        migration_state.error_log, job.job_id
                    )
                    _pending_names = {
                        c.chunk_id for c in job.chunks if c.status == "PENDING"
                    }

                    def _reason_for(name: str) -> str:
                        msg = _failure_reasons.get(name, "")
                        if msg:
                            return msg
                        if name in _pending_names:
                            return "Not loaded yet — the previous run ended first."
                        return ""

                    async def _confirm_retry_failed(event: object = None) -> None:
                        # Route the retry through the same confirm dialog as Start,
                        # as a CHECKLIST (all pre-checked) of the UNFINISHED tables +
                        # their reasons. Probing/Drop-vs-Append and the retry are
                        # scoped to the CHECKED subset, so the user can retry only the
                        # tables they're ready for.
                        retry_fn = retry_tables or (lambda _names: retry_failed_load())
                        await _open_full_load_confirm(
                            event,
                            probe_tables=list(failed),
                            on_confirm=retry_fn,
                            title=f"Retry {len(failed)} unfinished table(s)?",
                            table_reasons={n: _reason_for(n) for n in failed},
                        )

                    retry_btn = ui.button(
                        f"Retry unfinished tables ({len(failed)})",
                        on_click=_confirm_retry_failed,
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
            # A bare "connector X failed" is useless for troubleshooting -- it says a
            # failure happened but nothing about WHY, and the reason lives only in
            # CloudWatch/the in-memory error summary (gone after a restart). Attach the
            # per-connector task trace / DLQ context when the poll captured it, so the
            # durable log carries the cause and where to look next.
            detail = None
            if norm == "FAILED":
                detail = _connector_failure_detail(migration_state, view, name)
            _log_cdc_event(
                f"connector {name} {norm.lower()}",
                status=(
                    ActivityStatus.SUCCESS
                    if norm == "RUNNING"
                    else ActivityStatus.FAILURE
                ),
                detail=detail,
            )
    migration_state._last_logged_connector_states = {
        n: str(s).upper() for n, s in states.items()
    }


def _connector_failure_detail(migration_state, view, name: str) -> str:
    """Build the troubleshooting detail for a FAILED connector activity entry.

    Gathers whatever the last poll captured -- the connector's task error trace, the DLQ
    depth, and the latest per-table data errors -- into one durable line. Best-effort by
    design: every source is optional (a poll may not have reached CloudWatch yet), so a
    missing piece degrades the message instead of dropping the entry. Credential-free
    (Property 7): reasons and counts only, never row values.
    """
    parts: list[str] = []
    # Peer connector states: a sink failure while the source still runs (or vice versa)
    # localizes the fault to one side of the pipeline, which is the first thing you want
    # to know when reading this entry weeks later.
    peers = {
        peer: str(state).upper()
        for peer, state in (getattr(view, "connector_states", None) or {}).items()
        if peer != name
    }
    if peers:
        parts.append(
            "other connectors: "
            + ", ".join(f"{p}={s}" for p, s in sorted(peers.items()))
        )
    depth = getattr(view, "dlq_depth", None)
    if depth:
        parts.append(
            f"{depth} record(s) in the DLQ (rejected permanently, e.g. a value over "
            "DSQL's ~1 MiB per-value limit)"
        )
    summary = getattr(view, "error_summary", None)
    by_table = dict(getattr(summary, "errors_by_table", None) or {})
    if by_table:
        listed = ", ".join(
            f"{table}={count}" for table, count in sorted(by_table.items())[:5]
        )
        parts.append(f"data errors by table: {listed}")
    parts.append(
        "see the connector's CloudWatch log group (linked on the CDC step) for the "
        "task stack trace"
    )
    return "; ".join(parts)


def _quarantined_row_count(job: "MigrationJob", error_log: "ErrorLogStore") -> int:
    """Count permanently-quarantined rows for ``job``.

    Prefers the count recorded on the JOB's chunks, because that is what survives an app
    restart: the job store is durable while :class:`ErrorLogStore` is in-memory only.
    Reading the log alone made this return 0 for a restored session, which flipped
    :func:`_incomplete_is_quarantine_only` to ``False`` and hid the
    "Accept quarantined rows & continue" button -- the exact action the failure message
    tells the operator to use. That was a complete dead end: the run cannot be retried
    into success (a permanently-rejected value never loads) and the only escape left was
    Start over.

    Falls back to counting the log's ``quarantined row pk[...]`` entries so a job written
    by an older version (no per-chunk count) still works.
    """
    if job is None:
        return 0
    from_chunks = sum(
        getattr(chunk, "rows_quarantined", 0) or 0 for chunk in job.chunks
    )
    if from_chunks:
        return from_chunks
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
    so there are NO unfinished chunks; the run is incomplete only because rows were
    permanently dropped. That is the one case the accept-and-continue override may
    unblock -- any UNFINISHED table (a FAILED chunk, or one still PENDING because
    the run ended before it loaded) is retryable work and must still block.
    """
    if job is None:
        return False
    if unsettled_table_names(job):  # retryable/unfinished work is present
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


def _abbrev_count(n: "Optional[int]") -> str:
    """Compact count for a dense table: 1,180,000 -> "1.18M", 33585832 -> "33.6M".

    Keeps small numbers exact (with thousands separators) and abbreviates large
    ones to 3 significant figures so the "Rows (target / source)" column stays a
    single narrow line regardless of scale. ``None`` -> an em dash.
    """
    if n is None:
        return "—"
    n = int(n)
    if n < 100_000:
        return f"{n:,}"
    for div, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if n >= div:
            val = n / div
            # 3 sig figs: 1.18M, 33.6M, 747K
            return f"{val:.2f}{suffix}" if val < 10 else f"{val:.1f}{suffix}"
    return f"{n:,}"


def _rows_target_source_cell(row: "FullLoadTableRow") -> str:
    """Compact "<on target> / <source>" for the merged Rows column.

    Both counts abbreviated (:func:`_abbrev_count`) so the cell is one short line;
    the exact figures + the new/already-there breakdown live in the cell tooltip
    (:func:`_rows_breakdown_tooltip`).
    """
    return f"{_abbrev_count(row.rows_present)} / {_abbrev_count(row.expected_rows)}"


def _quarantined_cell_tooltip(row: "FullLoadTableRow") -> str:
    """Hover text for a table's "N dropped" badge (empty when nothing was dropped).

    Says what happened, that the rest of the table DID load (a quarantining table is
    ``DONE``, not failed), and where to act -- so the badge is self-explanatory instead
    of sending the reader hunting for the panel below the table.
    """
    dropped = row.rows_quarantined
    if dropped <= 0:
        return ""
    noun = "row was" if dropped == 1 else "rows were"
    return (
        f"{dropped:,} {noun} permanently dropped — a value Aurora DSQL could not "
        "store (e.g. over its ~1 MiB per-value limit). The rest of this table loaded "
        "normally. See the quarantine panel below for each row's primary key and "
        "reason; fix the source value and Reload this table to close the gap."
    )


def _rows_breakdown_tooltip(row: "FullLoadTableRow") -> str:
    """Exact figures + new/already-there split for the Rows cell's hover tooltip.

    Also explains the (normal) case of holding MORE rows than the source estimate:
    the watermark count is scan-free ``information_schema`` sampling and often
    undercounts, so "target > source" here is an estimate artifact, not duplicated
    data. Without this the arithmetic looks like a bug.
    """
    parts = [f"{row.rows_present:,} on target"]
    if row.rows_skipped:
        parts.append(
            f"{row.rows_loaded:,} new + {row.rows_skipped:,} already there"
        )
    if row.expected_rows is not None:
        parts.append(f"{row.expected_rows:,} source rows (estimate)")
        exceeded = row.expected_exceeded_pct
        if exceeded is not None:
            parts.append(
                f"{exceeded:.1f}% above the estimate — normal (the scan-free "
                "estimate undercounts); Validation (step 4) counts exactly"
            )
    return " · ".join(parts)


def _format_attempts_cell(row: "FullLoadTableRow") -> str:
    """Attempts, with a plain-language error marker so no Errors column is needed.

    ``"1"`` clean, ``"1 · 3 errors"`` when the table logged errors ("3 err" was cryptic --
    it read like part of the retry count).

    Quarantined rows are deliberately NOT repeated here: the same row's Status cell
    already carries a "3 dropped" badge with the full explanation on hover, so saying it
    again one column over was the same fact twice in one table row. A quarantine also
    logs an error per dropped row, so those rows still surface here as an error count.
    """
    if row.errors:
        noun = "error" if row.errors == 1 else "errors"
        return f"{row.attempts} · {row.errors} {noun}"
    return str(row.attempts)


def _parse_quarantined_pk(message: str) -> "Optional[str]":
    """Extract the primary key from a ``quarantined row pk[...]: reason`` message.

    Returns ``None`` when the message is not in that shape, so a caller falls back to
    showing it verbatim rather than mangling an unexpected format.
    """
    prefix = "quarantined row pk["
    text = str(message or "")
    if not text.startswith(prefix):
        return None
    end = text.find("]", len(prefix))
    if end == -1:
        return None
    return text[len(prefix) : end] or None


def _quarantined_reason(message: str) -> str:
    """Return just the reason from a ``quarantined row pk[...]: reason`` message."""
    text = str(message or "")
    marker = "]: "
    index = text.find(marker)
    if text.startswith("quarantined row pk[") and index != -1:
        return text[index + len(marker) :].strip()
    return text.strip()


_QUARANTINE_PK_CHIP_LIMIT = 12


def _group_quarantine_entries(
    entries: "Sequence[tuple[str, str]]",
) -> "list[tuple[str, list[str], list[str]]]":
    """Group ``(table, message)`` entries into ``(table, primary_keys, reasons)``.

    One group per TABLE, in first-seen order, because the shared facts (the table and --
    almost always -- the reason) belong stated once, and because Reload acts on the whole
    table: a per-row card offered one identical Reload button per row.

    ``reasons`` is deduplicated while preserving order: a table usually drops rows for the
    same reason (one oversized column), so this collapses to a single line; when the
    reasons genuinely differ they are all kept rather than picking one. A row whose message
    carries no parseable primary key contributes only its reason, so an unexpected format
    still surfaces instead of vanishing.
    """
    grouped: "dict[str, tuple[list[str], list[str]]]" = {}
    for table, message in entries:
        pks, reasons = grouped.setdefault(table, ([], []))
        pk = _parse_quarantined_pk(message)
        if pk:
            pks.append(pk)
        reason = _quarantined_reason(message)
        if reason and reason not in reasons:
            reasons.append(reason)
    return [(table, pks, reasons) for table, (pks, reasons) in grouped.items()]


def _quarantine_detail_row(
    ui,
    *,
    table: str,
    primary_keys: "Sequence[str]" = (),
    reasons: "Sequence[str]" = (),
    action=None,
) -> None:
    """Render one table's dropped rows: the table, every primary key, and the reason(s).

    Replaces a run-on log line ("quarantined row pk[id=3]: datatype limit greater than
    1048576 bytes not supported for bytea") in which the table name sat in a badge above
    and the PK was buried mid-sentence. The facts a reader acts on -- WHICH table, WHICH
    rows, WHY -- are separately labelled, with the primary keys as monospace chips because
    they are the handles you search the source with.

    One card per table keeps this compact as the count grows: 12 chips instead of 12 cards
    each repeating the same table name, reason and Reload button. Beyond that the chips are
    truncated with a "+N more" marker -- the full list is always in the downloadable error
    log, so the screen does not need to be exhaustive.
    """
    shown = list(primary_keys[:_QUARANTINE_PK_CHIP_LIMIT])
    hidden = max(0, len(primary_keys) - len(shown))
    with ui.row().classes(
        "items-start gap-3 w-full no-wrap rounded-md border border-amber-200 "
        "bg-amber-50 p-3"
    ):
        ui.icon("report_problem").classes("text-amber-600 text-lg")
        with ui.column().classes("gap-1 flex-1 min-w-0"):
            with ui.row().classes("items-center gap-2 flex-wrap"):
                ui.label(table).classes("text-sm font-semibold text-gray-900")
                # Count first: with many chips the number is what you read, and it also
                # covers the truncated case where the chips alone under-report.
                if primary_keys:
                    noun = "row" if len(primary_keys) == 1 else "rows"
                    ui.badge(f"{len(primary_keys)} {noun} dropped").props("color=amber-8")
                else:
                    ui.badge("dropped").props("color=amber-8")
            if shown:
                with ui.row().classes("items-center gap-1 flex-wrap"):
                    for pk in shown:
                        ui.badge(pk).props("color=amber-8 outline").classes("font-mono")
                    if hidden:
                        ui.label(f"+{hidden} more").classes(
                            "text-xs text-gray-500"
                        ).tooltip(
                            "The full list of dropped primary keys is in the "
                            "downloadable Full Load error log."
                        )
            for reason in reasons:
                ui.label(reason).classes("text-xs text-gray-700 break-words")
        if action is not None:
            with ui.row().classes("items-center gap-1 no-wrap shrink-0"):
                action()


# Friendly labels for each load state, used by the status-distribution chips.
_LOAD_STATE_LABELS: dict[str, str] = {
    "DONE": "Done",
    "IN_PROGRESS": "In progress",
    "FAILED": "Failed",
    "PENDING": "Pending",
}


def _render_table_state_summary(ui, rows: "Sequence[FullLoadTableRow]") -> None:
    """Render colored chips with the table count in each load state (O(tables)).

    A quarantining table settles as ``DONE``, so the state chips alone read "Done: 8" on
    a run that permanently dropped rows -- the at-a-glance summary looked clean. An amber
    chip is appended whenever anything was dropped so the loss is visible in the same
    glance, before the reader scrolls to the table.
    """
    counts = summarize_table_states(rows)
    dropped_rows = sum(row.rows_quarantined for row in rows)
    dropped_tables = sum(1 for row in rows if row.rows_quarantined > 0)
    with ui.row().classes("items-center gap-2 flex-wrap"):
        for state in _LOAD_STATE_ORDER:
            count = counts.get(state, 0)
            if count == 0:
                continue
            color = _LOAD_STATE_COLORS.get(state, "grey")
            ui.badge(f"{_LOAD_STATE_LABELS[state]}: {count}").props(
                f"color={color}"
            ).classes("text-sm q-px-sm q-py-xs")
        if dropped_rows:
            row_noun = "row" if dropped_rows == 1 else "rows"
            table_noun = "table" if dropped_tables == 1 else "tables"
            verb = "could not be stored and was" if dropped_rows == 1 else (
                "could not be stored and were"
            )
            ui.badge(f"Dropped: {dropped_rows} {row_noun}").props(
                "color=amber-8 outline"
            ).classes("text-sm q-px-sm q-py-xs").tooltip(
                f"{dropped_rows} {row_noun} across {dropped_tables} {table_noun} "
                f"{verb} permanently dropped. Those tables are marked in the Status "
                "column; the rest of their rows loaded normally."
            )


def _render_full_load_progress(
    ui, job: MigrationJob, rows: "Sequence[FullLoadTableRow]",
    *,
    reload_table=None,
    reload_confirm=None,
    # The accept-the-gap action moved OUT of this function: it now renders after the
    # completeness banner (see _render_accept_quarantine_action), so the button that
    # carries out the banner's remedy sits directly beneath the verdict rather than
    # above it. ``quarantine_only`` is kept because the panel still gates the per-row
    # Reload on a settled, quarantine-only run.
    quarantine_only: bool = False,
    # EVERY quarantined row, as ``(table, message)`` in log order. The panel used to be
    # built from ``latest_messages()``, which keeps one message per TABLE (last write
    # wins) -- so a table that dropped 3 rows listed exactly 1, and the count above it
    # disagreed with the list below. The primary key is the actionable part of each
    # entry, so every row has to appear.
    quarantine_records: "Sequence[tuple[str, str]]" = (),
    ai_error_opener=None,
    page_state=None,
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

    # Simplified 6-column layout (was 9): the exact row breakdown and source-row
    # figure moved into the "Rows" cell tooltip; Errors merged into Attempts; the
    # redundant "Complete" column dropped (Status + Progress already convey it).
    columns = [
        {"name": "table", "label": "Table", "field": "table", "align": "left"},
        {"name": "state", "label": "Status", "field": "state", "align": "left"},
        {
            # "(est.)" belongs in the header: the source side of this cell is the
            # watermark's scan-free information_schema estimate, so a reader scanning
            # the column must not read "3.01M / 2.77M" as the target holding 240k rows
            # MORE than the source (the estimate simply undercounted).
            "name": "rows",
            "label": "Rows (target / source est.)",
            "field": "rows",
            "align": "left",
        },
        {"name": "progress", "label": "Progress", "field": "progress", "align": "left"},
        {"name": "time", "label": "Time", "field": "time", "align": "left"},
        {"name": "attempts", "label": "Attempts", "field": "attempts", "align": "left"},
    ]
    now = datetime.now(timezone.utc)
    table_rows = [
        {
            "table": row.table,
            "state": row.state,
            "state_label": _LOAD_STATE_LABELS.get(row.state, row.state),
            "state_color": _LOAD_STATE_COLORS.get(row.state, "grey"),
            # Rows permanently dropped for this table. A quarantining table finishes
            # DONE, so its status badge was identical to a clean table's -- the only
            # hint was one amber panel below the whole table, which does not say WHICH
            # row it belongs to once the table is paginated. 0 renders no badge.
            "quarantined": row.rows_quarantined,
            "quarantined_tooltip": _quarantined_cell_tooltip(row),
            "rows": _rows_target_source_cell(row),
            "rows_tooltip": _rows_breakdown_tooltip(row),
            "progress": _format_progress_cell(row),
            "progress_value": (
                None if row.progress_pct is None else round(row.progress_pct / 100.0, 4)
            ),
            "time": format_table_timing(row, now),
            "attempts": _format_attempts_cell(row),
        }
        for row in rows
    ]
    # `wrap-cells` lets long cells (headers like "Time (ETA / total)") wrap to a
    # second line instead of forcing the row wider than the card, and `dense`
    # tightens padding -- together the 9 columns fit the card width so the table
    # never shows a bottom horizontal scrollbar.
    # Seed pagination from the persisted page (survives the ~1.5s poll rebuild) so
    # a user browsing page 2+ is not yanked back to page 1 on every tick. The page
    # is clamped to the current row count (a shrinking table can't leave you on a
    # now-empty page). ``on_pagination_change`` writes the user's page back.
    # BOTH the page and the rows-per-page must be persisted. The table is rebuilt on
    # every ~1.5s poll tick, so any pagination value that is hardcoded here is silently
    # restored on the next tick: picking a larger "Records per page" appeared to do
    # nothing (and the reverting select looked like the table was refreshing itself).
    _rows_per_page = 10
    _saved_page = 1
    if isinstance(page_state, dict):
        _rows_per_page = int(page_state.get("rowsPerPage", _rows_per_page))
        # rowsPerPage == 0 is Quasar's "All" option: everything on one page.
        _max_page = (
            max(1, -(-len(table_rows) // _rows_per_page))  # ceil-div
            if _rows_per_page > 0
            else 1
        )
        _saved_page = min(max(1, int(page_state.get("page", 1))), _max_page)

    def _on_pagination_change(event: object) -> None:
        if isinstance(page_state, dict):
            value = getattr(event, "value", None) or {}
            page_state["page"] = int(value.get("page", page_state.get("page", 1)))
            page_state["rowsPerPage"] = int(
                value.get("rowsPerPage", page_state.get("rowsPerPage", 10))
            )

    table = ui.table(
        columns=columns,
        rows=table_rows,
        row_key="table",
        pagination={"rowsPerPage": _rows_per_page, "page": _saved_page},
        on_pagination_change=_on_pagination_change,
    ).props("wrap-cells dense").classes("w-full")
    # Colored status badge per row (visualizes each table's load state), plus an amber
    # "N dropped" badge when rows were permanently quarantined. A quarantining table
    # finishes DONE, so without this it is indistinguishable from a clean one in the
    # Status column -- the amber panel below the table says a drop happened but not on
    # which row, and it scrolls out of view / the row can be on another page.
    table.add_slot(
        "body-cell-state",
        r"""
        <q-td :props="props">
          <div class="row items-center no-wrap" style="gap:4px">
            <q-badge :color="props.row.state_color" :label="props.row.state_label" />
            <q-badge v-if="props.row.quarantined > 0" color="amber-8" outline
                     class="items-center">
              <q-icon name="report_problem" size="12px" class="q-mr-xs" />
              {{ props.row.quarantined }} dropped
              <q-tooltip>{{ props.row.quarantined_tooltip }}</q-tooltip>
            </q-badge>
          </div>
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
    # Rows cell: the compact "target / source" value with the exact figures +
    # new/already-there breakdown on hover, so the detail is available without
    # widening the column or wrapping to a second line.
    table.add_slot(
        "body-cell-rows",
        r"""
        <q-td :props="props">
          <span>{{ props.value }}</span>
          <q-tooltip>{{ props.row.rows_tooltip }}</q-tooltip>
        </q-td>
        """,
    )
    # Header ⓘ on the Rows column (same idiom as the CDC status table): explain that
    # the source side is an estimate right where the eye is, so "target / source"
    # arithmetic that looks off is understood without hovering a cell.
    table.add_slot(
        "header-cell-rows",
        r"""
        <q-th :props="props">
          {{ props.col.label }}
          <q-icon name="info" size="14px" class="q-ml-xs text-grey-5"
            style="cursor:help">
            <q-tooltip class="text-body2" style="max-width:340px">
              Rows now on the target vs. the source row count recorded on the export
              watermark. The source figure is a scan-free information_schema
              ESTIMATE (InnoDB index sampling), so it is commonly off by a few
              percent and often UNDERCOUNTS — the target legitimately exceeding it is
              normal, not duplicated data. A finished table is 100% because the
              loader streamed it to exhaustion, not because the two numbers match.
              Validation (step 4) does the exact COUNT(*) comparison.
            </q-tooltip>
          </q-icon>
        </q-th>
        """,
    )

    # Surface the failure cause inline: list each failed table with its latest
    # error message so the user can diagnose without downloading the log. Always
    # shown (not collapsible) so the live poll re-render never hides it.
    failures = [row for row in rows if row.error_message]
    quar_prefix = "quarantined row pk["
    # An index that could not be created is recorded against the table like any other
    # error-log entry, but it is NOT a load failure: every row is present and only an
    # access path is missing. Split it out so it is reported as its own warning instead
    # of appearing among the failed tables (which would contradict the table's DONE
    # state and imply data was lost).
    index_prefix = "index not created: "
    index_only = [
        r for r in failures if str(r.error_message).startswith(index_prefix)
    ]
    real_failures = [
        r
        for r in failures
        if not str(r.error_message).startswith(quar_prefix)
        and not str(r.error_message).startswith(index_prefix)
    ]
    quarantined = [
        r for r in failures if str(r.error_message).startswith(quar_prefix)
    ]
    terminal = job.status in ("DONE", "FAILED", "CANCELLED")

    def _ai_btn(table_name: str, error_message: str) -> None:
        # Per-table AI Assist: opens the chat drawer to explain THIS failure's
        # cause + fix. Shown enabled only when AI Assist is on (an opener was
        # threaded in); otherwise a disabled, discoverable affordance points at
        # the Connect screen -- mirroring Schema Conversion / Validation.
        if ai_error_opener is not None:
            ui.button(
                "AI Assist",
                on_click=lambda n=table_name, e=error_message: ai_error_opener(n, e),
            ).props(
                "flat dense no-caps size=sm color=indigo-6 icon=auto_awesome"
            ).tooltip("Ask AI why this table failed and how to fix it.")
        else:
            ui.button("AI Assist").props(
                "flat dense no-caps size=sm color=grey icon=auto_awesome"
            ).props("disable").tooltip(
                "Enable AI Assist on the Connect screen to diagnose this "
                "failure with AI."
            )

    def _failure_row(*, table_name, message, tone, action=None) -> None:
        # One failure/quarantine entry with a STABLE layout that doesn't shift when
        # the error text is long: the table badge + wrapping message sit in a
        # flex-1 column on the left, and the fixed-width action (AI Assist, and for
        # quarantine a Reload) is pinned top-right -- so buttons never get pushed to
        # a second line by a long message (the old row-nowrap did).
        with ui.row().classes("items-start gap-2 w-full no-wrap"):
            with ui.column().classes("gap-0 flex-1 min-w-0"):
                # negative = a real failure, warning = quarantine (rows dropped),
                # info = data complete but an index is missing.
                _badge_color = {
                    "error": "negative",
                    "warning": "warning",
                    "info": "info",
                }.get(tone, "warning")
                ui.badge(table_name).props(f"color={_badge_color} outline")
                inline_hint(ui, message, tone=tone, classes="text-xs break-words")
            if action is not None:
                with ui.row().classes("items-center gap-1 no-wrap shrink-0"):
                    action()

    # Real, retryable table failures (red). Retry is driven by the single
    # "Retry unfinished tables" control below (a checklist), so no per-row Reload
    # here -- only per-table AI diagnosis, which is inherently per-table.
    if real_failures:
        with ui.column().classes("w-full gap-2"):
            render_notice(
                ui,
                tone="error",
                header=f"Failure details ({len(real_failures)})",
            )
            for row in real_failures:
                _failure_row(
                    table_name=row.table,
                    message=row.error_message,
                    tone="error",
                    action=(lambda r=row: _ai_btn(r.table, r.error_message or "")),
                )

    # Quarantined rows (amber): the table loaded -- these rows were permanently
    # dropped (e.g. a value over DSQL's ~1 MiB per-value limit), NOT a failure.
    # A quarantine table is DONE (not "unfinished"), so the retry checklist does
    # not cover it; a per-row Reload stays here for "I fixed the source value,
    # reload just this table".
    def _quar_reload(table_name: str):
        def _btn() -> None:
            if reload_confirm is not None and terminal:
                ui.button(
                    "Reload",
                    on_click=lambda e, n=table_name: reload_confirm(n, e),
                ).props(
                    "flat dense no-caps size=sm color=primary icon=replay"
                ).tooltip(
                    "Reload just this table (e.g. after fixing the source value)."
                )
            elif reload_table is not None and terminal:
                ui.button(
                    "Reload", on_click=lambda n=table_name: reload_table(n)
                ).props(
                    "flat dense no-caps size=sm color=primary icon=replay"
                ).tooltip(
                    "Reload just this table (e.g. after fixing the source value)."
                )
        return _btn

    if index_only:
        # Data-complete, index-missing. Deliberately its own INFO-toned block, apart
        # from failures and quarantine: nothing is missing from the target, so the
        # migration can proceed -- but the operator has to know an index they asked
        # for does not exist, since queries relying on it will be slow.
        with ui.column().classes("w-full gap-2"):
            render_notice(
                ui,
                tone="info",
                header=(
                    f"Indexes not created ({len(index_only)}) — the data loaded "
                    "completely"
                ),
                body=(
                    "Every row is on the target; only these secondary indexes are "
                    "missing, so no data was lost and you do not need to re-run the "
                    "load. Add them later, or reduce the table's index count — Aurora "
                    "DSQL allows 24 indexes per table, including the primary key. "
                    "Queries that depend on a missing index will be slower until it "
                    "exists."
                ),
            )
            for row in index_only:
                _failure_row(
                    table_name=f"{row.table} · Done — index missing",
                    message=row.error_message,
                    tone="info",
                )

    # Prefer the FULL per-row records: one entry per dropped row. Falls back to the
    # per-table view when the caller passed none (an older call site, or a restored
    # session whose in-memory log is gone) -- one entry per table is still better than
    # nothing, and the count above remains authoritative either way.
    quarantine_entries: "list[tuple[str, str]]" = [
        (table, message)
        for table, message in (quarantine_records or ())
        if str(message).startswith(quar_prefix)
    ] or [(r.table, str(r.error_message)) for r in quarantined]
    if quarantine_entries:
        # NO header notice here. The completeness banner below already states the verdict
        # ("N rows permanently dropped (table)") and the remedy, and the summary chip +
        # per-row Status badge state the count above -- a header repeating it made a
        # 3-run drop announced in four boxes on one screen. This section's job is the
        # per-row DETAIL (which row, why, and Reload), which nothing else provides.
        #
        # GROUPED BY TABLE, one card per table. A card per ROW repeated the table name and
        # the reason once per row, and -- worse -- showed a "Reload" button per row when
        # Reload is per-TABLE: three identical buttons doing the same thing, each looking
        # like it acted on its own row. Grouping states the shared facts once and lists the
        # primary keys as chips, which stays compact when a table drops many rows.
        with ui.column().classes("w-full gap-2"):
            for table_name, pks, reasons in _group_quarantine_entries(
                quarantine_entries
            ):
                _quarantine_detail_row(
                    ui,
                    table=table_name,
                    primary_keys=pks,
                    reasons=reasons,
                    action=_quar_reload(table_name),
                )

def _render_accept_quarantine_action(
    ui,
    *,
    quarantine_only: bool,
    terminal: bool,
    quarantine_accepted: bool,
    accept_quarantine_and_continue=None,
) -> None:
    """Render the accept-the-gap action (or its accepted state), or nothing.

    Rendered AFTER the completeness banner, not inside the quarantine panel: the banner
    states the verdict and names the remedy, so the button that carries out that remedy
    belongs directly beneath it. Sitting above the verdict, it asked the operator to
    decide before reading the conclusion they were deciding on.

    Only offered when quarantine is the ONLY incompleteness -- a retryable failure must be
    retried or reloaded first, never waved past.
    """
    if not (quarantine_only and terminal and accept_quarantine_and_continue is not None):
        return
    with ui.row().classes("items-center gap-2 w-full"):
        if quarantine_accepted:
            # A one-line confirmation, NOT a second success notice. Accepting flips the
            # completeness banner directly above to "Full Load complete — with an accepted
            # gap", which already states the count, the table, that the next step is
            # unblocked, and that Validation reports the gap. A full notice here repeated
            # all of that in a second green box -- two boxes, nearly the same words. What
            # the banner does NOT say is that the gap remains closable, so that is all
            # this line adds; the checkmark alone acknowledges the click.
            with ui.row().classes("items-center gap-1 no-wrap"):
                ui.icon("check_circle").classes("text-green-600 text-base")
                ui.label(
                    "Gap accepted — reloading a table after fixing its source value "
                    "still closes it."
                ).classes("text-xs text-gray-600")
        else:
            ui.button(
                "Accept quarantined rows & continue",
                on_click=accept_quarantine_and_continue,
                icon="check",
            ).props("unelevated no-caps color=warning")


def _render_completeness_banner(
    ui,
    completeness: "FullLoadCompleteness",
    *,
    approximate: bool = False,
    quarantine_accepted: bool = False,
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

    # The gap was explicitly ACCEPTED and nothing else is wrong: the run is complete by
    # the operator's own decision, so keeping the amber "finished with issues" contradicted
    # the green "Gap accepted" notice directly above it. State the gap plainly, but do not
    # re-flag as a problem something the operator has already resolved -- while never
    # claiming every row loaded, because they did not.
    if (
        quarantine_accepted
        and completeness.failed == 0
        and completeness.quarantined_rows
    ):
        row_noun = "row" if completeness.quarantined_rows == 1 else "rows"
        _render_notice(
            ui,
            tone="success",
            header="Full Load complete — with an accepted gap",
            body=(
                f"{completeness.quarantined_rows} {row_noun} could not be stored and "
                f"were permanently dropped ({', '.join(completeness.quarantined_tables)}"
                "); you accepted that gap, so the next step is unblocked. Every other "
                "row loaded. Validation (Step 4) still reports the gap against the "
                "source."
            ),
        )
        return

    # An estimate-only discrepancy (no real failure, approximate baseline) is an
    # informational note, not a failure: the counts simply differ from the
    # scan-free estimate. Surface it calmly (AWS-style info box) and defer to
    # Validation for the exact truth.
    # Quarantined rows are a CONFIRMED loss, so they can never be the calm
    # "counts differ from the estimate" note however approximate the baseline is --
    # nothing about a scan-free estimate explains a row the loader dropped.
    estimate_only = (
        approximate
        and completeness.failed == 0
        and completeness.quarantined_rows == 0
    )
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
    # Lead with the dropped rows: it is the one certainty in this list, and stating it
    # as its own item stops it hiding inside a generic "row-count mismatch".
    if completeness.quarantined_rows:
        row_noun = "row" if completeness.quarantined_rows == 1 else "rows"
        problems.append(
            f"{completeness.quarantined_rows} {row_noun} permanently dropped "
            f"({', '.join(completeness.quarantined_tables)})"
        )
    # Don't double-report a table already named as quarantined: its shortfall IS the
    # dropped rows, so listing it again as a "mismatch" reads like a second problem.
    _mismatched_only = [
        name
        for name in completeness.mismatched
        if name not in set(completeness.quarantined_tables)
    ]
    if _mismatched_only:
        problems.append(
            f"{len(_mismatched_only)} row-count mismatch "
            f"({', '.join(_mismatched_only)})"
        )
    if completeness.unknown:
        problems.append(
            f"{completeness.unknown} without a source count to compare"
        )
    # Tailor the remedy to the problems actually present. "Retry the failed tables" is
    # dead-end advice when nothing FAILED -- a quarantining table is DONE, so it is not
    # in the retry set; the way to recover those rows is to fix the source value and
    # reload that table.
    if completeness.failed:
        remedy = (
            "Retry the failed tables, or run Validation (Step 4) for a full "
            "row-count/checksum check."
        )
    elif completeness.quarantined_rows:
        remedy = (
            "The dropped rows are listed above with their reason: fix the source "
            "value(s) and Reload that table to load them, or accept the gap to "
            "continue (Validation reports it)."
        )
    else:
        remedy = "Run Validation (Step 4) for a full row-count/checksum check."
    _render_notice(
        ui,
        tone="warning",
        header="Full Load finished with issues",
        body="; ".join(problems) + ". " + remedy,
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
    load_running: bool = False,
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

    ``load_running`` is ``True`` while a Full Load is actively IN_PROGRESS. The
    checks are read-only and never touch the running job, so re-running them
    mid-load is harmless but pointless (the fresh result applies only to the NEXT
    run, not the in-flight one) and adds avoidable source read load. When set, the
    Check button is disabled with an explanatory notice, matching how the
    migration-type selector locks during a run.
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
        # Disable while the check itself runs, OR while a Full Load is in flight
        # (a mid-load re-run is inert w.r.t. the running job and only adds source
        # read load). The tooltip explains the load-running case.
        if running:
            check_btn.props("disable")
        elif load_running:
            check_btn.props("disable").tooltip(
                "A migration operation is in progress — prerequisite checks are "
                "read-only and apply to the NEXT run, not the one in progress."
            )
    # While a load runs, spell out that re-checking here won't affect it (the
    # button is disabled above; this notice says why, so the disabled state isn't
    # a mystery).
    if load_running and not running:
        ui.label(
            "A migration operation is in progress. These checks apply to the "
            "next run — they don't affect the running operation."
        ).classes("text-xs text-gray-400")
    # Immediate feedback directly below the button while a check runs, so the
    # click is acknowledged at once instead of seeming unresponsive.
    if running:
        with ui.row().classes("items-center gap-2"):
            ui.spinner(size="sm")
            ui.label(running_text).classes("text-sm text-gray-600")
    report = migration_state.get_prereq_report(mode)
    if report is not None:
        # A report can outlive the selection it covered (nothing clears it, and the
        # picker stays editable until a migration commits). If tables were ADDED since,
        # the verdict below is stale for them -- they were never checked -- and the run
        # guard blocks on exactly this. Surface it here too so the reason sits next to
        # the results the user is reading, not only on a disabled button elsewhere.
        added = prereq_scope_gap(report, migration_state.selection.selected_tables)
        if added:
            listed = ", ".join(added[:6]) + (
                f" +{len(added) - 6} more" if len(added) > 6 else ""
            )
            render_notice(
                ui,
                tone="warning",
                header="These results don't cover your current selection",
                body=(
                    f"{listed} {'were' if len(added) > 1 else 'was'} added after these "
                    "checks ran, so they were never checked. Re-run the checks to cover "
                    "the full selection before migrating."
                ),
            )
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
    # Full Load records only -- CDC shares this key, and its dead-lettered rows belong
    # to the CDC panel's own download (see full_load_error_records).
    records = full_load_error_records(migration_state.error_log, job_id)
    summary = full_load_error_summary(migration_state.error_log, job_id)
    # NO "Data errors" heading + count when there is nothing to download: with zero
    # errors it printed a section header over "No data errors recorded." -- a whole block
    # asserting an absence. And when there ARE errors, every one of them is already shown
    # above with its table, primary key and reason, so a heading restating the count was
    # the same fact a fourth time. What this section uniquely offers is the DOWNLOAD, so
    # it is now just that button (its label already names what it contains).
    if summary.total_errors > 0:
        def _download_log() -> None:
            try:
                # Serialize the FILTERED records; render_log(job_id) would re-read the
                # whole key and put CDC rows into a file labelled Full Load.
                payload = migration_state.error_log.render_records(records)
                ui.download.content(  # type: ignore[attr-defined]
                    payload, f"error_log_{job_id}.ndjson", "application/x-ndjson"
                )
            except Exception as exc:  # noqa: BLE001 - surface instead of silent
                _LOGGER.exception("Failed to render/download error log")
                ui.notify(  # type: ignore[attr-defined]
                    f"Could not generate the error log: {exc}", type="negative"
                )

        with ui.row().classes("items-center gap-2 w-full"):
            # Name it in the user's terms: WHAT it is ("Full Load error log") and HOW MUCH
            # ("3 errors"). "Download error log (NDJSON)" led with a file format nobody
            # asked about and never said which step's errors it held.
            noun = "error" if summary.total_errors == 1 else "errors"
            ui.button(
                f"Download Full Load error log ({summary.total_errors} {noun})",
                on_click=_download_log,
                icon="download",
            ).props("outline no-caps size=sm").tooltip(
                format_error_summary(summary)
                + " One line per error with the table, primary key, reason and "
                "timestamp — never row values. Saved as NDJSON (one JSON object per "
                "line), readable in any text editor."
            )










































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
    "quarantined_rows_by_table",
    "unsettled_table_names",
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
    "full_load_error_migration_context",
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
    "cdc_cascade_gap_tables",
    "cdc_handling_facts",
    "cdc_prerequisite_block_reason",
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
    _cdc_is_streaming,
    _cdc_tables_for_config,
    _cdc_target_region,
    _diagnose_for_dialog,
    _dlq_panel_tone,
    _open_cdc_delete_dialog,
    _open_cdc_infra_dialog,
    _open_cdc_start_dialog,
    _open_cdc_stop_dialog,
    _render_cdc_cost_estimate,
    _render_cdc_delete_action,
    _render_cdc_deploy_live,
    _render_cdc_dlq_breakdown,
    _render_cdc_dlq_panel,
    _render_cdc_dlq_records,
    _render_cdc_error_download,
    _render_cdc_handling_panel,
    _render_cdc_infra_deploy_action,
    _render_cdc_infra_form,
    _render_cdc_infra_prep_section,
    cdc_infra_prep_state,
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
    cdc_pipeline_live,
    cdc_infra_deploy_in_flight,
    cdc_streaming_started,
    cdc_unstable_message,
    classify_cdc_card_phase,
)

