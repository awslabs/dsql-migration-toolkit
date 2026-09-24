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

Package layout (the CDC-vs-Full-Load split is intentional, not an asymmetry to
"fix"). This package is a HYBRID: a per-FEATURE vertical slice for CDC -- which is a
genuinely larger subsystem (infra deploy, MSK, connectors, offset seeding, DLQ,
schema drift) -- and a per-LAYER split for Full Load and the shared parts:

- Full Load: ``_full_load_engine`` (backend run engine, NiceGUI-free) + ``_full_load_ui``
  (render). Core: :mod:`~dsql_migrator.core.exporter` + :mod:`~dsql_migrator.core.batched_import`.
- CDC: ``_cdc_ui`` (control render), ``_cdc_monitoring`` (post-start monitoring / DLQ
  render), ``_cdc_state`` (pure phase predicates), ``_cdc_status`` (status / controller /
  teardown + a couple of shared job/error-log helpers that co-locate by cohesion). Core:
  :mod:`~dsql_migrator.core.cdc` and siblings + :mod:`~dsql_migrator.core.msk_connect_controller`.
- Shared, used by BOTH data paths (the Full Load -> CDC watermark handoff, prerequisites,
  and table selection couple them, so they are ONE layer, not split by feature):
  ``_models`` (pure view-models / formatters / enums), ``_state`` (per-session
  :class:`DataMigrationState`), and this ``__init__`` (the
  :func:`build_data_migration_screen` orchestrator that wires both steps into one journey).

File count tracks intrinsic complexity, not symmetry: CDC's ~11 core modules vs Full
Load's 2 reflect that CDC is a distributed-systems subsystem while Full Load is a tight
stream -> batch-insert -> verify pipeline. Do NOT split Full Load further just to "match" CDC.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Mapping, Optional, Sequence

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
    target_primary_keys,
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
    SourceType,
    StepStatus,
    TableSelection,
    TargetInventory,
    Watermark,
)
from dsql_migrator.core.table_selection import TableSelector
from dsql_migrator.core.watermark import WatermarkCapturer
from dsql_migrator.ui.ai_assist import ai_is_usable
from dsql_migrator.core.cdc import cdc_teardown_estimate, cdc_teardown_reason
from dsql_migrator.core.cdc_postgres import pg_snapshot_mode
from dsql_migrator.ui.design import (
    NOTICE_STYLE,
    WRAP_CELLS_PROP,
    inline_hint,
    notice_container,
    render_notice,
)
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
from dsql_migrator.ui.data_migration._full_load_engine import (
    MigratorFactory,
    preserved_foreign_key_names,
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
    # The explicit "Apply foreign keys" action runs the SAME engine pass the load used to
    # run automatically (aliased, since `_apply_foreign_keys` is the engine's private name).
    _apply_foreign_keys as _apply_foreign_keys_for,
    pending_foreign_key_count,
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
    _CDC_MIGRATION_TYPES,
    _SUBSTEPS,
    source_supports_cdc,
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
    migration_type_blurb,
    migration_type_requirements,
    migration_type_tradeoff,
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
    lob_exclusion_scope_gap,
    schema_recreate_tables,
    prerequisite_block_reason,
    resnapshot_resolvable_failures,
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
    make_lob_candidates_for,
    session_source_type,
    scope_lob_candidates,
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
    cdc_prerequisite_block_header,
    cdc_prerequisite_block_reason,
)

# CDC status / controller / deploy-formatting logic (NiceGUI-free); re-exported so
# the package's public import surface is unchanged and the render code below
# resolves the pure helpers/constants it consumes.
from dsql_migrator.ui.data_migration._cdc_status import (
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
    is_stack_teardown_status,
    split_cleanup_by_progress,
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
    open_ai_scope: Optional[Callable[..., object]] = None,
    ai_post_event: Optional[Callable[..., object]] = None,
    ai_tools: "Optional[Sequence[Mapping[str, object]]]" = None,
    ai_tool_execute: "Optional[Callable[[str, Mapping[str, object]], str]]" = None,
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
        _conversion = SchemaConverter(
            source_type=source_config.source_type
        ).convert(inventory)
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
            # PostgreSQL Full Load + CDC: name the CDC stack so the watermark capture
            # creates the logical replication slot + publication on the source at the
            # consistency point (gapless handoff; see _capture_postgres_watermark).
            cdc_stack_name=_pg_cdc_handoff_stack(
                migration_state, source_config, job_manager
            ),
            # Source-agnostic "this load hands off to CDC" marker: defers the post-load
            # FK pass to cut over for EVERY Full Load + CDC run, covering the MySQL
            # Full-Load-first -> binlog-watermark handoff that cdc_coexisting /
            # cdc_stack_name both miss (else FKs applied at load end -> 23503 dead-letters).
            is_cdc_migration=migration_state.migration_type
            is MigrationType.FULL_LOAD_AND_CDC,
            table_conversions=applied_table_conversions(
                _conversion,
                conv_state.edited_target_ddls,
                preserve_foreign_keys=conv_state.preserve_foreign_keys,
            ),
            # Converted view DDLs so a "drop & reload" run can pre-drop / recreate
            # views that depend on a replaced table (else the DROP is blocked).
            dependent_view_ddls=applied_view_ddls(
                _conversion, conv_state.edited_target_ddls
            ),
            # The migration-wide oversized-LOB exclusion: drops these columns from
            # the Full Load INSERT list too (the same selection that feeds CDC's
            # column.exclude.list, so the two paths stay in lockstep).
            excluded_lob_columns={
                table: frozenset(columns)
                for table, columns in migration_state.lob_exclusions().items()
            },
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
        # A new load supersedes any previous foreign-key application: nothing cleared
        # `fk_apply_result`, so the card kept reporting "N applied -- referential integrity
        # is in place" for constraints this run may be about to destroy (a replace DROPs the
        # table, and DSQL drops its foreign keys with it). Clearing makes the step fall back
        # to "N foreign key(s) not yet applied" with the action offered -- true and
        # actionable. NOT done inside clear_outputs(): its other caller is the read-only
        # prerequisite check, which must not touch foreign-key state.
        migration_state.set_fk_apply_result(None)
        migration_state.set_fk_apply_progress(0, 0)
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
                accepted_quarantine_rows=migration_state.accepted_quarantine_rows,
                inputs=inputs,
            )

        migration_state.job_id = job_manager.submit(work)
        # Mirror the Full Load start into the AI activity feed (deterministic; the
        # completion event is posted from the live poll as the job reaches a terminal
        # state). ``tables`` is the resolved selection, so the count is exact here.
        if ai_post_event is not None:
            _n = len(tables)
            ai_post_event(
                text=f"Started Full Load for {_n} table{'' if _n == 1 else 's'}",
                status="started",
            )

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
            # session_source_type, NOT a source_config read with a MySQL fallback: a
            # RESTORED session (reconnect / task replacement) records the engine WITHOUT a
            # source_config, so the fallback built a MySQL converter for a PostgreSQL
            # inventory. Measured: that changes column TYPES (a PG `timestamp` becomes
            # TIMESTAMPTZ instead of TIMESTAMP -- a timezone-semantics change -- and
            # `bit(1)` becomes SMALLINT) and drops every PostgreSQL conversion warning.
            # Neither of the two values THIS site consumes diverges today (verified over
            # nine PK shapes incl. bytea/no-PK/composite, and with foreign keys), so the
            # fix is behaviour-preserving -- but only by luck: the moment an engine
            # difference reaches the re-key choice or FK ownership, the wrong CDC record
            # key would make the sink upsert on the wrong columns, silently.
            _conversion = SchemaConverter(
                source_type=session_source_type(session)
            ).convert(inventory)
            _applied = applied_table_conversions(
                _conversion,
                conv_state.edited_target_ddls,
                preserve_foreign_keys=conv_state.preserve_foreign_keys,
            )
            migration_state.set_cdc_message_key_columns(
                composite_key_columns_for_cdc(inventory.tables, _applied)
            )
            # Which FK constraints THIS migration OWNS, for the CDC-start precondition:
            # an enforced FK on the target dead-letters out-of-order child rows (23503),
            # so it must be removed before streaming -- but only ours, which cut over
            # then re-creates. Recomputed here (the only place conv_state is in scope).
            #
            # Ownership is deliberately computed with preserve_foreign_keys=TRUE, NOT the
            # user's current toggle. Ownership is a fact about what this migration's
            # conversion RENDERS; whether the user still wants them re-applied is a
            # separate choice. Deriving it from the toggle created a dead end: an operator
            # blocked here would go to Schema Conversion and untick "Preserve foreign
            # keys" -- which blanks foreign_key_ddls, so the tool disowned the very FKs it
            # had created, classified them as the user's, hid the Remove button, and left
            # CDC blocked with no in-UI way out.
            migration_state.set_cdc_preserved_foreign_keys(
                preserved_foreign_key_names(
                    applied_table_conversions(
                        _conversion,
                        conv_state.edited_target_ddls,
                        preserve_foreign_keys=True,
                    )
                )
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
            # THE gate for CDC_REPLICATION_OBJECTS, derived from the PROVISIONER'S OWN
            # predicate rather than re-derived from the migration type. _pg_cdc_handoff_stack
            # is the sole decider of whether this run creates the publication + slot -- its
            # return value IS the cdc_stack_name that _capture_postgres_watermark branches
            # on -- so reusing it makes it structurally impossible for the gate and the
            # provisioner to disagree. Writing `migration_type is CDC_ONLY` here would miss
            # its other None cases (MySQL; CDC already streaming, where the objects DO exist
            # and the check correctly passes).
            _handoff_stack = _pg_cdc_handoff_stack(
                migration_state, source_config, job_manager
            )
            request = PrerequisiteCheckRequest(
                mode=mode,
                tables=[table.name for table in tables],
                # Engine selects the CDC-only checks: a PostgreSQL source reports
                # CDC as not-yet-supported (INFO) instead of running MySQL binlog
                # checks that would falsely FAIL.
                source_type=source_config.source_type,
                provisions_replication=_handoff_stack is not None,
                # Will this start re-snapshot? Read off the EFFECTIVE snapshot mode --
                # the same pure rule the connector config and the Deploy/Start gates use --
                # not off the start mode alone. "manual" is only ONE of the two ways to end
                # up at snapshot.mode=initial: a CDC-only start with no provisioned slot on
                # the watermark lands there too, and keying on "manual" graded that run as
                # if it would resume from a slot that does not exist. The report then FAILed
                # on an absent publication the connector is in fact about to create, while
                # the gates (now on the same rule) said proceed -- a screen disagreeing with
                # the button next to it. When this is True the report re-grades the two
                # absences a re-snapshot repairs, so nothing leaves a red FAIL beside an
                # enabled primary button.
                cdc_start_resnapshots=(
                    source_config.source_type is SourceType.POSTGRES
                    and pg_snapshot_mode(
                        _cdc_watermark(
                            _current_job(
                                job_manager, getattr(migration_state, "job_id", None)
                            )
                        ),
                        force_initial_snapshot=(
                            migration_state.cdc_start_mode() == "manual"
                        ),
                    )
                    == "initial"
                ),
                cdc_stack_name=(
                    _handoff_stack
                    or getattr(migration_state, "cdc_stack_name", "")
                    or ""
                ),
                # The re-key map the connector will actually be configured with (already
                # recomputed onto state on every render from the APPLIED target DDL). A
                # re-keyed table whose source REPLICA IDENTITY cannot supply the added key
                # column loses every DELETE silently, and only the source catalog can say
                # whether it can -- so the check needs this map, not a re-derivation.
                message_key_columns={
                    table: list(cols)
                    for table, cols in (
                        migration_state.cdc_message_key_columns() or {}
                    ).items()
                    if table in set(names)
                },
            )
            from nicegui import run

            # Acknowledge the click immediately: clear any prior error, flag the
            # mode as running, and re-render so a spinner/"checking..." appears
            # right below the button before the (slow) read-only checks start.
            migration_state.clear_outputs()
            migration_state.set_prereq_running(mode)
            refresh()
            try:
                # Feed the migration-wide LOB exclusion into the gate so a column
                # excluded from the load is judged against the target as it will be
                # written: an excluded NOT NULL/no-default target column FAILs
                # loadability here instead of failing every batch mid-load.
                _prereq_exclusions = migration_state.lob_exclusions()
                report = await run.io_bound(
                    lambda: checker.check(
                        request,
                        tables=tables,
                        excluded_columns=_prereq_exclusions,
                    )
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
                # Gate the CDC tiles by source engine. MySQL and PostgreSQL both
                # support CDC now (PG via pgoutput logical replication -> MSK -> DSQL
                # sink), so all three tiles are enabled for them; the gate stays as a
                # defensive default that would disable the CDC tiles for any future
                # engine whose CDC path is not shipped.
                # session_source_type, NOT a source_config read with a MySQL fallback: a
                # RESTORED session (reconnect / task replacement) has the engine recorded
                # WITHOUT a source_config, so the fallback showed a PostgreSQL operator the
                # MySQL tile semantics -- which would have defeated the engine-aware copy
                # entirely. That helper exists for exactly this class of bug and is what
                # every other PostgreSQL-aware surface uses.
                source_type=session_source_type(session),
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
                            "reach the binary log — CDC replicates the parent change "
                            "but not the cascaded child change, leaving orphaned rows "
                            "on the target during replication. The tool re-creates "
                            "these foreign keys on Aurora DSQL only at cut over (never "
                            "during replication), so any such orphan will then BLOCK "
                            "the ADD CONSTRAINT and be reported by the Validation "
                            "orphan-record check. Replace the automatic actions with "
                            "explicit child-row statements in your application before "
                            "starting CDC, and quiesce source writes before the final "
                            "cut-over comparison. See Evaluation for the full list."
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
                from dataclasses import replace as _dc_replace

                from nicegui import run as _run

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
                # ``replace`` copies every field this rebuild does not change -- including
                # ``source_type``, which it used to drop, titling a PostgreSQL migration's
                # exported Evaluation report "MySQL to Aurora DSQL...".
                eval_state.set_result(
                    _dc_replace(
                        current,
                        inventory=new_inventory,
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

            # Migration-wide oversized-LOB exclusion, offered for any type that
            # includes a Full Load -- right after the picker, BEFORE the checks, so
            # the operator can drop a column before the load that would carry it and
            # the prerequisite gate then judges the exact post-exclusion column set.
            # (CDC-only keeps its copy inside the CDC sub-flow, next to the
            # column.exclude.list preview it feeds.)
            #
            # Editable up until the migration is COMMITTED, using the SAME lock as
            # the table picker (``selection_locked``): a prerequisite report existing
            # is a preview, not a commitment, so it must NOT freeze the choice (the
            # picker was explicitly changed away from that "too early and a dead end"
            # behavior). The exclusion is baked only once a Full Load has run, CDC is
            # streaming, or the CDC stack (with its column.exclude.list) is deployed
            # -- exactly ``selection_locked``. A change made after the checks ran but
            # before the load is caught at the Run button (see
            # ``full_load_run_guard_reason``), so a stale PASS never starts a load
            # against an unchecked column set. The lock is silent: the greyed picker
            # beside it already explains why inputs are frozen.
            # The tables a Full Load will migrate (same logic the run uses) -- used
            # both to re-surface the selection for confirmation below AND to scope the
            # oversized-LOB panel to the SELECTED tables. Computed BEFORE the panel so
            # the panel filters to the effective selection: picking one schema in
            # Schema Conversion only pre-ticks the picker (``migration_state.selection``
            # stays the empty "= all" default until touched), so passing the raw
            # selection used to list LOB columns from every schema.
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
            if migration_type is not MigrationType.CDC_ONLY:
                # The LOB tick boxes are frozen by the same lock as the table picker, but
                # they need the LOB-SPECIFIC reason: the picker's copy only talks about
                # "a different set of tables", while a user who forgot to exclude a column
                # is trying to change the COLUMNS -- so that sentence does not read as
                # applying to them, and they cannot find the way out (which is the same
                # 'Start over'). Passing lock_reason=None was worse still: the panel only
                # renders text `if locked and lock_reason`, so the boxes were frozen with
                # NO explanation at all -- the "silent frozen box reads as a bug" state
                # that panel's own comment says must never happen. lob_exclusion_lock
                # already writes the right sentence for each cause; reuse it.
                _lob_locked, _lob_reason = lob_exclusion_lock(
                    migration_state,
                    job_manager,
                    full_load_committed=(job is not None or status is StepStatus.DONE),
                )
                _render_cdc_lob_exclusion_panel(
                    ui,
                    migration_state,
                    inventory,
                    refresh,
                    locked=selection_locked or _lob_locked,
                    # Fall back to the SELECTION lock's reason when the LOB clause has
                    # none: the `locked` flag is an OR, so any contributor without a
                    # reason renders a frozen card with nothing explaining it (the panel
                    # only draws the text `if locked and lock_reason`). Adding "unstable"
                    # to lob_exclusion_lock closed today's instance; this closes the
                    # CLASS, so the next contributor cannot reopen it.
                    lock_reason=_lob_reason or selection_lock,
                    migration_wide=True,
                    selected_tables=selected_names,
                    source_type=session_source_type(session),
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
                    # Record the REQUEST, not just its outcome: the stop is cooperative, so
                    # minutes can pass between the click and the run ending, and without this
                    # the log could not tell an operator-requested stop from a crash that
                    # happened to end the run at the same moment.
                    log_activity(
                        ActivityCategory.FULL_LOAD,
                        "stop requested",
                        status=ActivityStatus.INFO,
                        detail=(
                            "operator requested a stop -- in-flight batches finish first, "
                            "and every table already loaded is kept"
                        ),
                    )
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
                if retry_replace:
                    # This retry will DROP+recreate these tables, and DSQL drops a table's
                    # foreign keys with it -- so a previously-applied result no longer
                    # describes the target. The reachable case is the tool's OWN recovery
                    # advice: "Exclude column & reload" forces a replace for that table, and
                    # the card went on claiming referential integrity was in place. An
                    # append-only retry does NOT invalidate it (the constraints survive and
                    # would reject a violating row), so this is scoped to replacements.
                    migration_state.set_fk_apply_result(None)
                    migration_state.set_fk_apply_progress(0, 0)
                _retry_conversion = SchemaConverter(
                    source_type=source_config.source_type
                ).convert(inventory)
                retry_inputs = DataMigrationInputs(
                    source_config=source_config,
                    source_password=session.source_password,
                    target_config=target_config,
                    inventory=inventory,
                    aws_profile=session.aws_profile,
                    staging_bucket=staging_bucket,
                    replace_tables=retry_replace,
                    cdc_coexisting=retry_cdc_coexisting,
                    is_cdc_migration=migration_state.migration_type
                    is MigrationType.FULL_LOAD_AND_CDC,
                    table_conversions=applied_table_conversions(
                        _retry_conversion,
                        conv_state.edited_target_ddls,
                        preserve_foreign_keys=conv_state.preserve_foreign_keys,
                    ),
                    dependent_view_ddls=applied_view_ddls(
                        _retry_conversion,
                        conv_state.edited_target_ddls,
                    ),
                    excluded_lob_columns={
                        table: frozenset(columns)
                        for table, columns in migration_state.lob_exclusions().items()
                    },
                )
                retry_tables = TableSelector().resolve(
                    inventory, TableSelection(selected_tables=names)
                )
                retry_migrator = migrator_factory(retry_inputs)
                error_log = migration_state.error_log
                prior_chunks = current.chunks
                # The retry gets a NEW job id, so hand it the prior run's error-log
                # owners: without them every table it does NOT re-run loses its reason
                # text (and its recovery action) while keeping its dropped-row count.
                prior_job_id = current.job_id
                prior_error_owners = dict(
                    getattr(current, "table_error_job_ids", None) or {}
                )
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
                        accepted_quarantine_rows=getattr(migration_state, 'accepted_quarantine_rows', None),
                        prior_job_id=prior_job_id,
                        prior_table_error_job_ids=prior_error_owners,
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

            # Built by a module-level factory so the real body is TESTABLE: as a
            # closure it was unreachable from any test, which is how an engine-unaware
            # call inside it shipped.
            lob_candidates_for = make_lob_candidates_for(inventory, session)

            def exclude_lob_and_reload(table_name: str, columns: Sequence[str]) -> None:
                """Exclude oversized-LOB column(s) AND reload the table, as ONE action.

                The recovery path for a load that quarantined rows over DSQL's ~1 MiB
                per-value limit: you cannot know WHICH column is too big until you load,
                but the exclusion tick boxes lock once a Full Load has run, and the only
                escape was Start over -- which discards Evaluation, Schema Conversion
                (including edited DDL), the migration type and the CDC inputs to change
                one checkbox. That lock exists because changing the exclusion would leave
                already-loaded rows inconsistent with what CDC captures; bundling the
                exclusion WITH a forced DROP+reload of that table removes those rows, so
                the inconsistency cannot arise and the lock does not need to apply.

                Refused while a CDC pipeline already owns the excluded-column set (see
                :func:`exclude_and_reload_block_reason`).
                """
                blocked = exclude_and_reload_block_reason(migration_state, job_manager)
                if blocked:
                    ui.notify(blocked, type="warning", position="top")
                    return
                if not columns:
                    return
                for column in columns:
                    migration_state.set_lob_exclusion(table_name, column, True)
                # FORCE the replace for exactly this table: an append would leave the
                # rows loaded under the old column set beside rows loaded without it.
                migration_state.set_replace_targets(frozenset({table_name}))
                log_activity(
                    ActivityCategory.FULL_LOAD,
                    "oversized-LOB column excluded",
                    status=ActivityStatus.INFO,
                    target=table_name,
                    detail=(
                        f"excluded {', '.join(sorted(columns))} and reloading this table "
                        "(DROP + recreate), so no row keeps a value loaded under the "
                        "previous column set"
                    ),
                )
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
            if ai_is_usable(session) and open_ai_scope is not None:

                def ai_error_opener(
                    table_name: str,
                    error_message: str,
                    *,
                    topic: str = "failure",
                    seed: "Optional[str]" = None,
                ) -> None:
                    # ``topic`` distinguishes a real FAILURE from a QUARANTINE (dropped
                    # rows) so each gets its own panel scope + a fitting seed question;
                    # both reuse the same Full Load error grounding (which explains
                    # quarantine + the standing-gap trap).
                    from dsql_migrator.core.assessment_strategist import (
                        AssessmentStrategist,
                        source_engine_word,
                    )

                    strategist = AssessmentStrategist(
                        session.ai_assist, aws_profile=session.aws_profile,
                        source_engine=source_engine_word(
                            getattr(session.source_config, "source_type", None)
                        ),
                    )
                    # Ground the reply in THIS migration's situation (type, CDC
                    # status, DROP+recreate) so it isn't generic.
                    migration_context = full_load_error_migration_context(
                        migration_state,
                        table_name=table_name,
                        cdc_live=cdc_streaming_started(migration_state, job_manager),
                    )
                    # Deep-link the per-table diagnosis into the persistent AI panel.
                    open_ai_scope(
                        scope_id=f"full_load:{topic}:{table_name}",
                        title="AI DBA",
                        subtitle=f"Full Load · {table_name}",
                        chip=f"Full Load · {table_name}",
                        seed_question=(
                            seed
                            or f"Why did loading {table_name} fail, and how do I fix it?"
                        ),
                        streamer=lambda messages, on_delta: (
                            strategist.stream_full_load_error_chat(
                                table_name,
                                error_message,
                                messages,
                                on_delta,
                                migration_context=migration_context,
                                # Same read-only tools as the other chats, so it can
                                # look up the converted DDL / target schema / source
                                # structure to root-cause a schema/DDL failure by name.
                                tools=ai_tools,
                                execute=ai_tool_execute,
                            )
                        ),
                    )

            # Opener for the CDC DLQ / schema-drift diagnosis chat (revives the CDC
            # assist path). None when AI is off -> the CDC panels show no AI affordance.
            cdc_ai_opener = None
            if ai_is_usable(session) and open_ai_scope is not None:

                def cdc_ai_opener(scope: str, facts: str, seed: str) -> None:
                    from dsql_migrator.core.assessment_strategist import (
                        AssessmentStrategist,
                        source_engine_word,
                    )

                    strategist = AssessmentStrategist(
                        session.ai_assist, aws_profile=session.aws_profile,
                        source_engine=source_engine_word(
                            getattr(session.source_config, "source_type", None)
                        ),
                    )
                    open_ai_scope(
                        scope_id=f"cdc:{scope}",
                        title="AI DBA",
                        subtitle=(
                            "CDC · dead-letter queue"
                            if scope == "dlq"
                            else "CDC · schema drift"
                        ),
                        chip="CDC · monitoring",
                        seed_question=seed,
                        streamer=lambda messages, on_delta: (
                            strategist.stream_cdc_chat(
                                facts, messages, on_delta, scope=scope,
                                # Shared read-only tools: look up the affected table's
                                # converted DDL / target schema to root-cause the drift.
                                tools=ai_tools, execute=ai_tool_execute,
                            )
                        ),
                    )

            def _fk_apply_inputs():
                """Rebuild the load inputs so the FK action applies the SAME conversions."""
                _src = session.source_config
                _tgt = session.target_config
                _inv = _inventory()
                _conv = SchemaConverter(source_type=_src.source_type).convert(_inv)
                return DataMigrationInputs(
                    source_config=_src,
                    source_password=session.source_password,
                    target_config=_tgt,
                    inventory=_inv,
                    aws_profile=session.aws_profile,
                    replace_tables=frozenset(),      # no data movement: DDL only
                    is_cdc_migration=migration_state.migration_type
                    is MigrationType.FULL_LOAD_AND_CDC,
                    table_conversions=applied_table_conversions(
                        _conv,
                        conv_state.edited_target_ddls,
                        preserve_foreign_keys=conv_state.preserve_foreign_keys,
                    ),
                    excluded_lob_columns={
                        table: frozenset(columns)
                        for table, columns in migration_state.lob_exclusions().items()
                    },
                )

            def fk_pending_count() -> int:
                """This run's TOTAL preserved-foreign-key count (static; no DB access).

                It used to return 0 as soon as ``fk_apply_result`` was set, which hid a
                STOPPED pass completely: the card reads this to work out how many foreign
                keys a finished pass never reached (total - applied - skipped - failed), so
                zeroing it made a 2-of-6 stop render as a green "2 applied / referential
                integrity is in place" with the Apply button withdrawn -- and nothing ever
                clears the result, so that screen was terminal. The renderer decides what to
                SHOW; this only reports the denominator.
                """
                try:
                    return pending_foreign_key_count(migrator_factory(_fk_apply_inputs()))
                except Exception:  # noqa: BLE001 - advisory count; never break the panel
                    return 0

            def cancel_foreign_keys() -> None:
                """Stop the FK pass. Already-applied constraints stay; the rest remain
                pending and the action can be run again (a duplicate ADD is a no-op, 42710).

                A sibling of the action, not nested inside it: the renderer needs it, and a
                nested definition is invisible there (the undefined-global guard caught that).
                """
                jid = migration_state.fk_apply_job_id
                if jid:
                    job_manager.request_cancel(jid)
                refresh()

            def apply_foreign_keys_now() -> None:
                """Apply the preserved foreign keys -- an explicit operator action.

                Foreign keys are no longer applied as part of the load: the orphan
                pre-check is O(child rows) and measured 28.0 min (serial) / 7.9 min
                (8-way) for 15 FKs over 15.5M child rows, all of it AFTER every row was
                already written. Running it behind a button reports the load as finished
                when it is finished, and makes the Full-Load-only path use the same flow
                a CDC migration already uses at cut over.

                The orphan pre-check itself is unchanged -- it cannot be skipped, because a
                failed `ALTER TABLE ASYNC ... VALIDATE CONSTRAINT` is unobservable on DSQL.
                """
                fk_inputs = _fk_apply_inputs()
                fk_migrator = migrator_factory(fk_inputs)

                total = pending_foreign_key_count(fk_migrator)
                # CLEAR THE PREVIOUS RESULT, and do it BEFORE submitting. _fk_apply_running()
                # returns False while a result exists, so a RESUMED pass (now reachable: the
                # card keeps the action when a stopped pass left foreign keys unreached) would
                # report as not-running for its whole duration -- stale summary, no spinner, no
                # per-FK progress, no "Stop applying" (that branch owns the cancel), no poll
                # re-arm, and an ENABLED Apply button the operator could click again to submit a
                # second concurrent pass. Ordering is load-bearing: clearing AFTER submit can
                # wipe a fast worker's fresh result and strand the card on "not yet applied".
                # Placed after the inputs/count so a raise in migrator_factory leaves the
                # previous result intact.
                migration_state.set_fk_apply_result(None)
                migration_state.set_fk_apply_progress(0, total)

                def work(handle: JobHandle) -> None:
                    # PER-FK progress. Before this the card only ever showed two values --
                    # (0, total) written before submit and (total, total) written in a
                    # finally -- so an operator watching a multi-minute pass saw
                    # "0 of N done" the whole way and concluded it had hung, which is the
                    # very thing the explicit action was introduced to stop. The engine now
                    # reports each FK as it settles, and re-publishes the total, so a
                    # denominator that disagreed with the pass is corrected too.
                    #
                    # No finally that forces (total, total): the card reads progress ONLY
                    # while the job is running, so a terminal write is never displayed --
                    # and forcing it would misreport a pass cut short by Stop as complete.
                    result = _apply_foreign_keys_for(
                        fk_migrator, handle,
                        on_progress=migration_state.set_fk_apply_progress,
                    )
                    migration_state.set_fk_apply_result(result)

                # Its OWN slot. Writing `migration_state.job_id` here (copied from the
                # retry path, where the new job IS the load) made the Full Load panel read
                # this job instead: per-table rows, the completion badge and the EXPORT
                # WATERMARK all disappeared, and CDC then had "no usable Full Load
                # watermark" -- the load's own result destroyed by the action the tool told
                # the operator to run.
                migration_state.set_fk_apply_job(job_manager.submit(work))
                ui.notify(
                    f"Applying {total} foreign key(s) — progress below. Each one is "
                    "orphan-checked first, which reads the whole child table.",
                    type="positive", position="top",
                )
                refresh()

            def _fk_apply_running() -> bool:
                """Is the FK action's OWN job still running?

                Reads ``fk_apply_job_id``, never ``job_id`` -- that slot belongs to the Full
                Load, and writing this job into it is what erased the load's per-table rows
                and export watermark.
                """
                jid = migration_state.fk_apply_job_id
                if not jid or migration_state.fk_apply_result is not None:
                    return False
                job = _current_job(job_manager, jid)
                return job is not None and job.status in ("PENDING", "RUNNING")

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
                quarantined = _quarantined_row_count(
                    current, migration_state.error_log
                )
                # Record WHAT was accepted, not just that something was. The flag used to be
                # sticky and unscoped, so consenting to a 3-row gap auto-accepted every later
                # run's gap of any size -- a load that dropped thousands then completed as a
                # success nobody had agreed to.
                migration_state.set_accept_quarantined_rows(True, gap=quarantined)
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
                        # Prefill the CDC VpcId from the source DB's own DBSubnetGroup.
                        # Runs HERE, off the event loop, because render must not block on a
                        # control-plane round-trip (one asyncio loop serves every browser
                        # session on Fargate). Best effort and only fills an empty field, so
                        # a non-RDS / cross-account / unpermitted source simply leaves it to
                        # the operator -- the flow before this existed.
                        _vpc_derived = False
                        try:
                            _vpc_derived = await _disc_run.io_bound(
                                derive_cdc_vpc_from_source, migration_state, session
                            )
                        except Exception:  # noqa: BLE001 - best-effort prefill
                            _vpc_derived = False
                        # Log any connector RUNNING/FAILED transition (on change).
                        _log_cdc_connector_transitions(migration_state, job_manager)
                        # A real change (first probe reporting, a new stack found, a
                        # connector appearing/going away) still refreshes, so the
                        # duplicate-MSK adopt guard shows up as soon as it is known.
                        if _vpc_derived or (
                            cdc_discovery_fingerprint(migration_state) != _before
                        ):
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
                    # A TEARDOWN is excluded for the same reason the infra create is:
                    # a delete runs ~15-25 min during which nothing streams, no
                    # connector exists and no load runs, so re-checking prerequisites is
                    # precisely what that time is for. Blocking it also blocked Full Load
                    # (no prereq report -> full_load_run_guard_reason refuses the start),
                    # so a teardown silently gated the main flow for up to 45 minutes
                    # behind the message "A migration operation is in progress".
                    #
                    # Both halves need the exclusion: the STATUS is DELETE_IN_PROGRESS,
                    # and an in-session delete records kind="delete", which
                    # cdc_streaming_started counts as streaming (it excludes only
                    # "infra"). Stop CDC (UPDATE_IN_PROGRESS) keeps blocking -- it leaves
                    # MSK, the topics and the immutable partition plan in place, so it
                    # really is a live pipeline operation.
                    _cdc_tearing_down = is_stack_teardown_status(
                        _stack_status
                    ) or cdc_delete_in_flight(migration_state, job_manager)
                    _cdc_deploying = (
                        (
                            _is_inflight_stack_status(_stack_status)
                            and not is_infra_create_stack_status(_stack_status)
                        )
                        or cdc_streaming_started(migration_state, job_manager)
                    ) and not _cdc_tearing_down
                    _route_choice_pending = _render_prerequisites_panel(
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
                        refresh=refresh,
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
                        elif _route_choice_pending:
                            # The panel just rendered the two routes WITH their buttons.
                            # Repeating the block reason here would state one failure's
                            # severity a fourth time (Blocked badge, the category's "1
                            # failed" badge, the situation notice, this line) and put
                            # "you must resolve it" BELOW the thing that resolves it. Say
                            # only what this empty spot means: the next action is one of
                            # the two above.
                            inline_hint(
                                ui,
                                "Pick one of the two routes above to continue.",
                                tone="info",
                                classes="text-sm",
                            )
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
                            # Recovery for rows quarantined over the ~1 MiB per-value
                            # limit: exclude the column that cannot fit and reload the
                            # table as ONE action, so the exclusion never applies to
                            # rows that are already loaded.
                            lob_candidates_for=lob_candidates_for,
                            exclude_lob_and_reload=exclude_lob_and_reload,
                            exclude_lob_block_reason=exclude_and_reload_block_reason(
                                migration_state, job_manager
                            ),
                            accept_quarantine_and_continue=accept_quarantine_and_continue,
                            # Foreign keys are an explicit action now, not part of the
                            # load: the orphan pre-check is O(child rows) and was holding
                            # the END of the load for minutes. Same flow a CDC migration
                            # already uses at cut over.
                            apply_foreign_keys=apply_foreign_keys_now,
                            # PROVIDERS, not values. The FK card is rendered inside the
                            # step's @ui.refreshable live region, so a value evaluated here
                            # is captured when the PAGE renders and then frozen: refreshing
                            # the region re-ran the render with the same stale "not running,
                            # no result" it had before the button was ever clicked. That is
                            # why the card stayed on its spinner even after all the
                            # constraints existed on the target, and only a manual page
                            # reload showed "6 applied". A callable is re-read on every
                            # refresh, so the region reflects the job as it advances.
                            foreign_keys_pending=fk_pending_count,
                            foreign_keys_applied=lambda: migration_state.fk_apply_result,
                            foreign_keys_running=_fk_apply_running,
                            foreign_keys_progress=lambda: migration_state.fk_apply_progress,
                            cancel_foreign_keys=cancel_foreign_keys,
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
                                        SchemaConverter(
                                            # Same restored-session fallback as above.
                                            # This site only reads the target PRIMARY KEY
                                            # (schema_recreate_tables ->
                                            # parse_target_primary_key), which does not
                                            # diverge by engine, so it is safe today --
                                            # changed anyway so one rule holds for every
                                            # engine read rather than two with different
                                            # reasons for being correct.
                                            source_type=session_source_type(session)
                                        ).convert(inventory),
                                        conv_state.edited_target_ddls,
                                        preserve_foreign_keys=(
                                            conv_state.preserve_foreign_keys
                                        ),
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
                                # ENGINE-SPLIT. The MySQL wording is byte-identical and
                                # true: its binlog retains history with no consumer, so a
                                # later CDC-only start really does resume from this
                                # watermark with no re-snapshot. For PostgreSQL it is
                                # false on BOTH counts -- only a replication slot retains
                                # WAL, a Full-load-only run created none, and a slot
                                # cannot be created at a past position -- so recommending
                                # it here handed the operator a one-click route into a
                                # dead start. (The app already says as much 1100 lines
                                # away in _cdc_ui's start-point card.)
                                if (
                                    getattr(
                                        getattr(session, "source_config", None),
                                        "source_type",
                                        None,
                                    )
                                    is SourceType.POSTGRES
                                ):
                                    body = (
                                        "This run created no replication slot, so there "
                                        "is no retained WAL to stream from: on PostgreSQL "
                                        "a gapless handoff needs the slot to exist BEFORE "
                                        "the load. To add replication now, \"CDC only\" "
                                        "can still do it by re-snapshotting every selected "
                                        "table (nothing is lost, but the source is read "
                                        "again); to avoid the re-read, start over as "
                                        "\"Full load + CDC\", which creates the slot at "
                                        "the snapshot point. Use the link below to jump to that "
                                        "setting."
                                    )
                                else:
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
                                        "yet, so you'll deploy it first (usually ~5 min) "
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
                            # AI DBA opener for the DLQ / schema-drift diagnosis chat +
                            # the activity-event seam for CDC transitions.
                            cdc_ai_opener=cdc_ai_opener,
                            ai_post_event=ai_post_event,
                            # So the CDC-step LOB card can lock once a Full Load has
                            # committed data under an exclusion set (survives a switch
                            # to cdc_only): FULL_LOAD stays DONE across the switch.
                            full_load_status=status,
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
    # Same idea for the LOB exclusion: a column excluded AFTER the checks removes it
    # from the load's column set, so a NOT NULL/no-default target column could turn
    # the passed loadability check into a mid-load failure the stale report can't
    # show. Block until the checks are re-run against the new exclusion. Asymmetric:
    # un-excluding a column only adds it back to the (already-checked) load, so it is
    # not a gap. Scoped to the mode the report is for (the exclusion is migration-wide).
    newly_excluded = lob_exclusion_scope_gap(
        state.prereq_report_lob_exclusions(prereq_mode),
        {
            table: frozenset(cols)
            for table, cols in state.lob_exclusions().items()
        },
    )
    if newly_excluded:
        listed = ", ".join(newly_excluded[:3]) + (
            " and more" if len(newly_excluded) > 3 else ""
        )
        return (
            f"Re-run the prerequisite checks — {listed} "
            f"{'were' if len(newly_excluded) > 1 else 'was'} excluded after the checks "
            "ran, so the target's requirements were never re-verified against the "
            "reduced column set."
        )
    # Pass the mode so the reason names the action it gates: on this screen "before
    # running" could mean the Full Load, the billable CDC infrastructure deploy or Start
    # CDC, and the operator cannot tell which is held.
    return prerequisite_block_reason(report, mode=prereq_mode)


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
    ).props("outline no-caps color=primary")


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
    # cdc_infra_prep_state folds EVERY non-stable stack status into "ready", so a stack
    # being TORN DOWN reached this clause and the remedy read "delete the CDC
    # infrastructure on the CDC step first" -- instructing the operator to start the
    # deletion that was already running, with no way to comply and no explanation when it
    # unlocked ~15-25 min later. The lock's own premise does not hold for a deleting stack
    # either: it exists because the stack OWNS each table's immutable Kafka topic
    # partition count, and a stack on its way out owns nothing.
    #
    # Narrowed to the DELETE_IN_PROGRESS literal, not generalised to "any teardown": Stop
    # CDC is an UPDATE that leaves MSK, the topics and that partition plan in place, so
    # unlocking there would let a table be added and then streamed forever on one
    # partition -- the exact harm this lock prevents.
    if (
        "cdc" in substeps_for_type(migration_type)
        and cdc_infra_prep_state(migration_state, job_manager)
        in ("ready", "deploying")
        and not _cdc_stack_being_deleted(migration_state, job_manager)
    ):
        return (
            "Locked — CDC infrastructure is deployed for this table set, and each "
            "table's Kafka topic partitions are fixed when it is created. A table "
            "added now would stream on a single partition. To change the set, delete "
            "the CDC infrastructure on the CDC step first."
        )
    return None


def _cdc_stack_being_deleted(migration_state, job_manager) -> bool:
    """True while THIS session's cdc-stack is being torn down.

    Deliberately keyed on the DELETE_IN_PROGRESS status (or an in-flight delete job), not on
    "any teardown": a Stop CDC is an UPDATE that keeps MSK, the topics and the immutable
    partition plan, so it must keep locking the table set. Pure apart from reading the job
    status; no AWS I/O.
    """
    status = getattr(migration_state, "cdc_stack_phase_status", None)
    if is_stack_teardown_status(status):
        return True
    return bool(
        job_manager is not None and cdc_delete_in_flight(migration_state, job_manager)
    )


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
        # Same split as the CDC step's adopt panel: an in-flight delete is not a failed
        # one, and recommending 'Retain resources' for it would abandon the resources it
        # is about to remove cleanly.
        _in_flight, _terminal = split_cleanup_by_progress(needs_cleanup)
        if _in_flight:
            _busy = ", ".join(f"{name} ({status})" for name, status in _in_flight)
            _has_seeder = _cdc_stack_has_seeder_lambda(migration_state)
            _render_notice(
                ui,
                tone="warning",
                header="CDC infrastructure is still being removed",
                body=(
                    f"{_busy}. This is in progress, not stuck: a cdc-stack delete takes "
                    f"{cdc_teardown_estimate(has_seeder_lambda=_has_seeder)} because "
                    f"{cdc_teardown_reason(has_seeder_lambda=_has_seeder)}. It cannot be "
                    "attached to or replaced while it runs, and MSK / NAT billing stops "
                    "when it completes. No action is needed."
                ),
            )
        if _terminal:
            listed = ", ".join(f"{name} ({status})" for name, status in _terminal)
            _render_notice(
                ui,
                tone="error",
                header=(
                    "Leftover CDC infrastructure needs cleanup — it may still be billing"
                ),
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


def _pg_cdc_handoff_stack(migration_state, source_config, job_manager):
    """Return the CDC stack name for a PostgreSQL Full-Load->CDC gapless handoff, else None.

    When set on :class:`DataMigrationInputs`, the Full Load watermark capture creates a
    logical replication slot + publication on the source at the consistency point (named
    for this stack), so CDC resumes with no gap (see ``_capture_postgres_watermark``).

    Returns the stack name ONLY for a PostgreSQL source on the combined ``Full load + CDC``
    type while CDC is not yet streaming (PostgreSQL is Full-Load-first: the slot bridges to
    a CDC that starts afterward). Returns None for MySQL (which hands off via the binlog
    offset-seeder, not a slot), a Full-Load-only run, or once CDC is already streaming --
    a slot with no consumer would pin the source WAL. Pure.
    """
    if getattr(source_config, "source_type", None) is not SourceType.POSTGRES:
        return None
    if migration_state.migration_type is not MigrationType.FULL_LOAD_AND_CDC:
        return None
    if cdc_streaming_started(migration_state, job_manager):
        return None
    return getattr(migration_state, "cdc_stack_name", None) or None


def _render_migration_type_selector(
    ui,
    migration_state,
    *,
    status,
    refresh,
    locked: Optional[bool] = None,
    lock_reason: Optional[str] = None,
    source_type: SourceType = SourceType.MYSQL,
) -> None:
    """Render the migration type as AWS-console-style radio tiles.

    Each type is a selectable card (radio + icon + title + description), matching
    the Cloudscape "tiles" pattern AWS uses for migration-type choices. The
    selected tile is highlighted (primary border + tint); changing the type
    resets the active sub-step and re-renders. ``locked`` (computed by the caller
    via :func:`migration_type_locked`) disables the whole group so the type
    cannot change once a migration has started; when ``None`` it falls back to
    locking only while this step is ``IN_PROGRESS``.

    ``source_type`` gates the CDC tiles by :func:`source_supports_cdc` (the single
    CDC-by-engine allowlist). MySQL and PostgreSQL both support CDC today, so all
    three tiles are enabled for them; the gate remains as a defensive default so any
    future source engine without CDC renders its CDC tiles disabled (with a note)
    rather than offering a non-functional deploy. Full load only is always
    available. Defaults to MySQL so existing callers keep all three tiles.
    """
    running = locked if locked is not None else (status is StepStatus.IN_PROGRESS)
    selected = migration_state.migration_type
    cdc_available = source_supports_cdc(source_type)

    def _gated(mt: MigrationType) -> bool:
        # A CDC-bearing type on a source whose CDC is not yet supported: disabled.
        return not cdc_available and mt in _CDC_MIGRATION_TYPES

    def _select(new_type: MigrationType) -> None:
        if running:
            return
        if _gated(new_type):
            # CDC is not available for this source engine yet; ignore the click so
            # the type cannot become a non-functional CDC selection.
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
            # Record the migration-type choice in the audit trail: it is the decision
            # that shapes the whole journey (Full Load only / CDC only / both), and it
            # is chosen HERE, not at assessment time (where the default is not yet a
            # real choice). Logged only on an actual change, so re-confirming the
            # current tile on a refresh never spams the log.
            log_activity(
                # The category has to follow the CHOICE: fixed at FULL_LOAD, picking
                # "CDC only" was filed under [full_load], so filtering the log by area
                # hid the decision that defines a CDC migration. A combined run touches
                # both, and the load is what it starts with, so it stays FULL_LOAD.
                ActivityCategory.CDC
                if new_type is MigrationType.CDC_ONLY
                else ActivityCategory.FULL_LOAD,
                "migration type selected",
                status=ActivityStatus.INFO,
                detail=f"migration type set to {new_type.value}",
            )
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
            gated = _gated(mt)
            # Non-interactive while a job runs (whole group locked) OR when this is a
            # CDC tile the source engine cannot yet use (PostgreSQL CDC not shipped).
            disabled = running or gated
            # Cloudscape-tile look: bordered card, primary border + tint when
            # selected, muted + non-interactive while a job runs.
            border = "border-blue-500" if is_selected else "border-gray-300"
            bg = "bg-blue-50" if is_selected else "bg-white"
            interactivity = (
                "opacity-60 cursor-not-allowed"
                if disabled
                else "cursor-pointer hover:border-blue-400"
            )
            tile = ui.card().classes(  # type: ignore[attr-defined]
                f"flex-1 p-3 rounded-lg border {border} {bg} {interactivity} "
                "transition-colors gap-1"
            )
            # _select refuses gated/running clicks, so attaching it unconditionally is
            # safe and keeps the disabled tile inert rather than silently missing.
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
                ui.label(  # type: ignore[attr-defined]
                    migration_type_blurb(mt, source_type)
                ).classes("text-xs text-gray-600")
                if meta.when:
                    ui.label(meta.when).classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-700 font-medium mt-1"
                    )
                if gated:
                    # This CDC tile is disabled because CDC is not available for the
                    # selected source engine. Say so where the user clicks (a silently
                    # dead tile reads as a bug), and steer them to Full load only. The
                    # MySQL binlog/MSK requirements note would be misleading here, so it
                    # is replaced by this one. (MySQL and PostgreSQL both support CDC, so
                    # this only shows for a future engine that does not.)
                    with ui.row().classes("items-start gap-1 no-wrap mt-1"):  # type: ignore[attr-defined]
                        ui.icon("schedule").classes(  # type: ignore[attr-defined]
                            "text-amber-600 text-xs mt-0.5"
                        )
                        ui.label(  # type: ignore[attr-defined]
                            "CDC is not available for this source engine — use Full load only."
                        ).classes("text-xs text-amber-700")
                elif meta.requirements:
                    # Purely informational: what the mode needs (verified later by
                    # the Prerequisites step). Kept a calm neutral gray -- NOT a
                    # warning/error color -- so it reads as a heads-up at decision
                    # time, not an alarm before anything has run. The icon still
                    # distinguishes "needs infra" (info) from "no extra infra"
                    # (check_circle), but both use the same quiet tone. The CDC
                    # requirement is source-aware (MySQL binlog vs. PostgreSQL logical
                    # replication), so a PG source never sees MySQL-only wording.
                    requirements = migration_type_requirements(mt, source_type)
                    needs_infra = "MSK" in requirements
                    with ui.row().classes("items-start gap-1 no-wrap mt-1"):  # type: ignore[attr-defined]
                        ui.icon(  # type: ignore[attr-defined]
                            "info" if needs_infra else "check_circle",
                        ).classes("text-gray-500 text-xs mt-0.5")
                        ui.label(requirements).classes("text-xs text-gray-500")
                # The consequence of this tile that its own copy cannot show, stated where
                # the choice is made. Amber (a real, non-blocking cost) rather than the
                # gray requirements tone: "Full load only" on PostgreSQL is the one option
                # that forfeits a gapless CDC handoff permanently, and an operator reading
                # only this tile -- the cheap, no-infrastructure one -- had no way to know.
                # Empty for every other tile/engine, so nothing else gains a line.
                tradeoff = migration_type_tradeoff(mt, source_type)
                if tradeoff and not gated:
                    with ui.row().classes("items-start gap-1 no-wrap mt-1"):  # type: ignore[attr-defined]
                        ui.icon("history_toggle_off").classes(  # type: ignore[attr-defined]
                            "text-amber-600 text-xs mt-0.5"
                        )
                        ui.label(tradeoff).classes("text-xs text-amber-700")  # type: ignore[attr-defined]
    if running:
        ui.label(  # type: ignore[attr-defined]
            "Migration type is locked once the migration has started."
        ).classes("text-xs text-gray-500")


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


def _log_cdc_event(
    action: str,
    *,
    detail: "Optional[str]" = None,
    status: ActivityStatus = ActivityStatus.STARTED,
    exc: "Optional[BaseException]" = None,
) -> None:
    """Append a discrete CDC lifecycle milestone to the activity log.

    Records control-plane actions (deploy / start / stop / teardown) and connector
    state transitions as one-line events -- the audit trail of WHAT happened to the
    CDC pipeline and WHEN. Continuous progress (replication lag / applied counts)
    stays out of the log (it would flood the rotated file); it lives in the live
    monitoring panel.
    """
    log_activity(
        ActivityCategory.CDC, action, status=status, detail=detail,
        # Forwarded so a DEBUG level attaches the (value-free) stacktrace. Without it a
        # failed deploy / start / stop / teardown -- the actions an operator is most likely
        # to be stuck on -- gained nothing from raising the level.
        exc=exc,
    )


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
    last = getattr(migration_state, "_last_logged_connector_states", None)
    if last is None:
        # FIRST observation of this session: SEED, do not log. "No last-seen state" was
        # being treated as "changed", so the very first poll of a session wrote
        # "connector X running" -- a positive factual claim, at that timestamp, about a
        # connector nobody had seen change. Observed live: a fresh page render logged two
        # connectors RUNNING for a cdc-stack that had been DELETE_COMPLETE for twelve
        # hours, and the activity log is an AUDIT TRAIL, so an investigation starts from
        # that false line. The docstring's promise -- only a CHANGE is logged -- is now
        # actually kept; a genuine transition after this seed still logs.
        migration_state._last_logged_connector_states = {
            n: str(st).upper() for n, st in states.items()
        }
        return
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
    # NAME the log group instead of promising a link that does not exist: nothing in the
    # UI ever rendered one (grep for msk-connect/ finds only the deployer's own read), so
    # "linked on the CDC step" sent the operator looking for something that was not there.
    # The name is deterministic -- cdc-stack.yaml creates /msk-connect/<stack>-cdc.
    _stack = getattr(migration_state, "cdc_stack_name", "") or CDC_DEFAULT_STACK_NAME
    parts.append(
        f"see CloudWatch log group /msk-connect/{_stack}-cdc for the task stack trace"
    )
    return "; ".join(parts)


# Checks that only CDC requires (MySQL binlog/GTID, PostgreSQL WAL/replication, MSK).
# Everything else is common to Full Load and CDC. Used to tag each result row with the
# phase that needs it so the combined "Full load + CDC" panel shows it covers both (the
# CDC run is a superset of the Full Load checks -- see core.prerequisites). The
# PostgreSQL CDC-only ids (WAL_LEVEL_LOGICAL / REPLICATION_ROLE / REPLICATION_SLOTS /
# SOURCE_IS_WRITER / REPLICA_IDENTITY) MUST be listed too: omitting them made a PG
# source's CDC-only failures (e.g. wal_level=replica, the RDS default) read as
# "Full Load: Blocked" on the combined panel, though Full Load does not need them.
_CDC_ONLY_CHECK_IDS = frozenset(
    {
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.BINLOG_RETENTION,
        PrerequisiteCheckId.GTID_MODE,
        PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
        PrerequisiteCheckId.REPLICATION_ROLE,
        PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
        PrerequisiteCheckId.REPLICATION_SLOTS,
        PrerequisiteCheckId.SOURCE_IS_WRITER,
        PrerequisiteCheckId.TABLE_REPLICABLE,
        PrerequisiteCheckId.REPLICA_IDENTITY,
        # SLOT_WAL_RETENTION had drifted out of this set, so the CDC-handoff WAL check --
        # the PostgreSQL counterpart of BINLOG_RETENTION, which IS listed -- was tagged
        # "Full load + CDC" on the combined panel even though only CDC needs it.
        PrerequisiteCheckId.SLOT_WAL_RETENTION,
        # Same drift risk as SLOT_WAL_RETENTION above: omitting it would tag a CDC-only
        # finding "Full load + CDC" and make it read as "Full Load: Blocked".
        PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
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
    refresh=None,
) -> bool:
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

    ``refresh`` re-renders the screen. Only the gapless-handoff choice below needs it (to
    move the operator onto the Full Load sub-step once its re-graded checks pass); every
    other action here re-renders via ``run_checks``. Optional so a test can render the
    panel without one.

    **Returns** whether the gapless-handoff route choice was rendered. The caller draws
    the run guard directly below this panel, and when a choice is pending that guard would
    be the FOURTH statement of one failure's severity -- sitting *under* the solution, so
    the screen reads "here is how to fix it" then "you must fix it". Returning the fact
    rather than letting the caller re-derive it keeps the two in lockstep by construction.
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
    if report is None:
        return False
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
    # The continuation affordance, BESIDE the failing row rather than in a dialog behind
    # the button this very failure disables. Without it a PostgreSQL "Full load only ->
    # CDC only" operator has no route at all: Deploy is gated on this FAIL, and the only
    # place that could record the re-snapshot decision was the Start CDC dialog, which
    # needs infrastructure that cannot be deployed.
    if mode is MigrationMode.CDC and resnapshot_resolvable_failures(report):
        _render_gapless_choice(ui, migration_state, run_checks, refresh=refresh)
        return True
    return False


def _render_gapless_choice(ui, migration_state, run_checks, *, refresh=None) -> None:
    """The PostgreSQL "Full load only -> CDC" fork: ONE situation, two routes, a button each.

    Shipped first (v0.1.510/511) as three stacked notices -- amber situation, blue
    recommendation, blue alternative -- plus a lone flat button. That layout said one
    problem's severity four times (Blocked badge, "1 failed" badge, amber notice, red
    guard line) with two blue panels wedged in between, and it left the ONLY clickable
    action on the route it explicitly does not recommend: visual pull and stated advice
    pointing opposite ways, with the recommended route reduced to "change the migration
    type above and run the Full Load again" -- instructions for work the tool can do
    itself. The codebase had already recorded this failure mode once, in
    ``stale_error_notice``: "three verdicts at once, and the user cannot tell which is
    true."

    So: one box states the situation, and the two routes sit inside it as peers, each with
    its own button and a one-line summary of what it leaves behind. Costs and mechanics go
    in a collapsed expansion, because the operator picking a route needs the difference
    (~11 lines of prose to reach "there are two options" was the complaint), not the full
    derivation up front.

    Both handlers RE-RUN the (read-only, seconds) prerequisite checks, so one re-graded
    report clears every gate at once instead of leaving a red FAIL beside an enabled
    primary button.
    """
    # RECOMMENDED FIRST, and deliberately so -- against the usual "primary action on the
    # right" rule, which governs a form's action row, not a two-way fork where reading
    # order IS the recommendation. Both routes re-read every table, so the re-read is not
    # what distinguishes them; re-running as "Full load + CDC" (a) uses the tool's own bulk
    # loader rather than the streaming pipeline, (b) creates the slot BEFORE the load so
    # everything from the snapshot point on is streamed, and (c) can DROP each target table
    # first, which is the only way to clear rows deleted on the source since the last load.
    # The re-snapshot route leaves those rows behind forever. Same cost, strictly better
    # result -- so it leads, and it is the one that carries color=primary.
    async def _start_over() -> None:
        migration_state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
        # Clear any re-snapshot decision recorded earlier in this session. Without this, an
        # operator who clicked "Re-snapshot every table" first and then changed their mind
        # would get snapshot.mode=initial + publication.autocreate=filtered on the very run
        # whose entire point is to stream from the slot the tool creates at the snapshot
        # point -- silently paying for a second full read and discarding the gapless
        # handoff this route exists to restore.
        migration_state.set_cdc_start_mode("auto")
        log_activity(
            ActivityCategory.FULL_LOAD,
            "migration type selected",
            status=ActivityStatus.INFO,
            detail=(
                "migration type set to "
                f"{MigrationType.FULL_LOAD_AND_CDC.value} to restore a gapless "
                "CDC handoff"
            ),
        )
        _mode = prereq_mode_for_type(MigrationType.FULL_LOAD_AND_CDC)
        await run_checks(_mode)
        # Land on the Full Load screen -- the work this route needs next -- but ONLY when
        # the re-graded report says it can actually run. Navigating unconditionally would
        # drop the operator on a blocked Run button with the explanation left behind on the
        # panel they were moved off.
        _report = migration_state.get_prereq_report(_mode)
        if _report is not None and getattr(_report, "can_proceed", False):
            migration_state.set_active_substep("full_load")
            if refresh is not None:
                refresh()

    async def _take_resnapshot() -> None:
        # ONE knob. "manual" forces snapshot.mode=initial, and
        # publication.autocreate.mode=filtered is DERIVED from that, so the connector's own
        # database user creates the publication this load never had (over exactly the
        # captured tables -- the tool itself still never writes to the source). This used to
        # be two independent values that both had to be set: v0.1.513 fixed this call site
        # by setting both, and the pair still broke from the start-position radio and from a
        # session restore (the second flag was never persisted), because discipline cannot
        # hold an invariant that the type system can.
        migration_state.set_cdc_start_mode("manual")
        log_activity(
            ActivityCategory.CDC,
            "CDC start mode selected",
            status=ActivityStatus.INFO,
            detail="re-snapshot every selected table (no gapless slot for this load)",
        )
        await run_checks(MigrationMode.CDC)

    def _route(
        *, label: str, icon: str, primary: bool, summary: str, detail: str, on_click
    ) -> None:
        with ui.column().classes("flex-1 min-w-0 gap-1"):
            btn = ui.button(label, icon=icon, on_click=on_click)  # type: ignore[attr-defined]
            btn.props("color=primary" if primary else "outline color=primary")
            # A fixed floor on the summary height keeps the two routes' expansions on the
            # same baseline: the summaries wrap to different line counts, and a ragged pair
            # reads as two unrelated blocks rather than two options being compared.
            ui.label(summary).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-700 min-h-[2.25rem]"
            )
            # header-class strips Quasar's own header padding so the expansion's label lines
            # up with the summary above it instead of sitting indented from it.
            with ui.expansion("What this does, and what it costs").props(  # type: ignore[attr-defined]
                'dense header-class="q-px-none"'
            ).classes("w-full text-xs"):
                ui.label(detail).classes("text-xs text-gray-700")  # type: ignore[attr-defined]

    with notice_container(
        ui,
        tone="warning",
        icon="history_toggle_off",
        header="A gapless handoff is no longer possible for this load",
        body=(
            "Nothing was retaining WAL while the Full Load ran, and a replication slot "
            "cannot be created at a past position, so the changes committed since the "
            "load cannot be replayed. Both ways forward re-read the source tables. They "
            "differ in what they leave behind."
        ),
    ):
        with ui.row().classes("w-full gap-4 items-start flex-wrap"):  # type: ignore[attr-defined]
            _route(
                label='Start over as "Full load + CDC"',
                icon="replay",
                primary=True,
                summary=(
                    "Recommended — also clears rows deleted on the source since the load."
                ),
                detail=(
                    "Switches the migration type and re-runs the prerequisite checks for "
                    "it, then opens the Full Load. The tool creates the replication slot "
                    "at the new snapshot point BEFORE loading, so CDC resumes from it with "
                    "no gap — and you can start the CDC phase whenever you like, it does "
                    "not have to be now. Choose to DROP each table when the load asks: "
                    "that also clears any row deleted on the source since the first load, "
                    "which nothing else here can do. It reads the source again, but so "
                    "does the alternative, and it uses the bulk loader rather than the "
                    "streaming pipeline."
                ),
                on_click=_start_over,
            )
            _route(
                label="Re-snapshot every table",
                icon="restart_alt",
                primary=False,
                summary=(
                    "Keeps this Full Load — but a row deleted on the source since the "
                    "load stays on the target."
                ),
                detail=(
                    "Adds CDC without a second load pass: the connector creates its own "
                    "publication over exactly the captured tables, snapshots them, then "
                    "streams from the slot it made — there is no window between the "
                    "snapshot and the stream. Choose this if re-running the load is not "
                    "acceptable. Two costs: the source tables are read again through the "
                    "streaming pipeline, and a row DELETED between the load and the start "
                    "is in neither the snapshot nor the stream, so it stays on the target "
                    "(Validation reports it as an extra row — reload that table to clear "
                    "it)."
                ),
                on_click=_take_resnapshot,
            )


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
            # BOTH, not "remediation or detail". ``detail`` carries the OBSERVED value and
            # ``remediation`` the instruction, and preferring remediation dropped the
            # measurement on every FAIL/WARN row -- so SLOT_WAL_RETENTION told the operator
            # to raise a cap whose current value the panel never showed, and a REPLICA
            # IDENTITY failure named no partition. The finding first, then what to do.
            "detail": " ".join(
                part for part in (result.detail, result.remediation) if part
            ),
            **(
                {"phase": prereq_phase_tag(result.check_id, combined=True)}
                if combined
                else {}
            ),
        }
        for result in results
    ]
    # WRAP the cells. "Detail / remediation" is by far the widest column -- a failed
    # PostgreSQL replication-objects check carries ~800 characters of detail plus
    # remediation -- and on one line it pinned the table ~3.5x its card, so the row was
    # clipped ("The connector does n...") and only reachable through a horizontal
    # scrollbar. The longest remediation is the one the operator most needs and was the
    # least visible. Costs row height, which the page can scroll; see WRAP_CELLS_PROP.
    ui.table(columns=columns, rows=rows, row_key="check").classes("w-full").props(
        WRAP_CELLS_PROP
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
    "resnapshot_resolvable_failures",
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
    "source_supports_cdc",
    "prereq_mode_for_type",
    "migration_type_requirements",
    "migration_type_tradeoff",
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
    "scope_lob_candidates",
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
    "cdc_prerequisite_block_header",
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
    derive_cdc_vpc_from_source,
    _cdc_is_streaming,
    _cdc_stack_has_seeder_lambda,
    _cdc_tables_for_config,
    _cdc_target_region,
    _cdc_watermark,
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
    exclude_and_reload_block_reason,
    lob_exclusion_lock,
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
    cdc_delete_in_flight,
    cdc_evidence_unverified,
    cdc_pipeline_live,
    cdc_infra_deploy_in_flight,
    cdc_streaming_started,
    cdc_unstable_message,
    classify_cdc_card_phase,
)



# The Full Load render cluster was extracted to _full_load_ui.py for maintainability
# (mirrors the _cdc_ui.py split above). Re-exported here -- AFTER the _cdc_ui re-export,
# since _full_load_ui imports cdc_streaming_started from _cdc_ui -- so dm.<name> and every
# consumer/test import resolve unchanged; content()'s calls resolve as module globals.
from dsql_migrator.ui.data_migration._full_load_ui import (  # noqa: E402,F401
    _LOAD_STATE_COLORS,
    _LOAD_STATE_LABELS,
    _QUARANTINE_PK_CHIP_LIMIT,
    _abbrev_count,
    _format_attempts_cell,
    _format_progress_cell,
    _format_rows_on_target_cell,
    _group_quarantine_entries,
    _incomplete_is_quarantine_only,
    _parse_quarantined_pk,
    _quarantine_detail_row,
    _quarantined_cell_tooltip,
    _quarantined_reason,
    _quarantined_row_count,
    _render_accept_quarantine_action,
    _render_completeness_banner,
    _render_error_log,
    _render_full_load_progress,
    _render_full_load_step,
    _render_load_status,
    _render_table_state_summary,
    _render_watermark,
    _rows_breakdown_tooltip,
    _rows_target_source_cell,
    format_selected_workloads,
)
