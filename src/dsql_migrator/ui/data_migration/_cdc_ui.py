# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC (Change Data Capture) UI for the Data Migration screen.

Extracted verbatim from ``data_migration/__init__.py`` -- a pure code move with
no behavior change. Every function and constant here previously lived in that
module and is re-exported from the package ``__init__`` (at the bottom of that
file), so ``dm.<name>`` attribute access and all existing monkeypatch targets keep
resolving exactly as before. This module owns the optional CDC data-plane UI:
source/start-point cards, infra + start/stop/delete dialogs and their control-plane
actions, the deploy/live-status panels, DLQ + pipeline-health monitoring, and the
shared live migration-table status grid. The Full Load screen, the activity-log
anchor (``_log_cdc_event``) and its connector-transition logger, and the
migration-type lock helper stay in ``__init__``.
"""

from __future__ import annotations

import inspect

from dataclasses import (
    dataclass,
)
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Mapping,
    Optional,
)

from dsql_migrator.core.cdc import (
    CDC_DEFAULT_DLQ_TOPIC,
    CDC_DEFAULT_STACK_NAME,
    CDC_DEFAULT_TOPIC_PREFIX,
    CDC_PLACEHOLDER_PREFIX,
    CDC_STACK_NAME_PREFIX,
    CdcPipelineOrchestrator,
    CdcResumePoint,
    MskSeedAdmission,
    build_cdc_stack_name,
    cdc_expected_connector_names,
    composite_cdc_excluded_key_columns,
    cdc_stack_name_suffix,
    cdc_stack_params_to_json,
)
from dsql_migrator.core.cdc_coords import (
    parse_binlog_coordinate,
    validate_gtid,
)
from dsql_migrator.core.job_manager import (
    JobNotFoundError,
    is_interrupted_by_restart,
)
from dsql_migrator.core.cdc import (
    cdc_teardown_estimate,
    cdc_teardown_reason,
    stack_has_seeder_lambda,
)
from dsql_migrator.core.cdc_postgres import (
    dispatch_cdc_infra_params,
    dispatch_cdc_stack_params,
    dispatch_source_config,
    pg_publication_autocreate_mode,
    pg_snapshot_mode,
)
from dsql_migrator.core.models import (
    LoadStatusView,
    MigrationMode,
    SourceInventory,
    SourceType,
    StepStatus,
    Watermark,
)
from dsql_migrator.core.table_selection import (
    TableSelector,
)
from dsql_migrator.ui.data_migration._models import (
    MigrationType,
    _BROKER_MESSAGE_LIMIT_MIB,
    _DSQL_VALUE_LIMIT_MIB,
    assess_dlq_health,
    build_lag_chart_option,
    build_migration_table_status,
    cdc_handling_facts,
    cdc_prerequisite_block_header,
    cdc_prerequisite_block_reason,
    connector_health_rows,
    connector_role_label,
    format_column_exclude_list,
    format_duration,
    lob_exclusion_candidates,
    per_table_counts_notice_body,
)
from dsql_migrator.core.cdc_stack_deployer import is_stable_stack_status
from dsql_migrator.ui.data_migration._cdc_status import (
    _CDC_ACTION_NOUN,
    _CDC_ACTION_TERMINAL,
    _CDC_ACTION_TITLE,
    _CDC_DEPLOY_STAGE_STYLE,
    _CDC_STAGE_ETA_SECONDS,
    _CDC_STAGE_LABELS,
    _CDC_TONE_STYLE,
    _apply_cdc_status,
    _ascii_log,
    _cdc_status_view,
    _current_job,
    _deploy_total_duration,
    _fetch_cdc_status,
    _fetch_migration_row_counts,
    _format_eta_hint,
    _is_inflight_stack_status,
    cdc_attach_scope_mismatch,
    split_attachable_stacks,
    split_cleanup_by_progress,
    _migration_status_tables,
    _read_cdc_template_body,
    cdc_dlq_records,
    cdc_dlq_summary,
    cdc_error_log_key,
    is_cdc_error_record,
    should_replace_teardown_marker,
)
from dsql_migrator.ui.design import (
    EXPANSION_PANEL_CLASSES,
    NOTICE_STYLE,
    definition_row,
    inline_hint,
    render_notice,
    section_header,
)

# These four names live in the package ``__init__``: the activity-log anchor
# ``_log_cdc_event`` (a monkeypatch target for the connector-transition logger that
# stays there), the ``migration_type_lock_reason`` helper, and the ``_LOGGER`` /
# ``_render_notice`` module constants. ``__init__`` imports THIS module for
# re-export, so the import below is only safe because ``__init__`` performs that
# re-export at the very bottom -- after these four names are already bound. The
# moved functions reference them as module globals, so they must be real imports
# here (a module-level ``__getattr__`` would not satisfy a function's LOAD_GLOBAL).
from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.ui.data_migration import (
    _log_cdc_event,
    migration_type_lock_reason,
    _LOGGER,
    _render_notice,
)

# The pure CDC state/phase predicates were extracted to _cdc_state.py so both this
# module and _cdc_monitoring.py can import them without a cycle. Re-exported here so
# _cdc_ui.<predicate> and every consumer/test import resolve unchanged.
from dsql_migrator.ui.data_migration._cdc_state import (  # noqa: E402,F401
    _CDC_POLL_INTERVAL_SECONDS,
    _cdc_is_streaming,
    cdc_infra_deploy_in_flight,
    cdc_monitoring_visible,
    cdc_delete_in_flight,
    cdc_evidence_unverified,
    cdc_pipeline_live,
    cdc_streaming_started,
    cdc_teardown_badge,
)


def _cdc_source_type(session) -> SourceType:
    """The session's source engine (default MySQL), for CDC config dispatch."""
    return getattr(
        getattr(session, "source_config", None), "source_type", SourceType.MYSQL
    )


def _cdc_source_database(session) -> str:
    """The session's source database name (PostgreSQL captures a single database)."""
    return getattr(getattr(session, "source_config", None), "database", "") or ""


def _session_source_credentials(session) -> Optional[tuple[str, str]]:
    """The session's own source username/password, for the teardown's slot drop.

    Property 7 is unchanged: these live in process memory only and are handed straight to a
    source connection -- never logged, never written to job state. They exist because the
    operator typed them to connect, which makes them the one credential path a teardown can
    rely on without an IAM grant. Reading the tool-managed CDC secret instead needs
    ``secretsmanager:GetSecretValue``, which the app's own identity does not have, and that
    is why a real teardown left a WAL-pinning replication slot behind.

    Returns ``None`` when there is no usable password (a Secrets-Manager-auth source, or a
    restored session whose password was not re-entered), so the caller falls back to the
    secrets path as before.
    """
    if session is None:
        return None
    config = getattr(session, "source_config", None)
    password = getattr(session, "source_password", None)
    if not (password or "").strip():
        return None
    return (getattr(config, "username", "") or "", password)


def _cdc_stack_has_seeder_lambda(migration_state) -> Optional[bool]:
    """Does THIS session's cdc-stack contain the in-VPC offset-seeder Lambda?

    It is the only slow resource in a teardown (measured 18m30s of ENI reclamation, against
    MSK Serverless's 93s), so it is what decides the estimate the operator is shown. Read off
    the bound session's source engine rather than plumbed through every render layer:
    ``DataMigrationState`` already holds the session (``bind_session``), and a PostgreSQL
    stack is always created without the Lambda. Returns ``None`` when the engine cannot be
    determined, so the caller shows a range covering both cases instead of guessing.
    """
    session = getattr(migration_state, "_session", None)
    if session is None:
        return None
    source_type = _cdc_source_type(session)
    if source_type is None:
        return None
    # The seed mode only matters for MySQL (PostgreSQL is forced external), and reading the
    # host config here would be I/O on a render path. The default IS lambda, so the MySQL
    # answer stays the conservative "expect the ENI wait".
    return stack_has_seeder_lambda(source_type, None)


def _cdc_start_seed_mode(source_type: SourceType, host_seed_mode: str) -> str:
    """Engine-aware CDC-Start SeedMode -- ONE source of truth shared with the infra build.

    A PostgreSQL cdc-stack is ALWAYS created ``SeedMode=External`` (no in-VPC offset-seeder
    Lambda; it resumes from a logical replication slot, and ``build_pg_cdc_infra_params``
    hard-codes ``seed_mode="external"``). The External path does its Kafka prep -- creating
    the pinned offset / DLQ / per-table data topics + seeding the offset -- **in-process at
    Start** (``run_cdc_start``'s ``if seed_mode == "external"`` branch). So Start MUST also
    use ``"external"`` for a PostgreSQL source, derived from the SOURCE ENGINE and NOT from
    the host's ``cdc_seed_mode``: on a host whose ``cdc_seed_mode`` is the default (not
    external) that branch would be skipped, the topics never created, and PG CDC never
    streams. MySQL keeps deriving from the host config (the in-VPC EC2 host sets
    ``external``; Fargate/local -> ``lambda``), so MySQL behavior is unchanged.
    """
    if source_type is SourceType.POSTGRES:
        return "external"
    return host_seed_mode


def _cdc_resume_signal(migration_state, session):
    """Resolve the engine-appropriate CDC start signal for the config builders.

    Returns ``(resume_override, force_initial_snapshot)`` so every ``dispatch_source_config``
    call site stays consistent:

    - **MySQL** -- a Manual choice supplies a GTID / binlog ``file:position`` that is seeded
      into ``connect-offsets``; pass it as ``resume_override`` (only when it has usable
      coordinates). ``force_initial_snapshot`` is always ``False`` (not a MySQL concept).
    - **PostgreSQL** -- Debezium resumes only from the replication slot and cannot start
      from an arbitrary WAL LSN, so Manual means "re-snapshot from scratch" (``initial``):
      there is no override value, just ``force_initial_snapshot = (mode == "manual")``.
      Automatic uses the gapless slot (``never``).
    """
    mode = migration_state.cdc_start_mode()
    if _cdc_source_type(session) is SourceType.POSTGRES:
        return None, mode == "manual"
    override = migration_state.cdc_start_override()
    usable = mode == "manual" and override is not None and override.has_coordinates()
    return (override if usable else None), False


def _logged_cdc_lifecycle(action: str, *, detail: str, work):
    """Wrap a CDC lifecycle job body so its OUTCOME reaches the activity log.

    The four lifecycle actions (deploy infra / start / stop / delete) each take
    minutes to tens of minutes, and the ``_log_cdc_event`` at their submit site only
    records that they were STARTED. Nothing logged their result, so the audit trail
    could not answer "did the Stop before cut-over actually succeed, and when?" --
    the question that matters most at the moment an operator is deciding to cut over.
    Connector-state transitions were the only proxy, and those are written by the UI
    POLLER, so they are missed entirely whenever the operator navigates away while a
    20-30 minute action runs.

    Wrapping the job body fixes both: this runs on the background job thread, so the
    outcome is recorded regardless of what the UI is showing. The wrapped ``work``
    keeps its own signature/behavior, and the ``core`` deployer functions are
    untouched -- ``core`` deliberately has no activity-log dependency, so the logging
    stays in this UI layer (mirroring how Full Load logs from ``_engine``).

    Records SUCCESS with the elapsed time, FAILURE with the elapsed time + the error
    (re-raising so the JobManager still marks the job FAILED), or INFO for a
    cooperative cancel -- ``run_cdc_*`` returns normally when cancelled, so the
    handle is what distinguishes "stopped early" from "finished".

    Known gap: if the PROCESS dies mid-action nothing runs here, so that job is left
    with only its STARTED line (the JobManager reconciles it to FAILED on restart,
    but does not log an activity event).
    """
    import time as _time

    def wrapped(handle) -> None:
        started = _time.monotonic()

        def _elapsed() -> str:
            return format_duration(max(0.0, _time.monotonic() - started))

        try:
            work(handle)
        except Exception as exc:  # noqa: BLE001 - log the outcome, then re-raise
            _log_cdc_event(
                action,
                status=ActivityStatus.FAILURE,
                detail=(
                    f"{detail} — failed after {_elapsed()}: "
                    f"{type(exc).__name__}: {exc}"
                ),
                exc=exc,
            )
            raise
        if bool(getattr(handle, "cancelled", False)):
            _log_cdc_event(
                action,
                status=ActivityStatus.INFO,
                detail=f"{detail} — cancelled after {_elapsed()}",
            )
            return
        _log_cdc_event(
            action,
            status=ActivityStatus.SUCCESS,
            detail=f"{detail} — completed in {_elapsed()}",
        )

    return wrapped


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
    full_load_status: "Optional[StepStatus]" = None,
    cdc_ai_opener=None,
    ai_post_event=None,
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
    # 1. Where CDC runs (orientation banner) -- only BEFORE the cdc-stack is
    #    deployed; once it exists the flow is self-evident, so hide it to cut noise.
    _render_cdc_runs_on_banner(
        ui, phase=getattr(migration_state, "cdc_stack_phase", None)
    )

    # NOTE: provisioning is NOT rendered here. The lifecycle card in step 4
    # (_render_cdc_start_action) already owns it: on an absent stack it renders the
    # same BYO-VPC deploy form (or the adopt choice), so adding a second call here
    # showed the identical form twice on one screen. The Prerequisites copy is the
    # *extra* entry point -- offered there only so the ~5 min MSK create can
    # overlap a Full Load -- and it is suppressed for CDC only, which has no Full Load
    # to overlap. See _render_cdc_infra_prep_section.

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
    #
    # Only for CDC ONLY: any type that also runs a Full Load renders this same
    # migration-wide selection on the Full Load screen (before the load, so the
    # load and prerequisite gate honor it), and locks it once the load's checks
    # run. Re-rendering it here would double the card and, worse, let the operator
    # think a post-Full-Load tick still changes what the completed load wrote. CDC
    # only has no Full Load screen, so this is its single home.
    if migration_type is MigrationType.CDC_ONLY:
        # A Full Load committed under an exclusion set survives a switch to cdc_only:
        # the FULL_LOAD workflow step stays DONE (even after the job record is pruned)
        # and job_id persists. Either signal means the exclusion is baked into loaded
        # data and must not change now (silent split-brain) -- so lock the card, exactly
        # as selection_lock_reason locks the Full Load screen's copy on `has_job or DONE`.
        full_load_committed = (
            full_load_status is StepStatus.DONE
            or getattr(migration_state, "job_id", None) is not None
        )
        lob_locked, lob_lock_reason = lob_exclusion_lock(
            migration_state, job_manager, full_load_committed=full_load_committed
        )
        _render_cdc_lob_exclusion_panel(
            ui,
            migration_state,
            inventory,
            refresh,
            locked=lob_locked,
            lock_reason=lob_lock_reason,
            source_type=_cdc_source_type(session),
        )

    # 4. START: actually deploy the connectors (cloudformation update_stack) and
    #    show step-by-step progress.
    _render_cdc_start_action(
        ui, migration_state, job_manager, refresh,
        inventory=inventory, session=session,
    )

    # 5. MONITOR: live connector health + DLQ, meaningful only once streaming.
    # session is threaded through so the drift banner can offer the opt-in
    # ADD COLUMN fix (it needs the source + target connections).
    _render_cdc_live_monitoring(
        ui, migration_state, job_manager, session=session,
        cdc_ai_opener=cdc_ai_opener, ai_post_event=ai_post_event,
    )

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
    watermark = _cdc_watermark(job)
    source_type = _cdc_source_type(session)
    is_pg = source_type is SourceType.POSTGRES
    override = migration_state.cdc_start_override()
    mode = migration_state.cdc_start_mode()
    # The watermark resume (gapless) is the Automatic option's source.
    wm_resume = (
        CdcResumePoint.from_watermark(watermark) if watermark is not None else None
    )
    if is_pg:
        # PostgreSQL Automatic = gapless resume from the logical replication slot created
        # at the Full Load consistency point. BOTH the WAL LSN and the recorded SLOT NAME
        # are required, and the slot is the load-bearing half: only a Full-Load-+-CDC run
        # writes it, and it is the tool's only record that a slot exists AT that LSN. The
        # comment here already CLAIMED "the WAL LSN + slot name" while the predicate tested
        # the LSN alone -- so a Full-load-only watermark rendered "Automatic -- gapless
        # from the replication slot (recommended)", a positive Ready badge and a green
        # "Start point set" line, promising gaplessness with no slot in existence. Same bug
        # shape as the MySQL GTID-only post-mortem 11 lines below. A slot cannot be created
        # at a past LSN, so without the record the only honest option is Manual.
        # Manual = re-snapshot from scratch (snapshot.mode=initial), which needs no
        # coordinate and is ALWAYS a resolvable start point -- Debezium PG resumes only from
        # the slot and cannot start from an arbitrary WAL LSN, so there is no binlog offset
        # to seed and the MySQL can_seed_offset()/gtid-only tests do not apply.
        wm_usable = (
            wm_resume is not None
            and wm_resume.can_resume_from_lsn()
            and bool(getattr(watermark, "slot_name", None))
        )
        wm_gtid_only = False
        effective_resume: Optional[CdcResumePoint] = (
            wm_resume if (mode == "auto" and wm_usable) else None
        )
    else:
        # Gate on what the SEEDER actually needs (binlog file:pos), not on the broad
        # "has any coordinate" test. A GTID-only watermark passed has_coordinates(), so the
        # card claimed "Automatic -- gapless from Full Load (recommended)" and showed Ready --
        # while build_watermark_params returned all-empty values, the template skipped the
        # seeder, and the connector started from the CURRENT binlog, losing every change made
        # during the load. The failure was silent until Validation, or after cut over.
        #
        # Reachable, not theoretical: the coordinates come from SEPARATE queries that degrade
        # independently -- SHOW MASTER STATUS needs REPLICATION CLIENT (commonly restricted on
        # RDS/Aurora) while @@GLOBAL.gtid_executed is a plain global read.
        wm_usable = wm_resume is not None and wm_resume.can_seed_offset()
        # A GTID set with NO file:pos: there IS a watermark and it looks usable, but it cannot
        # seed the offset. Called out separately so the reason is explicit instead of the
        # generic "no watermark" wording, which would be wrong here.
        wm_gtid_only = (
            wm_resume is not None
            and not wm_resume.can_seed_offset()
            and bool(wm_resume.gtid_executed)
        )
        # Effective start position: in manual mode an entered override wins; in auto
        # mode the watermark is used.
        if mode == "manual" and override is not None and override.has_coordinates():
            effective_resume = override
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
    # A stopped-but-previously-streamed pipeline resumes from its own committed offset, so
    # there is no start point left to choose. Without this the card contradicted the
    # button beneath it: "Action needed" / "needs a Full Load watermark" while Start CDC
    # was enabled and would have worked.
    resumes_from_offset = bool(
        getattr(migration_state, "cdc_has_committed_offset", False)
    )
    _render_cdc_start_point_card(
        ui,
        migration_state,
        refresh,
        wm_resume=wm_resume,
        wm_usable=wm_usable,
        effective_resume=effective_resume,
        mode=mode,
        locked=started,
        session=session,
        source_type=source_type,
        wm_gtid_only=wm_gtid_only,
        resumes_from_offset=resumes_from_offset,
    )

    # On a restart the connector config below is not what decides the start position (the
    # committed offset is), so an absent effective_resume must not hide the rest of the
    # card -- it is exactly the state a resume renders in. A PostgreSQL Manual choice
    # (re-snapshot, snapshot.mode=initial) is likewise a resolvable start with no coordinate,
    # so it must not early-return either.
    if (
        effective_resume is None
        and not resumes_from_offset
        and not (is_pg and mode == "manual")
    ):
        return

    # Build the connector config (pure -- no AWS calls). Restrict the table list
    # to what the watermark covered when inventory + watermark exist; otherwise
    # fall back to the user's confirmed selection (manual seed).
    exclusions = migration_state.lob_exclusions()
    exclude_value = format_column_exclude_list(
        {table: sorted(cols) for table, cols in exclusions.items()}
    )
    exclude_list = exclude_value.split(",") if exclude_value else None
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    _resume_override, _force_initial_snapshot = _cdc_resume_signal(migration_state, session)

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

    config = dispatch_source_config(
        _cdc_source_type(session),
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        database=_cdc_source_database(session),
        stack_name=getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME),
        column_exclude_list=exclude_list,
        resume_override=_resume_override,
        force_initial_snapshot=_force_initial_snapshot,
    )

    # The sink config (topics, keying, DLQ). DLQ name is the cdc-stack default --
    # NOT derived from a connector that may not exist yet (pre-deploy). Engine-neutral
    # (the DSQL sink is the same for both source engines); the deployed connector is
    # named ``<stack>-dsql-sink``, so the preview uses "dsql-sink" (not "mysql-sink",
    # which was wrong for BOTH engines and misleading on a PostgreSQL source).
    sink = CdcPipelineOrchestrator().build_sink_config(
        "dsql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC
    )

    # Build the deployable cdc-stack parameter set: tool-known values filled,
    # customer-environment values as <FILL_ME> placeholders.
    target = getattr(session, "target_config", None)
    params = dispatch_cdc_stack_params(
        config,
        sink,
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME),
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        # Same value the deploy would send, so this preview cannot show a
        # SinkMcuCount the actual Start CDC then contradicts.
        sink_mcu_count=_sink_mcu_count(),
    )

    # SECONDARY (collapsed): connector configuration to hand to the cdc-stack.
    # Human-readable summary + the actual deployable parameter file. Advanced
    # output, so progressive disclosure keeps the start-point decision primary.
    source_lines = [
        f"snapshot.mode = {config.snapshot_mode}",
        f"table.include.list = {', '.join(config.table_include_list) or '(all selected)'}",
    ]
    # MySQL start point (GTID / binlog file+pos) -- a PostgreSQL source config has
    # neither attribute, so read them defensively (accessing config.start_gtid on a
    # PostgresSourceConfig raised AttributeError and crashed the CDC step render).
    start_gtid = getattr(config, "start_gtid", None)
    if start_gtid:
        source_lines.append(f"gtid (start) = {start_gtid}")
    start_binlog_file = getattr(config, "start_binlog_file", None)
    if start_binlog_file:
        source_lines.append(
            f"binlog (start) = {start_binlog_file}:{getattr(config, 'start_binlog_pos', '')}"
        )
    # PostgreSQL start point: the logical-replication slot + publication (Debezium
    # pgoutput resumes from the slot's LSN); shown only when present (PG source).
    slot_name = getattr(config, "slot_name", None)
    if slot_name:
        source_lines.append(f"slot.name = {slot_name}")
    publication_name = getattr(config, "publication_name", None)
    if publication_name:
        source_lines.append(f"publication.name = {publication_name}")
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
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
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
    ).classes(f"w-full mt-2 {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
        inline_hint(  # type: ignore[attr-defined]
            ui,
            f"Replace every value starting with {CDC_PLACEHOLDER_PREFIX} "
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
            "Note: the CDC start position (the source's replication coordinate — "
            "MySQL binlog/GTID or PostgreSQL WAL LSN) is NOT a parameter here — it is "
            "seeded via the connect-offsets topic after deploy. See the start point "
            "shown above."
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
    confirmed table selection (the manual-seed case where no Full Load ran); and
    finally, for an ADOPTED / out-of-band pipeline (this session ran neither Full
    Load nor Start CDC -- e.g. after a reset + "Attach to <stack>"), to the table
    set reconciled from the live stack's ``TableIncludeList``. Returns an empty
    list only when none are available (config shows "all selected").
    """
    if inventory is None:
        return []
    if watermark is not None and watermark.table_row_counts:
        covered = set(watermark.table_row_counts)
        return [t for t in inventory.tables if t.name in covered]
    selection = migration_state.selection
    if selection is not None and selection.selected_tables:
        return TableSelector().resolve(inventory, selection)
    # Adopted / out-of-band pipeline: the session has no watermark or selection,
    # but the live stack's TableIncludeList tells us which tables are replicating.
    # Each include entry is a table's ``.name`` (build_source_config uses
    # ``[table.name for table in tables]``), so match on ``.name`` -- identical to
    # the watermark path above.
    reconciled = set(getattr(migration_state, "cdc_reconciled_table_names", []) or [])
    if reconciled:
        return [t for t in inventory.tables if t.name in reconciled]
    return []


def _cdc_row_counts_from_watermark(watermark, tables_for_config):
    """Per-table row estimates from a Full Load watermark, scoped to the capture.

    Returns ``{table_name: rows}`` (positive counts only) for the tables actually
    being captured, or ``None`` when the watermark carries no counts. These are the
    scan-free ``information_schema`` estimates captured at snapshot time -- exactly
    the RELATIVE size signal the partition planner needs. Pure.
    """
    counts = getattr(watermark, "table_row_counts", None) if watermark else None
    if not counts:
        return None
    names = {t.name for t in tables_for_config}
    scoped = {n: int(c) for n, c in counts.items() if n in names and c}
    return scoped or None


def _estimate_cdc_table_rows(session, table_names):
    """BLOCKING, read-only scan-free per-table row estimates from the source.

    Runs on a worker thread (caller uses ``run.io_bound``). Used to size CDC topic
    partitions proportionally to table size when no Full Load watermark is present
    (e.g. CDC infra deployed early). Best-effort: returns ``None`` when the source
    cannot be read (no connection/password after a restore) or on any error, so the
    deploy simply falls back to the uniform partition default. Mirrors the
    information_schema estimate the migration-status view already uses.
    """
    if not table_names:
        return None
    try:
        from dsql_migrator.core.watermark import estimate_source_rows
        from dsql_migrator.ui.connect import make_source_engine_factory

        from dsql_migrator.core.source_dialect import dialect_for

        source_config = getattr(session, "source_config", None)
        has_password = getattr(session, "source_password", None) is not None
        if source_config is None or not session.has_source() or not has_password:
            return None
        # Source-engine dialect (PG uses pg_class.reltuples, not MySQL information_schema)
        # so a PostgreSQL source's per-table sizing estimate does not raise + fall back to
        # the uniform partition default. Mirrors _full_load_engine's capture path.
        dialect = dialect_for(source_config.source_type)
        engine = make_source_engine_factory(session.source_password)(source_config)
        with engine.connect() as connection:
            estimates = estimate_source_rows(connection, list(table_names), dialect)
        scoped = {n: int(c) for n, c in estimates.items() if c}
        return scoped or None
    except Exception:  # noqa: BLE001 - optional sizing signal; uniform fallback
        return None


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
    session: object = None,
    source_type: SourceType = SourceType.MYSQL,
    # A watermark that HAS a GTID set but no binlog file:position -- present, yet unable
    # to seed the CDC start offset. Distinguished from "no watermark" so the card can name
    # the actual cause and its fix (the REPLICATION CLIENT grant) instead of implying the
    # Full Load never ran.
    wm_gtid_only: bool = False,
    # This stack streamed before, so its resume offset is already committed and there is
    # no start point left to choose -- the card reports the resume instead of demanding a
    # coordinate it does not need.
    resumes_from_offset: bool = False,
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

    For a **PostgreSQL** source the choice is Automatic (gapless from the replication
    slot created at Full Load) vs Manual (re-snapshot from scratch, ``snapshot.mode=
    initial``). Debezium PG resumes only from the slot and cannot be given an arbitrary
    WAL LSN, so Manual is a re-snapshot -- there is no coordinate to enter and no
    GTID/binlog inputs are shown.
    """
    is_pg = source_type is SourceType.POSTGRES
    # A PostgreSQL Manual choice (re-snapshot) is a resolved start with no coordinate, so
    # it is "ready" even though effective_resume is None.
    ready = resumes_from_offset or effective_resume is not None or (is_pg and mode == "manual")
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon(  # type: ignore[attr-defined]
                "play_circle",
                color="primary" if ready else "grey",
            ).classes("text-xl")
            ui.label("CDC start point").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            # "What is this?" moved to a hover ⓘ instead of a standing paragraph.
            ui.icon("info").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help"
            ).tooltip(
                "Where change streaming begins. Automatic resumes exactly where the "
                "Full Load snapshot ended (no gap, no overlap)."
            )
            ui.space()  # type: ignore[attr-defined]
            if locked:
                # The locked reason + how to change it rides on the badge tooltip, so
                # no separate standing "CDC has started — locked…" line is needed.
                ui.badge("Locked", color="grey").props("outline").tooltip(  # type: ignore[attr-defined]
                    "CDC has started — the start point is locked. To change it, "
                    "stop CDC first."
                )
            elif ready:
                ui.badge("Ready", color="positive").props("outline")  # type: ignore[attr-defined]
            else:
                ui.badge("Action needed", color="warning").props("outline")  # type: ignore[attr-defined]

        # Resuming: there is no choice to present. The position lives in the connector's
        # offsets topic, not in anything this card can set, so offering Automatic/Manual
        # here would imply the operator must pick one -- and "Automatic — needs a Full
        # Load watermark (unavailable)" would be actively wrong, since no watermark is
        # needed. State the fact and stop.
        if resumes_from_offset and not locked:
            render_notice(
                ui,
                tone="success",
                icon="restart_alt",
                header="Resuming from the last streamed position",
                body=(
                    "This pipeline has streamed before, so its resume position is already "
                    "committed on MSK (stopping CDC deleted the connectors, not the "
                    "position). Streaming continues from there — no start point to "
                    "choose, no Full Load watermark required, and nothing re-applied."
                ),
            )
            return

        if is_pg:
            # PostgreSQL: Automatic = gapless from the slot; Manual = re-snapshot (initial).
            auto_label = (
                "Automatic — gapless from the replication slot (recommended)"
                if wm_usable
                else "Automatic — needs a Full Load slot (unavailable)"
            )
            manual_label = "Manual — re-snapshot from scratch (initial)"
        else:
            if wm_usable:
                auto_label = "Automatic — gapless from Full Load (recommended)"
            elif wm_gtid_only:
                # Do NOT say "needs a Full Load watermark": there IS one. It simply lacks the
                # binlog file:position the offset seed is keyed on, so the honest label names
                # what is missing rather than implying the load never ran.
                auto_label = (
                    "Automatic — unavailable (the Full Load watermark has no binlog "
                    "position)"
                )
            else:
                auto_label = "Automatic — needs a Full Load watermark (unavailable)"
            manual_label = "Manual — enter a GTID or binlog position"

        def _on_mode(value: str) -> None:
            migration_state.set_cdc_start_mode(value)
            refresh()

        # AWS-console-style radio choice. Both options stay SELECTABLE even when no usable
        # watermark exists -- the steering is the label ("...(unavailable)") plus the notice
        # below, not a disabled option. (A previous version of this comment claimed the
        # option was disabled; no such code existed, and a Quasar radio built from a plain
        # dict has no per-option disable. Automatic also stays PRE-selected, since the start
        # mode defaults to "auto".) Picking Automatic without a provisioned slot is not a
        # dead end and not a silent gap: the effective snapshot mode is then `initial`
        # anyway (pg_snapshot_mode requires the slot RECORDED on the watermark), so the
        # connector re-snapshots and creates its own publication.
        # When CDC has started the whole choice is disabled (read-only).
        radio = ui.radio(  # type: ignore[attr-defined]
            {"auto": auto_label, "manual": manual_label},
            value=mode,
            on_change=lambda e: _on_mode(e.value),
        ).props("inline=false")
        if locked:
            # Read-only AND clearly greyed: a bare Quasar `disable` dims too subtly
            # to read as "locked", so mute the whole choice (opacity + not-allowed
            # cursor) to match the "Locked" badge.
            radio.props("disable").classes(
                "opacity-50 pointer-events-none cursor-not-allowed"
            )
        if not wm_usable and mode == "auto" and not locked:
            if is_pg:
                # PG has no offset seeder: Automatic needs the replication slot the tool
                # creates at the Full Load consistency point. Absent one, steer to Manual
                # (re-snapshot) rather than a dead end.
                render_notice(
                    ui,
                    tone="warning",
                    header="No replication slot from a Full Load in this session",
                    body=(
                        "Automatic resumes from the logical replication slot the tool "
                        "creates at the Full Load consistency point, and none is recorded "
                        "here. Run a Full Load with CDC enabled for a gapless handoff, or "
                        "choose Manual to re-snapshot every table from scratch."
                    ),
                )
            elif wm_gtid_only:
                # Name the cause AND the fix. This is the one case where the operator can
                # get a real gapless handoff by changing something on the source, so a
                # generic "no watermark" message would send them to Manual with a GTID
                # that cannot seed the offset either -- still not gapless.
                render_notice(
                    ui,
                    tone="warning",
                    header="This Full Load's watermark cannot give a gapless start",
                    body=(
                        "The watermark recorded a GTID set but no binlog "
                        "file:position, and the CDC start offset is keyed on that "
                        "position — so streaming would begin from the source's CURRENT "
                        "binlog and skip every change made during the Full Load. "
                        "SHOW MASTER STATUS is what supplies it; on RDS/Aurora it needs "
                        "the REPLICATION CLIENT grant. Grant it to the source user and "
                        "re-run the Full Load for a gapless handoff, or enter a binlog "
                        "position manually if you know the coordinate the load started "
                        "from."
                    ),
                )
            else:
                # Steer to manual: auto is not usable here.
                inline_hint(  # type: ignore[attr-defined]
                    ui,
                    "No usable Full Load watermark in this session "
                    "(run a Full Load first, or choose Manual).",
                    tone="warning",
                )

        if mode == "manual":
            if is_pg:
                _render_cdc_pg_manual_explanation(ui)
            else:
                _render_cdc_manual_inputs(
                    ui, migration_state, refresh, locked=locked, session=session,
                )
        elif wm_usable and wm_resume is not None:
            if is_pg:
                _render_cdc_pg_start_summary(ui, wm_resume.wal_lsn)
            else:
                _render_cdc_start_summary(
                    ui, wm_resume.gtid_executed, wm_resume.binlog_file,
                    wm_resume.binlog_position,
                )

        # Resolved start-point confirmation (the single source of truth).
        if is_pg:
            # PG has no MySQL-style coordinate: confirm the resolved mode instead.
            pg_confirm = None
            if mode == "manual":
                pg_confirm = "Start point set — re-snapshot every table (snapshot.mode=initial)"
            elif effective_resume is not None:
                lsn = effective_resume.wal_lsn or "(recorded)"
                pg_confirm = f"Start point set — gapless from the replication slot (WAL LSN {lsn})"
            if pg_confirm is not None:
                with ui.row().classes("items-center gap-2 no-wrap mt-1"):  # type: ignore[attr-defined]
                    ui.icon("check_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
                    ui.label(pg_confirm).classes("text-xs text-gray-700 font-mono")  # type: ignore[attr-defined]
        elif effective_resume is not None:
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

def _render_cdc_pg_manual_explanation(ui) -> None:
    """Explain the PostgreSQL Manual start choice (re-snapshot), shown instead of the
    MySQL GTID/binlog inputs.

    Debezium PostgreSQL resumes CDC only from the replication slot's committed position
    and cannot be told to start from an arbitrary WAL LSN, so there is no coordinate to
    enter. Manual means: skip the gapless slot and re-snapshot every selected table from
    scratch (``snapshot.mode=initial``) before streaming.
    """
    render_notice(
        ui,
        tone="info",
        icon="restart_alt",
        header="Manual re-snapshots every table (snapshot.mode=initial)",
        body=(
            "PostgreSQL resumes CDC from the replication slot created at Full Load — there "
            "is no way to start streaming from a specific WAL LSN, so there is nothing to "
            "enter here. Manual takes a fresh initial snapshot of every selected table, "
            "then streams. Use it when no Full Load slot is available, or when you want a "
            "clean re-copy instead of the gapless handoff."
        ),
    )


def _render_cdc_pg_start_summary(ui, wal_lsn) -> None:
    """Show the resolved WAL LSN for the PostgreSQL Automatic (gapless slot) choice."""
    with ui.row().classes("items-center gap-1 no-wrap mt-1"):  # type: ignore[attr-defined]
        ui.label("WAL LSN:").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
        ui.label(str(wal_lsn or "(none)")).classes("text-xs font-mono")  # type: ignore[attr-defined]


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

def _render_cdc_manual_inputs(
    ui, migration_state, refresh, *, locked: bool = False, session: object = None,
) -> None:
    """Render the Manual start-position inputs (GTID / binlog file:pos) + Apply.

    Shown only when the Manual radio is selected. Advisory validation: an
    unrecognized-but-valid GTID must not be rejected, so a bad-looking value shows
    an orange hint but is still stored; MSK Connect validates at connector start.
    ``locked`` (CDC already started) renders the inputs + button read-only.

    When a ``session`` with a live source connection is available, a "Fetch from
    source" button queries ``SHOW MASTER STATUS`` and populates the fields
    automatically — no manual copy-paste needed.
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

    async def _fetch_from_source() -> None:
        """Query the source binlog status and fill the input fields."""
        from nicegui import run
        from dsql_migrator.ui.connect import make_source_engine_factory
        from dsql_migrator.core.watermark import read_binlog_status_row

        source_config = getattr(session, "source_config", None)
        source_password = getattr(session, "source_password", None)
        if source_config is None or source_password is None:
            ui.notify(  # type: ignore[attr-defined]
                "Source connection not available — connect to the source first.",
                type="warning", position="top",
            )
            return

        fetch_btn.disable()
        fetch_btn.set_text("Fetching…")

        def _do_fetch():
            engine_factory = make_source_engine_factory(source_password)
            engine = engine_factory(source_config)
            with engine.connect() as conn:
                row = read_binlog_status_row(conn)
            engine.dispose()
            return row

        try:
            row = await run.io_bound(_do_fetch)
        except Exception as exc:  # noqa: BLE001
            ui.notify(  # type: ignore[attr-defined]
                f"Failed to fetch: {exc}",
                type="negative", position="top",
            )
            fetch_btn.set_text("Fetch current position")
            fetch_btn.enable()
            return

        fetch_btn.set_text("Fetch current position")
        fetch_btn.enable()

        if not row:
            ui.notify(  # type: ignore[attr-defined]
                "No binary log position returned — binary logging may be "
                "disabled.",
                type="warning", position="top",
            )
            return

        gtid_val = row.get("Executed_Gtid_Set") or ""
        binlog_file_val = row.get("File") or ""
        binlog_pos_val = row.get("Position")
        binlog_coord = (
            f"{binlog_file_val}:{binlog_pos_val}"
            if binlog_file_val and binlog_pos_val is not None
            else ""
        )

        gtid_input.set_value(gtid_val)
        binlog_input.set_value(binlog_coord)
        ui.notify(  # type: ignore[attr-defined]
            "Fetched current position from source — click 'Use this start point' "
            "to confirm.",
            type="positive", position="top",
        )

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
        # CDC started: the coordinate is already committed -- show it read-only AND
        # visibly greyed so it reads as locked (matches the radio + "Locked" badge).
        # The committed coordinate stays readable in the "Start point set — …" line.
        for _inp in (gtid_input, binlog_input):
            _inp.props("readonly").classes("opacity-50 pointer-events-none")

    if gtid_error["msg"]:
        inline_hint(ui, gtid_error["msg"], tone="warning")  # type: ignore[attr-defined]

    # "Use this start point" (not "Apply"/"Start") — this only records the
    # coordinate into the connector config; it does NOT begin streaming. Actual
    # streaming starts when the config is deployed to the cdc-stack. Disabled once
    # CDC has started (the start point is already seeded and cannot change).
    with ui.row().classes("items-center gap-2 mt-1"):  # type: ignore[attr-defined]
        # Fetch from source: auto-populates from SHOW MASTER STATUS.
        can_fetch = (
            session is not None
            and getattr(session, "source_config", None) is not None
            and getattr(session, "source_password", None) is not None
        )
        if can_fetch and not locked:
            fetch_btn = ui.button(  # type: ignore[attr-defined]
                "Fetch current position",
                on_click=_fetch_from_source,
                icon="download",
            ).props("size=sm flat color=primary")
        else:
            fetch_btn = None  # noqa: F841 — ref needed for the async handler above

        apply_btn = ui.button("Use this start point", on_click=_apply).props(  # type: ignore[attr-defined]
            "size=sm color=primary"
        )
        if locked:
            apply_btn.props("disable")

def _render_cdc_runs_on_banner(ui, phase=None) -> None:
    """Orientation banner: where CDC runs (source -> MSK -> DSQL sink).

    Shown only BEFORE the cdc-stack is deployed (phase ``"absent"``); once it exists
    the architecture is self-evident, so the banner is hidden to reduce noise. Also
    hidden for an unknown (``None``) phase, so it never flashes on a reconnect to an
    already-running pipeline (before the phase probe resolves).
    """
    if phase != "absent":
        return
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


def _cdc_start_detail(
    stack_name: str,
    watermark: object,
    *,
    mode: Optional[str],
    source_type: object,
    force_snapshot: bool = False,
    exclusions: Optional[Mapping[str, object]] = None,
) -> str:
    """The audit detail for a CDC start: where the stream resumes FROM, and how.

    States the resume coordinate through the same precedence the Full Load watermark line
    uses (:func:`~dsql_migrator.ui.data_migration._full_load_engine.watermark_coordinate`),
    so the two lines in one log agree rather than spelling it two ways. With no watermark
    the stream starts from the source's CURRENT position, which means anything written
    between an earlier snapshot and now is NOT replicated -- the one case a reader must be
    able to spot, so it is said explicitly rather than implied by an absent coordinate.

    Log positions, a mode, and column COUNTS only -- never a row value (Property 7).
    """
    from dsql_migrator.ui.data_migration._full_load_engine import watermark_coordinate

    parts = [f"stack {stack_name}"]
    if watermark is not None:
        parts.append(
            f"start point {watermark_coordinate(watermark, source_type)} "
            "(gapless from the Full Load watermark)"
        )
    else:
        parts.append(
            "start point: the source's CURRENT position — there is no Full Load "
            "watermark, so any change written before this moment is NOT replicated"
        )
    parts.append(f"mode {mode or 'auto'}")
    if force_snapshot:
        parts.append("a FULL initial snapshot is forced for this start")
    excluded = sum(len(cols or ()) for cols in (exclusions or {}).values())
    if excluded:
        parts.append(f"{excluded} column(s) excluded from replication (oversized LOB)")
    return "; ".join(parts)


def cdc_unstable_message(
    status: Optional[str], *, has_seeder_lambda: Optional[bool] = None
) -> tuple[str, str, str, str]:
    """Message for the ``unstable`` CDC card, keyed on the raw stack status.

    Returns ``(badge_text, tone, header, body)``. Three cases:

    - **DELETE_IN_PROGRESS** — a live teardown: the infra is being removed, so this
      is REASSURING (info), names the ~15-25 min native ENI-detach wait, and says
      billing stops on completion. Badge ``"Deleting…"``.
    - **any other ``*_IN_PROGRESS``** — some other live operation: wait for it
      (warning). Badge ``"Busy"``.
    - **terminal stuck** (ROLLBACK_*/DELETE_FAILED/…) — will not clear on its own;
      delete then redeploy (warning). Badge ``"Busy"``.

    Pure / NiceGUI-agnostic so the messaging is unit-testable.
    """
    raw = (status or "busy")
    upper = raw.upper()
    if upper == "DELETE_IN_PROGRESS":
        return (
            "Deleting…",
            "info",
            "CDC infrastructure is being deleted",
            f"The cdc-stack is being torn down (this takes "
            f"{cdc_teardown_estimate(has_seeder_lambda=has_seeder_lambda)} — "
            f"{cdc_teardown_reason(has_seeder_lambda=has_seeder_lambda)}). MSK / NAT "
            "billing stops once it completes. This view refreshes automatically; no "
            "action is needed.",
        )
    if _is_inflight_stack_status(raw):
        return (
            "Busy",
            "warning",
            "cdc-stack is busy",
            f"The cdc-stack is '{raw}'. Wait for the current operation to finish "
            "(progress refreshes), then the next action appears.",
        )
    return (
        "Busy",
        "warning",
        "cdc-stack needs cleanup",
        f"The cdc-stack is stuck in '{raw}' from a failed operation — it will not "
        "clear on its own. Use Delete CDC infrastructure below, then deploy again.",
    )

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


def cdc_state_is_undetermined(migration_state) -> bool:
    """True when the CDC state is UNKNOWN rather than known-absent.

    The lifecycle card treats a ``None`` phase as "absent / not yet probed" and offers
    the deploy form. Those are not the same thing. The AWS phase probe needs a target
    region (``session.target_config``) and returns silently without one -- and a
    restored session deliberately does not trust its old connections, so after an app
    restart the probe has not run and the phase is ``None`` while a real pipeline may be
    streaming. The card then showed a fresh-deploy form for infrastructure that already
    exists, with nothing on screen saying the state was simply unknown. (Observed: the
    app was restarted during Start CDC; both connectors reached RUNNING on AWS, but the
    CDC pipeline card came back blank.)

    ``cdc_stack_phase_checked`` is the discriminator: the probe sets it whenever it
    reports, including when it reports "absent". So an unset flag means "we have not
    looked", which is what this reports -- distinct from "we looked and there is
    nothing".

    Pure (reads already-populated state; no AWS I/O), so it is safe during render.
    """
    if getattr(migration_state, "cdc_stack_phase_checked", False):
        return False  # the probe reported -- absent is then a real answer
    return getattr(migration_state, "cdc_stack_phase", None) is None


def cdc_redeploy_needs_confirmation(migration_state) -> bool:
    """True when the deploy form should wait behind an explicit "redeploy?" prompt.

    Only after a teardown IN THIS SESSION (``cdc_action_kind == "delete"``, which
    outlives the finished job) and only until the operator says yes. A CDC delete takes
    ~20 min and removes a billable MSK cluster; the moment it lands, the answer the
    operator wants is "it's gone", not a ~20-line BYO-VPC form implying the tool is
    about to rebuild what they just removed.

    A first-ever deploy is deliberately NOT gated: there the form IS the next step, and
    an extra click to reach it would be pure friction.

    Pure (reads already-populated state); safe during render.
    """
    if getattr(migration_state, "cdc_redeploy_confirmed", False):
        return False
    return getattr(migration_state, "cdc_action_kind", None) == "delete"


def _render_cdc_redeploy_prompt(ui, migration_state, refresh) -> None:
    """Confirm the teardown, then offer redeploy as a choice rather than a form.

    Leads with the outcome the operator was waiting for (the infrastructure is gone and
    is no longer billing), and makes rebuilding an explicit opt-in that states the real
    cost of saying yes (~5 min, billable). Answering yes latches
    ``cdc_redeploy_confirmed`` so the form stays open across refreshes.
    """
    render_notice(
        ui,
        tone="success",
        icon="task_alt",
        header="CDC infrastructure deleted",
        body=(
            "The cdc-stack and its MSK Serverless cluster are gone, so they no longer "
            "incur charges. Nothing else in this migration was affected — the target "
            "data and the Full Load results are untouched."
        ),
    )

    def _confirm() -> None:
        migration_state.set_cdc_redeploy_confirmed(True)
        if callable(refresh):
            refresh()

    with ui.row().classes("items-center gap-2 no-wrap flex-wrap"):  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Deploy CDC infrastructure again?"
        ).classes("text-xs text-gray-600")
        ui.button(  # type: ignore[attr-defined]
            "Redeploy CDC infrastructure", on_click=_confirm
        ).props("outline size=sm").classes("normal-case")
    ui.label(  # type: ignore[attr-defined]
        "Rebuilding creates a new MSK Serverless cluster and takes ~5 minutes; "
        "it is billable. Leave this alone if you are done with CDC."
    ).classes("text-xs text-gray-500")


def _render_cdc_state_unknown_notice(
    ui, migration_state=None, refresh=None
) -> None:
    """Say the CDC state is unknown, WHY, and how to recover it.

    Without this the operator gets a deploy form for a pipeline that may already be
    running -- there is no way to tell from the screen that the tool simply has not
    looked yet. Two distinct causes, named so the user acts on the right one:

    * **probe FAILED** (``cdc_probe_error`` set) -- the read-only ``describe_stacks``
      check threw (bad/expired creds, missing ``cloudformation:DescribeStacks``, wrong
      region, throttle). Re-verifying the target will NOT help; the AWS access has to be
      fixed. Show the real error so it is actionable.
    * **probe NOT RUN** (no error) -- a restored session's connections are untrusted
      until re-verified, so the probe has not run yet. Re-verify the target on Connect.

    Either way a "Re-check CDC state" button re-runs the probe immediately (it clears
    the discovery throttle + refreshes), so recovery does not require navigating away.
    """
    probe_error = getattr(migration_state, "cdc_probe_error", None)
    if probe_error:
        body = (
            "The read-only AWS check that reads the live CDC state FAILED, so the tool "
            "cannot tell whether a pipeline exists. Re-verifying the target will not "
            f"help — fix the AWS access first. Reported error: {probe_error}. Check the "
            "credentials/permissions (cloudformation:DescribeStacks) and the target "
            "region, then re-check. Any pipeline you already started keeps streaming."
        )
    else:
        body = (
            "This session was restored, so its connections are not trusted until "
            "re-verified and the read-only AWS check that reads the live CDC state has "
            "not run. Any pipeline you already started is unaffected and keeps "
            "streaming. Re-verify the target connection on the Connect step to recover "
            "the real state here — do not start CDC again until it shows."
        )
    render_notice(ui, tone="warning", header="CDC state not determined yet", body=body)

    # A direct re-check so the user is not stuck: clear the discovery throttle so the
    # next render re-probes immediately, then refresh. (The discovery timer is armed on
    # render and gated by ``_cdc_discovery_monotonic``.)
    if migration_state is not None and callable(refresh):
        def _recheck(_e=None) -> None:
            try:
                migration_state._cdc_discovery_monotonic = None
            except Exception:  # noqa: BLE001 - best effort
                pass
            refresh()

        ui.button(  # type: ignore[attr-defined]
            "Re-check CDC state", icon="refresh", on_click=_recheck
        ).props("outline size=sm color=primary").classes("mt-2")


def _render_cdc_start_action(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Render the CDC lifecycle card: Deploy infra → Start → Stop → Delete.

    The UI owns the whole cdc-stack lifecycle as CloudFormation operations. The
    card branches on the cached stack phase (probed by ``_ensure_cdc_controller``
    / ``_probe_cdc_stack_phase``):

    * **absent** — no stack yet → show the BYO-VPC infrastructure form + a
      "Deploy CDC infrastructure" button (create_stack, ~5 min).
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
            # For the unstable phase the badge/notice depend on the raw stack status
            # (a live DELETE reads as "Deleting…", not a vague "Busy"). Derive both
            # from the single pure helper so badge and notice never diverge.
            _unstable_badge, _unstable_tone, _unstable_header, _unstable_body = (
                cdc_unstable_message(
                    getattr(migration_state, "cdc_stack_phase_status", None)
                )
            )
            _unstable_badge_color = "primary" if _unstable_tone == "info" else "warning"
            # A teardown in flight outranks the connector phase: CloudFormation has not
            # removed the connectors yet, so discovery still reports "running" and the
            # badge read a green "Streaming" beside a body saying "Deleting
            # infrastructure". See cdc_teardown_badge.
            _teardown = cdc_teardown_badge(migration_state, job_manager)
            badge_text, badge_color, icon_color = (
                (_teardown[0], _teardown[1], _teardown[1]) if _teardown is not None
                else ("Streaming", "positive", "positive") if phase == "running"
                else ("Provisioning…", "primary", "primary") if phase == "provisioning"
                else ("Working…", "primary", "primary") if deploying
                else ("Incomplete", "warning", "warning") if phase == "partial"
                else ("Infra ready", "primary", "primary") if phase == "infra"
                else (_unstable_badge, _unstable_badge_color, _unstable_badge_color)
                if phase == "unstable"
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
                    "~5 minutes). "
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
            render_notice(
                ui, tone=_unstable_tone, header=_unstable_header, body=_unstable_body
            )
            # A live operation (DELETE / other *_IN_PROGRESS) clears on its own, so
            # keep polling to flip the card when it settles; a terminal stuck state
            # will not clear, so no auto-poll (the user must act).
            if _is_inflight_stack_status(
                getattr(migration_state, "cdc_stack_phase_status", None)
            ):
                ui.timer(_CDC_POLL_INTERVAL_SECONDS, refresh, once=True)  # type: ignore[attr-defined]
        elif cdc_state_is_undetermined(migration_state):
            # NOT the same as absent: the probe has not run (a restored session's
            # connections are untrusted until re-verified), so offering a deploy form
            # here would invite a duplicate, billable MSK cluster for a pipeline that
            # may already be streaming.
            _render_cdc_state_unknown_notice(ui, migration_state, refresh)
        else:  # absent -- the probe reported, and there really is nothing
            # Account-scoped discovery: if CDC infra already exists under a name this
            # (reset) session does not target, offer to ADOPT it rather than deploy a
            # duplicate (a second, costly MSK cluster). Adoption re-reads the live
            # state from AWS, so a running pipeline lands on its monitoring view.
            other_stacks = getattr(migration_state, "cdc_other_stacks", []) or []
            if other_stacks:
                _render_cdc_adopt_or_deploy_choice(
                    ui, migration_state, job_manager, refresh, other_stacks,
                    inventory=inventory, session=session,
                )
            elif cdc_redeploy_needs_confirmation(migration_state):
                # Straight after a teardown, confirm the deletion and ASK before
                # showing the deploy form again (see the predicate's docstring).
                _render_cdc_redeploy_prompt(ui, migration_state, refresh)
            else:
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
            ).classes(f"w-full mt-2 text-red-700 {EXPANSION_PANEL_CLASSES}"):
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
    watermark = _cdc_watermark(job)
    override = migration_state.cdc_start_override()
    wm_resume = (
        CdcResumePoint.from_watermark(watermark) if watermark is not None else None
    )
    # A RESTART needs no start point. When this stack has streamed before, its resume
    # offset is already committed to the source connector's offsets topic -- which is
    # pinned to a fixed name and survives a Stop (a Stop only deletes the connectors) --
    # and the seeder skips re-seeding an offset that is at/past the watermark. So
    # streaming resumes exactly where it stopped.
    #
    # Gating `ready` on a watermark alone was wrong here, and wrong in the case that
    # matters most: the watermark is read off the Full Load JOB record, so after an app
    # restart (job record gone) or in a CDC-only session there is none -- and Start CDC
    # went disabled with "Set the CDC start point above first" even though the pipeline
    # could resume perfectly. The operator was pushed toward re-entering coordinates by
    # hand, or worse, re-running the Full Load, to recover something the connector had
    # not lost.
    resumes_from_offset = bool(
        getattr(migration_state, "cdc_has_committed_offset", False)
    )
    # A PostgreSQL CDC-only migration (no Full Load, so no watermark and no gapless
    # slot) is steered to Manual = fresh re-snapshot: that mode needs NO prior start
    # point, so it is a valid ready state on its own. The start-point card already
    # treats it as "Ready" (its gate includes `is_pg and mode == "manual"`), so the
    # button MUST mirror that clause -- otherwise the card says Ready while the button
    # stays disabled with "Set the CDC start point above first", and PG CDC-only can
    # never be started.
    is_pg = _cdc_source_type(session) is SourceType.POSTGRES
    mode = migration_state.cdc_start_mode()
    ready = (
        resumes_from_offset
        or (override is not None and override.has_coordinates())
        # The AUTOMATIC gapless handoff resumes from the Full Load watermark. MySQL seeds
        # the connect-offset from a binlog file:position (can_seed_offset); PostgreSQL
        # resumes from the logical-replication slot's WAL LSN (can_resume_from_lsn). Accept
        # EITHER so a PostgreSQL Full Load watermark (a WAL LSN, no binlog/GTID) enables
        # Start CDC. Gate MySQL on can_seed_offset() -- NOT has_coordinates() -- because a
        # GTID-only watermark (SHOW MASTER STATUS needs REPLICATION CLIENT, commonly
        # restricted on RDS, while @@GLOBAL.gtid_executed is a plain read) has no file:pos
        # to seed: auto mode would then silently resume from the CURRENT binlog and lose
        # every change made during the load. Such a watermark must steer the operator to
        # Manual (or grant REPLICATION CLIENT), so it must NOT enable auto Start.
        or (
            wm_resume is not None
            and (wm_resume.can_seed_offset() or wm_resume.can_resume_from_lsn())
        )
        or (is_pg and mode == "manual")
    )
    # A restart is a materially different operation from a first start -- it resumes an
    # existing position rather than establishing one -- so it must not be described with
    # the first-start copy. Saying only "begins streaming" left the operator to guess
    # whether stopping had cost them their place (and the answer, "no", is the one thing
    # they need before pressing this).
    if resumes_from_offset:
        render_notice(
            ui,
            tone="info",
            icon="restart_alt",
            header="Ready to resume CDC",
            body=(
                "This pipeline streamed before and its resume position is still recorded "
                "on MSK — stopping CDC removed only the connectors, not that position. "
                "Start CDC re-creates them and continues from exactly where streaming "
                "stopped: no gap, no re-load, and no need to re-enter a start point. It "
                "takes a few minutes; progress appears below."
            ),
        )
    else:
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

    # State the CONFIRMED table set and the remedy that ACTUALLY works -- do NOT advise
    # "pick all your tables up front". By the time this button renders the set is
    # already frozen: the button only appears for card phase "infra", which requires a
    # probed cdc_stack_phase of "infra", which makes cdc_infra_prep_state() "ready",
    # which is exactly what selection_lock_reason's CDC clause locks the picker on (the
    # CDC-bearing types are the only ones with a "cdc" sub-step, so there is no
    # reachable state where this renders with the picker still open). Telling the
    # operator to choose everything up front was advice they could not act on -- the
    # checkboxes were disabled while the tip pointed at them.
    #
    # The remedy depends on WHICH lock applies, and the two are not interchangeable:
    #   * Full Load already ran -> its clause wins and is NOT released by deleting the
    #     CDC stack (the export ran against this set; that fact is permanent). Only
    #     Start over clears it. Saying "delete the CDC infrastructure to re-scope" here
    #     would send the operator through a ~45 min teardown that leaves the picker
    #     exactly as locked as before.
    #   * CDC-only (no Full Load) -> the CDC clause is the only one, and deleting the
    #     infrastructure genuinely does release it.
    # So mirror selection_lock_reason's own precedence rather than assuming one exit.
    full_load_ran = _full_load_committed(job, migration_state)
    started_before = bool(getattr(migration_state, "cdc_connector_names", None))
    if started_before:
        # A REAL caution: repeated start/stop has already begun consuming MSK's
        # non-reclaimed partition capacity, and enough cycles wedge the cluster into a
        # delete-and-redeploy. Worth its own amber box.
        render_notice(
            ui,
            tone="warning",
            icon="warning",
            header="Re-starting CDC uses more MSK capacity each time",
            body=(
                f"{selection_line} Each restart re-creates connectors, and MSK's "
                "limited capacity isn't freed up between runs — so repeated "
                "start/stop cycles can eventually require deleting and redeploying "
                "the CDC infrastructure. Check the set above is the one you want "
                "before starting again."
            ),
        )
    else:
        # On the FIRST start this is not a warning at all -- it is the "which tables?"
        # answer plus a fact about why it is fixed. It used to be a second full-width
        # blue box directly under "Ready to start CDC", which gave a normal happy-path
        # state two equal-weight notices and buried the one line the operator actually
        # scans for (WHICH tables will stream) inside a paragraph about MSK partition
        # accounting. So: the table set is plain text (the verifiable fact, kept visible
        # -- it must not become hover-only), and the immutability rationale moves to an
        # info glyph beside it, since it is background the operator needs at most once.
        with ui.row().classes("items-center gap-1.5 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("playlist_add_check", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label(selection_line).classes("text-sm text-gray-700")  # type: ignore[attr-defined]
            ui.icon("info_outline").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help shrink-0"
            ).tooltip(
                "This set is fixed. Each table's Kafka topic partitions were sized when "
                "the infrastructure was created and cannot be changed, and MSK does not "
                "reclaim that capacity.\n\n"
                + (
                    "It matches the Full Load snapshot, which is what makes the handoff "
                    "gapless — streaming a different set means a fresh migration, via "
                    "'Start over' (top right)."
                    if full_load_ran
                    else "To stream a different set, delete the CDC infrastructure below "
                    "and deploy it again for the tables you want."
                )
            )

    # _open_cdc_start_dialog is async (it runs the read-only binlog-retention
    # pre-flight via run.io_bound), so it MUST be awaited -- otherwise the dialog
    # never opens and "Start CDC" appears to do nothing.
    async def _confirm() -> None:
        await _open_cdc_start_dialog(
            ui, migration_state,
            lambda: _start_cdc_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
            job_manager=job_manager,
            inventory=inventory,
            refresh_after_drop=refresh,
        )

    # Explicit CDC-prerequisite gate, independent of the sub-step ordering: a
    # source whose binary log is not ROW/FULL can never be streamed, and creating
    # connectors against it burns ~26 min of billable create before failing with an
    # undiagnosed error. Checked here as well as before the infra deploy, because a
    # session can reach Start CDC on already-deployed (or adopted) infrastructure
    # without having passed through the deploy action.
    prereq_block = cdc_prerequisite_block_reason(
        migration_state.get_prereq_report(MigrationMode.CDC),
        cdc_checks_already_passed=(
            getattr(migration_state, "prereq_gated_mode", None) is MigrationMode.CDC
        ),
    )
    if prereq_block:
        render_notice(
            ui,
            tone="warning",
            header=cdc_prerequisite_block_header(
                migration_state.get_prereq_report(MigrationMode.CDC),
                cdc_checks_already_passed=(
                    getattr(migration_state, "prereq_gated_mode", None)
                    is MigrationMode.CDC
                ),
            ),
            body=prereq_block,
        )

    start_btn = ui.button(  # type: ignore[attr-defined]
        "Start CDC", on_click=_confirm, icon="play_arrow"
    ).props("color=primary")
    if prereq_block:
        start_btn.props("disable")
        start_btn.tooltip(prereq_block)
    elif not ready:
        start_btn.props("disable")
        ui.label(  # type: ignore[attr-defined]
            "Set the CDC start point above first."
        ).classes("text-xs text-gray-500")

def _render_cdc_running_actions(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """The 'Stop CDC' action shown while connectors are running."""
    # No always-on blurb here: that the pipeline is streaming is already clear from
    # the live status right below, and the "Stop removes only the connectors, infra
    # is kept" impact is spelled out in the confirmation dialog (and the button
    # tooltip). Keeping it as static text just clutters the running view.
    def _confirm() -> None:
        _open_cdc_stop_dialog(
            ui, migration_state,
            lambda: _start_cdc_stop(ui, migration_state, job_manager, refresh, session=session),
        )

    ui.button(  # type: ignore[attr-defined]
        "Stop CDC", on_click=_confirm, icon="stop_circle"
    ).props("color=amber outline").tooltip(
        "Removes only the connectors — MSK, the VPC wiring and the plugins are "
        "kept, so you can Start CDC again quickly."
    )

def _cdc_prep_ready_badge(migration_state) -> tuple[str, str]:
    """Badge for the prep section's ``ready`` state, honouring the raw stack status.

    ``ready`` folds every non-stable status, so a flat ("Ready", "positive") badge
    labelled a deleting or failed stack green. Derives the badge from the same pure
    helper the CDC card uses, so the two surfaces cannot disagree again.
    """
    status = getattr(migration_state, "cdc_stack_phase_status", None)
    if status and not is_stable_stack_status(status):
        badge_text, _tone, _header, _body = cdc_unstable_message(status)
        return (badge_text, "warning")
    return ("Ready", "positive")


def cdc_infra_prep_state(migration_state, job_manager) -> str:
    """Classify the CDC-infrastructure situation for the Prerequisites-step section.

    Returns one of:

    * ``"deploying"`` -- an infra create is in flight (show live progress only).
    * ``"ready"``     -- a cdc-stack already exists (deployed earlier / adopted), so
      there is nothing to deploy here; Start CDC happens on the CDC sub-step.
    * ``"adopt"``     -- CDC infrastructure exists under a name this session does not
      target, so offer to attach instead of paying for a second MSK cluster.
    * ``"deploy"``    -- nothing exists yet: offer the BYO-VPC form + deploy.
    * ``"unknown"``   -- the account probe has not reported yet; render nothing rather
      than briefly showing a deploy form that could duplicate an existing pipeline.

    Pure (reads already-populated state; no AWS I/O) so it is safe during render.
    """
    if cdc_infra_deploy_in_flight(migration_state, job_manager):
        return "deploying"
    phase = getattr(migration_state, "cdc_stack_phase", None)
    if phase in ("infra", "running", "unstable", "provisioning", "partial"):
        return "ready"
    # Gate on the probe having actually reported: ``cdc_other_stacks`` is only
    # meaningful once the account-wide discovery ran. Showing a fresh-deploy form
    # before then risks a duplicate (billable) MSK cluster.
    if not getattr(migration_state, "cdc_stack_phase_checked", False):
        return "unknown"
    if getattr(migration_state, "cdc_other_stacks", None):
        return "adopt"
    return "deploy"


def _render_cdc_infra_prep_section(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """An EXTRA CDC-infrastructure entry point, at the bottom of Prerequisites.

    Not the only way to provision: the CDC step's lifecycle card
    (:func:`_render_cdc_start_action`) already renders the same BYO-VPC deploy form (or
    the adopt choice) whenever the stack is absent. This one exists purely so the
    ~5 min MSK create can be started EARLY and OVERLAP the Full Load, instead of
    waiting until the operator reaches the CDC step after the load. It also needs a real
    table set (the connector's ``TableIncludeList`` and the topic partition plan), and
    Prerequisites is the first point where both hold: running the checks pins and locks
    the confirmed selection, and that sub-step still precedes Full Load. Beside the
    migration-type tiles the picker is typically untouched, so the table set would
    resolve to "none".

    **Rendered only for types that HAVE a Full Load to overlap.** For CDC only the
    caller suppresses it: with no Full Load there is nothing to overlap, so it would be
    a second copy of the CDC step's own form on the same screen. Do not add a call to
    this from the CDC step -- that duplicates a billable deploy form.

    Scoped to the first-deploy affordance only: Start CDC, monitoring, Stop and Delete
    stay on the CDC sub-step, which remains reachable with no infrastructure deployed.
    """
    prep = cdc_infra_prep_state(migration_state, job_manager)
    if prep == "unknown":
        return

    ui.separator()  # type: ignore[attr-defined]
    section_header(
        ui,
        icon="cloud_upload",
        title="CDC streaming infrastructure",
        badge=(
            ("Deploying…", "primary") if prep == "deploying"
            else _cdc_prep_ready_badge(migration_state) if prep == "ready"
            else ("Not deployed", "grey")
        ),
    )

    if prep == "deploying":
        render_notice(
            ui,
            tone="info",
            busy=True,  # ~5 min live operation: spinner + "In progress" badge
            header="Deploying in the background — start your Full Load now",
            body=(
                "Amazon MSK takes ~5 minutes to provision. Nothing is streaming "
                "yet, so this does not hold up the snapshot: continue to Full Load "
                "and let the two run together. You can leave this screen; progress "
                "is kept and shown when you return."
            ),
        )
        _render_cdc_deploy_live(ui, migration_state, job_manager, refresh)
        return

    if prep == "ready":
        stack = getattr(migration_state, "cdc_stack_name", "the cdc-stack")
        # "ready" is a fold: EVERY non-stable CloudFormation status lands here, because
        # only CREATE_COMPLETE / UPDATE_COMPLETE / UPDATE_ROLLBACK_COMPLETE /
        # IMPORT_COMPLETE are in _STABLE_STACK_STATES. So a stack that is being torn down,
        # or parked in ROLLBACK_COMPLETE / CREATE_FAILED / DELETE_FAILED, was announced in
        # a green success box as "already deployed, so there is nothing to provision" --
        # while the CDC card one sub-step below, reading the SAME state, correctly said
        # "CDC infrastructure is being deleted". One screen contradicting itself.
        #
        # The fold itself is right (hiding the deploy affordance is correct: CreateStack
        # against a same-named deleting stack raises AlreadyExistsException), so only the
        # badge / tone / copy change, from the raw status through the same pure helper the
        # CDC card uses. No new action here -- Delete already lives one sub-step below.
        _raw_status = getattr(migration_state, "cdc_stack_phase_status", None)
        if _raw_status and not is_stable_stack_status(_raw_status):
            _badge, _tone, _header, _body = cdc_unstable_message(_raw_status)
            render_notice(ui, tone=_tone, header=_header, body=_body)
            # Re-poll while the status can still change on its own, so the section does
            # not sit on a stale in-flight message until something else refreshes. A
            # terminal status (DELETE_FAILED, ROLLBACK_COMPLETE) needs an operator
            # action, so arming a timer for it would poll forever for nothing.
            if _is_inflight_stack_status(_raw_status) and refresh is not None:
                ui.timer(  # type: ignore[attr-defined]
                    _CDC_POLL_INTERVAL_SECONDS, refresh, once=True
                )
            return
        # "after the Full Load" is false for CDC only -- there is no Full Load in that
        # plan, and the operator's next action is Start CDC on the (now-expanded) CDC
        # step. Naming a step that does not exist reads as a missing prerequisite.
        from dsql_migrator.ui.data_migration._models import MigrationType

        _cdc_only = (
            getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY
        )
        _next_step = (
            "You start streaming with Start CDC on the CDC step below."
            if _cdc_only
            else "You start streaming on the CDC step after the Full Load."
        )
        render_notice(
            ui,
            tone="success",
            header="CDC infrastructure is ready",
            body=(
                f"'{stack}' is already deployed, so there is nothing to provision "
                f"here. {_next_step}"
            ),
        )
        return

    if prep == "adopt":
        _render_cdc_adopt_or_deploy_choice(
            ui,
            migration_state,
            job_manager,
            refresh,
            getattr(migration_state, "cdc_other_stacks", []) or [],
            inventory=inventory,
            session=session,
        )
        return

    render_notice(
        ui,
        tone="info",
        icon="schedule",
        header="Deploy now so it is ready when the Full Load finishes",
        body=(
            "CDC needs Amazon MSK, which takes ~5 minutes to provision and bills "
            "while it exists. Deploying it here lets it run WHILE your Full Load "
            "does, instead of waiting afterwards — the snapshot is unaffected, and "
            "no data streams until you explicitly start CDC. You can also skip this "
            "and deploy later from the CDC step."
        ),
    )
    _render_cdc_infra_deploy_action(
        ui, migration_state, job_manager, refresh,
        inventory=inventory, session=session,
    )


def _render_cdc_adopt_or_deploy_choice(
    ui, migration_state, job_manager, refresh, other_stacks, *,
    inventory=None, session=None,
) -> None:
    """CDC infra exists under a name this (reset) session does not target: offer to
    ADOPT it instead of deploying a duplicate.

    Deploying a second cdc-stack means a second, billable Amazon MSK cluster, so the
    adopt path is primary. Adoption is read/attach-only -- it re-reads the live state
    from AWS (running / provisioning / infra), so a running pipeline lands straight on
    its monitoring view; starting fresh remains the explicit Stop/Delete path, never a
    side effect of adopting. A separate fresh deploy stays reachable but de-emphasized.
    """
    # Failed / rolled-back / deleting stacks are NOT adoptable: their resources are
    # partly gone, so attaching yields a dead session -- and the urgent fact ("a
    # teardown did not finish, MSK/NAT may still be billing") was hidden behind an
    # inviting "Attach to <stack> (DELETE_FAILED)" button.
    attachable, needs_cleanup = split_attachable_stacks(other_stacks)

    if needs_cleanup:
        # A delete that is simply STILL RUNNING is not a failed one. Both are equally
        # un-attachable, so they share the bucket -- but the failure copy told the
        # operator "a previous teardown did not finish" about a teardown progressing
        # normally, and recommended 'Retain resources', which would abandon the very MSK /
        # NAT resources the delete is about to remove (the opposite of stopping the cost).
        _in_flight, _terminal = split_cleanup_by_progress(needs_cleanup)
        if _in_flight:
            _busy = ", ".join(f"{name} ({status})" for name, status in _in_flight)
            _has_seeder = _cdc_stack_has_seeder_lambda(migration_state)
            render_notice(
                ui,
                tone="warning",
                header="CDC infrastructure is still being removed",
                body=(
                    f"{_busy}. This is in progress, not stuck: a cdc-stack delete takes "
                    f"{cdc_teardown_estimate(has_seeder_lambda=_has_seeder)} because "
                    f"{cdc_teardown_reason(has_seeder_lambda=_has_seeder)}. It cannot be "
                    "attached to while it runs, and MSK / NAT billing stops when it "
                    "completes. No action is needed — you can keep working on the earlier "
                    "steps meanwhile."
                ),
            )
        if _terminal:
            stuck = ", ".join(f"{name} ({status})" for name, status in _terminal)
            render_notice(
                ui,
                tone="error",
                header=(
                    "Leftover CDC infrastructure needs cleanup — it may still be billing"
                ),
                body=(
                    f"{stuck}. A previous teardown did not finish, so this stack cannot "
                    "be used or attached to (it is partly deleted) — but its Amazon MSK / "
                    "NAT resources may still be incurring cost. Finish the delete first: "
                    "use 'Delete CDC infrastructure' below, or delete the stack in the "
                    "CloudFormation console (a DELETE_FAILED stack usually needs 'Retain "
                    "resources' on whatever is stuck)."
                ),
            )

    # Split the candidates by whether they actually cover THIS session's tables. Attaching
    # promotes Data Migration to DONE and unlocks Validation, so a pipeline streaming a
    # different table set would report the migration complete while every loaded table
    # received no ongoing changes at all. (The same guard as the plan-level banner; this
    # panel is a separate render path and had none.)
    loaded_tables = list(migration_state.selection.selected_tables)
    tables_by_stack = getattr(migration_state, "cdc_other_stack_tables", None) or {}
    in_scope: "list[tuple[str, str]]" = []
    out_of_scope: "list[tuple[str, str, list[str]]]" = []
    for name, status in attachable:
        missing = cdc_attach_scope_mismatch(
            tables_by_stack.get(name, ()), loaded_tables
        )
        (out_of_scope.append((name, status, missing)) if missing
         else in_scope.append((name, status)))

    for name, _status, missing in out_of_scope:
        listed = ", ".join(missing[:6]) + (
            f" +{len(missing) - 6} more" if len(missing) > 6 else ""
        )
        noun = "table" if len(missing) == 1 else "tables"
        render_notice(
            ui,
            tone="warning",
            header=f"{name} streams a different set of tables — not safe to attach",
            body=(
                f"It does not replicate {len(missing)} {noun} this session loaded "
                f"({listed}), so attaching would mark the migration complete while those "
                "tables received no ongoing changes. Deploy a pipeline for this table set "
                "below. That existing stack keeps billing meanwhile — delete it below if "
                "it is no longer needed."
            ),
        )

    if in_scope:
        listed = ", ".join(f"{name} ({status})" for name, status in in_scope)
        render_notice(
            ui,
            tone="warning",
            header="Existing CDC infrastructure found",
            body=(
                f"This account already has CDC infrastructure: {listed}. Deploying a "
                "new one would create a SEPARATE Amazon MSK cluster (ongoing cost). "
                "Attach to the existing pipeline instead — the tool re-reads its live "
                "state from AWS (if its connectors are already running, you go "
                "straight to monitoring)."
            ),
        )
        with ui.row().classes("items-center gap-2 flex-wrap w-full"):  # type: ignore[attr-defined]
            for name, _status in in_scope:
                def _adopt(_name=name) -> None:
                    if migration_state.adopt_cdc_stack(_name):
                        refresh()
                ui.button(  # type: ignore[attr-defined]
                    f"Attach to {name}", on_click=_adopt, icon="link"
                ).props("color=primary")

    # Present the deploy path according to whether attaching is actually an option.
    #
    # When every candidate is out of scope, deploying is not the "risky alternative" -- it
    # is the ONLY correct way forward. Keeping it collapsed behind a warning-triangle
    # expansion labelled "instead" made the right action look like the dangerous one, and
    # hid it: the operator saw a prominent blue Attach button they must not press and a
    # folded warning they should. So it renders EXPANDED, titled as the way forward, with
    # no warning glyph.
    #
    # When a candidate IS attachable, a second MSK cluster is expensive and rarely
    # intended, so the deploy stays collapsed and de-emphasised as before.
    deploy_is_the_way = bool(out_of_scope) and not in_scope
    with ui.expansion(  # type: ignore[attr-defined]
        "Deploy a CDC pipeline for this table set"
        if deploy_is_the_way
        else "Deploy a separate CDC pipeline instead",
        icon="rocket_launch" if deploy_is_the_way else "warning",
        value=deploy_is_the_way,
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}"):
        _render_cdc_infra_deploy_action(
            ui, migration_state, job_manager, refresh,
            inventory=inventory, session=session,
        )


def _render_cdc_infra_deploy_action(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """The BYO-VPC infrastructure form + 'Deploy CDC infrastructure' button."""
    ui.label(  # type: ignore[attr-defined]
        "No cdc-stack is deployed yet. Provide your VPC and the plugin/source "
        "details below, then deploy the infrastructure (MSK Serverless, the "
        "connector networking, plugins and IAM role). This takes ~5 minutes "
        "and creates billable AWS resources; connectors are created later by "
        "Start CDC."
    ).classes("text-xs text-gray-600")

    _render_cdc_least_privilege_note(ui, session=session)
    # The form renders BEFORE the button but drives its enabled state, so it flips the
    # button in place (via this holder) instead of triggering a re-render, which would
    # recreate the very field being typed in. Same approach the Connect step uses to gate
    # its Next button from live input (``ui/connect.py``: ``update_next_state``).
    _gate: dict = {"button": None, "hint": None}

    def _sync_gate() -> None:
        """Enable Deploy only once the VPC ID is filled in; else say what is missing."""
        button, hint = _gate["button"], _gate["hint"]
        if button is None or hint is None:
            return  # a prerequisite check blocks first, so there is no gate to sync
        if getattr(button, "is_deleted", False) or getattr(hint, "is_deleted", False):
            return  # the panel was rebuilt while an input handler still held these
        missing = not (migration_state.cdc_infra_inputs().get("vpc_id") or "").strip()
        button.set_enabled(not missing)
        hint.set_text(
            "Enter your VPC ID above to enable the deploy." if missing else ""
        )

    _render_cdc_infra_form(
        ui, migration_state, session=session, on_vpc_change=_sync_gate
    )

    async def _confirm() -> None:
        _job = _current_job(job_manager, getattr(migration_state, "job_id", None))
        _needs_objects = pg_objects_required_before_deploy(migration_state, _job)
        await _open_cdc_infra_dialog(
            ui, migration_state,
            lambda: _start_cdc_infra_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
            pg_objects_required=_needs_objects,
            # The gate inside needs the same inputs the connector config gets: the captured
            # table set (from the inventory + watermark) and the re-snapshot intent.
            inventory=inventory,
            job=_job,
        )

    # Explicit CDC-prerequisite gate. MSK is billable and takes ~5 min to
    # create, and a source whose binlog is not ROW/FULL can never stream -- fixing
    # it needs a parameter-group change plus a reboot on RDS. So verify that BEFORE
    # any infrastructure is paid for, rather than discovering it as an undiagnosed
    # connector failure later.
    _prereq_block = cdc_prerequisite_block_reason(
        migration_state.get_prereq_report(MigrationMode.CDC),
        # The reports are never persisted, so a reconnected Full-load-+-CDC run
        # legitimately has none. (Nothing CLEARS them either -- an earlier version of
        # this comment said the Full Load does, which was never true: the start path
        # only records prereq_gated_mode. A report therefore outlives the selection it
        # covered, which is why the run guard also checks its scope.) The run could
        # only have STARTED once the CDC-superset checks passed, and THAT is recorded
        # durably -- use it instead of re-demanding the checks.
        cdc_checks_already_passed=(
            getattr(migration_state, "prereq_gated_mode", None) is MigrationMode.CDC
        ),
    )
    deploy_btn = ui.button(  # type: ignore[attr-defined]
        "Deploy CDC infrastructure", on_click=_confirm, icon="cloud_upload"
    ).props("color=primary")
    if _prereq_block:
        deploy_btn.props("disable")
        deploy_btn.tooltip(_prereq_block)
        render_notice(
            ui,
            tone="warning",
            # The header must not say "run the checks first" once they HAVE run and one
            # failed -- which is exactly the CDC_REPLICATION_OBJECTS case.
            header=cdc_prerequisite_block_header(
                migration_state.get_prereq_report(MigrationMode.CDC),
                cdc_checks_already_passed=(
                    getattr(migration_state, "prereq_gated_mode", None)
                    is MigrationMode.CDC
                ),
            ),
            body=_prereq_block,
        )
        return

    # VpcId is the one input the operator may still have to CHOOSE (subnets/NAT, the plugin
    # bucket, the DSQL cluster ARN, the source host and its secret are all resolved at deploy
    # time). For an RDS/Aurora source it is now PREFILLED from the source's own DBSubnetGroup
    # -- the same DescribeDBInstances response the tool already reads for the source security
    # group, so no extra API call and no extra IAM -- and the field stays editable, because
    # the CDC pipeline may legitimately run in another VPC reached by peering / Transit
    # Gateway / PrivateLink. It is still REQUIRED: a non-RDS host (self-managed PostgreSQL on
    # EC2), a cross-account endpoint or a missing rds:DescribeDBInstances leaves it blank.
    # It was validated only in the SUBMIT path: the button looked ready, the click
    # opened the confirmation dialog (which runs a network diagnosis and a cost
    # estimate), and only the final Deploy answered with an "Enter your VPC ID." toast.
    # State the requirement before the click instead.
    deploy_btn.tooltip(
        "Deploy the CDC infrastructure (MSK Serverless, networking, plugins, IAM)."
    )
    _gate["button"] = deploy_btn
    _gate["hint"] = inline_hint(ui, "", tone="info")
    _sync_gate()

def _render_cdc_least_privilege_note(ui, *, session=None) -> None:
    """Recommend a dedicated least-privilege CDC source user before deploy.

    When the source was connected with a username/password, those exact
    credentials are stored in Secrets Manager and injected into the live Debezium
    connector. If the operator connected as root/admin, that over-privileged
    credential becomes long-lived. This collapsible note recommends a dedicated
    CDC user with only the grants Debezium needs and shows a copyable snippet
    (engine-appropriate: a MySQL user or a PostgreSQL role).
    Shown only for password auth (an SM-auth source already chose its own secret).
    """
    if getattr(session, "source_secret_id", None):
        return  # SM auth -- the customer manages their own credential.
    is_pg = _cdc_source_type(session) is SourceType.POSTGRES
    with ui.expansion(  # type: ignore[attr-defined]
        "Recommended: use a dedicated least-privilege CDC user",
        icon="security",
        value=False,
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
        if is_pg:
            ui.label(  # type: ignore[attr-defined]
                "The username/password you connected with will be stored in AWS "
                "Secrets Manager and used by the CDC connector. Avoid using a "
                "superuser account: create a dedicated PostgreSQL role granted "
                "only what Debezium needs, reconnect on the Connect step as that "
                "role, then deploy. This limits blast radius if the secret is "
                "ever exposed."
            ).classes("text-xs text-gray-600")
            ui.code(  # type: ignore[attr-defined]
                "CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD '<strong-password>';\n"
                "GRANT USAGE ON SCHEMA public TO debezium;\n"
                "GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;\n"
                "CREATE PUBLICATION dbz_publication FOR TABLE <schema>.<table>;",
                language="sql",
            ).classes("w-full text-xs")
            ui.label(  # type: ignore[attr-defined]
                "REPLICATION lets Debezium read the WAL; SELECT reads table data "
                "for the initial snapshot. Debezium also needs the publication "
                "above (CREATE PUBLICATION FOR TABLE) and logical decoding enabled "
                "on the server: set wal_level=logical (on RDS/Aurora, set the "
                "rds.logical_replication=1 parameter and reboot). Scope the grant "
                "to specific schemas if your policy requires it."
            ).classes("text-xs text-gray-500")
        else:
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
    with ui.expansion("Delete CDC infrastructure", icon="delete_forever").classes(  # type: ignore[attr-defined]
        f"w-full {EXPANSION_PANEL_CLASSES}"
    ):
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

# (label, state-key, placeholder/help, required?) for the Deploy-infra form.
# The form asks for ONLY the VPC id; from it the tool diagnoses egress and either
# reuses existing NAT subnets, has the stack create its own NAT, or (blocked) asks
# for the subnet override below. Everything else is auto-discovered/derived, and for an
# RDS/Aurora source the VPC id itself is PREFILLED from the source's DBSubnetGroup (shown
# with its provenance, and editable -- the pipeline may belong in another VPC). It stays in
# the form because the derivation is best-effort: a non-RDS host, a cross-account endpoint
# or a missing rds:DescribeDBInstances leaves it for the operator to supply.
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

def _render_cdc_infra_form(
    ui, migration_state, *, session=None, on_vpc_change=None
) -> None:
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

    with ui.expansion("Infrastructure inputs", icon="lan", value=True).classes(  # type: ignore[attr-defined]
        f"w-full {EXPANSION_PANEL_CLASSES}"
    ):
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

            # The connector must reach the source database privately, so the
            # source's own VPC (or one with private connectivity to it) is the
            # safe default.
            if key == "vpc_id":
                # Show the PROVENANCE of a derived value. Prefilling silently and
                # prefilling visibly are different things: the operator has to be able to
                # see that this came from the source and that changing it is expected (the
                # pipeline may belong in another VPC, reached by peering / Transit Gateway /
                # PrivateLink). Absent when nothing could be derived.
                _provenance = getattr(migration_state, "cdc_vpc_provenance", None)
                if _provenance and (values.get(key) or "").strip():
                    ui.label(  # type: ignore[attr-defined]
                        f"Derived from {_provenance}. Change it if the CDC pipeline "
                        "should run elsewhere."
                    ).classes("w-full text-xs text-gray-500")
                else:
                    ui.label(  # type: ignore[attr-defined]
                        "Recommended: the same VPC as your source database (or one with "
                        "private connectivity to it) — the connector must reach the "
                        "source privately."
                    ).classes("w-full text-xs text-gray-500")

            def _save(_e, k=key, f=field) -> None:
                current = migration_state.cdc_infra_inputs()
                current[k] = (f.value or "").strip()
                migration_state.set_cdc_infra_inputs(current)
                # VpcId gates the Deploy button, so re-check it as the value changes.
                if k == "vpc_id" and on_vpc_change is not None:
                    on_vpc_change()

            field.on("blur", _save)
            # Also gate as the value changes, not only on blur: the user's next move
            # after entering the VPC ID is to click Deploy, and a click on a still-
            # disabled button is swallowed. on_value_change (as the Connect step uses)
            # covers a paste too, which fires no keystroke.
            if key == "vpc_id":
                field.on_value_change(_save)

        # Advanced: the cdc-stack name. The mandatory canonical "dsql-cdc-" prefix is
        # rendered INSIDE the field via Quasar's built-in `prefix` prop (baseline-
        # aligned with the typed text, like a "$" before an amount), and the user
        # types only the SUFFIX (e.g. "orders" -> dsql-cdc-orders) to run a
        # SECOND migration's CDC alongside an existing one. Editing only the suffix
        # makes it impossible to leave the cdc-stack family the deploy role
        # authorizes, so a bare "abcde" becomes the valid "dsql-cdc-abcde". (New
        # stacks always take the canonical prefix; a legacy "mysql-dsql-cdc-*" name is
        # still accepted for existing deployments -- see cdc_stack_name_is_valid.)
        name_field = ui.input(  # type: ignore[attr-defined]
            label="Advanced — CDC stack name (one per source DB)",
            value=cdc_stack_name_suffix(
                getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
            ),
            placeholder="stack",
        ).props(f'prefix="{CDC_STACK_NAME_PREFIX}"').classes("w-full text-sm")
        ui.label(  # type: ignore[attr-defined]
            "Full stack name = the fixed prefix + your suffix "
            "(e.g. dsql-cdc-orders). One stack per source DB."
        ).classes("w-full text-xs text-gray-500")

        def _current_suffix() -> str:
            return cdc_stack_name_suffix(
                getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
            )

        def _save_stack_name(_e, f=name_field) -> None:
            suffix = (f.value or "").strip()
            if not suffix:
                # Empty -> keep the current name; reflect its suffix back in the field.
                f.value = _current_suffix()
                return
            full = build_cdc_stack_name(suffix)
            if full is not None and migration_state.set_cdc_stack_name(full):
                f.value = cdc_stack_name_suffix(full)  # normalize what's shown
                return
            # Reject: revert to the current suffix and explain the (charset) rule --
            # the prefix is already guaranteed, so only the suffix charset can fail.
            f.value = _current_suffix()
            ui.notify(  # type: ignore[attr-defined]
                "CDC stack name suffix may use only letters, digits and hyphens "
                "(e.g. 'orders' -> dsql-cdc-orders).",
                type="warning", position="top",
            )

        name_field.on("blur", _save_stack_name)

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
            "The source connection is not active. Open the Connect step "
            "and test the source connection, then return here to deploy."
        )
    if getattr(session, "source_password", None) is None:
        return (
            "Re-enter the source credentials: the in-memory password is not kept "
            "after a restart (for security), and the CDC source secret is created "
            "from it. Test the source connection on the Connect step, then deploy."
        )
    return None

async def _open_cdc_infra_dialog(
    ui, migration_state, on_confirm, *, session=None, pg_objects_required=False,
    inventory=None, job=None,
) -> None:
    """Confirm dialog before the (~5 min, billable) infrastructure create.

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
    # Checked here, not after Deploy: a VpcId that cannot reach the source fails only once
    # the MSK cluster exists, ~20 minutes in, and leaves a stack to tear down.
    source_vpc_warning = await run.io_bound(
        _source_vpc_warning_for_dialog, migration_state, session
    )
    # Checked here, not after Deploy: a PostgreSQL cdc-stack is always created
    # SeedMode=External and is seeded BY THIS APP over MSK 9098, so a deployment that
    # cannot reach MSK -- or that the new cluster would not admit -- fails only at
    # Start CDC, after a billable MSK Serverless cluster already exists.
    # `or MskSeedAdmission()`: nicegui's run.io_bound returns None once the app is
    # stopping, and an attribute access on that would raise out of the dialog.
    seed_admission = (
        await run.io_bound(_msk_seed_admission, migration_state, session)
        or MskSeedAdmission()
    )
    # The COST gate. For a CDC-ONLY session, absent publication/slot means Start CDC cannot
    # work, so refuse BEFORE the ~5-minute billable MSK Serverless create. ASYMMETRIC by
    # migration type on purpose: in a Full-load-+-CDC run the objects legitimately do not
    # exist yet (the Full Load creates them), and deploying the infra first is the flow this
    # very card recommends -- so that case stays silent. It cannot strand anyone: the
    # Start-side gate RE-PROBES rather than remembering this verdict, so provisioning
    # between the two steps simply turns the block off.
    pg_infra_block: Optional[tuple] = None
    # ``pg_objects_required`` is computed by the caller (which holds job_manager) and is
    # deliberately NOT just "the type is CDC only". Switching the type to Full-load-+-CDC
    # after a FINISHED Full-load-only run re-grades the existence row to SKIP -- its detail
    # says "Full Load creates them for this run" -- so the prerequisite gate un-gates Deploy
    # while the objects still do not exist. The operator pays for MSK Serverless and Start
    # then refuses. Keying on the WATERMARK instead is precise: a fresh combined run has no
    # watermark (so the recommended deploy-before-load flow is untouched), a combined run
    # whose load provisioned carries a slot_name (untouched), and a load that finished
    # WITHOUT a slot is exactly the state that must block.
    if pg_objects_required:
        try:
            # Through the shared gate, so the operator's RECORDED re-snapshot decision is
            # honoured here exactly as it is at Start. Passing `[], None` with no
            # force_initial (v0.1.509) made this dialog re-block the route the tool had just
            # recommended, with a notice that is false for it, and sent the operator back to
            # "start over as Full load + CDC" -- the dead end v0.1.510 existed to remove.
            pg_infra_block = await run.io_bound(
                _pg_objects_gate_block,
                migration_state, session, inventory, _cdc_watermark(job),
            )
        except Exception:  # noqa: BLE001 - unreadable is not absent; never block on it
            pg_infra_block = None
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 460px"):  # type: ignore[attr-defined]
        ui.label("Deploy CDC infrastructure").classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            f"This creates the cdc-stack '{stack_name}' (CloudFormation): an MSK "
            "Serverless cluster, connector networking in your VPC, the plugins and "
            "an IAM role. It takes about 5 minutes and creates billable AWS "
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
        if source_vpc_warning:
            # A mismatch is not a blocker -- peering / TGW / PrivateLink are legitimate --
            # but it is the shape of a typo, so it warns and says what to confirm.
            _render_notice(
                ui,
                tone="warning",
                icon="lan",
                header="This is not the source's VPC",
                body=source_vpc_warning,
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
        if seed_admission.blocker:
            _render_notice(
                ui,
                tone="error",
                icon="vpn_lock",
                header="This deployment cannot start PostgreSQL CDC",
                body=(
                    seed_admission.blocker
                    + " Deploying now would create a billable MSK Serverless cluster "
                    "that Start CDC could never use."
                ),
            )
        elif seed_admission.warning:
            _render_notice(
                ui,
                tone="warning",
                icon="vpn_lock",
                header=(
                    seed_admission.header
                    or "MSK ingress for this host was not registered"
                ),
                body=seed_admission.warning,
            )
        elif seed_admission.note:
            # A normal, resolved state -> calm info, never a warning.
            _render_notice(
                ui,
                tone="info",
                icon="vpn_lock",
                header="MSK access for the PostgreSQL CDC seed",
                body=seed_admission.note,
            )
        if pg_infra_block is not None:
            _render_notice(
                ui,
                tone="error",
                icon="sync_problem",
                header=pg_infra_block[0],
                body=(
                    pg_infra_block[1]
                    + " Deploying now would create a billable MSK Serverless cluster that "
                    "Start CDC could not use. Fix this first, or start the migration over "
                    'as "Full load + CDC", which creates both at the snapshot point.'
                ),
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
            elif seed_admission.blocker:
                # Would create a billable MSK cluster this app could never seed.
                deploy_btn.props("disable")
                deploy_btn.tooltip(seed_admission.blocker)
            elif pg_infra_block is not None:
                # CDC-only, and the source has nothing to stream from.
                deploy_btn.props("disable")
                deploy_btn.tooltip(pg_infra_block[1])
    dialog.open()

def derive_cdc_vpc_from_source(migration_state, session) -> bool:
    """Prefill the CDC VpcId from the source DB's own DBSubnetGroup. True when it changed.

    The value comes from the SAME ``DescribeDBInstances`` response the tool already reads for
    the source security group, so there is no extra API call and no extra IAM -- only more
    fields off a response it has. Best effort, exactly like the rest of ``rds_metadata``: a
    non-RDS host (self-managed PostgreSQL on EC2), a cross-account endpoint or a missing
    ``rds:DescribeDBInstances`` leaves the field blank for the operator to fill, which is the
    flow this feature had before it existed.

    Only fills an EMPTY field -- an operator's own value is never overwritten -- and records
    the provenance so the prefill is visible rather than a silent substitution. Blocking I/O:
    callers MUST run it off the event loop.
    """
    if session is None:
        return False
    host = getattr(getattr(session, "source_config", None), "host", None)
    if not host:
        return False
    fields = migration_state.cdc_infra_inputs()
    if (fields.get("vpc_id") or "").strip():
        return False  # the operator (or an earlier derivation) already supplied one
    try:
        from dsql_migrator.core.rds_metadata import (
            build_rds_client,
            fetch_source_network,
            parse_rds_region,
        )

        client = build_rds_client(
            getattr(session, "aws_profile", None), parse_rds_region(host)
        )
        info = fetch_source_network(client, host)
    except Exception:  # noqa: BLE001 - best effort; the manual field remains
        return False
    if info is None or not info.vpc_id:
        return False
    fields["vpc_id"] = info.vpc_id
    # The advanced subnet override gets the same treatment: the source's own subnets are
    # the overwhelmingly common answer, and leaving it blank still auto-configures.
    if not (fields.get("connector_subnet_ids") or "").strip() and info.subnet_ids:
        fields["connector_subnet_ids"] = ",".join(info.subnet_ids)
    migration_state.set_cdc_infra_inputs(fields)
    where = info.db_identifier or "the source database"
    group = f" (subnet group {info.db_subnet_group})" if info.db_subnet_group else ""
    migration_state.cdc_vpc_provenance = f"the source {where}{group}"
    return True


def _source_vpc_warning_for_dialog(migration_state, session) -> str:
    """The "this is not the source's VPC" caution for the deploy dialog, or "".

    Checked BEFORE Deploy because a wrong VpcId is otherwise only discovered after roughly
    twenty minutes of MSK deployment, leaving a failed stack to clean up. Read-only, and
    silent whenever the answer is unknown -- a non-RDS host, a cross-account endpoint or a
    missing ``rds:DescribeDBInstances`` yields no warning rather than a false one. Blocking
    I/O: the caller runs it via ``run.io_bound`` alongside the network diagnosis.
    """
    if session is None:
        return ""
    source = getattr(session, "source_config", None)
    host = getattr(source, "host", None)
    if not host:
        return ""
    vpc_id = (migration_state.cdc_infra_inputs().get("vpc_id") or "").strip()
    if not vpc_id:
        return ""
    try:
        from dsql_migrator.core.rds_metadata import (
            build_rds_client,
            fetch_source_network,
            parse_rds_region,
            source_vpc_mismatch_warning,
        )

        client = build_rds_client(
            getattr(session, "aws_profile", None), parse_rds_region(host)
        )
        return source_vpc_mismatch_warning(vpc_id, fetch_source_network(client, host)) or ""
    except Exception:  # noqa: BLE001 - best effort; the dialog works without it
        return ""


def _msk_seed_admission(migration_state, session, *, vpc_id=None):
    """External-seed admission for the deploy dialog AND the create call (blocking).

    ONE decision used in two places, so the dialog cannot say "clear" while the
    submitted ``HostSubnetCidr`` is empty (or vice versa). Keyed on the existing
    :func:`_cdc_start_seed_mode` so a Lambda-seeded deploy (MySQL on Fargate/local)
    returns an all-empty verdict and behaves exactly as before -- no notice, no
    discovery, no AWS call, an empty ``HostSubnetCidr`` and therefore no ingress rule.
    An explicitly configured ``DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR`` short-circuits
    inside :func:`msk_seed_admission` before any discovery, which is what keeps the
    in-VPC EC2 host byte-identical.

    ``vpc_id`` overrides the state's own value: the submit path has already resolved
    ``fields`` (which may carry a derived VpcId the state has not been written back
    from), and the network the cdc-stack admits must be resolved against the VPC the
    create call is ACTUALLY about, not a possibly-staler copy.

    Blocking: the callers run it via ``run.io_bound``, like the network diagnosis
    beside it.
    """
    from dsql_migrator.config import load_config as _load_config
    from dsql_migrator.core.cdc import msk_seed_admission

    cfg = _load_config()
    seed_mode = _cdc_start_seed_mode(_cdc_source_type(session), cfg.cdc_seed_mode)
    if seed_mode != "external":
        return MskSeedAdmission()
    if vpc_id is None:
        vpc_id = migration_state.cdc_infra_inputs().get("vpc_id") or ""
    vpc_id = vpc_id.strip()
    lookup = None
    if not cfg.cdc_host_subnet_cidr and vpc_id:
        from dsql_migrator.core.ec2_metadata import (
            HostLookup,
            build_ec2_client,
            discover_host_network,
        )

        try:
            target = getattr(session, "target_config", None)
            lookup = discover_host_network(
                build_ec2_client(
                    getattr(session, "aws_profile", None),
                    getattr(target, "region", None) if target else None,
                ),
                vpc_id,
            )
        except Exception as exc:  # noqa: BLE001 - never break the deploy path
            # Distinguished, not collapsed: a failed build_ec2_client or a denied
            # describe used to render the SAME message as "no interface found", and
            # left no log line either -- three causes, one indistinguishable verdict.
            first = (str(exc).splitlines() or [""])[0][:160]
            _LOGGER.warning(
                "MSK seed host discovery failed for %s (%s: %s)",
                vpc_id,
                type(exc).__name__,
                first,
            )
            lookup = HostLookup(
                reason="lookup-failed", detail=f"{type(exc).__name__}: {first}"
            )
    return msk_seed_admission(
        seed_mode=seed_mode,
        cdc_seed_mode=cfg.cdc_seed_mode,
        cdc_msk_access=getattr(cfg, "cdc_msk_access", False),
        configured_host_cidr=cfg.cdc_host_subnet_cidr,
        vpc_id=vpc_id,
        lookup=lookup,
    )


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

def _probe_binlog_resume_gap(migration_state, job_manager, session) -> Optional[str]:
    """Read-only: is the watermark's binary log still on the source? (blocking)

    Runs one ``SHOW BINARY LOGS`` against the source and compares it with the Full
    Load watermark's binlog file. Returns an actionable reason when the log has been
    purged (a gapless resume is impossible), else ``None`` -- including whenever the
    answer is unknown (no watermark, manual start point, no source password after a
    restart, or the statement/privilege is unavailable), so this never blocks on
    uncertainty. Blocking I/O: callers MUST run it via ``run.io_bound``.
    """
    # A manual start point overrides the watermark, so the watermark's log being
    # gone is not what CDC will resume from -- nothing to warn about here.
    # ``cdc_start_override`` already returns None in "auto" mode, so this is the
    # single condition needed.
    if migration_state.cdc_start_override() is not None:
        return None
    job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    watermark = _cdc_watermark(job)
    watermark_file = getattr(watermark, "binlog_file", None)
    if not watermark_file:
        return None
    source_config = getattr(session, "source_config", None)
    if source_config is None:
        return None
    try:
        from dsql_migrator.core.watermark import (
            binlog_resume_gap_reason,
            list_binary_logs,
        )
        from dsql_migrator.ui.connect import make_source_engine_factory

        engine = make_source_engine_factory(
            getattr(session, "source_password", None)
        )(source_config)
        try:
            with engine.connect() as connection:
                retained = list_binary_logs(connection)
        finally:
            engine.dispose()
    except Exception:  # noqa: BLE001 - advisory pre-flight; unknown never blocks
        return None
    return binlog_resume_gap_reason(watermark_file, retained)


def pg_objects_required_before_deploy(migration_state, job) -> bool:
    """Must CDC's publication/slot exist BEFORE the (billable) infrastructure is created?

    Pure, so the decision is testable without a dialog. Deliberately NOT just "the type is
    CDC only": switching the type to Full-load-+-CDC after a FINISHED Full-load-only run
    re-grades the existence prerequisite to SKIP -- its detail says "Full Load creates them
    for this run" -- so the prerequisite gate un-gates Deploy while the objects still do not
    exist. The operator then pays for MSK Serverless and Start CDC refuses.

    Keying on the WATERMARK is precise:

    * a FRESH combined run has no watermark -> False, so the recommended
      deploy-the-infra-before-the-load flow is untouched;
    * a combined run whose load DID provision carries ``slot_name`` -> False;
    * a load that FINISHED WITHOUT a slot -> True, which is exactly the state that must be
      gated, whichever tile is selected now.
    """
    if getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY:
        return True
    watermark = _cdc_watermark(job)
    return watermark is not None and not getattr(watermark, "slot_name", None)


def _cdc_watermark(job):
    """The watermark a CDC start may resume from; the slot is suppressed on an UNFINISHED load.

    The watermark is attached BEFORE the first table loads, so a FAILED or partial run leaves
    one byte-identical to a successful run's -- same WAL LSN, same slot name. Nothing in the
    CDC-start path reads the job's status, so honouring that slot selects
    ``snapshot.mode=never`` and the never-loaded tables get NO baseline and NO backfill: their
    rows are permanently absent from the target, silently. Switching the type to "CDC only"
    even removes the "retry the failed tables first" hint, because that substep disappears.

    Dropping ONLY ``slot_name`` routes such a job to ``snapshot.mode=initial`` -- re-snapshot
    every captured table -- which is lossless. It SELF-HEALS: a retry carries the ORIGINAL
    watermark forward, so once the job reaches DONE the slot is honoured again and the gapless
    resume comes back on its own. It also fails in the safe direction: a load that completed
    but whose post-load pass raised costs a re-read, never correctness.
    """
    wm = getattr(job, "watermark", None) if job is not None else None
    if wm is None:
        return None
    if getattr(job, "status", None) != "DONE" and getattr(wm, "slot_name", None):
        return wm.model_copy(update={"slot_name": None})
    return wm


def _pg_objects_gate_block(
    migration_state, session, inventory, watermark
) -> Optional[tuple]:
    """The publication/slot gate, run with the SAME inputs the connector config will get.

    THE single entry point for every surface that gates on those objects, because deriving
    the probe's three inputs per call site is how this defect keeps coming back:

    * v0.1.507 fixed the Start path so a recorded re-snapshot decision stops the block.
    * v0.1.509 added the same gate to the Deploy dialog and passed ``[], None`` with no
      ``force_initial`` -- so the operator who took the re-snapshot route was re-blocked one
      screen later by a notice that is FALSE for that route ("the connector is configured
      with publication.autocreate.mode=disabled"), told to "start the migration over as
      'Full load + CDC'", and the continuation v0.1.510 built was a dead end again.
    * The Start DIALOG meanwhile graded coverage against every table in the inventory rather
      than the captured set, which can block on a publication that covers everything this
      migration actually replicates.

    ``pg_replication_objects_blocker``'s contract is to MIRROR ``build_pg_source_config``'s
    two decisions exactly; that is only checkable if the mirror lives in ONE place. So the
    tables come from :func:`_cdc_tables_for_config`, the resume signal from
    :func:`_cdc_resume_signal`, and the two graded decisions from
    :func:`pg_snapshot_mode` / :func:`pg_publication_autocreate_mode` -- the very functions
    the config builder itself calls. Re-deriving "will the connector autocreate?" from the
    start mode instead (which is what this did) made the gate agree with the config only by
    coincidence: it read ``manual`` as "autocreates", while the config read a SEPARATE flag
    that the start-position radio and session restore never set. The gate then waved through
    the one combination it exists to stop.

    Blocking source I/O: callers MUST run it via ``run.io_bound``.
    """
    tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    resume_override, force_initial = _cdc_resume_signal(migration_state, session)
    snapshot_mode = pg_snapshot_mode(
        watermark,
        resume_override=resume_override,
        force_initial_snapshot=force_initial,
    )
    return _probe_pg_replication_objects(
        migration_state,
        session,
        tables,
        watermark,
        snapshot_mode=snapshot_mode,
    )


def _probe_pg_replication_objects(
    migration_state, session, tables, watermark, *, snapshot_mode: str = "never"
) -> Optional[tuple]:
    """Read-only: do CDC's publication + slot exist on the PostgreSQL source? (blocking)

    Returns the blocker's ``(header, body)`` or None. Fails CLOSED on "the catalog says
    absent/incomplete" and OPEN on "I could not ask" -- unreadable is not evidence of
    absence, and a missing publication makes the connector die LOUDLY anyway. Same
    degrade-to-silence discipline as :func:`_probe_binlog_resume_gap`.

    The object names come from the SAME fallback ``dispatch_source_config`` uses (recorded
    on the watermark, else derived from the stack name), so the probe can never check a
    different object than the connector will. ``resumes_from_slot`` mirrors
    ``build_pg_source_config``'s invariant exactly -- the recorded slot, not the LSN -- so
    the check and the config agree by construction.

    Blocking I/O: callers MUST run it via ``run.io_bound`` (NEVER on the NiceGUI loop --
    one asyncio loop serves every browser session on Fargate).
    """
    if _cdc_source_type(session) is not SourceType.POSTGRES:
        return None
    source_config = getattr(session, "source_config", None)
    if source_config is None:
        return None
    stack = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    try:
        from dsql_migrator.core import cdc_pg_slot as _pg
        from dsql_migrator.ui.connect import make_source_engine_factory

        pub = (
            getattr(watermark, "publication_name", None)
            or _pg.pg_publication_name(stack)
        )
        slot = getattr(watermark, "slot_name", None) or _pg.pg_slot_name(stack)
        engine = make_source_engine_factory(getattr(session, "source_password", None))(
            source_config
        )
        try:
            with engine.connect() as connection:
                objects = _pg.read_pg_replication_objects(
                    connection, publication_name=pub, slot_name=slot
                )
        finally:
            engine.dispose()
    except Exception:  # noqa: BLE001 - unreadable is NOT absent; degrade to silence
        return None
    # MIRROR build_pg_source_config's two decisions exactly, or this refuses a
    # configuration that would have worked -- or, worse, passes one that cannot. Both are
    # read off the EFFECTIVE snapshot mode, through the same pure function the config uses:
    # ``initial`` means the connector does not resume from the slot and DOES create its own
    # publication, so neither absence blocks; ``never`` means it resumes from the
    # provisioned slot and may not touch the publication, so both absences block.
    autocreates = (
        pg_publication_autocreate_mode(snapshot_mode) == "filtered"
    )
    return _pg.pg_replication_objects_blocker(
        objects,
        [getattr(t, "name", str(t)) for t in (tables or [])],
        resumes_from_slot=(
            bool(getattr(watermark, "slot_name", None)) and snapshot_mode == "never"
        ),
        connector_autocreates_publication=autocreates,
    )


async def _open_cdc_start_dialog(
    ui, migration_state, on_confirm, *, session=None, job_manager=None, inventory=None,
    refresh_after_drop=None,
) -> None:
    """Confirm dialog before the (billable, partition-quota-using) Start.

    Two read-only pre-flight checks run first, so a doomed Start is caught before it
    consumes ~26 min of billable connector create and MSK partition quota:

    * :func:`cdc_deploy_connection_blocker` -- Start CDC builds the source
      connector's credentials secret from the in-memory source password (not
      restored after a restart) and needs a live target. Missing either is a hard
      block: the user reconnects first.
    * :func:`_probe_binlog_resume_gap` -- the Full Load watermark's binary log must
      still exist on the source, or the gapless hand-off is impossible. This one is
      a **warning**, not a block: starting with a gap can be a deliberate choice,
      and the check degrades to silence whenever the answer is unknown.

    The binlog probe is blocking source I/O, so it runs via ``run.io_bound`` -- which
    is why this helper is async and MUST be awaited (an un-awaited coroutine would
    silently never open the dialog).
    """
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    conn_blocker = cdc_deploy_connection_blocker(session)
    binlog_gap: Optional[str] = None
    fk_block = None
    pg_objects_block: Optional[tuple] = None
    inventory_tables = list(getattr(inventory, "tables", None) or [])
    if job_manager is not None:
        from nicegui import run

        try:
            binlog_gap = await run.io_bound(
                _probe_binlog_resume_gap, migration_state, job_manager, session
            )
        except Exception:  # noqa: BLE001 - advisory only; never block the dialog
            binlog_gap = None
        # Enforced foreign keys on the target are a HARD block (unlike the binlog gap):
        # the sink dead-letters an out-of-order child row permanently and silently, so
        # "unknown" must block too. Blocking DSQL I/O -> run.io_bound, never on the loop.
        if not conn_blocker:
            _job = _current_job(job_manager, migration_state.job_id)
            _wm = _cdc_watermark(_job)
            try:
                probed = await run.io_bound(
                    _probe_cdc_blocking_foreign_keys,
                    migration_state, session, inventory, _wm,
                )
            except Exception:  # noqa: BLE001 - probe itself failed -> cannot prove clean
                probed = None
                _log_cdc_event(
                    "foreign-key precondition unknown",
                    status=ActivityStatus.INFO,
                    detail=(
                        "could not read the target's foreign keys before Start CDC; "
                        "the job re-checks before creating any connector"
                    ),
                )
            if probed is not None and probed.blocking:
                fk_block = probed
        # The PostgreSQL publication/slot precondition. Checked HERE, before the operator
        # pays: starting without it deploys both connectors and the Debezium task dies on
        # "Publication autocreation is disabled" -- after the cdc-stack update. Blocking
        # source I/O -> run.io_bound, never on the loop.
        try:
            _job2 = _current_job(job_manager, migration_state.job_id)
            pg_objects_block = await run.io_bound(
                _pg_objects_gate_block,
                migration_state, session, inventory, _cdc_watermark(_job2),
            )
        except Exception:  # noqa: BLE001 - unreadable is not absent; never block on it
            pg_objects_block = None
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
        if binlog_gap:
            _render_notice(
                ui,
                tone="warning",
                icon="history_toggle_off",
                header="The snapshot's binary log has been purged",
                body=binlog_gap,
            )
        # The PostgreSQL publication/slot block, with its remedy in the SAME dialog (same
        # shape as the FK slot below): a block whose only advice is "re-run the whole Full
        # Load" is a dead end, and the pre-flight results just computed would be thrown
        # away by closing. There is deliberately NO "accept the gap and stream from now"
        # option: the window starts at Full Load START, so it spans the entire load plus
        # all think time, the tool cannot say how many rows or which tables are affected,
        # and Validation's default ROW_COUNT mode would then certify the result clean --
        # an acknowledgement nobody can evaluate is a click-through, not consent.
        _pg_state = {"resolved": False}
        if pg_objects_block is not None:
            pg_slot_area = ui.column().classes("w-full gap-2")  # type: ignore[attr-defined]

            def _render_pg_block() -> None:
                pg_slot_area.clear()
                with pg_slot_area:
                    _render_notice(
                        ui,
                        tone="error",
                        icon="sync_problem",
                        header=pg_objects_block[0],
                        body=pg_objects_block[1],
                    )
                    _render_notice(
                        ui,
                        tone="info",
                        icon="restart_alt",
                        header="Re-snapshot instead — gapless, no second Full Load",
                        body=(
                            "Debezium can create its own replication slot, snapshot every "
                            "selected table and then stream from that slot. There is no "
                            "window between the snapshot and the stream, so nothing is "
                            "lost, and the target load is idempotent (INSERT ... ON "
                            "CONFLICT), so re-reading a row is safe. The cost is reading "
                            "the source tables again. If the publication is also missing, "
                            "the CONNECTOR's database user creates one over exactly the "
                            "captured tables — the tool itself never writes to your source."
                        ),
                    )

                    def _take_resnapshot() -> None:
                        # "manual" is the WHOLE decision now: it forces
                        # snapshot.mode=initial, from which publication.autocreate.mode
                        # =filtered is derived, so the connector's own DB user creates the
                        # publication. There is no second flag to forget.
                        migration_state.set_cdc_start_mode("manual")
                        _pg_state["resolved"] = True
                        pg_slot_area.clear()
                        with pg_slot_area:
                            _render_notice(
                                ui,
                                tone="success",
                                icon="restart_alt",
                                header="Will re-snapshot every selected table",
                                body=(
                                    "CDC will snapshot the selected tables and then stream "
                                    "from the slot it creates, so the handoff has no gap. "
                                    "Start CDC is unlocked."
                                ),
                            )
                        start_btn.props(remove="disable")
                        start_btn.tooltip("")

                    ui.button(  # type: ignore[attr-defined]
                        "Re-snapshot every table instead",
                        icon="restart_alt",
                        on_click=_take_resnapshot,
                    ).props("color=primary")

        # Tracks whether the removal already happened, so Cancel can still refresh the card
        # behind the dialog (the drop is real even if the operator then backs out).
        _fk_state = {"cleared": False}
        if fk_block is not None:
            # The block notice and the Remove button live in a container that can be
            # REPLACED in place: the removal used to close the dialog and tell the operator
            # to "reopen Start CDC", which made a two-click job a four-click one and threw
            # away the pre-flight results (binlog probe, connection check) that had just
            # been computed. Now the same dialog reports the removal and unlocks Start.
            fk_slot = ui.column().classes("w-full gap-2")  # type: ignore[attr-defined]

            def _render_fk_block() -> None:
                fk_slot.clear()
                with fk_slot:
                    _render_notice(
                        ui,
                        tone="error",
                        icon="link",
                        header=_cdc_fk_block_reason(fk_block)[1],
                        body=_cdc_fk_block_body(fk_block),
                    )
                    if not fk_block.ours:
                        return

                    async def _remove_fks() -> None:
                        from nicegui import run

                        remove_btn.disable()
                        remove_btn.set_text("Removing…")
                        dropped, failed = await run.io_bound(
                            _drop_cdc_blocking_foreign_keys, fk_block.ours, session
                        )
                        if failed:
                            # Still blocked: an enforced FK that survived would make the
                            # stream dead-letter out-of-order child rows (23503), so Start
                            # must stay locked. Re-render the block with the outcome rather
                            # than closing -- the operator needs to see WHICH state they are
                            # in, and the remaining constraints are still listed above.
                            _fk_state["cleared"] = dropped > 0
                            fk_slot.clear()
                            with fk_slot:
                                _render_notice(
                                    ui,
                                    tone="error",
                                    icon="link",
                                    header=(
                                        f"{failed} foreign key(s) could not be removed — "
                                        "CDC is still blocked"
                                    ),
                                    body=(
                                        f"{dropped} removed, {failed} failed. The activity "
                                        "log names each one. Drop the remaining "
                                        "constraint(s) and reopen Start CDC; starting now "
                                        "would make the stream dead-letter out-of-order "
                                        "child rows (23503)."
                                    ),
                                )
                            return
                        # Cleared: report it HERE and unlock Start, so the operator carries
                        # straight on instead of re-opening the dialog and re-running the
                        # pre-flight checks.
                        _fk_state["cleared"] = True
                        fk_slot.clear()
                        with fk_slot:
                            _render_notice(
                                ui,
                                tone="success",
                                icon="link_off",
                                header=f"{dropped} foreign key(s) removed — ready to start",
                                body=(
                                    "The target no longer enforces the foreign keys this "
                                    "migration created, so the stream cannot dead-letter "
                                    "out-of-order child rows. Cut over re-creates them with "
                                    '"Apply foreign keys" once the stream has drained.'
                                ),
                            )
                        if not conn_blocker:
                            start_btn.enable()
                            start_btn.props(remove="disable")
                            start_btn.tooltip("")

                    remove_btn = ui.button(  # type: ignore[attr-defined]
                        "Remove foreign keys", on_click=_remove_fks, icon="link_off"
                    ).props("color=negative")
                    remove_btn.tooltip(
                        "Drops exactly the "
                        f"{len(fk_block.ours)} foreign key(s) this migration created; the "
                        "cut-over 'Apply foreign keys' step re-creates them."
                    )

            _render_fk_block()

        def _go() -> None:
            start_btn.disable()
            start_btn.set_text("Submitting…")
            ui.notify(  # type: ignore[attr-defined]
                "Submitting Start CDC…", type="info", position="top",
            )
            dialog.close()
            on_confirm()

        def _dismiss() -> None:
            dialog.close()
            # The removal is REAL even if the operator then backs out, so the card behind
            # the dialog must stop showing the FK block. The close used to happen inside the
            # removal, which refreshed implicitly; now that the dialog stays open, the
            # refresh has to hang off the dismissal instead.
            if _fk_state["cleared"] and refresh_after_drop is not None:
                refresh_after_drop()

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=_dismiss).props("flat")  # type: ignore[attr-defined]
            start_btn = ui.button("Start CDC", on_click=_go).props("color=primary")  # type: ignore[attr-defined]
            if conn_blocker:
                start_btn.props("disable")
                start_btn.tooltip(conn_blocker)
            elif fk_block is not None:
                # Hard block: an enforced FK makes the stream discard child rows
                # silently, so Start stays unavailable until the FKs are gone.
                start_btn.props("disable")
                start_btn.tooltip(
                    "Enforced foreign keys on the target would dead-letter "
                    "out-of-order child rows (23503). Remove them first."
                    if (fk_block.ours or fk_block.foreign)
                    else "The target's foreign keys could not be read. CDC stays "
                    "blocked while this is unknown — re-test the target connection "
                    "and retry."
                )
            elif pg_objects_block is not None and not _pg_state["resolved"]:
                start_btn.props("disable")
                start_btn.tooltip(pg_objects_block[1])
        # Rendered LAST because the remedy re-enables start_btn, which must exist first.
        if pg_objects_block is not None:
            _render_pg_block()
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
        # Spell out that the POSITION survives, not just the infrastructure. "You can
        # restart with Start CDC" left the operator to guess whether stopping cost them
        # their place in the binlog -- and the reasonable guess (that it does, since the
        # connectors are deleted) is wrong. That guess is expensive: it invites re-entering
        # coordinates by hand, or re-running the Full Load, to recover something the
        # connector never lost.
        body = (
            f"This updates the cdc-stack '{stack_name}' to delete the two CDC "
            "connectors and stop streaming. MSK Connect has no pause, so stopping means "
            "deleting the connectors — but MSK, the VPC wiring and the plugins are kept, "
            "and so is the recorded stream position. Start CDC re-creates the connectors "
            "and continues from exactly where streaming stopped: no gap, nothing "
            "re-applied, and no Full Load or start point needed again. You can stop and "
            "restart as often as you like."
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
            ui.button(confirm_label, on_click=_go).props("color=amber-8")  # type: ignore[attr-defined]
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

def _full_load_committed(job, migration_state) -> bool:
    """True when a Full Load has run for this table set (so its lock clause wins).

    Mirrors ``selection_lock_reason``'s FIRST clause (``has_job or status is DONE``),
    which takes precedence over the CDC-infrastructure clause -- and, unlike it, is NOT
    released by deleting the cdc-stack. The distinction decides which remedy the CDC
    notices may offer: telling an operator who has already loaded to "delete the CDC
    infrastructure to re-scope" would cost a ~45 min teardown and leave the picker just
    as locked, because the export really did run against this set (only Start over
    clears that).

    ``job`` is the current Full Load job (the caller already resolved it) -- its mere
    existence is the lock's ``has_job``. A migration type without a ``"full_load"``
    sub-step can never trip that clause, so CDC-only is excluded outright rather than
    inferred. Deliberately conservative: when unsure this returns False, which offers
    the cheaper (delete-and-redeploy) remedy -- wrong-but-recoverable, versus sending
    someone to Start over who did not need it.
    """
    from dsql_migrator.ui.data_migration._models import substeps_for_type

    try:
        if "full_load" not in substeps_for_type(migration_state.migration_type):
            return False
    except Exception:  # noqa: BLE001 - unknown type: fall through to the job check
        pass
    return job is not None


def _sink_mcu_count() -> int:
    """The operator's configured sink MCU count, read FRESH at deploy time.

    Read here rather than captured at import/render so a change made in Settings ->
    Performance -> CDC is picked up by the very next Start CDC without a restart
    (``load_config`` re-reads the environment, which is where ``set_tuning_value``
    writes). Falls back to the template-matching default if the config cannot be
    read, so a config problem can never block a deploy or silently resize a
    connector.
    """
    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT

    try:
        from dsql_migrator.config import load_config

        return int(load_config().cdc_sink_mcu_count)
    except Exception:  # noqa: BLE001 - never block a deploy on a config read
        return CDC_DEFAULT_SINK_MCU_COUNT


def _cdc_fk_connection_factory(session):
    """Return a fresh-DSQL-connection factory for the target, or ``None``.

    Same seam the cut-over FK apply uses (``DsqlConnector(...).connect``), so the
    precondition reads and the drop run against exactly the target that will be
    streamed to.
    """
    target = getattr(session, "target_config", None)
    if target is None:
        return None
    from dsql_migrator.core.target_connection import DsqlConnector

    return DsqlConnector(
        target, aws_profile=getattr(session, "aws_profile", None)
    ).connect


def _probe_cdc_blocking_foreign_keys(migration_state, session, inventory, watermark):
    """Read the target's enforced FKs on the to-be-streamed tables. BLOCKING I/O.

    An enforced foreign key cannot coexist with a live CDC stream: the sink applies
    change records across several tasks with NO parent-before-child ordering, so a child
    row can arrive before its parent, be rejected with SQLSTATE 23503, and be
    dead-lettered PERMANENTLY (the sink treats 23503 as a poison row -- no retry -- while
    offsets advance and the task survives, so the loss is silent). A preceding
    ``Full load only`` run leaves FKs applied AND validated, which is exactly how a
    session that later switches to CDC arrives here.

    Returns a :class:`CdcBlockingForeignKeys`, or ``None`` when there is nothing to check
    (no tables resolved yet -- the caller's own empty-selection guard covers that).

    Opens a DSQL connection, so callers MUST run it off the NiceGUI event loop
    (``run.io_bound`` in the dialog, or the job's worker thread).
    """
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys
    from dsql_migrator.core.target_introspector import target_foreign_keys

    preserved = migration_state.cdc_preserved_foreign_keys()
    tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    streamed = [t.name for t in tables if getattr(t, "name", None)]
    if not streamed:
        return None
    # Check every table this migration gives foreign keys to, not just the streamed set.
    # An FK on a NON-streamed child still blocks: a streamed DELETE on the parent is
    # rejected (23503) because the static child rows still reference it, and that DELETE
    # is then dead-lettered permanently. The extra tables cost one more catalog query each
    # on the SAME connection, and keying by child table keeps ownership matching valid.
    table_names = sorted(set(streamed) | set(preserved))
    connect = _cdc_fk_connection_factory(session)
    if connect is None:
        # No target configured: report every table UNKNOWN rather than "clean", so the
        # gate fails closed (the connection blocker will also fire in the dialog).
        return cdc_blocking_foreign_keys({name: None for name in table_names}, preserved)
    return cdc_blocking_foreign_keys(
        target_foreign_keys(table_names, connection_factory=connect), preserved
    )


def _drop_cdc_blocking_foreign_keys(pairs, session) -> tuple[int, int]:
    """Drop the given ``(table, constraint)`` FKs so CDC can stream. BLOCKING I/O.

    Only ever called with the ``ours`` half of :func:`cdc_blocking_foreign_keys` -- the
    constraints THIS migration's conversion renders, which cut over re-creates with the
    shared idempotent apply pass. Each drop is its own single autocommit DDL with OCC
    retry (DSQL allows one DDL per transaction) and is logged individually under the CDC
    category, so the audit trail names every constraint removed.

    Returns ``(dropped, failed)``.
    """
    from dsql_migrator.core.schema_applier import drop_foreign_key

    connect = _cdc_fk_connection_factory(session)
    if connect is None:
        return (0, len(list(pairs)))
    dropped = failed = 0
    for table_name, constraint_name in pairs:
        target = f"{table_name}.{constraint_name}"
        try:
            drop_foreign_key(table_name, constraint_name, connection_factory=connect)
            dropped += 1
            _log_cdc_event(
                "foreign key removed for CDC",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{target} dropped so the stream cannot dead-letter out-of-order "
                    "child rows (23503); re-created by the cut-over 'Apply foreign "
                    "keys' step"
                ),
            )
        except Exception:  # noqa: BLE001 - report per FK; never abort the whole pass
            failed += 1
            _log_cdc_event(
                "foreign key not removed",
                status=ActivityStatus.FAILURE,
                detail=(
                    f"{target} could not be dropped; remove it manually before "
                    "starting CDC or the stream will dead-letter child rows (23503)"
                ),
            )
    return (dropped, failed)


def _cdc_fk_block_reason(blocking) -> tuple[str, str]:
    """Return the (clause, header) naming what ACTUALLY blocked. Pure.

    ``blocking`` is true for an unreadable catalog too (fail closed), and that is the
    likeliest case in practice -- a transient DSQL connect blip. Claiming "the target
    still has enforced foreign keys" then sends the operator to ``pg_constraint`` to find
    nothing, instead of to the connection test, which is the real remedy.
    """
    if blocking.ours or blocking.foreign:
        return (
            "the target still has enforced foreign keys",
            "Remove the target's foreign keys before streaming",
        )
    return (
        "the target's foreign keys could not be verified",
        "Could not verify the target's foreign keys",
    )


def _cdc_fk_block_body(blocking, *, can_remove_here: bool = True) -> str:
    """Compose the CDC-start FK precondition message. Pure.

    ``can_remove_here`` is False for the consumers that render no Remove control (the job
    error and the activity log), so the copy does not point at a button that is not there.
    """
    parts: list[str] = []
    if blocking.ours:
        removal = (
            "Remove them below"
            if can_remove_here
            else "Reopen Start CDC and use 'Remove foreign keys'"
        )
        parts.append(
            "These foreign keys are enforced on the target and were created by this "
            "migration: "
            + ", ".join(f"{t}.{c}" for t, c in blocking.ours)
            + f". {removal} — cut over re-creates them once the stream has drained."
        )
    if blocking.foreign:
        parts.append(
            "These enforced foreign keys are NOT accounted for by this migration, so "
            "the tool will not remove them (it could not put them back): "
            + ", ".join(f"{t}.{c}" for t, c in blocking.foreign)
            + ". Drop them yourself before starting CDC."
        )
    if blocking.unknown_tables:
        parts.append(
            "The target's constraints could not be read for: "
            + ", ".join(blocking.unknown_tables)
            + ". Re-test the target connection and retry — CDC is blocked while this "
            "is unknown, because an undetected foreign key silently discards rows."
        )
    parts.append(
        "Why: the sink applies change records across several tasks with no "
        "parent-before-child ordering, so a child row can arrive first, be rejected "
        "with SQLSTATE 23503, and be dead-lettered permanently — the task keeps "
        "running and offsets advance, so the loss is silent."
    )
    return " ".join(parts)


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
    watermark = _cdc_watermark(job)
    override = migration_state.cdc_start_override()
    exclusions = migration_state.lob_exclusions()
    exclude_value = format_column_exclude_list(
        {table: sorted(cols) for table, cols in exclusions.items()}
    )
    exclude_list = exclude_value.split(",") if exclude_value else None
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    _resume_override, _force_initial_snapshot = _cdc_resume_signal(migration_state, session)
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
    # Composite-PK re-key: scope the stored key map to the tables actually being
    # replicated, then gate the ONE precondition -- a composite key column must not
    # be dropped at capture (column.exclude.list), or Debezium can't build the key.
    selected_names = {t.name for t in tables_for_config}
    message_key_columns = {
        table: cols
        for table, cols in migration_state.cdc_message_key_columns().items()
        if table in selected_names
    }
    bad_key_cols = composite_cdc_excluded_key_columns(
        message_key_columns, exclude_list or []
    )
    if bad_key_cols:
        render_notice(
            ui,
            tone="error",
            header="Composite key column is excluded from capture",
            body=(
                "These composite primary-key columns are in the column exclude list, "
                "so Debezium cannot read them to build the record key: "
                + ", ".join(bad_key_cols)
                + ". Remove them from the LOB-exclusion selection before starting CDC "
                "(a key column is small and safe to capture)."
            ),
        )
        return
    # FIX 1 (PostgreSQL): a COMPOSITE_KEY re-key prepends a non-PK "leading" column to the
    # CDC record key. Under REPLICA IDENTITY DEFAULT the UPDATE/DELETE before-image carries
    # only the source PK, so the sink cannot build the re-keyed DELETE and the delete is
    # silently lost. The tool sets REPLICA IDENTITY FULL on exactly those tables during Full
    # Load provisioning (cdc_pg_slot); surface the requirement here so a CDC-only start
    # (no Full Load in this session) knows it must be set on the source first. MySQL is
    # immune (its binlog before-image is the full row), so this is PostgreSQL-only.
    if _cdc_source_type(session) is SourceType.POSTGRES:
        from dsql_migrator.core import cdc_pg_slot as _pg_slot

        # The publication/slot precondition used to be WARNED about here and decided from
        # the watermark. Both halves were wrong. The watermark is not the record of
        # provisioning it claimed to be -- it dies with an app restart and SURVIVES a
        # Delete-infra that DROPS both objects -- and the advice "or create them on the
        # source with the same names" is, at CDC-start time, the defect itself: a slot
        # created now starts at the CURRENT WAL, so resuming from it silently skips every
        # change since the load. The check is now a source PROBE that BLOCKS, in the Start
        # dialog (with a one-click re-snapshot remedy) and again on the worker thread just
        # before anything is created -- see _probe_pg_replication_objects and work() below.
        _rekeyed_full = _pg_slot.rekeyed_tables_needing_full_identity(
            message_key_columns,
            {t.name: list(t.primary_key) for t in tables_for_config},
        )
        if _rekeyed_full:
            # WARNING, not info, and it no longer says the tool has handled it. The old copy
            # read "The tool sets REPLICA IDENTITY FULL on them during Full Load" -- true
            # ONLY when the Full Load provisions the slot, which a Full-load-ONLY run does
            # not do. An operator who had in fact run Full Load therefore concluded it was
            # handled, started CDC, and lost every delete on the re-keyed table silently
            # (live-observed on Aurora PostgreSQL 17.7). The authoritative gate is now the
            # REPLICA_IDENTITY_COVERS_KEY prerequisite, which reads the source's actual
            # relreplident and BLOCKS; this notice only points at it, so the two can never
            # disagree about whether the source is ready.
            render_notice(
                ui,
                tone="warning",
                header="Re-keyed tables need REPLICA IDENTITY FULL on the source",
                body=(
                    "These tables are keyed on a composite (re-keyed) primary key, so a "
                    "DELETE must carry the added leading key column or it is applied to 0 "
                    "rows and silently lost: " + ", ".join(_rekeyed_full) + ". The tool sets "
                    "REPLICA IDENTITY FULL during Full Load ONLY when that run also "
                    "provisions the replication slot — a Full-load-only run does not, and "
                    "neither does a CDC-only start. Run the CDC prerequisite checks: they "
                    "read the source's actual REPLICA IDENTITY and block the start with the "
                    "exact ALTER TABLE to run."
                ),
            )
    # FIX 4: SeedMode is engine-aware -- the SINGLE source of truth shared with the infra
    # build. A PostgreSQL stack is always created SeedMode=External, so Start must run the
    # in-process External Kafka prep regardless of the host's cdc_seed_mode; MySQL keeps
    # deriving from the host config (unchanged).
    from dsql_migrator.config import load_config as _load_config

    _host_cfg = _load_config()
    seed_mode = _cdc_start_seed_mode(_cdc_source_type(session), _host_cfg.cdc_seed_mode)
    # The External prep runs IN-PROCESS over the MSK IAM bootstrap, so a deployment
    # without 9098 egress + data-plane kafka-cluster IAM cannot do it at all. This used
    # to WARN ("this host does not appear to be in-VPC") and proceed, which was both
    # wrong on Fargate -- the task IS in the cdc-stack VPC; it is simply not admitted
    # and holds no data-plane IAM -- and useless, because the seed then died a minute
    # later with a raw KafkaTimeoutError. Block instead: nothing has been submitted yet.
    # Keyed on the DEPLOYMENT's declared capability, NOT on cdc_seed_mode: Fargate must
    # keep cdc_seed_mode="lambda" so MySQL keeps using the cdc-stack's in-VPC seeder
    # Lambda, so the old condition fired even on a correctly equipped app stack. Whether
    # the DEPLOYED cdc-stack admits this host is a separate, fact-based check in
    # cdc_deployer._run_external_seed.
    if seed_mode == "external":
        from dsql_migrator.core.cdc import msk_seed_capability_blocker

        _seed_blocker = msk_seed_capability_blocker(
            cdc_seed_mode=_host_cfg.cdc_seed_mode,
            cdc_msk_access=getattr(_host_cfg, "cdc_msk_access", False),
        )
        if _seed_blocker:
            render_notice(
                ui,
                tone="error",
                icon="vpn_lock",
                header="This deployment cannot start PostgreSQL CDC",
                body=(
                    _seed_blocker
                    + " No connectors were created and the cdc-stack is unchanged."
                ),
            )
            return
    source_config = dispatch_source_config(
        _cdc_source_type(session),
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        database=_cdc_source_database(session),
        stack_name=migration_state.cdc_stack_name,
        column_exclude_list=exclude_list,
        resume_override=_resume_override,
        force_initial_snapshot=_force_initial_snapshot,
        message_key_columns=message_key_columns,
        # publication.autocreate.mode is NOT passed: it is derived from the snapshot mode
        # (pg_publication_autocreate_mode). It used to come from a separate piece of UI
        # state that the start-position radio and session restore never set, which is how
        # snapshot.mode=initial + autocreate=disabled -- guaranteed connector death, after
        # both connectors are billed -- became reachable.
    )
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC
    )
    target, region = _cdc_target_region(ui, session)
    if region is None:
        return
    params = dispatch_cdc_stack_params(
        source_config, sink_config,
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=migration_state.cdc_stack_name,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        sink_mcu_count=_sink_mcu_count(),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name
    aws_profile = getattr(session, "aws_profile", None)
    assume_role_arn = getattr(migration_state, "cdc_deploy_role_arn", None)

    def work(handle) -> None:
        # Everything blocking (the deploy-role AssumeRole in the deployer build, the STS
        # account lookup, the ~50 KiB template read, the config load) runs HERE on the
        # job's worker thread -- NEVER on the NiceGUI event loop. On Fargate one asyncio
        # loop serves every browser session, so a blocking call on it freezes them all;
        # the sibling _start_cdc_infra_deploy offloads for exactly this reason.
        # BACKSTOP for the foreign-key precondition, re-checked on the worker thread
        # right before anything is created. The Start dialog already blocks on this, but
        # "Retry CDC" (_render_cdc_partial_actions) calls this job with NO dialog, and a
        # cached dialog verdict can be stale. An enforced FK makes the sink dead-letter
        # out-of-order child rows PERMANENTLY and SILENTLY (23503 is not retriable), so
        # refuse rather than stream into data loss. Fails closed: an unreadable catalog
        # counts as blocking.
        fk_state = _probe_cdc_blocking_foreign_keys(
            migration_state, session, inventory, watermark
        )
        if fk_state is not None and fk_state.blocking:
            _detail = _cdc_fk_block_body(fk_state, can_remove_here=False)
            _log_cdc_event(
                "start blocked by target foreign keys",
                status=ActivityStatus.FAILURE,
                detail=_detail,
            )
            raise RuntimeError(
                f"Start CDC blocked: {_cdc_fk_block_reason(fk_state)[0]}. " + _detail
            )
        # BACKSTOP for the PostgreSQL publication/slot precondition, on the worker thread
        # right before anything is created. The Start dialog already blocks on this, but
        # "Retry CDC" (_render_cdc_partial_actions) calls this job with NO dialog, and a
        # cached dialog verdict can be stale. It must live HERE and not in
        # _start_cdc_deploy's body: that runs ON the NiceGUI event loop and this probe is
        # blocking source I/O. Fails OPEN on an unreadable source (the probe returns
        # None) -- unlike the FK gate, because a missing publication kills the connector
        # LOUDLY whereas an enforced FK loses rows silently.
        _pg_block = _pg_objects_gate_block(
            migration_state, session, inventory, watermark
        )
        if _pg_block is not None:
            _log_cdc_event(
                "start blocked by missing PostgreSQL replication objects",
                status=ActivityStatus.FAILURE,
                detail=_pg_block[1],
            )
            raise RuntimeError(f"Start CDC blocked: {_pg_block[0]}. {_pg_block[1]}")
        deployer = build_cdc_stack_deployer(
            region, aws_profile=aws_profile, assume_role_arn=assume_role_arn
        )
        # The cdc-stack template exceeds CFn's 51,200-byte inline limit, so the deployer
        # stages it in S3 via TemplateURL. Derive the bucket name from the deterministic
        # naming convention (same bucket the infra deploy created).
        from dsql_migrator.core.s3_provision import plugin_bucket_name as _pbucket
        try:
            _acct = deployer._client("sts").get_caller_identity()["Account"]  # type: ignore[attr-defined]
            deployer.template_s3_bucket = _pbucket(_acct, region)
        except Exception:  # noqa: BLE001 — best-effort; a later step fails with a clear message
            pass
        template_body = _read_cdc_template_body()
        # SeedMode is derived ENGINE-AWARE above (FIX 4): a PostgreSQL source forces
        # "external" (its stack is always SeedMode=External) so the in-process Kafka prep
        # runs; MySQL keeps the host config ("Host is the mode": the in-VPC EC2 host sets
        # DSQL_MIGRATOR_CDC_SEED_MODE=external, Fargate/local leave it -> "lambda").
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
            template_body=template_body,
            seed_mode=seed_mode,
        )

    _action = "start CDC connectors"
    # The RESUME POINT is the fact this line exists for: it is what makes the Full
    # Load -> CDC handoff gapless, and it cannot be reconstructed afterwards (the
    # connector's own offsets are consumed and the deploy log is per-action and
    # ephemeral). The detail was the stack name alone, so a downloaded activity log
    # recorded that CDC was started but not FROM WHERE.
    _detail = _cdc_start_detail(
        stack_name,
        watermark,
        mode=mode,
        source_type=_cdc_source_type(session),
        force_snapshot=_force_initial_snapshot,
        exclusions=exclusions,
    )
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="start")
    # Count per-table applied-ops (I/U/D) only from THIS migration's Full Load
    # watermark onward, so the monitor shows post-Full-Load CDC events -- not stale
    # ops from prior CDC runs still inside the metric's trailing window. No watermark
    # (CDC-only / manual) -> now (count from this CDC start).
    migration_state.set_cdc_ops_window_start(
        watermark.snapshot_timestamp if watermark is not None else None
    )
    _log_cdc_event(_action, detail=_detail)
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

    # HostSubnetCidr: the network the cdc-stack admits on MSK 9098 so a
    # SeedMode=External (PostgreSQL, or the in-VPC EC2 host) Start can seed in-process.
    # Re-resolved here rather than trusting the dialog's verdict -- the dialog may have
    # been open for minutes and its EC2 describe can since have been throttled or
    # denied, and deploying with an empty value would create a cluster nothing admits.
    # Checked FIRST, before the network diagnosis and the Secrets Manager upsert, so a
    # refusal leaves nothing behind. Explicit DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR wins
    # inside msk_seed_admission (so the EC2 host is byte-identical); a Lambda-seeded
    # deploy resolves to "" -> no ingress rule, unchanged.
    _admission = (
        await run.io_bound(
            _msk_seed_admission, migration_state, session, vpc_id=fields["vpc_id"]
        )
        or MskSeedAdmission()
    )
    if _admission.blocker:
        # "negative", matching the dialog's error tone for the same string: this is a
        # refusal, and the deploy does not happen.
        ui.notify(_admission.blocker, type="negative", position="top")  # type: ignore[attr-defined]
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
    if connector_subnet_ids:
        # User-supplied subnets: verify they have NAT egress. MSK Connect assigns
        # private IPs only, so IGW-only subnets cannot reach Secrets Manager or
        # any HTTPS AWS endpoint, causing a silent 10-minute deploy failure.
        def _verify_manual_subnets():
            from dsql_migrator.core.ec2_metadata import (
                build_ec2_client,
                verify_subnet_egress,
            )
            ec2 = build_ec2_client(aws_profile, region)
            return verify_subnet_egress(ec2, connector_subnet_ids.split(","))

        try:
            egress_ok, egress_reason = await run.io_bound(_verify_manual_subnets)
        except Exception:  # noqa: BLE001 — best-effort; don't block on EC2 errors
            egress_ok = True  # assume OK if we can't verify
            egress_reason = ""
        if not egress_ok:
            ui.notify(  # type: ignore[attr-defined]
                egress_reason, type="negative", position="top",
            )
            return

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
            # Double-check: verify the discovered subnets still have NAT egress
            # at this moment (they may have lost it if the owning stack was
            # deleted between diagnosis and now — race condition).
            def _verify_discovered():
                from dsql_migrator.core.ec2_metadata import (
                    build_ec2_client,
                    verify_subnet_egress,
                )
                ec2 = build_ec2_client(aws_profile, region)
                return verify_subnet_egress(ec2, connector_subnet_ids.split(","))

            try:
                disc_ok, disc_reason = await run.io_bound(_verify_discovered)
            except Exception:  # noqa: BLE001
                disc_ok = True
                disc_reason = ""
            if not disc_ok:
                ui.notify(  # type: ignore[attr-defined]
                    disc_reason, type="negative", position="top",
                )
                return
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
    watermark = _cdc_watermark(job)
    override = migration_state.cdc_start_override()
    exclusions = migration_state.lob_exclusions()
    exclude_value = format_column_exclude_list(
        {table: sorted(cols) for table, cols in exclusions.items()}
    )
    exclude_list = exclude_value.split(",") if exclude_value else None
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    _resume_override, _force_initial_snapshot = _cdc_resume_signal(migration_state, session)
    mode = migration_state.cdc_start_mode()
    source_config = dispatch_source_config(
        _cdc_source_type(session),
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        database=_cdc_source_database(session),
        stack_name=migration_state.cdc_stack_name,
        column_exclude_list=exclude_list,
        resume_override=_resume_override,
        force_initial_snapshot=_force_initial_snapshot,
    )
    # Infra-only deploy (DeploySink=false, no connector yet): an empty table
    # selection is allowed here -- SinkTopics is populated later at Start CDC.
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC, allow_empty=True
    )
    # Size-proportional topic partitions (skewed-workload fix): weight Kafka
    # partitions toward the largest tables so a hot table is not serialized on a
    # single sink task. Partition counts are fixed at topic-creation (which happens
    # at Start CDC, but the source connector reads these persisted params), so they
    # must be decided here at create. Prefer the Full Load watermark's scan-free
    # estimates; if absent (infra deployed before Full Load), fetch fresh
    # information_schema estimates off the loop. Best-effort -> None -> uniform.
    row_counts_by_table = _cdc_row_counts_from_watermark(watermark, tables_for_config)
    if not row_counts_by_table:
        row_counts_by_table = await run.io_bound(
            _estimate_cdc_table_rows, session, [t.name for t in tables_for_config]
        )
    # (c) source-DB security group. Scope the connector's egress-to-source rule to
    #     the source DB's own SG so the stack does NOT fall back to an open
    #     0.0.0.0/0 egress on the source port. Best effort + off the loop: if the
    #     user supplied one it wins; otherwise look it up from RDS (read-only). A
    #     non-RDS host / missing rds:DescribeDBInstances just leaves it empty (the
    #     stack then uses the documented 0.0.0.0/0 fallback, as before).
    source_db_security_group_id = (fields.get("source_db_security_group_id") or "").strip()
    if not source_db_security_group_id:
        # source_db_hostname was prefilled from the source config by _cdc_infra_prefill.
        source_host = fields.get("source_db_hostname", "")

        def _lookup_source_sg():
            from dsql_migrator.core.rds_metadata import (
                build_rds_client,
                fetch_source_security_group_id,
                parse_rds_region,
            )

            sg_region = parse_rds_region(source_host)
            if sg_region is None:
                return None
            client = build_rds_client(aws_profile, sg_region)
            return fetch_source_security_group_id(client, source_host)

        if source_host:
            try:
                source_db_security_group_id = (
                    await run.io_bound(_lookup_source_sg)
                ) or ""
            except Exception:  # noqa: BLE001 - optional; fall back to open egress
                source_db_security_group_id = ""

    # Plugin bucket + keys are left EMPTY here; the deploy job ensures the managed
    # bucket, uploads the bundled artifacts, and patches these in before create.
    # "Host is the mode": the in-VPC EC2 host sets DSQL_MIGRATOR_CDC_SEED_MODE=external
    # (and DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR to its own subnet) so the cdc-stack is
    # CREATED as SeedMode=External (no in-VPC seeder Lambda) admitting the host on
    # 9098. Fargate/local leave both unset -> Lambda mode, no ingress (unchanged).
    from dsql_migrator.config import load_config as _load_config

    _cfg = _load_config()
    params = dispatch_cdc_infra_params(
        source_config, sink_config,
        vpc_id=fields["vpc_id"],
        connector_subnet_ids=connector_subnet_ids,
        nat_public_subnet_id=nat_public_subnet_id,
        private_subnet_cidr_a=private_subnet_cidr_a,
        private_subnet_cidr_b=private_subnet_cidr_b,
        private_subnet_az_a=private_subnet_az_a,
        private_subnet_az_b=private_subnet_az_b,
        source_db_security_group_id=source_db_security_group_id,
        plugin_bucket_arn="",
        debezium_plugin_s3_key="",
        dsql_sink_plugin_s3_key="",
        source_db_hostname=fields.get("source_db_hostname", ""),
        source_db_port=int(getattr(
            getattr(session, "source_config", None), "port", 3306
        ) or 3306),
        source_secret_arn=source_secret_arn,
        source_secret_name=source_secret_name,
        dsql_cluster_arn=fields["dsql_cluster_arn"],
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=migration_state.cdc_stack_name,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        row_counts_by_table=row_counts_by_table,
        sink_mcu_count=_sink_mcu_count(),
        seed_mode=_cfg.cdc_seed_mode,
        host_subnet_cidr=_admission.cidr,
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

    _action = "deploy CDC infrastructure"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="infra")
    _log_cdc_event(_action, detail=_detail)
    ui.notify("Infrastructure deploy started (~5 min).", type="positive", position="top")  # type: ignore[attr-defined]
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

    _action = "stop CDC connectors"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="stop")
    # Durable marker → the persistent cross-view "teardown in progress" banner (so
    # navigating away from the CDC step doesn't hide the running stop). Ownership
    # guard: don't clobber a DIFFERENT teardown still running (rare two-tab race).
    if should_replace_teardown_marker(
        job_manager, migration_state.cdc_teardown_job_id, job_id
    ):
        migration_state.set_cdc_teardown(
            job_id,
            kind="stop",
            stack=stack_name,
            ctx={
                "region": region,
                "role_arn": getattr(migration_state, "cdc_deploy_role_arn", None),
                "profile": getattr(session, "aws_profile", None),
                "cleanup_secret": False,
            },
        )
    _log_cdc_event(_action, detail=_detail)
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
            # The credentials this process already holds, so dropping the PostgreSQL
            # replication slot does not depend on secretsmanager:GetSecretValue (which the
            # app's identity is not granted for the tool-managed CDC secret).
            source_credentials=_session_source_credentials(session),
        )

    _action = "delete CDC infrastructure"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="delete")
    # Clear any latched redeploy answer: this teardown must prompt again when it lands,
    # otherwise a deploy -> delete -> delete sequence silently reuses the earlier "yes"
    # and drops the operator straight back onto the deploy form.
    migration_state.set_cdc_redeploy_confirmed(False)
    # Durable marker → the persistent cross-view "teardown in progress" banner (the
    # delete runs ~15–45 min; the banner keeps it visible on every step, not just
    # the CDC card the user may navigate away from). Ownership guard: don't clobber a
    # DIFFERENT teardown still running (rare two-tab race).
    if should_replace_teardown_marker(
        job_manager, migration_state.cdc_teardown_job_id, job_id
    ):
        migration_state.set_cdc_teardown(
            job_id,
            kind="delete",
            stack=stack_name,
            ctx={
                "region": region,
                "role_arn": getattr(migration_state, "cdc_deploy_role_arn", None),
                "profile": aws_profile,
                "cleanup_secret": cleanup_secret,
            },
        )
    _log_cdc_event(_action, detail=_detail)
    ui.notify("Delete CDC infrastructure submitted.", type="warning", position="top")  # type: ignore[attr-defined]
    refresh()

def _render_cdc_deploy_live(ui, migration_state, job_manager, refresh) -> None:
    """Render the active lifecycle job's stages + event log; poll while running.

    The displayed stage labels + terminal messages adapt to which operation
    (``cdc_action_kind``) is running. When the job finishes it re-probes the
    stack phase and triggers a full ``refresh`` so the card flips to the next
    action (e.g. infra-deploy DONE → Start button appears).
    """
    # The deploy log expansion is rebuilt on every 5s poll -- both by the inner
    # refreshable AND by the OUTER CDC panel poll (``_poll_cdc``), which re-invokes
    # this whole function. A local dict would be recreated on that outer rebuild and
    # snap an opened log shut every few seconds, so anchor the open/closed state on
    # the session-scoped migration state (survives every level of re-render).
    log_state = migration_state.cdc_deploy_log_ui_state

    @ui.refreshable
    def _deploy_live() -> None:  # type: ignore[misc]
        job = _current_job(job_manager, migration_state.cdc_deploy_job_id)
        if job is None:
            return
        kind = getattr(migration_state, "cdc_action_kind", None) or "start"
        # "Refresh now" forces an immediate poll of the same live region (the 5s
        # timer keeps running too); reuses _poll_deploy so a finished job also
        # advances the card, identical to the automatic tick.
        _render_deploy_stages(
            ui, job, kind, on_refresh=_poll_deploy,
            has_seeder_lambda=_cdc_stack_has_seeder_lambda(migration_state),
        )
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
                # ``job_error`` is the REAL reason -- the CloudFormation failure text the
                # deployer already extracted -- and it was read just above only to ask
                # whether a restart interrupted the job, then discarded in favour of the
                # generic per-kind sentence. So an operator whose stack failed for a
                # nameable reason (a quota, a subnet without egress, an IAM gap) was shown
                # boilerplate recommending a second Delete. Lead with the generic sentence
                # (it carries the next action) and append what AWS actually said.
                render_notice(
                    ui,
                    tone="error",
                    header=f"{fail_noun} failed"
                    + (f" (after {total})" if total else ""),
                    body=(
                        f"{fail_msg} Reported cause: {job_error}"
                        if job_error
                        else fail_msg
                    ),
                )

    def _poll_deploy() -> None:
        job = _current_job(job_manager, migration_state.cdc_deploy_job_id)
        # When the operation finishes, re-probe the stack so the lifecycle card advances
        # to the next action; otherwise just refresh the live region.
        #
        # ``job is None`` (how _current_job reports a lost job record) must take the
        # refresh() branch too: _deploy_live early-returns on a missing job, so refreshing
        # only the live region emptied the card and left it blank forever. A full refresh
        # re-probes the stack, so a lost job resolves from live AWS state instead.
        if job is None or job.status not in ("PENDING", "RUNNING"):
            refresh()
        else:
            _deploy_live.refresh()

    _deploy_live()

def _render_deploy_stages(
    ui, job, kind: str = "start", on_refresh=None, *, has_seeder_lambda=None
) -> None:
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
    # Delete shows no per-stage ETA: its dominant stage (stack_delete) waits on
    # unpredictable ENI reclamation, so a "~5 min" hint on it is misleading. The
    # upper-bound line under the title carries the expectation instead. An empty map
    # makes every _format_eta_hint(...) below return "" for delete.
    etas = {} if kind == "delete" else _CDC_STAGE_ETA_SECONDS.get(kind, {})
    now = datetime.now(timezone.utc)
    running = job.status in ("PENDING", "RUNNING")
    with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        ui.label(_CDC_ACTION_TITLE.get(kind, "Progress")).classes(  # type: ignore[attr-defined]
            "text-sm font-semibold"
        )
        # Whole-operation hint. Delete is special: its wall-clock is dominated by
        # AWS reclaiming the in-VPC seeder Lambda's ENIs before the MSK cluster can go,
        # which is unpredictable and has been measured at ~20+ min against the old
        # ~5 min estimate. A precise-looking "est. ~5 min remaining" that overshoots by
        # 4x reads as a stuck UI, so for a delete show an honest UPPER BOUND ("up to
        # ~20 min") instead of a countdown. Other operations keep the summed ETA.
        if running:
            if kind == "delete":
                ui.label(  # type: ignore[attr-defined]
                    "can take "
                    + cdc_teardown_estimate(has_seeder_lambda=has_seeder_lambda)
                ).classes("text-xs text-gray-400")
            else:
                remaining_total = sum(
                    etas.get(c.chunk_id, 0) for c in job.chunks if c.status != "DONE"
                )
                total_hint = _format_eta_hint(remaining_total)
                if total_hint:
                    ui.label(f"est. {total_hint} remaining").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-400"
                    )
        if running and on_refresh is not None:
            # Manual refresh button only (the header's spinning icon + deploying
            # badge already signals the operation is live; a redundant spinner +
            # "Auto-refreshing…" text was just visual clutter).
            ui.space()  # type: ignore[attr-defined]
            ui.button(on_click=on_refresh).props(  # type: ignore[attr-defined]
                "flat dense round size=sm icon=refresh"
            ).tooltip("Refresh now")
    for chunk in job.chunks:
        # When the job itself has ended (FAILED/DONE), any stage still marked
        # IN_PROGRESS was interrupted — show it as FAILED so the spinner stops
        # and the user sees a definitive state, not a stale hourglass.
        effective_status = chunk.status
        if not running and chunk.status == "IN_PROGRESS":
            effective_status = "FAILED"
        icon, color = _CDC_DEPLOY_STAGE_STYLE.get(effective_status, _CDC_DEPLOY_STAGE_STYLE["PENDING"])
        label = labels.get(chunk.chunk_id, chunk.chunk_id)
        in_progress = effective_status == "IN_PROGRESS"
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
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}"):
        # ASCII-only separator ("-"), and sanitize each message, so the monospace
        # ui.code font never renders a missing-glyph box (tofu) for punctuation
        # like the em-dash / ellipsis some deploy messages contained.
        # Timestamps are UTC (the deploy driver stamps datetime.now(timezone.utc)).
        # Show the zone explicitly so a line reads unambiguously and matches the
        # downloaded activity log / CloudWatch / CloudFormation events (all UTC).
        text = "\n".join(
            f"{ts.strftime('%H:%M:%S')} UTC - {_ascii_log(msg)}" for ts, msg in log_lines
        )
        ui.code(text).classes("w-full text-xs")  # type: ignore[attr-defined]



# The post-start CDC monitoring / DLQ / status render panels were extracted to
# _cdc_monitoring.py for maintainability (after the _cdc_state predicate split, so the
# panels import those predicates from _cdc_state, not back from here -- no cycle).
# Re-exported so _cdc_ui.<name>, _render_cdc_step's calls, and every consumer/test import
# resolve unchanged.
from dsql_migrator.ui.data_migration._cdc_monitoring import (  # noqa: E402,F401
    _DLQ_LEVEL_TONE,
    _DLQ_RECORD_LIST_LIMIT,
    _DLQ_RECORD_PAGE_SIZE,
    _DRIFT_LABELS,
    _dlq_panel_tone,
    _open_add_column_dialog,
    _render_cdc_dlq_breakdown,
    _render_cdc_dlq_panel,
    _render_cdc_dlq_records,
    _render_cdc_error_download,
    _render_cdc_handling_panel,
    _render_cdc_live_monitoring,
    _render_cdc_lob_exclusion_panel,
    _render_cdc_pipeline_health,
    _render_cdc_schema_drift_banner,
    _render_change_flow_status,
    _render_full_load_quarantine_pointer,
    _render_migration_table_status,
    exclude_and_reload_block_reason,
    lob_exclusion_lock,
)
