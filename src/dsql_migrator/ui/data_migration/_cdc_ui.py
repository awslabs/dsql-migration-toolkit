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
    build_cdc_infra_params,
    build_cdc_stack_name,
    build_cdc_stack_params,
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
from dsql_migrator.core.models import (
    LoadStatusView,
    MigrationMode,
    SourceInventory,
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
    cdc_prerequisite_block_reason,
    connector_health_rows,
    connector_role_label,
    format_column_exclude_list,
    format_duration,
    lob_exclusion_candidates,
    per_table_counts_notice_body,
)
from dsql_migrator.ui.data_migration._status import (
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


# How often the CDC step polls MSK Connect + the DSQL target for live status.
# Slower than the Full Load poll: these are network round-trips to AWS/DSQL, and
# connector state / replication lag change on the order of seconds, not 0.5s.
_CDC_POLL_INTERVAL_SECONDS = 5.0


def _cdc_is_streaming(migration_state) -> bool:
    """True when a CDC controller + connector names are wired (pipeline is live).

    The single gate for arming the ~5 s CDC poll timers: both the live-status
    region and the per-table net-rows table re-render on this cadence only while
    streaming, and stay static (manual refresh only) otherwise. Duck-typed so the
    UI double and a bare state object both work.
    """
    return getattr(migration_state, "cdc_controller", None) is not None and bool(
        getattr(migration_state, "cdc_connector_names", []) or []
    )


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
    # *extra* entry point -- offered there only so the ~15-20 min MSK create can
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
        )

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

def cdc_pipeline_live(migration_state) -> bool:
    """True only when CDC connectors actually exist and are streaming.

    The narrow, no-false-positive signal: connectors have been detected (a live
    controller with connector names), or the cdc-stack phase is ``running``. Unlike
    :func:`cdc_streaming_started`, an *in-flight start job* does NOT count here --
    the connectors are still coming up (~10-20 min on MSK Connect) and no row has
    reached the target yet.

    Use this for anything that asserts CDC is DONE / data has flowed (e.g. promoting
    the Data Migration step, which drives the "Success" badge and unlocks
    Validation). Use :func:`cdc_streaming_started` instead for "the start point is
    committed, stop editing inputs", which must latch the moment Start is pressed.
    """
    controller = getattr(migration_state, "cdc_controller", None)
    names = getattr(migration_state, "cdc_connector_names", []) or []
    if controller is not None and names:
        return True  # connectors detected -> streaming
    return getattr(migration_state, "cdc_stack_phase", None) == "running"


def cdc_infra_deploy_in_flight(migration_state, job_manager) -> bool:
    """True while this session's cdc-stack CREATE job is PENDING/RUNNING.

    Deliberately DISTINCT from :func:`cdc_streaming_started`, which excludes an ``infra``
    job: that predicate answers "are CDC's inputs committed / is anything streaming?"
    (it gates promoting the step to DONE, which an infra create must never do). This one
    answers "is a long, billable provisioning run under way?" -- the question the
    migration-type lock and the oversized-LOB exclusion lock both need, since
    ``ColumnExcludeList`` is baked into the stack at create time and the create's only
    progress/teardown controls live on the CDC sub-step.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    if getattr(migration_state, "cdc_action_kind", None) != "infra":
        return False
    job = _current_job(job_manager, getattr(migration_state, "cdc_deploy_job_id", None))
    return job is not None and job.status in ("PENDING", "RUNNING")


def cdc_streaming_started(migration_state, job_manager) -> bool:
    """True once CDC has been started, so its inputs must no longer change.

    "Started" means the pipeline is live (:func:`cdc_pipeline_live` -- detected
    connectors or cdc-stack phase ``running``), or a **connector-level** CDC
    lifecycle job (start/stop/delete) is in flight. After this point the start
    position is already seeded into the MSK connect-offsets topic and the table set
    is fixed by the running source connector, so editing the CDC start point or the
    table selection would have no effect on the live pipeline and only mislead the
    operator. Mirrors the "running" detection in :func:`_render_cdc_start_action`.
    Read-only/best-effort.

    This latches earlier than :func:`cdc_pipeline_live` on purpose: an in-flight
    connector start job counts, because the inputs are already committed even though
    the connectors have not reached RUNNING yet. It is therefore the wrong signal
    for "data has arrived" -- use :func:`cdc_pipeline_live` for that (promoting the
    Data Migration step / the "Success" badge).

    An in-flight ``kind="infra"`` job is deliberately EXCLUDED: the infrastructure
    create (``create_stack``: MSK Serverless, networking, plugins, IAM) makes no
    connectors -- the template gates both on ``HasBootstrapServers``, which the
    infra pass leaves blank -- so nothing is streaming and no offset is seeded for
    the ~15-20 min it runs. Treating it as "streaming" made an infra deploy
    masquerade as a live pipeline: it promoted the Data Migration step to DONE
    (unlocking Validation with zero rows loaded, and never downgrading), disabled
    Start Full Load, froze the table picker, and silently turned a "Drop & reload"
    re-run into an append. Excluding it is also what lets the ~15-20 min MSK create
    overlap the Full Load instead of serializing after it.
    """
    if cdc_pipeline_live(migration_state):
        return True
    # Only a connector-level lifecycle job counts (start/stop/delete); an "infra"
    # create touches no connector, so it must not read as streaming.
    if getattr(migration_state, "cdc_action_kind", None) == "infra":
        return False
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
        wm_gtid_only=wm_gtid_only,
        resumes_from_offset=resumes_from_offset,
    )

    # On a restart the connector config below is not what decides the start position (the
    # committed offset is), so an absent effective_resume must not hide the rest of the
    # card -- it is exactly the state a resume renders in.
    if effective_resume is None and not resumes_from_offset:
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

        source_config = getattr(session, "source_config", None)
        has_password = getattr(session, "source_password", None) is not None
        if source_config is None or not session.has_source() or not has_password:
            return None
        engine = make_source_engine_factory(session.source_password)(source_config)
        with engine.connect() as connection:
            estimates = estimate_source_rows(connection, list(table_names))
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
    """
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon(  # type: ignore[attr-defined]
                "play_circle",
                color="primary" if effective_resume is not None else "grey",
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
            elif resumes_from_offset or effective_resume is not None:
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
            # Read-only AND clearly greyed: a bare Quasar `disable` dims too subtly
            # to read as "locked", so mute the whole choice (opacity + not-allowed
            # cursor) to match the "Locked" badge.
            radio.props("disable").classes(
                "opacity-50 pointer-events-none cursor-not-allowed"
            )
        if not wm_usable and mode == "auto" and not locked:
            if wm_gtid_only:
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
            _render_cdc_manual_inputs(
                ui, migration_state, refresh, locked=locked, session=session,
            )
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
        """Query SHOW MASTER STATUS on the source and fill the input fields."""
        from nicegui import run
        from dsql_migrator.ui.connect import make_source_engine_factory
        from sqlalchemy import text

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
                row = conn.execute(text("SHOW MASTER STATUS")).mappings().first()
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
                "SHOW MASTER STATUS returned no data — binary logging may be "
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

def cdc_teardown_badge(migration_state, job_manager) -> Optional[tuple[str, str]]:
    """``(badge_text, color)`` while a teardown is in flight, else ``None``.

    The CDC pipeline card derives its badge from the live connector phase, and a
    teardown does not remove the connectors instantly: CloudFormation is still
    deleting them, so discovery keeps reporting both as RUNNING and
    :func:`classify_cdc_card_phase` keeps returning ``"running"``. Because the badge
    chain tested ``phase == "running"`` FIRST, pressing "Delete CDC infrastructure"
    left a green **"Streaming"** pinned next to a card whose body already said
    "Deleting infrastructure" -- two contradictory verdicts, and the reassuring one
    was the wrong one (nothing will be streaming shortly).

    A teardown is therefore checked BEFORE the phase: the operator's own committed
    action outranks a connector state that is only true for another minute or two.
    Names which teardown it is, since Stop CDC (connectors only, MSK kept) and Delete
    infrastructure (everything) leave very different systems behind.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    kind = getattr(migration_state, "cdc_action_kind", None)
    if kind not in ("delete", "stop"):
        return None
    job = _current_job(job_manager, getattr(migration_state, "cdc_deploy_job_id", None))
    if job is None or job.status not in ("PENDING", "RUNNING"):
        return None
    # "primary" (not positive/warning): a teardown the operator asked for is a normal
    # in-progress operation, not a fault.
    return ("Deleting…", "primary") if kind == "delete" else ("Stopping…", "primary")


def cdc_monitoring_visible(migration_state, job_manager) -> bool:
    """Whether the CDC monitoring surfaces should render at all.

    One predicate for the three views that only make sense against a live pipeline --
    **Live status** (connector health + stream-lag chart), the **dead-letter queue**
    panel nested inside it, and the **per-table migration status** table. They read the
    same signals, so a divergent gate would leave the screen self-contradictory.

    Two conditions, both required:

    * CDC has STARTED (:func:`cdc_streaming_started`) -- true from the moment Start CDC
      is pressed, so the views are present through the connectors' ~10-20 min ramp,
      which is exactly when the operator wants to watch them come up.
    * No teardown is in flight (:func:`cdc_teardown_badge`). This is the gap: a Delete
      (or Stop) does not remove the connectors instantly, so discovery keeps reporting
      them and ``cdc_streaming_started`` stays true for the whole ~20 min teardown --
      leaving a live stream-lag chart, connector health, and per-table replication
      figures on screen for a pipeline being dismantled. Those numbers are about to
      become meaningless, and the delete progress is what the operator needs instead.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    if not cdc_streaming_started(migration_state, job_manager):
        return False
    return cdc_teardown_badge(migration_state, job_manager) is None


def cdc_unstable_message(status: Optional[str]) -> tuple[str, str, str, str]:
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
            "The cdc-stack is being torn down (this takes ~15–25 min — the in-VPC "
            "Lambda's network interfaces take time to detach). MSK / NAT billing "
            "stops once it completes. This view refreshes automatically; no action "
            "is needed.",
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
    cost of saying yes (~15-20 min, billable). Answering yes latches
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
        "Rebuilding creates a new MSK Serverless cluster and takes ~10-15 minutes; "
        "it is billable. Leave this alone if you are done with CDC."
    ).classes("text-xs text-gray-500")


def _render_cdc_state_unknown_notice(ui) -> None:
    """Say the CDC state is unknown, and how to recover it.

    Without this the operator gets a deploy form for a pipeline that may already be
    running -- there is no way to tell from the screen that the tool simply has not
    looked yet. Names the one action that recovers it (re-verify the target, which is
    what the probe needs) and states plainly that nothing was broken by the restart, so
    nobody re-runs Start CDC on a live pipeline.
    """
    render_notice(
        ui,
        tone="warning",
        header="CDC state not determined yet",
        body=(
            "This session was restored, so its connections are not trusted until "
            "re-verified and the read-only AWS check that reads the live CDC state has "
            "not run. Any pipeline you already started is unaffected and keeps "
            "streaming. Re-verify the target connection on the Connect step to recover "
            "the real state here — do not start CDC again until it shows."
        ),
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
            _render_cdc_state_unknown_notice(ui)
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
    watermark = getattr(job, "watermark", None) if job is not None else None
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
    ready = (
        resumes_from_offset
        or (override is not None and override.has_coordinates())
        or (wm_resume is not None and wm_resume.has_coordinates())
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
            header="Run the CDC prerequisite checks first",
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
    ~15-20 min MSK create can be started EARLY and OVERLAP the Full Load, instead of
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
            else ("Ready", "positive") if prep == "ready"
            else ("Not deployed", "grey")
        ),
    )

    if prep == "deploying":
        render_notice(
            ui,
            tone="info",
            busy=True,  # ~15-20 min live operation: spinner + "In progress" badge
            header="Deploying in the background — start your Full Load now",
            body=(
                "Amazon MSK takes ~10-15 minutes to provision. Nothing is streaming "
                "yet, so this does not hold up the snapshot: continue to Full Load "
                "and let the two run together. You can leave this screen; progress "
                "is kept and shown when you return."
            ),
        )
        _render_cdc_deploy_live(ui, migration_state, job_manager, refresh)
        return

    if prep == "ready":
        stack = getattr(migration_state, "cdc_stack_name", "the cdc-stack")
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
            "CDC needs Amazon MSK, which takes ~10-15 minutes to provision and bills "
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
        stuck = ", ".join(f"{name} ({status})" for name, status in needs_cleanup)
        render_notice(
            ui,
            tone="error",
            header="Leftover CDC infrastructure needs cleanup — it may still be billing",
            body=(
                f"{stuck}. A previous teardown did not finish, so this stack cannot be "
                "used or attached to (it is partly deleted) — but its Amazon MSK / NAT "
                "resources may still be incurring cost. Finish the delete first: use "
                "'Delete CDC infrastructure' below, or delete the stack in the "
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
        "connector networking, plugins and IAM role). This takes ~10-15 minutes "
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
        await _open_cdc_infra_dialog(
            ui, migration_state,
            lambda: _start_cdc_infra_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
        )

    # Explicit CDC-prerequisite gate. MSK is billable and takes ~15-20 min to
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
            header="Run the CDC prerequisite checks first",
            body=_prereq_block,
        )
        return

    # VpcId is the one input the tool cannot infer (subnets/NAT, the plugin bucket, the
    # DSQL cluster ARN, the source host and its secret are all resolved at deploy time),
    # but it was validated only in the SUBMIT path: the button looked ready, the click
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
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
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

        # Advanced: the cdc-stack name. The mandatory "mysql-dsql-cdc-" prefix is
        # rendered INSIDE the field via Quasar's built-in `prefix` prop (baseline-
        # aligned with the typed text, like a "$" before an amount), and the user
        # types only the SUFFIX (e.g. "orders" -> mysql-dsql-cdc-orders) to run a
        # SECOND migration's CDC alongside an existing one. Editing only the suffix
        # makes it impossible to leave the mysql-dsql-cdc-* family the deploy role
        # authorizes, so a bare "abcde" becomes the valid "mysql-dsql-cdc-abcde".
        name_field = ui.input(  # type: ignore[attr-defined]
            label="Advanced — CDC stack name (one per source DB)",
            value=cdc_stack_name_suffix(
                getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
            ),
            placeholder="stack",
        ).props(f'prefix="{CDC_STACK_NAME_PREFIX}"').classes("w-full text-sm")
        ui.label(  # type: ignore[attr-defined]
            "Full stack name = the fixed prefix + your suffix "
            "(e.g. mysql-dsql-cdc-orders). One stack per source DB."
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
                "(e.g. 'orders' -> mysql-dsql-cdc-orders).",
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
            "an IAM role. It takes about 10-15 minutes and creates billable AWS "
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
    watermark = getattr(job, "watermark", None) if job is not None else None
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


async def _open_cdc_start_dialog(
    ui, migration_state, on_confirm, *, session=None, job_manager=None
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
    if job_manager is not None:
        from nicegui import run

        try:
            binlog_gap = await run.io_bound(
                _probe_binlog_resume_gap, migration_state, job_manager, session
            )
        except Exception:  # noqa: BLE001 - advisory only; never block the dialog
            binlog_gap = None
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

        def _go() -> None:
            start_btn.disable()
            start_btn.set_text("Submitting…")
            ui.notify(  # type: ignore[attr-defined]
                "Submitting Start CDC…", type="info", position="top",
            )
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
    exclusions = migration_state.lob_exclusions()
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
    source_config = CdcPipelineOrchestrator().build_source_config(
        "mysql-source",
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        column_exclude_list=exclude_list,
        resume_override=override if (mode == "manual" and override is not None and override.has_coordinates()) else None,
        message_key_columns=message_key_columns,
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
        sink_mcu_count=_sink_mcu_count(),
    )
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=getattr(session, "aws_profile", None),
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    # The cdc-stack template exceeds CFn's 51,200-byte inline limit, so the
    # deployer stages it in S3 via TemplateURL. Derive the bucket name from the
    # deterministic naming convention (same bucket the infra deploy created).
    from dsql_migrator.core.s3_provision import plugin_bucket_name as _pbucket
    try:
        _sts = deployer._client("sts")
        _acct = _sts.get_caller_identity()["Account"]  # type: ignore[attr-defined]
        deployer.template_s3_bucket = _pbucket(_acct, region)
    except Exception:  # noqa: BLE001 — best-effort; will fail later with a clear message
        pass
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name

    template_body = _read_cdc_template_body()

    # "Host is the mode": the in-VPC EC2 host sets DSQL_MIGRATOR_CDC_SEED_MODE=external
    # so the app does the CDC Kafka prep in-process (Lambda-free); Fargate/local leave
    # it unset -> "lambda" (the in-VPC seeder Lambda does the prep, unchanged).
    from dsql_migrator.config import load_config as _load_config

    seed_mode = _load_config().cdc_seed_mode

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
            template_body=template_body,
            seed_mode=seed_mode,
        )

    _action = "start CDC connectors"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="start")
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
    watermark = getattr(job, "watermark", None) if job is not None else None
    override = migration_state.cdc_start_override()
    exclusions = migration_state.lob_exclusions()
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
    params = build_cdc_infra_params(
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
    ui.notify("Infrastructure deploy started (~10-15 min).", type="positive", position="top")  # type: ignore[attr-defined]
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
                ui.label("can take up to ~20 min").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-400"
                )
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

def _render_migration_table_status(
    ui, migration_state, job_manager, session, *, inventory=None
) -> None:
    """Per-table consistency view: Full Load vs CDC, source-vs-target, DLQ.

    Answers the operator's real question -- "did CDC replicate everything, is
    anything missing?". It separates the one-shot Full Load row count from the
    changes CDC has applied since, shows the source-vs-target consistency verdict, and
    surfaces per-table quarantined (DLQ) events -- changes that did NOT reach the
    target. The per-op **Inserts / Updates / Deletes** columns are fed scan-free by the
    sink's own ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied`` CloudWatch
    metrics (DMS-style cumulative counters, refreshed each CDC poll), so they need no
    COUNT(*). The source/target-count columns still come from
    a direct COUNT(*) on each side, which scans the source, so those remain an
    explicit "Refresh counts" action (not an auto-poll).

    Rendered only once CDC has actually STARTED. It used to appear as soon as the CDC
    sub-step did -- i.e. during the ~15-20 min infrastructure create -- where every
    CDC-specific column (Stream lag, Quarantined, Inserts/Updates/Deletes, Consistency)
    is necessarily empty because no connector exists yet. That read as "CDC is running
    and replicating nothing", the opposite of the truth, and it padded the deploy screen
    with a table that could not answer its own question. Nothing is lost by waiting: the
    Full Load's own per-table table (Table / Status / Rows / Progress / Time / Attempts)
    stays on the Full Load step, so its results remain visible there.

    Matches ``_render_cdc_live_monitoring`` directly above it, which is likewise
    "meaningful only once streaming".
    """
    # cdc_monitoring_visible: appears the moment Start CDC is pressed (the connectors
    # take ~10-20 min to reach RUNNING and the operator wants the per-table view during
    # that ramp), and disappears again while a teardown is in flight -- replication
    # figures for a pipeline being dismantled are about to be meaningless.
    if not cdc_monitoring_visible(migration_state, job_manager):
        return
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

        # Persistent-table holder: the table ELEMENT + its header ⓘ tooltips are built
        # once; later polls swap ONLY the row data in place (see the early-return
        # below), so a tooltip is not torn down mid-hover by the ~5s poll.
        _status_tbl: dict = {"el": None}

        def _status_table() -> None:  # type: ignore[misc]
            job = _current_job(job_manager, migration_state.job_id)
            # Per-table quarantined (DLQ) counts: change events that did NOT reach the
            # target -- the "missing" the customer cares about for consistency.
            # CDC-sourced ONLY. The error log is keyed by the Full Load job id (both
            # phases write under it -- cdc_error_log_key), so an unfiltered summary put
            # Full Load quarantines in a column headed "Quarantined" on the CDC table,
            # AND disagreed with the DLQ card right below it: the card read "0
            # quarantined" while this column read 3 for the same session. Same key, same
            # filter, so the two now always agree.
            dlq_counts = dict(
                cdc_dlq_summary(
                    migration_state, cdc_error_log_key(migration_state)
                ).errors_by_table
            )
            rows_model = build_migration_table_status(
                table_names,
                full_load_job=job,
                target_counts=getattr(migration_state, "row_count_target", {}),
                source_counts=getattr(migration_state, "row_count_source", {}),
                dlq_counts=dlq_counts,
                source_max_pk=getattr(migration_state, "row_max_pk_source", {}),
                target_max_pk=getattr(migration_state, "row_max_pk_target", {}),
                applied_ops_metric=getattr(
                    migration_state, "cdc_applied_ops_by_table", {}
                ),
                replication_lag_ms=getattr(
                    migration_state, "cdc_replication_lag_by_table", {}
                ),
            )
            # Columns separate the one-shot Full Load contribution from the ongoing
            # CDC contribution, then show the source-vs-target consistency verdict
            # and anything quarantined (not applied).
            columns = [
                {"name": "table", "label": "Table", "field": "table", "align": "left"},
                {"name": "fl", "label": "Full Load", "field": "fl", "align": "left"},
                {"name": "fl_rows", "label": "Full Load rows", "field": "fl_rows"},
                # DMS-style per-op columns: cumulative INSERT/UPDATE/DELETE counts CDC
                # has applied since it started streaming (each its own column so the
                # counters read like DMS table statistics).
                {"name": "ins", "label": "Inserts", "field": "ins", "align": "right"},
                {"name": "upd", "label": "Updates", "field": "upd", "align": "right"},
                {"name": "del", "label": "Deletes", "field": "del", "align": "right"},
                # Header says "(est.)" once: the source figure here is the scan-free
                # information_schema estimate (an exact COUNT(*) on a large production
                # source is not run from this view), so the approximation belongs in
                # the column's name rather than only repeated in every cell.
                {"name": "source", "label": "Source rows (est.)", "field": "source"},
                {"name": "target", "label": "Target rows", "field": "target"},
                {"name": "stream", "label": "Stream lag", "field": "stream"},
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

            def _fmt_count(n: "Optional[int]") -> str:
                # Bare thousands-separated count for the I/U/D sub-cells (no sign:
                # each op count is non-negative). "0" when the metric is present but
                # that op hasn't happened; the has_ops flag drives the "—" empty case.
                return "0" if n is None else f"{n:,}"

            def _fmt_lag(ms: "Optional[int]") -> str:
                # Time-based replication lag (ms) -> human "behind" text. Sub-second
                # rounds to "caught up" (effectively real-time); else s / m·s / h·m.
                if ms is None:
                    return "—"
                if ms < 1000:
                    return "caught up"
                secs = ms / 1000.0
                if secs < 60:
                    return f"{secs:.1f}s behind"
                if secs < 3600:
                    return f"{int(secs // 60)}m {int(secs % 60)}s behind"
                return f"{int(secs // 3600)}h {int((secs % 3600) // 60)}m behind"

            # User-facing consistency label + the verdict key (drives the badge color).
            # NB: there is deliberately no "target ahead" verdict. The source figure
            # here is an ESTIMATE that typically UNDERCOUNTS, so a target exceeding it
            # is the normal case -- flagging it as an anomaly made most healthy tables
            # look broken (see MigrationTableStatus.consistency).
            _CONSISTENCY_LABEL = {
                "consistent": "consistent",
                "quarantined": "data quarantined",
                "behind": "replicating…",
                "gap": "rows missing",
                "unknown": "refresh to check",
            }
            table_rows = []
            for r in rows_model:
                # The header already says "(est.)" (the normal case), so only the
                # UNUSUAL case is marked per-cell: an EXACT source count, which is
                # worth flagging because it makes the row's verdict authoritative.
                source_label = _fmt(r.source_rows)
                if not r.source_estimate and r.source_rows is not None:
                    source_label += " (exact)"
                # Stream lag: prefer the sink's TIME-based end-to-end lag
                # (ReplicationLagMs = apply time − source commit time) — accurate and
                # PK-agnostic. Fall back to the MAX(pk) leading-edge check (caught up /
                # N behind) only when the metric is unavailable (older plugin) or the
                # counts weren't refreshed. A metric present but idle (no recent
                # datapoint) means the stream drained → "caught up".
                if r.replication_lag_ms is not None:
                    stream = _fmt_lag(r.replication_lag_ms)
                elif r.stream_caught_up is True:
                    stream = "caught up"
                elif r.stream_caught_up is False:
                    stream = f"{r.pk_gap:,} behind (PK)"
                else:
                    stream = "—"
                verdict = r.consistency
                table_rows.append(
                    {
                        "table": r.table,
                        # Title-case label (Done/In progress/…) to match the Full Load
                        # stats table's status badge; fl_state stays raw for the color.
                        "fl": {
                            "DONE": "Done",
                            "IN_PROGRESS": "In progress",
                            "FAILED": "Failed",
                            "PENDING": "Pending",
                        }.get(r.full_load_state, r.full_load_state) or "—",
                        "fl_state": r.full_load_state or "",
                        "fl_rows": _fmt(r.full_load_rows),
                        # DMS-style per-op counters (one column each): cumulative
                        # inserts / updates / deletes the sink applied since streaming
                        # began (scan-free). has_ops is False when the metrics are
                        # unavailable (older plugin / not yet emitting) -> the cells
                        # show "—".
                        "has_ops": r.cdc_applied_ops is not None,
                        "ins": _fmt_count(r.cdc_inserts),
                        "upd": _fmt_count(r.cdc_updates),
                        "del": _fmt_count(r.cdc_deletes),
                        "source": source_label,
                        "target": _fmt(r.target_rows),
                        "stream": stream,
                        "dlq": _fmt(r.dlq_count) if r.dlq_count else "0",
                        "consistency": _CONSISTENCY_LABEL.get(verdict, verdict),
                        "verdict": verdict,
                    }
                )
            # Poll update path: once the table exists, swap ONLY its row data in
            # place (no element/slot teardown) so the header ⓘ tooltips stay open
            # while hovering. Everything below (element, body/header slots, timer)
            # runs ONCE, on the first build.
            existing = _status_tbl["el"]
            if existing is not None:
                existing.rows[:] = table_rows  # type: ignore[attr-defined]
                existing.update()  # type: ignore[attr-defined]
                return
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
            _status_tbl["el"] = table
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
            # DMS-style per-op counters, one column each: cumulative inserts (green) /
            # updates (sky) / deletes (red) CDC has applied. Just the running count in
            # the op's colour (no leading glyph), or "—" when the metrics aren't
            # available yet (older plugin / sink not emitting).
            def _op_cell(name: str, colour: str) -> None:
                table.add_slot(
                    f"body-cell-{name}",
                    r"""
                    <q-td :props="props" class="text-right">
                      <span v-if="props.row.has_ops" class="%s">{{ props.value }}</span>
                      <span v-else>—</span>
                    </q-td>
                    """
                    % colour,
                )

            _op_cell("ins", "text-green-700")
            _op_cell("upd", "text-sky-700")
            _op_cell("del", "text-red-700")
            # Color the consistency verdict (green=consistent, red=quarantined/gap,
            # amber=behind, grey=unknown) so a problem is obvious at a glance.
            table.add_slot(
                "body-cell-consistency",
                r"""
                <q-td :props="props">
                  <q-badge
                    :color="{'consistent':'positive','quarantined':'negative','gap':'negative','behind':'warning','unknown':'grey'}[props.row.verdict] || 'grey'"
                    :label="props.value" outline />
                </q-td>
                """,
            )
            # In-context help: an ⓘ next to the three columns whose meaning isn't
            # obvious from the label, so the explanation is right where the eye is
            # (the legend below is the fuller reference). Quasar `header-cell-<name>`
            # slot renders the label + a hover tooltip.
            def _hdr_info(name: str, tip: str) -> None:
                table.add_slot(
                    f"header-cell-{name}",
                    r"""
                    <q-th :props="props">
                      {{ props.col.label }}
                      <q-icon name="info" size="14px" class="q-ml-xs text-grey-5"
                        style="cursor:help">
                        <q-tooltip class="text-body2" style="max-width:340px">"""
                    + tip
                    + r"""</q-tooltip>
                      </q-icon>
                    </q-th>
                    """,
                )

            _hdr_info("ins", "Cumulative inserts CDC has applied since it started streaming.")
            _hdr_info("upd", "Cumulative updates CDC has applied since it started streaming.")
            _hdr_info("del", "Cumulative deletes CDC has applied since it started streaming.")
            _hdr_info(
                "stream",
                "How far behind the target is, in time — the age of the newest "
                "source change not yet applied. “caught up” = the target is current "
                "(safe to cut over). “N behind (PK)” is a fallback shown only when "
                "the time-based metric isn't available yet.",
            )
            _hdr_info(
                "source",
                "Approximate row count from the source's information_schema — a "
                "scan-free ESTIMATE, so this view never runs a COUNT(*) full scan "
                "against your live source. InnoDB derives it from index sampling, so "
                "it commonly differs from the true count by several percent (more on "
                "a large table) and often UNDERCOUNTS — a target that slightly "
                "exceeds it is normal, not data duplication. For an exact "
                "source-vs-target comparison, run Validation (step 4).",
            )
            _hdr_info(
                "consistency",
                "Overall verdict for the table: “consistent” (green) = the stream has "
                "caught up and nothing indicates missing rows · “replicating…” "
                "(amber) = target still catching up · “rows missing” (red) = newest "
                "rows landed but some in between are missing · “data quarantined” "
                "(red) = changes failed and were set aside (DLQ). Because Source rows "
                "is an estimate, “consistent” means nothing looks wrong rather than a "
                "proven exact match — Validation (step 4) is the exact check. Any "
                "non-green = worth investigating.",
            )
            # Keep the per-op Inserts/Updates/Deletes columns (and lag/quarantined)
            # live while CDC streams. Created ONCE (this block only runs on the first
            # build); a REPEATING timer re-invokes _status_table, which early-returns
            # to an in-place row swap on this persistent table. Reads state only (NO
            # network / no COUNT), so it stays scan-free. A repeating timer is safe
            # here (unlike the old .refresh() path) precisely because it never
            # re-renders / tears down slots -- so it can't trigger the "parent slot
            # deleted" crash, and the header ⓘ tooltips survive each poll. Armed only
            # while CDC is active; idle/Full-Load-only leaves the table static.
            if _cdc_is_streaming(migration_state):
                ui.timer(_CDC_POLL_INTERVAL_SECONDS, _status_table)  # type: ignore[attr-defined]

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
                    _status_table()  # in-place row update (table built once already)

        # Top: the one thing to know before reading the numbers -- the source side
        # is a scan-free estimate, so it adds no load on a large-scale source but is
        # approximate (Validation does the exact reconciliation).
        # Before the first refresh the Consistency column reads "refresh to check" on
        # every row; the notice body then names the button that fills it (the column
        # and the action are otherwise only linked by a coincidence of wording). After
        # a refresh it drops the prompt and states the estimate caveat.
        _counts_fetched = (
            getattr(migration_state, "row_counts_fetched_at", None) is not None
        )
        render_notice(
            ui,
            tone="info",
            header="Source rows are an estimate (no load on the source)",
            body=per_table_counts_notice_body(counts_fetched=_counts_fetched),
        )
        refresh_btn = ui.button(  # type: ignore[attr-defined]
            "Refresh source/target counts",
            on_click=_refresh_counts,
            icon="sync",
        ).props("color=primary outline size=sm")
        _status_table()
        # Below the table (reference help, not a status to act on): the column
        # legend as scannable definition rows in a quiet bordered panel. Each term
        # matches a table header, so the mapping is obvious; the Consistency row
        # renders the REAL badge chips (same Quasar colors as the table cells), so
        # the reader sees the actual green/amber/red instead of imagining it. The
        # per-column ⓘ tooltips above carry the same help in-context.
        with ui.column().classes(  # type: ignore[attr-defined]
            "gap-1 w-full mt-2 border border-gray-200 rounded-md bg-gray-50 p-3"
        ):
            with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                ui.icon("help_outline").classes("text-sky-600 text-base")  # type: ignore[attr-defined]
                ui.label("How to read this table").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-gray-900"
                )
            definition_row(
                ui, "Full Load rows", "Rows the one-shot snapshot loaded."
            )
            definition_row(
                ui,
                "Inserts / Updates / Deletes",
                "Cumulative row changes CDC has applied since it started streaming — "
                "green = inserts, blue = updates, red = deletes (running totals, live "
                "from the sink, scan-free). “—” = the metrics aren't available yet.",
            )
            definition_row(
                ui,
                "Source / Target rows",
                "Source = scan-free information_schema ESTIMATE (never a COUNT(*) on "
                "your live source) · Target = exact count. The estimate comes from "
                "InnoDB index sampling and often undercounts by a few percent, so a "
                "target slightly above it is normal — the two numbers are not meant "
                "to match exactly. Run Validation (step 4) for the exact comparison.",
            )
            definition_row(
                ui,
                "Stream lag",
                "How far behind the target is, in time — the age of the newest source "
                "change not yet applied. “caught up” = the target is current (safe to "
                "cut over); “N behind (PK)” is a fallback shown only when the "
                "time-based metric isn't available yet.",
            )
            # Consistency: the actual badge chips, colored exactly like the table's
            # body-cell-consistency slot (consistent→positive, behind→warning,
            # gap/quarantined→negative). Keep these labels/colors in sync with that
            # slot and _CONSISTENCY_LABEL above.
            _consistency_desc = definition_row(ui, "Consistency")
            with _consistency_desc:  # type: ignore[attr-defined]
                ui.badge("consistent").props("color=positive outline")  # type: ignore[attr-defined]
                ui.badge("replicating…").props("color=warning outline")  # type: ignore[attr-defined]
                ui.badge("rows missing").props("color=negative outline")  # type: ignore[attr-defined]
                ui.badge("data quarantined").props("color=negative outline")  # type: ignore[attr-defined]
                ui.label(  # type: ignore[attr-defined]
                    "— any non-green badge means investigate. “consistent” means "
                    "nothing looks wrong (the source count is an estimate, so it is "
                    "not a proven exact match); Validation (step 4) is the exact check."
                ).classes("text-xs text-gray-600")

def _render_cdc_live_monitoring(ui, migration_state, job_manager) -> None:
    """Live connector health + DLQ, polled read-only from MSK Connect.

    Mirrors the Full Load poll chain: a refreshable region arms a one-shot timer
    at the END of its render, and the poll re-renders + re-arms -- a
    self-perpetuating single-shot chain that avoids the "parent slot deleted"
    crash a repeating timer causes. Meaningful only once streaming, so it is
    placed after the start action.

    Hidden entirely until CDC has STARTED. Before that there is no pipeline to report
    on -- the whole section (the "Live status" header, the empty stream-lag chart, and
    the "appears once connectors are detected" placeholder) was just dead space on the
    deploy screen, with the header sitting borderless above nothing. Gated on
    :func:`cdc_streaming_started` (not ``cdc_pipeline_live``) so it appears the moment
    Start CDC is pressed and stays through the connectors' ~10-20 min ramp -- which is
    exactly when the operator wants to watch them come up. Matches the per-table table
    below, which uses the same gate. The dead-letter panel is rendered INSIDE this
    section, so it inherits the same visibility -- see :func:`cdc_monitoring_visible`,
    which also hides all of it while a teardown is in flight.
    """
    if not cdc_monitoring_visible(migration_state, job_manager):
        return
    ui.label("Live status").classes("text-sm font-semibold")  # type: ignore[attr-defined]

    # --- Live "Stream lag" chart -------------------------------------------------
    # Created ONCE here, OUTSIDE the 5s refreshable below, and updated IN PLACE via
    # chart.update() on each poll -- so the line extends continuously like a
    # CloudWatch graph instead of the whole echart being torn down and recreated
    # every 5s (which flickered). The card is hidden until there are >=2 points (a
    # single dot is not a trend). The rolling series behind it is a hybrid: seeded
    # from CloudWatch's 1-min history (survives reload) then extended each poll.
    lag = {"card": None, "chart": None, "empty": None, "stalled": None}
    with ui.card().classes("w-full") as _lag_card:  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-1.5 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("show_chart", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Stream lag").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            # The interpretation guidance (what flat/rising means, cut-over safety)
            # is genuinely useful but reads as clutter when always on -- move it to a
            # hover ⓘ, matching the per-table header tooltips. The chart title +
            # y-axis "lag (ms)" carry the basics on their own.
            ui.icon("info").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help"
            ).tooltip(
                "Worst end-to-end replication lag across tables, live (max lag in "
                "ms). Flat near zero = caught up (safe to cut over); a rising line "
                "means the pipeline is falling behind."
            )
        lag["chart"] = (  # type: ignore[assignment]
            ui.echart(  # type: ignore[attr-defined]
                {"xAxis": {"type": "time"}, "yAxis": {"type": "value"}, "series": []}
            )
            .classes("w-full")
            .style("height: 220px")
        )
        # Shown INSTEAD of the chart when there is no >=2-point trend but CDC is live:
        # the sink emits ReplicationLagMs only while applying events, so a drained /
        # caught-up pipeline has no recent datapoint to plot. Without this the whole
        # panel vanished after a session restore (the in-memory trend is not persisted
        # and a caught-up pipeline can't be re-seeded from CloudWatch), so the operator
        # saw NO stream-lag signal at all. Now the metric is always present when CDC is
        # running -- as a "caught up" line when there is nothing to trend.
        with ui.row().classes("items-center gap-1.5 no-wrap") as _lag_empty:  # type: ignore[attr-defined]
            ui.icon("check_circle").classes("text-green-600 text-base")  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "Caught up — no replication lag in the recent window."
            ).classes("text-sm text-gray-700")
        lag["empty"] = _lag_empty  # type: ignore[assignment]
        # The SAME "no datapoints" input means the opposite thing when the sink has
        # stalled: the sink emits ReplicationLagMs only while applying, so a DEAD sink
        # produces no datapoint and rendered the green "Caught up" line above -- a total
        # replication outage shown as the strongest possible all-clear. This row
        # replaces it in that case (see _update_lag_chart).
        with ui.row().classes("items-center gap-1.5 no-wrap") as _lag_stalled:  # type: ignore[attr-defined]
            ui.icon("error").classes("text-red-600 text-base")  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "No lag data because the sink is applying nothing — this is a stall, "
                "not being caught up."
            ).classes("text-sm font-semibold text-red-700")
        lag["stalled"] = _lag_stalled  # type: ignore[assignment]
    lag["card"] = _lag_card  # type: ignore[assignment]

    def _update_lag_chart() -> None:
        """Push the latest rolling series into the persistent echart IN PLACE and
        toggle the card's states: the trend chart (>=2 points), a "caught up" line (CDC
        live but no trend to plot -- e.g. right after a session restore of a drained
        pipeline), the STALLED line instead of that (no datapoints because the sink is
        applying nothing -- the same input, opposite meaning), or fully hidden (CDC not
        streaming)."""
        option = build_lag_chart_option(
            getattr(migration_state, "cdc_replication_lag_series", None) or []
        )
        card, chart, empty = lag["card"], lag["chart"], lag["empty"]
        stalled_row = lag.get("stalled")
        activity = getattr(migration_state, "cdc_activity", None)
        stalled = bool(getattr(activity, "sink_stall_confirmed", False))
        if card is None or chart is None:
            return
        if option is not None:
            chart.options.clear()  # type: ignore[attr-defined]
            chart.options.update(option)  # type: ignore[attr-defined]
            chart.update()  # type: ignore[attr-defined]
            chart.set_visibility(True)  # type: ignore[attr-defined]
            if empty is not None:
                empty.set_visibility(False)  # type: ignore[attr-defined]
            if stalled_row is not None:
                stalled_row.set_visibility(False)  # type: ignore[attr-defined]
            card.set_visibility(True)  # type: ignore[attr-defined]
        elif _cdc_is_streaming(migration_state):
            # No trend, and the pipeline is live. Which line to show depends on WHY
            # there is no datapoint: caught up (sink applied everything) or stalled
            # (sink applied nothing) -- never claim the former when it is the latter.
            chart.set_visibility(False)  # type: ignore[attr-defined]
            if empty is not None:
                empty.set_visibility(not stalled)  # type: ignore[attr-defined]
            if stalled_row is not None:
                stalled_row.set_visibility(stalled)  # type: ignore[attr-defined]
            card.set_visibility(True)  # type: ignore[attr-defined]
        else:
            card.set_visibility(False)  # type: ignore[attr-defined]

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
                # Wrap the waiting message in an info notice, not a bare label: every
                # other section on this screen is a bordered card/notice, so a loose
                # grey line here read as an unstyled gap. This is a normal pre-CDC
                # state (connectors not yet detected), so it is info, not a warning.
                render_notice(
                    ui,
                    tone="info",
                    icon="hourglass_empty",
                    header="No live pipeline yet",
                    body=(
                        "Live connector health and replication lag appear here once "
                        "the cdc-stack connectors are detected."
                    ),
                )
        if _cdc_is_streaming(migration_state):
            ui.timer(_CDC_POLL_INTERVAL_SECONDS, _poll_cdc, once=True)  # type: ignore[attr-defined]

    async def _poll_cdc() -> None:
        # The MSK Connect + CloudWatch reads are blocking network I/O; run them on
        # a worker thread (run.io_bound) so they never block the NiceGUI event loop.
        # Blocking the loop here previously starved the WebSocket keep-alive and
        # made the browser drop the connection. The pure view-build + state write
        # happens back on the loop after the fetch returns.
        from nicegui import run

        # Scope the scan-free net-rows metric read to the migrated table set.
        tables = _migration_status_tables(migration_state, job_manager)
        try:
            fetched = await run.io_bound(_fetch_cdc_status, migration_state, tables)
        except Exception:  # noqa: BLE001 - keep the last good view on any error
            fetched = None
        if fetched is not None:
            _apply_cdc_status(migration_state, fetched)
        _update_lag_chart()  # persistent chart: update in place (no flicker)
        _cdc_live.refresh()  # connector health / change flow / DLQ (text) redraw

    _update_lag_chart()  # initial state (hidden until >=2 points)
    _cdc_live()

def _render_cdc_pipeline_health(
    ui,
    status_view: LoadStatusView,
    activity: "Optional[CdcActivitySummary]",
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
        # Compact one-line-per-connector: status icon + friendly role label (raw
        # connector id in a hover tooltip) + a colour-coded state BADGE (green
        # "Running", etc.) for at-a-glance health + a muted detail. Keeps the minimal
        # list but restores the visible state badge (plain "streaming normally" text
        # read as too subtle); outline + title-case to match the other status chips.
        _badge_color = {"ok": "positive", "warn": "warning", "bad": "negative"}
        for row in rows:
            _border, _bg, icon_color, icon = _CDC_TONE_STYLE.get(
                row.tone, _CDC_TONE_STYLE["warn"]
            )
            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.icon(icon, color=icon_color).classes("text-sm shrink-0")  # type: ignore[attr-defined]
                ui.label(row.label or row.name).classes(  # type: ignore[attr-defined]
                    "text-sm text-gray-800 shrink-0"
                ).tooltip(row.name)
                if row.state:
                    ui.badge(  # type: ignore[attr-defined]
                        row.state.title(), color=_badge_color.get(row.tone, "grey")
                    ).props("outline")
                ui.label(row.detail).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500 truncate"
                )

        # --- Change flow ------------------------------------------------------
        if activity is not None:
            ui.separator().classes("my-1")  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                ui.label("Change flow").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold text-gray-500 uppercase tracking-wide"
                )
                # Both the "what/why" and the data-source note move to a hover ⓘ so
                # the change-flow block is just the state line + rate gauges.
                ui.icon("info").classes(  # type: ignore[attr-defined]
                    "text-gray-400 text-sm cursor-help"
                ).tooltip(
                    "Whether changes are still streaming from the source to the "
                    "target. When you quiesce the source for cutover, watch this "
                    "drop to idle — the pipeline has drained. Rates are from "
                    "CloudWatch (about the last few minutes)."
                )
            _render_change_flow_status(ui, activity)

def _render_change_flow_status(ui, activity: "CdcActivitySummary") -> None:
    """The change-flow state line + CloudWatch throughput (inner block, no card).

    Pure information for the operator's cutover judgement — NOT a gate or a
    recommendation. Honest about "unknown": when a rate is unavailable it is shown
    as such, never as 0/idle.
    """
    def _fmt(rate: "Optional[float]") -> str:
        # Unit = change-event RECORDS per second (SourceRecordPollRate /
        # SinkRecordSendRate); spell "rec/s" so a bare "/s" is not ambiguous.
        return f"{rate:.2f} rec/s" if rate is not None else "unknown"

    with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        if activity.idle is True:
            ui.icon("pause_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
            ui.label("No changes flowing — pipeline idle").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-700"
            )
        elif activity.sink_stall_confirmed:
            # Checked BEFORE the "streaming" branch: a stalled sink is not idle, so it
            # used to fall through to "Streaming — changes are flowing" — asserting the
            # pipeline was healthy off the SOURCE rate alone while nothing reached DSQL.
            ui.icon("error", color="negative").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Sink stalled — changes are NOT reaching DSQL").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-red-700"
            )
        elif activity.idle is False:
            ui.icon("sync", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Streaming — changes are flowing").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-700"
            )
        else:
            ui.icon("help_outline", color="grey").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Activity unknown").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
    if activity.sink_stall_confirmed:
        # The divergence is already on screen as two bars (source > 0, sink 0) -- what
        # was missing is reading it. Say what it means and what to do, per the project's
        # "what happened, what to do next" rule: the connector will still show RUNNING,
        # so the operator needs to be told not to trust that.
        render_notice(
            ui,
            tone="error",
            header="The sink is not applying changes",
            body=(
                "The source is producing changes but the sink has applied none, so the "
                "target is falling behind and the gap will not close on its own. The "
                "connector can still report RUNNING and no errored task while this is "
                "happening (a sink consumer ejected from its group keeps its thread "
                "alive), so RUNNING is not evidence that replication works — compare "
                "target row counts. Check the sink connector log for repeating "
                '"Commit of offsets timed out"; if you see it, Stop CDC and Start CDC '
                "to rejoin the group, and do NOT cut over until the sink send rate "
                "recovers."
            ),
        )
    # Visual rate gauges: source poll vs sink send on the SAME scale, so at a glance
    # you can see whether the sink is keeping up with the source (matched bars) or
    # falling behind (shorter sink bar). Unknown rates show "unknown" with no bar.
    sp, ss = activity.source_poll_rate, activity.sink_send_rate
    scale = max([r for r in (sp, ss) if r is not None] or [0.0])

    def _rate_bar(label: str, rate: "Optional[float]") -> None:
        # Compact fixed-width row: label + a FIXED-width bar (not flex-1, so it never
        # stretches to the card edge) + the value right after it. Indent with pl-6
        # (padding, inside the width) NOT ml-6 (margin, which added to a w-full row
        # pushed the trailing value outside the Pipeline health card). No w-full → the
        # row sizes to its content and stays within the card.
        with ui.row().classes("items-center gap-2 no-wrap pl-6"):  # type: ignore[attr-defined]
            ui.label(label).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-600 shrink-0"
            ).style("width: 76px")
            with (
                ui.element("div")  # type: ignore[attr-defined]
                .classes("relative h-2 rounded bg-gray-200 shrink-0")
                .style("width: 140px")
            ):
                if rate is not None and scale > 0:
                    pct = max(3.0, min(100.0, rate / scale * 100.0))
                    ui.element("div").classes(  # type: ignore[attr-defined]
                        "absolute inset-y-0 left-0 rounded bg-sky-500"
                    ).style(f"width: {pct:.1f}%")
            ui.label(_fmt(rate)).classes(  # type: ignore[attr-defined]
                "text-xs font-mono text-gray-700 shrink-0"
            )

    _rate_bar("Source poll", sp)
    _rate_bar("Sink send", ss)
    # Data-source/freshness note moved to the "Change flow" header ⓘ tooltip; the
    # gauges stand on their own here (no standing provenance caption).

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

def _dlq_panel_tone(health, *, sink_stalled: bool = False) -> str:
    """Map a :class:`DlqHealth` to a notice tone.

    ``ok`` with a non-zero depth is downgraded to ``info`` (sporadic, isolated
    poison is an FYI, not a success), while a truly clean stream (depth 0) stays
    ``success``. Calibrated to the project's severity rules (info/warning/error).

    ``sink_stalled`` blocks the ``success`` upgrade: depth is counted from quarantine
    log lines, and a sink that has stopped applying never reaches a record to
    quarantine -- so depth 0 means "nothing was even attempted", not "nothing went
    wrong". Painting that green made the panel assert an all-clear during total data
    loss. Downgraded to ``info`` (the stall itself is reported, loudly, by the change-
    flow signal; this panel just must not contradict it).
    """
    if health.level == "ok":
        return "success" if health.depth == 0 and not sink_stalled else "info"
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
    _activity = getattr(migration_state, "cdc_activity", None)
    _stalled = bool(getattr(_activity, "sink_stall_confirmed", False))
    tone = _dlq_panel_tone(health, sink_stalled=_stalled)
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
        if _stalled and health.depth == 0:
            # "No records quarantined." is literally true but reads as reassurance, so
            # say why it proves nothing right now: a stalled sink never reaches a record
            # to quarantine, so a zero depth is the EXPECTED reading during a stall.
            ui.label(  # type: ignore[attr-defined]
                "A zero count is expected while the sink is stalled — it never reaches "
                "a record to quarantine, so this is not evidence that nothing was lost."
            ).classes("text-xs font-semibold text-red-700")
        _render_cdc_dlq_breakdown(ui, status_view)
        # CDC commonly has no Full Load job_id this session, so key the record list /
        # download off the same stable CDC key the fold used (cdc_error_log_key) --
        # not _current_job, which would be None and hide everything.
        log_key = cdc_error_log_key(migration_state)
        _render_cdc_dlq_records(ui, migration_state, log_key)
        if health.depth > 0:
            _render_cdc_error_download(ui, migration_state, log_key)
        _render_full_load_quarantine_pointer(ui, migration_state, log_key)

def _render_full_load_quarantine_pointer(ui, migration_state, log_key: str) -> None:
    """Note that the Full Load set rows aside too, and where to see them.

    The DLQ panel now counts CDC records only, which is correct -- but the Full Load's
    quarantines must not simply vanish from view: they are rows that never reached the
    target, and cut-over depends on knowing about them. So when this session's error log
    also holds Full Load records, say so in one neutral line that points at the Full
    Load section rather than folding them back into a DLQ count they do not belong to.

    Deliberately NOT a warning: the Full Load already reported these where they belong,
    and this line is a cross-reference, not a new problem.
    """
    if not log_key:
        return
    try:
        records = migration_state.error_log.records(log_key) or []
    except Exception:  # noqa: BLE001 - advisory line; never break the panel
        return
    full_load = [r for r in records if not is_cdc_error_record(r)]
    if not full_load:
        return
    noun = "row" if len(full_load) == 1 else "rows"
    ui.label(  # type: ignore[attr-defined]
        f"The Full Load also set {len(full_load)} {noun} aside. Those are separate from "
        "this dead-letter queue (the batch loader isolated them; they never entered the "
        "stream) — see the Full Load section above for the details."
    ).classes("text-xs text-gray-500")


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
    # CDC-sourced records only: the log key is shared with the Full Load, so an
    # unfiltered read listed batch-loader quarantines under "Dead-letter queue".
    records = cdc_dlq_records(migration_state, log_key)
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
    """Offer the CDC-sourced error records as a download.

    Both the COUNT in the label and the file contents are filtered to CDC. The log key
    is shared with the Full Load, so this button used to say "Download CDC error log
    (3 errors)" and hand over three Full Load quarantines -- see is_cdc_error_record.
    """
    records = cdc_dlq_records(migration_state, log_key)
    if not records:
        return
    # A filesystem-safe slug for the filename (the CDC fallback key is "cdc:<stack>").
    safe = log_key.replace(":", "_").replace("/", "_")

    def _download_log() -> None:
        try:
            # Serialize the FILTERED records; render_log(log_key) would re-read the
            # whole key and put Full Load rows back into a file labelled CDC.
            payload = migration_state.error_log.render_records(records)
            ui.download.content(  # type: ignore[attr-defined]
                payload, f"cdc_error_log_{safe}.ndjson", "application/x-ndjson"
            )
        except Exception as exc:  # noqa: BLE001 - surface instead of silent
            _LOGGER.exception("Failed to render/download CDC error log")
            ui.notify(  # type: ignore[attr-defined]
                f"Could not generate the error log: {exc}", type="negative"
            )

    # Named the same way as the Full Load download: WHAT it is and HOW MUCH, not the file
    # format. Saying "CDC" also matters here -- both steps offer a download, and
    # "Download error log" alone gave no way to tell which one you were getting.
    noun = "error" if len(records) == 1 else "errors"
    ui.button(  # type: ignore[attr-defined]
        f"Download CDC error log ({len(records)} {noun})",
        on_click=_download_log,
        icon="download",
    ).props("outline dense no-caps").tooltip(  # type: ignore[attr-defined]
        "One line per error with the table, primary key, reason and timestamp — never "
        "row values. Saved as NDJSON (one JSON object per line), readable in any text "
        "editor."
    )

def lob_exclusion_lock(
    migration_state, job_manager, *, full_load_committed: bool = False
) -> tuple[bool, Optional[str]]:
    """Return ``(locked, reason)`` for the LOB-exclusion tick boxes.

    The selection is not a live setting: it is baked into the cdc-stack's
    ``ColumnExcludeList`` parameter when the infrastructure is created, and handed to
    the source connector at Start CDC. So once either operation is under way -- or the
    connectors exist -- a further tick changes state that nothing will read, and the box
    silently lies about what CDC captures. The boxes previously had no lock at all: a
    tick registered and the state really changed, it just never reached the pipeline.

    Every locked case now NAMES its reason. An earlier version left the
    "infrastructure exists" case silent on the theory that the greyed-out boxes were
    self-explanatory -- but a stopped-CDC operator who sees a ticked-and-frozen box
    with no explanation cannot tell WHY it is closed or how to change it (they read
    it as a bug). So the reason is always given; severity is carried by the render
    TONE (neutral for these expected/no-action states, not an alarming warning), not
    by withholding the text.

    Why the exclusion is locked once infrastructure exists (not merely once CDC is
    streaming): the excluded-column set is captured into the pipeline, and once CDC
    has streamed even once its resume offset is committed on MSK (a Stop deletes only
    the connectors, not the offset). Changing the exclusion then would leave the rows
    already migrated/streamed under the old set inconsistent with rows processed
    after -- silent partial data. The safe way to change it is to delete the CDC
    infrastructure and redeploy with the new set (a fresh offset + re-run), which is
    the remedy named below -- matching how the table picker locks on the same phase.

    ``full_load_committed`` is the FIRST and strongest clause: a Full Load has already
    committed rows to the target under a specific exclusion set (a completed
    ``full_load_only`` run, whose ``WorkflowStep.FULL_LOAD`` status stays ``DONE`` and
    whose ``job_id`` survives even after the type is switched to ``cdc_only`` to add
    replication). Editing the exclusion THEN is silent split-brain in EITHER direction:
    UN-excluding a column CDC then captures for post-snapshot changes while the loaded
    rows stay NULL; ADDING one makes CDC drop updates to a column the load populated,
    going stale. This clause is NOT released by deleting the cdc-stack (the load really
    ran against this set), so the only re-scope is Start over -- exactly like
    ``selection_lock_reason``'s ``has_job or status is DONE`` clause, which governs the
    Full Load screen's copy of this card. Without it, switching to ``cdc_only`` after a
    load moved the card to this (pre-deploy, weakly-locked) home and left it editable.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    if full_load_committed:
        return True, (
            "Locked — a Full Load has already loaded data using this exclusion set, so "
            "the excluded columns are fixed for this migration (changing them now would "
            "leave the already-loaded rows inconsistent with what CDC captures — the "
            "un-excluded column would be NULL on loaded rows but populated on later "
            "changes, or vice versa). To migrate a different column set, use 'Start over'."
        )
    if cdc_streaming_started(migration_state, job_manager):
        return True, (
            "Locked — the excluded columns were handed to the source connector when "
            "CDC started. Stop CDC to change them."
        )
    # An infra create is in flight: the parameter went out with the stack, but the
    # operator was just here choosing, so say what happened.
    if cdc_infra_deploy_in_flight(migration_state, job_manager):
        return True, (
            "Locked while the CDC infrastructure is being created — the excluded "
            "columns are part of the stack's parameters and were submitted with it."
        )
    if getattr(migration_state, "cdc_stack_phase", None) in (
        "infra",
        "running",
        "provisioning",
        "partial",
    ):
        # Infrastructure is deployed (e.g. after a Stop CDC, which keeps the stack and
        # its committed offset). Explain the lock and name the only safe remedy --
        # changing the exclusion on a pipeline that has already streamed would diverge
        # already-migrated rows from later ones.
        return True, (
            "Locked — the CDC infrastructure is deployed for this table set, so the "
            "excluded columns are fixed for this pipeline (changing them after CDC has "
            "streamed would leave already-migrated rows inconsistent with later ones). "
            "To change them, delete the CDC infrastructure on the CDC step first, then "
            "redeploy."
        )
    return False, None


def _render_cdc_lob_exclusion_panel(
    ui,
    migration_state,
    inventory: Optional[SourceInventory],
    refresh,
    *,
    locked: bool,
    lock_reason: Optional[str] = None,
    migration_wide: bool = False,
) -> None:
    """Render the explicit, opt-in oversized-LOB column exclusion (H13).

    Lists the columns the evaluation flagged as able to exceed the DSQL 1 MiB
    per-value limit and lets the user exclude them. The selection is
    migration-wide: a ticked column is dropped from BOTH the Full Load INSERT list
    and CDC capture (Debezium ``column.exclude.list``) -- one choice, so the two
    data paths never disagree across the gapless handoff. Excluding is the only
    safe handling for values that can also exceed the broker limit -- runtime
    isolation can't recover those. No silent loss: nothing is excluded unless the
    user ticks it, and (in the CDC context) the resulting list is shown verbatim.

    ``migration_wide`` switches the copy: when True the card is rendered on the
    Full Load screen (before the load), so it speaks of "this migration"; when
    False it renders inside the CDC sub-flow and speaks of CDC capture (keeping the
    familiar wording and the ``column.exclude.list`` preview there). The underlying
    selection is the same either way.

    When no oversized-LOB column qualifies (the common case), there is nothing to
    configure -- so instead of a full section the panel collapses to a single calm
    INFO notice (AWS-style), keeping the result discoverable without the visual
    weight of a settings card. The full opt-in card is shown only when there are
    actual candidates to exclude.
    """
    all_candidates = lob_exclusion_candidates(inventory)
    selected_tables = set(migration_state.selection.selected_tables)
    candidates = (
        [c for c in all_candidates if c.table in selected_tables]
        if selected_tables
        else all_candidates
    )
    selection = migration_state.lob_exclusions()
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
                "needs excluding from "
                + ("this migration." if migration_wide else "CDC capture.")
            ),
        )
        return
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            ui.icon("data_object", color="warning").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Oversized LOB columns (optional exclusion)").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold"
            )
        if migration_wide:
            ui.label(  # type: ignore[attr-defined]
                "These MySQL LOB/TEXT columns can hold values over the Aurora DSQL "
                f"{_DSQL_VALUE_LIMIT_MIB} MiB limit. Ticking one drops it from this "
                "migration entirely — the Full Load never writes it and (if CDC is "
                "used) capture excludes it too, so the two stay in lockstep. Leave a "
                "column ticked-off to load it normally; any single value that then "
                "exceeds the limit is quarantined per-row instead. Nothing is "
                "excluded unless you tick it."
            ).classes("text-xs text-gray-500")
        else:
            ui.label(  # type: ignore[attr-defined]
                "These MySQL LOB/TEXT columns can hold values over the Aurora DSQL "
                f"{_DSQL_VALUE_LIMIT_MIB} MiB limit. A value over the "
                f"{_BROKER_MESSAGE_LIMIT_MIB} MiB broker limit can't be streamed at "
                "all, so exclude such columns here to keep CDC from stalling. "
                "Nothing is excluded unless you tick it."
            ).classes("text-xs text-gray-500")
        # Always explain the lock when one is in effect. These are expected,
        # no-action-needed states (a deployed/started CDC pipeline), so the reason
        # renders NEUTRAL, not as an amber warning -- severity calibration: a normal
        # state must not read as a problem, but it must still say why the boxes are
        # frozen and how to change them (a silent frozen box reads as a bug).
        if locked and lock_reason:
            inline_hint(ui, lock_reason, tone="neutral")  # type: ignore[attr-defined]
        for candidate in candidates:
            excluded = selection.get(candidate.table, set())
            for column in candidate.columns:
                def _toggle(
                    event, _table=candidate.table, _column=column
                ) -> None:
                    migration_state.set_lob_exclusion(
                        _table, _column, bool(event.value)
                    )
                    if callable(refresh):
                        refresh()

                _box = ui.checkbox(  # type: ignore[attr-defined]
                    f"{candidate.table}.{column}",
                    value=column in excluded,
                    on_change=None if locked else _toggle,
                ).props("dense")
                if locked:
                    # disable() greys the box AND stops the click, so the visual state
                    # and the actual behaviour agree -- greying alone would still let
                    # the tick through.
                    _box.disable()
        # The Debezium ``column.exclude.list`` preview is a CDC-connector detail, so
        # show it only in the CDC context. On the Full Load screen the same
        # selection just means "not written", so the raw connector value would be
        # noise there.
        if not migration_wide:
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

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        # Collapsed by DEFAULT: this is reference-only info (no action), and the full
        # text is long -- showing it expanded on every visit adds noise and can
        # confuse the operator. They open it when they want the contract details.
        with ui.expansion("CDC behavior & limits", icon="info").classes(  # type: ignore[attr-defined]
            f"w-full {EXPANSION_PANEL_CLASSES}"
        ):
            with ui.column().classes("w-full gap-3 mt-1"):  # type: ignore[attr-defined]
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
