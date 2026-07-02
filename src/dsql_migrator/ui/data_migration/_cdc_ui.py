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
    build_cdc_stack_params,
    cdc_expected_connector_names,
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
    build_migration_table_status,
    cdc_handling_facts,
    connector_health_rows,
    connector_role_label,
    format_column_exclude_list,
    format_duration,
    lob_exclusion_candidates,
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
    _migration_status_tables,
    _read_cdc_template_body,
    cdc_error_log_key,
)
from dsql_migrator.ui.design import (
    NOTICE_STYLE,
    inline_hint,
    render_notice,
)

# These four names live in the package ``__init__``: the activity-log anchor
# ``_log_cdc_event`` (a monkeypatch target for the connector-transition logger that
# stays there), the ``migration_type_lock_reason`` helper, and the ``_LOGGER`` /
# ``_render_notice`` module constants. ``__init__`` imports THIS module for
# re-export, so the import below is only safe because ``__init__`` performs that
# re-export at the very bottom -- after these four names are already bound. The
# moved functions reference them as module globals, so they must be real imports
# here (a module-level ``__getattr__`` would not satisfy a function's LOAD_GLOBAL).
from dsql_migrator.ui.data_migration import (
    _log_cdc_event,
    migration_type_lock_reason,
    _LOGGER,
    _render_notice,
)


# How often the CDC step polls MSK Connect + the DSQL target for live status.
# Slower than the Full Load poll: these are network round-trips to AWS/DSQL, and
# connector state / replication lag change on the order of seconds, not 0.5s.
_CDC_POLL_INTERVAL_SECONDS = 5.0

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
