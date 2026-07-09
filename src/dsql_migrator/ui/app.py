# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""NiceGUI application entrypoint.

Run with::

    uv run python -m dsql_migrator.ui.app

This module wires up the UI layer as a sidebar layout: a header, a left
navigation drawer (the preliminary Connect screen plus the four workflow steps —
Evaluation, Schema Conversion, Data Migration, Validation — with their status),
and a main content area that renders the selected screen.

Session credentials entered in the Connect screen are held only in process
memory, scoped per browser session (a stable, cookie-backed browser id), so a
page refresh continues the same session instead of starting over. Credentials
are never persisted to disk, logs, reports, or job state, and are discarded when
the process ends (Property 7 / Requirement 9.2).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from dsql_migrator import __version__
from dsql_migrator.config import (
    AppConfig,
    ConnectDefaults,
    load_config,
    load_connect_defaults,
    read_env_file,
)
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.ui.connect import build_connect_page
from dsql_migrator.ui.data_migration import (
    DataMigrationStore,
    build_data_migration_screen,
    cdc_streaming_started,
    full_load_run_guard_reason,
)
from dsql_migrator.ui.evaluation import EvaluationStore, build_evaluation_screen
from dsql_migrator.ui.migration_plan import build_migration_plan_screen
from dsql_migrator.ui.schema_conversion import (
    SchemaConversionStore,
    build_schema_conversion_screen,
)
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.query_playground import (
    PlaygroundStore,
    build_query_playground_screen,
)
from dsql_migrator.ui.validation import (
    ValidationStore,
    build_cutover_screen,
    build_validation_screen,
)
from dsql_migrator.ui.workflow import (
    OptionalTool,
    WorkflowStep,
    build_workflow_sidebar,
)

# Stable view key for the Query Playground optional tool (persisted active_view).
_QUERY_PLAYGROUND_VIEW = "query_playground"

# In-memory, per-session connection state. A plain process-memory store keeps
# credentials out of any persisted storage (Property 7).
SESSION_STORE = SessionStore()

# Per-session evaluation inputs/outputs (process memory only).
EVALUATION_STORE = EvaluationStore()

# Per-session schema-conversion inputs/outputs (process memory only).
SCHEMA_CONVERSION_STORE = SchemaConversionStore()

# Per-session data-migration job id / error (process memory only).
DATA_MIGRATION_STORE = DataMigrationStore()

# Per-session validation options / report (process memory only).
VALIDATION_STORE = ValidationStore()

# Per-session Query Playground inputs/outputs (process memory only).
PLAYGROUND_STORE = PlaygroundStore()

# Runs long-running steps (e.g. introspection) off the UI event loop (Req 9.3).
JOB_MANAGER = JobManager()

# Durable per-session workbench state (attached in main()); None until then so
# tests/imports do not touch disk. Holds the latest persisted snapshot signature
# per session to skip redundant writes (large inventories are not re-serialized
# on every poll).
SESSION_STATE_STORE: object | None = None
_LAST_SESSION_SIGNATURE: dict[str, tuple] = {}

# Retention caps to bound durable-store growth across many migrations: keep the
# most recent N completed jobs and N session snapshots; resumable/active jobs are
# never pruned. Pruning runs once at startup.
_KEEP_DONE_JOBS = 100
_KEEP_SESSIONS = 200


def build_page(
    config: AppConfig,
    session_id: str,
    connect_defaults: ConnectDefaults | None = None,
) -> None:
    """Build the page content for one session.

    Renders the app as a sidebar layout: a top header, a left navigation drawer
    (the preliminary Connect screen plus the four workflow steps with their
    status), and a main content area that shows the selected screen. Only
    non-secret configuration is surfaced; credential values are never displayed
    in the UI. ``connect_defaults`` optionally prefills the Connect form for
    local development; when ``None`` the form starts blank.
    """
    # Build each step's (content_builder, runner). These only prepare closures;
    # nothing renders until the sidebar selects and invokes a screen.
    # Step 0 (Migration plan): choose the migration mode early and, for CDC modes,
    # provision the cdc-stack infrastructure in the background ("provision early").
    migration_plan_content, migration_plan_runner = build_migration_plan_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        migration_store=DATA_MIGRATION_STORE,
    )
    evaluation_content, evaluation_runner = build_evaluation_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
    )
    # Step 2 (Schema Conversion): object browsing, DDL preview, query conversion,
    # and target apply.
    # Late-bound navigation: build_workflow_sidebar hands back its ``select``
    # function (below) so the Schema Conversion screen can jump straight to Data
    # Migration when the user clicks "Skip conversion & continue".
    _nav: dict[str, Callable[[object], None]] = {}

    schema_content, schema_runner = build_schema_conversion_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        conv_store=SCHEMA_CONVERSION_STORE,
        on_continue_to_data_migration=lambda: _nav["select"](
            WorkflowStep.FULL_LOAD
        ),
        # Block applying schema while CDC is live: the sink is writing the target
        # tables and a REPLACE would drop them (DDL is not replicated). Probes the
        # same data-migration state that drives the CDC lock.
        cdc_active_check=lambda: cdc_streaming_started(
            DATA_MIGRATION_STORE.get_or_create(session_id), JOB_MANAGER
        ),
    )
    # Data Migration is a single step with an inner migration-type selector
    # (Full load only / CDC only / Full load + CDC). One builder serves it; the
    # Full Load phase drives the snapshot run, and in the combined type CDC
    # CDC step opens automatically from the Full Load watermark.
    data_migration_content, data_migration_runner = build_data_migration_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        migration_store=DATA_MIGRATION_STORE,
        conv_store=SCHEMA_CONVERSION_STORE,
        staging_bucket=config.staging_bucket,
        cdc_deploy_role_arn=config.cdc_deploy_role_arn,
        cdc_secret_kms_key_id=config.cdc_secret_kms_key_id,
        validation_store=VALIDATION_STORE,
    )
    # Step 4 (Validation): compares the migrated target against the source as-of
    # the Step 3 watermark and reports consistency and drift.
    validation_content, validation_runner = build_validation_screen(
        SESSION_STORE,
        session_id,
        job_manager=JOB_MANAGER,
        eval_store=EVALUATION_STORE,
        migration_store=DATA_MIGRATION_STORE,
        validation_store=VALIDATION_STORE,
    )
    # Step 6 (Cut over): guidance for switching the application from MySQL to
    # DSQL. The tool cannot perform/verify the cut-over, so this step has no job —
    # the runner marks it DONE on the user's acknowledgement; the content reflects
    # the last validation verdict (clean MATCH -> runbook, else "validate first").
    cutover_content, cutover_runner = build_cutover_screen(
        SESSION_STORE,
        session_id,
        validation_store=VALIDATION_STORE,
    )
    # Optional tool (not a workflow step): the Query Playground — convert a MySQL
    # statement to DSQL and non-destructively test whether it runs on the target.
    query_playground_content = build_query_playground_screen(
        SESSION_STORE,
        session_id,
        playground_store=PLAYGROUND_STORE,
    )

    def schema_run_guard() -> str | None:
        # Disable the bulk Run until the user has selected (ticked) at least one
        # object in the Schema Conversion object browser.
        conv_state = SCHEMA_CONVERSION_STORE.get_or_create(session_id)
        if not conv_state.ticked_node_ids:
            return "Select one or more objects in the Object browser first."
        return None

    def data_migration_run_guard() -> str | None:
        # Disable the Full Load Run until the Full Load prerequisite checks have
        # been run and all required checks pass (Property 14).
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        eval_state = EVALUATION_STORE.get_or_create(session_id)
        result = eval_state.result
        inventory = result.inventory if result is not None else None
        return full_load_run_guard_reason(migration_state, inventory)

    # Resume support (Property 4): restore this session's persisted snapshot once
    # per process when the in-memory session is still fresh, and persist a
    # snapshot on each state change. The save is dirty-checked by a cheap
    # signature so a large inventory is never re-serialized on every UI poll.
    def _persist_session() -> None:
        if SESSION_STATE_STORE is None:
            return
        from dsql_migrator.ui.session_persistence import (
            capture_session_snapshot,
            session_signature,
        )

        session = SESSION_STORE.get_or_create(session_id)
        eval_state = EVALUATION_STORE.get_or_create(session_id)
        conv_state = SCHEMA_CONVERSION_STORE.get_or_create(session_id)
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        validation_state = VALIDATION_STORE.get_or_create(session_id)
        signature = session_signature(
            session, eval_state, conv_state, migration_state, validation_state
        )
        if _LAST_SESSION_SIGNATURE.get(session_id) == signature:
            return
        _LAST_SESSION_SIGNATURE[session_id] = signature
        SESSION_STATE_STORE.save(
            capture_session_snapshot(
                session_id, session, eval_state, conv_state, migration_state,
                validation_state,
            )
        )

    # Restore whenever the in-memory session is still FRESH (uninitialized), not
    # just once per process. A reopened browser tab can hand back a brand-new,
    # empty session object for the same (cookie-stable) session_id; gating on
    # freshness re-hydrates it from the snapshot, while a populated/in-progress
    # session is never clobbered (session_is_fresh is False for it). This is what
    # makes "close the tab, reopen" resume instead of showing a blank workflow.
    if SESSION_STATE_STORE is not None:
        from dsql_migrator.ui.session_persistence import (
            apply_session_snapshot,
            session_is_fresh,
        )

        _session = SESSION_STORE.get_or_create(session_id)
        _eval = EVALUATION_STORE.get_or_create(session_id)
        _conv = SCHEMA_CONVERSION_STORE.get_or_create(session_id)
        _mig = DATA_MIGRATION_STORE.get_or_create(session_id)
        _val = VALIDATION_STORE.get_or_create(session_id)
        if session_is_fresh(_session, _eval, _mig):
            _snapshot = SESSION_STATE_STORE.load(session_id)
            if _snapshot is not None:
                apply_session_snapshot(
                    _snapshot, _session, _eval, _conv, _mig, _val
                )
                # Back-compat: CDC was folded into the unified Data Migration nav
                # step (WorkflowStep.FULL_LOAD). A session saved while viewing the
                # old standalone "cdc" nav step would restore an active_view that
                # no longer maps to a step_content entry; redirect it so the page
                # opens the Data Migration step instead of a blank view.
                if _session.active_view == WorkflowStep.CDC.value:
                    _session.set_active_view(WorkflowStep.FULL_LOAD.value)

    def _reset_session() -> None:
        # "Start over": wipe ALL per-session in-memory state + the durable
        # snapshot for this session. Clears only the tool's workbench -- never any
        # AWS resource. Use reset_in_place (not clear/pop): the workflow screen and
        # its content builders captured these state objects in closures at build
        # time, and Start over does NOT rebuild the page (it just refreshes). A
        # pop+recreate would orphan those captured references -- e.g. re-verifying
        # the connections would update a NEW session object while the nav guard
        # still reads the old (locked) one, so steps never unlock. Resetting the
        # SAME objects in place keeps every closure pointing at the live, wiped
        # state.
        SESSION_STORE.reset_in_place(session_id)
        EVALUATION_STORE.reset_in_place(session_id)
        SCHEMA_CONVERSION_STORE.reset_in_place(session_id)
        DATA_MIGRATION_STORE.reset_in_place(session_id)
        if VALIDATION_STORE is not None:
            try:
                VALIDATION_STORE.reset_in_place(session_id)
            except Exception:  # noqa: BLE001 - reset is best-effort
                pass
        try:
            PLAYGROUND_STORE.reset_in_place(session_id)
        except Exception:  # noqa: BLE001 - reset is best-effort
            pass
        if SESSION_STATE_STORE is not None:
            SESSION_STATE_STORE.delete(session_id)
        _LAST_SESSION_SIGNATURE.pop(session_id, None)

    def _cdc_deployed() -> bool:
        """True when ANY CDC AWS resource exists, so Start over can offer to tear it
        down. Existence -- not health -- is what matters: a connector or stack that
        is FAILED / mid-rollback / half-deployed still bills for MSK / NAT and must
        be offered for teardown just like a running one.

        So this is truthy when either (a) any of MY connectors exist in ANY state
        (``cdc_connector_names`` is populated existence-based by ``_filter_mine``,
        regardless of RUNNING), or (b) the cdc-stack phase is anything other than
        ``absent`` -- i.e. ``running`` / ``infra`` / ``unstable`` (a stuck/rolled-
        back stack is still deployed). Only ``absent`` (no stack at all) is safe.
        This matches the CDC step, which already offers Delete for the ``unstable``
        phase."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        if getattr(migration_state, "cdc_connector_names", None):
            return True
        phase = getattr(migration_state, "cdc_stack_phase", None)
        return phase is not None and phase != "absent"

    def _cdc_stack_name() -> Optional[str]:
        """The session's current cdc-stack name, so Start over's warning can name a
        custom (non-default) stack that a fresh session would not re-discover."""
        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        return getattr(migration_state, "cdc_stack_name", None)

    def _cdc_teardown_in_flight() -> bool:
        """True when a CDC stop/delete is CURRENTLY running, so Start over must not
        race it. Two authoritative signals, both refreshed by ``_cdc_probe`` just
        before the Start-over dialog opens:

        (a) a local lifecycle job for a stop/delete is still PENDING/RUNNING, or
        (b) the freshly-probed cdc-stack status is a live CloudFormation operation
            (ends in ``_IN_PROGRESS`` -- e.g. ``DELETE_IN_PROGRESS``). We test the
            raw status, NOT the coarse ``unstable`` phase, so a settled but stuck
            stack (``ROLLBACK_COMPLETE`` / ``DELETE_FAILED``) is NOT over-blocked --
            the user should still be able to Start over and choose to delete it.

        Resetting mid-teardown would fire a second background teardown and then wipe
        the session, leaving the in-flight delete invisible (and, for a custom stack
        name, unre-discoverable) -- so we block the reset while this is true.
        """
        from dsql_migrator.ui.data_migration._status import (
            _current_job,
            _is_inflight_stack_status,
        )

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        job = _current_job(JOB_MANAGER, getattr(migration_state, "cdc_deploy_job_id", None))
        if (
            job is not None
            and getattr(job, "status", None) in ("PENDING", "RUNNING")
            and getattr(migration_state, "cdc_action_kind", None) in ("stop", "delete")
        ):
            return True
        return _is_inflight_stack_status(
            getattr(migration_state, "cdc_stack_phase_status", None)
        )

    def _cdc_probe() -> None:
        """Refresh the cached CDC deployment state from a live, read-only AWS probe.

        Called (off the event loop) when the user opens Start over, so the dialog
        reflects the ACTUAL deployed CDC -- and offers the stop/delete tiles --
        regardless of which step the user was on. ``_ensure_cdc_controller`` is
        throttled per session; clear the throttle timestamp first so this explicit
        user action always gets a fresh read. Best-effort and read-only."""
        from dsql_migrator.ui.data_migration._status import _ensure_cdc_controller

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        session = SESSION_STORE.get_or_create(session_id)
        migration_state._cdc_discovery_monotonic = None  # bypass the render throttle
        try:
            _ensure_cdc_controller(migration_state, session)
        except Exception:  # noqa: BLE001 - leave cached state; dialog opens regardless
            pass

    def _cdc_teardown_on_reset(mode: str) -> None:
        """Submit a CDC teardown as part of Start over (called BEFORE the reset).

        ``mode`` is ``"stop"`` (delete only the 2 MSK connectors, keep MSK/VPC/
        IAM for a fast restart) or ``"delete"`` (tear down the whole cdc-stack).
        All config is captured into the job closure now, so the imminent session
        reset cannot race it; the teardown runs in the background (no UI log sink).
        """
        from dsql_migrator.core.cdc_deployer import (
            build_cdc_stack_deployer,
            run_cdc_delete,
            run_cdc_stop,
        )

        migration_state = DATA_MIGRATION_STORE.get_or_create(session_id)
        session = SESSION_STORE.get_or_create(session_id)
        target = getattr(session, "target_config", None)
        region = getattr(target, "region", None) if target else None
        if not region and target is not None:
            endpoint = getattr(target, "cluster_endpoint", "") or ""
            if ".dsql." in endpoint and ".on.aws" in endpoint:
                region = endpoint.split(".dsql.")[1].split(".on.aws")[0]
        stack_name = getattr(migration_state, "cdc_stack_name", None)
        if not region or not stack_name:
            return
        aws_profile = getattr(session, "aws_profile", None)
        role_arn = getattr(migration_state, "cdc_deploy_role_arn", None)
        deployer = build_cdc_stack_deployer(
            region, aws_profile=aws_profile, assume_role_arn=role_arn
        )
        if mode == "delete":
            cleanup_secret = not getattr(session, "source_secret_id", None)

            def work(handle) -> None:
                run_cdc_delete(
                    handle,
                    stack_name=stack_name,
                    deployer=deployer,
                    on_log=lambda _ts, _msg: None,
                    region=region,
                    aws_profile=aws_profile,
                    cleanup_source_secret=cleanup_secret,
                )

        else:

            def work(handle) -> None:
                run_cdc_stop(
                    handle,
                    stack_name=stack_name,
                    deployer=deployer,
                    on_log=lambda _ts, _msg: None,
                )

        job_id = JOB_MANAGER.submit(work)
        # The teardown runs in the background after the session resets, so poll it
        # and surface a completion toast — the user knows when the connectors (or
        # the whole stack) are gone and a fresh migration can start.
        from nicegui import ui

        if mode == "delete":
            started = "Deleting CDC infrastructure in the background (~45 min)…"
            done = "CDC infrastructure deleted — MSK/NAT billing stopped."
        else:
            started = "Removing the CDC connectors in the background…"
            done = "CDC connectors removed — you can start a new migration."
        ui.notify(started, type="info", position="top", timeout=6000)  # type: ignore[attr-defined]

        timer_ref: dict = {"t": None}

        def _poll() -> None:
            try:
                status = JOB_MANAGER.get_status(job_id).status
            except Exception:  # noqa: BLE001 - treat an unreadable job as failed
                status = "FAILED"
            if status not in ("DONE", "FAILED", "CANCELLED"):
                return
            if timer_ref["t"] is not None:
                timer_ref["t"].active = False
            if status == "DONE":
                ui.notify(done, type="positive", position="top", timeout=8000)  # type: ignore[attr-defined]
            else:
                ui.notify(  # type: ignore[attr-defined]
                    "CDC teardown did not complete cleanly — check the CDC step or "
                    "CloudWatch.",
                    type="warning",
                    position="top",
                    timeout=8000,
                )

        timer_ref["t"] = ui.timer(10.0, _poll)  # type: ignore[attr-defined]

    build_workflow_sidebar(
        SESSION_STORE,
        session_id,
        app_title="MySQL to Aurora DSQL Migration Tool",
        version=__version__,
        connect_builder=lambda go_to_first_step, on_connection_change: (
            build_connect_page(
                SESSION_STORE,
                session_id,
                on_next=go_to_first_step,
                on_connection_change=on_connection_change,
                defaults=connect_defaults,
            )
        ),
        step_content={
            WorkflowStep.MIGRATION_PLAN: migration_plan_content,
            WorkflowStep.EVALUATION: evaluation_content,
            WorkflowStep.SCHEMA_CONVERSION: schema_content,
            WorkflowStep.FULL_LOAD: data_migration_content,
            WorkflowStep.VALIDATION: validation_content,
            WorkflowStep.CUT_OVER: cutover_content,
        },
        runners={
            WorkflowStep.MIGRATION_PLAN: migration_plan_runner,
            WorkflowStep.EVALUATION: evaluation_runner,
            WorkflowStep.SCHEMA_CONVERSION: schema_runner,
            WorkflowStep.FULL_LOAD: data_migration_runner,
            WorkflowStep.VALIDATION: validation_runner,
            WorkflowStep.CUT_OVER: cutover_runner,
        },
        run_guards={
            WorkflowStep.SCHEMA_CONVERSION: schema_run_guard,
            WorkflowStep.FULL_LOAD: data_migration_run_guard,
        },
        on_state_change=_persist_session,
        nav_export=lambda select_fn: _nav.__setitem__("select", select_fn),
        footer_extra=lambda: _render_footer_tools(config.activity_log_path),
        on_reset=_reset_session,
        on_reset_cdc=_cdc_teardown_on_reset,
        cdc_deployed_getter=_cdc_deployed,
        cdc_stack_name_getter=_cdc_stack_name,
        cdc_teardown_in_flight_getter=_cdc_teardown_in_flight,
        cdc_probe=_cdc_probe,
        optional_tools={
            _QUERY_PLAYGROUND_VIEW: OptionalTool(
                view_key=_QUERY_PLAYGROUND_VIEW,
                label="Query validation",
                caption="Optional · Convert & test app queries",
                icon="science",
                content=query_playground_content,
            ),
        },
    )


def _render_footer_tools(activity_log_path: str) -> None:
    """Render the sidebar footer tools: activity-log download + Diagnostics."""
    _render_activity_log_download(activity_log_path)
    _render_performance_tuning_controls()
    _render_diagnostics_controls()


def _render_performance_tuning_controls() -> None:
    """Render runtime Full Load / Validation parallelism controls in the footer.

    Like Diagnostics, these are NOT deploy-time inputs: the loader and validator
    re-read the config on every run, so an operator can retune parallelism between
    runs from here -- no redeploy/restart. Changes apply to the NEXT Full Load /
    Validation, are app-wide (single-task app), and reset to the deploy/startup
    values on restart. Each field is bounded by the same limits as the config.
    """
    from nicegui import ui

    from dsql_migrator.config import (
        TUNABLE_KNOBS,
        TuningValueError,
        current_tuning_values,
        set_tuning_value,
    )
    from dsql_migrator.ui.design import render_notice

    current = current_tuning_values()

    with ui.expansion("Performance tuning", icon="speed").props("dense").classes(
        "w-full"
    ):
        # Cloudscape "form" treatment, kept compact for the narrow sidebar: a
        # single-line info Alert with the operational caveat up top, then the
        # knobs laid out as grouped form fields -- one dense row per knob
        # (label + allowed range + bounded input), with the longer description
        # moved to a hover tooltip (Cloudscape's "info" idiom) so each field
        # stays a single line.
        render_notice(
            ui,
            tone="info",
            header="Applies to the next run",
            body="Live, app-wide; resets on restart. Connections ≈ tables × batches.",
        )

        def _on_change(event: object, k) -> None:
            raw = getattr(event, "value", None)
            try:
                applied = set_tuning_value(k.field, raw)
            except TuningValueError as exc:
                ui.notify(str(exc), type="warning", position="top")
                return
            ui.notify(
                f"{k.label} = {applied} (applies to the next run).",
                type="info",
            )

        current_group: str | None = None
        for knob in TUNABLE_KNOBS:
            # Emit a Cloudscape-style section subheader when the group changes
            # (knobs are ordered by group, so this groups them into "Full Load"
            # / "Validation" sections without re-sorting).
            if knob.group != current_group:
                ui.label(knob.group).classes(
                    "text-xs uppercase tracking-wide text-gray-500 font-medium "
                    "mt-2 mb-0"
                )
                current_group = knob.group

            # One compact Cloudscape "form field" per row: label (+ range hint)
            # on the left, a small info glyph carrying the description tooltip,
            # and the bounded input on the right.
            with ui.row().classes("items-center gap-1 no-wrap w-full"):
                with ui.column().classes("gap-0 flex-1 min-w-0"):
                    with ui.row().classes("items-center gap-1 no-wrap"):
                        ui.label(knob.short_label).classes(
                            "text-sm text-gray-900 truncate"
                        )
                        ui.icon("info").classes(
                            "text-gray-400 text-xs cursor-help"
                        ).tooltip(knob.description)
                    ui.label(f"{knob.minimum}–{knob.maximum}").classes(
                        "text-xs text-gray-400 leading-none"
                    )
                ui.number(
                    value=current[knob.field],
                    min=knob.minimum,
                    max=knob.maximum,
                    step=1,
                    format="%d",
                    on_change=lambda e, k=knob: _on_change(e, k),
                ).props("dense outlined").classes("w-20 text-sm")


def _render_diagnostics_controls() -> None:
    """Render runtime troubleshooting controls in the sidebar footer.

    Deployment is kept parameter-light: the log level and the optional CloudWatch
    mirror are NOT deploy-time inputs but are adjusted here at runtime, so an
    operator can flip INFO<->DEBUG (DEBUG adds failure stacktraces) and start/stop
    mirroring the activity log to stdout (forwarded to CloudWatch on ECS) while
    troubleshooting -- no redeploy. Changes apply app-wide (single-task app) and
    reset to the startup defaults on restart.
    """
    import logging

    from nicegui import ui

    from dsql_migrator.core.activity_log import (
        activity_stdout_enabled,
        configure_activity_stdout_log,
        current_activity_log_level,
        disable_activity_stdout_log,
        set_activity_log_level,
    )

    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    current = logging.getLevelName(current_activity_log_level())
    if current not in levels:
        current = "INFO"

    with ui.expansion("Diagnostics", icon="tune").props("dense").classes("w-full"):
        ui.label(
            "Live, app-wide; resets on restart."
        ).classes("text-xs text-gray-400")

        def _on_level(event: object) -> None:
            value = str(getattr(event, "value", "INFO"))
            set_activity_log_level(getattr(logging, value, logging.INFO))
            ui.notify(f"Log level set to {value}.", type="info")

        ui.select(
            levels, value=current, label="Log level", on_change=_on_level
        ).props("dense outlined").classes("w-full text-xs")

        def _on_toggle(event: object) -> None:
            if bool(getattr(event, "value", False)):
                configure_activity_stdout_log(level=current_activity_log_level())
                ui.notify(
                    "Mirroring activity log to stdout (CloudWatch on ECS).",
                    type="info",
                )
            else:
                disable_activity_stdout_log()
                ui.notify("Stopped mirroring activity log to stdout.", type="info")

        ui.switch(
            "Send to CloudWatch (stdout)",
            value=activity_stdout_enabled(),
            on_change=_on_toggle,
        ).props("dense").classes("text-xs")


def _render_activity_log_download(activity_log_path: str) -> None:
    """Render a global "Download activity log" button in the sidebar footer.

    Always available so the operator can pull the full UTC, one-line-per-event
    timeline (connection / assessment / schema apply / Full Load / CDC) whenever
    needed, independent of which step is open. Downloads the human-readable text
    rendering; the raw NDJSON file remains on disk for tooling.
    """
    from nicegui import ui

    from dsql_migrator.core.activity_log import read_activity_log

    def _download() -> None:
        data = read_activity_log(activity_log_path, "text")
        if not data:
            ui.notify("No activity has been logged yet.", type="info")
            return
        ui.download(data, "migration_activity.log")

    ui.button("Download activity log", on_click=_download).props(
        "flat dense icon=download size=sm"
    ).classes("text-xs")


def main() -> None:
    """Configure and launch the NiceGUI application."""
    import secrets as _secrets

    from nicegui import app, ui

    # AWS Console / Cloudscape uses sentence-case button labels, but Quasar (the
    # NiceGUI button backend) defaults to ALL-CAPS. Set the default once here so
    # every button in the app reads "Run" / "Deploy" rather than a mix of
    # "RUN" and "Run" -- a single source of truth instead of per-button no-caps.
    ui.button.default_props("no-caps")

    config = load_config()

    # On Fargate the task has credentials (container provider) but no region, so
    # any region-less boto3 client (e.g. the AI-assist Bedrock client when
    # BEDROCK_REGION is blank) would raise NoRegionError. Seed AWS_DEFAULT_REGION
    # from DSQL_MIGRATOR_AWS_REGION (= ${AWS::Region}) when nothing else set it, so
    # every region-less client has a floor. Clients that parse a region from their
    # endpoint (DSQL/Secrets/CDC) are unaffected.
    from dsql_migrator.core.aws_session import ensure_default_region

    ensure_default_region(config.aws_region)

    # Configure the tool's logger with timestamps and the configured level so
    # the tool's messages (e.g. per-table Full Load failures) are timestamped
    # and filterable in the terminal. Done explicitly on the package logger so
    # it applies regardless of how the web server configures the root logger.
    # Honors DSQL_MIGRATOR_LOG_LEVEL.
    _level = getattr(logging, str(config.log_level).upper(), logging.INFO)
    _pkg_logger = logging.getLogger("dsql_migrator")
    _pkg_logger.setLevel(_level)
    if not _pkg_logger.handlers:
        _handler = logging.StreamHandler()
        _handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )
        _pkg_logger.addHandler(_handler)
        _pkg_logger.propagate = False

    # Structured activity log (UTC, one JSON line per event), downloadable from
    # the UI: connection tests, assessment, per-object schema apply, per-table
    # Full Load outcomes, and CDC control-plane actions are appended here so the
    # operator has an auditable, time-sortable record of the whole migration.
    from dsql_migrator.core.activity_log import (
        ActivityCategory,
        ActivityStatus,
        configure_activity_file_log,
        configure_activity_stdout_log,
        log_activity,
    )

    # The activity log is an audit convenience, NOT a startup prerequisite: if its
    # path is unwritable (e.g. a non-root container whose WORKDIR /app is read-only
    # and ACTIVITY_LOG_PATH was left at the relative default), opening the rotating
    # file raises PermissionError/OSError. That must NOT crash the app on boot --
    # previously it sent the container into a restart loop and ECS rolled the
    # deploy back as NotStabilized. Fall back to stdout-only logging and continue.
    activity_to_stdout = config.activity_log_to_stdout
    try:
        configure_activity_file_log(config.activity_log_path, level=_level)
    except OSError as exc:
        activity_to_stdout = True  # ensure the audit trail still goes somewhere
        _pkg_logger.warning(
            "activity log file %s is not writable (%s); falling back to stdout-only "
            "activity logging. Set DSQL_MIGRATOR_ACTIVITY_LOG_PATH to a writable "
            "path (e.g. /tmp/migration_activity.log) to keep the rotating file.",
            config.activity_log_path, exc,
        )
    # Optional: also stream activity events to stdout so the container's awslogs
    # driver forwards them to CloudWatch Logs (durable, survives task
    # replacement). Off by default; the rotating file remains the local copy.
    if activity_to_stdout:
        configure_activity_stdout_log(level=_level)
    log_activity(
        ActivityCategory.SYSTEM, "app started", status=ActivityStatus.INFO
    )

    # Durable job state (resumability, Property 4): persist Full Load job
    # snapshots to the configured SQLite file and reload any interrupted jobs so
    # an app restart does not lose progress. Interrupted in-flight jobs are
    # reconciled to FAILED on load so the "retry failed tables" path resumes the
    # unfinished work.
    from dsql_migrator.core.job_store import SqliteJobStore

    JOB_MANAGER.attach_store(SqliteJobStore(config.job_state_path))
    # Bound growth: drop all but the most recent completed jobs (resumable/active
    # jobs are never pruned).
    JOB_MANAGER.prune_terminal(_KEEP_DONE_JOBS)

    # Durable per-session state (resumability, Property 4): persist each
    # session's non-secret workbench state so a reconnecting browser resumes
    # where it left off after a restart.
    global SESSION_STATE_STORE
    from dsql_migrator.core.session_state_store import SqliteSessionStateStore

    SESSION_STATE_STORE = SqliteSessionStateStore(config.session_state_path)
    SESSION_STATE_STORE.prune(_KEEP_SESSIONS)

    # Dev convenience: prefill the Connect form from the local .env / environment
    # (gitignored). os.environ takes precedence over the .env file. Source fields
    # reuse DB_*; target fields use TARGET_*.
    project_root = Path(__file__).resolve().parents[3]
    merged_env = {**read_env_file(str(project_root / ".env")), **os.environ}
    connect_defaults = load_connect_defaults(merged_env)

    # Secret used to sign the browser session cookie that backs
    # ``app.storage.browser``. A configured value (DSQL_MIGRATOR_STORAGE_SECRET)
    # keeps the SAME browser id across process restarts AND across closing/
    # reopening the browser, so the persisted session snapshot is found and the
    # user resumes where they left off (e.g. a CDC deploy in flight) instead of
    # landing on a fresh session. Without it a per-process random secret is used,
    # which only survives page refreshes within one running process. Read from the
    # merged env (.env + os.environ, os.environ wins) like the other dev settings;
    # it is a secret, so it is intentionally NOT placed on the log-safe AppConfig.
    storage_secret = (
        merged_env.get("DSQL_MIGRATOR_STORAGE_SECRET") or _secrets.token_urlsafe(32)
    )

    @ui.page("/")
    def _index() -> None:
        # Identify the session by the stable, cookie-backed browser id rather
        # than the per-connection client id, so reloading the page continues the
        # same session (workflow progress, verified connections, results) instead
        # of starting over.
        session_id = app.storage.browser["id"]
        build_page(config, session_id, connect_defaults)

    ui.run(
        host=config.app_host,
        port=config.app_port,
        title="DSQL Migration Tool",
        reload=False,
        show=False,
        storage_secret=storage_secret,
        # Behind an ALB on Fargate the UI WebSocket can briefly drop (load-balancer
        # connection recycling, brief network blips). NiceGUI's default 3s
        # reconnect window is too short to ride those out, so the page would give
        # up and reload -- losing the workbench view mid-deploy even though the
        # job runs server-side. A longer window reconnects to the same task and
        # keeps the long-running CDC/Full-Load session visible.
        reconnect_timeout=60.0,
    )


if __name__ in {"__main__", "__mp_main__"}:
    main()
