"""Migration plan screen: decide whether CDC is in scope, provision CDC infra early.

This is the first workflow step after Connect. Mature migration tooling separates
three concerns -- connection (mode-agnostic), the migration MODE, and (for CDC)
the streaming infrastructure -- so this step owns the latter two:

1. **CDC decision** (Include CDC? Yes/No). The step's only durable effect is
   whether CDC streaming infrastructure is provisioned early, so it asks exactly
   that rather than the full three-way type tiles (Full Load always runs; the type
   is freely changeable on Data Migration, and Full Load + CDC vs CDC-only is
   picked there). The answer writes the session ``migration_type`` enum
   (No -> FULL_LOAD_ONLY, Yes -> FULL_LOAD_AND_CDC) so every later step reads it;
   it is reversible (the user can return and add CDC after starting Full-load-only).
2. **CDC infrastructure (provision early, use late)**. When the mode includes CDC,
   the BYO-VPC inputs are collected here and the cdc-stack infra deploy
   (``create_stack``: MSK Serverless, networking, plugins, IAM -- ~15-20 min, no
   connectors yet) can be kicked off in the BACKGROUND. The user proceeds through
   Evaluation / Schema Conversion while MSK warms up, so by the time they reach
   Start CDC the stack is ready.

The screen reuses the Data Migration screen's mode-selector, infra form, deploy
trigger, and live progress log (one source of truth for the CDC lifecycle UI).
The mode/infra inputs live on the SESSION, and the bound ``DataMigrationState``
reads through to it, so a deploy started here is visible on the CDC sub-step too.
"""

from __future__ import annotations

from typing import Callable, Optional

from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.ui.design import inline_hint, render_notice, section_header
from dsql_migrator.ui.data_migration import (
    DataMigrationStore,
    MigrationType,
    _open_cdc_infra_dialog,
    _probe_cdc_stack_phase,
    _render_cdc_decision,
    _render_cdc_deploy_live,
    _render_cdc_infra_form,
    _start_cdc_infra_deploy,
    migration_type_locked,
)
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    StepStatus,
    WorkflowStep,
    with_status,
)

# The two modes that need CDC streaming infrastructure.
_CDC_MODES = (MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC)


def build_migration_plan_screen(
    store: SessionStore,
    session_id: str,
    *,
    job_manager: JobManager,
    migration_store: DataMigrationStore,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Migration plan screen, returning ``(content_builder, runner)``.

    ``content_builder`` renders the mode tiles and, for CDC modes, the BYO-VPC
    infra form + background-deploy trigger + live progress. ``runner`` marks the
    step DONE ("plan confirmed") -- it is a choice, not a long-running job, and it
    deliberately does NOT block on the (optional, background) infra deploy so the
    user can proceed to Evaluation while MSK provisions.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    migration_state = migration_store.get_or_create(session_id)
    # Bind the session so the mode tiles + infra form read/write the session-level
    # migration_type / cdc_infra_inputs (the authoritative early-choice store).
    migration_state.bind_session(session)

    def runner() -> None:
        """Confirm the plan: mark the step DONE so Evaluation unlocks."""
        session.set_workflow(
            with_status(session.workflow, WorkflowStep.MIGRATION_PLAN, StepStatus.DONE)
        )

    def content(refresh: Callable[[], None]) -> None:
        status = _migration_plan_status(session)

        with ui.card().classes("w-full"):
            section_header(ui, icon="alt_route", title="Will this migration use CDC?")
            ui.label(
                "The one thing to decide up front is whether you need continuous "
                "replication (CDC). If yes, its streaming infrastructure (MSK, "
                "~15-20 min) is provisioned now so it is ready by the time you "
                "stream — you continue through Evaluation while it warms up. Full "
                "Load always runs; you pick the exact tables and pattern on the "
                "Data Migration step, and you can change this later."
            ).classes("text-sm text-gray-600")
            # The Migration Plan's only durable effect is the CDC-infra gate, so it
            # asks exactly that (Include CDC? Yes/No) rather than the full three-way
            # type tiles. Writes the same session-bound migration_type enum
            # (No → FULL_LOAD_ONLY, Yes → FULL_LOAD_AND_CDC); the finer Full Load +
            # CDC vs CDC-only choice stays on the Data Migration step. Locked once
            # the migration has started (a job in flight, or CDC infrastructure
            # deployed/streaming) so it cannot change out from under billable
            # resources.
            _render_cdc_decision(
                ui,
                migration_state,
                status=status,
                refresh=refresh,
                locked=migration_type_locked(
                    migration_state, job_manager, status=status
                ),
            )

        # CDC modes: collect BYO-VPC inputs + offer an early background deploy.
        if session.migration_type in _CDC_MODES:
            _render_infra_section(
                ui, migration_state, job_manager, refresh, session=session
            )

    return content, runner


def _migration_plan_status(session) -> StepStatus:
    """Return the Migration plan step's current status from the session workflow."""
    from dsql_migrator.ui.workflow import get_status

    return get_status(session.workflow, WorkflowStep.MIGRATION_PLAN)


def _render_infra_section(
    ui, migration_state, job_manager, refresh, *, session
) -> None:
    """Render the CDC infrastructure form + provision-early deploy + live progress."""
    deploy_job = _current_deploy_job(job_manager, migration_state)
    deploying = deploy_job is not None and deploy_job.status in ("PENDING", "RUNNING")
    deployed_ok = deploy_job is not None and deploy_job.status == "DONE"

    # Probe the real cdc-stack phase (read-only describe_stacks) so a stack that
    # already exists -- e.g. deployed in a previous session/browser tab, where the
    # in-memory deploy_job is gone -- is recognized instead of showing the VPC form
    # again. Only when no live deploy job is driving the view.
    # Read the CACHED stack phase (no blocking call during render). When no deploy
    # job is driving the view and we have not probed this session yet, run the
    # blocking describe_stacks OFF the event loop via a one-shot timer, then
    # refresh -- mirroring the CDC step's off-loop discovery so render never blocks
    # the NiceGUI WebSocket. ``cdc_stack_phase_checked`` (set by the probe) gates
    # re-arming so this happens once per session (reset clears it).
    probed_phase = getattr(migration_state, "cdc_stack_phase", None)
    if deploy_job is None and not getattr(
        migration_state, "cdc_stack_phase_checked", False
    ):
        from nicegui import run as _probe_run

        async def _probe_phase_async() -> None:
            try:
                await _probe_run.io_bound(
                    _probe_cdc_stack_phase, migration_state, session
                )
            except Exception:  # noqa: BLE001 - best effort; falls back to the form
                pass
            # Refresh ONLY when the probe recorded a result (sets
            # cdc_stack_phase_checked). On a persistent failure it does not, so
            # skipping the refresh here avoids a refresh -> re-render -> re-arm ->
            # probe loop; a later user interaction re-renders and retries.
            if getattr(migration_state, "cdc_stack_phase_checked", False):
                refresh()

        ui.timer(0.05, _probe_phase_async, once=True)  # type: ignore[attr-defined]
    already_deployed = deployed_ok or probed_phase in ("infra", "running", "unstable")

    with ui.card().classes("w-full"):
        with ui.row().classes("items-center gap-2 no-wrap w-full"):
            ui.icon("cloud_upload", color="primary").classes("text-2xl")
            ui.label("CDC streaming infrastructure").classes("text-lg font-semibold")
            ui.space()
            if deploying:
                # In-progress: a FILLED primary badge + animated spinner so the
                # "deploying now" state reads at a glance (not a quiet outline).
                with ui.row().classes("items-center gap-1 no-wrap"):
                    ui.spinner(size="sm", color="primary")
                    ui.badge("Deploying…", color="primary")  # filled (no outline)
            else:
                badge = (
                    ("Streaming", "positive") if probed_phase == "running"
                    else ("Ready", "positive") if already_deployed
                    else ("Not deployed", "grey")
                )
                ui.badge(badge[0], color=badge[1]).props("outline")

        # Live progress for an in-flight (or finished) infra deploy.
        if deploy_job is not None:
            _render_cdc_deploy_live(ui, migration_state, job_manager, refresh)
            if deploying:
                return  # hide the form/button while a deploy is in flight

        # Already deployed (this or a prior session): do NOT show the VPC form
        # again. Confirm it exists and point the user to the CDC step to Start.
        if already_deployed:
            stack = getattr(migration_state, "cdc_stack_name", "the cdc-stack")
            if probed_phase == "running":
                body = (
                    f"CDC infrastructure '{stack}' is already deployed and streaming. "
                    "To change or tear it down, use the CDC step's controls."
                )
            else:
                body = (
                    f"CDC infrastructure '{stack}' is already deployed and ready. "
                    "Start streaming from the Data Migration → CDC step. No need to "
                    "re-enter your VPC; to change or tear it down, use the CDC "
                    "step's controls."
                )
            render_notice(
                ui, tone="success", header="CDC infrastructure ready", body=body
            )
            return

        render_notice(
            ui,
            tone="info",
            header="This migration includes CDC",
            body=(
                "Provide your VPC and plugin/source details, then deploy the "
                "infrastructure now so MSK (~15-20 min) is ready by the time you "
                "start streaming. You can continue to Evaluation while it deploys "
                "in the background."
            ),
        )

        # The BYO-VPC inputs (prefilled from target/source config where known).
        _render_cdc_infra_form(ui, migration_state, session=session)

        async def _deploy_and_confirm_plan() -> None:
            # Deploying CDC infrastructure is a definitive "plan confirmed" signal:
            # mark the Migration plan step DONE so the workflow flow advances (and
            # survives a reconnect) without requiring a separate Confirm click.
            # _start_cdc_infra_deploy is async (it offloads its AWS round-trips) --
            # await it so the deploy actually starts and failures surface, instead
            # of creating an un-awaited coroutine that silently no-ops.
            await _start_cdc_infra_deploy(
                ui, migration_state, job_manager, refresh, session=session
            )
            session.set_workflow(
                with_status(
                    session.workflow, WorkflowStep.MIGRATION_PLAN, StepStatus.DONE
                )
            )

        async def _confirm() -> None:
            # _open_cdc_infra_dialog is async (it runs the VPC diagnosis via
            # run.io_bound), so it MUST be awaited -- otherwise the dialog never
            # opens and the "Deploy CDC infrastructure" button appears to do nothing.
            await _open_cdc_infra_dialog(
                ui, migration_state,
                _deploy_and_confirm_plan,
                session=session,
            )

        ui.button(
            "Deploy CDC infrastructure", on_click=_confirm, icon="cloud_upload"
        ).props("color=primary no-caps")
        inline_hint(
            ui,
            "Optional now — you can deploy later from the CDC step. Proceeding to "
            "Evaluation does not require the infrastructure to finish.",
        )


def _current_deploy_job(job_manager, migration_state):
    """Best-effort fetch of the current CDC lifecycle job (or None)."""
    from dsql_migrator.core.job_manager import JobNotFoundError

    job_id = getattr(migration_state, "cdc_deploy_job_id", None)
    if job_id is None:
        return None
    try:
        return job_manager.get_status(job_id)
    except JobNotFoundError:
        return None


__all__ = ["build_migration_plan_screen"]
