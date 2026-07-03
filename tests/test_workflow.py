"""Unit tests for the four-step workflow shell logic (NiceGUI-agnostic).

Covers step ordering/titles, status reads and transitions, prerequisite gating
guidance (advisory, not a hard block — Requirement 8.6), status display mappings
(Requirement 8.7), navigation helpers, and per-session workflow state isolation.
None of these tests import or render NiceGUI.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import StepStatus, WorkflowState
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    WorkflowStep,
    gating_message,
    get_status,
    is_prerequisite_met,
    next_step,
    ordered_steps,
    prerequisite,
    previous_step,
    status_color,
    status_icon,
    status_label,
    step_definitions,
    step_run_label,
    step_title,
    with_status,
)


def test_ordered_steps_follow_the_migration_sequence() -> None:
    # Migration plan is the first post-Connect step (choose the mode early);
    # Data Migration is a single nav step (backed by WorkflowStep.FULL_LOAD); the
    # Full load / CDC / both choice is an inner type selector, so CDC is no longer
    # a separate nav step. Cut over is the final step (operational guidance).
    assert ordered_steps() == (
        WorkflowStep.MIGRATION_PLAN,
        WorkflowStep.EVALUATION,
        WorkflowStep.SCHEMA_CONVERSION,
        WorkflowStep.FULL_LOAD,
        WorkflowStep.VALIDATION,
        WorkflowStep.CUT_OVER,
    )


def test_step_enum_values_match_workflow_state_fields() -> None:
    # Each step value must be a real field on WorkflowState so status read/write
    # by attribute name is valid.
    fields = set(WorkflowState.model_fields)
    assert {step.value for step in WorkflowStep} == fields


def test_step_titles_are_english_human_readable() -> None:
    assert step_title(WorkflowStep.MIGRATION_PLAN) == "Migration plan"
    assert step_title(WorkflowStep.EVALUATION) == "Evaluation"
    assert step_title(WorkflowStep.SCHEMA_CONVERSION) == "Schema Conversion"
    assert step_title(WorkflowStep.DATA_MIGRATION) == "Data Migration"
    assert step_title(WorkflowStep.VALIDATION) == "Validation"


def test_step_group_and_breadcrumb() -> None:
    from dsql_migrator.ui.workflow import step_breadcrumb, step_group

    # Data Migration is now a single top-level step (no nav groups); its
    # breadcrumb is just the title.
    assert step_group(WorkflowStep.FULL_LOAD) is None
    assert step_breadcrumb(WorkflowStep.FULL_LOAD) == "Data Migration"
    # Top-level steps have no group and the breadcrumb is just the title.
    assert step_group(WorkflowStep.EVALUATION) is None
    assert step_breadcrumb(WorkflowStep.EVALUATION) == "Evaluation"


def test_prerequisite_chain() -> None:
    # Migration plan is the new first step (no prerequisite); Evaluation now
    # depends on it.
    assert prerequisite(WorkflowStep.MIGRATION_PLAN) is None
    assert prerequisite(WorkflowStep.EVALUATION) is WorkflowStep.MIGRATION_PLAN
    assert prerequisite(WorkflowStep.SCHEMA_CONVERSION) is WorkflowStep.EVALUATION
    # The unified Data Migration step depends only on Schema Conversion.
    assert prerequisite(WorkflowStep.FULL_LOAD) is WorkflowStep.SCHEMA_CONVERSION
    # Validation now has a single linear prerequisite: the Data Migration step
    # (WorkflowStep.FULL_LOAD), which is set DONE regardless of migration type.
    assert prerequisite(WorkflowStep.VALIDATION) is WorkflowStep.FULL_LOAD


def test_validation_unlocks_after_data_migration() -> None:
    from dsql_migrator.ui.workflow import is_prerequisite_met

    base = WorkflowState()
    assert is_prerequisite_met(base, WorkflowStep.VALIDATION) is False
    # The unified Data Migration step sets WorkflowStep.FULL_LOAD = DONE for all
    # migration types (Full load only / CDC only / both), which unlocks Validation.
    after_data_migration = with_status(base, WorkflowStep.FULL_LOAD, StepStatus.DONE)
    assert is_prerequisite_met(after_data_migration, WorkflowStep.VALIDATION) is True


def test_data_migration_needs_schema_conversion() -> None:
    from dsql_migrator.ui.workflow import is_prerequisite_met

    base = WorkflowState()
    assert is_prerequisite_met(base, WorkflowStep.FULL_LOAD) is False
    ready = with_status(base, WorkflowStep.SCHEMA_CONVERSION, StepStatus.DONE)
    assert is_prerequisite_met(ready, WorkflowStep.FULL_LOAD) is True


def test_evaluation_needs_migration_plan() -> None:
    from dsql_migrator.ui.workflow import is_prerequisite_met

    base = WorkflowState()
    # Evaluation now depends on the Migration plan step being DONE.
    assert is_prerequisite_met(base, WorkflowStep.EVALUATION) is False
    ready = with_status(base, WorkflowStep.MIGRATION_PLAN, StepStatus.DONE)
    assert is_prerequisite_met(ready, WorkflowStep.EVALUATION) is True


def test_step_definitions_are_ordered_and_consistent() -> None:
    definitions = step_definitions()
    assert [d.step for d in definitions] == list(ordered_steps())
    assert all(step_title(d.step) == d.title for d in definitions)


def test_navigation_previous_and_next() -> None:
    # Migration plan is the first step now; Evaluation follows it.
    assert previous_step(WorkflowStep.MIGRATION_PLAN) is None
    assert next_step(WorkflowStep.MIGRATION_PLAN) is WorkflowStep.EVALUATION
    assert previous_step(WorkflowStep.EVALUATION) is WorkflowStep.MIGRATION_PLAN
    assert next_step(WorkflowStep.EVALUATION) is WorkflowStep.SCHEMA_CONVERSION
    # Data Migration (WorkflowStep.FULL_LOAD) is followed directly by Validation
    # now that CDC is folded into it.
    assert next_step(WorkflowStep.FULL_LOAD) is WorkflowStep.VALIDATION
    assert previous_step(WorkflowStep.VALIDATION) is WorkflowStep.FULL_LOAD
    # Cut over is the final step: it follows Validation and has no next.
    assert next_step(WorkflowStep.VALIDATION) is WorkflowStep.CUT_OVER
    assert previous_step(WorkflowStep.CUT_OVER) is WorkflowStep.VALIDATION
    assert next_step(WorkflowStep.CUT_OVER) is None


def test_get_status_defaults_to_not_started() -> None:
    state = WorkflowState()
    for step in WorkflowStep:
        assert get_status(state, step) is StepStatus.NOT_STARTED


@pytest.mark.parametrize("status", list(StepStatus))
def test_with_status_sets_only_target_step_and_is_immutable(status: StepStatus) -> None:
    original = WorkflowState()
    updated = with_status(original, WorkflowStep.DATA_MIGRATION, status)

    # Target step updated on the returned copy.
    assert get_status(updated, WorkflowStep.DATA_MIGRATION) is status
    # Other steps are unchanged.
    assert get_status(updated, WorkflowStep.EVALUATION) is StepStatus.NOT_STARTED
    assert get_status(updated, WorkflowStep.VALIDATION) is StepStatus.NOT_STARTED
    # Original is not mutated.
    assert get_status(original, WorkflowStep.DATA_MIGRATION) is StepStatus.NOT_STARTED


def test_with_status_supports_full_transition_lifecycle() -> None:
    state = WorkflowState()
    state = with_status(state, WorkflowStep.EVALUATION, StepStatus.IN_PROGRESS)
    assert get_status(state, WorkflowStep.EVALUATION) is StepStatus.IN_PROGRESS
    state = with_status(state, WorkflowStep.EVALUATION, StepStatus.DONE)
    assert get_status(state, WorkflowStep.EVALUATION) is StepStatus.DONE
    # Re-run: a DONE step can move back to IN_PROGRESS and then FAILED.
    state = with_status(state, WorkflowStep.EVALUATION, StepStatus.IN_PROGRESS)
    state = with_status(state, WorkflowStep.EVALUATION, StepStatus.FAILED)
    assert get_status(state, WorkflowStep.EVALUATION) is StepStatus.FAILED


def test_first_step_prerequisite_always_met() -> None:
    state = WorkflowState()
    # Migration plan is the new first step and has no prerequisite.
    assert is_prerequisite_met(state, WorkflowStep.MIGRATION_PLAN) is True
    assert gating_message(state, WorkflowStep.MIGRATION_PLAN) is None


def test_later_step_is_gated_until_prerequisite_done() -> None:
    state = WorkflowState()
    assert is_prerequisite_met(state, WorkflowStep.SCHEMA_CONVERSION) is False

    message = gating_message(state, WorkflowStep.SCHEMA_CONVERSION)
    assert message is not None
    # Guidance names the prerequisite and the step, and signals it is advisory.
    assert "Evaluation" in message
    assert "Schema Conversion" in message
    assert "independently" in message


@pytest.mark.parametrize(
    "incomplete_status",
    [StepStatus.NOT_STARTED, StepStatus.IN_PROGRESS, StepStatus.FAILED],
)
def test_prerequisite_only_met_when_done(incomplete_status: StepStatus) -> None:
    state = with_status(WorkflowState(), WorkflowStep.EVALUATION, incomplete_status)
    assert is_prerequisite_met(state, WorkflowStep.SCHEMA_CONVERSION) is False
    assert gating_message(state, WorkflowStep.SCHEMA_CONVERSION) is not None


def test_gating_clears_once_prerequisite_done() -> None:
    state = with_status(WorkflowState(), WorkflowStep.EVALUATION, StepStatus.DONE)
    assert is_prerequisite_met(state, WorkflowStep.SCHEMA_CONVERSION) is True
    assert gating_message(state, WorkflowStep.SCHEMA_CONVERSION) is None


def test_gating_is_advisory_not_a_hard_block() -> None:
    # A later step can be marked DONE even though its prerequisite never ran;
    # the logic provides guidance but does not prevent the transition.
    state = with_status(WorkflowState(), WorkflowStep.VALIDATION, StepStatus.DONE)
    assert get_status(state, WorkflowStep.VALIDATION) is StepStatus.DONE
    assert gating_message(state, WorkflowStep.VALIDATION) is not None


@pytest.mark.parametrize("status", list(StepStatus))
def test_status_display_mappings_cover_every_status(status: StepStatus) -> None:
    # Labels are English and non-empty; color/icon are defined for every status.
    assert status_label(status) and isinstance(status_label(status), str)
    assert status_color(status) and isinstance(status_color(status), str)
    assert status_icon(status) and isinstance(status_icon(status), str)


def test_status_labels_are_expected_english_strings() -> None:
    assert status_label(StepStatus.NOT_STARTED) == "Not started"
    assert status_label(StepStatus.IN_PROGRESS) == "In progress"
    assert status_label(StepStatus.DONE) == "Success"
    assert status_label(StepStatus.FAILED) == "Failed"


def test_workflow_state_is_isolated_per_session() -> None:
    store = SessionStore()
    state_a = store.get_or_create("session-a")
    state_b = store.get_or_create("session-b")

    # Advance session A's evaluation; session B must be unaffected.
    state_a.set_workflow(
        with_status(state_a.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )

    assert get_status(state_a.workflow, WorkflowStep.EVALUATION) is StepStatus.DONE
    assert get_status(state_b.workflow, WorkflowStep.EVALUATION) is StepStatus.NOT_STARTED


def test_clearing_a_session_resets_its_workflow_state() -> None:
    store = SessionStore()
    state = store.get_or_create("session-a")
    state.set_workflow(
        with_status(state.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )

    store.clear("session-a")

    # A fresh state for the same id starts from NOT_STARTED.
    refreshed = store.get_or_create("session-a")
    assert get_status(refreshed.workflow, WorkflowStep.EVALUATION) is StepStatus.NOT_STARTED


# ---------------------------------------------------------------------------
# Migration overview diagram (Source -> Migration Tool -> Aurora DSQL)
# ---------------------------------------------------------------------------


def test_build_migration_diagram_placeholders_before_connection() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    source, tool, target = build_migration_diagram(SessionConnectionState())

    assert source.title == "Source MySQL"
    assert source.subtitle == "RDS / Aurora MySQL"
    assert source.connected is False
    assert tool.title == "Migration Tool" and tool.connected is True
    assert target.title == "Aurora DSQL"
    assert target.subtitle == "PostgreSQL-compatible"
    assert target.connected is False


def test_build_migration_diagram_reflects_configured_connections() -> None:
    from dsql_migrator.core.models import (
        SourceConnectionConfig,
        TargetConnectionConfig,
    )
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    state.source_config = SourceConnectionConfig(host="db.example.com", database="app")
    state.target_config = TargetConnectionConfig(
        cluster_endpoint="abc.dsql.us-east-1.on.aws", region="us-east-1"
    )
    state.source_verified = True
    state.target_verified = True

    source, _tool, target = build_migration_diagram(state)

    # Source shows the cluster/instance name as the primary line; the endpoint
    # (host) appears as a small detail and the region above the icon.
    assert source.subtitle == "db"
    assert ("dns", "Endpoint: db.example.com") in source.details
    assert source.connected is True  # source verified
    # Target shows the cluster name, the full endpoint as a detail, region apart.
    assert target.subtitle == "abc"
    assert ("dns", "Endpoint: abc.dsql.us-east-1.on.aws") in target.details
    assert target.region == "us-east-1"
    assert target.connected is True


def test_build_migration_diagram_hides_details_until_verified() -> None:
    from dsql_migrator.core.models import (
        SourceConnectionConfig,
        TargetConnectionConfig,
    )
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    # Configured but NOT verified -> generic placeholders, no details/region.
    state = SessionConnectionState()
    state.source_config = SourceConnectionConfig(host="db.example.com", database="app")
    state.target_config = TargetConnectionConfig(
        cluster_endpoint="abc.dsql.us-east-1.on.aws", region="us-east-1"
    )

    source, _tool, target = build_migration_diagram(state)

    assert source.subtitle == "RDS / Aurora MySQL"
    assert source.details == ()
    assert source.region is None
    assert source.connected is False
    assert target.subtitle == "PostgreSQL-compatible"
    assert target.details == ()
    assert target.region is None
    assert target.connected is False


# ---------------------------------------------------------------------------
# Reconnect / resume hint (Property 4): re-verify connections after a restart
# ---------------------------------------------------------------------------


def test_reconnect_notice_none_for_fresh_session() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import reconnect_notice

    assert reconnect_notice(SessionConnectionState()) is None


def test_reconnect_notice_shown_when_progressed_but_unverified() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import reconnect_notice

    state = SessionConnectionState()
    # Restored progress (workflow advanced) but connections not verified -- the
    # typical post-restart state (credentials are never persisted).
    state.set_workflow(
        with_status(state.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    notice = reconnect_notice(state)
    assert notice is not None
    assert "Re-verify" in notice


def test_reconnect_notice_none_when_connections_verified() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import reconnect_notice

    state = SessionConnectionState()
    state.set_workflow(
        with_status(state.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    state.source_verified = True
    state.target_verified = True
    assert reconnect_notice(state) is None


def test_reconnect_notice_shown_for_cdc_infra_with_no_step_started() -> None:
    # The real bug: CDC infrastructure was deployed from the Migration plan step
    # before any workflow step ran, so every step is NOT_STARTED. The banner must
    # STILL show on reconnect (there is a deployed cdc-stack + plan to resume).
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import reconnect_notice

    state = SessionConnectionState()
    # No workflow step started, connections not verified (post-reconnect), but a
    # CDC plan was chosen and infra inputs were entered.
    state.set_migration_type("full_load_and_cdc")
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0abc"})
    notice = reconnect_notice(state)
    assert notice is not None
    assert "Re-verify" in notice


def test_reconnect_notice_shown_when_parked_past_connect() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import reconnect_notice

    state = SessionConnectionState()
    state.set_active_view("migration_plan")  # was looking at a step, not Connect
    assert reconnect_notice(state) is not None


def test_reconnect_notice_none_for_default_plan_on_connect() -> None:
    # The full-load-only default with no other progress and parked on Connect is
    # NOT resumable progress -> no banner (avoids a false "reconnected" on a fresh
    # session that merely defaulted its plan).
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import reconnect_notice

    state = SessionConnectionState()
    state.set_active_view("connect")
    assert reconnect_notice(state) is None


# --- Start over (session reset) -----------------------------------------------


def test_start_over_cdc_warning_when_cdc_plan_with_infra() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _start_over_cdc_warning

    state = SessionConnectionState()
    state.set_migration_type("full_load_and_cdc")
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0abc"})
    warning = _start_over_cdc_warning(state)
    assert warning is not None
    assert "Delete CDC infrastructure" in warning


def test_start_over_cdc_warning_none_without_infra() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _start_over_cdc_warning

    # CDC plan chosen but no infra inputs entered yet -> nothing deployed to orphan.
    state = SessionConnectionState()
    state.set_migration_type("cdc_only")
    assert _start_over_cdc_warning(state) is None

    # Full-load-only with infra inputs somehow set -> still no CDC infra concern.
    state2 = SessionConnectionState()
    state2.set_cdc_infra_inputs({"vpc_id": "vpc-x"})
    assert _start_over_cdc_warning(state2) is None


def test_start_over_cdc_warning_names_custom_stack() -> None:
    # A custom (non-default) cdc-stack name is NOT re-discovered by a fresh session
    # (which reverts to the default name), so the warning must name it explicitly so
    # the operator knows exactly what to delete.
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _start_over_cdc_warning

    state = SessionConnectionState()
    state.set_migration_type("full_load_and_cdc")
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0abc"})

    custom = _start_over_cdc_warning(state, "mysql-dsql-cdc-orders")
    assert custom is not None
    assert "mysql-dsql-cdc-orders" in custom  # names the exact stack
    assert "AWS console" in custom  # tells them where to delete if already reset

    # The DEFAULT name (re-discoverable by a fresh session) keeps the generic text
    # and does NOT clutter the message with the stack name.
    default = _start_over_cdc_warning(state, CDC_DEFAULT_STACK_NAME)
    assert default is not None
    assert CDC_DEFAULT_STACK_NAME not in default
    # No stack name passed (back-compat) behaves like the default-name case.
    assert _start_over_cdc_warning(state) == default


def test_build_migration_diagram_shows_source_server_version() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    source, _tool, _target = build_migration_diagram(state)
    assert source.details == ()  # nothing captured yet

    # Details only populate once the source connection test has passed.
    state.source_config = SourceConnectionConfig(
        host="db.cluster-x.us-east-1.rds.amazonaws.com", database="app"
    )
    state.source_verified = True
    state.set_source_version("8.0.mysql_aurora.3.10.4", "8.0.42")
    source, _tool, _target = build_migration_diagram(state)
    texts = [text for _icon, text in source.details]
    assert "Aurora MySQL 3.10.4 (MySQL 8.0.42)" in texts
    assert source.title == "Aurora MySQL"  # classified from the version marker
    assert source.region == "us-east-1"  # parsed from the RDS host, shown above icon

    # Instance class is appended (labeled) once the RDS lookup succeeds.
    state.set_source_instance_class("db.r6g.large")
    source, _tool, _target = build_migration_diagram(state)
    texts = [text for _icon, text in source.details]
    assert "Instance type: db.r6g.large" in texts


def test_build_migration_diagram_tool_shows_current_stage() -> None:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import (
        WorkflowStep,
        build_migration_diagram,
        step_title,
    )

    state = SessionConnectionState()
    _source, tool, _target = build_migration_diagram(
        state, WorkflowStep.SCHEMA_CONVERSION
    )
    stage = step_title(WorkflowStep.SCHEMA_CONVERSION)
    # The stage and AI-assist status are shown as bordered chips.
    assert (f"Current stage: {stage}", "active") in tool.badges
    assert ("AI assist: Off", "neutral") in tool.badges  # AI off by default
    assert all(len(b) == 2 for b in tool.badges)


def test_build_migration_diagram_connectivity_and_ai_badges() -> None:
    from dsql_migrator.core.models import AiAssistConfig
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import WorkflowStep, build_migration_diagram

    state = SessionConnectionState()
    # Before any test: not connected.
    source, _tool, target = build_migration_diagram(state)
    assert ("Not connected", "neutral") in source.badges
    assert ("Not connected", "neutral") in target.badges

    # After verifying + enabling AI assist.
    state.source_verified = True
    state.target_verified = True
    state.ai_assist = AiAssistConfig(enabled=True)
    source, tool, target = build_migration_diagram(state, WorkflowStep.EVALUATION)
    assert ("Connected", "ok") in source.badges
    assert ("Connected", "ok") in target.badges
    assert ("AI assist: On", "ok") in tool.badges


def test_migration_type_meta_reflects_chosen_type() -> None:
    # The shared journey header reads the session's chosen migration type so the
    # choice stays visible on every step.
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _migration_type_meta

    state = SessionConnectionState()
    state.set_migration_type("full_load_and_cdc")
    label, icon, blurb = _migration_type_meta(state)
    assert label == "Full load + CDC"
    assert icon == "merge"
    assert blurb  # a descriptive blurb is present

    state.set_migration_type("cdc_only")
    label, _icon, _blurb = _migration_type_meta(state)
    assert label == "CDC only"


def test_migration_type_meta_falls_back_without_type() -> None:
    # A state with no resolvable migration type must not break the header.
    from dsql_migrator.ui.workflow import _migration_type_meta

    class _Bare:
        migration_type = None

    label, icon, _blurb = _migration_type_meta(_Bare())
    assert label == "Migration" and icon == "tune"


class _HeaderUi:
    """Minimal NiceGUI double capturing emitted label text for the journey header."""

    def __init__(self) -> None:
        self.texts: list[str] = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def on(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def label(self, text="", *_a, **_k):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self)

    def icon(self, *_a, **_k):
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)


def _render_header_texts(step) -> list[str]:
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _render_journey_header

    state = SessionConnectionState()
    state.set_migration_type("full_load_and_cdc")
    ui = _HeaderUi()
    _render_journey_header(ui, state, step, lambda _s: None)
    return ui.texts


def test_journey_header_hides_type_banner_on_migration_plan() -> None:
    # On the Migration Plan step the "Include CDC?" control is the source of truth,
    # so the "Migration type:" banner is omitted to avoid a redundant/conflicting
    # three-value label above the two-value decision.
    texts = _render_header_texts(WorkflowStep.MIGRATION_PLAN)
    assert "Migration type:" not in texts
    assert "Full load + CDC" not in texts
    # Band 1 (the stepper) still renders — the plan step label is present.
    assert any("Migration plan" in t for t in texts)


def test_journey_header_shows_type_banner_on_later_steps() -> None:
    # Every step after the plan keeps the banner for continuity.
    texts = _render_header_texts(WorkflowStep.EVALUATION)
    assert "Migration type:" in texts
    assert "Full load + CDC" in texts


def test_build_migration_diagram_reconnect_state_on_resume() -> None:
    # A restored-but-unverified session (resumable progress, no live connection
    # re-test this process) shows "Reconnect to resume" -- NOT a flat grey "Not
    # connected" that would read as "never set up".
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    # Mirror a restore: workflow advanced + non-secret target config restored, but
    # neither connection re-verified (source config is never persisted at all).
    state.set_workflow(
        with_status(state.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    state.target_config = TargetConnectionConfig(
        cluster_endpoint="abc.dsql.us-east-1.on.aws", region="us-east-1"
    )

    source, _tool, target = build_migration_diagram(state)

    # Both endpoint nodes signal a resumable reconnect (amber), not "Not connected".
    assert ("Reconnect to resume", "reconnect") in source.badges
    assert ("Reconnect to resume", "reconnect") in target.badges
    assert source.reconnect is True and source.connected is False
    assert target.reconnect is True and target.connected is False
    # The restored (non-secret) target endpoint/region are previewed even though
    # unverified -- the user sees what they are resuming.
    assert ("dns", "Endpoint: abc.dsql.us-east-1.on.aws") in target.details
    assert target.region == "us-east-1"
    # Source carries credentials and is never persisted, so it has no remembered
    # details to preview -- only the reconnect cue.
    assert source.details == ()


def test_build_migration_diagram_not_connected_for_fresh_session() -> None:
    # No restorable progress -> a brand-new session still reads "Not connected"
    # (grey), never the amber reconnect cue.
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    source, _tool, target = build_migration_diagram(SessionConnectionState())
    assert ("Not connected", "neutral") in source.badges
    assert ("Not connected", "neutral") in target.badges
    assert source.reconnect is False and target.reconnect is False


def test_build_migration_diagram_prefers_dsql_name_tag() -> None:
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    state.target_config = TargetConnectionConfig(
        cluster_endpoint="abc.dsql.us-east-1.on.aws", region="us-east-1"
    )
    state.target_verified = True
    state.set_target_cluster_name("prod-orders")  # the cluster's Name tag

    _source, _tool, target = build_migration_diagram(state)
    # The Name tag is the primary line; the cluster id is shown as a detail.
    assert target.subtitle == "prod-orders"
    assert ("badge", "Cluster id: abc") in target.details
    assert ("dns", "Endpoint: abc.dsql.us-east-1.on.aws") in target.details


def test_build_migration_diagram_uses_aurora_version_var() -> None:
    # Newer Aurora MySQL reports only the community patch in VERSION(); the Aurora
    # engine version comes from @@aurora_version and a .cluster- endpoint.
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    state.source_config = SourceConnectionConfig(
        host="mycluster.cluster-abc123.ap-southeast-2.rds.amazonaws.com", database="app"
    )
    state.source_verified = True
    state.set_source_version("8.0.42", "8.0.42", "3.07.1")
    source, _tool, _target = build_migration_diagram(state)
    assert source.title == "Aurora MySQL"
    texts = [text for _icon, text in source.details]
    assert "Aurora MySQL 3.07.1 (MySQL 8.0.42)" in texts
    assert source.subtitle == "mycluster"
    assert source.region == "ap-southeast-2"


def test_format_source_engine_variants() -> None:
    from dsql_migrator.ui.workflow import format_source_engine

    # Aurora with the full community patch known.
    assert (
        format_source_engine("8.0.mysql_aurora.3.10.4", "8.0.42")
        == "Aurora MySQL 3.10.4 (MySQL 8.0.42)"
    )
    # Aurora without a clean community version -> fall back to major.minor base.
    assert (
        format_source_engine("8.0.mysql_aurora.3.10.4", None)
        == "Aurora MySQL 3.10.4 (MySQL 8.0)"
    )
    # @@aurora_version is preferred even when VERSION() lacks the Aurora tag.
    assert (
        format_source_engine("8.0.42", "8.0.42", "3.07.1")
        == "Aurora MySQL 3.07.1 (MySQL 8.0.42)"
    )
    # Plain MySQL: VERSION() already carries the full version.
    assert format_source_engine("8.0.35", None) == "MySQL 8.0.35"
    assert format_source_engine(None, None) is None


def test_step_run_label_is_step_specific_on_first_run() -> None:
    # A bare "Run" is ambiguous; each step names its action on the first run.
    from dsql_migrator.core.models import StepStatus

    assert step_run_label(WorkflowStep.VALIDATION, StepStatus.NOT_STARTED) == (
        "Start validation"
    )
    assert step_run_label(WorkflowStep.FULL_LOAD, StepStatus.NOT_STARTED) == (
        "Start migration"
    )
    assert step_run_label(WorkflowStep.EVALUATION, StepStatus.NOT_STARTED) == (
        "Run evaluation"
    )


def test_step_run_label_is_step_specific_rerun_once_not_pending() -> None:
    from dsql_migrator.core.models import StepStatus

    # Re-run is also step-specific (parallel to the start verb), not a bare "Re-run".
    for status in (StepStatus.DONE, StepStatus.IN_PROGRESS, StepStatus.FAILED):
        assert step_run_label(WorkflowStep.VALIDATION, status) == "Re-run validation"
        assert step_run_label(WorkflowStep.FULL_LOAD, status) == "Re-run migration"


class _DialogUi:
    """NiceGUI double for _open_start_over_dialog: captures label text and records
    whether any button was wired with an on('click', ...) handler (the reset
    button). Enough to tell the blocking path (no reset wiring) from the normal one."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.click_wired = False  # True once a button gets on('click', ...)
        self.dialog_opened = False

    class _El:
        def __init__(self, ui, is_button=False):
            self._ui = ui
            self._is_button = is_button

        def classes(self, *_a, **_k):
            return self

        def style(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

        def on(self, event=None, *_a, **_k):
            if self._is_button and event == "click":
                self._ui.click_wired = True
            return self

        def open(self, *_a, **_k):  # dialog.open() at the end of the builder
            self._ui.dialog_opened = True
            return self

        def close(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def label(self, text="", *_a, **_k):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self)

    def button(self, text="", *_a, on_click=None, **_k):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self, is_button=True)

    def input(self, *_a, **_k):
        return self._El(self)

    def icon(self, *_a, **_k):
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def card(self, *_a, **_k):
        return self._El(self)

    def dialog(self, *_a, **_k):
        return self._El(self)

    def open(self):  # dialog.open() is called on the returned _El in real code;
        self.dialog_opened = True  # our _El has no open(), so this is unused — kept


def _render_start_over_dialog(**kwargs):
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _open_start_over_dialog

    ui = _DialogUi()
    state = SessionConnectionState()
    _open_start_over_dialog(
        ui, state, lambda: None, lambda _v: None, lambda: None, object(), **kwargs
    )
    return ui


def test_start_over_blocked_while_cdc_teardown_in_flight() -> None:
    # The reported bug: resetting while a CDC delete is mid-flight. The dialog must
    # explain and NOT offer a working reset (no reset button wired to click).
    ui = _render_start_over_dialog(cdc_teardown_in_flight=True, cdc_deployed=True)
    assert any("CDC teardown is already running" in t for t in ui.texts)
    assert not ui.click_wired  # reset is NOT executable on the block path
    assert any("Close" in t for t in ui.texts)  # only Close is offered


def test_start_over_normal_path_wires_reset_when_not_in_flight() -> None:
    # Regression: with no teardown in flight, the normal type-to-confirm reset flow
    # renders (reset button wired for click) and the block notice is absent.
    ui = _render_start_over_dialog(cdc_teardown_in_flight=False, cdc_deployed=False)
    assert not any("CDC teardown is already running" in t for t in ui.texts)
    assert ui.click_wired  # the reset button is wired on the normal path
    assert any("Type RESET to confirm:" in t for t in ui.texts)
