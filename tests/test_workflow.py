# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the five-step workflow shell logic (NiceGUI-agnostic).

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
    # Five steps. Evaluation is the first post-Connect step: the retired Migration
    # plan step asked "Include CDC?" before Evaluation had produced any evidence to
    # decide on, and duplicated the migration-type selector Data Migration owns.
    # Data Migration is a single nav step (backed by WorkflowStep.FULL_LOAD); the
    # Full load / CDC / both choice is an inner type selector, so CDC is not a
    # separate nav step. Cut over is the final step (operational guidance).
    assert ordered_steps() == (
        WorkflowStep.EVALUATION,
        WorkflowStep.SCHEMA_CONVERSION,
        WorkflowStep.FULL_LOAD,
        WorkflowStep.VALIDATION,
        WorkflowStep.CUT_OVER,
    )
    # The retired step must NOT be a nav entry...
    assert WorkflowStep.MIGRATION_PLAN not in ordered_steps()


def test_retired_migration_plan_step_still_resolves() -> None:
    # ...but it must stay resolvable: real persisted snapshots name it (a stored
    # active_view, or a status field), so step_title/prerequisite must not KeyError.
    assert step_title(WorkflowStep.MIGRATION_PLAN) == "Migration plan"
    assert prerequisite(WorkflowStep.MIGRATION_PLAN) is None
    # And WorkflowState must still accept the field, or every old snapshot would
    # fail to validate (the model is extra="forbid").
    assert "migration_plan" in WorkflowState.model_fields
    state = WorkflowState.model_validate({"migration_plan": "DONE"})
    assert state.migration_plan is StepStatus.DONE


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
    # Evaluation is the first step, so it has no prerequisite (Connect is gated
    # separately by the sidebar's workflow-unlock latch, not by this chain).
    assert prerequisite(WorkflowStep.EVALUATION) is None
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


def test_evaluation_is_reachable_immediately() -> None:
    from dsql_migrator.ui.workflow import is_prerequisite_met

    base = WorkflowState()
    # Evaluation is the first step: nothing in the workflow gates it (Connect is
    # enforced by the sidebar's unlock latch). It must be open on a fresh session --
    # previously it waited on the Migration plan step being marked DONE.
    assert is_prerequisite_met(base, WorkflowStep.EVALUATION) is True
    assert gating_message(base, WorkflowStep.EVALUATION) is None


def test_step_definitions_are_ordered_and_consistent() -> None:
    definitions = step_definitions()
    assert [d.step for d in definitions] == list(ordered_steps())
    assert all(step_title(d.step) == d.title for d in definitions)


def test_navigation_previous_and_next() -> None:
    # Evaluation is the first step, so it has no previous.
    assert previous_step(WorkflowStep.EVALUATION) is None
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
    # Evaluation is the first step and has no prerequisite.
    assert is_prerequisite_met(state, WorkflowStep.EVALUATION) is True
    assert gating_message(state, WorkflowStep.EVALUATION) is None


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
    # The real bug: CDC infrastructure was deployed before any workflow step
    # completed (it is offered on Data Migration's Prerequisites sub-step so the MSK
    # create overlaps the Full Load), so every step is NOT_STARTED. The banner must
    # STILL show on reconnect (there is a deployed cdc-stack to resume).
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

    # CDC type chosen but no infra inputs entered yet -> nothing deployed to orphan.
    state = SessionConnectionState()
    state.set_migration_type("cdc_only")
    assert _start_over_cdc_warning(state) is None

    # A totally untouched session (no type, no inputs) is likewise silent.
    assert _start_over_cdc_warning(SessionConnectionState()) is None


def test_start_over_cdc_warning_fires_on_infra_inputs_alone() -> None:
    # Entered infra inputs are the real "something may be deployed" signal, so they
    # alone must warn. Requiring the migration type to STILL name a CDC mode was a
    # hole: the type is freely switchable, so a user who deployed MSK and then flipped
    # back to Full-load-only got no warning and could silently orphan a billing
    # cluster. (With the Migration plan step gone, the type is even easier to change.)
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _start_over_cdc_warning

    state = SessionConnectionState()
    state.set_cdc_infra_inputs({"vpc_id": "vpc-x"})
    assert state.migration_type.value == "full_load_only"
    warning = _start_over_cdc_warning(state)
    assert warning is not None
    assert "MSK/NAT keep billing" in warning

    # A confirmed-absent live probe still short-circuits it (nothing to orphan).
    assert _start_over_cdc_warning(state, cdc_confirmed_absent=True) is None


def test_start_over_cdc_warning_fires_on_custom_stack_without_inputs() -> None:
    # A non-default stack name is the case MOST at risk: a fresh session only
    # re-discovers the DEFAULT name, so this one would be orphaned with no in-tool
    # pointer -- even if the session has no infra inputs left (e.g. after a restore).
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _start_over_cdc_warning

    warning = _start_over_cdc_warning(
        SessionConnectionState(), "mysql-dsql-cdc-orders"
    )
    assert warning is not None
    assert "mysql-dsql-cdc-orders" in warning


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


def test_start_over_cdc_warning_suppressed_when_confirmed_absent() -> None:
    # After a live probe confirms NO CDC is deployed (the user just deleted the
    # stack), the "MSK/NAT keep billing" caution must NOT show even though the
    # session still carries a CDC plan + infra inputs (those are wiped by the reset).
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import _start_over_cdc_warning

    state = SessionConnectionState()
    state.set_migration_type("full_load_and_cdc")
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0abc"})

    # Without the flag (probe unknown / not run) the caution still shows (hedge).
    assert _start_over_cdc_warning(state) is not None
    # With a confirmed-absent probe, it is suppressed — nothing to orphan.
    assert (
        _start_over_cdc_warning(state, cdc_confirmed_absent=True) is None
    )
    # Also suppressed for a custom stack name once confirmed absent.
    assert (
        _start_over_cdc_warning(
            state, "mysql-dsql-cdc-orders", cdc_confirmed_absent=True
        )
        is None
    )


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


def test_header_has_a_what_next_readiness_briefing() -> None:
    # The header carries a proactive "What's next?" briefing so the panel isn't only
    # reactive: it opens a dedicated readiness scope on the general (tool-backed)
    # streamer, seeded to read the real session state and name the blocking objects.
    import inspect

    from dsql_migrator.ui import workflow as wf

    src = inspect.getsource(wf.build_workflow_sidebar)
    assert '"What\'s next?"' in src
    assert 'scope_id="readiness"' in src
    assert "ai_general_streamer_factory()" in src  # same tool-backed streamer as chat
    assert "top risks" in src


def test_start_over_and_version_live_in_the_drawer_footer_not_the_header() -> None:
    # "Start over" is destructive (it can also tear down CDC infra), so it must not sit
    # one click from "AI DBA" in the header where it invited mis-clicks. It — and the
    # version label — moved to the FOOT of the left drawer (below Settings). Guard the
    # placement so a refactor doesn't drift it back next to the AI controls.
    import inspect

    from dsql_migrator.ui import workflow as wf

    src = inspect.getsource(wf.build_workflow_sidebar)
    header_block = src[src.index("with ui.header()") : src.index("# --- Left drawer")]
    drawer_block = src[src.index("# --- Left drawer") :]

    # The header keeps only the AI controls; the Start over row (identified by its
    # unique "Reset this session" caption) and the version label are gone from it.
    caption = '"Reset this session"'
    assert '"AI DBA"' in header_block
    assert "Start over" not in header_block
    assert caption not in header_block
    assert 'f"version {version}"' not in header_block

    # Both now render in the drawer, and the Start over row comes AFTER the Settings
    # footer_extra() slot (so it reads as the last, least-prominent control).
    assert "Start over" in drawer_block
    assert caption in drawer_block
    assert 'f"version {version}"' in drawer_block
    assert drawer_block.index("footer_extra()") < drawer_block.index(caption)


def test_journey_header_shows_the_type_banner_only_on_data_migration() -> None:
    # The banner rides only on the Data Migration step (WorkflowStep.CDC), next to the
    # selector that owns the choice. It used to render on every step, but a single
    # session.migration_type cannot describe a session that ran Full Load and THEN
    # switched to "CDC only": Validation/Cut over then showed a stale, sometimes-false
    # summary they could not correct. On the Data Migration step the banner and the
    # selector are always consistent.
    from dsql_migrator.ui.workflow import WorkflowStep

    on_step = _render_header_texts(WorkflowStep.CDC)
    assert "Migration type:" in on_step
    assert "Full load + CDC" in on_step

    for step in ordered_steps():
        if step is WorkflowStep.CDC:
            continue
        texts = _render_header_texts(step)
        assert "Migration type:" not in texts, step
        assert "Full load + CDC" not in texts, step
        # Band 1 (the stepper) still renders, so the header is not blank.
        assert any("Evaluation" in t for t in texts), step


def test_journey_header_hides_the_type_banner_until_a_real_choice() -> None:
    """No banner before the user actually picks a type.

    ``migration_type`` always answers -- it defaults to full-load-only -- so
    rendering it unconditionally presented that default as a settled decision:
    Evaluation opened with "Migration type: Full load only" plus its full blurb,
    describing a migration nobody had chosen. Under the retired Migration plan step
    this could not happen (the choice came first), which is why removing that step
    exposed it.
    """
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import WorkflowStep, _render_journey_header

    fresh = SessionConnectionState()
    assert fresh.migration_type_chosen() is False
    assert fresh.migration_type.value == "full_load_only"  # the default still answers

    # On the Data Migration step (where the banner now lives), an unchosen type still
    # shows no banner -- the default must not read as a settled decision.
    ui = _HeaderUi()
    _render_journey_header(ui, fresh, WorkflowStep.CDC, lambda _s: None)
    assert "Migration type:" not in ui.texts
    assert "Full load only" not in ui.texts
    assert any("Evaluation" in t for t in ui.texts)  # band 1 still renders

    # Choosing the type -- even choosing the value that was already the default --
    # makes the banner appear (on the Data Migration step).
    chose_default = SessionConnectionState()
    chose_default.set_migration_type("full_load_only")
    ui = _HeaderUi()
    _render_journey_header(ui, chose_default, WorkflowStep.CDC, lambda _s: None)
    assert "Migration type:" in ui.texts
    assert "Full load only" in ui.texts


def test_journey_header_banner_gate_is_fail_closed() -> None:
    # A state object without the flag (an older snapshot, a bare test double) must
    # omit the banner rather than assert a choice that was never made.
    from dsql_migrator.ui.workflow import _migration_type_chosen

    class _NoFlag:
        pass

    class _Raises:
        def migration_type_chosen(self):
            raise RuntimeError("boom")

    assert _migration_type_chosen(_NoFlag()) is False
    assert _migration_type_chosen(_Raises()) is False
    assert _migration_type_chosen(None) is False


def test_retired_view_redirect_is_wired_for_the_migration_plan_step() -> None:
    """A session parked on the retired step must land on Evaluation, not Connect.

    ``_restore_view`` silently falls back to Connect for any stored view with no
    step_content entry, so without an explicit redirect the user loses their place.
    This is not hypothetical: 8 of the 19 snapshots in the repo's real
    session_state.sqlite are parked on ``"migration_plan"``. The redirect lives in
    ``build_page``'s restore block (a closure), so the contract is pinned
    structurally, next to the identical back-compat redirect for the old "cdc" view.
    """
    import ast
    import pathlib

    import dsql_migrator.ui.app as app_mod

    src = pathlib.Path(app_mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    # Find `if _session.active_view == WorkflowStep.MIGRATION_PLAN.value:` and assert
    # its body sets the view to EVALUATION.
    redirects = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and "MIGRATION_PLAN" in ast.dump(node.test)
    ]
    assert redirects, "no active_view redirect for the retired MIGRATION_PLAN step"
    body = ast.dump(ast.Module(body=redirects[0].body, type_ignores=[]))
    assert "set_active_view" in body
    assert "EVALUATION" in body


def test_journey_header_stepper_is_five_numbered_steps() -> None:
    texts = _render_header_texts(WorkflowStep.EVALUATION)
    assert "1. Evaluation" in texts
    assert "2. Schema Conversion" in texts
    assert "3. Data Migration" in texts
    assert "4. Validation" in texts
    assert "5. Cut over" in texts
    # The retired step must not appear anywhere in the stepper.
    assert not any("Migration plan" in t for t in texts)


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

    def refreshable(self, fn):
        # The CDC teardown tiles render inside an @ui.refreshable. NiceGUI's decorator
        # returns a wrapper that renders when CALLED and carries .refresh(); mirror that
        # so the tile text is actually emitted (without this the whole tiles branch was
        # unreachable from tests -- an AttributeError, not a silent skip).
        def _wrapper(*a, **k):
            return fn(*a, **k)

        _wrapper.refresh = lambda *a, **k: None  # type: ignore[attr-defined]
        return _wrapper

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


def test_start_over_warns_but_allows_reset_when_cdc_deploy_in_flight() -> None:
    # Concern #2 (deploy/start extension): a running Deploy/Start is re-discoverable
    # and must not trap the user, so Start over WARNS rather than hard-blocks --
    # the reset stays executable (reset button wired), unlike the teardown block.
    ui = _render_start_over_dialog(cdc_op_in_flight="infra", cdc_deployed=False)
    assert any("CDC infrastructure deploy is still running" in t for t in ui.texts)
    assert ui.click_wired  # warn, not block: reset is still executable
    assert any("Type RESET to confirm:" in t for t in ui.texts)


def test_start_over_warns_when_cdc_start_in_flight() -> None:
    ui = _render_start_over_dialog(cdc_op_in_flight="start", cdc_deployed=False)
    assert any("Start CDC is still running" in t for t in ui.texts)
    assert ui.click_wired


def test_start_over_teardown_block_takes_precedence_over_op_warning() -> None:
    # A teardown in flight hard-blocks even if a deploy/start is also flagged: the
    # early-return block path wins and no working reset is offered.
    ui = _render_start_over_dialog(
        cdc_teardown_in_flight=True, cdc_op_in_flight="infra", cdc_deployed=True
    )
    assert any("CDC teardown is already running" in t for t in ui.texts)
    assert not ui.click_wired
    assert not any("deploy is still running" in t for t in ui.texts)


# ---------------------------------------------------------------------------
# Persistent CDC-teardown banner (cross-view "teardown in progress")
# ---------------------------------------------------------------------------


def test_cdc_teardown_banner_copy_running_failed_and_none() -> None:
    from dsql_migrator.ui.workflow import _cdc_teardown_banner_copy

    assert _cdc_teardown_banner_copy(None) is None
    assert _cdc_teardown_banner_copy({}) is None
    # running delete → info tone.
    tone, header, body = _cdc_teardown_banner_copy(
        {"state": "running", "kind": "delete", "stack": "cdc-x"}
    )
    assert tone == "info"
    assert "teardown in progress" in header.lower()
    assert "cdc-x" in body and "billing" in body.lower()
    # running stop → info tone, connector wording.
    tone, header, body = _cdc_teardown_banner_copy(
        {"state": "running", "kind": "stop", "stack": "cdc-y"}
    )
    assert tone == "info" and "connector" in header.lower() and "cdc-y" in body
    # failed → error tone + actionable wording.
    tone, header, body = _cdc_teardown_banner_copy(
        {"state": "failed", "kind": "delete", "stack": "cdc-z"}
    )
    assert tone == "error"
    assert "failed" in header.lower()
    assert "cdc-z" in body and "DELETE_FAILED" in body
    # No state defaults to running; missing stack → generic label (no dangling quote).
    _, _, fb = _cdc_teardown_banner_copy({"kind": "delete"})
    assert "the cdc-stack" in fb


def test_cdc_banner_copy_covers_an_in_flight_infra_deploy() -> None:
    # The ~15-20 min infrastructure create is the one CDC operation the user is meant
    # to walk away from (it should overlap the Full Load), so it needs the cross-view
    # banner too -- otherwise, once off the Data Migration screen, there is no sign it
    # is still running and the user waits on it instead of starting the snapshot.
    from dsql_migrator.ui.workflow import _cdc_teardown_banner_copy

    tone, header, body = _cdc_teardown_banner_copy(
        {"state": "running", "kind": "infra", "stack": "cdc-infra-1"}
    )
    assert tone == "info"
    assert "deploying" in header.lower()
    assert "cdc-infra-1" in body
    # It must say the Full Load is NOT blocked -- that is the whole point of
    # surfacing it (the deploy overlaps the load).
    assert "does not block" in body.lower()
    assert "streaming" in body.lower()
    # Missing stack still reads cleanly.
    _, _, generic = _cdc_teardown_banner_copy({"state": "running", "kind": "infra"})
    assert "the cdc-stack" in generic


def test_cdc_banner_copy_infra_failure_keeps_the_actionable_error() -> None:
    # A FAILED infra job must NOT be softened into the reassuring "deploying in the
    # background" copy: the failure branch (with Retry/Dismiss) has to win.
    from dsql_migrator.ui.workflow import _cdc_teardown_banner_copy

    tone, header, _body = _cdc_teardown_banner_copy(
        {"state": "failed", "kind": "infra", "stack": "cdc-infra-1"}
    )
    assert tone == "error"
    assert "failed" in header.lower()


class _BannerUi:
    """NiceGUI double for _render_cdc_teardown_banner: records notice text and
    whether a poll timer was armed. Supports the @ui.refreshable decorator (returns
    the fn with a no-op .refresh) and render_notice's row/icon/column/label."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.timer_armed = False
        self.buttons: list = []  # (label, on_click) for the failed-state actions
        self.spinners = 0        # animated "still running" glyphs
        self.badges: list[str] = []
        self.icons: list[str] = []

    class _El:
        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def style(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def refreshable(self, fn):
        fn.refresh = lambda *_a, **_k: None
        return fn

    def timer(self, *_a, **_k):
        self.timer_armed = True
        return self._El()

    def label(self, text="", *_a, **_k):
        if text is not None:
            self.texts.append(str(text))
        return self._El()

    def icon(self, name="", *_a, **_k):
        if name:
            self.icons.append(str(name))
        return self._El()

    def spinner(self, *_a, **_k):
        self.spinners += 1
        return self._El()

    def badge(self, text="", *_a, **_k):
        self.badges.append(str(text))
        return self._El()

    def space(self, *_a, **_k):
        return self._El()

    def row(self, *_a, **_k):
        return self._El()

    def column(self, *_a, **_k):
        return self._El()

    def card(self, *_a, **_k):
        return self._El()

    def button(self, text="", *_a, on_click=None, **_k):
        self.buttons.append((str(text), on_click))
        return self._El()


def test_render_cdc_teardown_banner_shows_while_in_flight_and_arms_poll() -> None:
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    ui = _BannerUi()
    _render_cdc_teardown_banner(ui, lambda: {"kind": "delete", "stack": "cdc-z"})
    assert any("teardown in progress" in t.lower() for t in ui.texts)
    assert any("cdc-z" in t for t in ui.texts)
    assert ui.timer_armed  # re-arms a one-shot poll so it clears itself on settle


def test_render_cdc_teardown_banner_shows_live_progress_affordances() -> None:
    # A teardown runs ~15-45 min. With only a static icon the banner read as an inert
    # message and the user could not tell the work was still moving, so a RUNNING
    # banner gets an animated spinner (in place of the glyph) + an "In progress" badge.
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    for kind in ("delete", "stop", "infra"):
        ui = _BannerUi()
        _render_cdc_teardown_banner(ui, lambda k=kind: {"kind": k, "stack": "cdc-z"})
        assert ui.spinners == 1, kind
        assert "In progress" in ui.badges, kind
        assert ui.icons == [], kind  # the spinner REPLACES the static glyph


def test_render_cdc_teardown_banner_failed_state_is_static_not_busy() -> None:
    # A FAILED teardown is terminal until the user acts: showing a spinner would
    # imply work is still happening. It keeps the static error glyph and its actions.
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    ui = _BannerUi()
    _render_cdc_teardown_banner(
        ui,
        lambda: {"state": "failed", "kind": "delete", "stack": "cdc-z"},
        on_retry=lambda: None,
        on_dismiss=lambda: None,
    )
    assert ui.spinners == 0
    assert "In progress" not in ui.badges
    assert ui.icons  # the static error icon is still rendered
    assert not ui.timer_armed  # terminal -> no self-poll
    assert [label for label, _ in ui.buttons] == ["Retry cleanup", "Dismiss"]


def test_render_cdc_teardown_banner_silent_when_nothing_in_flight() -> None:
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    ui = _BannerUi()
    _render_cdc_teardown_banner(ui, lambda: None)  # getter: no teardown in flight
    assert ui.texts == []
    assert not ui.timer_armed  # no pointless polling when idle


def test_render_cdc_teardown_banner_none_getter_is_noop() -> None:
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    ui = _BannerUi()
    _render_cdc_teardown_banner(ui, None)  # feature not wired
    assert ui.texts == []
    assert not ui.timer_armed


def test_render_cdc_teardown_banner_survives_getter_error() -> None:
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    def _boom():
        raise RuntimeError("probe failed")

    ui = _BannerUi()
    _render_cdc_teardown_banner(ui, _boom)  # a broken getter must never break render
    assert ui.texts == []


def test_render_cdc_teardown_banner_running_has_no_action_buttons() -> None:
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    ui = _BannerUi()
    _render_cdc_teardown_banner(
        ui, lambda: {"state": "running", "kind": "delete", "stack": "cdc-z"}
    )
    assert any("teardown in progress" in t.lower() for t in ui.texts)
    assert ui.buttons == []  # no retry/dismiss while running
    assert ui.timer_armed  # running self-polls


def test_render_cdc_teardown_banner_failed_shows_retry_and_dismiss() -> None:
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    calls = {"retry": 0, "dismiss": 0}
    ui = _BannerUi()
    _render_cdc_teardown_banner(
        ui,
        lambda: {"state": "failed", "kind": "delete", "stack": "cdc-z"},
        on_retry=lambda: calls.__setitem__("retry", calls["retry"] + 1),
        on_dismiss=lambda: calls.__setitem__("dismiss", calls["dismiss"] + 1),
    )
    assert any("failed" in t.lower() for t in ui.texts)
    assert not ui.timer_armed  # failed is terminal-until-acted; no self-poll churn
    by_label = {label: cb for label, cb in ui.buttons}
    assert "Retry cleanup" in by_label and "Dismiss" in by_label
    # Clicking each action invokes the wired callback.
    by_label["Retry cleanup"]()
    by_label["Dismiss"]()
    assert calls == {"retry": 1, "dismiss": 1}


# ---------------------------------------------------------------------------
# Connect nav icon: shows the CONNECTION state, not just the selected view
# ---------------------------------------------------------------------------


def test_connection_nav_state_distinguishes_the_three_situations() -> None:
    """The icon used to reflect only whether Connect was SELECTED.

    So a session whose credentials had been dropped by an app restart looked exactly like
    a healthy one, and nothing signalled that Connect had to be revisited before anything
    could run.
    """
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import connection_nav_state

    # A fresh session is not connected, but that is normal -- not something to flag.
    assert connection_nav_state(SessionConnectionState()) == "unset"

    # Both verified in this process.
    live = SessionConnectionState()
    live.source_verified = True
    live.target_verified = True
    assert connection_nav_state(live) == "connected"

    # Restored progress but unverified -> must be flagged for re-verification.
    restored = SessionConnectionState()
    restored.set_workflow(
        with_status(restored.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    assert connection_nav_state(restored) == "reconnect"

    # Half-verified still needs reconnection: both are required before anything runs.
    half = SessionConnectionState()
    half.source_verified = True
    half.set_workflow(
        with_status(half.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    assert connection_nav_state(half) == "reconnect"


def test_connection_nav_state_agrees_with_the_reconnect_banner() -> None:
    # The icon and the banner describe the same condition, so they are driven by the same
    # signal -- otherwise one could say "reconnect" while the other stayed silent.
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import connection_nav_state, reconnect_notice

    fresh = SessionConnectionState()
    restored = SessionConnectionState()
    restored.set_workflow(
        with_status(restored.workflow, WorkflowStep.EVALUATION, StepStatus.DONE)
    )
    for state in (fresh, restored):
        expects_reconnect = reconnect_notice(state) is not None
        assert (connection_nav_state(state) == "reconnect") is expects_reconnect
def test_start_over_tiles_name_the_stacks_they_would_delete() -> None:
    """"Delete all CDC infrastructure" must say WHAT it deletes.

    The account can hold a pipeline the operator must NOT touch (another window's), and
    the stack carries no owner tag, so the tool cannot decide for them. The name is the
    only thing that lets them answer safely -- and in the notice alone it reads as context
    for the question rather than as the delete target.
    """
    ui = _render_start_over_dialog(
        cdc_deployed=True,
        on_reset_cdc=lambda _mode: None,
        cdc_stack_name="cdc-a",
        cdc_stack_names=["cdc-a", "cdc-b"],
    )
    blob = " ".join(ui.texts)
    # Both names appear on the destructive choice itself, not only in the notice.
    delete_tile = next(t for t in ui.texts if t.startswith("Delete all"))
    assert "cdc-a" in delete_tile and "cdc-b" in delete_tile
    # The count is stated rather than a bare "all".
    assert "2" in delete_tile
    # Plural wording throughout, and the reset is still executable.
    assert "2 CDC pipelines are running" in blob
    assert ui.click_wired
    # The notice above the tiles also names them: it is what explains that a pipeline may
    # belong to another window, so it cannot fall back to a generic "the cdc-stack".
    notice = next(t for t in ui.texts if "keeps running on AWS" in t or "keep running on AWS" in t)
    assert "cdc-a" in notice and "cdc-b" in notice

    # Single stack: named, singular wording.
    one = _render_start_over_dialog(
        cdc_deployed=True,
        on_reset_cdc=lambda _mode: None,
        cdc_stack_name="cdc-solo",
        cdc_stack_names=["cdc-solo"],
    )
    solo_blob = " ".join(one.texts)
    solo_tile = next(t for t in one.texts if t.startswith("Delete all"))
    assert "cdc-solo" in solo_tile
    assert "A CDC pipeline is running" in solo_blob
    assert "2 CDC pipelines" not in solo_blob
    solo_notice = next(t for t in one.texts if "keeps running on AWS" in t)
    assert "cdc-solo" in solo_notice




# ---------------------------------------------------------------------------
# Multi-stack teardown: the banner must follow EVERY stack, then report the result
# ---------------------------------------------------------------------------


class _QJob:
    def __init__(self, status: str) -> None:
        self.status = status


class _QueueJobManager:
    def __init__(self, statuses: dict) -> None:
        self._statuses = statuses

    def get_status(self, job_id):
        # JobNotFoundError, NOT KeyError: that is what the real JobManager raises and the
        # only exception _current_job catches. A KeyError-raising double would make an
        # unknown job crash the banner instead of reading as "settled" -- exactly the
        # pruned-record case this must tolerate.
        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id not in self._statuses:
            raise JobNotFoundError(job_id)
        return _QJob(self._statuses[job_id])


def test_banner_advances_through_every_stack_in_a_multi_stack_teardown() -> None:
    """Start over can tear down several cdc-stacks, but the durable marker is ONE slot.

    Only the first stack claims it (``should_replace_teardown_marker`` refuses the rest
    while it runs), so the banner followed stack 1 and vanished the instant it settled --
    while the others were still deleting and still billing for MSK / NAT, with nothing on
    screen. The queue lets the banner move to the next unfinished stack.
    """
    from dsql_migrator.ui.data_migration._cdc_status import next_unfinished_teardown

    queue = [("j1", "cdc-a"), ("j2", "cdc-b"), ("j3", "cdc-c")]

    # First stack running -> follow it, and report its position.
    jm = _QueueJobManager({"j1": "RUNNING", "j2": "PENDING", "j3": "PENDING"})
    assert next_unfinished_teardown(jm, queue) == ("j1", "cdc-a", 1, 3)

    # It finishes -> advance to the next, do NOT go silent.
    jm = _QueueJobManager({"j1": "DONE", "j2": "RUNNING", "j3": "PENDING"})
    assert next_unfinished_teardown(jm, queue) == ("j2", "cdc-b", 2, 3)

    jm = _QueueJobManager({"j1": "DONE", "j2": "DONE", "j3": "RUNNING"})
    assert next_unfinished_teardown(jm, queue) == ("j3", "cdc-c", 3, 3)

    # Only when every one has settled is there nothing to follow.
    jm = _QueueJobManager({"j1": "DONE", "j2": "DONE", "j3": "DONE"})
    assert next_unfinished_teardown(jm, queue) is None

    # A job whose record is gone (pruned / lost across a restart) counts as settled,
    # matching how the single-stack path already treats an unknown job.
    jm = _QueueJobManager({"j2": "RUNNING"})
    assert next_unfinished_teardown(jm, queue) == ("j2", "cdc-b", 2, 3)
    assert next_unfinished_teardown(_QueueJobManager({}), queue) is None
    # Empty queue is the single-stack case -> nothing to advance to.
    assert next_unfinished_teardown(_QueueJobManager({}), []) is None


def test_running_banner_states_which_stack_of_how_many() -> None:
    """Naming one stack made a multi-stack teardown look like a single one -- so it
    appeared to finish early while the rest were still going."""
    from dsql_migrator.ui.workflow import _cdc_teardown_banner_copy

    _tone, _header, body = _cdc_teardown_banner_copy(
        {"state": "running", "kind": "delete", "stack": "cdc-a", "index": 1, "total": 3}
    )
    assert "1 of 3" in body
    assert "the rest follow" in body

    # On the LAST stack "the rest follow" would be false -- and this is exactly where the
    # operator decides whether it is worth waiting.
    _t, _h, last = _cdc_teardown_banner_copy(
        {"state": "running", "kind": "delete", "stack": "cdc-c", "index": 3, "total": 3}
    )
    assert "3 of 3" in last
    assert "the rest follow" not in last
    assert "the last one" in last

    # A single stack says nothing about position (the old, correct wording).
    _t2, _h2, single = _cdc_teardown_banner_copy(
        {"state": "running", "kind": "delete", "stack": "cdc-a"}
    )
    assert "of 1" not in single


def test_finished_teardown_reports_a_result_that_survives_a_refresh() -> None:
    """Completion was signalled ONLY by a ui.notify toast, which hangs off a ui.timer and
    so is gone after a refresh -- leaving nothing to distinguish "the 45-minute teardown
    finished" from "it never ran", for an operation designed to be walked away from."""
    from dsql_migrator.ui.workflow import _cdc_teardown_banner_copy

    tone, header, body = _cdc_teardown_banner_copy(
        {"state": "done", "kind": "delete", "stacks": ["cdc-a", "cdc-b"]}
    )
    assert tone == "success"
    assert "deleted" in header.lower()
    # Names EVERY stack (a multi-stack teardown previously surfaced only one) and the
    # thing the operator cares about: billing stopped.
    assert "cdc-a" in body and "cdc-b" in body
    assert "billing has stopped" in body

    # A stop reports what it kept, and that a restart resumes rather than re-loads.
    tone, header, body = _cdc_teardown_banner_copy(
        {"state": "done", "kind": "stop", "stacks": ["cdc-a"]}
    )
    assert tone == "success"
    assert "connectors" in header.lower()
    assert "resumes from where streaming left off" in body


def test_completion_notice_is_dismissed_by_the_user_not_a_timer() -> None:
    """Rendered with a close button and NO self-poll: auto-hiding is what made
    "finished" indistinguishable from "never ran"."""
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    dismissed: list[bool] = []
    ui = _BannerUi()
    _render_cdc_teardown_banner(
        ui,
        lambda: None,  # nothing in flight
        done_getter=lambda: {"kind": "delete", "stacks": ["cdc-a"]},
        on_done_dismiss=lambda: dismissed.append(True),
    )
    blob = " ".join(ui.texts)
    assert "CDC infrastructure deleted" in blob
    # A close affordance exists, and it clears the notice.
    assert ui.buttons, "expected a dismiss button"
    _label, handler = ui.buttons[0]
    assert handler is not None
    handler()
    assert dismissed == [True]
    # No poll timer: the notice is terminal until the user acts. Auto-hiding is what made
    # "finished" indistinguishable from "never ran".
    assert not ui.timer_armed, "the completion notice must not self-poll away"


def test_in_flight_teardown_wins_over_a_stale_completion_notice() -> None:
    """While a teardown runs, an earlier run's completion notice is stale."""
    from dsql_migrator.ui.workflow import _render_cdc_teardown_banner

    ui = _BannerUi()
    _render_cdc_teardown_banner(
        ui,
        lambda: {"state": "running", "kind": "delete", "stack": "cdc-new"},
        done_getter=lambda: {"kind": "delete", "stacks": ["cdc-old"]},
        on_done_dismiss=lambda: None,
    )
    blob = " ".join(ui.texts)
    assert "teardown in progress" in blob
    assert "CDC infrastructure deleted" not in blob
    # The running banner DOES self-poll (so it flips when the job settles).
    assert ui.timer_armed


def test_teardown_queue_progress_and_finished_stacks() -> None:
    """The two pure decisions the banner getter delegates to.

    Both were previously inline in ``build_page``'s closure, where no test could reach
    them -- so "queue never recorded", "banner goes silent", and "no durable completion"
    all survived mutation testing.
    """
    from dsql_migrator.ui.data_migration._cdc_status import (
        finished_teardown_stacks,
        teardown_queue_progress,
    )

    queue = [("j1", "cdc-a"), ("j2", "cdc-b"), ("j3", "cdc-c")]
    # Position within a multi-stack teardown, so the banner can say "2 of 3".
    assert teardown_queue_progress(queue, "j1") == (1, 3)
    assert teardown_queue_progress(queue, "j3") == (3, 3)
    # A single stack has nothing to disambiguate -> no "1 of 1" noise.
    assert teardown_queue_progress([("j1", "cdc-a")], "j1") is None
    assert teardown_queue_progress([], "j1") is None
    # A job outside the queue (or none) contributes no position rather than guessing.
    assert teardown_queue_progress(queue, "unknown") is None
    assert teardown_queue_progress(queue, None) is None

    # The completion notice must name EVERY stack that was torn down; a multi-stack
    # teardown previously surfaced only the tracked one, understating what happened.
    assert finished_teardown_stacks(queue, "cdc-a") == ["cdc-a", "cdc-b", "cdc-c"]
    # Single-stack path: fall back to the tracked stack.
    assert finished_teardown_stacks([], "cdc-solo") == ["cdc-solo"]
    assert finished_teardown_stacks([], None) == []


def test_banner_getter_wires_the_queue_advance_and_completion() -> None:
    """Structural guard for the three behaviours that live in build_page's closure.

    They cannot be called directly (the closure needs a live session + stores), and each
    failure mode is SILENT -- the banner simply stops mentioning stacks that are still
    deleting and still billing. So assert the wiring: the queue is recorded, the advance
    is consulted, and completion is persisted before the marker is cleared.
    """
    import ast
    import inspect

    from dsql_migrator.ui import app as app_module

    tree = ast.parse(inspect.getsource(app_module))
    by_name = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }

    # 1. Every launched teardown is recorded, or the banner has nothing to advance to.
    teardown = ast.unparse(by_name["_cdc_teardown_on_reset"])
    assert "set_cdc_teardown_queue(" in teardown
    assert "track_in_queue=launched_queue" in teardown

    # 2. On settle the banner advances to the next unfinished stack instead of clearing.
    banner = ast.unparse(by_name["_cdc_teardown_banner"])
    assert "next_unfinished_teardown(" in banner
    assert "advance_cdc_teardown(" in banner
    # The advance must be REACHED, not merely present: gating it on a constant-false test
    # (or dropping the branch body) puts the banner straight back to going silent while
    # later stacks are still deleting. Assert the guard tests the resolver's result.
    advance_guard = [
        node
        for node in ast.walk(by_name["_cdc_teardown_banner"])
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and isinstance(node.test.left, ast.Name)
        and node.test.left.id == "following"
    ]
    assert advance_guard, (
        "the queue-advance must be guarded on `following is not None`, not a constant"
    )
    assert "advance_cdc_teardown(" in ast.unparse(advance_guard[0])
    # 3. ...and records a durable completion notice before clearing the marker, so a
    #    refresh can still tell "finished" from "never ran".
    assert "set_cdc_teardown_done(" in banner
    done_at = banner.index("set_cdc_teardown_done(")
    cleared_at = banner.index("clear_cdc_teardown()")
    assert done_at < cleared_at, "completion must be recorded BEFORE the marker is cleared"
    # The dismiss path exists for the close button.
    assert "dismiss_cdc_teardown_done" in ast.unparse(
        by_name["_cdc_teardown_done_dismiss"]
    )


def test_build_migration_diagram_labels_postgres_source() -> None:
    # A PostgreSQL source renders "Aurora PostgreSQL" / "PostgreSQL" wording (not MySQL),
    # driven by source_config.source_type.
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    state.source_config = SourceConnectionConfig(
        host="mydb.cluster-abc123.ap-northeast-2.rds.amazonaws.com",
        database="app", source_type=SourceType.POSTGRES,
    )
    state.source_verified = True
    # PG version shapes: verbose banner + clean server_version + aurora_version()
    state.set_source_version("PostgreSQL 16.4 on aarch64", "16.4", "16.4")
    source, _tool, _target = build_migration_diagram(state)
    assert source.title == "Aurora PostgreSQL"  # .cluster- host + aurora_version
    texts = [text for _icon, text in source.details]
    assert "Aurora PostgreSQL 16.4 (PostgreSQL 16.4)" in texts


def test_build_migration_diagram_postgres_placeholder_before_verify() -> None:
    # Unverified PG source -> the placeholder wording is PostgreSQL, not MySQL.
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import build_migration_diagram

    state = SessionConnectionState()
    state.source_config = SourceConnectionConfig(
        host="h", database="app", source_type=SourceType.POSTGRES
    )
    state.source_verified = False
    source, _tool, _target = build_migration_diagram(state)
    assert source.title == "Source PostgreSQL"
    assert source.subtitle == "RDS / Aurora PostgreSQL"


def test_format_source_engine_postgres_variants() -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.workflow import format_source_engine

    pg = SourceType.POSTGRES
    # Aurora PostgreSQL with the clean server_version alongside.
    assert (
        format_source_engine("PostgreSQL 16.4 on x", "16.4", "16.4", source_type=pg)
        == "Aurora PostgreSQL 16.4 (PostgreSQL 16.4)"
    )
    # Plain PostgreSQL from the clean server_version.
    assert format_source_engine(None, "16.4", None, source_type=pg) == "PostgreSQL 16.4"
    # Only the verbose banner captured -> shown as-is.
    assert (
        format_source_engine("PostgreSQL 16.4 on x", None, None, source_type=pg)
        == "PostgreSQL PostgreSQL 16.4 on x"
    )
    assert format_source_engine(None, None, None, source_type=pg) is None
    # MySQL default path is unchanged.
    assert format_source_engine("8.0.35", None) == "MySQL 8.0.35"
