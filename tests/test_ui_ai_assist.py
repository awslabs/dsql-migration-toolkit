"""Unit tests for the AI-assisted conversion UI logic (NiceGUI-agnostic).

These cover the parts of the Step 2 AI-assist integration that do not touch
NiceGUI:

- Building :class:`AiAssistConfig` from the settings form: opt-in default
  (disabled) and default model id ``us.anthropic.claude-sonnet-4-6`` (Requirements 11.1-11.4).
- Per-session AI config state isolation (Requirement 9.2 pattern).
- Suggestion review status transitions (edit / approve / reject) and the
  invariant that editing revokes any prior approval.
- The human-review gate (Property 13): only an explicitly APPROVED suggestion is
  forwarded to the Schema Applier path; rejected / pending / edited-but-not
  -approved and non-SCHEMA suggestions are excluded (Requirements 10.9, 11.7).
"""

from __future__ import annotations

from dsql_migrator.core.models import (
    AiAccessCheckResult,
    AiAssistConfig,
    AiConversionSuggestion,
    AssessmentItem,
    AssessmentReport,
    Classification,
)
from dsql_migrator.ui.ai_assist import (
    ACCESS_CHECK_NOTIFY_TYPES,
    AI_STATUS_APPROVED,
    AI_STATUS_EDITED,
    AI_STATUS_PENDING_REVIEW,
    AI_STATUS_REJECTED,
    DEFAULT_BEDROCK_MODEL_ID,
    approve_suggestion,
    approved_suggestions,
    build_ai_assist_config,
    edit_suggestion,
    is_approved,
    map_access_check_display,
    reject_suggestion,
    run_verify_ai_access,
)
from dsql_migrator.ui.schema_conversion import (
    SchemaConversionState,
    SchemaConversionStore,
    ai_candidate_object_names,
    build_ai_apply_objects,
)
from dsql_migrator.ui.session import SessionStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _suggestion(
    object_name: str = "audit_trigger",
    *,
    kind: str = "SCHEMA",
    sql: str = "CREATE TABLE foo (id int PRIMARY KEY)",
    status: str = AI_STATUS_PENDING_REVIEW,
    approved: bool = False,
) -> AiConversionSuggestion:
    return AiConversionSuggestion(
        object_name=object_name,
        kind=kind,  # type: ignore[arg-type]
        suggested_sql_or_expr=sql,
        rationale="example",
        model_id="us.anthropic.claude-sonnet-4-6",
        status=status,  # type: ignore[arg-type]
        approved_by_user=approved,
    )


# ---------------------------------------------------------------------------
# AiAssistConfig model defaults
# ---------------------------------------------------------------------------


def test_ai_assist_config_defaults_disabled_and_default_model() -> None:
    config = AiAssistConfig()
    assert config.enabled is False
    assert config.model_id == "us.anthropic.claude-sonnet-4-6"
    assert config.region is None


def test_ai_conversion_suggestion_defaults_pending_and_unapproved() -> None:
    suggestion = _suggestion()
    assert suggestion.status == AI_STATUS_PENDING_REVIEW
    assert suggestion.approved_by_user is False


# ---------------------------------------------------------------------------
# build_ai_assist_config (settings form -> config)
# ---------------------------------------------------------------------------


def test_build_ai_assist_config_disabled_with_default_model() -> None:
    config = build_ai_assist_config(enabled=False)
    assert config.enabled is False
    assert config.model_id == DEFAULT_BEDROCK_MODEL_ID
    assert config.region is None


def test_build_ai_assist_config_blank_model_falls_back_to_default() -> None:
    config = build_ai_assist_config(enabled=True, model_id="   ", region="  ")
    assert config.enabled is True
    assert config.model_id == DEFAULT_BEDROCK_MODEL_ID
    assert config.region is None


def test_build_ai_assist_config_keeps_explicit_values() -> None:
    config = build_ai_assist_config(
        enabled=True, model_id="anthropic.claude-x", region="us-east-1"
    )
    assert config.enabled is True
    assert config.model_id == "anthropic.claude-x"
    assert config.region == "us-east-1"


# ---------------------------------------------------------------------------
# Per-session AI config state isolation
# ---------------------------------------------------------------------------


def test_session_ai_assist_defaults_disabled() -> None:
    store = SessionStore()
    state = store.get_or_create("session-a")
    assert state.ai_assist.enabled is False
    assert state.ai_assist.model_id == "us.anthropic.claude-sonnet-4-6"


def test_session_ai_assist_is_isolated_per_session() -> None:
    store = SessionStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")

    a.set_ai_assist(build_ai_assist_config(enabled=True, model_id="model-x"))

    assert a.ai_assist.enabled is True
    assert a.ai_assist.model_id == "model-x"
    # Session B is unaffected and keeps the opt-in default.
    assert b.ai_assist.enabled is False
    assert b.ai_assist.model_id == "us.anthropic.claude-sonnet-4-6"


def test_session_clear_resets_ai_assist_to_default() -> None:
    store = SessionStore()
    state = store.get_or_create("session-a")
    state.set_ai_assist(build_ai_assist_config(enabled=True, model_id="model-x"))

    state.clear()

    assert state.ai_assist.enabled is False
    assert state.ai_assist.model_id == "us.anthropic.claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# Suggestion status transitions
# ---------------------------------------------------------------------------


def test_edit_suggestion_sets_edited_and_clears_approval() -> None:
    edited = edit_suggestion(_suggestion(), "CREATE TABLE bar (id int PRIMARY KEY)")
    assert edited.status == AI_STATUS_EDITED
    assert edited.approved_by_user is False
    assert edited.suggested_sql_or_expr == "CREATE TABLE bar (id int PRIMARY KEY)"


def test_approve_suggestion_marks_approved() -> None:
    approved = approve_suggestion(_suggestion())
    assert approved.status == AI_STATUS_APPROVED
    assert approved.approved_by_user is True


def test_reject_suggestion_marks_rejected_and_unapproved() -> None:
    rejected = reject_suggestion(approve_suggestion(_suggestion()))
    assert rejected.status == AI_STATUS_REJECTED
    assert rejected.approved_by_user is False


def test_editing_an_approved_suggestion_revokes_approval() -> None:
    """Property 13: an edit after approval must require re-approval."""
    approved = approve_suggestion(_suggestion())
    re_edited = edit_suggestion(approved, "CREATE TABLE baz (id int PRIMARY KEY)")
    assert is_approved(re_edited) is False
    assert re_edited.status == AI_STATUS_EDITED


# ---------------------------------------------------------------------------
# Review gate -- Property 13
# ---------------------------------------------------------------------------


def test_is_approved_only_for_approved_and_flagged() -> None:
    assert is_approved(approve_suggestion(_suggestion())) is True
    assert is_approved(_suggestion(status=AI_STATUS_PENDING_REVIEW)) is False
    assert is_approved(_suggestion(status=AI_STATUS_EDITED)) is False
    assert is_approved(_suggestion(status=AI_STATUS_REJECTED)) is False
    # status says APPROVED but the explicit flag is missing: not approved.
    assert is_approved(_suggestion(status=AI_STATUS_APPROVED, approved=False)) is False


def test_approved_suggestions_excludes_unapproved() -> None:
    pending = _suggestion("a", status=AI_STATUS_PENDING_REVIEW)
    edited = _suggestion("b", status=AI_STATUS_EDITED)
    rejected = _suggestion("c", status=AI_STATUS_REJECTED)
    approved = approve_suggestion(_suggestion("d"))

    result = approved_suggestions([pending, edited, rejected, approved])

    assert [s.object_name for s in result] == ["d"]


def test_build_ai_apply_objects_forwards_only_approved_schema() -> None:
    """Property 13: only explicitly approved SCHEMA suggestions reach apply."""
    approved = approve_suggestion(
        _suggestion("orders_v2", sql="CREATE TABLE orders_v2 (id int PRIMARY KEY)")
    )
    pending = _suggestion("pending_obj")
    edited = _suggestion("edited_obj", status=AI_STATUS_EDITED)
    rejected = _suggestion("rejected_obj", status=AI_STATUS_REJECTED)

    objects = build_ai_apply_objects([approved, pending, edited, rejected])

    assert [obj.object_name for obj in objects] == ["orders_v2"]
    assert objects[0].ddls == ("CREATE TABLE orders_v2 (id int PRIMARY KEY)",)


def test_build_ai_apply_objects_excludes_non_schema_suggestions() -> None:
    approved_data = approve_suggestion(
        _suggestion("col_expr", kind="DATA", sql="CAST(x AS TEXT)")
    )
    approved_query = approve_suggestion(
        _suggestion("q1", kind="QUERY", sql="SELECT 1")
    )
    assert build_ai_apply_objects([approved_data, approved_query]) == []


def test_build_ai_apply_objects_splits_multiple_statements() -> None:
    approved = approve_suggestion(
        _suggestion(
            "multi",
            sql="CREATE TABLE multi (id int PRIMARY KEY); CREATE INDEX ASYNC i ON multi (id)",
        )
    )
    objects = build_ai_apply_objects([approved])
    assert len(objects) == 1
    assert len(objects[0].ddls) == 2
    assert objects[0].ddls[0].lower().startswith("create table")
    assert "CREATE INDEX ASYNC" in objects[0].ddls[1]


def test_build_ai_apply_objects_drops_approved_but_empty_text() -> None:
    approved_empty = approve_suggestion(_suggestion("empty", sql="   ;  "))
    assert build_ai_apply_objects([approved_empty]) == []


# ---------------------------------------------------------------------------
# AI candidate selection (Requirement 11.5)
# ---------------------------------------------------------------------------


def _assessment() -> AssessmentReport:
    return AssessmentReport.from_items(
        [
            AssessmentItem(
                object_name="customers",
                rule_id="R1",
                classification=Classification.AUTO,
            ),
            AssessmentItem(
                object_name="audit_trigger",
                rule_id="R2",
                classification=Classification.MANUAL,
            ),
            AssessmentItem(
                object_name="legacy_proc",
                rule_id="R3",
                classification=Classification.UNSUPPORTED,
            ),
        ]
    )


def test_ai_candidate_object_names_selects_manual_and_unsupported() -> None:
    names = ai_candidate_object_names(_assessment())
    assert names == ["audit_trigger", "legacy_proc"]


def test_ai_candidate_object_names_dedupes() -> None:
    report = AssessmentReport.from_items(
        [
            AssessmentItem(
                object_name="dup",
                rule_id="R1",
                classification=Classification.MANUAL,
            ),
            AssessmentItem(
                object_name="dup",
                rule_id="R2",
                classification=Classification.UNSUPPORTED,
            ),
        ]
    )
    assert ai_candidate_object_names(report) == ["dup"]


def test_ai_candidate_object_names_empty_when_all_auto() -> None:
    report = AssessmentReport.from_items(
        [
            AssessmentItem(
                object_name="t", rule_id="R1", classification=Classification.AUTO
            )
        ]
    )
    assert ai_candidate_object_names(report) == []


# ---------------------------------------------------------------------------
# Per-session schema-conversion suggestion storage
# ---------------------------------------------------------------------------


def test_state_suggestion_storage_set_get_all_clear() -> None:
    state = SchemaConversionState()
    assert state.all_suggestions() == []
    assert state.get_suggestion("x") is None

    state.set_suggestion(_suggestion("x"))
    assert state.get_suggestion("x") is not None
    assert [s.object_name for s in state.all_suggestions()] == ["x"]

    # Re-storing the same object replaces, not duplicates.
    state.set_suggestion(approve_suggestion(_suggestion("x")))
    assert len(state.all_suggestions()) == 1
    assert state.get_suggestion("x").status == AI_STATUS_APPROVED  # type: ignore[union-attr]

    state.clear_suggestions()
    assert state.all_suggestions() == []


def test_store_suggestions_isolated_per_session() -> None:
    store = SchemaConversionStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")

    a.set_suggestion(_suggestion("only-a"))

    assert [s.object_name for s in a.all_suggestions()] == ["only-a"]
    assert b.all_suggestions() == []


# ---------------------------------------------------------------------------
# "Verify AI access" result-display mapping (Requirements 11.13, 11.14)
# ---------------------------------------------------------------------------


def _access_result(
    *,
    ok: bool,
    reason: str,
    detail: str = "actionable next step",
    model_id: str = "us.anthropic.claude-sonnet-4-6",
    region: str | None = "us-east-1",
) -> AiAccessCheckResult:
    return AiAccessCheckResult(
        ok=ok,
        reason=reason,  # type: ignore[arg-type]
        detail=detail,
        model_id=model_id,
        region=region,
    )


def test_map_access_check_display_ok_is_positive() -> None:
    result = _access_result(ok=True, reason="OK", detail="AI access verified.")
    display = map_access_check_display(result)
    assert display.notify_type == "positive"
    # The message is exactly the result's (credential-free) detail.
    assert display.message == "AI access verified."


def test_map_access_check_display_access_denied_is_negative() -> None:
    result = _access_result(ok=False, reason="ACCESS_DENIED", detail="Add permission.")
    display = map_access_check_display(result)
    assert display.notify_type == "negative"
    assert display.message == "Add permission."


def test_map_access_check_display_model_not_enabled_is_negative() -> None:
    result = _access_result(ok=False, reason="MODEL_NOT_ENABLED")
    assert map_access_check_display(result).notify_type == "negative"


def test_map_access_check_display_throttled_is_warning() -> None:
    result = _access_result(ok=False, reason="THROTTLED")
    assert map_access_check_display(result).notify_type == "warning"


def test_map_access_check_display_unknown_is_warning() -> None:
    result = _access_result(ok=False, reason="UNKNOWN")
    assert map_access_check_display(result).notify_type == "warning"


def test_access_check_notify_types_cover_every_reason() -> None:
    # Each reason in the model literal has an explicit notify type.
    assert set(ACCESS_CHECK_NOTIFY_TYPES) == {
        "OK",
        "ACCESS_DENIED",
        "MODEL_NOT_ENABLED",
        "THROTTLED",
        "UNKNOWN",
    }


def test_map_access_check_display_message_never_exposes_credentials() -> None:
    # The mapping only forwards the result's detail; it reads no credential and
    # the detail is credential-free by construction (Requirement 11.15).
    detail = "Enable the model in BEDROCK_REGION us-east-1, then retry."
    result = _access_result(ok=False, reason="MODEL_NOT_ENABLED", detail=detail)
    display = map_access_check_display(result)
    assert display.message == detail


# ---------------------------------------------------------------------------
# "Verify AI access" runner -- global profile threading (Requirements 11.15)
# ---------------------------------------------------------------------------


class _FakeVerifyAssistant:
    """Returns a canned access-check result for verify_access()."""

    def __init__(self, result: AiAccessCheckResult) -> None:
        self._result = result

    def verify_access(self) -> AiAccessCheckResult:
        return self._result


def test_run_verify_ai_access_threads_profile_to_assistant_factory() -> None:
    captured: dict[str, object] = {}
    config = AiAssistConfig(enabled=True, model_id="model-x", region="us-east-1")
    expected = _access_result(ok=True, reason="OK", model_id="model-x")

    def factory(cfg: AiAssistConfig, aws_profile: str | None) -> _FakeVerifyAssistant:
        captured["config"] = cfg
        captured["aws_profile"] = aws_profile
        return _FakeVerifyAssistant(expected)

    result = run_verify_ai_access(config, "prod", assistant_factory=factory)

    # The selected (non-secret) profile name is threaded into the build path that
    # constructs the assistant/Bedrock client (the Task 17.2 shared-session seam).
    assert captured["aws_profile"] == "prod"
    assert captured["config"] is config
    assert result is expected


def test_run_verify_ai_access_default_profile_is_none() -> None:
    captured: dict[str, object] = {}
    config = AiAssistConfig()

    def factory(cfg: AiAssistConfig, aws_profile: str | None) -> _FakeVerifyAssistant:
        captured["config"] = cfg
        captured["aws_profile"] = aws_profile
        return _FakeVerifyAssistant(_access_result(ok=True, reason="OK"))

    run_verify_ai_access(config, None, assistant_factory=factory)

    # No profile selected -> None -> standard AWS credential chain (Req 9.6).
    assert captured["aws_profile"] is None
    assert captured["config"] is config
