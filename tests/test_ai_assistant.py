"""Unit tests for AI-assist data models, config loading, and the Bedrock seam.

Covers Task 16.1:

- ``AiAssistConfig`` / ``AiConversionSuggestion`` serialization round-trips,
  defaults, and validation (Requirements 11.1-11.4).
- Loading ``BEDROCK_MODEL_ID`` / ``BEDROCK_REGION`` into an ``AiAssistConfig``
  (default model id, region, opt-in disabled).
- Constructing the ``bedrock-runtime`` client from an injected fake session:
  the right region is forwarded, no credentials are passed (IAM-based auth), and
  building the client performs no network call.

All Bedrock interaction is mocked through an injected fake session, so these
tests never reach AWS.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError

from dsql_migrator.core.ai_assistant import (
    BEDROCK_RUNTIME_SERVICE,
    ENV_BEDROCK_MODEL_ID,
    ENV_BEDROCK_REGION,
    MAX_SUGGESTION_CHARS,
    AiAssistUnavailableError,
    AiConversionAssistant,
    build_bedrock_runtime_client,
    load_ai_assist_config,
    validate_suggested_sql,
)
from dsql_migrator.core.ai_assistant import (
    _extract_suggestion_text,  # internal: exercised for untrusted-output safety
)
from dsql_migrator.core.models import (
    AiAccessCheckResult,
    AiAssistConfig,
    AiConversionSuggestion,
)


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeClient:
    """A stand-in bedrock-runtime client that performs no work."""


class _FakeSession:
    """A fake boto3 session that records how ``client`` was called.

    It never performs network I/O, so constructing a client through it proves
    the construction path does not call AWS.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeClient:
        self.calls.append((service_name, kwargs))
        return _FakeClient()


# ---------------------------------------------------------------------------
# AiAssistConfig model: defaults, serialization round-trip, validation
# ---------------------------------------------------------------------------


def test_ai_assist_config_defaults_are_opt_in() -> None:
    config = AiAssistConfig()
    assert config.enabled is False
    assert config.model_id == "us.anthropic.claude-sonnet-4-6"
    assert config.region is None


def test_ai_assist_config_serialization_round_trip() -> None:
    config = AiAssistConfig(enabled=True, model_id="anthropic.claude-x", region="us-east-1")
    restored = AiAssistConfig.model_validate(config.model_dump())
    assert restored == config


def test_ai_assist_config_rejects_blank_model_id() -> None:
    with pytest.raises(ValidationError):
        AiAssistConfig(model_id="")


def test_ai_assist_config_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        AiAssistConfig(unexpected="value")


# ---------------------------------------------------------------------------
# AiConversionSuggestion model: defaults, serialization round-trip, validation
# ---------------------------------------------------------------------------


def test_ai_conversion_suggestion_defaults() -> None:
    suggestion = AiConversionSuggestion(
        object_name="audit_trigger",
        kind="SCHEMA",
        suggested_sql_or_expr="CREATE TABLE t (id int PRIMARY KEY)",
        model_id="us.anthropic.claude-sonnet-4-6",
    )
    assert suggestion.status == "PENDING_REVIEW"
    assert suggestion.approved_by_user is False
    assert suggestion.confidence is None
    assert suggestion.rationale == ""


def test_ai_conversion_suggestion_serialization_round_trip() -> None:
    suggestion = AiConversionSuggestion(
        object_name="legacy_proc",
        kind="QUERY",
        suggested_sql_or_expr="SELECT 1",
        rationale="rewrite",
        confidence=0.5,
        model_id="us.anthropic.claude-sonnet-4-6",
        status="APPROVED",
        approved_by_user=True,
    )
    restored = AiConversionSuggestion.model_validate(suggestion.model_dump())
    assert restored == suggestion


def test_ai_conversion_suggestion_rejects_invalid_kind() -> None:
    with pytest.raises(ValidationError):
        AiConversionSuggestion(
            object_name="t",
            kind="TABLE",  # not one of SCHEMA/DATA/QUERY
            suggested_sql_or_expr="x",
            model_id="us.anthropic.claude-sonnet-4-6",
        )


def test_ai_conversion_suggestion_rejects_invalid_status() -> None:
    with pytest.raises(ValidationError):
        AiConversionSuggestion(
            object_name="t",
            kind="SCHEMA",
            suggested_sql_or_expr="x",
            model_id="us.anthropic.claude-sonnet-4-6",
            status="DONE",  # not a valid review status
        )


def test_ai_conversion_suggestion_requires_model_id() -> None:
    with pytest.raises(ValidationError):
        AiConversionSuggestion(
            object_name="t",
            kind="SCHEMA",
            suggested_sql_or_expr="x",
            model_id="",
        )


# ---------------------------------------------------------------------------
# load_ai_assist_config (BEDROCK_MODEL_ID / BEDROCK_REGION)
# ---------------------------------------------------------------------------


def test_load_ai_assist_config_uses_defaults_when_env_empty() -> None:
    config = load_ai_assist_config(env={})
    assert config.enabled is False
    assert config.model_id == "us.anthropic.claude-sonnet-4-6"
    assert config.region is None


def test_load_ai_assist_config_reads_model_id_and_region() -> None:
    env = {
        ENV_BEDROCK_MODEL_ID: "anthropic.claude-3",
        ENV_BEDROCK_REGION: "us-west-2",
    }
    config = load_ai_assist_config(env=env)
    assert config.model_id == "anthropic.claude-3"
    assert config.region == "us-west-2"
    # Still opt-in: env never enables AI assist.
    assert config.enabled is False


def test_load_ai_assist_config_treats_blank_values_as_unset() -> None:
    env = {ENV_BEDROCK_MODEL_ID: "   ", ENV_BEDROCK_REGION: "  "}
    config = load_ai_assist_config(env=env)
    assert config.model_id == "us.anthropic.claude-sonnet-4-6"
    assert config.region is None


def test_load_ai_assist_config_keys_are_not_prefixed() -> None:
    # The keys are exactly BEDROCK_MODEL_ID / BEDROCK_REGION (no DSQL_MIGRATOR_).
    assert ENV_BEDROCK_MODEL_ID == "BEDROCK_MODEL_ID"
    assert ENV_BEDROCK_REGION == "BEDROCK_REGION"
    env = {"DSQL_MIGRATOR_BEDROCK_MODEL_ID": "ignored"}
    config = load_ai_assist_config(env=env)
    assert config.model_id == "us.anthropic.claude-sonnet-4-6"


# ---------------------------------------------------------------------------
# build_bedrock_runtime_client (injected session seam, no AWS calls)
# ---------------------------------------------------------------------------


def test_build_bedrock_runtime_client_uses_injected_session_and_region() -> None:
    session = _FakeSession()
    config = AiAssistConfig(enabled=True, region="eu-central-1")

    client = build_bedrock_runtime_client(config, session=session)

    assert isinstance(client, _FakeClient)
    assert session.calls == [
        (BEDROCK_RUNTIME_SERVICE, {"region_name": "eu-central-1"})
    ]


def test_build_bedrock_runtime_client_omits_region_when_unset() -> None:
    session = _FakeSession()
    config = AiAssistConfig(enabled=True, region=None)

    build_bedrock_runtime_client(config, session=session)

    # When no region is configured, none is forwarded; the session decides.
    assert session.calls == [(BEDROCK_RUNTIME_SERVICE, {})]


def test_build_bedrock_runtime_client_passes_no_credentials() -> None:
    session = _FakeSession()
    config = AiAssistConfig(enabled=True, region="us-east-1")

    build_bedrock_runtime_client(config, session=session)

    _, kwargs = session.calls[0]
    # IAM-based auth only: no hardcoded credentials are ever passed.
    assert "aws_access_key_id" not in kwargs
    assert "aws_secret_access_key" not in kwargs
    assert "aws_session_token" not in kwargs


def test_build_bedrock_runtime_client_performs_no_network_call() -> None:
    # The fake session returns a client without any I/O; reaching this assert
    # without an exception confirms construction does not call AWS.
    session = _FakeSession()
    client = build_bedrock_runtime_client(AiAssistConfig(enabled=True), session=session)
    assert client is not None


# ---------------------------------------------------------------------------
# AiConversionAssistant: grounded prompts, InvokeModel call, suggestion output
# ---------------------------------------------------------------------------


class _FakeBedrockRuntimeClient:
    """A fake bedrock-runtime client recording InvokeModel calls.

    It returns a canned Anthropic-style response so suggestion generation never
    reaches AWS. Each call's kwargs are recorded so tests can assert the
    configured ``modelId`` was used and the request body is grounded.
    """

    def __init__(self, suggestion_text: str) -> None:
        self._suggestion_text = suggestion_text
        self.calls: list[dict[str, Any]] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        payload = {"content": [{"type": "text", "text": self._suggestion_text}]}
        return {"body": json.dumps(payload).encode("utf-8")}


def _decode_prompt(call: dict[str, Any]) -> str:
    """Return the user prompt text from a recorded InvokeModel call body."""
    body = json.loads(call["body"])
    return body["messages"][0]["content"][0]["text"]


def test_suggest_schema_conversion_grounds_prompt_and_returns_suggestion() -> None:
    client = _FakeBedrockRuntimeClient("CREATE TABLE t (id uuid PRIMARY KEY)")
    config = AiAssistConfig(enabled=True, model_id="anthropic.claude-test")
    assistant = AiConversionAssistant(config, client=client)

    suggestion = assistant.suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT AUTO_INCREMENT PRIMARY KEY)",
        deterministic_result="MANUAL: monotonic AUTO_INCREMENT key risks hot partition",
        dsql_constraints="Foreign keys unsupported; primary key required; indexes async",
    )

    # InvokeModel was called once with the configured model id (provenance).
    assert len(client.calls) == 1
    assert client.calls[0]["modelId"] == "anthropic.claude-test"

    # The prompt is grounded with source DDL, DSQL constraints, deterministic result.
    prompt = _decode_prompt(client.calls[0])
    assert "CREATE TABLE `orders`" in prompt
    assert "Foreign keys unsupported; primary key required; indexes async" in prompt
    assert "monotonic AUTO_INCREMENT key risks hot partition" in prompt

    # The returned suggestion is reviewable and carries provenance.
    assert isinstance(suggestion, AiConversionSuggestion)
    assert suggestion.kind == "SCHEMA"
    assert suggestion.object_name == "orders"
    assert suggestion.suggested_sql_or_expr == "CREATE TABLE t (id uuid PRIMARY KEY)"
    assert suggestion.model_id == "anthropic.claude-test"
    assert suggestion.status == "PENDING_REVIEW"
    assert suggestion.approved_by_user is False
    assert suggestion.rationale


def test_suggest_data_transformation_grounds_on_schema_converter_mapping() -> None:
    client = _FakeBedrockRuntimeClient("value::boolean")
    config = AiAssistConfig(enabled=True, model_id="anthropic.claude-test")
    assistant = AiConversionAssistant(config, client=client)

    suggestion = assistant.suggest_data_transformation(
        object_name="users.is_active",
        source_type="TINYINT(1)",
        sample_values=["0", "1"],
        deterministic_mapping="MANUAL: TINYINT(1) -> boolean by convention",
    )

    assert len(client.calls) == 1
    assert client.calls[0]["modelId"] == "anthropic.claude-test"

    prompt = _decode_prompt(client.calls[0])
    # Grounded on the source type, sample values, and deterministic mapping.
    assert "TINYINT(1)" in prompt
    assert "MANUAL: TINYINT(1) -> boolean by convention" in prompt
    # Grounded consistently with the SchemaConverter export-time type mapping:
    # TINYINT(1) maps to boolean, and the canonical mapping is cited in the prompt.
    assert "boolean" in prompt
    assert "SchemaConverter" in prompt

    assert suggestion.kind == "DATA"
    assert suggestion.object_name == "users.is_active"
    assert suggestion.suggested_sql_or_expr == "value::boolean"
    assert suggestion.model_id == "anthropic.claude-test"
    assert suggestion.status == "PENDING_REVIEW"
    assert suggestion.approved_by_user is False


def test_suggest_data_transformation_handles_unparsable_source_type() -> None:
    # An unparsable source type must not raise; the canonical mapping is reported
    # as unavailable while still grounding the rest of the prompt.
    client = _FakeBedrockRuntimeClient("(no change)")
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    suggestion = assistant.suggest_data_transformation(
        object_name="t.c",
        source_type="NOT A TYPE",
        sample_values=[],
        deterministic_mapping=None,
    )

    prompt = _decode_prompt(client.calls[0])
    assert "canonical mapping unavailable" in prompt
    assert suggestion.kind == "DATA"


def test_assistant_builds_client_lazily_when_none_injected(monkeypatch: Any) -> None:
    # With no client injected, the assistant builds one lazily on first use via
    # the module's default-session factory. Patching that factory to return a
    # fake session keeps the test offline and proves construction itself does no
    # AWS work (the factory is only touched on the first suggest call).
    fake_client = _FakeBedrockRuntimeClient("CREATE TABLE t (id uuid PRIMARY KEY)")

    class _LazySession:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def client(self, service_name: str, **kwargs: Any) -> Any:
            self.calls.append((service_name, kwargs))
            return fake_client

    session = _LazySession()
    monkeypatch.setattr(
        "dsql_migrator.core.ai_assistant._default_session", lambda: session
    )

    assistant = AiConversionAssistant(AiAssistConfig(enabled=True, region="us-east-1"))
    # No session/client touched at construction time.
    assert session.calls == []

    suggestion = assistant.suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result=None,
        dsql_constraints="primary key required",
    )

    # The lazy client was built from the configured region and used for InvokeModel.
    assert session.calls == [(BEDROCK_RUNTIME_SERVICE, {"region_name": "us-east-1"})]
    assert len(fake_client.calls) == 1
    assert suggestion.suggested_sql_or_expr == "CREATE TABLE t (id uuid PRIMARY KEY)"


# ---------------------------------------------------------------------------
# Task 16.3: untrusted-output handling + graceful degradation
# ---------------------------------------------------------------------------


class _RaisingBedrockRuntimeClient:
    """A fake bedrock-runtime client whose ``invoke_model`` always raises.

    Used to simulate Bedrock failures (permission/throttle/network/unknown)
    without reaching AWS.
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.calls: list[dict[str, Any]] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        raise self._exc


class _RawBodyBedrockRuntimeClient:
    """A fake client returning an arbitrary, possibly malformed, response body."""

    def __init__(self, body: Any) -> None:
        self._body = body

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        return {"body": self._body}


def _client_error(code: str) -> Exception:
    """Build a botocore ClientError-like exception carrying an error ``code``.

    A minimal stand-in (with the ``response['Error']['Code']`` shape botocore
    uses) so the classifier can be tested without importing botocore.
    """

    class _ClientError(Exception):
        def __init__(self, error_code: str) -> None:
            self.response = {"Error": {"Code": error_code, "Message": "denied"}}
            super().__init__(error_code)

    return _ClientError(code)


# --- Safe parsing of untrusted output (never raises) -----------------------


def test_extract_suggestion_text_handles_malformed_bodies_without_raising() -> None:
    # Each of these untrusted bodies must yield "" rather than raising.
    assert _extract_suggestion_text({}) == ""
    assert _extract_suggestion_text({"body": None}) == ""
    assert _extract_suggestion_text({"body": b"not json"}) == ""
    assert _extract_suggestion_text({"body": "{not valid json"}) == ""
    assert _extract_suggestion_text({"body": json.dumps([1, 2, 3])}) == ""
    assert _extract_suggestion_text({"body": json.dumps({"content": "x"})}) == ""
    assert _extract_suggestion_text({"body": json.dumps({"other": 1})}) == ""


def test_extract_suggestion_text_ignores_non_text_blocks() -> None:
    payload = json.dumps(
        {
            "content": [
                {"type": "tool_use", "id": "abc"},
                {"type": "text", "text": "keep me"},
                {"type": "text"},  # missing text
            ]
        }
    )
    assert _extract_suggestion_text({"body": payload}) == "keep me"


def test_extract_suggestion_text_caps_oversized_output() -> None:
    huge = "A" * (MAX_SUGGESTION_CHARS + 5_000)
    payload = json.dumps({"content": [{"type": "text", "text": huge}]})
    extracted = _extract_suggestion_text({"body": payload})
    assert len(extracted) == MAX_SUGGESTION_CHARS


def test_invoke_maps_unparseable_output_to_invalid_output() -> None:
    client = _RawBodyBedrockRuntimeClient(b"not json at all")
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert outcome.available is False
    assert outcome.reason == "INVALID_OUTPUT"
    assert outcome.suggestion is None
    assert "deterministic" in outcome.detail.lower()


# --- Forbidden-statement validation (Req 11.8) -----------------------------


def test_validate_suggested_sql_allows_object_ddl_and_expressions() -> None:
    assert validate_suggested_sql("CREATE TABLE t (id uuid PRIMARY KEY)").is_safe
    assert validate_suggested_sql(
        "CREATE TABLE t (id uuid PRIMARY KEY); CREATE INDEX ASYNC i ON t (id)"
    ).is_safe
    assert validate_suggested_sql(
        "CREATE FUNCTION f() RETURNS int LANGUAGE SQL AS $$ SELECT 1 $$"
    ).is_safe
    # A data-transformation expression carries no statement keyword.
    assert validate_suggested_sql("value::boolean").is_safe
    assert validate_suggested_sql("").is_safe


def test_validate_suggested_sql_blocks_forbidden_statements() -> None:
    for forbidden in (
        "DROP DATABASE prod",
        "DROP TABLE orders",
        "DELETE FROM orders",
        "UPDATE orders SET x = 1",
        "TRUNCATE orders",
        "GRANT ALL ON orders TO public",
        "REVOKE ALL ON orders FROM public",
        "CREATE ROLE attacker",
        "ALTER SYSTEM SET x = 1",
        "COPY orders TO '/tmp/x'",
        "COMMIT",
        "DO $$ BEGIN PERFORM 1; END $$",
    ):
        result = validate_suggested_sql(forbidden)
        assert result.is_safe is False, forbidden
        assert result.reason


def test_validate_suggested_sql_blocks_statement_injection() -> None:
    # A dangerous statement appended after a benign one is still caught.
    injected = "CREATE TABLE t (id int PRIMARY KEY); DROP DATABASE prod"
    assert validate_suggested_sql(injected).is_safe is False


def test_validate_suggested_sql_ignores_commented_out_dangerous_statements() -> None:
    # A dangerous statement that only appears inside a comment is not executed,
    # so it must not be flagged (comments are stripped before checking).
    assert validate_suggested_sql(
        "/* DROP DATABASE p */ CREATE TABLE t (id int PRIMARY KEY)"
    ).is_safe
    assert validate_suggested_sql(
        "CREATE TABLE t (id int PRIMARY KEY) -- DROP DATABASE p"
    ).is_safe


def test_suggest_schema_conversion_flags_forbidden_output_as_rejected() -> None:
    # Untrusted model output containing a forbidden statement must come back
    # flagged REJECTED (not produced as an APPLY-ready suggestion).
    client = _FakeBedrockRuntimeClient("DROP DATABASE prod; CREATE TABLE t (id int)")
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    suggestion = assistant.suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert suggestion.status == "REJECTED"
    assert suggestion.approved_by_user is False
    assert "forbidden" in suggestion.rationale.lower()


# --- Graceful degradation on Bedrock errors (Req 11.10) --------------------


def test_try_suggest_schema_conversion_degrades_on_access_denied() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("AccessDeniedException"))
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert outcome.available is False
    assert outcome.reason == "ACCESS_DENIED"
    assert outcome.suggestion is None
    # Clear, actionable message that keeps the deterministic result/flag.
    assert "deterministic" in outcome.detail.lower()
    assert "bedrock:InvokeModel".lower() in outcome.detail.lower()


def test_try_suggest_schema_conversion_degrades_on_throttling() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("ThrottlingException"))
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert outcome.available is False
    assert outcome.reason == "THROTTLED"


def test_try_suggest_schema_conversion_degrades_on_network_error() -> None:
    class EndpointConnectionError(Exception):
        """Mimics botocore's connectivity error by class name."""

    client = _RaisingBedrockRuntimeClient(EndpointConnectionError("unreachable"))
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_data_transformation(
        object_name="users.is_active",
        source_type="TINYINT(1)",
        sample_values=["0", "1"],
        deterministic_mapping="MANUAL",
    )

    assert outcome.available is False
    assert outcome.reason == "NETWORK"


def test_try_suggest_schema_conversion_degrades_on_unknown_error() -> None:
    client = _RaisingBedrockRuntimeClient(ValueError("boom"))
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert outcome.available is False
    assert outcome.reason == "UNAVAILABLE"


def test_try_suggest_schema_conversion_returns_ok_on_success() -> None:
    client = _FakeBedrockRuntimeClient("CREATE TABLE t (id uuid PRIMARY KEY)")
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert outcome.available is True
    assert outcome.reason == "OK"
    assert outcome.suggestion is not None
    assert outcome.suggestion.status == "PENDING_REVIEW"


def test_suggest_schema_conversion_raises_typed_error_on_failure() -> None:
    # The raising form surfaces a typed, catchable signal (the existing UI and
    # the try_* wrappers both handle it) rather than a raw boto exception.
    client = _RaisingBedrockRuntimeClient(_client_error("ThrottlingException"))
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    with pytest.raises(AiAssistUnavailableError) as excinfo:
        assistant.suggest_schema_conversion(
            object_name="orders",
            source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
            deterministic_result="MANUAL",
            dsql_constraints="primary key required",
        )
    assert excinfo.value.reason == "THROTTLED"


def test_degradation_detail_never_leaks_credentials() -> None:
    # The classifier must never echo the raw exception text (Property 7).
    secret = "AKIA_SECRET_SHOULD_NOT_LEAK"
    client = _RaisingBedrockRuntimeClient(ValueError(f"creds={secret}"))
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    outcome = assistant.try_suggest_schema_conversion(
        object_name="orders",
        source_ddl="CREATE TABLE `orders` (`id` INT PRIMARY KEY)",
        deterministic_result="MANUAL",
        dsql_constraints="primary key required",
    )

    assert secret not in outcome.detail


# --- Apply gate: refuse unapproved and forbidden suggestions (Property 13) --


def test_apply_gate_refuses_unapproved_and_passes_only_approved_safe() -> None:
    from dsql_migrator.ui.ai_assist import approve_suggestion, is_approved
    from dsql_migrator.ui.schema_conversion import build_ai_apply_objects

    pending = AiConversionSuggestion(
        object_name="pending",
        kind="SCHEMA",
        suggested_sql_or_expr="CREATE TABLE pending (id int PRIMARY KEY)",
        model_id="us.anthropic.claude-sonnet-4-6",
    )
    approved = approve_suggestion(
        AiConversionSuggestion(
            object_name="approved",
            kind="SCHEMA",
            suggested_sql_or_expr="CREATE TABLE approved (id int PRIMARY KEY)",
            model_id="us.anthropic.claude-sonnet-4-6",
        )
    )

    assert is_approved(pending) is False
    assert is_approved(approved) is True

    objects = build_ai_apply_objects([pending, approved])
    assert [obj.object_name for obj in objects] == ["approved"]


def test_apply_gate_refuses_approved_but_forbidden_suggestion() -> None:
    # Even an explicitly approved suggestion is refused if its (untrusted) text
    # contains a forbidden statement, so it is never applied (Req 11.8).
    from dsql_migrator.ui.ai_assist import approve_suggestion
    from dsql_migrator.ui.schema_conversion import build_ai_apply_objects

    dangerous = approve_suggestion(
        AiConversionSuggestion(
            object_name="dangerous",
            kind="SCHEMA",
            suggested_sql_or_expr="DROP DATABASE prod",
            model_id="us.anthropic.claude-sonnet-4-6",
            status="APPROVED",
            approved_by_user=True,
        )
    )

    assert build_ai_apply_objects([dangerous]) == []


# ---------------------------------------------------------------------------
# Task 17.3: AiAccessCheckResult model + verify_access() preflight
# ---------------------------------------------------------------------------


def test_ai_access_check_result_serialization_round_trip() -> None:
    result = AiAccessCheckResult(
        ok=False,
        reason="MODEL_NOT_ENABLED",
        detail="enable the model in the configured region",
        model_id="anthropic.claude-test",
        region="us-east-1",
    )
    restored = AiAccessCheckResult.model_validate(result.model_dump())
    assert restored == result


def test_ai_access_check_result_allows_region_none_and_defaults_detail() -> None:
    result = AiAccessCheckResult(
        ok=True, reason="OK", model_id="anthropic.claude-test", region=None
    )
    assert result.detail == ""
    assert result.region is None


def test_ai_access_check_result_rejects_invalid_reason() -> None:
    with pytest.raises(ValidationError):
        AiAccessCheckResult(
            ok=False,
            reason="NETWORK",  # not part of the verify_access taxonomy
            model_id="anthropic.claude-test",
            region=None,
        )


def test_ai_access_check_result_rejects_blank_model_id() -> None:
    with pytest.raises(ValidationError):
        AiAccessCheckResult(ok=True, reason="OK", model_id="", region=None)


def test_ai_access_check_result_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        AiAccessCheckResult(
            ok=True,
            reason="OK",
            model_id="anthropic.claude-test",
            region=None,
            unexpected="value",
        )


def test_verify_access_returns_ok_on_success() -> None:
    client = _FakeBedrockRuntimeClient("pong")
    config = AiAssistConfig(
        enabled=True, model_id="anthropic.claude-test", region="us-east-1"
    )
    assistant = AiConversionAssistant(config, client=client)

    result = assistant.verify_access()

    assert result.ok is True
    assert result.reason == "OK"
    # The result echoes the configuration the preflight ran against.
    assert result.model_id == "anthropic.claude-test"
    assert result.region == "us-east-1"
    assert result.detail
    # A single, least-cost InvokeModel was issued against the configured model.
    assert len(client.calls) == 1
    assert client.calls[0]["modelId"] == "anthropic.claude-test"
    body = json.loads(client.calls[0]["body"])
    assert body["max_tokens"] == 1


def test_verify_access_maps_access_denied() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("AccessDeniedException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "ACCESS_DENIED"
    assert "bedrock:InvokeModel".lower() in result.detail.lower()


def test_verify_access_maps_validation_exception_to_model_not_enabled() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("ValidationException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "MODEL_NOT_ENABLED"
    assert "enable" in result.detail.lower()


def test_verify_access_maps_resource_not_found_to_model_not_enabled() -> None:
    # A model-not-found / not-available style error is also MODEL_NOT_ENABLED.
    client = _RaisingBedrockRuntimeClient(_client_error("ResourceNotFoundException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "MODEL_NOT_ENABLED"


def test_verify_access_maps_throttling() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("ThrottlingException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "THROTTLED"
    assert "retry" in result.detail.lower()


def test_verify_access_maps_network_error_to_unknown() -> None:
    # Per the 17.3 taxonomy, network/other failures collapse to UNKNOWN.
    class EndpointConnectionError(Exception):
        """Mimics botocore's connectivity error by class name."""

    client = _RaisingBedrockRuntimeClient(EndpointConnectionError("unreachable"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "UNKNOWN"


def test_verify_access_maps_unexpected_error_to_unknown() -> None:
    client = _RaisingBedrockRuntimeClient(ValueError("boom"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region=None), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "UNKNOWN"
    assert result.region is None


def test_verify_access_never_raises_and_detail_is_credential_free() -> None:
    # Even on an unexpected failure whose text contains a secret, verify_access
    # must not raise and must never echo the raw exception text (Property 7).
    secret = "AKIA_SECRET_SHOULD_NOT_LEAK"
    client = _RaisingBedrockRuntimeClient(RuntimeError(f"creds={secret}"))
    config = AiAssistConfig(enabled=True, model_id="m", region="us-east-1")
    assistant = AiConversionAssistant(config, client=client)

    result = assistant.verify_access()

    assert result.ok is False
    assert result.detail
    assert secret not in result.detail
    # The configuration is echoed back regardless of the failure.
    assert result.model_id == "m"
    assert result.region == "us-east-1"



def test_split_sql_and_rationale_separates_code_from_prose() -> None:
    from dsql_migrator.core.ai_assistant import _split_sql_and_rationale

    text = (
        "## Analysis\nThe object is a VIEW.\n\n"
        "```sql\nCREATE VIEW v AS SELECT 1;\n```\n\n"
        "## Caveats\nViews are not materialized."
    )
    sql, rationale = _split_sql_and_rationale(text)
    assert sql == "CREATE VIEW v AS SELECT 1;"
    assert "Analysis" in rationale and "Caveats" in rationale
    assert "CREATE VIEW" not in rationale  # the SQL is not duplicated into prose


def test_split_sql_and_rationale_joins_multiple_sql_blocks() -> None:
    from dsql_migrator.core.ai_assistant import _split_sql_and_rationale

    text = (
        "Intro.\n```sql\nCREATE VIEW v AS SELECT 1;\n```\n"
        "Indexes:\n```sql\nCREATE INDEX ASYNC i ON t (c);\n```"
    )
    sql, _rationale = _split_sql_and_rationale(text)
    assert "CREATE VIEW v AS SELECT 1;" in sql
    assert "CREATE INDEX ASYNC i ON t (c);" in sql


def test_split_sql_and_rationale_no_fence_falls_back_to_raw() -> None:
    from dsql_migrator.core.ai_assistant import _split_sql_and_rationale

    sql, rationale = _split_sql_and_rationale("CREATE VIEW v AS SELECT 1;")
    assert sql == "CREATE VIEW v AS SELECT 1;"
    assert rationale == ""
