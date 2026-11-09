# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for AI-assist data models, config loading, and the Bedrock seam.

Covers:

- ``AiAssistConfig`` serialization round-trips, defaults, and validation
  (Requirements 11.1-11.4).
- Loading ``BEDROCK_MODEL_ID`` / ``BEDROCK_REGION`` into an ``AiAssistConfig``
  (default model id, region, opt-in disabled).
- Constructing the ``bedrock-runtime`` client from an injected fake session:
  the right region is forwarded, no credentials are passed (IAM-based auth), and
  building the client performs no network call.
- Untrusted-output parsing, the forbidden-statement validator, the typed
  Bedrock-failure classification (Requirements 11.8, 11.10), and the
  ``verify_access`` preflight (Requirements 11.13-11.16).

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
from dsql_migrator.core.ai_assistant import (  # internal: untrusted-output safety
    _classify_bedrock_error,
    _extract_suggestion_text,
)
from dsql_migrator.core.models import AiAccessCheckResult, AiAssistConfig


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
    assert config.model_id == "global.anthropic.claude-sonnet-5"
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
# load_ai_assist_config (BEDROCK_MODEL_ID / BEDROCK_REGION)
# ---------------------------------------------------------------------------


def test_load_ai_assist_config_uses_defaults_when_env_empty() -> None:
    config = load_ai_assist_config(env={})
    assert config.enabled is False
    assert config.model_id == "global.anthropic.claude-sonnet-5"
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
    assert config.model_id == "global.anthropic.claude-sonnet-5"
    assert config.region is None


def test_load_ai_assist_config_keys_are_not_prefixed() -> None:
    # The keys are exactly BEDROCK_MODEL_ID / BEDROCK_REGION (no DSQL_MIGRATOR_).
    assert ENV_BEDROCK_MODEL_ID == "BEDROCK_MODEL_ID"
    assert ENV_BEDROCK_REGION == "BEDROCK_REGION"
    env = {"DSQL_MIGRATOR_BEDROCK_MODEL_ID": "ignored"}
    config = load_ai_assist_config(env=env)
    assert config.model_id == "global.anthropic.claude-sonnet-5"


# ---------------------------------------------------------------------------
# build_bedrock_runtime_client (injected session seam, no AWS calls)
# ---------------------------------------------------------------------------


def test_build_bedrock_runtime_client_uses_injected_session_and_region() -> None:
    session = _FakeSession()
    config = AiAssistConfig(enabled=True, region="eu-central-1")

    client = build_bedrock_runtime_client(config, session=session)

    assert isinstance(client, _FakeClient)
    assert len(session.calls) == 1
    service, kwargs = session.calls[0]
    assert service == BEDROCK_RUNTIME_SERVICE
    assert kwargs["region_name"] == "eu-central-1"
    # Bounded connect/read timeouts so a hung Bedrock socket can't spin forever.
    assert kwargs["config"].connect_timeout == 10
    assert kwargs["config"].read_timeout == 60


def test_build_bedrock_runtime_client_omits_region_when_unset() -> None:
    session = _FakeSession()
    config = AiAssistConfig(enabled=True, region=None)

    build_bedrock_runtime_client(config, session=session)

    # When no region is configured, none is forwarded; the session decides.
    assert len(session.calls) == 1
    service, kwargs = session.calls[0]
    assert service == BEDROCK_RUNTIME_SERVICE
    assert "region_name" not in kwargs
    assert kwargs["config"].connect_timeout == 10
    assert kwargs["config"].read_timeout == 60


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
# AiConversionAssistant: fake bedrock-runtime client + lazy client build
# ---------------------------------------------------------------------------


class _FakeBedrockRuntimeClient:
    """A fake bedrock-runtime client recording InvokeModel calls.

    It returns a canned Anthropic-style response so the preflight never reaches
    AWS. Each call's kwargs are recorded so tests can assert which ``modelId``
    was used.
    """

    def __init__(self, suggestion_text: str) -> None:
        self._suggestion_text = suggestion_text
        self.calls: list[dict[str, Any]] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        payload = {"content": [{"type": "text", "text": self._suggestion_text}]}
        return {"body": json.dumps(payload).encode("utf-8")}


def test_assistant_builds_client_lazily_when_none_injected(monkeypatch: Any) -> None:
    # With no client injected, the assistant builds one lazily on first use via
    # the module's default-session factory. Patching that factory to return a
    # fake session keeps the test offline and proves construction itself does no
    # AWS work (the factory is only touched on the first verify_access call).
    fake_client = _FakeBedrockRuntimeClient("pong")

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

    result = assistant.verify_access()

    # The lazy client was built from the configured region and used for InvokeModel.
    assert len(session.calls) == 1
    service, kwargs = session.calls[0]
    assert service == BEDROCK_RUNTIME_SERVICE
    assert kwargs["region_name"] == "us-east-1"
    assert kwargs["config"].read_timeout == 60
    assert len(fake_client.calls) == 1
    assert result.ok is True


# ---------------------------------------------------------------------------
# Untrusted-output handling + typed Bedrock-failure classification
# ---------------------------------------------------------------------------


class _RaisingBedrockRuntimeClient:
    """A fake bedrock-runtime client whose ``invoke_model`` always raises.

    Used to simulate Bedrock failures (permission/throttle/network/unknown) in
    the preflight without reaching AWS.
    """

    def __init__(self, exc: BaseException) -> None:
        self._exc = exc
        self.calls: list[dict[str, Any]] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        raise self._exc


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


# --- Graceful degradation on Bedrock errors (Req 11.10) --------------------


def test_bedrock_access_denied_maps_to_a_typed_actionable_error() -> None:
    error = AiAssistUnavailableError(
        _classify_bedrock_error(_client_error("AccessDeniedException"))
    )

    assert error.reason == "ACCESS_DENIED"
    # Clear, actionable message: the deterministic workflow is unaffected, and the grant
    # to add.
    assert "deterministic" in error.detail.lower()
    assert "bedrock:InvokeModel".lower() in error.detail.lower()


def test_bedrock_throttling_maps_to_throttled() -> None:
    assert _classify_bedrock_error(_client_error("ThrottlingException")) == "THROTTLED"


def test_bedrock_network_error_maps_to_network() -> None:
    class EndpointConnectionError(Exception):
        """Mimics botocore's connectivity error by class name."""

    assert _classify_bedrock_error(EndpointConnectionError("unreachable")) == "NETWORK"


def test_bedrock_unknown_error_maps_to_unavailable() -> None:
    assert _classify_bedrock_error(ValueError("boom")) == "UNAVAILABLE"


def test_degradation_detail_never_leaks_credentials() -> None:
    # The classifier must never echo the raw exception text (Property 7).
    secret = "AKIA_SECRET_SHOULD_NOT_LEAK"
    error = AiAssistUnavailableError(_classify_bedrock_error(ValueError(f"creds={secret}")))

    assert secret not in error.detail
    assert secret not in str(error)


def test_every_unavailable_copy_describes_the_ai_dba_not_the_deleted_suggestion_flow() -> None:
    """The fixed copy is the body of the AI DBA panel's "AI reply unavailable" notice, and a
    latched one is also the composer hint, the "What's next?" toast and the Connect screen's
    AI status. It described the suggestion pipeline X1 deleted -- "the deterministic
    conversion result and its MANUAL/UNSUPPORTED flag are kept", "to enable AI
    suggestions", "Re-run", "the deterministic result above" -- none of which a chat has."""
    from dsql_migrator.core.ai_assistant import _UNAVAILABLE_DETAILS
    from dsql_migrator.ui.ai_assist import AI_LATCHING_REASONS

    for reason, detail in _UNAVAILABLE_DETAILS.items():
        assert "The deterministic workflow is unaffected." in detail, reason
        for stale in (
            "MANUAL/UNSUPPORTED",
            "suggestion",
            "conversion result",
            "result above",
            "re-run",
            "could not be parsed",
        ):
            assert stale.casefold() not in detail.casefold(), (reason, stale)
        if reason in AI_LATCHING_REASONS:
            # Latched, it is also shown off the Connect screen: say where the button is.
            assert "Verify AI access on the Connect screen" in detail, reason


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


def test_verify_access_sends_the_access_check_prompt_as_one_user_text_turn() -> None:
    """The request body ``_build_invoke_body`` builds, through its one live caller.

    Verify AI access sends an Anthropic messages-API body: one user turn whose one content
    block is text carrying the access-check prompt, capped at one output token. Nothing else
    checked this shape once the suggestion calls went (X1). An empty or misplaced prompt is
    an invalid request -- a ValidationException, which the preflight reports as
    MODEL_NOT_ENABLED, sending the operator after the wrong fix.
    """
    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_PROMPT

    client = _FakeBedrockRuntimeClient("pong")
    config = AiAssistConfig(
        enabled=True, model_id="anthropic.claude-test", region="us-east-1"
    )

    assert AiConversionAssistant(config, client=client).verify_access().ok is True

    [call] = client.calls
    body = json.loads(call["body"])
    assert body["max_tokens"] == 1
    [message] = body["messages"]
    assert message["role"] == "user"
    [block] = message["content"]
    assert block["type"] == "text"
    assert block["text"] == _ACCESS_CHECK_PROMPT
    assert block["text"].strip()


class _PerModelBedrockRuntimeClient:
    """A fake client that succeeds only for an allowlisted set of model ids.

    Models outside ``enabled`` raise a model-not-enabled error, mirroring an account
    that can see a `global.` inference profile as ACTIVE but has not been granted
    access to invoke it -- the exact case the preflight fallback exists for.
    """

    def __init__(self, enabled: set[str]) -> None:
        self._enabled = enabled
        self.calls: list[dict[str, Any]] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if kwargs["modelId"] not in self._enabled:
            raise _client_error("ValidationException")
        return _FakeBedrockRuntimeClient("pong").invoke_model(**kwargs)

    @property
    def model_ids(self) -> list[str]:
        return [call["modelId"] for call in self.calls]


def test_verify_access_falls_back_when_the_configured_model_is_not_enabled() -> None:
    """The reported gap: a preflight that dead-ends when the default isn't enabled.

    A `global.` profile being ACTIVE in a region does not mean this account may invoke
    it -- model access is per account. Rather than reporting only failure, the check
    retries the fallback and reports which model actually answered.
    """
    from dsql_migrator.ui.ai_assist import (
        BEDROCK_MODEL_FALLBACKS,
        DEFAULT_BEDROCK_MODEL_ID,
    )

    fallback = BEDROCK_MODEL_FALLBACKS[0]
    client = _PerModelBedrockRuntimeClient(enabled={fallback})
    assistant = AiConversionAssistant(
        AiAssistConfig(
            enabled=True, model_id=DEFAULT_BEDROCK_MODEL_ID, region="ap-northeast-2"
        ),
        client=client,
    )

    result = assistant.verify_access()

    assert result.ok is True
    # model_id echoes what ANSWERED, not what was configured -- otherwise the screen
    # would name a model the suggestions cannot come from.
    assert result.model_id == fallback
    assert client.model_ids == [DEFAULT_BEDROCK_MODEL_ID, fallback]
    # The substitution is disclosed, naming both models and how to restore the choice.
    assert fallback in result.detail
    assert DEFAULT_BEDROCK_MODEL_ID in result.detail
    assert "not enabled" in result.detail


def test_verify_access_falls_back_when_the_configured_model_is_account_denied() -> None:
    """A model the ACCOUNT cannot invoke must reach the fallback chain.

    Reproduces the measured failure: Bedrock answers AccessDeniedException -- not
    ValidationException -- for a model this account has no access to, so keying the
    fallback on MODEL_NOT_ENABLED alone made the chain unreachable in the one case it
    exists for, and the operator was told to fix an IAM policy that was fine.
    """
    from dsql_migrator.ui.ai_assist import (
        BEDROCK_MODEL_FALLBACKS,
        DEFAULT_BEDROCK_MODEL_ID,
    )

    fallback = BEDROCK_MODEL_FALLBACKS[0]

    class _AccountDeniedClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []
            self.model_ids: list[str] = []

        def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            self.model_ids.append(kwargs["modelId"])
            if kwargs["modelId"] == fallback:
                return _FakeBedrockRuntimeClient("pong").invoke_model(**kwargs)
            raise _client_error("AccessDeniedException")

    client = _AccountDeniedClient()
    assistant = AiConversionAssistant(
        AiAssistConfig(
            enabled=True, model_id=DEFAULT_BEDROCK_MODEL_ID, region="ap-northeast-2"
        ),
        client=client,
    )

    result = assistant.verify_access()

    assert result.ok is True
    assert result.model_id == fallback
    assert client.model_ids == [DEFAULT_BEDROCK_MODEL_ID, fallback]
    # The denial variant names both models AND both remediations (model access OR the
    # deployment's InvokeModel scope) -- the not-enabled wording names only one.
    assert fallback in result.detail
    assert DEFAULT_BEDROCK_MODEL_ID in result.detail
    assert "not authorized to invoke" in result.detail
    assert "bedrock:InvokeModel" in result.detail
    assert "not enabled" not in result.detail


def test_verify_access_reports_the_configured_reason_when_the_fallback_is_denied_too(
) -> None:
    # Both denied: the verdict is still the configured model's, with the IAM/re-auth
    # remediation. One extra minimal InvokeModel is the deliberate cost of telling an
    # account-level model denial apart from a real permission gap.
    client = _RaisingBedrockRuntimeClient(_client_error("AccessDeniedException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "ACCESS_DENIED"
    assert result.model_id == assistant._config.model_id
    assert "bedrock:InvokeModel" in result.detail
    assert len(client.calls) == 2, "the fallback is probed exactly once"


def test_verify_access_does_not_probe_a_fallback_on_expired_credentials() -> None:
    # A credential-identity failure is NOT fixable by another model, so it must return
    # immediately. This is what keeps the code-only split honest: only the
    # authorization-ambiguous denial codes reach the chain.
    client = _RaisingBedrockRuntimeClient(_client_error("ExpiredTokenException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "ACCESS_DENIED"
    assert len(client.calls) == 1, "must not retry a fallback on an expired credential"


def test_verify_access_reports_the_configured_models_reason_not_the_fallbacks() -> None:
    """A fallback's own failure must not be misreported as the operator's problem.

    Configured model not enabled (retryable) but the fallback is IAM-denied: the
    verdict has to name the configured model and its reason, or the operator chases
    an IAM gap on a model they never chose.
    """
    from dsql_migrator.ui.ai_assist import (
        BEDROCK_MODEL_FALLBACKS,
        DEFAULT_BEDROCK_MODEL_ID,
    )

    fallback = BEDROCK_MODEL_FALLBACKS[0]

    class _MixedClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(kwargs)
            if kwargs["modelId"] == fallback:
                raise _client_error("AccessDeniedException")
            raise _client_error("ValidationException")

    client = _MixedClient()
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, model_id=DEFAULT_BEDROCK_MODEL_ID),
        client=client,
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "MODEL_NOT_ENABLED"
    assert result.model_id == DEFAULT_BEDROCK_MODEL_ID
    assert fallback not in result.detail


def test_verify_access_never_probes_the_same_model_twice() -> None:
    # Configuring the fallback explicitly must not cost two identical InvokeModel
    # round trips -- the preflight is meant to be the cheapest possible check.
    from dsql_migrator.ui.ai_assist import BEDROCK_MODEL_FALLBACKS

    fallback = BEDROCK_MODEL_FALLBACKS[0]
    client = _PerModelBedrockRuntimeClient(enabled=set())
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, model_id=fallback), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert client.model_ids == [fallback]


def test_verify_access_reports_failure_when_every_candidate_is_unavailable() -> None:
    # Exhausting the chain must still produce the actionable MODEL_NOT_ENABLED
    # verdict against the configured model, not a fallback-flavoured one.
    from dsql_migrator.ui.ai_assist import DEFAULT_BEDROCK_MODEL_ID

    client = _PerModelBedrockRuntimeClient(enabled=set())
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, model_id=DEFAULT_BEDROCK_MODEL_ID), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "MODEL_NOT_ENABLED"
    assert result.model_id == DEFAULT_BEDROCK_MODEL_ID


def test_fallback_model_is_an_accepted_deploy_parameter_value() -> None:
    """The fallback must be deployable, or it fails on IAM rather than model access.

    The task role's bedrock:InvokeModel scope is derived from BedrockModelId's
    AllowedValues, so a fallback outside that list would be denied by IAM in a real
    deploy even though the preflight chose it.
    """
    import pathlib

    import yaml

    deploy_dir = pathlib.Path(__file__).resolve().parents[1] / "deploy"
    template = yaml.safe_load(
        (deploy_dir / "cloudformation.yaml").read_text(encoding="utf-8")
    )
    allowed = template["Parameters"]["BedrockModelId"]["AllowedValues"]

    from dsql_migrator.ui.ai_assist import (
        BEDROCK_MODEL_FALLBACKS,
        DEFAULT_BEDROCK_MODEL_ID,
    )

    assert DEFAULT_BEDROCK_MODEL_ID in allowed
    for fallback in BEDROCK_MODEL_FALLBACKS:
        assert fallback in allowed, (
            f"fallback {fallback} is not an accepted BedrockModelId value, so the "
            "derived IAM scope would not cover it"
        )
    assert DEFAULT_BEDROCK_MODEL_ID not in BEDROCK_MODEL_FALLBACKS


def test_verify_access_maps_access_denied() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("AccessDeniedException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.ok is False
    assert result.reason == "ACCESS_DENIED"
    assert "bedrock:InvokeModel".lower() in result.detail.lower()


def test_expired_credentials_classified_as_access_denied_both_paths() -> None:
    # Regression: an expired/invalid-credentials error used to fall through to a
    # vague UNAVAILABLE (suggestion) / UNKNOWN (preflight) with an unhelpful
    # message. It now maps to ACCESS_DENIED on BOTH classifiers, whose message
    # tells the user to re-authenticate.
    from dsql_migrator.core.ai_assistant import (
        _classify_access_check_error,
        _classify_bedrock_error,
    )

    for code in (
        "ExpiredTokenException",
        "ExpiredToken",
        "InvalidSignatureException",
        "InvalidClientTokenId",
    ):
        exc = _client_error(code)
        assert _classify_bedrock_error(exc) == "ACCESS_DENIED"
        assert _classify_access_check_error(exc) == "ACCESS_DENIED"


def test_verify_access_expired_credentials_detail_mentions_reauth() -> None:
    client = _RaisingBedrockRuntimeClient(_client_error("ExpiredTokenException"))
    assistant = AiConversionAssistant(
        AiAssistConfig(enabled=True, region="us-east-1"), client=client
    )

    result = assistant.verify_access()

    assert result.reason == "ACCESS_DENIED"
    assert "re-authenticate" in result.detail.lower()


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
