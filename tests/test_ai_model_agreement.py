# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G18: a Bedrock model-agreement (AWS Marketplace) denial is MODEL_AGREEMENT_REQUIRED.

Amazon Bedrock denies a third-party model whose AWS Marketplace subscription is missing
or failed with the SAME error code as a missing IAM grant (``AccessDeniedException``),
and in a new account the model's first calls can even succeed while that subscription is
still being attempted in the background. Classifying that denial as ACCESS_DENIED told
the operator to add a ``bedrock:InvokeModel`` permission -- the wrong fix -- and the
preflight's fallback chain could then "pass" on another model inside its own grace
window.

These tests pin the fix:

- a denial in ``_MODEL_ACCESS_DENIED_CODES`` whose structured message carries the fixed
  "aws marketplace" marker maps to MODEL_AGREEMENT_REQUIRED on BOTH classifiers, with
  fixed copy naming the real remedy (accept the model's agreement once);
- the message is only ever presence-tested: it never reaches a detail, an exception, a
  model dump or a log (Requirement 11.15 / Property 7);
- credential-identity errors and marker-less denials keep today's reason and copy;
- the preflight stops on the denial instead of probing a fallback;
- every passing preflight carries the first-call caveat;
- every reason taxonomy stays complete (a missing entry would raise inside an
  ``except`` block and chain the original message into a log).

Offline only: real botocore ``ClientError`` objects and fake clients -- never AWS.
"""

from __future__ import annotations

import json
import logging
import typing
from typing import Any

import pytest
from botocore.exceptions import ClientError

from dsql_migrator.core import ai_assistant as aa
from dsql_migrator.core.ai_assistant import (
    AiAssistUnavailableError,
    AiConversionAssistant,
    _classify_access_check_error,
    _classify_bedrock_error,
)
from dsql_migrator.core.models import AiAccessCheckResult, AiAssistConfig
from dsql_migrator.ui.ai_assist import (
    BEDROCK_MODEL_FALLBACKS,
    DEFAULT_BEDROCK_MODEL_ID,
    map_access_check_display,
    run_verify_ai_access,
)

# A credential-shaped sentinel planted in every message: it must never surface anywhere.
SENTINEL = "arn:aws:sts::123456789012:assumed-role/WSParticipantRole/g18-SENTINEL"
# The denial the Workshop Studio reviewer saw, with the sentinel planted at BOTH ends so
# a truncated leak -- a prefix (``message[:60]``, ``str(exc)[:200]``) or a suffix -- still
# carries it; the "AWS Marketplace" marker stays mid-message.
MARKETPLACE_MSG = (
    SENTINEL
    + " Model access is denied due to IAM user or service role is not authorized to "
    "perform the required AWS Marketplace actions (aws-marketplace:ViewSubscriptions, "
    "aws-marketplace:Subscribe) to enable access to this model. Refer to the Amazon "
    "Bedrock User Guide for further details. Your AWS Marketplace subscription for this "
    "model cannot be completed at this time. " + SENTINEL
)
# The phrase the docs quote for this denial (manual chapter 2, DEPLOYMENT troubleshooting):
# it must appear word for word in BOTH fixed copies, so the AI DBA panel's sentence and
# the Verify AI access result can each be matched to the docs.
DOC_PHRASE = "the configured model is not enabled for this AWS account yet"
# An ordinary IAM denial whose ROLE and SESSION names contain "Marketplace": it must not
# trip the marker (an ARN / principal name can never contain the marker's space).
PLAIN_IAM_MSG = (
    "User: arn:aws:sts::123456789012:assumed-role/MarketplaceOps/aws-marketplace-session "
    "is not authorized to perform: bedrock:InvokeModel on resource: "
    "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-sonnet-4-6 because no "
    "identity-based policy allows the bedrock:InvokeModel action"
)

_OK_BODY = {"body": json.dumps({"content": [{"type": "text", "text": "p"}]})}


def _ce(code: str, msg: str, op: str = "InvokeModel") -> ClientError:
    """A REAL botocore ClientError in the parsed rest-json shape bedrock-runtime uses."""
    return ClientError({"Error": {"Code": code, "Message": msg}}, op)


# The credential-identity codes: in the denial bucket but NOT authorization-ambiguous.
_CREDENTIAL_CODES = sorted(aa._ACCESS_DENIED_CODES - aa._MODEL_ACCESS_DENIED_CODES)


class _RaisingClient:
    """A fake bedrock-runtime client whose every call raises ``exc``."""

    def __init__(self, exc: BaseException) -> None:
        self.exc = exc
        self.model_ids: list[str] = []

    def invoke_model(self, **kwargs: Any) -> dict:
        self.model_ids.append(kwargs["modelId"])
        raise self.exc

    def invoke_model_with_response_stream(self, **kwargs: Any) -> dict:
        self.model_ids.append(kwargs["modelId"])
        raise self.exc


class _ScriptedClient:
    """A fake client answering per model id: an exception to raise, or a pass."""

    def __init__(self, outcomes: dict[str, BaseException | None]) -> None:
        self.outcomes = outcomes
        self.model_ids: list[str] = []

    def invoke_model(self, **kwargs: Any) -> dict:
        model_id = kwargs["modelId"]
        self.model_ids.append(model_id)
        exc = self.outcomes[model_id]
        if exc is not None:
            raise exc
        return dict(_OK_BODY)


def _verify(client: Any, model_id: str = DEFAULT_BEDROCK_MODEL_ID) -> AiAccessCheckResult:
    config = AiAssistConfig(enabled=True, model_id=model_id, region="us-east-1")
    return AiConversionAssistant(config, client=client).verify_access()


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------


def test_marketplace_denial_maps_to_model_agreement_required_on_both_classifiers() -> None:
    exc = _ce("AccessDeniedException", MARKETPLACE_MSG)
    assert _classify_bedrock_error(exc) == "MODEL_AGREEMENT_REQUIRED"
    assert _classify_access_check_error(exc) == "MODEL_AGREEMENT_REQUIRED"


@pytest.mark.parametrize("code", sorted(aa._MODEL_ACCESS_DENIED_CODES))
@pytest.mark.parametrize(
    "msg",
    [
        "... the required AWS MARKETPLACE actions ...",
        "aws marketplace subscription failed",
        "the required AWS\xa0Marketplace actions",
        "the required AWS\n  Marketplace actions",
    ],
)
def test_marker_is_case_and_whitespace_insensitive(code: str, msg: str) -> None:
    exc = _ce(code, msg)
    assert _classify_bedrock_error(exc) == "MODEL_AGREEMENT_REQUIRED"
    assert _classify_access_check_error(exc) == "MODEL_AGREEMENT_REQUIRED"


@pytest.mark.parametrize("code", _CREDENTIAL_CODES)
def test_credential_codes_with_the_marker_stay_access_denied(code: str) -> None:
    # An expired token / bad signature is about the caller's identity, never about the
    # model: its classification stays code-only even if the message names Marketplace,
    # and the preflight still returns after ONE call (no fallback probe).
    exc = _ce(code, MARKETPLACE_MSG)
    assert _classify_bedrock_error(exc) == "ACCESS_DENIED"
    assert _classify_access_check_error(exc) == "ACCESS_DENIED"

    client = _RaisingClient(exc)
    result = _verify(client)
    assert (result.ok, result.reason) == (False, "ACCESS_DENIED")
    assert len(client.model_ids) == 1


@pytest.mark.parametrize(
    "msg",
    [
        PLAIN_IAM_MSG,
        "User: arn:aws:sts::123456789012:assumed-role/r/s is not authorized to "
        "perform: aws-marketplace:Subscribe",
        "You don't have access to the model with the specified model ID.",
        "denied",
    ],
)
def test_plain_denials_keep_access_denied_and_their_copy(msg: str) -> None:
    exc = _ce("AccessDeniedException", msg)
    reason = _classify_bedrock_error(exc)
    assert reason == "ACCESS_DENIED"
    plain_copy = aa._UNAVAILABLE_DETAILS["ACCESS_DENIED"]
    assert AiAssistUnavailableError(reason).detail == plain_copy
    preflight = _classify_access_check_error(exc)
    assert preflight == "ACCESS_DENIED"
    assert "bedrock:InvokeModel" in aa._ACCESS_CHECK_DETAILS[preflight]


class _ShapedError(Exception):
    """An exception carrying an arbitrary ``response`` (botocore-like or not)."""

    def __init__(self, response: Any, text: str = "shaped") -> None:
        super().__init__(text)
        self.response = response


class _RaisingResponseError(Exception):
    """An exception whose ``response`` accessor itself raises (with the sentinel)."""

    @property
    def response(self) -> Any:
        raise RuntimeError(SENTINEL)


def _denied(**error: Any) -> _ShapedError:
    return _ShapedError({"Error": {"Code": "AccessDeniedException", **error}})


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("no response"),
        _ShapedError(None),
        _ShapedError(["not", "a", "mapping"]),
        _ShapedError({"Error": "x"}),
        _denied(),  # Message missing
        _denied(Message=None),
        _denied(Message=42),
        _denied(Message=b"AWS Marketplace"),
        _RaisingResponseError("raising accessor"),
        # str(exc) names AWS Marketplace, the structured message does not: only the
        # structured field is ever read.
        _ShapedError(
            {"Error": {"Code": "AccessDeniedException", "Message": "denied"}},
            "the required AWS Marketplace actions",
        ),
    ],
)
def test_marker_check_is_total_and_reads_only_the_structured_message(
    exc: Exception,
) -> None:
    assert aa._has_model_agreement_marker(exc) is False
    assert _classify_bedrock_error(exc) in typing.get_args(aa.UnavailableReason)
    assert _classify_access_check_error(exc) in typing.get_args(aa.AccessCheckReason)


def test_error_code_survives_a_raising_response_accessor() -> None:
    # Both classifiers run inside ``except`` blocks: an exception escaping here would
    # chain the ORIGINAL error (and its message) into a log.
    assert aa._error_code(_RaisingResponseError("raising accessor")) == ""


# ---------------------------------------------------------------------------
# Fixed copy -- the right remedy, and never the message
# ---------------------------------------------------------------------------


def test_agreement_copy_names_the_right_remedy_and_not_the_wrong_one() -> None:
    chat = aa._UNAVAILABLE_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    preflight = aa._ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    for detail in (chat, preflight):
        for needle in (
            "not enabled for this AWS account yet",
            "AWS Marketplace",
            "agreement",
            "list-foundation-model-agreement-offers",
            "create-foundation-model-agreement",
            "<foundation-model-id>",
            "first-time use form",
            "payment method",
            "2 minutes",
            "No IAM change",
        ):
            assert needle in detail, needle
        # The wrong remedies for this denial, and no unformatted template braces.
        for absent in ("bedrock:InvokeModel", "BedrockModelArns", "{", "}"):
            assert absent not in detail, absent
        # Both copies are also shown OFF the Connect screen (a latched detail reaches the
        # AI DBA composer hint and the "What's next?" toast), so the retry step says
        # where the button is.
        assert "Verify AI access on the Connect screen" in detail
    # A plain denial keeps its own, unchanged remedy.
    assert "Grant a scoped bedrock:InvokeModel permission" in aa._UNAVAILABLE_DETAILS[
        "ACCESS_DENIED"
    ]
    assert "Add a scoped bedrock:InvokeModel permission" in aa._ACCESS_CHECK_DETAILS[
        "ACCESS_DENIED"
    ]


def test_both_agreement_copies_carry_the_phrase_the_docs_quote() -> None:
    # The docs quote one sentence for this denial -- as what the AI DBA panel and the
    # Connect status say, and as the troubleshooting row's symptom. The chat copy used to
    # read "the configured Amazon Bedrock model is not enabled ...", so a user pasting
    # the panel's sentence into a docs search found nothing.
    def flat(text: str) -> str:
        return " ".join(text.split()).casefold()

    for table in (aa._UNAVAILABLE_DETAILS, aa._ACCESS_CHECK_DETAILS):
        assert flat(DOC_PHRASE) in flat(table["MODEL_AGREEMENT_REQUIRED"])


def _assert_no_leak(*texts: str) -> None:
    # The bare account id too: it survives a truncation that cuts the ARN short, and
    # case-folding.
    for text in texts:
        assert SENTINEL not in text
        assert "123456789012" not in text


def test_every_path_maps_the_marketplace_denial_without_leaking_it(caplog) -> None:
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.cdc_assist import CdcAssistant, CdcReadinessSignals
    from dsql_migrator.core.models import AssessmentItem, Classification, EffortLevel

    caplog.set_level(logging.DEBUG)
    exc = _ce("AccessDeniedException", MARKETPLACE_MSG)
    chat_copy = aa._UNAVAILABLE_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    preflight_copy = aa._ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    cfg = AiAssistConfig(
        enabled=True, model_id=DEFAULT_BEDROCK_MODEL_ID, region="us-east-1"
    )

    error = AiAssistUnavailableError(_classify_bedrock_error(exc))
    assert (error.reason, error.detail) == ("MODEL_AGREEMENT_REQUIRED", chat_copy)
    _assert_no_leak(error.detail, str(error), repr(error.args))

    assistant = AiConversionAssistant(cfg, client=_RaisingClient(exc))
    item = AssessmentItem(
        object_name="orders",
        rule_id="FK_PRESERVED",
        classification=Classification.MANUAL,
        effort=EffortLevel.SIMPLE,
        kind="TABLE",
    )
    strategist = AssessmentStrategist(cfg, client=_RaisingClient(exc))
    outcomes = {
        "try_suggest_schema_conversion": assistant.try_suggest_schema_conversion(
            object_name="t",
            source_ddl="CREATE TABLE t (id INT PRIMARY KEY)",
            deterministic_result="MANUAL",
            dsql_constraints="PK required",
        ),
        "try_suggest_data_transformation": assistant.try_suggest_data_transformation(
            object_name="t.c",
            source_type="SET('a','b')",
            sample_values=["a"],
            deterministic_mapping="MANUAL",
        ),
        "stream_chat": strategist.stream_chat(
            "SYS", [{"role": "user", "text": "hi"}], lambda _delta: None
        ),
        "tool_chat": strategist.tool_chat(
            "SYS",
            [{"role": "user", "text": "hi"}],
            lambda _delta: None,
            tools=[],
            execute=lambda _name, _input: "{}",
        ),
        "try_generate_object_guidance": strategist.try_generate_object_guidance(item),
        "stream_object_guidance": strategist.stream_object_guidance(
            item, lambda _delta: None
        ),
        "CdcAssistant.try_readiness": CdcAssistant(
            cfg, client=_RaisingClient(exc)
        ).try_readiness(CdcReadinessSignals(), ["app.t"]),
    }
    for path, outcome in outcomes.items():
        assert outcome.available is False, path
        assert (outcome.reason, outcome.detail) == (
            "MODEL_AGREEMENT_REQUIRED",
            chat_copy,
        ), path
        _assert_no_leak(repr(outcome))

    def _raising_factory(_cfg: AiAssistConfig, _profile: object) -> Any:
        # Building the client itself fails (run_verify_ai_access's factory path).
        raise exc

    verified = AiConversionAssistant(cfg, client=_RaisingClient(exc)).verify_access()
    built = run_verify_ai_access(cfg, None, assistant_factory=_raising_factory)
    for result in (verified, built):
        assert (result.ok, result.reason, result.detail) == (
            False,
            "MODEL_AGREEMENT_REQUIRED",
            preflight_copy,
        )
        _assert_no_leak(result.model_dump_json(), repr(result))

    _assert_no_leak(caplog.text)
    # Nor any slice of the body itself, however truncated or redacted.
    for fragment in ("Model access is denied", "AWS Marketplace actions"):
        assert fragment not in caplog.text, fragment


# ---------------------------------------------------------------------------
# Preflight: the fallback chain stops on an agreement denial (Option B)
# ---------------------------------------------------------------------------


def test_verify_stops_on_an_agreement_denial_without_probing_the_fallback() -> None:
    # In a new account the fallback could "pass" inside its OWN background-subscription
    # window -- a second false pass that Connect would adopt as the configured model.
    fallback = BEDROCK_MODEL_FALLBACKS[0]
    client = _ScriptedClient(
        {
            DEFAULT_BEDROCK_MODEL_ID: _ce("AccessDeniedException", MARKETPLACE_MSG),
            fallback: None,  # would succeed if probed
        }
    )
    result = _verify(client)
    assert result.ok is False
    assert result.reason == "MODEL_AGREEMENT_REQUIRED"
    assert result.model_id == DEFAULT_BEDROCK_MODEL_ID
    assert client.model_ids == [DEFAULT_BEDROCK_MODEL_ID]  # one call, no fallback probe
    assert fallback not in result.detail
    assert result.detail == aa._ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"]


@pytest.mark.parametrize(
    ("configured_exc", "expected"),
    [
        (_ce("ValidationException", "not enabled"), "MODEL_NOT_ENABLED"),
        (_ce("AccessDeniedException", "denied"), "ACCESS_DENIED"),
    ],
)
def test_a_fallbacks_agreement_denial_still_reports_the_configured_reason(
    configured_exc: ClientError, expected: str
) -> None:
    # The chain still probes the fallback for the failures it can fix; when the
    # fallback is itself agreement-denied, the operator's own model's reason is shown.
    fallback = BEDROCK_MODEL_FALLBACKS[0]
    client = _ScriptedClient(
        {
            DEFAULT_BEDROCK_MODEL_ID: configured_exc,
            fallback: _ce("AccessDeniedException", MARKETPLACE_MSG),
        }
    )
    result = _verify(client)
    assert (result.ok, result.reason, result.model_id) == (
        False,
        expected,
        DEFAULT_BEDROCK_MODEL_ID,
    )
    assert client.model_ids == [DEFAULT_BEDROCK_MODEL_ID, fallback]
    assert result.detail == aa._ACCESS_CHECK_DETAILS[expected]


# ---------------------------------------------------------------------------
# A pass is described honestly
# ---------------------------------------------------------------------------


def test_ok_copy_is_honest_and_carries_the_first_call_caveat() -> None:
    caveat = aa._FIRST_CALL_CAVEAT
    assert "15 minutes" in caveat and "agreement" in caveat
    # It promises nothing about a later denial's wording: only a denial that names AWS
    # Marketplace gets the agreement copy; e.g. a missing first-time use form need not.
    assert "denial message" not in caveat

    clean = _verify(_ScriptedClient({DEFAULT_BEDROCK_MODEL_ID: None}))
    assert (clean.ok, clean.reason) == (True, "OK")
    assert clean.detail == aa._ACCESS_CHECK_DETAILS["OK"]
    assert clean.detail.startswith(
        "Amazon Bedrock access verified: the configured model answered just now"
    )
    assert caveat in clean.detail
    assert "reachable" not in clean.detail
    # Still a clean green pass: the caveat is an info-level note, not a warning.
    display = map_access_check_display(clean, configured_model_id=DEFAULT_BEDROCK_MODEL_ID)
    assert display.notify_type == "positive"

    fallback = BEDROCK_MODEL_FALLBACKS[0]
    not_enabled = _verify(
        _ScriptedClient(
            {DEFAULT_BEDROCK_MODEL_ID: _ce("ValidationException", "x"), fallback: None}
        )
    )
    assert (not_enabled.ok, not_enabled.model_id) == (True, fallback)
    assert caveat in not_enabled.detail

    denied = _verify(
        _ScriptedClient(
            {
                DEFAULT_BEDROCK_MODEL_ID: _ce("AccessDeniedException", "denied"),
                fallback: None,
            }
        )
    )
    assert (denied.ok, denied.model_id) == (True, fallback)
    assert caveat in denied.detail
    assert "not enabled" not in denied.detail  # the two fallback variants stay distinct


# ---------------------------------------------------------------------------
# Taxonomy completeness (a SECURITY control, not housekeeping)
# ---------------------------------------------------------------------------


def test_every_reason_taxonomy_is_complete() -> None:
    # Every table lookup / Literal check below runs inside an ``except`` block at
    # runtime; a missing entry would raise there and chain the original botocore
    # error -- with its message -- into the panel's log.
    from dsql_migrator.core.assessment_strategist import (
        AiAssessmentOutcome,
        ObjectGuidanceOutcome,
    )
    from dsql_migrator.ui.ai_assist import ACCESS_CHECK_NOTIFY_TYPES

    unavailable = set(typing.get_args(aa.UnavailableReason))
    assert "MODEL_AGREEMENT_REQUIRED" in unavailable
    assert set(aa._UNAVAILABLE_DETAILS) == unavailable
    for cls in (aa.AiSuggestionOutcome, AiAssessmentOutcome, ObjectGuidanceOutcome):
        hint = typing.get_type_hints(cls)["reason"]
        assert set(typing.get_args(hint)) == {"OK"} | unavailable, cls.__name__

    access = set(typing.get_args(aa.AccessCheckReason))
    assert "MODEL_AGREEMENT_REQUIRED" in access
    assert set(aa._ACCESS_CHECK_DETAILS) == access
    assert set(ACCESS_CHECK_NOTIFY_TYPES) == access
    model_literal = AiAccessCheckResult.model_fields["reason"].annotation
    assert set(typing.get_args(model_literal)) == access

    for reason in sorted(unavailable):
        AiAssistUnavailableError(reason)  # type: ignore[arg-type]
    for reason in sorted(access):
        AiAccessCheckResult(ok=reason == "OK", reason=reason, detail="d", model_id="m")
