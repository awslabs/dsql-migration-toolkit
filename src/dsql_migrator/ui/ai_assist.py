# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI-assisted conversion: NiceGUI-agnostic config, review gate, and seam.

This module holds the AI-assist logic that is independent of NiceGUI so it can
be unit tested directly:

- building the per-session :class:`~dsql_migrator.core.models.AiAssistConfig`
  from the settings form (default disabled / opt-in, default model id
  ``global.anthropic.claude-sonnet-5`` -- Requirements 11.1, 11.2, 11.3, 11.4),
- the injectable :class:`AiConversionAssistant` Protocol seam. The real
  Bedrock-backed assistant lands in Task 16; the UI depends only on this seam so
  it can be wired later without changing the screen,
- suggestion status transitions (edit / approve / reject), and
- the human-review gate (Property 13): only an explicitly ``APPROVED`` suggestion
  is eligible for the Schema Applier path; rejected / pending /
  edited-but-not-approved suggestions are excluded.

AI assist augments the deterministic (sqlglot) path; it never replaces it. When
AI is disabled (the default) or no assistant is wired, no suggestions are
generated and the review gate yields nothing, so the workflow behaves exactly
as the deterministic-only path (Requirements 11.1, 11.2).
"""

from __future__ import annotations

from typing import Callable, NamedTuple, Optional, Protocol, Sequence

from dsql_migrator.core.models import (
    AiAccessCheckResult,
    AiAssistConfig,
    AiConversionSuggestion,
)

# Review lifecycle status values (mirror AiConversionSuggestion.status).
AI_STATUS_PENDING_REVIEW = "PENDING_REVIEW"
AI_STATUS_EDITED = "EDITED"
AI_STATUS_APPROVED = "APPROVED"
AI_STATUS_REJECTED = "REJECTED"

# Default Bedrock model id used when the model input is left blank (Req 11.3).
# This MUST be a real Bedrock model / inference-profile id (it is passed verbatim
# as ``modelId`` to ``invoke_model``), not a display name. Sonnet 5 is the
# general, cost-effective default; AI assist is opt-in and every suggestion
# passes the human-review gate (Property 13) before it can touch the schema.
# The ``global.`` (not ``us.``) cross-region-inference profile is used so the
# default is reachable from any commercial region -- a ``us.*`` profile is
# US-geography-scoped and fails InvokeModel in e.g. ap-northeast-2 (Seoul).
DEFAULT_BEDROCK_MODEL_ID = "global.anthropic.claude-sonnet-5"

# Fallback used when the default model is not enabled for THIS account/region.
# A `global.` profile existing in a region does not mean the account may invoke
# it -- model access is granted per account, so a fresh account can have the
# profile ACTIVE and still get a model-not-enabled error. Rather than leaving the
# operator on a dead preflight, the check retries the next entry and reports which
# model it settled on.
#
# Sonnet 4.6, not 4.5: it is already an accepted BedrockModelId value (so the
# derived IAM scope and the deploy-time AllowedValues already cover it) and it is
# the newer model. Adding 4.5 would mean a new allowed value whose only effect is
# a worse fallback.
#
# Kept to ONE alternative on purpose. Each attempt is a real InvokeModel round
# trip, so a long chain turns a quick preflight into a slow one -- and a chain
# that silently walks several tiers hides from the operator which model their
# suggestions actually came from.
BEDROCK_MODEL_FALLBACKS: tuple[str, ...] = ("global.anthropic.claude-sonnet-4-6",)

# The curated set the Connect "Model ID" dropdown offers. GLOBAL cross-region Anthropic
# inference profiles only (they resolve from every commercial region), so the picker
# never offers a US-only id that fails outside the US. MUST stay in sync with the
# ``BedrockModelId`` AllowedValues in deploy/cloudformation.yaml -- a deployment test
# asserts the two match, so a model added in one place must be added in both. The IAM
# scope is provider-wide (``anthropic.*``), so any of these is invokable without a
# redeploy (still subject to per-account Bedrock model access). ``DEFAULT_BEDROCK_MODEL_ID``
# is the first entry.
SUPPORTED_BEDROCK_MODELS: tuple[str, ...] = (
    "global.anthropic.claude-sonnet-5",
    "global.anthropic.claude-opus-5",
    "global.anthropic.claude-opus-4-8",
    "global.anthropic.claude-sonnet-4-6",
)


# ---------------------------------------------------------------------------
# Settings form -> AiAssistConfig
# ---------------------------------------------------------------------------


def build_ai_assist_config(
    *,
    enabled: bool,
    model_id: Optional[str] = None,
    region: Optional[str] = None,
) -> AiAssistConfig:
    """Build an :class:`AiAssistConfig` from the settings form input.

    AI assist is opt-in: ``enabled`` defaults to ``False`` via the model and is
    coerced to ``bool`` here. A blank ``model_id`` falls back to the default
    ``global.anthropic.claude-sonnet-5`` (``BEDROCK_MODEL_ID``); a blank ``region``
    becomes ``None`` so the model's default applies (``BEDROCK_REGION``).
    """
    cleaned_model = (model_id or "").strip()
    cleaned_region = (region or "").strip() or None
    return AiAssistConfig(
        enabled=bool(enabled),
        model_id=cleaned_model or DEFAULT_BEDROCK_MODEL_ID,
        region=cleaned_region,
    )


# ---------------------------------------------------------------------------
# AI Conversion Assistant seam (Task 16 wires the real Bedrock-backed impl)
# ---------------------------------------------------------------------------


class AiConversionAssistant(Protocol):
    """Generates a reviewable schema-conversion suggestion (design.md section).

    Mirrors the design's ``AiConversionAssistant.suggest_schema_conversion``. The
    UI depends only on this seam so the real Bedrock-backed assistant (Task 16)
    can be injected later. Implementations treat their output as untrusted data
    and never write to the source (Property 1, 13).
    """

    def suggest_schema_conversion(
        self,
        *,
        object_name: str,
        source_ddl: str,
        deterministic_result: Optional[str],
        dsql_constraints: str,
    ) -> AiConversionSuggestion:
        """Return an AI suggestion for one MANUAL/UNSUPPORTED schema object."""


# ---------------------------------------------------------------------------
# Suggestion status transitions (edit / approve / reject)
# ---------------------------------------------------------------------------


def edit_suggestion(
    suggestion: AiConversionSuggestion, new_text: str
) -> AiConversionSuggestion:
    """Return a copy with edited text, status ``EDITED`` and approval cleared.

    Editing a suggestion always revokes any prior approval: an edited suggestion
    must be explicitly approved again before it can be applied (Property 13).
    """
    return suggestion.model_copy(
        update={
            "suggested_sql_or_expr": new_text,
            "status": AI_STATUS_EDITED,
            "approved_by_user": False,
        }
    )


def approve_suggestion(suggestion: AiConversionSuggestion) -> AiConversionSuggestion:
    """Return a copy marked ``APPROVED`` with ``approved_by_user`` set.

    Only after this explicit approval may a suggestion flow to the Schema
    Applier path (Property 13 / Requirements 10.9, 11.7).
    """
    return suggestion.model_copy(
        update={"status": AI_STATUS_APPROVED, "approved_by_user": True}
    )


def reject_suggestion(suggestion: AiConversionSuggestion) -> AiConversionSuggestion:
    """Return a copy marked ``REJECTED`` with approval cleared.

    A rejected suggestion is never applied; the deterministic result and the
    object's ``MANUAL``/``UNSUPPORTED`` flag are kept (Requirement 11.10).
    """
    return suggestion.model_copy(
        update={"status": AI_STATUS_REJECTED, "approved_by_user": False}
    )


# ---------------------------------------------------------------------------
# Human-review gate (Property 13)
# ---------------------------------------------------------------------------


def is_approved(suggestion: AiConversionSuggestion) -> bool:
    """Return whether ``suggestion`` is explicitly approved for apply.

    A suggestion is approved only when both its ``status`` is ``APPROVED`` and
    ``approved_by_user`` is ``True``. Pending, edited, and rejected suggestions
    are never considered approved (Property 13).
    """
    return suggestion.status == AI_STATUS_APPROVED and suggestion.approved_by_user


def approved_suggestions(
    suggestions: Sequence[AiConversionSuggestion],
) -> list[AiConversionSuggestion]:
    """Return only the explicitly approved suggestions from ``suggestions``.

    This is the heart of the review gate (Property 13): rejected, pending, and
    edited-but-not-approved suggestions are excluded, so nothing reaches the
    Schema Applier path without an explicit human approval.
    """
    return [suggestion for suggestion in suggestions if is_approved(suggestion)]


# ---------------------------------------------------------------------------
# "Verify AI access" result display mapping (Requirements 11.13, 11.14)
# ---------------------------------------------------------------------------


class AiAccessDisplay(NamedTuple):
    """A NiceGUI-ready rendering of an :class:`AiAccessCheckResult`.

    ``message`` is the actionable, credential-free text to surface (it is the
    result's ``detail`` verbatim, so it never exposes credentials -- Requirement
    11.15 / Property 7) and ``notify_type`` is the NiceGUI notification severity.
    """

    message: str
    notify_type: str


# Notification severity per preflight reason: success is positive; permission /
# model-not-enabled failures are hard errors (negative); throttling and unknown
# (e.g. transient network) are warnings the user can retry.
ACCESS_CHECK_NOTIFY_TYPES: dict[str, str] = {
    "OK": "positive",
    "ACCESS_DENIED": "negative",
    "MODEL_NOT_ENABLED": "negative",
    "THROTTLED": "warning",
    "UNKNOWN": "warning",
}


def map_access_check_display(
    result: AiAccessCheckResult, *, configured_model_id: Optional[str] = None
) -> AiAccessDisplay:
    """Map an :class:`AiAccessCheckResult` to a ``(message, notify_type)`` pair.

    The message is the result's ``detail`` (already actionable and
    credential-free); the notify type is chosen from
    :data:`ACCESS_CHECK_NOTIFY_TYPES` by ``reason`` (defaulting to ``warning``
    for any unrecognized reason). This mapping never reads or echoes
    credentials.

    Pass ``configured_model_id`` (the model the operator selected) to have a pass
    that landed on a fallback shown as a warning rather than a clean success --
    without it the fallback is indistinguishable from a normal pass. Optional so
    existing callers keep their behaviour.
    """
    notify_type = ACCESS_CHECK_NOTIFY_TYPES.get(result.reason, "warning")
    # A preflight that passed on a FALLBACK is reason="OK", but showing it in the
    # same green as a clean pass would read as "your configured model works" when it
    # does not. AI assist is usable, so this is not an error -- it is the design
    # system's definition of a warning: a real but non-blocking issue the operator
    # should know about. Detected by the model that answered differing from the one
    # configured, which is exactly what the fallback path reports.
    if (
        result.reason == "OK"
        and configured_model_id is not None
        and result.model_id != configured_model_id
    ):
        notify_type = "warning"
    return AiAccessDisplay(message=result.detail, notify_type=notify_type)


# ---------------------------------------------------------------------------
# "Verify AI access" runner (Requirements 11.13-11.16)
# ---------------------------------------------------------------------------


class VerifyAccessAssistant(Protocol):
    """Seam for the assistant used by the Verify AI access button.

    Only ``verify_access`` is needed here; the real Bedrock-backed
    :class:`~dsql_migrator.core.ai_assistant.AiConversionAssistant` satisfies it.
    """

    def verify_access(self) -> AiAccessCheckResult:
        """Run the non-blocking Bedrock access preflight."""


# Builds the assistant for a given AI-assist config and optional global AWS
# profile. Injectable so tests pass a fake and never reach AWS.
VerifyAssistantFactory = Callable[
    [AiAssistConfig, Optional[str]], VerifyAccessAssistant
]


def _default_verify_assistant_factory(
    config: AiAssistConfig, aws_profile: Optional[str]
) -> VerifyAccessAssistant:
    """Build the real assistant with a Bedrock client for ``aws_profile``.

    The optional global profile is threaded through
    :func:`~dsql_migrator.core.ai_assistant.build_bedrock_runtime_client`'s
    ``aws_profile`` parameter (the Task 17.2 shared-session seam), so the
    preflight uses the same single credential context as every other AWS client
    (Requirement 9.5/9.7). ``boto3`` stays lazily imported inside that builder,
    and building the client performs no network call.
    """
    from dsql_migrator.core.ai_assistant import (
        AiConversionAssistant as _BedrockAssistant,
        build_bedrock_runtime_client,
    )

    client = build_bedrock_runtime_client(config, aws_profile=aws_profile)
    return _BedrockAssistant(config, client=client)


def run_verify_ai_access(
    config: AiAssistConfig,
    aws_profile: Optional[str],
    *,
    assistant_factory: Optional[VerifyAssistantFactory] = None,
) -> AiAccessCheckResult:
    """Run the "Verify AI access" preflight for the configured model/region.

    Builds the assistant via ``assistant_factory`` (defaulting to the real
    Bedrock-backed one) using the optional global AWS profile, then calls its
    non-blocking :meth:`verify_access`. ``verify_access`` never raises into the
    workflow and never exposes credentials (Requirements 11.15, 11.16), so this
    helper is safe to run off the event loop without crashing the UI.
    """
    factory = (
        assistant_factory
        if assistant_factory is not None
        else _default_verify_assistant_factory
    )
    # Building the assistant constructs a boto3 client, which can raise before
    # verify_access() runs (e.g. NoRegionError when no region is resolvable, or
    # UnknownServiceError). verify_access() itself never raises, but the factory
    # can -- so classify that failure to an actionable, credential-free result
    # instead of letting a raw exception escape into the UI (Requirement 11.15/16).
    try:
        assistant = factory(config, aws_profile)
    except Exception as exc:  # noqa: BLE001 - client construction must not crash the UI
        from dsql_migrator.core.ai_assistant import (
            _ACCESS_CHECK_DETAILS,
            _classify_access_check_error,
        )

        reason = _classify_access_check_error(exc)
        return AiAccessCheckResult(
            ok=False,
            reason=reason,
            detail=_ACCESS_CHECK_DETAILS[reason],
            model_id=config.model_id,
            region=config.region,
        )
    return assistant.verify_access()


__all__ = [
    "AI_STATUS_PENDING_REVIEW",
    "AI_STATUS_EDITED",
    "AI_STATUS_APPROVED",
    "AI_STATUS_REJECTED",
    "DEFAULT_BEDROCK_MODEL_ID",
    "SUPPORTED_BEDROCK_MODELS",
    "build_ai_assist_config",
    "AiConversionAssistant",
    "edit_suggestion",
    "approve_suggestion",
    "reject_suggestion",
    "is_approved",
    "approved_suggestions",
    "AiAccessDisplay",
    "ACCESS_CHECK_NOTIFY_TYPES",
    "map_access_check_display",
    "VerifyAccessAssistant",
    "VerifyAssistantFactory",
    "run_verify_ai_access",
]
