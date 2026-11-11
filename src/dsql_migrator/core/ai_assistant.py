# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI assist plumbing shared by the AI DBA: config, Bedrock client, output safety.

The AI data models (:class:`~dsql_migrator.core.models.AiAssistConfig` and
:class:`~dsql_migrator.core.models.AiAccessCheckResult`) live in ``core/models.py``.
This module holds the Bedrock plumbing the AI DBA (``core/assessment_strategist.py``)
and the Connect screen's "Verify AI access" build on:

- constructing the ``bedrock-runtime`` ``boto3`` client used to call Bedrock
  (Requirement 11.9) for an ``AiAssistConfig``, whose model id and region the Connect
  screen seeds from ``BEDROCK_MODEL_ID`` / ``BEDROCK_REGION``
  (:func:`dsql_migrator.config.load_connect_defaults`; Requirements 11.3, 11.4);
- :func:`validate_suggested_sql`, which blocks forbidden statements (e.g. data
  mutation, ``DROP``, role/grant, transaction control, ``COPY``) in AI-proposed SQL
  before Schema Conversion adopts it as a target DDL (Requirement 11.8);
- the mapping of Bedrock failures (permission / model agreement / throttle /
  network / unavailable / unparseable output) to a typed, credential-free
  :class:`AiAssistUnavailableError` (Requirement 11.10); and
- :meth:`AiConversionAssistant.verify_access`, the least-cost "Verify AI access"
  preflight (Requirements 11.13-11.16).

Nothing here applies anything: AI output is advisory, and SQL from a reply reaches
the target only after the operator adopts it as a target DDL and runs Apply.

Credential handling (Requirement 11.9 / Property 7):

- The client uses IAM-based authentication via the standard AWS credential
  chain; no credentials are hardcoded.
- The ``boto3`` session/client is injectable. The shared ``boto3.Session``
  (``core/aws_session.build_session``), which honors an optional global AWS
  profile, is used to build the client; callers may also pass a session or a
  fake in explicitly without changing call sites, and unit tests supply a fake
  so they never reach AWS. Constructing the client performs no network call.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Optional

from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.models import AiAccessCheckResult, AiAssistConfig

# The boto3 service name for the Bedrock runtime (InvokeModel) client.
BEDROCK_RUNTIME_SERVICE = "bedrock-runtime"

# Anthropic-style InvokeModel request shaping (the default model is an
# Anthropic Claude model). Kept minimal and localized so it is easy to adjust
# for other model families.
_ANTHROPIC_VERSION = "bedrock-2023-05-31"


def _default_session() -> BotoSessionLike:
    """Create the shared default ``boto3.Session`` (standard credential chain).

    Delegates to :func:`dsql_migrator.core.aws_session.build_session` with no
    profile, so the Bedrock-runtime client is built from the same shared session
    as every other AWS client and there is no Bedrock-only credential context
    (Requirement 9.7). The ``boto3`` import stays lazy inside ``build_session``.
    The global profile is threaded through :func:`build_bedrock_runtime_client`'s
    ``aws_profile`` parameter rather than here, so this stays a zero-argument
    seam that tests can replace.
    """
    return build_session(None)


def build_bedrock_runtime_client(
    config: AiAssistConfig,
    *,
    session: Optional[BotoSessionLike] = None,
    aws_profile: Optional[str] = None,
) -> Any:
    """Construct a ``bedrock-runtime`` client for the configured region.

    Authentication is IAM-based via the session's credential chain; no
    credentials are passed or hardcoded (Requirement 11.9). The client is created
    from the single shared ``boto3.Session`` so it uses the same credential
    context as DSQL token generation and Secrets Manager -- there is no
    Bedrock-only credential context (Requirement 9.7).

    Session selection:

    - ``session`` is the explicit dependency-injection seam: when provided it is
      used as-is (tests pass a fake; callers may pass an already-built shared
      session). ``aws_profile`` is ignored in this case.
    - otherwise, when ``aws_profile`` is set, the shared session is built for
      that global profile via :func:`build_session` (Requirements 9.5, 9.6);
    - otherwise the default shared session (standard credential chain) is built
      via :func:`_default_session`.

    ``config.region`` (``BEDROCK_REGION``) is forwarded as ``region_name`` when
    set; otherwise the client falls back to the session's region. This only
    builds the client (a lazy ``boto3`` operation) and performs no network call,
    so it must be invoked on demand by an enabled AI-assist workflow.
    """
    if session is not None:
        active_session = session
    elif aws_profile:
        active_session = build_session(aws_profile)
    else:
        active_session = _default_session()
    # Bound the connect/read timeouts so a hung TCP connection to Bedrock cannot
    # leave an "AI is writing…"/"Verifying…" state spinning forever -- a stalled
    # socket surfaces as a NETWORK/timeout error the caller classifies and shows.
    # For a streaming response read_timeout applies per chunk (a healthy,
    # progressing stream resets it), so it never truncates a long-but-live reply.
    from botocore.config import Config as _BotoConfig

    client_config = _BotoConfig(connect_timeout=10, read_timeout=60)
    if config.region:
        return active_session.client(
            BEDROCK_RUNTIME_SERVICE, region_name=config.region, config=client_config
        )
    return active_session.client(BEDROCK_RUNTIME_SERVICE, config=client_config)


# ---------------------------------------------------------------------------
# Untrusted-output handling: forbidden-statement validation (Req 11.8)
# ---------------------------------------------------------------------------

# Statement-leading keywords that must never appear in AI-written SQL that
# Schema Conversion's "Use as target DDL" adopts as an object's target DDL, which
# Apply then runs on the target. That SQL is expected to be the object's own DDL
# (its CREATE TABLE or CREATE VIEW, and a table's indexes and foreign keys). The
# following are out of that scope and dangerous if applied, so they are blocked:
# data mutation (DELETE/UPDATE/INSERT/TRUNCATE), object/database removal (DROP),
# privilege/role changes (GRANT/REVOKE), transaction/session control (BEGIN/
# START/COMMIT/ROLLBACK/SAVEPOINT/SET), bulk/file I/O (COPY), and arbitrary
# procedural execution (CALL/DO). The set is intentionally small and documented;
# the operator's explicit action (Property 13: adopting the reply, then running
# Apply) is the primary control, and this validation is defense-in-depth so
# unsafe output never becomes a target DDL.
_FORBIDDEN_LEADING_KEYWORDS = frozenset(
    {
        "DROP",
        "DELETE",
        "UPDATE",
        "INSERT",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "BEGIN",
        "START",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "SET",
        "COPY",
        "CALL",
        "DO",
        "VACUUM",
        "REINDEX",
        "CLUSTER",
        "REASSIGN",
        "LOCK",
    }
)

# Second token blocked after an otherwise-allowed CREATE/ALTER: these target
# databases/schemas/roles/users/the running system rather than a single
# migratable object. CREATE TABLE/INDEX/VIEW/SEQUENCE/TYPE/FUNCTION and
# ALTER TABLE/INDEX/SEQUENCE remain allowed.
_FORBIDDEN_CREATE_ALTER_TARGETS = frozenset(
    {"ROLE", "USER", "DATABASE", "SCHEMA", "SYSTEM", "GROUP", "TABLESPACE"}
)

_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
_LINE_COMMENT_PATTERN = re.compile(r"--[^\n]*")


@dataclass(frozen=True)
class SqlValidation:
    """Outcome of validating AI-written SQL (Req 11.8).

    ``is_safe`` is ``True`` only when no forbidden statement was found; when it is
    ``False``, ``reason`` is a short, human-readable explanation of the blocked
    construct, which "Use as target DDL" shows instead of adopting the SQL.
    """

    is_safe: bool
    reason: str = ""


def _strip_sql_comments(text: str) -> str:
    """Remove block and line comments so they cannot hide forbidden statements."""
    without_block = _COMMENT_PATTERN.sub(" ", text)
    return _LINE_COMMENT_PATTERN.sub(" ", without_block)


def validate_suggested_sql(text: str) -> SqlValidation:
    """Validate untrusted model output for forbidden statements (Req 11.8).

    The model output is untrusted data, so before AI-written SQL can become an
    object's target DDL ("Use as target DDL" in Schema Conversion) it is scanned
    for statements outside the scope of one object's DDL. Each ``;``-separated
    statement is checked by its leading keyword against
    :data:`_FORBIDDEN_LEADING_KEYWORDS` (plus the ``CREATE``/``ALTER`` target
    guard), which also catches a dangerous statement appended after a benign one
    (statement-injection). This is a deliberately small, documented denylist used
    as defense-in-depth behind the operator's explicit action (Property 13); it is
    not a full SQL parser.

    Empty/whitespace text is treated as safe here (it carries no forbidden
    statement); the caller decides separately whether empty output is usable.
    """
    cleaned = _strip_sql_comments(text)
    for raw_statement in cleaned.split(";"):
        tokens = raw_statement.split()
        if not tokens:
            continue
        leading = tokens[0].upper()
        if leading in _FORBIDDEN_LEADING_KEYWORDS:
            return SqlValidation(
                is_safe=False,
                reason=f"forbidden statement '{leading}' is not allowed in a conversion suggestion",
            )
        if leading in {"CREATE", "ALTER"} and len(tokens) > 1:
            target = tokens[1].upper()
            if target in _FORBIDDEN_CREATE_ALTER_TARGETS:
                return SqlValidation(
                    is_safe=False,
                    reason=f"forbidden statement '{leading} {target}' is not allowed in a conversion suggestion",
                )
    return SqlValidation(is_safe=True)


# ---------------------------------------------------------------------------
# Graceful degradation: typed unavailability (Req 11.10)
# ---------------------------------------------------------------------------

# Why an AI reply could not be produced. The outcome type built from it
# (``ObjectGuidanceOutcome`` in core/assessment_strategist.py) adds "OK" for a usable
# reply; every reason here means the deterministic workflow continues without the
# AI part (Requirement 11.10).
UnavailableReason = Literal[
    "ACCESS_DENIED",
    "MODEL_AGREEMENT_REQUIRED",
    "THROTTLED",
    "NETWORK",
    "UNAVAILABLE",
    "INVALID_OUTPUT",
]

# The one-time remedy for MODEL_AGREEMENT_REQUIRED, shared by the AI DBA reply copy
# and the preflight copy so the two cannot drift. Fixed text only (Property 7): the
# denial's own message is never echoed. ``--model-id`` takes the FOUNDATION-model id,
# not the ``global.`` inference-profile id this tool is configured with, hence the
# placeholder rather than an interpolated id; an agreement is per account, so no region
# is claimed. The caller appends how to retry.
_MODEL_AGREEMENT_REMEDY = (
    "Someone with AWS Marketplace permissions must accept the model's agreement once "
    "for the account: in the Amazon Bedrock console, or with aws bedrock "
    "list-foundation-model-agreement-offers --model-id <foundation-model-id> and then "
    "aws bedrock create-foundation-model-agreement --model-id <foundation-model-id> "
    "--offer-token <offer-token> (<foundation-model-id> is the configured model id "
    "without its global. prefix). Anthropic models may also need the one-time "
    "first-time use form, and the account needs a valid payment method. No IAM change "
    "to this tool's role is needed. Wait about 2 minutes, then "
)

# Fixed, credential-free, actionable messages for an AI DBA reply that could not be
# produced. They never echo the raw boto/Bedrock exception text, so no credential or
# endpoint detail can leak (Property 7), and each one says what failed, that the
# deterministic workflow is unaffected, and what to do next. Every reason is shown in
# the AI DBA panel's "AI reply unavailable" notice, whichever chat asked (a step's chat,
# the general chat or the "What's next?" briefing). A reason that latches the session
# unavailable (ui.ai_assist.AI_LATCHING_REASONS: ACCESS_DENIED and
# MODEL_AGREEMENT_REQUIRED here) is also shown away from that reply -- the composer
# hint, the "What's next?" toast and the Connect screen's AI status notice -- so its
# copy says where Verify AI access is. Verify AI access reports its own copy
# (_ACCESS_CHECK_DETAILS).
_UNAVAILABLE_DETAILS: dict[str, str] = {
    "ACCESS_DENIED": (
        "AI assist is unavailable: Amazon Bedrock denied the request. The "
        "deterministic workflow is unaffected. Grant scoped bedrock:InvokeModel and "
        "bedrock:InvokeModelWithResponseStream permissions for the configured model "
        "-- or, if your AWS credentials or session have expired, re-authenticate -- "
        "then run Verify AI access on the Connect screen to turn AI assist back on."
    ),
    # "the configured model is not enabled for this AWS account yet" is word for word the
    # preflight copy's phrase and the one the docs quote, so the panel's sentence can be
    # matched to the manual and the troubleshooting table.
    "MODEL_AGREEMENT_REQUIRED": (
        "AI assist is unavailable: the configured model is not enabled for this AWS "
        "account yet (its AWS Marketplace subscription is missing or did not "
        "complete). The deterministic workflow is unaffected. "
        + _MODEL_AGREEMENT_REMEDY
        + "retry, or run Verify AI access on the Connect screen."
    ),
    "THROTTLED": (
        "AI assist is temporarily unavailable: Amazon Bedrock throttled the "
        "request. The deterministic workflow is unaffected. Ask again later."
    ),
    "NETWORK": (
        "AI assist is unavailable: Amazon Bedrock could not be reached or did not "
        "answer in time. The deterministic workflow is unaffected. Ask again; if it "
        "keeps failing, check connectivity to the Bedrock endpoint."
    ),
    "UNAVAILABLE": (
        "AI assist is unavailable: the Amazon Bedrock request failed. The "
        "deterministic workflow is unaffected. Ask again; if it keeps failing, run "
        "Verify AI access on the Connect screen to check the configured model and "
        "region."
    ),
    "INVALID_OUTPUT": (
        "AI assist could not produce a usable reply this time: the model's reply "
        "came back empty. The deterministic workflow is unaffected. Ask again, or "
        "rephrase the question."
    ),
}


class AiAssistUnavailableError(RuntimeError):
    """Raised when an AI reply cannot be produced (Requirement 11.10).

    This is a *typed, catchable* signal -- not a raw boto/Bedrock exception -- so
    a caller can map it to "AI unavailable" without breaking the deterministic
    workflow. ``reason`` is one of :data:`UnavailableReason` and ``detail`` is a
    fixed, credential-free message (Property 7). The AI DBA's chats in
    ``core/assessment_strategist.py`` (``stream_chat``, ``tool_chat``) turn it into
    an unavailable outcome, whose ``detail`` the AI DBA panel shows.
    """

    def __init__(self, reason: UnavailableReason) -> None:
        self.reason: UnavailableReason = reason
        self.detail: str = _UNAVAILABLE_DETAILS[reason]
        super().__init__(self.detail)


# IAM / SigV4 auth failures: either no permission, or the credentials/session
# have expired or are otherwise invalid. Both map to ACCESS_DENIED -- the
# actionable message covers granting the permission AND re-authenticating -- so
# an expired-token error no longer degrades to a vague generic "unavailable".
# The one exception is a denial in _MODEL_ACCESS_DENIED_CODES whose message carries
# the fixed AWS Marketplace marker (see _has_model_agreement_marker): that is
# MODEL_AGREEMENT_REQUIRED, which needs a different remedy.
_ACCESS_DENIED_CODES = frozenset(
    {
        "AccessDeniedException",
        "AccessDenied",
        "UnauthorizedException",
        "UnrecognizedClientException",
        "ExpiredTokenException",
        "ExpiredToken",
        "RequestExpired",
        "InvalidSignatureException",
        "SignatureDoesNotMatch",
        "InvalidClientTokenId",
        "InvalidAccessKeyId",
        "AuthFailure",
    }
)

# The authorization-AMBIGUOUS subset of _ACCESS_DENIED_CODES: a denial that may be about
# this model rather than about the caller's identity. Both classifiers apply the
# model-agreement marker test to these codes only, and the preflight's fallback chain is
# reachable from them. Bedrock reports "this model is not available for this account" as
# AccessDeniedException -- NOT ValidationException -- so keying the fallback on
# _MODEL_NOT_ENABLED_CODES alone made it unreachable in exactly the case it was written
# for (measured: a global. inference profile denied for the account while a different
# model answered on the same credentials).
#
# The credential-identity codes in _ACCESS_DENIED_CODES (ExpiredToken*, RequestExpired,
# InvalidSignature*, SignatureDoesNotMatch, InvalidClientTokenId, InvalidAccessKeyId,
# UnrecognizedClientException, AuthFailure) are deliberately NOT here: another modelId
# provably cannot fix a bad or expired credential, so probing one would only add a round
# trip and delay the real cause, and they keep their code-only ACCESS_DENIED whatever
# their message says. The split is by error code; the only thing ever read from a
# message is the one fixed-marker presence test, whose text is never returned, stored,
# logged, shown or sent (Requirement 11.15 / Property 7).
_MODEL_ACCESS_DENIED_CODES = frozenset(
    {
        "AccessDeniedException",
        "AccessDenied",
        "UnauthorizedException",
    }
)


def _client_error_field(exc: BaseException, key: str) -> object:
    """Return ``exc.response['Error'][key]`` from a botocore ``ClientError``, else ``None``.

    Total on purpose: both classifiers run inside ``except`` blocks, where any exception
    raised here would chain the ORIGINAL error -- and its message -- into a log.
    """
    try:
        response = getattr(exc, "response", None)
        if isinstance(response, Mapping):
            error = response.get("Error")
            if isinstance(error, Mapping):
                return error.get(key)
    except Exception:  # noqa: BLE001 - classification must never raise
        pass
    return None


def _error_code(exc: BaseException) -> str:
    """Return a botocore ``ClientError``'s ``response['Error']['Code']``, or ``""``.

    Together with :func:`_has_model_agreement_marker` (one fixed-marker presence test on
    the structured message), this is all that is read off an exception anywhere in this
    module's classification: never ``str(exc)``, the endpoint or any credential, and no
    message text is ever returned, stored, logged, shown or sent (Requirement 11.15 /
    Property 7). Shared by both classifiers and by the preflight's fallback decision so
    there is one definition of "which code was this". Never raises.
    """
    return str(_client_error_field(exc, "Code") or "")


# The ONE fixed marker a classifier may look for in a denial's message. Bedrock denies a
# third-party model whose AWS Marketplace subscription is missing or failed with the SAME
# code as a missing IAM grant (AccessDeniedException); only the message tells them apart
# ("... not authorized to perform the required AWS Marketplace actions ..."). It is
# "aws marketplace" WITH its space: no ARN, role/session name, model id or resource id can
# contain a space, so a plain IAM denial naming e.g. a role "MarketplaceOps" never matches.
_MODEL_AGREEMENT_MARKER = "aws marketplace"


def _has_model_agreement_marker(exc: BaseException) -> bool:
    """Return whether a denial's ``response['Error']['Message']`` names AWS Marketplace.

    PRESENCE TEST ONLY (Requirement 11.15 / Property 7): the message is
    whitespace-collapsed and case-folded in a local, tested for the fixed
    :data:`_MODEL_AGREEMENT_MARKER`, and dropped. It is never returned, stored, logged,
    formatted or sent anywhere -- the caller gets a bool and maps it to a fixed reason
    with fixed copy. Total: every failure (no response, non-Mapping shapes, a missing or
    non-str message, an accessor that raises) reads as ``False``.
    """
    message = _client_error_field(exc, "Message")
    if not isinstance(message, str):
        return False
    return _MODEL_AGREEMENT_MARKER in " ".join(message.split()).casefold()


def _classify_bedrock_error(exc: BaseException) -> UnavailableReason:
    """Map a boto/Bedrock exception to a credential-free unavailability reason.

    Classifies by the structured error code (``response['Error']['Code']`` on a
    botocore ``ClientError``) and by exception class name for connectivity
    errors, without importing botocore. Anything unrecognized degrades to
    ``UNAVAILABLE`` so a failure never escapes as a raw exception. The one read of
    the structured message is the fixed-marker presence test that tells a model
    agreement / AWS Marketplace denial (``MODEL_AGREEMENT_REQUIRED``) from a missing
    IAM grant; the message text itself is never returned, stored, logged or shown,
    and ``str(exc)`` is never used, so no credential/endpoint detail can leak
    (Property 7).
    """
    code = _error_code(exc)

    # Only the authorization-ambiguous codes, and BEFORE the generic denial bucket: a
    # credential-identity failure keeps its code-only ACCESS_DENIED even if its message
    # happened to contain the marker.
    if code in _MODEL_ACCESS_DENIED_CODES and _has_model_agreement_marker(exc):
        return "MODEL_AGREEMENT_REQUIRED"
    if code in _ACCESS_DENIED_CODES:
        return "ACCESS_DENIED"
    if code in {
        "ThrottlingException",
        "ThrottledException",
        "TooManyRequestsException",
        "ServiceQuotaExceededException",
    }:
        return "THROTTLED"

    name = type(exc).__name__
    if (
        name
        in {
            "EndpointConnectionError",
            "ConnectTimeoutError",
            "ReadTimeoutError",
            "ConnectionClosedError",
            "ConnectionError",
        }
        or "Timeout" in name
        or "Connection" in name
    ):
        return "NETWORK"
    return "UNAVAILABLE"


# ---------------------------------------------------------------------------
# AI access verification preflight ("Verify AI access") -- Req 11.13-11.16
# ---------------------------------------------------------------------------

# Reason taxonomy for verify_access. This is deliberately distinct from
# :data:`UnavailableReason`, the AI DBA reply path's: the preflight reports a
# permission/connectivity verdict for the configured model/region, so it uses
# MODEL_NOT_ENABLED (model not enabled / not available in the region) and a
# single catch-all UNKNOWN (network/other) instead of the reply path's
# NETWORK/UNAVAILABLE/INVALID_OUTPUT. The two classifications are kept separate
# on purpose and neither changes the other's behavior. MODEL_AGREEMENT_REQUIRED (the
# model's AWS Marketplace subscription / agreement is missing or failed) is the one
# reason both taxonomies share, produced by the same rule in both classifiers.
AccessCheckReason = Literal[
    "OK",
    "ACCESS_DENIED",
    "MODEL_AGREEMENT_REQUIRED",
    "MODEL_NOT_ENABLED",
    "THROTTLED",
    "UNKNOWN",
]

# A tiny, least-cost preflight request: a one-character prompt capped at a
# single output token, so verification is the cheapest possible InvokeModel
# call (design: "a minimal/least-cost InvokeModel or an equivalent capability
# check"). It is never shown to the user.
_ACCESS_CHECK_PROMPT = "ping"
_ACCESS_CHECK_MAX_TOKENS = 1

# Appended to EVERY passing preflight (configured model or fallback). In a new account a
# third-party model's first calls can succeed while its AWS Marketplace subscription is
# still being set up in the background; if that subscription then fails, later calls are
# denied. That cannot be detected without IAM actions this tool deliberately lacks, so a
# pass is described as what it is -- the model answered just now -- plus this calm,
# info-level note (no action is needed for a normal pass). It promises nothing about the
# wording of a later denial: only one whose message names AWS Marketplace becomes
# MODEL_AGREEMENT_REQUIRED (with the remedy spelled out); another subscription failure,
# e.g. a missing first-time use form, may surface under a different reason.
_FIRST_CALL_CAVEAT = (
    "In a new AWS account a model's first calls can succeed for up to about 15 "
    "minutes while its AWS Marketplace subscription is still being set up in the "
    "background; if AI replies are denied after that, the model's agreement must be "
    "accepted once for the account."
)

# Fixed, credential-free, actionable messages. They never echo the raw boto/
# Bedrock exception text, so no credential or endpoint detail can leak
# (Requirement 11.15 / Property 7), and each failure states the next step.
_ACCESS_CHECK_DETAILS: dict[str, str] = {
    "OK": (
        "Amazon Bedrock access verified: the configured model answered just now in "
        "the configured region. " + _FIRST_CALL_CAVEAT
    ),
    "ACCESS_DENIED": (
        "Access to Amazon Bedrock InvokeModel was denied. Add a scoped "
        "bedrock:InvokeModel permission for the configured model and region, "
        "then retry. If your AWS credentials or session have expired, "
        "re-authenticate first."
    ),
    # A Verify latch also shows this OFF the Connect screen (the AI DBA composer hint,
    # the "What's next?" toast), so it says where the button is.
    "MODEL_AGREEMENT_REQUIRED": (
        "The configured model is not enabled for this AWS account yet: its AWS "
        "Marketplace subscription is missing or did not complete. "
        + _MODEL_AGREEMENT_REMEDY
        + "run Verify AI access on the Connect screen again."
    ),
    "MODEL_NOT_ENABLED": (
        "The configured model is not enabled or not available in the configured "
        "region. Enable the model in BEDROCK_REGION (or correct "
        "BEDROCK_MODEL_ID / BEDROCK_REGION), then retry."
    ),
    "THROTTLED": (
        "Amazon Bedrock throttled the verification request. Retry later."
    ),
    "UNKNOWN": (
        "Amazon Bedrock access could not be verified. Check connectivity and "
        "the BEDROCK_MODEL_ID / BEDROCK_REGION settings, then retry."
    ),
}

# Error codes that mean the model is not enabled / not available for this
# account in the configured region (as opposed to a missing IAM permission).
_MODEL_NOT_ENABLED_CODES = frozenset(
    {
        "ValidationException",
        "ResourceNotFoundException",
        "ModelNotReadyException",
        "ModelNotReady",
        "ModelErrorException",
    }
)


def _fallback_ok_detail(
    *, configured: str, used: str, denied: bool = False
) -> str:
    """Detail for a preflight that passed on a fallback rather than the configured model.

    Says so explicitly rather than reporting a plain success: the operator picked a
    model, and silently answering with a different one would leave them believing
    suggestions come from a model that is not actually reachable. Names both ids and
    the one action that restores their choice.

    ``denied`` distinguishes the two ways the configured model can be unusable while
    another answers. Without it, a model the ACCOUNT is not authorized to invoke was
    described as merely "not enabled", which points at only one of the two fixes; the
    denial wording names both (un-granted model access, or a deployment whose
    ``bedrock:InvokeModel`` scope excludes the model). The phrase "not enabled" is kept
    out of the denial variant so the two paths stay distinguishable.

    A model-agreement denial (``MODEL_AGREEMENT_REQUIRED``) never reaches this: the
    preflight stops on it rather than probing a fallback (see ``verify_access``). Both
    variants end with :data:`_FIRST_CALL_CAVEAT`, because the fallback's pass is as
    point-in-time as any other.

    Contains only the two model ids and fixed prose -- no exception text, endpoint,
    or credential (Requirement 11.15 / Property 7).
    """
    if denied:
        return (
            f"Amazon Bedrock access verified, but using {used} instead of the "
            f"configured {configured}, which this account is not authorized to invoke "
            "in this region. AI assist works and suggestions will come from the "
            f"fallback model. To use {configured}, either grant its model access in the "
            "Amazon Bedrock console for this account and region, or widen this "
            "deployment's bedrock:InvokeModel scope (BedrockModelArns) to cover it, "
            "then verify again. " + _FIRST_CALL_CAVEAT
        )
    return (
        f"Amazon Bedrock access verified, but using {used} instead of the "
        f"configured {configured}, which is not enabled for this account in this "
        "region. AI assist works and suggestions will come from the fallback model. "
        f"To use {configured}, enable its model access in the Amazon Bedrock console "
        "for this account and region, then verify again. " + _FIRST_CALL_CAVEAT
    )


def _classify_access_check_error(exc: BaseException) -> AccessCheckReason:
    """Map a boto/Bedrock exception to a verify_access failure reason.

    This is the preflight's own mapping (Task 17.3), kept separate from
    :func:`_classify_bedrock_error` (the AI DBA reply path): an
    authorization-ambiguous denial (:data:`_MODEL_ACCESS_DENIED_CODES`) whose message
    names AWS Marketplace becomes ``MODEL_AGREEMENT_REQUIRED`` (the same rule as the
    reply path); any other ``AccessDeniedException`` or credential failure
    becomes ``ACCESS_DENIED``; a ``ValidationException`` or a model-not-enabled /
    not-found style error becomes ``MODEL_NOT_ENABLED``; a ``ThrottlingException``
    becomes ``THROTTLED``; and everything else (network errors, anything
    unrecognized) degrades to ``UNKNOWN``. It classifies by the structured
    ``response['Error']['Code']`` plus that one fixed-marker presence test on
    ``response['Error']['Message']``; the message text is never returned, stored,
    logged or shown and ``str(exc)`` is never read, so no credential/endpoint detail
    can leak (Requirement 11.15 / Property 7).
    """
    code = _error_code(exc)

    # Same rule, same order as _classify_bedrock_error (see there).
    if code in _MODEL_ACCESS_DENIED_CODES and _has_model_agreement_marker(exc):
        return "MODEL_AGREEMENT_REQUIRED"
    if code in _ACCESS_DENIED_CODES:
        return "ACCESS_DENIED"
    if code in _MODEL_NOT_ENABLED_CODES:
        return "MODEL_NOT_ENABLED"
    if code in {
        "ThrottlingException",
        "ThrottledException",
        "TooManyRequestsException",
        "ServiceQuotaExceededException",
    }:
        return "THROTTLED"
    return "UNKNOWN"


# ---------------------------------------------------------------------------
# InvokeModel request shaping
# ---------------------------------------------------------------------------


def _build_invoke_body(prompt: str, max_tokens: int) -> str:
    """Serialize an Anthropic-style messages InvokeModel request body.

    Model-specific request shaping is intentionally minimal and localized here
    so it is easy to adjust for other model families. Its one caller, the
    access-check preflight (Task 17.3), passes a tiny ``max_tokens`` so the
    verification request is least-cost.
    """
    body = {
        "anthropic_version": _ANTHROPIC_VERSION,
        "max_tokens": max_tokens,
        # Extended thinking off, as in every AI DBA request (_THINKING_OFF in
        # core/assessment_strategist.py): Claude 5 thinks by default, and thinking
        # tokens count against max_tokens.
        "thinking": {"type": "disabled"},
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
    }
    return json.dumps(body)


# ---------------------------------------------------------------------------
# Bedrock-backed assistant: the "Verify AI access" preflight
# ---------------------------------------------------------------------------


class AiConversionAssistant:
    """Runs the "Verify AI access" Bedrock preflight (Req 11.13-11.16).

    :meth:`verify_access` makes one least-cost ``InvokeModel`` per candidate model
    via the injected client (IAM-based auth; no credentials here) and reports a
    credential-free :class:`~dsql_migrator.core.models.AiAccessCheckResult`. The
    Connect screen calls it through
    :func:`dsql_migrator.ui.ai_assist.run_verify_ai_access`; the AI DBA's replies
    come from ``core/assessment_strategist.py``, not from here.
    """

    def __init__(
        self, config: AiAssistConfig, client: Optional[Any] = None
    ) -> None:
        """Store the AI-assist config and an optional bedrock-runtime client.

        ``client`` is the injection seam: tests pass a fake so they never reach
        AWS. When omitted, a real client is built lazily on first use via
        :func:`build_bedrock_runtime_client` so constructing the assistant
        performs no network call and needs no AWS config.
        """
        self._config = config
        self._client = client

    def _get_client(self) -> Any:
        """Return the bedrock-runtime client, building it lazily if needed."""
        if self._client is None:
            self._client = build_bedrock_runtime_client(self._config)
        return self._client

    def verify_access(self) -> AiAccessCheckResult:
        """Run a lightweight, non-blocking Bedrock access preflight (Req 11.13-11.16).

        Performs one least-cost ``InvokeModel`` per candidate -- the configured
        ``model_id`` first, then (only after a failure a different model can fix)
        the fallback chain -- in the configured ``region``, through the same
        injected/lazy client, so it uses the shared ``boto3.Session`` and optional
        global AWS profile (Task 17.2) -- there is no Bedrock-only credential
        context. On success it returns ``ok=True`` / ``reason="OK"``, whose detail
        says the model answered just now plus :data:`_FIRST_CALL_CAVEAT` (a pass
        in a new account can be temporary); on any failure it maps the exception
        with :func:`_classify_access_check_error` to one of ``ACCESS_DENIED`` /
        ``MODEL_AGREEMENT_REQUIRED`` / ``MODEL_NOT_ENABLED`` / ``THROTTLED`` /
        ``UNKNOWN`` and a fixed, credential-free, actionable ``detail``.

        It never raises into the workflow (every exception is caught) and never
        echoes raw exception text or credentials (Requirement 11.15 / Property
        7), so it does not block the deterministic conversion workflow
        (Requirement 11.16). It only checks connectivity/permission.
        """
        # Try the configured model, then the fallback chain -- but only for the failures a
        # DIFFERENT model can actually fix: MODEL_NOT_ENABLED, and a denial whose code is
        # in _MODEL_ACCESS_DENIED_CODES. That second case is the common one and used to be
        # excluded: a `global.` inference profile can be ACTIVE in a region while this
        # account lacks access to it, and Bedrock reports that as AccessDeniedException,
        # so the chain was unreachable in precisely the situation it exists for and the
        # operator was told to fix an IAM policy that was not the problem.
        #
        # A credential-identity denial (expired token, bad signature) and THROTTLED /
        # UNKNOWN (network) are properties of the caller or the network, so they still
        # return immediately -- retrying another model would only add latency and bury the
        # real cause.
        #
        # MODEL_AGREEMENT_REQUIRED (the configured model's AWS Marketplace subscription is
        # missing or failed) also returns immediately, for the configured model: its
        # prerequisites -- the caller's aws-marketplace:* permissions, Anthropic's
        # first-time use form, a valid payment method -- are caller- or account-wide, so a
        # fallback that "answers" in a new account is most likely inside its OWN
        # background-subscription window: a second false pass, which the Connect screen
        # would then adopt as the configured model. Probing it would also start a
        # subscription attempt for a model the operator never chose. Classification is by
        # error code plus the one fixed-marker presence test; no message text is kept.
        first_reason: Optional[AccessCheckReason] = None
        for candidate in self._access_check_candidates():
            try:
                self._get_client().invoke_model(
                    modelId=candidate,
                    body=_build_invoke_body(
                        _ACCESS_CHECK_PROMPT, max_tokens=_ACCESS_CHECK_MAX_TOKENS
                    ),
                    contentType="application/json",
                    accept="application/json",
                )
            except Exception as exc:  # noqa: BLE001 - never raise into the workflow
                reason = _classify_access_check_error(exc)
                if first_reason is None:
                    first_reason = reason
                if reason == "MODEL_NOT_ENABLED" or (
                    reason == "ACCESS_DENIED"
                    and _error_code(exc) in _MODEL_ACCESS_DENIED_CODES
                ):
                    continue
                # Report the reason for the model the OPERATOR configured, not a
                # fallback's -- an IAM gap on the fallback is not their problem.
                return AiAccessCheckResult(
                    ok=False,
                    reason=first_reason,
                    detail=_ACCESS_CHECK_DETAILS[first_reason],
                    model_id=self._config.model_id,
                    region=self._config.region,
                )
            # ``model_id`` echoes what actually answered, so the screen never claims
            # a model the suggestions won't come from.
            return AiAccessCheckResult(
                ok=True,
                reason="OK",
                detail=(
                    _ACCESS_CHECK_DETAILS["OK"]
                    if candidate == self._config.model_id
                    else _fallback_ok_detail(
                        configured=self._config.model_id,
                        used=candidate,
                        # A fallback reached after a DENIAL needs different remediation
                        # from one reached after "not enabled": the configured model may
                        # be un-granted for the account OR outside this deployment's
                        # bedrock:InvokeModel scope, and the operator cannot tell which
                        # from a sentence that only mentions model access.
                        denied=first_reason == "ACCESS_DENIED",
                    )
                ),
                model_id=candidate,
                region=self._config.region,
            )

        # Every candidate failed in a way a different model could have fixed.
        reason = first_reason or "MODEL_NOT_ENABLED"
        return AiAccessCheckResult(
            ok=False,
            reason=reason,
            detail=_ACCESS_CHECK_DETAILS[reason],
            model_id=self._config.model_id,
            region=self._config.region,
        )

    def _access_check_candidates(self) -> list[str]:
        """Return the configured model followed by any distinct fallbacks.

        The configured model is always tried first, and a fallback equal to it is
        dropped so the preflight never pays for the same InvokeModel twice.
        """
        from dsql_migrator.ui.ai_assist import BEDROCK_MODEL_FALLBACKS

        candidates = [self._config.model_id]
        for fallback in BEDROCK_MODEL_FALLBACKS:
            if fallback not in candidates:
                candidates.append(fallback)
        return candidates


__all__ = [
    "BEDROCK_RUNTIME_SERVICE",
    "BotoSessionLike",
    "SqlValidation",
    "UnavailableReason",
    "AccessCheckReason",
    "AiAssistUnavailableError",
    "AiConversionAssistant",
    "AiAccessCheckResult",
    "build_bedrock_runtime_client",
    "validate_suggested_sql",
]
