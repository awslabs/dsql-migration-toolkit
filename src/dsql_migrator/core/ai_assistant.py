# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI-assisted conversion: config/env loading and Bedrock-runtime client seam.

Task 16.1 scope. The AI data models (:class:`~dsql_migrator.core.models.AiAssistConfig`
and :class:`~dsql_migrator.core.models.AiConversionSuggestion`) live in
``core/models.py``; this module adds the two remaining pieces of 16.1:

- loading the ``BEDROCK_MODEL_ID`` (default ``global.anthropic.claude-sonnet-4-6``) and
  ``BEDROCK_REGION`` settings from the environment into an ``AiAssistConfig``
  (Requirements 11.3, 11.4), and
- constructing the ``bedrock-runtime`` ``boto3`` client used to call Bedrock
  (Requirement 11.9).

Task 16.2 adds the concrete :class:`AiConversionAssistant` and its two
suggestion methods (``suggest_schema_conversion`` / ``suggest_data_transformation``):
they ground a prompt with the already-extracted source DDL/types, the DSQL
constraints, and the deterministic conversion result, call bedrock-runtime
``InvokeModel`` via the injected client, and return a reviewable
:class:`~dsql_migrator.core.models.AiConversionSuggestion` carrying provenance
(``model_id``) and status ``PENDING_REVIEW`` (Requirements 11.5, 11.6).

Task 16.3 hardens the untrusted model output and adds graceful degradation:

- :func:`_extract_suggestion_text` now treats the InvokeModel response as
  untrusted data and never raises into the workflow on a malformed/empty/
  non-JSON body, missing/unexpected fields, or oversized output; it returns the
  text it can safely recover (capped at :data:`MAX_SUGGESTION_CHARS`) or an empty
  string (Requirement 11.8).
- :func:`validate_suggested_sql` blocks forbidden statements (e.g. data
  mutation, ``DROP``, role/grant, transaction control, ``COPY``) so an unsafe
  suggestion is never produced as APPLY-ready (Requirement 11.8 / Property 13).
  A schema/data suggestion that fails validation is returned flagged
  ``REJECTED`` with a clear reason rather than as a usable suggestion.
- Bedrock failures (permission / throttle / network / unavailable / unparseable
  output) are mapped to :class:`AiAssistUnavailableError`. The
  ``try_suggest_*`` methods catch it and return an :class:`AiSuggestionOutcome`
  so the caller (converter routing, Task 16.4) can keep the deterministic result
  and its ``MANUAL``/``UNSUPPORTED`` flag and continue the workflow with a clear,
  credential-free message (Requirement 11.10).

Nothing is ever auto-applied: every generated suggestion stays ``PENDING_REVIEW``
(or ``REJECTED`` when it fails validation) and only an explicit human approval,
gated in :mod:`dsql_migrator.ui.ai_assist`, makes it eligible for the Schema
Applier path (Property 13). Cost/latency guidance (Requirement 11.12) is exposed
by the UI (Connect and Schema Conversion screens); no engine-level duplicate is
added. Converter routing and ``verify_access`` are implemented in later subtasks
(16.4, 17.3).

Credential handling (Requirement 11.9 / Property 7):

- The client uses IAM-based authentication via the standard AWS credential
  chain; no credentials are hardcoded.
- The ``boto3`` session/client is injectable. The shared ``boto3.Session``
  (``core/aws_session.build_session``), which honors an optional global AWS
  profile, is used to build the client; callers may also pass a session or a
  fake in explicitly without changing call sites, and unit tests supply a fake
  so they never reach AWS. Constructing the client performs no network call.

The ``BEDROCK_MODEL_ID`` / ``BEDROCK_REGION`` keys are intentionally *not*
prefixed with ``DSQL_MIGRATOR_``: they are documented as those exact names in
the requirements/design and ``BEDROCK_REGION`` is a Bedrock-specific setting
distinct from the global AWS profile/region.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Literal, Mapping, Optional, Sequence

from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.converter import map_mysql_type
from dsql_migrator.core.models import AiAccessCheckResult, AiAssistConfig, AiConversionSuggestion

# Documented config keys (NOT DSQL_MIGRATOR_-prefixed): Requirements 11.3, 11.4.
ENV_BEDROCK_MODEL_ID = "BEDROCK_MODEL_ID"
ENV_BEDROCK_REGION = "BEDROCK_REGION"

# The boto3 service name for the Bedrock runtime (InvokeModel) client.
BEDROCK_RUNTIME_SERVICE = "bedrock-runtime"

# Anthropic-style InvokeModel request shaping (the default model is an
# Anthropic Claude model). Kept minimal and localized so it is easy to adjust
# for other model families; the response is treated as text we extract from.
_ANTHROPIC_VERSION = "bedrock-2023-05-31"
_MAX_TOKENS = 1024

# Upper bound on how much model output we accept. The output is untrusted data
# (Requirement 11.8); an oversized body is truncated rather than processed in
# full so a single suggestion cannot exhaust memory or hide content past a huge
# prefix. A human still reviews whatever survives before anything is applied.
MAX_SUGGESTION_CHARS = 20_000


def _read(env: Mapping[str, str], name: str) -> Optional[str]:
    """Read an environment variable, returning None when unset or blank."""
    raw = env.get(name)
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def load_ai_assist_config(env: Optional[Mapping[str, str]] = None) -> AiAssistConfig:
    """Load AI-assist settings from ``BEDROCK_MODEL_ID`` / ``BEDROCK_REGION``.

    ``model_id`` defaults to ``global.anthropic.claude-sonnet-4-6`` (the model's own
    default, kept as the single source of truth) when ``BEDROCK_MODEL_ID`` is unset or blank,
    and ``region`` comes from ``BEDROCK_REGION`` (``None`` when unset). AI assist
    is opt-in, so ``enabled`` is always ``False`` here; the user turns it on in
    the UI (Requirements 11.1, 11.2). No credential value is ever read.
    """
    source = os.environ if env is None else env

    values: dict[str, object] = {"enabled": False}
    if (model_id := _read(source, ENV_BEDROCK_MODEL_ID)) is not None:
        values["model_id"] = model_id
    if (region := _read(source, ENV_BEDROCK_REGION)) is not None:
        values["region"] = region

    return AiAssistConfig(**values)


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

# Statement-leading keywords that must never appear in an AI conversion
# suggestion that could be applied to the target. A single-object conversion
# suggestion is expected to be object DDL (CREATE TABLE/INDEX/VIEW/SEQUENCE/TYPE,
# CREATE FUNCTION for a stored-procedure reimplementation, or a value expression
# for a data transformation). The following are out of that scope and dangerous
# if auto-applied, so they are blocked: data mutation (DELETE/UPDATE/INSERT/
# TRUNCATE), object/database removal (DROP), privilege/role changes
# (GRANT/REVOKE), transaction/session control (BEGIN/START/COMMIT/ROLLBACK/
# SAVEPOINT/SET), bulk/file I/O (COPY), and arbitrary procedural execution
# (CALL/DO). The set is intentionally small and documented; the human review
# gate (Property 13) is the primary control and this validation is
# defense-in-depth so unsafe output is never produced as APPLY-ready.
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
    """Outcome of validating an AI suggestion's SQL/expression (Req 11.8).

    ``is_safe`` is ``True`` only when no forbidden statement was found; when it is
    ``False``, ``reason`` is a short, human-readable explanation of the blocked
    construct so the suggestion can be surfaced as flagged for review rather than
    applied.
    """

    is_safe: bool
    reason: str = ""


def _strip_sql_comments(text: str) -> str:
    """Remove block and line comments so they cannot hide forbidden statements."""
    without_block = _COMMENT_PATTERN.sub(" ", text)
    return _LINE_COMMENT_PATTERN.sub(" ", without_block)


def validate_suggested_sql(text: str) -> SqlValidation:
    """Validate untrusted model output for forbidden statements (Req 11.8).

    The model output is untrusted data, so before a suggestion can ever be
    applied it is scanned for statements outside the scope of a single-object
    conversion suggestion. Each ``;``-separated statement is checked by its
    leading keyword against :data:`_FORBIDDEN_LEADING_KEYWORDS` (plus the
    ``CREATE``/``ALTER`` target guard), which also catches a dangerous statement
    appended after a benign one (statement-injection). This is a deliberately
    small, documented denylist used as defense-in-depth behind the human review
    gate (Property 13); it is not a full SQL parser.

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
# Graceful degradation: typed unavailability + outcome (Req 11.10)
# ---------------------------------------------------------------------------

# Why the AI suggestion could not be produced. "OK" means a suggestion is
# available; the others mean the deterministic result and its MANUAL/UNSUPPORTED
# flag must be kept while the workflow continues (Requirement 11.10).
UnavailableReason = Literal[
    "ACCESS_DENIED", "THROTTLED", "NETWORK", "UNAVAILABLE", "INVALID_OUTPUT"
]

# Fixed, credential-free, actionable messages. They never echo the raw boto/
# Bedrock exception text so no credential or endpoint detail can leak (Property
# 7), and each one states that the deterministic result/flag is retained.
_UNAVAILABLE_DETAILS: dict[str, str] = {
    "ACCESS_DENIED": (
        "AI assist is unavailable: access to Amazon Bedrock InvokeModel was "
        "denied. The deterministic conversion result and its MANUAL/UNSUPPORTED "
        "flag are kept. Grant a scoped bedrock:InvokeModel permission to enable "
        "AI suggestions -- or, if your AWS credentials or session have expired, "
        "re-authenticate and retry."
    ),
    "THROTTLED": (
        "AI assist is temporarily unavailable: Amazon Bedrock throttled the "
        "request. The deterministic conversion result and its MANUAL/UNSUPPORTED "
        "flag are kept. Retry later to get an AI suggestion."
    ),
    "NETWORK": (
        "AI assist is unavailable: Amazon Bedrock could not be reached (network "
        "error). The deterministic conversion result and its MANUAL/UNSUPPORTED "
        "flag are kept. Check connectivity to the Bedrock endpoint and retry."
    ),
    "UNAVAILABLE": (
        "AI assist is unavailable: the Amazon Bedrock request failed. The "
        "deterministic conversion result and its MANUAL/UNSUPPORTED flag are "
        "kept. The workflow continues without an AI suggestion."
    ),
    "INVALID_OUTPUT": (
        "AI assist could not produce a usable result this time: the model's "
        "reply could not be parsed. This is optional and non-blocking -- the "
        "deterministic result above is complete and authoritative. Re-run to try "
        "again."
    ),
}


class AiAssistUnavailableError(RuntimeError):
    """Raised when an AI suggestion cannot be produced (Requirement 11.10).

    This is a *typed, catchable* signal -- not a raw boto/Bedrock exception -- so
    a caller can map it to "AI unavailable; keep the deterministic result and the
    MANUAL/UNSUPPORTED flag" without breaking the workflow. ``reason`` is one of
    :data:`UnavailableReason` and ``detail`` is a fixed, credential-free message
    (Property 7). The ``try_suggest_*`` methods catch it and return an
    :class:`AiSuggestionOutcome`; the existing UI also degrades gracefully when
    it surfaces.
    """

    def __init__(self, reason: UnavailableReason) -> None:
        self.reason: UnavailableReason = reason
        self.detail: str = _UNAVAILABLE_DETAILS[reason]
        super().__init__(self.detail)


@dataclass(frozen=True)
class AiSuggestionOutcome:
    """A suggestion-or-failure result that never breaks the workflow (Req 11.10).

    This is the representation the converter routing (Task 16.4) consumes: call
    :meth:`AiConversionAssistant.try_suggest_schema_conversion` /
    :meth:`AiConversionAssistant.try_suggest_data_transformation`, then:

    - if :attr:`available` is ``True``, offer :attr:`suggestion` for human review
      (it may itself be flagged ``REJECTED`` when its SQL failed validation), and
    - if :attr:`available` is ``False``, keep the deterministic result and the
      object's ``MANUAL``/``UNSUPPORTED`` flag and show :attr:`detail` (a clear,
      credential-free message). :attr:`reason` distinguishes the cause.

    ``reason`` is ``"OK"`` exactly when :attr:`available` is ``True``.
    """

    available: bool
    reason: Literal["OK", "ACCESS_DENIED", "THROTTLED", "NETWORK", "UNAVAILABLE", "INVALID_OUTPUT"]
    detail: str
    suggestion: Optional[AiConversionSuggestion] = None

    @classmethod
    def ok(cls, suggestion: AiConversionSuggestion) -> "AiSuggestionOutcome":
        """Build a successful outcome carrying a reviewable ``suggestion``."""
        return cls(available=True, reason="OK", detail="", suggestion=suggestion)

    @classmethod
    def unavailable(cls, error: "AiAssistUnavailableError") -> "AiSuggestionOutcome":
        """Build a failed outcome from a typed unavailability error."""
        return cls(available=False, reason=error.reason, detail=error.detail)


# IAM / SigV4 auth failures: either no permission, or the credentials/session
# have expired or are otherwise invalid. Both map to ACCESS_DENIED -- the
# actionable message covers granting the permission AND re-authenticating -- so
# an expired-token error no longer degrades to a vague generic "unavailable".
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


def _classify_bedrock_error(exc: BaseException) -> UnavailableReason:
    """Map a boto/Bedrock exception to a credential-free unavailability reason.

    Classifies by the structured error code (``response['Error']['Code']`` on a
    botocore ``ClientError``) and by exception class name for connectivity
    errors, without importing botocore. Anything unrecognized degrades to
    ``UNAVAILABLE`` so a failure never escapes as a raw exception. The raw
    exception text is intentionally not used, so no credential/endpoint detail
    can leak (Property 7).
    """
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error")
        if isinstance(error, Mapping):
            code = str(error.get("Code") or "")

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

# Reason taxonomy for verify_access. This is deliberately distinct from the
# suggestion-path :data:`UnavailableReason` (16.3): the preflight reports a
# permission/connectivity verdict for the configured model/region, so it uses
# MODEL_NOT_ENABLED (model not enabled / not available in the region) and a
# single catch-all UNKNOWN (network/other) instead of the suggestion path's
# NETWORK/UNAVAILABLE/INVALID_OUTPUT. The two classifications are kept separate
# on purpose and neither changes the other's behavior.
AccessCheckReason = Literal[
    "OK", "ACCESS_DENIED", "MODEL_NOT_ENABLED", "THROTTLED", "UNKNOWN"
]

# A tiny, least-cost preflight request: a one-character prompt capped at a
# single output token, so verification is the cheapest possible InvokeModel
# call (design: "a minimal/least-cost InvokeModel or an equivalent capability
# check"). It is never shown to the user.
_ACCESS_CHECK_PROMPT = "ping"
_ACCESS_CHECK_MAX_TOKENS = 1

# Fixed, credential-free, actionable messages. They never echo the raw boto/
# Bedrock exception text, so no credential or endpoint detail can leak
# (Requirement 11.15 / Property 7), and each failure states the next step.
_ACCESS_CHECK_DETAILS: dict[str, str] = {
    "OK": (
        "Amazon Bedrock access verified: the configured model is reachable in "
        "the configured region."
    ),
    "ACCESS_DENIED": (
        "Access to Amazon Bedrock InvokeModel was denied. Add a scoped "
        "bedrock:InvokeModel permission for the configured model and region, "
        "then retry. If your AWS credentials or session have expired, "
        "re-authenticate first."
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


def _classify_access_check_error(exc: BaseException) -> AccessCheckReason:
    """Map a boto/Bedrock exception to a verify_access failure reason.

    This is the preflight's own mapping (Task 17.3), kept separate from
    :func:`_classify_bedrock_error` (the suggestion path, 16.3): an
    ``AccessDeniedException`` becomes ``ACCESS_DENIED``; a ``ValidationException``
    or a model-not-enabled / not-found style error becomes ``MODEL_NOT_ENABLED``;
    a ``ThrottlingException`` becomes ``THROTTLED``; and everything else
    (network errors, anything unrecognized) degrades to ``UNKNOWN``. It
    classifies by the structured ``response['Error']['Code']`` and never reads
    the raw exception text, so no credential/endpoint detail can leak
    (Requirement 11.15 / Property 7).
    """
    code = ""
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error")
        if isinstance(error, Mapping):
            code = str(error.get("Code") or "")

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
# Prompt grounding (Requirements 11.5, 11.6)
# ---------------------------------------------------------------------------


def _canonical_dsql_type(source_type: str) -> str:
    """Return the SchemaConverter export-time DSQL type for ``source_type``.

    Reuses :func:`dsql_migrator.core.converter.map_mysql_type` (the canonical
    export-time type mapping) so a data/value transformation suggestion is
    grounded to be consistent with how the schema export maps the same type
    (Requirement 11.6), rather than re-deriving the mapping here. Any
    non-lossless mapping note is appended so the model sees the same caveat the
    deterministic converter records. Best-effort: if the type cannot be parsed,
    the canonical mapping is reported as unavailable instead of raising.
    """
    try:
        target_type, warning = map_mysql_type(source_type)
    except ValueError:
        return "(canonical mapping unavailable: source type could not be parsed)"
    if warning is not None:
        return f"{target_type} (note: {warning.message})"
    return target_type


def _build_schema_prompt(
    object_name: str,
    source_ddl: str,
    deterministic_result: Optional[str],
    dsql_constraints: str,
) -> str:
    """Build the grounded prompt for a schema (DDL) conversion suggestion.

    The prompt is grounded with the already-extracted source DDL, the DSQL
    constraints supplied by the caller (FK unsupported, ``C`` collation, async
    indexes, PK required, transaction limits, etc.), and the deterministic
    converter's result/reason, so the model augments (not replaces) the
    deterministic path (Requirement 11.5).
    """
    return (
        "You are assisting a MySQL -> Amazon Aurora DSQL (PostgreSQL-compatible) "
        "schema migration. The deterministic (sqlglot) converter flagged the "
        "following object as MANUAL/UNSUPPORTED. Suggest a DSQL-compatible "
        "conversion (DDL, or an application-level reimplementation when DSQL "
        "cannot express the construct) for this object only.\n\n"
        f"Object name:\n{object_name}\n\n"
        f"Source MySQL DDL:\n{source_ddl}\n\n"
        f"Aurora DSQL constraints to respect:\n{dsql_constraints}\n\n"
        "Deterministic converter result / reason:\n"
        f"{deterministic_result or '(none provided)'}\n\n"
        "Response format (IMPORTANT):\n"
        "- Put the executable Aurora DSQL DDL in a SINGLE ```sql fenced code "
        "block, with one statement per statement terminated by ';'. The code "
        "block must contain ONLY runnable SQL -- no prose, headings, tables, or "
        "comments other than SQL comments.\n"
        "- Put any explanation, caveats, or notes as brief prose OUTSIDE the "
        "code block.\n"
        "- If DSQL cannot express the construct, put a single SQL comment "
        "stating the manual step inside the code block."
    )


def _build_data_prompt(
    object_name: str,
    source_type: str,
    sample_values: Sequence[str],
    deterministic_mapping: Optional[str],
) -> str:
    """Build the grounded prompt for a data/value transformation suggestion.

    The prompt is grounded with the canonical export-time type mapping from the
    SchemaConverter (:func:`_canonical_dsql_type`) so the value/type suggestion
    stays consistent with how the schema export maps the same source type
    (Requirement 11.6), alongside the source type, sample values, and the
    deterministic mapping/reason.
    """
    canonical_target = _canonical_dsql_type(source_type)
    samples = (
        "\n".join(f"- {value}" for value in sample_values)
        if sample_values
        else "(none provided)"
    )
    return (
        "You are assisting a MySQL -> Amazon Aurora DSQL (PostgreSQL-compatible) "
        "data/value migration. The deterministic (sqlglot) converter flagged the "
        "following value/type conversion as MANUAL/UNSUPPORTED. Suggest a value "
        "transformation expression or a semantic target type for this column "
        "only.\n\n"
        f"Object name:\n{object_name}\n\n"
        f"Source MySQL type:\n{source_type}\n\n"
        "Canonical export-time type mapping (SchemaConverter) you MUST stay "
        f"consistent with:\n{canonical_target}\n\n"
        f"Sample source values:\n{samples}\n\n"
        "Deterministic mapping / reason:\n"
        f"{deterministic_mapping or '(none provided)'}\n"
    )


def _build_invoke_body(prompt: str, max_tokens: int = _MAX_TOKENS) -> str:
    """Serialize an Anthropic-style messages InvokeModel request body.

    Model-specific request shaping is intentionally minimal and localized here
    so it is easy to adjust for other model families (Task 16.3+ may broaden it).
    ``max_tokens`` defaults to :data:`_MAX_TOKENS`; the access-check preflight
    (Task 17.3) passes a tiny value so the verification request is least-cost.
    """
    body = {
        "anthropic_version": _ANTHROPIC_VERSION,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
    }
    return json.dumps(body)


# Matches a Markdown fenced code block, capturing the (optional) language tag
# and the body, so an LLM that wraps DDL in ```sql ... ``` can be split into the
# executable SQL (for the editor/apply) and the surrounding prose (rationale).
_FENCE_RE = re.compile(r"```[ \t]*([A-Za-z0-9_+-]*)[ \t]*\r?\n(.*?)```", re.DOTALL)
_SQL_FENCE_LANGS = {"", "sql", "postgres", "postgresql", "plpgsql", "pgsql", "ddl"}


def _split_sql_and_rationale(text: str) -> tuple[str, str]:
    """Split a model reply into executable SQL and the surrounding prose.

    Models often answer with a Markdown report: prose + one or more ```sql
    fenced blocks. The editable/apply field must hold only the executable DDL,
    so this returns ``(sql, rationale)`` where ``sql`` is the concatenation of
    the SQL code blocks (preferring ``sql``-tagged ones) and ``rationale`` is the
    text with the code fences removed. When the reply has no code fence at all,
    ``sql`` is the whole text (best-effort fallback) and ``rationale`` is empty.
    """
    text = text or ""
    blocks = _FENCE_RE.findall(text)
    if not blocks:
        return text.strip(), ""
    sql_blocks = [
        body for lang, body in blocks if lang.lower() in _SQL_FENCE_LANGS
    ]
    chosen = sql_blocks if sql_blocks else [body for _lang, body in blocks]
    sql = "\n\n".join(body.strip() for body in chosen).strip()
    rationale = _FENCE_RE.sub("", text).strip()
    return sql, rationale


def _extract_suggestion_text(response: Mapping[str, Any]) -> str:
    """Safely extract suggestion text from an untrusted InvokeModel response.

    The response is untrusted data (Requirement 11.8): this never raises into the
    workflow on a malformed/empty/non-JSON body, missing/unexpected fields, or an
    oversized payload. It defensively reads the Anthropic-style ``content`` text
    blocks and returns whatever text it can recover, capped at
    :data:`MAX_SUGGESTION_CHARS`; if nothing usable can be parsed it returns an
    empty string and the caller treats that as no usable output (``INVALID_OUTPUT``).
    """
    try:
        raw = response.get("body") if isinstance(response, Mapping) else None
        if raw is None:
            return ""
        payload = raw.read() if hasattr(raw, "read") else raw
        data = json.loads(payload)
    except (AttributeError, ValueError, TypeError, json.JSONDecodeError):
        # Non-Mapping response, unreadable body, non-JSON, or non-decodable bytes:
        # treat as no usable output rather than raising into the workflow.
        return ""

    if not isinstance(data, Mapping):
        return ""
    content = data.get("content")
    if not isinstance(content, Sequence) or isinstance(content, (str, bytes)):
        return ""

    text_parts = [
        block.get("text", "")
        for block in content
        if isinstance(block, Mapping)
        and block.get("type") == "text"
        and isinstance(block.get("text"), str)
    ]
    return "".join(text_parts).strip()[:MAX_SUGGESTION_CHARS]


# ---------------------------------------------------------------------------
# AI Conversion Assistant (concrete Bedrock-backed implementation)
# ---------------------------------------------------------------------------


class AiConversionAssistant:
    """Generates reviewable Bedrock conversion suggestions (Req 11.5, 11.6).

    Concrete implementation of the design's ``AiConversionAssistant`` (the
    ``ui.ai_assist`` module declares a matching Protocol used purely as a seam).
    It augments — never replaces — the deterministic (sqlglot) path and is only
    meant to be called for items the deterministic converter flagged
    ``MANUAL``/``UNSUPPORTED`` (the converter routing in Task 16.4 enforces the
    trigger condition; this class assumes it).

    Each method grounds a prompt with the already-extracted source DDL/types,
    DSQL constraints, and the deterministic result, calls bedrock-runtime
    ``InvokeModel`` via the injected client (IAM-based auth; no credentials
    here), and returns an :class:`AiConversionSuggestion` carrying ``model_id``
    provenance and status ``PENDING_REVIEW`` — nothing is ever auto-applied
    (Property 13). The source DB is never written to: inputs are pre-extracted
    (Property 1).
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

    def _invoke(self, prompt: str) -> str:
        """Call bedrock-runtime ``InvokeModel`` and return the suggestion text.

        Uses ``config.model_id`` as the ``modelId`` for provenance and IAM-based
        auth via the injected/lazy client. Any boto/Bedrock failure is mapped to
        a typed :class:`AiAssistUnavailableError` (credential-free) instead of a
        raw exception, and empty/unparseable output becomes ``INVALID_OUTPUT``,
        so the caller can keep the deterministic result and continue the workflow
        (Requirement 11.10). The response body is treated as untrusted data and
        parsed safely (Requirement 11.8).
        """
        try:
            response = self._get_client().invoke_model(
                modelId=self._config.model_id,
                body=_build_invoke_body(prompt),
                contentType="application/json",
                accept="application/json",
            )
        except AiAssistUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001 - mapped to a typed, safe signal
            raise AiAssistUnavailableError(_classify_bedrock_error(exc)) from None

        text = _extract_suggestion_text(response)
        if not text:
            raise AiAssistUnavailableError("INVALID_OUTPUT")
        return text

    def verify_access(self) -> AiAccessCheckResult:
        """Run a lightweight, non-blocking Bedrock access preflight (Req 11.13-11.16).

        Performs a single least-cost ``InvokeModel`` against the configured
        ``model_id`` / ``region`` through the same injected/lazy client, so it
        uses the shared ``boto3.Session`` and optional global AWS profile (Task
        17.2) -- there is no Bedrock-only credential context. On success it
        returns ``ok=True`` / ``reason="OK"``; on any failure it maps the
        exception with :func:`_classify_access_check_error` to one of
        ``ACCESS_DENIED`` / ``MODEL_NOT_ENABLED`` / ``THROTTLED`` / ``UNKNOWN``
        and a fixed, credential-free, actionable ``detail``.

        It never raises into the workflow (every exception is caught) and never
        echoes raw exception text or credentials (Requirement 11.15 / Property
        7), so it does not block the deterministic conversion workflow
        (Requirement 11.16). It only checks connectivity/permission and does not
        change the AI suggestion review gate (Property 13).
        """
        try:
            self._get_client().invoke_model(
                modelId=self._config.model_id,
                body=_build_invoke_body(
                    _ACCESS_CHECK_PROMPT, max_tokens=_ACCESS_CHECK_MAX_TOKENS
                ),
                contentType="application/json",
                accept="application/json",
            )
        except Exception as exc:  # noqa: BLE001 - never raise into the workflow
            reason = _classify_access_check_error(exc)
            return AiAccessCheckResult(
                ok=False,
                reason=reason,
                detail=_ACCESS_CHECK_DETAILS[reason],
                model_id=self._config.model_id,
                region=self._config.region,
            )
        return AiAccessCheckResult(
            ok=True,
            reason="OK",
            detail=_ACCESS_CHECK_DETAILS["OK"],
            model_id=self._config.model_id,
            region=self._config.region,
        )

    def _build_suggestion(
        self,
        *,
        object_name: str,
        kind: Literal["SCHEMA", "DATA", "QUERY"],
        suggested: str,
        rationale: str,
    ) -> AiConversionSuggestion:
        """Build a reviewable suggestion, flagging unsafe output as REJECTED.

        The model output is untrusted: it is validated for forbidden statements
        (:func:`validate_suggested_sql`). Safe output is returned
        ``PENDING_REVIEW``; unsafe output is returned flagged ``REJECTED`` with
        the blocking reason prepended to the rationale, so it is surfaced for
        review and is never produced as an APPLY-ready suggestion (Requirement
        11.8 / Property 13). Either way the suggestion is never auto-applied.
        """
        validation = validate_suggested_sql(suggested)
        if validation.is_safe:
            return AiConversionSuggestion(
                object_name=object_name,
                kind=kind,
                suggested_sql_or_expr=suggested,
                rationale=rationale,
                model_id=self._config.model_id,
            )
        return AiConversionSuggestion(
            object_name=object_name,
            kind=kind,
            suggested_sql_or_expr=suggested,
            rationale=(
                f"Blocked by output validation: {validation.reason}. "
                "Treated as untrusted output and flagged for review; it will not "
                "be applied. Edit it to a safe conversion before approving."
            ),
            model_id=self._config.model_id,
            status="REJECTED",
        )

    def suggest_schema_conversion(
        self,
        object_name: str,
        source_ddl: str,
        deterministic_result: Optional[str],
        dsql_constraints: str,
    ) -> AiConversionSuggestion:
        """Suggest a DSQL schema conversion for one MANUAL/UNSUPPORTED object.

        Grounds the prompt with the source DDL, DSQL constraints, and the
        deterministic result (Requirement 11.5), then returns a reviewable
        suggestion (``kind="SCHEMA"``, status ``PENDING_REVIEW``) with
        ``model_id`` provenance. The suggestion is never auto-applied.
        """
        prompt = _build_schema_prompt(
            object_name, source_ddl, deterministic_result, dsql_constraints
        )
        raw = self._invoke(prompt)
        # Keep the editable/apply field to executable DDL only; surface the
        # model's explanation/caveats as the rationale (shown for review).
        sql, prose = _split_sql_and_rationale(raw)
        default_rationale = (
            "Bedrock suggestion grounded on the source DDL, Aurora DSQL "
            "constraints, and the deterministic converter result; augments "
            "the deterministic path for a MANUAL/UNSUPPORTED object. Review "
            "and approve before applying."
        )
        return self._build_suggestion(
            object_name=object_name,
            kind="SCHEMA",
            suggested=sql or raw,
            rationale=(prose or default_rationale) if sql else default_rationale,
        )

    def suggest_data_transformation(
        self,
        object_name: str,
        source_type: str,
        sample_values: list[str],
        deterministic_mapping: Optional[str],
    ) -> AiConversionSuggestion:
        """Suggest a value/type transformation for a MANUAL/UNSUPPORTED case.

        Grounds the prompt with the canonical SchemaConverter export-time type
        mapping for ``source_type`` so the suggestion stays consistent with the
        schema export (Requirement 11.6), then returns a reviewable suggestion
        (``kind="DATA"``, status ``PENDING_REVIEW``) with ``model_id``
        provenance. The suggestion is never auto-applied.
        """
        prompt = _build_data_prompt(
            object_name, source_type, sample_values, deterministic_mapping
        )
        suggested = self._invoke(prompt)
        return self._build_suggestion(
            object_name=object_name,
            kind="DATA",
            suggested=suggested,
            rationale=(
                "Bedrock suggestion grounded on the SchemaConverter export-time "
                "type mapping, sample values, and the deterministic mapping; "
                "kept consistent with the export-time type mapping. Review and "
                "approve before applying."
            ),
        )

    def try_suggest_schema_conversion(
        self,
        object_name: str,
        source_ddl: str,
        deterministic_result: Optional[str],
        dsql_constraints: str,
    ) -> AiSuggestionOutcome:
        """Graceful-degradation wrapper around :meth:`suggest_schema_conversion`.

        Never raises on a Bedrock failure or unparseable output: it returns an
        :class:`AiSuggestionOutcome`. This is the representation the converter
        routing (Task 16.4) consumes -- when the outcome is not ``available`` the
        caller keeps the deterministic result and the object's
        ``MANUAL``/``UNSUPPORTED`` flag and surfaces ``detail`` (Requirement
        11.10).
        """
        try:
            suggestion = self.suggest_schema_conversion(
                object_name, source_ddl, deterministic_result, dsql_constraints
            )
        except AiAssistUnavailableError as error:
            return AiSuggestionOutcome.unavailable(error)
        return AiSuggestionOutcome.ok(suggestion)

    def try_suggest_data_transformation(
        self,
        object_name: str,
        source_type: str,
        sample_values: list[str],
        deterministic_mapping: Optional[str],
    ) -> AiSuggestionOutcome:
        """Graceful-degradation wrapper around :meth:`suggest_data_transformation`.

        Mirrors :meth:`try_suggest_schema_conversion`: it never raises on a
        Bedrock failure or unparseable output, returning an
        :class:`AiSuggestionOutcome` so the workflow continues with the
        deterministic result retained (Requirement 11.10).
        """
        try:
            suggestion = self.suggest_data_transformation(
                object_name, source_type, sample_values, deterministic_mapping
            )
        except AiAssistUnavailableError as error:
            return AiSuggestionOutcome.unavailable(error)
        return AiSuggestionOutcome.ok(suggestion)


__all__ = [
    "ENV_BEDROCK_MODEL_ID",
    "ENV_BEDROCK_REGION",
    "BEDROCK_RUNTIME_SERVICE",
    "MAX_SUGGESTION_CHARS",
    "BotoSessionLike",
    "SqlValidation",
    "UnavailableReason",
    "AccessCheckReason",
    "AiAssistUnavailableError",
    "AiSuggestionOutcome",
    "AiConversionAssistant",
    "AiAccessCheckResult",
    "load_ai_assist_config",
    "build_bedrock_runtime_client",
    "validate_suggested_sql",
]
