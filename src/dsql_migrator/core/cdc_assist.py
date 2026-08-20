# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI-assisted CDC: control-plane readiness assessment + DLQ triage (Req 12.13-12.15).

Bedrock augments CDC only on the **control plane** (assessment/diagnosis) and is
never on the data plane (capture/stream/apply -- Req 12.15). This module reuses
the existing AI-assist seam without adding any new infrastructure:

- the shared Bedrock-runtime client/plumbing from
  :mod:`dsql_migrator.core.ai_assistant`
  (:func:`build_bedrock_runtime_client`, ``_build_invoke_body``,
  ``_extract_suggestion_text``, ``_classify_bedrock_error``,
  :class:`AiAssistUnavailableError`), and
- the defensive parser + outcome/report models from
  :mod:`dsql_migrator.core.assessment_strategist`
  (:func:`parse_assessment_output`, :class:`AiAssessmentOutcome`) and
  :class:`~dsql_migrator.core.models.AiAssessmentReport`.

Two opt-in capabilities (no-bloat -- only what Req 12.13/12.14 call for):

1. **CDC readiness assessment** (Req 12.13): grounded on **deterministic facts**
   (binlog ROW format / GTID enabled, primary-key-less tables, columns near the
   2 MiB row limit, hot-PK OCC contention, FK-chain apply-order) it adds a
   prioritized risk narrative. The deterministic facts remain authoritative
   (Property 8 unaffected); AI never changes them.
2. **DLQ triage** (Req 12.14): runs only over **already dead-lettered** events
   (off the hot path; no per-event LLM call) to explain root causes, cluster
   similar failures, and suggest fixes.

Guarantees mirror the rest of the AI-assist path: opt-in, deterministic-first,
human-reviewed (nothing auto-applied), graceful degradation (``try_*`` never
raises), credential-free messages (Property 7), and read-only inputs.
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.ai_assistant import (
    AiAssistUnavailableError,
    _build_invoke_body,
    _classify_bedrock_error,
    _extract_suggestion_text,
    build_bedrock_runtime_client,
)
from dsql_migrator.core.assessment_strategist import (
    AiAssessmentOutcome,
    parse_assessment_output,
)
from dsql_migrator.core.cdc import CdcConnectorError
from dsql_migrator.core.models import AiAssessmentReport, AiAssistConfig

# CDC readiness / triage return a structured report (summary + prioritized
# findings), so they use a larger budget than the per-object suggestion path.
# Output-token cap for CDC DLQ triage. Generous because the current models
# (Claude 5 family) use EXTENDED THINKING, whose tokens count against max_tokens:
# too small a cap and a reasoning-heavy turn spends it all on thinking and emits no
# answer (see the note on the token budgets in assessment_strategist).
_CDC_MAX_TOKENS = 8192

# Cap how many dead-lettered errors are summarized into a triage prompt so an
# unbounded DLQ cannot flood the request; triage clusters them anyway.
_MAX_TRIAGE_ERRORS = 100


def _yes_no(value: bool) -> str:
    """Render a boolean deterministic fact as a stable yes/no token."""
    return "yes" if value else "no"


def _bullet_list(items: Sequence[str]) -> str:
    """Render a list of names as a compact comma list (or '(none)')."""
    return ", ".join(items) if items else "(none)"


class CdcReadinessSignals(BaseModel):
    """Deterministic CDC-readiness facts that ground the AI assessment (Req 12.13).

    These are produced by deterministic checks (e.g. the prerequisite checker and
    inventory analysis), not by the model. The model only prioritizes and
    explains them; it never overrides a fact (Property 8). All fields default to
    a safe/empty value so a partial signal set is still usable.
    """

    model_config = ConfigDict(extra="forbid")

    binlog_row_format_ok: bool = Field(
        default=False, description="True when binlog_format=ROW and row image FULL."
    )
    gtid_enabled: bool = Field(default=False, description="True when gtid_mode=ON.")
    tables_without_pk: list[str] = Field(
        default_factory=list,
        description="Tables with no primary key (block CDC keying/idempotent upsert).",
    )
    large_row_columns: list[str] = Field(
        default_factory=list,
        description="Columns near the DSQL 2 MiB row limit (qualified name).",
    )
    hot_pk_tables: list[str] = Field(
        default_factory=list,
        description="Tables with monotonic/hot PKs at OCC contention risk.",
    )
    fk_chain_tables: list[str] = Field(
        default_factory=list,
        description="Tables in FK chains with apply-order dependencies.",
    )


def build_cdc_readiness_prompt(
    signals: CdcReadinessSignals, tables: Sequence[str]
) -> str:
    """Build the grounded prompt for the CDC readiness assessment (Req 12.13).

    Grounds the model on the deterministic facts (authoritative) and asks for a
    single JSON object so the output can be parsed defensively. Per-table
    ``insights`` must reference only ``tables``.
    """
    return (
        "You are a senior AWS database migration analyst assessing readiness for "
        "a MySQL -> Amazon Aurora DSQL streaming CDC pipeline (Debezium + Amazon "
        "MSK + a custom DSQL Sink Connector). The deterministic facts below are "
        "authoritative; do not contradict them. Add a prioritized risk narrative "
        "and concrete recommendations for a low-risk cutover.\n\n"
        "Deterministic CDC facts:\n"
        f"- binlog ROW format + full row image: {_yes_no(signals.binlog_row_format_ok)}\n"
        f"- GTID enabled (gapless resume): {_yes_no(signals.gtid_enabled)}\n"
        f"- tables without a primary key (block keying/idempotent upsert): "
        f"{_bullet_list(signals.tables_without_pk)}\n"
        f"- columns near the 2 MiB row limit: {_bullet_list(signals.large_row_columns)}\n"
        f"- hot primary-key tables (OCC 40001 contention risk): "
        f"{_bullet_list(signals.hot_pk_tables)}\n"
        f"- FK-chain tables (apply-order dependency; FKs unsupported on DSQL): "
        f"{_bullet_list(signals.fk_chain_tables)}\n\n"
        "Respond with ONLY a single JSON object, no prose or code fences:\n"
        "{\n"
        '  "strategy_summary": "overall CDC readiness verdict and prioritized plan",\n'
        '  "insights": [\n'
        '    {"object_name": "<table from the list below>", '
        '"recommendation": "concrete fix", "rationale": "why", '
        '"effort": "SIMPLE|MEDIUM|SIGNIFICANT"}\n'
        "  ],\n"
        '  "additional_findings": [\n'
        '    {"area": "<topic>", "risk": "cross-cutting CDC risk", '
        '"recommendation": "what to do"}\n'
        "  ]\n"
        "}\n"
        f"Only reference table names from this list: {_bullet_list(list(tables))}."
    )


def _summarize_errors(errors: Sequence[CdcConnectorError]) -> str:
    """Render dead-lettered errors as compact grounding text (capped)."""
    lines = []
    for error in list(errors)[:_MAX_TRIAGE_ERRORS]:
        code = error.error_code or "-"
        lines.append(f"- {error.table} | code={code} | {error.message}")
    return "\n".join(lines) if lines else "(no dead-lettered events)"


def build_dlq_triage_prompt(
    errors: Sequence[CdcConnectorError], target_tables: Sequence[str]
) -> str:
    """Build the grounded prompt for DLQ triage over dead-lettered events (Req 12.14).

    Operates only on already-quarantined events (off the hot path). Asks the
    model to cluster similar failures, explain root causes, and suggest fixes,
    returning a single JSON object for defensive parsing.
    """
    return (
        "You are triaging dead-lettered change events from a MySQL -> Amazon "
        "Aurora DSQL CDC pipeline. These events were already quarantined to a "
        "dead-letter queue (you are OFF the hot path; do not propose per-event "
        "processing). Cluster similar failures, explain the root cause of each "
        "cluster, and suggest a concrete fix (e.g. a Debezium/connector setting "
        "or a schema change). Nothing you suggest is auto-applied; a human "
        "reviews it.\n\n"
        "Dead-lettered events:\n"
        f"{_summarize_errors(errors)}\n\n"
        "Respond with ONLY a single JSON object, no prose or code fences:\n"
        "{\n"
        '  "strategy_summary": "overall root-cause summary across clusters",\n'
        '  "insights": [\n'
        '    {"object_name": "<affected table>", "recommendation": "fix", '
        '"rationale": "root cause", "effort": "SIMPLE|MEDIUM|SIGNIFICANT"}\n'
        "  ],\n"
        '  "additional_findings": [\n'
        '    {"area": "<failure cluster>", "risk": "root cause", '
        '"recommendation": "fix"}\n'
        "  ]\n"
        "}\n"
        f"Only reference table names from: {_bullet_list(list(target_tables))}."
    )


class CdcAssistant:
    """AI-assisted CDC readiness assessment and DLQ triage (control plane only).

    Augments -- never replaces -- the deterministic CDC checks. The Bedrock
    client is injectable (tests pass a fake) and otherwise built lazily from the
    shared session honoring the optional global AWS profile, so constructing the
    assistant performs no network call (Property 7). All ``try_*`` methods
    degrade gracefully and never raise (Req 12 / 11.10).
    """

    def __init__(
        self,
        config: AiAssistConfig,
        *,
        aws_profile: Optional[str] = None,
        client: Optional[Any] = None,
    ) -> None:
        """Create an assistant; the Bedrock client is built lazily if not given."""
        self._config = config
        self._aws_profile = aws_profile
        self._client = client

    def _get_client(self) -> Any:
        """Return the bedrock-runtime client, building it lazily if needed."""
        if self._client is None:
            self._client = build_bedrock_runtime_client(
                self._config, aws_profile=self._aws_profile
            )
        return self._client

    def _invoke(self, prompt: str, valid_object_names: set[str]) -> AiAssessmentReport:
        """Invoke Bedrock and parse the output into an :class:`AiAssessmentReport`.

        Bedrock failures map to a typed, credential-free
        :class:`AiAssistUnavailableError`; empty/unparseable output becomes
        ``INVALID_OUTPUT``.
        """
        try:
            response = self._get_client().invoke_model(
                modelId=self._config.model_id,
                body=_build_invoke_body(prompt, max_tokens=_CDC_MAX_TOKENS),
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
        return parse_assessment_output(
            text, model_id=self._config.model_id, valid_object_names=valid_object_names
        )

    def generate_readiness(
        self, signals: CdcReadinessSignals, tables: Sequence[str]
    ) -> AiAssessmentReport:
        """Produce the CDC readiness assessment (raises on failure/invalid output)."""
        prompt = build_cdc_readiness_prompt(signals, tables)
        return self._invoke(prompt, set(tables))

    def try_readiness(
        self, signals: CdcReadinessSignals, tables: Sequence[str]
    ) -> AiAssessmentOutcome:
        """Graceful-degradation wrapper around :meth:`generate_readiness`.

        Never raises: on any Bedrock failure or unparseable output it returns an
        unavailable outcome so the deterministic CDC checks stand alone.
        """
        try:
            report = self.generate_readiness(signals, tables)
        except AiAssistUnavailableError as error:
            return AiAssessmentOutcome.unavailable(error)
        return AiAssessmentOutcome.ok(report)

    def generate_dlq_triage(
        self,
        errors: Sequence[CdcConnectorError],
        target_tables: Sequence[str],
    ) -> AiAssessmentReport:
        """Triage already dead-lettered events (raises on failure/invalid output)."""
        prompt = build_dlq_triage_prompt(errors, target_tables)
        valid = set(target_tables) | {error.table for error in errors}
        return self._invoke(prompt, valid)

    def try_dlq_triage(
        self,
        errors: Sequence[CdcConnectorError],
        target_tables: Sequence[str],
    ) -> AiAssessmentOutcome:
        """Graceful-degradation wrapper around :meth:`generate_dlq_triage`.

        Never raises: DLQ quarantine/alarm/reprocessing stand alone when AI is
        unavailable (Req 12.14).
        """
        try:
            report = self.generate_dlq_triage(errors, target_tables)
        except AiAssistUnavailableError as error:
            return AiAssessmentOutcome.unavailable(error)
        return AiAssessmentOutcome.ok(report)


__all__ = [
    "CdcReadinessSignals",
    "CdcAssistant",
    "build_cdc_readiness_prompt",
    "build_dlq_triage_prompt",
]
