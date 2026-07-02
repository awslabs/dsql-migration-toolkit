"""Unit tests for AI-assisted CDC (readiness assessment + DLQ triage, Task 23.4).

Fake Bedrock client (no AWS). Covers (Req 12.13-12.15 / Property 11/13):
- Readiness assessment parses a well-formed response; insights are limited to
  the provided tables (the model cannot invent tables -- Property 8).
- The deterministic facts ground the prompt.
- DLQ triage parses a well-formed response over already dead-lettered events.
- Graceful degradation: a Bedrock failure yields an unavailable outcome that
  never raises (deterministic checks / DLQ handling stand alone).
"""

from __future__ import annotations

import json

from dsql_migrator.core.cdc import CdcConnectorError
from dsql_migrator.core.cdc_assist import (
    CdcAssistant,
    CdcReadinessSignals,
    build_cdc_readiness_prompt,
)
from dsql_migrator.core.models import AiAssistConfig


class _FakeClient:
    """Fake bedrock-runtime client returning a canned InvokeModel body."""

    def __init__(self, text: str) -> None:
        self._text = text
        self.calls: list[dict] = []

    def invoke_model(self, **kwargs: object) -> dict:
        self.calls.append(kwargs)
        envelope = json.dumps({"content": [{"type": "text", "text": self._text}]})
        return {"body": envelope}


class _BoomClient:
    def invoke_model(self, **kwargs: object) -> dict:
        raise RuntimeError("network down")


def _config() -> AiAssistConfig:
    return AiAssistConfig(enabled=True, model_id="test-model")


def _signals() -> CdcReadinessSignals:
    return CdcReadinessSignals(
        binlog_row_format_ok=True,
        gtid_enabled=False,
        tables_without_pk=["app.audit_log"],
        hot_pk_tables=["app.orders"],
    )


def _readiness_text() -> str:
    return json.dumps(
        {
            "strategy_summary": "Enable GTID before cutover; add a PK to audit_log.",
            "insights": [
                {
                    "object_name": "app.orders",
                    "recommendation": "Use a random/UUID key to spread writes.",
                    "rationale": "Hot PK causes OCC 40001 contention.",
                    "effort": "MEDIUM",
                },
                {
                    "object_name": "not_a_real_table",
                    "recommendation": "ignored",
                    "rationale": "invented",
                },
            ],
            "additional_findings": [
                {
                    "area": "GTID",
                    "risk": "GTID disabled blocks gapless resume.",
                    "recommendation": "Enable gtid_mode=ON.",
                }
            ],
        }
    )


def test_readiness_parses_and_limits_to_known_tables() -> None:
    assistant = CdcAssistant(_config(), client=_FakeClient(_readiness_text()))
    outcome = assistant.try_readiness(
        _signals(), tables=["app.orders", "app.audit_log"]
    )

    assert outcome.available is True
    report = outcome.report
    assert report is not None
    assert "Enable GTID" in report.strategy_summary
    # The invented table is dropped (Property 8: AI cannot invent objects).
    assert [i.object_name for i in report.insights] == ["app.orders"]
    assert report.additional_findings[0].area == "GTID"
    assert report.model_id == "test-model"


def test_readiness_prompt_grounds_deterministic_facts() -> None:
    prompt = build_cdc_readiness_prompt(
        _signals(), tables=["app.orders", "app.audit_log"]
    )
    assert "GTID enabled (gapless resume): no" in prompt
    assert "binlog ROW format + full row image: yes" in prompt
    assert "app.audit_log" in prompt  # tables without PK
    assert "app.orders" in prompt  # hot PK


def test_readiness_graceful_degradation() -> None:
    assistant = CdcAssistant(_config(), client=_BoomClient())
    outcome = assistant.try_readiness(_signals(), tables=["app.orders"])
    assert outcome.available is False
    assert outcome.reason == "UNAVAILABLE"
    assert outcome.report is None


def _triage_text() -> str:
    return json.dumps(
        {
            "strategy_summary": "Two clusters: decimal overflow and missing PK.",
            "insights": [
                {
                    "object_name": "app.orders",
                    "recommendation": "Set decimal.handling.mode=string.",
                    "rationale": "DECIMAL(65,30) exceeds target precision.",
                    "effort": "SIMPLE",
                }
            ],
            "additional_findings": [],
        }
    )


def test_dlq_triage_parses_over_dead_lettered_events() -> None:
    errors = [
        CdcConnectorError(
            table="app.orders", message="numeric overflow", error_code="22003"
        ),
        CdcConnectorError(table="app.orders", message="numeric overflow"),
    ]
    assistant = CdcAssistant(_config(), client=_FakeClient(_triage_text()))
    outcome = assistant.try_dlq_triage(errors, target_tables=["app.orders"])

    assert outcome.available is True
    assert outcome.report is not None
    assert "decimal.handling.mode=string" in outcome.report.insights[0].recommendation


def test_dlq_triage_graceful_degradation() -> None:
    assistant = CdcAssistant(_config(), client=_BoomClient())
    outcome = assistant.try_dlq_triage(
        [CdcConnectorError(table="app.orders", message="x")],
        target_tables=["app.orders"],
    )
    assert outcome.available is False
    assert outcome.report is None
