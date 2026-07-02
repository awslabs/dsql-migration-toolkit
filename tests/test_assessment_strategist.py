"""Unit tests for the AI-led assessment strategist.

These tests use a fake Bedrock client (no AWS) and cover:

- Parsing a well-formed structured response into an :class:`AiAssessmentReport`.
- Untrusted-output handling: invented object names dropped, bad efforts coerced
  to None, code fences/extra prose tolerated, garbage -> INVALID_OUTPUT.
- Graceful degradation: a Bedrock failure maps to an unavailable outcome that
  never raises, so the deterministic assessment stands alone.
- The augment-not-replace contract: the strategist only references objects from
  the deterministic assessment.
"""

from __future__ import annotations

import json

from dsql_migrator.core.assessment_strategist import (
    AiAssessmentOutcome,
    AssessmentStrategist,
    parse_assessment_output,
)
from dsql_migrator.core.models import (
    AiAssistConfig,
    AssessmentItem,
    AssessmentReport,
    Classification,
    EffortLevel,
    SourceInventory,
    TableDef,
    TargetInventory,
)


# ---------------------------------------------------------------------------
# Fakes / fixtures
# ---------------------------------------------------------------------------


class _FakeClient:
    """A fake bedrock-runtime client returning a canned InvokeModel body."""

    def __init__(self, text: str) -> None:
        self._text = text
        self.calls: list[dict] = []

    def invoke_model(self, **kwargs: object) -> dict:
        self.calls.append(kwargs)
        envelope = json.dumps({"content": [{"type": "text", "text": self._text}]})
        return {"body": envelope}


class _BoomClient:
    """A fake client whose InvokeModel raises a generic error."""

    def invoke_model(self, **kwargs: object) -> dict:
        raise RuntimeError("network down")


def _config() -> AiAssistConfig:
    return AiAssistConfig(enabled=True, model_id="test-model")


def _assessment() -> AssessmentReport:
    return AssessmentReport.from_items(
        [
            AssessmentItem(
                object_name="orders",
                rule_id="FK_UNSUPPORTED",
                classification=Classification.MANUAL,
                risk="FK not supported",
                recommendation="drop FK",
                effort=EffortLevel.SIMPLE,
            ),
            AssessmentItem(
                object_name="audit_log",
                rule_id="NO_PRIMARY_KEY",
                classification=Classification.UNSUPPORTED,
                risk="no PK",
                recommendation="add PK",
                effort=EffortLevel.MEDIUM,
            ),
        ]
    )


def _inventory() -> SourceInventory:
    return SourceInventory(tables=[TableDef(name="orders"), TableDef(name="audit_log")])


def _well_formed_text() -> str:
    return json.dumps(
        {
            "strategy_summary": "Migrate reference tables first, then orders.",
            "insights": [
                {
                    "object_name": "orders",
                    "recommendation": "Enforce FK in the application layer.",
                    "rationale": "DSQL has no foreign keys.",
                    "effort": "SIMPLE",
                },
                {
                    "object_name": "ghost_table",  # not in the assessment -> dropped
                    "recommendation": "should be ignored",
                    "rationale": "invented",
                    "effort": "MEDIUM",
                },
            ],
            "additional_findings": [
                {
                    "area": "orders",
                    "risk": "Hot partition on sequential id.",
                    "recommendation": "Use a random/UUID key.",
                }
            ],
        }
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parse_well_formed_output_keeps_valid_objects_only() -> None:
    report = parse_assessment_output(
        _well_formed_text(),
        model_id="test-model",
        valid_object_names={"orders", "audit_log"},
    )
    assert report.strategy_summary.startswith("Migrate reference tables")
    # The invented "ghost_table" insight is dropped; only "orders" remains.
    assert [i.object_name for i in report.insights] == ["orders"]
    assert report.insights[0].ai_effort is EffortLevel.SIMPLE
    assert report.additional_findings[0].area == "orders"
    assert report.model_id == "test-model"


def test_parse_tolerates_code_fences_and_prose() -> None:
    fenced = "Here is the report:\n```json\n" + _well_formed_text() + "\n```"
    report = parse_assessment_output(
        fenced, model_id="m", valid_object_names={"orders", "audit_log"}
    )
    assert report.insights and report.insights[0].object_name == "orders"


def test_parse_coerces_unknown_effort_to_none() -> None:
    text = json.dumps(
        {
            "strategy_summary": "s",
            "insights": [
                {"object_name": "orders", "recommendation": "r", "effort": "HUGE"}
            ],
            "additional_findings": [],
        }
    )
    report = parse_assessment_output(
        text, model_id="m", valid_object_names={"orders"}
    )
    assert report.insights[0].ai_effort is None


def test_parse_garbage_raises_invalid_output() -> None:
    import pytest

    from dsql_migrator.core.ai_assistant import AiAssistUnavailableError

    with pytest.raises(AiAssistUnavailableError) as exc:
        parse_assessment_output("not json at all", model_id="m", valid_object_names=set())
    assert exc.value.reason == "INVALID_OUTPUT"


# ---------------------------------------------------------------------------
# Strategist generate / try_generate
# ---------------------------------------------------------------------------


def test_strategist_generates_report_via_fake_client() -> None:
    narrative = (
        "## Migration strategy\nMigrate reference tables first, then orders.\n\n"
        "## Prioritized recommendations\n- orders: enforce FK in the app (Simple)."
    )
    client = _FakeClient(narrative)
    strategist = AssessmentStrategist(_config(), client=client)

    report = strategist.generate(
        _inventory(), _assessment(), TargetInventory(), []
    )

    assert report.model_id == "test-model"
    # Best practice: the AI reply is a free-form Markdown narrative kept verbatim
    # (no brittle JSON parsing) in strategy_summary; rendered as Markdown by the UI.
    assert report.strategy_summary == narrative
    assert report.insights == []
    # The model id is used for provenance on the InvokeModel call.
    assert client.calls and client.calls[0]["modelId"] == "test-model"


def test_try_generate_returns_available_outcome() -> None:
    strategist = AssessmentStrategist(_config(), client=_FakeClient(_well_formed_text()))
    outcome = strategist.try_generate(
        _inventory(), _assessment(), TargetInventory(), []
    )
    assert isinstance(outcome, AiAssessmentOutcome)
    assert outcome.available is True
    assert outcome.reason == "OK"
    assert outcome.report is not None


def test_try_generate_degrades_gracefully_on_failure() -> None:
    strategist = AssessmentStrategist(_config(), client=_BoomClient())
    outcome = strategist.try_generate(
        _inventory(), _assessment(), TargetInventory(), []
    )
    # Never raises; the deterministic assessment stands alone.
    assert outcome.available is False
    assert outcome.report is None
    assert outcome.detail  # a clear, credential-free message
    assert "network down" not in outcome.detail


def test_try_generate_invalid_output_is_unavailable() -> None:
    # Only an empty/blank reply is INVALID_OUTPUT now -- any prose is a valid
    # Markdown narrative (best practice: no brittle JSON parsing).
    strategist = AssessmentStrategist(_config(), client=_FakeClient("   "))
    outcome = strategist.try_generate(
        _inventory(), _assessment(), TargetInventory(), []
    )
    assert outcome.available is False
    assert outcome.reason == "INVALID_OUTPUT"


# ---------------------------------------------------------------------------
# Streaming on-demand object guidance (Evaluation drawer)
# ---------------------------------------------------------------------------


class _FakeStreamClient:
    """A fake client whose streaming InvokeModel yields Anthropic deltas."""

    def __init__(self, texts: list[str]) -> None:
        self._texts = texts
        self.calls: list[dict] = []

    def invoke_model_with_response_stream(self, **kwargs: object) -> dict:
        self.calls.append(kwargs)

        def gen():
            for text in self._texts:
                payload = json.dumps(
                    {"type": "content_block_delta", "delta": {"text": text}}
                ).encode("utf-8")
                yield {"chunk": {"bytes": payload}}

        return {"body": gen()}


class _BoomStreamClient:
    """A fake client whose streaming InvokeModel raises before yielding."""

    def invoke_model_with_response_stream(self, **kwargs: object) -> dict:
        raise RuntimeError("network down")


def _guidance_item() -> AssessmentItem:
    return AssessmentItem(
        object_name="orders",
        rule_id="FK_UNSUPPORTED",
        classification=Classification.MANUAL,
        effort=EffortLevel.SIMPLE,
        kind="TABLE",
    )


def test_stream_object_guidance_emits_chunks_and_returns_markdown() -> None:
    client = _FakeStreamClient(["## Why\n", "Foreign keys ", "are unsupported."])
    strategist = AssessmentStrategist(_config(), client=client)
    seen: list[str] = []

    outcome = strategist.stream_object_guidance(_guidance_item(), seen.append)

    # Every delta was emitted in order, and the final markdown is their join.
    assert seen == ["## Why\n", "Foreign keys ", "are unsupported."]
    assert outcome.available is True
    assert outcome.reason == "OK"
    assert outcome.markdown == "## Why\nForeign keys are unsupported."
    assert outcome.model_id == "test-model"
    assert client.calls and client.calls[0]["modelId"] == "test-model"


def test_stream_object_guidance_graceful_when_stream_fails_to_start() -> None:
    strategist = AssessmentStrategist(_config(), client=_BoomStreamClient())
    seen: list[str] = []

    outcome = strategist.stream_object_guidance(_guidance_item(), seen.append)

    assert seen == []  # nothing was emitted
    assert outcome.available is False
    assert outcome.detail  # clear, credential-free message
    assert "network down" not in outcome.detail


def test_stream_object_guidance_empty_stream_is_invalid_output() -> None:
    strategist = AssessmentStrategist(_config(), client=_FakeStreamClient([]))
    outcome = strategist.stream_object_guidance(_guidance_item(), lambda _t: None)
    assert outcome.available is False
    assert outcome.reason == "INVALID_OUTPUT"


def test_iter_stream_text_skips_malformed_events() -> None:
    from dsql_migrator.core.assessment_strategist import _iter_stream_text

    events = [
        {"chunk": {"bytes": b"not json"}},  # undecodable -> skipped
        {"chunk": {}},  # no bytes -> skipped
        {"nope": 1},  # no chunk -> skipped
        {
            "chunk": {
                "bytes": json.dumps(
                    {"type": "message_start"}
                ).encode("utf-8")  # wrong type -> skipped
            }
        },
        {
            "chunk": {
                "bytes": json.dumps(
                    {"type": "content_block_delta", "delta": {"text": "hello"}}
                ).encode("utf-8")
            }
        },
    ]
    assert list(_iter_stream_text({"body": iter(events)})) == ["hello"]


# ---------------------------------------------------------------------------
# Multi-turn streaming chat (Evaluation drawer follow-ups)
# ---------------------------------------------------------------------------


def test_stream_object_chat_streams_and_sends_transcript() -> None:
    client = _FakeStreamClient(["Sure — ", "drop the FK."])
    strategist = AssessmentStrategist(_config(), client=client)
    seen: list[str] = []
    messages = [{"role": "user", "text": "How do I handle orders?"}]

    outcome = strategist.stream_object_chat(_guidance_item(), messages, seen.append)

    assert seen == ["Sure — ", "drop the FK."]
    assert outcome.available is True
    assert outcome.markdown == "Sure — drop the FK."
    # The request carried the system grounding + the full transcript.
    body = json.loads(client.calls[0]["body"])
    assert "Aurora DSQL constraints" in body["system"]
    assert body["messages"][0]["role"] == "user"
    assert body["messages"][0]["content"][0]["text"] == "How do I handle orders?"


def test_stream_object_chat_graceful_on_failure() -> None:
    strategist = AssessmentStrategist(_config(), client=_BoomStreamClient())
    outcome = strategist.stream_object_chat(
        _guidance_item(), [{"role": "user", "text": "hi"}], lambda _t: None
    )
    assert outcome.available is False
    assert "network down" not in outcome.detail


def test_build_chat_body_preserves_alternating_roles() -> None:
    from dsql_migrator.core.assessment_strategist import (
        _build_chat_body,
        build_object_chat_system,
    )

    system = build_object_chat_system(_guidance_item())
    msgs = [
        {"role": "user", "text": "q1"},
        {"role": "assistant", "text": "a1"},
        {"role": "user", "text": "q2"},
    ]
    body = json.loads(_build_chat_body(system, msgs, 100))
    assert [m["role"] for m in body["messages"]] == ["user", "assistant", "user"]
    assert body["max_tokens"] == 100
    assert body["messages"][1]["content"][0]["text"] == "a1"


def test_chat_system_prompt_scopes_to_this_object_topic() -> None:
    from dsql_migrator.core.assessment_strategist import build_object_chat_system

    system = build_object_chat_system(_guidance_item())
    # The model is told to stay on-topic and decline off-topic questions.
    assert "Stay strictly on the topic" in system
    assert "politely" in system and "decline" in system
    # And it is grounded on the specific object + DSQL constraints.
    assert "orders" in system
    assert "Aurora DSQL constraints" in system


def test_build_query_chat_system_grounds_on_query_and_error() -> None:
    from dsql_migrator.core.assessment_strategist import build_query_chat_system

    # Without a target error: grounded on the original + converted SQL, on-topic.
    system = build_query_chat_system(
        "SELECT JSON_UNQUOTE(x) FROM t", "SELECT x FROM t"
    )
    assert "SELECT JSON_UNQUOTE(x) FROM t" in system  # original (MySQL)
    assert "SELECT x FROM t" in system  # deterministic conversion
    assert "Aurora DSQL constraints" in system
    assert "decline" in system  # scope guard
    # No error block is woven in when no target error is supplied.
    assert "fix the statement" not in system.lower()

    # With a target error: the exact DSQL error is woven in so the AI fixes it.
    system_err = build_query_chat_system(
        "SELECT JSON_UNQUOTE(x) FROM t",
        "SELECT x FROM t",
        target_error='function json_unquote(json) does not exist (SQLSTATE 42883)',
    )
    assert "json_unquote(json) does not exist" in system_err
    assert "fix the statement" in system_err.lower()


def test_build_query_optimize_system_grounds_on_dsql_efficiency_and_bans_pg_lore() -> None:
    from dsql_migrator.core.assessment_strategist import build_query_optimize_system

    # With a captured EXPLAIN ANALYZE plan + DPU: the real plan and cost are woven
    # in so the advice is grounded in THIS query's actual DSQL execution.
    plan = "Full Scan (btree-table) on public.orders  (cost=..)"
    system = build_query_optimize_system(
        "SELECT * FROM orders WHERE created_at > '2025-01-01'",
        "SELECT * FROM orders WHERE created_at > '2025-01-01'",
        plan=plan,
        dpu_total=3.39262,
        analyzed=True,
    )
    assert plan in system  # reason from the real plan
    assert "3.39262 DPU" in system  # the measured cost baseline
    # Grounded on DSQL-specific execution facts, not vanilla PostgreSQL.
    assert "DPU" in system
    assert "Full Scan" in system  # DSQL term (not "Seq Scan")
    assert "filter" in system.lower() and "storage" in system.lower()
    assert "identical" in system.lower()  # must not change results
    # CRITICAL: it must explicitly forbid vanilla-PG tuning lore that is wrong/
    # inexpressible on DSQL, so the model doesn't hallucinate it.
    lowered = system.lower()
    assert "vacuum" in lowered and "reindex" in lowered  # named in the ban list
    assert "do not" in lowered or "not suggest" in lowered

    # Without a captured plan: it must NOT fabricate a plan/DPU; it should nudge to
    # run Test-on-target with ANALYZE first.
    no_plan = build_query_optimize_system("SELECT 1", "SELECT 1")
    assert "no query plan was captured" in no_plan.lower()
    assert "do not invent" in no_plan.lower() or "do not fabricate" in no_plan.lower() \
        or "not invent" in no_plan.lower()


def test_build_validation_chat_system_grounds_on_facts_and_recovery() -> None:
    from dsql_migrator.core.assessment_strategist import build_validation_chat_system

    facts = (
        "Table: orders\nSource row count: 72,590\nTarget row count: 72,516\n"
        "Row counts match: False\nRecord reconciliation: 74 missing on target."
    )
    system = build_validation_chat_system(facts, scope="table")
    # The deterministic facts are woven in verbatim and called authoritative.
    assert "72,590" in system and "74 missing on target" in system
    assert "authoritative" in system.lower()
    # The recovery model (re-run to backfill a standing gap, stop CDC for cut-over)
    # is grounded so the AI's advice matches what the tool can do.
    assert "standing gap" in system.lower()
    assert "Data Migration" in system  # where CDC is stopped
    assert "Aurora DSQL constraints" in system
    assert "decline" in system.lower()  # scope guard
    # Run scope only tunes the framing sentence.
    assert "validation run with mismatches" in build_validation_chat_system(
        facts, scope="run"
    )


def test_trim_chat_messages_keeps_recent_within_budget() -> None:
    from dsql_migrator.core.assessment_strategist import _trim_chat_messages

    msgs = [
        {"role": "user", "text": "a" * 100},
        {"role": "assistant", "text": "b" * 100},
        {"role": "user", "text": "c" * 100},
        {"role": "assistant", "text": "d" * 100},
        {"role": "user", "text": "e" * 100},
    ]
    # Budget of 250 chars keeps the most recent turns (oldest dropped first).
    kept = _trim_chat_messages(msgs, 250)
    assert [m["text"][0] for m in kept] == ["c", "d", "e"]
    # The result still starts with a user turn (Anthropic requirement).
    assert kept[0]["role"] == "user"


def test_trim_chat_messages_drops_leading_assistant_after_trim() -> None:
    from dsql_migrator.core.assessment_strategist import _trim_chat_messages

    msgs = [
        {"role": "user", "text": "x" * 100},
        {"role": "assistant", "text": "y" * 100},
        {"role": "user", "text": "z" * 100},
    ]
    # A tiny budget would keep only the last turn (and it is a user turn).
    kept = _trim_chat_messages(msgs, 10)
    assert [m["text"][0] for m in kept] == ["z"]
    assert kept[0]["role"] == "user"
