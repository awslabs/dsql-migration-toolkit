# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
                rule_id="FK_PRESERVED",
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
                    "recommendation": "Preserve the foreign key; DSQL enforces it after load.",
                    "rationale": "Aurora DSQL enforces foreign keys.",
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
        rule_id="FK_PRESERVED",
        classification=Classification.MANUAL,
        effort=EffortLevel.SIMPLE,
        kind="TABLE",
    )


def test_stream_object_guidance_emits_chunks_and_returns_markdown() -> None:
    client = _FakeStreamClient(["## Why\n", "Foreign keys ", "are enforced."])
    strategist = AssessmentStrategist(_config(), client=client)
    seen: list[str] = []

    outcome = strategist.stream_object_guidance(_guidance_item(), seen.append)

    # Every delta was emitted in order, and the final markdown is their join.
    assert seen == ["## Why\n", "Foreign keys ", "are enforced."]
    assert outcome.available is True
    assert outcome.reason == "OK"
    assert outcome.markdown == "## Why\nForeign keys are enforced."
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
    # Focused on THIS object, but allowed to help with the wider migration (via tools)
    # rather than refusing; still declines genuinely off-topic questions.
    assert "focus is migrating THIS object" in system
    assert "WIDER migration" in system and "TOOLS" in system
    assert "decline" in system
    # And it is grounded on the specific object + DSQL constraints.
    assert "orders" in system
    assert "Aurora DSQL constraints" in system


def test_build_reimplementation_chat_system_names_tools_and_per_kind_paths() -> None:
    from dsql_migrator.core.assessment_strategist import (
        build_reimplementation_chat_system,
    )

    system = build_reimplementation_chat_system()
    # It must drive the model to look up the REAL object names, not invent them.
    assert "list_unsupported_objects" in system
    assert "get_source_object_detail" in system
    # A concrete path per unconvertible kind, incl. the external-scheduler answer.
    assert "Trigger" in system and "procedure" in system.lower() and "EVENT" in system
    assert "EventBridge Scheduler" in system
    # CDC caveat (trigger/cascade side effects aren't replicated) is grounded.
    assert "not replicated" in system.lower() or "NOT replicated" in system
    assert "Aurora DSQL constraints" in system
    assert "decline" in system.lower()  # scope guard retained


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

    # Tool-aware + loosened (matches the object/conversion chats): it is told it HAS
    # the read-only tools and should use them, not "stay strictly / decline off-topic".
    assert "USE ANY TOOLS" in system
    assert "get_target_schema" in system and "get_converted_ddl" in system

    # The tool's own deterministic conversion warnings are woven in as authoritative.
    system_warn = build_query_chat_system(
        "SELECT ... FOR UPDATE", "SELECT ...",
        warnings=["[MANUAL] SELECT ... FOR UPDATE lock semantics differ on DSQL"],
    )
    assert "FOR UPDATE lock semantics differ" in system_warn
    assert "authoritative" in system_warn.lower()


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
    # Tool-aware: before recommending an index/INCLUDE it should CHECK what exists.
    assert "USE ANY TOOLS" in system and "get_target_schema" in system
    assert "existing indexes" in system.lower() or "already exists" in system.lower()

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
    # The loosened guard lets it help with the wider migration via tools.
    assert "USE ANY TOOLS" in system


def test_stream_validation_chat_routes_to_tool_chat_when_tools_given() -> None:
    # With tools + execute, a mismatch chat runs the agentic tool loop so it can
    # look up the real converted DDL / target schema / counts to root-cause the
    # divergence -- not just reason over the frozen facts. Without them it stays a
    # plain grounded stream (covered by the other tests).
    class _ToolClient:
        def __init__(self) -> None:
            self._round = 0

        def invoke_model(self, **kwargs: object) -> dict:
            self._round += 1
            if self._round == 1:
                return {"body": json.dumps({
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "t1",
                                 "name": "get_validation_summary", "input": {}}],
                })}
            return {"body": json.dumps({
                "stop_reason": "end_turn",
                "content": [{"type": "text",
                             "text": "orders is short 74 rows -- a standing gap."}],
            })}

    strategist = AssessmentStrategist(_config(), client=_ToolClient())
    executed: list[str] = []

    def execute(name: str, inp) -> str:  # noqa: ANN001
        executed.append(name)
        return json.dumps({"is_match": False, "missing_on_target": 74})

    tools = [{"name": "get_validation_summary", "description": "x",
              "input_schema": {"type": "object", "properties": {}}}]
    outcome = strategist.stream_validation_chat(
        "Table: orders", [{"role": "user", "text": "why is it short?"}],
        lambda _t: None, scope="table", tools=tools, execute=execute,
    )
    assert outcome.available and "74" in outcome.markdown
    assert executed == ["get_validation_summary"]  # the tool loop actually ran


def test_build_full_load_error_chat_system_grounds_on_table_error_and_context() -> None:
    from dsql_migrator.core.assessment_strategist import (
        build_full_load_error_chat_system,
    )

    error = (
        "DependentObjectsStillExist: cannot drop table customers_sample.countries "
        "because other objects depend on it DETAIL: view "
        "customers_sample.customer_order_summary depends on table ... "
        "HINT: Use DROP ... CASCADE to drop the dependent objects too."
    )
    system = build_full_load_error_chat_system(
        "customers_sample.countries",
        error,
        migration_context=(
            "Migration type: Full Load + CDC (change data capture will follow)\n"
            "Target table 'customers_sample.countries' already existed and was "
            "being DROP+recreated\nCDC has not started streaming yet."
        ),
    )
    # The failed table + its error are woven in verbatim, called authoritative.
    assert "customers_sample.countries" in system
    assert "DependentObjectsStillExist" in system
    assert "authoritative" in system.lower()
    # The migration situation is grounded so the reply is specific, not generic.
    assert "Full Load + CDC" in system
    assert "DROP+recreated" in system
    # The tool's Full Load recovery model + DSQL constraints are grounded.
    assert "Reload" in system  # per-table recovery action
    assert "idempotent" in system.lower()
    # The standing-gap trap is grounded: CDC won't backfill a failed/quarantined table,
    # so it must be resolved BEFORE starting CDC (else the target is silently short).
    assert "standing gap" in system.lower() or "STANDING gap" in system
    assert "backfill" in system.lower()
    assert "Aurora DSQL constraints" in system
    assert "decline" in system.lower()  # scope guard (kept, but loosened)
    # The loosened guard tells the (tool-wired) chat it may look up wider migration data
    # to root-cause the failure, instead of refusing -- so the wired tools get used.
    assert "USE ANY TOOLS" in system
    # Context is optional -- omitting it still produces a valid grounding.
    minimal = build_full_load_error_chat_system("t1", "InternalError_: server unavailable")
    assert "InternalError_" in minimal
    assert "Current migration context" not in minimal


def test_stream_full_load_error_chat_routes_to_tool_chat_when_tools_given() -> None:
    # With tools + execute, the failed-table diagnosis runs the agentic loop so it can
    # look up the real converted DDL / target schema to root-cause a schema/DDL failure.
    class _ToolClient:
        def __init__(self) -> None:
            self._round = 0

        def invoke_model(self, **kwargs: object) -> dict:
            self._round += 1
            if self._round == 1:
                return {"body": json.dumps({
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "t1",
                                 "name": "get_converted_ddl",
                                 "input": {"object_name": "orders"}}],
                })}
            return {"body": json.dumps({
                "stop_reason": "end_turn",
                "content": [{"type": "text", "text": "The DDL dropped a NOT NULL."}],
            })}

    strategist = AssessmentStrategist(_config(), client=_ToolClient())
    executed: list[str] = []

    def execute(name: str, inp) -> str:  # noqa: ANN001
        executed.append(name)
        return json.dumps({"target_ddl": "CREATE TABLE orders (...)"})

    tools = [{"name": "get_converted_ddl", "description": "x",
              "input_schema": {"type": "object",
                               "properties": {"object_name": {"type": "string"}},
                               "required": ["object_name"]}}]
    outcome = strategist.stream_full_load_error_chat(
        "orders", "NotNullViolation", [{"role": "user", "text": "why?"}],
        lambda _t: None, migration_context="Full Load + CDC",
        tools=tools, execute=execute,
    )
    assert outcome.available and "NOT NULL" in outcome.markdown
    assert executed == ["get_converted_ddl"]  # the tool loop actually ran


def test_build_cutover_chat_system_grounds_on_facts_and_cutover_model() -> None:
    from dsql_migrator.core.assessment_strategist import build_cutover_chat_system

    facts = (
        "Target coordinates (non-secret): endpoint=abc.dsql.us-east-1.on.aws "
        "region=us-east-1 database=postgres role=admin.\n"
        "Migration path: Full Load + CDC.\n"
        "Last validation: match=True, matched=8/8 tables.\n"
        "Identity-sync: NOT RUN in this session."
    )
    system = build_cutover_chat_system(facts)
    # The facts are woven in verbatim and called authoritative.
    assert "endpoint=abc.dsql.us-east-1.on.aws" in system
    assert "Identity-sync: NOT RUN" in system
    assert "authoritative" in system.lower()
    # The cut-over model is grounded: IAM tokens + sslmode + OCC 40001 + rollback
    # asymmetry + no-writes-from-chat rule + identity sequences.
    assert "IAM" in system and "sslmode=require" in system
    assert "40001" in system  # OCC retry framing
    assert "ROLLBACK" in system or "Rollback" in system or "rollback" in system.lower()
    assert "identity" in system.lower() and "23505" in system
    # It must NEVER propose writes -- writes to DSQL go through the tool's approval.
    assert "Do NOT propose changes to the target from a chat" in system
    # Loosened guard for tool-wired chat.
    assert "USE ANY TOOLS" in system
    assert "decline" in system.lower()


def test_stream_cutover_chat_routes_to_tool_chat_when_tools_given() -> None:
    class _ToolClient:
        def __init__(self) -> None:
            self._round = 0

        def invoke_model(self, **kwargs: object) -> dict:
            self._round += 1
            if self._round == 1:
                return {"body": json.dumps({
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "t1",
                                 "name": "get_validation_summary", "input": {}}],
                })}
            return {"body": json.dumps({
                "stop_reason": "end_turn",
                "content": [{"type": "text",
                             "text": "GO. Repoint with sslmode=require and mint a fresh IAM token per connection."}],
            })}

    strategist = AssessmentStrategist(_config(), client=_ToolClient())
    executed: list[str] = []

    def execute(name: str, inp) -> str:  # noqa: ANN001
        executed.append(name)
        return json.dumps({"is_match": True, "matched_tables": 8, "total_tables": 8})

    tools = [{"name": "get_validation_summary", "description": "x",
              "input_schema": {"type": "object", "properties": {}}}]
    outcome = strategist.stream_cutover_chat(
        "Cut over facts", [{"role": "user", "text": "safe to cut over?"}],
        lambda _t: None, tools=tools, execute=execute,
    )
    assert outcome.available and "GO" in outcome.markdown
    assert executed == ["get_validation_summary"]  # the tool loop actually ran


def test_build_cdc_error_chat_system_grounds_on_facts_and_cdc_model() -> None:
    from dsql_migrator.core.assessment_strategist import build_cdc_error_chat_system

    facts = (
        "CDC dead-letter queue: 12 poison record(s).\n"
        "Top SQLSTATEs: 23502 (9), 22P02 (3)."
    )
    system = build_cdc_error_chat_system(facts, scope="dlq")
    assert "12 poison record(s)" in system and "23502" in system  # facts woven in
    assert "authoritative" in system.lower()
    assert "dead-letter" in system.lower()  # DLQ framing
    assert "does NOT propagate DDL" in system  # the CDC model is grounded
    assert "standing" in system.lower() and "cut over" in system.lower()
    assert "Aurora DSQL constraints" in system
    assert "decline" in system.lower()  # scope guard (loosened)
    assert "USE ANY TOOLS" in system  # tool-wired chat is told it may look things up
    # The drift scope tunes the subject line.
    assert "drift" in build_cdc_error_chat_system("x", scope="drift").lower()


def test_stream_cdc_chat_routes_to_tool_chat_when_tools_given() -> None:
    class _ToolClient:
        def __init__(self) -> None:
            self._round = 0

        def invoke_model(self, **kwargs: object) -> dict:
            self._round += 1
            if self._round == 1:
                return {"body": json.dumps({
                    "stop_reason": "tool_use",
                    "content": [{"type": "tool_use", "id": "t1",
                                 "name": "get_cdc_status", "input": {}}],
                })}
            return {"body": json.dumps({
                "stop_reason": "end_turn",
                "content": [{"type": "text",
                             "text": "SQLSTATE 23502 means a dropped/added column."}],
            })}

    strategist = AssessmentStrategist(_config(), client=_ToolClient())
    executed: list[str] = []

    def execute(name: str, inp) -> str:  # noqa: ANN001
        executed.append(name)
        return json.dumps({"dlq_depth": 12, "schema_drift": []})

    tools = [{"name": "get_cdc_status", "description": "x",
              "input_schema": {"type": "object", "properties": {}}}]
    outcome = strategist.stream_cdc_chat(
        "CDC DLQ: 12 poison records", [{"role": "user", "text": "why?"}],
        lambda _t: None, scope="dlq", tools=tools, execute=execute,
    )
    assert outcome.available and "dropped" in outcome.markdown.lower()
    assert executed == ["get_cdc_status"]  # the tool loop actually ran


def test_build_connection_error_chat_system_grounds_on_side_and_error() -> None:
    from dsql_migrator.core.assessment_strategist import (
        build_connection_error_chat_system,
    )

    # Source (MySQL) failure: the coordinates + error are woven in, scoped + on-topic.
    src = build_connection_error_chat_system(
        side="source",
        coordinates="MySQL host=db.example.internal port=3306 database=shop",
        error_message="OperationalError: (2003) Can't connect to MySQL server",
    )
    assert "db.example.internal" in src and "2003" in src
    assert "MySQL" in src and "authoritative" in src.lower()
    assert "decline" in src.lower()  # scope guard
    # Target (DSQL) failure: names the IAM-token / dsql:DbConnect class of cause.
    tgt = build_connection_error_chat_system(
        side="target",
        coordinates="Aurora DSQL endpoint=abc.dsql.us-east-1.on.aws region=us-east-1",
        error_message="AccessDeniedException: not authorized to perform dsql:DbConnect",
    )
    assert "dsql:DbConnect" in tgt and "IAM" in tgt
    assert "us-east-1" in tgt
    # The target guidance states DSQL's no-password (IAM-token) auth model.
    assert "no password" in tgt.lower()


def test_tool_chat_runs_a_tool_then_answers() -> None:
    # The agentic loop: round 1 the model asks to call a tool, we execute it and feed
    # the result back, round 2 the model answers. Verify the tool ran with the model's
    # input, the result was fed back as a tool_result user turn, and the final text is
    # returned + streamed. Tools + the response-style directive ride in the request.
    class _ToolClient:
        def __init__(self) -> None:
            self.calls: list[dict] = []
            self._round = 0

        def invoke_model(self, **kwargs: object) -> dict:
            self.calls.append(kwargs)
            self._round += 1
            if self._round == 1:
                body = json.dumps(
                    {
                        "stop_reason": "tool_use",
                        "content": [
                            {"type": "text", "text": "Let me look that up."},
                            {
                                "type": "tool_use",
                                "id": "t1",
                                "name": "get_converted_ddl",
                                "input": {"object_name": "orders"},
                            },
                        ],
                    }
                )
            else:
                body = json.dumps(
                    {
                        "stop_reason": "end_turn",
                        "content": [
                            {"type": "text", "text": "Here is the `orders` DDL."}
                        ],
                    }
                )
            return {"body": body}

    client = _ToolClient()
    strategist = AssessmentStrategist(_config(), client=client)
    executed: list[tuple] = []

    def execute(name: str, inp) -> str:  # noqa: ANN001
        executed.append((name, dict(inp)))
        return json.dumps({"target_ddl": "CREATE TABLE orders (...)"})

    tools = [
        {
            "name": "get_converted_ddl",
            "description": "Get converted DDL for one object.",
            "input_schema": {
                "type": "object",
                "properties": {"object_name": {"type": "string"}},
                "required": ["object_name"],
            },
        }
    ]
    deltas: list[str] = []
    outcome = strategist.tool_chat(
        "SYSTEM", [{"role": "user", "text": "show orders ddl"}], deltas.append,
        tools=tools, execute=execute,
    )
    assert outcome.available and "orders" in outcome.markdown
    assert executed == [("get_converted_ddl", {"object_name": "orders"})]
    assert len(client.calls) == 2  # tool_use round, then the answer round
    assert deltas and "orders" in "".join(deltas)  # final answer streamed to the panel
    # The tools + response-style directive were sent on the first request.
    first = json.loads(client.calls[0]["body"])
    assert first["tools"][0]["name"] == "get_converted_ddl"
    assert "How to answer" in first["system"]  # _RESPONSE_STYLE appended
    # Round 2 fed the tool result back as the trailing user (tool_result) turn.
    second = json.loads(client.calls[1]["body"])
    assert second["messages"][-1]["role"] == "user"
    assert second["messages"][-1]["content"][0]["type"] == "tool_result"


def test_tool_chat_maps_bedrock_failure_to_unavailable() -> None:
    strategist = AssessmentStrategist(_config(), client=_BoomClient())
    outcome = strategist.tool_chat(
        "SYSTEM", [{"role": "user", "text": "hi"}], lambda _t: None,
        tools=[], execute=lambda _n, _i: "{}",
    )
    assert not outcome.available  # never raises; degrades to an unavailable outcome


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
