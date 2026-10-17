"""G84: the review of G72 -- AI DBA's paging within its tool budget, a 'What's next?' asked
while a reply streams, two tabs of one session, a view no apply path creates."""

from __future__ import annotations

import io
import json
import threading
from types import SimpleNamespace

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump
from tests.test_g72_ai_review import _executor


def test_a_paged_listing_is_answered_within_the_tool_budget() -> None:
    """3,000 objects paged ~120 at a time ran out of tool rounds: the turn ended
    'The assistant kept looking things up without answering' with no answer at all."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import (
        AiAssistConfig,
        AssessmentItem,
        AssessmentReport,
        Classification,
    )
    from dsql_migrator.ui.ai_tools import AI_TOOL_SCHEMAS

    items = [
        AssessmentItem(object_name=f"shop.order_line_archive_{i:04d}", kind="TABLE",
                       rule_id="COMPATIBLE", classification=Classification.AUTO,
                       risk="", recommendation="")
        for i in range(3000)
    ]
    execute = _executor(SimpleNamespace(assessment=AssessmentReport.from_items(items)))
    budget_seen: list = []

    class Bedrock:
        """Pages until told the budget is spent, then answers with what it has."""

        def invoke_model(self, **kw):  # noqa: ANN003
            last = json.loads(kw["body"])["messages"][-1]
            blocks = last["content"] if isinstance(last["content"], list) else []
            if any(b.get("type") == "text" and "Tool budget" in b.get("text", "") for b in blocks):
                budget_seen.append(True)
                out = {"stop_reason": "end_turn", "content": [
                    {"type": "text", "text": "3,000 tables convert automatically; the first pages are listed."}]}
                return {"body": io.BytesIO(json.dumps(out).encode())}
            offset = 0
            if blocks and blocks[0].get("type") == "tool_result":
                offset = json.loads(blocks[0]["content"]).get("next_offset") or 0
            out = {"stop_reason": "tool_use", "content": [{
                "type": "tool_use", "id": f"t{offset}", "name": "list_objects_by_status",
                "input": {"classification": "AUTO", "offset": offset}}]}
            return {"body": io.BytesIO(json.dumps(out).encode())}

    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=Bedrock()).tool_chat(
        "sys", [{"role": "user", "text": "List every table that converts automatically."}],
        lambda _d: None, tools=AI_TOOL_SCHEMAS, execute=execute,
    )
    assert budget_seen and outcome.available, outcome.detail


def test_a_whats_next_asked_while_a_reply_streams_is_asked_after_it() -> None:
    """open_scope(seed None) then send(seed) while busy: the briefing was silently dropped."""
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    gate = threading.Event()

    def slow(messages, on_delta):  # noqa: ANN001
        gate.wait(5)
        on_delta("A1")
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown="A1")

    panel.open_scope(scope_id="eval:orders", title="orders", streamer=slow, seed_question="Q1")
    asked: list = []
    send = panel.open_scope(
        scope_id="readiness", title="AI DBA", streamer=_make_streamer("BRIEFING", record=asked),
        seed_question=None,
    )
    send("what should I do next?")
    gate.set()
    _pump(ui, max_iters=400)
    _pump(ui, max_iters=400)
    texts = [m["text"] for m in state.ai_conversation.messages]
    assert texts == ["Q1", "A1", "what should I do next?", "BRIEFING"]


def test_a_tabs_follow_up_carries_only_its_own_turns() -> None:
    """tab1's follow-up on orders was sent with tab2's items exchange from the shared
    transcript."""
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui1, ui2 = _Ui(), _Ui()
    tab1, tab2 = build_ai_panel(ui1, state=state), build_ai_panel(ui2, state=state)
    gate = threading.Event()
    sent: list = []

    def slow(messages, on_delta):  # noqa: ANN001
        sent.append([m["text"] for m in messages])
        if len(sent) == 1:
            gate.wait(5)
        on_delta("A")
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown=f"A{len(sent)}")

    send1 = tab1.open_scope(scope_id="eval:orders", title="orders", streamer=slow, seed_question="QA")
    tab2.open_scope(scope_id="eval:items", title="items", streamer=_make_streamer("B"), seed_question="QB")
    _pump(ui2)
    gate.set()
    _pump(ui1, max_iters=400)
    send1("does the orders key keep generating?")
    _pump(ui1)
    assert sent[-1] == ["QA", "A1", "does the orders key keep generating?"]


def test_a_reply_of_a_journey_started_over_in_another_tab_is_not_recorded() -> None:
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui1, ui2 = _Ui(), _Ui()
    tab1, tab2 = build_ai_panel(ui1, state=state), build_ai_panel(ui2, state=state)
    gate = threading.Event()

    def slow(messages, on_delta):  # noqa: ANN001
        gate.wait(5)
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown="OLD")

    tab1.open_scope(scope_id="eval:orders", title="orders", streamer=slow, seed_question="QA")
    state.ai_conversation.reset()
    tab2.reset()
    gate.set()
    _pump(ui1, max_iters=400)
    assert state.ai_conversation.messages == []


def test_a_view_no_apply_path_creates_is_not_reported_as_applied() -> None:
    """An adopted reply stored for a MANUAL view read 'edited: true' as what Apply runs."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    table = TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                     primary_key=["id"])
    inventory = SourceInventory(tables=[table], views=[ViewDef(name="shop.v_orders", definition="")])
    state = SchemaConversionState()
    state.set_edited_target_ddl("shop.v_orders", 'CREATE VIEW "shop"."v_orders" AS SELECT "id" FROM "shop"."orders";')
    execute = _executor(SimpleNamespace(inventory=inventory, assessment=None), state)
    out = json.loads(execute("get_converted_ddl", {"object_name": "shop.v_orders"}))
    assert out["edited"] is False and "not converted" in out["note"]
    assert "SELECT \"id\"" not in out["target_ddl"]


def test_short_preambles_and_answers_are_told_apart() -> None:
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    for preamble in (
        "Let me check your DLQ and the Full Load status. One moment.",
        "Let me check the dead-letter queue to see if you have any 23505 records.",
        "I'll check a few things first:\n1. The Evaluation verdict\n2. Its converted DDL",
    ):
        assert is_tool_preamble(preamble), preamble
    long_answer = (
        "Your dead-letter queue holds 3 records, all SQLSTATE 23505 on app.users, and every "
        "one of them is an INSERT that a re-snapshot replayed after the Full Load had already "
        "written the same rows, so the target already holds exactly what the source holds and "
        "they need no repair; Validation's CHECKSUM for app.users matched after the last dead "
        "letter, which is why Cut over no longer holds the table, and nothing else in the "
        "stream is failing right now. I'll check the DLQ depth again before you cut over."
    )
    for answer in (
        long_answer,
        "app.orders converts automatically. Once Schema Apply has run, I'll be able to check the index build.",
        "Your DLQ holds 3 records; they need no repair. I'll review it again if a new code appears.",
    ):
        assert not is_tool_preamble(answer), answer
