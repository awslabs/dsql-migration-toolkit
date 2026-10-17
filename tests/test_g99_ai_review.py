"""G99: the review of G93 -- a failed nudge round never costs the first reply, the punt
detector reads deferrals by what else the reply says and knows more Korean / Japanese
forms, a not-converted view's note names only what blocks it, a chat instance is this
tab's, and programmatic asks queue first in, first out."""

from __future__ import annotations

import io
import json
import threading
from types import SimpleNamespace

import pytest

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump
from tests.test_g72_ai_review import _executor

_ANSWER = (
    "Let me review your blockers:\n1. app.orders has 3 missing rows (CHECKSUM mismatch).\n"
    "2. The DLQ holds 2 records (23505) on app.users.\nReload app.orders, then re-validate."
)


def _tool_chat(second):
    """tool_chat whose first round answers (and is nudged) and whose second round is
    ``second``: a reply body, or an exception to raise."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    calls: list = []

    class Bedrock:
        def invoke_model(self, **_kw):  # noqa: ANN003
            calls.append(1)
            if len(calls) == 1:
                body = {"stop_reason": "end_turn", "content": [{"type": "text", "text": _ANSWER}]}
            elif isinstance(second, Exception):
                raise second
            else:
                body = second
            return {"body": io.BytesIO(json.dumps(body).encode())}

    shown: list = []
    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=Bedrock()).tool_chat(
        "sys", [{"role": "user", "text": "What's blocking me?"}], shown.append,
        tools=[{"name": "x", "description": "x", "input_schema": {"type": "object", "properties": {}}}],
        execute=lambda _n, _i: "{}", max_rounds=3,
    )
    return outcome, shown, len(calls)


@pytest.mark.parametrize(
    "second",
    [
        RuntimeError("ThrottlingException: Rate exceeded"),
        {"stop_reason": "end_turn", "content": []},
        # G104: a nudge round that calls a tool says the first reply was a punt, so when its
        # look-ups never answer, the failure is the outcome (test_g104_ai_review), not that reply.
    ],
)
def test_a_failed_nudge_round_keeps_the_first_reply(second) -> None:
    """A throttled or empty nudge round showed 'AI reply unavailable' and lost the answer
    already received."""
    outcome, shown, calls = _tool_chat(second)
    assert outcome.available and outcome.markdown == _ANSWER and shown == [_ANSWER]
    assert calls >= 2


@pytest.mark.parametrize(
    "punt",
    [
        "I'll check your assessment, schema conversion, data load, CDC health and validation "
        "state to show what is blocking you before you cut over.",
        "Before you cut over, let me check your DLQ, CDC lag and validation results.",
        "Let me check what happened to the sink after you deployed CDC.",
        "Let me check the DLQ. Would you like me to focus on CDC?",
        "현재 마이그레이션 상태를 확인해 드리겠습니다.",
        "현재 상태를 확인해 보도록 하겠습니다.",
        "다음에 해야 할 작업을 확인해 보겠습니다.",
        "Full Load 이후에 무슨 일이 있었는지 확인해 보겠습니다.",
        "確認させていただきます。",
        "見ていきます。",
        "お調べします。",
    ],
)
def test_punts_are_nudged(punt: str) -> None:
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert is_tool_preamble(punt)


@pytest.mark.parametrize(
    "answer",
    [
        "外部キーは親テーブルを参照します。",
        "Validation ステップでは行数とチェックサムを確認します。",
        "Let me explain why you see it missing: the view reads the table before the load.",
        "I'll keep the query as is; it already uses the primary key.",
        "To answer that, I need to know which schema you migrated it to.",
        "Your DLQ holds 3 records; they need no repair. I'll review it again if a new code appears.",
    ],
)
def test_statements_of_fact_and_promises_beside_an_answer_are_not_nudged(answer: str) -> None:
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert not is_tool_preamble(answer)


def test_a_colliding_views_note_names_only_what_blocks_it() -> None:
    """Its body's 'rewrite it in the target DDL' followed 'an edit is not applied'."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    long = "order_line_archive_" + "x" * 50
    inventory = SourceInventory(
        tables=[TableDef(name=f"shop.{long}_table", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                         primary_key=["id"])],
        views=[__import__("dsql_migrator.core.models", fromlist=["ViewDef"]).ViewDef(
            name=f"shop.{long}_view",
            definition=f"select substring_index(`t`.`id`,',',1) AS `x` from `shop`.`{long}_table` `t`",
        )],
    )
    execute = _executor(SimpleNamespace(inventory=inventory, assessment=None), SchemaConversionState())
    note = json.loads(execute("get_converted_ddl", {"object_name": f"shop.{long}_view"}))["note"]
    assert "Shorten the names on the source" in note and "SUBSTRING_INDEX" not in note


def test_another_tab_moving_the_session_scope_keeps_this_tabs_chat_open() -> None:
    """Tab A re-focused its own still-open tune chat after tab B opened another one, got a
    new instance, and its running re-test was refused."""
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui1, ui2 = _Ui(), _Ui()
    tab_a, tab_b = build_ai_panel(ui1, state=state), build_ai_panel(ui2, state=state)
    asked: list = []
    send = tab_a.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                            streamer=_make_streamer("A", record=asked), seed_question="Tune Q1")
    _pump(ui1)
    tab_b.open_scope(scope_id="readiness", title="AI DBA", streamer=_make_streamer("R"), seed_question="Next?")
    _pump(ui2)
    tab_a.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                     streamer=_make_streamer("A", record=asked), seed_question="Tune Q1")
    _pump(ui1)
    assert send.is_open()
    send("RETEST")
    _pump(ui1)
    assert any(m.get("text") == "RETEST" for call in asked for m in call)


def test_an_earlier_chats_send_says_it_is_closed() -> None:
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                            streamer=_make_streamer("A"), seed_question=None)
    panel.open_scope(scope_id="readiness", title="AI DBA", streamer=_make_streamer("R"), seed_question=None)
    assert not send.is_open()


def test_programmatic_asks_queue_first_in_first_out() -> None:
    """Two asks while a reply streamed shared one slot: the first was overwritten."""
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    gate = threading.Event()
    seen: list = []

    def streamer(messages, on_delta):  # noqa: ANN001
        seen.append(messages[-1]["text"])
        if len(seen) == 1:
            gate.wait(5)
        on_delta("A")
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown=f"A{len(seen)}")

    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA", streamer=streamer,
                            seed_question="Tune Q1")
    send("RETEST 1")
    send("RETEST 2")
    gate.set()
    for _ in range(4):
        _pump(ui, max_iters=400)
    assert seen == ["Tune Q1", "RETEST 1", "RETEST 2"]


def test_a_queued_ask_dropped_by_another_chat_is_announced() -> None:
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    notices: list = []
    ui.notify = lambda message, **_k: notices.append(message)  # type: ignore[method-assign]
    panel = build_ai_panel(ui, state=state)
    gate = threading.Event()

    def slow(messages, on_delta):  # noqa: ANN001
        gate.wait(5)
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown="A")

    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA", streamer=slow,
                            seed_question="Tune Q1")
    send("RETEST")
    panel.open_scope(scope_id="query-converter:review:q1", title="AI DBA", streamer=_make_streamer("R"),
                     seed_question=None)
    gate.set()
    _pump(ui, max_iters=400)
    assert notices and "no longer open" in notices[-1]
