"""G116: the review of G106-G111 (AI) -- tool_chat's reply comes through an answer tool. Since
G84 the chat guessed from a reply's wording whether the model had punted (announced a look-up
and ended its turn without calling a tool), nudged it once and asked for KEEP_PREVIOUS_REPLY
when it had answered; every review of that heuristic found new punts shown as the reply and new
answers nudged and then lost. Now every round asks for a tool call (tool_choice "any") -- a
look-up, or the answer tool, whose Markdown is the reply -- and the last round can only answer,
so a turn cannot end with "Let me check ..." and always ends with an answer."""

from __future__ import annotations

import io
import json

import pytest

from tests.test_ai_panel import _Ui, _enabled_state, _pump
from tests.test_g104_ai_review import _bedrock, _ClientError

_TOOLS = [{"name": "list_cdc_dlq_samples", "description": "x",
           "input_schema": {"type": "object", "properties": {}}}]
_REAL = "The DLQ holds 3 records, all 23505 duplicates from a re-snapshot; nothing needs repair."


def _look(call_id: str = "t1") -> dict:
    return {"stop_reason": "tool_use", "content": [
        {"type": "tool_use", "id": call_id, "name": "list_cdc_dlq_samples", "input": {}}]}


def _answer(markdown, *, stop: str = "tool_use", call_id: str = "a1") -> dict:
    given = {} if markdown is None else {"markdown": markdown}
    return {"stop_reason": stop, "content": [
        {"type": "tool_use", "id": call_id, "name": "answer", "input": given}]}


def _text(text: str) -> dict:
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}


def _chat(client, *, max_rounds: int = 6):
    """tool_chat over ``client``: (outcome, what on_delta received, the look-ups it ran)."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    shown: list = []
    ran: list = []

    def execute(name, inp):  # noqa: ANN001, ANN202
        ran.append((name, dict(inp)))
        return '{"samples": []}'

    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=client).tool_chat(
        "sys", [{"role": "user", "text": "Why is app.orders missing rows?"}], shown.append,
        tools=_TOOLS, execute=execute, max_rounds=max_rounds,
    )
    return outcome, shown, ran


class _Recorder:
    """A Bedrock client answering each call with the next body of ``script`` (the last one
    repeats) and keeping every request body it was sent."""

    def __init__(self, script: list) -> None:
        self.script = script
        self.sent: list = []

    def invoke_model(self, **kw):  # noqa: ANN003
        self.sent.append(json.loads(kw["body"]))
        step = self.script[min(len(self.sent), len(self.script)) - 1]
        if isinstance(step, Exception):
            raise step
        return {"body": io.BytesIO(json.dumps(step).encode())}


def test_every_round_asks_for_a_tool_call_and_the_last_one_for_the_answer() -> None:
    """The request offered the look-ups only and let the model end its turn in text, so "Let me
    check the DLQ samples." was a whole reply: the answer tool rides beside the look-ups, every
    round asks for a tool call, and the last one for the answer."""
    client = _Recorder([_look("t1"), _look("t2"), _answer(_REAL)])
    outcome, shown, ran = _chat(client, max_rounds=3)
    assert outcome.available and shown == [_REAL] and len(ran) == 2
    first, _second, last = client.sent
    assert [tool["name"] for tool in first["tools"]] == ["list_cdc_dlq_samples", "answer"]
    answer_tool = first["tools"][-1]
    assert answer_tool["input_schema"]["required"] == ["markdown"]
    assert "never use it to say you will look something up" in answer_tool["description"]
    assert [body["tool_choice"] for body in client.sent[:-1]] == [{"type": "any"}] * 2
    assert last["tool_choice"] == {"type": "tool", "name": "answer"}
    # Forced tool use goes with thinking off, and the system says how the reply is delivered.
    assert all(body["thinking"] == {"type": "disabled"} for body in client.sent)
    assert "How to answer" in first["system"] and "with the answer tool" in first["system"]
    # One round: it can only answer.
    single = _Recorder([_answer(_REAL)])
    assert _chat(single, max_rounds=1)[0].markdown == _REAL
    assert single.sent[0]["tool_choice"]["type"] == "tool"


def test_a_look_up_then_the_answer() -> None:
    client = _Recorder([
        {"stop_reason": "tool_use", "content": [{"type": "tool_use", "id": "t1",
         "name": "list_cdc_dlq_samples", "input": {"table": "app.orders"}}]},
        _answer(_REAL),
    ])
    outcome, shown, ran = _chat(client)
    assert outcome.available and outcome.markdown == _REAL and shown == [_REAL]
    assert ran == [("list_cdc_dlq_samples", {"table": "app.orders"})]
    fed_back = client.sent[1]["messages"][-1]
    assert fed_back["role"] == "user"
    assert fed_back["content"][0] == {
        "type": "tool_result", "tool_use_id": "t1", "content": '{"samples": []}',
    }


def test_an_immediate_answer_is_one_round() -> None:
    reply = (
        "Let me review your blockers:\n1. app.orders has 3 missing rows (CHECKSUM mismatch).\n"
        "2. The DLQ holds 2 records (23505) on app.users.\nReload app.orders, then re-validate."
    )
    client = _Recorder([_answer(reply), _look()])
    outcome, shown, ran = _chat(client)
    assert outcome.available and outcome.markdown == reply and shown == [reply]
    assert len(client.sent) == 1 and ran == []


def test_a_turn_of_plain_text_is_the_reply() -> None:
    """A model or proxy that ignores tool_choice can still end its turn in text: that text is the
    reply, as before the detector existed -- never second-guessed from its wording."""
    for text in ("Aurora DSQL requires a primary key.", "Let me check the DLQ samples."):
        client = _Recorder([_text(text), _look(), _ClientError("ThrottlingException")])
        outcome, shown, _ran = _chat(client)
        assert outcome.available and shown == [text] and len(client.sent) == 1


def test_an_announcement_beside_a_look_up_is_never_the_reply() -> None:
    """With tool_choice auto a model writes "Let me check ..." before its tool call; the reply
    is only ever what reaches the answer tool."""
    client = _Recorder([
        {"stop_reason": "tool_use", "content": [
            {"type": "text", "text": "Let me check the DLQ samples."},
            {"type": "tool_use", "id": "t1", "name": "list_cdc_dlq_samples", "input": {}}]},
        _answer(_REAL),
    ])
    outcome, shown, _ran = _chat(client)
    assert outcome.markdown == _REAL and shown == [_REAL]


def test_an_answer_beside_look_ups_waits_for_their_results() -> None:
    """An answer called in the same turn as look-ups was written before their results came back,
    so it is not the reply: the look-ups run, the answer call is told so, and the model answers
    again from what they returned -- unless no round is left to read them."""
    client = _Recorder([
        {"stop_reason": "tool_use", "content": [
            {"type": "tool_use", "id": "t1", "name": "list_cdc_dlq_samples", "input": {}},
            {"type": "tool_use", "id": "a1", "name": "answer",
             "input": {"markdown": "The DLQ is probably empty."}}]},
        _answer(_REAL, call_id="a2"),
    ])
    outcome, shown, ran = _chat(client)
    assert outcome.markdown == _REAL and shown == [_REAL]
    assert ran == [("list_cdc_dlq_samples", {})]  # the answer tool never reaches execute
    results = client.sent[1]["messages"][-1]["content"]
    assert [r["tool_use_id"] for r in results] == ["t1", "a1"]
    assert "Not shown to the operator" in results[1]["content"]
    # On the last round no round is left to read them: the answer is the reply, and the
    # look-ups beside it are not run.
    beside = {"stop_reason": "tool_use", "content": [
        {"type": "tool_use", "id": "t2", "name": "list_cdc_dlq_samples", "input": {}},
        {"type": "tool_use", "id": "a3", "name": "answer", "input": {"markdown": _REAL}}]}
    outcome, shown, ran = _chat(_Recorder([_look(), beside]), max_rounds=2)
    assert outcome.markdown == _REAL and shown == [_REAL] and len(ran) == 1


@pytest.mark.parametrize(
    "markdown", [None, "", "   ", 42],
    ids=["no markdown", "empty", "blank", "not a string"],
)
def test_an_answer_without_markdown_is_unavailable(markdown) -> None:
    outcome, shown, _ran = _chat(_Recorder([_answer(markdown)]))
    assert not outcome.available and outcome.reason == "INVALID_OUTPUT" and shown == []


def test_an_answer_cut_short_says_so() -> None:
    """Cut at the model's output limit, the reply ended mid-sentence as if it were whole."""
    from dsql_migrator.core.assessment_strategist import _MAX_NARRATIVE_CHARS

    cut = "app.orders is missing 3 rows. Reload it from Data Migration, then"
    outcome, shown, _ran = _chat(_Recorder([_answer(cut, stop="max_tokens")]))
    assert outcome.available and outcome.markdown.startswith(cut + "\n\n")
    assert "cut at its length limit" in outcome.markdown and shown == [outcome.markdown]
    # Cut with nothing of the answer in it: no reply.
    outcome, shown, _ran = _chat(_Recorder([_answer(None, stop="max_tokens")]))
    assert outcome.reason == "INVALID_OUTPUT" and shown == []
    # Past the panel's cap it is cut there, and says so too.
    long = "x" * (_MAX_NARRATIVE_CHARS + 50)
    outcome, _shown, _ran = _chat(_Recorder([_answer(long)]))
    assert outcome.markdown.startswith("x" * _MAX_NARRATIVE_CHARS + "\n\n")
    assert "cut at its length limit" in outcome.markdown


def test_the_rounds_end_with_the_answer_or_say_why_not() -> None:
    """A model that keeps looking things up is made to answer on the last round; one that still
    does not (a proxy that ignores tool_choice) ends the turn with an honest failure."""
    outcome, _shown, ran = _chat(_Recorder([_look(), _look(), _answer(_REAL)]), max_rounds=3)
    assert outcome.markdown == _REAL and len(ran) == 2
    outcome, shown, ran = _chat(_Recorder([_look()]), max_rounds=3)
    assert not outcome.available and outcome.reason == "UNAVAILABLE" and shown == []
    assert "kept looking things up" in outcome.detail and len(ran) == 3


# The G106-G111 verifiers' replies, each one the punt detector read wrong on 5d955cc: a punt (the
# model announced a look-up and ended its turn) shown as the whole reply, or a complete answer
# nudged -- the model then looked something up and that round was throttled, so 'AI reply
# unavailable' replaced it.
_PUNTS = [
    ("F1", "There are a few possible causes here. I'll check the CDC metrics and figure out if "
           "the sink has fallen behind."),
    ("F1", "Missing rows usually point to a failed batch. I'll check the row counts for "
           "app.orders after the Full Load."),
    ("F2", "This can happen after a re-snapshot. I'll query the target and work out if any rows "
           "are missing."),
    ("F3", "最初は、DLQのサンプルを確認します。"),
    ("F3", "app.ordersの場合は、変換後のDDLを確認します。"),
    ("F3", "私の方では、まず検証結果を確認します。"),
    ("F4", "Let me start with a quick look at the DLQ samples for app.orders."),
    ("F4", "I'll give the DLQ samples a quick look."),
    ("F7", "Let me explain the likely cause, then take a look at the DLQ samples."),
    ("F12", "Happy to help with that. I'll check the DLQ before you cut over."),
    ("F13", "シンクは停止しています。ログを確認します。"),
    ("F13", "現在のCDCの遅延は45秒です。原因を調べます。"),
    ("REG-1", "DLQ 상태를 확인해 보겠습니다. 궁금한 점이 있으면 알려 주세요."),
    ("REG-1", "DLQの状態を確認します。他に気になる点があれば教えてください。"),
    ("REG-2", "I'll start by taking a look at the DLQ samples for app.orders."),
    ("REG-2", "Let me do some digging into the DLQ."),
    ("REG-3", "Here's what I'll do: check the DLQ samples for app.orders and the CDC lag."),
    ("REG-4", "原因はいくつか考えられます。まずDLQのサンプルを確認します。"),
    ("REG-4", "app.ordersはMANUALに分類されています。理由を確認します。"),
    ("REG-5", "That makes sense. Before you cut over, I'll check the DLQ and the CDC lag."),
    ("REG-5", "### Cut over readiness\nI'll check the DLQ and the CDC lag before you cut over."),
    ("REG-5", "I'll check the index build later. I'll check the DLQ before you cut over."),
    ("REG-6", "All 11 tables match. Before you cut over, I'll check the DLQ now."),
    ("REG-6", "The sink stalled. Let me check what happened after you cut over."),
    ("REG-7", "Let me pull the DLQ samples and check the sink logs if needed."),
]
_ANSWERS = [
    ("F4", "I'll make sure the team can review it: the DDL is in the Schema Conversion step."),
    ("F4", "I'll add a note so the next reviewer can see why the view is MANUAL."),
    ("F5", "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다. "
           "필요한 경우 다른 테이블도 확인해 드리겠습니다."),
    ("F5", "app.ordersのCHECKSUMが一致しており、欠落行もないので、カットオーバーできます。必要とあれば、"
           "他のテーブルも確認します。"),
    ("F6", "Full Load가 마지막 배치 직전에 중단되어 app.orders의 행 3개가 누락되었습니다. Data "
           "Migration에서 다시 로드하세요. 다시 로드하신 후에 몇 개의 행이 복구되었는지 확인해 드리겠습니다."),
    ("F6", "Full Loadが最後のバッチの直前で止まったため、app.ordersの3行が欠落しています。Data "
           "Migrationで再ロードしてください。再ロードの後に、何行が復旧したか確認します。"),
    ("F8", "Before you cut over, I'll check the DLQ once more. As of the last validation, all 11 "
           "tables match and the CDC lag is 0 s, so nothing blocks you."),
    ("F9", "app.orders converts automatically: its key becomes an identity and every column "
           "maps. I'll check the plan again after the updated view runs."),
    ("F10", "ステップ4のValidationについて説明します。各テーブルの行数が一致するか確認します。次に、"
            "チェックサムが同じか確認します。"),
    ("F11", "I'll need to see your source table definition to tell you why it fails. Which table "
            "do you mean?"),
    ("F11", "I'll need to see your source table definition to tell you why it fails. Can you "
            "tell me which table it is?"),
    ("F14", "Full Load가 마지막 배치 직전에 중단되어 app.orders의 행 3개가 누락되었습니다. 다시 로드한 "
            "후 CHECKSUM을 다시 확인해 드리겠습니다."),
    ("REG-8", "The sink is healthy and the lag is 0 s. I'll make sure to check the DLQ when you "
              "cut over."),
    ("REG-8", "app.orders is missing 3 rows; reload it from Data Migration. I'll look at why it "
              "stalls if it stalls again."),
    ("REG-9", "Validationでは行数を比較しますが、チェックサムも確認します。"),
    ("REG-10", "I'll tell you what I see: the DLQ holds 3 records, all 23505 duplicates from a "
               "re-snapshot, so nothing needs repair."),
    ("REG-10", "Let me summarize the situation and then look at your options: reload app.orders, "
               "or skip the 3 rows and fix them by hand."),
    ("REG-11", "If it recurs, I'll look at the sink logs. This time the cause was a broker "
               "restart: the sink paused for 40 s and caught up on its own."),
    ("REG-12", "app.orders is missing 3 rows because the Full Load stopped early. I'll check the "
               "CHECKSUM after we reload it."),
    ("REG-13", "かしこまりました、すべてのテーブルが一致しているので、カットオーバーできます。カットオーバー"
               "の前に、DLQを確認します。"),
]


class _Model:
    """Bedrock as the API keeps tool_choice: asked for a tool call ("any" / "tool"), the turn is
    one, never text -- a punt calls the look-up it announced and then answers, an answer comes
    through the answer tool. Allowed to end its turn in text, the model gives ``reply``, as the
    reviews saw; nudged, it looks something up, and the round after that is answered for a punt
    and throttled for an answer."""

    def __init__(self, reply: str, punt: bool) -> None:
        self.reply, self.punt, self.calls = reply, punt, 0

    def invoke_model(self, **kw):  # noqa: ANN003
        self.calls += 1
        body = json.loads(kw["body"])
        looked = any(
            isinstance(block, dict) and block.get("type") == "tool_result"
            for message in body["messages"] if isinstance(message["content"], list)
            for block in message["content"]
        )
        if body.get("tool_choice", {}).get("type") in ("any", "tool"):
            if self.punt and not looked:
                step = _look()
            else:
                step = _answer(_REAL if self.punt else self.reply)
        elif self.calls == 1:
            step = _text(self.reply)
        elif not looked:
            step = _look()
        elif self.punt:
            step = _text(_REAL)
        else:
            raise _ClientError("ThrottlingException")
        return {"body": io.BytesIO(json.dumps(step).encode())}


def _ids(cases: list) -> list:
    return [f"{problem}-{i}" for i, (problem, _reply) in enumerate(cases)]


@pytest.mark.parametrize(("problem", "punt"), _PUNTS, ids=_ids(_PUNTS))
def test_a_reply_that_would_have_been_a_punt_can_no_longer_end_a_turn(
    problem: str, punt: str
) -> None:
    """Each was shown as the whole reply after one Bedrock call."""
    client = _Model(punt, punt=True)
    outcome, shown, ran = _chat(client)
    assert outcome.available and shown == [_REAL] and ran and client.calls == 2, problem


@pytest.mark.parametrize(("problem", "answer"), _ANSWERS, ids=_ids(_ANSWERS))
def test_a_complete_answer_is_the_reply_after_one_round(problem: str, answer: str) -> None:
    """Each was nudged; the model looked something up, that round was throttled, and the answer
    was lost to 'AI reply unavailable'."""
    client = _Model(answer, punt=False)
    outcome, shown, ran = _chat(client)
    assert outcome.available and shown == [answer] and not ran and client.calls == 1, problem


def test_the_panel_shows_the_answer_once_as_the_reply() -> None:
    """End to end: the panel turn is one reply, the answer tool's Markdown, after the look-up."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    client = _bedrock([_look(), _answer(_REAL)])

    def streamer(messages, on_delta):  # noqa: ANN001
        return AssessmentStrategist(state.ai_assist, client=client).tool_chat(
            "sys", messages, on_delta, tools=_TOOLS, execute=lambda _n, _i: '{"samples": []}'
        )

    panel = build_ai_panel(ui, state=state)
    panel.open_scope(scope_id="cdc:dlq:app.orders", title="AI DBA", streamer=streamer,
                     seed_question="Why is app.orders missing rows?")
    _pump(ui, max_iters=400)
    turns = [
        (m["role"], m["text"]) for m in state.ai_conversation.messages
        if m.get("role") in ("user", "assistant")
    ]
    assert turns == [("user", "Why is app.orders missing rows?"), ("assistant", _REAL)]


def test_the_chat_without_tools_is_unchanged() -> None:
    """stream_chat has no tools: no answer tool, no tool_choice, and no word of either."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    sent: list = []

    class Streaming:
        def invoke_model_with_response_stream(self, **kw):  # noqa: ANN003
            sent.append(json.loads(kw["body"]))
            event = {"type": "content_block_delta", "delta": {"text": "ok"}}
            return {"body": [{"chunk": {"bytes": json.dumps(event).encode()}}]}

    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=Streaming()).stream_chat(
        "sys", [{"role": "user", "text": "hi"}], lambda _d: None
    )
    assert outcome.markdown == "ok"
    assert "tools" not in sent[0] and "tool_choice" not in sent[0]
    assert "answer tool" not in sent[0]["system"]
