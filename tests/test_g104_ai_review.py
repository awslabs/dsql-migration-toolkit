"""G104: the review of G97-G100 (AI) -- a look-up round that fails is the outcome, its denial
latched; identical queued asks run once, and a send is open only while it can be asked. (Its
punt-detector tests went with the detector in G116: tool_chat's reply comes through an answer
tool, test_g116_ai_protocol.)"""

from __future__ import annotations

import io
import json
import threading

import pytest

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump


class _ClientError(Exception):
    """botocore-shaped: the classifier reads only ``response``."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code, "Message": "x"}}


_LOOK = {"stop_reason": "tool_use",
         "content": [{"type": "tool_use", "id": "t1", "name": "list_cdc_dlq_samples", "input": {}}]}
_TOOLS = [{"name": "list_cdc_dlq_samples", "description": "x",
           "input_schema": {"type": "object", "properties": {}}}]


def _bedrock(script: list):
    """A Bedrock client answering each call with the next step of ``script`` (a reply body,
    or an exception to raise); the last step repeats."""

    class Bedrock:
        calls = 0

        def invoke_model(self, **_kw):  # noqa: ANN003
            Bedrock.calls += 1
            step = script[min(Bedrock.calls, len(script)) - 1]
            if isinstance(step, Exception):
                raise step
            return {"body": io.BytesIO(json.dumps(step).encode())}

    return Bedrock()


@pytest.mark.parametrize(
    ("after_the_look_up", "reason"),
    [
        (_ClientError("ThrottlingException"), "THROTTLED"),
        (_ClientError("ExpiredTokenException"), "ACCESS_DENIED"),
        ({"stop_reason": "end_turn", "content": []}, "INVALID_OUTPUT"),
        (_LOOK, "UNAVAILABLE"),  # it keeps looking things up until the rounds run out
    ],
)
def test_a_failed_round_after_a_look_up_is_the_outcome(after_the_look_up, reason: str) -> None:
    """The model called a tool and the next round failed: nothing is shown as the answer, and
    the failure's reason is the outcome."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    shown: list = []
    ran: list = []
    outcome = AssessmentStrategist(
        AiAssistConfig(enabled=True), client=_bedrock([_LOOK, after_the_look_up])
    ).tool_chat(
        "sys", [{"role": "user", "text": "Why is app.orders missing rows?"}], shown.append,
        tools=_TOOLS, execute=lambda name, _i: ran.append(name) or '{"samples": []}',
    )
    assert ran and not outcome.available and outcome.reason == reason
    assert shown == [] and outcome.markdown == ""


def test_the_panel_reports_the_failed_look_up_and_latches_its_denial() -> None:
    """A look-up round that fails is the turn's outcome in the panel: 'AI reply unavailable',
    the question rolled back, and the expired token of that round latched."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    client = _bedrock([_LOOK, _ClientError("ExpiredTokenException")])

    def streamer(messages, on_delta):  # noqa: ANN001
        return AssessmentStrategist(state.ai_assist, client=client).tool_chat(
            "sys", messages, on_delta, tools=_TOOLS, execute=lambda _n, _i: '{"samples": []}'
        )

    panel = build_ai_panel(ui, state=state)
    panel.open_scope(scope_id="cdc:dlq:app.orders", title="AI DBA", streamer=streamer,
                     seed_question="Why is app.orders missing rows?")
    _pump(ui, max_iters=400)
    assert any(el.text == "AI reply unavailable" for el in ui.labels)
    assert state.ai_verified is False
    assert not [m for m in state.ai_conversation.messages if m.get("role") in ("user", "assistant")]


def test_the_same_ask_queued_twice_while_a_reply_streams_is_asked_once() -> None:
    """'What's next?' clicked twice while another chat's reply streamed (its question not
    shown yet, so the header's re-ask check could not see it) ran two identical briefings."""
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    gate = threading.Event()
    asked: list = []

    def streamer(messages, on_delta):  # noqa: ANN001
        asked.append(messages[-1]["text"])
        if len(asked) == 1:
            gate.wait(5)
        on_delta("reply")
        return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown="reply")

    panel.open_scope(scope_id="schema_conversion:app.orders", title="AI DBA", streamer=streamer,
                     seed_question="Why is app.orders MANUAL?")
    for _ in range(2):
        send = panel.open_scope(scope_id="readiness", title="AI DBA", streamer=streamer,
                                seed_question=None)
        send("What should I do next?")
    gate.set()
    for _ in range(4):
        _pump(ui, max_iters=400)
    assert asked == ["Why is app.orders MANUAL?", "What should I do next?"]


def test_a_send_is_open_only_while_its_chat_can_ask() -> None:
    """With AI Assist turned off (and after it came back, until the chat was opened again)
    is_open() said True and the ask was then dropped without a word."""
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    notices: list = []
    ui.notify = lambda message, **_k: notices.append(message)  # type: ignore[method-assign]
    panel = build_ai_panel(ui, state=state)
    asked: list = []

    def open_tune():  # noqa: ANN202
        return panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                                streamer=_make_streamer("rewrite", record=asked),
                                seed_question="Tune Q1")

    send = open_tune()
    _pump(ui)
    assert send.is_open()
    state.ai_assist.enabled = False  # turned off on Connect
    assert not send.is_open()
    send("RETEST")
    assert len(asked) == 1 and "AI Assist is off" in notices[-1]
    state.ai_assist.enabled = True  # back on, but the chat lost its streamer meanwhile
    assert not send.is_open()
    send("RETEST")
    assert len(asked) == 1 and "no longer open" in notices[-1]
    open_tune()  # opened again: the same instance, live again
    assert send.is_open()


def test_test_rewrite_runs_no_probe_for_a_re_test_that_would_not_be_asked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With AI Assist off, Test rewrite on target still ran the rewrite's EXPLAIN ANALYZE on
    the target, and its ask was then dropped."""
    import asyncio
    import sys
    import types

    import dsql_migrator.core.assessment_strategist as strategist_mod
    from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
    from dsql_migrator.core.models import Classification
    from dsql_migrator.core.query_converter import QueryConversionResult, StatementKind
    from dsql_migrator.core.query_playground import ExecutionProbe, ProbeMode, ProbeOutcome
    from dsql_migrator.ui import query_playground as qp
    from dsql_migrator.ui.ai_assist import build_ai_assist_config
    from dsql_migrator.ui.ai_panel import build_ai_panel
    from dsql_migrator.ui.query_playground import PlaygroundStore
    from dsql_migrator.ui.session import SessionStore
    from tests.test_ui_connect_ai_status import _io_bound
    from tests.test_ui_connect_ai_status import _Ui as _ScreenUi

    reply = "Cheaper on DSQL:\n```sql\nSELECT id FROM app.orders WHERE id > 10\n```"

    class _Strategist:
        def __init__(self, *_a, **_k) -> None:  # noqa: ANN002, ANN003
            pass

        def stream_chat(self, _system, _messages, on_delta, **_k):  # noqa: ANN001, ANN003
            on_delta(reply)
            return ObjectGuidanceOutcome(available=True, reason="OK", detail="", markdown=reply)

        tool_chat = stream_chat

    probed: list = []
    monkeypatch.setattr(strategist_mod, "AssessmentStrategist", _Strategist)
    monkeypatch.setattr(qp, "probe_statement", lambda *a, **_k: probed.append(a))
    store = SessionStore()
    session = store.get_or_create("s")
    session.set_ai_assist(build_ai_assist_config(enabled=True, model_id="model-a"))
    playground = PlaygroundStore()
    pstate = playground.get_or_create("s")
    pstate.result = QueryConversionResult(original_sql="SELECT 1", converted_sql="SELECT 1",
                                          classification=Classification.AUTO,
                                          statement_kind=StatementKind.SELECT)
    pstate.set_probe(ExecutionProbe(outcome=ProbeOutcome.PASSED, mode=ProbeMode.EXPLAIN))
    panel_ui = _Ui()
    panel = build_ai_panel(panel_ui, state=session)
    opened: list = []

    def open_ai_scope(**kwargs):  # noqa: ANN003, ANN202
        opened.append(kwargs)
        return panel.open_scope(**kwargs)

    screen_ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = screen_ui  # type: ignore[attr-defined]
    fake.run = types.SimpleNamespace(io_bound=_io_bound, cpu_bound=_io_bound)  # type: ignore[attr-defined]
    saved = sys.modules.get("nicegui")
    sys.modules["nicegui"] = fake
    try:
        content = qp.build_query_playground_screen(store, "s", playground_store=playground,
                                                   open_ai_scope=open_ai_scope)
        content(lambda: None)
        screen_ui.find_button("Tune with AI DBA").click()
        _pump(panel_ui)
        retest = opened[-1]["footer_action"]

        session.set_ai_assist(build_ai_assist_config(enabled=False, model_id="model-a"))
        asyncio.run(retest(reply))
        assert probed == [] and "Enable AI Assist" in screen_ui.toasts[-1][0]

        for timer in panel_ui.timers:  # the panel notices AI is off and drops the chat's streamer
            if timer.interval == 0.25:
                timer.cb()
        session.set_ai_assist(build_ai_assist_config(enabled=True, model_id="model-a"))
        asyncio.run(retest(reply))
        assert probed == [] and "no longer open" in screen_ui.toasts[-1][0]
    finally:
        if saved is None:
            sys.modules.pop("nicegui", None)
        else:
            sys.modules["nicegui"] = saved
