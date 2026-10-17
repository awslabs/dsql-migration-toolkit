"""G93: the review of G91 -- a nudge that can keep an answer, so the preamble detector errs
toward nudging; a send bound to its scope INSTANCE; a chat per Query Converter query; a
not-converted view's note with every reason."""

from __future__ import annotations

import io
import json
from types import SimpleNamespace

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump
from tests.test_g72_ai_review import _executor


def test_punts_in_every_shape_are_nudged() -> None:
    """Two-sentence punts, plans of questions or '**Area**: what' items, look-ups of what
    already happened, 'again', and Korean / Japanese punts were shown as the answer."""
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    for punt in (
        "I'll check your assessment, schema conversion, data load, CDC health and validation "
        "state. This will show what is blocking you before you cut over.",
        "Let me pull your real migration state. That way I can name the exact tables "
        "blocking cut over.",
        "I'll start with the assessment results. Then I'll look at the data load and CDC health.",
        "Let me check a few things first:\n1. Is CDC caught up?\n2. Are there DLQ records?",
        "I'll check these areas:\n- **Assessment**: blockers on your tables\n- **CDC**: lag",
        "Let me check what happened to the sink after you started the Full Load.",
        "Let me look into why the sink stalled when it reconnected.",
        "Let me check the DLQ to determine if it has any failed records.",
        "Let me check the DLQ again.",
        "현재 마이그레이션 상태를 확인해 보겠습니다.",
        "移行の状態を確認します。少々お待ちください。",
    ):
        assert is_tool_preamble(punt), punt


def test_a_correction_or_a_question_to_the_operator_is_not_nudged() -> None:
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    for answer in (
        "To answer that, Validation (Step 4) has to run first -- it compares row counts "
        "and checksums for every table.",
        "You're right, I need to correct that: Aurora DSQL supports and enforces foreign keys.",
        "I need to know which table you mean.",
        "Your DLQ holds 3 records; they need no repair. I'll review it again if a new code appears.",
        "The DLQ is empty. I'll check it before you cut over.",
    ):
        assert not is_tool_preamble(answer), answer


def _tool_chat(replies: list):
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    sent: list = []

    class Bedrock:
        def invoke_model(self, **kw):  # noqa: ANN003
            sent.append(json.loads(kw["body"])["messages"])
            out = replies[min(len(sent), len(replies)) - 1]
            return {"body": io.BytesIO(json.dumps(out).encode())}

    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=Bedrock()).tool_chat(
        "sys", [{"role": "user", "text": "What's next?"}], lambda _d: None,
        tools=[{"name": "get_dlq", "description": "x",
                "input_schema": {"type": "object", "properties": {}}}],
        execute=lambda _n, _i: '{"dlq": 0}',
    )
    return outcome, sent


def _text(text: str) -> dict:
    return {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}


def test_a_nudge_lets_the_model_keep_its_answer_or_look_things_up() -> None:
    from dsql_migrator.core.assessment_strategist import KEEP_REPLY_MARKER

    answer = "Let me look at that. app.orders converts automatically."
    outcome, sent = _tool_chat([_text(answer), _text(f"`{KEEP_REPLY_MARKER}`")])
    assert outcome.markdown == answer and len(sent) == 2
    assert KEEP_REPLY_MARKER in sent[1][-1]["content"][0]["text"]

    outcome, sent = _tool_chat([
        _text("Let me check the DLQ."),
        {"stop_reason": "tool_use", "content": [
            {"type": "tool_use", "id": "t1", "name": "get_dlq", "input": {}}]},
        _text("The DLQ is empty."),
    ])
    assert outcome.markdown == "The DLQ is empty." and len(sent) == 3


def _panel():
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    notices: list = []
    ui.notify = lambda message, **_k: notices.append(message)  # type: ignore[method-assign]
    general: list = []
    panel = build_ai_panel(
        ui, state=state,
        general_streamer_factory=lambda: _make_streamer("GENERAL", record=general),
    )
    return state, ui, panel, notices, general


def test_a_send_asks_only_while_its_scope_instance_is_open() -> None:
    """Bound to a scope id, a re-test still landed in the SAME id reopened in a new window,
    in the new journey after Start over, and in the general chat after AI was re-homed."""
    state, ui, panel, notices, general = _panel()
    asked: list = []
    send = panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                            streamer=_make_streamer("A", record=asked), seed_question="Tune Q1")
    _pump(ui)
    panel.open_scope(scope_id="readiness", title="AI DBA", streamer=_make_streamer("R"),
                     seed_question="What next?")
    _pump(ui)
    reopened: list = []
    panel.open_scope(scope_id="query-converter:tune:q1", title="AI DBA",
                     streamer=_make_streamer("A2", record=reopened), seed_question="Tune Q1")
    _pump(ui)
    send("RETEST of the first window's rewrite")
    _pump(ui)
    assert len(reopened) == 1 and notices  # refused, and the operator is told

    # Start over: a send of the old journey never asks in the new one.
    notices.clear()
    send2 = panel.open_scope(scope_id="query-converter:tune:q2", title="AI DBA",
                             streamer=_make_streamer("B"), seed_question=None)
    state.ai_conversation.reset()
    panel.reset()
    send2("RETEST after Start over")
    assert notices and not state.ai_conversation.messages

    # Re-homed to general (AI off and on, panel reopened): the tune send is refused there.
    notices.clear()
    send3 = panel.open_scope(scope_id="query-converter:tune:q3", title="AI DBA",
                             streamer=_make_streamer("C"), seed_question=None)
    state.ai_assist.enabled = False
    send3("anything")  # inert while AI is off: the composer's streamer is dropped
    state.ai_assist.enabled = True
    panel.set_visible(True)
    send3("RETEST after AI came back")
    _pump(ui)
    assert not any("RETEST" in str(m) for call in general for m in call)
    assert notices


def test_each_query_opens_its_own_chat() -> None:
    from dsql_migrator.core.models import Classification
    from dsql_migrator.core.query_converter import QueryConversionResult, StatementKind
    from dsql_migrator.ui.query_playground import _query_scope_key

    def result(sql: str) -> QueryConversionResult:
        return QueryConversionResult(original_sql=sql, converted_sql=sql,
                                     classification=Classification.AUTO,
                                     statement_kind=StatementKind.SELECT)

    assert _query_scope_key(result("SELECT 1")) != _query_scope_key(result("SELECT 2"))
    assert _query_scope_key(result("SELECT 1")) == _query_scope_key(result("SELECT 1"))


def test_a_not_converted_views_note_names_every_reason() -> None:
    """A materialized view whose name also collides was told only to shorten the names."""
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef
    from dsql_migrator.ui.schema_conversion import SchemaConversionState

    long = "daily_totals_" + "x" * 55
    inventory = SourceInventory(
        tables=[TableDef(name=f"shop.{long}_a", columns=[ColumnDef(name="id", mysql_type="int",
                                                                      nullable=False)],
                         primary_key=["id"])],
        views=[ViewDef(name=f"shop.{long}_b", unsupported_kind="materialized view")],
    )
    execute = _executor(SimpleNamespace(inventory=inventory, assessment=None), SchemaConversionState())
    note = json.loads(execute("get_converted_ddl", {"object_name": f"shop.{long}_b"}))["note"]
    assert "materialized view" in note and "Shorten the names on the source" in note
