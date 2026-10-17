"""G104: the review of G97-G100 (AI) -- the punt detector no longer reads an indirect question,
a moment already past or part of a word as a deferral, nor a sentence that only motivates the
look-up as an answer; Japanese openers and embedded questions are no subject, a weak verb after
a helper phrase is a look-up, and a Korean / Japanese offer or recommendation is part of an
answer. A punt the model confirmed by looking things up is never shown as the answer when those
look-ups fail, identical queued asks run once, and a send is open only while it can be asked."""

from __future__ import annotations

import io
import json
import threading

import pytest

from tests.test_ai_panel import _Ui, _enabled_state, _make_streamer, _pump

# A corpus of realistic replies: the model announced a look-up and ended its turn without
# calling a tool (PUNTS, to be nudged), or answered (ANSWERS, never nudged). It holds the
# shapes earlier reviews recorded and the ones the G97-G100 review found shown as the whole
# reply.
PUNTS_EN = [
    # An indirect question or a moment already past, then the sentence that motivates it.
    "Let me check the dead-letter queue to see if you have any 23505 records. This will show "
    "what is blocking you before you cut over.",
    "Let me check the DLQ to determine if it has any failed records. This will show what is "
    "blocking you before you cut over.",
    "Let me look into why the sink stalled when it reconnected. That should explain the gap "
    "you're seeing.",
    "Let me check what happened to the sink after the Full Load finished. That tells us whether "
    "anything was dropped.",
    "Let me look at when the DLQ records started arriving. That will tell us which change caused "
    "them.",
    "Let me check if the sink connector is running. This will tell us whether the DLQ is still "
    "growing.",
    "I'll check the current CDC state to see if the sink has caught up. This will help us "
    "understand whether it's safe to cut over.",
    "Let me check if the table has a primary key. That determines whether Aurora DSQL can load it.",
    "I'll pull the validation summary to see if the mismatches are still there. Then I can tell "
    "you exactly what to fix.",
    "Let me look at the DLQ samples to see if the errors are FK violations. That would explain "
    "why the sink keeps dead-lettering rows.",
    "I'll fetch the CDC diagnostics to see if it is the sink or the source connector. Each one "
    "has a different fix.",
    # Tied to a moment still to come, with nothing but a motive beside it.
    "I'll check your assessment, schema conversion, data load, CDC health and validation state "
    "to find what is blocking you before you cut over. This will tell us whether it is safe to "
    "cut over.",
    "Before you cut over, I'll check the DLQ, the CDC lag and the validation verdicts. That way "
    "I can tell you exactly what is still open.",
    "Let me check the DLQ before you cut over. If records remain in it, the cut over is blocked.",
    # A weak verb after a helper phrase, and -ing forms.
    "Let me go ahead and review the converted DDL for app.orders.",
    "I'll need to review the converted DDL first.",
    "Let me run a quick query to see what landed on the target.",
    "I'll take a moment to review the Schema Apply result.",
    "I'll start by checking the assessment results for app.orders.",
    "Let me start by looking at the Full Load status for shop.orders.",
    # Plain ones.
    "Let me check the DLQ samples for app.orders. One moment.",
    "I'm going to look at the validation results for app.orders to find where the mismatch "
    "comes from.",
    "To answer that I need the current Full Load status. Give me a moment.",
    "Good question. Let me check if the view exists on the target first, since Schema Apply may "
    "have skipped it.",
]
PUNTS_KO = [
    "Full Load 후에 행 수를 확인해 보겠습니다. 이를 통해 누락된 데이터가 있는지 알 수 있습니다.",
    "컷오버 전에 DLQ 상태를 확인해 보겠습니다. DLQ에 남은 레코드가 있으면 컷오버를 진행할 수 없습니다.",
    "CDC를 배포한 후에 싱크에 무슨 일이 있었는지 확인해 보겠습니다. 이 정보로 데이터 누락 여부를 판단할 수 "
    "있습니다.",
    "이전에 실패한 테이블 목록을 확인해 보겠습니다. 이를 통해 누락된 데이터가 있는지 알 수 있습니다.",
    "싱크가 재시작된 후에 무슨 일이 있었는지 확인해 보겠습니다. 그러면 누락된 행의 원인을 알 수 있습니다.",
    "먼저 현재 CDC 상태를 확인해 보겠습니다.",
    "DLQ 샘플을 조회해 보겠습니다. 잠시만 기다려 주세요.",
    "app.orders의 변환된 DDL을 살펴보겠습니다.",
    "컷오버하기 전에 평가, 스키마 변환, 데이터 로드, CDC 상태와 검증 결과를 확인하겠습니다. 이를 통해 지금 "
    "컷오버해도 되는지 알 수 있습니다.",
    "검증이 끝나면 결과를 알려드릴 수 있도록 지금 검증 요약을 조회해 보겠습니다. 잠시만 기다려 주세요.",
]
PUNTS_JA = [
    "まずは現在のCDCの状態を確認します。",
    "では、変換後のDDLを確認します。",
    "はい、DLQの状態を確認します。",
    "それでは、検証結果を確認します。",
    "ご質問ありがとうございます。まずはDLQのサンプルを確認します。",
    "承知しました。では、スキーマ適用の結果をチェックします。",
    "DLQにレコードが残っていないか確認します。",
    "CDCの遅延が解消されたか確認します。",
    "主キーが正しく変換されたかチェックします。",
    "移行の状態を確認します。少々お待ちください。",
    "DLQのサンプルを調べてみます。",
    "では、DLQの状態を確認しましょう。",
    "フルロードの後にシンクで何が起きたか確認します。これにより、欠落した行の原因がわかります。",
    "カットオーバーの前に、DLQとCDCの遅延と検証結果を確認します。これにより、カットオーバーを妨げているものが"
    "わかります。",
]
ANSWERS_EN = [
    "Your DLQ holds 3 records; they need no repair. I'll review it again if a new code appears.",
    "app.orders converts automatically. Once Schema Apply has run, I'll be able to check the "
    "index build.",
    "The table is ready to migrate: its key is an identity and every column maps. Let me know if "
    "you'd like me to review the trigger body.",
    "The DLQ holds 3 records, all 23505 on app.users. If you're going to cut over today, check "
    "the DLQ depth first.",
    "Full Load runs at 8K rows/s. Let's look at throughput instead of the index.",
    "Let me explain why you see it missing: the view reads the table before the load.",
    "Let me explain what the CHECKSUM check does: it hashes every row on both sides and compares "
    "the two.",
    "I'll keep the query as is; it already uses the primary key.",
    "To answer that, I need to know which schema you migrated it to.",
    "To answer that, Validation (Step 4) has to run first -- it compares row counts and checksums "
    "for every table.",
    "You're right, I need to correct that: Aurora DSQL supports and enforces foreign keys.",
    "The sink is healthy and the DLQ is empty, so you can cut over. I'll check the DLQ before you "
    "cut over.",
    "The DLQ has 2 records, both 23503 on app.order_items. I'll review them if you want.",
    "app.orders is missing 3 rows because the Full Load stopped before its last batch. Reload "
    "app.orders from Data Migration, then re-run Validation; I'll check the CHECKSUM again after "
    "you reload it.",
    "CDC lag is 0 s and every table matched its CHECKSUM, so nothing blocks cut over. If a new DLQ "
    "record appears before you cut over, I'll look at it then.",
    "The view v_orders failed because Aurora DSQL has no SUBSTRING_INDEX. Rewrite it with "
    "split_part(col, ',', 1) and re-run Schema Apply. I can check the other views too if you want.",
    "Yes. The DLQ is empty, the sink is RUNNING and Validation matched all 11 tables, so you can "
    "cut over now.",
    "No action is needed: those 23505 records are duplicates a re-snapshot replayed, and the "
    "target already holds the same rows.",
    "**Verdict:** ready to cut over.\n- DLQ: empty\n- CDC lag: 0 s\n- Validation: 11 of 11 tables "
    "match\n\nI'll check the DLQ once more when you start the cut over.",
]
ANSWERS_KO = [
    "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다. 필요하시면 "
    "다른 테이블도 확인해 드리겠습니다.",
    "DLQ에 23505 레코드가 3건 있지만 모두 재스냅샷으로 인한 중복이라 복구가 필요 없습니다. 원하시면 각 "
    "레코드를 자세히 분석해 드리겠습니다.",
    "이 뷰는 이름 충돌 때문에 생성되지 않습니다. 소스에서 이름을 줄여 주세요. 필요하시면 다른 뷰도 확인해 "
    "드리겠습니다.",
    "Validation 단계에서는 행 수와 체크섬을 확인합니다.",
    "DLQ는 비어 있고 CDC 지연도 0초이므로 지금 컷오버하셔도 됩니다. 컷오버 전에 DLQ를 한 번 더 확인해 보세요.",
    "app.orders의 행 3개가 누락되었습니다. Data Migration에서 app.orders를 다시 로드한 후 Validation을 다시 "
    "실행하세요. 다시 로드한 후에 CHECKSUM을 다시 확인해 드리겠습니다.",
    "외래 키는 Aurora DSQL에서 지원되며 적용됩니다. 추가로 궁금한 점이 있으면 말씀해 주세요.",
]
ANSWERS_JA = [
    "外部キーは親テーブルを参照します。",
    "Validation ステップでは行数とチェックサムを確認します。",
    "Validationでは、行数が一致するか確認します。",
    "app.ordersのCHECKSUMは一致しています。欠落行はないので、カットオーバーできます。次に検証結果を確認しましょう。",
    "DLQに3件ありますが、すべて再スナップショットによる重複なので修復は不要です。カットオーバーの前に、もう一度DLQ"
    "を確認しましょう。",
    "このビューは名前の衝突のため作成されません。ソース側で名前を短くしてください。必要であれば、他のビューも確認"
    "します。",
    "DLQは空で、CDCの遅延も0秒です。カットオーバーを進めて問題ありません。ご不明な点があればお知らせください。",
    "app.ordersの行が3件欠落しています。Data Migrationで再ロードしてから検証を再実行してください。再ロードの後に、"
    "CHECKSUMをもう一度確認します。",
]
PUNTS = PUNTS_EN + PUNTS_KO + PUNTS_JA
ANSWERS = ANSWERS_EN + ANSWERS_KO + ANSWERS_JA


@pytest.mark.parametrize("punt", PUNTS)
def test_punts_in_the_corpus_are_nudged(punt: str) -> None:
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert is_tool_preamble(punt)


@pytest.mark.parametrize("answer", ANSWERS)
def test_answers_in_the_corpus_are_not_nudged(answer: str) -> None:
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert not is_tool_preamble(answer)


@pytest.mark.parametrize(
    "punt",
    [
        # 'see if the', 'when it restarted', 'after the Full Load finished' name no moment to
        # wait for, and the second sentence only motivates the look-up.
        "I'll check the sink to see if the lag is gone. That should explain the gap you are seeing.",
        "I'll look at the DLQ samples from when it restarted. This will tell us which rows were lost.",
        "I'll pull the row counts from after the Full Load finished. Then I can tell you what to reload.",
        # 전에 inside 이전에 'previously', 後に inside 最後に 'finally'.
        "이전에 실패한 테이블 목록을 조회하겠습니다. 이 목록으로 다시 로드할 테이블을 고를 수 있습니다.",
        "最後に、検証結果を確認します。一致しない行があれば、テーブルごとに一覧にします。",
    ],
)
def test_a_look_up_that_names_no_moment_to_wait_for_is_no_promise(punt: str) -> None:
    """An indirect question, a moment already past, or part of a word read as 'tied to a
    moment', and the punt's own 'This will tell us ...' as the answer beside it: the punt
    was shown as the whole reply."""
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert is_tool_preamble(punt)


@pytest.mark.parametrize(
    ("reply", "nudged"),
    [
        ("まずはDLQのサンプルをチェックします。", True),
        ("では、変換後の主キーを確認します。", True),
        ("それでは、CDCの遅延を調べます。", True),
        ("はい、検証結果を取得します。", True),
        ("シンクにエラーが出ていないか確認します。", True),
        # A topic that governs the verb states what something does.
        ("シンクは主キーで既存の行を参照します。", False),
        ("Full Loadでは、行数が一致するか確認します。", False),
    ],
)
def test_a_japanese_opener_or_an_embedded_question_is_no_subject(reply: str, nudged: bool) -> None:
    """The は of まずは / では / それでは / はい and the が of an embedded 'whether' clause
    read as a subject, so the punt was taken for a statement of fact."""
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert is_tool_preamble(reply) is nudged


@pytest.mark.parametrize(
    ("reply", "nudged"),
    [
        ("Let me go ahead and review the Schema Apply log.", True),
        ("I'll need to review the DLQ samples before I answer.", True),
        ("Let me run a quick query to see how many rows landed.", True),
        ("I'll take a moment to review the converted view.", True),
        ("Let me begin by reviewing the assessment for app.orders.", True),
        ("I'll start by checking the CDC lag.", True),
        # Explaining is no look-up, however close a look-up verb follows.
        ("Let me explain what this check does: it compares a hash of every row on both sides.", False),
    ],
)
def test_a_weak_verb_after_a_helper_phrase_announces_a_look_up(reply: str, nudged: bool) -> None:
    """A weak verb counted only right after the cue, and 'checking' never: these punts were
    shown as the reply."""
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert is_tool_preamble(reply) is nudged


@pytest.mark.parametrize(
    ("reply", "nudged"),
    [
        ("검증 결과 모든 테이블이 일치합니다. 원하시면 DLQ도 확인해 드리겠습니다.", False),
        ("app.orders는 자동으로 변환됩니다. 필요하실 경우 인덱스도 검토해 드리겠습니다.", False),
        ("DLQは空で、CDCの遅延も0秒です。念のため、カットオーバーの前にDLQを確認しましょう。", False),
        ("欠落行はありません。すべてのテーブルでCHECKSUMが一致しています。次のデプロイの後に、もう一度確認します。", False),
        # An offer never hides a punt, and advice with nothing beside it is the punt.
        ("DLQ 샘플을 조회하겠습니다. 필요하시면 CDC 상태도 확인해 드리겠습니다.", True),
        ("まず、DLQのサンプルを確認しましょう。", True),
    ],
)
def test_a_korean_or_japanese_offer_or_recommendation_is_part_of_the_answer(
    reply: str, nudged: bool
) -> None:
    """'필요하시면 … 확인해 드리겠습니다' and '〜を確認しましょう' closing a complete answer were
    nudged, and a Japanese sentence of substance counted as one word."""
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    assert is_tool_preamble(reply) is nudged


class _ClientError(Exception):
    """botocore-shaped: the classifier reads only ``response``."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.response = {"Error": {"Code": code, "Message": "x"}}


_PUNT = "Let me check the DLQ samples for app.orders."
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
def test_a_punt_the_model_confirmed_by_looking_things_up_is_never_the_answer(
    after_the_look_up, reason: str
) -> None:
    """Nudged, the model called a tool -- so its first reply was no answer -- and the next
    round failed: that punt came back as a successful reply."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    punt = {"stop_reason": "end_turn", "content": [{"type": "text", "text": _PUNT}]}
    shown: list = []
    ran: list = []
    outcome = AssessmentStrategist(
        AiAssistConfig(enabled=True), client=_bedrock([punt, _LOOK, after_the_look_up])
    ).tool_chat(
        "sys", [{"role": "user", "text": "Why is app.orders missing rows?"}], shown.append,
        tools=_TOOLS, execute=lambda name, _i: ran.append(name) or '{"samples": []}',
    )
    assert ran and not outcome.available and outcome.reason == reason
    assert shown == [] and outcome.markdown == ""


def test_the_panel_reports_the_failed_look_up_and_latches_its_denial() -> None:
    """The operator was left with 'Let me check ...' as a normal reply: no 'AI reply
    unavailable', the question kept, and the expired token of that round never latched."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.ui.ai_panel import build_ai_panel

    state = _enabled_state()
    ui = _Ui()
    punt = {"stop_reason": "end_turn", "content": [{"type": "text", "text": _PUNT}]}
    client = _bedrock([punt, _LOOK, _ClientError("ExpiredTokenException")])

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
