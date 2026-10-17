"""G110: the review of G101-G105 (AI) -- the punt detector reads a reply by what it says before
its look-up: a punt announces first and explains after, an answer answers first and closes with
its promise, so nothing after the look-up makes a punt an answer and three words before it make
a promise part of one. An if / when after a verb of finding out, a purpose infinitive or a
wh-word asks a question; 'we' and 'until' name no moment to wait for, and only a Korean /
Japanese question that asks what happened cancels one. Japanese openers at any clause start,
frames, closed clauses and an embedded question's own は set no topic, and an earlier statement's
topic governs the verbs after it. A look-up word is the model's only as its own verb, and a reply
that asks the operator for a fact is no punt. The nudge keeps an answer that offers or promises a
later check. Every corpus the G101-G105 verifiers built is here, with a held-out one."""

from __future__ import annotations

import io
import itertools
import json

import pytest

from tests.test_g104_ai_review import _LOOK, _TOOLS, _bedrock, _ClientError


def _misread(texts, punt: bool) -> list:
    """The replies in ``texts`` the detector reads wrong: a punt it does not nudge (``punt``)
    or an answer it nudges."""
    from dsql_migrator.core.assessment_strategist import is_tool_preamble

    return [text for text in texts if is_tool_preamble(text) is not punt]


def _korean(text: str) -> bool:
    return any("가" <= ch <= "힣" for ch in text)


# ---- G99-DET-1 (P1): an if / when that asks a question, then the punt's own follow-up -------
P1_PUNTS = [
    "I'll check the current CDC status to understand if the sink has caught up. Once I have "
    "that, I can tell you whether it's safe to cut over.",
    "I'm going to pull the validation results to figure out if the mismatch is real. Based on "
    "that, I'll recommend the next step.",
    "I'll look at the DLQ samples to understand when the errors started. With that information, "
    "I can pinpoint the cause.",
    "I'll review the Full Load status to understand if any tables failed. Knowing that will help "
    "us decide whether a reload is needed.",
    "I'll query the target to identify if any rows are missing. That information will tell us "
    "whether a reload is needed.",
    "I'll check the sink connector to understand if it is still running. This information will "
    "help me give you an accurate answer.",
    "I'll pull the CDC metrics to figure out when the lag started. From that I can work out what "
    "caused it.",
    "I'll check the Schema Apply log to understand if the view was created. If it was skipped, "
    "the log will say why.",
    "I'm going to look at the DLQ to assess if the records are safe to ignore. I want to give "
    "you an answer based on the actual data.",
    "I'll inspect the validation summary to understand if the CHECKSUM mismatch is still there. "
    "Based on what I find, I'll suggest the fix.",
    "I'll check the row counts for the tables after the Full Load. I want to make sure nothing "
    "was dropped.",
    "I will fetch the connector status to understand if the task is failing. Once I know, I'll "
    "walk you through the fix.",
]
_P1_CUES = ["I'll", "I'm going to", "I will"]
_P1_LOOKS = [
    "check the CDC metrics",
    "pull the sink status",
    "look at the connector logs",
    "inspect the DLQ samples",
]
_P1_PURPOSES = [
    " to understand if the sink has fallen behind",
    " to figure out if the sink is still lagging",
    " to work out when the lag started",
    " to find when the lag started",
    " to learn if it is still lagging",
    " to judge if the stream is healthy",
    " to assess if the sink has caught up",
    " to make sure the sink is running",
    " to check whether the sink is running",
    " to see if any records are stuck",
    " to tell if we lost any rows",
    " for the window after the Full Load",
    " to see what happens when the sink restarts",
    " to see whether the lag clears if the sink is restarted",
    " from before the sink restart",
    " to identify when the errors began",
]
_P1_FOLLOWUPS = [
    " Lag usually builds up when the source runs a large transaction.",
    " Once I have that, I can tell you whether it is safe to cut over.",
    " That way we will know whether anything needs a reload.",
    " This should give us a clear picture of where the rows went.",
    " That lets me tell you exactly what to fix.",
    " Then we'll know whether it is safe to cut over.",
    " Hold on a moment.",
]


def test_a_look_up_whose_if_or_when_asks_a_question_is_nudged_whatever_follows() -> None:
    """'to understand / figure out / assess if the ...', 'after the Full Load' read as a moment
    still to come, and the punt's own 'Once I have that, I can tell you ...' as the answer beside
    it: the punt was shown as the whole reply."""
    templated = [
        f"{cue} {look}{purpose}.{follow}"
        for cue, look, purpose, follow in itertools.product(
            _P1_CUES, _P1_LOOKS, _P1_PURPOSES, _P1_FOLLOWUPS
        )
    ]
    assert _misread(P1_PUNTS, True) == []
    assert len(templated) == 1344 and _misread(templated, True) == []


# ---- P2: 'any' / 'we' / 'they' after a purpose infinitive ---------------------------------
_P2_CUES = ["I'll", "I'm going to"]
_P2_LOOKS = ["query the target", "check the DLQ samples", "pull the validation summary"]
_P2_PURPOSES = [
    " to identify if any rows are missing",
    " to understand if any records are stuck",
    " to figure out if we lost any rows",
    " to work out if they match the source",
    " to understand if any tables still differ",
]
_P2_FOLLOWUPS = [
    " That information will tell us whether a reload is needed.",
    " Based on that, I'll recommend the next step.",
    " Once I have that, I can tell you whether it is safe to cut over.",
]


def test_an_if_after_a_purpose_infinitive_asks_a_question_whatever_its_subject() -> None:
    """'to identify if any rows are missing', 'to work out if they match the source' read as tied
    to a moment once 'any' / 'we' / 'they' became deferral subjects: shown as the reply."""
    templated = [
        f"{cue} {look}{purpose}.{follow}"
        for cue, look, purpose, follow in itertools.product(
            _P2_CUES, _P2_LOOKS, _P2_PURPOSES, _P2_FOLLOWUPS
        )
    ]
    assert len(templated) == 90 and _misread(templated, True) == []


# ---- G99-DET-2 (P3): Japanese openers, frames and embedded questions ----------------------
P3_PUNTS = [
    "原因を調べるために、まずはDLQのサンプルを確認します。",
    "app.ordersがMANUALになっている理由を確かめるため、まずは評価結果を確認します。",
    "正確にお答えするために、まずは現在のCDCの状態を確認します。",
    "承知しました、では、DLQの状態を確認します。",
    "かしこまりました、それでは検証結果を確認します。",
    "わかりました、まずは変換後のDDLを確認します。",
    "カットオーバーできるかどうかを判断するため、まずはDLQとCDCの遅延を確認します。",
    "原因を特定するには、まずDLQのサンプルを確認します。",
    "正確にお答えするためには、現在のCDCの状態を確認します。",
    "カットオーバーの可否を判断するには、検証結果を確認します。",
    "この質問に答えるには、変換後のDDLを確認します。",
    "次は、検証結果を確認します。",
    "今度は、CDCの状態を確認します。",
    "最後は、DLQのサンプルを確認します。",
    "ひとまずは、DLQのサンプルを確認します。",
    "とりあえずは、変換後のDDLを確認します。",
    "今は、CDCの遅延を確認します。",
    "それではまずは、DLQの状態を確認します。",
    "app.ordersについては、変換後のDDLを確認します。",
    "この問題については、まずCDCの状態を確認します。",
    "app.ordersの件は、DLQのサンプルを確認します。",
    "DLQのレコードは残っていないか確認します。",
    "CDCの遅延は解消されたかどうかを確認します。",
    "シンクは正常に動いているか確認します。",
    "DLQのサンプルを見ながら、エラーの原因を確認します。",
    "したがって、DLQの状態を確認します。",
    "前回はFull Loadが失敗しましたが、まずDLQの状態を確認します。",
    "DLQのサンプルを確認します。",
    "では、DLQのサンプルを確認します。",
]
P3_FACTS = ["外部キーは親テーブルを参照します。", "Validationでは行数とチェックサムを確認します。", "シンクは主キーで既存の行を参照します。"]


def test_a_japanese_opener_frame_or_embedded_question_sets_no_topic() -> None:
    """まずは after a purpose clause, 〜するには, については, の件は, 次は / 今は, が inside ながら or
    したがって, a clause closed by 〜ましたが and an embedded question's own は read as a topic or
    subject stating a fact, so the punt was shown; a topic that governs the verb still states
    one."""
    assert _misread(P3_PUNTS, True) == []
    assert _misread(P3_FACTS, False) == []


# ---- G99-DET-3 (P4): a look-up word after any helper phrase -------------------------------
P4_PUNTS = [
    "I'll try to review the converted DDL for app.orders.",
    "I'll do a quick review of the converted DDL for app.orders.",
    "Let me run a query against the target to see what landed.",
    "Let me go ahead and run a quick query to see what landed on the target.",
    "I'm going to go ahead and review the Schema Apply log.",
    "Let me quickly go ahead and review the DLQ samples.",
    "I'll want to review the validation summary first.",
    "Let me also take a quick look at the DLQ.",
    "I'll first need to review the CDC status.",
    "Let me go and review the Full Load log.",
    "I'll be querying the target for the row counts.",
    "Let me just run a check to see whether the sink is healthy.",
    "I'll pull up and read the connector log.",
    "I'll query the target first.",
]
P4_ANSWERS = [
    "I'll keep the query as is; it already uses the primary key.",
    "Let me explain why you see it missing: the view reads the table before the load.",
    "Let me show you why the view fails: it reads a column that no longer exists.",
]


def test_a_look_up_word_after_any_helper_phrase_is_a_look_up() -> None:
    """'I'll try to review', 'I'll do a quick review of', 'Let me run a query against the target
    to see' had no look-up verb (only a closed list of helpers reached one): shown as the reply.
    A look-up word used as a noun ('keep the query as is') or by someone else stays none."""
    assert _misread(P4_PUNTS, True) == []
    assert _misread(P4_ANSWERS, False) == []


# ---- G99-DET-4 (P5): Korean / Japanese conditional offers --------------------------------
_P5_FACTS_KO = [
    "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다.",
    "DLQ에 23505 레코드가 3건 있지만 모두 재스냅샷으로 인한 중복이라 복구가 필요 없습니다.",
]
_P5_OFFERS_KO = [
    " 필요하다면 다른 테이블도 확인해 드리겠습니다.",
    " 필요하면 다른 테이블도 확인해 드리겠습니다.",
    " 원하면 각 레코드를 자세히 분석해 드리겠습니다.",
    " 필요하신 경우 다른 테이블도 확인해 드리겠습니다.",
    " 더 궁금하시면 각 레코드를 분석해 드리겠습니다.",
    " 다른 테이블 이름을 알려 주시면 확인해 드리겠습니다.",
]
_P5_FACTS_JA = [
    "app.ordersのCHECKSUMは一致しています。欠落行はないので、カットオーバーできます。",
    "DLQに3件ありますが、すべて再スナップショットによる重複なので修復は不要です。",
]
_P5_OFFERS_JA = [
    "必要があれば、他のテーブルも確認します。",
    "ご要望があれば、他のテーブルも確認します。",
    "ご希望があれば、各レコードを詳しく確認します。",
    "必要なら、他のテーブルも確認します。",
    "必要に応じて、他のテーブルも確認します。",
]


def test_a_korean_or_japanese_conditional_offer_closes_an_answer() -> None:
    """필요하면 / 원하면 (nudged since G104 dropped bare 하면), 필요하다면, 필요하신 경우, 궁금하시면,
    알려 주시면, 必要なら, 必要に応じて, 必要があれば and ご要望があれば closing a complete answer
    were nudged, and the nudge invited the offered look-up."""
    answers = [fact + offer for fact, offer in itertools.product(_P5_FACTS_KO, _P5_OFFERS_KO)]
    answers += [fact + offer for fact, offer in itertools.product(_P5_FACTS_JA, _P5_OFFERS_JA)]
    assert _misread(answers, False) == []


# ---- P6: a closing promise in 해 보겠습니다 / 〜してみます, or with a whether clause ----------
_P6_PROMISES_KO = [
    " 컷오버 전에 DLQ를 한 번 더 확인해 보겠습니다.",
    " 다시 로드하신 후에 CHECKSUM을 다시 확인해 보겠습니다.",
    " Validation이 끝나면 결과를 다시 확인해 보겠습니다.",
]
_P6_PROMISES_JA = ["カットオーバーの前に、もう一度DLQを確認してみます。", "再ロードの後に、CHECKSUMをもう一度確認してみます。"]
P6_ANSWERS = [
    "The DLQ is empty and every table matched its CHECKSUM, so you can cut over. Before you cut "
    "over, I'll check whether any new DLQ record arrived.",
    "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다. 컷오버 전에 DLQ에 새 레코드가 들어왔는지 한 번 더 "
    "확인해 드리겠습니다.",
    "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다. 다시 로드하신 후에 행 수가 맞는지 다시 확인하겠습니다.",
    "app.ordersのCHECKSUMは一致しています。欠落行はないので、カットオーバーできます。カットオーバーの前に、DLQに新しいレコードがないか確認します。",
]


def test_a_promise_for_a_moment_still_to_come_closes_an_answer_in_any_form() -> None:
    """'컷오버 전에 … 확인해 보겠습니다', 'カットオーバーの前に…確認してみます' read as said for now, and a
    whether clause after the moment ('…들어왔는지', '…ないか') cancelled it, so a complete answer's
    closing promise was nudged; the English twin was not."""
    answers = [fact + p for fact, p in itertools.product(_P5_FACTS_KO, _P6_PROMISES_KO)]
    answers += [fact + p for fact, p in itertools.product(_P5_FACTS_JA, _P6_PROMISES_JA)]
    assert _misread(answers, False) == []
    assert _misread(P6_ANSWERS, False) == []


# ---- The regression verifier's templated corpora (G101-AI-REG-1 .. REG-7) -----------------
_R_CUES = ["Let me", "I'll", "I will", "I'm going to"]
_R_EXPLAIN = ["explain", "summarize", "outline", "recap", "describe", "clarify"]
_R_MIDDLE = ["what we know so far", "the likely cause", "the situation", "what this error means"]
_R_CONNECTOR = [", then", " and then", " and"]
_R_LOOKUP = [
    "check the DLQ for the failing rows",
    "pull the current validation results",
    "look at the CDC lag",
    "fetch the converted DDL for app.orders",
]


def test_an_explanation_then_a_look_up_is_nudged() -> None:
    """G101-AI-REG-1: 'Let me explain / summarize / outline ..., then check X' -- the explanation
    verb right after the cue dropped the whole cue, and the punt was shown as the answer."""
    punts = [
        f"{cue} {verb} {middle}{connector} {lookup}."
        for cue, verb, middle, connector, lookup in itertools.product(
            _R_CUES, _R_EXPLAIN, _R_MIDDLE, _R_CONNECTOR, _R_LOOKUP
        )
    ]
    assert len(punts) == 1152 and _misread(punts, True) == []


_R_CUE2 = ["I'll", "I will", "I'm going to"]
_R_MOMENT = [
    "before we proceed",
    "before we go further",
    "before we change anything",
    "before we decide",
    "before we move on",
]
_R_LOOK2 = [
    "check the current CDC state",
    "pull the DLQ samples for app.orders",
    "look at the validation verdicts",
]
_R_HYP = [
    "Debezium may have restarted the snapshot after the connector failed.",
    "The 23505 codes usually come from a re-snapshot of the same rows.",
    "A stalled sink task is the most common cause of missing rows.",
    "Foreign key violations are the usual reason the sink dead-letters a row.",
]


def test_before_we_names_the_moment_at_hand() -> None:
    """G101-AI-REG-2: 'Before we proceed, I'll check X.' and a hypothesis after it -- 'we' made the
    look-up tied to a moment still to come and the hypothesis the answer beside it."""
    punts = []
    for cue, moment, look, hypothesis in itertools.product(_R_CUE2, _R_MOMENT, _R_LOOK2, _R_HYP):
        punts.append(f"{moment[0].upper() + moment[1:]}, {cue} {look}. {hypothesis}")
        punts.append(f"{cue} {look} {moment}. {hypothesis}")
    assert len(punts) == 360 and _misread(punts, True) == []


_R_FACTS = [
    "app.orders converts automatically: its key becomes an identity and every column maps.",
    "The DLQ holds 3 records, all 23505 duplicates that a re-snapshot replayed, so nothing needs "
    "repair.",
    "app.orders is missing 3 rows because the Full Load stopped before its last batch.",
]
_R_PROMISE = [
    "I'll check the index build after the converted DDL is applied.",
    "I'll check the CHECKSUM again after the failed batch is reloaded.",
    "I'll review the DLQ again when the updated view is applied.",
    "I'll check the row counts again after the affected tables are reloaded.",
    "I'll look at the plan again once more after the edited view is applied.",
    "I'll check the lag again before the migrated tables are cut over.",
]
MORE_SHAPES = [
    (
        False,
        "검증 결과 11개 테이블의 행 수와 CHECKSUM이 모두 일치하므로 지금 컷오버하셔도 됩니다. 컷오버 전에 DLQ에 새 레코드가 있는지 한 번 더 확인해 "
        "드리겠습니다.",
    ),
    (
        False,
        "Full Load가 마지막 배치 직전에 중단되어 app.orders의 행 3개가 누락되었습니다. 다시 로드한 후에 누락된 행이 남아 있는지 확인해 "
        "드리겠습니다.",
    ),
    (False, "DLQ의 레코드 3건은 모두 재스냅샷으로 생긴 중복이라 복구할 필요가 없습니다. 컷오버 전에 새 레코드가 들어왔는지 다시 확인해 드리겠습니다."),
    (False, "검증 결과 11개 테이블의 행 수와 CHECKSUM이 모두 일치합니다. 지금은 DLQ가 비어 있지만, 컷오버 전에 한 번 더 확인해 드리겠습니다."),
    (
        False,
        "Validation matched all 11 tables on row count and CHECKSUM. The DLQ is empty right now, "
        "but I'll check it again before you cut over.",
    ),
    (
        False,
        "Your Full Load finished and all 11 tables match. Now that CDC is running, I'll check "
        "the lag again before you cut over.",
    ),
    (
        False,
        "app.orders is missing 3 rows because the Full Load stopped before its last batch. "
        "Reload it from Data Migration and I'll verify after you reload it.",
    ),
    (
        False,
        "The sink is healthy and the DLQ is empty, so nothing blocks the cut over. I'll "
        "double-check before you start it.",
    ),
    (
        False,
        "Every table matched its CHECKSUM and CDC lag is 0 s, so you can cut over now. I'll "
        "confirm after you finish the cut over.",
    ),
]


def test_an_english_promise_for_a_moment_still_to_come_is_a_promise() -> None:
    """G101-AI-REG-4: 'after the converted DDL is applied' (an -ed adjective read as the past
    verb), 'I'll double-check before you start it' (the 'check if' lookbehind cancelling
    'before') and 'right now' / 'Now that' (read as said for now) nudged complete answers."""
    answers = [f"{fact} {promise}" for fact, promise in itertools.product(_R_FACTS, _R_PROMISE)]
    assert len(answers) == 18 and _misread(answers, False) == []
    assert _misread([text for _want, text in MORE_SHAPES if not _korean(text)], False) == []


_R_DIRECTIVE = [
    "Reload app.orders from Data Migration.",
    "Yes, re-run Validation.",
    "Rewrite the view with split_part.",
    "The sink skipped them.",
]
_R_MOTIVE_LIKE = [
    "That will give you the 3 missing rows back.",
    "That would explain the 3 missing rows on app.orders.",
    "It will help the planner push the filter down to storage.",
    "This will confirm that no rows are missing after the reload.",
    "It should help the sink catch up within a few minutes.",
    "That will let you cut over without a second Full Load.",
]
_R_PROMISE4 = [
    "I'll check the CHECKSUM again after you reload it.",
    "I'll check the DLQ again before you cut over.",
    "I'll review the plan again if it is still slow.",
]


def test_a_sentence_of_substance_before_the_promise_is_never_a_motive() -> None:
    """G101-AI-REG-3: 'That will give you the 3 missing rows back.', 'That would explain ...',
    'It will help ...' before a closing promise were discounted as motives, wherever they stood,
    so the complete answer was nudged."""
    answers = [
        f"{directive} {sentence} {promise}"
        for directive, sentence, promise in itertools.product(
            _R_DIRECTIVE, _R_MOTIVE_LIKE, _R_PROMISE4
        )
    ]
    assert len(answers) == 72 and _misread(answers, False) == []


_R_KO_FACTS = [
    "Full Load가 마지막 배치 직전에 중단되어 app.orders의 행 3개가 누락되었습니다.",
    "현재 CDC 지연은 45초이며 싱크가 스냅샷 백로그를 따라잡는 중입니다.",
    "app.orders 테이블은 기본 키가 있고 모든 열이 매핑되므로 자동으로 변환됩니다.",
]
_R_KO_PROM = [
    "그 후에 CHECKSUM을 다시 확인해 드리겠습니다.",
    "지연이 0초가 되면 DLQ와 검증 결과를 다시 확인해 드리겠습니다.",
    "Schema Apply를 실행하면 인덱스 빌드 상태를 확인해 드리겠습니다.",
    "다시 로드하면 CHECKSUM을 다시 확인하겠습니다.",
    "컷오버 전에 DLQ를 한 번 더 확인해 보겠습니다.",
]


def test_a_korean_promise_for_a_moment_still_to_come_is_a_promise() -> None:
    """G101-AI-REG-5: a present 'whether' clause cancelled 전에 / 후에, '그 후에' read as time ago,
    …되면 / …하면 no longer deferred, and 지금은 read as said for now: complete answers nudged."""
    answers = [f"{fact} {promise}" for fact, promise in itertools.product(_R_KO_FACTS, _R_KO_PROM)]
    assert len(answers) == 15 and _misread(answers, False) == []
    assert _misread([text for _want, text in MORE_SHAPES if _korean(text)], False) == []


_R_TOPIC = [
    "Validation（ステップ4）では、ソースとターゲットのデータを比較します。",
    "カットオーバー前のチェックは三つあります。",
    "Validationでは二つの比較を行います。",
]
_R_STEP = [
    "各テーブルの行数が一致するか確認します。",
    "まず、各テーブルの行数が一致するか確認します。",
    "DLQにレコードが残っていないか確認します。",
    "次に、チェックサムで行の内容が同じか確認します。",
    "主キーが正しく変換されたかチェックします。",
]


def test_a_japanese_topic_an_earlier_statement_set_governs_the_verbs_after_it() -> None:
    """G101-AI-REG-6: 'Validation（ステップ4）では…比較します。各テーブルの行数が一致するか確認します。'
    -- the topic was read only inside each sentence, so the explanation was nudged."""
    answers = [f"{topic}{step}" for topic, step in itertools.product(_R_TOPIC, _R_STEP)]
    assert len(answers) == 15 and _misread(answers, False) == []


_R_ASK = [
    "I'll need to see your source table definition to tell you why it fails.",
    "I'll need to see the exact error message from your application.",
    "I'll have to see the query's WHERE clause to suggest an index.",
    "To be sure, I'll need to review your application's connection string settings.",
    "I'll need to read the trigger body you are migrating.",
]
_R_FOLLOW = [
    "Could you paste it here?",
    "Can you share it?",
    "Paste it in the Query Converter.",
    "Please copy it from your source.",
]


def test_a_request_to_the_operator_for_a_missing_fact_is_no_punt() -> None:
    """G101-AI-REG-7: 'I'll need to see your source table definition ... Could you paste it here?'
    -- the 'need to' helper made it a look-up, and the nudge said to CALL the tools."""
    answers = [f"{ask} {follow}" for ask, follow in itertools.product(_R_ASK, _R_FOLLOW)]
    assert len(answers) == 20 and _misread(answers, False) == []


# ---- G101-AI-REG-8: a mistaken nudge is cheap again ---------------------------------------
def test_the_nudge_keeps_an_answer_that_offers_or_promises_a_check() -> None:
    """The nudge kept a reply only when it 'already was the complete answer (nothing is left to
    look up)', so an answer that offered or promised a later check invited the look-up -- and
    since G104 a failed round after a look-up costs that answer."""
    from dsql_migrator.core.assessment_strategist import KEEP_REPLY_MARKER, AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    sent: list = []
    replies = ["Let me check the DLQ samples for app.orders.", KEEP_REPLY_MARKER]

    class Bedrock:
        def invoke_model(self, **kw):  # noqa: ANN003
            sent.append(json.loads(kw["body"])["messages"])
            text = replies[min(len(sent), len(replies)) - 1]
            body = {"stop_reason": "end_turn", "content": [{"type": "text", "text": text}]}
            return {"body": io.BytesIO(json.dumps(body).encode())}

    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=Bedrock()).tool_chat(
        "sys", [{"role": "user", "text": "Why is app.orders missing rows?"}], lambda _d: None,
        tools=_TOOLS, execute=lambda _n, _i: "{}",
    )
    nudge = sent[1][-1]["content"][0]["text"]
    assert "CALL the tools" in nudge and KEEP_REPLY_MARKER in nudge
    assert "offers or promises a later check" in nudge and "asks the operator" in nudge
    assert "do not start a new look-up only because that reply offered one" in nudge
    assert outcome.available and outcome.markdown == replies[0]


@pytest.mark.parametrize(
    "answer",
    [
        "I'll need to see your source table definition to tell you why it fails. Could you paste "
        "it here?",
        "Reload app.orders from Data Migration. That will give you the 3 missing rows back. I'll "
        "check the CHECKSUM again after you reload it.",
        "app.orders converts automatically: its key becomes an identity and every column maps. "
        "I'll check the index build after the converted DDL is applied.",
        "Validation（ステップ4）では、ソースとターゲットのデータを比較します。各テーブルの行数が一致するか確認します。",
        "Full Load가 마지막 배치 직전에 중단되어 app.orders의 행 3개가 누락되었습니다. 그 후에 CHECKSUM을 "
        "다시 확인해 드리겠습니다.",
        "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다. 필요하면 "
        "다른 테이블도 확인해 드리겠습니다.",
        "Yes, it's safe. I'll check the DLQ before you cut over.",
    ],
)
def test_a_complete_answer_is_shown_after_one_round(answer: str) -> None:
    """Nudged by mistake, the model did what the nudge said -- it looked something up -- and the
    throttled round after that was the outcome: 'AI reply unavailable' instead of the answer the
    first round had already given."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    first = {"stop_reason": "end_turn", "content": [{"type": "text", "text": answer}]}
    client = _bedrock([first, _LOOK, _ClientError("ThrottlingException")])
    shown: list = []
    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=client).tool_chat(
        "sys", [{"role": "user", "text": "q"}], shown.append, tools=_TOOLS,
        execute=lambda _n, _i: '{"ok": true}',
    )
    assert type(client).calls == 1 and outcome.available and shown == [answer]


@pytest.mark.parametrize(
    "punt",
    [
        "I'll check the current CDC status to understand if the sink has caught up. Once I have "
        "that, I can tell you whether it's safe to cut over.",
        "I'll query the target to identify if any rows are missing. That information will tell "
        "us whether a reload is needed.",
        "原因を調べるために、まずはDLQのサンプルを確認します。",
        "この質問に答えるには、変換後のDDLを確認します。",
        "I'll do a quick review of the converted DDL for app.orders.",
        "Let me summarize what we know so far, then check the DLQ for the failing rows.",
        "Before we proceed, I'll check the current CDC state. Debezium may have restarted the "
        "snapshot after the connector failed.",
    ],
)
def test_a_punt_is_nudged_into_the_real_answer(punt: str) -> None:
    """Each was returned as the answer after one Bedrock call."""
    from dsql_migrator.core.assessment_strategist import AssessmentStrategist
    from dsql_migrator.core.models import AiAssistConfig

    answer = "The DLQ holds 3 records, all 23505 duplicates."
    first = {"stop_reason": "end_turn", "content": [{"type": "text", "text": punt}]}
    last = {"stop_reason": "end_turn", "content": [{"type": "text", "text": answer}]}
    client = _bedrock([first, _LOOK, last])
    outcome = AssessmentStrategist(AiAssistConfig(enabled=True), client=client).tool_chat(
        "sys", [{"role": "user", "text": "Why is app.orders missing rows?"}], lambda _d: None,
        tools=_TOOLS, execute=lambda _n, _i: '{"samples": []}',
    )
    assert type(client).calls == 3 and outcome.markdown == answer


# ---- The regression verifier's differential corpus ----------------------------------------
DIFF_CORPUS = [
    (
        False,
        "Reload app.orders from Data Migration. That will give you the 3 missing rows back. I'll "
        "check the CHECKSUM again after you reload it.",
    ),
    (
        False,
        "Yes. That will give you a consistent cut over point. I'll check the DLQ before you cut "
        "over.",
    ),
    (
        False,
        "The sink skipped them. That would explain the 3 missing rows on app.orders. I'll check "
        "the DLQ again before you cut over.",
    ),
    (
        False,
        "Rewrite the view with split_part. It will help the planner push the filter down. I'll "
        "check the plan again after you re-run Schema Apply.",
    ),
    (
        False,
        "Run Validation now. It will tell you whether any rows are missing. I'll review the "
        "result when you share it.",
    ),
    (
        False,
        "Yes, re-run Validation. This will confirm that no rows are missing after the reload. "
        "I'll check the CHECKSUM again if it mismatches.",
    ),
    (
        False,
        "Turn on the DLQ. That lets you see every row the sink rejects. I'll review the samples "
        "when they arrive.",
    ),
    (
        False,
        "Your Full Load finished and all 11 tables match. Now that CDC is running, I'll check "
        "the lag again before you cut over.",
    ),
    (
        False,
        "All 11 tables match. I'll check the DLQ before you cut over, since a new CDC error "
        "would show up there first.",
    ),
    (
        False,
        "I'll need to see your source table definition to tell you why it fails. Could you paste "
        "the CREATE TABLE here?",
    ),
    (
        False,
        "To tell you which index to add, I'll need to see the query's WHERE clause. Paste it in "
        "the Query Converter.",
    ),
    (False, "I'll have to see the error message itself. Can you copy it from the activity log?"),
    (
        False,
        "I'll assume you're looking at app.orders: it converts automatically and needs no change.",
    ),
    (
        False,
        "I'm going to recommend checking the DLQ depth before cutting over, because CDC errors "
        "land there.",
    ),
    (
        False,
        "I'll be honest: checking every table by hand is slow, and Validation (Step 4) does it "
        "for you.",
    ),
    (
        False,
        "app.orders converts automatically. I'll check the index build later, once Schema Apply "
        "has run. If the DLQ shows new 23505 records before you cut over, I'll review them then.",
    ),
    (True, "Let me summarize what we know so far, then check the DLQ for the failing rows."),
    (True, "Let me outline the plan and then pull the current validation results."),
    (True, "Let me recap the situation and check the current CDC state."),
    (True, "Let me explain the likely cause and then check the sink status to confirm it."),
    (
        True,
        "Before we go further, I'll check the current CDC state. Debezium may have restarted the "
        "snapshot after the connector failed.",
    ),
    (
        True,
        "Before we change anything, I'll pull the DLQ samples for app.orders. The 23505 codes "
        "usually come from a re-snapshot.",
    ),
    (
        True,
        "I'll check the sink status before we decide. A stalled task would explain the missing "
        "rows on app.orders.",
    ),
    (
        True,
        "I'll look at the validation verdicts when we have the latest run. The mismatch is "
        "probably on app.order_items.",
    ),
    (
        True,
        "I'll check the DLQ samples to find out whether the rows failed, and if they did, which "
        "constraint rejected them.",
    ),
    (
        True,
        "I'll keep checking the lag until it drops to zero. The sink is still replaying the "
        "snapshot backlog.",
    ),
    (
        True,
        "I'll check the DLQ before any rows are dropped. Records there are kept for seven days "
        "by default.",
    ),
    (False, "DLQ는 비어 있고 CDC 지연도 0초이므로 컷오버를 진행하셔도 됩니다. 컷오버 전에 DLQ를 한 번 더 확인해 보겠습니다."),
    (
        False,
        "app.orders의 행 3개가 마지막 배치 전에 중단되어 누락되었습니다. Data Migration에서 다시 로드하세요. 다시 로드한 후에 "
        "CHECKSUM을 확인해 보겠습니다.",
    ),
    (
        False,
        "Full Load가 마지막 배치 직전에 중단되어 app.orders의 행 3개가 누락되었습니다. Data Migration에서 app.orders를 다시 "
        "로드하세요. 그 후에 CHECKSUM을 다시 확인해 드리겠습니다.",
    ),
    (False, "현재 CDC 지연은 45초이며 싱크가 스냅샷 백로그를 따라잡는 중입니다. 지연이 0초가 되면 DLQ와 검증 결과를 다시 확인해 드리겠습니다."),
    (
        False,
        "app.orders 테이블은 기본 키가 있고 모든 열이 매핑되므로 자동으로 변환됩니다. Schema Apply를 실행하면 인덱스 빌드 상태를 확인해 "
        "드리겠습니다.",
    ),
    (False, "app.orders를 다시 로드하세요. 그러면 누락된 3개 행이 복구됩니다. 다시 로드한 후에 CHECKSUM을 확인해 드리겠습니다."),
    (False, "검증 결과 11개 테이블이 모두 일치하므로 지금 컷오버하셔도 됩니다. 이후에 새 DLQ 레코드가 생기면 바로 확인해 드리겠습니다."),
    (True, "컷오버 전에 DLQ 상태를 확인하겠습니다. 외래 키 위반 때문에 싱크가 레코드를 거부했을 수 있습니다."),
    (True, "Full Load 이후의 행 수를 조회하겠습니다. 마지막 배치가 실패했을 가능성이 있습니다."),
    (
        False,
        "Validation（ステップ4）では、ソースとターゲットのデータを比較します。まず、各テーブルの行数が一致するか確認します。次に、チェックサムで行の内容が同じか確認します。",
    ),
    (False, "Validationでは二つの比較を行います。各テーブルの行数が一致するか確認します。さらに、行ごとのチェックサムが同じか確認します。"),
    (False, "カットオーバー前のチェックは三つあります。DLQにレコードが残っていないか確認します。CDCの遅延が0秒か確認します。"),
    (False, "DLQに3件ありますが、すべて再スナップショットによる重複なので修復は不要です。"),
    (True, "現在の状態はどうなっているか確認します。"),
    (True, "DLQのサンプルを確認しましょう。外部キー違反が原因かもしれません。"),
]


def test_the_differential_corpus_reads_right() -> None:
    """G101-AI-REG-1..7: the shapes G99 read right and G104 wrong, and the ones both read wrong
    ('All 11 tables match. I'll check ...', 'until it drops to zero', a hedge after advice)."""
    assert _misread([text for want, text in DIFF_CORPUS if want], True) == []
    assert _misread([text for want, text in DIFF_CORPUS if not want], False) == []


# ---- The G99 reviewer's corpora -----------------------------------------------------------
G99R_PUNTS = [
    "Let me check if the sink connector is running. This will tell us whether the DLQ is still "
    "growing.",
    "I'll check the current CDC state to see if the sink has caught up. This will help us "
    "understand whether it's safe to cut over.",
    "Let me check if the table has a primary key. That determines whether Aurora DSQL can load "
    "it.",
    "Let me look at what happened when the sink restarted. That should explain the gap you're "
    "seeing.",
    "Let me check the row counts after the Full Load. That tells us whether anything was dropped.",
    "I'll pull the validation summary to see if the mismatches are still there. Then I can tell "
    "you exactly what to fix.",
    "Let me look at the DLQ samples to see if the errors are FK violations. That would explain "
    "why the sink keeps dead-lettering rows.",
    "I'll fetch the CDC diagnostics to see if it is the sink or the source connector. Each one "
    "has a different fix.",
    "Good question. Let me check if the view exists on the target first, since Schema Apply may "
    "have skipped it.",
    "Let me go ahead and review the converted DDL for app.orders.",
    "I'll need to review the converted DDL first.",
    "Let me run a quick query to see what landed on the target.",
    "I'll take a moment to review the Schema Apply result.",
    "Let me check the DLQ samples.",
    "I'll check whether the sink is healthy.",
    "Let me check if the table exists on the target.",
    "Full Load 후에 행 수를 확인해 보겠습니다. 이를 통해 누락된 데이터가 있는지 알 수 있습니다.",
    "컷오버 전에 DLQ 상태를 확인해 보겠습니다. DLQ에 남은 레코드가 있으면 컷오버를 진행할 수 없습니다.",
    "CDC를 배포한 후에 싱크에 무슨 일이 있었는지 확인해 보겠습니다. 이 정보로 데이터 누락 여부를 판단할 수 있습니다.",
    "CDC 싱크가 정상인지 확인해 보겠습니다.",
    "まずは現在のCDCの状態を確認します。",
    "では、変換後のDDLを確認します。",
    "はい、DLQの状態を確認します。",
    "それでは、検証結果を確認します。",
    "ご質問ありがとうございます。まずはDLQのサンプルを確認します。",
    "DLQの状態を確認します。",
    "承知しました。では、スキーマ適用の結果をチェックします。",
]
G99R_ANSWERS = [
    "The sink is healthy and the DLQ is empty, so you can cut over. I'll check the DLQ before "
    "you cut over.",
    "Yes, it's safe. I'll check the DLQ before you cut over.",
    "The DLQ has 2 records. I'll review them if you want.",
    "外部キーは親テーブルを参照します。",
    "Validation ステップでは行数とチェックサムを確認します。",
]
_G99R_FACTS_KO = [
    "app.orders 테이블은 CHECKSUM이 일치합니다. 누락된 행은 없으며 컷오버를 진행하셔도 됩니다.",
    "DLQ에 23505 레코드가 3건 있지만 모두 재스냅샷으로 인한 중복이라 복구가 필요 없습니다.",
    "이 뷰는 이름 충돌 때문에 생성되지 않습니다. 소스에서 이름을 줄여 주세요.",
]
_G99R_CLOSINGS_KO = [
    " 필요하시면 다른 테이블도 확인해 드리겠습니다.",
    " 원하시면 각 레코드를 자세히 분석해 드리겠습니다.",
    " 추가로 궁금한 점이 있으면 말씀해 주세요.",
]
_G99R_FACTS_JA = [
    "app.ordersのCHECKSUMは一致しています。欠落行はないので、カットオーバーできます。",
    "DLQに3件ありますが、すべて再スナップショットによる重複なので修復は不要です。",
    "このビューは名前の衝突のため作成されません。ソース側で名前を短くしてください。",
]
_G99R_CLOSINGS_JA = ["カットオーバーの前に、もう一度DLQを確認しましょう。", "次に検証結果を確認しましょう。", "ご不明な点があればお知らせください。"]
_G99R_JA_LEADS = ["", "まずは", "では、", "それでは、", "はい、", "承知しました。", "まず、"]
_G99R_JA_OBJECTS = ["DLQのサンプル", "CDCの状態", "変換後のDDL", "検証結果"]
_G99R_JA_VERBS = ["を確認します。", "をチェックします。", "を取得します。", "を確認してみます。", "を調べます。"]
_G99R_JA_EMBEDDED = [
    "CDCの遅延が解消されたか確認します。",
    "DLQにレコードが残っていないか確認します。",
    "主キーが正しく変換されたかチェックします。",
    "シンクが停止していないか確認します。",
]
_G99R_KO_PUNTS = [
    "Full Load 후에 행 수를 확인해 보겠습니다. 이를 통해 누락된 데이터가 있는지 알 수 있습니다.",
    "컷오버 전에 DLQ 상태를 확인해 보겠습니다. DLQ에 남은 레코드가 있으면 컷오버를 진행할 수 없습니다.",
    "싱크가 재시작된 후에 무슨 일이 있었는지 확인해 보겠습니다. 그러면 누락된 행의 원인을 알 수 있습니다.",
    "CDC가 따라잡았는지 확인하면 컷오버 여부를 판단할 수 있으니 먼저 확인해 보겠습니다.",
    "검증이 끝나면 결과를 알려드릴 수 있도록 지금 검증 요약을 조회해 보겠습니다. 잠시만 기다려 주세요.",
]
_G99R_BASE = [
    "Let me check the dead-letter queue to see if you have any 23505 records.",
    "Let me check the DLQ to determine if it has any failed records.",
    "Let me look into why the sink stalled when it reconnected.",
    "Let me check what happened to the sink after the Full Load finished.",
    "Let me look at when the DLQ records started arriving.",
    "Full Load 후에 행 수를 확인해 보겠습니다.",
    "이전에 실패한 테이블 목록을 확인해 보겠습니다.",
]
_G99R_TAIL_EN = " This will show what is blocking you before you cut over."
_G99R_TAIL_KO = " 이를 통해 누락된 데이터가 있는지 알 수 있습니다."
_G99R_T_CUES = ["Let me", "I'll", "I'm going to"]
_G99R_T_VERBS = [
    "check",
    "look at",
    "pull",
    "review",
    "query",
    "go ahead and review",
    "start by checking",
]
_G99R_T_OBJECTS = [
    "the DLQ samples",
    "the CDC status",
    "the converted DDL for app.orders",
    "the validation summary",
]
_G99R_T_PURPOSES = [
    "",
    " to see if the sink has caught up",
    " to check if it is still lagging",
    " from after the Full Load finished",
    " to see what changed when the sink restarted",
    " to find what blocks you before you cut over",
]
_G99R_T_FOLLOWUPS = [
    "",
    " This will tell us whether it is safe to cut over.",
    " That should explain the gap you are seeing.",
    " One moment.",
]


def test_the_g99_reviewers_corpora_read_right() -> None:
    """'Yes, it's safe. I'll check the DLQ before you cut over.' was nudged: three words before a
    promise were too few to count."""
    punts = list(G99R_PUNTS) + _G99R_JA_EMBEDDED + _G99R_KO_PUNTS + _G99R_BASE
    punts += [lead + obj + verb for lead, obj, verb in itertools.product(
        _G99R_JA_LEADS, _G99R_JA_OBJECTS, _G99R_JA_VERBS)]
    punts += [base + (_G99R_TAIL_KO if _korean(base) else _G99R_TAIL_EN) for base in _G99R_BASE]
    templated = [
        f"{cue} {verb} {obj}{purpose}.{follow}"
        for cue, verb, obj, purpose, follow in itertools.product(
            _G99R_T_CUES, _G99R_T_VERBS, _G99R_T_OBJECTS, _G99R_T_PURPOSES, _G99R_T_FOLLOWUPS
        )
    ]
    answers = list(G99R_ANSWERS)
    answers += [f + c for f, c in itertools.product(_G99R_FACTS_KO, _G99R_CLOSINGS_KO)]
    answers += [f + c for f, c in itertools.product(_G99R_FACTS_JA, _G99R_CLOSINGS_JA)]
    assert _misread(punts, True) == []
    assert len(templated) == 2016 and _misread(templated, True) == []
    assert _misread(answers, False) == []


# ---- Held out: replies in no verifier corpus, aimed at each new rule -----------------------
HELD_OUT_PUNTS = [
    "Let me take a look at the DLQ samples for app.orders.",
    "I'll dig into the CDC metrics to understand when the lag started growing.",
    "I'll check the Full Load log for app.orders. Large tables often stall on their last batch.",
    "Let me have a quick look at the validation verdicts.",
    "I'll run a check on the sink connector to see if it is still running. A stopped task is the "
    "usual cause.",
    "I'm going to review the converted DDL to figure out if the identity column is causing the "
    "conflict. Duplicate keys typically come from the sequence starting too low.",
    "Sure. Before you cut over, I'll check the DLQ and the CDC lag.",
    "Got it. I'll check the validation results before you cut over.",
    "I'll pull the DLQ samples for app.orders when the next batch fails.",
    "Let me gather the current state: the DLQ depth, the CDC lag and the last validation run.",
    "I'll examine the Schema Apply results to work out why the view was skipped.",
    "Let me fetch the assessment for app.orders so I can tell you what blocks it.",
    "I will look up the converted DDL for app.orders.",
    "I'll read through the activity log to find the failing statement.",
    "Thanks for the question. I'll look at the CDC lag before you cut over.",
    "I'll check whether the sink is caught up. If it is, you can cut over.",
    "Let me query the target to count the rows in app.orders. That will show whether the reload "
    "worked.",
    "I'm going to inspect the DLQ to determine when the errors started. They usually begin right "
    "after a schema change on the source.",
    "I'll keep looking at the lag until it settles. The snapshot backlog is still draining.",
    "Let me continue checking the DLQ for new records.",
    "That's a good question. I'll check the DLQ before you cut over.",
    "Happy to help. Before you cut over, I'll review the validation verdicts.",
    "DLQ 샘플을 확인해 보겠습니다. 외래 키 위반이 원인일 가능성이 높습니다.",
    "알겠습니다. 컷오버 전에 CDC 지연을 확인하겠습니다.",
    "app.orders의 변환된 DDL을 조회하겠습니다. 기본 키 설정이 원인일 수 있습니다.",
    "검증 결과를 분석해 보겠습니다.",
    "좋은 질문입니다. Full Load 로그를 살펴보겠습니다.",
    "컷오버하기 전에 DLQ와 CDC 지연을 확인하겠습니다. 외래 키 위반이 있으면 싱크가 레코드를 거부합니다.",
    "承知しました。DLQのサンプルを確認します。",
    "かしこまりました。カットオーバーの前に、CDCの遅延を確認します。",
    "検証結果を調べてみます。外部キー違反が原因かもしれません。",
    "では、app.ordersの変換後のDDLを取得します。",
    "まず、現在のCDCの状態をチェックします。少々お待ちください。",
    "承知いたしました。フルロードの後に何が起きたか確認します。",
    "싱크가 재시작된 후에 왜 멈췄는지 확인해 보겠습니다. 이를 통해 원인을 알 수 있습니다.",
    "Full Load 직후에 어떤 오류가 있었는지 확인하겠습니다.",
    "再起動の後にシンクがなぜ止まったか確認します。",
    "I'll check the DLQ once more. A new record may have arrived.",
    "Let me check a few things first: the DLQ, the CDC lag and the validation verdicts.",
    "I'll pull your current state; that way I can tell you exactly what blocks the cut over.",
    "Let me see what the DLQ holds for app.orders.",
    "I'll go through the validation results for app.orders and check each mismatch.",
    "I'll look into it.",
    "Let me double-check the converted DDL for app.orders.",
    "Let me run it to see what lands on the target.",
    "I'll check the DLQ and, if needed, the CDC lag.",
    "それは良い質問です。DLQの状態を確認します。",
]
HELD_OUT_ANSWERS = [
    "app.orders is MANUAL because it has no primary key; add one on the source or let the tool "
    "add a surrogate key. I'll review the converted DDL again after you re-run Schema Apply.",
    "Yes, you can cut over: the DLQ is empty and all tables match. I'll check the lag once more "
    "when you start.",
    "The view failed because Aurora DSQL has no FULLTEXT index. Drop the index and re-run Schema "
    "Apply. If it still fails, I'll look at the new error.",
    "No, not yet: 2 tables still mismatch. Reload them, and I'll check the CHECKSUM again after "
    "the reload finishes.",
    "Those 23503 records come from a child row arriving before its parent. They clear once the "
    "parent lands. I'll check the DLQ again before you cut over.",
    "I'll keep it short: your cut over is safe.",
    "I'll leave the review of the trigger body to you; the converted DDL is ready.",
    "I'll rewrite the query to use split_part: split_part(name, ',', 1) keeps the first field.",
    "Your sink is RUNNING with 0 s lag. I'll check it again if the lag grows.",
    "The DLQ is empty. I'll take another look before you cut over.",
    "app.orders converts automatically, so no change is needed. Let me know if you'd like me to "
    "review the trigger body.",
    "The lag is 45 s and falling; the sink is catching up on the snapshot backlog. Let me check "
    "it again after you deploy the next batch.",
    "Yes. The sink is healthy and the DLQ is empty. Before you cut over, let me check the lag "
    "once more.",
    "The CHECKSUM mismatch on app.orders is a float rounding difference, not lost data. I'll "
    "check the other tables if you want.",
    "Aurora DSQL enforces foreign keys, so the child rows wait for their parents. I'll review "
    "the DLQ when the parents land.",
    "I'll make sure you see the new column: the converted DDL already adds it.",
    "I'll walk you through what the query does: it joins app.orders to app.users on user_id.",
    "I'm going to keep the review short: the DDL converts as is.",
    "I'll note that the sink reads from MSK, so a broker outage pauses it.",
    "I'll be brief: checking the DLQ before the cut over is enough.",
    "All 11 tables match and the DLQ is empty. Let me check the lag again right before you cut "
    "over.",
    "To tell you why it fails, I'll need to see the error text. Can you copy it from the "
    "activity log?",
    "DLQ의 23505 레코드는 재스냅샷으로 생긴 중복이므로 복구할 필요가 없습니다. 컷오버하기 전에 DLQ를 다시 확인하겠습니다.",
    "검증이 모두 일치하므로 컷오버하셔도 됩니다. 컷오버가 끝나면 CDC 지연을 다시 확인해 드리겠습니다.",
    "app.orders는 자동으로 변환됩니다. Schema Apply를 실행한 후에 인덱스가 만들어졌는지 확인해 드리겠습니다.",
    "현재 CDC 지연은 0초입니다. 다음 배치가 들어오면 다시 확인해 드리겠습니다.",
    "네, 컷오버하셔도 됩니다. 모든 테이블이 일치합니다. 컷오버 후에 DLQ를 한 번 더 확인해 보겠습니다.",
    "DLQは空で、すべてのテーブルが一致しています。カットオーバーの後に、CDCの遅延をもう一度確認します。",
    "このビューは自動で変換されます。Schema Applyの後にインデックスが作成されたか確認します。",
    "外部キーはAurora DSQLで強制されます。必要に応じて、DLQも確認します。",
    "Validationでは各テーブルの行数を比較します。さらに、チェックサムが一致するか確認します。",
    "Validation found 3 missing rows in app.orders. Once you reload it, I'll check the CHECKSUM "
    "again.",
    "누락된 행은 3개입니다. 다시 로드하신 후에 행이 모두 복구되었는지 확인해 드리겠습니다.",
    "DLQは空です。カットオーバーの前に、新しいレコードが入ったか確認します。",
    "I'll say it plainly: re-run Validation and see whether the CHECKSUM matches.",
    "I'll leave it to you to check the CHECKSUM after the reload; the steps are in Data "
    "Migration.",
    "I'll give you the steps: reload app.orders, then check the CHECKSUM in Validation.",
    "I'm going to be direct: the view you see is stale and needs a re-run of Schema Apply.",
    "The DLQ is empty and the lag is 0 s. I'll check it again if needed.",
    "All 11 tables match, so you can cut over. I'll review the DLQ again if necessary.",
    "I'll keep it simple: you can cut over now.",
    "I'd recommend you check the DLQ yourself before you cut over; it is on the CDC screen.",
    "If needed, I'll review the DLQ again before you cut over; right now it is empty.",
]


def test_a_reply_is_read_by_what_it_says_before_its_look_up() -> None:
    """A hypothesis or a 'This will show ...' after a look-up never made it an answer, and an
    answer's closing promise ('once you reload it', '…실행한 후에 …만들어졌는지', 'if needed', 'let me
    check it again after you deploy') was nudged; nor is a look-up word after a colon, or the
    operator's ('leave it to you to check'), the model's."""
    assert _misread(HELD_OUT_PUNTS, True) == []
    assert _misread(HELD_OUT_ANSWERS, False) == []


_OPENERS = [
    "Sure.",
    "Got it.",
    "Okay, sounds good.",
    "Of course, happy to help.",
    "Thanks for the question.",
    "That's a great question.",
    "Understood.",
    "Great question!",
    "Sure thing.",
    "Alright.",
]
_DEFERRED = [
    "I'll check the DLQ before you cut over.",
    "Before you cut over, I'll review the validation verdicts.",
    "I'll look at the CDC lag after you reload app.orders.",
]
_FACTS = [
    "Yes, it's safe.",
    "All 11 tables match.",
    "The DLQ is empty.",
    "No, two tables still differ.",
    "CDC lag is 0 s.",
]
_KO_OPENERS = ["네, 알겠습니다.", "좋은 질문입니다.", "감사합니다."]
_KO_DEFERRED = ["컷오버 전에 DLQ를 확인하겠습니다.", "다시 로드하신 후에 CHECKSUM을 확인해 드리겠습니다."]
_KO_FACTS = ["모든 테이블이 일치합니다.", "DLQ는 비어 있습니다."]
_JA_OPENERS = ["はい、わかりました。", "かしこまりました。", "承知いたしました。", "ご質問ありがとうございます。"]
_JA_DEFERRED = ["カットオーバーの前に、DLQを確認します。", "再ロードの後に、CHECKSUMを確認します。"]
_JA_FACTS = ["すべてのテーブルが一致しています。", "DLQは空です。"]


def test_a_short_fact_before_a_promise_is_substance_and_a_courtesy_is_none() -> None:
    """'All 11 tables match. I'll check the DLQ before you cut over.' was nudged (a fact needed
    six words); 'Okay, sounds good.' or 'はい、わかりました。' before the same look-up says nothing."""
    punts = [f"{o} {d}" for o, d in itertools.product(_OPENERS, _DEFERRED)]
    punts += [f"{o} {d}" for o, d in itertools.product(_KO_OPENERS, _KO_DEFERRED)]
    punts += [o + d for o, d in itertools.product(_JA_OPENERS, _JA_DEFERRED)]
    answers = [f"{f} {d}" for f, d in itertools.product(_FACTS, _DEFERRED)]
    answers += [f"{f} {d}" for f, d in itertools.product(_KO_FACTS, _KO_DEFERRED)]
    answers += [f + d for f, d in itertools.product(_JA_FACTS, _JA_DEFERRED)]
    assert _misread(punts, True) == []
    assert _misread(answers, False) == []
