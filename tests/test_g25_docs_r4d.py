# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 4, last step (docs): the manual, in English, Korean and Japanese alike, says what
the last step's PostgreSQL Start CDC code does on a stack whose last Start CloudFormation rolled
back, and where the re-snapshot remedy is withheld or its own checks could not read the source.

* f6: every ``UPDATE_ROLLBACK_*`` status counts as started (complete, in progress, failed,
  cleanup in progress), and so do this stack's connectors that MSK Connect lists, whatever the
  stack's status. Chapter 4 named ``UPDATE_ROLLBACK_COMPLETE`` alone.
* f0, f4: a rolled-back Start's CDC topics may still hold changes it streamed, which the next
  sink would apply over a reload. So a block on such a stack names one route, new topics first
  (Delete CDC infrastructure and deploy it again, then the Full Load re-run); chapter 4 named the
  Full Load re-run on its own as one of two routes there, and said a re-snapshot "would not run
  there" (it cannot be relied on there). The inheritance block's own Full Load run gets the same
  order there, and so does the "Publication autocreation is disabled" diagnosis.
* f2, f7: the Start CDC card and the Start's activity-log line promise no snapshot there: the
  next Start continues from the position the rolled-back Start recorded, if it recorded one.
* f5: that retry is gapless only while the CDC topics still retain that Start's changes, which
  Start CDC does not check; chapter 4 said it continues "with no gap".
* f1: where the re-snapshot remedy is withheld, the dialog names the route that does work, the
  Full Load re-run; chapters 4 and 13 say so.
* f3: a failed read of the re-snapshot route's own checks is reported as exactly that (*The
  re-snapshot route could not be checked*), not as the publication and slot checks not running.

Each check reads the code first, through its own constants or behaviour, then the text with the
hard wrap undone (and, in ja, with no whitespace at all). Each fails on the manual before this
step.

Round 5 (test_g25_docs_r5.py, the route rule) gives a stopped stack the same one route, new
topics first, says Debezium's own snapshot rule after a rollback (a recorded position whose
initial snapshot had not completed snapshots again), and names after a withheld re-snapshot
the route that works for that refusal. The pins here it reworded read its words now, with this
step's wording kept as stale.
"""

from __future__ import annotations

import inspect
import re
from types import SimpleNamespace

import pytest

from tests.test_g25_docs import (
    _CDC_UI,
    _F43,
    _PAGE,
    LANGS,
    _fact,
    _flat,
    _function,
    _has,
    _page,
    _route_halves,
    _section_43,
    _steps_in_order,
)
from tests.test_g25_docs_r4 import _stale

ROLLED_BACK = "UPDATE_ROLLBACK_COMPLETE"
ROLLBACK_STATUSES = (
    "UPDATE_ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
    "UPDATE_ROLLBACK_IN_PROGRESS",
    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
)
DELETE = "Delete CDC infrastructure and deploy it again"
RERUN = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
_PUBLICATION_MISSING = (
    "CDC's publication does not exist on the source",
    "The publication is not on the source.",
)
# Debezium's own line (debezium-connector-postgres 2.7.4) for a missing publication.
_PUBLICATION_GONE_TAIL = (
    "org.apache.kafka.connect.errors.ConnectException: Publication autocreation is disabled, "
    "please create one and restart the connector."
)


def _gate(monkeypatch, status: str, *, verdict=_PUBLICATION_MISSING, cdc_only=False,
          committed=False, force_initial=None):
    """The REAL publication/slot gate of a PostgreSQL Start on a stack in ``status``, with the
    source read faked to return ``verdict``. ``committed``: the stack was started and stopped
    (``DeploySink=true``, a blank bootstrap). Returns the gate's block, the migration state and
    the slot reads a resumed Start made for the other modes."""
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration import _cdc_ui as cu
    from dsql_migrator.ui.data_migration._models import MigrationType

    slot_reads: list = []

    def _resume_slot(*_args, **_kwargs):
        slot_reads.append(True)

    monkeypatch.setattr(cu, "_probe_pg_replication_objects", lambda *_a, **_k: verdict)
    monkeypatch.setattr(cu, "_probe_pg_resume_slot", _resume_slot)
    monkeypatch.setattr(cu, "_cdc_tables_for_config", lambda *_a, **_k: ["app.t"])
    state = SimpleNamespace(
        cdc_start_mode=lambda: "auto",
        cdc_has_committed_offset=committed,
        cdc_stack_phase="infra",
        cdc_stack_phase_status=status,
        cdc_deployed_watermark=None,
        migration_type=MigrationType.CDC_ONLY if cdc_only else MigrationType.FULL_LOAD_AND_CDC,
    )
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES))
    block = cu._pg_objects_gate_block(state, session, None, _watermark(),
                                      force_initial=force_initial)
    return block, state, slot_reads


def _watermark():
    """A Full Load + CDC run's watermark: its LSN and the slot it created."""
    return SimpleNamespace(
        binlog_file=None, binlog_position=None, gtid_executed=None, server_uuid=None,
        wal_lsn="0/1000000", slot_name="s", publication_name="p",
    )


def _new_topics_first(lang: str, text: str) -> bool:
    """Every sentence of ``text`` that names Re-run Full Load names Delete CDC infrastructure
    before it: no route there re-runs the Full Load on the old topics."""
    rerun, delete = _flat(lang, "Re-run Full Load"), _flat(lang, "Delete CDC infrastructure")
    sentences = text.split("。") if lang == "ja" else re.split(r"(?<=[.!?]) ", text)
    named = [s for s in sentences if rerun in s]
    return bool(named) and all(delete in s[: s.index(rerun)] for s in named)


# -- f6: every rollback status, and connectors MSK Connect lists, count as started -----------

# §4.3 (answer-first) names the slot-position block by its title and says a restart, or a Start
# after a rolled-back one, continues from the last recorded position (the slot's, on PostgreSQL).
# Which stacks count as started -- every UPDATE_ROLLBACK_* status, and the connectors MSK Connect
# lists -- is the code assertions above; the page keeps what the reader acts on.
# The closing step's wording: one rollback status, and a record without the listed connectors.
_STALE_STARTED = {
    "en": (
        "(`UPDATE_ROLLBACK_COMPLETE`)",
        "not even one whose CloudFormation update rolled back — Start CDC refuses",
        "The check reads only what the stack records, so",
    ),
    "ko": (
        "(`UPDATE_ROLLBACK_COMPLETE`)",
        "롤백된 Start의 기록도 없어야 함)에서는",
        "이 검사는 스택이 기록한 것만 읽으므로",
    ),
    "ja": (
        "(`UPDATE_ROLLBACK_COMPLETE`)",
        "Start の記録もないこと) では",
        "このチェックはスタックが記録しているものだけを読むため",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_every_rollback_status_and_a_listed_connector_count_as_started(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the stack record as the stack-phase probe read it, and the connectors the
    # CDC step found on MSK Connect.
    for status in ROLLBACK_STATUSES:
        rolled = SimpleNamespace(cdc_has_committed_offset=False, cdc_stack_phase="unstable",
                                 cdc_stack_phase_status=status)
        assert cu._pg_start_rolled_back(rolled) is True, status
        assert cu._pg_stack_started(rolled) is True, status
    listed = SimpleNamespace(cdc_has_committed_offset=False, cdc_stack_phase="unstable",
                             cdc_stack_phase_status="UPDATE_IN_PROGRESS",
                             cdc_connector_names=["stk-debezium-source"])
    assert cu._pg_stack_started(listed) is True
    for status in ("CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_IN_PROGRESS", "ROLLBACK_COMPLETE"):
        never = SimpleNamespace(cdc_has_committed_offset=False, cdc_stack_phase="infra",
                                cdc_stack_phase_status=status, cdc_connector_names=[])
        assert cu._pg_stack_started(never) is False, status
    # So the slot-position block, which says the stack records no Start "(not even one that
    # rolled back)", reaches none of them.
    from tests.test_g25_docs_r4 import _slot_objects

    assert pg.pg_replication_objects_blocker(
        _slot_objects(), ["app.t"], resumes_from_slot=True, start_lsn="0/1000000",
        stack_started=cu._pg_stack_started(listed),
    ) is None

    section = _section_43(lang)
    assert _has(lang, section, f"*{pg.SLOT_POSITION_HEADER}*"), lang
    assert _fact(lang, section, "restart-row") and _fact(lang, section, "pg-slot-position"), lang
    assert not _stale(lang, _STALE_STARTED[lang]), lang


# -- f0, f4: on a rolled-back stack the one route takes new topics first ---------------------

# G25 round 5: every stack whose connectors were started before -- a stopped one too, whose
# topics may hold the sink's unapplied tail -- gets that one route, said in its own paragraph
# (the rest of it: test_g25_docs_r5.py).
# G25 round 5: every stack whose connectors were started before -- a stopped one too, whose
# topics may hold the sink's unapplied tail -- gets that one route (the rest of it:
# test_g25_docs_r5.py). §4.3 says it in the started half of its closing route paragraph.
# Stale since round 5: a stopped stack's two routes, the one route said of a rolled-back stack
# alone, and the lost slot's advice that only a stack whose Start rolled back needs new topics
# before the Full Load re-run.
_STALE_BY_STATE = {
    "en": (
        "names the gapless routes instead of a re-snapshot, which cannot be relied on there: on "
        "a stopped stack, Re-run Full Load with **Drop & reload**, or Delete CDC infrastructure "
        "and deploy it again (with CDC only, the latter); after a rolled-back Start, only the "
        "route in the next paragraph",
        "a missing or unusable slot says so there and names one route",
        "you must re-run Full Load to capture a fresh watermark (after a Start that "
        "CloudFormation rolled back, Delete CDC infrastructure and deploy it again first; see "
        "below)",
    ),
    "ko": (
        "그곳에서는 신뢰할 수 없는 다시 스냅샷 대신 무손실 경로를 알려 줍니다: 멈춘 스택에서는 Re-run "
        "Full Load를 **Drop & reload**로, 또는 Delete CDC infrastructure 후 다시 배포(CDC only에서는 "
        "후자), Start가 롤백된 뒤에는 다음 문단의 경로 하나뿐입니다",
        "슬롯이 없거나 쓸 수 없다는 차단은 그런 스택에서 그렇게 알리고 경로를 하나만 알려 줍니다",
        "새 워터마크를 얻기 위해 Full Load를 다시 실행해야 합니다(CloudFormation이 Start를 롤백한 뒤라면 "
        "먼저 Delete CDC infrastructure 후 다시 배포하세요. 아래 참고)",
    ),
    "ja": (
        "そこでは当てにできない再スナップショットの代わりにギャップのない方法を示します: 停止したスタック"
        "では **Drop & reload** での Re-run Full Load、または Delete CDC infrastructure して再デプロイ "
        "(CDC only では後者)、Start がロールバックされた後は次の段落の 1 つの方法だけです",
        "スロットがないか使用できないというブロックは、そのようなスタックではそう伝えて方法を 1 つだけ"
        "示します",
        "新しいウォーターマークを取得するために Full Load を再実行する必要があります (CloudFormation が "
        "Start をロールバックした後は、先に Delete CDC infrastructure して再デプロイしてください。下記を参照)",
    ),
}
# The closing step's wording: one pair of routes for both states, and a re-snapshot that
# "would not run" there.
_STALE_ROUTES = {
    "en": (
        "which would not run there",
        "(Re-run Full Load with **Drop & reload**, or Delete CDC infrastructure and deploy it "
        "again; with CDC only, the latter)",
    ),
    "ko": (
        "그곳에서는 실행되지 않는 다시 스냅샷",
        "(Re-run Full Load를 **Drop & reload**로, 또는 Delete CDC infrastructure 후 다시 배포. CDC "
        "only에서는 후자)",
    ),
    "ja": (
        "そこでは実行されない再スナップショット",
        "(**Drop & reload** での Re-run Full Load、または Delete CDC infrastructure して再デプロイ。"
        "CDC only では後者)",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_stacks_route_takes_new_topics_before_any_full_load_rerun(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the REAL gate on each state. A rolled-back stack's block says why and
    # names one route, new topics first; a stopped stack keeps both routes.
    for status in ROLLBACK_STATUSES:
        block, state, _reads = _gate(monkeypatch, status)
        routes = cu._pg_block_route(state, _PUBLICATION_MISSING[0])
        assert block == (_PUBLICATION_MISSING[0], f"{_PUBLICATION_MISSING[1]} {routes}"), status
        assert "its CDC topics may still hold changes that no sink applied" in routes
        assert f"{DELETE} (the new stack starts with empty topics), then {RERUN}" in routes
        assert "Two gapless routes" not in routes
        assert _new_topics_first("en", routes), routes
    _block, cdc_only, _reads = _gate(monkeypatch, ROLLED_BACK, cdc_only=True)
    only = cu._pg_block_route(cdc_only, _PUBLICATION_MISSING[0])
    assert f"The gapless route: use {DELETE}" in only and "Re-run Full Load" not in only
    # G25 round 5: a stopped stack's topics may hold the sink's unapplied tail too (Stop CDC
    # does not drain it), so its block names the same one route, new topics first.
    block, stopped, _reads = _gate(monkeypatch, "UPDATE_COMPLETE", committed=True)
    assert block == (_PUBLICATION_MISSING[0], f"{_PUBLICATION_MISSING[1]} {routes}")
    assert cu._pg_block_route(stopped, block[0]) == routes
    # The "Publication autocreation is disabled" diagnosis names that one route for every state.
    guidance = _diagnose_connector_log("dsql-cdc-stack-debezium-source", _PUBLICATION_GONE_TAIL)
    assert "With the connectors removed:" not in guidance
    assert "After a rolled-back Start:" not in guidance
    assert guidance.index(DELETE) < guidance.index(RERUN)

    started = _route_halves(lang)[1]
    assert _steps_in_order(
        lang, started, "Delete CDC infrastructure", "Re-run Full Load", "Start CDC"
    ), lang
    assert _fact(lang, started, "topics-hold"), lang
    assert _new_topics_first(lang, started), lang
    assert not _stale(lang, _STALE_ROUTES[lang]), lang
    # G25 round 5: stale since -- the stopped stack's two routes, the one route said of a
    # rolled-back stack alone, and the lost slot's advice that only a stack whose Start rolled
    # back needs new topics before the Full Load re-run.
    assert not _stale(lang, _STALE_BY_STATE[lang]), lang


# §4.3's inheritance row: its own fix is a Full load + CDC run, and on a stack whose connectors
# were started before that run is told to follow Delete CDC infrastructure.


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_inheritance_block_asks_for_new_topics_before_its_full_load(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the block keeps its own fix on every state, and on a rolled-back stack
    # that fix (a Full load + CDC run) is told to get new topics first.
    refused = (pg.INHERITANCE_SNAPSHOT_HEADER, "Its own fix.")
    first = pg.pg_cdc_route(refused[0], started=True, cdc_only=False)
    assert first.endswith(
        f"So before that Full Load, use {DELETE} (the new stack starts with empty topics).")
    # G25 round 5: a stopped stack is told so too (its topics may hold the sink's unapplied
    # tail); one never started keeps its own fix alone.
    for status, committed in ((ROLLED_BACK, False), ("UPDATE_COMPLETE", True)):
        block, _state, _reads = _gate(monkeypatch, status, verdict=refused, committed=committed,
                                      force_initial=True)
        assert block == (refused[0], f"{refused[1]} {first}"), (status, committed)
    block, _state, _reads = _gate(monkeypatch, "UPDATE_COMPLETE", verdict=refused,
                                  force_initial=True)
    assert block == refused
    _block, refusal = pg._inheritance_snapshot_block((("app.par", 1, ("app.kid",)),))
    assert "a \"Full load + CDC\" run whose Full Load has finished" in refusal

    section = _section_43(lang)
    assert _has(lang, section, f"*{pg.INHERITANCE_SNAPSHOT_HEADER}*"), lang
    assert _fact(lang, section, "inheritance-started-first"), lang


# -- f2, f7, f5: the next Start continues from the recorded position, gapless only within the
# -- topics' retention ------------------------------------------------------------------------

# §4.3 keeps the user-visible half: a restart (or a Start after a rolled-back one) continues from
# the last recorded position, and a source connector that never completed its snapshot snapshots
# every selected table again. The cards' titles, what each promises and the one unchecked case
# (a first Start that rolled back and sent snapshot.mode=initial does not check the slot) are in
# the UI; the old wording stays pinned as stale below.
# Stale since round 5: a snapshot "only if no position was ever recorded" (Debezium also
# snapshots again from a position whose initial snapshot had not completed), and the card and
# line that "promise no snapshot" with nothing said of the start-point card.
_STALE_CONTINUES_R4 = {
    "en": (
        "Debezium snapshots every selected table first only if no position was ever recorded on "
        "the stack",
        "So the Start CDC card (*{header}*) and the Start's activity-log line promise no snapshot",
    ),
    "ko": (
        "스택에 기록된 위치가 하나도 없을 때만 Debezium이 먼저 선택한 모든 테이블을 스냅샷합니다",
        "그래서 Start CDC 카드(*{header}*)와 그 Start의 활동 로그 줄은 스냅샷을 약속하지 않습니다",
    ),
    "ja": (
        "スタックに記録された位置が 1 つもない場合にだけ、Debezium が先に選択したすべてのテーブルを"
        "スナップショットします",
        "そのため、Start CDC のカード (*{header}*) とその Start のアクティビティログの行はスナップショット"
        "を約束しません",
    ),
}
# The closing step's wording: an unconditional "no gap", and a Start that "snapshots".
_STALE_CONTINUES = {
    "en": (
        "the next Start continues from that position, with no gap",
        "a Start that snapshots does not check that the slot exists",
    ),
    "ko": (
        "다음 Start는 그 위치에서 빈틈 없이 이어 갑니다",
        "스냅샷하는 Start는 슬롯이 있는지 확인하지 않습니다",
    ),
    "ja": (
        "次の Start はその位置からギャップなく続けます",
        "スナップショットする Start は、スロットが存在するかを確認しません",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_stacks_card_and_start_line_promise_no_snapshot(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the card's text and header, the Start's activity-log line, and the slot
    # a Start that sends initial does not read on a stack whose first Start rolled back.
    recorded = "continues from the position the rolled-back Start recorded, if it recorded one"
    never = cu._pg_resume_continues("s", snapshot_never=True, rolled_back=True)
    initial = cu._pg_resume_continues("s", snapshot_never=False, rolled_back=True)
    for text in (never, initial):
        assert recorded in text and "FRESH SNAPSHOT" not in text and "stopping CDC" not in text
    assert "else from replication slot s's confirmed position, with no snapshot" in never
    # G25 round 5: a stored position whose initial snapshot had not completed snapshots again.
    assert "(or from where an earlier run stopped streaming). If that Start (or the earlier " \
           "run) had not completed its initial snapshot, or nothing was recorded, Debezium " \
           "snapshots every selected table first" in initial
    assert "with no snapshot" not in initial
    card = _function(_CDC_UI, "_render_cdc_start_button")
    header = "Ready to start CDC again"
    assert f'header="{header}" if _rolled_back else "Ready to resume CDC",' in card
    assert "elif is_pg and (resumes_from_offset or _pg_start_rolled_back(migration_state)):" in card
    auto = cu._cdc_start_detail("stk", _watermark(), mode="auto", source_type=SourceType.POSTGRES,
                                rolled_back=True, snapshot_mode="never")
    manual = cu._cdc_start_detail("stk", _watermark(), mode="manual",
                                  source_type=SourceType.POSTGRES, rolled_back=True,
                                  force_snapshot=True, snapshot_mode="initial")
    for line in (auto, manual):
        assert "this stack's last Start was rolled back by CloudFormation" in line
        assert recorded in line and "start point: a snapshot" not in line
    assert "Manual's full snapshot runs only if nothing was recorded on this stack, or the run " \
           "that recorded its position had not completed its initial snapshot" in manual
    assert "a FULL initial snapshot is forced" not in manual
    assert "rolled_back=_pg_start_rolled_back(migration_state)," in _function(
        _CDC_UI, "_start_cdc_deploy"
    )
    assert _gate(monkeypatch, ROLLED_BACK, verdict=None, force_initial=True)[2] == []
    assert _gate(monkeypatch, "UPDATE_COMPLETE", verdict=None, committed=True,
                 force_initial=True)[2] == [True]  # a stopped stack's slot is read

    section = _section_43(lang)
    assert _fact(lang, section, "restart-row") and _fact(lang, section, "pg-slot-position"), lang
    assert _fact(lang, section, "mid-snapshot"), lang
    assert not _stale(lang, _STALE_CONTINUES[lang]), lang
    # G25 round 5: stale since -- a snapshot "only if no position was ever recorded" (Debezium
    # also snapshots again from a position whose initial snapshot had not completed).
    stale = tuple(phrase.format(header=header) for phrase in _STALE_CONTINUES_R4[lang])
    assert not _stale(lang, stale), lang


@pytest.mark.parametrize("lang", LANGS)
def test_a_retry_after_a_rollback_is_gapless_only_while_the_topics_retain_its_changes(
    lang: str,
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the card and the line state the bound and the route past it; no
    # unconditional claim is left in the started rule's own description.
    retained = "gapless only while the topics still retain them"
    full = cu._pg_resume_continues("s", snapshot_never=True, rolled_back=True)
    only = cu._pg_resume_continues("s", snapshot_never=False, rolled_back=True, cdc_only=True)
    assert retained in full and retained in only
    assert (
        "retention period on MSK has passed since that Start, a Start here would skip them, so "
        f"instead use {DELETE} (the new stack starts with empty topics), then {RERUN} (it creates "
        "the slot again at its own snapshot point), then Start CDC."
    ) in full
    assert (
        f"so instead use {DELETE} (the new stack starts with empty topics), then Start CDC on the "
        "new stack, which snapshots every selected table"
    ) in only
    line = cu._cdc_start_detail("stk", _watermark(), mode="auto", source_type=SourceType.POSTGRES,
                                rolled_back=True, snapshot_mode="never")
    assert retained in line
    doc = " ".join((cu._pg_start_rolled_back.__doc__ or "").split())
    assert "a retry is gapless only while the topics still retain them" in doc
    assert "a retry is gapless." not in doc

    section = _section_43(lang)
    assert _fact(lang, section, "gapless-while-retained"), lang
    assert _steps_in_order(
        lang, section, _F43["restart-row"][lang], "Delete CDC infrastructure", "Re-run Full Load",
        "Start CDC", _F43["restart-cdc-only"][lang],
    ), lang
    assert _new_topics_first(lang, _route_halves(lang)[1]), lang


# -- f1: a withheld re-snapshot names the route that does work -------------------------------

# G25 round 5: the route named after that refusal is the one that works for it -- after an
# inheritance refusal a Full Load re-run (with CDC only, after a switch to Full load + CDC); the
# rest of the route rule's cases: test_g25_docs_r5.py.
# Chapter 13 keeps this sentence. §4.3 (answer-first) names the withheld re-snapshot by the notice
# the dialog shows (*Re-snapshot every table is not offered here: …*) and gives the refusal's own
# fix in the blocked-Start table: for an inheritance parent, a Full Load re-run (with CDC only,
# after the type switch) or unticking the parent.
_WITHHELD = {
    "13": {
        "en": "otherwise the dialog shows that Start's refusal (for example for a captured table "
        "with inheritance child tables; see [§4.3](04-cdc-and-dsql-constraints.md)) and then the "
        "route that works there: after an inheritance refusal, Re-run Full Load with \"Drop & "
        "reload\" instead, so the Start CDC after it takes no snapshot (with CDC only, switch the "
        "migration type to Full load + CDC first)",
        "ko": "그렇지 않으면 다이얼로그가 그 Start의 거부를 보여 줍니다(예컨대 상속 자식 테이블이 있는 "
        "캡처 대상 테이블. [§4.3](04-cdc-and-dsql-constraints.md) 참고). 그 뒤에는 그곳에서 통하는 경로를 "
        "알려 줍니다: 상속 때문에 거부되었다면 대신 \"Drop & reload\"로 Re-run Full Load를 실행하면 그 "
        "뒤의 Start CDC는 스냅샷하지 않습니다(CDC only에서는 먼저 마이그레이션 유형을 Full load + CDC로 "
        "바꿈)",
        "ja": "そうでなければダイアログがその Start の拒否を表示します (たとえば継承の子テーブルを持つ"
        "キャプチャ対象テーブル。[§4.3](04-cdc-and-dsql-constraints.md) を参照)。その後に、そこで使える"
        "方法を示します: 継承のための拒否なら、代わりに「Drop & reload」で Re-run Full Load すると、その後"
        "の Start CDC はスナップショットしません (CDC only では、先にマイグレーションの種類を Full load + "
        "CDC に切り替えます)",
    },
}
# Stale since round 5: one route after every refusal, CDC only and a slot it cannot use alike.
_STALE_WITHHELD = {
    "en": (
        "It then names the route that works there: Re-run Full Load with **Drop & reload**, which "
        "creates the slot again at its own snapshot point",
        "and then the route that works: Re-run Full Load with \"Drop & reload\", then Start CDC",
    ),
    "ko": (
        "그 뒤에는 그곳에서 통하는 경로를 알려 줍니다: Re-run Full Load를 **Drop & reload**로 실행하면",
        "그 뒤에는 통하는 경로를 알려 줍니다: \"Drop & reload\"로 Re-run Full Load를 실행한 뒤 Start CDC",
    ),
    "ja": (
        "その後に、そこで使える方法を示します: **Drop & reload** で Re-run Full Load すると",
        "その後に、使える方法を示します: 「Drop & reload」で Re-run Full Load してから Start CDC",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_withheld_re_snapshot_names_the_full_load_rerun(lang: str) -> None:
    from dsql_migrator.core.cdc_postgres import pg_snapshot_mode
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the refusal of the Start the remedy would unlock is followed by the route
    # that works, and the Start after that Full Load (its slot recorded) takes no snapshot.
    from dsql_migrator.core import cdc_pg_slot as pg

    lead = cu.PG_RESNAPSHOT_REFUSED_LEAD
    # G25 round 5: the route rule's words after that refusal (Full load + CDC; CDC only names
    # the switch to Full load + CDC first, and a slot it cannot use, the route after its fix).
    route = pg.pg_cdc_route(pg.INHERITANCE_SNAPSHOT_HEADER, started=False, cdc_only=False,
                            resnapshot=pg.RESNAPSHOT_REFUSED)
    assert route == (
        f"{RERUN} instead: it creates the slot again at its own snapshot point, and the Start "
        "CDC after it takes no snapshot."
    )
    dialog = inspect.getsource(cu._open_cdc_start_dialog)
    assert 'f"{PG_RESNAPSHOT_REFUSED_LEAD} {pg_resnapshot_block[1]}",' in dialog
    assert "resnapshot=RESNAPSHOT_REFUSED" in dialog
    assert pg_snapshot_mode(_watermark()) == "never"
    # Only a stack whose connectors were never started is offered (or refused) the remedy.
    assert "and _pg_stack_started(" in dialog

    section = _section_43(lang)
    assert _has(lang, section, f"*{lead}*"), lang
    assert _fact(lang, section, "inheritance-rerun") and _fact(lang, section, "cdc-only-switch"), lang
    assert _has(lang, _page(lang, _PAGE["13"]), _WITHHELD["13"][lang]), lang
    assert not _stale(lang, _STALE_WITHHELD[lang]), lang


# -- f3: a failed read of the re-snapshot route's own checks is reported as that -------------

# §4.3 no longer spells out the warning for a failed read of the re-snapshot route's OWN checks
# (*The re-snapshot route could not be checked*) or its activity-log line: they are shown by the
# dialog when the read fails. It keeps the publication and slot checks' notice, and the old
# wording (which blamed that notice for the re-snapshot's read) stays pinned as stale below.
# The closing step's wording: the publication and slot notice for the re-snapshot's own read.
_STALE_NOT_CHECKED = {
    "en": ("the remedy stays, and the dialog warns *CDC's publication and replication slot could "
           "not be checked*",),
    "ko": ("그 방법은 그대로 남고, 대화 상자가 *CDC's publication and replication slot could not be "
           "checked* 경고를",),
    "ja": ("その方法は残り、ダイアログが *CDC's publication and replication slot could not be "
           "checked* という警告",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_failed_re_snapshot_check_read_is_reported_as_that(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: its own notice and activity line, its own unread list, and the
    # publication/slot notice kept for a gate whose own read failed.
    header, action = cu.PG_RESNAPSHOT_NOT_CHECKED_HEADER, cu.PG_RESNAPSHOT_NOT_CHECKED_ACTION
    checks = cu.PG_CHECKS_NOT_RUN_HEADER
    assert header == "The re-snapshot route could not be checked"
    assert action == "PostgreSQL re-snapshot route check not run"
    body = cu.pg_resnapshot_not_checked_body("R")
    assert body.startswith("Start CDC checked CDC's publication and replication slot, but could "
                           "not read the PostgreSQL source again (R)")
    assert body.endswith("The remedy is still offered; the Start CDC job runs those checks before "
                         "it creates any connector, and goes ahead without them if it cannot read "
                         "the source either.")
    dialog = inspect.getsource(cu._open_cdc_start_dialog)
    assert "on_unread=pg_resnapshot_unread.append, force_initial=True," in dialog
    assert "elif pg_resnapshot_unread and not conn_blocker:" in dialog
    assert "header=PG_RESNAPSHOT_NOT_CHECKED_HEADER," in dialog
    assert "PG_RESNAPSHOT_NOT_CHECKED_ACTION," in dialog
    assert "if pg_unread and not conn_blocker:" in dialog

    assert _has(lang, _section_43(lang), f"*{checks}*"), lang
    assert not _stale(lang, _STALE_NOT_CHECKED[lang]), lang
