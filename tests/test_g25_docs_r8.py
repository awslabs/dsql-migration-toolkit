# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 8 (docs, the last round): the manual, in English, Korean and Japanese alike, states
what round 8 shipped, and what it said before is pinned as stale so it cannot come back.

* A first MySQL Start that CloudFormation rolled back. The rollback restores the stack's
  parameters, so the stack records only THAT it rolled back a Start, and the app keeps the start
  point that Start offered in memory. While the app holds it, Start CDC refuses a later point
  and lets a retry from the same one through; after an app restart every start point is refused
  on that stack, the same one included. Chapter 3 said a later point was refused "only until the
  app restarts", and the chapter 6 lead (finding [14]) claimed a refusal the code did not make
  after a restart. The code now makes it, so the lead states exactly that, and chapter 4 too.
* Rule E: any route that re-runs a Full Load for a new copy (before CDC from a new start point,
  or a fresh copy before cut over) names Drop & reload, because the dialog's default Append keeps
  the rows already on the target. Filling only unfinished work is Retry unfinished tables. No
  page calls an Append re-run idempotent.
* Rule F: Full load only has no CDC step, so a route from it switches the type first and deletes
  and deploys on the CDC step (chapters 4, 6, 10 and 13).
* PostgreSQL: a Start after a Full load only run continues from the stack's slot only if its
  source connector streamed. Otherwise Debezium snapshots every selected table first, so the
  manual no longer promises "no re-snapshot".
* Validation's recovery card, as round 8 renders it. A CDC type whose stack never streamed
  re-runs with Drop & reload (the card shown after the streamed order's redeploy too). The
  extra-row card of a CDC only session with no Full Load record names a new Full Load start
  point.
* The CDC-live Retry / Reload confirm dialog states the CDC-topics condition (rule D).

Each check reads the CODE first (its own constants and the real derivations, the real screens
where a notice is rendered) and only then the manual text, with the hard wrap undone (in ja,
with no whitespace at all).

G26 changed what three of these said, so their pins moved to test_g26_docs.py: after an app
restart the app reads a rolled-back first Start's start point back from the stack's
CloudFormation events, so every start point is refused only when they do not give it (chapters
3, 4 and 6); with CDC only, the refusal names the Drop & reload condition of its new-stack route
itself (chapter 3); and the chapter 6 lead's reloaded tables converge only where the stack's
source connector streamed (in the snapshot case a row deleted on the source in between stays on
the target).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.test_g12_table_set_after_load import _c, _job, _loaded
from tests.test_g12_table_set_after_load import _screen as _flo_screen
from tests.test_g25_docs import LANGS, ROOT, _has, _page
from tests.test_g25_docs_r4 import _stale
from tests.test_g25_docs_r7 import _joined
from tests.test_g25_fl_r6 import NEVER_STARTED, ROLLED_BACK, STOPPED, _probe, _state
from tests.test_ui_validation import _CopyUi, _drift_na, _release_summary

_P01 = "01-setup.md"
_P03 = "03-full-load.md"
_P04 = "04-cdc-and-dsql-constraints.md"
_P05 = "05-validation.md"
_P06 = "06-limitations.md"
_P10 = "10-conclusion.md"
_P11 = "11-customer-faq.md"
_P13 = "13-excluding-tables-and-columns.md"
_SRC = ROOT / "src" / "dsql_migrator"
_NOTICE_HEADER = "Full Load complete — want continuous replication (CDC) next?"


def _seed(file: str, pos: int) -> SimpleNamespace:
    return SimpleNamespace(binlog_file=file, binlog_position=pos)


def _check(lang: str, pins: dict) -> None:
    """Every ``{page: {key: {lang: phrase}}}`` pin is on its page."""
    for page, by_key in pins.items():
        text = _page(lang, page)
        for key, by_lang in by_key.items():
            assert _has(lang, text, by_lang[lang]), (lang, page, key)


def _in_order(text: str, *words: str) -> bool:
    positions = [text.index(word) for word in words]
    return positions == sorted(positions)


def _wm_job(file: str, pos: int):
    """A finished Full Load whose watermark is ``file:pos``."""
    job = _job("DONE", [_c("app.a"), _c("app.b")], counts={"app.a": 1, "app.b": 1})
    job.watermark = job.watermark.model_copy(
        update={"binlog_file": file, "binlog_position": pos}
    )
    return job


def _stopped_at(file: str, pos: int):
    """A stopped stack whose last Start offered ``file:pos``."""
    params, status = STOPPED
    return ({**params, "WatermarkBinlogFile": file, "WatermarkBinlogPos": str(pos)}, status)


def _notice(monkeypatch, *, job, stack, pg=False) -> str:
    """The Full load only completion notice's body, from the REAL screen over the REAL
    stack-phase probe."""
    screen = _flo_screen(monkeypatch, job=job, selected=["app.a", "app.b"], pg=pg)
    _probe(monkeypatch, screen.state, stack)
    screen.content(lambda: None)
    texts = screen.ui.texts
    return texts[texts.index(_NOTICE_HEADER) + 1]


def _recovery(**kwargs) -> _CopyUi:
    """Validation's REAL recovery section for an unexplained gap (``extra``: target rows the
    source does not have)."""
    from dsql_migrator.ui.validation import _render_recovery_section

    extra = kwargs.pop("extra", 0)
    ui = _CopyUi()
    _render_recovery_section(
        ui, _release_summary(is_match=False, mismatched=1, extra=extra), _drift_na(), **kwargs
    )
    return ui


def _steps(ui: _CopyUi) -> str:
    texts = ui.texts[ui.texts.index("Steps to recover") + 1 :]
    return " ".join(texts[1::2])


# -- the rolled-back first Start, after an app restart (finding [14]) ------------------------

# G26 moved the "judged" pins of chapters 3, 4 and 6 to test_g26_docs.py: after an app restart
# the app reads the start point a rolled-back first Start offered back from the stack's
# CloudFormation events, so every start point is refused there only when they do not give it.
_ROLLED_BACK = {
    _P06: {
        # The claim finding [14] doubted, true as written now.
        "no-detour": {
            "en": "so a detour through **Full load only** does not get around it",
            "ko": "**Full load only**를 거쳐 돌아와도 우회되지 않습니다",
            "ja": "**Full load only** を経由しても回避できません",
        },
    },
}
# Round 7's "until the app restarts" refusal, and the caveat finding [14] proposed for chapters
# 4 and 6, both false since round 8: a later point is refused after a restart too (since G26
# judged from the stack's events, and every start point when they do not give it).
_ROLLED_BACK_STALE = {
    "en": (
        "a start point later than the one that Start offered is refused only until the app "
        "restarts",
        "only until the app restarts",
    ),
    "ko": ("앱을 재시작하기 전까지만 거부합니다", "재시작하기 전까지만"),
    "ja": ("アプリが再起動するまでだけ拒否します", "再起動するまでだけ"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_first_start_is_refused_from_the_stack_record_after_a_restart(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.cdc_start_point_gate import OFFER_UNKNOWN_HEADER, new_topics_route
    from dsql_migrator.ui.data_migration._cdc_status import newer_start_point
    from dsql_migrator.ui.data_migration._cdc_ui import newer_start_point_refusal
    from dsql_migrator.ui.data_migration._models import MigrationType

    offered = ("mysql-bin.000002", 500)
    earlier, same, later = (
        _seed("mysql-bin.000002", 4), _seed(*offered), _seed("mysql-bin.000003", 4)
    )

    def judge(seed, offered_seed, source_type=SourceType.MYSQL):
        return newer_start_point(
            seed, has_committed_offset=False, connectors_exist=False, rolled_back=True,
            deployed_watermark=None, offered_seed=offered_seed, source_type=source_type,
        )

    # The code first. While the app holds the offer: a later point is refused, a retry from
    # the same one is not.
    assert judge(later, offered) is not None and judge(later, offered).rolled_back
    assert judge(same, offered) is None
    # With no record of the offer (G26: after an app restart, only when the stack's events do
    # not give it): every start point is refused, the same one included; a Start that offers
    # none is not judged, nor is PostgreSQL.
    for seed in (earlier, same, later):
        refused = judge(seed, None)
        assert refused is not None and refused.rolled_back and refused.last is None, seed
    assert judge(None, None) is None
    assert judge(later, None, SourceType.POSTGRES) is None
    # The real stack-phase probe over a rolled-back stack, with the offer gone (as after an app
    # restart whose stack events do not give it): the refusal the cards, the click and the job
    # show, with the route by type.
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        state = _state(monkeypatch, ROLLED_BACK, mtype=mtype)
        assert state.cdc_offered_seed is None
        notice, line = newer_start_point_refusal(state, same, SourceType.MYSQL)
        # DELIBERATE G26 UPDATE: this header does not call that same point newer (G26 CDC-2:
        # the offer is unknown here because this probe's deployer reads no stack events).
        assert notice[2] == OFFER_UNKNOWN_HEADER
        assert notice[3].startswith(
            "CloudFormation rolled back this stack's last Start, and the start offset that "
            "Start offered is no longer known"
        )
        assert new_topics_route(mtype) in notice[3]
        assert len("Start CDC blocked: " + line) <= 300

    _check(lang, _ROLLED_BACK)
    assert not _stale(lang, _ROLLED_BACK_STALE[lang]), lang


# -- rule F: Full load only has no CDC step ---------------------------------------------------

_TYPE_SWITCH_FIRST = {
    _P04: {
        "route": {
            "en": "On one that has streamed, Start CDC refuses that newer start point: set the "
            "migration type to CDC only, then on its CDC step Delete CDC infrastructure and "
            "deploy it again before Start CDC (Full load only has no CDC step;",
            "ko": "스트리밍한 적 있는 스택에서는 Start CDC가 그 더 새 시작 지점을 거부하므로, 마이그레이션 "
            "유형을 CDC only로 바꾼 뒤 그 CDC 단계에서 Delete CDC infrastructure 후 다시 배포하고 Start "
            "CDC 하세요(Full load only에는 CDC 단계가 없습니다.",
            "ja": "ストリーミングしたことのあるスタックでは Start CDC がそのより新しい開始位置を拒否する"
            "ため、マイグレーションの種類を CDC only に切り替えてから、その CDC ステップで Delete CDC "
            "infrastructure して再デプロイし、Start CDC してください (Full load only には CDC ステップが"
            "ありません。",
        },
    },
    _P06: {
        "detour": {
            "en": "Full load only has no CDC step: on a detour through it, switch the migration "
            "type first, then delete and deploy on the CDC step.",
            "ko": "Full load only에는 CDC 단계가 없으므로, 그 유형을 거쳐 가는 중이라면 먼저 마이그레이션 "
            "유형을 바꾼 뒤 CDC 단계에서 삭제하고 다시 배포하세요.",
            "ja": "Full load only には CDC ステップがないため、それを経由している場合は、先にマイグレー"
            "ションの種類を切り替えてから、CDC ステップで削除して再デプロイしてください。",
        },
    },
    _P13: {
        "lock": {
            "en": "after that re-run, switch the type, and if the CDC stack that streamed still "
            "exists, Delete CDC infrastructure and deploy it again on the CDC step before Start "
            "CDC: Full load only has no CDC step, and a stack that has streamed only continues "
            "from where it stopped, never from a newer start point",
            "ko": "그 재실행 뒤에 유형을 바꾸고, 스트리밍한 CDC 스택이 아직 있으면 Start CDC 전에 CDC "
            "단계에서 Delete CDC infrastructure 후 다시 배포하세요: Full load only에는 CDC 단계가 없으며, "
            "스트리밍한 적 있는 스택은 멈춘 곳에서만 이어 가며, 더 새 시작 지점에서는 결코 시작하지 "
            "않습니다",
            "ja": "その再実行の後でタイプを切り替え、ストリーミングした CDC スタックがまだあれば、Start "
            "CDC の前に CDC ステップで Delete CDC infrastructure して再デプロイしてください: Full load "
            "only には CDC ステップがなく、ストリーミングしたことのあるスタックは停止した位置から続ける"
            "だけで、より新しい開始位置から開始することは決してありません",
        },
        "legacy-null": {
            "en": "1. If a CDC stack exists, delete the CDC infrastructure first (Stop CDC first "
            "while it streams; Full load only has no CDC step, so switch to Full load + CDC to "
            "delete it).",
            "ko": "1. CDC 스택이 있으면 먼저 CDC 인프라를 삭제하세요(스트리밍 중이면 그 전에 Stop CDC. Full "
            "load only에는 CDC 단계가 없으므로 Full load + CDC로 바꿔서 삭제).",
            "ja": "1. CDC スタックが存在する場合は、まず CDC インフラを削除します(ストリーミング中は先に "
            "Stop CDC。Full load only には CDC ステップがないため、Full load + CDC に切り替えてから削除"
            "します)。",
        },
    },
}
_TYPE_SWITCH_FIRST[_P10] = _TYPE_SWITCH_FIRST[_P04]
# The round-7 wording: a Delete CDC infrastructure the reader cannot reach from Full load only.
_DELETE_FIRST_STALE = {
    "en": (
        "Start CDC refuses that newer start point: Delete CDC infrastructure and deploy it "
        "again first",
        "before that re-run, if this migration's CDC stack still exists, Delete CDC "
        "infrastructure and deploy it again",
    ),
    "ko": (
        "그 더 새 시작 지점을 거부하므로, 먼저 Delete CDC infrastructure 후 다시 배포하세요",
        "그 재실행 전에, 이 마이그레이션의 CDC 스택이 아직 있으면 Delete CDC infrastructure 후 다시 "
        "배포하세요",
    ),
    "ja": (
        "そのより新しい開始位置を拒否するため、先に Delete CDC infrastructure して再デプロイしてください",
        "その再実行の前に、この移行の CDC スタックがまだあれば Delete CDC infrastructure して再デプロイ"
        "してください",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_full_load_only_has_no_cdc_step_so_the_type_switch_comes_first(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType, substeps_for_type

    # The code first: Full load only has no CDC sub-step (no Delete CDC infrastructure on the
    # screen), so its own completion notice on a MySQL stack that has streamed switches the type
    # before it names the CDC step's Delete and deploy.
    assert "cdc" not in substeps_for_type(MigrationType.FULL_LOAD_ONLY)
    assert "cdc" in substeps_for_type(MigrationType.CDC_ONLY)
    body = _notice(
        monkeypatch, job=_wm_job("mysql-bin.000009", 4), stack=_stopped_at("mysql-bin.000003", 100)
    )
    assert _in_order(
        body, 'set the migration type to "CDC only"',
        "on its CDC step Delete CDC infrastructure and deploy it again", "then Start CDC",
    )

    _check(lang, _TYPE_SWITCH_FIRST)
    assert not _stale(lang, _DELETE_FIRST_STALE[lang]), lang


# -- rule E: a new copy is a Drop & reload ---------------------------------------------------

_DROP_AND_RELOAD = {
    _P01: {
        "lost-slot": {
            "en": "with Full load + CDC, re-run Full Load with **Drop & reload**, after Delete CDC "
            "infrastructure and a new deploy if the stack's connectors were started before.",
            "ko": "Full load + CDC에서는 **Drop & reload**로 Full Load를 다시 실행하되, 스택의 커넥터를 "
            "전에 시작했다면 먼저 Delete CDC infrastructure 후 다시 배포합니다.",
            "ja": "Full load + CDC では **Drop & reload** で Full Load を再実行します。スタックのコネクタを"
            "以前に開始していた場合は、その前に Delete CDC infrastructure して再デプロイします。",
        },
    },
    _P03: {
        "batch": {
            "en": "`INSERT ... ON CONFLICT` makes re-running a batch idempotent (no duplicates).",
            "ko": "`INSERT ... ON CONFLICT` 덕분에 배치를 다시 실행해도 중복이 생기지 않음.",
            "ja": "`INSERT ... ON CONFLICT` により、バッチの再実行は冪等になります(重複なし)。",
        },
        "coverage-lock": {
            "en": "Choose **Drop & reload** for that re-run: CDC then starts from its watermark, "
            "and an *Append* keeps the rows the earlier load wrote as they were, without the "
            "source's updates and deletes since.",
            "ko": "그 재실행은 **Drop & reload**로 하세요: 그러면 CDC가 그 워터마크에서 시작하는데, "
            "*Append*는 앞선 적재가 쓴 행을 그 뒤 소스의 UPDATE와 DELETE 없이 그대로 둡니다.",
            "ja": "その再実行は **Drop & reload** で行ってください: CDC はそのウォーターマークから開始"
            "しますが、*Append* は以前のロードが書いた行を、その後のソースの UPDATE や DELETE を反映"
            "しないまま残します。",
        },
        "rerun-all": {
            "en": "is loaded by Re-run all tables instead, choosing **Drop & reload**: it takes a "
            "fresh start point for every table, and an append would keep the rows already loaded "
            "as they were.",
            "ko": "대신 Re-run all tables가 로드하며, 이때 **Drop & reload**를 선택하세요: 모든 테이블에 "
            "대해 새 시작 지점을 잡는데, append는 이미 로드된 행을 그대로 두기 때문입니다.",
            "ja": "代わりに Re-run all tables がロードします。その際は **Drop & reload** を選んで"
            "ください: すべてのテーブルに新しい開始点を取りますが、append はロード済みの行をそのまま"
            "残すためです。",
        },
        # G26 moved the "cdc-only-route" pin to test_g26_docs.py: the refusal itself now names
        # that route's Drop & reload condition, with the type switch CDC only needs.
    },
    _P04: {
        "after-full-load-only": {
            "en": "Either way CDC starts from the watermark and carries only the changes after "
            "it, so if that Full load only run appended to rows an earlier load left on the "
            "target, first Re-run Full Load choosing **Drop & reload**.",
            "ko": "어느 쪽이든 CDC는 워터마크에서 시작해 그 뒤의 변경만 가져오므로, 그 Full load only "
            "실행이 앞선 적재가 타깃에 남긴 행에 append했다면 먼저 **Drop & reload**로 Re-run Full Load "
            "하세요.",
            "ja": "どちらの場合も CDC はウォーターマークから開始してその後の変更しか運ばないため、その "
            "Full load only の実行が以前のロードがターゲットに残した行に append した場合は、先に "
            "**Drop & reload** で Re-run Full Load してください。",
        },
        "rerun-all": {
            "en": "needs Re-run all tables, with **Drop & reload**, instead",
            "ko": "대신 **Drop & reload**로 Re-run all tables가 필요합니다",
            "ja": "代わりに **Drop & reload** での Re-run all tables が必要です",
        },
        "binlog-purged": {
            "en": "you'd re-run Full Load with **Drop & reload** to capture a fresh watermark (an "
            "append would keep the rows already loaded as they were)",
            "ko": "새 워터마크를 얻기 위해 **Drop & reload**로 Full Load를 다시 실행해야 합니다(append는 "
            "이미 로드된 행을 그대로 둡니다)",
            "ja": "新しいウォーターマークを取得するために **Drop & reload** で Full Load を再実行する"
            "ことになります (append はロード済みの行をそのまま残します)",
        },
    },
    _P10: {
        "principle": {
            "en": "**Idempotent and resumable.** Re-running a Full Load batch or replaying CDC "
            "never creates duplicates, and an interrupted load resumes only the unfinished work "
            "(**Retry unfinished tables**). A fresh copy is a **Drop & reload**: a re-run's "
            "default *Append* only adds the missing rows.",
            "ko": "**멱등적이고 재개 가능.** Full Load 배치 재실행이나 CDC 재적용은 중복을 만들지 않으며, "
            "중단된 적재는 끝나지 않은 작업만 이어서 재개합니다(**Retry unfinished tables**). 새 복사본은 "
            "**Drop & reload**로 만듭니다: 재실행의 기본값 *Append*는 빠진 행만 채웁니다.",
            "ja": "**冪等で再開可能。** Full Load のバッチの再実行や CDC の再適用が重複を作ることはなく、"
            "中断されたロードはまだ終わっていない作業だけを再開します (**Retry unfinished tables**)。"
            "新しいコピーは **Drop & reload** で作ります: 再実行の既定の *Append* は欠けている行を追加"
            "するだけです。",
        },
    },
    _P11: {
        "start-over": {
            "en": "Where the target tables still hold the earlier run's rows, choose **Drop & "
            "reload** in the new run's Start Full Load dialog: the default *Append* keeps them as "
            "they were.",
            "ko": "타깃 테이블에 이전 실행의 행이 아직 있으면, 새 실행의 Start Full Load 대화 상자에서 "
            "**Drop & reload**를 선택하세요: 기본값 *Append*는 그 행을 그대로 둡니다.",
            "ja": "ターゲットのテーブルに以前の実行の行がまだ残っている場合は、新しい実行の Start Full "
            "Load のダイアログで **Drop & reload** を選んでください: 既定の *Append* はそれらの行をその"
            "まま残します。",
        },
    },
    _P13: {
        "coverage-lock": {
            "en": "Choose **Drop & reload** for that re-run: CDC starts from its fresh watermark, "
            "and an *Append* keeps the rows the earlier load wrote as they were, without the "
            "source's updates and deletes since.",
            "ko": "그 재실행은 **Drop & reload**로 하세요: CDC는 그 새 워터마크에서 시작하는데, *Append*는 "
            "앞선 적재가 쓴 행을 그 뒤 소스의 UPDATE와 DELETE 없이 그대로 둡니다.",
            "ja": "その再実行は **Drop & reload** で行ってください: CDC はその新しいウォーターマークから"
            "開始しますが、*Append* は以前のロードが書いた行を、その後のソースの UPDATE や DELETE を反映"
            "しないまま残します。",
        },
        "no-definition": {
            "en": "use **Re-run all tables**, choosing **Drop & reload** (it takes a fresh start "
            "point for every table, and an append keeps the rows already loaded as they were), "
            "or untick it. A definition changed since the start point also needs Re-run all "
            "tables with Drop & reload.",
            "ko": "**Drop & reload**로 **Re-run all tables**를 사용하거나(모든 테이블에 새 시작 지점을 "
            "잡으므로, append는 이미 로드된 행을 그대로 둡니다) 체크를 해제하세요. 시작 지점 이후 정의가 "
            "바뀐 경우에도 Drop & reload로 Re-run all tables가 필요합니다.",
            "ja": "**Drop & reload** で **Re-run all tables** を使うか(すべてのテーブルに新しい開始点を"
            "取るため、append はロード済みの行をそのまま残します)、チェックを外してください。開始点以降に"
            "定義が変わった場合も、Drop & reload での Re-run all tables が必要です。",
        },
        "after-delete": {
            "en": "adding a table needs Re-run all tables, with Drop & reload.",
            "ko": "테이블을 추가하려면 Drop & reload로 Re-run all tables가 필요합니다.",
            "ja": "テーブルを追加するには、Drop & reload での Re-run all tables が必要です。",
        },
        "current-set": {
            "en": "Choose **Drop & reload** for it: CDC starts from that watermark, and an append "
            "keeps the rows already loaded as they were.",
            "ko": "그 재실행은 **Drop & reload**로 하세요: CDC는 그 워터마크에서 시작하는데, append는 이미 "
            "로드된 행을 그대로 둡니다.",
            "ja": "その再実行は **Drop & reload** で行ってください: CDC はそのウォーターマークから開始"
            "しますが、append はロード済みの行をそのまま残します。",
        },
    },
}
_DROP_AND_RELOAD[_P10]["after-full-load-only"] = _DROP_AND_RELOAD[_P04]["after-full-load-only"]
# The wording rule E retires: an Append re-run called idempotent, and new-copy routes with no
# Drop & reload.
_APPEND_STALE = {
    "en": (
        "makes any re-run idempotent",
        "Re-running Full Load or replaying CDC never creates duplicates",
        "you'd re-run Full Load to capture a fresh watermark",
        "or run the Full Load again (under its own CDC stack name",
        "is loaded by Re-run all tables instead.",
        "needs Re-run all tables instead",
        "use **Re-run all tables**, or untick it",
    ),
    "ko": (
        "몇 번을 다시 실행해도 중복이 생기지 않음",
        "Full Load 재실행이나 CDC 재적용은 중복을 만들지 않으며",
        "새 워터마크를 얻기 위해 Full Load를 다시 실행해야 합니다",
        "모든 테이블을 다시 스냅샷하거나 Full Load를 다시 실행하세요",
        "대신 Re-run all tables가 로드합니다.",
        "대신 Re-run all tables가 필요합니다",
        "**Re-run all tables**를 사용하거나 체크를 해제하세요",
    ),
    "ja": (
        "どの再実行も冪等になります",
        "Full Load の再実行や CDC の再適用が重複を作ることはなく",
        "新しいウォーターマークを取得するために Full Load を再実行することになります",
        "すべてのテーブルを再スナップショットするか、Full Load を再実行してください",
        "代わりに Re-run all tables がロードします。Full load only は変わりません",
        "代わりに Re-run all tables が必要です",
        "**Re-run all tables** を使うか、チェックを外してください",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_every_route_to_a_new_copy_names_drop_and_reload(monkeypatch, lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.cdc_start_point_gate import new_topics_route
    from dsql_migrator.ui.data_migration._models import MigrationType

    # The code first: every Full Load dialog opens on Append, which keeps the rows already on
    # the target, so a re-run for a new copy must choose Drop & reload -- as the routes the code
    # names do (PostgreSQL's and the new-topics one), and the Full load only notice whose CDC
    # only Start seeds this run's watermark.
    full_load_ui = _joined(_SRC / "ui" / "data_migration" / "_full_load_ui.py")
    assert 'migration_state.set_reload_mode("append")' in full_load_ui
    assert '"append": "Append — keep existing rows, load only the missing ones' in full_load_ui
    assert pg._RERUN_FULL_LOAD == "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
    assert "choosing 'Drop & reload'" in new_topics_route(MigrationType.FULL_LOAD_AND_CDC)
    body = _notice(monkeypatch, job=_wm_job("mysql-bin.000009", 4), stack=NEVER_STARTED)
    assert (
        "If this run appended to rows an earlier load left on the target, first Re-run Full "
        "Load choosing 'Drop & reload'" in body
    )

    _check(lang, _DROP_AND_RELOAD)
    assert not _stale(lang, _APPEND_STALE[lang]), lang


# -- PostgreSQL: a stack started before continues from its slot only if it streamed ----------

_PG_SLOT = {
    _P03: {
        "en": "so CDC continues from where it stopped if the stack's source connector streamed "
        "(otherwise Debezium snapshots every selected table first), as in any restart after Stop "
        "CDC (above).",
        "ko": "스택의 소스 커넥터가 스트리밍했다면 CDC는 Stop CDC 후의 모든 재시작처럼(위) 멈춘 곳에서 "
        "이어 갑니다(그렇지 않으면 Debezium이 먼저 선택한 모든 테이블을 스냅샷합니다).",
        "ja": "スタックの source connector がストリーミングしていれば、CDC は Stop CDC 後のあらゆる"
        "再起動と同じく (上記) 停止したところから続きます (そうでなければ Debezium が先に選択したすべて"
        "のテーブルをスナップショットします)。",
    },
    _P04: {
        "en": "on a CDC stack whose connectors were started before, Start CDC instead continues "
        "from that stack's own slot if its source connector streamed (otherwise Debezium "
        "snapshots every selected table first): gapless only while the CDC topics still retain "
        "what the sink had not applied",
        "ko": "커넥터를 전에 시작한 CDC 스택에서는 대신 Start CDC가 그 스택 자체의 슬롯에서 이어 가되, 그 "
        "스택의 소스 커넥터가 스트리밍하지 않았다면 Debezium이 먼저 선택한 모든 테이블을 스냅샷합니다: "
        "싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안만 빈틈이 없음",
        "ja": "以前にコネクタを開始した CDC スタックでは、代わりに Start CDC がそのスタック自身の"
        "スロットから続けます。ただし、そのスタックの source connector がストリーミングしていなければ、"
        "Debezium が先に選択したすべてのテーブルをスナップショットします: sink が適用していなかった変更を "
        "CDC トピックがまだ保持している間だけギャップがありません",
    },
    # G26 moved the chapter 6 pin to test_g26_docs.py: the reloaded tables converge only where
    # the stack's source connector streamed, not in the snapshot case.
}
_PG_SLOT[_P10] = _PG_SLOT[_P04]
# The promise round 8 withdrew: a Start after a Full load only run records no slot, so it sends
# snapshot.mode=initial, which snapshots first when the source connector never streamed.
_PG_SLOT_STALE = {
    "en": (
        "own slot, with no re-snapshot",
        "so CDC continues from where it stopped, as in any restart after Stop CDC (above).",
        "so CDC continues from where it stopped (gapless only while",
    ),
    "ko": (
        "재스냅샷 없이 이어 갑니다",
        "CDC는 Stop CDC 후의 모든 재시작처럼(위) 멈춘 곳에서 이어 갑니다.",
        "CDC는 멈춘 곳에서 이어 가고(싱크가 적용하지 않은",
    ),
    "ja": (
        "再スナップショットなしで続けます",
        "CDC は Stop CDC 後のあらゆる再起動と同じく (上記) 停止したところから続きます。",
        "CDC は停止した位置から続き (sink が適用していなかった",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_postgresql_continues_from_its_slot_only_if_its_source_connector_streamed(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core.cdc_postgres import pg_snapshot_mode
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: a Full load only run records no slot, so its watermark's Start sends
    # snapshot.mode=initial, which continues from the slot only from a stored position (the
    # source connector streamed) and otherwise snapshots first -- the card and the step's own
    # notice say so.
    job = _loaded()
    assert pg_snapshot_mode(job.watermark) == "initial"
    card = cu._pg_resume_continues("s", snapshot_never=False)
    assert "If its source connector streamed, it picks up where streaming stopped." in card
    assert "Debezium snapshots every selected table first" in card
    body = _notice(monkeypatch, job=job, stack=STOPPED, pg=True)
    assert (
        "continues from that stack's replication slot if its source connector streamed "
        "(otherwise Debezium snapshots every selected table first)" in body
    )
    assert "no re-snapshot" not in body

    for page, by_lang in _PG_SLOT.items():
        assert _has(lang, _page(lang, page), by_lang[lang]), (lang, page)
    assert not _stale(lang, _PG_SLOT_STALE[lang]), lang


# -- Validation's recovery card (05) ---------------------------------------------------------

_CARD = {
    _P05: {
        "never-streamed": {
            "en": "On a stack that never streamed the order is shorter (with CDC only, switch the "
            "migration type to Full load + CDC first → **Re-run Full Load** (**Re-run all "
            "tables** when some failed) choosing **Drop & reload** → **Start CDC**, which starts "
            "from the new Full Load's start point → **Re-run validation**). These are the steps "
            "above without Stop CDC and the redeploy, and the card shows them once that redeploy "
            "is done: the re-run is a **Drop & reload** there too, because CDC starts from its "
            "new start point and an append would keep rows older than it.",
            "ko": "스트리밍한 적 없는 스택에서는 순서가 더 짧습니다(CDC only에서는 먼저 마이그레이션 유형을 "
            "Full load + CDC로 바꿈 → **Re-run Full Load**(일부가 실패했다면 **Re-run all tables**)를 "
            "**Drop & reload**로 → **Start CDC**(새 Full Load의 시작 지점에서 시작) → **Re-run "
            "validation**). 위 단계에서 Stop CDC와 재배포를 뺀 것이며, 그 재배포를 마친 뒤에도 카드가 이 "
            "단계를 보여 줍니다: CDC가 새 시작 지점에서 시작하고 append는 그보다 오래된 행을 그대로 두므로, "
            "여기서도 재실행은 **Drop & reload**입니다.",
            "ja": "ストリーミングしたことのないスタックでは手順はもっと短くなります (CDC only では、先に"
            "マイグレーションの種類を Full load + CDC に切り替えます → **Drop & reload** で **Re-run Full "
            "Load** (一部が失敗していれば **Re-run all tables**) → **Start CDC** (新しい Full Load の開始"
            "位置から始まります) → **Re-run validation**)。上記の手順から Stop CDC と再デプロイを除いた"
            "もので、その再デプロイを終えた後もカードはこの手順を示します: CDC は新しい開始位置から始まり、"
            "append はそれより古い行をそのまま残すため、ここでも再実行は **Drop & reload** です。",
        },
        "extra-rows": {
            "en": "When the target holds rows the source does not, the card says *The target "
            "holds rows the source does not — a delete never landed*: a re-run only adds rows, "
            "so it names that per-table reload (**Reload specific tables**, with CDC stopped, "
            "choosing **Drop & reload**; with CDC only, after switching the migration type to "
            "Full load + CDC).",
            "ko": "타깃에 소스에 없는 행이 있으면 카드는 *The target holds rows the source does not — a "
            "delete never landed*라고 말합니다: 재실행은 행을 채우기만 하므로, 카드는 그 테이블별 다시 "
            "적재(CDC를 멈춘 채 **Reload specific tables**, **Drop & reload** 선택. CDC only에서는 "
            "마이그레이션 유형을 Full load + CDC로 바꾼 뒤)를 알려 줍니다.",
            "ja": "ターゲットにソースにない行がある場合、カードは *The target holds rows the source does "
            "not — a delete never landed* と述べます: 再実行は行を埋めるだけなので、カードはそのテーブル"
            "ごとの再ロード (CDC を停止して **Reload specific tables**、**Drop & reload** を選択。CDC "
            "only では、マイグレーションの種類を Full load + CDC に切り替えてから) を示します。",
        },
        "extra-rows-no-record": {
            "en": "With CDC only and no Full Load record in this session there is no table to "
            "reload, so it names a new Full Load start point instead: Stop CDC if it is running "
            "and **Delete CDC infrastructure** and deploy it again (on a stack that has streamed, "
            "or whose CDC state is not read yet), then switch the migration type to Full load + "
            "CDC, run its Full Load choosing **Drop & reload**, and **Start CDC**.",
            "ko": "CDC only이고 이 세션에 Full Load 기록이 없으면 다시 적재할 테이블이 없으므로, 대신 새 Full "
            "Load 시작 지점을 알려 줍니다: CDC가 실행 중이면 Stop CDC 후 **Delete CDC infrastructure** 하고 "
            "다시 배포한 다음(스트리밍한 적 있거나 CDC 상태를 아직 읽지 못한 스택이면), 마이그레이션 유형을 "
            "Full load + CDC로 바꿔 그 Full Load를 **Drop & reload**로 실행하고 **Start CDC** 합니다.",
            "ja": "CDC only でこのセッションに Full Load の記録がない場合は再ロードするテーブルがないため、"
            "代わりに新しい Full Load の開始位置を示します: CDC が動いていれば Stop CDC し、**Delete CDC "
            "infrastructure** して再デプロイしてから (ストリーミングしたことのある、または CDC の状態を"
            "まだ読み取っていないスタックの場合)、マイグレーションの種類を Full load + CDC に切り替え、"
            "その Full Load を **Drop & reload** で実行して **Start CDC** します。",
        },
    },
}
# The never-streamed order before round 8: a re-run with the dialog's default (Append), then CDC
# from the new snapshot's start point -- rows the source changed before it stay stale.
_CARD_STALE = {
    "en": ("Start/Re-run on the Full Load sub-step, then Start CDC from the new snapshot's start "
           "point",),
    "ko": ("Full Load 하위 단계에서 Start/Re-run, 그다음 새 스냅샷의 시작 지점에서 Start CDC",),
    "ja": ("Full Load のサブステップで Start/Re-run、次に新しいスナップショットの開始位置から Start CDC",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_validations_recovery_card_is_described_as_round_8_renders_it(lang: str) -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    flc, cdc_only = MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY
    # The code first. A CDC type whose stack never streamed: the re-run is a Drop & reload and
    # CDC starts from its start point, with no Stop CDC and no redeploy.
    for mtype in (flc, cdc_only):
        steps = _steps(_recovery(migration_type=mtype))
        assert "choose 'Drop & reload'" in steps, mtype
        assert "Start CDC: it starts from the new Full Load's start point" in steps, mtype
        assert "Stop CDC" not in steps and "Delete CDC infrastructure" not in steps, mtype
        assert "Start/Re-run to backfill" not in steps, mtype
    # Extra rows: a session with a Full Load record reloads the one table (original start
    # point); CDC only with none takes a new Full Load start point, behind new topics on a stack
    # that has streamed.
    kept = _recovery(migration_type=cdc_only, cdc_stack_streamed=True, extra=1).body()
    assert "The target holds rows the source does not — a delete never landed" in kept
    assert "'Reload specific tables…', with CDC stopped" in kept
    assert "keeps the original start point" in kept
    none = _recovery(
        migration_type=cdc_only, cdc_stack_streamed=True, full_load_record=False, extra=1
    ).body()
    assert "Reload specific tables" not in none
    assert _in_order(
        none, "Stop CDC if it is running", "Delete CDC infrastructure and deploy it again",
        "switch the migration type to Full load + CDC", "run its Full Load choosing Drop & "
        "reload", "and Start CDC.",
    )
    never = _recovery(migration_type=cdc_only, full_load_record=False, extra=1).body()
    assert "Delete CDC infrastructure" not in never
    assert "run its Full Load choosing Drop & reload" in never

    _check(lang, _CARD)
    assert not _stale(lang, _CARD_STALE[lang]), lang


# -- rule D: the CDC-live Retry / Reload confirm dialog --------------------------------------

_CDC_LIVE = {
    _P03: {
        "keeps-start-point": {
            "en": "The confirm dialog of **Retry unfinished tables**, **Reload specific tables** "
            "or a per-table **Reload**, which keep the original start point, says instead to Stop "
            "CDC first, run it, then Start CDC again: CDC then continues from where it stopped, "
            "gapless only while the CDC topics still retain what the sink had not applied (their "
            "retention period on MSK).",
            "ko": "원래 시작 지점을 유지하는 **Retry unfinished tables**, **Reload specific tables**, "
            "테이블별 **Reload**의 확인 대화 상자는 대신 먼저 Stop CDC를 하고 그 작업을 실행한 뒤 Start "
            "CDC를 다시 하라고 알려 줍니다: 그러면 CDC는 멈춘 곳에서 이어 가며, 싱크가 적용하지 않은 변경을 "
            "CDC 토픽이 아직 보존하는 동안(MSK에서의 보존 기간)만 빈틈이 없습니다.",
            "ja": "元の開始位置を維持する **Retry unfinished tables**、**Reload specific tables**、"
            "テーブルごとの **Reload** の確認ダイアログは、代わりに、先に Stop CDC してからその操作を実行"
            "し、もう一度 Start CDC するよう示します: CDC はその後、停止した位置から続き、ギャップがない"
            "のは sink が適用していなかった変更を CDC トピックがまだ保持している間 (MSK での保持期間) "
            "だけです。",
        },
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_cdc_live_retry_and_reload_dialog_states_the_topics_condition(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    # The code first: the dialog's way on for an action that keeps the original start point.
    assert fl._CDC_LIVE_KEEPS_START_POINT.startswith(
        "Stop CDC first (CDC step → Stop CDC), run this, then Start CDC again: this run keeps "
        "the original start point, so CDC continues from where it stopped"
    )
    assert fl._CDC_LIVE_KEEPS_START_POINT.endswith(
        "gapless only while the CDC topics still retain what the sink had not applied (their "
        "retention period on MSK)."
    )

    _check(lang, _CDC_LIVE)
