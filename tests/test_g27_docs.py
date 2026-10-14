# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G27 (docs): the manual, in English, Korean and Japanese alike, states what G27 shipped where it
describes those surfaces, and the quotes G27 or its repair made false are pinned as stale.

* Item [0]: a Start from the watermark of a Full Load that may have appended to an earlier copy.
  The start-point card, the Start CDC dialog and the Start's activity-log line state the Drop &
  reload condition (G26's RELOAD_FIRST meaning) before a Start that is not refused. G27 showed it
  only when Start CDC had been submitted in this migration, from an earlier start point, before
  the Full Load ran and the Full Load found rows already on the target ("already there"). The
  G27 repair adds a sign that needs no Start: a table found such rows on its FIRST attempt. The
  condition now covers an Append re-run made before CDC was ever started (the Re-run dialog's
  default, and where the binlog-purged remedy sent the operator), and it outlasts a Start that
  failed before it updated the stack and an Attach; the earlier-Start sign alone still ends at
  the next Start CDC submission and at an Attach, and Start over ends both. Chapter 4 §4.3 says
  so -- both signs, a first copy, a Drop & reload re-run, Retry and Reload, the rare signs on a
  copy that did not append, and how long each lasts -- and chapter 3's CDC only refusal points
  to it for the Start on the new stack without naming the earlier-Start sign alone. G27's "An
  Append re-run made before CDC was ever started ... gives no such sign" and "the notice ends
  once Start CDC is submitted from that watermark" are pinned as stale.
* Item [0], PostgreSQL (repair): when the start point is set to re-snapshot, or the Full Load did
  not finish, the card's re-snapshot notice gives a resume from that Full Load's slot as the way
  back; while the condition is due it no longer calls that resume gapless and names the Drop &
  reload re-run. Chapter 4 says so.
* Item [0], the Full Load confirm dialog (repair, rule E): in Full load + CDC, an action that
  takes a new start point no longer marks Append "recommended" and names Drop & reload under the
  choice; Append stays the default. Chapter 4 says so.
* Item [0], the binlog-purged remedy (repair, rule E): the Start CDC dialog's warning now names
  Drop & reload. Chapter 4 already did (tests/test_g25_docs_r8.py pins it); it is checked here
  against the code.
* Item [5]: the PostgreSQL Delete CDC infrastructure dialog says what the teardown does to the
  source -- once the stack is deleted it drops the stack's slot and its publication, each only
  if it exists, which releases the WAL the slot retains, and a drop that fails leaves the
  teardown complete with the statements to run in its log -- and what a later Start then needs.
  The repair: with password authentication it also says that a slot the teardown cannot drop
  keeps the source-credentials secret the tool created (the credentials needed to drop it), as
  the teardown does. Chapter 13's "Starting CDC again after a delete" says so, and FAQ Q31 no
  longer says the Start over teardown always removes that secret.
* Item [7]: Change exclusions & reload's per-table figure is the watermark's scan-free estimate
  and now reads "~N rows re-read (estimate)". Chapter 13 quoted the old "N rows re-read".
* Items [1] (the finished-teardown banner a redeploy of the same stack now clears), [2] (the
  migration-type lock's wording by CDC state), [3] (Deploy saves the typed stack-name suffix
  and subnets before its dialog) and [4] (Redeploy opens the CDC sub-step) change copy or
  behaviour no manual page describes or quotes, and item [6] (the page that showed Validation)
  changed no code: nothing to pin for them. Nor for the repair's GTID-only start-point notice,
  which no manual page quotes.

Each check reads the CODE first (its own constants, the REAL card, dialogs, Start click and
teardown over the G27 task tests' doubles, and Full Load records the REAL engine wrote) and only
then the manual text, with the hard wrap undone (in ja, with no whitespace at all).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.test_g25_docs import LANGS, ROOT, _function, _has, _page
from tests.test_g25_docs_r4 import _stale
from tests.test_g25_docs_r7 import _joined
from tests.test_g25_docs_r8 import _DROP_AND_RELOAD, _check

_P03 = "03-full-load.md"
_P04 = "04-cdc-and-dsql-constraints.md"
_P11 = "11-customer-faq.md"
_P13 = "13-excluding-tables-and-columns.md"
_SRC = ROOT / "src" / "dsql_migrator"


def _flat_source(path, name: str) -> str:
    """The function ``name`` of ``path``, its whitespace collapsed (a docstring's wrap undone)."""
    return " ".join(_function(path, name).split())


# -- item [0]: a Start after a Full Load that may have appended states the condition ---------

_RELOAD_CONDITION = {
    _P04: {
        "signal": {
            "en": "**After a Full Load that may have appended to an earlier copy**, the CDC step "
            "states the Drop & reload condition before a Start that begins at that Full Load's "
            "watermark and is not refused. It does so when this session shows that the Full Load "
            "found rows already on the target (*already there* in its row counts on the Full "
            "Load step) on a table's first attempt (*Attempts* 1), or that Start CDC was "
            "submitted in this migration, from an earlier start point, before that Full Load ran "
            "and the Full Load found any — for example a re-run with the default *Append*, made "
            "before CDC was ever started or after it had streamed.",
            "ko": "**앞선 복사본 위에 append했을 수 있는 Full Load 뒤에는**, 그 Full Load의 워터마크에서 "
            "시작하고 거부되지 않는 Start 전에 CDC 단계가 Drop & reload 조건을 알려 줍니다. 이 세션이 "
            "그 Full Load가 타깃에 이미 있던 행(Full Load 단계의 행 수에 *already there*로 표시)을 "
            "어떤 테이블의 첫 시도(*Attempts* 1)에서 찾았음을, 또는 이 마이그레이션에서 그 Full Load가 "
            "실행되기 전에 더 이른 시작 지점에서 Start CDC를 제출했고 그 Full Load가 그런 행을 하나라도 "
            "찾았음을 보여 줄 때입니다 — 예를 들어 CDC를 한 번도 시작하기 전에 했든 CDC가 스트리밍한 "
            "뒤에 했든, 기본값 *Append*로 한 재실행입니다.",
            "ja": "**以前のコピーに append した可能性のある Full Load の後は**、その Full Load の"
            "ウォーターマークから始まり、拒否されない Start の前に、CDC ステップが Drop & reload の"
            "条件を示します。このセッションが、その Full Load がターゲットに既にあった行 (Full Load "
            "ステップの行数に *already there* と表示されます) をあるテーブルの最初の試行 (*Attempts* "
            "が 1) で見つけたこと、または、このマイグレーションでその Full Load が実行される前に、"
            "より前の開始位置から Start CDC が送信されており、その Full Load がそのような行を一つでも"
            "見つけたことを示す場合です — たとえば CDC を一度も開始する前でも CDC がストリーミングした"
            "後でも、既定の *Append* で行った再実行です。",
        },
        "card": {
            "en": "The start-point card then shows *Gapless only if this Full Load did not "
            "append*: if that Full Load appended to them, Re-run Full Load choosing **Drop & "
            "reload** before Start CDC (CDC only has no Full Load step: on MySQL switch the "
            "migration type to Full load only for that run, then back to CDC only; on PostgreSQL "
            "switch it to Full load + CDC and re-run it there, which creates the slot again at "
            "its own snapshot point), because an append keeps those rows as they were and CDC "
            "from a watermark carries only the changes after it.",
            "ko": "그러면 시작 지점 카드가 *Gapless only if this Full Load did not append*를 보여 "
            "줍니다: 그 Full Load가 그 행에 append했다면 Start CDC 전에 **Drop & reload**로 Re-run "
            "Full Load 하세요(CDC only에는 Full Load 단계가 없습니다: MySQL에서는 그 실행 동안 "
            "마이그레이션 유형을 Full load only로 바꾼 뒤 다시 CDC only로 돌아오고, PostgreSQL에서는 "
            "유형을 Full load + CDC로 바꿔 그곳에서 다시 실행하며, 그 실행이 슬롯을 자신의 스냅샷 "
            "지점에 다시 만듭니다). append는 그 행을 그대로 두고, 워터마크에서 시작한 CDC는 그 뒤의 "
            "변경만 가져오기 때문입니다.",
            "ja": "そのとき開始位置のカードが *Gapless only if this Full Load did not append* を"
            "示します: その Full Load がそれらの行に append した場合は、Start CDC の前に **Drop & "
            "reload** で Re-run Full Load してください (CDC only には Full Load ステップがありません: "
            "MySQL ではその実行の間はマイグレーションの種類を Full load only に切り替え、その後 CDC "
            "only に戻します。PostgreSQL では種類を Full load + CDC に切り替えてそこで再実行し、その"
            "実行がスロットを自身のスナップショット位置に作り直します)。append はそれらの行をそのまま"
            "残し、ウォーターマークからの CDC はその後の変更しか運ばないためです。",
        },
        "surfaces": {
            "en": "The card no longer calls that start gapless, the Start CDC dialog shows the "
            "same notice without blocking Start, and the Start's activity-log line adds *(from "
            "the Full Load watermark; gapless only if that Full Load did not append to rows "
            "already on the target)* after its start point.",
            "ko": "카드는 그 시작을 더 이상 빈틈이 없다고 하지 않고, Start CDC 대화 상자는 Start를 막지 "
            "않은 채 같은 알림을 보여 주며, 그 Start의 활동 로그 줄은 시작 지점 뒤에 *(from the Full "
            "Load watermark; gapless only if that Full Load did not append to rows already on the "
            "target)*를 붙입니다.",
            "ja": "カードはその開始をもうギャップがないとは呼ばず、Start CDC のダイアログは Start を"
            "ブロックせずに同じ通知を示し、その Start のアクティビティログの行は開始位置の後に *(from "
            "the Full Load watermark; gapless only if that Full Load did not append to rows already "
            "on the target)* を付けます。",
        },
        "first-copy": {
            "en": "A first copy into empty tables finds no rows already there.",
            "ko": "빈 테이블로의 첫 복사는 이미 있던 행을 찾지 않습니다.",
            "ja": "空のテーブルへの最初のコピーは既にあった行を見つけません。",
        },
        "drop-and-reload": {
            "en": "A Drop & reload re-run recreates each table it loads, so the only rows it finds "
            "already there are ones it loaded itself (in a retry, or in a re-read after a dropped "
            "source connection).",
            "ko": "Drop & reload 재실행은 로드하는 각 테이블을 다시 만들므로, 그 실행이 타깃에 이미 있던 "
            "행으로 찾는 것은 자신이 로드한 행뿐입니다(재시도, 또는 소스 연결이 끊긴 뒤의 다시 "
            "읽기에서).",
            "ja": "Drop & reload の再実行はロードする各テーブルを作り直すため、その実行がターゲットに"
            "既にあった行として見つけるのは、自分でロードした行 (リトライ、またはソース接続が切れた後の"
            "再読み込みで) だけです。",
        },
        "retry": {
            "en": "Retry unfinished tables and Reload specific tables keep that Full Load's "
            "watermark and continue each table's attempt count from the earlier run, so the rows "
            "a table they load again finds give no first-attempt sign, and a table they leave as "
            "it was keeps its own. So a Retry or Reload that loads again every table that showed "
            "the sign ends the notice, even when that Full Load appended.",
            "ko": "Retry unfinished tables와 Reload specific tables는 그 Full Load의 워터마크를 유지하고 "
            "각 테이블의 시도 횟수를 앞선 실행에서 이어 세므로, 그 작업이 다시 로드하는 테이블이 찾은 "
            "행은 첫 시도의 표시를 남기지 않으며, 그대로 둔 테이블은 자신의 표시를 유지합니다. 그래서 "
            "표시를 남긴 모든 테이블을 다시 로드하는 Retry나 Reload는, 그 Full Load가 append했더라도 "
            "알림을 없앱니다.",
            "ja": "Retry unfinished tables と Reload specific tables はその Full Load のウォーター"
            "マークを維持し、各テーブルの試行回数を以前の実行から引き継いで数えるため、それらが再び"
            "ロードするテーブルが見つけた行は最初の試行の手がかりを残さず、そのまま残したテーブルは"
            "自身の手がかりを保ちます。そのため、手がかりを残したすべてのテーブルを再びロードする "
            "Retry や Reload は、その Full Load が append した場合でも通知を消します。",
        },
        "rare": {
            "en": "The notice can also show after a copy that did not append (its wording is "
            "conditional): when a table's first attempt re-read rows after a dropped source "
            "connection, when Aurora DSQL answered a batch that had committed with a temporary "
            "error (XX000) or the connection dropped after the commit, so that the batch's retry "
            "found its own rows, or when a multi-process load did not count one of a retried "
            "table's attempts.",
            "ko": "append하지 않은 복사 뒤에도 알림이 보일 수 있습니다(알림은 조건으로만 말합니다): "
            "테이블의 첫 시도가 소스 연결이 끊긴 뒤 행을 다시 읽었을 때, Aurora DSQL이 커밋된 배치에 "
            "일시적 오류(XX000)로 응답했거나 커밋 뒤 연결이 끊겨 그 배치의 재시도가 자신의 행을 "
            "찾았을 때, 또는 멀티프로세스 로드가 재시도한 테이블의 시도 하나를 세지 못했을 때입니다.",
            "ja": "append しなかったコピーの後にも通知が表示されることがあります (通知は条件として"
            "述べるだけです): テーブルの最初の試行がソース接続が切れた後に行を再読み込みした場合、"
            "Aurora DSQL がコミット済みのバッチに一時的なエラー (XX000) で応答したか、コミットの後に"
            "接続が切れたためにそのバッチのリトライが自身の行を見つけた場合、またはマルチプロセスの"
            "ロードがリトライしたテーブルの試行の一つを数えなかった場合です。",
        },
        "limits": {
            "en": "Rows found on a first attempt keep the notice for as long as that Full Load is "
            "the latest one, also after a Start CDC that failed before it updated the stack and "
            "after an Attach to an existing stack; otherwise the notice ends at the next Start "
            "CDC submission, from any start point, and after an Attach. Start over ends it in both "
            "cases.",
            "ko": "첫 시도에서 찾은 행은 그 Full Load가 최신인 동안 알림을 유지하며, 스택을 업데이트하기 "
            "전에 실패한 Start CDC 뒤에도, 기존 스택으로 Attach한 뒤에도 그렇습니다. 그렇지 않으면 "
            "알림은 어떤 시작 지점에서든 다음 Start CDC를 제출하면, 그리고 Attach하면 사라집니다. "
            "Start over는 두 경우 모두 알림을 없앱니다.",
            "ja": "最初の試行で見つかった行は、その Full Load が最新である限り通知を保ち、スタックを"
            "更新する前に失敗した Start CDC の後や、既存スタックへの Attach の後も同じです。そうでない"
            "場合、通知は、どの開始位置からであれ次に Start CDC を送信したとき、そして Attach の後に"
            "消えます。Start over はどちらの場合も通知を消します。",
        },
    },
    _P03: {
        "new-stack": {
            "en": "On the new stack that Start is not refused, and the CDC step states the "
            "condition before it when this session shows that the Full Load may have appended to "
            "an earlier copy "
            "([§4.3](04-cdc-and-dsql-constraints.md#43-the-gapless-full-load--cdc-handoff)).",
            "ko": "새 스택에서는 그 Start가 거부되지 않으며, 이 세션이 그 Full Load가 앞선 복사본 위에 "
            "append했을 수 있음을 보여 주면 CDC 단계가 그 Start 전에 그 조건을 알려 "
            "줍니다([§4.3](04-cdc-and-dsql-constraints.md#43-무손실-full-load--cdc-핸드오프)).",
            "ja": "新しいスタックではその Start は拒否されず、このセッションが、その Full Load が"
            "以前のコピーに append した可能性があることを示していれば、CDC ステップがその Start の"
            "前にその条件を示します "
            "([§4.3](04-cdc-and-dsql-constraints.md#43-ギャップのない-full-load--cdc-ハンドオフ))。",
        },
    },
}
# What G27 wrote that its repair made false: the earlier-Start sign named as the only one, an
# Append re-run before any Start said to give no sign, and the notice said to end at a Start.
_RELOAD_CONDITION_STALE = {
    "en": (
        "It does so when this session shows both",
        "An Append re-run made before CDC was ever started for this migration gives no such sign",
        "the notice ends once Start CDC is submitted from that watermark",
        "when this session shows that the Full Load ran after an earlier Start CDC",
    ),
    "ko": (
        "이 세션이 다음 두 가지를 모두 보여 줄 때입니다",
        "이 마이그레이션에서 CDC를 한 번도 시작하기 전에 한 Append 재실행은 그런 표시를 남기지 않으며",
        "알림은 그 워터마크에서 Start CDC를 제출하면",
        "Full Load가 앞선 Start CDC 뒤에 실행되어 타깃에 이미 있던 행을 찾았음을",
    ),
    "ja": (
        "このセッションが次の両方を示す場合です",
        "このマイグレーションで CDC を一度も開始する前に行った Append の再実行はそのような手がかりを"
        "残さず",
        "通知は、そのウォーターマークから Start CDC を送信したとき",
        "その Full Load が以前の Start CDC の後に実行され",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_start_after_a_full_load_that_may_have_appended_states_the_drop_and_reload_condition(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.ui.cdc_start_point_gate import (
        RELOAD_CONDITION_HEADER,
        RELOAD_CONDITION_LINE,
        reload_condition,
    )
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
    from dsql_migrator.ui.data_migration._cdc_status import (
        cdc_started_before_load,
        drop_reload_condition_due,
    )
    from tests.test_g27_cdc import (
        PG_W2,
        T1,
        T2,
        W1,
        W2,
        _card,
        _dialog,
        _job,
        _new_stack_after_the_detour,
        _pg_new_stack,
        _pg_session,
        _start_click,
        _start_line,
    )
    from tests.test_g27_repair_cdc import (
        _APPENDED,
        _FIRST,
        _current,
        _fresh,
        _mysql_never_started,
        _retry,
    )

    flc, cdc_only = MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY
    # The code first: the notice's header and the activity line's words, which the manual quotes.
    assert RELOAD_CONDITION_HEADER == "Gapless only if this Full Load did not append"
    assert RELOAD_CONDITION_LINE == (
        "(from the Full Load watermark; gapless only if that Full Load did not append to rows "
        "already on the target)"
    )
    # The notice's body: the re-run to make before Start CDC, and why -- for both engines with
    # Full load + CDC, and with CDC only the type switch each engine's re-run needs. It names an
    # earlier Start only when one preceded the Full Load; otherwise it says what the Full Load
    # found, which holds whichever sign showed the condition.
    why = (
        "An append keeps those rows as they were, and CDC from a watermark carries only the "
        "changes after it."
    )
    for postgres in (False, True):
        for started in (False, True):
            body = reload_condition(flc, postgres=postgres, started=started)
            assert (
                "if the Full Load appended to them, Re-run Full Load on the Full Load step "
                "choosing 'Drop & reload' before Start CDC." in body
            )
            assert body.endswith(why)
        assert reload_condition(flc, postgres=postgres, started=True).startswith(
            "This migration started CDC from an earlier start point before this Full Load ran"
        )
        assert reload_condition(flc, postgres=postgres).startswith(
            "This Full Load found rows already on the target."
        )
    assert (
        "(CDC only has no Full Load step: switch the migration type to Full load only for that "
        "run, then back to CDC only)" in reload_condition(cdc_only)
    )
    pg_only = reload_condition(cdc_only, postgres=True)
    assert (
        "switch the migration type to Full load + CDC and Re-run Full Load there choosing "
        "'Drop & reload' before Start CDC (it creates the slot again at its own snapshot point)"
        in pg_only
    )
    assert "back to CDC only" not in pg_only
    # The first-attempt sign, on records the REAL engine wrote (a run that takes a new start
    # point counts each table's attempts from 0): an Append re-run over the first copy, before
    # CDC was ever started. The REAL card states the condition and the REAL dialog repeats it
    # without blocking Start, with the body that names no Start. (Before the Start click below,
    # whose doubles record notices instead of rendering them.)
    first = _fresh(monkeypatch, W1, _FIRST)
    append = _fresh(monkeypatch, W2, _APPENDED)
    dropped = _fresh(monkeypatch, W2, {"orders": (11, 0), "customers": (3, 0)})  # Drop & reload
    assert {c.chunk_id: (c.attempts, c.rows_skipped) for c in append.chunks} == {
        "orders": (1, 10), "customers": (1, 3),
    }
    never = _mysql_never_started()
    texts = " ".join(_card(never, _current(append)).texts)
    assert RELOAD_CONDITION_HEADER in texts and "gapless from Full Load" not in texts
    assert reload_condition(flc) in texts
    ui = _dialog(monkeypatch, never, _current(append))
    assert ui.bodies[RELOAD_CONDITION_HEADER] == reload_condition(flc)
    assert ui.buttons["Start CDC"].enabled is True
    # PostgreSQL, after the detour with a Full load + CDC Append re-run (it created the slot
    # again): the REAL card states it, and its start from that slot no longer reads "gapless".
    texts = " ".join(_card(_pg_new_stack(), _job(PG_W2), session=_pg_session()).texts)
    assert "Start point set — from the replication slot (WAL LSN 0/5A000060)" in texts
    assert RELOAD_CONDITION_HEADER in texts and "gapless from the replication slot" not in texts
    # The live detour (MySQL, Full load + CDC): a Start CDC from W1, then a Full load only Append
    # re-run (W2), Delete, the type back, Redeploy. The REAL card states it and drops "gapless",
    # the REAL dialog repeats it without blocking Start, and the REAL Start click's activity
    # line follows its start point with the condition. These doubles' chunks count no attempt,
    # so only the earlier-Start sign shows here, and the click (it moves the window to W2) ends it.
    state = _new_stack_after_the_detour(monkeypatch)
    texts = " ".join(_card(state, _job(W2)).texts)
    assert RELOAD_CONDITION_HEADER in texts and "gapless from Full Load" not in texts
    ui = _dialog(monkeypatch, state, _job(W2))
    assert ("warning", RELOAD_CONDITION_HEADER) in ui.notices
    assert ui.buttons["Start CDC"].enabled is True
    run = _start_click(monkeypatch, state, W2, job=_job(W2))
    assert f"start point binlog mysql-bin.000100:4 {RELOAD_CONDITION_LINE}" in _start_line(run)
    assert drop_reload_condition_due(state, _job(W2)) is False
    # The signs. A first copy into empty tables and a Drop & reload re-run give none; an Append
    # re-run gives the first-attempt sign with no Start at all.
    fresh = DataMigrationState()
    assert drop_reload_condition_due(fresh, first) is False
    assert drop_reload_condition_due(fresh, dropped) is False
    assert drop_reload_condition_due(fresh, append) is True
    assert cdc_started_before_load(fresh, append) is False
    # It outlasts a Start submitted from its watermark (whatever that Start's outcome: the window
    # moves to the watermark's time) and an Attach.
    fresh.set_cdc_ops_window_start(T1)
    assert cdc_started_before_load(fresh, append) is True
    fresh.set_cdc_ops_window_start(T2)
    assert cdc_started_before_load(fresh, append) is False
    assert drop_reload_condition_due(fresh, append) is True
    assert fresh.adopt_cdc_stack("dsql-cdc-other")
    assert drop_reload_condition_due(fresh, append) is True
    # Rows found only on a later attempt (Reload specific tables re-reading the Drop & reload
    # copy): the earlier-Start sign alone, which the next Start submission and an Attach end.
    reread = _retry(monkeypatch, dropped, {"orders": (0, 11)})
    assert {c.chunk_id: (c.attempts, c.rows_skipped) for c in reread.chunks} == {
        "orders": (2, 11), "customers": (1, 0),
    }
    later = DataMigrationState()
    assert drop_reload_condition_due(later, reread) is False  # no Start before that Full Load
    later.set_cdc_ops_window_start(T1)
    assert drop_reload_condition_due(later, reread) is True
    later.set_cdc_ops_window_start(T2)
    assert drop_reload_condition_due(later, reread) is False
    later.set_cdc_ops_window_start(T1)
    assert later.adopt_cdc_stack("dsql-cdc-other")
    assert drop_reload_condition_due(later, reread) is False
    # Retry unfinished tables and Reload specific tables continue a table's attempt count: the
    # rows a table they load again finds give no first-attempt sign (an earlier copy's too),
    # and a table they carry keeps its own (an Append re-run's).
    failed = _fresh(monkeypatch, W1, _FIRST, fail=("customers",))
    retried = _retry(monkeypatch, failed, {"customers": (1, 2)})
    assert {c.chunk_id: c.attempts for c in retried.chunks} == {"orders": 1, "customers": 2}
    assert drop_reload_condition_due(DataMigrationState(), retried) is False
    append_failed = _fresh(monkeypatch, W2, _APPENDED, fail=("customers",))
    append_retried = _retry(monkeypatch, append_failed, {"customers": (0, 3)})
    assert drop_reload_condition_due(DataMigrationState(), append_retried) is True
    reloaded = _retry(monkeypatch, append, {"orders": (0, 11)})  # Reload specific tables
    assert {c.chunk_id: (c.attempts, c.rows_skipped) for c in reloaded.chunks} == {
        "orders": (2, 11), "customers": (1, 3),
    }
    assert drop_reload_condition_due(DataMigrationState(), reloaded) is True  # customers' sign
    # A first attempt whose re-read found rows (after a dropped source connection) counts them:
    # the record cannot tell its own rows from an earlier copy's.
    reread_first = _fresh(monkeypatch, W1, {"orders": (7, 3)})
    assert drop_reload_condition_due(DataMigrationState(), reread_first) is True
    # Start over resets the state in place: no current Full Load is left for the surfaces.
    fresh.job_id = append.job_id
    fresh.__init__()
    assert fresh.job_id is None and fresh.cdc_ops_window_start is None
    # "Already there" and "Attempts" are the Full Load step's own words, and the signal's
    # docstring names the cases the manual names.
    fl_ui = _joined(_SRC / "ui" / "data_migration" / "_full_load_ui.py")
    assert 'f"{row.rows_skipped:,} already there"' in fl_ui
    assert '"label": "Attempts"' in fl_ui
    signal = _flat_source(_SRC / "ui" / "data_migration" / "_cdc_status.py",
                          "drop_reload_condition_due")
    assert "a Drop & reload recreates each table it loads, so it finds none" in signal
    assert (
        "Rows it re-read itself (after a dropped source connection) or that a Retry had loaded "
        "count too" in signal
    )
    assert "a first attempt's re-read after a dropped source connection counts its own rows" in (
        signal
    )
    assert (
        "a re-run table whose start the multiprocess progress drain applied only after the table "
        "finished, or never, keeps its earlier count" in signal
    )

    _check(lang, _RELOAD_CONDITION)
    page = _page(lang, _P04)
    for quoted in (RELOAD_CONDITION_HEADER, RELOAD_CONDITION_LINE):
        assert _has(lang, page, f"*{quoted}*"), (lang, quoted)
    assert not _stale(lang, _RELOAD_CONDITION_STALE[lang]), lang


# -- item [0], PostgreSQL: the re-snapshot notices' resume from the slot is conditional too ----

_PG_RESNAPSHOT = {
    _P04: {
        "pg-resnapshot": {
            "en": "On PostgreSQL, when the start point is set to re-snapshot (*Manual "
            "re-snapshots instead of resuming from this load's slot*) or the Full Load did not "
            "finish, the card's re-snapshot notice says the same of the resume from that Full "
            "Load's slot it gives as the way back, and names the Drop & reload re-run for the case "
            "where that Full Load appended.",
            "ko": "PostgreSQL에서는 시작 지점이 다시 스냅샷하도록 설정되었을 때(*Manual re-snapshots "
            "instead of resuming from this load's slot*)나 Full Load가 끝나지 않았을 때, 카드의 "
            "재스냅샷 알림이 되돌아가는 방법으로 제시하는 그 Full Load 슬롯에서의 재개에 대해서도 같은 "
            "조건을 말하고, 그 Full Load가 append했을 경우를 위해 Drop & reload 재실행을 알려 줍니다.",
            "ja": "PostgreSQL では、開始位置がスナップショットを取り直す設定のとき (*Manual "
            "re-snapshots instead of resuming from this load's slot*) や Full Load が完了していない"
            "とき、カードの再スナップショットの通知が、戻る方法として挙げるその Full Load のスロット"
            "からの再開についても同じ条件を述べ、その Full Load が append した場合のために Drop & "
            "reload の再実行を示します。",
        },
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_4_says_the_postgresql_resnapshot_notices_state_the_condition_too(
    lang: str,
) -> None:
    from dsql_migrator.ui.cdc_start_point_gate import RELOAD_CONDITION_TOOLTIP
    from dsql_migrator.ui.data_migration import MigrationType, _cdc_ui
    from tests.test_g27_cdc import PG_W2, _card, _job, _pg_new_stack, _pg_session
    from tests.test_g27_repair_cdc import (
        _FL_CDC_RERUN,
        _MANUAL_RECOPIED_CAUSE,
        _MANUAL_REMEDY,
        _UNFINISHED_RECOPIED_REMEDY,
    )

    flc = MigrationType.FULL_LOAD_AND_CDC
    # The code first: the REAL card after the detour (the condition is due), its start point set
    # to re-snapshot. Its notice no longer promises a gapless resume from the slot, and names
    # the Drop & reload re-run beside the way back.
    state = _pg_new_stack()
    state.set_cdc_start_mode("manual")
    card = _card(state, _job(PG_W2), session=_pg_session())
    texts = " ".join(card.texts)
    assert "Manual re-snapshots instead of resuming from this load's slot" in texts
    assert "A gapless resume IS available here" not in texts
    assert _MANUAL_RECOPIED_CAUSE in texts
    assert (
        f"{_MANUAL_REMEDY} If that Full Load appended, {_FL_CDC_RERUN} before Start CDC." in texts
    )
    assert _cdc_ui.PG_RESUME_FROM_SLOT_ACTION in [b.text for b in card.buttons]
    assert RELOAD_CONDITION_TOOLTIP in card.tooltips
    # A Full Load that did not finish: the resume that comes back after the retry is conditional
    # too, with the re-run instead.
    _header, body = _cdc_ui.pg_resnapshot_notice(
        "unfinished_load", recopied=True, migration_type=flc
    )
    assert _UNFINISHED_RECOPIED_REMEDY.format(
        rerun=_FL_CDC_RERUN.replace("Re-run Full Load", "Re-run all tables"), note=""
    ) in body
    # A first copy keeps today's promise, word for word.
    assert "A gapless resume IS available here" in _cdc_ui.pg_resnapshot_notice("manual")[1]
    assert "the gapless resume comes back on its own" in (
        _cdc_ui.pg_resnapshot_notice("unfinished_load")[1]
    )

    _check(lang, _PG_RESNAPSHOT)


# -- item [0], rule E: the Full Load confirm dialog names Drop & reload for CDC's new start ----

_CONFIRM_DIALOG = {
    _P04: {
        "dialog": {
            "en": "Before such a Full Load, its confirm dialog on the Full Load step names Drop & "
            "reload too: in Full load + CDC, where selected tables already hold rows, the dialog "
            "of Start Full Load, Re-run Full Load or Re-run all tables (each takes a new start "
            "point) does not mark *Append* recommended, and a line under the choice reads *CDC "
            "from this run's new start point needs 'Drop & reload' …*, while *Append* stays the "
            "default. Retry unfinished tables and Reload specific tables keep the start point, and "
            "their choice is unchanged.",
            "ko": "그런 Full Load 전에는 Full Load 단계의 확인 대화 상자도 Drop & reload를 알려 줍니다: "
            "Full load + CDC에서 선택한 테이블에 이미 행이 있으면, Start Full Load, Re-run Full Load, "
            "Re-run all tables(모두 새 시작 지점을 잡습니다)의 대화 상자는 *Append*를 "
            "권장(recommended)으로 표시하지 않고 선택지 아래에 *CDC from this run's new start point "
            "needs 'Drop & reload' …* 줄을 보여 주며, 기본값은 여전히 *Append*입니다. Retry unfinished "
            "tables와 Reload specific tables는 시작 지점을 유지하며, 그 선택지는 바뀌지 않습니다.",
            "ja": "そのような Full Load の前には、Full Load ステップの確認ダイアログも Drop & reload "
            "を示します: Full load + CDC で、選択したテーブルに既に行がある場合、Start Full Load、"
            "Re-run Full Load、Re-run all tables (いずれも新しい開始位置を取ります) のダイアログは "
            "*Append* を推奨 (recommended) と表示せず、選択肢の下に *CDC from this run's new start "
            "point needs 'Drop & reload' …* という行を表示し、既定は引き続き *Append* です。Retry "
            "unfinished tables と Reload specific tables は開始位置を維持し、その選択肢は変わりませ"
            "ん。",
        },
    },
}
_QUOTED_LINE = "CDC from this run's new start point needs 'Drop & reload'"


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_4_says_the_confirm_dialog_names_drop_and_reload_for_a_new_cdc_start_point(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.ui.data_migration import _full_load_ui
    from tests.test_g27_repair_copy import (
        _APPEND_PLAIN,
        _APPEND_RECOMMENDED,
        _LINE,
        FLC,
        FLO,
        MY,
        _open,
    )

    # The code first: the line the manual quotes the start of ...
    assert _full_load_ui._APPEND_NEW_CDC_START_POINT == _LINE
    assert _LINE.startswith(_QUOTED_LINE + ": an Append keeps the rows already on the target")
    # ... is under the REAL dialog's choice for a run that takes a new start point in Full load +
    # CDC, where Append is not marked recommended and stays the default ...
    ui, state, (options, value) = _open(
        monkeypatch, engine=MY, mtype=FLC, action="Re-run Full Load"
    )
    assert options["append"] == _APPEND_PLAIN and "recommended" not in options["append"]
    assert _LINE in ui.texts
    assert value == "append" and state.reload_mode == "append"
    # ... and not under a Retry's (the original start point) or a Full load only run's.
    for mtype, action in ((FLC, "Retry unfinished tables"), (FLO, "Re-run Full Load")):
        ui, _state, (options, value) = _open(monkeypatch, engine=MY, mtype=mtype, action=action)
        assert options["append"] == _APPEND_RECOMMENDED and _LINE not in ui.texts
        assert value == "append"

    _check(lang, _CONFIRM_DIALOG)


# -- item [0], rule E: the binlog-purged route names Drop & reload, as the Start CDC warning ---


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_4s_binlog_purged_route_names_drop_and_reload_as_the_start_cdc_warning_does(
    lang: str,
) -> None:
    from dsql_migrator.core.watermark import binlog_resume_gap_reason

    # The code first: the Start CDC dialog's warning for a Full Load watermark whose binary log
    # the source purged names the Drop & reload re-run, and why.
    reason = binlog_resume_gap_reason("mysql-bin.000090", ["mysql-bin.000095"])
    assert (
        "Re-run the Full Load choosing 'Drop & reload' to take a fresh snapshot (and raise the "
        "source's binlog retention first" in reason
    )
    assert (
        "an Append keeps the rows already on the target as they were, and CDC from the new "
        "watermark never replays the changes made before it." in reason
    )
    assert "Re-run the Full Load to take a fresh snapshot" not in reason

    _check(lang, {_P04: {"binlog-purged": _DROP_AND_RELOAD[_P04]["binlog-purged"]}})


# -- item [5]: the PostgreSQL delete drops the slot and publication, as its dialog says -------

_PG_DELETE = {
    _P13: {
        "dialog": {
            "en": "On PostgreSQL, deleting the stack drops the replication slot and the "
            "publication. The Delete CDC infrastructure dialog says so, naming the slot (and the "
            "publication when this session knows its name): once the stack is deleted, the "
            "teardown drops each of them that exists on the source, which releases the WAL the "
            "slot retains, and if it cannot drop them, the teardown still completes and its log "
            "gives the statements to run on the source. It then keeps the source-credentials "
            "secret the tool created for CDC (with password authentication) instead of scheduling "
            "it for deletion, because that secret holds the credentials needed to drop the slot: "
            "delete it yourself once the slot is gone, as the dialog also says. When the slot it "
            "drops is the Full Load's, Start CDC can no longer resume from it, so choose "
            "**\"Re-snapshot every table instead\"** in the Start CDC dialog.",
            "ko": "PostgreSQL에서는 스택을 삭제하면 복제 슬롯과 publication이 drop됩니다. Delete CDC "
            "infrastructure 다이얼로그가 슬롯 이름(그리고 이 세션이 publication 이름을 알면 그 "
            "이름도)과 함께 그렇게 알려 줍니다: 스택이 삭제되면 teardown이 소스에서 그 둘을 각각 존재할 "
            "때만 drop하여 슬롯이 보관하던 WAL을 해제하며, drop하지 못해도 teardown은 그대로 끝나고 그 "
            "로그가 소스에서 실행할 문을 알려 줍니다. 그때 teardown은 도구가 CDC용으로 만든 소스 "
            "자격증명 시크릿(비밀번호 인증일 때)을 삭제 예약하지 않고 남겨 둡니다: 그 시크릿에 슬롯을 "
            "drop하는 데 필요한 자격증명이 있기 때문이며, 다이얼로그도 알려 주듯 슬롯이 없어진 뒤 직접 "
            "삭제하세요. drop하는 슬롯이 Full Load의 슬롯이면 Start CDC는 더 이상 그 슬롯에서 재개할 수 "
            "없으므로, Start CDC 다이얼로그에서 **\"Re-snapshot every table instead\"**를 선택하세요.",
            "ja": "PostgreSQL では、スタックを削除するとレプリケーションスロットと publication が"
            "削除されます。Delete CDC infrastructure のダイアログは、スロットの名前 (このセッションが "
            "publication の名前を知っていればその名前も) を挙げてそう示します: スタックが削除されると、"
            "teardown はソースにあるそれぞれを存在する場合にだけ削除し、スロットが保持していた WAL を"
            "解放します。削除できなかった場合も teardown はそのまま完了し、そのログがソースで実行する"
            "文を示します。その場合 teardown は、ツールが CDC 用に作成したソース認証情報シークレット "
            "(パスワード認証の場合) を削除予定にせず残します: そのシークレットにスロットの削除に必要な"
            "認証情報があるためで、ダイアログも示すとおり、スロットがなくなった後に自分で削除して"
            "ください。削除されるスロットが Full Load のスロットであれば、Start CDC はもうその"
            "スロットから再開できないため、Start CDC ダイアログで **「Re-snapshot every table "
            "instead」** を選んでください。",
        },
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_13_says_what_the_postgresql_delete_drops_as_its_dialog_does(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core import cdc_deployer, cdc_pg_slot
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_g27_cdc import (
        _OLD_SLOT,
        _PUB,
        _SLOT,
        E2E26PG,
        _delete_dialog_texts,
        _pg_delete_state,
        _pg_load,
    )

    # The code first: the REAL Delete action's dialog on a PostgreSQL stack whose slot is the one
    # the Full Load recorded names both objects, says when and how they are dropped, and that
    # Start CDC then cannot resume from that slot, with the re-snapshot route.
    text = " ".join(_delete_dialog_texts(_pg_delete_state(), _pg_load()))
    assert (
        "On PostgreSQL, once the stack is deleted, the teardown also drops this stack's "
        f"replication slot '{_SLOT}' and its publication '{_PUB}' on the source (each only if it "
        "exists), which releases the WAL the slot retains. If it cannot drop them, the teardown "
        "still completes and its log gives the statements to run on the source."
    ) in text
    assert "Start CDC cannot resume from the Full Load's slot. Re-snapshot every table instead" in (
        text
    )
    # A publication whose name this session does not know is not named; the slot always is. And
    # when the slot it drops is not the Full Load's, nothing is said of a resume: that one stays.
    other = _cdc_ui._pg_delete_disclosure(_pg_delete_state(deployed=_OLD_SLOT), _pg_load())
    assert f"replication slot '{_OLD_SLOT}' and its publication on the source" in other
    assert "Full Load's slot" not in other
    # The teardown drops them only once the stack delete is done ...
    delete = _function(_SRC / "core" / "cdc_deployer.py", "run_cdc_delete")
    assert delete.index('driver.stage("stack_delete", "DONE")') < delete.index(
        "_drop_pg_source_replication("
    )
    # ... each only if it exists ...
    assert "if not slot_exists(connection, name):" in _function(
        _SRC / "core" / "cdc_pg_slot.py", "drop_replication_slot"
    )
    assert 'DROP PUBLICATION IF EXISTS "{ident}"' in _function(
        _SRC / "core" / "cdc_pg_slot.py", "drop_publication"
    )
    # ... and a drop that fails does not fail the teardown: its log gives the statements.
    def _unreachable(*_a, **_k):
        raise RuntimeError("could not connect to server")

    monkeypatch.setattr(cdc_pg_slot, "build_pg_source_write_engine", _unreachable)
    logged: list = []
    dropped = cdc_deployer._drop_pg_source_replication(
        SimpleNamespace(log=logged.append),
        stack_name=E2E26PG,
        stack_params={
            "EngineType": "postgres",
            "SourceDbHostname": "pg.example.internal",
            "SourceDbPort": "5432",
            "PgDatabaseName": "app",
            "PgSlotName": _SLOT,
            "PgPublicationName": _PUB,
        },
        region="ap-northeast-2",
        aws_profile=None,
        source_credentials=("app_user", "not-a-real-password"),
    )
    assert dropped is False
    assert (
        f"SELECT pg_drop_replication_slot('{_SLOT}'); DROP PUBLICATION IF EXISTS \"{_PUB}\";"
    ) in logged[-1]

    _check(lang, _PG_DELETE)


# -- item [5], repair: a slot the PostgreSQL teardown cannot drop keeps the secret ------------

_SECRET_KEPT = {
    _P11: {
        "start-over": {
            "en": "secret it created; on PostgreSQL it keeps that secret if it cannot drop the "
            "replication slot, since it holds the credentials needed to drop the slot).",
            "ko": "도구가 만든 소스 자격증명 시크릿도 함께 제거됩니다. 단, PostgreSQL에서 복제 슬롯을 "
            "drop하지 못하면 그 시크릿은 남겨 둡니다: 슬롯을 drop하는 데 필요한 자격증명이기 "
            "때문입니다).",
            "ja": "マネージドのソース認証情報シークレットも削除します。ただし PostgreSQL でレプリケーション"
            "スロットを削除できなかった場合は、スロットの削除に必要な認証情報であるため、そのシークレット"
            "を残します）。",
        },
    },
}
# FAQ Q31 before the repair: the Start over teardown said to remove the secret, always.
_SECRET_STALE = {
    "en": ("credentials secret it created). If the app is already gone",),
    "ko": ("도구가 만든 소스 자격증명 시크릿도 함께 제거됩니다). 앱이 이미",),
    "ja": ("マネージドのソース認証情報シークレットも削除します）。アプリがすでに消えている",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_says_a_slot_the_postgresql_teardown_cannot_drop_keeps_the_secret(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core.secrets import cdc_source_secret_name
    from tests.test_g27_cdc import E2E26PG, _delete_dialog_texts, _pg_delete_state, _pg_load
    from tests.test_g27_repair_cdc import _teardown

    secret = cdc_source_secret_name(E2E26PG)
    # The code first: the REAL dialog (password auth: the tool created the secret) says it ...
    texts = _delete_dialog_texts(_pg_delete_state(), _pg_load())
    assert (
        f"The source-credentials secret the tool created for CDC ('{secret}') will also be "
        "scheduled for deletion with a 7-day recovery window, so your database credentials do not "
        "linger in Secrets Manager. If the replication slot cannot be dropped, the teardown keeps "
        "the secret instead: it holds the credentials needed to drop the slot, so delete it "
        "yourself once the slot is gone."
    ) in texts
    # ... as the REAL teardown does it: a slot it dropped -> the secret is scheduled for
    # deletion; one it could not drop -> the secret stays, and its log says why.
    _logs, deleted = _teardown(monkeypatch, slot_dropped=True)
    assert deleted == [E2E26PG]
    logs, deleted = _teardown(monkeypatch, slot_dropped=False)
    assert deleted == []
    assert any(
        f"Keeping the source-credentials secret '{secret}' in place: the replication slot could "
        "not be dropped" in m for m in logs
    )
    # Start over's "Delete all CDC infrastructure" runs the same teardown, secret cleanup on.
    launch = _function(_SRC / "ui" / "app.py", "_launch_cdc_teardown")
    assert "run_cdc_delete(" in launch and "cleanup_source_secret=cleanup_secret," in launch

    _check(lang, _PG_DELETE)
    _check(lang, _SECRET_KEPT)
    assert not _stale(lang, _SECRET_STALE[lang]), lang


# -- item [7]: Change & reload's row figure is quoted as the estimate it is ------------------

_ESTIMATE = {
    _P13: {
        "row-figure": {
            "en": "with the column diff (`+ c (NULL)`, `- c`) and a row estimate (\"~N rows "
            "re-read (estimate)\")",
            "ko": "컬럼 diff(`+ c (NULL)`, `- c`)와 행 추정치(\"~N rows re-read (estimate)\")",
            "ja": "(`+ c (NULL)`、`- c`)と行数の見積もり(「~N rows re-read (estimate)」)",
        },
    },
}
# Chapter 13 before G27: the figure quoted as a count.
_ESTIMATE_STALE = {
    "en": ("a row estimate (\"N rows re-read\")",),
    "ko": ("행 추정치(\"N rows re-read\")",),
    "ja": ("行数の見積もり(「N rows re-read」)",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_13_quotes_the_reload_row_figure_as_the_estimate_it_is(lang: str) -> None:
    from tests.test_g27_ui import _drop_card_texts

    # The code first: the REAL result card's table line, over a watermark whose row counts are
    # the scan-free estimate (2,000 for a table that held 2,890 rows, live); none when unknown.
    (line,) = [t for t in _drop_card_texts({"app.orders": 2000}) if t.startswith("app.orders")]
    assert line.endswith("   ~2,000 rows re-read (estimate)")
    (unknown,) = [t for t in _drop_card_texts({"app.orders": None}) if t.startswith("app.orders")]
    assert "rows re-read" not in unknown

    _check(lang, _ESTIMATE)
    assert not _stale(lang, _ESTIMATE_STALE[lang]), lang
