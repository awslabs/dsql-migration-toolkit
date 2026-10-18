# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 6 (docs): the manual, in English, Korean and Japanese alike, states what round 6
shipped -- decision (A) the new-start-point gate, (B) the route rule for the two surfaces that
still named a route of their own, and (C) the narrowed publish list's own fix before any route.

Each check reads the CODE first (through its own constants and the real derivations) and only
then the manual text, with the hard wrap undone (in ja, with no whitespace at all). What the
manual said before this round is pinned as stale, so it cannot come back:

* (A) a Full Load that takes a NEW start point -- Start Full Load, Re-run Full Load, Re-run all
  tables, and the page header's Run -- is refused on a Full load + CDC migration whose CDC
  stack has streamed, because Stop CDC leaves the sink's unapplied tail in the topics and its
  consumer group would re-apply it over the reloaded tables. Chapter 3 described no gate at
  all; chapter 4 still promised a resume from a slot "a Full Load run again since then
  re-creates" (test_g25_docs_r4's pin moved with this round), and chapters 4 and 6 sent a
  streamed stack straight to a Full Load re-run for a newer start point.
* (A) Validation's recovery runbook puts new CDC topics before the re-run on such a stack,
  and a reload of specific tables -- which keeps the original start point -- is never gated.
* (B) the CDC monitor's invalidated-slot notice takes the route rule's route (and names no
  Full Load step with CDC only).
* (C) the narrowed publish list names ALTER PUBLICATION ... SET (publish = ...) first, says
  why that alone is not enough (pgoutput publishes from the publication as of each change's
  commit), and only then the route.
* the Stop CDC dialog states the retained-topics condition for BOTH engines, and so do the
  MySQL resume cards; the inheritance block names "untick" only while the table picker can
  change the set, and names "Resume from the Full Load's slot instead" where a recorded
  re-snapshot would otherwise loop; a same-named slot CDC cannot use is reported, and fixed,
  first; and after a Stop the retention bound of a rolled-back Start counts from the Stop.
"""

from __future__ import annotations

import inspect
import re
from dataclasses import replace
from types import SimpleNamespace

import pytest

from tests.test_g25_docs import _CDC_UI, _PAGE, LANGS, _flat, _function, _has, _page
from tests.test_g25_docs_r4 import _slot_objects, _stale

_PAGE_03 = "03-full-load.md"
_PAGE_05 = "05-validation.md"
_ROOT = _CDC_UI.parents[4]
_VALIDATION = _ROOT / "src/dsql_migrator/ui/validation.py"
_DATA_MIGRATION = _ROOT / "src/dsql_migrator/ui/data_migration/__init__.py"
_CDC_MONITORING = _ROOT / "src/dsql_migrator/ui/data_migration/_cdc_monitoring.py"


def _runner_source() -> str:
    """The Data Migration screen's ``runner()`` -- the one place every new-start-point Full
    Load is submitted from (the Start dialog's Confirm and the page header's Run alike).
    Sliced here because it is nested and takes no argument, which ``_function`` cannot read."""
    source = _DATA_MIGRATION.read_text(encoding="utf-8")
    head = source.index("    def runner() -> None:")
    return source[head : source.index("\n    def ", head + 10)]


def _state(*, streamed=False, checked=True, cdc_only=False, probe_error=None):
    """A migration state the stack-phase probe could have left: ``streamed`` is a stack stopped
    after a Start (a committed offset), the state the gate holds on."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    return SimpleNamespace(
        migration_type=MigrationType.CDC_ONLY if cdc_only else MigrationType.FULL_LOAD_AND_CDC,
        cdc_has_committed_offset=streamed,
        cdc_stack_phase="infra",
        cdc_stack_phase_status="UPDATE_COMPLETE",
        cdc_stack_phase_checked=checked,
        cdc_connector_names=[],
        cdc_probe_error=probe_error,
    )


# -- (A) the new-start-point gate, in chapter 3 ---------------------------------------------

_GATE = {
    "actions": {
        "en": "on a **Full load + CDC** migration whose CDC stack has **streamed** — stopped "
        "after a Start, its connectors in place, or a Start that CloudFormation rolled back — "
        "every action that takes one is **refused**: **Start Full Load**, **Re-run Full Load** "
        "and **Re-run all tables** are disabled, and the page header's **Run** starts nothing",
        "ko": "CDC 스택이 **스트리밍한 적 있는** **Full load + CDC** 마이그레이션 — Start 뒤에 멈췄거나, "
        "커넥터가 그대로 있거나, CloudFormation이 Start를 롤백한 스택 — 에서는 새 워터마크를 가져가는 모든 "
        "작업이 **거부됩니다**: **Start Full Load**, **Re-run Full Load**, **Re-run all tables**는 "
        "비활성화되고, 페이지 헤더의 **Run**은 아무것도 시작하지 않습니다",
        "ja": "CDC スタックが **ストリーミングしたことのある** **Full load + CDC** のマイグレーション — "
        "Start の後に停止した、コネクタがそのまま残っている、または CloudFormation が Start をロールバック"
        "したスタック — では、新しいウォーターマークを取る操作はすべて **拒否されます**: "
        "**Start Full Load**、**Re-run Full Load**、**Re-run all tables** は無効になり、ページヘッダーの "
        "**Run** は何も開始しません",
    },
    "why": {
        "en": "Stop CDC (and a rolled-back Start) removes the sink without draining it, so its "
        "consumer group would re-apply changes older than the new snapshot over the reloaded "
        "tables, while the source skips the changes between the old stop point and the new "
        "snapshot",
        "ko": "Stop CDC(그리고 롤백된 Start)는 싱크를 드레인하지 않고 제거하므로, 그 싱크의 컨슈머 그룹은 "
        "새 스냅샷보다 오래된 변경을 다시 적재한 테이블 위에 다시 적용하고, 그동안 소스는 이전 정지 지점과 "
        "새 스냅샷 사이의 변경을 건너뜁니다",
        "ja": "Stop CDC (およびロールバックされた Start) は sink をドレインせずに削除するため、その sink の"
        "コンシューマグループは新しいスナップショットより古い変更を再ロードしたテーブルの上に再適用し、その"
        "一方でソースは以前の停止位置と新しいスナップショットの間の変更をスキップします",
    },
    "route": {
        "en": "**Delete CDC infrastructure** and deploy it again (the new stack starts with "
        "empty topics), then **Re-run Full Load** choosing **Drop & reload** (on PostgreSQL that "
        "creates the replication slot again at its own snapshot point), then **Start CDC**",
        "ko": "**Delete CDC infrastructure** 후 다시 배포(새 스택은 빈 토픽으로 시작), 그다음 **Re-run Full "
        "Load**를 **Drop & reload**로(PostgreSQL에서는 복제 슬롯을 자신의 스냅샷 지점에 다시 만듦), 그다음 "
        "**Start CDC**",
        "ja": "**Delete CDC infrastructure** して再デプロイ (新しいスタックは空のトピックで始まります)、次に "
        "**Drop & reload** で **Re-run Full Load** (PostgreSQL ではレプリケーションスロットが自身の"
        "スナップショット位置に作り直されます)、次に **Start CDC** です",
    },
    "header-run": {
        "en": "**The page header's Run is gated the same way**, because it takes a new start "
        "point just like Start / Re-run Full Load. On a stack that has streamed it starts "
        "nothing: a warning toast names the same route, no job is created, no watermark is "
        "captured (on PostgreSQL the replication slot is not replaced), and the step status and "
        "the previous Full Load record stay as they were.",
        "ko": "**페이지 헤더의 Run도 같은 방식으로 막힙니다.** Start / Re-run Full Load와 똑같이 새 시작 "
        "지점을 가져가기 때문입니다. 스트리밍한 적 있는 스택에서는 아무것도 시작하지 않습니다: 경고 토스트가 "
        "같은 경로를 알려 주고, 작업은 만들어지지 않고, 워터마크도 캡처되지 않으며(PostgreSQL에서는 복제 "
        "슬롯이 교체되지 않음), 단계 상태와 이전 Full Load 기록도 그대로 남습니다.",
        "ja": "**ページヘッダーの Run も同じように制限されます。** Start / Re-run Full Load とまったく同じく"
        "新しい開始位置を取るためです。ストリーミングしたことのあるスタックでは何も開始しません: 警告の"
        "トーストが同じ方法を示し、ジョブは作成されず、ウォーターマークもキャプチャされず (PostgreSQL では"
        "レプリケーションスロットも置き換えられません)、ステップのステータスと以前の Full Load の記録も"
        "そのまま残ります。",
    },
    "unread": {
        "en": "a new start point **waits** rather than guessing: the notice says so and points "
        "at **Re-check CDC state** on the CDC step",
        "ko": "새 시작 지점은 추측하지 않고 **기다립니다**: 알림이 그렇게 말하고 CDC 단계의 "
        "**Re-check CDC state**를 알려 줍니다",
        "ja": "新しい開始位置は推測せずに **待ちます**: 通知がそう述べ、CDC ステップの "
        "**Re-check CDC state** を示します",
    },
    # G25 round 7 moved this pin with the text: a restart after Stop CDC is gapless only while
    # the CDC topics retain the sink's unapplied changes (rule D), and switching to Full load
    # only or CDC only is no way around the gate (Start CDC's own refusal, test_g25_docs_r7).
    # G132 moved it again: they are not gated by the NEW START POINT, but while CDC is live they
    # are disabled like every Full Load (G129), as the entry above says -- a bare "Not gated:"
    # right under that entry contradicted it.
    "not-gated": {
        "en": "**Not gated by the new start point** (while CDC is live they are disabled too, as "
        "above): **Retry unfinished tables**, **Reload specific tables**, a per-table **Reload** "
        "and **Change & reload** all keep the **original** start point, so CDC continues from "
        "where it stopped — like any restart after Stop CDC, gapless only while the CDC topics "
        "still retain the changes the sink had not applied",
        "ko": "**새 시작 지점으로 막히지 않는 것**(CDC가 살아 있는 동안에는 위와 같이 이들도 비활성화됨): "
        "**Retry unfinished tables**, **Reload specific tables**, 테이블별 **Reload**, **Change & "
        "reload**는 모두 **원래** 시작 지점을 유지하므로 CDC가 멈춘 곳에서 이어 가고 — Stop CDC 후의 모든 "
        "재시작처럼, 싱크가 적용하지 않은 변경을 CDC 토픽이 아직 보존하는 동안만 빈틈이 없음",
        "ja": "**新しい開始位置では制限されないもの** (CDC が動いている間は、上のとおりこれらも無効になり"
        "ます): **Retry unfinished tables**、**Reload specific tables**、テーブルごとの **Reload**、"
        "**Change & reload** はいずれも **元の** 開始位置を維持するため、CDC は止まったところから続き — "
        "Stop CDC 後のあらゆる再起動と同じく、sink が適用していなかった変更を CDC トピックがまだ保持して"
        "いる間だけギャップがありません",
    },
    "no-detour": {
        "en": "and the reloaded tables converge. The Full Load step does not gate a **Full load "
        "only** or **CDC only** migration, but switching to one is no way around this gate (next "
        "entry).",
        "ko": "다시 적재한 테이블은 수렴합니다. Full Load 단계는 **Full load only**와 **CDC only** "
        "마이그레이션을 막지 않지만, 그 유형으로 바꿔도 이 게이트를 우회할 수는 없습니다(다음 항목).",
        "ja": "再ロードしたテーブルは収束します。Full Load ステップは **Full load only** と **CDC only** "
        "のマイグレーションを制限しませんが、それらに切り替えてもこのゲートは回避できません (次の項目)。",
    },
    "engine": {
        "en": "If the stack starts streaming between the click and the job starting, the engine "
        "refuses on the job thread before it reads or writes anything: the run is recorded as "
        "failed with *FullLoadRefusedError* and the same one-line reason.",
        "ko": "클릭과 작업 시작 사이에 스택이 스트리밍을 시작하면, 엔진이 작업 스레드에서 아무것도 읽거나 "
        "쓰기 전에 거부합니다: 그 실행은 *FullLoadRefusedError*와 같은 한 줄 이유로 실패로 기록됩니다.",
        "ja": "クリックとジョブの開始の間にスタックがストリーミングを始めた場合は、エンジンがジョブ"
        "スレッドで何も読み書きする前に拒否します: その実行は *FullLoadRefusedError* と同じ 1 行の理由で"
        "失敗として記録されます。",
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_3_states_the_new_start_point_gate_as_the_code_applies_it(lang: str) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._full_load_engine import (
        NEW_START_POINT_HEADER,
        NEW_START_POINT_UNREAD_HEADER,
        new_start_point_block,
        new_start_point_refusal,
        run_full_load,
    )

    # The code first: the gate holds on a Full load + CDC stack that has streamed, for either
    # engine, and on nothing else; its one line fits a job error's first line.
    for engine in (SourceType.MYSQL, SourceType.POSTGRES):
        block = new_start_point_block(_state(streamed=True), engine)
        assert block is not None and block[0] == NEW_START_POINT_HEADER
        assert len(block[2]) <= 300 and block[2].startswith("This CDC stack has streamed")
        assert block[2].index("Delete CDC infrastructure and deploy it again") < block[2].index(
            "Re-run Full Load"
        )
        assert new_start_point_block(_state(), engine) is None  # never streamed
        assert new_start_point_block(_state(streamed=True, cdc_only=True), engine) is None
        unread = new_start_point_block(_state(checked=False), engine)
        assert unread is not None and unread[0] == NEW_START_POINT_UNREAD_HEADER
        assert "Re-check CDC state" in unread[2]
        failed = new_start_point_block(_state(checked=False, probe_error="boom"), engine)
        assert failed[2].startswith("The CDC state could not be read")
    # ...and every new-start-point run goes through it: the page header's Run reaches runner()
    # without the step's button or the Start dialog, which checks before anything changes and
    # hands the engine the same gate for the job thread.
    runner = _runner_source()
    assert "_start_point_block = new_start_point_block(migration_state, _engine)" in runner
    assert 'ui.notify(_start_point_block[2], type="warning"' in runner
    assert "start_point_refusal=lambda: new_start_point_refusal(migration_state, _engine)" in (
        runner
    )
    assert runner.index("new_start_point_block") < runner.index("session.has_source()")
    assert "start_point_refusal" in inspect.signature(run_full_load).parameters
    assert new_start_point_refusal(_state(streamed=True), SourceType.MYSQL).startswith(
        "This CDC stack has streamed"
    )
    assert new_start_point_refusal(_state(), SourceType.MYSQL) is None

    page = _page(lang, _PAGE_03)
    for key in (
        "actions", "why", "route", "header-run", "unread", "not-gated", "no-detour", "engine"
    ):
        assert _has(lang, page, _GATE[key][lang]), (lang, key)


# -- (A) Validation's recovery runbook ------------------------------------------------------

_RUNBOOK = {
    "steps": {
        "en": "that backfill takes a **new start point**, so the steps put new CDC topics "
        "first: Stop CDC if it is running → **Delete CDC infrastructure** and deploy it again → "
        "**Re-run Full Load** (**Re-run all tables** when some failed) choosing **Drop & "
        "reload** → **Start CDC**, which starts from the new Full Load's start point → "
        "**Re-run validation**",
        "ko": "그 백필이 **새 시작 지점**을 가져가므로, 단계가 새 CDC 토픽을 먼저 둡니다: CDC가 실행 중이면 "
        "Stop CDC → **Delete CDC infrastructure** 후 다시 배포 → **Re-run Full Load**(일부가 실패했다면 "
        "**Re-run all tables**)를 **Drop & reload**로 → **Start CDC**(새 Full Load의 시작 지점에서 시작) → "
        "**Re-run validation**",
        "ja": "そのバックフィルが **新しい開始位置** を取るため、手順は新しい CDC トピックを先に置きます: "
        "CDC が動いていれば Stop CDC → **Delete CDC infrastructure** して再デプロイ → **Drop & reload** で "
        "**Re-run Full Load** (一部が失敗していれば **Re-run all tables**) → **Start CDC** (新しい Full "
        "Load の開始位置から始まります) → **Re-run validation**",
    },
    "not-gapless": {
        "en": "a resume after a Full Load that took a new start point is **not** gapless, which "
        "is why the new topics come first",
        "ko": "새 시작 지점을 가져간 Full Load 뒤의 재개는 **빈틈 없지 않으며**, 그래서 새 토픽이 먼저 "
        "옵니다",
        "ja": "新しい開始位置を取った Full Load の後の再開には **ギャップがない保証がなく**、だからこそ"
        "新しいトピックが先に来ます",
    },
    "one-table": {
        "en": "Reloading **one table** is different: **Drop & reload** of specific tables keeps "
        "the **original** start point, so the per-table way forward in 5.5–5.7 is never gated.",
        "ko": "**한 테이블**만 다시 적재하는 것은 다릅니다: 특정 테이블의 **Drop & reload**는 **원래** 시작 "
        "지점을 유지하므로, 5.5~5.7의 테이블별 해결 경로는 전혀 막히지 않습니다.",
        "ja": "**1 つのテーブル** だけを再ロードするのは別です: 特定のテーブルの **Drop & reload** は "
        "**元の** 開始位置を維持するため、5.5〜5.7 のテーブルごとの対処手順はまったく制限されません。",
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_validations_runbook_puts_new_topics_before_the_re_run(lang: str) -> None:
    from dsql_migrator.ui.validation import _cdc_stack_streamed, _cdc_state_unread

    # The code first: the runbook branch is chosen by the stack record, whatever the type
    # (G25 round 7: a CDC only stack streams too), and by a CDC state not read yet -- held as
    # the Full Load step's gate holds it -- and its steps are Stop CDC -> Delete + deploy ->
    # the re-run -> Start CDC -> re-run validation.
    assert _cdc_stack_streamed(_state(streamed=True)) is True
    assert _cdc_stack_streamed(_state()) is False
    assert _cdc_stack_streamed(_state(streamed=True, cdc_only=True)) is True
    assert _cdc_state_unread(_state(checked=False)) is True  # nothing read yet
    assert _cdc_state_unread(_state()) is False
    section = _function(_VALIDATION, "_render_recovery_section")
    assert (
        "new_topics_first = not full_load_only and (cdc_stack_streamed or cdc_state_unread)"
        in section
    )
    branch = section[section.index("elif new_topics_first:") : section.index("        else:")]
    order = [
        branch.index(word)
        for word in (
            "click Stop CDC first",
            "click Delete CDC infrastructure",
            "click Re-run Full Load",
            "'Drop & reload'",
            "Start CDC",
            "Re-run validation",
        )
    ]
    assert order == sorted(order), branch
    assert "resumes gaplessly" not in section

    page = _page(lang, _PAGE_05)
    for key in ("steps", "not-gapless", "one-table"):
        assert _has(lang, page, _RUNBOOK[key][lang]), (lang, key)


# -- (B) the CDC monitor's invalidated-slot notice ------------------------------------------

_LOST_SLOT = {
    "en": "The CDC monitor's slot panel reports the same slot as *Replication slot invalidated* "
    "and names that route in full: Delete CDC infrastructure and deploy it again, then Re-run "
    "Full Load with **Drop & reload**, then Start CDC; with CDC only, Delete CDC infrastructure "
    "and deploy it again, then Start CDC on the new stack, which snapshots every selected table "
    "(that type has no Full Load step to re-run).",
    "ko": "CDC 모니터의 슬롯 패널도 같은 슬롯을 *Replication slot invalidated*로 보고하며 그 경로를 모두 "
    "알려 줍니다: Delete CDC infrastructure 후 다시 배포, 그다음 Re-run Full Load를 **Drop & reload**로, "
    "그다음 Start CDC. CDC only에서는 Delete CDC infrastructure 후 다시 배포한 뒤 새 스택에서 Start CDC 하며, "
    "그 Start가 선택한 모든 테이블을 스냅샷합니다(그 유형에는 다시 실행할 Full Load 단계가 없습니다).",
    "ja": "CDC モニターのスロットパネルも同じスロットを *Replication slot invalidated* として報告し、その"
    "方法をすべて示します: Delete CDC infrastructure して再デプロイ、次に **Drop & reload** で Re-run Full "
    "Load、次に Start CDC。CDC only では Delete CDC infrastructure して再デプロイしてから新しいスタックで "
    "Start CDC し、その Start が選択したすべてのテーブルをスナップショットします (その種類には再実行する "
    "Full Load ステップがありません)。",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_invalidated_slot_notice_names_the_route_rules_route(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.core.cdc_postgres import SlotHealth, classify_slot_health

    # The code first: the classification gives no route of its own, and the panel appends the
    # route rule's -- new topics first, and no Full Load step with CDC only.
    tone, headline, detail = classify_slot_health(
        SlotHealth(slot_name="s", exists=True, active=False, wal_status="lost")
    )
    assert (tone, headline) == ("error", "Replication slot invalidated")
    assert "Re-run Full Load" not in detail and "Full Load" not in detail
    panel = _function(_CDC_MONITORING, "_render_cdc_slot_health")
    assert "pg_cdc_route(\n                SLOT_MISSING_HEADER,\n                started=True," in (
        panel
    )
    assert "is MigrationType.CDC_ONLY," in panel
    full_load = pg.pg_cdc_route(pg.SLOT_MISSING_HEADER, started=True, cdc_only=False)
    cdc_only = pg.pg_cdc_route(pg.SLOT_MISSING_HEADER, started=True, cdc_only=True)
    assert full_load.index("Delete CDC infrastructure") < full_load.index("Re-run Full Load")
    assert "Re-run Full Load" not in cdc_only and "snapshots every selected table" in cdc_only

    assert _has(lang, _page(lang, _PAGE["04"]), _LOST_SLOT[lang]), lang


# -- (C) the narrowed publish list: its own fix first, then the route ----------------------

_PUBLISH_LIST = {
    "fix-first": {
        "en": "that block names its own fix first: as the publication's owner, run `ALTER "
        "PUBLICATION … SET (publish = 'insert, update, delete')` on the source",
        "ko": "그 차단은 자체 해결책을 먼저 알려 줍니다: publication 소유자로 소스에서 `ALTER PUBLICATION … "
        "SET (publish = 'insert, update, delete')`를 실행하세요",
        "ja": "そのブロックは自身の解決策を先に示します: publication の所有者として、ソースで "
        "`ALTER PUBLICATION … SET (publish = 'insert, update, delete')` を実行してください",
    },
    "why-not-enough": {
        "en": "pgoutput decides what to publish from the publication as it stood when each "
        "change committed, so setting the list back does not bring back the changes committed "
        "while it was narrowed, and a Start that continues from the slot would still skip them",
        "ko": "pgoutput은 각 변경이 커밋된 시점의 publication을 보고 무엇을 publish할지 결정하므로, 목록을 "
        "되돌려도 좁혀져 있던 동안 커밋된 변경은 돌아오지 않고, 슬롯에서 이어 가는 Start는 그 변경을 여전히 "
        "건너뜁니다",
        "ja": "pgoutput は各変更がコミットされた時点の publication を見て何を publish するかを決めるため、"
        "リストを戻しても、狭められていた間にコミットされた変更は戻らず、スロットから続ける Start はそれらの"
        "変更を依然としてスキップします",
    },
    "route": {
        "en": "The route follows that fix — on a stack whose connectors were never started, "
        "Re-run Full Load with **Drop & reload**, then Start CDC (with CDC only, switch the "
        "migration type to Full load + CDC first); on one whose connectors were started before, "
        "new topics first, as above.",
        "ko": "그 해결책 다음에 경로가 옵니다 — 커넥터를 시작한 적 없는 스택에서는 Re-run Full Load를 "
        "**Drop & reload**로 실행한 뒤 Start CDC(CDC only에서는 먼저 마이그레이션 유형을 Full load + CDC로 "
        "바꿈), 커넥터를 전에 시작한 스택에서는 위와 같이 새 토픽 먼저.",
        "ja": "その解決策の後に方法が続きます — コネクタを開始したことのないスタックでは **Drop & reload** で "
        "Re-run Full Load してから Start CDC (CDC only では、先にマイグレーションの種類を Full load + CDC に"
        "切り替えます)、以前にコネクタを開始したスタックでは上記のとおり新しいトピックが先です。",
    },
    "no-remedy": {
        "en": "because a re-snapshot — like a Full Load re-run — reuses the publication exactly "
        "as it is",
        "ko": "다시 스냅샷해도 (Full Load 재실행과 마찬가지로) publication을 그대로 재사용하므로",
        "ja": "再スナップショットも (Full Load の再実行と同様に) publication をそのまま再利用するため",
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_narrowed_publish_list_names_its_own_fix_then_the_route(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg

    # The code first: the block's own fix, the reason it is not enough, no "Start CDC again",
    # and the route rule's route after it (a Full Load re-run, or new topics first).
    narrowed = replace(_slot_objects(), publication_publishes_all_dml=False)
    block = pg.pg_replication_objects_blocker(
        narrowed, ["app.t"], resumes_from_slot=True, start_lsn="0/2000000"
    )
    assert block[0] == pg.PUBLISH_LIST_HEADER
    assert f"{pg.publish_list_fix('p')} first." in block[1]
    assert "pgoutput decides what to publish from the publication as it stood when each change "
    assert "then Start CDC again" not in block[1]
    assert pg.PUBLISH_LIST_HEADER in pg.BLOCKS_WITH_THEIR_OWN_FIX
    never = pg.pg_cdc_route(pg.PUBLISH_LIST_HEADER, started=False, cdc_only=False)
    assert never.startswith("Then Re-run Full Load") and "re-snapshot" not in never.lower()
    only = pg.pg_cdc_route(pg.PUBLISH_LIST_HEADER, started=False, cdc_only=True)
    assert only.startswith("Then switch the migration type to Full load + CDC, then Re-run Full")
    started = pg.pg_cdc_route(pg.PUBLISH_LIST_HEADER, started=True, cdc_only=False)
    assert "After that fix, the gapless route" in started
    assert started.index("Delete CDC infrastructure") < started.index("Re-run Full Load")

    page = _page(lang, _PAGE["04"])
    for key in ("fix-first", "why-not-enough", "route", "no-remedy"):
        assert _has(lang, page, _PUBLISH_LIST[key][lang]), (lang, key)


# -- the Stop CDC dialog's retained-topics condition, both engines --------------------------

_STOP = {
    "mysql-restart": {
        "en": "Stop CDC removes both connectors without draining the sink, so the changes the "
        "sink had not applied when CDC stopped reach Aurora DSQL on that restart only from the "
        "CDC topics, re-applied from the sink's last commit (idempotent, so nothing is "
        "duplicated): the restart is gapless only while the topics still retain them — their "
        "retention period on MSK. The *Ready to resume CDC* card and the *Resuming from the "
        "last streamed position* start-point card state that condition; neither promises *no "
        "gap* or that nothing is re-applied.",
        "ko": "Stop CDC는 두 커넥터를 싱크를 드레인하지 않고 제거하므로, CDC가 멈출 때 싱크가 적용하지 않은 "
        "변경은 그 재시작에서 CDC 토픽에서만 Aurora DSQL에 도달하며(싱크의 마지막 커밋부터 다시 적용되므로 "
        "중복은 생기지 않습니다): 그 재시작에 빈틈이 없는 것은 토픽이 그 변경을 아직 보존하는 동안 — MSK에서의 "
        "보존 기간 — 뿐입니다. *Ready to resume CDC* 카드와 *Resuming from the last streamed position* 시작 "
        "지점 카드가 그 조건을 말하며, 둘 중 어느 것도 *빈틈 없음*이나 다시 적용되는 것이 없다고 약속하지 "
        "않습니다.",
        "ja": "Stop CDC は sink をドレインせずに両方のコネクタを削除するため、CDC が停止した時点で sink が"
        "適用していなかった変更は、その再起動では CDC トピックからしか Aurora DSQL に届きません (sink の"
        "最後のコミットから再適用されるため、重複は生じません): その再起動にギャップがないのは、トピックが"
        "まだその変更を保持している間 (MSK での保持期間) だけです。*Ready to resume CDC* のカードと "
        "*Resuming from the last streamed position* の開始位置のカードがその条件を述べ、どちらも "
        "*ギャップなし* や、再適用されるものがないとは約束しません。",
    },
    "dialog": {
        "en": "The **Stop CDC** dialog states the same condition before you stop — for either "
        "source — and on PostgreSQL it names that route for after the retention period.",
        "ko": "**Stop CDC** 대화 상자도 멈추기 전에 같은 조건을 두 소스 모두에 말해 주고, PostgreSQL에서는 "
        "그 보존 기간이 지난 뒤의 경로까지 알려 줍니다.",
        "ja": "**Stop CDC** のダイアログも、停止する前に同じ条件をどちらのソースについても述べ、PostgreSQL "
        "ではその保持期間が過ぎた後の方法まで示します。",
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_stop_dialog_and_the_mysql_cards_state_the_topics_condition(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: one condition for both engines in the Stop dialog (PostgreSQL also names
    # the route), the same condition on the MySQL resume cards, and no "no gap" claim left.
    dialog = inspect.getsource(cu._open_cdc_stop_dialog)
    assert dialog.count("_STOP_TOPICS_CONDITION") == 2  # the PostgreSQL branch and MySQL's
    assert "pg_cdc_route(" in dialog and "started=True," in dialog
    assert "no gap" not in dialog and "nothing re-applied" not in dialog
    assert cu._STOP_TOPICS_CONDITION.endswith(
        "so a restart is gapless only while the topics still retain them"
    )
    assert cu._MYSQL_TOPICS_CONDITION == (
        "Changes the sink had not applied when CDC stopped are applied from the CDC topics, so "
        "this is gapless only while the topics still retain them (their retention period on "
        "MSK)."
    )
    for name in ("_render_cdc_start_button", "_render_cdc_start_point_card"):
        assert "_MYSQL_TOPICS_CONDITION" in _function(_CDC_UI, name), name
    rendered = _CDC_UI.read_text(encoding="utf-8")
    for gone in ("no gap, nothing re-applied", "with no gap, and no Full Load"):
        assert gone not in rendered, gone

    page = _page(lang, _PAGE["04"])
    for key in ("mysql-restart", "dialog"):
        assert _has(lang, page, _STOP[key][lang]), (lang, key)


# -- the inheritance block's two fixes ------------------------------------------------------

_INHERITANCE = {
    "untick": {
        "en": "*…or untick `<the parent>`: each child table migrates as a table of its own* "
        "appears only while the table picker can still change the set. Once a Start has been "
        "submitted the set is locked, so the Start CDC job's own refusal never names it and only "
        "the Start from a Full Load's snapshot point is left.",
        "ko": "*…or untick `<부모 테이블>`: each child table migrates as a table of its own*은 테이블 "
        "선택기가 아직 집합을 바꿀 수 있는 동안에만 나옵니다. Start를 한 번 보내면 집합이 잠기므로 Start CDC "
        "작업 자체의 거부는 그 방법을 알려 주지 않고, Full Load의 스냅샷 지점에서 시작하는 방법만 남습니다.",
        "ja": "*…or untick `<親テーブル>`: each child table migrates as a table of its own* が現れるのは、"
        "テーブルのピッカーがまだ集合を変更できる間だけです。Start を一度送ると集合はロックされるため、"
        "Start CDC のジョブ自身の拒否はその方法を示さず、Full Load のスナップショット位置から開始する方法"
        "だけが残ります。",
    },
    "resume-control": {
        "en": "the block names the way back: **Resume from the Full Load's slot instead**, a "
        "control on the CDC start point card that sets the start point back to Automatic — so a "
        "recorded re-snapshot never loops on the same block",
        "ko": "그 차단은 돌아가는 길도 알려 줍니다: 시작 지점을 다시 Automatic으로 돌리는 CDC 시작 지점 "
        "카드의 **Resume from the Full Load's slot instead** 컨트롤입니다 — 그래서 기록된 재스냅샷이 같은 "
        "차단을 되풀이하지 않습니다",
        "ja": "そのブロックは戻る方法も示します: 開始位置を Automatic に戻す、CDC の開始位置のカードの "
        "**Resume from the Full Load's slot instead** というコントロールです — そのため、記録された"
        "再スナップショットが同じブロックを繰り返すことはありません",
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_inheritance_block_names_only_the_fixes_the_ui_can_carry_out(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the untick alternative is the block's `untick` flag, the Start dialog
    # passes the table picker's own lock and the Start CDC job passes False.
    parents = [("app.orders", 2, ["app.a", "app.b"])]
    assert pg._inheritance_snapshot_block(parents, untick=True)[1].endswith(
        "-- or untick app.orders: each child table migrates as a table of its own."
    )
    assert pg._inheritance_snapshot_block(parents, untick=False)[1].endswith(
        "its CDC starts without a snapshot."
    )
    assert "untick" not in pg._inheritance_snapshot_block(parents, untick=False)[1]
    assert "_pg_untick = _cdc_source_type(session) is SourceType.POSTGRES and _table_set_open(" in (
        inspect.getsource(cu._open_cdc_start_dialog)
    )
    assert "untick=False," in _function(_CDC_UI, "_start_cdc_deploy")
    assert (
        _function(_CDC_UI, "_table_set_open").count("return False")  # a lock it cannot read
        == 1
    )
    # ...and the way back to the Full Load's slot is a real control, named by the block.
    assert cu.PG_RESUME_FROM_SLOT_ACTION == "Resume from the Full Load's slot instead"
    assert cu.PG_RESUME_FROM_SLOT_ACTION in cu.PG_RESUME_FROM_SLOT_HINT
    assert "PG_RESUME_FROM_SLOT_HINT" in _function(_CDC_UI, "_pg_objects_gate_block")
    card = _function(_CDC_UI, "_render_cdc_start_point_card")
    assert 'if resnapshot_reason == "manual":' in card  # only over a slot the load recorded
    control = card[card.index('if resnapshot_reason == "manual":') :]
    assert control.index("PG_RESUME_FROM_SLOT_ACTION,") < control.index('_on_mode("auto")')
    assert cu.PG_RESUME_FROM_SLOT_ACTION in cu.pg_resnapshot_notice("manual")[1]

    page = _page(lang, _PAGE["04"])
    for key in ("untick", "resume-control"):
        assert _has(lang, page, _INHERITANCE[key][lang]), (lang, key)


# -- a same-named slot CDC cannot use: its own fix first ------------------------------------

_SAME_NAMED_SLOT = {
    "en": "So where a slot of that name exists but CDC cannot use it — another database's, or "
    "not a logical `pgoutput` slot — every block names that slot's own fix first (drop it, or "
    "give this migration its own CDC stack name), ahead of any Full Load re-run, which would "
    "refuse such a slot before any write; the block for a missing slot says the same, and such a "
    "slot is reported before a captured table's inheritance children, whose own fix is that very "
    "re-run.",
    "ko": "그래서 그 이름의 슬롯이 있지만 CDC가 쓸 수 없을 때 — 다른 데이터베이스의 것이거나 논리 "
    "`pgoutput` 슬롯이 아닐 때 — 모든 차단은 그 슬롯의 자체 해결책(그 슬롯을 삭제하거나 이 마이그레이션에 "
    "고유한 CDC 스택 이름을 사용)을 Full Load 재실행보다 먼저 알려 줍니다. 그런 슬롯은 Full Load 재실행도 "
    "쓰기 전에 거부하기 때문입니다. 슬롯이 없다는 차단도 같은 말을 하며, 그런 슬롯은 캡처 대상 테이블의 상속 "
    "자식 테이블보다 먼저 보고됩니다. 상속 차단의 자체 해결책이 바로 그 재실행이기 때문입니다.",
    "ja": "そのため、その名前のスロットがあっても CDC が使用できない場合 — 別のデータベースのものか、論理 "
    "`pgoutput` スロットでない場合 — すべてのブロックは、そのスロット自身の解決策 (そのスロットを削除するか、"
    "このマイグレーションに独自の CDC スタック名を使う) を Full Load の再実行より先に示します。そのような"
    "スロットは Full Load の再実行も書き込みの前に拒否するためです。スロットがないというブロックも同じことを"
    "述べ、そのようなスロットはキャプチャ対象テーブルの継承の子テーブルより先に報告されます。継承のブロック"
    "自身の解決策がまさにその再実行だからです。",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_same_named_slot_it_cannot_use_is_named_before_an_inheritance_parent(lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg

    # The code first: with an inheritance parent AND an unusable slot of that name, the slot is
    # reported first (its fix comes before any Full Load re-run), and the missing-slot block
    # names that slot too.
    objects = replace(
        _slot_objects(),
        slot_usable=False,
        inheritance_children=[("app.t", 2, ["app.a", "app.b"])],
    )
    unusable = pg.pg_replication_objects_blocker(
        objects, ["app.t"], resumes_from_slot=False,
        connector_autocreates_publication=True, snapshots=True,
    )
    assert unusable[0] == pg.SLOT_UNUSABLE_HEADER
    assert unusable[1].endswith(
        "Drop it, or use a different CDC stack name so the derived slot name differs."
    )
    missing = pg.pg_replication_objects_blocker(
        objects, ["app.t"], resumes_from_slot=True, snapshots=True
    )
    assert missing[0] == pg.SLOT_MISSING_HEADER
    assert missing[1].endswith(
        "drop it or give this migration its own CDC stack name first -- a Full Load re-run "
        "refuses such a slot before any write."
    )

    assert _has(lang, _page(lang, _PAGE["04"]), _SAME_NAMED_SLOT[lang]), lang


# -- stopped, then a Start rolled back: the retention bound counts from the Stop ------------

_STOPPED_ROLLBACK = {
    "en": "If CDC had been **stopped** before that rolled-back Start, the cards say so and count "
    "that period from the **Stop** instead (*since CDC was stopped*): the changes the sink had "
    "not applied when CDC stopped are the oldest in the topics, so they expire first.",
    "ko": "롤백된 그 Start 전에 CDC를 **멈췄**다면 카드가 그 사실을 말하고 그 기간을 그 Start가 아니라 "
    "**Stop** 시점부터 셉니다(*since CDC was stopped*): CDC가 멈출 때 싱크가 적용하지 않은 변경이 토픽에서 "
    "가장 오래된 것이므로 먼저 만료됩니다.",
    "ja": "ロールバックされたその Start の前に CDC を **停止** していた場合、カードはその事実を述べ、その"
    "期間をその Start ではなく **Stop** の時点から数えます (*since CDC was stopped*): CDC が停止した時点で "
    "sink が適用していなかった変更がトピックの中で最も古く、先に期限切れになるためです。",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_stopped_stacks_rolled_back_start_counts_retention_from_the_stop(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: with ``stopped`` the rolled-back copy keeps the Stop and counts from it;
    # without it, from that Start.
    stopped = cu._pg_resume_continues("s", snapshot_never=True, rolled_back=True, stopped=True)
    assert "CDC had been stopped before that Start" in stopped
    assert "retention period on MSK has passed since CDC was stopped" in stopped
    first = cu._pg_resume_continues("s", snapshot_never=True, rolled_back=True)
    assert "retention period on MSK has passed since that Start" in first
    assert "stopped" not in first.replace("stopped streaming", "")

    assert _has(lang, _page(lang, _PAGE["04"]), _STOPPED_ROLLBACK[lang]), lang


# -- nothing promises a gapless resume after a Full Load that took a new start point --------

_STALE_RESUME = {
    "en": (
        "a Full Load run again since then re-creates the slot at its own start point",
        "or the Full Load's start point if nothing has streamed since",
    ),
    "ko": (
        "그 뒤 다시 실행한 Full Load는 슬롯을 자신의 시작 지점에 다시 만들기 때문입니다",
        "그 뒤 아무것도 스트리밍하지 않았으면 Full Load의 시작 지점",
    ),
    "ja": (
        "その後に再実行した Full Load はスロットを自身の開始位置に作り直すためです",
        "その後何もストリーミングしていなければ Full Load の開始位置",
    ),
}
_NEVER_STREAMED = {
    "en": "or the Full Load's start point if its source connector never streamed",
    "ko": "그 스택의 소스 커넥터가 한 번도 스트리밍하지 않았으면 Full Load의 시작 지점",
    "ja": "そのスタックの source connector が一度もストリーミングしていなければ Full Load の開始位置",
}


@pytest.mark.parametrize("lang", LANGS)
def test_no_page_promises_a_resume_after_a_new_start_point_full_load(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the stopped stack's card describes a resume only from where streaming
    # stopped, or from the Full Load's start point when the source connector never streamed --
    # never from a slot a Full Load re-created since (that Full Load is refused).
    card = cu._pg_resume_continues("s", snapshot_never=True)
    assert "or from the Full Load's start point if its source connector never streamed." in card
    assert "since that load created it" not in card and "recreated the slot" not in card

    assert _has(lang, _page(lang, _PAGE["04"]), _NEVER_STREAMED[lang]), lang
    assert not _stale(lang, _STALE_RESUME[lang]), lang


# -- chapter 6: every re-run it prescribes for a newer start point waits for new topics ----

# G25 round 7 moved this pin with the text: a newer Manual point is a new start point too, and on
# a MySQL source Start CDC refuses a newer one whatever the migration type (decision A2).
_CH06 = {
    "en": "Wherever this chapter sends you to **Re-run Full Load with \"Drop & reload\"** (or "
    "**Re-run all tables**), or to a newer **Manual** start point, to give CDC a newer start "
    "point, that start point is **refused** while this migration's CDC stack has streamed — "
    "stopped after a Start, its connectors in place, or a Start CloudFormation rolled back. The "
    "Full Load step refuses the re-run (Full load + CDC), and on a MySQL source Start CDC "
    "refuses a start point newer than the one the stack last started from, whatever the "
    "migration type, so a detour through **Full load only** does not get around it. **Delete "
    "CDC infrastructure** and deploy it again first, then the re-run (or the Manual point), "
    "then **Start CDC**",
    "ko": "이 장에서 CDC에 더 새로운 시작 지점을 주기 위해 **Re-run Full Load를 \"Drop & reload\"로**(또는 "
    "**Re-run all tables**) 실행하거나 더 새 **Manual** 시작 지점을 쓰라고 안내하는 곳은 모두, 이 "
    "마이그레이션의 CDC 스택이 스트리밍한 적 있는 동안 — Start 뒤에 멈췄거나, 커넥터가 그대로 있거나, "
    "CloudFormation이 Start를 롤백한 동안 — 그 시작 지점이 **거부됩니다**. Full Load 단계가 재실행을 "
    "거부하고(Full load + CDC), MySQL 소스에서는 Start CDC가 마이그레이션 유형과 상관없이 스택이 "
    "마지막으로 시작한 지점보다 새 시작 지점을 거부하므로, **Full load only**를 거쳐 돌아와도 우회되지 "
    "않습니다. 먼저 **Delete CDC infrastructure** 후 다시 배포하고, 그다음 재실행(또는 Manual 지점), "
    "그다음 **Start CDC**",
    "ja": "この章が CDC により新しい開始位置を与えるために **Drop & reload で Re-run Full Load**(または "
    "**Re-run all tables**)、またはより新しい **Manual** の開始位置を案内している箇所はすべて、この"
    "マイグレーションの CDC スタックがストリーミングしたことのある間 — Start の後に停止した、コネクタが"
    "そのまま残っている、または CloudFormation が Start をロールバックした間 — その開始位置は "
    "**拒否されます**。Full Load ステップがその再実行を拒否し (Full load + CDC)、MySQL ソースでは "
    "Start CDC がマイグレーションの種類にかかわらず、スタックが最後に開始した位置より新しい開始位置を"
    "拒否するため、**Full load only** を経由しても回避できません。先に **Delete CDC infrastructure** "
    "して再デプロイし、次に再実行 (または Manual の位置)、次に **Start CDC** です",
}
_CH04_BINLOG = {
    "en": "on a stack that has streamed, after Delete CDC infrastructure and a new deploy, "
    "because the Full Load step refuses a new start point until then",
    "ko": "스트리밍한 적 있는 스택에서는 Delete CDC infrastructure 후 다시 배포한 뒤에 실행합니다. 그때까지 "
    "Full Load 단계가 새 시작 지점을 거부하기 때문입니다",
    "ja": "ストリーミングしたことのあるスタックでは、Delete CDC infrastructure して再デプロイした後に"
    "実行します。それまでは Full Load ステップが新しい開始位置を拒否するためです",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_newer_start_point_waits_for_new_topics_wherever_the_manual_prescribes_one(
    lang: str,
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._full_load_engine import new_start_point_block

    # The code first: the gate is engine-independent, so a MySQL start point moved by a Full
    # Load re-run (chapter 6's route) is refused on a stack that has streamed just the same.
    assert new_start_point_block(_state(streamed=True), SourceType.MYSQL) is not None
    assert new_start_point_block(_state(), SourceType.MYSQL) is None

    assert _has(lang, _page(lang, _PAGE["06"]), _CH06[lang]), lang
    assert _has(lang, _page(lang, _PAGE["04"]), _CH04_BINLOG[lang]), lang


# -- the gate's words are the step's own, in every language --------------------------------


@pytest.mark.parametrize("lang", LANGS)
def test_the_manuals_route_is_the_steps_own_one_line_route(lang: str) -> None:
    """Chapter 3's route and the step's one-line refusal name the same three actions in the same
    order, so the manual cannot drift from the toast the operator actually sees."""
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._full_load_engine import new_start_point_block

    line = new_start_point_block(_state(streamed=True), SourceType.POSTGRES)[2]
    steps = ("Delete CDC infrastructure", "Re-run Full Load", "Start CDC")
    assert [line.index(step) for step in steps] == sorted(line.index(s) for s in steps)

    page = _page(lang, _PAGE_03)
    route = _flat(lang, _GATE["route"][lang]).strip()
    assert route in page, lang
    at = page.index(route)
    section = page[at : at + len(route)]
    marks = [
        section.index(_flat(lang, word).strip())
        for word in ("Delete CDC infrastructure", "Re-run Full Load", "Start CDC")
    ]
    assert marks == sorted(marks), (lang, section)
