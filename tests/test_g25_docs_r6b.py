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

from tests.test_g25_docs import (
    _CDC_UI,
    _PAGE,
    LANGS,
    _F43,
    _fact,
    _flat,
    _function,
    _has,
    _page,
    _route_halves,
    _section_43,
    _steps_in_order,
)
from tests.test_g25_docs_r4 import _slot_objects, _stale

_PAGE_03 = "03-full-load.md"
_F43_ROUTE_MARK = _F43["route-mark"]
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
    # are disabled like every Full Load (G129). Chapter 3 (2026-10-25) shows the two columns as a
    # table by CDC state, so the pin is the column header and the stopped row's cell; the header
    # Run's toast / no-job / slot detail and the exception a raced run is recorded with
    # (FullLoadRefusedError) are CHANGELOG detail ("the page header's Run starts nothing" stays in
    # "actions").
    "not-gated": {
        "en": "Original start point: **Retry unfinished tables**, **Reload specific tables**, a "
        "per-table **Reload**, **Change & reload**",
        "ko": "원래 시작 지점: **Retry unfinished tables**, **Reload specific tables**, 테이블별 "
        "**Reload**, **Change & reload**",
        "ja": "元の開始位置: **Retry unfinished tables**、**Reload specific tables**、テーブルごとの "
        "**Reload**、**Change & reload**",
    },
    "converge": {
        "en": "Allowed: CDC continues from where it stopped (as above), and the reloaded tables "
        "converge.",
        "ko": "허용: CDC는 멈춘 곳에서 이어 가며(위와 같음), 다시 적재한 테이블은 수렴합니다.",
        "ja": "許可: CDC は止まったところから続き(上と同じ)、再ロードしたテーブルは収束します。",
    },
    "no-detour": {
        "en": "The Full Load step does not gate a **Full load only** or **CDC only** migration, but "
        "switching to one is no way around this gate",
        "ko": "Full Load 단계는 **Full load only**와 **CDC only** 마이그레이션을 막지 않지만, 그 "
        "유형으로 바꿔도 이 게이트를 우회할 수는 없습니다",
        "ja": "Full Load ステップは **Full load only** と **CDC only** のマイグレーションを制限しませんが、"
        "それらに切り替えてもこのゲートは回避できません",
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
    for key in ("actions", "why", "route", "unread", "not-gated", "converge", "no-detour"):
        assert _has(lang, page, _GATE[key][lang]), (lang, key)


# -- (A) Validation's recovery runbook ------------------------------------------------------

# Chapter 5 (answer-first) gives the recovery card as a table, one row per kind of migration; the
# pins are the row's steps (the order is the point), the reason new topics come first, and the
# per-table reload that is never gated.
_RUNBOOK = {
    "steps": {
        "en": "Stop CDC if it is running → **Delete CDC infrastructure** and deploy it again → "
        "**Re-run Full Load** (**Re-run all tables** when some failed) choosing **Drop & reload** "
        "→ **Start CDC**, which starts from the new Full Load's start point → **Re-run "
        "validation**",
        "ko": "CDC가 실행 중이면 Stop CDC → **Delete CDC infrastructure** 후 다시 배포 → **Re-run Full "
        "Load**(일부가 실패했다면 **Re-run all tables**)를 **Drop & reload**로 → **Start CDC**(새 Full "
        "Load의 시작 지점에서 시작) → **Re-run validation**",
        "ja": "CDC が動いていれば Stop CDC → **Delete CDC infrastructure** して再デプロイ → **Drop & "
        "reload** で **Re-run Full Load**（一部が失敗していれば **Re-run all tables**）→ **Start CDC**"
        "（新しい Full Load の開始位置から始まります）→ **Re-run validation**",
    },
    "not-gapless": {
        "en": "a resume after a Full Load that took a new start point is **not** gapless",
        "ko": "새 시작 지점을 가져간 Full Load 뒤의 재개는 **빈틈 없지 않습니다**",
        "ja": "新しい開始位置を取った Full Load の後の再開には **ギャップがない保証がありません**",
    },
    "one-table": {
        "en": "of specific tables (CDC stopped) keeps the **original** start point, so the "
        "per-table way forward in 5.5–5.7 is never gated",
        "ko": "특정 테이블의 **Drop & reload**(CDC를 멈춘 채)는 **원래** 시작 지점을 유지하므로, 5.5~5.7의 "
        "테이블별 해결 경로는 전혀 막히지 않습니다",
        "ja": "特定のテーブルの **Drop & reload**（CDC を停止して）は **元の** 開始位置を維持するため、"
        "5.5〜5.7 のテーブルごとの対処手順はまったく制限されません",
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

# §4.3 (answer-first) lists *Replication slot invalidated* with the other blocks that take "the
# route", and the route paragraph carries the CDC only variant (no Full Load step to re-run).
_LOST_SLOT_STEPS = ("Delete CDC infrastructure", "Re-run Full Load", "Start CDC")


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

    section = _section_43(lang)
    assert _has(lang, section, "*Replication slot invalidated*"), lang
    started = _route_halves(lang)[1]
    assert _steps_in_order(lang, started, *_LOST_SLOT_STEPS), lang
    assert _fact(lang, started, "cdc-only-new-stack"), lang


# -- (C) the narrowed publish list: its own fix first, then the route ----------------------

# §4.3's blocked-Start table gives this block one row: the fix first, said to be followed by the
# route (the closing paragraph); that the fix alone brings back nothing is kept as a fact. Why
# (pgoutput publishes from the publication as of each change's commit) and why no re-snapshot is
# offered (it reuses the publication as it is) are in the block itself.
_PUBLISH_LIST_HEADER = "*CDC's publication does not publish every change type*"
_PUBLISH_LIST_FIX = "`ALTER PUBLICATION … SET (publish = 'insert, update, delete')`"


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

    section = _section_43(lang)
    route_mark = _F43_ROUTE_MARK[lang]
    assert _steps_in_order(lang, section, _PUBLISH_LIST_HEADER, _PUBLISH_LIST_FIX, route_mark), lang
    assert _fact(lang, section, "narrowed-not-back"), lang


# -- the Stop CDC dialog's retained-topics condition, both engines --------------------------

# §4.3's "Starting again" says the condition once, for both engines, and names the two surfaces
# that repeat it (the Stop CDC dialog and the *Ready to resume CDC* card); the MySQL cards' own
# titles and what each promises are in the UI.
_STOP_SURFACES = ("**Stop CDC**", "*Ready to resume CDC*")


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

    section = _section_43(lang)
    assert _fact(lang, section, "gapless-while-retained"), lang
    for surface in _STOP_SURFACES:
        assert _has(lang, section, surface), (lang, surface)


# -- the inheritance block's two fixes ------------------------------------------------------

# §4.3 keeps the two UI strings a reader looks for: "untick" is offered only while the table
# picker can still change the set (the Start CDC job's own refusal never names it), and the
# start-point card's control that undoes a recorded re-snapshot.
_INHERITANCE_UI = (
    "**Resume from the Full Load's slot instead**",
    "*Manual re-snapshots instead of resuming from this load's slot*",
)


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

    section = _section_43(lang)
    assert _fact(lang, section, "untick"), lang
    for quoted in _INHERITANCE_UI:
        assert _has(lang, section, quoted), (lang, quoted)


# -- a same-named slot CDC cannot use: its own fix first ------------------------------------

# §4.3's blocked-Start table gives the slot of that name its own row, ahead of the inheritance
# row (the order in which the blocks are reported), and names the slot's own fix first.
_INHERITANCE_ROW = "*CDC cannot snapshot a table that has inheritance child tables*"


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

    section = _section_43(lang)
    assert _fact(lang, section, "unusable-fix"), lang
    assert _steps_in_order(
        lang, section, _F43["unusable-cell"][lang], _F43["unusable-fix"][lang], _INHERITANCE_ROW
    ), lang


# -- stopped, then a Start rolled back: the retention bound counts from the Stop ------------

# §4.3 puts a restart after Stop CDC and a restart after a rolled-back Start in one row of its
# "Starting again" table; where the retention bound is counted from (the Stop, or that Start) is
# on the cards themselves.


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

    section = _section_43(lang)
    assert _fact(lang, section, "restart-row"), lang
    assert _fact(lang, section, "gapless-while-retained"), lang


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
# §4.3 states the same fact from the restart's side: a source connector that never completed its
# snapshot snapshots every selected table again (the Full Load's start point is on the card).


@pytest.mark.parametrize("lang", LANGS)
def test_no_page_promises_a_resume_after_a_new_start_point_full_load(lang: str) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the stopped stack's card describes a resume only from where streaming
    # stopped, or from the Full Load's start point when the source connector never streamed --
    # never from a slot a Full Load re-created since (that Full Load is refused).
    card = cu._pg_resume_continues("s", snapshot_never=True)
    assert "or from the Full Load's start point if its source connector never streamed." in card
    assert "since that load created it" not in card and "recreated the slot" not in card

    assert _fact(lang, _section_43(lang), "mid-snapshot"), lang
    assert not _stale(lang, _STALE_RESUME[lang]), lang


# -- chapter 6: every re-run it prescribes for a newer start point waits for new topics ----

# G25 round 7 moved this pin with the text: a newer Manual point is a new start point too, and on
# a MySQL source Start CDC refuses a newer one whatever the migration type (decision A2).
_CH06 = {  # 2026-10-26: the lead sentence and the route stay; the per-engine refusals are table cells now
    "en": (
        "Wherever this chapter sends you to **Re-run Full Load with \"Drop & reload\"** (or "
        "**Re-run all tables**), or to a newer **Manual** start point, to give CDC a newer start "
        "point, that start point is **refused** while this migration's CDC stack has streamed — "
        "stopped after a Start, its connectors in place, or a Start CloudFormation rolled back.",
        "**Delete CDC infrastructure** and deploy it again first, then the re-run (or the Manual "
        "point), then **Start CDC**",
        "The Full Load step refuses the re-run",
        "Start CDC refuses a start point newer than the one the stack last started from, whatever "
        "the migration type, so a detour through **Full load only** does not get around it",
    ),
    "ko": (
        "이 장에서 CDC에 더 새로운 시작 지점을 주기 위해 **Re-run Full Load를 \"Drop & reload\"로**(또는 "
        "**Re-run all tables**) 실행하거나 더 새 **Manual** 시작 지점을 쓰라고 안내하는 곳은 모두, 이 "
        "마이그레이션의 CDC 스택이 스트리밍한 적 있는 동안 — Start 뒤에 멈췄거나, 커넥터가 그대로 있거나, "
        "CloudFormation이 Start를 롤백한 동안 — 그 시작 지점이 **거부됩니다**.",
        "먼저 **Delete CDC infrastructure** 후 다시 배포하고, 그다음 재실행(또는 Manual 지점), "
        "그다음 **Start CDC**",
        "Full Load 단계가 재실행을 거부합니다",
        "Start CDC가 마이그레이션 유형과 상관없이 스택이 마지막으로 시작한 지점보다 새 시작 지점을 "
        "거부하므로, **Full load only**를 거쳐 돌아와도 우회되지 않습니다",
    ),
    "ja": (
        "この章が CDC により新しい開始位置を与えるために **Drop & reload で Re-run Full Load**(または "
        "**Re-run all tables**)、またはより新しい **Manual** の開始位置を案内している箇所はすべて、この"
        "マイグレーションの CDC スタックがストリーミングしたことのある間 — Start の後に停止した、コネクタが"
        "そのまま残っている、または CloudFormation が Start をロールバックした間 — その開始位置は "
        "**拒否されます**。",
        "先に **Delete CDC infrastructure** して再デプロイし、次に再実行 (または Manual の位置)、次に "
        "**Start CDC** です",
        "Full Load ステップがその再実行を拒否します",
        "Start CDC がマイグレーションの種類にかかわらず、スタックが最後に開始した位置より新しい開始位置を"
        "拒否するため、**Full load only** を経由しても回避できません",
    ),
}
# Chapter 4's binlog bullet carries the same condition in its parenthesis (the reason is §4.3's
# "newer start point" row).


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

    for fact in _CH06[lang]:
        assert _has(lang, _page(lang, _PAGE["06"]), fact), (lang, fact)
    section = _section_43(lang)
    assert _fact(lang, section, "binlog-started"), lang
    assert _fact(lang, section, "newer-refused"), lang


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
