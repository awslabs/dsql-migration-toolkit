# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 7 (docs): the manual, in English, Korean and Japanese alike, states what round 7
shipped, and what it said before is pinned as stale so it cannot come back.

* (A2) Start CDC's own backstop to the Full Load step's new-start-point gate. On a MySQL source
  it refuses to move a CDC stack that has streamed forward to a start point newer than the one
  that stack last started from -- whatever the migration type, so a detour through Full load
  only (or CDC only) is no way around the gate -- and names the redeploy route. A start point
  at or before that one continues the stream; after a first Start CloudFormation rolled back, a
  later one is refused while the app still holds the start point that Start offered, and every
  one after an app restart (G25 round 8; the pins of what round 8 changed are in
  test_g25_docs_r8.py) -- since G26 only when the stack's CloudFormation events do not give that
  point either (test_g26_docs.py). On PostgreSQL nothing is refused: Start CDC
  continues from the stack's own slot, which a Full load only run leaves alone. Chapter 3 said
  a Full load only or CDC only migration was "not gated at all", chapters 4 and 10 promised a
  later MySQL CDC start "from the watermark" on any stack, chapter 6 listed a Start after a
  newer Full Load or Manual point as an ordinary recovery-mode start, and its lead said a
  Manual point "keeps the original start point".
* (B) chapter 6's remedies that give a stack that has streamed a new start point put Delete CDC
  infrastructure and a new deploy first, with no same-stack Manual route.
* (D) no restart after Stop CDC is called gapless without the retained-topics condition: the
  heartbeat, the restart of a pipeline an earlier version started, the purged-GTID resume, a
  sink-capacity change (chapter 7 and the Settings help text) and the Not gated entry.
* Validation's recovery card, as round 7 renders it: an unread CDC state takes the streamed
  order, CDC only switches the type, a stack that never streamed has no Stop CDC step, and Full
  load only has no CDC step.
* Accuracy outside A-D: Full load only's cut-over re-run chooses Drop & reload (the default
  Append applies no update or delete), the plugin list names v53, and Start over keeps a CDC
  stack whose new Full Load start point needs a redeploy.

Each check reads the CODE first (its own constants and the real derivations) and only then the
manual text, with the hard wrap undone (in ja, with no whitespace at all).
"""

from __future__ import annotations

import re
from types import SimpleNamespace

import pytest

from tests.test_g25_docs import _CDC_UI, LANGS, ROOT, _function, _has, _page
from tests.test_g25_docs_r4 import _stale

_P03 = "03-full-load.md"
_P04 = "04-cdc-and-dsql-constraints.md"
_P05 = "05-validation.md"
_P06 = "06-limitations.md"
_P07 = "07-performance-and-tuning.md"
_P10 = "10-conclusion.md"
_P11 = "11-customer-faq.md"
_P13 = "13-excluding-tables-and-columns.md"
_SRC = ROOT / "src" / "dsql_migrator"


def _joined(path) -> str:
    """A Python source with adjacent string literals joined, so a message split across lines
    reads as the one string it is."""
    return re.sub(r'"\s*\n\s*"', "", path.read_text(encoding="utf-8"))


def _seed(file: str, pos: int) -> SimpleNamespace:
    return SimpleNamespace(binlog_file=file, binlog_position=pos)


def _all(lang: str, page: str, phrases: dict) -> None:
    text = _page(lang, page)
    for key, by_lang in phrases.items():
        assert _has(lang, text, by_lang[lang]), (lang, page, key)


# -- (A2) chapter 3: Start CDC refuses a newer MySQL start point --------------------------

_CH03_A2 = {
    "what": {
        "en": "**Start CDC refuses a newer start point (MySQL).** On a MySQL source, Start CDC "
        "refuses to move a CDC stack that has streamed forward to a start point newer than the "
        "one it last started from (or, once it is stopped, to any start point when its last "
        "Start offered none), whatever the migration type: for example the watermark of a "
        "Re-run Full Load made in **Full load only** after switching away from Full load + CDC, "
        "or a later **Manual** point.",
        "ko": "**Start CDC도 더 새 시작 지점을 거부합니다(MySQL).** MySQL 소스에서 Start CDC는 스트리밍한 "
        "적 있는 CDC 스택을 마이그레이션 유형과 상관없이, 스택이 마지막으로 시작한 지점보다 새 시작 "
        "지점으로(또는 멈춘 스택의 마지막 Start가 시작 지점을 주지 않았다면 어떤 시작 지점으로도) 앞당기는 "
        "것을 거부합니다: 예를 들어 Full load + CDC에서 **Full load only**로 바꾼 뒤 Re-run Full Load가 "
        "잡은 워터마크나, 더 뒤의 **Manual** 지점입니다.",
        "ja": "**Start CDC もより新しい開始位置を拒否します (MySQL)。** MySQL ソースでは、Start CDC は、"
        "ストリーミングしたことのある CDC スタックを、マイグレーションの種類にかかわらず、スタックが最後に"
        "開始した位置より新しい開始位置へ (または、停止したスタックの最後の Start が開始位置を渡さなかった"
        "場合は、どの開始位置へも) 進めることを拒否します: たとえば Full load + CDC から **Full load only** "
        "に切り替えた後の Re-run Full Load が取ったウォーターマークや、より後の **Manual** の位置です。",
    },
    "surfaces": {
        "en": "(*Start CDC blocked: This CDC stack has streamed, so Start CDC does not move it "
        "forward to a newer start point …*), refuse it too. A refused Manual point also says "
        "when setting the start point back to **Automatic** would continue the stream.",
        "ko": "(*Start CDC blocked: This CDC stack has streamed, so Start CDC does not move it "
        "forward to a newer start point …*)도 거부합니다. 거부된 Manual 지점은 시작 지점을 "
        "**Automatic**으로 되돌리면 스트림이 이어지는 경우 그렇다고도 알려 줍니다.",
        "ja": "(*Start CDC blocked: This CDC stack has streamed, so Start CDC does not move it "
        "forward to a newer start point …*) も拒否します。拒否された Manual の位置は、開始位置を "
        "**Automatic** に戻せばストリームが続く場合、そのことも示します。",
    },
    # G25 round 8 moved the rolled-back Start's half of this pin to test_g25_docs_r8.py: after
    # an app restart every start point is refused there, not only a later one until it.
    "not-refused": {
        "en": "A start point at or before that one, or none, continues the stream and is not "
        "refused.",
        "ko": "그 지점과 같거나 이전인 시작 지점, 또는 시작 지점이 없는 Start는 스트림을 이어 가며 거부되지 "
        "않습니다.",
        "ja": "その位置と同じかそれより前の開始位置、または開始位置のない Start はストリームを続け、拒否され"
        "ません。",
    },
}
_CH03_STALE = {
    "en": ("Nor is a **Full load only** or **CDC only** migration gated at all.",),
    "ko": ("**Full load only**와 **CDC only** 마이그레이션은 아예 막히지 않습니다.",),
    "ja": ("**Full load only** と **CDC only** のマイグレーションはまったく制限されません。",),
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_3_states_start_cdcs_newer_start_point_refusal_as_the_code_applies_it(
    lang: str,
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.cdc_start_point_gate import (
        NEWER_POINT_HEADER,
        new_topics_route,
        newer_point_line,
        newer_point_notice,
    )
    from dsql_migrator.ui.data_migration._cdc_status import newer_start_point
    from dsql_migrator.ui.data_migration._models import MigrationType

    mysql = SourceType.MYSQL
    last = ("mysql-bin.000002", 500)
    newer, same, older = (
        _seed("mysql-bin.000003", 4),
        _seed(*last),
        _seed("mysql-bin.000002", 4),
    )

    def judge(seed, **facts):
        base = dict(
            has_committed_offset=False, connectors_exist=False, rolled_back=False,
            deployed_watermark=None, source_type=mysql,
        )
        return newer_start_point(seed, **{**base, **facts})

    # The code first. A stopped stack: a newer seed is refused, one at or before the last start
    # point (or no seed) continues, and with no recorded start point any seed is refused.
    assert judge(newer, has_committed_offset=True, deployed_watermark=last) is not None
    assert judge(same, has_committed_offset=True, deployed_watermark=last) is None
    assert judge(older, has_committed_offset=True, deployed_watermark=last) is None
    assert judge(None, has_committed_offset=True, deployed_watermark=last) is None
    assert judge(newer, has_committed_offset=True).last is None
    # Connectors in place (Retry CDC): refused only when provably past it -- hence "once it is
    # stopped" for the no-start-point case.
    assert judge(newer, connectors_exist=True, deployed_watermark=last) is not None
    assert judge(newer, connectors_exist=True) is None
    # A rolled-back first Start: judged against the seed it offered while the app knows it; the
    # retry from the same point is allowed. With no record of it, every seed is refused (G25
    # round 8) -- since G26 after an app restart only when the stack's events do not give it.
    assert judge(newer, rolled_back=True, offered_seed=last).rolled_back is True
    assert judge(same, rolled_back=True, offered_seed=last) is None
    unknown = judge(newer, rolled_back=True)
    assert unknown is not None and unknown.last is None and unknown.rolled_back is True
    assert judge(same, rolled_back=True) is not None
    # A stack never started is not judged.
    assert judge(newer) is None
    # The words: the header, the job's one-line form (under 300 characters as a job error's
    # first line), the route by type, and the Manual point's way back to Automatic.
    assert NEWER_POINT_HEADER == "This CDC stack cannot start from a newer start point"
    line = newer_point_line(MigrationType.FULL_LOAD_AND_CDC)
    assert line.startswith(
        "This CDC stack has streamed, so Start CDC does not move it forward to a newer start "
        "point"
    )
    assert len("Start CDC blocked: " + line) <= 300
    assert new_topics_route(MigrationType.CDC_ONLY).endswith("then Start CDC on the new stack")
    notice = newer_point_notice(
        point="the Manual start point (binlog mysql-bin.000003:4)",
        last="binlog mysql-bin.000002:500",
        rolled_back=False,
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
        automatic_resumes=True,
    )
    assert notice[2] == NEWER_POINT_HEADER
    assert "set the CDC start point back to Automatic" in notice[3]

    page = _page(lang, _P03)
    assert _has(lang, page, "*This CDC stack cannot start from a newer start point*"), lang
    _all(lang, _P03, _CH03_A2)
    assert not _stale(lang, _CH03_STALE[lang]), lang


# -- (A2) PostgreSQL: nothing is refused, the stream continues from the stack's slot -------

# G25 round 8: the stream continues from the slot only if the stack's source connector streamed
# (a Full load only watermark has no slot, so the Start sends snapshot.mode=initial, which
# snapshots first otherwise); the re-worded pins are in test_g25_docs_r8.py.
_PG_CONTINUES = {
    _P03: {
        "en": "On PostgreSQL there is no such refusal: Start CDC continues from the stack's own "
        "replication slot whatever the Full Load's watermark says, and a Full load only run "
        "leaves that slot alone",
        "ko": "PostgreSQL에는 이런 거부가 없습니다: Start CDC는 Full Load의 워터마크와 상관없이 스택 자체의 "
        "복제 슬롯에서 이어 가고, Full load only 실행은 그 슬롯을 건드리지 않으므로",
        "ja": "PostgreSQL にはこの拒否はありません: Start CDC は Full Load のウォーターマークにかかわらず"
        "スタック自身のレプリケーションスロットから続け、Full load only の実行はそのスロットに触れない"
        "ため",
    },
    _P06: {
        "en": "On a PostgreSQL source Start CDC has no such refusal: it continues from the "
        "stack's own replication slot, which a Full load only run leaves as it is",
        "ko": "PostgreSQL 소스의 Start CDC에는 이런 거부가 없습니다: 스택 자체의 복제 슬롯에서 이어 가고 "
        "Full load only 실행은 그 슬롯을 그대로 두므로",
        "ja": "PostgreSQL ソースの Start CDC にはこの拒否はありません: スタック自身のレプリケーション"
        "スロットから続け、Full load only の実行はそのスロットをそのまま残すため",
    },
}


@pytest.mark.parametrize("lang", LANGS)
def test_postgresql_refuses_no_newer_start_point_and_continues_from_its_slot(
    lang: str,
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_status import newer_start_point
    from dsql_migrator.ui.data_migration._cdc_ui import newer_start_point_refusal
    from dsql_migrator.ui.data_migration._models import MigrationType

    # The code first: no PostgreSQL stack is ever refused by the backstop, in any state.
    seed = _seed("mysql-bin.000003", 4)
    for facts in (
        dict(has_committed_offset=True, connectors_exist=False, rolled_back=False),
        dict(has_committed_offset=False, connectors_exist=True, rolled_back=False),
        dict(has_committed_offset=False, connectors_exist=False, rolled_back=True),
    ):
        assert newer_start_point(
            seed, deployed_watermark=("mysql-bin.000002", 500), offered_seed=None,
            source_type=SourceType.POSTGRES, **facts,
        ) is None
    stopped = SimpleNamespace(
        cdc_has_committed_offset=True, cdc_connector_names=[], cdc_stack_phase="infra",
        cdc_deployed_watermark=("mysql-bin.000002", 500), cdc_offered_seed=None,
        cdc_stack_phase_status="UPDATE_COMPLETE", migration_type=MigrationType.CDC_ONLY,
    )
    assert newer_start_point_refusal(stopped, seed, SourceType.POSTGRES) is None
    assert newer_start_point_refusal(stopped, seed, SourceType.MYSQL) is not None

    for page, by_lang in _PG_CONTINUES.items():
        assert _has(lang, _page(lang, page), by_lang[lang]), (lang, page)


# -- (A2) chapters 4 and 10: a later CDC start after a Full load only run ------------------

# G25 round 8 re-worded the rest of both sentences -- PostgreSQL continues from the slot only if
# the stack's source connector streamed, and MySQL's route switches the type before the CDC
# step's Delete (rule F) with a Drop & reload copy after an append (rule E): those pins, and the
# round-7 wording as stale, are in test_g25_docs_r8.py.
_LATER_START = {
    "pg": {
        "en": "Start CDC instead continues from that stack's own slot",
        "ko": "대신 Start CDC가 그 스택 자체의 슬롯에서 이어 가",
        "ja": "代わりに Start CDC がそのスタック自身のスロットから続け",
    },
    "mysql": {
        "en": "On MySQL the binary log retains history with no consumer, so a later CDC start on "
        "a CDC stack that has never streamed starts from the watermark with no re-read. On one "
        "that has streamed, Start CDC refuses that newer start point",
        "ko": "MySQL은 소비자가 없어도 바이너리 로그가 히스토리를 보관하므로, 스트리밍한 적 없는 CDC "
        "스택에서는 나중에 CDC를 시작해도 재읽기 없이 워터마크부터 시작합니다. 스트리밍한 적 있는 "
        "스택에서는 Start CDC가 그 더 새 시작 지점을 거부하므로",
        "ja": "MySQL はコンシューマーがいなくてもバイナリログが履歴を保持するため、一度もストリーミングした"
        "ことのない CDC スタックでは、後から CDC を開始してもウォーターマークから再読み込みなしで開始"
        "します。ストリーミングしたことのあるスタックでは Start CDC がそのより新しい開始位置を拒否する"
        "ため",
    },
}
_LATER_START_STALE = {
    "en": ("so a later CDC start resumes from the watermark with no re-read",),
    "ko": ("나중에 CDC를 시작해도 재읽기 없이 워터마크부터 재개합니다",),
    "ja": ("後から CDC を開始してもウォーターマークから再読み込みなしで再開できます",),
}
_RECOVERY_START = {
    "en": "On a stack that has streamed, Start CDC refuses a newer Full Load or Manual point "
    "(*This CDC stack cannot start from a newer start point*) until the infrastructure is "
    "deployed again: its topics may hold changes no sink applied, which its sink would write "
    "over anything loaded since, while the source would skip every change between where it "
    "stopped and the new point",
    "ko": "스트리밍한 적 있는 스택에서는 더 새 Full Load나 Manual 지점에서의 Start를 인프라를 다시 배포할 "
    "때까지 Start CDC가 거부합니다(*This CDC stack cannot start from a newer start point*): 그 스택의 "
    "토픽에는 어떤 싱크도 적용하지 않은 변경이 남아 있을 수 있고, 그 싱크는 그 변경을 그 뒤에 적재한 것 "
    "위에 쓰는 한편 소스는 멈춘 곳과 새 지점 사이의 모든 변경을 건너뛰기 때문입니다",
    "ja": "ストリーミングしたことのあるスタックでは、インフラを再デプロイするまで、Start CDC がより新しい "
    "Full Load や Manual の位置を拒否します (*This CDC stack cannot start from a newer start "
    "point*): そのスタックのトピックにはどの sink も適用していない変更が残っている可能性があり、その "
    "sink はそれをその後にロードしたものの上に書き込み、その一方でソースは停止した位置と新しい位置の"
    "間のすべての変更をスキップするためです",
}
_RECOVERY_STALE = {
    "en": (
        "the first Start, a Start after a newer Full Load or Manual point, or the first Start "
        "after Delete CDC infrastructure",
        "a Start after **Re-run Full Load** or a newer Manual point, and the first Start after",
    ),
    "ko": (
        "첫 Start, 더 새 Full Load나 Manual 지점 이후의 Start, Delete CDC infrastructure 후의 첫 Start",
        "**Re-run Full Load**나 더 새 Manual 지점 이후의 Start, 그리고",
    ),
    "ja": (
        "初回の Start、より新しい Full Load や Manual 位置の後の Start、Delete CDC infrastructure 後の"
        "初回 Start",
        "**Re-run Full Load** やより新しい Manual 位置の後の Start、そして",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_later_cdc_start_after_full_load_only_is_scoped_to_the_stack_it_meets(
    lang: str,
) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._cdc_status import newer_start_point

    # The code first: the Full load only watermark is newer than where a streamed MySQL stack
    # last started, so it is refused; on a stack that never streamed it is the first start.
    wm = _seed("mysql-bin.000009", 4)
    common = dict(connectors_exist=False, rolled_back=False, source_type=SourceType.MYSQL)
    assert newer_start_point(
        wm, has_committed_offset=True, deployed_watermark=("mysql-bin.000002", 500), **common
    ) is not None
    assert newer_start_point(
        wm, has_committed_offset=False, deployed_watermark=None, **common
    ) is None

    for page in (_P04, _P10):
        text = _page(lang, page)
        for key in ("pg", "mysql"):
            assert _has(lang, text, _LATER_START[key][lang]), (lang, page, key)
    assert _has(lang, _page(lang, _P04), _RECOVERY_START[lang]), lang
    assert not _stale(lang, _LATER_START_STALE[lang] + _RECOVERY_STALE[lang]), lang


# -- chapter 6's lead: a Manual point is a new start point ---------------------------------

# G132 moved this pin with the text: "not affected" read as "available whatever CDC does", but
# while CDC streams they wait for Stop CDC like every Full Load (G129).
_MANUAL_IS_NEW = {
    "en": "**Reload specific tables** and **Retry unfinished tables** keep the original start "
    "point, so this refusal does not apply to them (while CDC streams they wait for Stop CDC, "
    "like every Full Load). A Manual point is a new start point: the changes between "
    "where CDC stopped and it are not replicated, on the new stack too, so reload or correct "
    "those tables and validate with CHECKSUM, as each remedy below says.",
    "ko": "**Reload specific tables**와 **Retry unfinished tables**는 원래 시작 지점을 유지하므로 이 "
    "거부는 적용되지 않습니다(CDC가 스트리밍하는 동안에는 모든 Full Load처럼 Stop CDC를 기다립니다). "
    "Manual 지점은 새 시작 지점입니다: CDC가 멈춘 곳과 그 지점 사이의 변경은 새 스택에서도 "
    "복제되지 않으므로, 아래 각 해결 방법이 말하는 대로 그 테이블을 다시 적재하거나 바로잡고 CHECKSUM으로 "
    "검증하세요.",
    "ja": "**Reload specific tables** と **Retry unfinished tables** は元の開始位置を維持するため、この"
    "拒否は適用されません (CDC のストリーミング中は、すべての Full Load と同じく Stop CDC を待ちます)。"
    "Manual の位置は新しい開始位置です: CDC が停止した位置とその位置の間の変更は新しい"
    "スタックでも複製されないため、以下の各対処方法が述べるとおり、それらのテーブルを再ロードまたは"
    "修正し、CHECKSUM で検証してください。",
}
_MANUAL_STALE = {
    "en": (
        "**Manual** start points, **Reload specific tables** and **Retry unfinished tables** "
        "keep the original start point",
        "**A new CDC start point needs new CDC topics first (Full load + CDC).**",
    ),
    "ko": (
        "**Manual** 시작 지점, **Reload specific tables**, **Retry unfinished tables**는 원래 시작 "
        "지점을 유지하므로",
        "**새 CDC 시작 지점에는 새 CDC 토픽이 먼저 필요합니다 (Full load + CDC).**",
    ),
    "ja": (
        "**Manual** の開始位置、**Reload specific tables**、**Retry unfinished tables** は元の開始"
        "位置を維持するため",
        "**新しい CDC の開始位置には、まず新しい CDC トピックが必要です (Full load + CDC)。**",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_6_lead_says_a_manual_point_is_a_new_start_point(lang: str) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.cdc_start_point_gate import NEWER_POINT_HEADER
    from dsql_migrator.ui.data_migration._cdc_ui import newer_start_point_refusal
    from dsql_migrator.ui.data_migration._models import MigrationType

    # The code first: on a stopped MySQL stack a Manual point past where it last started is
    # refused (it is a new start point, not the original one), and the notice names Automatic
    # as the way to continue the stream when the job's watermark would.
    stopped = SimpleNamespace(
        cdc_has_committed_offset=True, cdc_connector_names=[], cdc_stack_phase="infra",
        cdc_deployed_watermark=("mysql-bin.000002", 500), cdc_offered_seed=None,
        cdc_stack_phase_status="UPDATE_COMPLETE",
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    refused = newer_start_point_refusal(
        stopped, _seed("mysql-bin.000003", 4), SourceType.MYSQL, origin="manual",
        job_watermark=_seed("mysql-bin.000002", 500),
    )
    assert refused is not None and refused[0][2] == NEWER_POINT_HEADER
    assert "set the CDC start point back to Automatic" in refused[0][3]

    assert _has(lang, _page(lang, _P06), _MANUAL_IS_NEW[lang]), lang
    assert not _stale(lang, _MANUAL_STALE[lang]), lang


# -- (B) chapter 6's remedies on a stack that has streamed: new topics first ---------------

_CH06_ROUTES = {
    "ddl": {
        "en": "**Way forward.** Stop CDC, then **Delete CDC infrastructure** and deploy it again: "
        "the stack has streamed, so a new start point needs new, empty topics (until then the "
        "Full Load step refuses a new start point, and Start CDC a newer one). Then give CDC a "
        "start point after the statement:",
        "ko": "**해결 방법.** CDC를 Stop한 다음 **Delete CDC infrastructure** 후 다시 배포하세요: 스택이 "
        "스트리밍했으므로 새 시작 지점에는 새 빈 토픽이 필요합니다(그때까지 Full Load 단계는 새 시작 "
        "지점을, Start CDC는 더 새 시작 지점을 거부합니다). 그다음 그 문 이후의 시작 지점을 CDC에 주세요:",
        "ja": "**解決方法。** CDC を Stop してから **Delete CDC infrastructure** して再デプロイして"
        "ください: スタックはストリーミングしたため、新しい開始位置には新しい空のトピックが必要です (それ"
        "までは Full Load ステップが新しい開始位置を、Start CDC がより新しい開始位置を拒否します)。その後、"
        "その文より後の開始位置を CDC に与えてください:",
    },
    "schema-change": {
        "en": "On a stack that has streamed (a restart after Stop CDC, or a connector that "
        "stopped mid-run), **Delete CDC infrastructure** and deploy it again first: you get new, "
        "empty topics, and until then the Full Load step refuses a new start point and Start "
        "CDC refuses a newer one.",
        "ko": "스트리밍한 적 있는 스택(Stop CDC 후의 재시작, 또는 실행 도중 멈춘 커넥터)이면 먼저 **Delete "
        "CDC infrastructure** 후 다시 배포하세요: 새 빈 토픽을 얻으며, 그때까지 Full Load 단계는 새 시작 "
        "지점을, Start CDC는 더 새 시작 지점을 거부합니다.",
        "ja": "ストリーミングしたことのあるスタック (Stop CDC 後の再起動、または実行の途中で停止した"
        "コネクタ) では、先に **Delete CDC infrastructure** して再デプロイしてください: 新しい空の"
        "トピックが得られ、それまでは Full Load ステップが新しい開始位置を、Start CDC がより新しい開始"
        "位置を拒否します。",
    },
    "schema-change-start": {
        "en": "The new run's watermark is past the change, and Start CDC on the new stack (or the "
        "stack's first Start) starts from it.",
        "ko": "새 실행의 워터마크는 변경 이후이며, 새 스택에서의(또는 스택의 첫) Start CDC가 그 지점에서 "
        "시작합니다.",
        "ja": "新しい実行のウォーターマークは変更より後で、新しいスタックでの (または スタックの初回の) "
        "Start CDC がそこから開始します。",
    },
    "gtid-mode": {
        "en": "or give CDC a new start point — for a restart after Stop CDC (a stack that has "
        "streamed), after **Delete CDC infrastructure** and a new deploy first:",
        "ko": "CDC에 새 시작 지점을 주세요 — Stop CDC 후의 재시작(스트리밍한 적 있는 스택)이라면 먼저 "
        "**Delete CDC infrastructure** 후 다시 배포한 뒤:",
        "ja": "CDC に新しい開始位置を与えてください — Stop CDC 後の再起動(ストリーミングしたことのある"
        "スタック)では、先に **Delete CDC infrastructure** して再デプロイした後に:",
    },
    "purged": {
        "en": "Or give CDC a new start point, as the message says: this stack has streamed, so "
        "first **Delete CDC infrastructure** and deploy it again, then **Re-run Full Load with "
        "\"Drop & reload\"** for all tables",
        "ko": "또는 메시지대로 CDC에 새 시작 지점을 주세요: 이 스택은 스트리밍했으므로 먼저 **Delete CDC "
        "infrastructure** 후 다시 배포한 뒤, 모든 테이블에 대해 **\"Drop & reload\"로 Re-run Full Load**",
        "ja": "または、メッセージのとおり CDC に新しい開始位置を与えてください: このスタックはストリーミング"
        "したため、先に **Delete CDC infrastructure** して再デプロイし、次にすべてのテーブルについて "
        "**「Drop & reload」で Re-run Full Load**",
    },
}
_CH06_ROUTES_STALE = {
    "en": (
        "Stop CDC, then give CDC a start point after the statement:",
        "so Start CDC moves the offset forward to it",
        "Start never rewinds a stopped stack's offset, but it does advance it",
        "Manual — start from a later binlog position instead",
    ),
    "ko": (
        "CDC를 Stop한 다음, 그 문 이후의 시작 지점을 CDC에 주세요:",
        "Start CDC가 오프셋을 그 위치로 앞당깁니다",
        "Start는 멈춘 스택의 오프셋을 되감지는 않지만 앞으로는 이동합니다",
        "Manual — start from a later binlog position instead",
    ),
    "ja": (
        "CDC を Stop してから、その文より後の開始位置を CDC に与えてください:",
        "Start CDC がオフセットをそこまで進めます",
        "Start は停止中のスタックのオフセットを巻き戻しませんが、前には進めます",
        "Manual — start from a later binlog position instead",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_6_remedies_put_new_topics_first_on_a_stack_that_has_streamed(
    lang: str,
) -> None:
    from dsql_migrator.core import cdc_deployer as deployer
    from dsql_migrator.ui.cdc_start_point_gate import NEW_TOPICS, _ways_after_change
    from dsql_migrator.ui.data_migration._models import MigrationType

    # The code first: every connector diagnosis that offers a new start point names new topics
    # before any re-run, with the Manual alternative on the new stack.
    route = deployer._NEW_START_POINT_ROUTE
    assert route.startswith(
        "This stack has streamed, so a new start point needs new CDC topics: Delete CDC "
        "infrastructure and deploy it again"
    )
    assert route.index("Delete CDC infrastructure") < route.index("Re-run Full Load") < (
        route.index("Start CDC")
    )
    assert "On the new stack you can instead choose Manual" in route
    for text in (
        deployer._UNPARSEABLE_DDL_GUIDANCE,
        deployer._PURGED_GTIDS_GUIDANCE,
        deployer._ANONYMOUS_GTID_GUIDANCE,
        deployer._UNKNOWN_TABLE_GUIDANCE,
    ):
        assert route in text
    # ...and the start-point check's way forward on a resume: new topics first, Manual only on
    # the new stack, and the start-point card offers no same-stack Manual position any more.
    for kind in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        way = _ways_after_change("binlog mysql-bin.000002:500", kind, streamed=True)
        assert NEW_TOPICS in way
    cdc_only = _ways_after_change("binlog f:1", MigrationType.CDC_ONLY, streamed=True)
    assert "choose Manual on the new stack" in cdc_only
    assert "start from a later binlog position instead" not in _CDC_UI.read_text(
        encoding="utf-8"
    )

    _all(lang, _P06, _CH06_ROUTES)
    assert not _stale(lang, _CH06_ROUTES_STALE[lang]), lang


# -- (D) a restart after Stop CDC is gapless only while the CDC topics retain the tail ------

_TOPICS_CONDITION = {
    _P04: {
        "en": "so a restart's source position is not left behind by an idle window (the restart "
        "is gapless only while the CDC topics still retain the changes the sink had not applied: "
        "§4.3)",
        "ko": "재시작 지점이 유휴 구간 때문에 뒤처지지 않게 하는 **하트비트** 토픽",
        "ja": "再起動時のソース位置がアイドル区間のせいで取り残されないようにする **ハートビート** トピック",
    },
    _P06: {
        "en": "which then continues from where it stopped: the changes the sink had not applied "
        "at the Stop come from the CDC topics, so this is gapless only while the topics still "
        "retain them",
        "ko": "그 뒤에는 멈춘 곳에서 이어 갑니다(Stop 때 싱크가 적용하지 않은 변경은 CDC 토픽에서 오므로 "
        "토픽이 그 변경을 아직 보존하는 동안만 빈틈이 없습니다",
        "ja": "その後は停止した位置から続行します: Stop の時点で sink が適用していなかった変更は CDC "
        "トピックから届くため、ギャップがないのはトピックがまだその変更を保持している間だけです",
    },
    _P07: {
        "en": "| **Already streaming** | **Nothing changes until the next Start CDC: Stop CDC, "
        "then Start CDC.** |",
        "ko": "| **이미 스트리밍 중** | **다음 Start CDC까지 아무것도 바뀌지 않습니다: Stop CDC 후 Start "
        "CDC.** |",
        "ja": "| **すでにストリーミング中** | **次の Start CDC まで何も変わりません: Stop CDC の後に "
        "Start CDC。** |",
    },
}
_SINK_CAPACITY = {
    "en": "While CDC streams, the CDC step offers only **Stop CDC**, so a new sink capacity takes "
    "effect when you Stop CDC and Start CDC again. The connectors are then re-created with it "
    "(each connector create/delete uses MSK Serverless partitions), and the changes the sink "
    "had not applied at the Stop are applied from the CDC topics, so that restart is gapless "
    "only while the topics still retain them",
    "ko": "CDC가 스트리밍하는 동안 CDC 단계는 **Stop CDC**만 제공하므로, 새 싱크 용량은 Stop CDC 후 "
    "Start CDC를 다시 할 때 적용됩니다. 그때 커넥터가 새 용량으로 다시 만들어지고(커넥터 생성·삭제마다 "
    "MSK Serverless 파티션을 씀), Stop 때 싱크가 적용하지 않은 변경은 CDC 토픽에서 적용되므로, 그 "
    "재시작은 토픽이 그 변경을 아직 보존하는 동안만 빈틈이 없습니다",
    "ja": "CDC のストリーミング中、CDC ステップには **Stop CDC** しかないため、新しいシンク容量は Stop "
    "CDC の後にもう一度 Start CDC したときに反映されます。そのときコネクタはその容量で作り直され"
    "（コネクタの作成・削除のたびに MSK Serverless のパーティションを使います）、Stop の時点でシンクが"
    "適用していなかった変更は CDC トピックから適用されるため、その再起動にギャップがないのはトピックが"
    "まだその変更を保持している間だけです",
}
_TOPICS_STALE = {
    "en": (
        "so a restart can still resume gaplessly",
        "which then resumes exactly where it stopped",
        "**Nothing changes until you run Start CDC again.**",
        "and there is no gap in replication",
        "with no gap (if the binlog retention also ran out",
    ),
    "ko": (
        "재시작 시 무손실 재개를 가능케 하는",
        "그 뒤에는 멈춘 곳에서 정확히 재개합니다",
        "**Start CDC를 다시 실행할 때까지 아무것도 바뀌지 않습니다.**",
        "복제에 gap도 생기지 않습니다",
        "`file:position`으로 빈틈 없이 재개합니다",
    ),
    "ja": (
        "再起動後もギャップなく再開できるようにする",
        "その後は停止した位置から正確に再開します",
        "**Start CDC を再実行するまで何も変わりません。**",
        "レプリケーションにギャップも生じません",
        "`file:position` で欠落なく再開します(binlog の保持期間も",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_no_restart_after_stop_cdc_is_called_gapless_without_the_topics_condition(
    lang: str,
) -> None:
    from dsql_migrator.config import TUNABLE_KNOBS
    from dsql_migrator.core import cdc_deployer as deployer
    from dsql_migrator.ui.data_migration import _cdc_ui

    # The code first: the purged-GTID resume and the resume cards state the condition, and the
    # Settings help text of the sink capacity names the Stop and the condition, not an in-place
    # resize "with no gap".
    assert (
        "with no gap while the CDC topics still retain the changes the sink had not applied "
        "when CDC stopped" in deployer._PURGED_GTIDS_GUIDANCE
    )
    assert "gapless only while the topics still retain them" in _cdc_ui._MYSQL_TOPICS_CONDITION
    help_text = {k.field: k for k in TUNABLE_KNOBS}["cdc_sink_mcu_count"].help_text
    assert "offers only Stop CDC, so Stop CDC and Start CDC again" in help_text
    assert "gapless only while the topics still retain them" in help_text
    assert "no gap in replication" not in help_text and "updates in place" not in help_text

    for page, by_lang in _TOPICS_CONDITION.items():
        assert _has(lang, _page(lang, page), by_lang[lang]), (lang, page)
    assert _has(lang, _page(lang, _P07), _SINK_CAPACITY[lang]), lang
    assert not _stale(lang, _TOPICS_STALE[lang]), lang


# -- Validation's recovery card (05): unread, CDC only, never streamed, Full load only -----

_RUNBOOK_R7 = {
    "unread": {
        "en": "or whose CDC state has not been read yet (after an app restart, until the Data "
        "Migration step reads it; the card says so, and shows the steps for the stack as it is "
        "once it has been read)",
        "ko": "또는 CDC 상태를 아직 읽지 못한(앱 재시작 후 Data Migration 단계가 읽기 전. 카드가 그렇다고 "
        "말하며, 읽은 뒤에는 그 스택 상태에 맞는 단계를 보여 줍니다)",
        "ja": "または CDC の状態をまだ読み取っていない (アプリの再起動後、Data Migration ステップが読み取る"
        "まで。カードはそう述べ、読み取った後はそのスタックの状態に合った手順を示します)",
    },
    "cdc-only": {
        "en": "With CDC only, a step after the redeploy sets the migration type to Full load + "
        "CDC, which has the Full Load step.",
        "ko": "CDC only에서는 다시 배포한 다음 단계에서 마이그레이션 유형을 Full Load 단계가 있는 Full "
        "load + CDC로 바꿉니다.",
        "ja": "CDC only では、再デプロイの後の手順でマイグレーションの種類を、Full Load ステップのある "
        "Full load + CDC に切り替えます。",
    },
    "full-load-only": {
        "en": "In **Full load only** the card is *Re-run Full Load to backfill the gap*: the "
        "re-run, then **Re-run validation**, with no CDC step.",
        "ko": "**Full load only**에서는 카드가 *Re-run Full Load to backfill the gap*이며, 재실행 다음 "
        "**Re-run validation**이고 CDC 단계는 없습니다.",
        "ja": "**Full load only** ではカードは *Re-run Full Load to backfill the gap* で、再実行の次に "
        "**Re-run validation** となり、CDC の手順はありません。",
    },
}
# The never-streamed order, between its opening words and its closing parenthesis.
_NEVER_STREAMED_ORDER = {
    "en": ("On a stack that never streamed the order is shorter (", ")."),
    "ko": ("스트리밍한 적 없는 스택에서는 순서가 더 짧습니다(", ")."),
    "ja": ("ストリーミングしたことのないスタックでは手順はもっと短くなります(", ")。"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_validations_recovery_card_is_described_for_every_type_and_an_unread_state(
    lang: str,
) -> None:
    from tests.test_g25_docs import _flat

    # The code first: an unread state takes the new-topics order (a CDC type only), CDC only
    # adds the type switch, Full load only has its own header and no CDC step, and the
    # never-streamed order has no Stop CDC step.
    section = _function(_SRC / "ui" / "validation.py", "_render_recovery_section")
    assert (
        "new_topics_first = not full_load_only and (cdc_stack_streamed or cdc_state_unread)"
        in section
    )
    assert section.count("*((switch_type,) if cdc_only else ())") == 2
    assert '"Re-run Full Load to backfill the gap"\n                    if full_load_only' in (
        section
    )
    never = section[section.index("        else:\n            render_notice(") :]
    assert "Stop CDC" not in never

    page = _page(lang, _P05)
    for key, by_lang in _RUNBOOK_R7.items():
        assert _has(lang, page, by_lang[lang]), (lang, key)
    head, tail = (_flat(lang, part).strip() for part in _NEVER_STREAMED_ORDER[lang])
    order = page[page.index(head) :]
    order = order[: order.index(tail)]
    assert "Stop CDC" not in order.replace(" ", ""), (lang, order)
    assert "Full load + CDC" in order or "Fullload+CDC" in order, (lang, order)


# -- chapter 13: the type switch and a deleted CDC-only stack's re-copy ---------------------

_CH13 = {
    # G25 round 8 (rule F): the Delete and the redeploy come after the type switch, on the CDC
    # step -- Full load only has none (the pin of that order: test_g25_docs_r8.py).
    "switch": {
        "en": "a stack that has streamed only continues from where it stopped, never from a "
        "newer start point",
        "ko": "스트리밍한 적 있는 스택은 멈춘 곳에서만 이어 가며, 더 새 시작 지점에서는 결코 시작하지 "
        "않습니다",
        "ja": "ストリーミングしたことのあるスタックは停止した位置から続けるだけで、より新しい開始"
        "位置から開始することは決してありません",
    },
    "re-copy": {
        "en": "switch to Full load + CDC and run its Full Load before the new stack's first Start "
        "CDC (once that stack has streamed, a new Full Load start point needs another Delete and "
        "redeploy)",
        "ko": "Full load + CDC로 전환해 새 스택의 첫 Start CDC 전에 그 Full Load를 실행하고(그 스택이 "
        "스트리밍한 뒤에는 새 Full Load 시작 지점에 Delete와 재배포가 한 번 더 필요합니다)",
        "ja": "Full load + CDC に切り替えて、新しいスタックの最初の Start CDC の前にその Full Load を実行し "
        "(そのスタックがストリーミングした後では、新しい Full Load の開始位置にはもう一度 Delete と"
        "再デプロイが必要です)",
    },
    "v54": {
        "en": "Later releases changed the sink plugin (v51, v52, v53, v54)",
        "ko": "이후 릴리스에서 싱크 플러그인이 바뀌었으므로(v51, v52, v53, v54)",
        "ja": "その後のリリースでシンクプラグインが変わったため(v51、v52、v53、v54)",
    },
}
_CH13_STALE = {
    "en": ("changed the sink plugin (v51, v52),", "changed the sink plugin (v51, v52, v53),"),
    "ko": ("싱크 플러그인이 바뀌었으므로(v51, v52),", "싱크 플러그인이 바뀌었으므로(v51, v52, v53),"),
    "ja": ("シンクプラグインが変わったため(v51、v52)、", "シンクプラグインが変わったため(v51、v52、v53)、"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_chapter_13_names_new_topics_for_the_type_switch_and_the_re_copy(lang: str) -> None:
    from dsql_migrator.core.s3_provision import PLUGIN_VERSION
    from dsql_migrator.ui.data_migration._models import coverage_lock_reason

    # The code first: the type lock once CDC started, the CDC-only delete's re-copy, and the
    # shipped plugin version.
    lock = coverage_lock_reason((["app.a"], []), started=True)
    # The lock's reason for new topics. (Where the manual puts the Delete -- after the type
    # switch, on the CDC step, since Full load only has none -- is pinned in r8.)
    assert lock.endswith(
        "a stack that has streamed only continues from where it stopped, never from a newer "
        "start point."
    )
    monitoring = _joined(_SRC / "ui" / "data_migration" / "_cdc_monitoring.py")
    assert (
        "before the new stack's first Start CDC: once that stack has streamed, a new Full Load "
        "start point needs another Delete and redeploy." in monitoring
    )
    assert PLUGIN_VERSION == "v54"

    _all(lang, _P13, _CH13)
    assert not _stale(lang, _CH13_STALE[lang]), lang


# -- accuracy: Full load only's cut-over re-run, and Start over on a kept CDC stack ---------

_CUTOVER_RERUN = {
    "en": "**Re-run Full Load choosing \"Drop & reload\"** if the source took writes since the "
    "snapshot (the default *Append* only adds missing rows and applies no update or delete made "
    "since), then validate with CHECKSUM.",
    "ko": "**스냅샷 이후 소스에 쓰기가 있었다면 \"Drop & reload\"로 Full Load 재실행** — 기본값 *Append*는 "
    "빠진 행만 채우고 그 뒤의 UPDATE/DELETE는 적용하지 않습니다 — 그다음 CHECKSUM으로 검증.",
    "ja": "スナップショット以降にソースへの書き込みがあった場合は **「Drop & reload」で Full Load を"
    "再実行** します (既定の *Append* は欠けている行を追加するだけで、その後の UPDATE や DELETE は"
    "適用しません)。その後 CHECKSUM で検証します。",
}
_CUTOVER_STALE = {
    "en": ("it's idempotent, so it only fills the unfinished work, never duplicates",),
    "ko": ("멱등적이라 다시 실행해도 아직 끝나지 않은 부분만 채우고 중복은 만들지 않습니다",),
    "ja": ("冪等なので、まだ終わっていない作業だけを埋め、決して重複を作りません",),
}
_START_OVER = {
    "en": "If CDC has streamed on the CDC stack you keep, a fresh Full load + CDC run on it "
    "needs **Delete CDC infrastructure** and a new deploy first: the Full Load step refuses a "
    "new start point until then",
    "ko": "남겨 둔 CDC 스택에서 CDC가 스트리밍한 적 있다면, 그 스택에서 새로 Full load + CDC를 실행하려면 "
    "먼저 **Delete CDC infrastructure** 후 다시 배포해야 합니다: 그때까지 Full Load 단계가 새 시작 지점을 "
    "거부합니다",
    "ja": "残しておく CDC スタックで CDC がストリーミングしたことがある場合、そのスタックで新しく Full load "
    "+ CDC を実行するには、先に **Delete CDC infrastructure** して再デプロイする必要があります: それまでは "
    "Full Load ステップが新しい開始位置を拒否します",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_full_load_only_cut_over_and_start_over_name_what_the_code_does(lang: str) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration._full_load_engine import new_start_point_block
    from dsql_migrator.ui.data_migration._models import MigrationType

    # The code first: a re-run's dialog defaults to Append, which keeps existing rows; and a
    # kept CDC stack that has streamed holds a new Full Load start point, which the Start over
    # dialog's keep-infrastructure choice now says.
    full_load_ui = _joined(_SRC / "ui" / "data_migration" / "_full_load_ui.py")
    assert 'migration_state.set_reload_mode("append")' in full_load_ui
    assert '"append": "Append — keep existing rows, load only the missing ones' in full_load_ui
    streamed = SimpleNamespace(
        migration_type=MigrationType.FULL_LOAD_AND_CDC, cdc_has_committed_offset=True,
        cdc_stack_phase="infra", cdc_stack_phase_status="UPDATE_COMPLETE",
        cdc_stack_phase_checked=True, cdc_connector_names=[], cdc_probe_error=None,
    )
    assert new_start_point_block(streamed, SourceType.MYSQL) is not None
    start_over = _joined(_SRC / "ui" / "start_over.py")
    assert "A fresh Full load + CDC run needs new CDC topics: choose Delete instead." in (
        start_over
    )

    assert _has(lang, _page(lang, _P10), _CUTOVER_RERUN[lang]), lang
    assert _has(lang, _page(lang, _P11), _START_OVER[lang]), lang
    assert not _stale(lang, _CUTOVER_STALE[lang]), lang
