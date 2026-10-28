# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 4, closing step (docs): the manual, in English, Korean and Japanese alike, says what
the closing step's PostgreSQL Start CDC code does.

* Decision (2), confirmed[0], pending[0]/[1]: a cdc-stack whose last update CloudFormation
  rolled back (``UPDATE_ROLLBACK_COMPLETE``) counts as started. Chapter 4 defined a stack's
  first Start as one "while its CloudFormation parameters record no Start of its connectors",
  which a rollback restores, and judged "a Start that failed before its connectors existed" the
  same way. It now says that such a stack is never judged on the slot's position and why (its
  source connector may have streamed and committed its position before the rollback deleted
  it), that a block on it names the gapless routes, and that the same-stack-name residual also
  covers a slot re-created after this stack's first Start rolled back. The rolled-back stack's
  own residual is documented too: a Start that snapshots there does not check that the slot
  exists.
* confirmed[1]: "Re-snapshot every table instead" is offered only when the Start it would
  unlock (``snapshot.mode=initial``) passes its own route's checks, judged as the Start CDC
  dialog opens. Chapter 4 says so, quoting the lead the dialog shows when it is withheld, and
  chapters 6 and 13, which tell the reader to choose it after Delete CDC infrastructure, say
  it is offered only then.
* pending[2]: the "Publication autocreation is disabled" diagnosis sends the operator to the
  Start CDC dialog's gapless routes, on a stack whose connectors were stopped and on one whose
  Start rolled back; chapter 4 says the dialog names them on both.

Each check reads the code first, through its own constants or behaviour, then the text with the
hard wrap undone (and, in ja, with no whitespace at all). Each fails on the manual before this
step.

The last step (test_g25_docs_r4d.py) counts every ``UPDATE_ROLLBACK_*`` status, and this stack's
connectors that MSK Connect lists, as started; makes the retry after a rolled-back Start
gapless only while the CDC topics retain that Start's changes; gives a rolled-back stack one
route, new topics first; and reports a failed read of the re-snapshot route's own checks as
that. The pins those changes rewrote moved there (this step's wording is stale there); the
ones here still hold.

Round 5 (test_g25_docs_r5.py) gives every stack whose connectors were started before -- a
stopped one too -- that one route, and ends a block whose re-snapshot is refused for its own
reason with the route that works there. The two pins here it reworded (what the diagnosis
names, and that block) read its words now; this step's wording is stale.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from tests.test_g25_docs import (
    _PAGE,
    LANGS,
    _fact,
    _has,
    _inheritance_blocks,
    _page,
    _route_halves,
    _section_43,
    _steps_in_order,
)
from tests.test_g25_docs_r4 import _slot_objects, _stale

ROLLED_BACK = "UPDATE_ROLLBACK_COMPLETE"
_PUBLICATION_MISSING = (
    "CDC's publication does not exist on the source",
    "The publication is not on the source.",
)


def _gate(monkeypatch, status: str, *, force_initial=None):
    """The REAL publication/slot gate of a Full load + CDC Start on a stack in ``status`` whose
    parameters record no Start (``DeploySink=false``, a blank bootstrap: the ``infra`` phase).
    The source read is faked to report a missing publication. Returns the gate's block, the
    ``snapshot.mode`` it judged and the migration state."""
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration import _cdc_ui as cu
    from dsql_migrator.ui.data_migration._models import MigrationType

    judged: list = []

    def _probe(*_args, snapshot_mode="never", on_unread=None):
        judged.append(snapshot_mode)
        return _PUBLICATION_MISSING

    monkeypatch.setattr(cu, "_probe_pg_replication_objects", _probe)
    monkeypatch.setattr(cu, "_cdc_tables_for_config", lambda *_a, **_k: ["app.t"])
    state = SimpleNamespace(
        cdc_start_mode=lambda: "auto",
        cdc_has_committed_offset=False,
        cdc_stack_phase="infra",
        cdc_stack_phase_status=status,
        cdc_deployed_watermark=None,
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES))
    watermark = SimpleNamespace(  # the Full Load's: its LSN and the slot it created
        binlog_file=None, binlog_position=None, gtid_executed=None, server_uuid=None,
        wal_lsn="0/1000000", slot_name="s", publication_name="p",
    )
    block = cu._pg_objects_gate_block(state, session, None, watermark, force_initial=force_initial)
    (mode,) = judged
    return block, mode, state


# -- Decision (2): a Start CloudFormation rolled back counts as started ----------------------

# What a first Start is, which stacks count as started, how a retry continues after a rollback
# and the unchecked case's wording changed in the last step: those pins are in
# test_g25_docs_r4d.py (this step's wording is stale there). These still hold.
# §4.3 (answer-first) keeps the residual the reader acts on: the slot's name derives from the CDC
# stack name, so a second migration of the same source needs its own CDC stack name. How a refused
# Start, an app restart or Start over leave the next Start judged, and the rolled-back-then-dropped
# slot case, are the code assertions below.
# Round 4's wording: a first Start defined by the stack's parameters, which a rollback restores;
# a Start that failed before its connectors existed as the one failure judged the same way; the
# residual without the rollback; the routes on a stopped stack only.
_STALE_ROLLED_BACK_RULE = {
    "en": (
        "while its CloudFormation parameters record no Start of its connectors",
        "a Start that failed before its connectors existed",
        "re-creates the slot after this stack has streamed, so give each migration",
        "On a stack that was started and then stopped, a block for",
    ),
    "ko": (
        "CloudFormation 파라미터에 커넥터를 시작한 기록이 아직 없는 스택",
        "커넥터가 생기기 전에 실패한 Start",
        "이 스택이 스트리밍한 뒤 같은 CDC 스택 이름을 쓰는",
        "시작했다가 멈춘 스택에서는",
    ),
    "ja": (
        "CloudFormation のパラメータにコネクタを開始した記録がまだないスタック",
        "コネクタができる前に失敗した Start",
        "このスタックがストリーミングした後に、同じ CDC スタック名",
        "開始してから停止したスタックでは、",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_start_cloudformation_rolled_back_counts_as_started(monkeypatch, lang: str) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the stack record, as the stack-phase probe read it (one describe).
    rolled = SimpleNamespace(cdc_has_committed_offset=False, cdc_stack_phase="infra",
                             cdc_stack_phase_status=ROLLED_BACK)
    never = SimpleNamespace(cdc_has_committed_offset=False, cdc_stack_phase="infra",
                            cdc_stack_phase_status="UPDATE_COMPLETE")
    assert cu._pg_start_rolled_back(rolled) is True
    assert cu._pg_stack_started(rolled) is True
    assert cu._pg_stack_started(never) is False
    # A moved slot is refused only on a stack that records no Start, a rolled-back one
    # included, and the block claims no more than that record.
    kw = dict(resumes_from_slot=True, start_lsn="0/1000000")
    refused = pg.pg_replication_objects_blocker(_slot_objects(), ["app.t"], stack_started=False,
                                                **kw)
    assert refused is not None and refused[0] == pg.SLOT_POSITION_HEADER
    assert "records no Start of its connectors (not even one that rolled back)" in refused[1]
    assert pg.pg_replication_objects_blocker(
        _slot_objects(), ["app.t"], stack_started=cu._pg_stack_started(rolled), **kw
    ) is None
    # On a rolled-back stack a block names the gapless routes, as on a stopped one; on a stack
    # that records no Start it does not (the dialog offers its re-snapshot there).
    block, mode, state = _gate(monkeypatch, ROLLED_BACK)
    assert mode == "never" and block[0] == _PUBLICATION_MISSING[0]
    assert block[1] == f"{_PUBLICATION_MISSING[1]} {cu._pg_block_route(state, block[0])}"
    assert _gate(monkeypatch, "UPDATE_COMPLETE")[0] == _PUBLICATION_MISSING
    dialog = inspect.getsource(cu._open_cdc_start_dialog)
    # No re-snapshot offered there (G25 round 5: the started rule, rollback included).
    assert "and _pg_stack_started(\n        migration_state\n    )" in dialog
    assert "not _pg_resumes and pg_objects_block[0] not in BLOCKS_WITH_THEIR_OWN_FIX" in dialog

    assert _fact(lang, _section_43(lang), "slot-name"), lang
    assert not _stale(lang, _STALE_ROLLED_BACK_RULE[lang]), lang


# -- pending[2]: the publication diagnosis sends the operator to the routes the dialog names --

# G25 round 5: one route for every stack started before (stopped, in place or rolled back), new
# topics first, and the diagnosis names it too -- the manual says so in the started stack's own
# paragraph (its route: test_g25_docs_r4d.py and test_g25_docs_r5.py). This step's wording, the
# blocks naming "the gapless routes" by state, is stale.
# (§4.3 does not describe the diagnosis itself -- it is the connector log's guidance in the UI --
# but the two routes it names are the started half of the route paragraph, which the page keeps.)
_STALE_ROUTES = {
    "en": "On a stack that was started and then stopped, or whose last Start rolled back, a block "
    "for a missing publication, a publication that misses a selected table, or a missing or "
    "unusable slot names the gapless routes instead of a re-snapshot",
    "ko": "시작했다가 멈췄거나 마지막 Start가 롤백된 스택에서는 publication이 없거나, publication에 "
    "선택한 테이블이 빠졌거나, 슬롯이 없거나 쓸 수 없다는 차단이",
    "ja": "開始してから停止したスタックや最後の Start がロールバックされたスタックでは、publication が"
    "ない、publication に選択したテーブルが含まれていない、スロットがないか使用できないというブロックは、",
}
# Debezium's own line (debezium-connector-postgres 2.7.4) for a missing publication.
_PUBLICATION_GONE_TAIL = (
    "org.apache.kafka.connect.errors.ConnectException: Publication autocreation is disabled, "
    "please create one and restart the connector."
)


@pytest.mark.parametrize("lang", LANGS)
def test_the_publication_diagnosis_routes_are_the_ones_the_manual_names(
    monkeypatch, lang: str
) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    # The code first: the job error names the step for each stack state, then the route rule's
    # one route of a stack started before, for both migration types (G25 round 5).
    guidance = _diagnose_connector_log("dsql-cdc-stack-debezium-source", _PUBLICATION_GONE_TAIL)
    assert guidance is not None
    assert "If the CDC step still shows the connectors, Stop CDC first" in guidance
    assert "a Start whose CloudFormation update rolled back left none to remove" in guidance
    # G25 round 5: the route rule's own words, for both states and both migration types.
    both = pg.pg_cdc_route(_PUBLICATION_MISSING[0], started=True, cdc_only=None)
    assert both in guidance
    assert "then, for Full load + CDC," in both
    assert "and for CDC only, Start CDC on the new stack" in both
    _block, _mode, state = _gate(monkeypatch, ROLLED_BACK)
    for route in ("Re-run Full Load on the Full Load step choosing 'Drop & reload'",
                  "Delete CDC infrastructure and deploy it again"):
        assert route in guidance and route in _pg_block_route(state, _block[0]), route

    started = _route_halves(lang)[1]
    assert _steps_in_order(
        lang, started, "Delete CDC infrastructure", "Re-run Full Load", "Start CDC"
    ), lang
    assert _fact(lang, started, "cdc-only-new-stack"), lang
    assert not _stale(lang, (_STALE_ROUTES[lang],)), lang


# -- confirmed[1]: the re-snapshot remedy is offered only when its own Start passes -----------

# §4.3 keeps the offer's condition (the dialog offers the remedy only when the Start it unlocks
# would itself pass its checks) and the lead the dialog shows when it is withheld. Which refusal
# comes first, and the warning for a failed read of those checks, are in the dialog.
# This step's wording: a block refused for its own reason "shown alone", with no route.
_STALE_RESNAPSHOT = {
    "en": "If that Start would be refused for the block's own reason, the block is shown alone",
    "ko": "그 Start가 차단과 같은 이유로 거부되면 차단만 보여 주고",
    "ja": "その Start がブロックと同じ理由で拒否される場合はブロックだけを表示し",
}
# Chapters 13 and 6 tell the reader to choose the remedy after Delete CDC infrastructure.
_OFFERED_WHEN = {
    "13": {
        "en": "so choose **\"Re-snapshot every table instead\"** in the Start CDC dialog. It is "
        "offered only when the Start it unlocks passes its own checks; otherwise the dialog "
        "shows that Start's refusal (for example for a captured table with inheritance child "
        "tables; see [§4.3](04-cdc-and-dsql-constraints.md))",
        "ko": "**\"Re-snapshot every table instead\"**를 선택하세요. 이 방법은 그것이 풀어 줄 Start가 "
        "자체 검사를 통과할 때만 제시되며, 그렇지 않으면 다이얼로그가 그 Start의 거부를 보여 줍니다(예컨대 "
        "상속 자식 테이블이 있는 캡처 대상 테이블. [§4.3](04-cdc-and-dsql-constraints.md) 참고)",
        "ja": "**「Re-snapshot every table instead」** を選んでください。この方法は、それが解除する "
        "Start が自身のチェックを通過する場合にだけ提示され、そうでなければダイアログがその Start の拒否を"
        "表示します (たとえば継承の子テーブルを持つキャプチャ対象テーブル。"
        "[§4.3](04-cdc-and-dsql-constraints.md) を参照)",
    },
    "06": {
        "en": "(Full load + CDC: choose **\"Re-snapshot every table instead\"** when the Start CDC "
        "dialog offers it)",
        "ko": "(Full load + CDC: Start CDC 대화 상자가 제시하면 **\"Re-snapshot every table "
        "instead\"**를 선택)",
        "ja": "(Full load + CDC: Start CDC のダイアログが提示すれば **「Re-snapshot every table "
        "instead」** を選択)",
    },
}
_STALE_OFFERED = {  # ch. 6 before: the remedy named as if it were always there
    "en": "(Full load + CDC: choose **\"Re-snapshot every table instead\"**)",
    "ko": "(Full load + CDC: **\"Re-snapshot every table instead\"**를 선택)",
    "ja": "(Full load + CDC: **「Re-snapshot every table instead」** を選択)",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_re_snapshot_remedy_is_offered_only_when_its_own_start_passes(
    monkeypatch, lang: str
) -> None:
    from dataclasses import replace

    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the dialog judges the Start the remedy unlocks (Manual: initial) with
    # the same gate, as it opens, and shows that Start's refusal instead of the remedy.
    # (Round 4d: a failed read of that Start's checks has its own notice -- the publication and
    # slot checks before it did run: test_g25_docs_r4d.py.)
    lead, header = cu.PG_RESNAPSHOT_REFUSED_LEAD, cu.PG_RESNAPSHOT_NOT_CHECKED_HEADER
    assert lead == (
        "Re-snapshot every table is not offered here: a Start that re-snapshots is refused too."
    )
    assert _gate(monkeypatch, "UPDATE_COMPLETE", force_initial=True)[1] == "initial"
    assert _gate(monkeypatch, "UPDATE_COMPLETE")[1] == "never"
    dialog = inspect.getsource(cu._open_cdc_start_dialog)
    # (Round 4d: that read keeps its own unread reason, and the refusal is followed by the route
    # that works -- tests/test_g25_pg_r4d.py.)
    assert "on_unread=pg_resnapshot_unread.append, force_initial=True," in dialog
    # Own reason: no second notice -- its route follows the block (G25 round 5).
    assert "if pg_resnapshot_block[0] == pg_objects_block[0]:" in dialog
    assert 'f"{PG_RESNAPSHOT_REFUSED_LEAD} {pg_resnapshot_block[1]}",' in dialog
    # Two refusals of that Start the gapless one never meets: an inheritance parent, and a
    # slot of that name it cannot use (an invalidated one, which the gapless Start calls
    # missing).
    refused, _unread = _inheritance_blocks()
    assert refused[0] == pg.INHERITANCE_SNAPSHOT_HEADER
    lost = replace(_slot_objects(), slot_usable=False, slot_confirmed_flush_lsn=None)
    initial = pg.pg_replication_objects_blocker(
        lost, ["app.t"], resumes_from_slot=False, connector_autocreates_publication=True,
        snapshots=True,
    )
    never = pg.pg_replication_objects_blocker(lost, ["app.t"], resumes_from_slot=True)
    assert initial[0] == "A replication slot of that name is not usable for CDC"
    assert never[0] == "CDC's replication slot does not exist on the source"

    never_started = _route_halves(lang)[0]
    assert _has(lang, never_started, "**Re-snapshot every table instead**"), lang
    assert _fact(lang, never_started, "offered-only-when"), lang
    assert _has(lang, never_started, f"*{lead}*"), lang
    for chapter, phrases in _OFFERED_WHEN.items():
        assert _has(lang, _page(lang, _PAGE[chapter]), phrases[lang]), (lang, chapter)
    assert not _stale(lang, (_STALE_OFFERED[lang], _STALE_RESNAPSHOT[lang])), lang
