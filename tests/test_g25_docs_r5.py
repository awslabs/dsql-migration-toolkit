# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 5 (docs): the manual, in English, Korean and Japanese alike, states the PostgreSQL
CDC route rule as the code implements it (:func:`dsql_migrator.core.cdc_pg_slot.pg_cdc_route`,
the one function every surface takes its route from).

* The route table first: for each stack state (never started; stopped; rolled back, in every
  ``UPDATE_ROLLBACK_*`` status; connectors in place), migration type and block kind, the code's
  route is read through the real ``_pg_block_route`` and the manual must name that route.
* f0: a STOPPED stack gets the one route a rolled-back stack gets, new topics first -- Stop CDC
  removes the connectors without draining them, so the topics may hold the sink's unapplied
  tail, which a Full Load re-run on them would have applied over the reloaded tables. Chapter 4
  named "Re-run Full Load with Drop & reload" first on a stopped stack, said a Start after Stop
  CDC continues "with no gap", and asked for new topics before a lost slot's Full Load re-run
  only after a rolled-back Start (chapter 1 asked for the re-run alone); the FAQ's
  restart-in-place tip promised a resume "where it left off" with no word of the topics.
* f1: after a rolled-back Start, ``initial`` continues without a snapshot only from a recorded
  position whose initial snapshot had completed; chapter 4 said Debezium snapshots "only if no
  position was ever recorded".
* f2: on a rolled-back stack the start-point card resumes like the Start card (*Starting again
  after a rolled-back Start*), offering no start point or re-snapshot there.
* f3: the route named after a withheld re-snapshot is the one that works for that refusal: a
  Full Load re-run (with CDC only, after a switch to Full load + CDC) after an inheritance
  refusal, and a step after its own fix for a slot of that name it cannot use (with CDC only,
  Start CDC again); chapters 4 and 13 named the Full Load re-run after every refusal.
* f4: a publication that misses a selected table on a stack never started ends its block with
  its route (Full load + CDC: the Full Load re-run adds the table; CDC only: Delete CDC
  infrastructure), and the Start CDC job's refusal names the routes too; chapter 4 said that
  block "is shown alone".

Each check reads the code first, then the text with the hard wrap undone (in ja, with no
whitespace at all). Each fails on the manual before this round.
"""

from __future__ import annotations

import inspect
import re
from types import SimpleNamespace

import pytest

from tests.test_g25_docs import _CDC_UI, _PAGE, LANGS, _flat, _function, _has, _page
from tests.test_g25_docs_r4 import _stale
from tests.test_g25_docs_r4d import _INHERITANCE_FIRST, ROLLBACK_STATUSES, _new_topics_first

_DELETE = "Delete CDC infrastructure and deploy it again"
_RERUN = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"

# -- the manual's words for each route of the rule ------------------------------------------

_MANUAL = {
    # A stack started before: why, and its one route (Full load + CDC; CDC only).
    "started-why": {
        "en": "the CDC topics may still hold changes that no sink applied: Stop CDC and a "
        "rolled-back Start remove the connectors without draining them, and this stack's sink "
        "resumes from its last commit, so a Full Load re-run on the old topics would have those "
        "changes applied over the reloaded tables",
        "ko": "CDC 토픽에 아직 어떤 싱크도 적용하지 않은 변경이 남아 있을 수 있습니다: Stop CDC와 롤백된 "
        "Start는 커넥터를 드레인하지 않고 제거하고 이 스택의 싱크는 마지막 커밋에서 재개하므로, 이전 토픽을 "
        "둔 채로 Full Load를 다시 실행하면 그 변경이 다시 적재한 테이블 위에 적용됩니다",
        "ja": "CDC トピックに、まだどの sink も適用していない変更が残っている可能性があります: Stop CDC と"
        "ロールバックされた Start はコネクタをドレインせずに削除し、このスタックの sink は最後のコミットから"
        "再開するため、古いトピックのまま Full Load を再実行すると、それらの変更が再ロードしたテーブルの上に"
        "適用されます",
    },
    "started-full-load": {
        "en": "names one route, new topics first: Delete CDC infrastructure and deploy it again "
        "(the new stack starts with empty topics), then Re-run Full Load with **Drop & reload** "
        "(it creates the slot again at its own snapshot point), then Start CDC",
        "ko": "경로를 하나만, 새 토픽 먼저 알려 줍니다: Delete CDC infrastructure 후 다시 배포(새 스택은 빈 "
        "토픽으로 시작), 그다음 Re-run Full Load를 **Drop & reload**로(슬롯을 자신의 스냅샷 지점에 다시 "
        "만듦), 그다음 Start CDC",
        "ja": "方法を 1 つだけ、新しいトピックを先に示します: Delete CDC infrastructure して再デプロイ (新しい"
        "スタックは空のトピックで始まります)、次に **Drop & reload** で Re-run Full Load (スロットが自身の"
        "スナップショット位置に作り直されます)、次に Start CDC",
    },
    "started-cdc-only": {
        "en": "with CDC only, Delete CDC infrastructure and deploy it again, then Start CDC on the "
        "new stack, which snapshots every selected table (a row deleted on the source in between "
        "stays on Aurora DSQL; Validation reports it as an EXTRA row)",
        "ko": "CDC only에서는 Delete CDC infrastructure 후 다시 배포한 뒤 새 스택에서 Start CDC 하며, 그 "
        "Start가 선택한 모든 테이블을 스냅샷합니다(그 사이에 소스에서 삭제된 행은 Aurora DSQL에 남고, "
        "Validation이 EXTRA 행으로 보고합니다)",
        "ja": "CDC only では Delete CDC infrastructure して再デプロイしてから新しいスタックで Start CDC し、"
        "その Start が選択したすべてのテーブルをスナップショットします (その間にソースで削除された行は Aurora "
        "DSQL に残り、Validation が EXTRA の行として報告します)",
    },
    "started-after-fix": {
        "en": "The block for a slot of that name it cannot use names the route as a step after its "
        "own fix (drop that slot, or use a different CDC stack name)",
        "ko": "그 이름의 슬롯을 쓸 수 없다는 차단은 그 경로를 자체 해결책(그 슬롯을 삭제하거나 다른 CDC 스택 "
        "이름을 사용) 다음 단계로 알려 줍니다",
        "ja": "その名前のスロットを使用できないというブロックは、その方法を自身の解決策 (そのスロットを削除する"
        "か、別の CDC スタック名を使う) の次の手順として示します",
    },
    "started-no-remedy": {
        "en": "No **Re-snapshot every table instead** is offered on such a stack",
        "ko": "그런 스택에는 **Re-snapshot every table instead**를 제시하지 않습니다",
        "ja": "そのようなスタックには **Re-snapshot every table instead** は提示されません",
    },
    # A stack never started.
    "never-offered": {
        "en": "It is offered only on a stack whose connectors were never started, for a missing "
        "publication, a missing slot or a slot that is not at the Full Load's start point",
        "ko": "그 방법은 커넥터를 시작한 적 없는 스택에만, publication이 없거나 슬롯이 없거나 슬롯이 Full "
        "Load의 시작 지점에 있지 않을 때만 제시됩니다",
        "ja": "この方法が提示されるのは、コネクタを開始したことのないスタックで、publication がない、スロットが"
        "ない、またはスロットが Full Load の開始位置にない場合だけです",
    },
    "never-job-both-ways": {
        "en": "for a missing publication, a missing slot or a slot that is not at the Full Load's "
        "start point it names both ways out: Re-snapshot every table instead (the Start CDC "
        "dialog offers it when the Start that re-snapshots passes its own checks), or Re-run Full "
        "Load with **Drop & reload**, then Start CDC (with CDC only, after switching the "
        "migration type to Full load + CDC)",
        "ko": "publication이 없거나 슬롯이 없거나 슬롯이 Full Load의 시작 지점에 있지 않다는 거부는 두 가지 "
        "길을 알려 줍니다: Re-snapshot every table instead(다시 스냅샷하는 Start가 자체 검사를 통과하면 "
        "Start CDC 대화 상자가 제시), 또는 Re-run Full Load를 **Drop & reload**로 실행한 뒤 Start CDC(CDC "
        "only에서는 마이그레이션 유형을 Full load + CDC로 바꾼 뒤)",
        "ja": "publication がない、スロットがない、またはスロットが Full Load の開始位置にないという拒否は "
        "2 つの方法を示します: Re-snapshot every table instead (再スナップショットする Start が自身のチェック"
        "を通過すれば Start CDC のダイアログが提示します)、または **Drop & reload** で Re-run Full Load "
        "してから Start CDC (CDC only では、マイグレーションの種類を Full load + CDC に切り替えてから)",
    },
    "never-job-same-route": {
        "en": "On a stack never started, its refusal for a publication that misses a selected "
        "table, or for a slot of that name it cannot use, ends with the same route",
        "ko": "커넥터를 시작한 적 없는 스택에서 publication에 선택한 테이블이 빠졌다거나 그 이름의 슬롯을 쓸 "
        "수 없다는 그 작업의 거부는 같은 경로로 끝나고",
        "ja": "コネクタを開始したことのないスタックで publication に選択したテーブルが含まれていない、またはその"
        "名前のスロットを使用できないというそのジョブの拒否は同じ方法で終わり",
    },
    "never-inheritance-full-load": {
        "en": "After an inheritance refusal (or a failed read of them), that is Re-run Full Load "
        "with **Drop & reload** instead, which creates the slot again at its own snapshot point, "
        "so the Start CDC after it takes no snapshot",
        "ko": "상속 때문에(또는 그 읽기에 실패해) 거부되었다면 대신 Re-run Full Load를 **Drop & reload**로 "
        "실행하세요. 슬롯을 자신의 스냅샷 지점에 다시 만들므로 그 뒤의 Start CDC는 스냅샷하지 않습니다",
        "ja": "継承のため (またはその読み取りに失敗したため) の拒否なら、代わりに **Drop & reload** で "
        "Re-run Full Load してください。スロットが自身のスナップショット位置に作り直されるため、その後の "
        "Start CDC はスナップショットしません",
    },
    "never-inheritance-cdc-only": {
        "en": "with CDC only, which has no Full Load step, switch the migration type to Full "
        "load + CDC first",
        "ko": "(Full Load 단계가 없는 CDC only에서는 먼저 마이그레이션 유형을 Full load + CDC로 바꿈)",
        "ja": "(Full Load ステップのない CDC only では、先にマイグレーションの種類を Full load + CDC に"
        "切り替えます)",
    },
    "never-unusable-full-load": {
        "en": "After the refusal of a slot of that name it cannot use, it is a step after that "
        "refusal's own fix (drop that slot, or use a different CDC stack name): then Re-run Full "
        "Load with **Drop & reload**, then Start CDC",
        "ko": "그 이름의 슬롯을 쓸 수 없어 거부되었다면, 그 거부의 자체 해결책(그 슬롯을 삭제하거나 다른 CDC "
        "스택 이름을 사용) 다음 단계로 Re-run Full Load를 **Drop & reload**로 실행한 뒤 Start CDC 하세요",
        "ja": "その名前のスロットを使用できないための拒否なら、その拒否自身の解決策 (そのスロットを削除するか、"
        "別の CDC スタック名を使う) の次の手順として、**Drop & reload** で Re-run Full Load してから Start "
        "CDC してください",
    },
    "never-unusable-cdc-only": {
        "en": "then Start CDC (with CDC only, Start CDC again)",
        "ko": "Start CDC 하세요(CDC only에서는 Start CDC를 다시 실행)",
        "ja": "Start CDC してください (CDC only では Start CDC をもう一度実行します)",
    },
    "never-coverage-full-load": {
        "en": "A publication that misses a selected table is refused for its own reason (the Start "
        "that re-snapshots grades the publication's coverage too), so its block ends with its "
        "route: Re-run Full Load with **Drop & reload**, which adds each missing selected table to "
        "the publication and creates the slot again at its own snapshot point, then Start CDC",
        "ko": "publication에 선택한 테이블이 빠졌다면 다시 스냅샷하는 Start도 같은 이유로 거부되므로(그 검사도 "
        "publication의 포함 범위를 판정함), 그 차단은 자신의 경로로 끝납니다: Re-run Full Load를 **Drop & "
        "reload**로 실행하면 빠진 선택 테이블을 publication에 추가하고 슬롯을 자신의 스냅샷 지점에 다시 "
        "만들며, 그 뒤 Start CDC",
        "ja": "publication に選択したテーブルが含まれていない場合は、再スナップショットする Start も同じ理由で"
        "拒否されるため (そのチェックも publication の対象範囲を判定します)、そのブロックは自身の方法で終わり"
        "ます: **Drop & reload** で Re-run Full Load すると、含まれていない選択したテーブルを publication に"
        "追加し、スロットを自身のスナップショット位置に作り直します。その後に Start CDC します",
    },
    "never-coverage-cdc-only": {
        "en": "(with CDC only, Delete CDC infrastructure and deploy it again: it drops the "
        "publication, so the new stack's Start CDC can create one over the selected tables)",
        "ko": "(CDC only에서는 Delete CDC infrastructure 후 다시 배포: publication을 삭제하므로 새 스택의 "
        "Start CDC가 선택한 테이블로 publication을 만들 수 있습니다)",
        "ja": "(CDC only では Delete CDC infrastructure して再デプロイ: publication を削除するため、新しい"
        "スタックの Start CDC が選択したテーブルで publication を作成できます)",
    },
}


def _cdc4(lang: str) -> str:
    return _page(lang, _PAGE["04"])


def _paragraph(lang: str, start: dict, end: dict) -> str:
    """Chapter 4's flattened text from ``start[lang]`` up to ``end[lang]``."""
    cdc = _cdc4(lang)
    head = cdc.index(_flat(lang, start[lang]).strip())
    return cdc[head : cdc.index(_flat(lang, end[lang]).strip(), head)]


_STARTED_HEAD = {
    "en": "**On a stack whose connectors were started before**",
    "ko": "**커넥터를 전에 시작한 스택에서는**",
    "ja": "**以前にコネクタを開始したスタックでは**",
}
_ROLLED_BACK_HEAD = {
    "en": "**After a Start that CloudFormation rolled back**",
    "ko": "**CloudFormation이 Start를 롤백한 뒤**",
    "ja": "**CloudFormation が Start をロールバックした後**",
}
_NEVER_HEAD = {
    "en": "Before a block offers **Re-snapshot every table instead**",
    "ko": "차단이 **Re-snapshot every table instead**를 제시하기 전에",
    "ja": "ブロックが **Re-snapshot every table instead** を提示する前に",
}
_NEVER_END = {
    "en": "If those checks cannot read the source, the remedy stays",
    "ko": "그 검사가 소스를 읽지 못하면 그 방법은 그대로 남고",
    "ja": "それらのチェックがソースを読めない場合、その方法は残り",
}


# -- the code's states, fed to the real route derivation ------------------------------------


def _state(stack: str, cdc_only: bool):
    """A migration state the stack-phase probe could have left: ``stack`` names it."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    fields = {
        "never-started": dict(cdc_has_committed_offset=False, cdc_stack_phase="infra",
                              cdc_stack_phase_status="UPDATE_COMPLETE"),
        "stopped": dict(cdc_has_committed_offset=True, cdc_stack_phase="infra",
                        cdc_stack_phase_status="UPDATE_COMPLETE"),
        "connectors-in-place": dict(cdc_has_committed_offset=False, cdc_stack_phase="running",
                                    cdc_stack_phase_status="UPDATE_COMPLETE"),
        "listed-connectors": dict(cdc_has_committed_offset=False, cdc_stack_phase="unstable",
                                  cdc_stack_phase_status="UPDATE_IN_PROGRESS",
                                  cdc_connector_names=["stk-debezium-source"]),
    }.get(stack)
    if fields is None:  # a rollback status
        fields = dict(cdc_has_committed_offset=False, cdc_stack_phase="infra",
                      cdc_stack_phase_status=stack)
    return SimpleNamespace(
        migration_type=MigrationType.CDC_ONLY if cdc_only else MigrationType.FULL_LOAD_AND_CDC,
        **fields,
    )


_STARTED_STATES = ("stopped", "connectors-in-place", "listed-connectors") + ROLLBACK_STATUSES


def _route_table():
    """(stack, cdc_only, block, resnapshot) -> (code words in the route, manual keys). The
    reachable cases: a stack never started gets the remedy's standing the dialog can judge (a
    re-snapshot Start never sends ``never``, so its refusal is never a missing publication, a
    missing slot or a moved slot), and the Start CDC job's (not judged: ``None``)."""
    from dsql_migrator.core import cdc_pg_slot as pg

    lead = ("its CDC topics may still hold changes that no sink applied (Stop CDC and a "
            "rolled-back Start remove the connectors without draining them)")
    full_load = (f"use {_DELETE} (the new stack starts with empty topics), then {_RERUN} (it "
                 "creates the slot again at its own snapshot point), then Start CDC.")
    cdc_only = (f"use {_DELETE} (the new stack starts with empty topics), then Start CDC on the "
                "new stack, which snapshots every selected table (a row deleted on the source in "
                "between stays on the target; Validation reports it as an EXTRA row).")
    routed = (pg.SLOT_MISSING_HEADER, pg.PUBLICATION_MISSING_HEADER,
              pg.PUBLICATION_COVERAGE_HEADER, pg.SLOT_UNUSABLE_HEADER)
    rows = []
    for stack in _STARTED_STATES:
        for only in (False, True):
            words = cdc_only if only else full_load
            keys = ("started-why", "started-cdc-only" if only else "started-full-load",
                    "started-no-remedy")
            for block in routed:
                head = ("After that fix, the gapless route: " if block == pg.SLOT_UNUSABLE_HEADER
                        else "The gapless route: ")
                extra = ("started-after-fix",) if block == pg.SLOT_UNUSABLE_HEADER else ()
                rows.append((stack, only, block, None, (lead, head + words), keys + extra))
            rows.append((stack, only, pg.INHERITANCE_SNAPSHOT_HEADER, None,
                         (f"So before that Full Load, use {_DELETE} (the new stack starts with "
                          "empty topics).",), ("inheritance-first",)))
    for only in (False, True):
        resumable = (pg.SLOT_POSITION_HEADER, pg.SLOT_MISSING_HEADER,
                     pg.PUBLICATION_MISSING_HEADER)
        for block in resumable:
            rows.append(("never-started", only, block, pg.RESNAPSHOT_OFFERED, ("",),
                         ("never-offered",)))
            job = ("Re-snapshot every table instead (the Start CDC dialog offers it when the "
                   "Start that re-snapshots passes its own checks), or "
                   + ("switch the migration type to Full load + CDC, then " if only else "")
                   + _RERUN)
            rows.append(("never-started", only, block, None, (job,), ("never-job-both-ways",)))
        refusals = {
            pg.INHERITANCE_SNAPSHOT_HEADER: (
                ("Or switch the migration type to Full load + CDC, then " + _RERUN + ": it "
                 "creates the slot again at its own snapshot point, and the Start CDC after it "
                 "takes no snapshot.",)
                if only else
                (_RERUN + " instead: it creates the slot again at its own snapshot point, and "
                 "the Start CDC after it takes no snapshot.",),
                ("never-inheritance-cdc-only",) if only else ("never-inheritance-full-load",),
            ),
            pg.SLOT_UNUSABLE_HEADER: (
                ("Then Start CDC again.",) if only else
                (f"Then {_RERUN} (it creates the slot again at its own snapshot point), then "
                 "Start CDC.",),
                ("never-unusable-cdc-only",) if only else ("never-unusable-full-load",),
            ),
            pg.PUBLICATION_COVERAGE_HEADER: (
                ("Use Delete CDC infrastructure and deploy it again: it drops the publication, "
                 "so the new stack's Start CDC can create one over the selected tables.",)
                if only else
                (f"{_RERUN}, which adds each missing selected table to the publication and "
                 "creates the slot again at its own snapshot point, then Start CDC.",),
                ("never-coverage-cdc-only",) if only else ("never-coverage-full-load",),
            ),
        }
        refusals[pg.INHERITANCE_UNREADABLE_HEADER] = refusals[pg.INHERITANCE_SNAPSHOT_HEADER]
        for block, (words, keys) in refusals.items():
            rows.append(("never-started", only, block, pg.RESNAPSHOT_REFUSED, words, keys))
            if block in (pg.SLOT_UNUSABLE_HEADER, pg.PUBLICATION_COVERAGE_HEADER):
                # The job's refusal names the same route (the remedy not judged there).
                rows.append(("never-started", only, block, None, words,
                             keys + ("never-job-same-route",)))
    return rows


_ROWS = _route_table()


@pytest.mark.parametrize("lang", LANGS)
@pytest.mark.parametrize(
    "stack, cdc_only, block, resnapshot, words, keys",
    _ROWS,
    ids=[f"{r[0]}-{'cdc-only' if r[1] else 'full-load-and-cdc'}-{r[2][:28]}-{r[3]}"
         for r in _ROWS],
)
def test_the_manual_names_the_route_rules_route(lang, stack, cdc_only, block, resnapshot,
                                                words, keys) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the one derivation every surface uses, on that state.
    state = _state(stack, cdc_only)
    assert cu._pg_stack_started(state) is (stack != "never-started"), stack
    route = cu._pg_block_route(state, block, resnapshot=resnapshot)
    for word in words:
        assert (route == "") if word == "" else (word in route), (route, word)
    if stack != "never-started" and "Re-run Full Load" in route:
        assert route.index(_DELETE) < route.index("Re-run Full Load"), route
    if cdc_only and "Full Load step" in route:
        assert "switch the migration type to full load + cdc" in route.lower(), route

    # Then the manual: that route, in that stack's paragraph.
    for key in keys:
        if key == "inheritance-first":
            phrase = _INHERITANCE_FIRST[lang].format(header=pg.INHERITANCE_SNAPSHOT_HEADER)
            text = _paragraph(lang, _STARTED_HEAD, _ROLLED_BACK_HEAD)
        elif key.startswith("started"):
            phrase, text = _MANUAL[key][lang], _paragraph(lang, _STARTED_HEAD, _ROLLED_BACK_HEAD)
        else:
            phrase, text = _MANUAL[key][lang], _paragraph(lang, _NEVER_HEAD, _NEVER_END)
        assert _has(lang, text, phrase), (lang, key, phrase)


@pytest.mark.parametrize("lang", LANGS)
def test_a_started_stacks_paragraph_names_new_topics_before_any_full_load_rerun(lang) -> None:
    # Every sentence of the started stack's paragraph that names Re-run Full Load names Delete
    # CDC infrastructure before it; the never-started routes are in their own paragraph.
    started = _paragraph(lang, _STARTED_HEAD, _ROLLED_BACK_HEAD)
    assert _new_topics_first(lang, started), lang
    assert _flat(lang, "Re-snapshot every table instead (").strip() not in started, lang


# -- f0: a stopped stack -- one route, and a resume gapless only within the topics' retention --

_STOPPED_RESUME = {
    "en": (
        "Stop CDC removes the connectors without draining them, so the changes the sink had not "
        "applied by then reach Aurora DSQL only from the CDC topics: such a Start is gapless only "
        "while the topics still retain them, which Start CDC does not check",
        "Once their retention period on MSK has passed, it would skip them, so the Start CDC card "
        "(*Ready to resume CDC*) and the start-point card (*Resuming from this stack's "
        "replication slot*) name the route to take instead: that of a stack whose connectors were "
        "started before (new topics first; see the PostgreSQL note below)",
    ),
    "ko": (
        "Stop CDC는 커넥터를 드레인하지 않고 제거하므로, 그때까지 싱크가 적용하지 않은 변경은 CDC 토픽에서만 "
        "Aurora DSQL에 도달합니다: 그런 Start에 빈틈이 없는 것은 토픽이 그 변경을 아직 보존하는 동안뿐이며, "
        "Start CDC는 이를 확인하지 않습니다",
        "MSK에서 토픽의 보존 기간이 지나면 그 Start는 그 변경을 건너뛰므로, Start CDC 카드(*Ready to resume "
        "CDC*)와 시작 지점 카드(*Resuming from this stack's replication slot*)는 대신 택할 경로로 커넥터를 "
        "전에 시작한 스택의 경로(새 토픽 먼저. 아래 PostgreSQL 설명 참고)를 알려 줍니다",
    ),
    "ja": (
        "Stop CDC はコネクタをドレインせずに削除するため、その時点で sink が適用していなかった変更は CDC "
        "トピックからしか Aurora DSQL に届きません: そのような Start にギャップがないのはトピックがまだその"
        "変更を保持している間だけで、Start CDC はそれを確認しません",
        "MSK でトピックの保持期間が過ぎるとその Start はそれらの変更をスキップするため、Start CDC のカード "
        "(*Ready to resume CDC*) と開始位置のカード (*Resuming from this stack's replication slot*) は、"
        "代わりに取る方法として、以前にコネクタを開始したスタックの方法 (新しいトピックが先。下記の "
        "PostgreSQL の説明を参照) を示します",
    ),
}
# G25 round 8 (rule E): the re-run that takes the fresh watermark is a Drop & reload.
_LOST_SLOT = {
    "04": {
        "en": "`wal_status='lost'` (slot invalidated), gapless resume from it is broken, and Start "
        "CDC blocks and names the way out (below): with Full load + CDC, re-run Full Load with "
        "**Drop & reload** to capture a fresh watermark, after Delete CDC infrastructure and a "
        "new deploy if the stack's connectors were started before",
        "ko": "`wal_status='lost'`(슬롯 무효화)를 보고하면 그 슬롯에서의 무손실 재개가 깨지고, Start CDC가 "
        "차단하며 해결 경로를 알려 줍니다(아래): Full load + CDC에서는 새 워터마크를 얻기 위해 **Drop & "
        "reload**로 Full Load를 다시 실행하되, 스택의 커넥터를 전에 시작했다면 먼저 Delete CDC "
        "infrastructure 후 다시 배포합니다",
        "ja": "`wal_status='lost'` (スロット無効化) を報告すると、そのスロットからのギャップのない再開が"
        "壊れ、Start CDC がブロックして解決方法を示します (下記): Full load + CDC では新しいウォーター"
        "マークを取得するために **Drop & reload** で Full Load を再実行し、スタックのコネクタを以前に開始"
        "していた場合は、その前に Delete CDC infrastructure して再デプロイします",
    },
    "01": {
        "en": "`wal_status='lost'` means the slot was invalidated → gapless resume is broken → "
        "Start CDC blocks and names the way out ([§4.3](04-cdc-and-dsql-constraints.md)): with "
        "Full load + CDC, re-run Full Load with **Drop & reload**, after Delete CDC "
        "infrastructure and a new deploy if the stack's connectors were started before",
        "ko": "`wal_status='lost'`는 슬롯이 무효화됐다는 뜻으로 → gapless 재개가 깨졌으니 → Start CDC가 "
        "차단하며 해결 경로를 알려 줍니다([§4.3](04-cdc-and-dsql-constraints.md)): Full load + CDC에서는 "
        "**Drop & reload**로 Full Load를 다시 실행하되, 스택의 커넥터를 전에 시작했다면 먼저 Delete CDC "
        "infrastructure 후 다시 배포합니다",
        "ja": "`wal_status='lost'` はスロットが無効化されたことを意味します → gapless な再開が壊れる → "
        "Start CDC がブロックして解決方法を示します ([§4.3](04-cdc-and-dsql-constraints.md)): Full load + "
        "CDC では **Drop & reload** で Full Load を再実行します。スタックのコネクタを以前に開始していた"
        "場合は、その前に Delete CDC infrastructure して再デプロイします",
    },
}
# Before this round: an unconditional "no gap" after Stop CDC, and a lost slot's Full Load
# re-run with no new topics first on a stopped stack.
_STALE_STOPPED = {
    "en": (
        "has streamed since), with no gap; with `initial`",
        "→ gapless resume is broken → re-run Full Load.",
    ),
    "ko": (
        "Full Load의 시작 지점)에서 빈틈 없이 이어 가지만",
        "→ gapless 재개가 깨졌으니 → Full Load를 다시 실행해야 합니다.",
    ),
    "ja": (
        "Full Load の開始位置) からギャップなく続けます",
        "→ gapless な再開が壊れる → Full Load を再実行してください。",
    ),
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_stopped_stack_gets_the_one_route_and_a_resume_bounded_by_the_topics(lang) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: a stopped stack's block route is a rolled-back one's, new topics first.
    for only in (False, True):
        stopped = cu._pg_block_route(_state("stopped", only), pg.SLOT_MISSING_HEADER)
        rolled = cu._pg_block_route(_state("UPDATE_ROLLBACK_COMPLETE", only),
                                    pg.SLOT_MISSING_HEADER)
        assert stopped == rolled and "Two gapless routes" not in stopped
        assert stopped.index(_DELETE) < (stopped.index("Re-run Full Load")
                                         if "Re-run Full Load" in stopped else len(stopped))
    # ...and its Start CDC / start-point card bounds the resume by the topics' retention.
    for never in (True, False):
        card = cu._pg_resume_continues("s", snapshot_never=never)
        assert "stopping CDC removed only the connectors (without draining them)" in card
        assert ("Changes the sink had not applied when CDC stopped are applied from the CDC "
                "topics, so this is gapless only while the topics still retain them: once their "
                "retention period on MSK has passed, a Start here would skip them, so instead "
                f"{pg.pg_cdc_route(None, started=True, cdc_only=False)}.") in card
        assert "No gap either way" not in card and "with no gap" not in card
    point = _function(_CDC_UI, "_render_cdc_start_point_card")
    assert '"Resuming from this stack\'s replication slot"' in point
    assert 'header="Ready to start CDC again" if _rolled_back else "Ready to resume CDC",' in (
        _function(_CDC_UI, "_render_cdc_start_button")
    )
    # A lost slot is not a usable one, so a Start that resumes from it is blocked.
    from dsql_migrator.core.cdc_pg_slot import read_pg_replication_objects

    assert "coalesce(wal_status, 'reserved') <> 'lost'" in inspect.getsource(
        read_pg_replication_objects
    )

    cdc = _cdc4(lang)
    for phrase in _STOPPED_RESUME[lang]:
        assert _has(lang, cdc, phrase), (lang, phrase)
    assert _has(lang, cdc, _LOST_SLOT["04"][lang]), lang
    assert _has(lang, _page(lang, "01-setup.md"), _LOST_SLOT["01"][lang]), lang
    assert not _stale(lang, _STALE_STOPPED[lang]), lang


_FAQ_RESTART = {
    "en": "Stop CDC does not drain the sink, though: the changes it had not applied by then are "
    "applied from the CDC topics, so that resume is gapless only while the topics still retain "
    "them (their retention period on MSK; for a PostgreSQL source, "
    "[§4.3](04-cdc-and-dsql-constraints.md) names the route once it has passed)",
    "ko": "다만 Stop CDC는 싱크를 드레인하지 않으므로, 그때까지 싱크가 적용하지 않은 변경은 CDC 토픽에서 "
    "적용됩니다: 그 재개에 빈틈이 없는 것은 토픽이 그 변경을 아직 보존하는 동안(MSK에서의 보존 기간)"
    "뿐입니다. PostgreSQL 소스에서 그 기간이 지난 뒤의 경로는 [§4.3](04-cdc-and-dsql-constraints.md)에 "
    "있습니다",
    "ja": "ただし Stop CDC は sink をドレインしないため、その時点で sink が適用していなかった変更は CDC "
    "トピックから適用されます: その再開にギャップがないのは、トピックがまだその変更を保持している間 (MSK "
    "での保持期間) だけです。PostgreSQL ソースでその期間が過ぎた後の方法は "
    "[§4.3](04-cdc-and-dsql-constraints.md) にあります",
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_faqs_restart_in_place_tip_states_the_topics_condition(lang) -> None:
    # The code first: Stop CDC blanks the bootstrap servers so CloudFormation deletes both
    # connectors in one update (no drain), for either source; the resumed PostgreSQL Start's
    # card states the bound.
    from dsql_migrator.core.cdc_deployer import run_cdc_stop
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    doc = " ".join((run_cdc_stop.__doc__ or "").split())
    assert "Stop CDC by blanking ``MskBootstrapServers`` so CFN deletes both connectors." in doc
    assert "drain" not in inspect.getsource(run_cdc_stop).lower()
    assert "gapless only while the topics still retain them" in cu._pg_resume_continues(
        "s", snapshot_never=True
    )

    assert _has(lang, _page(lang, "11-customer-faq.md"), _FAQ_RESTART[lang]), lang


# -- f1: after a rolled-back Start, Debezium's own snapshot rule -----------------------------

_SNAPSHOT_RULE = {
    "en": "With `initial`, though, Debezium continues without a snapshot only from a recorded "
    "position whose initial snapshot had completed: if that Start (or the earlier run) had not "
    "completed its initial snapshot, or nothing was recorded, it snapshots every selected table "
    "first, then streams from the slot",
    "ko": "다만 `initial`에서 Debezium이 스냅샷 없이 이어 가는 것은 초기 스냅샷을 끝낸, 기록된 위치에서뿐"
    "입니다: 그 Start(또는 이전 실행)가 초기 스냅샷을 끝내지 못했거나 기록된 것이 없으면, 먼저 선택한 모든 "
    "테이블을 스냅샷한 뒤 슬롯에서 스트리밍합니다",
    "ja": "ただし `initial` で Debezium がスナップショットなしで続けるのは、初期スナップショットを完了した、"
    "記録された位置からだけです: その Start (または以前の実行) が初期スナップショットを完了していなかったか、"
    "何も記録されていなければ、先に選択したすべてのテーブルをスナップショットしてから、スロットから"
    "ストリーミングします",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_start_promises_no_snapshot_outcome_debezium_does_not(lang) -> None:
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the cards and the start line, initial (Manual included) and never.
    initial = cu._pg_resume_continues("s", snapshot_never=False, rolled_back=True)
    assert ("If that Start (or the earlier run) had not completed its initial snapshot, or "
            "nothing was recorded, Debezium snapshots every selected table first, then streams "
            "from replication slot s.") in initial
    assert "with no snapshot" not in initial
    never = cu._pg_resume_continues("s", snapshot_never=True, rolled_back=True)
    assert "with no snapshot (snapshot.mode=never)" in never
    watermark = SimpleNamespace(binlog_file=None, binlog_position=None, gtid_executed=None,
                                server_uuid=None, wal_lsn="0/1000000", slot_name="s",
                                publication_name="p")
    line = cu._cdc_start_detail("stk", watermark, mode="manual", source_type=SourceType.POSTGRES,
                                rolled_back=True, force_snapshot=True, snapshot_mode="initial")
    assert "if that Start (or the earlier run) had not completed its initial snapshot, or " \
           "nothing was recorded, Debezium snapshots every selected table first" in line
    assert "Manual's full snapshot runs only if nothing was recorded on this stack, or the run " \
           "that recorded its position had not completed its initial snapshot" in line

    rolled = _paragraph(lang, _ROLLED_BACK_HEAD, _NEVER_HEAD)
    assert _has(lang, rolled, _SNAPSHOT_RULE[lang]), lang


# -- f2: the start-point card on a rolled-back stack resumes like the Start card -------------

_START_POINT_CARD = {
    "en": "the start-point card above it (*Starting again after a rolled-back Start*, which offers "
    "no start point or re-snapshot there)",
    "ko": "그 위의 시작 지점 카드(*Starting again after a rolled-back Start*. 그곳에서는 시작 지점도 다시 "
    "스냅샷도 제시하지 않음)",
    "ja": "その上の開始位置のカード (*Starting again after a rolled-back Start*。そこでは開始位置も再"
    "スナップショットも提示しません)",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_rolled_back_stacks_start_point_card_is_named_as_it_reads(lang) -> None:
    # The code first: the card takes the resume branch on a rolled-back PostgreSQL stack, under
    # its own header, and returns before the Automatic/Manual choice and the re-snapshot notice.
    card = _function(_CDC_UI, "_render_cdc_start_point_card")
    assert "pg_rolled_back = is_pg and _pg_start_rolled_back(migration_state)" in card
    assert "if (resumes_from_offset or pg_rolled_back) and not locked:" in card
    assert '"Starting again after a rolled-back Start"' in card
    assert 'tone="success" if (_never and not pg_rolled_back) else "info",' in card
    assert '+ " No start point or Full Load watermark is needed."' in card
    branch = card[card.index("if (resumes_from_offset or pg_rolled_back) and not locked:"):]
    # The branch's own return (12 spaces) comes before the PostgreSQL choice below it (8).
    assert branch.index("\n            return\n") < branch.index("\n        if is_pg:\n")

    rolled = _paragraph(lang, _ROLLED_BACK_HEAD, _NEVER_HEAD)
    assert _has(lang, rolled, _START_POINT_CARD[lang]), lang


# -- f3: after a withheld re-snapshot, the route that works for that refusal -----------------

_CH13_UNUSABLE = {
    "en": "after the refusal of a slot of that name it cannot use, drop that slot or use a "
    "different CDC stack name, then Re-run Full Load with \"Drop & reload\" and Start CDC (with "
    "CDC only, Start CDC again)",
    "ko": "그 이름의 슬롯을 쓸 수 없어 거부되었다면 그 슬롯을 삭제하거나 다른 CDC 스택 이름을 쓴 뒤 "
    "\"Drop & reload\"로 Re-run Full Load를 실행하고 Start CDC 하세요(CDC only에서는 Start CDC를 다시 "
    "실행)",
    "ja": "その名前のスロットを使用できないための拒否なら、そのスロットを削除するか別の CDC スタック名を"
    "使ってから、「Drop & reload」で Re-run Full Load して Start CDC してください (CDC only では "
    "Start CDC をもう一度実行します)",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_withheld_re_snapshot_names_the_route_that_works_for_its_refusal(lang) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: the withheld notice follows the refusal with the route rule's route for
    # the refusal's own header -- a step after its fix for a slot it cannot use, and no Full
    # Load step with CDC only before the type switch.
    dialog = inspect.getsource(cu._open_cdc_start_dialog)
    assert 'f"{PG_RESNAPSHOT_REFUSED_LEAD} {pg_resnapshot_block[1]}",' in dialog
    refusal_route = (
        "pg_resnapshot_block[0],\n                                resnapshot=RESNAPSHOT_REFUSED,"
    )
    assert refusal_route in dialog
    unusable = pg.pg_cdc_route(pg.SLOT_UNUSABLE_HEADER, started=False, cdc_only=False,
                               resnapshot=pg.RESNAPSHOT_REFUSED)
    assert unusable.startswith("Then ") and "instead" not in unusable
    assert pg.pg_cdc_route(pg.SLOT_UNUSABLE_HEADER, started=False, cdc_only=True,
                           resnapshot=pg.RESNAPSHOT_REFUSED) == "Then Start CDC again."
    switch = pg.pg_cdc_route(pg.INHERITANCE_SNAPSHOT_HEADER, started=False, cdc_only=True,
                             resnapshot=pg.RESNAPSHOT_REFUSED)
    assert switch.startswith("Or switch the migration type to Full load + CDC, then ")
    # The refusal names its own fix first.
    from dataclasses import replace

    from tests.test_g25_docs_r4 import _slot_objects

    lost = replace(_slot_objects(), slot_usable=False, slot_confirmed_flush_lsn=None)
    refused = pg.pg_replication_objects_blocker(
        lost, ["app.t"], resumes_from_slot=False, connector_autocreates_publication=True,
        snapshots=True,
    )
    assert refused[0] == pg.SLOT_UNUSABLE_HEADER
    assert refused[1].endswith("Drop it, or use a different CDC stack name so the derived slot "
                               "name differs.")

    never = _paragraph(lang, _NEVER_HEAD, _NEVER_END)
    for key in ("never-inheritance-full-load", "never-inheritance-cdc-only",
                "never-unusable-full-load", "never-unusable-cdc-only"):
        assert _has(lang, never, _MANUAL[key][lang]), (lang, key)
    assert _has(lang, _page(lang, _PAGE["13"]), _CH13_UNUSABLE[lang]), lang


# -- f4: a publication that misses a table on a stack never started names its route ----------

_STALE_SHOWN_ALONE = {
    "en": "the block is shown alone",
    "ko": "차단만 보여 주고",
    "ja": "ブロックだけを表示し",
}


@pytest.mark.parametrize("lang", LANGS)
def test_a_never_started_stacks_coverage_block_ends_with_its_route(lang) -> None:
    from dsql_migrator.core import cdc_pg_slot as pg
    from dsql_migrator.ui.data_migration import _cdc_ui as cu

    # The code first: a refusal for the block's own reason appends the route to the block (no
    # second notice); the Start CDC job's refusal on a stack never started appends it too.
    dialog = inspect.getsource(cu._open_cdc_start_dialog)
    same = dialog[dialog.index("if pg_resnapshot_block[0] == pg_objects_block[0]:"):]
    assert same.index("_pg_block_body = _with_route(") < same.index("else:")
    job = _function(_CDC_UI, "_start_cdc_deploy")
    assert ("if not _pg_stack_started(migration_state):" in job
            and "_pg_body = _with_route(_pg_body, _pg_block_route(migration_state, _pg_block[0]))"
            in job)
    for only, words in ((False, "which adds each missing selected table to the publication"),
                        (True, "it drops the publication, so the new stack's Start CDC can "
                               "create one over the selected tables")):
        for resnapshot in (pg.RESNAPSHOT_REFUSED, None):
            route = cu._pg_block_route(_state("never-started", only),
                                       pg.PUBLICATION_COVERAGE_HEADER, resnapshot=resnapshot)
            assert words in route, (only, resnapshot)

    never = _paragraph(lang, _NEVER_HEAD, _NEVER_END)
    for key in ("never-coverage-full-load", "never-coverage-cdc-only", "never-job-same-route",
                "never-job-both-ways"):
        assert _has(lang, never, _MANUAL[key][lang]), (lang, key)
    assert not _stale(lang, (_STALE_SHOWN_ALONE[lang],)), lang


# -- the started stack's paragraph: no route of its own for a stack never started ------------


@pytest.mark.parametrize("lang", LANGS)
def test_the_routes_of_a_stack_never_started_name_no_new_topics_but_where_the_code_does(
    lang,
) -> None:
    # The never-started paragraph names Delete CDC infrastructure only where the route rule
    # does for such a stack: CDC only's route for a publication that misses a table.
    from dsql_migrator.core import cdc_pg_slot as pg

    named = [
        (block, only, resnapshot)
        for block in (pg.SLOT_POSITION_HEADER, pg.SLOT_MISSING_HEADER, pg.SLOT_UNUSABLE_HEADER,
                      pg.PUBLICATION_MISSING_HEADER, pg.PUBLICATION_COVERAGE_HEADER,
                      pg.INHERITANCE_SNAPSHOT_HEADER, pg.INHERITANCE_UNREADABLE_HEADER)
        for only in (False, True)
        for resnapshot in (None, pg.RESNAPSHOT_OFFERED, pg.RESNAPSHOT_REFUSED)
        if "Delete CDC infrastructure" in pg.pg_cdc_route(block, started=False, cdc_only=only,
                                                            resnapshot=resnapshot)
    ]
    assert {(b, o) for b, o, _r in named} == {(pg.PUBLICATION_COVERAGE_HEADER, True)}

    never = _paragraph(lang, _NEVER_HEAD, _NEVER_END)
    delete = _flat(lang, "Delete CDC infrastructure").strip()
    sentences = never.split("。") if lang == "ja" else re.split(r"(?<=[.!?]) ", never)
    (with_delete,) = [s for s in sentences if delete in s]
    assert _has(lang, with_delete, _MANUAL["never-coverage-cdc-only"][lang]), (lang, with_delete)
