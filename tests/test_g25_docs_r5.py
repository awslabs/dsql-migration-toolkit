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
from tests.test_g25_docs_r4d import ROLLBACK_STATUSES, _new_topics_first

_DELETE = "Delete CDC infrastructure and deploy it again"
_RERUN = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"

# -- what the manual owes the reader for each route of the rule -----------------------------
# §4.3 (answer-first) says each route once: the blocked-Start table names a block's own fix, and
# the closing route paragraph has one half for a stack whose connectors were never started and
# one for a stack started before. The route table below keeps its code assertions -- the REAL
# derivation for every stack state, migration type and block kind -- and pins, per key, the FACT
# the page states for it (UI strings are English in every language; the rest are the verbatim
# fragments of ``_F43``). The per-block variants of the CDC only route (a missing publication's
# Delete CDC infrastructure, a Start CDC again after a dropped slot) are in the blocks the UI
# shows, which name their own route.

_STEPS_STARTED = ("Delete CDC infrastructure", "Re-run Full Load", "Start CDC")
_RE_SNAPSHOT = "**Re-snapshot every table instead**"


def _manual_names(lang: str, key: str) -> None:
    never, started = _route_halves(lang)
    section = _section_43(lang)
    route_mark = _F43["route-mark"][lang]
    if key == "started-why":
        assert _fact(lang, started, "topics-hold"), (lang, key)
    elif key == "started-full-load":
        assert _steps_in_order(lang, started, *_STEPS_STARTED), (lang, key)
        assert _has(lang, started, "**Drop & reload**"), (lang, key)
    elif key == "started-cdc-only":
        assert _fact(lang, started, "cdc-only-new-stack"), (lang, key)
        assert _fact(lang, started, "extra-row"), (lang, key)
    elif key == "started-after-fix":
        assert _steps_in_order(
            lang, section, _F43["unusable-cell"][lang], _F43["unusable-fix"][lang], route_mark
        ), (lang, key)
    elif key == "started-no-remedy":
        assert _fact(lang, started, "no-resnapshot"), (lang, key)
        assert not _has(lang, started, _RE_SNAPSHOT), (lang, key)
    elif key == "inheritance-first":
        assert _fact(lang, section, "inheritance-started-first"), (lang, key)
    elif key == "never-offered":
        assert _has(lang, never, _RE_SNAPSHOT) and _fact(lang, never, "offered-only-when"), key
    elif key == "never-job-both-ways":
        assert _has(lang, never, _RE_SNAPSHOT), (lang, key)
        assert _steps_in_order(lang, never, "Re-run Full Load", "Start CDC"), (lang, key)
        assert _has(lang, never, "**Drop & reload**"), (lang, key)
        assert _fact(lang, never, "cdc-only-switch"), (lang, key)
    elif key == "never-job-same-route":
        assert _steps_in_order(lang, section, _F43["coverage-cell"][lang], route_mark), (lang, key)
    elif key == "never-inheritance-full-load":
        assert _fact(lang, section, "inheritance-rerun"), (lang, key)
    elif key == "never-inheritance-cdc-only":
        assert _fact(lang, section, "cdc-only-switch"), (lang, key)
    elif key == "never-unusable-full-load":
        assert _fact(lang, section, "unusable-fix"), (lang, key)
        assert _steps_in_order(lang, never, "Re-run Full Load", "Start CDC"), (lang, key)
    elif key == "never-unusable-cdc-only":
        assert _fact(lang, section, "unusable-cdc-only"), (lang, key)
    elif key == "never-coverage-full-load":
        assert _fact(lang, section, "coverage-cell"), (lang, key)
        assert _steps_in_order(lang, never, "Re-run Full Load", "Start CDC"), (lang, key)
    elif key == "never-coverage-cdc-only":
        assert _fact(lang, never, "cdc-only-switch"), (lang, key)
    else:  # pragma: no cover
        raise AssertionError(key)


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

    # Then the manual: the fact it states for that route.
    for key in keys:
        _manual_names(lang, key)


@pytest.mark.parametrize("lang", LANGS)
def test_a_started_stacks_paragraph_names_new_topics_before_any_full_load_rerun(lang) -> None:
    # Every sentence of the started stack's half of the route paragraph that names Re-run Full
    # Load names Delete CDC infrastructure before it; the never-started routes are in the other.
    started = _route_halves(lang)[1]
    assert _new_topics_first(lang, started), lang
    assert _flat(lang, "Re-snapshot every table instead (").strip() not in started, lang


# -- f0: a stopped stack -- one route, and a resume gapless only within the topics' retention --

# §4.3 states the stopped stack's bound once ("Starting again"): a restart is gapless only
# while the topics still retain the changes the sink had not applied, the Stop CDC dialog and the
# *Ready to resume CDC* card say so, and past the retention period the route is new topics first.
# The start-point card's title and its wording are in the UI.
# G25 round 8 (rule E): the re-run that takes the fresh watermark is a Drop & reload.
_LOST_SLOT = {
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

    section = _section_43(lang)
    assert _fact(lang, section, "gapless-while-retained"), lang
    assert _has(lang, section, "*Ready to resume CDC*"), lang
    assert _steps_in_order(
        lang, section, _F43["restart-row"][lang], "Delete CDC infrastructure", "Re-run Full Load",
        "Start CDC",
    ), lang
    assert _fact(lang, section, "lost-slot"), lang
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

# §4.3's restart row keeps the user-visible half of Debezium's rule: a source connector that never
# completed its snapshot snapshots every selected table again. When a recorded position counts
# is on the cards and the Start's activity-log line.


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

    assert _fact(lang, _section_43(lang), "mid-snapshot"), lang


# -- f2: the start-point card on a rolled-back stack resumes like the Start card -------------

# §4.3 puts a rolled-back Start in the restart row; the start-point card's own title (*Starting
# again after a rolled-back Start*) and the start point and re-snapshot it does not offer are in
# the UI.


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

    assert _fact(lang, _section_43(lang), "restart-row"), lang


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

    for key in ("never-inheritance-full-load", "never-inheritance-cdc-only",
                "never-unusable-full-load", "never-unusable-cdc-only"):
        _manual_names(lang, key)
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

    for key in ("never-coverage-full-load", "never-coverage-cdc-only", "never-job-same-route",
                "never-job-both-ways"):
        _manual_names(lang, key)
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

    # §4.3's never-started half names no new topics at all: the one exception (CDC only's route
    # for a publication that misses a table) is left to the block, which names its own route.
    assert not _has(lang, _route_halves(lang)[0], "Delete CDC infrastructure"), lang
