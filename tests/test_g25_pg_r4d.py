# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 4, last step, package PG: a PostgreSQL cdc-stack whose last Start CloudFormation
rolled back.

* Its CDC topics outlive the rollback and may hold changes that Start streamed which no sink
  applied; the next sink applies them first. So a block on such a stack names only new topics
  (Delete CDC infrastructure and deploy it again, then -- Full load + CDC -- the Full Load
  re-run), never "Re-run Full Load, then Start CDC" on the old ones, which would replay those
  changes over the reloaded tables (findings 0 and 4); the publication diagnosis says the
  same for that state.
* Its Start card and its permanent start line promise no snapshot: Debezium skips it once
  that Start recorded a position. They say the Start continues from the position the
  rolled-back Start recorded, if it recorded one, and that this is gapless only while the
  topics still retain that Start's changes (findings 2, 5 and 7).
* Every rollback status counts as started, and so do connectors of the stack still in place:
  an ``UPDATE_ROLLBACK_FAILED`` stack whose source connector moved the slot is never refused
  on the slot position, nor told "not even one that rolled back" (finding 6).
* Where the re-snapshot remedy is withheld (an inheritance parent), the dialog names the
  route that does work: the Full Load re-run (finding 1).
* A failed read of the re-snapshot pre-check is reported as exactly that, not as "the
  publication and replication slot could not be checked" beside the block those checks
  produced (finding 3).

A stack never started (``CREATE_COMPLETE`` / ``UPDATE_COMPLETE``, no Start) keeps its
behaviour: still blocked on a moved slot, the re-snapshot remedy offered when its checks pass.
Fakes only: no real database or AWS.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.models import SourceType
from tests.test_g25_pg import AT_START, MOVED, SLOT_REFUSAL
from tests.test_g25_pg_r4 import (
    NEVER_STARTED,
    RERUN_AT,
    STOPPED,
    _cdc_only,
    _dialog,
    _fresh,
    _headers,
    _pg_session,
    _rerun_watermark,
    _start,
)
from tests.test_g25_pg_r4c import (
    _PUBLICATION_GONE_TAIL,
    PUBLICATION_MISSING,
    REMEDY,
    ROLLED_BACK,
    SLOT_MISSING,
    _bodies,
    _probe,
    _slot,
    _source,
)

DELETE = "Delete CDC infrastructure and deploy it again"
RERUN = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
RECORDED = "continues from the position the rolled-back Start recorded"
RETAINED = "gapless only while the topics still retain them"
ROLLBACK_STATUSES = (
    "UPDATE_ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
    "UPDATE_ROLLBACK_IN_PROGRESS",
    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
)
_ORDERS_PARENT = (("app.orders", 1, ("app.orders_2026",)),)


def _new_topics_first(text: str) -> bool:
    """Every Full Load re-run ``text`` names comes after Delete CDC infrastructure: no route on
    a rolled-back stack re-runs the Full Load on the old topics."""
    lowered = text.lower()
    reruns = [i for i in range(len(lowered)) if lowered.startswith("re-run", i)]
    if not reruns:
        return True
    first_delete = lowered.find(DELETE.lower())
    return first_delete != -1 and first_delete < reruns[0]


def _start_line(run) -> str:
    """The Start's permanent activity line (what ``_cdc_start_detail`` wrote at submit)."""
    (line,) = [
        kw["detail"] for action, kw in run["events"]
        if action == "start CDC connectors" and kw.get("status") is None
    ]
    return line


def _start_card(monkeypatch, state, *, watermark=None) -> str:
    """The REAL Start CDC card of a PostgreSQL stack in the ``infra`` phase, as text."""
    from dsql_migrator.core.job_manager import JobNotFoundError
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _RecordingUi

    fl_job = SimpleNamespace(job_id="fl-job", watermark=watermark, status="DONE")

    class _JM:
        def get_status(self, job_id):
            if job_id == "fl-job" and watermark is not None:
                return fl_job
            raise JobNotFoundError(job_id)

    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _RecordingUi()
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=None, target_config=None, aws_profile=None,
    )
    _cdc_ui._render_cdc_start_button(ui, state, _JM(), lambda: None, inventory=None,
                                     session=session)
    return " ".join(ui.texts)


def _cdc_only_start(monkeypatch, state):
    """The real Start CDC of a CDC only migration (no Full Load watermark)."""
    from tests.test_ui_data_migration import _g5_inventory, _run_start_cdc_capture

    return _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES, job_watermark=None,
        source_password="pw", selected=["app.orders"], inventory=_g5_inventory(),
        catch=True, msk_access=True, state=state,
    )


# ---------------------------------------------------------------------------
# Findings 0 and 4: a rolled-back stack's block names new topics, never a Full Load re-run
# on the old ones
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "objects, header",
    [
        (_slot(None, slot_present_any_database=False, slot_usable=False), SLOT_MISSING),
        (_slot(AT_START, publication_present=False, publication_tables=frozenset()),
         PUBLICATION_MISSING),
    ],
    ids=["slot-missing", "publication-missing"],
)
def test_a_rolled_back_stacks_block_names_new_topics_before_any_full_load_rerun(
    monkeypatch, objects, header
) -> None:
    # FAILS WITHOUT THE FIX: the block's first "gapless route" was "Re-run Full Load ...
    # 'Drop & reload' ..., then Start CDC" -- the next sink, with no committed offsets, then
    # applies the rolled-back Start's changes still in the topics over the reloaded tables.
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    _source(monkeypatch, objects)
    ui, notices, _events = _dialog(monkeypatch, state)
    ((_tone, shown, body),) = [n for n in notices if n[0] == "error"]
    routes = _pg_block_route(state, header)
    assert shown == header and body.endswith(routes)
    assert "Two gapless routes" not in routes and "; or use Delete" not in routes
    assert routes.endswith(
        "The gapless route: use Delete CDC infrastructure and deploy it again (the new stack "
        "starts with empty topics), then Re-run Full Load on the Full Load step choosing "
        "'Drop & reload' (it creates the slot again at its own snapshot point), then Start CDC."
    )
    assert "its CDC topics may still hold changes that no sink applied" in routes
    assert _new_topics_first(routes)
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False
    # The worker's refusal (Retry CDC, or a stale dialog) names the same route; its job error
    # keeps a short first line.
    run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and routes in str(run["error"])
    assert len(str(run["error"]).splitlines()[0]) <= 300


@pytest.mark.parametrize("status", ROLLBACK_STATUSES)
def test_every_rollback_status_gets_the_new_topics_route(monkeypatch, status) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    for mtype in (None, _cdc_only()):
        state = _fresh(mtype)
        _probe(monkeypatch, state, NEVER_STARTED, status)
        routes = _cdc_ui._pg_block_route(state, PUBLICATION_MISSING)
        assert DELETE in routes and _new_topics_first(routes), (status, mtype, routes)
        assert "Two gapless routes" not in routes
    cdc_only = _fresh(_cdc_only())
    _probe(monkeypatch, cdc_only, NEVER_STARTED, status)
    assert "Re-run Full Load" not in _cdc_ui._pg_block_route(cdc_only, PUBLICATION_MISSING)


def test_a_stopped_stack_gets_the_new_topics_route_too(monkeypatch) -> None:
    # G25 round 5 (finding 0): started, then stopped -- Stop CDC deletes the source and the sink
    # in parallel, with no drain, so the topics may hold changes the sink had not applied, and
    # its group resumes from its last commit: "Re-run Full Load, then Start CDC" on them would
    # replay those changes over the reloaded tables. So the stopped stack's one route is the
    # rolled-back one's: new topics first (tests/test_g25_pg_r5.py: the dialog and the job).
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    stopped, rolled = _fresh(), _fresh()
    _probe(monkeypatch, stopped, STOPPED, "UPDATE_COMPLETE")
    _probe(monkeypatch, rolled, NEVER_STARTED, ROLLED_BACK)
    routes = _pg_block_route(stopped, PUBLICATION_MISSING)
    assert routes == _pg_block_route(rolled, PUBLICATION_MISSING)
    assert "Two gapless routes" not in routes and _new_topics_first(routes)
    assert "Stop CDC and a rolled-back Start remove the connectors without draining them" \
        in routes


def test_the_publication_diagnosis_puts_new_topics_first_after_a_rolled_back_start(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX (finding 0): the diagnosis named the Full Load re-run as a route
    # for a rolled-back Start too.
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log

    guidance = _diagnose_connector_log("dsql-cdc-stack-debezium-source", _PUBLICATION_GONE_TAIL)
    assert guidance is not None and REMEDY not in guidance
    # G25 round 5: one route for every state this is read on (stopped or rolled back), new
    # topics first, then -- per migration type -- the Full Load re-run or the new stack's Start.
    assert "After a rolled-back Start:" not in guidance
    assert "With the connectors removed:" not in guidance
    assert _new_topics_first(guidance)
    assert (
        "The gapless route: use Delete CDC infrastructure and deploy it again (the new stack "
        "starts with empty topics); then, for Full load + CDC, Re-run Full Load on the Full Load "
        "step choosing 'Drop & reload' (it creates the slot again at its own snapshot point) and "
        "Start CDC, and for CDC only, Start CDC on the new stack (it snapshots every selected "
        "table)."
    ) in guidance
    assert "Do NOT create a replication slot by hand" in guidance
    # ...and the dialog that stack's operator opens names the same order.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    _source(monkeypatch, _slot(AT_START, publication_present=False,
                               publication_tables=frozenset()))
    _ui, notices, _events = _dialog(monkeypatch, state)
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == PUBLICATION_MISSING and _new_topics_first(body)


def test_a_rolled_back_inheritance_block_asks_for_new_topics_before_its_full_load(
    monkeypatch,
) -> None:
    # The inheritance block's own fix is a Full load + CDC run; on a stack whose last Start
    # rolled back that run needs new topics first.
    from dsql_migrator.core.cdc_pg_slot import INHERITANCE_SNAPSHOT_HEADER

    first = (
        "So before that Full Load, use Delete CDC infrastructure and deploy it again (the new "
        "stack starts with empty topics)."
    )
    rolled = _fresh(_cdc_only())
    _probe(monkeypatch, rolled, NEVER_STARTED, ROLLED_BACK)
    _source(monkeypatch, _slot(AT_START), parents=_ORDERS_PARENT)
    _ui, notices, _events = _dialog(monkeypatch, rolled)
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == INHERITANCE_SNAPSHOT_HEADER
    assert body.endswith(first)
    assert "its CDC topics may still hold changes that no sink applied" in body
    # Guard: a never-started stack's block is unchanged.
    never = _fresh(_cdc_only())
    _probe(monkeypatch, never, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(AT_START), parents=_ORDERS_PARENT)
    _ui, notices, _events = _dialog(monkeypatch, never)
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == INHERITANCE_SNAPSHOT_HEADER
    assert first not in body
    # G25 round 6: CDC only's table picker is locked while its infrastructure exists, so the
    # block names no untick -- its own fix (a Full Load's snapshot point) stands alone.
    assert body.endswith("and its CDC starts without a snapshot.")
    assert "untick" not in body


# ---------------------------------------------------------------------------
# Findings 2, 5 and 7: the Start card and the start line promise no snapshot there
# ---------------------------------------------------------------------------


def test_a_rolled_back_cdc_only_start_card_promises_no_snapshot(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "this start has nothing to resume from, so the source connector
    # then takes a FRESH SNAPSHOT of every selected table" -- which Debezium skips once the
    # rolled-back Start recorded a position.
    state = _fresh(_cdc_only())
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    card = _start_card(monkeypatch, state)
    assert "Ready to start CDC again" in card
    for absent in ("FRESH SNAPSHOT", "nothing to resume from", "stopping CDC", "Re-run Full Load"):
        assert absent not in card, absent
    # G25 round 5 (finding 1): no "with no snapshot" -- a stored position whose initial
    # snapshot had not completed snapshots again (Debezium 2.7.4).
    assert (
        "CloudFormation rolled back this stack's last Start, which removed its connectors. "
        "Start CDC creates them again and continues from the position the rolled-back Start "
        "recorded, if it recorded one (or from where an earlier run stopped streaming). If that "
        "Start (or the earlier run) had not completed its initial snapshot, or nothing was "
        "recorded, Debezium snapshots every selected table first, then streams from replication "
        "slot"
    ) in card
    assert "with no snapshot" not in card
    # Finding 5: the retry's gaplessness is bounded by the topics' retention, said plainly.
    assert (
        "Any changes that Start streamed are applied from the CDC topics, so this is gapless "
        "only while the topics still retain them: once their retention period on MSK has passed "
        "since that Start, a Start here would skip them, so instead use Delete CDC "
        "infrastructure and deploy it again (the new stack starts with empty topics), then "
        "Start CDC on the new stack, which snapshots every selected table"
    ) in card


@pytest.mark.parametrize("start_mode", ["auto", "manual"])
def test_a_rolled_back_full_load_and_cdc_start_card_promises_no_snapshot(
    monkeypatch, start_mode
) -> None:
    from tests.test_g25_pg import _watermark

    state = _fresh()
    state.job_id = "fl-job"
    state.set_cdc_start_mode(start_mode)
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    card = _start_card(monkeypatch, state, watermark=_watermark())
    assert "Ready to start CDC again" in card and RECORDED in card and RETAINED in card
    for absent in ("FRESH SNAPSHOT", "nothing to resume from", "stopping CDC"):
        assert absent not in card, absent
    if start_mode == "auto":
        assert (
            "Start CDC creates them again and continues from the position the rolled-back Start "
            "recorded, if it recorded one, else from replication slot slot's confirmed position, "
            "with no snapshot (snapshot.mode=never)."
        ) in card
    else:
        assert (
            "If that Start (or the earlier run) had not completed its initial snapshot, or "
            "nothing was recorded, Debezium snapshots every selected table first"
        ) in card
    # Its fallback names new topics before the Full Load re-run.
    assert (
        "so instead use Delete CDC infrastructure and deploy it again (the new stack starts with "
        "empty topics), then Re-run Full Load on the Full Load step choosing 'Drop & reload' (it "
        "creates the slot again at its own snapshot point), then Start CDC."
    ) in card
    assert _new_topics_first(card)


def test_the_never_started_and_stopped_start_cards_are_unchanged(monkeypatch) -> None:
    # Guards: a stack never started still announces its snapshot; a stopped one its resume.
    never = _fresh(_cdc_only())
    _probe(monkeypatch, never, NEVER_STARTED, "CREATE_COMPLETE")
    card = _start_card(monkeypatch, never)
    assert "Ready to start CDC" in card and "FRESH SNAPSHOT" in card
    assert "rolled back" not in card
    stopped = _fresh(_cdc_only())
    _probe(monkeypatch, stopped, STOPPED, "UPDATE_COMPLETE")
    card = _start_card(monkeypatch, stopped)
    assert "Ready to resume CDC" in card
    assert "stopping CDC removed only the connectors" in card and "rolled back" not in card


def test_a_rolled_back_cdc_only_start_line_promises_no_snapshot(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the permanent record said "start point: a snapshot — ... the
    # connector snapshots every selected table".
    state = _fresh(_cdc_only())
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    _source(monkeypatch, _slot(AT_START))
    run = _cdc_only_start(monkeypatch, state)
    assert run["error"] is None and run["kwargs"]
    line = _start_line(run)
    assert "start point: a snapshot" not in line
    assert "the connector snapshots every selected table, then streams" not in line
    assert "this stack's last Start was rolled back by CloudFormation" in line
    assert (
        "the connector continues from the position the rolled-back Start recorded, if it "
        "recorded one, or from where an earlier run stopped streaming; if that Start (or the "
        "earlier run) had not completed its initial snapshot, or nothing was recorded, Debezium "
        "snapshots every selected table first"
    ) in line
    assert "with no snapshot" not in line
    assert "any changes that Start streamed are applied from the CDC topics, gapless only " \
        "while the topics still retain them" in line


@pytest.mark.parametrize("start_mode", ["auto", "manual"])
def test_a_rolled_back_full_load_and_cdc_start_line_promises_no_snapshot(
    monkeypatch, start_mode
) -> None:
    state = _fresh()
    state.set_cdc_start_mode(start_mode)
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    _source(monkeypatch, _slot(AT_START))
    run = _start(monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]
    line = _start_line(run)
    assert RECORDED in line and "gapless only while the topics still retain them" in line
    for absent in ("a FULL initial snapshot is forced", "start point: a snapshot",
                   "re-snapshot: every selected table is read again",
                   "gapless from the Full Load watermark"):
        assert absent not in line, absent
    if start_mode == "manual":
        assert line.endswith(
            "Manual's full snapshot runs only if nothing was recorded on this stack, or the run "
            "that recorded its position had not completed its initial snapshot")
    else:
        assert (
            "snapshot.mode=never, so no snapshot runs and the connector continues from the "
            "position the rolled-back Start recorded, if it recorded one, else from the slot's "
            "confirmed position"
        ) in line


def test_the_start_line_ignores_a_rollback_on_mysql_and_on_a_never_started_stack() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_start_detail

    # MySQL keeps its line whatever the stack status.
    assert _cdc_start_detail("s", None, mode="auto", source_type=SourceType.MYSQL,
                             resumes=True, rolled_back=True) == _cdc_start_detail(
        "s", None, mode="auto", source_type=SourceType.MYSQL, resumes=True)
    # A PostgreSQL stack never started still records its snapshot.
    first = _cdc_start_detail("s", None, mode="manual", source_type=SourceType.POSTGRES,
                              force_snapshot=True)
    assert "start point: a snapshot" in first
    assert first.endswith("a FULL initial snapshot is forced for this start")


def test_the_rolled_back_docstring_no_longer_claims_an_unconditional_gapless_retry() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_start_rolled_back

    doc = " ".join((_pg_start_rolled_back.__doc__ or "").split())
    assert "so a retry is gapless," not in doc
    assert "a retry is gapless only while the topics still retain them" in doc


# ---------------------------------------------------------------------------
# Finding 6: every rollback status, and connectors in place, count as started
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status, rolled_back",
    [(s, True) for s in ROLLBACK_STATUSES]
    + [("CREATE_COMPLETE", False), ("UPDATE_COMPLETE", False), ("UPDATE_IN_PROGRESS", False),
       ("ROLLBACK_COMPLETE", False)],
)
def test_any_update_rollback_status_counts_as_started(monkeypatch, status, rolled_back) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, status)
    assert _cdc_ui._pg_start_rolled_back(state) is rolled_back
    assert _cdc_ui._pg_stack_started(state) is rolled_back


def test_connectors_of_the_stack_in_place_count_as_started(monkeypatch) -> None:
    from dsql_migrator.core.cdc import cdc_expected_connector_names
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "UPDATE_IN_PROGRESS")
    assert state.cdc_stack_phase == "unstable" and _cdc_ui._pg_stack_started(state) is False
    state.set_cdc_connector_names([cdc_expected_connector_names(state.cdc_stack_name)[0]])
    assert _cdc_ui._pg_stack_started(state) is True


@pytest.mark.parametrize("leftover", [True, False], ids=["leftover-connector", "no-connector"])
def test_retry_on_an_update_rollback_failed_stack_is_not_refused_on_the_slot(
    monkeypatch, leftover
) -> None:
    # FAILS WITHOUT THE FIX: a Start/Retry/Stop update failed and so did its rollback; the
    # source connector left behind streamed and moved the slot. Retry CDC was refused before
    # run_cdc_start (whose rollback auto-recovery never ran) with "this CDC stack records no
    # Start of its connectors (not even one that rolled back)".
    from dsql_migrator.core.cdc import cdc_expected_connector_names

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "UPDATE_ROLLBACK_FAILED")
    assert state.cdc_stack_phase == "unstable"
    if leftover:
        state.set_cdc_connector_names([cdc_expected_connector_names(state.cdc_stack_name)[0]])
    _source(monkeypatch, _slot(MOVED))
    run = _start(monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]  # run_cdc_start ran (and its recovery)
    logged = " ".join(str(kw.get("detail")) for _action, kw in run["events"])
    assert "not even one that rolled back" not in logged
    assert SLOT_REFUSAL not in logged


@pytest.mark.parametrize("status", ["CREATE_COMPLETE", "UPDATE_COMPLETE"])
def test_a_never_started_stack_is_still_refused_and_offered_the_remedy(monkeypatch, status) -> None:
    # Guard: no Start, no connectors, no rollback -- blocked on a moved slot, with the
    # re-snapshot remedy (its checks pass), in the dialog and the worker.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, status)
    _source(monkeypatch, _slot(MOVED))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [
        ("error", SLOT_REFUSAL), ("info", "Re-snapshot instead — gapless, no second Full Load")]
    assert "(not even one that rolled back)" in notices[0][2]
    assert REMEDY in ui.buttons and ui.buttons["Start CDC"].enabled is False
    run = _start(monkeypatch, state=state)
    assert SLOT_REFUSAL in str(run["error"]) and run["kwargs"] == {}


# ---------------------------------------------------------------------------
# Finding 1: a withheld re-snapshot names the route that works
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "objects, header",
    [
        (_slot(MOVED), SLOT_REFUSAL),
        (_slot(None, slot_present_any_database=False, slot_usable=False), SLOT_MISSING),
        (_slot(AT_START, publication_present=False, publication_tables=frozenset()),
         PUBLICATION_MISSING),
    ],
    ids=["slot-moved", "slot-missing", "publication-missing"],
)
def test_a_withheld_re_snapshot_names_the_full_load_rerun(monkeypatch, objects, header) -> None:
    # FAILS WITHOUT THE FIX: the only advice beside the block was "Start CDC from a Full
    # Load's snapshot point instead" -- the very Start the block refuses -- or unticking the
    # parent. The Full Load re-run, which creates the slot again, was named nowhere.
    from dsql_migrator.core.cdc_pg_slot import INHERITANCE_SNAPSHOT_HEADER
    from dsql_migrator.ui.data_migration._cdc_ui import PG_RESNAPSHOT_REFUSED_LEAD

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, objects, parents=_ORDERS_PARENT)
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", header), ("error", INHERITANCE_SNAPSHOT_HEADER)]
    body = notices[1][2]
    assert body.startswith(f"{PG_RESNAPSHOT_REFUSED_LEAD} app.orders has 1 inheritance")
    # The route rule's way out after that refusal (pg_cdc_route), Full load + CDC. (G25 round
    # 6: no untick -- this harness's table lock cannot be read; test_g25_pg_r6.py.)
    assert body.endswith(
        "and its CDC starts without a snapshot. Re-run Full "
        "Load on the Full Load step choosing 'Drop & reload' instead: it creates the slot again "
        "at its own snapshot point, and the Start CDC after it takes no snapshot.")
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False


def test_the_named_full_load_rerun_route_does_work(monkeypatch) -> None:
    # The route the withheld notice names, followed: the re-run's watermark and the slot it
    # created at that point -- the never-started stack's Start is not blocked, reads no
    # inheritance (it takes no snapshot) and goes ahead.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    reads = _source(monkeypatch, _slot(RERUN_AT), parents=_ORDERS_PARENT)
    ui, notices, _events = _dialog(monkeypatch, state, watermark=_rerun_watermark())
    assert not [n for n in notices if n[0] == "error"], notices
    assert ui.buttons["Start CDC"].enabled is not False
    assert reads == ["objects"]
    run = _start(monkeypatch, state=state, watermark=_rerun_watermark())
    assert run["error"] is None and run["kwargs"]


# ---------------------------------------------------------------------------
# Finding 3: a failed re-snapshot pre-check read is reported as exactly that
# ---------------------------------------------------------------------------

_LEAKY = (
    'connection to server at "db.internal.example" (10.0.0.5), port 5432 failed: FATAL: '
    'too many connections for role "migrator_admin"'
)


def _second_read_fails(monkeypatch, objects) -> None:
    """A source whose first connection reads ``objects`` and whose next ones fail with an error
    that carries a host, an address and a user name."""
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.ui import connect

    connects: list = []

    class OperationalError(Exception):
        pass

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

    class _Engine:
        def connect(self):
            connects.append(1)
            if len(connects) > 1:
                raise OperationalError(_LEAKY)
            return _Conn()

        def dispose(self):
            pass

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine()))
    monkeypatch.setattr(cdc_pg_slot, "read_pg_replication_objects", lambda *_a, **_k: objects)


def test_a_failed_pre_check_read_is_reported_as_the_re_snapshot_route_not_checked(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: the warning "CDC's publication and replication slot could not be
    # checked" (Start CDC "did not check CDC's publication and replication slot") sat above
    # the very block those checks produced, and the activity log said "its publication,
    # replication slot and inheritance checks did not run".
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _second_read_fails(monkeypatch, _slot(MOVED))
    ui, notices, events = _dialog(monkeypatch, state)
    assert _headers(notices) == [
        ("warning", _cdc_ui.PG_RESNAPSHOT_NOT_CHECKED_HEADER),
        ("error", SLOT_REFUSAL),
        ("info", "Re-snapshot instead — gapless, no second Full Load"),
    ]
    assert _cdc_ui.PG_RESNAPSHOT_NOT_CHECKED_HEADER == "The re-snapshot route could not be checked"
    body = notices[0][2]
    assert body == _cdc_ui.pg_resnapshot_not_checked_body("the connection failed: OperationalError")
    assert body.startswith(
        "Start CDC checked CDC's publication and replication slot, but could not read the "
        "PostgreSQL source again (the connection failed: OperationalError)")
    assert "did not check CDC's publication and replication slot" not in body
    assert "The remedy is still offered" in body
    assert REMEDY in ui.buttons  # unreadable is not refused: the remedy stays
    (line,) = [e for e in events if e[0] == _cdc_ui.PG_RESNAPSHOT_NOT_CHECKED_ACTION]
    detail = line[1]["detail"]
    assert line[1]["status"] is ActivityStatus.WARNING
    assert "the publication and replication slot checks ran" in detail
    assert "its publication, replication slot and inheritance checks did not run" not in detail
    assert not [e for e in events if e[0] == _cdc_ui.PG_CHECKS_NOT_RUN_ACTION]
    assert not [n for n in notices if n[1] == _cdc_ui.PG_CHECKS_NOT_RUN_HEADER]
    # Property 7: the exception class at most -- no host, address or user name.
    for text in (body, detail):
        for value in ("db.internal", "10.0.0.5", "migrator_admin", _LEAKY):
            assert value not in text, value


def test_a_first_read_that_fails_keeps_the_publication_and_slot_notice(monkeypatch) -> None:
    # Guard: when the gate's own read fails no block exists, no pre-check runs, and the
    # existing notice and line say the publication and slot checks did not run.
    from tests.test_g25_pg_r4 import _unreachable

    from dsql_migrator.ui.data_migration import _cdc_ui

    _unreachable(monkeypatch)
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    ui, notices, events = _dialog(monkeypatch, state, session=_pg_session())
    assert _headers(notices) == [("warning", _cdc_ui.PG_CHECKS_NOT_RUN_HEADER)]
    assert ui.buttons["Start CDC"].enabled is True
    assert [e[0] for e in events] == [_cdc_ui.PG_CHECKS_NOT_RUN_ACTION]
    assert "re-snapshot route" not in _bodies(notices)
