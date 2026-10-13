# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 5, package PG: ONE route rule for every PostgreSQL CDC block whose stream cannot
continue as is (:func:`dsql_migrator.core.cdc_pg_slot.pg_cdc_route`).

* A stack started before -- stopped, with connectors in place, or whose last Start
  CloudFormation rolled back -- may have changes no sink applied in its CDC topics (Stop CDC
  deletes the source and the sink in parallel, with no drain; a rollback deletes them
  mid-stream), and its sink group resumes from its last commit. So its ONLY route is new topics
  first: Delete CDC infrastructure and deploy it again, then (Full load + CDC) Re-run Full Load
  with Drop & reload, then Start CDC; (CDC only) Start CDC on the new stack. A stopped stack
  named "Re-run Full Load, then Start CDC" as its first route, which replays that tail over the
  reloaded tables (finding 0).
* A stack never started: the re-snapshot remedy when its own checks pass, else a Start that
  takes no snapshot after a Full Load re-run -- AFTER the refusal's own fix when it names one
  (a slot of another database: the Full Load refuses it too), and behind a switch to Full load
  + CDC with CDC only, which has no Full Load step (finding 3). A publication that misses a
  table names its working route, in the dialog and in the job's refusal (finding 4).
* A rolled-back Start's copy claims no snapshot outcome Debezium does not guarantee: a stored
  position whose initial snapshot had not completed snapshots again (finding 1), and the
  start-point card above the Start card no longer promises a snapshot or an unconditional
  gapless start there (finding 2). Every gapless claim that rests on the topics states it.

The route table is first; each finding's test FAILS on the code before this round. Fakes only:
no real database or AWS.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from dsql_migrator.core import cdc_pg_slot as pg
from dsql_migrator.core.models import SourceType
from tests.test_g25_pg import AT_START, MOVED, SLOT_REFUSAL
from tests.test_g25_pg_r4 import (
    NEVER_STARTED,
    RUNNING,
    STOPPED,
    _cdc_only,
    _dialog,
    _fresh,
    _headers,
    _start,
)
from tests.test_g25_pg_r4c import (
    COVERAGE,
    PUBLICATION_MISSING,
    REMEDY,
    ROLLED_BACK,
    SLOT_MISSING,
    SLOT_UNUSABLE,
    _probe,
    _slot,
    _source,
)

_ORDERS_PARENT = (("app.orders", 1, ("app.orders_2026",)),)
_DELETE = "Delete CDC infrastructure and deploy it again"

# -- the route rule's exact words ------------------------------------------------------------

_STARTED_LEAD = (
    "This stack's connectors were started before: if its source connector streamed, Debezium "
    "skips the snapshot on every later Start, so a re-snapshot cannot be relied on here, and "
    "its CDC topics may still hold changes that no sink applied (Stop CDC and a rolled-back "
    "Start remove the connectors without draining them), which this stack's sink would apply "
    "over a reload."
)
_NEW_TOPICS = (
    "use Delete CDC infrastructure and deploy it again (the new stack starts with empty topics)"
)
_NEW_TOPICS_FL = (
    f"{_NEW_TOPICS}, then Re-run Full Load on the Full Load step choosing 'Drop & reload' (it "
    "creates the slot again at its own snapshot point), then Start CDC"
)
_NEW_TOPICS_CDC = (
    f"{_NEW_TOPICS}, then Start CDC on the new stack, which snapshots every selected table (a "
    "row deleted on the source in between stays on the target; Validation reports it as an "
    "EXTRA row)"
)
_STARTED = {
    False: f"{_STARTED_LEAD} The gapless route: {_NEW_TOPICS_FL}.",
    True: f"{_STARTED_LEAD} The gapless route: {_NEW_TOPICS_CDC}.",
}
_STARTED_AFTER_FIX = {
    False: f"{_STARTED_LEAD} After that fix, the gapless route: {_NEW_TOPICS_FL}.",
    True: f"{_STARTED_LEAD} After that fix, the gapless route: {_NEW_TOPICS_CDC}.",
}
_STARTED_INHERITANCE = (
    "This stack's connectors were started before, and its CDC topics may still hold changes "
    "that no sink applied (Stop CDC and a rolled-back Start remove the connectors without "
    "draining them), which this stack's sink would apply over a reload. So before that Full "
    f"Load, {_NEW_TOPICS}."
)
_RERUN = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
_NO_SNAPSHOT_FL = f"{_RERUN} (it creates the slot again at its own snapshot point), then Start CDC"
_NO_SNAPSHOT_CDC = f"switch the migration type to Full load + CDC, then {_NO_SNAPSHOT_FL}"
_UNCHECKED = (
    "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
    "re-snapshots passes its own checks), or "
)
_POSITION_NOTE = "; give this migration its own CDC stack name if another migration shares this one"
_NEVER = {
    # block -> (Full load + CDC, CDC only), for resnapshot=None / "refused"
    COVERAGE: (
        f"{_RERUN}, which adds each missing selected table to the publication and creates the "
        "slot again at its own snapshot point, then Start CDC.",
        "Use Delete CDC infrastructure and deploy it again: it drops the publication, so the new "
        "stack's Start CDC can create one over the selected tables.",
    ),
    SLOT_UNUSABLE: (f"Then {_NO_SNAPSHOT_FL}.", "Then Start CDC again."),
}
_REFUSED_SNAPSHOT = (
    f"{_RERUN} instead: it creates the slot again at its own snapshot point, and the Start CDC "
    "after it takes no snapshot.",
    f"Or switch the migration type to Full load + CDC, then {_RERUN}: it creates the slot again "
    "at its own snapshot point, and the Start CDC after it takes no snapshot.",
)

# The stack states, as the real stack-phase probe reads them: (parameters, status).
_STATES = {
    "never-started": (NEVER_STARTED, "CREATE_COMPLETE"),
    "stopped": (STOPPED, "UPDATE_COMPLETE"),
    "rolled-back": (NEVER_STARTED, ROLLED_BACK),
    "stopped-then-rolled-back": (STOPPED, ROLLED_BACK),
    "connectors-in-place": (RUNNING, "UPDATE_COMPLETE"),
}
_BLOCKS = (SLOT_REFUSAL, SLOT_MISSING, SLOT_UNUSABLE, PUBLICATION_MISSING, COVERAGE,
           pg.INHERITANCE_SNAPSHOT_HEADER, pg.INHERITANCE_UNREADABLE_HEADER,
           pg.PUBLISH_LIST_HEADER)


def _expected(started: bool, cdc_only: bool, block: str, resnapshot) -> str:
    """The route table: (stack state, migration type, block kind, remedy standing) -> words."""
    if started:
        if block == pg.INHERITANCE_UNREADABLE_HEADER:
            return ""  # its own fix (a read again) works on any stack
        if block == pg.INHERITANCE_SNAPSHOT_HEADER:
            return _STARTED_INHERITANCE
        # G25 round 6 (decision C): a narrowed publish list's own fix comes first, like a slot
        # of that name it cannot use, and does not bring back the narrowed window.
        fix_first = block in (SLOT_UNUSABLE, pg.PUBLISH_LIST_HEADER)
        return (_STARTED_AFTER_FIX if fix_first else _STARTED)[cdc_only]
    if block in _NEVER:
        return _NEVER[block][cdc_only]
    if block in (pg.INHERITANCE_SNAPSHOT_HEADER, pg.INHERITANCE_UNREADABLE_HEADER):
        return _REFUSED_SNAPSHOT[cdc_only] if resnapshot == pg.RESNAPSHOT_REFUSED else ""
    if block == pg.PUBLISH_LIST_HEADER:
        # After the list is set back, a Full Load re-run takes a new start point (CDC only:
        # behind the type switch): a Start from the slot would skip the narrowed window.
        return f"Then {_NO_SNAPSHOT_CDC if cdc_only else _NO_SNAPSHOT_FL}."
    # A Start that resumes from the Full Load's slot: the re-snapshot fixes it when offered.
    if resnapshot == pg.RESNAPSHOT_OFFERED:
        return ""
    note = _POSITION_NOTE if block == SLOT_REFUSAL else ""
    route = (_NO_SNAPSHOT_CDC if cdc_only else _NO_SNAPSHOT_FL).replace(
        "own snapshot point)", f"own snapshot point{note})")
    if resnapshot == pg.RESNAPSHOT_REFUSED:
        return f"{route[:1].upper()}{route[1:]}."
    return f"{_UNCHECKED}{route}."


@pytest.mark.parametrize("resnapshot", [None, pg.RESNAPSHOT_OFFERED, pg.RESNAPSHOT_REFUSED])
@pytest.mark.parametrize("block", _BLOCKS)
@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
@pytest.mark.parametrize("stack", list(_STATES))
def test_the_route_table(monkeypatch, stack, cdc_only, block, resnapshot) -> None:
    # Stack state x migration type x block kind -> the exact route, read through the one
    # derivation every surface uses (the real stack-phase probe, then _pg_block_route).
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh(_cdc_only() if cdc_only else None)
    parameters, status = _STATES[stack]
    _probe(monkeypatch, state, parameters, status)
    started = stack != "never-started"
    assert _cdc_ui._pg_stack_started(state) is started, stack
    route = _cdc_ui._pg_block_route(state, block, resnapshot=resnapshot)
    assert route == _expected(started, cdc_only, block, resnapshot)
    # Never a Full Load step that CDC only does not have, unless after the type switch.
    if cdc_only and "Full Load step" in route:
        assert "switch the migration type to full load + cdc" in route.lower(), route
    # A stack started before: new topics before any Full Load re-run, and never the remedy.
    if started:
        assert "Re-snapshot every table instead" not in route
        if "Re-run Full Load" in route:
            assert route.index(_DELETE) < route.index("Re-run Full Load"), route


@pytest.mark.parametrize("cdc_only", [False, True, None])
def test_the_card_clause_is_the_started_route(cdc_only) -> None:
    # No block: the clause a card names mid-sentence, for a stack started before only.
    clause = pg.pg_cdc_route(None, started=True, cdc_only=cdc_only)
    if cdc_only is None:
        assert clause == (
            f"{_NEW_TOPICS}; then, for Full load + CDC, {_RERUN} (it creates the slot again at "
            "its own snapshot point) and Start CDC, and for CDC only, Start CDC on the new stack "
            "(it snapshots every selected table)"
        )
    else:
        assert clause == (_NEW_TOPICS_CDC if cdc_only else _NEW_TOPICS_FL)
    assert pg.pg_cdc_route(None, started=False, cdc_only=cdc_only) == ""


def test_the_per_surface_route_strings_are_gone() -> None:
    # One function names every route: the surfaces' own copies are deleted, so none can drift.
    from dsql_migrator.core import cdc_deployer
    from dsql_migrator.ui.data_migration import _cdc_ui

    for name in ("pg_resume_block_routes", "PG_RESNAPSHOT_REFUSED_ROUTE",
                 "PG_ROLLED_BACK_FULL_LOAD_FIRST"):
        assert not hasattr(_cdc_ui, name), name
    assert not hasattr(pg, "SLOT_POSITION_ROUTE")
    for fn in (_cdc_ui._pg_resume_continues, _cdc_ui._pg_objects_gate_block,
               _cdc_ui._pg_block_route, _cdc_ui._cdc_start_detail,
               _cdc_ui._render_cdc_start_point_card, pg.pg_replication_objects_blocker):
        source = inspect.getsource(fn)
        assert "Drop & reload" not in source and "deploy it again (" not in source, fn
    (guidance,) = [g for n, g in cdc_deployer._CONNECTOR_LOG_DIAGNOSES
                   if n == "publication autocreation is disabled"]
    assert pg.pg_cdc_route(PUBLICATION_MISSING, started=True, cdc_only=None) in guidance


# -- finding 0: a stopped stack's route is new topics first -------------------------------------


@pytest.mark.parametrize(
    "objects, header",
    [
        (_slot(None, slot_present_any_database=False, slot_usable=False), SLOT_MISSING),
        (_slot(AT_START, publication_present=False, publication_tables=frozenset()),
         PUBLICATION_MISSING),
        (_slot(AT_START, publication_tables=frozenset()), COVERAGE),
    ],
    ids=["slot-missing", "publication-missing", "coverage"],
)
@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_stopped_stacks_block_names_new_topics_first(monkeypatch, objects, header,
                                                       cdc_only) -> None:
    # FAILS WITHOUT THE FIX: "Two gapless routes: Re-run Full Load ... 'Drop & reload' ...,
    # then Start CDC; or use Delete CDC infrastructure ..." -- after Stop CDC the sink group has
    # committed only up to the last records it applied, so the first route replays the tail
    # (changes, inserts later deleted, deletes later re-inserted) over the reloaded tables.
    state = _fresh(_cdc_only() if cdc_only else None)
    _probe(monkeypatch, state, STOPPED, "UPDATE_COMPLETE")
    if cdc_only:
        state.job_id = "fl-job"  # a Full Load recorded the slot (a type switch since)
    _source(monkeypatch, objects)
    ui, notices, _events = _dialog(monkeypatch, state)
    ((_tone, shown, body),) = [n for n in notices if n[0] == "error"]
    route = _STARTED[cdc_only]
    assert shown == header and body.endswith(route)
    assert "Two gapless routes" not in body and REMEDY not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False
    # The worker's refusal (Retry CDC, a stale dialog) names the same route; its job error
    # keeps the short first line.
    run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and str(run["error"]).splitlines()[1].endswith(route)
    assert len(str(run["error"]).splitlines()[0]) <= 300


def test_the_publication_diagnosis_names_one_route_for_every_state() -> None:
    # FAILS WITHOUT THE FIX: "With the connectors removed: Re-run Full Load ..., then Start CDC;
    # or Delete CDC infrastructure ..." -- the stopped stack's replaying route.
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log
    from tests.test_g25_pg_r4c import _PUBLICATION_GONE_TAIL

    guidance = _diagnose_connector_log("dsql-cdc-stack-debezium-source", _PUBLICATION_GONE_TAIL)
    assert "With the connectors removed" not in guidance and "Two gapless" not in guidance
    assert (
        "a Start whose CloudFormation update rolled back left none to remove. "
        f"{_STARTED_LEAD} The gapless route: {_NEW_TOPICS}; then, for Full load + CDC, {_RERUN} "
        "(it creates the slot again at its own snapshot point) and Start CDC, and for CDC only, "
        "Start CDC on the new stack (it snapshots every selected table). Do NOT create a "
        "replication slot by hand"
    ) in guidance
    assert guidance.index(_DELETE) < guidance.index("Re-run Full Load")


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_stopped_stacks_inheritance_block_asks_for_new_topics_before_its_full_load(
    monkeypatch, cdc_only
) -> None:
    # FAILS WITHOUT THE FIX: only a rolled-back stack was told to get new topics before the
    # Full Load run its own fix names; a stopped one has the same unapplied tail.
    state = _fresh(_cdc_only() if cdc_only else None)
    _probe(monkeypatch, state, STOPPED, "UPDATE_COMPLETE")
    state.set_cdc_start_mode("manual")  # snapshot.mode=initial reads the parent's children
    _source(monkeypatch, _slot(AT_START), parents=_ORDERS_PARENT)
    _ui, notices, _events = _dialog(monkeypatch, state)
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == pg.INHERITANCE_SNAPSHOT_HEADER
    # G25 round 6: a stack started before has a fixed table set, so no untick is named.
    assert body.endswith(f"and its CDC starts without a snapshot. {_STARTED_INHERITANCE}")
    assert "untick" not in body


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_stopped_stacks_resume_cards_state_the_topics_condition(monkeypatch, cdc_only) -> None:
    # FAILS WITHOUT THE FIX: "No gap either way." / "picks up where streaming stopped, with no
    # gap" -- the changes the sink had not applied at Stop come only from the CDC topics.
    from tests.test_g25_pg_r4d import _start_card

    state = _fresh(_cdc_only() if cdc_only else None)
    _probe(monkeypatch, state, STOPPED, "UPDATE_COMPLETE")
    if not cdc_only:
        state.job_id = "fl-job"
    from tests.test_g25_pg import _watermark

    card = _start_card(monkeypatch, state, watermark=None if cdc_only else _watermark())
    assert "Ready to resume CDC" in card
    for absent in ("No gap either way", "with no gap"):
        assert absent not in card, absent
    assert (
        "Changes the sink had not applied when CDC stopped are applied from the CDC topics, so "
        "this is gapless only while the topics still retain them: once their retention period "
        "on MSK has passed, a Start here would skip them, so instead "
        f"{_NEW_TOPICS_CDC if cdc_only else _NEW_TOPICS_FL}."
    ) in card


# -- finding 1: a rolled-back Start promises no snapshot outcome Debezium does not guarantee ----

_INCOMPLETE = (
    "If that Start (or the earlier run) had not completed its initial snapshot, or nothing was "
    "recorded, Debezium snapshots every selected table first, then streams"
)


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc-manual", "cdc-only"])
def test_a_rolled_back_start_card_does_not_claim_no_snapshot(monkeypatch, cdc_only) -> None:
    # FAILS WITHOUT THE FIX: "continues from the position the rolled-back Start recorded, if it
    # recorded one (...), with no snapshot. If no position was ever recorded, Debezium snapshots
    # every selected table first" -- a Start rolled back mid-snapshot recorded a position whose
    # snapshot had not completed, and Debezium 2.7.4 snapshots again from it.
    from tests.test_g25_pg import _watermark
    from tests.test_g25_pg_r4d import _start_card

    state = _fresh(_cdc_only() if cdc_only else None)
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    if not cdc_only:
        state.job_id = "fl-job"
        state.set_cdc_start_mode("manual")
    card = _start_card(monkeypatch, state, watermark=None if cdc_only else _watermark())
    assert "Ready to start CDC again" in card
    assert "continues from the position the rolled-back Start recorded, if it recorded one" in card
    assert _INCOMPLETE in card
    for absent in ("with no snapshot", "If no position was ever recorded"):
        assert absent not in card, absent


def test_a_rolled_back_never_mode_start_still_says_no_snapshot(monkeypatch) -> None:
    # Guard: snapshot.mode=never never snapshots, whatever was recorded.
    from tests.test_g25_pg import _watermark
    from tests.test_g25_pg_r4d import _start_card

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    state.job_id = "fl-job"
    card = _start_card(monkeypatch, state, watermark=_watermark())
    assert "with no snapshot (snapshot.mode=never)" in card and _INCOMPLETE not in card


@pytest.mark.parametrize("start_mode", ["auto", "manual"])
def test_a_rolled_back_start_line_does_not_claim_no_snapshot(monkeypatch, start_mode) -> None:
    # FAILS WITHOUT THE FIX: the permanent record said "with no snapshot; if no position was
    # ever recorded, ..." (CDC only) and "Manual's full snapshot runs only if no position was
    # ever recorded on this stack" (Manual).
    from tests.test_g25_pg_r4d import _cdc_only_start, _start_line

    state = _fresh(_cdc_only() if start_mode == "auto" else None)
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    state.set_cdc_start_mode(start_mode)
    _source(monkeypatch, _slot(AT_START))
    run = _cdc_only_start(monkeypatch, state) if start_mode == "auto" else _start(
        monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]
    line = _start_line(run)
    assert (
        "if that Start (or the earlier run) had not completed its initial snapshot, or nothing "
        "was recorded, Debezium snapshots every selected table first, then streams from the slot"
    ) in line
    assert "with no snapshot" not in line and "no position was ever recorded" not in line
    if start_mode == "manual":
        assert line.endswith(
            "Manual's full snapshot runs only if nothing was recorded on this stack, or the run "
            "that recorded its position had not completed its initial snapshot")


# -- finding 2: the start-point card above the Start card, on a rolled-back stack ---------------


def _start_point_card(monkeypatch, state, *, watermark=None) -> str:
    """The REAL CDC source-config card (whose first card is the start-point card), as text."""
    from dsql_migrator.core.job_manager import JobNotFoundError
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _CardUi

    fl_job = SimpleNamespace(job_id="fl-job", watermark=watermark, status="DONE")

    class _JM:
        def get_status(self, job_id):
            if job_id == "fl-job" and watermark is not None:
                return fl_job
            raise JobNotFoundError(job_id)

    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _CardUi()
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=None, target_config=None, aws_profile=None,
    )
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(), lambda: None, inventory=None,
        migration_type=getattr(state, "migration_type", None), session=session,
    )
    return " ".join(ui.texts)


@pytest.mark.parametrize(
    "case", ["cdc-only", "manual", "automatic", "stopped-then-rolled-back"],
)
def test_a_rolled_back_stacks_start_point_card_resumes_like_the_start_card(
    monkeypatch, case
) -> None:
    # FAILS WITHOUT THE FIX: CDC only "This CDC start will snapshot every selected table first
    # ... re-snapshot every table (snapshot.mode=initial)"; Manual "Manual takes a fresh copy
    # instead"; Automatic "Start point set — gapless from the replication slot"; a stopped stack
    # whose next Start rolled back "Resuming from this stack's replication slot ... No gap
    # either way." -- right above the Start card that promises none of it.
    from tests.test_g25_pg import _watermark

    cdc_only = case == "cdc-only"
    state = _fresh(_cdc_only() if cdc_only else None)
    _probe(monkeypatch, state, STOPPED if case == "stopped-then-rolled-back" else NEVER_STARTED,
           ROLLED_BACK)
    if not cdc_only:
        state.job_id = "fl-job"
    state.set_cdc_start_mode("manual" if case == "manual" else "auto")
    card = _start_point_card(monkeypatch, state, watermark=None if cdc_only else _watermark())
    assert "Starting again after a rolled-back Start" in card
    assert "CloudFormation rolled back this stack's last Start" in card
    assert "gapless only while the topics still retain them" in card
    assert f"so instead {_NEW_TOPICS_CDC if cdc_only else _NEW_TOPICS_FL}." in card
    for absent in ("This CDC start will snapshot every selected table first",
                   "re-snapshot every table (snapshot.mode=initial)",
                   "Manual takes a fresh copy instead", "Start point set",
                   "Resuming from this stack's replication slot", "No gap either way"):
        assert absent not in card, absent
    if case in ("automatic", "stopped-then-rolled-back"):  # Automatic: snapshot.mode=never
        assert "with no snapshot (snapshot.mode=never)" in card and _INCOMPLETE not in card
    else:
        assert _INCOMPLETE in card and "with no snapshot" not in card


def test_a_never_started_stacks_start_point_card_is_unchanged(monkeypatch) -> None:
    # Guard: no Start yet -- the re-snapshot disclosure (CDC only) and the gapless confirmation
    # (Automatic) stay.
    from tests.test_g25_pg import _watermark

    only = _fresh(_cdc_only())
    _probe(monkeypatch, only, NEVER_STARTED, "CREATE_COMPLETE")
    card = _start_point_card(monkeypatch, only)
    assert "This CDC start will snapshot every selected table first" in card
    assert "rolled back" not in card
    auto = _fresh()
    _probe(monkeypatch, auto, NEVER_STARTED, "UPDATE_COMPLETE")
    auto.job_id = "fl-job"
    card = _start_point_card(monkeypatch, auto, watermark=_watermark())
    assert f"Start point set — gapless from the replication slot (WAL LSN {AT_START})" in card


# -- finding 3: the route after a withheld re-snapshot is named only where it works -------------


def test_a_slot_of_another_database_names_the_full_load_after_its_own_fix(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Drop it, or use a different CDC stack name ... Re-run Full Load
    # ... instead: it creates the slot again" -- that Full Load refuses such a slot before any
    # write ("give this migration its own CDC stack name, then run the Full Load again").
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(None, slot_present_any_database=True, slot_usable=False))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", SLOT_MISSING), ("error", SLOT_UNUSABLE)]
    refusal = notices[1][2]
    assert refusal.endswith(
        "Drop it, or use a different CDC stack name so the derived slot name differs. Then "
        f"{_NO_SNAPSHOT_FL}.")
    assert " instead: it creates the slot again" not in refusal
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False


def test_a_cdc_only_withheld_re_snapshot_names_the_type_switch(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: CDC only after a type switch (its Full Load recorded a slot, so
    # this Start sends snapshot.mode=never), and app.orders is an inheritance parent: the notice
    # said "Re-run Full Load on the Full Load step ... instead" -- a step CDC only does not have.
    state = _fresh(_cdc_only())
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    state.job_id = "fl-job"
    _source(monkeypatch, _slot(MOVED), parents=_ORDERS_PARENT)
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", SLOT_REFUSAL), ("error", pg.INHERITANCE_SNAPSHOT_HEADER)]
    # G25 round 6: CDC only's table picker is locked while its infrastructure exists: no
    # untick is named.
    assert notices[1][2].endswith(
        f"and its CDC starts without a snapshot. {_REFUSED_SNAPSHOT[True]}")
    assert f"{_RERUN} instead" not in notices[1][2]
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False
    # Guard: Full load + CDC keeps the Full Load re-run, its own step.
    full = _fresh()
    _probe(monkeypatch, full, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(MOVED), parents=_ORDERS_PARENT)
    _ui, notices, _events = _dialog(monkeypatch, full)
    assert notices[1][2].endswith(_REFUSED_SNAPSHOT[False])


# -- finding 4: a publication that misses a table on a never-started stack names its route -----


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_publication_that_misses_a_table_names_the_route_that_works(monkeypatch,
                                                                      cdc_only) -> None:
    # FAILS WITHOUT THE FIX: the re-snapshot grades coverage too, so its refusal had the same
    # header, and the dialog showed the block alone with a disabled Start -- no way out named.
    state = _fresh(_cdc_only() if cdc_only else None)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(AT_START, publication_tables=frozenset({"app.other"})))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", COVERAGE)]
    route = _NEVER[COVERAGE][cdc_only]
    assert notices[0][2].endswith(f"which is worse than a visible failure. {route}")
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False
    if cdc_only:
        assert "Full Load" not in route
    # ...and so does the job's refusal (Retry CDC, a stale dialog).
    if cdc_only:
        from tests.test_g25_pg_r4d import _cdc_only_start

        run = _cdc_only_start(monkeypatch, state)
    else:
        run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and str(run["error"]).splitlines()[1].endswith(route)


def test_the_workers_never_started_refusal_names_both_ways_out(monkeypatch) -> None:
    # The job does not judge the re-snapshot remedy, so a never-started stack's refusal names
    # it as the dialog offers it, and the Full Load re-run beside it.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(None, slot_present_any_database=False, slot_usable=False))
    run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {}
    assert str(run["error"]).splitlines()[1].endswith(f"{_UNCHECKED}{_NO_SNAPSHOT_FL}.")
