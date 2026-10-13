# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 6, package CDC: the PostgreSQL CDC findings.

* Decision A (findings 0 and 6): ONE answer to "has this CDC stack ever streamed, or rolled
  back a Start?" for MySQL and PostgreSQL alike (``_cdc_status.cdc_stack_has_streamed``, read
  from the cached stack probe) -- what the Full Load step gates a Full Load that takes a new
  start point on. Stop CDC removes the sink without a drain, so its group would apply changes
  older than a new snapshot over the reloaded tables. The resume card and the start line no
  longer describe a resume from a slot a Full Load re-created after the stack streamed.
* Findings 1 and 7: the CDC monitor's lost-slot notice names the route rule's route (new topics
  first; CDC only: no Full Load step), not "Re-run Full Load".
* Finding 8 (decision C): a narrowed publish list's own fix comes FIRST and is not enough:
  pgoutput decides what to publish from the publication as it stood at each change's commit,
  so the route rule's route follows it.
* Findings 3 and 10: the inheritance block names unticking only while the table picker can
  change the set, and a recorded PostgreSQL Manual mode has a way back to the Full Load's slot
  (the start-point card's control the block names), so it never loops.
* Finding 4: a same-named slot in another database beside an inheritance parent: the refusal
  of the re-snapshot leads with that slot's own fix, never a Full Load re-run it refuses.
* Finding 5: stopped, then a Start rolled back: the retention bound counts from the Stop.

Each finding's test FAILS on the code before this round; the never-started and first-rollback
cases are guards. Fakes only: no real database or AWS.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from dsql_migrator.core import cdc_pg_slot as pg
from dsql_migrator.core.models import SourceType
from dsql_migrator.ui.data_migration._models import MigrationType
from tests.test_g25_pg import AT_START, MOVED, SLOT_REFUSAL, _watermark
from tests.test_g25_pg_r4 import (
    NEVER_STARTED,
    RUNNING,
    STOPPED,
    _cdc_only,
    _dialog,
    _fresh,
    _headers,
    _pg_session,
    _start,
)
from tests.test_g25_pg_r4c import (
    REMEDY,
    ROLLED_BACK,
    SLOT_MISSING,
    SLOT_UNUSABLE,
    _probe,
    _slot,
    _source,
)
from tests.test_g25_pg_r4d import ROLLBACK_STATUSES, _start_card, _start_line
from tests.test_g25_pg_r5 import (
    _NEW_TOPICS_CDC,
    _NEW_TOPICS_FL,
    _NO_SNAPSHOT_CDC,
    _NO_SNAPSHOT_FL,
    _STARTED_AFTER_FIX,
    _start_point_card,
)

_ORDERS_PARENT = (("app.orders", 1, ("app.orders_2026",)),)
_DELETE = "Delete CDC infrastructure and deploy it again"


def _error_line(run) -> str:
    """The refusal notice body the Start CDC job raised (its second line)."""
    return str(run["error"]).splitlines()[1]


# ---------------------------------------------------------------------------
# Decision A: the one "has this stack streamed" answer, both engines
# ---------------------------------------------------------------------------

# (parameters, status) as the real stack-phase probe reads them -> streamed?
_STACKS = {
    "never-started": (NEVER_STARTED, "CREATE_COMPLETE", False),
    "stopped": (STOPPED, "UPDATE_COMPLETE", True),
    "connectors-in-place": (RUNNING, "UPDATE_COMPLETE", True),
    "stopped-then-rolled-back": (STOPPED, ROLLED_BACK, True),
    **{f"rolled-back-{s.lower()}": (NEVER_STARTED, s, True) for s in ROLLBACK_STATUSES},
}


@pytest.mark.parametrize("engine", ["mysql", "postgres"])
@pytest.mark.parametrize("stack", list(_STACKS))
def test_one_answer_says_whether_the_stack_has_streamed(monkeypatch, stack, engine) -> None:
    # FAILS WITHOUT THE FIX: no such helper -- the Full Load step had only "Stop CDC first"
    # (cdc_streaming_started), which is False on a stopped or rolled-back stack, so a Full Load
    # at a new start point ran and the next Start applied the sink's old tail over it.
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_state import cdc_streaming_started
    from dsql_migrator.ui.data_migration._cdc_status import cdc_stack_has_streamed

    parameters, status, streamed = _STACKS[stack]
    # Each engine's stack carries its own parameters beside the two the answer reads.
    extra = (
        {"WatermarkBinlogFile": "mysql-bin-changelog.000007", "WatermarkBinlogPos": "4",
         "StartGtidMode": "ON", "SnapshotMode": "schema_only"}
        if engine == "mysql"
        else {"PgSlotName": "dsqlmig_s", "EngineType": "postgres"}
    )
    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, {**parameters, **extra}, status)
    assert cdc_stack_has_streamed(state) is streamed, (stack, engine)
    # The PostgreSQL started rule is the same answer, whatever the engine reads it.
    assert _cdc_ui._pg_stack_started(state) is streamed
    if stack in ("stopped", "stopped-then-rolled-back") or stack.startswith("rolled-back"):
        # The gap the gate closes: "Stop CDC first" does not hold any of these back.
        assert cdc_streaming_started(state, None) is False


def test_the_streamed_answer_is_pure_and_reads_only_the_cached_probe() -> None:
    from dsql_migrator.ui.data_migration._cdc_status import (
        cdc_stack_has_streamed,
        cdc_start_rolled_back,
    )

    assert cdc_stack_has_streamed(SimpleNamespace()) is False  # never probed
    assert cdc_stack_has_streamed(SimpleNamespace(cdc_has_committed_offset=True)) is True
    assert cdc_stack_has_streamed(SimpleNamespace(cdc_stack_phase="running")) is True
    assert cdc_stack_has_streamed(SimpleNamespace(cdc_connector_names=["x-dsql-sink"])) is True
    rolled = SimpleNamespace(cdc_stack_phase_status=" update_rollback_complete ")
    assert cdc_start_rolled_back(rolled) is True and cdc_stack_has_streamed(rolled) is True
    assert cdc_stack_has_streamed(SimpleNamespace(cdc_stack_phase_status="ROLLBACK_COMPLETE")) \
        is False


def test_a_streamed_stacks_resume_is_never_described_after_a_recreated_slot(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the resume card said "... or from the Full Load's start point if
    # nothing has streamed from the slot since that load created it", and the start line "or
    # the Full Load watermark ... if a Full Load run since then recreated the slot" -- a resume
    # after a new-start-point Full Load, presented as gapless; with the gate that path is gone.
    from tests.test_g25_cdc_ui import _WM_WITH_TABLE, _pg_resume_surfaces

    _state, start, card, _stop = _pg_resume_surfaces(
        MigrationType.FULL_LOAD_AND_CDC, watermark=_WM_WITH_TABLE)
    for surface in (start, card):
        assert "since that load created it" not in surface
        assert "or from the Full Load's start point if its source connector never streamed." \
            in surface
    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, STOPPED, "UPDATE_COMPLETE")
    run = _start(monkeypatch, state=state, confirmed=MOVED)
    assert run["error"] is None and run["kwargs"]
    line = _start_line(run)
    assert "recreated the slot" not in line
    assert "if the connector never streamed from it)" in line, line


# ---------------------------------------------------------------------------
# Findings 1 and 7: the lost-slot notice names the route rule's route
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_the_lost_slot_notice_names_new_topics_first(monkeypatch, mtype) -> None:
    # FAILS WITHOUT THE FIX: "... cannot resume gaplessly. Re-run Full Load to re-establish a
    # consistency point." -- on a started stack that replays the sink's tail over the reload,
    # and CDC only has no Full Load step.
    from dsql_migrator.core.cdc_postgres import SlotHealth
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    shown: list = []
    monkeypatch.setattr(cm, "render_notice", lambda _ui, **kw: shown.append(kw))
    state = SimpleNamespace(
        cdc_slot_health=SlotHealth("dsqlmig_s", exists=True, active=False, wal_status="lost"),
        migration_type=mtype,
    )
    cm._render_cdc_slot_health(object(), state)
    (notice,) = shown
    assert (notice["tone"], notice["header"]) == ("error", "Replication slot invalidated")
    cdc_only = mtype is MigrationType.CDC_ONLY
    route = pg.pg_cdc_route(pg.SLOT_MISSING_HEADER, started=True, cdc_only=cdc_only)
    assert notice["body"] == (
        f"The source removed WAL this slot needed, so CDC cannot resume gaplessly. {route}")
    assert notice["body"].endswith(f"{_NEW_TOPICS_CDC if cdc_only else _NEW_TOPICS_FL}.")
    assert "re-establish a consistency point" not in notice["body"]
    if cdc_only:
        assert "Full Load" not in notice["body"]
    else:
        assert notice["body"].index(_DELETE) < notice["body"].index("Re-run Full Load")


# ---------------------------------------------------------------------------
# Finding 8 (decision C): a narrowed publish list's fix first, then the route
# ---------------------------------------------------------------------------

_FIX_FIRST = (
    "As its owner, run ALTER PUBLICATION \"pub\" SET (publish = 'insert, update, delete') on "
    "the source first. Setting the list back does not bring back the changes committed while "
    "it was narrowed: pgoutput decides what to publish from the publication as it stood when "
    "each change committed, so a Start that continues from the slot would still skip them."
)


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_narrowed_publish_list_on_a_never_started_stack_names_the_full_load_after_its_fix(
    monkeypatch, cdc_only
) -> None:
    # FAILS WITHOUT THE FIX: "... As its owner, run ALTER PUBLICATION ... on the source, then
    # Start CDC again." -- the Start then resumes from the slot, and every UPDATE and DELETE
    # committed while the list was narrowed is missing on Aurora DSQL, with no error.
    state = _fresh(_cdc_only() if cdc_only else MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    reads = _source(monkeypatch, _slot(AT_START, publication_publishes_all_dml=False))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", pg.PUBLISH_LIST_HEADER)]
    body = notices[0][2]
    route = f"Then {_NO_SNAPSHOT_CDC if cdc_only else _NO_SNAPSHOT_FL}."
    assert body.endswith(f"{_FIX_FIRST} {route}")
    assert "then Start CDC again" not in body
    # No re-snapshot is offered (it reuses the publication as it is) and none is judged.
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False
    assert reads.count("objects") == 1  # no second read: the remedy is not judged
    # The job's refusal (Retry CDC, a stale dialog) names the same way out.
    if cdc_only:
        from tests.test_g25_pg_r4d import _cdc_only_start

        run = _cdc_only_start(monkeypatch, state)
    else:
        run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and _error_line(run).endswith(f"{_FIX_FIRST} {route}")
    assert len(str(run["error"]).splitlines()[0]) <= 300


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_narrowed_publish_list_on_a_stopped_stack_names_new_topics_after_its_fix(
    monkeypatch, cdc_only
) -> None:
    # FAILS WITHOUT THE FIX: the stopped stack got the bare fix "then Start CDC again" -- the
    # resume from its slot skips the narrowed window, and the route rule's new-topics route
    # was skipped.
    state = _fresh(_cdc_only() if cdc_only else MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, STOPPED, "UPDATE_COMPLETE")
    if cdc_only:
        state.job_id = "fl-job"  # a Full Load recorded the slot (a type switch since)
    _source(monkeypatch, _slot(AT_START, publication_publishes_all_dml=False))
    ui, notices, _events = _dialog(monkeypatch, state)
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == pg.PUBLISH_LIST_HEADER
    assert body.endswith(f"{_FIX_FIRST} {_STARTED_AFTER_FIX[cdc_only]}")
    assert body.index("ALTER PUBLICATION") < body.index(_DELETE)
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False


# ---------------------------------------------------------------------------
# Findings 3 and 10: the inheritance block's fixes are ones the UI can carry out
# ---------------------------------------------------------------------------


class _JM:
    """A job manager holding the Full Load's record ``fl-job`` (DONE)."""

    def __init__(self, watermark):
        self.job = SimpleNamespace(job_id="fl-job", watermark=watermark, status="DONE",
                                   table_states={})

    def get_status(self, job_id):
        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id == "fl-job":
            return self.job
        raise JobNotFoundError(job_id)


def _dialog_jm(monkeypatch, state, watermark):
    """The REAL Start CDC dialog with a job manager the table picker's lock can read (the
    publication/slot gate reads whatever source the caller patched in). Returns
    ``(ui, notices)``, notices as ``(tone, header, body)``."""
    import nicegui

    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _DialogUi, _g5_inventory

    ui = _DialogUi()
    notices: list = []

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        notices.append((tone, header, body))

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    state.job_id = "fl-job"
    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_probe_binlog_resume_gap", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_log_cdc_event", lambda *a, **k: None)
    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    asyncio.run(_cdc_ui._open_cdc_start_dialog(
        ui, state, lambda: None, session=_pg_session(), job_manager=_JM(watermark),
        inventory=_g5_inventory(),
    ))
    return ui, notices


def _slotless_watermark():
    """A finished Full Load that recorded no slot: a Start from it snapshots every table."""
    from dsql_migrator.core.models import Watermark

    wm = _watermark()
    return Watermark(wal_lsn=wm.wal_lsn, snapshot_timestamp=wm.snapshot_timestamp,
                     table_row_counts=dict(wm.table_row_counts))


def test_the_inheritance_block_names_untick_only_while_the_picker_is_open(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "... or untick app.orders" on every surface, though the table
    # picker locks for good once a Start was submitted (cdc_ops_window_start, never cleared by
    # Stop or Delete) and the job's refusal always comes after one.
    from datetime import datetime, timezone

    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(AT_START), parents=_ORDERS_PARENT)
    _ui, notices = _dialog_jm(monkeypatch, state, _slotless_watermark())
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == pg.INHERITANCE_SNAPSHOT_HEADER
    # Guard: the set can still change (Full load + CDC, CDC never started): untick is named.
    assert body.endswith("or untick app.orders: each child table migrates as a table of its own.")
    # A Start was submitted (the job refused it, say): the set is fixed from then on.
    state.set_cdc_ops_window_start(datetime(2026, 10, 11, tzinfo=timezone.utc))
    _ui, notices = _dialog_jm(monkeypatch, state, _slotless_watermark())
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == pg.INHERITANCE_SNAPSHOT_HEADER
    assert "untick" not in body
    assert body.endswith("and its CDC starts without a snapshot.")
    # The job never names it: a submitted Start fixes the set.
    run = _start(monkeypatch, state=_fresh(MigrationType.FULL_LOAD_AND_CDC),
                 watermark=_slotless_watermark())
    assert run["kwargs"] == {}
    assert pg.INHERITANCE_SNAPSHOT_HEADER in str(run["error"]) and "untick" not in str(
        run["error"])


def _card(monkeypatch, state, watermark):
    """The REAL CDC source-config card (its first card is the start-point card): the ui, its
    notices' headers and bodies recorded as text."""
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _CardUi

    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _CardUi()
    monkeypatch.setattr(
        _cdc_ui, "render_notice",
        lambda _ui, *, header="", body="", **_k: ui.texts.extend((header, body)))
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=None, target_config=None, aws_profile=None,
    )
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(watermark), lambda: None, inventory=None,
        migration_type=state.migration_type, session=session,
    )
    return ui


def test_a_recorded_manual_mode_has_a_way_back_to_the_full_loads_slot(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the re-snapshot remedy, taken unchecked (the second read failed),
    # recorded Manual; the job then refused its snapshot of an inheritance parent with "Start
    # CDC from a Full Load's snapshot point instead" -- this migration already IS such a run,
    # and no PostgreSQL control set the mode back, so every later Start (a Full Load re-run
    # keeps Manual) was refused the same way: only Start over was left.
    from dsql_migrator.ui.data_migration._cdc_ui import (
        PG_RESUME_FROM_SLOT_ACTION,
        PG_RESUME_FROM_SLOT_HINT,
    )

    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    state.set_cdc_start_mode("manual")  # "Re-snapshot every table instead" was taken
    _source(monkeypatch, _slot(AT_START), parents=_ORDERS_PARENT)
    ui, notices = _dialog_jm(monkeypatch, state, _watermark())
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == pg.INHERITANCE_SNAPSHOT_HEADER
    assert body.endswith(PG_RESUME_FROM_SLOT_HINT)
    assert PG_RESUME_FROM_SLOT_ACTION in PG_RESUME_FROM_SLOT_HINT
    assert ui.buttons["Start CDC"].enabled is False and REMEDY not in ui.buttons
    # The job's refusal names the same way back.
    run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and PG_RESUME_FROM_SLOT_HINT in _error_line(run)
    # The start-point card carries the control the notice names, and it sets Automatic.
    card = _card(monkeypatch, state, _watermark())
    (back,) = [b for b in card.buttons if b.text == PG_RESUME_FROM_SLOT_ACTION]
    assert "Manual re-snapshots instead of resuming from this load's slot" in card.texts
    assert f'use "{PG_RESUME_FROM_SLOT_ACTION}" on the CDC start point card' in " ".join(
        card.texts)
    back.on_click()
    assert state.cdc_start_mode() == "auto"
    after = _card(monkeypatch, state, _watermark())
    assert PG_RESUME_FROM_SLOT_ACTION not in [b.text for b in after.buttons]
    # The Start that resumes from the slot takes no snapshot, so it is not refused.
    _ui, notices = _dialog_jm(monkeypatch, state, _watermark())
    assert not [n for n in notices if n[0] == "error"], notices
    run = _start(monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]


def test_no_way_back_is_named_where_there_is_no_slot_to_resume_from(monkeypatch) -> None:
    # Guard: a load that recorded no slot (or a CDC only Start) snapshots whatever the mode.
    from dsql_migrator.ui.data_migration._cdc_ui import PG_RESUME_FROM_SLOT_HINT

    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    state.set_cdc_start_mode("manual")
    _source(monkeypatch, _slot(AT_START), parents=_ORDERS_PARENT)
    _ui, notices = _dialog_jm(monkeypatch, state, _slotless_watermark())
    ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
    assert header == pg.INHERITANCE_SNAPSHOT_HEADER and PG_RESUME_FROM_SLOT_HINT not in body


# ---------------------------------------------------------------------------
# Finding 4: another database's slot beside an inheritance parent
# ---------------------------------------------------------------------------


def test_another_databases_slot_leads_the_refusal_before_an_inheritance_parent(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: the re-snapshot's refusal was the inheritance block, ending "Re-run
    # Full Load ... instead: it creates the slot again at its own snapshot point" -- and that
    # Full Load refuses the same-named slot of the other database before any write.
    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(None, slot_present_any_database=True, slot_usable=False),
            parents=_ORDERS_PARENT)
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", SLOT_MISSING), ("error", SLOT_UNUSABLE)]
    refusal = notices[1][2]
    assert refusal.endswith(
        "Drop it, or use a different CDC stack name so the derived slot name differs. Then "
        f"{_NO_SNAPSHOT_FL}.")
    assert " instead: it creates the slot again" not in refusal
    assert pg.INHERITANCE_SNAPSHOT_HEADER not in [h for _t, h, _b in notices]
    assert REMEDY not in ui.buttons and ui.buttons["Start CDC"].enabled is False


_ELSEWHERE = (
    "A slot of that name does exist in this cluster (invalidated, in another database, or not "
    "a logical pgoutput slot): if it is another database's or of another type, drop it or give "
    "this migration its own CDC stack name first -- a Full Load re-run refuses such a slot "
    "before any write."
)


def test_the_jobs_missing_slot_refusal_names_another_databases_slot_first(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the job (Retry CDC, a stale dialog) does not judge the re-snapshot,
    # so its refusal named "... or Re-run Full Load ... (it creates the slot again ...)" alone --
    # a Full Load that refuses the other database's slot.
    state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(None, slot_present_any_database=True, slot_usable=False),
            parents=_ORDERS_PARENT)
    run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and SLOT_MISSING in str(run["error"])
    refusal = _error_line(run)
    assert _ELSEWHERE in refusal
    assert refusal.index(_ELSEWHERE) < refusal.index("Re-run Full Load")
    # Guard: no slot of that name anywhere -- nothing to drop first.
    gone = _fresh(MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, gone, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(None, slot_present_any_database=False, slot_usable=False))
    run = _start(monkeypatch, state=gone)
    assert SLOT_MISSING in str(run["error"]) and "does exist in this cluster" not in str(
        run["error"])


def test_the_blocker_reports_a_slot_it_cannot_use_before_an_inheritance_parent() -> None:
    from tests.test_g25_pg_publication import _objects

    objects = _objects(publication_tables=frozenset({"app.orders"}),
                       slot_present_any_database=True, slot_usable=False,
                       inheritance_children=_ORDERS_PARENT)
    verdict = pg.pg_replication_objects_blocker(
        objects, ["app.orders"], resumes_from_slot=False,
        connector_autocreates_publication=True, snapshots=True)
    assert verdict is not None and verdict[0] == pg.SLOT_UNUSABLE_HEADER
    # Guard: with a usable slot the parent still blocks the snapshot.
    usable = _objects(publication_tables=frozenset({"app.orders"}),
                      inheritance_children=_ORDERS_PARENT)
    verdict = pg.pg_replication_objects_blocker(
        usable, ["app.orders"], resumes_from_slot=False,
        connector_autocreates_publication=True, snapshots=True)
    assert verdict is not None and verdict[0] == pg.INHERITANCE_SNAPSHOT_HEADER


# ---------------------------------------------------------------------------
# Finding 5: stopped, then a Start rolled back -- the bound counts from the Stop
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_a_stopped_stack_whose_next_start_rolled_back_counts_retention_from_the_stop(
    monkeypatch, cdc_only
) -> None:
    # FAILS WITHOUT THE FIX: "once their retention period on MSK has passed since that Start"
    # -- the sink's tail from before the Stop is older, and expires first.
    stopped_first = (
        "once their retention period on MSK has passed since CDC was stopped, a Start here "
        "would skip them"
    )
    state = _fresh(_cdc_only() if cdc_only else MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, state, STOPPED, ROLLED_BACK)
    if not cdc_only:
        state.job_id = "fl-job"
    watermark = None if cdc_only else _watermark()
    for surface in (_start_card(monkeypatch, state, watermark=watermark),
                    _start_point_card(monkeypatch, state, watermark=watermark)):
        assert "CDC had been stopped before that Start" in surface
        assert (
            "The changes the sink had not applied when CDC stopped, and any that Start "
            "streamed, are applied from the CDC topics"
        ) in surface
        assert stopped_first in surface and "since that Start, a Start here" not in surface
        assert f"so instead {_NEW_TOPICS_CDC if cdc_only else _NEW_TOPICS_FL}." in surface
    # Guard: a FIRST Start rolled back (never stopped) keeps its bound from that Start.
    first = _fresh(_cdc_only() if cdc_only else MigrationType.FULL_LOAD_AND_CDC)
    _probe(monkeypatch, first, NEVER_STARTED, ROLLED_BACK)
    if not cdc_only:
        first.job_id = "fl-job"
    card = _start_card(monkeypatch, first, watermark=watermark)
    assert "since that Start, a Start here would skip them" in card
    assert "CDC had been stopped" not in card


def test_the_slot_refusal_still_judges_only_a_never_started_stack(monkeypatch) -> None:
    # Guard (the stateless slot-position rule stands): a stopped stack is not judged on the slot
    # position; a never-started one is.
    for parameters, refused in ((STOPPED, False), (NEVER_STARTED, True)):
        state = _fresh(MigrationType.FULL_LOAD_AND_CDC)
        _probe(monkeypatch, state, parameters,
               "UPDATE_COMPLETE" if parameters is STOPPED else "CREATE_COMPLETE")
        _source(monkeypatch, _slot(MOVED))
        _ui, notices, _events = _dialog(monkeypatch, state)
        assert (("error", SLOT_REFUSAL) in _headers(notices)) is refused
