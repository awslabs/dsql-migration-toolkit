# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G27 item 0, task CDC-0: a Start from the watermark of a Full Load that is not this migration's
first copy states the Drop & reload condition -- on the start-point card, in the Start CDC dialog
and on the Start's activity line.

Live Seoul E2E, 2026-10-14 (MySQL): Full load + CDC streamed on dsql-cdc-e2e26 from W1, Stop CDC,
the type switched to Full load only, Re-run Full Load with the default Append (W2), Delete CDC
infrastructure, the type back to Full load + CDC, Redeploy. The CDC step's start-point card said
"Automatic — gapless from Full Load (recommended)" and "Start point set — binlog ...W2" with no
Drop & reload condition. An append keeps the rows already on the target as they were, and CDC
from W2 carries only the changes after it: a row updated or deleted between the old stream's last
apply and W2 stays stale (the G26 RELOAD_FIRST condition).

The signal, with no new persisted field (``_cdc_status.drop_reload_condition_due``): the session's
``cdc_ops_window_start`` -- stamped by every Start CDC submission with the job watermark's
snapshot time, persisted with the session, cleared only by Start over and an Attach -- is earlier
than the Full Load watermark's snapshot time (Start CDC was submitted before that Full Load ran),
and a chunk of that Full Load holds rows "already there" (``rows_skipped``; a Drop & reload
recreates each table it loads, so it finds none). Both engines stamp and count it the same way.
The G27 repair adds a second sign that needs no Start -- a chunk that found rows already there
on its first attempt -- in tests/test_g27_repair_cdc.py; the records here (``_job``) carry no
attempt count, so they exercise the window sign alone.

Each test FAILS on the code before this change, except the guards that pin what stays: a first
copy, a Drop & reload re-run (either engine), a PostgreSQL re-snapshot and a Start from the same
watermark keep today's text (the last one also calls the new signal). Fakes only: no AWS.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import Watermark
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import ChunkState, MigrationJob, SourceType
from dsql_migrator.ui.cdc_start_point_gate import (
    RELOAD_CONDITION_HEADER,
    RELOAD_CONDITION_LINE,
    RELOAD_CONDITION_TOOLTIP,
    StartPointVerdict,
    reload_condition,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType, _cdc_ui
from dsql_migrator.ui.data_migration._cdc_status import drop_reload_condition_due

MYSQL = SourceType.MYSQL
POSTGRES = SourceType.POSTGRES
FL_CDC = MigrationType.FULL_LOAD_AND_CDC
CDC_ONLY = MigrationType.CDC_ONLY

T1 = datetime(2026, 10, 14, 1, 0, tzinfo=timezone.utc)
T2 = T1 + timedelta(hours=2)
# The Full Load the first stack streamed from (W1), and the Full load only Append re-run (W2).
W1 = Watermark(snapshot_timestamp=T1, binlog_file="mysql-bin.000090", binlog_position=4)
W2 = Watermark(snapshot_timestamp=T2, binlog_file="mysql-bin.000100", binlog_position=4)
# PostgreSQL: a Full load + CDC re-run creates its slot again at its own snapshot point.
PG_W2 = Watermark(
    snapshot_timestamp=T2, wal_lsn="0/5A000060", slot_name="dsqlmig_dsql_cdc_e2e26pg_a1b2",
    publication_name="dsqlmig_pub_a1b2", table_row_counts={"app.orders": 2890},
)

_TODAY_LABEL = "Automatic — gapless from Full Load (recommended)"
_CONDITION_LABEL = "Automatic — from the Full Load watermark (recommended)"
_TODAY_TOOLTIP = (
    "Where change streaming begins. Automatic resumes exactly where the Full Load snapshot ended "
    "(no gap, no overlap)."
)
_TODAY_LINE = "(gapless from the Full Load watermark)"
_GAPLESS_SET = "which is what makes the handoff gapless"
# The condition's exact words for Full load + CDC (either engine).
_FL_CDC_BODY = (
    "This migration started CDC from an earlier start point before this Full Load ran, and the "
    "Full Load found rows already on the target. Start CDC begins at its watermark, so if the Full "
    "Load appended to them, Re-run Full Load on the Full Load step choosing 'Drop & reload' before "
    "Start CDC. An append keeps those rows as they were, and CDC from a watermark carries only the "
    "changes after it."
)


def _job(wm, *, skipped=(2890,), job_id="fl-job", attempts=0):
    """The Full Load record: one DONE chunk per entry, each with that many rows "already there"
    (``rows_skipped``) -- what an append over an earlier copy counts, and a Drop & reload never
    does (it recreates each table first). ``attempts``: each chunk's attempt count -- 0 by
    default, which gives no first-attempt sign (G27 repair), so these tests see the window sign
    alone; tests/test_g27_repair_cdc.py builds the records with the real engine instead."""
    names = ("orders", "customers", "items")
    return MigrationJob(
        job_id=job_id,
        status="DONE",
        progress_pct=100.0,
        watermark=wm,
        chunks=[
            ChunkState(
                chunk_id=names[i], status="DONE", rows_loaded=10, rows_skipped=n,
                attempts=attempts,
            )
            for i, n in enumerate(skipped)
        ],
    )


class _JM:
    def __init__(self, job):
        self._job = job

    def get_status(self, job_id):
        if self._job is not None and job_id == self._job.job_id:
            return self._job
        raise JobNotFoundError(job_id)


def _start_click(monkeypatch, state, wm, *, job=None):
    """The real Start CDC click and job body for the Full Load ``wm`` (``job``: its record, when
    the click must see its chunks). Returns the capture (``details``: the activity lines)."""
    from tests.test_g25_cdc_r7 import _capture

    if job is not None:
        monkeypatch.setattr(
            _cdc_ui, "_current_job",
            lambda _jm, job_id: job if job_id == job.job_id else None,
        )
    return _capture(monkeypatch, state, wm)


def _new_stack_after_the_detour(monkeypatch, *, mtype=FL_CDC):
    """The live scenario up to the new stack: the REAL Start CDC from W1 (it stamps the ops
    window), then Stop CDC, the type switched to Full load only for an Append re-run (W2), Delete
    CDC infrastructure, the type back (or on to CDC only), Redeploy: a new stack, never started."""
    from tests.test_g25_cdc_r7 import _never_started

    from dsql_migrator.ui.design import render_notice

    state = _never_started()
    first = _start_click(monkeypatch, state, W1)
    assert first["submitted"] and first["error"] is None
    assert state.cdc_ops_window_start == T1  # the click stamped W1's snapshot time
    # The click's capture records notices instead of rendering them; the cards below render.
    monkeypatch.setattr(_cdc_ui, "render_notice", render_notice)
    # Stop CDC: the connectors are removed, the position stays committed.
    state.set_cdc_has_committed_offset(True)
    state.set_cdc_deployed_watermark(("mysql-bin.000090", "4"))
    # Full load only: Re-run Full Load with the default Append -- a new record, W2.
    state.migration_type = MigrationType.FULL_LOAD_ONLY
    state.job_id = "fl-job"
    # Delete CDC infrastructure: the stack is gone.
    state.set_cdc_stack_phase("absent")
    state.set_cdc_has_committed_offset(False)
    state.set_cdc_deployed_watermark(None)
    # Back to a CDC type; Redeploy (an infra job): a new stack whose connectors never started.
    state.migration_type = mtype
    state.set_cdc_deploy_job_id("redeploy-job", kind="infra")
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    return state


def _card(state, job, *, session=None):
    from tests.test_cdc_restart_start_point import _CardUi

    ui = _CardUi()
    _cdc_ui._render_cdc_source_config_card(
        ui, state, _JM(job), lambda: None, inventory=None,
        migration_type=getattr(state, "migration_type", None), session=session,
    )
    return ui


def _dialog(monkeypatch, state, job, *, session=None):
    """The REAL Start CDC dialog with every source/target read stubbed out (they are not what
    this is about). Returns the dialog double; ``ui.bodies`` maps a notice header to its body."""
    import asyncio

    import nicegui

    from tests.test_ui_data_migration import _DialogUi

    for name in (
        "_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_binlog_resume_gap",
        "_probe_cdc_blocking_foreign_keys", "_pg_objects_gate_block",
    ):
        monkeypatch.setattr(_cdc_ui, name, lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    ui = _DialogUi()
    ui.bodies = {}

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        ui.bodies[header] = body

    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)
    asyncio.run(
        _cdc_ui._open_cdc_start_dialog(
            ui, state, lambda: None, session=session, job_manager=_JM(job), inventory=None,
        )
    )
    return ui


def _start_line(run) -> str:
    return next(d for d in run["details"] if d and "start point" in d)


# --- the live scenario: MySQL, Full load + CDC ---------------------------------------------------


def test_the_live_detour_states_the_condition_on_the_card(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Automatic — gapless from Full Load (recommended)", the ⓘ "resumes
    # exactly where the Full Load snapshot ended (no gap, no overlap)", and no condition.
    state = _new_stack_after_the_detour(monkeypatch)
    card = _card(state, _job(W2))
    texts = " ".join(card.texts)
    assert _CONDITION_LABEL in texts and _TODAY_LABEL not in texts
    assert RELOAD_CONDITION_HEADER in texts and _FL_CDC_BODY in texts
    assert RELOAD_CONDITION_TOOLTIP in card.tooltips and _TODAY_TOOLTIP not in card.tooltips
    # Not a block: the start point stays set and ready.
    assert "Start point set — binlog mysql-bin.000100:4" in texts
    assert "Ready" in texts and "Action needed" not in texts


def test_the_live_detour_states_the_condition_in_the_dialog(monkeypatch) -> None:
    state = _new_stack_after_the_detour(monkeypatch)
    ui = _dialog(monkeypatch, state, _job(W2))
    assert ("warning", RELOAD_CONDITION_HEADER) in ui.notices
    assert ui.bodies[RELOAD_CONDITION_HEADER] == _FL_CDC_BODY
    # Advisory: the tool cannot tell whether that Full Load appended, so Start stays enabled.
    assert ui.buttons["Start CDC"].enabled is True


def test_the_live_detour_records_the_condition_on_the_start_line(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the durable activity line certified "(gapless from the Full Load
    # watermark)" for the Start from W2.
    state = _new_stack_after_the_detour(monkeypatch)
    run = _start_click(monkeypatch, state, W2, job=_job(W2))
    assert run["submitted"] and run["error"] is None
    line = _start_line(run)
    assert "start point binlog mysql-bin.000100:4" in line
    assert RELOAD_CONDITION_LINE in line and _TODAY_LINE not in line
    # The Start moved the window to W2, which ends the window sign. (This record carries no
    # attempt count; a real one keeps the condition by its first-attempt sign -- G27 repair,
    # tests/test_g27_repair_cdc.py, a failed Start then a retry.)
    assert state.cdc_ops_window_start == T2
    assert drop_reload_condition_due(state, _job(W2)) is False


def test_the_live_detour_drops_the_gapless_claim_from_the_will_stream_tooltip(
    monkeypatch,
) -> None:
    from tests.test_ui_data_migration import _RecordingUi

    state = _new_stack_after_the_detour(monkeypatch)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_button(ui, state, _JM(_job(W2)), lambda: None,
                                     inventory=None, session=None)
    hover = " ".join(ui.tooltips)
    assert "drawn from the Full Load snapshot" in hover and _GAPLESS_SET not in hover


def test_a_manual_point_does_not_call_automatic_the_gapless_handoff(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Choose Automatic for the gapless handoff."
    state = _new_stack_after_the_detour(monkeypatch)
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000110", binlog_pos=4)
    texts = " ".join(_card(state, _job(W2)).texts)
    assert "Your manual point skips part of the stream" in texts
    assert "Choose Automatic for the gapless handoff." not in texts
    assert (
        "Choose Automatic instead: it starts at the snapshot's end, which is gapless only if that "
        "Full Load did not append to rows already on the target."
    ) in texts
    # The notice itself is for Automatic's start: a Manual one is not from the watermark.
    assert RELOAD_CONDITION_HEADER not in texts


# --- what stays: a first copy, a Drop & reload re-run, a Start from the same watermark ----------


def test_a_first_copy_keeps_todays_text(monkeypatch) -> None:
    # Guard: a first Full Load whose Retry re-read the rows it had partly loaded (rows "already
    # there"), with no Start before it -- the normal first-run flow is not alarmed. The re-run
    # table carries its earlier attempt (``_seed_retry_chunks``), so it is on its 2nd (G27
    # repair: a first attempt that found rows is the sign this guard must not give).
    from tests.test_g25_cdc_r7 import _never_started
    from tests.test_ui_data_migration import _RecordingUi

    state = _never_started()
    state.job_id = "fl-job"
    job = _job(W1, skipped=(120,), attempts=2)
    card = _card(state, job)
    assert _TODAY_LABEL in " ".join(card.texts) and _TODAY_TOOLTIP in card.tooltips
    assert RELOAD_CONDITION_HEADER not in " ".join(card.texts)
    ui = _dialog(monkeypatch, state, job)
    assert RELOAD_CONDITION_HEADER not in [h for _t, h in ui.notices]
    hover = _RecordingUi()
    _cdc_ui._render_cdc_start_button(hover, state, _JM(job), lambda: None, inventory=None,
                                     session=None)
    assert _GAPLESS_SET in " ".join(hover.tooltips)
    run = _start_click(monkeypatch, state, W1, job=job)
    assert _TODAY_LINE in _start_line(run)


def test_a_start_from_the_same_watermark_keeps_todays_text(monkeypatch) -> None:
    # Guard: a Start from W1 (it stamped the window with W1), then a Retry or a Reload, which
    # keep the watermark: their rows "already there" are this copy's own.
    from tests.test_g25_cdc_r7 import _never_started

    state = _never_started()
    _start_click(monkeypatch, state, W1)
    job = _job(W1, skipped=(5, 0))
    assert drop_reload_condition_due(state, job) is False
    assert _TODAY_LABEL in " ".join(_card(state, job).texts)


def test_a_drop_and_reload_rerun_keeps_todays_text(monkeypatch) -> None:
    # Guard: the same detour with Drop & reload -- every table recreated before its rows, so the
    # re-run found none already there: a first copy, today's text.
    state = _new_stack_after_the_detour(monkeypatch)
    job = _job(W2, skipped=(0, 0))
    card = _card(state, job)
    texts = " ".join(card.texts)
    assert _TODAY_LABEL in texts and RELOAD_CONDITION_HEADER not in texts
    assert _TODAY_TOOLTIP in card.tooltips
    ui = _dialog(monkeypatch, state, job)
    assert RELOAD_CONDITION_HEADER not in [h for _t, h in ui.notices]
    run = _start_click(monkeypatch, state, W2, job=job)
    assert _TODAY_LINE in _start_line(run)


# --- CDC only, and PostgreSQL ---------------------------------------------------------------------


def test_cdc_only_names_the_type_switch_for_the_re_run(monkeypatch) -> None:
    # The CDC only detour (the G26 scenario, after Delete CDC infrastructure and a redeploy): the
    # new stack starts from W2 -- FAILS WITHOUT THE FIX: "gapless from Full Load", no condition.
    state = _new_stack_after_the_detour(monkeypatch, mtype=CDC_ONLY)
    # ``started``: the detour's Start from W1 preceded this Full Load (the body says so first).
    body = reload_condition(CDC_ONLY, started=True)
    assert body == (
        "This migration started CDC from an earlier start point before this Full Load ran, and "
        "the Full Load found rows already on the target. Start CDC begins at its watermark, so if "
        "the Full Load appended to them, run it again choosing 'Drop & reload' before Start CDC "
        "(CDC only has no Full Load step: switch the migration type to Full load only for that "
        "run, then back to CDC only). An append keeps those rows as they were, and CDC from a "
        "watermark carries only the changes after it."
    )
    texts = " ".join(_card(state, _job(W2)).texts)
    assert _CONDITION_LABEL in texts and body in texts
    ui = _dialog(monkeypatch, state, _job(W2))
    assert ui.bodies[RELOAD_CONDITION_HEADER] == body


def _pg_new_stack(mtype=FL_CDC):
    """PostgreSQL after the same detour (Start CDC from W1 stamped the window; Stop; Delete CDC
    infrastructure, which drops the slot; a Full load + CDC Append re-run created its slot
    again at W2; Redeploy): a new stack, never started."""
    from dsql_migrator.core.models import MigrationMode, TableSelection

    state = DataMigrationState()
    state.migration_type = mtype
    state.set_selection(TableSelection(selected_tables=["app.orders"]))
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.set_cdc_ops_window_start(T1)  # what the Start from W1 stamped
    state.set_cdc_deploy_job_id("redeploy-job", kind="infra")
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.job_id = "fl-job"
    return state


def _pg_session():
    return SimpleNamespace(
        source_config=SimpleNamespace(source_type=POSTGRES, database="app"),
        source_password=None, target_config=None, aws_profile=None,
    )


def test_postgres_states_the_condition_for_the_slot_start(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Start point set — gapless from the replication slot", no condition.
    state = _pg_new_stack()
    card = _card(state, _job(PG_W2), session=_pg_session())
    texts = " ".join(card.texts)
    assert "Start point set — from the replication slot (WAL LSN 0/5A000060)" in texts
    assert "gapless from the replication slot" not in texts
    assert RELOAD_CONDITION_HEADER in texts and _FL_CDC_BODY in texts
    assert RELOAD_CONDITION_TOOLTIP in card.tooltips
    ui = _dialog(monkeypatch, state, _job(PG_W2), session=_pg_session())
    assert ui.bodies[RELOAD_CONDITION_HEADER] == _FL_CDC_BODY
    assert ui.buttons["Start CDC"].enabled is True
    line = _cdc_ui._cdc_start_detail(
        "dsql-cdc-e2e26pg", PG_W2, mode="auto", source_type=POSTGRES, recopied=True
    )
    assert RELOAD_CONDITION_LINE in line and _TODAY_LINE not in line
    # CDC only on PostgreSQL: the re-run that creates the slot again is a Full load + CDC one.
    assert reload_condition(CDC_ONLY, postgres=True, started=True) == (
        "This migration started CDC from an earlier start point before this Full Load ran, and "
        "the Full Load found rows already on the target. Start CDC begins at its watermark, so if "
        "the Full Load appended to them, switch the migration type to Full load + CDC and Re-run "
        "Full Load there choosing 'Drop & reload' before Start CDC (it creates the slot again at "
        "its own snapshot point). An append keeps those rows as they were, and CDC from a "
        "watermark carries only the changes after it."
    )


def test_postgres_keeps_todays_text_for_a_drop_and_reload_and_a_re_snapshot() -> None:
    # Guard: a Drop & reload re-run (no row already there) keeps the gapless slot start; a Full
    # Load with no slot re-snapshots, whose own disclosure stands (no condition there).
    texts = " ".join(_card(_pg_new_stack(), _job(PG_W2, skipped=(0,)),
                           session=_pg_session()).texts)
    assert "Start point set — gapless from the replication slot (WAL LSN 0/5A000060)" in texts
    assert RELOAD_CONDITION_HEADER not in texts
    slotless = PG_W2.model_copy(update={"slot_name": None, "publication_name": None})
    texts = " ".join(_card(_pg_new_stack(), _job(slotless), session=_pg_session()).texts)
    assert RELOAD_CONDITION_HEADER not in texts and "re-snapshot" in texts


# --- the signal ----------------------------------------------------------------------------------


def test_the_signal_needs_an_earlier_start_and_rows_already_there() -> None:
    # The window sign (these chunks record no attempt, so the G27 repair's first-attempt sign,
    # tests/test_g27_repair_cdc.py, does not show).
    state = DataMigrationState()
    job = _job(W2)
    assert drop_reload_condition_due(state, job) is False  # no Start in this migration
    state.set_cdc_ops_window_start(T2)
    assert drop_reload_condition_due(state, job) is False  # a Start from this very watermark
    state.set_cdc_ops_window_start(T2 + timedelta(minutes=1))
    assert drop_reload_condition_due(state, job) is False  # a Start after this Full Load
    state.set_cdc_ops_window_start(T1)
    assert drop_reload_condition_due(state, job) is True
    assert drop_reload_condition_due(state, _job(W2, skipped=(0, 0, 0))) is False
    assert drop_reload_condition_due(state, _job(W2, skipped=(0, 7, 0))) is True
    assert drop_reload_condition_due(state, None) is False
    assert drop_reload_condition_due(state, _job(None)) is False
    # A naive time (an older record) is read as UTC, never a TypeError.
    state.cdc_ops_window_start = T1.replace(tzinfo=None)
    assert drop_reload_condition_due(state, job) is True


def test_the_signal_survives_a_restart_and_ends_with_start_over() -> None:
    from tests.test_g25_pg_r4 import _restart, _snapshot

    state = DataMigrationState()
    state.set_cdc_ops_window_start(T1)
    restored = _restart(_snapshot(state))
    assert drop_reload_condition_due(restored, _job(W2)) is True
    # Start over builds this migration's state afresh: no earlier Start, today's text.
    restored.__init__()
    assert drop_reload_condition_due(restored, _job(W2)) is False


@pytest.mark.parametrize("mtype", [FL_CDC, CDC_ONLY])
def test_the_notice_stays_one_screen_of_text(mtype) -> None:
    # The body is a notice body, not a job error line, but keep it bounded like its G26 sibling.
    for postgres in (False, True):
        assert len(reload_condition(mtype, postgres=postgres)) < 600
    assert len(RELOAD_CONDITION_LINE) < 200


# =================================================================================================
# G27 task CDC-1 -- items 3, 4 and 5 of the 2026-10-14 Seoul live E2E. Fakes only: no AWS.
#
# [3] The CDC stack-name suffix saved only on blur: Playwright fill('e2e26') then a click on
#     Deploy opened the confirm dialog with "This creates the cdc-stack 'dsql-cdc-stack'". The
#     Deploy click now saves the form first (blur-save stays; a rejected suffix reverts with the
#     same notice, and then no dialog opens).
# [4] 'Redeploy CDC infrastructure' re-rendered the step with the Full Load sub-step open: the
#     operator had opened the CDC section in the browser, which is not the active sub-step.
# [5] The PostgreSQL 'Delete CDC infrastructure' dialog never said that the teardown drops the
#     stack's replication slot and publication on the source (both were gone after the delete).
# =================================================================================================

from dsql_migrator.core.cdc_pg_slot import (  # noqa: E402
    SLOT_MISSING_HEADER,
    pg_cdc_route,
    pg_publication_name,
    pg_slot_name,
)

# The notice a rejected suffix shows, on blur and (now) on the Deploy click alike.
_CHARSET_NOTICE = (
    "CDC stack name suffix may use only letters, digits and hyphens "
    "(e.g. 'orders' -> dsql-cdc-orders)."
)


def _deploy_ui():
    """``_GateUi`` that keeps each button's click handler and records the toasts."""
    from tests.test_ui_data_migration import _GateUi

    class _DeployUi(_GateUi):
        def __init__(self) -> None:
            super().__init__()
            self.notified: list = []

        def button(self, label="", *_a, on_click=None, **_k):
            element = super().button(label)
            element.on_click = on_click
            return element

        def notify(self, message="", *_a, **k):
            self.notified.append((str(message), k.get("type")))

        def name_field(self):
            return next(i for i in self.inputs if "CDC stack name" in i.label)

        def subnet_field(self):
            return next(i for i in self.inputs if "connector subnet IDs" in i.label)

    return _DeployUi()


def _ready_for_deploy():
    from tests.test_ui_data_migration import _cdc_state_ready_for_deploy

    state = _cdc_state_ready_for_deploy()
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0123456789abcdef0"})
    return state


def _deploy_form(state):
    """The real 'Deploy CDC infrastructure' form + button for ``state``."""
    from tests.test_ui_data_migration import _StubJobManager

    ui = _deploy_ui()
    _cdc_ui._render_cdc_infra_deploy_action(
        ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
    )
    return ui


def _click_deploy(monkeypatch, ui) -> list:
    """Click the form's Deploy. Returns, per confirm dialog built, the stack name and the infra
    inputs the dialog would read (it names the stack and diagnoses the subnets from them)."""
    import asyncio

    built: list = []

    async def _dialog(_ui, migration_state, _on_confirm, **_k):
        built.append((migration_state.cdc_stack_name, dict(migration_state.cdc_infra_inputs())))

    monkeypatch.setattr(_cdc_ui, "_open_cdc_infra_dialog", _dialog)
    asyncio.run(ui.deploy_button().on_click())
    return built


# --- [3] the stack-name suffix is applied before the Deploy confirm dialog is built ------------


def test_deploy_applies_the_typed_stack_name_before_the_dialog_is_built(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the dialog was built with 'dsql-cdc-stack' (the suffix typed with no
    # blur was never saved), and with the subnet override typed the same way left out.
    state = _ready_for_deploy()
    ui = _deploy_form(state)
    ui.name_field().value = "e2e26"  # fill('e2e26'), then the click -- no blur in between
    ui.subnet_field().value = "subnet-aaa,subnet-bbb"
    built = _click_deploy(monkeypatch, ui)
    assert [name for name, _inputs in built] == ["dsql-cdc-e2e26"]
    assert built[0][1]["connector_subnet_ids"] == "subnet-aaa,subnet-bbb"
    assert built[0][1]["vpc_id"] == "vpc-0123456789abcdef0"
    assert ui.notified == []


def test_a_rejected_suffix_at_deploy_reverts_with_the_same_notice_and_opens_no_dialog(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: the dialog opened naming 'dsql-cdc-stack', with no notice at all.
    state = _ready_for_deploy()
    ui = _deploy_form(state)
    ui.name_field().value = "e2e_26"  # '_' is outside the CloudFormation stack-name charset
    assert _click_deploy(monkeypatch, ui) == []
    assert ui.name_field().value == "stack" and state.cdc_stack_name == "dsql-cdc-stack"
    assert ui.notified == [(_CHARSET_NOTICE, "warning")]
    # Fixed, the next click opens the dialog with the fixed name.
    ui.name_field().value = "e2e26"
    assert [name for name, _inputs in _click_deploy(monkeypatch, ui)] == ["dsql-cdc-e2e26"]


def test_blur_still_saves_the_suffix_and_reverts_a_rejected_one() -> None:
    # Guard: the blur-save is kept as it was.
    state = _ready_for_deploy()
    ui = _deploy_form(state)
    ui.name_field().enter("orders", event="blur")
    assert state.cdc_stack_name == "dsql-cdc-orders" and ui.name_field().value == "orders"
    ui.name_field().enter("bad_name", event="blur")
    assert state.cdc_stack_name == "dsql-cdc-orders" and ui.name_field().value == "orders"
    assert ui.notified == [(_CHARSET_NOTICE, "warning")]


def test_one_copy_of_the_form_does_not_write_its_unedited_fields_over_the_other(
    monkeypatch,
) -> None:
    # Guard on the flush: Prerequisites and the CDC step can both show the form for one session.
    # The suffix and VPC saved in one copy stand when Deploy is clicked in the other, whose own
    # fields still show the values it was rendered with.
    state = _ready_for_deploy()
    prerequisites, cdc_step = _deploy_form(state), _deploy_form(state)
    prerequisites.name_field().enter("e2e26", event="blur")
    prerequisites.vpc_field().enter("vpc-0fedcba9876543210")
    built = _click_deploy(monkeypatch, cdc_step)
    assert [name for name, _inputs in built] == ["dsql-cdc-e2e26"]
    assert built[0][1]["vpc_id"] == "vpc-0fedcba9876543210"
    assert cdc_step.notified == []


# --- [4] 'Redeploy CDC infrastructure' leaves the operator on the CDC sub-step -----------------


@pytest.mark.parametrize("mtype", [FL_CDC, CDC_ONLY])
def test_redeploy_opens_the_cdc_substep_where_the_deploy_form_is(mtype) -> None:
    # FAILS WITHOUT THE FIX: no sub-step was chosen, so the refresh resolved Full Load (Full
    # load + CDC with its Full Load done) and the CDC section the operator had opened in the
    # browser -- which the server never hears about -- closed.
    from tests.test_ui_data_migration import _RecordingUi

    from dsql_migrator.ui.data_migration._models import resolve_active_substep_for_type

    state = DataMigrationState()
    state.migration_type = mtype
    state.set_cdc_deploy_job_id("delete-job", kind="delete")  # the teardown finished
    state.set_active_substep(None)  # the type switch back reset the choice (live scenario)
    ui = _RecordingUi()
    on_refresh: list = []
    _cdc_ui._render_cdc_redeploy_prompt(
        ui, state, lambda: on_refresh.append(state.active_substep)
    )
    redeploy = next(b for b in ui.buttons if b.text == "Redeploy CDC infrastructure")
    redeploy.on_click()
    assert on_refresh == ["cdc"]  # chosen BEFORE the refresh re-renders the step
    assert state.cdc_redeploy_confirmed is True
    assert resolve_active_substep_for_type(
        state.active_substep, migration_type=mtype, has_job=True, full_load_done=True,
    ) == "cdc"


# --- [5] the PostgreSQL Delete CDC infrastructure dialog states the slot and publication drop ---

E2E26PG = "dsql-cdc-e2e26pg"
_SLOT = pg_slot_name(E2E26PG)
_PUB = pg_publication_name(E2E26PG)
_PG_DROP = (
    "On PostgreSQL, once the stack is deleted, the teardown also drops this stack's replication "
    f"slot '{_SLOT}' and its publication '{_PUB}' on the source (each only if it exists), which "
    "releases the WAL the slot retains. If it cannot drop them, the teardown still completes and "
    "its log gives the statements to run on the source."
)
_RESUME_LOST = (
    " A slot cannot be created again at a past position, so after you deploy again, Start CDC "
    "cannot resume from the Full Load's slot. "
)
_PG_ROUTE_FL_CDC = (
    "Re-snapshot every table instead (the Start CDC dialog offers it when the Start that "
    "re-snapshots passes its own checks), or Re-run Full Load on the Full Load step choosing "
    "'Drop & reload' (it creates the slot again at its own snapshot point), then Start CDC."
)
_SNAPSHOTS_AGAIN = (
    " A slot cannot be created again at a past position, so after you deploy again, Start CDC on "
    "the new stack snapshots every selected table before it streams (a row deleted on the source "
    "in between stays on the target; Validation reports it as an EXTRA row)."
)
# The MySQL dialog, word for word as before G27 (stack dsql-cdc-e2e26, password auth).
_MYSQL_DELETE_DIALOG = [
    "Delete CDC infrastructure",
    "This deletes the entire cdc-stack — MSK, the VPC wiring, plugins, IAM role and any "
    "connectors. This cannot be undone.",
    "On MySQL this also deletes the recorded stream position and Debezium's schema history: "
    "after you deploy again, Start CDC reads the binary log again from the Full Load watermark "
    "(or your Manual start point). It refuses if a captured table's columns changed since then, "
    "but only warns if that start point has no recorded table definitions (one recorded by an "
    "earlier version of the tool or typed by hand).",
    "Stack name to confirm:",
    "dsql-cdc-e2e26",
    "The source-credentials secret the tool created for CDC "
    "('mysql-dsql-migrator/cdc/dsql-cdc-e2e26/source') will also be scheduled for deletion with "
    "a 7-day recovery window, so your database credentials do not linger in Secrets Manager.",
    "Type the stack name above to enable Delete",
    "",
    "Delete",
    "Cancel",
]


def _pg_load(slot=_SLOT, publication=_PUB):
    """The Full Load record (Full load + CDC): its watermark carries the slot and publication it
    created at its snapshot point."""
    wm = Watermark(
        snapshot_timestamp=T1, wal_lsn="0/5A000060", slot_name=slot, publication_name=publication,
    )
    return _job(wm, skipped=(0,))


def _pg_delete_state(mtype=FL_CDC, *, deployed=_SLOT, started=False):
    """A PostgreSQL stack the phase probe described (``deployed``: its PgSlotName)."""
    state = DataMigrationState()
    state.migration_type = mtype
    state.set_cdc_stack_name(E2E26PG)
    state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    state.set_cdc_deployed_slot_name(deployed)
    if started:
        state.set_cdc_has_committed_offset(True)  # started, then stopped
    state.job_id = "fl-job"
    return state


def _delete_dialog_texts(state, job, *, session=None) -> list:
    """Click the real 'Delete CDC infrastructure' action; the texts of the dialog it opens."""
    from tests.test_ui_data_migration import _RecordingUi

    ui = _RecordingUi()
    _cdc_ui._render_cdc_delete_action(
        ui, state, _JM(job), lambda: None, session=session or _pg_session()
    )
    action = next(b for b in ui.buttons if b.text == "Delete CDC infrastructure")
    before = len(ui.texts)
    action.on_click()
    return ui.texts[before:]


def test_the_pg_delete_dialog_says_the_teardown_drops_the_slot_and_publication() -> None:
    # FAILS WITHOUT THE FIX (the live dialog): nothing about the slot or the publication.
    texts = _delete_dialog_texts(_pg_delete_state(), _pg_load())
    assert _PG_DROP + _RESUME_LOST + _PG_ROUTE_FL_CDC in texts
    assert _PG_ROUTE_FL_CDC == pg_cdc_route(SLOT_MISSING_HEADER, started=False, cdc_only=False)
    assert "schema history" not in " ".join(texts)
    # The rest of the dialog is as it was.
    assert "Stack name to confirm:" in texts and E2E26PG in texts


def test_the_mysql_delete_dialog_is_unchanged() -> None:
    from tests.test_ui_data_migration import _RecordingUi

    state = DataMigrationState()
    state.set_cdc_stack_name("dsql-cdc-e2e26")
    session = SimpleNamespace(
        source_config=SimpleNamespace(source_type=MYSQL, database="app"), source_secret_id=None,
    )
    ui = _RecordingUi()
    _cdc_ui._open_cdc_delete_dialog(ui, state, lambda: None, session=session, job=_job(W1))
    assert ui.texts == _MYSQL_DELETE_DIALOG


def _teardown_drops(monkeypatch, stack_params) -> dict:
    """The slot and publication the REAL teardown drop (``_drop_pg_source_replication``) passes
    to the source write for ``stack_params``, with the source write faked."""
    from dsql_migrator.core import cdc_deployer, cdc_pg_slot

    dropped: dict = {}

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    class _Engine:
        def connect(self):
            return _Connection()

        def dispose(self):
            return None

    def _deprovision(_connection, *, slot_name, publication_name, on_log=None):
        dropped.update(slot=slot_name, publication=publication_name)

    monkeypatch.setattr(cdc_pg_slot, "build_pg_source_write_engine", lambda *_a, **_k: _Engine())
    monkeypatch.setattr(cdc_pg_slot, "deprovision_pg_replication", _deprovision)
    ok = cdc_deployer._drop_pg_source_replication(
        SimpleNamespace(log=lambda _message: None),
        stack_name=E2E26PG,
        stack_params=stack_params,
        region="ap-northeast-2",
        aws_profile=None,
        source_credentials=("app_user", "not-a-real-password"),
    )
    assert ok is True
    return dropped


_OLD_SLOT = pg_slot_name("dsql-cdc-older")
_OLD_PUB = pg_publication_name("dsql-cdc-older")


@pytest.mark.parametrize(
    "deployed_slot, deployed_publication, job",
    [
        # The stack's names are the ones derived from its name (a Start from its Full Load).
        (_SLOT, _PUB, _pg_load()),
        # An older stack without the parameters: the teardown derives both from the stack name.
        ("", "", None),
        # The stack started from a Full Load recorded under another CDC stack name (the deployed
        # names are that load's), so the dialog takes the publication from the load's record.
        (_OLD_SLOT, _OLD_PUB, _pg_load(_OLD_SLOT, _OLD_PUB)),
    ],
)
def test_the_dialog_names_the_slot_and_publication_the_teardown_drops(
    monkeypatch, deployed_slot, deployed_publication, job,
) -> None:
    dropped = _teardown_drops(monkeypatch, {
        "EngineType": "postgres",
        "SourceDbHostname": "pg.example.internal",
        "SourceDbPort": "5432",
        "PgDatabaseName": "app",
        "PgSlotName": deployed_slot,
        "PgPublicationName": deployed_publication,
    })
    # The stack-phase probe records the deployed PgSlotName (None when the stack has none).
    text = _cdc_ui._pg_delete_disclosure(_pg_delete_state(deployed=deployed_slot or None), job)
    assert (
        f"replication slot '{dropped['slot']}' and its publication '{dropped['publication']}' "
        "on the source"
    ) in text


def test_a_publication_name_the_session_does_not_know_is_not_guessed() -> None:
    # Deployed names that are neither derived nor this session's Full Load's: the slot is named
    # (the probe read it), the publication is not.
    text = _cdc_ui._pg_delete_disclosure(_pg_delete_state(deployed=_OLD_SLOT), _pg_load())
    assert f"replication slot '{_OLD_SLOT}' and its publication on the source" in text
    assert _OLD_PUB not in text and _PUB not in text
    # Nor does it claim the Full Load's slot is lost: the teardown leaves that one.
    assert "Full Load's slot" not in text


def test_what_a_later_start_needs_follows_the_slot_the_teardown_drops() -> None:
    disclose = _cdc_ui._pg_delete_disclosure
    # CDC only, the slot an earlier Full load + CDC run recorded: its route names the type switch.
    assert disclose(_pg_delete_state(CDC_ONLY), _pg_load()) == (
        _PG_DROP + _RESUME_LOST + pg_cdc_route(SLOT_MISSING_HEADER, started=False, cdc_only=True)
    )
    assert "switch the migration type to Full load + CDC" in disclose(
        _pg_delete_state(CDC_ONLY), _pg_load()
    )
    # A stack that streamed with no Full Load slot (CDC only stand-alone): the next Start, on the
    # new stack, snapshots on its own.
    assert disclose(_pg_delete_state(CDC_ONLY, started=True), None) == _PG_DROP + _SNAPSHOTS_AGAIN
    # Deployed before any Full Load and never started: no slot of a start point exists yet.
    assert disclose(_pg_delete_state(FL_CDC), None) == _PG_DROP
