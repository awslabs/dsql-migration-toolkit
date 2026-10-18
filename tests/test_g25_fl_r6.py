# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 6, package FL: the new-start-point gate (decision A, findings #0 / #6).

On a Full load + CDC migration whose CDC stack has streamed, or rolled back a Start, a Full
Load that takes a NEW start point -- Start Full Load, Re-run Full Load, Re-run all tables: a new
watermark -- is refused until the CDC infrastructure is deleted and deployed again. Stop CDC
removes the sink without a drain, so its consumer group would re-apply changes older than the
new snapshot over the reloaded tables, while the source skips the changes between the old stop
point and the new snapshot. Before this round the only gate was "CDC is live": after Stop CDC
the re-run went ahead and the next Start CDC replayed the sink's tail over it, silently.

* The step disables Start / Re-run Full Load and Re-run all tables with the notice (both
  engines; the PostgreSQL route is the route rule's own words), while CDC is live it keeps
  "Stop CDC first" and names the rest of the route, and a stack not read yet holds the action
  until it is.
* Retry unfinished tables, Reload specific tables (and a G17 addition) keep the original start
  point: unchanged.
* A stale button, the page header's Run (through the Start dialog) and a dialog opened before
  the stack streamed are refused at the click and at the confirm.
* The engine refuses a run whose ``start_point_refusal`` says so before anything is written
  or read, its job error's first line under 300 characters.
* Validation's runbook names Delete CDC infrastructure and deploy it again before the Full Load
  re-run on a stack that has streamed, and no copy promises a resume "from the new snapshot".

Each stack state comes from the REAL stack-phase probe over one describe of the cdc-stack, as
after every lifecycle job and app restart. Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType, StepStatus, TableSelection
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.data_migration._full_load_engine import (
    NEW_START_POINT_HEADER,
    NEW_START_POINT_UNREAD_HEADER,
    FullLoadRefusedError,
    new_start_point_block,
    new_start_point_refusal,
)

from tests.test_g12_table_set_after_load import _c, _job, _Jobs, _loaded
from tests.test_g17_full_load_and_cdc_additions import _G17PgSession, _G17Session
from tests.test_start_dialog_overlay import _Btn, _DialogUi
from tests.test_ui_data_migration import _ConfirmDialogUi

MY, PG = SourceType.MYSQL, SourceType.POSTGRES
FLC = MigrationType.FULL_LOAD_AND_CDC

# What the cdc-stack's CloudFormation parameters (and status) say, as the probe reads them.
NEVER_STARTED = ({"DeploySink": "false", "MskBootstrapServers": ""}, "CREATE_COMPLETE")
STOPPED = ({"DeploySink": "true", "MskBootstrapServers": ""}, "UPDATE_COMPLETE")
# A Start on an infra stack that CloudFormation rolled back: its parameters are restored.
ROLLED_BACK = ({"DeploySink": "false", "MskBootstrapServers": ""}, "UPDATE_ROLLBACK_COMPLETE")
RUNNING = (
    {"DeploySink": "true", "MskBootstrapServers": "b-1.msk.example:9098"}, "UPDATE_COMPLETE"
)
STREAMED = {"stopped": STOPPED, "rolled back": ROLLED_BACK, "running": RUNNING}
ALL_STACKS = {"never started": NEVER_STARTED, **STREAMED}

_ROUTE_WORDS = ("Delete CDC infrastructure and deploy it again", "'Drop & reload'", "Start CDC")
_WHY = (
    "Stop CDC removes the sink without a drain (a rolled-back Start removes it mid-stream), so "
    "its consumer group would re-apply changes older than the new snapshot over the reloaded "
    "tables, while the source skips the changes between the old stop point and the new "
    "snapshot."
)
# The resume claim the decision retires: a resume after a new-start-point Full Load is gapless.
_RETIRED = ("resumes from the new snapshot", "resumes gaplessly from the new snapshot",
            "to run it.")


def _probe(monkeypatch, state, stack) -> None:
    """The REAL stack-phase probe over one describe of a stable cdc-stack in ``stack``."""
    import dsql_migrator.core.cdc_deployer as dep
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    parameters, status = stack

    class _Deployer:
        def describe_stack_or_none(self, _name):
            return SimpleNamespace(
                stack_status=status, is_stable=True, current_parameters=dict(parameters),
            )

        def list_cdc_stacks(self):
            return []

    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _Deployer())
    _probe_cdc_stack_phase(
        state,
        SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None),
    )


def _state(monkeypatch=None, stack=None, *, mtype=FLC, job_id="job-1"):
    state = DataMigrationState()
    state.migration_type = mtype
    state.job_id = job_id
    state.selection = TableSelection(selected_tables=["app.a", "app.b"])
    state.selection_touched = True
    if stack is not None:
        _probe(monkeypatch, state, stack)
    return state


def _session(engine):
    return _G17PgSession() if engine is PG else _G17Session()


def _first_line(text):
    return str(text).splitlines()[0]


# ---------------------------------------------------------------------------
# 1. The gate itself (pure), over the real probe
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", list(STREAMED), ids=list(STREAMED))
def test_a_stack_that_has_streamed_holds_a_new_start_point(monkeypatch, engine, stack) -> None:
    # FAILS WITHOUT THE FIX (#0, #6): nothing judged a Full Load against the stack's record.
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route

    block = new_start_point_block(_state(monkeypatch, STREAMED[stack]), engine)
    assert block is not None
    header, body, line = block
    assert header == NEW_START_POINT_HEADER
    assert _WHY in body
    for word in _ROUTE_WORDS:
        assert word in body and word in line, word
    # The route comes first, the Full Load after it, Start CDC last.
    assert body.index("Delete CDC infrastructure") < body.index("Re-run Full Load") < body.rindex(
        "Start CDC")
    if engine is PG:  # the route rule's own words (decision B)
        route = pg_cdc_route(None, started=True, cdc_only=False)
        assert f"{route[:1].upper()}{route[1:]}." in body
    else:
        assert "slot" not in body
    assert "Retry unfinished tables and Reload specific tables keep the original start" in body
    refusal = new_start_point_refusal(_state(monkeypatch, STREAMED[stack]), engine)
    assert _first_line(refusal) == line and len(line) <= 300


@pytest.mark.parametrize("engine", [MY, PG])
def test_a_never_started_stack_and_no_stack_are_not_held(monkeypatch, engine) -> None:
    # Guard: the first Start of a stack, and a migration with no stack yet, take the Full Load
    # as before.
    assert new_start_point_block(_state(monkeypatch, NEVER_STARTED), engine) is None
    absent = _state()
    absent.set_cdc_stack_phase("absent")
    assert new_start_point_block(absent, engine) is None
    assert new_start_point_refusal(absent, engine) is None


@pytest.mark.parametrize("engine", [MY, PG])
def test_an_unread_stack_holds_until_it_is_read(monkeypatch, engine) -> None:
    # An unread stack may have streamed: the gate waits for the read (the G17 addition guard's
    # rule), with the probe's failure named when there is one.
    unread = _state()
    header, body, line = new_start_point_block(unread, engine)
    assert header == NEW_START_POINT_UNREAD_HEADER and body == line
    assert "not been read yet" in line and "Re-check CDC state" in line
    failed = _state()
    failed.set_cdc_probe_error("AccessDenied")
    _h, _b, failed_line = new_start_point_block(failed, engine)
    assert "could not be read" in failed_line and "fix the AWS access" in failed_line
    for state in (unread, failed):
        refusal = new_start_point_refusal(state, engine)
        assert refusal == _first_line(refusal) and len(refusal) <= 300
    # ...and opens once the probe reads a stack that never streamed.
    _probe(monkeypatch, unread, NEVER_STARTED)
    assert new_start_point_block(unread, engine) is None


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_ONLY, MigrationType.CDC_ONLY])
@pytest.mark.parametrize("stack", list(ALL_STACKS), ids=list(ALL_STACKS))
def test_only_full_load_and_cdc_is_gated(monkeypatch, mtype, stack) -> None:
    # Guard: Full load only has no CDC to hand off to; CDC only has no Full Load step.
    state = _state(monkeypatch, ALL_STACKS[stack], mtype=mtype)
    assert new_start_point_block(state, MY) is None
    assert new_start_point_block(_state(mtype=mtype), PG) is None  # unread too


def test_a_restart_before_the_probe_with_recorded_connectors_names_the_route(monkeypatch):
    # The connector names survive a restore (the probe has not run yet): they are a Start the
    # stack records, so the route is named rather than the wait.
    state = _state()
    state.set_cdc_connector_names(["dsql-cdc-stack-debezium-source"])
    assert new_start_point_block(state, MY)[0] == NEW_START_POINT_HEADER


# ---------------------------------------------------------------------------
# 2. The Full Load step: each action on each stack, both engines
# ---------------------------------------------------------------------------


class _TipBtn(_Btn):
    def __init__(self, ui, text):
        super().__init__(ui, text)
        self.tips: list[str] = []

    def tooltip(self, text="", *_a, **_k):
        self.tips.append(str(text))
        return self


class _Ui(_DialogUi):
    """The overlay test's dialog double, recording each button's tooltips, the toasts, and an
    async timer callback (the page header Run's pending Start dialog) to run after render."""

    def __init__(self):
        super().__init__()
        self.notified: list[str] = []
        self.pending: list = []

    def button(self, text="", *_a, on_click=None, **_k):
        _ConfirmDialogUi.button(self, text, on_click=on_click)
        btn = _TipBtn(self, str(text))
        self.btns.append(btn)
        return btn

    def notify(self, message="", *_a, **_k):
        self.notified.append(str(message))

    def timer(self, _interval, callback=None, *_a, **_k):
        if callable(callback):
            result = callback()
            if asyncio.iscoroutine(result):
                self.pending.append(result)
        return _ConfirmDialogUi._El(self)


def _render(monkeypatch, *, state, job, engine, status=None, cdc_live=None, rows=()):
    """Render the real Full Load step (target probe patched); returns the UI double plus what
    the actions recorded."""
    import nicegui.context as ctxobj
    import nicegui.run as nrun

    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl
    from tests.test_ui_data_migration import _StubConnector

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()),
                        raising=False)

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    rec = SimpleNamespace(started=0, retried=[], refreshed=0, probed=0)

    def _rows(names, **_k):
        rec.probed += 1
        return [n for n in names if n in rows]

    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(fl, "DsqlConnector", lambda *a, **k: _StubConnector())
    monkeypatch.setattr(fl, "tables_with_rows", _rows)
    monkeypatch.setattr(fl, "target_primary_keys", lambda names, **k: {n: ["id"] for n in names})
    monkeypatch.setattr(fl, "tables_present", lambda names, **k: list(names))
    if cdc_live is not None:
        monkeypatch.setattr(fl, "cdc_streaming_started", lambda *a, **k: cdc_live)
    if status is None:
        status = StepStatus.NOT_STARTED if job is None else StepStatus.DONE
    ui = _Ui()

    def _start():
        rec.started += 1

    def _refresh(*_a):
        rec.refreshed += 1

    dm._render_full_load_step(
        ui, state, _Jobs({"job-1": job} if job is not None else {}), _session(engine), job=job,
        status=status, selected_names=["app.a", "app.b"],
        retry_tables=lambda names: rec.retried.append(list(names)), guard_reason=None,
        start_full_load=_start, retry_failed_load=lambda *a: None,
        reload_table=lambda *a: None, accept_quarantine_and_continue=lambda *a: None,
        stop_full_load=lambda *a: None, refresh=_refresh,
        schema_recreate_candidates=lambda: [],
    )
    return ui, rec


def _btn(ui, prefix):
    (btn,) = [b for b in ui.btns if b.text.startswith(prefix)]
    return btn


def _click(ui, label):
    handlers = [h for (text, h) in ui.handlers if text.startswith(label)]
    result = handlers[-1]()
    if asyncio.iscoroutine(result):
        asyncio.run(result)


def _failed_b():
    """A Full load + CDC record whose app.b failed (Re-run all tables + Retry unfinished)."""
    return _job("FAILED", [_c("app.a"), _c("app.b", status="FAILED")],
                counts={"app.a": 1, "app.b": 1})


# The new-start-point actions: (button label prefix, job record, step status).
_NEW_START = {
    "Start Full Load": (lambda: None, StepStatus.NOT_STARTED),
    "Re-run Full Load": (_loaded, StepStatus.DONE),
    "Re-run all tables": (_failed_b, StepStatus.FAILED),
}


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", ["stopped", "rolled back"])
@pytest.mark.parametrize("action", list(_NEW_START))
def test_a_new_start_point_on_a_stopped_or_rolled_back_stack_is_disabled_with_the_notice(
    monkeypatch, engine, stack, action
) -> None:
    # FAILS WITHOUT THE FIX (#0, #6): the button was enabled and its only CDC gate was "CDC is
    # live" -- after Stop CDC (or a rolled-back Start) the re-run went ahead.
    make_job, status = _NEW_START[action]
    job = make_job()
    state = _state(monkeypatch, STREAMED[stack], job_id="job-1" if job else None)
    ui, rec = _render(monkeypatch, state=state, job=job, engine=engine, status=status)
    btn = _btn(ui, action)
    _h, body, line = new_start_point_block(state, engine)
    assert btn.enabled is False and btn.tips == [line]
    assert NEW_START_POINT_HEADER in ui.texts and body in ui.texts
    texts = " ".join(ui.texts)
    assert not [w for w in _RETIRED if w in texts]
    # The click (as a stale render would offer it) is refused before any probe or dialog.
    _click(ui, action)
    assert ui.opened == 0 and rec.probed == 0 and rec.started == 0
    assert ui.notified == [line] and rec.refreshed == 1


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("action", list(_NEW_START))
def test_a_new_start_point_on_a_never_started_stack_still_runs(monkeypatch, engine, action):
    # Guard: a stack whose connectors never started takes a new start point as before.
    make_job, status = _NEW_START[action]
    job = make_job()
    state = _state(monkeypatch, NEVER_STARTED, job_id="job-1" if job else None)
    ui, rec = _render(monkeypatch, state=state, job=job, engine=engine, status=status)
    assert _btn(ui, action).enabled is True
    assert NEW_START_POINT_HEADER not in ui.texts
    _click(ui, action)
    assert ui.opened == 1 and rec.probed == 1
    assert ui.start().enabled
    ui.handlers[-1][1]()  # Confirm
    assert rec.started == 1 and ui.notified == []


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("action", ["Start Full Load", "Re-run Full Load"])
def test_while_cdc_runs_the_step_keeps_stop_cdc_first_and_names_the_route(
    monkeypatch, engine, action
) -> None:
    # FAILS WITHOUT THE FIX: "Stop CDC first (CDC step → Stop CDC) to run it" -- after which the
    # re-run replayed the sink's tail. The live gate is unchanged; its copy names the route.
    make_job, status = _NEW_START[action]
    job = make_job()
    state = _state(monkeypatch, RUNNING, job_id="job-1" if job else None)
    ui, rec = _render(monkeypatch, state=state, job=job, engine=engine, status=status)
    btn = _btn(ui, action)
    (tip,) = btn.tips
    assert btn.enabled is False
    route = (
        "Stop CDC first (CDC step → Stop CDC), then Delete CDC infrastructure and deploy it "
        "again before a new Full Load"
    )
    for text in (tip, " ".join(ui.texts)):
        assert route in text
        assert not [w for w in _RETIRED if w in text]
    _click(ui, action)
    assert ui.opened == 0 and rec.started == 0


@pytest.mark.parametrize("engine", [MY, PG])
def test_re_run_all_tables_on_a_running_stack_is_held_with_stop_cdc_first(monkeypatch, engine):
    # Re-run all tables had no live gate (its dialog warned and offered "Re-run anyway"); a
    # running stack has streamed, so the gate holds it now, Stop CDC first in its tooltip.
    state = _state(monkeypatch, RUNNING)
    ui, rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                      status=StepStatus.FAILED)
    btn = _btn(ui, "Re-run all tables")
    assert btn.enabled is False and "Stop CDC first" in btn.tips[0]
    assert NEW_START_POINT_HEADER in ui.texts
    _click(ui, "Re-run all tables")
    assert ui.opened == 0 and rec.started == 0


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", ["never started", "stopped", "rolled back"])
def test_retry_unfinished_keeps_the_original_start_point_on_every_stack(
    monkeypatch, engine, stack
) -> None:
    # Guard: Retry unfinished tables runs under the original watermark and converges.
    # DELIBERATE G129 UPDATE: a running stack is no longer one of them -- while CDC streams a
    # retry would write back the rows the sink deletes during the copy, so it is disabled
    # (tests/test_g129_coexisting_reload.py); after Stop CDC it runs, as here.
    state = _state(monkeypatch, ALL_STACKS[stack])
    ui, rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                      status=StepStatus.FAILED)
    retry = _btn(ui, "Retry unfinished tables")
    assert retry.enabled is True
    _click(ui, "Retry unfinished tables")
    assert ui.opened == 1 and ui.notified == []
    ui.handlers[-1][1]()  # Confirm (the checklist is pre-ticked)
    assert rec.retried == [["app.b"]] and rec.started == 0


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", ["never started", "stopped", "rolled back"])
def test_reload_specific_tables_keeps_the_original_start_point(monkeypatch, engine, stack):
    # Guard: Reload specific tables runs under the original watermark and converges.
    state = _state(monkeypatch, ALL_STACKS[stack])
    ui, rec = _render(monkeypatch, state=state, job=_loaded(), engine=engine,
                      status=StepStatus.DONE)
    reload_btn = _btn(ui, "Reload specific tables")
    assert reload_btn.enabled is True
    _click(ui, "Reload specific tables")
    assert ui.opened == 1 and ui.notified == []
    from tests.test_g25_fl import _tick

    _tick(ui, "app.a", True)
    ui.handlers[-1][1]()
    assert rec.retried == [["app.a"]] and rec.started == 0


@pytest.mark.parametrize("engine", [MY, PG])
def test_reload_specific_tables_while_cdc_runs_keeps_its_stop_cdc_first(monkeypatch, engine):
    # Guard: the live gate on Reload is unchanged (after Stop CDC it runs, as above).
    ui, _rec = _render(monkeypatch, state=_state(monkeypatch, RUNNING), job=_loaded(),
                       engine=engine, status=StepStatus.DONE)
    reload_btn = _btn(ui, "Reload specific tables")
    assert reload_btn.enabled is False and "Stop CDC first" in reload_btn.tips[0]


@pytest.mark.parametrize("engine", [MY, PG])
def test_an_unread_stack_holds_start_with_the_wait_notice(monkeypatch, engine) -> None:
    ui, rec = _render(monkeypatch, state=_state(job_id=None), job=None, engine=engine)
    btn = _btn(ui, "Start Full Load")
    assert btn.enabled is False
    assert NEW_START_POINT_UNREAD_HEADER in ui.texts
    assert "Re-check CDC state" in btn.tips[0]


@pytest.mark.parametrize("engine", [MY, PG])
def test_a_dialog_opened_before_the_stack_streamed_is_refused_at_its_confirm(monkeypatch, engine):
    # The CDC state can change while the dialog is open (the probe runs off the loop): the
    # confirm re-checks it, and closes as a Cancel.
    state = _state(monkeypatch, NEVER_STARTED)
    ui, rec = _render(monkeypatch, state=state, job=_loaded(), engine=engine)
    _click(ui, "Re-run Full Load")
    assert ui.opened == 1
    _probe(monkeypatch, state, STOPPED)  # Stop CDC landed meanwhile (another tab)
    ui.handlers[-1][1]()  # Confirm
    assert rec.started == 0
    assert ui.notified == [new_start_point_block(state, engine)[2]]


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_page_header_run_through_the_start_dialog_is_refused(monkeypatch, engine) -> None:
    # The header Run opens the Start dialog when exclusions are in play: refused there too.
    state = _state(monkeypatch, STOPPED)
    state.pending_full_load_confirm = True
    ui, rec = _render(monkeypatch, state=state, job=_loaded(), engine=engine)
    (pending,) = ui.pending
    asyncio.run(pending)
    assert ui.opened == 0 and rec.probed == 0 and rec.started == 0
    assert ui.notified == [new_start_point_block(state, engine)[2]]


# ---------------------------------------------------------------------------
# 3. The confirm dialog's "CDC is currently streaming" notice
# ---------------------------------------------------------------------------


def _live_notice(ui) -> str:
    texts = ui.texts
    i = texts.index("CDC is currently streaming")
    return texts[i + 1]


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_live_notice_of_a_new_start_point_names_new_topics(monkeypatch, engine) -> None:
    # FAILS WITHOUT THE FIX: "...then start CDC again so it resumes from the new snapshot".
    # Reachable while a Start is in flight on a stack not yet read as streamed.
    state = _state(monkeypatch, NEVER_STARTED)
    ui, _rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED, cdc_live=True)
    _click(ui, "Re-run all tables")
    body = _live_notice(ui)
    assert "Stop CDC first (CDC step → Stop CDC)" in body
    assert body.index("Stop CDC first") < body.index("Delete CDC infrastructure and deploy it "
                                                     "again") < body.index("Start CDC.")
    assert not [w for w in _RETIRED if w in body]
    assert "ORIGINAL watermark" not in body


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_live_notice_of_a_retry_says_cdc_continues_where_it_stopped(monkeypatch, engine):
    state = _state(monkeypatch, NEVER_STARTED)
    ui, _rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED, cdc_live=True)
    _click(ui, "Retry unfinished tables")
    body = _live_notice(ui)
    assert "keeps the original start point, so CDC continues from where it stopped" in body
    assert "Delete CDC infrastructure" not in body
    assert not [w for w in _RETIRED if w in body]


# ---------------------------------------------------------------------------
# 4. The engine backstop
# ---------------------------------------------------------------------------


def test_the_engine_refuses_before_anything_is_written_or_read(monkeypatch) -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import _full_load_engine as engine_mod
    from dsql_migrator.ui.data_migration._full_load_engine import run_full_load
    from tests.test_ui_data_migration import _FakeMigrator, _tables

    state = _state(monkeypatch, STOPPED)
    events = []
    monkeypatch.setattr(engine_mod, "log_activity", lambda *a, **k: events.append((a, k)))
    migrator = _FakeMigrator(rows_by_table={"orders": 1, "customers": 1})
    manager = JobManager()
    job_id = manager.submit(lambda h: run_full_load(
        h, _tables(), migrator=migrator, error_log=ErrorLogStore(),
        start_point_refusal=lambda: new_start_point_refusal(state, MY),
    ))
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)
    assert job.status == "FAILED"
    # No watermark captured (on PostgreSQL that replaces the slot), no table read, and no
    # chunk seeded -- a refused record with PENDING tables would offer a Retry with no start
    # point at all.
    assert migrator.captured == 0 and migrator.migrated == [] and job.chunks == []
    line = new_start_point_block(state, MY)[2]
    assert manager.get_error(job_id) == f"{FullLoadRefusedError.__name__}: {line}"
    assert len(line) <= 300
    ((args, kwargs),) = events
    assert args[1] == "run refused" and line in kwargs["detail"]


def test_the_engine_runs_when_the_gate_is_open_or_absent(monkeypatch) -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration._full_load_engine import run_full_load
    from tests.test_ui_data_migration import _FakeMigrator, _tables

    state = _state(monkeypatch, NEVER_STARTED)
    for gate in (lambda: new_start_point_refusal(state, PG), None):
        migrator = _FakeMigrator(rows_by_table={"orders": 1, "customers": 1})
        manager = JobManager()
        job_id = manager.submit(lambda h, gate=gate, m=migrator: run_full_load(
            h, _tables(), migrator=m, error_log=ErrorLogStore(), start_point_refusal=gate,
        ))
        assert manager.wait(job_id, timeout=5.0)
        assert manager.get_status(job_id).status == "DONE"
        assert migrator.captured == 1


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", list(STREAMED) + ["unread"])
def test_every_refusal_fits_a_job_errors_first_line(monkeypatch, engine, stack) -> None:
    state = _state() if stack == "unread" else _state(monkeypatch, STREAMED[stack])
    refusal = new_start_point_refusal(state, engine)
    assert refusal and len(_first_line(refusal)) <= 300
    if stack == "unread":
        assert "Re-check CDC state" in refusal
    else:
        assert all(w in _first_line(refusal) for w in _ROUTE_WORDS)


# ---------------------------------------------------------------------------
# 5. Validation's recovery runbook
# ---------------------------------------------------------------------------


def _runbook(*, streamed):
    from dsql_migrator.ui.validation import _render_recovery_section
    from tests.test_ui_validation import _CopyUi, _drift_na, _release_summary

    ui = _CopyUi()
    _render_recovery_section(
        ui, _release_summary(is_match=False, mismatched=1), _drift_na(),
        cdc_stack_streamed=streamed,
    )
    return ui.body()


def test_the_runbook_on_a_streamed_stack_deletes_and_deploys_before_the_re_run() -> None:
    # FAILS WITHOUT THE FIX (#6): "Stop CDC ... re-run the Full Load ... start CDC again -- it
    # resumes gaplessly from the new snapshot", the replay over the reload.
    body = _runbook(streamed=True)
    assert "Re-run Full Load + CDC to backfill the gap" in body and "Steps to recover" in body
    order = [body.index(w) for w in (
        "click Stop CDC first", "click Delete CDC infrastructure, and deploy it again",
        "click Re-run Full Load", "'Drop & reload'", "Start CDC: it starts from the new Full "
        "Load's start point", "click Re-run validation")]
    assert order == sorted(order)
    assert "consumer group would re-apply changes older than the new snapshot" in body
    assert not [w for w in _RETIRED if w in body]


def test_the_runbook_on_a_stack_that_never_streamed_keeps_its_order_without_a_resume_claim():
    body = _runbook(streamed=False)
    assert "Re-run Full Load + CDC to backfill the gap" in body and "Steps to recover" in body
    assert "Delete CDC infrastructure" not in body
    # A stack that never streamed cannot be running: no Stop CDC step (G25 round 7).
    assert "If CDC is running" not in body and "Stop CDC" not in body
    assert not [w for w in _RETIRED if w in body]
    assert "would resume from the old snapshot point" not in body


@pytest.mark.parametrize("stack", list(ALL_STACKS), ids=list(ALL_STACKS))
def test_validation_reads_the_stack_record_for_the_runbook(monkeypatch, stack) -> None:
    from dsql_migrator.ui.validation import _cdc_stack_streamed

    from dsql_migrator.ui.validation import _cdc_state_unread

    # Whatever the type (G25 round 7): a CDC only migration's stack streams too.
    streamed = stack != "never started"
    for mtype in (FLC, MigrationType.FULL_LOAD_ONLY, MigrationType.CDC_ONLY):
        state = _state(monkeypatch, ALL_STACKS[stack], mtype=mtype)
        assert _cdc_stack_streamed(state) is streamed
        assert _cdc_state_unread(state) is False
    # Not read yet: unknown, which the runbook reads as streamed on a CDC type.
    assert _cdc_stack_streamed(_state()) is False and _cdc_state_unread(_state()) is True
    assert _cdc_stack_streamed(object()) is False and _cdc_state_unread(object()) is True


def test_the_report_threads_the_stack_record_to_the_runbook() -> None:
    from dsql_migrator.ui.validation import _render_result
    from tests.test_ui_validation import _failing_report_for_render, _ScreenUi

    for streamed in (True, False):
        ui = _ScreenUi()
        _render_result(ui, _failing_report_for_render(), cdc_stack_streamed=streamed)
        blob = " ".join(ui.texts)
        assert "Steps to recover" in blob
        assert ("click Delete CDC infrastructure, and deploy it again" in blob) is streamed
