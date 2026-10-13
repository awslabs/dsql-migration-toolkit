# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 4 (the closing round), package PG.

* Decision (2), closing f[1], f[3], f[7] and f[8]: the PostgreSQL slot-position Start check
  (Defect B(b)) is STATELESS. It judges only a stack's FIRST Start: one whose CloudFormation
  parameters, as the stack-phase probe reads them, record no Start of its connectors.
  ``DeploySink="true"`` with a blank ``MskBootstrapServers`` is started-then-stopped
  (``cdc_has_committed_offset``); a set ``MskBootstrapServers`` has connectors in place
  (Retry CDC). The session-persisted "went ahead" window is gone. So a Start that
  ``run_cdc_start`` refused or failed before any connector existed never exempts a later one
  (f[1]). A restart before the next save cannot turn a real Stop -> Start into a false block
  (f[7]). Start over leaves nothing behind, so a later MySQL journey saves its own CDC window
  (f[3], f[8]).
* f[2]: the pre-flight does not block when it cannot connect to the source, and it is never
  silent about it. The Start dialog shows a warning, and the dialog and the worker each write
  an activity line saying the checks did not run, and why.
* f[0]: Full load only lists the inheritance WARN too (one catalog read).
* f[4]: the oversized-value prerequisite probe reads an inheritance parent FROM ONLY.

Each finding's test FAILS on the code before this round. The never-started, re-run, MySQL and
readable-source cases are guards against over-correcting. Fakes only: no real database (the
two catalog reads, the size probe and the slot-position gate were also run live on PostgreSQL
16.10, see the round notes).
"""

from __future__ import annotations

import json
from datetime import timedelta
from types import SimpleNamespace

import pytest

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.models import (
    ColumnDef,
    MigrationMode,
    PrerequisiteCheckRequest,
    PrerequisiteStatus,
    SourceType,
    TableDef,
    TableSelection,
    Watermark,
)
from tests.test_g25_pg import AT_START, CAPTURED, MOVED, SLOT_REFUSAL, _slot_at, _watermark

# A Full Load re-run after the stack streamed: a new start point, captured later.
RERUN_AT = "0/2A00000"
RERUN = CAPTURED + timedelta(hours=1)

# What the cdc-stack's CloudFormation parameters say, as the stack-phase probe reads them.
NEVER_STARTED = {"DeploySink": "false", "MskBootstrapServers": ""}
STOPPED = {"DeploySink": "true", "MskBootstrapServers": ""}
RUNNING = {"DeploySink": "true", "MskBootstrapServers": "b-1.msk.example:9098"}

# A driver error that carries what an exception message may hold (a host, an address, a user
# name): none of it may reach a notice or an activity line (Property 7).
_LEAKY = (
    'connection to server at "db.internal.example" (10.0.0.5), port 5432 failed: FATAL: '
    'password authentication failed for user "migrator_admin"'
)
_SECRET = "arn:aws:secretsmanager:us-east-1:111122223333:secret:src-AbCdEf"


def _fresh(mtype=None):
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["app.orders"]))
    if mtype is not None:
        state.migration_type = mtype
    return state


def _phase_probe(monkeypatch, state, parameters) -> None:
    """The REAL stack-phase probe over one describe of a stable cdc-stack with
    ``parameters`` -- what runs after every lifecycle job and after an app restart."""
    import dsql_migrator.core.cdc_deployer as dep
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    class _Deployer:
        def describe_stack_or_none(self, _name):
            return SimpleNamespace(
                stack_status="UPDATE_COMPLETE", is_stable=True,
                current_parameters=dict(parameters),
            )

        def list_cdc_stacks(self):
            return []

    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _Deployer())
    _probe_cdc_stack_phase(
        state,
        SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None),
    )


def _start(monkeypatch, *, state, confirmed=None, watermark=None, **kw):
    """The real Start CDC (its job run after ``_start_cdc_deploy`` returned, the production
    order) for the Full Load ``watermark``; ``confirmed`` puts the slot there, else the
    caller patched the source."""
    from tests.test_ui_data_migration import _g5_inventory, _run_start_cdc_capture

    if confirmed is not None:
        _slot_at(monkeypatch, confirmed)
    return _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.POSTGRES,
        job_watermark=watermark if watermark is not None else _watermark(),
        source_password="pw", selected=["app.orders"], inventory=_g5_inventory(),
        catch=True, msk_access=True, state=state, **kw,
    )


def _rerun_watermark():
    return Watermark(slot_name="slot", publication_name="pub", wal_lsn=RERUN_AT,
                     snapshot_timestamp=RERUN, table_row_counts={"app.orders": 1})


def _snapshot(state):
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import capture_session_snapshot

    return capture_session_snapshot(
        "s1", SessionConnectionState(), EvaluationState(), SchemaConversionState(), state)


def _restart(snapshot):
    """A real restart: the saved snapshot through JSON, as every store writes it, applied to a
    fresh state. The stack-phase probe has not run yet (the caller runs it)."""
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import apply_session_snapshot

    fresh = DataMigrationState()
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(snapshot.model_dump_json()),
        SessionConnectionState(), EvaluationState(), SchemaConversionState(), fresh)
    return fresh


def _pg_session(*, password="pw", secret_id=None):
    return SimpleNamespace(
        source_config=SimpleNamespace(source_type=SourceType.POSTGRES, database="app"),
        source_password=password, source_secret_id=secret_id,
    )


def _dialog(monkeypatch, state, *, session=None, watermark=None, conn_blocker=None):
    """The REAL Start CDC dialog, its publication/slot gate reading whatever source the caller
    patched in (``conn_blocker``: the reconnect notice, if any). Returns ``(ui, notices,
    events)``: ``notices`` as ``(tone, header, body)``, ``events`` every activity line as
    ``(action, kwargs)``."""
    import asyncio

    import nicegui

    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _DialogUi, _g5_inventory

    ui = _DialogUi()
    notices: list = []
    events: list = []

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        notices.append((tone, header, body))

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    wm = watermark if watermark is not None else _watermark()
    fl_job = SimpleNamespace(job_id="fl-job", watermark=wm, status="DONE")
    if getattr(state, "migration_type", None) is not _cdc_only():
        state.job_id = "fl-job"
    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)
    monkeypatch.setattr(
        _cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: conn_blocker)
    monkeypatch.setattr(_cdc_ui, "_probe_binlog_resume_gap", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_log_cdc_event", lambda *a, **k: events.append((a[0], k)))
    monkeypatch.setattr(
        _cdc_ui, "_current_job", lambda _jm, job_id: fl_job if job_id == "fl-job" else None)
    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    asyncio.run(_cdc_ui._open_cdc_start_dialog(
        ui, state, lambda: None, session=session or _pg_session(), job_manager=object(),
        inventory=_g5_inventory(),
    ))
    return ui, notices, events


def _cdc_only():
    from dsql_migrator.ui.data_migration._models import MigrationType

    return MigrationType.CDC_ONLY


def _headers(notices):
    return [(tone, header) for tone, header, _body in notices]


# ---------------------------------------------------------------------------
# Decision (2): the slot-position check judges a stack's FIRST Start only
# ---------------------------------------------------------------------------


def test_a_never_started_stacks_moved_slot_is_refused_by_the_dialog_and_the_worker(
    monkeypatch,
) -> None:
    # The check itself stays (guard): a stack whose parameters record no Start, and a slot that
    # is not where this Full Load created it -- another run with the same CDC stack name
    # re-created it.
    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    _slot_at(monkeypatch, MOVED)
    ui, notices, _events = _dialog(monkeypatch, state)
    assert ("error", SLOT_REFUSAL) in _headers(notices)
    assert ui.buttons["Start CDC"].enabled is False
    # A re-snapshot does run on a stack never started, so the dialog still offers it.
    assert "Re-snapshot every table instead" in ui.buttons
    run = _start(monkeypatch, state=state, confirmed=MOVED)
    assert SLOT_REFUSAL in str(run["error"]) and run["kwargs"] == {}


def test_a_start_that_failed_before_any_connector_existed_never_exempts_the_next(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX (f[1]): the Start was marked "went ahead" right before
    # run_cdc_start, which then failed before it created any connector (here the stack was
    # UPDATE_IN_PROGRESS) -- and from then on no Start was checked, in this process or, the
    # mark being saved, after a restart: the next Start streamed from the re-created slot.
    from dsql_migrator.core.cdc_deployer import CdcDeployError

    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    failed = _start(monkeypatch, state=state, confirmed=AT_START,
                    run_raises=CdcDeployError("cdc-stack is UPDATE_IN_PROGRESS"))
    assert isinstance(failed["error"], CdcDeployError) and failed["kwargs"]
    # The finished job re-probes the stack: the failed update recorded nothing.
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    again = _start(monkeypatch, state=state, confirmed=MOVED)
    assert SLOT_REFUSAL in str(again["error"]) and again["kwargs"] == {}
    # ...and after an app restart, through the real session snapshot.
    restored = _restart(_snapshot(state))
    _phase_probe(monkeypatch, restored, NEVER_STARTED)
    _ui, notices, _e = _dialog(monkeypatch, restored)
    assert ("error", SLOT_REFUSAL) in _headers(notices)
    after = _start(monkeypatch, state=restored, confirmed=MOVED)
    assert SLOT_REFUSAL in str(after["error"]) and after["kwargs"] == {}


def test_a_start_the_check_refused_is_judged_again_after_a_restart(monkeypatch) -> None:
    # Guard: a refusal changes nothing the stack records, so the next Start -- in this process
    # or after a restart -- is refused for the same moved slot.
    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    refused = _start(monkeypatch, state=state, confirmed=MOVED)
    assert SLOT_REFUSAL in str(refused["error"])
    restored = _restart(_snapshot(state))
    _phase_probe(monkeypatch, restored, NEVER_STARTED)
    again = _start(monkeypatch, state=restored, confirmed=MOVED)
    assert SLOT_REFUSAL in str(again["error"]) and again["kwargs"] == {}


def _start_saved_at_submit(monkeypatch, *, state, confirmed):
    """The real Start CDC, with the session snapshot captured as the submit's own refresh()
    saves it: after ``_start_cdc_deploy`` returned (the loop has recorded the Start's ops
    window), before its worker ran. Returns ``(run, snapshot)``."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    saved: list = []
    real = _cdc_ui._logged_cdc_lifecycle

    def _lifecycle(action, *, detail, work, residue=None):
        wrapped = real(action, detail=detail, work=work, residue=residue)

        def _job(handle):
            saved.append(_snapshot(state))
            return wrapped(handle)

        return _job

    monkeypatch.setattr(_cdc_ui, "_logged_cdc_lifecycle", _lifecycle)
    try:
        run = _start(monkeypatch, state=state, confirmed=confirmed)
    finally:
        monkeypatch.setattr(_cdc_ui, "_logged_cdc_lifecycle", real)
    return run, saved[0]


def test_a_started_then_stopped_stack_is_exempt_also_after_a_restart(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (f[7]): the "went ahead" mark lived in process memory until the
    # next UI-driven save; restored from the save the submit made, the session held no Start,
    # and the real Stop -> Start was refused ("another run ... re-created or read it") --
    # its own connector had moved the slot. Now the stack's record decides.
    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    went, saved = _start_saved_at_submit(monkeypatch, state=state, confirmed=AT_START)
    assert went["error"] is None and went["kwargs"]  # the connectors were created
    _phase_probe(monkeypatch, state, RUNNING)  # streaming...
    _phase_probe(monkeypatch, state, STOPPED)  # ...then Stop CDC
    assert state.cdc_has_committed_offset is True
    _ui, notices, _e = _dialog(monkeypatch, state)
    assert ("error", SLOT_REFUSAL) not in _headers(notices)
    in_process = _start(monkeypatch, state=state, confirmed=MOVED)
    assert in_process["error"] is None and in_process["kwargs"]
    # The app restarted before any later save: restored from the submit's save, then the
    # stack-phase probe reads the stopped stack (Start CDC is offered only after it).
    restored = _restart(saved)
    _phase_probe(monkeypatch, restored, STOPPED)
    _ui, notices, _e = _dialog(monkeypatch, restored)
    assert ("error", SLOT_REFUSAL) not in _headers(notices), notices
    resumed = _start(monkeypatch, state=restored, confirmed=MOVED)
    assert resumed["error"] is None and resumed["kwargs"]


def test_retry_cdc_on_a_stack_whose_connectors_exist_is_never_judged_on_the_slot(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: Retry CDC (no dialog; a connector is missing, the other streams)
    # on a restored session had no "went ahead" mark, so the worker refused it on the slot the
    # surviving source connector had moved. Connectors in place are a Start the stack records.
    state = _fresh()
    _phase_probe(monkeypatch, state, RUNNING)
    assert state.cdc_has_committed_offset is False and state.cdc_stack_phase == "running"
    retry = _start(monkeypatch, state=state, confirmed=MOVED)
    assert retry["error"] is None and retry["kwargs"]


def test_a_streamed_stack_gates_a_new_start_point_full_load(monkeypatch) -> None:
    # G25 round 6 (decision A; this was test_a_full_load_rerun_on_a_streamed_stack_is_not_
    # blocked, a guard that Stop -> Re-run Full Load -> Start CDC went ahead): that path is f0's
    # tail replay with no block -- Stop CDC removed the sink without a drain, so its group
    # resumes at its last commit and applies changes older than the re-run's snapshot over the
    # reloaded tables, while the source continues from the new slot. A Full Load that takes a
    # new start point now waits for new topics on such a stack: the Full Load step gates on
    # the CDC step's one answer, read from the same stack probe.
    from dsql_migrator.ui.data_migration._cdc_state import cdc_streaming_started
    from dsql_migrator.ui.data_migration._cdc_status import cdc_stack_has_streamed

    state = _fresh()
    _phase_probe(monkeypatch, state, STOPPED)
    assert cdc_stack_has_streamed(state) is True
    # "Stop CDC first" alone never held it back: nothing streams on a stopped stack.
    assert cdc_streaming_started(state, None) is False
    # Guard: a stack whose connectors were never started takes the new start point (the slot
    # the Full Load re-creates is judged at that stack's first Start).
    never = _fresh()
    _phase_probe(monkeypatch, never, NEVER_STARTED)
    assert cdc_stack_has_streamed(never) is False


def test_a_mysql_session_is_untouched(monkeypatch) -> None:
    # Guard: no PostgreSQL read, no attribute of its own, and the snapshot saves the CDC ops
    # window exactly as the Start recorded it.
    from dsql_migrator.ui import connect
    from dsql_migrator.ui.data_migration import DataMigrationState
    from tests.test_ui_data_migration import _job_wm, _run_start_cdc_capture

    def _no_source(*_a, **_k):
        raise AssertionError("a MySQL Start must not read the PostgreSQL catalog")

    monkeypatch.setattr(connect, "make_source_engine_factory", _no_source)
    run = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.MYSQL, start_mode="auto", job_watermark=_job_wm(),
        catch=True,
    )
    assert run["error"] is None and run["kwargs"]
    state = run["state"]
    assert set(vars(state)) == set(vars(DataMigrationState()))
    assert not [e for e in run["events"] if "PostgreSQL" in str(e[0])]
    snapshot = _snapshot(state)
    assert snapshot.cdc_ops_window_start == _job_wm().snapshot_timestamp
    assert _restart(snapshot).cdc_ops_window_start == _job_wm().snapshot_timestamp


def test_start_over_leaves_nothing_behind(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (f[3], f[8]): the PostgreSQL window was an undeclared attribute,
    # which Start over (reset_in_place re-runs __init__ on the same object) kept, so a later
    # MySQL journey in the session saved the old PostgreSQL value as its CDC ops window -- after
    # a restart the DLQ cut-over gate then named a wrong CDC start and held Cut over.
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.data_migration._state import DataMigrationStore
    from tests.test_ui_data_migration import _job_wm, _run_start_cdc_capture

    store = DataMigrationStore()
    state = store.get_or_create("s1")
    state.set_selection(TableSelection(selected_tables=["app.orders"]))
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    went = _start(monkeypatch, state=state, confirmed=AT_START)  # went ahead
    assert went["error"] is None
    refused = _start(monkeypatch, state=state, confirmed=MOVED)  # refused
    assert SLOT_REFUSAL in str(refused["error"])
    store.reset_in_place("s1")
    # Nothing of the PostgreSQL Starts is left on the object (the one other undeclared entry,
    # the refused Start's notice, is keyed to its job, which Start over cleared).
    leftover = set(vars(state)) - set(vars(DataMigrationState()))
    assert not [name for name in leftover if "pg" in name or "window" in name], leftover
    assert state.cdc_ops_window_start is None and state.cdc_has_committed_offset is False
    assert state.cdc_stack_phase is None
    # A MySQL journey in the same session saves its own window.
    state.set_selection(TableSelection(selected_tables=["orders"]))
    mysql = _run_start_cdc_capture(
        monkeypatch, source_type=SourceType.MYSQL, start_mode="auto", job_watermark=_job_wm(),
        catch=True, state=state,
    )
    assert mysql["error"] is None
    assert _snapshot(state).cdc_ops_window_start == _job_wm().snapshot_timestamp


def test_the_persisted_window_machinery_is_gone() -> None:
    # Decision (2): nothing beside what the stack records; no snapshot change for it.
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui import session_persistence
    from dsql_migrator.ui.data_migration import _cdc_ui

    for name in ("_PG_START_WINDOW_ATTR", "_hold_pg_start_window", "_note_pg_start_went_ahead",
                 "_pg_started_from", "cdc_ops_window_to_persist"):
        assert not hasattr(_cdc_ui, name), name
    assert not hasattr(session_persistence, "_ops_window_to_persist")
    assert not hasattr(cdc_pg_slot, "_started_since")
    assert not [name for name in SessionSnapshot.model_fields if "pg_start" in name]


def test_the_blocker_judges_a_stack_by_what_it_records() -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_replication_objects_blocker as blocker
    from tests.test_g25_pg_publication import _objects

    kw = dict(resumes_from_slot=True, start_lsn=AT_START)
    moved = blocker(_objects(confirmed=MOVED), ["app.a"], stack_started=False, **kw)
    assert moved is not None and moved[0] == SLOT_REFUSAL
    # Its way out is the route rule's (pg_cdc_route, G25 round 5), not the block's own.
    assert moved[1].endswith("silently skip the changes in between.")
    assert "this CDC stack records no Start of its connectors" in moved[1]
    assert blocker(_objects(confirmed=MOVED), ["app.a"], stack_started=True, **kw) is None
    assert blocker(_objects(confirmed=AT_START), ["app.a"], stack_started=False, **kw) is None


def test_the_gate_reads_the_stack_record_from_the_phase_probe(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    for parameters, started in ((NEVER_STARTED, False), (STOPPED, True), (RUNNING, True)):
        state = _fresh()
        _phase_probe(monkeypatch, state, parameters)
        assert _cdc_ui._pg_stack_started(state) is started, parameters


# ---------------------------------------------------------------------------
# f[2]: a source the pre-flight cannot connect to does not block -- and is never silent
# ---------------------------------------------------------------------------


def _unreachable(monkeypatch) -> list:
    """A source whose every connection fails with a message that carries a host and a user.
    Returns the list of connect attempts."""
    from dsql_migrator.ui import connect

    attempts: list = []

    class OperationalError(Exception):
        pass

    class _Engine:
        def connect(self):
            attempts.append(1)
            raise OperationalError(_LEAKY)

        def dispose(self):
            pass

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine()))
    return attempts


def _value_free(text: str) -> bool:
    return not any(s in text for s in ("db.internal", "10.0.0.5", "migrator_admin", _LEAKY))


@pytest.mark.parametrize("cdc_only", [False, True], ids=["full-load-and-cdc", "cdc-only"])
def test_the_dialog_warns_when_the_source_cannot_be_read_and_does_not_block(
    monkeypatch, cdc_only
) -> None:
    # FAILS WITHOUT THE FIX (f[2]): Secrets Manager auth after an app restart -- the secret is
    # read only when the connection is tested, so this session holds no credentials -- and
    # the pre-flight returned None: no block, no word. The slot-position check (Full load +
    # CDC) or the inheritance check (CDC only snapshots) silently did not run.
    from dsql_migrator.ui.data_migration import _cdc_ui

    _unreachable(monkeypatch)
    state = _fresh(_cdc_only() if cdc_only else None)
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    ui, notices, events = _dialog(
        monkeypatch, state, session=_pg_session(password=None, secret_id=_SECRET))
    assert ui.buttons["Start CDC"].enabled is True  # not blocked
    assert not [n for n in notices if n[0] == "error"], notices
    (warning,) = [n for n in notices if n[1] == _cdc_ui.PG_CHECKS_NOT_RUN_HEADER]
    tone, _header, body = warning
    assert tone == "warning"
    assert "this session holds no source credentials" in body
    assert "Test the source connection on the Connect step" in body
    assert "a replication slot another run with the same CDC stack name re-created" in body
    (line,) = [e for e in events if e[0] == _cdc_ui.PG_CHECKS_NOT_RUN_ACTION]
    assert line[1]["status"] is ActivityStatus.WARNING
    assert "this session holds no source credentials" in line[1]["detail"]
    assert "did not run" in line[1]["detail"]
    assert _value_free(body) and _value_free(line[1]["detail"])


def test_the_worker_goes_ahead_on_an_unreadable_source_and_records_why(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (f[2]): Retry CDC (no dialog), or a source lost since the dialog:
    # the Start went ahead with no record that its publication/slot checks never ran.
    from dsql_migrator.ui.data_migration import _cdc_ui

    attempts = _unreachable(monkeypatch)
    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    run = _start(monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]  # fail-open: the Start went ahead
    assert len(attempts) == 1  # one try, not one per check
    (line,) = [e for e in run["events"] if e[0] == _cdc_ui.PG_CHECKS_NOT_RUN_ACTION]
    status, detail = line[1]["status"], line[1]["detail"]
    assert status is ActivityStatus.WARNING
    assert detail.startswith(
        "could not read the PostgreSQL source (the connection failed: OperationalError), so "
        "the publication, replication slot and inheritance checks did not run")
    assert detail.endswith("this Start CDC went ahead without them")
    assert _value_free(detail)


def test_the_reason_names_the_credentials_or_the_failure_class_only() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_checks_not_run_reason

    exc = RuntimeError(_LEAKY)
    secret = _pg_checks_not_run_reason(_pg_session(password=None, secret_id=_SECRET), exc)
    assert secret.startswith("this session holds no source credentials: the Secrets Manager")
    no_password = _pg_checks_not_run_reason(_pg_session(password=None), exc)
    assert no_password == "this session holds no source password: an app restart clears it"
    assert _pg_checks_not_run_reason(_pg_session(), exc) == "the connection failed: RuntimeError"
    assert all(_value_free(r) and _SECRET not in r for r in (secret, no_password))


def test_the_reconnect_notice_is_not_repeated_as_a_not_run_warning(monkeypatch) -> None:
    # Guard: with no source password the dialog already says "Reconnect before starting CDC"
    # and disables Start; a second notice for the same cause would only add noise.
    from dsql_migrator.ui.data_migration import _cdc_ui

    _unreachable(monkeypatch)
    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    _ui, notices, events = _dialog(
        monkeypatch, state, session=_pg_session(password=None),
        conn_blocker="Re-enter the source credentials.")
    assert ("error", "Reconnect before starting CDC") in _headers(notices)
    assert not [n for n in notices if n[1] == _cdc_ui.PG_CHECKS_NOT_RUN_HEADER]
    assert not [e for e in events if e[0] == _cdc_ui.PG_CHECKS_NOT_RUN_ACTION]


def test_a_readable_source_says_nothing_about_checks_not_run(monkeypatch) -> None:
    # Guard: the notice and the line are for a source that could not be read only.
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh()
    _phase_probe(monkeypatch, state, NEVER_STARTED)
    _slot_at(monkeypatch, AT_START)
    _ui, notices, events = _dialog(monkeypatch, state)
    assert notices == [] and events == []
    run = _start(monkeypatch, state=state, confirmed=AT_START)
    assert run["error"] is None
    assert not [e for e in run["events"] if e[0] == _cdc_ui.PG_CHECKS_NOT_RUN_ACTION]


def test_a_resumed_starts_slot_read_that_fails_is_reported_once(monkeypatch) -> None:
    # A stopped stack's snapshot.mode=initial Start reads its slot a second time
    # (_probe_pg_resume_slot); a failure there is reported too, once.
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.ui import connect
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_g25_pg_publication import _objects
    from tests.test_ui_data_migration import _g5_inventory

    calls: list = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def execute(self, statement, _params=None):  # the inheritance read: none
            assert "pg_catalog.pg_inherits" in str(statement)
            return SimpleNamespace(fetchall=lambda: [])

    class _Engine:
        def connect(self):
            calls.append(1)
            if len(calls) > 1:
                raise ConnectionError("lost")
            return _Conn()

        def dispose(self):
            pass

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine()))
    monkeypatch.setattr(cdc_pg_slot, "read_pg_replication_objects", lambda *_a, **_k: _objects(
        publication_name="pub", slot_name="slot", publication_tables=frozenset({"app.orders"})))
    state = _fresh(_cdc_only())
    _phase_probe(monkeypatch, state, STOPPED)
    told: list = []
    verdict = _cdc_ui._pg_objects_gate_block(
        state, _pg_session(), _g5_inventory(), None, on_unread=told.append)
    assert verdict is None and len(calls) == 2
    assert told == ["the connection failed: ConnectionError"]


def test_a_connected_source_whose_inheritance_read_fails_still_refuses(monkeypatch) -> None:
    # Guard: only a source the tool cannot connect to fails open. A Start that snapshots and
    # whose inheritance read alone failed is still refused (G25 round 3).
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.ui import connect
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_g25_pg_publication import _objects
    from tests.test_ui_data_migration import _g5_inventory

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def execute(self, *_a, **_k):
            raise RuntimeError("permission denied for table pg_inherits")

    monkeypatch.setattr(connect, "make_source_engine_factory", lambda *_a, **_k: (
        lambda _cfg: SimpleNamespace(connect=_Conn, dispose=lambda: None)))
    monkeypatch.setattr(cdc_pg_slot, "read_pg_replication_objects", lambda *_a, **_k: _objects(
        publication_name="pub", slot_name="slot", publication_tables=frozenset({"app.orders"})))
    state = _fresh(_cdc_only())
    told: list = []
    verdict = _cdc_ui._pg_objects_gate_block(
        state, _pg_session(), _g5_inventory(), None, on_unread=told.append)
    assert verdict is not None and verdict[0] == cdc_pg_slot.INHERITANCE_UNREADABLE_HEADER
    assert told == []


# ---------------------------------------------------------------------------
# f[0]: Full load only lists the inheritance WARN too
# ---------------------------------------------------------------------------

_INHERITANCE_TITLE = "Inheritance child tables are migrated as tables of their own"


class _PgSource:
    """A prerequisite source probe for a PostgreSQL source with inheritance parents."""

    def __init__(self, *, reachable=True, inheritance=None, fails=False):
        from dsql_migrator.core.models import ConnectionResult

        self._reachable = ConnectionResult(success=reachable)
        self._inheritance = dict(inheritance or {})
        self._fails = fails
        self.asked: list = []

    def reachable(self):
        return self._reachable

    def grants(self):
        return []

    def variables(self):
        return {}

    def cdc_prerequisites(self, *_a, **_k):
        return None

    def inheritance_children(self, table_names):
        self.asked.append(list(table_names))
        if self._fails:
            raise RuntimeError("catalog read failed")
        return dict(self._inheritance)


def _tables():
    cols = [ColumnDef(name="id", mysql_type="integer")]
    return [TableDef(name="app.par", columns=cols, primary_key=["id"]),
            TableDef(name="app.t", columns=cols, primary_key=["id"])]


def _full_load_report(source, source_type=SourceType.POSTGRES):
    from dsql_migrator.core.prerequisites import PrerequisiteChecker
    from tests.test_prerequisites import _FakeTarget

    checker = PrerequisiteChecker(
        source_probe=source, target_probe=_FakeTarget(existing={"app.par", "app.t"}))
    return checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, source_type=source_type,
                                 tables=["app.par", "app.t"]),
        tables=_tables(),
    )


def test_full_load_only_lists_the_inheritance_warn_for_a_postgresql_parent() -> None:
    # FAILS WITHOUT THE FIX (f[0]): only the CDC prerequisites read the children, so in Full
    # load only a parent whose rows all live in its INHERITS children (inheritance used as
    # partitioning) loaded no rows -- FROM ONLY -- Validation said 0 = 0 MATCH, and nothing
    # said why.
    from dsql_migrator.core import prerequisites_postgres as pp

    children = {"app.par": (2, ("app.par_2026_01", "app.par_2026_02"))}
    source = _PgSource(inheritance=children)
    report = _full_load_report(source)
    assert source.asked == [["app.par", "app.t"]]  # one catalog read for the selection
    (row,) = [r for r in report.results if r.title == _INHERITANCE_TITLE]
    assert (row.target, row.status, row.required) == ("app.par", PrerequisiteStatus.WARN, False)
    # The CDC modes' row and text, word for word.
    expected = pp.check_inheritance_children(
        _tables()[0], pp.PostgresCdcFacts(inheritance_children=children), snapshots=False)
    assert row == expected
    assert "only app.par's own rows (FROM ONLY)" in row.detail
    assert "Select each child table whose rows you need" in row.remediation


@pytest.mark.parametrize("case", ["mysql", "unreachable", "read-fails", "none"])
def test_the_full_load_inheritance_row_is_absent_when_it_does_not_apply(case) -> None:
    source = _PgSource(
        reachable=case != "unreachable", fails=case == "read-fails",
        inheritance={} if case == "none" else {"app.par": (1, ("app.kid",))},
    )
    report = _full_load_report(
        source, SourceType.MYSQL if case == "mysql" else SourceType.POSTGRES)
    assert not [r for r in report.results if r.title == _INHERITANCE_TITLE]
    assert source.asked == ([] if case in ("mysql", "unreachable") else [["app.par", "app.t"]])


def test_the_session_probe_reads_the_children_with_one_catalog_statement() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    class _Conn:
        def __init__(self, fails=False):
            self.calls: list = []
            self._fails = fails

        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def execute(self, statement, params=None):
            self.calls.append((" ".join(str(statement).split()), params))
            if self._fails:
                raise RuntimeError("catalog read failed")
            return SimpleNamespace(fetchall=lambda: [("app.par", 2, ["app.k1", "app.k2"])])

    def _probe(source_type, conn, disposed):
        probe = SessionSourceProbe(
            SourceConnectionConfig(source_type=source_type, host="h", port=5432, username="u"),
            None)
        probe._engine_factory = lambda _cfg: SimpleNamespace(  # type: ignore[assignment]
            connect=lambda: conn, dispose=lambda: disposed.append(True))
        return probe

    disposed: list = []
    conn = _Conn()
    assert _probe(SourceType.POSTGRES, conn, disposed).inheritance_children(
        ["app.par", "app.t"]) == {"app.par": (2, ("app.k1", "app.k2"))}
    ((sql, params),) = conn.calls
    assert "JOIN pg_catalog.pg_inherits i ON i.inhparent = c.oid" in sql
    assert params == {"names": ["app.par", "app.t"]}
    assert disposed == [True]
    broken = _Conn(fails=True)
    assert _probe(SourceType.POSTGRES, broken, disposed).inheritance_children(["app.par"]) is None
    mysql = _Conn()
    assert _probe(SourceType.MYSQL, mysql, disposed).inheritance_children(["app.par"]) is None
    assert mysql.calls == []


# ---------------------------------------------------------------------------
# f[4]: the oversized-value probe reads an inheritance parent FROM ONLY
# ---------------------------------------------------------------------------


def _size_conn(oversized, *, parents=(), catalog_fails=False):
    """tests/test_prerequisite_probes.py's size-probe fake, whose catalog says which tables
    are inheritance parents."""
    from tests.test_prerequisite_probes import _SizeProbeConn

    class _Conn(_SizeProbeConn):
        def __init__(self):
            super().__init__(set(oversized))
            self.catalog_reads: list = []

        def execute(self, statement, params=None):
            sql = " ".join(str(statement).split())
            if "AS inheritance_parent" in sql:
                self.statements.append(sql)
                self.catalog_reads.append(dict(params or {}))
                if catalog_fails:
                    raise RuntimeError("catalog read failed")
                hit = f"{params['schema']}.{params['name']}" in set(parents)
                return SimpleNamespace(scalar=lambda: hit)
            return super().execute(statement, params)

    return _Conn()


def _measured(conn) -> list:
    return [s for s in conn.statements if s.startswith("SELECT EXISTS")]


def test_the_oversized_probe_reads_an_inheritance_parent_from_only() -> None:
    # FAILS WITHOUT THE FIX (f[4]): "SELECT EXISTS (SELECT 1 FROM "shop"."par" ...)" -- a
    # child table's 1.1 MB note was reported as the parent's, whose own rows (all the Full Load
    # and CDC move) hold none, and the advice was to exclude the column.
    from tests.test_prerequisite_probes import _size_probe

    conn = _size_conn({"note"}, parents={"shop.par"})
    answer = _size_probe(SourceType.POSTGRES, conn).oversized_values(
        "shop.par", ["note", "body"], limit_bytes=1024 * 1024, timeout_seconds=5.0)
    assert answer == {"note": True, "body": False}
    assert _measured(conn) == [
        'SELECT EXISTS (SELECT 1 FROM ONLY "shop"."par" WHERE octet_length("note") > :limit)',
        'SELECT EXISTS (SELECT 1 FROM ONLY "shop"."par" WHERE octet_length("body") > :limit)',
    ]
    # One read per table, the Full Load's own live rule, and under the statement cap.
    assert conn.catalog_reads == [{"schema": "shop", "name": "par"}]
    (catalog,) = [s for s in conn.statements if "AS inheritance_parent" in s]
    assert "(c.relkind = 'r' AND c.relhassubclass)" in catalog
    assert conn.statements.index(catalog) > next(
        i for i, s in enumerate(conn.statements) if "statement_timeout" in s)


@pytest.mark.parametrize("table", ["shop.plain", "shop.ptab"], ids=["plain", "partitioned"])
def test_a_plain_or_partitioned_tables_size_statement_is_unchanged(table) -> None:
    # Guard: ONLY on a partitioned table would read none of its rows (they are its partitions').
    from tests.test_prerequisite_probes import _size_probe

    conn = _size_conn(set(), parents={"shop.par"})
    _size_probe(SourceType.POSTGRES, conn).oversized_values(
        table, ["note"], limit_bytes=1024 * 1024, timeout_seconds=5.0)
    schema, name = table.split(".")
    assert _measured(conn) == [
        f'SELECT EXISTS (SELECT 1 FROM "{schema}"."{name}" WHERE octet_length("note") > :limit)'
    ]


def test_an_unreadable_parent_check_measures_nothing_and_mysql_never_asks() -> None:
    from tests.test_prerequisite_probes import _size_probe

    broken = _size_conn({"note"}, catalog_fails=True)
    assert _size_probe(SourceType.POSTGRES, broken).oversized_values(
        "shop.par", ["note"], limit_bytes=1024 * 1024, timeout_seconds=5.0) is None
    assert _measured(broken) == []  # not determined -- never a guess either way
    mysql = _size_conn({"note"}, parents={"shop.par"})
    assert _size_probe(SourceType.MYSQL, mysql).oversized_values(
        "shop.par", ["note"], limit_bytes=1024 * 1024, timeout_seconds=5.0) == {"note": True}
    assert mysql.catalog_reads == []


def test_round_4_keeps_the_saved_snapshot_shape() -> None:
    # The window rides in no field of its own, and the existing one is saved as recorded: a
    # snapshot this round writes is one an older build reads.
    from dsql_migrator.core.session_state_store import SessionSnapshot

    state = _fresh()
    state.set_cdc_ops_window_start(CAPTURED)
    payload = json.loads(_snapshot(state).model_dump_json())
    assert payload["cdc_ops_window_start"].startswith("2026-10-09T12:00:00")
    assert set(payload) <= set(SessionSnapshot.model_fields)
