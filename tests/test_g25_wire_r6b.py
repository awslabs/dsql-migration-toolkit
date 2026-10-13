# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 6b, package WIRE: every Full Load run that takes a NEW start point carries the
new-start-point gate (decision A), the page header's direct Run included.

Round 6 (package FL) gated the Full Load step's buttons, the Start dialog and the engine
(``run_full_load(start_point_refusal=...)``), but nothing passed ``start_point_refusal``, and
the page header's Run -- ``header_runner`` -> ``runner()`` when no column-exclusion overlay is
in play -- reached the engine with no gate at all. So on a Full load + CDC migration whose CDC
stack had streamed (stopped, running, or a rolled-back Start), the header Run still took a new
watermark: on PostgreSQL its capture replaced the replication slot, and the next Start CDC
re-applied the old sink tail over the reload while the source skipped the changes in between.

* ``runner()`` -- the one place a new-watermark run is submitted (the Start dialog's Confirm
  for Start / Re-run Full Load / Re-run all tables, and the header's direct Run) -- refuses it
  before anything changes, with the toast a click on the step's disabled action gets.
* The engine re-reads the same gate on the job thread before it seeds a chunk or captures the
  watermark, so a stack that streamed after the click is refused there too.
* Retry unfinished tables, Reload specific tables and the per-table Reload keep the original
  start point (the retry engine) and are not gated.

Each stack state comes from the REAL stack-phase probe over one describe of the cdc-stack.
Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import ast
import pathlib
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType, StepStatus, TableSelection
from dsql_migrator.ui.data_migration import MigrationType
from dsql_migrator.ui.data_migration._full_load_engine import (
    FullLoadRefusedError,
    new_start_point_block,
    new_start_point_refusal,
)
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

from tests.test_g12_table_set_after_load import _c, _job, _Jobs, _loaded
from tests.test_g25_fl_r6 import NEVER_STARTED, STREAMED, _probe

MY, PG = SourceType.MYSQL, SourceType.POSTGRES
FLC = MigrationType.FULL_LOAD_AND_CDC
NAMES = ["app.a", "app.b"]


def _failed_b():
    """A Full load + CDC record whose app.b failed (Retry unfinished tables offers it)."""
    return _job("FAILED", [_c("app.a"), _c("app.b", status="FAILED")],
                counts={"app.a": 1, "app.b": 1})


# The new-start-point runs: (job record, step status). Start Full Load has no record yet.
_NEW_START = {
    "Start Full Load": (lambda: None, StepStatus.NOT_STARTED),
    "Re-run Full Load": (_loaded, StepStatus.DONE),
}


class _RunningJobs(_Jobs):
    """Records a submitted job and runs it at once on the caller's thread."""

    def submit(self, fn):
        job_id = super().submit(fn)
        fn(SimpleNamespace(job_id=job_id))
        return job_id


def _probe_absent(monkeypatch, state) -> None:
    """The REAL stack-phase probe over a describe that finds no cdc-stack at all."""
    import dsql_migrator.core.cdc_deployer as dep
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    class _Deployer:
        def describe_stack_or_none(self, _name):
            return None

        def list_cdc_stacks(self):
            return []

    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _Deployer())
    _probe_cdc_stack_phase(
        state,
        SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None),
    )


def _screen(monkeypatch, engine, *, job=None, status=StepStatus.NOT_STARTED, stack=None,
            jobs=None, migrator=None):
    """The REAL Data Migration screen of a Full load + CDC session (app.a and app.b, both
    with target DDL), its cdc-stack probed into ``stack`` ("absent", or a fl-r6 stack)."""
    import sys
    import types

    from dsql_migrator.core.models import (
        AssessmentReport,
        ColumnDef,
        SourceConnectionConfig,
        SourceInventory,
        TableDef,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

    from tests.test_ui_data_migration import _ScreenUi

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    pg = engine is PG
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(
        source_type=engine, host="h", port=5432 if pg else 3306, database="app", username="u"),
        None)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"))
    session.set_source_verified(True)
    session.set_target_verified(True)
    session.set_migration_type(FLC)
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, status))
    tables = [TableDef(name=n, columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                       primary_key=["id"]) for n in NAMES]
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=SourceInventory(tables=tables), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = [f"table:{n}" for n in NAMES]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.job_id = "job-1" if job is not None else None
    state.set_selection(TableSelection(selected_tables=list(NAMES)))
    if stack == "absent":
        _probe_absent(monkeypatch, state)
    elif stack is not None:
        _probe(monkeypatch, state, stack)
    if jobs is None:
        jobs = _Jobs({"job-1": job} if job is not None else {})
    built: list = []

    def _factory(inputs):
        built.append(inputs)
        return migrator if migrator is not None else SimpleNamespace()

    content, header_run = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs, migrator_factory=_factory,
    )
    return SimpleNamespace(content=content, header_run=header_run, ui=ui, session=session,
                           state=state, jobs=jobs, built=built)


def _step_callbacks(monkeypatch, s) -> dict:
    """Render the screen and hand back the callbacks it gives the Full Load step."""
    from dsql_migrator.ui import data_migration as dm

    captured: dict = {}
    monkeypatch.setattr(dm, "_render_full_load_step",
                        lambda *_a, **kw: captured.update(kw))
    s.content(lambda: None)
    assert captured, "the Full Load step was not rendered"
    return captured


def _record_new_start_point_runs(monkeypatch) -> list:
    from dsql_migrator.ui import data_migration as dm

    runs: list = []
    monkeypatch.setattr(dm, "run_full_load",
                        lambda _h, tables, **kw: runs.append(([t.name for t in tables], kw)))
    return runs


def _toasts(ui) -> "list[str]":
    return [text for text, _kw in ui.notified]


# ---------------------------------------------------------------------------
# 1. A stack that has streamed: refused before anything changes, from every entry point
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", list(STREAMED), ids=list(STREAMED))
@pytest.mark.parametrize("action", list(_NEW_START))
@pytest.mark.parametrize("entry", ["page header Run", "Start dialog Confirm"])
def test_a_new_start_point_on_a_streamed_stack_is_refused_before_any_chunk_or_capture(
    monkeypatch, engine, stack, action, entry
) -> None:
    # FAILS WITHOUT THE FIX (decision A): the page header's direct Run submitted a new-watermark
    # run on a stack that had streamed -- on PostgreSQL its capture replaced the slot -- and
    # nothing handed the engine the gate.
    make_job, status = _NEW_START[action]
    job = make_job()
    s = _screen(monkeypatch, engine, job=job, status=status, stack=STREAMED[stack])
    runs = _record_new_start_point_runs(monkeypatch)
    if entry == "page header Run":
        before = get_status(s.session.workflow, WorkflowStep.FULL_LOAD)
        s.header_run()
    else:
        # A Confirm reached runner() although its own re-check passed (the CDC state can
        # change in between): runner() refuses on its own.
        callbacks = _step_callbacks(monkeypatch, s)
        before = get_status(s.session.workflow, WorkflowStep.FULL_LOAD)
        callbacks["start_full_load"]()
    line = new_start_point_block(s.state, engine)[2]
    # Nothing submitted, no migrator built: no chunk seeded, no watermark captured, no slot.
    assert s.jobs.submitted == [] and s.built == [] and runs == []
    assert _toasts(s.ui) == [line] and s.ui.notified[0][1].get("type") == "warning"
    assert len(line) <= 300
    # Nothing changed: the record, the step, no red "Migration failed".
    assert s.state.job_id == ("job-1" if job is not None else None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is before
    assert s.state.error is None


@pytest.mark.parametrize("engine", [MY, PG])
def test_a_header_run_on_an_unread_stack_waits_for_the_read(monkeypatch, engine) -> None:
    # The step's own Start waits for the CDC state to be read (round 6); so does the header.
    s = _screen(monkeypatch, engine, job=_loaded(), status=StepStatus.DONE, stack=None)
    s.header_run()
    assert s.jobs.submitted == [] and s.built == []
    assert _toasts(s.ui) == [new_start_point_block(s.state, engine)[2]]
    assert "Re-check CDC state" in _toasts(s.ui)[0]


# ---------------------------------------------------------------------------
# 2. A stack that never streamed: the header Run runs, and hands the engine the gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", ["never started", "absent"])
@pytest.mark.parametrize("action", list(_NEW_START))
def test_a_header_run_on_a_stack_that_never_streamed_runs_with_the_gate(
    monkeypatch, engine, stack, action
) -> None:
    make_job, status = _NEW_START[action]
    job = make_job()
    s = _screen(monkeypatch, engine, job=job, status=status,
                stack=NEVER_STARTED if stack == "never started" else "absent",
                jobs=_RunningJobs({"job-1": job} if job is not None else {}))
    runs = _record_new_start_point_runs(monkeypatch)
    s.header_run()
    ((tables, kwargs),) = runs
    assert tables == NAMES and len(s.built) == 1 and _toasts(s.ui) == []
    assert s.state.job_id == "job-2"
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.IN_PROGRESS
    gate = kwargs["start_point_refusal"]
    assert gate() is None
    # The gate is read when the engine calls it (the job thread), not frozen at the click:
    # a Stop CDC that lands meanwhile is seen.
    _probe(monkeypatch, s.state, STREAMED["stopped"])
    assert gate() == new_start_point_refusal(s.state, engine)
    assert gate().splitlines()[0] == new_start_point_block(s.state, engine)[2]


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_engine_refuses_a_header_run_whose_stack_streamed_after_the_click(
    monkeypatch, engine
) -> None:
    # The backstop, with the REAL engine and JobManager: the stack is read as never started at
    # the click, a Stop CDC lands before the job thread runs, and the engine refuses before
    # a chunk is seeded or the watermark captured (on PostgreSQL: before the slot is replaced).
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import _full_load_engine as engine_mod

    from tests.test_ui_data_migration import _FakeMigrator

    events: list = []
    monkeypatch.setattr(engine_mod, "log_activity", lambda *a, **k: events.append((a, k)))
    holder: dict = {}

    class _StreamedMeanwhile(JobManager):
        def submit(self, work, **kw):
            _probe(monkeypatch, holder["state"], STREAMED["stopped"])
            return super().submit(work, **kw)

    manager = _StreamedMeanwhile()
    migrator = _FakeMigrator(rows_by_table={n: 1 for n in NAMES})
    s = _screen(monkeypatch, engine, stack=NEVER_STARTED, jobs=manager, migrator=migrator)
    holder["state"] = s.state
    s.header_run()
    job_id = s.state.job_id
    assert job_id is not None and _toasts(s.ui) == []  # the click's check passed
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)
    assert job.status == "FAILED" and job.chunks == [] and job.watermark is None
    assert migrator.captured == 0 and migrator.migrated == []
    line = new_start_point_block(s.state, engine)[2]
    assert manager.get_error(job_id) == f"{FullLoadRefusedError.__name__}: {line}"
    assert [a[1] for a, _k in events] == ["run refused"]


# ---------------------------------------------------------------------------
# 3. Retry unfinished / Reload keep the original start point: never gated
# ---------------------------------------------------------------------------


_KEEPS_START_POINT = {
    # action: (job record, step status, the step callback's call, the tables it reloads)
    "Retry unfinished tables": (_failed_b, StepStatus.FAILED,
                                lambda cb: cb["retry_failed_load"](), ["app.b"]),
    "Reload specific tables": (_loaded, StepStatus.DONE,
                               lambda cb: cb["retry_tables"](["app.a"]), ["app.a"]),
    "Reload": (_loaded, StepStatus.DONE, lambda cb: cb["reload_table"]("app.b"), ["app.b"]),
}


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("stack", ["stopped", "rolled back"])
@pytest.mark.parametrize("action", list(_KEEPS_START_POINT))
def test_retry_and_reload_on_a_streamed_stack_still_run_under_the_original_start_point(
    monkeypatch, engine, stack, action
) -> None:
    from dsql_migrator.ui import data_migration as dm

    make_job, status, call, reloaded = _KEEPS_START_POINT[action]
    job = make_job()
    s = _screen(monkeypatch, engine, job=job, status=status, stack=STREAMED[stack],
                jobs=_RunningJobs({"job-1": job}))
    assert new_start_point_block(s.state, engine) is not None  # the gate holds a new one
    retries: list = []
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, prior, tables, **kw: retries.append(
                            ([t.name for t in tables], kw)))
    new_runs = _record_new_start_point_runs(monkeypatch)
    call(_step_callbacks(monkeypatch, s))
    ((tables, kwargs),) = retries
    assert tables == reloaded and new_runs == []
    assert kwargs["watermark"] is job.watermark  # the original start point
    assert "start_point_refusal" not in kwargs
    line = new_start_point_block(s.state, engine)[2]
    assert line not in _toasts(s.ui)
    assert any(t.startswith("Retrying ") for t in _toasts(s.ui))
    assert s.state.job_id == "job-2"


# ---------------------------------------------------------------------------
# 4. Every new-start-point run in the UI carries the gate
# ---------------------------------------------------------------------------


def test_every_run_full_load_call_in_the_ui_passes_the_gate() -> None:
    # The callers of run_full_load under src/dsql_migrator/ui: the screen's runner() (Start /
    # Re-run Full Load / Re-run all tables through the Start dialog, and the page header's
    # Run). The engine's own back-compat wrapper (run_data_migration) has no session to judge
    # and no UI caller. A new caller without start_point_refusal fails here.
    root = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator" / "ui"
    calls: list = []
    for path in sorted(root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for fn in ast.walk(tree):
            if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for node in ast.walk(fn):
                if (
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "run_full_load"
                ):
                    calls.append((path.name, fn.name, {k.arg for k in node.keywords}))
    callers = {(name, fn) for name, fn, _kw in calls}
    assert ("__init__.py", "work") in callers
    for name, fn, keywords in calls:
        if (name, fn) == ("_full_load_engine.py", "run_data_migration"):
            continue
        assert "start_point_refusal" in keywords, (name, fn)
