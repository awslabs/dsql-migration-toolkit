# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G27 UI items from the 2026-10-14 Seoul live E2E.

* Item 2: with the connectors RUNNING, the Data Migration type lock said "Locked because CDC
  is starting". ``cdc_streaming_started`` covers a live pipeline and a Start / Stop / Delete
  job in flight alike, and the lock had one sentence for all of them. It now names the state:
  "starting" only while the Start job runs, "started" once the pipeline is live (both keep
  the Stop CDC remedy), and a running Stop or Delete is a wait.
* Item 6: the page twice showed the Validation step while the operator was on Data
  Migration. No server path re-selects a view: the shell writes its selected view only in
  ``select()`` (a click on a nav control) and at page build (``_restore_view()`` from the
  session's ``active_view``, which only ``select()`` writes). Pinned here, so a refresh that
  re-selects a "current stage" cannot creep in unnoticed.
* Item 7: Change & reload's per-table figure is the watermark's scan-free information_schema
  estimate (2,000 for a 2,890-row table), so it is worded as one; nothing scans the source.

Pure state + a NiceGUI double; no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import Watermark
from dsql_migrator.ui.data_migration import (
    DataMigrationState,
    migration_type_lock_reason,
    migration_type_locked,
)
from dsql_migrator.ui.workflow import StepStatus

_REMEDY = "Stop CDC on the CDC step to change the type."


class _Job:
    def __init__(self, status: str, watermark=None) -> None:
        self.status = status
        self.watermark = watermark


class _Jobs:
    """``get_status`` per id; an unknown id is a pruned record (JobNotFoundError)."""

    def __init__(self, jobs: dict) -> None:
        self._jobs = jobs

    def get_status(self, job_id):
        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)
        return self._jobs[job_id]


def _live(state: DataMigrationState) -> DataMigrationState:
    """Connectors detected this session: a wired controller plus my connector names."""
    state.set_cdc_controller(object())
    state.set_cdc_connector_names(
        ["dsql-cdc-e2e26-debezium-source", "dsql-cdc-e2e26-dsql-sink"]
    )
    return state


def _reason(state, jobs) -> str:
    reason = migration_type_lock_reason(
        state, status=StepStatus.NOT_STARTED, job_manager=jobs
    )
    assert reason is not None
    # The lock itself is unchanged: only its wording follows the CDC state.
    assert migration_type_locked(state, jobs, status=StepStatus.NOT_STARTED) is True
    return reason


# ---------------------------------------------------------------------------
# Item 2: the type lock names the CDC state it rests on
# ---------------------------------------------------------------------------


def test_streaming_pipeline_says_started_not_starting() -> None:
    """The live finding: connectors RUNNING, no CDC job in flight."""
    reason = _reason(_live(DataMigrationState()), _Jobs({}))
    assert "starting" not in reason
    assert reason == (
        "Locked because CDC has started — the start point and table set are already "
        "committed. " + _REMEDY
    )


def test_probed_running_stack_says_started() -> None:
    state = DataMigrationState()
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    reason = _reason(state, _Jobs({}))
    assert "has started" in reason and "starting" not in reason
    assert reason.endswith(_REMEDY)


def test_finished_start_job_with_live_connectors_says_started() -> None:
    state = _live(DataMigrationState())
    state.set_cdc_deploy_job_id("start-1", kind="start")
    reason = _reason(state, _Jobs({"start-1": _Job("DONE")}))
    assert "has started" in reason and "starting" not in reason


@pytest.mark.parametrize("job_status", ["PENDING", "RUNNING"])
@pytest.mark.parametrize("connectors_listed", [False, True])
def test_start_in_flight_still_says_starting(job_status, connectors_listed) -> None:
    """"Starting" only while the Start job runs -- also once its connectors are listed."""
    state = DataMigrationState()
    state.set_cdc_stack_phase("infra", status="UPDATE_IN_PROGRESS")
    if connectors_listed:
        _live(state)
    state.set_cdc_deploy_job_id("start-1", kind="start")
    reason = _reason(state, _Jobs({"start-1": _Job(job_status)}))
    assert reason == (
        "Locked because CDC is starting — the start point and table set are already "
        "committed. " + _REMEDY
    )


def test_stop_in_flight_is_a_wait_not_stop_cdc() -> None:
    """A Stop still lists the connectors for a minute or two: "Stop CDC" would ask for the
    action that is already running, and "starting" was false."""
    state = _live(DataMigrationState())
    state.set_cdc_deploy_job_id("stop-1", kind="stop")
    reason = _reason(state, _Jobs({"stop-1": _Job("RUNNING")}))
    assert reason == (
        "Locked while CDC is being stopped. Wait for the Stop CDC that is running to "
        "finish to change the type."
    )


def test_delete_in_flight_is_a_wait_with_the_delete_estimate() -> None:
    from dsql_migrator.ui.data_migration import cdc_delete_eta

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("delete-1", kind="delete")
    reason = _reason(state, _Jobs({"delete-1": _Job("RUNNING")}))
    estimate, _why = cdc_delete_eta(state)
    assert reason == (
        "Locked while the CDC infrastructure is being deleted (a cdc-stack delete takes "
        f"{estimate}). Wait for the deletion to finish to change the type."
    )
    assert "starting" not in reason and _REMEDY not in reason


def test_probed_delete_in_progress_with_listed_connectors_is_a_wait() -> None:
    """A restored session / an out-of-band delete: no job, the probe reports the delete."""
    state = _live(DataMigrationState())
    state.set_cdc_stack_phase("unstable", status="DELETE_IN_PROGRESS")
    reason = _reason(state, _Jobs({}))
    assert reason.startswith("Locked while the CDC infrastructure is being deleted")


def test_the_other_type_locks_are_unchanged() -> None:
    """Only the started-CDC sentence changed: the restored-connectors lock (no controller
    this session) and an idle session read as before."""
    restored = DataMigrationState()
    restored.set_cdc_connector_names(["dsql-cdc-e2e26-dsql-sink"])
    assert _reason(restored, _Jobs({})).startswith(
        "Locked because CDC connectors from a previous run are still deployed"
    )
    assert (
        migration_type_lock_reason(
            DataMigrationState(), status=StepStatus.NOT_STARTED, job_manager=_Jobs({})
        )
        is None
    )


# ---------------------------------------------------------------------------
# Item 6: nothing but select() (and the page build) changes the shell's view
# ---------------------------------------------------------------------------


def _shell_tree() -> ast.Module:
    from dsql_migrator.ui import workflow

    return ast.parse(textwrap.dedent(inspect.getsource(workflow.build_workflow_sidebar)))


def _enclosing_functions(tree: ast.AST) -> dict:
    """``{id(node): name of the innermost enclosing def}`` for every node in ``tree``."""
    owner: dict = {}

    def walk(node: ast.AST, name: str) -> None:
        for child in ast.iter_child_nodes(node):
            inner = (
                child.name
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                else name
            )
            owner[id(child)] = inner
            walk(child, inner)

    walk(tree, "<module>")
    return owner


def test_only_select_writes_the_selected_view() -> None:
    """The jump seen live is not a refresh path: every refresh (``refresh_all``, a job poll,
    a timer) re-renders ``selected["view"]``, which only ``select()`` writes -- a click on a
    nav control -- besides its initial value from ``_restore_view()``."""
    tree = _shell_tree()
    owner = _enclosing_functions(tree)
    writers = set()
    for node in ast.walk(tree):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            targets = [node.target]
        for target in targets:
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name)
                and target.value.id == "selected"
            ):
                writers.add(owner[id(node)])
    assert writers == {"select"}, writers
    # ...and its initial value is the restored view, read once at page build.
    (initial,) = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and node.target.id == "selected"
    ]
    assert "_restore_view()" in ast.unparse(initial.value)


def test_only_select_writes_the_session_active_view_in_the_shell() -> None:
    """``_restore_view()`` reads ``active_view`` -- written in the shell only by
    ``select()``, so a page rebuild lands on the step the operator last selected."""
    tree = _shell_tree()
    owner = _enclosing_functions(tree)
    writers = {
        owner[id(node)]
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_active_view"
    }
    assert writers == {"select"}, writers


# ---------------------------------------------------------------------------
# Item 7: Change & reload's per-table row figure is worded as an estimate
# ---------------------------------------------------------------------------


class _El:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def classes(self, *_a, **_k):
        return self

    def props(self, *_a, **_k):
        return self


class _Ui:
    def __init__(self) -> None:
        self.texts: list[str] = []

    def column(self, *_a, **_k):
        return _El()

    def row(self, *_a, **_k):
        return _El()

    def label(self, text="", *_a, **_k):
        self.texts.append(str(text))
        return _El()


def _drop_card_texts(counts: dict) -> list[str]:
    from dsql_migrator.ui.data_migration._exclusion_dialog import (
        ExclusionDialogContext,
        _render_drop_card,
    )

    watermark = Watermark(
        snapshot_timestamp=datetime(2026, 10, 14, tzinfo=timezone.utc),
        table_row_counts=counts,
        row_counts_approximate=True,
    )
    state = DataMigrationState()
    state.job_id = "fl-1"
    ctx = ExclusionDialogContext(
        migration_state=state,
        job_manager=_Jobs({"fl-1": _Job("DONE", watermark)}),
        session=None,
        inventory_fn=lambda: None,
        inputs_fn=lambda: None,
        refresh=lambda: None,
    )
    resolution = SimpleNamespace(
        diff=SimpleNamespace(added={"app.orders": ("note",)}, removed={}),
        retained_legacy=set(),
        selected_after=None,
    )
    ui = _Ui()
    _render_drop_card(ui, ctx, resolution, ["app.orders"])
    return ui.texts


def test_reload_row_figure_reads_as_an_estimate() -> None:
    texts = _drop_card_texts({"app.orders": 2000})
    (line,) = [t for t in texts if t.startswith("app.orders")]
    assert line == "app.orders   + note (NULL)   ~2,000 rows re-read (estimate)"


def test_reload_row_figure_is_omitted_when_the_estimate_is_unknown() -> None:
    """A never-analyzed PostgreSQL table's estimate is None: no number, no claim."""
    texts = _drop_card_texts({"app.orders": None})
    (line,) = [t for t in texts if t.startswith("app.orders")]
    assert line == "app.orders   + note (NULL)"
