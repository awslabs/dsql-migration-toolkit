# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A lost job record ends the Schema apply / Evaluation poll in a settled state (review A6c).

Schema Conversion's and Evaluation's ``_install_poll_timer`` returned bare on
``JobNotFoundError`` from a repeating timer: when the job record was gone (the job store
reaped or pruned it) the poll never settled the step, so it stayed IN_PROGRESS -- spinner
up, Apply / Run locked -- until an app restart triggered the reconcile in
``session_persistence``. Validation's poll already reconciles a lost record itself; the same
handling is ported here, nothing else:

* Schema Conversion: the step goes to FAILED with an actionable error (the same advice the
  restart reconcile gives: re-run the apply; "Skip if exists" leaves existing objects
  untouched) and the screen refreshes, which re-enables the Apply controls.
* Evaluation: the screen refreshes, and its render's existing reconcile
  (``reconcile_evaluation_step``) settles the step -- DONE when the result is there, else
  NOT_STARTED with the interrupted notice -- exactly as it does after a restart.

Offline: the UI, the job manager and the session are doubles.
"""

from __future__ import annotations

from types import SimpleNamespace

from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.ui.workflow import (
    StepStatus,
    WorkflowState,
    WorkflowStep,
    get_status,
    with_status,
)


class _Ui:
    def __init__(self) -> None:
        self.timers: list = []

    def timer(self, interval, callback, **kwargs):
        self.timers.append((interval, callback, kwargs))
        return SimpleNamespace()


class _LostJobs:
    def get_status(self, job_id):
        raise JobNotFoundError(job_id)

    def get_error(self, job_id):  # pragma: no cover - a lost record has no error to read
        raise JobNotFoundError(job_id)


class _Session:
    def __init__(self, step: WorkflowStep) -> None:
        self.workflow = with_status(WorkflowState(), step, StepStatus.IN_PROGRESS)

    def set_workflow(self, workflow) -> None:
        self.workflow = workflow


def test_a_lost_schema_apply_job_fails_the_step_with_the_re_run_advice() -> None:
    from dsql_migrator.ui.schema_conversion import SchemaConversionState, _install_poll_timer

    state = SchemaConversionState()
    state.job_id = "apply-1"
    session = _Session(WorkflowStep.SCHEMA_CONVERSION)
    refreshed: list = []
    ui = _Ui()
    _install_poll_timer(ui, _LostJobs(), session, state, lambda: refreshed.append(True))
    ((_interval, poll, _kw),) = ui.timers
    poll()  # the job record is gone

    # FAILED WITHOUT THE FIX: the poll returned, the step stayed IN_PROGRESS, no refresh.
    assert get_status(session.workflow, WorkflowStep.SCHEMA_CONVERSION) is StepStatus.FAILED
    assert refreshed == [True]
    error = state.error or ""
    assert "apply job record was lost" in error
    assert "Re-run the apply" in error and '"Skip if exists"' in error


def test_a_lost_evaluation_job_refreshes_so_the_render_settles_the_step() -> None:
    from dsql_migrator.ui.evaluation import (
        EvaluationState,
        _eval_job_alive,
        _install_poll_timer,
        reconcile_evaluation_step,
    )

    state = EvaluationState()
    state.job_token = state.begin_run()
    state.job_id = "eval-1"
    session = _Session(WorkflowStep.EVALUATION)
    jobs = _LostJobs()
    refreshed: list = []
    ui = _Ui()
    _install_poll_timer(ui, jobs, session, state, lambda: refreshed.append(True))
    ((_interval, poll, _kw),) = ui.timers
    poll()  # the job record is gone

    # FAILED WITHOUT THE FIX: the poll returned without a refresh, so nothing re-rendered.
    assert refreshed == [True]
    # The refresh's render reconciles it (the step's own rule, unchanged): no live job and no
    # result is the interrupted case.
    status = get_status(session.workflow, WorkflowStep.EVALUATION)
    assert status is StepStatus.IN_PROGRESS  # settled by the render, not by the poll
    assert reconcile_evaluation_step(
        status, job_alive=_eval_job_alive(jobs, state.job_id), has_result=state.result is not None
    ) is StepStatus.NOT_STARTED
