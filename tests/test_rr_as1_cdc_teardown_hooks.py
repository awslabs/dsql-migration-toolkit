# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/data_migration/_cdc_teardown.py: Start over's CDC teardown and the teardown banner,
driven through the hooks themselves.

The hooks were closures inside ``ui/app.py``'s ``build_page`` (AS-1 moved them here), so the
queue advance, the completion notice and Start over's submission through
``submit_cdc_teardown`` were pinned by parsing app.py's source. ``build_cdc_teardown_hooks``
takes the stores and the job manager, so these tests call the hooks with the real stores, a
job manager that records the submitted work and its status, and stubbed deployer functions:
no AWS, no source.

Pinned here:

* a multi-stack Start over records every teardown it launched, in order, behind the one
  marker the first stack claims -- without the queue the banner has nothing to advance to;
* when the tracked stack settles while another is still deleting, the banner moves the
  marker to that one and says which of how many, instead of going silent: to the FIRST
  stack in the queue still deleting, past any that finished before the tracked one;
* once every stack has settled, the banner records the finished-teardown notice -- every
  stack, and the teardown's kind, both read from the marker -- before it clears the marker,
  and the notice's close button dismisses it;
* Start over's "Delete all CDC infrastructure" submits through the CDC step's own
  ``submit_cdc_teardown``, with the source-secret cleanup on for a password-authenticated
  source (the tool created the secret) and off for a Secrets Manager one, and
  ``run_cdc_delete`` receives it. tests/test_g27_docs.py reads the first case;
* a hook reads its session's state when it is called, not when it is built;
* the page hands ``build_workflow_sidebar`` each hook under its kwarg.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

from tests.test_rr_a1_cdc_teardown_paths import (  # noqa: F401 - page is a fixture
    STACK,
    _capture_events,
    _connect,
    _stub_teardown,
    page,
)
from tests.test_rr_int_teardown_retry_source import _JM

OTHER = "dsql-cdc-as1-discovered"
SESSION = "rr-as1"

# Each teardown kwarg of build_workflow_sidebar -> the CdcTeardownHooks field it receives.
_SIDEBAR_HOOKS = {
    "on_reset_cdc": "cdc_teardown_on_reset",
    "cdc_deployed_getter": "cdc_deployed",
    "cdc_stack_name_getter": "cdc_stack_name",
    "cdc_stack_names_getter": "cdc_teardown_stack_names",
    "cdc_teardown_in_flight_getter": "cdc_teardown_in_flight",
    "cdc_teardown_banner_getter": "cdc_teardown_banner",
    "cdc_teardown_retry": "cdc_teardown_retry",
    "cdc_teardown_dismiss": "cdc_teardown_dismiss",
    "cdc_teardown_done_getter": "cdc_teardown_done",
    "cdc_teardown_done_dismiss": "cdc_teardown_done_dismiss",
    "cdc_op_in_flight_getter": "cdc_op_in_flight",
    "cdc_probe": "cdc_probe",
}


class _Ui:
    """The two NiceGUI calls Start over's teardown makes: a toast and the poll timer."""

    def notify(self, *_a, **_k) -> None:
        return None

    def timer(self, *_a, **_k) -> SimpleNamespace:
        return SimpleNamespace(active=True)


def _fake_nicegui(monkeypatch) -> None:
    fake = types.ModuleType("nicegui")
    fake.ui = _Ui()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)


def _hooks(monkeypatch) -> SimpleNamespace:
    """The hooks for a session whose own PostgreSQL CDC stack is live (password auth)."""
    from dsql_migrator.ui.data_migration._cdc_teardown import build_cdc_teardown_hooks
    from dsql_migrator.ui.data_migration._state import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore

    _fake_nicegui(monkeypatch)
    calls = _stub_teardown(monkeypatch, [])
    _capture_events(monkeypatch)
    sessions, migrations, jm = SessionStore(), DataMigrationStore(), _JM()
    hooks = build_cdc_teardown_hooks(
        session_id=SESSION,
        session_store=sessions,
        data_migration_store=migrations,
        job_manager=jm,
    )
    session = sessions.get_or_create(SESSION)
    _connect(session)
    state = migrations.get_or_create(SESSION)
    assert state.set_cdc_stack_name(STACK)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    return SimpleNamespace(hooks=hooks, jm=jm, calls=calls, session=session, state=state)


def start_over_delete(monkeypatch, *, source_secret_id=None) -> "tuple[dict, dict]":
    """Start over's "Delete all CDC infrastructure" of the session's own stack, run to the end.

    Returns the keyword arguments ``submit_cdc_teardown`` was given and those
    ``run_cdc_delete`` received. ``source_secret_id`` connects the source through Secrets
    Manager instead of a password.
    """
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    rig = _hooks(monkeypatch)
    if source_secret_id is not None:
        rig.session.set_source_secret_id(source_secret_id)
    submitted: list = []
    real_submit = cdc_ui.submit_cdc_teardown

    def _spy(*args, **kwargs):  # noqa: ANN002
        submitted.append(kwargs)
        return real_submit(*args, **kwargs)

    monkeypatch.setattr(cdc_ui, "submit_cdc_teardown", _spy)
    rig.hooks.cdc_teardown_on_reset("delete")
    (job_id,) = rig.jm.works
    rig.jm.run(job_id)
    (submit,) = submitted
    (delete,) = rig.calls["delete"]
    return submit, delete


def _start_over_two_stacks(monkeypatch) -> SimpleNamespace:
    """Start over's delete with the session's own stack and a discovered one."""
    rig = _hooks(monkeypatch)
    rig.state.set_cdc_other_stacks([(OTHER, "UPDATE_COMPLETE")])
    rig.hooks.cdc_teardown_on_reset("delete")
    rig.first, rig.second = rig.jm.works
    return rig


def test_a_multi_stack_start_over_records_every_teardown_it_launched(monkeypatch) -> None:
    rig = _start_over_two_stacks(monkeypatch)

    assert rig.state.cdc_teardown_queue == [(rig.first, STACK), (rig.second, OTHER)]
    # One marker, the first stack's: the banner follows it and the queue keeps the rest.
    assert rig.state.cdc_teardown_job_id == rig.first
    assert (rig.state.cdc_teardown_kind, rig.state.cdc_teardown_stack) == ("delete", STACK)
    assert rig.hooks.cdc_teardown_banner() == {
        "state": "running", "kind": "delete", "stack": STACK, "index": 1, "total": 2,
    }


def test_the_banner_moves_to_the_next_unfinished_stack_when_the_tracked_one_settles(
    monkeypatch,
) -> None:
    rig = _start_over_two_stacks(monkeypatch)
    rig.jm.run(rig.first)

    assert rig.hooks.cdc_teardown_banner() == {
        "state": "running", "kind": "delete", "stack": OTHER, "index": 2, "total": 2,
    }
    assert (rig.state.cdc_teardown_job_id, rig.state.cdc_teardown_stack) == (
        rig.second, OTHER,
    )
    assert rig.hooks.cdc_teardown_done() is None  # nothing is reported finished yet


def test_the_banner_moves_past_stacks_that_finished_first_to_the_first_still_deleting(
    monkeypatch,
) -> None:
    """The teardowns run side by side, so a later stack can finish before the tracked one,
    and a tab left alone can see several finish between two reads.

    The banner follows the first stack in the queue that is still deleting
    (``next_unfinished_teardown``), past any that finished. A banner that took the entry
    after the tracked one would find it finished, record the whole teardown as done and
    clear the marker while two stacks are still deleting and billing for MSK / NAT. The
    two-stack tests cannot tell the two apart: there, the next stack is the only one left.
    """
    rig = _hooks(monkeypatch)
    discovered = ["dsql-cdc-as1-second", "dsql-cdc-as1-third", "dsql-cdc-as1-fourth"]
    rig.state.set_cdc_other_stacks([(name, "UPDATE_COMPLETE") for name in discovered])
    rig.hooks.cdc_teardown_on_reset("delete")
    stacks, jobs = [STACK, *discovered], list(rig.jm.works)
    assert rig.state.cdc_teardown_queue == list(zip(jobs, stacks))

    def running(index: int) -> dict:
        return {
            "state": "running", "kind": "delete", "stack": stacks[index - 1],
            "index": index, "total": 4,
        }

    rig.jm.run(jobs[1])  # the second stack finishes first
    assert rig.hooks.cdc_teardown_banner() == running(1)  # the tracked one is still deleting
    rig.jm.run(jobs[0])
    # Past the finished second stack, to the third: the first still deleting, not the last.
    assert rig.hooks.cdc_teardown_banner() == running(3)
    assert (rig.state.cdc_teardown_job_id, rig.state.cdc_teardown_stack) == (
        jobs[2], stacks[2],
    )
    assert rig.hooks.cdc_teardown_done() is None
    # The last two finish between two reads: the next read reports all four finished.
    rig.jm.run(jobs[2])
    rig.jm.run(jobs[3])
    assert rig.hooks.cdc_teardown_banner() is None
    assert rig.hooks.cdc_teardown_done() == {"kind": "delete", "stacks": stacks}
    assert rig.state.cdc_teardown_job_id is None


def test_the_banner_records_the_finished_teardown_before_it_clears_the_marker(
    monkeypatch,
) -> None:
    rig = _start_over_two_stacks(monkeypatch)
    rig.jm.run(rig.first)
    rig.jm.run(rig.second)

    assert rig.hooks.cdc_teardown_banner() is None
    # The notice names every stack and the kind, which it reads from the marker and the
    # queue: recorded after the clear, it would say neither.
    assert rig.hooks.cdc_teardown_done() == {"kind": "delete", "stacks": [STACK, OTHER]}
    assert rig.state.cdc_teardown_job_id is None
    assert rig.state.cdc_teardown_queue == []
    # The notice's close button.
    rig.hooks.cdc_teardown_done_dismiss()
    assert rig.hooks.cdc_teardown_done() is None


def test_start_over_delete_submits_through_the_cdc_steps_submit_with_the_secret_cleanup(
    monkeypatch,
) -> None:
    submit, delete = start_over_delete(monkeypatch)
    assert (submit["mode"], submit["stack_name"]) == ("delete", STACK)
    # Password authentication: the tool created the source-credentials secret.
    assert submit["cleanup_secret"] is True
    assert delete["cleanup_source_secret"] is True


def test_start_over_delete_keeps_a_secrets_manager_sources_secret(monkeypatch) -> None:
    submit, delete = start_over_delete(
        monkeypatch,
        source_secret_id="arn:aws:secretsmanager:ap-northeast-2:111122223333:secret:src",
    )
    assert submit["cleanup_secret"] is False
    assert delete["cleanup_source_secret"] is False


def test_a_hook_reads_its_sessions_state_when_it_is_called(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_teardown import build_cdc_teardown_hooks
    from dsql_migrator.ui.data_migration._state import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore

    sessions, migrations = SessionStore(), DataMigrationStore()
    hooks = build_cdc_teardown_hooks(
        session_id=SESSION, session_store=sessions, data_migration_store=migrations,
        job_manager=_JM(),
    )
    # Building creates no state: the page is built for every connecting browser.
    assert sessions.get(SESSION) is None and migrations.get(SESSION) is None
    assert hooks.cdc_deployed() is False
    state = migrations.get(SESSION)
    assert state is not None
    state.set_cdc_other_stacks([(OTHER, "UPDATE_COMPLETE")])
    assert hooks.cdc_deployed() is True
    assert hooks.cdc_teardown_stack_names() == [OTHER]


def test_the_page_hands_the_sidebar_each_hook_under_its_kwarg(page) -> None:
    """``build_page`` unpacks the hooks by position, so each kwarg is checked by name."""
    from dsql_migrator.ui.data_migration._cdc_teardown import CdcTeardownHooks

    assert sorted(_SIDEBAR_HOOKS.values()) == sorted(CdcTeardownHooks._fields)
    for kwarg, field in _SIDEBAR_HOOKS.items():
        assert page.hooks[kwarg].__qualname__ == (
            f"build_cdc_teardown_hooks.<locals>._{field}"
        ), kwarg
    # Built for this page's session, over the app's stores and job manager.
    page.state.set_cdc_teardown_done(kind="stop", stacks=[STACK])
    assert page.hooks["cdc_teardown_done_getter"]() == {"kind": "stop", "stacks": [STACK]}
    page.hooks["on_reset_cdc"]("stop")  # the session's region, its stack, the app's jobs
    (job_id,) = page.jm.works
    page.jm.run(job_id)
    assert [kwargs["stack_name"] for kwargs in page.calls["stop"]] == [STACK]
