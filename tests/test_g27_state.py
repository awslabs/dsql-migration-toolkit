# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G27 item 1: the finished-teardown notice does not outlive a new deploy of the same stack.

Live Seoul E2E, 2026-10-14 (MySQL): Delete CDC infrastructure on dsql-cdc-e2e26 finished, then
Redeploy and Start CDC ran on the SAME stack name. The top banner still said "CDC infrastructure
deleted -- Teardown of the cdc-stack (dsql-cdc-e2e26) finished -- MSK / NAT billing has stopped"
while MSK was billing and CDC was streaming. The notice (``DataMigrationState.cdc_teardown_done``)
was cleared only by its close button or by Start over.

Now a deploy (``kind="infra"``) or a Start (``kind="start"``) of a stack the notice names takes
that stack out of the notice, and the notice goes once no stack is left. Every deploy and Start
path goes through ``set_cdc_deploy_job_id``. The banner records the notice the next time it reads
the settled job, so a stack deployed or started since its teardown was submitted is also left out
of a notice recorded later. A new teardown of the stack (``set_cdc_teardown`` /
``set_cdc_teardown_queue``, the only sources of the stacks a notice names) makes its result
reported again. A notice about another stack stays. The in-flight teardown marker and queue are
never touched, and no persisted field is added.

Each test FAILS on the code before this change, except the three that pin what stays
(``test_deploying_an_unrelated_stack_keeps_the_notice``,
``test_a_teardown_submitted_after_a_deploy_or_start_is_reported`` and
``test_teardown_kinds_and_a_cleared_job_link_keep_the_notice``). Fakes only: no AWS.
"""

from __future__ import annotations

import itertools

from dsql_migrator.ui.data_migration._cdc_status import finished_teardown_stacks
from dsql_migrator.ui.data_migration._state import DataMigrationState, DataMigrationStore
from dsql_migrator.ui.start_over import _render_cdc_teardown_banner
from tests.test_workflow import _BannerUi

# Neither name contains the other, so "not in" checks on the banner text are meaningful.
A = "dsql-cdc-e2e26"
B = "dsql-cdc-pg26"
DELETED = "CDC infrastructure deleted"
REMOVED = "CDC connectors removed"
CTX = {"region": "ap-northeast-2", "cleanup_secret": False}
_JOB_IDS = itertools.count(1)


def _state(stack: str = A) -> DataMigrationState:
    state = DataMigrationState()
    assert state.set_cdc_stack_name(stack)
    return state


def _teardown_on_the_cdc_step(state: DataMigrationState, kind: str) -> None:
    """Stop CDC or Delete CDC infrastructure on the CDC step (``_cdc_ui._start_cdc_stop`` /
    ``_start_cdc_delete``)."""
    job_id = f"j-{kind}-{next(_JOB_IDS)}"
    state.set_cdc_deploy_job_id(job_id, kind=kind)
    state.set_cdc_teardown(job_id, kind=kind, stack=state.cdc_stack_name, ctx=CTX)


def _settled(state: DataMigrationState) -> None:
    """What app.py's banner getter does when the tracked teardown finished cleanly."""
    state.set_cdc_teardown_done(
        kind=state.cdc_teardown_kind,
        stacks=finished_teardown_stacks(state.cdc_teardown_queue, state.cdc_teardown_stack),
    )
    state.clear_cdc_teardown()


def _banner_text(state: DataMigrationState) -> str:
    """The cross-view banner with nothing in flight, reading the notice as app.py does."""
    ui = _BannerUi()
    _render_cdc_teardown_banner(
        ui,
        lambda: None,
        done_getter=lambda: dict(state.cdc_teardown_done or {}) or None,
        on_done_dismiss=state.dismiss_cdc_teardown_done,
    )
    return " ".join(ui.texts)


def test_the_live_sequence_deploy_start_delete_redeploy() -> None:
    """Deploy, Start, Delete, then Redeploy and Start CDC on the same stack, then Delete again."""
    state = _state(A)
    state.set_cdc_deploy_job_id("j-infra-1", kind="infra")
    state.set_cdc_deploy_job_id("j-start-1", kind="start")
    _teardown_on_the_cdc_step(state, "delete")
    _settled(state)
    text = _banner_text(state)
    assert DELETED in text and f"({A})" in text and "billing has stopped" in text

    # Opening the deploy form ("Redeploy CDC infrastructure") does not deploy anything yet.
    state.set_cdc_redeploy_confirmed(True)
    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [A]}

    state.set_cdc_deploy_job_id("j-infra-2", kind="infra")
    assert state.cdc_teardown_done == {}
    assert _banner_text(state) == ""
    # The Start CDC that follows does not bring it back.
    state.set_cdc_deploy_job_id("j-start-2", kind="start")
    assert _banner_text(state) == ""

    # The next teardown of the same stack is reported again.
    _teardown_on_the_cdc_step(state, "delete")
    _settled(state)
    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [A]}
    assert DELETED in _banner_text(state)


def test_deploying_an_unrelated_stack_keeps_the_notice() -> None:
    """Delete A, then deploy and start B: A's teardown result still holds."""
    state = _state(A)
    _teardown_on_the_cdc_step(state, "delete")
    _settled(state)

    assert state.set_cdc_stack_name(B)
    state.set_cdc_deploy_job_id("j-infra", kind="infra")
    state.set_cdc_deploy_job_id("j-start", kind="start")

    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [A]}
    text = _banner_text(state)
    assert DELETED in text and f"({A})" in text


def test_a_multi_stack_notice_loses_only_the_redeployed_stack() -> None:
    """Start over deleted A and B. Deploying A again leaves B's result; deploying B clears it."""
    state = _state(A)
    state.set_cdc_teardown_done(kind="delete", stacks=[A, B])
    assert f"({A}, {B})" in _banner_text(state)

    state.set_cdc_deploy_job_id("j-infra-a", kind="infra")
    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [B]}
    text = _banner_text(state)
    assert f"Teardown of the cdc-stack ({B}) finished" in text
    assert A not in text

    assert state.set_cdc_stack_name(B)
    state.set_cdc_deploy_job_id("j-infra-b", kind="infra")
    assert state.cdc_teardown_done == {}
    assert _banner_text(state) == ""


def test_starting_the_stopped_stack_clears_the_connectors_removed_notice() -> None:
    """"The CDC connectors on A are gone and streaming has stopped" is false once A starts again."""
    state = _state(A)
    _teardown_on_the_cdc_step(state, "stop")
    _settled(state)
    assert REMOVED in _banner_text(state)

    state.set_cdc_deploy_job_id("j-start", kind="start")
    assert state.cdc_teardown_done == {}
    assert _banner_text(state) == ""


def test_a_deploy_never_touches_an_in_flight_teardown() -> None:
    """Only the finished notice changes: the marker and the queue still track what is running."""
    state = _state(A)
    state.set_cdc_teardown_done(kind="delete", stacks=[A])  # an earlier teardown's result
    state.set_cdc_teardown("j-del-b", kind="delete", stack=B, ctx=CTX)
    state.set_cdc_teardown_queue([("j-del-b", B)])

    state.set_cdc_deploy_job_id("j-infra", kind="infra")

    assert state.cdc_teardown_done == {}
    assert state.cdc_teardown_job_id == "j-del-b"
    assert state.cdc_teardown_kind == "delete"
    assert state.cdc_teardown_stack == B
    assert state.cdc_teardown_ctx == CTX
    assert state.cdc_teardown_queue == [("j-del-b", B)]


def test_a_notice_recorded_after_the_deploy_started_leaves_that_stack_out() -> None:
    """The banner records the notice only when it next reads the settled job.

    Start over deleted A and B. A finished first and the new journey deployed it again while
    B was still deleting. When B settled, the notice was recorded for the whole teardown and
    named A as deleted, with billing stopped, while A was being deployed.
    """
    store = DataMigrationStore()
    state = store.get_or_create("S1")
    state.set_cdc_teardown("j-a", kind="delete", stack=A, ctx=CTX)
    state.set_cdc_teardown_queue([("j-a", A), ("j-b", B)])
    store.reset_in_place("S1")  # Start over submits the teardown, then resets

    assert state.set_cdc_stack_name(A)
    state.set_cdc_deploy_job_id("j-infra", kind="infra")
    # Not cleared: the banner still tracks the running teardown.
    assert state.cdc_teardown_job_id == "j-a"
    assert state.cdc_teardown_queue == [("j-a", A), ("j-b", B)]

    _settled(state)  # B settles

    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [B]}
    text = _banner_text(state)
    assert f"({B})" in text and A not in text


def test_a_teardown_submitted_after_a_deploy_or_start_is_reported() -> None:
    """Only a deploy or Start made AFTER a stack's latest teardown leaves it out of the notice.

    The banner's Retry cleanup re-launches a failed teardown without touching the lifecycle job
    link, so the link can still name a Start made before it (a failed Stop that left one
    connector offers Retry CDC). The notice reports the newer teardown, so it names the stack.
    """
    state = _state(A)
    _teardown_on_the_cdc_step(state, "stop")  # it failed and left one connector
    state.set_cdc_deploy_job_id("j-retry-cdc", kind="start")  # Retry CDC
    state.clear_cdc_teardown()  # Retry cleanup (app.py _cdc_teardown_retry) ...
    state.set_cdc_teardown("j-retry-stop", kind="stop", stack=A, ctx=CTX)  # ... re-launches it
    _settled(state)
    assert state.cdc_teardown_done == {"kind": "stop", "stacks": [A]}
    assert REMOVED in _banner_text(state)

    # A multi-stack teardown launched after both stacks were deployed reports both.
    state = _state(A)
    state.set_cdc_deploy_job_id("j-infra-a", kind="infra")
    assert state.set_cdc_stack_name(B)
    state.set_cdc_deploy_job_id("j-infra-b", kind="infra")
    state.set_cdc_teardown("j-a", kind="delete", stack=A, ctx=CTX)
    state.set_cdc_teardown_queue([("j-a", A), ("j-b", B)])
    _settled(state)
    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [A, B]}


def test_teardown_kinds_and_a_cleared_job_link_keep_the_notice() -> None:
    """Only a deploy or Start drops a stack, and only with a job linked."""
    state = _state(A)
    _teardown_on_the_cdc_step(state, "delete")
    _settled(state)

    for kind in ("stop", "delete"):
        state.set_cdc_deploy_job_id(f"j-{kind}", kind=kind)
        assert state.cdc_teardown_done == {"kind": "delete", "stacks": [A]}, kind
    state.set_cdc_deploy_job_id(None, kind="infra")  # clearing the link is not a deploy
    assert state.cdc_teardown_done == {"kind": "delete", "stacks": [A]}
    assert DELETED in _banner_text(state)
