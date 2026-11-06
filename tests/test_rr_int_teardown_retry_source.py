# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Retry cleanup hands the session's password only to the stack it was entered for.

The teardown marker's ``ctx["source"]`` is the non-secret ``(host, port, database)`` the
session's credentials were entered for, and ``submit_cdc_teardown`` records it only for the
session's OWN stack (``None`` for a discovered one, which may capture another source).
Retry cleanup (``ui/app.py`` ``_cdc_teardown_retry``) hands over the live session's
credentials only while the session still targets that source.

A multi-stack Start over launches every stack behind one marker, and the banner moves the
marker on to the next unfinished stack when the tracked one settles
(``advance_cdc_teardown``). The move kept the FIRST stack's whole ``ctx``, its source
included, so the marker then named a discovered stack with the session's source recorded
beside it. When that stack's delete failed and the operator reconnected to their own
source, Retry cleanup passed both checks (Start over had reset ``cdc_stack_name`` to the
default name the discovered stack carried) and sent the password to that stack's source
host. The slot drop does not fall back to the secret when handed credentials, so that
source's slot stayed too.

These tests drive the REAL ``build_page`` hooks (``on_reset_cdc``, ``on_reset``, the
banner getter, ``cdc_teardown_retry``) with a fake NiceGUI, a job manager that records the
submitted work and its status, and stubbed deployer functions: no AWS, no source.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest

from dsql_migrator.core.job_manager import JobNotFoundError
from tests.test_rr_a1_cdc_teardown_paths import (
    PW,
    USER,
    _capture_events,
    _connect,
    _reconnect,
    _stub_teardown,
)

SOURCE_A = ("source.example.internal", 5432, "app")
OWN = "dsql-cdc-mine"  # the session's own stack, renamed from the default


class _JM:
    """Records submitted work and a status per job; ``run`` runs one job to DONE."""

    def __init__(self) -> None:
        self.works: dict = {}
        self.status: dict = {}

    def submit(self, work, **_k):  # noqa: ANN001
        job_id = f"rr-int-job-{len(self.works) + 1}"
        self.works[job_id] = work
        self.status[job_id] = "PENDING"
        return job_id

    def get_status(self, job_id):  # noqa: ANN001
        if job_id not in self.works:
            raise JobNotFoundError(job_id)
        return SimpleNamespace(job_id=job_id, status=self.status[job_id])

    def get_error(self, _job_id):  # noqa: ANN001
        return None

    def request_cancel(self, _job_id) -> None:  # noqa: ANN001
        return None

    def run(self, job_id) -> None:  # noqa: ANN001
        self.works[job_id](SimpleNamespace(cancelled=False))
        self.status[job_id] = "DONE"


@pytest.fixture
def page(monkeypatch, request):
    """The real page's teardown hooks: a PostgreSQL session on its own, renamed stack."""
    from tests.test_ai_panel import _Ui as _PageUi

    fake = types.ModuleType("nicegui")
    fake.ui = _PageUi()  # type: ignore[attr-defined]
    fake.run = SimpleNamespace(io_bound=None, cpu_bound=None)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "nicegui", fake)

    from dsql_migrator.ui import app as app_module

    hooks: dict = {}

    def _spy_sidebar(*_a, **kwargs):  # noqa: ANN002
        hooks.update(kwargs)
        return (None, lambda *_a, **_k: None)

    jm = _JM()
    monkeypatch.setattr(app_module, "build_workflow_sidebar", _spy_sidebar)
    monkeypatch.setattr(app_module, "JOB_MANAGER", jm)
    calls = _stub_teardown(monkeypatch, [])
    _capture_events(monkeypatch)

    session_id = f"rr-int-{request.node.name}"
    app_module.build_page(app_module.AppConfig(), session_id)
    session = app_module.SESSION_STORE.get_or_create(session_id)
    _connect(session)
    state = app_module.DATA_MIGRATION_STORE.get_or_create(session_id)
    assert state.set_cdc_stack_name(OWN)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    try:
        yield SimpleNamespace(hooks=hooks, jm=jm, calls=calls, session=session, state=state)
    finally:
        for store in ("SESSION_STORE", "DATA_MIGRATION_STORE"):
            getattr(app_module, store).clear(session_id)


def _retry(page) -> dict:
    """Click Retry cleanup and run the job it submitted; return run_cdc_delete's kwargs."""
    before = set(page.jm.works)
    page.hooks["cdc_teardown_retry"]()
    (job_id,) = [job for job in page.jm.works if job not in before]
    seen = len(page.calls["delete"])
    page.jm.run(job_id)
    (kwargs,) = page.calls["delete"][seen:]
    return kwargs


def test_a_retry_of_the_stack_the_banner_moved_on_to_passes_no_credentials(page) -> None:
    """Own renamed stack + a discovered DEFAULT-named stack (which may capture another
    source): the own stack finishes first, the banner moves to the discovered one, its
    delete fails, the operator reconnects to their own source and clicks Retry cleanup."""
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME

    page.state.set_cdc_other_stacks([(CDC_DEFAULT_STACK_NAME, "UPDATE_COMPLETE")])
    page.hooks["on_reset_cdc"]("delete")
    page.hooks["on_reset"]()
    own_job, other_job = list(page.jm.works)
    assert page.state.cdc_teardown_stack == OWN
    assert page.state.cdc_teardown_ctx["source"] == SOURCE_A

    page.jm.run(own_job)
    info = page.hooks["cdc_teardown_banner_getter"]()
    assert (info["state"], info["stack"]) == ("running", CDC_DEFAULT_STACK_NAME)
    # The recorded source belonged to the own stack; it does not follow the marker.
    assert page.state.cdc_teardown_ctx.get("source") is None
    # The rest of the retry context is the same for every stack.
    assert page.state.cdc_teardown_ctx["region"] == "ap-northeast-2"

    page.jm.status[other_job] = "FAILED"
    info = page.hooks["cdc_teardown_banner_getter"]()
    assert (info["state"], info["stack"]) == ("failed", CDC_DEFAULT_STACK_NAME)
    _reconnect(page.session, host=SOURCE_A[0], user=USER, password=PW)
    assert page.state.cdc_stack_name == CDC_DEFAULT_STACK_NAME  # Start over reset it

    kwargs = _retry(page)
    assert kwargs["stack_name"] == CDC_DEFAULT_STACK_NAME
    assert kwargs.get("source_credentials") is None, (
        "the own source's password was handed to a discovered stack's source host"
    )


def test_moving_the_marker_to_another_stack_drops_only_the_recorded_source() -> None:
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    state = DataMigrationState()
    ctx = {"region": "r", "role_arn": None, "profile": None, "cleanup_secret": True,
           "source": SOURCE_A}
    state.set_cdc_teardown("job-1", kind="delete", stack=OWN, ctx=ctx)
    state.advance_cdc_teardown("job-2", "dsql-cdc-other")
    assert (state.cdc_teardown_job_id, state.cdc_teardown_stack) == ("job-2", "dsql-cdc-other")
    assert state.cdc_teardown_kind == "delete"
    assert state.cdc_teardown_ctx == {**ctx, "source": None}
