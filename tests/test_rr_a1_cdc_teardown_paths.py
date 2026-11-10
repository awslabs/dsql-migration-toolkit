# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A1: Start over and Retry cleanup tear CDC down the way Stop / Delete on the CDC step do.

Start over and the cross-view banner's Retry cleanup reached the CDC teardown through their
own copy of the submit sequence in ``ui/app.py`` (``_launch_cdc_teardown``). That copy fell
behind the CDC step's ``_start_cdc_delete``: it called ``run_cdc_delete`` without the
session's source credentials, without the residue and source-write callbacks, and submitted
the job without ``_logged_cdc_lifecycle``. For a PostgreSQL source on Fargate the app's
identity cannot read the tool-managed CDC secret (no ``secretsmanager:GetSecretValue``), so
the Start over teardown could not drop the replication slot -- which keeps pinning source
WAL -- and nothing durable recorded that it was left, that the source was written, or how
the teardown ended.

Both paths now go through one ``_cdc_ui.submit_cdc_teardown``. These tests drive the REAL
hooks ``build_page`` hands ``build_workflow_sidebar`` (``_cdc_teardown.py``) with a fake NiceGUI,
a job manager that only records the submitted work, and stubbed deployer functions: no AWS.
The order is the real one -- Start over's teardown is submitted, then the session is reset,
then the job body runs -- so credentials read lazily inside the job would come back empty.
"""

from __future__ import annotations

import sys
import types
from types import SimpleNamespace

import pytest

from dsql_migrator.config import SecretValue
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import (
    SourceConnectionConfig,
    SourceType,
    TargetConnectionConfig,
)

USER = "pgadmin"
# Not a real credential: a sentinel the tests search for wherever it must NOT appear.
PW = "rr-a1-sentinel-not-a-password"
STACK = "dsql-cdc-rra1"
REGION = "ap-northeast-2"
SLOT_LEFT = (
    "the PostgreSQL logical replication slot 'dsql_cdc_rra1' is STILL ON THE SOURCE and "
    "keeps pinning WAL -- drop it manually"
)
SOURCE_WRITE = "Dropping the PostgreSQL replication slot 'dsql_cdc_rra1' on the source"


class _JM:
    """Records submitted work without running it; ``run`` runs one on demand."""

    def __init__(self) -> None:
        self.works: dict = {}

    def submit(self, work, **_k):  # noqa: ANN001
        job_id = f"rr-a1-job-{len(self.works) + 1}"
        self.works[job_id] = work
        return job_id

    def get_status(self, job_id):  # noqa: ANN001
        if job_id not in self.works:
            raise JobNotFoundError(job_id)
        return SimpleNamespace(job_id=job_id, status="PENDING")

    def request_cancel(self, _job_id) -> None:  # noqa: ANN001
        return None

    def run(self, job_id) -> None:  # noqa: ANN001
        self.works[job_id](SimpleNamespace(cancelled=False))


class _Ui:
    def notify(self, *_a, **_k) -> None:
        return None


def _connect(session) -> None:
    """A PostgreSQL source the operator connected with a password, plus a DSQL target."""
    session.set_source(
        SourceConnectionConfig(
            source_type=SourceType.POSTGRES,
            host="source.example.internal",
            port=5432,
            database="app",
            username=USER,
        ),
        SecretValue(PW),
    )
    session.set_target(
        TargetConnectionConfig(
            cluster_endpoint=f"abc.dsql.{REGION}.on.aws", region=REGION
        )
    )


def _stub_teardown(monkeypatch, residue: list) -> dict:
    """Stub the core deployer functions; return the kwargs each was called with.

    The delete stub behaves like the real one for the two callbacks: it reports the source
    write it made and, when ``residue`` is non-empty, what it could not remove.
    """
    import dsql_migrator.core.cdc_deployer as dep

    calls: dict = {"delete": [], "stop": []}

    def _delete(_handle, **kwargs) -> None:  # noqa: ANN001
        calls["delete"].append(kwargs)
        if kwargs.get("on_source_write") is not None:
            kwargs["on_source_write"](SOURCE_WRITE)
        for line in residue:
            if kwargs.get("on_residue") is not None:
                kwargs["on_residue"](line)

    def _stop(_handle, **kwargs) -> None:  # noqa: ANN001
        calls["stop"].append(kwargs)

    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *a, **k: object())
    monkeypatch.setattr(dep, "run_cdc_delete", _delete)
    monkeypatch.setattr(dep, "run_cdc_stop", _stop)
    return calls


def _capture_events(monkeypatch) -> list:
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.core.activity_log import ActivityStatus

    events: list = []

    # The real anchor's default status is STARTED (the submit-site line).
    def _fake(action, *, detail=None, status=ActivityStatus.STARTED, exc=None):  # noqa: ANN001
        events.append((action, getattr(status, "value", status), detail))

    monkeypatch.setattr(cdc_ui, "_log_cdc_event", _fake)
    return events


@pytest.fixture
def page(monkeypatch, request):
    """The real page's teardown hooks, for a session with a live PostgreSQL CDC stack."""
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
    residue: list = []
    calls = _stub_teardown(monkeypatch, residue)
    events = _capture_events(monkeypatch)

    session_id = f"rr-a1-{request.node.name}"
    app_module.build_page(app_module.AppConfig(), session_id)
    session = app_module.SESSION_STORE.get_or_create(session_id)
    _connect(session)
    state = app_module.DATA_MIGRATION_STORE.get_or_create(session_id)
    assert state.set_cdc_stack_name(STACK)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")
    try:
        yield SimpleNamespace(
            hooks=hooks, jm=jm, calls=calls, events=events, residue=residue,
            session=session, state=state,
        )
    finally:
        for store in ("SESSION_STORE", "DATA_MIGRATION_STORE"):
            getattr(app_module, store).clear(session_id)


def _start_over(page, mode: str) -> str:
    """Start over with the given CDC choice, in the dialog's order; return the job id."""
    page.hooks["on_reset_cdc"](mode)
    page.hooks["on_reset"]()
    # The reset wiped the session's password, so a lookup made when the job runs gets None.
    assert page.session.source_password is None
    (job_id,) = page.jm.works
    return job_id


def _teardown_fields(state) -> str:
    return repr(
        (
            state.cdc_teardown_job_id,
            state.cdc_teardown_kind,
            state.cdc_teardown_stack,
            state.cdc_teardown_ctx,
            state.cdc_teardown_queue,
        )
    )


# -- Start over -------------------------------------------------------------------------------


def test_start_over_delete_drops_the_slot_with_credentials_captured_before_the_reset(
    page,
) -> None:
    job_id = _start_over(page, "delete")
    page.jm.run(job_id)

    (kwargs,) = page.calls["delete"]
    assert kwargs.get("source_credentials") == (USER, PW), (
        "the PostgreSQL slot drop needs the session's credentials: the app's identity "
        "cannot read the CDC secret"
    )
    assert callable(kwargs.get("on_residue")), "a surviving slot would go unrecorded"
    assert callable(kwargs.get("on_source_write")), "the source write would go unaudited"
    # What the Start-over copy already did is unchanged.
    assert kwargs["stack_name"] == STACK
    assert kwargs["region"] == REGION
    assert kwargs["cleanup_source_secret"] is True  # password auth: the tool's secret


def test_start_over_teardown_logs_its_outcome_source_writes_and_residue(page) -> None:
    page.residue.append(SLOT_LEFT)
    job_id = _start_over(page, "delete")
    page.jm.run(job_id)

    writes = [e for e in page.events if e[0] == "CDC source replication teardown"]
    assert writes, f"the source write is not in the activity log: {page.events}"
    assert writes[0][1] == "info" and SOURCE_WRITE in writes[0][2]
    outcomes = [e for e in page.events if e[0] == "delete CDC infrastructure"]
    assert outcomes, f"no outcome line for the Start-over teardown: {page.events}"
    _action, status, detail = outcomes[-1]
    assert status == "warning", (status, detail)
    assert "LEFT BEHIND" in detail and "STILL ON THE SOURCE" in detail, detail
    assert f"stack {STACK}" in detail, detail


def test_start_over_stop_runs_under_the_lifecycle_logger(page) -> None:
    job_id = _start_over(page, "stop")
    page.jm.run(job_id)

    (kwargs,) = page.calls["stop"]
    assert kwargs["stack_name"] == STACK
    assert page.calls["delete"] == []
    outcomes = [e for e in page.events if e[0] == "stop CDC connectors"]
    assert outcomes and outcomes[-1][1] == "success", page.events
    assert page.state.cdc_teardown_kind == "stop"
    assert page.state.cdc_teardown_ctx["cleanup_secret"] is False


OTHER = "dsql-cdc-othersource"


def _creds_by_stack(page) -> dict:
    """Run every submitted job; return each torn-down stack's ``source_credentials``."""
    for job_id in list(page.jm.works):
        page.jm.run(job_id)
    return {k["stack_name"]: k.get("source_credentials") for k in page.calls["delete"]}


def test_start_over_hands_the_credentials_only_to_the_sessions_own_stack(page) -> None:
    """A discovered stack may capture a different source: it keeps its own secret path.

    ``cdc_other_stacks`` comes from an account-wide listing, so it can be another
    migration's pipeline on another database. The slot drop tries the session's
    credentials first against THAT stack's ``SourceDbHostname`` and does not fall back to
    the secret when they fail, so handing them over sent the operator's password to a
    server it was never entered for and left that source's slot pinning WAL.
    """
    page.state.set_cdc_other_stacks([(OTHER, "UPDATE_COMPLETE")])
    page.hooks["on_reset_cdc"]("delete")
    page.hooks["on_reset"]()

    assert _creds_by_stack(page) == {STACK: (USER, PW), OTHER: None}


def test_start_over_with_only_a_discovered_stack_passes_no_credentials(page) -> None:
    page.state.set_cdc_stack_phase("absent", status=None)
    page.state.set_cdc_other_stacks([(OTHER, "UPDATE_COMPLETE")])
    page.hooks["on_reset_cdc"]("delete")
    page.hooks["on_reset"]()

    assert _creds_by_stack(page) == {OTHER: None}
    # The banner follows the discovered stack, so a later Retry cleanup targets it too.
    assert page.state.cdc_teardown_stack == OTHER


# -- Retry cleanup ----------------------------------------------------------------------------


def test_retry_cleanup_passes_the_live_sessions_credentials(page) -> None:
    """A failed CDC-step Delete keeps the session; Retry cleanup must use its credentials."""
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    # The real CDC-step Delete writes the marker (its job is left unrun: it failed).
    cdc_ui._start_cdc_delete(_Ui(), page.state, page.jm, lambda: None, session=page.session)
    (failed,) = page.jm.works
    page.hooks["cdc_teardown_retry"]()
    (job_id,) = [job for job in page.jm.works if job != failed]
    page.jm.run(job_id)

    (kwargs,) = page.calls["delete"]
    assert kwargs.get("source_credentials") == (USER, PW)
    assert callable(kwargs.get("on_residue")) and callable(kwargs.get("on_source_write"))
    assert kwargs["cleanup_source_secret"] is True
    outcomes = [e for e in page.events if e[0] == "delete CDC infrastructure"]
    assert outcomes and outcomes[-1][1] == "success", page.events
    assert page.state.cdc_teardown_job_id == job_id


def test_retry_cleanup_after_start_over_falls_back_to_the_secret(page) -> None:
    """After Start over wiped the session there is no password; the secret path stays."""
    first = _start_over(page, "delete")
    page.hooks["cdc_teardown_retry"]()
    (retry,) = [job for job in page.jm.works if job != first]
    page.jm.run(retry)

    (kwargs,) = page.calls["delete"]
    assert kwargs.get("source_credentials") is None
    assert callable(kwargs.get("on_residue")) and callable(kwargs.get("on_source_write"))
    assert [e for e in page.events if e[0] == "delete CDC infrastructure"], page.events


def test_retry_cleanup_of_another_stack_passes_no_credentials(page) -> None:
    """The marker can name a discovered stack: its ctx records no source, so Retry gives it
    None while the session is connected to its own source.

    submit_cdc_teardown records ``ctx["source"]`` for the session's own stack only, and the
    marker drops it when it moves on to another stack (tests/test_rr_int_teardown_retry_
    source.py). A recorded source is what lets a retry skip the stack-name check (a renamed
    own stack after Start over), so a marker naming another stack must not carry one.
    """
    page.state.set_cdc_teardown(
        "rr-a1-failed-delete",
        kind="delete",
        stack=OTHER,
        ctx={
            "region": REGION, "role_arn": None, "profile": None, "cleanup_secret": False,
            "source": None,
        },
    )
    page.hooks["cdc_teardown_retry"]()

    assert _creds_by_stack(page) == {OTHER: None}


# -- Retry cleanup after the session moved to another source ---------------------------------
#
# The stack-name check alone does not stop the session's credentials from reaching another
# source: Start over resets ``cdc_stack_name`` to the default name and keeps the marker, so
# a failed teardown of a DEFAULT-named stack still passes it after the operator reconnects
# to a different source -- and a reconnect without Start over keeps the name as it was. The
# slot drop tries the credentials it is handed first, against the stack's own source host,
# and does not fall back to the secret: the new source's password would reach the old
# server and the old slot would stay. Retry cleanup now passes the live session's
# credentials only while the session still targets the source the teardown was submitted
# for (recorded, non-secret, as the marker's ``ctx["source"]``).

OTHER_PW = "rr-a1-other-sentinel-not-a-password"


def _reconnect(session, *, host: str, user: str, password: str) -> None:
    session.set_source(
        SourceConnectionConfig(
            source_type=SourceType.POSTGRES,
            host=host,
            port=5432,
            database="app",
            username=user,
        ),
        SecretValue(password),
    )


def _retry(page) -> dict:
    """Click Retry cleanup and run the job it submitted; return run_cdc_delete's kwargs."""
    before = set(page.jm.works)
    page.hooks["cdc_teardown_retry"]()
    (job_id,) = [job for job in page.jm.works if job not in before]
    seen = len(page.calls["delete"])
    page.jm.run(job_id)
    (kwargs,) = page.calls["delete"][seen:]
    return kwargs


def _on_the_default_stack(page) -> None:
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME

    assert page.state.set_cdc_stack_name(CDC_DEFAULT_STACK_NAME)
    page.state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")


def test_retry_after_start_over_and_a_reconnect_to_another_source_passes_no_credentials(
    page,
) -> None:
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME

    _on_the_default_stack(page)
    _start_over(page, "delete")  # left unrun: the stack delete failed
    assert page.state.cdc_stack_name == CDC_DEFAULT_STACK_NAME
    assert page.state.cdc_teardown_stack == CDC_DEFAULT_STACK_NAME
    _reconnect(
        page.session, host="other-source-b.example.internal", user="bob", password=OTHER_PW
    )

    kwargs = _retry(page)
    assert kwargs["stack_name"] == CDC_DEFAULT_STACK_NAME
    assert kwargs.get("source_credentials") is None, (
        "source B's password would be sent to the torn-down stack's source A"
    )
    assert OTHER_PW not in _teardown_fields(page.state)


def test_retry_after_a_cdc_step_delete_and_a_reconnect_to_another_source_passes_no_credentials(
    page,
) -> None:
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    cdc_ui._start_cdc_delete(_Ui(), page.state, page.jm, lambda: None, session=page.session)
    _reconnect(
        page.session, host="other-source-b.example.internal", user="bob", password=OTHER_PW
    )

    kwargs = _retry(page)
    assert kwargs["stack_name"] == STACK
    assert kwargs.get("source_credentials") is None


def test_retry_after_start_over_and_a_reconnect_to_the_same_source_uses_its_credentials(
    page,
) -> None:
    _on_the_default_stack(page)
    _start_over(page, "delete")
    # Wiped by the reset: the first retry has nothing to pass and reads the secret ...
    assert _retry(page).get("source_credentials") is None
    # ... and once the operator reconnects to the SAME source, the next retry uses them.
    _reconnect(page.session, host="source.example.internal", user=USER, password=PW)
    assert _retry(page).get("source_credentials") == (USER, PW)


def test_retry_of_a_discovered_default_named_stack_after_start_over_passes_no_credentials(
    page,
) -> None:
    """Start over resets ``cdc_stack_name`` to the default name, so a DISCOVERED stack that
    has it passes the stack-name check on a retry: its marker must not record the session's
    source, or reconnecting to that source would hand its password to another source's stack.
    """
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME

    page.state.set_cdc_stack_phase("absent", status=None)
    page.state.set_cdc_other_stacks([(CDC_DEFAULT_STACK_NAME, "UPDATE_COMPLETE")])
    _start_over(page, "delete")
    assert page.state.cdc_teardown_stack == CDC_DEFAULT_STACK_NAME
    assert page.state.cdc_stack_name == CDC_DEFAULT_STACK_NAME
    assert page.state.cdc_teardown_ctx.get("source") is None
    _reconnect(page.session, host="source.example.internal", user=USER, password=PW)

    assert _retry(page).get("source_credentials") is None


# -- Property 7 -------------------------------------------------------------------------------


def test_the_teardown_marker_and_the_activity_log_never_hold_the_password(page) -> None:
    page.residue.append(SLOT_LEFT)
    page.hooks["on_reset_cdc"]("delete")
    assert PW not in _teardown_fields(page.state)
    assert set(page.state.cdc_teardown_ctx) == {
        "region", "role_arn", "profile", "cleanup_secret", "source",
    }
    # The non-secret source the retry checks: host, port, database -- no username either.
    assert page.state.cdc_teardown_ctx["source"] == ("source.example.internal", 5432, "app")
    page.hooks["on_reset"]()
    assert PW not in _teardown_fields(page.state)
    assert PW not in repr(vars(page.state))
    for job_id in list(page.jm.works):
        page.jm.run(job_id)
    page.hooks["cdc_teardown_retry"]()
    assert PW not in _teardown_fields(page.state)
    assert page.events and PW not in repr(page.events)


# -- Stop / Delete on the CDC step: unchanged -------------------------------------------------


def _cdc_step_session():
    from dsql_migrator.ui.session import SessionConnectionState

    session = SessionConnectionState()
    _connect(session)
    return session


def test_the_cdc_step_delete_behaves_as_before(monkeypatch) -> None:
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    calls = _stub_teardown(monkeypatch, [])
    events = _capture_events(monkeypatch)
    state = DataMigrationState()
    assert state.set_cdc_stack_name(STACK)
    state.set_cdc_redeploy_confirmed(True)
    jm = _JM()
    refreshed: list = []

    cdc_ui._start_cdc_delete(
        _Ui(), state, jm, lambda: refreshed.append(1), session=_cdc_step_session()
    )
    (job_id,) = jm.works
    assert (state.cdc_deploy_job_id, state.cdc_action_kind) == (job_id, "delete")
    assert state.cdc_redeploy_confirmed is False
    assert (state.cdc_teardown_job_id, state.cdc_teardown_kind) == (job_id, "delete")
    assert state.cdc_teardown_stack == STACK
    assert state.cdc_teardown_ctx == {
        "region": REGION, "role_arn": None, "profile": None, "cleanup_secret": True,
        "source": ("source.example.internal", 5432, "app"),
    }
    assert events == [("delete CDC infrastructure", "started", f"stack {STACK}")]
    assert refreshed == [1]

    jm.run(job_id)
    (kwargs,) = calls["delete"]
    assert kwargs["source_credentials"] == (USER, PW)
    assert kwargs["on_log"] == state.append_cdc_deploy_log
    assert kwargs["cleanup_source_secret"] is True
    assert callable(kwargs["on_residue"]) and callable(kwargs["on_source_write"])
    assert [e[0] for e in events[1:]] == [
        "CDC source replication teardown",
        "delete CDC infrastructure",
    ]
    assert events[-1][1] == "success"
    assert PW not in repr(events) and PW not in _teardown_fields(state)


def test_the_cdc_step_stop_behaves_as_before(monkeypatch) -> None:
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    calls = _stub_teardown(monkeypatch, [])
    events = _capture_events(monkeypatch)
    state = DataMigrationState()
    assert state.set_cdc_stack_name(STACK)
    state.set_cdc_applied_ops_by_table(
        {"orders": {"inserts": 3, "updates": 1, "deletes": 0}}
    )
    state.set_cdc_deployed_snapshot_mode("recovery")
    jm = _JM()

    cdc_ui._start_cdc_stop(_Ui(), state, jm, lambda: None, session=_cdc_step_session())
    (job_id,) = jm.works
    assert (state.cdc_deploy_job_id, state.cdc_action_kind) == (job_id, "stop")
    assert (state.cdc_teardown_job_id, state.cdc_teardown_kind) == (job_id, "stop")
    assert state.cdc_teardown_ctx == {
        "region": REGION, "role_arn": None, "profile": None, "cleanup_secret": False,
        "source": None,
    }
    assert [e[0] for e in events] == ["CDC applied rows (final)", "stop CDC connectors"]
    assert events[-1][1] == "started"

    jm.run(job_id)
    (kwargs,) = calls["stop"]
    assert kwargs["stack_name"] == STACK
    assert kwargs["on_log"] == state.append_cdc_deploy_log
    assert state.cdc_deployed_snapshot_mode is None
    assert events[-1][:2] == ("stop CDC connectors", "success")
