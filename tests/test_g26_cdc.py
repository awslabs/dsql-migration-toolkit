# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G26 item 1, package CDC-1: the CDC only route of Start CDC's newer-start-point refusal (decision
A2) and of the unknown-offer refusal names the Drop & reload condition.

The route was "Delete CDC infrastructure and deploy it again, then Start CDC on the new stack".
That Start seeds the new stack from the latest Full Load's watermark. If that Full Load appended to
rows already on the target, which the Re-run dialog does by default with Append, those rows stay as
they were: CDC from a watermark carries only the changes after it. An update made on the source
before that point stays stale, a delete stays an extra row, and a ROW_COUNT validation can still
match. The scenario: MySQL, Full load + CDC, streamed from W1, Stop CDC, a Full load only Re-run
that appended (W2), then CDC only. The manual states the condition (03 section 3.5, the 04 section
4.3 note, 10.1): after a Full load only re-run that appended to rows already on the target, run it
again choosing Drop & reload before that Start. Every surface of the refusal now says so: the
start-point card, the Start CDC card and its disabled Start's tooltip, the dialog and its tooltip,
the click, and the Start CDC job's first line (300 characters at most).

The job record keeps no load mode. ``MigrationJob`` has no such field, and the run's "load mode:
APPEND / REPLACE" goes only to the activity log. A chunk's rows "already there" also counts the
rows that a source-drop re-read or a Retry wrote in the same run. So the condition is stated, not
inferred. A Manual start point (the new stack starts from that point) and Full load + CDC (whose
route already re-runs the Full Load with Drop & reload) keep their routes.

Each test FAILS on the code before this change, except the one that pins what stays
(``test_a_manual_point_and_full_load_and_cdc_keep_their_routes``). Fakes only: no AWS.

G26 item 2, package CDC-2 (the second part below): after an app restart, a MySQL first Start that
CloudFormation rolled back can be retried from its own start point again. The rollback restored
the stack's parameters, and the app kept the offered start offset only in memory, so after a
restart every seed was refused (the retry too, under a header that called that same point
newer). The decision: persist nothing; read the offer back from the stack's OWN record. The
rolled-back update's stack events carry the offset seeder custom resource's
``ResourceProperties`` (``CdcStartPrepResource``: ``WatermarkBinlogFile`` /
``WatermarkBinlogPos``). The stack-phase probe reads them read-only, only for an
``UPDATE_ROLLBACK_*`` status, from that update's forward phase (the rollback phase carries the
restored properties), with bounded pagination that stops at the stack-level
``UPDATE_IN_PROGRESS``. It records the offer as this process records its own, so the same seed
is allowed (a gapless retry), a later one refused (A2) and an earlier one refused, exactly as
before a restart. It fails closed as before when the events cannot be read, the Start seeded in
the app (SeedMode=External: no custom resource), or the properties are missing or malformed; that
refusal's header no longer calls the refused point newer.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import MigrationMode, SourceType
from dsql_migrator.ui.cdc_start_point_gate import (
    NEWER_POINT_HEADER,
    OFFER_UNKNOWN_HEADER,
    new_topics_route,
    newer_point_line,
    newer_point_notice,
)
from dsql_migrator.ui.data_migration import MigrationType, _cdc_ui
from tests.test_g25_cdc_r7 import (
    _JM,
    _W0,
    _W1,
    _W2,
    _capture,
    _never_started,
    _point_card,
    _rolled_back,
    _start_card,
    _stopped,
)
from tests.test_g25_cdc_r8 import _NEW_WAY, _OFFER, _RESTORED, _UNKNOWN_LEAD, _restarted

MYSQL = SourceType.MYSQL
CDC_ONLY = MigrationType.CDC_ONLY
FL_CDC = MigrationType.FULL_LOAD_AND_CDC

# The condition, in the notice (card, dialog, click) right after the route's "then Start CDC on the
# new stack." -- the manual's words, with the type switch CDC only needs for a Full Load re-run
# (G147: to Full load + CDC, whose Full Load makes the start point exact).
_RELOAD_FIRST = (
    "That Start begins at the Full Load's watermark, so if that Full Load appended to rows already "
    "on the target, run it again choosing 'Drop & reload' before that Start (CDC only has no Full "
    "Load step: switch the migration type to Full load + CDC for that run, whose Full Load makes "
    "the CDC start point exact). An append keeps those rows as they were, and CDC from a "
    "watermark carries only the changes after it."
)
# The one-line forms: the disabled Start's tooltip, and the job's first line after the prefix.
_ROUTE_LINE = (
    "Delete CDC infrastructure and deploy it again, then Start CDC (if the Full Load appended to "
    "rows already on the target, re-run it choosing 'Drop & reload' first)"
)
_NEWER_LINE = (
    "This CDC stack has streamed, so Start CDC does not move it forward to a newer start point: "
    f"{_ROUTE_LINE}. Nothing was changed."
)
_UNKNOWN_LINE = (
    "This CDC stack's last Start rolled back and the start offset it offered is no longer known: "
    f"{_ROUTE_LINE}. Nothing was changed."
)
# The routes before this change, which the cases that keep theirs still name.
_CDC_ONLY_ROUTE = (
    "To start from a new start point, use Delete CDC infrastructure and deploy it again (the new "
    "stack starts with empty topics), then Start CDC on the new stack."
)
_OLD_CDC_ONLY_LINE = (
    "This CDC stack has streamed, so Start CDC does not move it forward to a newer start point "
    "(its sink would apply older changes over newer rows): Delete CDC infrastructure and deploy it "
    "again, then Start CDC on the new stack. Nothing was changed."
)
_FL_CDC_LINE = (
    "This CDC stack has streamed, so Start CDC does not move it forward to a newer start point "
    "(its sink would apply older changes over newer rows): Delete CDC infrastructure and deploy it "
    "again, then Re-run Full Load choosing 'Drop & reload', then Start CDC. Nothing was changed."
)
_PREFIX = "Start CDC blocked: "


def _detour():
    """The scenario: streamed from W1 and stopped (committed offset, deployed at 000090:4); a Full
    load only Re-run took W2; then CDC only."""
    return _stopped(migration_type=CDC_ONLY)


def _refused(monkeypatch, which):
    """``(state, the refusal's header, its lead or None, its one-line form)``: A2 on the stopped
    stack (``newer``), or the unknown-offer refusal on a stack whose first Start rolled back,
    after an app restart, whose events give no offer (``unknown offer``)."""
    if which == "newer":
        return _detour(), NEWER_POINT_HEADER, None, _NEWER_LINE
    return _restarted(monkeypatch, CDC_ONLY), OFFER_UNKNOWN_HEADER, _UNKNOWN_LEAD, _UNKNOWN_LINE


# --- the cards: the Start CDC card (and its tooltip) and the start-point card ------------------


@pytest.mark.parametrize("which", ["newer", "unknown offer"])
def test_the_cards_and_the_tooltip_name_the_drop_and_reload_condition(monkeypatch, which) -> None:
    state, header, lead, line = _refused(monkeypatch, which)
    start = _start_card(state, _W2)
    blob = " ".join(start.texts)
    assert header in blob
    if lead is not None:
        assert lead in blob
    # The route stays, now followed by the condition. FAILS WITHOUT THE FIX: the route named
    # only the redeploy, so following it seeded the new stack at W2 over the rows the append
    # kept.
    assert f"then Start CDC on the new stack. {_RELOAD_FIRST}" in blob
    assert start.start_disabled is True
    assert start.start_tooltip == line
    assert len(_PREFIX + start.start_tooltip) <= 300
    card = " ".join(_point_card(state, _W2).texts)
    assert header in card and _RELOAD_FIRST in card
    assert "Action needed" in card


def test_the_unknown_offer_refusal_names_it_for_the_rolled_back_starts_own_watermark(
    monkeypatch,
) -> None:
    # After a restart, while the stack's events do not give the offer (package CDC-2 below),
    # even the rolled-back Start's own watermark (W1) is refused. Its Full Load may also have
    # appended to rows an earlier load left, so the same condition applies there.
    state = _restarted(monkeypatch, CDC_ONLY)
    start = _start_card(state, _W1)
    assert _RELOAD_FIRST in " ".join(start.texts)
    assert start.start_tooltip == _UNKNOWN_LINE


# --- the click and the dialog -------------------------------------------------------------------


@pytest.mark.parametrize("which", ["newer", "unknown offer"])
def test_the_click_names_it_before_any_job(monkeypatch, which) -> None:
    state, header, lead, _line = _refused(monkeypatch, which)
    run = _capture(monkeypatch, state, _W2)
    assert not run["submitted"] and run["kwargs"] == {}
    assert [n["header"] for n in run["notices"]] == [header]
    body = run["notices"][0]["body"]
    if lead is not None:
        assert body.startswith(lead)
    assert f"then Start CDC on the new stack. {_RELOAD_FIRST}" in body
    assert body.endswith(" No connectors were changed.")


@pytest.mark.parametrize("which", ["newer", "unknown offer"])
def test_the_dialog_names_it_and_its_start_tooltip_too(monkeypatch, which) -> None:
    import asyncio

    import nicegui

    from tests.test_ui_data_migration import _DialogUi

    state, header, _lead, line = _refused(monkeypatch, which)
    for name in ("_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_binlog_resume_gap"):
        monkeypatch.setattr(_cdc_ui, name, lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_pg_objects_gate_block", lambda *_a, **_k: None)
    ui = _DialogUi()
    bodies: dict = {}

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        bodies[header] = body

    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    asyncio.run(
        _cdc_ui._open_cdc_start_dialog(
            ui, state, lambda: None, session=None, job_manager=_JM(_W2), inventory=None,
        )
    )
    assert ("error", header) in ui.notices
    assert _RELOAD_FIRST in bodies[header]
    start = ui.buttons["Start CDC"]
    assert start.enabled is False
    assert start.tooltip_text == line


# --- the Start CDC job's first line ---------------------------------------------------------------


def test_the_job_line_names_it_within_300_characters(monkeypatch) -> None:
    # A2 on the worker's fresh describe: the page read a stack never started; another session
    # started it from W1 and stopped it since, and the Start offers the detour's W2.
    state = _never_started(migration_type=CDC_ONLY)
    run = _capture(
        monkeypatch, state, _W2,
        describe_params={
            "DeploySink": "true", "MskBootstrapServers": "",
            "WatermarkBinlogFile": "mysql-bin.000090", "WatermarkBinlogPos": "4",
        },
    )
    assert run["submitted"] and run["kwargs"] == {}  # run_cdc_start never called
    first = str(run["error"]).splitlines()[0]
    # FAILS WITHOUT THE FIX: "... then Start CDC on the new stack. Nothing was changed."
    assert first == _PREFIX + _NEWER_LINE
    assert len(first) <= 300
    recorded = getattr(state, "cdc_start_refusal_notice")
    assert recorded[3] == NEWER_POINT_HEADER and _RELOAD_FIRST in recorded[4]
    # The unknown-offer refusal on the worker's describe of a rolled-back first Start.
    fresh = _never_started(migration_type=CDC_ONLY)
    refusal = _cdc_ui.newer_start_point_refusal(
        fresh, _W2, MYSQL, stack_params=dict(_RESTORED),
        stack_status="UPDATE_ROLLBACK_COMPLETE",
    )
    assert refusal is not None and _RELOAD_FIRST in refusal[0][3]
    first = str(_cdc_ui._start_refusal(fresh, refusal[0], "job-9", first_line=refusal[1]))
    first = first.splitlines()[0]
    assert first == _PREFIX + _UNKNOWN_LINE
    assert len(first) <= 300


def test_the_one_line_forms_say_what_the_notice_says() -> None:
    # Same meaning on every surface: the condition and the Drop & reload re-run before that Start.
    for line in (newer_point_line(CDC_ONLY), _cdc_ui._offer_unknown_refusal("x", CDC_ONLY)[1]):
        assert len(_PREFIX + line) <= 300, line
        assert "Delete CDC infrastructure and deploy it again, then Start CDC" in line
        assert "if the Full Load appended to rows already on the target" in line
        assert "re-run it choosing 'Drop & reload' first" in line
    tone, _icon, header, body = newer_point_notice(
        point="the Full Load watermark (binlog mysql-bin.000100:4)",
        last="binlog mysql-bin.000090:4",
        rolled_back=False,
        migration_type=CDC_ONLY,
    )
    assert (tone, header) == ("error", NEWER_POINT_HEADER)
    assert f"use {new_topics_route(CDC_ONLY)}. {_RELOAD_FIRST} No connectors were changed." in body


# --- what stays -----------------------------------------------------------------------------------


def test_a_manual_point_and_full_load_and_cdc_keep_their_routes(monkeypatch) -> None:
    # A Manual point: the new stack starts from that point, not from a Full Load's watermark,
    # so no Full Load is named. (Automatic would continue the stream: W1 is where it started.)
    state = _detour()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=4)
    seed, origin = _cdc_ui._cdc_start_seed(state, _W1, MYSQL)
    assert origin == "manual"
    notice, line = _cdc_ui.newer_start_point_refusal(
        state, seed, MYSQL, origin=origin, job_watermark=_W1,
    )
    assert _CDC_ONLY_ROUTE in notice[3] and "Drop & reload" not in notice[3]
    assert "set the CDC start point back to Automatic" in notice[3]
    assert line == _OLD_CDC_ONLY_LINE
    # The same Manual point after a restart on a stack whose first Start rolled back.
    restarted = _restarted(monkeypatch, CDC_ONLY)
    restarted.set_cdc_start_mode("manual")
    restarted.set_cdc_start_position(binlog_file="mysql-bin.000200", binlog_pos=4)
    seed, origin = _cdc_ui._cdc_start_seed(restarted, _W1, MYSQL)
    notice, line = _cdc_ui.newer_start_point_refusal(
        restarted, seed, MYSQL, origin=origin, job_watermark=_W1,
    )
    assert notice[3].startswith(_UNKNOWN_LEAD)
    assert "Drop & reload" not in notice[3] and "Drop & reload" not in line
    assert line.endswith("then Start CDC on the new stack. Nothing was changed.")
    # Full load + CDC: its route already re-runs the Full Load with Drop & reload.
    notice, line = _cdc_ui.newer_start_point_refusal(_stopped(), _W2, MYSQL)
    assert _RELOAD_FIRST not in notice[3]
    assert line == _FL_CDC_LINE == newer_point_line(FL_CDC)
    # PostgreSQL is never refused here (its Start continues from the stack's slot).
    assert _cdc_ui.newer_start_point_refusal(_detour(), _W2, SourceType.POSTGRES) is None


# =================================================================================================
# G26 item 2, package CDC-2: a rolled-back first Start's offer, read back from the stack's events
# =================================================================================================

_STACK = "dsql-cdc-stack"  # the cdc-stack a new session targets
_SEEDER = "CdcStartPrepResource"  # the offset seeder's custom resource (cdc-stack.yaml)
_CONNECTOR = "AWS::KafkaConnect::Connector"
_CONNECTOR_PROPS = json.dumps({"ConnectorName": "dsql-cdc-stack-debezium-source"})


def _props(file: str = "mysql-bin.000090", pos: str = "4") -> str:
    """The seeder resource's ResourceProperties as CloudFormation renders them (a JSON object of
    strings): the start offset a Start offered (W1 by default) among its other properties."""
    return json.dumps(
        {
            "ServiceToken": "arn:aws:lambda:us-east-1:111122223333:function:dsql-cdc-seeder",
            "SinkTopics": "dsql-cdc-stack.app.orders",
            "TopicPartitions": "3",
            "SinkTopicPartitions": "",
            "MaxMessageBytes": "4194304",
            "DlqTopicName": "dsql-cdc-stack-dlq",
            "WatermarkBinlogFile": file,
            "WatermarkBinlogPos": pos,
            "WatermarkGtids": "",
            "WatermarkTsSec": "1760230800",
        }
    )


def _stack_event(status: str, reason: str = "") -> dict:
    return {
        "StackName": _STACK, "LogicalResourceId": _STACK,
        "ResourceType": "AWS::CloudFormation::Stack", "ResourceStatus": status,
        "ResourceStatusReason": reason,
    }


def _event(logical: str, status: str, props=None, rtype="AWS::CloudFormation::CustomResource"):
    event = {
        "StackName": _STACK, "LogicalResourceId": logical, "ResourceType": rtype,
        "ResourceStatus": status,
    }
    if props is not None:
        event["ResourceProperties"] = props
    return event


_BEGAN = _stack_event("UPDATE_IN_PROGRESS", "User Initiated")
_ROLLING_BACK = _stack_event(
    "UPDATE_ROLLBACK_IN_PROGRESS",
    "The following resource(s) failed to create: [DebeziumSourceConnector].",
)


def _rolled_back_start(seeder=None, *, cleanup=None) -> list:
    """The events of a first Start CloudFormation rolled back, NEWEST FIRST (as
    DescribeStackEvents returns them): its update created the seeder with ``seeder`` as its
    properties (default W1; ``False``: no seeder resource, as with SeedMode=External) and failed
    on the source connector; the rollback deleted what it created (the cleanup's events carry
    ``cleanup``, default the same properties). Older: the infra deploy."""
    seeder = _props() if seeder is None else seeder
    cleanup = seeder if cleanup is None else cleanup
    has_seeder = seeder is not False
    return [
        _stack_event("UPDATE_ROLLBACK_COMPLETE"),
        *(
            [_event(_SEEDER, "DELETE_COMPLETE", cleanup),
             _event(_SEEDER, "DELETE_IN_PROGRESS", cleanup)]
            if has_seeder else []
        ),
        _stack_event("UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS"),
        _event("DebeziumSourceConnector", "DELETE_COMPLETE", _CONNECTOR_PROPS, _CONNECTOR),
        _ROLLING_BACK,
        _event("DebeziumSourceConnector", "CREATE_FAILED", _CONNECTOR_PROPS, _CONNECTOR),
        _event("DebeziumSourceConnector", "CREATE_IN_PROGRESS", _CONNECTOR_PROPS, _CONNECTOR),
        *(
            [_event(_SEEDER, "CREATE_COMPLETE", seeder),
             _event(_SEEDER, "CREATE_IN_PROGRESS", seeder),
             _event(_SEEDER, "CREATE_IN_PROGRESS", seeder)]
            if has_seeder else []
        ),
        _BEGAN,
        _stack_event("CREATE_COMPLETE"),
        _event("MskCluster", "CREATE_COMPLETE", "{}", "AWS::MSK::ServerlessCluster"),
        _stack_event("CREATE_IN_PROGRESS", "User Initiated"),
    ]


def _rolled_back_update(forward: str, restored: str) -> list:
    """A Start that UPDATED a seeder already in place (``forward``) and whose rollback restored
    it (``restored``), newest first."""
    return [
        _stack_event("UPDATE_ROLLBACK_COMPLETE"),
        _event(_SEEDER, "UPDATE_COMPLETE", restored),
        _event(_SEEDER, "UPDATE_IN_PROGRESS", restored),
        _ROLLING_BACK,
        _event("DebeziumSourceConnector", "UPDATE_FAILED", _CONNECTOR_PROPS, _CONNECTOR),
        _event(_SEEDER, "UPDATE_COMPLETE", forward),
        _event(_SEEDER, "UPDATE_IN_PROGRESS", forward),
        _BEGAN,
    ]


def _continued_rollback(forward: str, restored: str) -> list:
    """A rollback that failed and was continued (``continue_update_rollback``): two rollback
    phases over the same forward phase, newest first."""
    return [
        _stack_event("UPDATE_ROLLBACK_COMPLETE"),
        _event(_SEEDER, "UPDATE_COMPLETE", restored),
        _stack_event("UPDATE_ROLLBACK_IN_PROGRESS"),
        _stack_event("UPDATE_ROLLBACK_FAILED", "The following resource(s) failed to update: [x]."),
        _event(_SEEDER, "UPDATE_FAILED", restored),
        _ROLLING_BACK,
        _event(_SEEDER, "UPDATE_COMPLETE", forward),
        _BEGAN,
    ]


class _Cfn:
    """A CloudFormation client double: one describe of the stack (``status``, its parameters
    restored by the rollback), no other cdc-stack, and DescribeStackEvents over ``pages`` (each a
    list of events, newest first; ``NextToken`` is the next page's index) -- or ``pages`` raised.
    ``event_calls``: the NextToken of each DescribeStackEvents call."""

    def __init__(self, pages, status: str = "UPDATE_ROLLBACK_COMPLETE") -> None:
        self.pages, self.status, self.event_calls = pages, status, []

    def describe_stacks(self, StackName):  # noqa: N803 - boto3's keyword
        params = [{"ParameterKey": k, "ParameterValue": v} for k, v in _RESTORED.items()]
        return {"Stacks": [{"StackName": StackName, "StackStatus": self.status,
                            "Parameters": params}]}

    def list_stacks(self, **_kwargs):
        return {"StackSummaries": []}

    def describe_stack_events(self, StackName, NextToken=None):  # noqa: N803 - boto3's keywords
        assert StackName == _STACK
        self.event_calls.append(NextToken)
        if isinstance(self.pages, Exception):
            raise self.pages
        index = int(NextToken or 0)
        page = {"StackEvents": list(self.pages[index])}
        if index + 1 < len(self.pages):
            page["NextToken"] = str(index + 1)
        return page


class _Session:
    def __init__(self, cfn: _Cfn) -> None:
        self.cfn = cfn

    def client(self, service, region_name=None):
        assert service == "cloudformation", service
        return self.cfn


def _deployer(cfn: _Cfn):
    """The REAL deployer over the CloudFormation double."""
    from dsql_migrator.core.cdc_stack_deployer import CdcStackDeployer

    return CdcStackDeployer("us-east-1", session=_Session(cfn))


def _restarted_reading(monkeypatch, pages, mtype=FL_CDC, *, status="UPDATE_ROLLBACK_COMPLETE"):
    """The session after an app restart: a new state (nothing in memory), then the REAL
    stack-phase probe over the REAL deployer, whose CloudFormation client describes the stack
    (``status``, its parameters restored) and holds ``pages`` of its events -- or raises
    ``pages``. ``(state, client)``."""
    import dsql_migrator.core.cdc_deployer as dep
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.data_migration import _cdc_status as st

    cfn = _Cfn(pages, status)
    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _deployer(cfn))
    state = DataMigrationState()
    state.migration_type = mtype
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.job_id = "job-1"
    st._probe_cdc_stack_phase(
        state, SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
    )
    return state, cfn


def _dialog(monkeypatch, state, wm):
    """The Start CDC dialog over ``state`` (its source reads stubbed out): ``(ui, bodies)``."""
    import asyncio

    import nicegui

    from tests.test_ui_data_migration import _DialogUi

    for name in ("_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_binlog_resume_gap"):
        monkeypatch.setattr(_cdc_ui, name, lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_pg_objects_gate_block", lambda *_a, **_k: None)
    ui = _DialogUi()
    bodies: dict = {}

    def _notice(_ui, *, tone="info", icon=None, header="", body=""):
        ui.notices.append((tone, header))
        bodies[header] = body

    monkeypatch.setattr(_cdc_ui, "_render_notice", _notice)

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    asyncio.run(
        _cdc_ui._open_cdc_start_dialog(
            ui, state, lambda: None, session=None, job_manager=_JM(wm), inventory=None,
        )
    )
    return ui, bodies


_ROLLED_BACK_LEAD = (
    "CloudFormation rolled back this stack's last Start, which offered binlog "
    "mysql-bin.000090:4; "
)


# --- reading the offer from the events (pure) ----------------------------------------------------


def test_the_offer_is_read_from_the_forward_phase_of_the_rolled_back_update() -> None:
    from dsql_migrator.core.cdc_stack_deployer import rolled_back_start_offer_from_events as read

    # A first Start whose update created the seeder, then failed and was rolled back.
    assert read(_rolled_back_start(), _STACK) == _OFFER == ("mysql-bin.000090", 4)
    # The rollback phase is ignored whatever it carries: a cleanup's events, the restored blank
    # properties of a seeder the Start updated, another point, a continued rollback.
    w0 = _props("mysql-bin.000080", "4")
    assert read(_rolled_back_start(cleanup=w0), _STACK) == _OFFER
    assert read(_rolled_back_update(_props(), _props("", "")), _STACK) == _OFFER
    assert read(_rolled_back_update(_props(), w0), _STACK) == _OFFER
    assert read(_continued_rollback(_props(), w0), _STACK) == _OFFER
    # So a seeder event in the rollback phase alone gives nothing.
    rollback_only = [
        e for e in _rolled_back_update(_props(), w0) if e.get("ResourceProperties") != _props()
    ]
    assert read(rollback_only, _STACK) is None
    # The newest forward-phase seeder event WITH properties counts (the field is optional).
    no_props = [
        {k: v for k, v in e.items() if k != "ResourceProperties"}
        if e["LogicalResourceId"] == _SEEDER and e["ResourceStatus"] == "CREATE_COMPLETE"
        else e
        for e in _rolled_back_start()
    ]
    assert read(no_props, _STACK) == _OFFER


@pytest.mark.parametrize(
    "events",
    [
        _rolled_back_start(seeder=False),
        _rolled_back_start(seeder="{not json"),
        _rolled_back_start(seeder="[]"),
        _rolled_back_start(seeder=json.dumps({"WatermarkBinlogFile": "mysql-bin.000090"})),
        _rolled_back_start(seeder=_props(pos="4x")),
        _rolled_back_start(seeder=_props(pos="４")),  # a full-width digit
        _rolled_back_start(seeder=_props(file="mysql bin.000090")),
        _rolled_back_start(seeder=_props(file="mysql-bin")),
        _rolled_back_start(seeder=_props("", "")),  # a Start with no start point
        [
            {k: v for k, v in e.items() if k != "ResourceProperties"}
            if e["LogicalResourceId"] == _SEEDER else e
            for e in _rolled_back_start()
        ],
        _rolled_back_start()[: _rolled_back_start().index(_BEGAN)],  # the update's start unread
        [_stack_event("UPDATE_COMPLETE"), _event(_SEEDER, "CREATE_COMPLETE", _props()), _BEGAN],
    ],
    ids=[
        "SeedMode=External: no seeder resource",
        "properties not JSON",
        "properties not an object",
        "no position",
        "position not a number",
        "position not ASCII",
        "file name with a space",
        "file name without a sequence",
        "no start offset offered",
        "seeder events without properties",
        "the update's start not reached",
        "an update that did not roll back",
    ],
)
def test_an_offer_the_events_do_not_show_is_unknown(events) -> None:
    from dsql_migrator.core.cdc_stack_deployer import rolled_back_start_offer_from_events as read

    assert read(events, _STACK) is None


# --- the read-only DescribeStackEvents read ------------------------------------------------------


def test_the_events_read_pages_back_to_the_update_and_stops_there() -> None:
    events = _rolled_back_start()
    began = events.index(_BEGAN)
    split = events.index(_ROLLING_BACK) + 1
    # Page 1: the rollback; page 2: the forward phase down to the stack-level UPDATE_IN_PROGRESS;
    # page 3: older events, never requested.
    cfn = _Cfn([events[:split], events[split : began + 1], events[began + 1 :]])
    assert _deployer(cfn).rolled_back_start_offer(_STACK) == _OFFER
    assert cfn.event_calls == [None, "1"]


def test_the_events_read_is_bounded_and_unknown_past_the_bound() -> None:
    from dsql_migrator.core.cdc_stack_deployer import _OFFER_EVENT_PAGES

    rollback = _rolled_back_start()[: _rolled_back_start().index(_ROLLING_BACK) + 1]
    cfn = _Cfn([rollback] * (_OFFER_EVENT_PAGES + 3))
    assert _deployer(cfn).rolled_back_start_offer(_STACK) is None
    assert len(cfn.event_calls) == _OFFER_EVENT_PAGES


def test_unreadable_events_give_no_offer() -> None:
    cfn = _Cfn(RuntimeError("AccessDenied: cloudformation:DescribeStackEvents"))
    assert _deployer(cfn).rolled_back_start_offer(_STACK) is None
    assert cfn.event_calls == [None]


# --- after an app restart: judged against the offer read, as before the restart ------------------


def test_the_probe_records_the_offer_read_for_a_rollback_status_only(monkeypatch) -> None:
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import _cdc_status as st

    # FAILS WITHOUT THE FIX: nothing read the stack's events, so the offer stayed unknown.
    state, cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    assert st.cdc_start_rolled_back(state) and state.cdc_stack_phase == "infra"
    assert state.cdc_offered_seed == _OFFER and cfn.event_calls == [None]
    # Recorded as this process records its own offer: in memory only (no session field), and a
    # discovery that reads it re-renders the cards.
    assert not [name for name in SessionSnapshot.model_fields if "offer" in name]
    assert st.cdc_discovery_fingerprint(state) != st.cdc_discovery_fingerprint(
        _restarted_reading(monkeypatch, RuntimeError("AccessDenied"))[0]
    )
    # Any rollback status is read; any other status is not (no events call at all).
    for status in ("UPDATE_ROLLBACK_IN_PROGRESS", "UPDATE_ROLLBACK_FAILED"):
        state, cfn = _restarted_reading(monkeypatch, [_rolled_back_start()], status=status)
        assert state.cdc_offered_seed == _OFFER and cfn.event_calls == [None], status
    for status in ("UPDATE_COMPLETE", "CREATE_COMPLETE"):
        state, cfn = _restarted_reading(monkeypatch, [_rolled_back_start()], status=status)
        assert state.cdc_offered_seed is None and cfn.event_calls == [], status
    # Cleared with the stack, like any offer this process records: Delete CDC infrastructure
    # (the probe reads it absent), or another stack adopted.
    state, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    state.set_cdc_stack_phase("absent", status=None)
    assert state.cdc_offered_seed is None
    state, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    assert state.adopt_cdc_stack("dsql-cdc-other") and state.cdc_offered_seed is None


def test_after_a_restart_every_start_point_is_judged_as_before_it(monkeypatch) -> None:
    before = _rolled_back()  # this process made that Start and recorded its offer (W1)
    after, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    assert after.cdc_offered_seed == before.cdc_offered_seed == _OFFER
    for seed in (_W0, _W1, _W2):
        assert _cdc_ui.newer_start_point_refusal(after, seed, MYSQL) == (
            _cdc_ui.newer_start_point_refusal(before, seed, MYSQL)
        ), seed
        assert _cdc_ui.seed_refused_by_an_earlier_offer(after, seed, MYSQL) == (
            _cdc_ui.seed_refused_by_an_earlier_offer(before, seed, MYSQL)
        ), seed
    # The same seed: neither check refuses it (FAILS WITHOUT THE FIX: the unknown-offer refusal).
    assert _cdc_ui.newer_start_point_refusal(after, _W1, MYSQL) is None
    assert _cdc_ui.seed_refused_by_an_earlier_offer(after, _W1, MYSQL) is None
    # A later one: A2, against that offer.
    (tone, _icon, header, body), line = _cdc_ui.newer_start_point_refusal(after, _W2, MYSQL)
    assert (tone, header) == ("error", NEWER_POINT_HEADER) and body.startswith(_ROLLED_BACK_LEAD)
    assert line == newer_point_line(FL_CDC)
    # An earlier one: the earlier-offer check (the seeder never rewinds).
    reason = _cdc_ui.seed_refused_by_an_earlier_offer(after, _W0, MYSQL)
    assert reason.startswith(
        "An earlier Start already offered the start offset mysql-bin.000090:4 "
    )
    assert _NEW_WAY in reason
    # PostgreSQL is never judged here.
    assert _cdc_ui.newer_start_point_refusal(after, _W2, SourceType.POSTGRES) is None


def test_after_a_restart_the_retry_from_its_start_point_is_taken_everywhere(monkeypatch) -> None:
    state, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    # The cards. FAILS WITHOUT THE FIX: the unknown-offer refusal and a disabled Start.
    start = _start_card(state, _W1)
    blob = " ".join(start.texts)
    assert "Ready to start CDC again" in blob
    assert NEWER_POINT_HEADER not in blob and OFFER_UNKNOWN_HEADER not in blob
    assert start.start_disabled is False
    card = " ".join(_point_card(state, _W1).texts)
    assert "Starting again after a rolled-back Start" in card
    assert NEWER_POINT_HEADER not in card and OFFER_UNKNOWN_HEADER not in card
    # The dialog.
    ui, _bodies = _dialog(monkeypatch, state, _W1)
    headers = [header for _tone, header in ui.notices]
    assert NEWER_POINT_HEADER not in headers and OFFER_UNKNOWN_HEADER not in headers
    assert ui.buttons["Start CDC"].enabled is True
    # The click and the job, judged again on the worker's describe: a seeded Start from that
    # same point (the seeder keeps any offset that Start's connector committed at or past it).
    assert _cdc_ui.newer_start_point_refusal(
        state, _W1, MYSQL, stack_params=dict(_RESTORED), stack_status="UPDATE_ROLLBACK_COMPLETE",
    ) is None
    run = _capture(monkeypatch, state, _W1, describe_params=dict(_RESTORED))
    assert run["submitted"] and run["error"] is None
    seed = run["kwargs"]["watermark"]
    assert (seed.binlog_file, seed.binlog_position) == _OFFER
    assert dict(run["kwargs"]["params"].filled)["SnapshotMode"] == "recovery"


def test_after_a_restart_a_later_or_earlier_start_point_is_refused(monkeypatch) -> None:
    # Later (the detour's W2, on to CDC only): A2 against the offer read -- the cards, the
    # disabled Start's tooltip, the click.
    state, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()], CDC_ONLY)
    start = _start_card(state, _W2)
    blob = " ".join(start.texts)
    assert NEWER_POINT_HEADER in blob and _ROLLED_BACK_LEAD in blob
    assert _UNKNOWN_LEAD not in blob and OFFER_UNKNOWN_HEADER not in blob
    assert start.start_disabled is True and start.start_tooltip == newer_point_line(CDC_ONLY)
    card = " ".join(_point_card(state, _W2).texts)
    assert NEWER_POINT_HEADER in card and _ROLLED_BACK_LEAD in card
    run = _capture(monkeypatch, state, _W2)
    assert not run["submitted"] and run["kwargs"] == {}
    assert [n["header"] for n in run["notices"]] == [NEWER_POINT_HEADER]
    # Earlier (W0): the earlier-offer check refuses the click, before any job.
    state, _cfn = _restarted_reading(monkeypatch, [_rolled_back_start()])
    run = _capture(monkeypatch, state, _W0)
    assert not run["submitted"] and run["kwargs"] == {}
    assert [n["header"] for n in run["notices"]] == ["This start point cannot be applied"]
    assert "offered the start offset mysql-bin.000090:4 " in run["notices"][0]["body"]


@pytest.mark.parametrize(
    "pages",
    [
        RuntimeError("AccessDenied: cloudformation:DescribeStackEvents"),
        [_rolled_back_start(seeder=False)],
        [_rolled_back_start(seeder="{not json")],
    ],
    ids=["events unreadable", "SeedMode=External", "malformed properties"],
)
def test_after_a_restart_an_offer_the_events_do_not_give_still_fails_closed(
    monkeypatch, pages
) -> None:
    state, _cfn = _restarted_reading(monkeypatch, pages)
    assert state.cdc_offered_seed is None
    # Every start point, that same one included, under a header that does not call it newer.
    for seed in (_W0, _W1, _W2):
        (tone, _icon, header, body), line = _cdc_ui.newer_start_point_refusal(state, seed, MYSQL)
        assert (tone, header) == ("error", OFFER_UNKNOWN_HEADER), seed
        assert body.startswith(_UNKNOWN_LEAD) and "newer" not in header
        assert len("Start CDC blocked: " + line) <= 300
    start = _start_card(state, _W1)
    assert OFFER_UNKNOWN_HEADER in " ".join(start.texts) and start.start_disabled is True
    run = _capture(monkeypatch, state, _W1)
    assert not run["submitted"] and run["kwargs"] == {}
    assert [n["header"] for n in run["notices"]] == [OFFER_UNKNOWN_HEADER]
