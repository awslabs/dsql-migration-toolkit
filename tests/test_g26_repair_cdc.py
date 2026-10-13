# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G26 repair, task R-CDC: three follow-ups to G26 item 2, where the start offset of a first MySQL
Start that CloudFormation rolled back is read back from the stack's own CloudFormation events.

[A] The Start CDC worker judged the backstop (decision A2) on its OWN describe of the stack, but
against the page's cached offer. Suppose the cache was older than the describe: another session's
first Start rolled back after this page's last probe. Then the worker refused the gapless retry
from that Start's own start point without ever reading the events, and the notice it recorded
said the app's record "does not survive an app restart" and that the offset "could not be read
from the stack's events". Both claims were false there. Now the worker reads the offer itself:
read-only, only when its describe shows a rollback and this session holds no offer, failing closed.
It records the offer and then runs both checks the click makes against it: A2 refuses a later
point, and the earlier-offer check refuses an earlier one (the seeder never rewinds). Without the
earlier-offer check, reading the offer would turn a fail-closed refusal of an earlier point into a
Start that silently skips the changes in between. If the offer is still unknown, the notice names
an app restart and a Start made outside this session only as possible reasons, and says the events
did not give the offset only after a read did not find it.

[B] The events parser stopped at ANY stack-level UPDATE_IN_PROGRESS. CloudFormation also emits
stack-level events that carry a DetailedStatus, and those mark a point inside a phase, not its
start. CONFIGURATION_COMPLETE is emitted once every resource has reached that point, and a resource
can still fail after it. VALIDATION_FAILED is the other value (StackEvent API reference). Now only a
stack-level UPDATE_IN_PROGRESS without a DetailedStatus ends the scan.

[C] Changing the CDC stack name kept the offer recorded for the previous stack. Start's
earlier-offer check then refused an earlier start point on a fresh stack whose seeder was never
offered anything. A name change now clears the offer. The probe reads a rolled-back stack's offer
back from that stack's events.

Each regression test FAILS on the code before this repair. The ones marked "what stays" pass on
both versions. Fakes only: no AWS.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import dsql_migrator.core.cdc_deployer as dep
from dsql_migrator.core.cdc_stack_deployer import (
    CdcStackDeployer,
    rolled_back_start_offer_from_events,
)
from dsql_migrator.core.models import MigrationMode, SourceType, TableSelection
from dsql_migrator.ui.cdc_start_point_gate import (
    NEWER_POINT_HEADER,
    OFFER_UNKNOWN_HEADER,
    StartPointVerdict,
    newer_point_line,
    offer_unknown_line,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType, _cdc_ui
from dsql_migrator.ui.data_migration import _cdc_status as st
from tests.test_g25_cdc_r7 import _W0, _W1, _W2, _capture, _point_card, _rolled_back, _start_card
from tests.test_g25_cdc_r8 import _NEW_WAY, _OFFER, _RESTORED
from tests.test_g26_cdc import (
    _BEGAN,
    _CONNECTOR,
    _CONNECTOR_PROPS,
    _ROLLING_BACK,
    _STACK,
    _Cfn,
    _deployer,
    _dialog,
    _event,
    _props,
    _restarted_reading,
    _rolled_back_start,
    _rolled_back_update,
    _stack_event,
)

MYSQL = SourceType.MYSQL
FL_CDC = MigrationType.FULL_LOAD_AND_CDC
_SESSION = SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None)
# The unknown-offer notice's lead. It says what this session holds and what a read of the events
# gave, and names an app restart and a Start made outside this session only as possible reasons.
_LEAD = (
    "CloudFormation rolled back this stack's last Start, and the start offset that Start offered "
    "is no longer known: the rollback restored the stack's parameters, this session holds no "
    "record of that offset (the app may have restarted since that Start, or that Start may have "
    "been made outside this session), and it could not be read from the stack's events."
)
# The cause the lead used to assert, true or not.
_OLD_CAUSE = "the app's own record of that offset does not survive an app restart"
_EARLIER = "This start point cannot be applied"


# =================================================================================================
# [A] the Start CDC worker reads the offer its own describe shows
# =================================================================================================


class _LiveCfn:
    """A CloudFormation (and STS) client double for one cdc-stack whose status, parameters and
    events change mid-scenario. At first the stack is deployed and never started.
    ``event_calls`` records the NextToken of each DescribeStackEvents call, and
    ``events_error`` makes that call raise. ``on_describe`` runs once, at the next
    DescribeStacks call."""

    def __init__(self) -> None:
        self.status = "CREATE_COMPLETE"
        self.params = dict(_RESTORED)
        self.events = [
            _stack_event("CREATE_COMPLETE"),
            _event("MskCluster", "CREATE_COMPLETE", "{}", "AWS::MSK::ServerlessCluster"),
            _stack_event("CREATE_IN_PROGRESS", "User Initiated"),
        ]
        self.events_error = None
        self.event_calls: list = []
        self.on_describe = None

    def describe_stacks(self, StackName):  # noqa: N803 - boto3's keyword
        hook, self.on_describe = self.on_describe, None
        if hook is not None:
            hook()
        params = [{"ParameterKey": k, "ParameterValue": v} for k, v in self.params.items()]
        return {"Stacks": [{"StackName": StackName, "StackStatus": self.status,
                            "Parameters": params}]}

    def list_stacks(self, **_kwargs):
        return {"StackSummaries": []}

    def describe_stack_events(self, StackName, NextToken=None):  # noqa: N803 - boto3's keywords
        assert StackName == _STACK
        self.event_calls.append(NextToken)
        if self.events_error is not None:
            raise self.events_error
        return {"StackEvents": list(self.events)}

    def get_caller_identity(self):  # the Start CDC worker's bucket lookup (STS)
        return {"Account": "111122223333"}

    def first_start_rolled_back(self, seeder=None) -> None:
        """A first Start from W1 (``seeder``: its seeder properties) that CloudFormation rolled
        back. The parameters are restored, and the forward phase's seeder events carry W1."""
        self.status = "UPDATE_ROLLBACK_COMPLETE"
        self.params = dict(_RESTORED)
        self.events = _rolled_back_start(seeder)


class _Session:
    def __init__(self, cfn) -> None:
        self.cfn = cfn

    def client(self, service, region_name=None):
        assert service in ("cloudformation", "sts"), service
        return self.cfn


def _real(cfn) -> CdcStackDeployer:
    """The REAL deployer over the client double."""
    return CdcStackDeployer("us-east-1", session=_Session(cfn))


class _RealDeployer:
    """``monkeypatch`` for the Start CDC capture harness, except that the harness's fake deployer
    is replaced with the REAL one over ``cfn``. The worker's describe then carries the stack's
    real status, and the worker can read the stack's events."""

    def __init__(self, monkeypatch, cfn) -> None:
        self._mp, self._cfn = monkeypatch, cfn

    def setattr(self, target, name, value=None, *args, **kwargs):
        if target is dep and name == "build_cdc_stack_deployer":
            value = lambda *_a, **_k: _real(self._cfn)  # noqa: E731
        return self._mp.setattr(target, name, value, *args, **kwargs)

    def __getattr__(self, item):
        return getattr(self._mp, item)


def _probe(monkeypatch, cfn, state) -> None:
    """The REAL stack-phase probe (a CDC-step render's discovery) over the REAL deployer."""
    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _real(cfn))
    st._probe_cdc_stack_phase(state, _SESSION)


def _page(monkeypatch, cfn) -> DataMigrationState:
    """This page's session, as its last probe left it."""
    state = DataMigrationState()
    state.migration_type = FL_CDC
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.job_id = "job-1"
    _probe(monkeypatch, cfn, state)
    return state


def _click(monkeypatch, cfn, state, wm):
    """The REAL Start CDC click, the job it submits, and that job's worker. As in the repo's
    capture harness, run_cdc_start and the source reads are stubbed."""
    from tests.test_ui_data_migration import _run_start_cdc_capture

    state.set_selection(TableSelection(selected_tables=["orders"]))
    return _run_start_cdc_capture(
        _RealDeployer(monkeypatch, cfn), source_type=MYSQL, job_watermark=wm, state=state,
        catch=True, start_point_read=lambda *_a, **_k: StartPointVerdict(kind="ok"),
    )


def _stale_page(monkeypatch, seeder=None):
    """The scenario. This page's last probe read the stack as never started, so it read no
    events. Since then another session's first Start from W1 rolled back, and this page has not
    re-rendered, so no probe has read that either."""
    cfn = _LiveCfn()
    state = _page(monkeypatch, cfn)
    assert (state.cdc_stack_phase, state.cdc_stack_phase_status) == ("infra", "CREATE_COMPLETE")
    assert state.cdc_offered_seed is None and cfn.event_calls == []
    cfn.first_start_rolled_back(seeder)
    return cfn, state


def _refusal(run, state):
    """``(the job's first line, the recorded notice's (tone, header, body))``."""
    assert run["submitted"] and run["kwargs"] == {}  # refused before run_cdc_start
    first = str(run["error"]).splitlines()[0]
    assert len(first) <= 300
    _job, tone, _icon, header, body = getattr(state, "cdc_start_refusal_notice")
    return first, (tone, header, body)


def test_the_worker_reads_the_offer_its_describe_shows_and_takes_the_retry(monkeypatch) -> None:
    cfn, state = _stale_page(monkeypatch)
    run = _click(monkeypatch, cfn, state, _W1)
    # FAILS WITHOUT THE FIX. The worker refused the retry from W1 with the unknown-offer line,
    # and nothing had read the events, which hold W1.
    assert run["error"] is None and run["submitted"]
    assert cfn.event_calls == [None]  # one read-only DescribeStackEvents page, on the worker
    assert state.cdc_offered_seed == _OFFER
    seed = run["kwargs"]["watermark"]
    assert (seed.binlog_file, seed.binlog_position) == _OFFER
    assert dict(run["kwargs"]["params"].filled)["SnapshotMode"] == "recovery"
    assert getattr(state, "cdc_start_refusal_notice", None) is None


def test_the_worker_refuses_a_later_point_against_the_offer_it_read(monkeypatch) -> None:
    cfn, state = _stale_page(monkeypatch)
    first, (tone, header, body) = _refusal(_click(monkeypatch, cfn, state, _W2), state)
    # FAILS WITHOUT THE FIX: the unknown-offer refusal, judged against an offer nothing read.
    assert first == "Start CDC blocked: " + newer_point_line(FL_CDC)
    assert (tone, header) == ("error", NEWER_POINT_HEADER)
    assert body.startswith(
        "CloudFormation rolled back this stack's last Start, which offered binlog "
        "mysql-bin.000090:4; "
    )
    assert cfn.event_calls == [None]


def test_the_worker_refuses_an_earlier_point_than_the_offer_it_read(monkeypatch) -> None:
    # A2 does not refuse an earlier point. The click ran the earlier-offer check while it did not
    # have the offer, so the worker runs that check again on the offer it just read. The seeder
    # keeps the offset W1 that Start seeded, so a Start from W0 would silently skip W0..W1.
    cfn, state = _stale_page(monkeypatch)
    first, (tone, header, body) = _refusal(_click(monkeypatch, cfn, state, _W0), state)
    # FAILS WITHOUT THE FIX: the unknown-offer refusal.
    assert first.startswith(f"Start CDC blocked: {_EARLIER}. ")
    assert (tone, header) == ("error", _EARLIER)
    assert body.startswith("An earlier Start already offered the start offset mysql-bin.000090:4 ")
    # Its way on is judged on the worker's describe. That describe shows a rolled-back Start, so
    # a later point is refused too and "or later" is not offered, although the page's cache still
    # reads the stack as never started.
    assert _NEW_WAY in body and "or later" not in body
    assert not st.cdc_stack_has_streamed(state)
    # Once a probe reads the rollback, the click refuses W0 before any job, with the same way on.
    _probe(monkeypatch, cfn, state)
    run = _capture(monkeypatch, state, _W0)
    assert not run["submitted"] and run["kwargs"] == {}
    assert [n["header"] for n in run["notices"]] == [_EARLIER]
    assert _NEW_WAY in run["notices"][0]["body"]


def test_the_worker_refuses_an_earlier_point_than_an_offer_recorded_since_the_click(
    monkeypatch,
) -> None:
    # The click judged without the offer. A probe from another render then recorded it before
    # the job's worker ran; here that happens at the worker's own describe. The offer is held,
    # so the worker reads nothing more, and the earlier-offer check refuses W0. FAILS WITHOUT
    # THE FIX: the worker did not run that check, so the Start went ahead from W0. The seeder
    # keeps W1, so that Start would have silently skipped W0..W1.
    cfn, state = _stale_page(monkeypatch)
    cfn.on_describe = lambda: state.note_cdc_offered_seed(_W1)
    first, (tone, header, body) = _refusal(_click(monkeypatch, cfn, state, _W0), state)
    assert first.startswith(f"Start CDC blocked: {_EARLIER}. ")
    assert (tone, header) == ("error", _EARLIER) and cfn.event_calls == []
    assert body.startswith("An earlier Start already offered the start offset mysql-bin.000090:4 ")


@pytest.mark.parametrize(
    "unread",
    ["events unreadable", "SeedMode=External: no seeder resource"],
)
def test_the_worker_asserts_no_cause_when_the_events_do_not_give_the_offer(
    monkeypatch, unread
) -> None:
    cfn, state = _stale_page(monkeypatch, seeder=False if unread.startswith("SeedMode") else None)
    if unread == "events unreadable":
        cfn.events_error = RuntimeError("Throttling: Rate exceeded")
    first, (tone, header, body) = _refusal(_click(monkeypatch, cfn, state, _W1), state)
    # FAILS WITHOUT THE FIX: nothing had read the events, yet the notice said they "could not be
    # read", and it blamed an app restart that never happened.
    assert cfn.event_calls == [None]
    assert state.cdc_offered_seed is None
    assert first == "Start CDC blocked: " + offer_unknown_line(FL_CDC)
    assert (tone, header) == ("error", OFFER_UNKNOWN_HEADER)
    assert body.startswith(_LEAD) and _OLD_CAUSE not in body


def test_the_unknown_offer_notice_asserts_no_cause_on_any_surface(monkeypatch) -> None:
    # After an app restart, the probe's own read of the events failed. The cards, the dialog and
    # the click show the same notice, with the same lead. FAILS WITHOUT THE FIX: the lead asserted
    # the restart.
    state, cfn = _restarted_reading(monkeypatch, RuntimeError("AccessDenied"))
    assert cfn.event_calls == [None] and state.cdc_offered_seed is None
    start = " ".join(_start_card(state, _W1).texts)
    card = " ".join(_point_card(state, _W1).texts)
    for blob in (start, card):
        assert OFFER_UNKNOWN_HEADER in blob and _LEAD in blob and _OLD_CAUSE not in blob
    _ui, bodies = _dialog(monkeypatch, state, _W1)
    assert bodies[OFFER_UNKNOWN_HEADER].startswith(_LEAD)
    run = _capture(monkeypatch, state, _W1)
    assert not run["submitted"]
    (notice,) = run["notices"]
    assert notice["header"] == OFFER_UNKNOWN_HEADER and notice["body"].startswith(_LEAD)


def test_the_worker_reads_no_events_without_a_rollback_or_with_an_offer_held(monkeypatch) -> None:
    # What stays. With a stack never started, on the worker's describe too, nothing reads the
    # events and the Start runs.
    cfn = _LiveCfn()
    state = _page(monkeypatch, cfn)
    run = _click(monkeypatch, cfn, state, _W1)
    assert run["error"] is None and run["submitted"] and cfn.event_calls == []
    # With an offer held, the worker reads nothing more. Here the probe read the offer from a
    # rolled-back Start, and the retry from it runs.
    cfn = _LiveCfn()
    cfn.first_start_rolled_back()
    state = _page(monkeypatch, cfn)
    assert state.cdc_offered_seed == _OFFER and cfn.event_calls == [None]
    run = _click(monkeypatch, cfn, state, _W1)
    assert run["error"] is None and run["submitted"] and cfn.event_calls == [None]


# =================================================================================================
# [B] a stack-level event that carries a DetailedStatus does not begin a phase
# =================================================================================================


def _point(status: str, detailed: str) -> dict:
    """A stack-level event that carries a DetailedStatus: a point inside a phase."""
    return {**_stack_event(status), "DetailedStatus": detailed}


def _failed_after_a_stack_point(detailed: str = "CONFIGURATION_COMPLETE") -> list:
    """A first Start, newest first. Its source connector failed AFTER the update's stack-level
    ``detailed`` event, which here is CONFIGURATION_COMPLETE (every resource reached it: the
    connector then failed its stabilization). Then CloudFormation rolled the Start back."""
    events = _rolled_back_start()
    cut = events.index(_ROLLING_BACK) + 2  # through the connector's CREATE_FAILED
    connector = _event(
        "DebeziumSourceConnector", "CREATE_IN_PROGRESS", _CONNECTOR_PROPS, _CONNECTOR
    )
    return [
        *events[:cut],
        _point("UPDATE_IN_PROGRESS", detailed),
        {**connector, "DetailedStatus": "CONFIGURATION_COMPLETE"},
        *events[cut:],
    ]


@pytest.mark.parametrize("detailed", ["CONFIGURATION_COMPLETE", "VALIDATION_FAILED"])
def test_a_stack_level_point_inside_the_update_does_not_end_the_scan(detailed) -> None:
    events = _failed_after_a_stack_point(detailed)
    assert events.index(_point("UPDATE_IN_PROGRESS", detailed)) < events.index(_BEGAN)
    # FAILS WITHOUT THE FIX: the scan ended at that event, before the seeder's events, so the
    # offer was unknown and the retry from W1 was refused.
    assert rolled_back_start_offer_from_events(events, _STACK) == _OFFER


@pytest.mark.parametrize(
    "events",
    [
        # The cleanup (seeder deleted, carrying W0 here), then a stack-level point of the rollback.
        [
            *_rolled_back_start(cleanup=_props("mysql-bin.000080", "4"))[:4],
            _point("UPDATE_ROLLBACK_IN_PROGRESS", "CONFIGURATION_COMPLETE"),
            *_rolled_back_start(cleanup=_props("mysql-bin.000080", "4"))[4:],
        ],
        # The rollback of a seeder the Start updated: its restored W0 lies past the point.
        [
            *_rolled_back_update(_props(), _props("mysql-bin.000080", "4"))[:1],
            _point("UPDATE_ROLLBACK_IN_PROGRESS", "CONFIGURATION_COMPLETE"),
            *_rolled_back_update(_props(), _props("mysql-bin.000080", "4"))[1:],
        ],
    ],
    ids=["after the cleanup", "before the restored seeder"],
)
def test_a_stack_level_point_inside_the_rollback_still_gives_the_forward_offer(events) -> None:
    # What stays: the rollback phase is ignored whatever it holds.
    assert rolled_back_start_offer_from_events(events, _STACK) == _OFFER


def test_the_events_read_pages_past_such_a_point_to_the_updates_start() -> None:
    events = _failed_after_a_stack_point()
    point = events.index(_point("UPDATE_IN_PROGRESS", "CONFIGURATION_COMPLETE"))
    began = events.index(_BEGAN)
    # Page 1 ends at that stack-level point, and page 2 holds the forward phase down to the
    # update's start. Page 3, the older events, is never requested.
    cfn = _Cfn([events[: point + 1], events[point + 1 : began + 1], events[began + 1 :]])
    # FAILS WITHOUT THE FIX: the read stopped on page 1 with no offer.
    assert _deployer(cfn).rolled_back_start_offer(_STACK) == _OFFER
    assert cfn.event_calls == [None, "1"]


def test_an_update_whose_start_is_not_reached_stays_unknown() -> None:
    # What stays: a stack-level point is never taken for the update's start. If the read never
    # reaches that start, the offer is unknown, and Start fails closed.
    events = _failed_after_a_stack_point()
    unreached = events[: events.index(_BEGAN)]
    assert rolled_back_start_offer_from_events(unreached, _STACK) is None


# =================================================================================================
# [C] a stack name change clears the offer recorded for the previous stack
# =================================================================================================


def test_an_offer_recorded_for_one_stack_never_refuses_a_seed_on_another(monkeypatch) -> None:
    state = _rolled_back()  # A (dsql-cdc-stack): its rolled-back Start offered W1, recorded here
    assert state.cdc_stack_name == _STACK and state.cdc_offered_seed == _OFFER
    assert _cdc_ui.seed_refused_by_an_earlier_offer(state, _W0, MYSQL) is not None
    # The same name keeps it, and so does a name that is rejected.
    assert state.set_cdc_stack_name(_STACK) and state.cdc_offered_seed == _OFFER
    assert not state.set_cdc_stack_name("not a stack name") and state.cdc_offered_seed == _OFFER
    # B, another stack, deployed and never started.
    assert state.set_cdc_stack_name("dsql-cdc-orders")
    # FAILS WITHOUT THE FIX: A's offer stayed, and it refused W0 on B, whose seeder was never
    # offered anything.
    assert state.cdc_offered_seed is None
    fresh = _Cfn([], status="CREATE_COMPLETE")
    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _deployer(fresh))
    st._probe_cdc_stack_phase(state, _SESSION)
    assert state.cdc_stack_phase_status == "CREATE_COMPLETE" and fresh.event_calls == []
    assert _cdc_ui.seed_refused_by_an_earlier_offer(state, _W0, MYSQL) is None
    run = _capture(monkeypatch, state, _W0)
    assert run["submitted"] and run["error"] is None
    # Back on A, the probe reads the offer of A's rolled-back Start from A's events again.
    assert state.set_cdc_stack_name(_STACK) and state.cdc_offered_seed is None
    rolled_back = _Cfn([_rolled_back_start()])
    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _deployer(rolled_back))
    st._probe_cdc_stack_phase(state, _SESSION)
    assert state.cdc_offered_seed == _OFFER and rolled_back.event_calls == [None]
    assert _cdc_ui.seed_refused_by_an_earlier_offer(state, _W0, MYSQL) is not None
