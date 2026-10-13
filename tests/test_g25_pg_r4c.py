# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 4, closing step, package PG.

* Decision (2), confirmed[0] and pending[0]/[1]: a cdc-stack whose last update CloudFormation
  ROLLED BACK (``UPDATE_ROLLBACK_COMPLETE`` with a blank ``MskBootstrapServers``) counts as
  STARTED. Only a Start updates an infra stack, and its source connector may have streamed,
  and committed an offset Debezium continues from, before the rollback deleted it. So the
  next Start is never refused on the slot position (the slot-position block, which said
  "nothing of this migration has streamed from it", is not shown for it), and a block on it
  names the started-stack routes, never "Re-snapshot every table instead" (Debezium skips
  that snapshot once an offset is committed). Read from the stack-phase probe's own
  ``cdc_stack_phase_status`` (the same describe as the phase): no new saved field.
* confirmed[1]: "Re-snapshot every table instead" is offered only when the Start it unlocks
  passes the checks of its own route (judged in the dialog, off the event loop, with
  ``snapshot.mode=initial``): an inheritance parent, a slot of that name it cannot use, or
  the same refusal again withhold it, and the refusal is shown instead.
* pending[2]: the "Publication autocreation is disabled" diagnosis names the step for each
  stack state (connectors that still exist are stopped first; a rolled-back Start left none)
  and the routes the Start dialog shows in both.

Each regression test FAILS on the code before this step; the never-started and
offered-remedy cases are guards. Fakes only: no real database.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from tests.test_g25_pg import AT_START, MOVED, SLOT_REFUSAL
from tests.test_g25_pg_publication import _objects
from tests.test_g25_pg_r4 import (
    NEVER_STARTED,
    RUNNING,
    STOPPED,
    _cdc_only,
    _dialog,
    _fresh,
    _headers,
    _restart,
    _snapshot,
    _start,
)

ROLLED_BACK = "UPDATE_ROLLBACK_COMPLETE"
REMEDY = "Re-snapshot every table instead"
SLOT_MISSING = "CDC's replication slot does not exist on the source"
SLOT_UNUSABLE = "A replication slot of that name is not usable for CDC"
PUBLICATION_MISSING = "CDC's publication does not exist on the source"
COVERAGE = "CDC's publication does not cover every selected table"
# Debezium's own line (debezium-connector-postgres 2.7.4) when snapshot.mode=never finds no
# publication (publication.autocreate.mode=disabled).
_PUBLICATION_GONE_TAIL = (
    "ERROR WorkerSourceTask{id=dsql-cdc-stack-debezium-source-0} Task threw an uncaught and "
    "unrecoverable exception.\n"
    "org.apache.kafka.connect.errors.ConnectException: Publication autocreation is disabled, "
    "please create one and restart the connector."
)


def _probe(monkeypatch, state, parameters, status) -> None:
    """The REAL stack-phase probe over one describe of the cdc-stack in ``status``."""
    import dsql_migrator.core.cdc_deployer as dep
    from dsql_migrator.core.cdc_stack_deployer import is_stable_stack_status
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    class _Deployer:
        def describe_stack_or_none(self, _name):
            return SimpleNamespace(
                stack_status=status, is_stable=is_stable_stack_status(status),
                current_parameters=dict(parameters),
            )

        def list_cdc_stacks(self):
            return []

    monkeypatch.setattr(dep, "build_cdc_stack_deployer", lambda *_a, **_k: _Deployer())
    _probe_cdc_stack_phase(
        state,
        SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"), aws_profile=None),
    )


def _source(monkeypatch, objects, *, parents=(), fail_from=None) -> list:
    """A PostgreSQL source: the publication/slot read returns ``objects``; the inheritance read
    answers ``parents`` (``(table, child count, first children)``) for the tables it is asked
    about. Connection number ``fail_from`` (1-based) and every later one fail. Returns the
    reads, in order: ``"objects"`` or ``("inheritance", names)``."""
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.ui import connect

    reads: list = []
    connects: list = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

        def execute(self, statement, params=None):
            assert "pg_catalog.pg_inherits" in str(statement), statement
            names = sorted((params or {}).get("names") or ())
            reads.append(("inheritance", names))
            rows = [(t, n, list(s)) for t, n, s in parents if t in names]
            return SimpleNamespace(fetchall=lambda: rows)

    class _Engine:
        def connect(self):
            connects.append(1)
            if fail_from is not None and len(connects) >= fail_from:
                raise ConnectionError("the source is unreachable")
            return _Conn()

        def dispose(self):
            pass

    def _read(_connection, *, publication_name, slot_name):
        reads.append("objects")
        return objects

    monkeypatch.setattr(
        connect, "make_source_engine_factory", lambda *_a, **_k: (lambda _cfg: _Engine()))
    monkeypatch.setattr(cdc_pg_slot, "read_pg_replication_objects", _read)
    return reads


def _slot(confirmed=MOVED, **kw):
    """The Full Load's publication (covering app.orders) and its slot at ``confirmed``."""
    base = dict(publication_name="pub", slot_name="slot", confirmed=confirmed,
                publication_tables=frozenset({"app.orders"}))
    base.update(kw)
    return _objects(**base)


def _bodies(notices) -> str:
    return " ".join(body for _tone, _header, body in notices)


# ---------------------------------------------------------------------------
# Decision (2): a Start CloudFormation rolled back counts as started
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status, parameters, started",
    [
        ("CREATE_COMPLETE", NEVER_STARTED, False),
        ("UPDATE_COMPLETE", NEVER_STARTED, False),
        (ROLLED_BACK, NEVER_STARTED, True),  # FAILS WITHOUT THE FIX
        (ROLLED_BACK, STOPPED, True),
        (ROLLED_BACK, RUNNING, True),
        ("UPDATE_COMPLETE", STOPPED, True),
        ("UPDATE_COMPLETE", RUNNING, True),
    ],
)
def test_the_stack_record_counts_a_start_cloudformation_rolled_back(
    monkeypatch, status, parameters, started
) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh()
    _probe(monkeypatch, state, parameters, status)
    # The status is the phase probe's own (the same describe as the phase), not a new field.
    assert state.cdc_stack_phase_status == status
    assert _cdc_ui._pg_stack_started(state) is started


def test_a_first_start_that_rolled_back_is_never_refused_on_the_slot_it_moved(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX (confirmed[0], pending[0]/[1]): the first Start's update created
    # both connectors, the source streamed (its slot moved past the watermark), the sink
    # failed and CloudFormation rolled the update back -- DeploySink=false, a blank bootstrap.
    # The next Start was refused by the slot-position check, which blamed "another run with
    # the same CDC stack name" and said "nothing of this migration has streamed from it".
    # Debezium continues from the stored offset, so the retry is gapless while the CDC topics
    # still retain that Start's changes (round 4d).
    from dsql_migrator.core.cdc_deployer import CdcDeployError

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    first = _start(
        monkeypatch, state=state, confirmed=AT_START,
        run_raises=CdcDeployError("Stack operation ended in 'UPDATE_ROLLBACK_COMPLETE'."),
    )
    assert isinstance(first["error"], CdcDeployError) and first["kwargs"]  # the update ran
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)  # the job-end re-probe
    assert state.cdc_stack_phase == "infra" and state.cdc_has_committed_offset is False
    _source(monkeypatch, _slot(MOVED))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert ("error", SLOT_REFUSAL) not in _headers(notices)
    assert not [n for n in notices if n[0] == "error"], notices
    assert "nothing of this migration has streamed" not in _bodies(notices)
    assert ui.buttons["Start CDC"].enabled is True
    run = _start(monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]  # the worker went ahead
    # ...and after an app restart: the stack record comes back with the phase probe.
    restored = _restart(_snapshot(state))
    _probe(monkeypatch, restored, NEVER_STARTED, ROLLED_BACK)
    again = _start(monkeypatch, state=restored)
    assert again["error"] is None and again["kwargs"]


@pytest.mark.parametrize("status", ["CREATE_COMPLETE", "UPDATE_COMPLETE"])
def test_a_never_started_stacks_moved_slot_is_still_refused(monkeypatch, status) -> None:
    # Guard: a stack that records no Start -- not stopped, no connectors, no rollback -- is
    # still judged on its first Start, in the dialog and in the worker; a re-snapshot runs
    # there, so it stays offered.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, status)
    _source(monkeypatch, _slot(MOVED))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert ("error", SLOT_REFUSAL) in _headers(notices)
    assert ui.buttons["Start CDC"].enabled is False
    assert REMEDY in ui.buttons
    # Its copy claims only what the stack records: no Start of ITS connectors, a rolled-back
    # one included -- not that nothing of the migration ever streamed.
    (body,) = [b for _t, h, b in notices if h == SLOT_REFUSAL]
    assert (
        "this CDC stack records no Start of its connectors (not even one that rolled back), "
        "so none of them has streamed from it: another run with the same CDC stack name"
    ) in body
    assert "nothing of this migration" not in body
    run = _start(monkeypatch, state=state)
    assert SLOT_REFUSAL in str(run["error"]) and run["kwargs"] == {}


@pytest.mark.parametrize(
    "objects, header",
    [
        (_slot(None, slot_present_any_database=False, slot_usable=False), SLOT_MISSING),
        (_slot(AT_START, publication_present=False, publication_tables=frozenset()),
         PUBLICATION_MISSING),
    ],
    ids=["slot-missing", "publication-missing"],
)
def test_a_rolled_back_stacks_block_names_the_started_stack_routes(
    monkeypatch, objects, header
) -> None:
    # FAILS WITHOUT THE FIX (pending[0]): the rolled-back stack was "never started", so the
    # block offered "Re-snapshot every table instead" -- whose snapshot Debezium skips once
    # the rolled-back Start committed an offset -- and named no gapless route.
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, ROLLED_BACK)
    reads = _source(monkeypatch, objects)
    ui, notices, _events = _dialog(monkeypatch, state)
    ((tone, shown, body),) = [n for n in notices if n[0] == "error"]
    assert shown == header
    assert body.endswith(_pg_block_route(state, header))
    assert "so a re-snapshot cannot be relied on here" in body
    assert REMEDY not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False
    assert reads == ["objects"]  # no re-snapshot is judged on a started stack
    run = _start(monkeypatch, state=state)
    assert run["kwargs"] == {} and header in str(run["error"])
    assert _pg_block_route(state, header) in str(run["error"])


# ---------------------------------------------------------------------------
# confirmed[1]: the re-snapshot remedy runs the checks of its own route first
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "objects, header",
    [
        (_slot(MOVED), SLOT_REFUSAL),
        (_slot(None, slot_present_any_database=False, slot_usable=False), SLOT_MISSING),
        (_slot(AT_START, publication_present=False, publication_tables=frozenset()),
         PUBLICATION_MISSING),
    ],
    ids=["slot-moved", "slot-missing", "publication-missing"],
)
def test_the_re_snapshot_remedy_is_withheld_when_its_snapshot_is_refused(
    monkeypatch, objects, header
) -> None:
    # FAILS WITHOUT THE FIX (confirmed[1]): app.orders is an inheritance parent (a legitimate
    # Full load + CDC capture: read FROM ONLY, and the gapless Start takes no snapshot). The
    # dialog offered the re-snapshot for each block of the gapless Start, rendered "Start CDC
    # is unlocked" on its click, and the Start job then refused that snapshot.
    from dsql_migrator.core.cdc_pg_slot import INHERITANCE_SNAPSHOT_HEADER
    from dsql_migrator.ui.data_migration._cdc_ui import PG_RESNAPSHOT_REFUSED_LEAD

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    reads = _source(monkeypatch, objects, parents=(("app.orders", 1, ("app.orders_2026",)),))
    ui, notices, _events = _dialog(monkeypatch, state)
    remedy = ui.buttons.get(REMEDY)
    if remedy is not None:  # the code before this step offered it...
        remedy.on_click()  # ...and its click unlocked Start
    assert "Start CDC is unlocked" not in _bodies(notices)
    assert remedy is None
    assert ui.buttons["Start CDC"].enabled is False
    assert _headers(notices) == [("error", header), ("error", INHERITANCE_SNAPSHOT_HEADER)]
    body = notices[1][2]
    assert PG_RESNAPSHOT_REFUSED_LEAD == (
        "Re-snapshot every table is not offered here: a Start that re-snapshots is refused too.")
    assert body.startswith(
        f"{PG_RESNAPSHOT_REFUSED_LEAD} app.orders has 1 inheritance (INHERITS) child table: "
        "app.orders_2026. This Start sends snapshot.mode=initial")
    # G25 round 6: unticking the parent is named only while the table picker can change the
    # set; this harness's table lock cannot be read, so none is named (the open-picker case:
    # test_g25_pg_r6.py).
    assert "or untick" not in body
    assert ("inheritance", ["app.orders"]) in reads  # the remedy's own check ran
    assert state.cdc_start_mode() == "auto"  # nothing was changed
    # The worker stays the backstop: the gapless Start is still refused.
    run = _start(monkeypatch, state=state)
    assert header in str(run["error"]) and run["kwargs"] == {}


def test_the_re_snapshot_remedy_is_withheld_for_a_slot_it_cannot_use(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: an INVALIDATED slot (wal_status='lost') is "no usable slot" to the
    # gapless Start, which offered the re-snapshot -- whose Start reuses that slot's name and
    # is refused for it.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    lost = _slot(None, slot_present_any_database=True, slot_usable=False)
    _source(monkeypatch, lost)
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", SLOT_MISSING), ("error", SLOT_UNUSABLE)]
    assert "Drop it, or use a different CDC stack name" in notices[1][2]
    assert REMEDY not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False
    # What that remedy unlocked before: a Start the job refuses.
    state.set_cdc_start_mode("manual")
    run = _start(monkeypatch, state=state)
    assert SLOT_UNUSABLE in str(run["error"]) and run["kwargs"] == {}


def test_a_re_snapshot_refused_for_the_same_reason_is_withheld_without_a_second_notice(
    monkeypatch,
) -> None:
    # FAILS WITHOUT THE FIX: CDC only always re-snapshots, so the remedy unlocked the very
    # Start the block had just refused (a publication that misses a selected table).
    state = _fresh(_cdc_only())
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(AT_START, publication_tables=frozenset({"app.other"})))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [("error", COVERAGE)]
    assert REMEDY not in ui.buttons
    assert ui.buttons["Start CDC"].enabled is False


def test_the_re_snapshot_remedy_is_offered_when_its_checks_pass_and_its_start_runs(
    monkeypatch,
) -> None:
    # Guard: no inheritance parent, the slot simply moved on a never-started stack -- the
    # remedy is offered after its own checks ran, its click unlocks Start, and the job runs.
    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    reads = _source(monkeypatch, _slot(MOVED))
    ui, notices, _events = _dialog(monkeypatch, state)
    assert _headers(notices) == [
        ("error", SLOT_REFUSAL), ("info", "Re-snapshot instead — gapless, no second Full Load")]
    assert reads == ["objects", "objects", ("inheritance", ["app.orders"])]
    ui.buttons[REMEDY].on_click()
    assert ui.buttons["Start CDC"].enabled is True and state.cdc_start_mode() == "manual"
    assert notices[-1][:2] == ("success", "Will re-snapshot every selected table")
    assert notices[-1][2].endswith("Start CDC is unlocked.")
    run = _start(monkeypatch, state=state)
    assert run["error"] is None and run["kwargs"]


def test_a_re_snapshot_check_that_cannot_read_the_source_keeps_the_remedy_and_says_so(
    monkeypatch,
) -> None:
    # A source lost between the two reads: unreadable is not "refused" (the pre-flight's own
    # rule), so the remedy stays -- and the dialog says that the re-snapshot route's own
    # checks did not run, once (round 4d: not that the publication and slot checks did not,
    # beside the block those checks produced -- tests/test_g25_pg_r4d.py).
    from dsql_migrator.core.activity_log import ActivityStatus
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _fresh()
    _probe(monkeypatch, state, NEVER_STARTED, "CREATE_COMPLETE")
    _source(monkeypatch, _slot(MOVED), fail_from=2)
    ui, notices, events = _dialog(monkeypatch, state)
    assert _headers(notices)[:2] == [
        ("warning", _cdc_ui.PG_RESNAPSHOT_NOT_CHECKED_HEADER), ("error", SLOT_REFUSAL)]
    assert "the connection failed: ConnectionError" in notices[0][2]
    assert REMEDY in ui.buttons
    (line,) = [e for e in events if e[0] == _cdc_ui.PG_RESNAPSHOT_NOT_CHECKED_ACTION]
    assert line[1]["status"] is ActivityStatus.WARNING
    assert not [e for e in events if e[0] == _cdc_ui.PG_CHECKS_NOT_RUN_ACTION]


# ---------------------------------------------------------------------------
# pending[2]: the publication diagnosis is true whatever the stack's state
# ---------------------------------------------------------------------------


def test_the_publication_diagnosis_names_the_step_for_each_stack_state(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (pending[2]): the diagnosis said "Stop CDC ..., then open Start
    # CDC: its dialog names the gapless routes" -- but the failure usually ends in a rollback,
    # which leaves nothing to stop, and that rolled-back stack's dialog offered "Re-snapshot
    # every table instead" and named no route.
    from dsql_migrator.core.cdc_deployer import _diagnose_connector_log
    from dsql_migrator.ui.data_migration._cdc_ui import _pg_block_route

    guidance = _diagnose_connector_log("dsql-cdc-stack-debezium-source", _PUBLICATION_GONE_TAIL)
    assert guidance is not None
    # G25 round 5: the one route of every stack this failure is read on (started before) --
    # new topics first -- named here by the route rule itself.
    assert (
        "If the CDC step still shows the connectors, Stop CDC first (or Clean up leftover "
        "connector on an incomplete pipeline); a Start whose CloudFormation update rolled back "
        "left none to remove. This stack's connectors were started before"
    ) in guidance
    assert REMEDY not in guidance
    assert "Do NOT create a replication slot by hand" in guidance
    routes = ("Re-run Full Load on the Full Load step choosing 'Drop & reload'",
              "Delete CDC infrastructure and deploy it again")
    # The Start dialog it sends the operator to: connectors stopped (DeploySink=true), or a
    # first Start that rolled back, or a re-Start after Stop that rolled back.
    for status, parameters in (
        ("UPDATE_COMPLETE", STOPPED), (ROLLED_BACK, NEVER_STARTED), (ROLLED_BACK, STOPPED),
    ):
        state = _fresh()
        _probe(monkeypatch, state, parameters, status)
        _source(monkeypatch, _slot(AT_START, publication_present=False,
                                   publication_tables=frozenset()))
        ui, notices, _events = _dialog(monkeypatch, state)
        ((_tone, header, body),) = [n for n in notices if n[0] == "error"]
        assert header == PUBLICATION_MISSING, (status, parameters)
        assert body.endswith(_pg_block_route(state, header)), (status, parameters)
        assert REMEDY not in ui.buttons, (status, parameters)
        for route in routes:
            assert route in guidance and route in body, (status, parameters, route)
