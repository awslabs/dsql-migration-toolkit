# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G26 item [0]: the Full load only completion notice on an UNREAD CDC state.

Full load only never arms the stack-phase probe (the plan-level discovery arms for a CDC type,
or while a restored session's exclusions are still being classified), so after an app restart --
or a probe that failed -- the CDC state is unread, and the notice read that as never started:
'set the migration type to "CDC only" — it streams from this Full Load's watermark onto the
already-loaded target (no re-snapshot) ... CDC streaming infrastructure isn't deployed yet'
(PostgreSQL: '... so there is no retained WAL to stream from ... "CDC only" can still do it by
re-snapshotting every selected table ...'). On a stack that streamed, or rolled back a Start, each
of those is false. Unread, the notice now states nothing about the stack: the CDC step reads it
once the type is switched, what a stack never started does comes only under that condition, and
rule E's Drop & reload advice holds either way. Once read, the notice is unchanged.

Every read goes through the REAL stack-phase probe over one describe; a restart is the REAL
session snapshot round trip. Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from dsql_migrator.ui.data_migration import MigrationType

from tests.test_g12_table_set_after_load import _grown_and_loaded, _loaded
from tests.test_g12_table_set_after_load import _screen as _flo_screen
from tests.test_g25_fl_r6 import NEVER_STARTED, ROLLED_BACK, STOPPED, _probe
from tests.test_g25_fl_r7 import _deployed, _in_order, _wm_job

_HEADER = "Full Load complete — want continuous replication (CDC) next?"
_SELECTED = ["app.a", "app.b"]
ABSENT = "absent"  # read: the describe finds no cdc-stack
# What an unread notice says in place of a stack fact.
_READS_IT = (
    "set the migration type to \"CDC only\": its CDC step reads this migration's CDC "
    "infrastructure and says what Start CDC will do there (deploy the infrastructure first if "
    "there is none)."
)
_UNLESS = "Unless this migration's CDC stack was started before, "
# The never-started facts the notice stated on an unread state before G26.
_NOT_DEPLOYED = "CDC streaming infrastructure isn't deployed yet"
_MYSQL_NEVER_STARTED = "\"CDC only\" — it streams from this Full Load's watermark"
_PG_NEVER_STARTED = (
    "so there is no retained WAL to stream from",
    "\"CDC only\" can still do it by re-snapshotting every selected table",
)
_DROP_RELOAD = (
    "If this run appended to rows an earlier load left on the target, first Re-run Full Load "
    "choosing 'Drop & reload': an append keeps those rows as they were, and CDC from a "
    "watermark carries only the changes after it."
)
# A MySQL Full Load whose watermark is newer than the stopped stack's last start point.
_NEWER_JOB = ("mysql-bin.000009", 4)
_STOPPED_EARLIER = _deployed("mysql-bin.000003", 100)


def _read(monkeypatch, state, stack) -> None:
    """The REAL stack-phase probe over one describe of ``stack`` (``ABSENT``: none found)."""
    if stack != ABSENT:
        _probe(monkeypatch, state, stack)
        return
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


def _body(ui) -> str:
    texts = ui.texts
    return texts[texts.index(_HEADER) + 1]


def _notice(monkeypatch, *, job=None, stack=None, pg=False, selected=_SELECTED,
            prepare=None) -> str:
    """The Full load only completion notice's body from the REAL screen (``stack`` None:
    never read, as after an app restart)."""
    s = _flo_screen(monkeypatch, job=job or _loaded(), selected=selected, pg=pg)
    if stack is not None:
        _read(monkeypatch, s.state, stack)
    if prepare is not None:
        prepare(s.state)
    s.content(lambda: None)
    return _body(s.ui)


def _across_a_restart(monkeypatch, *, job, stack, pg=False) -> "tuple[str, str]":
    """The notice as the app read ``stack``, then on the session the app restores after a
    restart: the snapshot every store writes (capture, JSON), applied to a fresh screen."""
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration._cdc_status import cdc_stack_has_streamed
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    first = _flo_screen(monkeypatch, job=job, selected=_SELECTED, pg=pg)
    _read(monkeypatch, first.state, stack)
    assert cdc_stack_has_streamed(first.state)  # the premise: the app knew it streamed
    first.content(lambda: None)
    raw = capture_session_snapshot(
        "s1", first.session, EvaluationState(), SchemaConversionState(), first.state
    ).model_dump_json()
    second = _flo_screen(monkeypatch, job=job, selected=_SELECTED, pg=pg)
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(raw), second.session, EvaluationState(),
        SchemaConversionState(), second.state,
    )
    # Nothing the probe read is persisted, and Full load only arms no probe to read it again.
    assert second.state.cdc_stack_phase_checked is False
    assert cdc_stack_has_streamed(second.state) is False
    second.content(lambda: None)
    return _body(first.ui), _body(second.ui)


def _assert_states_no_stack_fact(body: str, *, pg: bool) -> None:
    assert _NOT_DEPLOYED not in body
    assert _READS_IT in body
    if pg:
        assert body.startswith("This run created no replication slot (a Full load only run "
                               "never does).")
        for phrase in _PG_NEVER_STARTED:
            assert phrase not in body, phrase
        assert (_UNLESS + "Start CDC re-snapshots every selected table first (nothing is "
                "lost, but the source is read again).") in body
        assert "no re-snapshot" not in body
    else:
        assert _MYSQL_NEVER_STARTED not in body
        assert (_UNLESS + "CDC streams from this Full Load's watermark onto the already-loaded "
                "target (no re-snapshot).") in body
        # Rule E holds either way: CDC from this watermark (a stack never started, or a new
        # stack after a refused newer start point) carries only the changes after it.
        assert _DROP_RELOAD in body
    # Rule F: the type switch first; what the stack needs is said on the CDC step.
    assert _in_order(body, "set the migration type to \"CDC only\"", "its CDC step reads",
                     "Use the link below to jump to that setting.")
    for claim in ("needs a new stack", "Delete CDC infrastructure", "was started before, so",
                  "has streamed, or rolled back a Start, so", "continues from"):
        assert claim not in body, claim


# ---------------------------------------------------------------------------
# 1. Unread: no claim about the stack
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pg", [False, True], ids=["mysql", "postgresql"])
def test_an_unread_cdc_state_states_no_never_started_fact(monkeypatch, pg) -> None:
    # FAILS WITHOUT THE FIX: the never-started notice, "CDC streaming infrastructure isn't
    # deployed yet" included, on a state nothing has read.
    job = _wm_job(*_NEWER_JOB) if not pg else _loaded()
    _assert_states_no_stack_fact(_notice(monkeypatch, job=job, pg=pg), pg=pg)


@pytest.mark.parametrize("pg", [False, True], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("stack", ["stopped", "rolled back"])
def test_after_an_app_restart_a_stack_that_streamed_gets_no_never_started_claim(
    monkeypatch, stack, pg
) -> None:
    # FAILS WITHOUT THE FIX (the follow-up's scenario): before the restart the notice names the
    # stack that streamed or rolled back a Start; after it -- the same stack, the same Full
    # Load -- it said that CDC only streams from this watermark onto the loaded target and that
    # the infrastructure is not deployed yet (PostgreSQL: no retained WAL, re-snapshot).
    record = {"stopped": _STOPPED_EARLIER if not pg else STOPPED, "rolled back": ROLLED_BACK}
    job = _wm_job(*_NEWER_JOB) if not pg else _loaded()
    before, after = _across_a_restart(monkeypatch, job=job, stack=record[stack], pg=pg)
    if pg:
        assert "This migration's CDC stack was started before" in before
    else:
        assert "This migration's CDC stack has streamed, or rolled back a Start" in before
    _assert_states_no_stack_fact(after, pg=pg)


@pytest.mark.parametrize("pg", [False, True], ids=["mysql", "postgresql"])
def test_a_failed_probe_is_an_unread_state(monkeypatch, pg) -> None:
    # FAILS WITHOUT THE FIX: a probe that could not read the stack (here AccessDenied, from a
    # discovery while the type was a CDC one) left it unread, and the notice called it never
    # started and not deployed.
    body = _notice(monkeypatch, pg=pg, prepare=lambda state: state.set_cdc_probe_error(
        "AccessDenied: cloudformation:DescribeStacks"))
    _assert_states_no_stack_fact(body, pg=pg)


@pytest.mark.parametrize("pg", [False, True], ids=["mysql", "postgresql"])
def test_the_coverage_notice_claims_no_infrastructure_state_unread(monkeypatch, pg) -> None:
    # FAILS WITHOUT THE FIX: the table-set notice (G12) also ended with "CDC streaming
    # infrastructure isn't deployed yet" on an unread state. Its body is unchanged.
    from dsql_migrator.ui.data_migration._models import COVERAGE_GAP_NOTICE_BODY

    grown = {"job": _grown_and_loaded(), "selected": ["app.a", "app.b", "app.c"], "pg": pg}
    unread = _notice(monkeypatch, **grown)
    assert unread.startswith(COVERAGE_GAP_NOTICE_BODY)
    assert _NOT_DEPLOYED not in unread
    # Read, with no cdc-stack: the sentence is true, and stays.
    absent = _notice(monkeypatch, stack=ABSENT, **grown)
    assert absent.startswith(COVERAGE_GAP_NOTICE_BODY)
    assert absent.endswith(" CDC streaming infrastructure isn't deployed yet, so you'll deploy "
                           "it first (usually ~5 min) on the CDC step.")


def test_the_cdc_step_reads_the_state_the_unread_notice_defers_to(monkeypatch) -> None:
    # The unread copy's own promise. Full load only arms no discovery (so nothing reads the
    # stack there); once the type is "CDC only" the plan-level discovery is armed and its run
    # reads the stack -- here the REAL probe over a rolled-back stack.
    from dsql_migrator.ui import data_migration as dm

    s = _flo_screen(monkeypatch, job=_loaded(), selected=_SELECTED)
    armed: list = []
    s.ui.timer = lambda _interval, callback, **_k: armed.append(callback)
    reads: list = []

    def _discover(state, _session):
        reads.append(state)
        _read(monkeypatch, state, ROLLED_BACK)

    monkeypatch.setattr(dm, "_ensure_cdc_controller", _discover)
    monkeypatch.setattr(dm, "adopt_deployed_exclusions_if_safe", lambda *_a, **_k: None)
    monkeypatch.setattr(dm, "derive_cdc_vpc_from_source", lambda *_a, **_k: False)
    s.content(lambda: None)
    assert not [cb for cb in armed if getattr(cb, "__name__", "") == "_discover_cdc"]
    assert s.state.cdc_stack_phase_checked is False

    s.session.set_migration_type(MigrationType.CDC_ONLY)
    s.content(lambda: None)
    discovery = [cb for cb in armed if getattr(cb, "__name__", "") == "_discover_cdc"]
    assert len(discovery) == 1
    asyncio.run(discovery[0]())
    assert reads == [s.state]
    assert s.state.cdc_stack_phase_checked is True
    assert s.state.cdc_stack_phase_status == "UPDATE_ROLLBACK_COMPLETE"


# ---------------------------------------------------------------------------
# 2. Read: unchanged
# ---------------------------------------------------------------------------


def test_read_never_started_mysql_keeps_todays_notice(monkeypatch) -> None:
    job = _wm_job(*_NEWER_JOB)
    for stack, deployed in ((ABSENT, False), (NEVER_STARTED, True)):
        body = _notice(monkeypatch, job=job, stack=stack)
        assert body.startswith("To keep the target in sync with ongoing source changes, set "
                               "the migration type to " + _MYSQL_NEVER_STARTED)
        assert "(no re-snapshot)" in body and _DROP_RELOAD in body
        assert (_NOT_DEPLOYED in body) is not deployed, stack
        assert _READS_IT not in body and _UNLESS not in body


def test_read_never_started_postgresql_keeps_todays_notice(monkeypatch) -> None:
    for stack, deployed in ((ABSENT, False), (NEVER_STARTED, True)):
        body = _notice(monkeypatch, stack=stack, pg=True)
        assert body.startswith("This run created no replication slot, so there is no retained "
                               "WAL to stream from")
        assert _PG_NEVER_STARTED[1] in body
        assert (_NOT_DEPLOYED in body) is not deployed, stack
        assert _READS_IT not in body and _UNLESS not in body


@pytest.mark.parametrize("stack", ["stopped", "rolled back"])
def test_read_streamed_mysql_keeps_the_new_stack_route(monkeypatch, stack) -> None:
    record = {"stopped": _STOPPED_EARLIER, "rolled back": ROLLED_BACK}[stack]
    body = _notice(monkeypatch, job=_wm_job(*_NEWER_JOB), stack=record)
    assert body.startswith("This migration's CDC stack has streamed, or rolled back a Start, "
                           "so CDC from this Full Load's watermark needs a new stack")
    assert _in_order(body, "first Re-run Full Load choosing 'Drop & reload'",
                     "set the migration type to \"CDC only\"",
                     "Delete CDC infrastructure and deploy it again", "then Start CDC")
    assert _NOT_DEPLOYED not in body and _READS_IT not in body
    assert _MYSQL_NEVER_STARTED not in body


def test_read_streamed_mysql_not_newer_keeps_the_continuing_notice(monkeypatch) -> None:
    body = _notice(monkeypatch, job=_wm_job("mysql-bin.000003", 100), stack=_STOPPED_EARLIER)
    assert ("so CDC continues from where streaming stopped (gapless only while the CDC topics "
            "still retain what the sink had not applied)") in body
    assert _NOT_DEPLOYED not in body and _READS_IT not in body


@pytest.mark.parametrize("stack", ["stopped", "rolled back"])
def test_read_streamed_postgresql_keeps_the_slot_notice(monkeypatch, stack) -> None:
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route

    body = _notice(monkeypatch, stack={"stopped": STOPPED, "rolled back": ROLLED_BACK}[stack],
                   pg=True)
    assert ("This migration's CDC stack was started before, so Start CDC on its CDC step "
            "continues from that stack's replication slot if its source connector streamed "
            "(otherwise Debezium snapshots every selected table first)") in body
    assert pg_cdc_route(None, started=True, cdc_only=True) in body
    assert _NOT_DEPLOYED not in body and _READS_IT not in body
    for phrase in _PG_NEVER_STARTED:
        assert phrase not in body, phrase
