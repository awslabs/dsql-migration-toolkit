# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G25 round 7, package FL: the Full Load side of the new-start-point rules (A, A2, B, C, D).

* Validation's recovery runbook reads the CDC stack whatever the migration type, and treats a
  CDC state not read yet (a session restored after an app restart) the way the Full Load
  step's gate does: never the shorter, never-streamed order before the state is read. CDC
  only switches the type first; Full load only has no CDC steps; a stack that never streamed
  has no Stop CDC step.
* Every remedy that sends the operator to a Full Load re-run on a CDC stack that has streamed
  names Delete CDC infrastructure and deploy it again first (rule B), or a route that keeps
  the original start point; no copy promises a gapless handoff that depends on the CDC topics
  without saying so (rule D).
* A narrowed PostgreSQL publish list names its fix first and the new start point after it
  (rule C): the G17 addition refusal and the prerequisite row.
* The PostgreSQL publication/slot prerequisite row defers to the Start CDC dialog on a stack
  whose connectors were started before (no re-snapshot, no "run as Full load + CDC", no
  "nothing is lost").
* The Full load only completion notice: on MySQL a CDC stack that has streamed refuses a newer
  start point (A2), so CDC from a re-run's watermark needs a new stack; on PostgreSQL CDC only
  continues from the stack's own slot.

Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timezone

import pytest

from dsql_migrator.core.models import (
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteStatus,
    SourceType,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

from tests.test_g12_table_set_after_load import _c, _job, _Jobs, _loaded
from tests.test_g12_table_set_after_load import _screen as _flo_screen
from tests.test_g25_fl_r6 import NEVER_STARTED, ROLLED_BACK, STOPPED, _probe
from tests.test_ui_validation import _CopyUi, _drift_na, _release_summary

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
CDC_ONLY = MigrationType.CDC_ONLY

_DELETE = "click Delete CDC infrastructure, and deploy it again"
_TS = datetime(2026, 10, 1, tzinfo=timezone.utc)
_SWITCH = "Set the migration type to Full load + CDC"


def _runbook(**kwargs):
    from dsql_migrator.ui.validation import _render_recovery_section

    ui = _CopyUi()
    _render_recovery_section(
        ui, _release_summary(is_match=False, mismatched=1), _drift_na(), **kwargs
    )
    return ui


def _steps(ui) -> list:
    """The numbered steps, in order (a number label, then its text)."""
    texts = ui.texts[ui.texts.index("Steps to recover") + 1 :]
    numbers = texts[0::2]
    assert numbers == [str(n) for n in range(1, len(numbers) + 1)], numbers
    return texts[1::2]


def _in_order(text: str, *words: str) -> bool:
    positions = [text.index(w) for w in words]
    return positions == sorted(positions)


# ---------------------------------------------------------------------------
# 1. Validation's recovery runbook
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY, None], ids=["flc", "cdc_only", "unknown"])
def test_an_unread_cdc_state_never_gets_the_never_streamed_order(mtype) -> None:
    # FAILS WITHOUT THE FIX (#3/#4): a restored session (the CDC state not read yet) got
    # "Start/Re-run ... then Start CDC from the new snapshot's start point" -- the run the Full
    # Load step then refuses, first as unread and then as streamed.
    ui = _runbook(cdc_state_unread=True, migration_type=mtype)
    body = ui.body()
    assert _DELETE in body
    assert "has not been read yet" in body and "this card then shows the steps" in body
    # Not claimed: the stack is not known to have streamed.
    assert "This migration's CDC stack has streamed" not in body
    assert "Start/Re-run to backfill" not in body
    steps = " ".join(_steps(ui))
    assert _in_order(steps, "Stop CDC first", _DELETE, "'Drop & reload'", "Start CDC",
                     "Re-run validation")


def test_a_cdc_only_stack_that_has_streamed_switches_the_type_after_new_topics() -> None:
    # FAILS WITHOUT THE FIX: _cdc_stack_streamed was False for CDC only, whose stack has
    # necessarily streamed, so it got "Stop CDC, then Re-run Full Load, then Start CDC".
    ui = _runbook(cdc_stack_streamed=True, migration_type=CDC_ONLY)
    steps = " ".join(_steps(ui))
    assert _in_order(steps, _DELETE, _SWITCH, "click Re-run Full Load", "'Drop & reload'",
                     "Start CDC", "Re-run validation")
    assert "This migration's CDC stack has streamed" in ui.body()


def test_a_cdc_only_stack_that_never_streamed_switches_the_type_without_new_topics() -> None:
    # G25 round 8 (rule E): the re-run is a Drop & reload -- CDC continues from its new start
    # point, and an append keeps rows older than it (test_g25_fl_r8).
    ui = _runbook(migration_type=CDC_ONLY)
    steps = _steps(ui)
    joined = " ".join(steps)
    assert _in_order(joined, _SWITCH, "click Re-run Full Load", "'Drop & reload'",
                     "Start CDC", "Re-run validation")
    assert "Delete CDC infrastructure" not in joined and "Stop CDC" not in joined


@pytest.mark.parametrize(
    "kwargs",
    [{}, {"cdc_stack_streamed": True}, {"cdc_state_unread": True}],
    ids=["never", "streamed", "unread"],
)
def test_full_load_only_has_no_cdc_steps(kwargs) -> None:
    # FAILS WITHOUT THE FIX: step 4 told a Full load only migration to "Continue to CDC and
    # Start CDC", a phase it does not have. Its re-run is not gated.
    ui = _runbook(migration_type=FLO, **kwargs)
    body = ui.body()
    assert "Re-run Full Load to backfill the gap" in body
    assert "Re-run Full Load + CDC" not in body
    steps = _steps(ui)
    assert len(steps) == 3
    joined = " ".join(steps)
    assert "Start/Re-run to backfill" in joined and "Re-run validation" in joined
    for word in ("CDC", "Delete", "Continue to"):
        assert word not in joined, word


@pytest.mark.parametrize("mtype", [FLC, None], ids=["flc", "unknown"])
def test_a_stack_that_never_streamed_has_no_stop_cdc_step(mtype) -> None:
    # FAILS WITHOUT THE FIX: "If CDC is running, ... click Stop CDC first" -- a running stack
    # has streamed, so it takes the other branch; here the step was dead.
    ui = _runbook(migration_type=mtype)
    steps = " ".join(_steps(ui))
    assert "Stop CDC" not in steps and "If CDC is running" not in steps
    assert _in_order(steps, "click Re-run Full Load", "'Drop & reload'",
                     "Continue to CDC and Start CDC", "Re-run validation")


def test_the_stack_record_is_read_whatever_the_type(monkeypatch) -> None:
    from dsql_migrator.ui.validation import _cdc_stack_streamed, _cdc_state_unread

    from tests.test_g25_fl_r6 import _state

    for mtype in (FLC, FLO, CDC_ONLY):
        stopped = _state(monkeypatch, STOPPED, mtype=mtype)
        assert _cdc_stack_streamed(stopped) is True and _cdc_state_unread(stopped) is False
        rolled = _state(monkeypatch, ROLLED_BACK, mtype=mtype)
        assert _cdc_stack_streamed(rolled) is True
        never = _state(monkeypatch, NEVER_STARTED, mtype=mtype)
        assert _cdc_stack_streamed(never) is False and _cdc_state_unread(never) is False
        unread = _state(mtype=mtype)
        assert _cdc_stack_streamed(unread) is False and _cdc_state_unread(unread) is True


def _validation_screen(monkeypatch, *, mtype, probe=None):
    """The REAL validation screen over a failing report, as after an app restart: the
    session's type is set and the CDC state is read only when ``probe`` is given."""
    from tests.test_ui_validation import (
        _failing_report_for_render,
        _g1_screen,
        _ModeAwareValidator,
        _TimerUi,
    )

    ui = _TimerUi()
    s = _g1_screen(monkeypatch, _ModeAwareValidator(), ledger_tables=(), ui=ui)
    s.session.set_migration_type(mtype)
    if probe is not None:
        _probe(monkeypatch, s.mstate, probe)
    s.vstate.set_result(_failing_report_for_render())
    s.content(lambda: None)
    return ui.body()


def test_the_screen_threads_the_type_and_an_unread_state_to_the_runbook(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (#3): a restored Full load + CDC session got the never-streamed
    # order before the Data Migration step read the stack.
    unread = _validation_screen(monkeypatch, mtype=FLC)
    assert "Steps to recover" in unread and _DELETE in unread
    assert "has not been read yet" in unread
    read_never = _validation_screen(monkeypatch, mtype=FLC, probe=NEVER_STARTED)
    assert "Steps to recover" in read_never and _DELETE not in read_never
    assert "Continue to CDC and Start CDC" in read_never
    stopped_cdc_only = _validation_screen(monkeypatch, mtype=CDC_ONLY, probe=STOPPED)
    assert _in_order(stopped_cdc_only, _DELETE, _SWITCH)
    flo = _validation_screen(monkeypatch, mtype=FLO)
    assert "Re-run Full Load to backfill the gap" in flo and _DELETE not in flo


def test_extra_rows_are_cleared_by_a_reload_that_keeps_the_start_point() -> None:
    # FAILS WITHOUT THE FIX: "re-run the Full Load and choose to DROP that table" -- a new
    # start point, refused on a CDC stack that has streamed.
    ui = _CopyUi()
    from dsql_migrator.ui.validation import _render_recovery_section

    _render_recovery_section(
        ui, _release_summary(is_match=False, mismatched=1, extra=1), _drift_na()
    )
    body = ui.body()
    assert "re-run the Full Load and choose to DROP" not in body
    assert _in_order(body, "'Reload specific tables…', with CDC stopped", "Drop & reload",
                     "keeps the original start point", "With CDC only, switch the migration "
                     "type to Full load + CDC first")


def test_cdc_only_excluded_values_put_new_topics_first() -> None:
    # FAILS WITHOUT THE FIX: "migrate the table with a Full load + CDC migration" -- a new
    # start point on a stack CDC wrote through, refused until new CDC topics.
    from dsql_migrator.ui.validation import excluded_values_remedy

    state = DataMigrationState()
    state.migration_type = CDC_ONLY
    text = excluded_values_remedy(state, _Jobs({}))
    assert _in_order(text, "Delete CDC infrastructure and deploy it again",
                     "switch the migration type to Full load + CDC", "Drop & reload",
                     "Start CDC")


def test_the_deferred_sequence_sync_points_at_how_to_recover() -> None:
    from dsql_migrator.ui import validation

    source = inspect.getsource(validation.build_validation_screen)
    assert "re-run Full Load for a standing gap" not in source
    assert "follow How to recover: on a" in source
    assert "infrastructure and deploy it again), re-validate" in source


# ---------------------------------------------------------------------------
# 2. Decision C: a narrowed publish list names its fix, then the new start point
# ---------------------------------------------------------------------------


def test_the_g17_publish_list_refusal_names_the_fix_before_the_re_run() -> None:
    # FAILS WITHOUT THE FIX (#9): "Re-run all tables, or untick it" -- that re-run reuses
    # the narrowed publication, and Start CDC then refuses it anyway.
    from dsql_migrator.core.cdc_pg_slot import publish_list_fix
    from dsql_migrator.ui.data_migration._full_load_engine import PG_DML

    text = PG_DML.format(pub="dsqlmig_pub_s", fix=publish_list_fix("dsqlmig_pub_s"))
    assert _in_order(text, "does not publish every change type",
                     "ALTER PUBLICATION \"dsqlmig_pub_s\" SET (publish = 'insert, update, "
                     "delete')", "first, then Re-run all tables",
                     "changes committed while the list was narrowed are not in CDC's stream")
    assert "untick" not in text and "\n" not in text


def test_the_publish_list_prerequisite_names_the_route_after_its_fix() -> None:
    # FAILS WITHOUT THE FIX: the remediation named only the fix -- once the list is set back
    # the row passes and the Start CDC block no longer fires, so a Start from the Full Load's
    # slot streamed without the changes committed while it was narrowed.
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects

    from tests.test_g17_full_load_and_cdc_additions import _pg_facts, _tbl

    row = check_cdc_replication_objects(
        _pg_facts(publication_publishes_all_dml=False), [_tbl("app.a"), _tbl("app.b")],
        provisions_replication=False,
    )
    assert row.status is PrerequisiteStatus.FAIL and row.required
    assert _in_order(row.remediation, "ALTER PUBLICATION", "first",
                     "does not bring back the changes committed while it was narrowed",
                     "Re-run Full Load", "'Drop & reload'", "Start CDC")


# ---------------------------------------------------------------------------
# 3. The PostgreSQL publication/slot row on a stack whose connectors were started before
# ---------------------------------------------------------------------------

_NEVER_STARTED_COPY = (
    "re-snapshot option",
    "Nothing is lost",
    'run this migration as "Full load + CDC"',
    "FRESH SNAPSHOT",
    "takes a fresh snapshot",
    "Re-run the Full Load under",
)


@pytest.mark.parametrize("resnapshots", [False, True])
@pytest.mark.parametrize(
    "facts",
    [
        {"publication_present": False},
        {"publication_tables": frozenset({"app.a"})},
        {"publication_publishes_all_dml": False},
        {"slot_usable": False},
        {},
    ],
    ids=["no-publication", "missing-table", "narrowed", "no-slot", "present"],
)
def test_a_started_stack_defers_the_row_to_the_start_cdc_dialog(facts, resnapshots) -> None:
    # FAILS WITHOUT THE FIX: the row offered a re-snapshot, "run as Full load + CDC" and
    # "nothing is lost" on a stack whose Start continues from its committed offset --
    # contradicting the Start CDC block, and its FAIL opened the "gapless handoff is no longer
    # possible" fork whose two routes are both refused there.
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects

    from tests.test_g17_full_load_and_cdc_additions import _pg_facts, _tbl

    tables = [_tbl("app.a"), _tbl("app.b")]
    row = check_cdc_replication_objects(
        _pg_facts(**facts), tables, provisions_replication=False,
        cdc_start_resnapshots=resnapshots, cdc_stack_started=True,
    )
    assert row.status is PrerequisiteStatus.INFO and row.required is False
    assert not row.resolvable_by_resnapshot
    text = f"{row.detail} {row.remediation or ''}"
    assert "connectors were started before, so Start CDC judges" in text
    assert "new CDC topics first" in text
    for phrase in _NEVER_STARTED_COPY:
        assert phrase not in text, phrase
    # A stack never started keeps today's grading.
    never = check_cdc_replication_objects(
        _pg_facts(**facts), tables, provisions_replication=False,
        cdc_start_resnapshots=resnapshots,
    )
    assert "connectors were started before" not in never.detail


def test_the_checker_threads_the_stack_record_to_the_row() -> None:
    from dsql_migrator.core.models import PrerequisiteCheckRequest
    from dsql_migrator.core.prerequisites import PrerequisiteChecker

    from tests.test_prerequisites import (
        _FakeMsk,
        _FakeSource,
        _FakeTarget,
        _pg_facts_healthy,
        _table,
    )

    facts = _pg_facts_healthy(
        replica_identity={"app.orders": "d"}, unlogged={"app.orders": False},
        checked_publication_name="dsqlmig_pub_s", checked_slot_name="dsqlmig_s",
        publication_present=False,
    )

    def _row(**kwargs):
        checker = PrerequisiteChecker(
            source_probe=_FakeSource(cdc_facts=facts),
            target_probe=_FakeTarget(existing={"app.orders"}),
            msk_probe=_FakeMsk(),
        )
        report = checker.check(
            PrerequisiteCheckRequest(
                mode=MigrationMode.CDC, tables=["app.orders"],
                source_type=SourceType.POSTGRES, provisions_replication=False,
                cdc_stack_name="dsql-cdc-s",
            ),
            tables=[_table("app.orders")],
            **kwargs,
        )
        (row,) = [r for r in report.results
                  if r.check_id is PrerequisiteCheckId.CDC_REPLICATION_OBJECTS]
        return row

    assert _row().status is PrerequisiteStatus.FAIL
    assert _row(cdc_stack_started=True).status is PrerequisiteStatus.INFO


def test_the_inheritance_fix_on_a_started_stack_puts_new_topics_before_its_full_load() -> None:
    # Its own fix is a Full load + CDC run -- a new start point, refused on a stack started
    # before until new CDC topics -- so the route follows it there, as in the Start CDC block.
    from dsql_migrator.core.cdc_pg_slot import INHERITANCE_SNAPSHOT_HEADER, pg_cdc_route
    from dsql_migrator.core.prerequisites_postgres import (
        check_inheritance_children,
        check_postgres_cdc_prerequisites,
    )

    from tests.test_g17_full_load_and_cdc_additions import _pg_facts, _tbl

    facts = _pg_facts(inheritance_children={"app.a": (2, ("app.a_2025", "app.a_2026"))})
    route = pg_cdc_route(INHERITANCE_SNAPSHOT_HEADER, started=True, cdc_only=None)
    started = check_inheritance_children(_tbl("app.a"), facts, snapshots=True, started=True)
    assert started.status is PrerequisiteStatus.FAIL
    assert started.remediation.endswith(route) and "Delete CDC infrastructure" in route
    never = check_inheritance_children(_tbl("app.a"), facts, snapshots=True)
    assert "Delete CDC infrastructure" not in never.remediation
    # The wrapper threads the stack record to it.
    rows = check_postgres_cdc_prerequisites(
        facts, [_tbl("app.a")], provisions_replication=False, cdc_start_resnapshots=True,
        cdc_stack_started=True,
    )
    (row,) = [r for r in rows if r.check_id is PrerequisiteCheckId.TABLE_REPLICABLE
              and r.status is PrerequisiteStatus.FAIL]
    assert row.remediation.endswith(route)


@pytest.mark.parametrize("stack, started", [(STOPPED, True), (NEVER_STARTED, False)],
                         ids=["stopped", "never-started"])
def test_the_prerequisite_run_reads_the_stack_record(monkeypatch, stack, started) -> None:
    from dsql_migrator.ui import data_migration as dm

    from tests.test_g16_full_load_and_cdc_narrowing import _fc_screen

    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], pg=True,
                   render=False)
    s.session.set_source_verified(True)
    s.session.set_target_verified(True)
    _probe(monkeypatch, s.state, stack)
    captured: dict = {}
    real_panel = dm._render_prerequisites_panel

    def _spy(ui_, state, run_checks, *a, **k):
        captured["run_checks"] = run_checks
        return real_panel(ui_, state, run_checks, *a, **k)

    calls: list = []

    class _Checker:
        def check(self, request, **kwargs):
            calls.append(kwargs)
            return PrerequisiteReport.build(request.mode, [])

    monkeypatch.setattr(dm, "_render_prerequisites_panel", _spy)
    monkeypatch.setattr(dm, "build_prerequisite_checker", lambda **k: _Checker())
    s.content(lambda: None)
    asyncio.run(captured["run_checks"](MigrationMode.CDC))
    (kwargs,) = calls
    assert kwargs["cdc_stack_started"] is started


# ---------------------------------------------------------------------------
# 4. The Full load only completion notice (the detour's Full Load side)
# ---------------------------------------------------------------------------


def _wm_job(file=None, pos=None):
    job = _job("DONE", [_c("app.a"), _c("app.b")], counts={"app.a": 1, "app.b": 1})
    if file is not None:
        job.watermark = job.watermark.model_copy(
            update={"binlog_file": file, "binlog_position": pos})
    return job


def _deployed(file, pos):
    params, status = STOPPED
    return ({**params, "WatermarkBinlogFile": file, "WatermarkBinlogPos": str(pos)}, status)


def _completion_notice(monkeypatch, *, job, stack, pg=False) -> str:
    s = _flo_screen(monkeypatch, job=job, selected=["app.a", "app.b"], pg=pg)
    if stack is not None:
        _probe(monkeypatch, s.state, stack)
    s.content(lambda: None)
    return " ".join(s.ui.texts)


def test_mysql_a_newer_watermark_on_a_streamed_stack_needs_a_new_stack(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX (#6): after the detour (Full load + CDC streamed and stopped,
    # switch to Full load only, re-run), the notice still said "set the migration type to CDC
    # only -- it streams from this Full Load's watermark": the old sink then replays its tail
    # over the reload (and Start CDC refuses that start point, decision A2).
    body = _completion_notice(
        monkeypatch, job=_wm_job("mysql-bin.000009", 4),
        stack=_deployed("mysql-bin.000003", 100),
    )
    assert "so CDC from this Full Load's watermark needs a new stack" in body
    assert "Start CDC refuses a start point newer than the one it streamed from" in body
    # G25 round 8 (rules E, F): a Drop & reload copy, then the type switch, then the CDC
    # step's Delete and deploy (test_g25_fl_r8).
    assert _in_order(body, "Re-run Full Load choosing 'Drop & reload'",
                     "set the migration type to \"CDC only\"",
                     "Delete CDC infrastructure and deploy it again", "then Start CDC")
    assert "streams from this Full Load's watermark onto the already-loaded" not in body
    # A stack whose last Start rolled back has no position to compare: a new stack too.
    rolled = _completion_notice(monkeypatch, job=_wm_job("mysql-bin.000009", 4),
                                stack=ROLLED_BACK)
    assert "Delete CDC infrastructure and deploy it again" in rolled


def test_mysql_a_start_point_not_newer_continues_on_the_streamed_stack(monkeypatch) -> None:
    body = _completion_notice(
        monkeypatch, job=_wm_job("mysql-bin.000003", 100),
        stack=_deployed("mysql-bin.000003", 100),
    )
    assert "CDC continues from where streaming stopped" in body
    assert "gapless only while the CDC topics still retain" in body
    assert "needs a new stack" not in body


def test_mysql_a_stack_that_never_streamed_keeps_todays_notice(monkeypatch) -> None:
    for stack in (None, NEVER_STARTED):
        body = _completion_notice(monkeypatch, job=_wm_job("mysql-bin.000009", 4),
                                  stack=stack)
        assert "streams from this Full Load's watermark onto the already-loaded" in body
        assert "needs a new stack" not in body


def test_postgresql_cdc_only_continues_from_a_started_stacks_slot(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "CDC only can still do it by re-snapshotting every selected table
    # (nothing is lost ...)" and "start over as Full load + CDC" -- on a stack started before,
    # CDC only continues from its slot (no re-snapshot) and a new Full load + CDC start point
    # is refused until the CDC infrastructure is deployed again.
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route

    body = _completion_notice(monkeypatch, job=_loaded(), stack=STOPPED, pg=True)
    # G25 round 8: no unconditional "no re-snapshot" (test_g25_fl_r8).
    assert ("continues from that stack's replication slot if its source connector streamed "
            "(otherwise Debezium snapshots every selected table first)") in body
    assert "gapless only while the CDC topics still retain what the sink had not applied" in body
    assert pg_cdc_route(None, started=True, cdc_only=True) in body
    assert "can still do it by re-snapshotting" not in body
    never = _completion_notice(monkeypatch, job=_loaded(), stack=NEVER_STARTED, pg=True)
    assert "This run created no replication slot" in never


@pytest.mark.parametrize("pg", [False, True])
def test_the_coverage_notice_names_new_topics_once_cdc_started(monkeypatch, pg) -> None:
    from tests.test_g12_table_set_after_load import _grown_and_loaded

    s = _flo_screen(monkeypatch, job=_grown_and_loaded(), selected=["app.a", "app.b", "app.c"],
                    pg=pg)
    s.state.set_cdc_ops_window_start(_TS)
    s.content(lambda: None)
    body = " ".join(s.ui.texts)
    assert "never from a newer start point" in body
    assert "Delete CDC infrastructure and deploy it again" in body
    if pg:
        # The started stack's slot remains: CDC re-reads only on a stack deployed again.
        assert "after the CDC infrastructure is deleted and deployed again, CDC re-reads" in body
        assert "CDC then re-reads the tables either way" not in body


def test_the_coverage_lock_names_new_topics_only_once_cdc_started() -> None:
    from dsql_migrator.ui.data_migration._models import (
        COVERAGE_GAP_NOTICE_BODY,
        COVERAGE_GAP_NOTICE_BODY_STARTED,
        coverage_lock_reason,
    )

    started = coverage_lock_reason((["app.c"], ["app.b"]), started=True)
    assert _in_order(started, "re-run Full Load over the current set",
                     "Delete CDC infrastructure and deploy it again",
                     "never from a newer start point")
    assert "Delete CDC infrastructure" not in coverage_lock_reason((["app.c"], []))
    assert "Delete CDC infrastructure and deploy it again" in COVERAGE_GAP_NOTICE_BODY_STARTED
    assert "Delete CDC infrastructure" not in COVERAGE_GAP_NOTICE_BODY


# ---------------------------------------------------------------------------
# 5. The other Full Load-side copy (rules B and D)
# ---------------------------------------------------------------------------


def test_a_carried_over_full_load_gap_names_the_routes_that_keep_the_start_point() -> None:
    # FAILS WITHOUT THE FIX: "Re-run Full Load to close it" -- a new start point, refused on
    # a CDC stack that has streamed.
    from dsql_migrator.ui.data_migration._models import stale_error_notice

    tone, _header, body = stale_error_notice(
        "FullLoadIncompleteError: 1 of 2 table(s) did not fully load.",
        migration_type=CDC_ONLY, error_migration_type=FLO,
    )
    assert tone == "warning" and "not backfill" in body
    assert "Re-run Full Load to close it" not in body
    assert _in_order(body, "Retry unfinished tables", "Reload specific tables",
                     "keeps the original start point",
                     "Delete CDC infrastructure and deploy it again first")


def test_the_manual_reset_does_not_promise_a_captured_gapless_start_point() -> None:
    # FAILS WITHOUT THE FIX: "the watermark this load captures is the gapless start point" --
    # also logged at a Retry or Reload, which captures none, and on a stack that has
    # streamed, where the next Start continues from its committed position.
    from dsql_migrator.ui.data_migration import reset_stale_manual_start_point

    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000003", binlog_pos=154)
    logged: list = []
    assert reset_stale_manual_start_point(state, FLC, SourceType.MYSQL,
                                          log=lambda **kw: logged.append(kw))
    detail = logged[0]["detail"]
    assert "mysql-bin.000003:154" in detail and "back to Automatic" in detail
    assert "gapless" not in detail and "choose Manual again after the load" not in detail
    assert "a Retry or Reload keeps the original one" in detail
    assert "refuses a newer start point" in detail


def test_the_full_load_step_copy_names_new_topics_and_the_retention_condition() -> None:
    from dsql_migrator.ui.data_migration import _full_load_ui

    source = inspect.getsource(_full_load_ui)
    # The record is gone, so running the load again is a new start point.
    assert "Re-run the load, or check Validation" not in source
    assert "stack has streamed, Delete CDC infrastructure and deploy it again first: a " in source
    # The scoped reload converges; its gaplessness depends on the CDC topics there.
    assert "later CDC start is still gapless" not in source
    assert "gapless only while the CDC topics " in source


def test_the_validation_ai_grounding_names_the_streamed_order() -> None:
    from dsql_migrator.core.assessment_strategist import _validation_recovery_context

    text = _validation_recovery_context()
    assert _in_order(text, "has streamed (or rolled back a Start)", "Stop CDC",
                     "Delete CDC infrastructure and deploy it again",
                     "Re-run Full Load with Drop & reload", "then Start CDC")
    assert "keeps the original start point and needs no redeploy" in text

