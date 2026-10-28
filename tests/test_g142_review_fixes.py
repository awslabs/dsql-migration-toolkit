# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G142 review fixes: what may open Cut over's Finish, and what the screens say about it.

1. An old settled catalog read never opens Finish after the keys were dropped: a new load,
   a Drop & reload retry and Start CDC's "Remove foreign keys" take them out of the record,
   a CDC start clears the cut-over result, a migration-type change, a newer Validation
   verdict and opening Cut over make the screen read the keys again.
2. Finish's key set is every pending foreign-key DDL (a key the operator added or renamed
   too); in place only when every one is; this session's failed apply is never masked; a
   renamed key's own cascade action counts as one CDC can break.
3. The cut-over pass shows "Adding foreign keys" at once and its result when it ends.
4. The "orphans" hold says what each held key waits for.
5. The foreign-key block says Finish waits for the keys that hold it.
6. Stale copy: the Data Migration cascade warning and the validator docstring.
7. The lows: an old job's read in flight, the Full load card's tone, the scoped waiver, an
   explaining count of 0 kept, PAD SPACE keys, the listing's key window.
8. Round 2: Finish waits for exactly the keys the pass adds (never an absent unselected
   child's), every drop of the keys voids Cut over's result and waivers, and the keys not in
   place and the keys not validated are two holds with two waivers.
9. Round 3: an unquoted constraint name is the one Aurora DSQL stores, a key added without a
   name gets the name PostgreSQL gives it, and the Full load card counts the pass's keys.

Doubles only: an in-memory Aurora DSQL, a job manager that never runs a job; no AWS.
"""

from __future__ import annotations

import inspect
import re
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import dsql_migrator.ui.data_migration._fk_validation as fkv
import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    ForeignKeyValidationJob,
    TableDef,
)
from dsql_migrator.core.target_introspector import ForeignKeyValidationRead
from dsql_migrator.ui.data_migration._state import DataMigrationState
from tests.test_cutover_defaults_ui import _El, _Ui
from tests.test_g142_fk_add_first_validate_after import (
    KEY_LINES,
    KEY_ORDERS,
    ORDERS,
    _Conn,
    _conversions,
    _Cur,
    _cutover_inputs,
    _Dsql,
    _RenderUi,
    _recorded,
    _text,
    _view,
)

KEY = ("shop.orders", "fk_o_c")  # the key of _cutover_inputs
Submission = engine.ForeignKeyValidationSubmission


def _read(*, present=True, validated=False, status=None, details=None):
    return ForeignKeyValidationRead(
        present=present, validated=validated, job_status=status, job_details=details
    )


class _Jobs:
    """A job manager double: submissions are recorded and never run; statuses by hand."""

    def __init__(self) -> None:
        self.works: dict = {}
        self.statuses: dict = {}

    def submit(self, work, **_kw):
        job_id = f"job-{len(self.works) + 1}"
        self.works[job_id] = work
        self.statuses[job_id] = "RUNNING"
        return job_id

    def get_status(self, job_id):
        if job_id not in self.statuses:
            raise JobNotFoundError(job_id)
        return SimpleNamespace(status=self.statuses[job_id], job_id=job_id)

    def get_error(self, _job_id):
        return None

    def is_cancel_requested(self, _job_id):
        return False


class _TimerUi(_Ui):
    def __init__(self) -> None:
        super().__init__()
        self.timers: list = []

    def timer(self, interval, callback, *_a, **_k):
        self.timers.append((interval, callback))
        return _El(self, "timer")


def _screen(monkeypatch, *, jobs=None, cascade=True, edited=None, cdc=True, inputs=None):
    """Cut over for a MySQL CDC migration of ``_cutover_inputs`` (or ``inputs``) with a
    clean verdict."""
    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.core.models import (
        SourceType,
        TableValidationResult,
        ValidationMode,
        ValidationReport,
    )
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen

    session_obj, eval_store, conv_store = inputs or _cutover_inputs(
        source_type=SourceType.MYSQL, cascade=cascade
    )
    if edited is not None:
        conv_store = SimpleNamespace(get=lambda _sid: SimpleNamespace(
            edited_target_ddls=edited, preserve_foreign_keys=True))
    ui = _TimerUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(uiv, "_cdc_in_use", lambda _s: cdc)
    monkeypatch.setattr(uiv, "render_notice", lambda _ui, **kw: (
        ui.texts.append(kw.get("header", "")), ui.texts.append(kw.get("body", ""))))
    logged: list = []
    monkeypatch.setattr(uiv, "log_activity", lambda _c, action, **kw: logged.append((action, kw)))
    sessions = SessionStore()
    live = sessions.get_or_create("s1")
    live.set_target(session_obj.target_config)
    live.set_source(session_obj.source_config, None)
    vstore, mstore = ValidationStore(), DataMigrationStore()
    content, runner = build_cutover_screen(
        sessions, "s1", validation_store=vstore, migration_store=mstore,
        eval_store=eval_store, conversion_store=conv_store, job_manager=jobs,
    )
    vstate = vstore.get_or_create("s1")
    vstate.set_result(ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[
        TableValidationResult(table="shop.orders", source_row_count=1, target_row_count=1,
                              row_count_match=True, matched=True)]))
    return SimpleNamespace(
        ui=ui, content=content, runner=runner, vstate=vstate,
        mstate=mstore.get_or_create("s1"), logged=logged, session=live,
        eval_store=eval_store, conv_store=conv_store,
    )


def _draw(s) -> str:
    s.ui.texts.clear()
    s.ui.buttons.clear()
    s.ui.timers.clear()
    s.content(lambda: None)
    return s.ui.body()


def _finish(s):
    (button,) = s.ui.by("I've cut over to DSQL")
    return button


# ---------------------------------------------------------------------------
# 1. An old settled read never opens Finish
# ---------------------------------------------------------------------------


def test_a_read_older_than_the_latest_verdict_is_read_again_before_finish_opens(
    monkeypatch,
) -> None:
    # The keys were recorded and read validated by an earlier pass, and the cut-over apply
    # never ran in this session. A Validation verdict newer than that read must not let it
    # open Finish: the screen drops it, says it is reading, and starts a watch at once.
    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs)
    old = datetime.now(timezone.utc) - timedelta(minutes=5)
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}),
                                  now=old - timedelta(seconds=1))
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"}, now=old)
    assert s.mstate.fk_validation_view().settled
    s.vstate.mark_run_finished()  # the latest verdict, after that read
    body = _draw(s)
    assert _finish(s).disabled
    assert "Confirming the foreign keys are in place" in body
    assert "Reading Aurora DSQL's validation of 1 foreign key" in body
    assert s.mstate.fk_validation_view().checking
    assert len(jobs.works) == 1, "a watch reads the keys again"
    # A read taken after the verdict that finds the key validated opens it.
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"})
    _draw(s)
    assert not _finish(s).disabled


def test_a_dropped_read_is_taken_again_even_right_after_a_watch_ended(monkeypatch) -> None:
    # A watch that ended a moment ago keeps the screen from starting another for a while;
    # a read the screen has just dropped as too old is still taken again at once.
    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs)
    earlier = datetime.now(timezone.utc) - timedelta(minutes=1)
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}),
                                  now=earlier - timedelta(seconds=1))
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"}, now=earlier)
    assert s.mstate.fk_validation_view().settled
    s.mstate.note_fk_validation_watch_ended()
    s.vstate.mark_run_finished()
    _draw(s)
    assert s.mstate.fk_validation_view().checking and len(jobs.works) == 1
    # Not again on the next render: nothing newer than the read is left to drop.
    _draw(s)
    assert len(jobs.works) == 1


def test_opening_cut_over_reads_the_keys_again(monkeypatch) -> None:
    # A settled read from before the operator opened Cut over (keys could have been dropped
    # meanwhile in a way the tool does not see) is read again on opening; once the new read
    # lands, the next renders keep it.
    from dsql_migrator.ui.workflow import WorkflowStep

    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs)
    s.vstate.set_cutover_fk_apply((1, 0, 0))
    earlier = datetime.now(timezone.utc) - timedelta(seconds=30)
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}),
                                  now=earlier - timedelta(seconds=1))
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"}, now=earlier)
    assert s.mstate.fk_validation_view().settled
    s.session.set_active_view(WorkflowStep.CUT_OVER.value)
    _draw(s)
    assert s.mstate.fk_validation_view().checking and len(jobs.works) == 1
    assert _finish(s).disabled  # the cascade key is held until it is read validated
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"})
    _draw(s)
    assert not s.mstate.fk_validation_view().checking and not _finish(s).disabled
    # Re-selecting the same view keeps its time: no read is dropped again.
    s.session.set_active_view(WorkflowStep.CUT_OVER.value)
    _draw(s)
    assert not s.mstate.fk_validation_view().checking and len(jobs.works) == 1


def test_keys_removed_for_cdc_and_a_cdc_start_never_leave_finish_open(monkeypatch) -> None:
    s = _screen(monkeypatch, jobs=_Jobs())
    s.vstate.set_cutover_fk_apply((1, 0, 0))
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}))
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"})
    _draw(s)
    assert not _finish(s).disabled
    # Start CDC's "Remove foreign keys" drops the key, and CDC then starts.
    assert s.mstate.forget_fk_validation_keys([KEY]) == 1
    s.vstate.note_cdc_start()
    body = _draw(s)
    assert _finish(s).disabled
    assert "Foreign keys are not applied yet" in body
    assert s.vstate.cutover_fk_apply is None
    assert "no longer on Aurora DSQL" not in body  # the tool's own removal is no mystery


def test_remove_foreign_keys_takes_the_dropped_keys_out_of_the_record(monkeypatch) -> None:
    import dsql_migrator.core.schema_applier as applier
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    state = DataMigrationState()
    state.record_fk_validation(Submission(
        in_place=(KEY_ORDERS, KEY_LINES), jobs={KEY_ORDERS: "j1", KEY_LINES: "j2"}))
    monkeypatch.setattr(cdc_ui, "_cdc_fk_connection_factory", lambda _s: (lambda: None))
    monkeypatch.setattr(cdc_ui, "_log_cdc_event", lambda *_a, **_k: None)

    def _drop(table, name, *, connection_factory):
        if name == "fk_lines_order":
            raise RuntimeError("busy")

    monkeypatch.setattr(applier, "drop_foreign_key", _drop)
    assert cdc_ui._drop_cdc_blocking_foreign_keys(
        [KEY_ORDERS, KEY_LINES], object(), state) == (1, 1)
    assert set(state.fk_validation_records()) == {KEY_LINES}
    src = inspect.getsource(cdc_ui._open_cdc_start_dialog)
    assert "_drop_cdc_blocking_foreign_keys, fk_block.ours, session,\n" in src
    assert "migration_state," in src[src.index("_drop_cdc_blocking_foreign_keys, fk_block"):][:120]


def test_a_new_load_or_a_drop_and_reload_forgets_every_fk_verdict() -> None:
    from dsql_migrator.ui.validation import ValidationState

    state = DataMigrationState()
    vstate = ValidationState()
    vstate.set_cutover_fk_apply((2, 0, 0))
    vstate.waive_foreign_keys()
    vstate.waive_fk_validation([KEY_ORDERS])
    state.validation_state_provider = lambda: vstate
    state.set_fk_apply_result((2, 0, 0))
    state.record_fk_validation(Submission(in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j"}))
    state.forget_applied_foreign_keys()
    assert state.fk_apply_result is None and state.fk_validation_view() is None
    assert vstate.cutover_fk_apply is None and not vstate.proceed_without_foreign_keys
    assert vstate.proceed_without_fk_validation == frozenset()
    # Both sites call it (they are closures of the page builder).
    import dsql_migrator.ui.data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    assert src.count("migration_state.forget_applied_foreign_keys()") == 2


def test_a_migration_type_change_reads_the_keys_again() -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    state = DataMigrationState()
    state.record_fk_validation(Submission(in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j"}))
    started = datetime.now(timezone.utc)
    state.set_fk_validation_reads({KEY_ORDERS: _read(validated=True)}, {KEY_ORDERS: "j"})
    state.set_migration_type(state.migration_type)  # re-confirming the same type
    assert not state.fk_validation_view().checking
    state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    assert state.fk_validation_view().checking
    # A read still in flight from before the change is not stored.
    *_ignored, stored = state.set_fk_validation_reads(
        {KEY_ORDERS: _read(validated=True)}, {KEY_ORDERS: "j"}, now=started)
    assert stored is None and state.fk_validation_view().checking


def test_a_cdc_start_clears_the_cut_over_fk_result_and_its_waiver() -> None:
    from dsql_migrator.ui.validation import ValidationState

    vstate = ValidationState()
    vstate.set_cutover_fk_apply((3, 0, 0))
    vstate.waive_foreign_keys()
    vstate.waive_fk_validation([KEY])
    vstate.note_cdc_start()
    assert vstate.cutover_fk_apply is None
    assert not vstate.proceed_without_foreign_keys
    assert vstate.proceed_without_fk_validation == frozenset()


def test_a_verdict_time_ahead_of_this_clock_does_not_refuse_every_read() -> None:
    state = DataMigrationState()
    state.record_fk_validation(Submission(in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j"}))
    state.invalidate_fk_validation_reads(
        before=datetime.now(timezone.utc) + timedelta(hours=1))
    *_ignored, stored = state.set_fk_validation_reads(
        {KEY_ORDERS: _read(validated=True)}, {KEY_ORDERS: "j"})
    assert stored is not None and not state.fk_validation_view().checking


# ---------------------------------------------------------------------------
# 2. Every pending key counts; a failed apply is never masked
# ---------------------------------------------------------------------------

_EDITED = (
    'CREATE SCHEMA IF NOT EXISTS "shop";\n'
    'CREATE TABLE "shop"."orders" (\n  "id" INT,\n  "cid" INT,\n  PRIMARY KEY ("id")\n);\n'
    'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_o_c" FOREIGN KEY ("cid") '
    'REFERENCES "shop"."customers" ("id") NOT VALID;\n'
    # A key the operator added in Schema Conversion: no source metadata.
    'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_extra" FOREIGN KEY ("cid") '
    'REFERENCES "shop"."customers" ("id") ON DELETE CASCADE NOT VALID;\n'
)
EXTRA = ("shop.orders", "fk_extra")


def test_a_key_the_operator_added_holds_finish_until_it_is_in_place(monkeypatch) -> None:
    import dsql_migrator.ui.validation as uiv

    s = _screen(monkeypatch, jobs=_Jobs(), cascade=False, edited={"shop.orders": _EDITED})
    keys, breakable, defs = uiv._cutover_fk_keys(
        s.session, s.eval_store, s.conv_store, "s1", migration_state=s.mstate, cdc_in_use=True)
    assert keys == [KEY, EXTRA]
    assert set(defs) == {KEY}  # only fk_o_c has source metadata
    # Its own ON DELETE CASCADE makes the added key one CDC can break.
    assert breakable == frozenset({EXTRA})
    # The cut-over pass added fk_o_c and failed on fk_extra: (1, 0, 1). A read showing
    # fk_o_c in place never masks that.
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}))
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"})
    s.vstate.set_cutover_fk_apply((1, 0, 1))
    _draw(s)
    assert _finish(s).disabled
    # A Validation re-run clears the result; the read still lacks fk_extra: not applied.
    s.vstate.set_cutover_fk_apply(None)
    body = _draw(s)
    assert _finish(s).disabled
    assert "1 foreign key of this schema's 2 is not on Aurora DSQL (shop.orders.fk_extra)" in body


def test_in_place_needs_every_pending_key_named_and_confirmed() -> None:
    from dsql_migrator.ui.validation import cutover_fk_in_place

    view = _view({KEY_ORDERS: "validating", KEY_LINES: "validated"})
    assert cutover_fk_in_place([KEY_ORDERS, KEY_LINES], 2, view) == (True, False, [])
    # A pending key the record does not hold is missing (never added by the last pass).
    extra = (ORDERS, "fk_extra")
    assert cutover_fk_in_place([KEY_ORDERS, KEY_LINES, extra], 3, view) == (
        False, False, ["shop.orders.fk_extra"])
    # A key gone from the target is missing too.
    gone = _view({KEY_ORDERS: "absent", KEY_LINES: "validated"})
    assert cutover_fk_in_place([KEY_ORDERS, KEY_LINES], 2, gone)[0] is False
    # A DDL whose name cannot be read can never be confirmed, nor a count that disagrees.
    assert cutover_fk_in_place([KEY_ORDERS, KEY_LINES, (ORDERS, None)], 3, view) == (
        None, False, [])
    assert cutover_fk_in_place([KEY_ORDERS, KEY_LINES], 3, view) == (None, False, [])
    # Not read yet: reading.
    checking = fkv.fk_validation_view(
        {KEY_ORDERS: ForeignKeyValidationJob(table=ORDERS, constraint="fk_orders_customer")},
        {}, None)
    assert cutover_fk_in_place([KEY_ORDERS], 1, checking) == (None, True, [])


def test_the_gate_never_masks_a_failed_apply_and_reads_before_it_says_pending() -> None:
    from dsql_migrator.ui.validation import cutover_finish_fk_gate

    def gate(**kw):
        base = dict(cdc_in_use=True, fk_pending_count=2, fk_apply_result=None,
                    proceed_without_fks=False)
        base.update(kw)
        return cutover_finish_fk_gate(**base)

    assert gate(fk_apply_result=(1, 0, 1), fks_in_place=True) == "incomplete"
    assert gate(fk_apply_result=(2, 0, 0), fks_in_place=False) == "pending"
    assert gate(fks_in_place=None, fks_reading=True) == "checking"
    assert gate(fks_in_place=None) == "pending"
    assert gate(fk_apply_result=(2, 0, 0), fks_in_place=None, fks_reading=True) is None


def test_a_renamed_key_keeps_its_own_cascade_and_loose_text_key_for_cdc() -> None:
    fk = ForeignKeyDef(name="fk_old", columns=["code"], referenced_table="shop.customers",
                       referenced_columns=["code"])
    renamed_cascade = (
        'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_new" FOREIGN KEY ("code") '
        'REFERENCES "shop"."customers" ("code") ON UPDATE SET NULL NOT VALID'
    )
    renamed_plain = renamed_cascade.replace(" ON UPDATE SET NULL", "").replace(
        '"fk_new"', '"fk_plain"')
    conv = {ORDERS: engine.TableConversion(
        table=ORDERS, target_ddl="CREATE TABLE x (id int)",
        foreign_key_ddls=[renamed_cascade, renamed_plain], preserved_foreign_keys=[fk])}
    ints = [TableDef(name="shop.customers", columns=[ColumnDef(name="code", mysql_type="int")],
                     primary_key=["code"])]
    assert fkv.foreign_keys_cdc_can_break(conv, ints) == frozenset({(ORDERS, "fk_new")})
    texts = [TableDef(name="shop.customers", columns=[
        ColumnDef(name="code", mysql_type="varchar(8)", collation="utf8mb4_0900_ai_ci")],
        primary_key=["code"])]
    assert fkv.foreign_keys_cdc_can_break(conv, texts) == frozenset(
        {(ORDERS, "fk_new"), (ORDERS, "fk_plain")})


# ---------------------------------------------------------------------------
# 3. The cut-over pass: "Adding foreign keys" at once, the result when it ends
# ---------------------------------------------------------------------------


def test_the_cut_over_pass_shows_running_at_once_and_its_result_at_its_end(monkeypatch) -> None:
    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.ui.validation import ValidationState

    session_obj, eval_store, conv_store = _cutover_inputs(source_type=SourceType.MYSQL,
                                                          cascade=False)
    vstate = ValidationState()
    vstate.set_cutover_fk_apply((1, 0, 0))  # an earlier pass
    jobs = _Jobs()
    seen: list = []
    monkeypatch.setattr(engine, "apply_preserved_foreign_keys", lambda *_a, **_k: (0, 0, 1))
    uiv._run_cutover_foreign_keys(
        session_obj, vstate, eval_store=eval_store, conversion_store=conv_store,
        session_id="s1", job_manager=jobs, refresh=lambda: seen.append(vstate.cutover_fk_apply),
    )
    # The old result went before the submit, and the screen came back right after it.
    assert seen == [None] and vstate.cutover_fk_apply is None
    ((job_id, work),) = jobs.works.items()
    assert vstate.cutover_fk_apply_job_id == job_id
    work(None)  # the pass; its job still reads RUNNING
    assert vstate.cutover_fk_apply == (0, 0, 1) and seen[-1] == (0, 0, 1)


def test_the_screen_polls_while_the_pass_runs_and_shows_its_result_inside_its_job(
    monkeypatch,
) -> None:
    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs, cascade=False)
    s.vstate.set_cutover_cdc_drained_confirmed(True)
    jobs.statuses["job-pass"] = "RUNNING"
    s.vstate.set_cutover_fk_apply_job("job-pass")
    s.vstate.set_cutover_fk_apply_progress(0, 1)
    body = _draw(s)
    assert "Adding foreign keys: 0 of 1" in body
    assert s.ui.by("Apply foreign keys")[0].disabled
    assert [t for t, _cb in s.ui.timers if t == 1.5], "a short poll while it runs"
    # The pass recorded its result and refreshes from inside its job (still RUNNING): the
    # render shows the result, not "Adding", and Apply is back -- even with no key in place.
    s.vstate.set_cutover_fk_apply((0, 0, 1))
    body = _draw(s)
    assert "Adding foreign keys" not in body
    assert "Some foreign keys were not applied" in body
    assert not s.ui.by("Apply foreign keys")[0].disabled
    assert not [t for t, _cb in s.ui.timers if t == 1.5]


# ---------------------------------------------------------------------------
# 4. The "orphans" hold says what each held key waits for
# ---------------------------------------------------------------------------


def test_the_orphans_hold_is_worded_from_each_held_keys_state() -> None:
    from dsql_migrator.ui.validation import _cutover_orphans_hold_copy

    a, b = ("shop.orders", "fk_a"), ("shop.lines", "fk_b")
    header, body = _cutover_orphans_hold_copy({a: "failed"})
    assert header == "Foreign keys CDC can break failed Aurora DSQL's validation"
    assert "1 foreign key (shop.orders.fk_a) can be left" in body
    assert "fix the rows (see the details above), then click Validate again" in body
    header, body = _cutover_orphans_hold_copy({a: "not-validated"})
    assert header == "Foreign keys CDC can break are not validated"
    assert "No validation job runs for shop.orders.fk_a (it could not be submitted" in body
    assert "still running" not in body and "click Validate again" in body
    assert "It is in place and refuses every new write" in body
    header, body = _cutover_orphans_hold_copy({a: "failed", b: "not-validated"})
    assert "2 foreign keys (shop.orders.fk_a, shop.lines.fk_b) can be left" in body
    assert "Validate again." in body and "No validation job runs for shop.lines.fk_b" in body
    assert "Each is in place and refuses every new write" in body
    # A key not in place is the other hold's: this copy never sends to Apply foreign keys.
    assert "Apply foreign keys" not in body
    header, body = _cutover_orphans_hold_copy({a: "validating"})
    assert header == "Waiting for Aurora DSQL to validate the foreign keys CDC can break"
    assert "Aurora DSQL's check of shop.orders.fk_a is still running." in body
    header, body = _cutover_orphans_hold_copy({a: "unknown"})
    assert header == "Reading Aurora DSQL's validation of the foreign keys CDC can break"
    assert "is being read now" in body and "still running" not in body


def test_the_block_says_is_for_one_key_and_are_for_several() -> None:
    no_job = {key: ForeignKeyValidationJob(table=key[0], constraint=key[1])
              for key in (KEY_ORDERS, KEY_LINES)}
    ui = _RenderUi()
    fkv.render_fk_validation(ui, fkv.fk_validation_view(
        {KEY_ORDERS: no_job[KEY_ORDERS]}, {KEY_ORDERS: _read()}, datetime.now(timezone.utc)))
    assert "1 of 1 foreign key is not validated" in ui.body()
    assert "No validation job runs for it" in ui.body()
    ui = _RenderUi()
    fkv.render_fk_validation(ui, fkv.fk_validation_view(
        no_job, {KEY_ORDERS: _read(), KEY_LINES: _read()}, datetime.now(timezone.utc)))
    assert "2 of 2 foreign keys are not validated" in ui.body()
    assert "No validation job runs for them" in ui.body()


# ---------------------------------------------------------------------------
# 5. The block says Finish waits for the keys that hold it
# ---------------------------------------------------------------------------


def test_the_block_says_finish_waits_for_the_keys_that_hold_it() -> None:
    view = _view({KEY_ORDERS: "validating", KEY_LINES: "validated"})
    ui = _RenderUi()
    fkv.render_fk_validation(ui, view, holding={KEY_ORDERS})
    assert "does not hold the next step" not in ui.body()
    assert "Finish waits for Aurora DSQL's check of shop.orders.fk_orders_customer" in ui.body()
    ui = _RenderUi()
    fkv.render_fk_validation(ui, view)
    assert "does not hold the next step" in ui.body()
    # A mixed outcome names the key still being validated beside the failure.
    mixed = _view({KEY_ORDERS: "failed", KEY_LINES: "validating"})
    ui = _RenderUi()
    fkv.render_fk_validation(ui, mixed, holding={KEY_ORDERS, KEY_LINES})
    body = ui.body()
    assert "Finish waits for this key to validate" in body
    assert "Still being validated by Aurora DSQL: shop.order_lines.fk_lines_order." in body
    assert "Finish waits for Aurora DSQL's check of shop.order_lines.fk_lines_order" in body


def test_cut_over_names_the_held_key_in_the_block_unless_it_was_waived(monkeypatch) -> None:
    s = _screen(monkeypatch)
    s.vstate.set_cutover_fk_apply((1, 0, 0))
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}))
    s.mstate.set_fk_validation_reads({KEY: _read(status="processing")}, {KEY: "j1"})
    body = _draw(s)
    assert "Finish waits for Aurora DSQL's check of shop.orders.fk_o_c" in body
    assert "does not hold the next step" not in body
    (waive,) = s.ui.by("Cut over without this check")
    waive.on_click()
    body = _draw(s)
    assert not _finish(s).disabled
    assert "does not hold the next step" in body


# ---------------------------------------------------------------------------
# 6. Stale copy
# ---------------------------------------------------------------------------


def test_the_cascade_warning_and_the_validator_say_validation_fails_not_add_blocks() -> None:
    import dsql_migrator.core.validator as validator
    import dsql_migrator.ui.data_migration as dm

    src = inspect.getsource(dm)
    assert "BLOCK " not in src.split("CDC cannot replicate this schema's cascading")[1][:1500]
    flat = " ".join(src.split())
    assert ("orphan then fails Aurora \" \"DSQL's validation of that key, which holds the cut "
            "over \" \"until it is fixed") in flat
    doc = validator.__doc__ or ""
    assert "PRE-APPLY GATE" not in doc and "EARLY WARNING" in doc


# ---------------------------------------------------------------------------
# 7. The lows
# ---------------------------------------------------------------------------


def test_an_old_jobs_failed_read_in_flight_is_never_charged_to_the_new_job() -> None:
    state = DataMigrationState()
    state.record_fk_validation(Submission(in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j-old"}))
    events: list = []
    explained: list = []
    reads = iter([
        # Validate again lands while this read for j-old is in flight.
        lambda: (state.record_fk_revalidation({KEY_ORDERS: "j-new"}),
                 {KEY_ORDERS: _read(status="failed", details="violates")})[1],
        lambda: {KEY_ORDERS: _read(status="failed", details="violates")},
    ])
    original_log, original_explain = fkv.log_activity, fkv.explain_foreign_key_failure
    fkv.log_activity = lambda _c, action, **kw: events.append(action)
    fkv.explain_foreign_key_failure = lambda *_a, **_k: (
        explained.append(1) or (5, "SELECT 1", None))
    original_watch = fkv.watch_foreign_key_validation

    def _watch(connection_factory, records, **kw):
        kw["read"] = lambda keys, jobs, *, connection_factory: next(reads)()
        return original_watch(connection_factory, records, **kw)

    fkv.watch_foreign_key_validation = _watch
    try:
        fkv.run_fk_validation_watch(
            state, connection_factory=lambda: None,
            fk_defs=lambda: fkv.foreign_key_defs(_conversions()), sleep=lambda _s: None,
        )
    finally:
        fkv.log_activity, fkv.explain_foreign_key_failure = original_log, original_explain
        fkv.watch_foreign_key_validation = original_watch
    record = state.fk_validation_record(KEY_ORDERS)
    # The first read (for j-old) changed nothing for j-new; the second, for j-new, did.
    assert record.job_id == "j-new" and record.status == "failed"
    assert explained == [1] and record.orphan_count == 5
    assert events.count("foreign key not validated") == 1


def test_the_watch_judges_a_key_only_by_a_read_for_its_current_job() -> None:
    # A caller that stores nothing (on_read returns None): the watch itself drops the
    # answer for a job the key no longer has.
    recs = {KEY_ORDERS: ForeignKeyValidationJob(table=ORDERS, constraint="fk_orders_customer",
                                                job_id="j-old", status="submitted")}
    polls = {"n": 0}

    def _reader(keys, jobs, *, connection_factory):
        polls["n"] += 1
        if polls["n"] == 1:  # Validate again lands while this read for j-old is in flight
            recs[KEY_ORDERS] = recs[KEY_ORDERS].model_copy(update={"job_id": "j-new"})
            return {KEY_ORDERS: _read(status="failed")}
        return {KEY_ORDERS: _read(validated=True)}

    failed: list = []
    fkv.watch_foreign_key_validation(
        lambda: None, lambda: dict(recs), on_read=lambda *_a: None, on_failed=failed.append,
        read=_reader, sleep=lambda _s: None,
    )
    assert failed == [] and polls["n"] == 2


def test_a_read_the_session_refused_is_never_judged() -> None:
    # The same job, but the reads were invalidated while this one was in flight (a
    # migration-type change): the session refuses it, and so does the watch.
    state = DataMigrationState()
    state.record_fk_validation(Submission(in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j1"}))
    reads = iter([
        lambda: (state.invalidate_fk_validation_reads(), {KEY_ORDERS: _read(status="failed")})[1],
        lambda: {KEY_ORDERS: _read(validated=True)},
    ])
    explained: list = []
    original_explain = fkv.explain_foreign_key_failure
    original_watch = fkv.watch_foreign_key_validation
    fkv.explain_foreign_key_failure = lambda *_a, **_k: (
        explained.append(1) or (1, "SELECT 1", None))

    def _watch(connection_factory, records, **kw):
        kw["read"] = lambda keys, jobs, *, connection_factory: next(reads)()
        return original_watch(connection_factory, records, **kw)

    fkv.watch_foreign_key_validation = _watch
    try:
        fkv.run_fk_validation_watch(
            state, connection_factory=lambda: None,
            fk_defs=lambda: fkv.foreign_key_defs(_conversions()), sleep=lambda _s: None,
        )
    finally:
        fkv.explain_foreign_key_failure, fkv.watch_foreign_key_validation = (
            original_explain, original_watch)
    assert explained == []
    assert state.fk_validation_view().state_of(KEY_ORDERS) == fkv.FK_VALIDATED


def test_the_full_load_card_takes_its_tone_from_aurora_dsqls_verdict() -> None:
    import dsql_migrator.ui.data_migration._full_load_ui as flu

    def _card(view):
        captured: list = []
        real = flu.render_notice
        flu.render_notice = lambda ui, *, tone="info", header="", body="", **k: captured.append(
            (tone, header, body))
        try:
            flu._render_foreign_key_action(
                _RenderUi(), terminal=True, pending=2, applied=(2, 0, 0),
                apply_foreign_keys=lambda: None, connections_ready=True, validation=view)
        finally:
            flu.render_notice = real
        return captured[0]

    tone, _header, body = _card(_view({KEY_ORDERS: "failed", KEY_LINES: "validated"}))
    assert tone == "warning" and "Referential integrity is in place" not in body
    assert "has not validated the rows already loaded for 1 of them" in body
    tone, _header, body = _card(_view({KEY_ORDERS: "validating", KEY_LINES: "validated"}))
    assert tone == "success" and "validates the rows already loaded in the background" in body
    tone, _header, body = _card(_view({KEY_ORDERS: "validated", KEY_LINES: "validated"}))
    assert tone == "success" and "Referential integrity is in place on the target" in body


def test_a_waiver_of_keys_not_applied_does_not_cover_their_failed_validation(monkeypatch) -> None:
    s = _screen(monkeypatch)
    _draw(s)
    (waive,) = s.ui.by("Cut over without enforced foreign keys")
    waive.on_click()
    _draw(s)
    assert not _finish(s).disabled  # the keys-not-applied hold is waived
    # The operator applies them anyway, and the cascade key's validation fails.
    s.vstate.set_cutover_fk_apply((1, 0, 0))
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}))
    s.mstate.set_fk_validation_reads({KEY: _read(status="failed", details="violates")},
                                     {KEY: "j1"})
    body = _draw(s)
    assert _finish(s).disabled
    assert "Foreign keys CDC can break failed Aurora DSQL's validation" in body
    assert s.ui.by("Cut over without this check")


def test_an_explaining_count_of_zero_survives_the_snapshot() -> None:
    record = ForeignKeyValidationJob(table=ORDERS, constraint="fk_orders_customer",
                                     job_id="j", status="failed", orphan_count=0)
    payload = record.model_dump_json()
    assert '"orphan_count":0' in payload
    restored = ForeignKeyValidationJob.model_validate_json(payload)
    view = fkv.fk_validation_view({KEY_ORDERS: restored}, {KEY_ORDERS: _read()},
                                  datetime.now(timezone.utc))
    (item,) = view.items
    assert item.state == fkv.FK_FAILED and item.orphan_count == 0 and not item.counting
    unset = ForeignKeyValidationJob(table="t", constraint="c")
    assert "orphan_count" not in unset.model_dump_json()


@pytest.mark.parametrize(
    "mysql_type, collation, breakable",
    [
        ("varchar(8)", "utf8mb4_bin", True),  # PAD SPACE: 'a ' matches 'a' on MySQL
        ("varchar(8)", "latin1_bin", True),
        ("varchar(8)", "utf8mb4_general_cs", True),
        ("char(8)", "utf8mb4_bin", False),  # char(n) ignores trailing spaces on DSQL too
        ("varchar(8)", "utf8mb4_0900_bin", False),  # NO PAD
        ("varchar(8)", "utf8mb4_0900_as_cs", False),
        ("varbinary(8)", "binary", False),
        ("char(8)", "utf8mb4_general_ci", True),  # case-insensitive, whatever the type
    ],
)
def test_a_text_key_mysql_matches_with_trailing_spaces_is_one_cdc_can_break(
    mysql_type, collation, breakable
) -> None:
    tables = [TableDef(name="shop.customers", columns=[
        ColumnDef(name="id", mysql_type=mysql_type, collation=collation)], primary_key=["id"])]
    got = fkv.foreign_keys_cdc_can_break(_conversions(), tables)
    assert (KEY_ORDERS in got) is breakable


def test_the_listing_reads_the_window_where_the_count_found_the_first_orphan(
    monkeypatch,
) -> None:
    from psycopg import sql

    from dsql_migrator.core.validation_sql import build_orphan_listing_sql

    fk = ForeignKeyDef(name="fk_orders_customer", columns=["customer_id"],
                       referenced_table="shop.customers", referenced_columns=["id"])
    windowed = build_orphan_listing_sql(
        ORDERS, fk, ["id"], page_size=5000, after=[sql.Placeholder("after_0")]
    ).as_string(None)
    assert windowed.startswith(
        'WITH pg AS MATERIALIZED (SELECT "id", "customer_id" FROM "shop"."orders" '
        'WHERE ("id") > (%(after_0)s) ORDER BY "shop"."orders"."id" LIMIT 5000)')
    assert windowed.endswith(
        'WHERE pg."customer_id" IS NOT NULL AND p."id" IS NULL ORDER BY pg."id" LIMIT 100')

    # The count tells where the first orphan is: after key 4999 (the second page).
    def _count(_factory, _table, _fk, *, on_page=None, on_orphans=None):
        on_orphans((4999,))
        return 3

    monkeypatch.setattr(engine, "_count_orphans", _count)
    monkeypatch.setattr(fkv, "target_primary_key_columns", lambda *_a, **_k: ["id"])
    count, template, runnable = fkv.explain_foreign_key_failure(lambda: None, ORDERS, fk)
    assert count == 3
    assert 'WHERE ("id") > (%(after_0)s)' in template and "4999" not in template
    assert 'WHERE ("id") > (4999)' in runnable
    # The record keeps the literal-free one; the screen shows the runnable one, and after a
    # restart (no runnable one) says where its window start comes from.
    state = DataMigrationState()
    state.record_fk_validation(Submission(in_place=(KEY_ORDERS,), jobs={KEY_ORDERS: "j"}))
    state.set_fk_validation_reads({KEY_ORDERS: _read(status="failed", details="d")},
                                  {KEY_ORDERS: "j"})
    state.note_fk_explanation(KEY_ORDERS, "j", count, template, listing_runnable=runnable)
    assert state.fk_validation_record(KEY_ORDERS).listing_sql == template
    (item,) = state.fk_validation_view().items
    assert item.listing_sql == runnable and not item.listing_start_unknown
    restored = DataMigrationState()
    restored.restore_fk_validation(state.fk_validation_records().values())
    restored.set_fk_validation_reads({KEY_ORDERS: _read(status="failed")}, {KEY_ORDERS: "j"})
    (item,) = restored.fk_validation_view().items
    assert item.listing_sql == template and item.listing_start_unknown
    ui = _RenderUi()
    fkv.render_fk_validation(ui, restored.fk_validation_view())
    assert "is not kept across a restart" in ui.body()
    # No orphan found now: no listing at all.
    monkeypatch.setattr(engine, "_count_orphans", lambda *_a, **_k: 0)
    assert fkv.explain_foreign_key_failure(lambda: None, ORDERS, fk) == (0, None, None)


def test_the_counts_report_the_cursor_of_the_first_page_holding_an_orphan() -> None:
    from dsql_migrator.core.validator import target_orphan_count_composite

    fk = ForeignKeyDef(name="f", columns=["c"], referenced_table="shop.p",
                       referenced_columns=["id"])
    pages = iter([(0, 9, 2), (2, 19, 2), (1, 21, 1)])  # (orphans, last pk, rows)
    told: list = []

    class _Conn:
        def cursor(self):
            return SimpleNamespace(execute=lambda *_a: None, fetchone=lambda: next(pages),
                                   close=lambda: None)

    assert engine._count_orphans_keyset(_Conn(), "shop.c", fk, "id", 2,
                                        on_orphans=told.append) == 3
    assert told == [(9,)]
    composite = iter([(0, 2, "1", "a"), (1, 2, "1", "c"), (0, 1, "2", "a")])
    told.clear()
    assert target_orphan_count_composite(
        None, "shop.c", fk, ["k0", "k1"], 2,
        page_row=lambda _statement, _params: next(composite), on_orphans=told.append,
    ) == 1
    assert told == [("1", "a")]
    # The first page holding one: no cursor at all.
    first = iter([(1, 5, 1)])
    told.clear()

    class _One:
        def cursor(self):
            return SimpleNamespace(execute=lambda *_a: None, fetchone=lambda: next(first),
                                   close=lambda: None)

    engine._count_orphans_keyset(_One(), "shop.c", fk, "id", 2, on_orphans=told.append)
    assert told == [None]


# ---------------------------------------------------------------------------
# 8. Round 2: one key set, every drop voids the result, two holds and two waivers
# ---------------------------------------------------------------------------

AUDIT = ("shop.audit", "fk_a_c")  # an unselected child pointing into the migration


def _inputs(*tables):
    """``_cutover_inputs``'s session and stores over ``tables`` (MySQL)."""
    from dsql_migrator.core.models import (
        SourceConnectionConfig,
        SourceInventory,
        SourceType,
        TargetConnectionConfig,
    )

    inv = SourceInventory(tables=list(tables))
    session = SimpleNamespace(
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                             region="us-east-1"),
        aws_profile=None,
        source_config=SourceConnectionConfig(host="h", database="d",
                                             source_type=SourceType.MYSQL),
    )
    eval_store = SimpleNamespace(get_or_create=lambda _sid: SimpleNamespace(
        result=SimpleNamespace(inventory=inv)))
    conv_store = SimpleNamespace(get=lambda _sid: SimpleNamespace(
        edited_target_ddls={}, preserve_foreign_keys=True))
    return session, eval_store, conv_store


def _table(name, *keys):
    """A table with an ``id`` key and, per ``(constraint, parent, on_delete)``, a column and
    a foreign key to ``parent``."""
    columns = [ColumnDef(name="id", mysql_type="int")]
    fks = []
    for constraint, parent, on_delete in keys:
        columns.append(ColumnDef(name=f"{constraint}_id", mysql_type="int"))
        fks.append(ForeignKeyDef(name=constraint, columns=[f"{constraint}_id"],
                                 referenced_table=parent, referenced_columns=["id"],
                                 on_delete=on_delete))
    return TableDef(name=name, columns=columns, primary_key=["id"], foreign_keys=fks)


def _dsql_double(monkeypatch, *, absent=("shop.audit",)):
    """The in-memory Aurora DSQL behind every connection the cut-over code opens, with the
    ``absent`` tables not on it. No row breaks a key."""
    import dsql_migrator.core.target_connection as target_connection

    db = _Dsql(polls=1)
    db.children = {name: ("x", "shop.customers", {}) for name in ("shop.orders", "shop.audit")}
    monkeypatch.setattr(target_connection, "DsqlConnector",
                        lambda *_a, **_k: SimpleNamespace(connect=db.connect))
    monkeypatch.setattr(engine, "_target_table_present", lambda _cf, name: name not in absent)
    monkeypatch.setattr(engine, "log_activity", lambda *_a, **_k: None)
    monkeypatch.setattr(fkv, "log_activity", lambda *_a, **_k: None)
    return db


def _run_jobs(jobs, *, skip=()):
    """Run each submitted job once, in order (a pass, then the watch it starts)."""
    done = set(skip)
    while (pending := [job_id for job_id in jobs.works if job_id not in done]):
        for job_id in pending:
            done.add(job_id)
            jobs.works[job_id](SimpleNamespace(heartbeat=lambda: None, cancelled=False))
            jobs.statuses[job_id] = "DONE"


@pytest.mark.parametrize("migrated_child", [True, False])
def test_an_absent_unselected_child_never_holds_finish_after_the_pass(
    monkeypatch, migrated_child
) -> None:
    # Migrating customers (and orders): shop.audit, never created on Aurora DSQL, keeps its
    # key into customers in scope. The pass leaves it out by design (nothing to re-create),
    # so Finish -- whose key set is the pass's own -- opens right after the pass, stays open
    # once the read lands, and still does after a restart. As its only key a cascade
    # (one CDC can break), it would otherwise have held "being read" forever.
    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.core.models import TableSelection

    tables = [_table("shop.customers")]
    if migrated_child:
        tables.append(_table("shop.orders", ("fk_o_c", "shop.customers", None)))
    tables.append(_table("shop.audit", ("fk_a_c", "shop.customers", "CASCADE")))
    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs, inputs=_inputs(*tables))
    _dsql_double(monkeypatch)
    selected = ["shop.customers"] + (["shop.orders"] if migrated_child else [])
    s.mstate.set_selection(TableSelection(selected_tables=selected))
    s.vstate.set_cutover_cdc_drained_confirmed(True)

    def _keys(state=None):
        return uiv._cutover_fk_keys(s.session, s.eval_store, s.conv_store, "s1",
                                    migration_state=state or s.mstate, cdc_in_use=True)[0]

    expected = [KEY] if migrated_child else []
    assert _keys() == expected + [AUDIT]  # before any pass: the audit key may be needed
    _draw(s)
    assert _finish(s).disabled
    (apply_button,) = s.ui.by("Apply foreign keys")
    apply_button.on_click()
    (pass_job,) = jobs.works
    jobs.works[pass_job](SimpleNamespace(heartbeat=lambda: None, cancelled=False))
    jobs.statuses[pass_job] = "DONE"
    assert s.vstate.cutover_fk_apply == ((1, 0, 0) if migrated_child else (0, 0, 0))
    assert s.mstate.fk_absent_children() == frozenset({"shop.audit"})
    # One key set: exactly what the pass put in place.
    assert _keys() == expected == sorted(s.mstate.fk_validation_records())
    assert uiv._cutover_pending_foreign_keys(
        s.session, s.eval_store, s.conv_store, "s1", migration_state=s.mstate) == len(expected)
    body = _draw(s)
    assert not _finish(s).disabled, body  # right after the pass, before the read lands
    _run_jobs(jobs, skip={pass_job})  # the watch reads Aurora DSQL
    body = _draw(s)
    assert not _finish(s).disabled
    assert "shop.audit" not in body
    if migrated_child:
        assert "All 1 foreign key validated by Aurora DSQL" in body
    s.runner()
    (ack,) = [kw for action, kw in s.logged if action == "cut over acknowledged"]
    assert ("foreign keys 1/1 applied" if migrated_child
            else "foreign keys: none in scope") in ack["detail"]
    # After a restart the record still says what the pass left out.
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    payload = capture_session_snapshot("s1", SessionConnectionState(), EvaluationState(),
                                       SchemaConversionState(), s.mstate).model_dump_json()
    assert '"foreign_key_absent_children":["shop.audit"]' in payload
    restored = DataMigrationState()
    restored.set_selection(TableSelection(selected_tables=selected))
    apply_session_snapshot(SessionSnapshot.model_validate_json(payload),
                           SessionConnectionState(), EvaluationState(),
                           SchemaConversionState(), restored)
    assert restored.fk_absent_children() == frozenset({"shop.audit"})
    assert _keys(restored) == expected


def test_a_child_left_out_as_absent_counts_again_once_it_is_selected() -> None:
    # The recorded absent set only ever drops an UNSELECTED child pointing in: once the
    # table is part of the migration its keys are the pass's again (an absent one fails).
    conv = {
        "shop.customers": engine.TableConversion(table="shop.customers",
                                                 target_ddl="CREATE TABLE c (id int)"),
        "shop.audit": engine.TableConversion(
            table="shop.audit", target_ddl="CREATE TABLE a (id int)",
            foreign_key_ddls=[_ddl_for("shop.audit", "fk_a_c", "shop.customers")],
            preserved_foreign_keys=[ForeignKeyDef(
                name="fk_a_c", columns=["c"], referenced_table="shop.customers",
                referenced_columns=["id"])]),
    }
    absent = {"shop.audit"}
    for selection, kept in ((["shop.customers"], {"shop.customers"}),
                            (["shop.customers", "shop.audit"], {"shop.customers", "shop.audit"})):
        scoped = engine.foreign_keys_in_scope(conv, selection)
        optional = engine.foreign_keys_pointing_in(scoped, selection)
        assert set(engine.foreign_keys_to_apply(scoped, optional, absent)) == kept


def _ddl_for(table, constraint, parent):
    schema, name = table.split(".")
    pschema, pname = parent.split(".")
    return (f'ALTER TABLE "{schema}"."{name}" ADD CONSTRAINT "{constraint}" FOREIGN KEY ("c") '
            f'REFERENCES "{pschema}"."{pname}" ("id") NOT VALID')


@pytest.mark.parametrize("cascade", [False, True])
def test_remove_foreign_keys_without_a_cdc_start_closes_finish_with_the_apply_again_copy(
    monkeypatch, cascade
) -> None:
    # The keys were applied and validated; then Start CDC's "Remove foreign keys" dropped
    # them and CDC was not started (Cancel, a blocked dialog, an early refusal). Cut over's
    # own result and both waivers go with them, and Finish asks for the keys again.
    import dsql_migrator.core.schema_applier as applier
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    s = _screen(monkeypatch, jobs=_Jobs(), cascade=cascade)
    s.mstate.validation_state_provider = lambda: s.vstate  # as the Data Migration step wires it
    s.vstate.set_cutover_fk_apply((1, 0, 0))
    s.mstate.record_fk_validation(Submission(in_place=(KEY,), jobs={KEY: "j1"}))
    s.mstate.set_fk_validation_reads({KEY: _read(validated=True)}, {KEY: "j1"})
    s.vstate.waive_foreign_keys()
    s.vstate.waive_fk_validation([KEY])
    _draw(s)
    assert not _finish(s).disabled
    dropped: list = []
    monkeypatch.setattr(cdc_ui, "_cdc_fk_connection_factory", lambda _s: (lambda: None))
    monkeypatch.setattr(cdc_ui, "_log_cdc_event", lambda *_a, **_k: None)
    monkeypatch.setattr(applier, "drop_foreign_key",
                        lambda table, name, *, connection_factory: dropped.append((table, name)))
    assert cdc_ui._drop_cdc_blocking_foreign_keys([KEY], s.session, s.mstate) == (1, 0)
    assert dropped == [KEY]
    assert s.vstate.cutover_fk_apply is None
    assert not s.vstate.proceed_without_foreign_keys
    assert s.vstate.proceed_without_fk_validation == frozenset()
    body = _draw(s)
    assert _finish(s).disabled
    assert "Foreign keys are not applied yet" in body
    assert "and again whenever Start CDC or a reload removed it" in body
    assert "Applied 1 foreign key" not in body and "being read now" not in body
    assert s.ui.by("Cut over without enforced foreign keys")
    assert not s.ui.by("Cut over without this check")


K1 = ("shop.orders", "fk_o_c")  # ON DELETE CASCADE: a key CDC can break
K2 = ("shop.lines", "fk_l_o")  # plain


@pytest.mark.parametrize("case", ["incomplete", "pending"])
def test_keys_not_in_place_and_keys_not_validated_are_two_holds_with_two_waivers(
    monkeypatch, case
) -> None:
    # K1 is in place and failed Aurora DSQL's validation; K2 is not in place (the pass
    # could not add it, or a read finds it gone). Finish shows both holds, each with its own
    # waiver: lifting one never lifts, or hides, the other -- in either order.
    s = _screen(monkeypatch, jobs=_Jobs(), inputs=_inputs(
        _table("shop.customers"),
        _table("shop.orders", ("fk_o_c", "shop.customers", "CASCADE")),
        _table("shop.lines", ("fk_l_o", "shop.orders", None)),
    ))
    if case == "incomplete":
        s.vstate.set_cutover_fk_apply((1, 0, 1))
        s.mstate.record_fk_validation(Submission(in_place=(K1,), jobs={K1: "j1"}))
        s.mstate.set_fk_validation_reads({K1: _read(status="failed", details="violates")},
                                         {K1: "j1"})
    else:
        s.vstate.set_cutover_fk_apply((2, 0, 0))
        s.mstate.record_fk_validation(Submission(in_place=(K1, K2), jobs={K1: "j1", K2: "j2"}))
        s.mstate.set_fk_validation_reads(
            {K1: _read(status="failed", details="violates"), K2: _read(present=False)},
            {K1: "j1", K2: "j2"})
    body = _draw(s)
    assert _finish(s).disabled
    assert "Foreign keys are not applied yet" in body
    assert "Foreign keys CDC can break failed Aurora DSQL's validation" in body
    assert "Finish waits for this key to validate" in body
    order = (["Cut over without enforced foreign keys", "Cut over without this check"]
             if case == "incomplete"
             else ["Cut over without this check", "Cut over without enforced foreign keys"])
    (first,) = s.ui.by(order[0])
    first.on_click()
    body = _draw(s)
    assert _finish(s).disabled, "one waiver never lifts the other hold"
    assert not s.ui.by(order[0]) and s.ui.by(order[1])
    if order[0] == "Cut over without enforced foreign keys":
        assert "Foreign keys CDC can break failed Aurora DSQL's validation" in body
    else:
        assert "Foreign keys are not applied yet" in body
        assert "Finish waits for this key to validate" not in body
    (second,) = s.ui.by(order[1])
    second.on_click()
    _draw(s)
    assert not _finish(s).disabled
    lines = [kw["detail"] for action, kw in s.logged if action == "foreign keys waived at cut over"]
    assert len(lines) == 2
    assert any("WITHOUT applying 1 preserved foreign key(s)" in line for line in lines)
    assert any("1 foreign key(s) Aurora DSQL has NOT validated (shop.orders.fk_o_c)" in line
               for line in lines)
    s.runner()
    (ack,) = [kw for action, kw in s.logged if action == "cut over acknowledged"]
    assert ("foreign keys WAIVED (1 not applied), validation check WAIVED; validated by "
            "Aurora DSQL: 0 of 1 (1 NOT validated: shop.orders.fk_o_c)") in ack["detail"]


# ---------------------------------------------------------------------------
# 9. Round 3: the name Aurora DSQL stores, a name for an unnamed key, the card's count
# ---------------------------------------------------------------------------

_BARE_ADD_RE = re.compile(r"(ADD\s+CONSTRAINT\s+)([A-Za-z_]\w*)(\s+FOREIGN\s+KEY)", re.I)
_UNNAMED_ADD_RE = re.compile(r'ALTER TABLE "([^"]+)"\."([^"]+)" ADD FOREIGN KEY \(([^)]*)\)')


class _PgCur(_Cur):
    """The double's cursor behind PostgreSQL's own parser: an unquoted constraint name is
    folded to lower case, and a key added without a name is named
    ``<table>_<columns>_fkey`` (``fkey1``, ... while that name is taken). A VALIDATE job
    settles at once: validated, or failed while a child row has no parent."""

    def execute(self, statement, params=None) -> None:
        db, text = self.db, _text(statement)
        text = _BARE_ADD_RE.sub(lambda m: f'{m.group(1)}"{m.group(2).lower()}"{m.group(3)}', text)
        if (match := _UNNAMED_ADD_RE.search(text)) is not None:
            table = f"{match.group(1)}.{match.group(2)}"
            base = "_".join([match.group(2)] + [
                column.strip().strip('"') for column in match.group(3).split(",")])
            name, number = f"{base}_fkey", 0
            while (table, name) in db.constraints:
                number += 1
                name = f"{base}_fkey{number}"
            text = text.replace(" ADD FOREIGN KEY", f' ADD CONSTRAINT "{name}" FOREIGN KEY', 1)
        super().execute(text, params)
        if text.startswith("ALTER TABLE ASYNC") and self.rows:
            entry = db.jobs[self.rows[0][0]]
            if db.orphans(entry[0][0]):
                entry[1], entry[2] = "failed", f'violates foreign key constraint "{entry[0][1]}"'
            else:
                entry[1] = "completed"
                db.constraints[entry[0]] = True


class _PgConn(_Conn):
    def cursor(self):
        return _PgCur(self.db)


def _pg_double(monkeypatch, *, orphan=False):
    """``_dsql_double`` behind PostgreSQL's parser (every table on it); ``orphan``: a row of
    shop.orders names a customer that is not on Aurora DSQL."""
    db = _dsql_double(monkeypatch, absent=())
    db.connect = lambda: _PgConn(db)
    db.children["shop.orders"] = ("cid", "shop.customers", {10: 99 if orphan else 1})
    return db


def _orders_script(key_line: str) -> str:
    """shop.orders' edited Schema Conversion script with ``key_line`` as its only key."""
    return (
        'CREATE SCHEMA IF NOT EXISTS "shop";\n'
        'CREATE TABLE "shop"."orders" (\n  "id" INT,\n  "cid" INT,\n  PRIMARY KEY ("id")\n);\n'
        f"{key_line};\n"
    )


def _cut_over_pass(s, jobs) -> str:
    """Tick the drained attestation, click Apply foreign keys and run the pass's job."""
    s.vstate.set_cutover_cdc_drained_confirmed(True)
    _draw(s)
    (apply_button,) = s.ui.by("Apply foreign keys")
    before = set(jobs.works)
    apply_button.on_click()
    (pass_job,) = [job_id for job_id in jobs.works if job_id not in before]
    jobs.works[pass_job](SimpleNamespace(heartbeat=lambda: None, cancelled=False))
    jobs.statuses[pass_job] = "DONE"
    return pass_job


def test_an_unquoted_constraint_name_is_read_as_aurora_dsql_stores_it() -> None:
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys

    ddl = ('ALTER TABLE "shop"."orders" ADD CONSTRAINT FK_Orders_Cust FOREIGN KEY ("cid") '
           'REFERENCES "shop"."customers" ("id") NOT VALID')
    assert engine._constraint_name_from_ddl(ddl) == "fk_orders_cust"
    quoted = ddl.replace("FK_Orders_Cust", '"FK_Orders_Cust"')
    assert engine._constraint_name_from_ddl(quoted) == "FK_Orders_Cust"
    # Only A-Z fold, as in PostgreSQL.
    assert engine._constraint_name_from_ddl(ddl.replace("FK_Orders_Cust", "FK_Ünique")) == (
        "fk_Ünique")
    conv = {ORDERS: engine.TableConversion(table=ORDERS, target_ddl="CREATE TABLE x (id int)",
                                           foreign_key_ddls=[ddl])}
    assert fkv.pending_foreign_keys(conv) == [(ORDERS, "fk_orders_cust")]
    # The CDC precondition owns the key under the name the catalog lists.
    owned = engine.preserved_foreign_key_names(conv)
    assert owned == {ORDERS: ["fk_orders_cust"]}
    assert cdc_blocking_foreign_keys({ORDERS: ["fk_orders_cust"]}, owned).ours == [
        (ORDERS, "fk_orders_cust")]


def test_a_key_renamed_mysql_style_is_read_in_place_validated_and_opens_finish(
    monkeypatch,
) -> None:
    # The operator renamed the key FK_Orders_Cust, unquoted: Aurora DSQL stores
    # fk_orders_cust. Read under the name as typed it was "absent" after a clean (1, 0, 0),
    # its VALIDATE was never submitted and Finish stayed closed, with only the waiver of keys
    # not applied (whose line said it was not applied) to get past it.
    import dsql_migrator.ui.validation as uiv

    key = (ORDERS, "fk_orders_cust")
    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs, cascade=False, edited={ORDERS: _orders_script(
        'ALTER TABLE "shop"."orders" ADD CONSTRAINT FK_Orders_Cust FOREIGN KEY ("cid") '
        'REFERENCES "shop"."customers" ("id") ON DELETE CASCADE NOT VALID')})
    db = _pg_double(monkeypatch)
    keys, breakable, _defs = uiv._cutover_fk_keys(
        s.session, s.eval_store, s.conv_store, "s1", migration_state=s.mstate, cdc_in_use=True)
    assert keys == [key] and breakable == frozenset({key})
    pass_job = _cut_over_pass(s, jobs)
    assert s.vstate.cutover_fk_apply == (1, 0, 0)
    assert db.statements('VALIDATE CONSTRAINT "fk_orders_cust"')
    assert db.constraints == {key: True}
    assert set(s.mstate.fk_validation_records()) == {key}
    _run_jobs(jobs, skip={pass_job})  # the watch reads Aurora DSQL
    body = _draw(s)
    assert not _finish(s).disabled, body
    assert "All 1 foreign key validated by Aurora DSQL" in body
    assert "no longer on Aurora DSQL" not in body
    assert not s.ui.by("Cut over without enforced foreign keys")
    # A Validation re-run clears this session's result: the read still finds it in place.
    s.vstate.set_cutover_fk_apply(None)
    _draw(s)
    assert not _finish(s).disabled
    s.runner()
    (ack,) = [kw for action, kw in s.logged if action == "cut over acknowledged"]
    assert "foreign keys 1/1 in place" in ack["detail"] and "WAIVED" not in ack["detail"]


def test_an_unnamed_key_gets_the_name_postgresql_gives_it_written_into_its_statement() -> None:
    # PostgreSQL 17 gave the same statements these names (live, a scratch database), but for
    # orders_x_fkey1: it keeps clear of a name a LATER key of the table writes, which
    # PostgreSQL, adding in order, gives the unnamed key -- and then refuses the named one
    # (42710, read as in place).
    named = engine._named_foreign_key_ddls
    plain = 'ALTER TABLE "s"."orders" ADD FOREIGN KEY ("cid") REFERENCES "s"."p" ("id") NOT VALID'
    on_x = plain.replace('("cid")', '("x")')
    explicit = on_x.replace("ADD FOREIGN KEY", 'ADD CONSTRAINT "orders_x_fkey" FOREIGN KEY')
    composite = "ALTER TABLE s.orders ADD FOREIGN KEY (cid, x) REFERENCES s.p2 (a, b) NOT VALID"
    result = named([plain, plain, on_x, explicit, composite])
    assert [name for name, _ in result] == [
        "orders_cid_fkey", "orders_cid_fkey1", "orders_x_fkey1", "orders_x_fkey",
        "orders_cid_x_fkey"]
    assert result[0][1] == plain.replace(
        "ADD FOREIGN KEY", 'ADD CONSTRAINT "orders_cid_fkey" FOREIGN KEY')
    assert result[3][1] is explicit  # a named key runs as written
    assert result[4][1] == composite.replace(
        "ADD FOREIGN KEY", 'ADD CONSTRAINT "orders_cid_x_fkey" FOREIGN KEY')
    assert engine._constraint_name_from_ddl(result[1][1]) == "orders_cid_fkey1"
    # Every reader names the keys alike: Finish's key set, the CDC ownership test.
    conv = {ORDERS: engine.TableConversion(table=ORDERS, target_ddl="CREATE TABLE x (id int)",
                                           foreign_key_ddls=[plain, plain])}
    assert fkv.pending_foreign_keys(conv) == [(ORDERS, "orders_cid_fkey"),
                                              (ORDERS, "orders_cid_fkey1")]
    assert engine.preserved_foreign_key_names(conv) == {
        ORDERS: ["orders_cid_fkey", "orders_cid_fkey1"]}
    # Names as stored: an unquoted one folded, a quoted one as written, each cut to 63 bytes;
    # then the longer of the table and the columns is cut first to fit 63 bytes in all.
    bare = "ALTER TABLE ONLY S.Lower_T ADD FOREIGN KEY (CustId) REFERENCES s.p (id) NOT VALID"
    assert named([bare]) == [("lower_t_custid_fkey", bare.replace(
        "ADD FOREIGN KEY", 'ADD CONSTRAINT "lower_t_custid_fkey" FOREIGN KEY'))]
    mixed = 'ALTER TABLE "s"."Orders" ADD FOREIGN KEY ("CustId") REFERENCES "s"."p" ("id")'
    assert named([mixed])[0][0] == "Orders_CustId_fkey"

    def _key(table, *columns):
        cols = ", ".join(f'"{column}"' for column in columns)
        return f'ALTER TABLE "s"."{table}" ADD FOREIGN KEY ({cols}) REFERENCES "s"."p" ("id")'

    long_table = "t" * 40 + "_orders_with_a_very_long_name_x"
    assert named([_key(long_table, "cid")])[0][0] == "t" * 40 + "_orders_with_a_cid_fkey"
    assert named([_key("b" * 45, "e" * 45)])[0][0] == "b" * 29 + "_" + "e" * 28 + "_fkey"
    assert [name for name, _ in named([_key("lc", "c" * 50, "d" * 50)] * 2)] == [
        "lc_" + "c" * 50 + "_dddd_fkey", "lc_" + "c" * 50 + "_ddd_fkey1"]
    assert [name for name, _ in named([_key("주문" * 12, "열이름")] * 2)] == [
        "주문" * 8 + "_열이름_fkey", "주문" * 7 + "주_열이름_fkey1"]
    # A statement whose key cannot be read runs as written, with no name.
    other = "ALTER TABLE t ADD COLUMN a int, ADD FOREIGN KEY (a) REFERENCES u (id)"
    assert named([other]) == [(None, other)]


def test_an_unnamed_cascade_key_is_validated_held_and_never_added_twice(monkeypatch) -> None:
    # An ON DELETE CASCADE key added without a name, with a child row whose customer is not
    # on Aurora DSQL. With no name to read it was never one CDC can break and never
    # validated, so Finish opened right after the pass; each re-apply added another copy
    # (orders_cid_fkey1, ...). It is added as orders_cid_fkey, validated, and held.
    import dsql_migrator.ui.validation as uiv

    key = (ORDERS, "orders_cid_fkey")
    jobs = _Jobs()
    s = _screen(monkeypatch, jobs=jobs, cascade=False, edited={ORDERS: _orders_script(
        'ALTER TABLE "shop"."orders" ADD FOREIGN KEY ("cid") '
        'REFERENCES "shop"."customers" ("id") ON DELETE CASCADE NOT VALID')})
    db = _pg_double(monkeypatch, orphan=True)
    keys, breakable, _defs = uiv._cutover_fk_keys(
        s.session, s.eval_store, s.conv_store, "s1", migration_state=s.mstate, cdc_in_use=True)
    assert keys == [key] and breakable == frozenset({key})
    pass_job = _cut_over_pass(s, jobs)
    assert s.vstate.cutover_fk_apply == (1, 0, 0)
    assert db.statements('ADD CONSTRAINT "orders_cid_fkey" FOREIGN KEY ("cid")')
    assert not db.statements("ADD FOREIGN KEY")
    assert set(s.mstate.fk_validation_records()) == {key}
    _run_jobs(jobs, skip={pass_job})  # the watch reads the failed validation
    body = _draw(s)
    assert _finish(s).disabled
    assert "Foreign keys CDC can break failed Aurora DSQL's validation" in body
    assert "shop.orders.orders_cid_fkey" in body
    assert s.ui.by("Cut over without this check")
    assert not s.ui.by("Cut over without enforced foreign keys")
    # Applied again: 42710, so the key is in place and no second copy is added.
    _cut_over_pass(s, jobs)
    assert s.vstate.cutover_fk_apply == (1, 0, 0)
    assert list(db.constraints) == [key]


def test_the_full_load_count_is_the_keys_the_pass_adds(monkeypatch) -> None:
    # The Data Migration card's denominator: an unselected child pointing in that the last
    # pass found absent is left out, as the pass and Cut over leave it out; a migrated table
    # is never left out. Every key left out: the pass publishes (0, 0), so the card cannot
    # fall back to the pre-count it showed before the pass.
    conv = {
        name: engine.TableConversion(
            table=name, target_ddl="CREATE TABLE x (id int)",
            foreign_key_ddls=[_ddl_for(name, constraint, "shop.customers")])
        for name, constraint in ((ORDERS, "fk_o_c"), ("shop.audit", "fk_a_c"))
    }
    inputs = SimpleNamespace(table_conversions=conv, fk_optional_tables=frozenset({"shop.audit"}),
                             is_cdc_migration=False, cdc_coexisting=False, cdc_stack_name=None)
    migrator = SimpleNamespace(_inputs=inputs)
    assert engine.pending_foreign_key_count(migrator) == 2  # no pass yet: it may be needed
    assert engine.pending_foreign_key_count(migrator, absent={"shop.audit"}) == 1
    assert engine.pending_foreign_key_count(migrator, absent={ORDERS}) == 2
    progress: list = []
    monkeypatch.setattr(engine, "_target_table_present", lambda _cf, _name: False)
    result = engine.apply_preserved_foreign_keys(
        {"shop.audit": conv["shop.audit"]}, lambda: None,
        skip_if_absent=frozenset({"shop.audit"}),
        on_progress=lambda done, total: progress.append((done, total)))
    assert result == (0, 0, 0) and progress == [(0, 0)]


def _full_load_card(monkeypatch, *, only_absent: bool):
    """The Data Migration step of a finished Full load only of shop.customers (and, unless
    ``only_absent``, shop.orders with its key into customers), with shop.audit --
    unselected, not on Aurora DSQL -- keeping its key into customers in scope. Returns the FK
    card's providers (``_render_full_load_step`` records them), the Aurora DSQL double, the
    job manager double and the screen."""
    from dsql_migrator.core.models import (
        AssessmentReport,
        ChunkState,
        MigrationJob,
        SourceConnectionConfig,
        SourceInventory,
        StepStatus,
        TableSelection,
        TargetConnectionConfig,
        TargetInventory,
        Watermark,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore, MigrationType
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.workflow import WorkflowStep, with_status
    from tests.test_ui_data_migration import _ScreenUi

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    db = _dsql_double(monkeypatch)  # shop.audit is not on Aurora DSQL
    monkeypatch.setattr(engine, "DsqlConnector",
                        lambda *_a, **_k: SimpleNamespace(connect=db.connect))
    card: dict = {}
    monkeypatch.setattr(dm, "_render_full_load_step", lambda *_a, **kw: card.update(kw))
    selected = ["shop.customers"] + ([] if only_absent else [ORDERS])
    tables = [_table("shop.customers"), _table("shop.audit", ("fk_a_c", "shop.customers", None))]
    if not only_absent:
        tables.append(_table(ORDERS, ("fk_o_c", "shop.customers", None)))
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(host="h", database="shop", username="u"), None)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"))
    session.set_migration_type(MigrationType.FULL_LOAD_ONLY)
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, StepStatus.DONE))
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=SourceInventory(tables=tables), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = [f"table:{t.name}" for t in tables]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.job_id = "load"
    state.set_selection(TableSelection(selected_tables=selected))
    jobs = _Jobs()
    load = MigrationJob(
        job_id="load", status="DONE",
        chunks=[ChunkState(chunk_id=name, status="DONE", attempts=1) for name in selected],
        watermark=Watermark(snapshot_timestamp=datetime(2026, 10, 1, tzinfo=timezone.utc),
                            table_row_counts={name: 1 for name in selected}))
    status_of = jobs.get_status
    jobs.get_status = lambda job_id: load if job_id == "load" else status_of(job_id)
    jobs.request_cancel = lambda _job_id: None
    content, _runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs,
    )
    content(lambda: None)
    return card, db, jobs, content


@pytest.mark.parametrize("only_absent", [False, True])
def test_the_full_load_card_clears_after_a_clean_pass_that_left_an_absent_child_out(
    monkeypatch, only_absent
) -> None:
    # Full load only: the card counted shop.audit's key -- left out by the pass by design --
    # so a clean pass showed "Foreign keys: 1 of 2 applied" and "1 foreign key(s) were NOT
    # reached" with Apply, forever ("0 of 1" when it was the only key), while Cut over said
    # every key was applied. It counts the pass's own key set now.
    import dsql_migrator.ui.data_migration._full_load_ui as flu

    card, db, jobs, content = _full_load_card(monkeypatch, only_absent=only_absent)
    # Before any pass the audit key may be needed.
    assert card["foreign_keys_pending"]() == (1 if only_absent else 2)
    card["apply_foreign_keys"]()
    (pass_job,) = [job_id for job_id in jobs.works if jobs.statuses[job_id] == "RUNNING"]
    jobs.works[pass_job](SimpleNamespace(heartbeat=lambda: None, cancelled=False))
    jobs.statuses[pass_job] = "DONE"
    content(lambda: None)
    applied = card["foreign_keys_applied"]()
    assert applied == ((0, 0, 0) if only_absent else (1, 0, 0))
    expected_pending = 0 if only_absent else 1
    assert card["foreign_keys_pending"]() == expected_pending
    assert list(db.constraints) == ([] if only_absent else [(ORDERS, "fk_o_c")])
    notices: list = []
    monkeypatch.setattr(flu, "render_notice", lambda _ui, *, tone="info", header="", body="",
                        **_k: notices.append((tone, header, body)))
    render_ui = _RenderUi()
    flu._render_foreign_key_action(
        render_ui, terminal=True, pending=card["foreign_keys_pending"](), applied=applied,
        apply_foreign_keys=lambda: None, connections_ready=True,
        progress=card["foreign_keys_progress"](), validation=None)
    ((tone, header, body),) = notices
    assert tone == "success", (header, body)
    assert header == f"Foreign keys: {applied[0]} applied"
    assert "NOT reached" not in body
    assert "Apply foreign keys" not in render_ui.body()
    # Applied again (a stopped pass, a changed table set): it announces the same count.
    card["apply_foreign_keys"]()
    assert card["foreign_keys_progress"]() == (0, expected_pending)
    assert sys.modules["nicegui"].ui.notified[-1][0].startswith(
        f"Adding {expected_pending} foreign key(s)")


# ---------------------------------------------------------------------------
# The manual, in three languages
# ---------------------------------------------------------------------------


def test_the_manual_names_pad_space_keys() -> None:
    # Chapter 2 is a user guide (2026-10-25): the listing window and the log placeholder are
    # CHANGELOG detail; the cut-over wait on PAD SPACE keys is what a user meets.
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual"
    needles = {
        ("en", "02-evaluation-and-schema-conversion.md"): (
            "PAD SPACE", "`utf8mb4_bin`"),
        ("en", "10-conclusion.md"): ("PAD SPACE",),
        ("ko", "02-evaluation-and-schema-conversion.md"): (
            "PAD SPACE", "`utf8mb4_bin`"),
        ("ko", "10-conclusion.md"): ("PAD SPACE",),
        ("ja", "02-evaluation-and-schema-conversion.md"): (
            "PAD SPACE", "`utf8mb4_bin`"),
        ("ja", "10-conclusion.md"): ("PAD SPACE",),
    }
    for (lang, name), phrases in needles.items():
        flat = " ".join((manual / lang / name).read_text(encoding="utf-8").split())
        for phrase in phrases:
            assert phrase in flat, (lang, name, phrase)


def test_the_manual_says_finish_waits_for_the_pass_keys_in_two_holds() -> None:
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual"
    needles = {
        # (2026-10-28) The runbook keeps the two facts a user acts on: apply the keys again after
        # the final drain (a CDC start and Start CDC's Remove foreign keys clear the last apply),
        # and that keys not in place and keys not validated are two holds with a choice each.
        "en": ("**Remove foreign keys**", "two separate holds, each with its own choice"),
        "ko": ("**Remove foreign keys**", "서로 다른 두 보류이고 각각 따로 선택합니다"),
        "ja": ("**Remove foreign keys**", "別々の二つの保留で、それぞれに別の選択があります"),
    }
    for lang, phrases in needles.items():
        flat = " ".join((manual / lang / "10-conclusion.md").read_text(encoding="utf-8").split())
        for phrase in phrases:
            assert phrase in flat, (lang, phrase)


def test_the_manual_says_how_a_key_you_add_or_rename_is_named() -> None:
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual"
    needles = {
        "en": "**A key you add or rename.**",
        "ko": "**직접 추가하거나 이름을 바꾼 키.**",
        "ja": "**自分で追加・名前変更したキー。**",
    }
    for lang, header in needles.items():
        flat = " ".join(
            (manual / lang / "02-evaluation-and-schema-conversion.md").read_text(
                encoding="utf-8").split())
        for phrase in (header, "`FK_Orders_Cust`", "`fk_orders_cust`"):
            assert phrase in flat, (lang, phrase)
