# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G12: in Full load only the table set can change after a Full Load (no Start over).

The lock lifts through ONE predicate (``table_set_editable_after_load``); every load,
Validation and Cut over surface then follows the CURRENT selection through a scoped view of
the job record, and a switch to a CDC type stays locked while a table selected since the
load has no fresh watermark (a removal alone never locks it while CDC never started for the
migration: CDC captures the loaded tables still selected). CDC only keeps today's lock (its
text also names Full load + CDC while that could still drop tables); Full load + CDC narrows
until CDC starts (G16, tests/test_g16_full_load_and_cdc_narrowing.py) and keeps today's lock
without per-table watermark counts or once CDC has started.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import (
    ChunkState,
    MigrationJob,
    StepStatus,
    TableSelection,
    Watermark,
    WorkflowState,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

from tests.test_ui_data_migration import _StubJob, _StubJobManager

TODAY = (
    "Locked — a Full Load has already run for this table set. Use 'Start over' to "
    "migrate a different set of tables."
)


def _job(status="DONE", chunks=(), counts=None, job_id="job-1"):
    return MigrationJob(
        job_id=job_id,
        status=status,
        chunks=[ChunkState(**c) if isinstance(c, dict) else c for c in chunks],
        watermark=Watermark(
            snapshot_timestamp=datetime(2026, 10, 1, tzinfo=timezone.utc),
            table_row_counts=dict(counts if counts is not None else {}),
        ),
    )


def _c(name, status="DONE", **kw):
    return ChunkState(chunk_id=name, status=status, attempts=kw.pop("attempts", 1), **kw)


class _Jobs:
    """A job manager returning real MigrationJob records (and their errors)."""

    def __init__(self, jobs=None, errors=None):
        self.jobs = dict(jobs or {})
        self.errors = dict(errors or {})
        self.submitted: list = []

    def get_status(self, job_id):
        if job_id not in self.jobs:
            raise JobNotFoundError(job_id)
        return self.jobs[job_id]

    def get_error(self, job_id):
        return self.errors.get(job_id)

    def is_cancel_requested(self, _job_id):
        return False

    def request_cancel(self, _job_id):
        return None

    def submit(self, fn):
        self.submitted.append(fn)
        return f"job-{len(self.submitted) + 1}"


def _flo(selected=("app.a", "app.b"), *, touched=True, job_id="job-1",
         mtype=MigrationType.FULL_LOAD_ONLY):
    state = DataMigrationState()
    state.migration_type = mtype
    state.job_id = job_id
    state.selection = TableSelection(selected_tables=list(selected))
    state.selection_touched = touched
    return state


def _session(status=StepStatus.DONE):
    return SimpleNamespace(
        workflow=with_status(WorkflowState(), WorkflowStep.FULL_LOAD, status)
    )


# ---------------------------------------------------------------------------
# 1. The one predicate: the lock lifts for Full load only, and only there
# ---------------------------------------------------------------------------


def test_full_load_only_table_set_is_editable_after_a_finished_load() -> None:
    # FAILS WITHOUT THE FIX: this exact state was "Locked — a Full Load has already run".
    from dsql_migrator.ui.data_migration import selection_lock_reason, table_set_lock_reason

    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_ONLY
    state.job_id = "job-1"
    mgr = _StubJobManager({"job-1": _StubJob("COMPLETED")})
    assert selection_lock_reason(
        state, mgr, status=StepStatus.DONE, migration_type=MigrationType.FULL_LOAD_ONLY,
        has_job=True,
    ) is None
    assert table_set_lock_reason(state, mgr, _session(StepStatus.DONE)) is None


@pytest.mark.parametrize("mtype", [MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY])
def test_cdc_types_keep_todays_lock_byte_for_byte(mtype) -> None:
    from dsql_migrator.ui.data_migration import selection_lock_reason, table_set_lock_reason

    state = DataMigrationState()
    state.migration_type = mtype
    state.job_id = "job-1"
    mgr = _StubJobManager({"job-1": _StubJob("COMPLETED")})
    assert selection_lock_reason(
        state, mgr, status=StepStatus.DONE, migration_type=mtype, has_job=True
    ) == TODAY
    assert table_set_lock_reason(state, mgr, _session(StepStatus.DONE)) == TODAY


def test_a_running_load_fk_pass_or_default_restore_is_a_wait_never_start_over() -> None:
    from dsql_migrator.ui.data_migration import (
        TABLE_SET_WAIT_FK,
        TABLE_SET_WAIT_LOAD,
        TABLE_SET_WAIT_RESTORE,
        selection_lock_reason,
    )

    def reason(state, mgr, status=StepStatus.DONE):
        return selection_lock_reason(
            state, mgr, status=status, migration_type=MigrationType.FULL_LOAD_ONLY,
            has_job=True,
        )

    running = _flo()
    assert reason(running, _StubJobManager({"job-1": _StubJob("RUNNING")})) == TABLE_SET_WAIT_LOAD
    done = _StubJobManager({"job-1": _StubJob("COMPLETED")})
    assert reason(_flo(), done, StepStatus.IN_PROGRESS) == TABLE_SET_WAIT_LOAD
    fk = _flo()
    fk.set_fk_apply_job("fk-1")
    fk_mgr = _StubJobManager({"job-1": _StubJob("COMPLETED"), "fk-1": _StubJob("RUNNING")})
    assert reason(fk, fk_mgr) == TABLE_SET_WAIT_FK
    restoring = _flo()
    restoring.cutover_restore_in_flight = lambda: True
    assert reason(restoring, done) == TABLE_SET_WAIT_RESTORE
    for text in (TABLE_SET_WAIT_LOAD, TABLE_SET_WAIT_FK, TABLE_SET_WAIT_RESTORE):
        assert "Start over" not in text and text.startswith("Locked while")
    assert reason(_flo(), done) is None


def _with_connectors(state):
    state.cdc_connector_names = ["x"]
    return state, _StubJobManager({"job-1": _StubJob("COMPLETED")})


def _with_phase(phase):
    def build(state):
        state.set_cdc_stack_phase(phase)
        return state, _StubJobManager({"job-1": _StubJob("COMPLETED")})
    return build


def _with_infra_job(state):
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    return state, _StubJobManager(
        {"job-1": _StubJob("COMPLETED"), "infra-1": _StubJob("RUNNING")}
    )


def _with_cdc_job(kind, phase=None):
    """A RUNNING CDC lifecycle job of ``kind`` (start / stop / delete), the phase unprobed
    unless given -- e.g. "absent" while a delete job still runs after the stack is gone."""
    def build(state):
        state.set_cdc_deploy_job_id(f"{kind}-1", kind=kind)
        if phase is not None:
            state.set_cdc_stack_phase(phase)
        return state, _StubJobManager(
            {"job-1": _StubJob("COMPLETED"), f"{kind}-1": _StubJob("RUNNING")})
    return build


def _with_deleting_stack(state):
    # A real probe pairs this status with phase "unstable"; phase None pins the
    # cdc_stack_being_deleted clause on its own (not through _EXCLUSION_BAKED_PHASES).
    state.set_cdc_stack_phase(None, status="DELETE_IN_PROGRESS")
    return state, _StubJobManager({"job-1": _StubJob("COMPLETED")})


@pytest.mark.parametrize(
    "evidence", [_with_connectors, _with_phase("infra"), _with_infra_job, _with_phase("running"),
                 _with_cdc_job("start"), _with_cdc_job("stop"),
                 _with_cdc_job("delete", phase="absent"), _with_deleting_stack]
)
def test_cdc_evidence_keeps_full_load_only_locked_with_todays_text(evidence) -> None:
    from dsql_migrator.ui.data_migration import (
        selection_lock_reason,
        table_set_editable_after_load,
    )

    state, mgr = evidence(_flo())
    assert table_set_editable_after_load(state, mgr) is False
    reason = selection_lock_reason(
        state, mgr, status=StepStatus.DONE, migration_type=MigrationType.FULL_LOAD_ONLY,
        has_job=True,
    )
    assert reason == TODAY


def test_a_pruned_record_keeps_todays_lock() -> None:
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = _flo()
    assert selection_lock_reason(
        state, _StubJobManager({}), status=StepStatus.DONE,
        migration_type=MigrationType.FULL_LOAD_ONLY, has_job=False,
    ) == TODAY


def test_the_r4_text_names_the_table_set_change_for_full_load_only_only() -> None:
    from dsql_migrator.ui.data_migration._cdc_monitoring import exclusion_lock_state

    mgr = _StubJobManager({"job-1": _StubJob("COMPLETED")})
    lock = exclusion_lock_state(_flo(), mgr)
    assert lock.tier == 3 and lock.cell == "R4" and lock.offers_change_and_reload
    assert "which tables migrate can still change" in lock.reason
    assert "left as is on Aurora DSQL (not dropped)" in lock.reason
    assert "needs Start over" not in lock.reason
    # Full load + CDC (no stack + a job): today's text.
    fc = exclusion_lock_state(_flo(mtype=MigrationType.FULL_LOAD_AND_CDC), mgr)
    assert fc.reason.endswith("Changing the table set needs Start over.")


# ---------------------------------------------------------------------------
# 2. The engine: a newly selected table is tracked, the watermark is never extended
# ---------------------------------------------------------------------------


def test_seed_retry_chunks_seeds_a_pending_chunk_for_a_name_the_prior_run_lacked() -> None:
    # FAILS WITHOUT THE FIX: "b" got no chunk, so its load was untracked.
    from dsql_migrator.ui.data_migration._full_load_engine import _seed_retry_chunks

    job = MigrationJob(job_id="retry-1")
    _seed_retry_chunks(job, [_c("a", rows_loaded=5)], {"b"})
    assert [(c.chunk_id, c.status, c.attempts) for c in job.chunks] == [
        ("a", "DONE", 1), ("b", "PENDING", 0)]
    assert job.chunks[0].rows_loaded == 5
    both = MigrationJob(job_id="retry-2")
    _seed_retry_chunks(both, [_c("a", attempts=2)], {"a", "b"})
    assert [(c.chunk_id, c.status, c.attempts) for c in both.chunks] == [
        ("a", "PENDING", 2), ("b", "PENDING", 0)]


@pytest.mark.parametrize("run_tables", [None, ["orders"]])
def test_a_retry_loads_a_new_table_under_the_original_watermark(monkeypatch, run_tables) -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    from tests.test_ui_data_migration import _FakeMigrator, _tables

    seen: dict = {}

    def _count(_migrator, tables=None):
        seen["fk"] = list(tables or [])
        return 0

    def _sync(_inputs, table_names, *, sync=None):
        seen["sync"] = list(table_names)
        return {}

    monkeypatch.setattr(engine, "pending_foreign_key_count", _count)
    monkeypatch.setattr(engine, "_log_identity_sequence_sync", _sync)
    watermark = Watermark(
        snapshot_timestamp=datetime(2026, 10, 1, tzinfo=timezone.utc),
        table_row_counts={"orders": 10},
    )
    manager = JobManager()
    retry = [t for t in _tables() if t.name == "customers"]
    job_id = manager.submit(lambda h: engine.run_full_load_retry(
        h, [_c("orders", rows_loaded=10)], retry, migrator=_FakeMigrator(
            rows_by_table={"customers": 3}),
        error_log=ErrorLogStore(), watermark=watermark, run_table_names=run_tables,
    ))
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)
    by = {c.chunk_id: c for c in job.chunks}
    assert by["customers"].status == "DONE" and by["customers"].rows_loaded == 3
    assert by["orders"].status == "DONE"
    # The original watermark, NOT extended: CDC's coverage signal stays the original set.
    assert set(job.watermark.table_row_counts) == {"orders"}
    expected = ["orders", "customers"] if run_tables is None else run_tables
    assert seen["fk"] == expected and seen["sync"] == expected


# ---------------------------------------------------------------------------
# 3. The scoped view of the record
# ---------------------------------------------------------------------------


def test_scope_full_load_job_is_the_record_itself_unless_the_set_changed() -> None:
    from dsql_migrator.ui.data_migration._models import scope_change, scope_full_load_job

    job = _job("FAILED", [_c("a"), _c("b", "FAILED")], counts={"a": 1, "b": 1})
    assert scope_full_load_job(job, None) is job
    assert scope_full_load_job(job, ["b", "a"]) is job
    running = _job("RUNNING", [_c("a")])
    assert scope_full_load_job(running, ["a", "c"]) is running
    view = scope_full_load_job(job, ["a", "c"])
    assert view is not job and view.job_id == job.job_id and view.watermark == job.watermark
    assert [(c.chunk_id, c.status, c.attempts) for c in view.chunks] == [
        ("a", "DONE", 1), ("c", "PENDING", 0)]
    assert view.progress_pct == 50.0 and view.error_count == 0
    assert [c.chunk_id for c in job.chunks] == ["a", "b"]  # never mutated
    assert scope_change(job, view) == (["c"], ["b"])
    assert scope_change(job, job) == ([], [])


def test_full_load_scope_names_only_in_the_lifted_lock_state() -> None:
    from dsql_migrator.ui.data_migration._models import full_load_scope_names

    mgr = _StubJobManager({"job-1": _StubJob("COMPLETED")})
    assert full_load_scope_names(_flo(), mgr, ["app.a"]) == ["app.a"]
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        assert full_load_scope_names(_flo(mtype=mtype), mgr, ["app.a"]) is None
    assert full_load_scope_names(_flo(touched=False), mgr, ["app.a"]) is None
    assert full_load_scope_names(_flo(), mgr, []) is None
    evidence = _flo()
    evidence.cdc_connector_names = ["c"]
    assert full_load_scope_names(evidence, mgr, ["app.a"]) is None
    assert full_load_scope_names(_flo(), _StubJobManager({}), ["app.a"]) is None


class _NoLog:
    def records(self, _key):
        return []


@pytest.mark.parametrize(
    "status, chunks, names, accepted, expected",
    [
        # Identity parity with the live poll's mapping (an unchanged set).
        ("DONE", [_c("a")], ["a"], False, StepStatus.DONE),
        ("FAILED", [_c("a", "FAILED")], ["a"], False, StepStatus.FAILED),
        ("CANCELLED", [_c("a")], ["a"], False, StepStatus.FAILED),
        ("FAILED", [_c("a", rows_quarantined=2)], ["a"], True, StepStatus.DONE),
        # Grow on a DONE job: the new table is not loaded yet.
        ("DONE", [_c("a")], ["a", "c"], False, StepStatus.FAILED),
        # The only unfinished table is no longer selected.
        ("FAILED", [_c("a"), _c("b", "FAILED")], ["a"], False, StepStatus.DONE),
        # A run-level failure (every chunk loaded) after a removal stays FAILED.
        ("FAILED", [_c("a"), _c("b")], ["a"], False, StepStatus.FAILED),
        ("CANCELLED", [_c("a"), _c("b")], ["a"], False, StepStatus.FAILED),
        # Unaccepted quarantine in a selected table holds the step...
        ("DONE", [_c("a", rows_quarantined=1), _c("b")], ["a"], False, StepStatus.FAILED),
        # ...and quarantine only in an unselected table does not.
        ("FAILED", [_c("a"), _c("b", rows_quarantined=1)], ["a"], False, StepStatus.DONE),
        ("RUNNING", [_c("a")], ["a", "c"], False, None),
    ],
)
def test_full_load_step_for_scope(status, chunks, names, accepted, expected) -> None:
    from dsql_migrator.ui.data_migration._full_load_ui import full_load_step_for_scope
    from dsql_migrator.ui.data_migration._models import scope_full_load_job

    job = _job(status, chunks)
    assert full_load_step_for_scope(
        job, scope_full_load_job(job, names), accepted=accepted, error_log=_NoLog()
    ) is expected


# ---------------------------------------------------------------------------
# 4. Retry: unselected tables are left as is, new ones need their checks
# ---------------------------------------------------------------------------


def test_retry_scope_drops_unselected_tables_and_guards_new_ones() -> None:
    from dsql_migrator.ui.data_migration._models import retry_scope_block_reason

    record = ["a", "b"]
    view = ["a", "c"]
    dropped = retry_scope_block_reason(["a", "b"], view, record)
    assert dropped.names == ("a",) and dropped.notice_tone == "info"
    assert dropped.notice.startswith("b is no longer selected for this migration")
    assert "left as is on Aurora DSQL and not reloaded" in dropped.notice
    only = retry_scope_block_reason(["b"], view, record)
    assert only.names == () and only.notice_tone == "warning"
    refused = retry_scope_block_reason(["a", "c"], view, record, lambda new: "Run checks.")
    assert refused.names == () and refused.new == ("c",)
    assert refused.refusal == (
        "Run checks. Untick c in the retry list to retry the other tables now.")
    allowed = retry_scope_block_reason(["a", "c"], view, record, lambda new: None)
    assert allowed.names == ("a", "c") and allowed.refusal is None


def test_the_run_guard_requires_the_checks_for_tables_added_after_the_load() -> None:
    from dsql_migrator.core.models import ColumnDef, MigrationMode, SourceInventory, TableDef
    from dsql_migrator.ui.data_migration import full_load_run_guard_reason

    from tests.test_ui_data_migration import _fl_report

    inv = SourceInventory(tables=[TableDef(
        name="shop.new", columns=[ColumnDef(name="id", mysql_type="int")], primary_key=["id"])])
    state = DataMigrationState()
    assert full_load_run_guard_reason(state, inv, has_run=True) is None  # unchanged
    blocked = full_load_run_guard_reason(state, inv, has_run=True, added_since_run=["shop.new"])
    assert blocked == (
        "Re-run the prerequisite checks — shop.new was added to the selection after the "
        "last Full Load, so it was never checked.")
    # A present report that lacks the table still blocks through prereq_scope_gap -- in
    # the same words, the pronoun following the count (it read "c was ... they were").
    state.set_prereq_report(MigrationMode.FULL_LOAD, _fl_report("orders"))
    state.selection = TableSelection(selected_tables=["orders", "shop.new"])
    assert full_load_run_guard_reason(
        state, inv, has_run=True, added_since_run=["shop.new"]) == (
        "Re-run the prerequisite checks — shop.new was added to the selection after the "
        "checks ran, so it was never checked.")
    state.selection = TableSelection(selected_tables=["orders", "shop.new", "shop.more"])
    assert full_load_run_guard_reason(state, inv, has_run=True).endswith(
        "shop.more, shop.new were added to the selection after the checks ran, so they "
        "were never checked.")


# ---------------------------------------------------------------------------
# 5. Switching to a CDC type: locked until a fresh watermark covers the set
# ---------------------------------------------------------------------------


def _coverage_state(selected, *, migratable=("a", "b", "c"), touched=True,
                    mtype=MigrationType.FULL_LOAD_ONLY):
    from dsql_migrator.ui.data_migration._models import ExclusionInputs

    state = _flo(selected, touched=touched, mtype=mtype)
    state.set_exclusion_inputs_resolver(lambda: ExclusionInputs(
        selected=list(selected), applied={}, preserve_foreign_keys=True,
        migratable=list(migratable), signature=("s",)))
    return state


def test_the_type_switch_waits_for_a_watermark_over_the_current_set() -> None:
    from dsql_migrator.ui.data_migration import migration_type_lock_reason

    jobs = _Jobs({"job-1": _job("DONE", [_c("a"), _c("b")], counts={"a": 1, "b": 2})})

    def lock(state, mgr=jobs):
        return migration_type_lock_reason(state, status=StepStatus.DONE, job_manager=mgr)

    # G16 (spec 6): a removal alone never locks the type -- CDC captures the loaded tables
    # still selected (the unticked b is simply not captured).
    assert lock(_coverage_state(["a"])) is None
    grown = lock(_coverage_state(["a", "b", "c"]))
    assert "(selected since: c)" in grown and "unselected since" not in grown
    assert "re-run Full Load over the current set" in grown
    assert "no gapless start point for it" in grown and "or untick it again" in grown
    # A swap still locks, on the added table only.
    swapped = lock(_coverage_state(["a", "c"]))
    assert "(selected since: c)" in swapped and "unselected since" not in swapped
    assert lock(_coverage_state(["a", "b"])) is None
    assert lock(_coverage_state(["a"], touched=False)) is None
    assert lock(_coverage_state(["a"], migratable=("a",))) is None  # b left the target
    assert lock(_coverage_state(["a"], mtype=MigrationType.FULL_LOAD_AND_CDC)) is None
    assert migration_type_lock_reason(_coverage_state(["a"]), status=StepStatus.DONE) is None


def test_watermark_coverage_gap() -> None:
    from dsql_migrator.ui.data_migration._models import watermark_coverage_gap

    job = _job("DONE", [_c("a")], counts={"a": 1, "b": 2})
    assert watermark_coverage_gap(job, ["a", "c"], ["a", "b", "c"]) == (["c"], ["b"])
    assert watermark_coverage_gap(None, ["a"], ["a"]) is None
    assert watermark_coverage_gap(_job("DONE", [_c("a")], counts={}), ["a"], ["a"]) is None


# ---------------------------------------------------------------------------
# 6. The FK result follows a real change of the set
# ---------------------------------------------------------------------------


def _fk_state(mtype=MigrationType.FULL_LOAD_ONLY, job_id="job-1"):
    state = _flo(["a", "b"], mtype=mtype, job_id=job_id)
    state.set_resolved_selection(["a", "b"])
    state.set_fk_apply_result((2, 0, 0))
    state.set_fk_apply_progress(2, 2)
    return state


def test_a_set_change_clears_a_previous_foreign_key_result() -> None:
    changed = _fk_state()
    changed.set_selection(TableSelection(selected_tables=["a", "c"]))
    assert changed.fk_apply_result is None and changed.fk_apply_progress == (0, 0)
    assert changed.fk_result_scope_changed is True
    same = _fk_state()
    same.set_selection(TableSelection(selected_tables=["b", "a"]))
    assert same.fk_apply_result == (2, 0, 0)
    for state in (_fk_state(job_id=None), _fk_state(MigrationType.FULL_LOAD_AND_CDC)):
        state.set_selection(TableSelection(selected_tables=["a", "c"]))
        assert state.fk_apply_result == (2, 0, 0)


def test_a_pure_removal_after_a_complete_pass_keeps_the_foreign_key_result() -> None:
    # FAILS WITHOUT THE FIX: unticking a table voided the result, and the card said the 2
    # applied keys were "not yet applied ... NOT in place" although nothing changed.
    kept = _fk_state()
    kept.set_selection(TableSelection(selected_tables=["a"]))
    assert kept.fk_apply_result == (2, 0, 0) and kept.fk_result_scope_changed is False
    # A STOPPED pass (1 of its 2 keys settled) is voided even by a removal: its unreached
    # key could leave the scope while the count read "in place".
    stopped = _fk_state()
    stopped.set_fk_apply_result((1, 0, 0))
    stopped.set_fk_apply_progress(1, 2)
    stopped.set_selection(TableSelection(selected_tables=["a"]))
    assert stopped.fk_apply_result is None and stopped.fk_result_scope_changed is True
    # Emptying the selection is "every table" to the key scope: never a shrink.
    emptied = _fk_state()
    emptied.set_selection(TableSelection(selected_tables=[]))
    assert emptied.fk_apply_result is None
    # Any new result write (a new load, a replace, a new pass) ends the scope reading.
    emptied.set_fk_apply_result(None)
    assert emptied.fk_result_scope_changed is False


def test_the_card_says_apply_again_for_a_changed_set_not_never_applied() -> None:
    # FAILS WITHOUT THE FIX: the voided result rendered the never-applied copy.
    from dsql_migrator.ui.data_migration._full_load_ui import _render_foreign_key_action

    from tests.test_ui_data_migration import _ConfirmDialogUi

    def card(**kw):
        ui = _ConfirmDialogUi()
        _render_foreign_key_action(ui, terminal=True, pending=6, applied=None,
                                   apply_foreign_keys=lambda: None, connections_ready=True,
                                   **kw)
        return " ".join(ui.texts), ui.buttons

    changed, buttons = card(scope_changed=True)
    assert "Foreign keys: apply again for the changed table set" in changed
    assert "keys already in place are kept" in changed
    assert "NOT in place" not in changed and "not yet applied" not in changed
    assert "Apply foreign keys" in buttons
    # Never applied (a new load, a replace): today's copy.
    never, _buttons = card()
    assert "6 foreign key(s) not yet applied" in never and "NOT in place" in never


def test_a_rule_save_that_changes_the_selection_clears_it_too() -> None:
    def resolution(after):
        return SimpleNamespace(can_save=True, columns={}, selected_after=after,
                               rule_excluded_after=frozenset())

    state = _fk_state()
    assert state.set_column_exclusions(resolution(None), text="") is None
    assert state.fk_apply_result == (2, 0, 0)
    # A pure removal after the complete pass keeps it; an addition voids it.
    assert state.set_column_exclusions(resolution(("a",)), text="exclude table b") is None
    assert state.fk_apply_result == (2, 0, 0)
    state.set_resolved_selection(["a"])
    assert state.set_column_exclusions(resolution(("a", "c")), text="") is None
    assert state.fk_apply_result is None and state.fk_result_scope_changed is True


# ---------------------------------------------------------------------------
# 7. The Full Load card follows the current set
# ---------------------------------------------------------------------------


class _VerifiedSession:
    target_config = None
    aws_profile = None
    source_verified = True
    target_verified = True

    def connection_ready(self):
        return True


def _render_step(monkeypatch, *, job, selected, guard_reason=None, errors=None,
                 status=StepStatus.DONE, quarantine_records=()):
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_ui_data_migration import _ConfirmDialogUi

    notices: list = []
    progress: list = []
    real_notice = fl.render_notice

    def _notice(ui, *, tone, header, body="", **k):
        notices.append((tone, header, body))
        return real_notice(ui, tone=tone, header=header, body=body, **k)

    def _progress(ui, current, rows, **kw):
        progress.append((current, [r.table for r in rows], kw))

    monkeypatch.setattr(fl, "render_notice", _notice)
    monkeypatch.setattr(fl, "_render_full_load_progress", _progress)
    monkeypatch.setattr(fl, "full_load_error_records_for", lambda _log, _job: [
        SimpleNamespace(table=t, message=m) for t, m in quarantine_records])
    state = _flo(selected)
    jobs = _Jobs({"job-1": job}, errors or {})
    ui = _ConfirmDialogUi()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, jobs, _VerifiedSession(), job=job, status=status,
        selected_names=list(selected), guard_reason=guard_reason, start_full_load=noop,
        retry_failed_load=noop, reload_table=noop, accept_quarantine_and_continue=noop,
        stop_full_load=noop, refresh=noop, retry_tables=noop,
        schema_recreate_candidates=lambda: [],
    )
    return ui, notices, progress


def test_the_card_lists_an_added_table_as_not_loaded_and_drops_an_unselected_one(
        monkeypatch) -> None:
    job = _job("DONE", [_c("a"), _c("b")], counts={"a": 1, "b": 1})
    ui, notices, progress = _render_step(monkeypatch, job=job, selected=["a", "c"])
    (tone, header, body), = [n for n in notices if n[1].startswith("Table set changed")]
    assert tone == "warning"
    assert "1 newly selected table (c) is not loaded yet" in body
    assert "1 table no longer selected (b) is left as is on Aurora DSQL (not dropped)" in body
    (_current, tables, _kw), = progress
    assert tables == ["a", "c"]  # b is not a progress row
    assert "Retry unfinished tables (1)" in ui.buttons
    # The checklist names the added table's reason.
    import asyncio

    asyncio.run(dict(ui.handlers)["Retry unfinished tables (1)"]())
    assert "Not loaded yet — selected after the last Full Load." in ui.texts


def test_a_removal_alone_is_info_and_the_unselected_failure_no_longer_shows(
        monkeypatch) -> None:
    job = _job("FAILED", [_c("a"), _c("b", "FAILED")], counts={"a": 1, "b": 1})
    ui, notices, _progress = _render_step(
        monkeypatch, job=job, selected=["a"], errors={"job-1": "load failed for b"},
        status=StepStatus.FAILED)
    (tone, _header, body), = [n for n in notices if n[1].startswith("Table set changed")]
    assert tone == "info"
    assert "b did not fully load in the last run" in body
    assert not any(n[1] == "Load failed" for n in notices)
    assert not any(b.startswith("Retry unfinished") for b in ui.buttons)
    assert "Re-run Full Load" in ui.buttons


def test_quarantine_records_of_an_unselected_table_are_not_offered(monkeypatch) -> None:
    job = _job("DONE", [_c("a", rows_quarantined=1), _c("b", rows_quarantined=1)])
    _ui, _notices, progress = _render_step(
        monkeypatch, job=job, selected=["a"],
        quarantine_records=[("a", "quarantined row pk[1]"), ("b", "quarantined row pk[2]")])
    (_current, tables, kw), = progress
    assert tables == ["a"]
    assert kw["quarantine_records"] == [("a", "quarantined row pk[1]")]


def test_the_retry_tooltip_names_the_added_tables_or_the_guard(monkeypatch) -> None:
    from tests.test_ui_data_migration import _ConfirmDialogUi

    tips: list = []
    monkeypatch.setattr(_ConfirmDialogUi._El, "tooltip",
                        lambda self, text="", *a, **k: tips.append(str(text)) or self,
                        raising=False)
    job = _job("DONE", [_c("a")], counts={"a": 1})
    _render_step(monkeypatch, job=job, selected=["a", "c"])
    assert any("selected after the last run" in t for t in tips), tips
    tips.clear()
    _render_step(monkeypatch, job=job, selected=["a", "c"], guard_reason="Run checks.")
    assert "Run checks." in tips


def test_an_unchanged_set_renders_no_scope_notice(monkeypatch) -> None:
    job = _job("DONE", [_c("a"), _c("b")], counts={"a": 1, "b": 1})
    _ui, notices, progress = _render_step(monkeypatch, job=job, selected=["a", "b"])
    assert not any(n[1].startswith("Table set changed") for n in notices)
    assert progress[0][0] is job  # the record itself


# ---------------------------------------------------------------------------
# 8. The screen: the step follows the set; the tree, the tiles and the notice agree
# ---------------------------------------------------------------------------


def _screen(monkeypatch, *, job, selected, pg=False):
    import sys
    import types

    from dsql_migrator.core.models import (
        AssessmentReport,
        ColumnDef,
        SourceConnectionConfig,
        SourceInventory,
        SourceType,
        TableDef,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

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
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(
        source_type=SourceType.POSTGRES if pg else SourceType.MYSQL, host="h",
        port=5432 if pg else 3306, database="app", username="u"), None)
    session.set_target(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"))
    session.set_migration_type(MigrationType.FULL_LOAD_ONLY)
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, StepStatus.DONE))
    tables = [TableDef(name=f"app.{n}", columns=[ColumnDef(name="id", mysql_type="int",
                                                            nullable=False)],
                       primary_key=["id"]) for n in ("a", "b", "c")]
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=SourceInventory(tables=tables), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = ["table:app.a", "table:app.b", "table:app.c"]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.job_id = "job-1"
    state.set_selection(TableSelection(selected_tables=list(selected)))
    jobs = _Jobs({"job-1": job})
    content, _runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs,
    )
    return SimpleNamespace(content=content, ui=ui, session=session, state=state, jobs=jobs)


def _loaded():
    return _job("DONE", [_c("app.a"), _c("app.b")], counts={"app.a": 1, "app.b": 1})


def _retry_route(monkeypatch, *, job, selected, report_tables=None):
    """Render the screen and hand back the scoped retry the popup / buttons call, with
    the retry engine recorded instead of run."""
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _exclusion_dialog as xd

    from tests.test_ui_data_migration import _fl_report

    captured: dict = {}
    real_row = xd.render_exclusion_button_row

    def _row(ui, ctx):
        captured["ctx"] = ctx
        return real_row(ui, ctx)

    calls: list = []
    monkeypatch.setattr(xd, "render_exclusion_button_row", _row)
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, prior, tables, **kw: calls.append(
                            (list(prior), [t.name for t in tables], kw)))
    s = _screen(monkeypatch, job=job, selected=selected)
    if report_tables is not None:
        s.state.set_prereq_report(MigrationMode.FULL_LOAD, _fl_report(*report_tables))
    s.content(lambda: None)
    s.jobs.submit = lambda fn: fn(SimpleNamespace()) or "job-2"
    return s, captured["ctx"].extra["run_retry_for"], calls


def test_a_grown_set_turns_the_step_failed_once_and_restoring_it_turns_it_back(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import migration_type_lock_reason

    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b", "app.c"])
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.FAILED
    assert s.state.error is None  # nothing failed: a table is just not loaded yet
    before = s.session.workflow
    s.content(lambda: None)
    assert s.session.workflow is before  # a steady render writes nothing
    # The tiles: CDC would stream the original set.
    reason = migration_type_lock_reason(s.state, status=StepStatus.FAILED, job_manager=s.jobs)
    assert reason is not None and "selected since: app.c" in reason
    s.state.set_selection(TableSelection(selected_tables=["app.a", "app.b"]))
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    assert migration_type_lock_reason(
        s.state, status=StepStatus.DONE, job_manager=s.jobs) is None


def test_the_tree_is_editable_with_the_hint_and_no_refresh_button(monkeypatch) -> None:
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    s.content(lambda: None)
    texts = " ".join(s.ui.texts)
    assert "A Full Load has run. A table you untick is left as is on Aurora DSQL" in texts
    assert "Select all" in texts and "Unselect all" in texts
    assert "Refresh source/target objects" not in s.ui.tooltips


def _grown_and_loaded():
    """app.c ticked after the load and loaded by Retry, under the ORIGINAL watermark (G12
    never extends it): the step is DONE, but the watermark does not cover app.c."""
    return _job("DONE", [_c("app.a"), _c("app.b"), _c("app.c")],
                counts={"app.a": 1, "app.b": 1})


@pytest.mark.parametrize("pg", [False, True])
def test_the_completion_notice_stops_recommending_cdc_from_this_watermark(
        monkeypatch, pg) -> None:
    s = _screen(monkeypatch, job=_grown_and_loaded(), selected=["app.a", "app.b", "app.c"],
                pg=pg)
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    texts = " ".join(s.ui.texts)
    assert "Tables were selected after this Full Load" in texts
    assert "re-run Full Load over the current set first" in texts
    assert "streams from this Full Load's watermark" not in texts
    assert ("creates no replication slot" in texts) is pg


@pytest.mark.parametrize("pg", [False, True])
def test_a_removal_keeps_todays_completion_notice(monkeypatch, pg) -> None:
    # G16 (spec 6): unticking a loaded table never locks the type, so the notice keeps
    # recommending CDC from this watermark (it captures the loaded tables still selected).
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a"], pg=pg)
    s.content(lambda: None)
    texts = " ".join(s.ui.texts)
    assert "re-run Full Load over the current set first" not in texts
    assert "Tables were selected after this Full Load" not in texts
    if pg:
        assert "This run created no replication slot" in texts
    else:
        assert "streams from this Full Load's watermark" in texts


def test_an_unchanged_set_keeps_todays_completion_notice(monkeypatch) -> None:
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    s.content(lambda: None)
    texts = " ".join(s.ui.texts)
    assert "streams from this Full Load's watermark" in texts
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE


def test_a_tick_rechecks_the_lock_at_the_click() -> None:
    from dsql_migrator.ui.data_migration import _render_table_selection

    from tests.test_ui_data_migration import _PickerUi, _inventory

    class _Ui(_PickerUi):
        def __init__(self):
            super().__init__()
            self.trees: list = []
            self.notes: list = []

        def tree(self, *_a, on_tick=None, **_k):
            self.trees.append(on_tick)
            return self._El(self)

        def notify(self, text="", *_a, **_k):
            self.notes.append(str(text))

    ui = _Ui()
    state = DataMigrationState()
    refreshed = []
    _render_table_selection(
        ui, _inventory(), state, ["orders", "customers"], target_existing=["orders"],
        lock_check=lambda: "Locked while a Full Load runs.", on_locked=lambda: refreshed.append(1),
        hint="A Full Load has run.", filter_state={"text": "ord"},
    )
    assert "A Full Load has run." in ui.texts
    assert not any("Refresh" in t for t in ui.tooltips)  # no on_refresh given
    (on_tick,) = ui.trees
    on_tick(SimpleNamespace(value=["table:orders"]))
    assert state.selection_touched is False  # nothing applied
    assert ui.notes == ["Locked while a Full Load runs. This change was not applied."]
    assert refreshed == [1]


# ---------------------------------------------------------------------------
# 9. Validation and Cut over notice a report that does not cover the set
# ---------------------------------------------------------------------------


def _report(*tables):
    from dsql_migrator.core.models import TableValidationResult, ValidationMode, ValidationReport

    return ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[TableValidationResult(
        table=t, source_row_count=1, target_row_count=1, row_count_match=True, matched=True)
        for t in tables])


def _inv(*names):
    from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef

    return SourceInventory(tables=[TableDef(name=n, columns=[ColumnDef(
        name="id", mysql_type="int")], primary_key=["id"]) for n in names])


def test_validation_scope_names_is_the_runners_formula() -> None:
    from dsql_migrator.ui.validation import validation_scope_names

    inv = _inv("a", "b", "c")
    sel = TableSelection(selected_tables=["a", "b"])
    assert validation_scope_names(inv, sel) == ["a", "b"]
    assert validation_scope_names(inv, sel, {"b"}) == ["a"]
    assert validation_scope_names(inv, sel, {"a", "b"}) == ["a", "b"]  # all off -> all
    assert validation_scope_names(inv, TableSelection(selected_tables=[])) == ["a", "b", "c"]


def test_validation_scope_gap() -> None:
    from dsql_migrator.ui.validation import validation_scope_gap

    inv = _inv("a", "b", "c")
    state = _flo(["a", "c"])
    assert validation_scope_gap(_report("a", "b"), inv, state) == (["c"], ["b"])
    assert validation_scope_gap(_report("a", "b"), inv, state, {"c"}) == ([], ["b"])
    assert validation_scope_gap(_report("a", "c"), inv, state) == ([], [])
    for other in (_flo(["a", "c"], mtype=MigrationType.FULL_LOAD_AND_CDC),
                  _flo(["a", "c"], job_id=None), _flo([])):
        assert validation_scope_gap(_report("a", "b"), inv, other) == ([], [])
    assert validation_scope_gap(None, inv, state) == ([], [])


def test_a_matching_report_that_misses_a_selected_table_is_not_ready() -> None:
    from dsql_migrator.ui.validation import _render_verdict, summarize_validation

    from tests.test_ui_validation import _CopyUi, _drift_na

    summary = summarize_validation(_report("a"))
    held = _CopyUi()
    _render_verdict(held, summary, _drift_na(), scope_missing=["c"])
    body = held.body()
    assert "Ready for cut-over" not in body
    # Cause-neutral: the table may have been selected since, or left out by a chip.
    assert "Tables match — cut over waits until the result covers every migrated table" in body
    assert "newly selected" not in body
    assert "c is part of this migration's table set but has no row in this result" in body
    plain = _CopyUi()
    _render_verdict(plain, summary, _drift_na())
    assert "Ready for cut-over" in plain.body()


def _cutover(monkeypatch, *, selected, report_tables, mtype=MigrationType.FULL_LOAD_ONLY,
             full_load=StepStatus.DONE, jobs=None, table_exclude=()):
    import sys
    import types

    from dsql_migrator.core.models import AssessmentReport, TargetInventory
    from dsql_migrator.ui import validation as uiv
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.session import SessionStore
    from dsql_migrator.ui.validation import ValidationStore, build_cutover_screen

    from tests.test_ui_validation import _ScreenUi

    class _Ui(_ScreenUi):
        def __init__(self):
            super().__init__()
            self.notes: list = []

        def notify(self, message="", **_k):
            self.notes.append(str(message))

    ui = _Ui()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    monkeypatch.setattr(uiv, "_cdc_in_use", lambda _s: False)
    monkeypatch.setattr(uiv, "_cutover_pending_foreign_keys", lambda *a, **k: 0)
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, full_load))
    session.set_workflow(with_status(session.workflow, WorkflowStep.VALIDATION, StepStatus.DONE))
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=_inv("a", "b", "c"), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    vstore, mstore = ValidationStore(), DataMigrationStore()
    state = mstore.get_or_create("s1")
    state.migration_type = mtype
    state.job_id = "job-1"
    state.selection = TableSelection(selected_tables=list(selected))
    state.selection_touched = True
    content, runner = build_cutover_screen(
        sessions, "s1", validation_store=vstore, migration_store=mstore, eval_store=evals,
        job_manager=jobs)
    vstate = vstore.get_or_create("s1")
    vstate.set_result(_report(*report_tables))
    vstate.table_exclude = set(table_exclude)
    return SimpleNamespace(content=content, runner=runner, ui=ui, sessions=sessions)


def test_cut_over_holds_until_the_report_covers_the_current_set(monkeypatch) -> None:
    held = _cutover(monkeypatch, selected=["a", "c"], report_tables=["a"])
    held.content(lambda: None)
    texts = " ".join(held.ui.texts)
    assert "Validation does not cover the current table set" in texts
    assert "I've cut over to DSQL" not in texts
    held.runner()
    assert held.ui.notes == [
        "Validation does not cover the current table set — re-run Validation first."]
    session = held.sessions.get_or_create("s1")
    assert get_status(session.workflow, WorkflowStep.CUT_OVER) is not StepStatus.DONE
    # An equal set releases exactly as today.
    released = _cutover(monkeypatch, selected=["a", "c"], report_tables=["a", "c"])
    released.content(lambda: None)
    assert "I've cut over to DSQL" in " ".join(released.ui.texts)
    # A CDC type is never held by it.
    cdc = _cutover(monkeypatch, selected=["a", "c"], report_tables=["a"],
                   mtype=MigrationType.FULL_LOAD_AND_CDC)
    cdc.content(lambda: None)
    assert "Validation does not cover the current table set" not in " ".join(cdc.ui.texts)


def test_the_cut_over_hold_leads_with_data_migration_while_it_is_not_complete(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the hold sent the user to Validation, which the nav refuses
    # while Data Migration is Failed (a table ticked since the load is not loaded yet).
    held = _cutover(monkeypatch, selected=["a", "c"], report_tables=["a"],
                    full_load=StepStatus.FAILED)
    held.content(lambda: None)
    texts = " ".join(held.ui.texts)
    assert "Validation does not cover the current table set" in texts
    assert "Retry unfinished tables loads what is left" in texts
    assert "Data Migration is not complete for the current table set" in texts
    assert "Re-run Validation (or turn its chip off" not in texts
    assert "I've cut over to DSQL" not in texts
    held.runner()
    assert held.ui.notes == [
        "Validation does not cover the current table set — finish Data Migration (load the "
        "newly selected tables), then re-run Validation."]


def test_a_restored_chip_narrowed_report_over_the_loaded_set_releases_cut_over(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: Validation ran with c's chip off over an UNCHANGED set; chips
    # are not kept across a restart, so c read as missing and Cut over was held.
    loaded = _Jobs({"job-1": _job("DONE", [_c("a"), _c("b"), _c("c")],
                                  counts={"a": 1, "b": 1, "c": 1})})
    restored = _cutover(monkeypatch, selected=["a", "b", "c"], report_tables=["a", "b"],
                        jobs=loaded)
    restored.content(lambda: None)
    texts = " ".join(restored.ui.texts)
    assert "Validation does not cover the current table set" not in texts
    assert "I've cut over to DSQL" in texts
    # A table selected SINCE the load still holds it.
    grown = _Jobs({"job-1": _job("DONE", [_c("a")], counts={"a": 1})})
    held = _cutover(monkeypatch, selected=["a", "c"], report_tables=["a"], jobs=grown)
    held.content(lambda: None)
    assert "Validation does not cover the current table set" in " ".join(held.ui.texts)


def test_validation_scope_gap_holds_only_tables_selected_since_the_load() -> None:
    from dsql_migrator.ui.validation import full_load_covered_tables, validation_scope_gap

    inv = _inv("a", "b", "c")
    state = _flo(["a", "b", "c"])
    assert validation_scope_gap(_report("a", "b"), inv, state, covered={"a", "b", "c"}) == (
        [], [])
    assert validation_scope_gap(_report("a"), inv, _flo(["a", "c"]), covered={"a"}) == (
        ["c"], [])
    # No record / no counts: every missing table holds (the safe side).
    assert validation_scope_gap(_report("a", "b"), inv, state, covered=None) == (["c"], [])
    jobs = _Jobs({"job-1": _job("DONE", [_c("a")], counts={"a": 1, "b": 2})})
    assert full_load_covered_tables(state, jobs) == frozenset({"a", "b"})
    assert full_load_covered_tables(state, _Jobs({})) is None
    assert full_load_covered_tables(state, None) is None
    # G17 (deliberately changed): a CDC type reads the run's ORIGINAL tables too (a table a
    # guarded Full load + CDC load added since is not "covered" for Validation).
    assert full_load_covered_tables(
        _flo(mtype=MigrationType.FULL_LOAD_AND_CDC), jobs) == frozenset({"a", "b"})


# ---------------------------------------------------------------------------
# 10. Cut-over DEFAULT restore: a table no longer selected is a manual decision
# ---------------------------------------------------------------------------


def test_a_ledger_table_outside_the_selection_is_never_restored_automatically() -> None:
    from dsql_migrator.ui.cutover_defaults import NOT_SELECTED_REASON, classify_cutover_defaults

    from tests.test_cutover_defaults import D1, _col, _entry, _facts, _record

    entries = [_entry("a", "'x'::character varying")]

    def classify(selected):
        items, _n = classify_cutover_defaults(
            {"app.t": _record(*entries)}, {"app.t": _facts({"a": _col()})},
            current_ddls={"app.t": D1}, still_excluded={"app.t": ["a"]},
            selected_tables=selected)
        (item,) = items
        return item

    assert classify(None).auto is True and classify(None).reason is None  # today
    assert classify(["app.t"]).auto is True
    outside = classify(["app.other"])
    assert outside.auto is False and outside.status == "pending"
    assert outside.reason == NOT_SELECTED_REASON.format(o="'x'::character varying")


_CD_DEFAULT = "'x'::character varying"


def _cd_ledger():
    from dsql_migrator.core.overlay_ledger import InMemoryOverlayLedger

    from tests.test_cutover_defaults import KEY, _entry, _record

    ledger = InMemoryOverlayLedger()
    ledger.put_tables(KEY, {"app.t": _record(_entry("a", _CD_DEFAULT))})
    return ledger


def _cd_facts(names, columns):
    from tests.test_cutover_defaults import _col, _facts

    return {n: _facts({c: _col() for c in columns[n]}) for n in names}


def test_the_read_forwards_the_selection_and_an_empty_one_is_every_table() -> None:
    from dsql_migrator.ui.cutover_defaults import NOT_SELECTED_REASON, read_cutover_defaults

    from tests.test_cutover_defaults import D1, KEY

    def fill(selected):
        (item,) = read_cutover_defaults(
            ledger=_cd_ledger(), ledger_key=KEY, read_facts=_cd_facts,
            current_ddls=lambda: {"app.t": D1}, still_excluded=lambda: {"app.t": ["a"]},
            selected_tables=selected, fill_key="k", generation=1).items
        return item

    outside = fill(lambda: ["app.other"])
    assert outside.auto is False and outside.reason == NOT_SELECTED_REASON.format(o=_CD_DEFAULT)
    assert fill(lambda: ["app.t"]).auto is True
    empty = fill(lambda: [])  # no selection recorded: every table, as before
    assert empty.auto is True and empty.reason is None


def test_the_cut_over_fill_forwards_the_migration_selection(monkeypatch) -> None:
    # The app's wiring: without it a dropped DEFAULT on a table no longer selected would be
    # restored automatically -- a DDL write to a table the migration left as is.
    import dsql_migrator.ui.data_migration._models as models
    import dsql_migrator.ui.validation as uiv
    from dsql_migrator.ui.cutover_defaults import NOT_SELECTED_REASON

    from tests.test_cutover_defaults import D1, TARGET

    monkeypatch.setattr(models, "effective_target_ddls", lambda *a, **k: {"app.t": D1})
    monkeypatch.setattr(uiv, "session_exclusion_split",
                        lambda *a, **k: ({}, {"app.t": frozenset({"a"})}, {}))
    mstate = SimpleNamespace(selection=TableSelection(selected_tables=["app.other"]))

    def fill(generation):
        (item,) = uiv._fill_cutover_defaults(
            SimpleNamespace(target_config=TARGET), mstate, None, None, _cd_ledger(), None,
            fill_key="k", generation=generation, facts_reader=_cd_facts).items
        return item

    outside = fill(1)
    assert outside.auto is False and outside.reason == NOT_SELECTED_REASON.format(o=_CD_DEFAULT)
    mstate.selection = TableSelection(selected_tables=["app.t"])
    assert fill(2).auto is True


def test_the_cut_over_fill_key_follows_the_selection() -> None:
    from dsql_migrator.ui.validation import ValidationState, _cutover_fill_key

    from tests.test_cutover_defaults import TARGET

    session = SimpleNamespace(target_config=TARGET)
    state = _flo(["a"])
    first = _cutover_fill_key(session, state, ValidationState(), None, None)
    state.selection = TableSelection(selected_tables=["a", "b"])
    assert _cutover_fill_key(session, state, ValidationState(), None, None) != first


# ---------------------------------------------------------------------------
# 11. Change & reload in Full load only: a table line saves the selection
# ---------------------------------------------------------------------------


def _flo_reload_ctx():
    from tests.test_exclusion_dialog import _ctx

    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_ONLY
    state.job_id = "job-1"
    state.restore_lob_exclusion("shop.t", "a")
    state.restore_exclusion_rules("exclude shop.t.a", ())
    jm = _StubJobManager({"job-1": _StubJob("DONE")})
    ctx, calls = _ctx(state, job_manager=jm)  # lock_reason_fn -> None (lifted)
    ctx.session.connection_ready = lambda: True
    submitted: list = []
    ctx.extra["run_retry_for"] = lambda tables: submitted.append(list(tables))
    return ctx, state, submitted


def test_change_and_reload_saves_a_table_line_without_e3_in_full_load_only() -> None:
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog

    from tests.test_exclusion_dialog import _Ui

    ctx, state, submitted = _flo_reload_ctx()
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    assert any(t.startswith("Column and table rules are editable.") for t in ui.texts)
    ui.type_text("exclude shop.t.a\nexclude table shop.u")
    assert not any("E3" in t or "Start over" in t for t in ui.texts)
    save = ui.buttons["Save"][-1][1]
    assert save.enabled and "color=primary" in " ".join(save.props_seen)
    assert any("stores the rules and the table selection" in t
               and "Unselects shop.u: left as is on Aurora DSQL (not dropped)" in t
               for t in ui.texts)
    ui.click("Save")
    assert submitted == []
    assert state.selection.selected_tables == ["shop.t", "shop.audit_1"]
    assert state.selection_touched is True


def test_change_and_reload_keeps_e3_and_todays_label_while_the_set_is_locked() -> None:
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog

    from tests.test_exclusion_dialog import _Ui

    ctx, _state, _submitted = _flo_reload_ctx()
    ctx.lock_reason_fn = lambda: TODAY
    ui = _Ui()
    open_exclusion_dialog(ui, ctx)
    ui.click("Change & reload")
    assert ("Column rules are editable. Tables are fixed after a Full Load (use Start over "
            "to change them).") in ui.texts
    ui.type_text("exclude shop.t.a\nexclude table shop.u")
    assert any("Start over" in t for t in ui.texts)


# ---------------------------------------------------------------------------
# 12. The scoped retry route (_run_retry_for): what is submitted
# ---------------------------------------------------------------------------


def _notes(s):
    return [m for m, _k in s.ui.notified]


def test_a_retry_never_reloads_an_unselected_table(monkeypatch) -> None:
    job = _job("FAILED", [_c("app.a", "FAILED"), _c("app.b", "FAILED")],
               counts={"app.a": 1, "app.b": 1})
    s, retry, calls = _retry_route(monkeypatch, job=job, selected=["app.a"])
    retry(["app.a", "app.b"])
    (prior, tables, kw), = calls
    assert tables == ["app.a"] and kw["run_table_names"] == ["app.a"]
    assert [c.chunk_id for c in prior] == ["app.a", "app.b"]  # the record keeps history
    assert any(n.startswith("app.b is no longer selected") for n in _notes(s))
    calls.clear()
    retry(["app.b"])
    assert calls == []  # nothing left to submit


def test_a_newly_selected_table_needs_its_checks_before_a_retry_loads_it(monkeypatch) -> None:
    s, retry, calls = _retry_route(monkeypatch, job=_loaded(),
                                   selected=["app.a", "app.b", "app.c"])
    retry(["app.c"])
    assert calls == []
    assert any("app.c was added to the selection after the last Full Load" in n
               for n in _notes(s))


def test_a_checked_new_table_is_loaded_by_the_retry_under_the_old_record(monkeypatch) -> None:
    job = _loaded()
    _s, retry, calls = _retry_route(monkeypatch, job=job, selected=["app.a", "app.b", "app.c"],
                                    report_tables=["app.a", "app.b", "app.c"])
    retry(["app.c"])
    (prior, tables, kw), = calls
    assert tables == ["app.c"] and prior == job.chunks
    assert kw["run_table_names"] == ["app.a", "app.b", "app.c"]
    assert kw["watermark"] is job.watermark


def test_an_unchanged_set_retries_exactly_as_before(monkeypatch) -> None:
    job = _job("FAILED", [_c("app.a"), _c("app.b", "FAILED")], counts={"app.a": 1, "app.b": 1})
    _s, retry, calls = _retry_route(monkeypatch, job=job, selected=["app.a", "app.b"])
    retry(["app.b"])
    (_prior, tables, kw), = calls
    assert tables == ["app.b"] and kw["run_table_names"] is None


def test_an_emptied_selection_reloads_nothing(monkeypatch) -> None:
    s, retry, calls = _retry_route(monkeypatch, job=_loaded(), selected=["app.a"])
    s.state.set_selection(TableSelection(selected_tables=[]))
    retry(["app.a"])
    assert calls == [] and any("No table is selected" in n for n in _notes(s))


def test_a_tree_tick_after_a_load_syncs_the_step_and_refreshes_the_page_once(
        monkeypatch) -> None:
    from dsql_migrator.ui import data_migration as dm

    captured: dict = {}
    real = dm._render_table_selection

    def _spy(*a, **k):
        captured.update(k)
        return real(*a, **k)

    monkeypatch.setattr(dm, "_render_table_selection", _spy)
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    timers: list = []
    s.ui.timer = lambda interval, callback=None, *a, **k: timers.append(interval) or s.ui._El(s.ui)
    s.content(lambda: None)
    assert captured["lock_check"]() is None and captured["on_refresh"] is None
    timers.clear()
    s.state.set_selection(TableSelection(selected_tables=["app.a", "app.b", "app.c"]))
    captured["on_selection_change"](["app.a", "app.b", "app.c"])
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.FAILED
    assert sorted(timers) == [0.3, 0.8]  # the row refresh + ONE coalesced page refresh
    # Full load + CDC before CDC starts (G16, probed: no stack): the same step sync and
    # ONE coalesced page refresh (the CDC step's "Will stream" follows the tick).
    cdc = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    cdc.session.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    cdc.state.set_cdc_stack_phase("absent", status=None)
    cdc_timers: list = []
    cdc.ui.timer = lambda interval, callback=None, *a, **k: (
        cdc_timers.append(interval) or cdc.ui._El(cdc.ui))
    cdc.content(lambda: None)
    cdc_timers.clear()
    cdc.state.set_selection(TableSelection(selected_tables=["app.a"]))
    captured["on_selection_change"](["app.a"])
    assert sorted(cdc_timers) == [0.3, 0.8]
    # ...and once CDC was ever started (the ops window survives a Stop): today's handler,
    # the row refresh only, no step write.
    started = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    started.session.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    started.state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
    started.state.set_cdc_ops_window_start(datetime(2026, 10, 1, tzinfo=timezone.utc))
    started_timers: list = []
    started.ui.timer = lambda interval, callback=None, *a, **k: (
        started_timers.append(interval) or started.ui._El(started.ui))
    started.content(lambda: None)
    started_timers.clear()
    captured["on_selection_change"](["app.a"])
    assert started_timers == [0.3]


def test_the_live_poll_maps_a_done_retry_with_an_unloaded_added_table_to_failed() -> None:
    # A retry of the loaded tables finished DONE while a table selected since is still not
    # loaded: the step is not DONE (Validation stays locked) and no job error is recorded.
    from dsql_migrator.ui import data_migration as dm

    from tests.test_ui_data_migration import _ConfirmDialogUi

    class _Ui(_ConfirmDialogUi):
        def __init__(self):
            super().__init__()
            self.timers: list = []

        def timer(self, _interval, callback=None, *_a, **_k):
            self.timers.append(callback)
            return self._El(self)

    jobs = _Jobs({"job-1": _job("RUNNING", [_c("a", "PENDING")], counts={"a": 1})})
    state = _flo(["a", "c"])
    errors: list = []
    state.set_error = errors.append
    session = SimpleNamespace(
        workflow=with_status(WorkflowState(), WorkflowStep.FULL_LOAD, StepStatus.IN_PROGRESS),
        source_verified=True, target_verified=True, target_config=None, aws_profile=None,
        connection_ready=lambda: True,
    )
    session.set_workflow = lambda w: setattr(session, "workflow", w)
    ui = _Ui()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, jobs, session, job=jobs.jobs["job-1"], status=StepStatus.IN_PROGRESS,
        selected_names=["a", "c"], guard_reason=None, start_full_load=noop,
        retry_failed_load=noop, reload_table=noop, accept_quarantine_and_continue=noop,
        stop_full_load=noop, refresh=noop, retry_tables=noop,
        schema_recreate_candidates=lambda: [],
    )
    (poll,) = [t for t in ui.timers if getattr(t, "__name__", "") == "_poll_live"]
    jobs.jobs["job-1"] = _job("DONE", [_c("a")], counts={"a": 1})
    poll()
    assert get_status(session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.FAILED
    assert errors == []
    # An unchanged set keeps the poll's own mapping: DONE.
    session.workflow = with_status(WorkflowState(), WorkflowStep.FULL_LOAD, StepStatus.IN_PROGRESS)
    state.selection = TableSelection(selected_tables=["a"])
    jobs.jobs["job-1"] = _job("RUNNING", [_c("a", "PENDING")], counts={"a": 1})
    ui2 = _Ui()
    dm._render_full_load_step(
        ui2, state, jobs, session, job=jobs.jobs["job-1"], status=StepStatus.IN_PROGRESS,
        selected_names=["a"], guard_reason=None, start_full_load=noop,
        retry_failed_load=noop, reload_table=noop, accept_quarantine_and_continue=noop,
        stop_full_load=noop, refresh=noop, retry_tables=noop,
        schema_recreate_candidates=lambda: [],
    )
    (poll2,) = [t for t in ui2.timers if getattr(t, "__name__", "") == "_poll_live"]
    jobs.jobs["job-1"] = _job("DONE", [_c("a")], counts={"a": 1})
    poll2()
    assert get_status(session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE


# ---------------------------------------------------------------------------
# 13. Change & reload over a real screen: what a column change reloads, and the row
# ---------------------------------------------------------------------------


def _reload_screen(monkeypatch, *, record=("app.a", "app.b"), rules="exclude table app.c",
                   rule_excluded=("app.c",), selection=("app.a", "app.b"), stored=(),
                   mtype=MigrationType.FULL_LOAD_ONLY, unlisted=()):
    """A Data Migration screen after a Full Load over ``record`` (tables with a nullable
    ``note`` column), the popup's context captured and the retry engine recorded.
    ``unlisted`` names extra source tables with no generated DDL (not migratable)."""
    import sys
    import types

    from dsql_migrator.core.models import (
        AssessmentReport,
        ColumnDef,
        SourceConnectionConfig,
        SourceInventory,
        SourceType,
        TableDef,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.data_migration import _exclusion_dialog as xd
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

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
    captured: dict = {}
    real_row = xd.render_exclusion_button_row

    def _row(u, ctx):
        captured["ctx"] = ctx
        return real_row(u, ctx)

    monkeypatch.setattr(xd, "render_exclusion_button_row", _row)
    tree: dict = {}
    real_tree = dm._render_table_selection

    def _tree(*a, **k):
        tree.update(k)
        return real_tree(*a, **k)

    monkeypatch.setattr(dm, "_render_table_selection", _tree)
    calls: list = []
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, prior, tables, **kw: calls.append([t.name for t in tables]))
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(source_type=SourceType.MYSQL, host="h", port=3306,
                                              database="app", username="u"), None)
    session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                              region="us-east-1"))
    session.set_migration_type(mtype)
    session.set_workflow(with_status(session.workflow, WorkflowStep.FULL_LOAD, StepStatus.DONE))
    cols = [ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="note", mysql_type="text", nullable=True)]
    tables = [TableDef(name=f"app.{n}", columns=cols, primary_key=["id"]) for n in "abc"]
    tables += [TableDef(name=n, columns=cols, primary_key=["id"]) for n in unlisted]
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=SourceInventory(tables=tables), assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]), target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = ["table:app.a", "table:app.b", "table:app.c"]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.job_id = "job-1"
    state.restore_exclusion_rules(rules, list(rule_excluded))
    for table, column in stored:
        state.restore_lob_exclusion(table, column)
    state.set_selection(TableSelection(selected_tables=list(selection)))
    jobs = _Jobs({"job-1": _job("DONE", [_c(n) for n in record],
                                counts={n: 1 for n in record})})
    content, _runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs)
    session.set_source_verified(True)
    session.set_target_verified(True)
    ui.timer = lambda interval, callback=None, *a, **k: ui._El(ui)
    content(lambda: None)
    jobs.submit = lambda fn: fn(SimpleNamespace()) or "job-2"  # run the retry inline
    return SimpleNamespace(ui=ui, state=state, ctx=captured["ctx"], calls=calls, jobs=jobs,
                           content=content, session=session, tree=tree)


def test_change_and_reload_reloads_the_loaded_tables_and_leaves_a_new_one_to_retry(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: app.c (never loaded) was in the Drop & reload set, so the
    # retry's prerequisite guard refused the WHOLE reload after the rules were stored --
    # app.a / app.b kept their note values although note was recorded as excluded.
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog

    from tests.test_exclusion_dialog import _Ui

    s = _reload_screen(monkeypatch)
    ui = _Ui()
    open_exclusion_dialog(ui, s.ctx)
    ui.click("Change & reload")
    assert any("a table you select that is not loaded yet is loaded by Retry unfinished "
               "tables" in t for t in ui.texts)
    ui.type_text("exclude app.*.note")  # re-selects app.c, excludes note everywhere
    assert "Drop & reload 2 tables" in ui.buttons
    assert any("Selects app.c: listed under Retry unfinished tables" in t for t in ui.texts)
    ui.click("Drop & reload 2 tables")
    assert s.calls == [["app.a", "app.b"]]
    assert not any("Untick" in n for n, _k in s.ui.notified)
    assert s.state.selection.selected_tables == ["app.a", "app.b", "app.c"]
    assert {t: set(c) for t, c in s.state.lob_exclusions().items()} == {
        "app.a": {"note"}, "app.b": {"note"}, "app.c": {"note"}}


def test_a_new_tables_column_rule_alone_drops_nothing(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog

    from tests.test_exclusion_dialog import _Ui

    s = _reload_screen(monkeypatch)
    ui = _Ui()
    open_exclusion_dialog(ui, s.ctx)
    ui.click("Change & reload")
    ui.type_text("exclude app.c.note")  # re-selects app.c with a column rule on it only
    assert not any(k.startswith("Drop & reload") for k in ui.buttons)
    assert any(t.startswith("Nothing to drop and reload: Save stores the rules and the "
                            "table selection. Selects app.c: listed under Retry") for t in ui.texts)
    ui.click("Save")
    assert s.calls == []
    assert s.state.selection.selected_tables == ["app.a", "app.b", "app.c"]
    assert {t: set(c) for t, c in s.state.lob_exclusions().items()} == {"app.c": {"note"}}


def test_full_load_and_cdc_change_and_reload_selects_an_uncovered_table(monkeypatch) -> None:
    # G16 let Full load + CDC before CDC starts DROP tables but kept re-selecting app.c (not in
    # the Full Load's snapshot) an E3. G17 (deliberately changed): the Save selects it -- it
    # is listed under Retry unfinished tables and CDC captures it once loaded -- and nothing
    # is dropped or reloaded for it here.
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog

    from tests.test_exclusion_dialog import _Ui

    s = _reload_screen(monkeypatch, mtype=MigrationType.FULL_LOAD_AND_CDC)
    s.state.set_cdc_stack_phase("absent", status=None)
    ui = _Ui()
    open_exclusion_dialog(ui, s.ctx)
    ui.click("Change & reload")
    ui.type_text("exclude app.*.note")
    assert not any("Save would re-select app.c" in t for t in ui.texts)
    # G17 review (deliberately changed): this watermark records no MySQL start point for
    # app.c, so Retry cannot add it -- the line names Re-run all tables too.
    assert any("Selects app.c: listed under Retry unfinished tables on the Full Load step "
               "until loaded (Re-run all tables loads it where that step says Retry cannot "
               "add it); CDC then captures it with the other tables." in t for t in ui.texts)
    action = [b for b in ui.elements if b.kind == "button"
              and (b.text == "Save" or str(b.text).startswith("Drop & reload"))][-1]
    assert action.enabled is True and action.text == "Drop & reload 2 tables"
    ui.click("Save")
    # Only the loaded tables are dropped and reloaded (their excluded column changed); app.c
    # is selected and left to Retry unfinished tables (its guarded load).
    assert s.calls == [["app.a", "app.b"]]
    assert s.state.selection.selected_tables == ["app.a", "app.b", "app.c"]


def test_the_selection_sentence_keeps_a_reselected_loaded_table_as_is() -> None:
    # FAILS WITHOUT THE FIX: a table the record has DONE was promised a Retry entry it never
    # gets (re-selecting a loaded table does not reload it).
    from dsql_migrator.ui.data_migration._exclusion_dialog import (
        ExclusionDialogContext,
        _selection_change_sentence,
    )
    from dsql_migrator.ui.data_migration._models import ExclusionInputs

    state = _flo(["a"])
    jobs = _Jobs({"job-1": _job("FAILED", [_c("a"), _c("b"), _c("d", "FAILED")])})
    inputs = ExclusionInputs(selected=["a"], applied={}, preserve_foreign_keys=True,
                             migratable=["a", "b", "c", "d"], signature=("s",))
    ctx = ExclusionDialogContext(
        migration_state=state, job_manager=jobs, session=None, inventory_fn=lambda: None,
        inputs_fn=lambda: inputs, refresh=lambda: None)
    text = _selection_change_sentence(
        ctx, SimpleNamespace(selected_after=["a", "b", "c", "d"]), [])
    assert ("Selects b again: already loaded by the last Full Load, kept as is (Reload "
            "specific tables on the Full Load step reloads it).") in text
    assert "Selects c, d: listed under Retry unfinished tables" in text  # new / unfinished
    # No record (or a CDC type): today's sentence for every selected table.
    other = ExclusionDialogContext(
        migration_state=_flo(["a"], mtype=MigrationType.FULL_LOAD_AND_CDC), job_manager=jobs,
        session=None, inventory_fn=lambda: None, inputs_fn=lambda: inputs,
        refresh=lambda: None)
    assert _selection_change_sentence(other, SimpleNamespace(selected_after=["a", "b"]), []) == (
        "Selects b: listed under Retry unfinished tables on the Full Load step until loaded.")


def test_a_reticked_rule_unselected_table_warns_that_its_columns_are_not_excluded(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: after a rule unselected loaded app.c (dropping its note entry)
    # and the tree ticked it again, the row read "Nothing excluded - locked" and a later
    # Re-run / Reload would have loaded the excluded column silently.
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_dialog
    from dsql_migrator.ui.data_migration._models import start_dialog_exclusion_notes

    from tests.test_exclusion_dialog import _Ui

    s = _reload_screen(monkeypatch, record=("app.a", "app.b", "app.c"),
                       rules="exclude app.c.note", rule_excluded=(),
                       selection=("app.a", "app.b", "app.c"), stored=[("app.c", "note")])
    assert any(t == "1 column excluded - locked" for t in s.ui.texts)  # the plain locked row
    ui = _Ui()
    open_exclusion_dialog(ui, s.ctx)
    ui.click("Change & reload")
    ui.type_text("exclude app.c.note\nexclude table app.c")
    ui.click("Save")
    assert s.state.lob_exclusions() == {}
    s.content(lambda: None)
    assert s.tree["lock_check"]() is None
    s.state.set_selection(TableSelection(selected_tables=["app.a", "app.b", "app.c"]))
    s.tree["on_selection_change"](["app.a", "app.b", "app.c"])
    s.ui.texts.clear()
    s.content(lambda: None)
    warning = ("Your rules exclude app.c.note on tables ticked again after your last Save "
               "(not excluded now), so a reload would load it - use Change & reload")
    assert warning in s.ui.texts
    assert "Nothing excluded - locked" not in s.ui.texts
    assert start_dialog_exclusion_notes(s.state) == [
        ("warning", f"{warning} in Exclude tables & columns.")]


# ---------------------------------------------------------------------------
# 14. The scope notice and Retry when a newly selected table still needs its checks
# ---------------------------------------------------------------------------


class _Btn:
    def __init__(self, text):
        self.text, self.disabled, self.tips = text, False, []

    def props(self, *_a, **_k):
        return self

    def classes(self, *_a, **_k):
        return self

    def disable(self):
        self.disabled = True
        return self

    def tooltip(self, text="", *_a, **_k):
        self.tips.append(str(text))
        return self

    def __getattr__(self, _name):
        return lambda *_a, **_k: self


def _render_with_buttons(monkeypatch, **kw):
    from tests.test_ui_data_migration import _ConfirmDialogUi

    made: dict = {}
    real = _ConfirmDialogUi.button

    def _button(self, text="", *a, on_click=None, **k):
        real(self, text, *a, on_click=on_click, **k)
        made[str(text)] = _Btn(str(text))
        return made[str(text)]

    monkeypatch.setattr(_ConfirmDialogUi, "button", _button)
    ui, notices, progress = _render_step(monkeypatch, **kw)
    return ui, notices, made


GUARD = ("Re-run the prerequisite checks — c was added to the selection after the last Full "
         "Load, so it was never checked.")


def test_retry_is_disabled_when_its_only_tables_still_need_their_checks(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the button stayed enabled and every click was refused.
    job = _job("DONE", [_c("a")], counts={"a": 1})
    _ui, notices, made = _render_with_buttons(monkeypatch, job=job, selected=["a", "c"],
                                              guard_reason=GUARD)
    retry = made["Retry unfinished tables (1)"]
    assert retry.disabled is True and retry.tips == [GUARD]
    assert made["Re-run all tables"].disabled is True, sorted(made)
    (tone, _header, body), = [n for n in notices if n[1].startswith("Table set changed")]
    # The notice names the checks FIRST, and why the step reads Failed.
    assert tone == "warning"
    assert (f"1 newly selected table (c) is not loaded yet, so this step reads Failed until it "
            f"is loaded. First: {GUARD} Then Retry unfinished tables below loads it") in body
    # Without the guard: enabled, and the notice does not mention it.
    _ui, notices, made = _render_with_buttons(monkeypatch, job=job, selected=["a", "c"])
    assert made["Retry unfinished tables (1)"].disabled is False
    (_tone, _header, body), = [n for n in notices if n[1].startswith("Table set changed")]
    assert "First:" not in body and "so this step reads Failed until it is loaded." in body


def test_mixed_retry_stays_enabled_and_the_new_rows_carry_the_guard(monkeypatch) -> None:
    import asyncio

    import nicegui.context as ctxobj

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()),
                        raising=False)
    job = _job("FAILED", [_c("a"), _c("b", "FAILED")], counts={"a": 1, "b": 1})
    ui, _notices, made = _render_with_buttons(monkeypatch, job=job, selected=["a", "b", "c"],
                                              guard_reason=GUARD)
    retry = made["Retry unfinished tables (2)"]
    assert retry.disabled is False and retry.tips == [GUARD]
    asyncio.run(dict(ui.handlers)["Retry unfinished tables (2)"]())
    assert f"Not loaded yet — selected after the last Full Load. {GUARD}" in ui.texts


def test_the_scope_helpers_stay_linear_at_a_large_table_count() -> None:
    # A changed-set render calls these a few times over every table (and the dialog's
    # sentence on every debounced preview): a set rebuilt per element was quadratic --
    # about 2 s at 6000 tables, minutes at this size.
    import time

    from dsql_migrator.ui.data_migration._exclusion_dialog import _selection_change_sentence
    from dsql_migrator.ui.data_migration._models import (
        scope_change,
        scope_full_load_job,
        scope_notice,
    )

    names = [f"app.t{i}" for i in range(10000)]
    job = _job("DONE", [_c(n) for n in names])
    scoped = scope_full_load_job(job, names[1:] + ["app.extra"])
    ctx = SimpleNamespace(
        inputs_fn=lambda: SimpleNamespace(selected=names),
        migration_state=SimpleNamespace(migration_type=None), job_manager=None)
    started = time.perf_counter()
    assert scope_change(job, scoped) == (["app.extra"], ["app.t0"])
    assert scope_notice(job, scoped)[0] == "warning"
    sentence = _selection_change_sentence(
        ctx, SimpleNamespace(selected_after=names[1:] + ["app.extra"]), [])
    assert "Unselects app.t0" in sentence and "Selects app.extra" in sentence
    assert time.perf_counter() - started < 2.0


# ---------------------------------------------------------------------------
# 15. The step sync's side effects: identity sync, error, status filter, popup Save
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status, left", [("FAILED", "FAILED"), ("CANCELLED", "PENDING")])
def test_unselecting_the_only_unfinished_table_runs_the_skipped_identity_sync(
        monkeypatch, status, left) -> None:
    # FAILS WITHOUT THE FIX: the step turned DONE with no identity-sequence sync -- the
    # incomplete run's finalize had skipped it, so app.a's identity key stayed at its start
    # value and the app's first insert after cut-over collided (23505).
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    synced: list = []
    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables",
                        lambda _cfg, names, **_k: synced.append(list(names)) or {})
    job = _job(status, [_c("app.a"), _c("app.b", left)], counts={"app.a": 1, "app.b": 1})
    s = _screen(monkeypatch, job=job, selected=["app.a"])
    s.session.set_workflow(
        with_status(s.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    (work,) = s.jobs.submitted
    work(SimpleNamespace())
    assert synced == [["app.a"]]  # the current set's loaded tables only, never app.b
    s.content(lambda: None)
    assert len(s.jobs.submitted) == 1  # once per flip


def test_a_done_record_flipping_back_to_done_submits_no_sync(monkeypatch) -> None:
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b", "app.c"])
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.FAILED
    s.state.set_selection(TableSelection(selected_tables=["app.a", "app.b"]))
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    assert s.jobs.submitted == []  # its own finalize synced it


def test_the_live_poll_syncs_identity_once_for_a_cancelled_scoped_retry() -> None:
    from dsql_migrator.ui import data_migration as dm

    from tests.test_ui_data_migration import _ConfirmDialogUi

    class _Ui(_ConfirmDialogUi):
        def __init__(self):
            super().__init__()
            self.timers: list = []

        def timer(self, _interval, callback=None, *_a, **_k):
            self.timers.append(callback)
            return self._El(self)

    jobs = _Jobs({"job-1": _job("RUNNING", [_c("a", "PENDING"), _c("b", "FAILED")],
                                counts={"a": 1, "b": 1})})
    state = _flo(["a"])
    session = SimpleNamespace(
        workflow=with_status(WorkflowState(), WorkflowStep.FULL_LOAD, StepStatus.IN_PROGRESS),
        source_verified=True, target_verified=True, target_config=object(), aws_profile=None,
        connection_ready=lambda: True,
    )
    session.set_workflow = lambda w: setattr(session, "workflow", w)
    ui = _Ui()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, jobs, session, job=jobs.jobs["job-1"], status=StepStatus.IN_PROGRESS,
        selected_names=["a"], guard_reason=None, start_full_load=noop,
        retry_failed_load=noop, reload_table=noop, accept_quarantine_and_continue=noop,
        stop_full_load=noop, refresh=noop, retry_tables=noop,
        schema_recreate_candidates=lambda: [],
    )
    (poll,) = [t for t in ui.timers if getattr(t, "__name__", "") == "_poll_live"]
    jobs.jobs["job-1"] = _job("CANCELLED", [_c("a"), _c("b", "FAILED")],
                              counts={"a": 1, "b": 1})
    poll()
    assert get_status(session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    assert len(jobs.submitted) == 1


def test_unticking_the_only_failed_table_clears_its_stale_error(monkeypatch) -> None:
    job = _job("FAILED", [_c("app.a"), _c("app.b", "FAILED")], counts={"app.a": 1, "app.b": 1})
    s = _screen(monkeypatch, job=job, selected=["app.a"])
    s.jobs.errors["job-1"] = "load failed for app.b"
    s.session.set_workflow(
        with_status(s.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.FAILED))
    s.state.set_error("load failed for app.b")
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    assert s.state.error is None
    assert "Migration failed" not in " ".join(s.ui.texts)


def test_a_not_started_step_is_never_written_by_the_sync(monkeypatch) -> None:
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b", "app.c"])
    s.session.set_workflow(
        with_status(s.session.workflow, WorkflowStep.FULL_LOAD, StepStatus.NOT_STARTED))
    before = s.session.workflow
    s.content(lambda: None)
    assert s.session.workflow is before


def test_a_popup_save_syncs_the_step_before_the_page_refresh(monkeypatch) -> None:
    # The nav renders before content(): without the sync in the Save's refresh it would
    # show the old step (DONE, Validation reachable) for the grown set.
    from dsql_migrator.ui.data_migration import _exclusion_dialog as xd

    captured: dict = {}
    real_row = xd.render_exclusion_button_row
    monkeypatch.setattr(xd, "render_exclusion_button_row",
                        lambda ui, ctx: captured.__setitem__("ctx", ctx) or real_row(ui, ctx))
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    seen: list = []
    s.content(lambda: seen.append(get_status(s.session.workflow, WorkflowStep.FULL_LOAD)))
    s.state.set_selection(TableSelection(selected_tables=["app.a", "app.b", "app.c"]))
    captured["ctx"].refresh()
    assert seen == [StepStatus.FAILED]


def test_the_screen_guard_holds_a_table_added_after_the_load(monkeypatch) -> None:
    # content() must pass added_since_run: a restored session (no report, has_run) would
    # otherwise enable "Re-run all tables" over a never-checked table.
    from dsql_migrator.ui import data_migration as dm

    seen: list = []
    real = dm._render_full_load_step

    def _spy(*a, **k):
        seen.append(k.get("guard_reason"))
        return real(*a, **k)

    monkeypatch.setattr(dm, "_render_full_load_step", _spy)
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b", "app.c"])
    s.session.set_source_verified(True)
    s.session.set_target_verified(True)
    s.content(lambda: None)
    assert seen and all(g and "app.c was added to the selection" in g for g in seen), seen
    assert any("app.c was added to the selection" in t for t in s.ui.tooltips)
    assert not any(t.startswith("Re-runs ALL selected tables") for t in s.ui.tooltips)
    seen.clear()
    same = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    same.content(lambda: None)
    assert seen and all(g is None for g in seen), seen


# ---------------------------------------------------------------------------
# 16. An accepted quarantine gap after a set change
# ---------------------------------------------------------------------------


def test_accepted_quarantine_stays_done_after_a_removal() -> None:
    from dsql_migrator.ui.data_migration._full_load_ui import full_load_step_for_scope
    from dsql_migrator.ui.data_migration._models import scope_full_load_job

    job = _job("FAILED", [_c("a", rows_quarantined=2), _c("b")])
    assert full_load_step_for_scope(
        job, scope_full_load_job(job, ["a"]), accepted=True, error_log=_NoLog()
    ) is StepStatus.DONE


def test_accept_quarantine_judges_the_current_set(monkeypatch) -> None:
    job = _job("FAILED", [_c("app.a", rows_quarantined=2), _c("app.b", "FAILED")],
               counts={"app.a": 3, "app.b": 1})
    s = _screen(monkeypatch, job=job, selected=["app.a"])
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.FAILED
    accept = [b for b in s.ui.buttons if b.text == "Accept quarantined rows & continue"]
    assert accept
    accept[-1].on_click()
    assert s.state.accept_quarantined_rows is True
    assert s.state.accepted_quarantine_rows == 2  # only the selected table's gap
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    s.content(lambda: None)  # a later render keeps the acceptance
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE


def test_an_accepted_gap_then_a_removal_keeps_the_step_done(monkeypatch) -> None:
    job = _job("FAILED", [_c("app.a", rows_quarantined=2), _c("app.b")],
               counts={"app.a": 3, "app.b": 1})
    s = _screen(monkeypatch, job=job, selected=["app.a", "app.b"])
    s.state.set_accept_quarantined_rows(True, gap=2)
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE
    s.state.set_selection(TableSelection(selected_tables=["app.a"]))
    s.content(lambda: None)
    assert get_status(s.session.workflow, WorkflowStep.FULL_LOAD) is StepStatus.DONE


# ---------------------------------------------------------------------------
# 17. A running poll tick reads the job no more than before
# ---------------------------------------------------------------------------


def test_a_running_poll_tick_reads_the_job_twice() -> None:
    # FAILS WITHOUT THE FIX: the scoped view ran the predicate's job read for a RUNNING
    # record it never scopes -- a deep copy under the manager lock every 1.5 s per tab.
    from dsql_migrator.ui import data_migration as dm

    from tests.test_ui_data_migration import _ConfirmDialogUi

    class _Counting(_Jobs):
        reads = 0

        def get_status(self, job_id):
            self.reads += 1
            return super().get_status(job_id).model_copy(deep=True)

    class _Ui(_ConfirmDialogUi):
        def __init__(self):
            super().__init__()
            self.timers: list = []

        def timer(self, _interval, callback=None, *_a, **_k):
            self.timers.append(callback)
            return self._El(self)

        def refreshable(self, fn):
            def _w(*a, **k):
                return fn(*a, **k)

            _w.refresh = lambda *_a, **_k: fn()  # a refresh re-renders (and re-reads)
            return _w

    names = ["app.a", "app.b", "app.c"]
    jobs = _Counting({"job-1": _job("RUNNING", [_c(n, "PENDING") for n in names],
                                    counts={n: 1 for n in names})})
    state = _flo(names)
    session = SimpleNamespace(
        workflow=with_status(WorkflowState(), WorkflowStep.FULL_LOAD, StepStatus.IN_PROGRESS),
        source_verified=True, target_verified=True, target_config=None, aws_profile=None,
        connection_ready=lambda: True)
    session.set_workflow = lambda w: setattr(session, "workflow", w)
    ui = _Ui()
    noop = lambda *_a, **_k: None  # noqa: E731
    dm._render_full_load_step(
        ui, state, jobs, session, job=jobs.jobs["job-1"], status=StepStatus.IN_PROGRESS,
        selected_names=names, guard_reason=None, start_full_load=noop,
        retry_failed_load=noop, reload_table=noop, accept_quarantine_and_continue=noop,
        stop_full_load=noop, refresh=noop, retry_tables=noop,
        schema_recreate_candidates=lambda: [])
    poll = [t for t in ui.timers if getattr(t, "__name__", "") == "_poll_live"][-1]
    jobs.reads = 0
    poll()
    assert jobs.reads == 2


# ---------------------------------------------------------------------------
# 18. The Validation screen's half of the scope hold
# ---------------------------------------------------------------------------


def _val_report(*tables, matched=True):
    from dsql_migrator.core.models import TableValidationResult, ValidationMode, ValidationReport

    return ValidationReport.build(mode=ValidationMode.CHECKSUM, items=[TableValidationResult(
        table=t, source_row_count=1, target_row_count=1 if matched else 2,
        row_count_match=matched, checksum_match=matched, matched=matched) for t in tables])


def _val_screen(monkeypatch, selected, report, mtype=MigrationType.FULL_LOAD_ONLY):
    from tests.test_ui_validation import _ModeAwareValidator, _TimerUi, _g1_screen

    ui = _TimerUi()
    s = _g1_screen(monkeypatch, _ModeAwareValidator(), ledger_tables=(), ui=ui)
    s.mstate.migration_type = mtype
    s.mstate.job_id = "job-1"
    s.mstate.selection = TableSelection(selected_tables=list(selected))
    s.mstate.selection_touched = True
    s.vstate.set_result(report)
    s.content(lambda: None)
    return ui


HELD = "Tables match — cut over waits until the result covers every migrated table"


def test_the_validation_screen_holds_a_report_missing_a_selected_table(monkeypatch) -> None:
    held = _val_screen(monkeypatch, ["orders", "customers"], _val_report("orders"))
    assert "Ready for cut-over" not in held.texts and HELD in held.texts
    assert "customers is part of this migration's table set" in held.body()
    covered = _val_screen(monkeypatch, ["orders", "customers"],
                          _val_report("orders", "customers"))
    assert "Ready for cut-over" in covered.texts
    cdc = _val_screen(monkeypatch, ["orders", "customers"], _val_report("orders"),
                      mtype=MigrationType.FULL_LOAD_AND_CDC)
    assert HELD not in cdc.texts


def test_the_validation_screen_names_the_gap_and_the_extra_rows(monkeypatch) -> None:
    bad = _val_screen(monkeypatch, ["orders", "customers"], _val_report("orders", matched=False))
    assert "This result does not cover the current table set" in bad.texts
    extra = _val_screen(monkeypatch, ["orders"], _val_report("orders", "customers"))
    assert any("This result also compares customers, no longer selected" in t
               for t in extra.texts)


# ---------------------------------------------------------------------------
# 19. The tree: the bulk buttons re-check the lock, ticks coalesce, the filter survives
# ---------------------------------------------------------------------------


def test_select_all_and_unselect_all_recheck_the_lock_and_the_filter_survives() -> None:
    from dsql_migrator.ui.data_migration import _render_table_selection

    from tests.test_ui_data_migration import _PickerUi, _inventory

    class _Ui(_PickerUi):
        def __init__(self):
            super().__init__()
            self.clicks: dict = {}
            self.inputs: list = []
            self.notes: list = []

        def button(self, text="", *_a, on_click=None, **_k):
            if text:
                self.buttons.append(str(text))
                self.clicks[str(text)] = on_click
            return self._El(self)

        def input(self, *_a, **k):
            self.inputs.append(k)
            return self._El(self)

        def notify(self, text="", *_a, **_k):
            self.notes.append(str(text))

    reason = {"v": "Locked while a Full Load runs."}
    ui = _Ui()
    state = DataMigrationState()
    refreshed: list = []
    changes: list = []
    filter_state = {"text": "ord"}
    _render_table_selection(
        ui, _inventory(), state, ["orders", "customers"], target_existing=["orders"],
        lock_check=lambda: reason["v"], on_locked=lambda: refreshed.append(1),
        on_selection_change=changes.append, filter_state=filter_state,
    )
    ui.clicks["Select all"]()
    ui.clicks["Unselect all"]()
    assert state.selection_touched is False and changes == []  # nothing applied
    assert refreshed == [1, 1]
    assert ui.notes == ["Locked while a Full Load runs. This change was not applied."] * 2
    reason["v"] = None  # the lock lifted: the click applies
    ui.clicks["Unselect all"]()
    assert state.selection_touched is True and changes == [[]]
    (field,) = ui.inputs
    assert field["value"] == "ord"  # restored after a refresh
    field["on_change"](SimpleNamespace(value="cust"))
    assert filter_state == {"text": "cust"}


def test_a_burst_of_ticks_leaves_one_page_refresh_timer(monkeypatch) -> None:
    from dsql_migrator.ui import data_migration as dm

    captured: dict = {}
    real = dm._render_table_selection

    def _spy(*a, **k):
        captured.update(k)
        return real(*a, **k)

    monkeypatch.setattr(dm, "_render_table_selection", _spy)
    s = _screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"])
    s.content(lambda: None)

    class _Timer:
        def __init__(self, interval):
            self.interval, self.cancelled = interval, False

        def cancel(self):
            self.cancelled = True

    timers: list = []
    s.ui.timer = lambda interval, callback=None, *a, **k: (
        timers.append(_Timer(interval)) or timers[-1])
    for chosen in (["app.a", "app.b", "app.c"], ["app.a"], ["app.a", "app.c"]):
        s.state.set_selection(TableSelection(selected_tables=chosen))
        captured["on_selection_change"](chosen)
    assert sorted(t.interval for t in timers if not t.cancelled) == [0.3, 0.8]
