# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G17: Full load + CDC can ADD a table after its Full Load, before CDC starts (guarded).

A table the Full Load did not load can be ticked while G16's narrowing holds (CDC never
started for the migration). It is unfinished on the Full Load step until Retry unfinished
tables loads it under the ORIGINAL watermark -- after guards that run before any snapshot
of the retry opens (CDC never started; the Aurora DSQL table exists and is empty or
replaced; MySQL: its live definition equals the start-point baseline recorded with the
watermark; PostgreSQL: it joins CDC's publication and every transaction open at that moment
ends) -- and only a guarded, completed, unquarantined load records it on the watermark
(``Watermark.cdc_added_tables``), so CDC captures it from the original start point. Start
CDC, Validation and Cut over wait until then. Once CDC has started, every surface is
today's. Doubles only: no AWS, no source, no Aurora DSQL, no network.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import (
    ChunkState,
    ColumnDef,
    MigrationJob,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
    SourceType,
    StepStatus,
    TableDef,
    TableSelection,
    Watermark,
)
from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType
from dsql_migrator.ui.workflow import WorkflowStep, get_status, with_status

from tests.test_g12_table_set_after_load import (
    TODAY,
    _c,
    _flo,
    _inv,
    _job,
    _Jobs,
    _loaded,
)
from tests.test_g16_full_load_and_cdc_narrowing import (
    _fc,
    _fc_screen,
    _job_reads,
    _render_tree,
    _rerender,
    _step,
    _wire_tree,
)
from tests.test_ui_data_migration import _RecordingUi

FLC = MigrationType.FULL_LOAD_AND_CDC
FLO = MigrationType.FULL_LOAD_ONLY
CDC_ONLY = MigrationType.CDC_ONLY
DONE = StepStatus.DONE
_TS = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _fp(ncols: int = 1, *, charset: str = "", names=("id",), types=("int",)):
    """A comparable MySQL start-point fingerprint (``start_point_value`` of a live table)."""
    from dsql_migrator.core.cdc_catalog_drift import LiveColumn, LiveTable, start_point_value

    columns = tuple(
        LiveColumn(name=n, type=t, nullable=False, generated=False, position=i,
                   charset=charset)
        for i, (n, t) in enumerate(zip(names, types))
    )
    return start_point_value(LiveTable(columns=columns, primary_key=frozenset({"id"})))


def _with(job, *, added=None, fingerprints=None, slot=None, publication=None):
    """``job`` with G17 facts on its watermark (a copy)."""
    update = {}
    if added is not None:
        update["cdc_added_tables"] = dict(added)
    if fingerprints is not None:
        update["table_fingerprints"] = dict(fingerprints)
    if slot is not None:
        update["slot_name"] = slot
        update["publication_name"] = publication or "dsqlmig_pub_x"
    job = job.model_copy(deep=True)
    job.watermark = job.watermark.model_copy(update=update)
    return job


def _three(status="DONE"):
    """A Full Load record over app.a/app.b plus a chunk for app.c (a DONE detour load)."""
    return _job(status, [_c("app.a"), _c("app.b"), _c("app.c")],
                counts={"app.a": 1, "app.b": 1})


def _cdc_report(*names, status=PrerequisiteStatus.PASS):
    """A CDC-mode prerequisite report that COVERED exactly ``names``."""
    results = [PrerequisiteResult(check_id=PrerequisiteCheckId.SOURCE_REACHABLE,
                                  title="Source reachable", status=PrerequisiteStatus.PASS,
                                  required=True)]
    for name in names:
        for check in (PrerequisiteCheckId.TABLE_PRIMARY_KEY,
                      PrerequisiteCheckId.TARGET_SCHEMA_READY):
            results.append(PrerequisiteResult(check_id=check, title=str(check.value),
                                              status=status, required=True, target=name))
    return PrerequisiteReport.build(MigrationMode.CDC, results)


# ---------------------------------------------------------------------------
# 1. The coverage model: the run's tables plus the guarded additions
# ---------------------------------------------------------------------------


def test_cdc_capture_follows_a_guarded_addition_and_never_an_uncovered_table() -> None:
    # FAILS WITHOUT THE FIX: the capture read the count keys alone, so a guarded addition
    # (recorded in cdc_added_tables) was never captured.
    from dsql_migrator.ui.data_migration import cdc_capture_names, cdc_covered_names
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_tables_for_config

    inv = _inv("app.a", "app.b", "app.c", "app.d")
    wm = _with(_loaded(), added={"app.c": 7}).watermark
    assert cdc_covered_names(wm) == {"app.a", "app.b", "app.c"}
    assert cdc_covered_names(_loaded().watermark) == {"app.a", "app.b"}
    assert cdc_covered_names(_job(counts={}).watermark) is None
    state = _fc(["app.a", "app.c", "app.d"])
    assert cdc_capture_names(state, wm) == {"app.a", "app.c"}  # app.d: not covered
    assert [t.name for t in _cdc_tables_for_config(state, inv, wm)] == ["app.a", "app.c"]
    unticked = _fc(["app.a"])
    assert cdc_capture_names(unticked, wm) == {"app.a"}
    # An untouched selection is the covered set, additions included.
    assert cdc_capture_names(_fc(["app.a"], touched=False), wm) == {
        "app.a", "app.b", "app.c"}


def test_cdc_added_tables_stays_out_of_the_payload_while_empty_and_round_trips(
        tmp_path) -> None:
    from dsql_migrator.core.job_store import SqliteJobStore

    empty = Watermark(snapshot_timestamp=_TS, table_row_counts={"app.a": 1})
    assert "cdc_added_tables" not in empty.model_dump_json()
    job = _with(_loaded(), added={"app.c": 5})
    assert '"cdc_added_tables":{"app.c":5}' in job.model_dump_json()
    store = SqliteJobStore(str(tmp_path / "jobs.sqlite"))
    store.save(job, None)
    (loaded,) = SqliteJobStore(str(tmp_path / "jobs.sqlite")).load_all()
    assert loaded.job.watermark.cdc_added_tables == {"app.c": 5}
    assert set(loaded.job.watermark.table_row_counts) == {"app.a", "app.b"}


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY])
def test_validation_covered_tables_are_the_original_run_set(mtype) -> None:
    # FAILS WITHOUT THE FIX: Full load + CDC / CDC only had no covered set at all.
    from dsql_migrator.ui.validation import full_load_covered_tables

    jobs = _Jobs({"job-1": _with(_three(), added={"app.c": 1})})
    assert full_load_covered_tables(_flo(mtype=mtype), jobs) == frozenset({"app.a", "app.b"})


# ---------------------------------------------------------------------------
# 2. The add-lock: rule-held tables only, and a truthful refusal for a never-loaded one
# ---------------------------------------------------------------------------


def test_the_add_lock_refuses_only_a_rule_held_table() -> None:
    from dsql_migrator.ui.data_migration import table_set_add_lock

    jobs = _Jobs({"job-1": _loaded()})
    state = _fc(["app.a"])
    lock = table_set_add_lock(state, jobs)
    # FAILS WITHOUT THE FIX ("not_loaded" in the tree and the popup).
    assert lock.code("app.c", tree=True) is None and lock.code("app.c", tree=False) is None
    state.restore_exclusion_rules("exclude table app.b\nexclude table app.c",
                                  ["app.b", "app.c"])
    lock = table_set_add_lock(state, jobs)
    assert lock.code("app.c", tree=True) == "rule" and lock.code("app.c", tree=False) is None
    unloaded = lock.refusal(["app.c"], tree=True)
    assert unloaded == (
        "An exclude table rule unselected app.c, and that Save dropped its excluded-column "
        "entries: ticked here, it would load and stream every column, including the ones its "
        "column rules exclude. To select it, delete that rule in Exclude tables & columns "
        "(Change & reload), which applies its column rules.")
    assert "loaded rows hold as NULL" not in unloaded
    assert lock.refusal(["app.b"], tree=True) == (
        "An exclude table rule unselected app.b, and that Save dropped its excluded-column "
        "entries: ticked here, CDC would stream values for columns its loaded rows hold as "
        "NULL. To select it again, delete that rule in Exclude tables & columns (Change & "
        "reload), which applies its column rules again.")
    # A guarded addition is covered, so it keeps the covered text.
    added = table_set_add_lock(state, _Jobs({"job-1": _with(_loaded(), added={"app.c": 1})}))
    assert "loaded rows hold as NULL" in added.refusal(["app.c"], tree=True)
    # Full load only, or a migration that started CDC: no add-lock (today's behaviour).
    assert table_set_add_lock(_flo(["app.a"]), jobs) is None
    for evidence in ("ops", "connectors", "offset"):
        started = _fc(["app.a"])
        if evidence == "ops":
            started.set_cdc_ops_window_start(_TS)
        elif evidence == "connectors":
            started.cdc_connector_names = ["dsql-cdc-x-source"]
        else:
            started.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
            started.set_cdc_has_committed_offset(True)
        assert table_set_add_lock(started, jobs) is None


def test_the_tree_stores_an_uncovered_tick_and_untick_stays_free() -> None:
    # FAILS WITHOUT THE FIX: the tick was refused whole ("use Start over").
    state = _fc(["app.a", "app.b"])
    jobs = _Jobs({"job-1": _loaded()})
    ui, changes, refreshed = _render_tree(state, jobs, ["app.a", "app.b", "app.c"])
    (on_tick,) = ui.trees
    on_tick(SimpleNamespace(value=["table:app.a", "table:app.b", "table:app.c"]))
    assert state.selection.selected_tables == ["app.a", "app.b", "app.c"]
    assert ui.notes == [] and refreshed == []
    on_tick(SimpleNamespace(value=["table:app.a"]))
    assert state.selection.selected_tables == ["app.a"]
    (template,) = [t for name, t in ui.slots if name == "header-table"]
    assert "not_loaded" not in template


# ---------------------------------------------------------------------------
# 3. The scoped Full Load view: a table the watermark does not cover is unfinished
# ---------------------------------------------------------------------------


def test_an_uncovered_selected_table_is_unfinished_in_the_full_load_and_cdc_view() -> None:
    # FAILS WITHOUT THE FIX (F20): app.c was silently dropped from the view -- the step
    # stayed DONE, Retry never offered it and CDC never captured it.
    from dsql_migrator.ui.data_migration import (
        full_load_step_for_scope,
        scoped_full_load_view,
        unsettled_table_names,
    )
    from dsql_migrator.ui.data_migration._models import full_load_scope_names

    jobs = _Jobs({"job-1": _loaded()})
    state = _fc(["app.a", "app.b", "app.c"])
    record = jobs.jobs["job-1"]
    assert full_load_scope_names(state, jobs, ["app.a", "app.b", "app.c"], job=record) == [
        "app.a", "app.b", "app.c"]
    view = scoped_full_load_view(state, jobs, ["app.a", "app.b", "app.c"], job=record)
    assert unsettled_table_names(view) == ["app.c"]
    assert full_load_step_for_scope(record, view, accepted=False,
                                    error_log=state.error_log) is StepStatus.FAILED
    # Nothing pending: G16's removal-only view, byte for byte.
    assert scoped_full_load_view(state.__class__(), jobs, [], job=record) is record
    narrowed = _fc(["app.a"])
    assert full_load_scope_names(narrowed, jobs, ["app.a"], job=record) == ["app.a"]
    # Once CDC has started: removal-only again (app.c is never added back).
    state.set_cdc_ops_window_start(_TS)
    assert full_load_scope_names(state, jobs, ["app.a", "app.b", "app.c"], job=record) is None
    assert scoped_full_load_view(state, jobs, ["app.a", "app.b", "app.c"],
                                 job=record) is record


@pytest.mark.parametrize("chunk", [_c("app.c"), _c("app.c", rows_quarantined=2)],
                         ids=["detour", "quarantined"])
def test_a_done_chunk_of_an_uncovered_table_is_presented_pending(chunk) -> None:
    # FAILS WITHOUT THE FIX: a DONE chunk (a Full load only detour, a quarantined guarded
    # load, a lost coverage write) read as loaded -- the step DONE, nothing captured it.
    from dsql_migrator.ui.data_migration import (
        _incomplete_is_quarantine_only,
        scope_full_load_job,
        scoped_full_load_view,
    )

    record = _job("DONE", [_c("app.a"), _c("app.b"), chunk], counts={"app.a": 1, "app.b": 1})
    state = _fc(["app.a", "app.b", "app.c"])
    jobs = _Jobs({"job-1": record})
    view = scoped_full_load_view(state, jobs, ["app.a", "app.b", "app.c"], job=record)
    assert view is not record
    by = {c.chunk_id: c for c in view.chunks}
    assert by["app.c"].status == "PENDING"
    assert by["app.c"].rows_quarantined == chunk.rows_quarantined  # kept
    assert by["app.c"].attempts == chunk.attempts
    assert record.chunks[2].status == "DONE"  # never mutated
    assert not _incomplete_is_quarantine_only(view, state.error_log)
    # The reset applies even when the names equal the record's tables.
    same = scope_full_load_job(record, ["app.a", "app.b", "app.c"], unfinished=["app.c"])
    assert same is not record and [c.status for c in same.chunks] == [
        "DONE", "DONE", "PENDING"]
    assert scope_full_load_job(record, ["app.a", "app.b", "app.c"]) is record


def test_the_step_fails_for_a_pending_addition_and_settles_on_untick(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the step stayed DONE with app.c selected and never loaded.
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables", lambda *_a, **_k: {})
    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], render=False)
    held = _wire_tree(s)
    _rerender(s)
    assert _step(s) is DONE
    held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b", "table:app.c"]))
    assert _step(s) is StepStatus.FAILED and s.state.error is None  # nothing failed
    _rerender(s)
    assert any(b.text.startswith("Retry unfinished tables (1)") for b in s.ui.buttons)
    held["on_tick"](SimpleNamespace(value=["table:app.a", "table:app.b"]))
    assert _step(s) is DONE


def _notice(job, view, **kw):
    from dsql_migrator.ui.data_migration._models import scope_notice

    return scope_notice(job, view, cdc=True, **kw)


def test_the_scope_notice_names_the_guarded_load_and_its_route() -> None:
    from dsql_migrator.ui.data_migration import scoped_full_load_view

    record = _loaded()
    state = _fc(["app.a", "app.b", "app.c"])
    view = scoped_full_load_view(state, _Jobs({"job-1": record}), ["app.a", "app.b", "app.c"],
                                 job=record)
    tone, header, body = _notice(record, view, pending=["app.c"])
    assert (tone, header) == ("warning", "Table set changed after the last Full Load")
    assert body == (
        "1 table selected after the last Full Load (app.c) is not loaded under its start point "
        "yet, so this step reads Failed until it is. Retry unfinished tables below loads it "
        "and keeps every loaded table. CDC then captures it from that start point with the "
        "other tables; Start CDC, Validation and Cut over wait until then.")
    guard = "Re-run the prerequisite checks — app.c was added to the selection after the last "
    first = _notice(record, view, pending=["app.c"], blocked_by=guard + "Full Load.")[2]
    assert f"it is. First: {guard}Full Load. Then Retry unfinished tables" in first
    pg = _notice(record, view, pending=["app.c"], pg=True)[2]
    assert ("loads it and keeps every loaded table; it first adds it to CDC's publication "
            "on the source. CDC then captures it") in pg
    # Without pending: G16's notice exactly (a removal, info).
    narrowed = scoped_full_load_view(_fc(["app.a"]), _Jobs({"job-1": record}), ["app.a"],
                                     job=record)
    assert _notice(record, narrowed)[0] == "info"


def test_the_scope_notice_never_promises_retry_for_a_table_retry_cannot_add() -> None:
    # FAILS WITHOUT THE FIX: the notice said "Retry unfinished tables below loads it" and
    # then "app.c cannot be added by Retry" -- while Retry was disabled for it.
    from dsql_migrator.ui.data_migration import scoped_full_load_view

    record = _loaded()
    abc = ["app.a", "app.b", "app.c"]
    view = scoped_full_load_view(_fc(abc), _Jobs({"job-1": record}), abc, job=record)
    blocked = _notice(record, view, pending=["app.c"], not_addable=["app.c"])[2]
    assert blocked == (
        "1 table selected after the last Full Load (app.c) is not loaded under its start point "
        "yet, so this step reads Failed until it is. app.c cannot be added by Retry: this Full "
        "Load recorded no start-point definition for it, so CDC could not decode its "
        "binary-log changes since then. Re-run all tables (a fresh start point over every "
        "selected table) loads it, or untick it.")
    # With the run guard, it leads to the route left -- which it holds too.
    guard = ("Re-run the prerequisite checks — app.c was added to the selection after the "
             "last Full Load.")
    first = _notice(record, view, pending=["app.c"], not_addable=["app.c"], blocked_by=guard)[2]
    assert f"since then. First: {guard} Then Re-run all tables (a fresh start point" in first
    assert "Retry unfinished tables below loads" not in first
    assert "CDC then captures it" not in first
    # Mixed: Retry loads the one it can add, by name; the other is named apart.
    abcd = abc + ["app.d"]
    mixed_view = scoped_full_load_view(_fc(abcd), _Jobs({"job-1": record}), abcd, job=record)
    mixed = _notice(record, mixed_view, pending=["app.c", "app.d"], not_addable=["app.c"],
                    blocked_by=guard)[2]
    assert (f"until they are. First: {guard} Then Retry unfinished tables below loads app.d "
            "and keeps every loaded table. CDC then captures it from that start point") in mixed
    assert mixed.endswith(
        " app.c cannot be added by Retry: this Full Load recorded no start-point definition for "
        "it, so CDC could not decode its binary-log changes since then. Re-run all tables (a "
        "fresh start point over every selected table) loads it, or untick it.")


def test_the_step_and_its_notice_agree_on_a_table_retry_cannot_add(monkeypatch) -> None:
    # The rendered step: Retry unfinished tables is disabled for a MySQL table with no
    # start-point definition, and the notice above it never says Retry loads it.
    from dsql_migrator.ui.data_migration import cdc_not_addable_refusal

    s, _retry, _calls = _retry_screen(monkeypatch, job=_loaded(),
                                      selected=["app.a", "app.b", "app.c"],
                                      report=("app.a", "app.b", "app.c"))
    _rerender(s)
    assert cdc_not_addable_refusal(["app.c"]) in s.ui.tooltips  # Retry: disabled with it
    assert not any("Retry unfinished tables below loads" in t for t in s.ui.texts)
    assert any("app.c cannot be added by Retry" in t for t in s.ui.texts)


def test_the_narrowing_copy_never_promises_retry_for_every_ticked_table() -> None:
    # FAILS WITHOUT THE FIX: the tree hint, R4, R1, the Start tooltip, Change & reload's
    # line and CDC only's lock all said Retry unfinished tables loads ANY ticked table --
    # false for a MySQL table the Full Load recorded no start point for (only Re-run all
    # tables loads it).
    from dsql_migrator.ui.data_migration import (
        CDC_ONLY_NARROWING_SENTENCE,
        R1_FL_CDC_SENTENCE,
        R4_FL_CDC_NARROWING,
        START_TOOLTIP_NARROWING,
        TREE_HINT_FL_CDC,
    )
    from dsql_migrator.ui.data_migration._exclusion_dialog import FL_CDC_RELOAD_LABEL

    route = "Retry unfinished tables, or Re-run all tables where that step says Retry cannot add it"
    for text in (TREE_HINT_FL_CDC, R4_FL_CDC_NARROWING, R1_FL_CDC_SENTENCE, FL_CDC_RELOAD_LABEL):
        assert route in text, text
        assert "from this Full Load's start point" not in text
        assert "from the Full Load's start point" not in text
    assert "is loaded on the Full Load step first" in START_TOOLTIP_NARROWING
    assert "Retry" not in START_TOOLTIP_NARROWING and "Retry" not in CDC_ONLY_NARROWING_SENTENCE
    # Change & reload drops and reloads only a table CDC captures (G17).
    assert FL_CDC_RELOAD_LABEL.startswith(
        "Column and table rules are editable. A column change drops and reloads a table CDC "
        "captures;")


def test_the_continue_hint_is_offered_for_a_pending_addition(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: untick_can_settle read the record (app.c DONE in it), so the
    # untick route was not offered although unticking settles the step.
    from dsql_migrator.ui.data_migration import CONTINUE_HINT_FL_CDC
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    monkeypatch.setattr(fl, "sync_identity_sequences_for_tables", lambda *_a, **_k: {})
    s = _fc_screen(monkeypatch, job=_three(), selected=["app.a", "app.b", "app.c"],
                   render=False)
    _rerender(s)
    _rerender(s)
    assert _step(s) is StepStatus.FAILED
    assert any(t.endswith(CONTINUE_HINT_FL_CDC) for t in s.ui.texts)


# ---------------------------------------------------------------------------
# 4. Performance: still linear, still a bounded number of job reads per render
# ---------------------------------------------------------------------------


def test_the_g17_helpers_stay_linear() -> None:
    from dsql_migrator.ui.data_migration import (
        cdc_covered_names,
        cdc_uncovered_selection,
        scope_full_load_job,
    )
    from dsql_migrator.ui.data_migration._models import full_load_scope_names

    names = [f"app.t{i}" for i in range(20000)]
    counts = {n: 1 for n in names[:10000]}
    added = {n: 1 for n in names[10000:15000]}
    job = _with(_job("DONE", [_c(n) for n in names[:15000]], counts=counts), added=added)
    state = _fc(names)
    jobs = _Jobs({"job-1": job})
    started = time.perf_counter()
    assert len(cdc_covered_names(job.watermark)) == 15000
    pending = cdc_uncovered_selection(state, jobs, names, job=job)
    assert pending == names[15000:]
    scope = full_load_scope_names(state, jobs, names, job=job)
    assert len(scope) == 20000
    view = scope_full_load_job(job, scope, unfinished=pending)
    assert sum(1 for c in view.chunks if c.status == "PENDING") == 5000
    assert time.perf_counter() - started < 2.0


def test_the_scope_notice_stays_linear_with_tables_retry_cannot_add() -> None:
    # FAILS WITHOUT THE FIX: set(added) was rebuilt once per not-addable name -- 11 s at
    # 20,000 tables, on the NiceGUI loop at every render of the Full Load step.
    from dsql_migrator.ui.data_migration._models import scope_full_load_job, scope_notice

    names = [f"app.t{i}" for i in range(20000)]
    job = _job("DONE", [_c(n) for n in names[:100]], counts={n: 1 for n in names[:100]})
    view = scope_full_load_job(job, names, unfinished=names[100:])
    started = time.perf_counter()
    tone, _header, body = scope_notice(job, view, cdc=True, pending=names[100:],
                                       not_addable=names[100:])
    assert time.perf_counter() - started < 2.0
    assert tone == "warning" and "cannot be added by Retry" in body
    started = time.perf_counter()
    mixed = scope_notice(job, view, cdc=True, pending=names[100:], not_addable=names[100::2])
    assert time.perf_counter() - started < 2.0 and "Retry unfinished tables below" in mixed[2]


def test_the_retry_click_reasons_stay_linear_with_many_pending_additions(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: each unfinished table's reason scanned the not-addable, pending
    # and added LISTS -- seconds on the NiceGUI loop at a Retry click over 20,000 tables.
    loaded = [f"app.l{i}" for i in range(100)]
    pending = [f"app.t{i:06d}" for i in range(20000)]
    job = _with(_job("DONE", [_c(t) for t in loaded], counts={t: 1 for t in loaded}),
                fingerprints={t: _fp() for t in pending})
    started = time.perf_counter()
    _open_retry_dialog(monkeypatch, state=_fc(loaded + pending), job=job, rows=(),
                       tables=loaded + pending)
    # About 1 s here, most of it the double drawing 20,000 checklist rows; about 5 s with the
    # per-name list scans. The bound leaves headroom for a slower machine.
    assert time.perf_counter() - started < 3.0


def test_a_render_with_a_pending_addition_reads_the_job_record_a_bounded_number_of_times(
        monkeypatch) -> None:
    # The same bound as G16's absent state (20): the pending view reads only the record the
    # render already holds.
    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b", "app.c"],
                   render=False)
    s.state.set_cdc_stack_phase("absent", status=None)
    s.content(lambda: None)
    assert _job_reads(s, lambda: s.content(lambda: None)) <= 20


# ---------------------------------------------------------------------------
# 5. Change & reload: a table line selects any migratable table; nothing uncovered drops
# ---------------------------------------------------------------------------


def test_an_include_line_selects_an_uncovered_table_with_its_column_rules(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the include line was an E3 ("... use Start over").
    from tests.test_g12_table_set_after_load import _reload_screen
    from tests.test_g16_full_load_and_cdc_narrowing import _action, _open_reload

    s = _reload_screen(monkeypatch, record=("app.a", "app.b"),
                       rules="exclude table app.c\nexclude app.c.note",
                       rule_excluded=("app.c",), selection=("app.a", "app.b"), mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    ui = _open_reload(s)
    ui.type_text("exclude app.c.note")  # deletes the table rule: app.c is selected again
    assert not any("E3" in t or "would select app.c" in t for t in ui.texts)
    # (This MySQL watermark records no start point for app.c, so Retry cannot add it: the
    # line names Re-run all tables too, and never promises the original start point.)
    assert any("Selects app.c: listed under Retry unfinished tables on the Full Load step "
               "until loaded (Re-run all tables loads it where that step says Retry cannot "
               "add it); CDC then captures it with the other tables." in t for t in ui.texts)
    assert _action(ui).enabled and _action(ui).text == "Save"
    ui.click("Save")
    assert s.calls == []  # nothing of app.c is dropped or reloaded here
    assert "app.c" in s.state.selection.selected_tables
    assert {t: set(c) for t, c in s.state.lob_exclusions().items()} == {"app.c": {"note"}}
    # Its load overlays the excluded column, and CDC excludes it once captured.
    from dsql_migrator.ui.data_migration import _overlay_inputs
    from dsql_migrator.ui.data_migration._models import cdc_capture_exclusions

    inventory = s.ctx.inventory_fn()
    applied = s.state.applied_conversions()[0]
    overlay = _overlay_inputs(s.state, inventory, applied, True, tables=["app.c"])
    assert overlay["overlay_columns"] == {"app.c": frozenset({"note"})}
    captured = [t for t in inventory.tables if t.name in ("app.a", "app.c")]
    assert "app.c" in cdc_capture_exclusions(s.state, inventory, captured).sent


@pytest.mark.parametrize("record_chunk", ["FAILED", "DONE"])
def test_change_and_reload_never_drops_an_uncovered_table(monkeypatch, record_chunk) -> None:
    # FAILS WITHOUT THE FIX: app.c's chunk in the record (a failed guarded load, a Full load
    # only detour) put it in the Drop & reload set -- reloaded outside its guards.
    from dsql_migrator.ui.data_migration._exclusion_dialog import _drop_reload_tables

    from tests.test_g12_table_set_after_load import _reload_screen

    s = _reload_screen(monkeypatch, record=("app.a", "app.b"), rules="",
                       rule_excluded=(), selection=("app.a", "app.b", "app.c"), mtype=FLC)
    s.state.set_cdc_stack_phase("absent", status=None)
    s.jobs.jobs["job-1"] = _job("FAILED" if record_chunk == "FAILED" else "DONE",
                                [_c("app.a"), _c("app.b"), _c("app.c", record_chunk)],
                                counts={"app.a": 1, "app.b": 1})
    from dsql_migrator.ui.data_migration._exclusion_dialog import _resolve

    inventory, inputs = s.ctx.inventory_fn(), s.ctx.inputs_fn()
    resolution = _resolve(s.ctx, inventory, inputs, "exclude app.*.note", slot="editor")
    assert _drop_reload_tables(s.ctx, resolution, inputs.migratable, []) == ["app.a", "app.b"]


_NO_CHANGE = "No table's excluded columns change"


def _uncaptured_reload_screen(monkeypatch, chunk, mtype=FLC):
    """Change & reload in a session whose selected app.c is NOT covered by the watermark
    (its record chunk: a quarantined or failed guarded load), CDC never started."""
    from tests.test_g12_table_set_after_load import _reload_screen

    s = _reload_screen(monkeypatch, record=("app.a", "app.b"), rules="", rule_excluded=(),
                       selection=("app.a", "app.b", "app.c"), mtype=mtype)
    s.state.set_cdc_stack_phase("absent", status=None)
    s.jobs.jobs["job-1"] = _job("DONE", [_c("app.a"), _c("app.b"), chunk],
                                counts={"app.a": 1, "app.b": 1})
    s.content(lambda: None)
    return s


def _typed(ui, text):
    # The double keeps every refresh's texts: only what the preview drew for ``text``.
    mark = len(ui.texts)
    ui.type_text(text)
    return ui.texts[mark:]


@pytest.mark.parametrize("chunk", [_c("app.c", rows_quarantined=2), _c("app.c", "FAILED")],
                         ids=["quarantined", "failed"])
def test_a_column_rule_on_a_table_cdc_does_not_capture_yet_is_never_called_no_change(
        monkeypatch, chunk) -> None:
    # FAILS WITHOUT THE FIX: the preview said "No table's excluded columns change" for the
    # very rule the quarantine card inserted -- the table is simply loaded later, by Retry.
    from tests.test_g16_full_load_and_cdc_narrowing import _action, _open_reload

    s = _uncaptured_reload_screen(monkeypatch, chunk)
    ui = _open_reload(s)
    shown = _typed(ui, "exclude app.c.note")
    assert not any(_NO_CHANGE in t for t in shown)
    assert any(t.startswith("Nothing is dropped and reloaded now: Save stores the rules. The "
                            "excluded columns of app.c change, but CDC does not capture it yet, "
                            "so it is not dropped here: its load on the Full Load step applies "
                            "the saved rules (it is listed under Retry unfinished tables there).")
               for t in shown)
    assert _action(ui).text == "Save"
    ui.click("Save")
    assert s.calls == []  # nothing dropped or reloaded here
    assert {t: set(c) for t, c in s.state.lob_exclusions().items()} == {"app.c": {"note"}}
    # Beside tables it does drop, it is named apart.
    s2 = _uncaptured_reload_screen(monkeypatch, chunk)
    ui2 = _open_reload(s2)
    mixed = _typed(ui2, "exclude app.*.note")
    assert _action(ui2).text == "Drop & reload 2 tables"
    assert any(t.startswith("The excluded columns of app.c change too, but CDC does not "
                            "capture it yet") for t in mixed)


def test_the_quarantine_route_lands_on_the_deferred_text(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._exclusion_dialog import open_exclusion_route

    from tests.test_exclusion_dialog import _Ui

    s = _uncaptured_reload_screen(monkeypatch, _c("app.c", rows_quarantined=2))
    ui = _Ui()
    open_exclusion_route(ui, s.ctx, "app.c",
                         "datatype limit greater than 1048576 bytes not supported for text",
                         lob_candidates_for=lambda _t: [("note", "longtext")])
    assert "exclude app.c.note" in ui.editors[-1].value
    first = ui.texts[next(i for i, t in enumerate(ui.texts) if "matches 1 column" in t):]
    assert not any(_NO_CHANGE in t for t in first)
    assert any("The excluded columns of app.c change" in t for t in first)


def test_full_load_only_and_a_true_no_change_keep_their_reload_texts(monkeypatch) -> None:
    from tests.test_g16_full_load_and_cdc_narrowing import _open_reload

    # Full load only (G12): the record's table is dropped and reloaded, as before.
    flo = _uncaptured_reload_screen(monkeypatch, _c("app.c", rows_quarantined=2), mtype=FLO)
    ui = _open_reload(flo)
    ui.type_text("exclude app.c.note")
    assert "Drop & reload app.c" in ui.buttons
    # A rule that changes nothing still says so.
    same = _uncaptured_reload_screen(monkeypatch, _c("app.c", rows_quarantined=2))
    ui2 = _open_reload(same)
    assert any(_NO_CHANGE in t for t in _typed(ui2, "# nothing excluded"))


def test_the_quarantine_card_says_what_exclude_column_does_for_an_uncaptured_table() -> None:
    # FAILS WITHOUT THE FIX: its tooltip promised "its Change & reload drops and reloads this
    # table" -- false for a table CDC does not capture yet.
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import (
        _render_full_load_progress,
        build_full_load_table_rows,
    )

    tips: list = []

    class _El:
        def __init__(self, kind="", text=""):
            self.kind, self.text = kind, text

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def tooltip(self, text="", *a, **k):
            if self.kind == "button":
                tips.append((self.text, str(text)))
            return self

        def __getattr__(self, _name):
            return lambda *a, **k: self

    class _BtnUi:
        def button(self, text="", *a, **k):
            return _El("button", str(text))

        def __getattr__(self, _name):
            return lambda *a, **k: _El()

    job = MigrationJob(job_id="j1", status="FAILED", progress_pct=100.0, chunks=[
        ChunkState(chunk_id="app.c", status="DONE", rows_loaded=4, rows_quarantined=1,
                   attempts=1)])
    records = [("app.c", "quarantined row pk[id=3]: datatype limit greater than 1048576 bytes")]
    for uncaptured, expected in ((frozenset({"app.c"}), "CDC does not capture this table yet, "
                                  "so its Change & reload does not drop it"),
                                 (frozenset(), "its Change & reload drops and reloads this "
                                  "table")):
        tips.clear()
        _render_full_load_progress(
            _BtnUi(), job, build_full_load_table_rows(job, None, {"app.c": records[0][1]}),
            reload_confirm=lambda *_a, **_k: None, quarantine_only=True,
            quarantine_records=records, lob_candidates_for=lambda _t: [("note", "longtext")],
            exclusion_route=lambda *_a: None, cdc_uncaptured=uncaptured)
        ((_label, tip),) = [t for t in tips if t[0] == "Exclude column & reload"]
        assert expected in tip


# ---------------------------------------------------------------------------
# 6. The Retry route: keyed by coverage, the guard chain at the click, what is submitted
# ---------------------------------------------------------------------------


def _retry_screen(monkeypatch, *, job, selected, report=(), pg=False, phase="absent"):
    from dsql_migrator.ui import data_migration as dm

    calls: list = []
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, prior, tables, **kw: calls.append(
                            ([c.chunk_id for c in prior], [t.name for t in tables], kw)))
    s = _fc_screen(monkeypatch, job=job, selected=selected, pg=pg, phase=phase)
    s.session.set_source_verified(True)
    s.session.set_target_verified(True)
    if report:
        s.state.set_prereq_report(MigrationMode.CDC, _cdc_report(*report))
    s.jobs.submit = lambda fn: fn(SimpleNamespace()) or "job-2"
    return s, s.captured["ctx"].extra["run_retry_for"], calls


def _baselined(job, *names):
    return _with(job, fingerprints={n: _fp() for n in names})


def test_retry_submits_the_additions_and_the_cdc_never_started_rule(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: app.c was not even in the view; nothing told the engine it
    # was a guarded addition.
    s, retry, calls = _retry_screen(
        monkeypatch, job=_baselined(_loaded(), "app.c"),
        selected=["app.a", "app.b", "app.c"], report=("app.a", "app.b", "app.c"))
    retry(["app.c"])
    ((_prior, tables, kw),) = calls
    assert tables == ["app.c"] and kw["cdc_additions"] == ("app.c",)
    assert kw["run_table_names"] == ["app.a", "app.b", "app.c"]
    assert kw["cdc_coverage_open"]() is True
    s.state.set_cdc_ops_window_start(_TS)  # CDC started meanwhile (another tab)
    assert kw["cdc_coverage_open"]() is False
    # A covered table alone is no addition: no guard, no closure.
    calls.clear()
    s.state.job_id = "job-1"
    s.state.cdc_ops_window_start = None
    retry(["app.a"])
    ((_prior, tables, kw),) = calls
    assert tables == ["app.a"] and kw["cdc_additions"] == () and kw["cdc_coverage_open"] is None


def test_a_second_attempt_at_an_uncovered_table_is_guarded_again(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the guard ran only for names outside the RECORD, so app.c (in
    # the record after a first, refused attempt) loaded with no prerequisite checks.
    record = _baselined(_job("FAILED", [_c("app.a"), _c("app.b"), _c("app.c", "FAILED")],
                             counts={"app.a": 1, "app.b": 1}), "app.c")
    s, retry, calls = _retry_screen(monkeypatch, job=record,
                                    selected=["app.a", "app.b", "app.c"])
    retry(["app.c"])
    assert calls == []
    assert any(m.startswith("Re-run the prerequisite checks — app.c was added")
               for m, _k in s.ui.notified)


def test_a_mysql_addition_without_a_baseline_is_refused_at_the_click(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: it was submitted, and only the engine refused it.
    s, retry, calls = _retry_screen(monkeypatch, job=_loaded(),
                                    selected=["app.a", "app.b", "app.c"],
                                    report=("app.a", "app.b", "app.c"))
    retry(["app.c"])
    assert calls == []
    assert any(m == (
        "app.c cannot be added to CDC by Retry: this Full Load recorded no start-point "
        "definition for it, so CDC could not decode its binary-log changes since then. "
        "Re-run all tables loads every selected table from a fresh start point, or untick it "
        "in the table list.") for m, _k in s.ui.notified)
    # The step: Retry unfinished tables carries the same text (it is disabled with it: the
    # only unfinished table cannot be added), Re-run all tables keeps its own tooltip (it is
    # the remedy, enabled).
    _rerender(s)
    assert any(t.startswith("app.c cannot be added to CDC by Retry") for t in s.ui.tooltips)
    assert any(t.startswith("Re-runs ALL selected tables from a fresh snapshot")
               for t in s.ui.tooltips)
    from dsql_migrator.ui.data_migration import CDC_NOT_ADDABLE_REASON

    assert CDC_NOT_ADDABLE_REASON.startswith("Cannot be added by Retry")


def test_the_addition_block_reasons(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import cdc_addition_block_reason, cdc_delete_eta

    jobs = _Jobs({"job-1": _loaded()})
    assert cdc_addition_block_reason(_fc(["app.c"]), jobs, ["app.c"]) is None
    started = _fc(["app.c"])
    started.set_cdc_ops_window_start(_TS)
    assert cdc_addition_block_reason(started, jobs, ["app.c"]) == (
        "app.c was selected after the Full Load, and CDC has started for this migration "
        "since, so CDC cannot capture it from the Full Load's start point and it is not "
        "loaded. To migrate it too, use Start over.")
    assert cdc_addition_block_reason(started, jobs, ["app.c", "app.d"]).startswith(
        "app.c, app.d were selected after the Full Load")
    unread = _fc(["app.c"], phase=None)
    assert cdc_addition_block_reason(unread, jobs, ["app.c"]) == (
        "Retry unfinished tables cannot load app.c yet: the CDC state has not been read, so "
        "the tool cannot confirm that CDC never started for this migration. This usually "
        "clears within seconds; if it does not, open the CDC step and use Re-check CDC state.")
    unreadable = _fc(["app.c"], phase=None)
    unreadable.set_cdc_probe_error("AccessDenied")
    assert cdc_addition_block_reason(unreadable, jobs, ["app.c"]) == (
        "Retry unfinished tables cannot load app.c: the CDC state could not be read, so the "
        "tool cannot confirm that CDC never started for this migration. The CDC step shows "
        "the error: fix the AWS access, use Re-check CDC state there, then retry.")
    deleting = _fc(["app.c"], phase="unstable", status="DELETE_IN_PROGRESS")
    assert cdc_addition_block_reason(deleting, jobs, ["app.c"]) == (
        "Retry unfinished tables cannot load app.c while the CDC infrastructure is being "
        "deleted. Wait for the deletion to finish, then retry.")
    assert cdc_delete_eta(deleting)  # (the wait elsewhere names the ETA)
    # An infrastructure create in flight and a failed create touch no source object.
    creating = _fc(["app.c"], phase="unstable", status="CREATE_IN_PROGRESS")
    assert cdc_addition_block_reason(creating, jobs, ["app.c"]) is None
    failed = _fc(["app.c"], phase="unstable", status="ROLLBACK_COMPLETE")
    assert cdc_addition_block_reason(failed, jobs, ["app.c"]) is None


_ABC = ["app.a", "app.b", "app.c"]


def test_a_postgresql_addition_is_submitted_and_retry_stays_enabled(monkeypatch) -> None:
    # PostgreSQL records no MySQL start-point baseline, by design: its additions are always
    # addable at the click (the engine guard decides), never refused with the binary-log text.
    s, retry, calls = _retry_screen(monkeypatch, job=_with(_loaded(), slot="dsqlmig_s"),
                                    selected=_ABC, report=_ABC, pg=True)
    _rerender(s)
    assert not any("cannot be added to CDC by Retry" in t for t in s.ui.tooltips)
    assert any("it first adds it to CDC's publication on the source" in t for t in s.ui.texts)
    assert any(b.text.startswith("Retry unfinished tables (1)") for b in s.ui.buttons)
    retry(["app.c"])
    assert not any("cannot be added to CDC by Retry" in m for m, _k in s.ui.notified)
    ((_prior, tables, kw),) = calls
    assert tables == ["app.c"] and kw["cdc_additions"] == ("app.c",)
    assert kw["cdc_coverage_open"]() is True


def test_the_render_time_guard_names_the_checks_for_a_pending_addition(monkeypatch) -> None:
    # No report covers app.c (a restored session): the scope notice leads with the checks,
    # and both Retry unfinished tables and Re-run all tables are held with them.
    from dsql_migrator.ui.data_migration._models import added_since_run_reason

    s, _retry, _calls = _retry_screen(monkeypatch, job=_baselined(_loaded(), "app.c"),
                                      selected=_ABC)
    _rerender(s)
    guard = added_since_run_reason(["app.c"])
    assert any(f"First: {guard} Then Retry unfinished tables" in t for t in s.ui.texts)
    assert s.ui.tooltips.count(guard) >= 2


def test_a_stack_carrying_a_started_set_closes_the_addition_route(monkeypatch) -> None:
    # An UPDATE_* (or a failed delete) stack closes G16's narrowing without durable "CDC
    # started" evidence; the engine re-checks only that durable rule, so the click is the one
    # place this state refuses an addition.
    from dsql_migrator.ui.data_migration import cdc_addition_block_reason
    from dsql_migrator.ui.data_migration._cdc_monitoring import _cdc_started_for_migration

    jobs = _Jobs({"job-1": _loaded()})
    closed = ("app.c was selected after the Full Load, and CDC has started for this migration "
              "since, so CDC cannot capture it from the Full Load's start point and it is not "
              "loaded. To migrate it too, use Start over.")
    for status in ("UPDATE_IN_PROGRESS", "UPDATE_ROLLBACK_COMPLETE", "DELETE_FAILED"):
        state = _fc(["app.c"], phase="unstable", status=status)
        assert not _cdc_started_for_migration(state, jobs)
        assert cdc_addition_block_reason(state, jobs, ["app.c"]) == closed, status
    # At the click: the record holds app.c (FAILED, an earlier refused attempt), so the
    # retry reaches the addition guard -- and submits nothing.
    record = _baselined(_job("FAILED", [_c("app.a"), _c("app.b"), _c("app.c", "FAILED")],
                             counts={"app.a": 1, "app.b": 1}), "app.c")
    s, retry, calls = _retry_screen(monkeypatch, job=record, selected=_ABC, report=_ABC)
    s.state.set_cdc_stack_phase("unstable", status="UPDATE_IN_PROGRESS")
    retry(["app.c"])
    assert calls == [] and any(m == closed for m, _k in s.ui.notified)


def test_a_full_load_only_retry_passes_no_addition(monkeypatch) -> None:
    from tests.test_g12_table_set_after_load import _retry_route

    s, retry, calls = _retry_route(monkeypatch, job=_loaded(),
                                   selected=["app.a", "app.b", "app.c"],
                                   report_tables=("app.a", "app.b", "app.c"))
    retry(["app.c"])
    ((_prior, tables, kw),) = calls
    assert tables == ["app.c"] and kw["cdc_additions"] == () and kw["cdc_coverage_open"] is None


# ---------------------------------------------------------------------------
# 7. The Retry dialog: an addition loads only into an EMPTY Aurora DSQL table
# ---------------------------------------------------------------------------


class _G17Session:
    target_config = object()
    aws_profile = None
    source_verified = True
    target_verified = True
    source_config = SimpleNamespace(source_type=SourceType.MYSQL)

    def connection_ready(self):
        return True


class _G17PgSession(_G17Session):
    source_config = SimpleNamespace(source_type=SourceType.POSTGRES)


def _open_retry_dialog(monkeypatch, *, state, job, rows, tables, click="Retry unfinished",
                       session=None, ui=None):
    import asyncio

    import nicegui.context as ctxobj
    import nicegui.run as nrun

    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    from tests.test_start_dialog_overlay import _DialogUi
    from tests.test_ui_data_migration import _StubConnector

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(type(ctxobj), "client", property(lambda _self: _Client()),
                        raising=False)

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(fl, "DsqlConnector", lambda *a, **k: _StubConnector())
    monkeypatch.setattr(fl, "tables_with_rows", lambda names, **_k: [n for n in names
                                                                       if n in rows])
    monkeypatch.setattr(fl, "target_primary_keys", lambda names, **k: {n: ["id"] for n in names})
    monkeypatch.setattr(fl, "tables_present", lambda names, **k: list(names))
    monkeypatch.setattr(fl, "cdc_streaming_started", lambda *a, **k: False)
    ui = ui if ui is not None else _DialogUi()
    picked: list = []

    def _retry_tables(names):
        picked.append((list(names), state.take_run_choice()[0]))

    jobs = _Jobs({"job-1": job})
    dm._render_full_load_step(
        ui, state, jobs, session or _G17Session(), job=job, status=StepStatus.FAILED,
        selected_names=list(tables), retry_tables=_retry_tables, guard_reason=None,
        start_full_load=lambda: None, retry_failed_load=lambda *a: None,
        reload_table=lambda *a: None, accept_quarantine_and_continue=lambda *a: None,
        stop_full_load=lambda *a: None, refresh=lambda *a: None,
        schema_recreate_candidates=lambda: [],
    )
    (handler,) = [h for (label, h) in ui.handlers if label.startswith(click)]
    asyncio.run(handler())
    return ui, picked


def test_a_pending_addition_holding_rows_is_forced_to_drop_and_reload(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: Append was offered (and preselected) over rows that may predate
    # the changes CDC captures for the table -- a permanent silent divergence.
    from tests.test_start_dialog_overlay import _body

    state = _fc(["app.a", "app.b", "app.c"])
    ui, picked = _open_retry_dialog(monkeypatch, state=state, job=_three("DONE"),
                                    rows=("app.c",), tables=["app.a", "app.b", "app.c"])
    assert state.exclusion_forced_replace.keys() == {"app.c"}
    assert ui.start().text == "Drop 1 table, recreate and load" and ui.start().color == "negative"
    assert ui.radios == []  # the only populated table is forced: no choice to draw
    body = _body(ui)
    assert "1 table will be dropped and reloaded" in body
    # True on both engines (this session is MySQL, where CDC DOES replay every change since
    # the start point): the rows may predate what CDC captures, and an append keeps them.
    assert ("app.c: selected after the Full Load, and Aurora DSQL already holds rows in it "
            "that may predate the changes CDC captures for it (an append keeps every row "
            "already there), so it is dropped and reloaded.") in body
    assert "not in CDC's stream" not in body
    from tests.test_start_dialog_overlay import _confirm

    _confirm(ui)
    ((names, replace),) = picked
    assert names == ["app.c"] and "app.c" in replace


def test_a_postgresql_addition_says_its_load_joins_the_publication_without_a_gap(
        monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the row said its load checks "that CDC can replay its changes
    # since then" -- on PostgreSQL CDC streams it only from its publication join.
    from dsql_migrator.ui.data_migration import (
        SCOPE_ADDED_REASON_FL_CDC,
        SCOPE_ADDED_REASON_PG_SUFFIX,
    )
    from tests.test_start_dialog_overlay import _body

    state = _fc(["app.a", "app.b", "app.c"])
    ui, _picked = _open_retry_dialog(monkeypatch, state=state,
                                     job=_with(_loaded(), slot="dsqlmig_s"), rows=(),
                                     tables=["app.a", "app.b", "app.c"],
                                     session=_G17PgSession())
    assert SCOPE_ADDED_REASON_FL_CDC + SCOPE_ADDED_REASON_PG_SUFFIX in _body(ui)
    assert "replay" not in SCOPE_ADDED_REASON_FL_CDC
    assert "checks that CDC can capture it with no gap" in SCOPE_ADDED_REASON_FL_CDC


class _TrackingDialogUi:
    """A dialog double whose ``refreshable`` re-renders into its own segment on refresh
    (NiceGUI semantics: a refresh REPLACES that region), so what is VISIBLE after a tick can
    be read back. Built on the repo's ``_DialogUi``."""

    def __new__(cls):
        from tests.test_start_dialog_overlay import _DialogUi

        class _Tracking(_DialogUi):
            def __init__(self):
                super().__init__()
                self._sink = None
                self.segments: list = []

            def _record(self, text):
                if text:
                    (self._sink if self._sink is not None else self.texts).append(str(text))
                return self._El(self)

            def refreshable(self, fn):
                segment: list = []
                self.segments.append(segment)

                def _render(*a, **k):
                    previous, self._sink = self._sink, segment
                    segment.clear()
                    try:
                        return fn(*a, **k)
                    finally:
                        self._sink = previous

                _render.refresh = _render
                return _render

            def timer(self, _interval, callback=None, *_a, once=False, **_k):
                if once and callable(callback):
                    callback()
                return self._El(self)

            def visible(self) -> str:
                return " ".join(self.texts + [t for seg in self.segments for t in seg])

        return _Tracking()


def test_the_forced_addition_notice_follows_the_retry_checklist(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: with no column exclusions (no overlay), the notice announcing
    # app.c's DROP stayed after app.c was unticked -- above a primary "Confirm and start"
    # that no longer drops anything.
    ui = _TrackingDialogUi()
    state = _fc(["app.a", "app.b", "app.c"])
    job = _with(_job("FAILED", [_c("app.a", "FAILED"), _c("app.b")],
                     counts={"app.a": 1, "app.b": 1}), fingerprints={"app.c": _fp()})
    before = len(ui.checkbox_handlers)
    _open_retry_dialog(monkeypatch, state=state, job=job, rows=("app.c",),
                       tables=["app.a", "app.b", "app.c"], ui=ui)
    app_a, app_c = ui.checkbox_handlers[before:]
    header = "1 table will be dropped and reloaded"
    assert header in ui.visible() and ui.start().text == "Drop 1 table, recreate and load"
    app_c(SimpleNamespace(value=False))
    assert ui.start().text == "Confirm and start" and ui.start().color == "primary"
    assert header not in ui.visible()
    assert "app.c: selected after the Full Load" not in ui.visible()
    app_c(SimpleNamespace(value=True))
    assert header in ui.visible() and ui.start().text == "Drop 1 table, recreate and load"
    app_a(SimpleNamespace(value=False))  # the other table: app.c is still dropped
    assert header in ui.visible()


def test_an_empty_addition_appends_and_a_covered_table_keeps_its_choice(monkeypatch) -> None:
    state = _fc(["app.a", "app.b", "app.c"])
    job = _job("FAILED", [_c("app.a"), _c("app.b", "FAILED")], counts={"app.a": 1, "app.b": 1})
    ui, _picked = _open_retry_dialog(monkeypatch, state=state, job=job, rows=("app.b",),
                                     tables=["app.a", "app.b", "app.c"])
    assert state.exclusion_forced_replace == {}  # app.c is empty: it appends
    (radio,) = ui.radios  # app.b (covered) holds rows: the operator chooses
    assert ui.start().text == "Append and load"
    # Cancel drops the forced set.
    state2 = _fc(["app.a", "app.b", "app.c"])
    ui2, _p2 = _open_retry_dialog(monkeypatch, state=state2, job=_three("DONE"),
                                  rows=("app.c",), tables=["app.a", "app.b", "app.c"])
    assert state2.exclusion_forced_replace.keys() == {"app.c"}
    ui2.dialogs[-1].close()
    assert state2.exclusion_forced_replace == {}


# ---------------------------------------------------------------------------
# 8. The engine: guards before any snapshot, refusals recorded, coverage written last
# ---------------------------------------------------------------------------


def _tbl(name):
    return TableDef(name=name, columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"])


class _GuardMigrator:
    """A fake migrator with the G17 guard hook (no ``_inputs``: replace set empty)."""

    def __init__(self, *, verdicts=None, rows=None, fail=(), quarantine=None, log=None,
                 hook=True):
        self.verdicts = dict(verdicts or {})
        self.rows = dict(rows or {})
        self.fail = set(fail)
        self.quarantine = dict(quarantine or {})
        self.log = log if log is not None else []
        self.guarded: list = []
        if not hook:
            self.guard_cdc_additions = None

    def capture_watermark(self, tables=None):  # never called by a retry
        raise AssertionError("a retry takes no new watermark")

    def guard_cdc_additions(self, names, *, watermark, replace_tables, heartbeat, cancelled):
        self.log.append("guard")
        self.guarded.append((list(names), set(replace_tables)))
        heartbeat()
        return {n: self.verdicts.get(n) for n in names}

    def migrate_table(self, table, *, on_rows=None, should_cancel=None):
        self.log.append(f"migrate {table.name}")
        if table.name in self.fail:
            raise RuntimeError(f"load failed for {table.name}")
        rows = self.rows.get(table.name, 0)
        if on_rows is not None and rows:
            on_rows(rows, 0)
        q = self.quarantine.get(table.name, 0)
        if q:
            from dsql_migrator.core.batched_import import QuarantineRecord
            from dsql_migrator.ui.data_migration._full_load_engine import TableLoadResult

            return TableLoadResult(rows_loaded=rows, rows_quarantined=q, quarantine_records=tuple(
                QuarantineRecord(table=table.name, primary_key=f"id={i}", error_code="54000",
                                 message="value too large") for i in range(q)))
        return rows


def _run_retry(monkeypatch, migrator, *, retry=("app.a", "app.c"), additions=("app.c",),
               coverage_open=lambda: True, prior=None, watermark=None, store=None,
               accept=False):
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    monkeypatch.setattr(engine, "pending_foreign_key_count", lambda *_a, **_k: 0)
    monkeypatch.setattr(engine, "_log_identity_sequence_sync", lambda *_a, **_k: {})
    lines: list = []
    real_log = engine.log_activity

    def _log(category, action, **kw):
        lines.append((action, kw))
        return real_log(category, action, **kw)

    monkeypatch.setattr(engine, "log_activity", _log)
    error_log = ErrorLogStore()
    watermark = watermark or _loaded().watermark
    prior = prior if prior is not None else [_c("app.a"), _c("app.b")]
    manager = JobManager(store=store) if store is not None else JobManager()
    job_id = manager.submit(lambda h: engine.run_full_load_retry(
        h, prior, [_tbl(n) for n in retry], migrator=migrator, error_log=error_log,
        watermark=watermark, cdc_additions=additions, cdc_coverage_open=coverage_open,
        accept_quarantined_rows=accept,
    ))
    assert manager.wait(job_id, timeout=10.0)
    return SimpleNamespace(job=manager.get_status(job_id), error=manager.get_error(job_id),
                           errors=error_log.records(job_id), lines=lines, manager=manager,
                           job_id=job_id)


def test_the_guard_runs_after_the_ledger_read_and_before_any_snapshot(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: no guard ran at all; a PostgreSQL snapshot opened before the
    # table was a publication member, losing its changes in between.
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    log: list = []
    monkeypatch.setattr(engine, "_open_run_ledger_view", lambda *a, **k: log.append("ledger"))
    monkeypatch.setattr(engine, "_predrop_blocking_foreign_keys",
                        lambda *a, **k: log.append("predrop fk"))
    monkeypatch.setattr(engine, "_predrop_dependent_views",
                        lambda *a, **k: log.append("predrop views"))
    migrator = _GuardMigrator(rows={"app.a": 1, "app.c": 4}, log=log)
    run = _run_retry(monkeypatch, migrator)
    assert run.job.status == "DONE"
    assert log[:4] == ["ledger", "guard", "predrop fk", "predrop views"]
    assert set(log[4:]) == {"migrate app.a", "migrate app.c"}
    # Every snapshot -- each reader's and the shared PostgreSQL anchor -- opens inside the
    # parallel phase, never in the retry's own body.
    tree = ast.parse(inspect.getsource(engine))
    callers = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for inner in ast.walk(node):
                if isinstance(inner, ast.Call) and getattr(inner.func, "id", None) == (
                        "_open_shared_snapshot"):
                    callers.add(node.name)
    assert callers == {"_migrate_tables_in_parallel"}


def test_a_refused_addition_is_recorded_failed_and_the_other_tables_still_load(
        monkeypatch) -> None:
    migrator = _GuardMigrator(verdicts={"app.c": "Not loaded: a fixed reason."},
                              rows={"app.a": 3})
    run = _run_retry(monkeypatch, migrator)
    by = {c.chunk_id: c for c in run.job.chunks}
    assert by["app.c"].status == "FAILED" and by["app.a"].status == "DONE"
    assert "migrate app.c" not in migrator.log and "migrate app.a" in migrator.log
    assert [r.message for r in run.errors if r.table == "app.c"] == ["Not loaded: a fixed reason."]
    assert any(a == "load table" and kw.get("target") == "app.c"
               and kw.get("detail") == "Not loaded: a fixed reason." for a, kw in run.lines)
    assert run.job.status == "FAILED" and "1 of 2" in (run.error or "")  # counted as failed
    assert run.job.watermark.cdc_added_tables == {}


def test_a_guarded_clean_load_extends_the_coverage_and_nothing_else_does(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the watermark was never extended, so CDC never captured app.c.
    clean = _run_retry(monkeypatch, _GuardMigrator(rows={"app.a": 3, "app.c": 9}))
    assert clean.job.status == "DONE"
    assert clean.job.watermark.cdc_added_tables == {"app.c": 9}
    assert set(clean.job.watermark.table_row_counts) == {"app.a", "app.b"}  # the original set
    assert any(a == "CDC capture extended" for a, _kw in clean.lines)
    quarantined = _run_retry(monkeypatch, _GuardMigrator(rows={"app.c": 9},
                                                         quarantine={"app.c": 1}), accept=True)
    assert quarantined.job.status == "DONE"  # the accepted gap completes the run...
    assert quarantined.job.watermark.cdc_added_tables == {}  # ...but never covers app.c
    failed = _run_retry(monkeypatch, _GuardMigrator(fail={"app.c"}))
    assert failed.job.watermark.cdc_added_tables == {}
    # A covered table in the retry is no addition: it is never guarded.
    covered = _GuardMigrator(rows={"app.a": 1})
    _run_retry(monkeypatch, covered, retry=("app.a",), additions=("app.a",))
    assert covered.guarded == []
    # The extension lands even when another table of the run failed.
    mixed = _run_retry(monkeypatch, _GuardMigrator(rows={"app.c": 2}, fail={"app.a"}))
    assert mixed.job.status == "FAILED"
    assert mixed.job.watermark.cdc_added_tables == {"app.c": 2}


def test_no_confirmation_that_cdc_never_started_refuses_every_addition(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import (
        CDC_STARTED,
        CDC_STATE_UNCONFIRMED,
        NO_GUARD,
    )

    for coverage_open, reason in ((lambda: False, CDC_STARTED), (None, CDC_STATE_UNCONFIRMED)):
        migrator = _GuardMigrator(rows={"app.a": 1, "app.c": 1})
        run = _run_retry(monkeypatch, migrator, coverage_open=coverage_open)
        assert migrator.guarded == []  # no source access before the refusal
        assert [r.message for r in run.errors if r.table == "app.c"] == [reason]
        assert {c.chunk_id: c.status for c in run.job.chunks}["app.c"] == "FAILED"
    plain = _run_retry(monkeypatch, _GuardMigrator(rows={"app.a": 1}, hook=False))
    assert [r.message for r in plain.errors if r.table == "app.c"] == [NO_GUARD]


def test_cdc_started_while_the_addition_loaded_is_recorded_failed(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: a start made mid-retry was followed by a coverage write the
    # running connector's table list never had -- a table the UI showed captured.
    from dsql_migrator.ui.data_migration._full_load_engine import CDC_STARTED_AFTER_LOAD

    answers = iter([True, False])
    run = _run_retry(monkeypatch, _GuardMigrator(rows={"app.c": 4}),
                     coverage_open=lambda: next(answers))
    by = {c.chunk_id: c for c in run.job.chunks}
    assert by["app.c"].status == "FAILED"
    assert [r.message for r in run.errors if r.table == "app.c"] == [CDC_STARTED_AFTER_LOAD]
    assert run.job.watermark.cdc_added_tables == {} and run.job.status == "FAILED"


def test_a_stop_during_the_guard_leaves_the_addition_pending(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import CDC_GUARD_STOPPED

    migrator = _GuardMigrator(verdicts={"app.c": CDC_GUARD_STOPPED})
    run = _run_retry(monkeypatch, migrator, retry=("app.c",))
    assert {c.chunk_id: c.status for c in run.job.chunks}["app.c"] == "PENDING"
    assert run.errors == [] and run.job.watermark.cdc_added_tables == {}


def test_the_save_carrying_the_terminal_status_already_holds_the_coverage(monkeypatch) -> None:
    from dsql_migrator.core.job_store import InMemoryJobStore

    saves: list = []

    class _Recording(InMemoryJobStore):
        def save(self, job, error):
            wm = job.watermark
            saves.append((job.status, dict(getattr(wm, "cdc_added_tables", {}) or {})))
            super().save(job, error)

    run = _run_retry(monkeypatch, _GuardMigrator(rows={"app.c": 6}), retry=("app.c",),
                     store=_Recording())
    assert run.job.status == "DONE"
    first_done = next(added for status, added in saves if status == "DONE")
    assert first_done == {"app.c": 6}


def test_a_refused_addition_is_never_dropped(monkeypatch) -> None:
    # FAILS WITHOUT THE REBIND: a refused table listed for Drop & reload was still dropped
    # by the recreate pre-pass (an empty table CDC would never fill).
    from dsql_migrator.core.models import SourceConnectionConfig, SourceInventory
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import DataMigrationInputs

    migrator = _GuardMigrator(verdicts={"app.c": "Not loaded: refused."}, rows={"app.a": 1})
    migrator._inputs = DataMigrationInputs(
        source_config=SourceConnectionConfig(host="h", database="app", username="u"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=[_tbl("app.a"), _tbl("app.c")]),
        replace_tables=frozenset({"app.a", "app.c"}),
    )
    migrator.bind_run_inputs = lambda inputs: setattr(migrator, "_inputs", inputs)
    _run_retry(monkeypatch, migrator)
    ((names, replaced),) = migrator.guarded
    assert names == ["app.c"] and replaced == {"app.a", "app.c"}
    assert migrator._inputs.replace_tables == frozenset({"app.a"})


def _never_covered(run, name="app.c", reason=None):
    """``name`` was refused (FAILED, ``reason`` its one error record), never copied, never
    covered."""
    by = {c.chunk_id: c for c in run.job.chunks}
    assert by[name].status == "FAILED"
    if reason is not None:
        assert [r.message for r in run.errors if r.table == name] == [reason]
    assert run.job.watermark.cdc_added_tables == {}


def test_a_guard_hook_that_raises_refuses_every_addition(monkeypatch) -> None:
    # FAIL CLOSED: an unexpected failure inside the guard is never a pass.
    from dsql_migrator.ui.data_migration._full_load_engine import CDC_GUARD_FAILED

    class _Raising(_GuardMigrator):
        def guard_cdc_additions(self, names, **_kw):
            self.log.append("guard")
            raise ValueError("boom")

    migrator = _Raising(rows={"app.a": 1, "app.c": 4})
    run = _run_retry(monkeypatch, migrator)
    _never_covered(run, reason=CDC_GUARD_FAILED.format(cls="ValueError"))
    assert "migrate app.c" not in migrator.log and "migrate app.a" in migrator.log


def test_a_cdc_state_that_cannot_be_read_at_the_guard_refuses_before_any_access(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import CDC_STATE_UNCONFIRMED

    def _unreadable():
        raise RuntimeError("job store unreadable")

    migrator = _GuardMigrator(rows={"app.a": 1, "app.c": 4})
    run = _run_retry(monkeypatch, migrator, coverage_open=_unreadable)
    assert migrator.guarded == [] and "migrate app.c" not in migrator.log
    _never_covered(run, reason=CDC_STATE_UNCONFIRMED)


def test_a_cdc_state_that_cannot_be_read_at_the_coverage_write_never_covers(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import (
        CDC_STATE_UNCONFIRMED_AFTER_LOAD,
    )

    answers = {"n": 0}

    def _open():
        answers["n"] += 1
        if answers["n"] > 1:
            raise RuntimeError("job store unreadable")
        return True

    run = _run_retry(monkeypatch, _GuardMigrator(rows={"app.c": 4}), coverage_open=_open)
    _never_covered(run, reason=CDC_STATE_UNCONFIRMED_AFTER_LOAD)
    assert run.job.status == "FAILED"


def test_a_second_guarded_retry_keeps_the_earlier_additions(monkeypatch) -> None:
    # A retry starting from a watermark that already covers app.c must keep it covered.
    covered = _with(_loaded(), added={"app.c": 3}).watermark
    run = _run_retry(monkeypatch, _GuardMigrator(rows={"app.d": 5}), retry=("app.d",),
                     additions=("app.d",), watermark=covered,
                     prior=[_c("app.a"), _c("app.b"), _c("app.c")])
    assert run.job.status == "DONE"
    assert run.job.watermark.cdc_added_tables == {"app.c": 3, "app.d": 5}


# ---------------------------------------------------------------------------
# 9. BatchedTableMigrator.guard_cdc_additions: the target, MySQL, PostgreSQL
# ---------------------------------------------------------------------------


class _Res:
    def __init__(self, rows):
        self._rows = list(rows)

    def first(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _PgSource:
    """One scripted PostgreSQL source behind both guard engines; every statement is logged
    as ``(engine, normalized sql)``. ``members`` is the publication's table list.
    ``usability`` answers the replica-identity usability read per table --
    ``(relkind, relreplident, usable, unusable partitions, sample)``, else a plain DEFAULT
    table with a valid primary key -- and ``usability_error`` makes it raise."""

    def __init__(self, *, members=("app.a", "app.b"), pub=True, all_dml=True, slot_ok=True,
                 via_root=True, add_errors=None, xmin_ready=(True,), rekey_reads=None,
                 objects_error=None, poll_error=None, usability=None, usability_error=None):
        self.log: list = []
        self.usability = dict(usability or {})
        self.usability_error = usability_error
        self.members = list(members)
        self.pub, self.all_dml, self.slot_ok, self.via_root = pub, all_dml, slot_ok, via_root
        self.add_errors = dict(add_errors or {})
        self.xmin_ready = list(xmin_ready)
        self.rekey_reads = rekey_reads
        self.objects_error = objects_error
        self.poll_error = poll_error
        self.polls = 0

    def engine(self, kind):
        source = self

        class _Conn:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, statement, params=None):
                return source.execute(kind, str(statement), params or {})

        class _Engine:
            disposed = False

            def connect(self):
                return _Conn()

            def dispose(self):
                _Engine.disposed = True
                source.log.append((kind, "DISPOSE"))

        return _Engine()

    def execute(self, kind, sql, params):
        import re

        up = " ".join(sql.upper().split())
        self.log.append((kind, up))
        if "AS IDENTITY_USABLE" in up:
            if self.usability_error is not None:
                raise self.usability_error
            rows = {n: self.usability.get(n, ("r", "d", True, 0, []))
                    for n in params.get("names", [])}
            # ``None``: the table is not found on the source (a dropped table, a view).
            return _Res([(n, *row) for n, row in rows.items() if row is not None])
        if "AS PUB_N" in up:
            if self.objects_error is not None:
                raise self.objects_error
            return _Res([(1 if self.pub else 0, self.all_dml, 1 if self.slot_ok else 0,
                          1 if self.slot_ok else 0)])
        if "FROM PG_PUBLICATION_TABLES" in up:
            return _Res([(t,) for t in self.members])
        if "PUBALLTABLES" in up:
            return _Res([(False, self.via_root)] if self.pub else [])
        if "PUBVIAROOT" in up:
            return _Res([(self.via_root,)] if self.pub else [])
        if "RELREPLIDENT::TEXT AS IDENTITY" in up:
            return _Res(self.rekey_reads or [])
        if "RELREPLIDENT" in up:
            return _Res([(n, "d") for n in params.get("names", [])])
        if up.startswith("ALTER PUBLICATION") and " ADD TABLE " in up:
            schema, table = re.search(r'ADD TABLE "([^"]+)"\."([^"]+)"', sql).groups()
            name = f"{schema}.{table}"
            if name in self.add_errors:
                raise self.add_errors[name]
            self.members.append(name)
            return _Res([])
        if up.startswith("ALTER PUBLICATION"):
            self.via_root = True
            return _Res([])
        if up.startswith("SELECT PG_CURRENT_XACT_ID"):
            return _Res([("9001",)])
        if "PG_SNAPSHOT_XMIN" in up:
            self.polls += 1
            if self.poll_error is not None:
                raise self.poll_error
            return _Res([(self.xmin_ready[min(self.polls - 1, len(self.xmin_ready) - 1)],)])
        return _Res([])

    def writes(self):
        return [sql for kind, sql in self.log if kind == "write"
                and (sql.startswith("ALTER") or sql.startswith("SELECT PG_CURRENT_XACT_ID"))]


def _pg_migrator(monkeypatch, source, *, facts=None, rekey=None):
    from dsql_migrator.core import cdc as core_cdc
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.core.models import SourceConnectionConfig, SourceInventory
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    monkeypatch.setattr(engine, "make_source_engine_factory",
                        lambda *_a, **_k: (lambda _cfg: source.engine("read")))
    monkeypatch.setattr(cdc_pg_slot, "build_pg_source_write_engine",
                        lambda *_a, **_k: source.engine("write"))
    monkeypatch.setattr(core_cdc, "composite_key_columns_for_cdc",
                        lambda *_a, **_k: dict(rekey or {}))
    monkeypatch.setattr(engine, "CDC_ADDITION_WAIT_POLL_SECONDS", 0.0)
    inputs = engine.DataMigrationInputs(
        source_config=SourceConnectionConfig(source_type=SourceType.POSTGRES, host="h",
                                             port=5432, database="app", username="u"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=[_tbl("app.a"), _tbl("app.c"), _tbl("app.d")]),
    )
    probes: list = []

    def _facts(_inputs, names, columns, *, probe_rows, primary_key_tables=frozenset()):
        probes.append((list(names), probe_rows))
        from dsql_migrator.core.target_introspector import TableOverlayFacts

        given = facts or {}
        return {n: given.get(n, TableOverlayFacts(True, False, None, {})) for n in names}

    migrator = engine.BatchedTableMigrator(
        inputs, exporter=object(), watermark_capturer=object(), table_recreator=object(),
        overlay_facts_reader=_facts,
    )
    migrator.probes = probes
    return migrator


def _pg_watermark(slot="dsqlmig_s"):
    return _with(_loaded(), slot=slot, publication="dsqlmig_pub_s").watermark if slot else (
        _loaded().watermark)


def _guard(migrator, names, watermark, *, replace=frozenset(), cancelled=None):
    beats: list = []
    out = migrator.guard_cdc_additions(names, watermark=watermark, replace_tables=replace,
                                       heartbeat=lambda: beats.append(1),
                                       cancelled=cancelled or (lambda: False))
    return out, beats


def test_the_target_must_exist_and_be_empty_or_replaced(monkeypatch) -> None:
    from dsql_migrator.core.target_introspector import TableOverlayFacts as T
    from dsql_migrator.ui.data_migration._full_load_engine import (
        TARGET_HAS_ROWS,
        TARGET_MISSING,
        TARGET_UNKNOWN,
    )

    source = _PgSource()
    migrator = _pg_migrator(monkeypatch, source, facts={
        "app.c": T(True, True, None, {}), "app.d": T(False, False, None, {})})
    out, _beats = _guard(migrator, ["app.c", "app.d"], _pg_watermark())
    assert out == {"app.c": TARGET_HAS_ROWS, "app.d": TARGET_MISSING}
    assert source.log == []  # a target refusal issues no source statement
    # Engine-neutral: on MySQL CDC does replay the changes since the start point, so the
    # reason is that the rows may be OLDER than what CDC captures, not that it captures none.
    assert "may predate the changes CDC captures for it" in TARGET_HAS_ROWS
    assert "no record of the changes" not in TARGET_HAS_ROWS
    assert migrator.probes == [(["app.c", "app.d"], True)]
    unknown = _pg_migrator(monkeypatch, _PgSource(), facts={"app.c": None})
    assert _guard(unknown, ["app.c"], _pg_watermark())[0] == {"app.c": TARGET_UNKNOWN}
    # A replaced (Drop & reload) addition is not probed: the load recreates it.
    replaced = _pg_migrator(monkeypatch, _PgSource())
    out, _beats = _guard(replaced, ["app.c"], _pg_watermark(), replace={"app.c"})
    assert out == {"app.c": None} and replaced.probes == []


def test_the_postgresql_guard_adds_then_waits_before_any_copy(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: nothing added the table to CDC's publication, so pgoutput never
    # decoded its changes -- lost silently between its copy and Start CDC.
    source = _PgSource(via_root=False, xmin_ready=(False, False, True))
    migrator = _pg_migrator(monkeypatch, source, rekey={"app.c": ["tenant", "id"]})
    source.rekey_reads = [("app.c", "f", ["id"], 0, [])]
    out, beats = _guard(migrator, ["app.c", "app.d"], _pg_watermark())
    assert out == {"app.c": None, "app.d": None}
    sql = [s for _kind, s in source.log if s != "DISPOSE"]
    kinds = [kind for kind, s in source.log if s != "DISPOSE"]
    identity = next(i for i, s in enumerate(sql) if "RELREPLIDENT::TEXT AS IDENTITY" in s)
    objects = next(i for i, s in enumerate(sql) if "AS PUB_N" in s)
    repair = next(i for i, s in enumerate(sql) if s.endswith("PUBLISH_VIA_PARTITION_ROOT = TRUE)"))
    adds = [i for i, s in enumerate(sql) if " ADD TABLE " in s]
    xid = next(i for i, s in enumerate(sql) if s.startswith("SELECT PG_CURRENT_XACT_ID"))
    polls = [i for i, s in enumerate(sql) if "PG_SNAPSHOT_XMIN" in s]
    assert kinds[identity] == "read"  # the identity is read through the read-only engine
    assert identity < objects < repair < adds[0] < adds[1] < xid < polls[0]
    assert len(polls) == 3 and len(beats) >= 3  # a heartbeat every poll
    assert source.writes() == [
        'ALTER PUBLICATION "DSQLMIG_PUB_S" SET (PUBLISH_VIA_PARTITION_ROOT = TRUE)',
        'ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE "APP"."C"',
        'ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE "APP"."D"',
        "SELECT PG_CURRENT_XACT_ID()::TEXT",
    ]
    assert not any(s.startswith("ALTER TABLE") for s in sql)  # never sets REPLICA IDENTITY
    assert ("read", "DISPOSE") in source.log and ("write", "DISPOSE") in source.log
    # The wait polls never go through the write path's allowlist (they are reads).
    assert all("PG_SNAPSHOT_XMIN" not in w for w in source.writes())


def test_an_existing_member_still_waits(monkeypatch) -> None:
    source = _PgSource(members=("app.a", "app.b", "app.c"))
    out, _beats = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": None}
    assert source.writes() == ["SELECT PG_CURRENT_XACT_ID()::TEXT"]  # no ADD, still the wait


def test_postgresql_refusals_are_isolated_and_fail_closed(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import (
        PG_ADD_LOCK,
        PG_ADD_OWNER,
        PG_DML,
        PG_IDENTITY_NOT_FULL,
        PG_IDENTITY_UNKNOWN,
        PG_OBJECTS_GONE,
        PG_OBJECTS_UNREADABLE,
    )

    class _Denied(Exception):
        sqlstate = "42501"

    class _Lock(Exception):
        sqlstate = "55P03"

    owner = _PgSource(add_errors={"app.c": _Denied("must be owner")})
    out, _b = _guard(_pg_migrator(monkeypatch, owner), ["app.c", "app.d"], _pg_watermark())
    assert out == {"app.c": PG_ADD_OWNER.format(pub="dsqlmig_pub_s"), "app.d": None}
    assert 'ADD TABLE "APP"."D"' in " ".join(owner.writes())  # app.d still added
    lock = _PgSource(add_errors={"app.c": _Lock("lock timeout")})
    assert _guard(_pg_migrator(monkeypatch, lock), ["app.c"], _pg_watermark())[0] == {
        "app.c": PG_ADD_LOCK.format(pub="dsqlmig_pub_s")}
    for gone in (_PgSource(pub=False), _PgSource(slot_ok=False)):
        out, _b = _guard(_pg_migrator(monkeypatch, gone), ["app.c"], _pg_watermark())
        assert out == {"app.c": PG_OBJECTS_GONE.format(slot="dsqlmig_s", pub="dsqlmig_pub_s")}
        assert gone.writes() == []
    unreadable = _PgSource(objects_error=RuntimeError("boom"))
    assert _guard(_pg_migrator(monkeypatch, unreadable), ["app.c"], _pg_watermark())[0] == {
        "app.c": PG_OBJECTS_UNREADABLE.format(cls="RuntimeError")}
    insert_only = _PgSource(all_dml=False)
    assert _guard(_pg_migrator(monkeypatch, insert_only), ["app.c"], _pg_watermark())[0] == {
        "app.c": PG_DML.format(pub="dsqlmig_pub_s")}
    # A re-keyed table that is not REPLICA IDENTITY FULL (or whose identity is unknown) is
    # refused -- the guard never ALTERs the table to fix it.
    weak = _PgSource(rekey_reads=[("app.c", "d", ["id"], 0, [])])
    out, _b = _guard(_pg_migrator(monkeypatch, weak, rekey={"app.c": ["tenant", "id"]}),
                     ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_IDENTITY_NOT_FULL} and weak.writes() == []
    missing = _PgSource(rekey_reads=[])
    out, _b = _guard(_pg_migrator(monkeypatch, missing, rekey={"app.c": ["tenant", "id"]}),
                     ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_IDENTITY_UNKNOWN}
    assert not any(s.startswith("ALTER TABLE") for _k, s in weak.log + missing.log)


def _adds(source):
    return [s for s in source.writes() if " ADD TABLE " in s]


def test_postgresql_source_failures_refuse_every_addition_fail_closed(monkeypatch) -> None:
    # FAIL CLOSED, each with its fixed reason: a write engine that cannot be built or cannot
    # connect, a target that cannot be read, a re-keyed identity that cannot be read.
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.ui.data_migration._full_load_engine import (
        PG_IDENTITY_UNKNOWN,
        PG_OBJECTS_UNREADABLE,
        TARGET_UNKNOWN,
    )

    class OperationalError(Exception):
        pass

    class _NoConnect(_PgSource):
        def engine(self, kind):
            real = super().engine(kind)
            if kind == "write":
                def _connect():
                    raise OperationalError("could not connect")
                real.connect = _connect
            return real

    no_connect = _NoConnect()
    out, _b = _guard(_pg_migrator(monkeypatch, no_connect), ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_OBJECTS_UNREADABLE.format(cls="OperationalError")}
    assert no_connect.writes() == []
    built = _PgSource()
    migrator = _pg_migrator(monkeypatch, built)

    def _no_engine(*_a, **_k):
        raise ValueError("no token")

    monkeypatch.setattr(cdc_pg_slot, "build_pg_source_write_engine", _no_engine)
    assert _guard(migrator, ["app.c"], _pg_watermark())[0] == {
        "app.c": PG_OBJECTS_UNREADABLE.format(cls="ValueError")}
    assert built.writes() == []
    target = _PgSource()
    blind = _pg_migrator(monkeypatch, target)

    def _unreachable(*_a, **_k):
        raise RuntimeError("target unreachable")

    blind.overlay_facts = _unreachable
    assert _guard(blind, ["app.c"], _pg_watermark())[0] == {"app.c": TARGET_UNKNOWN}
    assert target.log == []  # never "empty": no source statement either

    class _NoRekeyRead(_PgSource):
        def execute(self, kind, sql, params):
            if "RELREPLIDENT::TEXT AS IDENTITY" in " ".join(sql.upper().split()):
                raise RuntimeError("read timed out")
            return super().execute(kind, sql, params)

    rekeyed = _NoRekeyRead()
    out, _b = _guard(_pg_migrator(monkeypatch, rekeyed, rekey={"app.c": ["tenant", "id"]}),
                     ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_IDENTITY_UNKNOWN} and rekeyed.writes() == []


def test_a_failed_publication_repair_refuses_every_table(monkeypatch) -> None:
    # A publication left at publish_via_partition_root=false would stream a partitioned
    # addition under its leaf names, which CDC filters out -- never added on top of that.
    from dsql_migrator.ui.data_migration._full_load_engine import PG_ADD_OWNER, PG_CONNECT

    class _Denied(Exception):
        sqlstate = "42501"

    class _Lost(Exception):
        sqlstate = "08006"

    for error, expected in ((_Denied("must be owner of publication"),
                             PG_ADD_OWNER.format(pub="dsqlmig_pub_s")),
                            (_Lost("server closed the connection"),
                             PG_CONNECT.format(pub="dsqlmig_pub_s", cls="_Lost"))):
        class _NoRepair(_PgSource):
            def execute(self, kind, sql, params, _error=error):
                up = " ".join(sql.upper().split())
                if up.startswith("ALTER PUBLICATION") and "PUBLISH_VIA_PARTITION_ROOT" in up:
                    self.log.append((kind, up))
                    raise _error
                return super().execute(kind, sql, params)

        source = _NoRepair(via_root=False)
        out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c", "app.d"],
                         _pg_watermark())
        assert out == {"app.c": expected, "app.d": expected}
        assert _adds(source) == []


def test_a_publication_dropped_before_the_add_refuses(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import PG_OBJECTS_GONE

    class _DroppedAfterRead(_PgSource):
        def execute(self, kind, sql, params):
            result = super().execute(kind, sql, params)
            if "AS PUB_N" in " ".join(sql.upper().split()):
                self.pub = False  # Delete CDC infrastructure raced the guard
            return result

    source = _DroppedAfterRead()
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_OBJECTS_GONE.format(slot="dsqlmig_s", pub="dsqlmig_pub_s")}
    assert source.writes() == []


def test_an_unknown_add_failure_and_a_lost_connection_are_refused(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import PG_ADD_OTHER, PG_CONNECT

    class _Odd(Exception):
        sqlstate = "XX000"

    class _Lost(Exception):
        sqlstate = "08006"

    odd = _PgSource(add_errors={"app.c": _Odd("internal error")})
    out, _b = _guard(_pg_migrator(monkeypatch, odd), ["app.c", "app.d"], _pg_watermark())
    assert out == {"app.c": PG_ADD_OTHER.format(pub="dsqlmig_pub_s", cls="_Odd", code="XX000"),
                   "app.d": None}
    # A lost connection refuses every table not added yet -- app.e is never attempted.
    lost = _PgSource(add_errors={"app.d": _Lost("server closed the connection")})
    out, _b = _guard(_pg_migrator(monkeypatch, lost), ["app.c", "app.d", "app.e"],
                     _pg_watermark())
    refused = PG_CONNECT.format(pub="dsqlmig_pub_s", cls="_Lost")
    assert out == {"app.c": None, "app.d": refused, "app.e": refused}
    assert _adds(lost) == ['ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE "APP"."C"',
                           'ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE "APP"."D"']  # the lost one


def test_an_unexpected_failure_after_a_write_refuses_what_is_not_graded(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import CDC_GUARD_FAILED

    source = _PgSource()
    migrator = _pg_migrator(monkeypatch, source)
    beats = {"n": 0}

    def _heartbeat():
        beats["n"] += 1
        if beats["n"] == 3:  # after the target and identity reads: right after app.c's ADD
            raise RuntimeError("heartbeat store gone")

    out = migrator.guard_cdc_additions(["app.c", "app.d"], watermark=_pg_watermark(),
                                       replace_tables=frozenset(), heartbeat=_heartbeat,
                                       cancelled=lambda: False)
    assert out == {n: CDC_GUARD_FAILED.format(cls="RuntimeError") for n in ("app.c", "app.d")}


_NO_PK = ("r", "d", False, 0, [])


@pytest.mark.parametrize("usability, cause", [
    (_NO_PK, "REPLICA IDENTITY DEFAULT, but no valid primary key"),
    (("r", "i", False, 0, []),
     "REPLICA IDENTITY USING INDEX, but that index is gone or not valid"),
    (("r", "n", False, 0, []), "REPLICA IDENTITY NOTHING"),
    (("p", "f", True, 1, ["app.c_2026q1"]), "1 of its partitions has none: app.c_2026q1"),
    (("p", "d", False, 7, ["app.c_1", "app.c_2", "app.c_3", "app.c_4", "app.c_5"]),
     "7 of its partitions have none: app.c_1, app.c_2, app.c_3, app.c_4, app.c_5 and 2 more"),
], ids=["no-pk", "dropped-identity-index", "nothing", "a-nothing-leaf", "many-leaves"])
def test_an_addition_whose_updates_publishing_would_break_is_never_added(
        monkeypatch, usability, cause) -> None:
    # FAILS WITHOUT THE FIX: only the parent's own NOTHING was refused (and an unreadable
    # identity let the ADD through), so a table whose primary key was dropped after Step 1
    # -- or a partition set to NOTHING, or a dropped identity index -- joined CDC's
    # publication, and PostgreSQL then rejected every UPDATE/DELETE on that production table.
    from dsql_migrator.ui.data_migration._full_load_engine import PG_IDENTITY_UNUSABLE

    source = _PgSource(usability={"app.c": usability})
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c", "app.d"], _pg_watermark())
    assert out == {"app.c": PG_IDENTITY_UNUSABLE.format(cause=cause), "app.d": None}
    assert source.writes() == [  # app.c never joins; app.d (usable) still does
        'ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE "APP"."D"',
        "SELECT PG_CURRENT_XACT_ID()::TEXT",
    ]
    # Read through the READ-ONLY engine, before any write.
    read_at = next(i for i, (k, s) in enumerate(source.log)
                   if "AS IDENTITY_USABLE" in s and k == "read")
    first_write = next(i for i, (k, s) in enumerate(source.log)
                       if k == "write" and s.startswith(("ALTER", "SELECT PG_CURRENT")))
    assert read_at < first_write


@pytest.mark.parametrize("case", ["read-error", "not-found", "connect-error"])
def test_an_unreadable_replica_identity_refuses_before_any_write(monkeypatch, case) -> None:
    # FAILS WITHOUT THE FIX: an unreadable relreplident was not blocked (fail open).
    from dsql_migrator.ui.data_migration._full_load_engine import PG_IDENTITY_UNREADABLE

    if case == "read-error":
        source = _PgSource(usability_error=RuntimeError("permission denied for pg_class"))
    elif case == "not-found":
        source = _PgSource(usability={"app.c": None})
    else:
        class _NoReadEngine(_PgSource):
            def engine(self, kind):
                real = super().engine(kind)
                if kind == "read":
                    def _connect():
                        raise RuntimeError("could not connect")
                    real.connect = _connect
                return real

        source = _NoReadEngine()
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": PG_IDENTITY_UNREADABLE}
    assert source.writes() == [] and not any(k == "write" for k, _s in source.log)


@pytest.mark.parametrize("usability", [
    ("r", "d", True, 0, []), ("r", "f", True, 0, []), ("r", "i", True, 0, []),
    ("p", "d", False, 0, []),  # a parent's own identity is not enforced: its leaves are
], ids=["pk", "full", "valid-index", "usable-partitions"])
def test_an_addition_with_a_usable_replica_identity_is_still_added(monkeypatch,
                                                                   usability) -> None:
    source = _PgSource(usability={"app.c": usability})
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark())
    assert out == {"app.c": None}
    assert 'ALTER PUBLICATION "DSQLMIG_PUB_S" ADD TABLE "APP"."C"' in source.writes()


def test_the_add_itself_rechecks_the_identity_fail_closed(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the write layer refused only NOTHING, so an identity that broke
    # between the guard's read and the ADD (a primary key dropped meanwhile) still joined.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        PG_IDENTITY_UNREADABLE,
        PG_IDENTITY_UNUSABLE,
    )

    class _BrokeMeanwhile(_PgSource):
        def execute(self, kind, sql, params):
            up = " ".join(sql.upper().split())
            if kind == "write" and "AS IDENTITY_USABLE" in up:
                self.log.append((kind, up))
                if params["names"] == ["app.d"]:
                    raise RuntimeError("statement timeout")
                return _Res([("app.c", *_NO_PK)])
            return super().execute(kind, sql, params)

    source = _BrokeMeanwhile()
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c", "app.d"], _pg_watermark())
    assert out == {
        "app.c": PG_IDENTITY_UNUSABLE.format(
            cause="REPLICA IDENTITY DEFAULT, but no valid primary key"),
        "app.d": PG_IDENTITY_UNREADABLE,
    }
    assert source.writes() == []  # neither joined the publication


def test_a_slotless_postgresql_watermark_passes_with_no_statement(monkeypatch) -> None:
    # Start CDC re-snapshots every captured table there (snapshot.mode=initial).
    source = _PgSource()
    out, _b = _guard(_pg_migrator(monkeypatch, source), ["app.c"], _pg_watermark(slot=None))
    assert out == {"app.c": None} and source.log == []


@pytest.mark.parametrize("case", ["timeout", "cancel", "unreadable"])
def test_the_wait_fails_closed(monkeypatch, case) -> None:
    from dsql_migrator.ui.data_migration import _full_load_engine as engine
    from dsql_migrator.ui.data_migration._full_load_engine import (
        CDC_GUARD_STOPPED,
        PG_WAIT_TIMEOUT,
        PG_WAIT_UNREADABLE,
    )

    source = _PgSource(xmin_ready=(False,),
                       poll_error=RuntimeError("gone") if case == "unreadable" else None)
    migrator = _pg_migrator(monkeypatch, source)
    monkeypatch.setattr(engine, "CDC_ADDITION_WAIT_SECONDS", 0.0)
    stop = {"now": False}

    def _cancelled():
        return stop["now"]

    if case == "cancel":
        monkeypatch.setattr(engine, "CDC_ADDITION_WAIT_SECONDS", 3600.0)
        real = source.execute

        def _execute(kind, sql, params):
            result = real(kind, sql, params)
            if "PG_SNAPSHOT_XMIN" in sql.upper():
                stop["now"] = True
            return result

        source.execute = _execute
    out, _b = _guard(migrator, ["app.c"], _pg_watermark(), cancelled=_cancelled)
    expected = {
        "timeout": PG_WAIT_TIMEOUT.format(pub="dsqlmig_pub_s"),
        "cancel": CDC_GUARD_STOPPED,
        "unreadable": PG_WAIT_UNREADABLE.format(pub="dsqlmig_pub_s", cls="RuntimeError"),
    }[case]
    assert out == {"app.c": expected}


def test_wait_for_transactions_before_targets_the_xid_taken_after_the_add() -> None:
    from dsql_migrator.core.cdc_pg_slot import (
        PgReplicationError,
        WAIT_DONE,
        WAIT_TIMEOUT,
        wait_for_transactions_before,
    )

    from tests.test_cdc_pg_slot import _FakeConn

    # A transaction still running (xmin below the target) keeps the wait going -- even if
    # pg_snapshot_xmax were already past it.
    conn = _FakeConn(xmin_ready=(False, False, True))
    beats: list = []
    sleeps: list = []
    outcome, _s = wait_for_transactions_before(conn, "9001", heartbeat=lambda: beats.append(1),
                                               sleep=sleeps.append, clock=lambda: 0.0)
    assert outcome == WAIT_DONE and conn.polls == 3 and len(beats) == 3 and len(sleeps) == 2
    polled = [(sql, params) for sql, params in conn.statements if "pg_snapshot_xmin" in sql]
    assert all(params == {"xid": "9001"} for _sql, params in polled)
    assert all("xmax" not in sql.lower() for sql, _p in polled)
    assert conn.writes() == []  # reads only, never through the write path
    immediately = _FakeConn(xmin_ready=(True,))
    assert wait_for_transactions_before(immediately, "1", sleep=sleeps.append)[0] == WAIT_DONE
    ticks = iter([0.0, 5.0, 11.0, 11.0])
    late = _FakeConn(xmin_ready=(False,))
    assert wait_for_transactions_before(late, "1", timeout_seconds=10.0, sleep=lambda _s: None,
                                        clock=lambda: next(ticks))[0] == WAIT_TIMEOUT
    with pytest.raises(PgReplicationError):
        wait_for_transactions_before(_FakeConn(poll_error=RuntimeError("x")), "1")


# ---------------------------------------------------------------------------
# 10. MySQL: a definition provably unchanged since the start point, else no addition
# ---------------------------------------------------------------------------


def _live(*, charset="", names=("id",), types=("int",)):
    from dsql_migrator.core.cdc_catalog_drift import LiveColumn, LiveTable

    return LiveTable(columns=tuple(
        LiveColumn(name=n, type=t, nullable=False, generated=False, position=i, charset=charset)
        for i, (n, t) in enumerate(zip(names, types))), primary_key=frozenset({"id"}))


def _mysql_migrator(monkeypatch, *, live=None, read_error=None):
    from dsql_migrator.core import cdc_catalog_drift
    from dsql_migrator.core.models import SourceConnectionConfig, SourceInventory
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    reads: list = []

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

        def dispose(self):
            reads.append("dispose")

    def _factory(password, *, read_timeout_seconds=None):
        reads.append(("factory", read_timeout_seconds))
        return lambda _cfg: _Engine()

    def _read(connection, names, *, source_type):
        reads.append(("read", sorted(names), source_type))
        if read_error is not None:
            raise read_error
        return {n: v for n, v in (live or {}).items() if n in names}

    monkeypatch.setattr(engine, "make_source_engine_factory", _factory)
    monkeypatch.setattr(cdc_catalog_drift, "read_live_catalog", _read)
    inputs = engine.DataMigrationInputs(
        source_config=SourceConnectionConfig(host="h", database="app", username="u"),
        source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=[_tbl("app.a"), _tbl("app.c"), _tbl("app.d")]),
    )

    def _facts(_inputs, names, columns, *, probe_rows, primary_key_tables=frozenset()):
        from dsql_migrator.core.target_introspector import TableOverlayFacts

        return {n: TableOverlayFacts(True, False, None, {}) for n in names}

    migrator = engine.BatchedTableMigrator(
        inputs, exporter=object(), watermark_capturer=object(), table_recreator=object(),
        overlay_facts_reader=_facts)
    return migrator, reads


def test_the_mysql_guard_compares_the_live_definition_with_the_baseline(monkeypatch) -> None:
    from dsql_migrator.core.cdc_catalog_drift import REASON_DEF, REASON_NAMES
    from dsql_migrator.core.models import SourceType as ST
    from dsql_migrator.ui.data_migration._full_load_engine import MYSQL_CHANGED

    two = dict(names=("id", "note"), types=("int", "varchar(9)"))
    watermark = _with(_loaded(), fingerprints={
        "app.c": _fp(2, **two), "app.d": _fp(2, **two)}).watermark
    migrator, reads = _mysql_migrator(monkeypatch, live={
        "app.c": _live(**two),  # unchanged
        "app.d": _live(charset="utf8mb4", **two),  # a charset-only change
    })
    out, _b = _guard(migrator, ["app.c", "app.d"], watermark)
    assert out == {"app.c": None, "app.d": MYSQL_CHANGED.format(reason=REASON_DEF)}
    assert ("factory", 10) in reads  # the read-only engine, bounded
    assert ("read", ["app.c", "app.d"], ST.MYSQL) in reads
    reordered, _r = _mysql_migrator(monkeypatch, live={
        "app.c": _live(names=("note", "id"), types=("varchar(9)", "int"))})
    assert _guard(reordered, ["app.c"], watermark)[0] == {
        "app.c": MYSQL_CHANGED.format(reason=REASON_NAMES)}


def test_a_mysql_addition_without_a_comparable_baseline_is_refused_without_a_read(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import MYSQL_UNREADABLE, NO_START_POINT

    v1 = "1:1:00000000:0000000000000000"
    watermark = _with(_loaded(), fingerprints={"app.d": v1}).watermark
    migrator, reads = _mysql_migrator(monkeypatch)
    out, _b = _guard(migrator, ["app.c", "app.d"], watermark)
    assert out == {"app.c": NO_START_POINT, "app.d": NO_START_POINT}
    assert not any(r[0] == "read" for r in reads if isinstance(r, tuple))
    broken, _r = _mysql_migrator(monkeypatch, read_error=TimeoutError("secret host db:3306"))
    out, _b = _guard(broken, ["app.c"], _with(_loaded(), fingerprints={"app.c": _fp()}).watermark)
    assert out == {"app.c": MYSQL_UNREADABLE.format(cls="TimeoutError")}
    assert "secret" not in out["app.c"]  # the class name only


def test_the_watermark_records_baselines_in_a_separate_statement_before_the_position() -> None:
    # FAILS WITHOUT THE FIX: no baseline was recorded, so no MySQL table added after the
    # load could ever be proven unchanged since the start point.
    from dsql_migrator.core.watermark import SHOW_BINARY_LOG_STATUS, capture_watermark

    from tests.test_watermark import FIXED_NOW, _CatalogConnection, _ORDERS_ROWS, _STATUS

    items = [(p, s, "items", c, o, t, n, e, cs) for (p, s, _t, c, o, t, n, e, cs) in _ORDERS_ROWS]
    connection = _CatalogConnection(catalog_rows=_ORDERS_ROWS + items, master_status=_STATUS)
    watermark = capture_watermark(connection, ["app.orders"],
                                  baseline_tables=["app.items", "app.orders"],
                                  now=lambda: FIXED_NOW)
    catalog = [i for i, s in enumerate(connection.executed) if "information_schema.COLUMNS" in s]
    status_at = next(i for i, s in enumerate(connection.executed) if SHOW_BINARY_LOG_STATUS in s)
    assert len(catalog) == 2 and catalog[1] < status_at  # separate, before the position
    assert set(watermark.table_fingerprints) == {"app.orders", "app.items"}
    assert set(watermark.table_row_counts) == {"app.orders"}  # baselines are not run tables

    class _FailsSecond(_CatalogConnection):
        calls = 0

        def execute(self, statement, _parameters=None):
            if "information_schema.COLUMNS" in str(statement):
                _FailsSecond.calls += 1
                if _FailsSecond.calls == 2:
                    self.executed.append(str(statement))
                    raise RuntimeError("baseline read timed out")
            return super().execute(statement, _parameters)

    failing = _FailsSecond(catalog_rows=_ORDERS_ROWS + items, master_status=_STATUS)
    kept = capture_watermark(failing, ["app.orders"], baseline_tables=["app.items"],
                             now=lambda: FIXED_NOW)
    assert set(kept.table_fingerprints) == {"app.orders"}  # the run's fingerprint survives
    assert (kept.binlog_file, kept.binlog_position) == ("mysql-bin.000007", 120)


def test_the_capture_passes_baselines_only_when_there_are_some(monkeypatch) -> None:
    from dataclasses import replace

    from dsql_migrator.ui.data_migration._full_load_engine import _slim_worker_inputs

    migrator, _reads = _mysql_migrator(monkeypatch)
    calls: list = []

    class _OldCapturer:  # the older signature: no keyword
        def capture(self, source, tables):
            calls.append(("old", list(tables)))
            return _loaded().watermark

    migrator._watermark_capturer = _OldCapturer()
    migrator.capture_watermark([_tbl("app.a")])
    assert calls == [("old", ["app.a"])]

    class _NewCapturer:
        def capture(self, source, tables, *, baseline_tables=()):
            calls.append(("new", list(tables), tuple(baseline_tables)))
            return _loaded().watermark

    migrator._watermark_capturer = _NewCapturer()
    migrator.bind_run_inputs(replace(migrator._inputs, cdc_baseline_tables=("app.c", "app.d")))
    migrator.capture_watermark([_tbl("app.a")])
    assert calls[-1] == ("new", ["app.a"], ("app.c", "app.d"))
    assert _slim_worker_inputs(migrator._inputs, "app.a").cdc_baseline_tables == ()


@pytest.mark.parametrize("pg, mtype", [(False, FLC), (False, FLO), (True, FLC)])
def test_the_runner_records_baselines_for_the_migratable_tables_it_does_not_load(
        monkeypatch, pg, mtype) -> None:
    # FAILS WITHOUT THE FIX: a MySQL run recorded no baseline (both run types: a Full load
    # only set can be switched to Full load + CDC unchanged); PostgreSQL needs none.
    import sys
    import types

    from dsql_migrator.core.models import (
        AssessmentReport,
        SourceConnectionConfig,
        SourceInventory,
        TargetConnectionConfig,
        TargetInventory,
    )
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import DataMigrationStore
    from dsql_migrator.ui.evaluation import EvaluationResult, EvaluationStore
    from dsql_migrator.ui.schema_conversion import SchemaConversionStore
    from dsql_migrator.ui.session import SessionStore

    from tests.test_ui_data_migration import _ScreenUi

    ui = _ScreenUi()
    fake = types.ModuleType("nicegui")
    fake.ui = ui
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    sessions = SessionStore()
    session = sessions.get_or_create("s1")
    session.set_source(SourceConnectionConfig(
        source_type=SourceType.POSTGRES if pg else SourceType.MYSQL, host="h",
        port=5432 if pg else 3306, database="app", username="u"), None)
    session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws",
                                              region="us-east-1"))
    session.set_migration_type(mtype)
    evals = EvaluationStore()
    evals.get_or_create("s1").set_result(EvaluationResult(
        inventory=SourceInventory(tables=[_tbl("app.a"), _tbl("app.b"), _tbl("app.c")]),
        assessment=AssessmentReport.from_items([]), target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[]))
    convs = SchemaConversionStore()
    convs.get_or_create("s1").generated_node_ids = ["table:app.a", "table:app.b", "table:app.c"]
    migrations = DataMigrationStore()
    state = migrations.get_or_create("s1")
    state.bind_session(session)
    state.set_selection(TableSelection(selected_tables=["app.a"]))
    seen: list = []

    def _factory(inputs):
        seen.append(inputs.cdc_baseline_tables)
        return SimpleNamespace()

    jobs = _Jobs({})
    _content, runner = dm.build_data_migration_screen(
        sessions, "s1", job_manager=jobs, eval_store=evals, migration_store=migrations,
        conv_store=convs, migrator_factory=_factory)
    runner()
    assert seen == [() if pg else ("app.b", "app.c")]
    assert len(jobs.submitted) == 1


def test_the_start_point_gate_compares_a_covered_addition_with_its_baseline(monkeypatch) -> None:
    # Once covered, the G5 gate at Start reads the addition too, against its baseline: a
    # definition changed since then blocks.
    from dsql_migrator.core import cdc_catalog_drift
    from dsql_migrator.ui.data_migration import _cdc_status
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once
    from dsql_migrator.ui.data_migration._cdc_ui import _mysql_start_context

    job = _with(_three(), added={"app.c": 3}, fingerprints={
        "app.a": _fp(), "app.b": _fp(), "app.c": _fp()})
    state = _fc(["app.a", "app.b", "app.c"], phase="infra", status="CREATE_COMPLETE")
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL),
                              source_password=object())
    _seed, _origin, _facts, _mode, tables = _mysql_start_context(
        state, _Jobs({"job-1": job}), _inv("app.a", "app.b", "app.c"), session)
    assert [t.name for t in tables] == ["app.a", "app.b", "app.c"]

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Engine:
        def connect(self):
            return _Conn()

        def dispose(self):
            return None

    monkeypatch.setattr(_cdc_status, "make_source_engine_factory",
                        lambda *_a, **_k: (lambda _c: _Engine()))
    monkeypatch.setattr(cdc_catalog_drift, "read_live_catalog", lambda _c, names, **_k: {
        "app.a": _live(), "app.b": _live(), "app.c": _live(names=("id", "x"),
                                                           types=("int", "int"))})
    verdict = read_cdc_start_point_once(
        session, [t.name for t in tables], job.watermark.table_fingerprints)
    assert verdict.kind == "changed" and verdict.blocks
    assert [name for name, _reason in verdict.changed] == ["app.c"]


# ---------------------------------------------------------------------------
# 11. PostgreSQL: the publication add itself, and the prerequisite dead end
# ---------------------------------------------------------------------------


def test_add_table_to_publication_is_idempotent_and_refuses_before_any_write() -> None:
    from dsql_migrator.core.cdc_pg_slot import (
        PgPublicationMissingError,
        PgReplicationError,
        add_table_to_publication,
    )

    from tests.test_cdc_pg_slot import _FakeConn

    logs: list = []
    member = _FakeConn(pubs={"p": ["app.c"]})
    assert add_table_to_publication(member, name="p", table="app.c", on_log=logs.append) is False
    assert member.writes() == [] and logs == ['app.c is already in publication "p"; not added again']
    for_all = _FakeConn(pubs={"p": []}, all_tables={"p"})
    assert add_table_to_publication(for_all, name="p", table="app.c") is False
    assert for_all.writes() == []

    class _Duplicate(Exception):
        sqlstate = "42710"

    raced = _FakeConn(pubs={"p": []}, add_errors={"app.c": _Duplicate("already member")})
    assert add_table_to_publication(raced, name="p", table="app.c") is False
    with pytest.raises(PgPublicationMissingError):
        add_table_to_publication(_FakeConn(), name="p", table="app.c")
    nothing = _FakeConn(pubs={"p": []}, replident={"app.c": "n"})
    with pytest.raises(PgReplicationError, match="REPLICA IDENTITY NOTHING"):
        add_table_to_publication(nothing, name="p", table="app.c")
    assert nothing.writes() == []  # refused before any write
    logs.clear()
    added = _FakeConn(pubs={"p": ["app.a"]})
    assert add_table_to_publication(added, name="p", table="app.c", on_log=logs.append) is True
    assert added.writes() == ['ALTER PUBLICATION "p" ADD TABLE "app"."c"']
    # The audit line names the write AFTER it committed.
    assert logs == ['added app.c to publication "p" (selected after the Full Load; its changes '
                    'stream from now on)']


@pytest.mark.parametrize("usability, cause", [
    (("r", "d", False, 0, []), "REPLICA IDENTITY DEFAULT, but no valid primary key"),
    (("r", "i", False, 0, []),
     "REPLICA IDENTITY USING INDEX, but that index is gone or not valid"),
    (("p", "f", True, 2, ["app.c_a", "app.c_b"]), "2 of its partitions have none: "
                                                   "app.c_a, app.c_b"),
], ids=["no-pk", "dropped-identity-index", "nothing-leaves"])
def test_add_table_to_publication_refuses_a_table_whose_updates_would_break(
        usability, cause) -> None:
    # FAILS WITHOUT THE FIX: only relreplident 'n' on the table itself was refused.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError, add_table_to_publication

    from tests.test_cdc_pg_slot import _FakeConn

    conn = _FakeConn(pubs={"p": ["app.a"]}, usability={"app.c": usability})
    with pytest.raises(PgReplicaIdentityError) as refused:
        add_table_to_publication(conn, name="p", table="app.c")
    assert refused.value.cause == cause and not refused.value.unreadable
    assert "UPDATE and DELETE on it would fail on the source" in str(refused.value)
    assert conn.writes() == []


@pytest.mark.parametrize("case", ["read-error", "not-found"])
def test_add_table_to_publication_fails_closed_on_an_unreadable_identity(case) -> None:
    # FAILS WITHOUT THE FIX: an unreadable relreplident returned early and the ADD ran.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError, add_table_to_publication

    from tests.test_cdc_pg_slot import _FakeConn

    conn = (_FakeConn(pubs={"p": []}, usability_error=RuntimeError("denied"))
            if case == "read-error" else _FakeConn(pubs={"p": []}))
    if case == "not-found":
        real = conn.execute

        def _execute(statement, params=None):
            if "AS identity_usable" in str(statement):
                conn.statements.append((str(statement), params or {}))
                from tests.test_cdc_pg_slot import _Result

                return _Result([])
            return real(statement, params)

        conn.execute = _execute
    with pytest.raises(PgReplicaIdentityError) as refused:
        add_table_to_publication(conn, name="p", table="app.c")
    assert refused.value.unreadable and refused.value.cause is None
    assert conn.writes() == []


def test_a_reused_publication_never_gets_a_table_whose_updates_would_break() -> None:
    # The Re-run over a grown set adds a missing table through the same fail-closed check.
    from dsql_migrator.core.cdc_pg_slot import PgReplicaIdentityError, create_publication

    from tests.test_cdc_pg_slot import _FakeConn

    conn = _FakeConn(pubs={"p": ["app.orders"]},
                     usability={"app.customers": ("r", "d", False, 0, [])})
    with pytest.raises(PgReplicaIdentityError):
        create_publication(conn, name="p", tables=["app.orders", "app.customers"])
    assert conn.writes() == []


def test_the_usability_read_is_one_catalog_statement_with_the_relcache_rule() -> None:
    from dsql_migrator.core.cdc_pg_slot import (
        ReplicaIdentityUsability,
        read_replica_identity_usability,
    )

    from tests.test_cdc_pg_slot import _FakeConn

    conn = _FakeConn(usability={"app.p": ("p", "d", False, 1, ["app.p_1", None])})
    reads = read_replica_identity_usability(conn, ["app.p", "app.t", "app.p"])
    assert reads == {
        "app.p": ReplicaIdentityUsability("p", "d", False, 1, ("app.p_1",)),
        "app.t": ReplicaIdentityUsability("r", "d", True, 0, ()),
    }
    ((sql, params),) = conn.statements
    assert params == {"names": ["app.p", "app.t"]}  # one bounded, deduplicated statement
    flat = " ".join(sql.split())
    # DEFAULT needs a valid, immediate primary key; USING INDEX a valid identity index;
    # every leaf of a partitioned table is graded by the same rule, server-side.
    assert "i.indisprimary AND i.indisvalid AND i.indimmediate" in flat
    assert "i.indisreplident AND i.indisvalid AND i.indisunique AND i.indimmediate" in flat
    assert flat.count("t.isleaf AND NOT (l.relreplident = 'f'") == 2
    assert "c.relkind IN ('r','p')" in flat
    assert read_replica_identity_usability(conn, []) == {} and len(conn.statements) == 1


def test_take_xid_after_change_goes_through_the_audited_write_path() -> None:
    from dsql_migrator.core.cdc_pg_slot import take_xid_after_change

    from tests.test_cdc_pg_slot import _FakeConn

    logs: list = []
    conn = _FakeConn(xid="12345")
    assert take_xid_after_change(conn, on_log=logs.append) == "12345"
    assert conn.writes() == ["SELECT pg_current_xact_id()::text"]
    assert logs == ["took a transaction id after the publication change, to wait for the "
                    "transactions open then"]


def _pg_facts(**kw):
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

    base = dict(checked_publication_name="dsqlmig_pub_s", checked_slot_name="dsqlmig_s",
                publication_present=True, publication_publishes_all_dml=True,
                publication_tables=frozenset({"app.a", "app.b"}), slot_usable=True)
    base.update(kw)
    return PostgresCdcFacts(**base)


def test_a_pending_table_is_not_missing_from_the_publication_but_a_captured_one_is() -> None:
    # FAILS WITHOUT THE FIX: the required FAIL for app.c held the very Retry that adds it
    # to the publication -- and Deploy and Start with it (a dead end).
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects

    tables = [_tbl("app.a"), _tbl("app.b"), _tbl("app.c")]
    result = check_cdc_replication_objects(_pg_facts(), tables, provisions_replication=False,
                                           pending_tables=["app.c"])
    assert result.status is PrerequisiteStatus.PASS and result.required
    # FAILS WITHOUT THE FIX: "covers all 2 selected tables" with 3 selected.
    assert result.detail.startswith(
        'Publication "dsqlmig_pub_s" covers all 2 selected tables CDC captures now and '
        'replication slot')
    assert result.detail.endswith(
        " app.c is not in it yet: its load (Retry unfinished tables) adds it before its copy "
        "starts.")
    captured_missing = check_cdc_replication_objects(
        _pg_facts(publication_tables=frozenset({"app.a"})), tables,
        provisions_replication=False, pending_tables=["app.c"])
    assert captured_missing.status is PrerequisiteStatus.FAIL and captured_missing.required
    assert "1 of the 2 selected tables CDC captures now: app.b." in captured_missing.detail
    today = check_cdc_replication_objects(_pg_facts(), tables, provisions_replication=False)
    assert today.status is PrerequisiteStatus.FAIL  # without the pending set: as before
    assert "does not include 1 of the 3 selected tables: app.c." in today.detail
    unchanged = check_cdc_replication_objects(_pg_facts(), tables[:2],
                                              provisions_replication=False)
    assert "covers all 2 selected tables and " in unchanged.detail  # G16 wording kept
    # Only the pending table selected: the publication "exists" -- never "covers all 0".
    only = check_cdc_replication_objects(_pg_facts(), [_tbl("app.c")],
                                         provisions_replication=False, pending_tables=["app.c"])
    assert only.status is PrerequisiteStatus.PASS
    assert only.detail.startswith('Publication "dsqlmig_pub_s" exists and replication slot')
    assert "covers all 0" not in only.detail
    # Where its load would refuse it (the slot gone, a publication narrowed to some change
    # types), the row does not promise that load adds it: it already blocks or warns.
    for facts in (_pg_facts(slot_usable=False), _pg_facts(publication_publishes_all_dml=False)):
        blocked = check_cdc_replication_objects(facts, tables, provisions_replication=False,
                                                pending_tables=["app.c"])
        assert blocked.status is PrerequisiteStatus.FAIL
        assert "adds it before its copy starts" not in blocked.detail
    gone = check_cdc_replication_objects(_pg_facts(publication_present=False), tables,
                                         provisions_replication=False,
                                         pending_tables=["app.c"])
    assert gone.status is PrerequisiteStatus.FAIL and "not in it yet" not in gone.detail


def test_the_prerequisite_run_passes_the_pending_tables_on_postgresql(monkeypatch) -> None:
    import asyncio

    from dsql_migrator.ui import data_migration as dm

    for pg in (True, False):
        s = _fc_screen(monkeypatch, job=_with(_loaded(), slot="dsqlmig_s"),
                       selected=["app.a", "app.b", "app.c"], pg=pg, render=False)
        s.session.set_source_verified(True)
        s.session.set_target_verified(True)
        captured: dict = {}
        real_panel = dm._render_prerequisites_panel

        def _spy(ui_, state, run_checks, *a, _real=real_panel, **k):
            captured["run_checks"] = run_checks
            return _real(ui_, state, run_checks, *a, **k)

        requests: list = []

        class _Checker:
            def check(self, request, **_k):
                requests.append(request)
                return PrerequisiteReport.build(request.mode, [])

        monkeypatch.setattr(dm, "_render_prerequisites_panel", _spy)
        monkeypatch.setattr(dm, "build_prerequisite_checker", lambda **k: _Checker())
        s.content(lambda: None)
        asyncio.run(captured["run_checks"](MigrationMode.CDC))
        (request,) = requests
        assert request.cdc_pending_tables == (["app.c"] if pg else [])


# ---------------------------------------------------------------------------
# 12. Start CDC waits for a selected table the watermark does not cover; Deploy sizes
# ---------------------------------------------------------------------------


def _seedable(job):
    job = job.model_copy(deep=True)
    job.watermark = job.watermark.model_copy(
        update={"binlog_file": "mysql-bin.000001", "binlog_position": 4})
    return job


_MYSQL = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.MYSQL,
                                                       database="app"))


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY])
def test_start_cdc_waits_for_a_selected_table_not_loaded_under_the_start_point(mtype) -> None:
    # FAILS WITHOUT THE FIX: Start was the enabled primary action, and CDC would have started
    # without app.c -- then the set is fixed for good, and app.c's copy stays stale.
    from dsql_migrator.ui.data_migration._cdc_ui import (
        START_UNCOVERED_HEADER,
        _render_cdc_start_button,
        start_uncovered_body,
    )

    state = _fc(["app.a", "app.b", "app.c"], phase="infra", status="CREATE_COMPLETE")
    state.migration_type = mtype
    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _RecordingUi()
    _render_cdc_start_button(ui, state, _Jobs({"job-1": _seedable(_loaded())}), lambda: None,
                             inventory=_inv("app.a", "app.b", "app.c"), session=_MYSQL)
    body = start_uncovered_body(["app.c"], mtype)
    assert START_UNCOVERED_HEADER in ui.texts and body in ui.texts
    assert "Ready to start CDC" not in ui.texts
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert "disable" in start.props_seen and body in ui.tooltips
    if mtype is FLC:
        assert body == (
            "app.c is selected but not loaded under the Full Load's start point yet, so CDC "
            "would not capture it. Load it on the Full Load step (Retry unfinished tables, or "
            "Re-run all tables where that step says Retry cannot add it), or untick it in the "
            "table list, then start CDC.")
    else:
        assert body == (
            "app.c is selected but was not loaded by this migration's Full Load, so CDC would "
            "not capture it. Switch the migration type to Full load + CDC and load it there, "
            "or untick it in its table list.")


def test_start_cdc_has_no_gate_for_an_untouched_selection_or_a_started_migration() -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import (
        START_UNCOVERED_HEADER,
        _render_cdc_start_button,
    )

    jobs = _Jobs({"job-1": _seedable(_loaded())})
    for state in (_fc(["app.a", "app.c"], touched=False, phase="infra",
                      status="CREATE_COMPLETE"),
                  _fc(["app.a", "app.b", "app.c"], phase="infra", status="UPDATE_COMPLETE")):
        if state.selection_touched:
            state.set_cdc_ops_window_start(_TS)
        state.set_prereq_gated_mode(MigrationMode.CDC)
        ui = _RecordingUi()
        _render_cdc_start_button(ui, state, jobs, lambda: None,
                                 inventory=_inv("app.a", "app.b", "app.c"), session=_MYSQL)
        assert START_UNCOVERED_HEADER not in ui.texts


def test_the_source_config_card_names_the_load_when_only_pending_tables_are_selected() -> None:
    # FAILS WITHOUT THE FIX: with only app.c (selected after the Full Load, not loaded yet)
    # ticked, the card said "None of the tables the Full Load loaded is selected ... Tick at
    # least one" -- while the remedy is app.c's load on the Full Load step.
    from dsql_migrator.ui.data_migration._cdc_ui import (
        START_UNCOVERED_HEADER,
        _render_cdc_source_config_card,
        start_uncovered_body,
    )

    from tests.test_ui_data_migration import _ScreenUi

    def texts(selected, job):
        ui = _ScreenUi()
        _render_cdc_source_config_card(
            ui, _fc(selected, phase="infra", status="CREATE_COMPLETE"), _Jobs({"job-1": job}),
            lambda: None, inventory=_inv("app.a", "app.b", "app.c"), migration_type=FLC,
            session=_MYSQL)
        return ui.texts

    pending = texts(["app.c"], _seedable(_loaded()))
    assert START_UNCOVERED_HEADER in pending
    assert start_uncovered_body(["app.c"], FLC) in pending
    assert "Select at least one table before starting CDC" not in pending
    # Nothing selected at all: G16's text, unchanged.
    assert "Select at least one table before starting CDC" in texts([], _seedable(_loaded()))
    # Once a guarded load covers app.c, the card builds its config: no empty-capture notice.
    covered = texts(["app.c"], _seedable(_with(_loaded(), added={"app.c": 3})))
    assert START_UNCOVERED_HEADER not in covered
    assert "Select at least one table before starting CDC" not in covered


def _cdc_step_texts(mtype, selected, job, *, phase="infra", status="CREATE_COMPLETE"):
    """The WHOLE CDC step (its source config card, then its Start card) on one screen."""
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_step

    from tests.test_ui_data_migration import _ScreenUi

    state = _fc(selected, phase=phase, status=status)
    state.migration_type = mtype
    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _ScreenUi()
    _render_cdc_step(ui, state, _Jobs({"job-1": job}), lambda: None,
                     inventory=_inv("app.a", "app.b", "app.c"), migration_type=mtype,
                     session=_MYSQL)
    return ui


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY])
def test_the_cdc_step_says_start_waits_for_a_pending_table_once(mtype) -> None:
    # FAILS WITHOUT THE FIX: with only app.c (still to load) selected and the infrastructure
    # deployed, the source config card AND the Start card drew the same warning.
    from dsql_migrator.ui.data_migration._cdc_ui import START_UNCOVERED_HEADER, start_uncovered_body

    ui = _cdc_step_texts(mtype, ["app.c"], _seedable(_loaded()))
    body = start_uncovered_body(["app.c"], mtype)
    assert ui.texts.count(START_UNCOVERED_HEADER) == 1 and ui.texts.count(body) == 1
    (start,) = [b for b in ui.buttons if b.text == "Start CDC"]
    assert "disable" in start.props_seen and body in ui.tooltips  # Start still says why
    assert any(t.startswith("Will stream no tables yet: app.c") for t in ui.texts)
    # Beside a captured table, the Start card is the one place that says it.
    beside = _cdc_step_texts(mtype, ["app.a", "app.c"], _seedable(_loaded()))
    assert beside.texts.count(START_UNCOVERED_HEADER) == 1
    assert "Will stream 1 table: app.a" in beside.texts
    # Before the infrastructure: the config card, once.
    absent = _cdc_step_texts(mtype, ["app.c"], _seedable(_loaded()), phase="absent",
                             status=None)
    assert absent.texts.count(START_UNCOVERED_HEADER) == 1
    # Nothing selected (G16) and a covered addition: no such notice.
    nothing = _cdc_step_texts(mtype, [], _seedable(_loaded()))
    assert START_UNCOVERED_HEADER not in nothing.texts
    assert nothing.texts.count("Select at least one table before starting CDC") == 1
    covered = _cdc_step_texts(mtype, ["app.c"], _seedable(_with(_loaded(), added={"app.c": 3})))
    assert START_UNCOVERED_HEADER not in covered.texts


def test_the_start_dialog_holds_its_confirm_for_an_uncovered_table(monkeypatch) -> None:
    # The Start dialog re-reads the uncovered selection: its notice, a disabled confirm with
    # the same reason, and none of its source probes (this Start cannot run anyway).
    import asyncio

    import nicegui

    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.ui.data_migration._cdc_ui import START_UNCOVERED_HEADER, start_uncovered_body

    from tests.test_ui_data_migration import _DialogUi

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    probes: list = []
    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    monkeypatch.setattr(cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(cdc_ui, "_probe_binlog_resume_gap",
                        lambda *_a, **_k: probes.append("binlog"))
    ui = _DialogUi()
    monkeypatch.setattr(
        cdc_ui, "_render_notice",
        lambda _ui, *, tone="info", icon=None, header="", body="": ui.notices.append(
            (tone, header)))
    state = _fc(_ABC, phase="infra", status="CREATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    asyncio.run(cdc_ui._open_cdc_start_dialog(
        ui, state, lambda: None, session=_MYSQL,
        job_manager=_Jobs({"job-1": _seedable(_loaded())}), inventory=_inv(*_ABC)))
    assert ("warning", START_UNCOVERED_HEADER) in ui.notices
    start = ui.buttons["Start CDC"]
    assert start.enabled is False
    assert start.tooltip_text == start_uncovered_body(["app.c"], FLC)
    assert probes == []


def test_the_start_backstop_refuses_before_any_parameter_is_built(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: Retry CDC (no dialog) submitted a Start without the table.
    from tests.test_ui_data_migration import _inventory, _run_start_cdc_capture

    from dsql_migrator.ui.data_migration._cdc_ui import START_UNCOVERED_HEADER

    def _state():
        state = DataMigrationState()
        state.migration_type = FLC
        state.set_selection(TableSelection(selected_tables=["orders", "customers"]))
        return state

    wm = Watermark(binlog_file="mysql-bin.000042", binlog_position=120, snapshot_timestamp=_TS,
                   table_row_counts={"orders": 10})
    refused = _run_start_cdc_capture(monkeypatch, source_type=SourceType.MYSQL,
                                     job_watermark=wm, state=_state(), inventory=_inventory())
    assert not refused["submitted"] and refused["kwargs"] == {}
    assert [n["header"] for n in refused["notices"]] == [START_UNCOVERED_HEADER]
    # Covered by a guarded load: Start sends it.
    covered = wm.model_copy(update={"cdc_added_tables": {"customers": 5}})
    started = _run_start_cdc_capture(monkeypatch, source_type=SourceType.MYSQL,
                                     job_watermark=covered, state=_state(),
                                     inventory=_inventory())
    assert started["submitted"]
    filled = dict(started["kwargs"]["params"].filled)
    assert set(filled["TableIncludeList"].split(",")) == {"orders", "customers"}
    assert "customers" in filled["SinkTopics"] and "orders" in filled["SinkTopics"]


def _added_job(engine, *, slot=True):
    """A record whose watermark covers app.c through a guarded addition: MySQL seedable;
    PostgreSQL with the Full Load's slot, publication and LSN (the gapless route) unless
    ``slot`` is False (a start that re-snapshots every table)."""
    job = _with(_three(), added={"app.c": 3})
    if engine is SourceType.MYSQL:
        return _seedable(job)
    if slot:
        job = _with(job, slot="dsqlmig_s", publication="dsqlmig_pub_s")
        job.watermark = job.watermark.model_copy(update={"wal_lsn": "0/16B3748"})
    return job


@pytest.mark.parametrize("engine", [SourceType.MYSQL, SourceType.POSTGRES])
def test_the_start_card_discloses_a_captured_addition(engine) -> None:
    # FAILS WITHOUT THE FIX (PostgreSQL): the notice said CDC replays every change to app.c
    # since the Full Load's start point -- on PostgreSQL the slot decodes it only from when
    # it joined CDC's publication, just before its copy (the copy covers the time before).
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_button

    state = _fc(["app.a", "app.b", "app.c"], phase="infra", status="CREATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    job = _added_job(engine)
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=engine, database="app"))
    ui = _RecordingUi()
    _render_cdc_start_button(ui, state, _Jobs({"job-1": job}), lambda: None,
                             inventory=_inv("app.a", "app.b", "app.c"), session=session)
    assert "Ready to start CDC" in ui.texts
    assert "Tables added after the Full Load" in ui.texts
    (body,) = [t for t in ui.texts if t.startswith("app.c was loaded after the Full Load")]
    assert "dead-lettered; Cut over then waits for a CHECKSUM validation" in body
    assert "its Kafka topic gets the infrastructure's default partition count" in body
    if engine is SourceType.MYSQL:
        assert ("is captured from its start point like the other tables: CDC replays every "
                "change made to it since then over the newer copy, which converges.") in body
        assert body.endswith("since the Full Load's start point.")
    else:
        assert "replays every change made to it since then" not in body
        assert ("is captured from the same replication slot as the other tables: CDC streams "
                "every change made to it since it joined CDC's publication (at the latest "
                "just before its copy) over that copy, which converges.") in body
        assert "binary log" not in body
    assert "Will stream 3 tables: app.a, app.b, app.c" in ui.texts
    # Not once CDC has started.
    state.set_cdc_ops_window_start(_TS)
    later = _RecordingUi()
    _render_cdc_start_button(later, state, _Jobs({"job-1": job}), lambda: None,
                             inventory=_inv("app.a", "app.b", "app.c"), session=session)
    assert "Tables added after the Full Load" not in later.texts


def test_a_start_that_re_snapshots_every_table_does_not_disclose_a_replay() -> None:
    # FAILS WITHOUT THE FIX: the replay notice rendered under "takes a FRESH SNAPSHOT of
    # every selected table" -- nothing is replayed over a copy there.
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_button

    state = _fc(["app.a", "app.b", "app.c"], phase="infra", status="CREATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    session = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES,
                                                            database="app"))
    ui = _RecordingUi()
    _render_cdc_start_button(ui, state, _Jobs({"job-1": _added_job(SourceType.POSTGRES,
                                                                   slot=False)}),
                             lambda: None, inventory=_inv("app.a", "app.b", "app.c"),
                             session=session)
    assert any("FRESH SNAPSHOT" in t for t in ui.texts)
    assert "Tables added after the Full Load" not in ui.texts


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY])
def test_will_stream_names_the_pending_tables_when_nothing_else_is_selected(mtype) -> None:
    # FAILS WITHOUT THE FIX: the line promised "Retry unfinished tables ... loads it" -- for
    # a table this watermark recorded no start point for (Retry cannot add it) and, in CDC
    # only, on a Full Load step that type does not have.
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_button

    state = _fc(["app.c"], phase="infra", status="CREATE_COMPLETE")
    state.migration_type = mtype
    state.set_prereq_gated_mode(MigrationMode.CDC)
    ui = _RecordingUi()
    _render_cdc_start_button(ui, state, _Jobs({"job-1": _seedable(_loaded())}), lambda: None,
                             inventory=_inv("app.a", "app.b", "app.c"), session=_MYSQL)
    if mtype is FLC:
        assert ("Will stream no tables yet: app.c is not loaded under the Full Load's start "
                "point yet (load it on the Full Load step: Retry unfinished tables, or Re-run "
                "all tables where that step says Retry cannot add it).") in ui.texts
    else:
        assert ("Will stream no tables yet: app.c was not loaded by this migration's Full "
                "Load (switch the migration type to Full load + CDC and load it there).") in (
            ui.texts)
        assert not any("Full Load step" in t for t in ui.texts)


def test_deploy_sizes_the_capture_and_says_what_it_does_not_size(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration._cdc_ui import (
        DEPLOY_PENDING_EMPTY,
        _cdc_row_counts_from_watermark,
        deploy_pending_hint,
    )

    def deploy(s):
        return [b for b in s.ui.buttons if b.text == "Deploy CDC infrastructure"]

    s = _fc_screen(monkeypatch, job=_seedable(_loaded()), selected=["app.a", "app.b", "app.c"],
                   render=False)
    s.state.set_prereq_gated_mode(MigrationMode.CDC)
    _rerender(s)
    assert deploy(s) and all("disable" not in b.props_seen for b in deploy(s))
    assert deploy_pending_hint(["app.c"]) in s.ui.texts
    assert deploy_pending_hint(["app.c"]) == (
        "app.c is not loaded yet, so this deploy does not size its Kafka topic: Start CDC "
        "creates it later with the infrastructure's default partition count. Load it on the "
        "Full Load step first (Retry unfinished tables, or Re-run all tables where that step "
        "says Retry cannot add it) to size it too.")
    only = _fc_screen(monkeypatch, job=_seedable(_loaded()), selected=["app.c"], render=False)
    only.state.set_prereq_gated_mode(MigrationMode.CDC)
    _rerender(only)
    assert deploy(only) and all("disable" in b.props_seen for b in deploy(only))
    assert DEPLOY_PENDING_EMPTY in only.ui.tooltips and DEPLOY_PENDING_EMPTY in only.ui.texts
    # Once covered, the partition plan sizes app.c by the rows its load wrote.
    wm = _with(_loaded(), added={"app.c": 70}).watermark
    assert _cdc_row_counts_from_watermark(wm, [_tbl("app.a"), _tbl("app.c")]) == {
        "app.a": 1, "app.c": 70}


def test_the_deploy_click_refuses_an_infrastructure_for_pending_tables_only(monkeypatch) -> None:
    import asyncio
    import sys
    import types

    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_ui import DEPLOY_PENDING_EMPTY

    io_calls: list = []

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            io_calls.append(fn)
            raise AssertionError("no AWS call may run before the guard")

    fake = types.ModuleType("nicegui")
    fake.run = _Run
    monkeypatch.setitem(sys.modules, "nicegui", fake)
    notes: list = []
    state = _fc(["app.c"])
    asyncio.run(_cdc_ui._start_cdc_infra_deploy(
        SimpleNamespace(notify=lambda text="", **k: notes.append((text, k.get("type")))),
        state, _Jobs({"job-1": _loaded()}), lambda: None, inventory=_inv("app.a", "app.c"),
        session=SimpleNamespace(target_config=SimpleNamespace(region="us-east-1"))))
    assert notes == [(DEPLOY_PENDING_EMPTY, "warning")] and io_calls == []


# ---------------------------------------------------------------------------
# 13. Validation and Cut over: a report from before the addition holds
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mtype", [FLC, CDC_ONLY])
def test_a_report_from_before_an_addition_holds_cut_over(monkeypatch, mtype) -> None:
    # FAILS WITHOUT THE FIX: the scope gap was Full load only, so a report with no row for
    # app.c released Cut over in a CDC type.
    from dsql_migrator.ui.validation import full_load_covered_tables, validation_scope_gap

    from tests.test_g12_table_set_after_load import _cutover, _report

    job = _with(_job("DONE", [_c("a"), _c("b"), _c("c")], counts={"a": 1, "b": 1}),
                added={"c": 1})
    jobs = _Jobs({"job-1": job})
    state = _flo(["a", "b", "c"], mtype=mtype)
    covered = full_load_covered_tables(state, jobs)
    inv = _inv("a", "b", "c")
    assert validation_scope_gap(_report("a", "b"), inv, state, covered=covered) == (["c"], [])
    assert validation_scope_gap(_report("a", "c"), inv, state, covered=covered) == ([], [])
    assert validation_scope_gap(_report("a", "b", "c", "x"), inv, state,
                                covered=covered) == ([], [])  # extra stays [] in a CDC type
    assert validation_scope_gap(_report("a", "b"), inv, state, covered=None) == ([], [])
    held = _cutover(monkeypatch, selected=["a", "b", "c"], report_tables=["a", "b"], mtype=mtype,
                    jobs=jobs)
    held.content(lambda: None)
    assert "Validation does not cover the current table set" in " ".join(held.ui.texts)
    held.runner()
    assert held.ui.notes and held.ui.notes[0].startswith(
        "Validation does not cover the current table set")
    released = _cutover(monkeypatch, selected=["a", "b", "c"], report_tables=["a", "b", "c"],
                        mtype=mtype, jobs=jobs)
    released.content(lambda: None)
    assert "Validation does not cover the current table set" not in " ".join(
        released.ui.texts)
    # An unchanged session (nothing added) is never held -- not even with a chip off.
    plain = _Jobs({"job-1": _job("DONE", [_c("a"), _c("b")], counts={"a": 1, "b": 1})})
    unchanged = _cutover(monkeypatch, selected=["a", "b"], report_tables=["a"], mtype=mtype,
                         jobs=plain)
    unchanged.content(lambda: None)
    assert "Validation does not cover the current table set" not in " ".join(
        unchanged.ui.texts)


# ---------------------------------------------------------------------------
# 14. The type switch, a restored session
# ---------------------------------------------------------------------------


def test_a_guarded_addition_never_locks_the_type_switch_but_a_full_load_only_load_does(
        monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import migration_type_lock_reason, watermark_coverage_gap

    covered = _with(_three(), added={"app.c": 3})
    assert watermark_coverage_gap(covered, ["app.a", "app.b", "app.c"]) == ([], [])
    detour = _three()  # app.c loaded only by a Full load only retry
    assert watermark_coverage_gap(detour, ["app.a", "app.b", "app.c"]) == (["app.c"], [])
    from tests.test_g12_table_set_after_load import _screen

    for job, locked in ((covered, False), (detour, True)):
        s = _screen(monkeypatch, job=job, selected=["app.a", "app.b", "app.c"])
        s.content(lambda: None)
        reason = migration_type_lock_reason(s.state, status=DONE, job_manager=s.jobs)
        assert (reason is not None) is locked
        if locked:
            assert "(selected since: app.c)" in reason


def test_a_restored_session_reads_the_coverage_from_the_job_record(tmp_path) -> None:
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.job_store import SqliteJobStore
    from dsql_migrator.ui.data_migration import (
        cdc_capture_names,
        fl_cdc_pending_additions,
        table_set_add_lock,
    )

    path = str(tmp_path / "jobs.sqlite")
    store = SqliteJobStore(path)
    store.save(_with(_three(), added={"app.c": 3}), None)
    restored = JobManager(store=SqliteJobStore(path))
    state = _fc(["app.a", "app.b", "app.c"])
    job = restored.get_status("job-1")
    assert cdc_capture_names(state, job.watermark) == {"app.a", "app.b", "app.c"}
    assert fl_cdc_pending_additions(state, restored, ["app.a", "app.b", "app.c"], job=job) == []
    assert table_set_add_lock(state, restored).covered == {"app.a", "app.b", "app.c"}
    # A record written before the coverage (or without it) shows app.c pending.
    stale_path = str(tmp_path / "stale.sqlite")
    SqliteJobStore(stale_path).save(_three(), None)
    stale = JobManager(store=SqliteJobStore(stale_path))
    assert fl_cdc_pending_additions(state, stale, ["app.a", "app.b", "app.c"],
                                    job=stale.get_status("job-1")) == ["app.c"]
