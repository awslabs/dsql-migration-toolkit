# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G27 repair, task R-CDC: the CDC-step findings of G27's verified review.

* [M1+L1] The Drop & reload condition (``_cdc_status.drop_reload_condition_due``) showed only once
  a Start CDC had stamped the ops window, so an Append re-run made BEFORE any Start -- the Re-run
  dialog's default, and where the binlog-purged remedy sends the operator -- was still called
  gapless on every Automatic surface, and so was a retry after a Start from the new watermark
  that failed before it streamed (the submission had moved the window to it). A second sign,
  from the job record alone: a chunk that found rows already on the target on its FIRST attempt
  (``attempts == 1`` with ``rows_skipped``). Its notice then says only what the Full Load found
  -- no "This migration started CDC ...". Not flagged: a first copy into empty tables, a Drop &
  reload re-run, and the rows a Retry unfinished tables / Reload specific tables / G17 addition
  re-read (each re-run table carries its earlier attempt).
* [L3] PostgreSQL: the re-snapshot card's "manual" cause said "A gapless resume IS available
  here" and the "unfinished_load" remedy promised "the gapless resume comes back on its own"
  while that condition was due; both are conditional then (the Deploy dialog's brief too), and
  the full notice names the Drop & reload re-run.
* [GTID-only] The GTID-only start-point notice's re-run now names 'Drop & reload' (rule E).
* [L2] The PostgreSQL Delete CDC infrastructure dialog promised the tool-created source secret
  "will also be scheduled for deletion" though the teardown keeps it when the replication slot
  cannot be dropped; it now says so, as ``cdc_deployer.run_cdc_delete`` does. MySQL unchanged.

The Full Load records are written by the REAL engine (``run_full_load`` / ``run_full_load_retry``
over a fake migrator on a real JobManager), so their attempt counts are the engine's own. Each
test FAILS on the code before this repair, except the guards that pin what stays. Fakes only:
no AWS.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dsql_migrator.core.cdc import Watermark
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.models import (
    ChunkState,
    ColumnDef,
    MigrationJob,
    MigrationMode,
    TableDef,
    TableSelection,
)
from dsql_migrator.ui.cdc_start_point_gate import (
    RELOAD_CONDITION_HEADER,
    RELOAD_CONDITION_LINE,
    RELOAD_CONDITION_TOOLTIP,
    reload_condition,
)
from dsql_migrator.ui.data_migration import DataMigrationState, _cdc_ui
from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration._cdc_status import (
    cdc_started_before_load,
    drop_reload_condition_due,
)
from tests.test_g27_cdc import (
    _CONDITION_LABEL,
    _FL_CDC_BODY,
    _GAPLESS_SET,
    _TODAY_LABEL,
    _TODAY_LINE,
    _TODAY_TOOLTIP,
    CDC_ONLY,
    FL_CDC,
    MYSQL,
    PG_W2,
    POSTGRES,
    T1,
    T2,
    W1,
    W2,
    _card,
    _dialog,
    _JM,
    _pg_session,
    _start_line,
)

# The notice's body for the sign that needs no Start (``reload_condition(..., started=False)``).
_FOUND_FL_CDC = (
    "This Full Load found rows already on the target. Start CDC begins at its watermark, so if the "
    "Full Load appended to them, Re-run Full Load on the Full Load step choosing 'Drop & reload' "
    "before Start CDC. An append keeps those rows as they were, and CDC from a watermark carries "
    "only the changes after it."
)
_FOUND_CDC_ONLY = (
    "This Full Load found rows already on the target. Start CDC begins at its watermark, so if the "
    "Full Load appended to them, run it again choosing 'Drop & reload' before Start CDC (CDC only "
    "has no Full Load step: switch the migration type to Full load only for that run, then back to "
    "CDC only). An append keeps those rows as they were, and CDC from a watermark carries only the "
    "changes after it."
)
_FOUND_PG_CDC_ONLY = (
    "This Full Load found rows already on the target. Start CDC begins at its watermark, so if the "
    "Full Load appended to them, switch the migration type to Full load + CDC and Re-run Full Load "
    "there choosing 'Drop & reload' before Start CDC (it creates the slot again at its own "
    "snapshot point). An append keeps those rows as they were, and CDC from a watermark carries "
    "only the changes after it."
)
# The first copy (W1), and an Append re-run over it (W2): every row of W1 is "already there".
_FIRST = {"orders": (10, 0), "customers": (3, 0)}
_APPENDED = {"orders": (1, 10), "customers": (0, 3)}


# --- records written by the real engine ----------------------------------------------------------


def _tbl(name: str) -> TableDef:
    return TableDef(
        name=name, columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
        primary_key=["id"],
    )


class _Loader:
    """A fake DataMigrator: per table, the rows its load writes and the rows it finds already
    on the target (``(loaded, skipped)``); a table in ``fail`` raises. ``watermark`` None: a
    retry, which takes no new one. Passes every G17 guard (the target table is empty)."""

    def __init__(self, watermark, rows, fail=()):
        self._watermark = watermark
        self._rows = dict(rows)
        self._fail = set(fail)

    def capture_watermark(self, tables=None):
        assert self._watermark is not None, "a retry takes no new watermark"
        return self._watermark

    def guard_cdc_additions(self, names, *, watermark, replace_tables, heartbeat, cancelled):
        return {name: None for name in names}

    def migrate_table(self, table, *, on_rows=None, should_cancel=None):
        if table.name in self._fail:
            raise RuntimeError(f"load failed for {table.name}")
        loaded, skipped = self._rows.get(table.name, (0, 0))
        if on_rows is not None:
            on_rows(loaded, skipped)
        return engine.TableLoadResult(rows_loaded=loaded, rows_skipped=skipped)


def _run(monkeypatch, work) -> MigrationJob:
    monkeypatch.setattr(engine, "pending_foreign_key_count", lambda *_a, **_k: 0)
    monkeypatch.setattr(engine, "_log_identity_sequence_sync", lambda *_a, **_k: {})
    manager = JobManager()
    job_id = manager.submit(work)
    assert manager.wait(job_id, timeout=10.0)
    return manager.get_status(job_id)


def _fresh(monkeypatch, wm, rows, *, fail=()) -> MigrationJob:
    """``run_full_load``: a Full Load that takes a NEW start point (``wm``)."""
    return _run(monkeypatch, lambda handle: engine.run_full_load(
        handle, [_tbl(name) for name in rows], migrator=_Loader(wm, rows, fail),
        error_log=ErrorLogStore(),
    ))


def _retry(monkeypatch, prior, rows, *, additions=()) -> MigrationJob:
    """``run_full_load_retry`` from ``prior`` for the tables in ``rows``, under its watermark:
    Retry unfinished tables, Reload specific tables, or (``additions``) a G17 addition."""
    return _run(monkeypatch, lambda handle: engine.run_full_load_retry(
        handle, prior.chunks, [_tbl(name) for name in rows],
        migrator=_Loader(None, rows), error_log=ErrorLogStore(), watermark=prior.watermark,
        prior_job_id=prior.job_id, prior_table_error_job_ids=dict(prior.table_error_job_ids),
        cdc_additions=tuple(additions),
        cdc_coverage_open=(lambda: True) if additions else None,
    ))


def _current(job: MigrationJob) -> MigrationJob:
    """The record as the session's current Full Load (``job_id`` ``fl-job``, as the doubles of
    the card, the dialog and the Start click look it up)."""
    return job.model_copy(update={"job_id": "fl-job"})


def _click(monkeypatch, state, wm, job, **kw):
    """The real Start CDC click and job body from ``wm`` over ``job`` (``kw``: the capture's
    knobs, e.g. ``run_raises``). Returns the capture; ``details`` are the activity lines."""
    from tests.test_g25_cdc_r7 import _capture

    monkeypatch.setattr(
        _cdc_ui, "_current_job", lambda _jm, job_id: job if job_id == job.job_id else None
    )
    return _capture(monkeypatch, state, wm, **kw)


def _mysql_never_started(mtype=FL_CDC):
    """A MySQL cdc-stack deployed and never started; no Start CDC in this migration."""
    from tests.test_g25_cdc_r7 import _never_started

    state = _never_started()
    state.migration_type = mtype
    state.job_id = "fl-job"
    assert state.cdc_ops_window_start is None
    return state


def _pg_never_started(mtype=FL_CDC, *, selected=("app.orders",)):
    """A PostgreSQL cdc-stack deployed and never started; no Start CDC in this migration."""
    state = DataMigrationState()
    state.migration_type = mtype
    state.set_selection(TableSelection(selected_tables=list(selected)))
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.set_cdc_deploy_job_id("deploy-job", kind="infra")
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.job_id = "fl-job"
    return state


# --- [M1] an Append re-run before any Start CDC ------------------------------------------------


def test_the_engine_records_what_the_signs_read() -> None:
    # The first-attempt sign reads the engine's own bookkeeping: a run that takes a new start
    # point seeds 0 attempts and each start counts one; a retry seeds a re-run table with its
    # earlier count.
    job = MigrationJob(job_id="j")
    engine._seed_chunks(job, ["orders"])
    assert job.chunks[0].attempts == 0
    engine._start_chunk(job, "orders")
    assert job.chunks[0].attempts == 1
    retry = MigrationJob(job_id="r")
    engine._seed_retry_chunks(retry, job.chunks, {"orders"})
    assert retry.chunks[0].attempts == 1  # a re-run table carries its earlier attempt


@pytest.mark.parametrize("mtype, body", [(FL_CDC, _FOUND_FL_CDC), (CDC_ONLY, _FOUND_CDC_ONLY)])
def test_an_append_rerun_before_any_start_states_the_condition(monkeypatch, mtype, body) -> None:
    # FAILS WITHOUT THE FIX: no Start had stamped the ops window, so the card said "Automatic —
    # gapless from Full Load (recommended)" with no notice, the dialog showed no condition and
    # the Start's activity line certified "(gapless from the Full Load watermark)". (CDC only:
    # a Full load only Append re-run, then the type switched to CDC only.)
    from tests.test_ui_data_migration import _RecordingUi

    _fresh(monkeypatch, W1, _FIRST)  # the first copy; CDC was never started after it
    append = _current(_fresh(monkeypatch, W2, _APPENDED))
    assert {c.chunk_id: (c.attempts, c.rows_skipped) for c in append.chunks} == {
        "orders": (1, 10), "customers": (1, 3),
    }
    state = _mysql_never_started(mtype)
    card = _card(state, append)
    texts = " ".join(card.texts)
    assert _CONDITION_LABEL in texts and _TODAY_LABEL not in texts
    assert RELOAD_CONDITION_HEADER in texts and body in texts
    assert "This migration started CDC" not in texts  # no Start preceded this Full Load
    assert RELOAD_CONDITION_TOOLTIP in card.tooltips and _TODAY_TOOLTIP not in card.tooltips
    # Not a block: the start point stays set and ready.
    assert "Start point set — binlog mysql-bin.000100:4" in texts
    assert "Ready" in texts and "Action needed" not in texts
    ui = _dialog(monkeypatch, state, append)
    assert ("warning", RELOAD_CONDITION_HEADER) in ui.notices
    assert ui.bodies[RELOAD_CONDITION_HEADER] == body
    assert ui.buttons["Start CDC"].enabled is True
    hover = _RecordingUi()
    _cdc_ui._render_cdc_start_button(
        hover, state, _JM(append), lambda: None, inventory=None, session=None
    )
    assert _GAPLESS_SET not in " ".join(hover.tooltips)
    run = _click(monkeypatch, state, W2, append)
    assert run["submitted"] and run["error"] is None
    line = _start_line(run)
    assert "start point binlog mysql-bin.000100:4" in line
    assert RELOAD_CONDITION_LINE in line and _TODAY_LINE not in line


def test_postgres_states_the_condition_for_an_append_rerun_before_any_start(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Start point set — gapless from the replication slot", no notice.
    append = _current(_fresh(monkeypatch, PG_W2, {"app.orders": (1, 2890)}))
    state = _pg_never_started()
    assert state.cdc_ops_window_start is None
    card = _card(state, append, session=_pg_session())
    texts = " ".join(card.texts)
    assert "Start point set — from the replication slot (WAL LSN 0/5A000060)" in texts
    assert "gapless from the replication slot" not in texts
    assert RELOAD_CONDITION_HEADER in texts and _FOUND_FL_CDC in texts
    assert RELOAD_CONDITION_TOOLTIP in card.tooltips
    ui = _dialog(monkeypatch, state, append, session=_pg_session())
    assert ui.bodies[RELOAD_CONDITION_HEADER] == _FOUND_FL_CDC
    assert ui.buttons["Start CDC"].enabled is True
    # The Start's line takes the same signal (_start_cdc_deploy: recopied=...due(state, job)).
    line = _cdc_ui._cdc_start_detail(
        "dsql-cdc-e2e26pg", PG_W2, mode="auto", source_type=POSTGRES,
        recopied=drop_reload_condition_due(state, append),
    )
    assert RELOAD_CONDITION_LINE in line and _TODAY_LINE not in line
    # PostgreSQL CDC only: the re-run that creates the slot again is a Full load + CDC one.
    assert reload_condition(CDC_ONLY, postgres=True) == _FOUND_PG_CDC_ONLY


def test_each_sign_has_its_own_body() -> None:
    # The window sign keeps G27's body word for word; the first-attempt sign says only what the
    # Full Load found, which holds whichever sign shows.
    assert reload_condition(FL_CDC, started=True) == _FL_CDC_BODY
    assert reload_condition(FL_CDC, postgres=True, started=True) == _FL_CDC_BODY
    for postgres in (False, True):
        assert reload_condition(FL_CDC, postgres=postgres) == _FOUND_FL_CDC
    assert reload_condition(CDC_ONLY) == _FOUND_CDC_ONLY
    for mtype in (FL_CDC, CDC_ONLY):
        for postgres in (False, True):
            found = reload_condition(mtype, postgres=postgres)
            assert "This migration started CDC" not in found and "'Drop & reload'" in found
            assert len(found) < 600


def test_the_first_attempt_sign_on_the_engines_own_records(monkeypatch) -> None:
    first = _fresh(monkeypatch, W1, _FIRST)
    append = _fresh(monkeypatch, W2, _APPENDED)
    dropped = _fresh(monkeypatch, W2, {"orders": (11, 0), "customers": (3, 0)})  # Drop & reload
    state = DataMigrationState()
    assert drop_reload_condition_due(state, first) is False  # a first copy into empty tables
    assert drop_reload_condition_due(state, dropped) is False  # recreated: nothing already there
    assert drop_reload_condition_due(state, append) is True  # no Start needed
    assert cdc_started_before_load(state, append) is False
    # A Start from W1 before the re-run: both signs (the body then says so first).
    state.set_cdc_ops_window_start(T1)
    assert drop_reload_condition_due(state, append) is True
    assert cdc_started_before_load(state, append) is True
    assert drop_reload_condition_due(state, dropped) is False
    # A Start from W2 itself moved the window: the first-attempt sign stays (L1).
    state.set_cdc_ops_window_start(T2)
    assert cdc_started_before_load(state, append) is False
    assert drop_reload_condition_due(state, append) is True
    # The first-attempt sign belongs to the record: Start over builds the state afresh, with no
    # current Full Load (the surfaces look the record up by ``job_id``).
    state.job_id = append.job_id
    state.__init__()
    assert state.job_id is None and state.cdc_ops_window_start is None


# --- what stays: a first copy, a Drop & reload re-run, Retry, Reload, a G17 addition ----------


@pytest.mark.parametrize("window", [None, T1])
def test_a_first_copy_and_a_drop_and_reload_rerun_keep_todays_text(monkeypatch, window) -> None:
    # Guard, before any Start and after a Start from W1: a Full Load into empty tables, and a
    # Drop & reload re-run (it recreates each table, so it finds nothing already there).
    from dsql_migrator.ui.design import render_notice

    first = _current(_fresh(monkeypatch, W1, _FIRST))
    dropped = _current(_fresh(monkeypatch, W2, {"orders": (11, 0), "customers": (3, 0)}))
    for job, wm in ((first, W1), (dropped, W2)):
        state = _mysql_never_started()
        if window is not None:
            state.set_cdc_ops_window_start(window)
        # The previous click's capture records notices instead of rendering them.
        monkeypatch.setattr(_cdc_ui, "render_notice", render_notice)
        card = _card(state, job)
        texts = " ".join(card.texts)
        assert _TODAY_LABEL in texts and RELOAD_CONDITION_HEADER not in texts
        assert _TODAY_TOOLTIP in card.tooltips
        ui = _dialog(monkeypatch, state, job)
        assert RELOAD_CONDITION_HEADER not in [h for _t, h in ui.notices]
        run = _click(monkeypatch, state, wm, job)
        assert _TODAY_LINE in _start_line(run)


def test_retry_unfinished_reload_and_a_g17_addition_keep_todays_text(monkeypatch) -> None:
    # Guard: each table they re-run carries its earlier attempt, so the rows its re-read finds
    # -- this copy's own -- give no sign; the tables they carry keep the first copy's counts.
    first = _fresh(monkeypatch, W1, _FIRST)
    failed = _fresh(monkeypatch, W1, _FIRST, fail=("customers",))
    retried = _retry(monkeypatch, failed, {"customers": (1, 2)})  # Retry unfinished tables
    reloaded = _retry(monkeypatch, first, {"orders": (0, 10)})  # Reload specific tables (append)
    added = _retry(monkeypatch, first, {"items": (5, 0)}, additions=("items",))  # G17 addition
    assert added.watermark.cdc_added_tables == {"items": 5}
    assert {c.chunk_id: c.attempts for c in retried.chunks} == {"orders": 1, "customers": 2}
    assert {c.chunk_id: c.attempts for c in reloaded.chunks} == {"orders": 2, "customers": 1}
    for job in (retried, reloaded, added):
        assert job.table_error_job_ids  # a retry: it carries the prior run's tables
        for window in (None, T1):  # before any Start, and after a Start from W1 (this one)
            state = _mysql_never_started()
            if window is not None:
                state.set_cdc_ops_window_start(window)
            assert drop_reload_condition_due(state, job) is False
            texts = " ".join(_card(state, _current(job)).texts)
            assert _TODAY_LABEL in texts and RELOAD_CONDITION_HEADER not in texts


def test_a_retry_after_an_append_rerun_keeps_the_condition(monkeypatch) -> None:
    # The other side of the guard above: Retry unfinished tables completes the SAME copy, which
    # appended to the first one -- the table it carries found those rows on its first attempt.
    append_failed = _fresh(monkeypatch, W2, _APPENDED, fail=("customers",))
    retried = _retry(monkeypatch, append_failed, {"customers": (0, 3)})
    assert retried.status == "DONE"
    assert drop_reload_condition_due(DataMigrationState(), retried) is True


# --- [L1] a Start from the new watermark that fails, then a retry -------------------------------


@pytest.mark.parametrize("detour", [False, True])
def test_a_failed_start_then_a_retry_keeps_the_condition(monkeypatch, detour) -> None:
    # FAILS WITHOUT THE FIX: the Start submission moved the ops window to W2's time whatever its
    # outcome, so after it failed the card, the dialog and the retry's activity line were back to
    # today's gapless text. ``detour``: the G27 live scenario (a Start from W1 came first).
    from dsql_migrator.ui.design import render_notice

    append = _current(_fresh(monkeypatch, W2, _APPENDED))
    if detour:
        from tests.test_g27_cdc import _new_stack_after_the_detour

        state = _new_stack_after_the_detour(monkeypatch)
        assert state.cdc_ops_window_start == T1
    else:
        state = _mysql_never_started()
    failed = _click(
        monkeypatch, state, W2, append, run_raises=RuntimeError("connector create failed")
    )
    assert failed["submitted"] and failed["error"] is not None
    assert RELOAD_CONDITION_LINE in _start_line(failed)
    assert state.cdc_ops_window_start == T2  # the submission moved the window to W2
    # The capture records notices instead of rendering them; the card below renders.
    monkeypatch.setattr(_cdc_ui, "render_notice", render_notice)
    texts = " ".join(_card(state, append).texts)
    assert _CONDITION_LABEL in texts and _TODAY_LABEL not in texts
    assert RELOAD_CONDITION_HEADER in texts and _FOUND_FL_CDC in texts
    ui = _dialog(monkeypatch, state, append)
    assert ui.bodies[RELOAD_CONDITION_HEADER] == _FOUND_FL_CDC
    retry = _click(monkeypatch, state, W2, append)
    assert retry["submitted"] and retry["error"] is None
    assert RELOAD_CONDITION_LINE in _start_line(retry) and _TODAY_LINE not in _start_line(retry)


# --- [L3] PostgreSQL: the re-snapshot causes whose way back is a resume from the slot ---------

_MANUAL_RECOPIED_CAUSE = (
    "A resume from this load's slot is available here -- this Full Load created the replication "
    "slot -- but it is gapless only if that Full Load did not append to rows already on the "
    "target. Manual takes a fresh copy instead, which is a legitimate choice but reads the source "
    "again."
)
_MANUAL_REMEDY = (
    "Switch the start point back to Automatic to resume from the slot with no re-read: use "
    "\"Resume from the Full Load's slot instead\" on the CDC start point card."
)
_UNFINISHED_RECOPIED_REMEDY = (
    "Retry the failed tables first: once the load reaches DONE the slot is honoured again and the "
    "resume from it comes back on its own, with no re-read -- gapless only if this Full Load did "
    "not append to rows already on the target. If it did, {rerun} instead{note}."
)
_FL_CDC_RERUN = "Re-run Full Load on the Full Load step choosing 'Drop & reload'"
_PG_CDC_ONLY_RERUN = (
    "switch the migration type to Full load + CDC and Re-run Full Load there choosing 'Drop & "
    "reload'"
)
_PG_CDC_ONLY_NOTE = " (it creates the slot again at its own snapshot point)"


@pytest.mark.parametrize(
    "mtype, rerun, note",
    [(FL_CDC, _FL_CDC_RERUN, ""), (CDC_ONLY, _PG_CDC_ONLY_RERUN, _PG_CDC_ONLY_NOTE)],
)
def test_the_pg_manual_resnapshot_states_the_condition(monkeypatch, mtype, rerun, note) -> None:
    # FAILS WITHOUT THE FIX (the reviewed card): "A gapless resume IS available here" beside an ⓘ
    # saying the start is gapless only if that Full Load did not append.
    from tests.test_g27_cdc import _job, _pg_new_stack

    remedy = f"{_MANUAL_REMEDY} If that Full Load appended, {rerun} before Start CDC{note}."
    # The detour (a Start from W1 came first), and an Append re-run before any Start.
    for state, job in (
        (_pg_new_stack(mtype), _job(PG_W2)),
        (_pg_never_started(mtype), _current(_fresh(monkeypatch, PG_W2, {"app.orders": (1, 2890)}))),
    ):
        state.set_cdc_start_mode("manual")
        card = _card(state, job, session=_pg_session())
        texts = " ".join(card.texts)
        assert "Manual re-snapshots instead of resuming from this load's slot" in texts
        assert "A gapless resume IS available here" not in texts
        assert _MANUAL_RECOPIED_CAUSE in texts and remedy in texts
        assert _cdc_ui.PG_RESUME_FROM_SLOT_ACTION in [b.text for b in card.buttons]
        assert RELOAD_CONDITION_TOOLTIP in card.tooltips
        assert "Start point set — re-snapshot every table (snapshot.mode=initial)" in texts


def test_the_pg_manual_resnapshot_after_a_first_copy_keeps_todays_text(monkeypatch) -> None:
    # Guard: the Full Load is this migration's first copy, so the resume from its slot is gapless.
    first = _current(_fresh(monkeypatch, PG_W2, {"app.orders": (2890, 0)}))
    state = _pg_never_started()
    state.set_cdc_start_mode("manual")
    texts = " ".join(_card(state, first, session=_pg_session()).texts)
    assert "A gapless resume IS available here" in texts
    assert _MANUAL_RECOPIED_CAUSE not in texts and "If that Full Load appended" not in texts
    assert _cdc_ui.pg_resnapshot_notice("manual")[1] in texts  # today's notice, word for word


# Both tables are captured (selected and covered), so the load is unfinished for CDC.
_PG_W2_BOTH = PG_W2.model_copy(update={"table_row_counts": {"app.orders": 2890, "app.items": 40}})


def _pg_unfinished(monkeypatch, orders):
    """A PostgreSQL Full load + CDC Full Load whose ``app.items`` failed: its slot is not used."""
    job = _fresh(
        monkeypatch, _PG_W2_BOTH, {"app.orders": orders, "app.items": (0, 0)}, fail=("app.items",)
    )
    assert job.status == "FAILED"
    return _current(job)


def _deploy_brief(monkeypatch, state, job) -> str:
    """The re-snapshot disclosure the REAL Deploy CDC infrastructure dialog adds to its cost box
    (``_render_cdc_cost_estimate``'s ``extra``), its probes patched out."""
    import asyncio

    import nicegui.run as nrun

    from dsql_migrator.core.cdc import MskSeedAdmission
    from tests.test_ui_data_migration import _capture_grouped_notices, _DialogUi

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    extras: list = []
    monkeypatch.setattr(nrun, "io_bound", _io_bound)
    monkeypatch.setattr(_cdc_ui, "cdc_deploy_connection_blocker", lambda *_a, **_k: None)
    monkeypatch.setattr(
        _cdc_ui, "_diagnose_for_dialog", lambda *_a, **_k: ("Found subnets.", "discovered", "")
    )
    monkeypatch.setattr(_cdc_ui, "_source_vpc_warning_for_dialog", lambda *_a, **_k: ("", ""))
    monkeypatch.setattr(_cdc_ui, "_msk_seed_admission", lambda *_a, **_k: MskSeedAdmission())
    monkeypatch.setattr(_cdc_ui, "_probe_cdc_blocking_foreign_keys", lambda *_a, **_k: None)
    monkeypatch.setattr(_cdc_ui, "_render_notice", lambda _ui, **_k: None)
    _capture_grouped_notices(monkeypatch, _cdc_ui, [])
    monkeypatch.setattr(
        _cdc_ui, "_render_cdc_cost_estimate", lambda _ui, **k: extras.append(k.get("extra", ""))
    )
    asyncio.run(_cdc_ui._open_cdc_infra_dialog(
        _DialogUi(), state, lambda: None, session=_pg_session(), job=job,
    ))
    assert len(extras) == 1
    return extras[0]


def test_the_pg_unfinished_load_states_the_condition(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "the gapless resume comes back on its own" for an unfinished Append
    # re-run, on the card and in the Deploy dialog's brief.
    state = _pg_never_started(selected=("app.orders", "app.items"))
    job = _pg_unfinished(monkeypatch, (1, 2890))
    assert drop_reload_condition_due(state, job) is True
    texts = " ".join(_card(state, job, session=_pg_session()).texts)
    assert "This Full Load did not finish, so its replication slot is not used" in texts
    assert "the gapless resume comes back on its own" not in texts
    # FAILS WITHOUT THE R5 FIX: an unfinished load's Full Load step shows 'Retry unfinished
    # tables' and 'Re-run all tables', not 'Re-run Full Load' -- the remedy names the visible one.
    _visible_rerun = _FL_CDC_RERUN.replace("Re-run Full Load", "Re-run all tables")
    assert _visible_rerun.startswith("Re-run all tables")
    assert _UNFINISHED_RECOPIED_REMEDY.format(rerun=_visible_rerun, note="") in texts
    assert "Re-run Full Load" not in _UNFINISHED_RECOPIED_REMEDY.format(rerun=_visible_rerun, note="")
    brief = _deploy_brief(monkeypatch, state, job)
    assert (
        "Retry the failed tables first — the resume from the slot comes back on its own once the "
        "load reaches DONE, gapless only if this Full Load did not append to rows already on the "
        "target."
    ) in brief
    assert "the gapless resume comes back on its own" not in brief
    # CDC only (the same unfinished record): the re-run there needs the type switch.
    _, body = _cdc_ui.pg_resnapshot_notice(
        "unfinished_load", recopied=True, migration_type=CDC_ONLY
    )
    assert _UNFINISHED_RECOPIED_REMEDY.format(
        rerun=_PG_CDC_ONLY_RERUN.replace("Re-run Full Load", "Re-run all tables"),
        note=_PG_CDC_ONLY_NOTE,
    ) in body


def test_the_pg_unfinished_first_copy_keeps_todays_text(monkeypatch) -> None:
    # Guard: an unfinished FIRST copy keeps the promise -- once the retry completes it, the resume
    # from its slot is gapless.
    state = _pg_never_started(selected=("app.orders", "app.items"))
    job = _pg_unfinished(monkeypatch, (2890, 0))
    assert drop_reload_condition_due(state, job) is False
    texts = " ".join(_card(state, job, session=_pg_session()).texts)
    assert "the gapless resume comes back on its own, with no re-read." in texts
    brief = _deploy_brief(monkeypatch, state, job)
    assert "the gapless resume comes back on its own once the load reaches DONE." in brief


# --- [GTID-only] the re-run that takes a new copy names Drop & reload --------------------------

_GTID_ONLY = Watermark(
    snapshot_timestamp=T1, gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
)


def _gtid_only_notice(state) -> str:
    job = MigrationJob(
        job_id="fl-job", status="DONE", progress_pct=100.0, watermark=_GTID_ONLY,
        chunks=[ChunkState(chunk_id="orders", status="DONE", rows_loaded=10, attempts=1)],
    )
    state.job_id = "fl-job"
    return " ".join(_card(state, job).texts)


def test_the_gtid_only_notice_names_drop_and_reload(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: "Grant it to the source user and re-run the Full Load for a gapless
    # handoff" -- an Append re-run keeps the first load's rows as they were.
    from tests.test_g25_cdc_r7 import _rolled_back

    texts = _gtid_only_notice(_mysql_never_started())
    assert "This Full Load's watermark cannot give a gapless start" in texts
    assert (
        "Grant it to the source user and re-run the Full Load choosing 'Drop & reload' for a "
        "gapless handoff, or enter a binlog position manually if you know the coordinate the load "
        "started from."
    ) in texts
    assert "re-run the Full Load for a gapless handoff" not in texts
    # CDC only has no Full Load step.
    assert (
        "re-run the Full Load choosing 'Drop & reload' for a gapless handoff (CDC only has no Full "
        "Load step: switch the migration type to Full load only for that run, then back to CDC "
        "only), or enter a binlog position manually"
    ) in _gtid_only_notice(_mysql_never_started(CDC_ONLY))
    # After a Start CloudFormation rolled back: new CDC topics first, then the same re-run.
    assert (
        "Delete CDC infrastructure and deploy it again, then re-run the Full Load choosing 'Drop & "
        "reload' or enter the position, then Start CDC."
    ) in _gtid_only_notice(_rolled_back(offered=None))


# --- [L2] the PostgreSQL Delete dialog's secret line, as the teardown behaves ---------------------


def _teardown(monkeypatch, *, slot_dropped):
    """The REAL ``run_cdc_delete`` of a PostgreSQL stack with password auth (the tool-created
    secret), its slot drop faked to ``slot_dropped``. Returns ``(logs, secret deletions)``."""
    from dsql_migrator.core import cdc_deployer
    from tests.test_cdc_deploy_stages import _FakeDeployer, _FakeHandle, _logs
    from tests.test_g27_cdc import E2E26PG

    deleted: list = []
    monkeypatch.setattr(
        "dsql_migrator.core.secrets.delete_source_secret",
        lambda **kw: deleted.append(kw["stack_name"]) or "deleted",
    )
    monkeypatch.setattr(
        cdc_deployer, "_drop_pg_source_replication", lambda *_a, **_k: slot_dropped
    )
    logs, on_log = _logs()
    cdc_deployer.run_cdc_delete(
        _FakeHandle(), stack_name=E2E26PG,
        deployer=_FakeDeployer(
            existing=cdc_deployer.CdcStackDiscovery(
                "UPDATE_COMPLETE", {"EngineType": "postgres"}, True
            ),
            stack_statuses=("DELETE_IN_PROGRESS", None),
        ),
        on_log=on_log, region="ap-northeast-2", cleanup_source_secret=True,
        sleep=lambda _s: None, delete_timeout_seconds=5.0, poll_interval_seconds=0.0,
    )
    return logs, deleted


def test_the_pg_delete_dialog_says_a_slot_it_cannot_drop_keeps_the_secret(monkeypatch) -> None:
    # FAILS WITHOUT THE FIX: the dialog promised the deletion unconditionally.
    from dsql_migrator.core.secrets import cdc_source_secret_name
    from tests.test_g27_cdc import E2E26PG, _delete_dialog_texts, _pg_delete_state, _pg_load

    secret = cdc_source_secret_name(E2E26PG)
    texts = _delete_dialog_texts(_pg_delete_state(), _pg_load())  # password auth: no secret ref
    assert (
        f"The source-credentials secret the tool created for CDC ('{secret}') will also be "
        "scheduled for deletion with a 7-day recovery window, so your database credentials do not "
        "linger in Secrets Manager. If the replication slot cannot be dropped, the teardown keeps "
        "the secret instead: it holds the credentials needed to drop the slot, so delete it "
        "yourself once the slot is gone."
    ) in texts
    # What the teardown does: a slot it dropped -> the secret is scheduled for deletion; one it
    # could not drop -> the secret is kept, with the same reason.
    _logs, deleted = _teardown(monkeypatch, slot_dropped=True)
    assert deleted == [E2E26PG]
    logs, deleted = _teardown(monkeypatch, slot_dropped=False)
    assert deleted == []
    assert any(
        f"Keeping the source-credentials secret '{secret}' in place: the replication slot could "
        "not be dropped, and these are the credentials needed to drop it. Delete the secret "
        "yourself once the slot is gone." in m for m in logs
    )
    # Secrets Manager auth: the customer's own secret is untouched, as before.
    sm = SimpleNamespace(
        source_config=SimpleNamespace(source_type=POSTGRES, database="app"),
        source_secret_id="arn:aws:secretsmanager:ap-northeast-2:111122223333:secret:src",
        source_password=None, target_config=None, aws_profile=None,
    )
    texts = _delete_dialog_texts(_pg_delete_state(), _pg_load(), session=sm)
    assert "Your Secrets Manager source secret is left untouched (the tool never created it)." in (
        texts
    )
    assert not any("teardown keeps the secret" in t for t in texts)


def test_the_mysql_delete_dialog_secret_line_is_unchanged() -> None:
    from tests.test_g27_cdc import _MYSQL_DELETE_DIALOG
    from tests.test_ui_data_migration import _RecordingUi

    state = DataMigrationState()
    state.set_cdc_stack_name("dsql-cdc-e2e26")
    ui = _RecordingUi()
    _cdc_ui._open_cdc_delete_dialog(
        ui, state, lambda: None,
        session=SimpleNamespace(
            source_config=SimpleNamespace(source_type=MYSQL, database="app"),
            source_secret_id=None,
        ),
    )
    assert ui.texts == _MYSQL_DELETE_DIALOG


# --- [G27 repair review R4] the Will-stream narrowing tooltip makes no gapless claim -----------


def test_the_narrowing_tooltip_tail_follows_the_condition() -> None:
    # FAILS WITHOUT THE FIX: with the selection narrowed, the 'Will stream' info tooltip ended
    # "which is what makes the handoff gapless" even where the card states the Drop & reload
    # condition. The gapless tail is now chosen only where no condition is due (pinned in the
    # source: the renderer needs a full CDC card to reach the narrowing branch).
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = " ".join(inspect.getsource(_cdc_ui._render_cdc_start_button).split())
    assert (
        "START_TOOLTIP_NARROWING_TAIL if not _resnapshots and not "
        "drop_reload_condition_due(migration_state, job) else "
        "START_TOOLTIP_NARROWING_TAIL_RESNAPSHOT"
    ) in src
    assert "gapless" in _cdc_ui.START_TOOLTIP_NARROWING_TAIL
    assert "gapless" not in _cdc_ui.START_TOOLTIP_NARROWING_TAIL_RESNAPSHOT
