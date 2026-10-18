# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G129: a table re-loaded while CDC streams cannot bring back a row the stream deleted.

A Full Load beside a streaming CDC sink inserts only the rows missing on the target
(``SKIP_EXISTING``, the ``cdc_coexisting`` load). A row the source deletes while its table is
being copied reaches the sink first -- a ``DELETE`` that finds nothing, the copy has not
written it yet -- and the copy then writes it from its snapshot, where it stays: silently,
until a COUNT or CHECKSUM validation shows an extra row. Before this, Retry unfinished tables
and a per-table Reload stayed clickable while CDC streamed (their dialog offered "Re-run
anyway (CDC is live)"), the page header's Run passed a Start in flight on a stack that had
not streamed yet, and Start CDC stayed enabled while a Full Load ran. Stop CDC first, reload,
then Start CDC again converges: the restarted stream applies those deletes after the copy.

* The reason, on the real table path: a load beside the sink writes such a row back.
* The Full Load step: Retry unfinished tables, Reload specific tables, a per-table Reload and
  Re-run all tables are disabled while CDC is live, with Stop CDC first; a dialog that is open
  or opened anyway cannot be confirmed.
* The screen: the scoped retry (every Retry / Reload / Change & reload) and ``runner()`` refuse
  while CDC is live, before anything is built or submitted; after Stop CDC a retry runs.
* The CDC step: Start CDC waits while a Full Load runs, at the button and at the job.

Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import SourceType, StepStatus
from dsql_migrator.ui.data_migration import _cdc_ui
from dsql_migrator.ui.data_migration import _full_load_ui as fl
from dsql_migrator.ui.data_migration._cdc_ui import (
    START_LOAD_RUNNING_BODY,
    START_LOAD_RUNNING_HEADER,
    full_load_running,
)

from tests.test_g12_table_set_after_load import _c, _job, _loaded
from tests.test_g25_fl_r6 import (
    NEVER_STARTED,
    RUNNING,
    STOPPED,
    _btn,
    _click,
    _failed_b,
    _probe,
    _render,
    _state,
)

MY, PG = SourceType.MYSQL, SourceType.POSTGRES


# ---------------------------------------------------------------------------
# 1. The reason: a load beside the sink writes back a row the sink deleted
# ---------------------------------------------------------------------------


def test_a_load_beside_a_streaming_sink_writes_back_a_row_the_sink_deleted() -> None:
    # Why every gate below exists, on the real table path (BatchedTableMigrator ->
    # BatchedImporter SKIP_EXISTING): the source deletes id 3 after the copy's snapshot read
    # it, and a caught-up sink applies that DELETE before the copy writes id 3.
    from dsql_migrator.core.batched_import import BatchedImportOptions, OnConflictMode
    from dsql_migrator.ui.data_migration._full_load_engine import BatchedTableMigrator

    from tests.test_batched_import import _FakeStore, _importer
    from tests.test_ui_data_migration import (
        _FakeExporter,
        _FakeWatermarkCapturer,
        _inputs,
        _tables,
        _watermark,
    )

    store = _FakeStore()
    snapshot = [{"id": i} for i in range(1, 7)]

    class _SnapshotWithDelete(_FakeExporter):
        def stream_converted_rows(self, conn, table, **kwargs):
            super().stream_converted_rows(conn, table, **kwargs)

            def rows():
                yield from snapshot[:2]
                store.rows.pop((3,), None)  # the sink's DELETE ... WHERE id = 3: nothing yet
                yield from snapshot[2:]

            return rows()

    modes: list = []

    def _factory(_inputs_):
        importer = _importer(store, ["id"], ["id"], options=BatchedImportOptions(
            batch_size=2, parallelism=1))
        real = importer.import_rows
        importer.import_rows = lambda rows, table, **kw: (
            modes.append(kw.get("on_conflict")) or real(rows, table, **kw))
        return importer

    # Drop & reload was chosen, but beside a live sink the load never DROPs: it appends.
    inputs = dataclasses.replace(
        _inputs(), replace_tables=frozenset({"orders"}), cdc_coexisting=True
    )
    migrator = BatchedTableMigrator(
        inputs,
        exporter=_SnapshotWithDelete(),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=_factory,  # type: ignore[arg-type]
        table_recreator=lambda _t: pytest.fail("no DROP beside a live sink"),
        target_generated_reader=lambda _t: set(),
    )
    migrator.migrate_table(_tables()[0])
    assert modes == [OnConflictMode.SKIP_EXISTING]
    # Deleted on the source and by the sink -- and back on the target, with no error.
    assert sorted(key for (key,) in store.rows) == [1, 2, 3, 4, 5, 6]


# ---------------------------------------------------------------------------
# 2. The Full Load step while CDC is live
# ---------------------------------------------------------------------------


def _notice_body(ui, header: str) -> str:
    texts = ui.texts
    return texts[texts.index(header) + 1]


@pytest.mark.parametrize("engine", [MY, PG])
def test_retry_unfinished_is_disabled_while_cdc_streams(monkeypatch, engine) -> None:
    # FAILS WITHOUT THE FIX: enabled, and its dialog's "Re-run anyway (CDC is live)" ran the
    # retry beside the sink.
    ui, rec = _render(monkeypatch, state=_state(monkeypatch, RUNNING), job=_failed_b(),
                      engine=engine, status=StepStatus.FAILED)
    retry = _btn(ui, "Retry unfinished tables")
    assert retry.enabled is False and retry.tips == [fl._CDC_LIVE_RELOAD_TOOLTIP]
    assert "Stop CDC first (CDC step → Stop CDC)" in fl._CDC_LIVE_RELOAD_TOOLTIP
    # Visible, not hover-only: the hint below the row names the route.
    assert fl._CDC_LIVE_RETRY_HINT in ui.texts


@pytest.mark.parametrize("engine", [MY, PG])
def test_a_retry_dialog_reached_while_cdc_streams_cannot_be_confirmed(
    monkeypatch, engine
) -> None:
    # A stale button (CDC started after the render) still opens the dialog: it says why
    # nothing can run, and its confirm is disabled and runs nothing.
    ui, rec = _render(monkeypatch, state=_state(monkeypatch, RUNNING), job=_failed_b(),
                      engine=engine, status=StepStatus.FAILED)
    _click(ui, "Retry unfinished tables")
    assert ui.opened == 1
    body = _notice_body(ui, "CDC is currently streaming")
    assert body == f"{fl._CDC_LIVE_COLLISION} {fl._CDC_LIVE_KEEPS_START_POINT}"
    assert "after CDC has applied that delete" in body
    confirm = ui.btns[-1]
    assert confirm.text == "Confirm and start" and confirm.enabled is False
    assert not [b for b in ui.btns if "anyway" in b.text]
    ui.handlers[-1][1]()
    assert rec.retried == [] and rec.started == 0


@pytest.mark.parametrize("engine", [MY, PG])
def test_a_dialog_opened_before_cdc_started_is_refused_at_its_confirm(monkeypatch, engine):
    # CDC can start while the dialog is open: the confirm re-checks, closes as a Cancel.
    state = _state(monkeypatch, STOPPED)
    ui, rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                      status=StepStatus.FAILED)
    _click(ui, "Retry unfinished tables")
    assert ui.opened == 1 and ui.btns[-1].enabled is True
    _probe(monkeypatch, state, RUNNING)  # Start CDC landed meanwhile (another tab)
    ui.handlers[-1][1]()
    assert rec.retried == []
    assert ui.notified == [fl._CDC_LIVE_RELOAD_TOOLTIP]


@pytest.mark.parametrize("engine", [MY, PG])
def test_reload_specific_tables_while_cdc_streams_names_the_write_back(monkeypatch, engine):
    ui, _rec = _render(monkeypatch, state=_state(monkeypatch, RUNNING), job=_loaded(),
                       engine=engine, status=StepStatus.DONE)
    reload_btn = _btn(ui, "Reload specific tables")
    assert reload_btn.enabled is False and reload_btn.tips == [fl._CDC_LIVE_RELOAD_TOOLTIP]


@pytest.mark.parametrize("engine", [MY, PG])
def test_re_run_all_tables_during_a_start_in_flight_is_disabled(monkeypatch, engine) -> None:
    # FAILS WITHOUT THE FIX: a Start in flight on a stack that has not streamed yet passes the
    # new-start-point gate, so Re-run all tables stayed enabled while CDC came up.
    state = _state(monkeypatch, NEVER_STARTED)
    ui, _rec = _render(monkeypatch, state=state, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED, cdc_live=True)
    rerun = _btn(ui, "Re-run all tables")
    assert rerun.enabled is False and rerun.tips == [fl._CDC_LIVE_START_TOOLTIP]


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_step_hands_the_per_table_reload_its_cdc_live_block(monkeypatch, engine) -> None:
    # FAILS WITHOUT THE FIX: the quarantine card's Reload stayed enabled while CDC streamed.
    seen: list = []
    real = fl._render_full_load_progress

    def _progress(*a, **kw):
        seen.append(kw.get("reload_block"))
        return real(*a, **kw)

    monkeypatch.setattr(fl, "_render_full_load_progress", _progress)
    quarantined = _job("DONE", [_c("app.a", rows_quarantined=1), _c("app.b")],
                       counts={"app.a": 2, "app.b": 1})
    _render(monkeypatch, state=_state(monkeypatch, RUNNING), job=quarantined, engine=engine)
    _render(monkeypatch, state=_state(monkeypatch, STOPPED), job=quarantined, engine=engine)
    assert seen == [fl._CDC_LIVE_RELOAD_TOOLTIP, None]


def test_a_blocked_per_table_reload_is_shown_disabled_with_its_reason() -> None:
    from dsql_migrator.ui.data_migration import (
        _render_full_load_progress,
        build_full_load_table_rows,
    )

    from tests.test_ui_data_migration import _quar_render_ui

    _Ui, _qjob, reason = _quar_render_ui()
    rows = build_full_load_table_rows(_qjob(), None, {"ecommerce.product_media": reason})

    def _reload_buttons(**kw):
        ui = _Ui()
        _render_full_load_progress(
            ui, _qjob(), rows, quarantine_only=True,
            quarantine_records=[("ecommerce.product_media", reason)], connections_ready=True,
            reload_confirm=lambda *_a: None, **kw,
        )
        return [b for b in ui.buttons if b[0] == "Reload"]

    (blocked,) = _reload_buttons(reload_block=fl._CDC_LIVE_RELOAD_TOOLTIP)
    assert "disable" in blocked[1] and blocked[2] == fl._CDC_LIVE_RELOAD_TOOLTIP
    (enabled,) = _reload_buttons()
    assert "disable" not in enabled[1]


# ---------------------------------------------------------------------------
# 3. The screen: the scoped retry and runner() refuse while CDC is live
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("action", ["Retry unfinished tables", "Reload specific tables",
                                    "Reload"])
def test_a_retry_or_reload_while_cdc_streams_is_refused_before_anything_is_built(
    monkeypatch, engine, action
) -> None:
    # FAILS WITHOUT THE FIX: the retry ran with cdc_coexisting=True beside the sink.
    from dsql_migrator.ui import data_migration as dm

    from tests.test_g25_wire_r6b import _KEEPS_START_POINT, _RunningJobs, _screen
    from tests.test_g25_wire_r6b import _step_callbacks, _toasts

    make_job, status, call, reloaded = _KEEPS_START_POINT[action]
    retries: list = []
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, _prior, tables, **kw: retries.append(
                            ([t.name for t in tables], kw)))
    job = make_job()
    live = _screen(monkeypatch, engine, job=job, status=status, stack=RUNNING,
                   jobs=_RunningJobs({"job-1": job}))
    call(_step_callbacks(monkeypatch, live))
    assert retries == [] and live.built == [] and live.jobs.submitted == []
    assert _toasts(live.ui) == [fl._CDC_LIVE_RELOAD_TOOLTIP]
    assert live.state.job_id == "job-1" and live.state.error is None
    # After Stop CDC it runs -- never beside the sink, so nothing it copies is written back.
    job = make_job()
    stopped = _screen(monkeypatch, engine, job=job, status=status, stack=STOPPED,
                      jobs=_RunningJobs({"job-1": job}))
    call(_step_callbacks(monkeypatch, stopped))
    ((tables, kwargs),) = retries
    assert tables == reloaded and kwargs["inputs"].cdc_coexisting is False


@pytest.mark.parametrize("engine", [MY, PG])
def test_a_header_run_during_a_start_in_flight_is_refused(monkeypatch, engine) -> None:
    # FAILS WITHOUT THE FIX: the stack has not streamed yet, so the new-start-point gate let the
    # page header's Run submit a load with cdc_coexisting=True while the Start came up.
    from dsql_migrator.core.models import MigrationJob

    from tests.test_g25_wire_r6b import _record_new_start_point_runs, _screen, _toasts

    s = _screen(monkeypatch, engine, stack=NEVER_STARTED)
    s.jobs.jobs["cdc-1"] = MigrationJob(job_id="cdc-1", status="RUNNING")
    s.state.set_cdc_deploy_job_id("cdc-1", kind="start")
    runs = _record_new_start_point_runs(monkeypatch)
    s.header_run()
    assert runs == [] and s.built == [] and s.jobs.submitted == []
    # DELIBERATE G132 UPDATE: the refusal says to wait for the Start, not "CDC is streaming --
    # Stop CDC first" (tests/test_g132_cdc_operation_waits.py).
    assert _toasts(s.ui) == [fl._CDC_OP_WAIT_START["start"]]


# ---------------------------------------------------------------------------
# 4. The CDC step: Start CDC waits while a Full Load runs
# ---------------------------------------------------------------------------


def _start_state():
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.job_id = "job-1"
    return state


def _load(status):
    from datetime import datetime, timezone

    from dsql_migrator.core.cdc import Watermark

    return SimpleNamespace(status=status, chunks=[], watermark=Watermark(
        snapshot_timestamp=datetime(2026, 10, 1, tzinfo=timezone.utc),
        binlog_file="mysql-bin-changelog.000007", binlog_position=108063011,
    ))


class _JM:
    def __init__(self, job):
        self.job = job
        self.submitted: list = []

    def get_status(self, _job_id):
        return self.job

    def submit(self, work, **_kw):
        self.submitted.append(work)
        return "cdc-1"


@pytest.mark.parametrize("status", ["PENDING", "RUNNING"])
def test_start_cdc_waits_while_the_full_load_runs(status) -> None:
    # FAILS WITHOUT THE FIX: "Ready to start CDC" with Start enabled while the load still
    # copied -- the sink would then delete rows the copy writes back.
    from tests.test_ui_data_migration import _StartButtonUi

    ui = _StartButtonUi()
    _cdc_ui._render_cdc_start_button(ui, _start_state(), _JM(_load(status)), lambda: None,
                                     inventory=None, session=None)
    assert ui.start_disabled is True
    assert START_LOAD_RUNNING_HEADER in ui.texts and START_LOAD_RUNNING_BODY in ui.texts
    assert "Ready to start CDC" not in ui.texts
    # Once it has finished, Start as before.
    done = _StartButtonUi()
    _cdc_ui._render_cdc_start_button(done, _start_state(), _JM(_load("DONE")), lambda: None,
                                     inventory=None, session=None)
    assert done.start_disabled is False and START_LOAD_RUNNING_HEADER not in done.texts


def test_the_start_cdc_job_is_not_submitted_while_the_full_load_runs(monkeypatch) -> None:
    # The backstop: Retry CDC reaches _start_cdc_deploy without the Start button.
    notices: list = []
    monkeypatch.setattr(_cdc_ui, "render_notice",
                        lambda _ui, **kw: notices.append((kw["tone"], kw["header"])))
    jm = _JM(_load("RUNNING"))
    _cdc_ui._start_cdc_deploy(SimpleNamespace(notify=lambda *a, **k: None), _start_state(),
                              jm, lambda: None, inventory=None, session=None)
    assert jm.submitted == [] and notices == [("error", START_LOAD_RUNNING_HEADER)]


def test_full_load_running_reads_only_an_active_job() -> None:
    for status, running in (("PENDING", True), ("RUNNING", True), ("DONE", False),
                            ("FAILED", False), ("CANCELLED", False)):
        assert full_load_running(SimpleNamespace(status=status)) is running
    assert full_load_running(None) is False
