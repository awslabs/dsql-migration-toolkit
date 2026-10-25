# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G132: Retry and Reload say why they wait while a CDC Start, Stop or Delete runs, and every
screen, guide and comment tells the same story about loads beside a live sink.

G129 made the Full Load step run no load while CDC is live: Retry unfinished tables, Reload
specific tables, the per-table Reload, Re-run all tables, Start / Re-run Full Load and the page
header's Run are refused while ``cdc_streaming_started`` holds. That predicate also holds while
this session's Start CDC, Stop CDC or Delete CDC infrastructure job runs, and each refusal then
said "CDC is streaming -- Stop CDC first": untrue during a Delete of a stack that never
streamed, and an instruction the CDC step cannot follow while a Stop (or any operation) runs --
it shows only that job's progress. The refusal stays; the reason is now the wait for the
operation, an info hint where the step shows one.

* The pure helpers: which operation is in flight, and the reason (both engines, both kinds).
* The Full Load step on both engines, for each operation: every gated action, its hint, and
  a dialog opened anyway; a dialog opened before the operation started is refused with it.
* The screen backstops: the scoped retry and ``runner()`` refuse with the wait.
* A streaming stack with a failed table: the G25 notice no longer calls Retry "not affected"
  right under the hint that says it is disabled.
* The AI's Full Load grounding and migration context name the CDC-live caveat and the route.
* No comment or screen presents a load beside a live sink as available; the manual says the
  same in English, Korean and Japanese.

Doubles only: no AWS, no source, no Aurora DSQL.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace

import pytest

from dsql_migrator.core.models import MigrationJob, SourceType, StepStatus
from dsql_migrator.ui.data_migration import _full_load_ui as fl
from dsql_migrator.ui.data_migration._cdc_monitoring import cdc_operation_in_flight

from tests.test_g12_table_set_after_load import _c, _job, _Jobs, _loaded
from tests.test_g25_docs import LANGS, _has, _page
from tests.test_g25_docs_r4 import _stale
from tests.test_g25_fl_r6 import NEVER_STARTED, RUNNING, STOPPED, _btn, _click, _failed_b, _state

MY, PG = SourceType.MYSQL, SourceType.POSTGRES
ROOT = pathlib.Path(__file__).resolve().parents[1]

# Each operation on the stack it runs against: a Delete of a stack that never streamed (no sink
# exists at all), a Stop of a streaming stack, and a Start of a stopped one.
OPERATIONS = {"delete": NEVER_STARTED, "stop": RUNNING, "start": STOPPED}
_WAIT = "is in progress -- wait for it to finish (its progress is on the CDC step)"


def _running(job_id="cdc-1"):
    return MigrationJob(job_id=job_id, status="RUNNING")


# ---------------------------------------------------------------------------
# 1. The pure helpers
# ---------------------------------------------------------------------------


def _bare(kind=None, *, status="RUNNING", phase_status=None):
    state = SimpleNamespace(
        cdc_action_kind=kind, cdc_deploy_job_id="cdc-1" if kind else None,
        cdc_stack_phase_status=phase_status,
    )
    return state, _Jobs({"cdc-1": MigrationJob(job_id="cdc-1", status=status)})


@pytest.mark.parametrize("kind", ["start", "stop", "delete"])
def test_the_operation_in_flight_is_named_only_while_its_job_runs(kind) -> None:
    for status in ("PENDING", "RUNNING"):
        assert cdc_operation_in_flight(*_bare(kind, status=status)) == kind
    for status in ("DONE", "FAILED", "CANCELLED"):
        assert cdc_operation_in_flight(*_bare(kind, status=status)) is None


def test_an_infra_deploy_is_no_operation_and_a_probed_deleting_stack_is_a_delete() -> None:
    # The infra create makes no connector (cdc_streaming_started excludes it); a stack the probe
    # reads as DELETE_IN_PROGRESS (a restored session) is a delete without a job.
    assert cdc_operation_in_flight(*_bare("infra")) is None
    assert cdc_operation_in_flight(*_bare()) is None
    assert cdc_operation_in_flight(*_bare(phase_status="DELETE_IN_PROGRESS")) == "delete"


@pytest.mark.parametrize("keeps", [True, False], ids=["retry-or-reload", "new-start-point"])
@pytest.mark.parametrize("kind", list(OPERATIONS))
def test_the_hold_names_the_operation_and_never_stop_cdc_first_for_a_teardown(
    monkeypatch, kind, keeps
) -> None:
    # FAILS WITHOUT THE FIX: "CDC is streaming -- ... Stop CDC first (CDC step → Stop CDC)".
    state = _state(monkeypatch, OPERATIONS[kind])
    state.set_cdc_deploy_job_id("cdc-1", kind=kind)
    jobs = _Jobs({"cdc-1": _running()})
    reason, operation = fl.cdc_load_hold(state, jobs, keeps_watermark=keeps)
    assert operation == kind
    assert reason == (fl._CDC_OP_WAIT_RELOAD if keeps else fl._CDC_OP_WAIT_START)[kind]
    assert _WAIT in reason and "CDC is streaming" not in reason
    assert "Stop CDC first (CDC step → Stop CDC)" not in reason
    if kind != "start":
        assert "Stop CDC first" not in reason  # nothing to stop: it is stopping, or going
    # Once the job has ended, the state itself decides again.
    state.set_cdc_deploy_job_id(None)
    hold = fl.cdc_load_hold(state, jobs, keeps_watermark=keeps)
    if kind == "stop":  # the probe still reads a running stack until the next probe
        assert hold == (fl._CDC_LIVE_RELOAD_TOOLTIP if keeps else fl._CDC_LIVE_START_TOOLTIP,
                        None)
    else:
        assert hold is None


def test_the_hold_while_cdc_streams_is_unchanged(monkeypatch) -> None:
    # Guard: with no operation running, a streaming stack keeps G129's "Stop CDC first".
    state = _state(monkeypatch, RUNNING)
    assert fl.cdc_load_hold(state, _Jobs(), keeps_watermark=True) == (
        fl._CDC_LIVE_RELOAD_TOOLTIP, None)
    assert fl.cdc_load_hold(state, _Jobs(), keeps_watermark=False) == (
        fl._CDC_LIVE_START_TOOLTIP, None)
    assert fl.cdc_load_hold(_state(monkeypatch, STOPPED), _Jobs(), keeps_watermark=True) is None


def test_a_delete_of_a_streaming_stack_is_a_wait_too(monkeypatch) -> None:
    # Delete is offered on a running stack: its connectors still stream until it removes them,
    # and nothing will stream afterwards, so the reason is the wait, not Stop CDC first.
    state = _state(monkeypatch, RUNNING)
    state.set_cdc_deploy_job_id("cdc-1", kind="delete")
    assert fl.cdc_load_hold(state, _Jobs({"cdc-1": _running()}), keeps_watermark=True) == (
        fl._CDC_OP_WAIT_RELOAD["delete"], "delete")


# ---------------------------------------------------------------------------
# 2. The Full Load step while an operation runs
# ---------------------------------------------------------------------------


def _render_during(monkeypatch, kind, *, job, engine, status, extra=None):
    """The real Full Load step (tests.test_g25_fl_r6._render) while this session's CDC
    ``kind`` job runs (``kind`` None: no job yet -- ``extra`` can add one later), recording the
    inline hints and notices it draws with their tones."""
    import tests.test_g25_fl_r6 as r6

    state = _state(monkeypatch, OPERATIONS[kind] if kind else STOPPED)
    extra = extra if extra is not None else {}
    if kind:
        state.set_cdc_deploy_job_id("cdc-1", kind=kind)
        extra["cdc-1"] = _running()

    class _WithCdc(r6._Jobs):
        def get_status(self, job_id):
            return extra[job_id] if job_id in extra else super().get_status(job_id)

    hints: list = []
    notices: list = []
    real_hint, real_notice = fl.inline_hint, fl.render_notice

    def _hint(ui, text, *, tone="neutral", **kw):
        hints.append((str(text), tone))
        return real_hint(ui, text, tone=tone, **kw)

    def _notice(ui, *, tone, header, body, **kw):
        notices.append((tone, header, body))
        return real_notice(ui, tone=tone, header=header, body=body, **kw)

    monkeypatch.setattr(r6, "_Jobs", _WithCdc)
    monkeypatch.setattr(fl, "inline_hint", _hint)
    monkeypatch.setattr(fl, "render_notice", _notice)
    ui, rec = r6._render(monkeypatch, state=state, job=job, engine=engine, status=status)
    return SimpleNamespace(ui=ui, rec=rec, state=state, hints=hints, notices=notices,
                           extra=extra)


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("kind", list(OPERATIONS))
def test_retry_and_re_run_all_tables_say_to_wait_for_the_operation(
    monkeypatch, engine, kind
) -> None:
    # FAILS WITHOUT THE FIX: both disabled with "CDC is streaming ... Stop CDC first", under the
    # warning hint "CDC is live, so Retry unfinished tables is disabled" -- during a Delete of a
    # stack that never streamed, too.
    s = _render_during(monkeypatch, kind, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED)
    retry = _btn(s.ui, "Retry unfinished tables")
    assert retry.enabled is False and retry.tips == [fl._CDC_OP_WAIT_RELOAD[kind]]
    rerun = _btn(s.ui, "Re-run all tables")
    assert rerun.enabled is False and rerun.tips == [fl._CDC_OP_WAIT_START[kind]]
    # Visible, and an expected wait: an info hint, not the CDC-live warning.
    assert (fl._CDC_OP_WAIT_RELOAD[kind], "info") in s.hints
    assert fl._CDC_LIVE_RETRY_HINT not in s.ui.texts
    assert not [t for t in s.ui.texts if t.startswith("CDC is streaming")]


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("kind", list(OPERATIONS))
def test_reload_specific_tables_and_the_per_table_reload_say_to_wait(
    monkeypatch, engine, kind
) -> None:
    seen: list = []
    real = fl._render_full_load_progress

    def _progress(*a, **kw):
        seen.append(kw.get("reload_block"))
        return real(*a, **kw)

    monkeypatch.setattr(fl, "_render_full_load_progress", _progress)
    s = _render_during(monkeypatch, kind, job=_loaded(), engine=engine, status=StepStatus.DONE)
    reload_btn = _btn(s.ui, "Reload specific tables")
    assert reload_btn.enabled is False and reload_btn.tips == [fl._CDC_OP_WAIT_RELOAD[kind]]
    assert seen == [fl._CDC_OP_WAIT_RELOAD[kind]]


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("kind", list(OPERATIONS))
@pytest.mark.parametrize("label", ["Start Full Load", "Re-run Full Load"])
def test_start_and_re_run_full_load_say_to_wait(monkeypatch, engine, kind, label) -> None:
    job, status = (None, StepStatus.NOT_STARTED) if label == "Start Full Load" else (
        _loaded(), StepStatus.DONE)
    s = _render_during(monkeypatch, kind, job=job, engine=engine, status=status)
    btn = _btn(s.ui, label)
    assert btn.enabled is False and btn.tips == [fl._CDC_OP_WAIT_START[kind]]
    assert (fl._CDC_OP_WAIT_START[kind], "info") in s.hints
    assert fl._CDC_LIVE_START_HINT not in s.ui.texts


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("kind", list(OPERATIONS))
def test_a_dialog_opened_anyway_names_the_wait_and_runs_nothing(monkeypatch, engine, kind):
    # A stale button (rendered before the operation started) still opens the dialog: its notice
    # is the wait (info), not "CDC is currently streaming", and its confirm runs nothing.
    s = _render_during(monkeypatch, kind, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED)
    _click(s.ui, "Retry unfinished tables")
    assert s.ui.opened == 1
    assert ("info", fl._CDC_OP_WAIT_HEADER, fl._CDC_OP_WAIT_RELOAD[kind]) in s.notices
    assert "CDC is currently streaming" not in s.ui.texts
    confirm = s.ui.btns[-1]
    assert confirm.enabled is False
    s.ui.handlers[-1][1]()
    assert s.rec.retried == [] and s.rec.started == 0


@pytest.mark.parametrize("engine", [MY, PG])
def test_an_operation_started_while_the_dialog_is_open_is_named_at_the_confirm(
    monkeypatch, engine
) -> None:
    # The confirm re-checks (G129); the toast now says what is going on.
    s = _render_during(monkeypatch, None, job=_failed_b(), engine=engine,
                       status=StepStatus.FAILED)
    _click(s.ui, "Retry unfinished tables")
    assert s.ui.opened == 1 and s.ui.btns[-1].enabled is True
    s.state.set_cdc_deploy_job_id("cdc-1", kind="start")  # Start CDC pressed (another tab)
    s.extra["cdc-1"] = _running()
    s.ui.handlers[-1][1]()
    assert s.rec.retried == []
    assert s.ui.notified == [fl._CDC_OP_WAIT_RELOAD["start"]]


# ---------------------------------------------------------------------------
# 3. The screen backstops
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("kind", list(OPERATIONS))
@pytest.mark.parametrize("action", ["Retry unfinished tables", "Reload specific tables",
                                    "Reload"])
def test_the_scoped_retry_is_refused_with_the_wait(monkeypatch, engine, kind, action) -> None:
    # FAILS WITHOUT THE FIX: refused with "CDC is streaming ... Stop CDC first".
    from dsql_migrator.ui import data_migration as dm

    from tests.test_g25_wire_r6b import _KEEPS_START_POINT, _RunningJobs, _screen
    from tests.test_g25_wire_r6b import _step_callbacks, _toasts

    make_job, status, call, _reloaded = _KEEPS_START_POINT[action]
    retries: list = []
    monkeypatch.setattr(dm, "run_full_load_retry",
                        lambda _h, _prior, tables, **kw: retries.append(tables))
    job = make_job()
    s = _screen(monkeypatch, engine, job=job, status=status, stack=OPERATIONS[kind],
                jobs=_RunningJobs({"job-1": job, "cdc-1": _running()}))
    s.state.set_cdc_deploy_job_id("cdc-1", kind=kind)
    call(_step_callbacks(monkeypatch, s))
    assert retries == [] and s.built == [] and s.jobs.submitted == []
    assert _toasts(s.ui) == [fl._CDC_OP_WAIT_RELOAD[kind]]
    assert s.state.job_id == "job-1" and s.state.error is None


@pytest.mark.parametrize("engine", [MY, PG])
@pytest.mark.parametrize("kind", list(OPERATIONS))
def test_the_header_run_is_refused_with_the_wait(monkeypatch, engine, kind) -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import new_start_point_block

    from tests.test_g25_wire_r6b import _record_new_start_point_runs, _screen, _toasts

    s = _screen(monkeypatch, engine, stack=OPERATIONS[kind])
    s.jobs.jobs["cdc-1"] = _running()
    s.state.set_cdc_deploy_job_id("cdc-1", kind=kind)
    runs = _record_new_start_point_runs(monkeypatch)
    s.header_run()
    assert runs == [] and s.built == [] and s.jobs.submitted == []
    # A Stop or a Start on a stack that has streamed meets the G25 gate first (it holds a new
    # start point there whatever CDC does, and names its route); a Delete of a stack that never
    # streamed meets the wait (a first Start in flight: tests/test_g129_coexisting_reload.py).
    block = new_start_point_block(s.state, engine)
    assert (block is None) is (kind == "delete")
    expected = fl._CDC_OP_WAIT_START["delete"] if block is None else block[2]
    assert _toasts(s.ui) == [expected]


# ---------------------------------------------------------------------------
# 4. A streaming stack with a failed table: one story on the screen
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", [MY, PG])
def test_the_new_start_point_notice_no_longer_calls_a_disabled_retry_unaffected(
    monkeypatch, engine
) -> None:
    # FAILS WITHOUT THE FIX: the G25 notice ended "Retry unfinished tables and Reload specific
    # tables keep the original start point, so they are not affected." under the hint "CDC is
    # live, so Retry unfinished tables is disabled".
    import tests.test_g25_fl_r6 as r6
    from dsql_migrator.ui.data_migration._full_load_engine import NEW_START_POINT_HEADER

    ui, _rec = r6._render(monkeypatch, state=_state(monkeypatch, RUNNING), job=_failed_b(),
                          engine=engine, status=StepStatus.FAILED)
    assert _btn(ui, "Retry unfinished tables").enabled is False
    assert fl._CDC_LIVE_RETRY_HINT in ui.texts
    body = ui.texts[ui.texts.index(NEW_START_POINT_HEADER) + 1]
    assert "not affected" not in body
    assert body.endswith(
        "Retry unfinished tables and Reload specific tables keep the original start point, so "
        "they need no new CDC topics; while CDC streams they wait for Stop CDC, like every Full "
        "Load."
    )


# ---------------------------------------------------------------------------
# 5. The AI's Full Load guidance
# ---------------------------------------------------------------------------


def test_the_ai_full_load_grounding_names_the_cdc_live_caveat_and_route() -> None:
    # FAILS WITHOUT THE FIX: "RE-RUNNING a failed table is safe: use 'Retry unfinished
    # tables'" with no word of CDC -- a disabled button while CDC is live.
    from dsql_migrator.core.assessment_strategist import build_full_load_error_chat_system

    system = build_full_load_error_chat_system("app.orders", "InternalError_: server unavailable")
    flat = " ".join(system.split())
    assert "EXCEPT while CDC is live (Full Load + CDC)" in flat
    # (Without naming the per-table 'Reload' again: G26 keeps it on the quarantined-row line.)
    assert ("'Retry unfinished tables' is disabled, and so is every reload (a quarantined "
            "table's included)") in flat
    assert "a Start CDC, Stop CDC or Delete CDC infrastructure is still in progress" in flat
    assert "The route is: Stop CDC (CDC step), retry or reload, then Start CDC again" in flat
    assert "Never present a retry or a reload as available while CDC is live." in flat
    # The two other places that send the operator to Retry / Reload carry the condition.
    assert "'Retry unfinished tables' (with CDC stopped, as above)" in flat
    assert "re-runs Full Load for just that table (with CDC stopped, as above)" in flat


@pytest.mark.parametrize("engine", ["MySQL", "PostgreSQL"])
def test_the_validation_ai_grounding_offers_retry_and_reload_only_with_cdc_stopped(engine) -> None:
    # FAILS WITHOUT THE FIX (G135): Validation's grounding offered Retry unfinished tables, and
    # Reload specific tables with its default Append, for missing rows with no word of CDC --
    # "with CDC stopped" qualified only the Drop & reload -- while the Full Load step disables
    # both as long as CDC streams or a CDC operation runs (G129, G132).
    from dsql_migrator.core.assessment_strategist import (
        _validation_recovery_context,
        build_validation_chat_system,
    )

    (line,) = [ln for ln in _validation_recovery_context(engine).splitlines()
               if "ORIGINAL start point" in ln]
    assert ("Retry unfinished tables (for what a Full Load left unfinished) or, for a few "
            "tables, Reload specific tables, both only with CDC stopped -- while CDC streams, "
            "or a Start CDC, Stop CDC or Delete CDC infrastructure is still in progress, the "
            "Full Load step runs no load at all, so the route is: Stop CDC (Data Migration "
            "step), retry or reload, then Start CDC again (while a CDC operation is in "
            "progress, wait for it to finish first); with CDC stopped, Reload's 'Drop & "
            "reload' also rebuilds stale rows.") in line
    assert "(with CDC stopped, its 'Drop & reload' also rebuilds stale rows)" not in line
    assert line in build_validation_chat_system("Matched: 7; mismatched: 1", source_engine=engine)


def test_the_ai_migration_context_says_the_full_load_step_runs_no_load_while_cdc_is_live():
    from dsql_migrator.ui.data_migration import (
        DataMigrationState,
        MigrationType,
        full_load_error_migration_context,
    )

    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    live = full_load_error_migration_context(state, table_name="app.orders", cdc_live=True)
    assert "can collide with the live sink" not in live
    assert "Retry unfinished tables is disabled, and so is every reload" in live
    assert "Stop CDC (or wait for that operation to finish), then retry or reload, then " \
           "Start CDC again." in live
    idle = full_load_error_migration_context(state, table_name="app.orders", cdc_live=False)
    assert "CDC has not started streaming yet." in idle and "disabled" not in idle


# ---------------------------------------------------------------------------
# 6. No screen or comment presents a load beside a live sink as available
# ---------------------------------------------------------------------------


def test_no_source_comment_keeps_the_removed_button_or_a_supported_coexisting_retry() -> None:
    # The comments said a retry was "supported while CDC streams" and that the dialog left "only
    # a negative 'Re-run anyway (CDC is live)'": neither is true since G129.
    src = ROOT / "src" / "dsql_migrator"
    for path in src.rglob("*.py"):
        text = " ".join(path.read_text(encoding="utf-8").split())
        for stale in ("Re-run anyway", "both of which are supported while CDC streams",
                      "Existing rows keep their values while CDC streams"):
            assert stale not in text, (path.name, stale)


def test_the_dlq_card_says_the_full_load_step_runs_no_reload_while_cdc_streams() -> None:
    from dsql_migrator.core.models import ErrorLogSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    from tests.test_ui_data_migration import _DlqUi

    ui = _DlqUi()
    cm._render_cdc_dlq_breakdown(ui, SimpleNamespace(
        error_summary=ErrorLogSummary(total_errors=1, errors_by_table={"app.t": 1})))
    body = " ".join(ui.texts)
    assert ("Both repairs need Stop CDC first: while the sink is streaming, the Full Load step "
            "runs no reload") in body
    assert "Full Load collides with it" not in body


def test_the_reader_shards_help_says_when_a_mysql_source_splits_a_table() -> None:
    # G129 left MySQL reader-range sharding engaged only beside a streaming sink; G137 splits
    # a MySQL table in Full load + CDC, where CDC replays the load from its start point.
    from dsql_migrator.config import TUNABLE_KNOBS

    (knob,) = [k for k in TUNABLE_KNOBS if k.field == "full_load_reader_shards"]
    help_text = " ".join(knob.help_text.split())
    assert "A MySQL source always reads a table with one reader" not in help_text
    assert "A PostgreSQL source splits in any run, its ranges sharing one snapshot" in help_text
    assert ("A MySQL source splits only in Full load + CDC with a binlog start point, and only "
            "a table whose every unique key on Aurora DSQL includes that leading column") in help_text
    # P2: Full load only reads a MySQL table's ranges with one session, from one snapshot.
    assert "Full load only reads every MySQL table with one reader" not in help_text
    assert ("In Full load only, or without a binlog start point, a MySQL table's ranges are "
            "read by one reader session from one snapshot: one source connection and one "
            "process per table") in help_text
    assert "It applies only with 'Tables in parallel' above 1" in help_text
    # The trade-off, said where the knob is set (no more "Shard minimum rows" field).
    assert "rise up to Tables in parallel ×" in help_text
    assert "pair it with the source-load throttle" in help_text
    assert "DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS" in help_text
    assert "'Shard minimum rows'" not in help_text
    # The Settings row's visible helper says the same in one line.
    assert knob.description == (
        "Split a large table's read by its integer leading key (1 = off; MySQL: separate "
        "snapshots only in Full load + CDC)."
    )


# ---------------------------------------------------------------------------
# 7. The manual, in English, Korean and Japanese alike
# ---------------------------------------------------------------------------

_P03 = "03-full-load.md"
_P04 = "04-cdc-and-dsql-constraints.md"
_P06 = "06-limitations.md"
_P13 = "13-excluding-tables-and-columns.md"

_PINS = {
    _P03: {
        # G135: the wait is the Full Load step's; the page header's Run meets the G25 gate first
        # on a stack that has streamed (test_the_header_run_is_refused_with_the_wait). Chapter 3
        # shows it as one row of the CDC-state table (2026-10-25), so it is pinned per cell.
        "operation-wait": {
            "en": "wait for it to finish (the CDC step shows its progress)",
            "ko": "끝나기를 기다리세요(CDC 단계에 진행 상황이 표시됨)",
            "ja": "終わるまで待ってください (CDC ステップに進行状況が表示されます)",
        },
        # (2026-10-28) The manual no longer spells out what the page header's Run does during
        # a CDC operation, nor that a Retry or a Reload runs once a Stop or a Delete has
        # finished: both are UI behavior a tool user does not need to read (the Live row
        # already says Stop CDC comes first). The code assertions above still pin them.
        # G137: a MySQL table splits in Full load + CDC (CDC replays the load from its start
        # point), PostgreSQL in any run (one shared snapshot).
        "sharding": {
            "en": "**MySQL**: only in **Full load + CDC** whose start point has a binlog file and "
            "position, and only a table whose every unique key on Aurora DSQL includes the "
            "leading key column",
            "ko": "**MySQL**: 시작 지점에 binlog 파일과 위치가 있는 **Full load + CDC**에서만, 그리고 "
            "Aurora DSQL에서 모든 unique 키가 선행 키 컬럼을 포함하는 테이블만",
            "ja": "**MySQL**: 開始位置に binlog ファイルと位置がある **Full load + CDC** でのみ、そして "
            "Aurora DSQL 上のすべてのユニークキーが先頭のキー列を含むテーブルだけ",
        },
    },
    _P04: {
        "reload-with-cdc-stopped": {
            "en": "fix the oversized source value and **Reload** that table with CDC stopped (Stop "
            "CDC, Reload, then Start CDC again: the Full Load step runs no reload while CDC "
            "streams,",
            "ko": "컷오버 전에 초대형 소스 값을 고쳐 CDC를 멈춘 상태에서 해당 테이블을 **Reload**하거나"
            "(Stop CDC, Reload, 그다음 Start CDC 다시: CDC가 스트리밍하는 동안 Full Load 단계는 다시 "
            "적재를 실행하지 않습니다,",
            "ja": "カットオーバー前に特大サイズのソース値を修正して、CDC を停止した状態でそのテーブルを "
            "**Reload** するか (Stop CDC、Reload、それからもう一度 Start CDC: CDC のストリーミング中は "
            "Full Load ステップは再ロードを実行しません、",
        },
    },
    _P13: {
        "no-load-while-cdc-streams": {
            "en": "While CDC streams, no Full Load runs at all",
            "ko": "CDC가 스트리밍하는 동안에는 Full Load가 전혀 실행되지 않습니다",
            "ja": "CDC のストリーミング中は Full Load はまったく実行されません",
        },
    },
}
# What the manual said before, a load beside a live sink: gone from every page. Last in each
# (G135): the page header's Run giving the wait during any CDC operation.
_STALE = {
    "en": ("**Not gated:** **Retry unfinished tables**",
           "keep the original start point and are not affected",
           "while CDC streams, nothing is forced, and the table is appended with a WARNING",
           "fix the oversized source value and **Reload** that table before cutting over",
           "these actions stay disabled and the page header's **Run** starts nothing, but the "
           "reason they give is to wait"),
    "ko": ("**막히지 않는 것:** **Retry unfinished tables**",
           "원래 시작 지점을 유지하므로 영향을 받지 않습니다",
           "CDC가 스트리밍하는 동안에는 아무것도 강제되지 않으며, 테이블은 WARNING과 함께 append됩니다",
           "초대형 소스 값을 고쳐 해당 테이블을 **Reload**하거나",
           "페이지 헤더의 **Run**은 아무것도 시작하지 않지만, 이때 알려 주는 이유는"),
    "ja": ("**制限されないもの:** **Retry unfinished tables**",
           "元の開始位置を維持するため、影響を受けません",
           "CDC のストリーミング中は何も強制されず、テーブルは WARNING とともに append されます",
           "特大サイズのソース値を修正してそのテーブルを **Reload** するか",
           "ページヘッダーの **Run** も何も開始しませんが、示される理由は"),
}


@pytest.mark.parametrize("lang", LANGS)
def test_the_manual_tells_the_same_story_in_every_language(lang: str) -> None:
    # The code first: the waits the manual describes are the ones the step gives.
    assert all(_WAIT in text for text in fl._CDC_OP_WAIT_RELOAD.values())
    assert fl._CDC_OP_WAIT_RELOAD["stop"].endswith("reload, then Start CDC again.")
    assert fl._CDC_OP_WAIT_RELOAD["delete"].endswith("then reload.")
    assert "Once CDC streams, a reload needs Stop CDC first" in fl._CDC_OP_WAIT_RELOAD["start"]

    for page, by_key in _PINS.items():
        text = _page(lang, page)
        for key, by_lang in by_key.items():
            assert _has(lang, text, by_lang[lang]), (lang, page, key)
    assert not _stale(lang, _STALE[lang]), lang
