# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G147: a MySQL Full load + CDC start point is made exact before the Full Load reads.

The start point is recorded before any table is read, and every reader opens its own
snapshot after it. A GTID set covers only committed transactions; a binlog file:position
can also cover one that is still committing -- no snapshot sees it and CDC starts after
it, so the change was lost (reproduced on MySQL 8.0/8.4 and on Aurora MySQL 3.10.3 with
the commit held by binlog_group_commit_sync_delay). FLUSH BINARY LOGS waits for those
commits before switching binlog files; the Full load + CDC run issues it right after the
point is recorded (live on Aurora: it waited 0.66 s for the held commit, and the snapshot
saw the row). Aurora enhanced binlog without gtid_mode=ON is refused (the tool does not
rely on the flush there), and so is an XA transaction still prepared.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dsql_migrator.core import mysql_start_point_fence as fence
from dsql_migrator.core.introspector import is_write_or_ddl
from dsql_migrator.core.models import (
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteStatus,
    SourceConnectionConfig,
    SourceType,
    Watermark,
)
from dsql_migrator.core.prerequisites import check_start_point_fence


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class _DriverError(Exception):
    """A driver error the way SQLAlchemy wraps PyMySQL's (``.orig.args[0]`` = errno)."""

    def __init__(self, errno: int, message: str) -> None:
        super().__init__(message)
        self.orig = type("_Orig", (Exception,), {})(errno, message)


class _Source:
    """The fence's connection: answers the three fence statements, records them.

    ``xa`` is the XA RECOVER answer per poll (a list of rows each); the last one repeats.
    ``xa_error`` names the polls (0-based) that raise.
    """

    def __init__(self, *, enhanced: str | None = None, flush_error: int | None = None,
                 xa: list | None = None, xa_error: tuple = ()) -> None:
        self.statements: list[str] = []
        self._enhanced = enhanced
        self._flush_error = flush_error
        self._xa = list(xa or [[]])
        self._xa_error = set(xa_error)
        self._polls = 0

    def execute(self, statement):
        sql = str(statement)
        self.statements.append(sql)
        if sql == fence.ENHANCED_BINLOG_STATUS:
            return _Rows([("Aurora_enhanced_binlog", self._enhanced)] if self._enhanced else [])
        if sql == fence.FLUSH_BINARY_LOGS:
            if self._flush_error:
                raise _DriverError(self._flush_error, "Access denied; you need the RELOAD privilege")
            return _Rows([])
        if sql == fence.XA_RECOVER:
            poll, self._polls = self._polls, self._polls + 1
            if poll in self._xa_error:
                raise _DriverError(1227, "Access denied; you need XA_RECOVER_ADMIN")
            return _Rows(self._xa[min(poll, len(self._xa) - 1)])
        raise AssertionError(f"unexpected statement {sql!r}")


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class _Log:
    """``on_log``: ``(message, warning)`` pairs."""

    def __init__(self) -> None:
        self.lines: list[tuple[str, bool]] = []

    def __call__(self, message: str, *, warning: bool = False) -> None:
        self.lines.append((message, warning))


def _fence(source: _Source, gtid_mode: str | None, **kw):
    clock = _Clock()
    log = _Log()
    result = fence.fence_start_point(
        source, gtid_mode=gtid_mode, on_log=log, sleep=clock.sleep, monotonic=clock, **kw
    )
    return result, log


@pytest.mark.parametrize("mode", ["OFF", "OFF_PERMISSIVE", "ON_PERMISSIVE", None])
def test_a_file_position_start_point_is_fenced_with_flush_binary_logs(mode) -> None:
    source = _Source(enhanced="DISABLED")
    result, log = _fence(source, mode)
    assert result.method == "flush" and result.xa_checked
    # The enhanced-binlog status is read first, the FLUSH runs, then the XA check.
    assert source.statements == [fence.ENHANCED_BINLOG_STATUS, fence.FLUSH_BINARY_LOGS, fence.XA_RECOVER]
    assert any("FLUSH BINARY LOGS took" in m and not w for m, w in log.lines)


def test_a_gtid_start_point_needs_no_flush_but_is_still_checked_for_xa() -> None:
    source = _Source()
    result, _log = _fence(source, "ON")
    assert result.method == "gtid" and result.flush_seconds is None
    assert source.statements == [fence.XA_RECOVER]


def test_aurora_enhanced_binlog_without_gtid_is_refused_before_any_flush() -> None:
    source = _Source(enhanced="ACTIVE")
    with pytest.raises(fence.StartPointFenceError, match="gtid_mode=ON"):
        _fence(source, "OFF")
    assert fence.FLUSH_BINARY_LOGS not in source.statements


def test_no_reload_stops_the_run_with_the_remedy() -> None:
    with pytest.raises(fence.StartPointFenceError, match="RELOAD") as raised:
        _fence(_Source(flush_error=1227), "OFF")
    assert "gtid_mode=ON" in str(raised.value)


_XA_A, _XA_B = (1, 3, 0, "abc"), (1, 3, 0, "new")


def test_a_prepared_xa_transaction_is_waited_for_then_refused() -> None:
    # Finishes within the wait: the run goes on.
    result, _ = _fence(_Source(xa=[[_XA_A], [_XA_A], []]), "ON", xa_wait_seconds=5, poll_seconds=1)
    assert result.xa_checked
    # Still prepared after the wait: refused, with the remedy.
    with pytest.raises(fence.StartPointFenceError, match="1 XA transaction"):
        _fence(_Source(xa=[[_XA_A]]), "ON", xa_wait_seconds=3, poll_seconds=1)


def test_the_xa_wait_is_for_the_transactions_prepared_before_it_began() -> None:
    # One prepared after the first XA RECOVER follows the start point (CDC replays it), so
    # it does not hold the run: only the ones found first are waited for.
    result, _ = _fence(_Source(xa=[[_XA_A], [_XA_B]]), "ON", xa_wait_seconds=3, poll_seconds=1)
    assert result.xa_checked


def test_an_xa_recover_that_fails_while_waiting_refuses_the_start_point() -> None:
    # The transactions found first may still be prepared: never read as "none left".
    with pytest.raises(fence.StartPointFenceError, match="XA RECOVER failed"):
        _fence(_Source(xa=[[_XA_A]], xa_error=(1,)), "ON", xa_wait_seconds=5, poll_seconds=1)


def test_xa_recover_without_its_privilege_is_logged_as_a_warning_not_fatal() -> None:
    result, log = _fence(_Source(xa_error=(0,)), "ON")
    assert result.xa_checked is False
    assert [w for m, w in log.lines if "XA_RECOVER_ADMIN" in m] == [True]


def test_the_fence_engine_runs_only_its_three_statements() -> None:
    with pytest.raises(fence.StartPointFenceError, match="refused non-fence"):
        fence._run(_Source(), "FLUSH TABLES WITH READ LOCK")  # noqa: SLF001
    for statement in fence.FENCE_STATEMENTS:
        fence._assert_fence_statement(statement)  # noqa: SLF001


def test_the_read_only_guard_refuses_server_administration() -> None:
    for sql in ("FLUSH BINARY LOGS", "flush tables with read lock", "PURGE BINARY LOGS TO 'x'",
                "RESET MASTER", "KILL 42"):
        assert is_write_or_ddl(sql), sql
    for sql in ("SELECT 1", "SHOW MASTER STATUS", "XA RECOVER", "SHOW GLOBAL STATUS"):
        assert not is_write_or_ddl(sql), sql


def test_the_fence_engine_has_no_read_only_guard_and_runs_in_autocommit(monkeypatch) -> None:
    from dsql_migrator.config import SecretRef, SecretSource, SecretValue
    from dsql_migrator.ui.connect import make_source_engine_factory

    config = SourceConnectionConfig(host="db.example.com", username="admin")
    engine = fence.build_fence_engine(config, SecretValue("pw-1"))
    try:
        # The guarded read path refuses FLUSH; the fence engine carries no such listener.
        guarded = make_source_engine_factory(SecretValue("pw-1"))(config)
        assert len(guarded.dispatch.before_cursor_execute) > 0
        assert len(engine.dispatch.before_cursor_execute) == 0
        guarded.dispose()
        assert engine.get_execution_options()["isolation_level"] == "AUTOCOMMIT"
        assert engine.url.password == "pw-1" and engine.url.host == "db.example.com"
    finally:
        engine.dispose()
    # Secrets Manager / environment authentication: resolved like the read path's factory.
    monkeypatch.setenv("G147_FENCE_PW", "pw-2")
    with_secret = config.model_copy(
        update={"secret": SecretRef(source=SecretSource.ENVIRONMENT, locator="G147_FENCE_PW")}
    )
    engine = fence.build_fence_engine(with_secret, None)
    assert engine.url.password == "pw-2"
    engine.dispose()
    with pytest.raises(fence.StartPointFenceError, match="MySQL-only"):
        fence.build_fence_engine(
            SourceConnectionConfig(host="h", source_type=SourceType.POSTGRES), None
        )


def _watermark(**kw) -> Watermark:
    return Watermark(snapshot_timestamp=datetime(2026, 10, 22, tzinfo=timezone.utc), **kw)


def test_an_unfenced_file_position_start_point_is_recognised() -> None:
    fp = {"binlog_file": "mysql-bin.000009", "binlog_position": 4}
    assert fence.start_point_unfenced(_watermark(**fp))
    assert fence.start_point_unfenced(_watermark(gtid_mode="OFF", **fp))
    # A GTID set recorded under a mode other than ON is not where Debezium starts.
    assert fence.start_point_unfenced(_watermark(gtid_mode="OFF_PERMISSIVE", gtid_executed="u:1-5", **fp))
    assert not fence.start_point_unfenced(_watermark(gtid_mode="ON", **fp))
    # Recorded before gtid_mode was: a GTID set reads as a GTID start point (as
    # watermark_coordinate reads it), so no warning for an older GTID source's point.
    assert not fence.start_point_unfenced(_watermark(gtid_executed="u:1-5", **fp))
    assert not fence.start_point_unfenced(_watermark(start_point_fence="flush", **fp))
    assert not fence.start_point_unfenced(_watermark(wal_lsn="0/16B3748"))
    assert not fence.start_point_unfenced(_watermark())
    assert not fence.start_point_unfenced(None)


def test_the_fence_method_is_saved_only_when_set() -> None:
    assert "start_point_fence" not in _watermark().model_dump()  # older builds still load it
    assert _watermark(start_point_fence="flush").model_dump()["start_point_fence"] == "flush"


def test_the_capturer_fences_and_records_the_method() -> None:
    from dsql_migrator.core.watermark import WatermarkCapturer

    source = _Source(enhanced="DISABLED")
    disposed = []

    class _Engine:
        def connect(self):
            class _Ctx:
                def __enter__(self_inner):
                    return source

                def __exit__(self_inner, *exc):
                    return False

            return _Ctx()

        def dispose(self):
            disposed.append(True)

    capturer = WatermarkCapturer(fence_engine_factory=lambda _config: _Engine())
    wm = _watermark(binlog_file="mysql-bin.000009", binlog_position=4, gtid_mode="OFF")
    fenced = capturer.fence_start_point(SourceConnectionConfig(host="h"), wm)
    assert fenced.start_point_fence == "flush" and disposed == [True]
    assert fence.FLUSH_BINARY_LOGS in source.statements
    # No binlog coordinate at all: nothing to make exact, no statement.
    untouched = capturer.fence_start_point(SourceConnectionConfig(host="h"), _watermark())
    assert untouched.start_point_fence is None and len(disposed) == 1


def _fencing_capturer(calls: list):
    from tests.test_g17_full_load_and_cdc_additions import _loaded

    class _Capturer:
        def capture(self, source, tables):
            return _loaded().watermark

        def fence_start_point(self, source, watermark, *, password, on_log):
            calls.append(password)
            on_log("XA RECOVER could not run", warning=True)
            on_log("FLUSH BINARY LOGS took 0.004 s")
            return watermark.model_copy(update={"start_point_fence": "flush"})

    return _Capturer()


@pytest.mark.parametrize("cdc, expect_fence", [(True, True), (False, False)])
def test_only_a_full_load_and_cdc_run_fences_its_start_point(monkeypatch, cdc, expect_fence) -> None:
    import dsql_migrator.ui.data_migration._full_load_engine as engine
    from dsql_migrator.core.activity_log import ActivityStatus

    from tests.test_g17_full_load_and_cdc_additions import _mysql_migrator, _tbl

    migrator, _reads = _mysql_migrator(monkeypatch)
    events: list = []
    monkeypatch.setattr(engine, "log_activity", lambda *a, **kw: events.append((a, kw)))
    calls: list = []
    migrator._watermark_capturer = _fencing_capturer(calls)  # noqa: SLF001
    # A CDC stream already running does not make a run's start point CDC's (G129 holds the
    # load meanwhile anyway): only the migration type does.
    migrator.bind_run_inputs(replace(migrator._inputs, is_cdc_migration=cdc, cdc_coexisting=True))  # noqa: SLF001
    wm = migrator.capture_watermark([_tbl("app.a")])
    assert (wm.start_point_fence == "flush") is expect_fence
    if expect_fence:
        # The session's password goes to the fence's own engine; what could not be checked
        # is a WARNING in the activity log, what was done an INFO.
        assert calls == [migrator._inputs.source_password]  # noqa: SLF001
        statuses = [kw["status"] for a, kw in events if a[1] == "CDC start point"]
        assert statuses == [ActivityStatus.WARNING, ActivityStatus.INFO]
    else:
        assert calls == [] and not events


@pytest.mark.parametrize("cdc", [True, False])
def test_a_run_that_recorded_no_start_point_offers_no_retry_in_full_load_and_cdc(cdc) -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration._full_load_engine import run_full_load

    from tests.test_ui_data_migration import _FakeMigrator, _tables

    migrator = _FakeMigrator(
        watermark_error=fence.StartPointFenceError("The source user cannot run FLUSH BINARY LOGS")
    )
    migrator._inputs = SimpleNamespace(is_cdc_migration=cdc)  # noqa: SLF001
    manager = JobManager()
    job_id = manager.submit(
        lambda handle: run_full_load(handle, _tables(), migrator=migrator, error_log=ErrorLogStore())
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)
    assert job.status == "FAILED" and job.watermark is None and migrator.migrated == []
    assert "FLUSH BINARY LOGS" in manager.get_error(job_id)
    # Full load + CDC: no PENDING table, so no "Retry unfinished tables" under no start point
    # at all (Start runs it afresh). Full load only keeps its retry.
    assert [c.status for c in job.chunks] == ([] if cdc else ["PENDING", "PENDING"])


@pytest.mark.parametrize("variables, grants, status", [
    ({"gtid_mode": "ON"}, [], PrerequisiteStatus.PASS),
    ({"gtid_mode": "OFF", "aurora_enhanced_binlog_status": "ACTIVE"},
     ["GRANT RELOAD ON *.* TO `u`@`%`"], PrerequisiteStatus.FAIL),
    ({"gtid_mode": "OFF"}, ["GRANT SELECT, RELOAD, REPLICATION CLIENT ON *.* TO `u`@`%`"],
     PrerequisiteStatus.PASS),
    ({"gtid_mode": "OFF"}, ["GRANT ALL PRIVILEGES ON *.* TO `u`@`%`"], PrerequisiteStatus.PASS),
    ({"gtid_mode": "OFF"}, ["GRANT SELECT ON *.* TO `u`@`%`", "GRANT `rds_superuser_role`@`%` TO `u`@`%`"],
     PrerequisiteStatus.WARN),
    ({"gtid_mode": "OFF"}, ["GRANT SELECT, REPLICATION CLIENT ON *.* TO `u`@`%`"], PrerequisiteStatus.FAIL),
    ({}, ["GRANT SELECT ON *.* TO `u`@`%`"], PrerequisiteStatus.FAIL),
])
def test_the_prerequisite_says_whether_the_start_point_can_be_made_exact(variables, grants, status) -> None:
    result = check_start_point_fence(variables, list(grants))
    assert result.status is status
    if status is PrerequisiteStatus.FAIL:
        assert result.required and result.remediation
    # No Full Load of the migration records a start point (CDC only, or one is recorded
    # already): no fence runs, so the row is SKIP whatever the source says.
    skipped = check_start_point_fence(variables, list(grants), records_start_point=False)
    assert skipped.status is PrerequisiteStatus.SKIP


def test_the_report_skips_the_row_when_the_run_records_no_start_point() -> None:
    from dsql_migrator.core.models import PrerequisiteCheckRequest
    from dsql_migrator.core.prerequisites import PrerequisiteChecker

    from tests.test_prerequisites import _FakeMsk, _FakeSource, _FakeTarget, _table

    def _row(records: bool, *, reachable: bool = True):
        checker = PrerequisiteChecker(
            # gtid_mode OFF and no RELOAD: the graded row FAILs.
            source_probe=_FakeSource(
                reachable=reachable,
                grants=["GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO `u`@`%`"],
                variables={"log_bin": "ON", "binlog_format": "ROW", "binlog_row_image": "FULL",
                           "gtid_mode": "OFF", "binlog_expire_logs_seconds": "2592000"},
            ),
            target_probe=_FakeTarget(existing={"app.orders"}),
            msk_probe=_FakeMsk(),
        )
        report = checker.check(
            PrerequisiteCheckRequest(
                mode=MigrationMode.CDC, tables=["app.orders"], records_start_point=records
            ),
            tables=[_table("app.orders")],
        )
        (row,) = [r for r in report.results if r.check_id is PrerequisiteCheckId.START_POINT_FENCE]
        return row

    assert _row(True).status is PrerequisiteStatus.FAIL and _row(True).required
    assert _row(False).status is PrerequisiteStatus.SKIP
    # An unreachable source: "not checked" when it would be graded, the SKIP otherwise.
    assert _row(True, reachable=False).not_checked
    assert not _row(False, reachable=False).not_checked


def test_the_probe_folds_in_the_aurora_enhanced_binlog_status() -> None:
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    class _Conn:
        def __init__(self, status_rows):
            self._status_rows = status_rows

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

        def execute(self, statement, parameters=None):
            sql = str(statement)
            if sql.startswith("SHOW GLOBAL VARIABLES"):
                return _Rows([("gtid_mode", "OFF")])
            if sql == "SHOW GLOBAL STATUS LIKE 'aurora_enhanced_binlog'":
                return _Rows(self._status_rows)
            raise RuntimeError("no mysql.rds_configuration")

    def _variables(status_rows):
        probe = SessionSourceProbe(SourceConnectionConfig(host="db.example.com"), None)
        probe._engine_factory = lambda _config: SimpleNamespace(  # noqa: SLF001
            connect=lambda: _Conn(status_rows)
        )
        return probe.variables()

    assert _variables([("Aurora_enhanced_binlog", "ACTIVE")]) == {
        "gtid_mode": "OFF", "aurora_enhanced_binlog_status": "ACTIVE",
    }
    # Not Aurora (no such status) or another name: no key.
    assert _variables([]) == {"gtid_mode": "OFF"}
    assert _variables([("Aurora_enhanced_binlog_other", "ACTIVE")]) == {"gtid_mode": "OFF"}


_UNFENCED = {"start_point_fence": None}


@pytest.mark.parametrize("update, shown", [
    (_UNFENCED, True),                          # a Full load only run / an earlier version
    ({}, False),                                # fenced by a Full load + CDC run
    ({**_UNFENCED, "gtid_mode": "ON"}, False),  # a GTID start point is exact by itself
])
def test_start_cdc_warns_about_an_unfenced_automatic_start_point(update, shown) -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _WM, _card, _job, _state

    ui = _card(_state(committed=False), job=_job(_WM.model_copy(update=update)))
    blob = " ".join(ui.texts)
    assert "mysql-bin-changelog.000007:108063011" in blob  # the Automatic point is shown
    assert (_cdc_ui.UNFENCED_START_POINT_HEADER in blob) is shown
    # Its label and hover text make no gapless claim then.
    assert ("Automatic — gapless from Full Load (recommended)" in blob) is not shown
    assert (_cdc_ui.UNFENCED_START_POINT_TOOLTIP in ui.tooltips) is shown


def test_the_start_dialog_and_the_start_line_say_it_too() -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _WM, _job, _state

    unfenced = _WM.model_copy(update=_UNFENCED)
    header, _body = _cdc_ui._unfenced_start_point_notice(_state(committed=False), _job(unfenced), None)  # noqa: SLF001
    assert header == _cdc_ui.UNFENCED_START_POINT_HEADER
    # Not for a fenced point, nor for a Start that resumes the stack's committed offset.
    assert _cdc_ui._unfenced_start_point_notice(_state(committed=False), _job(_WM), None) is None  # noqa: SLF001
    resumed = _state(committed=True, deployed=("mysql-bin-changelog.000007", "108063011"))
    assert _cdc_ui._unfenced_start_point_notice(resumed, _job(unfenced), None) is None  # noqa: SLF001
    line = _cdc_ui._cdc_start_detail("dsql-cdc-x", unfenced, mode="auto", source_type=SourceType.MYSQL)  # noqa: SLF001
    assert _cdc_ui.UNFENCED_START_POINT_LINE in line and "gapless" not in line
    fenced = _cdc_ui._cdc_start_detail("dsql-cdc-x", _WM, mode="auto", source_type=SourceType.MYSQL)  # noqa: SLF001
    assert "(gapless from the Full Load watermark)" in fenced


def test_the_remedy_is_a_full_load_and_cdc_rerun_whatever_the_type() -> None:
    from dsql_migrator.ui.data_migration import MigrationType, _cdc_ui

    flc = _cdc_ui.unfenced_start_point_body(MigrationType.FULL_LOAD_AND_CDC)
    assert "Re-run Full Load on the Full Load step choosing 'Drop & reload' before Start CDC" in flc
    assert "(the Full load + CDC run makes its start point exact)" in flc
    # CDC only has no Full Load step: the re-run switches to Full load + CDC, never to Full
    # load only (whose binlog position no fence makes exact).
    cdc_only = _cdc_ui.unfenced_start_point_body(MigrationType.CDC_ONLY)
    assert "switch the migration type to Full load + CDC for that run" in cdc_only
    assert "Full load only for that run" not in cdc_only
    for body in (flc, cdc_only):
        assert body.endswith("or validate in CHECKSUM mode once CDC has caught up.")


@pytest.mark.parametrize("update, shown", [(_UNFENCED, True), ({}, False)])
def test_the_start_cdc_dialog_shows_the_warning(monkeypatch, update, shown) -> None:
    import asyncio

    import nicegui

    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _JM, _WM, _job, _state
    from tests.test_ui_data_migration import _DialogUi

    for name in ("_probe_cdc_gtid_start", "_probe_cdc_start_point", "_probe_binlog_resume_gap",
                 "cdc_deploy_connection_blocker", "_probe_cdc_blocking_foreign_keys",
                 "_pg_objects_gate_block"):
        monkeypatch.setattr(_cdc_ui, name, lambda *_a, **_k: None)
    ui = _DialogUi()
    monkeypatch.setattr(
        _cdc_ui, "_render_notice",
        lambda _ui, *, tone="info", icon=None, header="", body="": ui.notices.append((tone, header)),
    )

    class _Run:
        @staticmethod
        async def io_bound(fn, *a, **k):
            return fn(*a, **k)

    monkeypatch.setattr(nicegui, "run", _Run, raising=False)
    asyncio.run(
        _cdc_ui._open_cdc_start_dialog(  # noqa: SLF001
            ui, _state(committed=False), lambda: None, session=None,
            job_manager=_JM(_job(_WM.model_copy(update=update))), inventory=None,
        )
    )
    assert (("warning", _cdc_ui.UNFENCED_START_POINT_HEADER) in ui.notices) is shown
    # Not a block: Start stays available.
    assert ui.buttons["Start CDC"].enabled is not False


# ---------------------------------------------------------------------------
# Review round 2: the fence row on every Full load + CDC run, the refusals' budget, and the
# routes and lines that otherwise still called an unfenced point gapless.
# ---------------------------------------------------------------------------


def test_a_report_graded_for_the_other_cdc_type_is_not_used() -> None:
    from dsql_migrator.core.models import PrerequisiteReport
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()
    state.migration_type = MigrationType.CDC_ONLY
    report = PrerequisiteReport.build(MigrationMode.CDC, [])
    state.set_prereq_report(MigrationMode.CDC, report)
    assert state.get_prereq_report(MigrationMode.CDC) is report
    # CDC only SKIPs the fence row; Full load + CDC grades it: the checks run again.
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    assert state.get_prereq_report(MigrationMode.CDC) is None
    state.migration_type = MigrationType.CDC_ONLY
    assert state.get_prereq_report(MigrationMode.CDC) is report
    # The type recorded at the click wins over the one current when the checks end.
    state.set_prereq_report(MigrationMode.CDC, report, migration_type=MigrationType.FULL_LOAD_AND_CDC)
    assert state.get_prereq_report(MigrationMode.CDC) is None
    # Full load only's own report is one type's only.
    state.migration_type = MigrationType.FULL_LOAD_ONLY
    full = PrerequisiteReport.build(MigrationMode.FULL_LOAD, [])
    state.set_prereq_report(MigrationMode.FULL_LOAD, full)
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    assert state.get_prereq_report(MigrationMode.FULL_LOAD) is full


@pytest.mark.parametrize("mtype, graded", [("flc", True), ("cdc_only", False)])
def test_a_full_load_and_cdc_run_grades_the_fence_row_with_a_watermark_too(
        monkeypatch, mtype, graded) -> None:
    # FAILS WITHOUT THE FIX: a recorded watermark SKIPped the row, while Re-run Full Load
    # and Re-run all tables record (and fence) a new start point.
    import asyncio

    from dsql_migrator.core.models import PrerequisiteReport
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.ui.data_migration import MigrationType

    from tests.test_g12_table_set_after_load import _loaded
    from tests.test_g16_full_load_and_cdc_narrowing import _fc_screen

    s = _fc_screen(monkeypatch, job=_loaded(), selected=["app.a", "app.b"], render=False)
    assert s.jobs.get_status("job-1").watermark is not None
    if mtype == "cdc_only":
        s.session.set_migration_type(MigrationType.CDC_ONLY)
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
    assert request.records_start_point is graded


def test_the_fence_rows_fail_holds_the_runs_that_record_a_start_point_not_cdc() -> None:
    from dsql_migrator.core.models import PrerequisiteReport
    from dsql_migrator.ui.data_migration._models import cdc_prerequisite_block_reason
    from dsql_migrator.ui.data_migration._models import prerequisite_block_reason

    passes = [
        check_start_point_fence({"gtid_mode": "ON"}, []).model_copy(
            update={"check_id": check_id, "title": str(check_id)}
        )
        for check_id in (PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteCheckId.BINLOG_ROW_FORMAT)
    ]
    fail = check_start_point_fence({"gtid_mode": "OFF"}, ["GRANT SELECT ON *.* TO `u`@`%`"])
    report = PrerequisiteReport.build(MigrationMode.CDC, [*passes, fail])
    # Deploy / Start CDC: a point already recorded runs no fence.
    assert cdc_prerequisite_block_reason(report) is None
    # Start / Re-run Full Load: the fence would refuse it.
    assert "CDC start point can be made exact" in prerequisite_block_reason(report, mode=MigrationMode.CDC)


@pytest.mark.parametrize("source, gtid_mode", [
    (_Source(enhanced="ACTIVE"), "OFF_PERMISSIVE"),
    (_Source(flush_error=1227), "OFF"),
    (_Source(xa=[[_XA_A]]), "ON"),
    (_Source(xa=[[_XA_A]], xa_error=(1,)), "ON"),
])
def test_each_refusal_says_its_cause_and_remedy_within_a_job_errors_first_line(source, gtid_mode) -> None:
    # A job error keeps the first line, capped at 300 characters (JobManager._mark_failed).
    with pytest.raises(fence.StartPointFenceError) as raised:
        _fence(source, gtid_mode, xa_wait_seconds=2, poll_seconds=1)
    first = f"StartPointFenceError: {str(raised.value).splitlines()[0]}"
    assert len(first) <= 270, (len(first), first)
    assert "run the full load again" in first.lower()


def test_the_xa_refusal_leaves_the_prepared_transactions_to_their_owner() -> None:
    with pytest.raises(fence.StartPointFenceError) as raised:
        _fence(_Source(xa=[[_XA_A]]), "ON", xa_wait_seconds=2, poll_seconds=1)
    text = str(raised.value)
    assert "transaction manager" in text.splitlines()[0]
    assert "every schema and application" in text and "atomicity" in text


def test_the_manual_point_notice_does_not_call_an_unfenced_automatic_point_gapless() -> None:
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_ui_data_migration import _RecordingUi

    wm = _watermark(binlog_file="mysql-bin.000009", binlog_position=4)
    for manual, gapless_words in (
        (_watermark(binlog_file="mysql-bin.000010", binlog_position=4), "gapless handoff"),
        (_watermark(binlog_file="mysql-bin.000005", binlog_position=4), "exactly at the snapshot"),
    ):
        plain, unfenced = _RecordingUi(), _RecordingUi()
        _cdc_ui._render_manual_vs_watermark_notice(plain, manual, wm)  # noqa: SLF001
        _cdc_ui._render_manual_vs_watermark_notice(unfenced, manual, wm, unfenced=True)  # noqa: SLF001
        assert gapless_words in " ".join(plain.texts)
        blob = " ".join(unfenced.texts)
        assert gapless_words not in blob and "without waiting for in-flight commits" in blob


def test_with_the_reload_condition_too_the_start_line_and_the_tooltip_keep_the_caveat() -> None:
    from dsql_migrator.ui.cdc_start_point_gate import RELOAD_CONDITION_LINE
    from dsql_migrator.ui.data_migration import _cdc_ui
    from tests.test_cdc_restart_start_point import _WM

    unfenced = _WM.model_copy(update=_UNFENCED)
    line = _cdc_ui._cdc_start_detail(  # noqa: SLF001
        "dsql-cdc-x", unfenced, mode="auto", source_type=SourceType.MYSQL, recopied=True
    )
    assert RELOAD_CONDITION_LINE in line and _cdc_ui.UNFENCED_START_POINT_PART in line
    fenced = _cdc_ui._cdc_start_detail(  # noqa: SLF001
        "dsql-cdc-x", _WM, mode="auto", source_type=SourceType.MYSQL, recopied=True
    )
    assert _cdc_ui.UNFENCED_START_POINT_PART not in fenced
    # The hover text names no notice that may not be rendered (Manual, a Start in flight).
    assert "see the notice below" not in _cdc_ui.UNFENCED_START_POINT_TOOLTIP


def test_on_a_stack_that_has_streamed_the_remedy_starts_with_new_topics() -> None:
    from dsql_migrator.ui.cdc_start_point_gate import NEW_TOPICS
    from dsql_migrator.ui.data_migration import MigrationType, _cdc_ui

    body = _cdc_ui.unfenced_start_point_body(MigrationType.FULL_LOAD_AND_CDC, streamed=True)
    assert f"To rule it out, {NEW_TOPICS}, then Re-run Full Load" in body
    assert NEW_TOPICS not in _cdc_ui.unfenced_start_point_body(MigrationType.FULL_LOAD_AND_CDC)


def test_the_table_set_hover_makes_no_gapless_claim_for_an_unfenced_point() -> None:
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = " ".join(inspect.getsource(_cdc_ui._render_cdc_start_button).split())  # noqa: SLF001
    assert "start_point_unfenced(_cdc_watermark(job, migration_state))" in src
    assert src.count("_gapless") >= 3  # computed once, used by both gapless tails


def test_the_full_load_only_audit_line_does_not_call_its_point_the_gapless_handoff(monkeypatch) -> None:
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    lines: list = []
    monkeypatch.setattr(engine, "log_activity", lambda *a, **kw: lines.append(kw["detail"]))
    engine._log_captured_watermark(_watermark(binlog_file="mysql-bin.000009", binlog_position=4))  # noqa: SLF001
    engine._log_captured_watermark(  # noqa: SLF001
        _watermark(binlog_file="mysql-bin.000009", binlog_position=4, start_point_fence="flush")
    )
    assert "without waiting for in-flight commits" in lines[0] and "gapless" not in lines[0]
    assert lines[1].startswith("consistency point for the gapless CDC handoff")


def test_a_full_load_and_cdc_record_without_a_start_point_offers_no_retry(monkeypatch) -> None:
    # A restart during the capture or the fence leaves PENDING tables and no watermark
    # (the in-thread failure path keeps none): Retry would load them under no start point.
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration._full_load_engine import (
        NO_START_POINT_RETRY_REFUSAL,
        FullLoadRefusedError,
        run_full_load_retry,
    )

    from tests.test_g16_full_load_and_cdc_narrowing import _fc
    from tests.test_g17_full_load_and_cdc_additions import _G17Session
    from tests.test_g25_fl import _render_step
    from tests.test_ui_data_migration import _FakeMigrator, _tables

    pending = [ChunkState(chunk_id=n, status="PENDING") for n in ("app.a", "app.b")]
    no_point = MigrationJob(job_id="job-1", status="FAILED", chunks=pending, watermark=None)
    with_point = no_point.model_copy(update={"watermark": _watermark()})

    def _retry_enabled(job) -> bool:
        ui = _render_step(monkeypatch, state=_fc(["app.a", "app.b"]), job=job,
                          session=_G17Session(), tables=["app.a", "app.b"])
        (btn,) = [b for b in ui.btns if b.text.startswith("Retry unfinished tables")]
        return btn.enabled

    assert _retry_enabled(with_point) is True
    assert _retry_enabled(no_point) is False
    # And the engine refuses it before seeding, reading or writing anything.
    migrator = _FakeMigrator()
    migrator._inputs = SimpleNamespace(is_cdc_migration=True)  # noqa: SLF001
    manager = JobManager()
    job_id = manager.submit(lambda handle: run_full_load_retry(
        handle, pending, _tables(), migrator=migrator, error_log=ErrorLogStore(), watermark=None,
    ))
    assert manager.wait(job_id, timeout=5.0)
    assert manager.get_error(job_id) == f"{FullLoadRefusedError.__name__}: {NO_START_POINT_RETRY_REFUSAL}"
    assert migrator.migrated == [] and manager.get_status(job_id).chunks == []


@pytest.mark.parametrize("update, stack, shown", [
    ({}, "never started", True),                                     # the default route
    ({}, None, True),                                                # CDC state unread
    ({"gtid_mode": "ON", "gtid_executed": "u:1-5"}, "never started", False),  # a GTID point
])
def test_the_full_load_only_completion_notice_says_the_point_is_not_exact(
        monkeypatch, update, stack, shown) -> None:
    # FAILS WITHOUT THE FIX: "CDC only ... streams from this Full Load's watermark (no
    # re-snapshot)" with no word that a Full load only run's binlog position is not exact.
    from dsql_migrator.ui.data_migration import FULL_LOAD_ONLY_UNFENCED_NOTE

    from tests.test_g25_fl_r6 import NEVER_STARTED
    from tests.test_g25_fl_r7 import _wm_job
    from tests.test_g25_fl_r8 import _notice

    job = _wm_job("mysql-bin.000009", 4)
    job.watermark = job.watermark.model_copy(update=update)
    body = _notice(monkeypatch, job=job, stack=NEVER_STARTED if stack else None)
    assert (FULL_LOAD_ONLY_UNFENCED_NOTE.strip() in body) is shown, body


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_every_page_that_lists_the_sources_footprint_names_the_flush(lang) -> None:
    # The pages that list what the tool runs on a source said a MySQL source is never
    # written and a read-capable user is enough; a Full load + CDC run on a source not
    # running gtid_mode=ON now runs FLUSH BINARY LOGS (and needs RELOAD). The code first.
    from pathlib import Path

    assert fence.FLUSH_BINARY_LOGS in fence.FENCE_STATEMENTS
    root = Path(__file__).resolve().parents[1]
    manual = root / "docs" / "manual" / lang
    pages = [manual / name for name in (
        "00-before-you-begin.md", "01-setup.md", "03-full-load.md",
        "04-cdc-and-dsql-constraints.md", "10-conclusion.md", "11-customer-faq.md", "README.md",
    )]
    pages.append(root / ("README.md" if lang == "en" else f"README.{lang}.md"))
    for page in pages:
        text = page.read_text(encoding="utf-8")
        assert "FLUSH BINARY LOGS" in text, page
        # The pages that list the source user's privileges name the one it needs.
        if not page.name.startswith(("04-", "10-")):
            assert "RELOAD" in text, page
