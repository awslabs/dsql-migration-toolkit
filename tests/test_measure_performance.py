# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""scripts/measure_performance.py full-load: the measurement it reports (R0).

Never connects to anything: the script is imported as a module, its ``.env`` values are
emptied, and every source / target / job seam is a double. Pinned here:

* progress windows end on wall-clock multiples of the interval (CloudWatch's minutes) and
  the last one at the job's exact end, with the workers' counters per window;
* the report carries the workers' counters -- OC000 / OC001 apart, the INSERT round trip
  in ``write_rtt_ms`` -- never the start point's coordinate;
* ``--wait-sys-jobs`` waits a bounded time for Aurora DSQL's async jobs, and does not load
  when they do not drain; ``--cdc-mode`` runs the load as a Full load + CDC run.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from dsql_migrator.core.load_stats import LoadStats, LoadStatsBoard
from dsql_migrator.core.models import ChunkState, MigrationJob, Watermark

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "measure_performance.py"


@pytest.fixture
def mp(monkeypatch):
    """The script as a module, with no .env value reachable and its loggers restored."""
    monkeypatch.setattr(sys, "path", list(sys.path))  # the script prepends src/ and scripts/
    spec = importlib.util.spec_from_file_location("measure_performance_under_test", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "ENV", {})
    occ = logging.getLogger("dsql_migrator.core.occ")
    saved = (list(occ.handlers), occ.level, occ.propagate)
    yield module
    occ.handlers[:] = saved[0]
    occ.setLevel(saved[1])
    occ.propagate = saved[2]


class _Err(Exception):
    def __init__(self, message, sqlstate):
        super().__init__(message)
        self.sqlstate = sqlstate


def _unit(*, batches=0, oc000=0, oc001=0, xx000=0, cpu=None) -> dict:
    stats = LoadStats()
    for _ in range(batches):
        stats.record_batch(0.8, failed=False)
        stats.record_execute(0.75)
        stats.record_statement(1)
    for message, n in (("(OC000)", oc000), ("(OC001)", oc001)):
        for _ in range(n):
            stats.record_retry(_Err(message, "40001"), exhausted=False)
    for _ in range(xx000):
        stats.record_retry(_Err("unavailable", "XX000"), exhausted=False)
    snapshot = stats.snapshot()
    if cpu is not None:
        snapshot.update(cpu_s=cpu, wall_s=10.0, peak_rss_bytes=1 << 30)
    return snapshot


def test_the_script_names_resolve(mp) -> None:
    from tests.test_no_undefined_globals import _undefined_globals

    assert _undefined_globals(mp) == []
    assert not hasattr(mp, "_OccTraceCounter"), "the parent-logger counter is back"


# --------------------------------------------------------------------------- windows


@pytest.mark.parametrize("now, interval, boundary", [
    (1_000_000_007.3, 30.0, 1_000_000_020.0),
    (1_000_000_019.5, 30.0, 1_000_000_050.0),   # 0.5 s away: no sliver window
    (1_000_000_020.0, 60.0, 1_000_000_080.0),
    (1_000_000_021.0, 15.0, 1_000_000_035.0),
    (1_000_000_019.2, 1.0, 1_000_000_020.0),
    (1_000_000_019.9, 1.0, 1_000_000_021.0),
])
def test_windows_end_on_wall_clock_multiples_of_the_interval(mp, now, interval, boundary):
    assert mp._next_boundary(now, interval) == boundary


class _Clock:
    """``time`` for the script: wall and monotonic both read the fake clock."""

    def __init__(self, start: float) -> None:
        self.now = start

    def time(self) -> float:
        return self.now

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += max(0.0, seconds)


class _Jobs:
    """JobManager stand-in: the job's thread ends at ``finish_at`` on the fake clock, and
    every wait lets ``tick`` play the workers up to the moment it returns."""

    def __init__(self, clock, finish_at, job, tick) -> None:
        self.clock, self.finish_at, self.job, self.tick = clock, finish_at, job, tick

    def wait(self, job_id, timeout=None):
        target = self.clock.now + (timeout or 0.0)
        if target >= self.finish_at:
            self.clock.now = self.finish_at
            self.tick(self.clock.now, done=True)
            return True
        self.clock.now = target
        self.tick(self.clock.now, done=False)
        return False

    def get_status(self, job_id):
        return self.job.model_copy(deep=True)


def test_the_monitor_aligns_its_windows_and_ends_on_the_jobs_exact_finish(mp, monkeypatch):
    start = 1_000_000_007.3
    clock = _Clock(start)
    monkeypatch.setattr(mp, "time", clock)
    lines: list = []
    monkeypatch.setattr(mp, "log", lines.append)
    board = LoadStatsBoard()
    job = MigrationJob(job_id="j", status="RUNNING",
                       chunks=[ChunkState(chunk_id="perf.t1", status="IN_PROGRESS")])
    ticks = {"n": 0}

    def tick(now, *, done):
        ticks["n"] += 1
        n = ticks["n"]
        job.chunks[0].rows_loaded = 1000 * n
        # One OC001 per window, cumulative in the unit's snapshot.
        board.record("perf.t1", None, _unit(batches=2 * n, oc001=n, cpu=0.5 * n), final=done)
        if done:
            job.chunks[0].status = "DONE"
            job.status = "DONE"

    out = mp._monitor_full_load(_Jobs(clock, start + 85.95, job, tick), "j", ["perf.t1"],
                                interval=30, board=board)

    ends = [w["end"] for w in out["windows"]]
    assert ends == [
        "2001-09-09T01:47:00.000+00:00",   # first boundary after 01:46:47.3
        "2001-09-09T01:47:30.000+00:00",
        "2001-09-09T01:48:00.000+00:00",
        "2001-09-09T01:48:13.250+00:00",   # the job's own end, not a poll
    ]
    assert out["finished_at"] == pytest.approx(start + 85.95)
    assert [w["oc001"] for w in out["windows"]] == [1, 1, 1, 1]
    assert [w["batches"] for w in out["windows"]] == [2, 2, 2, 2]
    assert [w["rows"] for w in out["windows"]] == [1000, 1000, 1000, 1000]
    assert out["windows"][1]["seconds"] == 30.0 and out["windows"][1]["rows_per_sec"] == 33.3
    assert out["windows"][1]["worker_cores"] == pytest.approx(0.5 / 30, abs=0.01)
    progress = [line for line in lines if line.startswith("progress [")]
    assert progress[1].startswith("progress [01:47:00-01:47:30 UTC]")
    assert any("OC000 0, OC001 1" in line for line in lines)


def test_the_monitor_without_counters_still_reports_rows(mp, monkeypatch) -> None:
    clock = _Clock(1_000_000_000.5)
    monkeypatch.setattr(mp, "time", clock)
    monkeypatch.setattr(mp, "log", lambda _line: None)
    job = MigrationJob(job_id="j", status="RUNNING",
                       chunks=[ChunkState(chunk_id="t", status="IN_PROGRESS")])

    def tick(now, *, done):
        job.chunks[0].rows_loaded += 10
        if done:
            job.status = "DONE"

    out = mp._monitor_full_load(_Jobs(clock, 1_000_000_012.0, job, tick), "j", ["t"],
                                interval=5)
    assert [w["rows"] for w in out["windows"]] == [10, 10, 10]
    assert "oc001" not in out["windows"][0]


def test_a_windows_occ_total_counts_the_index_submissions_too(mp) -> None:
    # CloudWatch's OccConflicts counts every transaction an OCC conflict aborted. The
    # workers keep the post-load CREATE INDEX ASYNC retries apart from the data's, so a
    # window built from the data's alone missed every OC001 an index submission met.
    stats = LoadStats()
    stats.record_retry(_Err("(OC000)", "40001"), exhausted=False)
    stats.record_retry(_Err("unavailable", "XX000"), exhausted=False)
    stats.record_index_retry(_Err("(OC001)", "40001"))
    stats.record_index_retry(_Err("server closed the connection", None))
    counts = mp._window_counts(stats.snapshot(), 60.0)
    assert (counts["oc000"], counts["oc001"], counts["retries_other"]) == (1, 0, 1)
    assert counts["index_retries"] == 2 and counts["occ_conflicts"] == 2


def _process_unit(cpu: float, wall: float, *, blocked: float = 0.0) -> dict:
    """A process unit's cumulative snapshot: ``cpu`` CPU seconds over ``wall`` seconds."""
    snapshot = LoadStats().snapshot()
    snapshot.update(cpu_s=cpu, wall_s=wall, peak_rss_bytes=1 << 20, reader_blocked_s=blocked)
    return snapshot


def test_a_tables_window_counts_only_the_workers_that_ran_in_it(mp, monkeypatch) -> None:
    # perf.big is read in two ranges. In the second window range 0 has finished and range 1
    # runs alone at 0.9 core: dividing by both ranges read 0.45 core "per worker" and half
    # the queue waits -- the client-bound vs target-bound reading turned upside down.
    start = 1_000_000_000.0
    clock = _Clock(start)
    monkeypatch.setattr(mp, "time", clock)
    monkeypatch.setattr(mp, "log", lambda _line: None)
    board = LoadStatsBoard()
    job = MigrationJob(job_id="j", status="RUNNING", chunks=[
        ChunkState(chunk_id="perf.big", status="IN_PROGRESS"),
        ChunkState(chunk_id="perf.small", status="IN_PROGRESS"),
    ])
    ticks = {"n": 0}

    def tick(now, *, done):
        ticks["n"] += 1
        big, small = job.chunks
        if ticks["n"] == 1:  # both ranges run the whole first window
            board.record("perf.big", 0, _process_unit(9.0, 10.0, blocked=5.0))
            board.record("perf.big", 1, _process_unit(9.0, 10.0, blocked=5.0))
            board.record("perf.small", None, _process_unit(2.0, 10.0))
            big.rows_loaded, small.rows_loaded = 100, 10
        elif ticks["n"] == 2:  # range 0 ended as the window began; small ends within it
            board.record("perf.big", 0, _process_unit(9.0, 10.0, blocked=5.0), final=True)
            board.record("perf.big", 1, _process_unit(18.0, 20.0, blocked=7.5))
            board.record("perf.small", None, _process_unit(4.0, 20.0), final=True)
            big.rows_loaded, small.rows_loaded, small.status = 200, 20, "DONE"
        else:
            board.record("perf.big", 1, _process_unit(20.0, 22.5, blocked=7.5), final=True)
            big.rows_loaded, big.status, job.status = 260, "DONE", "DONE"

    out = mp._monitor_full_load(_Jobs(clock, start + 25.0, job, tick), "j",
                                ["perf.big", "perf.small"], interval=10, board=board)
    first, second, last = out["windows"]
    assert first["tables"]["perf.big"] == {
        "status": "IN_PROGRESS", "rows": 100, "rows_per_sec": None,  # started in it
        "workers": 2.0, "cores": 1.8, "cores_per_worker": 0.9,
        "reader_blocked_pct": 50.0, "writer_wait_pct": 0.0, "batch_ms_p50": None,
        "retries": 0,
    }
    big = second["tables"]["perf.big"]
    assert (big["workers"], big["cores"], big["cores_per_worker"]) == (1.0, 0.9, 0.9)
    assert big["reader_blocked_pct"] == 25.0 and big["rows_per_sec"] == 10.0
    # The run: 2 processes on average in the second window, 0.55 core each.
    assert (second["worker_processes"], second["cores_per_worker"]) == (2.0, 0.55)
    # A table that ended within a window still gets that last, partial window -- once.
    assert second["tables"]["perf.small"]["status"] == "DONE"
    assert second["tables"]["perf.small"]["rows"] == 10
    assert set(last["tables"]) == {"perf.big"}
    assert last["tables"]["perf.big"]["status"] == "DONE"
    json.dumps(out["windows"])  # report-ready


# --------------------------------------------------------------------------- report


def _job(*, watermark=None) -> MigrationJob:
    t0 = datetime(2026, 10, 25, 3, 0, 0, tzinfo=timezone.utc)
    return MigrationJob(job_id="j", status="DONE", watermark=watermark, chunks=[
        ChunkState(chunk_id="perf.a", status="DONE", rows_loaded=4000, started_at=t0,
                   finished_at=t0 + timedelta(seconds=10)),
        ChunkState(chunk_id="perf.b", status="DONE", rows_loaded=6000, started_at=t0,
                   finished_at=t0 + timedelta(seconds=12), reader_shards=2),
    ])


def _args(**overrides):
    base = dict(schema="perf", no_prefetch=False, table_parallelism=4, batch_parallelism=8)
    base.update(overrides)
    return SimpleNamespace(**base)


def test_the_report_carries_the_workers_counters(mp) -> None:
    board = LoadStatsBoard()
    board.record("perf.a", None, _unit(batches=4, oc000=1, cpu=2.0), final=True)
    board.record("perf.b", 0, _unit(batches=3, oc001=2, xx000=1, cpu=3.0), final=True)
    board.record("perf.b", 1, _unit(batches=3, cpu=3.0), final=True)
    watermark = Watermark(snapshot_timestamp=datetime(2026, 10, 25, tzinfo=timezone.utc),
                          binlog_file="mysql-bin.004242", binlog_position=4242,
                          start_point_fence="flush", table_row_counts={})
    report = mp._full_load_report(
        _args(), _job(watermark=watermark), board, elapsed=13.0,
        started_at=1_761_361_200.0, finished_at=1_761_361_213.0,
        monitor={"peak": {"cpu_pct": 190.0}, "windows": [{"rows": 1}]}, cdc_mode=True,
    )
    occ = report["occ"]
    assert occ["source"] == "workers"
    assert (occ["batches"], occ["statements"], occ["attempts"]) == (10, 10, 10)
    assert (occ["oc000"], occ["oc001"], occ["total_retries"]) == (1, 2, 4)
    assert occ["retries_by_sqlstate"] == {"40001/OC000": 1, "40001/OC001": 2, "XX000": 1}
    # The INSERT round trip fills the field the parent-logger counter left empty.
    assert report["write_rtt_ms"]["samples"] == 10
    assert report["write_rtt_ms"]["p50"] == pytest.approx(750.0, rel=0.05)
    assert report["batch_ms"]["p95"] == pytest.approx(800.0, rel=0.05)
    assert report["load_stats"]["worker_cores"] == pytest.approx(8.0 / 30.0, abs=0.001)
    b = next(t for t in report["per_table"] if t["table"] == "perf.b")
    assert b["reader_shards"] == 2 and b["load_stats"]["units"] == 2
    assert b["load_stats"]["oc001"] == 2
    assert report["data_phase_seconds"] == 12.0
    assert report["data_phase_started_at"] == "2026-10-25T03:00:00.000+00:00"
    assert report["started_at"] == "2025-10-25T03:00:00.000+00:00"
    assert report["windows"] == [{"rows": 1}] and report["peak_cpu_pct"] == 190.0
    assert report["cdc_mode"] is True
    assert report["start_point"] == {"binlog": True, "gtid": False, "wal_lsn": False,
                                     "fence": "flush"}
    text = json.dumps(report)
    assert "mysql-bin.004242" not in text and "4242" not in text  # never the coordinate
    lines: list = []
    mp.log = lines.append
    mp._log_full_load_summary(report)
    assert any("Full load + CDC: start point binlog=True" in line
               and "read in ranges: {'perf.b': 2}" in line for line in lines), lines
    assert any("OC000 1, OC001 2" in line for line in lines), lines
    # The keys the local A/B comparison reads are all still there.
    for key in ("data_phase_rows_per_sec", "overall_rows_per_sec", "wall_seconds"):
        assert key in report
    assert "pct_batches_with_retry" in occ and report["variant"]["label"] == "keep-integer"


def test_the_report_without_counters_keeps_its_shape(mp) -> None:
    report = mp._full_load_report(
        _args(), _job(), LoadStatsBoard(), elapsed=13.0, started_at=0.0, finished_at=13.0,
        monitor={}, cdc_mode=False,
    )
    assert report["occ"]["batches"] == 0 and report["occ"]["pct_batches_with_retry"] == 0.0
    assert report["write_rtt_ms"] == {"samples": 0, "p50": None, "p95": None, "max": None,
                                      "mean": None}
    assert report["start_point"] is None and report["load_window"]["seconds"] is None
    mp._log_full_load_summary(report)  # prints without counters


# --------------------------------------------------------------------------- sys.jobs


class _Cursor:
    def __init__(self, rows):
        self.rows, self.statements = rows, []

    def execute(self, statement, params=None):
        self.statements.append(statement)

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class _Conn:
    def __init__(self, rows):
        self._rows = rows
        self.closed = False
        self.statements: list = []

    def cursor(self):
        cursor = _Cursor(self._rows())
        cursor.statements = self.statements
        return cursor

    def close(self):
        self.closed = True


def test_the_gate_waits_until_no_async_job_is_running(mp, monkeypatch) -> None:
    monkeypatch.setattr(mp, "log", lambda _line: None)
    reads = iter([
        [("processing",), ("completed",)],
        [("SUBMITTED",), ("failed",)],
        [("completed",), ("failed",)],
    ])
    connections: list = []

    def connect():
        connections.append(_Conn(lambda: next(reads)))
        return connections[-1]

    sleeps: list = []
    clock = {"t": 0.0}

    def sleep(seconds):
        sleeps.append(seconds)
        clock["t"] += seconds

    gate = mp._wait_for_sys_jobs(connect, 600, poll=15, sleep=sleep, clock=lambda: clock["t"])
    assert gate == {"drained": True, "waited_seconds": 30.0, "running": {},
                    "read_errors": 0}
    assert sleeps == [15, 15]
    assert len(connections) == 1 and connections[0].closed
    assert connections[0].statements == ["SELECT status FROM sys.jobs"] * 3


def test_the_gate_is_bounded_and_never_reads_unknown_as_drained(mp, monkeypatch) -> None:
    monkeypatch.setattr(mp, "log", lambda _line: None)
    clock = {"t": 0.0}

    def sleep(seconds):
        clock["t"] += seconds

    busy = mp._wait_for_sys_jobs(lambda: _Conn(lambda: [("processing",)]), 40, poll=15,
                                 sleep=sleep, clock=lambda: clock["t"])
    assert busy["drained"] is False and busy["running"] == {"processing": 1}
    assert busy["waited_seconds"] == 40.0

    clock["t"] = 0.0
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise OSError("connect failed")
        return _Conn(lambda: [])

    recovered = mp._wait_for_sys_jobs(flaky, 600, poll=15, sleep=sleep,
                                      clock=lambda: clock["t"])
    assert recovered["drained"] is True and recovered["read_errors"] == 2

    clock["t"] = 0.0

    def down():
        raise OSError("connect failed")

    unknown = mp._wait_for_sys_jobs(down, 30, poll=15, sleep=sleep, clock=lambda: clock["t"])
    assert unknown["drained"] is False and unknown["read_errors"] >= 2
    assert unknown["running"] == {}  # never read: nothing is claimed to be running


def test_the_gate_keeps_the_last_jobs_it_read_when_reads_then_fail(mp, monkeypatch) -> None:
    # The last read failing must not erase what the one before it saw.
    monkeypatch.setattr(mp, "log", lambda _line: None)
    clock = {"t": 0.0}
    reads = {"n": 0}

    def rows():
        reads["n"] += 1
        if reads["n"] > 1:
            raise OSError("the connection dropped")
        return [("processing",), ("Processing",), ("completed",)]

    def sleep(seconds):
        clock["t"] += seconds

    gate = mp._wait_for_sys_jobs(lambda: _Conn(rows), 40, poll=15, sleep=sleep,
                                 clock=lambda: clock["t"])
    assert gate == {"drained": False, "waited_seconds": 40.0, "running": {"processing": 2},
                    "read_errors": 3}


# --------------------------------------------------------------------------- the command


def _command_doubles(mp, monkeypatch, tmp_path):
    """Every seam cmd_full_load reaches, as doubles. Returns what they saw."""
    import _e2e_tables  # the script put scripts/ on sys.path

    import dsql_migrator.ui.data_migration as data_migration
    import dsql_migrator.ui.evaluation as evaluation
    from dsql_migrator.core.models import (
        ColumnDef, SourceConnectionConfig, SourceInventory, TableDef, TargetConnectionConfig,
    )
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    seen: dict = {}
    table = TableDef(name="perf.t1", columns=[ColumnDef(name="id", mysql_type="int",
                                                        nullable=False)], primary_key=["id"])
    # cmd_full_load exports the tunables into os.environ; restore them after the test.
    for key in ("TABLE_PARALLELISM", "BATCH_PARALLELISM"):
        monkeypatch.setenv(f"DSQL_MIGRATOR_FULL_LOAD_{key}", "1")
    monkeypatch.setattr(mp, "log", lambda line: seen.setdefault("log", []).append(line))
    monkeypatch.setattr(mp, "_configs", lambda schema: (
        SourceConnectionConfig(host="db", database=schema),
        TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"),
        None,
    ))

    class _Introspector:
        def introspect(self, source):
            return SourceInventory(tables=[table.model_copy(deep=True)])

    monkeypatch.setattr(evaluation, "_default_introspector_factory",
                        lambda password: _Introspector())
    monkeypatch.setattr(_e2e_tables, "tables_for", lambda schema: ["t1"])

    def _migrator_factory(inputs):
        seen["inputs"] = inputs
        return object()

    def _run_full_load(handle, tables, *, migrator, error_log, load_stats=None):
        seen["board"] = load_stats
        handle.update(lambda job: engine._seed_chunks(job, [t.name for t in tables]))
        handle.update(lambda job: engine._start_chunk(job, "perf.t1"))
        load_stats.record("perf.t1", None, _unit(batches=5, oc001=1, cpu=1.0), final=True)
        handle.update(lambda job: engine._complete_chunk(job, "perf.t1", 10_000))

    monkeypatch.setattr(data_migration, "default_migrator_factory", _migrator_factory)
    monkeypatch.setattr(data_migration, "run_full_load", _run_full_load)
    report = tmp_path / "report.json"
    args = SimpleNamespace(
        schema="perf", yes=True, tables=None, table_parallelism=4, batch_parallelism=8,
        batch_rows=0, progress_interval=1.0, no_prefetch=False, reader_shards=0,
        composite_leading="", report=str(report), cdc_mode=False, wait_sys_jobs=None,
    )
    return args, seen, report


def test_the_command_reports_the_workers_counters_and_runs_cdc_mode(mp, monkeypatch, tmp_path):
    args, seen, report_path = _command_doubles(mp, monkeypatch, tmp_path)
    args.cdc_mode = True
    assert mp.cmd_full_load(args) == 0
    assert seen["inputs"].is_cdc_migration is True       # a Full load + CDC run
    assert seen["inputs"].cdc_stack_name is None         # ... with no slot, no CDC stack
    assert isinstance(seen["board"], LoadStatsBoard)
    report = json.loads(report_path.read_text())
    assert report["status"] == "DONE" and report["cdc_mode"] is True
    assert report["occ"]["oc001"] == 1 and report["occ"]["batches"] == 5
    assert report["per_table"][0]["load_stats"]["batches"] == 5
    assert report["sys_jobs_gate"] is None
    assert report["windows"], "no progress window was recorded"

    args.cdc_mode = False
    args.report = ""
    assert mp.cmd_full_load(args) == 0
    assert seen["inputs"].is_cdc_migration is False


def test_the_command_does_not_load_when_sys_jobs_do_not_drain(mp, monkeypatch, tmp_path):
    args, seen, report_path = _command_doubles(mp, monkeypatch, tmp_path)
    args.wait_sys_jobs = 60.0
    waits: list = []

    def _busy(connect, max_wait, **_kw):
        waits.append(max_wait)
        return {"drained": False, "waited_seconds": 60.0, "running": {"processing": 3},
                "read_errors": 0}

    monkeypatch.setattr(mp, "_wait_for_sys_jobs", _busy)
    assert mp.cmd_full_load(args) == 3
    assert waits == [60.0] and "board" not in seen and not report_path.exists()
    assert any("still 3 job(s) running" in line for line in seen["log"])

    # sys.jobs never read: said so, not "still 0 job(s) running".
    seen["log"].clear()
    monkeypatch.setattr(mp, "_wait_for_sys_jobs", lambda connect, max_wait, **_kw: {
        "drained": False, "waited_seconds": 60.0, "running": {}, "read_errors": 5})
    assert mp.cmd_full_load(args) == 3
    assert any("could not be read in 60s (5 failed read(s))" in line for line in seen["log"])
    assert not any("still 0 job(s)" in line for line in seen["log"])

    monkeypatch.setattr(mp, "_wait_for_sys_jobs", lambda connect, max_wait, **_kw: {
        "drained": True, "waited_seconds": 12.0, "running": {}, "read_errors": 0})
    assert mp.cmd_full_load(args) == 0
    gate = json.loads(report_path.read_text())["sys_jobs_gate"]
    assert gate == {"drained": True, "waited_seconds": 12.0, "running": {}, "read_errors": 0,
                    "max_wait_seconds": 60.0}


@pytest.mark.parametrize("argv, expected", [
    ([], None), (["--wait-sys-jobs"], 1800.0), (["--wait-sys-jobs", "300"], 300.0),
])
def test_the_new_flags_parse(mp, monkeypatch, argv, expected) -> None:
    captured: dict = {}
    monkeypatch.setattr(mp, "cmd_full_load", lambda args: captured.setdefault("args", args) and 0)
    monkeypatch.setenv("DB_HOST", "db.invalid")
    monkeypatch.setenv("DB_PASSWORD", "unused-by-this-test")
    monkeypatch.setattr("sys.argv", ["measure_performance.py", "full-load", *argv])
    assert mp.main() == 0
    assert captured["args"].wait_sys_jobs == expected
    assert captured["args"].cdc_mode is False
    monkeypatch.setattr("sys.argv", ["measure_performance.py", "full-load", "--cdc-mode"])
    captured.clear()
    mp.main()
    assert captured["args"].cdc_mode is True
