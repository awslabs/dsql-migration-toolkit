#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Measure migration PERFORMANCE on real infrastructure (throughput + CDC lag + OCC).

The functional harnesses answer "is it correct?"; this one answers "how fast, and
how does it behave under pressure?" -- the dimension none of the existing E2E
scripts measure. It drives the tool's OWN engine (nothing re-implemented) and
reports numbers an adopter needs to size a large-scale migration.

Two subcommands:

  full-load   Time the tool's Full Load over the selected schema and report
              wall-time, total + per-table rows/sec, and the write path as the
              table workers themselves count it (core.load_stats; counts and
              timings only): per-batch and per-INSERT time (p50/p95), attempts,
              retries by SQLSTATE with Aurora DSQL's OC000 (row conflict) and OC001
              (schema change) told apart, 23505 recoveries, reconnects and the
              connections kept after an error, how long the reader waited on a
              full read-ahead queue vs the writers on an empty one, and each
              worker process's CPU seconds and peak RSS. The workers are
              spawned processes with no logging, so these come back
              through the progress queue and the workers' results -- the parent's
              logger never saw a batch, which is why earlier reports said "OCC 0".
              Load, post-load index submission and the post-load COUNT(*) are timed
              apart; the data phase and the run carry exact UTC timestamps, and the
              progress windows end on wall-clock multiples of --progress-interval
              (12:03:00, 12:03:30, ...) so they line up with CloudWatch minutes.
              --wait-sys-jobs waits (bounded) for Aurora DSQL's async jobs to drain
              before starting; --wait-index-builds waits (bounded, 1800 s by default)
              after the run for the CREATE INDEX ASYNC builds it submitted -- read by
              their job ids, as the product's watch reads them -- and reports when the
              indexes were ready and the end-to-end time (run start to the last build
              finished, to the poll); --reader-shards sets Reader shards per large
              table for the run (0 = automatic, the loader's default; 1 = off; 2-32 =
              exactly that many ranges; unset: the environment's, else automatic);
              --cdc-mode runs the load as a Full load + CDC run
              (start point captured and fenced as the product does, no CDC stack
              needed) so a MySQL table's --reader-shards ranges can be read as parts
              from separate snapshots (without it, and with automatic ranges, one
              reader session reads them in turn from one snapshot);
              --inventory-order starts the tables in inventory order instead of the
              largest first, the other arm of the start-order A/B; --range-in-flight /
              --in-flight-budget set the INSERT batches in flight at most per key
              range (4) and per run (128) for the in-flight A/B, and the report says
              which caps applied and each table's. The loader puts a recreated table's
              secondary indexes on the empty table before its rows and waits (bounded)
              for their builds (P4d); --indexes-after-load submits them after every
              table's data instead, as builds before P4d did, the other arm of that A/B.

  cdc-lag     Measure END-TO-END CDC replication lag (source commit -> visible on
              the DSQL target) under a SUSTAINED trickle and under BURSTS, reporting
              p50 / p95 / max. The harness owns a dedicated ``cdc_perf.pulse`` table
              and is its SOLE writer: each row carries a monotonically increasing
              ``seq`` and the harness remembers the LOCAL wall-clock send time per
              seq, then polls the target for the newest replicated seq and computes
              lag on that ONE clock (no source/target clock-skew in the number).

PREREQUISITE for cdc-lag: an active CDC pipeline whose include-list covers
``cdc_perf.pulse`` (run ``--setup`` first to create the table, then point/redeploy
CDC to include it, or add it before starting CDC). Without CDC running, the target
never receives rows and every sample reports "no target row yet".

Connection reuses ``.env`` (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD source;
TARGET_ENDPOINT/TARGET_REGION/TARGET_DATABASE/TARGET_USERNAME DSQL), like the other
scripts. Safety: full-load is DESTRUCTIVE (DROP+recreate target tables, needs --yes);
cdc-lag WRITES only to its own ``cdc_perf`` schema on the source.

Usage (from repo root):
    set -a; source .env; set +a
    # Full Load throughput + OCC rate (DROP+recreate target, so --yes):
    .venv/bin/python scripts/measure_performance.py full-load --yes
    .venv/bin/python scripts/measure_performance.py full-load --yes \
        --table-parallelism 8 --batch-parallelism 16     # hotter -> more OCC
    # A benchmark arm: start only once sys.jobs is drained (wait up to 30 min), 60 s
    # windows, and read big MySQL tables in 4 parts, each from its own snapshot (needs a
    # binlog start point):
    .venv/bin/python scripts/measure_performance.py full-load --yes --wait-sys-jobs \
        --progress-interval 60 --cdc-mode --reader-shards 4
    # The one-reader baseline (the loader's default splits large tables automatically):
    .venv/bin/python scripts/measure_performance.py full-load --yes --reader-shards 1
    # Report the load only, without waiting for the index builds it submitted:
    .venv/bin/python scripts/measure_performance.py full-load --yes --wait-index-builds 0
    # The in-flight A/B: 8 INSERT batches per key range instead of the loader's 4:
    .venv/bin/python scripts/measure_performance.py full-load --yes --range-in-flight 8
    # The index-order A/B: secondary indexes after every table's data (before P4d):
    .venv/bin/python scripts/measure_performance.py full-load --yes --indexes-after-load

    # CDC lag (CDC must already stream cdc_perf.pulse):
    .venv/bin/python scripts/measure_performance.py cdc-lag --setup        # create table
    .venv/bin/python scripts/measure_performance.py cdc-lag --duration 120 --interval 0.2
    .venv/bin/python scripts/measure_performance.py cdc-lag --profile burst \
        --burst-size 500 --burst-every 15 --duration 120

    # write a machine-readable report:
    .venv/bin/python scripts/measure_performance.py full-load --yes --report perf.json

This is an operational utility (NOT shipped in the app).
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import logging
import math
import os
import statistics
import sys
import threading
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _e2e_tables


def _env(path: str) -> dict:
    out: dict = {}
    try:
        for raw in open(path, encoding="utf-8"):
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                out[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return out


ENV = _env(os.path.join(_ROOT, ".env"))


def cfg(key: str, default: str = "") -> str:
    return os.environ.get(key) or ENV.get(key) or default


def log(msg: str) -> None:
    import datetime as _dt
    print(f"[{_dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def region() -> str:
    ep = cfg("TARGET_ENDPOINT")
    parts = ep.split(".")
    return cfg("TARGET_REGION") or (parts[2] if len(parts) > 2 else "us-east-1")


def _configs(schema: str):
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import (
        SourceConnectionConfig, SourceType, TargetConnectionConfig,
    )
    # PERF_SOURCE_ENGINE selects the source dialect (default MySQL -- byte-identical to the
    # original perf harness). For a PostgreSQL source set it to "postgres": the Full Load path
    # routes through the injected engine factory / dialect_for(source_type), so this source_type
    # is the only wiring the full-load subcommand needs (the pymysql helper below is cdc-lag
    # only, unused by full-load).
    engine = cfg("PERF_SOURCE_ENGINE", "mysql").strip().lower()
    is_pg = engine in ("postgres", "postgresql", "pg")
    source = SourceConnectionConfig(
        source_type=SourceType.POSTGRES if is_pg else SourceType.MYSQL,
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "5432" if is_pg else "3306")),
        # MySQL: a database IS the schema, so connect to `schema`. PostgreSQL: a database
        # CONTAINS schemas, so connect to the fixed DB (DB_NAME, default perfdb) and let
        # `schema` (the --schema arg) qualify the tables as `schema.table` (matching PG
        # introspection's schema-qualified names -- see the `f"{schema}.{t}"` below).
        database=(cfg("DB_NAME", "perfdb") if is_pg else schema),
        username=cfg("DB_USER", "admin"),
    )
    target = TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"), region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    return source, target, SecretValue(cfg("DB_PASSWORD"))


def _pct(values: list[float], q: float) -> float:
    """Percentile ``q`` (0..100) of ``values`` (linear interpolation); 0 if empty."""
    if not values:
        return 0.0
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    pos = (len(s) - 1) * (q / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(s) - 1)
    frac = pos - lo
    return s[lo] * (1 - frac) + s[hi] * frac


# =========================================================================== #
# full-load: throughput + the table workers' own write-path counters
# =========================================================================== #
# The counters come from the table workers themselves (core.load_stats), carried to the
# parent over the progress queue and in each worker's result and merged per table and
# per run by a LoadStatsBoard passed to run_full_load. Counts and timings only: no row
# value, key or statement text is ever part of them.


def _utc(epoch: "float | None") -> str:
    """'HH:MM:SS' in UTC (CloudWatch's clock), or '?'."""
    if epoch is None:
        return "?"
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.timezone.utc).strftime(
        "%H:%M:%S"
    )


def _iso(value) -> "str | None":
    """An epoch float or an aware datetime as an ISO-8601 UTC string (ms), else None."""
    if value is None:
        return None
    if not isinstance(value, datetime.datetime):
        value = datetime.datetime.fromtimestamp(float(value), tz=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc).isoformat(timespec="milliseconds")


def _next_boundary(now: float, interval: float) -> float:
    """The next wall-clock instant (epoch s) that is a whole multiple of ``interval``.

    12:03:00, 12:03:30, 12:04:00, ... for 30 s, so the progress windows line up with
    CloudWatch's minutes whenever ``interval`` divides 60 (or is a multiple of it). One
    closer than half an interval (at most a second) is skipped, so no window is a sliver.
    """
    boundary = (math.floor(now / interval) + 1) * interval
    if boundary - now < min(1.0, interval / 2.0):
        boundary += interval
    return boundary


def _fmt_ms(ms: "float | None") -> str:
    if ms is None:
        return "?"
    return f"{ms / 1000.0:.2f}s" if ms >= 1000 else f"{ms:.0f}ms"


def _window_counts(delta: dict, seconds: float) -> dict:
    """The write-path counters of one window, from a ``diff_load_stats`` delta."""
    from dsql_migrator.core.load_stats import histogram_percentile

    retries = dict(delta.get("retries") or {})
    oc000 = retries.pop("40001/OC000", 0)
    oc001 = retries.pop("40001/OC001", 0)
    oc_unparsed = retries.pop("40001", 0)
    # The post-load CREATE INDEX ASYNC submissions' retries, kept apart from the data's.
    index_retries = dict(delta.get("index_retries") or {})
    index_occ = sum(n for code, n in index_retries.items() if str(code).startswith("40001"))
    cpu = delta.get("cpu_s")
    # Seconds the worker processes ran in the window, summed over them.
    busy = float(delta.get("wall_s") or 0.0)
    batch_ms = delta.get("batch_ms") or {}
    execute_ms = delta.get("execute_ms") or {}
    return {
        "batches": int(delta.get("batches") or 0),
        "batches_failed": int(delta.get("batches_failed") or 0),
        "batch_ms_p50": histogram_percentile(batch_ms, 50),
        "batch_ms_p95": histogram_percentile(batch_ms, 95),
        "execute_ms_p50": histogram_percentile(execute_ms, 50),
        "execute_ms_p95": histogram_percentile(execute_ms, 95),
        "oc000": int(oc000),
        "oc001": int(oc001),
        "oc_unparsed": int(oc_unparsed),
        # Every other retried code (XX000, a class-08 drop, "conn" for a socket that died
        # with no SQLSTATE), summed.
        "retries_other": int(sum(retries.values())),
        "index_retries": int(sum(index_retries.values())),
        # Every 40001 the workers met in the window, data writes and index submissions
        # alike: the figure to hold against CloudWatch's OccConflicts for the same minute.
        "occ_conflicts": int(oc000 + oc001 + oc_unparsed + index_occ),
        "unique_recoveries": int(delta.get("unique_recoveries") or 0),
        "reconnects": int(delta.get("reconnects") or 0),
        "reader_blocked_s": round(float(delta.get("reader_blocked_s") or 0.0), 3),
        "writer_wait_s": round(float(delta.get("writer_wait_s") or 0.0), 3),
        "worker_cpu_s": round(float(cpu), 3) if cpu is not None else None,
        # CPU cores the worker processes used across the window (all of them together).
        "worker_cores": round(float(cpu) / seconds, 2) if cpu is not None and seconds > 0
        else None,
        # How many worker processes ran, on average, and the cores each used while it ran:
        # the client-bound (about 1 core) vs target-bound reading.
        "worker_processes": round(busy / seconds, 2) if busy and seconds > 0 else None,
        "cores_per_worker": round(float(cpu) / busy, 2) if cpu is not None and busy else None,
    }


def _table_window(delta: dict, seconds: float, units: int) -> dict:
    """One table's write-path figures in one window, from a ``diff_load_stats`` delta of
    its merged units.

    The per-worker figures divide by the seconds the table's worker processes ran in the
    window (their ``wall_s``), so a shard that already finished, or has not started, does
    not dilute them. A unit with no process clock (the in-process tp=1 path) counts the
    whole window, once per unit.
    """
    from dsql_migrator.core.load_stats import histogram_percentile

    cpu = delta.get("cpu_s")
    busy = float(delta.get("wall_s") or 0.0) or seconds * max(1, int(units or 1))
    return {
        "workers": round(busy / seconds, 2) if seconds > 0 else None,
        "cores": round(float(cpu) / seconds, 2) if cpu is not None and seconds > 0 else None,
        "cores_per_worker": round(float(cpu) / busy, 2) if cpu is not None and busy else None,
        "reader_blocked_pct": round(
            100.0 * float(delta.get("reader_blocked_s") or 0.0) / busy, 1) if busy else None,
        "writer_wait_pct": round(
            100.0 * float(delta.get("writer_wait_s") or 0.0) / busy, 1) if busy else None,
        "batch_ms_p50": histogram_percentile(delta.get("batch_ms"), 50),
        "retries": int(sum((delta.get("retries") or {}).values())),
    }


def _abbrev(n: "int | None") -> str:
    """Compact human count: 1234567 -> '1.23M', 45000 -> '45.0K'."""
    if n is None:
        return "?"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.2f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _fmt_eta(seconds: "float | None") -> str:
    """A short ETA like '3m12s' / '45s' / '1h04m'; '?' when not estimable."""
    if seconds is None or seconds < 0:
        return "?"
    s = int(seconds)
    if s >= 3600:
        return f"{s // 3600}h{(s % 3600) // 60:02d}m"
    if s >= 60:
        return f"{s // 60}m{s % 60:02d}s"
    return f"{s}s"


# --------------------------------------------------------------------------- #
# CPU / memory sampling from the Linux cgroup (stdlib only, no psutil)
# --------------------------------------------------------------------------- #
# Full Load is CPU-bound (per-row conversion in Python), so a perf run wants to see
# whether the task is CPU-saturated. On Linux (Fargate is Linux) the container's own
# CPU time and memory live in the cgroup pseudo-filesystem -- read them directly, no
# dependency. cgroup v2 (/sys/fs/cgroup/cpu.stat + memory.current) is tried first,
# then v1 (cpuacct.usage + memory.usage_in_bytes). On macOS / no cgroup (local dev)
# these files are absent and the sampler reports "n/a" -- CPU% only means something
# in-container anyway, which is where it matters (the CPU-bound finding was in-VPC).

def _read_int(path: str) -> "int | None":
    try:
        with open(path, encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:  # noqa: BLE001 - absent/unreadable -> unknown
        return None


def _cpu_usage_seconds() -> "float | None":
    """Cumulative CPU seconds used by this cgroup, or None if unavailable."""
    # cgroup v2: cpu.stat has "usage_usec <N>" (microseconds).
    try:
        with open("/sys/fs/cgroup/cpu.stat", encoding="utf-8") as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return int(line.split()[1]) / 1_000_000.0
    except Exception:  # noqa: BLE001
        pass
    # cgroup v1: cpuacct.usage is nanoseconds.
    ns = _read_int("/sys/fs/cgroup/cpuacct/cpuacct.usage")
    if ns is None:
        ns = _read_int("/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage")
    return ns / 1_000_000_000.0 if ns is not None else None


def _mem_bytes() -> "tuple[int | None, int | None]":
    """Return (current_bytes, limit_bytes); either may be None if unavailable."""
    # cgroup v2.
    cur = _read_int("/sys/fs/cgroup/memory.current")
    lim_raw = None
    try:
        with open("/sys/fs/cgroup/memory.max", encoding="utf-8") as f:
            raw = f.read().strip()
            lim_raw = None if raw == "max" else int(raw)
    except Exception:  # noqa: BLE001
        pass
    if cur is not None:
        return cur, lim_raw
    # cgroup v1.
    cur = _read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    lim = _read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    # v1 reports a huge sentinel when unlimited; treat that as "no limit".
    if lim is not None and lim > (1 << 62):
        lim = None
    return cur, lim


def _fmt_gib(n: "int | None") -> str:
    """Bytes -> 'x.yG', or '?'."""
    return f"{n / (1024 ** 3):.2f}G" if n is not None else "?"


class _CpuSampler:
    """Turns cumulative cgroup CPU seconds into a %-of-cores-used between samples.

    100% = one full core. On a 4-vCPU task the ceiling is ~400%. Returns None until
    it has two samples, and None when cgroup CPU accounting is unavailable (macOS).
    """

    def __init__(self) -> None:
        self._prev_cpu = _cpu_usage_seconds()
        self._prev_t = time.monotonic()
        self.available = self._prev_cpu is not None

    def sample(self) -> "float | None":
        if not self.available:
            return None
        cpu = _cpu_usage_seconds()
        now = time.monotonic()
        if cpu is None or now <= self._prev_t:
            return None
        pct = 100.0 * (cpu - self._prev_cpu) / (now - self._prev_t)
        self._prev_cpu, self._prev_t = cpu, now
        return pct


def _monitor_full_load(jm, job_id: str, tables, interval: float, board=None) -> dict:
    """Block until the job finishes, logging one progress window per poll.

    Reads the same thread-safe ``get_status`` snapshot the UI polls (never mutates
    the job), so it is safe and read-only. For each poll it logs, per table that is
    IN_PROGRESS, the rows loaded vs the watermark's approximate target (a %), the
    instantaneous rows/s since the previous poll, and an ETA from the remaining rows at
    that instantaneous rate. The watermark row-counts are scan-free estimates
    (``information_schema``), so % / ETA are approximate by design -- they exist to show
    the run is alive and roughly how far along, not to be exact.

    The polls end on wall-clock multiples of ``interval`` (:func:`_next_boundary`), and
    the last one the moment the job's thread ends (``jm.wait``), so neither a window nor
    the run's wall time is rounded up to a poll. With ``board`` (the run's
    LoadStatsBoard) a window also carries the workers' write-path counters that arrived
    in it -- OC000 / OC001 per window, to hold against CloudWatch's OccConflicts (the
    workers send them every couple of seconds) -- and, under ``tables``, each running
    table's rows/s, worker cores and read-ahead queue waits (:func:`_table_window`).

    Also samples the container's cgroup CPU%/memory each poll (Linux/Fargate only;
    "n/a" on macOS). Returns ``{"peak": {"cpu_pct", "mem_bytes"}, "windows": [...],
    "finished_at": epoch}``.
    """
    from dsql_migrator.core.job_manager import JobNotFoundError
    from dsql_migrator.core.load_stats import diff_load_stats

    interval = max(1.0, float(interval))
    if 60.0 % interval and interval % 60.0:
        log(f"note: --progress-interval {interval:g}s does not divide a minute, so only "
            "some windows end on a minute boundary")
    total_expected = len(tables)
    # Per-table last-seen (rows_loaded, epoch) to derive instantaneous rows/s.
    last: dict[str, tuple[int, float]] = {}
    cpu_sampler = _CpuSampler()  # cgroup CPU% between polls (None on macOS/no cgroup)
    peak = {"cpu_pct": None, "mem_bytes": None}  # returned for the report
    windows: list[dict] = []
    start = window_start = time.time()
    prev_rows = 0
    prev_run: dict = {}
    prev_tables: dict[str, dict] = {}
    while True:
        boundary = _next_boundary(time.time(), interval)
        finished = jm.wait(job_id, timeout=max(0.0, boundary - time.time()))
        if not finished and boundary > time.time():
            time.sleep(boundary - time.time())  # a join can return a hair early
        try:
            job = jm.get_status(job_id)
        except JobNotFoundError:
            return {"peak": peak, "windows": windows, "finished_at": time.time()}
        now = time.time()
        seconds = max(1e-6, now - window_start)
        wm_counts = {}
        wm = getattr(job, "watermark", None)
        if wm is not None:
            wm_counts = getattr(wm, "table_row_counts", {}) or {}

        done = sum(1 for c in job.chunks if c.status == "DONE")
        failed = sum(1 for c in job.chunks if c.status == "FAILED")
        active = [c for c in job.chunks if c.status == "IN_PROGRESS"]
        total_loaded = sum(int(c.rows_loaded or 0) for c in job.chunks)
        overall_rps = total_loaded / (now - start) if now > start else 0.0
        window_rows = max(0, total_loaded - prev_rows)

        # Resource line: CPU% (100% = 1 core; a 4-vCPU task tops out ~400%) + memory.
        # Shows whether Full Load is CPU-saturated -- the bottleneck we tune CPU for.
        cpu_pct = cpu_sampler.sample()
        mem_cur, mem_lim = _mem_bytes()
        if cpu_pct is not None and (
            peak["cpu_pct"] is None or cpu_pct > peak["cpu_pct"]
        ):
            peak["cpu_pct"] = round(cpu_pct, 1)
        if mem_cur is not None and (
            peak["mem_bytes"] is None or mem_cur > peak["mem_bytes"]
        ):
            peak["mem_bytes"] = mem_cur
        res = ""
        if cpu_pct is not None or mem_cur is not None:
            cpu_s = f"cpu {cpu_pct:,.0f}%" if cpu_pct is not None else "cpu n/a"
            mem_s = (
                f"mem {_fmt_gib(mem_cur)}"
                + (f"/{_fmt_gib(mem_lim)}" if mem_lim is not None else "")
            ) if mem_cur is not None else "mem n/a"
            res = f" | {cpu_s} {mem_s}"

        run_stats = board.run() if board is not None else {}
        table_stats = board.tables() if board is not None else {}
        counts = (
            _window_counts(diff_load_stats(run_stats, prev_run), seconds)
            if run_stats else None
        )
        window = {
            "start": _iso(window_start), "end": _iso(now), "seconds": round(seconds, 3),
            "rows": window_rows, "rows_per_sec": round(window_rows / seconds, 1),
            "tables_done": done, "tables_failed": failed, "tables_active": len(active),
            "cpu_pct": round(cpu_pct, 1) if cpu_pct is not None else None,
            "mem_bytes": mem_cur,
        }
        if counts is not None:
            window.update(counts)
        windows.append(window)

        log(
            f"progress [{_utc(window_start)}-{_utc(now)} UTC]: {done}/{total_expected} done"
            f"{(', ' + str(failed) + ' failed') if failed else ''}"
            f" | {_abbrev(total_loaded)} rows | window {window_rows / seconds:,.0f} rows/s, "
            f"~{overall_rps:,.0f} rows/s overall{res}"
        )
        if counts is not None:
            cores = counts["worker_cores"]
            per_worker = counts["cores_per_worker"]
            log(
                f"  writes: {counts['batches']} batches (p50 {_fmt_ms(counts['batch_ms_p50'])},"
                f" p95 {_fmt_ms(counts['batch_ms_p95'])}; INSERT p50 "
                f"{_fmt_ms(counts['execute_ms_p50'])}) | OC000 {counts['oc000']}, OC001 "
                f"{counts['oc001']}, 40001 unparsed {counts['oc_unparsed']}, other retries "
                f"{counts['retries_other']}, index retries {counts['index_retries']} "
                f"(all 40001: {counts['occ_conflicts']})"
                f" | 23505 recovered {counts['unique_recoveries']}"
                f" | reconnects {counts['reconnects']} | reader blocked "
                f"{counts['reader_blocked_s']:.1f}s, writers waiting "
                f"{counts['writer_wait_s']:.1f}s"
                + (f" | workers {cores:.2f} cores" if cores is not None else "")
                + (f" ({counts['worker_processes']:.1f} processes, {per_worker:.2f} each)"
                   if per_worker is not None else "")
            )
        # Per table: every table running at this poll, plus one that ran at the previous
        # poll and has ended since (its last, partial window). Into the window's JSON too.
        per_table: dict[str, dict] = {}
        for c in job.chunks:
            running = c.status == "IN_PROGRESS"
            prev = last.get(c.chunk_id)
            if not running and prev is None:
                continue  # not started yet, or its last window is already reported
            loaded = int(c.rows_loaded or 0)
            target = wm_counts.get(c.chunk_id)
            inst_rps = None
            if prev is not None and now > prev[1]:
                inst_rps = (loaded - prev[0]) / (now - prev[1])
            entry = {
                "status": c.status,
                "rows": loaded - (prev[0] if prev is not None else 0),
                # None for the window it started in (it ran only part of it).
                "rows_per_sec": round(inst_rps, 1) if inst_rps is not None else None,
            }
            mine = table_stats.get(c.chunk_id)
            if mine:
                entry.update(_table_window(
                    diff_load_stats(mine, prev_tables.get(c.chunk_id)), seconds,
                    int(mine.get("units") or 1),
                ))
            per_table[c.chunk_id] = entry
            if not running:
                last.pop(c.chunk_id, None)
                continue
            last[c.chunk_id] = (loaded, now)
            pct = f"{100.0 * loaded / target:.1f}%" if target else "?"
            eta = None
            if target and inst_rps and inst_rps > 0:
                eta = max(0, (target - loaded)) / inst_rps
            line = (
                f"    {c.chunk_id:<32} {_abbrev(loaded)}/{_abbrev(target)} "
                f"({pct})  {(f'{inst_rps:,.0f}' if inst_rps is not None else '?'):>8} "
                f"rows/s  ETA {_fmt_eta(eta)}"
            )
            if mine:
                line += (
                    (f" | {entry['cores']:.2f} cores" if entry["cores"] is not None else "")
                    # A table read in K ranges runs K worker processes.
                    + (f" ({entry['cores_per_worker']:.2f}/worker)"
                       if entry["cores_per_worker"] is not None
                       and int(mine.get("units") or 1) > 1 else "")
                    + f" | reader blocked {entry['reader_blocked_pct'] or 0:.0f}%"
                    f", writers waiting {entry['writer_wait_pct'] or 0:.0f}%"
                    f" | batch p50 {_fmt_ms(entry['batch_ms_p50'])}"
                )
            log(line)
        if per_table:
            window["tables"] = per_table

        prev_rows, prev_run, prev_tables = total_loaded, run_stats, table_stats
        window_start = now
        if finished or job.status not in ("PENDING", "RUNNING"):
            log(f"job {job.status} after {now - start:.1f}s")
            return {"peak": peak, "windows": windows, "finished_at": now}


# Aurora DSQL's asynchronous jobs (CREATE INDEX ASYNC builds, VALIDATE CONSTRAINT, ...) still
# running. An arm that starts while an earlier run's index builds still run shares the
# cluster with them, so --wait-sys-jobs holds the start until none is left.
_SYS_JOBS_RUNNING = ("submitted", "processing")


def _sys_jobs_running(conn) -> dict:
    """``{status: count}`` of the sys.jobs rows still submitted or processing (a read)."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT status FROM sys.jobs")
        rows = cur.fetchall()
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001 - best effort
            pass
    counts: dict = {}
    for row in rows or ():
        status = str((row[0] if row else "") or "").strip().lower()
        if status in _SYS_JOBS_RUNNING:
            counts[status] = counts.get(status, 0) + 1
    return counts


def _wait_for_sys_jobs(connect, max_wait: float, *, poll: float = 15.0,
                       sleep=time.sleep, clock=time.monotonic) -> dict:
    """Wait until Aurora DSQL's sys.jobs has no submitted/processing job, at most
    ``max_wait`` seconds.

    Returns ``{"drained", "waited_seconds", "running", "read_errors"}``: ``running`` is the
    last count read successfully (``{}`` once drained, or when no read ever succeeded). A
    read that fails is retried on a fresh connection at the next poll; reads that keep
    failing leave ``drained`` False once the bound runs out -- unknown is never "drained".
    """
    started = clock()
    conn = None
    running: "dict | None" = None
    last_read: dict = {}
    errors = 0
    try:
        while True:
            try:
                if conn is None:
                    conn = connect()
                running = last_read = _sys_jobs_running(conn)
            except Exception as exc:  # noqa: BLE001 - a failed read is retried, never fatal
                errors += 1
                log(f"[sys.jobs] read failed ({type(exc).__name__}); retrying on a new "
                    "connection")
                try:
                    if conn is not None:
                        conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = None
            waited = clock() - started
            if running == {}:
                return {"drained": True, "waited_seconds": round(waited, 1),
                        "running": {}, "read_errors": errors}
            if waited >= max_wait:
                return {"drained": False, "waited_seconds": round(waited, 1),
                        "running": last_read, "read_errors": errors}
            if running:
                log(f"[sys.jobs] {sum(running.values())} async job(s) still running "
                    f"({', '.join(f'{k} {v}' for k, v in sorted(running.items()))}); "
                    f"waited {waited:.0f}s of {max_wait:.0f}s")
            running = None
            sleep(min(poll, max(0.0, max_wait - waited)))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


# --wait-index-builds: the default bound, and how often the harness reads the builds.
_WAIT_INDEX_BUILDS_SECONDS = 1800.0
_INDEX_BUILD_POLL_SECONDS = 15.0


def _wait_for_index_builds(connect, record, max_wait: float, *,
                           poll: float = _INDEX_BUILD_POLL_SECONDS, sleep=time.sleep,
                           clock=time.monotonic, now=None, read=None):
    """Wait, at most ``max_wait`` seconds (0: not at all, no read), for Aurora DSQL to finish
    the index builds the run submitted (P4b) -- the run itself never waits for them.

    Every ``poll`` seconds one read through the product's own reader (``index_build_states``:
    sys.jobs BY the recorded job ids, pg_index once a job is no longer listed -- never every
    sys.jobs row, which also counts DROP and ANALYZE jobs), folded into a local copy of the
    record exactly as the Data Migration step's watch folds it (``fold_index_build_reads``),
    until nothing is building. Each read opens its own connection, so one that failed is
    retried on a new connection at the next poll. Returns ``(record, waited_seconds,
    failed_reads)``. Read-only.
    """
    from dsql_migrator.core.target_introspector import index_build_states
    from dsql_migrator.ui.data_migration._index_builds import fold_index_build_reads

    if record is None or not record.building or max_wait <= 0:
        return record, 0.0, 0
    reader = read or index_build_states
    wall = now or (lambda: datetime.datetime.now(datetime.timezone.utc))
    started = clock()
    failed_reads = 0
    current = record
    while True:
        read_at = wall()
        reads = reader(list(current.building), connection_factory=connect)
        if not any(value is not None for value in reads.values()):
            failed_reads += 1
            log("[index builds] the read failed; retrying on a new connection")
        current, _failed, _finished = fold_index_build_reads(current, reads, read_at)
        waited = clock() - started
        if not current.building or waited >= max_wait:
            return current, round(waited, 1), failed_reads
        log(f"[index builds] {len(current.building)} of {current.submitted} still building; "
            f"waited {waited:.0f}s of {max_wait:.0f}s")
        sleep(min(poll, max(0.0, max_wait - waited)))


def _index_builds_report(record, *, waited: float, read_errors: int, started_at: float,
                         elapsed: float) -> "tuple[dict | None, float | None]":
    """``(index_builds, end_to_end_seconds)`` for the report: counts and times only. The end
    to end runs from the run's start to the later of its end and the last build's finish
    (as seen by the poll that saw it; ``None`` while any still builds), or is the run's own
    wall time when it submitted no build (nothing to wait for). Builds submitted on the empty
    tables before the rows (P4d) can finish before the data does: their
    ``seconds_after_load`` is 0 (ready when the data loaded) and the run's end bounds the
    end to end."""
    if record is None or not record.submitted:
        return None, round(elapsed, 2)
    loaded, finished = record.data_loaded_at, record.finished_at
    done = finished is not None and not record.building
    return {
        "submitted": record.submitted,
        "completed": record.completed,
        "failed": record.failed,
        "still_building": len(record.building),
        "data_loaded_at": _iso(loaded),
        "submitted_at": _iso(record.submitted_at),
        "finished_at": _iso(finished) if done else None,
        "seconds_after_load": round(max(0.0, (finished - loaded).total_seconds()), 1)
        if done and loaded is not None else None,
        "waited_seconds": waited,
        "read_errors": read_errors,
    }, (round(max(finished.timestamp() - started_at, elapsed), 2) if done else None)


@contextlib.contextmanager
def _start_order(inventory: bool):
    """--inventory-order: while the run is on, the process pool starts its units in the
    planner's own (inventory) order, as builds before R2 did, instead of the largest table
    first -- so one image measures both arms of that A/B. Put back afterwards."""
    if not inventory:
        yield
        return
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    largest_first = engine._largest_first
    engine._largest_first = lambda work_units, estimates: list(work_units)
    try:
        yield
    finally:
        engine._largest_first = largest_first


# --range-in-flight / --in-flight-budget: the loader's measurement seams they set (P3).
_IN_FLIGHT_SEAMS = (
    ("range_in_flight", "DSQL_MIGRATOR_FULL_LOAD_RANGE_IN_FLIGHT"),
    ("in_flight_budget", "DSQL_MIGRATOR_FULL_LOAD_IN_FLIGHT_BUDGET"),
)


def _reader_shards_setting() -> "int | str | None":
    """Reader shards per large table as the loader reads it now (P1): "auto" for 0
    (automatic, the default), else the number -- from the loader's own config after
    --reader-shards set it, not from .env (which the loader does not read). A value the
    config refuses (the run failed on it) is reported as it was set."""
    from dsql_migrator.config import load_config

    try:
        value = int(load_config().full_load_reader_shards)
    except Exception:  # noqa: BLE001 - the report says what was set
        return os.environ.get("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS")
    return "auto" if value == 0 else value


def _index_order() -> str:
    """Where the loader puts a recreated table's secondary indexes, as it reads its seam now:
    "before-load" (P4d, the default: on the empty table before its rows) or "after-load"
    (--indexes-after-load: after every table's data, as before P4d)."""
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    return "after-load" if engine._indexes_after_load() else "before-load"


def _in_flight_caps() -> dict:
    """The INSERT batches the loader keeps in flight at most per key range and per run
    (P3), as it reads them now: the product's 4 and 128 unless --range-in-flight /
    --in-flight-budget set its seams to an integer >= 1."""
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    return {"per_range": engine._range_in_flight(), "run_budget": engine._run_in_flight_budget()}


def cmd_full_load(args) -> int:
    from _e2e_tables import tables_for

    schema = args.schema
    if not args.yes:
        log(f"[plan] would Full Load (DROP+recreate) schema '{schema}' at "
            f"table-parallelism={args.table_parallelism} "
            f"batch-parallelism={args.batch_parallelism} and report throughput + OCC "
            "retry rate. Use --yes.")
        return 0

    # Set the loader's tunables BEFORE it builds the importer (load_config reads
    # os.environ fresh). Higher parallelism -> more concurrent writers on the same
    # hot AUTO_INCREMENT key range -> a higher OCC (40001) collision rate.
    os.environ["DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM"] = str(args.table_parallelism)
    os.environ["DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM"] = str(args.batch_parallelism)
    if args.batch_rows:
        os.environ["DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS"] = str(args.batch_rows)
    # --no-prefetch flips the loader's read-ahead queue OFF (the pre-improvement
    # code path), so one build measures BOTH the prefetch and baseline variants by
    # env alone -- the whole point of the in-VPC A/B on a single deployed image.
    if getattr(args, "no_prefetch", False):
        os.environ["DSQL_MIGRATOR_FULL_LOAD_PREFETCH"] = "0"
    # --reader-shards sets Reader shards per large table (primary-key ranges per large table
    # with an integer leading key) for this run: 0 = automatic (the loader's default, P1:
    # from each table's estimate), 1 = off, K = exactly K. Unset: the environment's, else
    # the default. Same one-image A/B pattern as prefetch. MySQL: automatic and Full load
    # only read the ranges with one reader session from one snapshot (P2); a Full load + CDC
    # run from a binlog start point (--cdc-mode) reads an explicit K >= 2 as K parts, each
    # from its own snapshot.
    if getattr(args, "reader_shards", None) is not None:
        os.environ["DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS"] = str(args.reader_shards)
    # --range-in-flight R / --in-flight-budget B: the batches in flight per key range and
    # per run (P3) -- the loader's measurement seams, so one image measures every arm of
    # the R A/B. Unset: the product's (4 and 128).
    for flag, key in _IN_FLIGHT_SEAMS:
        if getattr(args, flag, None) is not None:
            os.environ[key] = str(getattr(args, flag))
    # --indexes-after-load: the loader's measurement seam that submits a recreated table's
    # secondary indexes after every table's data (P4a's order) instead of on the empty table
    # before its rows (P4d), so one image measures both arms. Unset: the environment's.
    if getattr(args, "indexes_after_load", False):
        os.environ["DSQL_MIGRATOR_FULL_LOAD_INDEXES_AFTER_LOAD"] = "1"

    from dsql_migrator.core.converter import (
        PrimaryKeyStrategy, SchemaConvertOptions, SchemaConverter,
    )
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.load_stats import LoadStatsBoard
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    # These moved out of the (now-removed) _engine module into _full_load_engine and are
    # re-exported from the data_migration package -- import from the package so this keeps
    # working across the refactor.
    from dsql_migrator.ui.data_migration import (
        DataMigrationInputs, default_migrator_factory, run_full_load,
    )
    from dsql_migrator.ui.evaluation import _default_introspector_factory

    source, target, password = _configs(schema)
    inventory = _default_introspector_factory(password).introspect(source)
    present = {t.name: t for t in inventory.tables}
    # --tables lets a run measure a SUBSET of the schema's tables (e.g. just the
    # big ones) for a fast, repeatable before/after comparison; default = the
    # schema's full registered set.
    only = set(getattr(args, "tables", None) or [])
    wanted: list[str] = []
    for t in tables_for(schema):
        if only and t not in only:
            continue
        q = f"{schema}.{t}"
        if q in present:
            wanted.append(q)
        elif t in present:
            present[t].name = q
            wanted.append(q)
    if only:
        missing = only - {w.split(".", 1)[1] for w in wanted}
        if missing:
            raise SystemExit(f"--tables not found in {schema}: {sorted(missing)}")
    tables = TableSelector().resolve(inventory, TableSelection(selected_tables=wanted))

    # Surface the retry-loop diagnostics (per-attempt DEBUG + give-up WARNING) to
    # stdout/CloudWatch via a dedicated handler on the occ logger: which error, its
    # SQLSTATE, attempt count, and total elapsed per failed batch -- direct evidence
    # of WHY a batch failed (budget too small vs a storm longer than the budget vs a
    # non-transient error), instead of inferring from timing. It sees only the work
    # that runs in THIS process (the replace pre-pass, the run's index pass since P4a);
    # the table workers' retries are in the write-path counters below.
    occ_logger = logging.getLogger("dsql_migrator.core.occ")
    occ_logger.setLevel(logging.DEBUG)
    _occ_handler = logging.StreamHandler(sys.stdout)
    _occ_handler.setFormatter(logging.Formatter("[occ] %(levelname)s %(message)s"))
    occ_logger.addHandler(_occ_handler)
    occ_logger.propagate = False

    # --composite-leading COL: measure the COMPOSITE-KEY variant. Convert each
    # wanted table with the COMPOSITE_KEY strategy (leading column COL prepended to
    # the PK) and pass the results as per-table table_conversions -- the SAME field
    # the UI's Schema Conversion produces. This drives BOTH the DROP+recreate DDL
    # (target gets the (COL, id) key + the UNIQUE INDEX on id) and Full Load's
    # target-PK ON CONFLICT (Phase 0), so the A/B exercises the real production
    # path, not a special measurement path. A table lacking COL (or where COL is
    # not a valid leading column) is skipped from the composite set with a log line
    # and loaded with its unchanged key, so a mixed schema still runs.
    leading = getattr(args, "composite_leading", "") or ""
    table_conversions: dict = {}
    if leading:
        # Use the SOURCE dialect's converter (PostgreSQL vs MySQL); a MySQL converter would
        # mis-handle PG source types in the composite-key DDL.
        converter = SchemaConverter(source_type=source.source_type)
        for tdef in tables:
            conv = converter.convert_table(
                tdef,
                SchemaConvertOptions(
                    primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY,
                    composite_leading_column=leading,
                ),
            )
            # An invalid leading column yields an UNSUPPORTED (comment-placeholder)
            # conversion -- detect it by the absence of a real CREATE TABLE and skip.
            if conv.target_ddl.lstrip().upper().startswith("CREATE TABLE"):
                table_conversions[tdef.name] = conv
            else:
                log(f"[composite] skip {tdef.name}: '{leading}' is not a valid "
                    f"leading column ({conv.warnings[0].message if conv.warnings else 'n/a'})")
        log(f"[composite] leading='{leading}' applied to "
            f"{len(table_conversions)}/{len(tables)} tables")

    # --wait-sys-jobs: hold the start until Aurora DSQL's async jobs (an earlier run's
    # index builds) have drained, at most the given seconds -- an arm that starts on a
    # busy cluster measures that cluster, not this load. Read-only.
    gate = None
    if getattr(args, "wait_sys_jobs", None) is not None:
        log(f"[sys.jobs] waiting for no submitted/processing job "
            f"(at most {args.wait_sys_jobs:.0f}s) ...")
        gate = _wait_for_sys_jobs(_target_conn, float(args.wait_sys_jobs))
        gate["max_wait_seconds"] = float(args.wait_sys_jobs)
        if not gate["drained"]:
            if gate["running"]:
                log(f"[sys.jobs] still {sum(gate['running'].values())} job(s) running after "
                    f"{gate['waited_seconds']:.0f}s ({gate['read_errors']} failed read(s)); "
                    "not starting. Wait longer (--wait-sys-jobs SECONDS) or check sys.jobs.")
            else:
                log(f"[sys.jobs] could not be read in {gate['waited_seconds']:.0f}s "
                    f"({gate['read_errors']} failed read(s)); not starting. Check the "
                    "target connection, then run again.")
            return 3
        log(f"[sys.jobs] drained after {gate['waited_seconds']:.0f}s")

    # --cdc-mode: a Full load + CDC run (is_cdc_migration) -- the start point is captured
    # and, on MySQL, fenced (G147) exactly as the product does before any read, which is
    # what lets a MySQL table's --reader-shards ranges be read as parts from separate
    # snapshots (G137). No CDC stack is involved:
    # the load itself needs none, and no PostgreSQL slot is created (no cdc_stack_name).
    cdc_mode = bool(getattr(args, "cdc_mode", False))
    inputs = DataMigrationInputs(
        source_config=source, source_password=password, target_config=target,
        inventory=inventory, aws_profile=os.environ.get("AWS_PROFILE"),
        replace_tables=frozenset(wanted),  # clean slate: plain INSERT, real OCC surfaces
        table_conversions=table_conversions,
        is_cdc_migration=cdc_mode,
    )
    migrator = default_migrator_factory(inputs)
    error_log = ErrorLogStore()
    jm = JobManager(stall_timeout_seconds=None)
    # The table workers' write-path counters, live (beside each progress flush) and final
    # (with each result), per table and per run.
    board = LoadStatsBoard()
    # --inventory-order: the other arm of the start-order A/B (roadmap A7), on one image.
    inventory_order = bool(getattr(args, "inventory_order", False))

    def work(handle) -> None:
        with _start_order(inventory_order):
            run_full_load(
                handle, tables, migrator=migrator, error_log=error_log, load_stats=board
            )

    caps = _in_flight_caps()
    log(f"Full Load '{schema}' ({len(tables)} tables) at "
        f"tp={args.table_parallelism} bp={args.batch_parallelism} (in flight <= "
        f"{caps['per_range']} per key range, {caps['run_budget']} per run)"
        f"{' as Full load + CDC' if cdc_mode else ''}"
        f"{' in inventory order' if inventory_order else ''}"
        f"{' with the secondary indexes after the load' if _index_order() == 'after-load' else ''}"
        " ...")
    started_at = time.time()
    t0 = time.monotonic()
    job_id = jm.submit(work)
    # Instead of blocking silently on jm.wait(), poll the live job snapshot on a
    # cadence and log per-table progress (rows/target %, instantaneous + cumulative
    # rows/s, ETA). chunk.rows_loaded advances live (the engine flushes every
    # PROGRESS_FLUSH_ROWS as batches land), so this turns a long, opaque load into a
    # monitorable one -- the whole point of a perf run. Read-only: it only reads the
    # same get_status() snapshot the UI polls; it never mutates the job.
    monitor = _monitor_full_load(
        jm, job_id, tables, interval=args.progress_interval, board=board
    )
    # Exact: the monitor returns the moment the job's thread ends, not at the next poll.
    elapsed = time.monotonic() - t0

    job = jm.get_status(job_id)
    # --wait-index-builds: the run does not wait for the CREATE INDEX ASYNC builds it
    # submitted (P4b), so the harness does -- bounded, by their job ids -- and reports when
    # the indexes were ready and the end to end; an A/B must compare those, not the run.
    wait_builds = float(getattr(args, "wait_index_builds", _WAIT_INDEX_BUILDS_SECONDS) or 0.0)
    builds, builds_waited, builds_read_errors = _wait_for_index_builds(
        _target_conn, getattr(job, "index_builds", None), wait_builds
    )
    report = _full_load_report(
        args, job, board, elapsed=elapsed, started_at=started_at,
        finished_at=monitor["finished_at"], monitor=monitor, leading=leading,
        composite_tables=len(table_conversions), gate=gate, cdc_mode=cdc_mode,
        index_builds=builds, index_waited=builds_waited,
        index_read_errors=builds_read_errors,
    )

    log(f"status={job.status} wall={elapsed:.1f}s rows={report['total_rows']} "
        f"overall={report['overall_rows_per_sec']} rows/s")
    # Surface WHY the job failed. A failure that propagates out of run_full_load
    # (e.g. a pre-pass DROP+recreate error, before any table worker runs) is stored
    # only as the JobManager's captured exception text -- without this line the run
    # just prints "status=FAILED" with every table PENDING and no reason, which is
    # undiagnosable from the logs alone.
    if job.status == "FAILED":
        job_error = jm.get_error(job_id)
        report["error"] = job_error
        log(f"FAILURE REASON: {job_error or '(no error message captured)'}")
        # Also dump the per-table/-shard/-batch error records. A sharded table marks
        # itself FAILED when ANY shard fails, but the shard's actual reason is written
        # only to the error log (which the UI surfaces) -- without this the perf run
        # printed "one or more shards failed" with no cause. Print them so a failed
        # run is diagnosable from its logs alone (mirrors the FAILURE REASON line).
        data_errors = error_log.records(job_id)
        if data_errors:
            report["data_errors"] = [
                {"table": e.table, "chunk_id": e.chunk_id,
                 "error_code": e.error_code, "message": e.message}
                for e in data_errors
            ]
            log(f"DATA ERRORS ({len(data_errors)}):")
            for e in data_errors[:20]:
                log(f"  [{e.table}/{e.chunk_id}] {e.error_code or ''} {e.message}")
    _log_full_load_summary(report)
    _maybe_write_report(args, report)
    return 0 if job.status == "DONE" else 1


def _full_load_report(args, job, board, *, elapsed: float, started_at: float,
                      finished_at: float, monitor: dict, leading: str = "",
                      composite_tables: int = 0, gate=None, cdc_mode: bool = False,
                      index_builds=None, index_waited: float = 0.0,
                      index_read_errors: int = 0) -> dict:
    """The JSON report of one full-load run: the job's tables plus the table workers' own
    counters (``board``). Counts and timings only. ``index_builds``: the job's record of
    its index builds as --wait-index-builds left it (default: the job's own, unwaited)."""
    from dsql_migrator.core.load_stats import summarize_load_stats

    builds_report, end_to_end = _index_builds_report(
        index_builds if index_builds is not None else getattr(job, "index_builds", None),
        waited=index_waited, read_errors=index_read_errors, started_at=started_at,
        elapsed=elapsed,
    )

    total_rows = sum(int(c.rows_loaded or 0) for c in job.chunks)
    table_stats = board.tables() if board is not None else {}
    per_table = []
    for c in job.chunks:
        secs = None
        if c.started_at and c.finished_at:
            secs = (c.finished_at - c.started_at).total_seconds()
        rps = (c.rows_loaded / secs) if (secs and secs > 0) else None
        stats = summarize_load_stats(table_stats.get(c.chunk_id))
        window = stats.get("load_window_s") if stats else None
        per_table.append({
            "table": c.chunk_id, "status": c.status,
            "rows_loaded": c.rows_loaded, "seconds": round(secs, 2) if secs else None,
            "rows_per_sec": round(rps, 1) if rps else None,
            # How many parts read from separate snapshots the table was read in, the G137
            # record (None: one snapshot -- a table one reader session read in key ranges
            # shows them in "ranges" below).
            "reader_shards": getattr(c, "reader_shards", None),
            # P3, from the workers' counters (None: no counters): the key ranges its units
            # read and the INSERT batches they could keep in flight at most, summed over
            # its units, and (P1) how many units read it.
            "ranges": stats.get("ranges") if stats else None,
            "units": stats.get("units") if stats else None,
            "in_flight_cap": stats.get("in_flight_cap") if stats else None,
            # Rows over the table's own data pass only (no index submission, no COUNT).
            "load_rows_per_sec": round(c.rows_loaded / window, 1) if window else None,
            "load_stats": stats or None,
        })

    # DATA-PHASE throughput = total_rows / (last chunk finish - first chunk start).
    # This excludes wall-time spent OUTSIDE the actual row loading -- job submit,
    # DROP+recreate DDL, and the post-data tail where one table finished before the
    # other so parallelism dropped. The adversarial perf review found that tail
    # (~18.6s in the local prefetch run) dragged overall_rows_per_sec below the true
    # loading rate and made a killed-at-95%% baseline (a mid-run cumulative) look
    # falsely faster. data_phase_rows_per_sec is the apples-to-apples metric to
    # compare variants on; overall_rows_per_sec is kept for continuity. A chunk starts
    # when its worker begins and finishes when the parent records its result, so this
    # span includes each table's post-load COUNT; ``load_window`` below is the workers'
    # data passes alone. Since P4a it no longer includes the index submissions: the run
    # submits every table's after the last chunk finished (in wall_seconds, not here), so
    # compare it across that change with care.
    starts = [c.started_at for c in job.chunks if c.started_at]
    finishes = [c.finished_at for c in job.chunks if c.finished_at]
    data_phase_secs = None
    if starts and finishes:
        span = (max(finishes) - min(starts)).total_seconds()
        data_phase_secs = span if span > 0 else None
    data_phase_rps = (
        round(total_rows / data_phase_secs, 1) if data_phase_secs else None
    )

    run = summarize_load_stats(board.run()) if board is not None else {}
    window_secs = run.get("load_window_s")
    statements = run.get("statements") or 0
    wm = getattr(job, "watermark", None)
    peak = monitor.get("peak") or {}
    return {
        "subcommand": "full-load", "schema": args.schema,
        "prefetch": not getattr(args, "no_prefetch", False),
        # Composite-PK A/B: which variant this run measured. composite_leading is
        # the prepended leading column (None = KEEP-integer baseline); composite is
        # a convenience boolean, and composite_tables how many tables got the key.
        "composite": bool(leading and composite_tables),
        "composite_leading": leading or None,
        "composite_tables": composite_tables,
        # Short label consumed by perf_compare.py's comparison table.
        "variant": {
            "label": f"composite:{leading}" if (leading and composite_tables)
            else "keep-integer",
        },
        "table_parallelism": args.table_parallelism,
        "batch_parallelism": args.batch_parallelism,
        "batch_rows": int(cfg("DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS", "2000")),
        # P1: "auto" (0, the loader's default) or the number the loader read.
        "reader_shards": _reader_shards_setting(),
        # P3: the INSERT batches in flight the loader allowed per key range and per run
        # (its effective values; batch_parallelism above is the per-unit ceiling).
        "in_flight": _in_flight_caps(),
        # P4d: where a recreated table's secondary indexes went: "before-load" (on the
        # empty table, its builds waited for before the first row) or "after-load"
        # (--indexes-after-load: after every table's data).
        "index_order": _index_order(),
        # The order the pool started the tables in: the largest first (R2), or the
        # inventory's with --inventory-order. Each table's start is its load_started_at.
        "start_order": (
            "inventory" if getattr(args, "inventory_order", False) else "largest-first"
        ),
        # --cdc-mode: a Full load + CDC run. Whether its start point had a binlog
        # coordinate / an LSN (MySQL splits a table only with a binlog one) and which
        # fence made it exact -- never the coordinate itself.
        "cdc_mode": cdc_mode,
        "start_point": {
            "binlog": bool(getattr(wm, "binlog_file", None)),
            "gtid": bool(getattr(wm, "gtid_executed", None)),
            "wal_lsn": bool(getattr(wm, "wal_lsn", None)),
            "fence": getattr(wm, "start_point_fence", None),
        } if wm is not None else None,
        "sys_jobs_gate": gate,
        "status": job.status,
        "started_at": _iso(started_at), "finished_at": _iso(finished_at),
        "wall_seconds": round(elapsed, 2),
        "total_rows": total_rows,
        "overall_rows_per_sec": round(total_rows / elapsed, 1) if elapsed > 0 else None,
        "data_phase_started_at": _iso(min(starts)) if starts else None,
        "data_phase_finished_at": _iso(max(finishes)) if finishes else None,
        "data_phase_seconds": round(data_phase_secs, 2) if data_phase_secs else None,
        "data_phase_rows_per_sec": data_phase_rps,
        # The workers' data passes alone: the first batch of any table to the last of any.
        "load_window": {
            "started_at": run.get("load_started_at"),
            "finished_at": run.get("load_finished_at"),
            "seconds": window_secs,
            "rows_per_sec": round(total_rows / window_secs, 1) if window_secs else None,
        },
        # Seconds summed over the tables (the loads overlap in wall time): writing rows,
        # submitting the CREATE INDEX ASYNC statements (one at a time, never beside a load:
        # since P4d before the first row, the run then waiting for the builds -- the time
        # between started_at and data_phase_started_at -- else after every table's data),
        # and the post-load COUNT(*) of each replace table.
        "phases_seconds": {
            "load": run.get("load_s"), "index": run.get("index_s"),
            "count": run.get("count_s"),
        },
        # P4b: Aurora DSQL's builds of the indexes the run submitted, as --wait-index-builds
        # left them (counts and times only; None: the run submitted none), and the time from
        # the run's start until the last one finished -- None while one still builds, the
        # run's wall time when there was none to wait for.
        "index_builds": builds_report,
        "end_to_end_seconds": end_to_end,
        "peak_cpu_pct": peak.get("cpu_pct"),
        "peak_mem_bytes": peak.get("mem_bytes"),
        # The workers' own retry counts (before R0 this read the parent's logger, which no
        # worker writes to, so it was always 0). A "batch" here is a statement: one per
        # batch unless a permanently failing row split it.
        "occ": {
            "source": "workers",
            "batches": run.get("batches", 0),
            "statements": statements,
            "attempts": run.get("attempts", 0),
            "total_retries": run.get("retries_total", 0),
            "retries_by_sqlstate": run.get("retries", {}),
            "oc000": run.get("oc000", 0),
            "oc001": run.get("oc001", 0),
            "oc_unparsed": run.get("oc_unparsed", 0),
            "retries_exhausted": run.get("retries_exhausted", 0),
            "index_retries_by_sqlstate": run.get("index_retries", {}),
            "batches_with_retry": run.get("statements_retried", 0),
            "avg_retries_per_batch": round(
                (run.get("attempts", 0) - statements) / statements, 4
            ) if statements else 0.0,
            "pct_batches_with_retry": run.get("pct_statements_retried", 0.0),
        },
        # One INSERT execute (the driver's adaptation of the values + the round trip + the
        # commit), every attempt. Since R1 the statement text is rendered once per shape, not
        # per execute, so these times run lower than an R0 build's for the same server
        # latency. Tells write-bound vs client-bound: high & tight = DSQL write RTT is the
        # wall; low = the client isn't keeping enough batches in flight.
        "write_rtt_ms": run.get("execute_ms") or {
            "samples": 0, "p50": None, "p95": None, "max": None, "mean": None,
        },
        # A batch from its first attempt to its outcome (retries and backoff included).
        "batch_ms": run.get("batch_ms"),
        "load_stats": run or None,
        "windows": monitor.get("windows") or [],
        "per_table": per_table,
    }


def _log_full_load_summary(report: dict) -> None:
    """The end-of-run lines: the write path as the workers counted it, then per table."""
    occ = report["occ"]
    run = report.get("load_stats") or {}
    w = report["write_rtt_ms"]
    b = report.get("batch_ms") or {}
    win = report["load_window"]
    log(f"load window: {win['started_at']} -> {win['finished_at']} ({win['seconds']}s, "
        f"{win['rows_per_sec']} rows/s) | data phase {report['data_phase_seconds']}s "
        f"({report['data_phase_rows_per_sec']} rows/s)")
    ph = report["phases_seconds"]
    log(f"phases (summed over tables): load {ph['load']}s, index submission {ph['index']}s, "
        f"post-load COUNT {ph['count']}s")
    ib = report.get("index_builds")
    if ib:
        log(f"index builds: {ib['submitted']} submitted, {ib['completed']} built, "
            f"{ib['failed']} failed, {ib['still_building']} still building"
            + (f" | ready {ib['finished_at']}, {ib['seconds_after_load']}s after the data "
               "loaded" if ib["finished_at"] else "")
            + f" | end to end {report.get('end_to_end_seconds')}s | waited "
            f"{ib['waited_seconds']}s ({ib['read_errors']} failed read(s))")
    if report.get("cdc_mode"):
        sp = report.get("start_point") or {}
        split = {r["table"]: r["reader_shards"] for r in report["per_table"]
                 if r.get("reader_shards")}
        # A MySQL table is read in parts from separate snapshots only from a binlog start
        # point (G137); a table one reader session read in ranges is not listed here.
        log(f"Full load + CDC: start point binlog={sp.get('binlog')} gtid={sp.get('gtid')} "
            f"lsn={sp.get('wal_lsn')} fence={sp.get('fence')} | read in ranges: "
            f"{split or 'none'}")
    log(f"batches: {occ['batches']} (p50 {b.get('p50')}ms p95 {b.get('p95')}ms max "
        f"{b.get('max')}ms) | INSERT execute p50={w['p50']}ms p95={w['p95']}ms "
        f"max={w['max']}ms (n={w['samples']})")
    log(f"OCC: {occ['statements']} statements, {occ['attempts']} attempts, "
        f"{occ['total_retries']} retries {occ['retries_by_sqlstate']} -- OC000 "
        f"{occ['oc000']}, OC001 {occ['oc001']}, 40001 unparsed {occ['oc_unparsed']}; "
        f"{occ['pct_batches_with_retry']}% retried, {occ['retries_exhausted']} exhausted "
        f"at tp={report['table_parallelism']} bp={report['batch_parallelism']}")
    ifl = report.get("in_flight") or {}
    if ifl:
        log(f"in flight: at most {ifl.get('per_range')} per key range, "
            f"{report['batch_parallelism']} per table or part, {ifl.get('run_budget')} "
            "per run")
    if run:
        cores = run.get("worker_cores")
        log(f"connections: {run.get('connects')} opened, {run.get('reconnects')} reconnects, "
            f"{run.get('connections_kept')} kept after an error, "
            f"{run.get('connect_failures')} failed | 23505 recovered "
            f"{run.get('unique_recoveries')} | queue: reader blocked "
            f"{run.get('reader_blocked_s')}s, writers waiting {run.get('writer_wait_s')}s"
            + (f" | workers {run.get('cpu_s')} CPU-s over {run.get('wall_s')} unit-s "
               f"= {cores} cores each" if cores is not None else "")
            + (f", peak RSS {_fmt_gib(run.get('peak_rss_bytes'))}"
               if run.get("peak_rss_bytes") else ""))
    for r in report["per_table"]:
        s = r.get("load_stats") or {}
        extra = ""
        if s:
            extra = (f" | load {s.get('load_s')}s index {s.get('index_s')}s count "
                     f"{s.get('count_s')}s | batch p50 {(s.get('batch_ms') or {}).get('p50')}ms"
                     f" | retries {s.get('retries_total')}"
                     + (f" | in flight <= {s.get('in_flight_cap')} over {s.get('ranges')} "
                        "range(s)" if s.get("in_flight_cap") else "")
                     + (f" | {s.get('worker_cores')} cores" if s.get("worker_cores") is not None
                        else ""))
        log(f"  {r['table']:<28} {str(r['status']):<8} rows={r['rows_loaded']:<8} "
            f"{r['seconds']}s  {r['rows_per_sec']} rows/s{extra}")


# =========================================================================== #
# cdc-lag: end-to-end replication latency under sustained + burst load
# =========================================================================== #
_PERF_SCHEMA = "cdc_perf"
_PERF_TABLE = "pulse"


def _source_conn():
    import pymysql
    return pymysql.connect(
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "3306")),
        user=cfg("DB_USER", "admin"), password=cfg("DB_PASSWORD"),
        connect_timeout=15, read_timeout=60, charset="utf8mb4", autocommit=True,
    )


def _target_conn():
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.core.target_connection import DsqlConnector
    cfgt = TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"), region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    return DsqlConnector(cfgt, aws_profile=os.environ.get("AWS_PROFILE")).connect()


def _setup_pulse() -> None:
    """Create the dedicated cdc_perf.pulse table on the source (idempotent)."""
    conn = _source_conn()
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {_PERF_SCHEMA} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {_PERF_SCHEMA}.{_PERF_TABLE} (
          seq     BIGINT UNSIGNED NOT NULL,
          src_ts  DATETIME(6) NOT NULL,
          PRIMARY KEY (seq)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
    """)
    conn.close()
    log(f"Created {_PERF_SCHEMA}.{_PERF_TABLE} on the source (idempotent).")
    log(f"  IMPORTANT: CDC must include '{_PERF_SCHEMA}.{_PERF_TABLE}' in its "
        "TableIncludeList/SinkTopics for lag to be measurable. Add it before/at "
        "CDC start, then re-run without --setup.")


def _target_max_seq(tgt) -> "int | None":
    """Newest replicated seq on the target (None if the table/row is absent)."""
    cur = tgt.cursor()
    try:
        # Resolve the schema (qualified preferred, else public) like the other scripts.
        cur.execute(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name=%s AND table_schema IN (%s,'public') "
            "ORDER BY (table_schema=%s) DESC", (_PERF_TABLE, _PERF_SCHEMA, _PERF_SCHEMA))
        row = cur.fetchone()
        if not row:
            return None
        sch = row[0]
        cur.execute(f'SELECT MAX(seq) FROM "{sch}"."{_PERF_TABLE}"')
        v = cur.fetchone()[0]
        return int(v) if v is not None else None
    except Exception:  # noqa: BLE001
        try:
            tgt.rollback()
        except Exception:
            pass
        return None


def cmd_cdc_lag(args) -> int:
    import datetime as _dt

    if args.setup:
        _setup_pulse()
        return 0
    if not cfg("TARGET_ENDPOINT"):
        log("ERROR: TARGET_ENDPOINT not set.")
        return 2

    # seq -> local monotonic send time; the ONE clock the lag is measured on.
    send_time: dict[int, float] = {}
    lock = threading.Lock()
    stop = threading.Event()
    seq_counter = [0]

    def writer() -> None:
        """Insert pulse rows per the chosen profile until the duration elapses."""
        conn = _source_conn()
        cur = conn.cursor()
        end = time.monotonic() + args.duration
        next_burst = time.monotonic()
        try:
            while not stop.is_set() and time.monotonic() < end:
                if args.profile == "burst":
                    if time.monotonic() >= next_burst:
                        for _ in range(args.burst_size):
                            _emit_pulse(cur, send_time, lock, seq_counter)
                        log(f"  burst of {args.burst_size} rows sent "
                            f"(seq up to {seq_counter[0]})")
                        next_burst = time.monotonic() + args.burst_every
                    time.sleep(0.05)
                else:  # sustained
                    _emit_pulse(cur, send_time, lock, seq_counter)
                    time.sleep(args.interval)
        finally:
            conn.close()

    def _emit_pulse(cur, send_time, lock, seq_counter) -> None:
        seq_counter[0] += 1
        seq = seq_counter[0]
        cur.execute(
            f"INSERT INTO {_PERF_SCHEMA}.{_PERF_TABLE} (seq, src_ts) VALUES (%s, %s)",
            (seq, _dt.datetime.now()))
        with lock:
            send_time[seq] = time.monotonic()

    def sampler() -> list:
        """Poll the target's newest seq; record lag = now - local send time of it."""
        tgt = _target_conn()
        samples: list[dict] = []
        seen_seq = 0
        end = time.monotonic() + args.duration + args.drain
        try:
            while time.monotonic() < end and (not stop.is_set() or send_time):
                mx = _target_max_seq(tgt)
                if mx is not None and mx > seen_seq:
                    with lock:
                        sent = send_time.get(mx)
                    if sent is not None:
                        lag = time.monotonic() - sent
                        samples.append({"seq": mx, "lag_s": lag})
                    seen_seq = mx
                # Stop once we've drained past the last written seq.
                if stop.is_set() and seen_seq >= seq_counter[0] and seq_counter[0] > 0:
                    break
                time.sleep(args.sample_interval)
        finally:
            try:
                tgt.close()
            except Exception:
                pass
        return samples

    log(f"CDC lag — profile={args.profile} duration={args.duration}s "
        f"{'interval='+str(args.interval)+'s' if args.profile=='sustained' else ''}"
        f"{'burst='+str(args.burst_size)+'/'+str(args.burst_every)+'s' if args.profile=='burst' else ''} "
        f"drain={args.drain}s")
    log(f"  (requires CDC streaming {_PERF_SCHEMA}.{_PERF_TABLE}; run --setup first "
        "and include it in CDC.)")

    samples_box: list = []
    st = threading.Thread(target=lambda: samples_box.extend(sampler()), daemon=True)
    wt = threading.Thread(target=writer, daemon=True)
    st.start()
    wt.start()
    wt.join()
    stop.set()
    st.join(timeout=args.drain + 30)

    lags = [s["lag_s"] for s in samples_box]
    sent_total = seq_counter[0]
    landed = len(lags)
    report = {
        "subcommand": "cdc-lag", "profile": args.profile,
        "rows_sent": sent_total, "rows_sampled_on_target": landed,
        "lag_seconds": {
            "p50": round(_pct(lags, 50), 3),
            "p95": round(_pct(lags, 95), 3),
            "max": round(max(lags), 3) if lags else None,
            "mean": round(statistics.fmean(lags), 3) if lags else None,
        } if lags else None,
    }
    if not lags:
        log("No target rows observed — is CDC running and including "
            f"{_PERF_SCHEMA}.{_PERF_TABLE}? (See --setup.)")
    else:
        lg = report["lag_seconds"]
        log(f"Sent {sent_total} rows; sampled {landed} target arrivals.")
        log(f"end-to-end lag: p50={lg['p50']}s p95={lg['p95']}s max={lg['max']}s "
            f"mean={lg['mean']}s")
    _maybe_write_report(args, report)
    return 0 if lags else 1


# =========================================================================== #
# Shared
# =========================================================================== #
def _maybe_write_report(args, report: dict) -> None:
    path = getattr(args, "report", "")
    if not path:
        return
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    log(f"Report written -> {path}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    fl = sub.add_parser("full-load", help="Full Load throughput + OCC retry rate")
    fl.add_argument("--schema", default=os.environ.get("CDC_WORKLOAD_SCHEMA",
                                                       "customers_sample_new"))
    fl.add_argument("--yes", action="store_true",
                    help="actually run (DROP+recreate target tables)")
    fl.add_argument("--tables", nargs="*", default=None,
                    help="measure only these tables (subset of the schema); "
                         "default = the schema's full registered set")
    fl.add_argument("--table-parallelism", type=int, default=4)
    fl.add_argument("--batch-parallelism", type=int, default=8)
    fl.add_argument("--batch-rows", type=int, default=0,
                    help="rows per write batch (0 = tool default 2000, max 3000)")
    fl.add_argument("--progress-interval", type=float, default=15.0,
                    help="seconds per live progress window (default 15; min 1). Windows "
                         "end on wall-clock multiples of it (UTC), so 15/20/30/60 line "
                         "up with CloudWatch's minutes")
    fl.add_argument("--no-prefetch", action="store_true",
                    help="disable the read-ahead prefetch queue (measure the "
                         "pre-improvement baseline path); default keeps it ON")
    fl.add_argument("--reader-shards", type=int, default=None, metavar="N",
                    help="Reader shards per large table for this run: 0 = automatic (the "
                         "loader's default: from each table's row estimate), 1 = off (one "
                         "range), 2-32 = exactly N primary-key ranges per large table with "
                         "an integer leading key; unset: the environment's, else automatic. "
                         "MySQL: automatic and Full load only read the ranges with one "
                         "reader session from one snapshot; --cdc-mode with an explicit "
                         "value >= 2 reads them as parts from separate snapshots")
    fl.add_argument("--range-in-flight", type=int, default=None, metavar="N",
                    help="INSERT batches in flight at most per key range of a table "
                         "(default: the loader's 4; still at most --batch-parallelism "
                         "per table or part): the R arm of the in-flight A/B")
    fl.add_argument("--in-flight-budget", type=int, default=None, metavar="N",
                    help="INSERT batches in flight at most per run, divided among the "
                         "worker processes (default: the loader's 128)")
    fl.add_argument("--cdc-mode", action="store_true",
                    help="run the load as a Full load + CDC run: the start point "
                         "(binlog position / LSN) is captured -- and on MySQL fenced "
                         "with FLUSH BINARY LOGS when GTID is off, as the product does "
                         "-- before any read, so a MySQL table's --reader-shards ranges "
                         "can be read as parts from separate snapshots. No CDC stack is "
                         "needed or started, "
                         "and no PostgreSQL replication slot is created")
    fl.add_argument("--inventory-order", action="store_true",
                    help="start the tables in inventory order, as builds before R2 "
                         "did, instead of the largest first: one image runs both arms "
                         "of the start-order A/B")
    fl.add_argument("--wait-sys-jobs", type=float, nargs="?", const=1800.0, default=None,
                    metavar="SECONDS",
                    help="before starting, wait until Aurora DSQL's sys.jobs has no "
                         "submitted/processing job (e.g. an earlier run's index "
                         "builds), at most SECONDS (default 1800); exits 3 without "
                         "loading if it does not drain. It runs before this run drops "
                         "and recreates its tables, so it cannot wait out that drop")
    fl.add_argument("--indexes-after-load", action="store_true",
                    help="submit each recreated table's secondary indexes after every "
                         "table's data has loaded (builds before P4d) instead of on the "
                         "empty table before its rows (the loader's default, which waits "
                         "for those builds, bounded, before the first row): the other arm "
                         "of the index-order A/B")
    fl.add_argument("--wait-index-builds", type=float, default=_WAIT_INDEX_BUILDS_SECONDS,
                    metavar="SECONDS",
                    help="after the run, wait until Aurora DSQL finished the CREATE INDEX "
                         "ASYNC builds the run submitted (read every 15 s by their job "
                         "ids, never every sys.jobs row), at most SECONDS (default "
                         "1800; 0 = do not wait), and report when the indexes were "
                         "ready and the end-to-end time. The run itself never waits for "
                         "them")
    fl.add_argument("--composite-leading", default="",
                    help="measure the COMPOSITE-KEY variant: prepend this "
                         "high-cardinality column to each table's PK "
                         "(e.g. customer_id). Empty = KEEP-integer baseline. Drives "
                         "the real Schema Conversion + Full Load composite path.")
    fl.add_argument("--report", default="", help="write a JSON report here")
    fl.set_defaults(func=cmd_full_load)

    cl = sub.add_parser("cdc-lag", help="end-to-end CDC replication lag (p50/p95/max)")
    cl.add_argument("--setup", action="store_true",
                    help="create cdc_perf.pulse on the source and exit")
    cl.add_argument("--profile", choices=("sustained", "burst"), default="sustained")
    cl.add_argument("--duration", type=float, default=120.0,
                    help="seconds to generate load (default 120)")
    cl.add_argument("--interval", type=float, default=0.2,
                    help="sustained: seconds between single inserts (default 0.2)")
    cl.add_argument("--burst-size", type=int, default=500,
                    help="burst: rows per burst (default 500)")
    cl.add_argument("--burst-every", type=float, default=15.0,
                    help="burst: seconds between bursts (default 15)")
    cl.add_argument("--sample-interval", type=float, default=0.25,
                    help="seconds between target lag samples (default 0.25)")
    cl.add_argument("--drain", type=float, default=30.0,
                    help="seconds to keep sampling after writes stop (default 30)")
    cl.add_argument("--report", default="", help="write a JSON report here")
    cl.set_defaults(func=cmd_cdc_lag)

    args = ap.parse_args()

    if not cfg("DB_HOST") or not cfg("DB_PASSWORD"):
        log("ERROR: set DB_HOST/DB_PASSWORD in .env (`set -a; source .env; set +a`).")
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
