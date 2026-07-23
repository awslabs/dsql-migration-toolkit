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
              wall-time, total + per-table rows/sec, and -- the DSQL-specific
              signal -- the OPTIMISTIC-CONCURRENCY retry rate. DSQL detects write
              conflicts at commit (SQLSTATE 40001); a monotonic AUTO_INCREMENT PK
              makes every insert target the same "rightmost" key range, so raising
              parallelism raises the OCC collision rate (docs §7.1 "avoid hot
              partitions"). This captures that: it enables the loader's per-batch
              DEBUG trace, parses ``occ_retries=`` off every batch, and reports the
              retry rate at the chosen parallelism -- so you can see the hot-PK
              contention curve by re-running with higher --table/--batch-parallelism.

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
import json
import logging
import os
import re
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
        SourceConnectionConfig, TargetConnectionConfig,
    )
    source = SourceConnectionConfig(
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "3306")),
        database=schema, username=cfg("DB_USER", "admin"),
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
# full-load: throughput + OCC-retry rate (parsed from the loader's DEBUG trace)
# =========================================================================== #
class _OccTraceCounter(logging.Handler):
    """Captures the batched importer's per-batch DEBUG trace and tallies OCC retries.

    The loader logs one DEBUG line per batch:
    ``import batch chunk=... attempted=N inserted=M ... occ_retries=R`` (guarded by
    ``isEnabledFor(DEBUG)``, so it only fires because we raise the level here). We
    parse ``occ_retries`` + ``attempted`` off each line -- credential-free (PK
    values are not in these fields we read) -- to report the retry rate.
    """

    _RE_RETRIES = re.compile(r"occ_retries=(\d+)")
    _RE_ATTEMPTED = re.compile(r"attempted=(\d+)")
    _RE_ELAPSED = re.compile(r"elapsed_ms=([\d.]+)")

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.batches = 0
        self.total_retries = 0
        self.batches_with_retry = 0
        self.rows_attempted = 0
        # Per-batch write round-trip times (ms). The distribution answers the key
        # question when a load is write-bound: is throughput capped by DSQL write
        # RTT (high, tight per-batch ms) or by the client (low per-batch ms but few
        # in flight)? Compared across batch_parallelism/batch_rows sweeps.
        self.batch_ms: list[float] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = record.getMessage()
        except Exception:  # noqa: BLE001 - never let logging break the load
            return
        m = self._RE_RETRIES.search(msg)
        if not m:
            return
        retries = int(m.group(1))
        self.batches += 1
        self.total_retries += retries
        if retries:
            self.batches_with_retry += 1
        a = self._RE_ATTEMPTED.search(msg)
        if a:
            self.rows_attempted += int(a.group(1))
        e = self._RE_ELAPSED.search(msg)
        if e:
            self.batch_ms.append(float(e.group(1)))


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


def _monitor_full_load(jm, job_id: str, tables, interval: float) -> dict:
    """Block until the job leaves RUNNING, logging live per-table progress.

    Reads the same thread-safe ``get_status`` snapshot the UI polls (never mutates
    the job), so it is safe and read-only. For each poll it logs, per table that is
    IN_PROGRESS or newly DONE, the rows loaded vs the watermark's approximate target
    (a %), the instantaneous rows/s since the previous poll, and an ETA from the
    remaining rows at that instantaneous rate. The watermark row-counts are scan-free
    estimates (``information_schema``), so % / ETA are approximate by design -- they
    exist to show the run is alive and roughly how far along, not to be exact.

    Also samples the container's cgroup CPU%/memory each poll (Linux/Fargate only;
    "n/a" on macOS) and returns the PEAK ``{"cpu_pct", "mem_bytes"}`` for the report,
    so an A/B run records whether Full Load was CPU-saturated.
    """
    from dsql_migrator.core.job_manager import JobNotFoundError

    total_expected = len(tables)
    # Per-table last-seen (rows_loaded, monotonic_ts) to derive instantaneous rows/s.
    last: dict[str, tuple[int, float]] = {}
    cpu_sampler = _CpuSampler()  # cgroup CPU% between polls (None on macOS/no cgroup)
    peak = {"cpu_pct": None, "mem_bytes": None}  # returned for the report
    start = time.monotonic()
    while True:
        time.sleep(max(1.0, interval))
        try:
            job = jm.get_status(job_id)
        except JobNotFoundError:
            return peak
        now = time.monotonic()
        wm_counts = {}
        wm = getattr(job, "watermark", None)
        if wm is not None:
            wm_counts = getattr(wm, "table_row_counts", {}) or {}

        done = sum(1 for c in job.chunks if c.status == "DONE")
        failed = sum(1 for c in job.chunks if c.status == "FAILED")
        active = [c for c in job.chunks if c.status == "IN_PROGRESS"]
        total_loaded = sum(int(c.rows_loaded or 0) for c in job.chunks)
        overall_rps = total_loaded / (now - start) if now > start else 0.0

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

        header = (
            f"progress: {done}/{total_expected} done"
            f"{(', ' + str(failed) + ' failed') if failed else ''}"
            f" | {_abbrev(total_loaded)} rows | ~{overall_rps:,.0f} rows/s overall"
            f"{res}"
        )
        log(header)
        for c in active:
            loaded = int(c.rows_loaded or 0)
            target = wm_counts.get(c.chunk_id)
            prev = last.get(c.chunk_id)
            inst_rps = None
            if prev is not None and now > prev[1]:
                inst_rps = (loaded - prev[0]) / (now - prev[1])
            last[c.chunk_id] = (loaded, now)
            pct = f"{100.0 * loaded / target:.1f}%" if target else "?"
            eta = None
            if target and inst_rps and inst_rps > 0:
                eta = max(0, (target - loaded)) / inst_rps
            log(f"    {c.chunk_id:<32} {_abbrev(loaded)}/{_abbrev(target)} "
                f"({pct})  {(f'{inst_rps:,.0f}' if inst_rps is not None else '?'):>8} "
                f"rows/s  ETA {_fmt_eta(eta)}")

        if job.status not in ("PENDING", "RUNNING"):
            log(f"job {job.status} after {now - start:.1f}s")
            return peak


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
    # --reader-shards K sets reader range sharding (K concurrent readers per large
    # single-integer-PK table); 1 = off. Same one-image A/B pattern as prefetch.
    if getattr(args, "reader_shards", 0):
        os.environ["DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS"] = str(args.reader_shards)

    from dsql_migrator.core.converter import (
        PrimaryKeyStrategy, SchemaConvertOptions, SchemaConverter,
    )
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    from dsql_migrator.ui.data_migration._engine import (
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

    # Turn on the per-batch DEBUG trace and attach our OCC counter to the loader's
    # logger only (not the root), so we capture occ_retries without noisy output.
    counter = _OccTraceCounter()
    importer_logger = logging.getLogger("dsql_migrator.core.batched_import")
    prev_level = importer_logger.level
    importer_logger.setLevel(logging.DEBUG)
    importer_logger.addHandler(counter)
    # Surface the retry-loop diagnostics (per-attempt DEBUG + give-up WARNING) to
    # stdout/CloudWatch via a dedicated handler on the occ logger: which error, its
    # SQLSTATE, attempt count, and total elapsed per failed batch -- direct evidence
    # of WHY a batch failed (budget too small vs a storm longer than the budget vs a
    # non-transient error), instead of inferring from timing.
    occ_logger = logging.getLogger("dsql_migrator.core.occ")
    prev_occ_level = occ_logger.level
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
        converter = SchemaConverter()
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

    inputs = DataMigrationInputs(
        source_config=source, source_password=password, target_config=target,
        inventory=inventory, aws_profile=os.environ.get("AWS_PROFILE"),
        replace_tables=frozenset(wanted),  # clean slate: plain INSERT, real OCC surfaces
        table_conversions=table_conversions,
    )
    migrator = default_migrator_factory(inputs)
    error_log = ErrorLogStore()
    jm = JobManager(stall_timeout_seconds=None)

    def work(handle) -> None:
        run_full_load(handle, tables, migrator=migrator, error_log=error_log)

    log(f"Full Load '{schema}' ({len(tables)} tables) at "
        f"tp={args.table_parallelism} bp={args.batch_parallelism} ...")
    t0 = time.monotonic()
    job_id = jm.submit(work)
    # Instead of blocking silently on jm.wait(), poll the live job snapshot on a
    # cadence and log per-table progress (rows/target %, instantaneous + cumulative
    # rows/s, ETA). chunk.rows_loaded advances live (the engine flushes every
    # PROGRESS_FLUSH_ROWS as batches land), so this turns a long, opaque load into a
    # monitorable one -- the whole point of a perf run. Read-only: it only reads the
    # same get_status() snapshot the UI polls; it never mutates the job.
    peak_resources = _monitor_full_load(
        jm, job_id, tables, interval=args.progress_interval
    )
    elapsed = time.monotonic() - t0
    importer_logger.removeHandler(counter)
    importer_logger.setLevel(prev_level)

    job = jm.get_status(job_id)
    total_rows = sum(int(c.rows_loaded or 0) for c in job.chunks)
    per_table = []
    for c in job.chunks:
        secs = None
        if c.started_at and c.finished_at:
            secs = (c.finished_at - c.started_at).total_seconds()
        rps = (c.rows_loaded / secs) if (secs and secs > 0) else None
        per_table.append({
            "table": c.chunk_id, "status": c.status,
            "rows_loaded": c.rows_loaded, "seconds": round(secs, 2) if secs else None,
            "rows_per_sec": round(rps, 1) if rps else None,
        })

    # DATA-PHASE throughput = total_rows / (last chunk finish - first chunk start).
    # This excludes wall-time spent OUTSIDE the actual row loading -- job submit,
    # DROP+recreate DDL, and the post-data tail where one table finished before the
    # other so parallelism dropped. The adversarial perf review found that tail
    # (~18.6s in the local prefetch run) dragged overall_rows_per_sec below the true
    # loading rate and made a killed-at-95%% baseline (a mid-run cumulative) look
    # falsely faster. data_phase_rows_per_sec is the apples-to-apples metric to
    # compare variants on; overall_rows_per_sec is kept for continuity.
    starts = [c.started_at for c in job.chunks if c.started_at]
    finishes = [c.finished_at for c in job.chunks if c.finished_at]
    data_phase_secs = None
    if starts and finishes:
        span = (max(finishes) - min(starts)).total_seconds()
        data_phase_secs = span if span > 0 else None
    data_phase_rps = (
        round(total_rows / data_phase_secs, 1) if data_phase_secs else None
    )

    retry_rate = (counter.total_retries / counter.batches) if counter.batches else 0.0
    report = {
        "subcommand": "full-load", "schema": schema,
        "prefetch": not getattr(args, "no_prefetch", False),
        # Composite-PK A/B: which variant this run measured. composite_leading is
        # the prepended leading column (None = KEEP-integer baseline); composite is
        # a convenience boolean, and composite_tables how many tables got the key.
        "composite": bool(leading and table_conversions),
        "composite_leading": leading or None,
        "composite_tables": len(table_conversions),
        # Short label consumed by perf_compare.py's comparison table.
        "variant": {
            "label": f"composite:{leading}" if (leading and table_conversions)
            else "keep-integer",
        },
        "table_parallelism": args.table_parallelism,
        "batch_parallelism": args.batch_parallelism,
        "batch_rows": int(cfg("DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS", "2000")),
        "reader_shards": int(cfg("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "1")),
        "status": job.status, "wall_seconds": round(elapsed, 2),
        "total_rows": total_rows,
        "overall_rows_per_sec": round(total_rows / elapsed, 1) if elapsed > 0 else None,
        "data_phase_seconds": round(data_phase_secs, 2) if data_phase_secs else None,
        "data_phase_rows_per_sec": data_phase_rps,
        "peak_cpu_pct": peak_resources.get("cpu_pct"),
        "peak_mem_bytes": peak_resources.get("mem_bytes"),
        "occ": {
            "batches": counter.batches,
            "total_retries": counter.total_retries,
            "batches_with_retry": counter.batches_with_retry,
            "avg_retries_per_batch": round(retry_rate, 4),
            "pct_batches_with_retry": round(
                100.0 * counter.batches_with_retry / counter.batches, 2)
            if counter.batches else 0.0,
        },
        # Per-batch write round-trip distribution (ms). Tells write-bound vs
        # client-bound: high & tight = DSQL write RTT is the wall (raising
        # batch_parallelism helps until the target saturates); low = the client
        # isn't keeping enough batches in flight.
        "write_rtt_ms": {
            "samples": len(counter.batch_ms),
            "p50": round(_pct(counter.batch_ms, 50), 1) if counter.batch_ms else None,
            "p95": round(_pct(counter.batch_ms, 95), 1) if counter.batch_ms else None,
            "max": round(max(counter.batch_ms), 1) if counter.batch_ms else None,
            "mean": round(statistics.fmean(counter.batch_ms), 1)
            if counter.batch_ms else None,
        },
        "per_table": per_table,
    }

    log(f"status={job.status} wall={elapsed:.1f}s rows={total_rows} "
        f"overall={report['overall_rows_per_sec']} rows/s")
    if counter.batch_ms:
        w = report["write_rtt_ms"]
        log(f"write RTT/batch: p50={w['p50']}ms p95={w['p95']}ms max={w['max']}ms "
            f"mean={w['mean']}ms (n={w['samples']}) at bp={args.batch_parallelism} "
            f"batch_rows={report['batch_rows']}")
    log(f"OCC: {counter.batches} batches, {counter.total_retries} total 40001 retries, "
        f"{report['occ']['pct_batches_with_retry']}% of batches retried "
        f"(avg {retry_rate:.3f}/batch) at tp={args.table_parallelism} "
        f"bp={args.batch_parallelism}")
    log("  (re-run with higher --table/--batch-parallelism to see the hot-PK "
        "contention curve; a monotonic AUTO_INCREMENT PK raises this.)")
    for r in per_table:
        log(f"  {r['table']:<28} {str(r['status']):<8} rows={r['rows_loaded']:<8} "
            f"{r['seconds']}s  {r['rows_per_sec']} rows/s")

    _maybe_write_report(args, report)
    return 0 if job.status == "DONE" else 1


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
                    help="seconds between live progress logs during the load "
                         "(default 15; min 1)")
    fl.add_argument("--no-prefetch", action="store_true",
                    help="disable the read-ahead prefetch queue (measure the "
                         "pre-improvement baseline path); default keeps it ON")
    fl.add_argument("--reader-shards", type=int, default=0,
                    help="reader range sharding: concurrent readers per large "
                         "single-int-PK table (0/1 = off = one reader; >1 shards)")
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
