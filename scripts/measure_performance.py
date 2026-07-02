#!/usr/bin/env python3
"""Measure migration PERFORMANCE on real infrastructure (throughput + CDC lag + OCC).

The functional harnesses answer "is it correct?"; this one answers "how fast, and
how does it behave under pressure?" -- the dimension none of the existing E2E
scripts measure. It drives the tool's OWN engine (nothing re-implemented) and
reports numbers an adopter needs to size a TB-scale migration.

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

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.batches = 0
        self.total_retries = 0
        self.batches_with_retry = 0
        self.rows_attempted = 0

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
    wanted: list[str] = []
    for t in tables_for(schema):
        q = f"{schema}.{t}"
        if q in present:
            wanted.append(q)
        elif t in present:
            present[t].name = q
            wanted.append(q)
    tables = TableSelector().resolve(inventory, TableSelection(selected_tables=wanted))

    # Turn on the per-batch DEBUG trace and attach our OCC counter to the loader's
    # logger only (not the root), so we capture occ_retries without noisy output.
    counter = _OccTraceCounter()
    importer_logger = logging.getLogger("dsql_migrator.core.batched_import")
    prev_level = importer_logger.level
    importer_logger.setLevel(logging.DEBUG)
    importer_logger.addHandler(counter)

    inputs = DataMigrationInputs(
        source_config=source, source_password=password, target_config=target,
        inventory=inventory, aws_profile=os.environ.get("AWS_PROFILE"),
        replace_tables=frozenset(wanted),  # clean slate: plain INSERT, real OCC surfaces
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
    jm.wait(job_id, timeout=7200)
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

    retry_rate = (counter.total_retries / counter.batches) if counter.batches else 0.0
    report = {
        "subcommand": "full-load", "schema": schema,
        "table_parallelism": args.table_parallelism,
        "batch_parallelism": args.batch_parallelism,
        "batch_rows": int(cfg("DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS", "2000")),
        "status": job.status, "wall_seconds": round(elapsed, 2),
        "total_rows": total_rows,
        "overall_rows_per_sec": round(total_rows / elapsed, 1) if elapsed > 0 else None,
        "occ": {
            "batches": counter.batches,
            "total_retries": counter.total_retries,
            "batches_with_retry": counter.batches_with_retry,
            "avg_retries_per_batch": round(retry_rate, 4),
            "pct_batches_with_retry": round(
                100.0 * counter.batches_with_retry / counter.batches, 2)
            if counter.batches else 0.0,
        },
        "per_table": per_table,
    }

    log(f"status={job.status} wall={elapsed:.1f}s rows={total_rows} "
        f"overall={report['overall_rows_per_sec']} rows/s")
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
    fl.add_argument("--table-parallelism", type=int, default=4)
    fl.add_argument("--batch-parallelism", type=int, default=8)
    fl.add_argument("--batch-rows", type=int, default=0,
                    help="rows per write batch (0 = tool default 2000, max 3000)")
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
