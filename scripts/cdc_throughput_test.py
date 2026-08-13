#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC throughput test: generate a high-rate INSERT workload on the source
and measure how quickly the CDC sink catches up on the target (lag convergence).

This script has two modes:
  1. GENERATE: blast N rows/sec into the source MySQL for a fixed duration
  2. MEASURE: poll source vs target row counts and compute CDC lag + throughput

Usage:
    # Phase 1: Generate load (run for 5 minutes at 1000 rows/sec)
    .venv/bin/python scripts/cdc_throughput_test.py generate \
        --rate 1000 --duration 300 --table order_items

    # Phase 2: Measure catch-up (poll every 10s until lag=0 or timeout)
    .venv/bin/python scripts/cdc_throughput_test.py measure \
        --table order_items --interval 10 --timeout 600

    # All-in-one: generate for 5min, then measure until caught up
    .venv/bin/python scripts/cdc_throughput_test.py run \
        --rate 1000 --duration 300 --table order_items

Environment (from .env):
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME (source MySQL)
    TARGET_ENDPOINT (Aurora DSQL)

The test is ADDITIVE (INSERTs only) and does not disturb existing data.
Rows are inserted into a dedicated `_cdc_perf_test` table (auto-created)
so it doesn't interfere with the main schema.

CDC throughput = rows_delivered_to_target / wall_time_from_first_insert.
CDC lag = source_count - target_count at each poll point.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_dotenv(path: str) -> None:
    try:
        with open(path) as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")
                if key and key not in os.environ:
                    os.environ[key] = value
    except FileNotFoundError:
        pass


load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))


def _source_conn():
    import pymysql
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ.get("DB_USER", "admin"),
        password=os.environ["DB_PASSWORD"],
        database=os.environ.get("DB_NAME", "customers_sample"),
        autocommit=True,
        charset="utf8mb4",
    )


def _target_conn():
    """Connect to Aurora DSQL target (IAM auth)."""
    sys.path.insert(0, os.path.join(_PROJECT_ROOT, "src"))
    from dsql_migrator.core.target_connection import DsqlConnector
    from dsql_migrator.core.models import TargetConnectionConfig

    endpoint = os.environ["TARGET_ENDPOINT"]
    region = os.environ.get("TARGET_REGION")
    if not region:
        # Extract region from endpoint: xxx.dsql.<region>.on.aws
        parts = endpoint.split(".")
        idx = parts.index("dsql") if "dsql" in parts else -1
        region = parts[idx + 1] if idx >= 0 and idx + 1 < len(parts) else "us-east-1"

    config = TargetConnectionConfig(cluster_endpoint=endpoint, region=region)
    connector = DsqlConnector(config, aws_profile=os.environ.get("AWS_PROFILE"))
    return connector.connect()


PERF_TABLE = "_cdc_perf_test"
CREATE_DDL = f"""
CREATE TABLE IF NOT EXISTS {PERF_TABLE} (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    batch_id VARCHAR(36) NOT NULL,
    payload VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
# DSQL equivalent (for target — may already exist if CDC replicated the DDL via schema apply)
CREATE_DDL_DSQL = f"""
CREATE TABLE IF NOT EXISTS {PERF_TABLE} (
    id BIGINT PRIMARY KEY,
    batch_id VARCHAR(36) NOT NULL,
    payload VARCHAR(200) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""


def ensure_source_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_DDL)


def ensure_target_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_DDL_DSQL)
    conn.commit()


def cmd_generate(args) -> None:
    """Blast rows into the source at a target rate."""
    conn = _source_conn()
    ensure_source_table(conn)

    rate = args.rate  # rows/sec target
    duration = args.duration  # seconds
    batch_size = min(100, rate)  # rows per INSERT statement
    batch_id = str(uuid.uuid4())[:8]

    print(f"[generate] target={rate} rows/s, duration={duration}s, "
          f"batch_size={batch_size}, batch_id={batch_id}")

    total_inserted = 0
    t0 = time.monotonic()

    with conn.cursor() as cur:
        while True:
            elapsed = time.monotonic() - t0
            if elapsed >= duration:
                break

            # Calculate how many rows we should have inserted by now
            target_rows = int(elapsed * rate)
            deficit = target_rows - total_inserted
            if deficit <= 0:
                time.sleep(0.01)
                continue

            # Insert a batch. The row VALUES are BOUND, not formatted into the SQL
            # text: PyMySQL's executemany collapses an INSERT into one multi-row
            # statement (bounded by max_allowed_packet), so the batch is still a
            # single round trip -- the property this throughput test depends on --
            # without the generated payload ever becoming part of the statement.
            rows_this_batch = min(batch_size, deficit)
            payload = f"perf-{batch_id}-{uuid.uuid4().hex[:16]}"
            cur.executemany(
                f"INSERT INTO {PERF_TABLE} (batch_id, payload) VALUES (%s, %s)",
                [(batch_id, payload)] * rows_this_batch,
            )
            total_inserted += rows_this_batch

            # Progress every 10s
            if int(elapsed) % 10 == 0 and int(elapsed) > 0:
                actual_rate = total_inserted / elapsed
                print(f"  [{elapsed:.0f}s] {total_inserted:,} rows, "
                      f"actual rate: {actual_rate:,.0f} rows/s")

    elapsed = time.monotonic() - t0
    actual_rate = total_inserted / elapsed if elapsed > 0 else 0
    print(f"[generate] done: {total_inserted:,} rows in {elapsed:.1f}s "
          f"({actual_rate:,.0f} rows/s actual)")
    conn.close()


def cmd_measure(args) -> None:
    """Poll source vs target counts and report CDC lag + throughput."""
    src = _source_conn()
    tgt = _target_conn()
    ensure_target_table(tgt)

    interval = args.interval
    timeout = args.timeout
    schema = os.environ.get("DB_NAME", "customers_sample")
    table = f"{schema}.{PERF_TABLE}" if "." not in PERF_TABLE else PERF_TABLE

    print(f"[measure] polling every {interval}s, timeout={timeout}s")
    print(f"{'time':>6} | {'source':>10} | {'target':>10} | {'lag':>8} | {'CDC rows/s':>10}")
    print("-" * 60)

    t0 = time.monotonic()
    first_target = None
    while True:
        elapsed = time.monotonic() - t0
        if elapsed > timeout:
            print(f"[measure] timeout after {timeout}s")
            break

        with src.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {PERF_TABLE}")
            src_count = cur.fetchone()[0]

        with tgt.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {PERF_TABLE}")
            tgt_count = cur.fetchone()[0]

        lag = src_count - tgt_count
        if first_target is None and tgt_count > 0:
            first_target = (tgt_count, time.monotonic())

        cdc_rps = ""
        if first_target is not None:
            delivered = tgt_count - first_target[0]
            dt = time.monotonic() - first_target[1]
            if dt > 0 and delivered > 0:
                cdc_rps = f"{delivered / dt:,.0f}"

        print(f"{elapsed:>5.0f}s | {src_count:>10,} | {tgt_count:>10,} | "
              f"{lag:>8,} | {cdc_rps:>10}")

        if lag == 0 and src_count > 0:
            print(f"[measure] lag=0 — CDC fully caught up!")
            break

        time.sleep(interval)

    src.close()
    tgt.close()


def cmd_run(args) -> None:
    """Generate load then measure catch-up."""
    print("=" * 60)
    print(f"CDC Throughput Test: {args.rate} rows/s × {args.duration}s "
          f"= {args.rate * args.duration:,} rows")
    print("=" * 60)
    print()

    cmd_generate(args)
    print()
    print("--- Waiting 5s for CDC pipeline to start processing ---")
    time.sleep(5)
    print()
    cmd_measure(args)


def main():
    parser = argparse.ArgumentParser(description="CDC throughput test")
    sub = parser.add_subparsers(dest="command")

    gen = sub.add_parser("generate", help="Blast rows into source")
    gen.add_argument("--rate", type=int, default=1000, help="Target rows/sec")
    gen.add_argument("--duration", type=int, default=300, help="Seconds to run")

    meas = sub.add_parser("measure", help="Poll lag and report CDC throughput")
    meas.add_argument("--interval", type=int, default=10, help="Poll interval (sec)")
    meas.add_argument("--timeout", type=int, default=600, help="Max wait (sec)")

    run = sub.add_parser("run", help="Generate then measure (all-in-one)")
    run.add_argument("--rate", type=int, default=1000, help="Target rows/sec")
    run.add_argument("--duration", type=int, default=300, help="Generate duration (sec)")
    run.add_argument("--interval", type=int, default=10, help="Measure poll interval")
    run.add_argument("--timeout", type=int, default=600, help="Measure max wait")

    args = parser.parse_args()
    if args.command == "generate":
        cmd_generate(args)
    elif args.command == "measure":
        cmd_measure(args)
    elif args.command == "run":
        cmd_run(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
