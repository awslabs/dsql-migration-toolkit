#!/usr/bin/env python3
"""Emit a steady heartbeat into the source MySQL DB for CDC replication monitoring.

This is a standalone operational utility (not part of the shipped tool). It maintains a
tiny dedicated schema (default ``cdc_monitor``) with one ``heartbeat`` table and inserts
one row per tick (default every 1 second), each carrying the source-side timestamp.

Why a separate schema/table: it isolates the monitoring signal from the business tables
(see scripts/seed_sample_db.py) so you can measure end-to-end CDC lag cleanly. On the
target (Aurora DSQL) you read the latest replicated ``ts`` and compare it against the
current time:  replication_lag = NOW() - MAX(ts).  Because every insert is its own
committed transaction, the binlog event flushes promptly and the heartbeat cadence maps
directly to change events the CDC pipeline must capture and replay.

Connection settings are read from .env (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD), identical
to the seed/CDC-load scripts. The password is never logged.

Run:
    .venv/bin/python scripts/cdc_heartbeat.py            # loop forever, 1 beat/sec
    .venv/bin/python scripts/cdc_heartbeat.py --setup    # only create schema+table, exit
Stop with Ctrl+C. Configure via environment:
    CDC_HB_SCHEMA        monitoring schema (database) name (default cdc_monitor)
    CDC_HB_TABLE         heartbeat table name (default heartbeat)
    CDC_HB_INTERVAL_SEC  seconds between beats (default 1)
"""
from __future__ import annotations

import os
import sys
import time
import datetime as dt

import pymysql

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_dotenv(path: str) -> dict:
    """Minimal KEY=VALUE parser for a .env file (no external dependency)."""
    values: dict = {}
    try:
        with open(path) as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                val = val.strip().strip('"').strip("'")
                values[key.strip()] = val
    except FileNotFoundError:
        pass
    return values


_ENV = load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))


def _cfg(key: str, default: str) -> str:
    return _ENV.get(key) or os.environ.get(key) or default


HOST = _cfg("DB_HOST")
PORT = int(_cfg("DB_PORT", "3306"))
USER = _cfg("DB_USER", "admin")

SCHEMA = _cfg("CDC_HB_SCHEMA", "cdc_monitor")
TABLE = _cfg("CDC_HB_TABLE", "heartbeat")
INTERVAL_SEC = float(_cfg("CDC_HB_INTERVAL_SEC", "1"))


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def connect(db: str | None = None) -> pymysql.connections.Connection:
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not pw:
        log("FATAL: no DB password. Set DB_PASSWORD in .env (or MYSQL_PWD env var).")
        sys.exit(1)
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=pw, database=db,
        connect_timeout=15, read_timeout=60, write_timeout=60,
        autocommit=False, charset="utf8mb4",
    )


def ensure_schema() -> None:
    """Create the monitoring schema and heartbeat table if they do not exist.

    id           dense, monotonic beat sequence (gaps possible under interleaved
                 autoinc, but strictly increasing -- fine for ordering/dedup).
    ts           source-side beat time at microsecond precision; the value CDC
                 replicates and the target compares against NOW() to derive lag.
    """
    conn = connect(None)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS {SCHEMA} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} ("
                " id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                " ts DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),"
                " KEY idx_heartbeat_ts (ts)"
                ") ENGINE=InnoDB"
            )
        conn.commit()
        log(f"Ensured schema `{SCHEMA}` and table `{TABLE}` (id, ts).")
    finally:
        conn.close()


def main() -> None:
    ensure_schema()
    if "--setup" in sys.argv:
        log("Setup only (--setup): schema/table ready. Exiting.")
        return

    log(f"CDC heartbeat -> `{SCHEMA}.{TABLE}` on {HOST} every {INTERVAL_SEC:g}s. "
        "Ctrl+C to stop.")
    conn = connect(SCHEMA)
    beats = 0
    try:
        while True:
            start = time.time()
            try:
                with conn.cursor() as cur:
                    # Let the column default stamp ts at microsecond precision so the
                    # beat time is the DB's own clock (consistent reference for lag).
                    cur.execute(f"INSERT INTO {TABLE} () VALUES ()")
                    beat_id = cur.lastrowid
                conn.commit()
                beats += 1
                if beats <= 5 or beats % 60 == 0:
                    log(f"beat {beats}: id={beat_id} (logging every 60th beat)")
            except Exception as e:  # noqa: BLE001
                log(f"beat error: {str(e)[:200]} -- reconnecting")
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(min(5.0, INTERVAL_SEC))
                conn = connect(SCHEMA)
            elapsed = time.time() - start
            time.sleep(max(0.0, INTERVAL_SEC - elapsed))
    except KeyboardInterrupt:
        log(f"Stopped after {beats} beats.")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
