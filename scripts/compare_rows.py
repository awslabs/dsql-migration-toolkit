# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compare row counts between the source MySQL and the target Aurora DSQL.

A read-only consistency check for Full Load + CDC testing: for each table it
reports the source vs target row count (and single-column PK min/max range), and
whether they MATCH. Designed to be called repeatedly during a migration --
before Full Load (expect target empty), after Full Load (expect counts equal),
and during CDC (run again after each DML batch to watch the target converge).

Connection settings come from .env (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD for the
source; TARGET_ENDPOINT for DSQL), matching the other scripts/ helpers. The DSQL
side uses the tool's own IAM-token connector, and the MySQL side uses PyMySQL
(which handles native-password auth that the Homebrew mysql 9.x CLI dropped).

Usage:
  .venv/bin/python scripts/compare_rows.py                      # default: cdc_demo.orders
  .venv/bin/python scripts/compare_rows.py -t cdc_demo.orders -t cdc_demo.customers
  .venv/bin/python scripts/compare_rows.py --watch 10           # re-check every 10s (Ctrl-C to stop)

Exit code: 0 when every checked table matches, 1 when any differs/errors -- so it
can gate a test script (e.g. `python scripts/compare_rows.py && echo OK`).
"""
from __future__ import annotations

import argparse
import os
import sys
import time

import pymysql

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _common import load_dotenv, log, validate_identifier  # noqa: E402

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_ENV = load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))


def _cfg(key: str, default: str = "") -> str:
    return _ENV.get(key) or os.environ.get(key) or default


HOST = _cfg("DB_HOST")
PORT = int(_cfg("DB_PORT", "3306"))
USER = _cfg("DB_USER", "admin")
TARGET_ENDPOINT = _cfg("TARGET_ENDPOINT")
TARGET_REGION = _cfg("TARGET_REGION", "us-east-1")
TARGET_DATABASE = _cfg("TARGET_DATABASE", "postgres")
TARGET_USERNAME = _cfg("TARGET_USERNAME", "admin")


# --------------------------------------------------------------------------- #
# Connections
# --------------------------------------------------------------------------- #


def source_connect() -> pymysql.connections.Connection:
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not HOST or not pw:
        log("FATAL: source not configured. Set DB_HOST + DB_PASSWORD in .env.")
        sys.exit(2)
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=pw,
        connect_timeout=15, read_timeout=60, charset="utf8mb4", autocommit=True,
    )


def target_connect():
    if not TARGET_ENDPOINT:
        log("FATAL: target not configured. Set TARGET_ENDPOINT in .env.")
        sys.exit(2)
    # Imported lazily so the script still parses without the package installed.
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.core.target_connection import DsqlConnector

    cfg = TargetConnectionConfig(
        cluster_endpoint=TARGET_ENDPOINT, region=TARGET_REGION,
        database=TARGET_DATABASE, username=TARGET_USERNAME,
    )
    return DsqlConnector(cfg, aws_profile=os.environ.get("AWS_PROFILE")).connect()


# --------------------------------------------------------------------------- #
# Per-side stats
# --------------------------------------------------------------------------- #


def _split(table: str) -> tuple[str, str]:
    """Split 'schema.table' -> (schema, table); default schema is cdc_demo."""
    if "." in table:
        schema, name = table.split(".", 1)
    else:
        schema, name = "cdc_demo", table
    return schema, name


def source_stats(conn, schema: str, table: str) -> dict:
    # Identifiers are interpolated (they cannot be bound), so validate first.
    validate_identifier(schema, "schema")
    validate_identifier(table, "table")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s", (schema, table))
        if not cur.fetchone()[0]:
            return {"exists": False}
        cur.execute(
            "SELECT column_name FROM information_schema.key_column_usage "
            "WHERE table_schema=%s AND table_name=%s AND constraint_name='PRIMARY' "
            "ORDER BY ordinal_position", (schema, table))
        pk = [r[0] for r in cur.fetchall()]
        cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
        count = cur.fetchone()[0]
        mn = mx = None
        if len(pk) == 1:
            validate_identifier(pk[0], "pk column")
            cur.execute(f"SELECT MIN(`{pk[0]}`), MAX(`{pk[0]}`) FROM `{schema}`.`{table}`")
            mn, mx = cur.fetchone()
    return {"exists": True, "pk": pk, "count": count, "min": mn, "max": mx}


def target_stats(conn, schema: str, table: str, pk: list) -> dict:
    # Identifiers are interpolated (they cannot be bound), so validate first.
    validate_identifier(schema, "schema")
    validate_identifier(table, "table")
    cur = conn.cursor()
    # The Full Load writes to the schema-qualified target; the sink may also have
    # used 'public' historically -- prefer the qualified schema, fall back to public.
    cur.execute(
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name=%s AND table_schema IN (%s, 'public') "
        "ORDER BY (table_schema=%s) DESC", (table, schema, schema))
    rows = cur.fetchall()
    if not rows:
        return {"exists": False}
    sch = validate_identifier(rows[0][0], "target schema")
    cur.execute(f'SELECT COUNT(*) FROM "{sch}"."{table}"')
    count = cur.fetchone()[0]
    mn = mx = None
    if len(pk) == 1:
        validate_identifier(pk[0], "pk column")
        cur.execute(f'SELECT MIN("{pk[0]}"), MAX("{pk[0]}") FROM "{sch}"."{table}"')
        mn, mx = cur.fetchone()
    return {"exists": True, "schema": sch, "count": count, "min": mn, "max": mx}


# --------------------------------------------------------------------------- #
# Compare + report
# --------------------------------------------------------------------------- #


def compare_once(tables: list[str]) -> bool:
    """Compare all tables; return True iff every table matches. Prints a report."""
    src = source_connect()
    tgt = target_connect()
    all_match = True
    try:
        print(f"{'TABLE':<28} {'SOURCE':>10} {'TARGET':>10}  RESULT")
        print("-" * 66)
        for table in tables:
            schema, name = _split(table)
            label = f"{schema}.{name}"
            s = source_stats(src, schema, name)
            if not s.get("exists"):
                print(f"{label:<28} {'(absent)':>10} {'-':>10}  SOURCE MISSING")
                all_match = False
                continue
            t = target_stats(tgt, schema, name, s.get("pk") or [])
            s_cnt = s["count"]
            if not t.get("exists"):
                print(f"{label:<28} {s_cnt:>10} {'(absent)':>10}  TARGET MISSING")
                all_match = False
                continue
            t_cnt = t["count"]
            same = (s_cnt == t_cnt and s.get("min") == t.get("min")
                    and s.get("max") == t.get("max"))
            result = "MATCH" if same else f"DIFFER (Δ={s_cnt - t_cnt:+d})"
            sch_note = "" if t.get("schema") == schema else f" [target schema: {t.get('schema')}]"
            print(f"{label:<28} {s_cnt:>10} {t_cnt:>10}  {result}{sch_note}")
            if not same:
                all_match = False
                print(f"{'':<28} pk[{(s.get('pk') or ['?'])[0]}] "
                      f"src {s.get('min')}..{s.get('max')}  "
                      f"tgt {t.get('min')}..{t.get('max')}")
        print("-" * 66)
        print("VERDICT:", "ALL TABLES MATCH ✓" if all_match else "MISMATCH ✗")
        return all_match
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            tgt.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare source vs target row counts.")
    parser.add_argument(
        "-t", "--table", action="append", dest="tables", metavar="SCHEMA.TABLE",
        help="Table to compare (repeatable). Default: cdc_demo.orders.")
    parser.add_argument(
        "--watch", type=float, default=0, metavar="SECONDS",
        help="Re-check on this interval until all match or Ctrl-C.")
    args = parser.parse_args()
    tables = args.tables or ["cdc_demo.orders"]

    if args.watch and args.watch > 0:
        log(f"Watching {tables} every {args.watch:g}s (Ctrl-C to stop)...")
        try:
            while True:
                if compare_once(tables):
                    log("All tables match — done.")
                    sys.exit(0)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            log("Stopped.")
            sys.exit(1)
    else:
        sys.exit(0 if compare_once(tables) else 1)


if __name__ == "__main__":
    main()
