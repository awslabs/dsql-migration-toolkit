# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Compare row counts between the source (MySQL or PostgreSQL) and target Aurora DSQL.

A read-only consistency check for Full Load + CDC testing: for each table it
reports the source vs target row count (and single-column PK min/max range), and
whether they MATCH. Designed to be called repeatedly during a migration --
before Full Load (expect target empty), after Full Load (expect counts equal),
and during CDC (run again after each DML batch to watch the target converge).

Connection settings come from .env (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD for the
source; TARGET_ENDPOINT for DSQL), matching the other scripts/ helpers. The DSQL
side uses the tool's own IAM-token connector. The source side supports BOTH
engines: MySQL via PyMySQL (handles native-password auth the Homebrew mysql 9.x
CLI dropped) and PostgreSQL via psycopg -- select with ``SOURCE_TYPE`` (default
``mysql``; the PG-source E2E harness ``run_pg_cdc_e2e.py`` exports
``SOURCE_TYPE=postgres``). PK detection uses standard ``information_schema``
(``constraint_type='PRIMARY KEY'``), which is correct on both engines, and
identifiers are quoted per engine (backticks for MySQL, double quotes for PG).

Usage:
  .venv/bin/python scripts/compare_rows.py                      # default: cdc_demo.orders
  .venv/bin/python scripts/compare_rows.py -t cdc_demo.orders -t cdc_demo.customers
  .venv/bin/python scripts/compare_rows.py --watch 10           # re-check every 10s (Ctrl-C to stop)
  SOURCE_TYPE=postgres .venv/bin/python scripts/compare_rows.py -t sch.tbl   # PG source

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
    # os.environ FIRST so an explicit export wins over .env (matches run_e2e_migration.py
    # / run_pg_cdc_e2e.py). This lets a caller override the source engine/host/port —
    # e.g. run_pg_cdc_e2e.py exports SOURCE_TYPE=postgres + DB_PORT for a PG-source run
    # even though .env still holds the default MySQL DB_HOST/DB_PORT.
    return os.environ.get(key) or _ENV.get(key) or default


# Source engine: mysql (default) or postgres. The tool's own SourceType names are
# "mysql"/"postgres"; accept the common aliases too.
SOURCE_TYPE = _cfg("SOURCE_TYPE", "mysql").strip().lower()
_IS_PG = SOURCE_TYPE in ("postgres", "postgresql", "pg")

HOST = _cfg("DB_HOST")
PORT = int(_cfg("DB_PORT", "5432" if _IS_PG else "3306"))
USER = _cfg("DB_USER", "admin")
TARGET_ENDPOINT = _cfg("TARGET_ENDPOINT")
TARGET_REGION = _cfg("TARGET_REGION", "us-east-1")
TARGET_DATABASE = _cfg("TARGET_DATABASE", "postgres")
TARGET_USERNAME = _cfg("TARGET_USERNAME", "admin")


def quote_ident(ident: str) -> str:
    """Engine-correct SOURCE-side identifier quoting (PG double quotes / MySQL backticks)."""
    if _IS_PG:
        return '"' + ident.replace('"', '""') + '"'
    return "`" + ident.replace("`", "``") + "`"


# --------------------------------------------------------------------------- #
# Connections
# --------------------------------------------------------------------------- #


def source_connect():
    pw = _cfg("DB_PASSWORD") or os.environ.get("PGPASSWORD" if _IS_PG else "MYSQL_PWD")
    if not HOST or not pw:
        log("FATAL: source not configured. Set DB_HOST + DB_PASSWORD in .env.")
        sys.exit(2)
    if _IS_PG:
        import psycopg  # a project dependency; imported lazily so MySQL runs don't need it loaded

        conn = psycopg.connect(
            host=HOST, port=PORT, user=USER, password=pw,
            dbname=_cfg("DB_NAME", "postgres"), sslmode=_cfg("DB_SSLMODE", "require"),
            connect_timeout=15,
        )
        conn.autocommit = True
        return conn
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
    fq = f"{quote_ident(schema)}.{quote_ident(table)}"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s", (schema, table))
        if not cur.fetchone()[0]:
            return {"exists": False}
        # PK via standard SQL (works on MySQL AND PostgreSQL): join table_constraints
        # (constraint_type='PRIMARY KEY') to key_column_usage. MySQL's legacy
        # constraint_name='PRIMARY' filter would miss a PostgreSQL PK (named
        # "<table>_pkey"), so use the portable constraint_type instead.
        cur.execute(
            "SELECT kcu.column_name FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name=kcu.constraint_name "
            "  AND tc.table_schema=kcu.table_schema AND tc.table_name=kcu.table_name "
            "WHERE tc.table_schema=%s AND tc.table_name=%s "
            "  AND tc.constraint_type='PRIMARY KEY' "
            "ORDER BY kcu.ordinal_position", (schema, table))
        pk = [r[0] for r in cur.fetchall()]
        cur.execute(f"SELECT COUNT(*) FROM {fq}")
        count = cur.fetchone()[0]
        mn = mx = None
        if len(pk) == 1:
            validate_identifier(pk[0], "pk column")
            cur.execute(f"SELECT MIN({quote_ident(pk[0])}), MAX({quote_ident(pk[0])}) FROM {fq}")
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
