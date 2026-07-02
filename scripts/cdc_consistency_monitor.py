#!/usr/bin/env python3
"""Continuously verify Full Load + CDC row consistency: source MySQL vs Aurora DSQL.

Standalone operational utility (NOT part of the shipped tool). It pairs with
``scripts/cdc_workload_customers_sample_new.py``: the workload WRITES a random
INSERT/UPDATE/DELETE stream into ``customers_sample_new``; this monitor VERIFIES
those writes replicate into DSQL, every ``--interval`` seconds, and names the
specific primary keys that have NOT yet landed on the target.

Per tick, per table it reports:
- SOURCE count (scan-free ``information_schema`` ESTIMATE by default, prefixed
  ``~``; exact ``COUNT(*)`` only under ``--exact-count``) vs TARGET exact count.
- High-water ``MAX(pk)`` on each side -> distinguishes BEHIND (target's newest pk
  lags source's: the tail is still in flight) from GAP (newest pks match but counts
  differ: a mid-stream row was skipped/dropped).
- When drift is detected, a BOUNDED sampled PK-set diff (``--sample`` PKs, never a
  full scan) naming up to a few of the primary keys present on source but missing
  on target.

TB-safe by construction (CLAUDE.md): the default hot path per table is a single
``information_schema`` estimate + an index-only ``MAX(pk)`` on the source (never a
full scan); exact ``COUNT(*)`` on the source is opt-in via ``--exact-count``. The
source is read-only (autocommit SELECT/SHOW only). Drift is judged on ``MAX(pk)`` +
the sampled PK diff, NEVER on the approximate-vs-exact count delta alone.

Confidentiality (Property 7): only PRIMARY KEY values (integers) and counts are
printed -- never row values. The DSQL IAM token stays inside the connector.

Connection settings come from ``.env`` (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD for the
source; TARGET_ENDPOINT for DSQL), matching scripts/compare_rows.py.

Usage:
  .venv/bin/python scripts/cdc_consistency_monitor.py                 # 10s loop, default tables
  .venv/bin/python scripts/cdc_consistency_monitor.py --once
  .venv/bin/python scripts/cdc_consistency_monitor.py --interval 5
  .venv/bin/python scripts/cdc_consistency_monitor.py --exact-count   # exact source COUNT(*)
  .venv/bin/python scripts/cdc_consistency_monitor.py -t customers_sample_new.orders
  .venv/bin/python scripts/cdc_consistency_monitor.py --format json

Exit code (with --once): 0 when every table is IN SYNC, 1 otherwise -- so it can
gate a test (e.g. `... --once && echo CONSISTENT`).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time

import pymysql

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _e2e_tables

from _e2e_tables import table_pks_for  # noqa: E402


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
                values[key.strip()] = val.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return values


_ENV = load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))


def _cfg(key: str, default: str = "") -> str:
    return _ENV.get(key) or os.environ.get(key) or default


HOST = _cfg("DB_HOST")
PORT = int(_cfg("DB_PORT", "3306"))
USER = _cfg("DB_USER", "admin")
SCHEMA = _cfg("CDC_WORKLOAD_SCHEMA", "customers_sample_new")
TARGET_ENDPOINT = _cfg("TARGET_ENDPOINT")
TARGET_REGION = _cfg("TARGET_REGION", "us-east-1")
TARGET_DATABASE = _cfg("TARGET_DATABASE", "postgres")
TARGET_USERNAME = _cfg("TARGET_USERNAME", "admin")

# Default table set = the tables the workload generator touches, each with its
# single-column integer PK. Single source of truth: scripts/_e2e_tables.py, keyed
# by the active schema. Schema-qualified at runtime with --schema. (table, pk)
_DEFAULT_TABLES = tuple(table_pks_for(SCHEMA))


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


# --------------------------------------------------------------------------- #
# Connections (mirror scripts/compare_rows.py)
# --------------------------------------------------------------------------- #


def source_connect():
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
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.core.target_connection import DsqlConnector

    cfg = TargetConnectionConfig(
        cluster_endpoint=TARGET_ENDPOINT, region=TARGET_REGION,
        database=TARGET_DATABASE, username=TARGET_USERNAME,
    )
    return DsqlConnector(cfg, aws_profile=os.environ.get("AWS_PROFILE")).connect()


# --------------------------------------------------------------------------- #
# Per-side reads (source = read-only SELECT/SHOW; never a full scan by default)
# --------------------------------------------------------------------------- #


def _norm_pk(value):
    """Canonical PK string for cross-engine set comparison (int-safe).

    MySQL and psycopg can return a PK as int / Decimal / str depending on the
    column type; normalize integral values to a plain base-10 string so a PK that
    is present on both sides is never spuriously classed missing/extra.
    """
    if value is None:
        return None
    try:
        return str(int(value))
    except (TypeError, ValueError):
        return str(value)


def source_estimate_and_max(conn, schema: str, table: str, pk: str, exact: bool):
    """Return (exists, count, is_estimate, max_pk). Source-side, read-only.

    Count is the scan-free ``information_schema`` estimate (is_estimate=True) unless
    ``exact`` is set, in which case it is an exact ``COUNT(*)``. ``MAX(pk)`` is an
    index-only backward seek (not a scan).
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_rows FROM information_schema.tables "
            "WHERE table_schema=%s AND table_name=%s", (schema, table))
        row = cur.fetchone()
        if row is None:
            return (False, None, False, None)
        est = int(row[0]) if row[0] is not None else None
        if exact:
            cur.execute(f"SELECT COUNT(*) FROM `{schema}`.`{table}`")
            count, is_est = int(cur.fetchone()[0]), False
        else:
            count, is_est = est, True
        # MAX(pk) only for a known single-column PK; composite/keyless -> no high-water.
        mx = None
        if pk:
            cur.execute(f"SELECT MAX(`{pk}`) FROM `{schema}`.`{table}`")
            mx = cur.fetchone()[0]
    return (True, count, is_est, mx)


def _resolve_target_schema(cur, schema: str, table: str, pk: str = ""):
    """Resolve the actual target schema (qualified, else historical 'public').

    When ``pk`` is given, only a schema whose ``table`` actually has that PK column
    qualifies -- so an unrelated same-named table (different shape) in another
    schema is never matched. Prefers the qualified schema over ``public``.
    """
    cur.execute(
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name=%s AND table_schema IN (%s, 'public') "
        "ORDER BY (table_schema=%s) DESC", (table, schema, schema))
    candidates = [r[0] for r in cur.fetchall()]
    if not pk:
        return candidates[0] if candidates else None
    for sch in candidates:
        cur.execute(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s AND column_name=%s",
            (sch, table, pk))
        if cur.fetchone():
            return sch
    return None


def target_count_and_max(conn, schema: str, table: str, pk: str):
    """Return (exists, target_schema, count, max_pk). Target exact COUNT (system under test).

    Resolves the target schema by also confirming the PK column exists there, so an
    unrelated same-named table in another schema (e.g. a leftover ``public.orders``
    with a different shape) is not mistaken for the migrated table.
    """
    cur = conn.cursor()
    sch = _resolve_target_schema(cur, schema, table, pk)
    if sch is None:
        return (False, None, None, None)
    cur.execute(f'SELECT COUNT(*) FROM "{sch}"."{table}"')
    count = int(cur.fetchone()[0])
    mx = None
    if pk:
        cur.execute(f'SELECT MAX("{pk}") FROM "{sch}"."{table}"')
        mx = cur.fetchone()[0]
    return (True, sch, count, mx)


def sample_missing_pks(src_conn, tgt_conn, schema: str, tgt_schema: str,
                       table: str, pk: str, sample: int) -> list:
    """Return up to ``sample`` source PKs that are NOT present on the target.

    Bounded: reads at most ``sample`` source PKs (the most recent, ``ORDER BY pk
    DESC LIMIT N`` -- index-only, no full scan) and probes the target with a single
    ``WHERE pk IN (...)``. This is a SAMPLE for diagnosis, not a proof of full
    consistency (that is Step 4 Validation). PK values only -- never row values.
    """
    if sample <= 0 or not pk:
        return []
    with src_conn.cursor() as cur:
        cur.execute(
            f"SELECT `{pk}` FROM `{schema}`.`{table}` ORDER BY `{pk}` DESC LIMIT %s",
            (sample,))
        src_pks = [_norm_pk(r[0]) for r in cur.fetchall()]
    src_pks = [p for p in src_pks if p is not None]
    if not src_pks:
        return []
    tcur = tgt_conn.cursor()
    placeholders = ",".join(["%s"] * len(src_pks))
    tcur.execute(
        f'SELECT "{pk}" FROM "{tgt_schema}"."{table}" WHERE "{pk}" IN ({placeholders})',
        src_pks)
    present = {_norm_pk(r[0]) for r in tcur.fetchall()}
    return [p for p in src_pks if p not in present]


# --------------------------------------------------------------------------- #
# Classify + report
# --------------------------------------------------------------------------- #


def _status(count_delta, max_delta, missing_sample, sampled):
    """Classify a table: IN SYNC / BEHIND / GAP / UNKNOWN.

    Drift is judged on MAX(pk) + the sampled PK diff (deterministic), NEVER on the
    approximate-vs-exact count delta alone (the source count may be an estimate).
    """
    if max_delta is None:
        return "UNKNOWN"  # composite/non-int PK or unreadable max
    if max_delta > 0:
        return "BEHIND"   # target's newest pk lags -> tail still in flight
    # Newest pks match (max_delta <= 0). A nonzero count delta or named missing
    # PKs => a mid-stream row is gone (replication skipped/failed).
    if missing_sample:
        return "GAP"
    if count_delta not in (None, 0) and sampled:
        # Counts differ but the sample found nothing missing -> likely estimate
        # noise (source count was approximate), not a proven gap.
        return "IN SYNC?"
    if count_delta in (None, 0):
        return "IN SYNC"
    return "IN SYNC?"


def check_once(tables, *, exact: bool, sample: int, fmt: str) -> bool:
    """One pass over all tables. Returns True iff every table is IN SYNC."""
    src = source_connect()
    tgt = target_connect()
    all_sync = True
    records = []
    try:
        for schema, table, pk in tables:
            label = f"{schema}.{table}"
            try:
                rec, table_sync = _check_table(
                    src, tgt, schema, table, pk, exact=exact, sample=sample)
            except Exception as exc:  # noqa: BLE001 - one bad table can't abort the tick
                # psycopg aborts the txn on error; reset so later tables still read.
                try:
                    tgt.rollback()
                except Exception:
                    pass
                records.append({"table": label, "status": "ERROR",
                                "error": str(exc).splitlines()[0]})
                all_sync = False
                continue
            records.append(rec)
            if not table_sync:
                all_sync = False
    finally:
        for c in (src, tgt):
            try:
                c.close()
            except Exception:
                pass

    _emit(records, fmt)
    return all_sync


def _check_table(src, tgt, schema, table, pk, *, exact, sample):
    """Compare one table; return (record dict, is_in_sync). Raises on SQL error."""
    label = f"{schema}.{table}"
    s_exists, s_count, s_est, s_max = source_estimate_and_max(
        src, schema, table, pk, exact)
    if not s_exists:
        return ({"table": label, "status": "SOURCE MISSING"}, False)
    t_exists, t_sch, t_count, t_max = target_count_and_max(tgt, schema, table, pk)
    if not t_exists:
        return ({"table": label, "status": "TARGET MISSING",
                 "source_count": s_count}, False)
    count_delta = (s_count - t_count) if (s_count is not None and t_count is not None) else None
    max_delta = None
    if s_max is not None and t_max is not None:
        try:
            max_delta = int(s_max) - int(t_max)
        except (TypeError, ValueError):
            max_delta = None
    missing = []
    if (count_delta not in (None, 0)) or (max_delta not in (None, 0)):
        try:
            missing = sample_missing_pks(src, tgt, schema, t_sch, table, pk, sample)
        except Exception as exc:  # noqa: BLE001 - keep the tick alive
            missing = []
            log(f"  {label}: sample probe error: {str(exc).splitlines()[0]}")
    status = _status(count_delta, max_delta, missing, sample > 0)
    record = {
        "table": label, "status": status,
        "source_count": s_count, "source_estimate": s_est,
        "target_count": t_count, "count_delta": count_delta,
        "source_max_pk": _norm_pk(s_max), "target_max_pk": _norm_pk(t_max),
        "max_delta": max_delta,
        "target_schema": t_sch if t_sch != schema else None,
        "missing_pks_sample": missing[:20],
        "missing_pks_more": max(0, len(missing) - 20),
    }
    return (record, status == "IN SYNC")


def _emit(records, fmt: str) -> None:
    if fmt == "json":
        print(json.dumps({"ts": dt.datetime.now().isoformat(), "tables": records}), flush=True)
        return
    if fmt == "csv":
        for r in records:
            print(",".join(str(r.get(k, "")) for k in (
                "table", "status", "source_count", "target_count", "count_delta",
                "source_max_pk", "target_max_pk", "max_delta")), flush=True)
        return
    # human
    print(f"{'TABLE':<36} {'SOURCE':>11} {'TARGET':>11} {'MAXΔ':>6}  STATUS", flush=True)
    print("-" * 84, flush=True)
    drift = 0
    for r in records:
        st = r["status"]
        if st in ("SOURCE MISSING", "TARGET MISSING"):
            print(f"{r['table']:<36} {'':>11} {'':>11} {'':>6}  {st}", flush=True)
            drift += 1
            continue
        src_txt = ("~" if r.get("source_estimate") else "") + str(r.get("source_count"))
        maxd = r.get("max_delta")
        maxd_txt = f"{maxd:+d}" if isinstance(maxd, int) else "?"
        sch_note = f"  [target schema: {r['target_schema']}]" if r.get("target_schema") else ""
        line = f"{r['table']:<36} {src_txt:>11} {str(r.get('target_count')):>11} {maxd_txt:>6}  {st}{sch_note}"
        print(line, flush=True)
        if st != "IN SYNC":
            drift += 1
        if r.get("missing_pks_sample"):
            more = f" (+{r['missing_pks_more']} more)" if r.get("missing_pks_more") else ""
            print(f"{'':<36} missing on target [{r['table'].split('.')[-1]} pk]: "
                  f"{', '.join(r['missing_pks_sample'])}{more}", flush=True)
    print("-" * 84, flush=True)
    print("VERDICT: ALL IN SYNC" if drift == 0 else f"VERDICT: DRIFT ({drift} table(s))", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description="Monitor source vs target row consistency.")
    ap.add_argument("--schema", default=SCHEMA)
    ap.add_argument("-t", "--table", action="append", dest="tables", metavar="SCHEMA.TABLE",
                    help="Table to check (repeatable). PK auto-detected. Default: the workload's tables.")
    ap.add_argument("--interval", type=float, default=10.0, help="seconds between checks")
    ap.add_argument("--once", action="store_true", help="single check then exit")
    ap.add_argument("--exact-count", action="store_true",
                    help="exact source COUNT(*) instead of the scan-free estimate (heavier)")
    ap.add_argument("--sample", type=int, default=200,
                    help="bounded PK-sample size for the missing-PK diff (0 disables)")
    ap.add_argument("--format", choices=("human", "json", "csv"), default="human")
    args = ap.parse_args()

    # Resolve (schema, table, pk). For --table args the PK is looked up live on the source.
    if args.tables:
        tables = _resolve_pks(args.tables, args.schema)
    else:
        # Resolve the default table set from --schema at RUNTIME (not the import-time
        # _DEFAULT_TABLES, which is keyed to the CDC_WORKLOAD_SCHEMA env default): so
        # `--schema migration_typetest` switches the table set even when the env var
        # is unset. Falls back to the env-default set if --schema isn't a known schema.
        try:
            default_pks = table_pks_for(args.schema)
        except KeyError:
            log(f"WARNING: --schema {args.schema!r} not in the table registry "
                f"(scripts/_e2e_tables.py); using the env-default set instead.")
            default_pks = list(_DEFAULT_TABLES)
        tables = [(args.schema, t, pk) for t, pk in default_pks]

    log(f"Monitor -> source {HOST}:{PORT} vs DSQL {TARGET_ENDPOINT} | schema={args.schema} "
        f"tables={len(tables)} sample={args.sample} "
        f"{'exact-count' if args.exact_count else 'estimate(~)'} interval={args.interval}s")
    if args.once:
        return 0 if check_once(tables, exact=args.exact_count, sample=args.sample, fmt=args.format) else 1
    try:
        while True:
            check_once(tables, exact=args.exact_count, sample=args.sample, fmt=args.format)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        log("Stopped.")
    return 0


def _resolve_pks(table_args, default_schema):
    """Look up the single-column PK for each --table arg on the source (read-only)."""
    conn = source_connect()
    resolved = []
    try:
        for arg in table_args:
            schema, name = (arg.split(".", 1) if "." in arg else (default_schema, arg))
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.key_column_usage "
                    "WHERE table_schema=%s AND table_name=%s AND constraint_name='PRIMARY' "
                    "ORDER BY ordinal_position", (schema, name))
                pks = [r[0] for r in cur.fetchall()]
            if len(pks) != 1:
                log(f"  WARN {schema}.{name}: PK is {pks or 'missing'} (need single-column); "
                    "max/gap will be UNKNOWN.")
            resolved.append((schema, name, pks[0] if pks else None))
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return resolved


if __name__ == "__main__":
    raise SystemExit(main())
