#!/usr/bin/env python3
"""Run a Full Load from the command line, for YOUR own schema and tables.

A headless way to drive the tool's **own** Full Load engine (the same bulk loader
the web UI uses — it re-implements nothing) without opening the browser. Point it
at your source MySQL and target Aurora DSQL, pick the tables, and it streams each
table from the source and batch-loads it into DSQL.

What it does, faithfully reusing the shipped engine:
  1. Introspects the source MySQL schema (read-only) into an inventory.
  2. Selects the tables you name (or ALL tables in the schema if you name none),
     qualified as "<schema>"."<table>" so they land in a matching DSQL schema.
  3. Runs Full Load: keyset-streamed export -> bounded-parallel, idempotent
     ``INSERT ... ON CONFLICT`` batches with OCC retry (memory stays bounded to
     one page per table regardless of size; TB-scale safe).
  4. Captures the binlog/GTID watermark and (optionally) writes it to a file, so
     you can start CDC gaplessly from exactly that point afterwards.

Two load modes:
  * default (idempotent): inserts only the rows not already on the target
    (``INSERT ... ON CONFLICT DO NOTHING``). Safe to re-run; never duplicates.
    Target tables must already exist (create them via the tool's Schema
    Conversion step, or with --clean below).
  * ``--clean``: DROP + recreate each target table from the tool's converted DDL
    before loading (DSQL has no TRUNCATE). ⚠️ DESTRUCTIVE — it discards existing
    target data for those tables. Use for a fresh load.

The source is only ever READ. Nothing runs until you pass ``--yes``; without it
the script prints the plan and exits.

Connection settings come from the environment / ``.env`` (copy ``.env.example``):
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD                 (source MySQL, read-only)
  TARGET_ENDPOINT, TARGET_REGION, TARGET_DATABASE, TARGET_USERNAME  (Aurora DSQL)
The DSQL side authenticates with a short-lived IAM token (no password); your AWS
credentials must be able to connect to the cluster.

Usage (from the repo root):
    set -a; source .env; set +a

    # Plan only (no writes) — see what WOULD load
    .venv/bin/python scripts/run_full_load.py --schema sales

    # Load specific tables (idempotent; target tables must exist)
    .venv/bin/python scripts/run_full_load.py --schema sales --tables orders customers --yes

    # Fresh load of ALL tables in the schema (DROP+recreate target), save watermark
    .venv/bin/python scripts/run_full_load.py --schema sales --clean --yes \
        --watermark-out sales_watermark.json

Exit code: 0 when every selected table loaded; non-zero if any table failed or a
row was quarantined (permanently rejected, e.g. a value over DSQL's ~1 MiB limit)
-- so you can gate a shell script on it. Then verify with
``scripts/compare_rows.py`` / ``scripts/cdc_consistency_check.py``.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))


def _env(path: str) -> dict:
    """Minimal KEY=VALUE parser for a .env file (no external dependency)."""
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


def region(endpoint: str) -> str:
    # DSQL endpoint: <id>.dsql.<region>.on.aws
    parts = endpoint.split(".")
    return cfg("TARGET_REGION") or (parts[2] if len(parts) > 2 else "us-east-1")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--schema", required=True,
                    help="source MySQL database/schema to load from")
    ap.add_argument("--tables", nargs="*", default=[],
                    help="table names within --schema to load (default: ALL tables)")
    ap.add_argument("--clean", action="store_true",
                    help="DROP+recreate each target table before loading (DESTRUCTIVE)")
    ap.add_argument("--yes", action="store_true",
                    help="actually run (without it, prints the plan and exits)")
    ap.add_argument("--watermark-out", default="",
                    help="write the captured binlog/GTID watermark to this JSON file")
    ap.add_argument("--timeout", type=float, default=7200.0,
                    help="max seconds to wait for the load (default 7200)")
    args = ap.parse_args()

    host, pwd, endpoint = cfg("DB_HOST"), cfg("DB_PASSWORD"), cfg("TARGET_ENDPOINT")
    if not host or not pwd or not endpoint:
        print("ERROR: set DB_HOST / DB_PASSWORD / TARGET_ENDPOINT in .env "
              "(`set -a; source .env; set +a`).", file=sys.stderr)
        return 2

    # Imported here so --help works without the package installed.
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.models import (
        SourceConnectionConfig, TargetConnectionConfig,
    )
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    from dsql_migrator.ui.data_migration._engine import (
        DataMigrationInputs, default_migrator_factory, run_full_load,
    )
    from dsql_migrator.ui.evaluation import _default_introspector_factory

    schema = args.schema
    source = SourceConnectionConfig(
        host=host, port=int(cfg("DB_PORT", "3306")),
        database=schema, username=cfg("DB_USER", "admin"),
    )
    target = TargetConnectionConfig(
        cluster_endpoint=endpoint, region=region(endpoint),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    password = SecretValue(pwd)

    log(f"Introspecting source {host} schema={schema} (read-only) ...")
    try:
        inventory = _default_introspector_factory(password).introspect(source)
    except Exception as exc:  # noqa: BLE001 - surface a clean message, not a traceback
        print(f"ERROR: could not read the source schema '{schema}' at {host}: "
              f"{str(exc).splitlines()[0]}\n"
              "Check DB_HOST/DB_PORT/DB_USER/DB_PASSWORD in .env and that the schema "
              "exists and is reachable (read-only).", file=sys.stderr)
        return 2

    # Single-database introspection returns UNQUALIFIED names, which would land in
    # the target's ``public`` schema. Qualify each selected table as
    # "<schema>.<table>" in place so the converter emits a matching CREATE SCHEMA +
    # schema-qualified DDL, the exporter reads `schema`.`table`, and the importer
    # upserts into the qualified target -- consistent with how CDC writes, too.
    present = {t.name: t for t in inventory.tables}
    want_names = args.tables or [t.name for t in inventory.tables]
    wanted: list[str] = []
    missing: list[str] = []
    for name in want_names:
        qualified = f"{schema}.{name}"
        if qualified in present:
            wanted.append(qualified)
        elif name in present:
            present[name].name = qualified  # qualify in place
            wanted.append(qualified)
        else:
            missing.append(name)
    if missing:
        print(f"ERROR: tables not found in source schema '{schema}': {missing}\n"
              f"Available: {sorted(t.split('.')[-1] for t in present)}", file=sys.stderr)
        return 2
    if not wanted:
        print(f"ERROR: no tables found in source schema '{schema}'.", file=sys.stderr)
        return 2

    tables = TableSelector().resolve(inventory, TableSelection(selected_tables=wanted))

    log(f"Plan: Full Load {len(tables)} table(s) into {endpoint}")
    log(f"  schema:  {schema}")
    log(f"  tables:  {[t.split('.')[-1] for t in wanted]}")
    log(f"  clean (DROP+recreate target before load): {args.clean}")
    if args.watermark_out:
        log(f"  watermark -> {args.watermark_out}")
    if not args.yes:
        log("[plan only] re-run with --yes to execute"
            + (" (DESTRUCTIVE: --clean DROPs+recreates the target tables)."
               if args.clean else "."))
        return 0

    inputs = DataMigrationInputs(
        source_config=source, source_password=password, target_config=target,
        inventory=inventory, aws_profile=os.environ.get("AWS_PROFILE"),
        replace_tables=frozenset(wanted) if args.clean else frozenset(),
    )
    migrator = default_migrator_factory(inputs)
    error_log = ErrorLogStore()
    jm = JobManager(stall_timeout_seconds=None)

    def work(handle) -> None:
        run_full_load(handle, tables, migrator=migrator, error_log=error_log)

    log("Starting Full Load ...")
    job_id = jm.submit(work)
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        job = jm.get_status(job_id)
        done = sum(1 for c in job.chunks if c.status in ("DONE", "FAILED"))
        if job.status in ("DONE", "FAILED", "CANCELLED"):
            break
        cur = next((c.chunk_id for c in job.chunks if c.status == "IN_PROGRESS"), "")
        log(f"  [{job.status}] {done}/{len(job.chunks)} settled "
            f"{('· ' + cur.split('.')[-1]) if cur else ''}")
        time.sleep(5)

    job = jm.get_status(job_id)
    err = jm.get_error(job_id)
    summary = error_log.summary(job_id)
    log(f"Final status: {job.status}; per-table errors: {summary.total_errors}")
    for c in job.chunks:
        log(f"  {c.chunk_id.split('.')[-1]:<32} {c.status:<10} rows={c.rows_loaded}")
    if err:
        log(f"  job error: {err.splitlines()[0]}")

    wm = job.watermark
    if wm is not None and args.watermark_out:
        out = {
            "binlog_file": wm.binlog_file,
            "binlog_position": wm.binlog_position,
            "gtid_executed": wm.gtid_executed,
            "server_uuid": wm.server_uuid,
            "snapshot_timestamp": wm.snapshot_timestamp.isoformat(),
        }
        json.dump(out, open(args.watermark_out, "w", encoding="utf-8"), indent=2)
        log(f"Watermark written to {args.watermark_out} "
            f"({wm.binlog_file}:{wm.binlog_position})")

    if job.status == "DONE":
        log("Full Load complete. Verify with scripts/compare_rows.py or "
            "scripts/cdc_consistency_check.py.")
        return 0
    log("Full Load did NOT complete cleanly — see the per-table status and job "
        "error above. Fix the cause and re-run (the idempotent load fills only the "
        "gap; a re-run never duplicates).")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
