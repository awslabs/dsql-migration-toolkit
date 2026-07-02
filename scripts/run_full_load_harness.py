#!/usr/bin/env python3
"""Headless Full Load harness for the gapless-handoff verification (step 1-3).

Reuses the tool's OWN engine (no re-implementation): builds source/target configs
from .env, introspects the source, and runs ``run_full_load`` over the 11
``customers_sample_new`` tables. With ``--clean`` (default) the target tables are
DROPped + recreated from converted DDL before loading (DSQL has no TRUNCATE) for a
clean slate. The export watermark captured by the engine is written to a JSON file
so the in-VPC seeder (``scripts/seed_cdc_offset.py``) can start CDC gaplessly.

Connection settings from .env: DB_HOST/DB_PORT/DB_USER/DB_PASSWORD (source),
TARGET_ENDPOINT/TARGET_REGION/TARGET_DATABASE/TARGET_USERNAME (DSQL).

⚠️ DESTRUCTIVE with --clean: DROPs + recreates the 11 target tables. Requires
--yes to actually run; without it, prints the plan and exits.

Usage (from repo root):
    set -a; source .env; set +a
    .venv/bin/python scripts/run_full_load_harness.py                 # plan only
    .venv/bin/python scripts/run_full_load_harness.py --yes --watermark-out wm.json
    .venv/bin/python scripts/run_full_load_harness.py --yes --no-clean # load without DROP
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _e2e_tables

from _e2e_tables import tables_for  # noqa: E402
from dsql_migrator.config import SecretValue  # noqa: E402
from dsql_migrator.core.error_log import ErrorLogStore  # noqa: E402
from dsql_migrator.core.job_manager import JobManager  # noqa: E402
from dsql_migrator.core.models import (  # noqa: E402
    SourceConnectionConfig, TargetConnectionConfig,
)
from dsql_migrator.core.table_selection import TableSelection, TableSelector  # noqa: E402
from dsql_migrator.ui.data_migration._engine import (  # noqa: E402
    DataMigrationInputs, default_migrator_factory, run_full_load,
)
from dsql_migrator.ui.evaluation import _default_introspector_factory  # noqa: E402

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMA = os.environ.get("CDC_WORKLOAD_SCHEMA", "customers_sample_new")
TABLES = tables_for(SCHEMA)  # single source of truth: scripts/_e2e_tables.py


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


def _cfg(env: dict, key: str, default: str = "") -> str:
    return os.environ.get(key) or env.get(key) or default


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watermark-out", default="wm.json")
    ap.add_argument("--clean", dest="clean", action="store_true", default=True,
                    help="DROP+recreate target tables before load (default)")
    ap.add_argument("--no-clean", dest="clean", action="store_false")
    ap.add_argument("--yes", action="store_true", help="actually run (destructive)")
    ap.add_argument("--timeout", type=float, default=3600.0)
    args = ap.parse_args()

    env = _env(os.path.join(_ROOT, ".env"))
    pwd = _cfg(env, "DB_PASSWORD")
    source = SourceConnectionConfig(
        host=_cfg(env, "DB_HOST"), port=int(_cfg(env, "DB_PORT", "3306")),
        database=SCHEMA, username=_cfg(env, "DB_USER", "admin"),
    )
    target = TargetConnectionConfig(
        cluster_endpoint=_cfg(env, "TARGET_ENDPOINT"),
        region=_cfg(env, "TARGET_REGION", "us-east-1"),
        database=_cfg(env, "TARGET_DATABASE", "postgres"),
        username=_cfg(env, "TARGET_USERNAME", "admin"),
    )
    if not source.host or not pwd or not target.cluster_endpoint:
        print("ERROR: set DB_HOST/DB_PASSWORD/TARGET_ENDPOINT in .env", file=sys.stderr)
        return 2
    password = SecretValue(pwd)

    print(f"Introspecting source {source.host} schema={SCHEMA} ...")
    inventory = _default_introspector_factory(password).introspect(source)

    # Land Full Load in the SAME PostgreSQL schema the CDC sink writes to. The
    # Debezium source emits source.db ("customers_sample_new") as the schema, so
    # the sink upserts into "customers_sample_new"."orders". Single-database
    # introspection returns UNQUALIFIED names (-> they would land in `public`),
    # which would split Full Load and CDC across two schemas and break the gapless
    # handoff + validation. So qualify the selected tables as "<SCHEMA>.<table>"
    # in place: the converter then emits a matching CREATE SCHEMA + schema-
    # qualified DDL, the exporter quotes the source SELECT as `schema`.`table`,
    # and the importer upserts into the qualified target -- all consistent with CDC.
    present = {t.name: t for t in inventory.tables}
    wanted: list[str] = []
    missing: list[str] = []
    for t in TABLES:
        qualified = f"{SCHEMA}.{t}"
        if qualified in present:
            wanted.append(qualified)
        elif t in present:
            present[t].name = qualified  # qualify in place -> schema-qualified target
            wanted.append(qualified)
        else:
            missing.append(t)
    if missing:
        print(f"ERROR: tables not found in source inventory: {missing}", file=sys.stderr)
        return 2
    tables = TableSelector().resolve(inventory, TableSelection(selected_tables=wanted))

    replace = frozenset(wanted) if args.clean else frozenset()
    print(f"Plan: Full Load {len(tables)} tables into {target.cluster_endpoint}")
    print(f"  clean (DROP+recreate before load): {bool(replace)}")
    print(f"  tables: {wanted}")
    print(f"  watermark -> {args.watermark_out}")
    if not args.yes:
        print("\n[plan only] re-run with --yes to execute (DESTRUCTIVE with --clean).")
        return 0

    inputs = DataMigrationInputs(
        source_config=source, source_password=password, target_config=target,
        inventory=inventory, aws_profile=os.environ.get("AWS_PROFILE"),
        replace_tables=replace,
    )
    migrator = default_migrator_factory(inputs)
    error_log = ErrorLogStore()
    jm = JobManager()

    def work(handle) -> None:
        run_full_load(handle, tables, migrator=migrator, error_log=error_log)

    print("\nStarting Full Load ...")
    job_id = jm.submit(work)
    deadline = time.time() + args.timeout
    status = None
    while time.time() < deadline:
        job = jm.get_status(job_id)
        status = job.status
        done = sum(1 for c in job.chunks if c.status == "DONE")
        cur = next((c.chunk_id for c in job.chunks if c.status == "IN_PROGRESS"), "")
        print(f"  [{status}] {done}/{len(job.chunks)} done {('· '+cur) if cur else ''}")
        if status in ("DONE", "FAILED", "CANCELLED"):
            break
        time.sleep(5)

    job = jm.get_status(job_id)
    summary = error_log.summary(job_id)
    print(f"\nFinal status: {job.status}; errors: {summary.total_errors}")
    for c in job.chunks:
        print(f"  {c.chunk_id:<40} {c.status:<12} rows={c.rows_loaded}")

    wm = job.watermark
    if wm is None:
        print("ERROR: no watermark captured.", file=sys.stderr)
        return 1
    out = {
        "binlog_file": wm.binlog_file,
        "binlog_position": wm.binlog_position,
        "gtid_executed": wm.gtid_executed,
        "server_uuid": wm.server_uuid,
        "snapshot_timestamp": wm.snapshot_timestamp.isoformat(),
    }
    json.dump(out, open(args.watermark_out, "w", encoding="utf-8"), indent=2)
    print(f"\nWatermark written to {args.watermark_out}:")
    print(json.dumps(out, indent=2))
    return 0 if job.status == "DONE" else 1


if __name__ == "__main__":
    raise SystemExit(main())
