#!/usr/bin/env python3
"""Kill-and-resume harness: prove Full Load is a resumable, idempotent unit.

The tool's core promise (CLAUDE.md "Resumable, deterministic units"; docs §8
"Resumability") is that an interrupted Full Load loses and duplicates NOTHING: a
stop/retry re-runs only the unfinished PK ranges and converges to exactly the
uninterrupted state. The offline suite proves this deterministically with fakes;
this harness proves it on REAL infrastructure (RDS MySQL + Aurora DSQL), by
driving the tool's OWN engine -- ``run_full_load`` then ``run_full_load_retry`` on a
real :class:`JobManager` -- exactly as the UI's "Stop" + "Retry failed tables"
buttons do. It re-implements nothing.

Flow (each stage is idempotent; ``--only`` runs one in isolation):

  schema   Create the target tables EMPTY from converted DDL (Step 3 semantics via
           ``recreate_table``: DROP+CREATE, DSQL has no TRUNCATE), a clean slate so
           the load below has somewhere to write and the run is repeatable.
  interrupt Run the tool's Full Load into those empty tables (idempotent
           SKIP_EXISTING, the same mode the UI uses when tables pre-exist) on a
           background JobManager thread, then request a COOPERATIVE cancel
           (``request_cancel``) once the load has made partial progress
           (``--interrupt-after-rows``). In-flight tables stop mid-load and are
           marked FAILED-for-retry; already-done tables stay DONE -- the real
           partial state a Stop produces.
  resume   Carry the interrupted job's chunks forward and ``run_full_load_retry``
           ONLY the unfinished tables (idempotent, reusing the original watermark),
           exactly like "Retry failed tables". Because the load is SKIP_EXISTING,
           the resume inserts only the PKs the interrupt missed -- never a duplicate.
  verify   Run the tool's Validator (ROW_COUNT + per-PK reconcile) over every
           table and assert 0 missing / 0 extra vs the source: the resumed target
           equals the uninterrupted one. (Also flags any duplicate as extra.)

IMPORTANT: run against a STATIC source (no concurrent workload) so "converged to
the uninterrupted state" is a clean equality; a live workload would move the
source mid-run and the reconcile would report expected drift, not a bug. Schema
and table set come from ``scripts/_e2e_tables.py`` (``--schema`` /
CDC_WORKLOAD_SCHEMA); use a schema with enough rows that the interrupt lands
mid-load (a tiny schema may finish first -- the harness then reports that and the
resume is a valid no-op idempotency check).

Prerequisites & connection: identical to run_full_load_harness.py -- ``.env`` with
DB_HOST/DB_PORT/DB_USER/DB_PASSWORD (source) and TARGET_ENDPOINT/TARGET_REGION/
TARGET_DATABASE/TARGET_USERNAME (DSQL), plus AWS creds able to reach DSQL.

⚠️ DESTRUCTIVE: the schema stage DROPs+recreates the target tables. Requires
--yes; without it prints the plan and exits.

Usage (from repo root):
    set -a; source .env; set +a
    .venv/bin/python scripts/run_fullload_resume_harness.py                 # plan
    .venv/bin/python scripts/run_fullload_resume_harness.py --yes
    .venv/bin/python scripts/run_fullload_resume_harness.py --yes \
        --schema customers_sample_new --interrupt-after-rows 500

Exit code: 0 when the resumed target reconciles exactly (0 missing / 0 extra on
every table); non-zero otherwise -- so it can gate a test.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _e2e_tables

from _e2e_tables import tables_for  # noqa: E402

SCHEMA = os.environ.get("CDC_WORKLOAD_SCHEMA", "customers_sample_new")
WATERMARK_FILE = os.path.join(_ROOT, "resume_watermark.json")
STAGES = ("schema", "interrupt", "resume", "verify")


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


# --------------------------------------------------------------------------- #
# Shared engine wiring (reuses the tool's own components; mirrors the UI)
# --------------------------------------------------------------------------- #
def _configs():
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import (
        SourceConnectionConfig, TargetConnectionConfig,
    )
    source = SourceConnectionConfig(
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "3306")),
        database=SCHEMA, username=cfg("DB_USER", "admin"),
    )
    target = TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"), region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    return source, target, SecretValue(cfg("DB_PASSWORD"))


def _introspect_and_qualify(source, password):
    """Introspect the source and qualify the table set as "<SCHEMA>.<table>".

    Same qualification the Full Load harness / verifier use so Full Load, the
    empty-table create, and Validation all agree on the schema-qualified target.
    Returns (inventory, resolved TableDefs, qualified name list).
    """
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    from dsql_migrator.ui.evaluation import _default_introspector_factory

    inventory = _default_introspector_factory(password).introspect(source)
    present = {t.name: t for t in inventory.tables}
    wanted: list[str] = []
    missing: list[str] = []
    for t in tables_for(SCHEMA):
        qualified = f"{SCHEMA}.{t}"
        if qualified in present:
            wanted.append(qualified)
        elif t in present:
            present[t].name = qualified
            wanted.append(qualified)
        else:
            missing.append(t)
    if missing:
        raise SystemExit(f"ERROR: tables not found in source inventory: {missing}")
    tables = TableSelector().resolve(inventory, TableSelection(selected_tables=wanted))
    return inventory, tables, wanted


def _inputs(source, target, password, inventory, *, replace):
    """Build DataMigrationInputs mirroring the UI (empty replace -> SKIP_EXISTING)."""
    from dsql_migrator.ui.data_migration._engine import DataMigrationInputs
    return DataMigrationInputs(
        source_config=source, source_password=password, target_config=target,
        inventory=inventory, aws_profile=os.environ.get("AWS_PROFILE"),
        replace_tables=frozenset(replace),
    )


# --------------------------------------------------------------------------- #
# Stage: schema -- create the target tables EMPTY (clean slate, Step 3 semantics)
# --------------------------------------------------------------------------- #
def stage_schema(args) -> int:
    if not args.yes:
        log(f"[plan] would DROP+recreate the {len(tables_for(SCHEMA))} target tables "
            f"for schema '{SCHEMA}' EMPTY (clean slate). Use --yes.")
        return 0
    from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions
    from dsql_migrator.core.schema_applier import recreate_table
    from dsql_migrator.core.target_connection import DsqlConnector

    source, target, password = _configs()
    _inv, tables, wanted = _introspect_and_qualify(source, password)
    connector = DsqlConnector(target, aws_profile=os.environ.get("AWS_PROFILE"))
    converter = SchemaConverter()
    for table in tables:
        conv = converter.convert_table(table, SchemaConvertOptions())
        log(f"  recreate empty: {table.name}")
        # DROP+CREATE only (no data, no indexes yet -- exactly the engine's
        # _default_table_recreator does before a fresh load).
        recreate_table(conv.schema_ddls, conv.target_ddl,
                        connection_factory=connector.connect)
    log(f"Created {len(tables)} empty target table(s). ✓")
    return 0


# --------------------------------------------------------------------------- #
# Stage: interrupt -- run Full Load, cancel once partial progress is made
# --------------------------------------------------------------------------- #
def _submit_full_load(jm, tables, inputs, error_log, *, retry=False,
                      prior_chunks=None, watermark=None):
    """Submit run_full_load (or run_full_load_retry) and return the job id."""
    from dsql_migrator.ui.data_migration._engine import (
        default_migrator_factory, run_full_load, run_full_load_retry,
    )
    migrator = default_migrator_factory(inputs)

    if retry:
        def work(handle) -> None:
            run_full_load_retry(
                handle, prior_chunks, tables,
                migrator=migrator, error_log=error_log, watermark=watermark,
            )
    else:
        def work(handle) -> None:
            run_full_load(handle, tables, migrator=migrator, error_log=error_log)
    return jm.submit(work)


def _rows_loaded(job) -> int:
    return sum(int(c.rows_loaded or 0) for c in job.chunks)


def _persist_watermark(job) -> None:
    wm = job.watermark
    if wm is None:
        return
    out = {
        "binlog_file": wm.binlog_file, "binlog_position": wm.binlog_position,
        "gtid_executed": wm.gtid_executed, "server_uuid": wm.server_uuid,
        "snapshot_timestamp": wm.snapshot_timestamp.isoformat(),
    }
    json.dump(out, open(WATERMARK_FILE, "w", encoding="utf-8"), indent=2)


def stage_interrupt(args) -> int:
    if not args.yes:
        log(f"[plan] would run Full Load into the empty tables and request a "
            f"cooperative cancel after ~{args.interrupt_after_rows} rows land "
            "(simulating the UI 'Stop'). Use --yes.")
        return 0
    # Pin table parallelism low so the interrupt lands cleanly mid-load (a table
    # finishing while another is still streaming), and so the partial state is
    # legible. load_config() reads os.environ fresh, so setting it here takes effect.
    os.environ["DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM"] = str(args.table_parallelism)

    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager

    source, target, password = _configs()
    inventory, tables, wanted = _introspect_and_qualify(source, password)
    inputs = _inputs(source, target, password, inventory, replace=[])  # SKIP_EXISTING
    jm = JobManager(stall_timeout_seconds=None)
    error_log = ErrorLogStore()
    job_id = _submit_full_load(jm, tables, inputs, error_log)

    log(f"Full Load submitted (job {job_id}); waiting to interrupt after "
        f"~{args.interrupt_after_rows} rows ...")
    cancelled = False
    deadline = time.time() + 3600.0
    while time.time() < deadline:
        job = jm.get_status(job_id)
        if job.status in ("DONE", "FAILED", "CANCELLED"):
            break
        loaded = _rows_loaded(job)
        done = sum(1 for c in job.chunks if c.status == "DONE")
        if not cancelled and job.watermark is not None and (
            loaded >= args.interrupt_after_rows or done >= max(1, len(job.chunks) // 2)
        ):
            log(f"  interrupting now: {loaded} rows loaded, {done}/{len(job.chunks)} "
                "tables done -> request_cancel (cooperative stop)")
            jm.request_cancel(job_id)
            cancelled = True
        time.sleep(0.5)
    jm.wait(job_id, timeout=120)
    job = jm.get_status(job_id)
    _persist_watermark(job)

    done = [c.chunk_id for c in job.chunks if c.status == "DONE"]
    unfinished = [c.chunk_id for c in job.chunks if c.status != "DONE"]
    log(f"Interrupted run status={job.status}; rows_loaded={_rows_loaded(job)}")
    log(f"  DONE tables:       {done}")
    log(f"  unfinished tables: {unfinished}")
    # Persist the partial chunk snapshot so the resume stage can carry it forward
    # exactly like the UI's live job does (the harness resume runs in a new process
    # only if --only is used; otherwise it is handed the in-memory job below).
    _save_partial(job)
    if not cancelled:
        log("  NOTE: the load finished before the interrupt could fire (schema too "
            "small / threshold too high). Resume will be a no-op idempotency check; "
            "raise the row count or lower --interrupt-after-rows for a real interrupt.")
    return 0


_PARTIAL_FILE = os.path.join(_ROOT, "resume_partial_chunks.json")


def _save_partial(job) -> None:
    """Persist the interrupted job's chunk states for the resume stage."""
    chunks = [{
        "chunk_id": c.chunk_id, "status": c.status,
        "rows_loaded": c.rows_loaded, "rows_skipped": getattr(c, "rows_skipped", 0),
        "attempts": c.attempts,
    } for c in job.chunks]
    json.dump({"chunks": chunks}, open(_PARTIAL_FILE, "w", encoding="utf-8"), indent=2)


def _load_partial():
    from dsql_migrator.core.models import ChunkState, Watermark
    if not os.path.exists(_PARTIAL_FILE):
        raise SystemExit(f"partial file {_PARTIAL_FILE} missing — run stage 'interrupt' first.")
    data = json.load(open(_PARTIAL_FILE, encoding="utf-8"))
    chunks = [ChunkState(**c) for c in data["chunks"]]
    wm = None
    if os.path.exists(WATERMARK_FILE):
        import datetime as _dt
        d = json.load(open(WATERMARK_FILE, encoding="utf-8"))
        wm = Watermark(
            binlog_file=d.get("binlog_file"), binlog_position=d.get("binlog_position"),
            gtid_executed=d.get("gtid_executed"), server_uuid=d.get("server_uuid"),
            snapshot_timestamp=_dt.datetime.fromisoformat(d["snapshot_timestamp"]),
        )
    return chunks, wm


# --------------------------------------------------------------------------- #
# Stage: resume -- retry ONLY the unfinished tables (idempotent), like the UI
# --------------------------------------------------------------------------- #
def stage_resume(args) -> int:
    if not args.yes:
        log("[plan] would carry the interrupted job forward and run_full_load_retry "
            "ONLY the unfinished tables (idempotent SKIP_EXISTING). Use --yes.")
        return 0
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.core.table_selection import TableSelection, TableSelector

    prior_chunks, watermark = _load_partial()
    retry_names = [c.chunk_id for c in prior_chunks if c.status != "DONE"]
    if not retry_names:
        log("No unfinished tables — the interrupted run already completed. "
            "Resume is a no-op (idempotency holds trivially).")
        return 0
    log(f"Resuming {len(retry_names)} unfinished table(s): {retry_names}")

    source, target, password = _configs()
    inventory, _all_tables, _wanted = _introspect_and_qualify(source, password)
    retry_tables = TableSelector().resolve(
        inventory, TableSelection(selected_tables=retry_names))
    inputs = _inputs(source, target, password, inventory, replace=[])  # no re-drop
    jm = JobManager(stall_timeout_seconds=None)
    error_log = ErrorLogStore()
    job_id = _submit_full_load(
        jm, retry_tables, inputs, error_log,
        retry=True, prior_chunks=prior_chunks, watermark=watermark)

    deadline = time.time() + 3600.0
    while time.time() < deadline:
        job = jm.get_status(job_id)
        if job.status in ("DONE", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    job = jm.get_status(job_id)
    err = jm.get_error(job_id)
    for c in job.chunks:
        log(f"  {c.chunk_id:<32} {c.status:<10} rows={c.rows_loaded}")
    if err:
        log(f"  job error: {err.splitlines()[0]}")
    log(f"Resume status={job.status}")
    return 0 if job.status == "DONE" else 1


# --------------------------------------------------------------------------- #
# Stage: verify -- Validator (ROW_COUNT + per-PK reconcile): 0 missing / 0 extra
# --------------------------------------------------------------------------- #
def stage_verify(args) -> int:
    if not args.yes:
        log("[plan] would run Validation (ROW_COUNT + per-PK reconcile) and assert "
            "0 missing / 0 extra on every table (converged to the uninterrupted "
            "state, no duplicates). Use --yes.")
        return 0
    from dsql_migrator.core.models import ValidationMode
    from dsql_migrator.core.validator import Validator
    from dsql_migrator.ui.connect import make_source_engine_factory

    source, target, password = _configs()
    _inv, tables, _wanted = _introspect_and_qualify(source, password)
    log("Running Validation (ROW_COUNT + per-PK reconcile) ...")
    report = Validator(
        source_engine_factory=make_source_engine_factory(password),
    ).validate(
        source, target, list(tables), ValidationMode.ROW_COUNT,
        reconcile=True, max_workers=int(cfg("DSQL_MIGRATOR_VALIDATE_MAX_WORKERS", "4")),
    )
    ok = True
    for item in report.items:
        rec = item.reconcile
        missing = rec.missing_on_target if rec else None
        extra = rec.extra_on_target if rec else None
        converged = item.matched and (rec is None or (missing == 0 and extra == 0))
        ok = ok and converged
        flag = "CONVERGED ✓" if converged else "DIVERGED ✗"
        log(f"  {item.table:<32} {flag}  src={item.source_row_count} "
            f"tgt={item.target_row_count} missing={missing} extra={extra}")
    log("VERDICT: " + ("resumed target == uninterrupted (0 loss / 0 dup) ✓"
                       if ok and report.is_match else "DIVERGENCE — see above ✗"))
    return 0 if (ok and report.is_match) else 1


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
_STAGE_FUNCS = {
    "schema": stage_schema,
    "interrupt": stage_interrupt,
    "resume": stage_resume,
    "verify": stage_verify,
}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--yes", action="store_true",
                    help="actually run (destructive schema recreate needs this)")
    ap.add_argument("--only", choices=STAGES, help="run only this one stage")
    ap.add_argument("--interrupt-after-rows", type=int, default=1000,
                    help="request the cancel once this many rows have loaded (default 1000)")
    ap.add_argument("--table-parallelism", type=int, default=1,
                    help="Full Load table parallelism during the interrupt run "
                         "(default 1 for a clean, legible mid-load interrupt)")
    args = ap.parse_args()

    if not cfg("DB_HOST") or not cfg("DB_PASSWORD") or not cfg("TARGET_ENDPOINT"):
        log("ERROR: set DB_HOST/DB_PASSWORD/TARGET_ENDPOINT in .env "
            "(`set -a; source .env; set +a`).")
        return 2

    selected = [args.only] if args.only else list(STAGES)
    log(f"Full Load resume harness — schema={SCHEMA} region={region()} "
        f"stages={', '.join(selected)} (yes={args.yes})")
    if not args.yes:
        log("[plan] re-run with --yes to execute. DESTRUCTIVE: 'schema' recreates "
            "the target tables. Run against a STATIC source (no workload).")
    rc = 0
    for stage in selected:
        log(f"===== STAGE: {stage} =====")
        rc = _STAGE_FUNCS[stage](args) or rc
    log(f"Done (exit={rc}).")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
