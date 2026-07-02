#!/usr/bin/env python3
"""Full Load VALUE-fidelity verifier for the ``migration_edge`` schema.

Pairs with ``scripts/seed_fullload_edgecases.py``. It proves, on real
infrastructure, that the tool's OWN Full Load engine carries the pathological
value edges across MySQL -> Aurora DSQL byte-for-byte -- or fails loudly per the
documented contract -- rather than silently corrupting them. It re-implements
nothing: it drives ``run_full_load`` (the same engine the UI runs) and the
``Validator`` (the same authoritative comparison Step 4 runs), then reads back the
landmark rows on each side for a value-level spot check the count/checksum verdict
alone would not make legible.

What it does (headless, resumable per stage via --only):

  1. full-load   Run the tool's Full Load over the 5 CLEAN edge tables (into the
                 schema-qualified DSQL target, exactly like run_full_load_harness),
                 with --clean DROP+recreate for a repeatable slate.
  2. validate    Run the Validator in CHECKSUM mode + reconcile over the clean
                 tables. A "MATCH" here means the actual DATA is equal (checksums
                 + per-PK reconcile), not merely the counts -- so a byte-level
                 corruption of an emoji, a 2^64-1 integer, or a 9999-12-31 date is
                 caught, not hidden.
  3. landmarks   Read back the landmark PKs (from edge_landmarks.json) on BOTH
                 sides and print the stored form side by side, so the operator can
                 SEE that '' stayed distinct from NULL, the emoji round-tripped,
                 BIGINT UNSIGNED 2^64-1 survived as numeric, etc.
  4. loud        Attempt to Full Load ONLY the excluded edge_zerodate_loud table
                 and REPORT what the tool did with the zero-dates / out-of-range
                 TIME (loud row failure, quarantine, or a NULL shift) -- the tool's
                 real behavior, observed, not assumed.

Prerequisites & connection: identical to run_full_load_harness.py -- ``.env`` with
DB_HOST/DB_PORT/DB_USER/DB_PASSWORD (source) and TARGET_ENDPOINT/TARGET_REGION/
TARGET_DATABASE/TARGET_USERNAME (DSQL), plus AWS creds able to reach DSQL. Run the
seeder first: ``python scripts/seed_fullload_edgecases.py --yes``.

⚠️ DESTRUCTIVE (stage full-load with --clean DROPs+recreates the edge tables on the
DSQL target). Requires --yes; without it, prints the plan and exits.

Usage (from repo root):
    set -a; source .env; set +a
    python scripts/seed_fullload_edgecases.py --yes            # seed the source first
    .venv/bin/python scripts/verify_fullload_edgecases.py      # plan only
    .venv/bin/python scripts/verify_fullload_edgecases.py --yes
    .venv/bin/python scripts/verify_fullload_edgecases.py --yes --only landmarks

Exit code: 0 when every clean table reconciles (CHECKSUM MATCH, 0 missing / 0
extra); non-zero otherwise -- so it can gate a test.
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

SCHEMA = os.environ.get("CDC_WORKLOAD_SCHEMA", "migration_edge")
LOUD_TABLE = "edge_zerodate_loud"  # excluded from the migrated set (its own demo)
LANDMARKS_FILE = os.path.join(_ROOT, "edge_landmarks.json")
STAGES = ("full-load", "validate", "landmarks", "loud")


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
# Shared engine wiring (reuses the tool's own components)
# --------------------------------------------------------------------------- #
def _source_target():
    """Build the (source, target, password) configs from .env (tool models)."""
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import (
        SourceConnectionConfig, TargetConnectionConfig,
    )
    pwd = cfg("DB_PASSWORD")
    source = SourceConnectionConfig(
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "3306")),
        database=SCHEMA, username=cfg("DB_USER", "admin"),
    )
    target = TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"), region=region(),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )
    return source, target, SecretValue(pwd)


def _introspect_and_qualify(source, password, want_tables: list[str]):
    """Introspect the source and qualify the wanted tables as "<SCHEMA>.<table>".

    Same qualification the Full Load harness uses so Full Load and Validation both
    read the source as `schema`.`table` and the target as "schema"."table" -- the
    schema the converter/loader write to. Returns the resolved TableDef list.
    """
    from dsql_migrator.core.table_selection import TableSelection, TableSelector
    from dsql_migrator.ui.evaluation import _default_introspector_factory

    inventory = _default_introspector_factory(password).introspect(source)
    present = {t.name: t for t in inventory.tables}
    wanted: list[str] = []
    missing: list[str] = []
    for t in want_tables:
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


def _run_full_load(source, target, password, inventory, tables, wanted, *, clean: bool):
    """Run the tool's own Full Load over ``tables`` and return the finished job.

    Mirrors run_full_load_harness.py: builds the in-process migrator, submits the
    work on a JobManager, and polls to completion.
    """
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.job_manager import JobManager
    from dsql_migrator.ui.data_migration._engine import (
        DataMigrationInputs, default_migrator_factory, run_full_load,
    )

    replace = frozenset(wanted) if clean else frozenset()
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

    log(f"Full Load {len(tables)} table(s) into {target.cluster_endpoint} "
        f"(clean={bool(replace)}) ...")
    job_id = jm.submit(work)
    deadline = time.time() + 3600.0
    while time.time() < deadline:
        job = jm.get_status(job_id)
        done = sum(1 for c in job.chunks if c.status in ("DONE", "FAILED"))
        if job.status in ("DONE", "FAILED", "CANCELLED"):
            break
        cur = next((c.chunk_id for c in job.chunks if c.status == "IN_PROGRESS"), "")
        log(f"  [{job.status}] {done}/{len(job.chunks)} settled {('· '+cur) if cur else ''}")
        time.sleep(3)
    job = jm.get_status(job_id)
    err = jm.get_error(job_id)
    for c in job.chunks:
        log(f"  {c.chunk_id:<28} {c.status:<10} rows={c.rows_loaded}")
    if err:
        log(f"  job error: {err.splitlines()[0]}")
    return job, error_log.summary(job_id)


# --------------------------------------------------------------------------- #
# Stage 1: Full Load (clean tables)
# --------------------------------------------------------------------------- #
def stage_full_load(args) -> int:
    if not args.yes:
        log(f"[plan] would Full Load (DROP+recreate) {tables_for(SCHEMA)} into DSQL. "
            "Use --yes.")
        return 0
    source, target, password = _source_target()
    inventory, tables, wanted = _introspect_and_qualify(source, password, tables_for(SCHEMA))
    job, summary = _run_full_load(
        source, target, password, inventory, tables, wanted, clean=True)
    log(f"Full Load status={job.status}; errors={summary.total_errors}")
    return 0 if job.status == "DONE" else 1


# --------------------------------------------------------------------------- #
# Stage 2: Validation (CHECKSUM + reconcile) over the clean tables
# --------------------------------------------------------------------------- #
def stage_validate(args) -> int:
    from dsql_migrator.core.models import ValidationMode
    from dsql_migrator.core.validator import Validator
    from dsql_migrator.ui.connect import make_source_engine_factory

    if not args.yes:
        log("[plan] would run Validation (CHECKSUM + reconcile) over the clean "
            "edge tables. Use --yes.")
        return 0
    source, target, password = _source_target()
    _inv, tables, _wanted = _introspect_and_qualify(source, password, tables_for(SCHEMA))
    log("Running Validation (CHECKSUM + per-PK reconcile) ...")
    report = Validator(
        source_engine_factory=make_source_engine_factory(password),
    ).validate(
        source, target, list(tables), ValidationMode.CHECKSUM,
        reconcile=True, max_workers=int(cfg("DSQL_MIGRATOR_VALIDATE_MAX_WORKERS", "4")),
    )
    matched = sum(1 for i in report.items if i.matched)
    log(f"Validation: {matched}/{len(report.items)} tables matched; "
        f"is_match={report.is_match}")
    for item in report.items:
        ck = item.checksum_match
        rec = item.reconcile
        detail = (f"src={item.source_row_count} tgt={item.target_row_count} "
                  f"checksum_match={ck}")
        if rec is not None:
            detail += f" missing={rec.missing_on_target} extra={rec.extra_on_target}"
        if item.error:
            detail += f" ERROR={item.error.splitlines()[0]}"
        flag = "MATCH ✓" if item.matched else "MISMATCH ✗"
        log(f"  {item.table:<28} {flag}  ({detail})")
    return 0 if report.is_match else 1


# --------------------------------------------------------------------------- #
# Stage 3: landmark read-back (value-level spot check on both sides)
# --------------------------------------------------------------------------- #
def _source_conn():
    import pymysql
    pw = cfg("DB_PASSWORD")
    return pymysql.connect(
        host=cfg("DB_HOST"), port=int(cfg("DB_PORT", "3306")),
        user=cfg("DB_USER", "admin"), password=pw,
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


def _fmt(value: object) -> str:
    """Render a cell for legible side-by-side display (bytes -> len + prefix hex)."""
    if value is None:
        return "NULL"
    if isinstance(value, (bytes, bytearray, memoryview)):
        b = bytes(value)
        return f"<{len(b)} bytes 0x{b[:8].hex()}…>"
    s = repr(value)
    return s if len(s) <= 60 else s[:57] + "…"


def stage_landmarks(args) -> int:
    """Read back the landmark rows on both sides and print stored form side by side."""
    if not os.path.exists(LANDMARKS_FILE):
        log(f"landmark file {LANDMARKS_FILE} missing — run the seeder first.")
        return 2
    marks = json.load(open(LANDMARKS_FILE, encoding="utf-8"))
    # Columns of interest per table (skip the surrogate id + label plumbing).
    cols_by_table = {
        "edge_numbers": ["big_u", "big_s", "dec_max", "dec_scale", "int_s"],
        "edge_text": ["s_empty", "s_null", "s_uni", "s_ctrl", "j"],
        "edge_temporal": ["d_val", "dt_us", "t_val", "y_val"],
        "edge_wide": ["blob_big", "txt_big"],
    }
    src = _source_conn()
    tgt = _target_conn()
    mismatches = 0
    try:
        for table, cols in cols_by_table.items():
            pk_map = marks.get(table) or {}
            if not pk_map:
                continue
            log(f"=== {table} landmarks ===")
            for pk, desc in pk_map.items():
                col_list = ", ".join(f"`{c}`" for c in cols)
                with src.cursor() as c:
                    c.execute(f"SELECT {col_list} FROM `{SCHEMA}`.`{table}` WHERE id=%s", (pk,))
                    s_row = c.fetchone()
                tcur = tgt.cursor()
                tcol_list = ", ".join(f'"{c}"' for c in cols)
                try:
                    tcur.execute(
                        f'SELECT {tcol_list} FROM "{SCHEMA}"."{table}" WHERE id=%s', (pk,))
                    t_row = tcur.fetchone()
                except Exception as exc:  # noqa: BLE001
                    tgt.rollback()
                    log(f"  pk={pk} [{desc}]: target read error {str(exc).splitlines()[0]}")
                    mismatches += 1
                    continue
                log(f"  pk={pk} [{desc}]:")
                if s_row is None or t_row is None:
                    log(f"    MISSING  source={'∅' if s_row is None else 'ok'} "
                        f"target={'∅' if t_row is None else 'ok'}")
                    mismatches += 1
                    continue
                for i, col in enumerate(cols):
                    s_disp, t_disp = _fmt(s_row[i]), _fmt(t_row[i])
                    # Byte-length equality for LOBs; textual equality otherwise. The
                    # authoritative equality is Validation's checksum -- this is a
                    # human-legible spot check, so a benign cross-engine repr diff
                    # (e.g. Decimal vs int) is shown but not counted as a failure.
                    same = _loose_equal(s_row[i], t_row[i])
                    mark = "  " if same else "≠ "
                    log(f"    {mark}{col:<10} src={s_disp:<40} tgt={t_disp}")
        return 1 if mismatches else 0
    finally:
        for c in (src, tgt):
            try:
                c.close()
            except Exception:
                pass


def _loose_equal(a: object, b: object) -> bool:
    """Cross-engine value equality for the legible spot check (not authoritative).

    Bytes compare by content; numbers by numeric value (Decimal/int/float);
    everything else by string form. Validation's checksum is the real verdict; this
    only flags an obvious divergence for the human reading the table.
    """
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (bytes, bytearray, memoryview)) or isinstance(b, (bytes, bytearray, memoryview)):
        try:
            return bytes(a) == bytes(b)
        except Exception:  # noqa: BLE001
            return False
    from decimal import Decimal, InvalidOperation
    try:
        return Decimal(str(a)) == Decimal(str(b))
    except (InvalidOperation, ValueError):
        return str(a) == str(b)


# --------------------------------------------------------------------------- #
# Stage 4: loud table -- observe (never assume) the zero-date / bad-TIME handling
# --------------------------------------------------------------------------- #
def stage_loud(args) -> int:
    """Full Load ONLY edge_zerodate_loud and report the tool's real behavior."""
    source, target, password = _source_target()
    if not args.yes:
        log(f"[plan] would Full Load ONLY `{LOUD_TABLE}` (zero-dates / bad TIME) and "
            "report whether the tool fails loudly, quarantines, or shifts to NULL. "
            "Use --yes.")
        return 0
    try:
        inventory, tables, wanted = _introspect_and_qualify(source, password, [LOUD_TABLE])
    except SystemExit as exc:
        log(f"{LOUD_TABLE} not present ({exc}); did the seeder store the zero-dates? "
            "Skipping.")
        return 0
    job, summary = _run_full_load(
        source, target, password, inventory, tables, wanted, clean=True)
    log(f"{LOUD_TABLE}: Full Load status={job.status}; errors={summary.total_errors}")
    # Report, don't assert: the point is to record the observed contract behavior.
    if job.status == "DONE" and summary.total_errors == 0:
        log("  OBSERVED: the tool loaded the zero-date / bad-TIME rows without error "
            "-- inspect the target values (they may have been coerced). This is the "
            "documented generic-path behavior; confirm it matches expectations.")
    else:
        log("  OBSERVED: the tool refused / quarantined the zero-date / bad-TIME rows "
            "-- a loud failure (no silent corruption), the safe outcome. See the "
            "error summary above for the exact per-row reason.")
    return 0


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
_STAGE_FUNCS = {
    "full-load": stage_full_load,
    "validate": stage_validate,
    "landmarks": stage_landmarks,
    "loud": stage_loud,
}


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--yes", action="store_true",
                    help="actually run (Full Load DROP+recreate needs this)")
    ap.add_argument("--only", choices=STAGES, help="run only this one stage")
    args = ap.parse_args()

    if not cfg("DB_HOST") or not cfg("DB_PASSWORD") or not cfg("TARGET_ENDPOINT"):
        log("ERROR: set DB_HOST/DB_PASSWORD/TARGET_ENDPOINT in .env "
            "(`set -a; source .env; set +a`).")
        return 2

    selected = [args.only] if args.only else list(STAGES)
    log(f"Edge-fidelity verify — schema={SCHEMA} region={region()} "
        f"stages={', '.join(selected)} (yes={args.yes})")
    rc = 0
    for stage in selected:
        log(f"===== STAGE: {stage} =====")
        rc = _STAGE_FUNCS[stage](args) or rc
    log(f"Done (exit={rc}).")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
