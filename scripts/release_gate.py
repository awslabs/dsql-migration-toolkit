#!/usr/bin/env python3
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pre-release LIVE smoke gate — catch regressions the unit suite structurally cannot.

The unit suite (3600+ tests) connects to an in-memory DSQL DOUBLE that never validates
connection options or executes real SQL, so a LIVE-only regression sails through green.
The canonical example: v0.1.435 added ``-c lc_numeric=C`` to the DSQL connection options;
Aurora DSQL REJECTS that GUC, so EVERY real DSQL connection failed — yet all unit tests
passed and it shipped to production for ~3 days (fixed v0.1.438). This gate exists so that
class of bug is caught BEFORE an image is published.

Run it against a disposable migration-test target BEFORE building/deploying a release image:

    scripts/release_gate.py            # Tier 1: read-only connect smoke (~seconds)
    scripts/release_gate.py --roundtrip  # + a DSQL scratch CREATE/INSERT/SELECT/DROP + load
    scripts/release_gate.py && deploy/build_in_codebuild.sh   # gate the build

Exit code is 0 only when every check passes, non-zero otherwise, so it gates a script.

Tier 1 (default, READ-ONLY): connect to the live DSQL target via the tool's OWN
``DsqlConnector`` (the exact code path a connection-option regression breaks), run
``SELECT 1``, and confirm the pinned output-formatting GUCs (TimeZone / DateStyle /
IntervalStyle) were accepted by the server — i.e. the options string is valid for DSQL.
Also connects the source via the tool's own dialect engine when one is configured.

Tier 2 (``--roundtrip``): additionally CREATE a scratch table on DSQL with representative
types (integer PK, numeric, text, timestamptz), INSERT + read a row back, then DROP it —
proving DDL apply + typed round-trip work end-to-end on the live cluster. It also loads
a few rows through Full Load's own write path (the real ``BatchedImporter``: multi-row
INSERTs rendered once to ``$n`` text and sent through psycopg's ``RawCursor`` on loader
connections with prepared statements off), into a table with a ``%`` in a column name,
and reads them back value for value. Uses its own scratch schema
(``release_gate_smoke``); never touches migration data.

Config comes from .env / the environment (TARGET_* for DSQL; DB_* / SOURCE_TYPE for the
source), the same source of truth the other scripts/ helpers use.

This is a release-engineering utility (NOT shipped in the app). It complements — does not
replace — the full data-path E2E harnesses (run_e2e_migration.py for MySQL,
run_pg_cdc_e2e.py for PostgreSQL), which exercise Full Load + CDC + CHECKSUM end to end.
"""
from __future__ import annotations

import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(_ROOT, "src"))


def _read_env(path: str) -> dict:
    out: dict = {}
    try:
        with open(path, encoding="utf-8") as lines:
            for raw in lines:
                line = raw.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    out[k.strip()] = v.strip().strip('"').strip("'")
    except FileNotFoundError:
        pass
    return out


_ENV = _read_env(os.path.join(_ROOT, ".env"))


def cfg(key: str, default: str = "") -> str:
    # os.environ FIRST so an explicit export overrides .env.
    return os.environ.get(key) or _ENV.get(key) or default


def _profile():
    return os.environ.get("AWS_PROFILE")


SCRATCH_SCHEMA = "release_gate_smoke"


def _target_config():
    from dsql_migrator.core.models import TargetConnectionConfig

    return TargetConnectionConfig(
        cluster_endpoint=cfg("TARGET_ENDPOINT"),
        region=cfg("TARGET_REGION") or (cfg("TARGET_ENDPOINT").split(".")[2]
                                        if cfg("TARGET_ENDPOINT").count(".") > 2 else "us-east-1"),
        database=cfg("TARGET_DATABASE", "postgres"),
        username=cfg("TARGET_USERNAME", "admin"),
    )


def check_dsql(roundtrip: bool) -> tuple[bool, str]:
    """Connect to the live DSQL target via the tool's real connector and prove it works."""
    if not cfg("TARGET_ENDPOINT"):
        return False, "TARGET_ENDPOINT not set — cannot smoke-test the DSQL connection."
    from dsql_migrator.core.target_connection import DsqlConnector

    try:
        connector = DsqlConnector(_target_config(), aws_profile=_profile())
        conn = connector.connect()
    except Exception as e:  # noqa: BLE001 — the connect-option regression class lands here
        return False, f"DSQL connect FAILED (the lc_numeric-class check): {str(e).splitlines()[0]}"
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT 1")
        if cur.fetchone()[0] != 1:
            return False, "DSQL SELECT 1 did not return 1."
        # Confirm the pinned output-formatting GUCs were ACCEPTED by the server (i.e. the
        # connection options string is valid for DSQL — a bad GUC would have failed connect).
        gucs = {}
        for g in ("TimeZone", "DateStyle", "IntervalStyle", "extra_float_digits"):
            cur.execute(f"SHOW {g}")
            gucs[g] = cur.fetchone()[0]
        detail = f"SELECT 1 ok; GUCs {gucs}"
        if roundtrip:
            detail += "; " + _dsql_roundtrip(cur, connector.connect)
        cur.close()
        return True, detail
    except Exception as e:  # noqa: BLE001
        return False, f"DSQL smoke FAILED: {str(e).splitlines()[0]}"
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _dsql_roundtrip(cur, connect) -> str:
    """CREATE/INSERT/SELECT/DROP a scratch table with representative types on live DSQL,
    plus a multi-row load through Full Load's own write path (:func:`_importer_roundtrip`)
    on connections from ``connect``."""
    tbl = f'"{SCRATCH_SCHEMA}"."gate"'
    cur.execute(f'DROP SCHEMA IF EXISTS "{SCRATCH_SCHEMA}" CASCADE')
    cur.execute(f'CREATE SCHEMA "{SCRATCH_SCHEMA}"')
    cur.execute(
        f"CREATE TABLE {tbl} (id integer PRIMARY KEY, amount numeric(12,2), "
        f"note text, ts timestamptz)"
    )
    cur.execute(
        f"INSERT INTO {tbl} (id, amount, note, ts) VALUES "
        f"(1, 3.14, 'gate', TIMESTAMP '2020-01-02 03:04:05+00')"
    )
    cur.execute(f"SELECT amount, note FROM {tbl} WHERE id = 1")
    amount, note = cur.fetchone()
    loaded = _importer_roundtrip(cur, connect)
    cur.execute(f'DROP SCHEMA IF EXISTS "{SCRATCH_SCHEMA}" CASCADE')
    if str(amount) != "3.14" or note != "gate":
        raise RuntimeError(f"round-trip mismatch: amount={amount!r} note={note!r}")
    return f"scratch round-trip ok (numeric/text/ts); {loaded}"


GATE_ROWS_COLUMNS = ("id", "amount", "note", "ts", "pct%off")


def _gate_rows() -> list:
    """The rows :func:`_importer_roundtrip` loads, two per batch: two full-size batches and a
    partial one. NULLs, a negative and a 4-byte-limit integer, multi-byte text and text that
    looks like placeholders (``%s``, ``$1``) -- bound, never interpolated."""
    from datetime import datetime, timezone
    from decimal import Decimal

    utc = timezone.utc
    return [
        {"id": 1, "amount": Decimal("3.14"), "note": "gate",
         "ts": datetime(2020, 1, 2, 3, 4, 5, tzinfo=utc), "pct%off": 10},
        {"id": 2, "amount": Decimal("-0.50"), "note": "100% off, %s and $1 'q'",
         "ts": None, "pct%off": None},
        {"id": 3, "amount": None, "note": "ünïcode 日本語",
         "ts": datetime(2026, 10, 25, 0, 0, 0, 123456, tzinfo=utc), "pct%off": -7},
        {"id": 4, "amount": Decimal("1234567890.12"), "note": "",
         "ts": datetime(1999, 12, 31, 23, 59, 59, tzinfo=utc), "pct%off": 2147483647},
        {"id": 5, "amount": Decimal("0.00"), "note": None,
         "ts": datetime(2000, 2, 29, 12, 0, 0, tzinfo=utc), "pct%off": 0},
    ]


def _importer_roundtrip(cur, connect) -> str:
    """Load rows through the tool's real ``BatchedImporter`` on live DSQL and read them back.

    The exact Full Load write path: each INSERT shape rendered once to ``$n`` text and sent
    through psycopg's ``RawCursor``, on connections the importer's pool opens from
    ``connect`` (the real connector's) with prepared statements off. A clean load (plain
    INSERT, no ON CONFLICT), two rows per batch, into a table whose column name has a
    ``%`` -- which psycopg's own placeholder scan used to reject.
    """
    from dsql_migrator.core.batched_import import (
        BatchedImporter,
        BatchedImportOptions,
        OnConflictMode,
    )
    from dsql_migrator.core.models import ColumnDef, TableDef

    cur.execute(
        f'CREATE TABLE "{SCRATCH_SCHEMA}"."gate_rows" (id bigint PRIMARY KEY, '
        'amount numeric(12,2), note text, ts timestamptz, "pct%off" integer)'
    )
    rows = _gate_rows()
    table = TableDef(
        name=f"{SCRATCH_SCHEMA}.gate_rows",
        columns=[ColumnDef(name=name, mysql_type="text") for name in GATE_ROWS_COLUMNS],
        primary_key=["id"],
    )
    importer = BatchedImporter(
        BatchedImportOptions(on_conflict=OnConflictMode.NONE, batch_size=2, parallelism=1),
        connection_factory=connect,
    )
    result = importer.import_rows(iter(rows), table)
    if result.failures or result.rows_loaded != len(rows):
        raise RuntimeError(
            f"importer round-trip loaded {result.rows_loaded} of {len(rows)} row(s), "
            f"{result.failures} batch(es) failed: {result.first_error}"
        )
    cur.execute(
        f'SELECT id, amount, note, ts, "pct%off" FROM "{SCRATCH_SCHEMA}"."gate_rows" '
        "ORDER BY id"
    )
    back = [dict(zip(GATE_ROWS_COLUMNS, row)) for row in cur.fetchall()]
    if back != rows:
        sent = {row["id"]: row for row in rows}
        read = {row["id"]: row for row in back}
        differ = sorted(i for i in sent.keys() | read.keys() if sent.get(i) != read.get(i))
        raise RuntimeError(f"importer round-trip mismatch on id(s) {differ}")
    return (
        f"importer round-trip ok ({result.rows_loaded} rows in "
        f"{result.batches_completed} multi-row INSERTs)"
    )


def check_source() -> tuple[bool, str]:
    """Connect to the source (if configured) via the tool's own dialect engine."""
    host = cfg("DB_HOST")
    if not host or host.startswith("<") or not cfg("DB_PASSWORD"):
        return True, "source not configured — skipped (Tier-1 DSQL check is the gate)."
    from dsql_migrator.config import SecretValue
    from dsql_migrator.core.models import SourceConnectionConfig, SourceType
    from dsql_migrator.ui.connect import make_source_engine_factory
    from sqlalchemy import text

    stype_raw = cfg("SOURCE_TYPE", "mysql").strip().lower()
    is_pg = stype_raw in ("postgres", "postgresql", "pg")
    stype = SourceType.POSTGRES if is_pg else SourceType.MYSQL
    source = SourceConnectionConfig(
        source_type=stype, host=host,
        port=int(cfg("DB_PORT", "5432" if is_pg else "3306")),
        database=cfg("DB_NAME") or ("postgres" if is_pg else None),
        username=cfg("DB_USER", "admin"),
    )
    try:
        engine = make_source_engine_factory(SecretValue(cfg("DB_PASSWORD")))(source)
        try:
            with engine.connect() as c:
                c.execute(text("SELECT 1"))
        finally:
            engine.dispose()
        return True, f"source ({stype.value}) connect ok."
    except Exception as e:  # noqa: BLE001
        return False, f"source ({stype.value}) connect FAILED: {str(e).splitlines()[0]}"


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--roundtrip", action="store_true",
                    help="also run a DSQL scratch CREATE/INSERT/SELECT/DROP round-trip, "
                         "including a load through Full Load's write path")
    ap.add_argument("--skip-source", action="store_true",
                    help="skip the source connect check (DSQL check only)")
    args = ap.parse_args()

    print(f"release gate — DSQL {cfg('TARGET_ENDPOINT') or '<unset>'}"
          f" (roundtrip={args.roundtrip})", flush=True)
    checks = [("dsql", lambda: check_dsql(args.roundtrip))]
    if not args.skip_source:
        checks.append(("source", check_source))

    ok = True
    for name, fn in checks:
        passed, detail = fn()
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}: {detail}", flush=True)
        ok = ok and passed
    print("GATE:", "PASS ✓" if ok else "FAIL ✗", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
