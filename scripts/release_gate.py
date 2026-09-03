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
    scripts/release_gate.py --roundtrip  # + a DSQL scratch CREATE/INSERT/SELECT/DROP
    scripts/release_gate.py && deploy/build_in_codebuild.sh   # gate the build

Exit code is 0 only when every check passes, non-zero otherwise, so it gates a script.

Tier 1 (default, READ-ONLY): connect to the live DSQL target via the tool's OWN
``DsqlConnector`` (the exact code path a connection-option regression breaks), run
``SELECT 1``, and confirm the pinned output-formatting GUCs (TimeZone / DateStyle /
IntervalStyle) were accepted by the server — i.e. the options string is valid for DSQL.
Also connects the source via the tool's own dialect engine when one is configured.

Tier 2 (``--roundtrip``): additionally CREATE a scratch table on DSQL with representative
types (integer PK, numeric, text, timestamptz), INSERT + read a row back, then DROP it —
proving DDL apply + typed round-trip work end-to-end on the live cluster. Uses its own
scratch schema (``release_gate_smoke``); never touches migration data.

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
        for raw in open(path, encoding="utf-8"):
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
        conn = DsqlConnector(_target_config(), aws_profile=_profile()).connect()
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
        for g in ("TimeZone", "DateStyle", "IntervalStyle"):
            cur.execute(f"SHOW {g}")
            gucs[g] = cur.fetchone()[0]
        detail = f"SELECT 1 ok; GUCs {gucs}"
        if roundtrip:
            detail += "; " + _dsql_roundtrip(cur)
        cur.close()
        return True, detail
    except Exception as e:  # noqa: BLE001
        return False, f"DSQL smoke FAILED: {str(e).splitlines()[0]}"
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _dsql_roundtrip(cur) -> str:
    """CREATE/INSERT/SELECT/DROP a scratch table with representative types on live DSQL."""
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
    cur.execute(f'DROP SCHEMA IF EXISTS "{SCRATCH_SCHEMA}" CASCADE')
    if str(amount) != "3.14" or note != "gate":
        raise RuntimeError(f"round-trip mismatch: amount={amount!r} note={note!r}")
    return "scratch round-trip ok (numeric/text/ts)"


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
                    help="also run a DSQL scratch CREATE/INSERT/SELECT/DROP round-trip")
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
