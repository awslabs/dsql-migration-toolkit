#!/usr/bin/env python3
"""Create + seed the ``migration_edge`` schema: Full Load VALUE-fidelity edges.

Where ``seed_migration_typetest.py`` stresses the TYPE surface (one column per
MySQL type), this schema stresses the VALUE surface the type schema does not --
the pathological cell values a real MySQL source contains that a heterogeneous
MySQL -> Aurora DSQL migration must either carry across byte-for-byte or refuse
loudly. None of these are exercised by the existing E2E scripts:

  edge_numbers   integer/decimal boundaries -- BIGINT UNSIGNED at 2^64-1 (maps to
                 DSQL numeric(20,0): the range MUST survive, no signed-64 overflow),
                 DECIMAL(38,0) at the max DSQL precision, signed extremes, and 0.
  edge_text      string boundaries -- '' empty string vs NULL (must stay distinct),
                 4-byte UTF-8 (emoji), combining marks, CJK, CSV-hostile bytes
                 (comma/quote/backslash/newline/tab), and a JSON column with unicode.
  edge_temporal  in-range temporal extremes that MUST migrate -- '1000-01-01' and
                 '9999-12-31' (MySQL DATE bounds), DATETIME(6) microsecond precision,
                 and the YEAR range. (Zero-dates / out-of-range TIME are NOT here --
                 they are their own loud-failure demo below.)
  edge_wide      byte-budget-boundary rows -- several ~900 KiB LOB values (each UNDER
                 DSQL's 1 MiB per-value cap, so valid) whose combined batch exceeds
                 the loader's 8 MiB per-transaction byte budget, forcing the
                 byte-aware batch split. Every row MUST reconcile.
  edge_empty     an intentionally EMPTY table (zero rows) -- the empty-table edge.

It also seeds a small number of rows that SHOULD fail / shift, in a SEPARATE
throwaway table EXCLUDED from the migrated set (scripts/_e2e_tables.py) so it
never poisons the clean reconcile -- mirroring ``typetest_loud``:

  edge_zerodate_loud  MySQL zero-dates ('0000-00-00', '0000-00-00 00:00:00') and an
                      out-of-range TIME (negative / > 24h). These have no lossless
                      Aurora DSQL representation; the point of the row is to let
                      ``verify_fullload_edgecases.py`` REPORT the tool's real
                      behavior (loud row failure vs a silent NULL shift) rather than
                      assume it. Kept out of the migrated set so the clean tables
                      still reconcile 100%.

The primary keys of the landmark (extreme-value) rows and the loud rows are
written to ``edge_landmarks.json`` so the verifier reads back exactly those PKs
and compares the stored form on each side.

Connection reuses the repo ``.env`` (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD), exactly
like scripts/seed_migration_typetest.py -- a direct PyMySQL connection to the
source (no bastion). Safety: WRITES to the source; only touches ``migration_edge``;
DROPs and recreates that schema only. Operational utility, NOT shipped code.

Usage:
    python scripts/seed_fullload_edgecases.py            # plan (no writes)
    python scripts/seed_fullload_edgecases.py --yes      # create + seed
    python scripts/seed_fullload_edgecases.py --yes --wide 30 --wide-bytes 900000
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import string
import sys

import pymysql

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCHEMA = "migration_edge"
LANDMARKS_FILE = os.path.join(_PROJECT_ROOT, "edge_landmarks.json")


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


def log(msg: str) -> None:
    print(f"[{dt.datetime.now():%H:%M:%S}] {msg}", flush=True)


def connect(db: str | None = None) -> pymysql.connections.Connection:
    host = _cfg("DB_HOST")
    user = _cfg("DB_USER", "admin")
    port = int(_cfg("DB_PORT", "3306"))
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not host or not pw:
        log("FATAL: source not configured. Set DB_HOST + DB_PASSWORD in .env.")
        sys.exit(2)
    return pymysql.connect(
        host=host, port=port, user=user, password=pw, database=db,
        connect_timeout=15, read_timeout=600, write_timeout=600,
        autocommit=False, charset="utf8mb4",
    )


# --------------------------------------------------------------------------- #
# DDL -- one table per value-edge family. Every PK is a single integer column
# (id) so the tool's keyset export + the per-PK reconcile both apply cleanly.
# --------------------------------------------------------------------------- #
DDL_NUMBERS = """
CREATE TABLE edge_numbers (
  id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label     VARCHAR(40) NOT NULL DEFAULT '',
  big_u     BIGINT UNSIGNED NOT NULL,        -- 2^64-1 must survive (-> numeric(20,0))
  big_s     BIGINT NOT NULL,                 -- signed 64-bit extremes
  dec_max   DECIMAL(38,0) NOT NULL,          -- max DSQL numeric precision
  dec_scale DECIMAL(30,10) NOT NULL,         -- precision+scale round-trip
  int_s     INT NOT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

DDL_TEXT = """
CREATE TABLE edge_text (
  id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label    VARCHAR(40) NOT NULL DEFAULT '',
  s_empty  VARCHAR(255) NOT NULL DEFAULT '', -- '' empty string (NOT null)
  s_null   VARCHAR(255) NULL,                -- explicit NULL (must stay distinct)
  s_uni    VARCHAR(255) NULL,                -- emoji / combining / CJK (4-byte UTF-8)
  s_ctrl   VARCHAR(255) NULL,                -- comma/quote/backslash/newline/tab
  j        JSON NULL,                        -- JSON with unicode
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

DDL_TEMPORAL = """
CREATE TABLE edge_temporal (
  id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label    VARCHAR(40) NOT NULL DEFAULT '',
  d_val    DATE NOT NULL,                    -- 1000-01-01 .. 9999-12-31 bounds
  dt_us    DATETIME(6) NOT NULL,             -- microsecond precision boundary
  t_val    TIME NOT NULL,                    -- in-range 00:00:00 .. 23:59:59
  y_val    YEAR NOT NULL,                    -- 1901 .. 2155 range
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

DDL_WIDE = """
CREATE TABLE edge_wide (
  id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label    VARCHAR(40) NOT NULL DEFAULT '',
  blob_big LONGBLOB NULL,                    -- ~900 KiB each (< 1 MiB per-value cap)
  txt_big  LONGTEXT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

DDL_EMPTY = """
CREATE TABLE edge_empty (
  id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label VARCHAR(40) NOT NULL DEFAULT '',
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

# Throwaway table for the loud / ambiguous values (EXCLUDED from the migrated set).
# ``allowzero`` in the session sql_mode lets MySQL STORE zero-dates so the source
# genuinely holds them; the verifier then reports how the tool handles them.
DDL_LOUD = """
CREATE TABLE edge_zerodate_loud (
  id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label  VARCHAR(40) NOT NULL DEFAULT '',
  d_zero DATE NULL,          -- '0000-00-00'
  dt_zero DATETIME NULL,     -- '0000-00-00 00:00:00'
  t_bad  TIME NULL,          -- out-of-range: negative / > 24h (no DSQL time repr)
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

_DDL = [DDL_NUMBERS, DDL_TEXT, DDL_TEMPORAL, DDL_WIDE, DDL_EMPTY, DDL_LOUD]

_U64_MAX = 2 ** 64 - 1
_I64_MAX = 2 ** 63 - 1
_I64_MIN = -(2 ** 63)
_DEC38_MAX = 10 ** 38 - 1

# A single string packing the value edges that a naive CSV / text encoder breaks
# on: a 4-byte emoji, a combining acute accent, CJK, and CSV-hostile control bytes.
_UNICODE_STR = "é\U0001F600中文 café"          # é 😀 中文 café
_CTRL_STR = 'a,b"c\\d\te\nf'                                        # comma quote bs tab nl


def _rand_text(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=n))


def _insert(cur, table: str, row: dict) -> int:
    """INSERT a dict of column->value into ``table``; return lastrowid."""
    cols = list(row.keys())
    ph = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
    cur.execute(sql, [row[c] for c in cols])
    return cur.lastrowid


# --------------------------------------------------------------------------- #
# Seeding -- landmark rows carry known extremes; the verifier reads them back.
# --------------------------------------------------------------------------- #
def seed_numbers(cur, conn) -> dict:
    """Seed integer/decimal boundary rows; return {pk: description}."""
    rows = [
        ("u64-max", _U64_MAX, _I64_MAX, _DEC38_MAX, "12345678901234567890.0123456789", 2147483647),
        ("u64-zero", 0, _I64_MIN, -_DEC38_MAX, "-99999999999999999999.9999999999", -2147483648),
        ("mid", 9223372036854775808, 0, 0, "0.0000000000", 0),  # 2^63 (> signed max)
    ]
    marks: dict = {}
    for label, big_u, big_s, dec_max, dec_scale, int_s in rows:
        pk = _insert(cur, "edge_numbers", {
            "label": label, "big_u": big_u, "big_s": big_s,
            "dec_max": dec_max, "dec_scale": dec_scale, "int_s": int_s,
        })
        marks[str(pk)] = label
    conn.commit()
    return marks


def seed_text(cur, conn) -> dict:
    """Seed string boundary rows; return {pk: description}."""
    marks: dict = {}
    # empty-vs-null: s_empty='', s_null=NULL on one row; inverted on the next.
    pk = _insert(cur, "edge_text", {
        "label": "empty-and-null", "s_empty": "", "s_null": None,
        "s_uni": _UNICODE_STR, "s_ctrl": _CTRL_STR,
        "j": json.dumps({"emoji": "\U0001F600", "k": "中文"}, ensure_ascii=False),
    })
    marks[str(pk)] = "empty-and-null"
    pk = _insert(cur, "edge_text", {
        "label": "unicode-heavy", "s_empty": "", "s_null": _UNICODE_STR,
        "s_uni": "\U0001F937\U0001F3FD‍♂️",   # multi-codepoint ZWJ emoji
        "s_ctrl": '""', "j": json.dumps({"n": None, "arr": [1, "é"]}, ensure_ascii=False),
    })
    marks[str(pk)] = "unicode-heavy"
    conn.commit()
    return marks


def seed_temporal(cur, conn) -> dict:
    """Seed in-range temporal extremes that MUST migrate; return {pk: description}."""
    rows = [
        ("date-min", "1000-01-01", "1000-01-01 00:00:00.000001", "00:00:00", 1901),
        ("date-max", "9999-12-31", "9999-12-31 23:59:59.999999", "23:59:59", 2155),
        ("mid", "2024-02-29", "2024-02-29 12:34:56.654321", "12:00:00", 2024),
    ]
    marks: dict = {}
    for label, d_val, dt_us, t_val, y_val in rows:
        pk = _insert(cur, "edge_temporal", {
            "label": label, "d_val": d_val, "dt_us": dt_us,
            "t_val": t_val, "y_val": y_val,
        })
        marks[str(pk)] = label
    conn.commit()
    return marks


def seed_wide(cur, conn, *, n: int, nbytes: int) -> dict:
    """Seed ``n`` rows each holding ~``nbytes`` valid LOB values; return {pk: desc}.

    Each value is UNDER DSQL's 1 MiB per-value cap (so it is not quarantined), but
    a batch of several exceeds the loader's 8 MiB per-transaction byte budget,
    forcing the byte-aware split. Every row must reconcile.
    """
    log(f"Seeding {n} wide rows (~{nbytes} bytes/LOB each)...")
    marks: dict = {}
    for i in range(n):
        blob = bytes([(i + j) & 0xFF for j in range(nbytes)])  # deterministic bytes
        txt = _rand_text(nbytes // 2)
        pk = _insert(cur, "edge_wide", {
            "label": f"wide-{i}", "blob_big": blob, "txt_big": txt,
        })
        # Only the first/last are landmarks (read back); the rest just add volume.
        if i in (0, n - 1):
            marks[str(pk)] = f"wide-{i}"
        if i % 5 == 0:
            conn.commit()
    conn.commit()
    return marks


def seed_loud(cur, conn) -> dict:
    """Seed the zero-date / out-of-range-TIME rows (excluded table). Return {pk: desc}.

    MySQL only stores zero-dates when strict/NO_ZERO_DATE modes are off; the caller
    relaxes the session sql_mode before this runs. If the server still rejects a
    value, the row is skipped and noted (the behavior is still recorded).
    """
    marks: dict = {}
    cases = [
        ("zero-date", {"d_zero": "0000-00-00", "dt_zero": None, "t_bad": None}),
        ("zero-datetime", {"d_zero": None, "dt_zero": "0000-00-00 00:00:00", "t_bad": None}),
        ("time-negative", {"d_zero": None, "dt_zero": None, "t_bad": "-100:00:00"}),
        ("time-over-24h", {"d_zero": None, "dt_zero": None, "t_bad": "838:59:59"}),
    ]
    for label, cols in cases:
        try:
            pk = _insert(cur, "edge_zerodate_loud", {"label": label, **cols})
            conn.commit()
            marks[str(pk)] = label
        except Exception as exc:  # noqa: BLE001 - the server rejected this value
            conn.rollback()
            log(f"  edge_zerodate_loud[{label}]: source rejected -> {str(exc).splitlines()[0]}")
    return marks


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--yes", action="store_true",
                    help="actually create + seed (else plan only)")
    ap.add_argument("--wide", type=int, default=20,
                    help="number of byte-budget-boundary wide rows (default 20)")
    ap.add_argument("--wide-bytes", type=int, default=900_000,
                    help="approx bytes per wide LOB value (default 900000, < 1 MiB)")
    ap.add_argument("--seed", type=int, default=20260702, help="RNG seed")
    args = ap.parse_args()

    random.seed(args.seed)

    if not args.yes:
        log(f"[plan] would DROP+CREATE schema `{SCHEMA}` on the source and seed: "
            f"edge_numbers (3 int/decimal extremes), edge_text (2 unicode/empty/null "
            f"rows), edge_temporal (3 date/time extremes), edge_wide ({args.wide} rows "
            f"~{args.wide_bytes}B/LOB, byte-budget boundary), edge_empty (0 rows), + "
            "edge_zerodate_loud (zero-dates / out-of-range TIME, EXCLUDED from the "
            "migrated set). Use --yes to run.")
        return 0

    conn = connect(None)
    cur = conn.cursor()
    log(f"DROP + CREATE DATABASE {SCHEMA} ...")
    cur.execute(f"DROP DATABASE IF EXISTS {SCHEMA}")
    conn.commit()
    cur.execute(
        f"CREATE DATABASE {SCHEMA} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    conn.commit()
    cur.execute(f"USE {SCHEMA}")
    # Relax the session sql_mode so the loud table can actually STORE zero-dates
    # (a strict server would otherwise reject them and the edge could not be seeded).
    # This is session-scoped only -- it never changes the server default.
    cur.execute("SET SESSION sql_mode = ''")
    for stmt in _DDL:
        cur.execute(stmt)
    conn.commit()
    log("Schema + 6 tables created.")

    landmarks = {
        "schema": SCHEMA,
        "edge_numbers": seed_numbers(cur, conn),
        "edge_text": seed_text(cur, conn),
        "edge_temporal": seed_temporal(cur, conn),
        "edge_wide": seed_wide(cur, conn, n=args.wide, nbytes=args.wide_bytes),
        "edge_empty": {},  # intentionally empty
        "edge_zerodate_loud": seed_loud(cur, conn),
        "wide_bytes": args.wide_bytes,
        "note": "landmark PKs carry known extreme values; verify_fullload_edgecases.py "
                "reads them back and compares the stored form on each side. "
                "edge_zerodate_loud is EXCLUDED from the migrated set (its rows "
                "demonstrate the zero-date / out-of-range-TIME handling).",
    }
    with open(LANDMARKS_FILE, "w", encoding="utf-8") as f:
        json.dump(landmarks, f, indent=2, ensure_ascii=False)
    log(f"Wrote landmark PKs -> {LANDMARKS_FILE}")

    for t in ("edge_numbers", "edge_text", "edge_temporal", "edge_wide",
              "edge_empty", "edge_zerodate_loud"):
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        log(f"  {t}: {cur.fetchone()[0]} rows")
    conn.close()
    log("Seed complete. ✓")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
