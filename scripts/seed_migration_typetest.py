#!/usr/bin/env python3
"""Create + seed the ``migration_typetest`` schema on the source MySQL.

A small, maximally type-diverse schema for an end-to-end MySQL -> Aurora DSQL
migration test. It exercises as much of the MySQL type/syntax surface as possible
(every integer/unsigned variant, DECIMAL incl. precision>38, FLOAT/DOUBLE, BIT,
CHAR/VARCHAR incl. a case-insensitive collation, the full DATE/TIME family, ENUM,
SET, JSON, and the full TEXT/BLOB LOB family), across a parent -> child / lob FK
chain, so Full Load + CDC can be validated against realistic heterogeneous data.

It deliberately seeds a small number of rows that should FAIL the migration so the
failure paths can be verified end to end:
  - oversized LOB rows (a ~1.5 MiB LONGTEXT value, > DSQL's 1 MiB per-value limit)
    -> Full Load per-row QUARANTINE / CDC DLQ (the table itself keeps loading).
  - one TINYINT(1) value of 2 in a SEPARATE throwaway table ``typetest_loud`` ->
    a loud, table-fatal ValueConversionError in Full Load (kept OUT of the
    migrated set so it never poisons the clean reconcile).

The PKs of those intentional-failure rows are written to ``typetest_failrows.json``
so the verification step knows exactly which rows must be absent on the target /
present in the DLQ.

Connection reuses the repo ``.env`` (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD), exactly
like scripts/copy_to_smaller_db.py -- a direct PyMySQL connection to the source
(no bastion). Safety: WRITES to the source; only touches the ``migration_typetest``
schema; DROPs and recreates that schema only. Operational utility, NOT shipped code.

Usage:
    python scripts/seed_migration_typetest.py            # plan (no writes)
    python scripts/seed_migration_typetest.py --yes      # create + seed
    python scripts/seed_migration_typetest.py --yes --parents 500 --children 2000 --lobs 500
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

SCHEMA = "migration_typetest"
FAILROWS_FILE = os.path.join(_PROJECT_ROOT, "typetest_failrows.json")


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
# DDL -- the maximally type-diverse schema
# --------------------------------------------------------------------------- #
DDL_PARENT = """
CREATE TABLE typetest_parent (
  parent_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  c_tinyint      TINYINT,
  c_tinyint_u    TINYINT UNSIGNED,
  c_bool         TINYINT(1) NOT NULL DEFAULT 0,
  c_smallint     SMALLINT,
  c_smallint_u   SMALLINT UNSIGNED,
  c_mediumint    MEDIUMINT,
  c_mediumint_u  MEDIUMINT UNSIGNED,
  c_int          INT,
  c_int_u        INT UNSIGNED,
  c_bigint       BIGINT,
  c_bigint_u     BIGINT UNSIGNED,
  c_decimal      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  c_decimal_big  DECIMAL(38,0) NULL,
  -- NOTE: DECIMAL(65,30) (precision>38) is DDL-FATAL on DSQL ("NUMERIC precision
  -- must be between 1 and 38") and is the documented evaluation FLAG case. It is
  -- intentionally OMITTED here so the rest of the type-rich table migrates; the
  -- precision>38 path is demonstrated/document-only (see note 01).
  c_float        FLOAT,
  c_double       DOUBLE,
  c_bit          BIT(8),
  c_char         CHAR(10),
  c_varchar      VARCHAR(255) NOT NULL DEFAULT '',
  c_varchar_ci   VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  c_date         DATE,
  c_datetime     DATETIME,
  c_datetime6    DATETIME(6),
  c_timestamp    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  c_time         TIME,
  c_year         YEAR,
  c_enum         ENUM('alpha','beta','gamma','delta') NOT NULL DEFAULT 'alpha',
  c_set          SET('read','write','exec','admin') NULL,
  created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (parent_id),
  KEY idx_parent_enum (c_enum)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

DDL_CHILD = """
CREATE TABLE typetest_child (
  child_id    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  parent_id   BIGINT UNSIGNED NOT NULL,
  seq_no      INT UNSIGNED NOT NULL DEFAULT 1,
  qty         INT UNSIGNED NOT NULL DEFAULT 1,
  unit_price  DECIMAL(12,2) NOT NULL DEFAULT 0.00,
  line_total  DECIMAL(16,2) GENERATED ALWAYS AS (qty * unit_price) STORED,
  payload     JSON NULL,
  note_ci     VARCHAR(120) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (child_id),
  UNIQUE KEY uq_parent_seq (parent_id, seq_no),
  KEY idx_child_parent (parent_id),
  CONSTRAINT fk_child_parent FOREIGN KEY (parent_id)
    REFERENCES typetest_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

DDL_LOB = """
CREATE TABLE typetest_lob (
  lob_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  parent_id    BIGINT UNSIGNED NOT NULL,
  c_tinytext   TINYTEXT NULL,
  c_text       TEXT NULL,
  c_mediumtext MEDIUMTEXT NULL,
  c_longtext   LONGTEXT NULL,
  c_tinyblob   TINYBLOB NULL,
  c_blob       BLOB NULL,
  c_mediumblob MEDIUMBLOB NULL,
  c_longblob   LONGBLOB NULL,
  c_binary     BINARY(16) NULL,
  c_varbinary  VARBINARY(255) NULL,
  c_json       JSON NULL,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (lob_id),
  KEY idx_lob_parent (parent_id),
  CONSTRAINT fk_lob_parent FOREIGN KEY (parent_id)
    REFERENCES typetest_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

# Throwaway table for the loud, table-fatal TINYINT(1)-out-of-range demonstration.
# EXCLUDED from the migrated table set (scripts/_e2e_tables.py) so it never poisons
# the clean reconcile -- exercised only in its own separate Full Load run.
DDL_LOUD = """
CREATE TABLE typetest_loud (
  id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  label  VARCHAR(40) NOT NULL DEFAULT '',
  c_bool TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

# Optional, document-only: an UNSUPPORTED spatial type. Created so the evaluation
# "unsupported type" path can be demonstrated, but EXCLUDED from the migrated set.
DDL_SPATIAL = """
CREATE TABLE typetest_spatial (
  id   INT NOT NULL AUTO_INCREMENT,
  name VARCHAR(40) NOT NULL DEFAULT '',
  geom POINT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
"""

_DDL = [DDL_PARENT, DDL_CHILD, DDL_LOB, DDL_LOUD, DDL_SPATIAL]


# --------------------------------------------------------------------------- #
# Value generators (clean, in-range -- these rows MUST reconcile 100%)
# --------------------------------------------------------------------------- #
_ENUM = ("alpha", "beta", "gamma", "delta")
_SET = ("read", "write", "exec", "admin")
_WORDS = ("alpha", "Bravo", "charlie", "Delta", "echo", "Foxtrot", "GOLF", "hotel")


def _rand_text(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=n))


def _rand_set() -> str:
    k = random.randint(0, len(_SET))
    return ",".join(sorted(random.sample(_SET, k), key=_SET.index)) if k else ""


def _rand_parent_row():
    """A clean parent row: every value strictly in range / valid.

    c_bool is always 0/1 and c_enum/c_set always valid, so the clean Full Load is
    never broken by an out-of-range value. (precision>38 is a document-only DSQL
    FLAG case and is not a column here.)
    """
    now = dt.datetime.now()
    return {
        "c_tinyint": random.randint(-128, 127),
        "c_tinyint_u": random.randint(0, 255),
        "c_bool": random.randint(0, 1),
        "c_smallint": random.randint(-32768, 32767),
        "c_smallint_u": random.randint(0, 65535),
        "c_mediumint": random.randint(-8388608, 8388607),
        "c_mediumint_u": random.randint(0, 16777215),
        "c_int": random.randint(-2147483648, 2147483647),
        "c_int_u": random.randint(0, 4294967295),
        "c_bigint": random.randint(-(2**63), 2**63 - 1),
        "c_bigint_u": random.randint(0, 2**64 - 1),
        "c_decimal": round(random.uniform(0, 99999999), 2),
        "c_decimal_big": random.randint(0, 10**38 - 1),
        # NOTE: c_decimal_over (DECIMAL(65,30), precision>38) is intentionally NOT
        # a column on typetest_parent -- precision>38 is DDL-fatal on DSQL and is a
        # document-only evaluation FLAG case (see the DDL note above). It must not
        # appear here or the INSERT references a non-existent column.
        "c_float": round(random.uniform(-1e6, 1e6), 3),
        "c_double": random.uniform(-1e12, 1e12),
        "c_bit": random.randint(0, 255),
        "c_char": _rand_text(10),
        "c_varchar": _rand_text(random.randint(5, 200)),
        "c_varchar_ci": random.choice(_WORDS),
        "c_date": (now - dt.timedelta(days=random.randint(0, 3000))).date(),
        "c_datetime": now - dt.timedelta(seconds=random.randint(0, 10**7)),
        "c_datetime6": now - dt.timedelta(microseconds=random.randint(0, 10**9)),
        "c_time": dt.timedelta(seconds=random.randint(0, 86399)),
        "c_year": random.randint(1990, 2030),
        "c_enum": random.choice(_ENUM),
        "c_set": _rand_set(),
        "created_at": now,
    }


def _insert_dict(cur, table: str, row: dict) -> int:
    """INSERT a dict of column->value into ``table``; return lastrowid."""
    cols = list(row.keys())
    ph = ", ".join(["%s"] * len(cols))
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
    cur.execute(sql, [row[c] for c in cols])
    return cur.lastrowid


# --------------------------------------------------------------------------- #
# Seeding
# --------------------------------------------------------------------------- #
def seed_clean(cur, conn, *, n_parents: int, n_children: int, n_lobs: int) -> list[int]:
    """Seed the clean (must-reconcile) rows. Returns the parent_id list."""
    log(f"Seeding {n_parents} parents...")
    parent_ids: list[int] = []
    for _ in range(n_parents):
        parent_ids.append(_insert_dict(cur, "typetest_parent", _rand_parent_row()))
    conn.commit()

    log(f"Seeding {n_children} children...")
    for i in range(n_children):
        pid = random.choice(parent_ids)
        row = {
            "parent_id": pid,
            # seq_no unique per parent: use the running index to avoid uq_parent_seq
            # collisions (parent chosen randomly, so make seq globally unique-ish).
            "seq_no": i + 1,
            "qty": random.randint(1, 20),
            "unit_price": round(random.uniform(1, 999), 2),
            "payload": json.dumps({"k": random.choice(_WORDS),
                                   "n": random.randint(0, 1000),
                                   "ok": random.choice([True, False])}),
            "note_ci": random.choice(_WORDS),
            "created_at": dt.datetime.now(),
        }
        _insert_dict(cur, "typetest_child", row)
        if i % 500 == 0:
            conn.commit()
    conn.commit()

    log(f"Seeding {n_lobs} lob rows (small, valid)...")
    for _ in range(n_lobs):
        pid = random.choice(parent_ids)
        row = {
            "parent_id": pid,
            "c_tinytext": _rand_text(random.randint(10, 200)),
            "c_text": _rand_text(random.randint(100, 4000)),
            "c_mediumtext": _rand_text(random.randint(100, 8000)),
            "c_longtext": _rand_text(random.randint(100, 8000)),
            "c_tinyblob": _rand_text(random.randint(10, 200)).encode(),
            "c_blob": _rand_text(random.randint(100, 2000)).encode(),
            "c_mediumblob": _rand_text(random.randint(100, 4000)).encode(),
            "c_longblob": _rand_text(random.randint(100, 4000)).encode(),
            "c_binary": os.urandom(16),
            "c_varbinary": os.urandom(random.randint(8, 200)),
            "c_json": json.dumps({"tags": random.sample(_WORDS, 3),
                                 "score": round(random.uniform(0, 100), 2)}),
            "created_at": dt.datetime.now(),
        }
        _insert_dict(cur, "typetest_lob", row)
    conn.commit()
    return parent_ids


# Oversized value: >1 MiB (DSQL per-value limit) and <4 MiB (MaxMessageBytes), so
# in Full Load it is QUARANTINED per-row (the table keeps loading) and in CDC an
# INSERT/UPDATE of it is caught by the sink's oversize guard -> DLQ.
_OVERSIZE_BYTES = int(1.5 * 1024 * 1024)


def seed_oversized_lobs(cur, conn, parent_ids: list[int], n: int) -> list[int]:
    """Seed ``n`` typetest_lob rows whose c_longtext is ~1.5 MiB. Return their PKs."""
    log(f"Seeding {n} OVERSIZED lob rows (~1.5 MiB c_longtext each)...")
    big = "X" * _OVERSIZE_BYTES  # ASCII -> 1 byte/char -> ~1.5 MiB UTF-8
    pks: list[int] = []
    for _ in range(n):
        pid = random.choice(parent_ids)
        row = {
            "parent_id": pid,
            "c_text": "oversized-marker",
            "c_longtext": big,
            "c_json": json.dumps({"oversized": True}),
            "created_at": dt.datetime.now(),
        }
        pks.append(_insert_dict(cur, "typetest_lob", row))
    conn.commit()
    return pks


def seed_loud(cur, conn) -> int:
    """Seed the single TINYINT(1)=2 row in typetest_loud. Return its PK."""
    log("Seeding the loud TINYINT(1)=2 row in typetest_loud...")
    # A couple of clean rows + the one out-of-range value (table-fatal in Full Load).
    _insert_dict(cur, "typetest_loud", {"label": "clean-true", "c_bool": 1})
    _insert_dict(cur, "typetest_loud", {"label": "clean-false", "c_bool": 0})
    pk = _insert_dict(cur, "typetest_loud", {"label": "out-of-range", "c_bool": 2})
    conn.commit()
    return pk


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--yes", action="store_true",
                    help="actually create + seed (else plan only)")
    ap.add_argument("--parents", type=int, default=500)
    ap.add_argument("--children", type=int, default=2000)
    ap.add_argument("--lobs", type=int, default=500)
    ap.add_argument("--oversized", type=int, default=3,
                    help="number of >1 MiB lob rows (quarantine/DLQ test)")
    ap.add_argument("--seed", type=int, default=20260627, help="RNG seed")
    args = ap.parse_args()

    random.seed(args.seed)

    if not args.yes:
        log(f"[plan] would DROP+CREATE schema `{SCHEMA}` on the source and seed: "
            f"{args.parents} parents, {args.children} children, {args.lobs} clean "
            f"lobs, {args.oversized} OVERSIZED lobs, + typetest_loud (TINYINT(1)=2) "
            f"+ typetest_spatial (POINT, document-only). Use --yes to run.")
        return 0

    conn = connect(None)
    cur = conn.cursor()
    log(f"DROP + CREATE DATABASE {SCHEMA} ...")
    cur.execute(f"DROP DATABASE IF EXISTS {SCHEMA}")
    conn.commit()
    cur.execute(
        f"CREATE DATABASE {SCHEMA} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
    )
    conn.commit()
    cur.execute(f"USE {SCHEMA}")
    cur.execute("SET SESSION foreign_key_checks = 0")
    cur.execute("SET SESSION unique_checks = 0")
    for stmt in _DDL:
        cur.execute(stmt)
    conn.commit()
    log("Schema + 5 tables created.")

    parent_ids = seed_clean(
        cur, conn, n_parents=args.parents, n_children=args.children,
        n_lobs=args.lobs,
    )
    oversized_pks = seed_oversized_lobs(cur, conn, parent_ids, args.oversized)
    loud_pk = seed_loud(cur, conn)

    failrows = {
        "schema": SCHEMA,
        "oversized_lob_ids": oversized_pks,    # typetest_lob.lob_id (quarantine/DLQ)
        "loud_loud_id": loud_pk,               # typetest_loud.id (table-fatal)
        "oversize_bytes": _OVERSIZE_BYTES,
        "note": "oversized_lob_ids must be ABSENT on the DSQL target (quarantine/DLQ); "
                "loud row makes typetest_loud table-fatal in Full Load.",
    }
    with open(FAILROWS_FILE, "w", encoding="utf-8") as f:
        json.dump(failrows, f, indent=2)
    log(f"Wrote intentional-failure PKs -> {FAILROWS_FILE}")

    # Quick sanity counts.
    for t in ("typetest_parent", "typetest_child", "typetest_lob", "typetest_loud"):
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        log(f"  {t}: {cur.fetchone()[0]} rows")
    conn.close()
    log("Seed complete. ✓")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
