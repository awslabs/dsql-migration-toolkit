#!/usr/bin/env python3
"""Random CDC workload generator for the ``migration_typetest`` schema.

Standalone operational utility (NOT part of the shipped tool) for validating a
Full Load + CDC migration end to end: every ``--interval`` seconds it runs one
randomly chosen INSERT / UPDATE / DELETE against the parent -> child / lob tables
of ``migration_typetest`` (MySQL / Aurora MySQL), so a live CDC pipeline has a
steady, varied stream of row changes to replicate into Aurora DSQL.

It is referential-integrity aware: child / lob inserts pick a real existing
parent_id; deletes target leaf rows (child / lob) so a random DELETE never blocks
on a foreign key. The AUTO_INCREMENT PK and the generated column (``line_total``)
are never written. The steady loop only produces CLEAN, in-range values -- the
intentional oversized-LOB rows are seeded once by seed_migration_typetest.py and,
for the CDC DLQ test, injected on demand with ``--oversized-once``.

Connection reuses the repo ``.env`` (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD), exactly
like scripts/cdc_workload_customers_sample_new.py. Schema defaults to
``migration_typetest`` (override with ``--schema``).

Safety: WRITES to the source; only touches ``migration_typetest``; no DDL, never
drops anything. Stop with Ctrl-C.

Usage:
    python scripts/cdc_workload_migration_typetest.py                 # 10s loop
    python scripts/cdc_workload_migration_typetest.py --interval 2
    python scripts/cdc_workload_migration_typetest.py --once
    python scripts/cdc_workload_migration_typetest.py --dry-run
    python scripts/cdc_workload_migration_typetest.py --weights i=3,u=2,d=1
    python scripts/cdc_workload_migration_typetest.py --oversized-once  # one >1 MiB INSERT -> DLQ test
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


_ENUM = ("alpha", "beta", "gamma", "delta")
_SET = ("read", "write", "exec", "admin")
_WORDS = ("alpha", "Bravo", "charlie", "Delta", "echo", "Foxtrot", "GOLF", "hotel")
_OVERSIZE_BYTES = int(1.5 * 1024 * 1024)


def _now() -> dt.datetime:
    return dt.datetime.now()


def _rand_text(n: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits + " ", k=n))


def _rand_set() -> str:
    k = random.randint(0, len(_SET))
    return ",".join(sorted(random.sample(_SET, k), key=_SET.index)) if k else ""


class Workload:
    """Picks and executes a single random INSERT/UPDATE/DELETE per call."""

    def __init__(self, conn, schema: str, *, dry_run: bool = False, op_log=None) -> None:
        self.conn = conn
        self.s = schema
        self.dry_run = dry_run
        self._op_log = op_log

    def _q(self, table: str) -> str:
        return f"`{self.s}`.`{table}`"

    def _scalar(self, sql: str, args=()):
        cur = self.conn.cursor()
        cur.execute(sql, args)
        row = cur.fetchone()
        return row[0] if row else None

    def _rand_id(self, table: str, pk: str):
        """A random existing PK value via a cheap random offset (no full scan)."""
        mx = self._scalar(f"SELECT MAX(`{pk}`) FROM {self._q(table)}")
        if mx is None:
            return None
        for _ in range(6):
            guess = random.randint(1, int(mx))
            got = self._scalar(
                f"SELECT `{pk}` FROM {self._q(table)} WHERE `{pk}` >= %s "
                f"ORDER BY `{pk}` LIMIT 1",
                (guess,),
            )
            if got is not None:
                return got
        return self._scalar(f"SELECT `{pk}` FROM {self._q(table)} ORDER BY `{pk}` LIMIT 1")

    def _next_seq(self, parent_id) -> int:
        """Next free seq_no for a parent (keeps uq_parent_seq unique)."""
        mx = self._scalar(
            f"SELECT MAX(seq_no) FROM {self._q('typetest_child')} WHERE parent_id=%s",
            (parent_id,),
        )
        return int(mx or 0) + 1

    def _exec(self, label: str, sql: str, args=()):
        if self.dry_run:
            print(f"  DRY  {label}: {sql.strip()[:80]}...  ({len(args)} args)")
            return label
        cur = self.conn.cursor()
        cur.execute(sql, args)
        self.conn.commit()
        rid = cur.lastrowid
        detail = f" (id={rid})" if rid else f" ({cur.rowcount} row)"
        print(f"  OK   {label}{detail}")
        if self._op_log is not None:
            op, _, table = label.partition(" ")
            pk = rid if op == "INSERT" else (args[-1] if args else None)
            if not (op == "UPDATE" and cur.rowcount == 0):
                self._op_log.write(json.dumps({
                    "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "op": op, "table": table, "pk": pk,
                }) + "\n")
                self._op_log.flush()
        return label

    # -- INSERTs --
    def insert_parent(self):
        now = _now()
        return self._exec(
            "INSERT typetest_parent",
            f"INSERT INTO {self._q('typetest_parent')} "
            "(c_tinyint, c_tinyint_u, c_bool, c_smallint, c_smallint_u, c_mediumint, "
            "c_mediumint_u, c_int, c_int_u, c_bigint, c_bigint_u, c_decimal, "
            "c_decimal_big, c_float, c_double, c_bit, c_char, c_varchar, c_varchar_ci, "
            "c_date, c_datetime, c_datetime6, c_time, c_year, c_enum, c_set, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "%s,%s,%s,%s,%s,%s,%s,%s)",
            (random.randint(-128, 127), random.randint(0, 255), random.randint(0, 1),
             random.randint(-32768, 32767), random.randint(0, 65535),
             random.randint(-8388608, 8388607), random.randint(0, 16777215),
             random.randint(-2147483648, 2147483647), random.randint(0, 4294967295),
             random.randint(-(2**63), 2**63 - 1), random.randint(0, 2**64 - 1),
             round(random.uniform(0, 99999999), 2), random.randint(0, 10**38 - 1),
             round(random.uniform(-1e6, 1e6), 3), random.uniform(-1e12, 1e12),
             random.randint(0, 255), _rand_text(10), _rand_text(random.randint(5, 200)),
             random.choice(_WORDS),
             (now - dt.timedelta(days=random.randint(0, 3000))).date(),
             now - dt.timedelta(seconds=random.randint(0, 10**7)),
             now - dt.timedelta(microseconds=random.randint(0, 10**9)),
             dt.timedelta(seconds=random.randint(0, 86399)),
             random.randint(1990, 2030), random.choice(_ENUM), _rand_set(), now),
        )

    def insert_child(self):
        pid = self._rand_id("typetest_parent", "parent_id")
        if pid is None:
            return self.insert_parent()
        payload = json.dumps({"k": random.choice(_WORDS), "n": random.randint(0, 1000)})
        return self._exec(
            "INSERT typetest_child",
            f"INSERT INTO {self._q('typetest_child')} "
            "(parent_id, seq_no, qty, unit_price, payload, note_ci, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (pid, self._next_seq(pid), random.randint(1, 20),
             round(random.uniform(1, 999), 2), payload, random.choice(_WORDS), _now()),
        )

    def insert_lob(self):
        pid = self._rand_id("typetest_parent", "parent_id")
        if pid is None:
            return self.insert_parent()
        cjson = json.dumps({"tags": random.sample(_WORDS, 3),
                           "score": round(random.uniform(0, 100), 2)})
        return self._exec(
            "INSERT typetest_lob",
            f"INSERT INTO {self._q('typetest_lob')} "
            "(parent_id, c_tinytext, c_text, c_mediumtext, c_longtext, c_tinyblob, "
            "c_blob, c_mediumblob, c_longblob, c_binary, c_varbinary, c_json, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (pid, _rand_text(50), _rand_text(random.randint(100, 4000)),
             _rand_text(random.randint(100, 8000)), _rand_text(random.randint(100, 8000)),
             _rand_text(50).encode(), _rand_text(500).encode(),
             _rand_text(1000).encode(), _rand_text(1000).encode(),
             os.urandom(16), os.urandom(random.randint(8, 200)), cjson, _now()),
        )

    def insert_oversized_lob(self):
        """A >1 MiB c_longtext INSERT -- exercises the CDC DLQ path on purpose."""
        pid = self._rand_id("typetest_parent", "parent_id")
        if pid is None:
            return None
        big = "X" * _OVERSIZE_BYTES
        return self._exec(
            "INSERT typetest_lob",
            f"INSERT INTO {self._q('typetest_lob')} "
            "(parent_id, c_text, c_longtext, c_json, created_at) "
            "VALUES (%s,%s,%s,%s,%s)",
            (pid, "oversized-cdc-marker", big, json.dumps({"oversized": True}), _now()),
        )

    # -- UPDATEs --
    def update_parent(self):
        pid = self._rand_id("typetest_parent", "parent_id")
        if pid is None:
            return None
        return self._exec(
            "UPDATE typetest_parent",
            f"UPDATE {self._q('typetest_parent')} SET c_int=%s, c_varchar=%s, "
            "c_enum=%s, c_bool=%s WHERE parent_id=%s",
            (random.randint(-2147483648, 2147483647), _rand_text(40),
             random.choice(_ENUM), random.randint(0, 1), pid),
        )

    def update_child(self):
        cid = self._rand_id("typetest_child", "child_id")
        if cid is None:
            return None
        return self._exec(
            "UPDATE typetest_child",
            f"UPDATE {self._q('typetest_child')} SET qty=%s, unit_price=%s, "
            "note_ci=%s WHERE child_id=%s",
            (random.randint(1, 50), round(random.uniform(1, 999), 2),
             random.choice(_WORDS), cid),
        )

    def update_lob(self):
        lid = self._rand_id("typetest_lob", "lob_id")
        if lid is None:
            return None
        return self._exec(
            "UPDATE typetest_lob",
            f"UPDATE {self._q('typetest_lob')} SET c_text=%s, c_json=%s WHERE lob_id=%s",
            (_rand_text(random.randint(100, 2000)),
             json.dumps({"updated": True, "w": random.choice(_WORDS)}), lid),
        )

    # -- DELETEs (leaf rows only) --
    def delete_child(self):
        cid = self._rand_id("typetest_child", "child_id")
        if cid is None:
            return None
        return self._exec(
            "DELETE typetest_child",
            f"DELETE FROM {self._q('typetest_child')} WHERE child_id=%s", (cid,),
        )

    def delete_lob(self):
        lid = self._rand_id("typetest_lob", "lob_id")
        if lid is None:
            return None
        return self._exec(
            "DELETE typetest_lob",
            f"DELETE FROM {self._q('typetest_lob')} WHERE lob_id=%s", (lid,),
        )

    # -- dispatch --
    def operations(self):
        return {
            "i": [self.insert_parent, self.insert_child, self.insert_lob],
            "u": [self.update_parent, self.update_child, self.update_lob],
            "d": [self.delete_child, self.delete_lob],
        }

    def run_one(self, weights: dict[str, int]):
        ops = self.operations()
        kinds = [k for k in ("i", "u", "d") for _ in range(weights.get(k, 0))]
        kind = random.choice(kinds or ["i"])
        fn = random.choice(ops[kind])
        try:
            result = fn()
            if result is None:
                print("  SKIP (no eligible parent/child row yet)")
            return result
        except Exception as exc:  # noqa: BLE001 - keep the loop alive
            if not self.dry_run:
                self.conn.rollback()
            print(f"  ERR  {fn.__name__}: {str(exc).splitlines()[0]}")
            return None


def parse_weights(spec: str) -> dict[str, int]:
    weights = {"i": 3, "u": 2, "d": 1}
    if spec:
        for part in spec.split(","):
            k, _, v = part.partition("=")
            k = k.strip().lower()
            if k in ("i", "u", "d") and v.strip().isdigit():
                weights[k] = int(v)
    return weights


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--schema", default=_cfg("CDC_WORKLOAD_SCHEMA", "migration_typetest"))
    ap.add_argument("--interval", type=float, default=10.0, help="seconds between ops")
    ap.add_argument("--once", action="store_true", help="run a single op and exit")
    ap.add_argument("--oversized-once", action="store_true",
                    help="insert ONE >1 MiB lob row and exit (CDC DLQ test)")
    ap.add_argument("--dry-run", action="store_true", help="print SQL, do not execute")
    ap.add_argument("--weights", default="", help="op mix, e.g. i=3,u=2,d=1")
    ap.add_argument("--op-log", default="", help="append one JSONL {ts,op,table,pk} per op")
    args = ap.parse_args()

    host, user, pwd = _cfg("DB_HOST"), _cfg("DB_USER"), _cfg("DB_PASSWORD")
    port = int(_cfg("DB_PORT", "3306"))
    if not host or not user:
        print("ERROR: DB_HOST / DB_USER not set (in .env or env).", file=sys.stderr)
        return 2

    weights = parse_weights(args.weights)
    conn = pymysql.connect(host=host, port=port, user=user, password=pwd,
                           autocommit=False, connect_timeout=10)
    op_log = open(args.op_log, "a", encoding="utf-8") if args.op_log else None
    wl = Workload(conn, args.schema, dry_run=args.dry_run, op_log=op_log)

    if args.oversized_once:
        print(f"Injecting ONE oversized (~1.5 MiB) lob INSERT into {args.schema} "
              "(should land in the CDC DLQ).")
        wl.insert_oversized_lob()
        conn.close()
        if op_log:
            op_log.close()
        return 0

    print(f"CDC workload -> {host}:{port} schema={args.schema} "
          f"weights(i/u/d)={weights['i']}/{weights['u']}/{weights['d']} "
          f"interval={args.interval}s {'[DRY-RUN]' if args.dry_run else ''}")
    print("Ctrl-C to stop.\n")

    n = 0
    try:
        while True:
            n += 1
            print(f"[{dt.datetime.now():%H:%M:%S}] op #{n}")
            wl.run_one(weights)
            if args.once:
                break
            import time
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {n} op(s).")
    finally:
        conn.close()
        if op_log is not None:
            op_log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

