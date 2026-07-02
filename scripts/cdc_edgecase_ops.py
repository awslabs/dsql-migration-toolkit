#!/usr/bin/env python3
"""Inject SPECIFIC, individually-verifiable CDC edge operations into the source.

Where ``cdc_workload_<schema>.py`` drives a steady RANDOM INSERT/UPDATE/DELETE
stream (throughput + soak), this injects the NAMED corner cases a random loop
almost never produces but a real MySQL -> Aurora DSQL CDC pipeline must handle
correctly. Each op is small, deterministic, and appended to an op-log JSONL in the
exact ``{ts, op, table, pk}`` shape ``scripts/cdc_consistency_check.py --op-log``
consumes, so after CDC drains you can pin the exact per-op landing on the target.

The injected edge operations (``--op``, default ``all``):

  pk-change   UPDATE a row's PRIMARY KEY (pk A -> new pk B). MySQL emits one UPDATE
              binlog event, but Debezium turns a key change into a DELETE(A) +
              INSERT(B) pair (the key is the Kafka message key). The op-log records
              exactly that -- DELETE(A) then INSERT(B) -- so the check verifies A is
              GONE and B is PRESENT on the target. A classic CDC correctness trap.
  churn       Same-PK I -> D -> I inside one connection: INSERT explicit pk P,
              DELETE P, re-INSERT P with different values. Tests that the sink
              applies the ordered stream to the right final state (present, latest
              values), not a coalesced/incorrect one. Logs the final INSERT(P).
  transient   INSERT explicit pk P then DELETE P inside ONE transaction (net-absent).
              The sink receives an upsert followed by a delete for P -- exercising a
              DELETE whose row may not (yet) be on the target. Logs INSERT then
              DELETE(P); the check expects P ABSENT.
  update-storm  Many rapid UPDATEs to ONE row's non-key columns in a tight loop
              (--storm, default 50). Under DSQL optimistic concurrency the sink
              must converge to the LAST value without loss. Logs one UPDATE(P).
  big-txn     One COMMIT inserting --rows (default 2000) rows -- larger than a single
              MSK message / DSQL's 3000-row txn cap -- so the sink must split it into
              multiple bounded write transactions. Logs an INSERT per new pk.
  alter       Schema change mid-CDC: ALTER TABLE ADD COLUMN on the source, then
              INSERT a row USING the new column. CDC replicates ROW DATA, not DDL, so
              the new-shape row cannot be applied to the unchanged target and is
              expected to land in the DLQ (docs §8 "CDC replicates data, not DDL").
              This is a REPORT/DLQ op -- it deliberately produces a row that should
              NOT reconcile onto the current target; it is NOT written to the op-log
              (whose contract is ops that MUST apply). ADD COLUMN is dropped again
              at the end unless --keep-alter.

Generic by construction: it discovers a table's columns from
``information_schema`` and clones an existing row into new explicit PKs, skipping
the PK and any GENERATED/virtual column, so it works on ANY table with a single
integer primary key -- no per-schema generator. New PKs are allocated far above
``MAX(pk)`` (+ a large offset) so they never collide with a concurrent workload's
AUTO_INCREMENT stream.

Connection reuses the repo ``.env`` (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD), like
the other CDC scripts. Safety: WRITES to the source; only the chosen schema/table;
the only DDL is an idempotent ADD/DROP COLUMN for the ``alter`` op. Stop anytime.

Usage:
    set -a; source .env; set +a
    python scripts/cdc_edgecase_ops.py --dry-run                 # print the SQL, no writes
    python scripts/cdc_edgecase_ops.py --op all --op-log /tmp/cdc_ops.jsonl
    python scripts/cdc_edgecase_ops.py --op pk-change --table customers_sample_new.orders
    python scripts/cdc_edgecase_ops.py --op big-txn --rows 5000
    # then, after CDC drains:
    python scripts/cdc_consistency_check.py --op-log /tmp/cdc_ops.jsonl
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys

import pymysql

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))  # for _e2e_tables

# The offset that separates injected explicit PKs from the AUTO_INCREMENT stream a
# concurrent workload produces -- large enough that the two never overlap.
_PK_OFFSET = 1_000_000_000

# Marker column added/removed by the `alter` op (idempotent, clearly named).
_ALTER_COL = "_cdc_edge_probe"

_ALL_OPS = ("pk-change", "churn", "transient", "update-storm", "big-txn", "alter")


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


def _default_table(schema: str) -> str:
    """A sensible default LEAF table for a known schema (safe to churn PKs on).

    Leaf tables have no children pointing at them, so re-keying / deleting a row
    never orphans another table's FK. Falls back to the schema's last registered
    table (the FK-ordered list ends at a leaf) for an unknown schema.
    """
    try:
        from _e2e_tables import tables_for
        tabs = tables_for(schema)
        # Prefer a well-known leaf if present; else the last (leaf-most) table.
        for leaf in ("product_reviews", "payments", "order_items", "typetest_lob"):
            if leaf in tabs:
                return leaf
        return tabs[-1]
    except Exception:  # noqa: BLE001 - unknown schema: caller must pass --table
        return ""


class Injector:
    """Discovers a table's shape once, then injects each named edge operation."""

    def __init__(self, conn, schema: str, table: str, *, dry_run: bool, op_log=None):
        self.conn = conn
        self.s = schema
        self.t = table
        self.dry_run = dry_run
        self._op_log = op_log
        self.pk, self.insert_cols = self._describe()
        self._alloc = 0
        self._pk_base = None  # cached MAX(pk); read once by _new_pk (see there)

    def _q(self) -> str:
        return f"`{self.s}`.`{self.t}`"

    def _scalar(self, sql: str, args=()):
        cur = self.conn.cursor()
        cur.execute(sql, args)
        row = cur.fetchone()
        return row[0] if row else None

    def _describe(self):
        """Return (pk_column, insertable_columns) from information_schema.

        Insertable columns exclude the PK (we set it explicitly for clones) and any
        GENERATED / virtual column (writing those is an error). Requires a single-
        column PK -- the whole point is deterministic per-PK CDC verification.
        """
        cur = self.conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.key_column_usage "
            "WHERE table_schema=%s AND table_name=%s AND constraint_name='PRIMARY' "
            "ORDER BY ordinal_position", (self.s, self.t))
        pks = [r[0] for r in cur.fetchall()]
        if len(pks) != 1:
            raise SystemExit(
                f"{self.s}.{self.t}: PK is {pks or 'missing'} — this injector needs a "
                "single-column integer PK for deterministic per-PK CDC verification.")
        pk = pks[0]
        cur.execute(
            "SELECT column_name, extra, data_type FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
            (self.s, self.t))
        insert_cols, text_cols = [], []
        for name, extra, data_type in cur.fetchall():
            extra = (extra or "").lower()
            if "generated" in extra or "virtual" in extra or "stored" in extra:
                continue  # generated column: never write it
            if name == pk:
                continue  # PK set explicitly for clones
            insert_cols.append(name)
            if data_type in ("varchar", "char", "text", "tinytext",
                             "mediumtext", "longtext"):
                text_cols.append(name)
        self._text_cols = text_cols
        return pk, insert_cols

    def _existing_pk(self):
        """A real existing PK to clone from (max is fine; small tables OK)."""
        return self._scalar(f"SELECT MAX(`{self.pk}`) FROM {self._q()}")

    def _new_pk(self) -> int:
        """Allocate a fresh explicit PK far above the AUTO_INCREMENT range.

        The ``MAX(pk)`` base is read ONCE and cached, then a monotonic counter is
        added -- so allocating N PKs (e.g. big-txn's 2000 rows) costs one index seek,
        not N full ``MAX`` scans (which on a large table made big-txn effectively
        hang). Newly-inserted rows in this run are not yet visible to a fresh
        ``MAX`` anyway (same uncommitted txn), so caching the base is also correct.
        """
        if self._pk_base is None:
            self._pk_base = int(self._scalar(f"SELECT MAX(`{self.pk}`) FROM {self._q()}") or 0)
        self._alloc += 1
        return max(self._pk_base, 0) + _PK_OFFSET + self._alloc

    def _log_op(self, op: str, pk) -> None:
        if self._op_log is None:
            return
        self._op_log.write(json.dumps({
            "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
            "op": op, "table": self.t, "pk": pk,
        }) + "\n")
        self._op_log.flush()

    def _clone_sql(self, new_pk: int, src_pk):
        """Build an INSERT that clones ``src_pk``'s row into ``new_pk``.

        ``INSERT INTO t (pk, cols...) SELECT :new_pk, cols... FROM t WHERE pk=:src``
        -- generic over the table's shape (generated cols already excluded).
        """
        col_list = ", ".join(f"`{c}`" for c in [self.pk] + self.insert_cols)
        select_cols = ", ".join(["%s"] + [f"`{c}`" for c in self.insert_cols])
        sql = (f"INSERT INTO {self._q()} ({col_list}) "
               f"SELECT {select_cols} FROM {self._q()} WHERE `{self.pk}`=%s")
        return sql, (new_pk, src_pk)

    def _exec(self, label: str, sql: str, args=()):
        if self.dry_run:
            print(f"  DRY  {label}: {' '.join(sql.split())[:90]}  ({len(args)} args)")
            return
        cur = self.conn.cursor()
        cur.execute(sql, args)
        print(f"  OK   {label}  ({cur.rowcount} row)")

    # -- edge operations ----------------------------------------------------- #
    def op_pk_change(self):
        """UPDATE a row's PK A -> B; log DELETE(A) + INSERT(B) (Debezium semantics)."""
        old = self._existing_pk()
        if old is None:
            print("  SKIP pk-change: table is empty")
            return
        new = self._new_pk()
        self._exec(f"pk-change UPDATE {self.pk} {old}->{new}",
                   f"UPDATE {self._q()} SET `{self.pk}`=%s WHERE `{self.pk}`=%s",
                   (new, old))
        if not self.dry_run:
            self.conn.commit()
        # Debezium turns a key change into delete(old-key) + insert(new-key).
        self._log_op("DELETE", old)
        self._log_op("INSERT", new)

    def op_churn(self):
        """Same-PK I -> D -> I; log only the final INSERT (must be present, latest)."""
        src = self._existing_pk()
        if src is None:
            print("  SKIP churn: table is empty")
            return
        pk = self._new_pk()
        sql, args = self._clone_sql(pk, src)
        self._exec(f"churn INSERT {pk}", sql, args)
        self._exec(f"churn DELETE {pk}",
                   f"DELETE FROM {self._q()} WHERE `{self.pk}`=%s", (pk,))
        # Re-insert same PK, then bump a text column so the final value is distinct.
        self._exec(f"churn re-INSERT {pk}", sql, args)
        if self._text_cols:
            col = self._text_cols[0]
            self._exec(f"churn UPDATE {col} {pk}",
                       f"UPDATE {self._q()} SET `{col}`=%s WHERE `{self.pk}`=%s",
                       (f"churn-final-{pk}", pk))
        if not self.dry_run:
            self.conn.commit()
        self._log_op("INSERT", pk)  # net effect: present with the final value

    def op_transient(self):
        """INSERT then DELETE the same PK in ONE txn; log both (net-absent target)."""
        src = self._existing_pk()
        if src is None:
            print("  SKIP transient: table is empty")
            return
        pk = self._new_pk()
        sql, args = self._clone_sql(pk, src)
        self._exec(f"transient INSERT {pk}", sql, args)
        self._exec(f"transient DELETE {pk}",
                   f"DELETE FROM {self._q()} WHERE `{self.pk}`=%s", (pk,))
        if not self.dry_run:
            self.conn.commit()
        self._log_op("INSERT", pk)
        self._log_op("DELETE", pk)  # check expects P ABSENT

    def op_update_storm(self, n: int):
        """Rapid UPDATEs to one row's text column; log one UPDATE (last must win)."""
        if not self._text_cols:
            print("  SKIP update-storm: no text column to churn")
            return
        src = self._existing_pk()
        if src is None:
            print("  SKIP update-storm: table is empty")
            return
        pk = self._new_pk()
        sql, args = self._clone_sql(pk, src)
        self._exec(f"storm seed INSERT {pk}", sql, args)
        col = self._text_cols[0]
        upd_sql = f"UPDATE {self._q()} SET `{col}`=%s WHERE `{self.pk}`=%s"
        if self.dry_run:
            print(f"  DRY  storm UPDATE (x{n}): {' '.join(upd_sql.split())[:80]}")
            return
        final = ""
        cur = self.conn.cursor()
        for i in range(n):
            final = f"storm-{pk}-{i}"
            cur.execute(upd_sql, (final, pk))
        self.conn.commit()
        print(f"  OK   update-storm: {n} UPDATEs on {self.pk}={pk}, "
              f"final `{col}`={final!r}")
        self._log_op("INSERT", pk)

    def op_big_txn(self, rows: int):
        """One COMMIT inserting ``rows`` cloned rows; log an INSERT per new PK."""
        src = self._existing_pk()
        if src is None:
            print("  SKIP big-txn: table is empty")
            return
        pks = []
        for _ in range(rows):
            pk = self._new_pk()
            sql, args = self._clone_sql(pk, src)
            if self.dry_run:
                if not pks:
                    print(f"  DRY  big-txn INSERT (x{rows}): "
                          f"{' '.join(sql.split())[:80]}")
            else:
                cur = self.conn.cursor()
                cur.execute(sql, args)
            pks.append(pk)
        if not self.dry_run:
            self.conn.commit()
            print(f"  OK   big-txn: {rows} rows in ONE commit "
                  f"({self.pk} {pks[0]}..{pks[-1]})")
        for pk in pks:
            self._log_op("INSERT", pk)

    def op_alter(self, keep: bool):
        """ADD COLUMN, INSERT a row using it (expected DLQ), then DROP unless kept."""
        src = self._existing_pk()
        if src is None:
            print("  SKIP alter: table is empty")
            return
        # MySQL (unlike MariaDB) has no ALTER TABLE ... ADD COLUMN IF NOT EXISTS,
        # so probe information_schema first and only add when absent (idempotent
        # without the non-standard clause).
        exists = self._scalar(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s "
            "AND table_name=%s AND column_name=%s", (self.s, self.t, _ALTER_COL))
        if not exists:
            add_sql = (f"ALTER TABLE {self._q()} "
                       f"ADD COLUMN `{_ALTER_COL}` VARCHAR(16) NULL")
            self._exec("alter ADD COLUMN", add_sql)
        else:
            print(f"  (column `{_ALTER_COL}` already present — skip ADD)")
        pk = self._new_pk()
        # Clone then set the new column, so the row's SHAPE now differs from the
        # target's -> CDC (row data, not DDL) can't apply it -> expected DLQ.
        sql, args = self._clone_sql(pk, src)
        self._exec(f"alter INSERT (new-shape) {pk}", sql, args)
        self._exec(f"alter SET {_ALTER_COL} {pk}",
                   f"UPDATE {self._q()} SET `{_ALTER_COL}`=%s WHERE `{self.pk}`=%s",
                   ("edge", pk))
        if not self.dry_run:
            self.conn.commit()
        print(f"  NOTE alter: pk={pk} was inserted with an extra column the target "
              "lacks; CDC replicates DATA not DDL, so this row is EXPECTED to land "
              "in the DLQ (not on the target). Not written to the op-log.")
        if keep:
            print(f"  NOTE alter: keeping `{_ALTER_COL}` (--keep-alter).")
            return
        # Likewise no DROP COLUMN IF EXISTS in MySQL — the column was just added, so
        # a plain DROP is safe here.
        self._exec("alter DROP COLUMN",
                   f"ALTER TABLE {self._q()} DROP COLUMN `{_ALTER_COL}`")
        if not self.dry_run:
            self.conn.commit()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--schema", default=_cfg("CDC_WORKLOAD_SCHEMA", "customers_sample_new"))
    ap.add_argument("--table", default="",
                    help="SCHEMA.TABLE or TABLE (default: a leaf table of --schema)")
    ap.add_argument("--op", action="append", choices=(*_ALL_OPS, "all"),
                    help="edge op to inject (repeatable). Default: all")
    ap.add_argument("--rows", type=int, default=2000, help="rows for big-txn (default 2000)")
    ap.add_argument("--storm", type=int, default=50, help="UPDATEs for update-storm (default 50)")
    ap.add_argument("--keep-alter", action="store_true",
                    help="leave the ADD COLUMN in place after the alter op")
    ap.add_argument("--op-log", default="", help="append {ts,op,table,pk} JSONL here")
    ap.add_argument("--dry-run", action="store_true", help="print SQL, do not execute")
    args = ap.parse_args()

    schema = args.schema
    table_arg = args.table or _default_table(schema)
    if not table_arg:
        print(f"ERROR: pass --table SCHEMA.TABLE ('{schema}' is not a known schema "
              "with a default leaf table).", file=sys.stderr)
        return 2
    if "." in table_arg:
        schema, table = table_arg.split(".", 1)
    else:
        table = table_arg

    ops = args.op or ["all"]
    if "all" in ops:
        ops = list(_ALL_OPS)

    host, user, pwd = _cfg("DB_HOST"), _cfg("DB_USER", "admin"), _cfg("DB_PASSWORD")
    port = int(_cfg("DB_PORT", "3306"))
    if not host or not pwd:
        print("ERROR: DB_HOST / DB_PASSWORD not set (in .env or env).", file=sys.stderr)
        return 2

    conn = pymysql.connect(host=host, port=port, user=user, password=pwd,
                           autocommit=False, connect_timeout=10, charset="utf8mb4")
    op_log = open(args.op_log, "a", encoding="utf-8") if args.op_log else None
    try:
        inj = Injector(conn, schema, table, dry_run=args.dry_run, op_log=op_log)
        log(f"CDC edge ops -> {host}:{port} {schema}.{table} (pk={inj.pk}) "
            f"ops={ops} {'[DRY-RUN]' if args.dry_run else ''}"
            + (f" op-log={args.op_log}" if args.op_log else ""))
        dispatch = {
            "pk-change": inj.op_pk_change,
            "churn": inj.op_churn,
            "transient": inj.op_transient,
            "update-storm": lambda: inj.op_update_storm(args.storm),
            "big-txn": lambda: inj.op_big_txn(args.rows),
            "alter": lambda: inj.op_alter(args.keep_alter),
        }
        for op in ops:
            print(f"--- {op} ---")
            try:
                dispatch[op]()
            except Exception as exc:  # noqa: BLE001 - one op can't abort the rest
                if not args.dry_run:
                    conn.rollback()
                print(f"  ERR  {op}: {str(exc).splitlines()[0]}")
        log("Done. After CDC drains, verify with: "
            f"scripts/cdc_consistency_check.py"
            + (f" --op-log {args.op_log}" if args.op_log else ""))
    finally:
        conn.close()
        if op_log is not None:
            op_log.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
