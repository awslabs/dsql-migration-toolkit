#!/usr/bin/env python3
"""Zero-data-loss reconciliation between source MySQL and target Aurora DSQL.

Stronger than a row COUNT(*): for every table it loads the FULL single-column
primary-key set from both sides and reports, per table:

  * source / target COUNT(*),
  * missing_on_target  -- PKs in source but NOT in target  (LOST inserts/rows),
  * extra_on_target    -- PKs in target but NOT in source  (stale: a source
                          DELETE that never reached the target).

A migration is "zero data loss" when, after the source has stopped changing and
CDC has drained, every table has missing_on_target == 0 AND extra_on_target == 0.
Exit code is 0 only when every checked table is fully consistent, 1 otherwise --
so it can gate a repeatable test.

Optionally cross-checks an op-log (the ground-truth JSONL the workload writes via
``--op-log``): of every INSERTed pk, how many are missing on target; of every
DELETEd pk, how many still linger on target. This pins data loss to the exact
operations that were lost, independent of the COUNT comparison.

Read-only. Connection settings come from .env (DB_HOST/DB_PORT/DB_USER/
DB_PASSWORD for source; TARGET_ENDPOINT for DSQL), reusing scripts/compare_rows.py.

Usage:
    set -a; source .env; set +a
    .venv/bin/python scripts/cdc_consistency_check.py
    .venv/bin/python scripts/cdc_consistency_check.py --op-log /tmp/cdc_ops.jsonl
    .venv/bin/python scripts/cdc_consistency_check.py --json   # machine-readable
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# Reuse the proven connection + schema-resolution helpers from compare_rows.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from compare_rows import source_connect, target_connect, source_stats, target_stats  # noqa: E402

DEFAULT_SCHEMA = os.environ.get("CDC_WORKLOAD_SCHEMA", "customers_sample_new")

# The 11 customers_sample_new tables (each has a single integer PK).
DEFAULT_TABLES = [
    "categories", "countries", "regions", "suppliers", "products",
    "customers", "customer_addresses", "orders", "order_items",
    "payments", "product_reviews",
]


def _src_pk_set(conn, schema, table, pk) -> set:
    with conn.cursor() as cur:
        cur.execute(f"SELECT `{pk}` FROM `{schema}`.`{table}`")
        return {r[0] for r in cur.fetchall()}


def _tgt_pk_set(conn, sch, table, pk) -> set:
    cur = conn.cursor()
    cur.execute(f'SELECT "{pk}" FROM "{sch}"."{table}"')
    return {r[0] for r in cur.fetchall()}


def reconcile(schema, tables) -> dict:
    src = source_connect()
    tgt = target_connect()
    result = {"schema": schema, "tables": {}, "all_consistent": True}
    try:
        for table in tables:
            s = source_stats(src, schema, table)
            if not s.get("exists"):
                result["tables"][table] = {"status": "SOURCE_MISSING"}
                result["all_consistent"] = False
                continue
            pk_cols = s.get("pk") or []
            if len(pk_cols) != 1:
                result["tables"][table] = {"status": "NO_SINGLE_PK", "pk": pk_cols}
                continue
            pk = pk_cols[0]
            t = target_stats(tgt, schema, table, [pk])
            if not t.get("exists"):
                result["tables"][table] = {"status": "TARGET_MISSING",
                                           "source_count": s["count"]}
                result["all_consistent"] = False
                continue
            sch = t.get("schema", schema)
            s_set = _src_pk_set(src, schema, table, pk)
            t_set = _tgt_pk_set(tgt, sch, table, pk)
            missing = sorted(s_set - t_set)   # lost inserts
            extra = sorted(t_set - s_set)     # stale (delete not applied)
            ok = (not missing) and (not extra)
            if not ok:
                result["all_consistent"] = False
            result["tables"][table] = {
                "status": "CONSISTENT" if ok else "INCONSISTENT",
                "pk": pk,
                "source_count": len(s_set),
                "target_count": len(t_set),
                "missing_on_target": len(missing),
                "extra_on_target": len(extra),
                "missing_sample": missing[:25],
                "extra_sample": extra[:25],
                "target_schema": sch,
            }
        return result
    finally:
        for c in (src, tgt):
            try:
                c.close()
            except Exception:
                pass


def crosscheck_op_log(path, schema, tables) -> dict:
    """Compare the workload's ground-truth op-log against the live target.

    INSERTed pk should EXIST on target; DELETEd pk should NOT. Reports any
    violation -- the exact operations whose effect did not reach the target.
    """
    inserts: dict[str, set] = {t: set() for t in tables}
    deletes: dict[str, set] = {t: set() for t in tables}
    n = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            t, op, pk = rec.get("table"), rec.get("op"), rec.get("pk")
            if t not in inserts or pk is None:
                continue
            n += 1
            if op == "INSERT":
                inserts[t].add(pk)
                deletes[t].discard(pk)
            elif op == "DELETE":
                deletes[t].add(pk)
                inserts[t].discard(pk)
    tgt = target_connect()
    out = {"op_log": path, "ops": n, "tables": {}, "all_applied": True}
    try:
        for t in tables:
            if not inserts[t] and not deletes[t]:
                continue
            # resolve target schema once per table
            ts = target_stats(tgt, schema, t, [])
            sch = ts.get("schema", schema)
            # need pk column name -> read from source-side default map via a probe
            cur = tgt.cursor()
            cur.execute(
                "SELECT column_name FROM information_schema.key_column_usage "
                "WHERE table_schema=%s AND table_name=%s AND constraint_name='PRIMARY' "
                "ORDER BY ordinal_position", (sch, t))
            pkrows = [r[0] for r in cur.fetchall()]
            if len(pkrows) != 1:
                continue
            pk = pkrows[0]
            t_set = _tgt_pk_set(tgt, sch, t, pk)
            lost_inserts = sorted(inserts[t] - t_set)      # should exist, absent
            stale_deletes = sorted(deletes[t] & t_set)     # should be gone, present
            ok = (not lost_inserts) and (not stale_deletes)
            if not ok:
                out["all_applied"] = False
            out["tables"][t] = {
                "logged_inserts": len(inserts[t]),
                "logged_deletes": len(deletes[t]),
                "lost_inserts": len(lost_inserts),
                "stale_deletes": len(stale_deletes),
                "lost_insert_sample": lost_inserts[:25],
                "stale_delete_sample": stale_deletes[:25],
            }
        return out
    finally:
        try:
            tgt.close()
        except Exception:
            pass


def _print_report(rep: dict) -> None:
    print(f"{'TABLE':<22} {'SOURCE':>9} {'TARGET':>9} {'MISSING':>8} {'EXTRA':>7}  RESULT")
    print("-" * 72)
    for table, r in rep["tables"].items():
        st = r["status"]
        if st in ("CONSISTENT", "INCONSISTENT"):
            mark = "OK" if st == "CONSISTENT" else "FAIL"
            print(f"{table:<22} {r['source_count']:>9} {r['target_count']:>9} "
                  f"{r['missing_on_target']:>8} {r['extra_on_target']:>7}  {mark}")
            if st == "INCONSISTENT":
                if r["missing_sample"]:
                    print(f"{'':<22}   missing pk (lost): {r['missing_sample']}")
                if r["extra_sample"]:
                    print(f"{'':<22}   extra pk (stale delete): {r['extra_sample']}")
        else:
            print(f"{table:<22} {'-':>9} {'-':>9} {'-':>8} {'-':>7}  {st}")
    print("-" * 72)
    print("VERDICT:", "ZERO DATA LOSS ✓" if rep["all_consistent"]
          else "DATA LOSS / DRIFT ✗")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--schema", default=DEFAULT_SCHEMA)
    ap.add_argument("--tables", nargs="*", default=DEFAULT_TABLES,
                    help="table names (unqualified); default = the 11 tables")
    ap.add_argument("--op-log", default="", help="cross-check this workload op-log")
    ap.add_argument("--json", action="store_true", help="emit JSON only")
    args = ap.parse_args()

    rep = reconcile(args.schema, args.tables)
    cross = None
    if args.op_log:
        cross = crosscheck_op_log(args.op_log, args.schema, args.tables)

    if args.json:
        print(json.dumps({"reconcile": rep, "op_log_crosscheck": cross}, indent=2))
    else:
        _print_report(rep)
        if cross is not None:
            print(f"\nOP-LOG CROSS-CHECK ({cross['ops']} ops from {cross['op_log']})")
            print(f"{'TABLE':<22} {'INS':>6} {'DEL':>6} {'LOST_INS':>9} {'STALE_DEL':>10}")
            print("-" * 60)
            for t, r in cross["tables"].items():
                print(f"{t:<22} {r['logged_inserts']:>6} {r['logged_deletes']:>6} "
                      f"{r['lost_inserts']:>9} {r['stale_deletes']:>10}")
                if r["lost_insert_sample"]:
                    print(f"{'':<22}   lost inserts: {r['lost_insert_sample']}")
                if r["stale_delete_sample"]:
                    print(f"{'':<22}   stale deletes: {r['stale_delete_sample']}")
            print("CROSS-CHECK:", "ALL OPS APPLIED ✓" if cross["all_applied"]
                  else "SOME OPS NOT APPLIED ✗")

    ok = rep["all_consistent"] and (cross is None or cross["all_applied"])
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
