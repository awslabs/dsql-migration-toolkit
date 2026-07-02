#!/usr/bin/env python3
"""Random CDC workload generator for the ``customers_sample_new`` schema.

Standalone operational utility (NOT part of the shipped tool) for validating a
Full Load + CDC migration end to end: every ``--interval`` seconds it runs one
randomly chosen INSERT / UPDATE / DELETE against the related tables of
``customers_sample_new`` (MySQL / Aurora MySQL), so a live CDC pipeline has a
steady, varied stream of row changes to replicate into Aurora DSQL.

It is **referential-integrity aware**: inserts pick real parent ids (a new order
references an existing customer; a new order_item references a real order +
product), and deletes target leaf rows / cascade-safe rows so a random DELETE
never blocks on a foreign key. Generated columns (``full_name``, ``line_total``,
``margin``) and AUTO_INCREMENT primary keys are never written -- MySQL computes
them. ENUM / SET / JSON columns get realistic random values.

Connection reuses the repo's ``.env`` (DB_HOST / DB_PORT / DB_USER / DB_PASSWORD),
exactly like ``scripts/seed_sample_db.py``. The schema name defaults to
``customers_sample_new`` and can be overridden with ``--schema``.

Safety: this WRITES to the source database. It only touches the
``customers_sample_new`` schema and is meant for a disposable migration-test
source. It performs no DDL and never drops anything. Stop it with Ctrl-C.

Usage:
    python scripts/cdc_workload_customers_sample_new.py                 # 10s loop
    python scripts/cdc_workload_customers_sample_new.py --interval 5
    python scripts/cdc_workload_customers_sample_new.py --once          # one op
    python scripts/cdc_workload_customers_sample_new.py --dry-run       # print only
    python scripts/cdc_workload_customers_sample_new.py --weights i=3,u=2,d=1
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import string
import sys
import time

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


# --- value generators --------------------------------------------------------

_SEGMENTS = ("consumer", "smb", "enterprise", "vip")
_ORDER_STATUS = ("pending", "paid", "shipped", "delivered", "cancelled", "refunded")
_CHANNELS = ("web", "mobile", "store", "partner")
_CURRENCIES = ("USD", "EUR", "GBP", "AUD", "JPY")
_PAY_METHODS = ("card", "paypal", "bank_transfer", "wallet", "cod")
_PAY_STATUS = ("authorized", "captured", "failed", "refunded")
_PRODUCT_STATUS = ("active", "discontinued", "draft", "out_of_stock")
_PRODUCT_TAGS = ("new", "sale", "clearance", "featured", "eco", "imported")
_FIRST = ("Ava", "Liam", "Mia", "Noah", "Emma", "Lucas", "Olivia", "Ethan",
          "Sofia", "Aria", "Leo", "Zoe", "Kai", "Nora", "Owen", "Ivy")
_LAST = ("Kim", "Lee", "Park", "Choi", "Jung", "Cho", "Yoon", "Han",
         "Garcia", "Smith", "Nguyen", "Khan", "Silva", "Mehta", "Ito", "Wang")


def _now() -> dt.datetime:
    return dt.datetime.now()


def _rand_email() -> str:
    tag = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
    return f"cdc_{tag}@example.com"


def _rand_txn_ref() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=32))


def _rand_sku() -> str:
    return "CDC" + "".join(random.choices(string.ascii_uppercase + string.digits, k=29))


def _rand_tags() -> str:
    # SET column: a comma-joined subset (possibly empty).
    k = random.randint(0, 3)
    return ",".join(random.sample(_PRODUCT_TAGS, k)) if k else ""


# --- workload ----------------------------------------------------------------


class Workload:
    """Picks and executes a single random INSERT/UPDATE/DELETE per call."""

    def __init__(self, conn, schema: str, *, dry_run: bool = False,
                 op_log=None) -> None:
        self.conn = conn
        self.s = schema
        self.dry_run = dry_run
        # Optional ground-truth op log: one JSON line per applied change
        # {ts, op, table, pk}. Used by the consistency harness to reconcile
        # exactly which rows should exist on / be absent from the target.
        self._op_log = op_log

    # -- helpers --
    def _q(self, table: str) -> str:
        return f"`{self.s}`.`{table}`"

    def _scalar(self, sql: str, args=()):
        cur = self.conn.cursor()
        cur.execute(sql, args)
        row = cur.fetchone()
        return row[0] if row else None

    def _rand_id(self, table: str, pk: str):
        """A random existing primary-key value, via a cheap random offset.

        Uses ``LIMIT 1 OFFSET rand`` over a bounded window (the max pk) so it
        never scans the whole table -- fine for a test source.
        """
        mx = self._scalar(f"SELECT MAX(`{pk}`) FROM {self._q(table)}")
        if mx is None:
            return None
        # Sample by pk range (ids may be sparse after deletes; retry a few times).
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

    def _exec(self, label: str, sql: str, args=()):
        if self.dry_run:
            print(f"  DRY  {label}: {sql.strip()}  args={args}")
            return label
        cur = self.conn.cursor()
        cur.execute(sql, args)
        self.conn.commit()
        rid = cur.lastrowid
        detail = f" (id={rid})" if rid else f" ({cur.rowcount} row)"
        print(f"  OK   {label}{detail}")
        # Ground-truth op log: op + table from the label, pk from lastrowid
        # (INSERT) or the trailing WHERE-clause bind (UPDATE/DELETE always pass
        # the target pk as the last positional arg). UPDATEs with rowcount 0 (the
        # sampled row was concurrently deleted) are skipped -- nothing changed.
        if self._op_log is not None:
            op, _, table = label.partition(" ")
            if op == "INSERT":
                pk = rid
            else:
                pk = args[-1] if args else None
            if not (op == "UPDATE" and cur.rowcount == 0):
                self._op_log.write(json.dumps({
                    "ts": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "op": op, "table": table, "pk": pk,
                }) + "\n")
                self._op_log.flush()
        return label

    # -- INSERTs (reference real parents) --
    def insert_customer(self):
        country_id = self._rand_id("countries", "country_id")
        if country_id is None:
            return self.insert_order()
        prefs = json.dumps({"newsletter": random.choice([True, False]),
                            "theme": random.choice(["light", "dark"])})
        fn, ln = random.choice(_FIRST), random.choice(_LAST)
        return self._exec(
            "INSERT customers",
            f"INSERT INTO {self._q('customers')} "
            "(email, first_name, last_name, country_id, segment, loyalty_points, "
            "preferences, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (_rand_email(), fn, ln, country_id, random.choice(_SEGMENTS),
             random.randint(0, 5000), prefs, _now()),
        )

    def insert_order(self):
        customer_id = self._rand_id("customers", "customer_id")
        if customer_id is None:
            return None
        # A ship address that belongs to this customer (nullable -> ok if none).
        ship_address_id = self._scalar(
            f"SELECT address_id FROM {self._q('customer_addresses')} "
            "WHERE customer_id=%s ORDER BY address_id LIMIT 1",
            (customer_id,),
        )
        meta = json.dumps({"source": "cdc-workload", "priority": random.randint(1, 5)})
        return self._exec(
            "INSERT orders",
            f"INSERT INTO {self._q('orders')} "
            "(customer_id, ship_address_id, order_status, channel, currency, "
            "total_amount, metadata, order_ts) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            (customer_id, ship_address_id, random.choice(_ORDER_STATUS),
             random.choice(_CHANNELS), random.choice(_CURRENCIES),
             round(random.uniform(10, 2000), 2), meta, _now()),
        )

    def insert_order_item(self):
        order_id = self._rand_id("orders", "order_id")
        product_id = self._rand_id("products", "product_id")
        if order_id is None or product_id is None:
            return None
        # line_total is a generated column -- do NOT insert it.
        return self._exec(
            "INSERT order_items",
            f"INSERT INTO {self._q('order_items')} "
            "(order_id, product_id, quantity, unit_price, discount) "
            "VALUES (%s,%s,%s,%s,%s)",
            (order_id, product_id, random.randint(1, 9),
             round(random.uniform(5, 500), 2), round(random.uniform(0, 50), 2)),
        )

    def insert_payment(self):
        order_id = self._rand_id("orders", "order_id")
        if order_id is None:
            return None
        return self._exec(
            "INSERT payments",
            f"INSERT INTO {self._q('payments')} "
            "(order_id, method, amount, status, txn_ref, paid_ts) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (order_id, random.choice(_PAY_METHODS), round(random.uniform(10, 2000), 2),
             random.choice(_PAY_STATUS), _rand_txn_ref(), _now()),
        )

    def insert_review(self):
        product_id = self._rand_id("products", "product_id")
        customer_id = self._rand_id("customers", "customer_id")
        if product_id is None or customer_id is None:
            return None
        return self._exec(
            "INSERT product_reviews",
            f"INSERT INTO {self._q('product_reviews')} "
            "(product_id, customer_id, rating, title, body, helpful_votes, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (product_id, customer_id, random.randint(1, 5),
             "CDC test review", "Generated by the CDC workload script.",
             random.randint(0, 200), _now()),
        )

    def insert_product(self):
        category_id = self._rand_id("categories", "category_id")
        supplier_id = self._rand_id("suppliers", "supplier_id")
        if category_id is None or supplier_id is None:
            return None
        attrs = json.dumps({"color": random.choice(["red", "blue", "green"]),
                           "size": random.choice(["S", "M", "L"])})
        return self._exec(
            "INSERT products",
            f"INSERT INTO {self._q('products')} "
            "(category_id, supplier_id, sku, product_name, description, unit_price, "
            "cost_price, status, tags, attributes, weight_kg, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (category_id, supplier_id, _rand_sku(), "CDC Test Product",
             "Generated by the CDC workload script.",
             round(random.uniform(5, 800), 2), round(random.uniform(2, 600), 2),
             random.choice(_PRODUCT_STATUS), _rand_tags(), attrs,
             round(random.uniform(0.1, 25), 3), _now()),
        )

    # -- UPDATEs (mutate a random existing row) --
    def update_customer(self):
        cid = self._rand_id("customers", "customer_id")
        if cid is None:
            return None
        return self._exec(
            "UPDATE customers",
            f"UPDATE {self._q('customers')} SET loyalty_points=loyalty_points+%s, "
            "segment=%s WHERE customer_id=%s",
            (random.randint(1, 100), random.choice(_SEGMENTS), cid),
        )

    def update_order_status(self):
        oid = self._rand_id("orders", "order_id")
        if oid is None:
            return None
        return self._exec(
            "UPDATE orders",
            f"UPDATE {self._q('orders')} SET order_status=%s, total_amount=%s "
            "WHERE order_id=%s",
            (random.choice(_ORDER_STATUS), round(random.uniform(10, 2000), 2), oid),
        )

    def update_product_price(self):
        pid = self._rand_id("products", "product_id")
        if pid is None:
            return None
        return self._exec(
            "UPDATE products",
            f"UPDATE {self._q('products')} SET unit_price=%s, status=%s WHERE product_id=%s",
            (round(random.uniform(5, 800), 2), random.choice(_PRODUCT_STATUS), pid),
        )

    def update_review_votes(self):
        rid = self._rand_id("product_reviews", "review_id")
        if rid is None:
            return None
        return self._exec(
            "UPDATE product_reviews",
            f"UPDATE {self._q('product_reviews')} SET helpful_votes=helpful_votes+%s, "
            "rating=%s WHERE review_id=%s",
            (random.randint(1, 10), random.randint(1, 5), rid),
        )

    # -- DELETEs (leaf / cascade-safe rows only, so no FK blocks) --
    def delete_review(self):
        rid = self._rand_id("product_reviews", "review_id")
        if rid is None:
            return None
        return self._exec(
            "DELETE product_reviews",
            f"DELETE FROM {self._q('product_reviews')} WHERE review_id=%s", (rid,),
        )

    def delete_order_item(self):
        iid = self._rand_id("order_items", "order_item_id")
        if iid is None:
            return None
        return self._exec(
            "DELETE order_items",
            f"DELETE FROM {self._q('order_items')} WHERE order_item_id=%s", (iid,),
        )

    def delete_payment(self):
        pid = self._rand_id("payments", "payment_id")
        if pid is None:
            return None
        return self._exec(
            "DELETE payments",
            f"DELETE FROM {self._q('payments')} WHERE payment_id=%s", (pid,),
        )

    # -- dispatch --
    def operations(self):
        return {
            "i": [self.insert_customer, self.insert_order, self.insert_order_item,
                  self.insert_payment, self.insert_review, self.insert_product],
            "u": [self.update_customer, self.update_order_status,
                  self.update_product_price, self.update_review_votes],
            "d": [self.delete_review, self.delete_order_item, self.delete_payment],
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
    weights = {"i": 3, "u": 2, "d": 1}  # insert-heavy by default
    if spec:
        for part in spec.split(","):
            k, _, v = part.partition("=")
            k = k.strip().lower()
            if k in ("i", "u", "d") and v.strip().isdigit():
                weights[k] = int(v)
    return weights


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--schema", default=_cfg("CDC_WORKLOAD_SCHEMA", "customers_sample_new"))
    ap.add_argument("--interval", type=float, default=10.0, help="seconds between ops")
    ap.add_argument("--once", action="store_true", help="run a single op and exit")
    ap.add_argument("--dry-run", action="store_true", help="print SQL, do not execute")
    ap.add_argument("--weights", default="", help="op mix, e.g. i=3,u=2,d=1")
    ap.add_argument("--op-log", default="", help="append one ground-truth JSONL "
                    "line {ts,op,table,pk} per op to this file (for reconcile)")
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
    print(f"CDC workload -> {host}:{port} schema={args.schema} "
          f"weights(i/u/d)={weights['i']}/{weights['u']}/{weights['d']} "
          f"interval={args.interval}s "
          f"{'op-log=' + args.op_log + ' ' if args.op_log else ''}"
          f"{'[DRY-RUN]' if args.dry_run else ''}")
    print("Ctrl-C to stop.\n")

    n = 0
    try:
        while True:
            n += 1
            print(f"[{dt.datetime.now():%H:%M:%S}] op #{n}")
            wl.run_one(weights)
            if args.once:
                break
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
