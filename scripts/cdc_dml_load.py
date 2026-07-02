#!/usr/bin/env python3
"""Continuously apply small DML batches to the source MySQL DB for CDC testing.

This is a standalone operational utility (not part of the shipped tool). It drives a
steady stream of INSERT / UPDATE / DELETE statements against the seeded e-commerce
schema (see scripts/seed_sample_db.py) so a binlog-based Change Data Capture (CDC)
pipeline has a continuous flow of change events to capture and replay onto Aurora DSQL.

Design notes (scale-safe):
- Each tick INSERTs a fresh parent (customer -> address -> order -> items/payment/review)
  and uses the new AUTO_INCREMENT ids directly, so it never scans the large fact tables
  to find a valid foreign key. Product/country ids are sampled once at startup (PK-ordered
  LIMIT, cheap) and reused.
- UPDATEs and DELETEs target only rows this script created (tracked in small bounded
  in-memory queues), so they stay O(1) and never lock or scan large ranges.
- One transaction per tick (autocommit off + explicit commit) so binlog events flush
  promptly and stay bounded per commit, well under any per-transaction limits.

Connection settings are read from .env (DB_HOST/DB_PORT/DB_USER/DB_NAME/DB_PASSWORD),
identical to the seed script. The password is never logged.

Run:
    .venv/bin/python scripts/cdc_dml_load.py
Stop with Ctrl+C. Configure cadence/volume via environment:
    CDC_DML_INTERVAL_SEC   seconds between ticks (default 10)
    CDC_DML_ITEMS_PER_TICK order_items inserted per tick (default 3)
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
import uuid
import datetime as dt
from collections import deque

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
                val = val.strip().strip('"').strip("'")
                values[key.strip()] = val
    except FileNotFoundError:
        pass
    return values


_ENV = load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))


def _cfg(key: str, default: str) -> str:
    return _ENV.get(key) or os.environ.get(key) or default


HOST = _cfg("DB_HOST")
PORT = int(_cfg("DB_PORT", "3306"))
USER = _cfg("DB_USER", "admin")
DB = _cfg("DB_NAME", "customers_sample")

INTERVAL_SEC = float(_cfg("CDC_DML_INTERVAL_SEC", "10"))
ITEMS_PER_TICK = int(_cfg("CDC_DML_ITEMS_PER_TICK", "3"))

# Bounded history so UPDATE/DELETE always target rows we created (no large scans).
RECENT_CUSTOMERS_MAX = 200
RECENT_ORDERS_MAX = 200
REVIEW_RETENTION = 30  # delete a review once the queue grows beyond this many ticks

_SEGMENTS = ("consumer", "smb", "enterprise", "vip")
_ORDER_STATUSES = ("pending", "paid", "shipped", "delivered", "cancelled", "refunded")
_CHANNELS = ("web", "mobile", "store", "partner")
_CURRENCIES = ("USD", "EUR", "KRW")
_ADDRESS_TYPES = ("billing", "shipping", "both")
_PAY_METHODS = ("card", "paypal", "bank_transfer", "wallet", "cod")
_PAY_STATUSES = ("authorized", "captured", "failed", "refunded")
_FIRST_NAMES = ("James", "Mary", "John", "Patricia", "Robert", "Jennifer",
                "Michael", "Linda", "David", "Sarah")
_LAST_NAMES = ("Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
               "Miller", "Davis", "Lee", "Kim")


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def connect() -> pymysql.connections.Connection:
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not pw:
        log("FATAL: no DB password. Set DB_PASSWORD in .env (or MYSQL_PWD env var).")
        sys.exit(1)
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=pw, database=DB,
        connect_timeout=15, read_timeout=60, write_timeout=60,
        autocommit=False, charset="utf8mb4",
    )


def load_lookups(cur) -> tuple[list[int], list[int]]:
    """Sample small, stable id pools once (PK-ordered LIMIT, no large scans)."""
    cur.execute("SELECT country_id FROM countries ORDER BY country_id")
    country_ids = [int(r[0]) for r in cur.fetchall()]
    cur.execute("SELECT product_id FROM products ORDER BY product_id LIMIT 200")
    product_ids = [int(r[0]) for r in cur.fetchall()]
    if not country_ids or not product_ids:
        log("FATAL: source schema is empty (no countries/products). Seed it first.")
        sys.exit(1)
    log(f"Loaded lookups: {len(country_ids)} countries, {len(product_ids)} products")
    return country_ids, product_ids


def _new_email() -> str:
    return f"cdc_{int(time.time() * 1000)}_{random.randint(0, 999999)}@example.com"


def apply_tick(cur, country_ids, product_ids, recent_customers, recent_orders,
               review_queue) -> dict:
    """Apply one mixed DML batch. Returns per-operation counts for logging."""
    counts = {"insert": 0, "update": 0, "delete": 0}
    country_id = random.choice(country_ids)

    # INSERT customer ----------------------------------------------------------
    cur.execute(
        "INSERT INTO customers (email, first_name, last_name, country_id, segment, "
        "loyalty_points, preferences, created_at) VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())",
        (_new_email(), random.choice(_FIRST_NAMES), random.choice(_LAST_NAMES),
         country_id, random.choice(_SEGMENTS), random.randint(0, 10000),
         json.dumps({"newsletter": random.random() < 0.5, "source": "cdc_dml_load"})),
    )
    customer_id = cur.lastrowid
    counts["insert"] += 1

    # INSERT address -----------------------------------------------------------
    cur.execute(
        "INSERT INTO customer_addresses (customer_id, country_id, address_type, line1, "
        "city, postal_code, is_default) VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (customer_id, country_id, random.choice(_ADDRESS_TYPES),
         f"{random.randint(1, 9999)} Main St", "Springfield",
         f"{random.randint(0, 99999):05d}", 1),
    )
    address_id = cur.lastrowid
    counts["insert"] += 1

    # INSERT order -------------------------------------------------------------
    cur.execute(
        "INSERT INTO orders (customer_id, ship_address_id, order_status, channel, "
        "currency, total_amount, metadata, order_ts) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s, NOW())",
        (customer_id, address_id, random.choice(_ORDER_STATUSES),
         random.choice(_CHANNELS), random.choice(_CURRENCIES),
         round(random.uniform(10, 2000), 2),
         json.dumps({"gift": random.random() < 0.1, "source": "cdc_dml_load"})),
    )
    order_id = cur.lastrowid
    counts["insert"] += 1

    # INSERT order_items -------------------------------------------------------
    for _ in range(max(1, ITEMS_PER_TICK)):
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) "
            "VALUES (%s,%s,%s,%s,%s)",
            (order_id, random.choice(product_ids), random.randint(1, 8),
             round(random.uniform(5, 500), 2), round(random.uniform(0, 30), 2)),
        )
        counts["insert"] += 1

    # INSERT payment -----------------------------------------------------------
    cur.execute(
        "INSERT INTO payments (order_id, method, amount, status, txn_ref, paid_ts) "
        "VALUES (%s,%s,%s,%s,%s, NOW())",
        (order_id, random.choice(_PAY_METHODS), round(random.uniform(10, 2000), 2),
         random.choice(_PAY_STATUSES), uuid.uuid4().hex),
    )
    counts["insert"] += 1

    # INSERT review ------------------------------------------------------------
    cur.execute(
        "INSERT INTO product_reviews (product_id, customer_id, rating, title, body, "
        "helpful_votes, created_at) VALUES (%s,%s,%s,%s,%s,%s, NOW())",
        (random.choice(product_ids), customer_id, random.randint(1, 5),
         "CDC review", "Generated by cdc_dml_load for change-capture testing.",
         random.randint(0, 500)),
    )
    review_id = cur.lastrowid
    counts["insert"] += 1

    recent_customers.append(customer_id)
    recent_orders.append(order_id)
    review_queue.append(review_id)

    # UPDATE a recently created customer and order (targets our own rows only) --
    if recent_customers:
        cur.execute(
            "UPDATE customers SET loyalty_points = loyalty_points + %s WHERE customer_id = %s",
            (random.randint(1, 100), random.choice(recent_customers)),
        )
        counts["update"] += cur.rowcount
    if recent_orders:
        cur.execute(
            "UPDATE orders SET order_status = %s, total_amount = %s WHERE order_id = %s",
            (random.choice(_ORDER_STATUSES), round(random.uniform(10, 2000), 2),
             random.choice(recent_orders)),
        )
        counts["update"] += cur.rowcount

    # DELETE the oldest tracked review and one item of an older order ----------
    if len(review_queue) > REVIEW_RETENTION:
        old_review = review_queue.popleft()
        cur.execute("DELETE FROM product_reviews WHERE review_id = %s", (old_review,))
        counts["delete"] += cur.rowcount
        if len(recent_orders) > 1:
            cur.execute(
                "DELETE FROM order_items WHERE order_id = %s LIMIT 1",
                (recent_orders[0],),
            )
            counts["delete"] += cur.rowcount

    return counts


def main() -> None:
    log(f"CDC DML load -> `{DB}` on {HOST} every {INTERVAL_SEC:g}s "
        f"(items/tick={ITEMS_PER_TICK}). Ctrl+C to stop.")
    conn = connect()
    try:
        with conn.cursor() as cur:
            country_ids, product_ids = load_lookups(cur)
    except Exception:
        conn.close()
        raise

    recent_customers: deque = deque(maxlen=RECENT_CUSTOMERS_MAX)
    recent_orders: deque = deque(maxlen=RECENT_ORDERS_MAX)
    review_queue: deque = deque()

    tick = 0
    totals = {"insert": 0, "update": 0, "delete": 0}
    try:
        while True:
            start = time.time()
            try:
                with conn.cursor() as cur:
                    counts = apply_tick(cur, country_ids, product_ids,
                                        recent_customers, recent_orders, review_queue)
                conn.commit()
                tick += 1
                for k in totals:
                    totals[k] += counts[k]
                log(f"tick {tick}: +{counts['insert']} ins / {counts['update']} upd / "
                    f"{counts['delete']} del  "
                    f"(totals: {totals['insert']}/{totals['update']}/{totals['delete']})")
            except Exception as e:  # noqa: BLE001
                log(f"tick error: {str(e)[:200]} -- reconnecting")
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(min(5.0, INTERVAL_SEC))
                conn = connect()
            elapsed = time.time() - start
            time.sleep(max(0.0, INTERVAL_SEC - elapsed))
    except KeyboardInterrupt:
        log(f"Stopped after {tick} ticks. "
            f"Totals: {totals['insert']} ins / {totals['update']} upd / {totals['delete']} del.")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
