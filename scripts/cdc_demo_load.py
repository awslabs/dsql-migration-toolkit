#!/usr/bin/env python3
"""Emit a steady stream of realistic business rows for UI-driven CDC testing.

A standalone operational utility (not part of the shipped tool), modeled on
scripts/cdc_heartbeat.py but with a *realistic* table so the four-step UI
(schema conversion -> Full Load -> CDC) can be exercised from a user's
perspective over varied column types, not just (id, ts).

It maintains a dedicated schema (default ``cdc_demo``) with one ``orders``
table and inserts one new order per tick (default every 10 seconds). Each row
carries a mix of types a real migration must handle: an AUTO_INCREMENT BIGINT
PK, VARCHARs, a DECIMAL money column, an ENUM-like status string, an integer
quantity, and microsecond DATETIMEs. Use it to:

  * select ``cdc_demo.orders`` in the UI and run a Full Load (snapshot), then
  * watch CDC stream the rows inserted *after* the snapshot (gapless hand-off).

Connection settings come from .env (DB_HOST/DB_PORT/DB_USER/DB_PASSWORD),
identical to cdc_heartbeat.py / seed scripts. The password is never logged.

Run:
    .venv/bin/python scripts/cdc_demo_load.py            # loop forever, 1 row/10s
    .venv/bin/python scripts/cdc_demo_load.py --setup    # create schema+table, exit
    .venv/bin/python scripts/cdc_demo_load.py --seed 50  # insert 50 rows up front, then loop
Stop with Ctrl+C. Configure via environment:
    CDC_DEMO_SCHEMA       schema (database) name (default cdc_demo)
    CDC_DEMO_TABLE        table name (default orders)
    CDC_DEMO_INTERVAL_SEC seconds between inserts (default 10)
"""
from __future__ import annotations

import os
import sys
import time
import random
import datetime as dt

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

SCHEMA = _cfg("CDC_DEMO_SCHEMA", "cdc_demo")
TABLE = _cfg("CDC_DEMO_TABLE", "orders")
INTERVAL_SEC = float(_cfg("CDC_DEMO_INTERVAL_SEC", "10"))

_CUSTOMERS = [
    "Alice Kim", "Bob Lee", "Carol Park", "Dave Choi", "Erin Jung",
    "Frank Oh", "Grace Yoon", "Henry Seo", "Iris Han", "Jack Moon",
]
_PRODUCTS = [
    "Widget", "Gadget", "Sprocket", "Cog", "Flange",
    "Bracket", "Bolt", "Washer", "Gasket", "Bearing",
]
_STATUSES = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def connect(db: str | None = None) -> pymysql.connections.Connection:
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not pw:
        log("FATAL: no DB password. Set DB_PASSWORD in .env (or MYSQL_PWD env var).")
        sys.exit(1)
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=pw, database=db,
        connect_timeout=15, read_timeout=60, write_timeout=60,
        autocommit=False, charset="utf8mb4",
    )


def ensure_schema() -> None:
    """Create the demo schema and orders table if they do not exist.

    A deliberately varied set of columns so the migration exercises real type
    mapping (BIGINT PK, VARCHAR, DECIMAL, INT, status string, DATETIME(6)).
    """
    conn = connect(None)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS {SCHEMA} "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} ("
                " id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,"
                " customer_name VARCHAR(100) NOT NULL,"
                " product VARCHAR(100) NOT NULL,"
                " quantity INT NOT NULL,"
                " amount DECIMAL(10,2) NOT NULL,"
                " status VARCHAR(20) NOT NULL,"
                " created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),"
                " KEY idx_orders_created_at (created_at),"
                " KEY idx_orders_status (status)"
                ") ENGINE=InnoDB"
            )
        conn.commit()
        log(f"Ensured schema `{SCHEMA}` and table `{TABLE}` "
            "(id, customer_name, product, quantity, amount, status, created_at).")
    finally:
        conn.close()


def _random_order() -> tuple:
    """Build one synthetic order row (column order matches the INSERT)."""
    qty = random.randint(1, 20)
    unit_price = round(random.uniform(2.5, 499.99), 2)
    amount = round(qty * unit_price, 2)
    return (
        random.choice(_CUSTOMERS),
        random.choice(_PRODUCTS),
        qty,
        amount,
        random.choice(_STATUSES),
    )


def _insert_one(conn: pymysql.connections.Connection) -> int:
    """Insert one order; return its id. created_at is stamped by the DB clock."""
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {TABLE} "
            "(customer_name, product, quantity, amount, status) "
            "VALUES (%s, %s, %s, %s, %s)",
            _random_order(),
        )
        row_id = cur.lastrowid
    conn.commit()
    return row_id


def main() -> None:
    ensure_schema()
    if "--setup" in sys.argv:
        log("Setup only (--setup): schema/table ready. Exiting.")
        return

    # Optional up-front backfill so a Full Load snapshot has rows to copy.
    seed_n = 0
    if "--seed" in sys.argv:
        i = sys.argv.index("--seed")
        if i + 1 < len(sys.argv):
            seed_n = int(sys.argv[i + 1])

    conn = connect(SCHEMA)
    if seed_n > 0:
        log(f"Seeding {seed_n} rows up front (Full Load snapshot baseline)...")
        for _ in range(seed_n):
            _insert_one(conn)
        log(f"Seeded {seed_n} rows.")

    log(f"CDC demo load -> `{SCHEMA}.{TABLE}` on {HOST} every {INTERVAL_SEC:g}s. "
        "Ctrl+C to stop.")
    rows = 0
    try:
        while True:
            start = time.time()
            try:
                row_id = _insert_one(conn)
                rows += 1
                log(f"insert {rows}: id={row_id}")
            except Exception as e:  # noqa: BLE001
                log(f"insert error: {str(e)[:200]} -- reconnecting")
                try:
                    conn.rollback()
                except Exception:  # noqa: BLE001
                    pass
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                time.sleep(min(5.0, INTERVAL_SEC))
                conn = connect(SCHEMA)
            elapsed = time.time() - start
            time.sleep(max(0.0, INTERVAL_SEC - elapsed))
    except KeyboardInterrupt:
        log(f"Stopped after {rows} inserts.")
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass


if __name__ == "__main__":
    main()
