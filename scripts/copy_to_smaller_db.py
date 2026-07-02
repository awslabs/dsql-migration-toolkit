#!/usr/bin/env python3
"""Copy customers_sample (10GB) into customers_sample_new (~1GB) maintaining FK integrity.

Strategy:
- Copy ALL dimension tables (regions, countries, categories, suppliers, products) -- small
- Copy a customer subset (first N customers by PK)
- Copy dependent fact tables filtered to that subset
- Re-create view, stored procedure, trigger, fulltext indexes
"""
from __future__ import annotations

import os
import sys
import time
import datetime as dt

import pymysql

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_dotenv(path: str) -> dict:
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

HOST = _ENV.get("DB_HOST") or os.environ.get("DB_HOST")
PORT = int(_ENV.get("DB_PORT") or os.environ.get("DB_PORT", "3306"))
USER = _ENV.get("DB_USER") or os.environ.get("DB_USER", "admin")

SRC_DB = "customers_sample"
DST_DB = "customers_sample_new"
TARGET_GB = 1.0


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def connect(db: str | None = None) -> pymysql.connections.Connection:
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not pw:
        log("FATAL: no DB password. Set DB_PASSWORD in .env or MYSQL_PWD env var.")
        sys.exit(1)
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=pw, database=db,
        connect_timeout=15, read_timeout=7200, write_timeout=7200,
        autocommit=False, charset="utf8mb4",
    )


def db_size_bytes(cur, db: str) -> int:
    cur.execute(
        "SELECT COALESCE(SUM(data_length + index_length), 0) "
        "FROM information_schema.tables WHERE table_schema = %s",
        (db,),
    )
    return int(cur.fetchone()[0] or 0)


def gb(n: int) -> str:
    return f"{n / (1024 ** 3):.2f} GB"


def table_row_count(cur, db: str, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {db}.{table}")
    return int(cur.fetchone()[0])


def max_pk(cur, db: str, table: str, pk: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM {db}.{table}")
    return int(cur.fetchone()[0])


DDL = [
    "CREATE TABLE IF NOT EXISTS regions ("
    " region_id TINYINT UNSIGNED PRIMARY KEY,"
    " region_name VARCHAR(50) NOT NULL,"
    " UNIQUE KEY uq_region_name (region_name)) ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS countries ("
    " country_id SMALLINT UNSIGNED PRIMARY KEY,"
    " region_id TINYINT UNSIGNED NOT NULL,"
    " country_code CHAR(2) NOT NULL,"
    " country_name VARCHAR(80) NOT NULL,"
    " UNIQUE KEY uq_country_code (country_code),"
    " KEY idx_country_region (region_id),"
    " CONSTRAINT fk_country_region FOREIGN KEY (region_id) REFERENCES regions(region_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS categories ("
    " category_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " parent_category_id INT UNSIGNED NULL,"
    " category_name VARCHAR(100) NOT NULL,"
    " depth TINYINT UNSIGNED NOT NULL DEFAULT 0,"
    " KEY idx_cat_parent (parent_category_id),"
    " CONSTRAINT fk_cat_parent FOREIGN KEY (parent_category_id) REFERENCES categories(category_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS suppliers ("
    " supplier_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " country_id SMALLINT UNSIGNED NOT NULL,"
    " supplier_name VARCHAR(120) NOT NULL,"
    " contact_info JSON NULL,"
    " rating DECIMAL(3,2) NOT NULL DEFAULT 0.00,"
    " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    " KEY idx_supplier_country (country_id),"
    " CONSTRAINT fk_supplier_country FOREIGN KEY (country_id) REFERENCES countries(country_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS products ("
    " product_id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " category_id INT UNSIGNED NOT NULL,"
    " supplier_id INT UNSIGNED NOT NULL,"
    " sku CHAR(32) NOT NULL,"
    " product_name VARCHAR(150) NOT NULL,"
    " description TEXT NULL,"
    " unit_price DECIMAL(10,2) NOT NULL,"
    " cost_price DECIMAL(10,2) NOT NULL,"
    " margin DECIMAL(10,2) GENERATED ALWAYS AS (unit_price - cost_price) STORED,"
    " status ENUM('active','discontinued','draft','out_of_stock') NOT NULL DEFAULT 'active',"
    " tags SET('new','sale','clearance','featured','eco','imported') NULL,"
    " attributes JSON NULL,"
    " weight_kg DECIMAL(8,3) NULL,"
    " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    " UNIQUE KEY uq_product_sku (sku),"
    " KEY idx_product_category (category_id),"
    " KEY idx_product_supplier (supplier_id),"
    " KEY idx_product_status_price (status, unit_price),"
    " CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES categories(category_id),"
    " CONSTRAINT fk_product_supplier FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS customers ("
    " customer_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " email VARCHAR(160) NOT NULL,"
    " first_name VARCHAR(60) NOT NULL,"
    " last_name VARCHAR(60) NOT NULL,"
    " full_name VARCHAR(121) GENERATED ALWAYS AS (CONCAT(first_name,' ',last_name)) VIRTUAL,"
    " country_id SMALLINT UNSIGNED NOT NULL,"
    " segment ENUM('consumer','smb','enterprise','vip') NOT NULL DEFAULT 'consumer',"
    " loyalty_points INT NOT NULL DEFAULT 0,"
    " preferences JSON NULL,"
    " created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
    " UNIQUE KEY uq_customer_email (email),"
    " KEY idx_customer_country (country_id),"
    " KEY idx_customer_segment (segment),"
    " KEY idx_customer_created (created_at),"
    " CONSTRAINT fk_customer_country FOREIGN KEY (country_id) REFERENCES countries(country_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS customer_addresses ("
    " address_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " customer_id BIGINT UNSIGNED NOT NULL,"
    " country_id SMALLINT UNSIGNED NOT NULL,"
    " address_type ENUM('billing','shipping','both') NOT NULL DEFAULT 'shipping',"
    " line1 VARCHAR(150) NOT NULL,"
    " city VARCHAR(80) NOT NULL,"
    " postal_code VARCHAR(20) NOT NULL,"
    " is_default TINYINT(1) NOT NULL DEFAULT 0,"
    " KEY idx_addr_customer (customer_id),"
    " KEY idx_addr_country (country_id),"
    " CONSTRAINT fk_addr_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),"
    " CONSTRAINT fk_addr_country FOREIGN KEY (country_id) REFERENCES countries(country_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS orders ("
    " order_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " customer_id BIGINT UNSIGNED NOT NULL,"
    " ship_address_id BIGINT UNSIGNED NULL,"
    " order_status ENUM('pending','paid','shipped','delivered','cancelled','refunded') NOT NULL DEFAULT 'pending',"
    " channel ENUM('web','mobile','store','partner') NOT NULL DEFAULT 'web',"
    " currency CHAR(3) NOT NULL DEFAULT 'USD',"
    " total_amount DECIMAL(12,2) NOT NULL DEFAULT 0.00,"
    " metadata JSON NULL,"
    " order_ts DATETIME NOT NULL,"
    " KEY idx_order_customer (customer_id),"
    " KEY idx_order_ts (order_ts),"
    " KEY idx_order_status (order_status),"
    " CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id),"
    " CONSTRAINT fk_order_addr FOREIGN KEY (ship_address_id) REFERENCES customer_addresses(address_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS order_items ("
    " order_item_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " order_id BIGINT UNSIGNED NOT NULL,"
    " product_id INT UNSIGNED NOT NULL,"
    " quantity INT UNSIGNED NOT NULL DEFAULT 1,"
    " unit_price DECIMAL(10,2) NOT NULL,"
    " discount DECIMAL(10,2) NOT NULL DEFAULT 0.00,"
    " line_total DECIMAL(14,2) GENERATED ALWAYS AS (quantity * unit_price - discount) STORED,"
    " KEY idx_item_order (order_id),"
    " KEY idx_item_product (product_id),"
    " CONSTRAINT fk_item_order FOREIGN KEY (order_id) REFERENCES orders(order_id),"
    " CONSTRAINT fk_item_product FOREIGN KEY (product_id) REFERENCES products(product_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS payments ("
    " payment_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " order_id BIGINT UNSIGNED NOT NULL,"
    " method ENUM('card','paypal','bank_transfer','wallet','cod') NOT NULL,"
    " amount DECIMAL(12,2) NOT NULL,"
    " status ENUM('authorized','captured','failed','refunded') NOT NULL DEFAULT 'captured',"
    " txn_ref CHAR(32) NOT NULL,"
    " paid_ts DATETIME NOT NULL,"
    " UNIQUE KEY uq_payment_txn (txn_ref),"
    " KEY idx_payment_order (order_id),"
    " CONSTRAINT fk_payment_order FOREIGN KEY (order_id) REFERENCES orders(order_id)"
    ") ENGINE=InnoDB",

    "CREATE TABLE IF NOT EXISTS product_reviews ("
    " review_id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,"
    " product_id INT UNSIGNED NOT NULL,"
    " customer_id BIGINT UNSIGNED NOT NULL,"
    " rating TINYINT UNSIGNED NOT NULL,"
    " title VARCHAR(150) NULL,"
    " body TEXT NULL,"
    " helpful_votes INT UNSIGNED NOT NULL DEFAULT 0,"
    " created_at DATETIME NOT NULL,"
    " KEY idx_review_product (product_id),"
    " KEY idx_review_customer (customer_id),"
    " CONSTRAINT fk_review_product FOREIGN KEY (product_id) REFERENCES products(product_id),"
    " CONSTRAINT fk_review_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id)"
    ") ENGINE=InnoDB",
]

CHUNK = 100_000


def chunked_copy(cur, label: str, insert_sql: str, count_sql: str) -> int:
    """Copy rows in chunks using LIMIT/OFFSET to avoid huge transactions."""
    cur.execute(count_sql)
    total = int(cur.fetchone()[0])
    if total == 0:
        log(f"  {label}: 0 rows (skip)")
        return 0
    log(f"  {label}: {total:,} rows to copy...")
    copied = 0
    while copied < total:
        size = min(CHUNK, total - copied)
        sql = f"{insert_sql} LIMIT {size} OFFSET {copied}"
        cur.execute(sql)
        cur.connection.commit()
        copied += size
        if copied % 500_000 == 0 or copied >= total:
            log(f"    {label}: {copied:,}/{total:,}")
    return copied


def main() -> None:
    start = time.time()
    log(f"Copying {SRC_DB} -> {DST_DB} (target ~{TARGET_GB} GB)")

    conn = connect(None)
    cur = conn.cursor()

    # Get source DB size and row counts to determine subset fraction
    src_size = db_size_bytes(cur, SRC_DB)
    log(f"Source DB size: {gb(src_size)}")

    fraction = TARGET_GB * (1024**3) / src_size if src_size > 0 else 0.1
    log(f"Target fraction: {fraction:.3f}")

    # Get max customer_id to determine cutoff
    max_cust = max_pk(cur, SRC_DB, "customers", "customer_id")
    cust_cutoff = int(max_cust * fraction)
    log(f"Customer cutoff: customer_id <= {cust_cutoff:,} (max={max_cust:,})")

    # Create destination database
    log(f"Creating database {DST_DB}...")
    cur.execute(f"DROP DATABASE IF EXISTS {DST_DB}")
    conn.commit()
    cur.execute(f"CREATE DATABASE {DST_DB} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
    conn.commit()

    # Switch to destination DB
    cur.execute(f"USE {DST_DB}")
    cur.execute("SET SESSION foreign_key_checks = 0")
    cur.execute("SET SESSION unique_checks = 0")

    # Create schema
    log("Creating schema...")
    for stmt in DDL:
        cur.execute(stmt)
    conn.commit()

    # Copy dimension tables (all rows - they're small)
    log("Copying dimension tables (full copy)...")
    cur.execute(f"INSERT INTO {DST_DB}.regions SELECT * FROM {SRC_DB}.regions")
    conn.commit()
    cur.execute(f"INSERT INTO {DST_DB}.countries SELECT * FROM {SRC_DB}.countries")
    conn.commit()

    # categories - copy in order to satisfy self-referencing FK
    cur.execute(
        f"INSERT INTO {DST_DB}.categories (category_id, parent_category_id, category_name, depth) "
        f"SELECT category_id, parent_category_id, category_name, depth FROM {SRC_DB}.categories "
        f"ORDER BY category_id"
    )
    conn.commit()
    log("  regions, countries, categories: done")

    cur.execute(
        f"INSERT INTO {DST_DB}.suppliers (supplier_id, country_id, supplier_name, contact_info, rating, created_at) "
        f"SELECT supplier_id, country_id, supplier_name, contact_info, rating, created_at FROM {SRC_DB}.suppliers"
    )
    conn.commit()
    log("  suppliers: done")

    # Products - copy all (200K rows, ~200-400MB with TEXT descriptions)
    copied = chunked_copy(
        cur, "products",
        f"INSERT INTO {DST_DB}.products (product_id, category_id, supplier_id, sku, product_name, "
        f"description, unit_price, cost_price, status, tags, attributes, weight_kg, created_at) "
        f"SELECT product_id, category_id, supplier_id, sku, product_name, "
        f"description, unit_price, cost_price, status, tags, attributes, weight_kg, created_at "
        f"FROM {SRC_DB}.products ORDER BY product_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.products",
    )

    # Customers - subset by PK range
    copied = chunked_copy(
        cur, "customers",
        f"INSERT INTO {DST_DB}.customers (customer_id, email, first_name, last_name, "
        f"country_id, segment, loyalty_points, preferences, created_at) "
        f"SELECT customer_id, email, first_name, last_name, "
        f"country_id, segment, loyalty_points, preferences, created_at "
        f"FROM {SRC_DB}.customers WHERE customer_id <= {cust_cutoff} ORDER BY customer_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.customers WHERE customer_id <= {cust_cutoff}",
    )

    # Addresses - only those belonging to our customer subset
    copied = chunked_copy(
        cur, "customer_addresses",
        f"INSERT INTO {DST_DB}.customer_addresses (address_id, customer_id, country_id, "
        f"address_type, line1, city, postal_code, is_default) "
        f"SELECT address_id, customer_id, country_id, "
        f"address_type, line1, city, postal_code, is_default "
        f"FROM {SRC_DB}.customer_addresses WHERE customer_id <= {cust_cutoff} ORDER BY address_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.customer_addresses WHERE customer_id <= {cust_cutoff}",
    )

    # Orders - customer subset AND valid address
    # ship_address_id can be NULL or reference an address we've copied
    copied = chunked_copy(
        cur, "orders",
        f"INSERT INTO {DST_DB}.orders (order_id, customer_id, ship_address_id, order_status, "
        f"channel, currency, total_amount, metadata, order_ts) "
        f"SELECT o.order_id, o.customer_id, o.ship_address_id, o.order_status, "
        f"o.channel, o.currency, o.total_amount, o.metadata, o.order_ts "
        f"FROM {SRC_DB}.orders o "
        f"WHERE o.customer_id <= {cust_cutoff} "
        f"AND (o.ship_address_id IS NULL OR o.ship_address_id IN "
        f"  (SELECT address_id FROM {SRC_DB}.customer_addresses WHERE customer_id <= {cust_cutoff})) "
        f"ORDER BY o.order_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.orders o "
        f"WHERE o.customer_id <= {cust_cutoff} "
        f"AND (o.ship_address_id IS NULL OR o.ship_address_id IN "
        f"  (SELECT address_id FROM {SRC_DB}.customer_addresses WHERE customer_id <= {cust_cutoff}))",
    )

    # Check current size and adjust if needed
    cur.execute("SET SESSION foreign_key_checks = 1")
    cur.execute("SET SESSION unique_checks = 1")
    size = db_size_bytes(cur, DST_DB)
    log(f"Size after customers/orders: {gb(size)}")
    cur.execute("SET SESSION foreign_key_checks = 0")
    cur.execute("SET SESSION unique_checks = 0")

    # Order items - only for orders we copied, and products we have (all)
    copied = chunked_copy(
        cur, "order_items",
        f"INSERT INTO {DST_DB}.order_items (order_item_id, order_id, product_id, "
        f"quantity, unit_price, discount) "
        f"SELECT oi.order_item_id, oi.order_id, oi.product_id, "
        f"oi.quantity, oi.unit_price, oi.discount "
        f"FROM {SRC_DB}.order_items oi "
        f"WHERE oi.order_id IN (SELECT order_id FROM {DST_DB}.orders) "
        f"ORDER BY oi.order_item_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.order_items oi "
        f"WHERE oi.order_id IN (SELECT order_id FROM {DST_DB}.orders)",
    )

    # Payments - only for orders we copied
    copied = chunked_copy(
        cur, "payments",
        f"INSERT INTO {DST_DB}.payments (payment_id, order_id, method, amount, status, txn_ref, paid_ts) "
        f"SELECT p.payment_id, p.order_id, p.method, p.amount, p.status, p.txn_ref, p.paid_ts "
        f"FROM {SRC_DB}.payments p "
        f"WHERE p.order_id IN (SELECT order_id FROM {DST_DB}.orders) "
        f"ORDER BY p.payment_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.payments p "
        f"WHERE p.order_id IN (SELECT order_id FROM {DST_DB}.orders)",
    )

    # Product reviews - valid product AND valid customer
    copied = chunked_copy(
        cur, "product_reviews",
        f"INSERT INTO {DST_DB}.product_reviews (review_id, product_id, customer_id, "
        f"rating, title, body, helpful_votes, created_at) "
        f"SELECT r.review_id, r.product_id, r.customer_id, "
        f"r.rating, r.title, r.body, r.helpful_votes, r.created_at "
        f"FROM {SRC_DB}.product_reviews r "
        f"WHERE r.customer_id <= {cust_cutoff} "
        f"ORDER BY r.review_id",
        f"SELECT COUNT(*) FROM {SRC_DB}.product_reviews r "
        f"WHERE r.customer_id <= {cust_cutoff}",
    )

    # Re-enable checks
    cur.execute("SET SESSION foreign_key_checks = 1")
    cur.execute("SET SESSION unique_checks = 1")
    conn.commit()

    # Finalize: FULLTEXT indexes, view, stored procedure, trigger
    log("Adding FULLTEXT indexes, view, stored procedure, trigger...")
    for stmt in (
        f"ALTER TABLE {DST_DB}.products ADD FULLTEXT INDEX ftx_product_text (product_name, description)",
        f"ALTER TABLE {DST_DB}.product_reviews ADD FULLTEXT INDEX ftx_review_text (title, body)",
    ):
        try:
            cur.execute(stmt)
            conn.commit()
            log(f"  ok: FULLTEXT")
        except Exception as e:
            log(f"  WARN fulltext: {str(e)[:160]}")

    cur.execute(f"USE {DST_DB}")
    cur.execute("DROP VIEW IF EXISTS customer_order_summary")
    cur.execute(
        "CREATE VIEW customer_order_summary AS "
        "SELECT c.customer_id, c.email, c.segment, co.country_name, "
        "COUNT(DISTINCT o.order_id) AS order_count, "
        "COALESCE(SUM(o.total_amount), 0) AS lifetime_value, MAX(o.order_ts) AS last_order_ts "
        "FROM customers c JOIN countries co ON co.country_id = c.country_id "
        "LEFT JOIN orders o ON o.customer_id = c.customer_id "
        "GROUP BY c.customer_id, c.email, c.segment, co.country_name"
    )
    cur.execute("DROP PROCEDURE IF EXISTS get_customer_orders")
    cur.execute(
        "CREATE PROCEDURE get_customer_orders(IN p_customer_id BIGINT UNSIGNED) "
        "BEGIN SELECT o.order_id, o.order_ts, o.order_status, o.total_amount "
        "FROM orders o WHERE o.customer_id = p_customer_id ORDER BY o.order_ts DESC; END"
    )
    cur.execute("DROP TRIGGER IF EXISTS trg_orders_before_insert")
    cur.execute(
        "CREATE TRIGGER trg_orders_before_insert BEFORE INSERT ON orders FOR EACH ROW "
        "BEGIN IF NEW.order_ts IS NULL THEN SET NEW.order_ts = NOW(); END IF; "
        "IF NEW.currency IS NULL OR NEW.currency = '' THEN SET NEW.currency = 'USD'; END IF; END"
    )
    conn.commit()

    # Final report
    log("=" * 70)
    log("FINAL REPORT")
    cur.execute(
        "SELECT table_name, table_rows, data_length, index_length "
        "FROM information_schema.tables WHERE table_schema = %s AND table_type='BASE TABLE' "
        "ORDER BY (data_length + index_length) DESC",
        (DST_DB,),
    )
    total = 0
    for name, rows, dlen, ilen in cur.fetchall():
        size = int(dlen or 0) + int(ilen or 0)
        total += size
        log(f"  {name:<22} ~{int(rows or 0):>14,} rows   {gb(size):>10}")
    log(f"  {'TOTAL':<22} {'':>14}        {gb(total):>10}")
    log("=" * 70)

    conn.close()
    log(f"Done in {(time.time() - start) / 60:.1f} min.")


if __name__ == "__main__":
    main()
