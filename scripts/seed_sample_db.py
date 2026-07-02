#!/usr/bin/env python3
"""Seed a realistic, large (>=10GB) sample MySQL database for DSQL migration testing.

This is a standalone operational utility (not part of the shipped tool). It creates a
customer-centric e-commerce schema with rich relations and MySQL features that are
interesting for an Aurora DSQL compatibility assessment (foreign keys, AUTO_INCREMENT,
composite/unique/secondary indexes, ENUM/SET, JSON, generated columns, FULLTEXT index,
a view, a stored procedure, and a trigger).

Data is generated entirely server-side (set-based INSERT ... SELECT), so almost nothing
travels over the network -- only compact SQL statements. The large fact tables are grown
in bounded chunks (one transaction per chunk) until the database reaches the size target.

Connection: host/user are fixed for this cluster; the password is read from the MYSQL_PWD
environment variable (never logged).
"""
from __future__ import annotations

import os
import sys
import time
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
DB = _cfg("DB_NAME", "customers_sample")

# Default target ~10.6 GB so the final size is comfortably above 10 GB.
TARGET_BYTES = int(float(_cfg("SEED_TARGET_GB", "10.6")) * (1024 ** 3))

SEED_CHUNK = 250_000
EXPAND_CHUNK = 500_000

# Base/dimension sizes (seed). Kept modest; fact tables are grown by expansion.
N_COUNTRIES = 40          # set after countries insert
N_CATEGORIES = 300
N_SUPPLIERS = 5_000
N_PRODUCTS = 200_000
N_CUSTOMERS = 500_000
N_ADDRESSES = 750_000     # ~1.5 per customer
N_ORDERS = 1_000_000
N_ORDER_ITEMS = 3_000_000
N_PAYMENTS = 1_000_000
N_REVIEWS = 2_000_000


def log(msg: str) -> None:
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def connect(db: str | None = None) -> pymysql.connections.Connection:
    pw = _ENV.get("DB_PASSWORD") or os.environ.get("MYSQL_PWD")
    if not pw:
        try:
            with open("/tmp/.dsql_seed_pwd") as f:
                pw = f.read().strip()
        except FileNotFoundError:
            pass
    if not pw:
        log("FATAL: no DB password. Set DB_PASSWORD in .env (or MYSQL_PWD env var).")
        sys.exit(1)
    return pymysql.connect(
        host=HOST, port=PORT, user=USER, password=pw, database=db,
        connect_timeout=15, read_timeout=3600, write_timeout=3600,
        autocommit=False, charset="utf8mb4",
    )


def exec_sql(cur, sql: str) -> None:
    cur.execute(sql)


def db_size_bytes(cur) -> int:
    cur.execute(
        "SELECT COALESCE(SUM(data_length + index_length), 0) "
        "FROM information_schema.tables WHERE table_schema = %s",
        (DB,),
    )
    return int(cur.fetchone()[0] or 0)


def gb(n: int) -> str:
    return f"{n / (1024 ** 3):.2f} GB"


# --------------------------------------------------------------------------- DDL

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

# Reference data (small, kept realistic). region_id -> name.
REGIONS = [(1, "Americas"), (2, "EMEA"), (3, "APAC"), (4, "LATAM"), (5, "Other")]
COUNTRIES = [
    ("US", "United States", 1), ("CA", "Canada", 1), ("MX", "Mexico", 4),
    ("BR", "Brazil", 4), ("AR", "Argentina", 4), ("CL", "Chile", 4),
    ("GB", "United Kingdom", 2), ("FR", "France", 2), ("DE", "Germany", 2),
    ("ES", "Spain", 2), ("IT", "Italy", 2), ("NL", "Netherlands", 2),
    ("SE", "Sweden", 2), ("NO", "Norway", 2), ("FI", "Finland", 2),
    ("PL", "Poland", 2), ("IE", "Ireland", 2), ("PT", "Portugal", 2),
    ("CH", "Switzerland", 2), ("AT", "Austria", 2), ("AE", "United Arab Emirates", 2),
    ("SA", "Saudi Arabia", 2), ("ZA", "South Africa", 2), ("EG", "Egypt", 2),
    ("KR", "South Korea", 3), ("JP", "Japan", 3), ("CN", "China", 3),
    ("IN", "India", 3), ("AU", "Australia", 3), ("NZ", "New Zealand", 3),
    ("SG", "Singapore", 3), ("MY", "Malaysia", 3), ("TH", "Thailand", 3),
    ("VN", "Vietnam", 3), ("ID", "Indonesia", 3), ("PH", "Philippines", 3),
    ("TW", "Taiwan", 3), ("HK", "Hong Kong", 3), ("TR", "Turkey", 2),
    ("IL", "Israel", 2),
]


def build_numbers(cur) -> None:
    log("Building helper number tables (_digits, _numbers up to 1,000,000)...")
    exec_sql(cur, "DROP TABLE IF EXISTS _numbers")
    exec_sql(cur, "DROP TABLE IF EXISTS _digits")
    exec_sql(cur, "CREATE TABLE _digits (d TINYINT UNSIGNED NOT NULL)")
    exec_sql(cur, "INSERT INTO _digits (d) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)")
    exec_sql(cur, "CREATE TABLE _numbers (n INT UNSIGNED PRIMARY KEY)")
    exec_sql(
        cur,
        "INSERT INTO _numbers (n) SELECT 1 + d0.d + d1.d*10 + d2.d*100 + d3.d*1000 "
        "+ d4.d*10000 + d5.d*100000 "
        "FROM _digits d0, _digits d1, _digits d2, _digits d3, _digits d4, _digits d5",
    )
    cur.connection.commit()


# Tables this cluster creates with AUTO_INCREMENT get gaps under
# innodb_autoinc_lock_mode=2 (interleaved). To keep every synthetic foreign key valid,
# we reference parents through dense, gap-free lookup maps (map_<name>(seq, id)) built
# with ROW_NUMBER(), and pick a random parent via JOIN ... ON seq = 1 + FLOOR(RAND()*count).
MAP_NAMES = ["categories", "suppliers", "products", "customers", "addresses", "orders"]


def build_map(cur, name: str, src_table: str, pk: str) -> int:
    exec_sql(cur, f"DROP TABLE IF EXISTS map_{name}")
    exec_sql(cur, f"CREATE TABLE map_{name} (seq INT UNSIGNED PRIMARY KEY, id BIGINT UNSIGNED NOT NULL)")
    exec_sql(
        cur,
        f"INSERT INTO map_{name} (seq, id) "
        f"SELECT ROW_NUMBER() OVER (ORDER BY {pk}), {pk} FROM {src_table}",
    )
    cur.connection.commit()
    cur.execute(f"SELECT COUNT(*) FROM map_{name}")
    n = int(cur.fetchone()[0])
    log(f"  built map_{name}: {n:,} ids")
    return n


def seed_reference(cur) -> dict:
    log("Seeding reference data (regions, countries, categories, suppliers)...")
    cur.executemany("INSERT INTO regions (region_id, region_name) VALUES (%s, %s)", REGIONS)
    rows = [(i + 1, region, code, name) for i, (code, name, region) in enumerate(COUNTRIES)]
    cur.executemany(
        "INSERT INTO countries (country_id, region_id, country_code, country_name) "
        "VALUES (%s, %s, %s, %s)",
        rows,
    )
    n_countries = len(rows)  # countries use explicit contiguous ids (no gaps)

    # Top-level categories, then child categories referencing a real parent via map.
    exec_sql(
        cur,
        "INSERT INTO categories (parent_category_id, category_name, depth) "
        "SELECT NULL, CONCAT('Category ', n), 0 FROM _numbers WHERE n <= 50",
    )
    cur.connection.commit()
    c_parent = build_map(cur, "categories", "categories", "category_id")
    exec_sql(
        cur,
        "INSERT INTO categories (parent_category_id, category_name, depth) "
        "SELECT (SELECT id FROM map_categories ORDER BY RAND() LIMIT 1), "
        "CONCAT('Subcategory ', n), 1 FROM _numbers WHERE n <= 250",
    )
    cur.connection.commit()
    c_cat = build_map(cur, "categories", "categories", "category_id")  # rebuild over all

    exec_sql(
        cur,
        "INSERT INTO suppliers (country_id, supplier_name, contact_info, rating) "
        f"SELECT 1 + FLOOR(RAND() * {n_countries}), CONCAT('Supplier ', n), "
        "JSON_OBJECT('email', CONCAT('supplier', n, '@vendor.example'), "
        "'phone', CONCAT('+', 1 + FLOOR(RAND()*99), '-', FLOOR(RAND()*9000000))), "
        f"ROUND(RAND() * 5, 2) FROM _numbers WHERE n <= {N_SUPPLIERS}",
    )
    cur.connection.commit()
    c_sup = build_map(cur, "suppliers", "suppliers", "supplier_id")
    return {"countries": n_countries, "categories": c_cat, "suppliers": c_sup}


def seed_generated(cur, label: str, total: int, sql_template: str) -> None:
    """Run a chunked set-based INSERT ... SELECT FROM _numbers WHERE n BETWEEN a AND b.

    sql_template must contain a single ``{rng}`` placeholder for the WHERE range, and any
    range over [1, 1_000_000]. For totals > 1_000_000 the number range cycles.
    """
    log(f"Seeding {label}: target {total:,} rows...")
    done = 0
    while done < total:
        remaining = total - done
        size = min(SEED_CHUNK, remaining)
        base = done % 1_000_000
        a = base + 1
        b = base + size
        if b > 1_000_000:
            b = 1_000_000
            size = b - a + 1
        rng = f"n BETWEEN {a} AND {b}"
        exec_sql(cur, sql_template.format(rng=rng))
        cur.connection.commit()
        done += size
        if done % 1_000_000 == 0 or done >= total:
            log(f"  {label}: {done:,}/{total:,}")


def seed_fact_and_dims(cur, counts: dict) -> None:
    nc = counts["countries"]
    c_cat = counts["categories"]
    c_sup = counts["suppliers"]

    seed_generated(
        cur, "products", N_PRODUCTS,
        "INSERT INTO products (category_id, supplier_id, sku, product_name, description, "
        "unit_price, cost_price, status, tags, attributes, weight_kg, created_at) "
        "SELECT mc.id, ms.id, "
        "UPPER(SUBSTRING(MD5(CONCAT('sku', n, RAND())), 1, 32)), "
        "CONCAT('Product ', n, ' ', ELT(1+FLOOR(RAND()*6),'Pro','Max','Lite','Plus','Mini','Ultra')), "
        "CONCAT('High quality item ', n, '. ', REPEAT('lorem ipsum dolor sit amet ', 1+FLOOR(RAND()*8))), "
        "ROUND(5 + RAND()*995, 2), ROUND(2 + RAND()*400, 2), "
        "ELT(1+FLOOR(RAND()*4),'active','discontinued','draft','out_of_stock'), "
        "CASE FLOOR(RAND()*5) WHEN 0 THEN 'new' WHEN 1 THEN 'sale,featured' "
        "WHEN 2 THEN 'clearance' WHEN 3 THEN 'eco,imported' ELSE 'new,featured' END, "
        "JSON_OBJECT('color', ELT(1+FLOOR(RAND()*5),'red','blue','green','black','white'), "
        "'size', ELT(1+FLOOR(RAND()*4),'S','M','L','XL'), 'score', ROUND(RAND()*5,1)), "
        "ROUND(RAND()*20, 3), NOW() - INTERVAL FLOOR(RAND()*1000) DAY "
        f"FROM (SELECT n, 1+FLOOR(RAND()*{c_cat}) AS rs_cat, 1+FLOOR(RAND()*{c_sup}) AS rs_sup "
        "FROM _numbers WHERE {rng} LIMIT 4294967295) s "
        "JOIN map_categories mc ON mc.seq = s.rs_cat "
        "JOIN map_suppliers ms ON ms.seq = s.rs_sup",
    )
    c_prod = build_map(cur, "products", "products", "product_id")

    seed_generated(
        cur, "customers", N_CUSTOMERS,
        "INSERT INTO customers (email, first_name, last_name, country_id, segment, "
        "loyalty_points, preferences, created_at) "
        "SELECT CONCAT('user', n, '_', FLOOR(RAND()*100000), '@example.com'), "
        "ELT(1+FLOOR(RAND()*10),'James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda','David','Sarah'), "
        "ELT(1+FLOOR(RAND()*10),'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Lee','Kim'), "
        f"1 + FLOOR(RAND()*{nc}), "
        "ELT(1+FLOOR(RAND()*4),'consumer','smb','enterprise','vip'), FLOOR(RAND()*10000), "
        "JSON_OBJECT('newsletter', IF(RAND()<0.5,TRUE,FALSE), 'lang', ELT(1+FLOOR(RAND()*3),'en','ko','ja')), "
        "NOW() - INTERVAL FLOOR(RAND()*1500) DAY FROM _numbers WHERE {rng}",
    )
    c_cust = build_map(cur, "customers", "customers", "customer_id")

    seed_generated(
        cur, "customer_addresses", N_ADDRESSES,
        "INSERT INTO customer_addresses (customer_id, country_id, address_type, line1, city, "
        "postal_code, is_default) "
        f"SELECT mcu.id, 1 + FLOOR(RAND()*{nc}), "
        "ELT(1+FLOOR(RAND()*3),'billing','shipping','both'), "
        "CONCAT(1+FLOOR(RAND()*9999),' ', ELT(1+FLOOR(RAND()*5),'Main St','Oak Ave','Pine Rd','Elm Blvd','Cedar Ln')), "
        "ELT(1+FLOOR(RAND()*6),'Springfield','Riverside','Franklin','Greenville','Bristol','Clinton'), "
        "LPAD(FLOOR(RAND()*99999), 5, '0'), IF(RAND()<0.4,1,0) "
        f"FROM (SELECT n, 1+FLOOR(RAND()*{c_cust}) AS rs_cust "
        "FROM _numbers WHERE {rng} LIMIT 4294967295) s "
        "JOIN map_customers mcu ON mcu.seq = s.rs_cust",
    )
    c_addr = build_map(cur, "addresses", "customer_addresses", "address_id")

    seed_generated(
        cur, "orders", N_ORDERS,
        "INSERT INTO orders (customer_id, ship_address_id, order_status, channel, currency, "
        "total_amount, metadata, order_ts) "
        "SELECT mcu.id, ma.id, "
        "ELT(1+FLOOR(RAND()*6),'pending','paid','shipped','delivered','cancelled','refunded'), "
        "ELT(1+FLOOR(RAND()*4),'web','mobile','store','partner'), "
        "ELT(1+FLOOR(RAND()*3),'USD','EUR','KRW'), ROUND(10 + RAND()*2000, 2), "
        "JSON_OBJECT('coupon', IF(RAND()<0.3, CONCAT('CPN', FLOOR(RAND()*9999)), NULL), "
        "'gift', IF(RAND()<0.1,TRUE,FALSE)), "
        "NOW() - INTERVAL FLOOR(RAND()*1095) DAY - INTERVAL FLOOR(RAND()*86400) SECOND "
        f"FROM (SELECT n, 1+FLOOR(RAND()*{c_cust}) AS rs_cust, 1+FLOOR(RAND()*{c_addr}) AS rs_addr "
        "FROM _numbers WHERE {rng} LIMIT 4294967295) s "
        "JOIN map_customers mcu ON mcu.seq = s.rs_cust "
        "JOIN map_addresses ma ON ma.seq = s.rs_addr",
    )
    c_ord = build_map(cur, "orders", "orders", "order_id")

    seed_generated(
        cur, "order_items", N_ORDER_ITEMS,
        "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) "
        "SELECT mo.id, mp.id, 1 + FLOOR(RAND()*8), ROUND(5 + RAND()*500, 2), ROUND(RAND()*30, 2) "
        f"FROM (SELECT 1+FLOOR(RAND()*{c_ord}) AS rs_ord, 1+FLOOR(RAND()*{c_prod}) AS rs_prod "
        "FROM _numbers WHERE {rng} LIMIT 4294967295) s "
        "JOIN map_orders mo ON mo.seq = s.rs_ord "
        "JOIN map_products mp ON mp.seq = s.rs_prod",
    )
    seed_generated(
        cur, "payments", N_PAYMENTS,
        "INSERT INTO payments (order_id, method, amount, status, txn_ref, paid_ts) "
        "SELECT mo.id, ELT(1+FLOOR(RAND()*5),'card','paypal','bank_transfer','wallet','cod'), "
        "ROUND(10 + RAND()*2000, 2), "
        "ELT(1+FLOOR(RAND()*4),'authorized','captured','failed','refunded'), "
        "REPLACE(UUID(),'-',''), NOW() - INTERVAL FLOOR(RAND()*1095) DAY "
        f"FROM (SELECT 1+FLOOR(RAND()*{c_ord}) AS rs_ord "
        "FROM _numbers WHERE {rng} LIMIT 4294967295) s "
        "JOIN map_orders mo ON mo.seq = s.rs_ord",
    )
    seed_generated(
        cur, "product_reviews", N_REVIEWS,
        "INSERT INTO product_reviews (product_id, customer_id, rating, title, body, "
        "helpful_votes, created_at) "
        "SELECT mp.id, mcu.id, 1 + FLOOR(RAND()*5), CONCAT('Review ', s.n), "
        "CONCAT('This product ', ELT(1+FLOOR(RAND()*4),'exceeded','met','fell short of','matched'), "
        "' my expectations. ', REPEAT('Great value for the price. ', 1+FLOOR(RAND()*6))), "
        "FLOOR(RAND()*500), NOW() - INTERVAL FLOOR(RAND()*1000) DAY "
        f"FROM (SELECT n, 1+FLOOR(RAND()*{c_prod}) AS rs_prod, 1+FLOOR(RAND()*{c_cust}) AS rs_cust "
        "FROM _numbers WHERE {rng} LIMIT 4294967295) s "
        "JOIN map_products mp ON mp.seq = s.rs_prod "
        "JOIN map_customers mcu ON mcu.seq = s.rs_cust",
    )


# Expansion: copy rows from the seed pool [1, seed_max] in PK ranges, regenerating
# measure columns (and unique txn_ref) so new rows are fresh. FK columns are preserved
# and therefore remain valid. One transaction per chunk keeps undo/binlog bounded.
EXPAND_SPECS = {
    "order_items": {
        "pk": "order_item_id",
        "sql": "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount) "
               "SELECT order_id, product_id, 1+FLOOR(RAND()*8), ROUND(5+RAND()*500,2), "
               "ROUND(RAND()*30,2) FROM order_items WHERE order_item_id BETWEEN {a} AND {b}",
    },
    "product_reviews": {
        "pk": "review_id",
        "sql": "INSERT INTO product_reviews (product_id, customer_id, rating, title, body, "
               "helpful_votes, created_at) SELECT product_id, customer_id, rating, title, body, "
               "FLOOR(RAND()*500), created_at FROM product_reviews WHERE review_id BETWEEN {a} AND {b}",
    },
    "payments": {
        "pk": "payment_id",
        "sql": "INSERT INTO payments (order_id, method, amount, status, txn_ref, paid_ts) "
               "SELECT order_id, method, ROUND(10+RAND()*2000,2), status, REPLACE(UUID(),'-',''), "
               "paid_ts FROM payments WHERE payment_id BETWEEN {a} AND {b}",
    },
    "orders": {
        "pk": "order_id",
        "sql": "INSERT INTO orders (customer_id, ship_address_id, order_status, channel, currency, "
               "total_amount, metadata, order_ts) SELECT customer_id, ship_address_id, order_status, "
               "channel, currency, ROUND(10+RAND()*2000,2), metadata, order_ts "
               "FROM orders WHERE order_id BETWEEN {a} AND {b}",
    },
}

# order_items dominates the growth; reviews add wide (TEXT) rows; payments/orders for balance.
EXPAND_ROTATION = ["order_items", "order_items", "order_items", "product_reviews", "payments", "orders"]


def expand_to_target(cur) -> None:
    state = {}
    for tbl, spec in EXPAND_SPECS.items():
        cur.execute(f"SELECT COALESCE(MAX({spec['pk']}), 0) FROM {tbl}")
        seed_max = int(cur.fetchone()[0] or 0)
        state[tbl] = {"pos": 0, "seed_max": seed_max}
    log("Seed pool sizes for expansion: "
        + ", ".join(f"{t}={state[t]['seed_max']:,}" for t in EXPAND_SPECS))

    size = db_size_bytes(cur)
    log(f"Size before expansion: {gb(size)} (target {gb(TARGET_BYTES)})")
    rot = 0
    step = 0
    last_log = time.time()
    while size < TARGET_BYTES:
        tbl = EXPAND_ROTATION[rot % len(EXPAND_ROTATION)]
        rot += 1
        st = state[tbl]
        seed_max = st["seed_max"]
        if seed_max <= 0:
            continue
        a = st["pos"] + 1
        b = st["pos"] + EXPAND_CHUNK
        if a > seed_max:
            a, b = 1, min(EXPAND_CHUNK, seed_max)
        if b > seed_max:
            b = seed_max
        st["pos"] = b if b < seed_max else 0
        exec_sql(cur, EXPAND_SPECS[tbl]["sql"].format(a=a, b=b))
        cur.connection.commit()
        step += 1
        if step % 6 == 0 or (time.time() - last_log) > 30:
            size = db_size_bytes(cur)
            last_log = time.time()
            log(f"  expand step {step}: +{tbl} [{a:,}-{b:,}] -> {gb(size)}")
    log(f"Reached target. Size: {gb(size)}")


def finalize(cur) -> None:
    log("Adding FULLTEXT indexes, view, stored procedure, and trigger...")
    # FULLTEXT (added after bulk load so it is not maintained during inserts).
    for stmt in (
        "ALTER TABLE products ADD FULLTEXT INDEX ftx_product_text (product_name, description)",
        "ALTER TABLE product_reviews ADD FULLTEXT INDEX ftx_review_text (title, body)",
    ):
        try:
            exec_sql(cur, stmt)
            cur.connection.commit()
            log(f"  ok: {stmt.split('ADD')[1].strip()[:60]}")
        except Exception as e:  # noqa: BLE001
            log(f"  WARN fulltext: {str(e)[:160]}")

    exec_sql(cur, "DROP VIEW IF EXISTS customer_order_summary")
    exec_sql(
        cur,
        "CREATE VIEW customer_order_summary AS "
        "SELECT c.customer_id, c.email, c.segment, co.country_name, "
        "COUNT(DISTINCT o.order_id) AS order_count, "
        "COALESCE(SUM(o.total_amount), 0) AS lifetime_value, MAX(o.order_ts) AS last_order_ts "
        "FROM customers c JOIN countries co ON co.country_id = c.country_id "
        "LEFT JOIN orders o ON o.customer_id = c.customer_id "
        "GROUP BY c.customer_id, c.email, c.segment, co.country_name",
    )
    exec_sql(cur, "DROP PROCEDURE IF EXISTS get_customer_orders")
    exec_sql(
        cur,
        "CREATE PROCEDURE get_customer_orders(IN p_customer_id BIGINT UNSIGNED) "
        "BEGIN SELECT o.order_id, o.order_ts, o.order_status, o.total_amount "
        "FROM orders o WHERE o.customer_id = p_customer_id ORDER BY o.order_ts DESC; END",
    )
    exec_sql(cur, "DROP TRIGGER IF EXISTS trg_orders_before_insert")
    exec_sql(
        cur,
        "CREATE TRIGGER trg_orders_before_insert BEFORE INSERT ON orders FOR EACH ROW "
        "BEGIN IF NEW.order_ts IS NULL THEN SET NEW.order_ts = NOW(); END IF; "
        "IF NEW.currency IS NULL OR NEW.currency = '' THEN SET NEW.currency = 'USD'; END IF; END",
    )
    cur.connection.commit()

    log("Dropping helper tables (_numbers, _digits, map_*)...")
    exec_sql(cur, "DROP TABLE IF EXISTS _numbers")
    exec_sql(cur, "DROP TABLE IF EXISTS _digits")
    for name in MAP_NAMES:
        exec_sql(cur, f"DROP TABLE IF EXISTS map_{name}")
    cur.connection.commit()


def report(cur) -> None:
    log("=" * 70)
    log("FINAL REPORT")
    cur.execute(
        "SELECT table_name, table_rows, data_length, index_length "
        "FROM information_schema.tables WHERE table_schema = %s AND table_type='BASE TABLE' "
        "ORDER BY (data_length + index_length) DESC",
        (DB,),
    )
    total = 0
    for name, rows, dlen, ilen in cur.fetchall():
        size = int(dlen or 0) + int(ilen or 0)
        total += size
        log(f"  {name:<22} ~{int(rows or 0):>14,} rows   {gb(size):>10}")
    log(f"  {'TOTAL':<22} {'':>14}        {gb(total):>10}")

    cur.execute("SELECT COUNT(*) FROM information_schema.views WHERE table_schema=%s", (DB,))
    n_views = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema=%s", (DB,))
    n_routines = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema=%s", (DB,))
    n_triggers = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=%s AND index_type='FULLTEXT'",
        (DB,),
    )
    n_ft = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.referential_constraints WHERE constraint_schema=%s",
        (DB,),
    )
    n_fk = cur.fetchone()[0]
    log(f"  objects: views={n_views} routines={n_routines} triggers={n_triggers} "
        f"fulltext_indexes={n_ft} foreign_keys={n_fk}")
    log("=" * 70)


def main() -> None:
    start = time.time()
    log(f"Starting seed of `{DB}` on {HOST} (target {gb(TARGET_BYTES)})")

    root = connect(None)
    with root.cursor() as cur:
        exec_sql(cur, f"CREATE DATABASE IF NOT EXISTS {DB} "
                      "CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci")
        root.commit()
    root.close()

    conn = connect(DB)
    try:
        with conn.cursor() as cur:
            exec_sql(cur, "SET SESSION foreign_key_checks = 0")
            exec_sql(cur, "SET SESSION unique_checks = 0")
            log("Creating schema (11 tables, FKs, indexes, generated columns)...")
            for stmt in DDL:
                exec_sql(cur, stmt)
            conn.commit()

            build_numbers(cur)
            counts = seed_reference(cur)
            seed_fact_and_dims(cur, counts)

            exec_sql(cur, "SET SESSION foreign_key_checks = 1")
            exec_sql(cur, "SET SESSION unique_checks = 1")
            expand_to_target(cur)

            finalize(cur)
            report(cur)
    finally:
        conn.close()

    log(f"Done in {(time.time() - start) / 60:.1f} min.")


if __name__ == "__main__":
    main()
