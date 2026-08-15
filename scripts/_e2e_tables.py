# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Single source of truth for the E2E migration-test table sets.

The end-to-end harness (run_e2e_migration.py), the Full Load harness
(run_full_load_harness.py), and the CDC consistency monitor
(cdc_consistency_monitor.py) all need to agree on the EXACT ordered table list
(and per-table PK) for a given test schema. That list used to be copy-pasted into
each script, which drifts. This module is the one place it lives, keyed by schema
name (``CDC_WORKLOAD_SCHEMA``), so switching schemas is one env var + one entry.

Order matters: tables are listed in FK-dependency order (parents before children)
so Full Load / drop / compare iterate them safely.

This is an operational test utility (scripts/), NOT shipped app code.
"""
from __future__ import annotations

# schema -> (ordered table list, [(table, single-column PK)])
_SETS: dict[str, tuple[list[str], list[tuple[str, str]]]] = {
    # The original 11-table customers_sample_new schema (dependency order).
    "customers_sample_new": (
        [
            "categories", "countries", "regions", "suppliers", "products",
            "customers", "customer_addresses", "orders", "order_items",
            "payments", "product_reviews",
        ],
        [
            ("categories", "category_id"),
            ("countries", "country_id"),
            ("regions", "region_id"),
            ("suppliers", "supplier_id"),
            ("products", "product_id"),
            ("customers", "customer_id"),
            ("customer_addresses", "address_id"),
            ("orders", "order_id"),
            ("order_items", "order_item_id"),
            ("payments", "payment_id"),
            ("product_reviews", "review_id"),
        ],
    ),
    # Large-scale variant (~63.5M rows) used for Full Load THROUGHPUT measurement:
    # same 11-table shape as customers_sample_new but with big tables (order_items
    # ~33.6M, product_reviews ~11.7M, orders/payments ~8M). The customer_order_summary
    # VIEW is intentionally excluded (Full Load loads base-table rows only).
    "customers_sample": (
        [
            "categories", "countries", "regions", "suppliers", "products",
            "customers", "customer_addresses", "orders", "order_items",
            "payments", "product_reviews",
        ],
        [
            ("categories", "category_id"),
            ("countries", "country_id"),
            ("regions", "region_id"),
            ("suppliers", "supplier_id"),
            ("products", "product_id"),
            ("customers", "customer_id"),
            ("customer_addresses", "address_id"),
            ("orders", "order_id"),
            ("order_items", "order_item_id"),
            ("payments", "payment_id"),
            ("product_reviews", "review_id"),
        ],
    ),
    # Seoul E2E demo schema (scripts/seed_sample_db.py): same 11-table shape as
    # customers_sample_new, ~8.5M rows (order_items 3M, product_reviews 2M,
    # orders/payments 1M, customer_addresses 750k, customers 500k, products 200k).
    # Used for the ap-northeast-2 Full Load + CDC end-to-end run.
    "ecommerce_demo": (
        [
            "categories", "countries", "regions", "suppliers", "products",
            "customers", "customer_addresses", "orders", "order_items",
            "payments", "product_reviews",
        ],
        [
            ("categories", "category_id"),
            ("countries", "country_id"),
            ("regions", "region_id"),
            ("suppliers", "supplier_id"),
            ("products", "product_id"),
            ("customers", "customer_id"),
            ("customer_addresses", "address_id"),
            ("orders", "order_id"),
            ("order_items", "order_item_id"),
            ("payments", "payment_id"),
            ("product_reviews", "review_id"),
        ],
    ),
    # New type-coverage schema: a small parent->child/lob FK chain that exercises
    # the maximum MySQL type/syntax surface (incl. LOB). typetest_loud and
    # typetest_spatial are intentionally EXCLUDED from the migrated set -- they
    # demonstrate failure paths in their own isolated runs/notes.
    "migration_typetest": (
        ["typetest_parent", "typetest_child", "typetest_lob"],
        [
            ("typetest_parent", "parent_id"),
            ("typetest_child", "child_id"),
            ("typetest_lob", "lob_id"),
        ],
    ),
    # Value-fidelity edge schema (scripts/seed_fullload_edgecases.py): tables that
    # stress the VALUE surface the type-coverage schema does not -- MySQL zero-dates,
    # 4-byte UTF-8 / emoji / combining chars, NULL vs empty string, BIGINT UNSIGNED
    # near 2^64, an empty table, and byte-budget-boundary wide rows. These MUST
    # migrate byte-identically (verified by CHECKSUM + per-PK reconcile) or fail
    # loudly per the documented contract. ``edge_empty`` is intentionally row-free
    # (empty-table edge). ``edge_zerodate_loud`` is EXCLUDED from the migrated set
    # (its own loud-failure demonstration), mirroring typetest_loud.
    "migration_edge": (
        ["edge_numbers", "edge_text", "edge_temporal", "edge_wide", "edge_empty"],
        [
            ("edge_numbers", "id"),
            ("edge_text", "id"),
            ("edge_temporal", "id"),
            ("edge_wide", "id"),
            ("edge_empty", "id"),
        ],
    ),
    # Tricky-SCHEMA conversion edge schema (scripts/seed_schema_edgecases.py): the
    # FLOWABLE subset -- single integer PK, CDC-safe, that must convert (SC) then
    # Full Load + CDC byte-identically. Only the clean-reconciling tables are here.
    # EXCLUDED (validated at the Schema Conversion stage only, mirroring how
    # edge_zerodate_loud / typetest_spatial are left out here): sc_defaults (VIRTUAL
    # generated col not in the binlog), sc_spatial (bytea WKB / ST_AsBinary read),
    # sc_wide250 (>100 columns exceeds the Validator's per-row checksum arg limit --
    # a separate concern), the reserved/unicode/non-int/binary-PK tables, and every
    # sc_only_* table.
    "migration_schema": (
        [
            "sc_int_types", "sc_decimal_float", "sc_temporal", "sc_string_binary",
            "sc_enum_set", "sc_indexes", "sc_collation", "sc_partition", "sc_comments",
        ],
        [
            ("sc_int_types", "id"),
            ("sc_decimal_float", "id"),
            ("sc_temporal", "id"),
            ("sc_string_binary", "id"),
            ("sc_enum_set", "id"),
            ("sc_indexes", "id"),
            ("sc_collation", "id"),
            ("sc_partition", "id"),
            ("sc_comments", "id"),
        ],
    ),
    # us-east-1 large-scale Full Load THROUGHPUT test: 20 uniform tables (~52.7 GB
    # / ~43M rows each, ~1 TB total), each a single BIGINT UNSIGNED AUTO_INCREMENT
    # PK `id`, no secondary indexes. No FK chain (independent tables), so order is
    # arbitrary. Drives the in-VPC ECS RunTask perf measurement.
    "dsql_test_multi": (
        [f"t{n:02d}" for n in range(1, 21)],
        [(f"t{n:02d}", "id") for n in range(1, 21)],
    ),
    # us-east-1 SINGLE huge-table Full Load test: one ~1 TB table (~865M rows),
    # BIGINT UNSIGNED AUTO_INCREMENT PK `id`, no secondary indexes. Exercises the
    # single-reader (one large table) throughput path vs dsql_test_multi's
    # multi-table parallelism.
    "dsql_test_large": (
        ["big_events"],
        [("big_events", "id")],
    ),
    # FAST storm-repro: same 20 uniform tables as dsql_test_multi but only ~500K
    # rows each (copied from the head of dsql_test_multi), so a 16x32 (=512
    # connection) load finishes the front-16 together in ~1-2 min and reproduces
    # the connection-storm at simultaneous completion WITHOUT waiting for a 1 TB
    # load. Same shape (BIGINT auto_increment PK `id`, dist_key for composite).
    "dsql_test_small": (
        [f"t{n:02d}" for n in range(1, 21)],
        [(f"t{n:02d}", "id") for n in range(1, 21)],
    ),
}


def tables_for(schema: str) -> list[str]:
    """Return the ordered table list for ``schema`` (raises if unknown)."""
    if schema not in _SETS:
        raise KeyError(
            f"unknown E2E schema {schema!r}; known: {sorted(_SETS)}. "
            "Add it to scripts/_e2e_tables.py."
        )
    return list(_SETS[schema][0])


def table_pks_for(schema: str) -> list[tuple[str, str]]:
    """Return the ordered [(table, pk)] list for ``schema`` (raises if unknown)."""
    if schema not in _SETS:
        raise KeyError(
            f"unknown E2E schema {schema!r}; known: {sorted(_SETS)}. "
            "Add it to scripts/_e2e_tables.py."
        )
    return list(_SETS[schema][1])
