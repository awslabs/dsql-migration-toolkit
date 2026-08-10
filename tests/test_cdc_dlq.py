# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the sink DLQ log-line parser (core/cdc_dlq)."""

from datetime import datetime, timezone

from dsql_migrator.core.cdc_dlq import parse_dlq_log_message


def test_parse_quarantined_line_extracts_table_offset_and_sqlstate() -> None:
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, "
        "offset=42): DSQL apply failed (sqlstate=42804)"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "orders"
    assert "offset=42" in rec.message
    assert rec.error_code == "42804"


def test_parse_dropping_line_without_dlq() -> None:
    msg = (
        "Dropping unapplicable record (no DLQ configured) "
        "topic=dsqlcdc.shop.payments, partition=3, offset=7: bad type"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "payments"
    assert "offset=7" in rec.message
    assert rec.error_code is None


def test_parse_non_dlq_lines_return_none() -> None:
    assert parse_dlq_log_message("INFO some routine connector log") is None
    assert parse_dlq_log_message("") is None
    assert parse_dlq_log_message(None) is None


def test_parse_carries_occurred_at_and_never_includes_row_values() -> None:
    ts = datetime(2026, 6, 26, tzinfo=timezone.utc)
    rec = parse_dlq_log_message(
        "Quarantined record to DLQ (topic=a.b.customers, partition=0, offset=9): x",
        occurred_at=ts,
    )
    assert rec is not None
    assert rec.occurred_at == ts
    assert rec.table == "customers"


def test_parse_keeps_sql_template_in_message() -> None:
    # The sink now appends the rendered SQL TEMPLATE (placeholders only) to the
    # reason; the parser must keep it so the UI / activity log can show it. It
    # carries column names and ``?`` -- never row values.
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.products, partition=0, "
        'offset=21): ERROR: column "_dlq_probe" of relation "products" does not '
        'exist | sql: INSERT INTO "shop"."products" ("id", "_dlq_probe") VALUES '
        '(?, ?) ON CONFLICT ("id") DO UPDATE SET "_dlq_probe" = EXCLUDED."_dlq_probe"'
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "products"
    assert "sql: INSERT INTO" in rec.message
    assert "_dlq_probe" in rec.message
    # Placeholders only -- no row values leaked.
    assert "?" in rec.message


def test_parse_keeps_surrogate_pk_in_message() -> None:
    # The sink appends the failed row's PK so an engineer can locate the source
    # row. A surrogate (integer) PK value is shown and must survive parsing; it
    # rides inside the reason with no special handling (same as the SQL template).
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.ecommerce.product_media, "
        "partition=0, offset=3): Value for column 'full_description' exceeds "
        "DSQL's 1048576-byte limit; quarantined. | pk: product_id=14"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "product_media"
    assert "pk: product_id=14" in rec.message
    # A permanent-limit rejection carries no SQLSTATE.
    assert rec.error_code is None


def test_parse_keeps_withheld_natural_key_pk_in_message() -> None:
    # A natural-key PK value that may be sensitive is withheld by the sink; the
    # column name still appears so the engineer knows which key identifies the row.
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.accounts, partition=1, "
        "offset=8): DSQL apply failed | pk: email=<withheld>"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "accounts"
    assert "pk: email=<withheld>" in rec.message
