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
