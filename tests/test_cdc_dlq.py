# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the sink DLQ log-line parser (core/cdc_dlq)."""

from datetime import datetime, timezone

from dsql_migrator.core.cdc import SchemaDriftKind
from dsql_migrator.core.cdc_dlq import _table_from_topic, parse_dlq_log_message


def test_table_from_topic_returns_db_qualified_name() -> None:
    # <prefix>.<db>.<table> -> db.table (the key the monitor, table.include.list,
    # and the ADD COLUMN drift recovery all use). The db + table are the LAST two
    # segments regardless of how many segments the prefix has.
    assert _table_from_topic("dsqlcdc.shop.orders") == "shop.orders"
    assert _table_from_topic("a.b.customers") == "b.customers"
    # A multi-segment prefix still yields just db.table (last two segments).
    assert _table_from_topic("my.long.prefix.shop.orders") == "shop.orders"
    # Non-dotted / degenerate topics fall back to a stable, non-empty key.
    assert _table_from_topic("orders") == "orders"
    assert _table_from_topic("") == ""


def test_parse_quarantined_line_extracts_table_offset_and_sqlstate() -> None:
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, "
        "offset=42): DSQL apply failed (sqlstate=42804)"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    # db-qualified (db.table): the key the monitor / table.include.list / target
    # schema all use, and what the ADD COLUMN drift recovery needs.
    assert rec.table == "shop.orders"
    assert "offset=42" in rec.message
    assert rec.error_code == "42804"


def test_parse_dropping_line_without_dlq() -> None:
    msg = (
        "Dropping unapplicable record (no DLQ configured) "
        "topic=dsqlcdc.shop.payments, partition=3, offset=7: bad type"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "shop.payments"
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
    assert rec.table == "b.customers"


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
    assert rec.table == "shop.products"
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
    assert rec.table == "ecommerce.product_media"
    assert "pk: product_id=14" in rec.message
    # The PK is ALSO promoted to a structured field: an operator should not have to regex the
    # tool's own JSON to get it. It stays in the message because the downloadable error log
    # renders the message.
    assert rec.pk == "product_id=14"
    # A permanent-limit rejection carries no SQLSTATE (the sink's pre-write size guard raises a
    # DataException -- there is no server error), so it gets a stable synthetic class instead of
    # leaving the UI to match prose.
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_CODE

    assert rec.error_code == OVERSIZED_VALUE_CODE


def test_parse_keeps_withheld_natural_key_pk_in_message() -> None:
    # A natural-key PK value that may be sensitive is withheld by the sink; the
    # column name still appears so the engineer knows which key identifies the row.
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.accounts, partition=1, "
        "offset=8): DSQL apply failed | pk: email=<withheld>"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "shop.accounts"
    assert "pk: email=<withheld>" in rec.message


def test_parse_reads_leading_sqlstate_tag_and_derives_drift_kind() -> None:
    # v28 sink prefixes the quarantine reason with "sqlstate=<state> " so the
    # parser can classify the drift. 42703 = a source ADD COLUMN the target lacks.
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, "
        'offset=42): sqlstate=42703 ERROR: column "promo_code" does not exist '
        '| pk: id=7 | sql: INSERT INTO "shop"."orders" ("id", "promo_code") '
        'VALUES (?, ?) ON CONFLICT ("id") DO UPDATE SET "promo_code" = '
        'EXCLUDED."promo_code"'
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "shop.orders"
    assert rec.error_code == "42703"
    # The drift kind is derived from the SQLSTATE, not stored.
    assert rec.drift_kind is SchemaDriftKind.ADD_COLUMN


def test_parse_leading_sqlstate_takes_precedence_over_later_number() -> None:
    # The tag is at the FRONT so it is the first sqlstate= token even when the SQL
    # template or message later mentions other digits; 23502 = DROP of a NOT NULL col.
    msg = (
        "Quarantined record to DLQ (topic=a.b.line_items, partition=2, offset=99): "
        "sqlstate=23502 ERROR: null value in column violates not-null constraint"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.error_code == "23502"
    assert rec.drift_kind is SchemaDriftKind.DROP_COLUMN


def test_parse_ordinary_poison_row_has_no_drift_kind() -> None:
    # An oversized-value rejection carries no SQLSTATE tag -> not schema drift.
    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.media, partition=0, offset=3): "
        "Value for column 'blob' exceeds DSQL's 1048576-byte limit; quarantined."
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    # Classified, but NOT as schema drift: the synthetic class must never be mistaken for one
    # of the SQLSTATEs classify_schema_drift maps, or an oversized row would raise a "source
    # schema change" banner. Not a 5-character SQLSTATE shape, so it cannot collide.
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_CODE

    assert rec.error_code == OVERSIZED_VALUE_CODE
    assert len(OVERSIZED_VALUE_CODE) != 5
    assert rec.drift_kind is None


def test_the_quarantine_record_carries_the_op_that_decides_the_recovery() -> None:
    """Without the op an operator cannot tell which recovery applies, and one of the two
    cannot be discovered from the source at all.

    CDC replicates STATE, so a quarantined INSERT or UPDATE converges by re-reading the
    source's CURRENT row -- the intermediate value is not needed. A quarantined DELETE is the
    opposite: the row is gone from the source, so no source query can reveal that the target
    still holds it; it must be deleted on the target. A real operator grepped 2,705 sink log
    lines for the operation and found zero. The sink tags it as of plugin v43.
    """
    ops = {
        "d": "d",
        "c/r": "c/r",
        "u": "u",
        "d (tombstone)": "d (tombstone)",
    }
    for tag, expected in ops.items():
        msg = (
            "Quarantined record to DLQ (topic=dsqlcdc.app.orders, partition=1, offset=6): "
            f"boom | pk: id=510 | op: {tag}"
        )
        rec = parse_dlq_log_message(msg)
        assert rec is not None, tag
        assert rec.op == expected, tag
        # The op tag must not leak into the pk field.
        assert rec.pk == "id=510", tag

    # A record from a sink OLDER than v43 has no op. It must stay None, never guessed: a wrong
    # op sends the operator to the wrong recovery, which is worse than no op at all.
    old = parse_dlq_log_message(
        "Quarantined record to DLQ (topic=dsqlcdc.app.orders, partition=1, offset=6): "
        "boom | pk: id=510"
    )
    assert old is not None and old.op is None
    assert old.pk == "id=510"


def test_the_quarantine_record_keeps_the_kafka_coordinates() -> None:
    """For a quarantined DELETE the dead-letter topic is the ONLY place the row still exists.

    The source no longer has it, so the Kafka coordinates are the operator's only route to the
    original record (key + value + Connect context headers). The parser captured partition
    since its first version and then discarded it.
    """
    rec = parse_dlq_log_message(
        "Quarantined record to DLQ (topic=dsqlcdc.app.orders, partition=3, offset=99): boom"
    )
    assert rec is not None
    assert (rec.topic, rec.partition, rec.offset) == ("dsqlcdc.app.orders", 3, 99)
    assert rec.table == "app.orders"


def test_the_log4j_logger_suffix_is_stripped_from_the_operator_message() -> None:
    """The MSK Connect worker's layout appends "(logger:line)" to every line.

    Its log4j config is AWS-managed and not operator-editable, so the Java class name and a
    source line number were landing inside a field the operator reads. It cannot be turned off
    at the source; it is stripped here.
    """
    rec = parse_dlq_log_message(
        "Quarantined record to DLQ (topic=dsqlcdc.app.orders, partition=1, offset=6): "
        "Value for column 'content' exceeds DSQL's 1048576-byte limit; quarantined. "
        "| pk: id=1 (dev.dsqlmigrator.connect.DsqlSinkTask:759)"
    )
    assert rec is not None
    assert "DsqlSinkTask" not in rec.message, rec.message
    assert ":759" not in rec.message
    # ...and stripping it must not eat the PK that sits immediately before it.
    assert rec.pk == "id=1"
    assert rec.message.endswith("| pk: id=1")


def test_the_pk_parser_stops_at_the_next_section() -> None:
    """The sink appends " | op: ..." and " | sql: ..." after the PK; the PK must not swallow
    them (the SQL template is long, and a bloated pk field is unusable as a column)."""
    rec = parse_dlq_log_message(
        "Quarantined record to DLQ (topic=dsqlcdc.app.orders, partition=1, offset=6): "
        "sqlstate=23505 duplicate key | pk: user_id=42,id=510 | op: c/r "
        "| sql: INSERT INTO \"app\".\"orders\" (\"id\") VALUES (?) ON CONFLICT ..."
    )
    assert rec is not None
    assert rec.pk == "user_id=42,id=510"
    assert rec.op == "c/r"
    # A real SQLSTATE still wins over the synthetic class.
    assert rec.error_code == "23505"
    # The SQL template stays in the message (it is what the error log renders).
    assert "ON CONFLICT" in rec.message


def test_the_sink_wires_the_event_into_the_size_guard_quarantine() -> None:
    """Cross-language guard: the op is useless if the call site does not pass the event.

    Proven necessary by mutation — dropping ``event`` from the size-guard's reportOrThrow call
    COMPILES and every Java test still passed, because the overload without it is a valid
    signature. That path is the one the reported record came from, and it is the only path
    where the op cannot be inferred from anything else (no SQL is rendered, because nothing was
    attempted).
    """
    import pathlib

    sink = (
        pathlib.Path(__file__).resolve().parents[1]
        / "connectors/dsql-sink/src/main/java/dev/dsqlmigrator/connect/DsqlSinkTask.java"
    )
    src = sink.read_text(encoding="utf-8")

    # The size guard must use the 3-arg overload (record, event, cause).
    guard = src[src.index("String oversized = oversizedColumn(event);") :]
    guard = guard[: guard.index("batch.add(")]
    assert "reportOrThrow(\n            record,\n            event," in guard, guard[:600]

    # Both quarantine paths that HAVE an event must render the op.
    assert src.count("opSuffix(event)") >= 2, "the op must reach both quarantine reasons"
    # ...and the parse-failure path must NOT invent one (there is no event there).
    parse_fail = src[src.index("// Unparseable/poison envelope") :]
    parse_fail = parse_fail[: parse_fail.index("if (event.isDelete()")]
    assert "opSuffix" not in parse_fail
