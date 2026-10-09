# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the sink DLQ log-line parser (core/cdc_dlq)."""

from datetime import datetime, timezone

import pytest

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


def test_a_source_truncate_quarantine_is_classified_as_its_own_drift_kind() -> None:
    """A source TRUNCATE is invisible at CDC time unless it gets a named banner.

    Live-measured on PostgreSQL 17.7 -> MSK -> Aurora DSQL: ``TRUNCATE`` on a captured table
    produced NO source record at all (Debezium's ``skipped.operations`` defaults to ``t``), so
    the target kept all 9 truncated rows while the source held 1 -- permanently. Both cdc-stack
    source connectors (PostgreSQL and MySQL) now set ``skipped.operations: none`` and, since
    Aurora DSQL has no TRUNCATE statement, the sink dead-letters the event by name. The MySQL
    shape is pinned by ``test_a_mysql_source_truncate_quarantine_is_classified_and_held``.
    That single quarantine is far below the DLQ badge's 50-record warn threshold, so the DRIFT
    classification is what actually surfaces it -- otherwise only Validation would, after the
    cut-over decision was in progress.
    """
    from dsql_migrator.core.cdc import SchemaDriftKind
    from dsql_migrator.core.cdc_dlq import SOURCE_TRUNCATE_CODE

    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.v45arr.arr_dlq, partition=0, offset=31): "
        "Cannot apply a source TRUNCATE of table v45arr.arr_dlq: Aurora DSQL has no TRUNCATE "
        "statement, so there is no correct apply and this event is quarantined rather than "
        "guessed at. The target still holds the rows the source truncated, so the two now "
        "differ by exactly those rows (Validation reports them as extra). Recovery: reload "
        "this table on the Full Load step with 'Drop & reload', which recreates it and "
        "re-reads the source -- an upsert-only reload cannot remove rows the source no longer "
        "has. (dev.dsqlmigrator.connect.DsqlSinkTask:818)"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "v45arr.arr_dlq"
    # No SQLSTATE: nothing was sent to DSQL, so a synthetic class carries the signal.
    assert rec.error_code == SOURCE_TRUNCATE_CODE
    assert len(SOURCE_TRUNCATE_CODE) != 5  # can never be mistaken for a real SQLSTATE
    assert rec.drift_kind is SchemaDriftKind.SOURCE_TRUNCATE
    # A truncate carries no row and no key, so neither may be invented.
    assert rec.pk is None
    # The recovery must survive into the surfaced message (Drop & reload, not a plain Reload:
    # an upsert cannot remove a row the source no longer has).
    assert "Drop & reload" in rec.message


def test_a_mysql_source_truncate_quarantine_is_classified_and_held() -> None:
    """The MySQL-shaped truncate dead letter takes the same path as the PostgreSQL one.

    With ``skipped.operations: none`` on the MySQL source, debezium-connector-binlog 2.7.4
    (``BinlogStreamingChangeEventSource.handleQueryEvent`` ->
    ``BinlogChangeRecordEmitter.emitTruncateRecord``) emits one keyless op='t' record on the
    table's own ``<prefix>.<db>.<table>`` topic, with ``source.db``/``source.table`` set and
    no ``source.schema``. Fed to the shipped sink (verified offline against the shipped
    jars), it dead-letters as below -- no SQLSTATE, no pk, no op suffix. The parse, the drift
    kind and the Cut over ledger are engine-agnostic; this pins that they stay so.
    """
    from dsql_migrator.core.cdc_dlq import SOURCE_TRUNCATE_CODE, fold_dlq_ledger

    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.migration_schema.g3_trunc, partition=0, "
        "offset=7): Cannot apply a source TRUNCATE of table migration_schema.g3_trunc: Aurora "
        "DSQL has no TRUNCATE statement, so there is no correct apply and this event is "
        "quarantined rather than guessed at. The target still holds the rows the source "
        "truncated, so the two now differ by exactly those rows (Validation reports them as "
        "extra). Recovery: reload this table on the Full Load step with 'Drop & reload', which "
        "recreates it and re-reads the source -- an upsert-only reload cannot remove rows the "
        "source no longer has. (dev.dsqlmigrator.connect.DsqlSinkTask:818)"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.table == "migration_schema.g3_trunc"
    assert rec.error_code == SOURCE_TRUNCATE_CODE
    assert rec.drift_kind is SchemaDriftKind.SOURCE_TRUNCATE
    assert rec.pk is None
    # ...and it reaches the Cut over ledger under its own kind, which is what holds Cut over
    # for that table until a CHECKSUM match.
    ledger = fold_dlq_ledger(None, [rec], now=datetime.now(timezone.utc))
    entry = ledger["migration_schema.g3_trunc"]
    assert entry.count == 1
    assert entry.kinds == [SchemaDriftKind.SOURCE_TRUNCATE.value]


def test_a_source_truncate_code_never_overrides_a_real_sqlstate() -> None:
    """The synthetic code is only assigned when the driver reported no SQLSTATE.

    Guards the one way this classification could mislead: a server rejection that happened to
    mention the phrase must keep its real SQLSTATE (and therefore its real drift kind), because
    the drift banner and its recovery are chosen from that code.
    """
    from dsql_migrator.core.cdc import SchemaDriftKind
    from dsql_migrator.core.cdc_dlq import SOURCE_TRUNCATE_CODE

    msg = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, offset=4): "
        "sqlstate=42703 column \"notes\" of relation \"orders\" does not exist "
        "| sql: INSERT INTO shop.orders (id, notes) VALUES (?, ?) "
        "-- Cannot apply a source TRUNCATE"
    )
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.error_code == "42703"
    assert rec.error_code != SOURCE_TRUNCATE_CODE
    assert rec.drift_kind is SchemaDriftKind.ADD_COLUMN


def test_the_truncate_banner_is_not_announced_as_a_schema_change() -> None:
    """The banner must name what actually happened, and the right recovery.

    Three ways the existing banner would have lied about a truncate: the header ("Source schema
    change detected"), the shared runbook line ("Apply the matching change to the target schema
    (e.g. ALTER TABLE), then use per-table Reload to backfill the rows that were set aside"),
    and the direction -- a truncate leaves the target with EXTRA rows, so an upsert-only Reload
    cannot fix it and nothing was "set aside" to backfill.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.cdc import SchemaDriftKind
    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    kind = SchemaDriftKind.SOURCE_TRUNCATE.value
    # It is SOURCE-side, so it must not inherit the "rows may be missing" / stop-and-reload
    # treatment built for the target-side kinds.
    assert kind not in cm._TARGET_SIDE_DRIFT_KINDS
    assert kind not in cm._CERTAIN_LOSS_DRIFT_KINDS
    label, why = cm._DRIFT_LABELS[kind]
    assert "TRUNCATE" in label
    assert "no TRUNCATE statement" in why
    assert "EXTRA" in why

    class _El:
        def classes(self, *a, **k):
            return self

        def props(self, *a, **k):
            return self

        def tooltip(self, *a, **k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Ui:
        def __init__(self) -> None:
            self.texts: list[str] = []

        def column(self, *a, **k):
            return _El()

        def row(self, *a, **k):
            return _El()

        def label(self, text="", *a, **k):
            self.texts.append(str(text))
            return _El()

        def icon(self, name="", *a, **k):
            return _El()

        def button(self, text="", *a, **k):
            self.texts.append(str(text))
            return _El()

        def space(self, *a, **k):
            return _El()

        def card(self, *a, **k):
            return _El()

    view = SimpleNamespace(
        schema_drift=[SchemaDriftSummary(table="v45arr.arr_dlq", kind=kind, count=1)]
    )
    ui = _Ui()
    cm._render_cdc_schema_drift_banner(ui, view, session=None)
    blob = " ".join(ui.texts)

    assert "Source TRUNCATE was not replicated" in blob
    assert "Source schema change detected" not in blob
    assert "v45arr.arr_dlq" in blob
    # The truncate runbook, not the ALTER one.
    assert "Drop & reload" in blob
    assert "ALTER TABLE" not in blob
    assert "backfill" not in blob
    # No target-side escalation: nothing vanished from the target.
    assert "Stop CDC and reload these tables" not in blob
    # ADD COLUMN's ALTER button is useless here.
    assert "Fix target schema" not in blob


def test_mixed_drift_keeps_the_ddl_header_but_still_states_the_truncate() -> None:
    """A truncate alongside a real DDL change must not downgrade the DDL wording.

    The banner's existing rule is "mixed drift reports the most severe"; the truncate-specific
    header and runbook line therefore apply only when a truncate is the ONLY kind, while the
    per-table line still spells the truncate out in full.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    class _El:
        def classes(self, *a, **k):
            return self

        def props(self, *a, **k):
            return self

        def tooltip(self, *a, **k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Ui:
        def __init__(self) -> None:
            self.texts: list[str] = []

        def column(self, *a, **k):
            return _El()

        def row(self, *a, **k):
            return _El()

        def label(self, text="", *a, **k):
            self.texts.append(str(text))
            return _El()

        def icon(self, name="", *a, **k):
            return _El()

        def button(self, text="", *a, **k):
            self.texts.append(str(text))
            return _El()

        def space(self, *a, **k):
            return _El()

        def card(self, *a, **k):
            return _El()

    view = SimpleNamespace(
        schema_drift=[
            SchemaDriftSummary(table="shop.orders", kind="add-column", count=3),
            SchemaDriftSummary(table="v45arr.arr_dlq", kind="source-truncate", count=1),
        ]
    )
    ui = _Ui()
    cm._render_cdc_schema_drift_banner(ui, view, session=None)
    blob = " ".join(ui.texts)

    assert "Source schema change detected" in blob
    assert "Source TRUNCATE was not replicated" not in blob
    # ...and the truncate is still named on its own row, with its own explanation.
    assert "a source TRUNCATE was not replicated" in blob
    # The DDL runbook is back, because a real DDL change is present.
    assert "ALTER TABLE" in blob


def test_the_add_column_runbook_names_step_1_and_drop_and_reload() -> None:
    """The ADD COLUMN advice must be TRUE for what the code actually does.

    Measured: a plain per-table Reload repairs neither half of an ADD COLUMN drift. Its
    SELECT list is ``[c.name for c in table.columns]`` off the Step 1 inventory
    (``core/exporter.py``), which predates the source change, so the new column is not
    even read; and it appends with ``SKIP_EXISTING``, which never rewrites a primary key
    the target already has, so an existing row keeps its old values. So the runbook has
    to name re-running Step 1 AND 'Drop & reload' -- and must not promise that a plain
    Reload backfills anything.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    class _El:
        def classes(self, *a, **k):
            return self

        def props(self, *a, **k):
            return self

        def tooltip(self, *a, **k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class _Ui:
        def __init__(self) -> None:
            self.texts: list[str] = []

        def column(self, *a, **k):
            return _El()

        def row(self, *a, **k):
            return _El()

        def label(self, text="", *a, **k):
            self.texts.append(str(text))
            return _El()

        def icon(self, name="", *a, **k):
            return _El()

        def button(self, text="", *a, **k):
            self.texts.append(str(text))
            return _El()

        def space(self, *a, **k):
            return _El()

        def card(self, *a, **k):
            return _El()

    view = SimpleNamespace(
        schema_drift=[
            SchemaDriftSummary(table="shop.orders", kind="add-column", count=3)
        ]
    )
    ui = _Ui()
    cm._render_cdc_schema_drift_banner(ui, view, session=None)
    blob = " ".join(ui.texts)

    assert "re-run Step 1 (Evaluation)" in blob
    assert "'Drop & reload'" in blob
    assert "A plain append reload cannot backfill them" in blob
    # The old, false promise must be gone.
    assert "use per-table Reload to backfill" not in blob


def test_both_source_connectors_emit_truncates() -> None:
    """The config half of the pair, on both source engines.

    Debezium's ``skipped.operations`` defaults to ``t`` (verified in the shipped
    debezium-core-2.7.4.Final.jar: ``CommonConnectorConfig.SKIPPED_OPERATIONS`` is built
    ``.withDefault("t")``). On PostgreSQL that default is applied inside the pgoutput decoder;
    on MySQL, in debezium-connector-binlog's ``BinlogStreamingChangeEventSource.handleQueryEvent``
    -- either way the truncate never becomes a record and the target silently keeps the rows.
    The sink branch is useless without this key. Each block must carry exactly ONE
    ``skipped.operations`` key valued ``none`` (the only value that skips nothing). MySQL
    emission was verified offline against the shipped binlog 2.7.4 jar; the live MySQL check
    is still pending.
    """
    import pathlib
    import re

    template = (
        pathlib.Path(__file__).resolve().parents[1] / "deploy/cdc-stack/cdc-stack.yaml"
    ).read_text(encoding="utf-8")

    mysql_block = template[
        template.index("connector.class: io.debezium.connector.mysql.MySqlConnector") :
    ]
    pg_start = mysql_block.index(
        "connector.class: io.debezium.connector.postgresql.PostgresConnector"
    )
    pg_block = mysql_block[pg_start:]
    mysql_block = mysql_block[:pg_start]

    key = re.compile(r"^\s*skipped\.operations:\s*(\S+)\s*$", re.M)
    assert key.findall(pg_block) == ["none"]
    assert key.findall(mysql_block) == ["none"]
    # The comments must describe the key correctly, and the same way in both blocks: a value
    # skips exactly the op codes it lists (the default "t" only truncate), never "c/u/d".
    assert "skip c/u/d" not in mysql_block and "skip c/u/d" not in pg_block
    assert "skips exactly the op codes it lists" in mysql_block


class _BannerUi:
    """A NiceGUI stand-in that records every label/button text the drift banner renders."""

    class _El:
        def classes(self, *a, **k):
            return self

        def props(self, *a, **k):
            return self

        def tooltip(self, *a, **k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def __init__(self) -> None:
        self.texts: list[str] = []

    def column(self, *a, **k):
        return self._El()

    def row(self, *a, **k):
        return self._El()

    def label(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._El()

    def icon(self, name="", *a, **k):
        return self._El()

    def button(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._El()

    def space(self, *a, **k):
        return self._El()

    def card(self, *a, **k):
        return self._El()


@pytest.mark.parametrize("migration_type", [None, "FULL_LOAD_AND_CDC", "CDC_ONLY"])
def test_the_truncate_runbook_says_stop_cdc_and_fits_the_migration_type(migration_type) -> None:
    """The truncate runbook must be executable as written, for the run's migration type.

    Two ways the old line misled once a MySQL TRUNCATE (not just a PostgreSQL one) could reach
    it: it sent the operator to 'Drop & reload' without saying Stop CDC first -- that choice is
    only offered while CDC is not live (``_full_load_ui``), so the instruction could not be
    followed -- and on a CDC-only run it pointed at a Full Load step that does not exist.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    mt = getattr(MigrationType, migration_type) if migration_type else None
    view = SimpleNamespace(
        schema_drift=[
            SchemaDriftSummary(table="migration_schema.g3_trunc", kind="source-truncate", count=1)
        ]
    )
    ui = _BannerUi()
    cm._render_cdc_schema_drift_banner(ui, view, session=None, migration_type=mt)
    blob = " ".join(ui.texts)

    assert "Source TRUNCATE was not replicated" in blob
    assert "migration_schema.g3_trunc" in blob
    for absent in ("ALTER TABLE", "backfill", "Stop CDC and reload these tables"):
        assert absent not in blob
    assert "CHECKSUM" in blob
    (runbook,) = [t for t in ui.texts if t.startswith("CDC cannot replicate a TRUNCATE")]
    if mt is MigrationType.CDC_ONLY:
        assert "no Full Load step" in runbook
        assert "3,000 rows per transaction" in runbook
        assert runbook.index("Stop CDC") < runbook.index("delete the rows") < runbook.index(
            "Start CDC"
        )
        assert "'Drop & reload'" not in blob
        assert "on the Full Load step" not in blob
    else:
        assert "offered only while CDC is stopped" in runbook
        assert "'Append' only adds missing rows" in runbook
        assert runbook.index("Stop CDC") < runbook.index("'Drop & reload'") < runbook.index(
            "Start CDC"
        )


# ---------------------------------------------------------------------------
# fold_dlq_ledger: the per-table dead-letter memory the cut-over gate reads
# ---------------------------------------------------------------------------


def test_the_ledger_counts_per_table_and_keeps_the_latest_time():
    from datetime import datetime, timezone

    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.core.cdc_dlq import fold_dlq_ledger

    early = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
    late = datetime(2026, 9, 1, 11, 0, tzinfo=timezone.utc)
    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    errors = [
        CdcConnectorError(table="app.t", message="m1", occurred_at=late),
        CdcConnectorError(table="app.t", message="m2", occurred_at=early),
        CdcConnectorError(table="app.u", message="m3"),  # no time: stamped with now
    ]
    ledger = fold_dlq_ledger({}, errors, now=now)
    assert ledger["app.t"].count == 2 and ledger["app.t"].last_at == late
    assert ledger["app.u"].count == 1 and ledger["app.u"].last_at == now


def test_the_ledger_records_drift_kinds_sorted_unique_and_oversized_values():
    from datetime import datetime, timezone

    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.core.cdc_dlq import fold_dlq_ledger

    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    codes = ["42703", "23505", "42703", "SOURCE_TRUNCATE", "OVERSIZED_VALUE", "22001"]
    ledger = fold_dlq_ledger(
        {},
        [CdcConnectorError(table="app.t", message=f"m{i}", error_code=c) for i, c in enumerate(codes)],
        now=now,
    )
    # G9: a 22001 (too long for the Aurora DSQL column) is the value-fit kind now.
    assert ledger["app.t"].kinds == [
        "add-column", "oversized-value", "source-truncate", "unique-conflict",
        "value-does-not-fit",
    ]
    plain = fold_dlq_ledger(
        {}, [CdcConnectorError(table="app.v", message="x", error_code="22P02")], now=now
    )
    # A malformed value has no schema meaning: no kind, but it is still counted (Cut over
    # holds every dead-lettered table, classified or not).
    assert plain["app.v"].kinds == [] and plain["app.v"].count == 1


def test_the_ledger_fold_is_pure():
    from datetime import datetime, timezone

    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.core.cdc_dlq import fold_dlq_ledger

    now = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
    before = fold_dlq_ledger({}, [CdcConnectorError(table="app.t", message="a")], now=now)
    snapshot = dict(before)
    after = fold_dlq_ledger(before, [CdcConnectorError(table="app.t", message="b")], now=now)
    assert before == snapshot and before["app.t"].count == 1
    assert after is not before and after["app.t"].count == 2


# ---------------------------------------------------------------------------
# G8: the sink's value-fit refusal (a value its Aurora DSQL column would change)
# ---------------------------------------------------------------------------

_VALUE_FIT_LINE = (
    "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, offset=7): Value does "
    "not fit target column 'amount' of type numeric(10,2): a number with more than 2 decimal "
    "places would be rounded. Not applied and never retried: the Aurora DSQL column needs a "
    "type that holds the source values. | pk: id=7 | op: u "
    "(dev.dsqlmigrator.connect.DsqlSinkTask:301)"
)


class _BannerEl:
    def classes(self, *a, **k):
        return self

    def props(self, *a, **k):
        return self

    def tooltip(self, *a, **k):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _BannerUi:
    """Records every label/button text and every container's classes."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.classes_seen: list[str] = []

    def _el(self):
        ui = self

        class _Tracked(_BannerEl):
            def classes(self, *a, **k):
                ui.classes_seen.extend(str(x) for x in a)
                return self

        return _Tracked()

    def column(self, *a, **k):
        return self._el()

    def row(self, *a, **k):
        return self._el()

    def label(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()

    def icon(self, name="", *a, **k):
        return self._el()

    def button(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()

    def space(self, *a, **k):
        return self._el()

    def card(self, *a, **k):
        return self._el()

    def markdown(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()

    def html(self, text="", *a, **k):
        self.texts.append(str(text))
        return self._el()


def _render_drift(
    groups, migration_type=None, session=None, *, migration_state=None, ui=None
) -> "tuple[str, list[str]]":
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    view = SimpleNamespace(schema_drift=[SchemaDriftSummary(**g) for g in groups])
    ui = ui or _BannerUi()
    cm._render_cdc_schema_drift_banner(
        ui, view, session=session, migration_type=migration_type,
        migration_state=migration_state,
    )
    return " ".join(ui.texts), ui.classes_seen


def test_a_value_fit_refusal_is_classified_as_its_own_drift_kind() -> None:
    """Aurora DSQL would have ACCEPTED these values and stored them changed (true for a
    TINYINT(1) holding 2, a rounded decimal), so the sink refuses them before writing -- with
    no SQLSTATE. The synthetic code is what turns that into a banner instead of one more
    anonymous poison row."""
    from dsql_migrator.core.cdc import classify_schema_drift
    from dsql_migrator.core.cdc_dlq import (
        VALUE_DOES_NOT_FIT_CODE,
        oversized_column_from_reason,
        value_fit_column_from_reason,
    )

    rec = parse_dlq_log_message(_VALUE_FIT_LINE)
    assert rec is not None
    assert rec.table == "shop.orders"
    assert rec.error_code == VALUE_DOES_NOT_FIT_CODE == "VALUE_DOES_NOT_FIT"
    assert len(VALUE_DOES_NOT_FIT_CODE) != 5  # can never be mistaken for a real SQLSTATE
    assert classify_schema_drift(rec.error_code) is SchemaDriftKind.VALUE_DOES_NOT_FIT
    assert rec.drift_kind is SchemaDriftKind.VALUE_DOES_NOT_FIT
    assert rec.pk == "id=7"
    assert rec.op == "u"
    assert value_fit_column_from_reason(rec.message) == "amount numeric(10,2)"
    # The size guard's column extractor must not claim this record.
    assert oversized_column_from_reason(rec.message) is None
    assert value_fit_column_from_reason("Value for column 'x' exceeds DSQL's 1-byte limit") is None
    # The default-numeric type name (it has parentheses but no colon) is read whole.
    default = _VALUE_FIT_LINE.replace(
        "numeric(10,2)", "numeric (Aurora DSQL default numeric(18,6))"
    )
    assert value_fit_column_from_reason(default) == (
        "amount numeric (Aurora DSQL default numeric(18,6))"
    )


def test_a_value_fit_phrase_never_overrides_a_real_sqlstate() -> None:
    from dsql_migrator.core.cdc_dlq import VALUE_DOES_NOT_FIT_CODE

    msg = _VALUE_FIT_LINE.replace("offset=7): ", "offset=7): sqlstate=42804 ")
    rec = parse_dlq_log_message(msg)
    assert rec is not None
    assert rec.error_code == "42804" != VALUE_DOES_NOT_FIT_CODE
    assert rec.drift_kind is SchemaDriftKind.TYPE_CHANGE


def test_the_sink_runs_the_value_fit_guard_before_batching() -> None:
    """Cross-language anchor: the guard must sit in put() BEFORE batch.add -- a pre-write
    refusal, so a refused row costs no chunk rollback and no per-row replay -- and the Python
    parser must match the sink's exact leading phrase."""
    import pathlib

    from dsql_migrator.core.cdc_dlq import _VALUE_DOES_NOT_FIT

    base = pathlib.Path(__file__).resolve().parents[1] / "connectors/dsql-sink/src/main/java"
    sink = (base / "dev/dsqlmigrator/connect/DsqlSinkTask.java").read_text(encoding="utf-8")
    guard = sink[sink.index("String oversized = oversizedColumn(event);") :]
    guard = guard[: guard.index("batch.add(")]
    assert "valueFitRefusal(" in guard, guard[:800]
    assert "reportOrThrow(record, event, new DataException(unfit))" in guard
    value_fit = (base / "dev/dsqlmigrator/connect/ValueFit.java").read_text(encoding="utf-8")
    literal = "Value does not fit target column '"
    assert f'REASON_PREFIX = "{literal}"' in value_fit
    assert _VALUE_DOES_NOT_FIT.search(literal)


def test_every_drift_kind_has_a_label() -> None:
    """A kind missing from _DRIFT_LABELS renders as its raw string on the banner and on the
    cut-over line; nothing caught that before."""
    from dsql_migrator.ui import cdc_dlq_gate
    from dsql_migrator.ui.data_migration._cdc_monitoring import _DRIFT_LABELS

    for kind in SchemaDriftKind:
        assert kind.value in _DRIFT_LABELS, kind
        label, why = _DRIFT_LABELS[kind.value]
        assert label.strip() and why.strip(), kind
        assert cdc_dlq_gate._kind_label(kind.value) != kind.value, kind


def test_the_ledger_keeps_every_kind() -> None:
    """The persisted ledger (which holds Cut over) must not drop a kind: the old fixed cap of
    8 had already fallen behind the enum, and the ninth kind would have been cut. Every kind is
    reachable: from a code in the map, or -- NULLABILITY_CHANGE, record evidence only -- from a
    23502 whose SQL template carries the column it names."""
    from dsql_migrator.core.cdc import CdcConnectorError, _DRIFT_BY_SQLSTATE
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_CODE, fold_dlq_ledger

    now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    codes = sorted({code for code, kind in _DRIFT_BY_SQLSTATE.items()})
    errors = [
        CdcConnectorError(table="app.t", message=f"m{i}", error_code=c)
        for i, c in enumerate([*codes, OVERSIZED_VALUE_CODE])
    ]
    errors.append(
        CdcConnectorError(table="app.t", message=_NULL_SENT_REASON, error_code="23502")
    )
    kinds = fold_dlq_ledger({}, errors, now=now)["app.t"].kinds
    assert len(kinds) == len(SchemaDriftKind) + 1
    assert SchemaDriftKind.VALUE_DOES_NOT_FIT.value in kinds
    assert SchemaDriftKind.NULLABILITY_CHANGE.value in kinds


# A sink-shaped NULL-sent 23502 reason (synthetic: the DSQL wording is not documented).
_NULL_SENT_REASON = (
    'DLQ offset=5: sqlstate=23502 null value in column "note" of relation "orders" violates '
    'not-null constraint | pk: id=5 | op: u | sql: INSERT INTO "shop"."orders" ("id", "note") '
    'VALUES (?, ?) ON CONFLICT ("id") DO UPDATE SET "note" = EXCLUDED."note"'
)
_NOT_SENT_REASON = _NULL_SENT_REASON.replace('("id", "note")', '("id", "total")')


def test_the_ledger_folds_the_record_level_kind() -> None:
    """G9: the ledger (persisted, read by the cut-over gate) records the SAME kind the banner
    shows: 42P10 / 23514 / 22003 used to add no kind at all, and a NULL-sent 23502 was
    recorded as a dropped column."""
    from dsql_migrator.core.cdc import CdcConnectorError
    from dsql_migrator.core.cdc_dlq import VALUE_DOES_NOT_FIT_CODE, fold_dlq_ledger

    now = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)
    errors = [
        CdcConnectorError(table="app.t", message="m1", error_code="42P10"),
        CdcConnectorError(table="app.t", message="m2", error_code="23514"),
        CdcConnectorError(table="app.t", message="m3", error_code="22003"),
        CdcConnectorError(table="app.t", message=_NULL_SENT_REASON, error_code="23502"),
    ]
    entry = fold_dlq_ledger({}, errors, now=now)["app.t"]
    assert entry.kinds == [
        "check-violation", "key-change", "nullability-change", "value-does-not-fit",
    ]
    assert entry.count == 4
    # An unclassified data error raises the count (Cut over holds it) and adds no kind.
    more = fold_dlq_ledger(
        {"app.t": entry},
        [CdcConnectorError(table="app.t", message="m5", error_code="22P02")],
        now=now,
    )["app.t"]
    assert more.count == 5 and more.kinds == entry.kinds
    # The sink's refusal and Aurora DSQL's own 22003 are one kind, never double-counted.
    both = fold_dlq_ledger(
        {},
        [
            CdcConnectorError(table="app.u", message="a", error_code=VALUE_DOES_NOT_FIT_CODE),
            CdcConnectorError(table="app.u", message="b", error_code="22003"),
        ],
        now=now,
    )["app.u"]
    assert both.kinds == ["value-does-not-fit"] and both.count == 2
    # A proven-not-sent 23502 stays a dropped column.
    dropped = fold_dlq_ledger(
        {}, [CdcConnectorError(table="app.v", message=_NOT_SENT_REASON, error_code="23502")],
        now=now,
    )["app.v"]
    assert dropped.kinds == ["drop-column"]


def test_a_parsed_null_sent_23502_record_is_a_nullability_change() -> None:
    """End to end from the sink's log line: the parser keeps the template in the message, so
    the record-level kind is available to every consumer (banner, ledger, drift_kind)."""
    line = (
        "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, offset=5): "
        + _NULL_SENT_REASON.split(": ", 1)[1]
        + " (dev.dsqlmigrator.connect.DsqlSinkTask:900)"
    )
    rec = parse_dlq_log_message(line)
    assert rec is not None and rec.error_code == "23502"
    assert rec.drift_kind is SchemaDriftKind.NULLABILITY_CHANGE
    not_sent = parse_dlq_log_message(line.replace('("id", "note")', '("id", "total")'))
    assert not_sent.drift_kind is SchemaDriftKind.DROP_COLUMN


def test_check_constraint_from_reason_reads_the_name_never_a_value() -> None:
    from dsql_migrator.core.cdc_dlq import check_constraint_from_reason

    reason = (
        'sqlstate=23514 new row for relation "orders" violates check constraint '
        '"orders_status_check" [constraint: orders_status_check] | pk: id=3 | op: c'
    )
    assert check_constraint_from_reason(reason) == "orders_status_check"
    # The server's quoted name is the fallback when the suffix is missing.
    assert check_constraint_from_reason(
        'new row for relation "orders" violates check constraint "chk_qty"'
    ) == "chk_qty"
    assert check_constraint_from_reason("boom") is None
    assert check_constraint_from_reason(None) is None


def test_value_fit_banner_is_a_warning_with_its_own_header_columns_and_recreate_runbook() -> None:
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType
    from dsql_migrator.ui.design import NOTICE_STYLE

    kind = SchemaDriftKind.VALUE_DOES_NOT_FIT.value
    # Not a target-side loss and not a certain loss: amber, no "Stop CDC and reload" escalation.
    assert kind not in cm._TARGET_SIDE_DRIFT_KINDS
    assert kind not in cm._CERTAIN_LOSS_DRIFT_KINDS
    # And not a schema-change claim, so it never hides a source schema re-check finding.
    assert kind not in cm._DLQ_SHARED_SCHEMA_KINDS

    group = {"table": "shop.orders", "kind": kind, "count": 3,
             "columns": ["amount numeric(10,2)"]}
    blob, classes = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC)
    assert "Rows set aside: a value does not fit its Aurora DSQL column" in blob
    bg, border, _icon_color, _icon = NOTICE_STYLE["warning"]
    assert any(bg in c and border in c for c in classes), classes
    # A TINYINT(1) holding 2 involved no DDL: the banner must not claim one.
    assert "Source schema change detected" not in blob
    assert "amount numeric(10,2)" in blob
    for needle in ("cannot change a column's type in place", "Replace", "'Drop & reload'",
                   "smallint", "CHECKSUM"):
        assert needle in blob, needle
    # Aurora DSQL has no ALTER COLUMN TYPE: the generic ALTER advice would be impossible here.
    assert "e.g. ALTER TABLE" not in blob
    assert "Fix target schema" not in blob
    assert "Stop CDC and reload these tables" not in blob

    # CDC only has no Full Load step to reload from.
    blob, _ = _render_drift([group], MigrationType.CDC_ONLY)
    assert "Start over" in blob and "3,000" in blob
    assert "'Drop & reload'" not in blob

    # A PostgreSQL source has no TINYINT(1): its banner never talks about MySQL.
    from types import SimpleNamespace

    from dsql_migrator.core.models import SourceType

    pg = SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES))
    blob, _ = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC, session=pg)
    assert "MySQL" not in blob and "TINYINT" not in blob
    assert "changed to an integer type" in blob
    assert "cannot change a column's type in place" in blob

    # Mixed with a real DDL change: the DDL header and runbook stay, and the value-fit
    # sentence is added.
    blob, _ = _render_drift(
        [{"table": "shop.orders", "kind": "add-column", "count": 2}, group],
        MigrationType.FULL_LOAD_AND_CDC,
    )
    assert "Source schema change detected" in blob
    assert "e.g. ALTER TABLE" in blob
    assert "cannot change a column's type in place" in blob
    assert "a value its Aurora DSQL column cannot hold exactly" in blob


@pytest.mark.parametrize("mtype_name", ["FULL_LOAD_AND_CDC", "CDC_ONLY"])
def test_a_truncate_and_a_value_fit_refusal_together_claim_no_schema_change(mtype_name) -> None:
    """Neither kind is a source DDL change, so together (and with nothing else) they must not
    fall through to the DDL header and its ALTER TABLE runbook -- which also dropped the
    TRUNCATE repair, and on CDC only sent the user to a Drop & reload that does not exist."""
    from dsql_migrator.ui.data_migration._models import MigrationType
    from dsql_migrator.ui.design import NOTICE_STYLE

    mtype = MigrationType[mtype_name]
    groups = [
        {"table": "shop.events", "kind": "source-truncate", "count": 1},
        {"table": "shop.orders", "kind": "value-does-not-fit", "count": 2,
         "columns": ["flag boolean"]},
    ]
    blob, classes = _render_drift(groups, mtype)
    assert "Source schema change detected" not in blob
    assert "e.g. ALTER TABLE" not in blob
    assert (
        "Source TRUNCATE was not replicated, and a value does not fit its Aurora DSQL column"
        in blob
    )
    bg, border, _icon_color, _icon = NOTICE_STYLE["warning"]
    assert any(bg in c and border in c for c in classes), classes
    # Both repairs, each for its own table.
    assert "Aurora DSQL has no TRUNCATE statement" in blob
    assert "cannot change a column's type in place" in blob
    assert "flag boolean" in blob
    if mtype is MigrationType.CDC_ONLY:
        assert "delete the rows the source no longer has" in blob
        assert "'Drop & reload'" not in blob
    else:
        assert "cannot remove the stale ones" in blob


def test_a_truncate_beside_dsqls_own_value_rejection_gives_no_tinyint_advice() -> None:
    """The TINYINT(1) -> smallint advice belongs to the sink's own refusal (a boolean target
    holding 2); Aurora DSQL's 22003 beside a TRUNCATE never needs it."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    truncate = {"table": "shop.events", "kind": "source-truncate", "count": 1}
    server = {"table": "shop.orders", "kind": "value-does-not-fit", "count": 2,
              "codes": ["22003"]}
    blob, _ = _render_drift([truncate, server], MigrationType.FULL_LOAD_AND_CDC)
    assert "Aurora DSQL has no TRUNCATE statement" in blob
    assert "smallint" not in blob
    sink = dict(server, codes=["VALUE_DOES_NOT_FIT"])
    blob, _ = _render_drift([truncate, sink], MigrationType.FULL_LOAD_AND_CDC)
    assert "smallint" in blob


def test_a_truncate_mixed_with_a_ddl_change_keeps_the_ddl_banner() -> None:
    # Unchanged: a real DDL kind beside the TRUNCATE keeps the (more severe) DDL header and the
    # generic runbook (plus the TRUNCATE repair, see below), and a TRUNCATE alone keeps its own
    # header.
    from dsql_migrator.ui.data_migration._models import MigrationType

    truncate = {"table": "shop.events", "kind": "source-truncate", "count": 1}
    blob, _ = _render_drift(
        [truncate, {"table": "shop.orders", "kind": "add-column", "count": 2}],
        MigrationType.FULL_LOAD_AND_CDC,
    )
    assert "Source schema change detected" in blob
    assert "e.g. ALTER TABLE" in blob
    blob, _ = _render_drift([truncate], MigrationType.FULL_LOAD_AND_CDC)
    assert "Source TRUNCATE was not replicated" in blob
    assert "and a value does not fit" not in blob
    assert "e.g. ALTER TABLE" not in blob


@pytest.mark.parametrize("mtype_name", ["FULL_LOAD_AND_CDC", "CDC_ONLY"])
@pytest.mark.parametrize(
    "kind", ["key-change", "type-change", "add-column", "check-violation", "unique-conflict"]
)
def test_a_truncate_beside_any_other_kind_keeps_its_own_repair(kind, mtype_name) -> None:
    """The runbook of a banner mixing a TRUNCATE with a DDL or target-side kind gave the
    truncated table no repair at all (a type or key change's bare lead; on CDC only no delete
    step, and a TRUNCATE dead letter names no row to correct by hand), while its own row says
    the target keeps the stale rows "until you act"."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    mtype = MigrationType[mtype_name]
    ui = _BannerUi()
    _render_drift(
        [{"table": "shop.a", "kind": kind, "count": 2},
         {"table": "shop.b", "kind": "source-truncate", "count": 1}],
        mtype, ui=ui,
    )
    (runbook,) = [
        t for t in ui.texts
        if t.startswith(("CDC does not replicate DDL.", "CDC cannot replicate a TRUNCATE"))
    ]
    # A target-side kind is no source DDL, so only a real DDL kind gets the DDL lead.
    assert runbook.startswith("CDC does not replicate DDL.") is (kind != "unique-conflict")
    if mtype is MigrationType.CDC_ONLY:
        assert "delete the rows the source no longer has from shop.b on Aurora DSQL" in runbook
        assert "'Drop & reload'" not in runbook
    else:
        assert "reload shop.b on the Full Load step with 'Drop & reload'" in runbook
        assert "cannot remove the stale ones" in runbook
    # A TRUNCATE alone keeps its "the table" wording byte for byte.
    alone = _BannerUi()
    _render_drift([{"table": "shop.b", "kind": "source-truncate", "count": 1}], mtype, ui=alone)
    expected = (
        "delete the rows the source no longer has from the table on Aurora DSQL"
        if mtype is MigrationType.CDC_ONLY
        else "Stop CDC, reload the table on the Full Load step with 'Drop & reload'"
    )
    assert any(expected in t for t in alone.texts)


@pytest.mark.parametrize("kind", ["unique-conflict", "missing-table", "unbindable-value"])
@pytest.mark.parametrize("mtype_name", ["FULL_LOAD_AND_CDC", "CDC_ONLY"])
def test_a_target_side_only_banner_gets_no_ddl_runbook(kind, mtype_name) -> None:
    """Live PG E2E (2026-10-09): a primary-key change racing a secondary UNIQUE dead-lettered
    23505 and the banner told the operator "CDC does not replicate DDL. Apply the matching
    change to the target schema (e.g. ALTER TABLE)" -- there was no schema change. A
    target-side kind's repair is the stop-and-reload notice, so it gets no DDL lead."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    blob, _ = _render_drift(
        [{"table": "shop.customers", "kind": kind, "count": 3}], MigrationType[mtype_name]
    )
    assert "does not replicate DDL" not in blob
    assert "e.g. ALTER TABLE" not in blob
    assert "Stop CDC" in blob


def test_a_target_side_kind_beside_a_ddl_kind_keeps_the_ddl_lead() -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    blob, _ = _render_drift(
        [{"table": "shop.a", "kind": "add-column", "count": 1},
         {"table": "shop.b", "kind": "unique-conflict", "count": 1}],
        MigrationType.FULL_LOAD_AND_CDC,
    )
    assert "CDC does not replicate DDL. Apply the matching change" in blob


class _AiBannerUi(_BannerUi):
    """A _BannerUi that also keeps each button's on_click and the tooltips set on it."""

    def __init__(self) -> None:
        super().__init__()
        self.clicks: dict[str, object] = {}
        self.tooltips: list[str] = []

    def button(self, text="", *a, on_click=None, **k):
        self.texts.append(str(text))
        self.clicks[str(text)] = on_click
        ui = self

        class _Button(_BannerEl):
            def tooltip(self, tip="", *a, **k):
                ui.tooltips.append(str(tip))
                return self

        return _Button()


def _ask_ai_dba(groups, migration_type=None) -> "tuple[str, str, str, list[str]]":
    """Render the drift banner with a capturing AI opener, click Ask AI DBA, and return
    (scope, facts, seed, tooltips)."""
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    view = SimpleNamespace(schema_drift=[SchemaDriftSummary(**g) for g in groups])
    ui = _AiBannerUi()
    calls: list[tuple[str, str, str]] = []
    cm._render_cdc_schema_drift_banner(
        ui, view, migration_type=migration_type,
        cdc_ai_opener=lambda scope, facts, seed: calls.append((scope, facts, seed)),
    )
    ui.clicks["Ask AI DBA"]()
    scope, facts, seed = calls[-1]
    return scope, facts, seed, ui.tooltips


def test_the_value_fit_banner_asks_the_ai_dba_without_claiming_a_schema_change() -> None:
    """The seed becomes the operator's first chat message and the facts are 'authoritative' to
    the model, so a value-fit-only banner (a TINYINT(1) holding 2: no DDL at all) must not ask
    'what DDL to apply' for 'the source schema changed'."""
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.orders", "kind": "value-does-not-fit", "count": 2,
             "columns": ["is_gift boolean"]}
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        scope, facts, seed, tooltips = _ask_ai_dba([group], mtype)
        assert scope == "drift"
        assert seed == cm._CDC_VALUE_FIT_SEED
        assert "source schema changed" not in seed and "what DDL to apply" not in seed
        assert "ALTER COLUMN ... TYPE" in seed
        assert not facts.startswith("Source schema drift detected"), facts
        assert "value-does-not-fit (columns: is_gift boolean)" in facts
        assert tooltips and not any("what DDL" in t for t in tooltips), tooltips

    # A real DDL change (alone or beside a value-fit group) keeps the DDL seed and facts.
    add = {"table": "shop.orders", "kind": "add-column", "count": 1}
    for groups in ([add], [add, group]):
        _scope, facts, seed, tooltips = _ask_ai_dba(groups, MigrationType.FULL_LOAD_AND_CDC)
        assert seed == cm._CDC_DRIFT_SEED
        assert facts.startswith("Source schema drift detected")
        assert any("what DDL to apply" in t for t in tooltips), tooltips


def test_a_target_side_banner_asks_the_ai_dba_without_claiming_a_schema_change() -> None:
    """Live PG E2E (2026-10-09): a 23505 from a primary-key change racing a secondary UNIQUE
    is no source DDL, so neither the seed, the facts nor the tooltip may say so."""
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.customers", "kind": "unique-conflict", "count": 3}
    truncate = {"table": "shop.b", "kind": "source-truncate", "count": 1}
    for groups in ([group], [group, truncate]):
        _scope, facts, seed, tooltips = _ask_ai_dba(groups, MigrationType.FULL_LOAD_AND_CDC)
        assert seed == cm._CDC_TARGET_SIDE_SEED
        assert "what DDL to apply" not in seed
        assert not facts.startswith("Source schema drift detected"), facts
        assert tooltips and not any("what DDL" in t for t in tooltips), tooltips
    add = {"table": "shop.orders", "kind": "add-column", "count": 1}
    _scope, facts, seed, _tooltips = _ask_ai_dba([add, group], MigrationType.FULL_LOAD_AND_CDC)
    assert seed == cm._CDC_DRIFT_SEED
    assert facts.startswith("Source schema drift detected")


def test_the_ai_drift_facts_name_a_constraint_as_a_constraint_and_list_the_codes() -> None:
    """The facts are authoritative to the AI DBA: a 23514 group names its CHECK constraint
    (never "columns"), and every group carries its SQLSTATEs."""
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    facts = cm._cdc_drift_facts(SimpleNamespace(schema_drift=[
        SchemaDriftSummary(table="shop.orders", kind="check-violation", count=2,
                           columns=["orders_status_check"], codes=["23514"]),
        SchemaDriftSummary(table="shop.items", kind="nullability-change", count=1,
                           columns=["note"], codes=["23502"]),
    ]))
    assert "(constraint: orders_status_check)" in facts and "columns: orders" not in facts
    assert "[codes: 23514]" in facts
    assert "nullability-change (columns: note) [codes: 23502]" in facts


def test_the_ai_drift_facts_name_a_unique_conflicts_index_as_a_constraint() -> None:
    """A 23505 group carries the unique index Aurora DSQL named: never labelled a column."""
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    facts = cm._cdc_drift_facts(SimpleNamespace(schema_drift=[
        SchemaDriftSummary(table="shop.customers", kind="unique-conflict", count=1,
                           columns=["uk_email"], codes=["23505"]),
    ]))
    assert "unique-conflict (constraint: uk_email) [codes: 23505]" in facts
    assert "columns:" not in facts


def test_cdc_schema_drift_summary_lists_value_fit_columns(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _cdc_status

    flag_line = _VALUE_FIT_LINE.replace("'amount' of type numeric(10,2)", "'flag' of type boolean")
    records = [
        parse_dlq_log_message(_VALUE_FIT_LINE),
        parse_dlq_log_message(_VALUE_FIT_LINE.replace("offset=7", "offset=8")),
        parse_dlq_log_message(flag_line),
        parse_dlq_log_message(
            "Quarantined record to DLQ (topic=dsqlcdc.shop.orders, partition=0, offset=9): "
            "sqlstate=42703 column \"notes\" of relation \"orders\" does not exist"
        ),
    ]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda _state, _key: records)
    groups = {g.kind: g for g in _cdc_status.cdc_schema_drift_summary(object(), "k")}
    fit = groups["value-does-not-fit"]
    assert fit.count == 3
    assert fit.columns == ["amount numeric(10,2)", "flag boolean"]
    assert fit.codes == ["VALUE_DOES_NOT_FIT"]
    assert groups["add-column"].columns == []
    assert groups["add-column"].codes == ["42703"]


def _dlq_line(offset: int, reason: str, table: str = "orders") -> str:
    return (
        f"Quarantined record to DLQ (topic=dsqlcdc.shop.{table}, partition=0, "
        f"offset={offset}): {reason}"
    )


def test_cdc_schema_drift_summary_groups_and_names_by_record_evidence(monkeypatch) -> None:
    """G9: a sink refusal and a DSQL 22003 on one table are ONE value-fit group (one banner
    row, one runbook); a 23514 names its constraint; a 23502 names its column only when the
    record shape proves which case it is; every list is capped."""
    from dsql_migrator.ui.data_migration import _cdc_status

    null_sent = _NULL_SENT_REASON.split(": ", 1)[1]
    not_sent = _NOT_SENT_REASON.split(": ", 1)[1]
    records = [
        parse_dlq_log_message(_VALUE_FIT_LINE),
        parse_dlq_log_message(_dlq_line(20, "sqlstate=22003 integer out of range | pk: id=20")),
        parse_dlq_log_message(_dlq_line(
            21, 'sqlstate=23514 new row for relation "orders" violates check constraint '
            '"orders_status_check" [constraint: orders_status_check] | pk: id=21 | op: c',
            table="items",
        )),
        parse_dlq_log_message(_dlq_line(
            22, 'sqlstate=23514 new row for relation "orders" violates check constraint '
            '"chk_qty" | pk: id=22 | op: c', table="items",
        )),
        parse_dlq_log_message(_dlq_line(23, null_sent, table="notes")),
        parse_dlq_log_message(_dlq_line(24, not_sent, table="docs")),
        parse_dlq_log_message(_dlq_line(25, "sqlstate=23502 boom", table="bare")),
        parse_dlq_log_message(_dlq_line(26, "sqlstate=22P02 invalid input syntax", table="bad")),
        # The column is named but the SQL template is cut before ") VALUES (" (a wide table
        # past the reason's bound): nothing proves it was not sent -- it may have been a NULL.
        parse_dlq_log_message(_dlq_line(
            27, null_sent[: null_sent.index(") VALUES (")], table="wide",
        )),
    ]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda _state, _key: records)
    groups = {
        (g.table, g.kind): g for g in _cdc_status.cdc_schema_drift_summary(object(), "k")
    }
    fit = groups[("shop.orders", "value-does-not-fit")]
    assert fit.count == 2
    assert fit.codes == ["22003", "VALUE_DOES_NOT_FIT"]
    assert fit.columns == ["amount numeric(10,2)"]  # DSQL's 22003 names no column
    check = groups[("shop.items", "check-violation")]
    assert check.columns == ["orders_status_check", "chk_qty"] and check.codes == ["23514"]
    assert groups[("shop.notes", "nullability-change")].columns == ["note"]
    assert groups[("shop.docs", "drop-column")].columns == ["note"]  # proven not sent
    assert groups[("shop.bare", "drop-column")].columns == []  # no evidence: no guess
    assert groups[("shop.wide", "drop-column")].columns == []  # named, not proven absent
    assert not any(table == "shop.bad" for table, _kind in groups)  # 22P02: no kind
    # One group per (table, kind): no second value-fit or type-change group anywhere.
    assert not any(kind == "type-change" for _table, kind in groups)

    capped = [
        parse_dlq_log_message(_dlq_line(
            30 + i, f'sqlstate=23514 x [constraint: c{i}] | pk: id={i}', table="many",
        ))
        for i in range(9)
    ]
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda _state, _key: capped)
    (group,) = _cdc_status.cdc_schema_drift_summary(object(), "k")
    assert group.count == 9 and group.columns == [f"c{i}" for i in range(6)]


def test_cdc_schema_drift_summary_parses_each_record_once(monkeypatch) -> None:
    """The summary is re-run on every CDC poll over an uncapped, append-only log: it must
    parse only the new tail, and rebuild when the log is not the one it saw."""
    from types import SimpleNamespace

    import dsql_migrator.core.cdc as cdc
    from dsql_migrator.ui.data_migration import _cdc_status

    calls = {"classify": 0, "evidence": 0}
    real_classify, real_evidence = cdc.classify_schema_drift, cdc.not_null_evidence

    def _classify(code, message=None):
        calls["classify"] += 1
        return real_classify(code, message)

    def _evidence(message):
        calls["evidence"] += 1
        return real_evidence(message)

    monkeypatch.setattr(cdc, "classify_schema_drift", _classify)
    monkeypatch.setattr(cdc, "not_null_evidence", _evidence)
    null_sent = _NULL_SENT_REASON.split(": ", 1)[1]
    log = [parse_dlq_log_message(_dlq_line(i, null_sent)) for i in range(3)]
    view = {"records": list(log)}
    monkeypatch.setattr(_cdc_status, "cdc_dlq_records", lambda _state, _key: view["records"])
    state = SimpleNamespace()

    first = _cdc_status.cdc_schema_drift_summary(state, "k")
    assert [(g.kind, g.count) for g in first] == [("nullability-change", 3)]
    assert calls["classify"] == 3
    calls.update(classify=0, evidence=0)
    assert _cdc_status.cdc_schema_drift_summary(state, "k") == first
    assert calls == {"classify": 0, "evidence": 0}  # unchanged log: nothing parsed

    log.append(parse_dlq_log_message(_dlq_line(9, "sqlstate=42P10 no unique constraint")))
    view["records"] = list(log)  # cdc_dlq_records hands out a new list, same objects
    grown = _cdc_status.cdc_schema_drift_summary(state, "k")
    assert calls["classify"] == 1  # only the new tail
    assert [(g.kind, g.count) for g in grown] == [("key-change", 1), ("nullability-change", 3)]
    # The earlier view was not mutated by the incremental fold.
    assert [(g.kind, g.count) for g in first] == [("nullability-change", 3)]

    calls.update(classify=0, evidence=0)
    view["records"] = [parse_dlq_log_message(_dlq_line(i, null_sent)) for i in range(2)]
    rebuilt = _cdc_status.cdc_schema_drift_summary(state, "k")  # new objects: rebuild
    assert calls["classify"] == 2
    assert [(g.kind, g.count) for g in rebuilt] == [("nullability-change", 2)]
    calls.update(classify=0)
    assert _cdc_status.cdc_schema_drift_summary(state, "other-key")  # another key: rebuild
    assert calls["classify"] == 2
    view["records"] = []
    assert _cdc_status.cdc_schema_drift_summary(state, "other-key") == []  # a reset

    # NEW record objects, at least as many as were folded, under the same key: the identity
    # of the last folded record is what tells them apart -- rebuild, never extend the old.
    view["records"] = [parse_dlq_log_message(_dlq_line(i, null_sent)) for i in range(3)]
    _cdc_status.cdc_schema_drift_summary(state, "k")
    calls.update(classify=0)
    view["records"] = [
        parse_dlq_log_message(_dlq_line(40 + i, "sqlstate=42703 x", table="items"))
        for i in range(4)
    ]
    fresh = _cdc_status.cdc_schema_drift_summary(state, "k")
    assert calls["classify"] == 4
    assert [(g.table, g.kind, g.count) for g in fresh] == [("shop.items", "add-column", 4)]

    # Copy-then-extend: a reader still holding the stored entry (the AI tool reads it off the
    # event loop) folds the same tail again and must get the same counts, not double them.
    base = [parse_dlq_log_message(_dlq_line(50 + i, null_sent)) for i in range(2)]
    view["records"] = list(base)
    _cdc_status.cdc_schema_drift_summary(state, "k")
    stored = state._cdc_drift_summary_cache
    view["records"] = base + [parse_dlq_log_message(_dlq_line(52, null_sent))]
    once = [(g.kind, g.count, g.columns) for g in _cdc_status.cdc_schema_drift_summary(state, "k")]
    state._cdc_drift_summary_cache = stored
    twice = [(g.kind, g.count, g.columns) for g in _cdc_status.cdc_schema_drift_summary(state, "k")]
    assert once == twice == [("nullability-change", 3, ["note"])]


@pytest.mark.parametrize("new_count", [3, 4])
def test_start_over_never_shows_the_previous_migrations_dead_letters(new_count) -> None:
    """A CDC-only log key ("cdc:<stack>") is the same across Start over, and reset_in_place
    re-runs __init__ on the same object: the dead-letter memos lived on as dynamic attributes,
    so the new run (at the old record count) was served the previous migration's records --
    its red key-change row, its DLQ depth and chips."""
    from datetime import datetime, timezone

    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration import _cdc_status
    from dsql_migrator.ui.data_migration._state import DataMigrationState

    def _rec(table, code):
        return DataErrorRecord(table=table, error_code=code, message="m",
                               occurred_at=datetime(2026, 10, 3, tzinfo=timezone.utc))

    state = DataMigrationState()
    key = _cdc_status.cdc_error_log_key(state)
    for _ in range(3):
        state.error_log.record(key, _rec("old.orders", "42P10"))
    assert [g.kind for g in _cdc_status.cdc_schema_drift_summary(state, key)] == ["key-change"]
    assert _cdc_status.cdc_dlq_summary(state, key).errors_by_table == {"old.orders": 3}
    state.__init__()  # what reset_in_place does
    assert _cdc_status.cdc_error_log_key(state) == key
    for _ in range(new_count):
        state.error_log.record(key, _rec("new.items", "42703"))
    groups = _cdc_status.cdc_schema_drift_summary(state, key)
    assert [(g.table, g.kind, g.count) for g in groups] == [
        ("new.items", "add-column", new_count)
    ]
    assert _cdc_status.cdc_dlq_summary(state, key).errors_by_table == {"new.items": new_count}


# ---------------------------------------------------------------------------
# G9: every classified rejection gets an accurate label, its real cause and a repair Aurora
# DSQL supports (no ALTER COLUMN TYPE, no SET NOT NULL, an immutable primary key, a CHECK only
# NOT VALID), by migration type and engine.
# ---------------------------------------------------------------------------


def _pg_session():
    from types import SimpleNamespace

    from dsql_migrator.core.models import SourceType

    return SimpleNamespace(source_config=SimpleNamespace(source_type=SourceType.POSTGRES))


def _row(blob_texts, table):
    return [t for t in blob_texts if t.startswith(f"{table}: ")]


def test_a_key_change_banner_is_red_with_the_recreate_remedy() -> None:
    """42P10 dead-letters EVERY insert and update of the table (certain, ongoing loss: error,
    like 07006), but the table exists -- so never the dropped-table notice, and never the
    generic "apply the matching change (e.g. ALTER TABLE)": Aurora DSQL cannot change a key."""
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType
    from dsql_migrator.ui.design import NOTICE_STYLE

    assert "key-change" not in cm._TARGET_SIDE_DRIFT_KINDS
    assert "key-change" not in cm._CERTAIN_LOSS_DRIFT_KINDS
    group = {"table": "shop.orders", "kind": "key-change", "count": 4, "codes": ["42P10"]}
    blob, classes = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC)
    bg, border, _icon_color, _icon = NOTICE_STYLE["error"]
    assert any(bg in c and border in c for c in classes), classes
    assert (
        "Rows are being lost — the key no longer matches the Aurora DSQL primary key" in blob
    )
    assert "shop.orders: the change records' key no longer matches" in blob
    assert "SQLSTATE 42P10" in blob and "EVERY insert and update" in blob
    for needle in ("cannot change a primary key in place", "Replace", "'Drop & reload'",
                   "A table CDC re-keys", "Delete CDC infrastructure"):
        assert needle in blob, needle
    for absent in ("Stop CDC and reload these tables", "e.g. ALTER TABLE", "Fix target schema",
                   "Source schema change detected", "Target table missing"):
        assert absent not in blob, absent

    blob, _ = _render_drift([group], MigrationType.CDC_ONLY)
    assert "Start over" in blob and "restore the original key on the source" in blob
    assert "'Drop & reload'" not in blob and "e.g. ALTER TABLE" not in blob
    # Every insert and update was dead-lettered meanwhile and is never replayed: restoring
    # the key repairs only new writes (the Full Load + CDC remedy says "still need 'Drop &
    # reload'"; CDC only must say how to correct them).
    assert "rows set aside meanwhile are not replayed" in blob
    assert "correct them on Aurora DSQL by hand" in blob


def _rekey_state(current, deployed):
    from types import SimpleNamespace

    return SimpleNamespace(
        cdc_message_key_columns=lambda: current,
        cdc_deployed_message_key_columns=deployed,
        cdc_catalog_drift_findings={},
    )


def test_a_key_change_banner_says_redeploy_only_for_a_table_cdc_re_keys() -> None:
    """The Composite-key redeploy clause applies to a re-keyed table only: the banner reads
    the same re-key map the Fix dialog does, and keeps the conditional hedge only while it
    cannot tell (no state, or the deployed stack's key map not read yet -- a key removed
    after the deploy is still on the stack)."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    mt = MigrationType.FULL_LOAD_AND_CDC
    group = {"table": "shop.orders", "kind": "key-change", "count": 4, "codes": ["42P10"]}
    plain, _ = _render_drift([group], mt, migration_state=_rekey_state({}, {}))
    assert "A table CDC re-keys" not in plain and "Delete CDC infrastructure" not in plain
    assert "Or restore the original key on the source" in plain
    rekeyed, _ = _render_drift(
        [group], mt, migration_state=_rekey_state({}, {"shop.orders": ["tenant_id", "id"]})
    )
    assert "CDC re-keys this table on id, tenant_id" in rekeyed
    assert "A table CDC re-keys" not in rekeyed
    assert "Delete CDC infrastructure and deploy it again" in rekeyed
    unknown, _ = _render_drift([group], mt, migration_state=_rekey_state({}, None))
    assert "A table CDC re-keys" in unknown
    # Several key-change tables, one re-keyed: "this table" cannot name them all.
    two = [group, dict(group, table="shop.items")]
    several, _ = _render_drift(
        two, mt, migration_state=_rekey_state({"shop.items": ["sku"]}, {})
    )
    assert "A table CDC re-keys" in several and "re-keys this table" not in several
    # CDC only never names the Composite-key redeploy, whatever the state.
    for state in (None, _rekey_state({}, {"shop.orders": ["tenant_id", "id"]})):
        blob, _ = _render_drift([group], MigrationType.CDC_ONLY, migration_state=state)
        assert "re-keys" not in blob


def test_the_fix_dialog_key_refusal_text_is_unchanged_by_the_extraction() -> None:
    """_key_change_remedy is shared with the banner; the dialog's own text stays byte-equal."""
    from types import SimpleNamespace

    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    refusal = SimpleNamespace(renamed=(), gone=("id",), missing=(), before=("id",),
                              after=("code",), rekeyed=("tenant_id",))
    plan = SimpleNamespace(refusal=refusal)
    _header, body = cm._fix_refusal_notice(
        plan, "shop.orders", migration_type=MigrationType.FULL_LOAD_AND_CDC, is_pg=False
    )
    assert body.endswith(
        "re-apply shop.orders in Schema Conversion (Step 2) choosing Replace when asked (it "
        "recreates the table empty, with the new key), reload it on the Full Load step with "
        "'Drop & reload', then Start CDC and run Validation (step 4). Or restore the original "
        "key on the source; the rows set aside meanwhile still need 'Drop & reload'. CDC "
        "re-keys this table on tenant_id (Schema Conversion's Composite key), and the "
        "deployed CDC infrastructure carries that key: choose the Composite key again in "
        "Schema Conversion, then Delete CDC infrastructure and deploy it again before Start "
        "CDC. On MySQL, that Start needs a start point newer than this change; the Start CDC "
        "dialog explains the options."
    ), body
    plain = SimpleNamespace(refusal=SimpleNamespace(
        renamed=(), gone=("id",), missing=(), before=(), after=(), rekeyed=(),
    ))
    _h, body = cm._fix_refusal_notice(
        plain, "shop.orders", migration_type=MigrationType.FULL_LOAD_AND_CDC, is_pg=False
    )
    assert "re-keys" not in body and "A table CDC re-keys" not in body


@pytest.mark.parametrize("pg", [False, True])
def test_a_check_violation_banner_names_the_constraint_and_a_not_valid_repair(pg) -> None:
    """23514: usually the tool's own ENUM port (text + CHECK) after the source type gained a
    value. Aurora DSQL adds a CHECK only NOT VALID, one DDL per transaction, and the name is
    the one the dead-letter reason gives -- never a guessed auto-name."""
    from dsql_migrator.ui.data_migration._models import MigrationType
    from dsql_migrator.ui.design import NOTICE_STYLE

    group = {"table": "shop.orders", "kind": "check-violation", "count": 2,
             "columns": ["orders_status_check"], "codes": ["23514"]}
    session = _pg_session() if pg else None
    blob, classes = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC, session)
    bg, border, _icon_color, _icon = NOTICE_STYLE["warning"]
    assert any(bg in c and border in c for c in classes), classes
    assert "a CHECK constraint rejected the row (orders_status_check)" in blob
    for needle in ("SQLSTATE 23514", "DROP CONSTRAINT", "NOT VALID", "one statement per "
                   "transaction", "re-run Step 1"):
        assert needle in blob, needle
    if pg:
        assert "ALTER TYPE ... ADD VALUE" in blob and "MySQL" not in blob
    else:
        assert "MySQL ENUM" in blob
    assert "'Drop & reload' backfill recreates it" in blob
    blob, _ = _render_drift([group], MigrationType.CDC_ONLY, session)
    assert "NOT VALID" in blob and "'Drop & reload'" not in blob
    assert "no Full Load step" in blob
    assert "backfill recreates it" not in blob


def test_a_type_change_banner_recreates_instead_of_an_alter() -> None:
    """Aurora DSQL has no ALTER COLUMN ... TYPE, so 'Apply the matching change (e.g. ALTER
    TABLE)' was impossible advice for a type change (and for 42883 on a delete)."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.orders", "kind": "type-change", "count": 3,
             "codes": ["42804", "42883"]}
    blob, _ = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC)
    assert "Source schema change detected" in blob
    assert "cannot change a column's type in place" in blob and "Replace" in blob
    assert "42883" in blob
    assert "e.g. ALTER TABLE" not in blob
    assert "smallint" not in blob  # the TINYINT(1) advice belongs to the sink's refusal only
    blob, _ = _render_drift([group], MigrationType.CDC_ONLY)
    assert "add a column of that type" in blob
    assert "'Drop & reload'" not in blob


def test_an_add_column_banner_on_cdc_only_never_names_a_reload() -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.orders", "kind": "add-column", "count": 3}
    blob, _ = _render_drift([group], MigrationType.CDC_ONLY)
    assert "e.g. ALTER TABLE" in blob and "no Full Load step" in blob
    assert "'Drop & reload'" not in blob
    # Full Load + CDC keeps today's runbook verbatim.
    blob, _ = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC)
    assert "A plain append reload cannot backfill them" in blob


def test_a_dsql_value_rejection_shares_the_value_fit_banner_without_a_type_claim() -> None:
    """22001/22003 are Aurora DSQL's own "this column cannot hold the value": the value-fit
    header, seed and recreate runbook -- and never "column type changed"."""
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    mt = MigrationType.FULL_LOAD_AND_CDC
    server = {"table": "shop.orders", "kind": "value-does-not-fit", "count": 2,
              "codes": ["22003"]}
    blob, _ = _render_drift([server], mt)
    assert "Rows set aside: a value does not fit its Aurora DSQL column" in blob
    assert "SQLSTATE 22003" in blob and "Aurora DSQL rejected a value its column cannot hold" in blob
    for absent in ("Source schema change detected", "column type changed", "smallint"):
        assert absent not in blob, absent
    blob, _ = _render_drift([server], mt, _pg_session())
    assert "numeric(18,6)" in blob
    # No codes (an older summary): today's why, byte for byte.
    plain = {"table": "shop.orders", "kind": "value-does-not-fit", "count": 2}
    blob, _ = _render_drift([plain], mt)
    assert f"{cm._DRIFT_LABELS['value-does-not-fit'][1]}. 2 records dead-lettered." in blob
    assert "smallint" in blob
    # The sink's refusals AND DSQL's own: one row, the sink's why plus the DSQL sentence.
    both = dict(plain, codes=["22001", "VALUE_DOES_NOT_FIT"])
    blob, _ = _render_drift([both], mt)
    assert cm._DRIFT_LABELS["value-does-not-fit"][1] in blob
    assert "also rejected" in blob and "SQLSTATE 22001" in blob


def test_a_23502_banner_is_neutral_without_evidence_and_gives_both_repairs() -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.orders", "kind": "drop-column", "count": 2, "codes": ["23502"]}
    blob, _ = _render_drift([group], MigrationType.FULL_LOAD_AND_CDC)
    assert "shop.orders: a NOT NULL column got no value —" in blob
    assert "or now allows NULL in it" in blob
    assert "column dropped at the source" not in blob  # not without evidence
    for needle in ("DROP NOT NULL", "DROP COLUMN", "'Drop & reload' refuses",
                   "cannot make the column NOT NULL again"):
        assert needle in blob, needle
    blob, _ = _render_drift([group], MigrationType.CDC_ONLY)
    assert "DROP NOT NULL" in blob and "'Drop & reload'" not in blob
    # The records prove the column was not sent: PostgreSQL names the TOAST cause too.
    named = dict(group, columns=["note"])
    blob, _ = _render_drift([named], MigrationType.FULL_LOAD_AND_CDC, _pg_session())
    assert "a NOT NULL column got no value (note)" in blob
    # Both PostgreSQL TOAST clauses, each pinned on its own: the row's why and the runbook.
    assert "which PostgreSQL does not resend" in blob
    assert (
        "or your exclusions keep it out, or an update left its large (TOASTed) value unchanged"
        in blob
    )
    blob, _ = _render_drift([named], MigrationType.FULL_LOAD_AND_CDC)
    assert "TOAST" not in blob and "do not carry this column" in blob


@pytest.mark.parametrize("pg", [False, True])
def test_a_23502_row_on_a_table_with_exclusions_follows_the_column_the_records_name(pg) -> None:
    """The exclusions clause blamed an excluded column even when the records prove another
    one was not sent, and led with "usually the source dropped it" when the named column IS
    excluded. The named column decides; with no name the hedged clause stays."""
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    def _why(columns, excluded):
        view = SimpleNamespace(schema_drift=[SchemaDriftSummary(
            table="shop.orders", kind="drop-column", count=2, columns=columns, codes=["23502"],
        )])
        ui = _BannerUi()
        cm._render_cdc_schema_drift_banner(
            ui, view, session=_pg_session() if pg else None,
            migration_type=MigrationType.FULL_LOAD_AND_CDC,
            saved_exclusions={"shop.orders": excluded},
        )
        (row,) = _row(ui.texts, "shop.orders")
        return row

    other = _why(["note"], {"blob_col"})  # the records name a column the rules do not cover
    assert "saved column exclusions" not in other and "usually the source dropped it" in other
    excluded = _why(["note"], {"note"})  # the records name the excluded column: the cause
    assert "saved column exclusions keep it out" in excluded
    assert "usually the source dropped it" not in excluded and "TOAST" not in excluded
    partly = _why(["note", "memo"], {"memo"})
    assert "this table's saved column exclusions also keep out memo" in partly
    assert "usually the source dropped it" in partly
    hedged = _why([], {"body"})  # no evidence: the hedged clause, unchanged
    assert "since this table has saved column exclusions (body)" in hedged
    capped = _why([f"c{i}" for i in range(6)], {"body"})  # names capped: more may exist
    assert "since this table has saved column exclusions (body)" in capped


def test_a_null_sent_23502_is_never_called_a_dropped_column() -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.orders", "kind": "nullability-change", "count": 2,
             "columns": ["note"], "codes": ["23502"]}
    for mt in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        blob, _ = _render_drift([group], mt)
        assert "a NULL sent for a NOT NULL column (note)" in blob
        for needle in ("now allows NULL", "DROP NOT NULL", "cannot make the column NOT NULL "
                       "again", "still NOT NULL"):
            assert needle in blob, needle
        assert "column dropped at the source" not in blob
        assert "DROP COLUMN" not in blob


def _catalog_state(*findings):
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from dsql_migrator.core.models import CdcCatalogDriftChange, CdcCatalogDriftFinding

    out = {}
    for table, kind, column, before, after, resolved in findings:
        now = datetime(2026, 10, 3, tzinfo=timezone.utc)
        out[table] = CdcCatalogDriftFinding(
            kinds=[kind], first_seen_at=now,
            changes=[CdcCatalogDriftChange(kind=kind, column=column, before=before, after=after)],
            resolved_at=now if resolved else None, resolved_by="matched" if resolved else None,
        )
    return SimpleNamespace(cdc_catalog_drift_findings=out)


@pytest.mark.parametrize(
    "finding,group,label",
    [
        (("shop.orders", "drop-column", "note", "text", None, False),
         {"kind": "drop-column", "columns": []}, "column dropped at the source"),
        (("shop.orders", "drop-column", "note", "text", None, False),
         {"kind": "drop-column", "columns": ["note"]}, "column dropped at the source"),
        (("shop.orders", "nullability-change", "note", "NOT NULL", "NULL", False),
         {"kind": "nullability-change", "columns": ["note"]}, "NULL rule changed at the source"),
        (("shop.orders", "key-change", None, "id", "code", False),
         {"kind": "key-change"}, "primary key changed at the source"),
    ],
)
def test_an_open_re_check_finding_confirms_the_dead_letter_label(finding, group, label) -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    state = _catalog_state(finding)
    groups = [dict(group, table="shop.orders", count=2)]
    blob, _ = _render_drift(groups, MigrationType.FULL_LOAD_AND_CDC, migration_state=state)
    assert f"shop.orders: {label}" in blob


def test_a_confirmed_key_change_row_names_the_old_and_new_key() -> None:
    """The banner hides the re-check's key-change row, so it carries that row's facts: the
    remedy says "restore the original key", and this says which key that is."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    group = {"table": "shop.orders", "kind": "key-change", "count": 2, "codes": ["42P10"]}
    confirmed = _catalog_state(("shop.orders", "key-change", None, "id", "code", False))
    for mt in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        ui = _BannerUi()
        _render_drift([group], mt, migration_state=confirmed, ui=ui)
        (row,) = _row(ui.texts, "shop.orders")
        assert row.startswith(
            "shop.orders: primary key changed at the source (primary key was (id), now (code)) —"
        ), row
    # Unconfirmed (no finding, or a resolved one): the record label, no facts.
    resolved = _catalog_state(("shop.orders", "key-change", None, "id", "code", True))
    for state in (None, resolved):
        ui = _BannerUi()
        _render_drift([group], MigrationType.FULL_LOAD_AND_CDC, migration_state=state, ui=ui)
        (row,) = _row(ui.texts, "shop.orders")
        assert row.startswith("shop.orders: the change records' key no longer matches the "
                              "Aurora DSQL primary key —"), row


@pytest.mark.parametrize(
    "finding,group",
    [
        # A tightening (NULL -> NOT NULL) dead-letters nothing: it confirms no NULL row.
        (("shop.orders", "nullability-change", "note", "NULL", "NOT NULL", False),
         {"kind": "nullability-change", "columns": ["note"]}),
        # A resolved finding confirms nothing any more.
        (("shop.orders", "drop-column", "note", "text", None, True),
         {"kind": "drop-column", "columns": []}),
        # A drop of ANOTHER column than the records name.
        (("shop.orders", "drop-column", "memo", "text", None, False),
         {"kind": "drop-column", "columns": ["note"]}),
        # A finding of a different kind.
        (("shop.orders", "type-change", "note", "int", "bigint", False),
         {"kind": "drop-column", "columns": []}),
        # Another table's finding.
        (("shop.items", "drop-column", "note", "text", None, False),
         {"kind": "drop-column", "columns": []}),
    ],
)
def test_a_re_check_finding_that_does_not_match_keeps_the_record_label(finding, group) -> None:
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.data_migration._models import MigrationType

    state = _catalog_state(finding)
    groups = [dict(group, table="shop.orders", count=2)]
    blob, _ = _render_drift(groups, MigrationType.FULL_LOAD_AND_CDC, migration_state=state)
    assert f"shop.orders: {cm._DRIFT_LABELS[group['kind']][0]}" in blob


def test_the_dead_letter_banner_hides_only_re_check_rows_it_fully_repairs() -> None:
    """One banner per cause: a key change and a relaxed NULL rule are listed only in the
    banner (which carries their repair); a tightening, a finding with more changes than were
    kept, and a MySQL ENUM change behind a 23514 keep their re-check row (deliberately, like a
    rename: that row shows the new ENUM definition the replacement CHECK needs)."""
    from types import SimpleNamespace

    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    assert {"key-change", "nullability-change"} <= cm._DLQ_SHARED_SCHEMA_KINDS
    assert "check-violation" not in cm._DLQ_SHARED_SCHEMA_KINDS
    assert "value-does-not-fit" not in cm._DLQ_SHARED_SCHEMA_KINDS

    def _finding(kind, before=None, after=None, more=0, columns=("note",)):
        return SimpleNamespace(
            kinds=[kind], more_changes=more, fixable=False,
            changes=[SimpleNamespace(kind=kind, column=c, before=before, after=after)
                     for c in columns],
        )

    assert cm._listed_in_dlq_banner(_finding("key-change"), {"key-change"})
    relaxed = _finding("nullability-change", "NOT NULL", "NULL")
    assert cm._listed_in_dlq_banner(relaxed, {"nullability-change"}, {"note"})
    tightened = _finding("nullability-change", "NULL", "NOT NULL")
    assert not cm._listed_in_dlq_banner(tightened, {"nullability-change"}, {"note"})
    more = _finding("nullability-change", "NOT NULL", "NULL", more=1)
    assert not cm._listed_in_dlq_banner(more, {"nullability-change"}, {"note"})
    enum = _finding("type-change", "enum('a','b')", "enum('a','b','c')")
    assert not cm._listed_in_dlq_banner(enum, {"check-violation"})
    # The banner's NULL row names only the columns a NULL reached: a relaxed column it does
    # not name (none at all, another one, or one of two relaxed) is named only by the
    # re-check row, which therefore stays.
    assert not cm._listed_in_dlq_banner(relaxed, {"nullability-change"})
    assert not cm._listed_in_dlq_banner(relaxed, {"nullability-change"}, {"memo"})
    both = _finding("nullability-change", "NOT NULL", "NULL", columns=("note", "memo"))
    assert not cm._listed_in_dlq_banner(both, {"nullability-change"}, {"note"})
    assert cm._listed_in_dlq_banner(both, {"nullability-change"}, {"note", "memo"})


@pytest.mark.parametrize("pg", [False, True])
def test_mixed_drift_gets_one_repair_per_cause(pg) -> None:
    """A table set with several causes: one lead, one sentence per kind, the type change and
    the value-fit tables in ONE recreate sentence (never two), the header by severity."""
    from dsql_migrator.ui.data_migration._models import MigrationType

    groups = [
        {"table": "shop.a", "kind": "add-column", "count": 1},
        {"table": "shop.b", "kind": "type-change", "count": 1, "codes": ["42804"]},
        {"table": "shop.c", "kind": "value-does-not-fit", "count": 1, "codes": ["22001"]},
        {"table": "shop.d", "kind": "nullability-change", "count": 1, "columns": ["n"]},
        {"table": "shop.e", "kind": "check-violation", "count": 1},
        {"table": "shop.f", "kind": "key-change", "count": 1},
    ]
    session = _pg_session() if pg else None
    ui = _BannerUi()
    blob, _ = _render_drift(groups, MigrationType.FULL_LOAD_AND_CDC, session, ui=ui)
    (runbook,) = [t for t in ui.texts if t.startswith("CDC does not replicate DDL.")]
    assert runbook.count("cannot change a column's type in place") == 1
    assert "re-apply shop.b, shop.c in Schema Conversion" in runbook
    assert runbook.count("For shop.") == 3  # nullability, check, key
    assert "Rows are being lost — the key no longer matches" in blob
    for table in ("shop.a", "shop.b", "shop.c", "shop.d", "shop.e", "shop.f"):
        assert len(_row(ui.texts, table)) == 1, table


def test_the_monitor_names_the_sinks_value_fit_code_exactly() -> None:
    from dsql_migrator.core.cdc_dlq import VALUE_DOES_NOT_FIT_CODE
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    assert cm._SINK_VALUE_FIT_CODE == VALUE_DOES_NOT_FIT_CODE
