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


def test_a_source_truncate_quarantine_is_classified_as_its_own_drift_kind() -> None:
    """A source TRUNCATE is invisible at CDC time unless it gets a named banner.

    Live-measured on PostgreSQL 17.7 -> MSK -> Aurora DSQL: ``TRUNCATE`` on a captured table
    produced NO source record at all (Debezium's ``skipped.operations`` defaults to ``t``), so
    the target kept all 9 truncated rows while the source held 1 -- permanently. The cdc-stack's
    PostgreSQL source now sets ``skipped.operations: none`` and, since Aurora DSQL has no
    TRUNCATE statement, the sink dead-letters the event by name. That single quarantine is far
    below the DLQ badge's 50-record warn threshold, so the DRIFT classification is what actually
    surfaces it -- otherwise only Validation would, after the cut-over decision was in progress.
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


def test_the_postgres_source_connector_emits_truncates_and_mysql_does_not() -> None:
    """The config half of the pair, and the MySQL scope boundary.

    Debezium's ``skipped.operations`` defaults to ``t`` (verified in the shipped
    debezium-core-2.7.4.Final.jar: ``CommonConnectorConfig.SKIPPED_OPERATIONS`` is built
    ``.withDefault("t")``), and on PostgreSQL that default is applied inside the pgoutput
    decoder, so the truncate never becomes a record. The sink branch is useless without this
    key. MySQL CAN emit the same event -- debezium-connector-binlog's
    ``BinlogStreamingChangeEventSource`` dispatches a TRUNCATE change record off the parsed DDL
    -- but that is unverified against a live MySQL source, so the MySQL block keeps the default.
    """
    import pathlib

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

    assert "skipped.operations: none" in pg_block
    # MySQL stays byte-identical: no skipped.operations key at all.
    assert "skipped.operations" not in mysql_block
