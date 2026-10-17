"""G78: the review of G63/G64 -- PostgreSQL DEFAULTs, unkeyable columns, the PK picker."""

from __future__ import annotations

from dsql_migrator.core.models import ColumnDef, IndexDef, SourceInventory, SourceType, TableDef


def test_a_trailing_line_comment_survives_the_fk_section_round_trip() -> None:
    """The ';' appended to a statement whose last line is a '--' comment landed inside the
    comment, so the next statement was glued on: one DDL Schema Apply could not run."""
    from dsql_migrator.core.converter import terminated_sql_statement
    from dsql_migrator.ui.schema_conversion_apply import (
        join_target_ddl_fk_sections,
        split_sql_statements,
        split_target_ddl_fk_sections,
    )

    script = (
        'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, "deleted_at" TIMESTAMP, '
        'PRIMARY KEY ("id"));\n\n'
        'CREATE INDEX ASYNC "ix_live" ON "shop"."orders" ("id")\n'
        "-- WHERE (deleted_at IS NULL)\n;\n\n"
        'CREATE INDEX ASYNC "ix_deleted" ON "shop"."orders" ("deleted_at");\n\n'
        'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_x" FOREIGN KEY ("id") '
        'REFERENCES "shop"."customers" ("id") NOT VALID;'
    )
    before = split_sql_statements(script)
    non_fk, fk = split_target_ddl_fk_sections(script)
    after = split_sql_statements(join_target_ddl_fk_sections(non_fk, fk))
    assert len(after) == len(before) == 4
    assert terminated_sql_statement("SELECT 1 -- note") == "SELECT 1 -- note\n;"
    assert terminated_sql_statement("SELECT 1;") == "SELECT 1;"
    assert terminated_sql_statement("SELECT '--';") == "SELECT '--';"


def _customers() -> TableDef:
    return TableDef(
        name="shop.customers",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="email", mysql_type="varchar(255)", nullable=False),
            ColumnDef(name="tenant_id", mysql_type="int", nullable=False),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )


def test_a_leader_too_large_for_the_key_is_not_offered() -> None:
    """(email, id) is ~1028 bytes, over the 1 KiB key limit: the first click on 'Composite
    key' stored the refusal's comment as the table's only DDL, and the valid tenant_id
    could never be chosen."""
    from dsql_migrator.ui.schema_conversion_apply import (
        composite_leading_candidates,
        default_composite_leading,
    )

    assert composite_leading_candidates(_customers()) == ["tenant_id"]
    assert default_composite_leading(_customers()) == "tenant_id"
    pg = _customers().model_copy(update={"columns": [
        ColumnDef(name="id", mysql_type="bigint", nullable=False),
        ColumnDef(name="name", mysql_type="text", nullable=False),
        ColumnDef(name="tenant_id", mysql_type="integer", nullable=False),
    ]})
    assert composite_leading_candidates(pg, is_postgres=True) == ["tenant_id"]


def test_a_refused_composite_key_is_never_stored() -> None:
    import dsql_migrator.ui.schema_conversion as screen
    from dsql_migrator.core.converter import is_composite_key_placeholder
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.schema_conversion_apply import build_composite_conversion
    from tests.test_ui_schema_conversion import _PickerUi, _event

    converter_refusal = build_composite_conversion(
        screen.SchemaConverter(), _customers(), "email"
    )
    assert is_composite_key_placeholder(converter_refusal.target_ddl)

    ui = _PickerUi()
    notified: list = []
    ui.notify = lambda message, **_k: notified.append(message)
    state = SchemaConversionState()
    refreshed: list = []
    screen._render_pk_strategy_picker(ui, _customers(), state, lambda: refreshed.append(1))
    ui.tile_clicks[-1]()  # Composite key: tenant_id, the one that fits
    stored = state.get_edited_target_ddl("shop.customers")
    assert stored and not is_composite_key_placeholder(stored)
    # A refused leader (forced through the dropdown) keeps the stored script.
    ui = _PickerUi()
    ui.notify = lambda message, **_k: notified.append(message)
    screen._render_pk_strategy_picker(ui, _customers(), state, lambda: refreshed.append(1))
    ui.select_el.on_change(_event("email"))
    assert state.get_edited_target_ddl("shop.customers") == stored
    assert notified and "1024-byte key limit" in notified[-1]


def test_the_no_alternative_hint_gives_the_tables_own_reasons() -> None:
    """The hint said 'no single-column serial / identity key and no NOT NULL non-key
    column' for a varchar serial key beside jsonb / bytea NOT NULL columns."""
    from dsql_migrator.ui.schema_conversion import _no_alternative_reasons

    codes = TableDef(
        name="app.codes",
        columns=[
            ColumnDef(name="code", mysql_type="character varying(20)", nullable=False),
            ColumnDef(name="doc", mysql_type="jsonb", nullable=False),
            ColumnDef(name="blob", mysql_type="bytea", nullable=False),
        ],
        primary_key=["code"],
        auto_increment_column="code",
    )
    reasons = _no_alternative_reasons(codes, source_type=SourceType.POSTGRES)
    assert "'code' is character varying(20), not an integer" in reasons
    assert "doc becomes jsonb, which Aurora DSQL cannot index" in reasons
    assert "blob becomes bytea" in reasons
    plain = TableDef(
        name="app.t", columns=[ColumnDef(name="id", mysql_type="uuid", nullable=False)],
        primary_key=["id"],
    )
    assert _no_alternative_reasons(plain, source_type=SourceType.POSTGRES) == (
        "this table has no single-column serial / identity key, and no NOT NULL non-key "
        "column to lead a composite key"
    )


def test_a_no_pk_table_is_not_told_to_promote_an_unkeyable_unique_key() -> None:
    """Promoting ux_digest (a binary/bytea column) on the source makes the table refused
    at both steps: Aurora DSQL cannot key a bytea column."""
    from dsql_migrator.core.assessor import CompatibilityAssessor

    for engine, type_ in ((SourceType.MYSQL, "binary(16)"), (SourceType.POSTGRES, "bytea")):
        table = TableDef(
            name="app.blobs",
            columns=[ColumnDef(name="digest", mysql_type=type_, nullable=False)],
            indexes=[IndexDef(name="ux_digest", columns=["digest"], unique=True)],
        )
        (item,) = CompatibilityAssessor(source_type=engine).assess(
            SourceInventory(tables=[table])
        ).items
        (concern,) = [c for c in item.concerns if c.rule_id == "NO_PRIMARY_KEY"]
        assert "ux_digest" not in concern.recommendation, engine
        assert "UUID/random key" in concern.recommendation


def test_key_notes_word_a_non_integer_serial_key_by_its_type() -> None:
    """'The integer key ... was kept as a plain integer' for a varchar key; a composite
    double precision one was told to add an identity after apply, which needs bigint."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import SchemaConverter

    codes = TableDef(
        name="app.codes",
        columns=[ColumnDef(name="code", mysql_type="character varying(20)", nullable=False,
                           default="nextval('app.codes_seq'::regclass)")],
        primary_key=["code"],
        auto_increment_column="code",
    )
    events = TableDef(
        name="app.events",
        columns=[ColumnDef(name="tenant", mysql_type="integer", nullable=False),
                 ColumnDef(name="seq", mysql_type="double precision", nullable=False,
                           default="nextval('app.ev_seq'::regclass)")],
        primary_key=["tenant", "seq"],
        auto_increment_column="seq",
    )
    converter = SchemaConverter(source_type=SourceType.POSTGRES)
    (converted,) = converter.convert(SourceInventory(tables=[codes])).tables
    notes = " ".join(w.message for w in converted.warnings)
    assert "was kept as character varying(20)" in notes and "plain integer" not in notes
    (converted,) = converter.convert(SourceInventory(tables=[events])).tables
    notes = " ".join(w.message for w in converted.warnings)
    assert "plain integer" not in notes and "add an identity" not in notes
    (item,) = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[events])
    ).items
    (concern,) = [c for c in item.concerns if c.rule_id == "AUTO_INCREMENT"]
    assert "stays double precision" in concern.risk
    assert "add an identity" not in concern.recommendation


def test_the_formatted_default_note_does_not_contradict_the_sequence_item() -> None:
    """It prescribed CREATE SEQUENCE app.invoice_seq CACHE 65536 (blocks, out of order)
    where the source and Evaluation's sequence item had CACHE 1, and left the name
    unquoted."""
    from dsql_migrator.core.converter_postgres import pg_column_default_sql

    column = ColumnDef(
        name="invoice_no", mysql_type="text", nullable=False,
        default="('INV-'::text || lpad((nextval('\"App\".\"Invoice_seq\"'::regclass))::text, 8, '0'::text))",
    )
    _default, reason = pg_column_default_sql(column, is_key_column=False)
    assert "CACHE 65536" not in reason
    assert "PG_STANDALONE_SEQUENCE" in reason
    assert 'CREATE SEQUENCE "App"."Invoice_seq" CACHE 1' in reason


def test_a_hash_index_is_sized_on_the_type_the_conversion_emits() -> None:
    """Evaluation sized money as unbounded (LOSS, 'can hold longer values') where Schema
    Conversion sized its numeric(23,4) at 13 bytes (RECOMMENDATION)."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import SchemaConverter

    for type_, kind in (("money", "RECOMMENDATION"), ("text", "LOSS")):
        table = TableDef(
            name="app.pay",
            columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                     ColumnDef(name="amount", mysql_type=type_)],
            primary_key=["id"],
            indexes=[IndexDef(name="ix_amount", columns=["amount"], method="hash")],
        )
        inventory = SourceInventory(tables=[table])
        (item,) = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory).items
        (concern,) = [c for c in item.concerns if c.rule_id == "PG_INDEX_NOT_PRESERVED"]
        (converted,) = SchemaConverter(source_type=SourceType.POSTGRES).convert(inventory).tables
        (note,) = [w for w in converted.warnings if "hash" in w.message]
        assert concern.note_kind.value == note.kind.value == kind, type_
