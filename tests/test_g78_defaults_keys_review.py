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
    assert "was kept as VARCHAR(20)" in notes and "plain integer" not in notes
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


# --- G79: the review of G68-G78 -----------------------------------------------------------


def test_an_index_over_a_converted_enum_is_emitted_and_sized_by_its_labels() -> None:
    """A PostgreSQL enum becomes text with a CHECK over its labels, yet every index over it
    -- a UNIQUE one included -- was left out with a stale 'not a column type' note; and a
    MySQL ENUM's index was flagged as 'up to ~1025 bytes'."""
    from dsql_migrator.core.converter import SchemaConverter

    pg = TableDef(
        name="app.orders",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="status", mysql_type="app.order_status", type_kind="enum",
                           enum_labels=("new", "paid"), nullable=False)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_status", columns=["status"]),
                 IndexDef(name="ux_id_status", columns=["id", "status"], unique=True)],
    )
    (converted,) = SchemaConverter(source_type=SourceType.POSTGRES).convert(
        SourceInventory(tables=[pg])
    ).tables
    assert converted.index_ddls == [
        'CREATE INDEX ASYNC "ix_status" ON "app"."orders" ("status")',
        'CREATE UNIQUE INDEX ASYNC "ux_id_status" ON "app"."orders" ("id", "status")',
    ]
    mysql = TableDef(
        name="app.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="status", mysql_type="enum('new','paid')", nullable=False)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_status", columns=["status"])],
    )
    for table, engine in ((pg, SourceType.POSTGRES), (mysql, SourceType.MYSQL)):
        (converted,) = SchemaConverter(source_type=engine).convert(
            SourceInventory(tables=[table])
        ).tables
        assert not any("1024 bytes combined" in w.message for w in converted.warnings), engine


def test_a_unique_key_over_an_enum_is_still_offered_for_promotion() -> None:
    from dsql_migrator.core.assessor import CompatibilityAssessor

    table = TableDef(
        name="app.kinds",
        columns=[ColumnDef(name="tenant_id", mysql_type="integer", nullable=False),
                 ColumnDef(name="kind", mysql_type="app.kind_t", type_kind="enum",
                           enum_labels=("a", "b"), nullable=False)],
        indexes=[IndexDef(name="ux_tk", columns=["tenant_id", "kind"], unique=True)],
    )
    (item,) = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    ).items
    (concern,) = [c for c in item.concerns if c.rule_id == "NO_PRIMARY_KEY"]
    assert "ux_tk (tenant_id, kind)" in concern.recommendation


def test_a_mysql_bigint_unsigned_composite_key_is_not_told_to_add_an_identity() -> None:
    """It is emitted numeric(20,0): no identity can be added to it after apply."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import SchemaConverter

    table = TableDef(
        name="shop.events",
        columns=[ColumnDef(name="id", mysql_type="bigint unsigned", nullable=False),
                 ColumnDef(name="created_at", mysql_type="datetime", nullable=False)],
        primary_key=["id", "created_at"],
        auto_increment_column="id",
    )
    inventory = SourceInventory(tables=[table])
    (converted,) = SchemaConverter().convert(inventory).tables
    notes = " ".join(w.message for w in converted.warnings)
    assert "was kept as DECIMAL(20, 0)" in notes and "add an identity" not in notes
    (item,) = CompatibilityAssessor().assess(inventory).items
    (concern,) = [c for c in item.concerns if c.rule_id == "AUTO_INCREMENT"]
    assert "plain integer" not in concern.risk and "add an identity" not in concern.recommendation


def test_the_picker_renders_a_stored_leader_the_size_filter_hides() -> None:
    """A stored (code, id) key over a varchar(255) leader: the dropdown's value was not
    among its options and NiceGUI raised, stopping the whole generated list."""
    import dsql_migrator.ui.schema_conversion as screen
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from tests.test_ui_schema_conversion import _PickerUi

    table = _customers()
    stored = (
        'CREATE TABLE "shop"."customers" ("id" BIGINT NOT NULL, "email" VARCHAR(255) NOT NULL, '
        '"tenant_id" INT NOT NULL, PRIMARY KEY ("email", "id"))'
    )
    state = SchemaConversionState()
    state.set_edited_target_ddl("shop.customers", stored)
    ui = _PickerUi()
    screen._render_pk_strategy_picker(ui, table, state, lambda: None)
    assert ui.select_value == "email"
    options = ui.select_options
    assert "email" in options and "tenant_id" in options


def test_the_picker_does_not_convert_every_table_to_render() -> None:
    """Sizing the composite leaders converted each generated table on every render
    (12 s per render at 2,500 tables)."""
    import dsql_migrator.ui.schema_conversion as screen
    from dsql_migrator.core import converter as core
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from tests.test_ui_schema_conversion import _PickerUi

    calls: list = []
    original = core._oversized_composite_leaders

    def counting(*args):
        calls.append(args)
        return original(*args)

    core._oversized_composite_leaders = counting
    try:
        screen._render_pk_strategy_picker(
            _PickerUi(), _customers(), SchemaConversionState(), lambda: None
        )
    finally:
        core._oversized_composite_leaders = original
    assert calls == []


def test_a_resumed_or_renamed_pick_keeps_its_strategy_notes() -> None:
    """A Keep script stored by an earlier version (or before an index rename) no longer
    matched today's rendering byte for byte, so the screen showed the identity default's
    'KEEPS its value generation' beside a Keep DDL."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.schema_conversion import TABLE_PREFIX, generate_previews, render_target_ddl
    from dsql_migrator.ui.schema_conversion_apply import build_keep_conversion

    table = TableDef(
        name="shop.users",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="name", mysql_type="varchar(40)")],
        primary_key=["id"],
        auto_increment_column="id",
    )
    inventory = SourceInventory(tables=[table])
    converter = SchemaConverter()
    result = converter.convert(inventory)
    keep = render_target_ddl(build_keep_conversion(converter, table))
    older = " ".join(keep.split()).replace('"name" VARCHAR(40)', '"name" VARCHAR(40) NULL')
    (preview,) = generate_previews(
        [TABLE_PREFIX + "shop.users"], inventory, result, stored_ddl_of={"shop.users": older}.get
    )
    notes = " ".join(w.message for w in preview.warnings)
    assert "kept as a plain integer" in notes and "KEEPS its value generation" not in notes


def test_an_edit_without_its_final_semicolon_keeps_the_fk_section_apart() -> None:
    from dsql_migrator.ui.schema_conversion_apply import (
        join_target_ddl_fk_sections,
        split_sql_statements,
        split_target_ddl_fk_sections,
    )

    fk = (
        'ALTER TABLE "app"."orders" ADD CONSTRAINT "fk_c" FOREIGN KEY ("cid") '
        'REFERENCES "app"."customers" ("id") NOT VALID;'
    )
    edited = 'CREATE TABLE "app"."orders" ("id" INT, "cid" INT);\n\nCREATE INDEX ASYNC "ix_cid" ON "app"."orders" ("cid")'
    joined = join_target_ddl_fk_sections(edited, fk)
    assert len(split_sql_statements(joined)) == 3
    assert split_target_ddl_fk_sections(joined)[1].strip() == fk


def test_a_stored_composite_refusal_comment_is_no_edit() -> None:
    """0.1.570's picker stored '-- Could not apply a composite primary key ...' as the
    table's only DDL; a resumed session kept sending it to Schema Apply."""
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.evaluation import EvaluationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    placeholder = "-- Could not apply a composite primary key to shop.orders: too large"
    state = SchemaConversionState()
    state.set_edited_target_ddl("shop.orders", placeholder)
    assert state.get_edited_target_ddl("shop.orders") is None
    # A snapshot an earlier build wrote with the comment stored.
    state.edited_target_ddls = {"shop.orders": placeholder, "shop.items": "CREATE TABLE x (a int)"}
    snapshot = capture_session_snapshot(
        "s1", SessionConnectionState(), EvaluationState(), state, DataMigrationState()
    )
    restored = SchemaConversionState()
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(snapshot.model_dump_json()),
        SessionConnectionState(), EvaluationState(), restored, DataMigrationState(),
    )
    assert restored.edited_target_ddls == {"shop.items": "CREATE TABLE x (a int)"}
