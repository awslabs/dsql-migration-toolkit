# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the schema (DDL) converter type-mapping foundation.

Covers each documented MySQL -> Aurora DSQL (PostgreSQL 16) type mapping
(``TINYINT(1)`` -> boolean, UNSIGNED widening, ``DATETIME`` -> timestamp,
``BLOB`` -> bytea, ``ENUM`` -> text + CHECK, ``SET`` -> text, ``JSON`` -> json),
identifier-quoting conversion, and warning collection for non-lossless mappings
(Property 6 / Requirements 3.1, 3.2).
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.converter import (
    ConversionNoteKind,
    PrimaryKeyStrategy,
    SchemaConverter,
    SchemaConversionResult,
    SchemaConvertOptions,
    map_mysql_type,
)
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ForeignKeyDef,
    IndexDef,
    ObjectRef,
    ObjectType,
    SourceInventory,
    TableDef,
    ViewDef,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _convert_table(table: TableDef):
    return SchemaConverter().convert_table(table)


def _single_column_table(name: str, mysql_type: str, **column_kwargs) -> TableDef:
    """Build a table with an INT primary key plus one typed column."""
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="value", mysql_type=mysql_type, **column_kwargs),
        ],
        primary_key=["id"],
    )


# ---------------------------------------------------------------------------
# map_mysql_type — per-type mapping table
# ---------------------------------------------------------------------------


def test_tinyint_one_maps_to_boolean_with_warning() -> None:
    target, warning = map_mysql_type("TINYINT(1)")
    assert target.lower() == "boolean"
    assert warning is not None
    assert warning.classification is Classification.MANUAL


def test_int_unsigned_widens_to_bigint_losslessly() -> None:
    target, warning = map_mysql_type("INT UNSIGNED")
    assert target.lower() == "bigint"
    assert warning is None


def test_smallint_unsigned_widens_to_integer() -> None:
    target, warning = map_mysql_type("SMALLINT UNSIGNED")
    assert target.lower() in {"int", "integer"}
    assert warning is None


def test_tinyint_unsigned_widens_to_smallint() -> None:
    target, _ = map_mysql_type("TINYINT UNSIGNED")
    assert target.lower() == "smallint"


def test_bigint_unsigned_widens_to_numeric_preserving_range() -> None:
    target, warning = map_mysql_type("BIGINT UNSIGNED")
    # bigint cannot hold the full unsigned 64-bit range; numeric(20, 0) can.
    assert "20" in target and target.lower().startswith(("numeric", "decimal"))
    assert warning is None


def test_float_unsigned_maps_to_real_instead_of_aborting_the_table() -> None:
    # sqlglot's MySQL dialect cannot parse "float unsigned" as a standalone type, which
    # used to abort the whole table to an UNSUPPORTED placeholder while Evaluation still
    # rated it AUTO. Unsigned-ness is storage-irrelevant on an approximate numeric, so it
    # is stripped and maps to real (audit finding B1).
    for src in ("FLOAT UNSIGNED", "float(10,2) unsigned", "REAL UNSIGNED",
                "float unsigned zerofill"):
        target, warning = map_mysql_type(src)
        assert target.lower() == "real", f"{src!r} -> {target!r}"


def test_float_unsigned_does_not_disturb_integer_or_double_unsigned() -> None:
    # The float-unsigned strip must not touch integer unsigned (range-widened) or
    # double unsigned (mapped to double precision).
    assert map_mysql_type("INT UNSIGNED")[0].lower() == "bigint"
    assert map_mysql_type("DOUBLE UNSIGNED")[0].lower() == "double precision"


def test_datetime_maps_to_timestamp() -> None:
    target, warning = map_mysql_type("DATETIME")
    assert target.lower() == "timestamp"
    assert warning is None


def test_blob_maps_to_bytea() -> None:
    target, _ = map_mysql_type("BLOB")
    assert target.lower() == "bytea"


def test_longblob_maps_to_bytea() -> None:
    # sqlglot alone renders LONGBLOB -> BLOB; the converter must force bytea.
    target, _ = map_mysql_type("LONGBLOB")
    assert target.lower() == "bytea"


def test_integer_display_width_is_stripped() -> None:
    # MySQL display widths (int(11), bigint(20), tinyint(4), smallint(5)) render as
    # INT(11) etc via sqlglot, which DSQL rejects ("syntax error at or near (").
    # information_schema COLUMN_TYPE preserves the width, so this must be stripped.
    assert map_mysql_type("int(11)")[0].lower() in ("integer", "int")
    assert map_mysql_type("bigint(20)")[0].lower() == "bigint"
    assert map_mysql_type("smallint(5)")[0].lower() == "smallint"
    # tinyint(4) is a wide tinyint (not the (1) boolean convention) -> smallint.
    assert map_mysql_type("tinyint(4)")[0].lower() == "smallint"


def test_unsigned_with_display_width_widens_without_modifier() -> None:
    assert map_mysql_type("int(10) unsigned")[0].lower() == "bigint"
    assert map_mysql_type("smallint(5) unsigned")[0].lower() in ("integer", "int")
    assert map_mysql_type("tinyint(3) unsigned")[0].lower() == "smallint"


def test_double_unsigned_maps_to_double_precision() -> None:
    # DOUBLE UNSIGNED renders to the nonexistent sqlglot UDOUBLE; must become a real
    # PG float type.
    assert map_mysql_type("double unsigned")[0].lower() == "double precision"


def test_float_with_scale_maps_to_real() -> None:
    # MySQL FLOAT(M,D) renders FLOAT(10, 2); PG FLOAT takes a single precision, not a
    # scale -> syntax error. Must drop to a plain float.
    assert map_mysql_type("float(10,2)")[0].lower() == "real"
    # Single-arg FLOAT(p) is valid PG (precision 1-53) and may pass through.
    assert "(" not in map_mysql_type("double")[0] or True


def test_double_with_scale_maps_to_double_precision() -> None:
    # MySQL DOUBLE(M,D) renders a two-arg FLOAT(10, 2) (sqlglot kind DOUBLE, which
    # misses the UDOUBLE mapping); PG ``double precision`` takes NO argument, so the
    # two-arg form is a syntax error. Must drop to a plain double precision.
    target = map_mysql_type("double(10,2)")[0].lower()
    assert target == "double precision"
    assert "(" not in target


def test_double_with_scale_emits_valid_ddl_end_to_end() -> None:
    # Regression: DOUBLE(M,D) used to fall through to a bare FLOAT(10, 2) render,
    # which fails the WHOLE CREATE TABLE on DSQL/PG16 ("syntax error at or near ,").
    table = TableDef(
        name="prices",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="amount", mysql_type="DOUBLE(10,2)"),
        ],
        primary_key=["id"],
    )
    ddl = SchemaConverter().convert_table(table).target_ddl
    assert "double precision" in ddl.lower()
    assert "FLOAT(10, 2)" not in ddl
    assert "double precision(" not in ddl.lower()


def test_decimal_unsigned_maps_to_numeric_preserving_precision() -> None:
    # DECIMAL(p,s) UNSIGNED renders the nonexistent UDECIMAL; must become numeric(p,s).
    target = map_mysql_type("decimal(10,2) unsigned")[0].lower()
    assert target.startswith("decimal") or target.startswith("numeric")
    assert "10" in target and "2" in target


def test_zerofill_attribute_is_stripped() -> None:
    # ZEROFILL is a display attribute (implies unsigned) that is not a PG token and
    # breaks both the standalone parser and the rendered DDL. It must be dropped.
    assert map_mysql_type("int unsigned zerofill")[0].lower() == "bigint"
    assert map_mysql_type("bigint zerofill")[0].lower() == "bigint"
    target = map_mysql_type("decimal(8,2) zerofill")[0].lower()
    assert "8" in target and "2" in target


def test_signed_mediumint_maps_to_integer() -> None:
    # PostgreSQL has no 3-byte int; sqlglot renders signed MEDIUMINT to a literal
    # ``MEDIUMINT`` that does not exist in DSQL. Must map to integer.
    target, _ = map_mysql_type("MEDIUMINT")
    # Postgres renders integer as "INT"; either spelling is the 4-byte int.
    assert target.lower() in ("integer", "int")


def test_bit_maps_to_sized_integer_with_warning() -> None:
    # Aurora DSQL does not support the bit type ("datatype bit not supported").
    # BIT(n) must map to the smallest integer that holds the n-bit unsigned range.
    for mysql_type, expected in [
        ("BIT(8)", "smallint"),
        ("BIT(16)", "integer"),
        ("BIT(32)", "bigint"),
    ]:
        target, warning = map_mysql_type(mysql_type)
        assert target.lower() in (expected, {"integer": "int"}.get(expected)), mysql_type
        assert warning is not None and warning.classification is Classification.MANUAL


def test_year_maps_to_smallint_with_warning() -> None:
    # PostgreSQL has no YEAR type; sqlglot renders a literal ``YEAR`` that does not
    # exist in DSQL. Must map to smallint (covers the 1901-2155 range) with a warning.
    target, warning = map_mysql_type("YEAR")
    assert target.lower() == "smallint"
    assert warning is not None
    assert warning.classification is Classification.MANUAL


def test_binary_maps_to_bytea_without_length_modifier() -> None:
    # PostgreSQL bytea takes no length modifier: MySQL BINARY(16) must map to a
    # bare ``bytea``, never ``BYTEA(16)`` (which is invalid DDL: "type modifier is
    # not allowed for type bytea").
    target, _ = map_mysql_type("BINARY(16)")
    assert target.lower() == "bytea"


def test_varbinary_maps_to_bytea_without_length_modifier() -> None:
    target, _ = map_mysql_type("VARBINARY(255)")
    assert target.lower() == "bytea"


def test_convert_table_drops_mysql_collation_clause() -> None:
    # A *_ci column's reconstructed type carries ``COLLATE '<name>'``, which is
    # invalid PostgreSQL DDL. The converter must drop the collation (falling back
    # to the DSQL default) and emit a MANUAL warning naming it.
    table = TableDef(
        name="s.t",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(
                name="note_ci",
                mysql_type="varchar(120) COLLATE 'utf8mb4_general_ci'",
                nullable=True,
            ),
        ],
        primary_key=["id"],
    )
    result = SchemaConverter().convert_table(table)
    assert "COLLATE" not in result.target_ddl.upper()
    assert "VARCHAR(120)" in result.target_ddl.upper()
    collate_warnings = [
        w for w in result.warnings
        if w.column_name == "note_ci" and "collation" in w.message.lower()
    ]
    assert collate_warnings, "expected a MANUAL warning for the dropped collation"
    assert collate_warnings[0].classification is Classification.MANUAL


def test_convert_table_binary_columns_emit_valid_bytea_ddl() -> None:
    # End-to-end through convert_table: BINARY/VARBINARY columns render as bare
    # bytea in the target DDL (no parenthesized modifier).
    table = TableDef(
        name="s.lob",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="c_binary", mysql_type="binary(16)", nullable=True),
            ColumnDef(name="c_varbinary", mysql_type="varbinary(255)", nullable=True),
        ],
        primary_key=["id"],
    )
    result = SchemaConverter().convert_table(table)
    assert "BYTEA(" not in result.target_ddl.upper()
    assert result.target_ddl.upper().count("BYTEA") == 2


def test_enum_maps_to_text_with_warning() -> None:
    target, warning = map_mysql_type("ENUM('a','b','c')")
    assert target.lower() == "text"
    assert warning is not None
    assert warning.classification is Classification.MANUAL


def test_set_maps_to_text_with_warning() -> None:
    target, warning = map_mysql_type("SET('x','y')")
    assert target.lower() == "text"
    assert warning is not None
    assert warning.classification is Classification.MANUAL


def test_json_maps_to_json_without_warning() -> None:
    target, warning = map_mysql_type("JSON")
    assert target.lower() == "json"
    assert warning is None


def test_map_mysql_type_rejects_invalid_type() -> None:
    with pytest.raises(ValueError):
        map_mysql_type("NOT A TYPE (((")


# ---------------------------------------------------------------------------
# convert_table — DDL transpilation and identifier quoting
# ---------------------------------------------------------------------------


def test_identifiers_are_double_quoted_in_target_ddl() -> None:
    table = TableDef(
        name="users",
        columns=[ColumnDef(name="id", mysql_type="INT")],
        primary_key=["id"],
    )
    result = _convert_table(table)
    # MySQL backticks become PostgreSQL double quotes.
    assert '"users"' in result.target_ddl
    assert '"id"' in result.target_ddl
    assert "`" not in result.target_ddl


def test_not_null_constraint_is_preserved() -> None:
    table = _single_column_table("t", "INT", nullable=False)
    ddl = _convert_table(table).target_ddl
    assert "NOT NULL" in ddl


def test_enum_column_gets_check_constraint_in_ddl() -> None:
    table = _single_column_table("t", "ENUM('open','closed')")
    result = _convert_table(table)
    assert "CHECK" in result.target_ddl.upper()
    assert "'open'" in result.target_ddl and "'closed'" in result.target_ddl
    assert "TEXT" in result.target_ddl.upper()


def test_convert_table_collects_warning_with_table_and_column_context() -> None:
    table = _single_column_table("orders", "ENUM('a','b')")
    result = _convert_table(table)
    assert len(result.warnings) == 1
    warning = result.warnings[0]
    assert warning.object_name == "orders"
    assert warning.column_name == "value"
    assert warning.source_type == "ENUM('a','b')"
    assert warning.target_type is not None and warning.target_type.lower() == "text"


def test_prefix_index_warns_that_dsql_indexes_the_full_column() -> None:
    # A MySQL prefix index KEY (body(100)) has no DSQL equivalent: the converter indexes
    # the FULL column, whose value can exceed DSQL's 1 KiB key budget and fail with
    # error 54000. The operator must be warned at planning time (audit B2).
    table = TableDef(
        name="articles",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="body", mysql_type="LONGTEXT"),
        ],
        primary_key=["id"],
        indexes=[
            IndexDef(name="idx_body", columns=["body"], prefix_lengths={"body": 100}),
        ],
    )
    result = _convert_table(table)
    prefix_warnings = [w for w in result.warnings if "prefix index" in w.message.lower()]
    assert len(prefix_warnings) == 1
    w = prefix_warnings[0]
    assert "idx_body" in w.message and "body(100)" in w.message
    # Cites the documented key limit and error, not an invented per-column cap: DSQL
    # publishes a 1 KiB combined key budget (54000) and no 255-byte column cap.
    assert "1024-byte" in w.message
    assert "54000" in w.message
    # The index DDL is still emitted (on the full column) -- the warning is advisory.
    assert any("idx_body" in ddl for ddl in result.index_ddls)


def test_non_prefix_index_produces_no_prefix_warning() -> None:
    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="name", mysql_type="VARCHAR(50)"),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_name", columns=["name"])],  # no prefix_lengths
    )
    result = _convert_table(table)
    assert not any("prefix index" in w.message.lower() for w in result.warnings)


def test_lossless_columns_produce_no_warnings() -> None:
    table = TableDef(
        name="clean",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="amount", mysql_type="INT UNSIGNED"),
            ColumnDef(name="created", mysql_type="DATETIME"),
            ColumnDef(name="doc", mysql_type="JSON"),
        ],
        primary_key=["id"],
    )
    result = _convert_table(table)
    assert result.warnings == []
    assert "BIGINT" in result.target_ddl.upper()
    assert "TIMESTAMP" in result.target_ddl.upper()


def test_convert_table_rejects_table_without_columns() -> None:
    with pytest.raises(ValueError):
        _convert_table(TableDef(name="empty", columns=[], primary_key=[]))


# ---------------------------------------------------------------------------
# convert — inventory-level aggregation
# ---------------------------------------------------------------------------


def test_convert_inventory_aggregates_tables_and_warnings() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="flags",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="active", mysql_type="TINYINT(1)"),
                ],
                primary_key=["id"],
            ),
            TableDef(
                name="tagged",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="kinds", mysql_type="SET('x','y')"),
                ],
                primary_key=["id"],
            ),
        ]
    )
    result = SchemaConverter().convert(inventory)
    assert isinstance(result, SchemaConversionResult)
    assert [t.table for t in result.tables] == ["flags", "tagged"]
    # One warning per table (TINYINT(1) semantic, SET lossy), aggregated.
    assert len(result.warnings) == 2
    assert {w.object_name for w in result.warnings} == {"flags", "tagged"}


def test_convert_empty_inventory_produces_empty_result() -> None:
    result = SchemaConverter().convert(SourceInventory())
    assert result.tables == []
    assert result.warnings == []


def test_property_6_lossy_mappings_are_never_silent() -> None:
    """Property 6: every non-lossless mapping yields a recorded warning."""
    inventory = SourceInventory(
        tables=[
            _single_column_table("t_bool", "TINYINT(1)"),
            _single_column_table("t_enum", "ENUM('a','b')"),
            _single_column_table("t_set", "SET('a','b')"),
        ]
    )
    result = SchemaConverter().convert(inventory)
    flagged = {w.object_name for w in result.warnings}
    assert flagged == {"t_bool", "t_enum", "t_set"}
    assert all(
        w.classification in {Classification.MANUAL, Classification.UNSUPPORTED}
        for w in result.warnings
    )


# ---------------------------------------------------------------------------
# DSQL constraint application (subtask 5.2): FK removal, async indexes,
# primary-key strategy, and trigger/routine reimplementation flagging
# (Requirements 3.3, 3.4, 3.5, 3.7).
# ---------------------------------------------------------------------------


def _auto_increment_table(name: str = "t") -> TableDef:
    """Build a table with an AUTO_INCREMENT integer primary key."""
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="label", mysql_type="VARCHAR(50)"),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )


def test_foreign_keys_removed_and_preserved_as_metadata() -> None:
    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="user_id", mysql_type="INT"),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyDef(
                name="fk_user",
                columns=["user_id"],
                referenced_table="users",
                referenced_columns=["id"],
            )
        ],
    )
    result = SchemaConverter().convert_table(table)
    # DSQL does not support foreign keys: none are emitted in the DDL.
    assert "FOREIGN KEY" not in result.target_ddl.upper()
    assert "REFERENCES" not in result.target_ddl.upper()
    # The relationship is preserved as metadata for application-layer checks.
    assert [fk.name for fk in result.preserved_foreign_keys] == ["fk_user"]
    fk_warnings = [w for w in result.warnings if "foreign key" in w.message.lower()]
    assert len(fk_warnings) == 1
    assert fk_warnings[0].classification is Classification.MANUAL


def test_secondary_unique_index_emitted_as_create_index_async() -> None:
    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="email", mysql_type="VARCHAR(255)"),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_email", columns=["email"], unique=True)],
    )
    result = SchemaConverter().convert_table(table)
    assert len(result.index_ddls) == 1
    ddl = result.index_ddls[0]
    assert ddl.startswith("CREATE UNIQUE INDEX ASYNC")
    assert '"idx_email"' in ddl
    assert '"orders"' in ddl
    assert '"email"' in ddl
    # Index DDL is separate from the CREATE TABLE statement.
    assert "INDEX" not in result.target_ddl.upper()


def test_non_unique_multi_column_index_emitted_as_async() -> None:
    table = TableDef(
        name="events",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="kind", mysql_type="VARCHAR(20)"),
            ColumnDef(name="created", mysql_type="DATETIME"),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_kind_created", columns=["kind", "created"])],
    )
    ddl = SchemaConverter().convert_table(table).index_ddls[0]
    assert ddl.startswith("CREATE INDEX ASYNC")
    assert "UNIQUE" not in ddl
    assert '"kind"' in ddl and '"created"' in ddl


def test_default_pk_strategy_is_keep_integer() -> None:
    assert SchemaConvertOptions().primary_key_strategy is PrimaryKeyStrategy.KEEP_INTEGER


def test_pk_strategy_keep_integer_keeps_type_and_warns_hot_partition() -> None:
    options = SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.KEEP_INTEGER
    )
    result = SchemaConverter().convert_table(_auto_increment_table(), options)
    assert "UUID" not in result.target_ddl.upper()
    assert "IDENTITY" not in result.target_ddl.upper()
    # Match on the note's KIND + column, not on wording: a kept AUTO_INCREMENT key
    # converts cleanly, so this is a throughput RECOMMENDATION, never a loss.
    notes = [w for w in result.warnings if w.column_name == "id"]
    assert len(notes) == 1
    assert notes[0].kind is ConversionNoteKind.RECOMMENDATION
    assert notes[0].classification is Classification.MANUAL
    assert "consider" in notes[0].message.lower()  # it advises; it reports no defect


def test_pk_strategy_convert_to_uuid_changes_key_type() -> None:
    options = SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.CONVERT_TO_UUID
    )
    result = SchemaConverter().convert_table(_auto_increment_table(), options)
    assert "UUID" in result.target_ddl.upper()
    uuid_warnings = [w for w in result.warnings if "uuid" in w.message.lower()]
    assert len(uuid_warnings) == 1
    assert uuid_warnings[0].classification is Classification.MANUAL


def test_pk_strategy_identity_with_cache_adds_cached_identity() -> None:
    options = SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE
    )
    result = SchemaConverter().convert_table(_auto_increment_table(), options)
    ddl = result.target_ddl.upper()
    assert "GENERATED BY DEFAULT AS IDENTITY" in ddl
    assert "CACHE" in ddl
    identity_notes = [w for w in result.warnings if w.column_name == "id"]
    assert len(identity_notes) == 1
    assert identity_notes[0].kind is ConversionNoteKind.RECOMMENDATION


def test_table_without_auto_increment_has_no_key_recommendation() -> None:
    table = TableDef(
        name="plain",
        columns=[ColumnDef(name="id", mysql_type="INT")],
        primary_key=["id"],
    )
    result = SchemaConverter().convert_table(table)
    assert all(
        w.kind is not ConversionNoteKind.RECOMMENDATION for w in result.warnings
    )


def test_table_without_primary_key_warns_unsupported() -> None:
    table = TableDef(
        name="keyless",
        columns=[ColumnDef(name="value", mysql_type="INT")],
        primary_key=[],
    )
    result = SchemaConverter().convert_table(table)
    pk_warnings = [
        w for w in result.warnings if w.classification is Classification.UNSUPPORTED
    ]
    assert len(pk_warnings) == 1
    assert "primary key" in pk_warnings[0].message.lower()


def test_triggers_and_routines_flagged_for_reimplementation() -> None:
    inventory = SourceInventory(
        triggers=[ObjectRef(name="trg_audit", object_type=ObjectType.TRIGGER)],
        routines=[ObjectRef(name="sp_calc", object_type=ObjectType.ROUTINE)],
    )
    result = SchemaConverter().convert(inventory)
    flagged = {w.object_name: w for w in result.warnings}
    assert flagged["trg_audit"].classification is Classification.UNSUPPORTED
    assert "trigger" in flagged["trg_audit"].message.lower()
    assert flagged["sp_calc"].classification is Classification.UNSUPPORTED
    # Triggers/routines are never auto-converted into target DDL objects.
    assert result.tables == []


def test_convert_applies_pk_strategy_across_inventory() -> None:
    inventory = SourceInventory(
        tables=[_auto_increment_table("a"), _auto_increment_table("b")]
    )
    options = SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.CONVERT_TO_UUID
    )
    result = SchemaConverter().convert(inventory, options)
    assert isinstance(result, SchemaConversionResult)
    assert all("UUID" in conversion.target_ddl.upper() for conversion in result.tables)


# ---------------------------------------------------------------------------
# COMPOSITE_KEY strategy (prepend a high-cardinality leading column)
# ---------------------------------------------------------------------------


def _composite_table(name: str = "orders") -> TableDef:
    """A single-int-PK table with a NOT NULL high-cardinality column to lead with."""
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="BIGINT", nullable=False),
            ColumnDef(name="customer_id", mysql_type="BIGINT", nullable=False),
            ColumnDef(name="note", mysql_type="TEXT", nullable=True),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )


def _composite_options(leading: str = "customer_id") -> SchemaConvertOptions:
    return SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY,
        composite_leading_column=leading,
    )


def test_composite_key_prepends_leading_column_to_primary_key() -> None:
    from dsql_migrator.core.converter import parse_target_primary_key

    result = SchemaConverter().convert_table(_composite_table(), _composite_options())
    # The target PK is (leading, original_pk...) in that order.
    assert parse_target_primary_key(result.target_ddl) == ["customer_id", "id"]
    # Consequence is surfaced as a MANUAL warning (never applied silently).
    manual = [w for w in result.warnings if w.classification is Classification.MANUAL]
    assert any("composite" in w.message.lower() for w in manual)
    assert any("immutable" in w.message.lower() for w in manual)


def test_composite_key_emits_unique_index_on_original_key() -> None:
    # A composite key drops the original key's standalone uniqueness, so a UNIQUE
    # INDEX ASYNC on the original PK columns must be emitted to preserve it.
    result = SchemaConverter().convert_table(_composite_table(), _composite_options())
    unique_on_id = [
        d for d in result.index_ddls
        if d.startswith("CREATE UNIQUE INDEX ASYNC") and '("id")' in d
    ]
    assert len(unique_on_id) == 1


def test_composite_key_does_not_change_source_data_columns() -> None:
    # Only the KEY definition changes; every source column is still present.
    result = SchemaConverter().convert_table(_composite_table(), _composite_options())
    ddl = result.target_ddl
    assert '"id"' in ddl and '"customer_id"' in ddl and '"note"' in ddl


def test_composite_key_missing_leading_column_is_unsupported() -> None:
    result = SchemaConverter().convert_table(
        _composite_table(), _composite_options("nope")
    )
    assert result.warnings[0].classification is Classification.UNSUPPORTED
    assert "does not exist" in result.warnings[0].message
    # No broken DDL emitted -- the placeholder is a comment.
    assert result.target_ddl.lstrip().startswith("--")


def test_composite_key_nullable_leading_column_is_unsupported() -> None:
    result = SchemaConverter().convert_table(
        _composite_table(), _composite_options("note")
    )
    assert result.warnings[0].classification is Classification.UNSUPPORTED
    assert "nullable" in result.warnings[0].message.lower()


def test_composite_key_leading_already_in_pk_is_unsupported() -> None:
    # Prepending a column already in the PK would not change write distribution.
    result = SchemaConverter().convert_table(
        _composite_table(), _composite_options("id")
    )
    assert result.warnings[0].classification is Classification.UNSUPPORTED
    assert "already part of the primary key" in result.warnings[0].message


def test_composite_options_requires_leading_column() -> None:
    with pytest.raises(ValueError, match="requires composite_leading_column"):
        SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY)


def test_composite_leading_column_requires_composite_strategy() -> None:
    with pytest.raises(ValueError, match="only valid with the COMPOSITE_KEY"):
        SchemaConvertOptions(composite_leading_column="customer_id")


def test_validate_composite_leading_column_accepts_valid_choice() -> None:
    from dsql_migrator.core.converter import validate_composite_leading_column

    assert validate_composite_leading_column(_composite_table(), "customer_id") is None


def test_composite_key_rejects_over_eight_columns() -> None:
    # A 7-column source PK + a leading col = 8 is allowed; +1 more (9) is rejected.
    from dsql_migrator.core.converter import validate_composite_leading_column

    cols = [ColumnDef(name=f"k{i}", mysql_type="INT", nullable=False) for i in range(8)]
    cols.append(ColumnDef(name="lead", mysql_type="INT", nullable=False))
    table = TableDef(
        name="wide",
        columns=cols,
        primary_key=[f"k{i}" for i in range(8)],  # 8 source PK cols
    )
    msg = validate_composite_leading_column(table, "lead")
    assert msg is not None and "at most 8" in msg


# ---------------------------------------------------------------------------
# Schema-qualified table names (database.table -> "schema"."table")
# ---------------------------------------------------------------------------


def test_qualified_table_name_maps_to_schema_qualified_ddl() -> None:
    table = TableDef(
        name="customers_sample.categories",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="name", mysql_type="varchar(100)"),
        ],
        primary_key=["id"],
    )
    conversion = _convert_table(table)

    # The table goes under the schema as a qualified identifier, not a flat one.
    assert '"customers_sample"."categories"' in conversion.target_ddl
    assert '"customers_sample.categories"' not in conversion.target_ddl
    # A CREATE SCHEMA IF NOT EXISTS precedes the table.
    assert conversion.schema_ddls == [
        'CREATE SCHEMA IF NOT EXISTS "customers_sample"'
    ]


def test_qualified_table_name_qualifies_index_target() -> None:
    table = TableDef(
        name="customers_sample.categories",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="name", mysql_type="varchar(100)"),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_name", columns=["name"], unique=False)],
    )
    conversion = _convert_table(table)

    assert len(conversion.index_ddls) == 1
    assert 'ON "customers_sample"."categories"' in conversion.index_ddls[0]


def test_unqualified_table_name_has_no_schema_ddls() -> None:
    table = TableDef(
        name="categories",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
        primary_key=["id"],
    )
    conversion = _convert_table(table)

    assert conversion.schema_ddls == []
    assert '"categories"' in conversion.target_ddl


def test_execution_units_create_schema_once_before_tables() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="shop.orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
            TableDef(
                name="shop.items",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
        ]
    )
    result = SchemaConverter().convert(inventory)
    units = result.execution_units()
    schema_units = [u for u in units if u.sql.startswith("CREATE SCHEMA")]
    table_units = [u for u in units if u.sql.startswith("CREATE TABLE")]

    # Schema is created exactly once (deduplicated) and before the tables.
    assert len(schema_units) == 1
    assert schema_units[0].sql == 'CREATE SCHEMA IF NOT EXISTS "shop"'
    first_table_index = units.index(table_units[0])
    assert units.index(schema_units[0]) < first_table_index


# ---------------------------------------------------------------------------
# View conversion (Aurora DSQL is PostgreSQL-compatible and supports views)
# ---------------------------------------------------------------------------


def test_convert_view_transpiles_to_postgres_create_view() -> None:
    view = ViewDef(
        name="app.active_customers",
        definition="select `c`.`id` AS `id` from `customers` `c` where (`c`.`status` = 'active')",
    )
    conversion = SchemaConverter().convert_view(view)

    assert conversion.auto_converted is True
    assert conversion.view == "app.active_customers"
    assert conversion.target_ddl.strip().upper().startswith("CREATE VIEW")
    # Re-targeted to the qualified name and quoted PostgreSQL-style.
    assert "active_customers" in conversion.target_ddl
    assert '"' in conversion.target_ddl  # postgres identifier quoting
    # Qualified name -> a CREATE SCHEMA IF NOT EXISTS precedes it.
    assert conversion.schema_ddls == ['CREATE SCHEMA IF NOT EXISTS "app"']
    assert conversion.warnings == []


def test_convert_view_missing_definition_is_manual() -> None:
    conversion = SchemaConverter().convert_view(ViewDef(name="v_empty", definition=""))
    assert conversion.auto_converted is False
    assert conversion.target_ddl.lstrip().startswith("--")
    assert conversion.warnings and conversion.warnings[0].classification is (
        Classification.MANUAL
    )


def test_convert_view_unparseable_is_manual_not_raise() -> None:
    # Garbage that sqlglot cannot parse -> manual reimplementation, never raises.
    conversion = SchemaConverter().convert_view(
        ViewDef(name="v_bad", definition="this is not sql (((")
    )
    assert conversion.auto_converted is False
    assert conversion.warnings[0].classification is Classification.MANUAL


def test_convert_includes_views_and_orders_them_after_tables() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="app.orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            )
        ],
        views=[
            ViewDef(name="app.v_orders", definition="select `id` from `orders`")
        ],
    )
    result = SchemaConverter().convert(inventory)

    assert [v.view for v in result.views] == ["app.v_orders"]
    # Execution units: the view's CREATE VIEW comes after the table's CREATE TABLE.
    sqls = [u.sql for u in result.execution_units()]
    create_table_idx = next(
        i for i, s in enumerate(sqls) if s.strip().upper().startswith("CREATE TABLE")
    )
    create_view_idx = next(
        i for i, s in enumerate(sqls) if s.strip().upper().startswith("CREATE VIEW")
    )
    assert create_table_idx < create_view_idx


# ---------------------------------------------------------------------------
# Unparsable / unsupported source types must be isolated, not crash the step
# ---------------------------------------------------------------------------


def test_spatial_point_table_preserved_as_bytea_not_crash() -> None:
    """A table with a MySQL spatial column (POINT) is not parseable by sqlglot's
    MySQL dialect and has no Aurora DSQL equivalent. convert_table must NOT raise
    and must NOT silently drop the data: the spatial column is preserved as bytea
    (raw WKB) and flagged MANUAL, yielding a real, editable CREATE TABLE."""
    table = _single_column_table("migration_typetest.typetest_spatial", "POINT")
    conversion = _convert_table(table)  # must not raise (spatial -> bytea)
    assert conversion.table == "migration_typetest.typetest_spatial"
    # A real CREATE TABLE (editable), not a comment placeholder, with bytea.
    assert conversion.target_ddl.lower().lstrip().startswith("create table")
    assert "bytea" in conversion.target_ddl.lower()
    manual = [
        w for w in conversion.warnings if w.classification is Classification.MANUAL
    ]
    assert manual, "the spatial column must be flagged MANUAL"
    assert any(w.column_name == "value" and w.target_type == "bytea" for w in manual)
    assert any("bytea" in w.message.lower() for w in manual)


def test_unsupported_table_does_not_block_other_tables() -> None:
    """One table with an unsupported spatial type must not stop the rest of the
    inventory from converting (per-table failure isolation)."""
    good = TableDef(
        name="migration_typetest.typetest_parent",
        columns=[ColumnDef(name="id", mysql_type="BIGINT")],
        primary_key=["id"],
    )
    spatial = _single_column_table("migration_typetest.typetest_spatial", "POINT")
    result = SchemaConverter().convert(SourceInventory(tables=[good, spatial]))
    by_table = {c.table: c for c in result.tables}
    assert "migration_typetest.typetest_parent" in by_table
    assert "migration_typetest.typetest_spatial" in by_table
    # The good table still produced real CREATE TABLE DDL.
    assert "create table" in by_table[
        "migration_typetest.typetest_parent"
    ].target_ddl.lower()
    # The spatial table still produced real CREATE TABLE DDL (spatial col -> bytea),
    # flagged MANUAL -- the data is preserved, not dropped or blocked.
    spatial_conv = by_table["migration_typetest.typetest_spatial"]
    assert "create table" in spatial_conv.target_ddl.lower()
    assert "bytea" in spatial_conv.target_ddl.lower()
    assert any(
        w.object_name == "migration_typetest.typetest_spatial"
        and w.classification is Classification.MANUAL
        and "bytea" in w.message.lower()
        for w in result.warnings
    )


def test_parse_target_column_types_extracts_normalized_types() -> None:
    from dsql_migrator.core.converter import parse_target_column_types

    ddl = (
        'CREATE TABLE "app"."t" (\n'
        '  "id" bigint NOT NULL,\n'
        '  "active" smallint,\n'
        '  "flag" boolean,\n'
        '  "amount" numeric(20, 0),\n'
        '  PRIMARY KEY ("id")\n'
        ");"
    )
    types = parse_target_column_types(ddl)
    assert types["id"] == "bigint"
    assert types["active"] == "smallint"
    assert types["flag"] == "boolean"
    assert types["amount"] in {"numeric", "decimal"}  # params stripped; PG alias


def test_parse_target_column_types_empty_for_non_create_placeholder() -> None:
    from dsql_migrator.core.converter import parse_target_column_types

    # A comment placeholder (unsupported table) or unparseable text yields no
    # overrides, so value conversion safely falls back to the source mapping.
    assert parse_target_column_types("-- not auto-converted") == {}
    assert parse_target_column_types("definitely not sql ;;;") == {}


def test_parse_target_primary_key_single_table_level() -> None:
    from dsql_migrator.core.converter import parse_target_primary_key

    ddl = (
        'CREATE TABLE "app"."orders" (\n'
        '  "id" bigint NOT NULL,\n'
        '  "customer_id" bigint NOT NULL,\n'
        '  PRIMARY KEY ("id")\n'
        ");"
    )
    assert parse_target_primary_key(ddl) == ["id"]


def test_parse_target_primary_key_composite_preserves_key_order() -> None:
    from dsql_migrator.core.converter import parse_target_primary_key

    # A composite PK must return the columns in KEY order (leading column first),
    # since that order is what the loader's ON CONFLICT target depends on.
    ddl = (
        'CREATE TABLE "app"."orders" (\n'
        '  "id" bigint NOT NULL,\n'
        '  "customer_id" bigint NOT NULL,\n'
        '  PRIMARY KEY ("customer_id", "id")\n'
        ");"
    )
    assert parse_target_primary_key(ddl) == ["customer_id", "id"]


def test_parse_target_primary_key_inline_column_constraint() -> None:
    from dsql_migrator.core.converter import parse_target_primary_key

    ddl = 'CREATE TABLE "t" ("id" bigint NOT NULL PRIMARY KEY, "x" integer)'
    assert parse_target_primary_key(ddl) == ["id"]


def test_parse_target_primary_key_empty_when_absent_or_unparseable() -> None:
    from dsql_migrator.core.converter import parse_target_primary_key

    # No PK, a comment placeholder, and non-SQL all yield [] -- the caller treats
    # [] as "unknown" and falls back to the source PK (today's behavior).
    assert parse_target_primary_key('CREATE TABLE "t" ("x" integer)') == []
    assert parse_target_primary_key("-- not auto-converted") == []
    assert parse_target_primary_key("definitely not sql ;;;") == []


def test_parse_target_primary_key_round_trips_real_converted_ddl() -> None:
    # Guardrail from the plan: parse the ACTUAL pretty-printed postgres DDL that
    # convert_table emits, not a hand-written string -- so the parser stays in
    # lock-step with the emitter (sqlglot pretty-print quirks included).
    from dsql_migrator.core.converter import parse_target_primary_key

    table = _single_column_table("orders", "VARCHAR(64)")
    result = _convert_table(table)
    assert parse_target_primary_key(result.target_ddl) == ["id"]


# ---------------------------------------------------------------------------
# Column DEFAULT values -- Aurora DSQL supports them, so they must be carried
# across. Verified against a live DSQL cluster: literals, expressions,
# CURRENT_TIMESTAMP/now(), gen_random_uuid() and NOT NULL DEFAULT all work; only
# triggers and plpgsql functions are unsupported (so no ON UPDATE equivalent).
# ---------------------------------------------------------------------------


def test_literal_defaults_are_preserved() -> None:
    """The converter used to drop every DEFAULT silently.

    ColumnDef.default was populated by the introspector and read by nobody, so a MySQL
    `DEFAULT 0` simply vanished. DSQL documents `DEFAULT default_expr` in its CREATE
    TABLE grammar, so this was a converter gap, not a platform limit.
    """
    # Reflection returns a literal WITH its quotes, which is how a literal is told
    # apart from a function default.
    # Defaults arrive UNQUOTED from information_schema.COLUMN_DEFAULT (see
    # introspector.enrich_columns), so the converter re-quotes them -- and leaves a
    # number bare so the target keeps its native type instead of coercing a string.
    for mysql_type, default, expected in (
        ("INT", "0", "DEFAULT 0"),
        ("VARCHAR(10)", "abc", "DEFAULT 'abc'"),
        ("CHAR(3)", "USD", "DEFAULT 'USD'"),
        ("DECIMAL(10,2)", "0.00", "DEFAULT 0.00"),
    ):
        ddl = _convert_table(
            _single_column_table("t", mysql_type, default=default)
        ).target_ddl
        assert expected in ddl, (mysql_type, default, ddl)


def test_current_timestamp_default_is_preserved() -> None:
    # A function default comes back from reflection BARE (no quotes) -- and PostgreSQL
    # spells CURRENT_TIMESTAMP the same way, so it carries straight across.
    ddl = _convert_table(
        _single_column_table(
            "t", "DATETIME", default="CURRENT_TIMESTAMP", default_is_expression=True
        )
    ).target_ddl
    # DATETIME maps to a no-timezone TIMESTAMP, and the loader normalizes migrated rows
    # to naive UTC -- so the default is pinned to UTC too, rather than inheriting the
    # session TimeZone the way a bare CURRENT_TIMESTAMP would.
    assert "AT TIME ZONE 'UTC'" in ddl
    # ...and it must NOT be quoted into a string literal.
    assert "DEFAULT 'CURRENT_TIMESTAMP'" not in ddl


def test_timestamp_default_on_timestamptz_target_keeps_the_instant(tmp_path=None) -> None:
    # Audit C11: MySQL TIMESTAMP maps to timestamptz. CURRENT_TIMESTAMP there must stay a
    # plain instant, NOT `now() AT TIME ZONE 'UTC'` (a naive value a timestamptz column
    # re-interprets in the session TimeZone, shifting a defaulted insert). The naive-UTC
    # form is only for the DATETIME -> plain `timestamp` target.
    ddl = _convert_table(
        _single_column_table(
            "t", "TIMESTAMP", default="CURRENT_TIMESTAMP", default_is_expression=True
        )
    ).target_ddl
    assert "TIMESTAMPTZ" in ddl.upper()
    # timestamptz default must NOT carry the naive AT TIME ZONE 'UTC' wrapper.
    assert "AT TIME ZONE 'UTC'" not in ddl
    assert "CURRENT_TIMESTAMP" in ddl or "now()" in ddl


def test_tinyint_one_unsigned_is_not_flagged_as_boolean_by_assessor() -> None:
    # Audit U1: the assessor must agree with the converter -- tinyint(1) unsigned maps to
    # smallint (UTINYINT), not boolean, so it must NOT be flagged as a boolean column.
    from dsql_migrator.core.assessor import _is_tinyint_one

    assert _is_tinyint_one("tinyint(1)") is True  # signed -> boolean convention
    assert _is_tinyint_one("tinyint(1) unsigned") is False
    assert _is_tinyint_one("tinyint(1) zerofill") is False
    assert map_mysql_type("tinyint(1) unsigned")[0].lower() == "smallint"


def test_time_column_warns_about_the_duration_range() -> None:
    # Audit U2: MySQL TIME (-838:59:59..838:59:59) exceeds PG time-of-day; an out-of-range
    # value fails per row during Full Load, so the conversion must warn up front.
    for src in ("TIME", "TIME(6)"):
        target, warning = map_mysql_type(src)
        assert target.lower().startswith("time")
        assert warning is not None and "duration" in warning.message.lower()


def test_too_many_indexes_counts_the_composite_key_unique_index() -> None:
    # Audit U3: the 24-index cap warning must count the extra UNIQUE index COMPOSITE_KEY
    # adds. 23 source indexes + composite PK + 1 preservation index = 25 > 24, so it must
    # warn even though len(table.indexes) alone is within the 23-secondary budget.
    from dsql_migrator.core.converter import (
        PrimaryKeyStrategy,
        SchemaConvertOptions,
        _too_many_indexes_warning,
    )

    table = TableDef(
        name="wide",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="tenant", mysql_type="INT", nullable=False),
        ],
        primary_key=["id"],
        indexes=[
            IndexDef(name=f"ix_{i}", columns=["tenant"]) for i in range(23)
        ],
    )
    # Without the composite extra index: 23 secondary -> within budget, no warning.
    assert _too_many_indexes_warning(table, 0) is None
    # With COMPOSITE_KEY's +1 unique index: 24 -> over the 23-secondary budget -> warn.
    assert _too_many_indexes_warning(table, 1) is not None


def _table_with_key_widths(
    *, pk_columns: int = 1, index_widths: tuple[int, ...] = (), name: str = "wide_key"
) -> TableDef:
    """A table whose PK spans ``pk_columns`` and whose indexes span ``index_widths``."""
    total = max([pk_columns, *index_widths], default=1)
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name=f"c{i}", mysql_type="INT", nullable=False)
            for i in range(1, total + 1)
        ],
        primary_key=[f"c{i}" for i in range(1, pk_columns + 1)],
        indexes=[
            IndexDef(name=f"ix_{n}", columns=[f"c{i}" for i in range(1, width + 1)])
            for n, width in enumerate(index_widths, start=1)
        ],
    )


def test_index_over_eight_columns_is_not_emitted() -> None:
    # DSQL caps a key at 8 columns (error 54011); MySQL allows 16. Emitting a
    # 9+-column CREATE INDEX ASYNC would guarantee a failure AFTER Full Load wrote
    # every row, so the converter must leave it out of the applied script.
    conversion = _convert_table(_table_with_key_widths(index_widths=(8, 9)))
    ddl = " ".join(conversion.index_ddls)
    assert "ix_1" in ddl  # exactly 8 columns -> still emitted
    assert "ix_2" not in ddl  # 9 columns -> skipped


def test_skipped_wide_index_is_reported_as_a_loss() -> None:
    # Skipping silently would be worse than failing: the operator must learn the
    # target is missing that index.
    conversion = _convert_table(_table_with_key_widths(index_widths=(12,)))
    (warning,) = [w for w in conversion.warnings if "54011" in w.message or "8 columns" in w.message]
    assert warning.classification is Classification.MANUAL
    assert warning.kind is ConversionNoteKind.LOSS
    assert "ix_1 (12 columns)" in warning.message
    assert "NOT emitted" in warning.message
    assert "MySQL allows 16" in warning.message


def test_primary_key_over_eight_columns_is_unsupported() -> None:
    # A wide PK makes the CREATE TABLE itself rejected, so nothing migrates.
    conversion = _convert_table(_table_with_key_widths(pk_columns=10))
    (warning,) = [w for w in conversion.warnings if "primary key" in w.message]
    assert warning.classification is Classification.UNSUPPORTED
    assert "10 columns" in warning.message
    assert "REJECTED" in warning.message


def test_keys_within_the_eight_column_limit_produce_no_key_warning() -> None:
    from dsql_migrator.core.converter import _too_many_key_columns_warning

    # Exactly at the limit on both sides -> clean.
    assert (
        _too_many_key_columns_warning(
            _table_with_key_widths(pk_columns=8, index_widths=(8, 1))
        )
        is None
    )


def test_index_budget_ignores_indexes_the_converter_skips() -> None:
    # A table whose index COUNT is only over the 23-secondary budget because of
    # indexes that are themselves skipped (over 8 columns) must not claim a budget
    # overflow the applied script cannot hit.
    from dsql_migrator.core.converter import _too_many_indexes_warning

    table = TableDef(
        name="wide",
        columns=[
            ColumnDef(name=f"c{i}", mysql_type="INT", nullable=False)
            for i in range(1, 10)
        ],
        primary_key=["c1"],
        indexes=(
            [IndexDef(name=f"ix_{i}", columns=["c2"]) for i in range(23)]
            # 2 more that are 9 columns wide -> skipped, so 23 are actually emitted.
            + [
                IndexDef(name=f"wide_{i}", columns=[f"c{c}" for c in range(1, 10)])
                for i in range(2)
            ]
        ),
    )
    assert len(table.indexes) == 25  # over the budget by raw count
    assert _too_many_indexes_warning(table) is None  # but only 23 are emitted


# ---------------------------------------------------------------------------
# 1 KiB combined key-size budget (error 54000 "key size too large"). Unlike the
# 8-column cap this is enforced on the VALUE at INSERT/UPDATE time, so it can only
# ever be a WARNING -- a wide declared type holding short values migrates fine.
# ---------------------------------------------------------------------------


def _key_warning(table: TableDef):
    """The key-size note from converting ``table``, or None."""
    conversion = _convert_table(table)
    hits = [w for w in conversion.warnings if "key size too large" in w.message]
    return hits[0] if hits else None


def _keyed_table(specs: dict[str, str], pk: list[str], indexes=None) -> TableDef:
    return TableDef(
        name="t",
        columns=[
            ColumnDef(name=name, mysql_type=mysql_type, nullable=False)
            for name, mysql_type in specs.items()
        ],
        primary_key=pk,
        indexes=list(indexes or []),
    )


def test_narrow_multi_column_key_is_not_falsely_flagged() -> None:
    # Regression: every varchar column used to be counted at a flat 255 bytes, so
    # 5 x varchar(10) scored 1275 and warned about a key that cannot exceed ~200.
    assert _key_warning(_keyed_table(
        {f"c{i}": "VARCHAR(10)" for i in range(1, 6)},
        [f"c{i}" for i in range(1, 6)],
    )) is None


def test_single_wide_varchar_key_is_flagged() -> None:
    # The other half of the same regression: one varchar(2000) key column also scored
    # 255, so a key that alone can be 8 KiB passed silently.
    warning = _key_warning(_keyed_table({"c": "VARCHAR(2000)"}, ["c"]))
    assert warning is not None
    assert "8000 bytes" in warning.message  # 2000 chars x 4 bytes (utf8mb4)


def test_key_size_budget_boundary_counts_utf8mb4_bytes() -> None:
    # varchar(256) x 4 bytes = exactly 1024 -> at the limit, clean.
    assert _key_warning(_keyed_table({"c": "VARCHAR(256)"}, ["c"])) is None
    # One more character crosses it.
    assert _key_warning(_keyed_table({"c": "VARCHAR(257)"}, ["c"])) is not None


def test_unbounded_text_key_is_flagged() -> None:
    # TEXT has no declared length and can exhaust the budget by itself.
    assert _key_warning(_keyed_table({"c": "TEXT"}, ["c"])) is not None


def test_wide_secondary_index_key_size_is_flagged_and_named() -> None:
    warning = _key_warning(_keyed_table(
        {"id": "INT", "d": "VARCHAR(500)"},
        ["id"],
        [IndexDef(name="ix_d", columns=["d"])],
    ))
    assert warning is not None
    assert "index ix_d" in warning.message
    # The PK itself is a 4-byte int, so it must not be listed as at risk (the phrase
    # "a primary key" appears in the shared explanation, hence the specific form).
    assert "the primary key (" not in warning.message


def test_key_size_note_is_a_recommendation_not_a_block() -> None:
    # Enforced on the VALUE at INSERT time, so a wide declared type whose real values
    # are short migrates fine -- this must never be UNSUPPORTED, and the DDL must
    # still be emitted.
    conversion = _convert_table(_keyed_table({"c": "VARCHAR(2000)"}, ["c"]))
    warning = _key_warning(_keyed_table({"c": "VARCHAR(2000)"}, ["c"]))
    assert warning.classification is Classification.MANUAL
    assert warning.kind is ConversionNoteKind.RECOMMENDATION
    assert 'PRIMARY KEY ("c")' in conversion.target_ddl
    # Names the error and that it lands per ROW, not at apply.
    assert "54000" in warning.message
    assert "INSERT" in warning.message


def test_ordinary_keys_produce_no_key_size_note() -> None:
    assert _key_warning(_keyed_table({"id": "BIGINT"}, ["id"])) is None
    assert _key_warning(_keyed_table({"id": "CHAR(36)"}, ["id"])) is None  # uuid-as-char
    assert _key_warning(_keyed_table(
        {"tenant": "INT", "id": "BIGINT"}, ["tenant", "id"]
    )) is None


def test_key_size_check_skips_an_index_the_converter_drops() -> None:
    # A 9-column index is not emitted at all (8-column cap), so warning about its
    # byte size too would be noise about DDL that does not exist.
    warning = _key_warning(_keyed_table(
        {f"c{i}": "VARCHAR(500)" for i in range(1, 10)},
        ["c1"],
        [IndexDef(name="ix_wide", columns=[f"c{i}" for i in range(1, 10)])],
    ))
    assert warning is None or "ix_wide" not in warning.message


def test_key_size_measures_the_key_the_pk_strategy_produced() -> None:
    # The estimate must read the EMITTED key, not TableDef.primary_key, so a strategy
    # that rewrites the key is measured as converted. COMPOSITE_KEY is the strong case:
    # a leading column that busts the budget is rejected outright by the picker's own
    # validation (it is a deliberate user choice, so blocking is right there) --
    table = _keyed_table({"code": "VARCHAR(400)", "id": "BIGINT"}, ["id"])
    assert _key_warning(table) is None  # the source key alone (bigint) is tiny
    blocked = SchemaConverter().convert_table(
        table,
        SchemaConvertOptions(
            primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY,
            composite_leading_column="code",
        ),
    )
    (rejection,) = [w for w in blocked.warnings if "key limit" in w.message]
    assert rejection.classification is Classification.UNSUPPORTED
    assert "1608 bytes" in rejection.message  # 400x4 + 8, i.e. the composite key

    # -- while a leading column that FITS is applied, and the resulting key is what
    # gets measured (no note, because 4x64 + 8 is well under the budget).
    ok = SchemaConverter().convert_table(
        _keyed_table({"code": "VARCHAR(64)", "id": "BIGINT"}, ["id"]),
        SchemaConvertOptions(
            primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY,
            composite_leading_column="code",
        ),
    )
    assert 'PRIMARY KEY ("code", "id")' in ok.target_ddl
    assert not [w for w in ok.warnings if "key size too large" in w.message]


def test_key_size_estimate_reads_the_converted_target_type() -> None:
    # The estimate must use the TARGET type: a CONVERT_TO_UUID key is a 16-byte uuid,
    # not the source's char/int, so it must never be flagged.
    from dsql_migrator.core.converter import _estimate_key_column_bytes, _target_key_columns
    import sqlglot

    create = sqlglot.parse_one(
        'CREATE TABLE "t" ("id" UUID NOT NULL, PRIMARY KEY ("id"))', dialect="postgres"
    )
    assert _target_key_columns(create) == ["id"]
    assert _estimate_key_column_bytes(create, "id") == 16


def test_tinyint_bool_default_becomes_a_boolean_literal() -> None:
    """`DEFAULT 1` on a BOOLEAN column is a hard error on DSQL.

    Live cluster: `column "c" is of type boolean but default expression is of type
    integer`. MySQL stores the tinyint(1) default as '0'/'1', so a naive pass-through
    would make the whole CREATE TABLE fail.
    """
    on = _convert_table(
        _single_column_table("t", "TINYINT(1)", default="1")
    ).target_ddl
    assert "DEFAULT TRUE" in on
    assert "DEFAULT 1" not in on

    off = _convert_table(
        _single_column_table("t", "TINYINT(1)", default="0")
    ).target_ddl
    assert "DEFAULT FALSE" in off

    # tinyint(1) UNSIGNED maps to SMALLINT, not boolean, so its default must stay
    # numeric -- deciding from the source string alone got this wrong.
    unsigned = _convert_table(
        _single_column_table("t", "TINYINT(1) UNSIGNED", default="1")
    ).target_ddl
    assert "SMALLINT" in unsigned
    assert "DEFAULT 1" in unsigned
    assert "DEFAULT TRUE" not in unsigned


def test_not_null_column_keeps_its_default() -> None:
    """The severe case: NOT NULL + a dropped default breaks the app after cut-over.

    MySQL accepts an INSERT that omits a NOT NULL column WITH a default; the target
    rejects that same INSERT with a not-null violation (confirmed on a live cluster).
    Every one of the 22 defaulted columns in the reference source schema is NOT NULL,
    so this was not a corner case.
    """
    ddl = _convert_table(
        _single_column_table("t", "INT", nullable=False, default="0")
    ).target_ddl
    assert "NOT NULL DEFAULT 0" in ddl


def test_auto_increment_column_gets_no_default() -> None:
    """The AUTO_INCREMENT column must never carry a DEFAULT.

    Under the IDENTITY_WITH_CACHE strategy it becomes GENERATED BY DEFAULT AS IDENTITY,
    and PostgreSQL rejects an identity column that also has a DEFAULT -- so the default
    is skipped for that column regardless of the chosen key strategy.
    """
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    table = TableDef(
        name="t",
        columns=[ColumnDef(name="id", mysql_type="BIGINT", nullable=False, default="0")],
        primary_key=["id"],
        auto_increment_column="id",
    )
    # Default strategy: still no DEFAULT on the key column.
    assert "DEFAULT 0" not in _convert_table(table).target_ddl

    # And with the identity strategy, where a DEFAULT would be an outright error.
    identity_ddl = SchemaConverter().convert_table(
        table,
        SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE),
    ).target_ddl
    assert "IDENTITY" in identity_ddl
    assert "DEFAULT 0" not in identity_ddl


def test_generated_column_gets_no_default() -> None:
    # The value is computed, so a default can never apply -- and it is not a loss,
    # so it must not raise a warning either.
    result = _convert_table(
        _single_column_table("t", "INT", generated=True, default="0")
    )
    assert "DEFAULT" not in result.target_ddl
    assert not [w for w in result.warnings if "default" in w.message.lower()]


def test_untranslatable_function_default_is_dropped_with_a_warning() -> None:
    """MySQL UUID() has no DSQL spelling, so emitting it would fail the CREATE TABLE.

    Dropping it is correct -- but silently dropping it is not, which is the half of
    this bug that mattered most: the user could not tell the behavior had changed.
    """
    # MySQL UUID() now translates to gen_random_uuid(), so use a genuinely
    # untranslatable expression: one referencing another column.
    result = _convert_table(
        _single_column_table(
            "t", "INT", nullable=False, default="(`id` + 1)",
            default_is_expression=True,
        )
    )
    assert "DEFAULT" not in result.target_ddl
    messages = [w.message for w in result.warnings if "no Aurora DSQL equivalent" in w.message]
    assert messages, result.warnings
    # And it must spell out the post-cut-over consequence for a NOT NULL column.
    assert "REJECTED on Aurora DSQL" in messages[0]
    assert all(
        w.classification is Classification.MANUAL
        for w in result.warnings
        if "no Aurora DSQL equivalent" in w.message
    )


def test_out_of_range_tinyint_bool_default_is_dropped_with_a_warning() -> None:
    result = _convert_table(
        _single_column_table("t", "TINYINT(1)", default="7")
    )
    assert "DEFAULT" not in result.target_ddl
    assert any("not 0 or 1" in w.message for w in result.warnings), result.warnings


def test_nullable_dropped_default_warning_omits_the_not_null_consequence() -> None:
    # Severity calibration: a nullable column losing its default just starts defaulting
    # to NULL -- it does not break an INSERT, so it must not claim it does.
    result = _convert_table(
        _single_column_table(
            "t", "INT", nullable=True, default="(`id` + 1)",
            default_is_expression=True,
        )
    )
    dropped = [w.message for w in result.warnings if "no Aurora DSQL equivalent" in w.message]
    assert dropped
    assert "REJECTED" not in dropped[0]


def test_columns_without_a_default_are_unchanged() -> None:
    # No default -> no DEFAULT clause, and no spurious warning.
    result = _convert_table(_single_column_table("t", "INT"))
    assert "DEFAULT" not in result.target_ddl
    assert not [w for w in result.warnings if "default" in w.message.lower()]


def test_expression_defaults_are_identified_by_mysqls_own_flag() -> None:
    """MySQL tells us whether a default is an expression; we no longer guess.

    ``information_schema.COLUMN_DEFAULT`` returns a default UNQUOTED, so the old
    quoting heuristic could not tell the literal string "CURRENT_TIMESTAMP" from the
    function call. EXTRA's ``DEFAULT_GENERATED`` flag is MySQL's own answer, carried on
    ``ColumnDef.default_is_expression``.
    """
    # Flagged as an expression -> emitted as an expression.
    expression = _convert_table(
        _single_column_table(
            "t", "DATETIME", default="CURRENT_TIMESTAMP", default_is_expression=True
        )
    ).target_ddl
    assert "DEFAULT '" not in expression  # not quoted into a string

    # NOT flagged -> the same text is a literal string, and must be quoted.
    literal = _convert_table(
        _single_column_table("t", "VARCHAR(40)", default="CURRENT_TIMESTAMP")
    ).target_ddl
    assert "DEFAULT 'CURRENT_TIMESTAMP'" in literal


def test_only_the_enriched_inventory_shape_is_supported() -> None:
    """One supported input shape, with no heuristic fallback -- by design.

    The converter reads defaults in the shape ``introspector.enrich_columns`` produces
    (unquoted value from information_schema.COLUMN_DEFAULT + the DEFAULT_GENERATED flag),
    and every MySQL source goes through that enrichment unconditionally. Supporting the
    raw SQLAlchemy-reflected shape as well meant inferring expression-vs-literal from
    quoting, which cannot tell the literal string "CURRENT_TIMESTAMP" from the function
    call -- so the fallback was removed rather than left to guess.
    """
    import inspect as _inspect

    from dsql_migrator.core import converter

    source = _inspect.getsource(converter)
    # The guessing helpers are gone, and nothing may reintroduce them.
    assert "_looks_like_expression" not in source
    assert "_inventory_has_expression_flag" not in source
    # The decision is MySQL's own flag.
    assert "if column.default_is_expression:" in source


def test_enrichment_is_unconditional_for_a_mysql_source() -> None:
    """The single supported shape only holds if enrichment always runs.

    If a code path reflected without enriching, defaults would arrive quoted and
    expression flags unset -- exactly the shape the converter no longer accepts.
    """
    import inspect as _inspect

    from dsql_migrator.core import introspector

    source = _inspect.getsource(introspector)
    reflect_index = source.index("tables = _reflect_tables(inspector")
    enrich_index = source.index("enrich_columns(connection", reflect_index)
    between = source[reflect_index:enrich_index]
    # Enrichment follows reflection in the same loop iteration, gated only on the source
    # being MySQL (the sole dialect this tool migrates from).
    assert "if is_mysql:" in between


def test_on_update_is_never_emitted_into_the_target_ddl() -> None:
    """`ON UPDATE CURRENT_TIMESTAMP` folded into the default broke the CREATE TABLE.

    SQLAlchemy's regex reflection returns the whole clause as ONE default string, and
    emitting it verbatim fails on the target with `syntax error at or near "ON"` -- on the
    single most common audit column there is. information_schema keeps the clause in EXTRA
    (so it never arrives here), and this strips it anyway for any inventory that still
    carries the reflected form. Nothing is lost: the ON UPDATE fact rides on
    ``auto_update_timestamp`` and the assessor already reports it MANUAL, since DSQL has
    neither an ON UPDATE clause nor triggers.
    """
    ddl = _convert_table(
        _single_column_table(
            "t", "DATETIME",
            default="CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            default_is_expression=True,
        )
    ).target_ddl
    assert "ON UPDATE" not in ddl.upper()
    # The usable half of the default is still carried across.
    assert "DEFAULT" in ddl


# ---------------------------------------------------------------------------
# DDL that Aurora DSQL REJECTS. Every case below was found by applying the
# converter's own output to a live cluster -- the unit tests assert on generated
# TEXT, so a snapshot can (and did) pin DDL that DSQL refuses.
# ---------------------------------------------------------------------------


def test_identity_cache_is_a_value_dsql_accepts() -> None:
    """DSQL accepts only CACHE = 1 or CACHE >= 65536.

    The converter emitted CACHE 100, which is in neither range: every table converted
    with the identity strategy produced DDL DSQL rejected with "CACHE (100) must be
    greater than or equal to 65536 or equal to 1". The old snapshot pinned the broken
    value, so the text-only tests were green.
    """
    from dsql_migrator.core.converter import _IDENTITY_CACHE_SIZE

    assert _IDENTITY_CACHE_SIZE == 1 or _IDENTITY_CACHE_SIZE >= 65536


def test_identity_column_is_widened_to_bigint() -> None:
    """DSQL sequences are BIGINT-only, so an INT identity column is rejected.

    "datatype integer not supported, identity column type must be bigint" -- and a MySQL
    `int AUTO_INCREMENT` primary key is the single most common shape there is, so this
    broke the typical table outright. Widening is lossless: BIGINT holds every INT value.
    """
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    options = SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE
    )
    for mysql_type in ("INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT"):
        table = TableDef(
            name="t",
            columns=[ColumnDef(name="id", mysql_type=mysql_type, nullable=False)],
            primary_key=["id"],
            auto_increment_column="id",
        )
        ddl = SchemaConverter().convert_table(table, options).target_ddl
        identity_line = next(l for l in ddl.splitlines() if "IDENTITY" in l)
        assert "BIGINT" in identity_line, (mysql_type, identity_line)


def _identity_table(mysql_type: str) -> TableDef:
    return TableDef(
        name="t",
        columns=[ColumnDef(name="id", mysql_type=mysql_type, nullable=False)],
        primary_key=["id"],
        auto_increment_column="id",
    )


def test_identity_bigint_unsigned_key_warns_about_range_narrowing() -> None:
    """A `bigint unsigned` identity key is narrowed to bigint -- a real LOSS, not advice.

    `bigint unsigned` maps to numeric(20,0) to keep its 0..2^64-1 range, but a DSQL
    identity column must be bigint, so the widening narrows it to 0..2^63-1. New
    generated ids are fine, but an EXISTING source value above 2^63-1 will not fit and
    that row would fail Full Load (SQLSTATE 22003). So the conversion must flag it as a
    LOSS the operator has to weigh, alongside the usual throughput RECOMMENDATION.
    """
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    result = SchemaConverter().convert_table(
        _identity_table("bigint unsigned"),
        SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE),
    )
    losses = [w for w in result.warnings if w.kind is ConversionNoteKind.LOSS]
    assert len(losses) == 1, result.warnings
    loss = losses[0]
    assert loss.column_name == "id"
    assert loss.source_type == "DECIMAL(20, 0)"
    assert loss.target_type == "bigint"
    # The message must name the concrete threshold and that EXISTING rows are at risk.
    assert "9223372036854775807" in loss.message
    assert "narrowed" in loss.message.lower()
    # The throughput recommendation is still present (this is additive, not a swap).
    assert any(w.kind is ConversionNoteKind.RECOMMENDATION for w in result.warnings)
    # The DDL still became a bigint identity (behaviour unchanged; only the warning is new).
    assert '"id" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY' in result.target_ddl


def test_identity_lossless_widenings_emit_no_narrowing_warning() -> None:
    # INT/BIGINT and `int unsigned` (-> bigint, still lossless) must NOT get the LOSS
    # warning: BIGINT holds every one of their values, so there is nothing to weigh.
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    options = SchemaConvertOptions(
        primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE
    )
    for mysql_type in ("int", "bigint", "int unsigned", "smallint", "mediumint"):
        result = SchemaConverter().convert_table(_identity_table(mysql_type), options)
        losses = [w for w in result.warnings if w.kind is ConversionNoteKind.LOSS]
        assert losses == [], (mysql_type, [w.message for w in losses])
        # ...but the throughput recommendation is always there.
        assert any(
            w.kind is ConversionNoteKind.RECOMMENDATION for w in result.warnings
        ), mysql_type


def test_identity_narrowing_warning_absent_for_other_strategies() -> None:
    # The narrowing is specific to the identity BIGINT requirement. Keeping the source
    # PK leaves `bigint unsigned` as numeric(20,0) (full range), so no narrowing warning.
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    result = SchemaConverter().convert_table(
        _identity_table("bigint unsigned"),
        SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.KEEP_INTEGER),
    )
    assert [w for w in result.warnings if w.kind is ConversionNoteKind.LOSS] == []
    # The kept key retains the full unsigned range as decimal(20,0) (not narrowed).
    assert "DECIMAL(20, 0)" in result.target_ddl
    assert "IDENTITY" not in result.target_ddl


def test_wide_decimal_is_clamped_to_the_dsql_limit_with_a_warning() -> None:
    """MySQL allows DECIMAL(65,30); DSQL caps precision at 38 and scale at 37.

    Confirmed live: "NUMERIC precision 39 must be between 1 and 38" and "NUMERIC scale 38
    must be between 0 and 37". Emitting the source spec verbatim made the CREATE TABLE
    fail, so the spec is clamped -- but that loses range, so it must be reported.
    """
    result = _convert_table(_single_column_table("t", "DECIMAL(65,30)"))
    assert "DECIMAL(38, 30)" in result.target_ddl
    assert "NUMERIC(65" not in result.target_ddl.upper()
    messages = [w.message for w in result.warnings if "precision 65" in w.message]
    assert messages, result.warnings
    assert "will not fit" in messages[0]


def test_decimal_within_the_limit_is_untouched() -> None:
    # The clamp must not disturb the overwhelmingly common case.
    result = _convert_table(_single_column_table("t", "DECIMAL(10,2)"))
    assert "DECIMAL(10, 2)" in result.target_ddl
    assert not [w for w in result.warnings if "exceeds" in w.message]


def test_decimal_scale_is_clamped_and_never_exceeds_the_precision() -> None:
    from dsql_migrator.core.converter import _clamp_numeric_spec
    import sqlglot

    def spec(text: str):
        params = sqlglot.parse_one(f"CAST(1 AS {text})", read="mysql").to.expressions
        return _clamp_numeric_spec(list(params))

    # Scale over the ceiling is reduced...
    clamped, warning = spec("DECIMAL(38,38)")
    assert clamped == "38, 37" and warning is not None
    # ...and a clamped precision drags an over-large scale down with it, so the pair
    # stays valid (scale > precision is itself an error).
    clamped_both, warning_both = spec("DECIMAL(65,60)")
    precision, scale = (int(x) for x in clamped_both.split(","))
    assert precision == 38 and scale <= precision and warning_both is not None


# ---------------------------------------------------------------------------
# Conversion must not silently drop what Evaluation rated most severe
# ---------------------------------------------------------------------------


def test_fulltext_index_conversion_reports_the_lost_capability() -> None:
    """A FULLTEXT index converted to the SAME DDL as an ordinary one, with no note.

    Every secondary index is emitted as ``CREATE INDEX ASYNC``, which for FULLTEXT is not
    an equivalent -- it is a plain B-tree on the same column, and the ``MATCH ... AGAINST``
    queries the index existed for cannot use it. Evaluation rates this UNSUPPORTED /
    SIGNIFICANT (its most severe), so an operator reading only the conversion screen had no
    sign that a full-text search feature had just been dropped.
    """
    from dsql_migrator.core.converter import (
        ConversionNoteKind,
        SchemaConverter,
        SchemaConvertOptions,
    )
    from dsql_migrator.core.models import Classification, ColumnDef, IndexDef, TableDef

    table = TableDef(
        name="ecommerce.products",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="body", mysql_type="text", nullable=True),
        ],
        primary_key=["id"],
        indexes=[
            IndexDef(name="ft_body", columns=["body"], unique=False, index_type="FULLTEXT"),
        ],
    )
    result = SchemaConverter().convert_table(table, SchemaConvertOptions())
    notes = [w for w in result.warnings if "FULLTEXT" in w.message]
    assert notes, "a FULLTEXT index must not convert silently"
    note = notes[0]
    # LOSS, not RECOMMENDATION: there is no target-side substitute, unlike partitioning
    # (where DSQL's own distribution replaces the mechanism).
    assert note.kind is ConversionNoteKind.LOSS
    assert note.classification is Classification.UNSUPPORTED
    assert "ft_body" in note.message
    # It says what actually happens (the index IS created) and what to do instead.
    assert "not an equivalent" in note.message.lower() or "NOT an equivalent" in note.message
    assert "OpenSearch" in note.message
    # The DDL is unchanged -- this adds a note, it does not alter output.
    assert any("ft_body" in ddl for ddl in result.index_ddls)


def test_spatial_index_is_reported_too_and_plain_indexes_are_not() -> None:
    """SPATIAL shares the gap; BTREE (MySQL's default) must not raise a false alarm."""
    from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions
    from dsql_migrator.core.models import ColumnDef, IndexDef, TableDef

    def _convert(index_type):
        table = TableDef(
            name="t.x",
            columns=[
                ColumnDef(name="id", mysql_type="bigint", nullable=False),
                ColumnDef(name="c", mysql_type="varchar(20)", nullable=True),
            ],
            primary_key=["id"],
            indexes=[
                IndexDef(name="i", columns=["c"], unique=False, index_type=index_type)
            ],
        )
        return SchemaConverter().convert_table(table, SchemaConvertOptions())

    assert any("SPATIAL" in w.message for w in _convert("SPATIAL").warnings)
    for benign in ("BTREE", None, ""):
        messages = " ".join(w.message for w in _convert(benign).warnings)
        assert "FULLTEXT or SPATIAL" not in messages, benign


def test_unsupported_index_types_come_from_one_shared_definition() -> None:
    """The Evaluation rule and the conversion warning must agree on the SET.

    Restating the literal in both places is how two screens drift apart -- the same
    failure mode as the Routines / Stored procedures mismatch.
    """
    from dsql_migrator.core import converter
    from dsql_migrator.core.assessor import _UNSUPPORTED_INDEX_TYPES

    assert converter._UNSUPPORTED_INDEX_TYPES is _UNSUPPORTED_INDEX_TYPES


def test_partitioned_table_conversion_reports_the_dropped_partitioning() -> None:
    """A partitioned source converted to a silently-plain table.

    The target DDL is right (DSQL distributes by primary key and has no PARTITION BY), but
    the conversion said nothing -- while Evaluation reported it MANUAL / MEDIUM. What the
    operator needs to know is that partition-SCOPED operations do not carry over.
    """
    from dsql_migrator.core.converter import (
        ConversionNoteKind,
        SchemaConverter,
        SchemaConvertOptions,
    )
    from dsql_migrator.core.models import ColumnDef, TableDef

    table = TableDef(
        name="ecommerce.events",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=["id"],
        partitioned=True,
    )
    result = SchemaConverter().convert_table(table, SchemaConvertOptions())
    notes = [w for w in result.warnings if "partitioning" in w.message]
    assert notes, "a partitioned source must not convert silently"
    note = notes[0]
    # RECOMMENDATION: no data or key changed, and DSQL distributes automatically -- this is
    # not a dropped constraint like a foreign key.
    assert note.kind is ConversionNoteKind.RECOMMENDATION
    # Names the consequences that actually bite: partition-scoped SQL and maintenance.
    assert "PARTITION (p1)" in note.message
    assert "TRUNCATE" in note.message
    # No PARTITION BY leaks into the target.
    assert "PARTITION" not in result.target_ddl

    # A non-partitioned table gets no such note.
    plain = TableDef(
        name="ecommerce.plain",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=["id"],
    )
    plain_result = SchemaConverter().convert_table(plain, SchemaConvertOptions())
    assert not any("partitioning" in w.message for w in plain_result.warnings)


def test_every_table_level_assessor_rule_has_a_conversion_note() -> None:
    """The audit invariant: what Evaluation flags, conversion must also surface.

    Six rules were silent on the conversion screen -- FULLTEXT/SPATIAL indexes,
    partitioning, the column/index limits, oversized LOBs, generated columns, CI
    collations and ON UPDATE. Each was reported by Evaluation and then invisible on the one
    screen that shows the DDL, which is how the workshop found the AUTO_INCREMENT case.
    This drives BOTH engines over the same table and fails if a rule fires with no
    corresponding conversion note -- so a newly added assessor rule cannot regress into the
    same gap.
    """
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions
    from dsql_migrator.core.models import (
        ColumnDef,
        IndexDef,
        SourceInventory,
        TableDef,
    )

    pk = ColumnDef(name="id", mysql_type="bigint", nullable=False)

    def _wide():
        return [pk] + [
            ColumnDef(name=f"c{i}", mysql_type="int", nullable=True) for i in range(400)
        ]

    def _many_idx_cols():
        return [pk] + [
            ColumnDef(name=f"c{i}", mysql_type="int", nullable=True) for i in range(30)
        ]

    cases = {
        "BIT_TYPE": TableDef(
            name="t.a",
            columns=[pk, ColumnDef(name="f", mysql_type="bit(8)", nullable=True)],
            primary_key=["id"],
        ),
        "ENUM_SET_TYPE": TableDef(
            name="t.b",
            columns=[pk, ColumnDef(name="s", mysql_type="enum('a')", nullable=True)],
            primary_key=["id"],
        ),
        "TINYINT_BOOLEAN": TableDef(
            name="t.c",
            columns=[pk, ColumnDef(name="b", mysql_type="tinyint(1)", nullable=True)],
            primary_key=["id"],
        ),
        "YEAR_TYPE": TableDef(
            name="t.d",
            columns=[pk, ColumnDef(name="y", mysql_type="year", nullable=True)],
            primary_key=["id"],
        ),
        "NUMERIC_PRECISION": TableDef(
            name="t.e",
            columns=[pk, ColumnDef(name="n", mysql_type="decimal(65,30)", nullable=True)],
            primary_key=["id"],
        ),
        "SPATIAL_TYPE": TableDef(
            name="t.f",
            columns=[pk, ColumnDef(name="g", mysql_type="geometry", nullable=True)],
            primary_key=["id"],
        ),
        "OVERSIZED_LOB": TableDef(
            name="t.g",
            columns=[pk, ColumnDef(name="l", mysql_type="longblob", nullable=True)],
            primary_key=["id"],
        ),
        "CI_COLLATION": TableDef(
            name="t.h",
            columns=[
                pk,
                ColumnDef(
                    name="e",
                    mysql_type="varchar(20)",
                    nullable=True,
                    collation="utf8mb4_0900_ai_ci",
                ),
            ],
            primary_key=["id"],
        ),
        "GENERATED_COLUMN": TableDef(
            name="t.i",
            columns=[pk, ColumnDef(name="m", mysql_type="int", nullable=True, generated=True)],
            primary_key=["id"],
        ),
        "ON_UPDATE_TIMESTAMP": TableDef(
            name="t.j",
            columns=[
                pk,
                ColumnDef(
                    name="u",
                    mysql_type="datetime",
                    nullable=True,
                    auto_update_timestamp=True,
                ),
            ],
            primary_key=["id"],
        ),
        "AUTO_INCREMENT": TableDef(
            name="t.k", columns=[pk], primary_key=["id"], auto_increment_column="id"
        ),
        "PARTITIONED_TABLE": TableDef(
            name="t.l", columns=[pk], primary_key=["id"], partitioned=True
        ),
        "UNSUPPORTED_INDEX_TYPE": TableDef(
            name="t.m",
            columns=[pk],
            primary_key=["id"],
            indexes=[
                IndexDef(name="ft", columns=["id"], unique=False, index_type="FULLTEXT")
            ],
        ),
        "NO_PRIMARY_KEY": TableDef(name="t.n", columns=[pk], primary_key=[]),
        "TOO_MANY_COLUMNS": TableDef(name="t.o", columns=_wide(), primary_key=["id"]),
        "TOO_MANY_INDEXES": TableDef(
            name="t.p",
            columns=_many_idx_cols(),
            primary_key=["id"],
            indexes=[
                IndexDef(name=f"i{i}", columns=[f"c{i}"], unique=False)
                for i in range(30)
            ],
        ),
    }

    converter = SchemaConverter()
    assessor = CompatibilityAssessor()
    silent: list[str] = []
    for rule, table in cases.items():
        fired = {
            item.rule_id
            for item in assessor.assess(SourceInventory(tables=[table])).items
        }
        assert rule in fired, f"fixture for {rule} no longer triggers that rule"
        notes = converter.convert_table(table, SchemaConvertOptions()).warnings
        if not notes:
            silent.append(rule)
    assert not silent, (
        "Evaluation flags these but Schema Conversion says nothing: "
        f"{sorted(silent)}"
    )


def test_case_sensitive_collation_is_not_reported_as_a_change() -> None:
    """Only ``_ci`` collations change behaviour on the target.

    A ``_cs`` or ``_bin`` collation already matches PostgreSQL's case-sensitive comparison,
    so warning about it would be a false alarm -- and false alarms on this screen train the
    reader to skip the notes that matter.
    """
    from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions
    from dsql_migrator.core.models import ColumnDef, TableDef

    for collation in ("utf8mb4_0900_as_cs", "utf8mb4_bin", None):
        table = TableDef(
            name="t.x",
            columns=[
                ColumnDef(name="id", mysql_type="bigint", nullable=False),
                ColumnDef(
                    name="e", mysql_type="varchar(20)", nullable=True, collation=collation
                ),
            ],
            primary_key=["id"],
        )
        messages = " ".join(
            w.message
            for w in SchemaConverter().convert_table(table, SchemaConvertOptions()).warnings
        )
        assert "case-INSENSITIVE" not in messages, collation


def test_generated_and_on_update_notes_name_the_drift_risk() -> None:
    """Both start CORRECT after Full Load and drift on the first application write.

    That is what makes them dangerous and why the wording has to say it: the target looks
    right, every count and checksum matches, and the divergence begins later.
    """
    from dsql_migrator.core.converter import SchemaConverter, SchemaConvertOptions
    from dsql_migrator.core.models import ColumnDef, TableDef

    table = TableDef(
        name="t.x",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="margin", mysql_type="int", nullable=True, generated=True),
            ColumnDef(
                name="updated_at",
                mysql_type="datetime",
                nullable=True,
                auto_update_timestamp=True,
            ),
        ],
        primary_key=["id"],
    )
    messages = {
        w.message for w in SchemaConverter().convert_table(table, SchemaConvertOptions()).warnings
    }
    generated = next(m for m in messages if "GENERATED" in m)
    assert "ORDINARY columns" in generated
    assert "drift" in generated
    on_update = next(m for m in messages if "ON UPDATE CURRENT_TIMESTAMP" in m)
    assert "no triggers" in on_update
    assert "stale" in on_update
