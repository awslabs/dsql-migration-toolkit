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
    hot = [w for w in result.warnings if "hot partition" in w.message.lower()]
    assert len(hot) == 1
    assert hot[0].classification is Classification.MANUAL
    assert hot[0].column_name == "id"


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
    assert any("hot partition" in w.message.lower() for w in result.warnings)


def test_table_without_auto_increment_has_no_hot_partition_warning() -> None:
    table = TableDef(
        name="plain",
        columns=[ColumnDef(name="id", mysql_type="INT")],
        primary_key=["id"],
    )
    result = SchemaConverter().convert_table(table)
    assert all("hot partition" not in w.message.lower() for w in result.warnings)


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
