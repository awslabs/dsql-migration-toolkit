# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source DDL reconstruction (``converter_postgres.build_pg_source_ddl``)."""

import pytest
import sqlglot

from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.converter_postgres import (
    build_pg_source_ddl,
    normalize_pg_base_type,
    unsupported_dsql_reason,
)
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    SourceType,
    TableDef,
)


def _col(name: str, typ: str, nullable: bool = True) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=typ, nullable=nullable)


def test_renders_exact_pg_types_and_parses_as_postgres() -> None:
    table = TableDef(
        name="public.orders",
        columns=[
            _col("id", "bigint", nullable=False),
            _col("total", "numeric(12,2)"),
            _col("tags", "text[]"),
            _col("created_at", "timestamp with time zone"),
            _col("meta", "jsonb"),
            _col("blob", "bytea"),
            _col("uid", "uuid"),
        ],
        primary_key=["id"],
    )
    ddl = build_pg_source_ddl(table)
    assert '"public"."orders"' in ddl
    assert '"tags" text[]' in ddl
    assert '"created_at" timestamp with time zone' in ddl
    assert 'PRIMARY KEY ("id")' in ddl
    # Must parse cleanly as PostgreSQL (the converter reads it with read="postgres").
    assert sqlglot.parse_one(ddl, read="postgres") is not None


def test_composite_pk_and_not_null() -> None:
    table = TableDef(
        name="order_items",
        columns=[
            _col("order_id", "bigint", nullable=False),
            _col("line_no", "integer", nullable=False),
        ],
        primary_key=["order_id", "line_no"],
    )
    ddl = build_pg_source_ddl(table)
    assert 'PRIMARY KEY ("order_id", "line_no")' in ddl
    assert '"order_id" bigint NOT NULL' in ddl
    assert sqlglot.parse_one(ddl, read="postgres") is not None


def test_rejects_table_with_no_columns() -> None:
    with pytest.raises(ValueError):
        build_pg_source_ddl(TableDef(name="t", columns=[], primary_key=[]))


def test_double_quotes_are_escaped_and_reparse() -> None:
    table = TableDef(
        name='we"ird', columns=[_col('c"ol', "integer")], primary_key=[]
    )
    ddl = build_pg_source_ddl(table)
    assert '"we""ird"' in ddl and '"c""ol"' in ddl
    assert sqlglot.parse_one(ddl, read="postgres") is not None


# ---------------------------------------------------------------------------
# DSQL-unsupported PostgreSQL type detection (unsupported_dsql_reason)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "pg_type",
    [
        # Numeric / character / temporal / misc that DSQL supports (verified via
        # dsql_lint: 0 errors). Modifiers and multi-word spellings must be tolerated.
        "integer", "int", "int4", "smallint", "bigint", "int8",
        "numeric", "numeric(12,2)", "decimal(10, 0)", "double precision", "real",
        "text", "character varying(50)", "varchar(255)", "char(10)", "bpchar",
        "boolean", "uuid", "json", "jsonb", "bytea",
        "date", "time without time zone", "time(6) with time zone",
        "timestamp with time zone", "timestamp(6) without time zone", "timestamptz",
        # interval[fields][(p)] is DSQL-supported; format_type spells the fields inline.
        "interval", "interval(6)", "interval day to second", "interval year",
        "interval hour to minute", "interval second(3)", "interval day to second(6)",
    ],
)
def test_supported_pg_types_are_not_flagged(pg_type: str) -> None:
    assert unsupported_dsql_reason(pg_type) is None


@pytest.mark.parametrize(
    "pg_type",
    ["text[]", "integer[]", "numeric(10,2)[]", "character varying(50)[]"],
)
def test_array_types_are_flagged_as_unsupported(pg_type: str) -> None:
    reason = unsupported_dsql_reason(pg_type)
    assert reason is not None and "array" in reason.lower()


@pytest.mark.parametrize(
    "pg_type",
    ["money", "xml", "inet", "cidr", "macaddr", "point", "polygon",
     "tsvector", "bit(8)", "bit varying(8)", "mood", "public.currency", "vector(3)"],
)
def test_nonallowlisted_pg_types_are_flagged(pg_type: str) -> None:
    # Geometric/network/xml/money/bit/pgvector plus user-defined enum/composite type
    # NAMES (which format_type returns verbatim, e.g. "mood") fall outside DSQL's
    # documented supported set -> flagged so the user remodels rather than hitting a
    # rejected CREATE at apply time.
    reason = unsupported_dsql_reason(pg_type)
    assert reason is not None and pg_type in reason


def test_normalize_strips_modifiers_and_collapses_whitespace() -> None:
    assert normalize_pg_base_type("NUMERIC(12,2)") == "numeric"
    assert normalize_pg_base_type("character varying(50)") == "character varying"
    assert normalize_pg_base_type("timestamp(6) with time zone") == (
        "timestamp with time zone"
    )
    assert normalize_pg_base_type("  BIGINT  ") == "bigint"


def test_unsupported_dsql_reason_handles_empty() -> None:
    assert unsupported_dsql_reason(None) is None
    assert unsupported_dsql_reason("") is None


def test_unsupported_reason_names_the_faithful_remodel_target() -> None:
    # Option (a): the warning names WHAT to remodel each unsupported type to (the faithful
    # DSQL target), not a blanket "unsupported" -- and NOT a blanket bytea. No auto-substitution.
    assert "jsonb" in unsupported_dsql_reason("text[]").lower()  # array -> jsonb/child table
    assert "text" in unsupported_dsql_reason("inet").lower()
    assert "text" in unsupported_dsql_reason("cidr").lower()
    assert "text" in unsupported_dsql_reason("xml").lower()
    assert "numeric" in unsupported_dsql_reason("money").lower()  # currency -> numeric
    assert "text" in unsupported_dsql_reason("bit(8)").lower()
    assert "text" in unsupported_dsql_reason("tsvector").lower()
    assert "text" in unsupported_dsql_reason("point").lower()  # geometric -> text/numeric cols
    r_range = unsupported_dsql_reason("int4range").lower()
    assert "text" in r_range and ("bound" in r_range or "[1,5)" in r_range)
    r_vec = unsupported_dsql_reason("vector(3)").lower()
    assert "jsonb" in r_vec or "text" in r_vec
    # A user-defined enum/composite NAME (format_type returns it verbatim) -> generic
    # remodel guidance that still names concrete targets (enum -> text, composite -> cols).
    enum_reason = unsupported_dsql_reason("mood").lower()
    assert "mood" in enum_reason and ("enum" in enum_reason or "remodel" in enum_reason)
    # bytea is NOT used as a general fallback (only mentioned as a secondary option for bit).
    assert "bytea" not in unsupported_dsql_reason("inet").lower()
    assert "bytea" not in unsupported_dsql_reason("money").lower()


def test_convert_table_warns_on_array_column_for_pg_source() -> None:
    # A PostgreSQL-source table with a text[] column must convert (DDL faithful to the
    # source, since v1 does not auto-substitute) AND carry an UNSUPPORTED warning naming
    # the column, so the user remodels the array before applying to DSQL.
    table = TableDef(
        name="widgets",
        columns=[_col("id", "uuid", nullable=False), _col("tags", "text[]")],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    array_warnings = [
        w
        for w in conv.warnings
        if w.column_name == "tags"
        and w.classification is Classification.UNSUPPORTED
        and "array" in w.message.lower()
    ]
    assert len(array_warnings) == 1
    # The supported uuid PK column is not flagged.
    assert not any(w.column_name == "id" for w in conv.warnings)


@pytest.mark.parametrize(
    "interval_type",
    ["interval", "interval day to second", "interval year", "interval second(3)",
     "interval day to second(6)"],
)
def test_convert_table_handles_fields_qualified_interval(interval_type: str) -> None:
    # A fields-qualified interval (esp. with precision, e.g. "interval second(3)") must
    # convert cleanly: no spurious UNSUPPORTED warning AND no whole-table parse failure.
    # sqlglot's postgres reader can't parse "interval <fields>(N)", so build_pg_source_ddl
    # drops the (N) precision -- the table must still auto-convert with faithful DDL.
    table = TableDef(
        name="durations",
        columns=[_col("id", "bigint", nullable=False), _col("span", interval_type)],
        primary_key=["id"],
    )
    # build_pg_source_ddl output must parse as PostgreSQL (what convert_table relies on).
    assert sqlglot.parse_one(build_pg_source_ddl(table), read="postgres") is not None
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    # Not surfaced as an unparsable/unsupported table, and no spurious type warning.
    assert conv.table == "durations"
    assert "interval" in conv.target_ddl.lower()
    assert not any(
        w.column_name == "span" and w.classification is Classification.UNSUPPORTED
        for w in conv.warnings
    )


def test_convert_table_no_type_warnings_for_all_supported_pg_columns() -> None:
    table = TableDef(
        name="orders",
        columns=[
            _col("id", "bigint", nullable=False),
            _col("total", "numeric(12,2)"),
            _col("meta", "jsonb"),
            _col("created_at", "timestamp with time zone"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any(
        w.classification is Classification.UNSUPPORTED for w in conv.warnings
    )
