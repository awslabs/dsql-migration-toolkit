# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source DDL reconstruction (``converter_postgres.build_pg_source_ddl``)."""

import pytest
import sqlglot

from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.converter_postgres import (
    build_pg_source_ddl,
    clamp_pg_numeric,
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


def test_clamp_pg_numeric_reduces_over_precision_and_scale() -> None:
    # Aurora DSQL caps numeric at precision 38 / scale 37. A PG numeric beyond that must be
    # clamped (with a warning) -- otherwise the verbatim numeric(40,10) is REJECTED by DSQL
    # at CREATE TABLE with no prior signal. Mirrors the MySQL DECIMAL clamp.
    clamped, note = clamp_pg_numeric("numeric(40,10)")
    assert clamped == "numeric(38,10)" and note and "precision 40" in note
    clamped, note = clamp_pg_numeric("numeric(1000,500)")
    assert clamped == "numeric(38,37)" and note and "precision 1000" in note and "scale 500" in note
    # decimal/dec aliases + precision-only.
    assert clamp_pg_numeric("decimal(50)")[0] == "decimal(38)"
    # In-range, bare, and non-numeric types pass through untouched (no warning).
    for ok in ("numeric(12,2)", "numeric(38,37)", "numeric", "uuid", "text"):
        assert clamp_pg_numeric(ok) == (ok, None)


def test_convert_table_clamps_over_precision_pg_numeric_with_warning() -> None:
    # End-to-end: an over-precision PG numeric converts to a VALID DSQL DDL (clamped to
    # <=38/37) AND carries a MANUAL warning naming the column, so the fidelity loss is
    # surfaced instead of a silent apply-time CREATE TABLE failure.
    table = TableDef(
        name="prices",
        columns=[
            _col("id", "bigint", nullable=False),
            _col("huge", "numeric(40,10)"),
            _col("wild", "numeric(1000,500)"),
            _col("ok", "numeric(12,2)"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    ddl = conv.target_ddl.lower()  # sqlglot renders numeric as its `decimal` alias
    assert "decimal(38, 10)" in ddl and "decimal(38, 37)" in ddl  # clamped to DSQL limits
    assert "(40" not in ddl and "1000" not in ddl                 # originals gone
    assert "decimal(12, 2)" in ddl                                # in-range unchanged
    clamp_warns = {
        w.column_name
        for w in conv.warnings
        if w.classification is Classification.MANUAL and "aurora dsql" in w.message.lower()
    }
    assert {"huge", "wild"} <= clamp_warns
    assert "ok" not in clamp_warns  # the in-range column is not warned


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


def test_timetz_and_interval_pk_do_not_trigger_key_size_warning() -> None:
    # Tier-3 #12 (real bug fixed): a timetz / interval PRIMARY KEY is a small fixed-width
    # DSQL-supported key, but it used to hit the unbounded-varlen fallback (1025 bytes) and
    # falsely trip the ">1024-byte key" warning. Adding TIMETZ/INTERVAL to _KEY_TYPE_BYTES
    # sizes them correctly, so no false alarm -- while a genuinely oversized key still warns.
    conv = SchemaConverter(source_type=SourceType.POSTGRES)
    for typ in ("time with time zone", "timetz", "interval"):
        table = TableDef(name="public.t", columns=[_col("k", typ, nullable=False)], primary_key=["k"])
        assert not any("bytes combined" in w.message for w in conv.convert_table(table).warnings), typ
    big = TableDef(
        name="public.big",
        columns=[_col("k", "character varying(2000)", nullable=False)],
        primary_key=["k"],
    )
    assert any("bytes combined" in w.message for w in conv.convert_table(big).warnings)


def test_pg_source_pk_strategy_is_inert_without_auto_increment() -> None:
    # Tier-3 #9 (documented deferral tripwire): PG enrich never sets auto_increment_column,
    # so IDENTITY_WITH_CACHE / CONVERT_TO_UUID PK strategies are no-ops for a PG serial/
    # identity PK (and the monotonic hot-partition RECOMMENDATION never fires). This pins
    # that inert behavior so it fails loudly when the deferred serial/identity refinement
    # lands (introspector setting auto_increment_column for PG).
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions
    from dsql_migrator.core.models import ConversionNoteKind

    table = TableDef(name="public.users", columns=[_col("id", "integer", nullable=False)], primary_key=["id"])
    for strat in (PrimaryKeyStrategy.IDENTITY_WITH_CACHE, PrimaryKeyStrategy.CONVERT_TO_UUID):
        r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
            table, SchemaConvertOptions(primary_key_strategy=strat)
        )
        upper = r.target_ddl.upper()
        assert "GENERATED" not in upper and "UUID" not in upper  # strategy inert -> INT PK
        assert not any(w.kind is ConversionNoteKind.RECOMMENDATION for w in r.warnings)


def test_pg_source_emits_index_ddls_and_preserves_fk() -> None:
    # Tier-3 #10: secondary indexes + foreign keys are handled for a PG source (NOT dropped
    # by an is_postgres gate). CREATE [UNIQUE] INDEX ASYNC on the schema-qualified table,
    # FK preserved as metadata, and a FK-removal warning.
    from dsql_migrator.core.models import ForeignKeyDef, IndexDef

    table = TableDef(
        name="public.orders",
        columns=[_col("id", "bigint", False), _col("email", "text", False), _col("cust", "bigint", False)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_email", columns=["email"], unique=True)],
        foreign_keys=[ForeignKeyDef(name="fk_cust", columns=["cust"],
                                    referenced_table="cust", referenced_columns=["id"])],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert any(
        "CREATE UNIQUE INDEX ASYNC" in d and "ix_email" in d and '"public"."orders"' in d
        for d in r.index_ddls
    )
    assert [f.name for f in r.preserved_foreign_keys] == ["fk_cust"]
    assert any("foreign key" in w.message.lower() and "fk_cust" in w.message for w in r.warnings)


def test_pg_source_multi_schema_emits_create_schema() -> None:
    # Tier-3 #11: a schema-qualified PG table emits CREATE SCHEMA IF NOT EXISTS + a
    # schema-qualified CREATE TABLE; an unqualified name emits no schema DDL.
    conv = SchemaConverter(source_type=SourceType.POSTGRES)
    q = conv.convert_table(TableDef(name="sales.orders", columns=[_col("id", "bigint", False)], primary_key=["id"]))
    assert any('CREATE SCHEMA IF NOT EXISTS "sales"' in d for d in q.schema_ddls)
    assert '"sales"."orders"' in q.target_ddl
    u = conv.convert_table(TableDef(name="orders", columns=[_col("id", "bigint", False)], primary_key=["id"]))
    assert not u.schema_ddls
