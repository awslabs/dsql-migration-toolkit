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
    unconstrained_numeric_note,
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


def test_convert_table_warns_when_a_pg_default_is_dropped() -> None:
    # v1 does not re-emit PG column DEFAULTs; dropping one silently would hide a
    # post-cut-over INSERT change (Property 6), so a MANUAL warning must name it. A
    # NOT NULL column escalates (an omitted INSERT is rejected on the target).
    table = TableDef(
        name="events",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="status", mysql_type="text", nullable=False, default="'new'"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    default_warnings = [
        w for w in conv.warnings
        if w.column_name == "status" and "default" in w.message.lower()
    ]
    assert len(default_warnings) == 1
    assert "NOT NULL" in default_warnings[0].message


def test_convert_table_does_not_warn_on_a_serial_identity_default() -> None:
    # A serial/identity default (nextval) is the identity mechanism, handled by the PK
    # strategy + cut-over sequence sync -- skipped exactly like MySQL AUTO_INCREMENT.
    table = TableDef(
        name="seqs",
        columns=[
            ColumnDef(
                name="id", mysql_type="bigint", nullable=False,
                default="nextval('seqs_id_seq'::regclass)",
            ),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("default" in w.message.lower() for w in conv.warnings)


def test_convert_view_reads_pg_dialect_for_a_pg_source() -> None:
    # A PostgreSQL view must be parsed as PG, not MySQL -- otherwise PG-only syntax
    # (ANY(ARRAY[...]), ILIKE, `::` casts) is mangled into fabricated SQL.
    from dsql_migrator.core.models import ViewDef

    view = ViewDef(
        name="public.active",
        definition=(
            "CREATE VIEW public.active AS SELECT id FROM users "
            "WHERE status = ANY(ARRAY[1, 2]) AND name ILIKE 'a%'"
        ),
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_view(view)
    assert conv.auto_converted
    # PG-only syntax survives (pretty-print may wrap whitespace, so check the tokens).
    assert "ANY(" in conv.target_ddl and "ARRAY[1, 2]" in conv.target_ddl
    assert "ILIKE" in conv.target_ddl.upper()


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


def test_unconstrained_numeric_note_flags_only_bare_numeric() -> None:
    # B2: a bare `numeric`/`decimal`/`dec` (no declared precision) is stored by DSQL at its
    # default numeric(18,6) -- a >6-fractional-digit value is rounded on load. This must be
    # WARNED (not silent). A declared precision/scale, and non-numeric types, return None.
    for bare in ("numeric", "decimal", "dec", "  NUMERIC  "):
        note = unconstrained_numeric_note(bare)
        assert note is not None and "numeric(18,6)" in note and "6 fractional digits" in note
    for not_bare in ("numeric(12,2)", "numeric(38)", "decimal(10,4)", "uuid", "text", ""):
        assert unconstrained_numeric_note(not_bare) is None


def test_convert_table_warns_on_bare_numeric_for_pg_source() -> None:
    # End-to-end B2 regression: a PostgreSQL bare `numeric` converts (near-identity) but
    # MUST carry a MANUAL warning that DSQL caps it at numeric(18,6) -- otherwise the >6dp
    # rounding is silent (Property 6) and Validation, which compares such a column at scale
    # 6, would report a green MATCH over data DSQL truncated. Declared-scale numerics are
    # NOT warned; a bare numeric and an over-precision numeric are mutually exclusive
    # (the clamp path `continue`s), so a bare column never also gets a clamp warning.
    table = TableDef(
        name="ledger",
        columns=[
            _col("id", "bigint", nullable=False),
            _col("amount", "numeric"),        # bare -> warned
            _col("price", "decimal"),         # bare alias -> warned
            _col("rate", "numeric(12,4)"),    # declared -> not warned
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    bare_warns = {
        w.column_name
        for w in conv.warnings
        if w.classification is Classification.MANUAL
        and "declares no precision/scale" in (w.message or "")
    }
    assert bare_warns == {"amount", "price"}
    # The warning must name the DSQL default and be actionable (mention Schema Conversion).
    amount_msg = next(
        w.message for w in conv.warnings
        if w.column_name == "amount" and "declares no precision/scale" in (w.message or "")
    )
    assert "numeric(18,6)" in amount_msg and "Schema Conversion" in amount_msg
    # Each bare column gets exactly ONE such warning (not also a clamp warning).
    assert sum(
        1 for w in conv.warnings
        if w.column_name == "amount" and "declares no precision/scale" in (w.message or "")
    ) == 1


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
    # FK preserved as metadata AND re-created as a post-load ADD CONSTRAINT, with an
    # advisory FK note.
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
    assert r.foreign_key_ddls == [
        'ALTER TABLE "public"."orders" ADD CONSTRAINT "fk_cust" '
        'FOREIGN KEY ("cust") REFERENCES "cust" ("id") NOT VALID'
    ]
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


# --- Tier-4 PG converter fixes -----------------------------------------------


def test_pg_source_bytea_index_is_skipped_and_warned() -> None:
    # T4-7: DSQL cannot build a key/index on a bytea column ("datatype bytea is not
    # supported in a key"). On a PG source ColumnDef.mysql_type holds the exact PG type, so
    # a bytea column reads as "bytea"; _maps_to_bytea now recognizes it, so a secondary index
    # over it is SKIPPED (not emitted as a doomed CREATE INDEX ASYNC) and surfaced as a
    # MANUAL/LOSS warning. A sibling non-bytea index is still emitted.
    from dsql_migrator.core.models import ConversionNoteKind, IndexDef

    table = TableDef(
        name="public.docs",
        columns=[_col("id", "bigint", False), _col("payload", "bytea"), _col("name", "text")],
        primary_key=["id"],
        indexes=[
            IndexDef(name="ix_payload", columns=["payload"], unique=False),
            IndexDef(name="ix_name", columns=["name"], unique=False),
        ],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("ix_payload" in d for d in r.index_ddls)  # doomed bytea index dropped
    assert any("CREATE INDEX ASYNC" in d and "ix_name" in d for d in r.index_ddls)  # sibling kept
    warn = [w for w in r.warnings if "bytea" in w.message.lower() and "ix_payload" in w.message]
    assert warn, r.warnings
    assert warn[0].classification is Classification.MANUAL
    assert warn[0].kind is ConversionNoteKind.LOSS


def test_pg_source_bytea_primary_key_is_flagged_unsupported() -> None:
    # T4-7: a bytea PRIMARY KEY makes the generated CREATE TABLE REJECTED by DSQL. It was
    # SILENT for a PG source (the warning was is_postgres-gated off); now it is surfaced as
    # UNSUPPORTED rather than emitting a DDL that only fails at apply time.
    table = TableDef(
        name="public.blobs",
        columns=[_col("k", "bytea", nullable=False), _col("v", "text")],
        primary_key=["k"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert any(
        "bytea" in w.message.lower()
        and "primary key" in w.message.lower()
        and w.classification is Classification.UNSUPPORTED
        for w in r.warnings
    ), r.warnings


@pytest.mark.parametrize("bit_type", ["bit varying", "bit varying(10)"])
def test_pg_bit_varying_column_converts_without_aborting_table(bit_type: str) -> None:
    # T4-6: format_type spells varbit as the two-word "bit varying"[(n)], which sqlglot's
    # postgres reader CANNOT parse -> the whole table used to fall back to the generic
    # "could not auto-convert" and every sibling column lost its conversion. The emitted DDL
    # now uses the parseable one-word "varbit" alias, so the table converts, the bit column
    # is still flagged UNSUPPORTED (named by its original "bit varying" type), and the
    # sibling columns are preserved.
    table = TableDef(
        name="public.flags",
        columns=[_col("id", "bigint", False), _col("mask", bit_type), _col("label", "text")],
        primary_key=["id"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert "could not auto-convert" not in r.target_ddl.lower()  # not the whole-table fallback
    assert '"public"."flags"' in r.target_ddl
    assert any(
        w.column_name == "mask"
        and w.classification is Classification.UNSUPPORTED
        and "bit varying" in w.message
        for w in r.warnings
    ), r.warnings


def test_bit_varying_ddl_is_emitted_as_parseable_varbit() -> None:
    # T4-6 (unit): the two-word "bit varying"[(n)] is rewritten to the parseable "varbit"
    # alias; a fixed-width bit(n) already parses and is left untouched.
    from dsql_migrator.core.converter_postgres import _ddl_column_type

    assert _ddl_column_type("bit varying") == "varbit"
    assert _ddl_column_type("bit varying(10)") == "varbit(10)"
    assert _ddl_column_type("bit(8)") == "bit(8)"
    table = TableDef(
        name="t",
        columns=[_col("id", "bigint", False), _col("m", "bit varying(8)")],
        primary_key=["id"],
    )
    sqlglot.parse_one(build_pg_source_ddl(table), read="postgres")  # must not raise


@pytest.mark.parametrize(
    "bare",
    ["numeric", "varchar", "character varying", "char", "character",
     "timestamp", "time", "text", "bytea", "uuid", "boolean"],
)
def test_bare_parameterless_pg_types_emit_verbatim_and_parse(bare: str) -> None:
    # T4-5 regression guard: a parameterless PG type passes through _ddl_column_type
    # unchanged (no spurious numeric clamp) and the emitted CREATE TABLE still parses as
    # postgres (no parse abort).
    from dsql_migrator.core.converter_postgres import _ddl_column_type

    assert _ddl_column_type(bare) == bare
    table = TableDef(
        name="t", columns=[_col("id", "bigint", False), _col("c", bare)], primary_key=["id"]
    )
    sqlglot.parse_one(build_pg_source_ddl(table), read="postgres")  # must not raise


def test_pg_stored_generated_column_warns_manual_with_pg_wording() -> None:
    # T4-4: a PostgreSQL STORED generated column has no Aurora DSQL equivalent -> created as
    # an ordinary column; Full Load copies the computed value (target starts correct) but
    # nothing maintains it afterward. Surfaced as a MANUAL/LOSS warning with PG-specific
    # wording (not the MySQL "SHOW CREATE TABLE" variant).
    from dsql_migrator.core.models import ConversionNoteKind

    gen = ColumnDef(name="full_name", mysql_type="text", nullable=True, generated=True)
    table = TableDef(
        name="public.people",
        columns=[_col("id", "bigint", False), _col("first", "text"), gen],
        primary_key=["id"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    warn = [w for w in r.warnings if "generated" in w.message.lower() and "full_name" in w.message]
    assert warn, r.warnings
    assert warn[0].classification is Classification.MANUAL
    assert warn[0].kind is ConversionNoteKind.LOSS
    assert "PostgreSQL generated" in warn[0].message  # PG-worded
    assert "STORED or VIRTUAL" in warn[0].message  # covers both kinds (PG18 virtual)
    assert "SHOW CREATE TABLE" not in warn[0].message  # not the MySQL variant


def test_pg_virtual_generated_column_warns_manual_like_stored() -> None:
    # A PostgreSQL 18 VIRTUAL generated column (attgenerated='v', the default kind there) is
    # marked ColumnDef.generated by enrich exactly like a STORED one, so Schema Conversion
    # surfaces the same MANUAL/LOSS warning -- without the fix it produced ZERO signal since
    # VIRTUAL is PG18's default form of GENERATED ALWAYS AS (expr).
    from dsql_migrator.core.models import ConversionNoteKind

    gen = ColumnDef(name="area", mysql_type="numeric", nullable=True, generated=True)
    table = TableDef(
        name="public.shapes",
        columns=[_col("id", "bigint", False), _col("w", "numeric"), gen],
        primary_key=["id"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    warn = [w for w in r.warnings if "generated" in w.message.lower() and "area" in w.message]
    assert warn, r.warnings
    assert warn[0].classification is Classification.MANUAL
    assert warn[0].kind is ConversionNoteKind.LOSS
    assert "STORED or VIRTUAL" in warn[0].message
    assert "logically replicated" in warn[0].message  # CDC caveat surfaced


def test_pg_source_without_generated_columns_emits_no_generated_warning() -> None:
    # T4-4: no false positive -- an ordinary PG table emits no generated-column warning.
    table = TableDef(
        name="public.people",
        columns=[_col("id", "bigint", False), _col("first", "text")],
        primary_key=["id"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("generated" in w.message.lower() for w in r.warnings)


# --- Tier-4 adversarial-verify follow-ups (unsupported PG column in a key/index) ----


def test_pg_unsupported_type_pk_has_no_contradictory_key_size_note() -> None:
    # A varbit/bit-varying PRIMARY KEY converts (T4-6) and is flagged UNSUPPORTED as a column
    # type -- so the key-size estimator must NOT also fire a contradictory ">1 KiB / the DDL
    # itself applies fine" note (which used to be a false ~1025-byte alarm from the
    # unbounded-varlen fallback for a type the estimator does not recognize).
    table = TableDef(
        name="public.t3",
        columns=[_col("flags", "bit varying(64)", nullable=False), _col("note", "text")],
        primary_key=["flags"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert any(
        w.column_name == "flags" and w.classification is Classification.UNSUPPORTED
        for w in r.warnings
    )
    assert not any(
        "bytes combined" in w.message or "applies fine" in w.message for w in r.warnings
    ), r.warnings


@pytest.mark.parametrize("typ", ["bit varying(64)", "bytea[]", "int4range", "inet"])
def test_pg_index_over_unsupported_column_is_not_emitted(typ: str) -> None:
    # An index over a DSQL-unsupported PG column type (varbit, array, range, network, ...) is
    # SKIPPED, not emitted as a doomed post-load CREATE INDEX ASYNC, and does not trigger a
    # key-size note. The column itself is flagged UNSUPPORTED (must be remodelled).
    from dsql_migrator.core.models import IndexDef

    table = TableDef(
        name="public.t",
        columns=[_col("id", "bigint", False), _col("c", typ)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_c", columns=["c"], unique=False)],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("ix_c" in d for d in r.index_ddls)  # doomed index skipped
    assert not any("bytes combined" in w.message for w in r.warnings)
    assert any(
        w.column_name == "c" and w.classification is Classification.UNSUPPORTED
        for w in r.warnings
    )


def test_pg_geometric_key_column_is_not_mislabeled_as_bytea() -> None:
    # PG reuses the names point/polygon (which appear in the MySQL _SPATIAL_TYPES set), but a
    # PG source does NOT substitute them to bytea -- so a point key/index must NOT get the
    # MySQL "convert to bytea" wording. It is surfaced as an UNSUPPORTED column type (store as
    # text) and its index is skipped.
    from dsql_migrator.core.models import IndexDef

    table = TableDef(
        name="public.places",
        columns=[_col("id", "integer", False), _col("loc", "point")],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_loc", columns=["loc"], unique=False)],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("ix_loc" in d for d in r.index_ddls)  # geometric index skipped
    assert not any("bytea" in w.message.lower() for w in r.warnings)  # not mislabeled bytea
    assert any(
        w.column_name == "loc" and w.classification is Classification.UNSUPPORTED
        for w in r.warnings
    )


# --- Tier-4 PG index metadata + relation surfacing ---------------------------


def test_pg_partial_and_nonbtree_indexes_warn_but_still_emit() -> None:
    # A PostgreSQL PARTIAL index (WHERE predicate) and a non-btree method (GIN/GiST/...) have
    # no Aurora DSQL equivalent (btree-only, no partial index). The converter still emits a
    # plain btree/full CREATE INDEX ASYNC (never silently dropped), but MUST warn MANUAL/LOSS
    # -- a partial UNIQUE becoming a FULL unique changes semantics, and a GIN becomes a plain
    # btree that the operators it served cannot use.
    from dsql_migrator.core.models import ConversionNoteKind, IndexDef

    table = TableDef(
        name="public.docs",
        columns=[_col("id", "bigint", False), _col("body", "tsvector"), _col("active", "boolean")],
        primary_key=["id"],
        indexes=[
            IndexDef(name="ix_active", columns=["active"], unique=True, where="active IS true"),
            IndexDef(name="ix_body", columns=["body"], method="gin"),
        ],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    index_warnings = [
        w for w in r.warnings
        if w.classification is Classification.MANUAL
        and w.kind is ConversionNoteKind.LOSS
        and ("partial" in w.message.lower() or "btree" in w.message.lower())
    ]
    assert index_warnings, "expected a partial/non-btree index warning"
    msg = index_warnings[0].message
    assert "ix_active" in msg and "ix_body" in msg and "gin" in msg.lower()


def test_pg_plain_btree_index_produces_no_index_metadata_warning() -> None:
    # A plain (non-partial, btree) index must NOT trigger the partial/non-btree warning.
    from dsql_migrator.core.converter import _pg_partial_or_nonbtree_index_warning
    from dsql_migrator.core.models import IndexDef

    table = TableDef(
        name="public.orders",
        columns=[_col("id", "bigint", False), _col("email", "text")],
        primary_key=["id"],
        indexes=[
            IndexDef(name="ix_email", columns=["email"]),
            IndexDef(name="ix_email2", columns=["email"], method="btree"),  # explicit btree, no predicate
        ],
    )
    assert _pg_partial_or_nonbtree_index_warning(table) is None
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any(
        "partial" in w.message.lower() or "non-btree" in w.message.lower() for w in r.warnings
    )


def test_convert_view_surfaces_matview_unsupported_without_downgrading() -> None:
    # A PostgreSQL materialized view (carried as a ViewDef flagged unsupported_kind) must NOT
    # be transpiled into a plain CREATE VIEW (a silent downgrade); it is surfaced UNSUPPORTED
    # for manual reimplementation.
    from dsql_migrator.core.models import ViewDef

    view = ViewDef(
        name="analytics.daily_totals",
        definition="SELECT count(*) FROM orders",  # a real SELECT, but it is a MATVIEW
        unsupported_kind="materialized view",
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_view(view)
    assert conv.auto_converted is False
    assert "CREATE VIEW" not in conv.target_ddl.upper()
    assert any(
        w.classification is Classification.UNSUPPORTED and "materialized view" in w.message
        for w in conv.warnings
    )
