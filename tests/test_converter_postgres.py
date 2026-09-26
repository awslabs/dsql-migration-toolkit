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
    # An array is SUBSTITUTED here: this string is the conversion's parse INPUT, not the
    # source-DDL panel, and emitting `text[]` made the resulting CREATE TABLE fail at apply
    # ("datatype text[] not supported"). The panel still shows the exact source type.
    assert '"tags" jsonb' in ddl
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
    # An array is now SUBSTITUTED to jsonb (which the loader reads with to_jsonb), so it is
    # a MANUAL type change rather than an UNSUPPORTED dead end -- the DDL applies.
    assert '"tags" JSONB' in conv.target_ddl, conv.target_ddl
    array_warnings = [
        w
        for w in conv.warnings
        if w.column_name == "tags"
        and "converted to jsonb" in w.message
        and w.classification is Classification.MANUAL
        and "array" in w.message.lower()
    ]
    assert len(array_warnings) == 1
    # The supported uuid PK column is not flagged.
    assert not any(w.column_name == "id" for w in conv.warnings)


def test_convert_table_carries_a_simple_pg_default() -> None:
    # FIX 6: PG column DEFAULTs ARE now carried to the target (like the MySQL path). A
    # simple literal default parses fine, so it is emitted -- NOT dropped -- and no
    # default-loss warning fires (Property 6: only a genuinely-undroppable default warns).
    table = TableDef(
        name="events",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="status", mysql_type="text", nullable=False, default="'new'"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert "DEFAULT 'new'" in conv.target_ddl
    assert not [
        w for w in conv.warnings
        if w.column_name == "status" and "default" in w.message.lower()
    ]


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


@pytest.mark.parametrize("interval_type", ["interval(6)", "interval(3)"])
def test_precision_only_interval_emits_parseable_interval(interval_type: str) -> None:
    # FIX 1: a precision-only interval (interval(6)) used to render as the UNPARSABLE
    # "INTERVAL 6" (the (N) modifier becomes a bare integer argument) that DSQL/PostgreSQL
    # reject with a syntax error. Dropping the (N) precision emits a plain INTERVAL: the
    # DDL parses as postgres and carries no "INTERVAL <n>" token.
    from dsql_migrator.core.converter_postgres import _ddl_column_type

    assert _ddl_column_type(interval_type) == "interval"  # (N) stripped
    table = TableDef(
        name="public.durations",
        columns=[_col("id", "bigint", nullable=False), _col("span", interval_type)],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    up = conv.target_ddl.upper()
    assert "INTERVAL" in up
    assert "INTERVAL 6" not in up and "INTERVAL 3" not in up  # the unparsable form is gone
    assert sqlglot.parse_one(conv.target_ddl, read="postgres") is not None  # must parse


def test_pg_source_carries_column_defaults() -> None:
    # FIX 6: PG column DEFAULTs are carried to the target (like the MySQL path); the
    # read="postgres" re-parse normalizes each to DSQL-valid SQL. A serial (nextval)
    # default is NOT emitted (governed by the PK strategy), and the carried columns get no
    # default-loss warning.
    table = TableDef(
        name="public.acct",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="status", mysql_type="character varying(20)",
                      nullable=False, default="'active'::character varying"),
            ColumnDef(name="created_at", mysql_type="timestamp without time zone",
                      nullable=False, default="now()"),
            ColumnDef(name="cnt", mysql_type="integer", nullable=False, default="0"),
            ColumnDef(name="seqcol", mysql_type="integer", nullable=False,
                      default="nextval('acct_seqcol_seq'::regclass)"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    up = conv.target_ddl.upper()
    assert "DEFAULT CAST('ACTIVE' AS VARCHAR)" in up  # 'active'::character varying carried
    assert "DEFAULT CURRENT_TIMESTAMP" in up          # now() carried + normalized
    assert "DEFAULT 0" in up
    assert "NEXTVAL" not in up                         # serial default NOT emitted
    assert sqlglot.parse_one(conv.target_ddl, read="postgres") is not None
    assert not [
        w for w in conv.warnings
        if w.column_name in {"status", "created_at", "cnt"} and "default" in w.message.lower()
    ]


def test_pg_source_unparseable_default_is_dropped_with_a_warning() -> None:
    # FIX 6: a default that does NOT parse as a PostgreSQL expression is dropped (with a
    # MANUAL warning) rather than aborting the whole-table parse; a NOT NULL column
    # escalates (an omitted INSERT is rejected on the target).
    table = TableDef(
        name="public.t",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="c", mysql_type="text", nullable=False, default="((("),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert "could not auto-convert" not in conv.target_ddl.lower()  # table NOT aborted
    assert sqlglot.parse_one(conv.target_ddl, read="postgres") is not None
    warns = [
        w for w in conv.warnings if w.column_name == "c" and "default" in w.message.lower()
    ]
    assert warns and "NOT NULL" in warns[0].message


def test_pg_identity_pk_strategy_applies_when_auto_increment_is_set() -> None:
    # FIX 3: once enrich flags a serial/identity PK (auto_increment_column set), the
    # primary-key strategy governs it -- IDENTITY_WITH_CACHE emits GENERATED ... AS
    # IDENTITY (widened to bigint) and does NOT re-emit the nextval default; KEEP_INTEGER
    # emits the loud "DSQL will NOT auto-generate" RECOMMENDATION and keeps a plain integer.
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    table = TableDef(
        name="public.users",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False,
                      default="nextval('users_id_seq'::regclass)"),
            ColumnDef(name="email", mysql_type="text", nullable=False),
        ],
        primary_key=["id"],
        auto_increment_column="id",  # set by _pg_enrich_columns (see test_source_dialect)
    )
    identity = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
        table, SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE)
    )
    up = identity.target_ddl.upper()
    assert "GENERATED BY DEFAULT AS IDENTITY" in up and "BIGINT" in up
    assert "NEXTVAL" not in up  # governed by the strategy, not emitted as a DEFAULT

    keep = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
        table, SchemaConvertOptions(primary_key_strategy=PrimaryKeyStrategy.KEEP_INTEGER)
    )
    assert any("will NOT auto-generate" in w.message for w in keep.warnings)
    assert "GENERATED" not in keep.target_ddl.upper()


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
    # Aurora DSQL's DOCUMENTED maximum is precision 1000 / scale -1000..1000 (re-verified
    # live: numeric(1000,1000) accepted, numeric(1001,0) rejected). This test read 38/37 --
    # the service's OLD limit -- and so pinned a clamp that silently narrowed a perfectly
    # storable numeric(40,10). Anchored on the constants now, so the next service change
    # cannot leave the assertion asserting a stale quota.
    # clamped (with a warning) -- otherwise the verbatim numeric(40,10) is REJECTED by DSQL
    # at CREATE TABLE with no prior signal. Mirrors the MySQL DECIMAL clamp.
    from dsql_migrator.core.converter_postgres import (
        _DSQL_NUMERIC_MAX_PRECISION as P,
        _DSQL_NUMERIC_MAX_SCALE as S,
    )

    # Well within DSQL: carried VERBATIM, no warning. (This is the case that used to lose
    # digits.)
    assert clamp_pg_numeric("numeric(40,10)") == ("numeric(40,10)", None)
    assert clamp_pg_numeric(f"numeric({P},{S})") == (f"numeric({P},{S})", None)
    # Beyond the documented maximum: clamped, and the warning names what was reduced.
    clamped, note = clamp_pg_numeric(f"numeric({P + 1},10)")
    assert clamped == f"numeric({P},10)" and note and f"precision {P + 1}" in note
    # decimal/dec aliases + precision-only.
    assert clamp_pg_numeric("decimal(50)")[0] == "decimal(50)"  # inside the limit now
    assert clamp_pg_numeric(f"decimal({P + 12})")[0] == f"decimal({P})"
    # In-range, bare, and non-numeric types pass through untouched (no warning).
    for ok in ("numeric(12,2)", f"numeric({P},{S})", "numeric", "uuid", "text"):
        assert clamp_pg_numeric(ok) == (ok, None)


def test_convert_table_clamps_over_precision_pg_numeric_with_warning() -> None:
    from dsql_migrator.core.converter_postgres import (
        _DSQL_NUMERIC_MAX_PRECISION as _P,
    )

    # End-to-end: an over-precision PG numeric converts to a VALID DSQL DDL (clamped to
    # the documented maximum) AND carries a MANUAL warning naming the column, so the
    # fidelity loss is surfaced instead of a silent apply-time CREATE TABLE failure.
    # The bounds come from the constant: this test read 38/37 while the service had already
    # raised the maximum to 1000, so it pinned a clamp that narrowed storable values.
    table = TableDef(
        name="prices",
        columns=[
            _col("id", "bigint", nullable=False),
            # Beyond the DOCUMENTED maximum; numeric(40,10) now converts verbatim.
            _col("huge", f"numeric({_P + 1},10)"),
            _col("wild", f"numeric({_P + 2},{_P + 3})"),
            _col("ok", "numeric(12,2)"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    ddl = conv.target_ddl.lower()  # sqlglot renders numeric as its `decimal` alias
    assert f"decimal({_P}, 10)" in ddl, ddl            # precision clamped to the maximum
    assert f"decimal({_P}, {_P})" in ddl, ddl          # scale never exceeds the precision
    assert f"({_P + 1}" not in ddl and f"({_P + 2}" not in ddl  # originals gone
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
    # FIX 3: enrich sets auto_increment_column ONLY for a detected serial/identity PK. A
    # PLAIN integer PK (no nextval default, no attidentity) is NOT an identity column, so
    # auto_increment_column stays unset and the IDENTITY_WITH_CACHE / CONVERT_TO_UUID PK
    # strategies remain no-ops for it (and the monotonic hot-partition RECOMMENDATION never
    # fires). Pins that a non-identity key is left alone; the identity case is covered by
    # test_pg_identity_pk_strategy_* below.
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
    # The bit column is SUBSTITUTED to text now (it applies, and the loader reads a bit
    # string as its text), so the note is a MANUAL type change -- still naming the original
    # "bit varying" type, which is the half that mattered here.
    assert any(
        w.column_name == "mask"
        and w.classification is Classification.MANUAL
        and "bit varying" in w.message
        for w in r.warnings
    ), r.warnings


def test_bit_varying_ddl_is_emitted_as_parseable_varbit() -> None:
    # T4-6 (unit): the two-word "bit varying"[(n)] is rewritten to the parseable "varbit"
    # alias; a fixed-width bit(n) already parses and is left untouched.
    from dsql_migrator.core.converter_postgres import _ddl_column_type

    # Bit strings are SUBSTITUTED to text now (DSQL has no bit column type, and the loader
    # reads a bit string as its text -- binding it to bytea stored the ASCII digits). The
    # varbit alias rewrite is what kept the table parseable BEFORE the substitution; it is
    # retained for a type the substitution does not cover.
    assert _ddl_column_type("bit varying") == "text"
    assert _ddl_column_type("bit varying(10)") == "text"
    assert _ddl_column_type("bit(8)") == "text"
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
    # No expression was captured, so there is nothing to re-emit -- the note must say
    # WHICH reason applies rather than the old blanket "Aurora DSQL has no equivalent"
    # (it does support STORED; see test_a_stored_generated_column_is_preserved...).
    assert "expression was not captured" in warn[0].message, warn[0].message
    assert "has no equivalent" not in warn[0].message, warn[0].message
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
    # The kind now DECIDES the outcome (Aurora DSQL supports STORED and rejects VIRTUAL),
    # so the note names the specific reason instead of lumping the two kinds together.
    assert "could NOT be preserved" in warn[0].message, warn[0].message
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
    # A bit-varying PRIMARY KEY is now SUBSTITUTED to text, so it really IS an unbounded
    # text key and the key-size estimator's warning is CORRECT -- the contradiction this
    # test was written for (an "applies fine" note next to an UNSUPPORTED column) is gone
    # because the column is no longer unsupported. What must stay true is that the type
    # change is reported once, as MANUAL.
    table = TableDef(
        name="public.t3",
        columns=[_col("flags", "bit varying(64)", nullable=False), _col("note", "text")],
        primary_key=["flags"],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert any(
        w.column_name == "flags"
        and w.classification is Classification.MANUAL
        and "converted to text" in w.message
        for w in r.warnings
    ), r.warnings
    # It is a text key now, so a key-size caution is legitimate; what must NOT happen is
    # the old FALSE alarm shape -- a note claiming the DDL "applies fine" about a column
    # the same conversion called unsupported.
    assert not any(
        w.classification is Classification.UNSUPPORTED for w in r.warnings
    ), r.warnings


@pytest.mark.parametrize("typ", ["bytea[]", "text[]", "bytea"])
def test_pg_index_over_an_unindexable_column_is_not_emitted(typ: str) -> None:
    """An index is skipped when the column's TARGET type cannot be indexed on DSQL.

    Per the DSQL supported-data-types page json/jsonb and bytea have "Index support: No",
    so an array (substituted to jsonb) or a bytea keeps its index skipped rather than
    emitting a doomed post-load CREATE INDEX ASYNC.
    """
    from dsql_migrator.core.models import IndexDef

    table = TableDef(
        name="public.t",
        columns=[_col("id", "bigint", False), _col("c", typ)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_c", columns=["c"], unique=False)],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("ix_c" in d for d in r.index_ddls), r.index_ddls
    assert not any("bytes combined" in w.message for w in r.warnings)


@pytest.mark.parametrize("typ", ["bit varying(64)", "int4range", "inet", "xml"])
def test_pg_index_over_a_substituted_but_indexable_column_IS_emitted(typ: str) -> None:
    """These used to be dropped silently along with the column's "unsupported" verdict.

    Each is now substituted to an INDEXABLE target (text), so the column migrates and its
    index migrates with it -- previously the operator lost the index with no warning, on a
    column the tool then told them to store as text anyway.
    """
    from dsql_migrator.core.models import IndexDef

    table = TableDef(
        name="public.t",
        columns=[_col("id", "bigint", False), _col("c", typ)],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_c", columns=["c"], unique=False)],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert any("ix_c" in d for d in r.index_ddls), r.index_ddls
    # ... and the type change is still reported, as MANUAL rather than UNSUPPORTED.
    assert any(
        w.column_name == "c" and w.classification is Classification.MANUAL
        for w in r.warnings
    ), r.warnings


def test_pg_geometric_key_column_is_not_mislabeled_as_bytea() -> None:
    # PG reuses the names point/polygon (which appear in the MySQL _SPATIAL_TYPES set), but a
    # PG source must NOT get the MySQL "convert to bytea" wording: it is substituted to
    # TEXT (its canonical form), which is indexable -- so unlike the MySQL spatial path the
    # index now survives with the column.
    from dsql_migrator.core.models import IndexDef

    table = TableDef(
        name="public.places",
        columns=[_col("id", "integer", False), _col("loc", "point")],
        primary_key=["id"],
        indexes=[IndexDef(name="ix_loc", columns=["loc"], unique=False)],
    )
    r = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert any("ix_loc" in d for d in r.index_ddls), r.index_ddls  # text IS indexable
    assert not any("bytea" in w.message.lower() for w in r.warnings)  # not mislabeled bytea
    assert '"loc" TEXT' in r.target_ddl, r.target_ddl
    assert any(
        w.column_name == "loc" and w.classification is Classification.MANUAL
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


def test_non_pk_serial_default_is_reported_not_silently_dropped() -> None:
    # The PK strategy governs only `table.auto_increment_column`, and PG enrichment sets
    # that field for a PRIMARY KEY only -- so a non-key sequence column used to reach the
    # target with neither identity nor default and no warning at all.
    table = TableDef(
        name="shop.invoices",
        columns=[
            ColumnDef(
                name="id", mysql_type="bigint", nullable=False,
                default="nextval('invoices_id_seq'::regclass)",
            ),
            ColumnDef(
                name="invoice_no", mysql_type="bigint", nullable=False,
                default="nextval('invoices_invoice_no_seq'::regclass)",
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    # Neither sequence default is re-emitted -- DSQL has no sequence to point at.
    assert "nextval" not in conv.target_ddl
    # Filter on the non-key rule's OWN phrase. "sequence"/"serial" also appear in the
    # primary-key strategy's note (which names the mechanism it ports), so a substring
    # filter counted that note too once the PK note started mentioning them.
    seq_notes = [
        w for w in conv.warnings if "takes its value from a sequence" in w.message
    ]
    assert len(seq_notes) == 1, [w.message for w in conv.warnings]
    note = seq_notes[0]
    assert note.column_name == "invoice_no"  # the NON-key column, not the PK
    assert "writes NULL" in note.message
    # The NOT NULL half is appended by the shared default-loss loop.
    assert "REJECTED on Aurora DSQL" in note.message


def test_pk_serial_default_stays_silent_even_without_enrichment() -> None:
    # Keyed on the primary key, not on auto_increment_column: an inventory that was never
    # enriched (so auto_increment_column is None) must not produce a note claiming the key
    # column is "not the primary key".
    table = TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(
                name="id", mysql_type="bigint", nullable=False,
                default="nextval('orders_id_seq'::regclass)",
            ),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not any("sequence" in w.message for w in conv.warnings)


def test_pg_check_constraint_note_prints_the_expression_and_the_alter() -> None:
    # The expression was captured all along (CheckConstraintDef.expression) but nothing
    # displayed it, and the note pointed at Evaluation, which lists names only.
    from dsql_migrator.core.models import CheckConstraintDef

    table = TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="price", mysql_type="numeric(12,2)"),
        ],
        primary_key=["id"],
        check_constraints=[
            CheckConstraintDef(name="orders_price_check", expression="price > 0::numeric")
        ],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    note = next(w for w in conv.warnings if "CHECK constraint" in w.message)
    assert "price > 0::numeric" in note.message  # the expression itself
    # Identifiers are QUOTED: this statement is the only in-tool route to re-creating the
    # dropped CHECK, so an unquoted mixed-case or spaced name would make the remedy itself
    # a syntax error.
    assert (
        'ADD CONSTRAINT "orders_price_check" CHECK (price > 0::numeric) NOT VALID'
        in note.message
    )
    assert 'ALTER TABLE "shop"."orders"' in note.message
    assert "MySQL" not in note.message  # a PG source is not told about MySQL
    assert "shown in Evaluation" not in note.message  # the old, untrue pointer
    # The CHECK is still not emitted inline: DSQL rejects a plain ADD, and the load is
    # unordered, so it is offered as a post-load statement instead.
    assert "CHECK" not in conv.target_ddl


def test_mysql_check_constraint_note_keeps_its_engine_word() -> None:
    from dsql_migrator.core.models import CheckConstraintDef

    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="price", mysql_type="decimal(12,2)"),
        ],
        primary_key=["id"],
        check_constraints=[CheckConstraintDef(name="ck_price", expression="`price` > 0")],
    )
    conv = SchemaConverter().convert_table(table)
    note = next(w for w in conv.warnings if "CHECK constraint" in w.message)
    assert "a MySQL CHECK expression" in note.message
    assert "`price` > 0" in note.message


def test_pg_collation_is_captured_and_warned_but_mysql_ci_rule_does_not_run() -> None:
    table = TableDef(
        name="shop.people",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="nick", mysql_type="text", collation="ci"),
            ColumnDef(name="plain", mysql_type="text"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    note = next(w for w in conv.warnings if "collation" in w.message)
    assert "nick (ci)" in note.message
    assert "plain" not in note.message
    # Reported even though the name has no MySQL-style _ci suffix: a PG collname does not
    # encode its sensitivity, so any non-default collation is a behaviour change.
    assert "case-INSENSITIVE MySQL collation" not in note.message
    assert "COLLATE" not in conv.target_ddl


def test_pk_strategy_notes_never_say_auto_increment_for_a_pg_source() -> None:
    from dsql_migrator.core.converter import PrimaryKeyStrategy, SchemaConvertOptions

    table = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
        primary_key=["id"],
        auto_increment_column="id",
    )
    for strategy in (
        PrimaryKeyStrategy.CONVERT_TO_UUID,
        PrimaryKeyStrategy.IDENTITY_WITH_CACHE,
        PrimaryKeyStrategy.KEEP_INTEGER,
    ):
        conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
            table, SchemaConvertOptions(primary_key_strategy=strategy)
        )
        messages = " ".join(w.message for w in conv.warnings)
        assert "AUTO_INCREMENT" not in messages, strategy
        assert "serial / identity" in messages, strategy
        # A MySQL source keeps the original wording (a committed snapshot pins it).
        mysql_conv = SchemaConverter().convert_table(
            TableDef(
                name="orders",
                columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False)],
                primary_key=["id"],
                auto_increment_column="id",
            ),
            SchemaConvertOptions(primary_key_strategy=strategy),
        )
        assert "AUTO_INCREMENT column 'id'" in " ".join(
            w.message for w in mysql_conv.warnings
        )


def test_pg_source_gets_an_oversized_lob_note_at_schema_conversion() -> None:
    """Oversized-LOB was the ONLY entry in the converter's optional-warning tuple that was
    suppressed for PostgreSQL with no `_pg_*` counterpart -- and it is the one DSQL limit
    whose breach cannot be fixed by reloading. PG text/bytea are unbounded by DEFAULT, so a
    PG source is strictly MORE likely to hit it than MySQL."""
    table = TableDef(
        name="shop.media",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="content", mysql_type="bytea"),
            ColumnDef(name="body", mysql_type="text"),
            ColumnDef(name="code", mysql_type="character varying(50)"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    note = next(w for w in conv.warnings if "1 MiB per-value" in w.message)
    assert "content (bytea)" in note.message
    assert "body (text)" in note.message
    assert "code" not in note.message  # a bounded varchar cannot exceed the cap
    assert "MySQL" not in note.message
    # Names the in-tool remedy, not only "move it to S3".
    assert "exclude the column" in note.message


def test_mysql_oversized_lob_note_is_unchanged_for_a_mysql_source() -> None:
    table = TableDef(
        name="media",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="content", mysql_type="longblob"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter().convert_table(table)
    note = next(w for w in conv.warnings if "1 MiB per-value" in w.message)
    assert "are MySQL LOB/TEXT types" in note.message
    # And the PG variant does not also fire for a MySQL source.
    assert sum(1 for w in conv.warnings if "1 MiB per-value" in w.message) == 1


def test_non_pk_generated_as_identity_is_reported_not_only_serial() -> None:
    """R-4: the v0.1.492 fix keyed on a `nextval(` DEFAULT, but a PG10+
    `GENERATED ... AS IDENTITY` column has NO pg_attrdef default at all -- so the RECOMMENDED
    spelling still lost its value generation silently while only the legacy `serial` was
    covered."""
    table = TableDef(
        name="shop.invoices",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False, identity=True),
            ColumnDef(
                name="invoice_no", mysql_type="integer", nullable=False, identity=True
            ),
            ColumnDef(
                name="legacy_no", mysql_type="bigint", nullable=False,
                default="nextval('invoices_legacy_no_seq'::regclass)",
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    notes = [
        w for w in conv.warnings if "takes its value from a sequence" in w.message
    ]
    # BOTH non-key spellings are reported; the identity PRIMARY KEY stays silent (the PK
    # strategy governs it).
    assert sorted(n.column_name for n in notes) == ["invoice_no", "legacy_no"]
    assert "nextval" not in conv.target_ddl


def test_pg_oversized_lob_is_advice_not_per_table_manual_work() -> None:
    """R-5: `text` is PostgreSQL's IDIOMATIC string type, so rating each such table MANUAL
    made a plain schema read "Moderate effort" purely because a column has no length limit,
    with no evidence any value approaches 1 MiB -- the same unusable-signal shape this
    release fixed for extension objects."""
    from dsql_migrator.core.assessor_postgres import PgOversizedLobRule
    from dsql_migrator.core.models import ConversionNoteKind, SourceInventory

    table = TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="notes", mysql_type="text"),
        ],
        primary_key=["id"],
    )
    finding = PgOversizedLobRule().evaluate(SourceInventory(tables=[table]))[0]
    assert finding.note_kind is ConversionNoteKind.RECOMMENDATION


def test_an_index_skipped_for_an_unsupported_pg_type_is_named_not_silent() -> None:
    """The skip is deliberate; its SILENCE was the defect.

    The operator reads "remodel inet to text", does exactly that, and had no way to learn
    that the index on it went too — so the post-migration workload falls back to scans with
    nothing in the conversion output about it. The adjacent bytea skip in the same function
    IS reported by name.
    """
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import (
        ColumnDef,
        IndexDef,
        SourceType,
        TableDef,
    )

    table = TableDef(
        name="ecommerce.events",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="client_ip", mysql_type="inet"),
            ColumnDef(name="tags", mysql_type="text[]"),
            ColumnDef(name="note", mysql_type="text"),
        ],
        primary_key=["id"],
        indexes=[
            IndexDef(name="idx_ip", columns=["client_ip"]),
            IndexDef(name="idx_tags", columns=["tags"]),
            IndexDef(name="idx_note", columns=["note"]),
        ],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    # inet is substituted to TEXT, which is indexable, so its index now ships too -- it used
    # to be dropped silently. Only the array (-> jsonb, "Index support: No") is skipped.
    assert len(conv.index_ddls) == 2, conv.index_ddls
    assert any("idx_note" in d for d in conv.index_ddls)
    assert any("idx_ip" in d for d in conv.index_ddls)
    assert not any("idx_tags" in d for d in conv.index_ddls)

    skipped = [w for w in conv.warnings if "were NOT emitted because" in w.message]
    assert len(skipped) == 1, [w.message for w in conv.warnings]
    message = skipped[0].message
    assert "idx_tags (on tags text[])" in message, message
    assert "idx_ip" not in message, message  # it ships now, so it must not be listed
    # Remodelling the column is NOT sufficient -- say so, or the index stays missing.
    assert "not enough on its own" in message, message
    assert "1 secondary index(es)" in message, message


def test_a_table_with_no_unsupported_index_column_gets_no_such_warning() -> None:
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import ColumnDef, IndexDef, SourceType, TableDef

    table = TableDef(
        name="ecommerce.ok",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="note", mysql_type="text"),
        ],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_note", columns=["note"])],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert not [w for w in conv.warnings if "were NOT emitted because" in w.message]


def test_a_postgres_numeric_within_dsqls_documented_maximum_is_never_narrowed() -> None:
    """The clamp used to cut at 38 — a limit the service had already raised to 1000.

    So `numeric(40,10)`, which DSQL stores exactly (verified live: accepted, and a
    500+500-digit value round-tripped EXACT at `numeric(1000,500)`), was silently narrowed
    and lost digits, while the warning asserted a maximum that no longer existed.
    """
    from dsql_migrator.core.converter_postgres import (
        _DSQL_NUMERIC_MAX_PRECISION as P,
        _DSQL_NUMERIC_MAX_SCALE as S,
        clamp_pg_numeric,
    )

    # The documented figures, so a future service change has one place to update.
    assert (P, S) == (1000, 1000), (P, S)
    for spec in ("numeric(40,10)", "numeric(65,30)", f"numeric({P},{S})", "numeric(12,2)"):
        assert clamp_pg_numeric(spec) == (spec, None), spec
    # Only beyond it does the clamp fire, and it says what it reduced.
    clamped, note = clamp_pg_numeric(f"numeric({P + 1},10)")
    assert clamped == f"numeric({P},10)"
    assert note and f"maximum of {P}" in note, note


def test_a_default_cast_to_a_dsql_unsupported_type_is_reported_not_emitted() -> None:
    """A DEFAULT casting to an enum used to be emitted verbatim and fail at apply.

    The column itself is remodelled (`ecommerce.media_type` -> text), but its default kept
    the cast to the now-nonexistent type, so the whole CREATE TABLE was rejected on DSQL
    with "type ecommerce.media_type does not exist" -- a failure the operator had no
    warning about and no way to attribute to the default.
    """
    from dsql_migrator.core.converter_postgres import (
        _pg_default_unsupported_type,
        pg_column_default_sql,
    )
    from dsql_migrator.core.models import ColumnDef

    def default_of(expr: str):
        return pg_column_default_sql(
            ColumnDef(name="media_kind", mysql_type="text", default=expr),
            is_key_column=False,
        )

    for raw in (
        "CAST('image' AS ecommerce.media_type)",
        "'image'::ecommerce.media_type",
    ):
        emitted, note = default_of(raw)
        assert emitted is None, f"{raw} must not be carried into the DDL"
        assert note and "media_type" in note, note
        assert "keep failing even after the column itself is remodelled" in note, note

    # Defaults DSQL accepts are untouched -- including a cast to a supported type.
    for ok in (
        "CURRENT_TIMESTAMP",
        "now()",
        "1",
        "true",
        "'x'::text",
        "CAST(NULL AS VARCHAR)",
        "CAST('a' AS character varying)",
    ):
        assert default_of(ok)[1] is None, ok

    # A nextval default stays attributed to the SEQUENCE. Its `::regclass` is an EXPRESSION
    # cast, not a column type, so the new guard must not claim "DSQL does not support
    # regclass as a column type" and mis-explain the one default that has its own handling.
    seq_note = default_of("nextval('ecommerce.s'::regclass)")[1]
    assert seq_note and "takes its value from a sequence" in seq_note, seq_note
    assert "regclass" not in seq_note, seq_note
    assert _pg_default_unsupported_type("nextval('ecommerce.s'::regclass)") is None


def test_an_over_long_character_length_is_clamped_instead_of_failing_at_apply() -> None:
    """A PG varchar/char above DSQL's declared-length ceiling broke the whole CREATE TABLE.

    Live-verified on Aurora DSQL: ``VARCHAR(65536)`` -> ``Datatype limit greater than 65535
    bytes not supported for varchar``; ``CHAR(4097)`` -> the same for char. PostgreSQL
    allows ~1 GB, so such a column made the table's CREATE fail at APPLY with nothing
    having warned -- and ``dsql_lint`` returns 0 diagnostics on it, so the linter does not
    catch it either.
    """
    from dsql_migrator.core.converter_postgres import (
        _DSQL_MAX_CHAR_LENGTH,
        _DSQL_MAX_VARCHAR_LENGTH,
        clamp_pg_character,
    )

    # The documented/verified ceilings, pinned so a service change has one place to update.
    assert (_DSQL_MAX_VARCHAR_LENGTH, _DSQL_MAX_CHAR_LENGTH) == (65535, 4096)

    # Within the ceiling (and the boundary itself) passes through UNTOUCHED.
    for ok in (
        "character varying",
        "varchar",
        "character varying(1)",
        f"character varying({_DSQL_MAX_VARCHAR_LENGTH})",
        f"char({_DSQL_MAX_CHAR_LENGTH})",
        "character(1)",
        "text",
        "numeric(12,2)",
    ):
        assert clamp_pg_character(ok) == (ok, None), ok

    # Above it becomes text -- NOT the clamped length, which would reject values the source
    # legitimately holds.
    for over, kind in (
        (f"character varying({_DSQL_MAX_VARCHAR_LENGTH + 1})", "varchar"),
        ("character varying(200000)", "varchar"),
        ("varchar(90000)", "varchar"),
        (f"character({_DSQL_MAX_CHAR_LENGTH + 1})", "char"),
        ("bpchar(9000)", "char"),
    ):
        clamped, note = clamp_pg_character(over)
        assert clamped == "text", over
        assert note and f"declared {kind} length above" in note, note
        assert "CHECK (length(col) <=" in note, note
    # A fixed-length column also loses its blank padding; a varchar has none to lose.
    assert "blank padding" in clamp_pg_character("character(9000)")[1]
    assert "blank padding" not in clamp_pg_character("character varying(90000)")[1]


def test_the_over_long_character_clamp_reaches_the_emitted_ddl_and_a_warning() -> None:
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

    table = TableDef(
        name="ecommerce.wide",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="blurb", mysql_type="character varying(200000)"),
            ColumnDef(name="code", mysql_type="character(8192)"),
            ColumnDef(name="ok", mysql_type="character varying(100)"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert '"blurb" TEXT' in conv.target_ddl, conv.target_ddl
    assert '"code" TEXT' in conv.target_ddl, conv.target_ddl
    # The one within the ceiling keeps its declared length.
    assert '"ok" VARCHAR(100)' in conv.target_ddl, conv.target_ddl
    clamped = {w.column_name for w in conv.warnings if "converted to text" in w.message}
    assert clamped == {"blurb", "code"}, clamped

    # Evaluation must say the same thing, so the two steps cannot disagree.
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.models import SourceInventory

    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    finding = next(i for i in report.items if i.rule_id == "PG_CHARACTER_LENGTH")
    assert "blurb" in finding.risk and "code" in finding.risk
    assert "ok" not in finding.risk.replace("blurb", "").replace("code", "")
    assert "65535" in finding.risk and "4096" in finding.risk


def test_a_stored_generated_column_is_preserved_with_its_expression_verbatim() -> None:
    """The tool said "Aurora DSQL has no equivalent" and dropped the clause. It was FALSE.

    Live-verified on a real Aurora DSQL cluster: ``GENERATED ALWAYS AS (expr) STORED`` is
    accepted, the value is computed on INSERT (3 x 2.50 -> 7.50) and RECOMPUTED on UPDATE
    (4 x 2.50 -> 10.00), ``pg_attribute.attgenerated`` is ``'s'``, and an INSERT that
    supplies a value is rejected. VIRTUAL is a syntax error. So the operator was told to
    rewrite their application for a feature the target has -- irreversibly, because DSQL can
    DROP an expression from a column but never ADD one.

    The expression is injected as RAW TEXT, never re-rendered by sqlglot, which MANGLES real
    ``pg_get_expr`` output. Each shape below was taken from PostgreSQL's own catalog output
    and breaks a different way through sqlglot -- the last one raises ParseError, which would
    abort the WHOLE table and take every other column with it.
    """
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

    def table(expression: str, *, kind: str = "STORED", pk: str = "id") -> TableDef:
        return TableDef(
            name="ecommerce.order_items",
            columns=[
                ColumnDef(name="id", mysql_type="bigint", nullable=False),
                ColumnDef(name="quantity", mysql_type="integer", nullable=False),
                ColumnDef(name="unit_price", mysql_type="numeric(10,2)", nullable=False),
                ColumnDef(name="tags", mysql_type="jsonb"),
                ColumnDef(name="d", mysql_type="date"),
                ColumnDef(name="name", mysql_type="text"),
                ColumnDef(
                    name="g",
                    mysql_type="numeric(12,2)",
                    generated=True,
                    generated_kind=kind,
                    generated_expression=expression,
                ),
            ],
            primary_key=[pk],
        )

    for expression in (
        "((quantity)::numeric * unit_price)",
        "(tags #>> '{a,b}'::text[])",
        "((tags -> 'a'::text) ->> 'b'::text)",
        "date_part('year'::text, d)",
        "((name IS NOT NULL))::integer",
        "CASE WHEN quantity > 0 THEN upper(name) ELSE NULL::text END",
    ):
        ddl = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
            table(expression)
        ).target_ddl
        assert f"GENERATED ALWAYS AS ({expression}) STORED" in ddl, (expression, ddl)

    # VIRTUAL is NOT preserved (DSQL rejects it outright) ...
    virtual = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
        table("(quantity * 2)", kind="VIRTUAL")
    )
    assert "GENERATED ALWAYS AS" not in virtual.target_ddl, virtual.target_ddl
    # ... and a generated column that is part of the PRIMARY KEY is not either: the batch
    # INSERT must omit a generated column, while every key column has to be read back to
    # key the idempotent write, so preserving it would make the table unloadable.
    keyed = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(
        table("(quantity * 2)", pk="g")
    )
    assert "GENERATED ALWAYS AS" not in keyed.target_ddl, keyed.target_ddl

    # A MySQL source can never reach this: introspection does not read
    # GENERATION_EXPRESSION, so there is no expression and nothing changes for it.
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    mysql_shaped = TableDef(
        name="shop.t",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="g", mysql_type="int", generated=True),
        ],
        primary_key=["id"],
    )
    assert pg_preserved_generated_columns(mysql_shaped) == []


def test_the_applied_ddls_generated_columns_are_readable_back() -> None:
    """The loader MUST know which columns the target computes, or every batch dies (428C9).

    The parser has to survive the identity CACHE clause the converter also injects raw --
    sqlglot's postgres reader raises on it, which would report "no generated columns" for
    exactly the tables most likely to have one.
    """
    from dsql_migrator.core.converter import (
        SchemaConverter,
        parse_target_generated_columns,
    )
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

    table = TableDef(
        name="ecommerce.order_items",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False, identity=True),
            ColumnDef(name="quantity", mysql_type="integer", nullable=False),
            ColumnDef(
                name="line_total",
                mysql_type="numeric(12,2)",
                generated=True,
                generated_kind="STORED",
                generated_expression="((quantity)::numeric * 2)",
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    ddl = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table).target_ddl
    # The DDL carries BOTH raw-injected clauses.
    assert "GENERATED BY DEFAULT AS IDENTITY (CACHE" in ddl
    assert "GENERATED ALWAYS AS" in ddl
    assert parse_target_generated_columns(ddl) == {"line_total"}

    # An identity column alone is NOT a generated column.
    assert parse_target_generated_columns(
        'CREATE TABLE t (id bigint NOT NULL GENERATED BY DEFAULT AS IDENTITY (CACHE 65536),'
        ' q int, PRIMARY KEY (id))'
    ) == set()
    # A hand-edited DDL sqlglot parses structurally works too.
    assert parse_target_generated_columns(
        "CREATE TABLE t (id int PRIMARY KEY, q int, g int GENERATED ALWAYS AS (q*2) STORED)"
    ) == {"g"}
    # Unparseable DDL means UNKNOWN, which must read as "send the column" (loud 428C9)
    # rather than "skip it" (silent NULLs).
    assert parse_target_generated_columns("NOT SQL AT ALL (((") == set()


def test_a_stored_kind_without_a_captured_expression_is_not_preserved() -> None:
    """The expression gate is what keeps a MySQL source out, and it must stand alone.

    MySQL introspection sets ``generated=True`` but never reads GENERATION_EXPRESSION, so
    there is nothing to emit. A column claiming STORED with no expression must fall to the
    ordinary path rather than emit ``GENERATED ALWAYS AS () STORED``.
    """
    from dsql_migrator.core.converter import (
        SchemaConverter,
        pg_preserved_generated_columns,
    )
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

    table = TableDef(
        name="ecommerce.t",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(
                name="g", mysql_type="integer", generated=True, generated_kind="STORED"
            ),
        ],
        primary_key=["id"],
    )
    assert pg_preserved_generated_columns(table) == []
    ddl = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table).target_ddl
    assert "GENERATED ALWAYS AS" not in ddl, ddl


def test_a_postgres_enum_column_converts_to_text_with_its_values_preserved() -> None:
    """The finding named a type nobody could act on, and the DDL could not be applied.

    `format_type` returns only a user-defined type's NAME, so `ecommerce.order_status` was
    indistinguishable from a composite or a domain: the message had to hedge across all
    three, never said the real reason, and never showed the labels -- the ONE thing needed
    to remodel the column. Meanwhile the DDL emitted the enum type verbatim, and Aurora DSQL
    has no CREATE TYPE (live-verified: `CREATE TYPE ... AS ENUM` -> "CREATE TYPE not
    supported"), so the whole CREATE TABLE was rejected at apply.

    With the labels introspected the column now takes the same faithful port MySQL's ENUM
    has always had. Live-verified end to end on a real DSQL cluster: the converted DDL
    applies, the DEFAULT fills, a valid label inserts, an invalid one is rejected by the
    CHECK.
    """
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

    labels = ("pending", "processing", "shipped", "delivered", "cancelled")
    table = TableDef(
        name="ecommerce.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(
                name="status",
                mysql_type="ecommerce.order_status",
                type_kind="enum",
                enum_labels=labels,
                default="'pending'::ecommerce.order_status",
            ),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)

    line = next(l.strip().rstrip(",") for l in conv.target_ddl.splitlines() if '"status"' in l)
    # text, not the enum type -- the type cannot be created on the target.
    assert "ecommerce.order_status" not in line, line
    assert '"status" TEXT' in line, line
    # ... and the value domain survives as a CHECK over the SAME labels.
    for label in labels:
        assert f"'{label}'" in line, (label, line)
    # ... and the DEFAULT is carried as a plain text literal (the cast to the enum type
    # would have made the CREATE fail even after the column was remodelled).
    assert "DEFAULT 'pending'" in line, line
    assert "::ecommerce.order_status" not in line, line

    # It is no longer graded UNSUPPORTED (nothing prevents the migration now), and the
    # note leads with the OUTCOME rather than with "DSQL cannot".
    note = next(w for w in conv.warnings if w.column_name == "status")
    assert note.classification is Classification.MANUAL, note.classification
    assert note.target_type == "text"
    assert note.message.startswith("Column 'status' was converted"), note.message
    assert "preserves the SAME allowed values" in note.message
    assert "DEFAULT is carried over" in note.message
    # The two real consequences must both be stated.
    assert "DECLARATION order" in note.message
    assert "ADD CONSTRAINT" in note.message or "ADD/DROP" in note.message.upper()


def test_a_user_defined_type_message_names_its_KIND_not_a_type_called_enum() -> None:
    """PostgreSQL has NO type spelled `enum` -- live-verified: `CREATE TABLE t (c enum)`
    fails with `type "enum" does not exist`, and `pg_type` holds no row named 'enum'. So a
    message may describe the KIND ("a user-defined enumerated type") but must never present
    it as a type name. The kinds also have DIFFERENT outcomes, which is why they cannot stay
    collapsed: DSQL has no CREATE TYPE (so enum/composite/range cannot exist) but it DOES
    support CREATE DOMAIN.
    """
    from dsql_migrator.core.converter_postgres import unsupported_dsql_reason

    enum_reason = unsupported_dsql_reason(
        "ecommerce.order_status", type_kind="enum", enum_labels=("a", "b")
    )
    assert "user-defined ENUMERATED type" in enum_reason
    assert "no CREATE TYPE" in enum_reason
    assert "a, b" in enum_reason  # the labels, in order
    assert "the type 'enum'" not in enum_reason
    assert "the PostgreSQL type 'enum'" not in enum_reason

    composite = unsupported_dsql_reason("ecommerce.addr_type", type_kind="composite")
    assert "user-defined COMPOSITE type" in composite
    assert "no CREATE TYPE" in composite

    # A DOMAIN is NOT in the same boat: DSQL supports CREATE DOMAIN (live-verified --
    # created, used as a column type, and an out-of-range value rejected by its CHECK), so
    # the message must not claim the type is unsupported.
    domain = unsupported_dsql_reason("ecommerce.email_address", type_kind="domain")
    assert "supports CREATE DOMAIN" in domain
    assert "does not support the PostgreSQL type" not in domain
    assert "no CREATE TYPE" not in domain

    # With the kind UNKNOWN (an un-enriched inventory) it must hedge honestly rather than
    # assert one kind.
    unknown = unsupported_dsql_reason("ecommerce.order_status")
    assert "If it is a user-defined type" in unknown


def test_only_a_label_of_the_type_itself_is_carried_as_the_enum_default() -> None:
    """Rewriting any cast would put a value the new CHECK rejects into the DDL.

    The DEFAULT is only unwrapped when the literal is one of the type's OWN labels, so a
    default that the source allowed but the converted CHECK would not is left to the
    existing "not carried" path instead of becoming an INSERT that fails at run time.
    """
    from dsql_migrator.core.converter_postgres import (
        _pg_enum_default_literal,
        pg_column_default_sql,
    )
    from dsql_migrator.core.models import ColumnDef

    labels = ("pending", "shipped")
    # Both cast spellings PostgreSQL uses, for a value that IS a label.
    assert _pg_enum_default_literal("'pending'::ecommerce.order_status", labels) == "'pending'"
    assert (
        _pg_enum_default_literal("CAST('shipped' AS ecommerce.order_status)", labels)
        == "'shipped'"
    )
    # A literal that is NOT one of the labels must NOT be rewritten.
    assert _pg_enum_default_literal("'archived'::ecommerce.order_status", labels) is None
    # Nor an arbitrary expression that merely carries a cast.
    assert _pg_enum_default_literal("lower('PENDING')::ecommerce.order_status", labels) is None
    assert _pg_enum_default_literal("CURRENT_TIMESTAMP", labels) is None

    # End to end through the real default path: a non-label default falls through to the
    # unsupported-cast branch and is reported, not silently emitted.
    column = ColumnDef(
        name="status",
        mysql_type="ecommerce.order_status",
        type_kind="enum",
        enum_labels=labels,
        default="'archived'::ecommerce.order_status",
    )
    emitted, note = pg_column_default_sql(column, is_key_column=False)
    assert emitted is None
    assert note and "ecommerce.order_status" in note, note


def test_a_dsql_unsupported_pg_type_is_substituted_so_the_ddl_applies() -> None:
    """Schema Apply failed with "datatype text[] not supported" on a remodel the tool
    itself recommended and the loader was already prepared for.

    Evaluation said UNSUPPORTED and named the target (array -> jsonb, numrange -> text,
    money -> numeric, ...), but the converter emitted the source type verbatim on the ground
    that "v1 does not auto-substitute" -- so the CREATE TABLE was guaranteed to be rejected.
    The data path has read these targets since ``_pg_read_expression`` existed
    (``to_jsonb`` for an array, ``CAST(... AS numeric)`` for money, a text cast otherwise),
    so the substitution is what the tool was already promising.

    Verified end to end on a live Aurora DSQL cluster: the converted products DDL applies,
    and a row read through the tool's own SELECT expressions inserts -- tags land as a jsonb
    array (jsonb_array_length = 3) and numrange as '[300,400)'.
    """
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.converter_postgres import substitute_pg_unsupported_type
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

    for source, target in (
        ("text[]", "jsonb"),
        ("integer[]", "jsonb"),
        ("numrange", "text"),
        ("int4range", "text"),
        ("money", "numeric"),
        ("inet", "text"),
        ("cidr", "text"),
        ("xml", "text"),
        ("bit(8)", "text"),
        ("bit varying", "text"),
        ("tsvector", "text"),
        ("point", "text"),
    ):
        got, note = substitute_pg_unsupported_type(source)
        assert got == target, (source, got)
        assert note and f"converted to {target}" in note, (source, note)
        # The note must say what the application has to change, not just the new type.
        assert "application" in note, (source, note)

    # Types DSQL supports are untouched, and so is a user-defined kind (an enum has its own
    # text+CHECK path; a composite/domain must not be guessed at).
    for unchanged in ("text", "numeric(12,2)", "jsonb", "bytea", "timestamp with time zone"):
        assert substitute_pg_unsupported_type(unchanged) == (None, None), unchanged
    # A user-defined kind is excluded even when its NAME collides with a substitutable base
    # type (a domain may legally be called `money`): an enum has its own text+CHECK path,
    # and a composite/domain must not be silently retyped on a name match.
    for kind in ("enum", "composite", "domain"):
        assert substitute_pg_unsupported_type("ecommerce.x", type_kind=kind) == (None, None)
        assert substitute_pg_unsupported_type("money", type_kind=kind) == (None, None), kind
        assert substitute_pg_unsupported_type("inet", type_kind=kind) == (None, None), kind

    # End to end: the emitted DDL carries the substitutes, reported as MANUAL type changes
    # rather than UNSUPPORTED dead ends.
    table = TableDef(
        name="ecommerce.products",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="tags", mysql_type="text[]"),
            ColumnDef(name="price_band", mysql_type="numrange"),
            ColumnDef(name="ip", mysql_type="inet"),
        ],
        primary_key=["id"],
    )
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert '"tags" JSONB' in conv.target_ddl, conv.target_ddl
    assert '"price_band" TEXT' in conv.target_ddl, conv.target_ddl
    assert '"ip" TEXT' in conv.target_ddl, conv.target_ddl
    assert not any(
        w.classification is Classification.UNSUPPORTED for w in conv.warnings
    ), conv.warnings


def test_the_read_expression_matches_the_type_the_converter_now_emits() -> None:
    """The substitution is only safe because the EXPORTER reads into the new type.

    ``_pg_read_expression`` keys on the APPLIED target type, so if the converter's choice
    and the reader's expectation ever diverge the value arrives in the source type and DSQL
    rejects it with an opaque driver error -- the exact failure the substitution is meant to
    remove. This pins them together.
    """
    from dsql_migrator.core.converter import SchemaConverter, parse_target_column_types
    from dsql_migrator.core.converter_postgres import substitute_pg_unsupported_type
    from dsql_migrator.core.models import ColumnDef, SourceType, TableDef
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    columns = [ColumnDef(name="id", mysql_type="bigint", nullable=False)] + [
        ColumnDef(name=f"c{i}", mysql_type=t)
        for i, t in enumerate(("text[]", "numrange", "money", "inet"))
    ]
    table = TableDef(name="s.t", columns=columns, primary_key=["id"])
    conv = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    applied = parse_target_column_types(conv.target_ddl)
    dialect = PostgresSourceDialect()

    for column in columns[1:]:
        expected = substitute_pg_unsupported_type(column.mysql_type)[0]
        # The applied DDL really carries the substitute (this is what the loader reads).
        # `numeric` round-trips through sqlglot as its `decimal` alias, which the reader
        # accepts as the same target -- so the pair is treated as equivalent here.
        equivalent = {"numeric": {"numeric", "decimal"}}.get(expected, {expected})
        assert applied.get(column.name) in equivalent, (column.mysql_type, applied)
        read = dialect.select_column_sql(column, target_type=applied.get(column.name))
        if expected == "jsonb":
            assert "to_jsonb(" in read, read
        elif expected == "numeric":
            assert "AS numeric)" in read, read
        else:
            assert "AS text)" in read, read
