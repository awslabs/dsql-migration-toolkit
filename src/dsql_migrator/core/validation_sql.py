# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure cross-engine SQL builders for validation (extracted from ``validator.py``).

These helpers construct the MySQL and PostgreSQL/DSQL SQL the
:class:`~dsql_migrator.core.validator.Validator` runs -- per-table checksums,
primary-key sample / keyset-page tokens, and the orphan-count query -- plus the
PK-classification helpers (:func:`integer_pk_column` / :func:`single_pk_column`).

They are pure: each takes a :class:`~dsql_migrator.core.models.TableDef` /
:class:`~dsql_migrator.core.models.ColumnDef` (or a :class:`ForeignKeyDef`) and
returns a ``str`` / :class:`psycopg.sql.Composed`. They touch no connection,
thread, or run state, so they are unit-tested directly. See ``validator.py``'s
module docstring for the cross-engine checksum-normalization rationale.
"""

from __future__ import annotations

import re
from typing import Mapping, Optional

from psycopg import sql

from dsql_migrator.core.converter import is_spatial_mysql_type, map_mysql_type
from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, TableDef

# A length/precision/scale modifier "(N)" / "(p,s)" anywhere in a type string, used to
# reduce a format_type spelling to its base for a timetz classification (e.g.
# "time(6) with time zone" -> "time with time zone"). Mirrors converter_postgres.
_TYPE_MODIFIER_RE = re.compile(r"\(\s*\d+\s*(?:,\s*\d+\s*)?\)")

# Number of leading MD5 hex digits used to build a per-row checksum token. 15
# hex digits = 60 bits, which stays positive in both MySQL's unsigned CONV and
# PostgreSQL's signed bigint, so equal data yields equal checksums on both.
_CHECKSUM_HEX_DIGITS = 15

# PostgreSQL / Aurora DSQL cap a function call at FUNC_MAX_ARGS = 100 arguments, so
# ``concat_ws('|', v1, ..., vN)`` (N+1 args) fails at N >= 100 with "cannot pass more
# than 100 arguments to a function". A table can legitimately have up to 255 columns
# (see the assessor's column limit), so a wide table's per-row checksum would error on
# the target. When the term count would exceed the limit, the per-row token NESTS MD5s:
# hash each group of <= _CONCAT_MAX_TERMS rendered columns, then MD5 the group hashes.
# The nesting is applied IDENTICALLY on both engines so equal data still hashes equally
# (MySQL's own argument limit is higher, but the nested shape MUST match to compare).
# Group MD5s are 32 lowercase hex chars on both engines (no '|'/'~'), so the outer
# concat needs no re-escaping and stays injective; group order is fixed, so the nested
# form is exactly as deterministic as the flat one. A count <= _CONCAT_MAX_TERMS emits
# the ORIGINAL flat SQL unchanged, so narrow tables (and their golden tests) don't move.
_CONCAT_MAX_TERMS = 96


def _quote_mysql_identifier(name: str) -> str:
    """Quote a MySQL identifier with backticks, escaping embedded backticks."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _quote_mysql_table(name: str) -> str:
    """Quote a possibly schema-qualified table name as ``\\`schema\\`.\\`table\\```.

    Cluster-wide introspection qualifies names as ``database.table``; quoting the
    whole string as one identifier yields ``\\`database.table\\``` which MySQL
    reads as a single table in the (unset) current database -- causing "1046, No
    database selected". Split on the first dot so each part is quoted
    independently; an unqualified name quotes as before.
    """
    schema, separator, obj = name.partition(".")
    if separator and schema and obj:
        return f"{_quote_mysql_identifier(schema)}.{_quote_mysql_identifier(obj)}"
    return _quote_mysql_identifier(name)


def _pg_table_identifier(name: str) -> "sql.Identifier":
    """Return a psycopg identifier for a possibly schema-qualified table name.

    Splits ``schema.table`` so it composes to ``"schema"."table"`` rather than a
    single quoted ``"schema.table"`` identifier (which would not exist).
    """
    schema, separator, obj = name.partition(".")
    if separator and schema and obj:
        return sql.Identifier(schema, obj)
    return sql.Identifier(name)


# Cross-engine NULL sentinel for checksum/PK-token rendering. It MUST be
# backslash-free, NUL-free, and separator('|')-free so it parses to the SAME
# bytes under MySQL's backslash-escaping AND PostgreSQL's standard_conforming_
# strings (DSQL default). The old '\0' emitted a single NUL on MySQL but the
# two-char string 0x5C30 on PG, so any NULL-bearing row hashed differently on
# each engine -- the confirmed migration_edge.edge_text false-mismatch. NUL is
# also invalid in PG text, so it is correctly avoided.
# NULL sentinel. Must be UN-forgeable by a real (escaped) value: the per-value
# escape below turns every '~' into '~~', so an escaped real value never contains a
# LONE '~' followed by a non-'~'/non-'|' char. '~N' is exactly that shape, so no real
# value can produce it -- closing the old '<NULL>'-vs-literal-'<NULL>' collision.
# Backslash-free and NUL-free so it is byte-identical under MySQL backslash-escaping
# AND PostgreSQL standard_conforming_strings (see the history note that motivated it).
_NULL_SENTINEL = "~N"

# Concat-separator escape. CONCAT_WS('|', ...) joins with '|', so a value CONTAINING
# '|' could shift a delimiter across a column boundary -- CONCAT_WS('|','a|','b') and
# CONCAT_WS('|','a','|b') both yield 'a||b', a within-row token collision (false MATCH
# over unequal data). Escaping each value ('~'->'~~' then '|'->'~|') leaves the
# separator as the ONLY unescaped '|', making the concatenation injective. Backslash-
# free by design (a backslash scheme diverged between MySQL and PG before -- see the
# sentinel note). Applied IDENTICALLY on both engines so equal data still hashes equally.


def _mysql_concat_term(expr: str) -> str:
    """Wrap a MySQL render expr: escape the separator, then COALESCE NULL -> sentinel."""
    return (
        f"COALESCE(REPLACE(REPLACE({expr}, '~', '~~'), '|', '~|'), "
        f"'{_NULL_SENTINEL}')"
    )


def _pg_concat_term(expr: "sql.Composed") -> "sql.Composed":
    """PG counterpart of :func:`_mysql_concat_term` (byte-identical escaping)."""
    return sql.SQL(
        "COALESCE(replace(replace({e}, '~', '~~'), '|', '~|'), {s})"
    ).format(e=expr, s=sql.Literal(_NULL_SENTINEL))


# Aurora DSQL stores an UNCONSTRAINED ``numeric`` (no declared precision/scale) at its
# documented default of ``numeric(18,6)`` -- so a PostgreSQL-source unconstrained numeric
# lands with 6 fractional digits on the target. The checksum rounds such a column to this
# scale on both sides so equal values match (see ``_pg_checksum_expr``).
_DSQL_DEFAULT_NUMERIC_SCALE = 6


def _decimal_scale(mysql_type: str) -> int:
    """Return the declared scale of a MySQL DECIMAL(p,s) (default 0).

    ``DECIMAL`` / ``DECIMAL(p)`` have scale 0; ``DECIMAL(p, s)`` returns ``s``.
    Used to pin a canonical fixed-scale rendering on BOTH engines so a stored-
    scale / trailing-zero difference cannot cause a cross-engine false-mismatch.
    """
    inside = mysql_type.partition("(")[2].partition(")")[0]
    parts = [p.strip() for p in inside.split(",") if p.strip()]
    if len(parts) >= 2:
        try:
            return int(parts[1])
        except ValueError:
            return 0
    return 0


def _numeric_render_scale(column: "ColumnDef", source_is_postgres: bool) -> int:
    """Fixed scale to render a ``numeric`` column at, on BOTH engines, for the checksum.

    A DECLARED scale -- ``numeric(p,s)`` / ``numeric(p)`` (== scale 0) / any MySQL
    ``DECIMAL(p[,s])`` -- is rendered at that exact scale so a stored-scale / trailing-zero
    difference can never false-mismatch cross-engine.

    An UNCONSTRAINED ``numeric`` (bare, no parens -> arbitrary precision AND scale) exists
    ONLY for a PostgreSQL source. Aurora DSQL stores such a column at its DEFAULT
    ``numeric(18,6)``, so a source value is rounded to 6 fractional digits on the target
    (0.5 -> 0.500000). Rounding BOTH sides to 6 lets an equal value match despite the
    source's arbitrary scale, while a difference WITHIN what DSQL can store is still caught
    (scale 0 -- the old bug -- hid the whole fraction -> false MATCH; raw ``::text``
    false-MISMATCHED 0.5 vs 0.500000). That >6-digit rounding is a SCHEMA decision surfaced
    at Schema Conversion (the bare-``numeric`` warning; see
    ``converter_postgres.unconstrained_numeric_note``), not a hidden Validation mask.

    ``source_is_postgres`` gates the scale-6 default so it NEVER applies to a MySQL source:
    MySQL's paren-less spellings include ``bigint unsigned`` (MySQL 8.0.19+ / 8.4 / Aurora
    MySQL 3 report ``COLUMN_TYPE`` with no display width), which maps to ``numeric(20,0)``
    -- an integer. The unchanged MySQL source side (:func:`_mysql_checksum_expr`) renders it
    at scale 0, so the target side must too, or every ``BIGINT UNSIGNED`` row would
    false-mismatch. (MySQL 5.7 spells it ``bigint(20) unsigned`` -- parens -> scale 0
    already -- which is why that path was never observed to break.)
    """
    if source_is_postgres and "(" not in column.mysql_type:
        return _DSQL_DEFAULT_NUMERIC_SCALE
    return _decimal_scale(column.mysql_type)


def has_numeric_checksum_column(table: TableDef) -> bool:
    """Does ``table`` hold a column the CHECKSUM renders through ``round()``?

    ``numeric`` is the ONLY render family whose expression DISCARDS information: every
    other kind casts losslessly (``::text``, ``encode(...,'hex')``, ``to_char(...'US')``),
    while the numeric arm rounds to a FIXED scale on both engines
    (:func:`_numeric_render_scale`). Used to scope the stale-scale check
    (:func:`stale_numeric_render_scales`) to the only tables where a stale declared
    scale can hide a difference, so a table with no numeric column costs no extra read.
    """
    return any(_checksum_kind(column) == "numeric" for column in table.columns)


def stale_numeric_render_scales(
    table: TableDef,
    fresh_types: "Mapping[str, str]",
    source_is_postgres: bool,
) -> "list[tuple[str, str, str]]":
    """Numeric columns whose render scale disagrees with the CURRENT source type. Pure.

    Returns ``[(column, inventory_type, fresh_type)]`` -- empty when every numeric
    column still renders at the same scale.

    Why this has to be checked. The numeric arm of :func:`_pg_checksum_expr` rounds
    BOTH ends to :func:`_numeric_render_scale`, which is derived from
    ``column.mysql_type`` -- the spelling captured at Step 1 (Evaluation) and then
    persisted with the session. That rounding is DELIBERATE (a stored-scale /
    trailing-zero difference, and DSQL's ``numeric(18,6)`` default for an unconstrained
    source ``numeric``, must still compare equal) and is NOT removed here. But if the
    source column's DECLARED scale changed after Step 1 -- e.g. ``ALTER ... TYPE
    numeric(12,4)`` over a target still declared ``numeric(12,2)`` -- both ends get
    rounded to the OLD scale 2 and the extra digits are discarded on BOTH sides, so
    unequal data hashes EQUALLY. Measured on PostgreSQL 17.11 with this module's own
    builder: source ``numeric(12,4)`` 1.2345/9.8765/5.0000 vs target ``numeric(12,2)``
    1.23/9.88/5.00 both checksummed ``2909180158483770255`` (a MATCH over data that
    differs in 2 of 3 rows); rendering from the FRESH types gave
    ``2657482466277275930`` vs ``2909180158483770255``, i.e. the correct divergence.

    ``fresh_types`` maps column name -> the source's CURRENT declared type, read from
    the catalog at validation time in the SAME spelling Step 1 records (MySQL
    ``COLUMN_TYPE``; PostgreSQL ``format_type(atttypid, atttypmod)``) -- verified
    byte-identical across 151 columns / 28 tables on PostgreSQL 17.11, including
    domains, enums, arrays, ``bit varying`` and ``interval day to second(3)``. Only
    the derived SCALE is compared, never the raw strings, so a harmless spelling or
    case difference can never raise a false alarm. A column missing from
    ``fresh_types`` is skipped (nothing to compare against).
    """
    stale: list[tuple[str, str, str]] = []
    for column in table.columns:
        if _checksum_kind(column) != "numeric":
            continue
        fresh = fresh_types.get(column.name)
        if not fresh or fresh == column.mysql_type:
            continue
        recorded = _numeric_render_scale(column, source_is_postgres)
        current = _numeric_render_scale(
            column.model_copy(update={"mysql_type": fresh}), source_is_postgres
        )
        if recorded != current:
            stale.append((column.name, column.mysql_type, fresh))
    return stale


def _checksum_kind(column: "ColumnDef", source_is_postgres: bool = False) -> str:
    """Classify ``column`` into the render family used by the checksum builders.

    Returns one of: ``"binary"`` (bytea/spatial WKB), ``"bit"`` (BIT(n) ->
    integer target), ``"boolean"``, ``"timestamp"`` / ``"timestamptz"`` /
    ``"time"`` (temporal), ``"numeric"`` (DECIMAL), ``"float"`` (FLOAT/DOUBLE --
    rendered via ``to_jsonb`` for a PostgreSQL source, where one expression serves both
    ends, and excluded for a MySQL source whose float text is not that text),
    ``"json"`` (always excluded -- no byte-identical cross-engine
    text form), or ``"plain"`` (all safe types rendered by the engine's native text
    cast). Reuses the SAME converter classification the Full
    Load loader used to STORE the value (converter.map_mysql_type / the exporter's
    _target_kind), so the rendered text matches the stored value on the target.
    """
    mysql_type = column.mysql_type
    base = mysql_type.strip().lower().split("(", 1)[0].split()[0]

    # Spatial types map to bytea (WKB) and are read by the loader via ST_AsBinary.
    #
    # MySQL ONLY. ``is_spatial_mysql_type`` matches on the TYPE NAME, and MySQL's set
    # includes ``point`` and ``polygon`` -- two names PostgreSQL also uses for entirely
    # different types. Ungated, a PostgreSQL ``point`` column landed in kind ``binary`` and
    # rendered ``encode(col, 'hex')``, which does not exist for ``point`` on the SOURCE
    # (``function encode(point, unknown) does not exist``) NOR on the target, where the
    # column is text. One such column poisoned the WHOLE table's checksum, so the table
    # could never be verified at all -- reproduced end to end through the real
    # ``run_validation`` (csum=None, ERROR=UndefinedFunction). PostgreSQL's own geometric
    # types are remodelled to text by Schema Conversion, so they are handled by the
    # textual-target rule below instead.
    if not source_is_postgres and is_spatial_mysql_type(mysql_type):
        return "binary"
    # BIT(n) maps to an integer target; the loader decoded the big-endian bytes.
    if base == "bit":
        return "bit"
    # Prefer the APPLIED target type (set by Validation from the converted DDL) so the
    # render matches how the value was STORED -- honoring a Schema-Conversion target-type
    # remap (e.g. TINYINT(1) kept as smallint -> integer '0'/'1', not boolean
    # 'true'/'false', which would false-mismatch every row). It is in the same postgres
    # vocabulary map_mysql_type produces, so the branches below apply unchanged; falls
    # back to the source-derived default mapping when no applied type is known.
    applied = column.target_type
    # PostgreSQL timetz must be detected on the FULL type string BEFORE the "(" split
    # below: format_type spells a precision inline as "time(6) with time zone", which
    # the split collapses to bare "time" (losing the zone). A timetz is rendered
    # offset-insensitively (see _pg_checksum_expr) because the CDC sink stores it
    # UTC-normalized (Debezium's ZonedTime is always GMT) while Full Load keeps the
    # source offset -- the same instant, so an offset-sensitive ::text would false-
    # mismatch. Only a PostgreSQL source produces timetz (MySQL has no such type).
    #
    # Checked on the APPLIED spelling when there is one and on the SOURCE spelling
    # otherwise: the applied types are resolved from the converted DDL and can come back
    # empty for a table (e.g. a PK strategy whose DDL clause does not parse), and there
    # the source spelling is the only thing left. Gating this on ``applied`` stranded
    # exactly that case in the offset-SENSITIVE fallback, which false-MISMATCHes every
    # CDC-written row. Applied still wins, so a deliberate remap away from timetz
    # (applied = "text") correctly does not reach this arm.
    spelling = applied or mysql_type
    if " ".join(_TYPE_MODIFIER_RE.sub("", spelling).lower().split()) in (
        "time with time zone",
        "timetz",
    ):
        return "timetz"
    # A PostgreSQL ARRAY column whose target is jsonb. Schema Conversion substitutes
    # `<t>[]` -> jsonb (DSQL has no array types; converter_postgres.py's
    # `stripped.endswith("[]")` rule), so the two ends hold the SAME data in different
    # spellings -- `{audio,wireless}` on the source, `["audio", "wireless"]` on the target.
    # Rendered "plain", one `::text` produced those two strings and the column MISMATCHED on
    # every row of a perfectly-migrated table, which buried a real lost update in systematic
    # noise. `to_jsonb(col)::text` fixes it with ONE expression for BOTH ends, which is what
    # this builder needs: `source_is_postgres` is true for the target render too, so nothing
    # here can tell which end it is rendering. That works because `to_jsonb` is IDEMPOTENT on
    # a jsonb input -- live-verified on the DSQL target and on PostgreSQL:
    # `to_jsonb(ARRAY['a','b']::text[])::text` and `to_jsonb('["a","b"]'::jsonb)::text` both
    # give `["a", "b"]`.
    # Keyed on the SOURCE spelling, and accepting an EMPTY applied type: DSQL has no array
    # type at all, so an array source column is jsonb on the target whether or not the
    # applied DDL was resolvable for this table. Leaving the unresolved case on "plain" would
    # put exactly the table whose types could not be read back into the false-mismatch it
    # used to be in. A non-jsonb applied type (a deliberate remap to text) still wins.
    _source_is_array = mysql_type.strip().rstrip(")").endswith("[]")
    _applied_base = (applied or "").split("(", 1)[0].strip().lower()
    if _source_is_array and _applied_base in ("jsonb", ""):
        return "array_json"
    if applied:
        kind = applied.split("(", 1)[0].strip().lower()
    elif source_is_postgres:
        # A PostgreSQL source with no APPLIED type (the converted DDL did not parse for
        # this table) must not fall through to MySQL's ``map_mysql_type`` table: that map
        # knows nothing of PostgreSQL's own types, so ``point`` stayed uncheckummable and
        # ``money`` -- whose real target is numeric -- rendered as ``plain``, a FALSE
        # MISMATCH on every row ('$12.34' on the source vs '12.340000' on the target).
        # ``substitute_pg_unsupported_type`` is the SAME function Schema Conversion used to
        # choose the target, so this reproduces what the loader actually stored.
        from dsql_migrator.core.converter_postgres import substitute_pg_unsupported_type

        substituted, _reason = substitute_pg_unsupported_type(mysql_type)
        kind = (substituted or mysql_type).split("(", 1)[0].strip().lower()
    else:
        try:
            target_type, _ = map_mysql_type(mysql_type)
        except ValueError:
            return "plain"
        kind = target_type.split("(", 1)[0].strip().lower()
    # NOTE: no explicit "textual target -> plain" arm is needed. A PostgreSQL type that
    # Schema Conversion remodelled to a textual target derives ``kind == "text"`` just
    # above, which falls through every branch below to the ``plain`` default -- and
    # ``plain`` renders ``(col)::text``, byte-identical to what the loader stored
    # (``CAST(col AS text)``, see source_dialect/postgres.py). An earlier draft added such
    # an arm; it was removed after a mutation check proved it changed nothing for
    # point/polygon/xml/tsvector/inet/cidr. The behaviour that actually matters is the
    # PG-gated spatial arm above (which stops ``point`` being mistaken for MySQL's spatial
    # ``point``) and the substitution-derived fallback (which stops ``money`` rendering as
    # plain text when its target is numeric).
    if kind == "bytea":
        return "binary"
    if kind == "boolean":
        return "boolean"
    if kind in ("timestamp", "timestamptz", "time"):
        return kind
    # map_mysql_type renders DECIMAL(p,s) as "DECIMAL(...)"; normalized kind is
    # "decimal" (also accept "numeric" defensively for any alias).
    if kind in ("numeric", "decimal"):
        return "numeric"
    if kind in ("real", "double precision", "double", "float"):
        return "float"
    if kind == "json":
        # JSON has no byte-identical cross-engine text form: MySQL CAST(col AS CHAR)
        # emits a SPACED canonical form ({"k": "v"}), while a CDC-written row holds
        # Debezium's COMPACT serialization ({"k":"v"}) in the PG `json` column. The
        # values are logically equal but the text differs, so -- like FLOAT/DOUBLE --
        # JSON is excluded from the checksum (row counts + all other columns still
        # validate; a JSON-text diff is a false positive, not data loss).
        #
        # ``jsonb`` is deliberately NOT matched here and must not be added: it is stored
        # decomposed and re-serialized by ``jsonb_out`` on read, so BOTH ends emit the
        # same canonical text (keys sorted, whitespace normalized) whatever wrote the
        # row -- Debezium's compact form included. Widening this to jsonb would drop a
        # genuinely comparable column out of the only value-level check there is.
        return "json"
    return "plain"


def _mysql_checksum_expr(column: "ColumnDef") -> Optional[str]:
    """Inner MySQL render expression for one column (``None`` = omit from checksum).

    Normalizes each divergent type to the SAME canonical text the PG side
    produces (see :func:`_pg_checksum_expr`). ``float`` and ``json`` return ``None``
    so FLOAT/DOUBLE and JSON are excluded (no byte-identical cross-engine text form
    exists: MySQL's float text keeps an exponent and only ~6 significant digits for a
    FLOAT, and JSON's whitespace/formatting differs between MySQL's canonical form and
    the CDC sink's compact serialization). This renderer is used ONLY for a MySQL
    source, so the float omission is MySQL-only -- a PostgreSQL source's floats ARE
    value-compared (see :func:`_pg_checksum_expr`).
    """
    ident = _quote_mysql_identifier(column.name)
    kind = _checksum_kind(column)
    if kind == "binary":
        # Spatial -> ST_AsBinary (matches the loader's stored WKB); then lower-hex
        # to match PG encode(bytea,'hex'). MySQL HEX() is UPPERCASE -> LOWER().
        if is_spatial_mysql_type(column.mysql_type):
            return f"LOWER(HEX(ST_AsBinary({ident})))"
        return f"LOWER(HEX({ident}))"
    if kind == "bit":
        # BIT(n) target is an integer; render the numeric value (matches PG int text).
        return f"CAST({ident} AS UNSIGNED)"
    if kind == "boolean":
        # TINYINT(1) target is boolean; render PG's 'true'/'false' words. The IS NULL
        # guard is REQUIRED: without it a NULL renders 'true' (NULL = 0 is UNKNOWN, so
        # the CASE falls to ELSE), which both (a) FALSE-MISMATCHES a correctly-migrated
        # NULL->NULL row (PG col::text is NULL -> the '~N' sentinel) and (b) FALSE-MATCHES
        # a source-NULL vs target-TRUE row (both 'true'). Returning NULL for a NULL input
        # routes it through the shared COALESCE(..., '~N') sentinel like every other type.
        return f"CASE WHEN {ident} IS NULL THEN NULL WHEN {ident} = 0 THEN 'false' ELSE 'true' END"
    if kind in ("timestamp", "timestamptz"):
        # Fixed 6-digit fraction, no zone -> matches PG to_char(... 'YYYY-MM-DD HH24:MI:SS.US').
        return f"DATE_FORMAT({ident}, '%Y-%m-%d %H:%i:%s.%f')"
    if kind == "time":
        return f"DATE_FORMAT({ident}, '%H:%i:%s.%f')"
    if kind == "numeric":
        # Pin the canonical scale so a stored-scale/trailing-zero difference cannot
        # diverge. CAST(... AS DECIMAL(65, s)) prints a plain fixed-scale decimal
        # with NO grouping commas (FORMAT() would add them), matching the PG
        # round(col, s)::text side byte-for-byte -- see the sweep recorded at that renderer:
        # every in-range (value, scale) pair agrees, and the only old-vs-new divergence is the
        # |round(v,s)| >= 10^65 overflow band, which MySQL cannot reach (DECIMAL caps at 65).
        scale = _decimal_scale(column.mysql_type)
        return f"CAST({ident} AS DECIMAL(65, {scale}))"
    if kind in ("float", "json"):
        return None
    if "zerofill" in column.mysql_type.lower():
        # A MySQL ZEROFILL integer is stored as a plain integer on the target (the
        # converter strips the display attribute), rendering e.g. "42". But ZEROFILL is
        # a DISPLAY attribute and CAST(col AS CHAR) applies it, emitting "00042" -- a
        # false checksum mismatch against the target's "42". Arithmetic (col + 0)
        # produces a plain numeric result that has no ZEROFILL padding, so it matches.
        return f"CAST({ident} + 0 AS CHAR)"
    return f"CAST({ident} AS CHAR)"


def _pg_checksum_expr(
    column: "ColumnDef", source_is_postgres: bool = False
) -> "Optional[sql.Composed]":
    """Inner PG render expression for one column (``None`` = omit from checksum).

    The byte-identical counterpart of :func:`_mysql_checksum_expr`. This renders the DSQL
    TARGET side for every migration, and ALSO the source side when the source is itself
    PostgreSQL (one PG-16 renderer serves both ends). ``source_is_postgres`` is threaded in
    only so the unconstrained-``numeric`` scale-6 rule fires for a genuine PostgreSQL-source
    bare numeric and NOT for a MySQL paren-less type such as ``bigint unsigned`` (see
    :func:`_numeric_render_scale`).
    """
    ident = sql.Identifier(column.name)
    kind = _checksum_kind(column, source_is_postgres)
    if kind == "binary":
        return sql.SQL("encode({col}, 'hex')").format(col=ident)
    if kind == "array_json":
        # ONE expression for both ends: the source's array becomes canonical jsonb text,
        # and on the jsonb target to_jsonb is the identity. See _checksum_kind.
        return sql.SQL("to_jsonb({col})::text").format(col=ident)
    if kind == "bit":
        return sql.SQL("{col}::text").format(col=ident)
    if kind == "boolean":
        return sql.SQL("{col}::text").format(col=ident)  # PG boolean -> 'true'/'false'
    if kind == "timestamptz":
        # timestamptz: AT TIME ZONE 'UTC' converts the instant to a UTC wall-clock
        # (dropping the zone) so it matches the MySQL TIMESTAMP side rendered as UTC.
        return sql.SQL(
            "to_char({col} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US')"
        ).format(col=ident)
    if kind == "timestamp":
        # Plain timestamp (from DATETIME): render DIRECTLY. AT TIME ZONE 'UTC' is NOT
        # a no-op here -- on a `timestamp without time zone` it CONVERTS to timestamptz
        # and back through the session TimeZone, shifting the wall-clock under a non-UTC
        # session. The MySQL DATE_FORMAT side is TZ-independent, so render the stored
        # wall-clock as-is (the connection also pins TimeZone=UTC as belt-and-suspenders).
        return sql.SQL(
            "to_char({col}, 'YYYY-MM-DD HH24:MI:SS.US')"
        ).format(col=ident)
    if kind == "time":
        return sql.SQL("to_char({col}, 'HH24:MI:SS.US')").format(col=ident)
    if kind == "timetz":
        # timetz stores its offset. The CDC sink writes it UTC-normalized (Debezium's
        # ZonedTime is always GMT) while Full Load preserves the SOURCE offset -- the same
        # instant-of-day but a different stored offset, so an offset-sensitive ::text would
        # false-mismatch a CDC-written value against the source. Shift BOTH sides to the UTC
        # offset first (timetz AT TIME ZONE 'UTC' stays a timetz, re-expressed at +00), so an
        # equal instant renders identically regardless of which write path produced it.
        return sql.SQL("({col} AT TIME ZONE 'UTC')::text").format(col=ident)
    if kind == "numeric":
        # Pin the render scale on both engines. A DECLARED scale (numeric(p,s), incl.
        # numeric(p) == scale 0, and every MySQL DECIMAL(p,s)) keeps that exact scale; a
        # PostgreSQL-source UNCONSTRAINED numeric compares at DSQL's default numeric(18,6)
        # scale of 6 (that >6-digit rounding is surfaced at Schema Conversion). The
        # source-engine gate is REQUIRED so a MySQL paren-less ``bigint unsigned`` (->
        # numeric(20,0), an integer) does not wrongly gain 6 fractional digits on the
        # target while the MySQL source side stays at scale 0. See _numeric_render_scale.
        scale = _numeric_render_scale(column, source_is_postgres)
        # Rendered MASK-FREE: round() pins the scale, then numeric_out (``::text``) emits
        # the exact digits. NOT ``to_char(round(...), '<65 digit positions>')``: a FIXED-width
        # mask makes BOTH PostgreSQL and Aurora DSQL emit the OVERFLOW indicator ('####...')
        # for every |value| >= 10^65, so any two same-sign wide values hash EQUAL -- a
        # confirmed FALSE MATCH, the worst defect class here (the cut-over gate reads this
        # verdict). Measured with this builder's own SQL: two numeric(80,0) tables sharing
        # ZERO equal 66-digit values BOTH checksummed 1738510059675055814 on the live DSQL
        # target (and both the same value on local PG 17), while a 65-digit control diverged
        # correctly; with this render they diverge (1135099206046739538 vs
        # 2746917630161110524 -- identical on both engines). It is REACHABLE because
        # converter_postgres passes numeric(80,0) / numeric(100,10) through unchanged (it
        # clamps only above DSQL's documented precision maximum of 1000) and DSQL stores
        # those digits exactly. No mask WIDTH fixes it either: a BARE ``numeric`` declares no
        # precision at all, so there is nothing to size a mask from. The deleted mask's
        # docstring blamed MySQL's DECIMAL(65, s) for the 65-digit width -- that rationale
        # only ever covered a MySQL SOURCE (DECIMAL caps at precision 65, so a MySQL value
        # can never reach the overflow range); a PostgreSQL source is not bounded that way.
        #
        # Switching is SAFE for an in-flight migration: it is byte-IDENTICAL to the old
        # rendering for every value that did not overflow, so nothing starts false-MISMATCHing.
        # Over 216 (value, scale) pairs -- negatives, zero, trailing zeros, scale 0, NEGATIVE
        # scale, NaN, +/-Infinity, 10^64/10^65/10^66, a 1000-digit numeric(1000,500) -- run on
        # local PG 17 AND the live DSQL target, the ONLY 31 divergences are exactly the
        # |v| >= 10^65 overflow cases, and the two engines agree byte-for-byte on this form.
        # Against the UNCHANGED MySQL side (``CAST(col AS DECIMAL(65, s))``) it is identical
        # too, and a live ordinary numeric(30,4) DSQL table (15 values + a NULL) gave the same
        # checksum 9423634626081600268 under both forms.
        #
        # The in-range equivalence was re-measured independently: a sweep of 114 (value, scale)
        # pairs -- 19 values x 6 scales incl. 0, 2, 4, 6, 30 and NEGATIVE -2, covering 0, +/-1,
        # 0.5, -0.00001, +/-18446744073709551615, NaN, 1e63..1e66 and 64/65/66-nine runs --
        # differed old-vs-new in 31 pairs, and **every one of those 31 is |round(v,s)| >= 10^65**
        # (differences below 10^65: ZERO). So the only behaviour this changed is the overflow
        # band, which is exactly the false-MATCH band.
        #
        # It is locale-INDEPENDENT by construction -- numeric_out consults no locale, whereas
        # to_char's mask templates are the only reason ``lc_numeric`` had to be reasoned about
        # -- and CHEAPER: ~4-6x on 300k rows, with no extra scan and no per-row subquery
        # (measured 251 ms with to_char vs 51 ms with ``::text`` on a local PG 17.11; the ratio
        # is machine- and load-dependent, the direction is not).
        # ``(col)::numeric`` is a NO-OP for a real numeric and is what makes a PostgreSQL
        # ``money`` source checksummable at all: its target IS numeric (so this arm is
        # right), but ``round(money, integer)`` does not exist, and that one column crashed
        # the WHOLE table's checksum. The loader stored ``CAST(col AS numeric)`` for exactly
        # this column, so the cast reproduces the stored value rather than reinterpreting
        # it. Gated on the PG-source flag purely to keep a MySQL-source migration's
        # rendered SQL byte-identical -- the cast would be a harmless no-op there too, but
        # "unchanged" is a cheaper property to defend than "equivalent".
        if source_is_postgres:
            return sql.SQL("round(({col})::numeric, {scale})::text").format(
                col=ident, scale=sql.Literal(scale),
            )
        return sql.SQL("round({col}, {scale})::text").format(
            col=ident, scale=sql.Literal(scale),
        )
    if kind == "float":
        # Scalar real/double precision are value-compared ONLY for a PostgreSQL source,
        # where this ONE expression renders BOTH ends (like ``array_json``) so there is no
        # cross-ENGINE text to reconcile -- only a cross-CLUSTER one, and ``to_jsonb`` of a
        # float is ``float8out``'s shortest round-tripping decimal printed PLAIN. Measured
        # byte-identical on 2000 random float8 + 2000 random float4 bit patterns (plus 0,
        # -0.0, NaN, +/-Infinity, 5e-324, 1e-45, +/-MAX, the float4 denormal min and a
        # 17-significant-digit value) stored in and re-read from local PostgreSQL 17.11 and
        # the live Aurora DSQL target: one identical md5, 0/4000 value diffs.
        #
        # ``to_jsonb(col)::text`` and NOT ``col::text``, for two reasons:
        #   - ``col::text`` renders a stored ``-0.0`` as ``-0`` (live-verified on BOTH
        #     engines -- DSQL does keep the sign bit), but the CDC path CANNOT deliver one:
        #     Kafka Connect's ``JsonConverter`` reads ``-0.0`` back through
        #     ``BigDecimal.doubleValue()``, which has no signed zero, so the sink stores
        #     ``+0.0``. A ``::text`` render would therefore FALSE-MISMATCH a CDC-written
        #     ``-0.0`` against the source's ``-0`` while matching a Full-Loaded one -- the
        #     two write paths diverging on the same value. ``to_jsonb`` renders both signed
        #     zeros as ``0`` on both engines, so the comparison is immune to it.
        #   - it is already this module's cross-engine float form (``array_json``) and the
        #     PostgreSQL Full Load float-ARRAY read path, so there is one rule, not two.
        # NaN / +/-Infinity render as the JSON strings "NaN" / "Infinity" / "-Infinity" on
        # both engines, which is what makes the CDC sink's 0-for-a-special corruption
        # (JsonConverter maps the wire's "NaN" through ``JsonNode.doubleValue()`` -> 0.0)
        # a DETECTED mismatch instead of a silent one.
        #
        # A MySQL source stays EXCLUDED and must: MySQL's own float text is not this text
        # (measured on Aurora MySQL 8.0.42 -- DOUBLE 1e30 -> "1e30" vs "1000...0", 1e15 ->
        # "1e15" vs "1000000000000000", and FLOAT 1234567890123456.7 -> "1.23457e15", only
        # ~6 significant digits), so including it would false-MISMATCH most real float data.
        # MySQL also cannot STORE a float special at all (CAST rejects it, ERROR 1690) and
        # normalizes -0.0 to 0, so a MySQL source has nothing here to detect.
        if not source_is_postgres:
            return None
        return sql.SQL("to_jsonb({col})::text").format(col=ident)
    if kind == "json":
        return None
    return sql.SQL("{col}::text").format(col=ident)


def _checksum_omits_column(column: "ColumnDef", source_is_postgres: bool) -> bool:
    """True when the CHECKSUM cannot value-compare ``column`` and leaves it out.

    The SINGLE definition of that set, so the columns Validation DISCLOSES as
    not-value-compared (``TableValidationResult.checksum_excluded_columns``) can never
    drift from the columns the renderers actually omit. A drift either way is a
    trust bug: naming a column that WAS compared is noise, and staying silent about
    one that was NOT is the false-"every column verified" this disclosure exists to
    prevent. Asked of the renderer that actually renders the SOURCE end (the PG one
    for a PostgreSQL source, which renders both ends), so the answer is the renderers'
    own, never a second hand-maintained list of type names.
    """
    if source_is_postgres:
        return _pg_checksum_expr(column, True) is None
    return _mysql_checksum_expr(column) is None


# ---------------------------------------------------------------------------
# Checksum SQL builders (cross-engine normalized per-column rendering)
# ---------------------------------------------------------------------------


def _chunk_terms(terms: list, size: int = _CONCAT_MAX_TERMS) -> list:
    """Split ``terms`` into ordered groups of at most ``size`` (order preserved)."""
    return [terms[i : i + size] for i in range(0, len(terms), size)]


def _mysql_md5_concat(terms: list[str]) -> str:
    """``MD5(CONCAT_WS('|', ...))`` over ``terms``, nesting per group of
    ``_CONCAT_MAX_TERMS`` when the term count would exceed the PG/DSQL 100-argument
    limit. A count within the limit returns the ORIGINAL flat expression unchanged.
    """
    if len(terms) <= _CONCAT_MAX_TERMS:
        return f"MD5(CONCAT_WS('|', {', '.join(terms)}))"
    groups = [f"MD5(CONCAT_WS('|', {', '.join(g)}))" for g in _chunk_terms(terms)]
    return f"MD5(CONCAT_WS('|', {', '.join(groups)}))"


def _pg_md5_concat(terms: list) -> "sql.Composed":
    """PostgreSQL counterpart of :func:`_mysql_md5_concat` (byte-identical nesting:
    same group size, same order, lowercase hex group hashes)."""
    def _one(group: list) -> "sql.Composed":
        return sql.SQL("md5(concat_ws('|', {t}))").format(t=sql.SQL(", ").join(group))

    if len(terms) <= _CONCAT_MAX_TERMS:
        return _one(terms)
    return _one([_one(g) for g in _chunk_terms(terms)])


def _mysql_row_token(table: TableDef) -> str:
    """MySQL per-row checksum token: the integer value of the first
    ``_CHECKSUM_HEX_DIGITS`` MD5 hex digits over the row's rendered column values.

    This is the SINGLE definition of the per-row token that both the whole-table
    checksum (:func:`build_mysql_checksum_sql`), the row-diff sample
    (:func:`build_mysql_pk_token_sql`), and the bounded page checksum
    (:func:`build_mysql_page_checksum_first_sql`) reduce over -- factored out so a
    paged sub-sum can never drift from the whole-table sum (a drift would silently
    mismatch checksums). FLOAT/DOUBLE and JSON columns are omitted (no byte-identical
    cross-engine text form); an all-omitted table falls back to a constant sentinel.
    Wide tables (>= _CONCAT_MAX_TERMS rendered columns) nest MD5s per group so the
    per-row hash stays under the PG/DSQL 100-argument limit (see :func:`_mysql_md5_concat`).
    """
    rendered = [_mysql_checksum_expr(column) for column in table.columns]
    terms = [_mysql_concat_term(expr) for expr in rendered if expr is not None]
    if not terms:
        terms = [f"'{_NULL_SENTINEL}'"]
    return (
        f"CAST(CONV(SUBSTRING({_mysql_md5_concat(terms)}, 1, "
        f"{_CHECKSUM_HEX_DIGITS}), 16, 10) AS DECIMAL(65, 0))"
    )


def _pg_row_token(table: TableDef, source_is_postgres: bool = False) -> "sql.Composed":
    """PostgreSQL per-row checksum token -- the byte-identical counterpart of
    :func:`_mysql_row_token` (same MD5-prefix reduction as a positive ``bigint``).

    The single definition shared by :func:`build_pg_checksum_sql`,
    :func:`build_pg_pk_token_sql`, and the paged :func:`build_pg_page_checksum_first_sql`.
    ``source_is_postgres`` is forwarded to :func:`_pg_checksum_expr` for the numeric-scale
    rule (default ``False`` = the DSQL target of a MySQL-source migration). Wide tables
    nest MD5s per group (see :func:`_pg_md5_concat`) to stay under the 100-argument
    function limit, identically to the MySQL side.
    """
    rendered_pg = [
        _pg_checksum_expr(column, source_is_postgres) for column in table.columns
    ]
    terms = [_pg_concat_term(expr) for expr in rendered_pg if expr is not None]
    if not terms:
        terms = [sql.Literal(_NULL_SENTINEL)]
    digits = sql.Literal(_CHECKSUM_HEX_DIGITS)
    return sql.SQL(
        "('x' || lpad(substr({md5}, 1, {digits}), 16, '0'))::bit(64)::bigint"
    ).format(md5=_pg_md5_concat(terms), digits=digits)


def build_mysql_checksum_sql(table: TableDef) -> str:
    """Build an order-independent MySQL checksum query for ``table`` (read-only).

    Each row is reduced to the integer value of the first
    ``_CHECKSUM_HEX_DIGITS`` hex digits of an MD5 over its column values; the
    per-row values are summed (order-independent). Each column is rendered with
    the engine-normalized :func:`_mysql_checksum_expr` (so equal data hashes
    equally cross-engine) inside ``COALESCE(..., <sentinel>)`` so ``NULL`` columns
    map to the shared sentinel and never silently drop out of the concatenation.
    FLOAT/DOUBLE and JSON columns are omitted (``_mysql_checksum_expr`` returns
    ``None`` -- neither has a byte-identical cross-engine text form).
    """
    table_sql = _quote_mysql_table(table.name)
    return (
        f"SELECT COALESCE(SUM({_mysql_row_token(table)}), 0) FROM {table_sql}"
    )


def build_pg_checksum_sql(
    table: TableDef, source_is_postgres: bool = False
) -> sql.Composed:
    """Build an order-independent PostgreSQL checksum query for ``table``.

    Mirrors :func:`build_mysql_checksum_sql`: the first ``_CHECKSUM_HEX_DIGITS``
    MD5 hex digits of each row are summed as a positive ``bigint``. Each column is
    rendered with the engine-normalized :func:`_pg_checksum_expr` inside
    ``COALESCE(..., <sentinel>)`` (FLOAT/DOUBLE and JSON columns are omitted). Identifiers
    are composed with :class:`psycopg.sql.Identifier` so a column/table name can
    never break out of the SQL (Requirement 9.4). All access is a single
    ``SELECT`` (read-only). ``source_is_postgres`` selects the numeric-scale rule (see
    :func:`_numeric_render_scale`); the caller passes ``True`` only for a PostgreSQL source.
    """
    return sql.SQL(
        "SELECT COALESCE(SUM({token}), 0) FROM {table}"
    ).format(
        token=_pg_row_token(table, source_is_postgres),
        table=_pg_table_identifier(table.name),
    )


def build_mysql_pk_token_sql(table: TableDef, pk_column: str) -> str:
    """Build a bounded ``(pk, per-row token)`` MySQL query for the row-diff sample.

    Selects each row's primary key and the SAME per-row MD5/CONV token used inside
    :func:`build_mysql_checksum_sql`'s ``SUM`` -- so a token match here means the
    exact row equality the table-level checksum trusts -- ordered by primary key
    and bounded by ``:sample_size`` (``ORDER BY pk LIMIT N``). No ``COUNT(*)`` and
    no full materialization: the engine streams in PK order and stops at the LIMIT,
    reading only the first N rows via the primary-key index. Read-only.
    """
    pk_sql = _quote_mysql_identifier(pk_column)
    table_sql = _quote_mysql_table(table.name)
    return (
        f"SELECT {pk_sql} AS pk, {_mysql_row_token(table)} AS tok FROM {table_sql} "
        f"ORDER BY {pk_sql} LIMIT :sample_size"
    )


def build_pg_pk_token_sql(
    table: TableDef, pk_column: str, sample_size: int, source_is_postgres: bool = False
) -> sql.Composed:
    """Build the bounded ``(pk, per-row token)`` PostgreSQL counterpart.

    Mirrors :func:`build_mysql_pk_token_sql` using the same per-row token as
    :func:`build_pg_checksum_sql`. Identifiers are composed with
    :class:`psycopg.sql.Identifier` and the bound is a :class:`psycopg.sql.Literal`
    so nothing can break out of the SQL (Requirement 9.4). A single read-only
    ``SELECT`` ordered by primary key and bounded by ``LIMIT`` -- no scan, no count.
    ``source_is_postgres`` selects the numeric-scale rule (see :func:`_numeric_render_scale`).
    """
    return sql.SQL(
        "SELECT {pk} AS pk, {token} AS tok FROM {table} ORDER BY {pk} LIMIT {limit}"
    ).format(
        pk=sql.Identifier(pk_column), token=_pg_row_token(table, source_is_postgres),
        table=_pg_table_identifier(table.name), limit=sql.Literal(sample_size),
    )


# ---------------------------------------------------------------------------
# Bounded keyset PK-page SQL builders (full reconciliation, streaming)
# ---------------------------------------------------------------------------
#
# Reconciliation streams EVERY primary key from both engines in ascending order
# and merges them, so a whole table is never materialized (stream, never
# materialize). Each page reads only the next ``page_size`` PKs via the
# primary-key index (``WHERE pk > :last ORDER BY pk LIMIT N`` -- keyset, not
# OFFSET), exactly like the exporter's keyset stream. Only the PK column is
# selected (never row values, Property 7).


def build_mysql_pk_first_page_sql(table: TableDef, pk_column: str) -> str:
    """First keyset page of source primary keys (ascending, bounded by ``:page``)."""
    pk_sql = _quote_mysql_identifier(pk_column)
    table_sql = _quote_mysql_table(table.name)
    return f"SELECT {pk_sql} AS pk FROM {table_sql} ORDER BY {pk_sql} LIMIT :page"


def build_mysql_pk_next_page_sql(table: TableDef, pk_column: str) -> str:
    """Subsequent keyset page of source primary keys after ``:last`` (ascending)."""
    pk_sql = _quote_mysql_identifier(pk_column)
    table_sql = _quote_mysql_table(table.name)
    return (
        f"SELECT {pk_sql} AS pk FROM {table_sql} WHERE {pk_sql} > :last "
        f"ORDER BY {pk_sql} LIMIT :page"
    )


def build_pg_pk_first_page_sql(
    table: TableDef, pk_column: str, page_size: int
) -> sql.Composed:
    """First keyset page of target primary keys (ascending, bounded by ``page_size``).

    Identifiers compose with :class:`psycopg.sql.Identifier` and the bound is a
    :class:`psycopg.sql.Literal` so nothing can break out of the SQL (Req 9.4).
    """
    return sql.SQL(
        "SELECT {pk} AS pk FROM {table} ORDER BY {pk} LIMIT {limit}"
    ).format(
        pk=sql.Identifier(pk_column),
        table=_pg_table_identifier(table.name),
        limit=sql.Literal(page_size),
    )


def build_pg_pk_next_page_sql(
    table: TableDef, pk_column: str, page_size: int
) -> sql.Composed:
    """Subsequent keyset page of target primary keys after the ``last`` placeholder.

    The keyset value is bound as a query parameter (``%(last)s``) at execute time,
    so a billion-row table reuses one prepared statement across all pages.
    """
    return sql.SQL(
        "SELECT {pk} AS pk FROM {table} WHERE {pk} > {last} "
        "ORDER BY {pk} LIMIT {limit}"
    ).format(
        pk=sql.Identifier(pk_column),
        table=_pg_table_identifier(table.name),
        last=sql.Placeholder("last"),
        limit=sql.Literal(page_size),
    )


# ---------------------------------------------------------------------------
# Bounded keyset page-checksum SQL builders (single-column PK, streaming)
# ---------------------------------------------------------------------------
#
# The whole-table checksum is ONE ``SELECT SUM(md5-prefix) FROM table`` -- a single
# unbounded scan that, on a large table, exceeds Aurora DSQL's hard 300s transaction
# limit, so a big table could never produce a checksum. These builders sum the SAME
# per-row token over one keyset page at a time (``WHERE pk > :last ORDER BY pk LIMIT
# N`` -- the same keyset stream reconciliation uses); the caller accumulates the
# per-page sub-sums in Python. Because the token is per-row and SUM is
# order-independent, the accumulated total EQUALS the whole-table checksum exactly,
# while every statement stays bounded (one page) and memory stays at one row. Each
# page returns ``(sub_sum, last_pk, row_count)``: ``sub_sum`` folds into the running
# total, ``MAX(page_pk)`` is the next keyset boundary, and ``COUNT(*) < N`` signals the
# last page. Limited to a single-column PK (a self-consistent ascending order per
# engine); a composite/missing PK keeps the whole-table single-scan fallback.


def build_mysql_page_checksum_first_sql(table: TableDef, pk_column: str) -> str:
    """First keyset page of the MySQL checksum: ``(sub_sum, last_pk, row_count)``."""
    pk_sql = _quote_mysql_identifier(pk_column)
    table_sql = _quote_mysql_table(table.name)
    return (
        "SELECT COALESCE(SUM(page_tok), 0), MAX(page_pk), COUNT(*) FROM ("
        f"SELECT {pk_sql} AS page_pk, {_mysql_row_token(table)} AS page_tok "
        f"FROM {table_sql} ORDER BY {pk_sql} LIMIT :page) ckpage"
    )


def build_mysql_page_checksum_next_sql(table: TableDef, pk_column: str) -> str:
    """Subsequent keyset page of the MySQL checksum after ``:last`` (ascending)."""
    pk_sql = _quote_mysql_identifier(pk_column)
    table_sql = _quote_mysql_table(table.name)
    return (
        "SELECT COALESCE(SUM(page_tok), 0), MAX(page_pk), COUNT(*) FROM ("
        f"SELECT {pk_sql} AS page_pk, {_mysql_row_token(table)} AS page_tok "
        f"FROM {table_sql} WHERE {pk_sql} > :last ORDER BY {pk_sql} LIMIT :page) ckpage"
    )


def build_pg_page_checksum_first_sql(
    table: TableDef, pk_column: str, page_size: int, source_is_postgres: bool = False
) -> sql.Composed:
    """First keyset page of the PostgreSQL/DSQL checksum: ``(sub_sum, last_pk, count)``.

    The last (max) PK of the ascending page is taken as
    ``(array_agg(page_pk ORDER BY page_pk))[COUNT(*)]`` rather than ``MAX(page_pk)``:
    a ``uuid`` PK is orderable but has NO ``max()`` aggregate in PostgreSQL/DSQL
    (``function max(uuid) does not exist``), which would abort the keyset checksum for
    a uuid single-PK. array_agg + ORDER BY works for any orderable PK type (int / uuid /
    text / timestamp) and equals ``MAX`` for the common integer case.

    Identifiers compose with :class:`psycopg.sql.Identifier` and the bound is a
    :class:`psycopg.sql.Literal` so nothing can break out of the SQL (Req 9.4).
    """
    return sql.SQL(
        "SELECT COALESCE(SUM(page_tok), 0), "
        "(array_agg(page_pk ORDER BY page_pk))[COUNT(*)], COUNT(*) FROM ("
        "SELECT {pk} AS page_pk, {token} AS page_tok FROM {table} "
        "ORDER BY {pk} LIMIT {limit}) ckpage"
    ).format(
        pk=sql.Identifier(pk_column),
        token=_pg_row_token(table, source_is_postgres),
        table=_pg_table_identifier(table.name),
        limit=sql.Literal(page_size),
    )


def build_pg_page_checksum_next_sql(
    table: TableDef, pk_column: str, page_size: int, source_is_postgres: bool = False
) -> sql.Composed:
    """Subsequent keyset page of the PostgreSQL/DSQL checksum after the ``last`` param.

    The keyset value is bound as ``%(last)s`` at execute time, so a billion-row table
    reuses one prepared statement across all pages.
    """
    return sql.SQL(
        "SELECT COALESCE(SUM(page_tok), 0), "
        "(array_agg(page_pk ORDER BY page_pk))[COUNT(*)], COUNT(*) FROM ("
        "SELECT {pk} AS page_pk, {token} AS page_tok FROM {table} WHERE {pk} > {last} "
        "ORDER BY {pk} LIMIT {limit}) ckpage"
    ).format(
        pk=sql.Identifier(pk_column),
        token=_pg_row_token(table, source_is_postgres),
        table=_pg_table_identifier(table.name),
        last=sql.Placeholder("last"),
        limit=sql.Literal(page_size),
    )


# Base MySQL integer types eligible for reconciliation: a single-column,
# integer-like PK gives a well-defined ascending merge order on both engines.
_INTEGER_BASE_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
)


def integer_pk_column(table: TableDef) -> Optional[str]:
    """Return ``table``'s single integer primary-key column, or ``None``.

    Reconciliation is limited to single-column, integer-like primary keys so the
    ascending keyset order is identical and well-defined on both MySQL and DSQL
    (text/collation ordering can differ across engines). A composite, missing, or
    non-integer PK returns ``None`` (the table is reconciled by count/checksum
    only, never scanned).
    """
    if len(table.primary_key) != 1:
        return None
    pk = table.primary_key[0]
    column = next((c for c in table.columns if c.name == pk), None)
    if column is None:
        return None
    # Strip a display width ("int(11)" -> "int") and modifiers ("int unsigned"
    # -> "int") to the base type token.
    base = column.mysql_type.strip().lower().split("(")[0].split()[0]
    return pk if base in _INTEGER_BASE_TYPES else None


def single_pk_column(table: TableDef) -> Optional[str]:
    """Return ``table``'s single-column primary key of ANY type, or ``None``.

    Unlike :func:`integer_pk_column` (restricted to integer PKs so a CROSS-engine
    ascending merge order is well defined), this accepts any single-column PK
    (uuid/varchar/binary/...), because COUNTING the target by bounded keyset paging
    needs only a self-consistent order ON THE TARGET, not a cross-engine one. A
    composite or missing PK returns ``None`` (the target count then falls back to a
    single ``COUNT(*)``).
    """
    if len(table.primary_key) != 1:
        return None
    pk = table.primary_key[0]
    return pk if any(c.name == pk for c in table.columns) else None


def _orphan_predicates(
    fk: ForeignKeyDef, child_alias: str
) -> "tuple[sql.Composed, sql.Composed]":
    """Return ``(not_null, join_predicate)`` for one FK, qualified by ``child_alias``.

    ``not_null`` is ``<alias>.<col> IS NOT NULL AND ...`` over the FK columns (a null key
    is not a referential violation), and ``join_predicate`` is ``p.<ref> = <alias>.<col>
    AND ...`` matching parent to child. Shared by the single-scan and keyset-paged orphan
    builders so their orphan semantics can never drift. Identifiers are composed with
    :class:`psycopg.sql.Identifier` (Requirement 9.4).
    """
    not_null = sql.SQL(" AND ").join(
        sql.SQL("{alias}.{column} IS NOT NULL").format(
            alias=sql.SQL(child_alias), column=sql.Identifier(column)
        )
        for column in fk.columns
    )
    join_predicate = sql.SQL(" AND ").join(
        sql.SQL("p.{ref} = {alias}.{col}").format(
            ref=sql.Identifier(ref), alias=sql.SQL(child_alias), col=sql.Identifier(col)
        )
        for col, ref in zip(fk.columns, fk.referenced_columns)
    )
    return not_null, join_predicate


def build_orphan_count_sql(child_table: str, fk: ForeignKeyDef) -> sql.Composed:
    """Build a target query counting orphan child rows for one foreign key.

    A row is an orphan when all of its foreign-key columns are non-null (a null
    key is not a referential violation) yet no parent row matches on the
    referenced columns. Identifiers are composed with
    :class:`psycopg.sql.Identifier` (Requirement 9.4) and the statement is a
    single read-only ``SELECT``.

    A single unbounded scan: used ONLY for a composite/missing-PK child; a
    single-column-PK child is orphan-counted over BOUNDED keyset pages
    (:func:`build_pg_orphan_page_first_sql`) so a billion-row child never runs one
    transaction past DSQL's ~300s limit -- exactly as count/checksum do.
    """
    # Split a schema-qualified name into "schema"."table" (NOT one quoted
    # "schema.table" identifier, which would reference a table that doesn't exist):
    # the same composition the COUNT/checksum/reconcile queries use.
    child = _pg_table_identifier(child_table)
    parent = _pg_table_identifier(fk.referenced_table)
    not_null, join_predicate = _orphan_predicates(fk, "c")
    return sql.SQL(
        "SELECT COUNT(*) FROM {child} AS c WHERE {not_null} AND NOT EXISTS ("
        "SELECT 1 FROM {parent} AS p WHERE {join_predicate})"
    ).format(
        child=child,
        not_null=not_null,
        parent=parent,
        join_predicate=join_predicate,
    )


def _build_pg_orphan_page_sql(
    child_table: str, fk: ForeignKeyDef, pk_column: str, page_size: int, *, first: bool
) -> sql.Composed:
    """One keyset page of the orphan count for ``fk``: ``(orphan_sub_count, last_pk, count)``.

    The child's PK window is paged UNCONDITIONALLY (``WHERE pk > :last ORDER BY pk LIMIT
    N`` -- the same keyset stream the count/checksum use) and the orphan predicate is
    folded into a ``COUNT(*) FILTER`` so the keyset boundary advances over NON-orphan rows
    too. If the ``WHERE`` had dropped non-orphans, the ``MAX(pk)`` of a page could skip a
    PK range and under-count; selecting every row in the window and FILTERing the count
    keeps the boundary exact. The caller accumulates ``orphan_sub_count`` in Python,
    advances ``:last`` from ``array_agg`` (the last/max PK of the ascending window, which
    works for any orderable PK type -- ``uuid`` has no ``max()`` aggregate), and stops when
    ``count < page_size``. Identifiers/bounds compose safely (Requirement 9.4).
    """
    child = _pg_table_identifier(child_table)
    parent = _pg_table_identifier(fk.referenced_table)
    # The orphan predicate is applied to the paged subquery (alias ``pg``).
    not_null, join_predicate = _orphan_predicates(fk, "pg")
    # The FK columns are selected in the window so the FILTER can read them; the PK is
    # aliased ``page_pk`` for the keyset advance. A FK column that IS the PK is harmless
    # here -- it is selected under both ``page_pk`` and its own name.
    fk_cols = sql.SQL(", ").join(sql.Identifier(col) for col in fk.columns)
    where_clause = (
        sql.SQL("")
        if first
        else sql.SQL("WHERE {pk} > {last} ").format(
            pk=sql.Identifier(pk_column), last=sql.Placeholder("last")
        )
    )
    # The window is pinned in a MATERIALIZED CTE and scanned twice: once for the orphan
    # count as a LEFT JOIN anti-join, once for the keyset boundary + row count. That keeps
    # the boundary EXACT (it still advances over non-orphan rows) while letting the planner
    # hash-join the parent instead of probing it per row.
    #
    # WHY, measured on a live ap-northeast-2 cluster over a 100k-row window of a 3M-row
    # child: the previous form -- COUNT(*) FILTER (WHERE ... NOT EXISTS ...) -- took 59.2s
    # (0.59 ms/row); this one takes 2.19s (0.022 ms/row), a 27x reduction, and returns
    # BYTE-IDENTICAL results. FILTER was the whole cost: it makes the correlated NOT EXISTS
    # a per-row scalar expression, so the anti-join optimisation is unavailable. Neither the
    # page size (identical ms/row at 5k..250k, and 500k exceeds DSQL's 300s transaction
    # limit) nor the array_agg boundary trick (0.21s on its own) was the cost -- both were
    # measured and ruled out.
    #
    # This matters because the foreign-key pass is the pre-cut-over gate: on an 8.5M-row
    # schema with 15 foreign keys it projected to ~153 min, and for a CDC migration that
    # runs at CUT OVER, after the source is already frozen. Now ~6 min.
    #
    # array_agg stays for the boundary: max() has no uuid overload (verified live --
    # "function max(uuid) does not exist"), and it costs 0.21s per page.
    return sql.SQL(
        "WITH pg AS MATERIALIZED ("
        "SELECT {pk} AS page_pk, {fk_cols} FROM {child} {where}"
        "ORDER BY {pk} LIMIT {limit}) "
        "SELECT (SELECT COUNT(*) FROM pg LEFT JOIN {parent} AS p ON {join_predicate} "
        "WHERE {not_null} AND {parent_null}), "
        "(SELECT (array_agg(page_pk ORDER BY page_pk))[COUNT(*)] FROM pg), "
        "(SELECT COUNT(*) FROM pg)"
    ).format(
        not_null=not_null,
        parent=parent,
        join_predicate=join_predicate,
        # The anti-join's "no parent row matched" test. Any referenced column is enough
        # (they are the parent's key, so all-or-nothing), and it must be NOT NULL in the
        # parent, which a referenced key always is.
        parent_null=sql.SQL("p.{ref} IS NULL").format(
            ref=sql.Identifier(fk.referenced_columns[0])
        ),
        pk=sql.Identifier(pk_column),
        fk_cols=fk_cols,
        child=child,
        where=where_clause,
        limit=sql.Literal(page_size),
    )


def build_pg_orphan_page_first_sql(
    child_table: str, fk: ForeignKeyDef, pk_column: str, page_size: int
) -> sql.Composed:
    """First keyset page of the orphan count for ``fk`` (see :func:`_build_pg_orphan_page_sql`)."""
    return _build_pg_orphan_page_sql(
        child_table, fk, pk_column, page_size, first=True
    )


def build_pg_orphan_page_next_sql(
    child_table: str, fk: ForeignKeyDef, pk_column: str, page_size: int
) -> sql.Composed:
    """Subsequent keyset page of the orphan count after the ``last`` placeholder."""
    return _build_pg_orphan_page_sql(
        child_table, fk, pk_column, page_size, first=False
    )
