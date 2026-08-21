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

from typing import Optional

from psycopg import sql

from dsql_migrator.core.converter import is_spatial_mysql_type, map_mysql_type
from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, TableDef

# Number of leading MD5 hex digits used to build a per-row checksum token. 15
# hex digits = 60 bits, which stays positive in both MySQL's unsigned CONV and
# PostgreSQL's signed bigint, so equal data yields equal checksums on both.
_CHECKSUM_HEX_DIGITS = 15


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


def _pg_numeric_mask(scale: int) -> str:
    """A PG ``to_char`` numeric mask that emits a fixed ``scale`` decimal places.

    ``FM`` drops padding spaces; the integer part uses a long ``9...0`` run so any
    magnitude renders without a leading placeholder space, and ``0`` digits force
    a fixed fractional width matching MySQL ``CAST(col AS DECIMAL(65, scale))``.
    Mirrors the MySQL side so equal decimals render byte-identically.

    The integer run must be wide enough for the widest value the MySQL side can
    render. That side casts to ``DECIMAL(65, scale)`` (MySQL's max precision) and
    ``BIGINT UNSIGNED`` is stored as ``numeric(20, 0)`` on the target, so integer
    magnitudes reach up to 65 digits. A too-short mask makes ``to_char`` emit the
    overflow indicator (``#``...) instead of the digits, so a byte-identical value
    would produce a spurious cross-engine checksum MISMATCH. A generous run of
    ``9`` positions is safe: under ``FM`` the extra positions render nothing for
    smaller magnitudes (``9`` suppresses non-significant leading digits), so the
    output is identical to a tighter mask for every value that fits.
    """
    # 64 nines + a trailing zero = 65 integer digit positions, covering the full
    # DECIMAL(65, 0) integer range (and thus BIGINT UNSIGNED's 20-digit max).
    integer_part = "FM" + ("9" * 64) + "0"
    if scale <= 0:
        return integer_part
    return integer_part + "D" + ("0" * scale)


def _checksum_kind(column: "ColumnDef") -> str:
    """Classify ``column`` into the render family used by the checksum builders.

    Returns one of: ``"binary"`` (bytea/spatial WKB), ``"bit"`` (BIT(n) ->
    integer target), ``"boolean"``, ``"timestamp"`` / ``"timestamptz"`` /
    ``"time"`` (temporal), ``"numeric"`` (DECIMAL), ``"float"`` (FLOAT/DOUBLE) and
    ``"json"`` (both excluded from the checksum -- no byte-identical cross-engine
    text form), or ``"plain"`` (all safe types rendered by the engine's native text
    cast). Reuses the SAME converter classification the Full
    Load loader used to STORE the value (converter.map_mysql_type / the exporter's
    _target_kind), so the rendered text matches the stored value on the target.
    """
    mysql_type = column.mysql_type
    base = mysql_type.strip().lower().split("(", 1)[0].split()[0]

    # Spatial types map to bytea (WKB) and are read by the loader via ST_AsBinary.
    if is_spatial_mysql_type(mysql_type):
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
    if applied:
        kind = applied.split("(", 1)[0].strip().lower()
    else:
        try:
            target_type, _ = map_mysql_type(mysql_type)
        except ValueError:
            return "plain"
        kind = target_type.split("(", 1)[0].strip().lower()
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
        return "json"
    return "plain"


def _mysql_checksum_expr(column: "ColumnDef") -> Optional[str]:
    """Inner MySQL render expression for one column (``None`` = omit from checksum).

    Normalizes each divergent type to the SAME canonical text the PG side
    produces (see :func:`_pg_checksum_expr`). ``float`` and ``json`` return ``None``
    so FLOAT/DOUBLE and JSON are excluded (no byte-identical cross-engine text form
    exists: floats have no exact decimal string, and JSON's whitespace/formatting
    differs between MySQL's canonical form and the CDC sink's compact serialization).
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
        # to_char(round(...)) side byte-for-byte.
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


def _pg_checksum_expr(column: "ColumnDef") -> "Optional[sql.Composed]":
    """Inner PG render expression for one column (``None`` = omit from checksum).

    The byte-identical counterpart of :func:`_mysql_checksum_expr`.
    """
    ident = sql.Identifier(column.name)
    kind = _checksum_kind(column)
    if kind == "binary":
        return sql.SQL("encode({col}, 'hex')").format(col=ident)
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
    if kind == "numeric":
        # An UNCONSTRAINED numeric (bare ``numeric``/``decimal``, no parens -> arbitrary
        # precision AND scale, a PostgreSQL-source case) has no declared scale. Aurora
        # DSQL stores such a column at its DEFAULT scale numeric(18,6), so a source value
        # is rounded to 6 fractional digits on the target (0.5 -> 0.500000). Compare at
        # that scale -- round BOTH sides to 6 -- so an equal value matches despite the
        # source's arbitrary scale, AND a difference WITHIN what DSQL can store is still
        # caught (rounding to scale 0, the old bug, hid the whole fraction -> false MATCH;
        # comparing raw ::text false-MISMATCHED 0.5 vs 0.500000). Source precision beyond
        # 6 digits that DSQL cannot hold is a SCHEMA concern (surfaced in Schema
        # Conversion), not a Validation mismatch. A DECLARED scale (numeric(p,s), incl.
        # numeric(p) == scale 0, and every MySQL DECIMAL(p,s)) keeps that exact scale, so
        # a trailing-zero / stored-scale diff never false-mismatches cross-engine.
        scale = (
            _DSQL_DEFAULT_NUMERIC_SCALE
            if "(" not in column.mysql_type
            else _decimal_scale(column.mysql_type)
        )
        return sql.SQL("to_char(round({col}, {scale}), {mask})").format(
            col=ident,
            scale=sql.Literal(scale),
            mask=sql.Literal(_pg_numeric_mask(scale)),
        )
    if kind in ("float", "json"):
        return None
    return sql.SQL("{col}::text").format(col=ident)


# ---------------------------------------------------------------------------
# Checksum SQL builders (cross-engine normalized per-column rendering)
# ---------------------------------------------------------------------------


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
    """
    rendered = [_mysql_checksum_expr(column) for column in table.columns]
    columns = ", ".join(
        _mysql_concat_term(expr) for expr in rendered if expr is not None
    )
    if not columns:
        columns = f"'{_NULL_SENTINEL}'"
    return (
        "CAST(CONV(SUBSTRING(MD5(CONCAT_WS('|', "
        f"{columns})), 1, {_CHECKSUM_HEX_DIGITS}), 16, 10) AS DECIMAL(65, 0))"
    )


def _pg_row_token(table: TableDef) -> "sql.Composed":
    """PostgreSQL per-row checksum token -- the byte-identical counterpart of
    :func:`_mysql_row_token` (same MD5-prefix reduction as a positive ``bigint``).

    The single definition shared by :func:`build_pg_checksum_sql`,
    :func:`build_pg_pk_token_sql`, and the paged :func:`build_pg_page_checksum_first_sql`.
    """
    rendered_pg = [_pg_checksum_expr(column) for column in table.columns]
    terms = [_pg_concat_term(expr) for expr in rendered_pg if expr is not None]
    if not terms:
        terms = [sql.Literal(_NULL_SENTINEL)]
    column_terms = sql.SQL(", ").join(terms)
    digits = sql.Literal(_CHECKSUM_HEX_DIGITS)
    return sql.SQL(
        "('x' || lpad(substr(md5(concat_ws('|', {terms})), 1, {digits}), 16, '0'))"
        "::bit(64)::bigint"
    ).format(terms=column_terms, digits=digits)


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


def build_pg_checksum_sql(table: TableDef) -> sql.Composed:
    """Build an order-independent PostgreSQL checksum query for ``table``.

    Mirrors :func:`build_mysql_checksum_sql`: the first ``_CHECKSUM_HEX_DIGITS``
    MD5 hex digits of each row are summed as a positive ``bigint``. Each column is
    rendered with the engine-normalized :func:`_pg_checksum_expr` inside
    ``COALESCE(..., <sentinel>)`` (FLOAT/DOUBLE and JSON columns are omitted). Identifiers
    are composed with :class:`psycopg.sql.Identifier` so a column/table name can
    never break out of the SQL (Requirement 9.4). All access is a single
    ``SELECT`` (read-only).
    """
    return sql.SQL(
        "SELECT COALESCE(SUM({token}), 0) FROM {table}"
    ).format(token=_pg_row_token(table), table=_pg_table_identifier(table.name))


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


def build_pg_pk_token_sql(table: TableDef, pk_column: str, sample_size: int) -> sql.Composed:
    """Build the bounded ``(pk, per-row token)`` PostgreSQL counterpart.

    Mirrors :func:`build_mysql_pk_token_sql` using the same per-row token as
    :func:`build_pg_checksum_sql`. Identifiers are composed with
    :class:`psycopg.sql.Identifier` and the bound is a :class:`psycopg.sql.Literal`
    so nothing can break out of the SQL (Requirement 9.4). A single read-only
    ``SELECT`` ordered by primary key and bounded by ``LIMIT`` -- no scan, no count.
    """
    return sql.SQL(
        "SELECT {pk} AS pk, {token} AS tok FROM {table} ORDER BY {pk} LIMIT {limit}"
    ).format(
        pk=sql.Identifier(pk_column), token=_pg_row_token(table),
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
    table: TableDef, pk_column: str, page_size: int
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
        token=_pg_row_token(table),
        table=_pg_table_identifier(table.name),
        limit=sql.Literal(page_size),
    )


def build_pg_page_checksum_next_sql(
    table: TableDef, pk_column: str, page_size: int
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
        token=_pg_row_token(table),
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


def build_orphan_count_sql(child_table: str, fk: ForeignKeyDef) -> sql.Composed:
    """Build a target query counting orphan child rows for one foreign key.

    A row is an orphan when all of its foreign-key columns are non-null (a null
    key is not a referential violation) yet no parent row matches on the
    referenced columns. Identifiers are composed with
    :class:`psycopg.sql.Identifier` (Requirement 9.4) and the statement is a
    single read-only ``SELECT``.
    """
    # Split a schema-qualified name into "schema"."table" (NOT one quoted
    # "schema.table" identifier, which would reference a table that doesn't exist):
    # the same composition the COUNT/checksum/reconcile queries use.
    child = _pg_table_identifier(child_table)
    parent = _pg_table_identifier(fk.referenced_table)

    not_null = sql.SQL(" AND ").join(
        sql.SQL("c.{column} IS NOT NULL").format(column=sql.Identifier(column))
        for column in fk.columns
    )
    join_predicate = sql.SQL(" AND ").join(
        sql.SQL("p.{ref} = c.{col}").format(
            ref=sql.Identifier(ref), col=sql.Identifier(col)
        )
        for col, ref in zip(fk.columns, fk.referenced_columns)
    )
    return sql.SQL(
        "SELECT COUNT(*) FROM {child} AS c WHERE {not_null} AND NOT EXISTS ("
        "SELECT 1 FROM {parent} AS p WHERE {join_predicate})"
    ).format(
        child=child,
        not_null=not_null,
        parent=parent,
        join_predicate=join_predicate,
    )
