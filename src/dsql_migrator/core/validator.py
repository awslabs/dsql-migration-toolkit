# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Consistency validation of migrated data (design.md "6. Validator").

The :class:`Validator` compares a migrated DSQL target against its MySQL source
and produces a :class:`~dsql_migrator.core.models.ValidationReport`
(Requirement 6). It implements:

- Per-table row-count comparison (Requirement 6.1).
- Sample/checksum-based data comparison (Requirement 6.2): in
  :attr:`~dsql_migrator.core.models.ValidationMode.CHECKSUM` mode an
  order-independent per-table checksum is computed on both sides and compared,
  so a reported match means the data itself is equal.
- Optional orphan-record check (Requirement 6.3): because DSQL has no foreign
  keys, referential integrity moves to the application; this checks each
  preserved foreign-key rule (:class:`~dsql_migrator.core.models.ForeignKeyDef`)
  for child rows on the target whose key has no matching parent row.
- Validation report including mismatches (Requirement 6.4).
- As-of-watermark validation with drift reporting (Requirement 6.5): when a
  :class:`~dsql_migrator.core.models.Watermark` is supplied, per-table source row
  counts are taken from the watermark snapshot (the consistency point) and the
  current source GTID is compared to the watermark's to report changes since the
  snapshot (Property 11).

Two correctness properties are central here:

- **Validation soundness (Property 9).** ``matched``/``is_match`` are computed so
  that a reported match can never hide an unequal row count or checksum: a table
  is ``matched`` only when its counts are equal and (in CHECKSUM mode) its
  checksums are equal, and the report ``is_match`` only when every table matched
  and no orphans were found. This is sound by construction
  (:meth:`~dsql_migrator.core.models.ValidationReport.build`).
- **Read-only source (Property 1).** Every source statement is a ``SELECT`` or
  transaction-control statement issued inside a single
  ``START TRANSACTION WITH CONSISTENT SNAPSHOT`` ... ``COMMIT`` (the same
  read-only-guarded engine used by introspection/export). The target is only
  ever read with ``SELECT`` as well.

Dependencies are injectable -- the source engine factory (like
:class:`~dsql_migrator.core.introspector.SourceIntrospector`) and the target
connection factory (like :mod:`dsql_migrator.core.batched_import`) -- so unit
tests never reach a real MySQL or DSQL cluster.

Cross-engine checksum note: the default MySQL and PostgreSQL checksum SQL
(:func:`build_mysql_checksum_sql` / :func:`build_pg_checksum_sql`) both reduce a
row to an order-independent sum of a 60-bit MD5 prefix, chosen to stay in a
positive range on both engines. Per-column text rendering is now engine-
NORMALIZED so equal data hashes equally on both sides: the NULL sentinel, bytea /
spatial WKB, BIT(n), boolean, and the temporal (timestamp / timestamptz / time)
and DECIMAL type-classes each render to the SAME canonical text on MySQL and
PostgreSQL (driven by the same converter classification the Full Load loader used
to STORE the value). The single exception is FLOAT / DOUBLE: no byte-identical
cross-engine shortest-round-trip text form exists and any fixed-precision rounding
would degrade soundness for exact values, so float columns are intentionally
EXCLUDED from the checksum concatenation entirely (:func:`_checksum_kind` returns
``"float"`` and both renderers return ``None`` for them). Their equality is still
covered by the row-count comparison and, for keys, by reconciliation. The checksum
therefore stays SOUND (a reported match always means the two computed checksums
were equal) and is now sensitive for the common real-schema types, not just a
best-effort prototype.
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Iterator, Mapping, Optional, Protocol

from psycopg import sql
from sqlalchemy import text
from sqlalchemy.engine import Engine

from dsql_migrator.core.converter import is_spatial_mysql_type, map_mysql_type
from dsql_migrator.core.introspector import _default_engine_factory
from dsql_migrator.core.models import (
    ColumnDef,
    DriftReport,
    ForeignKeyDef,
    OrphanFinding,
    ReconcileResult,
    RowDiffFinding,
    RowDiffKind,
    RowDiffSample,
    SourceConnectionConfig,
    TableDef,
    TargetConnectionConfig,
    ValidationMode,
    ValidationReport,
    Watermark,
    TableValidationResult,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.watermark import COMMIT, START_CONSISTENT_SNAPSHOT

# Number of leading MD5 hex digits used to build a per-row checksum token. 15
# hex digits = 60 bits, which stays positive in both MySQL's unsigned CONV and
# PostgreSQL's signed bigint, so equal data yields equal checksums on both.
_CHECKSUM_HEX_DIGITS = 15


class _SourceConnection(Protocol):
    """Minimal SQLAlchemy-style source connection used by the read helpers."""

    def execute(self, statement: object, parameters: object = ...) -> object: ...


# A source engine factory builds a read-only-guarded SQLAlchemy engine for a
# connection config (default reuses the introspector's MySQL factory).
SourceEngineFactory = Callable[[SourceConnectionConfig], Engine]

# A target connection factory opens one psycopg-style DSQL connection for a
# target config. Injectable so tests never reach a real cluster.
TargetConnectionFactory = Callable[[TargetConnectionConfig], Any]


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
        # round() to the declared scale, then a fixed-scale to_char mask matching
        # MySQL CAST(col AS DECIMAL(65, s)) so equal decimals print identically.
        scale = _decimal_scale(column.mysql_type)
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
    rendered = [_mysql_checksum_expr(column) for column in table.columns]
    columns = ", ".join(
        _mysql_concat_term(expr) for expr in rendered if expr is not None
    )
    # Edge case: an all-float (all-omitted) table -> one constant term so the
    # SQL stays valid and identical on both engines (row count still catches drift).
    if not columns:
        columns = f"'{_NULL_SENTINEL}'"
    table_sql = _quote_mysql_table(table.name)
    return (
        "SELECT COALESCE(SUM(CAST(CONV("
        f"SUBSTRING(MD5(CONCAT_WS('|', {columns})), 1, {_CHECKSUM_HEX_DIGITS}), 16, 10"
        f") AS DECIMAL(65, 0))), 0) FROM {table_sql}"
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
    rendered_pg = [_pg_checksum_expr(column) for column in table.columns]
    terms = [
        _pg_concat_term(expr) for expr in rendered_pg if expr is not None
    ]
    # Edge case: an all-float (all-omitted) table -> one constant sentinel term so
    # the SQL stays valid and hashes identically to the MySQL side.
    if not terms:
        terms = [sql.Literal(_NULL_SENTINEL)]
    column_terms = sql.SQL(", ").join(terms)
    digits = sql.Literal(_CHECKSUM_HEX_DIGITS)
    return sql.SQL(
        "SELECT COALESCE(SUM("
        "('x' || lpad(substr(md5(concat_ws('|', {terms})), 1, {digits}), 16, '0'))"
        "::bit(64)::bigint), 0) FROM {table}"
    ).format(terms=column_terms, digits=digits, table=_pg_table_identifier(table.name))


def build_mysql_pk_token_sql(table: TableDef, pk_column: str) -> str:
    """Build a bounded ``(pk, per-row token)`` MySQL query for the row-diff sample.

    Selects each row's primary key and the SAME per-row MD5/CONV token used inside
    :func:`build_mysql_checksum_sql`'s ``SUM`` -- so a token match here means the
    exact row equality the table-level checksum trusts -- ordered by primary key
    and bounded by ``:sample_size`` (``ORDER BY pk LIMIT N``). No ``COUNT(*)`` and
    no full materialization: the engine streams in PK order and stops at the LIMIT,
    reading only the first N rows via the primary-key index. Read-only.
    """
    rendered = [_mysql_checksum_expr(column) for column in table.columns]
    columns = ", ".join(
        _mysql_concat_term(expr) for expr in rendered if expr is not None
    )
    # Edge case: an all-float (all-omitted) table -> one constant term (matches
    # build_mysql_checksum_sql so the per-row token stays identical).
    if not columns:
        columns = f"'{_NULL_SENTINEL}'"
    pk_sql = _quote_mysql_identifier(pk_column)
    table_sql = _quote_mysql_table(table.name)
    return (
        f"SELECT {pk_sql} AS pk, CAST(CONV("
        f"SUBSTRING(MD5(CONCAT_WS('|', {columns})), 1, {_CHECKSUM_HEX_DIGITS}), 16, 10"
        f") AS DECIMAL(65, 0)) AS tok FROM {table_sql} "
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
    rendered_pg = [_pg_checksum_expr(column) for column in table.columns]
    terms = [
        _pg_concat_term(expr) for expr in rendered_pg if expr is not None
    ]
    # Edge case: an all-float (all-omitted) table -> one constant sentinel term
    # (matches build_pg_checksum_sql so the per-row token stays identical).
    if not terms:
        terms = [sql.Literal(_NULL_SENTINEL)]
    column_terms = sql.SQL(", ").join(terms)
    digits = sql.Literal(_CHECKSUM_HEX_DIGITS)
    return sql.SQL(
        "SELECT {pk} AS pk, "
        "('x' || lpad(substr(md5(concat_ws('|', {terms})), 1, {digits}), 16, '0'))"
        "::bit(64)::bigint AS tok FROM {table} ORDER BY {pk} LIMIT {limit}"
    ).format(
        pk=sql.Identifier(pk_column), terms=column_terms, digits=digits,
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


def _norm_pk(value: object) -> str:
    """Canonical primary-key string for cross-engine set comparison (int-safe).

    MySQL and psycopg can return a PK as ``int`` / ``Decimal`` / ``str`` depending
    on the column type; normalizing integral values to a plain base-10 string keeps
    a PK that is present on both sides from being spuriously classified as
    missing/extra purely due to representation (mirrors ``max_pk_source``).
    """
    try:
        return str(int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Source reads (SQLAlchemy, read-only)
# ---------------------------------------------------------------------------


def _source_count(connection: _SourceConnection, table_name: str) -> int:
    """Return ``COUNT(*)`` for a source table (read-only ``SELECT``)."""
    statement = text(
        f"SELECT COUNT(*) FROM {_quote_mysql_table(table_name)}"
    )
    value = connection.execute(statement).scalar()  # type: ignore[attr-defined]
    return int(value) if value is not None else 0


def _source_checksum(connection: _SourceConnection, table: TableDef) -> str:
    """Return the source checksum for ``table`` as a string (read-only)."""
    value = connection.execute(text(build_mysql_checksum_sql(table))).scalar()  # type: ignore[attr-defined]
    return "0" if value is None else str(value)


def _source_pk_tokens(
    connection: _SourceConnection, table: TableDef, pk_column: str, sample_size: int
) -> dict[str, str]:
    """Return up to ``sample_size`` ``{pk: token}`` entries for the source (read-only).

    Bounded by ``LIMIT :sample_size`` -- at most ``sample_size`` rows are read, in
    PK order, via the primary-key index. Both PK and token are stringified for
    cross-engine comparison against the target.
    """
    statement = text(build_mysql_pk_token_sql(table, pk_column))
    result = connection.execute(statement, {"sample_size": sample_size})  # type: ignore[attr-defined]
    return {_norm_pk(row[0]): str(row[1]) for row in result}


def _source_gtid(connection: _SourceConnection) -> Optional[str]:
    """Read the current source ``@@GLOBAL.gtid_executed`` for drift, or ``None``.

    A failure (e.g. GTID mode off or restricted) degrades gracefully to ``None``
    so validation still produces a report.
    """
    try:
        value = connection.execute(
            text("SELECT @@GLOBAL.gtid_executed")
        ).scalar()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - drift info is optional; degrade gracefully
        return None
    if value is None:
        return None
    text_value = str(value)
    return text_value or None


def _source_binlog_position(
    connection: _SourceConnection,
) -> tuple[Optional[str], Optional[int]]:
    """Read the source's current binlog ``(file, position)``, or ``(None, None)``.

    The drift fallback for a source without GTID -- which is the normal case on RDS
    MySQL 8.0, where GTID cannot be enabled. Read-only (``SHOW MASTER STATUS``) and
    best-effort: any failure degrades to ``(None, None)`` so validation still
    produces a report, exactly like :func:`_source_gtid`.
    """
    try:
        row = connection.execute(
            text("SHOW MASTER STATUS")
        ).mappings().first()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - drift info is optional; degrade gracefully
        return None, None
    if not row:
        return None, None
    file_name = row.get("File") or None
    position = row.get("Position")
    try:
        return file_name, (int(position) if position is not None else None)
    except (TypeError, ValueError):  # non-numeric position: unusable for comparison
        return file_name, None


# ---------------------------------------------------------------------------
# Target reads (psycopg, read-only)
# ---------------------------------------------------------------------------


def _target_scalar(connection: Any, statement: Any) -> object:
    """Execute a read-only ``SELECT`` on the target and return the first column."""
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
        row = cursor.fetchone()
    finally:
        _safe_close(cursor)
    if row is None:
        return None
    return row[0]


def _target_count(connection: Any, table_name: str) -> int:
    """Return ``COUNT(*)`` for a target table (read-only ``SELECT``).

    A single unbounded scan: used ONLY as the fallback for a composite/missing PK
    (see :func:`_bounded_target_count`); on a very large such table it can still hit
    Aurora DSQL's 300s transaction limit -- a documented residual gap.
    """
    statement = sql.SQL("SELECT COUNT(*) FROM {table}").format(
        table=_pg_table_identifier(table_name)
    )
    value = _target_scalar(connection, statement)
    return int(value) if value is not None else 0


def _target_count_keyset(
    connection: Any, table: TableDef, pk_column: str, page_size: int
) -> int:
    """Exact target row count via BOUNDED keyset paging on a single-column PK.

    A single ``SELECT COUNT(*)`` scans the whole target in ONE transaction and, on a
    large table, exceeds Aurora DSQL's hard 300s transaction limit ("transaction age
    limit of 300s exceeded"), so a big table could otherwise never be validated. This
    pages the PK index (``WHERE pk > :last ORDER BY pk LIMIT N`` -- the same keyset
    stream reconciliation uses) and sums the per-page row counts, so every statement
    stays well under the limit and memory stays at one page. Reads only the PK column
    (Property 7); works for any orderable single-column PK (int/uuid/varchar/binary).
    """
    first_sql = build_pg_pk_first_page_sql(table, pk_column, page_size)
    next_sql = build_pg_pk_next_page_sql(table, pk_column, page_size)
    total = 0
    last: object = None
    while True:
        cursor = connection.cursor()
        try:
            if last is None:
                cursor.execute(first_sql)
            else:
                cursor.execute(next_sql, {"last": last})
            rows = cursor.fetchall()
        finally:
            _safe_close(cursor)
        total += len(rows)
        if len(rows) < page_size:
            return total
        last = rows[-1][0]


def _bounded_target_count(connection: Any, table: TableDef, page_size: int) -> int:
    """Target row count, bounded (keyset) for a single-column PK; ``COUNT(*)`` else.

    A composite/missing PK has no single keyset column, so it falls back to the
    unbounded ``COUNT(*)`` (which can still time out on a very large composite-PK
    table -- a residual gap not covered here).
    """
    pk_column = single_pk_column(table)
    if pk_column is not None:
        return _target_count_keyset(connection, table, pk_column, page_size)
    return _target_count(connection, table.name)


def _target_checksum(connection: Any, table: TableDef) -> str:
    """Return the target checksum for ``table`` as a string (read-only)."""
    value = _target_scalar(connection, build_pg_checksum_sql(table))
    return "0" if value is None else str(value)


def _target_pk_tokens(
    connection: Any, table: TableDef, pk_column: str, sample_size: int
) -> dict[str, str]:
    """Return up to ``sample_size`` ``{pk: token}`` entries for the target (read-only).

    Bounded by ``LIMIT`` -- at most ``sample_size`` rows are read, in PK order, via
    the primary-key index. PK and token stringified for cross-engine comparison.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(build_pg_pk_token_sql(table, pk_column, sample_size))
        rows = cursor.fetchall()
    finally:
        _safe_close(cursor)
    return {_norm_pk(row[0]): str(row[1]) for row in rows}


def _diff_pks(
    source_connection: _SourceConnection,
    target_connection: Any,
    table: TableDef,
    sample_size: int,
) -> Optional[RowDiffSample]:
    """Sample which primary keys diverge between source and target for ``table``.

    Reads at most ``sample_size`` ``(pk, token)`` rows from each side in PK order
    (``ORDER BY pk LIMIT N`` -- bounded, no scan), then classifies by set
    difference: source-only -> ``MISSING_ON_TARGET``; target-only ->
    ``EXTRA_ON_TARGET``; present on both with differing tokens -> ``VALUE_MISMATCH``.
    Findings are capped at ``sample_size`` (``truncated`` flags the cap). Returns
    ``None`` for a composite/missing primary key (skipped, never scanned). Logs
    PK values + checksum tokens only -- never row values (Property 7); a natural-key
    PK is the operator's risk and the feature is dev-gated/default-off.
    """
    if len(table.primary_key) != 1:
        return None  # composite/missing PK: skip rather than fall back to a scan
    pk_column = table.primary_key[0]

    source_tokens = _source_pk_tokens(source_connection, table, pk_column, sample_size)
    target_tokens = _target_pk_tokens(target_connection, table, pk_column, sample_size)

    findings: list[RowDiffFinding] = []
    for pk in sorted(set(source_tokens) | set(target_tokens)):
        if len(findings) >= sample_size:
            break
        in_source = pk in source_tokens
        in_target = pk in target_tokens
        if in_source and not in_target:
            findings.append(RowDiffFinding(
                pk=pk, kind=RowDiffKind.MISSING_ON_TARGET,
                source_checksum=source_tokens[pk],
            ))
        elif in_target and not in_source:
            findings.append(RowDiffFinding(
                pk=pk, kind=RowDiffKind.EXTRA_ON_TARGET,
                target_checksum=target_tokens[pk],
            ))
        elif source_tokens[pk] != target_tokens[pk]:
            findings.append(RowDiffFinding(
                pk=pk, kind=RowDiffKind.VALUE_MISMATCH,
                source_checksum=source_tokens[pk], target_checksum=target_tokens[pk],
            ))

    # truncated: the sample windows were full on either side, so divergences may
    # exist beyond the first N PKs (the sample explains, it does not bound).
    truncated = (
        len(findings) >= sample_size
        or len(source_tokens) >= sample_size
        or len(target_tokens) >= sample_size
    )
    return RowDiffSample(
        pk_column=pk_column,
        sample_size=sample_size,
        truncated=truncated,
        findings=findings,
    )


# ---------------------------------------------------------------------------
# Full PK-set reconciliation (streaming keyset merge, bounded memory)
# ---------------------------------------------------------------------------
#
# Default rows fetched per keyset page during reconciliation. Bounded so memory
# stays at one page per side regardless of table size (large-scale design).
_RECONCILE_PAGE_SIZE = 5000

# How many diverging PKs to record per side in a ReconcileResult sample. The full
# divergence COUNT is always exact; only the listed example PKs are capped.
_RECONCILE_SAMPLE_CAP = 50


def _iter_source_pks(
    connection: _SourceConnection, table: TableDef, pk_column: str, page_size: int
) -> "Iterator[int]":
    """Yield every source PK in ascending order via bounded keyset pagination.

    Reads one ``page_size``-row page at a time (``WHERE pk > :last ORDER BY pk
    LIMIT N``), advancing the keyset, so the table is never materialized. Only
    the integer PK column is read (Property 7). Read-only.
    """
    first_sql = text(build_mysql_pk_first_page_sql(table, pk_column))
    next_sql = text(build_mysql_pk_next_page_sql(table, pk_column))
    last: object = None
    while True:
        if last is None:
            result = connection.execute(first_sql, {"page": page_size})
        else:
            result = connection.execute(next_sql, {"last": last, "page": page_size})
        count = 0
        for row in result:  # type: ignore[attr-defined]
            count += 1
            last = row[0]
            yield int(row[0])
        if count < page_size:
            return


def _iter_target_pks(
    connection: Any, table: TableDef, pk_column: str, page_size: int
) -> "Iterator[int]":
    """Yield every target PK in ascending order via bounded keyset pagination.

    Mirrors :func:`_iter_source_pks` on the psycopg target: one bounded page per
    round-trip, keyset-advanced, so a billion-row table never lands in RAM.
    """
    first_sql = build_pg_pk_first_page_sql(table, pk_column, page_size)
    next_sql = build_pg_pk_next_page_sql(table, pk_column, page_size)
    last: object = None
    while True:
        cursor = connection.cursor()
        try:
            if last is None:
                cursor.execute(first_sql)
            else:
                cursor.execute(next_sql, {"last": last})
            rows = cursor.fetchall()
        finally:
            _safe_close(cursor)
        for row in rows:
            last = row[0]
            yield int(row[0])
        if len(rows) < page_size:
            return


# Poll the cancel check every this-many merge steps during reconciliation. Small
# enough that a cancel takes effect within a few thousand rows even on a huge
# table, large enough that the check adds no measurable overhead.
_RECONCILE_CANCEL_POLL_EVERY = 4096


def reconcile_pk_streams(
    pk_column: str,
    source_pks: "Iterator[int]",
    target_pks: "Iterator[int]",
    *,
    sample_cap: int = _RECONCILE_SAMPLE_CAP,
    should_cancel: Optional["CancelCheck"] = None,
) -> ReconcileResult:
    """Merge two ascending PK streams into a :class:`ReconcileResult` (pure).

    Both inputs MUST yield integer primary keys in ascending order (the keyset
    streams do). A classic sorted-merge walks both at once with O(1) extra memory
    beyond the bounded sample lists: a PK on the source but not the target is
    ``missing_on_target`` (a lost / not-yet-replicated row); a PK on the target
    but not the source is ``extra_on_target`` (a delete CDC has not applied). The
    divergence COUNTS are exact and unbounded-safe; only the example PK lists are
    capped at ``sample_cap`` (``sample_truncated`` flags the cap). The table is
    ``consistent`` only when both divergence counts are zero.

    ``should_cancel`` (polled every few thousand merged rows) makes a cancel
    responsive WITHIN a single large table: the reconciliation of one big table is
    the longest unit of work, so without this a cooperative stop would wait for
    the whole table. When it fires, :class:`ValidationCancelled` is raised.

    Kept pure (takes iterators, returns a model) so it is unit-testable without
    any database and so the same merge powers both engines.
    """
    cancel = should_cancel or (lambda: False)
    source_count = 0
    target_count = 0
    missing = 0  # source-only
    extra = 0  # target-only
    missing_sample: list[str] = []
    extra_sample: list[str] = []

    sentinel = object()
    s = next(source_pks, sentinel)
    t = next(target_pks, sentinel)
    steps = 0
    while s is not sentinel or t is not sentinel:
        steps += 1
        if steps % _RECONCILE_CANCEL_POLL_EVERY == 0 and cancel():
            raise ValidationCancelled(
                "Validation cancelled during record reconciliation."
            )
        if t is sentinel or (s is not sentinel and s < t):  # type: ignore[operator]
            source_count += 1
            missing += 1
            if len(missing_sample) < sample_cap:
                missing_sample.append(str(s))
            s = next(source_pks, sentinel)
        elif s is sentinel or (t is not sentinel and t < s):  # type: ignore[operator]
            target_count += 1
            extra += 1
            if len(extra_sample) < sample_cap:
                extra_sample.append(str(t))
            t = next(target_pks, sentinel)
        else:  # s == t: present on both, advance both
            source_count += 1
            target_count += 1
            s = next(source_pks, sentinel)
            t = next(target_pks, sentinel)

    return ReconcileResult(
        pk_column=pk_column,
        source_count=source_count,
        target_count=target_count,
        missing_on_target=missing,
        extra_on_target=extra,
        missing_sample=missing_sample,
        extra_sample=extra_sample,
        sample_truncated=missing > len(missing_sample) or extra > len(extra_sample),
        consistent=missing == 0 and extra == 0,
    )


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


def _target_orphan_count(connection: Any, child_table: str, fk: ForeignKeyDef) -> int:
    """Return the number of orphan child rows for ``fk`` on the target."""
    value = _target_scalar(connection, build_orphan_count_sql(child_table, fk))
    return int(value) if value is not None else 0


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def _default_target_connection_factory(target: TargetConnectionConfig) -> Any:
    """Open a default autocommit/TLS/IAM DSQL connection via :class:`DsqlConnector`.

    The IAM token is generated and kept confidential by the connector
    (Property 7 / Requirement 5.4).
    """
    return DsqlConnector(target).connect()


# A cancel check polled between tables: returns True once a cooperative stop has
# been requested (e.g. the JobManager handle's ``cancelled`` flag).
CancelCheck = Callable[[], bool]

# A progress callback invoked BEFORE each table is compared, with the table name,
# its 1-based index, and the total table count -- so the UI can show "checking
# table i of N: <name>" while a long run streams. Best-effort: the validator
# ignores any exception it raises so reporting never breaks the comparison.
ProgressCallback = Callable[[str, int, int], None]


class ValidationCancelled(RuntimeError):
    """Raised when a validation run is cooperatively cancelled mid-way.

    Validation stops at the next table boundary (its consistent-snapshot
    transaction is still closed cleanly) and raises this instead of returning a
    partial :class:`~dsql_migrator.core.models.ValidationReport`, so a cancelled
    run is never mistaken for a completed comparison. The caller maps it to a
    CANCELLED job state, not a failure.
    """


class Validator:
    """Validates a migrated DSQL target against its MySQL source (Requirement 6).

    The source engine factory and target connection factory are injectable so
    unit tests never reach a real MySQL or DSQL cluster; the defaults use the
    read-only-guarded MySQL engine and an IAM-authenticated
    :class:`~dsql_migrator.core.target_connection.DsqlConnector` connection.
    """

    def __init__(
        self,
        *,
        source_engine_factory: Optional[SourceEngineFactory] = None,
        target_connection_factory: Optional[TargetConnectionFactory] = None,
        row_diff_sample_size: int = 0,
        reconcile_page_size: int = _RECONCILE_PAGE_SIZE,
    ) -> None:
        """Create a validator with optional injected source/target factories.

        ``row_diff_sample_size`` (default ``0`` == OFF) enables the dev-only
        row-level diff: for any table that does NOT match, up to this many
        diverging primary keys are sampled (``ORDER BY pk LIMIT N``) and attached
        to the result as a :class:`RowDiffSample`. It runs only for mismatched
        tables, only at validation time, never on the export/import hot path, and
        is bounded so a large table is never scanned. Logs PK + checksum tokens
        only -- never row values (Property 7).

        ``reconcile_page_size`` is the keyset page size used by the full PK-set
        reconciliation (Property 7: PK values only); kept injectable so a test can
        force multi-page streaming with a tiny table.
        """
        self._source_engine_factory = source_engine_factory or _default_engine_factory
        self._target_connection_factory = (
            target_connection_factory or _default_target_connection_factory
        )
        self._row_diff_sample_size = max(0, int(row_diff_sample_size))
        self._reconcile_page_size = max(1, int(reconcile_page_size))

    def validate(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        tables: list[TableDef],
        mode: ValidationMode = ValidationMode.ROW_COUNT,
        *,
        watermark: Optional[Watermark] = None,
        check_orphans: bool = False,
        reconcile: bool = False,
        should_cancel: Optional[CancelCheck] = None,
        on_progress: Optional[ProgressCallback] = None,
        max_workers: int = 1,
        deep_only_on_count_mismatch: bool = False,
        quarantined_by_table: Optional[Mapping[str, int]] = None,
    ) -> ValidationReport:
        """Compare ``tables`` between source and target and return a report.

        Per-table row counts are compared (Requirement 6.1); in
        :attr:`ValidationMode.CHECKSUM` mode an order-independent checksum is also
        compared (Requirement 6.2). When ``reconcile`` is set, every primary key
        is streamed from both sides and merged to find the EXACT missing/extra
        rows (the pre-cut-over "no mismatched records" check), bounded so a large
        table is never materialized. When ``check_orphans`` is set, each table's
        preserved foreign keys are checked for orphan child rows on the target
        (Requirement 6.3). When ``watermark`` is supplied, per-table source row
        counts are taken from the watermark snapshot and the current source GTID
        is compared to the watermark's to report drift (Requirement 6.5 /
        Property 11). All source access is read-only inside one consistent-
        snapshot transaction (Property 1); the target is only read.

        A failure comparing ONE table (e.g. it does not exist on the target, or a
        query errors) is isolated to that table's :class:`TableValidationResult`
        ``error`` and never aborts the whole run, so the report still surfaces
        every other table and the bad table is shown as a failed check ("table
        errors") instead of crashing Validation.

        ``should_cancel`` (when given) is polled BEFORE each table; once it returns
        ``True`` the run stops at that table boundary and raises
        :class:`ValidationCancelled` (the consistent-snapshot transaction and
        connections are still closed cleanly via ``finally``), so a cancelled run
        never returns a partial report mistaken for a completed comparison.

        ``max_workers`` bounds table-level parallelism (default ``1`` ==
        sequential, the historical behavior). With ``> 1`` the tables are compared
        concurrently across a bounded thread pool, EACH worker opening its OWN
        source connection + consistent-snapshot transaction and its own target
        connection (a DB connection/transaction is not shareable across threads).
        Per-table comparison is independent -- each table is internally consistent
        within its own snapshot -- so cross-table snapshot identity is not needed
        for a correct per-table verdict; this cuts a large multi-table run's wall
        clock from the sum of per-table scans toward the slowest single table.
        Drift (a whole-source signal) is still read once on the main thread. A
        cancel still stops promptly: queued tables are skipped and the pool drains.

        ``deep_only_on_count_mismatch`` (default ``False``) is a speed optimization
        for the common case where most tables already match: the cheap row counts
        always run, but the EXPENSIVE checks (CHECKSUM-mode checksum and full PK
        reconciliation) run only for a table whose counts DIFFER. A count-matched
        table then reports ``checksum_match``/``reconcile`` as ``None`` (not run --
        an honest "verified by count", never a false equality claim), so soundness
        holds: ``matched`` only reflects checks that actually ran. This skips the
        per-row scans on the tables that need them least, cutting wall clock sharply
        on a healthy migration while still deep-checking every table that looks off.
        """
        cancel = should_cancel or (lambda: False)
        workers = max(1, int(max_workers))
        if workers > 1 and len(tables) > 1:
            return _with_quarantine_counts(
                self._validate_parallel(
                    source, target, tables, mode,
                    watermark=watermark, check_orphans=check_orphans,
                    reconcile=reconcile, cancel=cancel, on_progress=on_progress,
                    max_workers=workers,
                    deep_only_on_count_mismatch=deep_only_on_count_mismatch,
                ),
                quarantined_by_table,
            )

        def _report_progress(table_name: str, index: int, total: int) -> None:
            if on_progress is None:
                return
            try:
                on_progress(table_name, index, total)
            except Exception:  # noqa: BLE001 - progress is advisory; never break a run
                pass

        source_engine = self._source_engine_factory(source)
        items: list[TableValidationResult] = []
        orphan_findings: list[OrphanFinding] = []
        current_gtid: Optional[str] = None
        try:
            with source_engine.connect() as raw_connection:
                source_connection = raw_connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                source_connection.execute(text(START_CONSISTENT_SNAPSHOT))
                try:
                    current_gtid = _source_gtid(source_connection)
                    # Also capture file:pos -- the drift fallback when the source has
                    # no GTID (the normal case on RDS MySQL 8.0).
                    current_binlog_file, current_binlog_position = (
                        _source_binlog_position(source_connection)
                    )
                    target_connection = self._target_connection_factory(target)
                    try:
                        total = len(tables)
                        for index, table in enumerate(tables, start=1):
                            # Cooperative stop at the table boundary: cleanly exit
                            # (COMMIT + dispose run in the finally blocks) and
                            # signal the cancellation rather than return a partial.
                            if cancel():
                                raise ValidationCancelled(
                                    "Validation cancelled before completing all "
                                    "tables."
                                )
                            _report_progress(table.name, index, total)
                            items.append(
                                self._validate_table(
                                    source_connection,
                                    target_connection,
                                    table,
                                    mode,
                                    watermark,
                                    reconcile,
                                    cancel,
                                    deep_only_on_count_mismatch=(
                                        deep_only_on_count_mismatch
                                    ),
                                )
                            )
                            if check_orphans:
                                orphan_findings.extend(
                                    self._check_orphans(target_connection, table)
                                )
                    finally:
                        _safe_close(target_connection)
                finally:
                    source_connection.execute(text(COMMIT))
        finally:
            source_engine.dispose()

        drift = _build_drift(
            watermark, current_gtid, current_binlog_file, current_binlog_position
        )
        snapshot_timestamp = (
            watermark.snapshot_timestamp if watermark is not None else None
        )
        return _with_quarantine_counts(
            ValidationReport.build(
                mode=mode,
                items=items,
                orphan_findings=orphan_findings,
                orphan_check_performed=check_orphans,
                drift=drift,
                snapshot_timestamp=snapshot_timestamp,
            ),
            quarantined_by_table,
        )

    def _validate_parallel(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        tables: list[TableDef],
        mode: ValidationMode,
        *,
        watermark: Optional[Watermark],
        check_orphans: bool,
        reconcile: bool,
        cancel: CancelCheck,
        on_progress: Optional[ProgressCallback],
        max_workers: int,
        deep_only_on_count_mismatch: bool = False,
    ) -> ValidationReport:
        """Compare ``tables`` concurrently, one bounded worker per in-flight table.

        Each table is an independent unit (its own source snapshot + target
        connection), so they run across a :class:`ThreadPoolExecutor` capped at
        ``max_workers``. Results are reassembled in the ORIGINAL table order (the
        report must read deterministically regardless of completion order). Drift
        is a whole-source signal, so the current GTID is read once on its own
        short-lived snapshot here. A cooperative cancel skips not-yet-started
        tables; a per-table failure stays isolated to that table's result.
        """
        from concurrent.futures import ThreadPoolExecutor

        total = len(tables)
        # Slot results by original index so completion order never reorders the report.
        results: list[Optional[TableValidationResult]] = [None] * total
        orphans_by_index: list[list[OrphanFinding]] = [[] for _ in range(total)]
        # Progress is reported as tables COMPLETE (not start) under concurrency, so the
        # count rises monotonically; guard the shared counter + callback with a lock.
        progress_lock = threading.Lock()
        done_count = 0

        def _run_one(index: int, table: TableDef) -> None:
            nonlocal done_count
            if cancel():
                return
            item, orphans = self._validate_one_table_isolated(
                source, target, table, mode, watermark, reconcile, check_orphans,
                cancel, deep_only_on_count_mismatch=deep_only_on_count_mismatch,
            )
            results[index] = item
            orphans_by_index[index] = orphans
            if on_progress is not None:
                with progress_lock:
                    done_count += 1
                    try:
                        on_progress(table.name, done_count, total)
                    except Exception:  # noqa: BLE001 - advisory; never break a run
                        pass

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_run_one, index, table)
                for index, table in enumerate(tables)
            ]
            for future in futures:
                future.result()  # re-raise a worker exception (e.g. unexpected fatal)

        if cancel():
            raise ValidationCancelled(
                "Validation cancelled before completing all tables."
            )

        items = [r for r in results if r is not None]
        orphan_findings: list[OrphanFinding] = []
        for found in orphans_by_index:
            orphan_findings.extend(found)

        current_gtid, current_binlog_file, current_binlog_position = (
            self._read_source_position(source)
        )
        drift = _build_drift(
            watermark, current_gtid, current_binlog_file, current_binlog_position
        )
        snapshot_timestamp = (
            watermark.snapshot_timestamp if watermark is not None else None
        )
        return ValidationReport.build(
            mode=mode,
            items=items,
            orphan_findings=orphan_findings,
            orphan_check_performed=check_orphans,
            drift=drift,
            snapshot_timestamp=snapshot_timestamp,
        )

    def _validate_one_table_isolated(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        table: TableDef,
        mode: ValidationMode,
        watermark: Optional[Watermark],
        reconcile: bool,
        check_orphans: bool,
        cancel: CancelCheck,
        deep_only_on_count_mismatch: bool = False,
    ) -> "tuple[TableValidationResult, list[OrphanFinding]]":
        """Compare ONE table on its own source snapshot + target connection.

        The self-contained unit the parallel path runs per worker: open a fresh
        source engine/connection in a consistent-snapshot transaction (so each
        worker is internally consistent and read-only -- Property 1), open a fresh
        target connection, compare the one table, and -- when requested -- its
        orphans, then close everything cleanly via ``finally``. A per-table failure
        is already isolated inside :meth:`_validate_table`.
        """
        source_engine = self._source_engine_factory(source)
        orphans: list[OrphanFinding] = []
        try:
            with source_engine.connect() as raw_connection:
                source_connection = raw_connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                source_connection.execute(text(START_CONSISTENT_SNAPSHOT))
                try:
                    target_connection = self._target_connection_factory(target)
                    try:
                        item = self._validate_table(
                            source_connection, target_connection, table,
                            mode, watermark, reconcile, cancel,
                            deep_only_on_count_mismatch=deep_only_on_count_mismatch,
                        )
                        if check_orphans:
                            orphans = list(
                                self._check_orphans(target_connection, table)
                            )
                    finally:
                        _safe_close(target_connection)
                finally:
                    source_connection.execute(text(COMMIT))
        finally:
            source_engine.dispose()
        return item, orphans

    def _read_source_position(
        self, source: SourceConnectionConfig
    ) -> tuple[Optional[str], Optional[str], Optional[int]]:
        """Read the source's current ``(gtid, binlog_file, binlog_position)`` once.

        Used by the parallel path, where no shared main-thread snapshot exists. Both
        coordinates are read on the SAME short-lived connection so they describe the
        same instant. The binlog pair is the drift fallback for a source without GTID
        -- the normal case on RDS MySQL 8.0, where a GTID-only comparison could never
        determine drift at all. Best-effort and read-only: any failure degrades to
        ``None`` values (drift becomes "undeterminable"), never aborting the run.
        """
        engine = self._source_engine_factory(source)
        try:
            with engine.connect() as raw_connection:
                connection = raw_connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                gtid = _source_gtid(connection)
                binlog_file, binlog_position = _source_binlog_position(connection)
                return gtid, binlog_file, binlog_position
        except Exception:  # noqa: BLE001 - drift is advisory
            return None, None, None
        finally:
            engine.dispose()

    def _validate_table(
        self,
        source_connection: _SourceConnection,
        target_connection: Any,
        table: TableDef,
        mode: ValidationMode,
        watermark: Optional[Watermark],
        reconcile: bool,
        should_cancel: CancelCheck,
        deep_only_on_count_mismatch: bool = False,
    ) -> TableValidationResult:
        """Compare one table, isolating any failure to this table's result.

        Delegates to :meth:`_compare_table`; if comparing this one table raises
        (e.g. it is missing on the target, or a query errors), the exception is
        caught and surfaced as the result's ``error`` (a failed "table errors"
        check) so one bad table never aborts the whole validation run. The
        message is rendered log-safe (no row values / credentials, Property 7).
        A :class:`ValidationCancelled` is NOT a per-table failure -- it is a
        cooperative stop, so it propagates to abort the whole run cleanly.
        """
        try:
            return self._compare_table(
                source_connection,
                target_connection,
                table,
                mode,
                watermark,
                reconcile,
                should_cancel,
                deep_only_on_count_mismatch=deep_only_on_count_mismatch,
            )
        except ValidationCancelled:
            raise  # cooperative stop: abort the run, not a per-table error
        except Exception as exc:  # noqa: BLE001 - isolate per-table failure
            return TableValidationResult(
                table=table.name,
                source_row_count=0,
                target_row_count=0,
                row_count_match=False,
                matched=False,
                error=_safe_error_message(exc),
            )

    def _compare_table(
        self,
        source_connection: _SourceConnection,
        target_connection: Any,
        table: TableDef,
        mode: ValidationMode,
        watermark: Optional[Watermark],
        reconcile: bool,
        should_cancel: CancelCheck,
        deep_only_on_count_mismatch: bool = False,
    ) -> TableValidationResult:
        """Compare one table's row count (and, in CHECKSUM mode, its checksum).

        The source row count comes from the watermark snapshot when available
        (as-of the consistency point, Requirement 6.5); otherwise it is read
        live. ``matched`` is computed soundly: equal counts, (in CHECKSUM mode)
        equal checksums, AND -- when reconciliation ran -- no missing/extra
        records (Property 9).

        When ``deep_only_on_count_mismatch`` is set, the expensive checksum and PK
        reconciliation are skipped for a table whose counts already AGREE (they
        leave ``checksum_match``/``reconcile`` as ``None`` -- not run, never a
        false equality); a count DISAGREEMENT still triggers the full deep checks
        so the exact diverging rows are found. Soundness is preserved: ``matched``
        only credits checks that actually ran.
        """
        source_row_count = self._source_row_count(
            source_connection, table, watermark
        )
        # The TARGET count must be BOUNDED on Aurora DSQL: a single COUNT(*) scans the
        # whole table in one transaction and, at scale, exceeds DSQL's hard 300s limit,
        # so a large table could never report MATCH. We prefer the exact count that
        # reconciliation ALREADY streams (keyset-paged) for an integer PK; otherwise we
        # keyset-page the count ourselves for any single-column PK; COUNT(*) is used only
        # for a composite/missing PK. In deep-only mode the count is needed UP FRONT to
        # decide whether to run the deep checks; otherwise it is deferred to after
        # reconcile so reconcile's count is reused with no second scan.
        target_row_count: Optional[int] = None
        row_count_match: Optional[bool] = None
        if deep_only_on_count_mismatch:
            target_row_count = _bounded_target_count(
                target_connection, table, self._reconcile_page_size
            )
            row_count_match = source_row_count == target_row_count

        # Fast path: when asked to deep-check only on a count mismatch, a table whose
        # counts agree skips the per-row checksum + reconciliation scans entirely.
        # The result honestly reports those as not-run (None), so a "match" here
        # means "verified by row count" -- exactly the ROW_COUNT contract.
        run_deep = not (deep_only_on_count_mismatch and row_count_match)

        source_checksum: Optional[str] = None
        target_checksum: Optional[str] = None
        checksum_match: Optional[bool] = None
        if mode is ValidationMode.CHECKSUM and run_deep:
            source_checksum = _source_checksum(source_connection, table)
            target_checksum = _target_checksum(target_connection, table)
            checksum_match = source_checksum == target_checksum

        # Full PK-set reconciliation (the "no mismatched records" check): stream
        # every PK from both sides and merge. Only for single-column integer PKs
        # (well-defined cross-engine order); other tables fall back to
        # count/checksum (reconcile stays None). Bounded keyset paging -- never a
        # full materialization (Property 7: PK values only).
        reconcile_result: Optional[ReconcileResult] = None
        if reconcile and run_deep:
            pk_column = integer_pk_column(table)
            if pk_column is not None:
                reconcile_result = reconcile_pk_streams(
                    pk_column,
                    _iter_source_pks(
                        source_connection, table, pk_column, self._reconcile_page_size
                    ),
                    _iter_target_pks(
                        target_connection, table, pk_column, self._reconcile_page_size
                    ),
                    should_cancel=should_cancel,
                )

        # Default path: the target count was deferred so it could reuse reconcile's
        # exact keyset-streamed count (no second scan); when reconcile did not run
        # (non-integer PK, reconcile off, or CHECKSUM-only), keyset-page it directly --
        # a single unbounded COUNT(*) is used only for a composite/missing PK.
        if target_row_count is None:
            target_row_count = (
                reconcile_result.target_count
                if reconcile_result is not None
                else _bounded_target_count(
                    target_connection, table, self._reconcile_page_size
                )
            )
            row_count_match = source_row_count == target_row_count

        matched = (
            row_count_match
            and (checksum_match is not False)
            and (reconcile_result is None or reconcile_result.consistent)
        )

        # Dev-only diagnostic: for a table that did NOT match, sample which PKs
        # diverge. Bounded (LIMIT N), mismatched-tables-only, validation-time-only
        # -- never the hot path, never a full scan. Off when sample size is 0.
        row_diff_sample: Optional[RowDiffSample] = None
        if not matched and self._row_diff_sample_size > 0:
            row_diff_sample = _diff_pks(
                source_connection, target_connection, table,
                self._row_diff_sample_size,
            )

        return TableValidationResult(
            table=table.name,
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            row_count_match=row_count_match,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            checksum_match=checksum_match,
            matched=matched,
            row_diff_sample=row_diff_sample,
            reconcile=reconcile_result,
            deep_checks_skipped=not run_deep,
        )

    @staticmethod
    def _source_row_count(
        source_connection: _SourceConnection,
        table: TableDef,
        watermark: Optional[Watermark],
    ) -> int:
        """Return the as-of source row count for ``table``.

        Uses the watermark's recorded snapshot count only when it is exact; the
        Full Load watermark stores approximate (scan-free) estimates to spare the
        source, so validation re-counts the source live (exact) for a correct
        row-count comparison. The exact ``COUNT(*)`` therefore runs only at
        validation time, never during the migration itself.
        """
        if (
            watermark is not None
            and not watermark.row_counts_approximate
            and table.name in watermark.table_row_counts
        ):
            return watermark.table_row_counts[table.name]
        return _source_count(source_connection, table.name)

    @staticmethod
    def _check_orphans(
        target_connection: Any, table: TableDef
    ) -> list[OrphanFinding]:
        """Check each preserved foreign key on ``table`` for orphan child rows.

        Returns one :class:`OrphanFinding` per foreign key that has at least one
        orphan on the target (clean keys produce no finding).
        """
        findings: list[OrphanFinding] = []
        for fk in table.foreign_keys:
            orphan_count = _target_orphan_count(target_connection, table.name, fk)
            if orphan_count > 0:
                findings.append(
                    OrphanFinding(
                        table=table.name,
                        foreign_key=fk.name,
                        referenced_table=fk.referenced_table,
                        orphan_count=orphan_count,
                    )
                )
        return findings


def format_binlog_coordinate(
    file_name: Optional[str], position: Optional[int]
) -> Optional[str]:
    """Render a binlog coordinate as ``file:position``, or ``None`` if incomplete.

    Both halves are required: a file without a position cannot be compared, and a
    position without a file is meaningless (positions restart per file).
    """
    if not file_name or position is None:
        return None
    return f"{file_name}:{position}"


def binlog_advanced(
    watermark_file: Optional[str],
    watermark_position: Optional[int],
    current_file: Optional[str],
    current_position: Optional[int],
) -> Optional[bool]:
    """Whether the source advanced, comparing binlog coordinates. Pure.

    Returns ``None`` when either coordinate is incomplete (undeterminable).

    Only EQUALITY is needed, not ordering, which keeps this correct without having to
    reason about rotation: the position restarts at the top of each new binlog file,
    so a later file can hold a SMALLER position and any "is it greater" comparison of
    the raw values would be wrong. So: same file -> the position must be identical to
    count as unchanged; different file -> the log rotated (or was reset), which only
    happens if the server kept writing. Either way an inequality means the source did
    not stand still, which is exactly the question being asked. A coordinate that
    moved backwards (restored/rebuilt source, ``RESET MASTER``) therefore also reads
    as drift -- correctly, since it is certainly not "unchanged since the snapshot".
    """
    if not watermark_file or watermark_position is None:
        return None
    if not current_file or current_position is None:
        return None
    if watermark_file == current_file:
        return current_position != watermark_position
    # Different files -> the log rotated (or was reset), which means writes happened.
    return True


def _with_quarantine_counts(
    report: ValidationReport, quarantined_by_table: Optional[Mapping[str, int]]
) -> ValidationReport:
    """Attach per-table permanently-dropped row counts to a finished report.

    Applied once at the end of :meth:`Validator.validate` (both the serial and parallel
    paths) rather than threaded through every comparison layer: the counts come from the
    migration job, not from comparing the databases, so they are metadata ABOUT the run
    rather than an input to it. Keeping them out of the comparison also keeps
    ``_compare_table`` a pure source-vs-target function.

    Deliberately does NOT touch ``matched`` or the report's own verdict: the rows really
    are missing from the target, so a table that dropped rows must keep failing. This
    only records WHY, so the UI can separate an expected gap from unexplained loss --
    the operator previously had to reconstruct that by hand from the error log.

    Returns the report unchanged when there are no counts to attach.
    """
    if not quarantined_by_table:
        return report
    updated = [
        item.model_copy(
            update={"rows_quarantined": quarantined_by_table.get(item.table, 0) or 0}
        )
        for item in report.items
    ]
    return report.model_copy(update={"items": updated})


def _build_drift(
    watermark: Optional[Watermark],
    current_gtid: Optional[str],
    current_binlog_file: Optional[str] = None,
    current_binlog_position: Optional[int] = None,
) -> Optional[DriftReport]:
    """Build a drift report from the watermark and the source's current position.

    Returns ``None`` when no watermark was supplied (drift is undefined without a
    consistency point). GTID is preferred when BOTH sides have one; otherwise this
    falls back to comparing binlog ``file:position`` (Requirement 6.5 / Property 11).

    The fallback is the normal path on the primary supported source: RDS MySQL 8.0
    cannot enable GTID, so a GTID-only comparison reported "could not be determined"
    on every single run -- the section could never say anything. The watermark already
    captures file:pos (``SHOW MASTER STATUS`` returns it alongside the GTID, and CDC
    already resumes from it), so the coordinate needed to answer the question was
    being collected and then ignored.
    """
    if watermark is None:
        return None
    watermark_gtid = watermark.gtid_executed
    watermark_binlog = format_binlog_coordinate(
        watermark.binlog_file, watermark.binlog_position
    )
    current_binlog = format_binlog_coordinate(
        current_binlog_file, current_binlog_position
    )

    if watermark_gtid is not None and current_gtid is not None:
        drifted = watermark_gtid != current_gtid
        return DriftReport(
            watermark_gtid=watermark_gtid,
            current_gtid=current_gtid,
            drifted=drifted,
            detail=(
                "Source advanced since the snapshot (GTID changed)."
                if drifted
                else "No source changes since the snapshot."
            ),
            basis="gtid",
            watermark_binlog=watermark_binlog,
            current_binlog=current_binlog,
        )

    advanced = binlog_advanced(
        watermark.binlog_file,
        watermark.binlog_position,
        current_binlog_file,
        current_binlog_position,
    )
    if advanced is not None:
        return DriftReport(
            watermark_gtid=watermark_gtid,
            current_gtid=current_gtid,
            drifted=advanced,
            detail=(
                "Source advanced since the snapshot (binlog position moved from "
                f"{watermark_binlog} to {current_binlog})."
                if advanced
                else "No source changes since the snapshot (binlog position "
                f"unchanged at {watermark_binlog})."
            ),
            basis="binlog",
            watermark_binlog=watermark_binlog,
            current_binlog=current_binlog,
        )

    return DriftReport(
        watermark_gtid=watermark_gtid,
        current_gtid=current_gtid,
        drifted=False,
        detail=(
            "Neither a GTID nor a binlog position was available on both sides, so "
            "drift since the snapshot could not be determined."
        ),
        basis="",
        watermark_binlog=watermark_binlog,
        current_binlog=current_binlog,
    )


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise
        pass


def _safe_error_message(exc: Exception) -> str:
    """Render a per-table comparison failure as a short, log-safe message.

    Keeps only the exception type and its message text (the first line), so a
    table whose comparison failed is explained ("relation does not exist",
    "connection reset") without leaking a multi-line driver dump or any row
    values / credentials into the report (Property 7).
    """
    text_value = str(exc).strip().splitlines()
    detail = text_value[0].strip() if text_value else ""
    name = type(exc).__name__
    return f"{name}: {detail}" if detail else name


# ---------------------------------------------------------------------------
# Report rendering / export
# ---------------------------------------------------------------------------


def _render_table_line(item: TableValidationResult) -> str:
    """Render one per-table validation result as a readable text line."""
    # A table that could not be compared at all is reported as an error line.
    if item.error is not None:
        return f"- {item.table}: ERROR -- {item.error} [ERROR]"

    verdict = "MATCH" if item.matched else "MISMATCH"
    parts = [
        f"- {item.table}: source={item.source_row_count} "
        f"target={item.target_row_count} "
        f"(row count {'match' if item.row_count_match else 'MISMATCH'})"
    ]
    if item.checksum_match is not None:
        parts.append(
            f"checksum {'match' if item.checksum_match else 'MISMATCH'}"
        )
    reconcile = item.reconcile
    if reconcile is not None:
        parts.append(
            f"records {'consistent' if reconcile.consistent else 'INCONSISTENT'} "
            f"(missing={reconcile.missing_on_target}, extra={reconcile.extra_on_target})"
        )
    parts.append(f"[{verdict}]")
    line = ", ".join(parts)

    # Name the diverging PKs from the full reconciliation (PK values only --
    # never row values, Property 7): missing on target (lost / not-yet-replicated
    # rows) and extra on target (a source delete CDC has not applied).
    if reconcile is not None and not reconcile.consistent:
        sub = [line]
        if reconcile.missing_sample:
            sub.append(f"    missing on target (pk={reconcile.pk_column}): "
                       f"{reconcile.missing_sample}")
        if reconcile.extra_sample:
            sub.append(f"    extra on target (pk={reconcile.pk_column}): "
                       f"{reconcile.extra_sample}")
        if reconcile.sample_truncated:
            sub.append("    (sample truncated -- more diverging PKs exist)")
        line = "\n".join(sub)

    # Dev row-diff sample (only present when enabled and the table mismatched):
    # name the diverging PKs (PK + checksum tokens only -- never row values).
    sample = item.row_diff_sample
    if sample is not None:
        sub = [line, f"  diff sample (pk={sample.pk_column}, top-{sample.sample_size}):"]
        if not sample.findings:
            sub.append("    (no divergence within the sampled PK window)")
        for f in sample.findings:
            toks = []
            if f.source_checksum is not None:
                toks.append(f"src={f.source_checksum}")
            if f.target_checksum is not None:
                toks.append(f"tgt={f.target_checksum}")
            tok_str = f" [{' '.join(toks)}]" if toks else ""
            sub.append(f"    {f.pk} {f.kind.value}{tok_str}")
        if sample.truncated:
            sub.append("    (sample truncated -- more divergences may exist beyond the window)")
        return "\n".join(sub)
    return line


def _render_drift_lines(drift: Optional[DriftReport]) -> list[str]:
    """Render the drift-since-watermark section as readable text lines."""
    if drift is None:
        return [
            "Drift since snapshot: not available "
            "(validation ran without a watermark)."
        ]
    return [
        "Drift since snapshot:",
        f"- Watermark GTID: {drift.watermark_gtid or 'unavailable'}",
        f"- Current source GTID: {drift.current_gtid or 'unavailable'}",
        f"- Drifted: {'yes' if drift.drifted else 'no'}",
        f"- {drift.detail}",
    ]


def render_text_report(report: ValidationReport) -> str:
    """Render a :class:`ValidationReport` as a human-readable text summary.

    Includes the overall match verdict, the as-of snapshot timestamp, the
    per-table comparison, any orphan findings, and the drift-since-watermark
    section (Requirements 6.4, 6.5).
    """
    lines = ["Validation Report", "=================", ""]
    lines.append(f"Mode: {report.mode.value}")
    lines.append(f"Overall: {'MATCH' if report.is_match else 'MISMATCH'}")
    as_of = (
        report.snapshot_timestamp.isoformat()
        if report.snapshot_timestamp is not None
        else "live source (no watermark)"
    )
    lines.append(f"As-of (snapshot): {as_of}")
    lines.append("")

    # Cut-over readiness: the three checks summarized up front.
    errored = [item for item in report.items if item.error is not None]
    reconciled = [item for item in report.items if item.reconcile is not None]
    inconsistent = [item for item in reconciled if not item.reconcile.consistent]  # type: ignore[union-attr]
    lines.append("Cut-over readiness:")
    lines.append(
        f"- Data identical: {'yes' if report.is_match else 'NO'} "
        f"({sum(1 for i in report.items if i.matched)}/{len(report.items)} tables matched)"
    )
    if reconciled:
        total_missing = sum(i.reconcile.missing_on_target for i in reconciled)  # type: ignore[union-attr]
        total_extra = sum(i.reconcile.extra_on_target for i in reconciled)  # type: ignore[union-attr]
        lines.append(
            f"- No missing or extra records: {'yes' if not inconsistent else 'NO'} "
            f"({total_missing} missing on target, {total_extra} extra on target)"
        )
    else:
        lines.append(
            "- No missing or extra records: not checked (reconciliation off)"
        )
    lines.append(
        f"- No table errors: {'yes' if not errored else 'NO'} "
        f"({len(errored)} table(s) errored)"
    )
    lines.append("")

    lines.append(f"Tables ({len(report.items)}):")
    if report.items:
        lines.extend(_render_table_line(item) for item in report.items)
    else:
        lines.append("- (no tables compared)")
    lines.append("")

    lines.append(
        f"Orphan check: {'performed' if report.orphan_check_performed else 'skipped'}"
    )
    if report.orphan_findings:
        lines.append(f"Orphan findings ({len(report.orphan_findings)}):")
        lines.extend(
            f"- {finding.table}.{finding.foreign_key} -> "
            f"{finding.referenced_table}: {finding.orphan_count} orphan(s)"
            for finding in report.orphan_findings
        )
    else:
        lines.append("Orphan findings: none")
    lines.append("")

    lines.extend(_render_drift_lines(report.drift))
    return "\n".join(lines)


def export_report(report: ValidationReport, fmt: str = "json") -> str:
    """Export a validation report as ``"json"`` or ``"text"``.

    JSON is produced from the Pydantic model (machine-readable, downloadable);
    text is a readable summary. Raises ``ValueError`` for unknown formats.
    """
    normalized = fmt.lower()
    if normalized == "json":
        return report.model_dump_json(indent=2)
    if normalized == "text":
        return render_text_report(report)
    raise ValueError(f"unsupported report format: {fmt!r} (use 'json' or 'text')")


__all__ = [
    "Validator",
    "ValidationCancelled",
    "CancelCheck",
    "SourceEngineFactory",
    "TargetConnectionFactory",
    "build_mysql_checksum_sql",
    "build_pg_checksum_sql",
    "build_mysql_pk_token_sql",
    "build_pg_pk_token_sql",
    "build_mysql_pk_first_page_sql",
    "build_mysql_pk_next_page_sql",
    "build_pg_pk_first_page_sql",
    "build_pg_pk_next_page_sql",
    "build_orphan_count_sql",
    "integer_pk_column",
    "reconcile_pk_streams",
    "render_text_report",
    "export_report",
]
