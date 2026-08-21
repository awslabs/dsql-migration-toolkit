# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source DDL reconstruction for the schema converter.

Kept in its own module -- not tangled into ``converter.py``'s MySQL-source logic -- per
the per-engine separation principle. The migration TARGET is Aurora DSQL (PostgreSQL-16
wire), so a PostgreSQL source is near-identity: this rebuilds a PostgreSQL ``CREATE
TABLE`` from the reflected :class:`TableDef` (whose column types are the EXACT PostgreSQL
strings captured by ``PostgresSourceDialect.enrich`` via ``format_type`` -- e.g.
``text[]``, ``numeric(12,2)``, ``timestamp with time zone``, ``uuid``, ``jsonb``). The
converter parses this with ``read="postgres"`` and re-enters the shared DSQL-constraint
phase (FK removal, primary-key strategy, ``CREATE INDEX ASYNC``) unchanged.

v1 emits columns (name + exact type + NOT NULL) and the primary key. Column DEFAULTs
(incl. ``serial``/identity ``nextval`` and generated columns) are a refinement -- the
primary-key strategy already governs identity on the target -- so they are not emitted
here yet.
"""

from __future__ import annotations

import re
from typing import Optional

from dsql_migrator.core.models import TableDef
from dsql_migrator.core.source_dialect import PostgresSourceDialect

_PG = PostgresSourceDialect()

# PostgreSQL base types Aurora DSQL supports AS COLUMN TYPES, normalized (lower-case,
# type modifiers + whitespace collapsed). Source of truth: the Aurora DSQL User Guide
# "Supported data types" page (numeric / character / date-time / miscellaneous tables).
# NOTE arrays are supported only at QUERY RUNTIME, not as column types (see below), so
# they are NOT in this set. dsql_lint confirms `array_type` is an unfixable error, and
# the docs list geometric/pgvector (and, by omission, network/xml/money/bit/enum/
# composite/range) as unsupported column types.
_DSQL_SUPPORTED_PG_BASE_TYPES = frozenset(
    {
        # Numeric
        "smallint", "int2",
        "integer", "int", "int4",
        "bigint", "int8",
        "real", "float4",
        "double precision", "float8",
        "numeric", "decimal", "dec",
        # Character
        "character", "char",
        "character varying", "varchar",
        "bpchar", "text",
        # Date / time
        "date",
        "time", "time without time zone",
        "time with time zone", "timetz",
        "timestamp", "timestamp without time zone",
        "timestamp with time zone", "timestamptz",
        "interval",
        # Miscellaneous
        "boolean", "bool",
        "bytea", "uuid", "json", "jsonb",
    }
)

# A length/precision/scale modifier anywhere in a format_type string:
# "(50)", "(12,2)", "(6)" -- e.g. numeric(12,2), character varying(50),
# timestamp(6) with time zone.
_TYPE_MODIFIER_RE = re.compile(r"\(\s*\d+\s*(?:,\s*\d+\s*)?\)")


def _ddl_column_type(pg_type: str) -> str:
    """The type string to emit in the rebuilt ``CREATE TABLE`` (usually verbatim).

    PostgreSQL -> DSQL is near-identity, so types are emitted as-is. The one exception is
    a fields-qualified interval carrying a precision (e.g. ``interval second(3)``,
    ``interval day to second(6)``): sqlglot's ``postgres`` reader -- which the converter
    uses to re-parse this DDL -- cannot parse the fields + precision combination, so it
    would abort the whole table. Drop the ``(N)`` fractional-seconds precision (keeping
    the fields qualifier); DSQL accepts the result and the data is preserved (source
    values are already rounded). Plain ``interval``/``interval(6)`` (no fields) parse
    fine and are left untouched.
    """
    lowered = pg_type.lower()
    if lowered.startswith("interval") and " " in lowered:
        return _TYPE_MODIFIER_RE.sub("", pg_type).rstrip()
    return pg_type


def normalize_pg_base_type(pg_type: str) -> str:
    """Reduce a ``format_type`` string to its base type for a support lookup.

    Strips length/precision modifiers wherever they appear and lower-cases/collapses
    whitespace, so ``NUMERIC(12,2)`` -> ``numeric``, ``character varying(50)`` ->
    ``character varying``, ``timestamp(6) with time zone`` -> ``timestamp with time zone``.
    """
    return " ".join(_TYPE_MODIFIER_RE.sub("", pg_type).lower().split())


# The FAITHFUL DSQL remodel target per unsupported PostgreSQL type family, appended to
# the warning so the operator knows WHAT to use instead -- not just "unsupported".
# Chosen for round-trip fidelity, NOT a blanket bytea: a type with a canonical text form
# -> text; a currency -> numeric; an array -> jsonb (queryable) or a child table. bytea
# is only natural for a genuinely binary type (e.g. spatial WKB), so it is not used as a
# general fallback here. The column type still changes, so the application must adapt --
# hence a warning to remodel deliberately rather than a silent auto-substitution.
_PG_UNSUPPORTED_REMODEL = {
    "inet": "text (its canonical address string round-trips losslessly)",
    "cidr": "text (its canonical address string round-trips losslessly)",
    "macaddr": "text",
    "macaddr8": "text",
    "xml": "text",
    "money": "numeric (preserves the exact amount; avoid locale-formatted text)",
    "bit": "text (the bit string, e.g. '10101010') or bytea",
    "bit varying": "text (the bit string) or bytea",
    "varbit": "text (the bit string) or bytea",
    "tsvector": "text",
    "tsquery": "text",
    "point": "text, or separate numeric columns for the coordinates",
    "line": "text",
    "lseg": "text",
    "box": "text",
    "path": "text",
    "polygon": "text",
    "circle": "text",
    "vector": "jsonb or text (pgvector is an extension Aurora DSQL does not provide)",
}

# Range / multirange types (int4range, tsrange, ... and the PG14+ multirange variants):
# their canonical text form round-trips, or split into lower/upper bound columns.
_PG_RANGE_TYPES = frozenset(
    {
        "int4range", "int8range", "numrange", "tsrange", "tstzrange", "daterange",
        "int4multirange", "int8multirange", "nummultirange", "tsmultirange",
        "tstzmultirange", "datemultirange",
    }
)


def unsupported_dsql_reason(pg_type: Optional[str]) -> Optional[str]:
    """Return why a PostgreSQL column type is unsupported on Aurora DSQL, else ``None``.

    Arrays (any ``...[]``) are unsupported as column types; otherwise a base type outside
    DSQL's documented supported set (geometric, network, xml, money, bit, enum/composite/
    range, pgvector, ...) is unsupported. Returns a human-readable reason that names the
    FAITHFUL remodel target for the type (e.g. array -> jsonb, inet -> text, money ->
    numeric) so the operator knows what to change it to -- or ``None`` when the type is
    DSQL-supported (int/bigint/numeric/text/varchar/uuid/json[b]/bytea/boolean/date-time/
    interval). PG->DSQL is otherwise near-identity, so supported types pass through
    verbatim. The column is NOT auto-substituted (the app must adapt to the new type);
    the user remodels deliberately -- Property 6 (no silent loss / no silent degradation).
    """
    if not pg_type:
        return None
    if "[]" in pg_type:
        return (
            f"Aurora DSQL does not support array column types ('{pg_type}'). Store the "
            "array as jsonb (queryable with the JSON operators) or in a child table keyed "
            "by this row's primary key, and adapt the application before migrating this "
            "column."
        )
    base = normalize_pg_base_type(pg_type)
    # interval[fields][(p)] is supported (dsql_lint: 0 errors). format_type spells the
    # fields inline ("interval day to second", "interval second(3)"), so a bare-token
    # allowlist can't capture them -- match on the leading token instead.
    if base == "interval" or base.startswith("interval "):
        return None
    if base in _DSQL_SUPPORTED_PG_BASE_TYPES:
        return None
    if base in _PG_RANGE_TYPES:
        target = (
            "text (its canonical form, e.g. '[1,5)'), or two columns for the lower and "
            "upper bounds"
        )
    else:
        target = _PG_UNSUPPORTED_REMODEL.get(base)
    if target is not None:
        return (
            f"Aurora DSQL does not support the PostgreSQL type '{pg_type}' as a column "
            f"type. Store it as {target}, and adapt the application before migrating this "
            "column."
        )
    # Fallback for user-defined types (enum / composite / domain) whose NAME format_type
    # returns verbatim, and any type not individually mapped.
    return (
        f"Aurora DSQL does not support the PostgreSQL type '{pg_type}' as a column type. "
        "Remodel the column to a DSQL-supported type (a PostgreSQL enum -> text; a "
        "composite type -> separate columns or jsonb; see the Aurora DSQL supported data "
        "types) before migrating."
    )


def build_pg_source_ddl(table: TableDef) -> str:
    """Build a PostgreSQL ``CREATE TABLE`` string for ``table`` (columns + PK).

    Identifiers are double-quoted via the PostgreSQL dialect (injection-safe, and a
    ``schema.table`` name renders as ``"schema"."table"``). Foreign keys and secondary
    indexes are intentionally not emitted (foreign keys are removed for DSQL and
    preserved as metadata by the caller; indexes are rendered separately as ``CREATE
    INDEX ASYNC``). Raises ``ValueError`` if the table has no columns.
    """
    if not table.columns:
        raise ValueError(f"table {table.name!r} has no columns to convert")

    column_clauses: list[str] = []
    for column in table.columns:
        # column.mysql_type holds the EXACT PostgreSQL type string (from enrich's
        # format_type); emit it (near-verbatim; _ddl_column_type only massages a
        # fields+precision interval sqlglot can't parse) so the postgres reader parses it.
        clause = f"{_PG.quote_identifier(column.name)} {_ddl_column_type(column.mysql_type)}"
        if not column.nullable:
            clause += " NOT NULL"
        column_clauses.append(clause)

    if table.primary_key:
        pk_columns = ", ".join(
            _PG.quote_identifier(name) for name in table.primary_key
        )
        column_clauses.append(f"PRIMARY KEY ({pk_columns})")

    body = ", ".join(column_clauses)
    return f"CREATE TABLE {_PG.quote_table(table.name)} ({body})"


__all__ = [
    "build_pg_source_ddl",
    "normalize_pg_base_type",
    "unsupported_dsql_reason",
]
