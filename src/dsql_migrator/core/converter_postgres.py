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

from dsql_migrator.core.models import TableDef
from dsql_migrator.core.source_dialect import PostgresSourceDialect

_PG = PostgresSourceDialect()


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
        # format_type); emit it verbatim so sqlglot's postgres reader parses it.
        clause = f"{_PG.quote_identifier(column.name)} {column.mysql_type}"
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


__all__ = ["build_pg_source_ddl"]
