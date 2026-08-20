# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``PostgresSourceDialect`` -- RDS/Aurora PostgreSQL source-reading behavior.

The migration TARGET is Aurora DSQL (PostgreSQL-16 wire), so a PostgreSQL source is
near-identity: psycopg driver, double-quote identifiers, a REPEATABLE READ snapshot,
psycopg-native values. Scope is Full Load + Validation; CDC is deferred. Some methods
are staged: ``enrich`` is a v1 no-op (SQLAlchemy reflection already yields the
structure; PG-catalog enrichment is a later refinement) and ``value_converter`` is a
Phase-2 item that fails loudly rather than risk a silent mis-conversion.
"""

from __future__ import annotations

from typing import Optional

from dsql_migrator.core.introspector import SOURCE_CONNECT_TIMEOUT_SECONDS
from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect.base import SourceDialect

# PostgreSQL integer base types (lower-cased, precision stripped). Same sharding
# rationale as MySQL: only a collation-free integer leading PK column is range-shardable.
# Includes the internal aliases (int2/int4/int8) and the serial pseudo-types, which
# reflect as their underlying integer type but are listed for robustness.
_PG_INTEGER_PK_TYPES = frozenset(
    {
        "smallint",
        "integer",
        "int",
        "bigint",
        "int2",
        "int4",
        "int8",
        "smallserial",
        "serial",
        "bigserial",
    }
)


class PostgresSourceDialect(SourceDialect):
    """RDS/Aurora PostgreSQL source dialect (read-only)."""

    source_type = SourceType.POSTGRES

    @property
    def driver_scheme(self) -> str:
        # psycopg 3 (already a project dependency); the SQLAlchemy 2.x psycopg dialect.
        return "postgresql+psycopg"

    @property
    def default_port(self) -> int:
        return 5432

    @property
    def system_schemas(self) -> frozenset[str]:
        # Engine-internal schemas never part of a user's migratable inventory.
        return frozenset({"pg_catalog", "information_schema", "pg_toast"})

    def engine_kwargs(
        self, *, read_timeout_seconds: Optional[int] = None
    ) -> dict[str, object]:
        # Pin the session to UTC (mirrors MySQL's ``SET time_zone='+00:00'``) so
        # timestamp/timestamptz render deterministically vs the UTC target; psycopg
        # passes server settings via the libpq ``options`` connect arg. A read timeout
        # bounds a stalled stream via ``statement_timeout`` (milliseconds).
        options = "-c timezone=UTC"
        if read_timeout_seconds is not None:
            options += f" -c statement_timeout={int(read_timeout_seconds) * 1000}"
        connect_args: dict[str, object] = {
            "connect_timeout": SOURCE_CONNECT_TIMEOUT_SECONDS,
            "options": options,
        }
        return {"pool_pre_ping": True, "connect_args": connect_args}

    def enrich(
        self, connection: object, enrich_db: str, tables: list
    ) -> tuple[list, list, list]:
        # v1: no PostgreSQL-catalog enrichment. Structural reflection (columns/types/PK/
        # indexes/FK) is dialect-agnostic and already done by the caller via SQLAlchemy;
        # PG-specific enrichment (identity/generated columns, index method) and stored
        # trigger/function/event collection from pg_catalog are a later refinement.
        return ([], [], [])

    def quote_identifier(self, name: str) -> str:
        # PostgreSQL: double quotes, embedded double-quotes doubled.
        escaped = name.replace('"', '""')
        return f'"{escaped}"'

    def quote_table(self, name: str) -> str:
        # Split on the first dot so ``schema.table`` becomes "schema"."table"; each part
        # is quoted independently (a lone quoted "schema.table" would be one identifier).
        schema, separator, obj = name.partition(".")
        if separator and schema and obj:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(obj)}"
        return self.quote_identifier(name)

    @property
    def integer_pk_types(self) -> frozenset[str]:
        return _PG_INTEGER_PK_TYPES

    def select_column_sql(self, column: object) -> str:
        # v1: read every column as-is (quoted). PostGIS geometry is out of scope for a
        # first PostgreSQL-source release, so there is no ST_AsBinary-style special case.
        return self.quote_identifier(column.name)  # type: ignore[attr-defined]

    @property
    def snapshot_start_sql(self) -> str:
        # PostgreSQL consistent read snapshot for the streaming read.
        return "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"

    def value_converter(self, table: object, *, target_types: object = None) -> object:
        # Full Load value conversion for a PostgreSQL source is Phase 2 (psycopg returns
        # near-native types, but bit/bytea/json/array handling must be verified against a
        # live source before shipping). Fail loudly rather than risk silent corruption.
        raise NotImplementedError(
            "PostgreSQL-source Full Load value conversion is not implemented yet "
            "(Phase 2); PostgreSQL support currently covers Evaluation + Schema "
            "Conversion."
        )


__all__ = ["PostgresSourceDialect"]
