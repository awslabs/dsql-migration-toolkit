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

from sqlalchemy import text

from dsql_migrator.core.introspector import SOURCE_CONNECT_TIMEOUT_SECONDS
from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect.base import (
    SourceDialect,
    SourceVersions,
    estimate_row_counts_query,
    probe_scalar,
)

# System schemas excluded when resolving a bare (unqualified) table name to its columns.
_PG_SYSTEM_SCHEMAS_SQL = "('pg_catalog', 'information_schema', 'pg_toast')"


def _reads_as_text(type_string: str) -> bool:
    """True for PG types Full Load must read via a text cast rather than natively.

    psycopg's native round trip is lossy or parse-heavy for these:
    - ``json`` / ``jsonb``: the default loader ``json.loads`` -> a Python dict/list, which
      the target dumper would ``json.dumps`` back (a ~10x round trip) and which collapses a
      JSON literal ``null`` to Python ``None`` (-> SQL NULL);
    - ``interval`` (incl. fields-qualified ``interval day to second``): psycopg loads it as
      a ``datetime.timedelta``, which CANNOT hold months/years -- it silently collapses
      ``1 mon`` -> 30 days / ``1 year`` -> 365 days, and raises under a non-default
      ``IntervalStyle``.
    Reading these as their exact source text (``CAST(col AS text)``) and binding that text
    to the identical target column as an unknown-typed literal (oid 0, which the server
    re-parses) is faithful for all of them -- the same path MySQL's JSON text uses.
    """
    base = type_string.split("(", 1)[0].strip().lower()
    return base in ("json", "jsonb") or base.startswith("interval")

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
        # Pin locale/format GUCs so the source renders text IDENTICALLY to the Aurora
        # DSQL target (whose defaults are exactly these: timezone/DateStyle=ISO,
        # IntervalStyle=postgres, lc_numeric=C). Validation reuses the target's PG
        # checksum renderer, whose numeric to_char 'D' mask honors lc_numeric and whose
        # date/interval ::text honor DateStyle/IntervalStyle -- so a source DB with a
        # non-default locale (e.g. lc_numeric=de_DE -> '3,14') would otherwise produce a
        # FALSE checksum MISMATCH on byte-identical data. Pinning also makes the Full Load
        # interval text cast (see select_column_sql) style-consistent. UTC also keeps
        # timestamp/timestamptz deterministic. psycopg passes these via libpq ``options``;
        # a read timeout bounds a stalled stream via ``statement_timeout`` (milliseconds).
        options = (
            "-c timezone=UTC -c datestyle=ISO -c intervalstyle=postgres -c lc_numeric=C"
        )
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
        # Capture EXACT PostgreSQL type strings via format_type(atttypid, atttypmod):
        # generic SQLAlchemy reflection loses array element types (text[] -> "ARRAY"),
        # timestamptz -> "TIMESTAMP", precision, etc. This overwrites each column's
        # reflected type string in place so the converter/assessor see the true PG type.
        # A non-PostgreSQL connection (e.g. the SQLite test double) no-ops (mirrors the
        # MySQL dialect's runtime guard). Stored trigger/function/event collection from
        # pg_catalog is a later refinement, so triggers/routines/events stay empty.
        dialect_name = getattr(getattr(connection, "dialect", None), "name", None)
        if dialect_name != "postgresql":
            return ([], [], [])

        for table in tables:
            schema, _, bare = table.name.rpartition(".")
            params: dict[str, object] = {"rel": bare}
            if schema:
                schema_filter = "AND n.nspname = :nsp"
                params["nsp"] = schema
            else:
                # Bare name (single-schema reflection): restrict to a user schema.
                schema_filter = f"AND n.nspname NOT IN {_PG_SYSTEM_SCHEMAS_SQL}"
            rows = connection.execute(  # type: ignore[attr-defined]
                text(
                    "SELECT a.attname AS col, "
                    "format_type(a.atttypid, a.atttypmod) AS typ "
                    "FROM pg_attribute a "
                    "JOIN pg_class c ON c.oid = a.attrelid "
                    "JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE c.relname = :rel AND a.attnum > 0 "
                    f"AND NOT a.attisdropped {schema_filter}"
                ),
                params,
            ).mappings()
            exact = {row["col"]: row["typ"] for row in rows}
            for column in table.columns:
                resolved = exact.get(column.name)
                if resolved:
                    column.mysql_type = resolved
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
        # Most columns read as-is (quoted). json/jsonb/interval are read via a text cast so
        # Full Load streams their EXACT text and binds it back to the identical target
        # column as an unknown-typed literal (oid 0), which the server re-parses --
        # faithful AND fast (see _reads_as_text for why the native psycopg round trip is
        # lossy/parse-heavy for these). json/jsonb can't be a PK and an interval PK still
        # paginates correctly (the text boundary is cast back to interval for `> :last`).
        # PostGIS geometry is out of scope (no ST_AsBinary-style case) for a first release.
        quoted = self.quote_identifier(column.name)  # type: ignore[attr-defined]
        if _reads_as_text(column.mysql_type):  # type: ignore[attr-defined]
            return f"CAST({quoted} AS text) AS {quoted}"
        return quoted

    @property
    def snapshot_start_sql(self) -> str:
        # PostgreSQL consistent read snapshot for the streaming read.
        return "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"

    def value_converter(self, table: object, *, target_types: object = None) -> object:
        # PG->DSQL is psycopg-native on both ends, so Full Load value conversion is pure
        # pass-through (json/jsonb/interval fidelity is handled by select_column_sql's text
        # cast on read, not per value). Kept in its own module (exporter_postgres) per the
        # per-engine separation rule.
        from dsql_migrator.core.exporter_postgres import PostgresValueConverter

        return PostgresValueConverter(table, target_types=target_types)

    def estimate_row_counts(
        self, connection: object, tables: list[str]
    ) -> "dict[str, Optional[int]]":
        # PostgreSQL: pg_class.reltuples is the planner's row estimate (maintained by
        # ANALYZE/autovacuum); join pg_namespace for the schema, and the default schema is
        # current_schema() (NOT current_database()). relkind IN ('r','p') covers ordinary
        # + partitioned tables. reltuples is -1 for a never-analyzed table in PG14+ (and
        # can be a stale float); map negative/NULL to None ("unknown", not a real 0).
        return estimate_row_counts_query(
            connection,
            tables,
            current_schema_sql="SELECT current_schema()",
            select_from="FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace",
            schema_column="n.nspname",
            table_column="c.relname",
            estimate_column="c.reltuples::bigint",
            extra_filter="c.relkind IN ('r', 'p')",
            parse_estimate=lambda value: (
                None if value is None or int(value) < 0 else int(value)
            ),
        )

    def probe_versions(self, connection: object) -> SourceVersions:
        # version() is the verbose banner ("PostgreSQL 16.4 ... on <arch>").
        # SHOW server_version is "<numeric>[ (<packaging>)]" -- "16.10 (Homebrew)",
        # "16.4 (Debian ...)", or a clean "16.4" on RDS/Aurora -- so keep only the
        # leading numeric token for a clean engine_version. aurora_version() gives the
        # Aurora PostgreSQL engine version (Aurora only; community/RDS lacks the
        # function, so it best-efforts to None).
        server_version = probe_scalar(connection, "SHOW server_version")
        return SourceVersions(
            server_version=probe_scalar(connection, "SELECT version()"),
            engine_version=server_version.split()[0] if server_version else None,
            aurora_version=probe_scalar(connection, "SELECT aurora_version()"),
        )


__all__ = ["PostgresSourceDialect"]
