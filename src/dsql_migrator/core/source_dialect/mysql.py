# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""``MySQLSourceDialect`` -- RDS/Aurora MySQL source-reading behavior.

The original, default engine. Delegates to the existing introspector/exporter/watermark
helpers (lazy-imported inside methods where needed to avoid an import cycle) so the
MySQL path is byte-identical to before the dialect seam was introduced.
"""

from __future__ import annotations

from typing import Optional

from dsql_migrator.core.introspector import (
    MYSQL_DRIVER,
    MYSQL_SYSTEM_SCHEMAS,
    source_engine_kwargs,
)
from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect.base import (
    SourceDialect,
    SourceVersions,
    estimate_row_counts_query,
    probe_scalar,
)

# MySQL integer base types (lower-cased, display width / UNSIGNED / ZEROFILL stripped
# before matching). Reader range sharding bands the LEADING PK column, which is only
# safe for a collation-free integer column; a non-integer leading column falls back to
# a single reader.
_MYSQL_INTEGER_PK_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
)


class MySQLSourceDialect(SourceDialect):
    """RDS/Aurora MySQL source dialect -- the original, default engine."""

    source_type = SourceType.MYSQL

    @property
    def driver_scheme(self) -> str:
        return MYSQL_DRIVER

    @property
    def default_port(self) -> int:
        return 3306

    @property
    def system_schemas(self) -> frozenset[str]:
        return MYSQL_SYSTEM_SCHEMAS

    def engine_kwargs(
        self, *, read_timeout_seconds: Optional[int] = None
    ) -> dict[str, object]:
        return source_engine_kwargs(read_timeout_seconds=read_timeout_seconds)

    def enrich(
        self, connection: object, enrich_db: str, tables: list
    ) -> tuple[list, list, list]:
        # MySQL enrichment reads information_schema; run it only against a genuine
        # MySQL connection. A non-MySQL engine (e.g. the SQLite double used in tests)
        # safely no-ops, preserving the prior runtime gate
        # (``connection.dialect.name == "mysql"``).
        dialect_name = getattr(getattr(connection, "dialect", None), "name", None)
        if dialect_name != "mysql":
            return ([], [], [])
        from dsql_migrator.core.introspector import (
            collect_events,
            collect_routines,
            collect_triggers,
            enrich_columns,
            enrich_index_types,
            enrich_partitions,
        )

        enrich_columns(connection, enrich_db, tables)
        enrich_index_types(connection, enrich_db, tables)
        enrich_partitions(connection, enrich_db, tables)
        return (
            collect_triggers(connection, enrich_db),
            collect_routines(connection, enrich_db),
            collect_events(connection, enrich_db),
        )

    def quote_identifier(self, name: str) -> str:
        # MySQL: backticks, embedded backticks doubled.
        escaped = name.replace("`", "``")
        return f"`{escaped}`"

    def quote_table(self, name: str) -> str:
        # Split on the first dot so ``schema.table`` becomes `schema`.`table` rather
        # than one `schema.table` identifier (which MySQL reads as one table in the
        # unset current database -> "1046, No database selected").
        schema, separator, obj = name.partition(".")
        if separator and schema and obj:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(obj)}"
        return self.quote_identifier(name)

    @property
    def integer_pk_types(self) -> frozenset[str]:
        return _MYSQL_INTEGER_PK_TYPES

    def select_column_sql(self, column: object) -> str:
        # Spatial columns have no DSQL type -> read as ST_AsBinary (WKB bytes, matching
        # what Debezium delivers for CDC) aliased back to the name; others read as-is.
        from dsql_migrator.core.converter import is_spatial_mysql_type

        quoted = self.quote_identifier(column.name)  # type: ignore[attr-defined]
        if is_spatial_mysql_type(column.mysql_type):  # type: ignore[attr-defined]
            return f"ST_AsBinary({quoted}) AS {quoted}"
        return quoted

    @property
    def snapshot_start_sql(self) -> str:
        # MySQL: InnoDB REPEATABLE READ consistent snapshot (same as watermark capture).
        from dsql_migrator.core.watermark import START_CONSISTENT_SNAPSHOT

        return START_CONSISTENT_SNAPSHOT

    def value_converter(self, table: object, *, target_types: object = None) -> object:
        from dsql_migrator.core.exporter import ValueConverter

        return ValueConverter(table, target_types=target_types)

    def estimate_row_counts(
        self, connection: object, tables: list[str]
    ) -> "dict[str, Optional[int]]":
        # MySQL: information_schema.tables.table_rows is the storage-engine row estimate;
        # the default schema is the current DATABASE(). (Byte-identical to the query the
        # watermark module issued inline before the dialect seam.)
        return estimate_row_counts_query(
            connection,
            tables,
            current_schema_sql="SELECT DATABASE()",
            select_from="FROM information_schema.tables",
            schema_column="table_schema",
            table_column="table_name",
            estimate_column="table_rows",
        )

    def probe_versions(self, connection: object) -> SourceVersions:
        # VERSION() carries the raw wire version (with the Aurora tag before newer
        # builds dropped it); @@innodb_version exposes the full community patch (e.g.
        # 8.0.42); @@aurora_version gives the Aurora MySQL engine version (Aurora only).
        # Each is best effort -- a non-Aurora/RDS source simply has no @@aurora_version.
        return SourceVersions(
            server_version=probe_scalar(connection, "SELECT VERSION()"),
            engine_version=probe_scalar(connection, "SELECT @@innodb_version"),
            aurora_version=probe_scalar(connection, "SELECT @@aurora_version"),
        )


__all__ = ["MySQLSourceDialect"]
