# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-source-engine dialect adapter for the source-reading path.

A :class:`SourceDialect` factors out the pieces of reading a migration source that
differ by engine -- the SQLAlchemy driver scheme, default port, ``create_engine``
kwargs, and the system schemas excluded from a user's inventory -- so introspection,
connect, export, and validation can serve any supported source by looking up
:func:`dialect_for` on the config's ``source_type``. MySQL is the original and
default engine (this module wraps the existing introspector helpers so its behavior
is byte-identical); PostgreSQL is added incrementally.

Import note: consumers in ``introspector`` import :func:`dialect_for` lazily (inside
the engine factory) so this module can import the introspector's MySQL connection
helpers at module top without an import cycle.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from dsql_migrator.core.introspector import (
    MYSQL_DRIVER,
    MYSQL_SYSTEM_SCHEMAS,
    source_engine_kwargs,
)
from dsql_migrator.core.models import SourceType

# MySQL integer base types (lower-cased, display width / UNSIGNED / ZEROFILL stripped
# before matching). Reader range sharding bands the LEADING PK column, which is only
# safe for a collation-free integer column; a non-integer leading column falls back to
# a single reader. Owned here so the exporter's sharding is source-dialect-driven.
_MYSQL_INTEGER_PK_TYPES = frozenset(
    {"tinyint", "smallint", "mediumint", "int", "integer", "bigint"}
)


class SourceDialect(ABC):
    """Source-engine-specific behavior for reading a migration source (read-only)."""

    #: The ``SourceType`` this dialect serves.
    source_type: SourceType

    @property
    @abstractmethod
    def driver_scheme(self) -> str:
        """SQLAlchemy URL scheme for this source engine (e.g. ``mysql+pymysql``)."""

    @property
    @abstractmethod
    def default_port(self) -> int:
        """Default TCP port for this source engine."""

    @property
    @abstractmethod
    def system_schemas(self) -> frozenset[str]:
        """Schemas never part of a user's migratable inventory (engine internals)."""

    @abstractmethod
    def engine_kwargs(
        self, *, read_timeout_seconds: Optional[int] = None
    ) -> dict[str, object]:
        """``create_engine`` kwargs shared by every engine for this source."""

    @abstractmethod
    def enrich(
        self, connection: object, enrich_db: str, tables: list
    ) -> tuple[list, list, list]:
        """Enrich reflected ``tables`` and collect (triggers, routines, events).

        Engine-specific catalog reads for one schema (column defaults, index method,
        partitioning) applied to ``tables`` in place, plus the schema's stored
        triggers/routines/events returned as three lists. A dialect with no
        engine-specific enrichment returns three empty lists. Structural reflection
        (tables/columns/views) is dialect-agnostic and done by the caller.
        """

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """Quote a bare identifier for this engine (e.g. MySQL backticks)."""

    @abstractmethod
    def quote_table(self, name: str) -> str:
        """Quote a possibly ``schema.table`` name, quoting each part separately.

        Cluster-wide introspection qualifies names as ``schema.table``; each part
        must be quoted independently so the engine reads it as schema + table, not
        one identifier containing a dot.
        """

    @property
    @abstractmethod
    def integer_pk_types(self) -> frozenset[str]:
        """Base type names whose LEADING PK column is range-shardable (integers)."""

    @abstractmethod
    def select_column_sql(self, column: object) -> str:
        """SELECT-list expression to read one source column (quoted, engine-specific).

        MySQL wraps a spatial column as ``ST_AsBinary(col) AS col`` (WKB bytes,
        matching what Debezium delivers) so it can migrate to ``bytea``; an ordinary
        column is just the quoted name.
        """


class MySQLSourceDialect(SourceDialect):
    """RDS/Aurora MySQL source dialect -- the original, default engine.

    Delegates to the existing introspector helpers so the MySQL path is byte-identical
    to before the adapter seam was introduced.
    """

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


# Singleton dialect per source type. PostgreSQL is registered in a later phase; until
# then dialect_for(POSTGRES) raises rather than silently falling back to MySQL.
_DIALECTS: dict[SourceType, SourceDialect] = {
    SourceType.MYSQL: MySQLSourceDialect(),
}


def dialect_for(source_type: SourceType) -> SourceDialect:
    """Return the source dialect for ``source_type`` (default = MySQL).

    Raises ``NotImplementedError`` for a source type that has no dialect registered
    yet, so a not-yet-supported engine fails loudly instead of silently reading as
    MySQL.
    """
    try:
        return _DIALECTS[source_type]
    except KeyError:
        raise NotImplementedError(
            f"No source dialect registered for {source_type!r}."
        ) from None


__all__ = ["SourceDialect", "MySQLSourceDialect", "dialect_for"]
