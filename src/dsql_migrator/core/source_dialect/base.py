# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The :class:`SourceDialect` ABC -- the engine-agnostic source-reading contract.

Each concrete dialect lives in its own module (``mysql.py``, ``postgres.py``) so an
engine's specifics stay in one place; this base carries only the shared interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from dsql_migrator.core.models import SourceType


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

    @property
    @abstractmethod
    def snapshot_start_sql(self) -> str:
        """SQL that opens the read-only consistent-snapshot transaction for a stream."""

    @abstractmethod
    def value_converter(self, table: object, *, target_types: object = None) -> object:
        """Per-row value converter for reading ``table`` from this source.

        Turns a raw driver row into target-ready values (engine/driver-specific quirks
        -> canonical types); ``target_types`` optionally overrides the target type per
        column. MySQL returns the PyMySQL-aware :class:`~dsql_migrator.core.exporter.
        ValueConverter`.
        """


__all__ = ["SourceDialect"]
