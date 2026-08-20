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
