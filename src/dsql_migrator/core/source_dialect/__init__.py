# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-source-engine dialect adapter for the source-reading path.

A :class:`SourceDialect` factors out the pieces of reading a migration source that
differ by engine -- SQLAlchemy driver scheme, default port, ``create_engine`` kwargs,
identifier quoting, system schemas, snapshot SQL, enrichment, value conversion -- so
introspection, connect, export, and validation serve any supported source by looking up
:func:`dialect_for` on the config's ``source_type``.

Split by engine so an engine's specifics stay in one place and don't tangle:

- :mod:`.base` -- the engine-agnostic :class:`SourceDialect` ABC (the contract).
- :mod:`.mysql` -- :class:`MySQLSourceDialect` (the original, default engine).
- :mod:`.postgres` -- :class:`PostgresSourceDialect` (Full Load + Validation; CDC deferred).

Consumers import from this package (``dsql_migrator.core.source_dialect``) unchanged.

Import note: the concrete dialects import the introspector's connection helpers at
module top; consumers in ``introspector`` import :func:`dialect_for` lazily (inside the
engine factory) so this package can be built without an import cycle.
"""

from __future__ import annotations

from dsql_migrator.core.models import SourceType
from dsql_migrator.core.source_dialect.base import SourceDialect
from dsql_migrator.core.source_dialect.mysql import MySQLSourceDialect
from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

# Singleton dialect per source type. PostgreSQL Full Load value conversion is still a
# Phase-2 stub, but the dialect is registered so Evaluation / Schema Conversion resolve.
_DIALECTS: dict[SourceType, SourceDialect] = {
    SourceType.MYSQL: MySQLSourceDialect(),
    SourceType.POSTGRES: PostgresSourceDialect(),
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


__all__ = [
    "SourceDialect",
    "MySQLSourceDialect",
    "PostgresSourceDialect",
    "dialect_for",
]
