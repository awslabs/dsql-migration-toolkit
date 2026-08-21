# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source value conversion for Full Load.

Kept in its own module -- not tangled into ``exporter.py``'s MySQL ``ValueConverter`` --
per the per-engine separation principle. A PostgreSQL source migrates to Aurora DSQL
(PostgreSQL-16 wire) via psycopg on BOTH ends, so value conversion is **pure
pass-through**: psycopg returns native Python types (``int``/``Decimal``/``float``/``str``/
``bool``/``bytes``/``uuid.UUID``/``date``/``time``/``datetime``/``timedelta``, and Python
``list`` for arrays) that bind straight back to the same PostgreSQL type on the DSQL
target with psycopg's default dumpers -- no conversion needed (unlike MySQL, which must
translate ``TINYINT(1)``->bool, ``BLOB``->bytea, ``DATETIME``->naive UTC, ``TIME``
timedelta->time, ``BIT`` bytes->int). The session is pinned to UTC
(``PostgresSourceDialect.engine_kwargs``) so ``timestamptz`` needs no normalization.

The types whose native psycopg round trip is NOT faithful -- ``json``/``jsonb`` (parsed to
a Python dict, losing a JSON literal ``null`` and paying a ``json.loads``/``json.dumps``
round trip) and ``interval`` (loaded as a ``datetime.timedelta``, which collapses
months/years) -- are handled UPSTREAM on the READ, not here:
``PostgresSourceDialect.select_column_sql`` reads them via ``CAST(col AS text)`` so they
stream as their exact source text (a ``str``), which binds to the identical target column
as an unknown-typed literal (oid 0, the same path MySQL's JSON text uses) and the server
re-parses it. So by the time a value reaches this converter it is already a bind-ready
Python value, and the converter simply passes rows through unchanged.
"""

from __future__ import annotations

from typing import Mapping, Optional

from dsql_migrator.core.models import TableDef


class PostgresValueConverter:
    """Pass-through value converter for a PostgreSQL source.

    Mirrors :class:`~dsql_migrator.core.exporter.ValueConverter`'s interface
    (``convert_row``/``convert_value``) so the exporter uses either interchangeably via
    ``dialect.value_converter``. PostgreSQL->DSQL is psycopg-native on both ends, so no
    per-value translation is needed; json/jsonb is handled at the source connection (see
    the module docstring), leaving nothing engine-specific for the converter to do.
    ``target_types`` is accepted for interface parity but unused (no PG type needs a
    per-value transform).
    """

    def __init__(
        self,
        table: TableDef,
        *,
        target_types: Optional[Mapping[str, str]] = None,
    ) -> None:
        self._table = table

    def convert_value(self, column_name: str, value: object) -> object:
        """Return ``value`` unchanged (psycopg values bind natively to the DSQL target)."""
        return value

    def convert_row(self, row: Mapping[str, object]) -> dict[str, object]:
        """Return a plain dict copy of ``row`` (values already bind-ready)."""
        return dict(row)


__all__ = ["PostgresValueConverter"]
