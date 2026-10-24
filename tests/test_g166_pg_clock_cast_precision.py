# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G166 (PG-4): a PostgreSQL clock default cast to a type with a precision.

``created_at timestamp DEFAULT now()::timestamp(0)`` -- the common way to drop fractional
seconds -- is printed by pg_get_expr as ``(now())::timestamp(0) without time zone``, and the
clock-cast pattern (G149) accepted only ``timestamp without time zone`` / ``time without time
zone`` with no typmod. So under a source zone that is not UTC such a default was carried as
written and, on Aurora DSQL's UTC session, gave the UTC wall clock -- 9 hours behind the
Seoul values Full Load and CDC copied -- and SOURCE_TIME_ZONE's column list left it out.

Now the precision is accepted and kept: ``((now() AT TIME ZONE 'Asia/Seoul')::timestamp(0))``,
``::time(0)``, and the date of the rounded clock. Live on PostgreSQL 17.11 (throwaway local
container): pg_get_expr prints the five defaults below as quoted here; the rewritten ones,
evaluated in a UTC session, equal what the Seoul session stores (to the minute), and a table
created with them stores the same values.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessor import check_postgres_time_zone
from dsql_migrator.core.converter import SchemaConverter, zoned_default_columns
from dsql_migrator.core.converter_postgres import pg_zoned_clock_default
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.source_time_zone import SourceZone

_SEOUL = SourceZone("named", "Asia/Seoul", "TimeZone 'Asia/Seoul'")
_PLUS_0530 = SourceZone("offset", "+05:30", "TimeZone '<+05:30>-05:30'")

# (column, type, pg_get_expr's default, written under Asia/Seoul)
_CASES = [
    ("t5", "timestamp without time zone", "(now())::timestamp(0) without time zone",
     "((now() AT TIME ZONE 'Asia/Seoul')::timestamp(0))"),
    ("t9", "timestamp(3) without time zone", "(CURRENT_TIMESTAMP)::timestamp(3) without time zone",
     "((now() AT TIME ZONE 'Asia/Seoul')::timestamp(3))"),
    ("t10", "timestamp without time zone", "(clock_timestamp())::timestamp(6) without time zone",
     "((clock_timestamp() AT TIME ZONE 'Asia/Seoul')::timestamp(6))"),
    ("tm6", "time without time zone", "(now())::time(0) without time zone",
     "((now() AT TIME ZONE 'Asia/Seoul')::time(0))"),
    ("d5", "date", "(now())::timestamp(0) without time zone",
     "(((now() AT TIME ZONE 'Asia/Seoul')::timestamp(0))::date)"),
]


@pytest.mark.parametrize(("name", "pg_type", "default", "written"), _CASES)
def test_a_cast_with_a_precision_is_written_in_the_source_zone(name, pg_type, default, written) -> None:  # noqa: ANN001
    assert pg_zoned_clock_default(default, pg_type, _SEOUL) == written


def test_a_fixed_offset_keeps_the_precision_too() -> None:
    assert pg_zoned_clock_default(
        "(now())::timestamp(0) without time zone", "timestamp without time zone", _PLUS_0530
    ) == "(((now() AT TIME ZONE 'UTC') + INTERVAL '+05:30')::timestamp(0))"


def test_what_was_written_before_is_unchanged() -> None:
    assert pg_zoned_clock_default(
        "(now())::timestamp without time zone", "timestamp without time zone", _SEOUL
    ) == "(now() AT TIME ZONE 'Asia/Seoul')"
    assert pg_zoned_clock_default("(now())::date", "date", _SEOUL) == (
        "((now() AT TIME ZONE 'Asia/Seoul')::date)"
    )
    # A time of day is stored in a time column only, and an instant column keeps its default.
    assert pg_zoned_clock_default(
        "(now())::time(0) without time zone", "timestamp without time zone", _SEOUL
    ) is None
    assert pg_zoned_clock_default(
        "(now())::timestamp(0) without time zone", "timestamp with time zone", _SEOUL
    ) is None


def _table() -> TableDef:
    return TableDef(
        name="rev.clocks",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)]
        + [ColumnDef(name=name, mysql_type=pg_type, default=default)
           for name, pg_type, default, _written in _CASES],
        primary_key=["id"],
    )


def test_the_conversion_and_evaluation_name_those_columns() -> None:
    table = _table()
    assert zoned_default_columns(table, _SEOUL, is_postgres=True) == ["t5", "t9", "t10", "tm6", "d5"]
    inventory = SourceInventory(tables=[table], server_settings={"TimeZone": "Asia/Seoul"})
    (converted,) = SchemaConverter(source_type=SourceType.POSTGRES).convert(inventory).tables
    ddl = " ".join(converted.target_ddl.split())
    for name, _pg_type, _default, written in _CASES:
        assert f'"{name}" ' in ddl and f"DEFAULT {written}" in ddl, name
    (item,) = check_postgres_time_zone(inventory)
    assert "5 columns: rev.clocks.t5, rev.clocks.t9, rev.clocks.t10, rev.clocks.tm6, rev.clocks.d5" in item.risk
