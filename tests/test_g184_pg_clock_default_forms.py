# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G184: PostgreSQL clock defaults G166 left out of the source's time zone.

Under a source zone that is not UTC, Schema Conversion writes that zone into a clock default
on a column without a time zone, but only into a bare clock and one cast to a type WITHOUT a
zone. The common date_trunc('second', now()) (pg_get_expr: date_trunc('second'::text,
now())), date_trunc('day', now()) on a date column, date_trunc(..., LOCALTIMESTAMP) and
now()::timestamptz(0) (pg_get_expr: (now())::timestamp(0) with time zone) were carried as
written and computed in Aurora DSQL's UTC session -- 9 hours behind the Seoul values Full Load
and CDC copied, another date between 00:00 and 09:00 --, while SOURCE_TIME_ZONE said the zone
was written into those columns' clock defaults and listed only the others.

Now those forms are written in the zone, and every other clock default such a column carries
as written (clock arithmetic, the three-argument date_trunc) is named in SOURCE_TIME_ZONE.
Live on PostgreSQL 17.11 (throwaway container): the source defaults inserted in a Seoul
session and the converted ones in a UTC session, in one transaction, stored the same values.
"""

from __future__ import annotations

import pytest
import sqlglot

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter, unzoned_clock_default_columns
from dsql_migrator.core.converter_postgres import pg_unzoned_clock_default, pg_zoned_clock_default
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.source_time_zone import SourceZone

_PG = SourceType.POSTGRES
SEOUL = SourceZone("named", "Asia/Seoul", "TimeZone 'Asia/Seoul'")
UTC = SourceZone("utc", "UTC", "TimeZone 'UTC'")
_TS, _DATE, _TIME = "timestamp without time zone", "date", "time without time zone"
_CLOCK = "(now() AT TIME ZONE 'Asia/Seoul')"


@pytest.mark.parametrize(
    ("default", "pg_type", "expected"),
    [
        ("date_trunc('second'::text, now())", _TS, f"date_trunc('second'::text, {_CLOCK})"),
        ("date_trunc('minute'::text, LOCALTIMESTAMP)", _TS, f"date_trunc('minute'::text, {_CLOCK})"),
        ("date_trunc('second'::text, CURRENT_TIMESTAMP(3))", _TS,
         "date_trunc('second'::text, (CURRENT_TIMESTAMP(3) AT TIME ZONE 'Asia/Seoul'))"),
        ("date_trunc('day'::text, now())", _DATE, f"(date_trunc('day'::text, {_CLOCK})::date)"),
        ("date_trunc('second'::text, now())", _TIME, f"(date_trunc('second'::text, {_CLOCK})::time)"),
        ("(now())::timestamp(0) with time zone", _TS, f"({_CLOCK}::timestamp(0))"),
        ("(now())::timestamp(3) with time zone", _DATE, f"(({_CLOCK}::timestamp(3))::date)"),
        ("(now())::time(0) with time zone", _TIME, f"({_CLOCK}::time(0))"),
    ],
)
def test_date_trunc_and_a_zoned_cast_of_the_clock_are_written_in_the_zone(
    default: str, pg_type: str, expected: str
) -> None:
    assert pg_zoned_clock_default(default, pg_type, SEOUL) == expected
    assert not pg_unzoned_clock_default(default, pg_type, SEOUL)
    sqlglot.parse_one(f"SELECT {expected}", read="postgres")


@pytest.mark.parametrize(
    ("default", "pg_type"),
    [
        ("date_trunc('day'::text, now(), 'Asia/Seoul'::text)", _TS),  # a timestamptz, stored
        ("date_trunc('month'::text, CURRENT_DATE)", _DATE),
        ("(now() + '1 day'::interval)", _TS),
        ("(CURRENT_DATE + 1)", _DATE),
    ],
)
def test_other_clock_defaults_are_carried_as_written_and_told(default: str, pg_type: str) -> None:
    assert pg_zoned_clock_default(default, pg_type, SEOUL) is None
    assert pg_unzoned_clock_default(default, pg_type, SEOUL)
    assert not pg_unzoned_clock_default(default, pg_type, UTC)


@pytest.mark.parametrize(
    ("default", "pg_type"),
    [
        ("date_trunc('second'::text, now())", "timestamp with time zone"),  # an instant
        ("(now() + '1 day'::interval)", "timestamp with time zone"),
        ("(now() AT TIME ZONE 'UTC'::text)", _TS),  # names its zone
        ("timezone('UTC'::text, now())", _TS),
        ("'2024-01-01 00:00:00'::timestamp without time zone", _TS),
        ("0", "integer"),
    ],
)
def test_what_does_not_depend_on_the_sessions_zone_is_neither(default: str, pg_type: str) -> None:
    assert pg_zoned_clock_default(default, pg_type, SEOUL) is None
    assert not pg_unzoned_clock_default(default, pg_type, SEOUL)


def _clocks() -> TableDef:
    return TableDef(
        name="rev.clk",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)] + [
            ColumnDef(name=name, mysql_type=pg_type, default=default)
            for name, pg_type, default in (
                ("t2", _TS, "date_trunc('second'::text, now())"),
                ("t3", _TS, "(now())::timestamp(0) with time zone"),
                ("d1", _DATE, "date_trunc('day'::text, now())"),
                ("t7", _TS, "(now() + '1 day'::interval)"),
                ("d2", _DATE, "(CURRENT_DATE + 1)"),
                ("t8", _TS, "(now() AT TIME ZONE 'UTC'::text)"),
            )
        ],
        primary_key=["id"],
    )


def test_the_conversion_writes_them_and_source_time_zone_names_the_rest() -> None:
    inventory = SourceInventory(tables=[_clocks()], server_settings={"TimeZone": "Asia/Seoul"})
    (table,) = SchemaConverter(source_type=_PG).convert(inventory).tables
    ddl = " ".join(table.target_ddl.split())
    assert f'"t2" TIMESTAMP DEFAULT date_trunc(\'second\'::text, {_CLOCK})' in ddl
    assert f'"d1" DATE DEFAULT (date_trunc(\'day\'::text, {_CLOCK})::date)' in ddl
    assert '"t7" TIMESTAMP DEFAULT (now() + \'1 day\'::interval)' in ddl
    assert unzoned_clock_default_columns(_clocks(), SEOUL) == ["t7", "d2"]
    report = CompatibilityAssessor(source_type=_PG).assess(inventory)
    (item,) = [i for i in report.items if i.rule_id == "SOURCE_TIME_ZONE"]
    assert "(3 columns: rev.clk.t2, rev.clk.t3, rev.clk.d1)" in item.risk
    assert "are carried as written, so they compute in the session's zone" in item.risk
    assert "(2 columns: rev.clk.t7, rev.clk.d2)" in item.risk
    assert "Edit the clock defaults carried as written in Schema Conversion" in item.recommendation


def test_only_carried_clock_defaults_are_not_called_none() -> None:
    table = TableDef(
        name="rev.only",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="t7", mysql_type=_TS, default="(now() + '1 day'::interval)")],
        primary_key=["id"],
    )
    report = CompatibilityAssessor(source_type=_PG).assess(
        SourceInventory(tables=[table], server_settings={"TimeZone": "Asia/Seoul"})
    )
    (item,) = [i for i in report.items if i.rule_id == "SOURCE_TIME_ZONE"]
    assert "no column default reads the clock" not in item.risk
    assert "none is one it rewrites" in item.risk and "(1 column: rev.only.t7)" in item.risk
