# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G158: MySQL clock defaults PostgreSQL rejected, or computed in the wrong zone.

* ``DEFAULT (CURTIME())`` on a DATETIME, TIMESTAMP or (without a zone) DATE
  column -- information_schema reports ``curtime()`` -- became ``DEFAULT CURRENT_TIME()``, a
  syntax error that failed the table's CREATE TABLE. MySQL puts the time on the session's
  current date there, so it is what that column's NOW() / CURDATE() gets.
* ``DEFAULT (CURRENT_TIMESTAMP(6))`` / ``(NOW(3))`` -- reported ``now(6)`` -- on a
  TIMESTAMP, DATE or TIME column was carried as ``NOW(6)``: PostgreSQL has no now(integer),
  and the CREATE TABLE failed. It is CURRENT_TIMESTAMP, as the transpile writes
  CURRENT_TIMESTAMP(6).
* Under a source zone, a TIME column's clock arithmetic (``now() + interval 1
  hour``) kept the session's time of day -- UTC on Aurora DSQL, nine hours off for Seoul --
  where the bare ``now()`` already named the zone.

Checked on PostgreSQL 17 against MySQL 8.0's stored values (commit message); doubles here.
"""

from __future__ import annotations

import re

import pytest
import sqlglot

from dsql_migrator.core.converter import SchemaConverter, dropped_default_notes, zoned_default_columns
from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.source_time_zone import SourceZone

SEOUL = SourceZone("named", "Asia/Seoul", "time_zone 'Asia/Seoul'")
PLUS_NINE = SourceZone("offset", "+09:00", "time_zone '+09:00'")


def _table(*columns: "tuple[str, str, str]") -> TableDef:
    return TableDef(
        name="app.d",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False)] + [
            ColumnDef(name=name, mysql_type=mysql_type, default=default, default_is_expression=True)
            for name, mysql_type, default in columns
        ],
        primary_key=["id"],
    )


def _ddl(table: TableDef, zone=None) -> str:  # noqa: ANN001
    ddl = SchemaConverter(source_zone=zone).convert_table(table).target_ddl
    assert isinstance(sqlglot.parse_one(ddl, read="postgres"), sqlglot.exp.Create)
    return " ".join(ddl.split())


def _default_of(table: TableDef, column: str, zone=None) -> str:  # noqa: ANN001
    return _ddl(table, zone).split(f'"{column}" ', 1)[1].split(", \"", 1)[0].split(", PRIMARY KEY", 1)[0]


@pytest.mark.parametrize("zone", [None, SEOUL, PLUS_NINE])
@pytest.mark.parametrize("mysql_type", ["datetime", "datetime(6)", "timestamp", "timestamp(6)", "date"])
def test_curtime_on_a_column_that_holds_a_date_is_what_its_now_or_curdate_gets(zone, mysql_type: str) -> None:  # noqa: ANN001
    table = _table(("c", mysql_type, "curtime()"), ("n", mysql_type, "now()"), ("k", mysql_type, "curdate()"))
    ddl = _ddl(table, zone)
    assert re.search(r"CURRENT_TIME(?!STAMP)", ddl) is None, ddl
    curtime = _default_of(table, "c", zone).split(" DEFAULT ", 1)[1]
    same_as = "k" if mysql_type == "date" else "n"
    assert curtime == _default_of(table, same_as, zone).split(" DEFAULT ", 1)[1]


@pytest.mark.parametrize(
    ("zone", "mysql_type", "expected"),
    [
        (None, "datetime", "TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'UTC' )"),
        (None, "timestamp", "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"),
        (None, "date", "DATE DEFAULT CURRENT_DATE"),
        (None, "time", "TIME DEFAULT LOCALTIME"),
        (SEOUL, "datetime", "TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'Asia/Seoul' )"),
        (SEOUL, "timestamp", "TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP"),
        (PLUS_NINE, "datetime", "TIMESTAMP DEFAULT ( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' )"),
    ],
)
def test_curtime_is_written_as_the_target_computes_it(zone, mysql_type: str, expected: str) -> None:  # noqa: ANN001
    assert _default_of(_table(("c", mysql_type, "curtime()")), "c", zone) == expected


def test_a_zone_lists_the_curtime_defaults_it_writes_and_never_a_timestamps() -> None:
    table = _table(("dt", "datetime", "curtime()"), ("ts", "timestamp", "curtime()"), ("d", "date", "curtime()"))
    assert zoned_default_columns(table, SEOUL, is_postgres=False) == ["dt", "d"]
    assert zoned_default_columns(table, None, is_postgres=False) == []


@pytest.mark.parametrize("zone", [None, SEOUL, PLUS_NINE])
@pytest.mark.parametrize(
    ("mysql_type", "default"),
    [("timestamp(6)", "now(6)"), ("timestamp(3)", "now(3)"), ("date", "now(3)"), ("time", "now(6)"),
     ("time(6)", "now(6)"), ("timestamp(6)", "CURRENT_TIMESTAMP(6)")],
)
def test_a_fractional_seconds_clock_is_never_now_of_a_number(zone, mysql_type: str, default: str) -> None:  # noqa: ANN001
    ddl = _ddl(_table(("c", mysql_type, default)), zone)
    assert "NOW(6)" not in ddl and "NOW(3)" not in ddl, ddl


@pytest.mark.parametrize(
    ("mysql_type", "expected"),
    [
        ("timestamp(6)", "TIMESTAMPTZ(6) DEFAULT CURRENT_TIMESTAMP"),
        ("timestamp(3)", "TIMESTAMPTZ(3) DEFAULT CURRENT_TIMESTAMP"),
        ("date", "DATE DEFAULT CURRENT_TIMESTAMP"),
        ("time", "TIME DEFAULT CURRENT_TIMESTAMP"),
        # Unchanged: a DATETIME's wall clock, MySQL's CURRENT_TIMESTAMP(6) as it was.
        ("datetime(6)", "TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'UTC' )"),
    ],
)
def test_a_fractional_seconds_clock_is_current_timestamp_without_a_zone(mysql_type: str, expected: str) -> None:
    assert _default_of(_table(("c", mysql_type, "now(6)" if "(3)" not in mysql_type else "now(3)")), "c") == expected
    assert _default_of(_table(("c", "timestamp(6)", "CURRENT_TIMESTAMP(6)")), "c") == (
        "TIMESTAMPTZ(6) DEFAULT CURRENT_TIMESTAMP"
    )


@pytest.mark.parametrize(
    ("zone", "expected"),
    [
        (SEOUL, "TIME DEFAULT ( ( NOW() AT TIME ZONE 'Asia/Seoul' ) + INTERVAL '1 HOUR' )"),
        (PLUS_NINE, "TIME DEFAULT ( ( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' ) + INTERVAL '1 HOUR' )"),
        # A source in UTC: the session's time of day, as before.
        (None, "TIME DEFAULT NOW() + INTERVAL '1 HOUR'"),
    ],
)
def test_a_time_columns_clock_arithmetic_takes_the_zones_time_of_day(zone, expected: str) -> None:  # noqa: ANN001
    table = _table(("k", "time", "(now() + interval 1 hour)"))
    assert _default_of(table, "k", zone) == expected
    assert zoned_default_columns(table, zone, is_postgres=False) == (["k"] if zone is not None else [])


def test_no_default_is_dropped_that_was_carried() -> None:
    """Only the text changes: every one of these was carried (and failed the table) before."""
    table = _table(
        ("a", "datetime", "curtime()"), ("b", "timestamp", "curtime()"), ("c", "date", "curtime()"),
        ("d", "timestamp(6)", "now(6)"), ("e", "date", "now(3)"), ("f", "time", "now(6)"),
        ("g", "time", "(now() + interval 1 hour)"),
    )
    assert dropped_default_notes(table, is_postgres=False) == []
