# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G181: MySQL clock defaults G158 left out.

* ``DEFAULT (CURRENT_TIMESTAMP(6) + INTERVAL 1 DAY)`` -- information_schema reports
  ``(now(6) + interval 1 day)`` -- on a TIMESTAMP column (and, with no zone, on a DATE or
  TIME one) was carried as ``NOW(6) + INTERVAL '1 DAY'``: PostgreSQL has no now(integer), and
  the table's CREATE TABLE failed. G158 fixed the bare now(n) only.
* A clock default on a CHAR / VARCHAR / TEXT column: ``DEFAULT (CURTIME())`` became
  ``CURRENT_TIME()``, a syntax error (the CREATE TABLE failed), and NOW() / NOW(n) /
  CURDATE() became the clock itself, whose PostgreSQL text ('2026-10-24 04:43:37.171852+00',
  UTC) is neither MySQL's ('2026-10-24 13:43:37', in the session's zone) nor short enough for
  a VARCHAR(20). It is now TO_CHAR of the source zone's clock in MySQL's format.

MySQL 8.0.46 (throwaway container, session time_zone 'Asia/Seoul') stored, for an insert that
omitted them: '2026-10-24 14:55:00', '2026-10-24 14:55:00.941' (now(3)), '2026-10-24',
'14:55:00', UTC_TIMESTAMP()'s '2026-10-24 05:55:00', and '2026-10-25 14:55:00' for now() +
interval 1 day. PostgreSQL 17.11 in a UTC session (a stand-in for Aurora DSQL) created the
converted table and stored the same texts. Doubles here.
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
        name="app.clk",
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
    """The column's clause, with the pretty-printer's spaces inside parentheses removed."""
    clause = _ddl(table, zone).split(f'"{column}" ', 1)[1]
    clause = re.split(r", (?:\"|PRIMARY KEY)", clause, maxsplit=1)[0]
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", clause))


_NO_NOW_DIGIT = re.compile(r"NOW\(\s*\d", re.IGNORECASE)


@pytest.mark.parametrize("zone", [None, SEOUL, PLUS_NINE])
@pytest.mark.parametrize("mysql_type", ["timestamp(6)", "timestamp(3)"])
def test_a_fractional_clock_with_arithmetic_on_a_timestamp_is_current_timestamp(
    zone, mysql_type: str  # noqa: ANN001
) -> None:
    table = _table(("e", mysql_type, f"(now({mysql_type[-2]}) + interval 1 day)"))
    default = _default_of(table, "e", zone)
    assert _NO_NOW_DIGIT.search(default) is None, default
    assert default.endswith("DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 DAY'"), default


@pytest.mark.parametrize(
    ("mysql_type", "expression", "expected"),
    [
        ("date", "(now(3) + interval 1 day)", "DATE DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 DAY'"),
        ("time(3)", "(now(3) + interval 1 hour)",
         "TIME(3) DEFAULT CURRENT_TIMESTAMP + INTERVAL '1 HOUR'"),
    ],
)
def test_without_a_zone_a_date_or_time_column_gets_current_timestamp_too(
    mysql_type: str, expression: str, expected: str
) -> None:
    assert _default_of(_table(("e", mysql_type, expression)), "e") == expected


def test_the_zoned_date_and_time_arithmetic_is_unchanged() -> None:
    table = _table(("d", "date", "(now(3) + interval 1 day)"),
                   ("t", "time(3)", "(now(3) + interval 1 hour)"))
    assert _default_of(table, "d", SEOUL) == (
        "DATE DEFAULT ((NOW() AT TIME ZONE 'Asia/Seoul') + INTERVAL '1 DAY')"
    )
    assert _default_of(table, "t", SEOUL) == (
        "TIME(3) DEFAULT ((NOW() AT TIME ZONE 'Asia/Seoul') + INTERVAL '1 HOUR')"
    )


_UTC = "(NOW() AT TIME ZONE 'UTC')"
_SEOUL = "(NOW() AT TIME ZONE 'Asia/Seoul')"
_PLUS_NINE = "((NOW() AT TIME ZONE 'UTC') + INTERVAL '+09:00')"


@pytest.mark.parametrize(("zone", "clock"), [(None, _UTC), (SEOUL, _SEOUL), (PLUS_NINE, _PLUS_NINE)])
@pytest.mark.parametrize(
    ("mysql_type", "expression", "text_format"),
    [
        ("varchar(20)", "curtime()", "HH24:MI:SS"),
        ("char(8)", "curtime()", "HH24:MI:SS"),
        ("text", "curtime()", "HH24:MI:SS"),
        ("varchar(30)", "now()", "YYYY-MM-DD HH24:MI:SS"),
        ("varchar(30)", "current_timestamp()", "YYYY-MM-DD HH24:MI:SS"),
        ("varchar(30)", "now(3)", "YYYY-MM-DD HH24:MI:SS.FF3"),
        ("text", "now(6)", "YYYY-MM-DD HH24:MI:SS.FF6"),
        ("varchar(30)", "curdate()", "YYYY-MM-DD"),
    ],
)
def test_a_clock_default_on_a_textual_column_stores_mysqls_text(
    zone, clock: str, mysql_type: str, expression: str, text_format: str  # noqa: ANN001
) -> None:
    table = _table(("c", mysql_type, expression))
    default = _default_of(table, "c", zone).split(" DEFAULT ", 1)[1]
    assert default == f"TO_CHAR({clock}, '{text_format}')"
    assert re.search(r"CURRENT_TIME(?!STAMP)", _ddl(table, zone)) is None
    assert dropped_default_notes(table, is_postgres=False) == []


@pytest.mark.parametrize("zone", [None, SEOUL])
def test_utc_clocks_and_clock_arithmetic_on_a_textual_column(zone) -> None:  # noqa: ANN001
    table = _table(("u", "varchar(30)", "utc_timestamp()"), ("k", "varchar(30)", "utc_date()"),
                   ("g", "varchar(30)", "(now(3) + interval 1 day)"))
    assert _default_of(table, "u", zone) == (
        f"VARCHAR(30) DEFAULT TO_CHAR({_UTC}, 'YYYY-MM-DD HH24:MI:SS')"
    )
    assert _default_of(table, "k", zone) == f"VARCHAR(30) DEFAULT TO_CHAR({_UTC}, 'YYYY-MM-DD')"
    clock = _SEOUL if zone is not None else _UTC
    assert _default_of(table, "g", zone) == (
        f"VARCHAR(30) DEFAULT TO_CHAR(({clock} + INTERVAL '1 DAY'), 'YYYY-MM-DD HH24:MI:SS.FF3')"
    )


def test_a_textual_clock_default_is_named_among_the_zoned_columns() -> None:
    table = _table(("c", "varchar(30)", "now()"), ("u", "varchar(30)", "utc_timestamp()"))
    assert zoned_default_columns(table, SEOUL, is_postgres=False) == ["c"]


def test_a_fractional_clock_on_a_column_of_another_type_is_dropped_with_its_note() -> None:
    table = _table(("n", "int", "now(3)"))
    assert _default_of(table, "n") == "INT"
    ((column, note),) = dropped_default_notes(table, is_postgres=False)
    assert column.name == "n" and "has no Aurora DSQL equivalent" in note
