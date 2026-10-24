# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G191: mutation-check tests for e1cdc79..HEAD (G148-G190).

Each guard the change added was given a small semantic mutation (an inverted condition, a
dropped exception, a swapped sign, a wider pattern), and each mutant was run against the
G148-G190 tests and, where those passed, the whole suite. The tests here kill the mutants
the whole suite let through, on behaviour that was right and unpinned:

* MySQL's largest fixed offset, ``+14:00``, is a zone like any other (PostgreSQL's
  ``SET TIME ZONE 14`` too): the range check is inclusive.
* A DATETIME DEFAULT (CURDATE()) under a source zone is that zone's date, at midnight, not
  CURRENT_DATE -- the session's date, UTC on Aurora DSQL -- and SOURCE_TIME_ZONE lists it.
* Clock arithmetic in a composite unit with a plain number (``interval 2 hour_minute``) is
  dropped with its note like the quoted ``'1:30' hour_minute``: written, INTERVAL '2
  HOUR_MINUTE' fails the table's CREATE TABLE.
* A view over a view whose name sorts BEFORE the view it reads is still typed after it
  (G188's depth-first walk, not name order).
* ZERO_DATES grades a mode without strict mode that keeps NO_ZERO_DATE / NO_ZERO_IN_DATE,
  global or init_connect, beside a strict one: those flags only warn there.
* ``@@GLOBAL.sql_mode`` in init_connect is the GLOBAL value, also after a SET of the
  session's own mode.

The MySQL behaviour each pins was checked on a throwaway MySQL 8.0.46 container and the
DDL on PostgreSQL 17.11 (no published ports, removed afterwards; see the commit message).
Doubles only here: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

from typing import Optional

import pytest
import sqlglot

from dsql_migrator.core.assessor import ZERO_DATES_RULE_ID, CompatibilityAssessor
from dsql_migrator.core.converter import (
    SchemaConverter,
    dropped_default_notes,
    mysql_type_index,
    zoned_default_columns,
)
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef, ViewDef
from dsql_migrator.core.source_dialect.mysql import evaluate_sql_mode, init_connect_settings
from dsql_migrator.core.source_time_zone import SourceZone, init_connect_key, resolve_source_zone

_MY = SourceType.MYSQL
_PG = SourceType.POSTGRES
SEOUL = SourceZone("named", "Asia/Seoul", "time_zone 'Asia/Seoul'")
PLUS_NINE = SourceZone("offset", "+09:00", "time_zone '+09:00'")
# MySQL 8.0's default GLOBAL sql_mode (8.0.46 reports it in this order).
_STRICT = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)


def _flat(sql: str) -> str:
    return " ".join(sql.split())


def _table(*columns: ColumnDef) -> TableDef:
    return TableDef(
        name="app.d",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False), *columns],
        primary_key=["id"],
    )


def _clock(name: str, mysql_type: str, default: str) -> ColumnDef:
    """A clock default as information_schema reports it (DEFAULT_GENERATED)."""
    return ColumnDef(name=name, mysql_type=mysql_type, default=default, default_is_expression=True)


# ---------------------------------------------------------------------------
# 1. The zone: MySQL's largest fixed offset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("settings", "source_type", "kind", "zone"),
    [
        # MySQL 8.0.46 takes '-13:59' .. '+14:00' (Kiribati) and refuses '+14:01' (1298).
        ({"time_zone": "+14:00"}, _MY, "offset", "+14:00"),
        ({"time_zone": "-13:59"}, _MY, "offset", "-13:59"),
        ({"time_zone": "+14:01"}, _MY, "unknown", ""),
        # SET TIME ZONE 14 shows '<+14>-14'; ALTER DATABASE ... SET timezone = 14 stores '14'.
        ({"TimeZone": "<+14>-14"}, _PG, "offset", "+14:00"),
        ({"TimeZone": "14"}, _PG, "offset", "+14:00"),
        ({"TimeZone": "<+14:01>-14:01"}, _PG, "unknown", ""),
    ],
)
def test_the_largest_fixed_offset_is_a_zone(
    settings: dict, source_type: SourceType, kind: str, zone: str
) -> None:
    resolved = resolve_source_zone(settings, source_type)
    assert (resolved.kind, resolved.zone) == (kind, zone)


def test_a_source_at_the_largest_offset_gets_its_clock_defaults_written_in_it() -> None:
    table = _table(_clock("created", "datetime", "CURRENT_TIMESTAMP"))
    inventory = SourceInventory(tables=[table], server_settings={"time_zone": "+14:00"})
    ddl = _flat(SchemaConverter(source_type=_MY).convert(inventory).tables[0].target_ddl)
    assert (
        "\"created\" TIMESTAMP DEFAULT ( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+14:00' )"
    ) in ddl


# ---------------------------------------------------------------------------
# 2. MySQL clock defaults: CURDATE() on a DATETIME, a composite unit with a number
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("zone", "expected"),
    [
        (SEOUL, "TIMESTAMP DEFAULT ( CAST(( NOW() AT TIME ZONE 'Asia/Seoul' ) AS DATE) )"),
        (
            PLUS_NINE,
            "TIMESTAMP DEFAULT ( CAST(( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' ) AS "
            "DATE) )",
        ),
        (None, "TIMESTAMP DEFAULT CURRENT_DATE"),
    ],
)
def test_a_datetime_curdate_default_is_the_source_zones_date(
    zone: Optional[SourceZone], expected: str
) -> None:
    """MySQL stores the session's date at midnight there (2026-10-24 00:00:00 in a +09:00
    session, 8.0.46; information_schema reports curdate()). The zone's date, assigned to the
    timestamp column, is its midnight on PostgreSQL 17 in a UTC session too; CURRENT_DATE is
    the UTC date, a day off for part of every day."""
    table = _table(
        _clock("day_at", "datetime", "curdate()"), _clock("day_at6", "datetime(6)", "curdate()")
    )
    ddl = _flat(SchemaConverter(source_zone=zone).convert_table(table).target_ddl)
    assert f"\"day_at\" {expected}," in ddl, ddl
    assert f"\"day_at6\" {expected}," in ddl, ddl
    assert isinstance(sqlglot.parse_one(ddl, read="postgres"), sqlglot.exp.Create)
    listed = zoned_default_columns(table, zone, is_postgres=False)
    assert listed == ([] if zone is None else ["day_at", "day_at6"])


@pytest.mark.parametrize("zone", [None, SEOUL, PLUS_NINE])
@pytest.mark.parametrize("mysql_type", ["datetime", "date", "time", "timestamp"])
def test_a_composite_unit_with_a_plain_number_is_dropped_with_its_note(
    zone: Optional[SourceZone], mysql_type: str
) -> None:
    """DEFAULT (NOW() + INTERVAL 2 HOUR_MINUTE) -- MySQL adds two minutes -- is reported as
    (now() + interval 2 hour_minute) (8.0.46). Written, INTERVAL '2 HOUR_MINUTE' fails the
    CREATE TABLE ("invalid input syntax for type interval", PostgreSQL 17)."""
    table = _table(_clock("soon", mysql_type, "(now() + interval 2 hour_minute)"))
    conversion = SchemaConverter(source_zone=zone).convert_table(table)
    assert "HOUR_MINUTE" not in conversion.target_ddl.upper(), conversion.target_ddl
    dropped = [
        w.column_name for w in conversion.warnings if "no Aurora DSQL equivalent" in w.message
    ]
    assert dropped == ["soon"]
    noted = dropped_default_notes(table, is_postgres=False)
    assert [column.name for column, _note in noted] == ["soon"]
    assert isinstance(sqlglot.parse_one(conversion.target_ddl, read="postgres"), sqlglot.exp.Create)


# ---------------------------------------------------------------------------
# 3. A view over a view, named against the order it is typed in
# ---------------------------------------------------------------------------


_CLOCK = "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"
_DAY_AGO_INSTANT = f"( ( {_CLOCK} - INTERVAL '1 DAY' ) AT TIME ZONE 'Asia/Seoul' )"


def _view(name: str, body: str) -> ViewDef:
    """As SHOW CREATE VIEW stores it in single-database mode (own tables bare)."""
    return ViewDef(
        name=f"tzv.{name}",
        definition=(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW "
            f"`{name}` AS {body}"
        ),
    )


def test_a_view_is_typed_after_the_view_it_reads_whatever_their_names() -> None:
    """a_mid reads z_base, which sorts after it: in name order a_mid would be typed while
    z_base was still unknown, and top's comparison with the clock read untyped (the wall
    clock compared in the session's zone, as before G188)."""
    ev = TableDef(
        name="tzv.ev",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="ts", mysql_type="timestamp")],
        primary_key=["id"],
    )
    base = _view("z_base", "select `ev`.`id` AS `id`,`ev`.`ts` AS `ts` from `ev`")
    mid = _view("a_mid", "select `z_base`.`id` AS `id`,`z_base`.`ts` AS `ts` from `z_base`")
    top = _view(
        "top",
        "select `a_mid`.`id` AS `id` from `a_mid` where (`a_mid`.`ts` > (now() - interval 1 day))",
    )
    index = mysql_type_index([ev], [top, mid, base], SEOUL)
    assert dict(index["tzv.a_mid"][1]) == {"id": "int", "ts": "timestamp"}
    inventory = SourceInventory(
        tables=[ev], views=[top, mid, base], server_settings={"time_zone": "Asia/Seoul"}
    )
    ddl = {
        view.view: _flat(view.target_ddl)
        for view in SchemaConverter(source_type=_MY).convert(inventory).views
    }
    assert f'( "a_mid"."ts" > {_DAY_AGO_INSTANT} )' in ddl["tzv.top"], ddl["tzv.top"]


# ---------------------------------------------------------------------------
# 4. sql_mode: strict mode in one of the two modes, @@GLOBAL in init_connect
# ---------------------------------------------------------------------------


_DATED = TableDef(
    name="app.orders",
    columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
             ColumnDef(name="placed", mysql_type="date")],
    primary_key=["id"],
)


def _zero_dates_items(settings: dict) -> list:
    report = CompatibilityAssessor(source_type=_MY).assess(
        SourceInventory(tables=[_DATED], server_settings=settings)
    )
    return [item for item in report.items if item.rule_id == ZERO_DATES_RULE_ID]


@pytest.mark.parametrize(
    ("global_mode", "assigned"),
    [(_STRICT, "NO_ZERO_DATE,NO_ZERO_IN_DATE"), ("NO_ZERO_DATE,NO_ZERO_IN_DATE", _STRICT)],
)
def test_zero_dates_grades_a_mode_without_strict_mode_beside_a_strict_one(
    global_mode: str, assigned: str
) -> None:
    """Without strict mode NO_ZERO_DATE and NO_ZERO_IN_DATE only warn: a session of that mode
    stores '0000-00-00', and '2020-00-15' as '0000-00-00', with warning 1264 (8.0.46), so the
    other mode's strictness does not keep such dates out."""
    settings = {"sql_mode": global_mode, init_connect_key("sql_mode"): assigned}
    (item,) = _zero_dates_items(settings)
    assert (
        "(no strict mode (STRICT_TRANS_TABLES or STRICT_ALL_TABLES)): a zero date ('0000-00-00')"
    ) in item.risk
    assert "no NO_ZERO_DATE" not in item.risk and "no NO_ZERO_IN_DATE" not in item.risk
    # Both strict: nothing let in.
    assert _zero_dates_items({"sql_mode": _STRICT, init_connect_key("sql_mode"): _STRICT}) == []


def test_global_sql_mode_in_init_connect_is_the_global_value_after_a_session_set() -> None:
    """MySQL 8.0.46 gave a user without CONNECTION_ADMIN or SUPER the GLOBAL mode plus
    ALLOW_INVALID_DATES for this init_connect -- not the ANSI_QUOTES its first SET gave the
    session -- so ZERO_DATES and SQL_MODE grade that mode."""
    init_connect = (
        "SET sql_mode = 'ANSI_QUOTES'; "
        "SET sql_mode = CONCAT(@@GLOBAL.sql_mode, ',ALLOW_INVALID_DATES')"
    )
    recorded = init_connect_settings(init_connect, global_sql_mode=_STRICT)["sql_mode"]
    live = (
        "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,"
        "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
    )
    assert set(recorded.split(",")) == set(live.split(","))
    assert evaluate_sql_mode("CONCAT(@@GLOBAL.sql_mode, ',X')", "A", "G") == "G,X"
    assert evaluate_sql_mode("CONCAT(@@global.sql_mode, ',X')", "A", "G") == "G,X"
    assert evaluate_sql_mode("CONCAT(@@sql_mode, ',X')", "A", "G") == "A,X"
