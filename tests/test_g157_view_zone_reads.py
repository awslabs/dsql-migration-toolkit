# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G157: a MySQL view under the source's time zone reads every session-zone value in it.

G149 wrote a MySQL view's NOW() / CURDATE() as the source zone's wall clock -- a PostgreSQL
``timestamp`` -- and left the rest as it was. Where that wall clock met a MySQL TIMESTAMP (a
``timestamptz`` on Aurora DSQL), PostgreSQL read it as an instant in the SESSION's zone (UTC
on Aurora DSQL unless the session sets another), nine hours off for Seoul: ``ts > NOW() -
INTERVAL 1 DAY`` lost the rows of the first nine hours, ``ts >= CURDATE()`` lost today's,
and a COALESCE / GREATEST / CASE of both landed in the future (a regression: before G149
both sides were the session's clock). FROM_UNIXTIME() and a TIMESTAMP's date, time and text
(DATE(), HOUR(), DATE_FORMAT() ...) were still read in the session's zone, beside a clock
that no longer was (DATE(ts) = CURDATE() compared a UTC date with Seoul's).

* A TIMESTAMP read as a wall clock is read at the zone; FROM_UNIXTIME() is the zone's wall
  clock of the instant.
* The zone's wall clock where it meets a TIMESTAMP (compared, BETWEEN, IN, a COALESCE /
  IFNULL / GREATEST / LEAST / NULLIF / IF / CASE of both) is the instant it names there --
  the column side untouched, so an index on it still serves -- and in a UNION column the
  TIMESTAMP is read at the zone, MySQL's DATETIME.
* A view calling CONVERT_TZ() lost its source zone (a loop reused the name), and failed to
  convert when it also read the clock -- for a UTC source too.

Live-checked on PostgreSQL 17 against MySQL 8.0's answers (commit message); doubles only here.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import sqlglot

from dsql_migrator.core.assessor import SOURCE_TIME_ZONE_RULE_ID, CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef
from dsql_migrator.core.source_time_zone import SourceZone

SEOUL = SourceZone("named", "Asia/Seoul", "time_zone 'Asia/Seoul'")
PLUS_NINE = SourceZone("offset", "+09:00", "time_zone '+09:00'")
UTC = SourceZone("utc", "UTC", "time_zone 'UTC'")
UNKNOWN = SourceZone("unknown", "", "time_zone 'SYSTEM', system_time_zone 'KST'")

# The zone's wall clock as G149 writes it, and the instant it names there (G157).
_CLOCK = "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"
_INSTANT = f"( ( {_CLOCK} ) AT TIME ZONE 'Asia/Seoul' )"
_DAY_AGO = f"( ( {_CLOCK} - INTERVAL '1 DAY' ) AT TIME ZONE 'Asia/Seoul' )"
_TS_AT_SEOUL = "( \"e\".\"ts\" AT TIME ZONE 'Asia/Seoul' )"
_TS_AT_PLUS_NINE = "( ( \"e\".\"ts\" AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' )"


def _table() -> TableDef:
    return TableDef(
        name="app.ev",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="ts", mysql_type="timestamp"),
            ColumnDef(name="dt", mysql_type="datetime"),
            ColumnDef(name="d", mysql_type="date"),
            ColumnDef(name="u", mysql_type="bigint"),
        ],
        primary_key=["id"],
    )


def _converted(body: str, zone=SEOUL):  # noqa: ANN001
    view = ViewDef(
        name="app.v",
        definition=(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`a`@`%` SQL SECURITY DEFINER VIEW `app`.`v` AS "
            + body
        ),
    )
    return SchemaConverter(source_zone=zone).convert_view(view, tables=[_table()])


def _sql(body: str, zone=SEOUL) -> str:  # noqa: ANN001
    converted = _converted(body, zone)
    assert converted.auto_converted, converted.target_ddl
    return " ".join(converted.target_ddl.split())


def _where(predicate: str, zone=SEOUL) -> str:  # noqa: ANN001
    return _sql(f"select `e`.`id` AS `id` from `app`.`ev` `e` where ({predicate})", zone)


def _select(projection: str, zone=SEOUL) -> str:  # noqa: ANN001
    return _sql(f"select {projection} AS `x` from `app`.`ev` `e`", zone)


# ---------------------------------------------------------------------------
# 1. The zone's clock where it meets a TIMESTAMP: the instant it names there
# ---------------------------------------------------------------------------


def test_a_timestamp_compared_with_the_clock_meets_the_instant_the_zone_names() -> None:
    sql = _where("`e`.`ts` > (now() - interval 1 day)")
    # The column side as it was, so an index on it still serves the comparison.
    assert f"WHERE ( \"e\".\"ts\" > {_DAY_AGO} )" in sql


def test_a_fixed_offset_names_the_instant_with_the_utc_clock() -> None:
    sql = _where("`e`.`ts` > (now() - interval 1 day)", PLUS_NINE)
    assert (
        "\"e\".\"ts\" > ( ( ( DATE_TRUNC('second', ( ( CURRENT_TIMESTAMP AT TIME ZONE 'UTC' ) "
        "+ INTERVAL '+09:00' )) - INTERVAL '1 DAY' ) - INTERVAL '+09:00' ) AT TIME ZONE 'UTC' )"
    ) in sql
    assert "AT TIME ZONE '+09:00'" not in sql  # never an offset as a (POSIX) zone name


def test_a_timestamp_since_the_zones_midnight_meets_that_midnights_instant() -> None:
    sql = _where("`e`.`ts` >= curdate()")
    assert (
        "\"e\".\"ts\" >= ( ( CAST(CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS DATE) "
        "AS TIMESTAMP) ) AT TIME ZONE 'Asia/Seoul' )"
    ) in sql


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        ("`e`.`ts` between (now() - interval 1 day) and now()",
         f"\"e\".\"ts\" BETWEEN {_DAY_AGO} AND {_INSTANT}"),
        ("`e`.`ts` in (now(),`e`.`dt`)", f"\"e\".\"ts\" IN ( {_INSTANT}, \"e\".\"dt\" )"),
        ("(`e`.`ts` <=> now())", f"\"e\".\"ts\" IS NOT DISTINCT FROM {_INSTANT}"),
        ("coalesce(`e`.`ts`,now()) > (now() - interval 1 day)",
         f"COALESCE( \"e\".\"ts\", {_INSTANT} ) > {_DAY_AGO}"),
        ("(case `e`.`ts` when now() then 1 else 0 end) = 1", f"CASE \"e\".\"ts\" WHEN {_INSTANT} THEN"),
        ("(case `e`.`id` when 8 then now() else `e`.`ts` end) > (now() - interval 1 day)",
         f"CASE \"e\".\"id\" WHEN 8 THEN {_INSTANT} ELSE \"e\".\"ts\" END ) > {_DAY_AGO}"),
    ],
)
def test_every_comparison_and_choice_of_a_timestamp_with_the_clock_meets_the_instant(
    predicate: str, expected: str
) -> None:
    sql = _where(predicate)
    assert expected in sql, sql


@pytest.mark.parametrize(
    ("projection", "expected"),
    [
        ("coalesce(`e`.`ts`,now())", f"COALESCE( \"e\".\"ts\", {_INSTANT} )"),
        ("ifnull(`e`.`ts`,curdate())",
         "COALESCE( \"e\".\"ts\", ( ( CAST(CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS "
         "DATE) AS TIMESTAMP) ) AT TIME ZONE 'Asia/Seoul' ) )"),
        ("greatest(`e`.`ts`,now())", f"GREATEST( \"e\".\"ts\", {_INSTANT} )"),
        ("nullif(`e`.`ts`,now())", f"NULLIF( \"e\".\"ts\", {_INSTANT} )"),
        ("if((`e`.`id` > 1),`e`.`ts`,now())", f"THEN \"e\".\"ts\" ELSE {_INSTANT} END"),
        ("(case when (`e`.`id` > 1) then `e`.`ts` else now() end)", f"THEN \"e\".\"ts\" ELSE {_INSTANT} END"),
    ],
)
def test_a_choice_of_a_timestamp_and_the_clock_is_an_instant(projection: str, expected: str) -> None:
    """PostgreSQL types it a timestamptz, the wall clock read as an instant in the session's
    zone -- nine hours ahead under UTC, before."""
    assert expected in _select(projection)


def test_the_zones_clock_beside_no_timestamp_stays_its_wall_clock() -> None:
    """A DATETIME holds the zone's wall clocks (G149): compared with the clock as it is."""
    assert f"WHERE ( \"e\".\"dt\" > ( {_CLOCK} - INTERVAL '1 DAY' ) )" in _where(
        "`e`.`dt` > (now() - interval 1 day)"
    )
    assert f"{_CLOCK} AS \"x\"" in _select("now()")
    assert f"COALESCE(\"e\".\"dt\", {_CLOCK})" in _select("coalesce(`e`.`dt`,now())")


def test_a_timestamp_compared_with_a_datetime_column_is_as_before() -> None:
    """No clock of the zone there: compared in the session's zone, as before (documented)."""
    assert "WHERE ( \"e\".\"ts\" > \"e\".\"dt\" )" in _where("`e`.`ts` > `e`.`dt`")


# ---------------------------------------------------------------------------
# 2. A TIMESTAMP read as a wall clock, and FROM_UNIXTIME(): read at the zone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("projection", "expected"),
    [
        ("cast(`e`.`ts` as date)", f"CAST({_TS_AT_SEOUL} AS DATE)"),
        ("cast(`e`.`ts` as time)", f"CAST({_TS_AT_SEOUL} AS TIME)"),
        ("cast(`e`.`ts` as datetime)", f"CAST({_TS_AT_SEOUL} AS TIMESTAMP)"),
        ("cast(`e`.`ts` as char charset utf8mb4)", f"CAST({_TS_AT_SEOUL} AS TEXT)"),
        ("hour(`e`.`ts`)", f"EXTRACT(HOUR FROM {_TS_AT_SEOUL})"),
        ("dayofweek(`e`.`ts`)", f"EXTRACT(DOW FROM CAST({_TS_AT_SEOUL} AS DATE))"),
        ("date_format(`e`.`ts`,'%Y-%m-%d %H:%i')", f"TO_CHAR(CAST({_TS_AT_SEOUL} AS TIMESTAMP), 'YYYY-MM-DD HH24:MI')"),
        ("monthname(`e`.`ts`)", f"TO_CHAR(CAST({_TS_AT_SEOUL} AS DATE), 'FMMonth')"),
        ("last_day(`e`.`ts`)", f"DATE_TRUNC('MONTH', {_TS_AT_SEOUL})"),
        ("to_days(`e`.`ts`)", f"CAST({_TS_AT_SEOUL} AS DATE) - CAST('0001-01-01' AS DATE)"),
        ("(to_days(now()) - to_days(`e`.`ts`))", f"CAST({_TS_AT_SEOUL} AS DATE)"),
        ("concat('at ',`e`.`ts`)", f"'at ' || TO_CHAR({_TS_AT_SEOUL}, 'YYYY-MM-DD HH24:MI:SS')"),
        ("json_object('t',`e`.`ts`)", f"TO_CHAR({_TS_AT_SEOUL}, 'YYYY-MM-DD HH24:MI:SS.US')"),
        ("convert_tz(`e`.`ts`,'+00:00','+09:00')", f"CAST({_TS_AT_SEOUL} AS TIMESTAMP) AT TIME ZONE INTERVAL '+00:00'"),
    ],
)
def test_a_timestamp_read_as_a_wall_clock_is_read_at_the_zone(projection: str, expected: str) -> None:
    sql = _select(projection)
    assert expected in sql, sql
    # A DATETIME's wall clock is read as it is.
    assert "\"dt\" AT TIME ZONE" not in _select(projection.replace("`ts`", "`dt`"))


def test_a_timestamp_read_as_a_wall_clock_adds_a_fixed_offset_to_its_utc_clock() -> None:
    assert f"CAST({_TS_AT_PLUS_NINE} AS DATE)" in _select("cast(`e`.`ts` as date)", PLUS_NINE)
    assert f"EXTRACT(HOUR FROM {_TS_AT_PLUS_NINE})" in _select("hour(`e`.`ts`)", PLUS_NINE)


def test_a_timestamps_date_compared_with_the_zones_date_reads_both_in_the_zone() -> None:
    assert (
        f"WHERE ( CAST({_TS_AT_SEOUL} AS DATE) = CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS DATE) )"
    ) in _where("cast(`e`.`ts` as date) = curdate()")
    # A wall clock read from a TIMESTAMP meets another TIMESTAMP as the instant it names.
    assert (
        f"\"e\".\"ts\" > ( ( CAST(CAST({_TS_AT_SEOUL} AS DATE) AS TIMESTAMP) ) AT TIME ZONE 'Asia/Seoul' )"
    ) in _where("`e`.`ts` > cast(`e`.`ts` as date)")


def test_from_unixtime_is_the_zones_wall_clock_of_the_instant() -> None:
    sql = _select("from_unixtime(`e`.`u`)")
    assert "( TO_TIMESTAMP(\"e\".\"u\") AT TIME ZONE 'Asia/Seoul' ) AS \"x\"" in sql
    assert "CAST(TO_TIMESTAMP" not in sql
    offset = _select("from_unixtime(`e`.`u`)", PLUS_NINE)
    assert "( ( TO_TIMESTAMP(\"e\".\"u\") AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' ) AS \"x\"" in offset


def test_from_unixtime_with_a_format_is_written_in_the_zone_once() -> None:
    sql = _sql(
        "select from_unixtime(`e`.`u`) AS `f`,from_unixtime(`e`.`u`,'%Y-%m-%d %H') AS `g` from `app`.`ev` `e`"
    )
    assert "TO_CHAR(( TO_TIMESTAMP(\"e\".\"u\") AT TIME ZONE 'Asia/Seoul' ), 'YYYY-MM-DD HH24') AS \"g\"" in sql
    assert sql.count("AT TIME ZONE 'Asia/Seoul'") == 2


def test_from_unixtime_beside_the_clock_compares_wall_clocks_and_beside_a_timestamp_the_instant() -> None:
    assert (
        "WHERE ( ( TO_TIMESTAMP(\"e\".\"u\") AT TIME ZONE 'Asia/Seoul' ) > "
        f"( {_CLOCK} - INTERVAL '1 DAY' ) )"
    ) in _where("from_unixtime(`e`.`u`) > (now() - interval 1 day)")
    assert (
        "\"e\".\"ts\" > ( ( TO_TIMESTAMP(\"e\".\"u\") AT TIME ZONE 'Asia/Seoul' ) AT TIME ZONE 'Asia/Seoul' )"
    ) in _where("`e`.`ts` > from_unixtime(`e`.`u`)")


def test_a_union_column_of_a_timestamp_and_the_clock_is_the_zones_wall_clock() -> None:
    """MySQL types it a DATETIME, the TIMESTAMP read as its wall clock -- compared outside a
    derived table with the clock as one, too."""
    sql = _sql("select `e`.`ts` AS `t` from `app`.`ev` `e` union select now() AS `now()`")
    assert f"SELECT {_TS_AT_SEOUL} AS \"t\" FROM" in sql and f"UNION SELECT {_CLOCK} AS" in sql
    nested = _sql(
        "select count(0) AS `c` from (select `e`.`ts` AS `t` from `app`.`ev` `e` union all "
        "select (now() - interval 1 hour) AS `t`) `x` where (`x`.`t` > (now() - interval 1 day))"
    )
    assert f"SELECT {_TS_AT_SEOUL} AS \"t\" FROM" in nested
    assert f"WHERE ( \"x\".\"t\" > ( {_CLOCK} - INTERVAL '1 DAY' ) )" in nested


@pytest.mark.parametrize("zone", [None, UTC, UNKNOWN])
def test_utc_or_an_unknown_zone_reads_nothing_at_a_zone(zone) -> None:  # noqa: ANN001
    body = (
        "select `e`.`id` AS `id`,cast(`e`.`ts` as date) AS `d`,hour(`e`.`ts`) AS `h`,"
        "from_unixtime(`e`.`u`) AS `f`,from_unixtime(`e`.`u`,'%H') AS `g` from `app`.`ev` `e` "
        "where ((`e`.`ts` > (now() - interval 1 day)) and (cast(`e`.`ts` as date) = curdate()))"
    )
    sql = _sql(body, zone)
    assert "AT TIME ZONE" not in sql
    assert "CAST(TO_TIMESTAMP(\"e\".\"u\") AS TIMESTAMP)" in sql and "TO_CHAR(TO_TIMESTAMP(\"e\".\"u\"), 'HH24')" in sql


@pytest.mark.parametrize("zone", [SEOUL, PLUS_NINE])
def test_every_zoned_view_parses_as_postgresql(zone) -> None:  # noqa: ANN001
    body = (
        "select `e`.`id` AS `id`,coalesce(`e`.`ts`,now()) AS `c`,date_format(`e`.`ts`,'%H') AS `h`,"
        "from_unixtime(`e`.`u`,'%H') AS `g` from `app`.`ev` `e` where ((`e`.`ts` between "
        "(now() - interval 1 day) and now()) or (`e`.`ts` >= curdate()) or (`e`.`ts` in (now(),`e`.`dt`)))"
    )
    sqlglot.parse_one(_sql(body, zone), read="postgres")


# ---------------------------------------------------------------------------
# 3. CONVERT_TZ() no longer clobbers the source zone
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("zone", [None, UTC, SEOUL, PLUS_NINE])
def test_a_view_calling_convert_tz_and_reading_the_clock_converts(zone) -> None:  # noqa: ANN001
    converted = _converted(
        "select convert_tz(`e`.`dt`,'+00:00','+09:00') AS `x`,now() AS `n`,curdate() AS `d`,"
        "from_unixtime(`e`.`u`) AS `f` from `app`.`ev` `e`",
        zone,
    )
    assert converted.auto_converted, converted.target_ddl
    sql = " ".join(converted.target_ddl.split())
    assert "AT TIME ZONE INTERVAL '+00:00' AT TIME ZONE INTERVAL '+09:00'" in sql
    if zone is not None and zone.shifts:
        assert "CURRENT_TIMESTAMP AT TIME ZONE" in sql and "LOCALTIMESTAMP" not in sql
    else:
        assert "LOCALTIMESTAMP" in sql and "CURRENT_DATE" in sql


# ---------------------------------------------------------------------------
# 4. What Evaluation and the manual say
# ---------------------------------------------------------------------------


def _zone_risk(inventory: SourceInventory) -> str:
    report = CompatibilityAssessor().assess(inventory)
    (item,) = [i for i in report.items if any(c.rule_id == SOURCE_TIME_ZONE_RULE_ID for c in i.concerns)]
    return item.risk


def test_the_zone_item_says_the_views_read_timestamps_in_the_zone() -> None:
    view = ViewDef(name="app.v", definition="select `e`.`id` AS `id` from `app`.`ev` `e`")
    risk = _zone_risk(SourceInventory(tables=[_table()], views=[view], server_settings={"time_zone": "Asia/Seoul"}))
    assert (
        "the views read FROM_UNIXTIME() and a TIMESTAMP column's date, time and text (DATE(), "
        "HOUR(), DATE_FORMAT() ...) in that zone too, and compare that zone's clock with a "
        "TIMESTAMP column as the instant it names there, as MySQL does"
    ) in risk
    assert "FROM_UNIXTIME" not in _zone_risk(
        SourceInventory(tables=[_table()], server_settings={"time_zone": "Asia/Seoul"})
    )


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_a_view_reads_timestamps_in_the_source_zone(language: str) -> None:
    text = " ".join((_MANUAL / language / "02-evaluation-and-schema-conversion.md").read_text(encoding="utf-8").split())
    section = text.split("## 2.1", 1)[1].split("### ", 1)[0]
    for spelled in ("`FROM_UNIXTIME()`", "`DATE_FORMAT()`", "`ts > NOW() - INTERVAL 1 DAY`", "`UNION`"):
        assert spelled in section, (language, spelled)
