# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G192: a source zone named by a tzdata link, a TIMESTAMP's CURDATE() under a source zone,
and where the source server settings are shown.

* A zone name tzdata defines as a LINK -- another name of a zone -- was written verbatim:
  ``now() AT TIME ZONE 'US/Eastern'``. A PostgreSQL build need not know such a name (Debian
  ships tzdata's old names apart; the postgres 16 and 17 images know no US/Eastern,
  US/Central, US/Pacific, US/Alaska, US/East-Indiana, Asia/Calcutta, Brazil/East,
  Canada/Newfoundland, Canada/Saskatchewan, America/Buenos_Aires -- names the RDS and Aurora
  MySQL time_zone parameter offers -- nor Japan or ROK), and CREATE TABLE / CREATE VIEW still
  accept it: the error (22023) came only when a row took the default or the view was read,
  after cut over. Every link is now written as the zone it links to (the same zone by
  definition), from a table generated from tzdata.zi (core/tz_links.py), for MySQL's
  time_zone and PostgreSQL's TimeZone alike; G149's CET / MET / EET / WET come out as before
  (tzdata links them the same way), and the finding quotes the source's own name and says
  the zone's own is written. A MySQL view's (or a Query Converter statement's) CONVERT_TZ()
  names its zones itself: a literal link there is written as the zone it links to too.
* ``TIMESTAMP DEFAULT (CURDATE())`` was written CURRENT_DATE whatever the zone: UTC's
  midnight on Aurora DSQL, where MySQL stores the source zone's. It is the instant the
  zone's date begins now (``date_trunc``: the transpile reads a cast to TIMESTAMP as
  timestamptz) -- the first midnight where a DST change repeats it, as MySQL stores it --
  never with an offset as a zone name, and SOURCE_TIME_ZONE counts it.
* The manual (2.1, en / ko / ja) said the server settings are listed "on the report": they
  are on the Evaluation screen, and the downloaded reports leave the list out.

Checked on throwaway postgres:16 and postgres:17 containers against MySQL 8.0.46 (commit
message); doubles here: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

import re
import zoneinfo
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import sqlglot

from dsql_migrator.core import source_time_zone, tz_links
from dsql_migrator.core.assessor import SOURCE_TIME_ZONE_RULE_ID, CompatibilityAssessor
from dsql_migrator.core.converter import (
    SchemaConverter,
    _resolve_column_default,
    dropped_default_notes,
    zoned_default_columns,
)
from dsql_migrator.core.converter_postgres import pg_zoned_clock_default
from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    TargetInventory,
    ViewDef,
)
from dsql_migrator.core.source_time_zone import SourceZone, init_connect_key, resolve_source_zone
from dsql_migrator.core.tz_links import TZ_LINKS, TZDATA_VERSION

_MY = SourceType.MYSQL
_PG = SourceType.POSTGRES
SEOUL = SourceZone("named", "Asia/Seoul", "time_zone 'Asia/Seoul'")
PLUS_NINE = SourceZone("offset", "+09:00", "time_zone '+09:00'")
MINUS_FIVE_THIRTY = SourceZone("offset", "-05:30", "time_zone '-05:30'")
UTC = SourceZone("utc", "UTC", "time_zone 'UTC'")
UNKNOWN = SourceZone("unknown", "", "time_zone 'SYSTEM', system_time_zone 'KST'")

# The time_zone names the RDS and Aurora MySQL parameter offers that the postgres 16 and 17
# images do not know, and two more names an application session uses: each a tzdata link.
_LINKED = [
    ("US/Eastern", "America/New_York"),
    ("US/Central", "America/Chicago"),
    ("US/Pacific", "America/Los_Angeles"),
    ("US/Alaska", "America/Anchorage"),
    ("US/East-Indiana", "America/Indiana/Indianapolis"),
    ("Asia/Calcutta", "Asia/Kolkata"),
    ("Brazil/East", "America/Sao_Paulo"),
    ("Canada/Newfoundland", "America/St_Johns"),
    ("Canada/Saskatchewan", "America/Regina"),
    ("America/Buenos_Aires", "America/Argentina/Buenos_Aires"),
    ("Japan", "Asia/Tokyo"),
    ("ROK", "Asia/Seoul"),
]


def _flat(sql: str) -> str:
    return " ".join(sql.split())


def _clock(name: str, mysql_type: str, default: str, *, nullable: bool = True) -> ColumnDef:
    return ColumnDef(
        name=name, mysql_type=mysql_type, nullable=nullable, default=default,
        default_is_expression=True,
    )


def _table(*columns: ColumnDef, name: str = "app.orders") -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False), *columns],
        primary_key=["id"],
    )


def _zone_item(inventory: SourceInventory, source_type: SourceType = _MY):
    (item,) = [
        item
        for item in CompatibilityAssessor(source_type=source_type).assess(inventory).items
        if item.rule_id == SOURCE_TIME_ZONE_RULE_ID
    ]
    return item


# ---------------------------------------------------------------------------
# 1. A zone named by a tzdata link is written as the zone it links to
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("name", "zone"), _LINKED)
@pytest.mark.parametrize(
    ("settings", "source_type", "quoted"),
    [
        pytest.param(lambda name: {"time_zone": name}, _MY, "time_zone '{}'", id="mysql"),
        pytest.param(
            lambda name: {"time_zone": "UTC", init_connect_key("time_zone"): name}, _MY,
            "time_zone '{}' set by init_connect", id="mysql-init_connect",
        ),
        pytest.param(lambda name: {"TimeZone": name}, _PG, "TimeZone '{}'", id="postgresql"),
    ],
)
def test_a_zone_name_tzdata_links_is_the_zone_it_links_to(
    name: str, zone: str, settings, source_type: SourceType, quoted: str  # noqa: ANN001
) -> None:
    resolved = resolve_source_zone(settings(name), source_type)
    assert (resolved.kind, resolved.zone, resolved.link) == ("named", zone, name)
    assert resolved.setting.startswith(quoted.format(name))  # the source's own name
    assert resolved.clock_sql() == f"(now() AT TIME ZONE '{zone}')"


@pytest.mark.parametrize("spelled", ["us/eastern", "US/EASTERN", "Us/Eastern"])
def test_the_link_is_found_whatever_its_case_and_the_source_spelling_kept(spelled: str) -> None:
    """MySQL and PostgreSQL read a zone name without regard to case."""
    resolved = resolve_source_zone({"time_zone": spelled}, _MY)
    assert (resolved.zone, resolved.link) == ("America/New_York", spelled)
    assert resolved.setting == f"time_zone '{spelled}'"


@pytest.mark.parametrize(
    "name",
    ["Asia/Seoul", "America/New_York", "Europe/Paris", "Asia/Kolkata",
     "America/Argentina/Buenos_Aires", "America/Indiana/Indianapolis", "Etc/GMT-9"],
)
@pytest.mark.parametrize(("key", "source_type"), [("time_zone", _MY), ("TimeZone", _PG)])
def test_a_zone_tzdata_names_itself_is_written_as_it_is(name: str, key: str, source_type: SourceType) -> None:
    resolved = resolve_source_zone({key: name}, source_type)
    assert (resolved.kind, resolved.zone, resolved.link) == ("named", name, "")


@pytest.mark.parametrize(
    ("name", "zone"),
    [("CET", "Europe/Brussels"), ("met", "Europe/Brussels"), ("EET", "Europe/Athens"),
     ("Wet", "Europe/Lisbon")],
)
def test_the_abbreviation_named_zones_come_out_as_g149_wrote_them(name: str, zone: str) -> None:
    """AT TIME ZONE 'CET' is the abbreviation's +01:00 all year; tzdata links CET to the zone
    whose rules it follows, so the generated table writes what G149's own map wrote."""
    assert TZ_LINKS[name.lower()] == zone
    for key, source_type in (("time_zone", _MY), ("TimeZone", _PG)):
        resolved = resolve_source_zone({key: name}, source_type)
        assert (resolved.kind, resolved.zone, resolved.link) == ("named", zone, name)
    assert not hasattr(source_time_zone, "_ABBREVIATION_NAMED_ZONES")  # one table, not two


def test_a_link_to_utc_is_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    """A link tzdata may add to Etc/UTC is UTC itself: nothing to write, no finding."""
    monkeypatch.setitem(source_time_zone.TZ_LINKS, "etc/newutc", "Etc/UTC")
    assert resolve_source_zone({"time_zone": "Etc/NewUTC"}, _MY).kind == "utc"
    for name in ("UTC", "Etc/UCT", "Universal", "Zulu", "GMT0", "Etc/Greenwich"):
        assert resolve_source_zone({"time_zone": name}, _MY).kind == "utc", name


_CLOCKS = _table(
    _clock("created", "datetime", "CURRENT_TIMESTAMP", nullable=False),
    _clock("day", "date", "curdate()"),
    _clock("at", "time", "curtime()"),
    _clock("day_start", "timestamp", "curdate()"),
)
_VIEW = ViewDef(
    name="app.recent",
    definition="select `o`.`id` AS `id` from `app`.`orders` `o` where (`o`.`created` > (now() - interval 1 day))",
)


def test_schema_conversion_writes_the_zone_a_link_names_in_defaults_and_views() -> None:
    inventory = SourceInventory(tables=[_CLOCKS], views=[_VIEW], server_settings={"time_zone": "US/Eastern"})
    converted = SchemaConverter().convert(inventory)
    ddl = converted.tables[0].target_ddl + converted.views[0].target_ddl
    assert "US/Eastern" not in ddl
    assert ddl.count("AT TIME ZONE 'America/New_York'") >= 5, ddl
    assert "\"created\" TIMESTAMP NOT NULL DEFAULT ( NOW() AT TIME ZONE 'America/New_York' )" in _flat(ddl)


def test_a_postgresql_default_is_written_in_the_zone_a_link_names() -> None:
    zone = resolve_source_zone({"TimeZone": "Asia/Calcutta"}, _PG)
    assert pg_zoned_clock_default("now()", "timestamp without time zone", zone) == (
        "(now() AT TIME ZONE 'Asia/Kolkata')"
    )
    table = TableDef(
        name="app.events",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="day", mysql_type="date", default="CURRENT_DATE")],
        primary_key=["id"],
    )
    inventory = SourceInventory(tables=[table], server_settings={"TimeZone": "Asia/Calcutta"})
    ddl = SchemaConverter(source_type=_PG).convert(inventory).tables[0].target_ddl
    assert "DEFAULT ((now() AT TIME ZONE 'Asia/Kolkata')::date)" in ddl and "Calcutta" not in ddl


@pytest.mark.parametrize(
    ("settings", "source_type", "quoted"),
    [({"time_zone": "US/Eastern"}, _MY, "(time_zone 'US/Eastern'; "),
     ({"TimeZone": "US/Eastern"}, _PG, "neither its role nor the client sets one; ")],
)
def test_the_finding_quotes_the_sources_name_and_says_the_zones_own_is_written(
    settings: dict, source_type: SourceType, quoted: str
) -> None:
    table = _CLOCKS if source_type is _MY else TableDef(
        name="app.events",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="c", mysql_type="timestamp without time zone", default="now()")],
        primary_key=["id"],
    )
    item = _zone_item(SourceInventory(tables=[table], server_settings=settings), source_type)
    assert "time zone 'America/New_York'" in item.risk or "time zone is 'America/New_York'" in item.risk
    assert (
        f"{quoted}'US/Eastern' is a tzdata link to that zone, so Schema Conversion writes the "
        "zone's own name, which every PostgreSQL build reads as that zone -- a build may lack "
        "the link, and a default or view that names a zone the build does not know is accepted "
        "and fails only when a row takes the default or the view is read)"
    ) in item.risk
    assert "SET TIME ZONE 'America/New_York' on every new connection" in item.recommendation
    assert "US/Eastern" not in item.recommendation


def test_a_zone_named_by_its_own_name_gets_no_link_note() -> None:
    item = _zone_item(SourceInventory(tables=[_CLOCKS], server_settings={"time_zone": "America/New_York"}))
    assert "(time_zone 'America/New_York'). MySQL's NOW()" in item.risk
    assert "tzdata link" not in item.risk


# A MySQL view's CONVERT_TZ() names its zones itself: a link there failed the same way when
# the view was read (review of G192), whatever the source's own zone.
_EV = TableDef(
    name="app.ev",
    columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
             ColumnDef(name="dt", mysql_type="datetime"),
             ColumnDef(name="tz", mysql_type="varchar(64)")],
    primary_key=["id"],
)


def _view_sql(projection: str, zone: "SourceZone | None" = None) -> str:
    view = ViewDef(
        name="app.v",
        definition=(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`a`@`%` SQL SECURITY DEFINER VIEW `app`.`v` AS "
            f"select {projection} from `app`.`ev` `e`"
        ),
    )
    converted = SchemaConverter(source_zone=zone).convert_view(view, tables=[_EV])
    assert converted.auto_converted, converted.target_ddl
    return _flat(converted.target_ddl)


@pytest.mark.parametrize("zone", [None, UTC, SEOUL, resolve_source_zone({"time_zone": "US/Eastern"}, _MY)])
@pytest.mark.parametrize(
    ("given", "written"),
    [("US/Eastern", "America/New_York"), ("us/eastern", "America/New_York"),
     ("Asia/Calcutta", "Asia/Kolkata"), ("Japan", "Asia/Tokyo"), ("CET", "Europe/Brussels")],
)
def test_a_views_convert_tz_writes_a_link_as_the_zone_it_links_to(zone, given: str, written: str) -> None:  # noqa: ANN001
    sql = _view_sql(
        f"convert_tz(`e`.`dt`,'UTC','{given}') AS `a`,convert_tz(`e`.`dt`,'{given}','+09:00') AS `b`", zone
    )
    assert f"AT TIME ZONE 'UTC' AT TIME ZONE '{written}' AS \"a\"" in sql, sql
    assert f"AT TIME ZONE '{written}' AT TIME ZONE INTERVAL '+09:00' AS \"b\"" in sql, sql
    assert f"'{given}'" not in sql


def test_a_views_convert_tz_keeps_every_other_zone_as_written() -> None:
    sql = _view_sql(
        "convert_tz(`e`.`dt`,'utc','Asia/Seoul') AS `a`,convert_tz(`e`.`dt`,'GMT','Etc/UTC') AS `b`,"
        "convert_tz(`e`.`dt`,'UTC',`e`.`tz`) AS `c`,'US/Eastern' AS `d`"
    )
    assert "AT TIME ZONE 'utc' AT TIME ZONE 'Asia/Seoul' AS \"a\"" in sql, sql
    assert "AT TIME ZONE 'GMT' AT TIME ZONE 'Etc/UTC' AS \"b\"" in sql, sql
    assert "ELSE \"e\".\"tz\" END AS \"c\"" in sql, sql  # read at run time, as before
    assert "'US/Eastern' AS \"d\"" in sql  # a string, not a zone


def test_the_query_converter_writes_a_convert_tz_link_as_the_zone_it_links_to() -> None:
    from dsql_migrator.core.query_converter import QueryConverter

    converted = QueryConverter().convert("SELECT CONVERT_TZ(created, 'UTC', 'US/Pacific') FROM orders")
    assert "AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles'" in converted.converted_sql
    assert "US/Pacific" not in converted.converted_sql


@pytest.mark.parametrize(
    ("given", "written"),
    [("US/Eastern", "America/New_York"), ("ROK", "Asia/Seoul"), ("asia/calcutta", "Asia/Kolkata"),
     ("Asia/Seoul", "Asia/Seoul"), ("asia/seoul", "asia/seoul"), ("UTC", "UTC"), ("Etc/UCT", "Etc/UCT"),
     ("Zulu", "Zulu"), ("SYSTEM", "SYSTEM"), ("Mars/Olympus", "Mars/Olympus")],
)
def test_written_zone_name_maps_links_only(given: str, written: str) -> None:
    assert source_time_zone.written_zone_name(given) == written


# ---------------------------------------------------------------------------
# 2. The generated table
# ---------------------------------------------------------------------------

_TZ_LINKS_FILE = Path(tz_links.__file__)


def test_the_table_maps_each_link_in_lower_case_to_a_zone() -> None:
    name = re.compile(r"^[A-Za-z][A-Za-z0-9_+\-]*(?:/[A-Za-z0-9_+\-]+)*$")
    assert len(TZ_LINKS) > 200
    for link, zone in TZ_LINKS.items():
        assert link == link.lower() and name.match(link) and name.match(zone), (link, zone)
        assert zone.lower() not in TZ_LINKS, (link, zone)  # a zone, never another link
        assert zone.lower() != link


def test_the_header_names_the_tzdata_release_and_the_command_that_regenerates_it() -> None:
    header = _TZ_LINKS_FILE.read_text(encoding="utf-8").split('"""', 1)[0]
    assert f"tzdata.zi of tzdata {TZDATA_VERSION} (its {len(TZ_LINKS)} " in header
    assert "#   .venv/bin/python -m dsql_migrator.core.tz_links /usr/share/zoneinfo/tzdata.zi\n" in header
    assert TZDATA_VERSION.startswith("2026c")


def _tzdata_zi(version: str, links: "dict[str, str]") -> str:
    zones = sorted(set(links.values()))
    return "\n".join(
        [f"# version {version}", "# redo posix_only"]
        + [f"Z {zone} 0 - UTC" for zone in zones]
        + [f"L {zone} {link}" for link, zone in links.items()]
    ) + "\n"


def test_the_module_is_what_its_command_writes() -> None:
    """No hand edit: regenerated from its own links it comes out byte for byte the same."""
    module = _TZ_LINKS_FILE.read_text(encoding="utf-8")
    assert tz_links._regenerated(module, _tzdata_zi(TZDATA_VERSION, TZ_LINKS)) == module


def test_the_command_takes_another_releases_links() -> None:
    module = _TZ_LINKS_FILE.read_text(encoding="utf-8")
    rewritten = tz_links._regenerated(module, _tzdata_zi("2099z", {"ROK": "Asia/Seoul", "Japan": "Asia/Tokyo"}))
    assert "tzdata.zi of tzdata 2099z (its 2 " in rewritten
    assert 'TZDATA_VERSION = "2099z"' in rewritten
    table = rewritten.split("TZ_LINKS: dict[str, str] = {\n", 1)[1].split("\n}", 1)[0]
    assert table == '    "japan": "Asia/Tokyo",\n    "rok": "Asia/Seoul",'
    # Everything else is left as it was.
    assert rewritten.split("def _regenerated", 1)[1] == module.split("def _regenerated", 1)[1]
    with pytest.raises(ValueError):
        tz_links._regenerated(module, "Z Asia/Seoul 9 - KST\n")


def test_the_command_refuses_a_tzdata_zi_built_with_backzone_data() -> None:
    """Debian's tzdata.zi (2026c: 162 links, against 257 without backzone) makes Africa/Asmara
    a zone and Africa/Asmera a link to it: a build without backzone has Africa/Asmara only as a
    link, which a build may leave out."""
    module = _TZ_LINKS_FILE.read_text(encoding="utf-8")
    backzone = "# version 2026c\n# redo posix_only\n# ddeps backzone zone.tab\nL Africa/Asmara Africa/Asmera\n"
    with pytest.raises(ValueError, match="backzone"):
        tz_links._regenerated(module, backzone)


_HOST_TZDATA_ZI = Path("/usr/share/zoneinfo/tzdata.zi")


def test_the_table_is_what_its_command_writes_from_this_hosts_tzdata_of_that_release() -> None:
    """Against the real tzdata.zi, not the table itself: a hand edit that keeps the format
    (a link pointed at another zone of the same offsets) is caught where the host has the
    release the table names, built without backzone data (macOS's)."""
    try:
        zi = _HOST_TZDATA_ZI.read_text(encoding="utf-8")
    except OSError:
        pytest.skip("this host has no /usr/share/zoneinfo/tzdata.zi")
    if not zi.startswith(f"# version {TZDATA_VERSION}\n"):
        pytest.skip(f"this host's tzdata.zi is not tzdata {TZDATA_VERSION}")
    module = _TZ_LINKS_FILE.read_text(encoding="utf-8")
    try:
        regenerated = tz_links._regenerated(module, zi)
    except ValueError:
        pytest.skip("this host's tzdata.zi is built with backzone data")
    assert regenerated == module


def test_each_link_keeps_its_zones_offsets_where_this_hosts_tzdata_has_both() -> None:
    """The link and its zone give the same UTC offset every day of the coming year, where the
    host's tzdata (Python's zoneinfo) has both names -- as zones of their own, too, on a
    build with tzdata's backzone data (Debian's)."""
    available = {name.lower(): name for name in zoneinfo.available_timezones()}
    pairs = [
        (available[link], available[zone.lower()])
        for link, zone in TZ_LINKS.items()
        if link in available and zone.lower() in available
    ]
    if not pairs:
        pytest.skip("this host's tzdata has neither name of any link")
    start = datetime(2026, 10, 1, 12, tzinfo=timezone.utc)
    days = [start + timedelta(days=day) for day in range(366)]
    differ = [
        (link, zone)
        for link, zone in pairs
        if any(
            day.astimezone(zoneinfo.ZoneInfo(link)).utcoffset()
            != day.astimezone(zoneinfo.ZoneInfo(zone)).utcoffset()
            for day in days
        )
    ]
    assert differ == []


# ---------------------------------------------------------------------------
# 3. A TIMESTAMP's CURDATE() under a source zone
# ---------------------------------------------------------------------------

_SEOUL_DAY = "DATE_TRUNC('DAY', ( NOW() AT TIME ZONE 'Asia/Seoul' ))"
# The zone's midnight read back at the zone, or -- where a DST change repeats it -- the first
# one: the hour before, read there, plus the hour (review of G192).
_SEOUL_MIDNIGHT = (
    f"LEAST( ( {_SEOUL_DAY} AT TIME ZONE 'Asia/Seoul' ), ( ( ( {_SEOUL_DAY} - INTERVAL '1 HOUR' ) "
    "AT TIME ZONE 'Asia/Seoul' ) + INTERVAL '1 HOUR' ) )"
)


def _offset_midnight(offset: str) -> str:
    return (
        f"( ( DATE_TRUNC('DAY', ( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '{offset}' )) - "
        f"INTERVAL '{offset}' ) AT TIME ZONE 'UTC' )"
    )


@pytest.mark.parametrize(
    ("zone", "expected"),
    [
        (SEOUL, _SEOUL_MIDNIGHT),
        (PLUS_NINE, _offset_midnight("+09:00")),
        (MINUS_FIVE_THIRTY, _offset_midnight("-05:30")),
    ],
)
def test_a_timestamp_curdate_default_is_the_instant_the_zones_date_begins(zone, expected: str) -> None:  # noqa: ANN001
    """MySQL stores CURDATE() in a TIMESTAMP as the date's midnight in the session's zone
    (2026-10-23 15:00:00 UTC for Seoul's 2026-10-24); CURRENT_DATE is UTC's midnight on
    Aurora DSQL, nine hours later."""
    table = _table(
        _clock("day_start", "timestamp", "curdate()"),
        _clock("day_start3", "timestamp(3)", "curdate()", nullable=False),
        _clock("today", "timestamp", "CURRENT_DATE"),
    )
    ddl = _flat(SchemaConverter(source_zone=zone).convert_table(table).target_ddl)
    assert f"\"day_start\" TIMESTAMPTZ DEFAULT {expected}," in ddl, ddl
    assert f"\"day_start3\" TIMESTAMPTZ(3) NOT NULL DEFAULT {expected}," in ddl, ddl
    assert f"\"today\" TIMESTAMPTZ DEFAULT {expected}," in ddl, ddl
    assert "CURRENT_DATE" not in ddl
    # Never an offset as a zone name (POSIX sign), never a cast the transpile makes timestamptz.
    assert "AT TIME ZONE '+" not in ddl and "AT TIME ZONE '-" not in ddl
    assert "AS TIMESTAMPTZ" not in ddl
    assert isinstance(sqlglot.parse_one(ddl, read="postgres"), sqlglot.exp.Create)


@pytest.mark.parametrize("zone", [None, UTC, UNKNOWN])
def test_a_timestamp_curdate_default_stays_current_date_in_utc(zone) -> None:  # noqa: ANN001
    ddl = _flat(SchemaConverter(source_zone=zone).convert_table(_table(_clock("d", "timestamp", "curdate()"))).target_ddl)
    assert "\"d\" TIMESTAMPTZ DEFAULT CURRENT_DATE" in ddl


def test_the_zone_midnight_is_read_back_at_the_zone_or_less_the_offset_at_utc() -> None:
    """A named zone's: the earlier of its midnight read back at the zone -- PostgreSQL reads a
    midnight a DST change repeats (Atlantic/Azores' and America/Havana's fall-back day) as
    the later one -- and the hour before it read there plus the hour, the first midnight,
    which MySQL stores (8.0.46: 2026-10-25 00:00 UTC for the Azores, not 01:00)."""
    day = "date_trunc('day', (now() AT TIME ZONE 'Asia/Seoul'))"
    assert SEOUL.midnight_sql() == (
        f"LEAST(({day} AT TIME ZONE 'Asia/Seoul'), "
        f"((({day} - INTERVAL '1 hour') AT TIME ZONE 'Asia/Seoul') + INTERVAL '1 hour'))"
    )
    assert PLUS_NINE.midnight_sql() == (
        "((date_trunc('day', ((now() AT TIME ZONE 'UTC') + INTERVAL '+09:00')) - INTERVAL "
        "'+09:00') AT TIME ZONE 'UTC')"
    )
    for zone in (UTC, UNKNOWN):
        with pytest.raises(ValueError):
            zone.midnight_sql()


def test_source_time_zone_counts_a_timestamp_curdate_default() -> None:
    table = _table(
        _clock("day_start", "timestamp", "curdate()"),
        _clock("instant", "timestamp", "CURRENT_TIMESTAMP"),
        _clock("at", "timestamp", "curtime()"),
    )
    assert zoned_default_columns(table, SEOUL, is_postgres=False) == ["day_start"]
    assert zoned_default_columns(table, PLUS_NINE, is_postgres=False) == ["day_start"]
    assert zoned_default_columns(table, None, is_postgres=False) == []
    item = _zone_item(SourceInventory(tables=[table], server_settings={"time_zone": "Asia/Seoul"}))
    assert "and a TIMESTAMP column's CURDATE() the instant that zone's date begins" in item.risk
    assert "(1 column: app.orders.day_start)" in item.risk
    # Only the text changes: the default is carried with or without a zone.
    column = table.columns[1]
    assert _resolve_column_default(column, SEOUL)[1] == _resolve_column_default(column)[1] is None
    assert dropped_default_notes(table, is_postgres=False) == []


# ---------------------------------------------------------------------------
# 4. Where the server settings are shown, and what the manual says
# ---------------------------------------------------------------------------

_SENTINEL = "4242424"  # a group_concat_max_len no finding grades or quotes


def test_the_settings_list_is_on_the_screen_and_not_in_the_downloaded_reports() -> None:
    from dsql_migrator.ui.evaluation import (
        EvaluationResult,
        EvaluationState,
        _render_result,
        assessment_download,
        source_settings_caption,
    )
    from tests.test_ui_evaluation import _ItemUi

    inventory = SourceInventory(
        tables=[_table()],
        server_settings={"time_zone": "UTC", "group_concat_max_len": _SENTINEL},
    )
    result = EvaluationResult(
        inventory=inventory, assessment=CompatibilityAssessor().assess(inventory),
        target_inventory=TargetInventory(), target_conflicts=[],
    )
    ui = _ItemUi()
    _render_result(ui, result, eval_state=EvaluationState())
    assert source_settings_caption(_MY) in ui.texts and _SENTINEL in ui.texts
    for fmt in ("html", "json", "text"):
        content = assessment_download(result, fmt).content
        assert _SENTINEL not in content and "Source server settings" not in content, fmt


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_SAID = {
    # Chapter 2 is a user guide (2026-10-25): how link names and CURDATE() are written is
    # CHANGELOG detail; the manual keeps where the settings are shown.
    "en": (
        ("lists them on the Evaluation screen under the source object counts (**Source server "
         "settings**, collapsed; the downloaded reports leave the list out, though each item that "
         "grades a setting quotes its value)"),
    ),
    "ko": (
        ("읽은 설정은 Evaluation 화면의 소스 객체 수 아래(**Source server settings**, 접힌 상태)에 "
         "보여 줍니다(내려받는 리포트에는 이 목록이 들어가지 않으며"),
    ),
    "ja": (
        ("読み取った設定は、Evaluation 画面のソースオブジェクト数の下 (**Source server settings**、"
         "折りたたみ表示) に一覧されます (ダウンロードするレポートにはこの一覧は含まれませんが"),
    ),
}
_GONE = {
    "en": "lists them on the report under the source object counts",
    "ko": "읽은 설정은 리포트의 소스 객체 수 아래",
    "ja": "読み取った設定は、レポートのソースオブジェクト数の下",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_where_the_settings_are_and_how_links_and_curdate_are_written(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "02-evaluation-and-schema-conversion.md").read_text(encoding="utf-8").split()
    )
    section = text.split("## 2.1", 1)[1].split("### ", 1)[0]
    for claim in _SAID[language]:
        assert " ".join(claim.split()) in section, (language, claim)
    assert _GONE[language] not in text, language

