# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G149: the source's time zone -- zone-explicit DATETIME / timestamp defaults and clock
functions.

A MySQL source's NOW() / CURRENT_TIMESTAMP / CURDATE() read the wall clock of the session's
time_zone, and a PostgreSQL source's now() written into a timestamp without time zone / date
column reads the session's TimeZone; the column stores that wall clock as it is, and Full
Load and CDC copy it unchanged. An Aurora DSQL session runs in UTC unless it sets another
zone, so the conversion wrote every clock default and MySQL view in UTC whatever zone the
source ran in, and the manual said DATETIME was "normalized to UTC".

* The zone: resolve_source_zone reads G148's server_settings -- MySQL's time_zone (SYSTEM
  is system_time_zone, told only when UTC; a zone name; a fixed offset), PostgreSQL's
  TimeZone (a zone name; a POSIX fixed offset, west-signed) -- and SourceZone writes its wall
  clock: a zone name AT TIME ZONE, a fixed offset as the UTC clock plus an INTERVAL (never an
  offset as a zone name: PostgreSQL reads '+09:00' with the POSIX sign, UTC-9).
* The conversion: MySQL DATETIME / DATE / TIME clock defaults and PostgreSQL timestamp
  without time zone / date / time ones name the zone; TIMESTAMP / timestamptz keep now();
  MySQL views' NOW(), SYSDATE(), CURDATE(), CURTIME() read the zone's clock; PostgreSQL views
  are carried as deparsed. UTC and an unknown zone change nothing.
* The finding: SOURCE_TIME_ZONE (MANUAL) names what the conversion wrote and what the
  application must do (SET TIME ZONE on connect); a zone that cannot be told is an advisory
  note; UTC, or nothing read, says nothing.
* The data: Full Load copies a DATETIME's wall clock unchanged; the manual (en / ko / ja) now
  says so.

Doubles only: no MySQL, no PostgreSQL, no AWS.
"""

from __future__ import annotations

import inspect
from datetime import datetime
from pathlib import Path

import pytest
import sqlglot

from dsql_migrator.core.assessor import (
    SOURCE_TIME_ZONE_RULE_ID,
    CompatibilityAssessor,
)
from dsql_migrator.core.converter import (
    SchemaConverter,
    _converted_mysql_view,
    _resolve_column_default,
    dropped_default_notes,
    rewrite_mysql_expressions,
    zoned_default_columns,
)
from dsql_migrator.core.converter_postgres import pg_zoned_clock_default
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)
from dsql_migrator.core.source_time_zone import (
    SourceZone,
    resolve_source_zone,
    views_reading_the_clock,
)

_MY = SourceType.MYSQL
_PG = SourceType.POSTGRES
SEOUL = SourceZone("named", "Asia/Seoul", "time_zone 'Asia/Seoul'")
PLUS_NINE = SourceZone("offset", "+09:00", "time_zone '+09:00'")
MINUS_FIVE_THIRTY = SourceZone("offset", "-05:30", "time_zone '-05:30'")
UTC = SourceZone("utc", "UTC", "time_zone 'UTC'")
UNKNOWN = SourceZone("unknown", "", "time_zone 'SYSTEM', system_time_zone 'KST'")


def _flat(sql: str) -> str:
    return " ".join(sql.split())


# ---------------------------------------------------------------------------
# 1. The zone: what the source runs in, and how its clock is written
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("settings", "kind", "zone"),
    [
        ({"time_zone": "Asia/Seoul"}, "named", "Asia/Seoul"),
        ({"time_zone": "US/Pacific"}, "named", "US/Pacific"),
        ({"time_zone": "America/Argentina/Buenos_Aires"}, "named", "America/Argentina/Buenos_Aires"),
        ({"time_zone": "Etc/GMT-9"}, "named", "Etc/GMT-9"),
        ({"time_zone": "+09:00"}, "offset", "+09:00"),
        ({"time_zone": "+9:00"}, "offset", "+09:00"),
        ({"time_zone": "-05:30"}, "offset", "-05:30"),
        ({"time_zone": "UTC"}, "utc", "UTC"),
        ({"time_zone": "Etc/UTC"}, "utc", "UTC"),
        ({"time_zone": "gmt"}, "utc", "UTC"),
        ({"time_zone": "+00:00"}, "utc", "UTC"),
        ({"time_zone": "-00:00"}, "utc", "UTC"),
        # SYSTEM is the server's own zone, which MySQL reports by an abbreviation.
        ({"time_zone": "SYSTEM", "system_time_zone": "UTC"}, "utc", "UTC"),
        ({"time_zone": "SYSTEM", "system_time_zone": "KST"}, "unknown", ""),
        ({"time_zone": "SYSTEM", "system_time_zone": "+04"}, "unknown", ""),
        ({"time_zone": "SYSTEM"}, "unknown", ""),
        ({"time_zone": "+15:00"}, "unknown", ""),
        ({"time_zone": "+09:75"}, "unknown", ""),
        ({"time_zone": "Asia/Seoul'; DROP"}, "unknown", ""),
        ({}, "unknown", ""),
        ({"sql_mode": ""}, "unknown", ""),
    ],
)
def test_the_mysql_zone_is_resolved_from_its_global_time_zone(
    settings: dict, kind: str, zone: str
) -> None:
    resolved = resolve_source_zone(settings, _MY)
    assert (resolved.kind, resolved.zone) == (kind, zone)
    assert resolved.shifts is (kind in ("named", "offset"))


def test_the_resolved_zone_quotes_what_the_source_reported() -> None:
    assert resolve_source_zone({"time_zone": "Asia/Seoul"}, _MY).setting == "time_zone 'Asia/Seoul'"
    system = resolve_source_zone({"time_zone": "SYSTEM", "system_time_zone": "KST"}, _MY)
    assert system.setting == "time_zone 'SYSTEM', system_time_zone 'KST'"
    assert resolve_source_zone({}, _MY).setting == ""  # nothing was read
    assert resolve_source_zone({"TimeZone": "<+09>-09"}, _PG).setting == "TimeZone '<+09>-09'"


@pytest.mark.parametrize(
    ("value", "kind", "zone"),
    [
        ("Asia/Seoul", "named", "Asia/Seoul"),
        ("EST5EDT", "named", "EST5EDT"),  # a zone file of that name, not a POSIX string
        ("Japan", "named", "Japan"),
        ("UTC", "utc", "UTC"),
        ("Etc/UCT", "utc", "UTC"),
        # A POSIX fixed offset is signed WEST of UTC: '<+09>-09' (what SET TIME ZONE 9
        # shows) is UTC+09:00, 'UTC+9' and a bare '+09:00' are UTC-09:00.
        ("<+09>-09", "offset", "+09:00"),
        ("<+0530>-05:30", "offset", "+05:30"),
        ("JST-9", "offset", "+09:00"),
        ("UTC+9", "offset", "-09:00"),
        ("+09:00", "offset", "-09:00"),
        ("EST5", "offset", "-05:00"),
        ("UTC0", "utc", "UTC"),
        ("<+00>+00", "utc", "UTC"),
        # Daylight-saving rules, seconds, and the server's own zone cannot be written.
        ("EST5EDT,M3.2.0,M11.1.0", "unknown", ""),
        ("<+10>-10<+11>,M10.1.0,M4.1.0/3", "unknown", ""),
        ("<+0530>-05:30:15", "unknown", ""),
        ("localtime", "unknown", ""),
        ("9", "unknown", ""),  # a bare number is no POSIX string (SET TIME ZONE reads it ISO-signed)
    ],
)
def test_the_postgresql_zone_is_resolved_from_its_timezone(value: str, kind: str, zone: str) -> None:
    resolved = resolve_source_zone({"TimeZone": value}, _PG)
    assert (resolved.kind, resolved.zone) == (kind, zone)


def test_a_postgresql_zone_is_unknown_until_its_timezone_is_read() -> None:
    # The ALTER ROLE defaults are another role's: not the zone the database gives a session.
    settings = {"TimeZone (ALTER ROLE app)": "Asia/Seoul"}
    assert resolve_source_zone(settings, _PG).kind == "unknown"
    assert resolve_source_zone({"time_zone": "Asia/Seoul"}, _PG).kind == "unknown"
    assert resolve_source_zone({"TimeZone": "Asia/Seoul"}, _MY).kind == "unknown"


@pytest.mark.parametrize("name", ["CET", "met", "EET", "WET"])
def test_a_zone_postgresql_reads_as_a_fixed_abbreviation_is_written_by_its_rules(name: str) -> None:
    """AT TIME ZONE 'CET' is the abbreviation's +01:00 all year (PostgreSQL looks
    abbreviations up first), while the zone CET keeps daylight saving."""
    zone = resolve_source_zone({"time_zone": name}, _MY)
    assert zone.kind == "named" and "/" in zone.zone
    assert resolve_source_zone({"TimeZone": name}, _PG).zone == zone.zone


def test_the_zone_clock_is_a_name_at_time_zone_or_the_utc_clock_plus_an_offset() -> None:
    assert SEOUL.clock_sql() == "(now() AT TIME ZONE 'Asia/Seoul')"
    assert SEOUL.date_sql() == "((now() AT TIME ZONE 'Asia/Seoul')::date)"
    assert SEOUL.time_sql() == "((now() AT TIME ZONE 'Asia/Seoul')::time)"
    assert SEOUL.clock_sql("clock_timestamp()") == "(clock_timestamp() AT TIME ZONE 'Asia/Seoul')"
    # The live-verified INTERVAL form: an offset as a zone NAME is read with the POSIX sign.
    assert PLUS_NINE.clock_sql() == "((now() AT TIME ZONE 'UTC') + INTERVAL '+09:00')"
    assert PLUS_NINE.date_sql() == "(((now() AT TIME ZONE 'UTC') + INTERVAL '+09:00')::date)"
    assert MINUS_FIVE_THIRTY.time_sql() == "(((now() AT TIME ZONE 'UTC') + INTERVAL '-05:30')::time)"
    assert "AT TIME ZONE '+09:00'" not in PLUS_NINE.clock_sql()
    assert (SEOUL.label, PLUS_NINE.label) == ("'Asia/Seoul'", "UTC+09:00")
    for zone in (UTC, UNKNOWN):
        with pytest.raises(ValueError):
            zone.clock_sql()


def test_a_zone_is_hashable_so_a_conversion_cache_can_key_on_it() -> None:
    assert {SEOUL, SourceZone("named", "Asia/Seoul", "time_zone 'Asia/Seoul'")} == {SEOUL}


# ---------------------------------------------------------------------------
# 2. MySQL column defaults
# ---------------------------------------------------------------------------


def _mysql_table(*columns: ColumnDef, name: str = "app.orders") -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False), *columns],
        primary_key=["id"],
    )


def _clock(name: str, mysql_type: str, default: str, *, nullable: bool = True) -> ColumnDef:
    return ColumnDef(
        name=name, mysql_type=mysql_type, nullable=nullable, default=default,
        default_is_expression=True,
    )


_MYSQL_CLOCK_TABLE = _mysql_table(
    _clock("created", "datetime", "CURRENT_TIMESTAMP", nullable=False),
    _clock("created_ms", "datetime(3)", "CURRENT_TIMESTAMP(3)"),
    _clock("noted", "datetime", "now()"),
    _clock("local", "datetime", "LOCALTIMESTAMP"),
    _clock("day", "date", "curdate()"),
    _clock("day_now", "date", "now()"),
    _clock("at", "time", "curtime()"),
    _clock("instant", "timestamp", "CURRENT_TIMESTAMP"),
    _clock("utc", "datetime", "utc_timestamp()"),
    _clock("utc_day", "date", "utc_date()"),
    ColumnDef(name="label", mysql_type="varchar(10)", default="x"),
)


@pytest.mark.parametrize(
    "column_sql",
    [
        "\"created\" TIMESTAMP NOT NULL DEFAULT ( NOW() AT TIME ZONE 'Asia/Seoul' ),",
        "\"created_ms\" TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'Asia/Seoul' ),",
        "\"noted\" TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'Asia/Seoul' ),",
        "\"local\" TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'Asia/Seoul' ),",
        "\"day\" DATE DEFAULT ( CAST(( NOW() AT TIME ZONE 'Asia/Seoul' ) AS DATE) ),",
        "\"day_now\" DATE DEFAULT ( CAST(( NOW() AT TIME ZONE 'Asia/Seoul' ) AS DATE) ),",
        "\"at\" TIME DEFAULT ( CAST(( NOW() AT TIME ZONE 'Asia/Seoul' ) AS TIME) ),",
        # An instant needs no zone; UTC_* are UTC whatever the zone.
        "\"instant\" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,",
        "\"utc\" TIMESTAMP DEFAULT ( NOW() AT TIME ZONE 'UTC' ),",
        "\"utc_day\" DATE DEFAULT ( CAST(( NOW() AT TIME ZONE 'UTC' ) AS DATE) ),",
        "\"label\" VARCHAR(10) DEFAULT 'x',",
    ],
)
def test_a_mysql_clock_default_names_the_source_zone(column_sql: str) -> None:
    ddl = SchemaConverter(source_zone=SEOUL).convert_table(_MYSQL_CLOCK_TABLE).target_ddl
    assert column_sql in _flat(ddl), ddl


def test_a_fixed_offset_default_is_the_utc_clock_plus_the_offset() -> None:
    ddl = _flat(SchemaConverter(source_zone=PLUS_NINE).convert_table(_MYSQL_CLOCK_TABLE).target_ddl)
    assert "\"created\" TIMESTAMP NOT NULL DEFAULT ( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' )" in ddl
    assert "\"day\" DATE DEFAULT ( CAST(( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' ) AS DATE) )" in ddl
    assert "AT TIME ZONE '+09:00'" not in ddl  # never an offset as a zone name (POSIX sign)
    assert "\"instant\" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP" in ddl


@pytest.mark.parametrize("zone", [None, UTC, UNKNOWN])
def test_utc_or_an_unknown_zone_keeps_the_session_clock(zone) -> None:  # noqa: ANN001
    plain = SchemaConverter().convert_table(_MYSQL_CLOCK_TABLE).target_ddl
    assert SchemaConverter(source_zone=zone).convert_table(_MYSQL_CLOCK_TABLE).target_ddl == plain
    flat = _flat(plain)
    assert "\"created\" TIMESTAMP NOT NULL DEFAULT ( NOW() AT TIME ZONE 'UTC' )" in flat
    assert "\"day\" DATE DEFAULT CURRENT_DATE" in flat


def test_a_time_default_reading_the_clock_is_valid_postgresql_without_a_zone() -> None:
    """CURTIME() became CURRENT_TIME, which the transpile renders CURRENT_TIME() -- a syntax
    error in PostgreSQL that failed the whole CREATE TABLE. LOCALTIME is the same time of
    day of the session."""
    ddl = SchemaConverter().convert_table(_MYSQL_CLOCK_TABLE).target_ddl
    assert "\"at\" TIME DEFAULT LOCALTIME" in _flat(ddl)
    assert "CURRENT_TIME()" not in ddl


@pytest.mark.parametrize("zone", [SEOUL, PLUS_NINE, MINUS_FIVE_THIRTY, None])
def test_every_converted_create_table_parses_as_postgresql(zone) -> None:  # noqa: ANN001
    ddl = SchemaConverter(source_zone=zone).convert_table(_MYSQL_CLOCK_TABLE).target_ddl
    assert isinstance(sqlglot.parse_one(ddl, read="postgres"), sqlglot.exp.Create)


def test_the_zone_never_decides_whether_a_default_is_carried() -> None:
    """Only the text of a carried clock default changes, so the dropped-default notes --
    Evaluation's DEFAULT_DROPPED and Schema Conversion's -- need no zone."""
    columns = [
        *_MYSQL_CLOCK_TABLE.columns,
        _clock("later", "datetime", "(now() + interval 1 day)"),
        _clock("fsp_time", "time(3)", "curtime(3)"),
        ColumnDef(name="zero", mysql_type="date", default="0000-00-00"),
    ]
    for column in columns:
        for zone in (SEOUL, PLUS_NINE):
            assert (_resolve_column_default(column, zone)[0] is None) is (
                _resolve_column_default(column)[0] is None
            ), column.name
            assert _resolve_column_default(column, zone)[1] == _resolve_column_default(column)[1]
    dropped = dropped_default_notes(_mysql_table(*columns[1:]), is_postgres=False)
    assert [column.name for column, _note in dropped] == ["fsp_time", "zero"]


@pytest.mark.parametrize(
    ("zone", "expected"),
    [
        (None, "( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '30 DAY' )"),
        (SEOUL, "( ( NOW() AT TIME ZONE 'Asia/Seoul' ) + INTERVAL '30 DAY' )"),
        (PLUS_NINE, "( ( ( NOW() AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' ) + INTERVAL '30 DAY' )"),
    ],
)
def test_a_clock_default_with_date_arithmetic_keeps_the_arithmetic(zone, expected: str) -> None:  # noqa: ANN001
    """DATETIME DEFAULT (CURRENT_TIMESTAMP + INTERVAL 30 DAY) -- information_schema's
    ``now() + interval 30 day`` -- matched the clock by prefix and became the bare clock:
    every defaulted expiry 30 days early. A TIMESTAMP keeps its instant arithmetic, and a
    default that is no clock arithmetic is dropped with its note, never taken for a clock."""
    table = _mysql_table(
        _clock("expires", "datetime", "now() + interval 30 day"),
        _clock("expires_at", "timestamp", "(current_timestamp() + interval 30 day)"),
        _clock("odd", "datetime", "now() + 0"),
    )
    conversion = SchemaConverter(source_zone=zone).convert_table(table)
    ddl = _flat(conversion.target_ddl)
    assert f"\"expires\" TIMESTAMP DEFAULT {expected}," in ddl
    assert "\"expires_at\" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP + INTERVAL '30 DAY'," in ddl
    assert "\"odd\" TIMESTAMP," in ddl
    assert [w.column_name for w in conversion.warnings if "now() + 0" in w.message] == ["odd"]
    assert isinstance(sqlglot.parse_one(conversion.target_ddl, read="postgres"), sqlglot.exp.Create)


def test_clock_arithmetic_the_transpile_cannot_read_is_dropped_not_the_table() -> None:
    table = _mysql_table(_clock("broken", "datetime", "now() + interval (("))
    conversion = SchemaConverter(source_zone=SEOUL).convert_table(table)
    assert "\"broken\" TIMESTAMP," in _flat(conversion.target_ddl)
    assert [w.column_name for w in conversion.warnings if "no Aurora DSQL equivalent" in w.message] == [
        "broken"
    ]


@pytest.mark.parametrize("zone", [None, SEOUL, PLUS_NINE])
@pytest.mark.parametrize("mysql_type", ["datetime", "date", "timestamp"])
def test_clock_arithmetic_aurora_dsql_would_compute_otherwise_is_dropped_with_its_note(
    zone, mysql_type: str  # noqa: ANN001
) -> None:
    """Only whole numbers of one unit are written. A composite unit became INTERVAL '1:30
    HOUR_MINUTE', which failed the whole CREATE TABLE; MySQL rounds a fractional amount of
    one unit (1.5 hour is 2 hours) where PostgreSQL keeps it. Either is dropped with its
    note, whatever the zone, while a quoted whole number is written."""
    table = _mysql_table(
        _clock("composite", mysql_type, "(now() + interval '1:30' hour_minute)"),
        _clock("fraction", mysql_type, "(now() + interval 1.5 hour)"),
        _clock("quoted", mysql_type, "(now() + interval '30' day)"),
        _clock("chained", mysql_type, "(now() + interval 1 year + interval 2 month)"),
    )
    conversion = SchemaConverter(source_zone=zone).convert_table(table)
    ddl = _flat(conversion.target_ddl)
    assert "HOUR_MINUTE" not in ddl and "1.5" not in ddl
    assert "INTERVAL '30 DAY'" in ddl and "INTERVAL '1 YEAR' + INTERVAL '2 MONTH'" in ddl
    dropped = [w.column_name for w in conversion.warnings if "no Aurora DSQL equivalent" in w.message]
    assert dropped == ["composite", "fraction"]
    assert [column.name for column, _note in dropped_default_notes(table, is_postgres=False)] == dropped
    assert isinstance(sqlglot.parse_one(conversion.target_ddl, read="postgres"), sqlglot.exp.Create)


def test_convert_reads_the_zone_evaluation_read_into_the_inventory() -> None:
    seoul = SourceInventory(tables=[_MYSQL_CLOCK_TABLE], server_settings={"time_zone": "Asia/Seoul"})
    ddl = SchemaConverter().convert(seoul).tables[0].target_ddl
    assert "AT TIME ZONE 'Asia/Seoul'" in ddl
    # A converter's own zone wins; an inventory that read nothing -- or UTC -- keeps UTC.
    assert "Asia/Seoul" not in SchemaConverter(source_zone=UTC).convert(seoul).tables[0].target_ddl
    for settings in ({}, {"time_zone": "SYSTEM", "system_time_zone": "UTC"}):
        plain = SourceInventory(tables=[_MYSQL_CLOCK_TABLE], server_settings=settings)
        assert "Asia/Seoul" not in SchemaConverter().convert(plain).tables[0].target_ddl


def test_zoned_default_columns_names_what_the_conversion_wrote() -> None:
    assert zoned_default_columns(_MYSQL_CLOCK_TABLE, SEOUL, is_postgres=False) == [
        "created", "created_ms", "noted", "local", "day", "day_now", "at",
    ]
    for zone in (None, UTC, UNKNOWN):
        assert zoned_default_columns(_MYSQL_CLOCK_TABLE, zone, is_postgres=False) == []
    # The generated key gets no default at all.
    keyed = _MYSQL_CLOCK_TABLE.model_copy(update={"auto_increment_column": "created"})
    assert "created" not in zoned_default_columns(keyed, SEOUL, is_postgres=False)


def test_a_per_table_re_conversion_keeps_the_zone() -> None:
    """The primary-key picker re-converts one table and stores its script: with the
    inventory's zone, so the stored script keeps the defaults the whole conversion wrote."""
    from dsql_migrator.ui import schema_conversion
    from dsql_migrator.ui.schema_conversion import SchemaConversionState, _render_pk_strategy_picker
    from tests.test_ui_schema_conversion import _PickerUi

    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="BIGINT", nullable=False),
            ColumnDef(name="customer_id", mysql_type="BIGINT", nullable=False),
            _clock("created", "datetime", "CURRENT_TIMESTAMP"),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    for zone, expected in ((SEOUL, "AT TIME ZONE 'Asia/Seoul'"), (None, "AT TIME ZONE 'UTC'")):
        ui, state = _PickerUi(), SchemaConversionState()
        _render_pk_strategy_picker(ui, table, state, lambda: None, source_zone=zone)
        ui.tile_clicks[2]()  # Composite key
        assert expected in state.get_edited_target_ddl("orders")
    source = inspect.getsource(schema_conversion._render_browser_and_preview)
    assert "source_zone=resolve_source_zone(" in source


# ---------------------------------------------------------------------------
# 3. PostgreSQL column defaults
# ---------------------------------------------------------------------------


def _pg_table(*columns: ColumnDef) -> TableDef:
    return TableDef(
        name="app.events",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False), *columns],
        primary_key=["id"],
    )


@pytest.mark.parametrize(
    ("pg_type", "default", "expected"),
    [
        ("timestamp without time zone", "now()", "(now() AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp without time zone", "CURRENT_TIMESTAMP", "(now() AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp(3) without time zone", "CURRENT_TIMESTAMP(3)",
         "(CURRENT_TIMESTAMP(3) AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp without time zone", "LOCALTIMESTAMP", "(now() AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp(0) without time zone", "LOCALTIMESTAMP(0)",
         "(CURRENT_TIMESTAMP(0) AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp without time zone", "transaction_timestamp()",
         "(transaction_timestamp() AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp without time zone", "statement_timestamp()",
         "(statement_timestamp() AT TIME ZONE 'Asia/Seoul')"),
        # The time of each row stays the time of each row.
        ("timestamp without time zone", "clock_timestamp()",
         "(clock_timestamp() AT TIME ZONE 'Asia/Seoul')"),
        ("timestamp without time zone", "(now())::timestamp without time zone",
         "(now() AT TIME ZONE 'Asia/Seoul')"),
        ("date", "CURRENT_DATE", "((now() AT TIME ZONE 'Asia/Seoul')::date)"),
        ("date", "(now())::date", "((now() AT TIME ZONE 'Asia/Seoul')::date)"),
        ("date", "now()", "((now() AT TIME ZONE 'Asia/Seoul')::date)"),
        ("date", "(clock_timestamp())::date", "((clock_timestamp() AT TIME ZONE 'Asia/Seoul')::date)"),
        ("time without time zone", "CURRENT_TIME", "((now() AT TIME ZONE 'Asia/Seoul')::time)"),
        ("time(3) without time zone", "LOCALTIME(3)",
         "((CURRENT_TIMESTAMP(3) AT TIME ZONE 'Asia/Seoul')::time)"),
    ],
)
def test_a_postgresql_clock_default_names_the_source_zone(
    pg_type: str, default: str, expected: str
) -> None:
    assert pg_zoned_clock_default(default, pg_type, SEOUL) == expected
    column = ColumnDef(name="c", mysql_type=pg_type, nullable=False, default=default)
    ddl = SchemaConverter(source_type=_PG, source_zone=SEOUL).convert_table(_pg_table(column)).target_ddl
    assert f"NOT NULL DEFAULT {expected}," in ddl
    assert isinstance(sqlglot.parse_one(ddl, read="postgres"), sqlglot.exp.Create)


@pytest.mark.parametrize(
    ("pg_type", "default"),
    [
        ("timestamp with time zone", "now()"),  # an instant needs no zone
        ("time with time zone", "CURRENT_TIME"),
        ("timestamp without time zone", "(now() AT TIME ZONE 'utc'::text)"),  # explicit already
        ("timestamp without time zone", "date_trunc('day'::text, now())"),  # another expression
        ("timestamp without time zone", "(now() + '1 day'::interval)"),
        ("timestamp without time zone", "'2024-01-01 00:00:00'::timestamp without time zone"),
        ("timestamp without time zone", "CURRENT_TIME"),  # no such assignment
        ("text", "now()"),
    ],
)
def test_another_postgresql_default_is_carried_as_it_is(pg_type: str, default: str) -> None:
    assert pg_zoned_clock_default(default, pg_type, SEOUL) is None
    column = ColumnDef(name="c", mysql_type=pg_type, nullable=False, default=default)
    ddl = SchemaConverter(source_type=_PG, source_zone=SEOUL).convert_table(_pg_table(column)).target_ddl
    plain = SchemaConverter(source_type=_PG).convert_table(_pg_table(column)).target_ddl
    assert ddl == plain


def test_a_postgresql_fixed_offset_default_is_the_utc_clock_plus_the_offset() -> None:
    assert pg_zoned_clock_default("now()", "timestamp without time zone", PLUS_NINE) == (
        "((now() AT TIME ZONE 'UTC') + INTERVAL '+09:00')"
    )
    assert pg_zoned_clock_default("CURRENT_DATE", "date", MINUS_FIVE_THIRTY) == (
        "(((now() AT TIME ZONE 'UTC') + INTERVAL '-05:30')::date)"
    )
    for zone in (None, UTC, UNKNOWN):
        assert pg_zoned_clock_default("now()", "timestamp without time zone", zone) is None


def test_a_postgresql_inventory_converts_in_its_timezone() -> None:
    table = _pg_table(
        ColumnDef(name="created", mysql_type="timestamp without time zone", nullable=False,
                  default="now()"),
        ColumnDef(name="at", mysql_type="timestamp with time zone", default="now()"),
    )
    view = ViewDef(name="app.recent", definition=" SELECT e.id\n   FROM app.events e\n  WHERE (e.created > (now() - '1 day'::interval));")
    zoned = SourceInventory(tables=[table], views=[view], server_settings={"TimeZone": "Asia/Seoul"})
    plain = SourceInventory(tables=[table], views=[view], server_settings={"TimeZone": "UTC"})
    converted = SchemaConverter(source_type=_PG).convert(zoned)
    assert "\"created\" TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'Asia/Seoul')" in converted.tables[0].target_ddl
    assert "\"at\" TIMESTAMPTZ DEFAULT now()" in converted.tables[0].target_ddl
    # A PostgreSQL view is carried as PostgreSQL deparsed it, zone or not.
    assert converted.views[0].target_ddl == SchemaConverter(source_type=_PG).convert(plain).views[0].target_ddl
    assert "now() - '1 day'::interval" in converted.views[0].target_ddl
    assert zoned_default_columns(table, resolve_source_zone(zoned.server_settings, _PG), is_postgres=True) == ["created"]


# ---------------------------------------------------------------------------
# 4. MySQL views
# ---------------------------------------------------------------------------


def _view_sql(body: str, zone=None) -> "tuple[str, tuple]":  # noqa: ANN001
    converted = _converted_mysql_view("v", f"select {body} AS `x`", (), zone)
    assert converted is not None
    return _flat(converted[0]), converted[1]


@pytest.mark.parametrize(
    ("call", "zoned"),
    [
        ("now()", "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("now(3)", "DATE_TRUNC('milliseconds', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("current_timestamp()", "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("localtime()", "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("localtimestamp", "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("sysdate()", "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("sysdate(3)", "DATE_TRUNC('milliseconds', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ))"),
        ("curdate()", "CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS DATE)"),
        ("current_date", "CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS DATE)"),
        ("curtime()", "CAST(DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' )) AS TIME)"),
        ("current_time", "CAST(DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' )) AS TIME)"),
        ("(curdate() + interval 1 day)",
         "CAST(CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS DATE) + INTERVAL '1 DAY' AS DATE)"),
        ("(now() - interval 1 hour)",
         "DATE_TRUNC('second', ( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' )) - INTERVAL '1 HOUR'"),
        # UTC_* stay UTC.
        ("utc_timestamp()", "DATE_TRUNC('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"),
        ("utc_date()", "CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)"),
    ],
)
def test_a_mysql_view_reads_the_source_zones_clock(call: str, zoned: str) -> None:
    sql, untranslated = _view_sql(call, SEOUL)
    assert zoned in sql and untranslated == ()
    assert "LOCALTIMESTAMP" not in sql and "CURRENT_DATE" not in sql


def test_a_mysql_view_under_a_fixed_offset_adds_it_to_the_utc_clock() -> None:
    sql, _ = _view_sql("now()", PLUS_NINE)
    assert "DATE_TRUNC('second', ( ( CURRENT_TIMESTAMP AT TIME ZONE 'UTC' ) + INTERVAL '+09:00' ))" in sql
    sql, _ = _view_sql("curdate()", MINUS_FIVE_THIRTY)
    assert "CAST(( ( CURRENT_TIMESTAMP AT TIME ZONE 'UTC' ) + INTERVAL '-05:30' ) AS DATE)" in sql
    assert "AT TIME ZONE '+09:00'" not in sql and "AT TIME ZONE '-05:30'" not in sql


def test_a_time_put_on_the_current_date_takes_the_source_zones_date() -> None:
    """MySQL puts a TIME read as a date on the session's current date."""
    table = TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="t", mysql_type="time")],
        primary_key=["id"],
    )
    view = ViewDef(name="shop.v", definition="select date_format(`o`.`t`,'%h:%i %p') AS `x` from `shop`.`orders` `o`")
    plain = _flat(SchemaConverter().convert_view(view, tables=[table]).target_ddl)
    zoned = _flat(SchemaConverter(source_zone=SEOUL).convert_view(view, tables=[table]).target_ddl)
    assert "( CURRENT_DATE + \"o\".\"t\" )" in plain
    assert "( CAST(( CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Seoul' ) AS DATE) + \"o\".\"t\" )" in zoned


@pytest.mark.parametrize("zone", [None, UTC, UNKNOWN])
def test_a_mysql_view_keeps_the_session_clock_in_utc_or_an_unknown_zone(zone) -> None:  # noqa: ANN001
    view = ViewDef(name="v", definition="select now() AS `a`, curdate() AS `d`, sysdate() AS `s`")
    converted = SchemaConverter(source_zone=zone).convert_view(view)
    assert converted.target_ddl == SchemaConverter().convert_view(view).target_ddl
    assert "LOCALTIMESTAMP" in converted.target_ddl and "CURRENT_DATE" in converted.target_ddl
    # SYSDATE() is still the untranslated call it was.
    assert any("sysdate()" in warning.message for warning in converted.warnings)


def test_convert_writes_the_inventorys_zone_into_mysql_views() -> None:
    view = ViewDef(name="app.v", definition="select now() AS `a`")
    inventory = SourceInventory(views=[view], server_settings={"time_zone": "Asia/Seoul"})
    assert "AT TIME ZONE 'Asia/Seoul'" in SchemaConverter().convert(inventory).views[0].target_ddl


def test_evaluation_grades_a_mysql_view_in_the_zone_schema_conversion_writes_it_in() -> None:
    """SYSDATE() is untranslated under UTC (VIEW_NOT_CONVERTED) and written under a zone."""
    view = ViewDef(name="app.v", definition="select sysdate() AS `s`")

    def flagged(settings: dict) -> bool:
        report = CompatibilityAssessor().assess(SourceInventory(views=[view], server_settings=settings))
        return any(concern.rule_id == "VIEW_NOT_CONVERTED" for item in report.items for concern in item.concerns)

    assert flagged({"time_zone": "UTC"}) and flagged({})
    assert not flagged({"time_zone": "Asia/Seoul"})


def test_the_query_converter_writes_application_sql_for_the_session_zone() -> None:
    """Application SQL runs in the application's own session, which the finding tells it to
    give the source's zone: the Query Converter applies none."""
    from dsql_migrator.core.query_converter import QueryConverter

    converted = QueryConverter().convert("SELECT NOW() AS x, CURDATE() AS d").converted_sql
    assert "LOCALTIMESTAMP" in converted and "CURRENT_DATE" in converted
    assert "AT TIME ZONE" not in converted
    assert "zone" not in inspect.signature(rewrite_mysql_expressions).parameters


# ---------------------------------------------------------------------------
# 5. The views that read the clock
# ---------------------------------------------------------------------------


def test_views_reading_the_clock_skip_strings_and_quoted_names() -> None:
    mysql = [
        ViewDef(name="a", definition="select now() AS `x`"),
        ViewDef(name="b", definition="select `t`.`now` AS `now` from `t`"),
        ViewDef(name="c", definition="select 'now()' AS `x`, 'it''s curdate()' AS `y`"),
        ViewDef(name="d", definition="select (`t`.`d` < curdate()) AS `x` from `t`"),
        ViewDef(name="e", definition="select utc_timestamp() AS `x`"),
        ViewDef(name="f", definition=""),
    ]
    assert views_reading_the_clock(mysql, _MY) == ["a", "d"]
    postgres = [
        ViewDef(name="a", definition=" SELECT now() AS x;"),
        ViewDef(name="b", definition=" SELECT t.now FROM t;"),  # a column named now
        ViewDef(name="c", definition=" SELECT 'now()'::text AS x, \"CURRENT_DATE\" FROM t;"),
        ViewDef(name="d", definition=" SELECT t.id FROM t WHERE (t.d = CURRENT_DATE);"),
        ViewDef(name="e", definition=" SELECT clock_timestamp() AS x;"),
        ViewDef(name="f", definition=" SELECT now() AS x;", unsupported_kind="materialized view"),
    ]
    assert views_reading_the_clock(postgres, _PG) == ["a", "d", "e"]


# ---------------------------------------------------------------------------
# 6. Evaluation: SOURCE_TIME_ZONE
# ---------------------------------------------------------------------------


def _zone_item(inventory: SourceInventory, source_type: SourceType = _MY):
    items = [
        item
        for item in CompatibilityAssessor(source_type=source_type).assess(inventory).items
        if item.rule_id == SOURCE_TIME_ZONE_RULE_ID
    ]
    assert len(items) <= 1
    return items[0] if items else None


_CLOCK_VIEW = ViewDef(
    name="app.recent",
    definition="select `o`.`id` AS `id` from `app`.`orders` `o` where (`o`.`created` > (now() - interval 1 day))",
)
_PLAIN_VIEW = ViewDef(name="app.plain", definition="select `o`.`id` AS `id` from `app`.`orders` `o`")


def _assert_gap(item) -> None:  # noqa: ANN001
    assert item.classification is Classification.MANUAL  # "Review needed": the warning tone
    assert item.kind == "DATABASE" and item.effort is not None and item.effort.value == "SIMPLE"
    (concern,) = item.concerns
    assert concern.note_kind is ConversionNoteKind.LOSS and concern.risk == item.risk


def _assert_advisory(item) -> None:  # noqa: ANN001
    assert item.classification is Classification.AUTO and item.effort is None
    (concern,) = item.concerns
    assert concern.note_kind is ConversionNoteKind.RECOMMENDATION  # shown as "Recommended", info


def test_a_mysql_source_in_another_zone_is_told_what_the_conversion_wrote() -> None:
    inventory = SourceInventory(
        tables=[_MYSQL_CLOCK_TABLE], views=[_CLOCK_VIEW, _PLAIN_VIEW],
        server_settings={"time_zone": "Asia/Seoul", "system_time_zone": "UTC"},
    )
    item = _zone_item(inventory)
    _assert_gap(item)
    assert item.object_name == "time_zone"
    assert "time zone 'Asia/Seoul' (time_zone 'Asia/Seoul')" in item.risk
    assert "DEFAULT (now() AT TIME ZONE 'Asia/Seoul')" in item.risk
    assert ("7 columns: app.orders.created, app.orders.created_ms, app.orders.noted, "
            "app.orders.local, app.orders.day, app.orders.day_now, app.orders.at") in item.risk
    assert "1 view: app.recent" in item.risk and "app.plain" not in item.risk
    assert "Full Load and CDC copy those values unchanged" in item.risk
    assert "TIMESTAMP columns hold instants and keep DEFAULT now()" in item.risk
    assert "UTC_TIMESTAMP() and UTC_DATE() stay UTC" in item.risk
    # The application note: Aurora DSQL sessions default to UTC; SET TIME ZONE on connect.
    assert "gets UTC on Aurora DSQL" in item.recommendation
    assert "SET TIME ZONE 'Asia/Seoul' on every new connection (Aurora DSQL allows it)" in item.recommendation
    assert "now() AT TIME ZONE 'Asia/Seoul'" in item.recommendation


def test_a_mysql_fixed_offset_is_set_as_an_interval_never_as_a_zone_name() -> None:
    inventory = SourceInventory(tables=[_MYSQL_CLOCK_TABLE], server_settings={"time_zone": "+09:00"})
    item = _zone_item(inventory)
    _assert_gap(item)
    assert "time zone UTC+09:00 (time_zone '+09:00')" in item.risk
    assert "DEFAULT ((now() AT TIME ZONE 'UTC') + INTERVAL '+09:00')" in item.risk
    assert "SET TIME ZONE INTERVAL '+09:00' HOUR TO MINUTE" in item.recommendation
    assert "SET TIME ZONE '+09:00'" not in item.recommendation
    assert "would be read as a POSIX zone, west of UTC" in item.recommendation


def test_a_zone_with_nothing_that_reads_the_clock_still_carries_the_application_note() -> None:
    inventory = SourceInventory(tables=[_mysql_table()], server_settings={"time_zone": "Europe/Paris"})
    item = _zone_item(inventory)
    _assert_gap(item)
    assert "no column default reads the clock" in item.risk and "view" not in item.risk.split("clock:")[1]
    assert "SET TIME ZONE 'Europe/Paris'" in item.recommendation


def test_the_listed_columns_stop_at_ten() -> None:
    tables = [
        _mysql_table(_clock("created", "datetime", "CURRENT_TIMESTAMP"), name=f"app.t{index:02d}")
        for index in range(12)
    ]
    item = _zone_item(SourceInventory(tables=tables, server_settings={"time_zone": "Asia/Seoul"}))
    assert "12 columns: app.t00.created" in item.risk and "and 2 more" in item.risk


@pytest.mark.parametrize(
    "settings",
    [
        {"time_zone": "UTC"},
        {"time_zone": "+00:00"},
        {"time_zone": "SYSTEM", "system_time_zone": "UTC"},
        # Not read: a failed read, or a report from before Evaluation read the settings.
        {},
        {"sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY,NO_ZERO_DATE,NO_ZERO_IN_DATE"},
    ],
)
def test_no_mysql_zone_item_in_utc_or_when_the_zone_was_not_read(settings: dict) -> None:
    inventory = SourceInventory(tables=[_MYSQL_CLOCK_TABLE], views=[_CLOCK_VIEW], server_settings=settings)
    assert _zone_item(inventory) is None


def test_a_mysql_zone_known_only_by_its_abbreviation_is_an_advisory_note() -> None:
    inventory = SourceInventory(
        tables=[_MYSQL_CLOCK_TABLE], server_settings={"time_zone": "SYSTEM", "system_time_zone": "KST"}
    )
    item = _zone_item(inventory)
    _assert_advisory(item)
    assert "(time_zone 'SYSTEM', system_time_zone 'KST')" in item.risk
    assert "abbreviation" in item.risk and "Schema Conversion assumes UTC" in item.risk
    assert "SET TIME ZONE 'Area/City'" in item.recommendation
    # The conversion keeps today's UTC defaults.
    assert "AT TIME ZONE 'UTC'" in SchemaConverter().convert(inventory).tables[0].target_ddl


def test_a_postgresql_source_in_another_zone_lists_its_defaults_views_and_roles() -> None:
    table = _pg_table(
        ColumnDef(name="created", mysql_type="timestamp without time zone", nullable=False, default="now()"),
        ColumnDef(name="day", mysql_type="date", default="CURRENT_DATE"),
        ColumnDef(name="at", mysql_type="timestamp with time zone", default="now()"),
    )
    views = [
        ViewDef(name="app.recent", definition=" SELECT e.id FROM app.events e WHERE (e.day = CURRENT_DATE);"),
        ViewDef(name="app.plain", definition=" SELECT e.id FROM app.events e;"),
    ]
    settings = {
        "TimeZone": "Asia/Seoul",
        "TimeZone (ALTER DATABASE)": "Asia/Seoul",
        "TimeZone (ALTER ROLE ALL)": "UTC",
        "TimeZone (ALTER ROLE app)": "UTC",
        "TimeZone (ALTER ROLE report IN DATABASE)": "America/New_York",
    }
    item = _zone_item(SourceInventory(tables=[table], views=views, server_settings=settings), _PG)
    _assert_gap(item)
    assert item.object_name == "TimeZone"
    assert "default time zone is 'Asia/Seoul' (TimeZone 'Asia/Seoul'" in item.risk
    assert "DEFAULT now() on a timestamp column becomes DEFAULT (now() AT TIME ZONE 'Asia/Seoul')" in item.risk
    assert "2 columns: app.events.created, app.events.day" in item.risk
    assert "Views are carried as PostgreSQL deparsed them" in item.risk and "1 view: app.recent" in item.risk
    # The roles with a zone of their own -- not the database's, nor ALTER ROLE ALL.
    assert ("ALTER ROLE app 'UTC', ALTER ROLE report IN DATABASE 'America/New_York'") in item.risk
    assert "ALTER DATABASE '" not in item.risk and "ALTER ROLE ALL '" not in item.risk
    assert "SET TIME ZONE 'Asia/Seoul' on every new connection" in item.recommendation
    assert "Review the views listed" in item.recommendation


def test_a_postgresql_posix_offset_is_written_and_set_with_its_iso_sign() -> None:
    table = _pg_table(ColumnDef(name="created", mysql_type="timestamp without time zone", default="now()"))
    item = _zone_item(SourceInventory(tables=[table], server_settings={"TimeZone": "<+09>-09"}), _PG)
    _assert_gap(item)
    assert "UTC+09:00 (TimeZone '<+09>-09'" in item.risk
    assert "INTERVAL '+09:00'" in item.risk
    assert "SET TIME ZONE INTERVAL '+09:00' HOUR TO MINUTE" in item.recommendation


def test_a_postgresql_zone_that_cannot_be_written_or_read_is_an_advisory_note() -> None:
    table = _pg_table(ColumnDef(name="created", mysql_type="timestamp without time zone", default="now()"))
    rules = _zone_item(SourceInventory(tables=[table], server_settings={"TimeZone": "EST5EDT,M3.2.0,M11.1.0"}), _PG)
    _assert_advisory(rules)
    assert "not a zone name or a fixed offset the conversion can write" in rules.risk
    # The read captured the ALTER defaults but not the TimeZone: its connection failed.
    unread = _zone_item(SourceInventory(tables=[table], server_settings={"TimeZone (ALTER ROLE app)": "Asia/Seoul"}), _PG)
    _assert_advisory(unread)
    assert "could not be read" in unread.risk and "Re-run Evaluation" in unread.recommendation
    assert "ALTER ROLE app 'Asia/Seoul'" in unread.risk


@pytest.mark.parametrize("settings", [{"TimeZone": "UTC"}, {"TimeZone": "Etc/UTC"}, {}])
def test_no_postgresql_zone_item_in_utc_or_when_nothing_was_read(settings: dict) -> None:
    table = _pg_table(ColumnDef(name="created", mysql_type="timestamp without time zone", default="now()"))
    assert _zone_item(SourceInventory(tables=[table], server_settings=settings), _PG) is None


def test_the_zone_item_grades_the_report() -> None:
    inventory = SourceInventory(tables=[_mysql_table()], server_settings={"time_zone": "Asia/Seoul"})
    report = CompatibilityAssessor().assess(inventory)
    plain = CompatibilityAssessor().assess(inventory.model_copy(update={"server_settings": {}}))
    assert report.summary[Classification.MANUAL] == plain.summary[Classification.MANUAL] + 1


def test_run_evaluation_grades_the_zone_the_read_captured() -> None:
    from dsql_migrator.ui.evaluation import run_evaluation
    from tests.test_ui_evaluation import _empty_target, _inputs, _source_factory, _target_factory

    inventory = SourceInventory(tables=[_MYSQL_CLOCK_TABLE], server_settings={"time_zone": "Asia/Seoul"})
    source, _ = _source_factory(inventory)
    target, _ = _target_factory(_empty_target())
    result = run_evaluation(_inputs(), introspector_factory=source, target_browser_factory=target)
    assert [i.rule_id for i in result.assessment.items if i.kind == "DATABASE"] == [SOURCE_TIME_ZONE_RULE_ID]


# ---------------------------------------------------------------------------
# 7. The data stays as it is
# ---------------------------------------------------------------------------


def test_the_full_load_fallback_conversion_writes_the_inventorys_zone() -> None:
    """A table with no applied conversion is re-derived by Full Load (the recreate runs it):
    with the zone Evaluation read, so it carries the defaults Schema Conversion writes."""
    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.data_migration import DataMigrationInputs
    from dsql_migrator.ui.data_migration._full_load_engine import _derived_table_conversion

    def derived(settings: dict) -> str:
        inputs = DataMigrationInputs(
            source_config=SourceConnectionConfig(host="db", database="app"),
            source_password=None,
            target_config=TargetConnectionConfig(
                cluster_endpoint="cluster.dsql.example", region="us-east-1"
            ),
            inventory=SourceInventory(tables=[_MYSQL_CLOCK_TABLE], server_settings=settings),
        )
        return _derived_table_conversion(inputs, _MYSQL_CLOCK_TABLE).target_ddl

    seoul = SourceInventory(tables=[_MYSQL_CLOCK_TABLE], server_settings={"time_zone": "Asia/Seoul"})
    assert derived({"time_zone": "Asia/Seoul"}) == SchemaConverter().convert(seoul).tables[0].target_ddl
    assert "AT TIME ZONE 'Asia/Seoul'" in derived({"time_zone": "Asia/Seoul"})
    assert "Asia/Seoul" not in derived({})


def test_full_load_copies_a_datetime_wall_clock_unchanged() -> None:
    """A DATETIME holds the source's wall clock in whatever zone it was written: the loader
    binds that wall clock to the timestamp column, shifting nothing."""
    from dsql_migrator.core.exporter import ValueConverter

    converter = ValueConverter(_MYSQL_CLOCK_TABLE)
    seoul_morning = datetime(2026, 10, 23, 9, 30, 15, 123456)
    assert converter.convert_value("created", seoul_morning) == seoul_morning
    assert converter.convert_value("created", seoul_morning).tzinfo is None


# ---------------------------------------------------------------------------
# 8. The manual says the same in every language
# ---------------------------------------------------------------------------

_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


def _chapter(language: str, name: str) -> str:
    return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())


_NOT_UTC_CLAIMS = (
    "normalized to UTC", "Normalized to UTC", "Treated/normalized", "naive-UTC", "naive UTC",
    "UTC로 정규화", "UTC로 취급", "UTC に正規化", "UTC として扱われ", "(UTC wall-clock)",
)


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_no_longer_says_a_datetime_is_normalized_to_utc(language: str) -> None:
    for name in ("02-evaluation-and-schema-conversion.md", "03-full-load.md", "11-customer-faq.md"):
        text = _chapter(language, name)
        for claim in _NOT_UTC_CLAIMS:
            assert claim not in text, (language, name, claim)


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_what_the_conversion_writes_in_the_source_zone(language: str) -> None:
    chapter = _chapter(language, "02-evaluation-and-schema-conversion.md")
    section = chapter.split("## 2.1", 1)[1].split("### ", 1)[0]
    assert f"`{SOURCE_TIME_ZONE_RULE_ID}`" in section
    for spelled in (
        "`DEFAULT (now() AT TIME ZONE 'Asia/Seoul')`",
        "`(now() AT TIME ZONE 'UTC') + INTERVAL '+09:00'`",
        "`SET TIME ZONE 'Asia/Seoul'`",
        "`SYSDATE()`",
        "`system_time_zone`",
        "`TimeZone`",
    ):
        assert spelled in section, (language, spelled)
    for name in ("03-full-load.md", "11-customer-faq.md"):
        assert f"`{SOURCE_TIME_ZONE_RULE_ID}`" in _chapter(language, name), (language, name)
