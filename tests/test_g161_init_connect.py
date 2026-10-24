# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G161: what a MySQL source's init_connect sets for the application's sessions.

A parameter group's ``init_connect`` runs at the start of every session of a user without
CONNECTION_ADMIN or SUPER -- the application's -- after the GLOBAL defaults, a long-standing
way to give the application's sessions a time zone. Evaluation read
only the GLOBAL values, so a source whose sessions get ``SET time_zone = 'Asia/Seoul'`` there
was graded and converted as UTC (every DATETIME clock default nine hours behind the data the
application wrote), and an ``init_connect`` sql_mode was not graded at all.

* The read: ``init_connect`` is read with the other settings; only the time_zone / sql_mode
  it sets is recorded (``time_zone (init_connect)``), never its text, which is arbitrary SQL.
* The zone: the one init_connect sets, which Schema Conversion writes and SOURCE_TIME_ZONE
  grades; an expression there names none (advisory).
* The mode: SQL_MODE grades init_connect's mode; ZERO_DATES what either mode lets in.

Live-checked on MySQL 5.7, 8.0 and 8.4 (commit message); doubles only here.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import (
    SOURCE_TIME_ZONE_RULE_ID,
    SQL_MODE_RULE_ID,
    ZERO_DATES_RULE_ID,
    CompatibilityAssessor,
)
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect.mysql import init_connect_settings
from dsql_migrator.core.source_time_zone import init_connect_key, resolve_source_zone

_MY = SourceType.MYSQL
_STRICT = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
_TABLE = TableDef(
    name="app.orders",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="created", mysql_type="datetime", default="CURRENT_TIMESTAMP", default_is_expression=True),
        ColumnDef(name="placed", mysql_type="date"),
    ],
    primary_key=["id"],
)


# ---------------------------------------------------------------------------
# 1. What init_connect sets
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("init_connect", "expected"),
    [
        ("SET time_zone='Asia/Seoul'", {"time_zone": "Asia/Seoul"}),
        (
            "SET NAMES utf8mb4; SET SESSION time_zone = '+09:00', sql_mode = 'NO_ENGINE_SUBSTITUTION'",
            {"time_zone": "+09:00", "sql_mode": "NO_ENGINE_SUBSTITUTION"},
        ),
        ("SET @@session.time_zone = \"Europe/Paris\";", {"time_zone": "Europe/Paris"}),
        ("set @@time_zone := 'Asia/Tokyo'", {"time_zone": "Asia/Tokyo"}),
        ("SET LOCAL time_zone = SYSTEM", {"time_zone": "SYSTEM"}),
        ("SET sql_mode = ''", {"sql_mode": ""}),
        ("SET time_zone = 'Asia/Seoul'; SET time_zone = 'UTC'", {"time_zone": "UTC"}),  # the last one wins
        ("SET time_zone = 'Asia/Seoul'; SET time_zone = DEFAULT", {}),  # the GLOBAL value again
        ("SET sql_mode = CONCAT(@@sql_mode, ',NO_ZERO_DATE')", {"sql_mode": "CONCAT(@@sql_mode, ',NO_ZERO_DATE')"}),
        ("SET @tz = 'Asia/Seoul'; SET time_zone = @tz", {"time_zone": "@tz"}),
        # No session's: a GLOBAL / PERSIST assignment (the scope carries to the next one), a
        # user variable, other statements.
        ("SET GLOBAL time_zone = 'Asia/Seoul'", {}),
        ("SET GLOBAL max_connections = 10, time_zone = 'Asia/Seoul'", {}),
        ("SET PERSIST sql_mode = ''", {}),
        ("SET @@global.time_zone = 'Asia/Seoul'", {}),
        ("SET @time_zone = 'Asia/Seoul'", {}),
        ("SET autocommit = 0; SELECT 1; CALL audit_login()", {}),
        ("", {}),
    ],
)
def test_init_connect_settings_reads_what_a_session_gets(init_connect: str, expected: dict) -> None:
    assert init_connect_settings(init_connect) == expected


def test_the_read_records_what_init_connect_sets_and_never_its_text() -> None:
    from tests.test_g148_source_server_settings import _VariablesConnection

    connection = _VariablesConnection({
        "time_zone": "SYSTEM",
        "system_time_zone": "UTC",
        "sql_mode": _STRICT,
        "init_connect": "SET NAMES utf8mb4; SET time_zone = 'Asia/Seoul', sql_mode = ''; SET @k = 'secret'",
    })
    settings = dialect_for(_MY).server_settings(connection)
    assert settings[init_connect_key("time_zone")] == "Asia/Seoul"
    assert settings[init_connect_key("sql_mode")] == ""
    assert settings["time_zone"] == "SYSTEM" and settings["sql_mode"] == _STRICT
    assert "init_connect" not in settings and "secret" not in repr(settings)
    ((_statement, params),) = connection.executed
    assert "init_connect" in params.values()
    # An init_connect that sets neither records nothing more.
    plain = dialect_for(_MY).server_settings(_VariablesConnection({"time_zone": "UTC", "init_connect": "SET NAMES utf8"}))
    assert plain == {"time_zone": "UTC"}


# ---------------------------------------------------------------------------
# 2. The zone the application's sessions run in
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("assigned", "kind", "zone"),
    [
        ("Asia/Seoul", "named", "Asia/Seoul"),
        ("+09:00", "offset", "+09:00"),
        ("UTC", "utc", "UTC"),
        ("SYSTEM", "utc", "UTC"),  # system_time_zone is UTC here
        ("@tz", "unknown", ""),
        ("CONCAT('Asia/', 'Seoul')", "unknown", ""),
    ],
)
def test_the_zone_init_connect_sets_is_the_sources(assigned: str, kind: str, zone: str) -> None:
    settings = {"time_zone": "SYSTEM", "system_time_zone": "UTC", init_connect_key("time_zone"): assigned}
    resolved = resolve_source_zone(settings, _MY)
    assert (resolved.kind, resolved.zone) == (kind, zone)
    assert resolved.setting.startswith(f"time_zone '{assigned}' set by init_connect; GLOBAL time_zone 'SYSTEM'")


def test_schema_conversion_writes_the_zone_init_connect_sets() -> None:
    settings = {"time_zone": "SYSTEM", "system_time_zone": "UTC", init_connect_key("time_zone"): "Asia/Seoul"}
    ddl = SchemaConverter().convert(SourceInventory(tables=[_TABLE], server_settings=settings)).tables[0].target_ddl
    assert "DEFAULT ( NOW() AT TIME ZONE 'Asia/Seoul' )" in " ".join(ddl.split())


def _items(inventory: SourceInventory) -> dict:
    report = CompatibilityAssessor(source_type=_MY).assess(inventory)
    wanted = (SOURCE_TIME_ZONE_RULE_ID, SQL_MODE_RULE_ID, ZERO_DATES_RULE_ID)
    return {item.rule_id: item for item in report.items if item.rule_id in wanted}


def test_source_time_zone_grades_the_zone_init_connect_sets() -> None:
    settings = {"time_zone": "SYSTEM", "system_time_zone": "UTC", "sql_mode": _STRICT,
                init_connect_key("time_zone"): "Asia/Seoul"}
    item = _items(SourceInventory(tables=[_TABLE], server_settings=settings))[SOURCE_TIME_ZONE_RULE_ID]
    assert "time zone 'Asia/Seoul' (time_zone 'Asia/Seoul' set by init_connect; GLOBAL time_zone 'SYSTEM')" in item.risk
    assert "SET TIME ZONE 'Asia/Seoul'" in item.recommendation
    # Without init_connect the same source is UTC: nothing to say.
    del settings[init_connect_key("time_zone")]
    assert SOURCE_TIME_ZONE_RULE_ID not in _items(SourceInventory(tables=[_TABLE], server_settings=settings))


def test_an_init_connect_zone_that_cannot_be_read_is_an_advisory_note() -> None:
    settings = {"time_zone": "UTC", "sql_mode": _STRICT, init_connect_key("time_zone"): "@tz"}
    item = _items(SourceInventory(tables=[_TABLE], server_settings=settings))[SOURCE_TIME_ZONE_RULE_ID]
    assert item.effort is None and "(time_zone '@tz' set by init_connect; GLOBAL time_zone 'UTC')" in item.risk
    assert "SELECT @@SESSION.time_zone, @@system_time_zone as the application's user" in item.recommendation


# ---------------------------------------------------------------------------
# 3. The sql_mode the application's sessions run with
# ---------------------------------------------------------------------------


def test_sql_mode_grades_the_mode_init_connect_sets() -> None:
    lenient = {"sql_mode": _STRICT, init_connect_key("sql_mode"): "NO_ENGINE_SUBSTITUTION"}
    item = _items(SourceInventory(tables=[_TABLE], server_settings=lenient))[SQL_MODE_RULE_ID]
    assert "init_connect sets sql_mode 'NO_ENGINE_SUBSTITUTION'" in item.risk
    assert f"(the global sql_mode is '{_STRICT}')" in item.risk
    assert "ONLY_FULL_GROUP_BY" in item.risk
    assert "set sql_mode in the init_connect of the source's DB parameter group" in item.recommendation
    assert "which sets it after the global value; it reaches new connections" in item.recommendation
    # The other way round: the application's sessions run with ONLY_FULL_GROUP_BY.
    strict = {"sql_mode": "NO_ENGINE_SUBSTITUTION", init_connect_key("sql_mode"): _STRICT}
    assert SQL_MODE_RULE_ID not in _items(SourceInventory(tables=[_TABLE], server_settings=strict))


def test_an_init_connect_mode_that_cannot_be_read_is_named_beside_the_global_one() -> None:
    settings = {"sql_mode": "NO_ENGINE_SUBSTITUTION", init_connect_key("sql_mode"): "CONCAT(@@sql_mode, ',ANSI')"}
    item = _items(SourceInventory(tables=[_TABLE], server_settings=settings))[SQL_MODE_RULE_ID]
    assert "The source's global sql_mode is 'NO_ENGINE_SUBSTITUTION'." in item.risk
    assert "init_connect sets the application's sessions' sql_mode to CONCAT(@@sql_mode, ',ANSI')" in item.risk
    assert "SELECT @@SESSION.sql_mode" in item.risk


@pytest.mark.parametrize(
    ("global_mode", "assigned"),
    [(_STRICT, "NO_ENGINE_SUBSTITUTION"), ("NO_ENGINE_SUBSTITUTION", _STRICT)],
)
def test_zero_dates_counts_what_either_mode_lets_in(global_mode: str, assigned: str) -> None:
    settings = {"sql_mode": global_mode, init_connect_key("sql_mode"): assigned}
    item = _items(SourceInventory(tables=[_TABLE], server_settings=settings))[ZERO_DATES_RULE_ID]
    assert (
        f"The source's global sql_mode is '{global_mode}' and its init_connect sets '{assigned}' for "
        "the application's sessions, which between them let it store dates Aurora DSQL has no value for"
    ) in item.risk
    assert "no NO_ZERO_DATE" in item.risk and "a zero date ('0000-00-00')" in item.risk
    assert "and in its init_connect, which sets it after the global value" in item.recommendation
    # Both strict: nothing let in.
    both = {"sql_mode": _STRICT, init_connect_key("sql_mode"): _STRICT}
    assert ZERO_DATES_RULE_ID not in _items(SourceInventory(tables=[_TABLE], server_settings=both))


# ---------------------------------------------------------------------------
# 4. What the screen and the manual say
# ---------------------------------------------------------------------------


def test_the_settings_caption_says_what_init_connect_rows_are() -> None:
    from dsql_migrator.ui.evaluation import source_settings_caption

    caption = source_settings_caption(_MY)
    assert "init_connect" in caption and "CONNECTION_ADMIN or SUPER" in caption


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_init_connect_is_read(language: str) -> None:
    text = " ".join((_MANUAL / language / "02-evaluation-and-schema-conversion.md").read_text(encoding="utf-8").split())
    section = text.split("## 2.1", 1)[1].split("### ", 1)[0]
    assert section.count("`init_connect`") >= 3, language
    assert "`CONNECTION_ADMIN`" in section and "`SUPER`" in section and "`ZERO_DATES`" in section
