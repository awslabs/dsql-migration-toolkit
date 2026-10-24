# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G164 (PG-3): the migration user's own ALTER ROLE TimeZone is not the source's default.

Evaluation reads the TimeZone a new session gets over a short connection of the migration
login. When that login has a TimeZone default of its own (ALTER ROLE migr SET timezone ...,
pg_settings.source 'user' or 'database user'), that value outranks the database's and the
parameter group's, and it is what the probe saw: a UTC source whose DBA login was set to
Asia/Seoul for interactive use had every clock default rewritten to (now() AT TIME ZONE
'Asia/Seoul') -- 9 hours ahead of what the application writes -- and an ALTER DATABASE
default of Asia/Seoul the probe had captured was ignored when the login was set to UTC. The
login's own entry was also described as "a role with a zone of its own".

Now the login's own value is never taken for the source's: the source's default is the ALTER
DATABASE one, else the ALTER ROLE ALL one (what a role without its own gets), else unknown --
an advisory note says the login's own default hides it. The login's entries are marked in
the settings and described as the migration user's. A UTC default with a role of another
zone gets an advisory note naming the role (CX). Precedence ('database user' > 'user' >
'database' > 'global' > configuration file) and each scenario below were checked live on
PostgreSQL 16.15 / 17.11 (throwaway local containers); the tests use doubles.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import check_postgres_time_zone
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect import postgres as pg_dialect_module
from tests.test_g148_source_server_settings import _PgConnection, _ProbeEngine, _probe

_PG = SourceType.POSTGRES
_LOGIN_SEOUL = ("migr", False, "Asia/Seoul", True)


def _settings(monkeypatch, read, defaults) -> dict:  # noqa: ANN001
    _probe(monkeypatch, _ProbeEngine(read))
    return dialect_for(_PG).server_settings(_PgConnection(defaults))


def _clocks() -> TableDef:
    return TableDef(
        name="rev.clocks",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="t1", mysql_type="timestamp without time zone", default="now()"),
        ],
        primary_key=["id"],
    )


def _t1_default(settings: dict) -> str:
    inventory = SourceInventory(tables=[_clocks()], server_settings=settings)
    (table,) = SchemaConverter(source_type=_PG).convert(inventory).tables
    return " ".join(table.target_ddl.split()).split('"t1" TIMESTAMP ', 1)[1].split(",", 1)[0]


def test_the_logins_own_default_is_not_taken_for_the_sources(monkeypatch) -> None:  # noqa: ANN001
    """A UTC source whose migration login has ALTER ROLE ... SET timezone 'Asia/Seoul'."""
    settings = _settings(monkeypatch, ("Asia/Seoul", "user"), [_LOGIN_SEOUL])
    assert settings == {"TimeZone (ALTER ROLE migr, the migration user)": "Asia/Seoul"}
    (item,) = check_postgres_time_zone(SourceInventory(tables=[_clocks()], server_settings=settings))
    assert item.classification is Classification.AUTO and item.effort is None  # advisory
    assert item.risk.startswith(
        "The migration user has a TimeZone default of its own (ALTER ROLE migr 'Asia/Seoul'), "
        "which is what its own sessions get: it hides the source's default"
    )
    assert "Some roles" not in item.risk  # the login's own is not described a second time
    assert "Re-run Evaluation" not in item.recommendation
    assert "SHOW TimeZone in a new session of the application's role" in item.recommendation
    # The conversion assumes UTC, as it did before G149: the clock default is kept.
    assert _t1_default(settings) == "DEFAULT now()"


def test_an_alter_database_default_the_login_hid_is_the_sources(monkeypatch) -> None:  # noqa: ANN001
    """The reverse: ALTER DATABASE ... SET timezone 'Asia/Seoul', the login set to UTC."""
    settings = _settings(
        monkeypatch, ("UTC", "user"),
        [("", True, "Asia/Seoul", False), ("migr", False, "UTC", True)],
    )
    assert settings["TimeZone"] == "Asia/Seoul"
    (item,) = check_postgres_time_zone(SourceInventory(tables=[_clocks()], server_settings=settings))
    assert item.classification is Classification.MANUAL
    assert "default time zone is 'Asia/Seoul'" in item.risk
    assert "(the migration user's ALTER ROLE migr 'UTC')" in item.risk
    assert _t1_default(settings) == "DEFAULT (now() AT TIME ZONE 'Asia/Seoul')"


@pytest.mark.parametrize(
    ("defaults", "expected"),
    [
        # ALTER ROLE ALL is next when there is no ALTER DATABASE default.
        ([("", False, "Asia/Tokyo", False), ("migr", True, "Asia/Seoul", True)], "Asia/Tokyo"),
        # ALTER DATABASE outranks ALTER ROLE ALL.
        ([("", False, "Asia/Tokyo", False), ("", True, "Europe/Paris", False),
          ("migr", True, "Asia/Seoul", True)], "Europe/Paris"),
        # Nothing else names it: not read.
        ([("migr", True, "Asia/Seoul", True)], None),
    ],
)
def test_alter_role_in_database_is_the_logins_own_too(monkeypatch, defaults, expected) -> None:  # noqa: ANN001
    settings = _settings(monkeypatch, ("Asia/Seoul", "database user"), defaults)
    assert settings.get("TimeZone") == expected
    assert settings["TimeZone (ALTER ROLE migr IN DATABASE, the migration user)"] == "Asia/Seoul"


@pytest.mark.parametrize(
    ("read", "expected"),
    [(("Asia/Tokyo", "global"), "Asia/Tokyo"), (("Europe/Paris", "database"), "Europe/Paris"),
     (("Asia/Seoul", "configuration file"), "Asia/Seoul")],
)
def test_a_default_that_is_not_the_logins_own_stays_as_read(monkeypatch, read, expected) -> None:  # noqa: ANN001
    settings = _settings(monkeypatch, read, [("", True, "UTC", False)] if read[1] != "database" else [])
    assert settings["TimeZone"] == expected


def test_the_defaults_read_marks_the_logins_entries() -> None:
    assert "COALESCE(r.rolname = session_user, false) AS is_login" in (
        pg_dialect_module._PG_TIMEZONE_DEFAULTS_SQL
    )
    connection = _PgConnection(
        [("", True, "UTC", False), ("app", False, "Asia/Tokyo", False), _LOGIN_SEOUL,
         ("app", True, "Europe/Paris")]  # a row of the earlier three-column shape still reads
    )
    assert pg_dialect_module.pg_timezone_defaults(connection) == {
        "TimeZone (ALTER DATABASE)": "UTC",
        "TimeZone (ALTER ROLE app)": "Asia/Tokyo",
        "TimeZone (ALTER ROLE migr, the migration user)": "Asia/Seoul",
        "TimeZone (ALTER ROLE app IN DATABASE)": "Europe/Paris",
    }


def test_a_utc_default_with_a_role_of_another_zone_is_an_advisory_note() -> None:
    settings = {"TimeZone": "UTC", "TimeZone (ALTER ROLE app)": "Asia/Seoul",
                "TimeZone (ALTER ROLE migr, the migration user)": "UTC"}
    (item,) = check_postgres_time_zone(SourceInventory(tables=[_clocks()], server_settings=settings))
    assert item.classification is Classification.AUTO and item.effort is None
    assert item.risk == (
        "The source's default time zone is UTC (TimeZone 'UTC'), so Schema Conversion writes "
        "clock defaults in UTC. Some roles have a TimeZone default of their own (ALTER ROLE "
        "app 'Asia/Seoul', the migration user's ALTER ROLE migr 'UTC'): an application that "
        "connects as one of them gets that zone instead."
    )
    assert "If the application connects as one of those roles" in item.recommendation
    # Every role in UTC too: nothing to say.
    quiet = {"TimeZone": "UTC", "TimeZone (ALTER ROLE app)": "Etc/UTC"}
    assert check_postgres_time_zone(SourceInventory(tables=[_clocks()], server_settings=quiet)) == []


def test_the_settings_caption_says_the_logins_own_is_marked() -> None:
    from dsql_migrator.ui.evaluation import source_settings_caption

    caption = source_settings_caption(_PG)
    assert "when its role has no TimeZone default of its own" in caption
    assert "the migration user's own is marked, and is not taken for the source's" in caption


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_CLAIMS = {
    "en": ("when its role has no `TimeZone` default of its own", "*the migration user*",
           "or one the migration user's own `ALTER ROLE` default hides",
           "gets an advisory item that names it"),
    "ko": ("역할에 자기 `TimeZone` 기본값이 없는 새 세션", "*the migration user*",
           "마이그레이션 사용자 자신의 `ALTER ROLE` 기본값이 가린 `TimeZone`",
           "그 역할을 짚는 권고 항목이 나옵니다"),
    "ja": ("ロールに独自の `TimeZone` 既定値がない新しいセッション", "*the migration user*",
           "移行ユーザー自身の `ALTER ROLE` の既定値に隠された `TimeZone`",
           "そのロールを示すアドバイザリー項目が出ます"),
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_how_the_default_is_read(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "02-evaluation-and-schema-conversion.md")
        .read_text(encoding="utf-8").split()
    )
    evaluation = text.split("## 2.1", 1)[1].split("## 2.2", 1)[0]
    for claim in _CLAIMS[language]:
        assert " ".join(claim.split()) in evaluation, (language, claim)
