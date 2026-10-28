# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G165 (PG-10, CUX-9): a PostgreSQL fixed-offset TimeZone with minutes, and what is unknown.

PostgreSQL shows a fixed offset with minutes as '<+05:30>-05:30' (SET TIME ZONE 5.5, or
INTERVAL '+05:30' HOUR TO MINUTE; '<-03:30>+03:30' for -3.5), and the POSIX pattern did not
allow ':' inside the angle brackets, so such a source (India, Nepal, Newfoundland, ...) was
graded "unknown" and converted as UTC -- its clock defaults 5 h 30 min off -- while the
whole-hour '<+09>-09' was written. The unknown-zone advisory also gave the wrong reason ("a
POSIX rule with daylight saving"), and the manual (2.1) said a TimeZone "with daylight-saving
rules" cannot be told although every named zone with daylight saving (America/New_York) is
written: only a POSIX rule with a daylight-saving part, or localtime, is not. Verified on
PostgreSQL 17.11 (throwaway local container): the strings above are what SHOW TimeZone gives,
and in such a session LOCALTIMESTAMP equals (now() AT TIME ZONE 'UTC') + INTERVAL '+05:30'.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import check_postgres_time_zone
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.source_time_zone import resolve_source_zone

_PG = SourceType.POSTGRES


@pytest.mark.parametrize(
    ("shown", "offset"),
    [("<+05:30>-05:30", "+05:30"), ("<+09:30>-09:30", "+09:30"), ("<-03:30>+03:30", "-03:30"),
     ("<+05:45>-05:45", "+05:45")],
)
def test_a_minute_offset_is_read_with_its_iso_sign(shown: str, offset: str) -> None:
    zone = resolve_source_zone({"TimeZone": shown}, _PG)
    assert (zone.kind, zone.zone) == ("offset", offset)


@pytest.mark.parametrize("shown", ["EST5EDT,M3.2.0,M11.1.0", "localtime", "<+05:30:15>-05:30:15"])
def test_what_cannot_be_told_stays_unknown(shown: str) -> None:
    assert resolve_source_zone({"TimeZone": shown}, _PG).kind == "unknown"


def test_a_named_zone_with_daylight_saving_is_written() -> None:
    assert resolve_source_zone({"TimeZone": "America/New_York"}, _PG).kind == "named"


def _clocks() -> TableDef:
    return TableDef(
        name="app.events",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="at", mysql_type="timestamp without time zone", default="now()"),
        ],
        primary_key=["id"],
    )


def test_the_conversion_writes_a_minute_offset() -> None:
    inventory = SourceInventory(tables=[_clocks()], server_settings={"TimeZone": "<+05:30>-05:30"})
    (table,) = SchemaConverter(source_type=_PG).convert(inventory).tables
    assert "\"at\" TIMESTAMP DEFAULT ((now() AT TIME ZONE 'UTC') + INTERVAL '+05:30')" in (
        " ".join(table.target_ddl.split())
    )
    (item,) = check_postgres_time_zone(inventory)
    assert "SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE" in item.recommendation


def test_the_unknown_zone_note_no_longer_asserts_daylight_saving() -> None:
    inventory = SourceInventory(tables=[_clocks()], server_settings={"TimeZone": "localtime"})
    (item,) = check_postgres_time_zone(inventory)
    assert (
        "(such as a POSIX rule with a daylight-saving part, or localtime, the server's local "
        "zone)" in item.risk
    )
    assert "(a POSIX rule with daylight saving, the server's local zone)" not in item.risk


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_SAID = {
    # Chapter 2 is a user guide (2026-10-25): it states the unknown-zone behaviour only.
    "en": ("When the zone cannot be told, an advisory item says so and the conversion assumes UTC",
           "`TimeZone` with daylight-saving rules"),
    "ko": ("시간대를 판별할 수 없으면 권고 항목이 그렇다고 알리며, 변환은 UTC를 가정합니다",
           "서머타임 규칙이 있는 PostgreSQL `TimeZone`"),
    "ja": ("タイムゾーンを判別できない場合は、アドバイザリー項目がその旨を伝え、変換は UTC を前提とします",
           "夏時間の規則を持つ PostgreSQL の `TimeZone`"),
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_which_postgresql_zones_cannot_be_told(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "02-evaluation-and-schema-conversion.md")
        .read_text(encoding="utf-8").split()
    )
    said, gone = _SAID[language]
    assert said in text, language
    assert gone not in text, language
