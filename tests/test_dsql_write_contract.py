# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DSQL write-contract parity tests — the Python (bulk-loader) half.

Both write paths to Aurora DSQL must encode each MySQL source type identically:
the Python bulk loader (Full Load, ``ValueConverter``) and the Java sink (CDC,
``DebeziumTypeConverter``). This module asserts the bulk loader produces the
canonical encoding declared in the shared contract
(``tests/fixtures/dsql_write_contract.json`` ↔ ``converter.DSQL_WRITE_CONTRACT_CASES``);
the Java half (``DebeziumTypeConverterTest``) loads the SAME fixture and asserts
the sink agrees. One artifact governs both languages, so a divergence fails a test
rather than silently corrupting boundary rows at cutover.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from dsql_migrator.core.converter import DSQL_WRITE_CONTRACT_CASES
from dsql_migrator.core.exporter import ValueConverter, _target_kind
from dsql_migrator.core.models import ColumnDef, TableDef


def _time_timedelta(seconds: int) -> timedelta:
    """A MySQL TIME value as the driver returns it (timedelta)."""
    return timedelta(seconds=seconds)


def _time_obj(h: int, m: int, s: int) -> time:
    return time(hour=h, minute=m, second=s)

_FIXTURE = Path(__file__).parent / "fixtures" / "dsql_write_contract.json"
_JAVA_COPY = (
    Path(__file__).parent.parent
    / "connectors"
    / "dsql-sink"
    / "src"
    / "test"
    / "resources"
    / "dsql_write_contract.json"
)


def _convert(mysql_type: str, value: object) -> object:
    """Run one value through ValueConverter for a single-column table: ``convert_value``, and
    the Full Load read's own path (``row_converter``, a page's row by position), which must
    give the same value of the same type."""
    table = TableDef(
        name="t",
        columns=[ColumnDef(name="val", mysql_type=mysql_type)],
        primary_key=["id"],
    )
    expected = ValueConverter(table).convert_value("val", value)
    got = ValueConverter(table).row_converter(("val",))((value,))["val"]
    assert (type(got), repr(got)) == (type(expected), repr(expected)), mysql_type
    return got


# ---------------------------------------------------------------------------
# The contract table is internally consistent with the live mapping
# ---------------------------------------------------------------------------


def test_contract_dsql_kind_matches_live_mapping() -> None:
    # Every contract entry's declared dsql_kind must equal what the shared type
    # mapping actually produces, so the contract can't drift from converter.py.
    for mysql_type, dsql_kind, _schema, _note in DSQL_WRITE_CONTRACT_CASES:
        assert _target_kind(mysql_type) == dsql_kind, mysql_type


# ---------------------------------------------------------------------------
# The shared JSON fixture is present, valid, and mirrors the contract table
# ---------------------------------------------------------------------------


def test_fixture_exists_and_covers_the_contract() -> None:
    data = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    cases = data["cases"]
    fixture_kinds = {c["mysql_type"]: c["dsql_kind"] for c in cases}
    # Every fixture case's dsql_kind must agree with the live mapping.
    for case in cases:
        assert _target_kind(case["mysql_type"]) == case["dsql_kind"], case["name"]
    # The fixture's boundary types are a subset of the contract table's types.
    contract_types = {row[0] for row in DSQL_WRITE_CONTRACT_CASES}
    assert set(fixture_kinds).issubset(contract_types)


def test_java_resource_copy_matches_fixture() -> None:
    # The Java parity test loads its own copy from src/test/resources; it must be
    # byte-identical to the canonical fixture so both languages assert one truth.
    assert _JAVA_COPY.exists(), "Java test-resource copy of the contract is missing"
    assert _JAVA_COPY.read_text(encoding="utf-8") == _FIXTURE.read_text(
        encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# ValueConverter produces the canonical DSQL encoding (golden cases)
# ---------------------------------------------------------------------------

# (mysql_type, input value from MySQL, expected stored value). These are the
# Python-path expectations that correspond to the shared fixture's cases.
_GOLDEN: tuple[tuple[str, object, object], ...] = (
    # DATETIME / DATETIME(6) -> DSQL timestamp WITHOUT TIME ZONE: normalized to UTC
    # then returned NAIVE (tzinfo dropped) so the stored wall-clock is independent of
    # the DSQL session TimeZone (a tz-aware bind would shift to the session zone).
    ("DATETIME", datetime(2024, 1, 1, 0, 0, 0),
     datetime(2024, 1, 1, 0, 0, 0)),
    ("DATETIME(6)", datetime(2024, 1, 1, 0, 0, 0, 123),
     datetime(2024, 1, 1, 0, 0, 0, 123)),
    # Before the Gregorian reform (1582-10-15) the value is kept as it is: Python's datetime
    # is proleptic, so psycopg sends '1000-01-01 00:00:00' (G126: the CDC sink now sends the
    # same text; it used to send these days off).
    ("DATETIME", datetime(1, 1, 1), datetime(1, 1, 1)),
    ("DATETIME", datetime(1000, 1, 1), datetime(1000, 1, 1)),
    ("DATETIME(6)", datetime(1582, 10, 4, 23, 59, 59, 123456),
     datetime(1582, 10, 4, 23, 59, 59, 123456)),
    # DATE -> date: the datetime.date passes through, in the same proleptic calendar -- the
    # ten days the Gregorian reform skipped included (G133: the CDC sink sent those ten days
    # later).
    ("DATE", date(1582, 10, 10), date(1582, 10, 10)),
    ("DATE", date(1000, 1, 1), date(1000, 1, 1)),
    # TIMESTAMP -> timestamptz: an aware value is normalized to UTC (stays tz-aware,
    # the instant is correct regardless of session TimeZone).
    ("TIMESTAMP", datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
     datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)),
    # TINYINT(1): MySQL boolean convention.
    ("TINYINT(1)", 1, True),
    ("TINYINT(1)", 0, False),
    # BLOB / LONGBLOB: bytes preserved.
    ("BLOB", b"\x01\x02\x03", b"\x01\x02\x03"),
    ("LONGBLOB", bytearray(b"\xde\xad"), b"\xde\xad"),
    # GEOMETRY: WKB bytes (Full Load ST_AsBinary / Debezium .wkb) preserved as bytea.
    ("GEOMETRY", b"\x01\x01\x00\x00\x00", b"\x01\x01\x00\x00\x00"),
    # BIGINT UNSIGNED: Python int is arbitrary precision (no overflow).
    ("BIGINT UNSIGNED", 18446744073709551615, 18446744073709551615),
    # DECIMAL: exact decimal passes through.
    ("DECIMAL(10,4)", Decimal("1234.5678"), Decimal("1234.5678")),
    # JSON / ENUM: string passes through.
    ("JSON", '{"k": 1}', '{"k": 1}'),
    ("ENUM('a','b')", "a", "a"),
    # Integer family + unsigned: plain int passthrough (no overflow in Python).
    ("INT", 2147483647, 2147483647),
    ("INT UNSIGNED", 4294967295, 4294967295),
    ("TINYINT UNSIGNED", 255, 255),
    ("MEDIUMINT", -8388608, -8388608),
    # BIT: driver big-endian bytes -> unsigned int.
    ("BIT(8)", b"\xdb", 219),
    # YEAR: PyMySQL returns an int year -> passthrough.
    ("YEAR", 2024, 2024),
    # TIME: driver timedelta (in-range) -> datetime.time.
    ("TIME", _time_timedelta(19479), _time_obj(5, 24, 39)),
    # CHAR/VARCHAR/TEXT: strings pass through.
    ("VARCHAR(255)", "hello", "hello"),
    # BINARY/VARBINARY: bytes preserved (kind bytea).
    ("BINARY(16)", b"\x00\xff", b"\x00\xff"),
)


@pytest.mark.parametrize("mysql_type,value,expected", _GOLDEN)
def test_value_converter_matches_contract(mysql_type, value, expected) -> None:
    assert _convert(mysql_type, value) == expected


def test_datetime6_microseconds_preserved() -> None:
    # The microsecond component must survive (the sink converts micros->nanos).
    # DATETIME(6) -> naive UTC (timestamp without tz); micros preserved.
    out = _convert("DATETIME(6)", datetime(2024, 1, 1, 0, 0, 0, 123456))
    assert out == datetime(2024, 1, 1, 0, 0, 0, 123456)
    assert out.tzinfo is None
    assert out.microsecond == 123456


def test_fixture_timestamps_are_one_text_on_both_paths() -> None:
    # G126: each fixture timestamp case pins the TEXT both paths send for one Debezium number.
    # Here: the number is the proleptic epoch of that text, and the bulk loader keeps the
    # datetime (psycopg sends its ISO text). DebeziumTypeConverterTest asserts the sink's value
    # renders to the same text through pgjdbc -- before G126 a java.sql.Timestamp, rendered in
    # the Julian calendar before 1582-10-15, sent 1000-01-01 as 0999-12-27.
    data = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    epoch = datetime(1970, 1, 1)
    checked = []
    for case in data["cases"]:
        text = case.get("expected_dsql_text")
        if text is None or case["dsql_kind"] != "timestamp":
            continue
        wall = datetime.fromisoformat(text)
        unit = (
            timedelta(milliseconds=1)
            if case["debezium_schema_name"] == "io.debezium.time.Timestamp"
            else timedelta(microseconds=1)
        )
        assert (wall - epoch) // unit == case["debezium_encoded_long"], case["name"]
        out = _convert(case["mysql_type"], wall)
        assert out == wall and out.isoformat(sep=" ") == text, case["name"]
        checked.append(case["name"])
    assert {"datetime_year_0001", "datetime6_year_1000", "datetime6_1582_10_04"} <= set(checked)


def test_fixture_dates_are_one_text_on_both_paths() -> None:
    # G133: each fixture date case pins the TEXT both paths send for one Debezium epoch day.
    # Here: the day is the proleptic epoch day of that text, and the bulk loader keeps the
    # date (psycopg sends its ISO text). DebeziumTypeConverterTest asserts the sink's LocalDate
    # renders to the same text through pgjdbc -- before G133 a java.sql.Date, rendered through a
    # GregorianCalendar that skips 1582-10-05 .. 1582-10-14, sent 1582-10-10 as 1582-10-20.
    data = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    checked = []
    for case in data["cases"]:
        text = case.get("expected_dsql_text")
        if text is None or case["dsql_kind"] != "date":
            continue
        day = date.fromisoformat(text)
        assert (day - date(1970, 1, 1)).days == case["debezium_encoded_int"], case["name"]
        out = _convert(case["mysql_type"], day)
        assert out == day and out.isoformat() == text, case["name"]
        checked.append(case["name"])
    assert {
        "date_1582_10_05",
        "date_1582_10_10",
        "date_1582_10_14",
        "date_year_0001",
        "date_year_1000",
    } <= set(checked)


def test_the_manual_names_the_date_shift_of_an_earlier_sink() -> None:
    # G133, en / ko / ja alike: chapter 6's note on CDC infrastructure deployed by an earlier
    # version names the skipped days and the DATE example beside the pre-1582 timestamps.
    manual = Path(__file__).parent.parent / "docs" / "manual"
    for lang in ("en", "ko", "ja"):
        text = (manual / lang / "06-limitations.md").read_text(encoding="utf-8")
        for phrase in ("1582-10-05", "1582-10-14", "`1582-10-10`", "`1582-10-20`", "`0999-12-27`"):
            assert phrase in text, (lang, phrase)


# G135: beside "so does CDC from sink plugin v54", the PostgreSQL rows a Debezium snapshot copies
# are read through JDBC before the sink sees them -- the skipped days ten days later, a BC value
# without its era -- whatever the sink; Full load + CDC takes no snapshot.
_PG_SNAPSHOT_DATES = {
    "en": "On a PostgreSQL source, though, the rows a Debezium snapshot copies (every CDC only "
    "Start, a re-snapshot or a Manual start point; [§4.3](04-cdc-and-dsql-constraints.md)) are "
    "read through JDBC before they reach the sink, so with any sink plugin a `date` or `timestamp` "
    "from 1582-10-05 to 1582-10-14 lands ten days later and a BC one loses its era "
    "(`0044-03-15 BC` as `0044-03-15`), and a `timestamptz` before 1582-10-15 lands days off, read "
    "in the Julian calendar (`1000-01-01` five days later, `0001-01-01` two days earlier). Migrate "
    "such tables with **Full load + CDC** (its slot hand-off takes no snapshot) and validate them "
    "with CHECKSUM.",
    "ko": "다만 PostgreSQL 소스에서 Debezium 스냅샷이 복사하는 행(모든 CDC only Start, 다시 스냅샷, "
    "Manual 시작 지점; [§4.3](04-cdc-and-dsql-constraints.md))은 싱크에 닿기 전에 JDBC로 읽히므로, "
    "싱크 플러그인과 관계없이 1582-10-05~1582-10-14의 `date`나 `timestamp`는 열흘 뒤의 값으로 저장되고 "
    "BC 값은 기원전(BC) 표시를 잃으며(`0044-03-15 BC`가 `0044-03-15`로), 1582-10-15 이전의 "
    "`timestamptz`는 율리우스력으로 읽혀 며칠 어긋납니다(`1000-01-01`은 닷새 뒤, `0001-01-01`은 이틀 "
    "앞). 그런 테이블은 "
    "**Full load + CDC**로 마이그레이션하고(슬롯 핸드오프는 스냅샷을 하지 않음) CHECKSUM으로 "
    "검증하세요.",
    "ja": "ただし PostgreSQL ソースでは、Debezium のスナップショットがコピーする行(すべての CDC only の "
    "Start、再スナップショット、Manual の開始点。[§4.3](04-cdc-and-dsql-constraints.md))はシンクに届く前に "
    "JDBC で読み取られるため、シンクプラグインに関係なく、1582-10-05〜1582-10-14 の `date` や "
    "`timestamp` は 10 日後の値になり、BC の値は紀元前(BC)の表示を失います(`0044-03-15 BC` が "
    "`0044-03-15` に)。1582-10-15 より前の `timestamptz` はユリウス暦で読まれて数日ずれます"
    "(`1000-01-01` は 5 日後、`0001-01-01` は 2 日前)。そのようなテーブルは **Full load + CDC** で移行し"
    "(スロットの引き継ぎは"
    "スナップショットを取りません)、CHECKSUM で検証してください。",
}


@pytest.mark.parametrize("lang", ["en", "ko", "ja"])
def test_the_manual_names_the_postgresql_snapshot_date_shift(lang) -> None:
    # FAILS WITHOUT THE FIX: chapter 6 said CDC from sink plugin v54 stores a date as it is, with
    # no word of the rows a PostgreSQL snapshot reads -- for them a redeploy changes nothing.
    from tests.test_g25_docs import _has, _page

    assert _has(lang, _page(lang, "06-limitations.md"), _PG_SNAPSHOT_DATES[lang]), lang


def test_none_passes_through_for_every_kind() -> None:
    for mysql_type, _kind, _schema, _note in DSQL_WRITE_CONTRACT_CASES:
        assert _convert(mysql_type, None) is None
