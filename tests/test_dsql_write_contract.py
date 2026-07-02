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
from datetime import datetime, time, timedelta, timezone
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
    """Run one value through ValueConverter for a single-column table."""
    table = TableDef(
        name="t",
        columns=[ColumnDef(name="val", mysql_type=mysql_type)],
        primary_key=["id"],
    )
    return ValueConverter(table).convert_value("val", value)


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


def test_none_passes_through_for_every_kind() -> None:
    for mysql_type, _kind, _schema, _note in DSQL_WRITE_CONTRACT_CASES:
        assert _convert(mysql_type, None) is None
