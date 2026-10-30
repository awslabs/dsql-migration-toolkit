# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""The fast-path ``_estimate_row_bytes`` yields the same integer as the general rule (P-batch review)."""

from __future__ import annotations

import datetime as dt
import enum
import decimal

import pytest

from dsql_migrator.core.batched_import import _estimate_row_bytes


class _Str(str):
    pass


class _Int(int):
    pass


class _Color(enum.IntEnum):
    RED = 7


class _DateTime(dt.datetime):
    def __str__(self) -> str:
        return "a datetime"


class _Date(dt.date):
    def __str__(self) -> str:
        return "a date"


class _Decimal(decimal.Decimal):
    pass


def _reference(row) -> int:
    total = 0
    for value in row.values():
        if value is None:
            total += 1
        elif isinstance(value, (bytes, bytearray, memoryview)):
            total += len(value)
        elif isinstance(value, str):
            total += len(value.encode("utf-8"))
        else:
            total += len(str(value))
    return total


_UTC = dt.timezone.utc
_ROWS = [
    {"a": 1, "b": "plain", "c": None, "d": 2.5, "e": True, "f": b"\x00\x01"},
    {"a": "한국어", "b": "emoji \U0001F600", "c": "é", "d": ""},
    {"a": _Str("ascii"), "b": _Str("한"), "c": _Int(12345), "d": _Color.RED},
    {"a": dt.datetime(2026, 10, 28, 1, 2, 3, tzinfo=_UTC), "b": dt.date(2026, 1, 1),
     "c": dt.time(1, 2, 3), "d": dt.timedelta(seconds=5)},
    {"a": decimal.Decimal("NaN"), "b": decimal.Decimal("-Infinity"),
     "c": decimal.Decimal("1.2300"), "d": float("inf"), "e": float("nan")},
    {"a": bytearray(b"abc"), "b": memoryview(b"abcd"), "c": 10**40, "d": -3},
    {},
    # The converted MySQL temporal and numeric values (a naive datetime, a date, a Decimal and
    # a bool take exact-type paths): with and without microseconds, the year padded to four
    # digits, the range ends, a fold; an aware datetime and the subclasses take the general rule.
    {"a": dt.datetime(2024, 1, 2, 3, 4, 5), "b": dt.datetime(2024, 1, 2, 3, 4, 5, 1),
     "c": dt.datetime(2024, 1, 2, 3, 4, 5, 999999), "d": dt.datetime(1, 1, 1),
     "e": dt.datetime(999, 12, 31, 23, 59, 59), "f": dt.datetime.max, "g": dt.datetime.min,
     "h": dt.datetime(2024, 11, 3, 1, 30, fold=1)},
    {"a": dt.datetime(2024, 1, 2, tzinfo=dt.timezone(dt.timedelta(hours=5, minutes=30))),
     "b": dt.datetime(2024, 1, 2, 3, tzinfo=dt.timezone(dt.timedelta(seconds=-3601, microseconds=5))),
     "c": _DateTime(2024, 1, 2, 3, 4, 5, 6), "d": _DateTime(2024, 1, 2, tzinfo=_UTC)},
    {"a": dt.date(2024, 2, 29), "b": dt.date.min, "c": dt.date.max, "d": dt.date(999, 1, 1),
     "e": _Date(2024, 1, 1)},
    {"a": decimal.Decimal("123.456000"), "b": decimal.Decimal("-0"), "c": decimal.Decimal("1E+3"),
     "d": decimal.Decimal("-1.5E-7"), "e": decimal.Decimal("sNaN"), "f": _Decimal("9.99"),
     "g": decimal.Decimal(10) ** 40},
    {"a": True, "b": False, "c": dt.time(1, 2, 3, 4), "d": None, "e": 0.1},
]


@pytest.mark.parametrize("row", _ROWS)
def test_the_fast_path_matches_the_general_rule(row) -> None:
    assert _estimate_row_bytes(row) == _reference(row)


def test_a_lone_surrogate_fails_as_before() -> None:
    row = {"a": "x\ud800y"}
    with pytest.raises(UnicodeEncodeError):
        _reference(row)
    with pytest.raises(UnicodeEncodeError):
        _estimate_row_bytes(row)
