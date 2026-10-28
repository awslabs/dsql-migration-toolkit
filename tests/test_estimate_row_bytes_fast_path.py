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
