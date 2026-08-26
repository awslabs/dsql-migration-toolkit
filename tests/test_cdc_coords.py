# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for CDC start-coordinate validators/parsers (pure functions).

These guard the manual start-position input: GTID and binlog file:position
strings the operator types when seeding CDC without a Full Load watermark.
Validation is advisory (returns a message, never raises) and liberal about
legal-but-unusual MySQL GTID shapes.
"""

from __future__ import annotations

from dsql_migrator.core.cdc_coords import (
    parse_binlog_coordinate,
    validate_binlog_file,
    validate_gtid,
)

_UUID = "3E11FA47-71CA-11E1-9E33-C80AA9429562"


# ---------------------------------------------------------------------------
# validate_gtid
# ---------------------------------------------------------------------------


def test_gtid_single_uuid_single_interval_is_valid() -> None:
    assert validate_gtid(f"{_UUID}:1-100") is None


def test_gtid_single_transaction_is_valid() -> None:
    assert validate_gtid(f"{_UUID}:5") is None


def test_gtid_multiple_intervals_is_valid() -> None:
    assert validate_gtid(f"{_UUID}:1-5:10:20-30") is None


def test_gtid_multi_source_set_is_valid() -> None:
    other = "11111111-2222-3333-4444-555555555555"
    assert validate_gtid(f"{_UUID}:1-100,{other}:1-50") is None


def test_gtid_lowercase_uuid_is_valid() -> None:
    assert validate_gtid(f"{_UUID.lower()}:1-100") is None


def test_gtid_with_trailing_newline_and_spaces_is_valid() -> None:
    assert validate_gtid(f"  {_UUID}:1-100\n") is None


def test_gtid_with_block_comment_is_stripped_and_valid() -> None:
    # gtid_purged output is sometimes pasted with a /* ... */ comment.
    assert validate_gtid(f"/* comment */{_UUID}:1-100") is None


def test_gtid_empty_returns_message() -> None:
    assert validate_gtid("") is not None
    assert validate_gtid("   ") is not None


def test_gtid_missing_interval_is_invalid() -> None:
    assert validate_gtid(_UUID) is not None


def test_gtid_malformed_uuid_is_invalid() -> None:
    assert validate_gtid("not-a-uuid:1-100") is not None


def test_gtid_garbage_is_invalid() -> None:
    assert validate_gtid("hello world") is not None


# ---------------------------------------------------------------------------
# validate_binlog_file
# ---------------------------------------------------------------------------


def test_binlog_file_standard_name_is_valid() -> None:
    assert validate_binlog_file("mysql-bin.000123") is None


def test_binlog_file_other_base_is_valid() -> None:
    assert validate_binlog_file("binlog.000001") is None


def test_binlog_file_empty_returns_message() -> None:
    assert validate_binlog_file("") is not None


def test_binlog_file_without_numeric_suffix_is_invalid() -> None:
    assert validate_binlog_file("mysql-bin") is not None


def test_binlog_file_with_whitespace_is_invalid() -> None:
    assert validate_binlog_file("mysql bin.000123") is not None


# ---------------------------------------------------------------------------
# parse_binlog_coordinate
# ---------------------------------------------------------------------------


def test_parse_coordinate_valid() -> None:
    assert parse_binlog_coordinate("mysql-bin.000123:45678") == ("mysql-bin.000123", 45678)


def test_parse_coordinate_strips_whitespace() -> None:
    assert parse_binlog_coordinate("  mysql-bin.000001:0  ") == ("mysql-bin.000001", 0)


def test_parse_coordinate_missing_position_is_none() -> None:
    assert parse_binlog_coordinate("mysql-bin.000123") is None


def test_parse_coordinate_blank_is_none() -> None:
    assert parse_binlog_coordinate("") is None


def test_parse_coordinate_non_numeric_position_is_none() -> None:
    assert parse_binlog_coordinate("mysql-bin.000123:abc") is None


def test_parse_coordinate_negative_position_is_none() -> None:
    # "-5" is not all-digits, so it is rejected.
    assert parse_binlog_coordinate("mysql-bin.000123:-5") is None


def test_parse_coordinate_bad_file_is_none() -> None:
    assert parse_binlog_coordinate("not a file:100") is None


# ---------------------------------------------------------------------------
# validate_wal_lsn (PostgreSQL CDC start position)
# ---------------------------------------------------------------------------


def test_validate_wal_lsn_accepts_a_well_formed_lsn() -> None:
    from dsql_migrator.core.cdc_coords import validate_wal_lsn

    assert validate_wal_lsn("3/AF012B8") is None
    assert validate_wal_lsn("0/16B3748") is None
    assert validate_wal_lsn("  9/AABBCC  ") is None  # surrounding whitespace tolerated


def test_validate_wal_lsn_rejects_malformed_and_blank() -> None:
    from dsql_migrator.core.cdc_coords import validate_wal_lsn

    assert validate_wal_lsn("") is not None
    assert validate_wal_lsn("not-an-lsn") is not None
    assert validate_wal_lsn("3AF012B8") is not None  # missing the '/'
    assert validate_wal_lsn("mysql-bin.000123:100") is not None  # a binlog coord
