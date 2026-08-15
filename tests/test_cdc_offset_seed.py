# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the Debezium MySQL connect-offsets seed record builder.

Covers the fragile offset format: source partition, streaming offset (with and
without GTID), read-modify-write over a live base offset, the assembled
connect-offsets key/value pair, and the no-coordinates error.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from dsql_migrator.core.cdc_offset_seed import (
    OffsetSeedError,
    build_connect_offset_record,
    build_source_offset,
    build_source_partition,
)
from dsql_migrator.core.watermark import Watermark

_TS = datetime(2026, 6, 26, 12, 0, 0, tzinfo=timezone.utc)


def _wm(*, gtid=None, file="mysql-bin.000123", pos=4567) -> Watermark:
    return Watermark(
        binlog_file=file,
        binlog_position=pos,
        gtid_executed=gtid,
        server_uuid="abc-uuid",
        snapshot_timestamp=_TS,
    )


def test_source_partition_is_server_keyed_on_topic_prefix() -> None:
    assert build_source_partition("dsqlcdc") == {"server": "dsqlcdc"}


def test_source_partition_rejects_empty_prefix() -> None:
    with pytest.raises(OffsetSeedError):
        build_source_partition("")


def test_source_offset_with_gtid_sets_position_and_gtids() -> None:
    off = build_source_offset(_wm(gtid="abc-uuid:1-100"))
    assert off["file"] == "mysql-bin.000123"
    assert off["pos"] == 4567
    assert off["gtids"] == "abc-uuid:1-100"
    assert off["ts_sec"] == int(_TS.timestamp())
    # default streaming-offset scaffolding present
    assert off["row"] == 0 and off["server_id"] == 0 and off["event"] == 0


def test_source_offset_without_gtid_omits_gtids() -> None:
    off = build_source_offset(_wm(gtid=None))
    assert "gtids" not in off
    assert off["file"] == "mysql-bin.000123" and off["pos"] == 4567


def test_source_offset_read_modify_write_preserves_base_and_overrides_position() -> None:
    # The live connector's existing offset value (its exact shape) is preserved;
    # ts_sec/file/pos/gtids are overridden and snapshot markers stripped. The
    # position-relative row/event counters are RESET to the event boundary (see
    # test_source_offset_rmw_resets_row_event_at_new_position).
    base = {
        "transaction_id": None, "ts_sec": 111, "file": "mysql-bin.000001",
        "pos": 10, "gtids": "old-uuid:1-5", "row": 2, "server_id": 99,
        "event": 7, "snapshot": "true", "snapshot_completed": True,
    }
    off = build_source_offset(_wm(gtid="abc-uuid:1-100"), base_offset=base)
    assert off["transaction_id"] is None       # preserved
    assert off["server_id"] == 99              # preserved (source identity, not a counter)
    assert off["file"] == "mysql-bin.000123" and off["pos"] == 4567  # overridden
    assert off["gtids"] == "abc-uuid:1-100"     # overridden
    assert off["ts_sec"] == int(_TS.timestamp())
    assert "snapshot" not in off and "snapshot_completed" not in off  # stripped


def test_source_offset_rmw_resets_row_event_at_new_position() -> None:
    # A watermark position is an event boundary (row=0/event=0). A live base_offset
    # that stopped mid multi-row event carries a non-zero row/event meaningful only
    # at its OLD pos; carrying it onto the new (watermark) pos would make Debezium
    # skip that many rows/events at the resume point -> silent row loss. So both
    # counters must be reset when file/pos is overridden.
    base = {"file": "mysql-bin.000001", "pos": 10, "row": 5, "event": 7, "server_id": 99}
    off = build_source_offset(_wm(gtid=None), base_offset=base)
    assert off["file"] == "mysql-bin.000123" and off["pos"] == 4567  # moved to watermark
    assert off["row"] == 0 and off["event"] == 0  # reset to the boundary
    assert off["server_id"] == 99  # source identity preserved


def test_source_offset_rmw_drops_stale_gtids_when_source_has_none() -> None:
    base = {"file": "x", "pos": 1, "gtids": "old:1-9", "server_id": 1}
    off = build_source_offset(_wm(gtid=None), base_offset=base)
    assert "gtids" not in off  # no GTID on source -> must not keep stale gtids


def test_source_offset_requires_binlog_coordinates() -> None:
    wm = Watermark(snapshot_timestamp=_TS)  # no binlog file/pos
    with pytest.raises(OffsetSeedError):
        build_source_offset(wm)


def test_connect_offset_record_key_and_value_are_deterministic_json() -> None:
    key_json, value_json = build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source", "dsqlcdc", _wm(gtid="abc-uuid:1-100")
    )
    key = json.loads(key_json)
    assert key == ["mysql-dsql-cdc-stack-debezium-source", {"server": "dsqlcdc"}]
    value = json.loads(value_json)
    assert value["file"] == "mysql-bin.000123" and value["pos"] == 4567
    # deterministic (sorted keys) so re-seeds are byte-identical
    again, _ = build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source", "dsqlcdc", _wm(gtid="abc-uuid:1-100")
    )
    assert again == key_json


def test_connect_offset_record_rejects_empty_connector_name() -> None:
    with pytest.raises(OffsetSeedError):
        build_connect_offset_record("", "dsqlcdc", _wm(gtid="g:1-2"))
