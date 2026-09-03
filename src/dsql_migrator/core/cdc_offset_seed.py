# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Build the Kafka Connect ``connect-offsets`` record that seeds a Debezium
MySQL source connector to resume streaming from a Full Load watermark.

Why this exists
---------------
The gapless Full Load -> CDC handoff (Property 11) requires the Debezium MySQL
source connector to begin streaming from *exactly* the binlog/GTID position the
bulk snapshot ended at (``snapshot.mode=schema_only`` means Debezium does NOT
take its own snapshot). Debezium reads its start position from the compacted
``connect-offsets`` Kafka topic, keyed by ``[connector_name, source_partition]``.
The cdc-stack itself does not write that record, so without seeding the connector
starts from the *current* binlog at creation time -- losing every change between
the snapshot point and connector creation. This module builds the exact record;
the actual produce to MSK runs in-VPC via the offset-seeder Lambda the cdc-stack
deploys (``deploy/cdc-stack/lambda/seeder.py``).

This module is pure (no Kafka I/O) so the fragile record format is unit-tested.

Format (Debezium MySQL connector 2.7.4.Final; validated for that connector)
----------------------------------------------------------------
Version note: the MSK Connect *runtime* is Kafka Connect 3.7.x, but the offset
record format is dictated by the **Debezium MySQL connector** plugin version,
which is **2.7.4.Final** here (deployed custom plugin
``dsql-cdc-stack-debezium-mysql-v4``, built from
``debezium-connector-mysql-2.7.4.Final``). The Connect 3.7.x runtime does not
change the connector's offset schema. (The read-modify-write mode below further
insulates against any version drift by mirroring the live connector's record.)
- **Source partition** (the Kafka Connect offset *key*'s second element):
  ``{"server": "<topic.prefix>"}``.
- **Offset key** as stored on ``connect-offsets``:
  ``["<connector_name>", {"server": "<topic.prefix>"}]``.
- **Source offset** (the *value*): a streaming (non-snapshot) MySQL offset:
  ``{"ts_sec": <unix>, "file": "<binlog file>", "pos": <int>, "gtids":
  "<gtid set>", "row": 0, "server_id": 0, "event": 0}``. ``gtids`` is omitted
  when the source has no GTID set (file:pos resume only).

Because the precise value schema is connector-version sensitive, the builder
supports a **read-modify-write** mode: pass the connector's *existing* offset
value as ``base_offset`` and only the position fields (``ts_sec``/``file``/
``pos``/``gtids``) are overridden, preserving every other key the live connector
uses. Prefer that over building from scratch when an existing offset is readable.
"""
from __future__ import annotations

import json
from typing import Any, Mapping, Optional

from dsql_migrator.core.watermark import Watermark


class OffsetSeedError(ValueError):
    """Raised when a watermark lacks the coordinates needed to seed an offset."""


def build_source_partition(topic_prefix: str) -> dict[str, str]:
    """Return the Debezium MySQL source partition for ``topic.prefix``.

    For Debezium MySQL 2.x the source partition (the connector's logical-server
    identity) is ``{"server": "<topic.prefix>"}``.
    """
    if not topic_prefix:
        raise OffsetSeedError("topic_prefix must be a non-empty logical server name")
    return {"server": topic_prefix}


def build_source_offset(
    watermark: Watermark,
    *,
    base_offset: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Build the Debezium MySQL streaming source offset from ``watermark``.

    Requires binlog ``file``+``pos`` (GTID alone is not sufficient for the MySQL
    connector's offset value, which is binlog-coordinate based; ``gtids`` is
    added when present so GTID-aware resume is used). When ``base_offset`` is
    given (the live connector's current offset value read from ``connect-offsets``)
    its shape is preserved and only ``ts_sec``/``file``/``pos``/``gtids`` (plus the
    position-relative ``row``/``event`` counters) are overridden -- the safest seed
    because it mirrors exactly what the connector writes for its own version.

    **Why ``row``/``event`` are reset.** A watermark position (from ``SHOW MASTER
    STATUS``) is always an event boundary, so the resume point is ``row=0``/
    ``event=0``. The live ``base_offset``, however, may carry a NON-zero ``row``/
    ``event`` (the connector stopped mid multi-row event) that is only meaningful at
    ITS old ``pos``. Copying those forward onto a DIFFERENT (watermark) position
    would make Debezium skip that many rows/events in the first event after the
    resume point -- rows that are in neither Full Load nor CDC (silent loss). So
    when file/pos is overridden the counters are reset to the boundary.
    """
    if watermark.binlog_file is None or watermark.binlog_position is None:
        raise OffsetSeedError(
            "watermark has no binlog file:position; cannot build a MySQL "
            "streaming offset (enable binlog/GTID on the source and re-capture)"
        )
    ts_sec = int(watermark.snapshot_timestamp.timestamp())
    offset: dict[str, Any] = dict(base_offset) if base_offset else {
        "row": 0, "server_id": 0, "event": 0,
    }
    offset["ts_sec"] = ts_sec
    offset["file"] = watermark.binlog_file
    offset["pos"] = int(watermark.binlog_position)
    # The new position is an event boundary; reset the position-relative skip
    # counters so a stale row/event copied from base_offset can't make Debezium
    # skip rows/events at the resume point (server_id is source identity, not a
    # skip counter, so it is left as-is).
    offset["row"] = 0
    offset["event"] = 0
    if watermark.gtid_executed:
        offset["gtids"] = watermark.gtid_executed
    else:
        # No GTID on the source -> resume purely by file:pos; never leave a stale
        # gtids from a copied base_offset, which would mis-seed the connector.
        offset.pop("gtids", None)
    # A seeded *streaming* start must not look like an in-progress snapshot.
    offset.pop("snapshot", None)
    offset.pop("snapshot_completed", None)
    return offset


def build_connect_offset_record(
    connector_name: str,
    topic_prefix: str,
    watermark: Watermark,
    *,
    base_offset: Optional[Mapping[str, Any]] = None,
) -> tuple[str, str]:
    """Return ``(key_json, value_json)`` for the ``connect-offsets`` record.

    The key is ``["<connector_name>", {"server": "<topic.prefix>"}]`` and the
    value is the streaming source offset (see :func:`build_source_offset`). Both
    are compact JSON strings ready to be produced to the compacted
    ``connect-offsets`` topic by the in-VPC seeder. ``json.dumps`` uses sorted
    keys so the output is deterministic (stable for tests / idempotent re-seeds).
    """
    if not connector_name:
        raise OffsetSeedError("connector_name must be non-empty")
    key = [connector_name, build_source_partition(topic_prefix)]
    value = build_source_offset(watermark, base_offset=base_offset)
    return (
        json.dumps(key, sort_keys=True, separators=(",", ":")),
        json.dumps(value, sort_keys=True, separators=(",", ":")),
    )


__all__ = [
    "OffsetSeedError",
    "build_source_partition",
    "build_source_offset",
    "build_connect_offset_record",
]
