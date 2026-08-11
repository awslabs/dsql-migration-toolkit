# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Canonical, **pure** CDC Kafka-prep decision logic shared by the app and the
in-VPC offset-seeder Lambda.

Why this exists
---------------
The gapless Full Load -> CDC handoff needs three Kafka-side things done in the
VPC *before* the MSK Connect connectors are created (see
``deploy/cdc-stack/lambda/seeder.py``):

1. the compacted ``connect-offsets`` topic pre-created (MSK Connect requires a
   custom source-offset topic to exist with ``cleanup.policy=compact``);
2. the per-table sink topics + the DLQ topic pre-created with the right partition
   counts and a raised ``max.message.bytes`` (so source and sink deploy in
   parallel without the empty-partition-assignment race, and an oversized
   dead-letter doesn't ``RecordTooLarge`` the sink task);
3. the ``connect-offsets`` seed record produced from the Full Load watermark.

Today that logic lives in TWO places: the app builds the offset record
(:mod:`dsql_migrator.core.cdc_offset_seed`), while the Lambda re-vendors a
byte-compatible copy of the builders **and** owns the topic-shaping + no-clobber
decisions inline. This module is the single canonical **app-side** home for the
*pure* decisions so there is one place to test and reason about them:

* :func:`parse_partitions_map` / :func:`binlog_seq` / :func:`offset_already_at_or_past`
  — the three pure helpers that previously lived ONLY inside the Lambda;
* :class:`TopicSpec` / :func:`plan_topics` — the topic-shaping decision embedded
  in the Lambda's ``_ensure_*`` wrappers, lifted out as a testable plan;
* the offset-record builders — **re-exported** from
  :mod:`dsql_migrator.core.cdc_offset_seed` (not moved), keeping ONE canonical
  builder and zero churn to existing importers.

Purity contract (load-bearing)
-------------------------------
This module performs **no Kafka I/O** and imports **no** ``kafka`` /
``kafka.admin`` / ``kafka.errors`` / ``aws_msk_iam_sasl_signer`` — those are
shipped only inside the Lambda deployment zip and are deliberately NOT app
dependencies. It imports only the standard library plus the (equally pure)
``cdc_offset_seed`` module, so the app can import it with zero new dependencies.
The Lambda keeps its own byte-identical vendored copies of these functions; an
output-equivalence test suite (``tests/test_offset_seeder_lambda.py``) guards the
two copies against drift in either direction.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Sequence

# Re-export the canonical offset-record builders (kept in cdc_offset_seed so
# existing importers/tests are undisturbed; this module is the single import
# surface for pure CDC-Kafka-prep logic).
from dsql_migrator.core.cdc_offset_seed import (
    OffsetSeedError,
    build_connect_offset_record,
    build_source_offset,
    build_source_partition,
)


def parse_partitions_map(partitions_map_csv: Optional[str]) -> dict[str, int]:
    """Parse a ``"topic:count,topic:count"`` string into ``{topic: int}``.

    Skips empty / malformed entries (a bad count leaves that topic on the flat
    default). ``rpartition(":")`` splits on the LAST colon so a topic name that
    itself contains a colon is preserved. This is the exact inverse of the
    app-side serializer (``cdc.py`` builds ``SinkTopicPartitions`` as
    ``",".join(f"{t}:{p}" ...)``) and is byte-for-byte the Lambda's inline
    ``_parse_partitions_map``.
    """
    result: dict[str, int] = {}
    for pair in (partitions_map_csv or "").split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        topic, _, count = pair.rpartition(":")
        topic = topic.strip()
        try:
            result[topic] = int(count.strip())
        except ValueError:
            continue
    return result


def binlog_seq(file_name: Optional[str]) -> Optional[int]:
    """Return the integer sequence in a binlog file name (``mysql-bin.000123`` -> 123).

    MySQL binlog names are ``basename.NNNNNN`` and the numeric suffix is monotonic,
    but it GROWS in width at rollover (``.999999`` -> ``.1000000``). A lexicographic
    compare is therefore wrong across the width change (``'1000000' < '999999'``),
    which would make the no-clobber guard mis-classify an ADVANCED connector as
    behind and rewind it. Comparing the parsed integer is correct across the
    rollover. Returns ``None`` when the suffix is not a plain integer (unexpected
    name), so the caller can fall back to a lexicographic compare rather than crash.
    """
    if not file_name or "." not in file_name:
        return None
    suffix = file_name.rsplit(".", 1)[-1]
    return int(suffix) if suffix.isdigit() else None


def offset_already_at_or_past(
    existing: Optional[Mapping[str, Any]], wm: Mapping[str, Any]
) -> bool:
    """True when the connector's live offset is already at/past the watermark.

    Compares the binlog file by its NUMERIC sequence (not lexicographically -- the
    suffix widens at the ``.999999`` -> ``.1000000`` rollover, where a string
    compare inverts) and, within the same file, by position. Skipping the seed
    when this is true is the no-clobber guard: a legitimately-advanced connector
    must never be rewound by a re-deploy. Stays dict/Mapping-typed to match the
    Lambda's vendored copy exactly.
    """
    if not existing:
        return False
    cur_file = existing.get("file")
    cur_pos = existing.get("pos")
    if not cur_file or cur_pos is None:
        return False
    wm_file, wm_pos = wm["file"], int(wm["pos"])
    if cur_file != wm_file:
        cur_seq, wm_seq = binlog_seq(cur_file), binlog_seq(wm_file)
        if cur_seq is not None and wm_seq is not None:
            return cur_seq > wm_seq
        # Unparseable suffix (unexpected name): fall back to the old lexicographic
        # compare rather than guess -- still correct while both files are same-width.
        return cur_file > wm_file
    return int(cur_pos) >= wm_pos


@dataclass(frozen=True)
class TopicSpec:
    """Pure description of ONE Kafka topic to pre-create.

    ``configs`` is the ``topic_configs`` map: ``{}`` for a plain data topic,
    ``{"cleanup.policy": "compact"}`` for the offset topic, and/or
    ``{"max.message.bytes": "<n>"}`` for data/DLQ topics. ``replication_factor``
    is intentionally NOT a field: ``rf=-1`` (let the broker apply its default) is
    a uniform MSK-Serverless *execution* constant, not a per-topic decision, so it
    stays in the Lambda's I/O wrappers and this spec maps 1:1 onto the
    ``(name, num_partitions, topic_configs)`` the seeder already builds.
    """

    name: str
    partitions: int
    configs: dict[str, str] = field(default_factory=dict)


def plan_topics(
    *,
    offset_topic: str,
    offset_partitions: int | str = 1,
    sink_topics: Sequence[str],
    default_partitions: int | str,
    partitions_map: Optional[Mapping[str, int]] = None,
    max_message_bytes: Optional[int | str] = None,
    dlq_topic: Optional[str] = None,
) -> list[TopicSpec]:
    """Compute every topic the seeder must pre-create, as pure :class:`TopicSpec`.

    Mirrors the inline decisions in the Lambda's ``_ensure_compact_topic`` /
    ``_ensure_data_topics`` / ``_ensure_dlq_topic``:

    * one **compact offset** spec — ``cleanup.policy=compact``,
      ``partitions=int(offset_partitions)`` (MSK Connect requires this topic to
      pre-exist compacted);
    * N **data** specs — ``partitions = int(partitions_map.get(name,
      default_partitions))`` (size-proportional per-topic plan, flat default
      otherwise); ``max.message.bytes`` applied when set so a >1 MiB change event
      isn't rejected;
    * one **DLQ** spec when ``dlq_topic`` is given — ``partitions=1`` (a deliberate
      invariant: a dead-letter queue carries poison records and never needs
      scaling) with the same ``max.message.bytes`` so an oversized dead-letter is
      accepted rather than killing the sink task.

    Takes already-parsed inputs (call :func:`parse_partitions_map` on the CSV
    first); ``max_message_bytes`` is stringified as the broker expects. Order is
    offset topic, then data topics in ``sink_topics`` order, then the DLQ topic.
    """
    part_map = partitions_map or {}
    max_bytes_cfg: dict[str, str] = (
        {"max.message.bytes": str(int(max_message_bytes))} if max_message_bytes else {}
    )

    specs: list[TopicSpec] = [
        TopicSpec(
            name=offset_topic,
            partitions=int(offset_partitions),
            configs={"cleanup.policy": "compact"},
        )
    ]
    for name in sink_topics:
        name = name.strip()
        if not name:
            continue
        specs.append(
            TopicSpec(
                name=name,
                partitions=int(part_map.get(name, default_partitions)),
                configs=dict(max_bytes_cfg),
            )
        )
    if dlq_topic:
        specs.append(
            TopicSpec(name=dlq_topic, partitions=1, configs=dict(max_bytes_cfg))
        )
    return specs


__all__ = [
    "OffsetSeedError",
    "TopicSpec",
    "binlog_seq",
    "build_connect_offset_record",
    "build_source_offset",
    "build_source_partition",
    "offset_already_at_or_past",
    "parse_partitions_map",
    "plan_topics",
]
