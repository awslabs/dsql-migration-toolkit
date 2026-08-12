# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the canonical pure CDC Kafka-prep logic
(:mod:`dsql_migrator.core.cdc_kafka_prep`).

These cover the three pure helpers lifted out of the in-VPC offset-seeder Lambda
(``parse_partitions_map`` / ``binlog_seq`` / ``offset_already_at_or_past``), the
``TopicSpec`` / ``plan_topics`` topic-shaping planner, and confirm the offset
builders are re-exported (same objects) from :mod:`cdc_offset_seed`. The
vendored-vs-canonical drift guards live in ``tests/test_offset_seeder_lambda.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dsql_migrator.core import cdc_kafka_prep, cdc_offset_seed
from dsql_migrator.core.cdc_kafka_prep import (
    TopicSpec,
    binlog_seq,
    build_connect_offset_record,
    offset_already_at_or_past,
    parse_partitions_map,
    plan_topics,
)
from dsql_migrator.core.models import Watermark


# --------------------------------------------------------------------------- #
# parse_partitions_map
# --------------------------------------------------------------------------- #
def test_parse_partitions_map_well_formed() -> None:
    assert parse_partitions_map("a:2,b:3") == {"a": 2, "b": 3}


def test_parse_partitions_map_empty_and_none() -> None:
    assert parse_partitions_map("") == {}
    assert parse_partitions_map(None) == {}


def test_parse_partitions_map_skips_malformed() -> None:
    # 'bad' (no colon) skipped; 'a:' -> int('') ValueError skipped; ':2' -> topic ''
    # kept as '' with 2 (rpartition yields ('', ':', '2')); 'c:x' -> ValueError skipped.
    assert parse_partitions_map("bad,a:,c:x,d:4") == {"d": 4}


def test_parse_partitions_map_whitespace_trimmed() -> None:
    assert parse_partitions_map(" a : 2 , b : 3 ") == {"a": 2, "b": 3}


def test_parse_partitions_map_rpartition_keeps_colon_in_name() -> None:
    # A fully-qualified topic name never contains a colon here, but rpartition on
    # the LAST colon means a colon-bearing name is preserved rather than truncated.
    assert parse_partitions_map("pre.fix:host:5") == {"pre.fix:host": 5}


def test_parse_partitions_map_duplicate_key_last_wins() -> None:
    assert parse_partitions_map("a:1,a:9") == {"a": 9}


# --------------------------------------------------------------------------- #
# binlog_seq
# --------------------------------------------------------------------------- #
def test_binlog_seq_basic() -> None:
    assert binlog_seq("mysql-bin.000042") == 42


def test_binlog_seq_survives_width_rollover() -> None:
    # The whole point: .1000000 must compare GREATER than .999999 numerically.
    assert binlog_seq("mysql-bin.999999") == 999999
    assert binlog_seq("mysql-bin.1000000") == 1000000
    assert binlog_seq("mysql-bin.1000000") > binlog_seq("mysql-bin.999999")


def test_binlog_seq_non_numeric_and_edge() -> None:
    assert binlog_seq("weird-name") is None  # no dot
    assert binlog_seq("a.b") is None  # non-numeric suffix
    assert binlog_seq(None) is None
    assert binlog_seq("") is None


# --------------------------------------------------------------------------- #
# offset_already_at_or_past — the no-clobber guard
# --------------------------------------------------------------------------- #
def test_offset_compare_none_existing_is_false() -> None:
    assert offset_already_at_or_past(None, {"file": "f", "pos": 1}) is False


def test_offset_compare_same_file_by_pos() -> None:
    wm = {"file": "mysql-bin.000042", "pos": 100}
    assert offset_already_at_or_past({"file": "mysql-bin.000042", "pos": 200}, wm)
    assert offset_already_at_or_past({"file": "mysql-bin.000042", "pos": 100}, wm)
    assert not offset_already_at_or_past({"file": "mysql-bin.000042", "pos": 50}, wm)


def test_offset_compare_by_file() -> None:
    wm = {"file": "mysql-bin.000042", "pos": 100}
    assert offset_already_at_or_past({"file": "mysql-bin.000099", "pos": 1}, wm)
    assert not offset_already_at_or_past({"file": "mysql-bin.000001", "pos": 999}, wm)


def test_offset_compare_across_rollover() -> None:
    wm = {"file": "mysql-bin.999999", "pos": 500}
    assert offset_already_at_or_past({"file": "mysql-bin.1000000", "pos": 1}, wm)
    wm2 = {"file": "mysql-bin.1000000", "pos": 10}
    assert not offset_already_at_or_past({"file": "mysql-bin.999999", "pos": 999}, wm2)


def test_offset_compare_missing_fields_is_false() -> None:
    wm = {"file": "mysql-bin.000042", "pos": 100}
    assert offset_already_at_or_past({"file": None, "pos": 5}, wm) is False
    assert offset_already_at_or_past({"file": "mysql-bin.000042", "pos": None}, wm) is False


def test_offset_compare_unparseable_suffix_lexicographic_fallback() -> None:
    # Neither file has a numeric suffix -> lexicographic compare, no crash.
    wm = {"file": "weird-a", "pos": 1}
    assert offset_already_at_or_past({"file": "weird-b", "pos": 1}, wm)
    assert not offset_already_at_or_past({"file": "weird-A", "pos": 1}, wm)


# --------------------------------------------------------------------------- #
# TopicSpec / plan_topics
# --------------------------------------------------------------------------- #
def _by_name(specs) -> dict[str, TopicSpec]:
    return {s.name: s for s in specs}


def test_plan_topics_always_has_compact_offset_topic() -> None:
    specs = plan_topics(
        offset_topic="offsets", offset_partitions=1, sink_topics=[],
        default_partitions=1,
    )
    assert specs[0].name == "offsets"
    assert specs[0].partitions == 1
    assert specs[0].configs == {"cleanup.policy": "compact"}


def test_plan_topics_data_topics_use_per_topic_partitions_and_max_bytes() -> None:
    specs = plan_topics(
        offset_topic="offsets", offset_partitions=1,
        sink_topics=["p.app.hot", "p.app.cold"],
        default_partitions=1,
        partitions_map={"p.app.hot": 4},
        max_message_bytes=4194304,
    )
    by = _by_name(specs)
    assert by["p.app.hot"].partitions == 4  # mapped
    assert by["p.app.cold"].partitions == 1  # flat default
    assert by["p.app.hot"].configs == {"max.message.bytes": "4194304"}
    assert by["p.app.cold"].configs == {"max.message.bytes": "4194304"}


def test_plan_topics_no_max_bytes_gives_empty_data_configs() -> None:
    specs = plan_topics(
        offset_topic="offsets", sink_topics=["p.app.orders"], default_partitions=2,
    )
    assert _by_name(specs)["p.app.orders"].configs == {}


def test_plan_topics_dlq_present_only_when_named_and_single_partition() -> None:
    with_dlq = plan_topics(
        offset_topic="offsets", sink_topics=["p.app.o"], default_partitions=1,
        max_message_bytes=4194304, dlq_topic="dsql-sink-dlq",
    )
    by = _by_name(with_dlq)
    assert by["dsql-sink-dlq"].partitions == 1
    assert by["dsql-sink-dlq"].configs == {"max.message.bytes": "4194304"}

    without_dlq = plan_topics(
        offset_topic="offsets", sink_topics=["p.app.o"], default_partitions=1,
    )
    assert not any("dlq" in n.lower() for n in _by_name(without_dlq))


def test_plan_topics_string_inputs_coerced() -> None:
    # Partition counts / max bytes arrive as strings from the CFN param boundary.
    specs = plan_topics(
        offset_topic="offsets", offset_partitions="1",
        sink_topics=["p.app.o"], default_partitions="8", max_message_bytes="4194304",
    )
    by = _by_name(specs)
    assert by["offsets"].partitions == 1
    assert by["p.app.o"].partitions == 8


def test_plan_topics_skips_blank_sink_topics() -> None:
    specs = plan_topics(
        offset_topic="offsets", sink_topics=["p.app.o", "", "  "],
        default_partitions=1,
    )
    names = [s.name for s in specs]
    assert names == ["offsets", "p.app.o"]


def test_topicspec_is_frozen() -> None:
    import dataclasses
    import pytest

    spec = TopicSpec(name="t", partitions=1)
    assert spec.configs == {}
    with pytest.raises(dataclasses.FrozenInstanceError):
        spec.partitions = 2  # type: ignore[misc]


# --------------------------------------------------------------------------- #
# Re-export identity: ONE canonical builder, not a paraphrase
# --------------------------------------------------------------------------- #
def test_offset_builders_are_reexported_same_objects() -> None:
    assert cdc_kafka_prep.build_connect_offset_record is cdc_offset_seed.build_connect_offset_record
    assert cdc_kafka_prep.build_source_offset is cdc_offset_seed.build_source_offset
    assert cdc_kafka_prep.build_source_partition is cdc_offset_seed.build_source_partition
    assert cdc_kafka_prep.OffsetSeedError is cdc_offset_seed.OffsetSeedError


def test_reexported_builder_produces_record() -> None:
    key_json, value_json = build_connect_offset_record(
        "conn", "prefix",
        Watermark(
            binlog_file="mysql-bin.000042",
            binlog_position=15324,
            gtid_executed="UUID:1-9",
            snapshot_timestamp=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
        ),
    )
    assert '"server":"prefix"' in key_json
    assert '"file":"mysql-bin.000042"' in value_json
    assert '"pos":15324' in value_json
