# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The cdc-stack invariants the sink's tombstone skip relies on (sink plugin v57).

From v57 the sink skips every Debezium tombstone: the op=d record before it carries the same
record key and is the delete (a re-keyed table already skipped its tombstones, G15). That loses
nothing only while

- both source connectors emit every op=d: ``skipped.operations`` is ``none``. A value naming
  ``d`` would drop the delete and leave only its tombstone, which the sink now skips, so the
  row would stay on Aurora DSQL;
- no data topic is compacted. Compaction keeps only the latest record of a key, so it could
  drop the op=d and keep the tombstone (and later drop that too). The template sets no
  ``cleanup.policy`` (Debezium's ``topic.creation.*`` would otherwise create a data topic
  compacted), and both topic pre-create paths -- the Lambda seeder and the External seeder's
  plan (:func:`dsql_migrator.core.cdc_kafka_prep.plan_topics`) -- give each data topic and the
  DLQ topic ``max.message.bytes`` only; the source offsets topic is the one compacted topic.

The invariant tests are guards: they pass before and after v57, and fail when a later change
breaks an invariant. The last test pins the template's comments to the v57 sink. Doubles only:
no AWS, no Kafka.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from dsql_migrator.core.cdc_kafka_prep import plan_topics

from tests.test_offset_seeder_lambda import _props, seeder_env  # noqa: F401 (fixture)

_TEMPLATE = Path(__file__).resolve().parents[1] / "deploy" / "cdc-stack" / "cdc-stack.yaml"
_SOURCES = ("DebeziumSourceConnector", "PostgresSourceConnector")
_OFFSETS = "mysql-dsql-cdc-stack-debezium-source-offsets"  # seeder_env's OFFSETS_TOPIC


@pytest.fixture(scope="module")
def template_text() -> str:
    return _TEMPLATE.read_text(encoding="utf-8")


def _connector_configs(text: str) -> dict:
    resources = yaml.safe_load(text)["Resources"]
    return {
        name: resource["Properties"]["ConnectorConfiguration"]
        for name, resource in resources.items()
        if resource.get("Type") == "AWS::KafkaConnect::Connector"
    }


def test_both_source_connectors_emit_every_delete(template_text: str) -> None:
    configs = _connector_configs(template_text)
    for name in _SOURCES:
        assert configs[name]["skipped.operations"] == "none", name


def test_the_template_compacts_no_topic(template_text: str) -> None:
    assert "cleanup.policy" not in template_text
    for name, config in _connector_configs(template_text).items():
        assert not [key for key in config if "cleanup" in key], name


@pytest.mark.parametrize("max_bytes", [None, 4194304])
def test_the_external_seeders_plan_compacts_only_the_offsets_topic(max_bytes) -> None:
    specs = plan_topics(
        offset_topic="offsets", sink_topics=["dsqlcdc.app.orders", "dsqlcdc.app.orders.v2"],
        default_partitions=2, partitions_map={"dsqlcdc.app.orders": 4},
        max_message_bytes=max_bytes, dlq_topic="dsql-sink-dlq",
    )
    by_name = {spec.name: spec.configs for spec in specs}
    assert by_name.pop("offsets") == {"cleanup.policy": "compact"}
    assert set(by_name) == {"dsqlcdc.app.orders", "dsqlcdc.app.orders.v2", "dsql-sink-dlq"}
    for name, configs in by_name.items():
        assert set(configs) <= {"max.message.bytes"}, (name, configs)


@pytest.mark.parametrize("max_bytes", [None, "4194304"])
def test_the_lambda_seeder_compacts_only_the_offsets_topic(seeder_env, max_bytes) -> None:  # noqa: F811
    module, recorder = seeder_env
    recorder.topic_partitions = set()  # a new cluster: every topic is created
    props = _props(
        SinkTopics="dsqlcdc.app.orders,dsqlcdc.app.orders.v2", TopicPartitions="2",
        DlqTopicName="dsql-sink-dlq",
    )
    if max_bytes:
        props["MaxMessageBytes"] = max_bytes
    module._seed(props)
    by_name = {name: configs for name, _parts, configs in recorder.created_topics}
    assert by_name.pop(_OFFSETS) == {"cleanup.policy": "compact"}
    assert set(by_name) == {"dsqlcdc.app.orders", "dsqlcdc.app.orders.v2", "dsql-sink-dlq"}
    for name, configs in by_name.items():
        assert set(configs or {}) <= {"max.message.bytes"}, (name, configs)


def test_the_template_comments_describe_the_v57_sink(template_text: str) -> None:
    # FAILS WITHOUT THE FIX: the table.include.list comment said Start CDC refuses a name with
    # a second '.', and the sink connectors' comment that delete.enabled toggles tombstone
    # apply; from v57 a '.' inside a table name streams (its entry escaped) and every tombstone
    # is skipped.
    flat = " ".join(template_text.replace("#", " ").split())
    assert "or with a second '.' (topic_name_refusal)" not in flat
    assert "delete.enabled toggles DELETE/tombstone apply" not in flat
    assert (
        "A name whose table name holds a '.' has every '.' escaped (table_include_entry: "
        "shop\\.orders\\.v2 for table orders.v2 of database shop), so it matches only itself."
    ) in flat
    assert flat.count(
        "delete.enabled toggles applying a Debezium delete (op=d) as a DELETE; a tombstone is "
        "always skipped (the op=d before it is the delete)."
    ) == 2
