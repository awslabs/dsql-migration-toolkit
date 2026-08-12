# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the in-process CDC Kafka-I/O seed module
(:mod:`dsql_migrator.core.cdc_kafka_seed`, the "External" SeedMode path).

kafka-python / the MSK IAM signer are an OPTIONAL extra and are NOT installed in
the test venv, so these tests inject FAKE admin/consumer/producer factories (the
module's test seams) and never touch a real Kafka. The pure decisions
(plan_topics, offset record, no-clobber) live in cdc_kafka_prep and are tested
there; here we assert the I/O shells and the seed orchestration behave like the
Lambda's seeder._seed.
"""

from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timezone

import pytest

from dsql_migrator.core import cdc_kafka_seed
from dsql_migrator.core.cdc_kafka_prep import (
    TopicSpec,
    build_connect_offset_record,
    plan_topics,
)
from dsql_migrator.core.cdc_kafka_seed import (
    CdcSeedError,
    ensure_topics,
    seed_kafka_prep,
)
from dsql_migrator.core.models import Watermark


# --------------------------------------------------------------------------- #
# Fakes (the injected factories the module accepts as test seams)
# --------------------------------------------------------------------------- #
class _NewTopicRec:
    """What FakeAdmin records per create_topics call (mirrors kafka.admin.NewTopic)."""

    def __init__(self, name, num_partitions, replication_factor, topic_configs=None):
        self.name = name
        self.num_partitions = num_partitions
        self.replication_factor = replication_factor
        self.topic_configs = topic_configs or {}


class _FakeAdmin:
    def __init__(self, recorder, **kw):
        self._rec = recorder
        recorder.admin_kwargs = kw

    def create_topics(self, topics):
        for t in topics:
            if t.name in self._rec.existing_topics:
                raise self._rec.already_exists_exc()
            self._rec.created.append((t.name, t.num_partitions, t.topic_configs))

    def close(self):
        self._rec.admin_closed = True


class _FakeMsg:
    def __init__(self, key: str, value: str):
        self.key = key.encode()
        self.value = value.encode()


class _FakeTP:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return (self.topic, self.partition) == (other.topic, other.partition)


class _FakeConsumer:
    def __init__(self, recorder, **kw):
        self._rec = recorder
        self._assigned = []
        self._polled = False

    def partitions_for_topic(self, topic):
        return set(self._rec.topic_partitions)

    def assign(self, tps):
        self._assigned = list(tps)

    def seek_to_beginning(self, *tps):
        pass

    def end_offsets(self, tps):
        return {tp: len(self._rec.existing_records) for tp in tps}

    def poll(self, timeout_ms=0):
        if self._polled or not self._rec.existing_records:
            return {}
        self._polled = True
        return {self._assigned[0]: list(self._rec.existing_records)}

    def position(self, tp):
        return len(self._rec.existing_records)

    def close(self):
        self._rec.consumer_closed = True


class _FakeFuture:
    def __init__(self, recorder):
        self._rec = recorder

    def get(self, timeout=None):
        self._rec.future_awaited = True
        if self._rec.send_raises is not None:
            raise self._rec.send_raises("simulated broker produce failure")
        return object()


class _FakeProducer:
    def __init__(self, recorder, key_serializer=None, value_serializer=None, **kw):
        self._rec = recorder
        recorder.producer_kwargs = kw

    def send(self, topic, key=None, value=None):
        self._rec.produced.append((topic, key, value))
        return _FakeFuture(self._rec)

    def flush(self, timeout=None):
        pass

    def close(self):
        self._rec.producer_closed = True


class _Recorder:
    def __init__(self):
        self.created = []            # (name, partitions, configs)
        self.produced = []           # (topic, key, value)
        self.existing_topics = set()  # names that raise TopicAlreadyExistsError
        self.existing_records = []   # _FakeMsg list the consumer returns
        self.topic_partitions = {0}
        self.send_raises = None
        self.future_awaited = False
        self.admin_kwargs = {}
        self.producer_kwargs = {}
        self.admin_closed = False
        self.consumer_closed = False
        self.producer_closed = False

    def already_exists_exc(self):
        return _TopicAlreadyExists()

    def admin(self, **kw):
        return _FakeAdmin(self, **kw)

    def consumer(self, **kw):
        return _FakeConsumer(self, **kw)

    def producer(self, **kw):
        return _FakeProducer(self, **kw)


class _TopicAlreadyExists(Exception):
    pass


@pytest.fixture
def rec():
    return _Recorder()


@pytest.fixture(autouse=True)
def _fake_kafka(monkeypatch, rec):
    """Install a fake `kafka` package so the module's lazy _import_kafka() succeeds
    and iam_sasl_args() can build its token-provider subclass without the real deps.
    Individual tests still inject their FakeAdmin/Consumer/Producer via factories;
    this only satisfies the import + the NewTopic/TopicAlreadyExistsError symbols."""
    kafka = types.ModuleType("kafka")
    kafka.KafkaAdminClient = object
    kafka.KafkaConsumer = object
    kafka.KafkaProducer = object
    kafka.TopicPartition = _FakeTP
    admin_mod = types.ModuleType("kafka.admin")
    admin_mod.NewTopic = _NewTopicRec
    errors_mod = types.ModuleType("kafka.errors")
    errors_mod.TopicAlreadyExistsError = _TopicAlreadyExists
    # AbstractTokenProvider base for iam_sasl_args' subclass shim.
    net = types.ModuleType("kafka.net")
    net_sasl = types.ModuleType("kafka.net.sasl")
    net_sasl_oauth = types.ModuleType("kafka.net.sasl.oauth")

    class _AbstractTokenProvider:
        pass

    net_sasl_oauth.AbstractTokenProvider = _AbstractTokenProvider
    signer = types.ModuleType("aws_msk_iam_sasl_signer")

    class _MSKAuthTokenProvider:
        @staticmethod
        def generate_auth_token(region):
            return ("tok-" + region, 0)

    signer.MSKAuthTokenProvider = _MSKAuthTokenProvider
    for name, mod in [
        ("kafka", kafka), ("kafka.admin", admin_mod), ("kafka.errors", errors_mod),
        ("kafka.net", net), ("kafka.net.sasl", net_sasl),
        ("kafka.net.sasl.oauth", net_sasl_oauth),
        ("aws_msk_iam_sasl_signer", signer),
    ]:
        monkeypatch.setitem(sys.modules, name, mod)
    yield


def _wm(gtids="UUID:1-9"):
    return Watermark(
        binlog_file="mysql-bin.000042",
        binlog_position=15324,
        gtid_executed=gtids,
        snapshot_timestamp=datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc),
    )


# --------------------------------------------------------------------------- #
# ensure_topics
# --------------------------------------------------------------------------- #
def test_ensure_topics_creates_each_spec_with_rf_minus_one(rec) -> None:
    specs = [
        TopicSpec("offsets", 1, {"cleanup.policy": "compact"}),
        TopicSpec("p.app.orders", 4, {"max.message.bytes": "4194304"}),
    ]
    ensure_topics("boot:9098", "us-east-1", specs, admin_factory=rec.admin)
    assert rec.created == [
        ("offsets", 1, {"cleanup.policy": "compact"}),
        ("p.app.orders", 4, {"max.message.bytes": "4194304"}),
    ]
    # rf=-1 is supplied by the I/O layer (not a TopicSpec field); the fake records
    # what NewTopic received — assert via a dedicated capture.
    assert rec.admin_closed


def test_ensure_topics_supplies_replication_factor_minus_one(rec) -> None:
    captured = {}

    class _Admin(_FakeAdmin):
        def create_topics(self, topics):
            captured["rf"] = topics[0].replication_factor
            super().create_topics(topics)

    ensure_topics(
        "boot:9098", "us-east-1", [TopicSpec("t", 1)],
        admin_factory=lambda **kw: _Admin(rec, **kw),
    )
    assert captured["rf"] == -1


def test_ensure_topics_swallows_already_exists(rec) -> None:
    rec.existing_topics = {"p.app.orders"}
    specs = [TopicSpec("offsets", 1), TopicSpec("p.app.orders", 4)]
    ensure_topics("boot:9098", "us-east-1", specs, admin_factory=rec.admin)
    # offsets created; the already-existing one is skipped, no raise.
    assert ("offsets", 1, {}) in rec.created
    assert all(name != "p.app.orders" for name, _p, _c in rec.created)


def test_ensure_topics_empty_is_noop(rec) -> None:
    ensure_topics("boot:9098", "us-east-1", [], admin_factory=rec.admin)
    assert rec.created == []


# --------------------------------------------------------------------------- #
# seed_kafka_prep — decision matrix (mirrors seeder._seed)
# --------------------------------------------------------------------------- #
def test_seed_creates_topics_and_produces_when_watermark_present(rec) -> None:
    rec.topic_partitions = set()  # brand-new offsets topic -> no existing offset
    result = seed_kafka_prep(
        bootstrap="boot:9098", region="us-east-1",
        offset_topic="stack-debezium-source-offsets", offset_partitions=1,
        sink_topics=["dsqlcdc.app.orders"], default_partitions=8,
        sink_topic_partitions_csv=None, max_message_bytes="4194304",
        dlq_topic="dsql-sink-dlq",
        connector_name="stack-debezium-source", topic_prefix="dsqlcdc",
        watermark=_wm(),
        admin_factory=rec.admin, consumer_factory=rec.consumer,
        producer_factory=rec.producer,
    )
    assert result == "true"
    created = {name: (parts, cfg) for name, parts, cfg in rec.created}
    assert created["stack-debezium-source-offsets"] == (1, {"cleanup.policy": "compact"})
    assert created["dsqlcdc.app.orders"] == (8, {"max.message.bytes": "4194304"})
    assert created["dsql-sink-dlq"] == (1, {"max.message.bytes": "4194304"})
    # produced exactly one record, matching the canonical builder byte-for-byte.
    assert len(rec.produced) == 1
    topic, key, value = rec.produced[0]
    assert topic == "stack-debezium-source-offsets"
    exp_key, exp_value = build_connect_offset_record(
        "stack-debezium-source", "dsqlcdc", _wm()
    )
    assert (key, value) == (exp_key, exp_value)
    # idempotence MUST be off (regression guard) and the future MUST be awaited.
    assert rec.producer_kwargs.get("enable_idempotence") is False
    assert rec.future_awaited is True


def test_seed_skips_produce_when_no_watermark(rec) -> None:
    rec.topic_partitions = set()
    result = seed_kafka_prep(
        bootstrap="boot:9098", region="us-east-1",
        offset_topic="offsets", sink_topics=["dsqlcdc.app.orders"],
        default_partitions=4, max_message_bytes="4194304", dlq_topic="dsql-sink-dlq",
        connector_name="c", topic_prefix="dsqlcdc", watermark=None,
        admin_factory=rec.admin, consumer_factory=rec.consumer,
        producer_factory=rec.producer,
    )
    assert result == "skipped"
    # Topics still pre-created; nothing produced.
    assert any(name == "dsqlcdc.app.orders" for name, _p, _c in rec.created)
    assert rec.produced == []


def test_seed_skips_when_live_offset_already_advanced(rec) -> None:
    # Stage an existing offset PAST the watermark -> no-clobber skip.
    key_json, _ = build_connect_offset_record("c", "dsqlcdc", _wm())
    advanced = json.dumps(
        {"file": "mysql-bin.000099", "pos": 5, "row": 0, "server_id": 0, "event": 0}
    )
    rec.existing_records = [_FakeMsg(key_json, advanced)]
    result = seed_kafka_prep(
        bootstrap="boot:9098", region="us-east-1",
        offset_topic="offsets", sink_topics=["dsqlcdc.app.orders"],
        default_partitions=4, connector_name="c", topic_prefix="dsqlcdc",
        watermark=_wm(),
        admin_factory=rec.admin, consumer_factory=rec.consumer,
        producer_factory=rec.producer,
    )
    assert result == "skipped"
    assert rec.produced == []  # live connector not rewound
    assert rec.created  # topic ensure still ran


def test_seed_read_modify_write_preserves_live_offset_shape(rec) -> None:
    key_json, _ = build_connect_offset_record("c", "dsqlcdc", _wm())
    behind = json.dumps(
        {"file": "mysql-bin.000010", "pos": 2, "row": 0, "server_id": 7,
         "event": 0, "ts_usec": 12345}
    )
    rec.existing_records = [_FakeMsg(key_json, behind)]
    result = seed_kafka_prep(
        bootstrap="boot:9098", region="us-east-1",
        offset_topic="offsets", sink_topics=["dsqlcdc.app.orders"],
        default_partitions=4, connector_name="c", topic_prefix="dsqlcdc",
        watermark=_wm(),
        admin_factory=rec.admin, consumer_factory=rec.consumer,
        producer_factory=rec.producer,
    )
    assert result == "true"
    _topic, _key, value = rec.produced[0]
    produced = json.loads(value)
    assert produced["file"] == "mysql-bin.000042"  # overridden from watermark
    assert produced["pos"] == 15324
    assert produced["ts_usec"] == 12345  # preserved live key
    assert produced["server_id"] == 7


def test_seed_partition_plan_matches_plan_topics(rec) -> None:
    # The topics ensure_topics creates must equal plan_topics() for the same inputs.
    rec.topic_partitions = set()
    seed_kafka_prep(
        bootstrap="boot:9098", region="us-east-1",
        offset_topic="offsets", offset_partitions=1,
        sink_topics=["dsqlcdc.app.hot", "dsqlcdc.app.cold"], default_partitions=1,
        sink_topic_partitions_csv="dsqlcdc.app.hot:4", max_message_bytes="4194304",
        dlq_topic="dsql-sink-dlq",
        connector_name="c", topic_prefix="dsqlcdc", watermark=None,
        admin_factory=rec.admin, consumer_factory=rec.consumer,
        producer_factory=rec.producer,
    )
    created = {name: (parts, cfg) for name, parts, cfg in rec.created}
    plan = plan_topics(
        offset_topic="offsets", offset_partitions=1,
        sink_topics=["dsqlcdc.app.hot", "dsqlcdc.app.cold"], default_partitions=1,
        partitions_map={"dsqlcdc.app.hot": 4}, max_message_bytes="4194304",
        dlq_topic="dsql-sink-dlq",
    )
    planned = {s.name: (s.partitions, s.configs) for s in plan}
    assert created == planned


# --------------------------------------------------------------------------- #
# produce — silent-gap regression guard
# --------------------------------------------------------------------------- #
def test_produce_raises_when_broker_rejects(rec) -> None:
    rec.send_raises = RuntimeError
    with pytest.raises(RuntimeError):
        cdc_kafka_seed.produce(
            "boot:9098", "us-east-1", "offsets", "k", "v",
            producer_factory=rec.producer,
        )
    assert rec.future_awaited is True  # the future WAS awaited (not swallowed)


def test_produce_sets_idempotence_off_and_acks_all(rec) -> None:
    cdc_kafka_seed.produce(
        "boot:9098", "us-east-1", "offsets", "k", "v", producer_factory=rec.producer
    )
    assert rec.producer_kwargs.get("enable_idempotence") is False
    assert rec.producer_kwargs.get("acks") == "all"
    assert rec.producer_closed is True


# --------------------------------------------------------------------------- #
# iam_sasl_args
# --------------------------------------------------------------------------- #
def test_iam_sasl_args_shape_and_token(rec) -> None:
    args = cdc_kafka_seed.iam_sasl_args("ap-northeast-2")
    assert args["security_protocol"] == "SASL_SSL"
    assert args["sasl_mechanism"] == "OAUTHBEARER"
    provider = args["sasl_oauth_token_provider"]
    assert provider.token() == "tok-ap-northeast-2"


# --------------------------------------------------------------------------- #
# Missing-extra behavior: a clear CdcSeedError, not a bare ImportError
# --------------------------------------------------------------------------- #
def test_missing_extra_raises_cdcseederror(monkeypatch) -> None:
    # Simulate the extra not installed: make the lazy kafka import fail.
    import builtins

    real_import = builtins.__import__

    def _blocked(name, *a, **k):
        if name == "kafka" or name.startswith("kafka.") or name == "aws_msk_iam_sasl_signer":
            raise ImportError("no kafka")
        return real_import(name, *a, **k)

    for mod in [m for m in list(sys.modules) if m == "kafka" or m.startswith("kafka.")]:
        monkeypatch.delitem(sys.modules, mod, raising=False)
    monkeypatch.delitem(sys.modules, "aws_msk_iam_sasl_signer", raising=False)
    monkeypatch.setattr(builtins, "__import__", _blocked)
    with pytest.raises(CdcSeedError, match="cdc-external"):
        cdc_kafka_seed.ensure_topics("boot", "us-east-1", [TopicSpec("t", 1)])
