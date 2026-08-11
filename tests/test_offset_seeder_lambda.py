# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the in-VPC offset-seeder Lambda (deploy/cdc-stack/lambda/seeder.py).

The Lambda seeds the Debezium source connect-offsets record for a gapless Full
Load -> CDC handoff (Property 11). Its runtime deps (kafka-python, the MSK IAM
SASL signer) and the CloudFormation response helper are NOT installed in the test
venv -- they ship only in the deployment zip -- so these tests inject lightweight
FAKE modules into ``sys.modules`` and then load ``seeder.py`` straight from its
file path. The fakes record what the handler did (topic created, record produced)
and let each test script the "existing offset" the read-modify-write path sees.

What is covered:
- the vendored offset-record builder stays byte-identical to the canonical
  :mod:`dsql_migrator.core.cdc_offset_seed` (a drift guard -- the two must match
  or the seed mis-positions the connector);
- _seed creates the COMPACTED offset topic and produces the exact record;
- idempotent no-clobber: when the live offset is already at/past the watermark
  the seed is SKIPPED (a legitimately-advanced connector is never rewound);
- read-modify-write preserves the live connector's offset shape;
- Delete is a no-op success and the handler always sends a CFN response.
"""

from __future__ import annotations

import importlib.util
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

from dsql_migrator.core.cdc_offset_seed import build_connect_offset_record
from dsql_migrator.core.models import Watermark

SEEDER_PATH = (
    Path(__file__).resolve().parent.parent
    / "deploy" / "cdc-stack" / "lambda" / "seeder.py"
)


# --------------------------------------------------------------------------- #
# Fake kafka-python / signer / cfnresponse modules
# --------------------------------------------------------------------------- #
class _FakeTopicPartition:
    def __init__(self, topic, partition):
        self.topic = topic
        self.partition = partition

    def __hash__(self):
        return hash((self.topic, self.partition))

    def __eq__(self, other):
        return (self.topic, self.partition) == (other.topic, other.partition)


class _FakeMsg:
    def __init__(self, key: str, value: str):
        self.key = key.encode()
        self.value = value.encode()


class _Recorder:
    """Shared sink the fake Kafka clients write their activity into."""

    def __init__(self):
        self.created_topics = []   # list of (name, partitions, configs)
        self.produced = []         # list of (topic, key, value)
        self.producer_kwargs = {}  # KafkaProducer kwargs (assert idempotence off)
        self.send_raises = None    # exception class -> future.get() raises it
        self.existing_records = []  # _FakeMsg list the consumer will return
        self.topic_partitions = {0}  # partitions_for_topic result (empty -> none)
        self.create_raises = None  # set to an exception class to simulate exists


def _install_fakes(recorder: _Recorder) -> list[str]:
    """Register fake kafka/signer/cfnresponse modules; return the names added."""
    added = []

    # cfnresponse
    cfn = types.ModuleType("cfnresponse")
    cfn.SUCCESS = "SUCCESS"
    cfn.FAILED = "FAILED"
    cfn.sent = []  # (status, data, physical_id)

    def _send(event, context, status, data, physical_id=None, **kw):
        cfn.sent.append((status, data, physical_id))

    cfn.send = _send
    sys.modules["cfnresponse"] = cfn
    added.append("cfnresponse")

    # aws_msk_iam_sasl_signer (only imported lazily inside _iam_sasl_args; the
    # fakes never call .token(), so a stub provider is enough).
    signer = types.ModuleType("aws_msk_iam_sasl_signer")

    class _MSKAuthTokenProvider:
        @staticmethod
        def generate_auth_token(region):
            return ("tok", 0)

    signer.MSKAuthTokenProvider = _MSKAuthTokenProvider
    sys.modules["aws_msk_iam_sasl_signer"] = signer
    added.append("aws_msk_iam_sasl_signer")

    # kafka + kafka.admin + kafka.errors
    kafka = types.ModuleType("kafka")

    class _TopicAlreadyExistsError(Exception):
        pass

    class _FakeAdmin:
        def __init__(self, **kw):
            pass

        def create_topics(self, topics):
            if recorder.create_raises is not None:
                raise recorder.create_raises()
            for t in topics:
                recorder.created_topics.append(
                    (t.name, t.num_partitions, t.topic_configs)
                )

        def close(self):
            pass

    class _FakeConsumer:
        def __init__(self, **kw):
            self._assigned = []
            self._polled = False

        def partitions_for_topic(self, topic):
            return set(recorder.topic_partitions)

        def assign(self, tps):
            self._assigned = list(tps)

        def seek_to_beginning(self, *tps):
            pass

        def end_offsets(self, tps):
            return {tp: len(recorder.existing_records) for tp in tps}

        def poll(self, timeout_ms=0):
            if self._polled or not recorder.existing_records:
                return {}
            self._polled = True
            tp = self._assigned[0]
            return {tp: list(recorder.existing_records)}

        def position(self, tp):
            return len(recorder.existing_records)

        def close(self):
            pass

    class _FakeRecordMetadata:
        topic = "offsets"
        partition = 0
        offset = 0

    class _FakeFuture:
        # Mirrors kafka-python's FutureRecordMetadata: .get() returns metadata on
        # success (or raises on failure). The seeder now blocks on this so a failed
        # produce surfaces instead of being swallowed.
        def get(self, timeout=None):
            if recorder.send_raises is not None:
                raise recorder.send_raises("simulated broker produce failure")
            return _FakeRecordMetadata()

    class _FakeProducer:
        def __init__(self, key_serializer=None, value_serializer=None, **kw):
            self._ks = key_serializer or (lambda k: k)
            self._vs = value_serializer or (lambda v: v)
            # Record that idempotence was explicitly disabled (avoids the MSK
            # ClusterAuthorizationFailedError on InitProducerId).
            recorder.producer_kwargs = kw

        def send(self, topic, key=None, value=None):
            recorder.produced.append((topic, key, value))
            return _FakeFuture()

        def flush(self, timeout=None):
            pass

        def close(self):
            pass

    kafka.KafkaAdminClient = _FakeAdmin
    kafka.KafkaConsumer = _FakeConsumer
    kafka.KafkaProducer = _FakeProducer
    kafka.TopicPartition = _FakeTopicPartition

    admin_mod = types.ModuleType("kafka.admin")

    class _NewTopic:
        def __init__(self, name, num_partitions, replication_factor, topic_configs=None):
            self.name = name
            self.num_partitions = num_partitions
            self.replication_factor = replication_factor
            self.topic_configs = topic_configs or {}

    admin_mod.NewTopic = _NewTopic

    errors_mod = types.ModuleType("kafka.errors")
    errors_mod.TopicAlreadyExistsError = _TopicAlreadyExistsError

    kafka.admin = admin_mod
    kafka.errors = errors_mod
    sys.modules["kafka"] = kafka
    sys.modules["kafka.admin"] = admin_mod
    sys.modules["kafka.errors"] = errors_mod
    added.extend(["kafka", "kafka.admin", "kafka.errors"])

    # Expose the exception type so a test can trigger the "topic already exists" path.
    recorder.TopicAlreadyExistsError = _TopicAlreadyExistsError
    return added


def _load_seeder(recorder: _Recorder):
    """Install fakes and import seeder.py fresh as a module."""
    added = _install_fakes(recorder)
    # Always load a fresh copy (the fakes differ per test).
    sys.modules.pop("seeder", None)
    spec = importlib.util.spec_from_file_location("seeder", SEEDER_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, added


@pytest.fixture
def seeder_env(monkeypatch):
    """Set the Lambda environment the handler reads, and yield (module, recorder)."""
    recorder = _Recorder()
    monkeypatch.setenv("MSK_BOOTSTRAP", "boot:9098")
    monkeypatch.setenv("OFFSETS_TOPIC", "mysql-dsql-cdc-stack-debezium-source-offsets")
    monkeypatch.setenv("CONNECTOR_NAME", "mysql-dsql-cdc-stack-debezium-source")
    monkeypatch.setenv("TOPIC_PREFIX", "dsqlcdc")
    monkeypatch.setenv("OFFSET_PARTITIONS", "1")
    monkeypatch.setenv("TARGET_REGION", "us-east-1")
    monkeypatch.delenv("AWS_REGION", raising=False)
    module, added = _load_seeder(recorder)
    try:
        yield module, recorder
    finally:
        for name in added + ["seeder"]:
            sys.modules.pop(name, None)


def _props(**kw):
    base = {
        "WatermarkBinlogFile": "mysql-bin.000042",
        "WatermarkBinlogPos": "15324",
        "WatermarkGtids": "UUID:1-9",
        "WatermarkTsSec": "1782518400",
    }
    base.update(kw)
    return base


# --------------------------------------------------------------------------- #
# Drift guard: vendored builder must match the canonical one byte-for-byte
# --------------------------------------------------------------------------- #
def test_vendored_record_matches_canonical_builder(seeder_env) -> None:
    module, _ = seeder_env
    ts = datetime(2026, 6, 27, 12, 0, tzinfo=timezone.utc)
    ts_sec = int(ts.timestamp())
    wm_dict = {
        "file": "mysql-bin.000042",
        "pos": 15324,
        "gtids": "UUID:1-9",
        "ts_sec": ts_sec,
    }
    vendored = module._build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source", "dsqlcdc", wm_dict
    )
    canonical = build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source",
        "dsqlcdc",
        Watermark(
            binlog_file="mysql-bin.000042",
            binlog_position=15324,
            gtid_executed="UUID:1-9",
            snapshot_timestamp=ts,
        ),
    )
    assert vendored == canonical


def test_vendored_record_omits_gtids_when_absent(seeder_env) -> None:
    module, _ = seeder_env
    wm_dict = {"file": "mysql-bin.000007", "pos": 4, "gtids": None, "ts_sec": 1}
    _key, value_json = module._build_connect_offset_record("c", "p", wm_dict)
    assert "gtids" not in json.loads(value_json)


# --------------------------------------------------------------------------- #
# _offset_already_at_or_past — the no-clobber comparison
# --------------------------------------------------------------------------- #
def test_offset_compare_none_existing_is_false(seeder_env) -> None:
    module, _ = seeder_env
    assert module._offset_already_at_or_past(None, {"file": "f", "pos": 1}) is False


def test_offset_compare_same_file_higher_pos_is_past(seeder_env) -> None:
    module, _ = seeder_env
    wm = {"file": "mysql-bin.000042", "pos": 100}
    assert module._offset_already_at_or_past({"file": "mysql-bin.000042", "pos": 200}, wm)
    assert module._offset_already_at_or_past({"file": "mysql-bin.000042", "pos": 100}, wm)
    assert not module._offset_already_at_or_past({"file": "mysql-bin.000042", "pos": 50}, wm)


def test_offset_compare_later_file_is_past(seeder_env) -> None:
    module, _ = seeder_env
    wm = {"file": "mysql-bin.000042", "pos": 100}
    # A later binlog file is past, regardless of pos.
    assert module._offset_already_at_or_past({"file": "mysql-bin.000099", "pos": 1}, wm)
    assert not module._offset_already_at_or_past({"file": "mysql-bin.000001", "pos": 999}, wm)


def test_offset_compare_survives_the_binlog_width_rollover(seeder_env) -> None:
    # Audit C9: at the .999999 -> .1000000 rollover the suffix WIDENS, so a
    # lexicographic compare inverts ('1000000' < '999999') and would rewind an
    # advanced connector. The numeric-sequence compare must classify .1000000 as
    # LATER than .999999.
    module, _ = seeder_env
    wm = {"file": "mysql-bin.999999", "pos": 500}
    # Connector genuinely advanced across the rollover -> at/past, must NOT re-seed.
    assert module._offset_already_at_or_past({"file": "mysql-bin.1000000", "pos": 1}, wm)
    assert module._offset_already_at_or_past({"file": "mysql-bin.1000042", "pos": 1}, wm)
    # And the reverse: a connector still on the pre-rollover file is behind a
    # post-rollover watermark.
    wm2 = {"file": "mysql-bin.1000000", "pos": 10}
    assert not module._offset_already_at_or_past({"file": "mysql-bin.999999", "pos": 999}, wm2)
    # _binlog_seq parses the numeric suffix; a non-numeric suffix -> None (lexicographic
    # fallback, never a crash).
    assert module._binlog_seq("mysql-bin.1000000") == 1000000
    assert module._binlog_seq("mysql-bin.000042") == 42
    assert module._binlog_seq("weird-name") is None


# --------------------------------------------------------------------------- #
# _seed — topic create + produce + idempotent skip
# --------------------------------------------------------------------------- #
def test_seed_creates_compact_topic_and_produces_record(seeder_env) -> None:
    module, recorder = seeder_env
    recorder.topic_partitions = set()  # brand-new topic -> no existing offset
    result = module._seed(_props())
    assert result == {"Seeded": "true"}
    # Compacted topic created with the env partition count.
    assert recorder.created_topics, "offset topic was not created"
    name, parts, configs = recorder.created_topics[0]
    assert name == "mysql-dsql-cdc-stack-debezium-source-offsets"
    assert parts == 1
    assert configs.get("cleanup.policy") == "compact"
    # Idempotence MUST be explicitly disabled: with acks=all kafka-python would
    # otherwise send InitProducerId, which MSK rejects (ClusterAuthorizationFailed)
    # -> the seed silently fails and the gapless handoff breaks. Regression guard.
    assert recorder.producer_kwargs.get("enable_idempotence") is False
    # Exactly one record produced, to the offsets topic, matching the canonical key.
    assert len(recorder.produced) == 1
    topic, key, value = recorder.produced[0]
    assert topic == "mysql-dsql-cdc-stack-debezium-source-offsets"
    exp_key, exp_value = build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source",
        "dsqlcdc",
        Watermark(
            binlog_file="mysql-bin.000042",
            binlog_position=15324,
            gtid_executed="UUID:1-9",
            snapshot_timestamp=datetime.fromtimestamp(1782518400, tz=timezone.utc),
        ),
    )
    assert key == exp_key
    assert value == exp_value


def test_seed_raises_when_produce_fails(seeder_env) -> None:
    # Regression for the silent gapless-handoff failure: when the broker rejects
    # the produce (e.g. ClusterAuthorizationFailedError), the seeder MUST raise so
    # the CloudFormation custom resource reports FAILED -- not swallow it and let
    # the source connector start with no seeded offset (a silent contiguous gap).
    module, recorder = seeder_env
    recorder.topic_partitions = set()
    recorder.send_raises = RuntimeError
    with pytest.raises(RuntimeError):
        module._seed(_props())


def test_handler_reports_failed_when_produce_fails(seeder_env) -> None:
    # End-to-end: a failed produce must surface as cfnresponse.FAILED so the stack
    # op fails loudly instead of deploying a connector that skips the handoff.
    import cfnresponse  # the vendored module the handler imports

    module, recorder = seeder_env
    recorder.topic_partitions = set()
    recorder.send_raises = RuntimeError
    cfnresponse.sent.clear()
    module.handler(_event("Create"), _Ctx())
    status, _data, _pid = cfnresponse.sent[-1]
    assert status == cfnresponse.FAILED


def test_seed_skips_when_live_offset_already_advanced(seeder_env) -> None:
    module, recorder = seeder_env
    # Build the key the seeder will look up, then stage an already-advanced offset.
    key_json, _ = module._build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source",
        "dsqlcdc",
        {"file": "mysql-bin.000042", "pos": 15324, "gtids": "UUID:1-9", "ts_sec": 1782518400},
    )
    advanced = json.dumps(
        {"file": "mysql-bin.000099", "pos": 5, "row": 0, "server_id": 0, "event": 0}
    )
    recorder.existing_records = [_FakeMsg(key_json, advanced)]
    result = module._seed(_props())
    assert result == {"Seeded": "skipped"}
    # No-clobber: nothing produced (the live connector is not rewound).
    assert recorder.produced == []
    # The topic ensure still ran (idempotent create).
    assert recorder.created_topics


def test_seed_read_modify_write_preserves_live_offset_shape(seeder_env) -> None:
    module, recorder = seeder_env
    key_json, _ = module._build_connect_offset_record(
        "mysql-dsql-cdc-stack-debezium-source",
        "dsqlcdc",
        {"file": "mysql-bin.000042", "pos": 15324, "gtids": "UUID:1-9", "ts_sec": 1782518400},
    )
    # Live offset is BEHIND the watermark but carries an extra connector-specific
    # key ("ts_usec") that the seed must preserve (read-modify-write).
    behind = json.dumps(
        {"file": "mysql-bin.000010", "pos": 2, "row": 0, "server_id": 7,
         "event": 0, "ts_usec": 12345}
    )
    recorder.existing_records = [_FakeMsg(key_json, behind)]
    result = module._seed(_props())
    assert result == {"Seeded": "true"}
    _topic, _key, value = recorder.produced[0]
    produced = json.loads(value)
    # Position fields overridden from the watermark...
    assert produced["file"] == "mysql-bin.000042"
    assert produced["pos"] == 15324
    # ...while the live connector's extra keys are preserved.
    assert produced["ts_usec"] == 12345
    assert produced["server_id"] == 7


def test_seed_without_watermark_creates_topics_and_skips_seed(seeder_env) -> None:
    # No watermark (CDC-only start): the seeder still pre-creates the topics but
    # skips the offset seed (there is nothing to seed). It must NOT raise -- topic
    # pre-creation is unconditional so the sink can deploy in parallel.
    module, recorder = seeder_env
    recorder.topic_partitions = set()
    result = module._seed(
        _props(
            WatermarkBinlogFile="", WatermarkBinlogPos="",
            SinkTopics="dsqlcdc.app.orders,dsqlcdc.app.customers", TopicPartitions="4",
        )
    )
    assert result == {"Seeded": "skipped"}
    # The two data topics were pre-created (4 partitions each) plus the compact
    # offset topic; no offset record was produced (no watermark).
    created = {name: parts for name, parts, _cfg in recorder.created_topics}
    assert created.get("dsqlcdc.app.orders") == 4
    assert created.get("dsqlcdc.app.customers") == 4
    assert recorder.produced == []


def test_seed_data_topics_use_per_topic_partitions_and_max_bytes(seeder_env) -> None:
    # Pre-creation must reproduce Debezium topic.creation's shaping, or it regresses:
    # (1) size-proportional per-topic partitions from SinkTopicPartitions (a hot topic
    # gets 4, others the flat default), and (2) max.message.bytes so a >1 MiB event
    # isn't RecordTooLarge'd. Topic partition counts are immutable, so this must be
    # right at creation.
    module, recorder = seeder_env
    recorder.topic_partitions = set()
    module._seed(
        _props(
            WatermarkBinlogFile="", WatermarkBinlogPos="",  # CDC-only: topics only
            SinkTopics="dsqlcdc.app.hot,dsqlcdc.app.cold",
            TopicPartitions="1",  # flat fallback
            SinkTopicPartitions="dsqlcdc.app.hot:4",  # hot table elevated
            MaxMessageBytes="4194304",
        )
    )
    by_name = {name: (parts, cfg) for name, parts, cfg in recorder.created_topics}
    # Hot topic gets its mapped 4 partitions; cold falls back to the flat default 1.
    assert by_name["dsqlcdc.app.hot"][0] == 4
    assert by_name["dsqlcdc.app.cold"][0] == 1
    # Both data topics carry max.message.bytes.
    assert by_name["dsqlcdc.app.hot"][1].get("max.message.bytes") == "4194304"
    assert by_name["dsqlcdc.app.cold"][1].get("max.message.bytes") == "4194304"


def test_seed_pre_creates_dlq_topic_with_raised_max_bytes(seeder_env) -> None:
    # The sink dead-letters a record DSQL rejects on its 1 MiB per-value limit; that
    # record is itself >1 MiB, so the DLQ topic must accept a message larger than the
    # broker's ~1 MiB default or the quarantine RecordTooLarge's and the task dies.
    # The seeder must pre-create the DLQ topic at the same max.message.bytes as the
    # data topics (Kafka Connect would otherwise lazily auto-create it at 1 MiB).
    module, recorder = seeder_env
    recorder.topic_partitions = set()
    module._seed(
        _props(
            WatermarkBinlogFile="", WatermarkBinlogPos="",  # CDC-only: topics only
            SinkTopics="dsqlcdc.app.orders",
            TopicPartitions="1",
            MaxMessageBytes="4194304",
            DlqTopicName="dsql-sink-dlq",
        )
    )
    by_name = {name: (parts, cfg) for name, parts, cfg in recorder.created_topics}
    assert "dsql-sink-dlq" in by_name, "DLQ topic was not pre-created"
    assert by_name["dsql-sink-dlq"][1].get("max.message.bytes") == "4194304"


def test_seed_skips_dlq_topic_when_name_absent(seeder_env) -> None:
    # No DlqTopicName supplied (older param set / DLQ disabled): pre-creation is a
    # no-op and must not raise, so the seeder stays backward-compatible.
    module, recorder = seeder_env
    recorder.topic_partitions = set()
    module._seed(
        _props(
            WatermarkBinlogFile="", WatermarkBinlogPos="",
            SinkTopics="dsqlcdc.app.orders", TopicPartitions="1",
            MaxMessageBytes="4194304",
        )
    )
    created = {name for name, _p, _c in recorder.created_topics}
    assert not any("dlq" in n.lower() for n in created)


def test_seed_pre_creates_data_topics_before_seeding(seeder_env) -> None:
    # A gapless handoff (watermark present) both pre-creates the per-table topics
    # AND seeds the offset.
    module, recorder = seeder_env
    recorder.topic_partitions = set()
    result = module._seed(
        _props(SinkTopics="dsqlcdc.app.orders", TopicPartitions="8")
    )
    assert result == {"Seeded": "true"}
    created = {name: parts for name, parts, _cfg in recorder.created_topics}
    assert created.get("dsqlcdc.app.orders") == 8  # data topic pre-created
    assert recorder.produced  # offset seeded too


# --------------------------------------------------------------------------- #
# handler — Delete no-op + always sends a CFN response
# --------------------------------------------------------------------------- #
class _Ctx:
    log_stream_name = "log-stream"


def _event(request_type, **props):
    return {
        "RequestType": request_type,
        "ResourceProperties": _props(**props),
        "PhysicalResourceId": "pid-1",
    }


def test_handler_delete_is_noop_success(seeder_env) -> None:
    module, recorder = seeder_env
    import cfnresponse

    cfnresponse.sent.clear()
    module.handler({"RequestType": "Delete", "PhysicalResourceId": "pid-1"}, _Ctx())
    assert recorder.produced == []
    assert recorder.created_topics == []
    assert cfnresponse.sent[-1][0] == cfnresponse.SUCCESS


def test_handler_create_seeds_and_reports_success(seeder_env) -> None:
    module, recorder = seeder_env
    import cfnresponse

    cfnresponse.sent.clear()
    recorder.topic_partitions = set()
    module.handler(_event("Create"), _Ctx())
    status, data, _pid = cfnresponse.sent[-1]
    assert status == cfnresponse.SUCCESS
    assert data == {"Seeded": "true"}
    assert recorder.produced


def test_handler_reports_failed_on_exception(seeder_env) -> None:
    module, recorder = seeder_env
    import cfnresponse

    cfnresponse.sent.clear()
    # A broker-side failure (e.g. topic creation raises a non-"already exists" error)
    # -> _seed raises -> handler must send FAILED, not hang.
    recorder.create_raises = RuntimeError
    module.handler(_event("Create"), _Ctx())
    status, data, _pid = cfnresponse.sent[-1]
    assert status == cfnresponse.FAILED
    assert "Error" in data


# --------------------------------------------------------------------------- #
# The REAL vendored cfnresponse: bounded PUT retries (v21). A single failed PUT
# left CloudFormation with no response -> ~1h hang -> DELETE_FAILED (orphaned MSK).
# --------------------------------------------------------------------------- #


def _load_real_cfnresponse():
    """Load the actual deploy/cdc-stack/lambda/cfnresponse.py from its path (the
    tests elsewhere inject a FAKE cfnresponse; here we exercise the real one)."""
    path = SEEDER_PATH.parent / "cfnresponse.py"
    spec = importlib.util.spec_from_file_location("cfnresponse_real_under_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _CfnCtx:
    log_stream_name = "log-stream-xyz"


def _cfn_event():
    return {
        "ResponseURL": "https://s3.example/presigned-put",
        "StackId": "stack-1",
        "RequestId": "req-1",
        "LogicalResourceId": "CdcStartPrepResource",
    }


class _Resp200:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def test_cfnresponse_retries_put_until_success(monkeypatch) -> None:
    cfn = _load_real_cfnresponse()
    calls = {"n": 0}

    def _fake_urlopen(_req, timeout=None):  # noqa: ANN001
        calls["n"] += 1
        if calls["n"] < 3:
            raise OSError("egress not ready yet")  # transient, e.g. ENIs settling
        return _Resp200()

    monkeypatch.setattr(cfn.urllib.request, "urlopen", _fake_urlopen)
    monkeypatch.setattr(cfn.time, "sleep", lambda _s: None)  # skip real backoff
    cfn.send(_cfn_event(), _CfnCtx(), cfn.SUCCESS, {}, "pid")
    assert calls["n"] == 3  # failed twice, succeeded on the 3rd -> stopped retrying


def test_cfnresponse_gives_up_after_max_attempts_without_raising(monkeypatch) -> None:
    cfn = _load_real_cfnresponse()
    calls = {"n": 0}

    def _always_fail(_req, timeout=None):  # noqa: ANN001
        calls["n"] += 1
        raise OSError("egress down")

    monkeypatch.setattr(cfn.urllib.request, "urlopen", _always_fail)
    monkeypatch.setattr(cfn.time, "sleep", lambda _s: None)
    # A raising response helper would crash the handler; it must exhaust the BOUNDED
    # retries and return quietly (never loop forever, never propagate).
    cfn.send(_cfn_event(), _CfnCtx(), cfn.FAILED, {}, "pid", reason="boom")
    assert calls["n"] == cfn._SEND_MAX_ATTEMPTS  # bounded (not 1, not infinite)
