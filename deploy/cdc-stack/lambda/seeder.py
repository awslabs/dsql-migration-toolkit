# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CloudFormation custom-resource Lambda: seed the Debezium connect-offsets topic.

Runs IN-VPC during cdc-stack deployment to give the Full Load -> CDC handoff a
gapless start. The source connector uses snapshot.mode=schema_only (no data
snapshot); it resumes from whatever offset is in its compacted connect-offsets
topic. Without a seed it starts from the CURRENT binlog at creation time, silently
losing every change between the Full Load watermark and connector creation. MSK
Serverless's bootstrap is VPC-private, so the app host cannot produce that record;
this Lambda does, BEFORE the source connector is created (the connector DependsOn
this resource in the template), so the handoff is automatic and gapless.

Create/Update:
1. Create the fixed compacted offset-storage topic the source worker config points
   at (a custom source offset topic must exist with cleanup.policy=compact before
   the connector is created -- AWS MSK Connect requirement).
2. Read-modify-write: read any existing offset for the connector's key and SKIP the
   produce when the live offset is already at/past the watermark, so a legitimately
   advanced connector is never rewound on a stack re-deploy (idempotent).
3. Produce the seed record (key + value) built exactly like
   dsql_migrator.core.cdc_offset_seed.build_connect_offset_record -- vendored inline
   so the Lambda needs none of the app package (only kafka-python +
   aws-msk-iam-sasl-signer-python, pure-Python, bundled in the zip).

Delete is a no-op success. The handler always sends a CloudFormation response (via
the vendored cfnresponse) so a stack op never hangs on this resource -- BUT that
response is an HTTPS PUT to an S3 pre-signed URL, which a VPC-only Lambda can only
reach if the S3 egress path still exists. During teardown the NAT is torn down, so
the cdc-stack routes the response via the S3 GATEWAY endpoint instead and pins the
whole egress path (endpoint + both subnet route-table associations) to be deleted
AFTER this resource (see OffsetSeederFunction's S3_EGRESS_ORDERING env in
cdc-stack.yaml). Without that ordering the Delete PUT timed out and the custom
resource hung the teardown ~1h then DELETE_FAILED.
"""
from __future__ import annotations

import json
import os
import traceback

import cfnresponse

# kafka-python + the MSK IAM SASL signer are bundled into the deployment zip.
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


# --------------------------------------------------------------------------- #
# Offset record builder -- vendored from src/dsql_migrator/core/cdc_offset_seed.py
# (kept byte-compatible; a test guards drift). Duck-typed on a tiny watermark dict
# so the Lambda needs no pydantic / app package.
# --------------------------------------------------------------------------- #
def _build_source_partition(topic_prefix):
    if not topic_prefix:
        raise ValueError("topic_prefix must be a non-empty logical server name")
    return {"server": topic_prefix}


def _build_source_offset(wm, base_offset=None):
    """wm: dict with file, pos, gtids (optional), ts_sec. Mirrors build_source_offset."""
    if not wm.get("file") or wm.get("pos") in (None, ""):
        raise ValueError("watermark has no binlog file:position; cannot seed offset")
    offset = dict(base_offset) if base_offset else {"row": 0, "server_id": 0, "event": 0}
    offset["ts_sec"] = int(wm["ts_sec"])
    offset["file"] = wm["file"]
    offset["pos"] = int(wm["pos"])
    if wm.get("gtids"):
        offset["gtids"] = wm["gtids"]
    else:
        offset.pop("gtids", None)
    offset.pop("snapshot", None)
    offset.pop("snapshot_completed", None)
    return offset


def _build_connect_offset_record(connector_name, topic_prefix, wm, base_offset=None):
    if not connector_name:
        raise ValueError("connector_name must be non-empty")
    key = [connector_name, _build_source_partition(topic_prefix)]
    value = _build_source_offset(wm, base_offset=base_offset)
    return (
        json.dumps(key, sort_keys=True, separators=(",", ":")),
        json.dumps(value, sort_keys=True, separators=(",", ":")),
    )


# --------------------------------------------------------------------------- #
# Kafka (MSK Serverless, IAM SASL) helpers
# --------------------------------------------------------------------------- #
def _iam_sasl_args(region):
    """kafka-python SASL kwargs for MSK IAM via the AWS MSK IAM SASL signer.

    The OAUTHBEARER token provider MUST subclass kafka-python's
    ``AbstractTokenProvider`` -- the bundled kafka-python 3.x rejects a plain class
    ("sasl_oauth_token_provider must implement ...AbstractTokenProvider"). We locate
    the abstract base across the 3.x (``kafka.net.sasl.oauth``) and classic 2.x
    (``kafka.oauth.abstract``) layouts and fall back to ``object`` if neither is
    importable, so the same code works regardless of which kafka-python is bundled.
    """
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    base = object
    for module_path in ("kafka.net.sasl.oauth", "kafka.oauth.abstract", "kafka.oauth"):
        try:
            mod = __import__(module_path, fromlist=["AbstractTokenProvider"])
            base = getattr(mod, "AbstractTokenProvider")
            break
        except (ImportError, AttributeError):
            continue

    class _TokenProvider(base):  # type: ignore[misc, valid-type]
        def __init__(self):
            try:
                super().__init__()
            except TypeError:
                pass

        def token(self):
            tok, _ = MSKAuthTokenProvider.generate_auth_token(region)
            return tok

    return {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": _TokenProvider(),
    }


def _ensure_compact_topic(bootstrap, region, topic, partitions):
    """Create the compacted offset-storage topic if absent (idempotent).

    A custom source offset topic must exist with cleanup.policy=compact before the
    connector is created (MSK Connect requirement). replication_factor=-1 lets the
    broker apply its default (required for MSK Serverless).
    """
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap, **_iam_sasl_args(region)
    )
    try:
        admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=int(partitions),
                    replication_factor=-1,
                    topic_configs={"cleanup.policy": "compact"},
                )
            ]
        )
        print(f"created offset topic {topic} (compact, {partitions} partition(s))")
    except TopicAlreadyExistsError:
        print(f"offset topic {topic} already exists -- reusing")
    finally:
        admin.close()


def _read_existing_offset(bootstrap, region, topic, key_json):
    """Scan the compacted offsets topic for the latest value of ``key_json``.

    Returns the parsed offset dict, or None if the key has never been written.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        **_iam_sasl_args(region),
    )
    latest = None
    try:
        parts = consumer.partitions_for_topic(topic) or set()
        tps = [TopicPartition(topic, p) for p in parts]
        if not tps:
            return None
        consumer.assign(tps)
        consumer.seek_to_beginning(*tps)
        ends = consumer.end_offsets(tps)
        if not any(ends.values()):
            return None
        while True:
            recs = consumer.poll(timeout_ms=3000)
            if not recs:
                break
            for _tp, msgs in recs.items():
                for m in msgs:
                    k = m.key.decode() if m.key else None
                    if k == key_json and m.value:
                        try:
                            latest = json.loads(m.value.decode())
                        except Exception:  # noqa: BLE001
                            latest = None
            if all(consumer.position(tp) >= ends[tp] for tp in tps):
                break
    finally:
        consumer.close()
    return latest


def _offset_already_at_or_past(existing, wm):
    """True when the connector's live offset is already at/past the watermark.

    Compares the same binlog file (lexicographic, since rotated binlog file names
    are zero-padded and monotonic) and position. When the files differ, the larger
    file name is later. Skipping the seed in this case is the no-clobber guard: a
    legitimately-advanced connector must never be rewound by a re-deploy.
    """
    if not existing:
        return False
    cur_file = existing.get("file")
    cur_pos = existing.get("pos")
    if not cur_file or cur_pos is None:
        return False
    wm_file, wm_pos = wm["file"], int(wm["pos"])
    if cur_file != wm_file:
        return cur_file > wm_file
    return int(cur_pos) >= wm_pos


def _produce(bootstrap, region, topic, key_json, value_json):
    # enable_idempotence=False is REQUIRED, not just an optimization. With acks="all"
    # kafka-python turns on the idempotent producer, which sends an
    # InitProducerIdRequest -- and MSK Serverless rejects that with
    # ClusterAuthorizationFailedError (Error 31) unless the IAM role grants
    # kafka-cluster:WriteDataIdempotently. A single offset-seed record needs no
    # idempotence/transaction, so disabling it both avoids the extra permission and
    # removes the InitProducerId round-trip. (The IAM role also grants
    # WriteDataIdempotently as a belt-and-suspenders for any future idempotent path.)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: v.encode(),
        acks="all",
        enable_idempotence=False,
        **_iam_sasl_args(region),
    )
    try:
        # CRITICAL: block on the send Future's result so a broker-side failure
        # (auth, RecordTooLarge, etc.) RAISES here instead of being swallowed.
        # producer.flush() does NOT reliably raise when the producer aborts on a
        # fatal error, so a failed seed previously returned "success" and the
        # source connector started with NO seeded offset -> it skipped every change
        # between the Full Load watermark and CDC start (a silent contiguous gap).
        future = producer.send(topic, key=key_json, value=value_json)
        metadata = future.get(timeout=30)  # raises KafkaError on failure
        print(
            f"seed produced to {metadata.topic}-{metadata.partition}@{metadata.offset}"
        )
        producer.flush(timeout=30)
    finally:
        producer.close()


# --------------------------------------------------------------------------- #
# Custom-resource entry points
# --------------------------------------------------------------------------- #
def _seed(props):
    """Create the offset topic and seed the connect-offsets record (idempotent).

    ``props`` is the custom resource's ResourceProperties -- the watermark fields
    (WatermarkBinlogFile/WatermarkBinlogPos/WatermarkGtids/WatermarkTsSec) the Full
    Load captured. Everything else (bootstrap, topic, connector identity, region)
    comes from the Lambda environment the cdc-stack sets. Returns a small dict
    echoed back as the custom resource's Data.
    """
    bootstrap = os.environ["MSK_BOOTSTRAP"]
    topic = os.environ["OFFSETS_TOPIC"]
    connector_name = os.environ["CONNECTOR_NAME"]
    topic_prefix = os.environ["TOPIC_PREFIX"]
    # Match the source worker config's offset.storage.partitions (the cdc-stack
    # always sets OFFSET_PARTITIONS=1; the default here only guards a direct call).
    partitions = os.environ.get("OFFSET_PARTITIONS", "1")
    region = os.environ.get("AWS_REGION") or os.environ.get("TARGET_REGION", "us-east-1")

    wm = {
        "file": props.get("WatermarkBinlogFile"),
        "pos": props.get("WatermarkBinlogPos"),
        "gtids": props.get("WatermarkGtids") or None,
        "ts_sec": props.get("WatermarkTsSec"),
    }
    if not wm["file"] or wm["pos"] in (None, ""):
        raise ValueError(
            "no watermark binlog file:position in resource properties; cannot "
            "seed a gapless offset"
        )
    if wm["ts_sec"] in (None, ""):
        wm["ts_sec"] = 0

    # Always ensure the compacted offset topic exists -- the source connector's
    # worker config points at it and MSK Connect requires it pre-created.
    _ensure_compact_topic(bootstrap, region, topic, partitions)

    # Build the key first so we can read the connector's live offset for the
    # read-modify-write / no-clobber guard.
    key_json, _ = _build_connect_offset_record(connector_name, topic_prefix, wm)
    existing = _read_existing_offset(bootstrap, region, topic, key_json)
    if _offset_already_at_or_past(existing, wm):
        print(
            f"live offset for {connector_name} already at/past watermark "
            f"{wm['file']}:{wm['pos']} -- skipping seed (no-clobber)"
        )
        return {"Seeded": "skipped"}

    key_json, value_json = _build_connect_offset_record(
        connector_name, topic_prefix, wm, base_offset=existing
    )
    print(f"seeding {connector_name} offset -> {wm['file']}:{wm['pos']}")
    _produce(bootstrap, region, topic, key_json, value_json)
    return {"Seeded": "true"}


def handler(event, context):
    """CloudFormation custom-resource entry point.

    Delete is a no-op success (the compacted topic is owned by the stack and torn
    down with it; nothing to undo). Create/Update run :func:`_seed`. We ALWAYS send
    a CloudFormation response -- on success with the seed result, on any exception
    with FAILED + the error -- so a stack operation never hangs waiting on this
    resource. A stable PhysicalResourceId keeps Update from triggering a replace.
    """
    request_type = event.get("RequestType")
    physical_id = event.get("PhysicalResourceId") or "offset-seed"
    try:
        if request_type == "Delete":
            print("Delete -> no-op success")
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {}, physical_id)
            return
        data = _seed(event.get("ResourceProperties") or {})
        cfnresponse.send(event, context, cfnresponse.SUCCESS, data, physical_id)
    except Exception as exc:  # noqa: BLE001
        traceback.print_exc()
        cfnresponse.send(
            event,
            context,
            cfnresponse.FAILED,
            {"Error": str(exc)},
            physical_id,
        )