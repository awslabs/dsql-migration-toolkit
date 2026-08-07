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


def _parse_partitions_map(partitions_map_csv):
    """Parse a "topic:count,topic:count" string into {topic: int}. Skips malformed
    entries (a bad count leaves that topic on the flat default)."""
    result = {}
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


def _ensure_data_topics(
    bootstrap, region, topics_csv, default_partitions,
    partitions_map_csv=None, max_message_bytes=None,
):
    """Pre-create each per-table sink topic (idempotent), so the sink connector can
    be created IN PARALLEL with the source instead of after it.

    Without pre-created topics the sink hits the empty-partition-assignment race, so
    the deploy used to create the source first (and wait for Debezium to auto-create
    the topics) THEN the sink -- two serial MSK Connect creations. Creating the topics
    here up front (deterministic names ``<prefix>.<db>.<table>`` the caller already
    computes) lets both connectors deploy in one pass and removes the race at its
    source. Because Debezium's ``topic.creation`` only shapes topics IT creates, the
    seeder must reproduce that shaping itself so pre-creation is not a regression:

    * **Per-topic partitions** -- ``partitions_map_csv`` ("topic:count,...") carries
      the size-proportional plan (compute_cdc_partition_plan.partitions_by_topic);
      topics absent from it use ``default_partitions``. Partition counts are
      IRREVERSIBLE, so getting them right at creation is essential.
    * **max.message.bytes** -- set to ``max_message_bytes`` so a >1 MiB change event
      isn't rejected (the Kafka default is 1 MiB; the source producer uses this too).

    Each topic is created individually so already-existing ones are skipped (Debezium
    ``topic.creation`` then no-ops on them). ``replication_factor=-1`` lets the broker
    apply its default (required for MSK Serverless).
    """
    topics = [t.strip() for t in (topics_csv or "").split(",") if t.strip()]
    if not topics:
        print("no data topics to pre-create (empty SinkTopics)")
        return
    part_map = _parse_partitions_map(partitions_map_csv)
    topic_configs = None
    if max_message_bytes:
        topic_configs = {"max.message.bytes": str(int(max_message_bytes))}
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, **_iam_sasl_args(region))
    created = skipped = 0
    try:
        for name in topics:
            parts = int(part_map.get(name, default_partitions))
            try:
                admin.create_topics(
                    [NewTopic(name=name, num_partitions=parts,
                              replication_factor=-1, topic_configs=topic_configs)]
                )
                created += 1
            except TopicAlreadyExistsError:
                skipped += 1
    finally:
        admin.close()
    print(
        f"pre-created {created} data topic(s) (per-topic partitions from map, "
        f"default {default_partitions}; max.message.bytes={max_message_bytes}), "
        f"{skipped} already existed"
    )


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


def _binlog_seq(file_name):
    """Return the integer sequence in a binlog file name ('mysql-bin.000123' -> 123).

    MySQL binlog names are ``basename.NNNNNN`` and the numeric suffix is monotonic,
    but it GROWS in width at rollover (``.999999`` -> ``.1000000``). A lexicographic
    compare is therefore wrong across the width change ('1000000' < '999999'), which
    made the no-clobber guard mis-classify an ADVANCED connector as behind and rewind
    it. Comparing the parsed integer is correct across the rollover. Returns ``None``
    when the suffix is not a plain integer (unexpected name), so the caller can fall
    back to a lexicographic compare rather than crash.
    """
    if not file_name or "." not in file_name:
        return None
    suffix = file_name.rsplit(".", 1)[-1]
    return int(suffix) if suffix.isdigit() else None


def _offset_already_at_or_past(existing, wm):
    """True when the connector's live offset is already at/past the watermark.

    Compares the binlog file by its NUMERIC sequence (not lexicographically -- the
    suffix widens at the .999999 -> .1000000 rollover, where a string compare
    inverts) and, within the same file, by position. Skipping the seed here is the
    no-clobber guard: a legitimately-advanced connector must never be rewound by a
    re-deploy.
    """
    if not existing:
        return False
    cur_file = existing.get("file")
    cur_pos = existing.get("pos")
    if not cur_file or cur_pos is None:
        return False
    wm_file, wm_pos = wm["file"], int(wm["pos"])
    if cur_file != wm_file:
        cur_seq, wm_seq = _binlog_seq(cur_file), _binlog_seq(wm_file)
        if cur_seq is not None and wm_seq is not None:
            return cur_seq > wm_seq
        # Unparseable suffix (unexpected name): fall back to the old lexicographic
        # compare rather than guess -- still correct while both files are same-width.
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
    """Pre-create topics (ALWAYS) and seed the connect-offsets record (ONLY when a
    watermark is present).

    ``props`` is the custom resource's ResourceProperties: the per-table sink topic
    list + partition count (``SinkTopics`` / ``TopicPartitions``, always present) and
    the Full-Load watermark fields (``WatermarkBinlogFile`` etc., present only on the
    gapless Full-Load -> CDC handoff). Everything else (bootstrap, offset topic,
    connector identity, region) comes from the Lambda environment the cdc-stack sets.

    Two responsibilities, split so this resource can run on EVERY start (including
    CDC-only, which has no watermark):
    1. **Always** pre-create the per-table data topics + the compacted offset topic,
       so the source and sink connectors can be created in one parallel pass (the
       sink no longer waits for the source to auto-create topics).
    2. **Only with a watermark** seed the connect-offsets record for a gapless handoff.

    Returns a small dict echoed back as the custom resource's Data.
    """
    bootstrap = os.environ["MSK_BOOTSTRAP"]
    topic = os.environ["OFFSETS_TOPIC"]
    connector_name = os.environ["CONNECTOR_NAME"]
    topic_prefix = os.environ["TOPIC_PREFIX"]
    # Match the source worker config's offset.storage.partitions (the cdc-stack
    # always sets OFFSET_PARTITIONS=1; the default here only guards a direct call).
    partitions = os.environ.get("OFFSET_PARTITIONS", "1")
    region = os.environ.get("AWS_REGION") or os.environ.get("TARGET_REGION", "us-east-1")

    # (1) ALWAYS: pre-create the per-table sink topics (parallel-connector deploy) and
    # the compacted offset topic (MSK Connect requires it pre-created). Data topics
    # reproduce Debezium topic.creation's shaping: per-topic partitions from the
    # size-proportional map (SinkTopicPartitions), flat TopicPartitions as the
    # fallback, and max.message.bytes so a >1 MiB event isn't rejected.
    data_partitions = props.get("TopicPartitions") or os.environ.get(
        "TOPIC_PARTITIONS", "1"
    )
    _ensure_data_topics(
        bootstrap, region, props.get("SinkTopics"), data_partitions,
        partitions_map_csv=props.get("SinkTopicPartitions"),
        max_message_bytes=props.get("MaxMessageBytes"),
    )
    _ensure_compact_topic(bootstrap, region, topic, partitions)

    # (2) CONDITIONAL: seed the connect-offsets record only for a gapless Full-Load
    # handoff (a watermark is present). CDC-only starts have none -> topics are
    # created above and the connector starts from the current binlog (legacy).
    wm = {
        "file": props.get("WatermarkBinlogFile"),
        "pos": props.get("WatermarkBinlogPos"),
        "gtids": props.get("WatermarkGtids") or None,
        "ts_sec": props.get("WatermarkTsSec"),
    }
    if not wm["file"] or wm["pos"] in (None, ""):
        print("no watermark -> topics ensured, offset seed skipped (CDC-only)")
        return {"Seeded": "skipped"}
    if wm["ts_sec"] in (None, ""):
        wm["ts_sec"] = 0

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