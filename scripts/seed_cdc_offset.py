#!/usr/bin/env python3
"""Seed the Debezium MySQL connector's start offset for a GAPLESS Full Load -> CDC
handoff. RUN THIS INSIDE THE VPC (the MSK Serverless bootstrap is private).

What it does
------------
Produces one record to the Kafka Connect ``connect-offsets`` topic so that, on
its next (re)start, the Debezium MySQL source connector (snapshot.mode=schema_only)
resumes streaming from the exact binlog/GTID position captured by the Full Load
watermark -- with no gap and no overlap (Property 11).

It uses a read-modify-write: it first consumes the connector's CURRENT offset
record (to mirror the connector's exact value shape for this Debezium version),
then overrides only ts_sec/file/pos/gtids from the watermark. If no current
record exists it builds one from scratch via core.cdc_offset_seed.

Prerequisites (install inside the VPC host):
    pip install kafka-python aws-msk-iam-sasl-signer-python
AWS creds must allow MSK IAM connect + the connector's offsets topic.

Inputs:
    --watermark-file  JSON {"binlog_file","binlog_position","gtid_executed",
                      "snapshot_timestamp"} written by the Full Load harness.
    --bootstrap       MSK Serverless SASL/IAM bootstrap (host:9098).
    --offsets-topic   The connector's connect-offsets topic. For MSK Connect this
                      is set by the worker configuration (offset.storage.topic).
                      Find it via the connector's workerConfiguration or
                      `kafka-topics --list` (look for the *-offsets topic).
    --connector-name  default: mysql-dsql-cdc-stack-debezium-source
    --topic-prefix    default: dsqlcdc
    --dry-run         build + print the record, do NOT produce.

Safety: re-seeding a connector that has already committed offsets requires the
connector to be STOPPED first (delete/pause), then seed, then start -- otherwise
the running connector overwrites the seed with its own committed offset. This
script only writes the record; stop/start the connector via the cdc-stack /
`aws kafkaconnect` around it.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
from dsql_migrator.core.cdc_offset_seed import build_connect_offset_record  # noqa: E402
from dsql_migrator.core.watermark import Watermark  # noqa: E402


def _load_watermark(path: str) -> Watermark:
    data = json.load(open(path, encoding="utf-8"))
    ts = data.get("snapshot_timestamp")
    snapshot_ts = (
        datetime.fromisoformat(ts) if isinstance(ts, str)
        else datetime.now(timezone.utc)
    )
    return Watermark(
        binlog_file=data.get("binlog_file"),
        binlog_position=data.get("binlog_position"),
        gtid_executed=data.get("gtid_executed"),
        server_uuid=data.get("server_uuid"),
        snapshot_timestamp=snapshot_ts,
    )


def _iam_sasl_args(region: str) -> dict:
    """kafka-python SASL args for MSK IAM via the AWS MSK IAM SASL signer."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

    class _TokenProvider:
        def token(self):
            tok, _ = MSKAuthTokenProvider.generate_auth_token(region)
            return tok

    return {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": _TokenProvider(),
    }


def _read_existing_offset(consumer, topic, key_json):
    """Scan the compacted offsets topic for the latest value of ``key_json``."""
    from kafka import TopicPartition

    latest = None
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
                    except Exception:
                        latest = None
        if all(consumer.position(tp) >= ends[tp] for tp in tps):
            break
    return latest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--watermark-file", required=True)
    ap.add_argument("--bootstrap", default=os.environ.get("MSK_BOOTSTRAP", ""))
    ap.add_argument("--offsets-topic", required=True)
    ap.add_argument("--connector-name", default="mysql-dsql-cdc-stack-debezium-source")
    ap.add_argument("--topic-prefix", default="dsqlcdc")
    ap.add_argument("--region", default=os.environ.get("TARGET_REGION", "us-east-1"))
    ap.add_argument("--no-rmw", action="store_true",
                    help="skip read-modify-write; build offset value from scratch")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    wm = _load_watermark(args.watermark_file)
    # First pass: build the key (and a from-scratch value) so we can look up the
    # existing offset by key for read-modify-write.
    key_json, _ = build_connect_offset_record(
        args.connector_name, args.topic_prefix, wm)

    base = None
    consumer = None
    if not args.no_rmw and not args.dry_run:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            bootstrap_servers=args.bootstrap, enable_auto_commit=False,
            auto_offset_reset="earliest", consumer_timeout_ms=10000,
            **_iam_sasl_args(args.region),
        )
        base = _read_existing_offset(consumer, args.offsets_topic, key_json)
        print(f"existing offset for key: {'found' if base else 'none'}")

    key_json, value_json = build_connect_offset_record(
        args.connector_name, args.topic_prefix, wm, base_offset=base)

    print("OFFSETS TOPIC:", args.offsets_topic)
    print("KEY  :", key_json)
    print("VALUE:", value_json)

    if args.dry_run:
        print("[DRY-RUN] not produced.")
        return 0

    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: v.encode(),
        acks="all",
        **_iam_sasl_args(args.region),
    )
    producer.send(args.offsets_topic, key=key_json, value=value_json)
    producer.flush(timeout=30)
    producer.close()
    if consumer is not None:
        consumer.close()
    print("Seeded. Now (re)start the source connector so it reads this offset:")
    print("  - ensure the connector was STOPPED/absent before this seed, then")
    print("  - deploy/start it (cdc-stack DeploySink flow or aws kafkaconnect).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
