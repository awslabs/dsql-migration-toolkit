# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""In-process Kafka I/O for the Lambda-free ("External") CDC seed mode.

This is the **impure** counterpart to :mod:`dsql_migrator.core.cdc_kafka_prep`
(which is pure and forbids Kafka imports). It ports the four Kafka-I/O shells the
in-VPC offset-seeder Lambda uses (``deploy/cdc-stack/lambda/seeder.py``) so the
app can, when ``SeedMode=External``, do the CDC Kafka prep itself over the MSK
IAM bootstrap **before** the MSK Connect connectors are created:

1. pre-create the compacted ``connect-offsets`` topic + the per-table data topics
   + the DLQ topic (via :func:`cdc_kafka_prep.plan_topics`);
2. seed the ``connect-offsets`` record from the Full Load watermark (via
   :func:`cdc_kafka_prep.build_connect_offset_record`), with the same no-clobber
   guard the Lambda uses (:func:`cdc_kafka_prep.offset_already_at_or_past`).

Every *decision* is delegated to the pure module; this module is Kafka I/O only.

Dependency & purity notes
-------------------------
``kafka-python`` and ``aws-msk-iam-sasl-signer-python`` are an **optional extra**
(``pip install ".[cdc-external]"``), NOT core deps, so the default (Lambda-mode)
install and container image are unchanged. Every ``kafka`` / signer import here is
therefore **lazy** (inside functions), so this module imports fine without the
extra installed — only the actual I/O calls require it. If the extra is missing,
the lazy import raises ``ImportError`` and :func:`seed_kafka_prep` converts it to a
clear, actionable error.

Reachability: MSK Serverless's bootstrap is VPC-private. This module only works
from a host inside the cdc-stack VPC that is admitted on port 9098 and whose IAM
identity carries data-plane ``kafka-cluster:*`` — i.e. the future in-VPC EC2 host.
It is wired and unit-tested here (seams injected), but the security-group / IAM /
VPC-co-location changes that let a real host reach the cluster land separately.
"""
from __future__ import annotations

from typing import Any, Callable, Mapping, Optional, Sequence

from dsql_migrator.core.cdc_kafka_prep import (
    TopicSpec,
    build_connect_offset_record,
    offset_already_at_or_past,
    parse_partitions_map,
    plan_topics,
)
from dsql_migrator.core.watermark import Watermark


class CdcSeedError(RuntimeError):
    """Raised when the in-process CDC Kafka prep cannot complete.

    Notably raised (with an actionable message) when ``SeedMode=External`` is
    selected but the optional ``cdc-external`` dependency extra is not installed.
    """


# --------------------------------------------------------------------------- #
# IAM SASL auth (MSK Serverless, OAUTHBEARER)
# --------------------------------------------------------------------------- #
def iam_sasl_args(region: str) -> dict[str, Any]:
    """kafka-python SASL kwargs for MSK IAM via the AWS MSK IAM SASL signer.

    Ported verbatim from ``seeder.py``: the OAUTHBEARER token provider MUST
    subclass kafka-python's ``AbstractTokenProvider`` (kafka-python 3.x rejects a
    plain class). The abstract base moved across kafka-python versions
    (``kafka.net.sasl.oauth`` in 3.x, ``kafka.oauth.abstract`` in classic 2.x); we
    locate it across layouts and fall back to ``object`` so the same code works
    regardless of which kafka-python is installed.

    Lazy imports: raises :class:`CdcSeedError` if the ``cdc-external`` extra
    (kafka-python + the signer) is not installed.
    """
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as exc:  # pragma: no cover - exercised via seed_kafka_prep
        raise CdcSeedError(_MISSING_EXTRA_MSG) from exc

    base: type = object
    for module_path in ("kafka.net.sasl.oauth", "kafka.oauth.abstract", "kafka.oauth"):
        try:
            mod = __import__(module_path, fromlist=["AbstractTokenProvider"])
            base = getattr(mod, "AbstractTokenProvider")
            break
        except (ImportError, AttributeError):
            continue

    class _TokenProvider(base):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            try:
                super().__init__()
            except TypeError:
                pass

        def token(self) -> str:
            tok, _ = MSKAuthTokenProvider.generate_auth_token(region)
            return tok

    return {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "sasl_oauth_token_provider": _TokenProvider(),
    }


_MISSING_EXTRA_MSG = (
    "SeedMode=External requires the optional CDC Kafka client, which is not "
    "installed. Install it with:  pip install \".[cdc-external]\"  (adds "
    "kafka-python + aws-msk-iam-sasl-signer-python). SeedMode=Lambda does not "
    "need this extra."
)


def _import_kafka():
    """Lazily import the kafka-python symbols this module needs.

    Returns ``(KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition,
    NewTopic, TopicAlreadyExistsError)``. Raises :class:`CdcSeedError` with an
    actionable message when the optional extra is absent.
    """
    try:
        from kafka import (
            KafkaAdminClient,
            KafkaConsumer,
            KafkaProducer,
            TopicPartition,
        )
        from kafka.admin import NewTopic
        from kafka.errors import TopicAlreadyExistsError
    except ImportError as exc:
        raise CdcSeedError(_MISSING_EXTRA_MSG) from exc
    return (
        KafkaAdminClient,
        KafkaConsumer,
        KafkaProducer,
        TopicPartition,
        NewTopic,
        TopicAlreadyExistsError,
    )


# --------------------------------------------------------------------------- #
# Topic creation
# --------------------------------------------------------------------------- #
def ensure_topics(
    bootstrap: str,
    region: str,
    specs: Sequence[TopicSpec],
    *,
    admin_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Create each topic in ``specs`` (idempotent), one by one.

    ``specs`` is the output of :func:`cdc_kafka_prep.plan_topics`. Each spec maps
    1:1 onto ``NewTopic(name, num_partitions=spec.partitions,
    replication_factor=-1, topic_configs=spec.configs)``. ``replication_factor=-1``
    lets the broker apply its default (required for MSK Serverless) and is supplied
    here because it is a uniform I/O-execution constant, not a per-topic decision.
    An already-existing topic is left as-is (``TopicAlreadyExistsError`` swallowed),
    matching the Lambda and keeping re-runs idempotent.

    ``admin_factory`` is the injected test seam; it defaults to the real
    ``KafkaAdminClient`` (kwargs: ``bootstrap_servers`` + the IAM SASL args).
    """
    if not specs:
        return
    (KafkaAdminClient, _C, _P, _TP, NewTopic, TopicAlreadyExistsError) = _import_kafka()
    factory = admin_factory or KafkaAdminClient
    admin = factory(bootstrap_servers=bootstrap, **iam_sasl_args(region))
    try:
        for spec in specs:
            try:
                admin.create_topics(
                    [
                        NewTopic(
                            name=spec.name,
                            num_partitions=spec.partitions,
                            replication_factor=-1,
                            topic_configs=dict(spec.configs),
                        )
                    ]
                )
            except TopicAlreadyExistsError:
                continue
    finally:
        admin.close()


# --------------------------------------------------------------------------- #
# Offset read (no-clobber) + produce
# --------------------------------------------------------------------------- #
def read_existing_offset(
    bootstrap: str,
    region: str,
    topic: str,
    key_json: str,
    *,
    consumer_factory: Optional[Callable[..., Any]] = None,
) -> Optional[dict[str, Any]]:
    """Scan the compacted offsets topic for the latest value of ``key_json``.

    Returns the parsed offset dict, or ``None`` if the key has never been written
    (or the topic is empty / absent). Ported from ``seeder.py``: assigns all
    partitions, seeks to the beginning, and reads to the end, keeping the last
    value seen for the key. ``consumer_factory`` is the injected test seam.
    """
    import json

    (_A, KafkaConsumer, _P, TopicPartition, _NT, _E) = _import_kafka()
    factory = consumer_factory or KafkaConsumer
    consumer = factory(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        **iam_sasl_args(region),
    )
    latest: Optional[dict[str, Any]] = None
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


def produce(
    bootstrap: str,
    region: str,
    topic: str,
    key_json: str,
    value_json: str,
    *,
    producer_factory: Optional[Callable[..., Any]] = None,
) -> None:
    """Produce ONE ``connect-offsets`` seed record and BLOCK on its result.

    Ported verbatim from ``seeder.py`` — the details are correctness, not
    optimization:

    * ``enable_idempotence=False`` is REQUIRED: with ``acks="all"`` kafka-python
      turns on the idempotent producer, which sends an ``InitProducerIdRequest``
      that MSK Serverless rejects (``ClusterAuthorizationFailedError``) unless the
      IAM role grants ``kafka-cluster:WriteDataIdempotently``. A single seed record
      needs no idempotence.
    * We BLOCK on the send Future's ``.get()`` so a broker-side failure (auth,
      RecordTooLarge, etc.) RAISES here instead of being swallowed. ``flush()``
      does not reliably raise, so a failed seed would otherwise return "success"
      and the source connector would start with NO seeded offset — silently losing
      every change between the Full Load watermark and CDC start.

    ``producer_factory`` is the injected test seam.
    """
    (_A, _C, KafkaProducer, _TP, _NT, _E) = _import_kafka()
    factory = producer_factory or KafkaProducer
    producer = factory(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode(),
        value_serializer=lambda v: v.encode(),
        acks="all",
        enable_idempotence=False,
        **iam_sasl_args(region),
    )
    try:
        future = producer.send(topic, key=key_json, value=value_json)
        future.get(timeout=30)  # raises KafkaError on failure
        producer.flush(timeout=30)
    finally:
        producer.close()


# --------------------------------------------------------------------------- #
# Orchestrator — mirrors seeder._seed, decisions delegated to cdc_kafka_prep
# --------------------------------------------------------------------------- #
def seed_kafka_prep(
    *,
    bootstrap: str,
    region: str,
    offset_topic: str,
    offset_partitions: int | str = 1,
    sink_topics: Sequence[str],
    default_partitions: int | str,
    sink_topic_partitions_csv: Optional[str] = None,
    max_message_bytes: Optional[int | str] = None,
    dlq_topic: Optional[str] = None,
    connector_name: str,
    topic_prefix: str,
    watermark: Optional[Watermark] = None,
    admin_factory: Optional[Callable[..., Any]] = None,
    consumer_factory: Optional[Callable[..., Any]] = None,
    producer_factory: Optional[Callable[..., Any]] = None,
) -> str:
    """Do the full in-process CDC Kafka prep. Mirrors ``seeder._seed``.

    ALWAYS pre-creates the compacted offset topic + the per-table data topics + the
    DLQ topic (so the source and sink connectors can be created in parallel). Then,
    ONLY when ``watermark`` carries binlog coordinates, seeds the ``connect-offsets``
    record for a gapless Full Load -> CDC handoff, guarded by the read-modify-write
    no-clobber check so a legitimately-advanced connector is never rewound.

    Returns ``"true"`` when a seed record was produced, ``"skipped"`` when the seed
    was skipped (no watermark, or the live offset is already at/past it). Every
    decision (topic plan, offset record, no-clobber compare) is delegated to
    :mod:`cdc_kafka_prep`; this function only performs the Kafka I/O.

    Raises :class:`CdcSeedError` if the optional ``cdc-external`` extra is missing.
    """
    # (1) ALWAYS: pre-create the topics. plan_topics is the canonical shaping.
    specs = plan_topics(
        offset_topic=offset_topic,
        offset_partitions=offset_partitions,
        sink_topics=list(sink_topics),
        default_partitions=default_partitions,
        partitions_map=parse_partitions_map(sink_topic_partitions_csv),
        max_message_bytes=max_message_bytes,
        dlq_topic=dlq_topic,
    )
    ensure_topics(bootstrap, region, specs, admin_factory=admin_factory)

    # (2) CONDITIONAL: seed the connect-offsets record only for a gapless handoff.
    if watermark is None or not watermark.binlog_file or watermark.binlog_position is None:
        return "skipped"

    key_json, _ = build_connect_offset_record(connector_name, topic_prefix, watermark)
    existing = read_existing_offset(
        bootstrap, region, offset_topic, key_json, consumer_factory=consumer_factory
    )
    wm_compare: Mapping[str, Any] = {
        "file": watermark.binlog_file,
        "pos": watermark.binlog_position,
    }
    if offset_already_at_or_past(existing, wm_compare):
        return "skipped"

    key_json, value_json = build_connect_offset_record(
        connector_name, topic_prefix, watermark, base_offset=existing
    )
    produce(
        bootstrap, region, offset_topic, key_json, value_json,
        producer_factory=producer_factory,
    )
    return "true"


__all__ = [
    "CdcSeedError",
    "ensure_topics",
    "iam_sasl_args",
    "produce",
    "read_existing_offset",
    "seed_kafka_prep",
]
