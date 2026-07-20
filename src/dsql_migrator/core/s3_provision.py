# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Provision the S3 bucket + connector plugin artifacts for a CDC deploy.

The cdc-stack's two MSK Connect plugins (the Debezium MySQL source zip and our
custom DSQL sink jar) are loaded from S3. Rather than make the operator create a
bucket and upload the artifacts by hand, the tool manages a per-account/region
bucket and uploads the two bundled artifacts itself, then feeds the bucket ARN +
object keys into the deploy. This is what lets the deploy form ask for only a
VpcId.

Bucket name is deterministic per identity: ``mysql-dsql-migrator-plugins-<account>-<region>``
(globally unique because the account id is in the name). Uploads are idempotent
(skipped when the object already matches), so re-deploys are cheap.

Read/writes share the single profile-aware boto3 session (mirroring
:mod:`dsql_migrator.core.dsql_metadata`). Clients are injectable for tests. The
artifact UPLOAD is large (~42 MiB total) and MUST run in the background deploy
job, never on the UI thread.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

from dsql_migrator.core.aws_session import BotoSessionLike, build_session

# Canonical, version-independent object keys. The MSK Connect plugin RESOURCE
# names carry a version suffix (cdc-stack PluginVersion) for immutability; the S3
# keys themselves are stable so re-uploads overwrite in place.
DEBEZIUM_PLUGIN_KEY = "cdc-plugins/debezium-mysql-plugin.zip"
# The sink plugin is a ZIP bundle (sink jar + Glue Schema Registry Avro converter
# jar) -- not a bare jar -- because both connectors' worker configs declare the
# AWSKafkaAvroConverter, which must be on each plugin's classpath.
DSQL_SINK_PLUGIN_KEY = "cdc-plugins/dsql-sink-connector.zip"
# The offset-seeder Lambda deployment zip (CFN custom resource). Stable key; the
# cdc-stack reads it via the LambdaSeederS3Key parameter so the in-VPC Lambda code
# is fetched from the same managed bucket as the connector plugins.
LAMBDA_SEEDER_KEY = "cdc-plugins/offset-seeder-lambda.zip"

# Local artifact paths relative to the repo root (this file is at
# src/dsql_migrator/core/s3_provision.py, so parents[3] is the repo root).
_DEBEZIUM_PLUGIN_RELPATH = "connectors/plugins/debezium-mysql-plugin.zip"
_DSQL_SINK_PLUGIN_RELPATH = "connectors/plugins/dsql-sink-plugin.zip"
_LAMBDA_SEEDER_RELPATH = "connectors/plugins/offset-seeder-lambda.zip"

# The PluginVersion token stamped on the cdc-stack plugin resource names. Bumped
# only when the on-disk artifacts change in an incompatible way (MSK Connect
# CustomPlugins are immutable, so a new token forces fresh plugin resources).
# v19 adds a per-table ReplicationLagMs CloudWatch metric (same namespace/dims as
#    v18): the sink reads each event's source commit time (source.ts_ms) and, at
#    apply time, records the worst end-to-end lag (now - source.ts_ms) per table,
#    flushing it as a gauge per offset-commit (Milliseconds). This is a time-based,
#    PK-agnostic replication-lag signal that replaces the UI's imprecise MAX(pk)
#    leading-edge check. Reuses the v18 metric plumbing/IAM (cloudwatch:PutMetricData,
#    metrics.stack); no new dependency. Sink-jar change only.
# v18 emits a per-table NetRowsApplied CloudWatch metric (namespace
#    MysqlDsqlMigrator/CDC, dimensions Stack + Table) so the UI shows CDC net-rows
#    per table WITHOUT COUNT(*)-scanning the source. The sink keeps a running
#    per-table net counter (insert +1 / delete -1 / update 0, classified from the
#    Debezium op) and flushes it once per offset-commit via PutMetricData. Strictly
#    best-effort: a CloudWatch error is logged and NEVER fails replication; disabled
#    unless the cdc-stack passes metrics.stack. Adds the aws-sdk cloudwatch dep to the
#    shaded jar (~11.5->13 MiB) and requires cloudwatch:PutMetricData on the connector
#    execution role. Sink-jar + connector config + connector-role IAM change.
# v17 makes the sink survive a transient DSQL connectivity blip instead of dying.
#    On a transient failure (OCC budget exhausted, or a connection torn down by
#    DSQL's 1h idle close / IAM-token expiry / MSK Connect worker recycle),
#    isTransient() was correct but applyBatch/applyRecordByRecord re-raised a plain
#    org.apache.kafka.connect.errors.ConnectException -- which WorkerSinkTask treats
#    as FATAL (kills the task; offset never advances; CDC stalls until a manual
#    restart). v17 throws RetriableException instead (via a new transientRetryException
#    helper), which WorkerSinkTask catches and pauses+redelivers the same batch on the
#    next poll, so the sink self-heals across a reconnect. Retried indefinitely until
#    DSQL recovers (NOT bounded by errors.retry.timeout, which only wraps the
#    conversion stage). isTransient()'s classification and the permanent/quarantine
#    (DLQ) path are unchanged. Sink-jar change only.
# v16 removes a hidden per-row round-trip in the sink: bind() called
#    getParameterMetaData() for EVERY change event, and on pgjdbc that issues a
#    server-side Parse/Describe (a read-only transaction) per row. DSQL's
#    TotalTransactions ~= ReadOnlyTransactions ~= applied-rows/sec confirmed it --
#    the sink was spending ~1 round-trip/row just to look up param types, negating
#    much of the v13/v15 round-trip batching. Metadata is identical for all rows of
#    a given SQL, so it is now fetched ONCE per prepared statement and passed into
#    bind(). Sink-jar change only. (OccConflicts stayed 0 throughout: the ceiling
#    was never server-side write contention -- it was this client round-trip.)
# v15 rebuilds the sink plugin to further cut DSQL round-trips (still the final,
#    latency-bound bottleneck after v13/v14). Two coupled changes: (1) the JDBC
#    URL enables reWriteBatchedInserts=true, so pgjdbc collapses a batch of
#    single-row INSERTs into ONE multi-row "INSERT ... VALUES (..),(..) ON CONFLICT"
#    statement (N execute round-trips -> 1); (2) applyChunkBatched first dedupes
#    each same-SQL run to one row per PK (last image wins -- idempotent,
#    order-preserving), which is what makes the rewrite SAFE (a rewritten multi-row
#    ON CONFLICT rejects a duplicate conflict key: "cannot affect row a second
#    time"). Sink-jar change only.
# v14 tunes the SOURCE for throughput (the bottleneck moved to the source after
#    v13: ~2,000 rec/s at ~12% CPU = produce/queue-bound, not binlog-parse-bound).
#    The source WorkerConfiguration now sets producer.batch.size / producer.linger.ms
#    / producer.compression.type (SourceProducerBatchSize / SourceProducerLingerMs /
#    SourceProducerCompression) so the connector's producer sends larger, compressed
#    batches; the connector config gains max.batch.size / max.queue.size
#    (SourceMaxBatchSize / SourceMaxQueueSize) to drain more binlog events per
#    streaming iteration. The producer knobs MUST live in the worker config, not the
#    connector config, because MSK Connect rejects per-connector ".override." keys.
#    The source WorkerConfiguration is a custom-named immutable resource whose
#    PropertiesFileContent changed, so its name (and thus PluginVersion) must bump --
#    no plugin JAR changed. Template + PluginVersion only.
# v13 rebuilds the sink plugin for a throughput win: the apply path coalesces each
#    maximal run of CONSECUTIVE same-SQL change events into one JDBC executeBatch()
#    instead of a per-row executeUpdate(). DSQL is latency-bound (each statement is
#    a distributed round-trip; the sink task ran at ~5% CPU / ~550 rec/s), so
#    collapsing per-row round-trips into batched sends is the primary lever. Order
#    is preserved -- only contiguous identical-SQL events group, so an upsert
#    followed by a delete on the same PK still applies in arrival order; a run
#    breaks on any table/column-set/kind change. Poison-row isolation, OCC retry,
#    and idempotent replay are unchanged (a permanent failure still falls back to
#    record-by-record apply). Sink-jar change only.
# v12 rebuilds the sink plugin for a corrected start() advisory log only: when no
#    ErrantRecordReporter is wired, the message now states a permanently-rejected
#    record FAILS THE TASK (the actual quarantine() behavior) rather than the stale
#    "logged and skipped". No behavior change; sink-jar log string only.
# v10 fixes the ACTUAL cause of the typetest contiguous-gap data loss: the offset
#    seeder silently failed, so the source connector started with NO seeded offset
#    and skipped every change between the Full Load watermark and CDC start. Two
#    coupled bugs in deploy/cdc-stack/lambda/seeder.py + its IAM role:
#    (1) kafka-python enables the idempotent producer under acks=all, which sends
#        InitProducerId -- MSK rejected it with ClusterAuthorizationFailedError
#        (Error 31) because OffsetSeederRole lacked kafka-cluster:WriteDataIdempotently;
#    (2) _produce() only called flush() and never checked the send Future, so the
#        failed produce was SWALLOWED and the custom resource reported SUCCESS.
#    v10: seeder disables idempotence (enable_idempotence=False) AND blocks on
#    future.get() so a failed produce RAISES -> the custom resource reports FAILED
#    (loud) instead of deploying a connector that skips the handoff. The IAM role
#    also grants WriteDataIdempotently (cluster-scoped) defensively. Seeder-zip +
#    stack IAM change. (This, not the v9 sink change, was the gapless-handoff bug.)
# v9 hardens the sink against connection drops (defense-in-depth, NOT the gap cause).
#    isTransient() only treated SQLSTATE 40001/08* as retryable; a connection torn
#    down mid-batch (DSQL idle close / IAM-token expiry / worker recycle) can surface
#    from pgjdbc with a NULL SQLSTATE or class 57 -> was mis-classified PERMANENT and
#    quarantined. v9 also treats null-SQLSTATE/class-57/JDBC connection-exception
#    subclasses as transient (rethrow -> Connect replays the same offsets; apply is
#    idempotent), discards a failed connection so the retry truly reconnects, and
#    THROWS on quarantine-with-no-DLQ instead of silently skipping. Real robustness
#    win, but the typetest gap was the seeder (v10), not this. Sink-jar change only.
# v8 rebuilds the sink plugin so the sink converts two more CDC-path types to their
#    DSQL column types: MySQL BIT(n) (Debezium io.debezium.data.Bits little-endian
#    byte[]) -> integer, and MySQL TIME (io.debezium.time.MicroTime/Time long) ->
#    java.sql.Time. Without these the CDC rows DLQ'd ("column c_bit is of type
#    smallint but expression is of type bytea"). Sink-jar change only.
# v7 rebuilds the sink plugin so the sink binds MySQL TINYINT(1) (Debezium delivers
#    it as a plain INT16) to a DSQL boolean column via ParameterMetaData, matching
#    the Full Load TINYINT(1)->boolean mapping. Without it every CDC row for a table
#    with a boolean column was DLQ'd ("column is of type boolean but expression is
#    of type smallint"). Sink-jar change only; the debezium plugin is unchanged.
# v6 adds the automatic gapless offset seed: the SOURCE WorkerConfiguration now
#    pins offset.storage.topic=<stack>-debezium-source-offsets and an in-VPC Lambda
#    custom resource creates+seeds that compacted topic BEFORE the source connector
#    is created. The custom-named source WorkerConfiguration is immutable, so its
#    PropertiesFileContent change forces a rename -> version bump.
# v5 rebuilds the sink plugin so a DSQL-apply quarantine logs the rendered SQL
#    TEMPLATE (column names + `?` placeholders, no values -- Property 7) alongside
#    the failure reason, surfacing it in the DLQ panel / activity log. Sink-jar
#    change only; the debezium plugin is unchanged.
# v4 reverts to the spike's proven configuration: the built-in JSON converter
#    (schemas.enable=true) instead of the Glue Avro converter, so NEITHER plugin
#    bundles the ~59 MiB Glue converter jar (debezium zip 102->31 MiB, sink 64->10
#    MiB) and aws-msk-iam-auth is NOT bundled (its older shaded SDK conflicted with
#    msk-config-providers -> NoSuchFieldError AUTH_SCHEME_PROVIDER). The MSK Connect
#    3.7.x runtime provides the JSON converter and IAMLoginModule.
# v3 (defunct) bundled aws-msk-iam-auth -> SDK conflict, never reached RUNNING.
# v2 bundled the Glue Avro converter into both plugins.
# v1 was the DebeziumTypeConverter-fix generation.
PLUGIN_VERSION = "v19"


class S3ProvisionError(RuntimeError):
    """A bucket-ensure or plugin-upload step failed."""


@dataclass(frozen=True)
class PluginUploadResult:
    """Where the plugin artifacts ended up, for the cdc-stack parameters."""

    bucket_name: str
    bucket_arn: str
    debezium_key: str
    dsql_sink_key: str
    lambda_seeder_key: str
    plugin_version: str


def build_s3_client(
    aws_profile: Optional[str], region: Optional[str]
) -> BotoSessionLike:
    """Build an S3 client from the shared session (honoring the global profile)."""
    return build_session(aws_profile).client("s3", region_name=region)


def build_sts_client(
    aws_profile: Optional[str], region: Optional[str]
) -> BotoSessionLike:
    """Build an STS client from the shared session (honoring the global profile)."""
    return build_session(aws_profile).client("sts", region_name=region)


def get_account_id(sts_client: BotoSessionLike) -> str:
    """Return the caller's AWS account id via ``sts:GetCallerIdentity``."""
    try:
        return str(sts_client.get_caller_identity()["Account"])  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001
        raise S3ProvisionError(
            f"Could not resolve the AWS account id: {str(exc).splitlines()[0]}"
        ) from exc


def plugin_bucket_name(account_id: str, region: str) -> str:
    """Return the deterministic managed-plugin bucket name for this identity."""
    return f"mysql-dsql-migrator-plugins-{account_id}-{region}"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _artifact_paths() -> tuple[Path, Path, Path]:
    """Return (debezium_zip, dsql_sink_zip, lambda_seeder_zip) under the repo root.

    Overridable via env vars (so a container image can point elsewhere):
    ``DSQL_MIGRATOR_DEBEZIUM_PLUGIN_PATH`` / ``DSQL_MIGRATOR_SINK_PLUGIN_PATH`` /
    ``DSQL_MIGRATOR_LAMBDA_SEEDER_PATH``.
    """
    import os

    root = _repo_root()
    deb = os.environ.get("DSQL_MIGRATOR_DEBEZIUM_PLUGIN_PATH") or str(
        root / _DEBEZIUM_PLUGIN_RELPATH
    )
    sink = os.environ.get("DSQL_MIGRATOR_SINK_PLUGIN_PATH") or str(
        root / _DSQL_SINK_PLUGIN_RELPATH
    )
    seeder = os.environ.get("DSQL_MIGRATOR_LAMBDA_SEEDER_PATH") or str(
        root / _LAMBDA_SEEDER_RELPATH
    )
    return Path(deb), Path(sink), Path(seeder)


def ensure_plugin_bucket(
    s3_client: BotoSessionLike, account_id: str, region: str
) -> str:
    """Return the managed plugin bucket name, creating it if absent.

    ``head_bucket`` to probe existence; on a miss, ``create_bucket`` with the
    region's quirk handled (us-east-1 rejects a ``LocationConstraint``). A bucket
    we already own is fine (idempotent); a name owned by another account, or any
    other failure, raises :class:`S3ProvisionError`.
    """
    bucket = plugin_bucket_name(account_id, region)
    try:
        s3_client.head_bucket(Bucket=bucket)  # type: ignore[attr-defined]
        return bucket  # already exists and we can reach it
    except Exception:  # noqa: BLE001 - not found / no access; try to create
        pass
    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket)  # type: ignore[attr-defined]
        else:
            s3_client.create_bucket(  # type: ignore[attr-defined]
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)
        if "BucketAlreadyOwnedByYou" in msg:
            return bucket  # race / prior create; we own it
        if "BucketAlreadyExists" in msg:
            raise S3ProvisionError(
                f"S3 bucket '{bucket}' already exists in another account. "
                "This should not happen (the account id is in the name)."
            ) from exc
        raise S3ProvisionError(
            f"Could not create plugin bucket '{bucket}': {msg.splitlines()[0]}"
        ) from exc
    return bucket


def _local_md5(path: Path) -> str:
    h = hashlib.md5()  # noqa: S324 - matching S3's non-multipart ETag, not security
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_plugin(
    s3_client: BotoSessionLike,
    bucket: str,
    key: str,
    local_path: Path,
    *,
    on_progress: Optional[Callable[[str], None]] = None,
) -> None:
    """Upload ``local_path`` to ``s3://bucket/key``, skipping when already current.

    Idempotency: ``head_object`` for the existing size + ETag. If the size matches
    and the ETag equals the local file's MD5 (non-multipart upload), skip. A
    multipart/composite ETag (contains ``-``) can't be compared to a plain MD5, so
    a size match alone is accepted as "unchanged" for our stable artifacts.
    Uploads via ``upload_file`` when available (boto3 managed multipart), else
    ``put_object`` (the injectable-fake path). Raises :class:`S3ProvisionError`.
    """
    if not local_path.is_file():
        raise S3ProvisionError(f"Plugin artifact not found on disk: {local_path}")

    def _log(msg: str) -> None:
        if on_progress is not None:
            on_progress(msg)

    local_size = local_path.stat().st_size
    try:
        head = s3_client.head_object(Bucket=bucket, Key=key)  # type: ignore[attr-defined]
        remote_size = int(head.get("ContentLength", -1))
        etag = str(head.get("ETag", "")).strip('"')
        if remote_size == local_size:
            if "-" in etag or etag == _local_md5(local_path):
                _log(f"{key}: already up to date — skipping upload.")
                return
    except Exception:  # noqa: BLE001 - object absent or head not supported; upload
        pass

    _log(f"Uploading {key} ({local_size // (1024 * 1024)} MiB)…")
    try:
        upload_file = getattr(s3_client, "upload_file", None)
        if callable(upload_file):
            upload_file(str(local_path), bucket, key)
        else:
            with local_path.open("rb") as fh:
                s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())  # type: ignore[attr-defined]
    except Exception as exc:  # noqa: BLE001
        raise S3ProvisionError(
            f"Failed to upload {key}: {str(exc).splitlines()[0]}"
        ) from exc
    _log(f"{key}: uploaded.")


def ensure_and_upload_plugins(
    s3_client: BotoSessionLike,
    sts_client: BotoSessionLike,
    region: str,
    *,
    on_progress: Optional[Callable[[str], None]] = None,
) -> PluginUploadResult:
    """Ensure the managed bucket exists and upload both plugin artifacts.

    Single entry point for the deploy job's bucket/upload stages. Returns the
    bucket ARN + object keys + plugin version for the cdc-stack parameters.
    Raises :class:`S3ProvisionError` on any unrecoverable failure.
    """
    account_id = get_account_id(sts_client)
    bucket = ensure_plugin_bucket(s3_client, account_id, region)
    deb_path, sink_path, seeder_path = _artifact_paths()
    upload_plugin(s3_client, bucket, DEBEZIUM_PLUGIN_KEY, deb_path, on_progress=on_progress)
    upload_plugin(s3_client, bucket, DSQL_SINK_PLUGIN_KEY, sink_path, on_progress=on_progress)
    upload_plugin(s3_client, bucket, LAMBDA_SEEDER_KEY, seeder_path, on_progress=on_progress)
    return PluginUploadResult(
        bucket_name=bucket,
        bucket_arn=f"arn:aws:s3:::{bucket}",
        debezium_key=DEBEZIUM_PLUGIN_KEY,
        dsql_sink_key=DSQL_SINK_PLUGIN_KEY,
        lambda_seeder_key=LAMBDA_SEEDER_KEY,
        plugin_version=PLUGIN_VERSION,
    )


def extract_secret_name(secret_id: str) -> str:
    """Return the colon-free Secrets Manager secret NAME from an ARN or a name.

    The cdc-stack needs the secret *name* (no colons) for the connector's
    ``${secretsManager:<name>:<key>}`` provider syntax, separate from the ARN. For
    a full ARN ``arn:aws:secretsmanager:<region>:<acct>:secret:<name>-<6char>`` the
    name is the segment after ``:secret:`` with the random 6-char suffix stripped.
    A plain name is returned unchanged.
    """
    secret_id = (secret_id or "").strip()
    if not secret_id.startswith("arn:"):
        return secret_id
    marker = ":secret:"
    idx = secret_id.find(marker)
    if idx < 0:
        return secret_id
    tail = secret_id[idx + len(marker):]
    # Strip the Secrets Manager random suffix: "-XXXXXX" (6 chars) at the end.
    if len(tail) > 7 and tail[-7] == "-" and tail[-6:].isalnum():
        return tail[:-7]
    return tail


__all__ = [
    "S3ProvisionError",
    "PluginUploadResult",
    "DEBEZIUM_PLUGIN_KEY",
    "DSQL_SINK_PLUGIN_KEY",
    "LAMBDA_SEEDER_KEY",
    "PLUGIN_VERSION",
    "build_s3_client",
    "build_sts_client",
    "get_account_id",
    "plugin_bucket_name",
    "ensure_plugin_bucket",
    "upload_plugin",
    "ensure_and_upload_plugins",
    "extract_secret_name",
]
