# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Application configuration loading.

This module loads runtime configuration from environment variables (and, for
the NiceGUI UI, per-session state). It deliberately keeps credential values
out of persisted/serialized configuration.

Design principles enforced here:

- Credential confidentiality (Property 7 / Requirement 9.2): credential values
  are never stored in the serialized configuration and never appear in plaintext
  in logs, reprs, or model dumps. They are represented either as opaque
  references (``SecretRef``) or as masked values (``SecretValue``).
- Environment-driven settings (Requirement 9.2): connection details and secrets
  are not hardcoded; they are supplied at runtime via environment variables or
  the UI session.

Note: source/target connection configuration models (with their secret
references) are defined in a later task. This module only establishes the
configuration-loading scaffold and the secret-handling primitives.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Mapping, Optional

from pydantic import BaseModel, Field

ENV_PREFIX = "DSQL_MIGRATOR_"


class SecretSource(str, Enum):
    """Where a secret value should be resolved from at runtime.

    Resolution itself (e.g., calling Secrets Manager) is implemented in a later
    task. This enum lets configuration describe *how* to obtain a secret without
    ever embedding the secret value.
    """

    SECRETS_MANAGER = "SECRETS_MANAGER"
    ENVIRONMENT = "ENVIRONMENT"
    SESSION = "SESSION"


class SecretRef(BaseModel):
    """An opaque reference to a secret, never the secret value itself.

    A ``SecretRef`` describes where a credential can be resolved from (for
    example, a Secrets Manager ARN or an environment variable name) but does not
    contain the plaintext credential. It is safe to log and serialize.
    """

    source: SecretSource
    locator: str = Field(
        ...,
        description=(
            "Pointer to the secret: a Secrets Manager ARN, an environment "
            "variable name, or a session key. Not the secret value."
        ),
    )

    def describe(self) -> str:
        """Return a human-readable, log-safe description of this reference."""
        return f"{self.source.value}:{self.locator}"


class SecretValue:
    """A wrapper around a resolved secret value that resists accidental disclosure.

    The wrapped value is masked in ``repr``/``str`` output so it does not leak
    through logging, f-strings, or exception messages. The real value must be
    requested explicitly via :meth:`reveal`. Instances are intentionally not
    Pydantic fields and are excluded from any serialized configuration.
    """

    __slots__ = ("_value",)

    _MASK = "***"

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("SecretValue requires a string value")
        self._value = value

    def reveal(self) -> str:
        """Return the underlying plaintext secret. Call sites must be deliberate."""
        return self._value

    def __repr__(self) -> str:
        return f"SecretValue('{self._MASK}')"

    def __str__(self) -> str:
        return self._MASK

    def __eq__(self, other: object) -> bool:
        if isinstance(other, SecretValue):
            return self._value == other._value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)

    def __bool__(self) -> bool:
        return bool(self._value)


class AppConfig(BaseModel):
    """Top-level application configuration.

    Contains only non-secret runtime settings. Credentials are resolved on demand
    through ``SecretRef``/``SecretValue`` and are never stored on this model, so
    ``AppConfig`` is always safe to serialize and log.
    """

    model_config = {"frozen": True}

    app_host: str = Field(
        default="127.0.0.1",
        description="Host/interface the NiceGUI UI binds to.",
    )
    app_port: int = Field(
        default=8080,
        ge=1,
        le=65535,
        description="Port the NiceGUI UI listens on.",
    )
    aws_region: Optional[str] = Field(
        default=None,
        description="AWS region used for boto3 clients (e.g., DSQL token generation).",
    )
    aws_profile: Optional[str] = Field(
        default=None,
        description=(
            "Optional single global AWS named profile applied to ALL AWS clients "
            "(DSQL token generation, Secrets Manager, Bedrock-runtime). Non-secret: "
            "only the profile NAME is stored, so it is safe to persist and log "
            "(consistent with Property 7). When None, the standard AWS credential "
            "chain (default chain + AWS_PROFILE) is used. Config key: "
            "DSQL_MIGRATOR_AWS_PROFILE."
        ),
    )
    job_state_path: str = Field(
        default="job_state.sqlite",
        description="Path to the local job-state store used for resumable jobs.",
    )
    job_state_bucket: Optional[str] = Field(
        default=None,
        description=(
            "Optional S3 bucket for a DURABLE job-state store. When set (the "
            "container deploy points it at the tool's managed plugin bucket), Full "
            "Load job snapshots are written to S3, so a Fargate task replacement (a "
            "redeploy) no longer loses job/resume state -- unlike job_state_path, "
            "which lives on the task's EPHEMERAL /tmp and is wiped on replacement. "
            "When None, the local SQLite path is used (local dev). Config key: "
            "DSQL_MIGRATOR_JOB_STATE_BUCKET."
        ),
    )
    activity_log_path: str = Field(
        default="migration_activity.log",
        description=(
            "Path to the structured activity log file. Each migration event "
            "(connection test, assessment, per-object schema apply, per-table "
            "Full Load outcome, CDC control-plane action) is appended as one "
            "UTC-timestamped JSON line, downloadable from the UI. Config key: "
            "DSQL_MIGRATOR_ACTIVITY_LOG_PATH."
        ),
    )
    session_state_path: str = Field(
        default="session_state.sqlite",
        description=(
            "Path to the local per-session state store. Persists each session's "
            "non-secret workbench state (workflow progress, evaluation result, "
            "generated objects, migration job linkage) so a reconnecting browser "
            "resumes where it left off after an app restart. Config key: "
            "DSQL_MIGRATOR_SESSION_STATE_PATH."
        ),
    )
    session_state_bucket: Optional[str] = Field(
        default=None,
        description=(
            "Optional S3 bucket for a DURABLE per-session state store. When set "
            "(the container deploy points it at the tool's managed plugin bucket), "
            "each session's non-secret snapshot is written to S3, so a Fargate "
            "task replacement (a redeploy) no longer loses the resume state -- "
            "unlike session_state_path, which lives on the task's EPHEMERAL disk "
            "and is wiped on replacement. When None, the local SQLite path is used "
            "(local dev). Config key: DSQL_MIGRATOR_SESSION_STATE_BUCKET."
        ),
    )
    staging_bucket: Optional[str] = Field(
        default=None,
        description=(
            "Optional S3 bucket for Full Load staging. When set, large tables are "
            "exported to this bucket via a streaming multipart upload and loaded "
            "from the s3:// URI, so a whole-table CSV never lands on the "
            "container's ephemeral disk. When None, a bounded local temp CSV is "
            "used (local dev / small tables only). Config key: "
            "DSQL_MIGRATOR_STAGING_BUCKET."
        ),
    )
    cdc_deploy_role_arn: Optional[str] = Field(
        default=None,
        description=(
            "ARN of the dedicated CDC deploy role the app assumes (sts:AssumeRole) "
            "before any cdc-stack CloudFormation/MSK/IAM operation, so the long-"
            "running app's task role does not hold those broad privileges. When "
            "None (local dev / admin creds), the deployer uses the shared profile "
            "session directly. Only the non-secret ARN is stored. Config key: "
            "DSQL_MIGRATOR_CDC_DEPLOY_ROLE_ARN."
        ),
    )
    cdc_secret_kms_key_id: Optional[str] = Field(
        default=None,
        description=(
            "Optional customer-managed KMS key id/ARN/alias used to encrypt the "
            "tool-managed CDC source-credentials secret. When None, the secret is "
            "encrypted with the account's default aws/secretsmanager AWS-managed "
            "key. Set a CMK for stricter key-access control / auditing of the "
            "production database credentials. Config key: "
            "DSQL_MIGRATOR_CDC_SECRET_KMS_KEY_ID."
        ),
    )
    cdc_seed_mode: str = Field(
        default="lambda",
        description=(
            "Who performs the CDC Kafka prep (pre-create topics + seed the "
            "connect-offsets record) before the connectors are created. 'lambda' "
            "(default) uses the in-VPC offset-seeder Lambda the cdc-stack deploys — "
            "the behavior on Fargate and local runs. 'external' has the app do the "
            "prep in-process over the MSK IAM bootstrap, for the Lambda-free "
            "'EC2 + MSK only' host that runs INSIDE the cdc-stack VPC (its user-data "
            "sets this). 'host is the mode': only that EC2 deployment sets "
            "'external'; everything else stays 'lambda'. Config key: "
            "DSQL_MIGRATOR_CDC_SEED_MODE."
        ),
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level for the application.",
    )
    validate_row_diff_sample_size: int = Field(
        default=0,
        ge=0,
        description=(
            "Dev-only: when > 0, Validation (Step 4) samples up to this many "
            "diverging primary keys for each table that does NOT match, naming "
            "WHICH rows differ (missing / extra / value-mismatch). Bounded "
            "(ORDER BY pk LIMIT N), runs only for mismatched tables at validation "
            "time, never on the migration hot path, and logs PK + checksum tokens "
            "only -- never row values (Property 7). 0 (default) disables it. "
            "Config key: DSQL_MIGRATOR_VALIDATE_ROW_DIFF_SAMPLE_SIZE."
        ),
    )
    validate_max_workers: int = Field(
        default=4,
        ge=1,
        le=32,
        description=(
            "Validation (Step 4) table-level parallelism: tables are compared "
            "concurrently across this many workers, each on its own read-only "
            "consistent-snapshot source connection + target connection. Cuts a "
            "large multi-table run's wall clock from the sum of per-table scans "
            "toward the slowest single table. 1 = sequential (historical "
            "behavior). Bounded (<=32) to protect the source from too many "
            "concurrent scans. Config key: DSQL_MIGRATOR_VALIDATE_MAX_WORKERS."
        ),
    )
    full_load_table_parallelism: int = Field(
        default=4,
        ge=1,
        le=16,
        description=(
            "Full Load (Step 3) table-level parallelism: how many tables load "
            "concurrently. The total concurrent DSQL connections is roughly "
            "full_load_table_parallelism x full_load_batch_parallelism, so raise "
            "both together with care and keep the product within DSQL's per-cluster "
            "connection quota. Bounded (<=16). Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM."
        ),
    )
    full_load_batch_parallelism: int = Field(
        default=8,
        ge=1,
        le=32,
        description=(
            "Full Load per-table batch parallelism: how many batched "
            "INSERT ... ON CONFLICT statements are in flight at once for a single "
            "table, each on its own pooled DSQL connection. Higher values raise "
            "throughput but also the optimistic-concurrency (40001) collision rate "
            "on hot key ranges; keep table x batch parallelism within the cluster "
            "connection quota. Bounded (<=32). Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM."
        ),
    )
    full_load_batch_rows: int = Field(
        default=2000,
        ge=1,
        le=3000,
        description=(
            "Full Load rows per batched write. Hard-capped at 3000 = DSQL's "
            "per-transaction row limit; the effective size is additionally clamped "
            "per table to fit DSQL's bind-parameter and per-write-transaction byte "
            "limits. Config key: DSQL_MIGRATOR_FULL_LOAD_BATCH_ROWS."
        ),
    )
    full_load_occ_max_attempts: int = Field(
        default=20,
        ge=1,
        le=100,
        description=(
            "Full Load per-batch retry budget, shared by OCC (SQLSTATE 40001) "
            "conflicts AND transient connection failures (dropped socket, TLS eof, "
            "connect timeout). Each retry leases a FRESH connection and replays the "
            "idempotent batch, so a batch rides out a transient DSQL blip / "
            "connection storm instead of failing the whole table. Raised from the "
            "generic 10 to 20 because a large-scale load runs for hours and WILL "
            "meet a transient connection storm at a high-parallelism transition; "
            "with exponential backoff this spans ~70s of retrying. Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_OCC_MAX_ATTEMPTS."
        ),
    )
    full_load_source_retry_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            "How many times Full Load re-reads a table whose SOURCE connection "
            "dropped mid-load (e.g. an Aurora failover: writer promotion during "
            "patching, an instance replacement, or an AZ event). 1 = no retry (fail "
            "the table for a manual re-run). The retry RE-READS the table from a "
            "FRESH consistent snapshot rather than resuming the dead one, so the "
            "table stays internally consistent as-of a single point in time -- the "
            "gapless Full Load -> CDC watermark handoff depends on that. Already-"
            "written rows are skipped by the idempotent load, so a retry costs "
            "re-read I/O but never duplicates rows. Only CONNECTION-level failures "
            "retry; a data/schema error fails immediately. Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS."
        ),
    )
    full_load_source_retry_backoff_seconds: float = Field(
        default=15.0,
        ge=0.0,
        le=300.0,
        description=(
            "Base delay (seconds) before Full Load re-reads a table after a source "
            "connection drop, doubling per attempt (15s, 30s, 60s...). An Aurora "
            "failover typically completes within 30-60s, so the first wait is sized "
            "to let DNS re-point at the promoted writer before reconnecting -- "
            "retrying instantly would just fail again. Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS."
        ),
    )
    full_load_reader_shards: int = Field(
        default=1,
        ge=1,
        le=8,
        description=(
            "Full Load reader range sharding: how many concurrent readers split a "
            "LARGE single-integer-PK table's read (each streams a disjoint PK "
            "range from its own source snapshot). The single keyset reader is "
            "CPU-bound (per-row type conversion) and tops out near one core, so K "
            "readers let a big table use more cores. 1 = off (one reader, the "
            "previous behavior). Only applies to tables with a single integer PK "
            "and at least full_load_shard_min_rows estimated rows; composite/"
            "non-integer PKs and smaller tables always use one reader. Raises "
            "SOURCE read concurrency (total source readers = table_parallelism x "
            "this) -- keep it modest on a busy source. Bounded (<=8). Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS."
        ),
    )
    full_load_shard_min_rows: int = Field(
        default=1_000_000,
        ge=1,
        description=(
            "Minimum estimated row count for a table to be reader-range-sharded "
            "(see full_load_reader_shards). Below this, sharding's extra "
            "connection/snapshot overhead isn't worth it, so the table uses one "
            "reader. Uses the scan-free information_schema estimate. Config key: "
            "DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS."
        ),
    )
    cdc_sink_mcu_count: int = Field(
        default=4,
        ge=1,
        le=8,
        description=(
            "MSK Connect MCUs (1 vCPU + 4 GiB each) per worker for the CDC SINK "
            "connector -- the cdc-stack's SinkMcuCount parameter. Must be one of "
            "1 / 2 / 4 / 8 (the template's AllowedValues). Unlike the Full Load "
            "knobs this is NOT re-read per run: the value is passed to "
            "CloudFormation when Start CDC creates/updates the sink connector, so "
            "a change applies at the next Start CDC. The sink is the CPU-bound half "
            "of the pipeline (the single-task Debezium source has spare CPU), so "
            "this is the knob to raise when the sink cannot keep up. Config key: "
            "DSQL_MIGRATOR_CDC_SINK_MCU_COUNT."
        ),
    )
    activity_log_to_stdout: bool = Field(
        default=False,
        description=(
            "When true, also emit each activity-log event to stdout as a JSON "
            "line in addition to the rotating file. On ECS the container's "
            "awslogs driver forwards stdout to CloudWatch Logs, giving a "
            "durable, queryable copy of the audit trail that survives task "
            "replacement (the rotating file lives on ephemeral storage). "
            "Off by default. Config key: DSQL_MIGRATOR_ACTIVITY_LOG_STDOUT."
        ),
    )


def _read(env: Mapping[str, str], name: str) -> Optional[str]:
    """Read a prefixed environment variable, returning None when unset/blank."""
    raw = env.get(f"{ENV_PREFIX}{name}")
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def load_config(env: Optional[Mapping[str, str]] = None) -> AppConfig:
    """Load :class:`AppConfig` from environment variables.

    Environment variables are read with the ``DSQL_MIGRATOR_`` prefix, e.g.
    ``DSQL_MIGRATOR_APP_PORT``. Unset values fall back to model defaults. No
    credential values are read into the returned configuration.
    """
    source = os.environ if env is None else env

    values: dict[str, object] = {}
    if (app_host := _read(source, "APP_HOST")) is not None:
        values["app_host"] = app_host
    if (app_port := _read(source, "APP_PORT")) is not None:
        values["app_port"] = int(app_port)
    if (aws_region := _read(source, "AWS_REGION")) is not None:
        values["aws_region"] = aws_region
    if (aws_profile := _read(source, "AWS_PROFILE")) is not None:
        values["aws_profile"] = aws_profile
    if (job_state_path := _read(source, "JOB_STATE_PATH")) is not None:
        values["job_state_path"] = job_state_path
    if (job_state_bucket := _read(source, "JOB_STATE_BUCKET")) is not None:
        values["job_state_bucket"] = job_state_bucket
    if (activity_log_path := _read(source, "ACTIVITY_LOG_PATH")) is not None:
        values["activity_log_path"] = activity_log_path
    if (session_state_path := _read(source, "SESSION_STATE_PATH")) is not None:
        values["session_state_path"] = session_state_path
    if (session_state_bucket := _read(source, "SESSION_STATE_BUCKET")) is not None:
        values["session_state_bucket"] = session_state_bucket
    if (staging_bucket := _read(source, "STAGING_BUCKET")) is not None:
        values["staging_bucket"] = staging_bucket
    if (cdc_deploy_role_arn := _read(source, "CDC_DEPLOY_ROLE_ARN")) is not None:
        values["cdc_deploy_role_arn"] = cdc_deploy_role_arn
    if (cdc_secret_kms := _read(source, "CDC_SECRET_KMS_KEY_ID")) is not None:
        values["cdc_secret_kms_key_id"] = cdc_secret_kms
    if (cdc_seed_mode := _read(source, "CDC_SEED_MODE")) is not None:
        # Normalize case so "External"/"EXTERNAL" match run_cdc_start's == "external".
        values["cdc_seed_mode"] = cdc_seed_mode.lower()
    if (log_level := _read(source, "LOG_LEVEL")) is not None:
        values["log_level"] = log_level.upper()
    if (row_diff := _read(source, "VALIDATE_ROW_DIFF_SAMPLE_SIZE")) is not None:
        values["validate_row_diff_sample_size"] = int(row_diff)
    if (max_workers := _read(source, "VALIDATE_MAX_WORKERS")) is not None:
        values["validate_max_workers"] = int(max_workers)
    if (fl_table_par := _read(source, "FULL_LOAD_TABLE_PARALLELISM")) is not None:
        values["full_load_table_parallelism"] = int(fl_table_par)
    if (fl_batch_par := _read(source, "FULL_LOAD_BATCH_PARALLELISM")) is not None:
        values["full_load_batch_parallelism"] = int(fl_batch_par)
    if (fl_batch_rows := _read(source, "FULL_LOAD_BATCH_ROWS")) is not None:
        values["full_load_batch_rows"] = int(fl_batch_rows)
    if (fl_occ := _read(source, "FULL_LOAD_OCC_MAX_ATTEMPTS")) is not None:
        values["full_load_occ_max_attempts"] = int(fl_occ)
    if (fl_src_retry := _read(source, "FULL_LOAD_SOURCE_RETRY_ATTEMPTS")) is not None:
        values["full_load_source_retry_attempts"] = int(fl_src_retry)
    if (
        fl_src_backoff := _read(source, "FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS")
    ) is not None:
        values["full_load_source_retry_backoff_seconds"] = float(fl_src_backoff)
    if (fl_shards := _read(source, "FULL_LOAD_READER_SHARDS")) is not None:
        values["full_load_reader_shards"] = int(fl_shards)
    if (fl_shard_min := _read(source, "FULL_LOAD_SHARD_MIN_ROWS")) is not None:
        values["full_load_shard_min_rows"] = int(fl_shard_min)
    if (cdc_sink_mcu := _read(source, "CDC_SINK_MCU_COUNT")) is not None:
        values["cdc_sink_mcu_count"] = int(cdc_sink_mcu)
    if (to_stdout := _read(source, "ACTIVITY_LOG_STDOUT")) is not None:
        # Accept the common truthy spellings; anything else is treated as false.
        values["activity_log_to_stdout"] = to_stdout.lower() in (
            "1",
            "true",
            "yes",
            "on",
        )

    return AppConfig(**values)


# --- Runtime-tunable performance knobs -------------------------------------
# Integer knobs an operator can retune at runtime -- no redeploy/restart -- by
# setting the corresponding os.environ key. The UI's "Performance tuning" control
# uses these helpers so the same bounds (the AppConfig field ge/le) are the single
# source of truth; bounds are read from the Pydantic field metadata rather than
# duplicated here.
#
# Two DIFFERENT kinds of knob live here, which is why ``applies`` exists:
#   * Full Load / Validation -- read FRESH from the environment on every run (via
#     load_config()), so a change lands on the next run of that step.
#   * CDC -- NOT re-read per run. The value is a CloudFormation parameter passed
#     when Start CDC creates/updates the connectors, so a change lands at the next
#     Start CDC (and, for a pipeline already streaming, only after Stop + Start).
# Presenting both as "applies to the next run" would be wrong for the CDC group,
# so each knob states its own timing and the UI renders it per section.
_APPLIES_NEXT_RUN = "the next run"
_APPLIES_NEXT_CDC_START = "the next Start CDC"


@dataclass(frozen=True)
class TunableKnob:
    """A runtime-adjustable integer performance knob (env-backed).

    Carries the metadata the UI renders as an AWS Console (Cloudscape) "form
    field": ``group`` is the section it belongs to (e.g. "Full Load"),
    ``short_label`` is its field label within that section, and ``description``
    is the one-line helper text under the label. ``label`` (the fully-qualified
    name used in notifications / error messages) is derived from the two so the
    UI and the messages can never drift apart.

    ``applies`` is when a change takes effect (see the module note above) -- the
    UI shows it per section and the confirmation notification repeats it, so a CDC
    knob never implies it will affect a run already in progress.

    ``allowed``, when non-empty, is the EXACT set of legal values (not just a
    range): the cdc-stack declares ``SinkMcuCount`` with CloudFormation
    ``AllowedValues: [1, 2, 4, 8]``, so 3 would pass a 1..8 range check here and
    then be rejected by CloudFormation minutes into a billable deploy. The UI
    renders these as a dropdown instead of a free number field.
    """

    field: str  # AppConfig attribute name
    env_suffix: str  # env key sans the DSQL_MIGRATOR_ prefix
    group: str  # form section this knob belongs to ("Full Load" / "CDC" / ...)
    short_label: str  # field label within its group, e.g. "Tables in parallel"
    description: str  # one-line Cloudscape form-field helper text
    applies: str = _APPLIES_NEXT_RUN  # when a change takes effect
    allowed: tuple[int, ...] = ()  # exact legal values ( () = any in range )
    help_text: str = ""  # optional deeper guidance for the field's info tooltip

    @property
    def label(self) -> str:
        """Fully-qualified label for notifications / error messages.

        Derived from ``group`` + ``short_label`` (single source of truth) so it
        stays in sync with the UI, e.g. ``"Full Load — tables in parallel"``.
        """
        return f"{self.group} — {self.short_label.lower()}"

    @property
    def env_key(self) -> str:
        return f"{ENV_PREFIX}{self.env_suffix}"

    @property
    def minimum(self) -> int:
        return _field_bound(self.field, "ge", 1)

    @property
    def maximum(self) -> int:
        return _field_bound(self.field, "le", 1_000_000)


TUNABLE_KNOBS: tuple[TunableKnob, ...] = (
    TunableKnob(
        "full_load_table_parallelism",
        "FULL_LOAD_TABLE_PARALLELISM",
        "Full Load",
        "Tables in parallel",
        "How many source tables are loaded at the same time.",
    ),
    TunableKnob(
        "full_load_batch_parallelism",
        "FULL_LOAD_BATCH_PARALLELISM",
        "Full Load",
        "Batches per table",
        "Concurrent row batches loaded per table.",
    ),
    TunableKnob(
        "full_load_batch_rows",
        "FULL_LOAD_BATCH_ROWS",
        "Full Load",
        "Rows per batch",
        "Rows per INSERT batch (DSQL caps a transaction at 3000 rows).",
    ),
    # CDC before Validation: this tuple's group order IS the Settings tab order, and it
    # should follow the migration journey the operator is working through (Full Load ->
    # CDC -> Validation), not the order the knobs happened to be added. CDC also pairs
    # with Full Load -- both are data-movement throughput -- while Validation is the
    # after-the-fact check.
    TunableKnob(
        "cdc_sink_mcu_count",
        "CDC_SINK_MCU_COUNT",
        "CDC",
        "Sink compute (MCU)",
        "MSK Connect units per sink worker (1 MCU = 1 vCPU + 4 GiB). The sink is "
        "the CPU-bound half of CDC — raise this, not the source, when it lags.",
        applies=_APPLIES_NEXT_CDC_START,
        allowed=(1, 2, 4, 8),
        # The one knob here whose guidance does not fit a one-line description: it is a
        # CloudFormation parameter (so the timing is unlike every other knob), it costs
        # money, its ceiling is an AWS API limit rather than our choice, and raising it is
        # only the right move for a specific symptom. Kept out of `description` so the
        # row stays scannable, and out of the manual-only so it is answerable in place.
        help_text=(
            "When to raise it: the sink is behind (CDC lag growing) while the source "
            "keeps up. The sink is CPU-bound; the single-task Debezium source has spare "
            "CPU, so raising the SOURCE MCUs instead buys nothing.\n\n"
            "1 MCU = 1 vCPU + 4 GiB per worker. 8 is the ceiling — the MSK Connect API "
            "accepts only 1 / 2 / 4 / 8 — and each step up increases the MSK Connect "
            "bill for as long as the connector runs.\n\n"
            "Takes effect at the next Start CDC, because it is a cdc-stack "
            "CloudFormation parameter rather than something the app re-reads. A pipeline "
            "already streaming keeps its current capacity until you run Start CDC again; "
            "doing so purely to resize is safe — connector capacity updates in place, so "
            "the sink is resized rather than recreated, with no gap in replication and no "
            "MSK partition-quota cost."
        ),
    ),
    TunableKnob(
        "validate_max_workers",
        "VALIDATE_MAX_WORKERS",
        "Validation",
        "Tables in parallel",
        "How many tables are checksummed at the same time.",
    ),
)

_TUNABLE_BY_FIELD = {k.field: k for k in TUNABLE_KNOBS}


def _field_bound(field: str, kind: str, fallback: int) -> int:
    """Read a ge/le bound off an AppConfig field's Pydantic metadata."""
    info = AppConfig.model_fields[field]
    for meta in info.metadata:
        value = getattr(meta, kind, None)
        if value is not None:
            return int(value)
    return fallback


class TuningValueError(ValueError):
    """A proposed tuning value is out of range, not allowed, or not an integer."""


def current_tuning_values() -> dict[str, int]:
    """Return the CURRENTLY effective value of each tunable knob (from config)."""
    cfg = load_config()
    return {k.field: int(getattr(cfg, k.field)) for k in TUNABLE_KNOBS}


def tunable_groups() -> tuple[tuple[str, tuple[TunableKnob, ...]], ...]:
    """Group :data:`TUNABLE_KNOBS` into ``(group_name, knobs)`` in declared order.

    The UI renders one labelled section per group. Grouping here (rather than in the
    render loop) keeps the sections a property of the registry: adding a knob to a
    new group makes the section appear with no UI change, and a group's knobs cannot
    be split across two headers by a mis-ordered tuple -- the previous "emit a header
    when the group changes" loop silently did exactly that.
    """
    grouped: dict[str, list[TunableKnob]] = {}
    for knob in TUNABLE_KNOBS:
        grouped.setdefault(knob.group, []).append(knob)
    return tuple((name, tuple(knobs)) for name, knobs in grouped.items())


def group_applies(group: str) -> str:
    """When a change to any knob in ``group`` takes effect (e.g. ``"the next run"``).

    Every knob in a group must agree, so the UI can state the timing ONCE per
    section instead of per field. Raises :class:`TuningValueError` for an unknown
    group, or when a group mixes timings (a registry mistake that would otherwise
    show one arbitrary knob's timing as if it covered the whole section).
    """
    timings = {k.applies for k in TUNABLE_KNOBS if k.group == group}
    if not timings:
        raise TuningValueError(f"unknown tuning group: {group}")
    if len(timings) > 1:
        raise TuningValueError(
            f"tuning group '{group}' mixes apply-timings: {sorted(timings)}"
        )
    return timings.pop()


def set_tuning_value(field: str, value: int) -> int:
    """Validate ``value`` against the knob's bounds and set it in ``os.environ``.

    The new value is picked up without a restart, but WHEN depends on the knob (see
    ``TunableKnob.applies``): a Full Load / Validation knob is re-read by the next
    run, while a CDC knob is a CloudFormation parameter read at the next Start CDC.
    App-wide (single-task) and reset to the deploy/startup value on restart. Returns
    the value set. Raises :class:`TuningValueError` if not an integer, out of range,
    or -- for a knob with an ``allowed`` set -- not one of those exact values.
    """
    knob = _TUNABLE_BY_FIELD.get(field)
    if knob is None:
        raise TuningValueError(f"unknown tuning knob: {field}")
    try:
        ivalue = int(value)
    except (TypeError, ValueError):
        raise TuningValueError(f"{knob.label}: value must be an integer") from None
    if ivalue < knob.minimum or ivalue > knob.maximum:
        raise TuningValueError(
            f"{knob.label}: must be between {knob.minimum} and {knob.maximum}"
        )
    # An enum-valued knob (CloudFormation AllowedValues) must be rejected HERE. A
    # value inside the range but off the enum (e.g. 3 MCU) would otherwise be stored,
    # then fail the cdc-stack update minutes into a billable Start CDC.
    if knob.allowed and ivalue not in knob.allowed:
        raise TuningValueError(
            f"{knob.label}: must be one of "
            + " / ".join(str(v) for v in knob.allowed)
        )
    os.environ[knob.env_key] = str(ivalue)
    return ivalue


@dataclass(frozen=True)
class ConnectDefaults:
    """Optional, dev-only prefill values for the Connect screen.

    These populate the Connect form so a developer does not have to retype
    connection details every session. They are read from the local (gitignored)
    ``.env`` / environment and are never persisted by the app: the source
    password is held only as a masked :class:`SecretValue` and, like all
    credentials, stays in process memory (Property 7). All fields are optional;
    an unset value leaves the corresponding form field at its normal blank /
    default state.
    """

    source_host: Optional[str] = None
    source_port: Optional[int] = None
    source_database: Optional[str] = None
    source_username: Optional[str] = None
    source_password: Optional[SecretValue] = None
    target_endpoint: Optional[str] = None
    target_region: Optional[str] = None
    target_database: Optional[str] = None
    target_username: Optional[str] = None
    bedrock_model_id: Optional[str] = None
    bedrock_region: Optional[str] = None


def read_env_file(path: str) -> dict[str, str]:
    """Parse a simple ``KEY=VALUE`` ``.env`` file into a dict.

    Comment lines (``#``) and blank lines are ignored. This is a best-effort,
    dependency-free reader for local development: a missing or unreadable file
    yields an empty dict rather than raising.
    """
    values: dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    except OSError:
        return {}
    return values


def _read_plain(env: Mapping[str, str], name: str) -> Optional[str]:
    """Read an unprefixed env var, returning None when unset/blank."""
    raw = env.get(name)
    if raw is None:
        return None
    raw = raw.strip()
    return raw or None


def load_connect_defaults(
    env: Optional[Mapping[str, str]] = None,
) -> ConnectDefaults:
    """Load optional Connect prefill defaults from the environment (dev only).

    Source fields reuse the existing ``DB_*`` variables (the same source MySQL
    the seed script targets), so a developer's current ``.env`` prefills the
    source with no extra setup. The source database name (``DB_NAME``) is
    intentionally NOT prefilled, since a specific schema is selected later in the
    workflow. Target fields use ``TARGET_*`` variables and the AI-assist model id
    uses ``BEDROCK_MODEL_ID`` / ``BEDROCK_REGION``. Every value is optional; the
    source password is wrapped in :class:`SecretValue` so it is never logged in
    plaintext (Property 7).
    """
    source = os.environ if env is None else env
    port_raw = _read_plain(source, "DB_PORT")
    password_raw = _read_plain(source, "DB_PASSWORD")
    return ConnectDefaults(
        source_host=_read_plain(source, "DB_HOST"),
        source_port=int(port_raw) if port_raw and port_raw.isdigit() else None,
        source_username=_read_plain(source, "DB_USER"),
        source_password=SecretValue(password_raw) if password_raw else None,
        target_endpoint=_read_plain(source, "TARGET_ENDPOINT"),
        target_region=_read_plain(source, "TARGET_REGION"),
        target_database=_read_plain(source, "TARGET_DATABASE"),
        target_username=_read_plain(source, "TARGET_USERNAME"),
        bedrock_model_id=_read_plain(source, "BEDROCK_MODEL_ID"),
        bedrock_region=_read_plain(source, "BEDROCK_REGION"),
    )


def resolve_secret(
    ref: SecretRef,
    env: Optional[Mapping[str, str]] = None,
) -> SecretValue:
    """Resolve a :class:`SecretRef` into a masked :class:`SecretValue`.

    Only environment-variable resolution is implemented at this scaffolding
    stage; Secrets Manager and session resolution are completed in later tasks.
    The resolved value is wrapped in :class:`SecretValue` so it cannot be logged
    in plaintext, and it is never written back into :class:`AppConfig`.
    """
    if ref.source is SecretSource.ENVIRONMENT:
        source = os.environ if env is None else env
        raw = source.get(ref.locator)
        if raw is None:
            raise KeyError(
                f"environment variable for secret '{ref.describe()}' is not set"
            )
        return SecretValue(raw)

    raise NotImplementedError(
        f"secret resolution for source '{ref.source.value}' is not implemented yet"
    )


__all__ = [
    "ENV_PREFIX",
    "SecretSource",
    "SecretRef",
    "SecretValue",
    "AppConfig",
    "load_config",
    "TunableKnob",
    "TUNABLE_KNOBS",
    "TuningValueError",
    "current_tuning_values",
    "set_tuning_value",
    "ConnectDefaults",
    "read_env_file",
    "load_connect_defaults",
    "resolve_secret",
]
