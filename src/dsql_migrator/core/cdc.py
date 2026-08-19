# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Optional CDC catch-up mode: interface/stub (Requirement 5.5, optional goal).

This module defines the *interface* for an opt-in, binlog-based Change Data
Capture (CDC) catch-up mode that would run after the initial snapshot load to
reduce cut-over lag (design.md "CDC (선택적 목표)"). Requirement 5.5 marks this
as an **optional goal**, so per the project's product principles ("no bloat —
only necessary features"), this module is intentionally a stub: it fixes the
data contracts and the resume-from-watermark behavior, but does not implement a
live binlog event loop.

Resume-from-watermark contract (Property 11 / Requirement 5.5): a real CDC run
resumes from the *exact* consistency point captured at export start by
:mod:`dsql_migrator.core.watermark` -- the binlog coordinates
(``binlog_file:binlog_position``) and/or the GTID set (``gtid_executed``) recorded
on the :class:`~dsql_migrator.core.models.Watermark`. :class:`CdcResumePoint`
makes that starting point explicit so the catch-up begins precisely where the
snapshot ended, with no gap and no overlap.

Read-only source (Property 1): CDC reads the MySQL binary log only; it never
writes to or alters the source. The (future) backend streams binlog events and
applies the resulting changes to the *target* DSQL cluster, leaving the source
untouched.

Dependency note (deliberate, "no bloat"): the design names
``python-mysql-replication`` as the binlog reader for a real implementation.
Because this is a behind-a-flag stub, that package is **not** added as a
dependency and is **not** imported here -- importing this module never requires
it. Enabling a real CDC backend in the future would install
``python-mysql-replication`` and stream events starting from a
:class:`CdcResumePoint`; until then, an enabled :meth:`CdcCatchUp.start` returns
a structured :attr:`CdcStatus.NOT_IMPLEMENTED` result that carries the resume
point it *would* have used.

Result shape for the UI: :class:`CdcResult` mirrors what the design wants the UI
to show ("caught up to <time> / lag"): a status, the resume point, an optional
"caught up to" timestamp, and an optional lag in seconds. Only the data shape is
provided here -- there is no live implementation behind it yet.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import (
    DataErrorRecord,
    ErrorLogSummary,
    LoadKind,
    LoadStatusView,
    SchemaDriftSummary,
    SourceConnectionConfig,
    TableDef,
    TableStatusRow,
    TargetConnectionConfig,
    Watermark,
)


class CdcStatus(str, Enum):
    """Outcome status of a CDC catch-up attempt.

    ``DISABLED`` is returned when the opt-in flag is off (the default), so the
    caller can treat CDC as a no-op. ``NOT_IMPLEMENTED`` is returned when CDC is
    enabled but no binlog backend is wired in yet -- the stub state for this
    optional goal. ``CAUGHT_UP`` is reserved for a future real implementation
    that has applied all changes up to a target point.
    """

    DISABLED = "DISABLED"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    CAUGHT_UP = "CAUGHT_UP"


class CdcResumePoint(BaseModel):
    """The exact binlog/GTID coordinates a CDC run resumes from (Property 11).

    Built from the :class:`~dsql_migrator.core.models.Watermark` captured at
    export start so catch-up begins precisely where the snapshot ended. A real
    backend prefers the GTID set when present (more robust than file:position)
    and otherwise uses the binlog ``file:position`` coordinate.
    """

    model_config = ConfigDict(extra="forbid")

    binlog_file: Optional[str] = Field(
        default=None, description="MySQL binlog file name to resume from."
    )
    binlog_position: Optional[int] = Field(
        default=None, ge=0, description="Position within the binlog file to resume from."
    )
    gtid_executed: Optional[str] = Field(
        default=None,
        description="GTID set ('@@GLOBAL.gtid_executed') to resume from, if available.",
    )
    server_uuid: Optional[str] = Field(
        default=None,
        description="Source server UUID the coordinates belong to, if available.",
    )

    @classmethod
    def from_watermark(cls, watermark: Watermark) -> "CdcResumePoint":
        """Derive the resume coordinates from an export-start watermark.

        Copies the binlog coordinates, GTID set, and ``server_uuid`` recorded on
        the watermark so a CDC run can resume from the same consistency point
        (Property 11). Coordinates absent from the watermark stay ``None``.
        """
        return cls(
            binlog_file=watermark.binlog_file,
            binlog_position=watermark.binlog_position,
            gtid_executed=watermark.gtid_executed,
            server_uuid=watermark.server_uuid,
        )

    def has_coordinates(self) -> bool:
        """Return True when at least one usable resume coordinate is present.

        A real CDC run requires either a GTID set or a binlog ``file:position``
        to know where to start; without them the source's binlog/GTID metadata
        was unavailable at export time and CDC cannot resume.

        NOTE: this is the broad "something to resume from" test. It is NOT sufficient
        for the automatic gapless handoff -- see :meth:`can_seed_offset`.
        """
        has_binlog = self.binlog_file is not None and self.binlog_position is not None
        return bool(self.gtid_executed) or has_binlog

    def can_seed_offset(self) -> bool:
        """True when these coordinates can actually seed the CDC start offset.

        The gapless handoff works by writing the Full Load's position into MSK's
        ``connect-offsets`` topic before the source connector starts. That offset record
        is keyed on the binlog ``file`` + ``pos`` -- the in-VPC seeder REJECTS a watermark
        without them (``"watermark has no binlog file:position; cannot seed offset"``),
        and ``build_watermark_params`` returns all-empty parameters, which makes the
        template skip the seeder entirely so the connector starts from the CURRENT binlog.

        A GTID set alone therefore does NOT give a gapless start: it is optional
        reinforcement the seeder adds to the offset when present, not a substitute for the
        coordinate it is keyed on. Keeping this distinct from
        :meth:`has_coordinates` is what stops the UI promising "gapless from Full Load"
        for a watermark that would silently resume from the live binlog and lose every
        change made during the load.

        This case is reachable, not theoretical: the two coordinates are read by SEPARATE
        queries that degrade independently -- ``SHOW MASTER STATUS`` needs
        ``REPLICATION CLIENT`` (commonly restricted on RDS/Aurora) while
        ``@@GLOBAL.gtid_executed`` is a plain global read.
        """
        return self.binlog_file is not None and self.binlog_position is not None


class CdcOptions(BaseModel):
    """Opt-in configuration for the CDC catch-up mode (Requirement 5.5).

    CDC is an optional goal, so it is **disabled by default** (``enabled=False``),
    matching the opt-in pattern used elsewhere in the design. Kept intentionally
    minimal: a real backend would add its own tuning here, but no speculative
    settings are introduced for a stub.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        default=False,
        description="Opt-in flag for binlog-based CDC catch-up; disabled by default.",
    )


class CdcResult(BaseModel):
    """Structured outcome of a CDC catch-up attempt (UI/data shape only).

    Carries the :attr:`status`, the :class:`CdcResumePoint` the run started from
    (so the UI can show where catch-up resumed), and -- for a future real run --
    a ``caught_up_to`` timestamp and ``lag_seconds`` so the UI can display
    "caught up to <time> / lag" as the design describes. ``detail`` is a
    human-readable, log-safe message.
    """

    model_config = ConfigDict(extra="forbid")

    status: CdcStatus
    resume_point: Optional[CdcResumePoint] = Field(
        default=None,
        description="The watermark-derived coordinates the run resumed from.",
    )
    caught_up_to: Optional[datetime] = Field(
        default=None,
        description="Point-in-time CDC has applied up to (reserved for a real run).",
    )
    lag_seconds: Optional[float] = Field(
        default=None,
        ge=0.0,
        description="Replication lag in seconds (reserved for a real run).",
    )
    detail: str = Field(
        default="",
        description="Human-readable, log-safe status message.",
    )


# Documented integration note surfaced to callers when CDC is enabled but no
# binlog backend is wired in. Kept as a constant so the message is consistent
# across the stub result and any future log lines.
_NOT_IMPLEMENTED_DETAIL = (
    "CDC catch-up is enabled but not implemented in this build. A real backend "
    "would stream MySQL binlog events with 'python-mysql-replication' (read-only "
    "on the source, Property 1), resuming from the watermark coordinates "
    "(GTID set or binlog file:position) and applying changes to the target."
)

_NO_COORDINATES_DETAIL = (
    "CDC cannot resume: the export watermark has no GTID set or binlog "
    "file:position (source binlog/GTID metadata was unavailable at export time)."
)


class CdcCatchUp:
    """Interface for the optional binlog-based CDC catch-up mode (stub).

    Construct with :class:`CdcOptions` plus the source and target connection
    configs, then call :meth:`start` with the export-start
    :class:`~dsql_migrator.core.models.Watermark`. When CDC is disabled (the
    default), :meth:`start` is a no-op returning a :attr:`CdcStatus.DISABLED`
    result. When enabled, it returns a structured :attr:`CdcStatus.NOT_IMPLEMENTED`
    result documenting the intended ``python-mysql-replication`` integration and
    the resume point it would have used (Property 11). The source is only ever
    read (Property 1).
    """

    def __init__(
        self,
        options: CdcOptions,
        *,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
    ) -> None:
        """Create a catch-up coordinator.

        ``source`` is the read-only MySQL source whose binlog would be streamed;
        ``target`` is the DSQL cluster changes would be applied to. Neither is
        contacted by this stub.
        """
        self._options = options
        self._source = source
        self._target = target

    @property
    def options(self) -> CdcOptions:
        """Return the CDC options this coordinator was built with."""
        return self._options

    def start(self, watermark: Watermark) -> CdcResult:
        """Begin CDC catch-up from the export-start ``watermark`` (stub).

        Behavior:

        - When :attr:`CdcOptions.enabled` is ``False`` (default): no-op, returns a
          :attr:`CdcStatus.DISABLED` result. Nothing reads or writes anything.
        - When enabled: derives the :class:`CdcResumePoint` from ``watermark``
          (Property 11) and returns a :attr:`CdcStatus.NOT_IMPLEMENTED` result
          carrying that resume point. The result documents that a real backend
          would stream binlog events from this point using
          ``python-mysql-replication`` (read-only source, Property 1) and apply
          changes to the target. If the watermark lacks usable coordinates, the
          result's ``detail`` explains that CDC could not resume.

        This method never writes to the source (Property 1) and -- as a stub --
        does not contact the source or target at all.
        """
        if not self._options.enabled:
            return CdcResult(
                status=CdcStatus.DISABLED,
                detail="CDC catch-up is disabled (opt-in mode, Requirement 5.5).",
            )

        resume_point = CdcResumePoint.from_watermark(watermark)
        detail = _NOT_IMPLEMENTED_DETAIL
        if not resume_point.has_coordinates():
            detail = f"{_NOT_IMPLEMENTED_DETAIL} {_NO_COORDINATES_DETAIL}"

        return CdcResult(
            status=CdcStatus.NOT_IMPLEMENTED,
            resume_point=resume_point,
            caught_up_to=watermark.snapshot_timestamp,
            detail=detail,
        )


# ---------------------------------------------------------------------------
# CDC pipeline orchestration (managed Debezium + MSK + custom DSQL Sink Connector)
# ---------------------------------------------------------------------------
#
# Large-scale, continuous CDC (Requirement 12) runs as a pipeline whose data plane
# is Kafka Connect connectors on managed MSK Connect (design Decision Log 결정
# 변경 8): a Debezium MySQL source connector -> Amazon MSK -> our custom DSQL Sink
# Connector plugin -> Aurora DSQL. This tool is the **control plane**: it builds
# connector configs, seeds the source start offset from the Full Load watermark
# for a gapless handoff, monitors connector/task status, and surfaces connector/
# DLQ errors to the downloadable error log. It does NOT implement a sink consumer
# or any per-event apply loop (that is the Java connector plugin's job), and an
# LLM is never on the data plane (Req 12.15). All reads here are read-only and
# the source is never written (Property 1).


class ConnectorState(str, Enum):
    """Lifecycle state of a Kafka Connect connector/task (from DescribeConnector)."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    FAILED = "FAILED"
    UNASSIGNED = "UNASSIGNED"


class ConnectorStatus(BaseModel):
    """Read-only status of one connector for monitoring (Req 12.9).

    ``lag_seconds`` and ``caught_up_to`` (sink consumer lag / cutover readiness)
    come from managed signals (CloudWatch / Debezium ``MilliSecondsBehindSource``),
    not computed here.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    state: ConnectorState
    tasks_total: int = Field(default=0, ge=0)
    tasks_failed: int = Field(default=0, ge=0)
    lag_seconds: Optional[float] = Field(default=None, ge=0.0)
    caught_up_to: Optional[datetime] = None


class DebeziumSourceConfig(BaseModel):
    """Tool-generated config for the managed Debezium MySQL source connector.

    The selected tables map to ``table_include_list``; ``snapshot_mode`` defaults
    to ``recovery`` because the bulk loader (Full Load) already loaded the row
    data AND the start offset is seeded from the Full Load watermark. With a
    seeded connect-offset present, Debezium takes the resume path and needs an
    existing schema-history topic; ``recovery`` rebuilds that history from the
    current source tables WITHOUT re-reading rows, then resumes from the seeded
    offset (gapless -- Property 11). ``schema_only`` would die with "db history
    topic is missing" once the offset seed succeeds. See the cdc-stack template's
    source connector config for the full rationale.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    table_include_list: list[str] = Field(default_factory=list)
    snapshot_mode: Literal["recovery", "schema_only", "initial"] = "recovery"
    start_gtid: Optional[str] = None
    start_binlog_file: Optional[str] = None
    start_binlog_pos: Optional[int] = Field(default=None, ge=0)
    column_exclude_list: list[str] = Field(
        default_factory=list,
        description=(
            "Fully-qualified columns (db.table.column) dropped at capture via "
            "Debezium column.exclude.list -- oversized LOB columns whose values "
            "can exceed the Aurora DSQL 1 MiB per-value limit (spike H13). "
            "Maps to the cdc-stack ColumnExcludeList parameter. Empty = none."
        ),
    )
    message_key_columns: dict[str, list[str]] = Field(
        default_factory=dict,
        description=(
            "Per-table composite record-key override: {db.table: [leading, pk...]}. "
            "For a table whose TARGET has a composite primary key (leading column "
            "prepended), Debezium must key the change record on the SAME composite "
            "columns so the sink's ON CONFLICT / DELETE match the target key. Maps "
            "to Debezium message.key.columns; empty = key on the source PK "
            "(unchanged behavior). The leading column is read from the source "
            "row/before-image, so it must NOT be in column_exclude_list."
        ),
    )


class SinkConnectorConfig(BaseModel):
    """Tool-generated config for the custom DSQL Sink Connector plugin.

    The Python control plane only *builds* this config; the connector plugin (our
    Java artifact on MSK Connect) consumes it. Keying/idempotency knobs make the
    connector apply PK-keyed upsert/delete with DLQ quarantine (Req 12.3-12.6);
    statement-level OCC retry and <=3,000-row batching live in the plugin.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    topics: list[str] = Field(default_factory=list)
    pk_mode: Literal["record_key"] = "record_key"
    insert_mode: Literal["upsert"] = "upsert"
    delete_enabled: bool = True
    dlq_topic: str = Field(min_length=1)


class SchemaDriftKind(str, Enum):
    """The kind of source-schema drift a permanent sink rejection reveals.

    CDC does NOT propagate DDL: a source ``ALTER TABLE`` never changes the target,
    and the DDL event itself never reaches the sink. What DOES reach the sink is
    the first row written under the *new* source schema, whose column set no longer
    matches the target -- DSQL rejects it with a telltale SQLSTATE and the sink
    quarantines the row to the DLQ. Mapping that SQLSTATE back to a drift kind lets
    the tool surface "the source schema changed" instead of an opaque quarantine.
    Detection only -- the recovery (manual target ALTER, then per-table Reload to
    backfill missing rows) stays operator-driven (the tool never auto-alters the
    target: Property 6, no silent schema mutation).
    """

    ADD_COLUMN = "add-column"      # 42703 undefined_column: source added a column the target lacks
    DROP_COLUMN = "drop-column"    # 23502 not_null_violation: source dropped a column the target requires
    TYPE_CHANGE = "type-change"    # 42804 datatype_mismatch: source changed a column's type incompatibly


# SQLSTATE -> drift kind. Only STRUCTURAL rejections (tied to the row's column set
# or a column's declared type) map to drift; a per-VALUE data exception does NOT (see
# classify_schema_drift). 42804 (datatype_mismatch) is the type-change signal --
# deliberately NOT class 22 (22001/22003/…), which ordinary bad data raises.
_DRIFT_BY_SQLSTATE: dict[str, SchemaDriftKind] = {
    "42703": SchemaDriftKind.ADD_COLUMN,
    "23502": SchemaDriftKind.DROP_COLUMN,
    "42804": SchemaDriftKind.TYPE_CHANGE,
}


def classify_schema_drift(error_code: Optional[str]) -> Optional[SchemaDriftKind]:
    """Map a sink-quarantine SQLSTATE to a source-schema-drift kind, else ``None``.

    Pure: the single source of truth for "which SQLSTATE means which drift". Only the
    three STRUCTURAL SQLSTATEs map -- rejections determined by the row's COLUMN SET or
    a column's DECLARED TYPE, which a source DDL change produces:
    ``42703`` (a column the target lacks -> ADD COLUMN), ``23502`` (a NOT NULL target
    column got no value -> a dropped source column), ``42804`` (datatype_mismatch ->
    TYPE CHANGE).

    A class ``22`` data exception (``22001`` string-too-long / ``22003`` numeric range
    / ``22007`` bad datetime / ``22P02`` bad text) is deliberately NOT drift: it is a
    per-VALUE rejection that ordinary bad data raises with no DDL change at all, so
    mapping it to TYPE_CHANGE fired a false "source changed a column's type" banner on
    every oversized/out-of-range poison row. Such a code (and any other unknown /
    ``None`` code) returns ``None`` so it stays an ordinary quarantine, not drift.
    """
    if not error_code:
        return None
    code = error_code.strip().upper()
    return _DRIFT_BY_SQLSTATE.get(code)


class CdcConnectorError(BaseModel):
    """A read-only connector task failure or DLQ record for error surfacing.

    Credential-free (Property 7): ``table`` names the affected table/topic (or
    connector), ``message`` is an English reason, and ``error_code`` is an
    optional SQLSTATE-like code. ``occurred_at`` is provided by the source when
    known; otherwise the orchestrator stamps the current time. For a DLQ record
    the ``message`` may carry the failed row's primary key (``... | pk: id=14``):
    PK column names always, surrogate PK values (integer / UUID) only, and any
    natural-key value that may be sensitive withheld -- so still no arbitrary row
    values.
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    message: str = Field(min_length=1)
    error_code: Optional[str] = None
    occurred_at: Optional[datetime] = None

    @property
    def drift_kind(self) -> Optional[SchemaDriftKind]:
        """The source-schema-drift kind this error reveals (from ``error_code``),
        or ``None`` for an ordinary quarantine. Derived, not stored -- the SQLSTATE
        is the single source of truth (see :func:`classify_schema_drift`)."""
        return classify_schema_drift(self.error_code)


# Read-only suppliers of monitoring/error data. Injected so the control plane is
# unit-testable with fakes and never reaches AWS/MSK; a real wiring would read
# MSK Connect DescribeConnector + CloudWatch (status) and connector task state /
# DLQ topic (errors). Absent sources mean "no pipeline deployed yet".
StatusSource = Callable[[], Sequence[ConnectorStatus]]
ErrorSource = Callable[[], Sequence[CdcConnectorError]]


# cdc-stack deploy naming convention -- the SINGLE source of truth shared by
# the parameter generator (build_cdc_stack_params) and the connector-scoping
# filter in the UI. These must match deploy/cdc-stack/cdc-stack.yaml, where the
# connectors are named ``${AWS::StackName}-debezium-source`` / ``-dsql-sink`` and
# per-table topics are ``<TopicPrefix>.<db>.<table>`` (TopicPrefix default
# ``dsqlcdc``). Defining them once keeps "which connectors are mine" deterministic.
CDC_DEFAULT_STACK_NAME = "mysql-dsql-cdc-stack"
CDC_DEFAULT_TOPIC_PREFIX = "dsqlcdc"
CDC_DEFAULT_DLQ_TOPIC = "dsql-sink-dlq"
CDC_SOURCE_SUFFIX = "-debezium-source"
CDC_SINK_SUFFIX = "-dsql-sink"

# --- CDC throughput smart defaults ------------------------------------------
# The connector-scaling knobs (MSK Connect MCUs, sink tasks.max, per-table topic
# partition count) are NOT surfaced in the UI: partition count is IRREVERSIBLE
# (a topic's partitions can only be increased, never decreased -- and only by
# recreating the MSK cluster in practice), the knobs interact non-trivially, and
# a CDC connector change is a 15-20 min redeploy (unlike Full Load, there is no
# cheap retune loop). So the tool INFERS them from the one input it has -- the
# number of captured tables -- and an operator who truly needs to override can
# set an env var. This mirrors the product principle "infer anything that can be
# inferred instead of asking".
#
# The model is grounded in a throughput test (2026-07-08):
#   * Debezium (source) is single-task per MySQL server (one binlog stream); with
#     the v14 producer tuning it sustains ~30k rec/s and is NOT the bottleneck.
#   * The sink is DSQL-write-latency-bound. Throughput scales with the number of
#     partitions consumed in parallel (one sink task per partition), but only
#     SUBLINEARLY -- 4->8 partitions gave ~1.4x, not 2x, as concurrent upserts to
#     one table start contending in DSQL. So total effective sink parallelism is
#     capped at CDC_MAX_SINK_PARALLELISM; beyond that, more partitions mostly add
#     cost (MCU) without throughput.
#   * total effective parallelism = partitions_per_topic * num_tables (each table
#     is its own topic). When there are few tables we raise partitions_per_topic
#     to reach the cap; when there are many tables the tables themselves provide
#     the parallelism, so 1 partition each suffices.
CDC_MAX_SINK_PARALLELISM = 8  # effective sink-task ceiling before DSQL contention
CDC_DEFAULT_MCU_COUNT = 2  # MSK Connect MCUs per worker (cost-conscious default)
# Sink MCUs, sized SEPARATELY from the source (CDC_DEFAULT_MCU_COUNT above). The sink
# is the CPU-bound half -- once the per-row round-trips were removed it ran ~80% CPU /
# ~21,000 rows/s at 4 MCU -- while the single-task Debezium source has spare CPU. This
# must stay equal to the cdc-stack template's ``SinkMcuCount`` default: for a stack
# deployed before the tool began passing the parameter, CloudFormation reports the
# template default, and any other value here would read as a config change and bounce
# both RUNNING connectors on the next Start CDC. Unlike the source's, this one IS
# operator-tunable (config.cdc_sink_mcu_count / Settings -> Performance -> CDC).
CDC_DEFAULT_SINK_MCU_COUNT = 4
# Env overrides (read fresh, DSQL_MIGRATOR_ prefix). Empty/invalid -> smart default.
CDC_ENV_SINK_TASKS_MAX = "DSQL_MIGRATOR_CDC_SINK_TASKS_MAX"
CDC_ENV_MCU_COUNT = "DSQL_MIGRATOR_CDC_MCU_COUNT"
CDC_ENV_TOPIC_PARTITIONS = "DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS"
# ConnectorMcuCount is an AllowedValues enum in the template; keep overrides legal.
_CDC_ALLOWED_MCU = (1, 2, 4, 8)


@dataclass(frozen=True)
class CdcScalingDefaults:
    """The inferred (or overridden) CDC connector-scaling knobs for a deploy.

    ``source`` is the effect on the two connectors that actually matters:
    ``partitions_per_topic`` fixes the per-table topic partition count (set once,
    irreversibly, at create), ``sink_tasks_max`` the sink write parallelism, and
    ``mcu_count`` the MSK Connect compute per worker.
    """

    partitions_per_topic: int
    sink_tasks_max: int
    mcu_count: int
    num_tables: int


def _cdc_env_int(env: Mapping[str, str], key: str) -> Optional[int]:
    """Return a positive int override from ``env[key]``, or None if unset/invalid."""
    raw = (env.get(key) or "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None
    return value if value >= 1 else None


def compute_cdc_scaling_defaults(
    num_tables: int, env: Optional[Mapping[str, str]] = None
) -> CdcScalingDefaults:
    """Infer the CDC connector-scaling knobs from the captured-table count.

    Pure. ``num_tables`` is the number of tables being captured (one Kafka topic
    each). Env overrides (:data:`CDC_ENV_SINK_TASKS_MAX` /
    :data:`CDC_ENV_MCU_COUNT` / :data:`CDC_ENV_TOPIC_PARTITIONS`) take precedence
    when set to a valid positive integer; the MCU override is additionally snapped
    to the template's AllowedValues (1/2/4/8).

    Smart default (see :data:`CDC_MAX_SINK_PARALLELISM`): pick the smallest
    partitions-per-topic that brings total parallelism (partitions * tables) up to
    the sink cap, so few-table captures still parallelise while many-table
    captures use 1 partition each. ``sink_tasks_max`` matches total parallelism (a
    sink task can only consume up to the partition count anyway).
    """
    source = os.environ if env is None else env
    tables = max(1, int(num_tables))

    # Partitions per topic: reach the sink-parallelism cap when tables are few.
    part_override = _cdc_env_int(source, CDC_ENV_TOPIC_PARTITIONS)
    if part_override is not None:
        partitions = part_override
    else:
        # ceil(cap / tables), but never below 1 -- so tables>=cap -> 1 each.
        partitions = max(1, -(-CDC_MAX_SINK_PARALLELISM // tables))

    total_parallelism = min(partitions * tables, CDC_MAX_SINK_PARALLELISM)

    tasks_override = _cdc_env_int(source, CDC_ENV_SINK_TASKS_MAX)
    sink_tasks_max = tasks_override if tasks_override is not None else total_parallelism

    mcu_override = _cdc_env_int(source, CDC_ENV_MCU_COUNT)
    if mcu_override is not None and mcu_override in _CDC_ALLOWED_MCU:
        mcu_count = mcu_override
    else:
        mcu_count = CDC_DEFAULT_MCU_COUNT

    return CdcScalingDefaults(
        partitions_per_topic=partitions,
        sink_tasks_max=sink_tasks_max,
        mcu_count=mcu_count,
        num_tables=tables,
    )


# --- Size-proportional partition plan (skewed-workload fix) -----------------
#
# compute_cdc_scaling_defaults spreads partitions UNIFORMLY, which assumes write
# load is even across tables. When tables >= CDC_MAX_SINK_PARALLELISM the uniform
# default collapses to 1 partition per topic, and a Kafka topic with 1 partition
# is consumed by at most ONE sink task -- so a "hot" table (a few tables carrying
# most of the writes, e.g. a sysbench run) is serialized on a single task while
# the rest sit idle. This plan instead gives the hot tables MORE partitions (by
# scan-free row-count estimate) so each streams across several tasks in parallel,
# leaving small tables at 1. It is a no-op under uniform load (nothing is elevated)
# and is gated to the many-tables regime where the uniform default actually hurts.
#
# Partitions are discretized into a FIXED tier set so the plan maps onto a small,
# bounded number of Debezium ``topic.creation`` groups (the CloudFormation
# template carries a fixed number of group blocks). 4 is the per-table ceiling:
# the 2026-07-08 throughput test showed a single table's gain flattens past ~4
# partitions (4->8 was only ~1.4x) as concurrent upserts to one table contend in
# DSQL, so more partitions on one table mostly add MCU cost without throughput.
CDC_PARTITION_TIERS = (2, 4)  # elevated per-table partition counts (>1); default 1
# A table is elevated when its estimated rows, as a multiple of the AVERAGE
# captured table's rows ("excess over fair share" = rows * num_tables / total),
# cross these thresholds. 1.0 == an average-sized table; only clearly-dominant
# tables are lifted, so an even workload keeps 1 partition each. Highest first.
_CDC_TIER_THRESHOLDS = ((4.0, 4), (2.0, 2))


@dataclass(frozen=True)
class CdcTopicGroup:
    """One Debezium ``topic.creation`` group: a partition count + its topics.

    ``name`` is the group name used in the connector config (``p2`` / ``p4``);
    ``topics`` are the fully-qualified topic names (``<prefix>.<db>.<table>``)
    that should be auto-created with ``partitions`` partitions.
    """

    name: str
    partitions: int
    topics: tuple[str, ...]


@dataclass(frozen=True)
class CdcPartitionPlan:
    """A size-proportional per-topic partition allocation for a skewed workload.

    ``partitions_by_topic`` is the full topic->partition map; ``groups`` are just
    the elevated tiers (>1 partition) that have at least one topic, rendered as
    Debezium ``topic.creation`` groups; tier-1 topics use ``default_partitions``.
    ``sink_tasks_max`` sizes the sink so the elevated partitions can be consumed
    concurrently (capped for cost); ``total_partitions`` is the sum across topics.
    """

    partitions_by_topic: dict[str, int]
    default_partitions: int
    groups: tuple[CdcTopicGroup, ...]
    sink_tasks_max: int
    total_partitions: int


def compute_cdc_partition_plan(
    row_counts_by_topic: Mapping[str, int],
    *,
    env: Optional[Mapping[str, str]] = None,
) -> Optional[CdcPartitionPlan]:
    """Allocate Kafka topic partitions proportional to per-table size, or ``None``.

    ``row_counts_by_topic`` maps each captured table's topic name to its estimated
    row count (scan-free ``information_schema`` estimates are fine -- only the
    RELATIVE sizes matter). Returns a :class:`CdcPartitionPlan` when a hot table is
    worth elevating, else ``None`` so the caller falls back to the uniform
    :func:`compute_cdc_scaling_defaults`. Pure. Returns ``None`` (uniform) when:

    * an explicit ``DSQL_MIGRATOR_CDC_TOPIC_PARTITIONS`` override is set (the
      operator asked for a fixed uniform count);
    * fewer than :data:`CDC_MAX_SINK_PARALLELISM` tables -- the uniform default
      already gives each topic several partitions in that regime;
    * no usable size signal (empty / all-zero counts); or
    * load is even (no table crosses a tier threshold), so 1 each is already right.
    """
    source = os.environ if env is None else env
    if _cdc_env_int(source, CDC_ENV_TOPIC_PARTITIONS) is not None:
        return None

    counts = {t: max(0, int(c)) for t, c in row_counts_by_topic.items() if t}
    num_tables = len(counts)
    if num_tables < CDC_MAX_SINK_PARALLELISM:
        return None
    total_rows = sum(counts.values())
    if total_rows <= 0:
        return None

    fair_share = 1.0 / num_tables
    partitions_by_topic: dict[str, int] = {}
    elevated = False
    for topic, rows in counts.items():
        excess = (rows / total_rows) / fair_share  # 1.0 == an average-sized table
        partitions = 1
        for min_excess, tier in _CDC_TIER_THRESHOLDS:  # highest tier first
            if excess >= min_excess:
                partitions = tier
                elevated = True
                break
        partitions_by_topic[topic] = partitions
    if not elevated:
        return None

    groups: list[CdcTopicGroup] = []
    for tier in sorted(set(CDC_PARTITION_TIERS)):
        topics = tuple(
            sorted(t for t, p in partitions_by_topic.items() if p == tier)
        )
        if topics:
            groups.append(CdcTopicGroup(name=f"p{tier}", partitions=tier, topics=topics))

    total_partitions = sum(partitions_by_topic.values())
    tasks_override = _cdc_env_int(source, CDC_ENV_SINK_TASKS_MAX)
    sink_tasks_max = (
        tasks_override
        if tasks_override is not None
        else min(total_partitions, CDC_MAX_SINK_PARALLELISM)
    )
    return CdcPartitionPlan(
        partitions_by_topic=partitions_by_topic,
        default_partitions=1,
        groups=tuple(groups),
        sink_tasks_max=sink_tasks_max,
        total_partitions=total_partitions,
    )


# Every cdc-stack name MUST start with this prefix. The deploy role's IAM scopes
# the whole "mysql-dsql-cdc-*" naming family (see deploy/cloudformation.yaml CdcDeployRole)
# so one app can run many cdc-stacks concurrently (one per source DB) -- but only
# within that family. Enforcing the prefix here keeps the tool's stack names inside
# the role's authority and out of the parent app stack's namespace.
CDC_STACK_NAME_PREFIX = "mysql-dsql-cdc-"
# CloudFormation stack names: letters/digits/hyphens, start with a letter, <=128.
_CDC_STACK_NAME_RE = re.compile(r"^[a-zA-Z][-a-zA-Z0-9]*$")
CDC_STACK_NAME_MAX_LEN = 128


def cdc_stack_name_is_valid(name: str) -> bool:
    """True when ``name`` is a usable cdc-stack name within the deploy role's scope.

    A name is valid when it starts with :data:`CDC_STACK_NAME_PREFIX`, has at least
    one character after the prefix, uses only the CloudFormation stack charset
    (letters, digits, hyphens; leading letter), and is at most
    :data:`CDC_STACK_NAME_MAX_LEN` characters. The prefix requirement is what keeps
    a user-chosen name inside the ``mysql-dsql-cdc-*`` IAM family the deploy role grants;
    a name outside it would deploy resources the role cannot manage (AccessDenied).
    """
    if not name or len(name) > CDC_STACK_NAME_MAX_LEN:
        return False
    if not name.startswith(CDC_STACK_NAME_PREFIX):
        return False
    if len(name) <= len(CDC_STACK_NAME_PREFIX):
        return False  # prefix only, no distinguishing suffix
    return bool(_CDC_STACK_NAME_RE.match(name))


def cdc_stack_name_suffix(name: Optional[str]) -> str:
    """Return the part of a cdc-stack name AFTER the mandatory prefix.

    The UI edits only this suffix (the prefix is shown fixed), so a user can never
    type a name outside the ``mysql-dsql-cdc-*`` family. ``mysql-dsql-cdc-orders``
    -> ``orders``; the default ``mysql-dsql-cdc-stack`` -> ``stack``. A name without
    the prefix (shouldn't happen) yields the whole name; ``None`` yields the default
    suffix. Pure.
    """
    full = (name or CDC_DEFAULT_STACK_NAME)
    if full.startswith(CDC_STACK_NAME_PREFIX):
        return full[len(CDC_STACK_NAME_PREFIX):]
    return full


def build_cdc_stack_name(suffix: str) -> Optional[str]:
    """Build a full cdc-stack name from a user-entered suffix, or None if invalid.

    Prepends the mandatory prefix and validates the whole name, so the UI can offer
    a suffix-only field: ``orders`` -> ``mysql-dsql-cdc-orders``. Returns ``None``
    when the resulting name is invalid (e.g. the suffix has illegal characters or is
    empty), so the caller can keep the current name and explain the rule. Pure.
    """
    candidate = CDC_STACK_NAME_PREFIX + (suffix or "").strip()
    return candidate if cdc_stack_name_is_valid(candidate) else None


# Rough hourly USD cost components for the CDC pipeline while it is deployed.
# Order-of-magnitude figures for us-east-1 (2025), NOT a quote -- pricing varies by
# region and (for MSK Serverless / NAT data processing) by actual throughput. Used
# only to give the operator a ballpark before they create billable resources.
#   - MSK Serverless: ~$0.75/hr base per cluster (partition-hours + storage extra).
#   - MSK Connect:    ~$0.11/hr per MCU, two connectors at 1 MCU each = ~$0.22/hr.
#   - NAT gateway:    ~$0.045/hr per gateway (one), plus per-GB data processing.
# This tool runs CDC only for the duration of a migration cut-over, so the estimate
# is expressed per HOUR (not per month). Kept as a function so it is unit-testable
# and the caveat travels with the number.
_CDC_HOURLY_USD = {
    "msk_serverless": 0.75,
    "nat_gateway": 0.045,
}
# MSK Connect bills per MCU-hour (~$0.11 in us-east-1). The estimate must track the
# ACTUAL deployed compute -- the source connector's MCUs (CDC_DEFAULT_MCU_COUNT) PLUS
# the sink's (CDC_DEFAULT_SINK_MCU_COUNT) -- not a flat "two connectors at 1 MCU each":
# the defaults deploy 2 + 4 = 6 MCU, so a flat 0.22 understated MSK Connect ~3x and the
# whole estimate ~30-40%, the opposite of the cost/footprint-awareness principle.
_MSK_CONNECT_USD_PER_MCU_HOUR = 0.11


@dataclass(frozen=True)
class CdcCostEstimate:
    """A ballpark hourly cost for a deployed CDC pipeline (NOT a quote)."""

    hourly_low_usd: float
    hourly_high_usd: float
    includes_nat: bool
    caveat: str


def estimate_cdc_hourly_cost(
    *,
    includes_nat: bool = True,
    source_mcu: int = CDC_DEFAULT_MCU_COUNT,
    sink_mcu: int = CDC_DEFAULT_SINK_MCU_COUNT,
) -> CdcCostEstimate:
    """Return a rough hourly USD range for a running CDC pipeline.

    ``includes_nat`` adds the NAT gateway base when the stack creates its own NAT
    (it is omitted when existing NAT-egress subnets are reused). ``source_mcu`` /
    ``sink_mcu`` are the deployed MSK Connect MCUs (defaulting to the deploy defaults),
    so the MSK Connect component tracks the real compute (2 + 4 = 6 MCU by default)
    rather than a flat two. The range pads the base figure by ~40% on the high side to
    acknowledge throughput-driven costs (MSK Serverless partition-hours/storage, NAT
    data processing) that a flat base cannot capture. Pure; the caveat makes the
    estimate's nature clear.
    """
    msk_connect = (int(source_mcu) + int(sink_mcu)) * _MSK_CONNECT_USD_PER_MCU_HOUR
    hourly = _CDC_HOURLY_USD["msk_serverless"] + msk_connect
    if includes_nat:
        hourly += _CDC_HOURLY_USD["nat_gateway"]
    low = round(hourly, 2)
    high = round(hourly * 1.4, 2)
    caveat = (
        "Rough estimate for us-east-1 while deployed — actual cost varies by region "
        "and throughput (MSK Serverless partition-hours/storage, NAT data "
        "processing). Billing accrues only while the CDC infrastructure is deployed; "
        "delete it after cut-over to stop charges."
    )
    return CdcCostEstimate(
        hourly_low_usd=low,
        hourly_high_usd=high,
        includes_nat=includes_nat,
        caveat=caveat,
    )


def cdc_expected_connector_names(
    stack_name: str = CDC_DEFAULT_STACK_NAME,
) -> tuple[str, str]:
    """Return the ``(source_name, sink_name)`` a cdc-stack with this name creates.

    The cdc-stack CloudFormation template names its connectors
    ``${AWS::StackName}-debezium-source`` and ``${AWS::StackName}-dsql-sink``, so
    a deployment with ``stack_name`` produces exactly these two connector names.
    Used both to scope "my connectors" when monitoring and to label the generated
    deploy config, so the two never drift apart.
    """
    return f"{stack_name}{CDC_SOURCE_SUFFIX}", f"{stack_name}{CDC_SINK_SUFFIX}"


class CdcPipelineOrchestrator:
    """Control plane for the managed CDC pipeline: configure, seed, monitor.

    Builds the Debezium source and custom DSQL sink connector configs, seeds the
    source start offset from the Full Load watermark (gapless handoff --
    Property 11), reports connector status (read-only), and surfaces connector/
    DLQ errors into the single downloadable error log (Property 15). It runs no
    sink consumer/apply loop (decision-change 8) and never writes the source
    (Property 1). ``status_source``/``error_source`` are injectable read-only
    suppliers; when absent, status is empty and error surfacing is a no-op.
    """

    def __init__(
        self,
        *,
        status_source: Optional[StatusSource] = None,
        error_source: Optional[ErrorSource] = None,
        now: Optional[Callable[[], datetime]] = None,
    ) -> None:
        """Create an orchestrator with optional read-only status/error sources."""
        self._status_source = status_source
        self._error_source = error_source
        self._now = now or (lambda: datetime.now(timezone.utc))

    def build_source_config(
        self,
        name: str,
        tables: Sequence[TableDef],
        watermark: Watermark,
        *,
        column_exclude_list: Optional[Sequence[str]] = None,
        resume_override: Optional[CdcResumePoint] = None,
        message_key_columns: Optional[Mapping[str, Sequence[str]]] = None,
    ) -> DebeziumSourceConfig:
        """Build the Debezium source config for the selected tables.

        Maps the resolved selection to ``table.include.list`` and seeds the start
        offset from the Full Load ``watermark`` via
        :meth:`CdcResumePoint.from_watermark`, so streaming resumes exactly where
        the snapshot ended (gapless -- Property 11).

        ``snapshot_mode`` is chosen automatically:

        - **Gapless (watermark) path:** ``recovery`` — Debezium rebuilds its
          schema-history from the live DB (without re-reading rows) and resumes
          from the seeded offset. Chosen ONLY when the watermark can actually seed
          that offset (:meth:`CdcResumePoint.can_seed_offset` — binlog ``file`` +
          ``pos`` present), because ``recovery`` needs a seeded ``connect-offsets``
          entry to resume from; the schema-history topic need NOT pre-exist —
          ``recovery`` rebuilds it from the live source (verified in the Seoul E2E:
          "Snapshot step 6 - Persisting schema history" then "Snapshot step 7 -
          Skipping snapshotting of data").
        - **Non-seedable / manual override path:** ``schema_only`` — Debezium
          reads the current source schema from scratch and begins streaming from
          the current position. Safe for a brand-new connector with no
          pre-existing schema-history topic. Used for a manual override (CDC-only,
          no Full Load) AND for a watermark that cannot seed an offset (e.g.
          GTID-only, when ``SHOW MASTER STATUS`` was restricted): choosing
          ``recovery`` there would leave the connector in recovery with no seeded
          offset — a task-start failure or a silent resume from the live binlog.

        ``column_exclude_list`` (optional) names fully-qualified columns
        (``db.table.column``) to drop at capture via Debezium
        ``column.exclude.list`` -- oversized LOB columns the user opted to exclude
        (spike H13); it flows to the cdc-stack ``ColumnExcludeList`` parameter.

        ``resume_override`` (optional) supplies an explicit start position the
        user entered in the UI (a GTID set or binlog ``file:position``). When
        given it takes precedence over the watermark, so CDC can be seeded when no
        Full Load watermark exists in this session or when the operator needs a
        custom offset. When ``None`` the gapless watermark path is used.

        ``message_key_columns`` (optional) maps ``{db.table: [target key cols]}``
        for tables whose DSQL target has a composite primary key; Debezium then
        keys the change record on those columns so the sink's record-key apply
        matches the target key. Empty/omitted = key on the source PK (default).
        """
        resume = (
            resume_override
            if resume_override is not None
            else CdcResumePoint.from_watermark(watermark)
        )
        # recovery: rebuilds schema-history from the live DB and resumes from a
        # seeded offset. Requires a seeded connect-offsets entry (the resume
        # position the offset-seeder Lambda writes during the gapless Full-Load
        # path); it REBUILDS the schema-history topic from the live source, so
        # that topic need not pre-exist. (schema_only with a seeded offset but no
        # schema-history dies at task start: "The db history topic is missing".)
        # schema_only: reads the current source schema from scratch and starts
        # streaming from the given binlog position. Safe for a brand-new connector
        # with no pre-existing schema-history topic (the Manual/CDC-only path).
        #
        # Use recovery ONLY when the watermark can actually SEED an offset AND no
        # manual override is active -- that's the gapless path where the offset
        # seeder prepared the connect-offsets entry recovery resumes from. The gate
        # MUST match the seeder's own precondition (can_seed_offset == binlog
        # file+pos present): the seeder REJECTS a watermark without binlog
        # coordinates and build_watermark_params then returns empty, so the seeder
        # is skipped. A GTID-only watermark (SHOW MASTER STATUS restricted but
        # @@GLOBAL.gtid_executed readable) has no binlog file:pos, so choosing
        # recovery there yields recovery-with-NO-seeded-offset -- which either fails
        # the source task at start or silently resumes from the CURRENT binlog
        # (losing the Full-Load-window changes). Falling back to schema_only starts
        # the connector cleanly; the gapless-from-Full-Load promise was never
        # achievable for such a watermark anyway (the UI surfaces that separately).
        has_real_watermark = (
            resume_override is None
            and watermark is not None
            and resume.can_seed_offset()
        )
        mode = "recovery" if has_real_watermark else "schema_only"
        return DebeziumSourceConfig(
            name=name,
            table_include_list=[table.name for table in tables],
            snapshot_mode=mode,
            start_gtid=resume.gtid_executed,
            start_binlog_file=resume.binlog_file,
            start_binlog_pos=resume.binlog_position,
            column_exclude_list=list(column_exclude_list or []),
            message_key_columns={
                table: list(cols)
                for table, cols in (message_key_columns or {}).items()
            },
        )

    def build_sink_config(
        self,
        name: str,
        tables: Sequence[TableDef],
        dlq_topic: str,
        *,
        allow_empty: bool = False,
    ) -> SinkConnectorConfig:
        """Build the custom DSQL Sink Connector config for the selected tables.

        Produces one topic per table with PK keying + idempotent upsert/delete +
        a DLQ for poison messages (Req 12.3-12.6). Only config is built here; the
        connector plugin performs the apply on MSK Connect.

        Requires at least one table by default. Unlike the SOURCE config (an empty
        ``table.include.list`` is valid -- Debezium reads it as "all tables"), a
        Kafka Connect SINK MUST be given ``topics``/``topics.regex``: an empty
        ``topics`` makes MSK Connect reject the connector at ``POST /connectors``
        with HTTP 400 ("Must configure one of topics or topics.regex"), which only
        surfaces minutes later as an opaque connector CREATE failure. Failing here
        turns that into an early, actionable error before any (billable, slow)
        connector deploy is attempted.

        ``allow_empty=True`` is for the "provision infrastructure early" path only:
        the first cdc-stack deploy creates MSK / networking / plugins with
        ``DeploySink=false`` (no connector yet), so an empty topic list is harmless
        there -- the real, non-empty ``SinkTopics`` is supplied later at Start CDC.
        """
        if not tables and not allow_empty:
            raise ValueError(
                "Cannot start CDC without selecting at least one table: a Kafka "
                "Connect sink requires a non-empty topic list (SinkTopics). Choose "
                "the tables to replicate before starting CDC."
            )
        return SinkConnectorConfig(
            name=name,
            topics=[table.name for table in tables],
            dlq_topic=dlq_topic,
        )

    def status(self) -> list[ConnectorStatus]:
        """Return current connector/task status (read-only; empty if no source).

        A real wiring reads MSK Connect ``DescribeConnector`` + CloudWatch; this
        method only relays what the injected source reports (Req 12.9).
        """
        if self._status_source is None:
            return []
        return list(self._status_source())

    def surface_errors(self, job_id: str, error_log: ErrorLogStore) -> None:
        """Append connector/DLQ errors to the single error log (Property 15).

        Reads the injected read-only error source and records one credential-free
        :class:`~dsql_migrator.core.models.DataErrorRecord` per error under
        ``job_id``, so Full Load and CDC errors converge in one downloadable log
        (Req 13.2). A no-op when no error source is wired.
        """
        if self._error_source is None:
            return
        for error in self._error_source():
            error_log.record(
                job_id,
                DataErrorRecord(
                    table=error.table,
                    error_code=error.error_code,
                    message=error.message,
                    occurred_at=error.occurred_at or self._now(),
                ),
            )


def composite_key_columns_for_cdc(
    tables: Sequence[TableDef],
    table_conversions: "Mapping[str, object]",
) -> dict[str, list[str]]:
    """Map ``{db.table: [target key cols]}`` for tables with a composite target PK.

    For each selected table whose APPLIED target primary key differs from its
    source PK (a composite ``(leading, id)`` key chosen in Schema Conversion),
    return the target key columns. Debezium is then told to key the change record
    on exactly those columns (``message.key.columns``) so the sink's record-key
    ON CONFLICT / DELETE match the target key -- no sink change needed.

    Pure: reads each applied :class:`TableConversion`'s ``target_ddl`` and compares
    its parsed primary key to ``table.primary_key``. A table with no applied
    conversion, or an unparseable/absent target PK, is treated as unchanged (the
    parser returning ``[]`` means "unknown") and omitted -- it keeps the source-PK
    record key (today's behavior). Keys are the fully-qualified ``db.table`` names.
    """
    from dsql_migrator.core.converter import parse_target_primary_key

    result: dict[str, list[str]] = {}
    for table in tables:
        conversion = table_conversions.get(table.name)
        target_ddl = getattr(conversion, "target_ddl", None)
        if not target_ddl:
            continue
        target_pk = parse_target_primary_key(target_ddl)
        if target_pk and target_pk != list(table.primary_key):
            result[table.name] = target_pk
    return result


def format_message_key_columns(message_key_columns: "Mapping[str, Sequence[str]]") -> str:
    """Render the Debezium ``message.key.columns`` value from a per-table key map.

    Debezium syntax: table entries separated by ``;``, the table pattern and its
    columns separated by ``:``, and columns separated by ``,`` -- e.g.
    ``app.orders:customer_id,id;app.items:tenant_id,id``. The table part is a Java
    regex matched against ``db.table``; dots in the name are escaped so
    ``app.orders`` matches literally (not ``appXorders``). Sorted for a stable,
    reviewable value; empty input yields ``""`` (key on the source PK). Pure.
    """
    entries: list[str] = []
    for table in sorted(message_key_columns):
        cols = list(message_key_columns[table])
        if not cols:
            continue
        pattern = table.replace(".", r"\.")
        entries.append(f"{pattern}:{','.join(cols)}")
    return ";".join(entries)


def composite_cdc_excluded_key_columns(
    message_key_columns: "Mapping[str, Sequence[str]]",
    column_exclude_list: Sequence[str],
) -> list[str]:
    """Return ``db.table.column`` key columns that are wrongly in the exclude list.

    A composite key column is read from the source row / before-image, so it must
    NOT be dropped at capture via ``column.exclude.list`` -- if it were, Debezium
    could not populate the record key and every change for that table would fail.
    This is the ONE composite-CDC precondition to gate on before starting CDC
    (the rest works via the source re-key). Returns the offending fully-qualified
    columns (sorted); empty means safe to start. Pure.
    """
    excluded = set(column_exclude_list)
    offending: list[str] = []
    for table, cols in message_key_columns.items():
        for col in cols:
            qualified = f"{table}.{col}"
            if qualified in excluded:
                offending.append(qualified)
    return sorted(offending)


def build_cdc_status_view(
    statuses: Sequence[ConnectorStatus],
    error_summary: Optional[ErrorLogSummary] = None,
    *,
    dlq_depth: Optional[int] = None,
    schema_drift: Optional[Sequence[SchemaDriftSummary]] = None,
) -> LoadStatusView:
    """Map CDC connector statuses + error summary to the unified LoadStatusView.

    This is the CDC *provider* for the unified monitoring component (Req 13.1 /
    Task 24.4): a thin reader that relays managed signals without recomputation
    (Req 13.5). ``lag_seconds`` is the worst (max) reported connector lag and
    ``caught_up_to`` the latest applied point across connectors; per-table rows
    come from the single :class:`ErrorLogSummary` (Req 13.2), since CDC status is
    connector-centric rather than row-count-centric. ``schema_drift`` (when the
    caller has classified the DLQ) surfaces source DDL the target has not caught up
    to; see :func:`classify_schema_drift`.
    """
    connector_states = {status.name: status.state.value for status in statuses}
    lags = [s.lag_seconds for s in statuses if s.lag_seconds is not None]
    caught = [s.caught_up_to for s in statuses if s.caught_up_to is not None]
    by_table = error_summary.errors_by_table if error_summary is not None else {}
    rows = [
        TableStatusRow(table=table, state="STREAMING", errors=count)
        for table, count in sorted(by_table.items())
    ]
    return LoadStatusView(
        kind=LoadKind.CDC,
        tables=rows,
        lag_seconds=max(lags) if lags else None,
        caught_up_to=max(caught) if caught else None,
        connector_states=connector_states,
        dlq_depth=dlq_depth,
        schema_drift=list(schema_drift) if schema_drift else [],
        error_summary=error_summary,
    )


# Customer-environment cdc-stack parameters the tool cannot know -- each becomes
# a labeled placeholder the customer must fill before deploying. (key, description).
_CDC_PLACEHOLDER_PARAMS: tuple[tuple[str, str], ...] = (
    ("VpcId", "your VPC ID (must reach source MySQL and Aurora DSQL privately)"),
    ("ConnectorSubnetIds", "two or more private subnet IDs in distinct AZs"),
    ("PluginBucketArn", "ARN of the S3 bucket holding both plugin artifacts"),
    ("DebeziumPluginS3Key", "S3 key of the Debezium MySQL plugin zip"),
    ("DsqlSinkPluginS3Key", "S3 key of the custom DSQL sink connector jar"),
    ("SourceDbHostname", "source MySQL hostname"),
    ("SourceSecretArn", "Secrets Manager ARN for the source DB credentials"),
    ("SourceSecretName", "Secrets Manager secret name (no colons)"),
    ("DsqlClusterArn", "ARN of the target Aurora DSQL cluster"),
    (
        "MskBootstrapServers",
        "MSK Serverless bootstrap brokers (from GetBootstrapBrokers after pass 1)",
    ),
    (
        "SourceDbSecurityGroupId",
        "optional security group ID of the source RDS/Aurora (empty to skip)",
    ),
)

# Machine-checkable marker so an unfilled value is never mistaken for a real one.
CDC_PLACEHOLDER_PREFIX = "<FILL_ME:"


def _placeholder(key: str, description: str) -> str:
    """Build the unmistakable placeholder value for a customer-environment param."""
    return f"{CDC_PLACEHOLDER_PREFIX} {key} — {description}>"


class CdcStackParams(BaseModel):
    """A generated cdc-stack parameter set: tool-filled values + placeholders.

    ``filled`` are ``(ParameterKey, ParameterValue)`` pairs the tool knows from
    the user's CDC settings and target connection. ``placeholders`` are the same
    shape but carry :data:`CDC_PLACEHOLDER_PREFIX`-marked values for
    customer-environment parameters the tool cannot know (VPC, subnets, plugin S3
    keys, secrets, MSK bootstrap); the customer MUST replace them before
    deploying. ``stack_name``/``topic_prefix`` echo the naming convention used so
    the UI can render the deploy command without magic strings.
    """

    model_config = ConfigDict(extra="forbid")

    filled: list[tuple[str, str]]
    placeholders: list[tuple[str, str]]
    stack_name: str
    topic_prefix: str


# The cdc-stack watermark parameter keys, in template order. Exposed so the UI /
# E2E / deployer build them consistently; values are always strings (CFN params).
CDC_WATERMARK_PARAM_KEYS: tuple[str, ...] = (
    "WatermarkBinlogFile",
    "WatermarkBinlogPos",
    "WatermarkGtids",
    "WatermarkTsSec",
)


def build_watermark_params(
    watermark: Optional[Watermark],
) -> list[tuple[str, str]]:
    """Map a Full Load :class:`Watermark` to the cdc-stack Watermark* parameters.

    Returns ``(ParameterKey, ParameterValue)`` pairs for WatermarkBinlogFile/
    BinlogPos/Gtids/TsSec, driving the in-VPC offset seeder for a gapless handoff
    (Property 11). Every value is a string (CFN parameter type). When the watermark
    is absent or has no binlog file:position, ALL four come back empty — the
    template's ``SeedOffset`` condition then goes false and no seeder is deployed
    (the source connector starts from the current binlog, the legacy behavior).
    Pure: no AWS, no I/O.
    """
    if watermark is None or not watermark.binlog_file or watermark.binlog_position is None:
        return [(k, "") for k in CDC_WATERMARK_PARAM_KEYS]
    ts_sec = int(watermark.snapshot_timestamp.timestamp())
    return [
        ("WatermarkBinlogFile", watermark.binlog_file),
        ("WatermarkBinlogPos", str(int(watermark.binlog_position))),
        ("WatermarkGtids", watermark.gtid_executed or ""),
        ("WatermarkTsSec", str(ts_sec)),
    ]


def build_cdc_stack_params(
    source_config: DebeziumSourceConfig,
    sink_config: SinkConnectorConfig,
    *,
    target_endpoint: str,
    target_database: str = "postgres",
    target_username: str = "admin",
    stack_name: str = CDC_DEFAULT_STACK_NAME,
    topic_prefix: str = CDC_DEFAULT_TOPIC_PREFIX,
    deploy_sink: bool = True,
    sink_mcu_count: int = CDC_DEFAULT_SINK_MCU_COUNT,
) -> CdcStackParams:
    """Map tool-built source/sink configs + known values to a deployable param set.

    Fills every cdc-stack parameter the tool can know at config time and emits a
    labeled placeholder for each customer-environment parameter it cannot. Pure:
    no AWS, no I/O. The one transformation the cdc-stack template delegates to the
    caller -- building ``SinkTopics`` as ``<topic_prefix>.<db>.<table>`` for each
    captured table -- is done here.

    The CDC start position (GTID / binlog file:position) is deliberately NOT a
    parameter: the cdc-stack seeds it via the ``connect-offsets`` topic, not a
    connector config key, so the UI surfaces it as a separate "offset seeding"
    note rather than a CFN parameter.

    ``sink_mcu_count`` is the sink connector's MSK Connect compute. It belongs on
    THIS (Start CDC) path, not only the infra create, because the sink connector is
    created here -- the stack's ``DsqlSinkConnector`` is gated on
    ``DeploySink=true`` -- and ``AWS::KafkaConnect::Connector.Capacity`` updates
    with "No interruption", so an operator can also RESIZE a deployed sink by
    changing this and running Start CDC again. Must be one of 1 / 2 / 4 / 8 (the
    template's ``AllowedValues``); the caller validates (see
    ``config.set_tuning_value``).
    """
    sink_topics = ",".join(f"{topic_prefix}.{t}" for t in sink_config.topics)
    filled: list[tuple[str, str]] = [
        ("TableIncludeList", ",".join(source_config.table_include_list)),
        ("TopicPrefix", topic_prefix),
        ("SinkTopics", sink_topics),
        ("DlqTopicName", sink_config.dlq_topic),
        ("ColumnExcludeList", ",".join(source_config.column_exclude_list)),
        # Composite-PK re-key: Debezium keys the change record on the target's
        # composite key so the sink's record-key ON CONFLICT/DELETE match it.
        # Empty (the common case) leaves the record keyed on the source PK.
        ("MessageKeyColumns",
         format_message_key_columns(source_config.message_key_columns)),
        ("DsqlClusterEndpoint", target_endpoint),
        ("DsqlDatabaseName", target_database),
        ("DsqlConnectUser", target_username),
        # Snapshot mode: schema_only for new connectors / CDC-only; recovery on
        # the gapless path (offset seeded; recovery rebuilds schema-history from
        # the live source — it does not need the topic pre-populated).
        ("SnapshotMode", source_config.snapshot_mode),
        # Sink connector compute. Set on this path (not just infra create) because
        # the sink connector itself is created by Start CDC, and Capacity is an
        # in-place ("No interruption") connector update -- so re-running Start CDC
        # with a new value resizes a deployed sink instead of forcing a redeploy.
        ("SinkMcuCount", str(sink_mcu_count)),
        # Whether to create the sink connector. The cdc-stack deploys the source
        # first (DeploySink=false) on a fresh stack so the data topic exists before
        # the sink subscribes; an update (the tool's deploy path) sets true since
        # the topic already exists.
        ("DeploySink", "true" if deploy_sink else "false"),
    ]
    placeholders = [
        (key, _placeholder(key, desc)) for key, desc in _CDC_PLACEHOLDER_PARAMS
    ]
    return CdcStackParams(
        filled=filled,
        placeholders=placeholders,
        stack_name=stack_name,
        topic_prefix=topic_prefix,
    )


def _topic_group_include(group: "CdcTopicGroup") -> str:
    """Render a topic.creation group's topics as a comma-separated regex list.

    Each topic name is regex-escaped so it matches literally (Debezium treats the
    include entries as regexes and full-matches topic names). Pure.
    """
    return ",".join(re.escape(topic) for topic in group.topics)


def cdc_scaling_params(
    topics: Sequence[str],
    topic_prefix: str,
    *,
    row_counts_by_table: Optional[Mapping[str, int]] = None,
    env: Optional[Mapping[str, str]] = None,
) -> list[tuple[str, str]]:
    """Build the (partition/parallelism) CFN parameters for a CDC deploy.

    Returns the ``TopicDefaultPartitions`` / ``SinkTasksMax`` /
    ``TopicCreationGroups`` / ``TopicGroupInclude2`` / ``TopicGroupInclude4``
    pairs. When ``row_counts_by_table`` yields a size-proportional
    :func:`compute_cdc_partition_plan` (a skewed, many-table capture), the hot
    tables go into elevated ``topic.creation`` groups and the default drops to 1;
    otherwise it falls back to the uniform :func:`compute_cdc_scaling_defaults`
    (empty groups). ``topics`` are table names (``db.table``); the group include
    regexes use the real topic names (``<topic_prefix>.<db>.<table>``). Pure.
    """
    plan = None
    if row_counts_by_table:
        counts_by_topic = {
            f"{topic_prefix}.{table}": count
            for table, count in row_counts_by_table.items()
            if table in set(topics)
        }
        plan = compute_cdc_partition_plan(counts_by_topic, env=env)

    if plan is not None:
        by_tier = {group.partitions: group for group in plan.groups}
        return [
            ("TopicDefaultPartitions", str(plan.default_partitions)),
            ("SinkTasksMax", str(plan.sink_tasks_max)),
            ("TopicCreationGroups", ",".join(group.name for group in plan.groups)),
            ("TopicGroupInclude2",
             _topic_group_include(by_tier[2]) if 2 in by_tier else ""),
            ("TopicGroupInclude4",
             _topic_group_include(by_tier[4]) if 4 in by_tier else ""),
            # Explicit per-topic partition map for the seeder Lambda, which
            # PRE-CREATES the topics (Debezium topic.creation only applies to topics
            # it creates -- and the seeder now creates them first). Format
            # "topic:count,...". Without this the seeder would create every topic
            # with the flat TopicDefaultPartitions (=1 here), silently defeating the
            # size-proportional plan (topic partition counts are immutable).
            ("SinkTopicPartitions",
             ",".join(f"{t}:{p}" for t, p in sorted(plan.partitions_by_topic.items()))),
        ]

    scaling = compute_cdc_scaling_defaults(len(topics), env=env)
    return [
        ("TopicDefaultPartitions", str(scaling.partitions_per_topic)),
        ("SinkTasksMax", str(scaling.sink_tasks_max)),
        ("TopicCreationGroups", ""),
        ("TopicGroupInclude2", ""),
        ("TopicGroupInclude4", ""),
        # Uniform plan: every topic gets TopicDefaultPartitions, so no per-topic map
        # is needed (the seeder falls back to the flat count).
        ("SinkTopicPartitions", ""),
    ]


class CdcInfraParams(BaseModel):
    """A complete cdc-stack parameter set for the first ``create_stack`` deploy.

    Unlike :class:`CdcStackParams` (which leaves customer-environment values as
    ``<FILL_ME:>`` placeholders for a manual CLI deploy), every value here is
    ``filled`` -- a create has no previous stack state, so each parameter is
    passed explicitly. The two connector-control parameters are always pinned to
    "no connectors yet": ``MskBootstrapServers=""`` and ``DeploySink="false"``,
    so the create brings up MSK / VPC wiring / plugins / IAM but no connectors.
    Start CDC later sets those two to actually create the connectors.
    """

    model_config = ConfigDict(extra="forbid")

    filled: list[tuple[str, str]]
    stack_name: str
    topic_prefix: str


def build_cdc_infra_params(
    source_config: DebeziumSourceConfig,
    sink_config: SinkConnectorConfig,
    *,
    # VPC / networking (bring-your-own subnets OR tool-owned NAT)
    vpc_id: str,
    connector_subnet_ids: str = "",
    # When connector_subnet_ids is empty, the stack creates its own subnets + NAT;
    # these carry the tool-computed inputs (else left empty / unused).
    nat_public_subnet_id: str = "",
    private_subnet_cidr_a: str = "",
    private_subnet_cidr_b: str = "",
    private_subnet_az_a: str = "",
    private_subnet_az_b: str = "",
    source_db_security_group_id: str = "",
    # Plugin artifacts (S3)
    plugin_bucket_arn: str,
    debezium_plugin_s3_key: str,
    dsql_sink_plugin_s3_key: str,
    lambda_seeder_s3_key: str = "",
    # Source MySQL
    source_db_hostname: str,
    source_secret_arn: str,
    source_secret_name: str,
    source_db_port: int = 3306,
    source_db_server_id: int = 54321,
    # Target Aurora DSQL
    dsql_cluster_arn: str,
    target_endpoint: str,
    target_database: str = "postgres",
    target_username: str = "admin",
    # CDC / topic config
    stack_name: str = CDC_DEFAULT_STACK_NAME,
    topic_prefix: str = CDC_DEFAULT_TOPIC_PREFIX,
    plugin_version: str = "v1",
    # Per-table estimated row counts ({table_name: rows}, scan-free estimates).
    # When a skewed, many-table capture is detected, drives size-proportional
    # topic partitions so hot tables are not serialized on one sink task; absent /
    # even load falls back to the uniform partition default. Set at create because
    # partition counts are fixed for the life of the topic.
    row_counts_by_table: Optional[Mapping[str, int]] = None,
    # Sink connector MSK Connect compute (operator-tunable; 1/2/4/8). Carried on the
    # create path too so a fresh stack records the operator's value from the start,
    # rather than only picking it up at the first Start CDC.
    sink_mcu_count: int = CDC_DEFAULT_SINK_MCU_COUNT,
    # CDC seed mode + the host's subnet CIDR, both set at CREATE so a Lambda-free
    # "EC2 + MSK only" host produces a SeedMode=External stack that admits the host
    # on 9098 from the start. seed_mode is the lowercase config value ("lambda" /
    # "external"); it is mapped to the template's capitalized token below.
    # host_subnet_cidr is empty for the default (Lambda) path -> no 9098 ingress.
    seed_mode: str = "lambda",
    host_subnet_cidr: str = "",
) -> CdcInfraParams:
    """Build the full cdc-stack parameter set for a first-time ``create_stack``.

    Reuses the table/topic derivation from :func:`build_cdc_stack_params`
    (``TableIncludeList`` from the source config, ``SinkTopics`` as
    ``<topic_prefix>.<db>.<table>``, ``ColumnExcludeList``, ``DlqTopicName``) and
    fills every customer-environment parameter from the BYO-VPC inputs. Pure: no
    AWS, no I/O.

    ``connector_subnet_ids`` MUST be a comma-separated string (the template's
    ``List<AWS::EC2::Subnet::Id>`` type is passed as a comma-joined value, never a
    Python list). ``MskBootstrapServers`` and ``DeploySink`` are pinned to
    ``""`` / ``"false"`` so no connectors are created on the first deploy.

    The connector-scaling knobs (``TopicDefaultPartitions`` / ``SinkTasksMax`` /
    ``ConnectorMcuCount``) are INFERRED from the captured-table count via
    :func:`compute_cdc_scaling_defaults` (env-overridable). Partition count is set
    here, at create, because it is irreversible for the life of the topic.
    """
    sink_topics = ",".join(f"{topic_prefix}.{t}" for t in sink_config.topics)
    scaling = compute_cdc_scaling_defaults(len(sink_config.topics))
    scaling_params = cdc_scaling_params(
        sink_config.topics, topic_prefix, row_counts_by_table=row_counts_by_table
    )
    filled: list[tuple[str, str]] = [
        # VPC / networking
        ("VpcId", vpc_id),
        ("ConnectorSubnetIds", connector_subnet_ids),
        # Tool-owned NAT inputs (empty when connector_subnet_ids is provided).
        ("NatPublicSubnetId", nat_public_subnet_id),
        ("PrivateSubnetCidrA", private_subnet_cidr_a),
        ("PrivateSubnetCidrB", private_subnet_cidr_b),
        ("PrivateSubnetAzA", private_subnet_az_a),
        ("PrivateSubnetAzB", private_subnet_az_b),
        ("SourceDbSecurityGroupId", source_db_security_group_id),
        # Plugin artifacts
        ("PluginBucketArn", plugin_bucket_arn),
        ("DebeziumPluginS3Key", debezium_plugin_s3_key),
        ("DsqlSinkPluginS3Key", dsql_sink_plugin_s3_key),
        # Offset-seeder Lambda zip key. Set at create so it PERSISTS (the seeder
        # itself is only created at Start CDC, when the bootstrap string + a Full
        # Load watermark make the SeedOffset condition true).
        ("LambdaSeederS3Key", lambda_seeder_s3_key),
        # Source MySQL
        ("SourceDbHostname", source_db_hostname),
        ("SourceDbPort", str(source_db_port)),
        ("SourceDbServerId", str(source_db_server_id)),
        ("SourceSecretArn", source_secret_arn),
        ("SourceSecretName", source_secret_name),
        # Target DSQL
        ("DsqlClusterArn", dsql_cluster_arn),
        ("DsqlClusterEndpoint", target_endpoint),
        ("DsqlDatabaseName", target_database),
        ("DsqlConnectUser", target_username),
        # CDC / topic config (same derivation as build_cdc_stack_params)
        ("TableIncludeList", ",".join(source_config.table_include_list)),
        ("TopicPrefix", topic_prefix),
        ("SinkTopics", sink_topics),
        ("DlqTopicName", sink_config.dlq_topic),
        ("ColumnExcludeList", ",".join(source_config.column_exclude_list)),
        # Composite-PK re-key (empty on the create path -- no connectors yet;
        # Start CDC supplies the real value via build_cdc_stack_params).
        ("MessageKeyColumns",
         format_message_key_columns(source_config.message_key_columns)),
        # Snapshot mode: schema_only for new connectors / CDC-only; recovery on
        # the gapless path (offset seeded; recovery rebuilds schema-history from
        # the live source — it does not need the topic pre-populated).
        ("SnapshotMode", source_config.snapshot_mode),
        # Plugin resource-name version suffix (gotcha #5: lets a new artifact get a
        # uniquely-named CustomPlugin instead of colliding on a fixed name).
        ("PluginVersion", plugin_version),
        # Connector scaling inferred from the captured tables (env-overridable).
        # TopicDefaultPartitions / the topic.creation groups are set HERE because a
        # topic's partition count is fixed for its life -- it can only be raised (and
        # only by recreating the cluster in practice), never lowered, so it must be
        # right at create time. cdc_scaling_params emits TopicDefaultPartitions,
        # SinkTasksMax, and the size-proportional topic.creation group params
        # (TopicCreationGroups / TopicGroupInclude2 / TopicGroupInclude4).
        *scaling_params,
        ("ConnectorMcuCount", str(scaling.mcu_count)),
        # Sink compute: operator-tunable, unlike the source's inferred value above.
        ("SinkMcuCount", str(sink_mcu_count)),
        # No connectors on the first deploy -- Start CDC sets these two later.
        ("MskBootstrapServers", ""),
        ("DeploySink", "false"),
        # SeedMode set at CREATE (not just Start) so the in-VPC seeder Lambda + role
        # are NEVER created in External mode (they are gated on SeedByLambda at
        # create time); flipping only at Start would create-then-delete them,
        # incurring the slow ENI reclamation. Map the lowercase config value to the
        # template's case-sensitive AllowedValues ["Lambda","External"].
        ("SeedMode", "External" if seed_mode == "external" else "Lambda"),
        # Host subnet CIDR -> the 9098 ingress (ConnectorHostDiagnosticsIngress) that
        # the External in-process seed needs. Created here so the rule exists before
        # the first Start's pre-update seed; rides UsePreviousValue to the Start pass.
        # Empty (default/Lambda) -> no rule, unchanged.
        ("HostSubnetCidr", host_subnet_cidr),
    ]
    return CdcInfraParams(
        filled=filled, stack_name=stack_name, topic_prefix=topic_prefix
    )


def cdc_stack_params_to_json(params: CdcStackParams) -> str:
    """Serialize a :class:`CdcStackParams` to the cdc-stack params JSON format.

    Produces the ``[{"ParameterKey": ..., "ParameterValue": ...}, ...]`` shape the
    ``aws cloudformation deploy --parameter-overrides file://...`` flag accepts
    (matching deploy/cdc-stack/b2-params-*.json). Filled values first, then
    placeholders, so the customer sees what is ready vs. what they must complete.
    """
    import json

    combined = [
        {"ParameterKey": key, "ParameterValue": value}
        for key, value in (*params.filled, *params.placeholders)
    ]
    return json.dumps(combined, indent=2)


__all__ = [
    "CdcStatus",
    "CdcResumePoint",
    "CdcOptions",
    "CdcResult",
    "CdcCatchUp",
    "ConnectorState",
    "ConnectorStatus",
    "DebeziumSourceConfig",
    "SinkConnectorConfig",
    "CdcConnectorError",
    "SchemaDriftKind",
    "classify_schema_drift",
    "StatusSource",
    "ErrorSource",
    "CdcPipelineOrchestrator",
    "build_cdc_status_view",
    "CDC_DEFAULT_STACK_NAME",
    "CDC_DEFAULT_TOPIC_PREFIX",
    "CDC_DEFAULT_DLQ_TOPIC",
    "CDC_SOURCE_SUFFIX",
    "CDC_SINK_SUFFIX",
    "CDC_STACK_NAME_PREFIX",
    "CDC_STACK_NAME_MAX_LEN",
    "cdc_stack_name_is_valid",
    "CdcCostEstimate",
    "estimate_cdc_hourly_cost",
    "cdc_expected_connector_names",
    "CDC_PLACEHOLDER_PREFIX",
    "CdcStackParams",
    "build_cdc_stack_params",
    "CdcInfraParams",
    "build_cdc_infra_params",
    "cdc_stack_params_to_json",
    "CdcScalingDefaults",
    "compute_cdc_scaling_defaults",
    "CdcTopicGroup",
    "CdcPartitionPlan",
    "compute_cdc_partition_plan",
    "cdc_scaling_params",
    "CDC_PARTITION_TIERS",
    "CDC_MAX_SINK_PARALLELISM",
    "CDC_DEFAULT_MCU_COUNT",
    "CDC_DEFAULT_SINK_MCU_COUNT",
    "CDC_ENV_SINK_TASKS_MAX",
    "CDC_ENV_MCU_COUNT",
    "CDC_ENV_TOPIC_PARTITIONS",
]
