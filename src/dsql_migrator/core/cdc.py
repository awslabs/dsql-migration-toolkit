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

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Literal, Optional, Sequence

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import (
    DataErrorRecord,
    ErrorLogSummary,
    LoadKind,
    LoadStatusView,
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
        """
        has_binlog = self.binlog_file is not None and self.binlog_position is not None
        return bool(self.gtid_executed) or has_binlog


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
# TB-scale, continuous CDC (Requirement 12) runs as a pipeline whose data plane
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


class CdcConnectorError(BaseModel):
    """A read-only connector task failure or DLQ record for error surfacing.

    Credential-free (Property 7): ``table`` names the affected table/topic (or
    connector), ``message`` is an English reason, and ``error_code`` is an
    optional SQLSTATE-like code. ``occurred_at`` is provided by the source when
    known; otherwise the orchestrator stamps the current time.
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    message: str = Field(min_length=1)
    error_code: Optional[str] = None
    occurred_at: Optional[datetime] = None


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
    "msk_connect": 0.22,
    "nat_gateway": 0.045,
}


@dataclass(frozen=True)
class CdcCostEstimate:
    """A ballpark hourly cost for a deployed CDC pipeline (NOT a quote)."""

    hourly_low_usd: float
    hourly_high_usd: float
    includes_nat: bool
    caveat: str


def estimate_cdc_hourly_cost(*, includes_nat: bool = True) -> CdcCostEstimate:
    """Return a rough hourly USD range for a running CDC pipeline.

    ``includes_nat`` adds the NAT gateway base when the stack creates its own NAT
    (it is omitted when existing NAT-egress subnets are reused). The range pads the
    base figure by ~40% on the high side to acknowledge throughput-driven costs
    (MSK Serverless partition-hours/storage, NAT data processing) that a flat base
    cannot capture. Pure; the caveat string makes the estimate's nature clear.
    """
    hourly = _CDC_HOURLY_USD["msk_serverless"] + _CDC_HOURLY_USD["msk_connect"]
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
    ) -> DebeziumSourceConfig:
        """Build the Debezium source config for the selected tables.

        Maps the resolved selection to ``table.include.list`` and seeds the start
        offset from the Full Load ``watermark`` via
        :meth:`CdcResumePoint.from_watermark`, so streaming resumes exactly where
        the snapshot ended (gapless -- Property 11). Uses ``snapshot_mode=
        recovery``: the bulk loader already loaded the rows and the offset is
        seeded, so Debezium must rebuild its schema-history from the live DB
        (without re-reading rows) and resume from the seeded offset.

        ``column_exclude_list`` (optional) names fully-qualified columns
        (``db.table.column``) to drop at capture via Debezium
        ``column.exclude.list`` -- oversized LOB columns the user opted to exclude
        (spike H13); it flows to the cdc-stack ``ColumnExcludeList`` parameter.

        ``resume_override`` (optional) supplies an explicit start position the
        user entered in the UI (a GTID set or binlog ``file:position``). When
        given it takes precedence over the watermark, so CDC can be seeded when no
        Full Load watermark exists in this session or when the operator needs a
        custom offset. When ``None`` the gapless watermark path is used.
        """
        resume = (
            resume_override
            if resume_override is not None
            else CdcResumePoint.from_watermark(watermark)
        )
        return DebeziumSourceConfig(
            name=name,
            table_include_list=[table.name for table in tables],
            snapshot_mode="recovery",
            start_gtid=resume.gtid_executed,
            start_binlog_file=resume.binlog_file,
            start_binlog_pos=resume.binlog_position,
            column_exclude_list=list(column_exclude_list or []),
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


def build_cdc_status_view(
    statuses: Sequence[ConnectorStatus],
    error_summary: Optional[ErrorLogSummary] = None,
    *,
    dlq_depth: Optional[int] = None,
) -> LoadStatusView:
    """Map CDC connector statuses + error summary to the unified LoadStatusView.

    This is the CDC *provider* for the unified monitoring component (Req 13.1 /
    Task 24.4): a thin reader that relays managed signals without recomputation
    (Req 13.5). ``lag_seconds`` is the worst (max) reported connector lag and
    ``caught_up_to`` the latest applied point across connectors; per-table rows
    come from the single :class:`ErrorLogSummary` (Req 13.2), since CDC status is
    connector-centric rather than row-count-centric.
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
    """
    sink_topics = ",".join(f"{topic_prefix}.{t}" for t in sink_config.topics)
    filled: list[tuple[str, str]] = [
        ("TableIncludeList", ",".join(source_config.table_include_list)),
        ("TopicPrefix", topic_prefix),
        ("SinkTopics", sink_topics),
        ("DlqTopicName", sink_config.dlq_topic),
        ("ColumnExcludeList", ",".join(source_config.column_exclude_list)),
        ("DsqlClusterEndpoint", target_endpoint),
        ("DsqlDatabaseName", target_database),
        ("DsqlConnectUser", target_username),
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
    """
    sink_topics = ",".join(f"{topic_prefix}.{t}" for t in sink_config.topics)
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
        # Plugin resource-name version suffix (gotcha #5: lets a new artifact get a
        # uniquely-named CustomPlugin instead of colliding on a fixed name).
        ("PluginVersion", plugin_version),
        # No connectors on the first deploy -- Start CDC sets these two later.
        ("MskBootstrapServers", ""),
        ("DeploySink", "false"),
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
]
