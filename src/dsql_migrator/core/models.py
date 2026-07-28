# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared data models for the migration engine.

These Pydantic v2 models are the common vocabulary used across the engine
components (introspection, assessment, conversion, migration, validation) and
the UI. They validate untrusted input (Requirement 9.4) and never carry
plaintext credentials: connection models reference a secret via
:class:`~dsql_migrator.config.SecretRef` rather than embedding its value
(Requirement 9.2 / Property 7).

Concrete engine behavior is implemented in later tasks; this module only defines
the data contracts.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.config import SecretRef

# ---------------------------------------------------------------------------
# Connection configuration
# ---------------------------------------------------------------------------


class SourceConnectionConfig(BaseModel):
    """Connection settings for the source MySQL (RDS/Aurora) database.

    Credentials are not stored here: ``secret`` references where to resolve them
    (e.g., a Secrets Manager ARN), and ``username`` is non-secret. This keeps the
    model safe to serialize and log.
    """

    model_config = ConfigDict(extra="forbid")

    host: str = Field(min_length=1, description="Source database host.")
    port: int = Field(default=3306, ge=1, le=65535, description="Source port.")
    database: Optional[str] = Field(
        default=None,
        description=(
            "Source database/schema name. Optional for a connection test; "
            "required before introspection selects objects."
        ),
    )
    username: Optional[str] = Field(
        default=None, description="Non-secret username (used when no secret ref)."
    )
    secret: Optional[SecretRef] = Field(
        default=None, description="Reference to the credential; never the value."
    )


class ConnectionResult(BaseModel):
    """Outcome of a connection check.

    ``detail`` is a human-readable, log-safe message describing success or the
    failure reason. It must never contain plaintext credentials
    (Requirement 1.4 / 9.2 / Property 7).
    """

    model_config = ConfigDict(extra="forbid")

    success: bool
    detail: str = ""
    server_version: Optional[str] = Field(
        default=None,
        description=(
            "Source server version string read on a successful source test "
            "(e.g. '8.0.mysql_aurora.3.04.0'); None for the target or on failure."
        ),
    )
    mysql_version: Optional[str] = Field(
        default=None,
        description=(
            "Community MySQL engine version (e.g. '8.0.42') read from "
            "@@innodb_version; used to show the MySQL patch behind an Aurora "
            "version. None when unavailable."
        ),
    )
    aurora_version: Optional[str] = Field(
        default=None,
        description=(
            "Aurora MySQL engine version (e.g. '3.07.1') read from "
            "@@aurora_version; present only for Aurora MySQL sources. None for "
            "community/RDS MySQL, the target, or on failure."
        ),
    )


class TargetConnectionConfig(BaseModel):
    """Connection settings for the target Aurora DSQL cluster.

    DSQL uses short-lived IAM tokens instead of passwords, so no credential value
    is stored. The token is generated at connect time in a later task.
    """

    model_config = ConfigDict(extra="forbid")

    cluster_endpoint: str = Field(min_length=1, description="DSQL cluster endpoint.")
    region: str = Field(min_length=1, description="AWS region of the cluster.")
    database: str = Field(default="postgres", description="DSQL database name.")
    username: str = Field(default="admin", description="DSQL database role.")


# ---------------------------------------------------------------------------
# Source schema inventory
# ---------------------------------------------------------------------------


class ColumnDef(BaseModel):
    """A single column in a source table."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    mysql_type: str = Field(min_length=1)
    nullable: bool = True
    default: Optional[str] = None
    collation: Optional[str] = None
    generated: bool = Field(
        default=False,
        description="True for a MySQL generated/computed column (VIRTUAL/STORED).",
    )
    auto_update_timestamp: bool = Field(
        default=False,
        description="True when the column uses ON UPDATE CURRENT_TIMESTAMP.",
    )


class IndexDef(BaseModel):
    """A secondary index definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    columns: list[str] = Field(min_length=1)
    unique: bool = False
    index_type: Optional[str] = Field(
        default=None,
        description="MySQL index type (e.g. BTREE, FULLTEXT, SPATIAL), if known.",
    )


class ForeignKeyDef(BaseModel):
    """A foreign key constraint (removed during DSQL conversion)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    columns: list[str] = Field(min_length=1)
    referenced_table: str = Field(min_length=1)
    referenced_columns: list[str] = Field(min_length=1)
    # The MySQL referential ACTIONS (``CASCADE`` / ``SET NULL`` / ``RESTRICT`` /
    # ``NO ACTION`` / ``SET DEFAULT``), upper-cased, or ``None`` for the default
    # (``NO ACTION``). These matter beyond the dropped constraint itself: MySQL
    # performs a cascade INSIDE the InnoDB engine, so the resulting child-row
    # changes are never written to the binary log (MySQL bug #32506, closed as
    # documented behavior -- the same reason "cascaded foreign key actions do not
    # activate triggers"). Debezium reads the binary log, so a CDC stream CANNOT
    # replicate them, and DSQL has no foreign keys to re-perform the cascade. The
    # assessor uses these to flag the affected tables up front.
    on_delete: Optional[str] = None
    on_update: Optional[str] = None

    @property
    def has_cascade_action(self) -> bool:
        """True when this FK cascades (or nulls/defaults) child rows automatically.

        Any action MySQL performs on the child table by itself is invisible to the
        binary log, so all of them are un-replicable by CDC -- not just ``CASCADE``.
        ``RESTRICT``/``NO ACTION`` only REJECT the parent change, so they never
        produce an unlogged child write and are excluded.
        """
        actions = {
            (self.on_delete or "").upper().replace("_", " "),
            (self.on_update or "").upper().replace("_", " "),
        }
        return bool(actions & {"CASCADE", "SET NULL", "SET DEFAULT"})


class TableDef(BaseModel):
    """A source table definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    columns: list[ColumnDef] = Field(default_factory=list)
    primary_key: list[str] = Field(default_factory=list)
    indexes: list[IndexDef] = Field(default_factory=list)
    foreign_keys: list[ForeignKeyDef] = Field(default_factory=list)
    auto_increment_column: Optional[str] = None
    partitioned: bool = Field(
        default=False,
        description="True when the source table uses MySQL native partitioning.",
    )


class ViewDef(BaseModel):
    """A source view definition."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    definition: str = ""


class ObjectType(str, Enum):
    """Kinds of non-table/view source objects tracked in the inventory."""

    TRIGGER = "TRIGGER"
    ROUTINE = "ROUTINE"
    PROCEDURE = "PROCEDURE"
    FUNCTION = "FUNCTION"
    EVENT = "EVENT"


class ObjectRef(BaseModel):
    """A lightweight reference to a source object (trigger or routine)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    object_type: ObjectType


class SourceInventory(BaseModel):
    """The full set of source objects collected by introspection."""

    model_config = ConfigDict(extra="forbid")

    tables: list[TableDef] = Field(default_factory=list)
    views: list[ViewDef] = Field(default_factory=list)
    triggers: list[ObjectRef] = Field(default_factory=list)
    routines: list[ObjectRef] = Field(default_factory=list)
    events: list[ObjectRef] = Field(
        default_factory=list,
        description="MySQL scheduled EVENTs (no Aurora DSQL equivalent).",
    )


# ---------------------------------------------------------------------------
# Target catalog inventory (Aurora DSQL / PostgreSQL)
# ---------------------------------------------------------------------------


class TargetObjectKind(str, Enum):
    """Kind of a browsable relation discovered on the target catalog."""

    TABLE = "TABLE"
    VIEW = "VIEW"


class TargetColumnDef(BaseModel):
    """A single column of a target relation (read from the catalog)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    data_type: str = Field(min_length=1, description="PostgreSQL/DSQL data type.")
    nullable: bool = True


class TargetIndexDef(BaseModel):
    """An index that already exists on a target relation."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    unique: bool = False


class TargetRelation(BaseModel):
    """A table or view discovered on the target, with its columns and indexes.

    ``kind`` distinguishes tables from views so that existence/conflict reports
    can name the object kind, while ``schema_name``/``name`` together identify
    the object for pre-apply conflict detection (Requirement 10.3).
    """

    model_config = ConfigDict(extra="forbid")

    schema_name: str = Field(min_length=1)
    name: str = Field(min_length=1)
    kind: TargetObjectKind
    columns: list[TargetColumnDef] = Field(default_factory=list)
    indexes: list[TargetIndexDef] = Field(default_factory=list)

    @property
    def qualified_name(self) -> str:
        """Return the ``schema.name`` identifier for this relation."""
        return f"{self.schema_name}.{self.name}"


class TargetSchemaNode(BaseModel):
    """A schema node in the target object tree, grouping its relations."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    tables: list[TargetRelation] = Field(default_factory=list)
    views: list[TargetRelation] = Field(default_factory=list)


class TargetInventory(BaseModel):
    """The object tree browsed from the target DSQL catalog (Requirement 10.1).

    Schemas contain tables and views, each of which carries its columns and
    indexes. The tree is used to render the browsable object view and to detect
    whether an object already exists before applying converted DDL
    (Requirement 10.3).
    """

    model_config = ConfigDict(extra="forbid")

    schemas: list[TargetSchemaNode] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Schema apply (DDL executor)
# ---------------------------------------------------------------------------


class ApplyMode(str, Enum):
    """How to handle a target object that already exists when applying DDL.

    - ``SKIP_IF_EXISTS``: leave the existing object untouched and report it as
      skipped (non-destructive, the safe default).
    - ``REPLACE``: drop the existing object and recreate it. This is destructive
      and therefore requires an explicit confirmation before it is performed
      (Requirement 10.6 / Property 12).
    """

    SKIP_IF_EXISTS = "SKIP_IF_EXISTS"
    REPLACE = "REPLACE"


class ApplyStatus(str, Enum):
    """Per-object outcome of applying converted DDL to the target (Req 10.7)."""

    CREATED = "CREATED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"


class DdlPreview(BaseModel):
    """A side-by-side preview of an object's source vs. converted target DDL.

    Powers the SCT-like diff view (Requirement 10.2): it pairs the current source
    DDL with the converted target DDL and reports whether the object already
    exists on the target, so an existence/conflict can be surfaced before apply
    (Requirement 10.3). ``object_name`` is derived from the target DDL.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1)
    source_ddl: str = ""
    target_ddl: str = Field(min_length=1)
    exists: bool = False


class ApplyResult(BaseModel):
    """The per-object result of applying one converted DDL statement (Req 10.7).

    ``status`` is ``CREATED`` when the object was created (or replaced),
    ``SKIPPED`` when it already existed under :attr:`ApplyMode.SKIP_IF_EXISTS`,
    and ``FAILED`` when the apply could not be performed (e.g. a destructive
    ``REPLACE`` was not confirmed, or the target rejected the DDL). ``detail`` is
    a human-readable, log-safe explanation of the outcome or the failure reason.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1)
    status: ApplyStatus
    detail: str = ""


# ---------------------------------------------------------------------------
# Compatibility assessment
# ---------------------------------------------------------------------------


class Classification(str, Enum):
    """Migration classification for a source object."""

    AUTO = "AUTO"
    MANUAL = "MANUAL"
    UNSUPPORTED = "UNSUPPORTED"


class EffortLevel(str, Enum):
    """Estimated manual effort to convert a non-automatic object (SCT-style).

    Mirrors AWS Schema Conversion Tool's assessment effort buckets so the report
    conveys how much manual work a conversion that cannot be done automatically
    will take (see the SCT "Assessment report summary"):

    - ``SIMPLE``: can be completed in less than two hours.
    - ``MEDIUM``: more complex, two to six hours.
    - ``SIGNIFICANT``: very complex, more than six hours.

    ``AUTO`` items have no effort (``None``); only ``MANUAL``/``UNSUPPORTED``
    items carry an effort estimate.
    """

    SIMPLE = "SIMPLE"
    MEDIUM = "MEDIUM"
    SIGNIFICANT = "SIGNIFICANT"


class AssessmentItem(BaseModel):
    """One assessed object with its classification, effort, and guidance."""

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1)
    rule_id: str = Field(min_length=1)
    classification: Classification
    risk: str = ""
    recommendation: str = ""
    effort: Optional[EffortLevel] = Field(
        default=None,
        description=(
            "Estimated manual conversion effort for a non-automatic item; "
            "None for AUTO (no manual work)."
        ),
    )
    kind: str = Field(
        default="OBJECT",
        min_length=1,
        description="Object kind (e.g. TABLE/VIEW/TRIGGER/ROUTINE) for grouping.",
    )


class AssessmentReport(BaseModel):
    """Result of the compatibility assessment over a source inventory."""

    model_config = ConfigDict(extra="forbid")

    items: list[AssessmentItem] = Field(default_factory=list)
    summary: dict[Classification, int] = Field(default_factory=dict)
    effort_summary: dict[EffortLevel, int] = Field(default_factory=dict)

    @classmethod
    def from_items(cls, items: list[AssessmentItem]) -> "AssessmentReport":
        """Build a report and compute per-classification and per-effort summaries.

        Every :class:`Classification` and :class:`EffortLevel` is present in the
        respective summary (0 when absent), so consumers never need to guard
        against missing keys. The effort summary counts only items that carry an
        effort estimate (the non-automatic ones).
        """
        summary = {classification: 0 for classification in Classification}
        effort_summary = {level: 0 for level in EffortLevel}
        for item in items:
            summary[item.classification] += 1
            if item.effort is not None:
                effort_summary[item.effort] += 1
        return cls(items=list(items), summary=summary, effort_summary=effort_summary)


# ---------------------------------------------------------------------------
# Data migration job state
# ---------------------------------------------------------------------------


class ChunkSpec(BaseModel):
    """A planned unit of work: a PK range of a table to migrate."""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    pk_range_start: Optional[str] = None
    pk_range_end: Optional[str] = None
    estimated_rows: int = Field(default=0, ge=0)


class ChunkState(BaseModel):
    """Runtime state of a single chunk, persisted for resumability."""

    model_config = ConfigDict(extra="forbid")

    chunk_id: str = Field(min_length=1)
    status: Literal["PENDING", "IN_PROGRESS", "DONE", "FAILED"] = "PENDING"
    rows_loaded: int = Field(default=0, ge=0)
    # Rows that already existed on the target and were skipped by the idempotent
    # ``INSERT ... ON CONFLICT DO NOTHING`` load (not newly inserted). Counting
    # these separately lets the completeness check treat a table as complete when
    # every source row is either newly loaded OR already present, instead of
    # falsely flagging a "row-count mismatch" for a table whose rows pre-existed.
    rows_skipped: int = Field(default=0, ge=0)
    attempts: int = Field(default=0, ge=0)
    # Wall-clock timing for per-table ETA (while running) and total elapsed (when
    # finished). Set when the chunk starts and reaches a terminal state.
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class Watermark(BaseModel):
    """A consistency point captured at the start of a data export (Property 11).

    A watermark pins the exact point-in-time a snapshot reflects so that the
    export can be audited, a future CDC catch-up can be resumed, and validation
    can compare source/target as-of this point (Requirements 5.7, 5.8).

    All fields except ``snapshot_timestamp`` and ``table_row_counts`` are
    optional because binlog/GTID metadata may be unavailable (e.g. binary
    logging disabled, or ``SHOW MASTER STATUS`` restricted on RDS/Aurora). The
    capturer records whatever is available and leaves the rest ``None`` while
    still producing a valid watermark.
    """

    model_config = ConfigDict(extra="forbid")

    binlog_file: Optional[str] = Field(
        default=None, description="MySQL binlog file name (e.g. 'mysql-bin.000123')."
    )
    binlog_position: Optional[int] = Field(
        default=None, ge=0, description="Position within the binlog file."
    )
    gtid_executed: Optional[str] = Field(
        default=None,
        description="The '@@GLOBAL.gtid_executed' set at the snapshot point.",
    )
    server_uuid: Optional[str] = Field(
        default=None, description="The source server's '@@GLOBAL.server_uuid'."
    )
    snapshot_timestamp: datetime = Field(
        description="UTC timestamp captured at the consistent-snapshot point."
    )
    table_row_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Per-table row counts captured at the snapshot point.",
    )
    row_counts_approximate: bool = Field(
        default=False,
        description=(
            "True when table_row_counts are approximate estimates read from "
            "information_schema (no COUNT(*) table scan, to minimize source "
            "load). Consumers that need exact counts (e.g. validation) must "
            "re-count rather than trust these as exact."
        ),
    )


class MigrationJob(BaseModel):
    """Aggregate state of a data migration job."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=1)
    status: Literal["PENDING", "RUNNING", "DONE", "FAILED", "CANCELLED"] = "PENDING"
    chunks: list[ChunkState] = Field(default_factory=list)
    progress_pct: float = Field(default=0.0, ge=0.0, le=100.0)
    error_count: int = Field(default=0, ge=0)
    watermark: Optional[Watermark] = Field(
        default=None,
        description=(
            "Consistency point captured at export start. Persisted as part of "
            "the job record so it survives with the job state (Requirement 5.7)."
        ),
    )


# ---------------------------------------------------------------------------
# Data Migration sub-flow: mode, table selection, prerequisites, error log
# ---------------------------------------------------------------------------


class MigrationMode(str, Enum):
    """Which load mode the Data Migration sub-flow runs for selected tables.

    - ``FULL_LOAD``: one-time snapshot load (watermark -> export -> import).
    - ``CDC``: continuous change streaming after the snapshot (Requirement 12).
    """

    FULL_LOAD = "FULL_LOAD"
    CDC = "CDC"


class TableSelection(BaseModel):
    """A user's table selection for the Data Migration sub-flow (multi-table).

    ``selected_tables`` holds qualified ``database.table`` names. An empty list
    means "select all", inferred at resolve time so the common case needs no
    clicks (Usability-first); see :class:`~dsql_migrator.core.table_selection.TableSelector`.
    """

    model_config = ConfigDict(extra="forbid")

    selected_tables: list[str] = Field(
        default_factory=list,
        description="Qualified 'database.table' names; empty => select all.",
    )


class PrerequisiteStatus(str, Enum):
    """Outcome of a single prerequisite check.

    Only a ``FAIL`` on a required check gates progression (Property 14). The
    others are all non-blocking, in decreasing severity:
    ``WARN`` -- something is off and worth attention; ``INFO`` -- an optional
    recommendation or an expected, no-action-needed state (e.g. GTID is off but
    CDC still works; MSK is not yet provisioned because it is created at deploy
    time); ``SKIP`` -- the check does not apply to the requested mode. ``INFO`` is
    deliberately quieter than ``WARN`` (it is not a problem), so the UI shows it
    with an info tone and does not auto-expand its section.
    """

    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    INFO = "INFO"
    SKIP = "SKIP"


class PrerequisiteCheckId(str, Enum):
    """Identifies each prerequisite check (common + CDC-only)."""

    # Common (Full Load + CDC)
    SOURCE_REACHABLE = "SOURCE_REACHABLE"
    REPLICATION_GRANTS = "REPLICATION_GRANTS"
    TABLE_PRIMARY_KEY = "TABLE_PRIMARY_KEY"
    TARGET_DSQL_REACHABLE = "TARGET_DSQL_REACHABLE"
    TARGET_IAM_AUTH = "TARGET_IAM_AUTH"
    TARGET_SCHEMA_READY = "TARGET_SCHEMA_READY"
    # CDC-only
    BINLOG_ROW_FORMAT = "BINLOG_ROW_FORMAT"
    GTID_MODE = "GTID_MODE"
    MSK_AVAILABLE = "MSK_AVAILABLE"
    MSK_CONNECT_AVAILABLE = "MSK_CONNECT_AVAILABLE"


class PrerequisiteResult(BaseModel):
    """Result of one prerequisite check.

    ``required`` marks whether a ``FAIL`` here gates progression. ``detail`` and
    ``remediation`` are English, user-facing, and credential-free (Property 7):
    ``detail`` describes what was observed and ``remediation`` is the actionable
    next step on FAIL/WARN. ``target`` names the subject (e.g. the table) for
    per-table checks.
    """

    model_config = ConfigDict(extra="forbid")

    check_id: PrerequisiteCheckId
    title: str = Field(min_length=1, description="English, user-facing check title.")
    status: PrerequisiteStatus
    required: bool = True
    target: Optional[str] = None
    detail: str = ""
    remediation: str = ""


class PrerequisiteCheckRequest(BaseModel):
    """Input for a prerequisite run: the mode and the resolved selected tables.

    ``tables`` carries the qualified ``database.table`` names of the resolved
    selection (from :class:`~dsql_migrator.core.table_selection.TableSelector`).
    Source/target connection access is supplied to the checker separately as
    read-only probes, never stored here as secrets (Property 7).
    """

    model_config = ConfigDict(extra="forbid")

    mode: MigrationMode
    tables: list[str] = Field(default_factory=list)


class PrerequisiteReport(BaseModel):
    """All prerequisite results for a mode, with the gating verdict.

    ``can_proceed`` is ``True`` iff no ``required`` check is ``FAIL`` (Property
    14). Use :meth:`build` to compute it from a list of results.
    """

    model_config = ConfigDict(extra="forbid")

    mode: MigrationMode
    results: list[PrerequisiteResult] = Field(default_factory=list)
    can_proceed: bool = False

    @classmethod
    def build(
        cls, mode: "MigrationMode", results: list["PrerequisiteResult"]
    ) -> "PrerequisiteReport":
        """Build a report and compute ``can_proceed`` from the results.

        ``can_proceed`` is ``True`` only when every required check passed (no
        required check is ``FAIL``); ``WARN``/``SKIP`` and non-required failures
        never block progression (Property 14).
        """
        results = list(results)
        can_proceed = not any(
            r.required and r.status == PrerequisiteStatus.FAIL for r in results
        )
        return cls(mode=mode, results=results, can_proceed=can_proceed)


class DataErrorRecord(BaseModel):
    """One data error captured during a load (Full Load or CDC).

    ``message`` is English and credential-free; row values and secret columns are
    never stored in plaintext (Property 7). ``pk`` is the failing row's
    primary-key value when known, and ``chunk_id`` ties the error to a load
    chunk when applicable.
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    pk: Optional[str] = None
    chunk_id: Optional[str] = None
    error_code: Optional[str] = None
    message: str = Field(min_length=1)
    occurred_at: datetime


class ErrorLogSummary(BaseModel):
    """UI summary of captured data errors for a job (Property 15).

    ``total_errors`` equals the sum of ``errors_by_table`` values, and
    ``log_available`` indicates whether a downloadable artifact exists (i.e.
    there is at least one error).
    """

    model_config = ConfigDict(extra="forbid")

    total_errors: int = Field(default=0, ge=0)
    errors_by_table: dict[str, int] = Field(default_factory=dict)
    log_available: bool = False


# ---------------------------------------------------------------------------
# Unified monitoring view (Full Load + CDC, single normalized shape)
# ---------------------------------------------------------------------------


class LoadKind(str, Enum):
    """Which load a :class:`LoadStatusView` describes."""

    FULL_LOAD = "FULL_LOAD"
    CDC = "CDC"


class TableStatusRow(BaseModel):
    """One table's live status, the shared row shape for both load modes.

    ``state`` is the per-table status (``PENDING``/``IN_PROGRESS``/``DONE``/
    ``FAILED`` for Full Load; ``RUNNING``/``FAILED`` for CDC). ``rows_loaded`` is
    populated for Full Load; ``errors`` comes from the single
    :class:`ErrorLogSummary` (Req 13.2).
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    state: str = Field(min_length=1)
    rows_loaded: Optional[int] = Field(default=None, ge=0)
    errors: int = Field(default=0, ge=0)


class LoadStatusView(BaseModel):
    """Normalized monitoring read-model rendered identically for Full Load/CDC.

    The control plane builds this from existing signals (Job progress, the single
    error log, and—-for CDC—-managed connector/CloudWatch metrics) so one UI
    component renders both modes without duplication (Req 13.1/13.4). Full Load
    populates ``progress_pct``/``tables_done``/``tables_failed`` (terminal); CDC
    populates ``lag_seconds``/``caught_up_to``/``connector_states``/``dlq_depth``
    (continuous). ``error_summary`` is the single error path shared by both.
    """

    model_config = ConfigDict(extra="forbid")

    kind: LoadKind
    tables: list[TableStatusRow] = Field(default_factory=list)
    # Full Load (terminal)
    progress_pct: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    tables_done: int = Field(default=0, ge=0)
    tables_failed: int = Field(default=0, ge=0)
    # CDC (continuous) — read from managed signals, never computed (Req 13.5)
    lag_seconds: Optional[float] = Field(default=None, ge=0.0)
    caught_up_to: Optional[datetime] = None
    connector_states: dict[str, str] = Field(default_factory=dict)
    dlq_depth: Optional[int] = Field(default=None, ge=0)
    # Single error path shared by Full Load and CDC (Req 13.2)
    error_summary: Optional[ErrorLogSummary] = None


# ---------------------------------------------------------------------------
# Workflow steps
# ---------------------------------------------------------------------------


class StepStatus(str, Enum):
    """Status of a top-level workflow step."""

    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    DONE = "DONE"
    FAILED = "FAILED"


class WorkflowState(BaseModel):
    """Status of the top-level migration steps shown in the UI."""

    model_config = ConfigDict(extra="forbid")

    # Migration plan: the first post-Connect step where the migration pattern is
    # chosen (and CDC infra optionally provisioned early). Prerequisite of
    # Evaluation. Defaults NOT_STARTED; older snapshots lacking it still load.
    migration_plan: StepStatus = StepStatus.NOT_STARTED
    evaluation: StepStatus = StepStatus.NOT_STARTED
    schema_conversion: StepStatus = StepStatus.NOT_STARTED
    # Data Migration is split into two independent sub-steps: a one-shot Full
    # Load and an optional continuous CDC. Either, both, or neither may run; they
    # depend on Schema Conversion, not on each other. ``data_migration`` is kept
    # for back-compat with older persisted snapshots and is no longer a step.
    data_migration: StepStatus = StepStatus.NOT_STARTED
    full_load: StepStatus = StepStatus.NOT_STARTED
    cdc: StepStatus = StepStatus.NOT_STARTED
    validation: StepStatus = StepStatus.NOT_STARTED
    # Cut over: the final step guiding the operator through switching the
    # application from MySQL to DSQL. The tool cannot run/verify it, so it is
    # marked DONE by the user acknowledging the cut-over. Prerequisite: Validation.
    # Defaults NOT_STARTED; older snapshots lacking it still load.
    cut_over: StepStatus = StepStatus.NOT_STARTED


# ---------------------------------------------------------------------------
# AI-assisted conversion (optional, augmenting)
# ---------------------------------------------------------------------------


class AiAssistConfig(BaseModel):
    """User-controlled settings for the optional AI-assisted conversion (Req 11).

    AI assist augments the deterministic (sqlglot) conversion path; it never
    replaces it. It is opt-in: ``enabled`` defaults to ``False`` so the workflow
    runs the deterministic-only path unless the user explicitly turns it on
    (Requirements 11.1, 11.2). ``model_id`` maps to the ``BEDROCK_MODEL_ID``
    setting (default ``global.anthropic.claude-sonnet-4-6``) and ``region`` to
    ``BEDROCK_REGION`` (Requirements 11.3, 11.4); neither carries any credential
    value. The default must be a real Bedrock model / inference-profile id (it is
    passed verbatim as ``modelId`` to ``invoke_model``), not a display name.
    """

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    model_id: str = Field(
        default="global.anthropic.claude-sonnet-4-6",
        min_length=1,
        description="Bedrock model id to use (config key: BEDROCK_MODEL_ID).",
    )
    region: Optional[str] = Field(
        default=None, description="Bedrock region (config key: BEDROCK_REGION)."
    )


class AiConversionSuggestion(BaseModel):
    """A reviewable AI conversion suggestion for one MANUAL/UNSUPPORTED object.

    The model output is treated as untrusted data: a suggestion is only ever
    applied after a human explicitly approves it (Requirement 11.7, 11.8).
    ``status`` tracks the review lifecycle (``PENDING_REVIEW`` ->
    ``EDITED``/``APPROVED``/``REJECTED``) and ``approved_by_user`` records the
    explicit approval. Only an ``APPROVED`` suggestion may flow to the Schema
    Applier path; rejected/pending/edited suggestions never reach apply
    (Property 13 / Requirements 10.9, 11.7). ``model_id`` carries provenance:
    which Bedrock model produced the suggestion.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1)
    kind: Literal["SCHEMA", "DATA", "QUERY"]
    suggested_sql_or_expr: str
    rationale: str = ""
    confidence: Optional[float] = None
    model_id: str = Field(min_length=1)
    status: Literal["PENDING_REVIEW", "EDITED", "APPROVED", "REJECTED"] = (
        "PENDING_REVIEW"
    )
    approved_by_user: bool = False


class AiAssessmentInsight(BaseModel):
    """AI-generated, advisory analysis for one assessed source object (Req 11).

    Attached to a deterministic :class:`AssessmentItem` by ``object_name``. The
    deterministic classification and effort remain authoritative (Property 8 is
    unaffected); this only adds an expert remediation ``recommendation``, a
    ``rationale``, and an optional AI ``ai_effort`` opinion shown alongside the
    deterministic estimate. It is treated as untrusted, advisory output and is
    never auto-applied.
    """

    model_config = ConfigDict(extra="forbid")

    object_name: str = Field(min_length=1)
    recommendation: str = ""
    rationale: str = ""
    ai_effort: Optional[EffortLevel] = Field(
        default=None,
        description="AI's effort opinion; advisory, never overrides the rule effort.",
    )


class AiAssessmentFinding(BaseModel):
    """An AI-proposed additional risk the deterministic rules did not flag.

    Advisory only: it never changes a deterministic classification and is
    surfaced for human review. ``area`` names the object or topic the finding
    concerns.
    """

    model_config = ConfigDict(extra="forbid")

    area: str = Field(min_length=1)
    risk: str = ""
    recommendation: str = ""


class AiAssessmentReport(BaseModel):
    """AI-led migration assessment that augments the deterministic report (Req 11).

    The deterministic :class:`AssessmentReport` remains the factual backbone
    (Property 8). This adds an overall migration ``strategy_summary`` narrative,
    per-object expert ``insights``, and ``additional_findings`` the rules did not
    catch. ``model_id`` carries provenance. All content is untrusted/advisory and
    never overrides deterministic facts; the deterministic path runs first and
    stands alone when AI is disabled or unavailable (Requirements 11.1, 11.2,
    11.10).
    """

    model_config = ConfigDict(extra="forbid")

    strategy_summary: str = ""
    insights: list[AiAssessmentInsight] = Field(default_factory=list)
    additional_findings: list[AiAssessmentFinding] = Field(default_factory=list)
    model_id: str = Field(min_length=1)


class AiAccessCheckResult(BaseModel):
    """Outcome of the "Verify AI access" Bedrock preflight (Req 11.13, 11.14).

    A lightweight, non-blocking check of whether the configured model
    (``BEDROCK_MODEL_ID``) and region (``BEDROCK_REGION``) can be reached via
    Amazon Bedrock ``InvokeModel``. ``ok`` is ``True`` only when the preflight
    succeeded; otherwise ``reason`` names the specific failure
    (``ACCESS_DENIED`` / ``MODEL_NOT_ENABLED`` / ``THROTTLED`` / ``UNKNOWN``) and
    ``detail`` is a credential-free, actionable next-step message (Requirement
    11.15 / Property 7). ``model_id`` and ``region`` echo the configuration the
    check ran against. This only reports connectivity/permission; it does not
    alter the AI suggestion review gate (Property 13).
    """

    model_config = ConfigDict(extra="forbid")

    ok: bool
    reason: Literal["OK", "ACCESS_DENIED", "MODEL_NOT_ENABLED", "THROTTLED", "UNKNOWN"]
    detail: str = ""
    model_id: str = Field(min_length=1)
    region: Optional[str] = None


# ---------------------------------------------------------------------------
# Consistency validation
# ---------------------------------------------------------------------------


class ValidationMode(str, Enum):
    """How strictly the validator compares source and target data (Req 6.1/6.2).

    - ``ROW_COUNT``: compare per-table row counts only (cheap).
    - ``CHECKSUM``: also compare an order-independent per-table data checksum, so
      a reported match means the data itself is equal (Property 9).
    """

    ROW_COUNT = "ROW_COUNT"
    CHECKSUM = "CHECKSUM"


class RowDiffKind(str, Enum):
    """How a sampled primary key diverges between source and target (dev diag)."""

    MISSING_ON_TARGET = "MISSING_ON_TARGET"  # PK present on source, absent on target
    EXTRA_ON_TARGET = "EXTRA_ON_TARGET"      # PK present on target, absent on source
    VALUE_MISMATCH = "VALUE_MISMATCH"        # PK on both sides, per-row checksum differs


class RowDiffFinding(BaseModel):
    """One sampled primary key that diverges between source and target.

    A diagnostic detail that explains an already-failed table-level check by
    naming WHICH primary keys differ. Carries the primary-key value and the
    per-row checksum token on each side -- never the row's column VALUES, which
    could be PII (Property 7). A natural-key primary key is itself the operator's
    risk; the feature is dev-gated and default-off.
    """

    model_config = ConfigDict(extra="forbid")

    pk: str = Field(min_length=1)
    kind: RowDiffKind
    source_checksum: Optional[str] = None
    target_checksum: Optional[str] = None


class RowDiffSample(BaseModel):
    """A bounded sample of diverging primary keys for one mismatched table.

    Computed ONLY for a table that already failed the table-level check, ONLY at
    Validation time, and bounded to ``sample_size`` rows (``ORDER BY pk LIMIT N``)
    so it never scans a large table. It is an explanatory SAMPLE, not the verdict:
    the verdict is the table-level checksum, and ``truncated`` flags that more
    diffs may exist beyond the sampled window.
    """

    model_config = ConfigDict(extra="forbid")

    pk_column: str = Field(min_length=1)
    sample_size: int = Field(ge=0)
    truncated: bool = False
    findings: list["RowDiffFinding"] = Field(default_factory=list)


class ReconcileResult(BaseModel):
    """Full primary-key set reconciliation between source and target for one table.

    Stronger than a row ``COUNT(*)``: every primary key on both sides is compared
    (via a bounded, streaming keyset merge that never materializes a whole table)
    to report the EXACT divergence for a pre-cut-over readiness check. This is the
    "no mismatched records" guarantee Validation makes before cut-over:

    - ``missing_on_target`` -- PKs present on the source but absent on the target
      (rows that never landed: a lost Full Load row, or an INSERT that CDC has not
      yet replicated).
    - ``extra_on_target`` -- PKs present on the target but absent on the source (a
      source ``DELETE`` that CDC has not yet replicated, or a stray row).

    A table is reconciled (``consistent``) only when BOTH counts are zero. The
    sample PK lists are bounded (``*_sample``) so a large divergence never bloats
    the report, and ``sample_truncated`` flags that more PKs diverge than are
    listed. Only primary-key VALUES are carried, never row column values
    (Property 7); reconciliation is limited to single-column, integer-like PKs so
    the merge order is well-defined cross-engine and never scans a large table.
    """

    model_config = ConfigDict(extra="forbid")

    pk_column: str = Field(min_length=1)
    source_count: int = Field(ge=0)
    target_count: int = Field(ge=0)
    missing_on_target: int = Field(default=0, ge=0)
    extra_on_target: int = Field(default=0, ge=0)
    missing_sample: list[str] = Field(default_factory=list)
    extra_sample: list[str] = Field(default_factory=list)
    sample_truncated: bool = False
    consistent: bool = True


class TableValidationResult(BaseModel):
    """Per-table source/target comparison outcome (Requirements 6.1, 6.2).

    ``matched`` is the table-level verdict and is ``True`` only when the row
    counts are equal, (in :attr:`ValidationMode.CHECKSUM` mode) the checksums are
    equal, the full PK reconciliation (when run) found no missing/extra rows, and
    the table did not error. This makes a reported match sound: a ``True`` here
    never hides an unequal row count/checksum, a divergent record, or a table
    that could not be compared (Property 9).
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    source_row_count: int = Field(ge=0)
    target_row_count: int = Field(ge=0)
    row_count_match: bool
    source_checksum: Optional[str] = None
    target_checksum: Optional[str] = None
    checksum_match: Optional[bool] = None
    matched: bool
    # Dev-only diagnostic sample of diverging PKs, populated only when this table
    # did NOT match AND the row-diff sample size is configured > 0. None otherwise.
    row_diff_sample: Optional["RowDiffSample"] = None
    # Full PK-set reconciliation for this table (record-level missing/extra), when
    # the reconciliation pass ran and the table's PK is eligible. None otherwise.
    reconcile: Optional["ReconcileResult"] = None
    # Per-table error message: set when THIS table's comparison failed (e.g. it is
    # absent on the target, or a query errored), so one bad table is isolated and
    # reported instead of aborting the whole validation run. None on success.
    error: Optional[str] = None
    # True when the fast sweep (deep-check only on count mismatch) verified this
    # table by ROW COUNT alone and skipped the checksum/reconciliation because the
    # counts agreed. Lets the UI label it "verified by count" honestly -- distinct
    # from a composite-PK reconcile skip -- so a count-only pass is never shown as a
    # full record-level match. ``matched`` then means "counts equal", not "rows
    # proven identical".
    deep_checks_skipped: bool = False


class OrphanFinding(BaseModel):
    """An orphan-record finding on the target for a preserved referential rule.

    Because DSQL has no foreign keys, referential integrity moves to the
    application; this records child rows whose foreign-key value has no matching
    parent row on the target (Requirement 6.3). Only actual orphans (count > 0)
    are reported.
    """

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1)
    foreign_key: str = Field(min_length=1)
    referenced_table: str = Field(min_length=1)
    orphan_count: int = Field(ge=1)


class DriftReport(BaseModel):
    """Source change since the export watermark, by GTID comparison (Req 6.5).

    For a live source, validation is performed as-of the recorded watermark; this
    reports whether the source has advanced since then (the current
    ``gtid_executed`` differs from the watermark's), so a reviewer knows the
    comparison reflects the snapshot, not necessarily the source's current state
    (Property 11).
    """

    model_config = ConfigDict(extra="forbid")

    watermark_gtid: Optional[str] = None
    current_gtid: Optional[str] = None
    drifted: bool = False
    detail: str = ""


class ValidationReport(BaseModel):
    """Result of validating migrated data against the source (Requirement 6.4).

    ``is_match`` is the overall verdict: ``True`` only when every table matched
    and no orphan records were found. Combined with the per-table soundness of
    :class:`TableValidationResult`, this guarantees a reported overall match
    reflects genuinely equal data within the compared scope (Property 9). The
    ``drift`` field reports source changes since the watermark (Property 11).
    """

    model_config = ConfigDict(extra="forbid")

    mode: ValidationMode
    items: list[TableValidationResult] = Field(default_factory=list)
    orphan_findings: list[OrphanFinding] = Field(default_factory=list)
    orphan_check_performed: bool = False
    drift: Optional[DriftReport] = None
    snapshot_timestamp: Optional[datetime] = None
    is_match: bool = False

    @classmethod
    def build(
        cls,
        *,
        mode: "ValidationMode",
        items: list["TableValidationResult"],
        orphan_findings: Optional[list["OrphanFinding"]] = None,
        orphan_check_performed: bool = False,
        drift: Optional["DriftReport"] = None,
        snapshot_timestamp: Optional[datetime] = None,
    ) -> "ValidationReport":
        """Build a report and compute the sound overall ``is_match`` verdict.

        ``is_match`` is ``True`` only when every table matched and no orphan
        findings were reported, so it never claims a match while a row
        count/checksum differs or an orphan exists (Property 9).
        """
        findings = list(orphan_findings or [])
        items = list(items)
        is_match = all(item.matched for item in items) and not findings
        return cls(
            mode=mode,
            items=items,
            orphan_findings=findings,
            orphan_check_performed=orphan_check_performed,
            drift=drift,
            snapshot_timestamp=snapshot_timestamp,
            is_match=is_match,
        )


__all__ = [
    "SourceConnectionConfig",
    "ConnectionResult",
    "TargetConnectionConfig",
    "ColumnDef",
    "IndexDef",
    "ForeignKeyDef",
    "TableDef",
    "ViewDef",
    "ObjectType",
    "ObjectRef",
    "SourceInventory",
    "TargetObjectKind",
    "TargetColumnDef",
    "TargetIndexDef",
    "TargetRelation",
    "TargetSchemaNode",
    "TargetInventory",
    "ApplyMode",
    "ApplyStatus",
    "DdlPreview",
    "ApplyResult",
    "Classification",
    "AssessmentItem",
    "AssessmentReport",
    "EffortLevel",
    "ChunkSpec",
    "ChunkState",
    "Watermark",
    "MigrationJob",
    "MigrationMode",
    "TableSelection",
    "PrerequisiteStatus",
    "PrerequisiteCheckId",
    "PrerequisiteResult",
    "PrerequisiteCheckRequest",
    "PrerequisiteReport",
    "DataErrorRecord",
    "ErrorLogSummary",
    "LoadKind",
    "TableStatusRow",
    "LoadStatusView",
    "StepStatus",
    "WorkflowState",
    "AiAssistConfig",
    "AiConversionSuggestion",
    "AiAssessmentInsight",
    "AiAssessmentFinding",
    "AiAssessmentReport",
    "AiAccessCheckResult",
    "ValidationMode",
    "RowDiffKind",
    "RowDiffFinding",
    "RowDiffSample",
    "ReconcileResult",
    "TableValidationResult",
    "OrphanFinding",
    "DriftReport",
    "ValidationReport",
]
