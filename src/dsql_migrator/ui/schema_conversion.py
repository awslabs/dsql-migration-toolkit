# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Step 2 (Schema Conversion) screen of the four-step migration workflow.

This is the AWS Schema Conversion Tool-like (SCT-like) experience the design
maps to the Schema Conversion step (design.md "Schema Apply Design"). From the
source inventory produced by Step 1 (Evaluation) it lets the user:

1. browse source objects (schemas / tables / views) as a tree (Requirement 10.1),
2. select an object and compare its current source DDL against the converted
   target DDL side by side (Requirement 10.2),
3. when the same object already exists on the target, see the existence/conflict
   and choose SKIP or REPLACE, where the destructive REPLACE requires an
   explicit confirmation (Requirements 10.3, 10.6),
4. apply the converted DDL to the target DSQL and view a per-object result
   (CREATED / SKIPPED / FAILED + error) (Requirements 10.4, 10.7, Property 12).

Engine seams (Task 15 dependency). The Target Introspector and Schema Applier
(design.md sections 9-10, Task 15) are not implemented yet, so this screen is
written against two small, injectable :class:`typing.Protocol` seams consistent
with design.md: :class:`TargetExistenceChecker` (for the existence/conflict
display) and :class:`SchemaApplier` (for the apply path). A reference
:class:`OccRetryingSchemaApplier` that keeps the apply path's safety semantics
(single-DDL transaction per statement and OC001 idempotent retry via
:func:`~dsql_migrator.core.occ.with_occ_retry`) is provided so the apply
orchestration is complete and testable now; when Task 15 lands its real applier
can replace it through the same seam. When no applier is wired the screen surfaces
a clear status rather than silently breaking.

Safety (Property 12). The apply orchestration enforces the safety semantics it
owns: a destructive REPLACE is never applied without an explicit confirmation,
each statement is applied as a single DDL (one transaction each), and per-object
results are reported. The apply path takes only target-side collaborators and no
source handle, so it cannot write to the source (Property 1 / Requirement 10.8).

As with the Evaluation screen, the orchestration, tree assembly, DDL pairing,
conflict decision, and query-conversion assembly below are independent of
NiceGUI so they can be unit tested directly; only
:func:`build_schema_conversion_screen` and its render helpers touch NiceGUI.
"""

from __future__ import annotations

import difflib
import inspect
import logging
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Mapping, Optional, Protocol, Sequence

from dsql_migrator.core.ai_assistant import validate_suggested_sql
from dsql_migrator.core.assessment_strategist import (
    AssessmentStrategist,
    build_conversion_chat_system,
)
from dsql_migrator.core.converter import (
    ConversionWarning,
    PrimaryKeyStrategy,
    SchemaConversionResult,
    SchemaConvertOptions,
    SchemaConverter,
    TableConversion,
    ViewConversion,
    parse_target_primary_key,
    validate_composite_leading_column,
)
from dsql_migrator.core.models import (
    AiAssistConfig,
    AiConversionSuggestion,
    AssessmentReport,
    Classification,
    SourceInventory,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
    TargetInventory,
    ViewDef,
)
from dsql_migrator.core.occ import (
    DEFAULT_BASE_DELAY_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    JitterFunc,
    SleepFunc,
    with_occ_retry,
)
from dsql_migrator.core.job_manager import JobManager, JobNotFoundError
from dsql_migrator.ui.ai_assist import (
    AI_STATUS_APPROVED,
    AI_STATUS_EDITED,
    AI_STATUS_PENDING_REVIEW,
    AI_STATUS_REJECTED,
    AiConversionAssistant,
    approve_suggestion,
    approved_suggestions,
    reject_suggestion,
)
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.ui.design import inline_hint, render_notice, segmented_control
from dsql_migrator.ui.ai_chat_drawer import build_chat_drawer
from dsql_migrator.ui.evaluation import EvaluationStore, classification_label
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    WorkflowStep,
    get_status,
    status_label,
    with_status,
)

# Tree node id prefixes, so a selected node maps back to a source object.
TABLE_PREFIX = "table:"
VIEW_PREFIX = "view:"
TRIGGER_PREFIX = "trigger:"
ROUTINE_PREFIX = "routine:"

# Text shown as the converted target DDL for objects the deterministic converter
# does not auto-convert (views, triggers, routines). They require manual
# reimplementation for DSQL; the screen never fabricates a target DDL for them.
_NOT_AUTO_CONVERTED = (
    "-- Not auto-converted for Aurora DSQL. Reimplement this object manually."
)


# ---------------------------------------------------------------------------
# Apply contracts (NiceGUI-agnostic) -- the Task 15 Schema Applier seam
# ---------------------------------------------------------------------------


class ApplyMode(str, Enum):
    """How to apply a converted object when it already exists on the target.

    - ``SKIP_IF_EXISTS``: leave an existing object untouched (the safe default).
    - ``REPLACE``: drop and recreate the object. This is destructive and
      therefore requires an explicit confirmation before it is applied
      (Requirement 10.6 / Property 12).
    """

    SKIP_IF_EXISTS = "SKIP_IF_EXISTS"
    REPLACE = "REPLACE"


class ApplyOutcome(str, Enum):
    """The outcome of applying one object through a :class:`SchemaApplier`."""

    CREATED = "CREATED"
    SKIPPED = "SKIPPED"


class ObjectApplyStatus(str, Enum):
    """Per-object apply result reported to the user (Requirement 10.7)."""

    CREATED = "CREATED"
    SKIPPED = "SKIPPED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ObjectApplyResult:
    """The result of applying a single object: status plus an optional detail."""

    object_name: str
    status: ObjectApplyStatus
    detail: str = ""


@dataclass(frozen=True)
class StatementApplyResult:
    """The result of applying one DDL statement of an object (Requirement 10.7).

    An object (a table) is applied as several single DDL statements (its
    ``CREATE SCHEMA``/``CREATE TABLE`` plus ``CREATE INDEX ASYNC`` statements),
    so a per-statement result lets the screen report exactly which statement(s)
    failed -- e.g. the table was created but one index failed -- instead of
    surfacing only the first failing statement's message. ``label`` is a short
    human-readable description (e.g. ``TABLE categories``).
    """

    label: str
    status: ObjectApplyStatus
    detail: str = ""


def format_statement_summary(statements: Sequence[StatementApplyResult]) -> str:
    """Render per-statement results as one compact line for the apply detail.

    Each statement is shown as ``<label>: <STATUS>``; a failure also appends its
    reason. Statements are joined with ``; `` so the object's per-statement
    outcome fits in a single detail cell/badge.
    """
    parts: list[str] = []
    for statement in statements:
        text = f"{statement.label}: {statement.status.value}"
        if statement.status is ObjectApplyStatus.FAILED and statement.detail:
            text += f" — {statement.detail}"
        parts.append(text)
    return "; ".join(parts)


class ObjectApplyError(RuntimeError):
    """Raised when one or more statements of an object failed to apply.

    Carries the per-statement results so :func:`run_schema_apply` can report
    exactly which statement(s) failed (e.g. the table was created but an index
    failed), instead of surfacing only the first failing statement's message.
    """

    def __init__(
        self, object_name: str, statements: Sequence[StatementApplyResult]
    ) -> None:
        self.object_name = object_name
        self.statements: tuple[StatementApplyResult, ...] = tuple(statements)
        super().__init__(format_statement_summary(statements))


# The parsed CREATE kind for an index (matches the core applier's parser). An
# index statement is a leaf: its failure does not abort sibling statements,
# whereas a failed structural statement (SCHEMA/TABLE) aborts dependent indexes.
_INDEX_KIND = "INDEX"

# The parsed CREATE kind for a schema. ``CREATE SCHEMA IF NOT EXISTS`` is
# idempotent scaffolding the core applier always reports as CREATED ("schema
# ensured"), so it must NOT drive a per-object CREATED/SKIPPED verdict -- a
# re-apply of an existing qualified table would otherwise look CREATED just
# because its schema was re-ensured.
_SCHEMA_KIND = "SCHEMA"

# The parsed CREATE kind for a view. A view selecting from a table blocks that
# table's DROP during a destructive REPLACE, so apply-set views are dropped in a
# pre-pass before any table is recreated (see _predrop_dependent_views).
_VIEW_KIND = "VIEW"


def _describe_ddl(ddl: str) -> tuple[str, Optional[str]]:
    """Return a short ``(label, kind)`` for one DDL statement.

    ``kind`` is the parsed CREATE kind (``TABLE``/``INDEX``/...) or ``None`` when
    the statement cannot be parsed (e.g. a test placeholder); ``label`` is a
    compact human-readable description such as ``TABLE categories`` used in the
    per-statement apply summary.
    """
    from dsql_migrator.core.schema_applier import (
        SchemaApplyError,
        parse_create_object,
    )

    try:
        name, kind = parse_create_object(ddl)
        return f"{kind} {name}", kind
    except SchemaApplyError:
        snippet = " ".join(ddl.split())[:60]
        return (snippet or "statement"), None


def _clean_detail(detail: str) -> str:
    """Drop the core applier's ``Apply failed:`` prefix for the summary line.

    The per-statement summary already renders ``... : FAILED``, so the redundant
    prefix is stripped to keep the failure reason readable.
    """
    prefix = "Apply failed: "
    return detail[len(prefix):] if detail.startswith(prefix) else detail


@dataclass(frozen=True)
class ApplyObject:
    """One object to apply: its name and the ordered single DDL statements.

    Each entry in ``ddls`` is exactly one DDL statement (no terminator), so a
    statement is the boundary of one transaction when applied (Requirement 10.4 /
    Property 2). For a table the list is the ``CREATE TABLE`` followed by its
    ``CREATE INDEX ASYNC`` statements.
    """

    object_name: str
    ddls: tuple[str, ...]
    # When set, the object has no applicable CREATE DDL (e.g. a table the
    # converter could not auto-convert, like MySQL spatial types). It is reported
    # as SKIPPED with this reason and never sent to the applier.
    skip_reason: Optional[str] = None


class TargetExistenceChecker(Protocol):
    """Reports whether an object already exists on the target (Requirement 10.3).

    Mirrors the design's ``TargetIntrospector.object_exists`` (design.md section
    9). Used only to *display* existence/conflict before applying; the apply
    decision itself is made by the :class:`SchemaApplier`.
    """

    def object_exists(self, object_name: str) -> bool:
        """Return ``True`` if ``object_name`` already exists on the target."""


class SchemaApplier(Protocol):
    """Applies one converted object's DDL to the target (design.md section 10).

    The implementation MUST apply each statement as a single DDL in its own
    transaction and idempotently retry an OC001 (``SQLSTATE 40001``) schema
    conflict (Requirements 10.4, 10.5 / Property 12). It returns
    :attr:`ApplyOutcome.CREATED` when the object was created/replaced and
    :attr:`ApplyOutcome.SKIPPED` when an existing object was left untouched under
    :attr:`ApplyMode.SKIP_IF_EXISTS`; it raises on failure. The applier accesses
    only the target and never the source (Requirement 10.8 / Property 1).
    """

    def apply_object(
        self, object_name: str, ddls: Sequence[str], on_conflict: ApplyMode
    ) -> ApplyOutcome:
        """Apply ``ddls`` for ``object_name`` honoring ``on_conflict``."""


# A factory that builds a :class:`SchemaApplier` for a configured target.
ApplierFactory = Callable[[TargetConnectionConfig], SchemaApplier]


# ---------------------------------------------------------------------------
# Reference Schema Applier (keeps Property 12 apply safety semantics)
# ---------------------------------------------------------------------------


class TargetDdlExecutor(Protocol):
    """Low-level target catalog/DDL operations the reference applier delegates to.

    This is the seam the real Task 15 target layer (``psycopg`` over DSQL) plugs
    into: ``object_exists`` reads the catalog, ``execute_ddl`` runs one DDL in
    its own (autocommit) transaction, and ``drop_object`` removes an object for a
    REPLACE. Every method touches only the target.
    """

    def object_exists(self, object_name: str) -> bool:
        """Return whether ``object_name`` exists on the target."""

    def execute_ddl(self, ddl: str) -> None:
        """Execute exactly one DDL statement in its own transaction."""

    def drop_object(self, object_name: str) -> None:
        """Drop ``object_name`` from the target (for a REPLACE)."""


class OccRetryingSchemaApplier:
    """Reference :class:`SchemaApplier` that keeps the apply path's safety rules.

    It delegates catalog/DDL work to an injected :class:`TargetDdlExecutor` and:

    - checks existence once per object and, under
      :attr:`ApplyMode.SKIP_IF_EXISTS`, returns :attr:`ApplyOutcome.SKIPPED`
      without writing anything (Requirement 10.3);
    - for :attr:`ApplyMode.REPLACE`, drops the existing object first
      (destructive; the caller must already have obtained explicit confirmation
      -- Requirement 10.6 / Property 12);
    - applies each statement individually so every statement runs as a single
      DDL in its own transaction (Requirement 10.4 / Property 2); and
    - wraps each target write with :func:`~dsql_migrator.core.occ.with_occ_retry`
      so an OC001 (``SQLSTATE 40001``) schema conflict is idempotently retried
      (Requirement 10.5 / Property 5).

    It never receives or touches a source connection, so the apply path cannot
    write to the source (Requirement 10.8 / Property 1).
    """

    def __init__(
        self,
        executor: TargetDdlExecutor,
        *,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
        sleep: Optional[SleepFunc] = None,
        jitter: Optional[JitterFunc] = None,
    ) -> None:
        """Build the applier around ``executor`` with an OCC-retry policy.

        ``sleep``/``jitter`` are forwarded to
        :func:`~dsql_migrator.core.occ.with_occ_retry` so tests can make retries
        deterministic and instant.
        """
        self._executor = executor
        retry_kwargs: dict[str, object] = {}
        if sleep is not None:
            retry_kwargs["sleep"] = sleep
        if jitter is not None:
            retry_kwargs["jitter"] = jitter
        retry = with_occ_retry(max_attempts, base_delay, **retry_kwargs)  # type: ignore[arg-type]
        self._execute = retry(self._executor.execute_ddl)
        self._drop = retry(self._executor.drop_object)

    def apply_object(
        self, object_name: str, ddls: Sequence[str], on_conflict: ApplyMode
    ) -> ApplyOutcome:
        """Apply ``ddls`` for ``object_name`` per :class:`SchemaApplier`."""
        if self._executor.object_exists(object_name):
            if on_conflict is ApplyMode.SKIP_IF_EXISTS:
                return ApplyOutcome.SKIPPED
            # REPLACE: destructive recreation. Confirmation is enforced upstream
            # (run_schema_apply) before this method is ever reached.
            self._drop(object_name)
        for ddl in ddls:
            self._execute(ddl)
        return ApplyOutcome.CREATED


# ---------------------------------------------------------------------------
# Apply orchestration (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


def _is_applicable_target_ddl(target_ddl: str) -> bool:
    """Return True when the target DDL is an applicable ``CREATE`` statement.

    A converter that could not auto-convert an object emits a comment placeholder
    instead of a ``CREATE`` -- either the generic not-converted note (views,
    triggers, routines) or a specific reason (e.g. MySQL spatial columns Aurora
    DSQL has no type for). Such an object is shown read-only (no editable target,
    no Apply button) and surfaced for manual reimplementation; it is never sent to
    the applier.
    """
    return target_ddl.lstrip()[:6].upper() == "CREATE"


logger = logging.getLogger(__name__)


def _tree_leaf_ids(nodes: Sequence[dict]) -> list[str]:
    """Collect every leaf node id from a ``ui.tree`` node list (depth-first).

    A leaf is a node with no ``children`` -- i.e. an object (table / view /
    trigger / routine) rather than a schema or category grouping. Used by the
    source browser's "Select all" to tick exactly the selectable object leaves.
    """
    ids: list[str] = []
    for node in nodes:
        children = node.get("children")
        if children:
            ids.extend(_tree_leaf_ids(children))
        elif "id" in node:
            ids.append(str(node["id"]))
    return ids


def _safe_post_await_ui(client: object, action: Callable[[], None]) -> None:
    """Run a UI action after a slow ``await`` in the originating client's context.

    A slow ``await`` (e.g. ``run.io_bound`` for a per-object apply) can outlive the
    slot/element that triggered it: a page re-render or navigation deletes that slot,
    after which ``ui.notify`` / a refresh raise ``RuntimeError: The parent element
    this slot belongs to has been deleted``. Re-entering the captured client restores
    a valid context so the feedback still lands; a fully disconnected client is
    ignored so the background task never crashes (feedback is best-effort).
    """
    try:
        with client:  # type: ignore[attr-defined]
            action()
    except Exception:  # noqa: BLE001 - client/slot gone; UI feedback is best-effort
        logger.debug("Post-await UI update skipped: client context unavailable")


def _apply_should_replace(*, apply_mode: ApplyMode, edited: bool) -> bool:
    """Return True when applying an object must use REPLACE (drop + recreate).

    REPLACE is required when the global mode is REPLACE, OR when the user EDITED
    the object's target DDL: SKIP would leave an already-existing object untouched
    and silently drop the edit, so an edit can only reliably take effect via
    REPLACE. (REPLACE on an object that does not yet exist is a no-op DROP IF
    EXISTS followed by CREATE, so it is safe regardless of existence -- which also
    avoids depending on a possibly-stale UI existence check.)
    """
    return apply_mode is ApplyMode.REPLACE or edited


def _table_manual_reason(table: TableConversion) -> Optional[str]:
    """Return a reason when a table has no applicable CREATE DDL.

    The converter emits a comment placeholder (not a ``CREATE``) for a table it
    could not auto-convert -- e.g. MySQL spatial columns, which Aurora DSQL has no
    type for. Such a table must not be sent to the applier (which would reject
    the non-CREATE statement with a cryptic ``SchemaApplyError``); it is surfaced
    as SKIPPED with the redesign reason instead. Returns ``None`` for a normal,
    applicable table whose ``target_ddl`` is a ``CREATE`` statement.
    """
    if _is_applicable_target_ddl(table.target_ddl):
        return None
    for warning in table.warnings:
        if (
            warning.classification is Classification.UNSUPPORTED
            and warning.object_name == table.table
        ):
            return warning.message
    return table.target_ddl.lstrip("-").strip() or "Manual reimplementation required."


def build_apply_objects(result: SchemaConversionResult) -> list[ApplyObject]:
    """Flatten a conversion result into the per-object apply units.

    Each table :class:`ApplyObject` is its ``CREATE TABLE`` followed by its
    ``CREATE INDEX ASYNC`` statements; each auto-converted view is its
    ``CREATE VIEW`` (preceded by its schema), applied AFTER the tables it selects
    from. Each statement is applied as a single DDL in its own transaction
    (Property 2). A table the converter could not auto-convert (a comment
    placeholder, e.g. MySQL spatial types) carries a ``skip_reason`` so it is
    reported SKIPPED for manual reimplementation rather than sent to the applier.
    Views that could not be auto-converted (and triggers/routines) are surfaced
    for manual reimplementation instead of applied here.
    """
    objects: list[ApplyObject] = []
    for table in result.tables:
        reason = _table_manual_reason(table)
        if reason is not None:
            objects.append(
                ApplyObject(object_name=table.table, ddls=(), skip_reason=reason)
            )
        else:
            objects.append(
                ApplyObject(
                    object_name=table.table,
                    ddls=(*table.schema_ddls, table.target_ddl, *table.index_ddls),
                )
            )
    objects.extend(
        ApplyObject(
            object_name=view.view,
            ddls=(*view.schema_ddls, view.target_ddl),
        )
        for view in result.views
        if view.auto_converted
    )
    return objects


def override_apply_objects(
    objects: Sequence[ApplyObject],
    edited_ddls: dict[str, str],
) -> list[ApplyObject]:
    """Apply user edits to the deterministic apply units (Requirement 10.2/10.4).

    For each object with a user-edited target DDL in ``edited_ddls``, the edited
    script replaces the deterministic statements: it is split into single DDL
    statements so each is still applied in its own transaction (Property 2). An
    object with no edit, or whose edit contains no statement, keeps its
    deterministic DDL (so an accidental blank never silently applies nothing).
    Order is preserved.
    """
    overridden: list[ApplyObject] = []
    for obj in objects:
        edited = edited_ddls.get(obj.object_name)
        if edited is not None:
            ddls = tuple(split_sql_statements(edited))
            if ddls:
                overridden.append(ApplyObject(object_name=obj.object_name, ddls=ddls))
                continue
        overridden.append(obj)
    return overridden


# ---------------------------------------------------------------------------
# AI-assisted conversion: candidate selection + review-gate apply units
# ---------------------------------------------------------------------------


def ai_candidate_object_names(assessment: AssessmentReport) -> list[str]:
    """Return the object names eligible for an AI suggestion (Requirement 11.5).

    Only objects the deterministic assessment flagged ``MANUAL`` or
    ``UNSUPPORTED`` are eligible; auto-convertible objects keep their
    deterministic result and never trigger an AI call (avoiding unnecessary
    cost/latency). Order follows the assessment and duplicates are removed.
    """
    flagged = {Classification.MANUAL, Classification.UNSUPPORTED}
    names: list[str] = []
    for item in assessment.items:
        if item.classification in flagged and item.object_name not in names:
            names.append(item.object_name)
    return names


def composite_leading_candidates(table: TableDef) -> list[str]:
    """Columns eligible to lead a composite primary key for ``table``.

    A valid leading column is NOT NULL and not already part of the primary key
    (mirrors :func:`validate_composite_leading_column` so the picker never offers
    a choice that would then be rejected). Order follows the table's column order
    so the dropdown reads like the source schema.
    """
    return [
        column.name
        for column in table.columns
        if not column.nullable and column.name not in table.primary_key
    ]


def default_composite_leading(table: TableDef) -> Optional[str]:
    """A sensible default leading column, or ``None`` when none is eligible.

    Picks the first eligible NOT-NULL non-PK column so selecting "Composite key"
    always yields a representable (valid) state without forcing the user to also
    pick a column in the same click. The user can refine it via the dropdown.
    """
    candidates = composite_leading_candidates(table)
    return candidates[0] if candidates else None


def build_composite_conversion(
    converter: SchemaConverter, table: TableDef, leading: str
) -> TableConversion:
    """Per-table conversion for ``table`` under a composite PK on ``leading``.

    Runs the converter with the COMPOSITE_KEY strategy; the result's rendered
    script (CREATE TABLE with the ``(leading, original_pk...)`` key + the UNIQUE
    INDEX ASYNC that preserves the original key's uniqueness) is what the picker
    bakes into ``edited_target_ddls`` -- the same field Full Load and Schema Apply
    already consume -- so the composite choice is resume-safe (snapshotted) with no
    separate persisted state. An invalid/too-large leading yields an UNSUPPORTED
    conversion (comment placeholder), which the picker detects and surfaces as an
    error rather than storing broken DDL.
    """
    return converter.convert_table(
        table,
        SchemaConvertOptions(
            primary_key_strategy=PrimaryKeyStrategy.COMPOSITE_KEY,
            composite_leading_column=leading,
        ),
    )


def composite_leading_from_ddl(table: TableDef, target_ddl: str) -> Optional[str]:
    """Infer the composite leading column from a stored target DDL, if composite.

    The picker keeps no separate state: it reads the object's stored (edited)
    target DDL and, when that DDL's parsed primary key is the composite
    ``(leading, original_pk...)``, returns the leading column so the picker renders
    as "Composite key" with the dropdown preset. Returns ``None`` when the DDL's
    key equals the source key (not composite) or cannot be parsed -- i.e. the
    single source of truth for the picker's state is the DDL itself.
    """
    # The stored DDL is the full script (CREATE TABLE + CREATE INDEX ...), but the
    # PK parser reads a single statement -- isolate the CREATE TABLE first.
    create_table = next(
        (
            stmt
            for stmt in split_sql_statements(target_ddl)
            if stmt.strip().upper().split("(", 1)[0].startswith("CREATE TABLE")
        ),
        target_ddl,
    )
    target_pk = parse_target_primary_key(create_table)
    source_pk = list(table.primary_key)
    if target_pk and target_pk != source_pk and target_pk[1:] == source_pk:
        return target_pk[0]
    return None


def _classify_edited_table_conversion(
    deterministic: TableConversion, edited_script: str
) -> TableConversion:
    """Build a TableConversion from a user-edited target-DDL script.

    Splits the edited script and buckets statements by leading keyword
    (CREATE SCHEMA / CREATE TABLE / CREATE INDEX) so the structured conversion
    mirrors what Schema Apply applied. Anything missing falls back to the
    deterministic conversion (e.g. an edit that only changed the CREATE TABLE
    keeps the deterministic schema/index DDLs).
    """
    schema_ddls: list[str] = []
    create_ddl: Optional[str] = None
    index_ddls: list[str] = []
    for statement in split_sql_statements(edited_script):
        stmt = statement.strip()
        if not stmt:
            continue
        head = stmt.upper().split("(", 1)[0]
        if head.startswith("CREATE SCHEMA"):
            schema_ddls.append(stmt)
        elif head.startswith("CREATE TABLE"):
            create_ddl = stmt
        elif head.startswith("CREATE") and "INDEX" in head:
            index_ddls.append(stmt)
    return TableConversion(
        table=deterministic.table,
        target_ddl=create_ddl or deterministic.target_ddl,
        schema_ddls=schema_ddls or list(deterministic.schema_ddls),
        index_ddls=index_ddls,
        preserved_foreign_keys=list(deterministic.preserved_foreign_keys),
        warnings=list(deterministic.warnings),
    )


def applied_table_conversions(
    result: SchemaConversionResult,
    edited_target_ddls: Mapping[str, str],
) -> dict[str, TableConversion]:
    """Per-table APPLIED conversion (honoring user edits), keyed by table name.

    For a table the user edited in Schema Conversion, the edited target DDL is
    parsed into a structured :class:`TableConversion`; otherwise the deterministic
    conversion is used unchanged. Full Load consumes this so it uses the SAME
    target schema the Schema Apply step applied -- driving value conversion off
    the applied column types and recreating a fresh-load target from the applied
    (not re-derived) DDL.
    """
    conversions: dict[str, TableConversion] = {}
    for table in result.tables:
        edited = edited_target_ddls.get(table.table)
        conversions[table.table] = (
            _classify_edited_table_conversion(table, edited)
            if edited is not None
            else table
        )
    return conversions


def applied_view_ddls(
    result: SchemaConversionResult,
    edited_target_ddls: Mapping[str, str],
) -> dict[str, str]:
    """Per-view APPLIED target CREATE-VIEW DDL (honoring user edits), by view name.

    Full Load's "drop & reload" path consumes this to pre-drop and recreate the
    views that depend on a replaced table (a dependent view otherwise blocks the
    table's DROP). Only **auto-converted** views are included -- a view that needs
    manual reimplementation has a comment-placeholder ``target_ddl`` (not runnable
    DDL), so recreating it would fail; those are skipped. A user-edited view uses
    its edited DDL, mirroring :func:`applied_table_conversions`.
    """
    ddls: dict[str, str] = {}
    for view in result.views:
        if not view.auto_converted:
            continue
        edited = edited_target_ddls.get(view.view)
        ddls[view.view] = edited if edited is not None else view.target_ddl
    return ddls


def build_ai_apply_objects(
    suggestions: Sequence[AiConversionSuggestion],
) -> list[ApplyObject]:
    """Turn only the *approved, safe* SCHEMA suggestions into apply units.

    This is the review gate (Property 13). It forwards a suggestion to the Schema
    Applier path only when:

    - the user explicitly approved it (``status == APPROVED`` and
      ``approved_by_user``) -- reusing :func:`approved_suggestions`; rejected,
      pending, and edited-but-not-approved suggestions are excluded,
    - it is a SCHEMA suggestion (DATA/QUERY are not applied to the target),
    - its untrusted text passes :func:`validate_suggested_sql` -- so a suggestion
      that was edited to contain a forbidden statement (e.g. DROP/DELETE/GRANT)
      and then approved is still refused and never applied (Requirement 11.8 /
      Property 13), and
    - its text contains at least one DDL statement.

    Each suggestion's text is split into single DDL statements so every statement
    is applied in its own transaction (Property 2), consistent with the
    deterministic apply path.
    """
    objects: list[ApplyObject] = []
    for suggestion in approved_suggestions(suggestions):
        if suggestion.kind != "SCHEMA":
            continue
        # Untrusted output: refuse forbidden statements even when approved.
        if not validate_suggested_sql(suggestion.suggested_sql_or_expr).is_safe:
            continue
        ddls = tuple(split_sql_statements(suggestion.suggested_sql_or_expr))
        if not ddls:
            continue
        objects.append(ApplyObject(object_name=suggestion.object_name, ddls=ddls))
    return objects


def _apply_success_detail(outcome: ApplyOutcome, mode: ApplyMode) -> str:
    """Explain a successful per-object apply outcome (Requirement 10.7).

    Makes CREATED vs SKIPPED unambiguous in the results view so the user can
    tell what actually happened to the target object:

    - ``SKIPPED`` -> the object already existed and was left untouched (never
      dropped or modified).
    - ``CREATED`` under :attr:`ApplyMode.SKIP_IF_EXISTS` -> the object did not
      exist and was created fresh; SKIP mode never drops or replaces anything.
    - ``CREATED`` under :attr:`ApplyMode.REPLACE` -> applied destructively; an
      existing object was dropped and recreated (or created new if absent).
    """
    if outcome is ApplyOutcome.SKIPPED:
        return "Already existed on the target; left unchanged (not modified)."
    if mode is ApplyMode.REPLACE:
        return (
            "Applied in REPLACE mode: any existing object was dropped and "
            "recreated (created new if it did not exist)."
        )
    return (
        "Did not exist on the target; created new. SKIP mode never drops or "
        "replaces an existing object."
    )


def _view_create_ddls(objects: Sequence[ApplyObject]) -> list[str]:
    """Return the ``CREATE VIEW`` DDL of every view in ``objects``, in order.

    A view apply unit is its ``CREATE SCHEMA``(s) followed by the ``CREATE VIEW``;
    only the latter identifies a relation that can block a table DROP, so the
    schema scaffolding is skipped. Statements that do not parse as a view are
    ignored, so a table/index (or a test placeholder) never enters the pre-drop.
    """
    view_ddls: list[str] = []
    for obj in objects:
        for ddl in obj.ddls:
            _, kind = _describe_ddl(ddl)
            if kind == _VIEW_KIND:
                view_ddls.append(ddl)
    return view_ddls


def _predrop_dependent_views(
    objects: Sequence[ApplyObject], applier: SchemaApplier
) -> None:
    """Drop the apply set's views before any table is recreated (REPLACE only).

    Clears the view->table dependency that makes a table ``DROP`` fail with
    "other objects depend on it" during a destructive REPLACE. Views are dropped
    in reverse create order (a view built on another view drops first), each via
    ``applier.drop`` (``DROP VIEW IF EXISTS`` -- idempotent). Each view is
    recreated by its own apply unit, so this only reorders the drop. A no-op when
    the applier has no ``drop`` seam (test doubles) or there are no views; a drop
    error is swallowed so the per-object apply still surfaces the real failure.
    """
    drop = getattr(applier, "drop", None)
    if not callable(drop):
        return
    for view_ddl in reversed(_view_create_ddls(objects)):
        try:
            drop(view_ddl)
        except Exception:  # noqa: BLE001 - best-effort; real failure surfaces per-object
            pass


def run_schema_apply(
    objects: Sequence[ApplyObject],
    *,
    applier: SchemaApplier,
    mode: ApplyMode,
    confirmed: bool,
    on_object_start: Optional[Callable[[str], None]] = None,
    on_object_result: Optional[Callable[[ObjectApplyResult], None]] = None,
) -> list[ObjectApplyResult]:
    """Apply ``objects`` to the target and return a per-object result list.

    Property 12 safety semantics enforced here (the parts the UI owns):

    - A destructive :attr:`ApplyMode.REPLACE` is **never** applied unless
      ``confirmed`` is ``True``. When it is not confirmed, every object is
      reported ``SKIPPED`` with an explanatory detail and the applier is never
      invoked (Requirement 10.6).
    - Each object is delegated to ``applier.apply_object`` exactly once; the
      applier applies each statement as a single DDL and idempotently retries
      OC001 conflicts (Requirements 10.4, 10.5).
    - A failure for one object is captured as a ``FAILED`` result (with the error
      message) and does not abort the remaining objects (Requirement 10.7).

    The orchestration takes only the target-side ``applier`` and no source
    handle, so it cannot write to the source (Requirement 10.8 / Property 1).

    Progress callbacks (optional). ``on_object_start`` is invoked with the object
    name just before it is applied and ``on_object_result`` with each finished
    :class:`ObjectApplyResult`, so a caller (the background apply job) can stream
    live progress to the UI instead of only the final list. Both run on the apply
    worker thread; they must be thread-safe relative to any UI reader.
    """
    require_confirmation = mode is ApplyMode.REPLACE and not confirmed
    results: list[ObjectApplyResult] = []
    # Confirmed REPLACE pre-pass: a view that an earlier apply created selects
    # from a table, so dropping that table (to recreate it) fails with "other
    # objects depend on it". Drop the apply set's views first -- in reverse of
    # their create order, so a view depending on another view goes first -- to
    # clear the dependency without a blunt DROP ... CASCADE. Each view is
    # recreated by its own apply unit below, so this only reorders the drop and
    # stays idempotent (DROP ... IF EXISTS). Skipped unless the applier exposes a
    # drop seam (test doubles need not), so SKIP mode and unconfirmed REPLACE are
    # untouched.
    if mode is ApplyMode.REPLACE and not require_confirmation:
        _predrop_dependent_views(objects, applier)
    for obj in objects:
        if on_object_start is not None:
            on_object_start(obj.object_name)
        if obj.skip_reason is not None:
            # No applicable CREATE DDL (e.g. a table the converter could not
            # auto-convert). Report it as SKIPPED with the redesign reason rather
            # than sending a non-CREATE statement to the applier.
            result = ObjectApplyResult(
                object_name=obj.object_name,
                status=ObjectApplyStatus.SKIPPED,
                detail=obj.skip_reason,
            )
            results.append(result)
            if on_object_result is not None:
                on_object_result(result)
            continue
        if require_confirmation:
            result = ObjectApplyResult(
                object_name=obj.object_name,
                status=ObjectApplyStatus.SKIPPED,
                detail=(
                    "REPLACE is destructive and was not confirmed; the object "
                    "was not applied. Confirm REPLACE to recreate it."
                ),
            )
        else:
            try:
                outcome = applier.apply_object(obj.object_name, obj.ddls, mode)
            except ObjectApplyError as exc:
                # Per-statement failure: surface exactly which statement(s) failed
                # (e.g. table created, one index failed) rather than only the first.
                result = ObjectApplyResult(
                    object_name=obj.object_name,
                    status=ObjectApplyStatus.FAILED,
                    detail=str(exc),
                )
            except Exception as exc:  # noqa: BLE001 - reported as a per-object failure
                result = ObjectApplyResult(
                    object_name=obj.object_name,
                    status=ObjectApplyStatus.FAILED,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            else:
                status = (
                    ObjectApplyStatus.CREATED
                    if outcome is ApplyOutcome.CREATED
                    else ObjectApplyStatus.SKIPPED
                )
                result = ObjectApplyResult(
                    object_name=obj.object_name,
                    status=status,
                    detail=_apply_success_detail(outcome, mode),
                )
        results.append(result)
        if on_object_result is not None:
            on_object_result(result)
    return results


def apply_progress_text(done: int, total: int) -> str:
    """Build the in-progress apply line shown next to the spinner.

    Renders ``Applying converted DDL to the target... (<done> of <total> objects)``
    when ``total`` is known (> 0) so the user sees how far the apply has
    progressed; otherwise it falls back to the indeterminate message.
    """
    if total > 0:
        noun = "object" if total == 1 else "objects"
        return (
            f"Applying converted DDL to the target... "
            f"({done} of {total} {noun})"
        )
    return "Applying converted DDL to the target..."


def replace_confirmation_message(existing_names: Sequence[str]) -> str:
    """Build the body shown in the action-time REPLACE confirmation dialog.

    Lists the existing target objects a REPLACE will DROP and recreate so the
    user makes an informed decision at the moment of applying (Requirement 10.6 /
    Property 12), instead of a sticky checkbox set far from the action. When no
    existing object is known in scope (e.g. target introspection is unavailable)
    the message still warns that any existing object would be dropped.
    """
    if existing_names:
        listed = ", ".join(sorted(existing_names))
        return (
            f"REPLACE will DROP and recreate these existing target objects: "
            f"{listed}. This is destructive and cannot be undone."
        )
    return (
        "REPLACE will DROP and recreate any target object that already exists. "
        "This is destructive and cannot be undone."
    )


def job_status_to_step_status(job_status: str) -> Optional[StepStatus]:
    """Map a :class:`JobManager` job status to the Schema Conversion step status.

    Returns ``DONE``/``FAILED`` for terminal job states and ``None`` while the
    job is still ``PENDING``/``RUNNING`` (the step stays ``IN_PROGRESS``).
    """
    if job_status == "DONE":
        return StepStatus.DONE
    if job_status == "FAILED":
        return StepStatus.FAILED
    return None


def schema_apply_is_complete(
    applicable_names: "Sequence[str]",
    results: "Optional[list[ObjectApplyResult]]",
) -> bool:
    """True when every applicable object has been applied to the target OK.

    "Applied OK" is CREATED **or** SKIPPED -- a table already present on the
    target is skipped, which is success (the schema is ready), not a no-op that
    blocks progress. Returns False if there are no applicable objects, if any
    applicable object has no result yet, or if any result FAILED. This is the
    condition that should mark Schema Conversion DONE (so "Next: Data Migration"
    unlocks) for the per-object inline apply path, matching the bulk apply, which
    becomes DONE on job completion regardless of created-vs-skipped counts.
    """
    if not applicable_names:
        return False
    by_name = {r.object_name: r.status for r in (results or [])}
    ok = {ObjectApplyStatus.CREATED, ObjectApplyStatus.SKIPPED}
    return all(by_name.get(name) in ok for name in applicable_names)


# ---------------------------------------------------------------------------
# Object tree assembly (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


def _node(node_id: str, label: str, children: Optional[list[dict]] = None) -> dict:
    """Build one ``ui.tree`` node ``{id, label, children}``."""
    node: dict = {"id": node_id, "label": label}
    if children is not None:
        node["children"] = children
    return node


def _object_label(name: str, *, exists_on_target: Optional[bool]) -> str:
    """Annotate an object label with its target existence/conflict, if known."""
    if exists_on_target is True:
        return f"{name}  —  exists on target"
    return name


def _split_schema(name: str, default_schema: str) -> tuple[str, str]:
    """Split a possibly-qualified ``schema.object`` name into (schema, object).

    Cluster-wide introspection qualifies names as ``database.object``; a plain
    (single-database) name has no dot and falls under ``default_schema``.
    """
    if "." in name:
        schema, _, obj = name.partition(".")
        return schema, obj
    return default_schema, name


def build_object_tree(
    inventory: SourceInventory,
    *,
    schema_label: str = "source",
    existing_objects: Sequence[str] = (),
) -> list[dict]:
    """Assemble the source object tree for ``ui.tree`` (Requirement 10.1).

    Objects are grouped by their database/schema (parsed from a qualified
    ``schema.object`` name, or ``schema_label`` when unqualified). Each schema
    node expands into Tables / Views / Triggers / Routines categories. Node ids
    keep the full (qualified) object name so a tick/selection maps back to the
    inventory; labels show the short object name. ``existing_objects`` lists
    object names already present on the target so tables/views can be annotated
    with their existence/conflict (Requirement 10.3).
    """
    existing = set(existing_objects)
    order: list[str] = []
    buckets: dict[str, dict[str, list[dict]]] = {}

    def bucket(schema: str) -> dict[str, list[dict]]:
        if schema not in buckets:
            buckets[schema] = {"tables": [], "views": [], "triggers": [], "routines": []}
            order.append(schema)
        return buckets[schema]

    for table in inventory.tables:
        schema, obj = _split_schema(table.name, schema_label)
        table_node = _node(
            f"{TABLE_PREFIX}{table.name}",
            _object_label(obj, exists_on_target=table.name in existing),
        )
        # Carry a primary-key flag + a "header": "table" hook so the source
        # browser's "header-table" Quasar slot can show a small PK indicator
        # beside each table leaf (Aurora DSQL requires a primary key). Only table
        # leaves get this; views/triggers/routines have no PK concept.
        table_node["has_pk"] = bool(table.primary_key)
        table_node["header"] = "table"
        bucket(schema)["tables"].append(table_node)
    for view in inventory.views:
        schema, obj = _split_schema(view.name, schema_label)
        bucket(schema)["views"].append(
            _node(
                f"{VIEW_PREFIX}{view.name}",
                _object_label(obj, exists_on_target=view.name in existing),
            )
        )
    for trigger in inventory.triggers:
        schema, obj = _split_schema(trigger.name, schema_label)
        bucket(schema)["triggers"].append(_node(f"{TRIGGER_PREFIX}{trigger.name}", obj))
    for routine in inventory.routines:
        schema, obj = _split_schema(routine.name, schema_label)
        bucket(schema)["routines"].append(_node(f"{ROUTINE_PREFIX}{routine.name}", obj))

    schema_nodes: list[dict] = []
    for schema in order:
        b = buckets[schema]
        categories = [
            _node(f"category:tables:{schema}", f"Tables ({len(b['tables'])})", b["tables"]),
            _node(f"category:views:{schema}", f"Views ({len(b['views'])})", b["views"]),
        ]
        if b["triggers"]:
            categories.append(
                _node(
                    f"category:triggers:{schema}",
                    f"Triggers ({len(b['triggers'])})",
                    b["triggers"],
                )
            )
        if b["routines"]:
            categories.append(
                _node(
                    f"category:routines:{schema}",
                    f"Routines ({len(b['routines'])})",
                    b["routines"],
                )
            )
        schema_nodes.append(_node(f"schema:{schema}", f"Schema: {schema}", categories))
    return schema_nodes


def generate_previews(
    node_ids: Sequence[str],
    inventory: SourceInventory,
    result: SchemaConversionResult,
    *,
    existence_checker: Optional[TargetExistenceChecker] = None,
) -> list["DdlPreview"]:
    """Build DDL previews for the ticked table/view node ids (Requirement 10.2).

    Only object leaves (table/view nodes) are considered; schema/category ticks
    and trigger/routine ticks are ignored (the latter are not auto-converted).
    Each selected object is paired with its converted target DDL. Order follows
    ``node_ids``; unknown or non-previewable nodes are skipped.
    """
    previews: list[DdlPreview] = []
    for node_id in node_ids:
        if node_id.startswith(TABLE_PREFIX) or node_id.startswith(VIEW_PREFIX):
            preview = preview_for_selection(
                node_id, inventory, result, existence_checker=existence_checker
            )
            if preview is not None:
                previews.append(preview)
    return previews


# Object-leaf node id prefixes (a tick on one maps back to a source object).
_OBJECT_NODE_PREFIXES = (TABLE_PREFIX, VIEW_PREFIX, TRIGGER_PREFIX, ROUTINE_PREFIX)


def selected_object_names(node_ids: Sequence[str]) -> set[str]:
    """Return the source object names referenced by the ticked ``node_ids``.

    Strips the leaf-node prefixes (table/view/trigger/routine) so the result is
    the set of (possibly qualified) object names the user selected; schema and
    category ticks are ignored. Used to scope AI assistance to exactly the
    objects the user generated DDL for, instead of every flagged object.
    """
    names: set[str] = set()
    for node_id in node_ids:
        for prefix in _OBJECT_NODE_PREFIXES:
            if node_id.startswith(prefix):
                names.add(node_id[len(prefix):])
                break
    return names


# Tree node id prefix for target-side (browse-only) objects.
TARGET_PREFIX = "tgt:"


def build_target_object_tree(target: Optional[TargetInventory]) -> list[dict]:
    """Assemble the target DSQL object tree for ``ui.tree`` (browse-only).

    Mirrors the target catalog discovered in Step 1 (Evaluation): each schema
    groups its Tables and Views. Returns an empty list when no target inventory
    is available (target not introspected), so the screen can show a hint.
    """
    if target is None:
        return []
    schema_nodes: list[dict] = []
    for schema in target.schemas:
        table_nodes = [
            _node(f"{TARGET_PREFIX}table:{schema.name}.{table.name}", table.name)
            for table in schema.tables
        ]
        view_nodes = [
            _node(f"{TARGET_PREFIX}view:{schema.name}.{view.name}", view.name)
            for view in schema.views
        ]
        categories = [
            _node(
                f"{TARGET_PREFIX}cat:tables:{schema.name}",
                f"Tables ({len(table_nodes)})",
                table_nodes,
            ),
            _node(
                f"{TARGET_PREFIX}cat:views:{schema.name}",
                f"Views ({len(view_nodes)})",
                view_nodes,
            ),
        ]
        schema_nodes.append(
            _node(f"{TARGET_PREFIX}schema:{schema.name}", f"Schema: {schema.name}", categories)
        )
    return schema_nodes


# ---------------------------------------------------------------------------
# DDL preview pairing (source DDL vs converted target DDL)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DdlPreview:
    """A side-by-side source/target DDL pair for one object (Requirement 10.2).

    ``exists_on_target`` is ``None`` when target introspection is unavailable, so
    the screen can distinguish "known to exist" from "existence unknown".
    """

    object_name: str
    source_ddl: str
    target_ddl: str
    warnings: tuple[ConversionWarning, ...] = ()
    exists_on_target: Optional[bool] = None


def _quote_mysql(name: str) -> str:
    """Backtick-quote a MySQL identifier for display (doubling any backtick)."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def render_source_table_ddl(table: TableDef) -> str:
    """Reconstruct a readable MySQL ``CREATE TABLE`` for ``table`` (display only).

    This mirrors a ``SHOW CREATE TABLE`` view (columns, primary key, secondary
    indexes, and foreign keys) so the source side of the diff shows exactly what
    DSQL conversion changes (e.g. removed foreign keys, async indexes). It is
    rendered text only and is never executed.
    """
    clauses: list[str] = []
    for column in table.columns:
        clause = f"  {_quote_mysql(column.name)} {column.mysql_type}"
        if not column.nullable:
            clause += " NOT NULL"
        if column.default is not None:
            clause += f" DEFAULT {column.default}"
        clauses.append(clause)

    if table.primary_key:
        pk_columns = ", ".join(_quote_mysql(name) for name in table.primary_key)
        clauses.append(f"  PRIMARY KEY ({pk_columns})")

    for index in table.indexes:
        unique = "UNIQUE " if index.unique else ""
        columns = ", ".join(_quote_mysql(name) for name in index.columns)
        clauses.append(f"  {unique}KEY {_quote_mysql(index.name)} ({columns})")

    for foreign_key in table.foreign_keys:
        columns = ", ".join(_quote_mysql(name) for name in foreign_key.columns)
        ref_columns = ", ".join(
            _quote_mysql(name) for name in foreign_key.referenced_columns
        )
        clauses.append(
            f"  CONSTRAINT {_quote_mysql(foreign_key.name)} FOREIGN KEY ({columns}) "
            f"REFERENCES {_quote_mysql(foreign_key.referenced_table)} ({ref_columns})"
        )

    body = ",\n".join(clauses)
    return f"CREATE TABLE {_quote_mysql(table.name)} (\n{body}\n)"


def render_target_ddl(conversion: TableConversion) -> str:
    """Join a table conversion's statements into one displayable target script.

    Any ``CREATE SCHEMA IF NOT EXISTS`` is shown first, then the ``CREATE
    TABLE``, then each ``CREATE INDEX ASYNC``, each terminated with ``;`` and
    separated by a blank line so the single-DDL units are visually distinct.
    """
    statements = [
        *conversion.schema_ddls,
        conversion.target_ddl,
        *conversion.index_ddls,
    ]
    return "\n\n".join(f"{statement.rstrip().rstrip(';')};" for statement in statements)


def build_table_preview(
    table: TableDef,
    conversion: TableConversion,
    *,
    exists_on_target: Optional[bool] = None,
) -> DdlPreview:
    """Build the source/target DDL preview for a converted table (Req 10.2)."""
    return DdlPreview(
        object_name=table.name,
        source_ddl=render_source_table_ddl(table),
        target_ddl=render_target_ddl(conversion),
        warnings=tuple(conversion.warnings),
        exists_on_target=exists_on_target,
    )


def render_source_view_ddl(view: ViewDef) -> str:
    """Render a readable MySQL ``CREATE VIEW`` for the source side of the diff."""
    body = (view.definition or "").strip()
    if not body:
        return f"-- View definition unavailable for {view.name}."
    if body.upper().startswith("CREATE"):
        return body
    return f"CREATE VIEW {view.name} AS\n{body}"


def build_view_preview(
    view: ViewDef,
    conversion: Optional["ViewConversion"] = None,
    *,
    exists_on_target: Optional[bool] = None,
) -> DdlPreview:
    """Build the source/target DDL preview for a view (Req 10.2).

    When the view was auto-converted (``conversion`` present), the target side is
    its converted PostgreSQL ``CREATE VIEW`` (editable + applyable like a table).
    Without a conversion (or one that could not be auto-converted) the target is
    the not-auto-converted note for manual reimplementation.
    """
    if conversion is not None and conversion.auto_converted:
        target_ddl = conversion.target_ddl
    else:
        target_ddl = _NOT_AUTO_CONVERTED
    return DdlPreview(
        object_name=view.name,
        source_ddl=render_source_view_ddl(view),
        target_ddl=target_ddl,
        warnings=tuple(conversion.warnings) if conversion is not None else (),
        exists_on_target=exists_on_target,
    )


class DiffKind(str, Enum):
    """How one aligned source/target line pair differs in a DDL diff."""

    EQUAL = "equal"
    REPLACE = "replace"
    DELETE = "delete"  # present in source only (removed by conversion)
    INSERT = "insert"  # present in target only (added by conversion)


@dataclass(frozen=True)
class DiffRow:
    """One aligned row of a side-by-side DDL diff.

    ``left``/``right`` are the source/target line for the row, or ``None`` when
    that side has no line (an inserted target line has no source; a deleted
    source line has no target). ``kind`` classifies the row for highlighting.
    """

    left: Optional[str]
    right: Optional[str]
    kind: DiffKind


def diff_ddl_lines(source_ddl: str, target_ddl: str) -> list[DiffRow]:
    """Align source and target DDL into highlighted side-by-side rows (Req 10.2).

    Uses a line-level :class:`difflib.SequenceMatcher` so the user can see what
    the conversion changed: a line only in the source (e.g. a removed FOREIGN
    KEY) is ``DELETE``, a line only in the target (e.g. a ``CREATE INDEX ASYNC``)
    is ``INSERT``, a changed line (e.g. a remapped column type) is ``REPLACE``,
    and an unchanged line is ``EQUAL``. The two dialects differ in identifier
    quoting, so some lines read as changed; the alignment keeps the comparison
    scannable instead of two unaligned blocks.
    """
    left_lines = source_ddl.splitlines()
    right_lines = target_ddl.splitlines()
    matcher = difflib.SequenceMatcher(a=left_lines, b=right_lines, autojunk=False)
    rows: list[DiffRow] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for offset in range(i2 - i1):
                rows.append(
                    DiffRow(
                        left_lines[i1 + offset], right_lines[j1 + offset], DiffKind.EQUAL
                    )
                )
        elif tag == "replace":
            left_block = left_lines[i1:i2]
            right_block = right_lines[j1:j2]
            for offset in range(max(len(left_block), len(right_block))):
                left = left_block[offset] if offset < len(left_block) else None
                right = right_block[offset] if offset < len(right_block) else None
                rows.append(DiffRow(left, right, DiffKind.REPLACE))
        elif tag == "delete":
            for offset in range(i1, i2):
                rows.append(DiffRow(left_lines[offset], None, DiffKind.DELETE))
        elif tag == "insert":
            for offset in range(j1, j2):
                rows.append(DiffRow(None, right_lines[offset], DiffKind.INSERT))
    return rows


def preview_for_selection(
    node_id: Optional[str],
    inventory: SourceInventory,
    result: SchemaConversionResult,
    *,
    existence_checker: Optional[TargetExistenceChecker] = None,
) -> Optional[DdlPreview]:
    """Return the DDL preview for the selected tree ``node_id``, if any.

    Returns ``None`` for a category/schema node or an unknown selection. Tables
    are paired with their converted DDL; views show their definition with a
    not-auto-converted target note. Triggers/routines have no DDL preview (they
    are flagged for manual reimplementation in the conversion warnings).
    """
    if not node_id:
        return None

    if node_id.startswith(TABLE_PREFIX):
        name = node_id[len(TABLE_PREFIX):]
        table = _find_table(inventory, name)
        conversion = _find_conversion(result, name)
        if table is None or conversion is None:
            return None
        exists = _check_exists(existence_checker, name)
        return build_table_preview(table, conversion, exists_on_target=exists)

    if node_id.startswith(VIEW_PREFIX):
        name = node_id[len(VIEW_PREFIX):]
        view = next((v for v in inventory.views if v.name == name), None)
        if view is None:
            return None
        conversion = _find_view_conversion(result, name)
        exists = _check_exists(existence_checker, name)
        return build_view_preview(view, conversion, exists_on_target=exists)

    return None


def _check_exists(
    existence_checker: Optional[TargetExistenceChecker], name: str
) -> Optional[bool]:
    """Return target existence for ``name`` or ``None`` when unavailable."""
    if existence_checker is None:
        return None
    return existence_checker.object_exists(name)


def _find_table(inventory: SourceInventory, name: str) -> Optional[TableDef]:
    """Return the inventory table named ``name``, if present."""
    return next((table for table in inventory.tables if table.name == name), None)


def _find_conversion(
    result: SchemaConversionResult, name: str
) -> Optional[TableConversion]:
    """Return the table conversion named ``name``, if present."""
    return next((tc for tc in result.tables if tc.table == name), None)


def _find_view_conversion(
    result: SchemaConversionResult, name: str
) -> Optional["ViewConversion"]:
    """Return the view conversion named ``name``, if present."""
    return next((vc for vc in result.views if vc.view == name), None)


# ---------------------------------------------------------------------------
# SQL statement splitting
# ---------------------------------------------------------------------------


def split_sql_statements(text: str) -> list[str]:
    """Split ``text`` into individual SQL statements on ``;`` (empties dropped).

    A single edited target-DDL script is split here so each statement is applied
    as one DDL in its own transaction (Property 2) by the apply path.
    """
    return [statement.strip() for statement in text.split(";") if statement.strip()]


# ---------------------------------------------------------------------------
# Per-session schema-conversion state
# ---------------------------------------------------------------------------


class SchemaConversionState:
    """Per-session inputs/outputs for the Schema Conversion screen.

    UI-thread fields (selection, apply mode, confirmation) are read
    and written only on the UI thread. ``apply_results``/``error`` are produced
    by a background apply worker and read by the UI poller, so they are guarded
    by a lock for the cross-thread handoff (mirroring the Evaluation screen).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.selected_node_id: Optional[str] = None
        # Node ids currently ticked (checkboxes) in the source browser, and the
        # node ids committed for DDL generation by the "Generate DDL" button.
        # Generation is gated: previews render only for ``generated_node_ids``.
        self.ticked_node_ids: list[str] = []
        self.generated_node_ids: Optional[list[str]] = None
        # Whether the generated-object expansions render expanded (Expand all /
        # Collapse all toggle). UI-only; defaults to collapsed so a long list is
        # scannable via each header's status summary before opening one.
        self.expand_all: bool = False
        # Per-object expansion open-state (qualified object name) so a re-render
        # after an inline apply preserves which Generated-DDL panels are open
        # instead of collapsing them all.
        self.expanded_objects: set[str] = set()
        self.apply_mode: ApplyMode = ApplyMode.SKIP_IF_EXISTS
        self.replace_confirmed: bool = False
        self.job_id: Optional[str] = None
        # The current screen's refresh callback, recorded by ``content`` on each
        # render so deferred handlers (the REPLACE confirm dialog, the bulk
        # runner shared with the sidebar) can re-render after they act, even
        # though they are created outside the active render. UI-thread only.
        self.refresh_view: Optional[Callable[[], None]] = None
        # Set when a bulk apply finishes having created at least one object, so
        # the screen auto-refreshes the target browser once to show the new
        # objects. One-shot: consumed (cleared) by ``content`` after scheduling
        # the refresh. UI-thread only.
        self.pending_target_refresh: bool = False
        # Per-object, user-edited target DDL keyed by object name. When present,
        # the edited script overrides the deterministic target DDL on apply, so
        # the user can apply the generated DDL as-is or after editing it.
        self.edited_target_ddls: dict[str, str] = {}
        # Per-object AI suggestions keyed by object name, edited/approved on the
        # UI thread. Only APPROVED suggestions are forwarded to apply (Property
        # 13); see build_ai_apply_objects.
        self.ai_suggestions: dict[str, AiConversionSuggestion] = {}
        self._apply_results: Optional[list[ObjectApplyResult]] = None
        self._error: Optional[str] = None
        # Live apply progress, written by the background apply worker and read by
        # the UI poller (so guarded by the same lock as the results handoff):
        # ``_apply_total`` is the object count of the running apply, ``_apply_done``
        # how many have finished, and ``_apply_current`` the object being applied.
        self._apply_total: int = 0
        self._apply_done: int = 0
        self._apply_current: Optional[str] = None

    def set_suggestion(self, suggestion: AiConversionSuggestion) -> None:
        """Store/replace the AI suggestion for ``suggestion.object_name``."""
        self.ai_suggestions[suggestion.object_name] = suggestion

    def set_edited_target_ddl(self, object_name: str, ddl: str) -> None:
        """Record the user-edited target DDL for ``object_name``.

        A blank/whitespace-only edit clears the override (treated as "apply the
        generated DDL as-is") so the user can revert by emptying the field.
        """
        if ddl.strip():
            self.edited_target_ddls[object_name] = ddl
        else:
            self.edited_target_ddls.pop(object_name, None)

    def get_edited_target_ddl(self, object_name: str) -> Optional[str]:
        """Return the user-edited target DDL for ``object_name``, if any."""
        return self.edited_target_ddls.get(object_name)

    def clear_edited_target_ddl(self, object_name: str) -> None:
        """Discard the edit for ``object_name`` (revert to generated DDL)."""
        self.edited_target_ddls.pop(object_name, None)

    def get_suggestion(self, object_name: str) -> Optional[AiConversionSuggestion]:
        """Return the stored AI suggestion for ``object_name``, if any."""
        return self.ai_suggestions.get(object_name)

    def all_suggestions(self) -> list[AiConversionSuggestion]:
        """Return all stored AI suggestions for this session."""
        return list(self.ai_suggestions.values())

    def clear_suggestions(self) -> None:
        """Discard all stored AI suggestions for this session."""
        self.ai_suggestions.clear()

    def set_apply_results(self, results: list[ObjectApplyResult]) -> None:
        """Record a finished apply run's per-object results (clears any error)."""
        with self._lock:
            self._apply_results = list(results)
            self._error = None
            self._apply_current = None

    def start_apply(self, total: int) -> None:
        """Reset live progress for a new apply run of ``total`` objects.

        Called by the background apply worker before applying so the UI poller
        can show ``done of total`` progress as objects finish.
        """
        with self._lock:
            self._apply_total = total
            self._apply_done = 0
            self._apply_current = None

    def begin_apply_object(self, object_name: str) -> None:
        """Mark ``object_name`` as the object currently being applied."""
        with self._lock:
            self._apply_current = object_name

    def record_apply_progress(self, result: ObjectApplyResult) -> None:
        """Upsert one finished object's result and advance the progress counter.

        Lets the apply results table fill in live (one row per finished object)
        while the run is still in progress, instead of appearing only at the end.
        """
        with self._lock:
            by_name: dict[str, ObjectApplyResult] = {
                item.object_name: item for item in (self._apply_results or [])
            }
            by_name[result.object_name] = result
            self._apply_results = list(by_name.values())
            self._apply_done += 1
            self._apply_current = None
            self._error = None

    @property
    def apply_total(self) -> int:
        """Total object count of the current/last apply run (0 if none)."""
        with self._lock:
            return self._apply_total

    @property
    def apply_done(self) -> int:
        """Number of objects finished in the current/last apply run."""
        with self._lock:
            return self._apply_done

    @property
    def apply_current(self) -> Optional[str]:
        """Name of the object currently being applied, if any."""
        with self._lock:
            return self._apply_current

    def set_error(self, message: str) -> None:
        """Record a failure message for display."""
        with self._lock:
            self._error = message

    @property
    def apply_results(self) -> Optional[list[ObjectApplyResult]]:
        """Return the last apply run's results, if any."""
        with self._lock:
            return None if self._apply_results is None else list(self._apply_results)

    def get_apply_result(self, object_name: str) -> Optional[ObjectApplyResult]:
        """Return the apply result for a specific object, or ``None``."""
        with self._lock:
            if self._apply_results is None:
                return None
            for item in self._apply_results:
                if item.object_name == object_name:
                    return item
            return None

    def merge_apply_results(self, results: list[ObjectApplyResult]) -> None:
        """Upsert per-object apply results without discarding the others.

        Used by the inline per-object Apply so applying one object updates only
        that object's result and keeps any results from earlier applies.
        """
        with self._lock:
            by_name: dict[str, ObjectApplyResult] = {
                item.object_name: item for item in (self._apply_results or [])
            }
            for item in results:
                by_name[item.object_name] = item
            self._apply_results = list(by_name.values())
            self._error = None

    @property
    def error(self) -> Optional[str]:
        """Return the last failure message, if any."""
        with self._lock:
            return self._error

    def clear_outputs(self) -> None:
        """Discard the previous apply results/error before a (re-)run."""
        with self._lock:
            self._apply_results = None
            self._error = None
            self._apply_total = 0
            self._apply_done = 0
            self._apply_current = None

    def reset_generation(self) -> None:
        """Discard a prior generation/apply so the next run starts fresh.

        Clears the committed generation scope, per-object edits and AI
        suggestions, the apply results/progress, and the REPLACE confirmation --
        returning the screen to a ready-to-generate state (used by the Clear
        button), so "Clear" removes all prior analysis rather than only hiding
        the generated DDL list.
        """
        self.generated_node_ids = None
        self.edited_target_ddls.clear()
        self.ai_suggestions.clear()
        self.replace_confirmed = False
        self.job_id = None
        self.clear_outputs()


@dataclass
class SchemaConversionStore:
    """Process-memory map of session id to :class:`SchemaConversionState`.

    Mirrors :class:`~dsql_migrator.ui.evaluation.EvaluationStore`: each UI session
    sees only its own state and nothing is persisted to disk.
    """

    _states: dict[str, SchemaConversionState] = field(default_factory=dict)

    def get_or_create(self, session_id: str) -> SchemaConversionState:
        """Return the state for ``session_id``, creating an empty one if needed."""
        state = self._states.get(session_id)
        if state is None:
            state = SchemaConversionState()
            self._states[session_id] = state
        return state

    def get(self, session_id: str) -> Optional[SchemaConversionState]:
        """Return the state for ``session_id``, or ``None`` if absent."""
        return self._states.get(session_id)

    def clear(self, session_id: Optional[str]) -> None:
        """Remove the state for ``session_id`` (no-op if absent)."""
        if session_id is None:
            return
        self._states.pop(session_id, None)

    def reset_in_place(self, session_id: Optional[str]) -> None:
        """Reset the state WITHOUT replacing the object (no-op if absent).

        The workflow screen captures this state object in its builder closures at
        build time, so popping + recreating would orphan the captured reference.
        Re-initialising the SAME instance keeps every closure on the live object.
        """
        if session_id is None:
            return
        state = self._states.get(session_id)
        if state is not None:
            state.__init__()  # type: ignore[misc]  # re-run init on the same object


# ---------------------------------------------------------------------------
# NiceGUI screen
# ---------------------------------------------------------------------------

# Quasar color names reused for the inline status badge.
_STATUS_COLORS: dict[StepStatus, str] = {
    StepStatus.NOT_STARTED: "grey",
    StepStatus.IN_PROGRESS: "primary",
    StepStatus.DONE: "positive",
    StepStatus.FAILED: "negative",
}

# Quasar color names for each per-object apply status badge.
_APPLY_STATUS_COLORS: dict[ObjectApplyStatus, str] = {
    ObjectApplyStatus.CREATED: "positive",
    ObjectApplyStatus.SKIPPED: "grey",
    ObjectApplyStatus.FAILED: "negative",
}

# How often the screen polls the background apply job (seconds).
_POLL_INTERVAL_SECONDS = 0.5


# A factory that builds a core per-statement applier for a target + AWS profile.
# Injectable so tests drive the adapter with a fake core applier (no AWS).
CoreApplierFactory = Callable[[TargetConnectionConfig, Optional[str]], object]


def _build_core_applier(
    target: TargetConnectionConfig, aws_profile: Optional[str]
) -> object:
    """Build the real core :class:`SchemaApplier` for ``target`` (Task 15).

    Browses the target catalog once for the pre-apply existence check and opens
    DSQL connections through a :class:`DsqlConnector` bound to the optional
    global AWS profile, so the apply path shares the single credential context
    used by every other AWS client (Requirements 9.5, 9.7). ``boto3``/``psycopg``
    stay lazily imported. This performs a live target browse and so must be
    invoked inside the background apply job, never on the UI thread.
    """
    from dsql_migrator.core.schema_applier import SchemaApplier as _CoreApplier
    from dsql_migrator.core.target_connection import DsqlConnector
    from dsql_migrator.core.target_introspector import TargetIntrospector

    def connector_factory(conn: TargetConnectionConfig) -> object:
        return DsqlConnector(conn, aws_profile=aws_profile)

    introspector = TargetIntrospector(connector_factory=connector_factory)
    introspector.browse(target)
    return _CoreApplier(
        introspector,
        connection_factory=DsqlConnector(target, aws_profile=aws_profile).connect,
    )


class DsqlSchemaApplier:
    """UI :class:`SchemaApplier` backed by the core per-statement applier.

    Adapts :class:`dsql_migrator.core.schema_applier.SchemaApplier` (which
    applies one DDL statement at a time with its own existence check, OCC retry,
    and single-DDL transactions -- Property 12) to the UI's per-object
    :class:`SchemaApplier` protocol. The core applier (and its live target
    browse) is built lazily on first apply, so constructing this performs no
    network call and the live work happens inside the background apply job.
    """

    def __init__(
        self,
        target: TargetConnectionConfig,
        *,
        aws_profile: Optional[str] = None,
        core_factory: Optional[CoreApplierFactory] = None,
    ) -> None:
        self._target = target
        self._aws_profile = aws_profile
        self._core_factory = core_factory or _build_core_applier
        self._core: Optional[object] = None

    def apply_object(
        self, object_name: str, ddls: Sequence[str], on_conflict: ApplyMode
    ) -> ApplyOutcome:
        """Apply each DDL of ``object_name`` via the core applier.

        Each statement is applied individually (single-DDL transactions) and its
        result is collected, so a failure reports exactly which statement(s)
        failed (e.g. the table was created but one index failed) instead of only
        the first failing statement. A failed structural statement
        (``CREATE SCHEMA``/``CREATE TABLE``) aborts the dependent index
        statements -- they would only cascade-fail -- while an index failure does
        not stop the remaining indexes. If any statement failed an
        :class:`ObjectApplyError` carrying the per-statement results is raised;
        otherwise the object is reported CREATED when any statement
        created/replaced an object, else SKIPPED. Confirmation is enforced
        upstream (``run_schema_apply`` never reaches here for an unconfirmed
        REPLACE), so the core applier is called with ``confirmed=True``.
        """
        from dsql_migrator.core.models import ApplyMode as _CoreMode, ApplyStatus

        if self._core is None:
            self._core = self._core_factory(self._target, self._aws_profile)
        core = self._core
        core_mode = _CoreMode(on_conflict.value)

        statements: list[StatementApplyResult] = []
        outcome = ApplyOutcome.SKIPPED
        prerequisite_failed = False
        for ddl in ddls:
            label, kind = _describe_ddl(ddl)
            if prerequisite_failed:
                statements.append(
                    StatementApplyResult(
                        label,
                        ObjectApplyStatus.FAILED,
                        "Not applied: a prerequisite statement failed.",
                    )
                )
                continue
            result = core.apply(ddl, core_mode, confirmed=True)  # type: ignore[attr-defined]
            statements.append(
                StatementApplyResult(
                    label,
                    ObjectApplyStatus(result.status.value),
                    _clean_detail(result.detail),
                )
            )
            if result.status is ApplyStatus.FAILED:
                if kind != _INDEX_KIND:
                    prerequisite_failed = True
                continue
            # A CREATE SCHEMA IF NOT EXISTS is always reported CREATED ("schema
            # ensured") even when the schema already existed, so it must not make
            # the object look CREATED. The verdict reflects the real object
            # (table/view) and its indexes only.
            if result.status is ApplyStatus.CREATED and kind != _SCHEMA_KIND:
                outcome = ApplyOutcome.CREATED

        if any(s.status is ObjectApplyStatus.FAILED for s in statements):
            raise ObjectApplyError(object_name, statements)
        return outcome

    def drop(self, target_ddl: str) -> None:
        """Drop the object ``target_ddl`` defines via the core applier.

        Used by the confirmed-REPLACE pre-pass to drop a dependent view before
        the table it selects from is recreated (so the table DROP is not blocked
        by the view). Delegates to the core applier's ``drop`` (``DROP <kind> IF
        EXISTS`` with OC001 retry); the core applier is built lazily on first
        use, exactly as in :meth:`apply_object`.
        """
        if self._core is None:
            self._core = self._core_factory(self._target, self._aws_profile)
        self._core.drop(target_ddl)  # type: ignore[attr-defined]


def default_applier_factory(aws_profile: Optional[str]) -> ApplierFactory:
    """Return an :data:`ApplierFactory` that applies converted DDL to DSQL.

    The returned factory builds a :class:`DsqlSchemaApplier` for a target
    connection, threading the optional global AWS profile into the DSQL
    connector. Used as the UI default so Schema Conversion can apply to the
    target out of the box; tests inject their own applier factory instead.
    """
    def factory(target: TargetConnectionConfig) -> SchemaApplier:
        return DsqlSchemaApplier(target, aws_profile=aws_profile)

    return factory


# Builds the AI conversion assistant for a config + optional global AWS profile.
# Injectable so tests pass a fake and never reach AWS.
AiAssistantFactory = Callable[
    [AiAssistConfig, Optional[str]], AiConversionAssistant
]


def _default_ai_assistant_factory(
    config: AiAssistConfig, aws_profile: Optional[str]
) -> AiConversionAssistant:
    """Build the real Bedrock-backed AI conversion assistant (Req 11.5/11.6).

    Mirrors the Evaluation strategist and the Connect "Verify AI access"
    factories: the optional global AWS profile is threaded into
    :func:`~dsql_migrator.core.ai_assistant.build_bedrock_runtime_client` so the
    assistant shares the single credential context used by every other AWS
    client (Requirements 9.5, 9.7). ``boto3`` stays lazily imported inside that
    builder and constructing the client performs no network call.
    """
    from dsql_migrator.core.ai_assistant import (
        AiConversionAssistant as _BedrockAssistant,
        build_bedrock_runtime_client,
    )

    client = build_bedrock_runtime_client(config, aws_profile=aws_profile)
    return _BedrockAssistant(config, client=client)


def build_schema_conversion_screen(
    store: SessionStore,
    session_id: str,
    *,
    job_manager: JobManager,
    eval_store: EvaluationStore,
    conv_store: SchemaConversionStore,
    converter: Optional[SchemaConverter] = None,
    applier_factory: Optional[ApplierFactory] = None,
    existence_checker: Optional[TargetExistenceChecker] = None,
    assistant: Optional[AiConversionAssistant] = None,
    assistant_factory: Optional[AiAssistantFactory] = None,
    on_continue_to_data_migration: Optional[Callable[[], None]] = None,
    cdc_active_check: Optional[Callable[[], bool]] = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Schema Conversion screen, returning ``(content_builder, runner)``.

    ``content_builder`` renders the SCT-like screen (object tree, DDL diff,
    SKIP/REPLACE choice, and per-object apply results) and is
    given the workflow shell's refresh callback. ``runner`` is invoked by the
    step's Run/Re-run button to apply the converted DDL to the target.

    The source inventory is taken from the Step 1 (Evaluation) result so the
    source is not re-introspected (Property 1). ``applier_factory`` builds the
    target :class:`SchemaApplier`; when it (or the target connection) is
    unavailable the runner surfaces a clear status instead of breaking
    (Task 15 not yet wired). AI suggestions use ``assistant`` when injected,
    otherwise the assistant is built per session from ``assistant_factory``
    (defaulting to the real Bedrock-backed one) using the session's AI-assist
    config and global AWS profile, but only when AI assist is enabled. Both
    returns plug into :func:`~dsql_migrator.ui.workflow.build_workflow_sidebar`.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    conv_state = conv_store.get_or_create(session_id)
    eval_state = eval_store.get_or_create(session_id)
    schema_converter = converter or SchemaConverter()

    def _inventory() -> Optional[SourceInventory]:
        result = eval_state.result
        return result.inventory if result is not None else None

    def _conversion(inventory: SourceInventory) -> SchemaConversionResult:
        return schema_converter.convert(inventory, SchemaConvertOptions())

    def _resolve_assistant() -> Optional[AiConversionAssistant]:
        """Return the AI assistant to use, building it per session when enabled.

        An explicitly injected ``assistant`` always wins (tests). Otherwise the
        assistant is built from the session's AI-assist config and global AWS
        profile via ``assistant_factory`` (defaulting to the real Bedrock-backed
        one), but only when AI assist is enabled. Construction is guarded so a
        misconfigured client degrades to "not wired" guidance instead of
        breaking the screen (graceful degradation, Requirement 11.10).
        """
        if assistant is not None:
            return assistant
        if not session.ai_assist.enabled:
            return None
        factory = assistant_factory or _default_ai_assistant_factory
        try:
            return factory(session.ai_assist, session.aws_profile)
        except Exception:  # noqa: BLE001 - degrade gracefully on client build failure
            return None

    def _prepare_apply() -> Optional[tuple[SchemaApplier, ApplyMode, bool]]:
        """Validate apply preconditions and return (applier, mode, confirmed).

        Returns ``None`` and records a clear, actionable error when the source
        inventory, target connection / applier, or a required REPLACE
        confirmation is missing (the same guards the bulk Run uses), so both the
        step's Run button and the per-object Apply button behave consistently.
        """
        inventory = _inventory()
        if inventory is None:
            conv_state.set_error(
                "Run Step 1 (Evaluation) first to introspect the source schema, "
                "then apply the conversion."
            )
            return None
        if not session.has_target():
            conv_state.set_error(
                "Target apply is unavailable: configure and test the target "
                "connection on the Connect screen, then apply the conversion."
            )
            return None
        if conv_state.apply_mode is ApplyMode.REPLACE and not conv_state.replace_confirmed:
            conv_state.set_error(
                "REPLACE is destructive. Confirm REPLACE before applying so "
                "existing target objects are recreated intentionally."
            )
            return None
        target_config = session.target_config
        assert target_config is not None  # guaranteed by has_target()
        # Use the injected applier factory (tests) or the real DSQL-backed one
        # built for this session's global AWS profile.
        factory = applier_factory or default_applier_factory(session.aws_profile)
        return (
            factory(target_config),
            conv_state.apply_mode,
            conv_state.replace_confirmed,
        )

    def _submit_apply(
        objects: list[ApplyObject],
        applier: SchemaApplier,
        mode: ApplyMode,
        confirmed: bool,
        *,
        merge: bool = False,
    ) -> None:
        """Apply ``objects`` to the target on a background job (Req 9.3).

        ``merge`` keeps the previous per-object results and upserts the new ones
        (used by "Retry failed" so a retry of a subset does not erase the rest of
        the table); the default replaces the results for a full apply run.
        """
        if not merge:
            conv_state.clear_outputs()
        session.set_workflow(
            with_status(
                session.workflow, WorkflowStep.SCHEMA_CONVERSION, StepStatus.IN_PROGRESS
            )
        )

        def work(_handle: object) -> None:
            conv_state.start_apply(len(objects))

            def _record_and_log(result: ObjectApplyResult) -> None:
                # Record live progress, then log the per-object outcome to the
                # downloadable activity log (CREATED/SKIPPED = ok, FAILED = error).
                conv_state.record_apply_progress(result)
                if result.status is ObjectApplyStatus.FAILED:
                    activity_status = ActivityStatus.FAILURE
                elif result.status is ObjectApplyStatus.SKIPPED:
                    activity_status = ActivityStatus.INFO
                else:
                    activity_status = ActivityStatus.SUCCESS
                detail = result.status.value
                if result.detail:
                    detail = f"{detail}: {result.detail}"
                log_activity(
                    ActivityCategory.SCHEMA_CONVERSION,
                    "apply object",
                    status=activity_status,
                    target=result.object_name,
                    detail=detail,
                )

            results = run_schema_apply(
                objects,
                applier=applier,
                mode=mode,
                confirmed=confirmed,
                on_object_start=conv_state.begin_apply_object,
                on_object_result=_record_and_log,
            )
            if merge:
                conv_state.merge_apply_results(results)
            else:
                conv_state.set_apply_results(results)

        conv_state.job_id = job_manager.submit(work)

    def _selected_apply_names() -> set[str]:
        """Object names in the user's current apply scope (Requirement 10.1/10.4).

        The apply must honor the same selection the user ticked and reviewed --
        only those objects are applied, never the whole source inventory. Uses
        the committed ``generated_node_ids`` when DDL has been generated (the
        reviewed scope), otherwise the live ``ticked_node_ids`` (the sidebar Run
        path, which is gated on at least one ticked object). Schema/category
        ticks contribute no object name, so an empty set means nothing is in
        scope and nothing is applied.
        """
        node_ids = (
            conv_state.generated_node_ids
            if conv_state.generated_node_ids is not None
            else conv_state.ticked_node_ids
        )
        return selected_object_names(node_ids)

    def _all_apply_objects(inventory: SourceInventory) -> list[ApplyObject]:
        """Build the selected scope's apply units: deterministic + edits + approved AI (Prop 13).

        The result is restricted to the objects the user selected (see
        :func:`_selected_apply_names`) so a partial selection applies only those
        objects, mirroring the generated-DDL preview scope.
        """
        selected = _selected_apply_names()
        objects = override_apply_objects(
            build_apply_objects(_conversion(inventory)), conv_state.edited_target_ddls
        )
        objects.extend(build_ai_apply_objects(conv_state.all_suggestions()))
        return [obj for obj in objects if obj.object_name in selected]

    def _refresh_view() -> None:
        """Re-render the screen via the refresh callback recorded by ``content``."""
        if conv_state.refresh_view is not None:
            conv_state.refresh_view()

    def _open_replace_dialog(detail: str, on_confirm: Callable[[], object]) -> None:
        """Open the action-time destructive-REPLACE confirmation dialog.

        Created in the click handler so the dialog attaches to the client layout
        (top-level) and survives the workflow shell's post-run refresh. ``Cancel``
        dismisses without applying; ``Confirm REPLACE`` runs ``on_confirm`` (which
        may return an awaitable for the per-object path). This replaces the old
        sticky confirmation checkbox so a destructive apply is confirmed at the
        moment it runs, never left armed for an accidental later click.
        """
        from nicegui import context as _ctx

        # Build the dialog in the client's TOP-LEVEL context, not the (possibly
        # deeply-nested expansion/editor) slot the triggering button lives in: a
        # dialog created inside a nested/dynamic slot may never render as a page
        # overlay (the slot is detached on the next refresh), so the confirm dialog
        # would silently fail to appear. Re-entering the client guarantees it shows.
        dlg_client = _ctx.client

        def _build_dialog() -> None:
            with ui.dialog() as dialog, ui.card().classes("gap-2").style(
                "min-width: 360px"
            ):
                ui.label("Confirm REPLACE").classes("text-lg font-semibold")
                ui.label(detail).classes("text-sm text-red-700")
                with ui.row().classes("justify-end gap-2 w-full"):
                    ui.button("Cancel", on_click=dialog.close).props("flat")

                    async def _confirm() -> None:
                        dialog.close()
                        result = on_confirm()
                        if inspect.isawaitable(result):
                            await result

                    ui.button("Confirm REPLACE", on_click=_confirm).props(
                        "color=negative"
                    )
            dialog.open()

        with dlg_client:  # type: ignore[attr-defined]
            _build_dialog()

    def _open_conflict_dialog(
        object_name: str,
        on_replace: Callable[[], object],
        on_skip: Callable[[], object],
    ) -> None:
        """Open the action-time REPLACE/SKIP choice dialog for one existing object.

        Shown when a per-object apply targets an object that already exists on the
        target and the global mode is SKIP (no edit forcing REPLACE): instead of
        silently skipping, the user picks explicitly. ``Replace`` drops and
        recreates (destructive); ``Skip`` leaves the target unchanged; ``Cancel``
        does nothing. This makes "apply to an existing object" a clear, per-action
        choice rather than a silent SKIP whose reason is easy to miss. Built in the
        client's top-level context for the same reason as the REPLACE dialog.
        """
        from nicegui import context as _ctx

        dlg_client = _ctx.client

        def _build_dialog() -> None:
            with ui.dialog() as dialog, ui.card().classes("gap-2").style(
                "min-width: 380px"
            ):
                ui.label(f"'{object_name}' already exists on the target").classes(
                    "text-lg font-semibold"
                )
                ui.label(
                    "Choose how to apply it. Replace drops and recreates the object "
                    "(destructive — any data in it is lost); Skip leaves the existing "
                    "object unchanged."
                ).classes("text-sm text-gray-600")
                with ui.row().classes("justify-end gap-2 w-full"):
                    ui.button("Cancel", on_click=dialog.close).props("flat")

                    async def _do_skip() -> None:
                        dialog.close()
                        result = on_skip()
                        if inspect.isawaitable(result):
                            await result

                    async def _do_replace() -> None:
                        dialog.close()
                        result = on_replace()
                        if inspect.isawaitable(result):
                            await result

                    ui.button("Skip", on_click=_do_skip).props(
                        "outline no-caps color=grey-8"
                    )
                    ui.button("Replace", on_click=_do_replace).props(
                        "unelevated no-caps color=negative icon=find_replace"
                    )
            dialog.open()

        with dlg_client:  # type: ignore[attr-defined]
            _build_dialog()

    def _run_bulk_apply(
        only_names: Optional[set[str]] = None, *, merge: bool = False
    ) -> None:
        """Submit the converted-scope apply (deterministic + edits + approved AI).

        ``only_names`` restricts the apply to those object names (used by "Retry
        failed"); ``None`` applies the full scope. ``merge`` upserts the results
        into the existing table instead of replacing it (also used by retry).
        """
        prepared = _prepare_apply()
        if prepared is None:
            return
        # Applying converted DDL means the step is no longer "skipped".
        session.set_schema_conversion_skipped(False)
        applier, mode, confirmed = prepared
        inventory = _inventory()
        assert inventory is not None  # guaranteed by _prepare_apply()
        objects = _all_apply_objects(inventory)
        if only_names is not None:
            objects = [obj for obj in objects if obj.object_name in only_names]
        _submit_apply(objects, applier, mode, confirmed, merge=merge)

    def _cdc_blocks_apply() -> bool:
        """True (and warn) when a live CDC pipeline must block applying schema.

        Once CDC is streaming, the sink is actively writing the target tables and
        Debezium does not propagate DDL. Applying schema now -- especially a
        destructive REPLACE, which DROPs and recreates the table -- would corrupt
        or truncate what CDC is replicating (data loss / broken pipeline). So while
        CDC is live, block Apply and tell the operator to stop CDC first. Guarded by
        the injected ``cdc_active_check`` (best-effort; absent in tests / when the
        data-migration state is not wired, in which case apply is unaffected).
        """
        try:
            active = cdc_active_check is not None and cdc_active_check()
        except Exception:  # noqa: BLE001 - never let a status probe block the UI
            active = False
        if active:
            ui.notify(  # type: ignore[attr-defined]
                "CDC is running — applying schema now could drop or corrupt tables "
                "the sink is writing (DDL is not replicated). Stop CDC first, then "
                "apply schema changes.",
                type="warning", position="top", timeout=8000,
            )
        return active

    def _bulk_apply(
        only_names: Optional[set[str]] = None, *, merge: bool = False
    ) -> None:
        """Apply the converted scope; for REPLACE, confirm first (dialog).

        Shared by the step's sidebar Run button, the in-card "Apply all" button,
        and "Retry failed" (scoped via ``only_names``/``merge``). In SKIP mode it
        submits immediately. In REPLACE mode it opens the action-time confirmation
        dialog listing the existing target objects in scope that will be dropped;
        only on confirm is the (one-shot) confirmation set and the apply
        submitted, then the view is refreshed to show progress.
        """
        if _cdc_blocks_apply():
            return
        if conv_state.apply_mode is ApplyMode.REPLACE:
            inventory = _inventory()
            existing = (
                _existing_object_names(inventory, existence_checker)
                if inventory is not None
                else []
            )
            # Scope the destructive list to the selected objects (and, for retry,
            # the failed subset) so the dialog names only what will be dropped.
            selected = _selected_apply_names()
            existing = [name for name in existing if name in selected]
            if only_names is not None:
                existing = [name for name in existing if name in only_names]

            def _confirmed() -> None:
                conv_state.replace_confirmed = True
                try:
                    _run_bulk_apply(only_names, merge=merge)
                finally:
                    conv_state.replace_confirmed = False
                _refresh_view()

            _open_replace_dialog(replace_confirmation_message(existing), _confirmed)
            return
        _run_bulk_apply(only_names, merge=merge)
        # SKIP submits immediately; refresh here so the in-progress panel and
        # poll timer appear. (REPLACE refreshes on confirm, above.) Callers must
        # NOT refresh again right after, or they would tear down the REPLACE
        # confirm dialog before the user can act.
        _refresh_view()

    def runner() -> None:
        """Apply the full converted scope (sidebar Run / in-card Apply all)."""
        _bulk_apply()

    def retry_failed() -> None:
        """Re-apply only the objects whose last apply result was FAILED.

        Uses the current (possibly edited) DDL for those objects, so the typical
        flow -- a FAILED object, fix its target DDL, retry -- works. Results are
        merged so the rest of the table is preserved. For REPLACE it confirms via
        the same dialog. A no-op when there are no failed objects.
        """
        results = conv_state.apply_results or []
        failed = {
            item.object_name
            for item in results
            if item.status is ObjectApplyStatus.FAILED
        }
        if not failed:
            return
        # _bulk_apply refreshes itself (SKIP) or on REPLACE confirm; do not
        # refresh here or the REPLACE confirm dialog would be torn down at once.
        _bulk_apply(failed, merge=True)

    def _mark_schema_done_if_complete() -> None:
        """Mark Schema Conversion DONE once every applicable object is applied OK.

        Mirrors the bulk-apply semantics (a finished apply with no failures is
        DONE) for the per-object inline "Apply to target" path, which otherwise
        never sets the step status. Only flips to DONE when ALL selected applicable
        objects have a non-FAILED result, so a partial one-by-one apply does not
        unlock early. Idempotent and safe to call after each inline apply.
        """
        inventory = _inventory()
        if inventory is None:
            return
        names = [obj.object_name for obj in _all_apply_objects(inventory)]
        if schema_apply_is_complete(names, conv_state.apply_results):
            session.set_workflow(  # type: ignore[attr-defined]
                with_status(
                    session.workflow,  # type: ignore[attr-defined]
                    WorkflowStep.SCHEMA_CONVERSION,
                    StepStatus.DONE,
                )
            )

    async def apply_object_inline(object_name: str) -> "Optional[ObjectApplyResult]":
        """Apply one generated object's DDL to the target inline (no page rebuild).

        Uses exactly the DDL the user reviewed for that object (deterministic or
        edited), applied with the current SKIP/REPLACE mode and confirmation. The
        apply runs off the event loop and the per-object result is stored so the
        editor can show an inline 'Applied' badge while the rest of the page
        (expansion, scroll position) is preserved. Problems are surfaced via
        notifications rather than a page-level error render. Returns the result,
        or ``None`` when the apply could not be attempted.
        """
        from nicegui import context as _ctx
        from nicegui import run as _run

        # Capture the originating client BEFORE the slow apply await: the apply runs
        # off the event loop (io_bound) and can outlive the triggering slot, so any
        # post-await UI feedback must re-enter this client (see _safe_post_await_ui).
        client = _ctx.client

        prepared = _prepare_apply()
        if prepared is None:
            ui.notify(conv_state.error or "Cannot apply to target.", type="negative")
            return None
        applier, mode, confirmed = prepared
        inventory = _inventory()
        assert inventory is not None  # guaranteed by _prepare_apply()
        one = [
            obj
            for obj in _all_apply_objects(inventory)
            if obj.object_name == object_name
        ]
        if not one:
            ui.notify(
                f"'{object_name}' has no applicable target DDL (only tables are "
                "applied; views/triggers/routines are reimplemented manually).",
                type="negative",
            )
            return None

        results = await _run.io_bound(
            run_schema_apply, one, applier=applier, mode=mode, confirmed=confirmed
        )
        conv_state.merge_apply_results(results)
        result = results[0] if results else None
        if result is not None:
            notify_type = {
                ObjectApplyStatus.CREATED: "positive",
                ObjectApplyStatus.SKIPPED: "warning",
                ObjectApplyStatus.FAILED: "negative",
            }.get(result.status, "info")
            _safe_post_await_ui(
                client,
                lambda: ui.notify(
                    f"{object_name}: {result.status.value}"
                    + (f" — {result.detail}" if result.detail else ""),
                    type=notify_type,
                ),
            )
            # Mirror the bulk-apply path: when a per-object apply creates a target
            # object, request the one-shot target-browser refresh so the new object
            # appears in the Target (Aurora DSQL) tree without a manual refresh.
            if result.status is ObjectApplyStatus.CREATED:
                conv_state.pending_target_refresh = True
        # Once every applicable object is applied OK (created or already-present/
        # skipped), mark the step DONE so "Next: Data Migration" unlocks -- the
        # bulk path does this via job completion; the inline path needs it here.
        _mark_schema_done_if_complete()
        return result

    async def apply_object_confirmed(object_name: str) -> None:
        """Apply one object inline; for REPLACE, confirm via dialog first.

        In SKIP mode this is the inline per-object apply (no page rebuild). In
        REPLACE mode it opens the action-time confirmation dialog for just this
        object; on confirm it sets the one-shot confirmation, applies inline, and
        refreshes so the per-object 'Applied' badge reflects the new state.
        """
        if _cdc_blocks_apply():
            return
        # Applying converted DDL means the step is no longer "skipped".
        session.set_schema_conversion_skipped(False)
        from nicegui import context as _ctx

        # Capture the client now so the post-apply refresh can re-enter a valid
        # context even if the triggering slot is torn down during the slow apply.
        client = _ctx.client
        edited = conv_state.get_edited_target_ddl(object_name) is not None
        try:
            exists = existence_checker is not None and existence_checker.object_exists(
                object_name
            )
        except Exception as exc:  # noqa: BLE001 - existence check must never break apply
            logger.debug("existence_checker failed for %s: %r", object_name, exc)
            exists = False
        # Applying an EDITED object that already exists requires REPLACE: SKIP would
        # skip it and silently drop the edit (the target keeps its old DDL). Route
        # such an apply through the REPLACE confirmation even when the global mode is
        # SKIP, so the user's change lands after they confirm the drop + recreate.
        if _apply_should_replace(apply_mode=conv_state.apply_mode, edited=edited):

            def _confirmed() -> object:
                async def _run() -> None:
                    prev_mode = conv_state.apply_mode
                    conv_state.apply_mode = ApplyMode.REPLACE
                    conv_state.replace_confirmed = True
                    try:
                        await apply_object_inline(object_name)
                    finally:
                        conv_state.replace_confirmed = False
                        conv_state.apply_mode = prev_mode
                    _safe_post_await_ui(client, _refresh_view)

                return _run()

            _open_replace_dialog(
                replace_confirmation_message([object_name] if exists else []),
                _confirmed,
            )
            return
        # Global SKIP mode, unedited object that ALREADY EXISTS on the target: don't
        # silently skip (the user can't tell the apply did nothing, and can't reach
        # REPLACE from here). Ask explicitly whether to Replace (drop + recreate) or
        # Skip. This is the case where a user reverting a choice (e.g. composite -> keep
        # integer PK) expects the target to change but SKIP would leave it as-is.
        if exists:

            def _on_replace() -> object:
                async def _run() -> None:
                    prev_mode = conv_state.apply_mode
                    conv_state.apply_mode = ApplyMode.REPLACE
                    conv_state.replace_confirmed = True
                    try:
                        await apply_object_inline(object_name)
                    finally:
                        conv_state.replace_confirmed = False
                        conv_state.apply_mode = prev_mode
                    _safe_post_await_ui(client, _refresh_view)

                return _run()

            def _on_skip() -> object:
                async def _run() -> None:
                    await apply_object_inline(object_name)
                    _safe_post_await_ui(client, _refresh_view)

                return _run()

            _open_conflict_dialog(object_name, _on_replace, _on_skip)
            return
        # Object does not exist yet: a plain CREATE, no conflict to resolve.
        await apply_object_inline(object_name)
        # SKIP mode applies inline (no page rebuild for scroll/expansion), but the
        # Apply results panel and the Target browser still need to reflect the new
        # state, so re-render once after the apply (mirrors the REPLACE path). Run it
        # in the captured client so a slot torn down during the slow apply can't crash.
        _safe_post_await_ui(client, _refresh_view)

    def content(refresh: Callable[[], None]) -> None:
        status = get_status(session.workflow, WorkflowStep.SCHEMA_CONVERSION)
        inventory = _inventory()
        # Record the live refresh callback so deferred handlers (the REPLACE
        # confirm dialog and the bulk runner shared with the sidebar Run) can
        # re-render the screen after they act.
        conv_state.refresh_view = refresh

        with ui.column().classes("w-full gap-3"):
            ui.label(
                "Browse the source objects, compare each object's source DDL with "
                "the converted target DDL, choose how to handle existing target "
                "objects, then apply the conversion."
            ).classes("text-sm text-gray-500")

            with ui.row().classes("items-center gap-2"):
                ui.label("Schema Conversion status:").classes("text-sm text-gray-500")
                ui.badge(status_label(status)).props(f"color={_STATUS_COLORS[status]}")

            if inventory is None:
                render_notice(
                    ui,
                    tone="warning",
                    header="Run Step 1 first",
                    body=(
                        "No source inventory yet. Run Step 1 (Evaluation) to "
                        "introspect the source schema."
                    ),
                )
                return

            # "Skip conversion" path: when the target schema is already prepared
            # (conversion applied in a prior session or out of band), the user
            # can skip this step. Marking it Done unlocks Data Migration (which is
            # gated on this step being Done). No requirement that every source
            # table exist on the target -- Data Migration only loads the tables
            # that actually have a target table, so a partial schema is fine.
            def skip_schema_conversion() -> None:
                session.set_workflow(
                    with_status(
                        session.workflow,
                        WorkflowStep.SCHEMA_CONVERSION,
                        StepStatus.DONE,
                    )
                )
                session.set_schema_conversion_skipped(True)
                ui.notify(  # type: ignore[attr-defined]
                    "Schema Conversion skipped. Continue to Data Migration -- it "
                    "will load only the tables that already exist on the target.",
                    type="positive",
                )
                # Jump straight to Data Migration when a navigation hook is wired;
                # otherwise just refresh (the step is now unlocked).
                if on_continue_to_data_migration is not None:
                    on_continue_to_data_migration()
                else:
                    refresh()

            with ui.card().classes("w-full"):
                render_notice(
                    ui,
                    tone="info",
                    header="Schema already prepared?",
                    body=(
                        "If the target tables already exist (conversion applied "
                        "earlier or out of band), skip this step to unlock Data "
                        "Migration. Data Migration loads only the tables that have "
                        "a target table, so not every source table needs to exist."
                    ),
                )
                ui.button(
                    "Skip conversion & continue to Data Migration",
                    on_click=skip_schema_conversion,
                    icon="skip_next",
                ).props("outline")

            # Defer the (possibly expensive) deterministic conversion until it
            # is actually needed: opening the page shows only the object
            # browser. The conversion runs when the user clicks "Generate DDL
            # for selected" (preview) or when AI suggestions are rendered for
            # flagged objects. Memoized so one render computes it at most once.
            conversion_cache: dict[str, SchemaConversionResult] = {}

            def get_conversion() -> SchemaConversionResult:
                if "result" not in conversion_cache:
                    conversion_cache["result"] = _conversion(inventory)
                return conversion_cache["result"]

            eval_result = eval_state.result
            target_inventory = (
                eval_result.target_inventory if eval_result is not None else None
            )
            # Resolve the existence checker: prefer the explicitly injected one
            # (tests), otherwise build one from the target inventory (which is
            # available after Evaluation's target browse or a "Refresh target").
            # This enables the per-object Apply to detect existing tables and show
            # the Replace/Skip dialog.
            nonlocal existence_checker
            if existence_checker is None and target_inventory is not None:
                existence_checker = _InventoryExistenceChecker(target_inventory)
            # AI assistance is integrated per generated object (not a separate
            # section): for each object the deterministic conversion and the AI
            # suggestion are compared and shown as one view when identical, or as
            # "Converted" / "AI Suggested" tabs when they differ. Candidacy is
            # limited to MANUAL/UNSUPPORTED objects and only when AI is enabled.
            assessment = (
                eval_result.assessment if eval_result is not None else None
            )
            ai_candidates = (
                set(ai_candidate_object_names(assessment))
                if assessment is not None and session.ai_assist.enabled
                else set()
            )
            async def refresh_source() -> None:
                """Re-introspect the source DB and refresh the source tree."""
                from nicegui import run as _run

                from dsql_migrator.ui.evaluation import (
                    EvaluationResult as _ER,
                )
                from dsql_migrator.ui.evaluation import (
                    _default_introspector_factory,
                )

                if not session.has_source():
                    ui.notify(  # type: ignore[attr-defined]
                        "Source connection not configured.", type="negative"
                    )
                    return
                source_conn = session.source_config
                assert source_conn is not None
                introspector = _default_introspector_factory(session.source_password)
                try:
                    new_inventory = await _run.io_bound(
                        introspector.introspect, source_conn
                    )
                except Exception as exc:  # noqa: BLE001
                    ui.notify(  # type: ignore[attr-defined]
                        f"Could not refresh source: {exc}", type="negative"
                    )
                    return
                old_result = eval_state.result
                if old_result is not None:
                    eval_state.set_result(
                        _ER(
                            inventory=new_inventory,
                            assessment=old_result.assessment,
                            target_inventory=old_result.target_inventory,
                            target_conflicts=old_result.target_conflicts,
                        )
                    )
                ui.notify("Source browser refreshed.", type="positive")  # type: ignore[attr-defined]
                refresh()

            async def refresh_target() -> None:
                """Re-introspect the target DSQL catalog and refresh the tree."""
                from nicegui import run as _run

                from dsql_migrator.ui.evaluation import (
                    EvaluationResult as _ER,
                )
                from dsql_migrator.ui.evaluation import (
                    _default_target_browser_factory,
                    _find_target_conflicts,
                )

                if not session.has_target():
                    ui.notify(  # type: ignore[attr-defined]
                        "Target connection not configured.", type="negative"
                    )
                    return
                target_conn = session.target_config
                assert target_conn is not None
                browser = _default_target_browser_factory(session.aws_profile)
                try:
                    new_target = await _run.io_bound(browser.browse, target_conn)
                except Exception as exc:  # noqa: BLE001
                    ui.notify(  # type: ignore[attr-defined]
                        f"Could not refresh target: {exc}", type="negative"
                    )
                    return
                old_result = eval_state.result
                if old_result is not None:
                    eval_state.set_result(
                        _ER(
                            inventory=old_result.inventory,
                            assessment=old_result.assessment,
                            target_inventory=new_target,
                            target_conflicts=_find_target_conflicts(
                                old_result.inventory, new_target
                            ),
                        )
                    )
                ui.notify("Target browser refreshed.", type="positive")  # type: ignore[attr-defined]
                refresh()

            # Shared AI chat drawer (same component/look as the Evaluation
            # screen), opened per object to chat about converting it. Advisory
            # only: the user reads/copies SQL and pastes it into the object's
            # editor (no auto-adopt, since a reply can contain several illustrative
            # SQL blocks that must not all be applied).
            open_chat = build_chat_drawer(ui) if session.ai_assist.enabled else None

            def open_conversion_chat(
                object_name: str, source_ddl: str, deterministic: str
            ) -> None:
                if open_chat is None:
                    return
                strategist = AssessmentStrategist(
                    session.ai_assist, aws_profile=session.aws_profile
                )
                system = build_conversion_chat_system(
                    object_name, source_ddl, deterministic
                )
                open_chat(
                    title="AI conversion assistant",
                    subtitle=f"{object_name}",
                    first_question=(
                        f"How should I convert {object_name} to Aurora DSQL? "
                        "Walk me through the DDL changes."
                    ),
                    streamer=lambda messages, on_delta: strategist.stream_chat(
                        system, messages, on_delta
                    ),
                )

            with ui.card().classes("w-full"):
                _render_browser_and_preview(
                    ui,
                    inventory,
                    get_conversion,
                    conv_state,
                    existence_checker,
                    refresh,
                    target_inventory=target_inventory,
                    ai_candidates=ai_candidates,
                    assistant=(
                        _resolve_assistant() if session.ai_assist.enabled else None
                    ),
                    on_apply_object=apply_object_confirmed,
                    on_refresh_source=refresh_source,
                    on_refresh_target=refresh_target,
                    on_ai_chat=(
                        open_conversion_chat
                        if session.ai_assist.enabled
                        else None
                    ),
                )

            def apply_all() -> None:
                # Co-located with the apply settings (mode/confirmation) so the
                # action lives where it is configured. Applies the full converted
                # scope -- identical to the step's Run button. ``runner`` (the
                # bulk apply) refreshes the view ITSELF: SKIP submits and refreshes
                # to show progress; REPLACE opens its confirm dialog and refreshes
                # on confirm. We must NOT refresh here, or it would tear down the
                # REPLACE confirm dialog the instant it opens.
                runner()

            with ui.card().classes("w-full"):
                _render_apply_controls(
                    ui,
                    conv_state,
                    refresh,
                    on_apply_all=apply_all,
                    table_count=len(_all_apply_objects(inventory)),
                    in_progress=status is StepStatus.IN_PROGRESS,
                )

                error = conv_state.error
                if error and status is not StepStatus.IN_PROGRESS:
                    render_notice(
                        ui,
                        tone="error",
                        header="Schema apply failed",
                        body=error,
                    )

                if status is StepStatus.IN_PROGRESS:
                    done = conv_state.apply_done
                    total = conv_state.apply_total
                    current = conv_state.apply_current
                    with ui.row().classes("items-center gap-2"):
                        ui.spinner(size="sm")
                        ui.label(apply_progress_text(done, total)).classes(
                            "text-sm text-gray-500"
                        )
                    if total > 0:
                        ui.linear_progress(  # type: ignore[attr-defined]
                            value=done / total, show_value=False
                        ).props("instant-feedback").classes("w-full")
                    if current is not None:
                        ui.label(f"Currently applying: {current}").classes(
                            "text-xs text-gray-500"
                        )
                    _install_poll_timer(ui, job_manager, session, conv_state, refresh)

                apply_results = conv_state.apply_results
                if apply_results is not None:
                    _render_apply_results(
                        ui,
                        apply_results,
                        on_retry_failed=retry_failed,
                        in_progress=status is StepStatus.IN_PROGRESS,
                    )
                    ui.label(  # type: ignore[attr-defined]
                        "Newly created objects are reflected in the Target "
                        "(Aurora DSQL) browser above; use its refresh icon to "
                        "refresh again."
                    ).classes("text-xs text-gray-500")

                # Auto-refresh the target browser once after a successful apply so
                # newly created objects appear without a manual refresh. The flag
                # is one-shot (cleared here) and the refresh runs on a one-shot
                # timer because it is async (a live target browse).
                if conv_state.pending_target_refresh and session.has_target():
                    conv_state.pending_target_refresh = False
                    ui.timer(  # type: ignore[attr-defined]
                        0.1, refresh_target, once=True, immediate=False
                    )

    return content, runner


def _install_poll_timer(
    ui: object,
    job_manager: JobManager,
    session: object,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
) -> None:
    """Poll the running apply job and finalize the step status on completion."""
    job_id = conv_state.job_id
    if job_id is None:
        return

    def poll() -> None:
        try:
            job = job_manager.get_status(job_id)
        except JobNotFoundError:
            return
        mapped = job_status_to_step_status(job.status)
        if mapped is None:
            return
        if mapped is StepStatus.FAILED:
            conv_state.set_error(
                job_manager.get_error(job_id) or "Schema apply failed."
            )
        elif mapped is StepStatus.DONE:
            # Request a one-shot target-browser refresh when the apply created at
            # least one object, so newly created objects show without the user
            # having to click the target refresh icon.
            results = conv_state.apply_results or []
            if any(item.status is ObjectApplyStatus.CREATED for item in results):
                conv_state.pending_target_refresh = True
        session.set_workflow(  # type: ignore[attr-defined]
            with_status(
                session.workflow,  # type: ignore[attr-defined]
                WorkflowStep.SCHEMA_CONVERSION,
                mapped,
            )
        )
        refresh()

    ui.timer(_POLL_INTERVAL_SECONDS, poll)  # type: ignore[attr-defined]


def _render_browser_and_preview(
    ui: object,
    inventory: SourceInventory,
    result_provider: Callable[[], SchemaConversionResult],
    conv_state: SchemaConversionState,
    existence_checker: Optional[TargetExistenceChecker],
    refresh: Callable[[], None],
    *,
    target_inventory: Optional[TargetInventory] = None,
    ai_candidates: Optional[set[str]] = None,
    assistant: Optional[AiConversionAssistant] = None,
    on_apply_object: Optional[Callable[[str], None]] = None,
    on_refresh_source: Optional[Callable[[], object]] = None,
    on_refresh_target: Optional[Callable[[], object]] = None,
    on_ai_chat: Optional[Callable[[str, str, str], None]] = None,
) -> None:
    """Render side-by-side source/target browsers and the selected DDL diff.

    The two object browsers sit left (source MySQL) and right (target DSQL),
    each inside a fixed-height scroll area so an expanded tree never stretches
    the page. Selecting a source table/view shows its source DDL and the
    converted target DDL in a left/right comparison below. ``result_provider``
    lazily computes the deterministic conversion; it is invoked only when the
    user has generated DDL, so merely opening the screen runs no conversion.
    """
    existing = _existing_object_names(inventory, existence_checker)
    source_nodes = build_object_tree(
        inventory, schema_label="source", existing_objects=existing
    )
    target_nodes = build_target_object_tree(target_inventory)

    ui.label("Object browser").classes("text-lg font-semibold")  # type: ignore[attr-defined]
    ui.label(  # type: ignore[attr-defined]
        "Tick schemas or objects in the source browser, then click "
        "\"Generate DDL for selected\" to produce the source and target DDL."
    ).classes("text-sm text-gray-500")
    with ui.row().classes("w-full gap-4 items-stretch no-wrap"):  # type: ignore[attr-defined]
        # --- Source browser (checkboxes; selection drives DDL generation) -
        with ui.card().classes("w-1/2 min-w-0 !shadow-sm"):  # type: ignore[attr-defined]
            with ui.row().classes("items-center justify-between w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label("Source (MySQL)").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-blue-800"
                )
                if on_refresh_source is not None:
                    ui.button(on_click=on_refresh_source).props(  # type: ignore[attr-defined]
                        "flat dense round size=sm icon=refresh"
                    ).tooltip("Refresh source objects")
            # Bulk selection: tick/untick every selectable object leaf at once
            # (the per-object ticks still work for fine-grained picks). Programmatic
            # tick/untick does not fire on_tick, so keep ticked_node_ids in sync here.
            with ui.row().classes("items-center gap-1 w-full no-wrap"):  # type: ignore[attr-defined]

                def _sc_select_all() -> None:
                    leaf_ids = _tree_leaf_ids(source_nodes)
                    conv_state.ticked_node_ids = leaf_ids
                    tree.tick(leaf_ids)

                def _sc_unselect_all() -> None:
                    conv_state.ticked_node_ids = []
                    tree.untick()

                ui.button("Select all", on_click=_sc_select_all).props(  # type: ignore[attr-defined]
                    "flat dense no-caps size=sm color=primary icon=done_all"
                )
                ui.button("Unselect all", on_click=_sc_unselect_all).props(  # type: ignore[attr-defined]
                    "flat dense no-caps size=sm color=grey-7 icon=remove_done"
                )
            # Name filter: for large schemas (thousands of objects) typing here
            # narrows the q-tree to MATCHING nodes only, so the browser never has
            # to render an entire "Tables (N)" category at once. The full node set
            # stays in the tree (search still finds any object); only the rendered
            # DOM is bounded -- the key scalability lever for big schemas.
            src_filter = (
                ui.input(placeholder="Filter objects by name")  # type: ignore[attr-defined]
                .props("dense clearable outlined")
                .classes("w-full")
            )
            # Legend for the per-table primary-key indicator shown beside each
            # table leaf (matches the Step 3 "Tables to migrate" browser). Only
            # tables carry it; views/triggers/routines have no PK concept.
            with ui.row().classes(  # type: ignore[attr-defined]
                "items-center gap-3 w-full text-xs text-gray-500"
            ):
                with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon("check_circle", color="green-6").classes("text-sm")  # type: ignore[attr-defined]
                    ui.label("Table has a primary key")  # type: ignore[attr-defined]
                with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon("warning", color="amber-7").classes("text-sm")  # type: ignore[attr-defined]
                    ui.label("No primary key (required for Aurora DSQL)")  # type: ignore[attr-defined]
            with ui.scroll_area().classes(  # type: ignore[attr-defined]
                "w-full bg-white rounded-md border border-gray-200"
            ).style("height: 340px"):

                def on_tick(event: object) -> None:
                    value = getattr(event, "value", None)
                    conv_state.ticked_node_ids = list(value) if value else []

                tree = ui.tree(  # type: ignore[attr-defined]
                    source_nodes,
                    label_key="label",
                    node_key="id",
                    tick_strategy="leaf",
                    on_tick=on_tick,
                )
                tree.props("no-connectors")  # type: ignore[attr-defined]
                src_filter.bind_value_to(tree, "filter")  # type: ignore[attr-defined]
                # A small PK indicator beside each table leaf (client-side Vue
                # template, so no per-node Python work): green check when the
                # table has a primary key, amber warning when it does not. Only
                # nodes with "header": "table" (table leaves) use this slot.
                tree.add_slot(  # type: ignore[attr-defined]
                    "header-table",
                    r"""
                    <div class="row items-center no-wrap">
                      <span class="text-body2">{{ props.node.label }}</span>
                      <q-icon v-if="props.node.has_pk" name="check_circle"
                              color="green-6" size="16px" class="q-ml-xs">
                        <q-tooltip>Table has a primary key</q-tooltip>
                      </q-icon>
                      <q-icon v-else name="warning" color="amber-7" size="16px" class="q-ml-xs">
                        <q-tooltip>No primary key — required to migrate to Aurora DSQL</q-tooltip>
                      </q-icon>
                    </div>
                    """,
                )
                # Start collapsed so a schema expands to reveal its tables; keep
                # the ticked set across refreshes (the tree is rebuilt on Generate).
                if conv_state.ticked_node_ids:
                    tree.tick(list(conv_state.ticked_node_ids))

        # --- Target browser (browse-only: what already exists on DSQL) ----
        with ui.card().classes("w-1/2 min-w-0 !shadow-sm"):  # type: ignore[attr-defined]
            with ui.row().classes("items-center justify-between w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label("Target (Aurora DSQL)").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-green-800"
                )
                if on_refresh_target is not None:
                    ui.button(on_click=on_refresh_target).props(  # type: ignore[attr-defined]
                        "flat dense round size=sm icon=refresh"
                    ).tooltip("Refresh target catalog")
            if target_nodes:
                tgt_filter = (
                    ui.input(placeholder="Filter objects by name")  # type: ignore[attr-defined]
                    .props("dense clearable outlined")
                    .classes("w-full")
                )
            with ui.scroll_area().classes(  # type: ignore[attr-defined]
                "w-full bg-white rounded-md border border-gray-200"
            ).style("height: 340px"):
                if target_nodes:
                    tgt_tree = ui.tree(  # type: ignore[attr-defined]
                        target_nodes, label_key="label", node_key="id"
                    )
                    tgt_tree.props("no-connectors")  # type: ignore[attr-defined]
                    tgt_filter.bind_value_to(tgt_tree, "filter")  # type: ignore[attr-defined]
                else:
                    ui.label(  # type: ignore[attr-defined]
                        "No target objects to browse yet. Run Step 1 "
                        "(Evaluation) to introspect the target catalog."
                    ).classes("text-sm text-gray-500")

    # --- Generate DDL for the ticked objects ------------------------------
    def on_generate() -> None:
        conv_state.generated_node_ids = list(conv_state.ticked_node_ids)
        refresh()

    def on_clear() -> None:
        # Full reset: discard the generated DDL, per-object edits, AI
        # suggestions, and prior apply results so the screen is ready for a
        # fresh generate/run.
        conv_state.reset_generation()
        refresh()

    with ui.row().classes("gap-2 items-center"):  # type: ignore[attr-defined]
        gen_btn = ui.button(  # type: ignore[attr-defined]
            "Generate DDL for selected", on_click=on_generate
        ).props("color=primary")
        if conv_state.generated_node_ids is not None:
            # Lock re-generation until "Reset all": clicking Generate again would
            # silently re-run over the same committed scope with no visible change
            # (so it looks unresponsive). Disable it and require an explicit reset
            # to start a fresh generation, which makes the regeneration obvious.
            gen_btn.disable()  # type: ignore[attr-defined]
            ui.button("Reset all", on_click=on_clear).props(  # type: ignore[attr-defined]
                "flat icon=restart_alt"
            ).tooltip(
                "Discard the generated DDL, edits, AI suggestions, and apply "
                "results, and start fresh."
            )
            ui.label(  # type: ignore[attr-defined]
                'Generated below — use "Reset all" to generate a new selection.'
            ).classes("text-xs text-gray-500")

    # --- Generated DDL comparison (only after Generate) -------------------
    if conv_state.generated_node_ids is None:
        ui.label(  # type: ignore[attr-defined]
            "No DDL generated yet. Tick objects above and click "
            "\"Generate DDL for selected\"."
        ).classes("text-sm text-gray-500")
        return

    previews = generate_previews(
        conv_state.generated_node_ids,
        inventory,
        result_provider(),
        existence_checker=existence_checker,
    )
    if not previews:
        inline_hint(
            ui,
            "No tables or views were selected. Tick table/view objects (not just "
            "schemas) and generate again.",
            tone="neutral",
        )
        return

    # Warn that DSQL-unsupported source object kinds (triggers, stored
    # routines, scheduled events) are NOT auto-converted and not listed below;
    # they need manual reimplementation. Shown at the top of the generated list.
    unsupported_parts: list[str] = []
    if inventory.triggers:
        unsupported_parts.append(f"{len(inventory.triggers)} trigger(s)")
    if inventory.routines:
        unsupported_parts.append(f"{len(inventory.routines)} stored routine(s)")
    if inventory.events:
        unsupported_parts.append(f"{len(inventory.events)} event(s)")
    if unsupported_parts:
        render_notice(
            ui,
            tone="warning",
            header="Some source objects can't be converted to Aurora DSQL",
            body=(
                f"{', '.join(unsupported_parts)} are not shown below. Aurora DSQL "
                "does not support triggers, stored procedures/functions, or "
                "scheduled events — reimplement that logic in your application "
                "(or an external scheduler) instead."
            ),
        )

    def on_toggle_expand() -> None:
        conv_state.expand_all = not conv_state.expand_all
        if not conv_state.expand_all:
            conv_state.expanded_objects.clear()
        refresh()

    with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
        ui.label(f"Generated DDL ({len(previews)} object(s))").classes(  # type: ignore[attr-defined]
            "text-md font-semibold"
        )
        ui.space()  # type: ignore[attr-defined]
        ui.button(  # type: ignore[attr-defined]
            "Collapse all" if conv_state.expand_all else "Expand all",
            on_click=on_toggle_expand,
        ).props("flat dense")
    candidates = ai_candidates or set()

    def _render_preview_expansion(preview: DdlPreview) -> None:
        caption = _object_header_summary(
            preview,
            edited=conv_state.get_edited_target_ddl(preview.object_name) is not None,
            applied=conv_state.get_apply_result(preview.object_name),
        )
        expanded = (
            conv_state.expand_all
            or preview.object_name in conv_state.expanded_objects
        )

        def _on_expand_change(event: object, _name: str = preview.object_name) -> None:
            if getattr(event, "value", False):
                conv_state.expanded_objects.add(_name)
            else:
                conv_state.expanded_objects.discard(_name)

        with ui.expansion(  # type: ignore[attr-defined]
            preview.object_name,
            caption=caption or None,
            value=expanded,
            on_value_change=_on_expand_change,
        ).classes("w-full").props("expand-separator"):
            # An object is eligible for an AI suggestion when the assessment
            # flagged it MANUAL/UNSUPPORTED, OR the deterministic converter did
            # not produce a target DDL (views/triggers/routines/events/etc.).
            # So any "Not auto-converted" object also gets the AI conversion
            # button when AI assist is enabled.
            not_auto_converted = not _is_applicable_target_ddl(preview.target_ddl)
            # Per-table primary-key strategy picker (opt-in composite key), shown
            # above the DDL preview for an auto-converted TABLE only (views/other
            # objects have no primary-key hot-partition concern). Changing it bakes
            # the composite DDL into edited_target_ddls, so the preview below and
            # Full Load pick it up.
            source_table = _find_table(inventory, preview.object_name)
            if source_table is not None and not not_auto_converted:
                _render_pk_strategy_picker(ui, source_table, conv_state, refresh)
            _render_preview(
                ui,
                preview,
                conv_state,
                refresh,
                is_ai_candidate=(
                    preview.object_name in candidates or not_auto_converted
                ),
                assistant=assistant,
                on_apply_object=on_apply_object,
                on_ai_chat=on_ai_chat,
            )

    # Group the generated objects by kind (Tables, then Views) with a small
    # section header each, so a mixed selection reads by category instead of one
    # flat list. previews only ever contain tables/views, so anything that is not
    # a known view name is a table.
    view_names = {view.name for view in inventory.views}
    tables = [p for p in previews if p.object_name not in view_names]
    views = [p for p in previews if p.object_name in view_names]
    for section_label, group in (("Tables", tables), ("Views", views)):
        if not group:
            continue
        ui.label(f"{section_label} ({len(group)})").classes(  # type: ignore[attr-defined]
            "text-sm font-semibold text-gray-700 mt-2"
        )
        for preview in group:
            _render_preview_expansion(preview)


def _object_header_summary(
    preview: DdlPreview,
    *,
    edited: bool,
    applied: Optional[ObjectApplyResult],
) -> str:
    """Build the one-line caption shown on a generated-object expansion header.

    Surfaces an object's at-a-glance status -- exists on target, conversion
    warning count, whether the target DDL was edited, and the last apply result
    -- so the user can scan the (collapsed) Generated DDL list and tell which
    objects need attention without opening each one. Returns an empty string
    when there is nothing noteworthy to show.
    """
    parts: list[str] = []
    if preview.exists_on_target is True:
        parts.append("exists on target")
    if preview.warnings:
        count = len(preview.warnings)
        warning_text = f"{count} warning{'s' if count != 1 else ''}"
        # Surface the severity (Unsupported > Review needed) so the user sees that
        # an object needs manual work, not merely that it has "N warnings".
        classes = {w.classification for w in preview.warnings}
        if Classification.UNSUPPORTED in classes:
            parts.append(f"{classification_label('UNSUPPORTED')} · {warning_text}")
        elif Classification.MANUAL in classes:
            parts.append(f"{classification_label('MANUAL')} · {warning_text}")
        else:
            parts.append(warning_text)
    if edited:
        parts.append("edited")
    if applied is not None:
        parts.append(f"applied: {applied.status.value}")
    return " · ".join(parts)


_KEEP_PK = "KEEP"
_COMPOSITE_PK = "COMPOSITE"


def _render_pk_strategy_picker(
    ui: object,
    table: TableDef,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
) -> None:
    """Per-table primary-key strategy picker (Keep source PK vs Composite key).

    Opt-in and per-table: the default is Keep source PK (the source key
    unchanged). Choosing "Composite key" rewrites this table's target key to
    ``(leading, original_pk...)`` to spread writes across Aurora DSQL partitions
    (avoiding the monotonic-key hot partition). The choice is stored by baking the
    composite target script into ``conv_state.edited_target_ddls`` -- the same
    field Full Load and Schema Apply already consume and the session snapshot
    persists -- so the picker holds NO separate state and is resume-safe. The
    picker's rendered state is derived by parsing that stored DDL.
    """
    # Only tables with a primary key can have a monotonic-key hot partition to fix.
    if not table.primary_key:
        return
    candidates = composite_leading_candidates(table)
    name = table.name
    stored = conv_state.get_edited_target_ddl(name)
    current_leading = (
        composite_leading_from_ddl(table, stored) if stored is not None else None
    )
    is_composite = current_leading is not None
    converter = SchemaConverter()

    def _select_strategy(event: object) -> None:
        choice = getattr(event, "value", _KEEP_PK)
        if choice == _COMPOSITE_PK:
            leading = default_composite_leading(table)
            if leading is None:
                return  # no eligible column; the control stays on Keep (see below)
            conv_state.set_edited_target_ddl(
                name, render_target_ddl(build_composite_conversion(converter, table, leading))
            )
        else:
            # Back to the source key: drop the composite override so the table
            # uses the deterministic (unchanged-key) conversion again.
            conv_state.clear_edited_target_ddl(name)
        refresh()

    def _select_leading(event: object) -> None:
        leading = getattr(event, "value", None)
        if not leading:
            return
        conv_state.set_edited_target_ddl(
            name, render_target_ddl(build_composite_conversion(converter, table, leading))
        )
        refresh()

    with ui.card().classes("w-full !shadow-none border border-gray-200 bg-gray-50 p-3 gap-2"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-3 w-full no-wrap"):  # type: ignore[attr-defined]
            ui.label("Primary key").classes("text-sm font-semibold text-gray-700")  # type: ignore[attr-defined]
            picker = segmented_control(
                ui,
                {_KEEP_PK: "Keep source PK", _COMPOSITE_PK: "Composite key"},
                value=_COMPOSITE_PK if is_composite else _KEEP_PK,
                on_change=_select_strategy,
            )
            if not candidates:
                # Nothing valid to lead with (no NOT NULL non-PK column), so the
                # composite option cannot be offered for this table.
                picker.props("disable")
        if not candidates:
            inline_hint(
                ui,
                "No NOT NULL non-key column is available to lead a composite key "
                "for this table.",
                tone="neutral",
            )
            return
        if is_composite:
            ui.select(  # type: ignore[attr-defined]
                candidates,
                value=current_leading,
                label="Leading column (high-cardinality)",
                on_change=_select_leading,
            ).classes("w-full max-w-md").props("dense outlined")
            key_order = [current_leading, *table.primary_key]
            render_notice(
                ui,
                tone="warning",
                header="Queries must use the new composite key after cutover",
                body=(
                    f"The target primary key becomes ({', '.join(key_order)}). This "
                    "spreads writes across DSQL partitions, but the application's "
                    "queries, joins, and upserts must key on the full composite key, "
                    f"and '{current_leading}' must be immutable (DSQL keys cannot "
                    "change after creation). A UNIQUE index on the original key "
                    f"({', '.join(table.primary_key)}) preserves its uniqueness. "
                    "Not yet supported with CDC."
                ),
            )


def _render_preview(
    ui: object,
    preview: DdlPreview,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
    *,
    is_ai_candidate: bool = False,
    assistant: Optional[AiConversionAssistant] = None,
    on_apply_object: Optional[Callable[[str], None]] = None,
    on_ai_chat: Optional[Callable[[str, str, str], None]] = None,
) -> None:
    """Render one object's source-vs-target DDL diff (Req 10.2, 11.5).

    For an auto-converted table the source and target DDL are shown as a
    side-by-side, change-highlighted diff (via the editable target), so the user
    sees exactly what the conversion changed. The target is editable (the edit is
    remembered per object and used on apply, and the diff re-computes against the
    edit). When an AI suggestion exists it is compared with the deterministic
    conversion: if equivalent a single view is shown (with a note); if they
    differ the target is split into "Converted" and "AI Suggested" tabs. Objects
    that are not auto-converted (views, triggers, routines) show their source and
    a read-only not-converted note instead.
    """
    if preview.exists_on_target is True:
        render_notice(
            ui,
            tone="warning",
            header="Object already exists on target",
            body=(
                f"'{preview.object_name}' already exists on the target. Choose "
                "SKIP to keep it or REPLACE (destructive) to recreate it."
            ),
        )

    editable = _is_applicable_target_ddl(preview.target_ddl)
    suggestion = conv_state.get_suggestion(preview.object_name)
    with ui.column().classes("w-full gap-3"):  # type: ignore[attr-defined]
        if not editable:
            # Not auto-converted (view/trigger/routine). Show the source, then
            # either the AI suggestion (once generated, for review/approve) or
            # the not-converted note plus the "Generate AI suggestion" button so
            # the user can convert it with AI.
            with ui.row().classes("items-center gap-1 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label("Source DDL (MySQL)").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-blue-800"
                )
                ui.space()  # type: ignore[attr-defined]
                _render_copy_ddl_button(ui, preview.source_ddl, label="Source DDL")
            ui.code(preview.source_ddl, language="sql").classes("w-full")  # type: ignore[attr-defined]
            if suggestion is not None:
                _render_suggestion_review(ui, suggestion, conv_state, refresh)
            else:
                with ui.row().classes("items-center gap-1 w-full no-wrap"):  # type: ignore[attr-defined]
                    ui.label("Target DDL (Aurora DSQL)").classes(  # type: ignore[attr-defined]
                        "text-sm font-semibold text-green-800"
                    )
                    ui.space()  # type: ignore[attr-defined]
                    _render_copy_ddl_button(ui, preview.target_ddl, label="Target DDL")
                ui.code(preview.target_ddl, language="sql").classes("w-full")  # type: ignore[attr-defined]
                if is_ai_candidate:
                    _render_generate_suggestion(
                        ui,
                        object_name=preview.object_name,
                        source_ddl=preview.source_ddl,
                        deterministic=preview.target_ddl,
                        conv_state=conv_state,
                        assistant=assistant,
                        refresh=refresh,
                        on_ai_chat=on_ai_chat,
                    )
        elif suggestion is not None and not ddl_equivalent(
            preview.target_ddl, suggestion.suggested_sql_or_expr
        ):
            # Deterministic and AI conversions differ: separate them into tabs.
            # The "Converted" tab shows the source/target diff (editable).
            _render_target_tabs(
                ui, preview, suggestion, conv_state, refresh, on_apply_object
            )
        else:
            # Single view: the source/target diff over the deterministic
            # (editable) conversion. The AI guidance button sits inline on the
            # Edit/Apply toolbar (centered) via extra_actions for an AI candidate.
            def _ai_extra() -> None:
                _render_generate_suggestion(
                    ui,
                    object_name=preview.object_name,
                    source_ddl=preview.source_ddl,
                    deterministic=preview.target_ddl,
                    conv_state=conv_state,
                    assistant=assistant,
                    refresh=refresh,
                    on_ai_chat=on_ai_chat,
                )

            _render_editable_target(
                ui,
                preview,
                conv_state,
                on_apply_object,
                extra_actions=(_ai_extra if is_ai_candidate else None),
            )
            if suggestion is not None:
                ui.label(  # type: ignore[attr-defined]
                    "AI suggestion matches the deterministic conversion."
                ).classes("text-xs text-gray-500")

    if preview.warnings:
        _render_conversion_warnings(ui, preview.warnings)


def _render_target_tabs(
    ui: object,
    preview: DdlPreview,
    suggestion: AiConversionSuggestion,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
    on_apply_object: Optional[Callable[[str], None]] = None,
) -> None:
    """Show the deterministic vs AI target DDL as 'Converted' / 'AI Suggested' tabs.

    The "Converted" tab holds the editable deterministic DDL (applied as
    written); the "AI Suggested" tab holds the AI suggestion with its review
    controls. Only an explicitly approved suggestion is applied (Property 13);
    editing/approving happens on the AI tab.
    """
    with ui.tabs().props("dense").classes("w-full") as tabs:  # type: ignore[attr-defined]
        converted_tab = ui.tab("Converted")  # type: ignore[attr-defined]
        ai_tab = ui.tab("AI Suggested")  # type: ignore[attr-defined]
    with ui.tab_panels(tabs, value=converted_tab).classes("w-full"):  # type: ignore[attr-defined]
        with ui.tab_panel(converted_tab):  # type: ignore[attr-defined]
            _render_editable_target(
                ui,
                preview,
                conv_state,
                on_apply_object,
                read_only_caption=(
                    "Deterministic conversion — editable, applied as written"
                ),
            )
        with ui.tab_panel(ai_tab):  # type: ignore[attr-defined]
            _render_suggestion_review(ui, suggestion, conv_state, refresh)


def ddl_equivalent(left: str, right: str) -> bool:
    """Return whether two DDL scripts are equivalent ignoring formatting.

    Each script is split into single statements (on ``;``), trimmed, and
    compared. This treats trailing semicolons and surrounding whitespace as
    insignificant so a deterministic conversion and an AI suggestion that only
    differ cosmetically are shown as one view rather than as differing tabs.
    """
    def _normalize(text: str) -> list[str]:
        return [stmt.strip() for stmt in split_sql_statements(text)]

    return _normalize(left) == _normalize(right)


# Quasar badge colors for a conversion warning's classification (severity cue).
_WARNING_BADGE_COLOR: dict[str, str] = {
    Classification.UNSUPPORTED.value: "negative",
    Classification.MANUAL.value: "warning",
    Classification.AUTO.value: "positive",
}


def _render_conversion_warnings(
    ui: object, warnings: Sequence[ConversionWarning]
) -> None:
    """Render conversion warnings as a wrapping list (not a truncating table).

    Each warning is a full-width row: a severity badge, an optional column
    badge, and the message in a flexible cell that wraps, so long messages are
    never cut off on the right (unlike fixed table columns).
    """
    ui.label("Conversion warnings").classes("text-sm font-semibold")  # type: ignore[attr-defined]
    with ui.column().classes("w-full gap-1"):  # type: ignore[attr-defined]
        for warning in warnings:
            color = _WARNING_BADGE_COLOR.get(warning.classification.value, "grey")
            with ui.row().classes(  # type: ignore[attr-defined]
                "items-start gap-2 w-full no-wrap border rounded p-2"
            ):
                ui.badge(warning.classification.value).props(f"color={color}")  # type: ignore[attr-defined]
                if warning.column_name:
                    ui.badge(warning.column_name).props(  # type: ignore[attr-defined]
                        "color=blue-grey-6 outline"
                    )
                ui.label(warning.message).classes(  # type: ignore[attr-defined]
                    "text-sm flex-1 min-w-0 whitespace-normal break-words"
                )


# Tailwind background classes for a diff cell, keyed by (DiffKind value, side),
# tuned for a calm "editor" surface (slate base): removed (source-only) lines get
# a soft rose tint, added (target-only) lines a soft emerald tint, a changed line
# is gently tinted on both sides, and an unchanged line stays on the base surface.
_DIFF_CELL_BG: dict[tuple[str, str], str] = {
    (DiffKind.EQUAL.value, "left"): "",
    (DiffKind.EQUAL.value, "right"): "",
    (DiffKind.REPLACE.value, "left"): "bg-rose-50",
    (DiffKind.REPLACE.value, "right"): "bg-emerald-50",
    (DiffKind.DELETE.value, "left"): "bg-rose-100",
    (DiffKind.DELETE.value, "right"): "",
    (DiffKind.INSERT.value, "left"): "",
    (DiffKind.INSERT.value, "right"): "bg-emerald-100",
}


def _diff_cell_bg(kind: DiffKind, side: str) -> str:
    """Return the Tailwind background class for one diff cell."""
    return _DIFF_CELL_BG.get((kind.value, side), "")


def _render_diff_cell(
    ui: object, text: Optional[str], kind: DiffKind, side: str
) -> None:
    """Render one cell (one side of one diff row) with its highlight color."""
    bg = _diff_cell_bg(kind, side)
    # A non-breaking space keeps an empty cell's height equal to a text cell so
    # the two sides stay row-aligned.
    content = text if text else "\u00a0"
    # A subtle gutter divider on the left cell separates the two sides like an
    # editor's split view; calm slate text on the slate surface.
    divider = "border-r border-slate-200" if side == "left" else ""
    ui.label(content).classes(  # type: ignore[attr-defined]
        "w-1/2 min-w-0 px-3 py-0.5 font-mono text-xs leading-relaxed "
        f"text-slate-700 whitespace-pre-wrap break-all {divider} {bg}"
    )


def _render_copy_ddl_button(ui: object, text: str, *, label: str) -> None:
    """Render a small copy-to-clipboard icon button for a DDL block.

    ``label`` names what is copied (e.g. "Source DDL") so the confirmation toast
    and the button tooltip are specific. Mirrors the copy pattern used elsewhere
    (``ui.clipboard.write`` + a positive toast, with a graceful fallback when the
    browser clipboard is unavailable, e.g. non-HTTPS or denied permission).
    """

    def _copy() -> None:
        try:
            ui.clipboard.write(text)  # type: ignore[attr-defined]
            ui.notify(f"{label} copied.", type="positive", position="top")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - clipboard may be unavailable
            ui.notify(  # type: ignore[attr-defined]
                f"Select and copy the {label.lower()} from the block above.",
                type="info", position="top",
            )

    btn = ui.button(on_click=_copy).props(  # type: ignore[attr-defined]
        "flat dense round size=sm icon=content_copy color=grey-7"
    )
    btn.tooltip(f"Copy {label}")  # type: ignore[attr-defined]


def _render_ddl_diff(ui: object, source_ddl: str, target_ddl: str) -> None:
    """Render a side-by-side, change-highlighted Source vs Target DDL diff.

    Rows are aligned by :func:`diff_ddl_lines` and drawn as left/right cells in
    one row each (so the two sides stay vertically aligned). Removed source lines
    are red, added target lines are green, and changed lines are tinted on both
    sides, so the user sees exactly what the conversion changed (removed foreign
    keys, async indexes, remapped types).
    """
    rows = diff_ddl_lines(source_ddl, target_ddl)
    # An "editor"-style panel: one rounded, bordered surface in a calm slate tone
    # with a tab-like header bar naming each side, then the aligned diff lines in
    # a monospace split view (soft tints mark what the conversion changed).
    with ui.column().classes(  # type: ignore[attr-defined]
        "w-full gap-0 rounded-lg border border-slate-200 overflow-hidden bg-slate-50"
    ):
        with ui.row().classes(  # type: ignore[attr-defined]
            "w-full gap-0 no-wrap bg-slate-100 border-b border-slate-200"
        ):
            with ui.row().classes(  # type: ignore[attr-defined]
                "w-1/2 items-center gap-2 px-3 py-1.5 border-r border-slate-200 no-wrap"
            ):
                ui.icon("storage", color="blue-grey-5").classes("text-sm")  # type: ignore[attr-defined]
                ui.label("Source — MySQL").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold tracking-wide text-slate-500"
                )
                ui.space()  # type: ignore[attr-defined]
                _render_copy_ddl_button(ui, source_ddl, label="Source DDL")
            with ui.row().classes(  # type: ignore[attr-defined]
                "w-1/2 items-center gap-2 px-3 py-1.5 no-wrap"
            ):
                ui.icon("cloud_queue", color="blue-grey-5").classes("text-sm")  # type: ignore[attr-defined]
                ui.label("Target — Aurora DSQL").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold tracking-wide text-slate-500"
                )
                ui.space()  # type: ignore[attr-defined]
                _render_copy_ddl_button(ui, target_ddl, label="Target DDL")
        for row in rows:
            with ui.row().classes(  # type: ignore[attr-defined]
                "w-full gap-0 no-wrap items-stretch"
            ):
                _render_diff_cell(ui, row.left, row.kind, "left")
                _render_diff_cell(ui, row.right, row.kind, "right")


def _render_editable_target(
    ui: object,
    preview: DdlPreview,
    conv_state: SchemaConversionState,
    on_apply_object: Optional[Callable[[str], object]] = None,
    extra_actions: Optional[Callable[[], None]] = None,
    read_only_caption: Optional[str] = None,
) -> None:
    """Render the source/target DDL diff with an inline Edit toggle.

    The read-only view is a side-by-side, change-highlighted diff of the source
    DDL against the effective target DDL (the generated DDL, or the user's edit
    if any), so the user sees what the conversion changed before applying. The
    read-only/editor switch re-renders only a small local container (not the
    whole page), so toggling Edit/Done/Reset keeps the current expansion and page
    state intact. Edits are stored per object on :class:`SchemaConversionState`
    so apply uses exactly what the user approved. 'Apply to target' applies this
    single object's DDL; 'Reset to generated' discards the edit and re-renders
    inline.
    """
    editor_box = ui.column().classes("w-full gap-2")  # type: ignore[attr-defined]

    async def apply_click(button: object = None) -> None:
        if on_apply_object is None:
            return
        # Show the apply is running: disable the button and swap in a spinner +
        # "Applying…" label so a slow target round-trip (or a confirm dialog) never
        # looks like a dead click. Best-effort — a torn-down button must not raise.
        if button is not None:
            try:
                button.props("loading disable")  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - button slot already gone
                logger.debug("Apply button busy-state skipped: slot already rebuilt")
        try:
            await on_apply_object(preview.object_name)
        finally:
            if button is not None:
                try:
                    button.props(remove="loading disable")  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001 - button slot already gone
                    logger.debug("Apply button reset skipped: slot already rebuilt")
        # The apply handler may have already rebuilt this editor via a full
        # refresh (which deletes this slot), so re-render best-effort: a
        # torn-down editor_box must not raise "parent slot deleted".
        try:
            render(editing=False)
        except Exception:  # noqa: BLE001 - editor already rebuilt by the refresh
            logger.debug("Inline editor re-render skipped: slot already rebuilt")

    def reset_click() -> None:
        conv_state.clear_edited_target_ddl(preview.object_name)
        render(editing=False)

    def render(*, editing: bool) -> None:
        editor_box.clear()  # type: ignore[attr-defined]
        edited = conv_state.get_edited_target_ddl(preview.object_name)
        current = edited if edited is not None else preview.target_ddl
        applied = conv_state.get_apply_result(preview.object_name)
        with editor_box:  # type: ignore[attr-defined]
            if not editing:
                # Read-only view: side-by-side change-highlighted diff with an
                # Edit toggle. The caption is shown only here; in editing mode
                # the "Editing" badge suffices.
                if read_only_caption:
                    ui.label(read_only_caption).classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-500"
                    )
                _render_ddl_diff(ui, preview.source_ddl, current)
                ui.separator().classes("mt-1")  # type: ignore[attr-defined]
                with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                    ui.button(  # type: ignore[attr-defined]
                        "Edit", on_click=lambda: render(editing=True)
                    ).props("outline dense no-caps color=primary icon=edit")
                    if edited is not None:
                        ui.button(  # type: ignore[attr-defined]
                            "Reset to generated", on_click=reset_click
                        ).props("flat dense no-caps color=grey-7 icon=restart_alt")
                    if extra_actions is not None:
                        ui.space()  # type: ignore[attr-defined]
                        extra_actions()
                    ui.space()  # type: ignore[attr-defined]
                    if edited is not None:
                        ui.badge("Edited").props("color=amber-7")  # type: ignore[attr-defined]
                    if on_apply_object is not None:
                        _apply_btn = ui.button(  # type: ignore[attr-defined]
                            "Apply to target"
                        ).props("unelevated dense no-caps color=primary icon=cloud_upload")
                        _apply_btn.on_click(lambda _e=None, b=_apply_btn: apply_click(b))
            else:
                # Editing view (CodeMirror). Edits update the per-object buffer.
                def on_edit(event: object) -> None:
                    conv_state.set_edited_target_ddl(
                        preview.object_name, getattr(event, "value", "") or ""
                    )

                ui.codemirror(  # type: ignore[attr-defined]
                    current, language="SQL", line_wrapping=True, on_change=on_edit
                ).classes("w-full rounded-lg shadow-sm").style(
                    "max-height: 360px; border: 1px solid #e2e8f0"
                )
                with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                    ui.button(  # type: ignore[attr-defined]
                        "Done", on_click=lambda: render(editing=False)
                    ).props("outline dense no-caps color=primary icon=check")
                    ui.button(  # type: ignore[attr-defined]
                        "Reset to generated", on_click=reset_click
                    ).props("flat dense no-caps color=grey-7 icon=restart_alt")
                    if extra_actions is not None:
                        ui.space()  # type: ignore[attr-defined]
                        extra_actions()
                    ui.space()  # type: ignore[attr-defined]
                    ui.badge("Editing").props("color=amber-7")  # type: ignore[attr-defined]
                    if on_apply_object is not None:
                        _apply_btn_edit = ui.button(  # type: ignore[attr-defined]
                            "Apply to target"
                        ).props("unelevated dense no-caps color=primary icon=cloud_upload")
                        _apply_btn_edit.on_click(
                            lambda _e=None, b=_apply_btn_edit: apply_click(b)
                        )
                    if extra_actions is not None:
                        extra_actions()

            # Inline apply-result badge (set by the inline per-object Apply), so
            # the user sees the outcome without the page being rebuilt.
            if applied is not None:
                badge_color = {
                    ObjectApplyStatus.CREATED: "positive",
                    ObjectApplyStatus.SKIPPED: "warning",
                    ObjectApplyStatus.FAILED: "negative",
                }.get(applied.status, "grey")
                with ui.row().classes("items-center gap-2"):  # type: ignore[attr-defined]
                    ui.badge(f"Applied: {applied.status.value}").props(  # type: ignore[attr-defined]
                        f"color={badge_color}"
                    )
                    if applied.detail:
                        ui.label(applied.detail).classes(  # type: ignore[attr-defined]
                            "text-xs text-gray-600"
                        )

    # Always open in read-only mode; a prior edit is shown (with an "Edited"
    # badge) and the user clicks Edit to modify it again.
    render(editing=False)


# Quasar color names for each AI suggestion review status badge.
_AI_STATUS_COLORS: dict[str, str] = {
    AI_STATUS_PENDING_REVIEW: "primary",
    AI_STATUS_EDITED: "warning",
    AI_STATUS_APPROVED: "positive",
    AI_STATUS_REJECTED: "negative",
}

# DSQL constraints used to ground the AI prompt (design.md "AI-assisted
# Conversion Design"). Passed to the assistant seam so suggestions respect the
# target's limits.
_DSQL_CONSTRAINTS = (
    "Aurora DSQL constraints: foreign keys are unsupported, a primary key is "
    "required, secondary indexes are built asynchronously (CREATE INDEX ASYNC), "
    "the 'C' collation is used, and there are transaction limits (single DDL per "
    "transaction)."
)


def _render_generate_suggestion(
    ui: object,
    *,
    object_name: str,
    source_ddl: str,
    deterministic: str,
    conv_state: SchemaConversionState,
    assistant: Optional[AiConversionAssistant],
    refresh: Callable[[], None],
    on_ai_chat: Optional[Callable[[str, str, str], None]] = None,
) -> None:
    """Render the unified 'AI guidance' control that opens the chat drawer.

    Matches the Evaluation screen's AI button (flat, indigo ``auto_awesome``) and
    opens the SAME right chat drawer, scoped to converting THIS object, so the
    AI-assistance experience is identical across the app. The chat is advisory;
    its in-drawer "Use as target DDL" action pulls the latest reply's SQL into
    the editable target so it still flows into Apply. When AI is unavailable
    (disabled, or no opener wired) the button is shown disabled with a hint, so
    the affordance stays discoverable.
    """
    if assistant is None or on_ai_chat is None:
        disabled = ui.button("AI guidance")  # type: ignore[attr-defined]
        disabled.props("flat dense color=indigo-6 icon=auto_awesome")
        disabled.disable()
        disabled.tooltip(  # type: ignore[attr-defined]
            "Enable AI-assisted conversion on the Connect screen (toggle it on, "
            "set the Bedrock model, and re-test the connection), then reopen this "
            "step to chat about converting this object."
        )
        return

    ui.button(  # type: ignore[attr-defined]
        "AI guidance",
        on_click=lambda: on_ai_chat(object_name, source_ddl, deterministic),
    ).props("flat dense color=indigo-6 icon=auto_awesome").tooltip(
        "Open AI guidance for converting this object in the side panel."
    )


def _render_suggestion_review(
    ui: object,
    suggestion: AiConversionSuggestion,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
) -> None:
    """Render a suggestion with its provenance and approve/reject controls.

    Everything sits in one tinted panel: a header row (status badge + model
    provenance), the rationale rendered as Markdown (so bullets, bold, and
    inline code read cleanly instead of as one wall of text), the suggested SQL
    shown read-only like normal output, and the approve/reject actions. Only an
    explicitly approved suggestion is applied (Property 13).
    """
    with ui.column().classes(  # type: ignore[attr-defined]
        "w-full gap-2 p-3 rounded-lg border border-indigo-200 bg-indigo-50"
    ):
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("auto_awesome", color="indigo-6").classes("text-lg")  # type: ignore[attr-defined]
            ui.label("AI suggestion").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            ui.badge(suggestion.status).props(  # type: ignore[attr-defined]
                f"color={_AI_STATUS_COLORS.get(suggestion.status, 'grey')}"
            )
            ui.space()  # type: ignore[attr-defined]
            provenance = f"model: {suggestion.model_id}"
            if suggestion.confidence is not None:
                provenance += f"  \u00b7  confidence {suggestion.confidence:.2f}"
            ui.label(provenance).classes("text-xs text-gray-400")  # type: ignore[attr-defined]

        if suggestion.rationale:
            with ui.card().classes(  # type: ignore[attr-defined]
                "w-full bg-white !shadow-none border rounded"
            ):
                ui.label("Rationale & notes").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold text-gray-500"
                )
                ui.markdown(suggestion.rationale).classes("text-sm")  # type: ignore[attr-defined]

        ui.label("Suggested SQL / expression").classes(  # type: ignore[attr-defined]
            "text-sm text-gray-500"
        )
        ui.code(  # type: ignore[attr-defined]
            suggestion.suggested_sql_or_expr, language="sql"
        ).classes("w-full")

        if suggestion.approved_by_user:
            render_notice(
                ui,
                tone="success",
                header="Approved — will be applied on Run",
                body="Approved. This suggestion will be applied to the target on Run.",
            )
        else:
            render_notice(
                ui,
                tone="warning",
                header="Not approved — will be skipped",
                body=(
                    "Not approved. This suggestion will NOT be applied until you "
                    "approve it."
                ),
            )

        def on_approve() -> None:
            # Approve the suggestion as shown (read-only review; Property 13).
            conv_state.set_suggestion(approve_suggestion(suggestion))
            refresh()

        def on_reject() -> None:
            conv_state.set_suggestion(reject_suggestion(suggestion))
            refresh()

        with ui.row().classes("gap-2"):  # type: ignore[attr-defined]
            ui.button("Approve", on_click=on_approve).props(  # type: ignore[attr-defined]
                "color=positive"
            )
            ui.button("Reject", on_click=on_reject).props(  # type: ignore[attr-defined]
                "color=negative flat"
            )


def _render_apply_controls(
    ui: object,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
    *,
    on_apply_all: Optional[Callable[[], object]] = None,
    table_count: int = 0,
    in_progress: bool = False,
) -> None:
    """Render the apply-mode choice and the apply action (with REPLACE confirm).

    The apply action lives here, next to the setting it depends on (the
    SKIP/REPLACE mode), so the user configures and triggers the apply in one
    place instead of hunting for the step's Run button. ``on_apply_all`` runs the
    same selected-scope apply as Run (only the objects generated/selected above,
    plus approved AI suggestions); in REPLACE mode it opens an action-time
    confirmation dialog (so confirmation happens at the moment of applying, not
    via a sticky checkbox). ``table_count`` shows how many objects are in that
    apply scope; ``in_progress`` disables the button while an apply job is running.
    """
    ui.label("Apply to target").classes("text-lg font-semibold")  # type: ignore[attr-defined]
    ui.label(  # type: ignore[attr-defined]
        "Choose how to handle objects that already exist on the target, then "
        "apply all converted objects below, or apply a single object with its "
        "\"Apply to target\" button in the Generated DDL list above."
    ).classes("text-sm text-gray-500")

    def on_mode_change(event: object) -> None:
        value = getattr(event, "value", None)
        conv_state.apply_mode = (
            ApplyMode.REPLACE if value == ApplyMode.REPLACE.value
            else ApplyMode.SKIP_IF_EXISTS
        )
        if conv_state.apply_mode is not ApplyMode.REPLACE:
            conv_state.replace_confirmed = False
        refresh()

    ui.select(  # type: ignore[attr-defined]
        {
            ApplyMode.SKIP_IF_EXISTS.value: "Skip if the object already exists",
            ApplyMode.REPLACE.value: "Replace existing objects (destructive)",
        },
        value=conv_state.apply_mode.value,
        label="When an object already exists on the target",
        on_change=on_mode_change,
    ).classes("w-full max-w-md")

    if conv_state.apply_mode is ApplyMode.REPLACE:
        render_notice(
            ui,
            tone="error",
            header="REPLACE is destructive",
            body=(
                "REPLACE is destructive. When you apply, you'll confirm which "
                "existing target objects will be dropped and recreated."
            ),
        )

    if on_apply_all is not None:
        with ui.row().classes("items-center gap-3 w-full"):  # type: ignore[attr-defined]
            # While applying, keep the button LABEL visible (disabled "Applying…")
            # instead of Quasar's ``loading`` prop, which replaces the label with a
            # bare spinner (an empty-looking spinning button). The dedicated
            # progress panel below already shows the spinner and "(N of M)" count.
            apply_label = (
                "Applying…"
                if in_progress
                else f"Apply all to target ({table_count})"
            )
            apply_button = ui.button(  # type: ignore[attr-defined]
                apply_label,
                on_click=on_apply_all,
            ).props("unelevated no-caps color=primary icon=cloud_upload")
            if in_progress:
                apply_button.props("disable")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Applies the objects you generated/selected above (plus any approved "
            "AI suggestions) -- not the whole schema. Existing objects follow the "
            "rule selected here. You can also apply a single object with its "
            "\"Apply to target\" button in the Generated DDL list above."
        ).classes("text-xs text-gray-500")


def _render_apply_results(
    ui: object,
    results: Sequence[ObjectApplyResult],
    *,
    on_retry_failed: Optional[Callable[[], object]] = None,
    in_progress: bool = False,
) -> None:
    """Render the per-object apply results table (Requirement 10.7).

    When any object FAILED and ``on_retry_failed`` is provided, a "Retry failed"
    button re-applies just those objects (using their current/edited DDL), so a
    user can fix a failed object's DDL and retry without re-applying everything.
    The button is disabled while an apply job is running.
    """
    ui.label("Apply results").classes("text-lg font-semibold")  # type: ignore[attr-defined]
    summary = _summarize_apply(results)
    with ui.row().classes("items-center gap-3 w-full no-wrap"):  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            f"Summary — created: {summary[ObjectApplyStatus.CREATED]}, "
            f"skipped: {summary[ObjectApplyStatus.SKIPPED]}, "
            f"failed: {summary[ObjectApplyStatus.FAILED]}"
        ).classes("text-sm text-gray-600")
        failed_count = summary[ObjectApplyStatus.FAILED]
        if on_retry_failed is not None and failed_count:
            ui.space()  # type: ignore[attr-defined]
            retry_button = ui.button(  # type: ignore[attr-defined]
                f"Retry failed ({failed_count})", on_click=on_retry_failed
            ).props("color=negative outline dense")
            if in_progress:
                retry_button.props("disable")  # type: ignore[attr-defined]

    columns = [
        {"name": "object_name", "label": "Object", "field": "object_name", "align": "left"},
        {"name": "status", "label": "Result", "field": "status"},
        {"name": "detail", "label": "Detail", "field": "detail", "align": "left"},
    ]
    rows = [
        {
            "object_name": item.object_name,
            "status": item.status.value,
            "detail": item.detail,
        }
        for item in results
    ]
    ui.table(columns=columns, rows=rows, row_key="object_name").classes("w-full")  # type: ignore[attr-defined]


def _summarize_apply(
    results: Sequence[ObjectApplyResult],
) -> dict[ObjectApplyStatus, int]:
    """Count apply results per :class:`ObjectApplyStatus`."""
    summary = {status: 0 for status in ObjectApplyStatus}
    for item in results:
        summary[item.status] += 1
    return summary


class _InventoryExistenceChecker:
    """Existence checker backed by a previously browsed :class:`TargetInventory`.

    A lightweight adapter that answers ``object_exists`` from the in-memory
    target inventory (stored in the Evaluation result after Step 1 or a
    "Refresh target" action) without issuing any SQL. Case-insensitive
    matching mirrors PostgreSQL identifier folding.
    """

    def __init__(self, target_inventory: object) -> None:
        names: set[str] = set()
        for schema in getattr(target_inventory, "schemas", ()):
            for table in getattr(schema, "tables", ()):
                names.add(table.name.lower())
                names.add(f"{schema.name}.{table.name}".lower())
            for view in getattr(schema, "views", ()):
                names.add(view.name.lower())
                names.add(f"{schema.name}.{view.name}".lower())
        self._names = names

    def object_exists(self, object_name: str) -> bool:
        normalized = object_name.strip().lower()
        return normalized in self._names


def _existing_object_names(
    inventory: SourceInventory,
    existence_checker: Optional[TargetExistenceChecker],
) -> list[str]:
    """Return the source table/view names that already exist on the target.

    Returns an empty list when target introspection is unavailable, so the tree
    simply omits existence annotations rather than guessing.
    """
    if existence_checker is None:
        return []
    names: list[str] = []
    for table in inventory.tables:
        if existence_checker.object_exists(table.name):
            names.append(table.name)
    for view in inventory.views:
        if existence_checker.object_exists(view.name):
            names.append(view.name)
    return names


__all__ = [
    "ApplyMode",
    "ApplyOutcome",
    "ObjectApplyStatus",
    "ObjectApplyResult",
    "StatementApplyResult",
    "ObjectApplyError",
    "format_statement_summary",
    "ApplyObject",
    "TargetExistenceChecker",
    "SchemaApplier",
    "ApplierFactory",
    "TargetDdlExecutor",
    "OccRetryingSchemaApplier",
    "DsqlSchemaApplier",
    "default_applier_factory",
    "applied_table_conversions",
    "build_apply_objects",
    "override_apply_objects",
    "selected_object_names",
    "ddl_equivalent",
    "ai_candidate_object_names",
    "build_ai_apply_objects",
    "run_schema_apply",
    "replace_confirmation_message",
    "job_status_to_step_status",
    "build_object_tree",
    "DdlPreview",
    "DiffKind",
    "DiffRow",
    "diff_ddl_lines",
    "render_source_table_ddl",
    "render_target_ddl",
    "build_table_preview",
    "build_view_preview",
    "preview_for_selection",
    "split_sql_statements",
    "SchemaConversionState",
    "SchemaConversionStore",
    "build_schema_conversion_screen",
    "TABLE_PREFIX",
    "VIEW_PREFIX",
    "TRIGGER_PREFIX",
    "ROUTINE_PREFIX",
]
