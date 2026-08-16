# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""NiceGUI-agnostic schema-apply engine (extracted from ``schema_conversion.py``).

The apply contracts (ApplyMode/ApplyObject/SchemaApplier protocols), the
OCC-retrying orchestration (``run_schema_apply`` + ``build_apply_objects`` family),
the AI-assisted conversion-unit builders, and the DSQL applier adapter. Pure
domain/apply logic with NO NiceGUI dependency (heavily unit-tested); the screen
builder + render helpers stay in ``schema_conversion.py``, which re-exports these
names so every consumer/test import resolves unchanged. One-directional: this
module imports only stdlib + ``core.*`` + ``ui.ai_assist`` (never back from
``schema_conversion``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Mapping, Optional, Protocol, Sequence

from dsql_migrator.core.ai_assistant import validate_suggested_sql
from dsql_migrator.core.converter import (
    PrimaryKeyStrategy,
    SchemaConversionResult,
    SchemaConvertOptions,
    SchemaConverter,
    TableConversion,
    parse_target_primary_key,
)
from dsql_migrator.core.models import (
    AiConversionSuggestion,
    AssessmentReport,
    Classification,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.core.occ import (
    DEFAULT_BASE_DELAY_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    JitterFunc,
    SleepFunc,
    with_occ_retry,
)
from dsql_migrator.ui.ai_assist import approved_suggestions

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


def _apply_summary(results: Sequence[ObjectApplyResult]) -> tuple[bool, str]:
    """Roll a per-object apply result list into ``(any_failed, summary_detail)``.

    The run-level "schema apply completed" line: "N of M object(s) applied (C created,
    S skipped), F failed". ``any_failed`` drives the event's SUCCESS/FAILURE status so a
    run with any failure reads loud. Pure, so it is unit-tested without a screen harness.
    """
    created = sum(1 for r in results if r.status is ObjectApplyStatus.CREATED)
    skipped = sum(1 for r in results if r.status is ObjectApplyStatus.SKIPPED)
    failed = sum(1 for r in results if r.status is ObjectApplyStatus.FAILED)
    total = len(results)
    detail = (
        f"{created + skipped} of {total} object(s) applied "
        f"({created} created, {skipped} skipped), {failed} failed"
    )
    return failed > 0, detail


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


def build_identity_conversion(
    converter: SchemaConverter, table: TableDef
) -> TableConversion:
    """Per-table conversion for ``table`` under a server-generated identity key.

    Runs the converter with the IDENTITY_WITH_CACHE strategy; the auto-increment
    column becomes ``BIGINT ... GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)`` so
    DSQL fills the key server-side while spreading inserts across nodes (the cache
    hands each node its own value block, avoiding the monotonic-key hot partition).
    ``BY DEFAULT`` (not ``ALWAYS``) is deliberate: Full Load still inserts the
    source's own integer ids, and after the load the identity sequence is advanced
    past the max loaded value (see the Full Load engine's identity-sequence sync),
    so the app's subsequent inserts never collide. The rendered script is baked into
    ``edited_target_ddls`` -- the same field Full Load and Schema Apply consume -- so
    the choice is resume-safe (snapshotted) with no separate persisted state, exactly
    like the composite choice.
    """
    return converter.convert_table(
        table,
        SchemaConvertOptions(
            primary_key_strategy=PrimaryKeyStrategy.IDENTITY_WITH_CACHE,
        ),
    )


def identity_from_ddl(table: TableDef, target_ddl: str) -> bool:
    """Return whether the stored target DDL made the AUTO_INCREMENT key an identity.

    The picker keeps NO separate state: it infers the chosen strategy back out of the
    stored (edited) target DDL. This reports ``True`` when that DDL declares the
    table's auto-increment column as ``GENERATED ... AS IDENTITY`` (the
    IDENTITY_WITH_CACHE result), so the picker renders as "Server-generated
    (IDENTITY)". Returns ``False`` when there is no stored DDL, no auto-increment
    column, or the column is a plain key -- i.e. the DDL itself is the single source
    of truth for the picker's state (build -> render -> infer round-trips).

    Detection is a text scan of the column's clause, not a sqlglot parse, ON PURPOSE:
    the converter injects the identity as a raw ``GENERATED ... AS IDENTITY (CACHE
    65536)`` string precisely because sqlglot cannot render/parse the CACHE clause,
    so parsing this DDL would raise on exactly the string we need to detect.
    """
    column = table.auto_increment_column
    if not column or not target_ddl:
        return False
    clause = _column_clause(_first_create_table(target_ddl), column)
    if clause is None:
        return False
    upper = clause.upper()
    return "GENERATED" in upper and "AS IDENTITY" in upper


def _first_create_table(target_ddl: str) -> str:
    """Return the CREATE TABLE statement from a full target script (or the script).

    The stored DDL is the whole script (CREATE TABLE + CREATE INDEX ...), but the
    single-statement parsers here read one statement -- isolate the CREATE TABLE.
    """
    return next(
        (
            stmt
            for stmt in split_sql_statements(target_ddl)
            if stmt.strip().upper().split("(", 1)[0].startswith("CREATE TABLE")
        ),
        target_ddl,
    )


def _column_clause(create_table_ddl: str, column: str) -> Optional[str]:
    """Return the single column-definition clause for ``column`` from a CREATE TABLE.

    The converter pretty-prints one column per line, so the clause is the line whose
    first token is the (optionally double-quoted) column identifier. Matching on the
    leading token -- not a substring -- keeps a column merely NAMED like another from
    being confused for it. Returns ``None`` when no such column line is found.
    """
    for raw in create_table_ddl.splitlines():
        line = raw.strip().rstrip(",")
        token = line.split(" ", 1)[0]
        if token in (f'"{column}"', column):
            return line
    return None


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


def _dedupe_apply_objects(
    objects: Sequence[ApplyObject], ai_objects: Sequence[ApplyObject]
) -> list[ApplyObject]:
    """Merge deterministic + AI/edited apply units, deduped by ``object_name``.

    One object can yield BOTH a deterministic unit (e.g. an UNSUPPORTED ``skip_reason``
    placeholder) and an approved AI SCHEMA suggestion for the same name. Without dedupe the
    object is applied AND reported twice (a skip line plus a create line). The AI/edited unit
    wins and replaces the deterministic one IN PLACE -- insertion order is preserved, so the
    dependency-ordered position is kept -- and an AI unit for a name not already present is
    appended.
    """
    by_name: dict[str, ApplyObject] = {obj.object_name: obj for obj in objects}
    for ai_obj in ai_objects:
        by_name[ai_obj.object_name] = ai_obj
    return list(by_name.values())


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
# SQL statement splitting
# ---------------------------------------------------------------------------


def split_sql_statements(text: str) -> list[str]:
    """Split ``text`` into individual SQL statements on ``;`` (empties dropped).

    A single edited target-DDL script is split here so each statement is applied
    as one DDL in its own transaction (Property 2) by the apply path.
    """
    return [statement.strip() for statement in text.split(";") if statement.strip()]


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
