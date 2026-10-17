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
import re
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
    is_name_collision_placeholder,
    parse_target_primary_key,
    rename_index_statements,
    sql_without_comments,
    unkeyable_columns,
    views_in_dependency_order,
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


@dataclass(frozen=True)
class NotedApplyOutcome:
    """An :class:`ApplyOutcome` with something the user has to know about it, which an
    applier returns instead of the bare outcome (e.g. a view kept under the lower-case
    name an earlier version gave it). The note replaces the generic result detail."""

    outcome: ApplyOutcome
    note: str


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
        self,
        object_name: str,
        statements: Sequence[StatementApplyResult],
        notes: Sequence[str] = (),
    ) -> None:
        self.object_name = object_name
        self.statements: tuple[StatementApplyResult, ...] = tuple(statements)
        # Notes of the statements that did not fail (an earlier version's copy) still have
        # to reach the operator: the summary shows only failures' details.
        summary = format_statement_summary(statements)
        super().__init__(summary + "".join(f" {note}" for note in notes if note not in summary))


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
# parse_create_object's kind for a table, used to select the REPLACE's table targets.
_TABLE_KIND = "TABLE"


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
    ) -> "ApplyOutcome | NotedApplyOutcome":
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
    if is_name_collision_placeholder(table.target_ddl):
        # Its own reason, not the first UNSUPPORTED warning: a table with, say, a bytea key
        # too was told to change the key -- after which it was still skipped, for the name.
        return table.target_ddl.lstrip("-").strip()
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
        # Each view after the views it reads: in catalog (name) order a view over a later
        # view failed, and only a second apply created it.
        for view in views_in_dependency_order(
            result.views, tables=[table.table for table in result.tables]
        )
        if view.auto_converted
    )
    return objects


def _is_foreign_key_ddl(statement: str) -> bool:
    """Return True when ``statement`` is a preserved-foreign-key ALTER.

    A preserved foreign key is rendered as a post-load ``ALTER TABLE ... ADD
    CONSTRAINT ... FOREIGN KEY ... NOT VALID`` (NOT inlined in ``CREATE TABLE``),
    because the concurrent, cross-table bulk load has no parent-before-child
    ordering. Such a statement is applied by the post-load foreign-key pass, not
    at Schema Apply, so both apply paths -- the deterministic
    :func:`build_apply_objects` (which omits ``foreign_key_ddls``) and the edited
    :func:`override_apply_objects` -- exclude it; it is also the one non-``CREATE``
    statement the Schema Applier's CREATE-only parser would otherwise reject. The
    ``(`` split isolates the leading clause before the referencing column list,
    where both ``ALTER TABLE`` and ``FOREIGN KEY`` always appear (this is the
    single source of truth reused by :func:`_classify_edited_table_conversion`).
    """
    head = sql_without_comments(statement).strip().upper().split("(", 1)[0]
    return head.startswith("ALTER TABLE") and "FOREIGN KEY" in head


def edited_statements(script: str) -> list[str]:
    """The statements of an edited target-DDL script as they are applied: each without its
    comments, and a comment-only one dropped. A note typed above a statement made it one
    comment-led statement, which Schema Apply refused (it reads a CREATE) and Full Load
    filed under no kind -- falling back to the generated DDL without the edit's indexes."""
    return [
        clean
        for statement in split_sql_statements(script)
        if (clean := sql_without_comments(statement).strip())
    ]


# One identifier as the core applier's parser reads it (quoted with "" escapes, or bare), and
# a one- or two-part name of them. Never a shorter reading of a longer name: a bare part ends
# at its last name character, a quoted one at its real closing quote, and no further '.' may
# follow -- ``"shop" . "orders"`` and ``db.shop.orders``, which the applier reads as another
# name than PostgreSQL creates, name nothing here. A bare part is ASCII only: PostgreSQL folds
# only A-Z and the applier lower()s every letter, so ``shop.ordersÉ`` (PostgreSQL's
# "ordersÉ", the applier's "ordersé", which a REPLACE then dropped) names nothing either.
_IDENT_PART = r'(?:"(?:[^"]|"")+"(?!")|[A-Za-z_][A-Za-z0-9_$]*(?![\w$]))'
_RELATION_NAME = rf"{_IDENT_PART}(?:\.{_IDENT_PART})?(?!\s*\.)"
_OWN_CREATE_RE = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:RECURSIVE\s+(?=VIEW\b))?(TABLE|VIEW)\s+"
    rf"(?:IF\s+NOT\s+EXISTS\s+)?({_RELATION_NAME})",
    re.IGNORECASE,
)
_OWN_INDEX_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:ASYNC\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?:({_IDENT_PART})\s+)?ON\s+(?:ONLY\s+)?({_RELATION_NAME})",
    re.IGNORECASE,
)
_OWN_FOREIGN_KEY_RE = re.compile(
    rf"ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?({_RELATION_NAME})\s+ADD\s+"
    rf"(?:CONSTRAINT\s+({_IDENT_PART})\s+)?FOREIGN\s+KEY\b",
    re.IGNORECASE,
)
_OWN_SCHEMA_RE = re.compile(
    rf"CREATE\s+SCHEMA\s+(?:IF\s+NOT\s+EXISTS\s+)?({_IDENT_PART})(?!\s*\.)", re.IGNORECASE
)
_OWN_DROP_RE = re.compile(
    rf"DROP\s+(?:TABLE|VIEW)\s+(?:IF\s+EXISTS\s+)?({_RELATION_NAME})", re.IGNORECASE
)
# object_statement_role's role of a foreign-key ALTER.
_FOREIGN_KEY_ROLE = "FOREIGN KEY"


def _stored_parts(raw_name: str) -> "tuple[str, ...]":
    """A written name's parts as PostgreSQL stores them: quoted as written, unquoted folded,
    each cut to 63 bytes."""
    from dsql_migrator.core.converter import _chain_parts
    from dsql_migrator.core.target_introspector import pg_stored_name

    return tuple(pg_stored_name(part) for part in _chain_parts(raw_name)[0])


def _object_stored_parts(object_name: str) -> "tuple[str, str]":
    """``(schema, name)`` of a source object as the converter creates it: split on the first
    dot, an unqualified one in public (where Aurora DSQL's search_path puts it)."""
    from dsql_migrator.core.target_introspector import pg_stored_name

    schema, dot, name = object_name.partition(".")
    if not (dot and schema and name):
        schema, name = "public", object_name
    return pg_stored_name(schema), pg_stored_name(name)


def _names_object(raw_name: str, object_name: str) -> bool:
    """Whether a written relation name is ``object_name`` as PostgreSQL resolves it. A
    one-part name is public's: a bare ``CREATE TABLE orders`` for ``shop.orders`` creates
    ``public.orders``, while the load writes to ``"shop"."orders"``."""
    parts = _stored_parts(raw_name)
    schema, name = _object_stored_parts(object_name)
    if len(parts) == 2:
        return parts == (schema, name)
    return len(parts) == 1 and schema == "public" and parts[0] == name


def object_statement_role(statement: str, object_name: str) -> "Optional[tuple[str, str]]":
    """``(role, name)`` when ``statement`` is one of ``object_name``'s own: ``("TABLE" |
    "VIEW", "")`` for its CREATE, ``("INDEX", index name)`` for a CREATE INDEX on it,
    ``("FOREIGN KEY", constraint name)`` for a foreign-key ALTER of it -- a name as
    PostgreSQL stores it, ``""`` when the statement gives none. ``None`` for anything else
    (a SELECT, another object's statement, a CHECK added by ALTER). Comments are ignored."""
    clean = sql_without_comments(statement).strip()
    match = _OWN_CREATE_RE.match(clean)
    if match is not None:
        return (match.group(1).upper(), "") if _names_object(match.group(2), object_name) else None
    match = _OWN_INDEX_RE.match(clean)
    if match is not None:
        if not _names_object(match.group(2), object_name):
            return None
        return _INDEX_KIND, (_stored_parts(match.group(1))[0] if match.group(1) else "")
    match = _OWN_FOREIGN_KEY_RE.match(clean)
    # The same test the apply paths route a foreign key to the post-load pass by.
    if match is not None and _is_foreign_key_ddl(clean):
        if not _names_object(match.group(1), object_name):
            return None
        return _FOREIGN_KEY_ROLE, (_stored_parts(match.group(2))[0] if match.group(2) else "")
    return None


def object_create_kind(script: str, object_name: str) -> Optional[str]:
    """``"TABLE"`` / ``"VIEW"``: what ``script`` creates ``object_name`` as; ``None`` when no
    statement of it does (a not-converted note, a name-collision placeholder)."""
    for statement in edited_statements(script):
        role = object_statement_role(statement, object_name)
        if role is not None and role[0] in (_TABLE_KIND, _VIEW_KIND):
            return role[0]
    return None


def is_object_scaffolding(statement: str, object_name: str) -> bool:
    """Whether ``statement`` only prepares ``object_name``'s CREATE: the ``CREATE SCHEMA [IF
    NOT EXISTS]`` of its schema (a table's generated script has it, and Schema Apply puts the
    generated one first for an edit that has none -- :func:`override_apply_objects`) or a
    ``DROP`` of the object itself (an edit is applied in REPLACE mode, which drops it)."""
    clean = sql_without_comments(statement).strip()
    match = _OWN_SCHEMA_RE.match(clean)
    if match is not None:
        return _stored_parts(match.group(1)) == (_object_stored_parts(object_name)[0],)
    match = _OWN_DROP_RE.match(clean)
    return match is not None and _names_object(match.group(1), object_name)


_ALTER_OR_DROP_RE = re.compile(
    rf"(ALTER|DROP)\s+(TABLE|VIEW|INDEX)\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?({_RELATION_NAME})",
    re.IGNORECASE,
)
# What may stand between CREATE and the kind it creates: ``CREATE OR REPLACE FUNCTION``,
# ``CREATE UNIQUE INDEX``, ``CREATE TEMP TABLE``, ``CREATE CONSTRAINT TRIGGER``.
_CREATE_MODIFIERS = (
    r"(?:OR\s+REPLACE\s+)?"
    r"(?:(?:UNIQUE|GLOBAL|LOCAL|TEMP|TEMPORARY|UNLOGGED|MATERIALIZED|RECURSIVE|CONSTRAINT)\s+)*"
)
# Any written name, as a notice shows it (not only one the applier reads as PostgreSQL does).
_ANY_NAME_PART = r'(?:"(?:[^"]|"")+"|[^\W\d][\w$]*)'
_ANY_NAME = rf"{_ANY_NAME_PART}(?:\s*\.\s*{_ANY_NAME_PART})*"
_INDEX_LABEL_RE = re.compile(
    r"CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:ASYNC\s+|CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?:(?!ON\b)({_ANY_NAME})\s+)?ON\s+(?:ONLY\s+)?({_ANY_NAME})",
    re.IGNORECASE,
)
_CREATE_LABEL_RE = re.compile(
    rf"CREATE\s+{_CREATE_MODIFIERS}([A-Za-z]+)(?:\s+IF\s+NOT\s+EXISTS)?(?:\s+({_ANY_NAME}))?",
    re.IGNORECASE,
)


def statement_label(statement: str) -> str:
    """A statement named for a notice: ``CREATE TABLE shop.order_status`` for a CREATE (its
    modifiers left out: ``CREATE FUNCTION shop.touch`` for a CREATE OR REPLACE FUNCTION;
    ``CREATE INDEX ON shop.customers`` for an unnamed index), ``ALTER TABLE shop.orders`` for
    an ALTER or DROP, else its leading keyword (``SELECT``), with the next one for ``COMMENT
    ON`` and its kin. The first two words read 'CREATE OR' for any CREATE OR REPLACE, and
    the core parser takes an unnamed index's ON for its name."""
    clean = sql_without_comments(statement).strip()
    match = _INDEX_LABEL_RE.match(clean)
    if match is not None:
        if match.group(1):
            return f"CREATE INDEX {'.'.join(_stored_parts(match.group(1)))}"
        return f"CREATE INDEX ON {'.'.join(_stored_parts(match.group(2)))}"
    label, kind = _describe_ddl(clean)
    if kind is not None:
        return f"CREATE {label}"
    match = _CREATE_LABEL_RE.match(clean)
    if match is not None:
        name = ".".join(_stored_parts(match.group(2))) if match.group(2) else ""
        return f"CREATE {match.group(1).upper()} {name}".rstrip()
    match = _ALTER_OR_DROP_RE.match(clean)
    if match is not None:
        name = ".".join(_stored_parts(match.group(3)))
        return f"{match.group(1)} {match.group(2)} ".upper() + name
    words = clean.split()
    if not words:
        return "a comment"
    if words[0].upper() in ("ALTER", "CREATE", "DROP", "COMMENT") and len(words) > 1:
        return f"{words[0]} {words[1]}".upper()
    return words[0].upper()


# One token as PostgreSQL reads it: a quoted name, a string literal, a word, a number, a sign.
_SQL_TOKEN_RE = re.compile(
    r'"(?P<quoted>(?:[^"]|"")*)"|\'(?:[^\']|\'\')*\'|(?P<word>[^\W\d][\w$]*)|\d[\w.]*|\S'
)


def _key_definition(statement: str) -> "tuple[str, ...]":
    """What an index or foreign-key ALTER of an object creates, its name left out, token by
    token as PostgreSQL reads it (a quoted name as written, an unquoted word folded, comments
    and spacing dropped): UNIQUE or not and what follows ``ON <table>`` -- the key list, a
    predicate -- or what follows ``FOREIGN KEY`` -- the columns, REFERENCES, its actions."""
    from dsql_migrator.core.converter import fold_identifier

    clean = sql_without_comments(statement).strip()
    match = _OWN_INDEX_RE.match(clean)
    if match is not None:
        unique = re.match(r"CREATE\s+UNIQUE\b", clean, re.IGNORECASE) is not None
        head = ("UNIQUE" if unique else "INDEX",)
    else:
        match = _OWN_FOREIGN_KEY_RE.match(clean)
        if match is None:
            return ()
        head = ()
    return head + tuple(
        token.group("quoted").replace('""', '"') if token.group("quoted") is not None
        else fold_identifier(token.group(0)) if token.group("word") is not None
        else token.group(0)
        for token in _SQL_TOKEN_RE.finditer(clean, match.end())
    )


def merge_object_ddl(
    current: str, statements: Sequence[str], object_name: str
) -> "tuple[str, list[str]]":
    """``current`` -- an object's target-DDL script, edited or generated -- with
    ``statements``, the object's own from an AI reply, put in: each replaces its counterpart
    (its CREATE, the index or foreign key of the same name as PostgreSQL resolves it), one
    that creates what a statement of ``current`` already creates -- the same definition, name
    aside -- is that statement, one without a counterpart is added, and everything else of
    ``current`` is kept as written. So adopting a reply twice changes nothing: an unnamed
    index or foreign key matched nothing, and each click added another copy (two indexes, two
    constraints on one column), as did an unquoted restatement of a mixed-case name.

    Of two statements that create the same, the named one stands, and the current one when
    both are: a foreign key is paired with its metadata by its constraint name (the orphan
    check, a reload's pre-drop), which an unnamed or refolded restatement would lose.

    Returns the script and what was kept that the reply did not give (an index, a foreign
    key, another statement of the script; never its schema ensure) or gave under another
    name. A reply usually shows only the corrected CREATE TABLE: taken as the whole script,
    it dropped every secondary index and preserved foreign key of the table, which no apply
    path then created."""

    def identity(statement: str) -> "Optional[tuple]":
        # (kind, name, definition) of one of the object's statements, None for any other.
        role = object_statement_role(statement, object_name)
        if role is None:
            return None
        if role[0] in (_TABLE_KIND, _VIEW_KIND):
            return ("CREATE", "", ())
        return (*role, _key_definition(statement))

    pieces = split_sql_statements(current)
    identities = [identity(piece) for piece in pieces]
    names = {(i[0], i[1]) for i in identities if i is not None and i[1]}
    incoming: dict = {}
    for statement in statements:
        ident = identity(statement)
        if ident is None:
            continue
        kind, name, definition = ident
        # By name when the current DDL has its namesake, else by what it creates.
        key = (kind, name) if kind == "CREATE" or (kind, name) in names else (kind, definition)
        incoming.pop(key, None)  # the block's last one wins
        incoming[key] = (name, statement.strip())

    def kept_label(piece: str, ident: "Optional[tuple]") -> Optional[str]:
        if ident is not None and ident[0] in (_INDEX_KIND, _FOREIGN_KEY_ROLE):
            what = "index" if ident[0] == _INDEX_KIND else "foreign key"
            return f"{what} {ident[1]}" if ident[1] else f"an unnamed {what}"
        clean = sql_without_comments(piece).strip()
        if clean and not is_object_scaffolding(clean, object_name):
            return statement_label(clean)
        return None

    merged: list[str] = []
    kept: list[str] = []
    placed: set = set()
    for piece, ident in zip(pieces, identities):
        key, stands = None, False
        if ident is not None:
            kind, name, definition = ident
            if (kind, name) in incoming:
                key = (kind, name)
            elif (kind, definition) in incoming:
                key = (kind, definition)
                stands = bool(name) or not incoming[key][0]
        if key is None:
            merged.append(piece)
            said = True
        elif key in placed:
            continue  # a second copy of a replaced statement would create it twice
        else:
            merged.append(piece if stands else incoming[key][1])
            placed.add(key)
            # Kept under its own name where the reply names it otherwise: that is said.
            said = stands and bool(ident[1]) and bool(incoming[key][0])
        label = kept_label(piece, ident) if said else None
        if label:
            kept.append(label)
    created = {(i[0], i[2]) for i in map(identity, merged) if i is not None and i[0] != "CREATE"}
    for key, (_, statement) in incoming.items():
        kind, _, definition = identity(statement)
        if key in placed or (kind, definition) in created:
            continue  # placed, or what it creates is there already
        created.add((kind, definition))
        kinds = [ident[0] if ident else None for ident in map(identity, merged)]
        if key == ("CREATE", ""):
            # After the schema it is created in.
            at = next(
                (
                    i for i, existing in enumerate(merged)
                    if not is_object_scaffolding(existing, object_name)
                ),
                len(merged),
            )
        elif kind == _INDEX_KIND:
            # After the object's last index (or its CREATE); foreign keys come last.
            at = 1 + max(
                (i for i, k in enumerate(kinds) if k is not None and k != _FOREIGN_KEY_ROLE),
                default=len(merged) - 1,
            )
        else:
            at = len(merged)
        merged.insert(at, statement)
    from dsql_migrator.core.converter import terminated_sql_statement

    script = "\n\n".join(
        # A comment-only piece (a note at the end of an edit) stays as written.
        terminated_sql_statement(piece) if sql_without_comments(piece).strip() else piece
        for piece in merged
        if piece.strip()
    )
    return script, kept


def override_apply_objects(
    objects: Sequence[ApplyObject],
    edited_ddls: dict[str, str],
    *,
    conversion: Optional[SchemaConversionResult] = None,
) -> list[ApplyObject]:
    """Apply user edits to the deterministic apply units (Requirement 10.2/10.4).

    For each object with a user-edited target DDL in ``edited_ddls``, the edited
    script replaces the deterministic statements: it is split into single DDL
    statements so each is still applied in its own transaction (Property 2). A
    preserved foreign-key ``ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`` in
    the edited script is EXCLUDED here -- exactly as :func:`build_apply_objects`
    omits ``foreign_key_ddls`` -- because a foreign key must NOT exist during the
    concurrent, cross-table bulk load and is (re)created by the post-load
    foreign-key pass instead. That pass is driven by :func:`applied_table_conversions`,
    which re-parses the SAME edited script, so removing an FK line from the edit
    drops it and keeping one applies it post-load; passing the ``ALTER`` on to
    Schema Apply would only be rejected by the applier's CREATE-only parser. An
    object with no edit, or whose edit contains no applicable CREATE statement,
    keeps its deterministic DDL (so an accidental blank -- or an edit reduced to
    only its FK line -- never silently applies nothing). Order is preserved.

    ``conversion`` (the result ``objects`` came from) makes this agree with
    :func:`applied_table_conversions`, which Full Load uses: an edit stored before the
    converter renamed a table's colliding indexes is applied under the new names, and
    a stored edit never replaces the placeholder of tables whose names share a stored
    form (no target DDL can separate them).

    An edit with no ``CREATE SCHEMA`` of its own is applied after the object's generated
    schema ensure, as Full Load recreates an edited table: a view edit holds only its CREATE
    VIEW (an AI reply's schema ensure is not taken, a hand-typed one rarely has it), so a
    view in a schema with no tables failed with "schema does not exist".
    """
    tables = {table.table: table for table in conversion.tables} if conversion else {}
    renames_by_table = conversion.index_renames if conversion else {}
    overridden: list[ApplyObject] = []
    for obj in objects:
        edited = edited_ddls.get(obj.object_name)
        table = tables.get(obj.object_name)
        if table is not None and is_name_collision_placeholder(table.target_ddl):
            edited = None
        if edited is not None:
            ddls = tuple(
                stmt
                for stmt in edited_statements(edited)
                if not _is_foreign_key_ddl(stmt)
            )
            renames = renames_by_table.get(obj.object_name)
            if ddls and renames:
                ddls = tuple(rename_index_statements(ddls, renames))
            if ddls:
                if not any(_describe_ddl(ddl)[1] == _SCHEMA_KIND for ddl in ddls):
                    ddls = (
                        *(ddl for ddl in obj.ddls if _describe_ddl(ddl)[1] == _SCHEMA_KIND),
                        *ddls,
                    )
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


def composite_leading_candidates(
    table: TableDef, *, is_postgres: bool = False, sized: bool = True
) -> list[str]:
    """Columns eligible to lead a composite primary key for ``table``.

    A valid leading column is NOT NULL, not already part of the primary key, of a type
    Aurora DSQL can index, and small enough for the key-byte limit (mirrors
    :func:`validate_composite_leading_column` so the picker never offers a choice that
    would then be rejected: an interval or time with time zone leader made a PRIMARY KEY
    the CREATE TABLE was refused for, and a varchar(255) one a key over 1 KiB, which
    stored the refusal's comment as the table's script). Order follows the table's column
    order so the dropdown reads like the source schema. ``sized=False`` skips the size
    check, which converts the table: what the picker renders for every table (whether the
    tile applies at all), the check itself running only when the choice is made.
    """
    from dsql_migrator.core.converter import oversized_composite_leaders

    unkeyable = unkeyable_columns(table, is_postgres=is_postgres)
    names = [
        column.name
        for column in table.columns
        if not column.nullable
        and column.name not in table.primary_key
        and column.name not in unkeyable
    ]
    if not sized:
        return names
    oversized = oversized_composite_leaders(table, names, is_postgres=is_postgres)
    return [name for name in names if name not in oversized]


def default_composite_leading(table: TableDef, *, is_postgres: bool = False) -> Optional[str]:
    """A sensible default leading column, or ``None`` when none is eligible.

    Picks the first eligible NOT-NULL non-PK column so selecting "Composite key"
    always yields a representable (valid) state without forcing the user to also
    pick a column in the same click. The user can refine it via the dropdown.
    """
    candidates = composite_leading_candidates(table, is_postgres=is_postgres)
    return candidates[0] if candidates else None


def build_composite_conversion(
    converter: SchemaConverter,
    table: TableDef,
    leading: str,
    *,
    index_names: "Optional[Mapping[str, str]]" = None,
    size_checked: bool = True,
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
        index_names=index_names,
        # The notes of a key already stored (hand-written, or sized more laxly before)
        # are its own whatever the estimate says now; only a NEW pick is refused.
        composite_size_checked=size_checked,
    )


def build_identity_conversion(
    converter: SchemaConverter,
    table: TableDef,
    *,
    index_names: "Optional[Mapping[str, str]]" = None,
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
        index_names=index_names,
    )


def build_keep_conversion(
    converter: SchemaConverter,
    table: TableDef,
    *,
    index_names: "Optional[Mapping[str, str]]" = None,
) -> TableConversion:
    """Per-table conversion for ``table`` with the source key kept as a plain integer.

    Needed because "Keep source PK" can no longer be expressed by CLEARING the stored
    DDL: the converter's default is now IDENTITY_WITH_CACHE (it preserves the source's
    value generation), so for a table with a generated key the no-override path yields
    an identity. Keep therefore has to be stored like the other two choices, which also
    makes it resume-safe and round-trippable the same way (``identity_from_ddl`` reports
    False on this script).
    """
    return converter.convert_table(
        table,
        SchemaConvertOptions(
            primary_key_strategy=PrimaryKeyStrategy.KEEP_INTEGER,
        ),
        index_names=index_names,
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
    from dsql_migrator.core.converter import sql_without_comments

    return next(
        (
            stmt
            for stmt in split_sql_statements(target_ddl)
            if sql_without_comments(stmt).strip().upper().split("(", 1)[0].startswith("CREATE TABLE")
        ),
        target_ddl,
    )


def _column_clause(create_table_ddl: str, column: str) -> Optional[str]:
    """Return the single column-definition clause for ``column`` from a CREATE TABLE.

    The clause is the element of the column list -- split at its top-level commas, so a
    one-line or hand-laid-out CREATE reads like the converter's one-column-per-line one --
    whose leading identifier is the column (quoted, with ``""`` escapes and spaces, or
    bare). Matching on the leading identifier -- not a substring -- keeps a column merely
    NAMED like another from being confused for it. Returns ``None`` when no such column is
    found.
    """
    for element in _column_list_elements(create_table_ddl):
        if _leading_identifier(element) == column:
            return element
    return None


def _leading_identifier(element: str) -> Optional[str]:
    """The identifier an element of a column list starts with (unquoted as written)."""
    text = element.lstrip()
    if text.startswith('"'):
        end = 1
        while end < len(text):
            if text[end] == '"':
                if text[end + 1:end + 2] == '"':
                    end += 2
                    continue
                return text[1:end].replace('""', '"')
            end += 1
        return None
    token = text.split(None, 1)[0] if text.split() else ""
    return token or None


def _column_list_elements(create_table_ddl: str) -> "list[str]":
    """The comma-separated elements of a CREATE TABLE's parenthesised column list (column
    definitions and table constraints), comments dropped and whitespace collapsed -- the
    list's '(' found outside quoted names, strings and comments, and no comma inside
    parentheses, a string literal, a quoted identifier or a comment splitting it."""
    from dsql_migrator.core.converter import _sql_skip

    text = create_table_ddl or ""
    elements: list[str] = []
    current: list[str] = []
    depth = 0
    started = False
    i = 0
    while i < len(text):
        ch = text[i]
        skipped = _sql_skip(text, i) if ch in "'\"$-/eE" else None
        if skipped is not None:
            kind, end = skipped
            if started and kind != "comment":
                current.append(text[i:end])
            elif started:
                current.append(" ")
            i = end
            continue
        if not started:
            if ch == "(":
                started = True
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            if depth == 0:
                elements.append("".join(current))
                break
            depth -= 1
        elif ch == "," and depth == 0:
            elements.append("".join(current))
            current = []
            i += 1
            continue
        current.append(ch)
        i += 1
    return [" ".join(element.split()) for element in elements if element.strip()]


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
            for stmt in edited_statements(target_ddl)
            if stmt.upper().split("(", 1)[0].startswith("CREATE TABLE")
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
    (CREATE SCHEMA / CREATE TABLE / CREATE INDEX / ALTER TABLE ... ADD CONSTRAINT
    ... FOREIGN KEY) so the structured conversion mirrors what Schema Apply and the
    post-load foreign-key pass will apply. Anything missing falls back to the
    deterministic conversion (e.g. an edit that only changed the CREATE TABLE keeps
    the deterministic schema DDLs); indexes and foreign keys honor the edit exactly
    (removing one from the script drops it).
    """
    schema_ddls: list[str] = []
    create_ddl: Optional[str] = None
    index_ddls: list[str] = []
    foreign_key_ddls: list[str] = []
    for stmt in edited_statements(edited_script):
        head = stmt.upper().split("(", 1)[0]
        if head.startswith("CREATE SCHEMA"):
            schema_ddls.append(stmt)
        elif head.startswith("CREATE TABLE"):
            create_ddl = stmt
        elif head.startswith("CREATE") and "INDEX" in head:
            index_ddls.append(stmt)
        elif _is_foreign_key_ddl(stmt):
            # A preserved foreign key, applied as a post-load ADD CONSTRAINT
            # (excluded from Schema Apply by override_apply_objects -- same test).
            foreign_key_ddls.append(stmt)
    return TableConversion(
        table=deterministic.table,
        target_ddl=create_ddl or deterministic.target_ddl,
        schema_ddls=schema_ddls or list(deterministic.schema_ddls),
        index_ddls=index_ddls,
        foreign_key_ddls=foreign_key_ddls,
        preserved_foreign_keys=list(deterministic.preserved_foreign_keys),
        warnings=list(deterministic.warnings),
    )


def applied_table_conversions(
    result: SchemaConversionResult,
    edited_target_ddls: Mapping[str, str],
    *,
    preserve_foreign_keys: bool = True,
) -> dict[str, TableConversion]:
    """Per-table APPLIED conversion (honoring user edits), keyed by table name.

    For a table the user edited in Schema Conversion, the edited target DDL is
    parsed into a structured :class:`TableConversion`; otherwise the deterministic
    conversion is used unchanged. Full Load consumes this so it uses the SAME
    target schema the Schema Apply step applied -- driving value conversion off
    the applied column types and recreating a fresh-load target from the applied
    (not re-derived) DDL.

    ``preserve_foreign_keys`` is the single choke point for the Schema Conversion
    foreign-key toggle: when ``False`` the rendered FK DDL is dropped from EVERY
    conversion (whether it came from the deterministic result, a user edit, or a
    baked primary-key-strategy script), so Full Load never re-creates the foreign
    keys and referential integrity is left to the application. The FKs remain in
    ``preserved_foreign_keys`` as metadata (e.g. for the Validation orphan check).
    """
    conversions: dict[str, TableConversion] = {}
    for table in result.tables:
        edited = edited_target_ddls.get(table.table)
        if edited is not None and is_name_collision_placeholder(table.target_ddl):
            # No target DDL separates names that share a stored form (the load and CDC
            # write to the source name), so the placeholder stands.
            edited = None
        conv = (
            _classify_edited_table_conversion(table, edited)
            if edited is not None
            else table
        )
        renames = result.index_renames.get(table.table)
        if edited is not None and renames:
            # A script stored before the rename (a primary-key choice, an edit) still
            # names a colliding index by its source name; apply it under the new name.
            conv = conv.model_copy(
                update={"index_ddls": rename_index_statements(conv.index_ddls, renames)}
            )
        if not preserve_foreign_keys and conv.foreign_key_ddls:
            conv = conv.model_copy(update={"foreign_key_ddls": []})
        conversions[table.table] = conv
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

    Every other CREATE VIEW of an edit -- a helper view it creates -- is here too, under
    the name it creates (a source view's own DDL keeps its name): Schema Apply created it,
    so it blocks a table's DROP like any view. A view over a helper over the reloaded table
    was otherwise not selected (it reads no reloaded table itself), nothing was pre-dropped,
    and the reload's DROP TABLE was refused (2BP01).
    """
    from dsql_migrator.core.schema_applier import SchemaApplyError, parse_create_object

    def created_view(statement: str) -> Optional[str]:
        # The view a statement creates, as the reload's pre-drop reads it (one part: public's).
        try:
            name, kind = parse_create_object(statement)
        except SchemaApplyError:
            return None
        if kind != _VIEW_KIND:
            return None
        return name if "." in name else f"public.{name}"

    ddls: dict[str, str] = {}
    helpers: dict[str, str] = {}
    for view in result.views:
        if not view.auto_converted:
            continue
        edited = edited_target_ddls.get(view.view)
        if edited is None:
            ddls[view.view] = view.target_ddl
            continue
        # As Schema Apply runs them: the edit's CREATE VIEWs, without their comments (a note
        # above one made the reload's pre-drop and re-create refuse the view). The one that
        # creates THIS view is its DDL, never a helper view the edit creates first.
        views = [
            (stmt, name) for stmt in edited_statements(edited) if (name := created_view(stmt))
        ]
        own = [
            stmt for stmt, _ in views
            if object_statement_role(stmt, view.view) == (_VIEW_KIND, "")
        ]
        ddls[view.view] = (own or [stmt for stmt, _ in views] or [view.target_ddl])[-1]
        for stmt, name in views:
            if stmt != ddls[view.view]:
                helpers[name] = stmt
    for name, stmt in helpers.items():
        ddls.setdefault(name, stmt)
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


def replace_table_names(objects: Sequence[ApplyObject]) -> list[str]:
    """Return the names of the TABLES this apply set will recreate, in order. Pure.

    A REPLACE drops and recreates every selected object, so these are exactly the tables
    whose ``DROP TABLE`` an inbound foreign key can refuse. Feeds
    ``foreign_keys_blocking_replace`` to select the constraints to pre-drop -- the same
    selector the Full Load "drop & reload" path uses, so both paths agree on what is ours.
    Non-table statements (a view, an index, the schema scaffolding) are ignored.
    """
    from dsql_migrator.core.schema_applier import (
        SchemaApplyError,
        parse_create_object,
    )

    names: list[str] = []
    for obj in objects:
        for ddl in obj.ddls:
            try:
                # parse_create_object, NOT _describe_ddl: the latter returns a display
                # LABEL ("TABLE categories"), which would never match a conversion key.
                name, kind = parse_create_object(ddl)
            except SchemaApplyError:
                continue  # a placeholder / unparseable statement is not a table
            if kind == _TABLE_KIND and name and name not in names:
                names.append(name)
    return names


def predrop_owned_foreign_keys(
    pairs: "Sequence[tuple[str, str]]",
    drop_fk: "Callable[[str, str], None]",
    on_dropped: "Optional[Callable[[str, str], None]]" = None,
    on_failed: "Optional[Callable[[str, str, BaseException], None]]" = None,
) -> list[tuple[str, str]]:
    """Drop the ``(child_table, constraint)`` foreign keys blocking a REPLACE.

    The counterpart of :func:`_predrop_dependent_views` for foreign keys, and of the Full
    Load reload path's ``predrop_blocking_foreign_keys``. Schema Conversion's REPLACE
    pre-dropped only VIEWS, so a table referenced by a foreign key THIS MIGRATION had just
    created (via "Apply foreign keys") failed to recreate -- and the failure text told the
    operator the constraint was not the tool's to remove. The two destructive paths now
    behave the same way.

    ``pairs`` must already be narrowed to this migration's own constraints
    (``foreign_keys_blocking_replace``), so a hand-made constraint is never touched. Each
    drop is idempotent (``DROP CONSTRAINT IF EXISTS``) and best-effort: an error is
    swallowed so the per-object apply still surfaces the real failure rather than this
    pre-pass masking it. Returns the pairs it dropped, for the caller to report.
    """
    dropped: list[tuple[str, str]] = []
    for table_name, constraint_name in pairs:
        try:
            drop_fk(table_name, constraint_name)
        except Exception as exc:  # noqa: BLE001 - never fail the apply over the pre-pass
            # REPORT it. Swallowing it silently left the operator with the recreate failure
            # and a hint telling them to re-run -- which would fail again for the same
            # unreported reason, forever. The per-object apply still surfaces its own error;
            # this says WHY the pre-drop that should have prevented it did not happen.
            logger.warning(
                "Could not pre-drop foreign key %s.%s before the replace",
                table_name, constraint_name, exc_info=True,
            )
            if on_failed is not None:
                try:
                    on_failed(table_name, constraint_name, exc)
                except Exception:  # noqa: BLE001 - reporting must not break the pre-pass
                    pass
            continue
        dropped.append((table_name, constraint_name))
        if on_dropped is not None:
            try:
                on_dropped(table_name, constraint_name)
            except Exception:  # noqa: BLE001 - reporting must not break the pre-pass
                pass
    return dropped


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
    # Confirmed-REPLACE pre-pass for INBOUND FOREIGN KEYS, the sibling of the view pre-drop
    # below. It lives HERE, not at a call site, because run_schema_apply has two callers --
    # the bulk apply and the per-object "Apply to target" button -- and wiring it into only
    # the bulk one left the single-object REPLACE reproducing the exact failure this fixes
    # (an edited object forces REPLACE even in global SKIP mode). Injected rather than
    # imported so this module keeps no target-connection dependency and stays unit-testable.
    predrop_foreign_keys: Optional[Callable[[Sequence[ApplyObject]], None]] = None,
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
        # Foreign keys BEFORE views: a view blocking a table is independent of a constraint
        # blocking it, and doing the cheap ordered thing first keeps one pass per dependency
        # kind rather than interleaving them.
        if predrop_foreign_keys is not None:
            try:
                predrop_foreign_keys(objects)
            except Exception:  # noqa: BLE001 - advisory; per-object apply reports the truth
                logger.warning("Foreign-key pre-drop pass failed", exc_info=True)
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
                note = None
                if isinstance(outcome, NotedApplyOutcome):
                    outcome, note = outcome.outcome, outcome.note
                status = (
                    ObjectApplyStatus.CREATED
                    if outcome is ApplyOutcome.CREATED
                    else ObjectApplyStatus.SKIPPED
                )
                result = ObjectApplyResult(
                    object_name=obj.object_name,
                    status=status,
                    detail=note or _apply_success_detail(outcome, mode),
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


def replace_confirmation_message(
    existing_names: Sequence[str],
    foreign_keys: "Sequence[tuple[str, str]]" = (),
) -> str:
    """Build the body shown in the action-time REPLACE confirmation dialog.

    Lists the existing target objects a REPLACE will DROP and recreate so the
    user makes an informed decision at the moment of applying (Requirement 10.6 /
    Property 12), instead of a sticky checkbox set far from the action. When no
    existing object is known in scope (e.g. target introspection is unavailable)
    the message still warns that any existing object would be dropped.

    ``foreign_keys`` are the ``(child_table, constraint)`` constraints the apply will
    pre-drop to clear the way. They are disclosed because they sit on tables the operator
    did NOT select: the pre-drop happens on OTHER tables than the ones listed above, so
    without naming them the dialog understates what the confirmation authorises -- and
    nothing re-creates them afterwards (that is the explicit "Apply foreign keys" action).
    Defaulted, so a caller with nothing to disclose is unchanged.
    """
    if existing_names:
        listed = ", ".join(sorted(existing_names))
        body = (
            f"REPLACE will DROP and recreate these existing target objects: "
            f"{listed}. This is destructive and cannot be undone."
        )
    else:
        body = (
            "REPLACE will DROP and recreate any target object that already exists. "
            "This is destructive and cannot be undone."
        )
    if not foreign_keys:
        return body
    names = ", ".join(
        sorted(f"{table}.{constraint}" for table, constraint in foreign_keys)
    )
    one = len(list(foreign_keys)) == 1
    return (
        f"{body} It will also DROP the foreign "
        f"{'key' if one else 'keys'} {names} — on {'a table' if one else 'tables'} you "
        f"did not select — because {'it' if one else 'they'} would otherwise refuse the "
        f"recreate. Nothing re-creates {'it' if one else 'them'}: re-apply with "
        '"Apply foreign keys" after the next load.'
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
    """Split ``text`` into individual SQL statements on ``;`` (empties dropped), never on
    one inside a string literal, quoted identifier, dollar body or comment.

    A single edited target-DDL script is split here so each statement is applied
    as one DDL in its own transaction (Property 2) by the apply path.
    """
    from dsql_migrator.core.converter import sql_statement_spans

    return [
        text[start:end].strip()
        for start, end in sql_statement_spans(text)
        if text[start:end].strip()
    ]


# A line that starts a statement of its own. A WITH only as a CTE list (``WITH x AS (``),
# never ``WITH (fillfactor = 70)`` or ``WITH CHECK OPTION``; an ALTER / DROP / CREATE only of
# an object kind, never the ``ALTER COLUMN`` / ``DROP CONSTRAINT`` action of an ALTER TABLE.
_KIND_WORDS = (
    r"(?:TABLE|VIEW|INDEX|SCHEMA|SEQUENCE|FUNCTION|PROCEDURE|TRIGGER|TYPE|DOMAIN|EXTENSION"
    r"|ROLE|USER|POLICY|RULE|DATABASE)"
)
_CTE_HEAD = (
    rf"WITH\s+(?:RECURSIVE\s+)?{_ANY_NAME_PART}\s*(?:\(\s*\)\s*)?"
    r"AS\s*(?:(?:NOT\s+)?MATERIALIZED\s*)?\("
)
_STATEMENT_START_RE = re.compile(
    r"(?:SELECT|UPDATE|GRANT|EXPLAIN|INSERT\s+INTO|DELETE\s+FROM|COMMENT\s+ON"
    rf"|(?:CREATE\s+{_CREATE_MODIFIERS}|ALTER\s+|DROP\s+){_KIND_WORDS})(?![\w$])|{_CTE_HEAD}",
    re.IGNORECASE,
)
# The last word or sign of a statement that goes on with the next line's SELECT, INSERT, ...:
# a view's AS, a set operator, a rule's DO [INSTEAD], a MERGE's THEN, a trigger's BEFORE /
# OR events, FOR UPDATE, ON UPDATE of a foreign key, a list's comma.
_GOES_ON_RE = re.compile(
    r"(?:(?<![\w$])(?:AS|UNION|INTERSECT|EXCEPT|ALL|DISTINCT|DO|INSTEAD|ALSO|THEN|ELSE|BEFORE"
    r"|AFTER|OF|OR|AND|NOT|FOR|ON|IN|IS|BY|BEGIN|ATOMIC|EXPLAIN|ANALYZE|VERBOSE|GRANT)"
    r"|[,(=<>+\-*/|:])$",
    re.IGNORECASE,
)
# A head still waiting for what the next line starts: an ALTER's action (``ALTER TABLE t`` /
# ``ALTER type SET NOT NULL``, COLUMN left out), an INSERT's query, what an EXPLAIN explains.
_OPEN_HEAD_RE = re.compile(
    rf"ALTER\s+(?:MATERIALIZED\s+|FOREIGN\s+)?\w+\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?{_ANY_NAME}"
    rf"|INSERT\s+INTO\s+{_ANY_NAME}(?:\s+AS\s+{_ANY_NAME_PART})?(?:\s*\(\s*\))?"
    r"(?:\s+OVERRIDING\s+\w+\s+VALUE)?"
    r"|EXPLAIN(?:\s+(?:ANALYZE|VERBOSE))*(?:\s*\(\s*\))?",
    re.IGNORECASE,
)
_CTE_HEAD_RE = re.compile(rf"(?<![\w$]){_CTE_HEAD}", re.IGNORECASE)
_INDENT_RE = re.compile(r"[ \t]*")
# What a CTE list goes on to: once one follows it at its level, the list has its statement.
_CTE_STATEMENT_RE = re.compile(
    r"(?<![\w$])(?:SELECT|INSERT|UPDATE|DELETE|VALUES|TABLE|MERGE)(?![\w$])", re.IGNORECASE
)


def _statement_level(statement: str) -> "tuple[str, list[int]]":
    """``statement`` as read at its own level, every offset kept: a string or dollar literal
    one value (``0``), a comment blank, what is inside brackets blank; and where each line at
    that level starts."""
    from dsql_migrator.core.converter import _SQL_SPECIAL_RE, _sql_skip

    out: list[str] = []
    starts: list[int] = []
    depth = 0
    at = 0
    while at < len(statement):
        char = statement[at]
        skipped = _sql_skip(statement, at) if _SQL_SPECIAL_RE.match(statement, at) else None
        if skipped is not None:
            kind, end = skipped
            if depth or kind == "comment":
                out.append(" " * (end - at))
            elif kind == "quoted":
                out.append(statement[at:end].replace("\n", " "))
            else:
                out.append("0".ljust(end - at))
            at = end
            continue
        if char == "(":
            depth += 1
        out.append(char if depth == 0 or (depth == 1 and char in "()") or char == "\n" else " ")
        if char == ")":
            depth = max(depth - 1, 0)
        elif char == "\n" and depth == 0:
            starts.append(at + 1)
        at += 1
    return "".join(out), starts


def split_glued_statements(statement: str) -> list[str]:
    """``statement`` -- one ``;``-split statement of an AI reply -- cut where a line starts a
    statement of its own. A statement that lost its ``;`` ran into the next one, and the
    whole of it was taken as the object's CREATE: Schema Apply failed it (42601), and Full
    Load's applied DDL carried the verification SELECT or the side table's CREATE after it.

    A line is a cut only at the statement's own level (outside brackets, literals and
    comments), and only when what comes before it is whole: not a view's ``AS``, a set
    operator, the CTE list of the query that follows, an ALTER still without its action, and
    the like. A comment just above the line goes with it."""
    level, starts = _statement_level(statement)
    cuts: list[int] = []
    begin = 0
    for start in starts:
        head = _INDENT_RE.match(level, start).end()
        if not _STATEMENT_START_RE.match(level, head):
            continue
        before = level[begin:head].rstrip()
        if (
            not before.strip()
            or _GOES_ON_RE.search(before[-16:])
            or _OPEN_HEAD_RE.fullmatch(before.strip())
        ):
            continue
        ctes = list(_CTE_HEAD_RE.finditer(before))
        if ctes and not _CTE_STATEMENT_RE.search(before, ctes[-1].end()):
            continue  # the query of a CTE list
        cut = start
        while cut > begin:
            above = max(level.rfind("\n", begin, cut - 1) + 1, begin)
            if level[above:cut].strip():
                break
            cut = above
        cuts.append(cut)
        begin = cut
    bounds = [0, *cuts, len(statement)]
    return [statement[a:b].strip() for a, b in zip(bounds, bounds[1:]) if statement[a:b].strip()]


def _join_sql_statements(statements: Sequence[str]) -> str:
    """Join statements into one script: each ``;``-terminated, blank-line separated.

    Mirrors ``render_target_ddl``'s formatting so a split-then-join round-trips a
    rendered target script unchanged (modulo whitespace).
    """
    from dsql_migrator.core.converter import terminated_sql_statement

    return "\n\n".join(terminated_sql_statement(s) for s in statements if s.strip())


def split_target_ddl_fk_sections(script: str) -> tuple[str, str]:
    """Split a rendered target-DDL script into ``(non_fk, fk)`` sub-scripts.

    Separates the editable ``CREATE SCHEMA/TABLE/INDEX`` statements from the
    deferred post-load ``ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY`` statements
    so the UI can present the foreign keys in their own read-only "applied after
    Full Load" section instead of intermixed in the editable box (which reads as
    "Schema Apply will create these FKs" when it deliberately will not). Statement
    order within each bucket is preserved and each statement is ``;``-terminated;
    either sub-script may be empty (a table with no FKs -> empty ``fk``). This is a
    PRESENTATION split only: recombining with :func:`join_target_ddl_fk_sections`
    yields the same statement set, so the STORED edited DDL stays a full script and
    the apply path (which already excludes the FK ALTERs, see
    :func:`override_apply_objects`) is unaffected.
    """
    statements = split_sql_statements(script)
    fk = [stmt for stmt in statements if _is_foreign_key_ddl(stmt)]
    # Nothing to separate: return the script VERBATIM so a table with no foreign
    # keys (the common case) is displayed/edited exactly as before -- re-splitting
    # and re-joining would reflow whitespace and could mangle a trailing comment.
    if not fk:
        return script, ""
    non_fk = [stmt for stmt in statements if not _is_foreign_key_ddl(stmt)]
    return _join_sql_statements(non_fk), _join_sql_statements(fk)


def join_target_ddl_fk_sections(non_fk: str, fk: str) -> str:
    """Recombine an edited non-FK script with its read-only FK section into one
    stored target-DDL script (blank-line separated), so ``edited_target_ddls`` stays
    a full script and the deferred FKs still reach the post-load pass. The inverse
    of :func:`split_target_ddl_fk_sections`; an empty part is dropped.
    """
    from dsql_migrator.core.converter import terminated_sql_script

    parts = [part.strip() for part in (non_fk, fk) if part.strip()]
    if len(parts) == 2:
        # An edit without its final ';' would glue the first deferred FK ALTER onto it.
        parts[0] = terminated_sql_script(parts[0])
    return "\n\n".join(parts)


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
        self._converted_names: Optional[object] = None

    def set_converted_names(self, names: Optional[object]) -> None:
        """What the conversion emits (a core ``ConvertedNames``), passed to the core
        applier so a name an earlier version left on the target is told apart from the
        source's own object. Set before each apply run: a source refresh changes it."""
        self._converted_names = names
        self._sync_converted_names()

    def _ensure_core(self) -> object:
        if self._core is None:
            self._core = self._core_factory(self._target, self._aws_profile)
            self._sync_converted_names()
        return self._core

    def _sync_converted_names(self) -> None:
        setter = getattr(self._core, "set_converted_names", None)
        if callable(setter):
            setter(self._converted_names)

    def apply_object(
        self, object_name: str, ddls: Sequence[str], on_conflict: ApplyMode
    ) -> "ApplyOutcome | NotedApplyOutcome":
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
        REPLACE), so the core applier is called with ``confirmed=True``. A statement result
        with a ``note`` (an earlier version's copy kept under another name) makes the
        outcome a :class:`NotedApplyOutcome`.
        """
        from dsql_migrator.core.models import ApplyMode as _CoreMode, ApplyStatus

        core = self._ensure_core()
        core_mode = _CoreMode(on_conflict.value)

        statements: list[StatementApplyResult] = []
        notes: list[str] = []
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
            note = getattr(result, "note", "")
            if note:
                notes.append(_clean_detail(note))
            # A CREATE SCHEMA IF NOT EXISTS is always reported CREATED ("schema
            # ensured") even when the schema already existed, so it must not make
            # the object look CREATED. The verdict reflects the real object
            # (table/view) and its indexes only.
            if result.status is ApplyStatus.CREATED and kind != _SCHEMA_KIND:
                outcome = ApplyOutcome.CREATED

        if any(s.status is ObjectApplyStatus.FAILED for s in statements):
            raise ObjectApplyError(object_name, statements, notes)
        if notes:
            return NotedApplyOutcome(outcome, " ".join(notes))
        return outcome

    def drop(self, target_ddl: str) -> None:
        """Drop the object ``target_ddl`` defines via the core applier.

        Used by the confirmed-REPLACE pre-pass to drop a dependent view before
        the table it selects from is recreated (so the table DROP is not blocked
        by the view). Delegates to the core applier's ``drop`` (``DROP <kind> IF
        EXISTS`` with OC001 retry); the core applier is built lazily on first
        use, exactly as in :meth:`apply_object`.
        """
        self._ensure_core().drop(target_ddl)  # type: ignore[attr-defined]


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
