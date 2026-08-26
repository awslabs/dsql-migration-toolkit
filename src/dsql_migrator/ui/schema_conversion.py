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
import re
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Mapping, Optional, Protocol, Sequence

from dsql_migrator.core.ai_assistant import validate_suggested_sql
from dsql_migrator.core.assessment_strategist import (
    AssessmentStrategist,
    build_conversion_chat_system,
    build_reimplementation_chat_system,
)
from dsql_migrator.core.converter import (
    ConversionNoteKind,
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
from dsql_migrator.core.assessor import kind_label
from dsql_migrator.core.models import (
    AssessmentReport,
    Classification,
    ColumnDef,
    ObjectType,
    SourceInventory,
    SourceType,
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
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.ui.design import (
    CODE_HEADER_CLASSES,
    CODE_HEADER_LABEL_CLASSES,
    CODE_SURFACE_CLASSES,
    CODE_TEXT_CLASSES,
    inline_hint,
    radio_tiles,
    render_notice,
)
from dsql_migrator.ui.evaluation import EvaluationStore, classification_label
from dsql_migrator.ui.session import SessionStore
from dsql_migrator.ui.workflow import (
    WorkflowStep,
    get_status,
    with_status,
)

logger = logging.getLogger(__name__)

# The NiceGUI-agnostic apply engine was extracted to schema_conversion_apply.py for
# maintainability. Re-exported so this module's screen/render code, __all__, and every
# external/test import (`from dsql_migrator.ui.schema_conversion import run_schema_apply` ...)
# resolve unchanged.
from dsql_migrator.ui.schema_conversion_apply import (  # noqa: F401
    ApplierFactory,
    ApplyMode,
    ApplyObject,
    ApplyOutcome,
    CoreApplierFactory,
    DsqlSchemaApplier,
    ObjectApplyError,
    ObjectApplyResult,
    ObjectApplyStatus,
    OccRetryingSchemaApplier,
    SchemaApplier,
    StatementApplyResult,
    TargetDdlExecutor,
    TargetExistenceChecker,
    _INDEX_KIND,
    _SCHEMA_KIND,
    _VIEW_KIND,
    _apply_should_replace,
    _apply_success_detail,
    _apply_summary,
    _build_core_applier,
    _classify_edited_table_conversion,
    _clean_detail,
    _column_clause,
    _dedupe_apply_objects,
    _describe_ddl,
    _first_create_table,
    _is_applicable_target_ddl,
    _predrop_dependent_views,
    _safe_post_await_ui,
    _table_manual_reason,
    _tree_leaf_ids,
    _view_create_ddls,
    ai_candidate_object_names,
    applied_table_conversions,
    applied_view_ddls,
    apply_progress_text,
    build_ai_apply_objects,
    build_apply_objects,
    build_composite_conversion,
    build_identity_conversion,
    composite_leading_candidates,
    composite_leading_from_ddl,
    default_applier_factory,
    default_composite_leading,
    format_statement_summary,
    identity_from_ddl,
    job_status_to_step_status,
    override_apply_objects,
    replace_confirmation_message,
    run_schema_apply,
    schema_apply_is_complete,
    split_sql_statements,
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


def split_conversion_notes(
    notes: Sequence[ConversionWarning],
) -> tuple[list[ConversionWarning], list[ConversionWarning]]:
    """Split conversion notes into ``(losses, recommendations)``.

    ``Classification`` says how much WORK a note implies (MANUAL vs UNSUPPORTED) but
    not whether anything is actually wrong, so the two were conflated: a kept
    AUTO_INCREMENT key converts perfectly and works, yet it was listed under
    "Conversion warnings" with the same amber MANUAL badge as a removed foreign key.
    That presented throughput advice as a defect.

    Notes default to ``LOSS`` (what every note historically meant), so anything that
    does not explicitly opt into ``RECOMMENDATION`` -- including a payload
    deserialized from an older session snapshot -- keeps its current treatment. Pure.
    """
    losses = [
        n
        for n in notes
        if getattr(n, "kind", None) is not ConversionNoteKind.RECOMMENDATION
    ]
    recommendations = [
        n
        for n in notes
        if getattr(n, "kind", None) is ConversionNoteKind.RECOMMENDATION
    ]
    return losses, recommendations


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
            buckets[schema] = {
                "tables": [],
                "views": [],
                "triggers": [],
                # Routines are split by KIND, not lumped under one "Routines" heading:
                # Evaluation reports them as "Stored procedures" / "Functions" (its
                # KIND_LABELS), so a single "Routines (n)" node made the same objects
                # appear under a different name on the next screen -- reported from a
                # workshop, where attendees could not match one screen's list to the
                # other's. The introspector already distinguishes PROCEDURE from FUNCTION
                # (see ObjectType), so the tree was discarding information it had.
                "procedures": [],
                "functions": [],
                "routines": [],
            }
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
        # The node ID keeps ROUTINE_PREFIX regardless of kind: it is parsed back by
        # _OBJECT_NODE_PREFIXES (and the ticked/generated sets persist across renders), so
        # only the DISPLAY grouping changes here. An untyped routine (the introspector's
        # fallback when ROUTINE_TYPE is neither) still lands in the generic bucket rather
        # than being forced into one of the two named ones.
        key = {
            ObjectType.PROCEDURE: "procedures",
            ObjectType.FUNCTION: "functions",
        }.get(routine.object_type, "routines")
        bucket(schema)[key].append(_node(f"{ROUTINE_PREFIX}{routine.name}", obj))

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
        # Headings come from the assessor's KIND_LABELS -- the SAME mapping the Evaluation
        # report, the UI chart axis and the HTML export use. Hard-coding them here is how
        # "Routines (n)" drifted from Evaluation's "Stored procedures" / "Functions" in the
        # first place, so the label is looked up rather than restated.
        for key, kind in (
            ("procedures", "PROCEDURE"),
            ("functions", "FUNCTION"),
            ("routines", "ROUTINE"),
        ):
            if b[key]:
                categories.append(
                    _node(
                        f"category:{key}:{schema}",
                        f"{kind_label(kind)} ({len(b[key])})",
                        b[key],
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


def _render_source_default(column: ColumnDef) -> str:
    """Render a column's DEFAULT the way MySQL itself writes it.

    ``information_schema.COLUMN_DEFAULT`` stores the value UNQUOTED, so emitting it raw
    produced ``DEFAULT pending`` where MySQL's own ``SHOW CREATE TABLE`` says
    ``DEFAULT 'pending'``. That made the reconstruction invalid SQL -- and this pane has a
    "Copy Source DDL" button, so what it hands over could not be run: MySQL reads a bare
    ``pending`` as a column reference. Present since the initial release.

    ``default_is_expression`` is the same signal the converter keys on
    (``_column_default_sql``): it comes from ``EXTRA``'s ``DEFAULT_GENERATED`` flag, which
    is the only thing that can tell the literal string ``'CURRENT_TIMESTAMP'`` from the
    function call ``CURRENT_TIMESTAMP``. Guessing from the value's shape cannot, which is
    why the flag exists -- so an expression is emitted verbatim and everything else is
    quoted as a string literal.

    Numeric and boolean-ish literals are quoted too, deliberately: MySQL accepts
    ``DEFAULT '0'`` for an int column and prints defaults quoted in SHOW CREATE TABLE, so
    quoting is faithful and needs no per-type branch here (this is display text, never
    executed against the target -- the converter owns the target's typed default).
    """
    raw = column.default or ""
    if column.default_is_expression:
        return raw
    return "'" + raw.replace("'", "''") + "'"


def render_source_table_ddl(table: TableDef) -> str:
    """Reconstruct a readable MySQL ``CREATE TABLE`` for ``table`` (display only).

    This mirrors a ``SHOW CREATE TABLE`` view (columns, primary key, secondary
    indexes, and foreign keys) so the source side of the diff shows exactly what
    DSQL conversion changes (e.g. removed foreign keys, async indexes). It is
    rendered text only and is never executed.

    ``AUTO_INCREMENT`` and ``ON UPDATE CURRENT_TIMESTAMP`` are included because both are
    behaviour the target CANNOT reproduce, and the whole point of showing the two DDLs
    side by side is to make what changes visible. Omitting them made the diff claim
    nothing was lost, while Evaluation had already flagged both (its ``AUTO_INCREMENT``
    recommendation and ``ON_UPDATE_TIMESTAMP`` MANUAL rule) -- so an operator who came
    here to see the consequence of that finding could not locate it. Both need
    application changes: inserts must supply the key value, and ``updated_at`` must be
    written explicitly since DSQL has neither an ON UPDATE clause nor triggers.

    NOTE: this renderer is DISPLAY-ONLY and deliberately separate from
    ``converter._build_source_ddl``, whose output is *parsed* to produce the target. The
    clauses added here must never be added there: sqlglot transpiles MySQL
    ``AUTO_INCREMENT`` into ``GENERATED BY DEFAULT AS IDENTITY``, so emitting it on the
    conversion path would silently turn every default (``KEEP_INTEGER``) conversion into
    an identity column -- and an ``INT`` one, which Aurora DSQL rejects outright
    (identity must be BIGINT, and CACHE must be stated). That is why the two functions
    stay apart.
    """
    clauses: list[str] = []
    for column in table.columns:
        clause = f"  {_quote_mysql(column.name)} {column.mysql_type}"
        # COLLATE is shown because a case-INSENSITIVE collation is a behaviour change the
        # target cannot reproduce (DSQL/PostgreSQL compares case-sensitively), and the
        # assessor already reports it via its CI_COLLATION rule. Without it the diff hid
        # the one detail that silently changes query results after cut-over.
        if column.collation:
            clause += f" COLLATE {column.collation}"
        if not column.nullable:
            clause += " NOT NULL"
        if column.default is not None:
            clause += f" DEFAULT {_render_source_default(column)}"
        # MySQL's own SHOW CREATE TABLE order: DEFAULT then ON UPDATE, then the
        # AUTO_INCREMENT marker -- so the reconstruction reads like the real thing.
        if column.auto_update_timestamp:
            clause += " ON UPDATE CURRENT_TIMESTAMP"
        if table.auto_increment_column and column.name == table.auto_increment_column:
            clause += " AUTO_INCREMENT"
        # A generated column is marked, NOT reconstructed: introspection captures only the
        # boolean, never the ``GENERATED ALWAYS AS (<expr>)`` body. Emitting invented
        # syntax would make the source DDL lie in a new way, so this states the fact and
        # says the expression is not shown -- the assessor's GENERATED_COLUMN rule carries
        # the consequence (DSQL has no generated columns; the value must be computed by
        # the application or on read).
        if column.generated:
            clause += "  /* GENERATED column - expression not captured */"
        clauses.append(clause)

    if table.primary_key:
        pk_columns = ", ".join(_quote_mysql(name) for name in table.primary_key)
        clauses.append(f"  PRIMARY KEY ({pk_columns})")

    for index in table.indexes:
        unique = "UNIQUE " if index.unique else ""
        columns = ", ".join(_quote_mysql(name) for name in index.columns)
        # FULLTEXT / SPATIAL are index KINDS Aurora DSQL has no equivalent for, so an index
        # rendered as a plain KEY understated what conversion drops. BTREE is MySQL's
        # default and adds nothing, so it is left off to keep the diff readable.
        kind = (index.index_type or "").strip().upper()
        prefix = f"{kind} " if kind in ("FULLTEXT", "SPATIAL") else unique
        clauses.append(f"  {prefix}KEY {_quote_mysql(index.name)} ({columns})")

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
    rendered = f"CREATE TABLE {_quote_mysql(table.name)} (\n{body}\n)"
    # Native partitioning is table-level and, like a generated column, only the BOOLEAN is
    # captured -- the ``PARTITION BY`` clause itself is not. So it is noted rather than
    # reconstructed: DSQL has no user-visible partitioning (it partitions internally by
    # primary key), which the assessor reports via PARTITIONED_TABLE, and a diff that
    # showed no partitioning at all made a partitioned source look identical to a plain one.
    if table.partitioned:
        rendered += "\n/* PARTITION BY ... - partitioned source table; clause not captured */"
    return rendered


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
    """Render a readable MySQL ``CREATE VIEW`` for the source side of the diff.

    MySQL's ``SHOW CREATE VIEW`` returns the whole definition on ONE line, prefixed
    with server metadata (``ALGORITHM=``, ``DEFINER=``, ``SQL SECURITY``). Shown raw it
    was an unreadable wall of text -- and unusable in the side-by-side diff, where the
    target side is pretty-printed, so a one-line source could never align with it.

    So the definition is re-rendered with sqlglot in MySQL dialect (``pretty=True``).
    The ``ALGORITHM``/``DEFINER``/``SQL SECURITY`` prefix is stripped FIRST: it is
    server bookkeeping with no bearing on the conversion, and sqlglot mangles the
    ``DEFINER=`user`@`host`` backticks into double quotes when it round-trips them,
    which would show the user invalid MySQL. The original text is returned unchanged
    when it cannot be parsed: an unparseable definition is exactly the case where the
    user needs to see the source verbatim.
    """
    body = (view.definition or "").strip()
    if not body:
        return f"-- View definition unavailable for {view.name}."
    try:
        import sqlglot

        # Drop the server metadata between CREATE and VIEW (ALGORITHM=..., DEFINER=...,
        # SQL SECURITY ...) so what is shown is the view itself.
        stripped = re.sub(
            r"^CREATE\s+(?:ALGORITHM\s*=\s*\S+\s+|DEFINER\s*=\s*\S+\s+"
            r"|SQL\s+SECURITY\s+\w+\s+)+VIEW\b",
            "CREATE VIEW",
            body,
            count=1,
            flags=re.IGNORECASE,
        )
        # A definition that is only a SELECT body (some servers/introspection paths
        # return it without the CREATE prefix) must keep its CREATE VIEW header, or
        # the pretty-printed source would silently lose the object's identity.
        if not stripped.upper().startswith("CREATE"):
            stripped = f"CREATE VIEW {view.name} AS {stripped}"
        parsed = sqlglot.parse_one(stripped, read="mysql")
        if parsed is not None:
            pretty = parsed.sql(dialect="mysql", pretty=True).strip()
            if pretty:
                return pretty
    except Exception:  # noqa: BLE001 - unparseable: show the source verbatim
        logger.debug("View %s could not be pretty-printed; showing raw", view.name)
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
        # Which OBJECT-BROWSER tree nodes are expanded. The tree is rebuilt on every
        # render (Generate DDL, an apply, the progress poll), and NiceGUI's tree keeps
        # its open/closed state client-side -- so without restoring it the whole tree
        # snapped shut the moment the user pressed "Generate DDL for selected", hiding
        # the very tables they had just drilled into and ticked. Ticks were already
        # carried across; expansion was the missing half.
        self.expanded_node_ids: list[str] = []
        # Same, for the TARGET browser pane: it is what the operator compares against
        # while working, so a Generate/apply must not discard where they navigated to.
        self.target_expanded_node_ids: list[str] = []
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
        self._apply_results: Optional[list[ObjectApplyResult]] = None
        self._error: Optional[str] = None
        # Live apply progress, written by the background apply worker and read by
        # the UI poller (so guarded by the same lock as the results handoff):
        # ``_apply_total`` is the object count of the running apply, ``_apply_done``
        # how many have finished, and ``_apply_current`` the object being applied.
        self._apply_total: int = 0
        self._apply_done: int = 0
        self._apply_current: Optional[str] = None

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

# Quasar color names for each per-object apply status badge.
_APPLY_STATUS_COLORS: dict[ObjectApplyStatus, str] = {
    ObjectApplyStatus.CREATED: "positive",
    ObjectApplyStatus.SKIPPED: "grey",
    ObjectApplyStatus.FAILED: "negative",
}

# How often the screen polls the background apply job (seconds).
_POLL_INTERVAL_SECONDS = 0.5

# When a live CDC pipeline is streaming into the target, applying schema
# conversion is blocked (the sink is actively writing the target tables and
# Debezium does not propagate DDL, so a REPLACE would drop/corrupt what CDC is
# replicating). The message is single-sourced here so the persistent notice on
# the page and the on-Apply toast say the same thing and stay in sync.
CDC_APPLY_BLOCK_HEADER = "CDC is streaming to the target — the schema is already applied"
CDC_APPLY_BLOCK_BODY = (
    "A CDC pipeline is replicating live changes into the target right now, so "
    "its tables already exist. Applying conversion here — especially a REPLACE, "
    "which drops and recreates tables — would corrupt or truncate what the sink "
    "is writing (DDL is not replicated). Nothing needs converting: use \"Skip "
    "conversion & continue\" below to proceed. To change the schema, stop CDC "
    "in Data Migration first, then return here to apply."
)


def _cdc_apply_is_blocked(cdc_active_check: Optional[Callable[[], bool]]) -> bool:
    """Best-effort: True when a live CDC pipeline must block applying schema.

    Wraps the injected ``cdc_active_check`` probe so a status read can never
    break the UI: returns ``False`` when no probe is wired (tests / when the
    data-migration state is not connected) and ``False`` if the probe raises.
    Shared by the persistent page notice and the on-Apply toast so both agree.
    """
    if cdc_active_check is None:
        return False
    try:
        return bool(cdc_active_check())
    except Exception:  # noqa: BLE001 - a status probe must never break the page
        return False


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
    on_continue_to_data_migration: Optional[Callable[[], None]] = None,
    cdc_active_check: Optional[Callable[[], bool]] = None,
    open_ai_scope: Optional[Callable[..., object]] = None,
    ai_post_event: Optional[Callable[..., object]] = None,
    ai_tools: "Optional[Sequence[Mapping[str, object]]]" = None,
    ai_tool_execute: "Optional[Callable[[str, Mapping[str, object]], str]]" = None,
) -> tuple[Callable[[Callable[[], None]], None], Callable[[], None]]:
    """Build the Schema Conversion screen, returning ``(content_builder, runner)``.

    ``content_builder`` renders the SCT-like screen (object tree, DDL diff,
    SKIP/REPLACE choice, and per-object apply results) and is
    given the workflow shell's refresh callback. ``runner`` is invoked by the
    step's Run/Re-run button to apply the converted DDL to the target.

    The source inventory is taken from the Step 1 (Evaluation) result so the
    source is not re-introspected (Property 1). ``applier_factory`` builds the
    target :class:`SchemaApplier`; when it (or the target connection) is
    unavailable the runner surfaces a clear status instead of breaking. AI help
    is the per-object AI DBA chat (``open_ai_scope``): a per-warning icon and,
    for a not-auto-converted object, a per-object icon open a conversion chat
    whose "Use as target DDL" reply footer adopts a fix into the editable target
    -> Apply (no separate approve/reject suggestion surface). Both returns plug
    into :func:`~dsql_migrator.ui.workflow.build_workflow_sidebar`.
    """
    from nicegui import ui

    session = store.get_or_create(session_id)
    conv_state = conv_store.get_or_create(session_id)
    eval_state = eval_store.get_or_create(session_id)
    # Convert with the source engine's dialect (PostgreSQL vs the MySQL default);
    # source_config is None before Connect / on a resume, so fall back to MySQL. This one
    # construction feeds the whole memoized preview/apply/result path.
    _src_type = (
        session.source_config.source_type
        if session.source_config is not None
        else SourceType.MYSQL
    )
    schema_converter = converter or SchemaConverter(source_type=_src_type)

    # An INJECTED existence checker (tests) always wins and is never rebuilt. A DERIVED one
    # (built from the browsed target inventory) must be rebuilt when that snapshot changes --
    # a "Refresh target" replaces the inventory, and a checker bound once to the first
    # snapshot would answer existence from a stale catalog forever. ``_derived_checker_state``
    # remembers the snapshot the current derived checker was built from so it rebuilds only
    # on an actual change.
    existence_checker_injected = existence_checker is not None
    _derived_checker_state: dict[str, object] = {}

    # The target applier is reused across applies. A DsqlSchemaApplier browses the target
    # catalog once (lazily, on first apply) and caches it, so building a fresh one per inline
    # "Apply to target" click re-browses the whole catalog every time (N applies = N browses).
    # Cache it by target_config identity; a "Refresh target" clears it so a changed catalog is
    # re-browsed. (Snapshot staleness between applies matches the bulk-apply path, which also
    # browses once per run.)
    _applier_cache: dict[str, object] = {}

    def _inventory() -> Optional[SourceInventory]:
        result = eval_state.result
        return result.inventory if result is not None else None

    _conversion_cache: dict[str, object] = {}

    def _conversion(inventory: SourceInventory) -> SchemaConversionResult:
        """Deterministic conversion of ``inventory``, memoized per inventory identity.

        The conversion (sqlglot parse/transpile of every table + view) is deterministic for
        a given source inventory, but ``content`` re-renders on every action -- including the
        0.5s apply-progress poll -- and ``_all_apply_objects`` / ``_mark_schema_done_if_complete``
        both call this each render. Recomputing the full-schema conversion every render does
        not scale (thousands of tables). Cache it against the inventory object identity: a new
        Evaluation yields a new inventory -> ``is not`` miss -> recompute, and only the current
        inventory is ever held (one entry).
        """
        if _conversion_cache.get("inventory") is not inventory:
            _conversion_cache["inventory"] = inventory
            _conversion_cache["result"] = schema_converter.convert(
                inventory, SchemaConvertOptions()
            )
        return _conversion_cache["result"]  # type: ignore[return-value]

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
        # Reuse the applier for this target_config so a per-object inline apply does not
        # rebuild it (and re-browse the whole target catalog) on every click. Rebuilt only
        # when target_config identity changes; refresh_target clears the cache explicitly.
        if (
            _applier_cache.get("config") is not target_config
            or _applier_cache.get("applier") is None
        ):
            # Use the injected applier factory (tests) or the real DSQL-backed one
            # built for this session's global AWS profile.
            factory = applier_factory or default_applier_factory(session.aws_profile)
            _applier_cache["config"] = target_config
            _applier_cache["applier"] = factory(target_config)
        return (
            _applier_cache["applier"],  # type: ignore[return-value]
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
            # Bracket the per-object stream with a run-level start/summary so the
            # downloadable log has a "42 of 45 applied, 3 failed" roll-up (mirroring
            # Full Load's run started/completed), not just an unbracketed list of
            # per-object lines the reader must tally by hand.
            log_activity(
                ActivityCategory.SCHEMA_CONVERSION,
                "schema apply started" if not merge else "schema apply retry started",
                status=ActivityStatus.STARTED,
                detail=f"applying {len(objects)} object(s) to the target",
            )
            if ai_post_event is not None:
                ai_post_event(
                    text=f"Started applying schema to DSQL: {len(objects)} objects",
                    status="started",
                )

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

            any_failed, summary_detail = _apply_summary(results)
            log_activity(
                ActivityCategory.SCHEMA_CONVERSION,
                "schema apply completed",
                # A failed object already logged its own FAILURE line; the summary is
                # FAILURE when any object failed so the run-level verdict is loud too.
                status=ActivityStatus.FAILURE if any_failed else ActivityStatus.SUCCESS,
                detail=summary_detail,
            )
            if ai_post_event is not None:
                # A VISUAL apply summary (kind="apply" -> a Created/Skipped/Failed bar
                # in the panel, mirroring the Generate conversion event); the text is
                # the plain grounding fallback and names any failed objects.
                _apply_data = apply_event_data(results)
                ai_post_event(
                    text=apply_event_summary(_apply_data),
                    status="error" if any_failed else "success",
                    kind="apply",
                    data=_apply_data,
                )

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
        # One apply unit per object (the deterministic conversion, overlaid with any
        # user edit from the "Use as target DDL" chat action) restricted to the current
        # selection.
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
        CDC is live, block Apply and tell the operator what to do instead. Guarded
        by the injected ``cdc_active_check`` (best-effort; absent in tests / when
        the data-migration state is not wired, in which case apply is unaffected).
        The page already carries the persistent warning notice with the same
        guidance; this toast is the on-click echo for the operator who clicks Apply
        anyway, and it points to the two actionable paths (Skip / stop CDC).
        """
        active = _cdc_apply_is_blocked(cdc_active_check)
        if active:
            ui.notify(  # type: ignore[attr-defined]
                "CDC is streaming to the target — the schema is already applied. "
                "Applying it again could drop or corrupt the tables the sink is "
                "writing (DDL is not replicated). Use \"Skip conversion & "
                "continue\" to proceed, or stop CDC in Data Migration first to "
                "change the schema.",
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

            # When CDC is already streaming into the target, applying conversion is
            # blocked (the sink is writing the tables; DDL is not replicated). Make
            # the block a persistent, actionable notice at the top of the step --
            # not just the on-Apply toast -- because Data Migration (where CDC is
            # stopped) is prerequisite-locked behind this step, so the only path
            # forward is to Skip (the schema is already applied), which both
            # continues and unlocks Data Migration to stop CDC there if needed.
            cdc_active = _cdc_apply_is_blocked(cdc_active_check)
            with ui.card().classes("w-full"):
                if cdc_active:
                    render_notice(
                        ui,
                        tone="warning",
                        header=CDC_APPLY_BLOCK_HEADER,
                        body=CDC_APPLY_BLOCK_BODY,
                    )
                else:
                    render_notice(
                        ui,
                        tone="info",
                        header="Schema already prepared?",
                        body=(
                            "If the target tables already exist (conversion applied "
                            "earlier or out of band), skip this step to unlock Data "
                            "Migration. Data Migration loads only the tables that "
                            "have a target table, so not every source table needs "
                            "to exist."
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
            # Rebuild the DERIVED checker whenever the target snapshot changes (a refresh
            # replaces target_inventory); never touch an injected one. Binding only when
            # None left the checker pinned to the first snapshot, so existence verdicts went
            # stale after a "Refresh target".
            if (
                not existence_checker_injected
                and target_inventory is not None
                and _derived_checker_state.get("snapshot") is not target_inventory
            ):
                existence_checker = _InventoryExistenceChecker(target_inventory)
                _derived_checker_state["snapshot"] = target_inventory
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

            async def refresh_target(*, announce: bool = True) -> None:
                """Re-introspect the target DSQL catalog and refresh the tree.

                ``announce=False`` is used when this runs as a STEP of another action
                (Generate re-browses first, so the diffs' "exists on target" verdicts are
                current): there the "Target browser refreshed." toast and the extra
                re-render are noise, because the caller renders once it has committed its
                own state. The manual refresh button keeps both.
                """
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
                # The target catalog just changed, so drop the cached applier (its browsed
                # existence snapshot is now stale) -- the next apply rebuilds and re-browses.
                _applier_cache.clear()
                if announce:
                    ui.notify("Target browser refreshed.", type="positive")  # type: ignore[attr-defined]
                    refresh()

            # Shared AI chat drawer (same component/look as the Evaluation
            # screen), opened per object to chat about converting it. Advisory
            # only: the user reads/copies SQL and pastes it into the object's
            # editor (no auto-adopt, since a reply can contain several illustrative
            # SQL blocks that must not all be applied).
            # Per-object AI conversion help deep-links into the persistent app-wide
            # AI panel. None when the panel is not wired (open_ai_scope is None) or AI
            # is off, in which case the object row shows the disabled affordance.
            def open_conversion_chat(
                object_name: str,
                source_ddl: str,
                deterministic: str,
                note: "Optional[ConversionWarning]" = None,
            ) -> None:
                if open_ai_scope is None or not session.ai_assist.enabled:
                    return
                strategist = AssessmentStrategist(
                    session.ai_assist, aws_profile=session.aws_profile
                )
                system = build_conversion_chat_system(
                    object_name, source_ddl, deterministic
                )

                def _conversion_streamer(messages, on_delta):
                    # With the shared read-only tools, the chat can look up the real
                    # source structure / converted DDL / target schema and answer
                    # wider-migration questions; without them it stays a plain chat.
                    if ai_tools is not None and ai_tool_execute is not None:
                        return strategist.tool_chat(
                            system, messages, on_delta,
                            tools=ai_tools, execute=ai_tool_execute,
                        )
                    return strategist.stream_chat(system, messages, on_delta)

                # A per-NOTE icon passes its warning -> a distinct scope + a question
                # about THAT specific issue (mirrors Evaluation's per-finding chat); the
                # object-level fallback (note None) keeps the whole-object walkthrough.
                if note is not None:
                    scope_id = f"schema_conversion:{object_name}:{_note_scope_key(note)}"
                    subtitle = _note_subtitle(object_name, note)
                    seed = _conversion_note_question(object_name, note)
                else:
                    scope_id = f"schema_conversion:{object_name}"
                    subtitle = f"{object_name}"
                    seed = (
                        f"How should I convert {object_name} to Aurora DSQL? "
                        "Walk me through the DDL changes."
                    )

                # "Use as target DDL" footer: closes the advisory loop chat-natively.
                # When a reply contains a single fenced ```sql block, one click adopts
                # it as THIS object's edited target DDL (validated against the small
                # denylist first), so an AI fix flows into the editor -> Apply instead
                # of hand-copying. The footer button IS the explicit human approval
                # (Property 13); nothing is written to the target here -- Apply still does.
                def _footer_visible(md: str) -> bool:
                    from dsql_migrator.ui.query_playground import extract_sql_from_reply

                    return extract_sql_from_reply(md) is not None

                def _use_as_target_ddl(md: str) -> None:
                    from dsql_migrator.ui.query_playground import extract_sql_from_reply

                    sql = extract_sql_from_reply(md)
                    if not sql:
                        return
                    verdict = validate_suggested_sql(sql)
                    if not verdict.is_safe:
                        ui.notify(  # type: ignore[attr-defined]
                            f"Can't adopt this SQL: {verdict.reason}", type="warning"
                        )
                        return
                    conv_state.set_edited_target_ddl(object_name, sql)
                    ui.notify(  # type: ignore[attr-defined]
                        f"Set as {object_name}'s target DDL — review it and Apply.",
                        type="positive",
                    )
                    refresh()

                open_ai_scope(
                    scope_id=scope_id,
                    title="AI DBA",
                    subtitle=subtitle,
                    chip=f"Schema conversion · {object_name}",
                    seed_question=seed,
                    streamer=_conversion_streamer,
                    footer_label="Use as target DDL",
                    footer_visible=_footer_visible,
                    footer_action=_use_as_target_ddl,
                )

            # Opener for the "reimplement the unconvertible objects" chat, shown on
            # the banner listing the triggers/routines/events DSQL can't convert. It
            # deep-links AI DBA seeded to NAME each object (via list_unsupported_objects)
            # and give a per-kind reimplementation path -- turning the banner's bare
            # counts into actionable guidance. Requires the shared tools; when AI is off
            # (or the panel is not wired) no opener is passed and the banner has no button.
            def open_reimplementation_chat() -> None:
                if open_ai_scope is None or not session.ai_assist.enabled:
                    return
                strategist = AssessmentStrategist(
                    session.ai_assist, aws_profile=session.aws_profile
                )
                system = build_reimplementation_chat_system()

                def _reimpl_streamer(messages, on_delta):
                    if ai_tools is not None and ai_tool_execute is not None:
                        return strategist.tool_chat(
                            system, messages, on_delta,
                            tools=ai_tools, execute=ai_tool_execute,
                        )
                    return strategist.stream_chat(system, messages, on_delta)

                open_ai_scope(
                    scope_id="schema_conversion:reimplement",
                    title="AI DBA",
                    subtitle="Reimplement unconvertible objects",
                    chip="Schema conversion · unconvertible objects",
                    seed_question=(
                        "List every trigger, stored routine, and scheduled event in my "
                        "source that Aurora DSQL can't convert. For each one, give its "
                        "name, briefly explain what it does, and how to reimplement it "
                        "(application logic, or an external scheduler for events)."
                    ),
                    streamer=_reimpl_streamer,
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
                    on_apply_object=apply_object_confirmed,
                    on_refresh_source=refresh_source,
                    on_refresh_target=refresh_target,
                    on_sync_target=lambda: refresh_target(announce=False),
                    on_ai_chat=(
                        open_conversion_chat
                        if session.ai_assist.enabled
                        else None
                    ),
                    on_reimplement_chat=(
                        open_reimplementation_chat
                        if session.ai_assist.enabled
                        else None
                    ),
                    # Freeze the source selection while an apply is in flight: the
                    # worker already holds a fixed object list, so re-ticking cannot
                    # change what it writes and would only desynchronize the screen
                    # from the target.
                    apply_in_progress=status is StepStatus.IN_PROGRESS,
                    # Wire the AI activity feed so Generate posts its summary event
                    # (on_generate lives in this helper, not the builder scope).
                    ai_post_event=ai_post_event,
                    source_type=_src_type,
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


def generate_ddl_event_data(
    generated_node_ids: "Sequence[str]", inventory: SourceInventory
) -> dict:
    """Credential-free counts for the Generate-DDL AI activity event.

    ``converted`` = the table/view objects that got converted DDL; the trigger /
    stored-routine / scheduled-event counts are the source kinds Aurora DSQL cannot
    convert (the same ones the on-screen banner warns about). Pure + deterministic so
    the AI feed's Generate event (text AND its visual breakdown) is unit-testable.
    """
    tables = sum(1 for nid in generated_node_ids if nid.startswith(TABLE_PREFIX))
    views = sum(1 for nid in generated_node_ids if nid.startswith(VIEW_PREFIX))
    return {
        "converted": tables + views,
        "tables": tables,
        "views": views,
        "triggers": len(inventory.triggers),
        "routines": len(inventory.routines),
        "events": len(inventory.events),
    }


def generate_ddl_summary(data: dict) -> str:
    """One-line text summary from :func:`generate_ddl_event_data` (the AI's grounding
    fallback for the visual conversion event)."""
    converted = int(data.get("converted", 0) or 0)
    text = (
        f"Schema conversion generated target DDL for {converted} object"
        + ("" if converted == 1 else "s")
        + " — a preview to review; NOT applied to Aurora DSQL yet (Apply creates them "
        "on the target)"
    )
    unsupported = []
    if data.get("triggers"):
        unsupported.append(f"{data['triggers']} trigger(s)")
    if data.get("routines"):
        unsupported.append(f"{data['routines']} stored routine(s)")
    if data.get("events"):
        unsupported.append(f"{data['events']} event(s)")
    if unsupported:
        text += (
            ". Not converted — Aurora DSQL doesn't support "
            + ", ".join(unsupported)
            + " (reimplement that logic in the application)"
        )
    return text


def apply_event_data(results: "Sequence[ObjectApplyResult]") -> dict:
    """Credential-free counts for the schema-apply AI activity event.

    Mirrors :func:`generate_ddl_event_data`: created / skipped / failed / total, plus
    the NAMES of the objects that failed (bounded; schema-only, never row data) so the
    AI feed can say WHICH ones. Pure + deterministic so the visual apply event is
    unit-testable.
    """
    created = sum(1 for r in results if r.status is ObjectApplyStatus.CREATED)
    skipped = sum(1 for r in results if r.status is ObjectApplyStatus.SKIPPED)
    failed = [r.object_name for r in results if r.status is ObjectApplyStatus.FAILED]
    return {
        "created": created,
        "skipped": skipped,
        "failed": len(failed),
        "total": len(results),
        "failed_objects": failed[:20],
    }


def apply_event_summary(data: dict) -> str:
    """One-line text summary from :func:`apply_event_data` (the AI's grounding line)."""
    created = int(data.get("created", 0) or 0)
    skipped = int(data.get("skipped", 0) or 0)
    failed = int(data.get("failed", 0) or 0)
    total = int(data.get("total", 0) or 0)
    text = (
        f"Applied schema to Aurora DSQL: {created + skipped} of {total} object"
        + ("" if total == 1 else "s")
        + f" applied ({created} created, {skipped} skipped)"
    )
    if failed:
        names = list(data.get("failed_objects") or [])
        listed = ", ".join(names[:5])
        more = f" and {len(names) - 5} more" if len(names) > 5 else ""
        text += f", {failed} FAILED"
        if listed:
            text += f" ({listed}{more})"
    return text


async def generate_selected_ddl(
    conv_state: "SchemaConversionState",
    refresh: Callable[[], None],
    *,
    sync_target: Optional[Callable[[], object]] = None,
) -> None:
    """Re-read the target catalog, then commit the ticked objects as the DDL scope.

    Backs the "Generate DDL for selected" button. The re-read is the fix for a stale
    verdict: each diff's "already exists on target" comes from the cached
    ``TargetInventory`` snapshot (:class:`_InventoryExistenceChecker` answers from memory
    and issues no SQL), and that snapshot is only filled by Evaluation's browse or the
    manual "Refresh target" button. So a target emptied since then still warned "'x'
    already exists on the target. Choose SKIP ... or REPLACE (destructive)" about objects
    that were gone, pushing the user toward a destructive choice for nothing. The reverse
    is worse: an object created since the snapshot drew NO warning, and the user met an
    unexpected SKIP.

    The re-read is read-only (``information_schema`` SELECTs) and happens off the UI
    thread inside ``sync_target``, so the freshest catalog backs every diff without the
    user having to remember a refresh step first -- inferring what can be inferred rather
    than asking.

    A refresh failure is deliberately NOT fatal: the generation still proceeds, because
    the DDL diff is the point and a stale existence verdict cannot misroute the actual
    apply (``_build_core_applier`` browses the target again and ``apply()`` decides from
    that live read).
    """
    if sync_target is not None:
        try:
            outcome = sync_target()
            if inspect.isawaitable(outcome):
                await outcome
        except Exception:  # noqa: BLE001 - advisory; never block generation
            logger.exception(
                "Target re-browse before Generate failed; "
                "using the cached target snapshot"
            )
    conv_state.generated_node_ids = list(conv_state.ticked_node_ids)
    refresh()


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
    on_apply_object: Optional[Callable[[str], None]] = None,
    on_refresh_source: Optional[Callable[[], object]] = None,
    on_refresh_target: Optional[Callable[[], object]] = None,
    # Re-read the target catalog as a silent STEP of Generate (no toast, no extra
    # re-render) so each diff's "exists on target" verdict is current. Separate from
    # on_refresh_target, which is the user-facing button and announces itself.
    on_sync_target: Optional[Callable[[], object]] = None,
    on_ai_chat: Optional[Callable[..., None]] = None,
    on_reimplement_chat: Optional[Callable[[], None]] = None,
    apply_in_progress: bool = False,
    ai_post_event: Optional[Callable[..., object]] = None,
    source_type: SourceType = SourceType.MYSQL,
) -> None:
    """Render side-by-side source/target browsers and the selected DDL diff.

    The two object browsers sit left (source MySQL) and right (target DSQL),
    each inside a fixed-height scroll area so an expanded tree never stretches
    the page. Selecting a source table/view shows its source DDL and the
    converted target DDL in a left/right comparison below. ``result_provider``
    lazily computes the deterministic conversion; it is invoked only when the
    user has generated DDL, so merely opening the screen runs no conversion.

    ``apply_in_progress`` freezes the source selection while a schema apply is
    running. The apply worker was handed a fixed object list when it started, so
    re-ticking mid-run cannot change what it does -- it would only desynchronize what
    the screen shows from what is actually being written to the target, and a
    "Generate DDL" during the run could swap the DDL under the in-flight apply. The
    tree, the bulk buttons and the filter are all disabled with an explanatory note.
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
            # Header row: title on the left, the bulk-selection actions and the
            # refresh next to it on the right. Keeping Select all / Unselect all up
            # here (rather than on their own row below) is what lets the source and
            # target panels start their filter box at the SAME y-position -- the two
            # sides read as one comparison instead of being visibly offset.
            def _sc_select_all() -> None:
                leaf_ids = _tree_leaf_ids(source_nodes)
                conv_state.ticked_node_ids = leaf_ids
                tree.tick(leaf_ids)

            def _sc_unselect_all() -> None:
                conv_state.ticked_node_ids = []
                tree.untick()

            with ui.row().classes("items-center justify-between w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label("Source (MySQL)").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-blue-800"
                )
                with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                    # Bulk selection: tick/untick every selectable object leaf at once
                    # (the per-object ticks still work for fine-grained picks).
                    # Programmatic tick/untick does not fire on_tick, so
                    # ticked_node_ids is kept in sync in the handlers above.
                    select_all_btn = ui.button(  # type: ignore[attr-defined]
                        "Select all", on_click=_sc_select_all
                    ).props("flat dense no-caps size=sm color=primary icon=done_all")
                    unselect_all_btn = ui.button(  # type: ignore[attr-defined]
                        "Unselect all", on_click=_sc_unselect_all
                    ).props("flat dense no-caps size=sm color=grey-7 icon=remove_done")
                    if apply_in_progress:
                        for _btn in (select_all_btn, unselect_all_btn):
                            _btn.props("disable")
                            _btn.tooltip(
                                "The selection is locked while the apply is running."
                            )
                    if on_refresh_source is not None:
                        refresh_btn = ui.button(on_click=on_refresh_source).props(  # type: ignore[attr-defined]
                            "flat dense round size=sm icon=refresh"
                        )
                        if apply_in_progress:
                            refresh_btn.props("disable")
                            refresh_btn.tooltip(
                                "Re-introspecting would change the objects the "
                                "running apply was started with."
                            )
                        else:
                            refresh_btn.tooltip("Refresh source objects")
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
            if apply_in_progress:
                src_filter.props("disable")
            with ui.scroll_area().classes(  # type: ignore[attr-defined]
                "w-full bg-white rounded-md border border-gray-200"
            ).style("height: 340px"):

                def on_tick(event: object) -> None:
                    value = getattr(event, "value", None)
                    conv_state.ticked_node_ids = list(value) if value else []

                def on_expand(event: object) -> None:
                    # Record the open nodes so the next render can restore them. The
                    # tree is rebuilt on Generate / apply / poll, and its open state
                    # lives client-side, so anything not restored here collapses.
                    value = getattr(event, "value", None)
                    conv_state.expanded_node_ids = list(value) if value else []

                tree = ui.tree(  # type: ignore[attr-defined]
                    source_nodes,
                    label_key="label",
                    node_key="id",
                    tick_strategy="leaf",
                    on_tick=on_tick,
                    on_expand=on_expand,
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
                # Restore the OPEN nodes for the same reason. Pressing "Generate DDL for
                # selected" re-renders the screen, so without this the tree snapped back
                # to fully collapsed and hid the tables the user had just drilled into --
                # exactly the rows they were working with.
                if conv_state.expanded_node_ids:
                    tree.expand(list(conv_state.expanded_node_ids))
                if apply_in_progress:
                    # Freeze the selection: the apply worker was handed a fixed
                    # object list at start, so re-ticking cannot change what it
                    # writes -- it would only desynchronize the screen from the
                    # target.
                    #
                    # Quasar's q-tree has NO `disable` prop, so props("disable") is
                    # silently ignored and the tree stays fully clickable. Block it
                    # the way the Data Migration table picker already does:
                    # pointer-events-none stops the clicks, and the dimmed tree reads
                    # as "locked" rather than merely dead.
                    tree.classes("pointer-events-none opacity-70")  # type: ignore[attr-defined]
            # Legend for the per-table primary-key indicator shown beside each table
            # leaf (matches the Step 3 "Tables to migrate" browser). Only tables carry
            # it; views/triggers/routines have no PK concept. Kept BELOW the tree so
            # the source and target panels start their trees at the same y-position --
            # above the tree it pushed the source list down and the two sides no longer
            # lined up.
            with ui.row().classes(  # type: ignore[attr-defined]
                "items-center gap-3 w-full text-xs text-gray-500"
            ):
                with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon("check_circle", color="green-6").classes("text-sm")  # type: ignore[attr-defined]
                    ui.label("Table has a primary key")  # type: ignore[attr-defined]
                with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon("warning", color="amber-7").classes("text-sm")  # type: ignore[attr-defined]
                    ui.label("No primary key (required for Aurora DSQL)")  # type: ignore[attr-defined]
            if apply_in_progress:
                inline_hint(
                    ui,
                    "Selection is locked while the schema apply runs — it was "
                    "started with the objects listed above.",
                    tone="neutral",
                )

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

                    def on_target_expand(event: object) -> None:
                        value = getattr(event, "value", None)
                        conv_state.target_expanded_node_ids = (
                            list(value) if value else []
                        )

                    tgt_tree = ui.tree(  # type: ignore[attr-defined]
                        target_nodes,
                        label_key="label",
                        node_key="id",
                        on_expand=on_target_expand,
                    )
                    tgt_tree.props("no-connectors")  # type: ignore[attr-defined]
                    tgt_filter.bind_value_to(tgt_tree, "filter")  # type: ignore[attr-defined]
                    # Same rebuild-collapses-it problem as the source tree: this pane is
                    # for comparing against the target while working, so a Generate or an
                    # apply must not throw away where the operator had navigated to.
                    if conv_state.target_expanded_node_ids:
                        tgt_tree.expand(  # type: ignore[attr-defined]
                            list(conv_state.target_expanded_node_ids)
                        )
                else:
                    ui.label(  # type: ignore[attr-defined]
                        "No target objects to browse yet. Run Step 1 "
                        "(Evaluation) to introspect the target catalog."
                    ).classes("text-sm text-gray-500")

    # --- Generate DDL for the ticked objects ------------------------------
    async def on_generate() -> None:
        await generate_selected_ddl(
            conv_state, refresh, sync_target=on_sync_target
        )
        # Mirror the generate action into the AI activity feed with a real summary:
        # how many objects got converted DDL AND which source-object kinds Aurora DSQL
        # cannot convert (the same triggers/routines/events the on-screen banner warns
        # about) -- so the assistant reflects the full outcome, not just a count. The
        # assistant can then be asked about it via its tools (list_converted_tables /
        # get_converted_ddl).
        if ai_post_event is not None and conv_state.generated_node_ids is not None:
            # ``inventory`` is this helper's own param (always present). Post a VISUAL
            # conversion event (kind="conversion" -> a Converted/Not-supported bar in
            # the panel); the text is the plain grounding fallback.
            _data = generate_ddl_event_data(
                conv_state.generated_node_ids, inventory
            )
            ai_post_event(
                text=generate_ddl_summary(_data),
                status="success",
                kind="conversion",
                data=_data,
            )
            # The unconvertible triggers/routines/events are surfaced in the event (the
            # "Not supported" bar segment + a note) and the on-screen banner. Explaining
            # + reimplementing them is ON-DEMAND via the banner's "Ask AI DBA how to
            # reimplement these" button -- matching every other step's on-demand AI
            # pattern (no auto-fired turn on a deterministic action).

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
        if apply_in_progress:
            # Regenerating mid-apply would swap the DDL under the in-flight worker,
            # so the target could end up with statements the screen no longer shows.
            gen_btn.disable()  # type: ignore[attr-defined]
            gen_btn.tooltip(  # type: ignore[attr-defined]
                "Wait for the apply to finish — regenerating now would change the "
                "DDL the running apply is using."
            )
        elif conv_state.generated_node_ids is not None:
            # Lock re-generation until "Reset all": clicking Generate again would
            # silently re-run over the same committed scope with no visible change
            # (so it looks unresponsive). Disable it and require an explicit reset
            # to start a fresh generation, which makes the regeneration obvious.
            gen_btn.disable()  # type: ignore[attr-defined]
        if conv_state.generated_node_ids is not None:
            reset_btn = ui.button("Reset all", on_click=on_clear).props(  # type: ignore[attr-defined]
                "flat icon=restart_alt"
            )
            if apply_in_progress:
                # Reset discards the generated DDL the running apply is executing.
                reset_btn.disable()  # type: ignore[attr-defined]
                reset_btn.tooltip(  # type: ignore[attr-defined]
                    "Wait for the apply to finish — resetting now would discard the "
                    "DDL it is applying."
                )
            else:
                reset_btn.tooltip(  # type: ignore[attr-defined]
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
        # The detailed object list (names + what to do) is delivered in the AI window
        # ON DEMAND -- the button below opens it (matching every step's on-demand AI
        # pattern; there is no auto-fired turn on Generate) -- so it is intentionally
        # NOT duplicated inline under the banner.
        # AI DBA names each trigger/routine/event (via list_unsupported_objects) and
        # gives a per-kind reimplementation path. Shown only when AI assist is on.
        if on_reimplement_chat is not None:
            ui.button(  # type: ignore[attr-defined]
                "Ask AI DBA how to reimplement these",
                icon="auto_awesome",
                on_click=lambda: on_reimplement_chat(),
            ).props("flat dense no-caps color=primary").classes("mt-1")

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
                _render_pk_strategy_picker(
                    ui, source_table, conv_state, refresh, source_type=source_type
                )
            _render_preview(
                ui,
                preview,
                conv_state,
                refresh,
                is_ai_candidate=(
                    preview.object_name in candidates or not_auto_converted
                ),
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
        # Count the two kinds separately: a recommendation is not a warning, and
        # calling it one made a table whose ONLY note was throughput advice read as
        # "Review needed - 1 warning". Losses drive the severity label; advice is
        # reported on its own as "N recommendations".
        losses, recommendations = split_conversion_notes(preview.warnings)
        if losses:
            count = len(losses)
            warning_text = f"{count} warning{'s' if count != 1 else ''}"
            # Surface the severity (Unsupported > Review needed) so the user sees that
            # an object needs manual work, not merely that it has "N warnings".
            classes = {w.classification for w in losses}
            if Classification.UNSUPPORTED in classes:
                parts.append(f"{classification_label('UNSUPPORTED')} · {warning_text}")
            elif Classification.MANUAL in classes:
                parts.append(f"{classification_label('MANUAL')} · {warning_text}")
            else:
                parts.append(warning_text)
        if recommendations:
            n = len(recommendations)
            parts.append(f"{n} recommendation{'s' if n != 1 else ''}")
    if edited:
        parts.append("edited")
    if applied is not None:
        parts.append(f"applied: {applied.status.value}")
    return " · ".join(parts)


_KEEP_PK = "KEEP"
_IDENTITY_PK = "IDENTITY"
_COMPOSITE_PK = "COMPOSITE"


def _identity_eligible(table: TableDef) -> bool:
    """Return whether the "Server-generated (IDENTITY)" strategy applies to ``table``.

    Only a single-column primary key that IS the AUTO_INCREMENT column: that is the
    key the identity strategy rewrites (``BIGINT ... GENERATED BY DEFAULT AS IDENTITY``)
    and the overwhelmingly common MySQL shape. A composite source key, or an
    AUTO_INCREMENT column that is not the whole PK, is out of scope -- the tile is not
    offered there so the picker never suggests an identity it would not cleanly apply.
    """
    column = table.auto_increment_column
    return bool(column) and table.primary_key == [column]


def _render_pk_strategy_picker(
    ui: object,
    table: TableDef,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> None:
    """Per-table primary-key strategy picker (Keep / Server-generated / Composite).

    Opt-in and per-table: the default is Keep source PK (the source key unchanged).
    Two alternatives spread writes across Aurora DSQL partitions (avoiding the
    monotonic-key hot partition), each offered only when it applies to this table:

    * Server-generated (IDENTITY) -- for a single-column AUTO_INCREMENT key: the key
      becomes ``BIGINT ... GENERATED BY DEFAULT AS IDENTITY (CACHE 65536)``. ``BY
      DEFAULT`` means Full Load still inserts the source's own ids and the sequence is
      advanced past them afterwards (the Full Load engine's identity-sequence sync),
      so the data path is unchanged -- this is the faithful port of AUTO_INCREMENT.
    * Composite key -- prepends a high-cardinality column to the key, requiring the
      application to key on the full composite key.

    Every choice is stored by baking the chosen target script into
    ``conv_state.edited_target_ddls`` -- the same field Full Load and Schema Apply
    already consume and the session snapshot persists -- so the picker holds NO
    separate state and is resume-safe. The picker's rendered state is derived back out
    of that stored DDL (composite key columns / identity clause).

    UUID is deliberately NOT offered here: the converter's UUID strategy retypes the
    key column to ``uuid``, which the tool's Full Load cannot populate from the
    source's integer ids (an int->uuid insert fails), so it would steer a data
    migration into a broken load. It remains reachable via manual DDL edit for a
    schema-only / greenfield case.
    """
    # Only tables with a primary key can have a monotonic-key hot partition to fix.
    if not table.primary_key:
        return
    candidates = composite_leading_candidates(table)
    identity_ok = _identity_eligible(table)
    name = table.name
    stored = conv_state.get_edited_target_ddl(name)
    current_leading = (
        composite_leading_from_ddl(table, stored) if stored is not None else None
    )
    is_composite = current_leading is not None
    # Composite is checked first: it is the more structural rewrite (it changes the
    # key COLUMNS), and the two strategies are mutually exclusive in a stored DDL.
    is_identity = (
        not is_composite
        and stored is not None
        and identity_from_ddl(table, stored)
    )
    converter = SchemaConverter(source_type=source_type)

    def _select_strategy(choice: str) -> None:
        """Apply a primary-key strategy choice (called with the tile's value)."""
        if choice == _COMPOSITE_PK:
            leading = default_composite_leading(table)
            if leading is None:
                return  # no eligible column; the control stays on Keep (see below)
            conv_state.set_edited_target_ddl(
                name, render_target_ddl(build_composite_conversion(converter, table, leading))
            )
        elif choice == _IDENTITY_PK:
            conv_state.set_edited_target_ddl(
                name, render_target_ddl(build_identity_conversion(converter, table))
            )
        else:
            # Back to the source key: drop the override so the table uses the
            # deterministic (unchanged-key) conversion again.
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

    # Offer only the strategies that actually apply to this table (radio_tiles has a
    # single group-level lock, so a tile that cannot be used is not shown rather than
    # rendered dead). Keep is always present; Server-generated for an AUTO_INCREMENT
    # single-column key; Composite when a high-cardinality leading column exists.
    tiles: list[tuple[str, str, str, str]] = [
        (
            _KEEP_PK,
            "vpn_key",
            "Keep source PK",
            f"Target key stays ({', '.join(table.primary_key)}), exactly as on "
            "the source. Nothing in the application changes.",
        )
    ]
    if identity_ok:
        tiles.append(
            (
                _IDENTITY_PK,
                "pin",
                "Server-generated (IDENTITY)",
                "DSQL fills the integer key (GENERATED BY DEFAULT AS IDENTITY, "
                "CACHE 65536), spreading inserts across nodes. Faithful port of "
                "AUTO_INCREMENT: the loader keeps the source ids; gaps/loose order "
                "after.",
            )
        )
    if candidates:
        tiles.append(
            (
                _COMPOSITE_PK,
                "shuffle",
                "Composite key",
                "Prepend a high-cardinality column to spread writes across DSQL "
                "partitions. Higher insert throughput, but the application must "
                "key on the full composite key.",
            )
        )

    if is_composite:
        selected = _COMPOSITE_PK
    elif is_identity:
        selected = _IDENTITY_PK
    else:
        selected = _KEEP_PK

    with ui.card().classes("w-full !shadow-none border border-gray-200 bg-gray-50 p-3 gap-2"):  # type: ignore[attr-defined]
        ui.label("Primary key").classes("text-sm font-semibold text-gray-700")  # type: ignore[attr-defined]
        # Cloudscape "Tiles", not a segmented control: this is a decision with real
        # consequences (a composite/identity key changes what the application relies
        # on, and is immutable once created), so each option needs a sentence
        # explaining the trade-off. Shared with the Data Migration type picker via
        # ui/design.radio_tiles.
        radio_tiles(
            ui,
            tuple(tiles),
            selected=selected,
            on_select=_select_strategy,
            # Only the Keep tile applies -- no alternative strategy fits this table --
            # so the group is muted (nothing to switch to).
            locked=len(tiles) == 1,
            compact=True,
        )
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
        elif is_identity:
            render_notice(
                ui,
                tone="info",
                header="DSQL will generate the key; expect gaps and loose ordering",
                body=(
                    f"'{table.auto_increment_column}' becomes a cached identity "
                    "(BIGINT, CACHE 65536): each DSQL node draws its own block of "
                    "values, which spreads inserts but means the key is no longer "
                    "gap-free or strictly increasing. Fine when the id is just an "
                    "opaque surrogate; reconsider if the application relies on the "
                    "key being sequential (e.g. invoice/order numbers). Full Load "
                    "keeps the source ids and the sequence is advanced past them, so "
                    "the app's later inserts do not collide."
                ),
            )
        elif not candidates and not identity_ok:
            # No alternative strategy applies to this table (no eligible composite
            # leading column, not a single-column AUTO_INCREMENT key), so only Keep is
            # offered -- say why the alternatives are absent.
            inline_hint(
                ui,
                "No alternative primary-key strategy applies: this table has no "
                "single-column AUTO_INCREMENT key and no NOT NULL non-key column to "
                "lead a composite key.",
                tone="neutral",
            )


def _render_preview(
    ui: object,
    preview: DdlPreview,
    conv_state: SchemaConversionState,
    refresh: Callable[[], None],
    *,
    is_ai_candidate: bool = False,
    on_apply_object: Optional[Callable[[str], None]] = None,
    on_ai_chat: Optional[Callable[..., None]] = None,
) -> None:
    """Render one object's source-vs-target DDL diff (Req 10.2, 11.5).

    For an auto-converted table the source and target DDL are shown as a
    side-by-side, change-highlighted diff (via the editable target), so the user
    sees exactly what the conversion changed. The target is editable (the edit is
    remembered per object and used on apply, and the diff re-computes against the
    edit). Objects that are not auto-converted (views, triggers, routines) show
    their source + a read-only not-converted note, plus (for an AI candidate) the
    per-object AI-chat icon so the user can convert it with AI DBA -- whose
    "Use as target DDL" reply footer flows a fix into the editor -> Apply.
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
    with ui.column().classes("w-full gap-3"):  # type: ignore[attr-defined]
        if not editable:
            # Not auto-converted (view/trigger/routine): show the source + the
            # read-only not-converted target, plus (for an AI candidate) the AI-chat
            # icon to convert it with AI DBA.
            with ui.row().classes("items-center gap-1 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label("Source DDL (MySQL)").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-blue-800"
                )
                ui.space()  # type: ignore[attr-defined]
                _render_copy_ddl_button(ui, preview.source_ddl, label="Source DDL")
            ui.code(preview.source_ddl, language="sql").classes("w-full")  # type: ignore[attr-defined]
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
                    on_ai_chat=on_ai_chat,
                )
        else:
            # Auto-converted: the source/target diff over the deterministic (editable)
            # conversion. AI guidance is a compact icon next to each conversion warning
            # below (mirrors Evaluation), not a separate toolbar button.
            _render_editable_target(
                ui,
                preview,
                conv_state,
                on_apply_object,
            )

    if preview.warnings:
        _render_conversion_warnings(
            ui,
            preview.warnings,
            on_ai_chat=on_ai_chat,
            object_name=preview.object_name,
            source_ddl=preview.source_ddl,
            deterministic_ddl=preview.target_ddl,
        )


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


def _note_scope_key(note: ConversionWarning) -> str:
    """A stable, short scope suffix identifying ONE conversion note on an object.

    Uses the note kind + its column (like Evaluation keys a finding by rule), so a
    per-note AI chat gets its own scope -- clicking a different warning re-seeds with
    that warning instead of re-focusing the first one's conversation.
    """
    kind = getattr(getattr(note, "kind", None), "value", "") or "note"
    column = (getattr(note, "column_name", "") or "").strip()
    return f"{kind}:{column}" if column else kind


def _note_subtitle(object_name: str, note: ConversionWarning) -> str:
    """Panel subtitle for a per-note chat: object + the column (or note kind)."""
    column = (getattr(note, "column_name", "") or "").strip()
    if column:
        return f"{object_name} · {column}"
    kind = getattr(getattr(note, "kind", None), "value", "") or ""
    return f"{object_name} · {kind}" if kind else object_name


def _conversion_note_question(object_name: str, note: ConversionWarning) -> str:
    """Phrase a guidance request about ONE specific conversion note on an object.

    Embeds the note's actual message (and column) so the AI answers about THIS issue
    -- not the generic "walk me through converting this table" question.
    """
    message = (getattr(note, "message", "") or "").strip()
    column = (getattr(note, "column_name", "") or "").strip()
    where = f" on column {column}" if column else ""
    detail = f' The conversion flagged{where}: "{message}".' if message else where
    return (
        f"On {object_name}, migrating to Amazon Aurora DSQL,{detail} "
        "What does this mean, and exactly what should I do about it?"
    )


def _render_conversion_note_ai_icon(
    ui: object,
    on_ai_chat: Optional[Callable[..., None]],
    object_name: str,
    source_ddl: str,
    deterministic_ddl: str,
    note: Optional[ConversionWarning] = None,
) -> None:
    """Compact AI-guidance ICON next to a conversion note (mirrors Evaluation).

    Replaces the old labeled "AI guidance" toolbar button: one click opens the AI DBA
    panel scoped to converting THIS object. ``click.stop`` keeps the click from toggling
    an enclosing expansion. When AI is off (no ``on_ai_chat``) the icon is shown
    disabled with a hint so the affordance stays discoverable.
    """
    if on_ai_chat is not None and object_name:
        btn = ui.button(icon="auto_awesome")  # type: ignore[attr-defined]
        btn.props("flat round dense size=sm color=indigo-6")
        btn.on(  # type: ignore[attr-defined]
            "click.stop",
            # Pass THIS note so the chat is seeded with its specific issue, not the
            # generic "walk me through converting the table" question.
            lambda _e=None, _n=note: on_ai_chat(
                object_name, source_ddl, deterministic_ddl, _n
            ),
        )
        btn.tooltip(  # type: ignore[attr-defined]
            "Ask AI DBA about this issue" if note is not None
            else "Ask AI DBA about this conversion"
        )
    else:
        disabled = ui.button(icon="auto_awesome")  # type: ignore[attr-defined]
        disabled.props("flat round dense size=sm color=grey-5")
        disabled.disable()  # type: ignore[attr-defined]
        disabled.tooltip(  # type: ignore[attr-defined]
            "Enable AI Assist on the Connect screen to get conversion guidance."
        )


def _render_conversion_warnings(
    ui: object,
    warnings: Sequence[ConversionWarning],
    *,
    on_ai_chat: Optional[Callable[..., None]] = None,
    object_name: str = "",
    source_ddl: str = "",
    deterministic_ddl: str = "",
) -> None:
    """Render conversion notes as wrapping lists, split by kind.

    Two separate sections, because they are different claims:

    * **Conversion warnings** -- something could not be carried over or changed
      meaning (a removed foreign key, a dropped collation, an unmapped type). Keeps
      the severity badge (MANUAL amber / UNSUPPORTED red): the operator has to decide
      what to do.
    * **Recommendations** -- the conversion is complete and correct; this is advice
      for running well on DSQL (e.g. a kept AUTO_INCREMENT key works, but a
      UUID/random or cached-identity key spreads inserts). Rendered in a calm
      info-blue "RECOMMENDED" badge, never the amber warning treatment, so advice is
      not mistaken for a defect. Severity calibration per the design system: things
      that need no action are info, not warning.

    Each row is full-width: badges plus the message in a flexible cell that wraps, so
    long messages are never cut off on the right (unlike fixed table columns).
    """
    losses, recommendations = split_conversion_notes(warnings)

    def _rows(notes, *, badge_text=None, badge_color=None, advisory=False) -> None:
        with ui.column().classes("w-full gap-2"):  # type: ignore[attr-defined]
            for note in notes:
                color = badge_color or _WARNING_BADGE_COLOR.get(
                    note.classification.value, "grey"
                )
                # Same card treatment the Evaluation findings use: a tinted surface with a
                # matching *-200 border, tone chosen by what the note IS. A bare ``border``
                # renders Tailwind's default near-black, which read as an outlined table
                # cell rather than as one of this app's notice cards -- and put a harder
                # line around a recommendation than Evaluation puts around an UNSUPPORTED
                # finding. Advisory notes take the calm sky tone they already use there.
                surface = (
                    "border-sky-200 bg-sky-50"
                    if advisory
                    else "border-gray-200 bg-gray-50"
                )
                with ui.row().classes(  # type: ignore[attr-defined]
                    "items-start gap-2 w-full no-wrap rounded-md border p-3 " + surface
                ):
                    ui.badge(badge_text or note.classification.value).props(  # type: ignore[attr-defined]
                        f"color={color}"
                    )
                    if note.column_name:
                        ui.badge(note.column_name).props(  # type: ignore[attr-defined]
                            "color=blue-grey-6 outline"
                        )
                    ui.label(note.message).classes(  # type: ignore[attr-defined]
                        "text-sm flex-1 min-w-0 whitespace-normal break-words"
                    )
                    # Per-note AI-guidance icon on the right (mirrors Evaluation's
                    # per-finding icon) -- one click asks AI DBA about THIS note's issue.
                    _render_conversion_note_ai_icon(
                        ui, on_ai_chat, object_name, source_ddl, deterministic_ddl,
                        note=note,
                    )

    if losses:
        ui.label("Conversion warnings").classes("text-sm font-semibold")  # type: ignore[attr-defined]
        _rows(losses)
    if recommendations:
        # The "these are optional, not problems" explanation lives in a tooltip on a
        # help glyph rather than as a standing line of text: the RECOMMENDED badge and
        # the section title already carry the message, so a permanent paragraph
        # restating it is noise on a screen that repeats this block per object.
        with ui.row().classes(  # type: ignore[attr-defined]
            "items-center gap-1 no-wrap" + (" mt-2" if losses else "")
        ):
            ui.label("Recommendations").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            ui.icon("help_outline").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help"
            ).tooltip(
                "The conversion is complete — these are optional tuning suggestions "
                "for Aurora DSQL, not problems to fix."
            )
        _rows(
            recommendations,
            badge_text="RECOMMENDED",
            badge_color="info",
            advisory=True,
        )


def _render_copy_ddl_button(ui: object, text, *, label: str) -> None:
    """Render a small copy-to-clipboard icon button for a DDL block.

    ``label`` names what is copied (e.g. "Source DDL") so the confirmation toast
    and the button tooltip are specific. Mirrors the copy pattern used elsewhere
    (``ui.clipboard.write`` + a positive toast, with a graceful fallback when the
    browser clipboard is unavailable, e.g. non-HTTPS or denied permission).

    ``text`` may be a string OR a zero-arg callable read at click time. The editor
    header passes a callable: its DDL changes as the user types, and capturing the
    string at build time copied the pre-edit version while "Apply to target" sent the
    edited one -- copy and apply disagreeing on the same button row.
    """

    def _copy() -> None:
        value = text() if callable(text) else text
        try:
            ui.clipboard.write(value)  # type: ignore[attr-defined]
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


# CodeMirror renders its own DOM, so its height cannot be set from the wrapper: a
# ``max-height`` there left ``.cm-editor`` at its default 256px and the taller pane was cut
# off mid-statement with no scrollbar. These rules target the editor and its scroller
# directly. Injected once per page (``ui.add_css`` de-duplicates by content).
_DDL_PANE_CSS = """
.ddl-pane .cm-editor { height: 100%; }
.ddl-pane .cm-scroller { max-height: 26rem; min-height: 8rem; overflow: auto; }
/* The WRAPPER needs the height too. Sizing only the scroller left the outer element at
   its 256px default, so a 540px scroller overflowed it and the dialog showed a tall blank
   band under a clipped editor -- the same trap as the pane, one element further out. */
/* Grows WITH the DDL up to a cap, instead of a fixed box: 29 lines is the longest real DDL
   measured and ~44rem holds it, while a 4-line object gets a small dialog rather than the
   same tall one. ``height: auto`` on the wrapper is what allows that -- CodeMirror falls
   back to a fixed 256px otherwise, which pinned a 29-line DDL to the same height as a
   4-line one. ``max-height`` (not ``height``) is then what keeps a huge DDL from running
   off the screen, and the vh term keeps a short window usable. */
.ddl-expanded { height: auto; max-height: min(44rem, 74vh); }
.ddl-expanded .cm-editor { height: auto; max-height: min(44rem, 74vh); }
.ddl-expanded .cm-scroller { max-height: min(44rem, 74vh); min-height: 6rem; overflow: auto; }
"""


def _render_ddl_pane(
    ui: object,
    ddl: str,
    *,
    language: str,
    divider: bool,
) -> None:
    """Render one side of the DDL comparison in a real code editor.

    ``language`` is a CodeMirror language name (``"MySQL"`` / ``"PostgreSQL"``), so each
    side is highlighted in ITS OWN dialect -- backtick identifiers and MySQL types on the
    left, double-quoted identifiers and PostgreSQL types on the right.
    """
    classes = "w-1/2 min-w-0"
    if divider:
        classes += " border-r border-slate-200"
    with ui.element("div").classes(classes):  # type: ignore[attr-defined]
        # line_wrapping=False: one logical line stays one line and the editor scrolls
        # horizontally, the way a Markdown fence and every editor behave. The hand-rolled
        # view this replaces wrapped with ``break-all``, which split mid-token (an ENUM list
        # came out as ``'cancel`` / ``led')``) and turned one line into several rows.
        # ``disable``, NOT ``readonly``: NiceGUI's CodeMirror has no readonly prop, and
        # passing one is silently ignored -- the pane stayed editable, so a user could type
        # into this read-only comparison and see their change vanish on the next re-render
        # while "Apply to target" still sent the unedited DDL. ``disable`` reconfigures
        # CodeMirror's ``editable`` compartment, which actually blocks input (verified:
        # contenteditable=false and typing does nothing). Editing has its own mode, entered
        # with the Edit button, whose editor writes to the per-object buffer.
        #
        # The height has to land on CodeMirror's own scroller, not on the wrapper: a
        # ``max-height`` on the outer element left ``.cm-editor`` at its default 256px, so
        # the taller side was silently cut off mid-statement with no scrollbar to reveal it.
        # ``h-full`` makes both panes share the row's height so neither shows dead space,
        # and the min/max keep a one-line object readable without letting a large table run
        # off the page.
        ui.codemirror(  # type: ignore[attr-defined]
            ddl,
            language=language,
            theme="basicLight",
            line_wrapping=False,
        ).classes("w-full h-full ddl-pane").props("disable")


def _render_expand_ddl_button(
    ui: object, ddl: str, *, title: str, language: str
) -> None:
    """Render an expand icon that opens the DDL full-screen in a maximized dialog.

    The comparison is a split view, so each pane gets half the window: measured against a
    real source, 14 of 18 tables had a line too long for that width and 4 exceeded the
    pane's height. Both scroll, but reading a 144-character CHECK constraint through a
    half-width porthole is the kind of friction that makes an operator copy the DDL out to
    an editor instead of reviewing it here.

    Full-screen because the pane's limit is WIDTH first: simply making the pane taller
    would address the smaller half of the problem. Opt-in, so the default screen keeps its
    two-pane comparison and nothing moves for the objects that already fit.
    """

    def _open() -> None:
        # Sized to the CONTENT, not to the screen. Measured across a real source, the widest
        # DDL line is 144 characters and the longest is 29 lines -- roughly 1060 x 800px at
        # this font -- so a maximized dialog covered a 1440x900 display to show a panel with
        # room to spare. The caps stay in viewport units so a small window still gets a
        # usable dialog, and the surrounding page remains visible as context.
        with ui.dialog() as dialog, ui.card().classes("gap-2").style(  # type: ignore[attr-defined]
            "width: min(1100px, 92vw); max-width: 92vw"
        ):
            with ui.row().classes("items-center gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.label(title).classes("text-sm font-semibold")  # type: ignore[attr-defined]
                ui.space()  # type: ignore[attr-defined]
                _render_copy_ddl_button(ui, ddl, label=title)
                ui.button(on_click=dialog.close).props(  # type: ignore[attr-defined]
                    "flat dense round size=sm icon=close color=grey-7"
                ).tooltip("Close")
            # ``ddl-expanded`` (not ``ddl-pane``) so this editor fills the dialog instead
            # of inheriting the pane's 26rem cap -- the cap is the very thing being escaped.
            ui.codemirror(  # type: ignore[attr-defined]
                ddl,
                language=language,
                theme="basicLight",
                line_wrapping=False,
            ).classes("w-full ddl-expanded").props("disable")
        dialog.open()

    btn = ui.button(on_click=_open).props(  # type: ignore[attr-defined]
        "flat dense round size=sm icon=open_in_full color=grey-7"
    )
    btn.tooltip(f"Expand {title}")  # type: ignore[attr-defined]


def _render_ddl_header(
    ui: object,
    *,
    icon: str,
    title: str,
    copy_ddl,  # str | Callable[[], str]: a callable is read at click time
    copy_label: str,
    width: str = "w-1/2",
    divider: bool = False,
    expand_language: Optional[str] = None,
    trailing: Optional[Callable[[], None]] = None,
) -> None:
    """Render one header band naming a DDL pane, with its copy button.

    Shared by the read-only comparison and the editor, so the editor is never an unlabeled
    box: on entering Edit the two headers used to disappear, leaving a bare code area with
    no indication that it is the TARGET being changed -- which matters because the source is
    read-only by design and editing the wrong side is a plausible misread.

    ``trailing`` renders extra content after the copy button (the editor uses it for its
    "Editing" badge, keeping the state on the same band as the title it qualifies).
    """
    classes = f"{width} items-center gap-2 px-3 py-1.5 no-wrap"
    if divider:
        classes += " border-r border-slate-200"
    with ui.row().classes(classes):  # type: ignore[attr-defined]
        ui.icon(icon, color="blue-grey-5").classes("text-sm")  # type: ignore[attr-defined]
        ui.label(title).classes(CODE_HEADER_LABEL_CLASSES)  # type: ignore[attr-defined]
        ui.space()  # type: ignore[attr-defined]
        _render_copy_ddl_button(ui, copy_ddl, label=copy_label)
        if expand_language is not None:
            _render_expand_ddl_button(
                ui, copy_ddl, title=title, language=expand_language
            )
        if trailing is not None:
            trailing()


def _render_ddl_diff(ui: object, source_ddl: str, target_ddl: str) -> None:
    """Render the Source vs Target DDL side by side, each in a code editor.

    Uses NiceGUI's bundled CodeMirror rather than a hand-built diff table. That table
    aligned the two sides line-for-line via ``difflib``, which reads well until a line is
    long: it wrapped with ``break-all`` and split mid-token, and every attempt to stop
    wrapping traded one defect for another -- a fixed-width cell let unwrapped text print
    on top of the other column, and a content-sized cell made the divider zig-zag because
    each row sized independently. An editor solves all of that as a matter of course, and
    brings what the table never had: real SQL syntax highlighting per dialect, line numbers,
    code folding, and text selection that copies clean lines.

    What is given up is the line-for-line alignment: each pane starts at line 1, so a
    changed line is no longer physically beside its counterpart. The panes are short DDL for
    ONE object, and the conversion notes below already name what changed (removed foreign
    keys, async indexes, remapped types), so the pairing that mattered is stated in words
    rather than inferred from row positions.
    """
    ui.add_css(_DDL_PANE_CSS)  # type: ignore[attr-defined]
    # An AWS-Console code surface: a white, bordered panel with a quiet header bar naming
    # each side, then the two editors.
    with ui.column().classes(f"w-full gap-0 {CODE_SURFACE_CLASSES}"):  # type: ignore[attr-defined]
        with ui.row().classes(  # type: ignore[attr-defined]
            f"w-full gap-0 no-wrap {CODE_HEADER_CLASSES}"
        ):
            _render_ddl_header(
                ui,
                icon="storage",
                title="Source — MySQL",
                copy_ddl=source_ddl,
                copy_label="Source DDL",
                divider=True,
                expand_language="MySQL",
            )
            _render_ddl_header(
                ui,
                icon="cloud_queue",
                title="Target — Aurora DSQL",
                copy_ddl=target_ddl,
                copy_label="Target DDL",
                expand_language="PostgreSQL",
            )
        with ui.row().classes("w-full gap-0 no-wrap items-stretch"):  # type: ignore[attr-defined]
            _render_ddl_pane(ui, source_ddl, language="MySQL", divider=True)
            _render_ddl_pane(ui, target_ddl, language="PostgreSQL", divider=False)


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
                button.props("disable")  # type: ignore[attr-defined]
                button.set_text("Applying…")  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - button slot already gone
                logger.debug("Apply button busy-state skipped: slot already rebuilt")
        try:
            await on_apply_object(preview.object_name)
        finally:
            if button is not None:
                try:
                    button.props(remove="disable")  # type: ignore[attr-defined]
                    button.set_text("Apply to target")  # type: ignore[attr-defined]
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

                # SAME header band as the read-only comparison, so the editor is never an
                # unlabeled box. Entering Edit used to drop both headers, leaving a bare code
                # area with nothing saying it is the TARGET being changed -- and since the
                # source pane is read-only by design, mistaking one for the other is a
                # plausible misread. Only the target header shows here (full width): the
                # source is not on screen to be confused with, and repeating it would imply
                # it is editable too.
                ui.add_css(_DDL_PANE_CSS)  # type: ignore[attr-defined]
                with ui.column().classes(  # type: ignore[attr-defined]
                    f"w-full gap-0 {CODE_SURFACE_CLASSES}"
                ):
                    with ui.row().classes(  # type: ignore[attr-defined]
                        f"w-full gap-0 no-wrap {CODE_HEADER_CLASSES}"
                    ):
                        _render_ddl_header(
                            ui,
                            icon="cloud_queue",
                            title="Target — Aurora DSQL",
                            # A callable, read at click time, so Copy reflects the user's
                            # typing. on_edit writes each keystroke to the buffer; copying
                            # ``current`` (the string at build time) handed back the pre-edit
                            # DDL while Apply sent the edited one.
                            copy_ddl=lambda: (
                                conv_state.get_edited_target_ddl(preview.object_name)
                                or preview.target_ddl
                            ),
                            copy_label="Target DDL",
                            width="w-full",
                            # No expand here: the dialog is read-only, and offering it
                            # beside a live editor would invite edits into a copy that is
                            # discarded on close. In Edit the pane is already full width.
                            trailing=lambda: ui.badge("Editing").props(  # type: ignore[attr-defined]
                                "color=amber-7"
                            ),
                        )
                    ui.codemirror(  # type: ignore[attr-defined]
                        current,
                        language="PostgreSQL",
                        theme="basicLight",
                        line_wrapping=False,
                        on_change=on_edit,
                    ).classes("w-full ddl-pane")
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


def _render_generate_suggestion(
    ui: object,
    *,
    object_name: str,
    source_ddl: str,
    deterministic: str,
    on_ai_chat: Optional[Callable[..., None]] = None,
) -> None:
    """Render the per-object AI-guidance icon for a not-auto-converted object.

    Matches the Evaluation screen's AI button (flat, indigo ``auto_awesome``) and
    opens the SAME right chat drawer, scoped to converting THIS object, so the
    AI-assistance experience is identical across the app. The chat is advisory;
    its in-drawer "Use as target DDL" action pulls the latest reply's SQL into
    the editable target so it still flows into Apply. When AI is off (no opener
    wired) the icon is shown disabled with a hint, so it stays discoverable.
    """
    if on_ai_chat is None:
        disabled = ui.button(icon="auto_awesome")  # type: ignore[attr-defined]
        disabled.props("flat round dense size=sm color=grey-5")
        disabled.disable()
        disabled.tooltip(  # type: ignore[attr-defined]
            "Enable AI-assisted conversion on the Connect screen (toggle it on, "
            "set the Bedrock model, and re-test the connection), then reopen this "
            "step to chat about converting this object."
        )
        return

    ui.button(  # type: ignore[attr-defined]
        icon="auto_awesome",
        on_click=lambda: on_ai_chat(object_name, source_ddl, deterministic),
    ).props("flat round dense size=sm color=indigo-6").tooltip(
        "Ask AI DBA about converting this object"
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
    # Title names the OBJECT of the action ("generated DDL"), not just the action, so
    # this card reads as the bulk action ON the list above rather than a separate
    # feature. It also stops colliding with the per-object "Apply to target" button
    # label -- the same three words for two different scopes, one card apart, is what
    # made the bulk action feel unrelated to the DDL it applies.
    ui.label("Apply generated DDL to target").classes(  # type: ignore[attr-defined]
        "text-lg font-semibold"
    )
    # Restate the scope with its COUNT. The count is the concrete tie back to the list:
    # it moves with the user's selection, so the two sections visibly describe the same
    # set -- which is what the old "...in the Generated DDL list above" pointer was
    # trying (and failing) to do with words alone.
    noun = "object" if table_count == 1 else "objects"
    ui.label(  # type: ignore[attr-defined]
        f"Applies the {table_count} {noun} from the Generated DDL list above "
        "(plus any approved AI suggestions) — not the whole schema. Choose how to "
        "handle objects that already exist on the target, then apply."
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
            # Say WHAT is being applied, not just "all": "Apply all 7 generated
            # objects" is unambiguous about scope, where a bare "Apply all to target
            # (7)" reads as "everything on the source".
            apply_label = (
                "Applying…"
                if in_progress
                else f"Apply all {table_count} generated {noun} to target"
            )
            apply_button = ui.button(  # type: ignore[attr-defined]
                apply_label,
                on_click=on_apply_all,
            ).props("unelevated no-caps color=primary icon=cloud_upload")
            if in_progress:
                apply_button.props("disable")  # type: ignore[attr-defined]
        # The scope is now carried by the header + button label, so this line only adds
        # what neither says: the single-object alternative. Pointless when the scope IS
        # one object -- the button already applies exactly that -- so it is omitted
        # rather than telling the user to do what they are about to do.
        if table_count != 1:
            ui.label(  # type: ignore[attr-defined]
                "To apply just one object instead, use its \"Apply to target\" button "
                "in the Generated DDL list above."
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
