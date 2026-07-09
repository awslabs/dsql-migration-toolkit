# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AI conversion routing: augment the deterministic path for flagged items.

Task 16.4 scope. This is the thin orchestration seam that connects the
deterministic converters (Schema Converter -- Task 5, Query Converter -- Task 6)
to the optional :class:`~dsql_migrator.core.ai_assistant.AiConversionAssistant`
(Tasks 16.1--16.3), implementing the design's "AI-assisted Conversion Design"
trigger condition:

- AI suggestions are generated *only* for items the deterministic converter
  flagged ``MANUAL``/``UNSUPPORTED`` *and* only when AI assist is enabled. Items
  the deterministic path converts automatically (``AUTO``) are never sent to
  Bedrock, avoiding unnecessary cost/latency (Requirements 3.8, 4.5, 11.5).
- AI **augments, never replaces** the deterministic path: this layer never
  mutates or overwrites the deterministic
  :class:`~dsql_migrator.core.converter.SchemaConversionResult` /
  :class:`~dsql_migrator.core.query_converter.QueryConversionResult`. It returns
  a parallel :class:`AiRoutingResult` side-channel that carries the augmenting
  :class:`~dsql_migrator.core.ai_assistant.AiSuggestionOutcome` alongside the
  preserved ``MANUAL``/``UNSUPPORTED`` flag. The caller keeps the deterministic
  result unchanged (Property 6 / Requirement 11.10).
- When AI assist is disabled (the default) or no assistant is wired, this layer
  performs **no Bedrock calls at all** and returns an empty routing result, so
  the workflow behaves exactly as the deterministic-only path (Requirements
  11.1, 11.2).
- Graceful degradation is delegated to the assistant's ``try_suggest_*`` wrappers
  (Task 16.3): they never raise, returning an unavailable
  :class:`~dsql_migrator.core.ai_assistant.AiSuggestionOutcome`. When an outcome
  is unavailable, the deterministic result and its ``MANUAL``/``UNSUPPORTED``
  flag are kept and the outcome's ``detail`` is surfaced (Requirement 11.10).

Read-only guarantee (Property 1 / Requirement 11.11): this layer only reads
already-extracted, in-memory inputs (the source inventory models, the
deterministic conversion results) and calls the Bedrock-only assistant. It opens
no source connection and executes no SQL against the source -- it cannot write
to the source. Nothing is ever auto-applied: every suggestion stays
``PENDING_REVIEW`` (or ``REJECTED`` when it failed output validation) and only
the explicit human review/approve gate in :mod:`dsql_migrator.ui.ai_assist`
makes a suggestion eligible for the Schema Applier path (Property 13).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Protocol, Sequence

from dsql_migrator.core.ai_assistant import AiSuggestionOutcome
from dsql_migrator.core.converter import (
    ConversionWarning,
    SchemaConversionResult,
    TableConversion,
    build_source_table_ddl,
)
from dsql_migrator.core.models import AiAssistConfig, Classification, SourceInventory
from dsql_migrator.core.query_converter import QueryConversionResult

# The deterministic classifications that trigger an AI suggestion. Auto-converted
# (AUTO) items are never routed to Bedrock (design "trigger" rule).
_AI_TRIGGER = frozenset({Classification.MANUAL, Classification.UNSUPPORTED})

# DSQL constraints used to ground a schema (DDL) suggestion prompt (design
# "AI-assisted Conversion Design"). Kept here so the core routing layer does not
# depend on the UI; mirrors the constraints the Schema Converter applies.
SCHEMA_DSQL_CONSTRAINTS = (
    "Aurora DSQL constraints: foreign keys are unsupported, every table requires "
    "a primary key, secondary indexes are built asynchronously "
    "(CREATE INDEX ASYNC), the 'C' collation is used, and there are transaction "
    "limits (a single DDL statement per transaction)."
)

# DSQL constraints used to ground a query (DML) rewrite suggestion prompt.
QUERY_DSQL_CONSTRAINTS = (
    "Aurora DSQL (PostgreSQL-compatible) constraints: SELECT ... FOR UPDATE is "
    "allowed only on a single table with a full primary-key equality predicate, "
    "INSERT ... ON DUPLICATE KEY UPDATE must become INSERT ... ON CONFLICT with "
    "an explicit conflict target, and MySQL-only functions/idioms must use their "
    "PostgreSQL equivalents."
)


class ConversionSuggester(Protocol):
    """Minimal seam for the AI assistant this layer depends on (Task 16.3).

    Only the graceful-degradation schema wrapper is needed: query rewrites are
    grounded as a schema-style conversion suggestion and re-tagged ``QUERY`` by
    this layer. Typing the dependency as a ``Protocol`` keeps the routing layer
    decoupled from the concrete
    :class:`~dsql_migrator.core.ai_assistant.AiConversionAssistant` and lets unit
    tests pass a fake that never reaches AWS.
    """

    def try_suggest_schema_conversion(
        self,
        object_name: str,
        source_ddl: str,
        deterministic_result: Optional[str],
        dsql_constraints: str,
    ) -> AiSuggestionOutcome:
        """Return a never-raising suggestion outcome for one flagged item."""


@dataclass(frozen=True)
class RoutedItem:
    """One flagged item's AI outcome, augmenting (not replacing) the deterministic result.

    ``object_name`` identifies the deterministic item (a table/trigger/routine
    name for schema, a positional ``query_N`` label for queries). ``kind``
    records whether it came from the Schema or Query Converter. ``classification``
    preserves the deterministic ``MANUAL``/``UNSUPPORTED`` flag so the
    deterministic verdict is never lost. ``outcome`` carries the augmenting AI
    result: when :attr:`AiSuggestionOutcome.available` is ``True`` it offers a
    reviewable suggestion (status ``PENDING_REVIEW`` or ``REJECTED`` -- never
    auto-applied), and when ``False`` the deterministic result/flag is kept and
    :attr:`AiSuggestionOutcome.detail` explains why AI was unavailable.
    """

    object_name: str
    kind: Literal["SCHEMA", "QUERY"]
    classification: Classification
    outcome: AiSuggestionOutcome


@dataclass(frozen=True)
class AiRoutingResult:
    """The AI side-channel that augments a deterministic conversion result.

    ``enabled`` echoes whether AI assist was on. ``items`` holds one
    :class:`RoutedItem` per deterministic item that was flagged
    ``MANUAL``/``UNSUPPORTED`` and routed to the assistant; it is empty when AI
    assist is disabled, no assistant is wired, or there were no flagged items. It
    never contains the deterministic conversion objects themselves -- those are
    returned to the caller unchanged, so the AI path augments rather than
    replaces the deterministic path.
    """

    enabled: bool
    items: list[RoutedItem]


def _most_severe(classifications: Sequence[Classification]) -> Classification:
    """Return ``UNSUPPORTED`` if present, otherwise ``MANUAL``.

    Only flagged (``MANUAL``/``UNSUPPORTED``) classifications are passed in, so
    this collapses an object's several warnings into a single preserved flag,
    favoring the more severe ``UNSUPPORTED``.
    """
    if Classification.UNSUPPORTED in classifications:
        return Classification.UNSUPPORTED
    return Classification.MANUAL


def _group_flagged_warnings(
    result: SchemaConversionResult,
) -> dict[str, list[ConversionWarning]]:
    """Group ``MANUAL``/``UNSUPPORTED`` warnings by owning object, in order.

    The result's aggregated warnings cover per-table type/constraint findings as
    well as object-level trigger/routine reimplementation findings (Requirement
    3.7), so grouping by ``object_name`` yields every schema item the
    deterministic path flagged. Insertion order is preserved for a deterministic
    routing order.
    """
    grouped: dict[str, list[ConversionWarning]] = {}
    for warning in result.warnings:
        if warning.classification in _AI_TRIGGER:
            grouped.setdefault(warning.object_name, []).append(warning)
    return grouped


def _schema_source_ddl(object_name: str, inventory: SourceInventory) -> str:
    """Return the already-extracted source definition to ground a prompt.

    Tables reconstruct their MySQL ``CREATE TABLE`` DDL (read from the in-memory
    inventory only), views use their stored definition, and triggers/routines --
    for which introspection keeps a reference only -- report that the source DDL
    was not captured. This reads in-memory models exclusively; it never queries
    the source (Property 1).
    """
    table = next((t for t in inventory.tables if t.name == object_name), None)
    if table is not None:
        try:
            return build_source_table_ddl(table)
        except ValueError:
            return f"-- Source DDL for table {object_name} could not be reconstructed."

    view = next((v for v in inventory.views if v.name == object_name), None)
    if view is not None and view.definition:
        return view.definition

    return f"-- Source definition for {object_name} is not captured in the inventory."


def _schema_deterministic_reason(
    conversion: Optional[TableConversion], warnings: Sequence[ConversionWarning]
) -> str:
    """Build the deterministic result/reason passed to the assistant.

    Combines the converted target DDL (when the item is a table the deterministic
    path produced) with the flagged warning messages, so the model augments the
    deterministic result and knows why the item needs manual attention.
    """
    parts: list[str] = []
    if conversion is not None:
        parts.append(conversion.target_ddl)
    messages = [warning.message for warning in warnings]
    if messages:
        parts.append("Flagged MANUAL/UNSUPPORTED: " + " ".join(messages))
    return "\n".join(parts)


def route_schema_conversion(
    result: SchemaConversionResult,
    inventory: SourceInventory,
    *,
    config: AiAssistConfig,
    assistant: Optional[ConversionSuggester],
) -> AiRoutingResult:
    """Route the schema converter's ``MANUAL``/``UNSUPPORTED`` items to the assistant.

    Implements the design trigger condition for schema (DDL) conversion
    (Requirements 3.8, 11.5): when ``config.enabled`` is ``False`` or no
    ``assistant`` is wired, no Bedrock call is made and an empty
    :class:`AiRoutingResult` is returned, leaving ``result`` untouched. When
    enabled, each object the deterministic path flagged ``MANUAL``/``UNSUPPORTED``
    (tables, triggers, routines) -- and only those -- is grounded with its
    already-extracted source DDL, the DSQL constraints, and the deterministic
    result, then sent to the assistant's never-raising
    ``try_suggest_schema_conversion``. ``AUTO`` items are never sent. The returned
    outcomes augment, never replace, ``result`` (Property 6); nothing is
    auto-applied (Property 13).
    """
    if not config.enabled or assistant is None:
        return AiRoutingResult(enabled=config.enabled, items=[])

    conversion_by_name = {conversion.table: conversion for conversion in result.tables}
    items: list[RoutedItem] = []
    for object_name, warnings in _group_flagged_warnings(result).items():
        classification = _most_severe([w.classification for w in warnings])
        source_ddl = _schema_source_ddl(object_name, inventory)
        deterministic = _schema_deterministic_reason(
            conversion_by_name.get(object_name), warnings
        )
        outcome = assistant.try_suggest_schema_conversion(
            object_name, source_ddl, deterministic, SCHEMA_DSQL_CONSTRAINTS
        )
        items.append(
            RoutedItem(
                object_name=object_name,
                kind="SCHEMA",
                classification=classification,
                outcome=outcome,
            )
        )
    return AiRoutingResult(enabled=True, items=items)


def _query_deterministic_reason(conversion: QueryConversionResult) -> str:
    """Build the deterministic result/reason for a flagged query.

    Combines the converted SQL (or a note when it could not be rendered) with the
    flagged warning messages, so the rewrite suggestion augments the
    deterministic conversion and knows why the query needs manual review.
    """
    parts: list[str] = []
    if conversion.converted_sql is not None:
        parts.append(conversion.converted_sql)
    else:
        parts.append("-- The deterministic converter could not render this query.")
    messages = [warning.message for warning in conversion.warnings]
    if messages:
        parts.append("Flagged for manual review: " + " ".join(messages))
    return "\n".join(parts)


def _as_query_outcome(outcome: AiSuggestionOutcome) -> AiSuggestionOutcome:
    """Re-tag an available schema-style suggestion as a ``QUERY`` suggestion.

    The assistant grounds a query rewrite with the same schema-conversion prompt
    path, which tags the suggestion ``SCHEMA``. Re-tagging it ``QUERY`` keeps the
    suggestion's ``kind`` consistent with the model (SCHEMA/DATA/QUERY) and
    preserves the review status (``PENDING_REVIEW``/``REJECTED``). Unavailable
    outcomes are returned unchanged.
    """
    suggestion = outcome.suggestion
    if not outcome.available or suggestion is None or suggestion.kind == "QUERY":
        return outcome
    return AiSuggestionOutcome.ok(suggestion.model_copy(update={"kind": "QUERY"}))


def _playground_deterministic_reason(
    conversion: QueryConversionResult, probe_error: Optional[str]
) -> str:
    """Ground a Query Playground suggestion with the conversion + a target error.

    Combines the deterministic converted SQL (or a note when it could not be
    rendered), the deterministic warnings, and -- crucially for the playground --
    the actual Aurora DSQL error from the EXPLAIN / dry-run probe when one was
    captured, so the model fixes the real failure rather than guessing. The
    statement kind is included so the model knows whether it is rewriting a
    SELECT, a DDL, or a DML statement.
    """
    parts: list[str] = [f"Statement kind: {conversion.statement_kind.value}."]
    if conversion.converted_sql is not None:
        parts.append("Deterministic DSQL conversion:\n" + conversion.converted_sql)
    else:
        parts.append("-- The deterministic converter could not render this query.")
    messages = [warning.message for warning in conversion.warnings]
    if messages:
        parts.append("Deterministic warnings: " + " ".join(messages))
    if probe_error:
        parts.append(
            "When the converted statement was tested on the target, Aurora DSQL "
            f"rejected it with this error:\n{probe_error}\n"
            "Fix the statement so it runs on Aurora DSQL."
        )
    return "\n".join(parts)


def suggest_query_for_playground(
    conversion: QueryConversionResult,
    *,
    config: AiAssistConfig,
    assistant: Optional[ConversionSuggester],
    probe_error: Optional[str] = None,
) -> AiSuggestionOutcome:
    """Produce one AI suggestion for a Query Playground statement (augmenting).

    Unlike :func:`route_query_conversion` (which only routes the deterministic
    ``MANUAL``/``UNSUPPORTED`` items of a batch), the playground is interactive:
    the user explicitly asks for help on the single statement they are editing,
    so this is offered for ANY statement -- a clean ``AUTO`` conversion that
    nonetheless failed to run on the target, or one the user simply wants
    explained. It grounds a single :meth:`ConversionSuggester.try_suggest_schema_conversion`
    call (re-tagged ``QUERY``) with the original SQL, the deterministic conversion
    result/warnings, the DSQL query constraints, and -- when supplied -- the exact
    target ``EXPLAIN``/dry-run error so the model fixes the real failure.

    Returns an unavailable :class:`AiSuggestionOutcome` (no Bedrock call) when AI
    assist is disabled or no ``assistant`` is wired, mirroring the routing layer.
    The suggestion is never auto-applied (Property 13): it is returned for human
    review/test, exactly like the routing path.
    """
    if not config.enabled or assistant is None:
        return AiSuggestionOutcome(
            available=False,
            reason="UNAVAILABLE",
            detail=(
                "AI assist is off. Enable it on the Connect screen to get an AI "
                "rewrite/explanation for this query."
            ),
        )
    deterministic = _playground_deterministic_reason(conversion, probe_error)
    outcome = assistant.try_suggest_schema_conversion(
        "playground_query",
        conversion.original_sql,
        deterministic,
        QUERY_DSQL_CONSTRAINTS,
    )
    return _as_query_outcome(outcome)


def route_query_conversion(
    results: Sequence[QueryConversionResult],
    *,
    config: AiAssistConfig,
    assistant: Optional[ConversionSuggester],
) -> AiRoutingResult:
    """Route the query converter's ``MANUAL``/``UNSUPPORTED`` results to the assistant.

    Implements the design trigger condition for query (DML) conversion
    (Requirements 4.5, 11.5): when ``config.enabled`` is ``False`` or no
    ``assistant`` is wired, no Bedrock call is made and an empty
    :class:`AiRoutingResult` is returned, leaving ``results`` untouched. When
    enabled, each query the deterministic path flagged for manual review (and only
    those) is grounded with its original SQL, the DSQL query constraints, and the
    deterministic result, then sent to the assistant; the suggestion is re-tagged
    ``QUERY``. Auto-converted queries are never sent. Outcomes augment, never
    replace, the deterministic results (Property 6); nothing is auto-applied
    (Property 13). Items are labelled ``query_N`` by 1-based input position.
    """
    if not config.enabled or assistant is None:
        return AiRoutingResult(enabled=config.enabled, items=[])

    items: list[RoutedItem] = []
    for index, conversion in enumerate(results, start=1):
        if conversion.classification not in _AI_TRIGGER:
            continue
        object_name = f"query_{index}"
        deterministic = _query_deterministic_reason(conversion)
        outcome = assistant.try_suggest_schema_conversion(
            object_name,
            conversion.original_sql,
            deterministic,
            QUERY_DSQL_CONSTRAINTS,
        )
        items.append(
            RoutedItem(
                object_name=object_name,
                kind="QUERY",
                classification=conversion.classification,
                outcome=_as_query_outcome(outcome),
            )
        )
    return AiRoutingResult(enabled=True, items=items)


__all__ = [
    "SCHEMA_DSQL_CONSTRAINTS",
    "QUERY_DSQL_CONSTRAINTS",
    "ConversionSuggester",
    "RoutedItem",
    "AiRoutingResult",
    "route_schema_conversion",
    "route_query_conversion",
    "suggest_query_for_playground",
]
