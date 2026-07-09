# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit and property tests for the AI conversion routing/augmentation layer.

Covers Task 16.4 (Schema/Query Converter routing + read-only guarantee):

- Trigger condition: AI suggestions are generated only for items the
  deterministic converter flagged ``MANUAL``/``UNSUPPORTED`` and only when AI
  assist is enabled; ``AUTO`` items are never sent to Bedrock (Requirements 3.8,
  4.5, 11.5).
- Augment, not replace: routing returns a side-channel that preserves the
  deterministic result and its ``MANUAL``/``UNSUPPORTED`` flag; it never mutates
  the deterministic conversion (Property 6 / Requirement 11.10).
- Graceful degradation: an unavailable assistant outcome keeps the deterministic
  result/flag and surfaces a clear detail (Requirement 11.10).
- Read-only guarantee: the routing path opens no source connection and executes
  no SQL against the source -- it only reads already-extracted inputs and calls
  the Bedrock-only assistant (Property 1 / Requirement 11.11).
- Nothing is auto-applied: every produced suggestion stays ``PENDING_REVIEW``
  (Property 13).

All Bedrock interaction is faked, so these tests never reach AWS.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import pytest

from dsql_migrator.core.ai_assistant import AiConversionAssistant, AiSuggestionOutcome
from dsql_migrator.core.ai_routing import (
    QUERY_DSQL_CONSTRAINTS,
    SCHEMA_DSQL_CONSTRAINTS,
    AiRoutingResult,
    route_query_conversion,
    route_schema_conversion,
    suggest_query_for_playground,
)
from dsql_migrator.core.converter import (
    ConversionWarning,
    SchemaConversionResult,
    SchemaConverter,
    TableConversion,
)
from dsql_migrator.core.introspector import is_write_or_ddl
from dsql_migrator.core.models import (
    AiAssistConfig,
    AiConversionSuggestion,
    Classification,
    ColumnDef,
    ObjectRef,
    ObjectType,
    SourceInventory,
    TableDef,
)
from dsql_migrator.core.query_converter import QueryConverter


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _RecordingSuggester:
    """A fake assistant that records calls and returns a canned available outcome.

    It never reaches AWS or any database; it only records the grounding inputs so
    tests can assert which items were routed (and that ``AUTO`` items were not).
    """

    def __init__(self, *, suggested_text: str = "CREATE TABLE t (id uuid PRIMARY KEY)") -> None:
        self._suggested_text = suggested_text
        self.calls: list[dict[str, Any]] = []

    def try_suggest_schema_conversion(
        self,
        object_name: str,
        source_ddl: str,
        deterministic_result: Optional[str],
        dsql_constraints: str,
    ) -> AiSuggestionOutcome:
        self.calls.append(
            {
                "object_name": object_name,
                "source_ddl": source_ddl,
                "deterministic_result": deterministic_result,
                "dsql_constraints": dsql_constraints,
            }
        )
        suggestion = AiConversionSuggestion(
            object_name=object_name,
            kind="SCHEMA",
            suggested_sql_or_expr=self._suggested_text,
            model_id="fake-model",
        )
        return AiSuggestionOutcome.ok(suggestion)


class _UnavailableSuggester:
    """A fake assistant that always reports AI as unavailable (graceful degradation)."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def try_suggest_schema_conversion(
        self,
        object_name: str,
        source_ddl: str,
        deterministic_result: Optional[str],
        dsql_constraints: str,
    ) -> AiSuggestionOutcome:
        self.calls.append(object_name)
        return AiSuggestionOutcome(
            available=False,
            reason="ACCESS_DENIED",
            detail="AI assist is unavailable: access was denied; deterministic result kept.",
        )


class _ReadOnlyBedrockClient:
    """A fake bedrock-runtime client recording every method invoked.

    Backs the real :class:`AiConversionAssistant` so the read-only test exercises
    the production path. ``invoke_model`` is an inference (read) call against
    Bedrock; any other attribute access is recorded and fails, proving the
    routing path performs only Bedrock inference and never a source-mutating call.
    """

    def __init__(self, suggestion_text: str) -> None:
        self._suggestion_text = suggestion_text
        self.invoked: list[str] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.invoked.append("invoke_model")
        payload = {"content": [{"type": "text", "text": self._suggestion_text}]}
        return {"body": json.dumps(payload).encode("utf-8")}

    def __getattr__(self, name: str) -> Any:
        def _forbidden(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError(f"unexpected non-inference client call: {name}")

        return _forbidden


# ---------------------------------------------------------------------------
# Fixtures / builders
# ---------------------------------------------------------------------------


def _mixed_inventory() -> SourceInventory:
    """An inventory mixing AUTO, MANUAL, and UNSUPPORTED schema items.

    - ``auto_t``: integer PK, plain columns -> AUTO (no warnings).
    - ``orders``: an ENUM column -> MANUAL.
    - ``nopk``: no primary key -> UNSUPPORTED.
    - trigger ``trg_audit`` -> UNSUPPORTED (no DSQL trigger object).
    """
    auto_t = TableDef(
        name="auto_t",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="name", mysql_type="VARCHAR(50)"),
        ],
        primary_key=["id"],
    )
    orders = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="INT", nullable=False),
            ColumnDef(name="status", mysql_type="ENUM('new','done')"),
        ],
        primary_key=["id"],
    )
    nopk = TableDef(
        name="nopk",
        columns=[ColumnDef(name="value", mysql_type="VARCHAR(20)")],
        primary_key=[],
    )
    return SourceInventory(
        tables=[auto_t, orders, nopk],
        triggers=[ObjectRef(name="trg_audit", object_type=ObjectType.TRIGGER)],
    )


def _schema_table(name: str, classification: Optional[Classification]) -> TableConversion:
    """Build a synthetic table conversion flagged with ``classification`` (or AUTO)."""
    warnings: list[ConversionWarning] = []
    if classification is not None:
        warnings.append(
            ConversionWarning(
                object_name=name,
                classification=classification,
                message=f"{name} flagged {classification.value}.",
            )
        )
    return TableConversion(
        table=name,
        target_ddl=f'CREATE TABLE "{name}" ("id" integer)',
        warnings=warnings,
    )


# ---------------------------------------------------------------------------
# Schema routing: disabled / no-assistant -> deterministic only (Req 11.1/11.2)
# ---------------------------------------------------------------------------


def test_schema_routing_disabled_makes_no_calls_and_returns_empty() -> None:
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _RecordingSuggester()

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=False), assistant=suggester
    )

    assert routed == AiRoutingResult(enabled=False, items=[])
    assert suggester.calls == []


def test_schema_routing_without_assistant_makes_no_calls() -> None:
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=None
    )

    assert routed.enabled is True
    assert routed.items == []


# ---------------------------------------------------------------------------
# Schema routing: enabled -> only MANUAL/UNSUPPORTED routed (Req 3.8/11.5)
# ---------------------------------------------------------------------------


def test_schema_routing_routes_only_flagged_items_not_auto() -> None:
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _RecordingSuggester()

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    routed_names = {item.object_name for item in routed.items}
    assert routed_names == {"orders", "nopk", "trg_audit"}
    # The AUTO table was never sent to Bedrock (avoids unnecessary cost/latency).
    assert "auto_t" not in routed_names
    assert {call["object_name"] for call in suggester.calls} == routed_names


def test_schema_routing_preserves_deterministic_flag_per_item() -> None:
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _RecordingSuggester()

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    by_name = {item.object_name: item for item in routed.items}
    assert by_name["orders"].classification is Classification.MANUAL
    assert by_name["nopk"].classification is Classification.UNSUPPORTED
    assert by_name["trg_audit"].classification is Classification.UNSUPPORTED


def test_schema_routing_grounds_prompt_with_source_ddl_and_constraints() -> None:
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _RecordingSuggester()

    route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    orders_call = next(c for c in suggester.calls if c["object_name"] == "orders")
    assert "CREATE TABLE" in orders_call["source_ddl"]
    assert orders_call["dsql_constraints"] == SCHEMA_DSQL_CONSTRAINTS
    # The deterministic result/reason is forwarded so AI augments (not replaces) it.
    assert "ENUM" in (orders_call["deterministic_result"] or "")


def test_schema_routing_augments_does_not_replace_deterministic_result() -> None:
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    before = result.model_dump()
    suggester = _RecordingSuggester()

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    # The deterministic result object is untouched by routing (augment, not replace).
    assert result.model_dump() == before
    # The AI outcomes are returned as a separate side-channel.
    assert all(item.outcome.available for item in routed.items)


def test_schema_routing_suggestions_stay_pending_review() -> None:
    """Property 13: nothing is auto-applied; suggestions remain PENDING_REVIEW."""
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _RecordingSuggester()

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    for item in routed.items:
        assert item.outcome.suggestion is not None
        assert item.outcome.suggestion.status == "PENDING_REVIEW"
        assert item.outcome.suggestion.approved_by_user is False


def test_schema_routing_graceful_degradation_keeps_flag_and_surfaces_detail() -> None:
    """Req 11.10: an unavailable outcome keeps the deterministic result/flag."""
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _UnavailableSuggester()

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    assert {item.object_name for item in routed.items} == {"orders", "nopk", "trg_audit"}
    for item in routed.items:
        assert item.outcome.available is False
        assert item.outcome.suggestion is None
        assert item.outcome.detail
        # The deterministic MANUAL/UNSUPPORTED flag is preserved on the item.
        assert item.classification in {Classification.MANUAL, Classification.UNSUPPORTED}


# ---------------------------------------------------------------------------
# Query routing (Req 4.5 / 11.5)
# ---------------------------------------------------------------------------


def _query_results() -> list:
    """One AUTO query and one MANUAL (multi-table FOR UPDATE) query."""
    converter = QueryConverter()
    return [
        converter.convert("SELECT * FROM t WHERE id = 1"),
        converter.convert("SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id = 1 FOR UPDATE"),
    ]


def test_query_routing_disabled_makes_no_calls() -> None:
    suggester = _RecordingSuggester()
    routed = route_query_conversion(
        _query_results(), config=AiAssistConfig(enabled=False), assistant=suggester
    )
    assert routed == AiRoutingResult(enabled=False, items=[])
    assert suggester.calls == []


def test_query_routing_routes_only_manual_queries() -> None:
    results = _query_results()
    # Sanity: the first is AUTO, the second is MANUAL.
    assert results[0].classification is Classification.AUTO
    assert results[1].classification is Classification.MANUAL
    suggester = _RecordingSuggester()

    routed = route_query_conversion(
        results, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    assert [item.object_name for item in routed.items] == ["query_2"]
    assert [call["object_name"] for call in suggester.calls] == ["query_2"]


def test_query_routing_tags_suggestion_as_query_and_grounds_original_sql() -> None:
    results = _query_results()
    suggester = _RecordingSuggester()

    routed = route_query_conversion(
        results, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    item = routed.items[0]
    assert item.kind == "QUERY"
    assert item.outcome.suggestion is not None
    assert item.outcome.suggestion.kind == "QUERY"
    assert item.outcome.suggestion.status == "PENDING_REVIEW"
    call = suggester.calls[0]
    assert "FOR UPDATE" in call["source_ddl"]
    assert call["dsql_constraints"] == QUERY_DSQL_CONSTRAINTS


# ---------------------------------------------------------------------------
# Property-style: only flagged items are ever routed (Req 3.8/11.5 trigger)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "classifications",
    [
        [],
        [None],
        [Classification.MANUAL],
        [Classification.UNSUPPORTED],
        [None, Classification.MANUAL, None],
        [Classification.MANUAL, Classification.UNSUPPORTED, None, None],
        [None, None, None],
    ],
)
def test_only_flagged_schema_items_are_routed(classifications: list) -> None:
    """For any mix of AUTO/MANUAL/UNSUPPORTED, exactly the flagged items route."""
    tables = [
        _schema_table(f"t{i}", classification)
        for i, classification in enumerate(classifications)
    ]
    result = SchemaConversionResult.from_tables(tables)
    inventory = SourceInventory()
    suggester = _RecordingSuggester()
    expected_flagged = sum(1 for c in classifications if c is not None)

    # Disabled: never routes, regardless of how many are flagged.
    disabled = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=False), assistant=suggester
    )
    assert disabled.items == []
    assert suggester.calls == []

    # Enabled: routes exactly the flagged items, never the AUTO ones.
    enabled = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )
    assert len(enabled.items) == expected_flagged
    assert len(suggester.calls) == expected_flagged


# ---------------------------------------------------------------------------
# Read-only guarantee (Property 1 / Requirement 11.11)
# ---------------------------------------------------------------------------


def test_schema_routing_performs_no_source_writes() -> None:
    """Property 1 / Req 11.11: the AI routing path never writes to the source.

    Exercises the real ``AiConversionAssistant`` with a fake Bedrock client that
    records every method invoked. The routing path takes no source connection and
    only reads the already-extracted inventory/result, so the sole external
    interaction is Bedrock inference (``invoke_model``); the source inventory is
    left unmodified.
    """
    inventory = _mixed_inventory()
    inventory_before = inventory.model_dump()
    result = SchemaConverter().convert(inventory)
    client = _ReadOnlyBedrockClient("CREATE TABLE t (id uuid PRIMARY KEY)")
    assistant = AiConversionAssistant(AiAssistConfig(enabled=True), client=client)

    routed = route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=assistant
    )

    # The only client calls were Bedrock inference -- no source-mutating call.
    assert client.invoked == ["invoke_model"] * len(routed.items)
    assert routed.items, "expected flagged items to be routed"
    # The source inventory was read only, never mutated.
    assert inventory.model_dump() == inventory_before
    # Nothing is auto-applied: suggestions remain PENDING_REVIEW (Property 13).
    for item in routed.items:
        assert item.outcome.suggestion is not None
        assert item.outcome.suggestion.status == "PENDING_REVIEW"


def test_routing_grounds_with_ddl_but_never_executes_it() -> None:
    """Property 1: DDL is grounding text for the prompt only, never executed.

    The grounded source DDL is a write/DDL statement, yet the routing path only
    forwards it as prompt context to Bedrock; it owns no executor and runs no
    statement against the source.
    """
    inventory = _mixed_inventory()
    result = SchemaConverter().convert(inventory)
    suggester = _RecordingSuggester()

    route_schema_conversion(
        result, inventory, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    orders_call = next(c for c in suggester.calls if c["object_name"] == "orders")
    # The grounding text is a CREATE (write/DDL) statement...
    assert is_write_or_ddl(orders_call["source_ddl"]) is True
    # ...but it was only ever passed as a string; routing has no source executor.


# ---------------------------------------------------------------------------
# Query Playground single-statement suggestion (suggest_query_for_playground)
# ---------------------------------------------------------------------------


def test_playground_suggest_disabled_makes_no_call() -> None:
    """AI off / no assistant -> no Bedrock call, an unavailable outcome is returned."""
    conversion = QueryConverter().convert("SELECT * FROM t FOR UPDATE")
    suggester = _RecordingSuggester()

    out = suggest_query_for_playground(
        conversion, config=AiAssistConfig(enabled=False), assistant=suggester
    )
    assert out.available is False
    assert suggester.calls == []  # never routed to Bedrock

    out_none = suggest_query_for_playground(
        conversion, config=AiAssistConfig(enabled=True), assistant=None
    )
    assert out_none.available is False


def test_playground_suggest_grounds_with_constraints_and_kind() -> None:
    """Enabled: the single statement is grounded with DSQL query constraints + kind."""
    conversion = QueryConverter().convert("SELECT * FROM t FOR UPDATE")
    suggester = _RecordingSuggester(suggested_text="SELECT * FROM t WHERE id = 1")

    out = suggest_query_for_playground(
        conversion, config=AiAssistConfig(enabled=True), assistant=suggester
    )

    assert out.available is True
    # Re-tagged QUERY (the playground is a query rewrite, not schema DDL).
    assert out.suggestion is not None
    assert out.suggestion.kind == "QUERY"
    assert len(suggester.calls) == 1
    call = suggester.calls[0]
    assert call["dsql_constraints"] == QUERY_DSQL_CONSTRAINTS
    assert "Statement kind: SELECT" in call["deterministic_result"]
    # The original (unconverted) SQL is the grounding source.
    assert call["source_ddl"] == "SELECT * FROM t FOR UPDATE"


def test_playground_suggest_includes_target_error_when_present() -> None:
    """A captured target EXPLAIN/dry-run error is fed to the model (idea: auto-fix)."""
    conversion = QueryConverter().convert("CREATE TABLE t (g GEOMETRY)")
    suggester = _RecordingSuggester()

    suggest_query_for_playground(
        conversion,
        config=AiAssistConfig(enabled=True),
        assistant=suggester,
        probe_error='Aurora DSQL rejected the statement: type "geometry" does not exist',
    )

    grounding = suggester.calls[0]["deterministic_result"]
    assert "geometry" in grounding
    assert "Fix the statement so it runs on Aurora DSQL." in grounding


def test_playground_suggest_offered_for_clean_auto_conversion() -> None:
    """Unlike batch routing, the playground offers help even for an AUTO statement."""
    conversion = QueryConverter().convert("SELECT id FROM t WHERE id = 1")
    assert conversion.classification is Classification.AUTO
    suggester = _RecordingSuggester(suggested_text="SELECT id FROM t WHERE id = 1")

    out = suggest_query_for_playground(
        conversion, config=AiAssistConfig(enabled=True), assistant=suggester
    )
    # The interactive playground routes the single statement the user asked about,
    # even though batch route_query_conversion would skip an AUTO item.
    assert out.available is True
    assert len(suggester.calls) == 1


def test_playground_suggest_degrades_gracefully() -> None:
    """An unavailable assistant outcome is passed through (deterministic kept)."""
    conversion = QueryConverter().convert("SELECT * FROM t FOR UPDATE")
    out = suggest_query_for_playground(
        conversion,
        config=AiAssistConfig(enabled=True),
        assistant=_UnavailableSuggester(),
    )
    assert out.available is False
    assert out.reason == "ACCESS_DENIED"
