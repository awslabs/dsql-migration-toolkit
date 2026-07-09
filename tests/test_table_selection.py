# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the multi-table TableSelector (Task 19).

Covers (Requirement 5.9 / 9.4 / Property 16):
- A single and a multi-table selection resolve to the matching TableDefs.
- An empty selection infers "select all" (every inventory table).
- Unknown names raise TableSelectionError listing exactly the unknown names
  (untrusted input validated, never silently dropped).
- Resolved tables preserve inventory order and de-duplicate repeated selections.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import SourceInventory, TableDef, TableSelection
from dsql_migrator.core.table_selection import TableSelector, TableSelectionError


def _inventory(*names: str) -> SourceInventory:
    return SourceInventory(tables=[TableDef(name=name) for name in names])


def test_single_selection_resolves_to_matching_table() -> None:
    inv = _inventory("app.orders", "app.users")
    result = TableSelector().resolve(inv, TableSelection(selected_tables=["app.users"]))
    assert [t.name for t in result] == ["app.users"]


def test_multi_selection_resolves_to_matching_tables() -> None:
    inv = _inventory("app.orders", "app.users", "app.items")
    result = TableSelector().resolve(
        inv, TableSelection(selected_tables=["app.items", "app.orders"])
    )
    # Inventory order is preserved, not selection order.
    assert [t.name for t in result] == ["app.orders", "app.items"]


def test_empty_selection_infers_select_all() -> None:
    inv = _inventory("app.orders", "app.users")
    result = TableSelector().resolve(inv, TableSelection())
    assert [t.name for t in result] == ["app.orders", "app.users"]


def test_unknown_names_raise_with_offending_names() -> None:
    inv = _inventory("app.orders")
    with pytest.raises(TableSelectionError) as exc_info:
        TableSelector().resolve(
            inv, TableSelection(selected_tables=["app.orders", "app.missing", "x.y"])
        )
    assert exc_info.value.unknown_tables == ["app.missing", "x.y"]


def test_duplicate_selection_is_deduplicated() -> None:
    inv = _inventory("app.orders", "app.users")
    result = TableSelector().resolve(
        inv, TableSelection(selected_tables=["app.users", "app.users"])
    )
    assert [t.name for t in result] == ["app.users"]
