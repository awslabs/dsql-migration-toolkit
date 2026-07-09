# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Table selection for the Data Migration sub-flow (multi-table).

The Data Migration sub-flow (Prerequisites Check -> Full Load | CDC) operates on
a user-chosen set of tables. :class:`TableSelector` normalizes a
:class:`~dsql_migrator.core.models.TableSelection` against the introspected
:class:`~dsql_migrator.core.models.SourceInventory` into concrete
:class:`~dsql_migrator.core.models.TableDef` objects, so the same resolved
selection flows identically into prerequisite checks, Full Load, and CDC
(Property 16 / Requirement 5.9).

Behavior (design "Table selection (multi-table)"):

- **Inferred "select all" default**: an empty selection resolves to every table
  in the inventory, so the common case takes no clicks (Usability-first).
- **Validated, never silently dropped**: unknown table names raise
  :class:`TableSelectionError` listing exactly which names were not found, so
  untrusted input is rejected rather than ignored (Requirement 9.4).
- **Order preserved**: resolved tables keep the inventory's order for
  deterministic downstream behavior.

This module is pure (no I/O): it only matches names already present in an
inventory produced by read-only introspection.
"""

from __future__ import annotations

from dsql_migrator.core.models import SourceInventory, TableDef, TableSelection


class TableSelectionError(ValueError):
    """Raised when a selection references table names absent from the inventory.

    Carries the offending names on :attr:`unknown_tables` (in the order the user
    supplied them) so the UI can report exactly what to fix instead of silently
    dropping unknown entries (Requirement 9.4).
    """

    def __init__(self, unknown_tables: list[str]) -> None:
        self.unknown_tables = list(unknown_tables)
        joined = ", ".join(self.unknown_tables)
        super().__init__(f"Unknown table(s) not found in the source inventory: {joined}")


class TableSelector:
    """Resolves a :class:`TableSelection` to concrete inventory tables."""

    def resolve(
        self, inventory: SourceInventory, selection: TableSelection
    ) -> list[TableDef]:
        """Resolve ``selection`` against ``inventory`` to a list of tables.

        - Empty ``selection.selected_tables`` returns all ``inventory.tables``
          (inferred "select all"; fewer clicks).
        - Otherwise returns the inventory tables whose names match the selection,
          in **inventory order** (deterministic), de-duplicating repeated
          selections.
        - Any selected name not present in the inventory raises
          :class:`TableSelectionError` listing the unknown names (untrusted input
          is validated, never silently dropped — Requirement 9.4).
        """
        if not selection.selected_tables:
            return list(inventory.tables)

        by_name = {table.name: table for table in inventory.tables}

        unknown = [
            name for name in selection.selected_tables if name not in by_name
        ]
        if unknown:
            # Preserve user order and de-duplicate the reported unknown names.
            seen: set[str] = set()
            ordered_unknown = [
                name for name in unknown if not (name in seen or seen.add(name))
            ]
            raise TableSelectionError(ordered_unknown)

        selected = set(selection.selected_tables)
        # Inventory order, de-duplicated by construction (one TableDef per name).
        return [table for table in inventory.tables if table.name in selected]


__all__ = ["TableSelector", "TableSelectionError"]
