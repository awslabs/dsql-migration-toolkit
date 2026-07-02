"""Regression (snapshot-style) tests over a representative MySQL corpus.

These tests pin the *deterministic* conversion output of the engine so that any
unintended change to schema or query conversion is caught as a snapshot diff.
They require no external infrastructure and run by default (Task 14).

Inputs (the corpus) live under ``tests/fixtures/corpus/``:

- ``source_inventory.json`` -- a representative
  :class:`~dsql_migrator.core.models.SourceInventory` exercising the implemented
  type mappings and DSQL constraints (``TINYINT(1)`` -> boolean, ``UNSIGNED``
  widening, ``ENUM``/``SET`` -> text, ``DATETIME`` -> timestamp, ``BLOB`` ->
  bytea, ``JSON``, foreign-key removal, ``CREATE INDEX ASYNC``, a table with no
  primary key, and ``AUTO_INCREMENT`` hot-partition handling).
- ``queries.json`` -- a representative set of MySQL DML/SELECT statements
  (``ON DUPLICATE KEY UPDATE`` -> ``ON CONFLICT``, ``FOR UPDATE`` anti-patterns,
  MySQL function/``LIMIT`` rewrites).

Expected outputs (snapshots) live under ``tests/fixtures/snapshots/``. Each test
renders the current output and compares it byte-for-byte to the committed
snapshot. To regenerate after an intentional change, run with
``DSQL_MIGRATOR_UPDATE_SNAPSHOTS=1`` and review the diff before committing.

This mechanism intentionally adds no snapshot-testing dependency: it is plain
file read/compare, consistent with the project's minimal-dependency principle.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import SourceInventory
from dsql_migrator.core.query_converter import QueryConverter

_FIXTURES = Path(__file__).parent / "fixtures"
_CORPUS = _FIXTURES / "corpus"
_SNAPSHOTS = _FIXTURES / "snapshots"

# When set, snapshots are (re)written instead of compared. Use after an
# intentional conversion change, then review and commit the updated snapshot.
_UPDATE = os.environ.get("DSQL_MIGRATOR_UPDATE_SNAPSHOTS") == "1"


def _assert_matches_snapshot(name: str, actual: str) -> None:
    """Compare ``actual`` to the committed snapshot ``name`` (or update it).

    On ``DSQL_MIGRATOR_UPDATE_SNAPSHOTS=1`` the snapshot file is written and the
    test is skipped; otherwise the snapshot must already exist and match exactly.
    """
    snapshot_path = _SNAPSHOTS / name
    if _UPDATE:
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(actual, encoding="utf-8")
        pytest.skip(f"updated snapshot {name}")
    assert snapshot_path.exists(), (
        f"missing snapshot {snapshot_path}; regenerate with "
        "DSQL_MIGRATOR_UPDATE_SNAPSHOTS=1"
    )
    expected = snapshot_path.read_text(encoding="utf-8")
    assert actual == expected, (
        f"conversion output drifted from snapshot {name}; if intentional, "
        "regenerate with DSQL_MIGRATOR_UPDATE_SNAPSHOTS=1"
    )


def _load_inventory() -> SourceInventory:
    """Load the representative source inventory corpus."""
    raw = (_CORPUS / "source_inventory.json").read_text(encoding="utf-8")
    return SourceInventory.model_validate_json(raw)


def _load_queries() -> list[str]:
    """Load the representative query corpus."""
    raw = (_CORPUS / "queries.json").read_text(encoding="utf-8")
    return json.loads(raw)


def _render_schema_conversion() -> str:
    """Render the full schema-conversion output (script + warnings) as text."""
    result = SchemaConverter().convert(_load_inventory())

    lines: list[str] = ["-- DDL script", "", result.to_script(), "", "-- Warnings"]
    for warning in result.warnings:
        target = warning.object_name
        column = f".{warning.column_name}" if warning.column_name else ""
        lines.append(
            f"[{warning.classification.value}] {target}{column}: {warning.message}"
        )
    return "\n".join(lines) + "\n"


def _render_query_conversion() -> str:
    """Render the full query-conversion output for the corpus as text."""
    converter = QueryConverter()
    blocks: list[str] = []
    for index, sql in enumerate(_load_queries()):
        result = converter.convert(sql)
        block = [
            f"# query {index}",
            f"original:  {result.original_sql}",
            f"converted: {result.converted_sql}",
            f"class:     {result.classification.value}",
        ]
        for warning in result.warnings:
            block.append(f"warning:   [{warning.code}] {warning.message}")
        blocks.append("\n".join(block))
    return "\n\n".join(blocks) + "\n"


def test_schema_conversion_matches_snapshot() -> None:
    """Schema conversion output is stable for the representative corpus."""
    _assert_matches_snapshot("schema_conversion.txt", _render_schema_conversion())


def test_query_conversion_matches_snapshot() -> None:
    """Query conversion output is stable for the representative corpus."""
    _assert_matches_snapshot("query_conversion.txt", _render_query_conversion())


def test_schema_conversion_is_deterministic() -> None:
    """Converting the same inventory twice yields identical output."""
    assert _render_schema_conversion() == _render_schema_conversion()
