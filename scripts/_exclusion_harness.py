# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Column exclusions for the headless E2E harnesses (operational utility, NOT shipped).

``--exclude-columns table.col[,table.col...]`` on ``run_full_load_harness.py`` and
``run_pg_cdc_e2e.py`` excludes those columns the way the UI does: they are omitted from
the load (``excluded_lob_columns``) and made nullable with no DEFAULT on Aurora DSQL
before the first write (``overlay_columns``), with the dropped DEFAULTs recorded first in
an overlay ledger. The harness always builds a LOCAL SQLite ledger for that -- the engine
refuses an overlay with no ledger ("no overlay ledger configured").

``--exclude-rules FILE`` takes the popup's rule language instead (or as well): the file
is syntax-checked before any connection (:func:`check_rules_file`), then resolved against
the loaded tables through the SAME resolver and context as the UI
(:func:`resolve_rules_file`). Table rules are refused: a harness loads a fixed table set.
"""

from __future__ import annotations

import os
import tempfile
from typing import Iterable, Mapping, Optional, Sequence

DEFAULT_LEDGER_PATH = os.path.join(tempfile.gettempdir(), "dsql_e2e_overlay_ledger.sqlite")


def parse_exclude_columns(
    values: "Optional[Iterable[str]]", *, schema: Optional[str] = None
) -> "dict[str, frozenset[str]]":
    """``["orders.notes", "s.t.c,s.t.d"]`` -> ``{table: {columns}}``.

    The LAST dot separates the column; a bare table name is qualified with ``schema``
    (the harnesses load into ``<schema>.<table>``). Raises ``ValueError`` on an entry
    without a table part.
    """
    out: dict[str, set[str]] = {}
    for value in values or ():
        for raw in str(value).split(","):
            entry = raw.strip()
            if not entry:
                continue
            table, sep, column = entry.rpartition(".")
            if not sep or not table or not column:
                raise ValueError(f"--exclude-columns entry {entry!r}: expected table.column")
            if schema and "." not in table:
                table = f"{schema}.{table}"
            out.setdefault(table, set()).add(column)
    return {table: frozenset(columns) for table, columns in out.items()}


def _rule_errors(text: str, errors, global_errors=()) -> "list[str]":
    lines = (text or "").split("\n")
    out = []
    for number in sorted(errors):
        quoted = lines[number - 1].strip() if 0 < number <= len(lines) else ""
        for error in errors[number]:
            out.append(f'L{number} "{quoted}": {error.message}')
    out.extend(e.message for e in global_errors)
    return out


def read_rules_file(path: "Optional[str]") -> "Optional[str]":
    """The rules file's text, or ``None`` without one."""
    if not path:
        return None
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def check_rules_file(text: "Optional[str]") -> "list[str]":
    """Syntax problems (E1) of a rules text, naming each line: checked BEFORE any
    connection, so a typo never costs a source introspection."""
    if not text:
        return []
    from dsql_migrator.core.exclusion_rules import parse_rules

    _rules, errors = parse_rules(text)
    return _rule_errors(text, errors)


def resolve_rules_file(
    text: "Optional[str]", tables: Sequence, *, source_type=None, applied=None
) -> "dict[str, frozenset[str]]":
    """``{table: columns}`` the rules exclude over ``tables``, resolved exactly as the
    popup resolves them (the same core resolver and exclusion context: blocked columns
    are never excluded). Raises ``ValueError`` naming every problem; table rules are
    refused (the harness loads a fixed table set)."""
    if not text:
        return {}
    from dsql_migrator.core.column_exclusion import build_exclusion_context
    from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules
    from dsql_migrator.core.models import SourceInventory, SourceType

    tables = list(tables)
    inventory = SourceInventory(tables=tables)

    def factory(selected, focus):
        return build_exclusion_context(inventory, applied, selected_tables=selected,
                                       focus_tables=focus)

    resolution = resolve_exclusion_rules(
        text, migratable=tables, source_type=source_type or SourceType.MYSQL,
        effective_selection=[t.name for t in tables], context_factory=factory,
        applied_conversions=applied,
    )
    problems = _rule_errors(text, resolution.errors, resolution.global_errors)
    if resolution.selected_after is not None:
        problems.append("table rules are not supported here: the harness loads a fixed "
                        "table set")
    if problems:
        raise ValueError("; ".join(problems))
    return {t: frozenset(c) for t, c in resolution.columns.items()}


def merge_exclusions(*sets: "Mapping[str, Iterable[str]]") -> "dict[str, frozenset[str]]":
    out: dict[str, set[str]] = {}
    for one in sets:
        for table, columns in (one or {}).items():
            out.setdefault(table, set()).update(columns)
    return {t: frozenset(c) for t, c in out.items() if c}


def check_exclusions(excluded: "Mapping[str, Iterable[str]]", tables: Sequence) -> list[str]:
    """Problems with ``excluded`` against the resolved ``tables`` (empty = fine): an
    unknown table or column, or a primary-key column (a key is never excluded)."""
    by_name = {t.name: t for t in tables}
    problems: list[str] = []
    for table, columns in sorted(excluded.items()):
        tdef = by_name.get(table)
        if tdef is None:
            problems.append(f"{table}: not among the loaded tables")
            continue
        names = {c.name for c in tdef.columns}
        for column in sorted(columns):
            if column not in names:
                problems.append(f"{table}.{column}: no such column on the source")
            elif column in set(tdef.primary_key):
                problems.append(f"{table}.{column}: a primary-key column cannot be excluded")
    return problems


def local_overlay_ledger(path: str = DEFAULT_LEDGER_PATH):
    """The harness's own ledger (a local SQLite file; the UI uses S3 on Fargate)."""
    from dsql_migrator.core.overlay_ledger import SqliteOverlayLedger

    return SqliteOverlayLedger(path)


def overlay_input_fields(excluded: "Mapping[str, Iterable[str]]", ledger) -> dict:
    """The ``DataMigrationInputs`` fields an exclusion run needs ({} with none)."""
    if not excluded:
        return {}
    return {
        "excluded_lob_columns": {t: set(c) for t, c in excluded.items()},
        "overlay_columns": {t: frozenset(c) for t, c in excluded.items()},
        "overlay_ledger": ledger,
        "overlay_ledger_in_use": True,
    }


def cdc_column_exclude_list(
    excluded: "Mapping[str, Iterable[str]]", tables: Sequence
) -> "Optional[list[str]]":
    """The Debezium ``column.exclude.list`` entries for ``excluded`` (None with none),
    formatted exactly as the UI sends them."""
    if not excluded:
        return None
    from dsql_migrator.core.cdc import format_column_exclude_value, split_column_exclude_value

    value = format_column_exclude_value(
        {t: sorted(c) for t, c in excluded.items()}, tables
    )
    return split_column_exclude_value(value) or None


def _qualified(table: str) -> str:
    from psycopg import sql

    from dsql_migrator.core.validation_sql import _pg_table_identifier

    return _pg_table_identifier(table), sql


def assert_excluded_null(conn_dsql, table: str, column: str) -> int:
    """Target-only proof: no row holds a value in ``column`` (``count(col) == 0``).

    Returns the row count; raises ``AssertionError`` naming the non-NULL count."""
    ident, sql = _qualified(table)
    cur = conn_dsql.cursor()
    cur.execute(
        sql.SQL("SELECT COUNT(*), COUNT({c}) FROM {t}").format(c=sql.Identifier(column), t=ident)
    )
    rows, values = cur.fetchone()
    if values:
        raise AssertionError(f"{table}.{column}: {values} of {rows} row(s) hold a value (expected NULL)")
    return int(rows)


def assert_default_restored(conn_dsql, table: str, column: str, expected_expr: str) -> None:
    """After the cut-over restore: the live DEFAULT is the recorded original (pg_get_expr
    text) and the migrated rows still hold NULL (a DEFAULT affects new inserts only)."""
    from dsql_migrator.core.target_introspector import read_overlay_column_facts

    exists, facts = read_overlay_column_facts(conn_dsql, table, [column])
    live = facts.get(column) if exists else None
    if live is None:
        raise AssertionError(f"{table}.{column}: not on the Aurora DSQL table")
    if live.default_expr != expected_expr:
        raise AssertionError(
            f"{table}.{column}: DEFAULT {live.default_expr!r}, expected {expected_expr!r}"
        )
    assert_excluded_null(conn_dsql, table, column)


__all__ = [
    "DEFAULT_LEDGER_PATH",
    "assert_default_restored",
    "assert_excluded_null",
    "cdc_column_exclude_list",
    "check_exclusions",
    "check_rules_file",
    "local_overlay_ledger",
    "merge_exclusions",
    "overlay_input_fields",
    "parse_exclude_columns",
    "read_rules_file",
    "resolve_rules_file",
]
