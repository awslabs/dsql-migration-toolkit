# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Column exclusions for the headless E2E harnesses (operational utility, NOT shipped).

``--exclude-columns table.col[,table.col...]`` on ``run_full_load_harness.py`` and
``run_pg_cdc_e2e.py`` excludes those columns the way the UI does: they are omitted from
the load (``excluded_lob_columns``) and made nullable with no DEFAULT on Aurora DSQL
before the first write (``overlay_columns``), with the dropped DEFAULTs recorded first in
an overlay ledger. The harness always builds a LOCAL SQLite ledger for that -- the engine
refuses an overlay with no ledger ("no overlay ledger configured").

The rule-based grammar (``--exclude-rules``) arrives with P3; this is the plain list.
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
    "local_overlay_ledger",
    "overlay_input_fields",
    "parse_exclude_columns",
]
