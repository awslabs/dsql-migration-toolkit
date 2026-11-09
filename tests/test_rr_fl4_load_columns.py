# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""FL-4: every Full Load path reads and writes one column set, decided by ``_load_columns``.

The shard worker (``_migrate_shard_in_process``) drives the exporter and importer itself
rather than calling ``BatchedTableMigrator.migrate_table``, so it carried its own copy of
the front of ``migrate_table``. That drift shipped a bug once: the shard path forgot the
operator's oversized-LOB column exclusions, and "Exclude column & reload" was a no-op on
exactly the tables big enough to be sharded.

The column decision -- drop the operator's excluded LOB columns, then the columns the
target computes, never a primary-key column, keeping the source order -- is now the one
pure ``_load_columns``. Each path keeps its own ``resolve_generated_columns`` call (the
thread / whole-table path trusts the applied DDL for a final target; a shard always reads
the live catalog) and its own refusal wording when that is unknown.

Pinned here:

* the helper's rule, case by case;
* parity: the thread path (``migrate_table``), the whole-table process worker and the
  shard worker hand the exporter's SELECT and the importer's INSERT the same column list
  for the same table and inputs -- the lists the engine produced before the refactor;
* structure: both preludes call the helper after their own generated-column resolution,
  and nothing else in the engine calls ``apply_lob_exclusions``.

Doubles only: no source, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import ast
import dataclasses
import inspect
import textwrap

import pytest

from dsql_migrator.core.batched_import import BatchedImportResult
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    DataMigrationInputs,
)

# The real class, captured before any test patches the module global for a worker.
_REAL_MIGRATOR = BatchedTableMigrator


def _col(name: str, mysql_type: str = "text", **kw) -> ColumnDef:
    return ColumnDef(name=name, mysql_type=mysql_type, **kw)


DOCS = TableDef(
    name="docs",
    columns=[
        _col("id", "int", nullable=False),
        _col("title", "varchar(200)"),
        _col("body", "longtext"),
        _col("note"),
    ],
    primary_key=["id"],
)

# A PostgreSQL STORED generated column, which the converter preserves for Aurora DSQL.
ITEMS = TableDef(
    name="order_items",
    columns=[
        _col("id", "bigint", nullable=False),
        _col("quantity", "integer", nullable=False),
        _col(
            "line_total",
            "numeric(12,2)",
            generated=True,
            generated_kind="STORED",
            generated_expression="((quantity)::numeric * 2)",
        ),
        _col("comment"),
    ],
    primary_key=["id"],
)

# A composite source key: neither key column may ever leave the load.
TENANT_ROWS = TableDef(
    name="tenant_rows",
    columns=[
        _col("tenant_id", "int", nullable=False),
        _col("id", "int", nullable=False),
        _col("payload", "longtext"),
        _col("extra"),
    ],
    primary_key=["tenant_id", "id"],
)

# A MySQL VIRTUAL generated column: an ordinary column on the target (its MySQL expression
# is display-only), so it loads like any other.
MYSQL_GEN = TableDef(
    name="mysql_gen",
    columns=[
        _col("id", "int", nullable=False),
        _col("a", "int"),
        _col(
            "b",
            "int",
            generated=True,
            generated_kind="VIRTUAL",
            mysql_generated_expression="(`a` * 2)",
        ),
    ],
    primary_key=["id"],
)


def _pg_conversion(table: TableDef):
    conversion = SchemaConverter(source_type=SourceType.POSTGRES).convert_table(table)
    assert "GENERATED ALWAYS AS" in conversion.target_ddl  # the preserved clause
    return conversion


def _mysql_conversion(table: TableDef):
    return SchemaConverter(source_type=SourceType.MYSQL).convert_table(table)


def _inputs(table: TableDef, *, excluded=(), conversion=None, replace=False) -> DataMigrationInputs:
    return DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=None,
        target_config=TargetConnectionConfig(
            cluster_endpoint="cluster.dsql.example", region="us-east-1"
        ),
        inventory=SourceInventory(tables=[table]),
        table_conversions={table.name: conversion} if conversion is not None else {},
        excluded_lob_columns={table.name: frozenset(excluded)} if excluded else {},
        replace_tables=frozenset({table.name}) if replace else frozenset(),
    )


class _Exporter:
    """Records the column list of every row stream it opens (the keyset SELECT)."""

    def __init__(self) -> None:
        self.columns: list[list[str]] = []

    def plan_pk_shard_ranges(self, conn, table, shards, *, min_rows=0):
        return [(None, None)]

    def stream_converted_rows(self, conn, table, **_kw):
        self.columns.append([c.name for c in table.columns])
        return [{"id": 1}]


class _Importer:
    """Records the column list every import_rows call writes (the INSERT list)."""

    def __init__(self) -> None:
        self.columns: list[list[str]] = []

    def import_rows(self, rows, table, **_kw):
        self.columns.append([c.name for c in table.columns])
        loaded = len(list(rows))
        return BatchedImportResult(rows_loaded=loaded, failures=0)


def _migrator(inputs, exporter, importer, live_generated):
    return _REAL_MIGRATOR(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=object(),  # type: ignore[arg-type]
        importer_factory=lambda _i: importer,  # type: ignore[arg-type,return-value]
        target_counter=lambda _t: None,
        # The LIVE catalog's generated columns (the append path and every shard read it).
        target_generated_reader=lambda _t: set(live_generated),
    )


def _thread_path(inputs, table, *, live_generated, pre_recreated):
    exporter, importer = _Exporter(), _Importer()
    _migrator(inputs, exporter, importer, live_generated).migrate_table(
        table, pre_recreated=pre_recreated
    )
    return exporter.columns, importer.columns


def _patch_worker_migrator(monkeypatch, exporter, importer, live_generated) -> None:
    monkeypatch.setattr(engine, "_worker_progress_queue", None)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setattr(
        engine,
        "BatchedTableMigrator",
        lambda inp: _migrator(inp, exporter, importer, live_generated),
    )


def _table_worker_path(monkeypatch, inputs, table, *, live_generated, pre_recreated):
    exporter, importer = _Exporter(), _Importer()
    _patch_worker_migrator(monkeypatch, exporter, importer, live_generated)
    result = engine._migrate_one_table_in_process(
        engine._TableWorkerArgs(
            job_id="j",
            table=table,
            inputs=engine._slim_worker_inputs(inputs, table.name),
            pre_recreated=pre_recreated,
        )
    )
    assert result.status == "DONE", result.error_message
    return exporter.columns, importer.columns


def _shard_path(monkeypatch, inputs, table, *, live_generated):
    exporter, importer = _Exporter(), _Importer()
    _patch_worker_migrator(monkeypatch, exporter, importer, live_generated)
    result = engine._migrate_shard_in_process(
        engine._ShardWorkerArgs(
            job_id="j",
            table=table,
            inputs=engine._slim_worker_inputs(inputs, table.name),
            pk_lower=1,
            pk_upper=1000,
            shard_index=0,
        )
    )
    assert result.status == "DONE", result.error_message
    return exporter.columns, importer.columns


# (case id, table, excluded LOB columns, applied conversion, live generated columns,
#  replace (the parent pre-recreated the target), the column list every path loads).
# The expected lists are what the engine produced before FL-4 (the tests passed on it).
_PARITY_CASES = [
    ("no_exclusions", DOCS, (), None, set(), False, ["id", "title", "body", "note"]),
    ("lob_excluded_keeps_order", DOCS, ("body",), None, set(), False, ["id", "title", "note"]),
    ("lob_never_drops_the_key", DOCS, ("id", "body"), None, set(), False, ["id", "title", "note"]),
    ("lob_unknown_column_ignored", DOCS, ("gone",), None, set(), False,
     ["id", "title", "body", "note"]),
    ("generated_omitted", ITEMS, (), "pg", {"line_total"}, False, ["id", "quantity", "comment"]),
    ("generated_and_lob", ITEMS, ("comment",), "pg", {"line_total"}, False, ["id", "quantity"]),
    # Append onto a target whose column is ORDINARY (an older build stripped the clause):
    # the live catalog wins, so the column is loaded rather than nulled.
    ("live_target_not_generated", ITEMS, (), "pg", set(), False,
     ["id", "quantity", "line_total", "comment"]),
    # A replace the parent pre-recreated: the thread / whole-table path trusts the applied
    # DDL, the shard reads the live catalog -- both say line_total.
    ("replace_generated_and_lob", ITEMS, ("comment",), "pg", {"line_total"}, True,
     ["id", "quantity"]),
    ("composite_key_kept", TENANT_ROWS, ("tenant_id", "payload"), None, set(), False,
     ["tenant_id", "id", "extra"]),
    ("mysql_generated_is_ordinary", MYSQL_GEN, (), "mysql", set(), False, ["id", "a", "b"]),
    ("mysql_generated_excluded", MYSQL_GEN, ("b",), "mysql", set(), False, ["id", "a"]),
]


@pytest.mark.parametrize(
    "table, excluded, conversion, live_generated, replace, expected",
    [case[1:] for case in _PARITY_CASES],
    ids=[case[0] for case in _PARITY_CASES],
)
def test_every_full_load_path_loads_the_same_columns(
    monkeypatch, table, excluded, conversion, live_generated, replace, expected
) -> None:
    applied = {"pg": _pg_conversion, "mysql": _mysql_conversion, None: lambda _t: None}[
        conversion
    ](table)
    inputs = _inputs(table, excluded=excluded, conversion=applied, replace=replace)
    source_columns = [c.name for c in table.columns]

    thread = _thread_path(inputs, table, live_generated=live_generated, pre_recreated=replace)
    whole = _table_worker_path(
        monkeypatch, inputs, table, live_generated=live_generated, pre_recreated=replace
    )
    shard = _shard_path(monkeypatch, inputs, table, live_generated=live_generated)

    for read, written in (thread, whole, shard):
        assert read == [expected]  # the keyset SELECT
        assert written == [expected]  # the INSERT column list
    # The table handed in is never filtered in place (a recreate needs every column).
    assert [c.name for c in table.columns] == source_columns


# ---------------------------------------------------------------------------
# The helper's rule
# ---------------------------------------------------------------------------


def _names(table: TableDef) -> list[str]:
    return [c.name for c in table.columns]


def test_nothing_to_drop_returns_the_table_itself() -> None:
    inputs = _inputs(DOCS)
    assert engine._load_columns(inputs, DOCS, set()) is DOCS  # the common case: no copy
    assert engine._load_columns(inputs, DOCS, None) is DOCS


def test_the_excluded_lob_columns_are_dropped_in_source_order() -> None:
    loaded = engine._load_columns(_inputs(DOCS, excluded=("title", "note")), DOCS, set())
    assert _names(loaded) == ["id", "body"]
    assert loaded.name == DOCS.name and loaded.primary_key == DOCS.primary_key
    assert _names(DOCS) == ["id", "title", "body", "note"]  # the input is left whole


def test_the_exclusions_of_another_table_do_not_apply() -> None:
    inputs = dataclasses.replace(
        _inputs(DOCS), excluded_lob_columns={"other": frozenset({"body"})}
    )
    assert engine._load_columns(inputs, DOCS, set()) is DOCS


def test_a_key_column_is_never_dropped() -> None:
    inputs = _inputs(TENANT_ROWS, excluded=("tenant_id", "payload"))
    loaded = engine._load_columns(inputs, TENANT_ROWS, {"id", "extra"})
    assert _names(loaded) == ["tenant_id", "id"]
    assert loaded.primary_key == ["tenant_id", "id"]


def test_the_generated_columns_are_dropped_with_the_exclusions() -> None:
    assert _names(engine._load_columns(_inputs(ITEMS), ITEMS, {"line_total"})) == [
        "id", "quantity", "comment",
    ]
    loaded = engine._load_columns(_inputs(ITEMS, excluded=("comment",)), ITEMS, {"line_total"})
    assert _names(loaded) == ["id", "quantity"]
    # A generated column the source does not have is ignored.
    assert _names(engine._load_columns(_inputs(DOCS), DOCS, {"gone"})) == _names(DOCS)


@pytest.mark.parametrize("source_type", [SourceType.MYSQL, SourceType.POSTGRES])
def test_the_rule_does_not_depend_on_the_source_engine(source_type) -> None:
    inputs = dataclasses.replace(
        _inputs(ITEMS, excluded=("comment",)),
        source_config=SourceConnectionConfig(
            host="db", database="app", source_type=source_type
        ),
    )
    assert _names(engine._load_columns(inputs, ITEMS, {"line_total"})) == ["id", "quantity"]


# ---------------------------------------------------------------------------
# Structure: one decision, called by both preludes after their own resolution
# ---------------------------------------------------------------------------


def _calls(function_node: ast.AST, name: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(function_node)
        if isinstance(node, ast.Call)
        and (getattr(node.func, "id", None) == name or getattr(node.func, "attr", None) == name)
    ]


def _function_node(function) -> ast.AST:
    return ast.parse(textwrap.dedent(inspect.getsource(function))).body[0]


@pytest.mark.parametrize(
    "prelude",
    [BatchedTableMigrator.migrate_table, engine._migrate_shard_in_process],
    ids=["migrate_table", "shard_worker"],
)
def test_each_prelude_calls_the_helper_once_after_its_own_generated_resolution(prelude) -> None:
    node = _function_node(prelude)
    (helper_call,) = _calls(node, "_load_columns")
    (resolve_call,) = _calls(node, "resolve_generated_columns")
    assert resolve_call.lineno < helper_call.lineno
    # Handed the resolved set and the UNFILTERED table.
    assert ast.unparse(helper_call.args[2]) == "generated_columns"
    assert ast.unparse(helper_call.args[1]) in {"original_table", "args.table"}
    assert _calls(node, "apply_lob_exclusions") == []


def test_only_the_helper_applies_column_exclusions_in_the_engine() -> None:
    tree = ast.parse(inspect.getsource(engine))
    callers = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and _calls(node, "apply_lob_exclusions")
    }
    assert callers == {"_load_columns"}
