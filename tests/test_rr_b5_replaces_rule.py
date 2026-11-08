# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""One spelling of "this run DROPs and recreates the table": DataMigrationInputs.replaces.

A table is replaced only when the user confirmed loading it fresh (``replace_tables``) and
CDC is not streaming into the target (``cdc_coexisting``): a DROP+recreate would race the
live sink. The rule used to be hand-written at seven sites of the Full Load engine; one
forgotten copy would re-open that race. Every site now asks ``inputs.replaces(name)``,
except ``_thread_load_recreated``, which keeps a getattr spelling for the thread path's
test doubles (they carry only the two fields).

Doubles only: no source, no target.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from dsql_migrator.core.models import (
    SourceConnectionConfig,
    SourceInventory,
    TargetConnectionConfig,
)
from dsql_migrator.ui.data_migration._full_load_engine import DataMigrationInputs

_ENGINE = (
    Path(__file__).resolve().parents[1]
    / "src" / "dsql_migrator" / "ui" / "data_migration" / "_full_load_engine.py"
)


def _inputs(*, replace: frozenset[str], coexisting: bool) -> DataMigrationInputs:
    return DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=None,
        target_config=TargetConnectionConfig(
            cluster_endpoint="c.dsql.example", region="us-east-1"
        ),
        inventory=SourceInventory(tables=[]),
        replace_tables=replace,
        cdc_coexisting=coexisting,
    )


@pytest.mark.parametrize(
    ("replace", "coexisting", "expected"),
    [
        (frozenset({"app.t"}), False, True),
        (frozenset({"app.t"}), True, False),  # never beside a streaming sink
        (frozenset({"app.u"}), False, False),
        (frozenset(), False, False),
    ],
)
def test_replaces_is_the_confirmed_choice_unless_cdc_streams(
    replace: frozenset[str], coexisting: bool, expected: bool
) -> None:
    assert _inputs(replace=replace, coexisting=coexisting).replaces("app.t") is expected


def _reads_replace_tables(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr == "replace_tables":
            return True
        if isinstance(sub, ast.Constant) and sub.value == "replace_tables":
            return True  # the getattr spelling
    return False


def _reads_cdc_coexisting(node: ast.AST) -> bool:
    for sub in ast.walk(node):
        if isinstance(sub, ast.Attribute) and sub.attr == "cdc_coexisting":
            return True
        if isinstance(sub, ast.Constant) and sub.value == "cdc_coexisting":
            return True
        if isinstance(sub, ast.Name) and sub.id == "_coexisting":
            return True
    return False


def test_the_rule_is_spelled_out_only_in_replaces_and_the_thread_double_guard() -> None:
    """A membership test of ``replace_tables`` ANDed with a ``cdc_coexisting`` read is the
    rule itself; it may appear only in ``replaces`` and ``_thread_load_recreated``. (The
    run-level ``cdc_coexisting or not replace_tables`` check is not a per-table copy: it
    has no membership test.)"""
    tree = ast.parse(_ENGINE.read_text(encoding="utf-8"))
    sites: list[str] = []
    for func in ast.walk(tree):
        if not isinstance(func, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for node in ast.walk(func):
            if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And)):
                continue
            has_member = any(
                isinstance(v, ast.Compare)
                and any(isinstance(op, ast.In) for op in v.ops)
                and _reads_replace_tables(v)
                for v in node.values
            )
            has_cdc = any(
                isinstance(v, ast.UnaryOp)
                and isinstance(v.op, ast.Not)
                and _reads_cdc_coexisting(v)
                for v in node.values
            )
            if has_member and has_cdc:
                sites.append(func.name)
    # Nested functions are walked by their parents too: compare the set.
    assert set(sites) == {"replaces", "_thread_load_recreated"}, sites
