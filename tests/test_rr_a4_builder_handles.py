# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Everything the Data Migration builder attaches to the state survives Start over.

``build_data_migration_screen`` attaches handles to the per-session state ONCE (config
values, providers, callbacks, the session binding, the exclusion-inputs resolver), and
the builder does not re-run after Start over. ``DataMigrationStore.reset_in_place``
re-runs ``__init__`` on the same object, so it has to carry each handle across -- and
the two lists it used to keep by hand each missed one (the secret's CMK; the teardown
queue). The handles now live in ONE tuple, ``_BUILDER_HANDLES``, and these tests derive
the builder's attachments from its source, so a handle added without the tuple fails.
"""

from __future__ import annotations

import ast
import inspect
import textwrap

from dsql_migrator.ui import data_migration
from dsql_migrator.ui.data_migration import _state
from dsql_migrator.ui.data_migration._state import DataMigrationState, DataMigrationStore


def _build_scope(node: ast.AST):
    """The nodes the builder runs at BUILD time: nested functions and lambdas run later."""
    stack = list(ast.iter_child_nodes(node))
    while stack:
        child = stack.pop()
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda, ast.ClassDef)):
            continue
        yield child
        stack.extend(ast.iter_child_nodes(child))


def _self_assignments(method: object) -> set[str]:
    """The ``self.<attr>`` a DataMigrationState method assigns."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(method)))
    return {
        target.attr
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        for target in (node.targets if isinstance(node, ast.Assign) else [node.target])
        if isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id == "self"
    }


def _builder_attachments() -> set[str]:
    """Every state attribute the builder sets at build scope: ``migration_state.<a> = ...``
    and whatever a ``migration_state.<method>(...)`` call there assigns."""
    tree = ast.parse(textwrap.dedent(inspect.getsource(data_migration.build_data_migration_screen)))
    builder = tree.body[0]
    attached: set[str] = set()
    for node in _build_scope(builder):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            for target in node.targets if isinstance(node, ast.Assign) else [node.target]:
                if (
                    isinstance(target, ast.Attribute)
                    and isinstance(target.value, ast.Name)
                    and target.value.id == "migration_state"
                ):
                    attached.add(target.attr)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "migration_state"
        ):
            attached |= _self_assignments(getattr(DataMigrationState, node.func.attr))
    return attached


def test_the_scan_sees_the_builders_known_attachments() -> None:
    # Guard against a vacuous scan: an assignment, a registration call and a binding.
    attached = _builder_attachments()
    assert {"cdc_deploy_role_arn", "_exclusion_inputs_resolver", "_session"} <= attached


def test_every_builder_attachment_survives_start_over() -> None:
    store = DataMigrationStore()
    state = store.get_or_create("s1")
    sentinels = {name: object() for name in sorted(_builder_attachments())}
    for name, value in sentinels.items():
        setattr(state, name, value)
    state.job_id = "JOB-1"  # user state, which Start over must drop

    store.reset_in_place("s1")

    assert store.get("s1") is state  # the same object the builder's closures hold
    assert state.job_id is None
    lost = sorted(name for name, value in sentinels.items() if getattr(state, name) is not value)
    assert lost == []


def test_the_handle_tuple_is_exactly_what_the_builder_attaches() -> None:
    # One list: a new builder handle must join it, and a removed one must leave it.
    assert len(_state._BUILDER_HANDLES) == len(set(_state._BUILDER_HANDLES))
    assert sorted(_state._BUILDER_HANDLES) == sorted(_builder_attachments())
