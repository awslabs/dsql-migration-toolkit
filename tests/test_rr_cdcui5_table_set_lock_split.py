# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/data_migration/_table_set_lock.py, the column-exclusion and table-set locks:
_cdc_monitoring re-exports all of it, and it makes no NiceGUI call.

The lock block was split out of _cdc_monitoring.py (cdcui-5), whose docstring called the
module "Rendering only" while this block made no NiceGUI call at all. _cdc_monitoring
re-exports every name the new module defines, private ones too.

Pinned here:

* the re-export: every top-level definition of _table_set_lock.py, its constants and the
  ``_JOB_UNSET`` sentinel included, is the very same object through _cdc_monitoring. The E2E
  harness imports ``cdc_stack_being_deleted`` through _cdc_monitoring, and no test reads it
  there: without this check, dropping it from the re-export would leave the suite green;
* the module docstring's claim: no ``ui`` name and no ``nicegui`` import anywhere in the
  module, so a render cannot move in unnoticed.
"""

import ast
import pathlib


def _defined_names(path: pathlib.Path) -> "list[str]":
    """The names a module's top level defines (not imports), in source order."""
    names: "list[str]" = []
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names += [
                n.id
                for target in targets
                for n in ast.walk(target)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
            ]
    return names


def test_cdc_monitoring_re_exports_every_name_the_lock_module_defines() -> None:
    from dsql_migrator.ui.data_migration import _cdc_monitoring, _table_set_lock

    defined = _defined_names(pathlib.Path(_table_set_lock.__file__))
    # Not a vacuous scan: the lock, its texts and sentinel (which no __module__ would find)
    # and the name the harness reads through _cdc_monitoring are among them.
    assert {
        "exclusion_lock_state",
        "TableSetAddLock",
        "TABLE_SET_WAIT_LOAD",
        "_JOB_UNSET",
        "_EXCLUSION_BAKED_PHASES",
        "cdc_stack_being_deleted",
    } <= set(defined)
    absent = object()
    not_re_exported = [
        name
        for name in defined
        if getattr(_cdc_monitoring, name, absent) is not getattr(_table_set_lock, name)
    ]
    assert not_re_exported == [], f"_cdc_monitoring does not re-export {not_re_exported}"


def test_the_lock_module_makes_no_nicegui_call() -> None:
    from dsql_migrator.ui.data_migration import _table_set_lock

    tree = ast.parse(pathlib.Path(_table_set_lock.__file__).read_text(encoding="utf-8"))
    ui_names = [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id == "ui"]
    nicegui = [
        n.lineno
        for n in ast.walk(tree)
        if isinstance(n, ast.ImportFrom) and (n.module or "").split(".")[0] == "nicegui"
        or isinstance(n, ast.Import) and any(a.name.split(".")[0] == "nicegui" for a in n.names)
    ]
    assert ui_names == [] and nicegui == [], (ui_names, nicegui)
