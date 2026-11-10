# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/data_migration/_table_set_lock.py, the column-exclusion and table-set locks:
_cdc_monitoring re-exports all of it, it makes no NiceGUI call, and its patch notes name
every reader by how a patch reaches it.

The lock block was split out of _cdc_monitoring.py (cdcui-5), whose docstring called the
module "Rendering only" while this block made no NiceGUI call at all. _cdc_monitoring
re-exports every name the new module defines, private ones too.

Pinned here:

* the re-export: every top-level definition of _table_set_lock.py, its constants and the
  ``_JOB_UNSET`` sentinel included, is the very same object through _cdc_monitoring. The E2E
  harness imports ``cdc_stack_being_deleted`` through _cdc_monitoring, and no test reads it
  there: without this check, dropping it from the re-export would leave the suite green;
* the module docstring's claim: no ``ui`` name and no ``nicegui`` import anywhere in the
  module, so a render cannot move in unnoticed;
* the patch notes: which src modules a patch on _table_set_lock reaches (those that import
  from it inside a function, at call time) and which it does not (those that bind its names
  with a module-level import), as its docstring lists them, and the one lock name
  _cdc_monitoring's own code reads (``exclusion_lock_state``, in ``_exclusion_state_facts``),
  the read its note says a test patches there. A new reader, or one that moves its import,
  fails here until the notes say how a patch reaches it.
"""

import ast
import pathlib
import symtable

_PACKAGE = "dsql_migrator.ui.data_migration"
_LOCK_MODULE = f"{_PACKAGE}._table_set_lock"


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


def _inside_functions(tree: ast.AST) -> "set[int]":
    """The ids of the nodes inside a function or lambda: an import there runs at call time."""
    return {
        id(child)
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda))
        for child in ast.walk(node)
        if child is not node
    }


def test_the_patch_note_lists_every_src_reader_by_where_it_imports() -> None:
    import dsql_migrator

    src = pathlib.Path(dsql_migrator.__file__).parent
    at_call: "set[str]" = set()
    at_load: "set[str]" = set()
    unclassified: "list[str]" = []
    for path in sorted(src.rglob("*.py")):
        module = ".".join(path.relative_to(src.parent).with_suffix("").parts)
        tree = ast.parse(path.read_text(encoding="utf-8"))
        in_function = _inside_functions(tree)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == _LOCK_MODULE:
                (at_call if id(node) in in_function else at_load).add(module)
            elif (
                isinstance(node, ast.ImportFrom)
                and node.module == _PACKAGE
                and any(alias.name == "_table_set_lock" for alias in node.names)
            ) or (
                isinstance(node, ast.Import)
                and any(alias.name == _LOCK_MODULE for alias in node.names)
            ):
                # A module object read at call time: neither list covers it yet.
                unclassified.append(f"{module}:{node.lineno}")
    # Not a vacuous scan: the walk reached the package and the readers outside it.
    assert f"{_PACKAGE}._cdc_ui" in at_load and "dsql_migrator.ui.validation" in at_call
    assert at_call == {
        f"{_PACKAGE}._models",
        f"{_PACKAGE}._full_load_ui",
        "dsql_migrator.ui.validation",
    }, f"update _table_set_lock's patch note (a patch there reaches these): {sorted(at_call)}"
    assert at_load == {
        f"{_PACKAGE}._exclusion_dialog",
        f"{_PACKAGE}._cdc_ui",
        f"{_PACKAGE}._selection_ui",
        f"{_PACKAGE}.__init__",
        f"{_PACKAGE}._cdc_monitoring",
    }, f"update _table_set_lock's patch note (a patch there misses these): {sorted(at_load)}"
    assert unclassified == [], unclassified


def test_cdc_monitoring_reads_one_lock_name_itself() -> None:
    from dsql_migrator.ui.data_migration import _cdc_monitoring, _table_set_lock

    defined = set(_defined_names(pathlib.Path(_table_set_lock.__file__)))
    path = pathlib.Path(_cdc_monitoring.__file__)
    table = symtable.symtable(path.read_text(encoding="utf-8"), str(path), "exec")
    reads = {
        ("<module>", symbol.get_name())
        for symbol in table.get_symbols()
        if symbol.get_name() in defined and symbol.is_referenced()
    }
    scopes = list(table.get_children())
    while scopes:
        scope = scopes.pop()
        scopes.extend(scope.get_children())
        reads |= {
            (scope.get_name(), symbol.get_name())
            for symbol in scope.get_symbols()
            if symbol.get_name() in defined and symbol.is_referenced() and symbol.is_global()
        }
    assert reads == {("_exclusion_state_facts", "exclusion_lock_state")}, (
        f"update _cdc_monitoring's patch note (its own reads of lock names): {sorted(reads)}"
    )
