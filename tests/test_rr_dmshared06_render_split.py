# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/data_migration/_selection_ui.py and _prereq_ui.py: the table picker and the
prerequisites panel, split out of the package ``__init__`` (dmshared-06), which re-exports
every name both define.

Pinned here:

* the re-export: every top-level definition of the two modules, private ones and constants
  included, is the very same object through the package. The E2E harness reads four of them
  as ``dm.<name>`` (``migratable_table_names``, ``target_existing_table_names``,
  ``default_migration_selection``, ``effective_migration_selection``), and no test reads
  several of the private ones there (``_split_migration_schema``, ``_render_gapless_choice``,
  ``_PREREQ_STATUS_COLORS``, ...): without this check, dropping one from the re-export would
  leave the suite green;
* the one-directional imports both docstrings state: neither module imports the package
  ``__init__``, or ``_cdc_ui``, which imports names back from it -- at its top or inside a
  function;
* which moved names the package's own code reads: the only calls a patch on ``dm.<name>``
  reaches. The screen code calls the picker, the panel, the default and effective selection
  and the migratable / target-existing table names, and the suite patches the picker and the
  panel there (``_render_table_selection``, ``_render_prerequisites_panel``) to drive the
  screen. Every other moved name is read only inside the two modules, so a test patches it
  there. A new read in the package, or one that moves into a submodule, fails here until the
  patches that rely on it are checked.
"""

import ast
import pathlib
import symtable

_PACKAGE = "dsql_migrator.ui.data_migration"
_MODULES = ("_selection_ui", "_prereq_ui")


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


def _moved_names() -> "dict[str, list[str]]":
    import importlib

    return {
        module: _defined_names(
            pathlib.Path(importlib.import_module(f"{_PACKAGE}.{module}").__file__)
        )
        for module in _MODULES
    }


def test_the_package_re_exports_every_name_the_two_modules_define() -> None:
    import importlib

    import dsql_migrator.ui.data_migration as dm

    moved = _moved_names()
    # Not a vacuous scan: the harness's four, the private helpers and the constants are there.
    assert {
        "migratable_table_names",
        "target_existing_table_names",
        "default_migration_selection",
        "effective_migration_selection",
        "_split_migration_schema",
        "_render_table_selection",
    } <= set(moved["_selection_ui"])
    assert {
        "_CDC_ONLY_CHECK_IDS",
        "PrereqPhaseVerdict",
        "_PREREQ_STATUS_COLORS",
        "_render_gapless_choice",
        "_render_prerequisites_panel",
    } <= set(moved["_prereq_ui"])
    absent = object()
    not_re_exported = [
        f"{module}.{name}"
        for module, names in moved.items()
        for name in names
        if getattr(dm, name, absent)
        is not getattr(importlib.import_module(f"{_PACKAGE}.{module}"), name)
    ]
    assert not_re_exported == [], f"the package does not re-export {not_re_exported}"


def test_neither_module_imports_the_package_or_cdc_ui() -> None:
    import importlib

    banned = {_PACKAGE, f"{_PACKAGE}._cdc_ui"}
    offenders: "list[str]" = []
    for module in _MODULES:
        path = pathlib.Path(importlib.import_module(f"{_PACKAGE}.{module}").__file__)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom):
                targets = {node.module or ""}
                if node.level:  # from . import x / from ._cdc_ui import x
                    targets = {"<relative>"}
                elif node.module == _PACKAGE:
                    targets |= {f"{_PACKAGE}.{alias.name}" for alias in node.names}
            elif isinstance(node, ast.Import):
                targets = {alias.name for alias in node.names}
            else:
                continue
            if targets & (banned | {"<relative>"}):
                offenders.append(f"{module}:{node.lineno}: {ast.unparse(node)}")
    assert offenders == [], offenders


def test_the_package_code_reads_only_these_moved_names() -> None:
    import dsql_migrator.ui.data_migration as dm

    moved = {name for names in _moved_names().values() for name in names}
    path = pathlib.Path(dm.__file__)
    table = symtable.symtable(path.read_text(encoding="utf-8"), str(path), "exec")
    reads = {  # module-level code: the re-export binds the names, it does not read them
        symbol.get_name()
        for symbol in table.get_symbols()
        if symbol.get_name() in moved and symbol.is_referenced()
    }
    scopes = list(table.get_children())
    while scopes:
        scope = scopes.pop()
        scopes.extend(scope.get_children())
        reads |= {
            symbol.get_name()
            for symbol in scope.get_symbols()
            if symbol.get_name() in moved and symbol.is_referenced() and symbol.is_global()
        }
    assert reads == {
        "_render_table_selection",
        "default_migration_selection",
        "effective_migration_selection",
        "migratable_table_names",
        "target_existing_table_names",
        "_render_prerequisites_panel",
    }, f"the package's own reads of moved names changed: {sorted(reads)}"
