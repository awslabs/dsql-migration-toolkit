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
  function, by any import statement: ``import ...data_migration``, ``from ...data_migration
  import <name>``, a relative import, or the parent-package ``from dsql_migrator.ui import
  data_migration``, the E2E harness's own spelling -- nor reads either off an ancestor it
  imports, through an attribute chain: ``import dsql_migrator.ui`` then
  ``dsql_migrator.ui.data_migration.<name>``, ``from dsql_migrator import ui`` then
  ``ui.data_migration.<name>``, ``import dsql_migrator`` then
  ``dsql_migrator.ui.data_migration...``. A self-test feeds the scan each spelling, so a
  scan that stops seeing one fails;
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


def _back_imports(source: str) -> "list[str]":
    """The places anywhere in ``source`` that import the package or ``_cdc_ui``, or reach
    either through an attribute chain.

    ``from M import a`` reads ``a`` off ``M`` or loads the submodule ``M.a``, so ``M`` and
    ``M.a`` are both its targets: ``from dsql_migrator.ui import data_migration`` imports
    the package as ``import dsql_migrator.ui.data_migration`` does. Any relative import
    counts (both modules import absolutely).

    An import of an ancestor reaches the package without naming it: ``import
    dsql_migrator.ui`` then ``dsql_migrator.ui.data_migration.<name>``, ``from dsql_migrator
    import ui`` then ``ui.data_migration.<name>``, and any ``import dsql_migrator...``, which
    binds ``dsql_migrator``. So each name an import binds is followed through the attribute
    chains read off it, and a chain that passes through the package or ``_cdc_ui`` counts.
    """
    banned = {_PACKAGE, f"{_PACKAGE}._cdc_ui"}
    tree = ast.parse(source)
    found: "list[str]" = []
    bound: "dict[str, set[str]]" = {}  # a name an import binds -> the module paths it names
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.level:  # from . import x / from ._cdc_ui import x
                found.append(f"{node.lineno}: {ast.unparse(node)}")
                continue
            targets = {node.module} | {f"{node.module}.{alias.name}" for alias in node.names}
            for alias in node.names:
                bound.setdefault(alias.asname or alias.name, set()).add(
                    f"{node.module}.{alias.name}"
                )
        elif isinstance(node, ast.Import):
            targets = {alias.name for alias in node.names}
            for alias in node.names:
                # import a.b.c binds a, to a; import a.b.c as x binds x, to a.b.c
                top = alias.name.split(".")[0]
                bound.setdefault(alias.asname or top, set()).add(
                    alias.name if alias.asname else top
                )
        else:
            continue
        if targets & banned:
            found.append(f"{node.lineno}: {ast.unparse(node)}")
    for node in ast.walk(tree):
        # Every Attribute is checked, prefixes included: a.b.c's prefix a.b is its own node.
        if not isinstance(node, ast.Attribute):
            continue
        attrs, base = [], node
        while isinstance(base, ast.Attribute):
            attrs.insert(0, base.attr)
            base = base.value
        if isinstance(base, ast.Name) and any(
            ".".join([path, *attrs]) in banned for path in bound.get(base.id, ())
        ):
            found.append(f"{node.lineno}: {ast.unparse(node)}")
    return found


def test_neither_module_imports_the_package_or_cdc_ui() -> None:
    import importlib

    offenders = [
        f"{module}:{statement}"
        for module in _MODULES
        for statement in _back_imports(
            pathlib.Path(importlib.import_module(f"{_PACKAGE}.{module}").__file__).read_text(
                encoding="utf-8"
            )
        )
    ]
    assert offenders == [], offenders


def test_the_import_scan_catches_every_spelling_of_a_back_import() -> None:
    # The scan is only as good as the spellings it knows. Each of these imports the package
    # or _cdc_ui, or is relative, or imports an ancestor and reads the package or _cdc_ui off
    # it, and must be caught at a module's top and inside a function; the kinds of import the
    # two modules make, a sibling's parent-package spelling and a chain to another module
    # must not be.
    back_imports = [
        "import dsql_migrator.ui.data_migration",
        "import dsql_migrator.ui.data_migration as dm",
        "from dsql_migrator.ui import data_migration",
        "from dsql_migrator.ui import data_migration as dm",
        "from dsql_migrator.ui.data_migration import build_data_migration_screen",
        "from dsql_migrator.ui.data_migration import _cdc_ui",
        "from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_attach_row",
        "import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui",
        "from . import _cdc_ui",
        "from ._models import keep_unlisted_covered",
        # An ancestor, then an attribute chain through the package or _cdc_ui: the import
        # names neither, and any import dsql_migrator.<module> binds dsql_migrator.
        "import dsql_migrator.ui\ndsql_migrator.ui.data_migration.build_data_migration_screen()",
        "from dsql_migrator import ui\nui.data_migration.build_data_migration_screen()",
        "import dsql_migrator\n"
        "dsql_migrator.ui.data_migration._cdc_ui._render_cdc_attach_row()",
        "import dsql_migrator.ui as u\nu.data_migration.build_data_migration_screen()",
        "import dsql_migrator.ui.design\n"
        "dsql_migrator.ui.data_migration.build_data_migration_screen()",
    ]
    allowed = [
        "from dsql_migrator.ui.data_migration._models import keep_unlisted_covered",
        "from dsql_migrator.ui.data_migration._table_set_lock import LOCK_TIP_RULE",
        "from dsql_migrator.ui.design import render_notice",
        "from dsql_migrator.ui import design",
        "import dsql_migrator.ui.design\ndsql_migrator.ui.design.render_notice()",
        "from dsql_migrator import ui\nui.design.render_notice()",
        "import dsql_migrator\ndsql_migrator.core.models.SourceType",
    ]

    def placements(snippet: str) -> "list[str]":
        """At a module's top and inside a function; an import and a read, also split: the
        import at the top, the read inside a function."""
        lines = snippet.split("\n")
        indented = "".join(f"    {line}\n" for line in lines)
        out = [snippet, f"def f():\n{indented}"]
        if len(lines) == 2:
            out.append(f"{lines[0]}\ndef f():\n    {lines[1]}\n")
        return out

    missed = [
        statement
        for statement in back_imports
        for source in placements(statement)
        if not _back_imports(source)
    ]
    flagged = [
        statement
        for statement in allowed
        for source in placements(statement)
        if _back_imports(source)
    ]
    assert missed == [] and flagged == [], (missed, flagged)


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
