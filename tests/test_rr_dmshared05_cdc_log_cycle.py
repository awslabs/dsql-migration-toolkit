# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""dmshared-05: no module of ui/data_migration imports the package ``__init__`` at its top
level any more, so the bottom placement of ``__init__``'s own submodule imports is no
longer load-bearing.

``_cdc_ui`` -- which ``__init__`` imports at its bottom to re-export -- used to import four
names back from it, and ``_cdc_monitoring`` one. The activity-log anchor moved to the leaf
``_cdc_log`` (which ``__init__`` re-exports, so a patch on ``dm._log_cdc_event`` still
reaches ``__init__``'s own callers), the two loggers are bound by package name, the notice
renderer by its design-system alias, and the one import that was never used is gone.
"""

import ast
import logging
import pathlib
import subprocess
import sys

import dsql_migrator.ui.data_migration as dm
from dsql_migrator.ui import design
from dsql_migrator.ui.data_migration import _cdc_log, _cdc_monitoring, _cdc_ui

_PKG = "dsql_migrator.ui.data_migration"
_DIR = pathlib.Path(dm.__file__).resolve().parent


def _top_level_package_imports(path: pathlib.Path) -> "list[int]":
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return [
        node.lineno
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and getattr(node, "module", None) == _PKG
    ]


def test_no_submodule_imports_the_package_at_import_time() -> None:
    offenders = {
        path.name: lines
        for path in sorted(_DIR.glob("*.py"))
        if path.name != "__init__.py"
        and (lines := _top_level_package_imports(path))
    }
    assert offenders == {}, (
        "a top-level import back into the package makes the order of __init__'s own "
        f"submodule imports load-bearing: {offenders}"
    )


def test_the_leaf_log_module_imports_nothing_of_the_package() -> None:
    tree = ast.parse(pathlib.Path(_cdc_log.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert not (node.module or "").startswith(_PKG), node.lineno
    code = (
        "import sys, dsql_migrator.ui.data_migration._cdc_log as m; "
        "print(callable(m._log_cdc_event))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "True", out.stdout


def test_the_rebound_names_are_the_same_objects_as_before() -> None:
    # The package re-exports the anchor, so every caller still calls one function.
    assert _cdc_ui._log_cdc_event is dm._log_cdc_event is _cdc_log._log_cdc_event
    # getLogger(__package__) IS the package's logger: the same object, the same name, so
    # every line lands in the same place with the same logger name as before.
    assert _cdc_ui._LOGGER is dm._LOGGER is _cdc_monitoring._LOGGER
    assert dm._LOGGER is logging.getLogger(_PKG)
    assert _cdc_ui._render_notice is dm._render_notice is design.render_notice


def test_the_unused_lock_helper_import_is_gone() -> None:
    assert not hasattr(_cdc_ui, "migration_type_lock_reason")  # never called there
    assert callable(dm.migration_type_lock_reason)  # still the package's, unchanged
