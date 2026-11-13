# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""core/sql_text.py, the quote-aware SQL text scan, imports on its own (CONV-4).

It was split out of core/converter.py, and the converter imports it back to re-export every
name, private ones too. So it must not import the converter: if it did, importing it before
the converter (the source dialects, column exclusion and the Schema Conversion screen take
the scan from it) would fail on a partly initialised module. The view ordering that reads
it, ``views_in_dependency_order``, stays in the converter. The import check runs in a fresh
interpreter, because this test session has already imported the converter."""

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import converter, sql_text

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"


def _defined(tree: ast.Module) -> "list[str]":
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
    return names


_TREE = ast.parse((_SRC / "core" / "sql_text.py").read_text(encoding="utf-8"))
_NAMES = _defined(_TREE)


def test_imports_without_loading_the_schema_converter() -> None:
    code = (
        "import sys, dsql_migrator.core.sql_text; "
        "print('dsql_migrator.core.converter' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False", out.stdout


def test_it_imports_nothing_of_the_converter() -> None:
    for node in ast.walk(_TREE):
        if isinstance(node, ast.ImportFrom):
            assert node.module != "dsql_migrator.core.converter", node.lineno
            assert not (
                node.module == "dsql_migrator.core"
                and any(alias.name == "converter" for alias in node.names)
            ), node.lineno


def test_the_converter_re_exports_every_name() -> None:
    assert len(_NAMES) == 20
    for name in _NAMES:
        assert getattr(converter, name) is getattr(sql_text, name), name


def test_the_view_ordering_stays_in_the_converter() -> None:
    assert converter.views_in_dependency_order.__module__ == "dsql_migrator.core.converter"
    assert "views_in_dependency_order" not in _NAMES


def test_the_tool_takes_the_scan_from_its_own_module() -> None:
    # The modules of the tool import the scan from core/sql_text.py, not through the
    # converter's re-export (which stays for the tests and anything outside the package).
    moved = set(_NAMES)
    for path in sorted(_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "dsql_migrator.core.converter":
                taken = {alias.name for alias in node.names} & moved
                assert not taken, (str(path.relative_to(_SRC)), node.lineno, sorted(taken))
