# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""core/assessment_report.py, the assessment report's export forms, imports on its own (A8).

It was split out of core/assessor.py, which imports it back to re-export every name,
private ones too -- so it must not import the assessor at import time: that would be a
cycle, and importing the report module before the assessor would fail on a partly
initialised module. The HTML chart's kind label is the assessor's, imported inside the
function that draws it. The import check runs in a fresh interpreter, because this test
session has already imported the assessor.
"""

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import assessment_report as report_mod
from dsql_migrator.core import assessor

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_PATH = _SRC / "core" / "assessment_report.py"
_TREE = ast.parse(_PATH.read_text(encoding="utf-8"))


def _defined(tree: ast.Module) -> "list[str]":
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
    return names


_NAMES = _defined(_TREE)


def test_imports_without_loading_the_assessor() -> None:
    code = (
        "import sys, dsql_migrator.core.assessment_report; "
        "print('dsql_migrator.core.assessor' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False", out.stdout


def test_it_imports_nothing_of_the_assessor_at_module_level() -> None:
    for node in _TREE.body:
        if isinstance(node, ast.ImportFrom):
            assert node.module != "dsql_migrator.core.assessor", node.lineno
            assert not (
                node.module == "dsql_migrator.core"
                and any(alias.name == "assessor" for alias in node.names)
            ), node.lineno


def test_the_assessor_re_exports_every_name() -> None:
    assert len(_NAMES) == 13
    for name in _NAMES:
        assert getattr(assessor, name) is getattr(report_mod, name), name


def test_the_rule_engine_stays_in_the_assessor() -> None:
    for name in ("CompatibilityAssessor", "default_rules", "kind_label", "KIND_LABELS"):
        assert hasattr(assessor, name), name
        assert not hasattr(report_mod, name), name
