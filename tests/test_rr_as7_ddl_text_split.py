# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""AS-7 step 2: the DDL-text reading is core/ddl_text.py, on its own.

It was split out of ui/schema_conversion_apply.py, which re-exports every name defined
there -- private ones too -- so each existing import keeps resolving (``schema_conversion``
re-exports that module in turn, and the live harness imports ``identity_from_ddl`` from it).
It reads SQL text and nothing else: no UI module, no apply contract, no connection -- and so
it must not import the apply module back, which would be a cycle.
"""

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import ddl_text
from dsql_migrator.ui import schema_conversion as sc
from dsql_migrator.ui import schema_conversion_apply as sca

_PATH = pathlib.Path(ddl_text.__file__)
_TREE = ast.parse(_PATH.read_text(encoding="utf-8"))


def _defined(tree: ast.Module) -> "list[str]":
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names.extend(t.id for t in node.targets if isinstance(t, ast.Name))
    return names


_NAMES = _defined(_TREE)


def test_it_holds_the_whole_ddl_text_group() -> None:
    assert len(_NAMES) > 50
    for name in (
        "split_sql_statements",
        "split_glued_statements",
        "split_target_ddl_fk_sections",
        "join_target_ddl_fk_sections",
        "statement_label",
        "object_statement_role",
        "object_create_kind",
        "is_object_scaffolding",
        "merge_object_ddl",
        "edited_statements",
        "identity_from_ddl",
        "composite_leading_from_ddl",
        "_describe_ddl",
        "_key_definition",
        "_is_foreign_key_ddl",
    ):
        assert name in _NAMES, name


def test_every_name_is_re_exported_as_the_same_object() -> None:
    """A consumer that imports any of them from the old module gets the one function."""
    missing = [n for n in _NAMES if not hasattr(sca, n)]
    assert missing == [], missing
    assert [n for n in _NAMES if getattr(sca, n) is not getattr(ddl_text, n)] == []
    # ...and the names the screen module re-exports in turn are the same object too.
    through_screen = [n for n in _NAMES if hasattr(sc, n)]
    assert "split_sql_statements" in through_screen and "identity_from_ddl" in through_screen
    assert [n for n in through_screen if getattr(sc, n) is not getattr(ddl_text, n)] == []


def test_the_apply_module_defines_none_of_them_any_more() -> None:
    apply_tree = ast.parse(pathlib.Path(sca.__file__).read_text(encoding="utf-8"))
    assert set(_defined(apply_tree)) & set(_NAMES) == set()


def test_it_reads_sql_text_only_and_never_the_apply_module() -> None:
    for node in ast.walk(_TREE):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            module = getattr(node, "module", None) or ""
            assert not module.startswith("dsql_migrator.ui"), (module, node.lineno)
            assert "schema_conversion" not in module, (module, node.lineno)


def test_it_imports_without_the_apply_module_or_nicegui() -> None:
    # This session has already imported the apply module, so ask a fresh interpreter.
    code = (
        "import sys, dsql_migrator.core.ddl_text; "
        "print('dsql_migrator.ui.schema_conversion_apply' in sys.modules, "
        "'nicegui' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False False", out.stdout
