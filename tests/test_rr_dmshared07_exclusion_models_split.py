# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/data_migration/_exclusion_models.py, the column-exclusion read-models (dmshared-07):
_models re-exports all of it, it imports nothing from the package at its top, and the
session/engine helpers stay in _models.

Pinned here:

* the re-export: every top-level definition of _exclusion_models.py is the very same object
  through _models. The E2E harness binds several of them there (``effective_lob_exclusions``
  by signature, ``cdc_capture_exclusions``, ``lob_exclusion_candidates``, ...), and 55 test
  files import from there;
* its top-level imports are the standard library and ``core`` only: ``_models`` imports this
  module at its top, so a top-level import of ``_models`` (or the package) here would be a
  cycle. The two ``_models`` names it reads are imported inside the functions that read them;
* the engine rule and its neighbours (``session_source_type``, ``session_has_seeder_lambda``,
  ``effective_target_ddls``, ``cutover_restore_block_reason``) and the migration-type
  anchor are still defined in _models, not misfiled with the exclusions;
* no NiceGUI: no ``ui`` name and no ``nicegui`` import.
"""

import ast
import pathlib


def _tree():
    from dsql_migrator.ui.data_migration import _exclusion_models

    return ast.parse(pathlib.Path(_exclusion_models.__file__).read_text(encoding="utf-8"))


def test_models_re_exports_every_name_the_exclusion_module_defines() -> None:
    from dsql_migrator.ui.data_migration import _exclusion_models, _models

    defined: "list[str]" = []
    for node in _tree().body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.append(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            defined += [
                n.id
                for target in targets
                for n in ast.walk(target)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
            ]
    assert {
        "LobExclusionCandidate",
        "lob_exclusion_candidates",
        "make_lob_candidates_for",
        "effective_lob_exclusions",
        "classify_restored_exclusions",
        "exclusion_row_summary",
        "QUARANTINE_RULE_COMMENT",
        "cdc_capture_exclusions",
        "capture_ambiguity_warning",
    } <= set(defined)
    absent = object()
    not_re_exported = [
        name for name in defined
        if getattr(_models, name, absent) is not getattr(_exclusion_models, name)
    ]
    assert not_re_exported == [], f"_models does not re-export {not_re_exported}"


def test_the_exclusion_module_imports_nothing_from_the_package_at_its_top() -> None:
    top = [n for n in _tree().body if isinstance(n, (ast.Import, ast.ImportFrom))]
    modules = sorted({n.module for n in top if isinstance(n, ast.ImportFrom)})
    assert modules == [
        "__future__", "dataclasses", "dsql_migrator.core.assessor", "dsql_migrator.core.models",
        "typing",
    ]
    assert not [n for n in top if isinstance(n, ast.Import)]
    # The two _models names, imported inside the functions that read them.
    lazy = [
        node for node in ast.walk(_tree())
        if isinstance(node, ast.ImportFrom)
        and node.module == "dsql_migrator.ui.data_migration._models"
    ]
    assert sorted(alias.name for node in lazy for alias in node.names) == [
        "MigrationType", "session_source_type"
    ]
    assert all(node.col_offset > 0 for node in lazy)


def test_the_engine_rule_and_its_neighbours_stay_in_models() -> None:
    from dsql_migrator.ui.data_migration import _models

    for name in (
        "session_source_type",
        "session_has_seeder_lambda",
        "effective_target_ddls",
        "session_applied_conversions",
        "cutover_restore_block_reason",
        "MigrationType",
    ):
        assert getattr(_models, name).__module__ == _models.__name__, name
    assert "MIGRATION_TYPE_ANCHOR" in _models.__dict__
    assert "def make_lob_candidates_for(" not in pathlib.Path(_models.__file__).read_text(
        encoding="utf-8"
    )


def test_the_exclusion_module_makes_no_nicegui_call() -> None:
    tree = _tree()
    assert not [n.lineno for n in ast.walk(tree) if isinstance(n, ast.Name) and n.id == "ui"]
    assert "nicegui" not in ast.unparse(tree)
