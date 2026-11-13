"""FL-11: every change to the index-build record lives in _index_builds.

The run submits the builds and the step watches them, but the record itself
(``MigrationJob.index_builds``) is changed in ONE module: the void of a recreated table's
builds, the merge of newly submitted ones, the data-loaded stamp and the fold of the read
the run takes before the load now sit beside the fold the watch uses. _full_load_engine
calls each on its job handle and re-exports all four, so its own lambdas and the tests keep
their names; _index_builds must not import the engine back.
"""

from __future__ import annotations

import ast
import pathlib

from dsql_migrator.ui.data_migration import _full_load_engine as engine
from dsql_migrator.ui.data_migration import _index_builds as ib

_MOVED = (
    "_void_index_builds",
    "_merge_index_builds",
    "_stamp_data_loaded",
    "_fold_builds_before_load",
)
_SRC = pathlib.Path(ib.__file__)
_TREE = ast.parse(_SRC.read_text(encoding="utf-8"))


def _defined(tree: ast.Module) -> "set[str]":
    return {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    }


def test_the_record_transitions_are_defined_here() -> None:
    defined = _defined(_TREE)
    for name in (*_MOVED, "fold_index_build_reads"):
        assert name in defined, name


def test_the_engine_re_exports_each_under_its_own_name() -> None:
    for name in _MOVED:
        assert getattr(engine, name) is getattr(ib, name), name
    # And defines none of them any more: one definition, one place to patch.
    engine_tree = ast.parse(pathlib.Path(engine.__file__).read_text(encoding="utf-8"))
    assert _defined(engine_tree).isdisjoint(_MOVED)


def test_it_does_not_import_the_engine_back() -> None:
    for node in ast.walk(_TREE):
        if isinstance(node, ast.ImportFrom):
            assert node.module != "dsql_migrator.ui.data_migration._full_load_engine", (
                node.lineno
            )
            assert not (
                node.module == "dsql_migrator.ui.data_migration"
                and any(alias.name == "_full_load_engine" for alias in node.names)
            ), node.lineno
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "_full_load_engine" not in alias.name, node.lineno


def test_what_needs_the_run_stays_in_the_engine() -> None:
    # The handle wrapper (it logs on the engine's own logger), the record of one submitted
    # CREATE INDEX (it parses the DDL's name) and the bounded pre-load wait are the run's.
    for name in (
        "_void_recorded_index_builds",
        "_index_build_of",
        "_wait_for_index_builds_before_load",
    ):
        assert hasattr(engine, name), name
        assert not hasattr(ib, name), name
