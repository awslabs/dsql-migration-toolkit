"""CI-07: the column-exclusion overlay is core/exclusion_overlay.py, not the applier's tail.

The overlay (DROP NOT NULL / DROP DEFAULT on an excluded column) and the cut-over restore
of a dropped DEFAULT were the last 450 lines of core/schema_applier.py, whose business is
applying converted DDL. They are their own module now, and the dependency is ONE-WAY: the
overlay asks the applier for the reconnecting unit runner and the SchemaApplyError base,
and the applier imports nothing of the overlay -- so there is no cycle and no re-export
shim (nothing outside the repo binds these names). The import check runs in a fresh
interpreter, because this session has already imported the applier.
"""

from __future__ import annotations

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import exclusion_overlay as overlay
from dsql_migrator.core import schema_applier

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_APPLIER = _SRC / "core" / "schema_applier.py"
_OVERLAY = _SRC / "core" / "exclusion_overlay.py"

_MOVED = (
    "OverlayRefusedError",
    "OverlayVerifyError",
    "OverlayStatement",
    "OverlayOutcome",
    "RestoreOutcome",
    "build_overlay_statements",
    "ddl_column_facts",
    "apply_exclusion_overlay",
    "is_single_sql_expression",
    "restore_column_default",
    "restore_default_statement",
)


def test_every_overlay_name_lives_in_the_new_module_only() -> None:
    for name in _MOVED:
        assert hasattr(overlay, name), name
        assert not hasattr(schema_applier, name), name


def test_the_applier_does_not_import_the_overlay_back() -> None:
    tree = ast.parse(_APPLIER.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            assert node.module != "dsql_migrator.core.exclusion_overlay", node.lineno
            assert not (
                node.module == "dsql_migrator.core"
                and any(alias.name == "exclusion_overlay" for alias in node.names)
            ), node.lineno
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert "exclusion_overlay" not in alias.name, node.lineno


def test_the_overlay_imports_on_its_own() -> None:
    code = "import dsql_migrator.core.exclusion_overlay as o; print(o.OverlayStatement.__name__)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "OverlayStatement"


def test_the_overlay_errors_are_still_schema_apply_errors() -> None:
    # Callers that catch SchemaApplyError must keep catching a refused overlay.
    for error in (overlay.OverlayRefusedError, overlay.OverlayVerifyError):
        assert issubclass(error, schema_applier.SchemaApplyError)


def test_what_the_applier_keeps() -> None:
    for name in ("SchemaApplier", "recreate_table", "apply_foreign_key", "_run_unit_reconnecting"):
        assert hasattr(schema_applier, name), name
    # No stale export on either side.
    for module in (schema_applier, overlay):
        for name in getattr(module, "__all__", ()):
            assert hasattr(module, name), f"{module.__name__}.__all__ lists missing {name}"
