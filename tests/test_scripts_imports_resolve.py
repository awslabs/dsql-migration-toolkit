# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Guard: every ``dsql_migrator`` name the scripts/ harnesses import still exists.

WHY THIS EXISTS: three documented, committed entry points --
``scripts/run_full_load.py``, ``scripts/run_fullload_resume_harness.py`` and
``scripts/verify_fullload_edgecases.py`` -- imported
``dsql_migrator.ui.data_migration._engine``, a private submodule that the v0.1.346-351
refactor split into ``_full_load_engine``. Every invocation of all three died at
``ModuleNotFoundError`` and had done so for weeks. Nothing caught it: the scripts put
their imports inside ``main()`` (so ``--help`` still worked), the package's own suite
never imports them, and ``tests/test_no_undefined_globals.py`` walks ``dsql_migrator``
only -- ``scripts/`` is outside the package.

HOW: parse each script with ``ast`` and, for every ``from dsql_migrator... import name``,
assert the module imports and carries the name. Static on the script side -- the scripts
are NEVER executed or imported, so nothing connects to a database and a harness's
top-level ``load_dotenv`` never runs -- while the target side is resolved for real, which
is what makes a moved or renamed symbol fail here.

This deliberately checks only ``dsql_migrator`` imports: third-party availability is the
lockfile's job, and a script's own sibling helpers (``_common``) are not importable under
the test's ``sys.path``.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"


def _script_files() -> list[Path]:
    return sorted(p for p in _SCRIPTS.glob("*.py") if p.name != "__init__.py")


def _package_imports(path: Path) -> list[tuple[int, str, tuple[str, ...]]]:
    """Return ``(lineno, module, names)`` for each ``from dsql_migrator... import ...``."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    out: list[tuple[int, str, tuple[str, ...]]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level:
            continue  # relative imports are the script's own siblings
        module = node.module or ""
        if module != "dsql_migrator" and not module.startswith("dsql_migrator."):
            continue
        names = tuple(a.name for a in node.names if a.name != "*")
        out.append((node.lineno, module, names))
    return out


def test_the_scripts_directory_is_actually_being_scanned():
    # Without this, a path change would turn the whole guard into a silent no-op that
    # passes by iterating nothing -- the failure mode that let the broken imports ship.
    # The floor is deliberately well below the tracked count (11 at the time of writing):
    # several scripts in this directory are local-only (gitignored), so a fresh clone sees
    # fewer, and this assertion must fail only when the SCAN breaks, not when a script goes.
    files = _script_files()
    assert len(files) >= 8, f"expected the harness scripts, found {files}"
    assert any(p.name == "run_full_load.py" for p in files)
    assert any(
        _package_imports(p) for p in files
    ), "no dsql_migrator imports found at all -- the AST scan is not working"


@pytest.mark.parametrize("script", _script_files(), ids=lambda p: p.name)
def test_every_dsql_migrator_import_in_a_script_resolves(script: Path):
    problems: list[str] = []
    for lineno, module, names in _package_imports(script):
        try:
            mod = importlib.import_module(module)
        except Exception as exc:  # noqa: BLE001 - report, do not abort the whole file
            problems.append(f"{script.name}:{lineno} cannot import {module}: {exc}")
            continue
        for name in names:
            if hasattr(mod, name):
                continue
            # ``from pkg import submodule`` is legitimate even when the parent package
            # does not re-export it as an attribute.
            try:
                importlib.import_module(f"{module}.{name}")
            except Exception:  # noqa: BLE001
                problems.append(
                    f"{script.name}:{lineno} {module} has no attribute {name!r}"
                )
    assert not problems, "\n".join(problems)
