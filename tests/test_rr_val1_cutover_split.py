# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/cutover.py, the Cut over step: ui/validation.py re-exports all of it, and the two
modules import in either order.

Cut over was split out of ui/validation.py (VAL-1). Validation re-exports every name Cut over
defines, private ones too, and imports it at its END; Cut over reads Validation's per-session
state and the helpers both steps share, and imports them at ITS end, after its own
definitions.

Pinned here:

* the re-export: every top-level definition of ui/cutover.py, its two poll-interval constants
  included, is the very same object through ui/validation.py. The E2E harness reads some of
  them through validation, ``_cutover_fk_targets`` and ``_fk_validation_view_of`` among them,
  and no other test reads those two there: without this check, dropping either from the
  re-export would leave the suite green;
* the import order: either module can be imported first. Moving either import to the top of
  its module would make the other order fail on a partly initialised module, and nothing else
  in this session would see it, because both modules are already imported here: each check
  runs in a fresh interpreter.
"""

import ast
import pathlib
import subprocess
import sys

import pytest


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


def test_validation_re_exports_every_name_cut_over_defines() -> None:
    import dsql_migrator.ui.cutover as cutover
    import dsql_migrator.ui.validation as validation

    defined = _defined_names(pathlib.Path(cutover.__file__))
    # Not a vacuous scan: the screen, the two poll intervals (constants, which no __module__
    # would find) and the two names only the harness reads through validation are among them.
    assert {
        "build_cutover_screen",
        "_FK_APPLY_POLL_SECONDS",
        "_FK_VALIDATION_REFRESH_SECONDS",
        "_cutover_fk_targets",
        "_fk_validation_view_of",
    } <= set(defined)
    absent = object()
    not_re_exported = [
        name for name in defined if getattr(validation, name, absent) is not getattr(cutover, name)
    ]
    assert not_re_exported == [], f"ui.validation does not re-export {not_re_exported}"


@pytest.mark.parametrize("first", ["dsql_migrator.ui.cutover", "dsql_migrator.ui.validation"])
def test_either_module_imports_first_in_a_fresh_interpreter(first: str) -> None:
    code = (
        f"import importlib; importlib.import_module({first!r}); "
        "import dsql_migrator.ui.cutover as c, dsql_migrator.ui.validation as v; "
        "assert v.build_cutover_screen is c.build_cutover_screen; "
        "assert c.ValidationState is v.ValidationState; print('ok')"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "ok"
