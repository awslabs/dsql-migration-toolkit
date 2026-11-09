# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/cutover.py, the Cut over step, imports in either order with ui/validation.py.

Cut over was split out of ui/validation.py (VAL-1). Validation re-exports every name Cut over
defines and imports it at its END; Cut over reads Validation's per-session state and the
helpers both steps share, and imports them at ITS end, after its own definitions. So either
module can be imported first. Moving either import to the top of its module would make the
other order fail on a partly initialised module, and nothing else in this session would see
it, because both modules are already imported here: each check runs in a fresh interpreter."""

import subprocess
import sys

import pytest


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
