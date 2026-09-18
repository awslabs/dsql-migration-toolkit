# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Guard: no function references a global name that does not exist.

WHY THIS EXISTS (a shipped blocker): v0.1.448 added a call to
``lob_exclusion_lock(...)`` in ``ui/data_migration/__init__.py`` without importing it.
The module still IMPORTED cleanly -- a missing global is only raised when the function
RUNS -- so nothing caught it: the unit suite passed, the image was published to three
registries, both live app stacks were updated, and the Data Migration step then failed to
render with ``NameError`` for every user. The regression test written for that change used
``inspect.getsource``, which reads source TEXT and therefore passes with the import
missing.

That is the SECOND time in two releases that a test exercised a helper but not its call
site (v0.1.446's own commit message recorded the first). So this checks the call sites
themselves, mechanically, for the whole package.

HOW: after importing each module, every function's bytecode is scanned for ``LOAD_GLOBAL``
(the opcode that reads a module-level/builtin name). Each such name must resolve in the
module's namespace or in builtins. This catches the entire NameError/missing-import class
-- including nested functions and comprehensions -- with no NiceGUI double, no rendering,
and no new dependency, and it is deterministic (no flaky UI harness).

Names bound INSIDE a function (a function-local ``import x``, a parameter, an assignment)
compile to ``LOAD_FAST``/``LOAD_DEREF``, not ``LOAD_GLOBAL``, so the repo's deliberate
lazy-import pattern is unaffected.
"""

from __future__ import annotations

import builtins
import dis
import importlib
import pkgutil
import types

import pytest

import dsql_migrator


def _iter_code_objects(code: types.CodeType):
    """Yield ``code`` and every nested code object (inner defs, lambdas, comprehensions)."""
    yield code
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            yield from _iter_code_objects(const)


def _all_modules() -> list[str]:
    names = []
    for mod in pkgutil.walk_packages(dsql_migrator.__path__, "dsql_migrator."):
        names.append(mod.name)
    return sorted(names)


def _undefined_globals(module: types.ModuleType) -> list[str]:
    """Return ``"qualname: NAME"`` for every unresolvable LOAD_GLOBAL in the module."""
    missing: list[str] = []
    namespace = vars(module)
    for attr_name, value in list(namespace.items()):
        code = getattr(value, "__code__", None)
        if code is None and isinstance(value, type):
            # Methods defined on a class in THIS module.
            for member in list(vars(value).values()):
                inner = getattr(member, "__code__", None)
                if inner is not None and inner.co_filename == getattr(
                    module, "__file__", None
                ):
                    missing.extend(_missing_in_code(inner, namespace, f"{attr_name}."))
            continue
        if code is None or code.co_filename != getattr(module, "__file__", None):
            continue  # imported from elsewhere -- checked with its own module
        missing.extend(_missing_in_code(code, namespace, ""))
    return missing


def _missing_in_code(code: types.CodeType, namespace: dict, prefix: str) -> list[str]:
    out: list[str] = []
    for block in _iter_code_objects(code):
        for instruction in dis.get_instructions(block):
            if instruction.opname != "LOAD_GLOBAL":
                continue
            name = instruction.argval
            if not isinstance(name, str):
                continue
            if name in namespace or hasattr(builtins, name):
                continue
            out.append(f"{prefix}{block.co_qualname}: {name}")
    return out


@pytest.mark.parametrize("module_name", _all_modules())
def test_module_has_no_undefined_global_names(module_name: str) -> None:
    """Every global a function reads must exist once the module is imported."""
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # noqa: BLE001 - an unimportable module is its own failure
        pytest.skip(f"{module_name} is not importable in the test env: {exc}")

    missing = _undefined_globals(module)
    assert not missing, (
        f"{module_name} references global name(s) that do not exist -- these raise "
        "NameError the moment the function runs (the v0.1.448 blocker):\n  "
        + "\n  ".join(sorted(set(missing)))
    )
