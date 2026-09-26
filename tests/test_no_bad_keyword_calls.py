# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Every keyword argument passed to one of our own functions must be one it accepts.

WHY: a keyword-argument mismatch raises only when the call RUNS, so it ships green. The
reported case: ``_render_cdc_live_monitoring`` gained two hooks at its CALLER and at the
DLQ panel it renders, but not in its own signature in between -- so every render of the
Data Migration step died with

    TypeError: _render_cdc_live_monitoring() got an unexpected keyword argument
    'lob_candidates_for'

and the whole step fell to its error boundary ("Data Migration could not be displayed").
4401 unit tests passed over that, because nothing calls that render path with the real
wiring. This is the same shape as :mod:`test_no_undefined_globals` (a NameError that only
fires when the line executes), and it is checked the same way: statically, over the whole
package, with no need for a test to exercise the path.

HOW: parse each module, collect its module-level ``def``\\s, then check every call in that
module whose target is one of those names. A keyword not in the callee's parameters -- and
the callee has no ``**kwargs`` to absorb it -- is a guaranteed TypeError.

Cross-module calls are resolved too, which is what the reported bug actually was: the
caller lives in ``ui/data_migration/_cdc_ui.py`` and the callee in ``_cdc_monitoring.py``.
A first cut of this check only looked within one module and passed over the very bug it was
written for -- so the package's ``from . import x`` / ``from dsql_migrator.a.b import x``
imports are followed.

Deliberately conservative, so a failure is always real:
* the callee must be a module-level ``def`` in this package, reached either directly or
  through a resolvable import;
* skipped when the callee takes ``**kwargs``, or the call uses ``**spread``;
* a name rebound anywhere in the module (a local, a parameter, a reassignment) is skipped,
  since the call may not reach the module-level def.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

import dsql_migrator


def _source_files() -> list[pathlib.Path]:
    root = pathlib.Path(dsql_migrator.__path__[0])
    return sorted(p for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _module_name(path: pathlib.Path) -> str:
    """``dsql_migrator.a.b`` for a file inside the package, else ``""``.

    The empty string is for a file OUTSIDE the package (this module's own unit test writes
    temp files), where there are no package imports to resolve.
    """
    root = pathlib.Path(dsql_migrator.__path__[0]).parent
    try:
        rel = path.relative_to(root).with_suffix("")
    except ValueError:
        return ""
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _package_defs() -> dict[str, dict[str, ast.AST]]:
    """``{module_name: {function_name: FunctionDef}}`` for the whole package."""
    out: dict[str, dict[str, ast.AST]] = {}
    for path in _source_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - would fail the build elsewhere
            continue
        out[_module_name(path)] = {
            node.name: node
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
    return out


_PACKAGE_DEFS = _package_defs()


def _imported_defs(tree: ast.Module, module: str) -> dict[str, ast.AST]:
    """Module-level functions this module imported FROM the package, by local name."""
    found: dict[str, ast.AST] = {}
    if not module:
        return found
    pkg = module.rsplit(".", 1)[0] if "." in module else module
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if node.level:  # relative: from . import x / from .mod import x
            base = pkg
            for _ in range(node.level - 1):
                base = base.rsplit(".", 1)[0] if "." in base else base
            target = f"{base}.{node.module}" if node.module else base
        else:
            target = node.module or ""
        if not target.startswith("dsql_migrator"):
            continue
        for alias in node.names:
            # `from .mod import fn` -- and `from . import mod` is a MODULE, not a function.
            defs = _PACKAGE_DEFS.get(target, {})
            if alias.name in defs:
                found[alias.asname or alias.name] = defs[alias.name]
    return found


def _accepted_keywords(func: ast.FunctionDef | ast.AsyncFunctionDef) -> set[str] | None:
    """The keyword names ``func`` accepts, or ``None`` when it absorbs anything."""
    args = func.args
    if args.kwarg is not None:
        return None  # **kwargs takes everything
    names = {a.arg for a in args.args} | {a.arg for a in args.kwonlyargs}
    names |= {a.arg for a in getattr(args, "posonlyargs", [])}
    return names


def _bad_keyword_calls(path: pathlib.Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))

    # This module's own module-level defs PLUS the ones it imported from the package.
    defs: dict[str, ast.AST] = dict(_imported_defs(tree, _module_name(path)))
    for node in tree.body:  # module level wins over an import of the same name
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defs[node.name] = node

    # Any other binding of the same name makes the target ambiguous -> skip it.
    rebound: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            a = node.args
            for arg in (
                list(a.args)
                + list(a.kwonlyargs)
                + list(getattr(a, "posonlyargs", []))
                + [x for x in (a.vararg, a.kwarg) if x is not None]
            ):
                if arg.arg in defs:
                    rebound.add(arg.arg)
            if node is not defs.get(node.name):
                rebound.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            if node.id in defs:
                rebound.add(node.id)

    problems: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        callee = defs.get(node.func.id)
        if callee is None or node.func.id in rebound:
            continue
        if any(kw.arg is None for kw in node.keywords):
            continue  # **spread -- cannot know the names
        accepted = _accepted_keywords(callee)
        if accepted is None:
            continue
        for kw in node.keywords:
            if kw.arg not in accepted:
                problems.append(
                    f"line {node.lineno}: {node.func.id}(...) does not accept "
                    f"keyword {kw.arg!r} (defined line {callee.lineno})"
                )
    return problems


@pytest.mark.parametrize(
    "path", _source_files(), ids=lambda p: str(p).split("dsql_migrator/", 1)[-1]
)
def test_module_passes_no_unaccepted_keyword_to_its_own_functions(
    path: pathlib.Path,
) -> None:
    problems = _bad_keyword_calls(path)
    assert not problems, (
        f"{path}: keyword argument(s) no function of this module accepts -- each one is a "
        "TypeError the moment the call runs:\n  " + "\n  ".join(problems)
    )


def test_the_checker_catches_the_reported_shape(tmp_path: pathlib.Path) -> None:
    """The check must fail on the bug it was written for, and pass on the fixed form."""
    broken = tmp_path / "broken.py"
    broken.write_text(
        "def inner(ui, session=None):\n"
        "    return ui, session\n"
        "\n"
        "def outer(ui):\n"
        "    inner(ui, session=None, lob_candidates_for=None)\n",
        encoding="utf-8",
    )
    problems = _bad_keyword_calls(broken)
    assert len(problems) == 1, problems
    assert "lob_candidates_for" in problems[0]

    fixed = tmp_path / "fixed.py"
    fixed.write_text(
        "def inner(ui, session=None, lob_candidates_for=None):\n"
        "    return ui, session, lob_candidates_for\n"
        "\n"
        "def outer(ui):\n"
        "    inner(ui, session=None, lob_candidates_for=None)\n",
        encoding="utf-8",
    )
    assert _bad_keyword_calls(fixed) == []

    # **kwargs absorbs anything, so it must NOT be reported.
    absorbing = tmp_path / "absorbing.py"
    absorbing.write_text(
        "def inner(ui, **kwargs):\n"
        "    return ui, kwargs\n"
        "\n"
        "def outer(ui):\n"
        "    inner(ui, anything=1)\n",
        encoding="utf-8",
    )
    assert _bad_keyword_calls(absorbing) == []

    # A name rebound as a local is ambiguous, so it is skipped rather than false-flagged.
    shadowed = tmp_path / "shadowed.py"
    shadowed.write_text(
        "def inner(ui):\n"
        "    return ui\n"
        "\n"
        "def outer(ui, inner):\n"
        "    inner(ui, whatever=1)\n",
        encoding="utf-8",
    )
    assert _bad_keyword_calls(shadowed) == []
