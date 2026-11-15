# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The distribution and command are ``dsql-migration-toolkit`` since v0.1.590.

Before, both were ``mysql-dsql-migrator``. The rename has four places that must agree, and
each one fails differently when it does not:

- ``dsql_migrator.__version__`` reads the installed distribution by name. Looking up only
  the old name, a synced virtualenv (which no longer has it) shows ``0.0.0+dev`` in the UI.
- ``uv.lock``'s root package: ``uv sync --frozen`` (the image build, the EC2 host's first
  boot) stops with *Could not find root package* when it differs from pyproject's name.
- The old command stays as a permanent alias: a single EC2 host that an existing stack
  replaces runs that stack's ExecStart, ``mysql-dsql-migrator ui``, against a new clone.
- The image ENTRYPOINT and the EC2 ExecStart must each name a console script that exists.
"""

from __future__ import annotations

import re
from importlib import metadata
from pathlib import Path

import pytest

import dsql_migrator
from dsql_migrator.cli.main import build_parser

tomllib = pytest.importorskip("tomllib")

_REPO = Path(__file__).resolve().parent.parent
_PYPROJECT = tomllib.loads((_REPO / "pyproject.toml").read_text(encoding="utf-8"))
_PROJECT = _PYPROJECT["project"]
_NEW = "dsql-migration-toolkit"
_OLD = "mysql-dsql-migrator"
_ENTRY = "dsql_migrator.cli.main:main"


def test_the_distribution_is_named_dsql_migration_toolkit() -> None:
    assert _PROJECT["name"] == _NEW


def test_version_lookup_tries_the_pyproject_name_first_then_the_old_name() -> None:
    # FAILS WITHOUT THE FIX: __init__ looked up "mysql-dsql-migrator" only.
    assert dsql_migrator._DISTRIBUTION_NAMES == (_PROJECT["name"], _OLD)


def _lookup(installed: dict[str, str], calls: list[str]):
    def _version(name: str) -> str:
        calls.append(name)
        if name not in installed:
            raise metadata.PackageNotFoundError(name)
        return installed[name]

    return _version


@pytest.mark.parametrize(
    ("installed", "expected", "looked_up"),
    [
        # A synced virtualenv: the new name answers, the old one is not consulted.
        ({_NEW: "0.1.590"}, "0.1.590", [_NEW]),
        # A virtualenv installed before the rename and not synced since.
        ({_OLD: "0.1.589"}, "0.1.589", [_NEW, _OLD]),
        # A source tree with no install.
        ({}, "0.0.0+dev", [_NEW, _OLD]),
    ],
)
def test_installed_version_falls_back_name_by_name(
    monkeypatch: pytest.MonkeyPatch,
    installed: dict[str, str],
    expected: str,
    looked_up: list[str],
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(dsql_migrator, "_pkg_version", _lookup(installed, calls))
    assert dsql_migrator._installed_version() == expected
    assert calls == looked_up


def test_version_is_the_installed_distributions() -> None:
    # FAILS WITHOUT THE FIX: uv sync removes mysql-dsql-migrator once the distribution is
    # renamed, so the old lookup made the UI show 0.0.0+dev.
    try:
        installed = metadata.version(_PROJECT["name"])
    except metadata.PackageNotFoundError:
        pytest.skip("this virtualenv was installed before the rename: run uv sync")
    assert dsql_migrator.__version__ == installed


def test_uv_lock_root_package_is_the_pyproject_name_and_version() -> None:
    # `uv sync --frozen` refuses a lock whose root package name differs from pyproject's.
    lock = tomllib.loads((_REPO / "uv.lock").read_text(encoding="utf-8"))
    roots = [p for p in lock["package"] if p.get("source") == {"editable": "."}]
    assert [(p["name"], p["version"]) for p in roots] == [
        (_PROJECT["name"], _PROJECT["version"])
    ]
    assert _OLD not in {p["name"] for p in lock["package"]}


def test_both_console_scripts_run_the_same_main() -> None:
    assert _PROJECT["scripts"] == {_NEW: _ENTRY, _OLD: _ENTRY}


def test_the_ec2_execstart_command_is_a_console_script() -> None:
    template = (_REPO / "deploy" / "cloudformation-ec2.yaml").read_text(encoding="utf-8")
    commands = re.findall(r"^\s*ExecStart=(\S+)", template, re.M)
    assert len(commands) == 1, commands
    assert Path(commands[0]).name in _PROJECT["scripts"]


def test_the_image_entrypoint_is_a_console_script() -> None:
    dockerfile = (_REPO / "deploy" / "Dockerfile").read_text(encoding="utf-8")
    entrypoints = re.findall(r'^ENTRYPOINT \["([^"]+)"\]', dockerfile, re.M)
    assert entrypoints == [_NEW]
    assert entrypoints[0] in _PROJECT["scripts"]


def test_the_cli_names_itself_dsql_migration_toolkit(capsys: pytest.CaptureFixture) -> None:
    parser = build_parser()
    assert parser.prog == _NEW
    with pytest.raises(SystemExit):
        parser.parse_args(["--version"])
    assert capsys.readouterr().out == f"{_NEW} {dsql_migrator.__version__}\n"
