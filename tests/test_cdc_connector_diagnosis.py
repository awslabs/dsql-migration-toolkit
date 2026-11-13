# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""core/cdc_connector_diagnosis.py, what a connector's worker log means, imports on its
own (cdccore-6).

It was split out of core/cdc_deployer.py, which imports it back to re-export every name,
private ones too -- so it must not import cdc_deployer: that would be a cycle, and
importing the diagnosis before the deployer would fail on a partly initialised module. It
is also pure: no boto3, no session, no stack operation -- the deployer reads the tails and
asks this module what they mean. The import check runs in a fresh interpreter, because
this test session has already imported cdc_deployer.
"""

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import cdc_connector_diagnosis as diagnosis
from dsql_migrator.core import cdc_deployer

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_PATH = _SRC / "core" / "cdc_connector_diagnosis.py"
_TREE = ast.parse(_PATH.read_text(encoding="utf-8"))


def _defined(tree: ast.Module) -> "list[str]":
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.append(node.target.id)
    return names


_NAMES = _defined(_TREE)


def test_imports_without_loading_the_deployer() -> None:
    code = (
        "import sys, dsql_migrator.core.cdc_connector_diagnosis; "
        "print('dsql_migrator.core.cdc_deployer' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False", out.stdout


def test_it_imports_nothing_of_the_deployer_and_no_aws() -> None:
    for node in ast.walk(_TREE):
        if isinstance(node, ast.ImportFrom):
            assert node.module not in (
                "dsql_migrator.core.cdc_deployer",
                "dsql_migrator.core.cdc_stack_deployer",
            ), node.lineno
            assert not (
                node.module == "dsql_migrator.core"
                and any(
                    alias.name in ("cdc_deployer", "cdc_stack_deployer") for alias in node.names
                )
            ), node.lineno
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name.split(".")[0] not in ("boto3", "botocore"), node.lineno
    text = _PATH.read_text(encoding="utf-8")
    for forbidden in ("build_session", "boto3", "client("):
        assert forbidden not in text, forbidden


def test_the_deployer_re_exports_every_name() -> None:
    assert len(_NAMES) == 42
    for name in _NAMES:
        assert getattr(cdc_deployer, name) is getattr(diagnosis, name), name


def test_the_reads_and_the_stack_operations_stay_in_the_deployer() -> None:
    # What the deployer does with a tail -- read it, decide a timeout's or a rollback's
    # message -- stays there; only the meaning of a tail moved.
    for name in (
        "_read_connector_log",
        "_connector_timeout_message",
        "_connector_failure_message",
        "_rollback_failure_message",
        "_SNAPSHOT_MODE_LOG",
        "_PARTITION_QUOTA_GUIDANCE",
    ):
        assert name not in _NAMES, name
        assert hasattr(cdc_deployer, name), name
