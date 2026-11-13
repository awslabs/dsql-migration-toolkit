# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""core/cdc_pg_routes.py, the PostgreSQL CDC blocks and the route rule, imports on its own
(cdccore-10).

It was split out of core/cdc_pg_slot.py, which imports it back to re-export every name,
private ones too -- so it must not import cdc_pg_slot at run time: that would be a cycle,
and importing the routes before the slot module would fail on a partly initialised module
(cdc_connector_diagnosis even calls pg_cdc_route at import, for its quota text). It is also
pure: no SQLAlchemy, no connection, no SQL text -- cdc_pg_slot takes the readings and makes
the source writes, these functions only grade a reading and say what to do. The import check
runs in a fresh interpreter, because this test session has already imported cdc_pg_slot.
"""

import ast
import pathlib
import subprocess
import sys

from dsql_migrator.core import cdc_pg_routes as routes
from dsql_migrator.core import cdc_pg_slot

_SRC = pathlib.Path(__file__).resolve().parents[1] / "src" / "dsql_migrator"
_PATH = _SRC / "core" / "cdc_pg_routes.py"
_TEXT = _PATH.read_text(encoding="utf-8")
_TREE = ast.parse(_TEXT)


def _defined(tree: ast.Module) -> "list[str]":
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names.append(node.name)
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
    return names


_NAMES = _defined(_TREE)


def test_imports_without_loading_the_slot_module() -> None:
    code = (
        "import sys, dsql_migrator.core.cdc_pg_routes; "
        "print('dsql_migrator.core.cdc_pg_slot' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False", out.stdout


def test_it_takes_no_run_time_import_of_the_slot_module_and_no_sql() -> None:
    for node in _TREE.body:  # top level only: the TYPE_CHECKING block is not run
        if isinstance(node, ast.ImportFrom):
            assert node.module != "dsql_migrator.core.cdc_pg_slot", node.lineno
            assert not (
                node.module == "dsql_migrator.core"
                and any(alias.name == "cdc_pg_slot" for alias in node.names)
            ), node.lineno
    for forbidden in ("sqlalchemy", "text(", "connection.execute", "SELECT ", "ALTER TABLE "):
        assert forbidden not in _TEXT, forbidden


def test_the_slot_module_re_exports_every_name() -> None:
    assert len(_NAMES) == 28
    for name in _NAMES:
        assert getattr(cdc_pg_slot, name) is getattr(routes, name), name


def test_the_readings_and_the_source_writes_stay_in_the_slot_module() -> None:
    # Only the grading moved: every read and every write is still cdc_pg_slot's.
    for name in (
        "read_pg_replication_objects",
        "read_inheritance_children",
        "read_replica_identity_usability",
        "PgReplicationObjects",
        "provision_pg_replication",
        "create_publication",
        "pg_source_write_connection",
    ):
        assert name not in _NAMES, name
        assert hasattr(cdc_pg_slot, name), name
