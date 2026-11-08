# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""core/mysql_view_sql.py, the MySQL view / query expression rewriter, imports on its own.

It was split out of core/converter.py (B6), and the converter imports it back to re-export
every name. So it must not import the converter at its top level. If it did, importing it
before the converter, or importing the Query Converter (which reads the rewriter from it)
before the converter, would fail on a partly initialised module. Each check runs in a fresh
interpreter, because this test session has already imported the converter."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "module", ["dsql_migrator.core.mysql_view_sql", "dsql_migrator.core.query_converter"]
)
def test_imports_without_loading_the_schema_converter(module: str) -> None:
    code = (
        f"import importlib, sys; importlib.import_module({module!r}); "
        "print('dsql_migrator.core.converter' in sys.modules)"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "False", out.stdout
