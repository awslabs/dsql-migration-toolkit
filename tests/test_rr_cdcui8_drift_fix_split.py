# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""ui/data_migration/_cdc_drift_fix.py, the schema-drift copy and the Fix target schema
dialog (cdcui-8): _cdc_monitoring re-exports all of it, its top-level imports stay out of the
package, and the dialog is the one function in it that renders.

Pinned here:

* the re-export: every top-level definition of _cdc_drift_fix.py is the very same object
  through _cdc_monitoring, so the tests and readers that import the drift names from there
  resolve unchanged;
* its top-level imports are typing / logging / core.models / ui.design only: an import of the
  package or _cdc_monitoring at the top would put this module into the package's import
  order (cdc_dlq_gate imports _DRIFT_LABELS from here inside a function);
* ``_LOGGER`` is the package's logger, so the dialog's warnings log where they did;
* ``ui`` is read only inside ``_open_add_column_dialog``: the copy helpers stay pure.
"""

import ast
import logging
import pathlib

_PACKAGE = "dsql_migrator.ui.data_migration"


def _tree():
    from dsql_migrator.ui.data_migration import _cdc_drift_fix

    return ast.parse(pathlib.Path(_cdc_drift_fix.__file__).read_text(encoding="utf-8"))


def test_cdc_monitoring_re_exports_every_name_the_drift_module_defines() -> None:
    from dsql_migrator.ui.data_migration import _cdc_drift_fix, _cdc_monitoring

    defined: "list[str]" = []
    for node in _tree().body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.append(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            defined += [
                n.id
                for target in targets
                for n in ast.walk(target)
                if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Store)
            ]
    defined = [name for name in defined if not name.startswith("__")]
    assert {
        "_DRIFT_LABELS",
        "_UNIQUE_CONFLICT_WHY",
        "_drift_why",
        "_ddl_runbook",
        "_FixInputs",
        "_fix_inputs",
        "_open_add_column_dialog",
    } <= set(defined)
    absent = object()
    not_re_exported = [
        name
        for name in defined
        if getattr(_cdc_monitoring, name, absent) is not getattr(_cdc_drift_fix, name)
    ]
    assert not_re_exported == [], f"_cdc_monitoring does not re-export {not_re_exported}"


def test_the_drift_module_imports_nothing_from_the_package_at_its_top() -> None:
    from dsql_migrator.ui.data_migration import _cdc_drift_fix

    top = [n for n in _tree().body if isinstance(n, (ast.Import, ast.ImportFrom))]
    modules = sorted(
        {n.module for n in top if isinstance(n, ast.ImportFrom)}
        | {a.name for n in top if isinstance(n, ast.Import) for a in n.names}
    )
    assert modules == [
        "__future__", "dsql_migrator.core.models", "dsql_migrator.ui.design", "logging", "typing"
    ]
    assert _cdc_drift_fix._LOGGER is logging.getLogger(_PACKAGE)
    from dsql_migrator.ui import data_migration

    assert _cdc_drift_fix._LOGGER is data_migration._LOGGER


def test_only_the_dialog_reads_ui() -> None:
    readers = {
        node.name
        for node in _tree().body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(isinstance(n, ast.Name) and n.id == "ui" for n in ast.walk(node))
    }
    assert readers == {"_open_add_column_dialog"}
