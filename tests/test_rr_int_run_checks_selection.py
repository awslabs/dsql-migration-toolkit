# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The prerequisite checks run on the tables Start Full Load would migrate.

``run_checks`` (Data Migration's Check button) hand-copied the selection formula --
``migratable_table_names``, then ``effective_migration_selection`` over the picker with
``default_migration_selection`` as the untouched default -- that
``resolve_migration_selection`` holds for the card, the picker and the live gates, and that
Start Full Load's runner already reads (e64dcae). Nothing pinned it: replacing the copy with
every migratable table failed no test. It now resolves through the one formula.

The real ``build_data_migration_screen`` (tests.test_g12_table_set_after_load._screen: a
MySQL source, three tables with generated DDL), the prerequisites panel's ``run_checks``
captured, and a checker that only records its request: no source, no Aurora DSQL.
"""

from __future__ import annotations

import asyncio

from dsql_migrator.core.models import MigrationMode
from dsql_migrator.core.prerequisites import PrerequisiteReport
from tests.test_g12_table_set_after_load import _loaded, _screen


def _checked_tables(monkeypatch, *, selected, touched=True, spy=None) -> list:
    """Click Check (Full Load) and return the table list the checker was asked about."""
    from dsql_migrator.ui import data_migration as dm

    s = _screen(monkeypatch, job=_loaded(), selected=selected)
    s.state.selection_touched = touched
    s.session.set_source_verified(True)
    s.session.set_target_verified(True)
    captured: dict = {}
    real_panel = dm._render_prerequisites_panel

    def _panel(ui_, state, run_checks, *a, **k):
        captured["run_checks"] = run_checks
        return real_panel(ui_, state, run_checks, *a, **k)

    requests: list = []

    class _Checker:
        def check(self, request, **_k):
            requests.append(request)
            return PrerequisiteReport.build(request.mode, [])

    monkeypatch.setattr(dm, "_render_prerequisites_panel", _panel)
    monkeypatch.setattr(dm, "build_prerequisite_checker", lambda **_k: _Checker())
    s.content(lambda: None)
    if spy is not None:
        monkeypatch.setattr(dm, "resolve_migration_selection", spy)
    asyncio.run(captured["run_checks"](MigrationMode.FULL_LOAD))
    (request,) = requests
    return list(request.tables)


def test_the_checks_run_on_the_picker_or_the_default(monkeypatch) -> None:
    assert _checked_tables(monkeypatch, selected=["app.b"]) == ["app.b"]
    # Untouched: the Schema Conversion default (here every generated table).
    assert _checked_tables(monkeypatch, selected=["app.b"], touched=False) == [
        "app.a", "app.b", "app.c"]


def test_the_checks_resolve_their_tables_through_the_one_selection_formula(monkeypatch):
    from dsql_migrator.ui import data_migration as dm

    def _formula(inventory, conv_state, migration_state, target_inventory):
        # A set the copied formula cannot produce from this state (the picker holds app.b).
        return ["app.c"]

    assert dm.resolve_migration_selection is not _formula
    assert _checked_tables(monkeypatch, selected=["app.b"], spy=_formula) == ["app.c"], (
        "run_checks picked its tables with its own copy of the selection formula"
    )
