# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2-17: the E2E harness's --exclude-columns support (scripts/_exclusion_harness.py)."""

import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts"))

import _exclusion_harness as h  # noqa: E402

from dsql_migrator.core.models import ColumnDef, TableDef  # noqa: E402


def _tables():
    return [TableDef(name="s.orders", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="notes", mysql_type="text"),
        ColumnDef(name="blob", mysql_type="longblob")])]


def test_parse_qualifies_bare_tables_and_splits_on_the_last_dot():
    assert h.parse_exclude_columns(["orders.notes,orders.blob", "x.y.z"], schema="s") == {
        "s.orders": frozenset({"notes", "blob"}), "x.y": frozenset({"z"})}
    assert h.parse_exclude_columns([]) == {}
    with pytest.raises(ValueError):
        h.parse_exclude_columns(["notes"])


def test_check_refuses_unknown_names_and_keys():
    assert h.check_exclusions({"s.orders": {"notes"}}, _tables()) == []
    problems = h.check_exclusions({"s.orders": {"id", "nope"}, "s.other": {"c"}}, _tables())
    assert any("primary-key" in p for p in problems) and any("no such column" in p for p in problems)
    assert any("not among the loaded tables" in p for p in problems)


def test_the_inputs_fields_include_the_ledger_and_nothing_without_exclusions(tmp_path):
    from dsql_migrator.ui.data_migration import DataMigrationInputs

    ledger = h.local_overlay_ledger(str(tmp_path / "l.sqlite"))
    fields = h.overlay_input_fields({"s.orders": {"notes"}}, ledger)
    assert fields["overlay_columns"] == {"s.orders": frozenset({"notes"})}
    assert fields["excluded_lob_columns"] == {"s.orders": {"notes"}}
    assert fields["overlay_ledger"] is ledger and fields["overlay_ledger_in_use"] is True
    assert h.overlay_input_fields({}, ledger) == {}
    # They are real DataMigrationInputs fields.
    assert set(fields) <= set(DataMigrationInputs.__dataclass_fields__)


def test_the_cdc_exclude_list_is_the_ui_format():
    assert h.cdc_column_exclude_list({}, _tables()) is None
    assert h.cdc_column_exclude_list({"s.orders": {"notes", "blob"}}, _tables()) == ["s.orders.blob", "s.orders.notes"]


class _Cur:
    def __init__(self, row):
        self.row, self.sql = row, None

    def execute(self, statement, params=None):
        self.sql = statement.as_string(None) if hasattr(statement, "as_string") else statement

    def fetchone(self):
        return self.row


def test_assert_excluded_null():
    cur = _Cur((5, 0))
    conn = SimpleNamespace(cursor=lambda: cur)
    assert h.assert_excluded_null(conn, "s.orders", "notes") == 5
    assert cur.sql == 'SELECT COUNT(*), COUNT("notes") FROM "s"."orders"'
    with pytest.raises(AssertionError, match="2 of 5"):
        h.assert_excluded_null(SimpleNamespace(cursor=lambda: _Cur((5, 2))), "s.orders", "notes")


def test_assert_default_restored(monkeypatch):
    import dsql_migrator.core.target_introspector as ti
    from dsql_migrator.core.target_introspector import ColumnNullFacts

    live = ColumnNullFacts(not_null=False, default_expr="'active'::text", identity=False, generated=False)
    monkeypatch.setattr(ti, "read_overlay_column_facts", lambda conn, t, cols: (True, {"notes": live}))
    conn = SimpleNamespace(cursor=lambda: _Cur((3, 0)))
    h.assert_default_restored(conn, "s.orders", "notes", "'active'::text")
    with pytest.raises(AssertionError, match="expected"):
        h.assert_default_restored(conn, "s.orders", "notes", "'other'::text")
