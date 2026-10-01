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


# --- P3-C24: --exclude-rules ----------------------------------------------------------------


def test_a_rules_file_gives_the_same_columns_and_capture_value_as_the_column_list():
    tables = _tables()
    from_list = h.parse_exclude_columns(["s.orders.notes"])
    from_rules = h.resolve_rules_file("exclude s.orders.notes  # why", tables)
    assert from_rules == from_list
    assert h.cdc_column_exclude_list(from_rules, tables) == h.cdc_column_exclude_list(from_list, tables)
    assert h.merge_exclusions(from_list, {"s.orders": {"id"}}) == {"s.orders": frozenset({"notes", "id"})}


def test_a_broken_rules_file_is_refused_before_any_connection_naming_the_line(tmp_path):
    path = tmp_path / "rules.txt"
    path.write_text("exclude s.orders.notes\nexlcude s.orders.id\n", encoding="utf-8")
    text = h.read_rules_file(str(path))
    (problem,) = h.check_rules_file(text)
    assert problem.startswith('L2 "exlcude s.orders.id": Unknown first word')
    with pytest.raises(ValueError, match='L1 "exclude s.nosuch.x": No table "nosuch"'):
        h.resolve_rules_file("exclude s.nosuch.x", _tables())
    with pytest.raises(ValueError, match="table rules are not supported"):
        h.resolve_rules_file("exclude table s.orders", _tables() + [
            TableDef(name="s.other", primary_key=["id"], columns=[ColumnDef(name="id", mysql_type="int")])])


def test_the_harnesses_take_exclude_rules():
    import pathlib

    root = pathlib.Path(__file__).resolve().parent.parent / "scripts"
    # run_full_load_harness.py is local-only (git-ignored): a clean clone has no copy, so
    # it is checked only where it exists. run_pg_cdc_e2e.py is tracked and always checked.
    for name in ("run_full_load_harness.py", "run_pg_cdc_e2e.py"):
        if name == "run_full_load_harness.py" and not (root / name).exists():
            continue
        text = (root / name).read_text(encoding="utf-8")
        assert '"--exclude-rules"' in text and "check_rules_file(" in text, name
