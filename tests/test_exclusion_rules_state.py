"""P3-C09: DataMigrationState -- rule text, provenance, pending lines, the rules' setter."""

from __future__ import annotations

from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef, TableSelection
from dsql_migrator.ui.data_migration import DataMigrationState, DataMigrationStore

TABLES = [
    TableDef(name=n, primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="a", mysql_type="text"),
        ColumnDef(name="b", mysql_type="text")])
    for n in ("shop.t1", "shop.t2", "shop.audit_1", "shop.audit_2")
]
ALL = [t.name for t in TABLES]


def _resolve(state, text):
    return resolve_exclusion_rules(
        text, migratable=TABLES, source_type=SourceType.MYSQL,
        effective_selection=state.selection.selected_tables or ALL,
        rule_excluded_prev=state.rule_excluded_tables(),
        retick_overrides=state.rule_reticked_tables())


def _save(state, text, **kw):
    return state.set_column_exclusions(_resolve(state, text), text=text, **kw)


def test_an_atomic_replace_and_a_refused_resolution_writes_nothing():
    state = DataMigrationState()
    state.set_lob_exclusion("shop.t1", "b", True)
    assert _save(state, "exclude shop.t1.a\nexclude shop.t2.a\nexclude shop.t2.b") is None
    assert state.lob_exclusions() == {"shop.t1": {"a"}, "shop.t2": {"a", "b"}}
    before = (state.lob_exclusions(), state.exclusion_rules_text())
    refused = _save(state, "exclude shop.nosuch.x")
    assert refused and (state.lob_exclusions(), state.exclusion_rules_text()) == before


def test_resaving_unchanged_rules_keeps_legacy_and_omit_only_marks():
    state = DataMigrationState()
    state.restore_lob_exclusion("shop.t1", "a")
    state.restore_lob_exclusion("shop.t1", "b")
    state.restore_legacy_exclusions(["shop.t1:a"])
    overlay_before = state.lob_exclusions()
    assert _save(state, "exclude shop.t1.a\nexclude shop.t1.b") is None
    assert state.legacy_exclusions() == {"shop.t1": frozenset({"a"})}
    assert state.lob_exclusions() == overlay_before
    assert _save(state, "exclude shop.t1.b") is None
    assert state.legacy_exclusions() == {}  # the removed entry lost its mark


def test_a_column_only_save_leaves_the_selection_and_a_table_rule_materializes_it():
    state = DataMigrationState()
    assert _save(state, "exclude shop.t1.a") is None
    assert state.selection == TableSelection() and state.selection_touched is False
    assert _save(state, "exclude shop.t1.a\nexclude table shop.audit_*") is None
    assert state.selection.selected_tables == ["shop.t1", "shop.t2"] and state.selection_touched
    assert state.rule_excluded_tables() == {"shop.audit_1", "shop.audit_2"}
    assert _save(state, "exclude shop.t1.a") is None  # deleting the rule re-selects them
    assert state.selection.selected_tables == ALL and state.rule_excluded_tables() == frozenset()


def test_a_tree_retick_is_an_override_the_next_resolution_names():
    state = DataMigrationState()
    assert _save(state, "exclude table shop.audit_*") is None
    state.set_selection(TableSelection(selected_tables=["shop.t1", "shop.t2", "shop.audit_1"]))
    state.note_tree_retick(["shop.audit_1", "shop.t1"])
    assert state.rule_excluded_tables() == {"shop.audit_2"}
    res = _resolve(state, "exclude table shop.audit_*")
    assert any("ticked in the tree after your last Save" in n for n in res.line_status[1].notes)
    assert state.set_column_exclusions(res, text="exclude table shop.audit_*") is None
    assert state.rule_reticked_tables() == frozenset()


def test_pending_lines_lifecycle():
    state = DataMigrationState()
    assert state.add_pending_rule("exclude shop.t1.a  # from quarantine")
    assert not state.add_pending_rule("exclude   shop.t1.a")  # same rule, other spacing
    opened = state.pending_rules()
    assert opened == ["exclude shop.t1.a  # from quarantine"]
    # open -> Save (the editor held the pending line) -> reopen: it is in the rules, not pending
    assert _save(state, "\n".join(opened), opened_with=opened) is None
    assert state.pending_rules() == []
    assert not state.add_pending_rule("exclude shop.t1.a")  # already in the saved rules
    # open -> delete the line -> Save: it does not come back
    assert state.add_pending_rule("exclude shop.t2.b")
    opened = state.pending_rules()
    assert _save(state, "exclude shop.t1.a", opened_with=opened) is None
    assert state.pending_rules() == []
    # open -> Cancel (no Save): still pending; a refused Save leaves it too
    assert state.add_pending_rule("exclude shop.t2.b")
    opened = state.pending_rules()
    assert state.pending_rules() == opened
    assert _save(state, "exclude shop.nosuch.x", opened_with=opened)
    assert state.pending_rules() == opened
    # a line another tab added while the popup was open survives the Save
    assert state.add_pending_rule("exclude shop.t2.a")
    assert _save(state, "exclude shop.t1.a\nexclude shop.t2.b", opened_with=opened) is None
    assert state.pending_rules() == ["exclude shop.t2.a"]


def test_pending_dedupe_ignores_comments_and_respects_quotes():
    state = DataMigrationState()
    assert state.add_pending_rule('exclude shop."t#1".a  # why')
    assert not state.add_pending_rule('exclude shop."t#1".a')
    assert state.add_pending_rule("exclude shop.t")  # '#'-free different rule


def test_a_save_never_marks_the_ledger_in_use():
    state = DataMigrationState()
    assert _save(state, "exclude shop.t1.a") is None
    assert _save(state, "") is None
    assert state.overlay_ledger_in_use is False


def test_a_save_refuses_when_the_state_changed_since_open():
    state = DataMigrationState()
    token = state.exclusion_edit_token()
    res = _resolve(state, "exclude shop.t1.a")
    # meanwhile: a stack is attached and its set adopted
    state.cdc_stack_name = "dsql-cdc-x"
    state.cdc_stack_phase = "infra"
    state.cdc_stack_phase_checked = True
    assert state.adopt_deployed_exclusions({"shop.t2": ["b"]}, expect_stack="dsql-cdc-x")
    refused = state.set_column_exclusions(res, text="exclude shop.t1.a", token=token)
    assert refused and "open it again" in refused
    assert state.lob_exclusions() == {"shop.t2": {"b"}}
    assert state.set_column_exclusions(res, text="x", lock_check=lambda: "Locked: CDC streams.") == (
        "Locked: CDC streams.")


def test_adoption_writes_its_own_rule_lines():
    state = DataMigrationState()
    state.cdc_stack_name = "dsql-cdc-x"
    assert state.adopt_deployed_exclusions(
        {"shop.t1": ["a", "b"]}, legacy_columns={"shop.t1": ["b"]}, expect_stack="dsql-cdc-x")
    assert state.exclusion_rules_text().splitlines() == [
        "exclude shop.t1.a  # adopted from dsql-cdc-x",
        "exclude shop.t1.b  # adopted from dsql-cdc-x, omit-only",
    ]
    res = _resolve(state, state.exclusion_rules_text())
    assert {t: set(c) for t, c in res.columns.items()} == state.lob_exclusions()
    assert not state.adopt_deployed_exclusions({"shop.t2": ["a"]})  # still refuses a non-empty set


def test_reset_in_place_clears_every_new_field():
    store = DataMigrationStore()
    state = store.get_or_create("s1")
    state.add_pending_rule("exclude shop.t1.a")
    assert _save(state, "exclude table shop.audit_*", opened_with=[]) is None
    state.note_tree_retick(["shop.audit_1"])
    store.reset_in_place("s1")
    assert state.exclusion_rules_text() is None and state.rule_excluded_tables() == frozenset()
    assert state.pending_rules() == [] and state.rule_reticked_tables() == frozenset()
