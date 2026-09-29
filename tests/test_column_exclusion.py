# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The one exclusion predicate (core/column_exclusion.py): B1-B4, the effective set, and
the stored-entry check. Each rule has a positive AND a negative case so a mutation that
widens or narrows it fails here."""

from types import SimpleNamespace

from dsql_migrator.core.column_exclusion import (
    B1,
    B2,
    B3,
    B4,
    IssueSeverity,
    build_exclusion_context,
    check_stored_exclusions,
    column_exclusion_verdict,
    effective_exclusions,
)
from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, SourceInventory, TableDef


def _t(name, cols, pk=("id",), fks=()):
    return TableDef(
        name=name,
        columns=[ColumnDef(name=c, mysql_type="int") for c in cols],
        primary_key=list(pk),
        foreign_keys=list(fks),
    )


PARENT = _t("shop.users", ["id", "email", "bio"])
CHILD = _t(
    "shop.orders",
    ["id", "tenant", "user_email", "note", "total2"],
    fks=[ForeignKeyDef(name="fk_user", columns=["user_email"],
                       referenced_table="shop.users", referenced_columns=["email"])],
)
INV = SourceInventory(tables=[PARENT, CHILD])
APPLIED = {
    # Schema Conversion re-keyed orders on (tenant, id) and kept a generated column.
    "shop.orders": SimpleNamespace(target_ddl=(
        'CREATE TABLE "shop"."orders" ("id" INT NOT NULL, "tenant" TEXT NOT NULL, '
        '"user_email" TEXT, "note" TEXT, "total2" INT GENERATED ALWAYS AS (id * 2) STORED, '
        'PRIMARY KEY ("tenant", "id"))'
    )),
}


def _codes(verdict):
    return {f.code for f in verdict.findings}


def test_each_blocking_rule_and_its_negative():
    ctx = build_exclusion_context(INV, APPLIED)
    # B1: source PK.
    assert _codes(column_exclusion_verdict("shop.users", "id", ctx)) == {B1}
    # B2: target PK from the APPLIED DDL (not a source PK).
    assert _codes(column_exclusion_verdict("shop.orders", "tenant", ctx)) == {B2}
    # B3: referenced by a selected child's preserved FK.
    assert _codes(column_exclusion_verdict("shop.users", "email", ctx)) == {B3}
    # B4: computed by DSQL (GENERATED ... STORED in the applied DDL).
    assert _codes(column_exclusion_verdict("shop.orders", "total2", ctx)) == {B4}
    # Plain columns are OK -- including the FK CHILD column (that is only a warning, P3).
    for table, col in (("shop.users", "bio"), ("shop.orders", "note"), ("shop.orders", "user_email")):
        assert not column_exclusion_verdict(table, col, ctx).blocked, (table, col)


def test_b2_also_comes_from_the_cdc_rekey_map_without_applied_ddl():
    # The CDC builders do not hold the Schema Conversion state: message.key.columns carries it.
    ctx = build_exclusion_context(INV, None, message_key_columns={"shop.orders": ["tenant", "id"]})
    assert _codes(column_exclusion_verdict("shop.orders", "tenant", ctx)) == {B2}
    assert not column_exclusion_verdict("shop.orders", "note", ctx).blocked


def test_b3_only_while_fks_are_preserved_and_only_for_selected_children():
    stripped = build_exclusion_context(INV, APPLIED, preserve_foreign_keys=False)
    assert not column_exclusion_verdict("shop.users", "email", stripped).blocked
    unselected_child = build_exclusion_context(INV, APPLIED, selected_tables=["shop.users"])
    assert not column_exclusion_verdict("shop.users", "email", unselected_child).blocked
    msg = column_exclusion_verdict("shop.users", "email", build_exclusion_context(INV, APPLIED)).findings[0].message
    assert "shop.orders.fk_user" in msg


def test_effective_set_always_drops_key_columns_and_keeps_the_rest():
    ctx = build_exclusion_context(INV, APPLIED)
    eff = effective_exclusions(
        {"shop.users": {"id", "bio", "email"}, "shop.orders": {"tenant", "note", "total2"}}, ctx
    )
    # B1/B2 dropped on every path (they would break the load or the stream) ...
    assert {(d.table, d.column, d.finding.code) for d in eff.dropped} == {
        ("shop.users", "id", B1), ("shop.orders", "tenant", B2)}
    # ... B3/B4 and plain columns kept (B3/B4 are handled by the gate before data moves).
    assert eff.columns == {"shop.users": frozenset({"bio", "email"}),
                           "shop.orders": frozenset({"note", "total2"})}
    # A valid set passes through unchanged -- today's behaviour for every normal session.
    assert effective_exclusions({"shop.users": {"bio"}}, ctx).columns == {"shop.users": frozenset({"bio"})}
    assert effective_exclusions({}, ctx).columns == {}


def test_stored_entries_block_before_data_moved_and_only_warn_after():
    ctx = build_exclusion_context(INV, APPLIED)
    stored = {"shop.users": {"id", "email", "bio", "gone_col"}, "shop.orders": {"total2"},
              "shop.dropped_table": {"x"}}
    before = {(i.table, i.column): i.severity for i in check_stored_exclusions(stored, ctx, committed=False)}
    assert before == {
        ("shop.users", "id"): IssueSeverity.ERROR,
        ("shop.users", "email"): IssueSeverity.ERROR,
        ("shop.users", "gone_col"): IssueSeverity.ERROR,
        ("shop.orders", "total2"): IssueSeverity.ERROR,
        ("shop.dropped_table", "x"): IssueSeverity.ERROR,
    }  # "bio" is valid: no issue
    after = {(i.table, i.column): i.severity for i in check_stored_exclusions(stored, ctx, committed=True)}
    assert after[("shop.users", "id")] is IssueSeverity.WARNING
    assert after[("shop.users", "gone_col")] is IssueSeverity.WARNING
    assert after[("shop.orders", "total2")] is IssueSeverity.INFO  # "already omitted"
    assert ("shop.users", "bio") not in after
    assert check_stored_exclusions({"shop.users": {"bio"}}, ctx, committed=False) == ()


# ---------------------------------------------------------------------------
# The UI helper every path reads, and the guard that keeps every path on it
# ---------------------------------------------------------------------------


def test_effective_lob_exclusions_drops_keys_from_the_stored_set():
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.data_migration._models import effective_lob_exclusions

    state = DataMigrationState()
    for col in ("id", "bio"):
        state.set_lob_exclusion("shop.users", col, True)
    state.set_lob_exclusion("shop.orders", "tenant", True)
    state.set_lob_exclusion("shop.orders", "note", True)

    # Source PK dropped; re-key column dropped once CDC's key map (from the applied DDL) knows it.
    assert effective_lob_exclusions(state, INV, applied_conversions=APPLIED) == {
        "shop.users": frozenset({"bio"}), "shop.orders": frozenset({"note"})}
    state.set_cdc_message_key_columns({"shop.orders": ["tenant", "id"]})
    assert effective_lob_exclusions(state, INV) == {
        "shop.users": frozenset({"bio"}), "shop.orders": frozenset({"note"})}
    # A valid set is unchanged, and the STORED set is never rewritten (restore must round-trip it).
    assert state.lob_exclusions()["shop.users"] == {"id", "bio"}
    # No inventory: nothing to judge against -> the stored set as-is.
    assert effective_lob_exclusions(state, None)["shop.users"] == frozenset({"id", "bio"})


def test_every_path_reads_the_effective_set_not_the_raw_one():
    """Full Load, CDC and Validation must read effective_lob_exclusions.

    Reverting any one site to ``migration_state.lob_exclusions()`` would silently put a key
    column back on that path while the others drop it -- the exact disagreement this closes.
    The raw set is legitimately read only by persistence, the run-guard snapshot compare and
    the LOB card (which shows what is stored).
    """
    import pathlib

    import dsql_migrator.ui.data_migration as dm

    root = pathlib.Path(dm.__file__).parent
    ui = root.parent
    raw = "migration_state.lob_exclusions()"
    assert raw not in (root / "_cdc_ui.py").read_text(encoding="utf-8")
    assert raw not in (ui / "validation.py").read_text(encoding="utf-8")
    init = (root / "__init__.py").read_text(encoding="utf-8")
    assert init.count("excluded_lob_columns=effective_lob_exclusions(") == 3
    assert init.count(raw) == 0  # the run guard reads state.lob_exclusions() on its own name
    assert "_prereq_exclusions = effective_lob_exclusions(" in init
    for path in (root / "_cdc_ui.py", ui / "validation.py"):
        assert "effective_lob_exclusions(migration_state" in path.read_text(encoding="utf-8"), path


# ---------------------------------------------------------------------------
# The stored-entry gate: scope, the recorded applied conversion, the Full Load guard
# ---------------------------------------------------------------------------


def _state_with(entries, selected=None):
    from dsql_migrator.core.table_selection import TableSelection
    from dsql_migrator.ui.data_migration import DataMigrationState

    state = DataMigrationState()
    for table, col in entries:
        state.set_lob_exclusion(table, col, True)
    if selected is not None:
        state.set_selection(TableSelection(selected_tables=list(selected)))
    return state


def test_stored_issues_ignore_unselected_tables_but_keep_vanished_ones():
    from dsql_migrator.ui.data_migration._models import stored_exclusion_issues

    state = _state_with([("shop.users", "id"), ("shop.orders", "note"), ("gone.t", "x")])
    # users is not migrated -> its PK entry affects nothing; a table that left the source
    # is still reported (a rename would silently lapse the exclusion).
    scoped = stored_exclusion_issues(state, INV, committed=False, selected_tables=["shop.orders"])
    assert [(i.table, i.column) for i in scoped] == [("gone.t", "x")]
    # None falls back to the STORED selection (what the checks pinned), so the card and
    # the Deploy gate judge the same scope.
    pinned = _state_with([("shop.users", "id"), ("gone.t", "x")], selected=["shop.orders"])
    assert [(i.table, i.column) for i in stored_exclusion_issues(pinned, INV, committed=False)] == [
        ("gone.t", "x")]
    # ...and to every table while nothing is pinned.
    assert {(i.table, i.column) for i in stored_exclusion_issues(state, INV, committed=False)} == {
        ("shop.users", "id"), ("gone.t", "x")}
    assert stored_exclusion_issues(_state_with([]), INV, committed=False) == ()


def test_every_path_judges_against_the_recorded_applied_conversion():
    from dsql_migrator.ui.data_migration._models import (
        effective_lob_exclusions,
        stored_exclusion_issues,
    )

    state = _state_with([("shop.orders", "tenant"), ("shop.orders", "note"), ("shop.users", "email")])
    # Before any render nothing knows the composite key: only the source schema is judged.
    assert effective_lob_exclusions(state, INV)["shop.orders"] == frozenset({"tenant", "note"})
    # The render records the applied conversion; a path that holds no Schema Conversion
    # state (CDC builders, Validation) now drops the composite key's leading column too.
    state.set_applied_conversions(APPLIED, preserve_foreign_keys=False)
    assert effective_lob_exclusions(state, INV)["shop.orders"] == frozenset({"note"})
    # The recorded FK toggle reaches B3: with FKs stripped, users.email is excludable.
    issues = {(i.table, i.column) for i in stored_exclusion_issues(state, INV, committed=False)}
    assert issues == {("shop.orders", "tenant")}
    state.set_applied_conversions(APPLIED, preserve_foreign_keys=True)
    issues = {(i.table, i.column) for i in stored_exclusion_issues(state, INV, committed=False)}
    assert issues == {("shop.orders", "tenant"), ("shop.users", "email")}


def test_block_reason_names_the_entries_and_where_to_remove_them():
    from dsql_migrator.ui.data_migration._models import (
        stored_exclusion_block_reason,
        stored_exclusion_issues,
    )

    one = stored_exclusion_block_reason(
        stored_exclusion_issues(_state_with([("shop.users", "id")]), INV, committed=False))
    assert one.startswith("1 saved column exclusion no longer applies: shop.users.id")
    assert one.endswith("Remove it in 'Oversized LOB columns' above first.")
    many = _state_with([("shop.users", "id"), ("shop.users", "a"), ("shop.users", "b"), ("shop.users", "c")])
    text = stored_exclusion_block_reason(stored_exclusion_issues(many, INV, committed=False))
    assert text.startswith("4 saved column exclusions no longer apply:") and "And 1 more." in text
    assert "Remove them" in text
    # After the lock: warnings only -> nothing blocks.
    assert stored_exclusion_block_reason(
        stored_exclusion_issues(many, INV, committed=True)) is None


def test_full_load_guard_puts_the_stored_entry_gate_first():
    from dsql_migrator.ui.data_migration import full_load_run_guard_reason

    state = _state_with([])
    assert full_load_run_guard_reason(state, INV, exclusion_block="BLOCKED") == "BLOCKED"
    # No inventory still wins (nothing to judge against), and no block leaves the old answer.
    assert full_load_run_guard_reason(state, None, exclusion_block="BLOCKED").startswith("Run Step 1")
    assert full_load_run_guard_reason(state, INV) == full_load_run_guard_reason(
        state, INV, exclusion_block=None)
    # has_run does not bypass it: the caller already folds "data moved" into the lock.
    assert full_load_run_guard_reason(state, INV, has_run=True, exclusion_block="BLOCKED") == "BLOCKED"


def test_screen_wires_the_gate_with_the_cards_own_lock_and_records_the_conversion():
    """``content()`` is not rendered by the unit suite, so pin its wiring structurally.

    The gate's ``committed`` must be the SAME expression as the card's ``locked``: if they
    drift, the guard can name a Remove the frozen card does not offer (a dead end), or the
    card can offer a Remove for an entry the gate never blocks on.
    """
    import ast
    import inspect

    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))

    def _calls(name):
        return [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and getattr(n.func, "id", getattr(n.func, "attr", None)) == name
        ]

    def _kw(call, key):
        return next(ast.unparse(k.value) for k in call.keywords if k.arg == key)

    (panel,) = _calls("_render_cdc_lob_exclusion_panel")
    (issues,) = _calls("stored_exclusion_issues")
    assert _kw(panel, "locked") == _kw(issues, "committed") == "selection_locked or _lob_locked"
    assert _kw(panel, "selected_tables") == _kw(issues, "selected_tables") == "selected_names"
    (guard,) = _calls("full_load_run_guard_reason")
    assert _kw(guard, "exclusion_block") == "_exclusion_block"
    (record,) = _calls("set_applied_conversions")
    assert ast.unparse(record.args[0]) == "_applied"
    assert _kw(record, "preserve_foreign_keys") == "conv_state.preserve_foreign_keys"


# ---------------------------------------------------------------------------
# The recovery routes and the card: a refused column is never offered, never written
# ---------------------------------------------------------------------------


def _lob_inventory():
    from dsql_migrator.core.models import SourceType  # noqa: F401  (engine via _Mysql)

    docs = TableDef(
        name="shop.docs",
        columns=[ColumnDef(name=c, mysql_type=("int" if c == "id" else "longtext"))
                 for c in ("id", "tenant", "body", "summary", "ref")],
        primary_key=["id"],
    )
    notes = TableDef(
        name="shop.notes",
        columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name="doc_ref", mysql_type="longtext")],
        primary_key=["id"],
        foreign_keys=[ForeignKeyDef(name="fk_doc", columns=["doc_ref"],
                                    referenced_table="shop.docs", referenced_columns=["ref"])],
    )
    applied = {"shop.docs": SimpleNamespace(target_ddl=(
        'CREATE TABLE "shop"."docs" ("id" INT NOT NULL, "tenant" TEXT NOT NULL, "body" TEXT, '
        '"summary" TEXT GENERATED ALWAYS AS (left(body, 10)) STORED, "ref" TEXT, '
        'PRIMARY KEY ("tenant", "id"))'))}
    return SourceInventory(tables=[docs, notes]), applied


class _Mysql:
    source_config = None

    def __init__(self):
        from dsql_migrator.core.models import SourceType

        self.restored_source_type = SourceType.MYSQL


def test_recovery_candidates_drop_every_refused_column():
    from dsql_migrator.ui.data_migration._models import make_lob_candidates_for

    inv, applied = _lob_inventory()
    state = _state_with([])
    # Without the state (the old two-argument call) every LOB column is offered.
    assert {c for c, _ in make_lob_candidates_for(inv, _Mysql())("shop.docs")} == {
        "tenant", "body", "summary", "ref"}
    state.set_applied_conversions(applied, preserve_foreign_keys=True)
    # With it: the composite key's leading column (B2), the generated column (B4) and the
    # FK-referenced column (B3) are gone -- only the plain column can be excluded & reloaded.
    assert make_lob_candidates_for(inv, _Mysql(), state)("shop.docs") == (("body", "longtext"),)
    # B3 follows the FK toggle.
    state.set_applied_conversions(applied, preserve_foreign_keys=False)
    assert {c for c, _ in make_lob_candidates_for(inv, _Mysql(), state)("shop.docs")} == {"body", "ref"}


def test_click_time_refusal_writes_nothing_for_a_refused_column():
    from dsql_migrator.ui.data_migration._models import exclusion_refusal

    inv, applied = _lob_inventory()
    state = _state_with([])
    state.set_applied_conversions(applied, preserve_foreign_keys=True)
    refused = exclusion_refusal(state, inv, "shop.docs", ["body", "tenant"])
    assert "shop.docs.tenant cannot be excluded" in refused and refused.endswith("Nothing was changed.")
    assert "shop.docs.body" not in refused  # the plain column is not the problem
    assert "no longer" not in refused and "not in the source schema" in exclusion_refusal(
        state, inv, "shop.docs", ["gone"])
    assert exclusion_refusal(state, inv, "shop.docs", ["body"]) is None
    assert exclusion_refusal(state, None, "shop.docs", ["tenant"]) is None
    assert exclusion_refusal(state, inv, "shop.docs", []) is None


def test_both_recovery_actions_check_the_rule_before_writing():
    """The two actions are closures inside ``build_data_migration_screen``; pin the order."""
    import ast
    import inspect

    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    for name in ("exclude_lob_and_reload", "exclude_cdc_capture_columns"):
        (fn,) = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name]
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)]
        refusal = min(c.lineno for c in calls if getattr(c.func, "id", None) == "exclusion_refusal")
        write = min(c.lineno for c in calls if getattr(c.func, "attr", None) == "set_lob_exclusion")
        assert refusal < write, name
        (guard,) = [n for n in ast.walk(fn) if isinstance(n, ast.If) and ast.unparse(n.test) == "refused"]
        assert any(isinstance(b, ast.Return) for b in guard.body), name
    assert "make_lob_candidates_for(inventory, session, migration_state)" in inspect.getsource(
        dm.build_data_migration_screen)


def test_card_refuses_a_blocked_candidate_but_lets_a_ticked_one_be_cleared():
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_ui_data_migration import _RecordingUi

    inv, applied = _lob_inventory()
    state = _state_with([("shop.docs", "summary")])
    state.set_applied_conversions(applied, preserve_foreign_keys=True)
    ui = _RecordingUi()
    _cdc_ui._render_cdc_lob_exclusion_panel(
        ui, state, inv, lambda: None, locked=False, migration_wide=True)
    boxes = {b.label: b for b in ui.checkboxes}
    assert boxes["shop.docs.body"].enabled and boxes["shop.docs.body"].on_change
    for refused in ("shop.docs.tenant", "shop.docs.ref"):
        assert not boxes[refused].enabled and boxes[refused].on_change is None, refused
    # Already ticked (a saved B4 entry): still clearable from its box.
    assert boxes["shop.docs.summary"].enabled and boxes["shop.docs.summary"].on_change
    joined = " ".join(ui.texts)
    assert "Part of the target primary key" in joined and "Referenced by shop.notes.fk_doc" in joined
