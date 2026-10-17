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
_ORDERS_DDL = (
    'CREATE TABLE "shop"."orders" ("id" INT NOT NULL, "tenant" TEXT NOT NULL, '
    '"user_email" TEXT, "note" TEXT, "total2" INT GENERATED ALWAYS AS (id * 2) STORED, '
    'PRIMARY KEY ("tenant", "id"))'
)
_FK_DDL = (
    'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_user" FOREIGN KEY ("user_email") '
    'REFERENCES "shop"."users" ("email") NOT VALID'
)
APPLIED = {
    # Schema Conversion re-keyed orders on (tenant, id), kept a generated column, and the
    # FK pass will add fk_user -- the shape applied_table_conversions returns.
    "shop.orders": SimpleNamespace(
        target_ddl=_ORDERS_DDL, foreign_key_ddls=[_FK_DDL],
        preserved_foreign_keys=list(CHILD.foreign_keys)),
}
# The same with "Preserve foreign keys" off: applied_table_conversions empties the DDLs.
APPLIED_NO_FK = {
    "shop.orders": SimpleNamespace(
        target_ddl=_ORDERS_DDL, foreign_key_ddls=[],
        preserved_foreign_keys=list(CHILD.foreign_keys)),
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
    stripped = build_exclusion_context(INV, APPLIED_NO_FK, preserve_foreign_keys=False)
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
    val = (ui / "validation.py").read_text(encoding="utf-8")
    # Both builders (run and re-check) take the omitted set from the ONE split, which
    # starts from the effective set.
    assert val.count("excluded_columns=_split[0]") == 2
    assert val.count("_split = _exclusion_split(") == 2
    assert "effective_lob_exclusions(migration_state, inv, **kwargs)," in val
    # CDC: every builder, the Start/Deploy gates and the CDC-only preview go through the ONE
    # helper (effective set + captured generated columns + the chooser); none formats alone.
    cdc = (root / "_cdc_ui.py").read_text(encoding="utf-8")
    assert cdc.count("cdc_capture_exclusions(migration_state, inventory, tables_for_config)") == 3
    assert "format_column_exclude_list(" not in cdc and "cdc_excluded_columns(" not in cdc
    assert "format_column_exclude_list(" not in (root / "_cdc_monitoring.py").read_text(
        encoding="utf-8")


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
    state.set_applied_conversions(APPLIED_NO_FK, preserve_foreign_keys=False)
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
    assert one.endswith("Remove it in Exclude tables & columns (under the table list) first.")
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

    The card's ``locked`` must come from the SAME helper every gate uses
    (``exclusion_card_locked``); the in-content guard, the header Run and ``runner()`` must
    all read ``data_migration_exclusion_block``; and the render must record what the gates
    judge against (the resolved selection and the applied conversion).
    """
    import ast
    import inspect

    import dsql_migrator.ui.app as app
    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))

    def _calls(name, t=tree):
        return [
            n for n in ast.walk(t)
            if isinstance(n, ast.Call) and getattr(n.func, "id", getattr(n.func, "attr", None)) == name
        ]

    def _kw(call, key):
        return next(ast.unparse(k.value) for k in call.keywords if k.arg == key)

    # P3-C22: the old card is gone; the popup's row is rendered under the tree.
    assert not _calls("_render_cdc_lob_exclusion_panel")
    assert _calls("render_exclusion_button_row")
    (record_sel,) = _calls("set_resolved_selection")
    assert ast.unparse(record_sel.args[0]) == "selected_names"
    # In-content guard + runner() defense in depth.
    assert len(_calls("data_migration_exclusion_block")) == 2
    (guard,) = _calls("full_load_run_guard_reason")
    assert _kw(guard, "exclusion_block") == "_exclusion_block"
    (record,) = _calls("set_applied_conversions")
    assert ast.unparse(record.args[0]) == "_applied"
    assert _kw(record, "preserve_foreign_keys") == "conv_state.preserve_foreign_keys"
    # The page header's Run reads the same gate.
    app_tree = ast.parse(inspect.getsource(app))
    header = [c for c in _calls("full_load_run_guard_reason", app_tree)]
    assert header and all(
        "data_migration_exclusion_block(" in _kw(c, "exclusion_block") for c in header)


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
    # P3-C19: the quarantine route writes only through the popup's Save
    # (set_column_exclusions), never set_lob_exclusion.
    from dsql_migrator.ui.data_migration import _exclusion_dialog

    route = inspect.getsource(_exclusion_dialog.open_exclusion_route)
    assert "set_lob_exclusion" not in route and "exclude_lob_and_reload" not in inspect.getsource(
        dm.build_data_migration_screen)
    # P3-C20: the dead-letter route only records a pending rule (never an exclusion).
    dlq = inspect.getsource(_exclusion_dialog.open_dlq_route)
    assert "set_lob_exclusion" not in dlq and "add_pending_rule" in dlq
    assert "make_lob_candidates_for(inventory, session, migration_state)" in inspect.getsource(
        dm.build_data_migration_screen)


# ---------------------------------------------------------------------------
# A source column the applied target DDL lacks (42703): reported, never auto-excluded
# ---------------------------------------------------------------------------


def test_target_column_names_fold_like_postgresql():
    from dsql_migrator.core.converter import parse_target_column_names

    ddl = 'CREATE TABLE t ("MixedCase" TEXT, UPPER_UNQUOTED INT, "id" INT PRIMARY KEY)'
    assert parse_target_column_names(ddl) == frozenset({"MixedCase", "upper_unquoted", "id"})
    identity = 'CREATE TABLE t ("id" BIGINT GENERATED BY DEFAULT AS IDENTITY (CACHE 1) PRIMARY KEY, "v" TEXT)'
    assert parse_target_column_names(identity) == frozenset({"id", "v"})
    assert parse_target_column_names("not sql at all (") is None
    assert parse_target_column_names("SELECT 1") is None


def test_missing_target_columns_skips_excluded_and_unknown_tables():
    from dsql_migrator.core.column_exclusion import missing_target_columns

    users = _t("shop.users", ["id", "email", "bio", "Nick"])
    applied = {"shop.users": SimpleNamespace(target_ddl='CREATE TABLE u ("id" INT PRIMARY KEY, email TEXT, NICK TEXT)')}
    # bio was dropped from the DDL; "Nick" is quoted by the INSERT but the DDL's unquoted
    # NICK folds to "nick", so it is missing too -- PostgreSQL would say 42703 for both.
    assert missing_target_columns([users], applied) == {"shop.users": ("bio", "Nick")}
    assert missing_target_columns([users], applied, {"shop.users": {"bio", "Nick"}}) == {}
    # No applied conversion / unparseable DDL: unknown, never reported.
    assert missing_target_columns([users], {}) == {}
    assert missing_target_columns(
        [users], {"shop.users": SimpleNamespace(target_ddl="garbage(")}) == {}


def test_missing_column_block_reason_uses_the_recorded_conversion_and_effective_set():
    from dsql_migrator.ui.data_migration._models import missing_column_block_reason

    applied = {
        "shop.users": SimpleNamespace(target_ddl='CREATE TABLE u ("id" INT PRIMARY KEY, "email" TEXT)'),
        "shop.orders": SimpleNamespace(target_ddl=APPLIED["shop.orders"].target_ddl),
    }
    state = _state_with([])
    assert missing_column_block_reason(state, INV) is None  # nothing recorded yet: no claim
    state.set_applied_conversions(applied, preserve_foreign_keys=True)
    text = missing_column_block_reason(state, INV)
    assert text.startswith("shop.users.bio is not in your Aurora DSQL table;")
    assert "(42703, undefined column)" in text and "never dropped from a migration automatically" in text
    # Excluding it is what makes it fine -- and only the tables being migrated count.
    state.set_lob_exclusion("shop.users", "bio", True)
    assert missing_column_block_reason(state, INV) is None
    state.set_lob_exclusion("shop.users", "bio", False)
    assert missing_column_block_reason(state, INV, selected_tables=["shop.orders"]) is None


def test_deploy_gate_blocks_a_certain_missing_column_whatever_the_lock():
    """42703's remedy is Schema Conversion, which no lock blocks -- so it is not tied to the
    exclusion card's lock; it is skipped only while CDC streams."""
    from dsql_migrator.ui.data_migration._cdc_ui import _stored_exclusion_deploy_block

    from tests.test_ui_data_migration import _StubJob, _StubJobManager

    state = _state_with([])
    state.set_applied_conversions(
        {"shop.users": SimpleNamespace(target_ddl='CREATE TABLE u ("id" INT PRIMARY KEY, "email" TEXT)')},
        preserve_foreign_keys=True,
    )
    msg = "shop.users.bio is not in your Aurora DSQL table"
    assert msg in _stored_exclusion_deploy_block(state, _StubJobManager({}), INV, None)
    state.job_id = "job-1"  # the card is locked -- the 42703 gate still holds
    assert msg in _stored_exclusion_deploy_block(
        state, _StubJobManager({"job-1": _StubJob("COMPLETED")}), INV, None)
    state.set_cdc_stack_phase("running", status="UPDATE_COMPLETE")  # streaming: skipped
    assert _stored_exclusion_deploy_block(
        state, _StubJobManager({"job-1": _StubJob("COMPLETED")}), INV, None) is None

def test_memory_pressure_copy_does_not_offer_a_locked_exclusion_mid_load():
    """The warning fires DURING a load, when the exclusion card is already locked."""
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine

    src = inspect.getsource(_full_load_engine)
    assert "exclude oversized-LOB columns, or" not in src
    # P3-C22: both copies name the popup and its Change & reload, never the old card.
    assert "Oversized LOB columns" not in src
    assert src.count("Change & reload reloads the tables.") == 2


def test_b3_and_b4_follow_the_edited_script_not_the_source():
    """An FK removed from the edited script no longer blocks; one ADDED to it does; a
    GENERATED clause removed by an edit leaves an ordinary, excludable column."""
    edited = {"shop.orders": SimpleNamespace(
        target_ddl=_ORDERS_DDL.replace(" GENERATED ALWAYS AS (id * 2) STORED", ""),
        foreign_key_ddls=[  # fk_user deleted; a new FK on note -> users.bio added
            'ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_bio" FOREIGN KEY ("note") '
            'REFERENCES "shop"."users" ("bio") NOT VALID'],
        preserved_foreign_keys=list(CHILD.foreign_keys))}
    ctx = build_exclusion_context(INV, edited)
    assert not column_exclusion_verdict("shop.users", "email", ctx).blocked
    bio = column_exclusion_verdict("shop.users", "bio", ctx)
    assert _codes(bio) == {B3} and "shop.orders.fk_bio" in bio.findings[0].message
    assert not column_exclusion_verdict("shop.orders", "total2", ctx).blocked
    # Unquoted / unqualified REFERENCES spellings resolve the same way PostgreSQL does.
    unquoted = {"shop.orders": SimpleNamespace(
        target_ddl=_ORDERS_DDL, preserved_foreign_keys=[],
        foreign_key_ddls=['ALTER TABLE shop.orders ADD CONSTRAINT fk2 FOREIGN KEY (note) '
                          'REFERENCES shop.Users (BIO, "Email") NOT VALID'])}
    ctx2 = build_exclusion_context(
        SourceInventory(tables=[_t("shop.users", ["id", "bio", "Email"]), CHILD]), unquoted)
    assert _codes(column_exclusion_verdict("shop.users", "bio", ctx2)) == {B3}
    assert _codes(column_exclusion_verdict("shop.users", "Email", ctx2)) == {B3}
    # A statement whose REFERENCES cannot be read makes no claim.
    garbled = {"shop.orders": SimpleNamespace(target_ddl=_ORDERS_DDL, preserved_foreign_keys=[],
                                              foreign_key_ddls=["ALTER TABLE x ADD CONSTRAINT y FOREIGN KEY"])}
    assert not column_exclusion_verdict("shop.users", "email", build_exclusion_context(INV, garbled)).blocked


def test_validation_recognises_a_composite_key_after_a_restore(monkeypatch):
    """A restored session reaching Validation before any Data Migration render has no
    recorded conversion; the effective set must still drop the composite key's column."""
    import inspect

    import dsql_migrator.ui.validation as validation

    body = inspect.getsource(validation.session_exclusion_split)
    # Only when nothing is recorded, and with the Schema Conversion state's own inputs.
    assert "if not recorded and conv_state is not None:" in body
    assert "conv_state.edited_target_ddls" in body and "conv_state.preserve_foreign_keys" in body
    # The shape it relies on: an explicit applied conversion drops B2 with no recorded state.
    from dsql_migrator.ui.data_migration._models import effective_lob_exclusions

    state = _state_with([("shop.orders", "tenant"), ("shop.orders", "note")])
    assert effective_lob_exclusions(state, INV)["shop.orders"] == frozenset({"tenant", "note"})
    assert effective_lob_exclusions(state, INV, applied_conversions=APPLIED)["shop.orders"] == frozenset({"note"})


# ---------------------------------------------------------------------------
# Review fixes: one lock, one scope, every entry point, honest post-lock copy
# ---------------------------------------------------------------------------


def _session(status=None):
    from dsql_migrator.ui.workflow import WorkflowStep, with_status, WorkflowState

    wf = WorkflowState()
    if status is not None:
        wf = with_status(wf, WorkflowStep.FULL_LOAD, status)
    return SimpleNamespace(workflow=wf)


def test_exclusion_card_locked_keys_on_the_recorded_full_load_not_the_step():
    from dsql_migrator.core.models import StepStatus
    from dsql_migrator.ui.data_migration import MigrationType, exclusion_card_locked

    from tests.test_ui_data_migration import _StubJob, _StubJobManager

    empty = _StubJobManager({})
    for mtype in (MigrationType.FULL_LOAD_AND_CDC, MigrationType.CDC_ONLY):
        state = _state_with([])
        state.migration_type = mtype
        # CDC alone promotes the FULL_LOAD step to DONE: that is NOT a committed Full Load.
        assert exclusion_card_locked(state, empty, _session(StepStatus.DONE)) is False, mtype
        state.job_id = "job-1"
        # A recorded Full Load locks, whether or not its job record survived.
        assert exclusion_card_locked(state, empty, _session()), mtype
        assert exclusion_card_locked(state, _StubJobManager({"job-1": _StubJob("COMPLETED")}), None)
    fresh = _state_with([])
    fresh.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    assert exclusion_card_locked(fresh, empty, _session())  # a stack locks too

def test_every_gate_judges_the_tables_the_render_resolved():
    from tests.test_ui_data_migration import _StubJobManager
    from dsql_migrator.ui.data_migration._cdc_ui import _stored_exclusion_deploy_block

    state = _state_with([("shop.users", "id")])
    assert _stored_exclusion_deploy_block(state, _StubJobManager({}), INV, None)
    state.set_resolved_selection(["shop.orders"])  # users not migrated: its entry is inert
    assert _stored_exclusion_deploy_block(state, _StubJobManager({}), INV, None) is None
    state.set_resolved_selection([])  # an explicit "unselect all" judges nothing
    assert _stored_exclusion_deploy_block(state, _StubJobManager({}), INV, None) is None


def test_attach_is_held_like_deploy_and_refuses_at_click_time():
    from dsql_migrator.ui.data_migration import _cdc_ui

    from tests.test_ui_data_migration import _RecordingUi, _StubJobManager

    state = _state_with([("shop.users", "id")])
    state.set_cdc_other_stacks([("dsql-cdc-old", "CREATE_COMPLETE")])
    ui = _RecordingUi()
    _cdc_ui._render_cdc_adopt_or_deploy_choice(
        ui, state, _StubJobManager({}), lambda: None, [("dsql-cdc-old", "CREATE_COMPLETE")],
        inventory=INV, session=None)
    (attach,) = [b for b in ui.buttons if b.text == "Attach to dsql-cdc-old"]
    assert "disable" in attach.props_seen
    assert "shop.users.id cannot be excluded" in " ".join(ui.texts)
    attach.on_click()  # a stale enabled button must still refuse
    assert state.cdc_stack_name != "dsql-cdc-old"
    # Nothing to remove -> Attach offered normally.
    clean = _state_with([])
    ui2 = _RecordingUi()
    _cdc_ui._render_cdc_adopt_or_deploy_choice(
        ui2, clean, _StubJobManager({}), lambda: None, [("dsql-cdc-old", "CREATE_COMPLETE")],
        inventory=INV, session=None)
    (ok,) = [b for b in ui2.buttons if b.text == "Attach to dsql-cdc-old"]
    assert "disable" not in ok.props_seen


def test_the_context_parses_each_ddl_once_across_renders():
    from dsql_migrator.core import column_exclusion as ce

    ce._ddl_key_facts.cache_clear()
    for _ in range(5):
        build_exclusion_context(INV, APPLIED)
    info = ce._ddl_key_facts.cache_info()
    assert info.misses == 1 and info.hits == 4


def test_an_edit_that_drops_the_generated_clause_makes_a_pg_generated_column_excludable():
    src_table = TableDef(
        name="app.items",
        columns=[ColumnDef(name="id", mysql_type="integer"),
                 ColumnDef(name="doubled", mysql_type="integer", generated=True,
                           generated_kind="STORED", generated_expression="id * 2")],
        primary_key=["id"])
    inv = SourceInventory(tables=[src_table])
    # No applied DDL (restored session): the source says DSQL will compute it -> B4.
    assert _codes(column_exclusion_verdict("app.items", "doubled", build_exclusion_context(inv))) == {B4}
    # The applied (edited) DDL made it an ordinary column: the DDL alone decides.
    edited = {"app.items": SimpleNamespace(
        target_ddl='CREATE TABLE "app"."items" ("id" INTEGER PRIMARY KEY, "doubled" INTEGER)',
        foreign_key_ddls=[], preserved_foreign_keys=[])}
    assert not column_exclusion_verdict("app.items", "doubled", build_exclusion_context(inv, edited)).blocked


# --- P3-C05: column warnings W1-W6 and table-exclusion findings ---------------------

from dsql_migrator.core.column_exclusion import (  # noqa: E402
    W1, W2, W3, W4, W5, W6,
    column_exclusion_warnings,
    names_in,
    table_exclusion_findings,
    views_referencing,
)
from dsql_migrator.core.models import IndexDef  # noqa: E402

_W_DDL = (
    'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, "code" VARCHAR(10) NOT NULL DEFAULT \'\', '
    '"blob" BYTEA DEFAULT \'\\x00\'::bytea, "due" TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + INTERVAL '
    '\'1 day\'), "cust_id" BIGINT, "email" TEXT UNIQUE, "a" INT, "b" INT, "price" NUMERIC, '
    '"total" NUMERIC GENERATED ALWAYS AS (price * 2) STORED, "label" TEXT NOT NULL GENERATED ALWAYS '
    'AS (upper(code)) STORED, "notes" TEXT, PRIMARY KEY ("id"), UNIQUE ("a", "b"))'
)


def _w_ctx(*, preserve=True, ddl=_W_DDL, views=None, index_ddls=(), fk_ddls=None):
    orders = TableDef(
        name="shop.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="code", mysql_type="varchar(10)", nullable=False, default="''"),
            ColumnDef(name="blob", mysql_type="bytea"),
            ColumnDef(name="due", mysql_type="timestamp"),
            ColumnDef(name="cust_id", mysql_type="bigint"),
            ColumnDef(name="email", mysql_type="text"),
            ColumnDef(name="a", mysql_type="integer"),
            ColumnDef(name="b", mysql_type="integer"),
            ColumnDef(name="price", mysql_type="numeric"),
            ColumnDef(name="total", mysql_type="numeric", generated=True,
                      generated_expression="(price * (2)::numeric)"),
            ColumnDef(name="label", mysql_type="text", nullable=False, generated=True,
                      generated_expression="upper((code)::text)"),
            ColumnDef(name="notes", mysql_type="text"),
        ],
        primary_key=["id"],
        foreign_keys=[ForeignKeyDef(name="fk_cust", columns=["cust_id"],
                                    referenced_table="shop.customers", referenced_columns=["id"])],
    )
    customers = TableDef(name="shop.customers",
                         columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
    if fk_ddls is None:
        fk_ddls = ['ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_cust" FOREIGN KEY ("cust_id") '
                   'REFERENCES "shop"."customers" ("id") NOT VALID'] if preserve else []
    conv = SimpleNamespace(target_ddl=ddl, foreign_key_ddls=fk_ddls,
                           preserved_foreign_keys=orders.foreign_keys, index_ddls=list(index_ddls))
    return build_exclusion_context(
        SourceInventory(tables=[orders, customers]), {"shop.orders": conv},
        preserve_foreign_keys=preserve, view_ddls=views or {}, warnings=True,
    )


def _wcodes(found):
    return [f.code for f in found]


def test_w1_and_w2_read_the_applied_ddl_and_quote_its_default():
    ctx = _w_ctx()
    code = column_exclusion_warnings("shop.orders", "code", ctx)
    assert _wcodes(code) == [W1, W2, W5]
    assert code[0].message == (
        "NOT NULL will be dropped on Aurora DSQL. One-way: Aurora DSQL cannot add NOT NULL back "
        "in place (a later Drop & reload of this table restores it).")
    assert code[1].message == (
        "DEFAULT '' is dropped for the migration and restored at Cut over (new rows only).")
    # As the applied DDL writes it, not sqlglot's render (CAST(... AS BYTEA), '1 DAY').
    assert column_exclusion_warnings("shop.orders", "blob", ctx)[0].message.startswith(
        "DEFAULT '\\x00'::bytea is dropped")
    assert "DEFAULT (CURRENT_TIMESTAMP + INTERVAL '1 day') is dropped" in (
        column_exclusion_warnings("shop.orders", "due", ctx)[0].message)
    assert column_exclusion_warnings("shop.orders", "notes", ctx) == ()


def test_w3_only_while_foreign_keys_are_preserved():
    (w3,) = column_exclusion_warnings("shop.orders", "cust_id", _w_ctx())
    assert w3.code == W3 and w3.message == (
        "Its relationship to shop.customers is not migrated (NULL passes the foreign key).")
    assert column_exclusion_warnings("shop.orders", "cust_id", _w_ctx(preserve=False)) == ()


def test_w4_inline_table_and_index_uniqueness():
    ctx = _w_ctx(index_ddls=['CREATE UNIQUE INDEX ASYNC "ux_notes" ON "shop"."orders" ("notes")',
                             'CREATE INDEX ASYNC "ix_price" ON "shop"."orders" ("price")'])
    for column in ("email", "a", "b", "notes"):
        found = column_exclusion_warnings("shop.orders", column, ctx)
        assert W4 in _wcodes(found), column
        assert next(f for f in found if f.code == W4).message == (
            "Uniqueness is not enforced for migrated rows (all NULL).")
    assert W4 not in _wcodes(column_exclusion_warnings("shop.orders", "price", ctx))


def test_w5_names_the_generated_column_and_escalates_when_it_is_not_null():
    ctx = _w_ctx()
    price = column_exclusion_warnings("shop.orders", "price", ctx)
    assert [(f.code, f.message) for f in price] == [
        (W5, "total will be computed from NULL and differ from the source.")]
    escalated = next(f for f in column_exclusion_warnings("shop.orders", "code", ctx) if f.code == W5)
    assert escalated.message == (
        "label is NOT NULL and computed from this column: if UPPER(code) yields NULL "
        "when this column is NULL, every row is rejected (23502).")
    # an identifier boundary: "a" does not match inside "label" or "upper"
    assert W5 not in _wcodes(column_exclusion_warnings("shop.orders", "a", ctx))


def test_w6_view_text_match():
    views = {"shop.v_orders": 'CREATE VIEW "shop"."v_orders" AS SELECT id, notes FROM "shop"."orders"',
             "shop.v_other": 'CREATE VIEW "shop"."v_other" AS SELECT notes FROM "shop"."customers"'}
    (w6,) = column_exclusion_warnings("shop.orders", "notes", _w_ctx(views=views))
    assert w6.code == W6 and w6.message == (
        "View shop.v_orders mentions it (text match) and will read NULL.")


def test_table_findings_say_left_as_is_and_scope_the_fk_line_to_selected_children():
    views = {"shop.v_cust": 'CREATE VIEW "shop"."v_cust" AS SELECT * FROM "shop"."customers"'}
    ctx = _w_ctx(views=views)
    found = table_exclusion_findings("shop.customers", ctx)
    messages = [f.message for f in found]
    assert messages[0] == "Not loaded by this migration; left as is on Aurora DSQL (not dropped)."
    assert not any("stays empty" in m for m in messages)
    # Truthful about what the orphan check does (G12): rows an earlier load left on Aurora
    # DSQL satisfy it, and a table absent from the target means the key is not applied.
    assert ("shop.orders.fk_cust references it. When foreign keys are applied (Data "
            "Migration for Full load only; Cut over for CDC), that key is checked against the "
            "rows this table has on Aurora DSQL: applied if every referenced row is there, "
            "skipped by the orphan check if one is missing, and not applied if the table is "
            "not on Aurora DSQL.") in messages
    assert "View shop.v_cust will return no rows." in messages
    assert messages[-1] == "Column rules on it have no effect while it is unselected."
    unselected_child = build_exclusion_context(
        SourceInventory(tables=list(ctx.tables.values())), selected_tables=["shop.customers"],
        warnings=True)
    assert not any("orphan" in f.message for f in table_exclusion_findings(
        "shop.customers", unselected_child))


def test_warnings_never_leak_into_the_verdict_paths():
    plain = _w_ctx()
    stored = {"shop.orders": {"id", "code", "total", "cust_id"}}
    import dataclasses
    bare = build_exclusion_context(
        SourceInventory(tables=list(plain.tables.values())),
        {"shop.orders": SimpleNamespace(
            target_ddl=_W_DDL, preserved_foreign_keys=plain.tables["shop.orders"].foreign_keys,
            foreign_key_ddls=['ALTER TABLE "shop"."orders" ADD CONSTRAINT "fk_cust" FOREIGN KEY '
                              '("cust_id") REFERENCES "shop"."customers" ("id") NOT VALID'],
            index_ddls=[])},
    )
    for committed in (False, True):
        assert check_stored_exclusions(stored, plain, committed=committed) == (
            check_stored_exclusions(stored, bare, committed=committed))
    assert effective_exclusions(stored, plain) == effective_exclusions(stored, bare)
    for f in dataclasses.fields(bare):
        if f.name in ("tables", "target_pk", "message_key_columns", "generated", "fk_referenced"):
            assert getattr(bare, f.name) == getattr(plain, f.name), f.name
    assert bare.target_constraints == {} and bare.unique_columns == {}


def test_source_fallback_without_an_applied_ddl():
    t = TableDef(name="s.t", columns=[
        ColumnDef(name="id", mysql_type="bigint", nullable=False),
        ColumnDef(name="c", mysql_type="varchar(5)", nullable=False, default="'x'"),
        ColumnDef(name="u", mysql_type="int")], primary_key=["id"],
        indexes=[IndexDef(name="ux", columns=["u"], unique=True)])
    ctx = build_exclusion_context(SourceInventory(tables=[t]), warnings=True)
    assert _wcodes(column_exclusion_warnings("s.t", "c", ctx)) == [W1, W2]
    assert _wcodes(column_exclusion_warnings("s.t", "u", ctx)) == [W4]


def test_each_ddl_is_parsed_once_for_repeated_calls():
    from dsql_migrator.core import column_exclusion as ce

    ce._unique_members.cache_clear()
    for _ in range(5):
        _w_ctx(index_ddls=['CREATE UNIQUE INDEX ASYNC "ux_n" ON "shop"."orders" ("notes")'])
    info = ce._unique_members.cache_info()
    assert info.misses == 1 and info.hits == 4


def test_the_view_helpers_moved_to_core_unchanged():
    from dsql_migrator.ui.data_migration import _full_load_engine as engine

    assert engine._views_referencing is views_referencing and engine._names_in is names_in


# --- review fixes (C05-C11 review): views by reference, W5 from the applied DDL --------


from dsql_migrator.core.column_exclusion import T3  # noqa: E402


def _view_ctx(tables, views, *, selected=None):
    return build_exclusion_context(SourceInventory(tables=tables), selected_tables=selected,
                                   view_ddls=views, warnings=True)


def _tbl(name, *cols):
    return TableDef(name=name, primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint")] + [ColumnDef(name=c, mysql_type="text") for c in cols])


def test_views_are_matched_by_what_they_read_not_by_the_bare_name():
    tables = [_tbl("tenant1.orders", "notes"), _tbl("tenant2.orders", "notes"),
              _tbl("shop.products", "category"), _tbl("shop.category", "name")]
    views = {
        "tenant2.v_orders": 'CREATE VIEW "tenant2"."v_orders" AS SELECT "tenant2"."orders"."notes" '
                            'AS "notes" FROM "tenant2"."orders"',
        "shop.v_products": 'CREATE VIEW "shop"."v_products" AS SELECT "shop"."products"."category" '
                           'AS "category" FROM "shop"."products"',
    }
    ctx = _view_ctx(tables, views)
    assert not any(f.code == T3 for f in table_exclusion_findings("tenant1.orders", ctx))
    assert W6 not in _wcodes(column_exclusion_warnings("tenant1.orders", "notes", ctx))
    assert not any(f.code == T3 for f in table_exclusion_findings("shop.category", ctx))
    assert any(f.code == T3 for f in table_exclusion_findings("tenant2.orders", ctx))
    assert W6 in _wcodes(column_exclusion_warnings("tenant2.orders", "notes", ctx))
    assert views_referencing(views, frozenset({"tenant1.orders"}))  # the drop path is unchanged


def test_the_view_scan_runs_once_per_table_not_per_column(monkeypatch):
    from dsql_migrator.core import column_exclusion as ce

    tables = [_tbl(f"s.t{i}", "x", "y") for i in range(50)]
    views = {f"s.v{i}": f'CREATE VIEW "s"."v{i}" AS SELECT x FROM "s"."t{i}"' for i in range(20)}
    ctx = _view_ctx(tables, views)
    calls = {"n": 0}
    real = ce._view_table_refs

    def counting(ddl):
        calls["n"] += 1
        return real(ddl)

    monkeypatch.setattr(ce, "_view_table_refs", counting)
    for t in tables:
        for c in ("x", "y"):
            column_exclusion_warnings(t.name, c, ctx)
    assert calls["n"] == 50 * 20  # once per (table, view), never per column


def test_w5_reads_the_applied_expression():
    from types import SimpleNamespace

    t = TableDef(name="s.t", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="price", mysql_type="numeric"),
        ColumnDef(name="qty", mysql_type="integer"),
        ColumnDef(name="total", mysql_type="numeric", generated=True,
                  generated_expression="(price * (2)::numeric)")])
    ddl = ('CREATE TABLE "s"."t" ("id" BIGINT NOT NULL, "price" NUMERIC, "qty" INT, '
           '"total" NUMERIC NOT NULL GENERATED ALWAYS AS ((qty * 2)) STORED, PRIMARY KEY ("id"))')
    ctx = build_exclusion_context(SourceInventory(tables=[t]), {"s.t": SimpleNamespace(
        target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[], index_ddls=[])}, warnings=True)
    (w5,) = column_exclusion_warnings("s.t", "qty", ctx)
    assert w5.code == W5 and "(qty * 2)" in w5.message and "every row is rejected (23502)" in w5.message
    assert column_exclusion_warnings("s.t", "price", ctx) == ()


def test_legacy_notes_replace_the_overlay_story():
    from types import SimpleNamespace

    from dsql_migrator.core.column_exclusion import WL, WL_REJECT, legacy_exclusion_notes

    t = TableDef(name="shop.acct", primary_key=["id"], columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="code", mysql_type="text"),
        ColumnDef(name="note", mysql_type="text"), ColumnDef(name="must", mysql_type="text")])
    ddl = ('CREATE TABLE "shop"."acct" ("id" BIGINT NOT NULL, "code" TEXT NOT NULL DEFAULT \'x\', '
           '"note" TEXT, "must" TEXT NOT NULL, PRIMARY KEY ("id"))')
    conv = SimpleNamespace(target_ddl=ddl, foreign_key_ddls=[], preserved_foreign_keys=[],
                           index_ddls=['CREATE UNIQUE INDEX ASYNC "u" ON "shop"."acct" ("code")'])
    ctx = build_exclusion_context(SourceInventory(tables=[t]), {"shop.acct": conv}, warnings=True)
    (code,) = legacy_exclusion_notes("shop.acct", "code", ctx)
    assert code.code == WL_REJECT and "DEFAULT 'x'" in code.message and "23505" in code.message
    (note,) = legacy_exclusion_notes("shop.acct", "note", ctx)
    assert note.code == WL and "stores NULL" in note.message
    (must,) = legacy_exclusion_notes("shop.acct", "must", ctx)
    assert must.code == WL_REJECT and "23502" in must.message


def test_w2_quotes_each_default_as_the_applied_ddl_writes_it():
    """The note quoted sqlglot's render -- CURRENT_TIME(), a dropped time zone -- as the
    DEFAULT 'restored at Cut over', where the DDL (and the restore) carry PostgreSQL's own."""
    from types import SimpleNamespace

    ddl = (
        'CREATE TABLE "s"."t" ("id" bigint NOT NULL PRIMARY KEY, '
        "\"day\" timestamptz DEFAULT date_trunc('day'::text, now(), 'Asia/Seoul'::text), "
        '"clock" time DEFAULT CURRENT_TIME NOT NULL)'
    )
    t = TableDef(name="s.t", columns=[
        ColumnDef(name="id", mysql_type="bigint", nullable=False),
        ColumnDef(name="day", mysql_type="timestamptz"),
        ColumnDef(name="clock", mysql_type="time", nullable=False)], primary_key=["id"])
    ctx = build_exclusion_context(
        SourceInventory(tables=[t]),
        applied_conversions={"s.t": SimpleNamespace(target_ddl=ddl, index_ddls=[])},
        warnings=True,
    )
    notes = {f.code: f.message for f in column_exclusion_warnings("s.t", "day", ctx)}
    assert "DEFAULT date_trunc('day'::text, now(), 'Asia/Seoul'::text) is dropped" in notes[W2]
    notes = {f.code: f.message for f in column_exclusion_warnings("s.t", "clock", ctx)}
    assert "DEFAULT CURRENT_TIME is dropped" in notes[W2]


def test_w2_quotes_a_case_default_whole():
    """PostgreSQL deparses an ``ELSE NULL::text`` into every CASE, and the scanner stopped at
    that NULL -- quoting ``CASE WHEN ... ELSE`` as the DEFAULT 'restored at Cut over'."""
    from dsql_migrator.core.converter import parse_target_column_default_texts

    case = "CASE WHEN (now() > '2020-01-01'::timestamptz) THEN 'KRW'::text ELSE NULL::text END"
    ddl = (
        'CREATE TABLE "s"."t" ("id" bigint NOT NULL, '
        f'"cur" text DEFAULT {case} NOT NULL, '
        '"nested" text DEFAULT (CASE WHEN true THEN CASE WHEN false THEN NULL END END), '
        '"n" int DEFAULT 5)'
    )
    assert parse_target_column_default_texts(ddl) == {
        "cur": case,
        "nested": "(CASE WHEN true THEN CASE WHEN false THEN NULL END END)",
        "n": "5",
    }
