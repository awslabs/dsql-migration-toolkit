# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the opt-in ADD / RENAME COLUMN drift recovery (core/cdc_schema_evolve).

The planner is pure, so these run with no MySQL/DSQL/AWS: the column lists are
passed in directly and the applier gets a fake connection factory.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.cdc_catalog_drift import LiveColumn, LiveTable
from dsql_migrator.core.cdc_schema_evolve import (
    HELD_EXCLUDED_GONE,
    HELD_GENERATED,
    HELD_NAME_TAKEN,
    HELD_RENAMED_EXCLUDED,
    HELD_RULE,
    HELD_RULES_UNKNOWN,
    AddColumnPlan,
    AddColumnStep,
    HeldColumn,
    KeyRefusal,
    RenameColumnStep,
    apply_add_columns,
    plan_add_columns,
    plan_target_fix,
    read_source_table,
    read_target_columns,
)
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef


# --------------------------------------------------------------------------- #
# planner
# --------------------------------------------------------------------------- #
def test_plan_offers_only_the_columns_the_target_lacks() -> None:
    plan = plan_add_columns(
        "ecommerce.users",
        [("id", "bigint"), ("email", "varchar(255)"), ("nickname", "varchar(64)")],
        ["id", "email"],
    )
    assert [step.column for step in plan.steps] == ["nickname"]
    assert plan.skipped == ()
    assert not plan.is_empty


def test_plan_is_empty_when_target_already_matches() -> None:
    # The common case after a re-run: nothing to do, and the UI must say so
    # rather than offering an empty ALTER.
    plan = plan_add_columns(
        "ecommerce.users", [("id", "bigint"), ("email", "varchar(255)")], ["id", "email"]
    )
    assert plan.is_empty
    assert plan.ddl_text == ""


def test_plan_renders_nullable_add_column_ddl_with_quoted_identifiers() -> None:
    # Nullable + no default is deliberate: NOT NULL without a default is rejected
    # on a populated table, and a fabricated default would invent data for rows
    # CDC already applied. Identifiers are quoted so a reserved word or mixed case
    # cannot break (or inject into) the statement.
    plan = plan_add_columns(
        "ecommerce.product_media", [("order", "int")], []
    )
    (step,) = plan.steps
    # Assert the RENDERING (quoting + shape), not the mapping table itself -- the
    # MySQL->DSQL type mapping has its own tests in test_converter.
    assert step.ddl == (
        'ALTER TABLE "ecommerce"."product_media" '
        f'ADD COLUMN "order" {step.target_type}'
    )
    assert step.ddl.startswith('ALTER TABLE "ecommerce"."product_media" ADD COLUMN ')
    assert '"order"' in step.ddl, "a reserved-word column must stay quoted"
    assert "NOT NULL" not in step.ddl
    assert "DEFAULT" not in step.ddl


def test_plan_preserves_source_ordinal_order() -> None:
    # Multi-column drift should read like the source's own change history.
    plan = plan_add_columns(
        "app.t",
        [("a", "int"), ("b", "int"), ("c", "int"), ("d", "int")],
        ["b"],
    )
    assert [step.column for step in plan.steps] == ["a", "c", "d"]


def test_plan_add_columns_compares_names_exactly() -> None:
    # The converter quotes and preserves identifiers (CREATE TABLE ... ("Full_Name" ...))
    # and the sink writes column names exactly, so a target `full_name` does NOT hold a
    # source `Full_Name`: CDC was already dead-lettering it. (Whether that is a case-only
    # rename is plan_target_fix's call.) The old case-insensitive diff returned an empty
    # plan here.
    plan = plan_add_columns(
        "app.t", [("Full_Name", "varchar(50)")], ["full_name"]
    )
    (step,) = plan.steps
    assert step.ddl == f'ALTER TABLE "app"."t" ADD COLUMN "Full_Name" {step.target_type}'
    assert step.target_type == "VARCHAR(50)"


def test_plan_skips_unmappable_type_instead_of_guessing() -> None:
    # Never approximate a type: report it so the operator decides.
    plan = plan_add_columns(
        "app.t", [("ok", "int"), ("weird", "nonexistent_type_xyz(9)")], []
    )
    assert [step.column for step in plan.steps] == ["ok"]
    assert [skipped.column for skipped in plan.skipped] == ["weird"]
    assert "no Aurora DSQL mapping" in plan.skipped[0].reason


def test_plan_keeps_the_converter_warning_for_a_lossy_mapping() -> None:
    # A spatial type maps to bytea WITH a warning; the plan must carry it so the
    # approval dialog can show the caveat rather than presenting it as clean.
    plan = plan_add_columns("app.t", [("shape", "geometry")], [])
    (step,) = plan.steps
    assert step.target_type == "bytea"
    assert step.warning is not None and "no Aurora DSQL equivalent" in step.warning


def test_plan_ddl_text_is_one_statement_per_line() -> None:
    plan = plan_add_columns("app.t", [("a", "int"), ("b", "text")], [])
    assert plan.ddl_text.splitlines() == [step.ddl for step in plan.steps]
    assert len(plan.ddl_text.splitlines()) == 2


# --------------------------------------------------------------------------- #
# PostgreSQL source: near-identity mapping (no MySQL remap), so ADD COLUMN
# recovery works on a PG source instead of erroring through map_mysql_type.
# --------------------------------------------------------------------------- #
def test_plan_postgres_emits_pg_types_verbatim() -> None:
    from dsql_migrator.core.models import SourceType

    plan = plan_add_columns(
        "public.users",
        [("id", "bigint"), ("tags", "text"), ("created", "timestamp with time zone")],
        ["id"],
        source_type=SourceType.POSTGRES,
    )
    # The PG types are emitted as-is (no MySQL->DSQL remap); ordinal order preserved.
    assert [(s.column, s.target_type) for s in plan.steps] == [
        ("tags", "text"),
        ("created", "timestamp with time zone"),
    ]
    assert plan.skipped == ()


def test_plan_postgres_skips_a_dsql_unsupported_type() -> None:
    from dsql_migrator.core.models import SourceType

    # A PG array/geometric/etc. type DSQL cannot store as a column is skipped, not
    # approximated (mirrors the converter's PG table path).
    plan = plan_add_columns(
        "public.t",
        [("ok", "integer"), ("labels", "text[]")],
        [],
        source_type=SourceType.POSTGRES,
    )
    assert [s.column for s in plan.steps] == ["ok"]
    assert [s.column for s in plan.skipped] == ["labels"]


# --------------------------------------------------------------------------- #
# applier
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, recorder, fail_on=None) -> None:
        self._recorder = recorder
        self._fail_on = fail_on

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None):
        if self._fail_on is not None and self._fail_on in sql:
            raise RuntimeError("boom: column already exists")
        self._recorder.append(sql)


class _FakeConnection:
    def __init__(self, recorder, fail_on=None) -> None:
        self._recorder = recorder
        self._fail_on = fail_on
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._recorder, self._fail_on)

    def close(self) -> None:
        self.closed = True


def test_apply_runs_one_ddl_per_fresh_connection() -> None:
    # Aurora DSQL rejects two DDL statements in one transaction, so each ALTER
    # must get its own (autocommit) connection -- assert one connection per step.
    executed: list[str] = []
    opened: list[_FakeConnection] = []

    def factory():
        conn = _FakeConnection(executed)
        opened.append(conn)
        return conn

    plan = plan_add_columns("app.t", [("a", "int"), ("b", "text")], [])
    outcomes = apply_add_columns(plan, factory)
    assert [o.applied for o in outcomes] == [True, True]
    assert len(opened) == 2, "each DDL needs its own transaction/connection"
    assert len(executed) == 2
    assert all(conn.closed for conn in opened), "connections must not leak"


def test_apply_stops_at_the_first_failure_and_reports_it() -> None:
    # A partially-evolved table must be reported honestly; already-applied ALTERs
    # are durable (each committed on its own), so re-planning simply drops them.
    executed: list[str] = []

    def factory():
        return _FakeConnection(executed, fail_on='"b"')

    plan = plan_add_columns("app.t", [("a", "int"), ("b", "text"), ("c", "int")], [])
    outcomes = apply_add_columns(plan, factory)
    assert [o.column for o in outcomes] == ["a", "b"], "must stop after the failure"
    assert outcomes[0].applied is True
    assert outcomes[1].applied is False
    assert "already exists" in (outcomes[1].error or "")


def test_apply_reports_a_connection_failure_without_raising() -> None:
    def factory():
        raise RuntimeError("token expired")

    plan = plan_add_columns("app.t", [("a", "int")], [])
    outcomes = apply_add_columns(plan, factory)
    assert len(outcomes) == 1
    assert outcomes[0].applied is False
    assert "token expired" in (outcomes[0].error or "")


def test_apply_empty_plan_is_a_noop() -> None:
    def factory():  # pragma: no cover - must never be called
        raise AssertionError("an empty plan must not open a connection")

    assert apply_add_columns(AddColumnPlan(table="app.t"), factory) == ()


# --------------------------------------------------------------------------- #
# read helpers (query shape / read-only)
# --------------------------------------------------------------------------- #
class _RecordingCursor:
    def __init__(self, rows, calls) -> None:
        self._rows = rows
        self._calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None):
        self._calls.append((sql, params))

    def fetchall(self):
        return self._rows


class _RecordingConnection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.calls: list[tuple] = []

    def cursor(self):
        return _RecordingCursor(self.rows, self.calls)


def test_read_target_columns_is_parameterized_and_read_only() -> None:
    conn = _RecordingConnection([("id",), ("email",)])
    assert read_target_columns(conn, "ecommerce.users") == ["id", "email"]
    sql, params = conn.calls[0]
    assert params == ("ecommerce", "users")
    assert "information_schema.columns" in sql
    for forbidden in ("INSERT", "UPDATE", "DELETE", "ALTER"):
        assert forbidden not in sql.upper()


@pytest.mark.parametrize("table", ["ecommerce.users", "users"])
def test_read_helpers_accept_qualified_or_bare_table(table: str) -> None:
    # A bare name yields an empty schema filter rather than crashing; the caller
    # always passes schema-qualified names, but this must not raise.
    conn = _RecordingConnection([])
    read_target_columns(conn, table)
    assert len(conn.calls) == 1


# --------------------------------------------------------------------------- #
# plan_target_fix: the ONE plan both Fix target schema… entry points use (G6)
# --------------------------------------------------------------------------- #
MYSQL, PG = SourceType.MYSQL, SourceType.POSTGRES


def _live(cols, pk=("id",), generated=()) -> LiveTable:
    """A live source table: [(name, catalog type)] in position order."""
    return LiveTable(
        columns=tuple(
            LiveColumn(n, t, n not in pk, n in generated, i) for i, (n, t) in enumerate(cols)
        ),
        primary_key=frozenset(pk),
    )


def _base(cols, pk=("id",)) -> TableDef:
    """The Step 1 inventory definition: [(name, catalog type)]."""
    return TableDef(
        name="app.t",
        columns=[ColumnDef(name=n, mysql_type=t, nullable=n not in pk) for n, t in cols],
        primary_key=list(pk),
    )


_NOTES = _base([("id", "int"), ("notes", "text")])


def _fix(live, target, **kw):
    kw.setdefault("rule_matches", {})
    kw.setdefault("source_type", MYSQL)
    return plan_target_fix("app.t", live, target, **kw)


@pytest.mark.parametrize(
    "source_type,columns,target",
    [
        (MYSQL, [("id", "int"), ("x", "int"), ("shape", "geometry"),
                 ("weird", "nonexistent_type_xyz(9)")], ["id"]),
        (PG, [("id", "integer"), ("tags", "text"), ("labels", "text[]")], ["id"]),
    ],
    ids=["mysql", "postgres"],
)
def test_target_fix_adds_a_plain_new_column_exactly_like_plan_add_columns(
    source_type, columns, target
) -> None:
    # (d): a genuinely new, non-key, non-excluded, non-generated column gets the very
    # statement, warning and skip the ADD-only Fix gave it.
    plan = _fix(_live(columns), target, baseline=_base(columns[:1]), source_type=source_type)
    expected = plan_add_columns("app.t", columns, target, source_type=source_type)
    assert plan.steps == expected.steps and plan.skipped == expected.skipped
    assert all(isinstance(s, AddColumnStep) for s in plan.steps)
    assert plan.held == () and plan.orphans == () and plan.refusal is None
    assert plan.ddl_text == expected.ddl_text and plan.offers_fix
    if source_type is MYSQL:
        assert [s.column for s in plan.steps] == ["x", "shape"]
        assert plan.steps[1].warning and [s.column for s in plan.skipped] == ["weird"]


def test_target_fix_renames_a_column_the_inventory_pairs() -> None:
    # 5: a RENAME COLUMN notes -> memo used to be offered as ADD memo, leaving notes.
    plan = _fix(_live([("id", "int"), ("memo", "text")]), ["id", "notes"], baseline=_NOTES)
    assert plan.steps == (
        RenameColumnStep(
            column="memo", old="notes", source_type="text", evidence="inventory",
            ddl='ALTER TABLE "app"."t" RENAME COLUMN "notes" TO "memo"',
        ),
    )
    assert plan.ddl_text.splitlines() == [plan.steps[0].ddl] and plan.offers_fix
    assert plan.held == () and plan.orphans == () and plan.refusal is None
    # Ranks count EVERY column, so an excluded column earlier in the table cannot shift
    # the pairing (the re-check's skip-then-rank would).
    base = _base([("id", "int"), ("blob", "longblob"), ("notes", "text")])
    live = _live([("id", "int"), ("blob", "longblob"), ("memo", "text")])
    shifted = _fix(live, ["id", "blob", "notes"], baseline=base, excluded={"blob"})
    assert [(s.old, s.column) for s in shifted.steps] == [("notes", "memo")]


def test_target_fix_case_only_rename_with_and_without_inventory() -> None:
    # The old case-insensitive diff returned an EMPTY plan here, so the Fix said "the
    # target already has every source column" while every row dead-lettered.
    live = _live([("id", "int"), ("Notes", "text")])
    with_inventory = _fix(live, ["id", "notes"], baseline=_NOTES)
    (step,) = with_inventory.steps
    assert isinstance(step, RenameColumnStep) and step.evidence == "inventory"
    assert step.ddl == 'ALTER TABLE "app"."t" RENAME COLUMN "notes" TO "Notes"'
    without = _fix(live, ["id", "notes"])
    (step,) = without.steps
    assert isinstance(step, RenameColumnStep) and step.evidence == "case"
    assert (step.old, step.column) == ("notes", "Notes")
    # Two case twins on the target and no inventory: no guess -- an ADD, with both named.
    twins = _fix(live, ["id", "notes", "NOTES"])
    (step,) = twins.steps
    assert isinstance(step, AddColumnStep) and step.column == "Notes"
    assert twins.orphans == ("notes", "NOTES")


def test_target_fix_uses_a_rename_the_recheck_saw_after_step1_was_rerun() -> None:
    refreshed = _base([("id", "int"), ("memo", "text")])
    live = _live([("id", "int"), ("memo", "text")])
    seen = (("notes", "memo", "text"),)
    plan = _fix(live, ["id", "notes"], baseline=refreshed, seen_renames=seen)
    (step,) = plan.steps
    assert isinstance(step, RenameColumnStep) and step.evidence == "re-check"
    # Ignored when the type differs, when notes is still live, or when memo is not live.
    for kw in (
        dict(live=live, seen_renames=(("notes", "memo", "int"),)),
        dict(live=_live([("id", "int"), ("notes", "text"), ("memo", "text")]),
             seen_renames=seen),
        dict(live=_live([("id", "int"), ("other", "text")]), seen_renames=seen),
    ):
        again = _fix(kw["live"], ["id", "notes"], baseline=refreshed,
                     seen_renames=kw["seen_renames"])
        assert not any(isinstance(s, RenameColumnStep) for s in again.steps), kw


def test_target_fix_refuses_a_renamed_primary_key_column() -> None:
    # 5k: an ADD of uid would make every upsert fail 42P10 and every DELETE WHERE uid = ?
    # silently match no row (uid NULL on every old row).
    live = _live([("uid", "int"), ("notes", "text")], pk=("uid",))
    plan = _fix(live, ["id", "notes"], baseline=_NOTES, record_key=("id",))
    assert plan.refusal is not None and plan.refusal.renamed == (("id", "uid"),)
    assert plan.steps == () and plan.held == () and plan.skipped == ()
    assert not plan.offers_fix
    # No inventory: the live target key alone still refuses it.
    blind = _fix(live, ["id", "notes"], record_key={"id"})
    assert blind.refusal == KeyRefusal(gone=("id",), missing=("uid",))
    # Rules that could not be evaluated never hide a key refusal.
    assert _fix(live, ["id", "notes"], baseline=_NOTES, rule_matches=None).refusal is not None


def test_target_fix_refuses_a_renamed_rekey_column() -> None:
    base = _base([("id", "int"), ("tenant_id", "int")])
    live = _live([("id", "int"), ("org_id", "int")])
    plan = _fix(live, ["id", "tenant_id"], baseline=base, rekey_columns={"tenant_id", "id"})
    assert plan.refusal.renamed == (("tenant_id", "org_id"),)
    assert plan.refusal.rekeyed == ("tenant_id",) and plan.steps == ()


def test_target_fix_refuses_a_source_key_change() -> None:
    live = _live([("id", "int"), ("notes", "text")], pk=("id", "notes"))
    plan = _fix(live, ["id", "notes"], baseline=_NOTES)
    assert plan.refusal == KeyRefusal(before=("id",), after=("id", "notes"))
    # A new column that joined the key: missing on the target, refused too.
    joined = _fix(_live([("id", "int"), ("notes", "text"), ("r", "int")], pk=("id", "r")),
                  ["id", "notes"], baseline=_NOTES)
    assert joined.refusal.missing == ("r",) and joined.steps == ()


def test_target_fix_holds_a_new_column_a_saved_rule_matches() -> None:
    # 13a: the capture list holds names fixed at Save, so a new column a wildcard rule
    # matches is captured -- adding it would replicate what the rule keeps out.
    live = _live([("id", "int"), ("notes", "text"), ("pw_salt", "text"), ("x", "int")])
    plan = _fix(live, ["id", "notes"], baseline=_NOTES,
                rule_matches={"pw_salt": "exclude app.t.pw_*"})
    assert plan.held == (HeldColumn("pw_salt", HELD_RULE, rule="exclude app.t.pw_*"),)
    assert [(type(s), s.column) for s in plan.steps] == [(AddColumnStep, "x")]
    # A rename onto a rule-matched name is held too, with the old name.
    renamed = _fix(_live([("id", "int"), ("pw_note", "text")]), ["id", "notes"],
                   baseline=_NOTES, rule_matches={"pw_note": None})
    assert renamed.held == (HeldColumn("pw_note", HELD_RULE, old="notes"),)
    assert renamed.steps == () and renamed.offers_fix


def test_target_fix_holds_a_renamed_excluded_column() -> None:
    # 13a: an excluded ssn renamed to ssn2 slips past the deployed list; the target still
    # has ssn (NULL, the overlay). Neither ADD ssn2 nor RENAME ssn -> ssn2.
    base = _base([("id", "int"), ("ssn", "text")])
    live = _live([("id", "int"), ("ssn2", "text")])
    plan = _fix(live, ["id", "ssn"], baseline=base, excluded={"ssn"})
    assert plan.held == (HeldColumn("ssn2", HELD_RENAMED_EXCLUDED, old="ssn"),)
    assert plan.steps == () and plan.orphans == ()


def test_target_fix_holds_new_columns_while_an_excluded_column_is_gone() -> None:
    # Step 1 re-run after the rename: no pairing evidence, but ssn is gone and ssn2 new.
    base = _base([("id", "int"), ("ssn2", "text")])
    live = _live([("id", "int"), ("ssn2", "text")])
    plan = _fix(live, ["id", "ssn"], baseline=base, excluded={"ssn"})
    expected = plan_add_columns("app.t", [("ssn2", "text")], ["id", "ssn"])
    assert plan.held == (
        HeldColumn("ssn2", HELD_EXCLUDED_GONE, ddl=expected.steps[0].ddl, others=("ssn",)),
    )
    assert plan.steps == () and plan.offers_fix


def test_target_fix_never_adds_a_stored_excluded_column() -> None:
    # Not captured, so it cannot dead-letter: no statement and nothing to explain.
    live = _live([("id", "int"), ("notes", "text"), ("blob", "longblob")])
    plan = _fix(live, ["id", "notes"], baseline=_NOTES, excluded={"blob"})
    assert plan.steps == () and plan.held == () and not plan.offers_fix


def test_target_fix_holds_everything_when_the_rules_are_unknown() -> None:
    live = _live([("id", "int"), ("memo", "text"), ("x", "int")])
    plan = _fix(live, ["id", "notes"], baseline=_NOTES, rule_matches=None)
    assert plan.held == (
        HeldColumn("memo", HELD_RULES_UNKNOWN, old="notes"),
        HeldColumn("x", HELD_RULES_UNKNOWN),
    )
    assert plan.steps == () and plan.refusal is None


def test_target_fix_withholds_a_generated_column_and_adds_the_plain_one() -> None:
    live = _live([("id", "int"), ("notes", "text"), ("x", "int"), ("g", "int")],
                 generated=("g",))
    plan = _fix(live, ["id", "notes"], baseline=_NOTES)
    assert [s.column for s in plan.steps] == ["x"]
    assert plan.held == (HeldColumn("g", HELD_GENERATED),) and plan.offers_fix
    only = _fix(_live([("id", "int"), ("notes", "text"), ("g", "int")], generated=("g",)),
                ["id", "notes"], baseline=_NOTES)
    assert only.steps == () and not only.offers_fix


def test_target_fix_name_taken_and_already_renamed() -> None:
    live = _live([("id", "int"), ("memo", "text")])
    taken = _fix(live, ["id", "notes", "memo"], baseline=_NOTES)
    assert taken.held == (HeldColumn("memo", HELD_NAME_TAKEN, old="notes"),)
    assert taken.steps == ()
    done = _fix(live, ["id", "memo"], baseline=_NOTES)
    assert done.steps == () and done.held == () and done.skipped == ()
    assert done.refusal is None and not done.offers_fix


def test_target_fix_reports_unexplained_target_columns_beside_an_add() -> None:
    # Step 1 re-run after the rename, no re-check record: no evidence -- an ADD, with the
    # target-only column named so the operator can rename it by hand instead.
    refreshed = _base([("id", "int"), ("memo", "text")])
    live = _live([("id", "int"), ("memo", "text")])
    plan = _fix(live, ["id", "notes"], baseline=refreshed)
    assert [type(s) for s in plan.steps] == [AddColumnStep]
    assert plan.orphans == ("notes",)
    # A column a rename step accounts for is not reported.
    mixed = _fix(_live([("id", "int"), ("memo", "text"), ("x", "int")]),
                 ["id", "notes", "legacy"], baseline=_NOTES)
    assert [type(s) for s in mixed.steps] == [RenameColumnStep, AddColumnStep]
    assert mixed.orphans == ("legacy",)
    # No ADD step, no orphans.
    no_add = _fix(_live([("id", "int")]), ["id", "notes"], baseline=_base([("id", "int")]))
    assert no_add.orphans == () and no_add.steps == ()


def test_offers_fix_counts_statements_skipped_and_non_generated_holds() -> None:
    def _plan(**kw):
        return AddColumnPlan(table="app.t", **kw)

    step = plan_add_columns("app.t", [("x", "int")], []).steps[0]
    skipped = plan_add_columns("app.t", [("w", "nonexistent_type_xyz")], []).skipped[0]
    assert _plan(steps=(step,)).offers_fix
    assert _plan(skipped=(skipped,)).offers_fix
    assert _plan(held=(HeldColumn("x", HELD_RULE),)).offers_fix
    assert _plan(held=(HeldColumn("x", HELD_EXCLUDED_GONE),)).offers_fix
    assert not _plan(held=(HeldColumn("g", HELD_GENERATED),)).offers_fix
    assert not _plan(refusal=KeyRefusal(gone=("id",))).offers_fix
    assert not _plan().offers_fix


def test_apply_runs_renames_and_adds_one_connection_each_in_plan_order() -> None:
    executed: list[str] = []
    opened: list[_FakeConnection] = []

    def factory():
        conn = _FakeConnection(executed)
        opened.append(conn)
        return conn

    live = _live([("id", "int"), ("memo", "text"), ("x", "int")])
    plan = _fix(live, ["id", "notes"], baseline=_NOTES)
    assert [type(s) for s in plan.steps] == [RenameColumnStep, AddColumnStep]
    outcomes = apply_add_columns(plan, factory)
    assert [(o.column, o.applied) for o in outcomes] == [("memo", True), ("x", True)]
    assert executed == [s.ddl for s in plan.steps] and len(opened) == 2
    assert all(conn.closed for conn in opened)
    # Stops at the first failure: the rename failing leaves the add unrun.
    failing: list[str] = []
    stopped = apply_add_columns(plan, lambda: _FakeConnection(failing, fail_on="RENAME"))
    assert [(o.column, o.applied) for o in stopped] == [("memo", False)] and failing == []


class _SqlaConn:
    """A SQLAlchemy-shaped connection returning catalog rows."""

    def __init__(self, rows) -> None:
        self.rows, self.calls = rows, []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        rows = self.rows

        class _Result:
            def fetchall(self):
                return list(rows)

        return _Result()


def test_read_source_table_reads_one_qualified_table() -> None:
    from dsql_migrator.core.introspector import is_write_or_ddl

    rows = [
        ("c", "app", "t", "id", 1, "int", "NO", "", None),
        ("c", "app", "t", "Notes", 2, "varchar(50)", "YES", "", "utf8mb4"),
        ("c", "app", "t", "g", 3, "int", "YES", "VIRTUAL GENERATED", None),
        ("k", "app", "t", "id", 1, None, None, None, None),
    ]
    conn = _SqlaConn(rows)
    live = read_source_table(conn, "app.t", source_type=MYSQL)
    assert [(c.name, c.type, c.position, c.generated) for c in live.columns] == [
        ("id", "int", 0, False), ("Notes", "varchar(50)", 1, False), ("g", "int", 2, True),
    ]
    assert live.primary_key == frozenset({"id"})
    ((sql, params),) = conn.calls
    assert "COLUMN_TYPE" in sql and "MAX_EXECUTION_TIME" in sql and not is_write_or_ddl(sql)
    assert set(params.values()) == {"app", "t"}
    # Unqualified: never read.
    bare = _SqlaConn(rows)
    assert read_source_table(bare, "t", source_type=MYSQL) is None and bare.calls == []


def test_target_fix_uses_a_recheck_rename_whose_stored_type_was_cut() -> None:
    # The re-check stores type text cut to 60 characters (+ "..."); a 62-character ENUM
    # recorded that way must still pair once Step 1 was re-run, not fall back to ADD + orphan.
    from dsql_migrator.core.cdc_catalog_drift import diff_table

    enum = "enum('pending','processing','shipped','delivered','cancelled')"
    assert len(enum) > 60
    base = _base([("id", "int"), ("state", enum)])
    live = _live([("id", "int"), ("status", enum)])
    diff = diff_table(base, live, source_type=MYSQL)
    (change,) = diff.changes
    assert change.kind == "rename-column" and change.before.endswith("...")
    seen = ((change.column, change.after, change.before),)
    refreshed = _base([("id", "int"), ("status", enum)])
    plan = _fix(live, ["id", "state"], baseline=refreshed, seen_renames=seen)
    (step,) = plan.steps
    assert isinstance(step, RenameColumnStep) and step.evidence == "re-check"
    assert (step.old, step.column) == ("state", "status") and plan.orphans == ()
    # A live type that differs within the stored prefix is still no rename.
    other = _live([("id", "int"), ("status", "varchar(64)")])
    again = _fix(other, ["id", "state"], baseline=_base([("id", "int"), ("status", "varchar(64)")]),
                 seen_renames=seen)
    assert [type(s) for s in again.steps] == [AddColumnStep] and again.orphans == ("state",)


def test_target_fix_withholds_a_pg_generated_column_a_rule_matches_as_generated() -> None:
    # PostgreSQL never streams a generated column, so a saved rule matching one has nothing
    # to keep out: the generated hold ("add it by hand"), not the rule hold's CDC teardown.
    cols = [("id", "integer"), ("notes", "text"), ("pii_hash", "text"), ("x", "integer")]
    rules = {"pii_hash": "exclude app.t.pii_*"}
    pg = _fix(_live(cols, generated=("pii_hash",)), ["id", "notes"], baseline=_NOTES,
              rule_matches=rules, source_type=PG)
    assert pg.held == (HeldColumn("pii_hash", HELD_GENERATED),)
    assert [(type(s), s.column) for s in pg.steps] == [(AddColumnStep, "x")]
    # MySQL streams a generated column in the row image: the rule hold stays.
    mysql = _fix(_live(cols, generated=("pii_hash",)), ["id", "notes"], baseline=_NOTES,
                 rule_matches=rules, source_type=MYSQL)
    assert mysql.held == (HeldColumn("pii_hash", HELD_RULE, rule="exclude app.t.pii_*"),)


def test_target_fix_inventory_pairing_never_renames_without_rank_type_and_absence() -> None:
    # A RENAME of a target column the sink still writes (or one the new name already holds)
    # dead-letters every later event, so each pairing guard is pinned here.
    # Different type at the same rank: a DROP + ADD, not a rename.
    retyped = _fix(_live([("id", "int"), ("qty", "int")]), ["id", "notes"], baseline=_NOTES)
    assert [(type(s), s.column) for s in retyped.steps] == [(AddColumnStep, "qty")]
    assert retyped.orphans == ("notes",)
    # ADD memo AFTER id while notes is still on the source: only the ADD.
    inserted = _fix(_live([("id", "int"), ("memo", "text"), ("notes", "text")]),
                    ["id", "notes"], baseline=_NOTES)
    assert [(type(s), s.column) for s in inserted.steps] == [(AddColumnStep, "memo")]
    # notes dropped, the baseline's own memo shifted into its rank: nothing to do.
    shifted = _fix(_live([("id", "int"), ("memo", "text")]), ["id", "notes", "memo"],
                   baseline=_base([("id", "int"), ("notes", "text"), ("memo", "text")]))
    assert shifted.steps == () and shifted.held == ()
    # The last column dropped: the baseline is longer than the live table.
    dropped = _fix(_live([("id", "int")]), ["id", "notes"], baseline=_NOTES)
    assert dropped.steps == () and dropped.held == () and dropped.refusal is None
    for plan in (retyped, inserted, shifted, dropped):
        assert not any(isinstance(s, RenameColumnStep) for s in plan.steps)
        assert all(h.reason != HELD_NAME_TAKEN for h in plan.held)


def test_target_fix_case_and_recheck_pairs_use_each_name_once() -> None:
    def _pairs(plan):
        return [(type(s).__name__, getattr(s, "old", None), s.column) for s in plan.steps]

    # PostgreSQL with quoted "notes" and "Notes" both live: notes is still captured, so it
    # is never renamed to its case twin.
    twins = _fix(_live([("id", "integer"), ("notes", "text"), ("Notes", "text")]),
                 ["id", "notes"], source_type=PG)
    assert _pairs(twins) == [("AddColumnStep", None, "Notes")]
    # A column the target already has is never case-paired.
    have = _fix(_live([("id", "int"), ("notes", "text")]), ["id", "notes", "Notes"])
    assert have.steps == () and have.held == ()
    # The inventory pair used notes: NOTES is not a second rename of it.
    case_once = _fix(_live([("id", "int"), ("memo", "text"), ("NOTES", "text")]),
                     ["id", "notes"], baseline=_NOTES)
    assert _pairs(case_once) == [("RenameColumnStep", "notes", "memo"),
                                 ("AddColumnStep", None, "NOTES")]
    # The inventory pair beats a weaker re-check record for the same new name ...
    same_new = _fix(_live([("id", "int"), ("memo", "text")]), ["id", "notes", "other"],
                    baseline=_NOTES, seen_renames=(("other", "memo", "text"),))
    assert _pairs(same_new) == [("RenameColumnStep", "notes", "memo")]
    # ... and a re-check record never renames the same old name twice.
    same_old = _fix(_live([("id", "int"), ("memo", "text"), ("x", "text")]), ["id", "notes"],
                    baseline=_NOTES, seen_renames=(("notes", "x", "text"),))
    assert _pairs(same_old) == [("RenameColumnStep", "notes", "memo"), ("AddColumnStep", None, "x")]


def test_target_fix_refuses_on_each_key_trigger_alone() -> None:
    # missing only: no inventory, the target key unreadable, the live key uid not on the
    # target -- an ADD of uid would be the 5k silent loss.
    live = _live([("uid", "int"), ("notes", "text")], pk=("uid",))
    missing = _fix(live, ["id", "notes"])
    assert missing.refusal == KeyRefusal(missing=("uid",))
    assert missing.steps == () and not missing.offers_fix
    # gone only: a re-key column renamed AND retyped (no rename pair), source key unchanged.
    base = _base([("id", "int"), ("tenant_id", "int")])
    gone = _fix(_live([("id", "int"), ("org_id", "bigint")]), ["id", "tenant_id"], baseline=base,
                record_key=("tenant_id", "id"), rekey_columns={"tenant_id", "id"})
    assert gone.refusal == KeyRefusal(gone=("tenant_id",), rekeyed=("tenant_id",))
    assert gone.steps == ()


def test_target_fix_adds_beside_a_live_excluded_column() -> None:
    # A stored exclusion still on the source is not "gone": the plain ADD path stays exactly
    # what plan_add_columns renders (no excluded-gone hold on every table with a LOB rule).
    cols = [("id", "int"), ("notes", "text"), ("blob", "longblob"), ("x", "int")]
    target = ["id", "notes", "blob"]
    plan = _fix(_live(cols), target, baseline=_base(cols[:3]), excluded={"blob"})
    expected = plan_add_columns("app.t", cols, target)
    assert plan.steps == expected.steps and plan.skipped == expected.skipped
    assert [s.column for s in plan.steps] == ["x"]
    assert plan.held == () and plan.refusal is None and plan.offers_fix
