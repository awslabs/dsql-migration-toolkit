# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G4: the bulk source-catalog read and the pure baseline diff (core/cdc_catalog_drift.py)."""

from __future__ import annotations

import re

import pytest

from dsql_migrator.core.cdc_catalog_drift import (
    KIND_ADD_COLUMN,
    KIND_DROP_COLUMN,
    KIND_KEY_CHANGE,
    KIND_NULLABILITY_CHANGE,
    KIND_RENAME_COLUMN,
    KIND_SOURCE_TABLE_MISSING,
    KIND_TYPE_CHANGE,
    PG_CATALOG_SQL,
    LiveColumn,
    LiveTable,
    baseline_digest,
    build_mysql_catalog_statement,
    diff_table,
    read_live_catalog,
    same_type,
)
from dsql_migrator.core.introspector import is_write_or_ddl
from dsql_migrator.core.models import ColumnDef, SourceType, TableDef

MYSQL, PG = SourceType.MYSQL, SourceType.POSTGRES


def _table(name="app.orders", cols=(("id", "int", False), ("notes", "text", True)),
           pk=("id",), generated=()):
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name=n, mysql_type=t, nullable=nl, generated=n in generated)
            for n, t, nl in cols
        ],
        primary_key=list(pk),
    )


def _live(cols=(("id", "int", False), ("notes", "text", True)), pk=("id",), generated=()):
    return LiveTable(
        columns=tuple(
            LiveColumn(n, t, nl, n in generated, i) for i, (n, t, nl) in enumerate(cols)
        ),
        primary_key=frozenset(pk),
    )


class _Conn:
    def __init__(self, rows=(), fail=None):
        self.rows, self.fail, self.calls = list(rows), fail, []

    def execute(self, statement, params=None):
        if self.fail is not None:
            raise self.fail
        self.calls.append((str(statement), dict(params or {})))
        rows = self.rows

        class _R:
            def fetchall(self):
                return list(rows)

        return _R()


# --- the MySQL statement -------------------------------------------------------------------


def test_the_mysql_read_is_one_bounded_catalog_statement_with_every_name_bound() -> None:
    conn = _Conn()
    read_live_catalog(conn, ["app.orders", "app.o'ne", "app"], source_type=MYSQL)
    ((sql, params),) = conn.calls
    assert sql.startswith("SELECT /*+ MAX_EXECUTION_TIME(8000) */")
    assert is_write_or_ddl(sql) is False
    referenced = set(re.findall(r"information_schema\.(\w+)", sql))
    assert referenced == {"COLUMNS", "STATISTICS"}
    assert "INDEX_NAME = 'PRIMARY'" in sql and "COLUMN_KEY" not in sql
    assert "o'ne" not in sql and "orders" not in sql  # names only in the params
    assert sorted(params.values()) == ["app", "o'ne", "orders"]  # "app" (unqualified) skipped
    assert " OR " not in sql  # one schema: a single top-level equality


def test_two_schemas_give_two_groups_and_thousands_of_names_still_one_execute() -> None:
    sql, params = build_mysql_catalog_statement(["a.t1", "b.t2", "a.t3"])
    assert sql.count("(TABLE_SCHEMA = :s0 AND TABLE_NAME IN (:t0_0, :t0_1))") == 2
    assert sql.count("(TABLE_SCHEMA = :s1 AND TABLE_NAME IN (:t1_0))") == 2
    assert params == {"s0": "a", "t0_0": "t1", "t0_1": "t3", "s1": "b", "t1_0": "t2"}
    conn = _Conn()
    names = [f"{'a' if i % 2 else 'b'}.t{i}" for i in range(5000)]
    read_live_catalog(conn, names, source_type=MYSQL)
    assert len(conn.calls) == 1 and len(conn.calls[0][1]) == 5002


def test_no_qualified_name_reads_nothing() -> None:
    conn = _Conn()
    assert read_live_catalog(conn, ["orders", ""], source_type=MYSQL) == {}
    assert conn.calls == []


def test_the_postgres_read_is_one_pg_catalog_statement() -> None:
    conn = _Conn()
    read_live_catalog(conn, ["s.b", "s.a"], source_type=PG)
    ((sql, params),) = conn.calls
    assert params == {"names": ["s.a", "s.b"]}
    assert "information_schema" not in sql and "pg_catalog.pg_attribute" in sql
    assert "= ANY(:names)" in sql and "relkind IN ('r','p')" in sql
    assert "NOT a.attisdropped" in sql and "indisprimary" in sql
    assert sql == PG_CATALOG_SQL and is_write_or_ddl(sql) is False


def test_the_postgres_null_rule_folds_in_the_columns_own_not_null_domain() -> None:
    """A column typed with ``CREATE DOMAIN d AS text NOT NULL`` has attnotnull false, but the
    Step 1 inventory (SQLAlchemy reflection) records it NOT NULL. Reading attnotnull alone
    opened a permanent, false "NULL rule changed" finding that held Cut over for good."""
    assert "JOIN pg_catalog.pg_type t ON t.oid = a.atttypid" in PG_CATALOG_SQL
    assert "(a.attnotnull OR (t.typtype = 'd' AND t.typnotnull)) AS attnotnull" in PG_CATALOG_SQL


def _reflected_nullable(format_type: str, domains: dict) -> bool:
    """The Step 1 inventory's NULL rule for column ``c`` (attnotnull false), via the same
    SQLAlchemy PostgreSQL reflection the introspector uses."""
    from sqlalchemy.dialects.postgresql.base import PGDialect

    def _row(name, typ, not_null):
        return {"name": name, "table_name": "t", "format_type": typ, "default": None,
                "not_null": not_null, "generated": "", "identity_options": None,
                "comment": None, "collation": None}

    cols = PGDialect()._get_columns_info(
        [_row("id", "integer", True), _row("c", format_type, False)], domains, {}, "public"
    )[("public", "t")]
    return {c["name"]: c for c in cols}["c"]["nullable"]


@pytest.mark.parametrize(
    "format_type,domain_not_null,expected",
    [
        ("email_dom", True, False),  # NOT NULL domain: folded in by reflection and the read
        ("email_dom", False, True),  # nullable domain: nothing to fold
        ("email_dom[]", True, True),  # array of a domain (typtype 'b'): never folded
    ],
)
def test_a_domain_column_matches_its_inventory_null_rule(format_type, domain_not_null,
                                                         expected) -> None:
    domains = {("email_dom",): {
        "name": "email_dom", "schema": "public", "visible": True, "type": "text",
        "nullable": not domain_not_null, "default": None, "constraints": [], "collation": None,
    }}
    nullable = _reflected_nullable(format_type, domains)
    assert nullable is expected
    table = TableDef(
        name="public.t",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="c", mysql_type=format_type, nullable=nullable)],
        primary_key=["id"],
    )
    # The live row as PG_CATALOG_SQL returns it: attnotnull OR (own domain AND typnotnull).
    folded = domain_not_null and not format_type.endswith("[]")
    rows = [("public.t", "id", 1, "integer", True, "", True),
            ("public.t", "c", 2, format_type, folded, "", False)]
    live = read_live_catalog(_Conn(rows), ["public.t"], source_type=PG)["public.t"]
    assert diff_table(table, live, source_type=PG) is None


# --- row mapping ---------------------------------------------------------------------------


def test_mysql_rows_map_to_ranked_columns_a_primary_key_and_generated_flags() -> None:
    rows = [
        ("c", "app", "orders", "total", 3, "decimal(10,2)", "YES", ""),
        ("c", "app", "orders", "id", 1, "int", "NO", "auto_increment"),
        ("c", "app", "orders", "v", 5, "int", "YES", "VIRTUAL GENERATED"),
        ("c", "app", "orders", "made", 4, "datetime", "YES", "DEFAULT_GENERATED"),
        ("k", "app", "orders", "id", 1, None, None, None),
        ("c", "APP", "Items", "id", 1, b"bigint", "NO", ""),  # case-folded, bytes decoded
    ]
    live = read_live_catalog(_Conn(rows), ["app.orders", "app.items"], source_type=MYSQL)
    orders = live["app.orders"]
    assert [(c.name, c.position) for c in orders.columns] == [
        ("id", 0), ("total", 1), ("made", 2), ("v", 3)
    ]
    by = {c.name: c for c in orders.columns}
    assert by["id"].nullable is False and by["total"].nullable is True
    assert by["v"].generated is True and by["made"].generated is False
    assert orders.primary_key == frozenset({"id"})
    assert live["app.items"].columns[0].type == "bigint"
    assert live["app.items"].primary_key == frozenset()


def test_pg_rows_rank_densely_and_flag_stored_generated_and_key_columns() -> None:
    rows = [
        ("s.t", "id", 1, "integer", True, "", True),
        ("s.t", "total", 4, "numeric(12,2)", False, "", False),  # attnum gap: a dropped col
        ("s.t", "g", 5, "integer", False, "s", False),
    ]
    live = read_live_catalog(_Conn(rows), ["s.t"], source_type=PG)["s.t"]
    assert [(c.name, c.position) for c in live.columns] == [("id", 0), ("total", 1), ("g", 2)]
    assert [c.generated for c in live.columns] == [False, False, True]
    assert live.columns[0].nullable is False and live.columns[1].nullable is True
    assert live.primary_key == frozenset({"id"})


def test_a_connection_error_propagates() -> None:
    with pytest.raises(ConnectionError):
        read_live_catalog(_Conn(fail=ConnectionError("x")), ["app.t"], source_type=MYSQL)


# --- same_type -----------------------------------------------------------------------------


def test_same_type_normalizes_only_what_cannot_change_a_cdc_row() -> None:
    assert same_type("int(11)", "int", source_type=MYSQL)
    assert same_type("int(10) unsigned", "int unsigned", source_type=MYSQL)
    assert same_type("year(4)", "year", source_type=MYSQL)
    assert not same_type("tinyint(1)", "int", source_type=MYSQL)
    assert not same_type("tinyint(1)", "tinyint(4)", source_type=MYSQL)
    assert not same_type("enum('A','b')", "enum('a','b')", source_type=MYSQL)
    assert not same_type("decimal(10,2)", "decimal(12,4)", source_type=MYSQL)
    assert same_type("app.mood", "mood", source_type=PG)
    assert same_type("app.mood[]", "mood[]", source_type=PG)
    assert not same_type("numeric(10,2)", "numeric(12,4)", source_type=PG)


# --- the diff ------------------------------------------------------------------------------


def test_an_identical_table_has_no_diff() -> None:
    assert diff_table(_table(), _live(), source_type=MYSQL) is None


def test_a_dropped_nullable_column_is_reported_3a() -> None:
    diff = diff_table(_table(), _live(cols=(("id", "int", False),)), source_type=MYSQL)
    assert diff.kinds == (KIND_DROP_COLUMN,)
    (change,) = diff.changes
    assert change.column == "notes" and not diff.fixable


def test_a_plain_added_column_is_fixable_and_a_generated_one_is_not() -> None:
    plain = diff_table(
        _table(), _live(cols=(("id", "int", False), ("notes", "text", True), ("x", "int", True))),
        source_type=MYSQL,
    )
    assert plain.kinds == (KIND_ADD_COLUMN,) and plain.fixable
    assert plain.changes[0].after == "int" and not plain.changes[0].generated
    gen = diff_table(
        _table(),
        _live(cols=(("id", "int", False), ("notes", "text", True), ("g", "int", True)),
              generated=("g",)),
        source_type=PG,
    )
    assert gen.kinds == (KIND_ADD_COLUMN,) and gen.changes[0].generated and not gen.fixable


def test_a_plain_and_a_generated_added_column_together_are_not_fixable() -> None:
    """Fix target schema applies its whole plan and cannot tell a generated column apart, so
    offering it here would add the generated column as a plain, empty one -- while the row
    says Fix is not offered for it."""
    mixed = diff_table(
        _table(),
        _live(cols=(("id", "int", False), ("notes", "text", True), ("a", "int", True),
                    ("g", "int", True)), generated=("g",)),
        source_type=MYSQL,
    )
    assert mixed.kinds == (KIND_ADD_COLUMN,) and not mixed.fixable
    assert sorted((c.column, c.generated) for c in mixed.changes) == [("a", False), ("g", True)]


def test_a_rename_at_the_same_position_and_type_is_one_rename_even_case_only() -> None:
    renamed = diff_table(
        _table(), _live(cols=(("id", "int", False), ("memo", "text", True))), source_type=MYSQL
    )
    assert renamed.kinds == (KIND_RENAME_COLUMN,) and not renamed.fixable
    assert (renamed.changes[0].column, renamed.changes[0].after) == ("notes", "memo")
    case = diff_table(
        _table(), _live(cols=(("id", "int", False), ("Notes", "text", True))), source_type=MYSQL
    )
    assert case.kinds == (KIND_RENAME_COLUMN,)
    # A rename next to a plain added column: Fix would add an empty column for the rename.
    plus_add = diff_table(
        _table(),
        _live(cols=(("id", "int", False), ("memo", "text", True), ("x", "int", True))),
        source_type=MYSQL,
    )
    assert plus_add.kinds == (KIND_RENAME_COLUMN, KIND_ADD_COLUMN) and not plus_add.fixable


def test_a_drop_and_an_add_of_another_type_or_position_stay_apart() -> None:
    other_type = diff_table(
        _table(), _live(cols=(("id", "int", False), ("memo", "int", True))), source_type=MYSQL
    )
    assert other_type.kinds == (KIND_DROP_COLUMN, KIND_ADD_COLUMN)
    base = _table(cols=(("id", "int", False), ("a", "text", True), ("b", "text", True)))
    moved = diff_table(
        base, _live(cols=(("id", "int", False), ("b", "text", True), ("c", "text", True))),
        source_type=MYSQL,
    )
    assert moved.kinds == (KIND_DROP_COLUMN, KIND_ADD_COLUMN)


@pytest.mark.parametrize("before,after", [("decimal(10,2)", "decimal(12,4)"), ("tinyint(1)", "int")])
def test_a_widened_type_is_a_type_change_4b_4c(before, after) -> None:
    diff = diff_table(
        _table(cols=(("id", "int", False), ("v", before, True))),
        _live(cols=(("id", "int", False), ("v", after, True))),
        source_type=MYSQL,
    )
    assert diff.kinds == (KIND_TYPE_CHANGE,)
    assert (diff.changes[0].before, diff.changes[0].after) == (before, after)


def test_a_null_rule_flip_is_reported_but_never_on_a_generated_column() -> None:
    flip = diff_table(
        _table(), _live(cols=(("id", "int", False), ("notes", "text", False))), source_type=MYSQL
    )
    assert flip.kinds == (KIND_NULLABILITY_CHANGE,)
    assert (flip.changes[0].before, flip.changes[0].after) == ("NULL", "NOT NULL")
    gen = diff_table(
        _table(generated=("notes",)),
        _live(cols=(("id", "int", False), ("notes", "text", False)), generated=("notes",)),
        source_type=MYSQL,
    )
    assert gen is None
    # Generated on either side alone: the NULL rule is still not compared.
    flipped = _live(cols=(("id", "int", False), ("notes", "text", False)))
    assert diff_table(_table(generated=("notes",)), flipped, source_type=MYSQL) is None
    live_gen = _live(cols=(("id", "int", False), ("notes", "text", False)), generated=("notes",))
    assert diff_table(_table(), live_gen, source_type=MYSQL) is None


def test_the_primary_key_is_compared_as_a_set() -> None:
    changed = diff_table(
        _table(cols=(("id", "int", False), ("region", "int", False))),
        _live(cols=(("id", "int", False), ("region", "int", False)), pk=("id", "region")),
        source_type=MYSQL,
    )
    assert changed.kinds == (KIND_KEY_CHANGE,) and not changed.fixable
    assert (changed.changes[0].before, changed.changes[0].after) == ("id", "id, region")
    reordered = diff_table(
        _table(cols=(("a", "int", False), ("b", "int", False)), pk=("a", "b")),
        _live(cols=(("a", "int", False), ("b", "int", False)), pk=("b", "a")),
        source_type=MYSQL,
    )
    assert reordered is None
    # A composite key listed out of alphabetical order is the same key.
    composite = (("order_id", "int", False), ("line_no", "int", False))
    assert diff_table(
        _table(cols=composite, pk=("order_id", "line_no")),
        _live(cols=composite, pk=("order_id", "line_no")),
        source_type=MYSQL,
    ) is None
    # A dropped key and a replaced key are key changes, not only a widened one.
    two = (("id", "int", False), ("uuid", "char(36)", False))
    for live_pk in ((), ("uuid",)):
        replaced = diff_table(_table(cols=two), _live(cols=two, pk=live_pk), source_type=MYSQL)
        assert replaced.kinds == (KIND_KEY_CHANGE,), live_pk
    # A key change next to a plain added column is not fixable.
    keyed = diff_table(
        _table(cols=(("id", "int", False), ("r", "int", False))),
        _live(cols=(("id", "int", False), ("r", "int", False), ("x", "int", True)),
              pk=("id", "r")),
        source_type=MYSQL,
    )
    assert keyed.kinds == (KIND_KEY_CHANGE, KIND_ADD_COLUMN) and not keyed.fixable


def test_a_missing_table_is_only_that() -> None:
    diff = diff_table(_table(), None, source_type=MYSQL)
    assert diff.kinds == (KIND_SOURCE_TABLE_MISSING,) and diff.changes == ()


# --- what is skipped, and the baseline -----------------------------------------------------


def test_excluded_and_stored_generated_columns_never_drift_either_way() -> None:
    base = _table(cols=(("id", "int", False), ("blob", "longblob", True), ("n", "text", True)))
    dropped = _live(cols=(("id", "int", False), ("n", "text", True)))
    assert diff_table(base, dropped, skip={"blob"}, source_type=MYSQL) is None
    retyped = _live(cols=(("id", "int", False), ("blob", "text", True), ("n", "text", True)))
    assert diff_table(base, retyped, skip={"blob"}, source_type=MYSQL) is None
    # A NEW column that merely matches an exclusion RULE is not in the concrete skip set.
    new = _live(cols=(("id", "int", False), ("blob", "longblob", True), ("n", "text", True),
                      ("blob2", "longblob", True)))
    assert diff_table(base, new, skip={"blob"}, source_type=MYSQL).kinds == (KIND_ADD_COLUMN,)


def test_a_column_added_through_fix_is_baseline_unless_its_type_differs_or_it_is_generated() -> None:
    live = _live(cols=(("id", "int", False), ("notes", "text", True), ("x", "int", True)))
    assert diff_table(_table(), live, target_added={"x": "int"}, source_type=MYSQL) is None
    other = diff_table(_table(), live, target_added={"x": "bigint"}, source_type=MYSQL)
    assert other.kinds == (KIND_TYPE_CHANGE,)
    gen_live = _live(cols=(("id", "int", False), ("notes", "text", True), ("x", "int", True)),
                     generated=("x",))
    gen = diff_table(_table(), gen_live, target_added={"x": "int"}, source_type=MYSQL)
    assert gen.kinds == (KIND_ADD_COLUMN,) and gen.changes[0].generated and not gen.fixable


def test_defaults_comments_and_collations_are_not_compared() -> None:
    base = TableDef(
        name="app.t",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False, default="1",
                           comment="c", collation="utf8mb4_bin")],
        primary_key=["id"],
    )
    assert diff_table(base, _live(cols=(("id", "int", False),)), source_type=MYSQL) is None


def test_changes_are_bounded_and_carry_names_and_types_only() -> None:
    cols = [("id", "int", False)] + [(f"c{i}", "text", True) for i in range(20)]
    diff = diff_table(_table(cols=cols), _live(cols=(("id", "int", False),)), source_type=MYSQL)
    assert len(diff.changes) == 6 and diff.more_changes == 14
    long_type = "enum(" + ",".join(f"'v{i}'" for i in range(40)) + ")"
    typed = diff_table(
        _table(cols=(("id", "int", False), ("e", "enum('a')", True))),
        _live(cols=(("id", "int", False), ("e", long_type, True))),
        source_type=MYSQL,
    )
    after = typed.changes[0].after
    assert len(after) == 63 and after.endswith("...")
    assert set(typed.changes[0].model_dump()) == {"kind", "column", "before", "after", "generated"}


def test_the_baseline_digest_follows_exactly_the_compared_facts() -> None:
    base = _table()
    d = baseline_digest(base, source_type=MYSQL)
    assert len(d) == 16 and d == baseline_digest(_table(), source_type=MYSQL)
    with_default = TableDef(
        name="app.orders",
        columns=[ColumnDef(name="id", mysql_type="int(11)", nullable=False, comment="x"),
                 ColumnDef(name="notes", mysql_type="text", nullable=True, default="'a'",
                           collation="utf8mb4_bin")],
        primary_key=["id"],
    )
    assert baseline_digest(with_default, source_type=MYSQL) == d
    assert baseline_digest(_table(cols=(("id", "int", False),)), source_type=MYSQL) != d
    assert baseline_digest(base, skip={"notes"}, source_type=MYSQL) == baseline_digest(
        _table(cols=(("id", "int", False),)), source_type=MYSQL
    )
    for changed in (
        _table(cols=(("id", "int", False), ("Notes", "text", True))),
        _table(cols=(("id", "int", False), ("notes", "mediumtext", True))),
        _table(cols=(("id", "int", False), ("notes", "text", False))),
        _table(pk=("id", "notes")),
    ):
        assert baseline_digest(changed, source_type=MYSQL) != d
