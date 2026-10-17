"""G103: the review of G97-G100 -- the Query Converter reads a parenthesised join that
starts with a derived table (the form MySQL stores every such join in) as a join, a
starred source's label beside a real table as the statement's own, a quoted alias in the
case-only check, and resolves qualifiers in linear time; MySQL-only DML clauses are MANUAL
and every DML modifier parses. Each statement was checked on MySQL 8.4 and PostgreSQL 17."""

from __future__ import annotations

import time

import pytest
import sqlglot

from dsql_migrator.core.converter import MYSQL_EXPRESSION_DIALECT, SchemaConverter, table_type_index
from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef
from dsql_migrator.core.query_converter import (
    QueryConverter,
    _Scopes,
    _case_only_alias_warning,
    _mixed_case_names,
)


def _convert(sql: str):
    result = QueryConverter().convert(sql)
    return result.converted_sql, [w.code for w in result.warnings], result


@pytest.mark.parametrize(
    ("sql", "code", "absent"),
    [
        # MySQL returns 6 rows; converted, none: the inner X captures x. It was AUTO.
        ("SELECT x.total FROM ((SELECT total, customer_id FROM orders) x JOIN customers c ON "
         "c.id = x.customer_id) WHERE EXISTS (SELECT 1 FROM orders X WHERE X.total > x.total)",
         "CASE_ONLY_ALIASES", None),
        # Aurora DSQL fails 'column x.Total does not exist'. It was AUTO.
        ("SELECT `x`.`Total`, c.name FROM ((SELECT total AS Total, customer_id FROM orders) x "
         "JOIN customers c ON c.id = x.customer_id)", "QUOTE_MISMATCH", None),
        # A label of the statement's own: MIXED_CASE_NAME's advice (x."Total") failed.
        ("SELECT x.Total FROM ((SELECT total AS Total, customer_id FROM orders) AS x JOIN "
         "customers c ON c.id = x.customer_id)", "MIXED_CASE_LABEL", "MIXED_CASE_NAME"),
        # c hangs on the derived table, so it was in no scope: MySQL returns c, c, d;
        # converted, no rows (the inner C captures c).
        ("SELECT c.name FROM ((SELECT customer_id FROM orders) x JOIN customers c ON "
         "c.id = x.customer_id) WHERE EXISTS (SELECT 1 FROM payments C WHERE "
         "C.customer_id = c.id AND C.amount > 20)", "CASE_ONLY_ALIASES", None),
    ],
)
def test_a_join_that_starts_with_a_derived_table_keeps_both_in_scope(
    sql: str, code: str, absent: "str | None"
) -> None:
    _converted, codes, _ = _convert(sql)
    assert code in codes and absent not in codes, codes


_TABLES = [
    TableDef(
        name="shop.orders",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
        primary_key=["id"],
    ),
    TableDef(
        name="shop.items",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="order_id", mysql_type="int"),
            ColumnDef(name="code", mysql_type="blob"),
        ],
        primary_key=["id"],
    ),
]


def test_the_table_joined_to_a_derived_table_is_typed() -> None:
    """Typing missed it too, so CONVERT(blob USING utf8mb4) was a text cast: the view (as
    MySQL stores it) and the query returned '\\x626c6f626279' where MySQL returns 'blobby'."""
    view = SchemaConverter().convert_view(
        ViewDef(
            name="shop.v",
            definition=(
                "select `x`.`id` AS `id`,convert(`i`.`code` using utf8mb4) AS `t` from ((select "
                "`o`.`id` AS `id` from `shop`.`orders` `o`) `x` join `shop`.`items` `i` "
                "on((`i`.`order_id` = `x`.`id`)))"
            ),
        ),
        tables=_TABLES,
    )
    assert "CONVERT_FROM(\"i\".\"code\", 'UTF8')" in view.target_ddl
    converted = QueryConverter(column_types=tuple(table_type_index(_TABLES).values())).convert(
        "SELECT x.id, CONVERT(i.code USING utf8mb4) AS t FROM ((SELECT o.id FROM shop.orders o) x "
        "JOIN shop.items i ON i.order_id = x.id)"
    ).converted_sql
    assert "CONVERT_FROM(i.code, 'UTF8')" in converted


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT d.id, c.name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER "
        "BY created_at DESC) AS RowNum FROM orders) d JOIN customers c ON c.id = d.customer_id "
        "WHERE RowNum = 1",
        "WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY "
        "created_at DESC) AS RowNum FROM orders) SELECT ranked.id, c.name FROM ranked JOIN "
        "customers c ON c.id = ranked.customer_id WHERE RowNum = 1",
        "SELECT u.id, c.name FROM (SELECT *, 1 AS UserId FROM orders) u JOIN customers c ON "
        "c.id = u.customer_id JOIN (SELECT 1 AS UserId) k USING (UserId)",
    ],
)
def test_a_starred_sources_label_beside_a_real_table_is_its_own(sql: str) -> None:
    """MIXED_CASE_NAME, whose advice (WHERE "RowNum" = 1) fails on Aurora DSQL; as written,
    each returns MySQL's rows there. MySQL refuses a name two sources hold, so the one that
    holds it for certain is where it resolves."""
    _converted, codes, _ = _convert(sql)
    assert codes == []


def test_a_name_no_source_holds_for_certain_is_still_flagged() -> None:
    _converted, codes, _ = _convert(
        "SELECT d.id FROM (SELECT *, 1 AS RowNum FROM orders) d JOIN customers c ON "
        "c.id = d.customer_id WHERE Name = 'a'"
    )
    assert "MIXED_CASE_NAME" in codes


@pytest.mark.parametrize(
    "sql",
    [
        # MySQL returns 1, 2, 3, 5, 7; converted, all 7 rows: "o" captures the folded O.
        "SELECT O.id FROM orders O WHERE EXISTS (SELECT 1 FROM payments `o` WHERE "
        "`o`.order_id = O.id)",
        "SELECT `o`.id FROM orders `o` WHERE EXISTS (SELECT 1 FROM payments O WHERE "
        "O.order_id = o.id)",
        # MySQL reads `O` as the inner O, Aurora DSQL "O" as the outer one: 7 rows, then 2.
        "SELECT `O`.id FROM orders `O` WHERE EXISTS (SELECT 1 FROM payments O WHERE "
        "`O`.customer_id = 11)",
        # One FROM: Aurora DSQL refuses 'table name "o" specified more than once'.
        "SELECT O.id FROM orders O JOIN payments `o` ON `o`.order_id = O.id",
    ],
)
def test_a_quoted_alias_takes_part_in_the_case_only_check(sql: str) -> None:
    _converted, codes, result = _convert(sql)
    assert "CASE_ONLY_ALIASES" in codes
    assert "`" in result.warnings[codes.index("CASE_ONLY_ALIASES")].message  # named as written


def test_a_quoted_alias_both_engines_read_alike_is_not_flagged() -> None:
    _converted, codes, _ = _convert(
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM payments `O` WHERE "
        "`O`.order_id = o.id)"
    )
    assert codes == []


def _union(branches: int, backquoted: bool) -> str:
    if backquoted:
        return " UNION ALL ".join(
            f"SELECT orders.id, orders.total FROM `orders` WHERE orders.id = {i}"
            for i in range(branches)
        )
    return "SELECT o.id FROM orders o WHERE o.id IN (" + " UNION ALL ".join(
        f"SELECT p{i}.order_id FROM payments p{i} WHERE p{i}.customer_id = o.customer_id"
        for i in range(branches)
    ) + ")"


@pytest.mark.parametrize("backquoted", [False, True])
def test_a_long_unions_qualifiers_resolve_in_linear_time(backquoted: bool) -> None:
    """Each qualified column walked every enclosing UNION level to its FROM item: on 4,000
    branches the case-only check took 6.7 s over a backquoted table, the name scan 2.4 s
    reaching an outer alias -- on the UI's event loop."""

    def took(branches: int) -> float:
        tree = sqlglot.parse_one(_union(branches, backquoted), read=MYSQL_EXPRESSION_DIALECT)
        best = float("inf")
        for _ in range(3):
            started = time.perf_counter()
            scopes = _Scopes(tree)
            _mixed_case_names(tree, scopes, [])
            _case_only_alias_warning(tree, scopes)
            best = min(best, time.perf_counter() - started)
        return best

    took(100)  # warm up
    small, large = took(500), took(2000)
    assert large < small * 7  # linear: 4x the branches; it was 10-12x


@pytest.mark.parametrize(
    ("sql", "kept"),
    [
        # MySQL's chunked delete: Aurora DSQL refuses it ('syntax error at or near "ORDER"').
        ("DELETE FROM orders WHERE status = 'old' ORDER BY id LIMIT 1000", "LIMIT 1000"),
        ("DELETE FROM orders WHERE id = 1 LIMIT 1", "LIMIT 1"),
        ("UPDATE orders SET status = 'x' WHERE status = 'new' ORDER BY id LIMIT 10", "LIMIT 10"),
        ("UPDATE orders SET status = 'x' ORDER BY id", "ORDER BY id"),
        ("INSERT IGNORE INTO customers (id, name) VALUES (10, 'z')", "INSERT IGNORE INTO"),
        ("REPLACE INTO customers (id, name) VALUES (10, 'z')", "REPLACE INTO"),
    ],
)
def test_a_mysql_only_dml_clause_is_manual_and_left_as_written(sql: str, kept: str) -> None:
    """Each read 'Converted cleanly' and is a syntax error on Aurora DSQL -- a DML statement
    is never test-run, so nothing else told."""
    converted, codes, result = _convert(sql)
    assert codes == ["MYSQL_ONLY_DML"] and result.classification.value == "MANUAL"
    assert kept in converted


def test_insert_ignore_is_no_on_conflict_do_nothing() -> None:
    """Not exactly the same: MySQL's IGNORE also cuts a too-long value, stores a default for
    a NULL and skips a row with no parent, each of which Aurora DSQL fails on. The note names
    the form for the duplicates."""
    converted, _codes, result = _convert("INSERT IGNORE INTO customers (id, name) VALUES (10, 'z')")
    assert "ON CONFLICT" not in converted
    assert "ON CONFLICT DO NOTHING" in result.warnings[0].message


def test_a_select_clause_inside_dml_is_no_dml_clause() -> None:
    _converted, codes, _ = _convert(
        "INSERT INTO t (id, val) SELECT id, val FROM s ORDER BY id LIMIT 1"
    )
    assert codes == []


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # A table named low_priority aliased orders ('relation "low_priority" does not exist').
        ("update low_priority orders set status = 'x' where id = 1",
         "UPDATE orders SET status = 'x' WHERE id = 1"),
        # A MIXED_CASE_NAME LOW_PRIORITY, with quoting advice.
        ("UPDATE LOW_PRIORITY orders SET status = 'x' WHERE id = 1",
         "UPDATE orders SET status = 'x' WHERE id = 1"),
        # QUICK read as LOW_PRIORITY's alias, and only LOW_PRIORITY named.
        ("DELETE LOW_PRIORITY QUICK FROM orders WHERE id = 1", "DELETE FROM orders WHERE id = 1"),
        ("delete quick low_priority from orders where id = 1", "DELETE FROM orders WHERE id = 1"),
        # INSERT's were a PARSE_ERROR.
        ("INSERT LOW_PRIORITY INTO t VALUES (9, 9)", "INSERT INTO t VALUES (9, 9)"),
        ("INSERT HIGH_PRIORITY INTO t VALUES (9, 9)", "INSERT INTO t VALUES (9, 9)"),
        ("insert delayed into t values (9, 9)", "INSERT INTO t VALUES (9, 9)"),
        ("WITH x AS (SELECT 1 AS id) DELETE LOW_PRIORITY FROM orders WHERE id IN "
         "(SELECT id FROM x)",
         "WITH x AS (SELECT 1 AS id) DELETE FROM orders WHERE id IN (SELECT id FROM x)"),
    ],
)
def test_a_scheduling_modifier_is_left_out(sql: str, converted: str) -> None:
    """They only schedule MySQL's table-level locks (MySQL 8 ignores DELAYED): left out, the
    statement does on Aurora DSQL what it did on MySQL."""
    out, codes, _ = _convert(sql)
    assert out == converted and codes == []


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # DELETE / UPDATE IGNORE were a PARSE_ERROR ('Unable to parse the SQL as MySQL').
        ("DELETE IGNORE FROM orders WHERE id = 1", "DELETE IGNORE FROM orders WHERE id = 1"),
        ("UPDATE IGNORE orders SET status = 'x' WHERE id = 1",
         "UPDATE IGNORE orders SET status = 'x' WHERE id = 1"),
        ("UPDATE LOW_PRIORITY IGNORE orders AS o SET o.status = 'x' WHERE o.id = 1",
         "UPDATE IGNORE orders AS o SET status = 'x' WHERE o.id = 1"),
        ("INSERT HIGH_PRIORITY IGNORE INTO t VALUES (1, 9)", "INSERT IGNORE INTO t VALUES (1, 9)"),
    ],
)
def test_ignore_parses_and_is_manual(sql: str, converted: str) -> None:
    out, codes, result = _convert(sql)
    assert out == converted and codes == ["MYSQL_ONLY_DML"]
    assert "IGNORE" in result.warnings[0].message


def test_ignore_is_kept_in_the_pretty_conversion() -> None:
    converted = QueryConverter().convert(
        "WITH x AS (SELECT 1 AS id) UPDATE IGNORE orders SET status = 'y' WHERE id IN "
        "(SELECT id FROM x)",
        pretty=True,
    ).converted_sql
    assert "UPDATE IGNORE orders SET" in converted


def test_a_modifier_before_a_multi_table_delete_keeps_its_shape() -> None:
    """QUICK was read as a table aliased o: only 'remove it' was said, not the shape."""
    converted, codes, _ = _convert(
        "DELETE QUICK o FROM orders o JOIN payments p ON p.order_id = o.id"
    )
    assert codes == ["MULTI_TABLE_DML"] and converted.startswith("DELETE o FROM")


def test_an_unterminated_string_is_a_parse_error_not_a_crash() -> None:
    """The tokenizer's error escaped convert() and the Convert button's handler."""
    _converted, codes, result = _convert("DELETE LOW_PRIORITY FROM t WHERE a = 'x")
    assert codes == ["PARSE_ERROR"] and result.converted_sql is None
