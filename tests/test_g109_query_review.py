"""G109: the review of G101-G105 -- the Query Converter says how to keep a unique-key shift's
order, reports a PARTITION selection and writes an all-defaults INSERT as DEFAULT VALUES,
leaves out MySQL's SELECT modifiers at every SELECT, reads SET col = DEFAULT as the keyword,
relaxes the bare-name rule only where MySQL's own ambiguity check holds, resolves a
qualifier as MySQL does by its column too, finds DML modifiers after a hint, under EXPLAIN
and in each statement, writes WITH ROLLUP as ROLLUP (...) and reports user variables. Each
statement was checked on MySQL 8.4 and PostgreSQL 17."""

from __future__ import annotations

import pytest

from dsql_migrator.core.converter import table_type_index
from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.query_converter import QueryConverter


def _convert(sql: str, converter: "QueryConverter | None" = None):
    result = (converter or QueryConverter()).convert(sql)
    return result.converted_sql, [w.code for w in result.warnings], result


def test_an_update_that_shifts_a_unique_key_is_told_how_to_keep_its_order() -> None:
    """'Without a LIMIT, the order does not change which rows are touched: remove it.' MySQL
    checks UNIQUE (list_id, pos) row by row, so ORDER BY pos DESC is what lets it pass
    (pos 1, 2, 4, 5, 6); without it MySQL ('Duplicate entry 7-4') and PostgreSQL ('duplicate
    key value violates unique constraint') both fail. Two steps pass on PostgreSQL."""
    converted, codes, result = _convert(
        "UPDATE list_items SET pos = pos + 1 WHERE list_id = 7 AND pos >= 3 ORDER BY pos DESC"
    )
    assert codes == ["MYSQL_ONLY_DML"] and "ORDER BY pos DESC" in converted
    message = result.warnings[0].message
    assert "remove it" not in message and "row by row" in message
    assert "SET pos = -(pos + 1)" in message and "SET pos = -pos WHERE pos < 0" in message
    assert "If the SET changes no unique or primary key column" in message


def test_a_delete_order_by_keeps_its_wording() -> None:
    _converted, codes, result = _convert("DELETE FROM orders WHERE status = 'old' ORDER BY id")
    assert codes == ["MYSQL_ONLY_DML"]
    assert result.warnings[0].message.endswith(
        "Without a LIMIT, the order does not change which rows are touched: remove it."
    )


@pytest.mark.parametrize(
    ("sql", "code"),
    [
        # MySQL deletes only p0's rows (1, 3 remain of 1, 2, 3); PostgreSQL refuses the
        # syntax, and left out the DELETE would empty the table. Each was AUTO.
        ("DELETE FROM parts PARTITION (p0) WHERE v > 0", "MYSQL_ONLY_DML"),
        ("UPDATE parts PARTITION (p1) SET v = 9", "MYSQL_ONLY_DML"),
        ("INSERT INTO parts PARTITION (p0) (id, v) VALUES (4, 4)", "MYSQL_ONLY_DML"),
        ("INSERT INTO t SELECT id, v FROM parts PARTITION (p0, p1)", "MYSQL_ONLY_DML"),
        ("SELECT p.id FROM parts PARTITION (p0) p", "MYSQL_ONLY_SELECT"),
    ],
)
def test_a_partition_selection_is_manual_and_left_as_written(sql: str, code: str) -> None:
    converted, codes, result = _convert(sql)
    assert codes == [code] and result.classification.value == "MANUAL"
    assert "PARTITION(p" in converted and "no partitions" in result.warnings[0].message


@pytest.mark.parametrize(
    "sql", ["INSERT INTO dflt () VALUES ()", "INSERT INTO dflt VALUES ()", "insert into dflt () values ()"]
)
def test_a_row_of_defaults_is_default_values(sql: str) -> None:
    """It was 'INSERT INTO dflt VALUES ()', AUTO: 'syntax error at or near ")"' on
    PostgreSQL. DEFAULT VALUES inserts the row MySQL does (5, 'x')."""
    converted, codes, _ = _convert(sql)
    assert converted == "INSERT INTO dflt DEFAULT VALUES" and codes == []


def test_several_rows_of_defaults_are_manual() -> None:
    converted, codes, result = _convert("INSERT INTO dflt () VALUES (), ()")
    assert codes == ["MYSQL_ONLY_DML"] and "VALUES (), ()" in converted
    assert "(DEFAULT), (DEFAULT)" in result.warnings[0].message


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # Each read the modifier as a column ('column "high_priority" does not exist') or
        # failed to parse; MySQL runs each.
        ("SELECT HIGH_PRIORITY id FROM orders", "SELECT id FROM orders"),
        ("SELECT SQL_NO_CACHE id FROM orders", "SELECT id FROM orders"),
        ("select sql_no_cache * from orders", "SELECT * FROM orders"),
        ("SELECT SQL_BUFFER_RESULT id FROM orders", "SELECT id FROM orders"),
        ("SELECT SQL_SMALL_RESULT customer_id, COUNT(*) FROM orders GROUP BY customer_id",
         "SELECT customer_id, COUNT(*) FROM orders GROUP BY customer_id"),
        ("SELECT SQL_BIG_RESULT customer_id FROM orders", "SELECT customer_id FROM orders"),
        ("SELECT SQL_CACHE id FROM orders", "SELECT id FROM orders"),
        ("SELECT STRAIGHT_JOIN o.id FROM orders o JOIN payments p ON p.order_id = o.id",
         "SELECT o.id FROM orders AS o JOIN payments AS p ON p.order_id = o.id"),
        # In any order among DISTINCT / DISTINCTROW (SQL_NO_CACHE DISTINCT was a PARSE_ERROR).
        ("SELECT SQL_NO_CACHE DISTINCT customer_id FROM orders",
         "SELECT DISTINCT customer_id FROM orders"),
        ("SELECT DISTINCT SQL_NO_CACHE customer_id FROM orders",
         "SELECT DISTINCT customer_id FROM orders"),
        ("SELECT SQL_BIG_RESULT STRAIGHT_JOIN HIGH_PRIORITY DISTINCTROW customer_id FROM orders",
         "SELECT DISTINCT customer_id FROM orders"),
        # At every SELECT, and the STRAIGHT_JOIN join operator is a JOIN.
        ("SELECT id FROM orders WHERE id IN (SELECT SQL_NO_CACHE order_id FROM payments)",
         "SELECT id FROM orders WHERE id IN (SELECT order_id FROM payments)"),
        ("SELECT 1 UNION SELECT SQL_NO_CACHE 2", "SELECT 1 UNION SELECT 2"),
        ("SELECT o.id FROM orders o STRAIGHT_JOIN payments p ON p.order_id = o.id",
         "SELECT o.id FROM orders AS o JOIN payments AS p ON p.order_id = o.id"),
    ],
)
def test_select_modifiers_are_left_out(sql: str, converted: str) -> None:
    out, codes, _ = _convert(sql)
    assert out == converted and codes == []


def test_a_select_modifier_after_an_optimizer_hint_is_left_out() -> None:
    converted, codes, _ = _convert("SELECT /*+ MAX_EXECUTION_TIME(1000) */ SQL_NO_CACHE id FROM orders")
    assert converted == "SELECT id FROM orders" and "PARSE_ERROR" not in codes


def test_sql_cache_before_a_comma_is_a_column() -> None:
    """MySQL 8 dropped the SQL_CACHE modifier: SELECT SQL_CACHE, id reads a column of that
    name ('Unknown column SQL_CACHE'), so it stays."""
    converted, _codes, _ = _convert("SELECT SQL_CACHE, id FROM orders")
    assert converted == "SELECT SQL_CACHE, id FROM orders"


def test_sql_calc_found_rows_is_manual_and_left_out() -> None:
    """The rows are the same without it; the FOUND_ROWS() MySQL answers after it (7 here)
    has no Aurora DSQL form."""
    converted, codes, result = _convert("SELECT SQL_CALC_FOUND_ROWS id FROM orders LIMIT 2")
    assert converted == "SELECT id FROM orders LIMIT 2"
    assert codes == ["MYSQL_ONLY_SELECT"] and "COUNT(*)" in result.warnings[0].message


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # MIXED_CASE_NAME 'DEFAULT ... quote each', whose advice (SET v = "DEFAULT") fails.
        ("UPDATE parts SET v = DEFAULT WHERE id = 1", "UPDATE parts SET v = DEFAULT WHERE id = 1"),
        ("UPDATE parts SET v = Default WHERE id = 1", "UPDATE parts SET v = Default WHERE id = 1"),
    ],
)
def test_set_col_to_default_is_the_keyword(sql: str, converted: str) -> None:
    out, codes, _ = _convert(sql)
    assert out == converted and codes == []


def test_on_duplicate_key_update_to_default_is_the_keyword() -> None:
    _converted, codes, _ = _convert(
        "INSERT INTO parts (id, v) VALUES (1, 7) ON DUPLICATE KEY UPDATE v = DEFAULT"
    )
    assert codes == ["ON_DUPLICATE_KEY_UPDATE"]


@pytest.mark.parametrize(
    "sql",
    [
        # MySQL coalesces CustomerId (2 rows); on Aurora DSQL the CTE's label folds, the
        # NATURAL JOIN finds no common column and returns 9. It was AUTO.
        "WITH recent AS (SELECT customer_id AS CustomerId, MAX(created_at) AS last_order FROM "
        "orders GROUP BY customer_id) SELECT r.last_order FROM recent r NATURAL JOIN mc2 WHERE "
        "CustomerId > 10",
        # The same with no bare name: 3 rows on MySQL, 12 converted.
        "SELECT a.n FROM (SELECT customer_id AS CustomerId, COUNT(*) AS n FROM orders GROUP BY "
        "customer_id) a NATURAL JOIN mc2",
        # An ON sees only its join's operands: RowNum is mc's ('column "rownum" does not
        # exist'), not the derived table's joined after it.
        "SELECT mc.id FROM mc JOIN mc2 ON mc2.id = RowNum JOIN (SELECT 1 AS RowNum) d ON 1 = 1",
        # The comma binds looser than JOIN: USING's left side is mc alone ('column
        # "customerid" specified in USING clause does not exist in left table').
        "SELECT b.z FROM (SELECT 10 AS CustomerId) a, mc JOIN (SELECT 10 AS CustomerId, 1 AS z) "
        "b USING (CustomerId)",
        "SELECT a.CustomerId FROM (SELECT 10 AS CustomerId) a JOIN (mc JOIN (SELECT 10 AS "
        "CustomerId, 1 AS z) b USING (CustomerId)) ON 1 = 1",
    ],
)
def test_the_relaxed_bare_name_rule_holds_only_where_mysql_refuses_ambiguity(sql: str) -> None:
    _converted, codes, result = _convert(sql)
    assert "MIXED_CASE_NAME" in codes and result.classification.value == "MANUAL"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT d.id, c.name FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER "
        "BY created_at DESC) AS RowNum FROM orders) d JOIN customers c ON c.id = d.customer_id "
        "WHERE RowNum = 1",
        # The idiom's filter in a LEFT JOIN's ON: both operands are visible there.
        "SELECT c.name, d.total FROM customers c LEFT JOIN (SELECT *, ROW_NUMBER() OVER "
        "(PARTITION BY customer_id ORDER BY created_at DESC) AS RowNum FROM orders) d ON "
        "d.customer_id = c.id AND RowNum = 1",
        # A NATURAL JOIN's label quoted as the schema spells it matches on both engines.
        "WITH recent AS (SELECT customer_id AS `CustomerId`, MAX(created_at) AS last_order "
        "FROM orders GROUP BY customer_id) SELECT r.last_order FROM recent r NATURAL JOIN mc2 "
        "WHERE `CustomerId` > 10",
    ],
)
def test_the_top_n_per_group_idiom_stays_auto(sql: str) -> None:
    _converted, codes, _ = _convert(sql)
    assert codes == []


_TYPED = QueryConverter(
    column_types=tuple(
        table_type_index(
            [
                TableDef(
                    name="shop.customers",
                    columns=[
                        ColumnDef(name="id", mysql_type="int", nullable=False),
                        ColumnDef(name="name", mysql_type="varchar(20)"),
                    ],
                    primary_key=["id"],
                ),
                TableDef(
                    name="shop.orders",
                    columns=[
                        ColumnDef(name="id", mysql_type="int", nullable=False),
                        ColumnDef(name="customer_id", mysql_type="int"),
                    ],
                    primary_key=["id"],
                ),
            ]
        ).values()
    )
)


def test_a_qualifier_mysql_reads_past_for_its_column_is_no_case_only_clash() -> None:
    """customers has no customer_id, so MySQL reads O.customer_id from the outer O -- and so
    does Aurora DSQL, the unquoted O folding past the inner "O": the same rows, [1, 2]. It
    was CASE_ONLY_ALIASES. Without the inventory customers may have one: still flagged."""
    sql = (
        "SELECT O.id FROM orders O WHERE EXISTS (SELECT 1 FROM customers `O` WHERE "
        "`O`.name = 'a' AND `O`.id = O.customer_id)"
    )
    assert _convert(sql, _TYPED)[1] == []
    assert _convert(sql)[1] == ["CASE_ONLY_ALIASES"]


@pytest.mark.parametrize(
    ("sql", "typed"),
    [
        # MySQL reads `O`.id past the inner `O` (no id) from the outer O: 1, 2, 3, 5, 7;
        # Aurora DSQL stops at "O" ('column O.id does not exist'). It was AUTO.
        ("SELECT O.id FROM orders O WHERE EXISTS (SELECT 1 FROM ((SELECT order_id, customer_id "
         "FROM payments) `O` JOIN customers cc ON cc.id = `O`.customer_id) WHERE "
         "`O`.order_id = `O`.id)", False),
        # One spelling: MySQL 5 rows, PostgreSQL an error. AUTO in every tree before.
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM (SELECT order_id FROM "
         "payments) o WHERE o.order_id = o.id)", False),
        # A real table the inventory says has no customer_id: MySQL 7 rows, PostgreSQL 'column
        # o.customer_id does not exist'.
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM customers o WHERE "
         "o.id = o.customer_id)", True),
    ],
)
def test_an_inner_alias_mysql_reads_past_is_shadowed(sql: str, typed: bool) -> None:
    _converted, codes, result = _convert(sql, _TYPED if typed else None)
    assert codes == ["SHADOWED_ALIAS"]
    assert "does not exist" in result.warnings[0].message


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # Each left LOW_PRIORITY / QUICK read as a table: 'MySQL's multi-table DELETE t FROM
        # ...' and MIXED_CASE_NAME 'LOW_PRIORITY ... quote each'.
        ("DELETE /*+ BKA(o) */ LOW_PRIORITY FROM orders WHERE id = 1", "DELETE FROM orders WHERE id = 1"),
        ("DELETE /*+ NO_BNL() */ QUICK FROM orders WHERE id = 1", "DELETE FROM orders WHERE id = 1"),
        ("DELETE LOW_PRIORITY FROM orders WHERE id = 1; DELETE LOW_PRIORITY FROM orders WHERE id = 2",
         "DELETE FROM orders WHERE id = 1; DELETE FROM orders WHERE id = 2"),
        ("SELECT 1; DELETE QUICK FROM orders WHERE id = 2", "SELECT 1; DELETE FROM orders WHERE id = 2"),
        ("EXPLAIN DELETE QUICK FROM orders WHERE id = 1", "DESCRIBE DELETE FROM orders WHERE id = 1"),
        ("DESCRIBE DELETE LOW_PRIORITY FROM orders WHERE id = 1",
         "DESCRIBE DELETE FROM orders WHERE id = 1"),
        ("EXPLAIN FORMAT=JSON DELETE LOW_PRIORITY FROM orders WHERE id = 1",
         "DESCRIBE FORMAT=JSON DELETE FROM orders WHERE id = 1"),
    ],
)
def test_a_dml_modifier_is_found_after_a_hint_under_explain_and_in_each_statement(
    sql: str, converted: str
) -> None:
    out, codes, _ = _convert(sql)
    assert out == converted
    assert "MULTI_TABLE_DML" not in codes and "MIXED_CASE_NAME" not in codes


def test_ignore_is_put_back_in_its_own_statement() -> None:
    converted, codes, _ = _convert(
        "SELECT 1; UPDATE IGNORE orders SET status = 'x' WHERE id = 1; DELETE IGNORE FROM "
        "orders WHERE id = 2"
    )
    assert converted == (
        "SELECT 1; UPDATE IGNORE orders SET status = 'x' WHERE id = 1; DELETE IGNORE FROM "
        "orders WHERE id = 2"
    )
    assert codes == ["MYSQL_ONLY_DML", "MYSQL_ONLY_DML"]


@pytest.mark.parametrize(
    "sql",
    ["EXPLAIN SELECT id FROM orders", "EXPLAIN DELETE QUICK FROM orders WHERE id = 1", "DESCRIBE orders"],
)
def test_explain_is_manual(sql: str) -> None:
    """Written DESCRIBE ..., which PostgreSQL refuses ('syntax error at or near
    "DESCRIBE"'): it was AUTO."""
    _converted, codes, _ = _convert(sql)
    assert codes == ["EXPLAIN_STATEMENT"]


@pytest.mark.parametrize(
    ("sql", "group"),
    [
        # 'syntax error at or near "WITH"' on PostgreSQL; ROLLUP (...) returns MySQL's rows.
        ("SELECT o.customer_id, COUNT(*) AS n FROM orders o GROUP BY o.customer_id WITH ROLLUP",
         "GROUP BY ROLLUP (o.customer_id)"),
        ("SELECT region, product, SUM(amount) FROM sales GROUP BY region, product WITH ROLLUP",
         "GROUP BY ROLLUP (region, product)"),
        ("SELECT region, COUNT(*) AS n FROM sales GROUP BY region WITH ROLLUP HAVING n > 1",
         "GROUP BY ROLLUP (region) HAVING COUNT(*) > 1"),
    ],
)
def test_with_rollup_is_rollup_and_manual(sql: str, group: str) -> None:
    converted, codes, result = _convert(sql)
    assert group in converted and "WITH ROLLUP" not in converted
    assert codes == ["WITH_ROLLUP"] and "Test on target" in result.warnings[0].message


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # Written $rn, AUTO: 'syntax error at or near "$"'.
        ("SELECT @rn := @rn + 1 AS rn, id FROM t, (SELECT @rn := 0) init ORDER BY id",
         "SELECT @rn := @rn + 1 AS rn, id FROM t, (SELECT @rn := 0) AS init ORDER BY id NULLS FIRST"),
        ("SET @x = 1", "SET @x = 1"),
        ("SELECT @x", "SELECT @x"),
        ("SELECT id INTO @v FROM t LIMIT 1", "SELECT id INTO @v FROM t LIMIT 1"),
    ],
)
def test_a_user_variable_is_manual_and_left_as_written(sql: str, converted: str) -> None:
    out, codes, result = _convert(sql)
    assert out == converted and codes == ["USER_VARIABLE"]
    assert "ROW_NUMBER() OVER" in result.warnings[0].message
