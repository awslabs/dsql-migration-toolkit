"""G96: the review of G89 -- the Query Converter's HAVING inliner reads a GROUP BY name as
MySQL does, MySQL-only multi-table DML is MANUAL, a SELECT * derived table and a nested
parenthesised join are no statement-defined sources, aliases that differ only by case and
names quoted in one place only are flagged, a LATERAL is a derived table, and the scan is
linear in an expression's depth."""

from __future__ import annotations

import time

import pytest

from dsql_migrator.core.query_converter import QueryConverter


def _convert(sql: str):
    result = QueryConverter().convert(sql)
    return result.converted_sql, [w.code for w in result.warnings], result


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # The GROUP BY name is the alias itself: HAVING means it too (DSQL never reads an
        # output name in HAVING).
        ("SELECT DATE(created_at) AS day, SUM(total) AS revenue FROM orders GROUP BY day "
         "HAVING day >= '2024-01-01'", "HAVING CAST(created_at AS DATE) >= '2024-01-01'"),
        ("SELECT customer_id AS cid, COUNT(*) AS n FROM orders GROUP BY cid HAVING cid > 100",
         "HAVING customer_id > 100"),
        # A qualified GROUP BY column is a FROM column: it wins over the alias, as in MySQL.
        ("SELECT COALESCE(s.region, 'n/a') AS region, SUM(s.amount) AS total FROM sales s "
         "GROUP BY s.region HAVING region IS NOT NULL", "HAVING NOT region IS NULL"),
        # The alias reads a column of its own name: the FROM clause has it.
        ("SELECT COALESCE(region, 'n/a') AS region, SUM(amount) AS t FROM sales GROUP BY region "
         "HAVING region IS NOT NULL", "HAVING NOT region IS NULL"),
    ],
)
def test_a_having_name_is_read_as_mysql_reads_it(sql: str, having: str) -> None:
    converted, _codes, _ = _convert(sql)
    assert having in converted, converted


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE orders o JOIN (SELECT order_id, SUM(qty) AS TotalQty FROM items GROUP BY order_id) t "
        "ON t.order_id = o.id SET o.total_qty = t.TotalQty",
        "UPDATE orders O JOIN customers C ON C.id = O.cid SET O.region = C.region",
        "UPDATE orders o, customers c SET o.region = c.region WHERE c.id = o.cid",
        "DELETE O FROM orders AS O JOIN items i ON i.oid = O.id",
        "DELETE FROM O USING orders AS O JOIN items i ON i.oid = O.id",
    ],
)
def test_mysql_only_multi_table_dml_is_manual(sql: str) -> None:
    """Read 'Converted cleanly', and DML is never test-run on the target."""
    _converted, codes, result = _convert(sql)
    assert "MULTI_TABLE_DML" in codes and result.classification.value == "MANUAL"
    assert "MIXED_CASE_NAME" not in codes  # its aliases are the statement's own


def test_a_single_table_update_sets_its_own_column_unqualified() -> None:
    converted, codes, _ = _convert("UPDATE orders o SET o.status = 'x' WHERE o.id = 1")
    assert "SET status = 'x'" in converted and codes == []


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT u.name, o.spend FROM (SELECT * FROM users) u JOIN (SELECT user_id AS UserId, "
        "SUM(total) AS spend FROM orders GROUP BY user_id) o USING (UserId)",
        "SELECT a.x FROM (SELECT 1 AS UserId, 2 AS x) a JOIN ((users b JOIN profiles c ON "
        "b.id = c.id) JOIN prefs d ON d.id = c.id) USING (UserId)",
        "SELECT * FROM users u JOIN (SELECT user_id AS UserId FROM orders) o USING (UserId)",
    ],
)
def test_a_using_name_a_real_table_may_hold_is_flagged(sql: str) -> None:
    """A SELECT * derived table and a doubly parenthesised join looked statement-defined,
    so a real "UserId" column read clean and DSQL refused the USING."""
    _converted, codes, _ = _convert(sql)
    assert "MIXED_CASE_NAME" in codes


def test_aliases_that_differ_only_by_case_are_flagged() -> None:
    _converted, codes, _ = _convert(
        "SELECT o.id FROM orders o WHERE o.total = (SELECT MAX(O.total) FROM orders O "
        "WHERE O.customer_id = o.customer_id)"
    )
    assert "CASE_ONLY_ALIASES" in codes
    # Sibling scopes never meet: no warning.
    _converted, codes, _ = _convert(
        "SELECT (SELECT MAX(o.total) FROM orders o), (SELECT MIN(O.total) FROM orders O)"
    )
    assert "CASE_ONLY_ALIASES" not in codes


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT `O`.id FROM orders O WHERE `O`.status = 'x'",
        "SELECT d.`UserId` FROM (SELECT id AS UserId FROM users) d",
    ],
)
def test_a_name_quoted_in_one_place_only_is_flagged(sql: str) -> None:
    _converted, codes, _ = _convert(sql)
    assert "QUOTE_MISMATCH" in codes


def test_a_lateral_derived_tables_outputs_are_the_statements_own() -> None:
    _converted, codes, _ = _convert(
        "SELECT L.Total FROM orders o, LATERAL (SELECT SUM(qty) AS Total FROM items i "
        "WHERE i.oid = o.id) AS L"
    )
    assert "MIXED_CASE_NAME" not in codes


def test_a_deep_predicate_is_scanned_in_linear_time() -> None:
    """Every column walked its parents to its scope: 4,000 OR terms took 3.4 s."""

    def took(terms: int) -> float:
        sql = "SELECT * FROM t WHERE " + " OR ".join(f"(a = {i} AND b = {i})" for i in range(terms))
        started = time.perf_counter()
        QueryConverter().convert(sql)
        return time.perf_counter() - started

    took(200)  # warm up
    small, large = took(1000), took(4000)
    assert large < small * 8  # linear: 4x the terms, well under 16x the time
