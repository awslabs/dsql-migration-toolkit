"""G98: the review of G96 -- the Query Converter's HAVING reads an aggregate alias and a
qualified GROUP BY column as MySQL does, a SELECT * derived table keeps its explicit
labels, case-only aliases are flagged only where a reference is captured, and DELETE /
UPDATE edge shapes. Each HAVING case was checked on MySQL 8.4 and PostgreSQL 17."""

from __future__ import annotations

import pytest

from dsql_migrator.core.query_converter import QueryConverter


def _convert(sql: str):
    result = QueryConverter().convert(sql)
    return result.converted_sql, [w.code for w in result.warnings]


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # MySQL refuses GROUP BY on an aggregate alias, so GROUP BY col2 is t.col2, and so
        # is HAVING col2 (MySQL returns [1]; inlined, [2]).
        ("SELECT COUNT(col1) AS col2 FROM t GROUP BY col2 HAVING col2 = 2", "HAVING col2 = 2"),
        ("SELECT COUNT(*) AS total FROM orders GROUP BY total HAVING total > 5", "HAVING total > 5"),
        # A qualified GROUP BY column is written qualified in HAVING (bare, it is ambiguous
        # on PostgreSQL when a joined table has the name too).
        ("SELECT o.customer_id AS customer_id, COUNT(*) AS n FROM orders o JOIN payments p "
         "ON p.order_id = o.id GROUP BY o.customer_id HAVING customer_id > 11",
         "HAVING o.customer_id > 11"),
        ("SELECT DATE(created_at) AS day, SUM(total) AS revenue FROM orders GROUP BY day "
         "HAVING day >= '2024-01-01'", "HAVING CAST(created_at AS DATE) >= '2024-01-01'"),
    ],
)
def test_having_reads_a_name_as_mysql_does(sql: str, having: str) -> None:
    converted, _codes = _convert(sql)
    assert having in converted, converted


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT d.id FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY "
        "created_at DESC) AS RowNum FROM orders) d WHERE d.RowNum = 1",
        "WITH ranked AS (SELECT *, ROW_NUMBER() OVER (ORDER BY id) AS RowNum FROM orders) "
        "SELECT id FROM ranked WHERE RowNum = 1",
        "SELECT u.name FROM (SELECT *, 1 AS UserId FROM t) u JOIN (SELECT 1 AS UserId) o USING (UserId)",
    ],
)
def test_a_starred_sources_explicit_labels_are_its_own(sql: str) -> None:
    """They were flagged MIXED_CASE_NAME with quoting advice that breaks the statement."""
    _converted, codes = _convert(sql)
    assert "MIXED_CASE_NAME" not in codes


@pytest.mark.parametrize(
    "sql",
    [
        "WITH totals AS (SELECT C.id, COUNT(*) AS n FROM customers C GROUP BY C.id) "
        "SELECT c.name, t.n FROM customers c JOIN totals t ON t.id = c.id",
        "SELECT o.id FROM (SELECT O.id FROM orders O WHERE O.total > 5) o",
        "SELECT o.id FROM orders o WHERE o.customer_id IN (SELECT O.customer_id FROM orders O "
        "WHERE O.total > 5)",
    ],
)
def test_case_only_aliases_no_reference_crosses_are_not_flagged(sql: str) -> None:
    _converted, codes = _convert(sql)
    assert "CASE_ONLY_ALIASES" not in codes


def test_a_captured_correlated_reference_and_a_self_join_are_flagged() -> None:
    for sql in (
        "SELECT o.id FROM orders o WHERE o.total = (SELECT MAX(O.total) FROM orders O "
        "WHERE O.customer_id = o.customer_id)",
        "SELECT o.id FROM orders o JOIN orders O ON O.ref = o.id",
    ):
        assert "CASE_ONLY_ALIASES" in _convert(sql)[1], sql


def test_a_doubly_parenthesised_derived_table_keeps_its_alias() -> None:
    _converted, codes = _convert(
        "SELECT x.Total FROM ((SELECT total AS Total FROM orders ORDER BY id LIMIT 3)) AS x "
        "WHERE x.Total > 5"
    )
    assert "MIXED_CASE_NAME" not in codes and "MIXED_CASE_LABEL" in codes


def test_a_from_less_delete_is_reported_not_a_crash() -> None:
    converted, codes = _convert("DELETE orders WHERE id = 1")
    assert codes == ["MULTI_TABLE_DML"] and converted


def test_a_db_qualified_set_target_is_written_unqualified() -> None:
    converted, codes = _convert("UPDATE r.orders SET r.orders.status = 'zz' WHERE id = 1")
    assert "SET status = 'zz'" in converted and codes == []


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT `UserId` FROM (SELECT id AS UserId FROM users) d",
        "WITH c AS (SELECT id AS UserId FROM users) SELECT `UserId` FROM c",
    ],
)
def test_a_bare_quoted_reference_to_an_unquoted_output_is_flagged(sql: str) -> None:
    assert "QUOTE_MISMATCH" in _convert(sql)[1]


@pytest.mark.parametrize("modifier", ["LOW_PRIORITY", "QUICK"])
def test_a_delete_modifier_is_named_as_one(modifier: str) -> None:
    result = QueryConverter().convert(f"DELETE {modifier} FROM orders WHERE id = 99")
    (warning,) = result.warnings
    assert "modifier" in warning.message and "multi-table" not in warning.message
