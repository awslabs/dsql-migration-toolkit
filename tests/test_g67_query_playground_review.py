"""G67: the review of G44-G55 -- Query Playground's mixed-case check flagged names the
statement defines itself, and DUAL."""

from __future__ import annotations

import pytest

from dsql_migrator.core.query_converter import QueryConverter


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT O.id FROM orders AS O WHERE O.status = 'paid'",
        "SELECT A.id, B.total FROM users A JOIN orders B ON B.user_id = A.id",
        "WITH Recent AS (SELECT id FROM orders) SELECT id FROM Recent",
        "SELECT id AS userid FROM users ORDER BY UserId",
        "SELECT x.total FROM (SELECT amount AS total FROM payments) X",
        "SELECT total FROM (SELECT amount AS Total FROM payments) d",
        "WITH Recent(UserId) AS (SELECT id FROM orders) SELECT UserId AS uid FROM Recent",
        "SELECT 1 FROM DUAL",
    ],
)
def test_names_the_statement_defines_are_not_mixed_case_schema_names(sql: str) -> None:
    """An alias is folded the same where it is defined and where it is used, so the SQL
    runs unchanged -- yet it read MANUAL with advice to quote it, which breaks it."""
    result = QueryConverter().convert(sql)
    assert result.classification.value == "AUTO", [w.message for w in result.warnings]


def test_a_real_mixed_case_name_is_still_flagged() -> None:
    result = QueryConverter().convert("SELECT o.CustomerId FROM Orders AS o")
    (warning,) = [w for w in result.warnings if w.code == "MIXED_CASE_NAME"]
    assert "Orders, CustomerId" in warning.message


def test_from_dual_is_dropped_not_carried() -> None:
    """Aurora DSQL has no dual: the converted SQL failed with relation "dual" does not exist."""
    assert QueryConverter().convert("SELECT NOW() FROM dual WHERE 1 = 1").converted_sql == (
        "SELECT NOW() WHERE 1 = 1"
    )
    assert QueryConverter().convert("SELECT 1 FROM shop.dual").converted_sql == (
        "SELECT 1 FROM shop.dual"  # a real table of that name stays
    )


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        ("SELECT SUM(Amount) AS amount FROM orders", "Amount"),
        ("SELECT u.UserName AS username FROM users u", "UserName"),
        ("SELECT customer.id FROM Customer customer", "Customer"),
        ("DELETE FROM orders WHERE OrderId IN (SELECT OrderId AS orderid FROM archive)", "OrderId"),
        ("WITH orders AS (SELECT * FROM Orders) SELECT * FROM orders", "Orders"),
        ("SELECT SUM(x) OVER (ORDER BY UserId) AS s FROM t", "UserId"),
    ],
)
def test_a_real_name_that_folds_to_a_defined_one_is_still_flagged(sql: str, named: str) -> None:
    """G75: every name that folded to ANY alias or CTE was skipped -- the table carrying
    the alias, the column inside the aliased expression, a CTE's own body -- so SQL that
    fails on Aurora DSQL read 'Converted cleanly'."""
    result = QueryConverter().convert(sql)
    (warning,) = [w for w in result.warnings if w.code == "MIXED_CASE_NAME"]
    assert f"Name(s) {named} " in warning.message


def test_a_backquoted_dual_is_a_real_table() -> None:
    """MySQL's DUAL keyword is never quoted: `dual` is a table, and dropping its FROM made
    SELECT COUNT(*) FROM `dual` return 1."""
    converted = QueryConverter().convert("SELECT COUNT(*) FROM `dual`").converted_sql
    assert converted == 'SELECT COUNT(*) FROM "dual"'


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id AS UserId FROM users ORDER BY UserId",
        "SELECT id AS UserId FROM a UNION SELECT id FROM b",
        "SELECT d.Total FROM (SELECT SUM(x) AS Total FROM t) d",
    ],
)
def test_a_mixed_case_result_label_is_named(sql: str) -> None:
    """G75: Aurora DSQL folds an unquoted label (UserId comes back as userid): the query
    runs, but a client reading columns by name misses it -- it read 'Converted cleanly'."""
    result = QueryConverter().convert(sql)
    assert [w.code for w in result.warnings] == ["MIXED_CASE_LABEL"]
    assert result.classification.value == "MANUAL"
    assert "label it quoted" in result.warnings[0].message


def test_a_flagged_name_gets_no_second_label_note() -> None:
    result = QueryConverter().convert("SELECT CustomerId FROM orders")
    assert [w.code for w in result.warnings] == ["MIXED_CASE_NAME"]
