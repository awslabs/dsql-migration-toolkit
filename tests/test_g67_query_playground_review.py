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


# --- G80: the review of G76 -----------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        ("INSERT INTO orders (CustomerId, OrderDate) VALUES (1, NOW())", "CustomerId, OrderDate"),
        ("SELECT o.id FROM orders o JOIN items USING (OrderId)", "OrderId"),
        # A non-recursive CTE sees only the CTEs before it: Orders is the real table here.
        ("WITH recent AS (SELECT * FROM Orders WHERE id > 1), orders AS (SELECT * FROM recent) "
         "SELECT * FROM orders", "Orders"),
        # A CTE of another subquery is not in scope.
        ("SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM Customers) AND EXISTS "
         "(WITH Customers AS (SELECT 1 AS a) SELECT a FROM Customers)", "Customers"),
    ],
)
def test_names_are_resolved_in_their_own_scope(sql: str, named: str) -> None:
    """INSERT column lists and JOIN ... USING were never read, and a CTE hid a real table
    anywhere in the statement: SQL that fails on Aurora DSQL read 'Converted cleanly'."""
    result = QueryConverter().convert(sql)
    (warning,) = [w for w in result.warnings if w.code == "MIXED_CASE_NAME"]
    assert f"Name(s) {named} " in warning.message


def test_a_reused_derived_alias_is_resolved_per_branch() -> None:
    """t was keyed by alias text, the last one winning: t.UserId read as a real column and
    the advice to quote it broke the query."""
    result = QueryConverter().convert(
        "SELECT t.UserId FROM (SELECT id AS UserId FROM a) t "
        "UNION ALL SELECT t.Name FROM (SELECT name AS Name FROM b) t"
    )
    assert [w.code for w in result.warnings] == ["MIXED_CASE_LABEL"]


def test_a_having_alias_is_inlined_whatever_its_case() -> None:
    """MySQL resolves `HAVING Total` to the alias total; the inliner matched case-sensitively
    and left a reference Aurora DSQL rejects."""
    result = QueryConverter().convert(
        "SELECT g, SUM(x) AS total FROM t GROUP BY g HAVING Total > 5"
    )
    assert result.converted_sql.endswith("HAVING SUM(x) > 5")
    assert result.classification.value == "AUTO"


@pytest.mark.parametrize(
    "sql",
    [
        "WITH totals AS (SELECT customer_id, SUM(amount) AS TotalSpent FROM orders "
        "GROUP BY customer_id) SELECT * FROM totals",
        "SELECT * FROM (SELECT id AS UserId FROM users) d",
        "(SELECT id AS UserId FROM a) UNION (SELECT id FROM b)",
    ],
)
def test_a_label_that_reaches_the_client_through_a_star_is_named(sql: str) -> None:
    result = QueryConverter().convert(sql)
    assert [w.code for w in result.warnings] == ["MIXED_CASE_LABEL"]


# --- G89: the review of G80 -----------------------------------------------------------


def test_a_postgresql_having_is_never_inlined() -> None:
    """PostgreSQL's HAVING never names an output alias: the folded key turned
    `sum(total) AS Total ... HAVING sum(total) > 100` into SUM(SUM(total))."""
    from dsql_migrator.core.models import SourceType

    converter = QueryConverter(source_type=SourceType.POSTGRES)
    for sql, tail in (
        ("SELECT customer_id, sum(total) AS Total FROM orders GROUP BY customer_id HAVING sum(total) > 100",
         "HAVING SUM(total) > 100"),
        ("SELECT coalesce(region, 'n/a') AS Region, sum(amount) AS total FROM sales GROUP BY region "
         "HAVING region IS NOT NULL", "HAVING NOT region IS NULL"),
    ):
        assert converter.convert(sql).converted_sql.endswith(tail), sql


@pytest.mark.parametrize(
    ("sql", "tail"),
    [
        # MySQL reads a name inside an aggregate in HAVING from FROM, not the alias.
        ("SELECT customer_id, SUM(total) AS Total FROM orders GROUP BY customer_id HAVING SUM(total) > 100",
         "HAVING SUM(total) > 100"),
        # A HAVING name that is also a GROUP BY column means the column.
        ("SELECT COALESCE(region, 'n/a') AS Region, SUM(amount) AS total FROM sales GROUP BY region "
         "HAVING region IS NOT NULL", "HAVING NOT region IS NULL"),
        # ...while a plain alias reference is inlined, whatever its case.
        ("SELECT g, SUM(x) AS total FROM t GROUP BY g HAVING Total > 5", "HAVING SUM(x) > 5"),
    ],
)
def test_the_mysql_having_inliner_reads_names_as_mysql_does(sql: str, tail: str) -> None:
    assert QueryConverter().convert(sql).converted_sql.endswith(tail)


@pytest.mark.parametrize(
    ("sql", "named"),
    [
        # A backquoted definition keeps its case on Aurora DSQL; the unquoted reference folds.
        ("SELECT O.id FROM `orders` AS `O` WHERE O.status = 'x'", "O"),
        ("INSERT INTO t (a) SELECT d.UserId FROM (SELECT id AS `UserId` FROM x) d", "UserId"),
    ],
)
def test_a_reference_that_only_folds_to_a_quoted_definition_is_flagged(sql: str, named: str) -> None:
    result = QueryConverter().convert(sql)
    (warning,) = [w for w in result.warnings if w.code == "MIXED_CASE_NAME"]
    assert f"Name(s) {named} " in warning.message


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE orders O SET status = 'shipped' WHERE O.id = 1",
        "DELETE FROM orders AS O WHERE O.id = 1",
        "UPDATE orders AS O SET status = 'x' WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id = O.id)",
        "UPDATE orders o JOIN (SELECT order_id, SUM(qty) AS TotalQty FROM items GROUP BY order_id) t "
        "ON t.order_id = o.id SET o.total_qty = t.TotalQty",
        "SELECT C2.id FROM a JOIN (b JOIN c AS C2 ON b.id = C2.bid) ON a.id = b.aid",
    ],
)
def test_dml_and_nested_join_aliases_are_the_statements_own(sql: str) -> None:
    """Aliases outside a SELECT's FROM/JOIN list were flagged with advice to quote them,
    which breaks the statement."""
    result = QueryConverter().convert(sql)
    assert "MIXED_CASE_NAME" not in [w.code for w in result.warnings], result.warnings


def test_a_using_name_only_the_statements_own_sources_hold_is_not_a_schema_name() -> None:
    result = QueryConverter().convert(
        "WITH a AS (SELECT id AS UserId FROM t1), b AS (SELECT id AS UserId FROM t2) "
        "SELECT * FROM a JOIN b USING (UserId)"
    )
    assert [w.code for w in result.warnings] == ["MIXED_CASE_LABEL"]


def test_a_wide_statement_is_scanned_in_linear_time() -> None:
    """Each column re-resolved its scope: 4,000 columns over a CTE took 10 s on the event
    loop."""
    import time

    columns = ", ".join(f"c{i}" for i in range(3000))
    sql = f"WITH cte AS (SELECT {columns} FROM t) SELECT {columns} FROM cte"
    started = time.perf_counter()
    QueryConverter().convert(sql)
    assert time.perf_counter() - started < 3
