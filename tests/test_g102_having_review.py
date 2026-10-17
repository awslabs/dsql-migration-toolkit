"""G102: the review of G97-G100 -- the shared HAVING inliner reads a name as MySQL does: an
aggregate in a scalar subquery is the subquery's (the alias is still grouped by itself), a
HAVING name is a GROUP BY column or a select-list item written qualified whether or not
the select list has an alias, and a HAVING with neither GROUP BY nor an aggregate is the
row filter MySQL makes it (a WHERE). Each case was run on MySQL 8.4 and PostgreSQL 17 over
the same rows: a view as MySQL stores it and a Query Converter statement."""

from __future__ import annotations

import pytest
import sqlglot

from dsql_migrator.core.models import ViewDef
from dsql_migrator.core.query_converter import QueryConverter

_JOIN = "FROM orders o JOIN payments p ON p.order_id = o.id"


def _convert(sql: str):
    result = QueryConverter().convert(sql)
    return result.converted_sql, result.classification.value


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body))
    return " ".join(view.target_ddl.split()), view


# --- G98-2: an aggregate inside a scalar subquery belongs to the subquery -----------------


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # MySQL groups by the alias (orders bucketed by their payment count): [1->4, 2->1].
        ("SELECT (SELECT COUNT(*) FROM payments p WHERE p.order_id = o.id) AS pay_count, "
         "COUNT(*) AS orders FROM orders o GROUP BY pay_count HAVING pay_count > 0",
         "HAVING (SELECT COUNT(*) FROM payments AS p WHERE p.order_id = o.id) > 0"),
        # The subquery's p.amount is no FROM column of the outer query.
        ("SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.order_id = o.id) AS amount, "
         "COUNT(*) AS n FROM orders o GROUP BY amount HAVING amount > 10",
         "HAVING (SELECT MAX(p.amount) FROM payments AS p WHERE p.order_id = o.id) > 10"),
        # An aggregate of the alias's own level still makes the GROUP BY name a FROM column.
        ("SELECT COUNT(*) + (SELECT 1) AS n FROM t GROUP BY n HAVING n > 1", "HAVING n > 1"),
    ],
)
def test_a_subquery_alias_is_grouped_by_itself(sql: str, having: str) -> None:
    """The bare HAVING name was kept as a FROM column: 'column pay_count does not exist'."""
    converted, classification = _convert(sql)
    assert having in converted, converted
    assert classification == "AUTO"


def test_a_view_grouped_by_its_subquery_alias_reads_the_subquery() -> None:
    ddl, view = _view(
        "select (select count(0) from `shop`.`payments` `p` where (`p`.`order_id` = `o`.`id`)) "
        "AS `pay_count`,count(0) AS `orders` from `shop`.`orders` `o` group by `pay_count` "
        "having (`pay_count` > 0)"
    )
    assert 'HAVING ( ( SELECT COUNT(0) FROM "shop"."payments" AS "p" WHERE' in ddl, ddl
    assert view.auto_converted


# --- G98-5: a select-list column, with or without any alias ----------------------------


@pytest.mark.parametrize(
    "sql",
    [
        # No alias at all: the qualified GROUP BY column was skipped.
        f"SELECT o.customer_id, COUNT(*) {_JOIN} GROUP BY o.customer_id HAVING customer_id > 11",
        f"SELECT COUNT(*) {_JOIN} GROUP BY o.customer_id HAVING customer_id > 11",
        # MySQL reads the name from the select list: a plain column, the one GROUP BY 1 or a
        # GROUP BY alias points to, and one behind an alias.
        f"SELECT o.customer_id, MAX(o.total) AS mx {_JOIN} GROUP BY o.id HAVING customer_id > 11",
        f"SELECT o.customer_id, COUNT(*) {_JOIN} GROUP BY 1 HAVING customer_id > 11",
        f"SELECT o.customer_id AS cid, COUNT(*) {_JOIN} GROUP BY o.id HAVING customer_id > 11",
        f"SELECT o.customer_id AS cid, COUNT(*) {_JOIN} GROUP BY 1 HAVING customer_id > 11",
        f"SELECT o.customer_id AS cid, COUNT(*) {_JOIN} GROUP BY cid HAVING customer_id > 11",
    ],
)
def test_a_having_name_is_the_select_list_column_mysql_reads(sql: str) -> None:
    """Bare, it was 'column reference customer_id is ambiguous' on Aurora DSQL (payments
    has one too), classified AUTO."""
    converted, _classification = _convert(sql)
    assert converted.endswith("HAVING o.customer_id > 11"), converted


def test_an_alias_still_wins_over_a_select_list_column() -> None:
    """MySQL reads a non-column alias first (it never reaches the column behind it)."""
    converted, _ = _convert(
        "SELECT o.id, o.total * 2 AS total, COUNT(*) AS n FROM orders o JOIN payments p "
        "ON p.order_id = o.id GROUP BY o.id HAVING total > 20"
    )
    assert converted.endswith("HAVING o.total * 2 > 20"), converted


def test_a_correlated_having_name_is_read_where_mysql_reads_it() -> None:
    """MySQL reads HAVING customer_id from the select list -- here the outer o.customer_id
    ([4, 6, 7]); bare, PostgreSQL read the inner p.customer_id (every order) -- unless a
    GROUP BY column has the name, which stays the inner one."""
    converted, _ = _convert(
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT o.customer_id FROM payments p "
        "GROUP BY p.id HAVING customer_id > 12)"
    )
    assert converted.endswith("GROUP BY p.id HAVING o.customer_id > 12)"), converted
    converted, _ = _convert(
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT o.customer_id FROM payments p "
        "GROUP BY customer_id HAVING customer_id > 12)"
    )
    assert converted.endswith("GROUP BY customer_id HAVING customer_id > 12)"), converted


# --- G98-6: HAVING without GROUP BY or an aggregate filters rows -------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        # The store-locator idiom: MySQL returns each row whose distance is < 70.
        ("SELECT id, (val * 2) AS distance FROM t HAVING distance < 70 ORDER BY distance",
         "SELECT id, (val * 2) AS distance FROM t WHERE (val * 2) < 70 ORDER BY distance NULLS FIRST"),
        ("SELECT val AS v FROM t HAVING v > 30", "SELECT val AS v FROM t WHERE val > 30"),
        # ANDed with the query's own WHERE.
        ("SELECT id FROM t WHERE grp = 1 OR grp = 2 HAVING id > 1 AND id < 4",
         "SELECT id FROM t WHERE (grp = 1 OR grp = 2) AND (id > 1 AND id < 4)"),
        # A subquery's aggregate is the subquery's: still a row filter.
        ("SELECT id, (SELECT COUNT(*) FROM payments p WHERE p.order_id = o.id) AS c FROM orders o "
         "HAVING c > 0",
         "SELECT id, (SELECT COUNT(*) FROM payments AS p WHERE p.order_id = o.id) AS c FROM orders AS o "
         "WHERE (SELECT COUNT(*) FROM payments AS p WHERE p.order_id = o.id) > 0"),
        # MySQL filters before windows: ROW_NUMBER numbers the rows that pass (3->1).
        ("SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM t HAVING id > 2",
         "SELECT id, ROW_NUMBER() OVER (ORDER BY id NULLS FIRST) AS rn FROM t WHERE id > 2"),
        ("SELECT id, SUM(val) OVER () AS s FROM t HAVING id > 3",
         "SELECT id, SUM(val) OVER () AS s FROM t WHERE id > 3"),
        # A constant select list: PostgreSQL returned one row for MySQL's one per row.
        ("SELECT 'x' AS k FROM t HAVING k = 'x'", "SELECT 'x' AS k FROM t WHERE 'x' = 'x'"),
        (f"SELECT o.customer_id {_JOIN} HAVING customer_id > 11",
         "SELECT o.customer_id FROM orders AS o JOIN payments AS p ON p.order_id = o.id "
         "WHERE o.customer_id > 11"),
        ("SELECT * FROM (SELECT id, val * 2 AS d FROM t HAVING d < 70) x",
         "SELECT * FROM (SELECT id, val * 2 AS d FROM t WHERE val * 2 < 70) AS x"),
    ],
)
def test_a_having_without_group_by_or_aggregate_is_a_where(sql: str, expected: str) -> None:
    """PostgreSQL made it one group: 'column t.id must appear in the GROUP BY clause'."""
    converted, classification = _convert(sql)
    assert converted == expected
    assert classification == "AUTO"


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        ("SELECT COUNT(*) AS n FROM t HAVING n > 1", "HAVING COUNT(*) > 1"),
        ("SELECT 1 AS x FROM t HAVING COUNT(*) > 3", "HAVING COUNT(*) > 3"),
        # Aggregates sqlglot keeps anonymous, or the converter rewrites, are aggregates too.
        ("SELECT JSON_ARRAYAGG(id) AS j FROM t HAVING j IS NOT NULL", "HAVING NOT JSON_AGG(id) IS NULL"),
        ("SELECT GROUP_CONCAT(DISTINCT id ORDER BY id) AS ids FROM t HAVING ids <> ''",
         "HAVING ARRAY_TO_STRING("),
        # The inner SUM makes it one group (MySQL returns no row; as a WHERE, one).
        ("SELECT SUM(SUM(val)) OVER () AS s FROM t HAVING 1 = 0", "HAVING 1 = 0"),
        # An aggregate in a subquery over only the outer query's columns is the outer
        # query's: one group on both engines (as a WHERE, 'aggregate functions are not
        # allowed in WHERE').
        ("SELECT (SELECT COUNT(o.id)) AS c FROM orders o HAVING c > 1", "HAVING (SELECT COUNT(o.id)) > 1"),
        ("SELECT (SELECT MAX(o.total) FROM payments p LIMIT 1) AS m FROM orders o HAVING m > 10",
         "HAVING (SELECT MAX(o.total) FROM payments AS p LIMIT 1) > 10"),
    ],
)
def test_an_aggregated_having_without_group_by_stays_a_having(sql: str, having: str) -> None:
    converted, _classification = _convert(sql)
    tree = sqlglot.parse_one(converted, read="postgres")
    assert tree.args.get("where") is None and tree.args.get("having") is not None, converted
    assert having in converted, converted


def test_a_views_row_filter_having_is_a_where() -> None:
    ddl, view = _view(
        "select `shop`.`t`.`id` AS `id`,(`shop`.`t`.`val` * 2) AS `distance` from `shop`.`t` "
        "having (`distance` < 70) order by `distance`"
    )
    assert 'WHERE ( ( "shop"."t"."val" * 2 ) < 70 )' in ddl, ddl
    assert "HAVING" not in ddl
    assert view.auto_converted
    for body, having in (
        ("select count(0) AS `n` from `shop`.`t` having (`n` > 1)", "HAVING ( COUNT(0) > 1 )"),
        ("select (select count(`shop`.`t`.`id`)) AS `c` from `shop`.`t` having (`c` > 1)",
         'HAVING ( ( SELECT COUNT("shop"."t"."id") ) > 1 )'),
    ):
        ddl, _ = _view(body)
        assert having in ddl, ddl
