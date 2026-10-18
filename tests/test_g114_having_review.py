"""G114: the review of G106-G111 -- the shared HAVING inliner. Without the Evaluation
inventory a bare GROUP BY alias name is the alias when the alias reads another column of
its query (ONLY_FULL_GROUP_BY), else a guessed FROM column that fails loudly when wrong; a
parenthesised column and t.* beside a join are select-list columns; a view's subquery
HAVING reads an enclosing select-list alias as its value; and a statement without HAVING
reads nothing of the inventory. Each case was run on MySQL 8.4 and PostgreSQL 17 over the
same rows: a view as MySQL stores it and a Query Converter statement."""

from __future__ import annotations

import pytest
import sqlglot

from dsql_migrator.core.converter import SchemaConverter, inline_having_aliases, table_type_index
from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef
from dsql_migrator.core.query_converter import QueryConverter

_TABLES = {
    "orders": ("id", "customer_id", "total", "status", "qty", "amount", "created_at"),
    "payments": ("id", "order_id", "customer_id", "amount"),
    "customers": ("id", "name"),
    "statuses": ("code", "label"),
    "items": ("id", "order_id", "sku"),
}


def _inventory() -> tuple:
    """The Evaluation inventory's column types (``shop.<table>``): its tables, no views."""
    tables = [
        TableDef(
            name=f"shop.{name}",
            columns=[ColumnDef(name=column, mysql_type="int") for column in columns],
            primary_key=[columns[0]],
        )
        for name, columns in _TABLES.items()
    ]
    return tuple(table_type_index(tables).values())


def _convert(sql: str, column_types: tuple = ()):
    result = QueryConverter(column_types=column_types).convert(sql)
    return result.converted_sql, result


def _view(body: str):
    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body))
    return " ".join(view.target_ddl.split()), view


# --- having-fid-1: an alias that reads no column of its query is the FROM column ----------


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # orders has total: MySQL groups and filters by o.total (warning 1052), 3 rows;
        # inlined, Aurora DSQL tested 100 > 20 for every group (7).
        ("SELECT (SELECT MAX(p.amount) FROM payments p) AS total, COUNT(*) AS n FROM orders o "
         "GROUP BY total HAVING total > 20", "HAVING total > 20"),
        ("SELECT (SELECT MAX(p.amount) FROM payments p) AS qty, COUNT(*) AS n FROM orders o "
         "GROUP BY qty HAVING qty > 20", "HAVING qty > 20"),
        ("SELECT (SELECT COUNT(*) FROM payments p) AS qty, COUNT(*) AS n FROM orders o "
         "GROUP BY qty HAVING qty > 2", "HAVING qty > 2"),
        # A constant: MySQL [x 4]; inlined, [x 3, x 4].
        ("SELECT 'x' AS status, COUNT(*) AS n FROM orders o GROUP BY status "
         "HAVING status <> 'old'", "HAVING status <> 'old'"),
        # An aggregate the query computes (MySQL cannot GROUP BY it): the FROM column.
        ("SELECT (SELECT MAX(o.qty) FROM payments p LIMIT 1) AS total, COUNT(*) AS n "
         "FROM orders o GROUP BY total HAVING total > 20", "HAVING total > 20"),
    ],
)
def test_an_alias_reading_no_column_of_its_query_is_the_from_column(sql: str, having: str) -> None:
    converted, result = _convert(sql)
    assert converted.endswith(having), converted
    assert result.classification.value == "AUTO"


# --- having-fid-2 / having-reg-1: an alias that reads another column of its query --------


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # items has no amount: MySQL groups and filters by the alias ([1, 2]).
        ("SELECT o2.id FROM orders o2 WHERE EXISTS (SELECT (SELECT MAX(p.amount) FROM payments p "
         "WHERE p.order_id = i.order_id) AS amt FROM items i WHERE i.order_id = o2.id "
         "GROUP BY amt HAVING amt > 7)",
         "HAVING (SELECT MAX(p.amount) FROM payments AS p WHERE p.order_id = i.order_id) > 7)"),
        ("SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.order_id = i.order_id) AS amt, "
         "COUNT(*) AS n FROM items i GROUP BY amt HAVING amt > 10",
         "HAVING (SELECT MAX(p.amount) FROM payments AS p WHERE p.order_id = i.order_id) > 10"),
        # (An alias that also names a column of its name -- AS amount over p.amount -- may be a
        # key's name, which MySQL reads: kept the column and reported, tests/test_g120_having_
        # query_review.py.)
    ],
)
def test_an_alias_reading_another_column_of_its_query_is_the_group(sql: str, having: str) -> None:
    """Grouped by a FROM column of the alias's name, MySQL (ONLY_FULL_GROUP_BY) refuses an
    alias that reads another column of the query: it is grouping by the alias."""
    converted, result = _convert(sql)
    assert converted.endswith(having), converted
    assert result.classification.value == "AUTO"


def test_a_view_the_inventory_lacks_tells_nothing() -> None:
    """v_orders is a view (the inventory holds tables): MySQL [11, 13, 14] -- grouped by the
    alias, unless v_orders has name as a key o.customer_id depends on, which nothing tells
    (G120): kept the column, as 5d955cc had it, and both readings named."""
    converted, result = _convert(
        "SELECT c.id FROM customers c WHERE EXISTS (SELECT (SELECT c2.name FROM customers c2 "
        "WHERE c2.id = o.customer_id) AS name, COUNT(*) AS n FROM v_orders o "
        "WHERE o.id = c.id - 8 GROUP BY name HAVING name <> 'a')",
        _inventory(),
    )
    assert converted.endswith("GROUP BY name HAVING name <> 'a')"), converted
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]
    assert "else by the select-list alias name" in result.warnings[0].message


@pytest.mark.parametrize(
    ("sql", "tail"),
    [
        # A guess: right when orders has name, 'column o.name does not exist' when it has
        # not -- bare, Aurora DSQL's HAVING read the enclosing c.name ([11, 12, 13] for
        # MySQL's [10, 11, 12, 13]).
        ("SELECT c.id FROM customers c WHERE EXISTS (SELECT (SELECT MAX(c2.name) FROM customers "
         "c2) AS name FROM orders o WHERE o.customer_id = c.id GROUP BY name HAVING name > 'a')",
         "GROUP BY name HAVING o.name > 'a')"),
        ("SELECT c.id FROM customers c WHERE EXISTS (SELECT 'x' AS status FROM orders o "
         "WHERE o.customer_id = c.id GROUP BY status HAVING status <> 'old')",
         "GROUP BY status HAVING o.status <> 'old')"),
    ],
)
def test_a_guessed_from_column_in_a_subquery_is_written_with_its_one_source(
    sql: str, tail: str
) -> None:
    converted, result = _convert(sql)
    assert converted.endswith(tail), converted
    assert result.classification.value == "AUTO"
    # With the inventory it is no guess: orders has status, not name.
    converted, _ = _convert(sql, _inventory())
    assert "HAVING o." not in converted, converted


def test_a_guessed_from_column_beside_other_sources_is_manual() -> None:
    sql = ("SELECT c.id FROM customers c WHERE EXISTS (SELECT (SELECT MAX(c2.name) FROM "
           "customers c2) AS name FROM orders o JOIN payments p ON p.order_id = o.id "
           "WHERE o.customer_id = c.id GROUP BY name {having})")
    converted, result = _convert(sql.format(having="HAVING name > 'a'"))
    assert converted.endswith("GROUP BY name HAVING name > 'a')"), converted
    assert result.classification.value == "MANUAL"
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]
    message = result.warnings[0].message
    assert "column of o or p" in message and "select-list alias name" in message, message
    # A HAVING that does not read the name is as MySQL reads it.
    _, result = _convert(sql.format(having="HAVING COUNT(*) > 0"))
    assert result.classification.value == "AUTO"


def test_a_key_grouped_beside_the_alias_keeps_its_from_column() -> None:
    """o.id is orders' key: o.customer_id depends on it, so MySQL groups and filters by
    o.amount (5 rows) -- the alias reading o.customer_id tells nothing there."""
    converted, result = _convert(
        "SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.customer_id = o.customer_id) "
        "AS amount, COUNT(*) AS n FROM orders o GROUP BY amount, o.id HAVING amount > 10"
    )
    assert converted.endswith("GROUP BY amount, o.id HAVING amount > 10"), converted
    assert result.classification.value == "AUTO"


# --- having-fid-3 / having-reg-2: a parenthesised column, t.* beside other sources --------


@pytest.mark.parametrize(
    ("sql", "tail"),
    [
        # orders has no order_id: MySQL reads p.order_id ([3, 4, 6]).
        ("SELECT o.id FROM orders o WHERE o.id IN (SELECT (p.order_id) FROM payments p "
         "HAVING order_id > 2)", "FROM payments AS p WHERE p.order_id > 2)"),
        ("SELECT o.id FROM orders o WHERE o.id IN (SELECT (p.order_id) AS x FROM payments p "
         "HAVING order_id > 2)", "FROM payments AS p WHERE p.order_id > 2)"),
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT (p.amount) FROM payments p "
         "WHERE p.order_id = o.id HAVING amount > 12)", "WHERE p.order_id = o.id AND p.amount > 12)"),
    ],
)
def test_a_parenthesised_select_list_column_is_that_column(sql: str, tail: str) -> None:
    converted, result = _convert(sql)
    assert converted.endswith(tail), converted
    assert result.classification.value == "AUTO"


@pytest.mark.parametrize(
    ("sql", "tail"),
    [
        # p.* lists p.customer_id: MySQL reads it there ([]), not the enclosing o's.
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT p.*, 1 FROM payments p JOIN items i "
         "ON i.order_id = p.order_id WHERE p.order_id = o.id HAVING customer_id > 12)",
         "WHERE p.order_id = o.id AND p.customer_id > 12)"),
        # MySQL [2]: p.id through p.* (o.id would give []).
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT p.*, 1 FROM payments p JOIN items i "
         "ON i.order_id = p.order_id WHERE p.order_id = o.id HAVING id > 2)",
         "WHERE p.order_id = o.id AND p.id > 2)"),
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT p.* FROM payments p, items i "
         "WHERE i.order_id = p.order_id AND p.order_id = o.id HAVING customer_id > 12)",
         "AND p.customer_id > 12)"),
        ("SELECT o.id, l.amount FROM orders o, LATERAL (SELECT p.* FROM payments p JOIN items i "
         "ON i.order_id = p.order_id WHERE p.order_id = o.id HAVING amount > 12) l",
         "WHERE p.order_id = o.id AND p.amount > 12) AS l"),
    ],
)
def test_t_star_beside_a_join_lists_ts_columns(sql: str, tail: str) -> None:
    converted, result = _convert(sql, _inventory())
    assert converted.endswith(tail), converted
    assert result.classification.value == "AUTO"
    # Its columns untold, both readings are named -- MySQL's is p's when it has the column.
    converted, result = _convert(sql)
    assert "HAVING" in converted and result.classification.value == "MANUAL", converted
    message = result.warnings[0].message
    assert "through p.* if p has a column" in message and "else from the enclosing" in message


def test_a_derived_tables_outputs_tell_its_star() -> None:
    converted, result = _convert(
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT d.*, 1 FROM (SELECT p.order_id, "
        "p.customer_id FROM payments p) d JOIN items i ON i.order_id = d.order_id "
        "WHERE d.order_id = o.id HAVING customer_id > 12)"
    )
    assert converted.endswith("WHERE d.order_id = o.id AND d.customer_id > 12)"), converted
    assert result.classification.value == "AUTO"


# --- having-reg-3: a statement without HAVING or ANY_VALUE reads none of the inventory ----


class _Unread(tuple):
    """An inventory that fails when its tables are read."""

    def __iter__(self):
        raise AssertionError("the inventory was read")


def test_a_statement_without_having_reads_none_of_the_inventory() -> None:
    inventory = _Unread(_inventory())
    for sql in ("SELECT c1 FROM t1 WHERE c2 = 3", "SELECT c1, COUNT(*) FROM t1 GROUP BY c1",
                "UPDATE t1 SET c2 = 1 WHERE c0 = 5"):
        tree = sqlglot.parse_one(sql, read="mysql")
        assert inline_having_aliases(tree, stored=False, column_types=inventory) == []
    # A HAVING asks for the tables it reads, once it needs them.
    tree = sqlglot.parse_one(
        "SELECT 'x' AS status, COUNT(*) AS n FROM orders o GROUP BY status HAVING status <> 'old'",
        read="mysql",
    )
    inline_having_aliases(tree, stored=False, column_types=_inventory())
    assert tree.sql().endswith("HAVING status <> 'old'"), tree.sql()


# --- having-pre-1: a view's subquery HAVING that reads an enclosing alias ----------------


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        # MySQL (100 3, -10 NULL, NULL NULL, 300 3); kept, 'column "tt" does not exist'.
        ("select (`o`.`score` * 2) AS `tt`,(select count(0) from `shop`.`items` `i` "
         "having (`tt` > 30)) AS `c` from `shop`.`orders` `o`",
         '( SELECT COUNT(0) FROM "shop"."items" AS "i" HAVING ( ( "o"."score" * 2 ) > 30 ) ) AS "c"'),
        # A subquery's row filter: a WHERE.
        ("select `o`.`id` AS `id`,(`o`.`score` * 2) AS `tt`,(select `i`.`id` from `shop`.`items` "
         "`i` where (`i`.`order_id` = `o`.`id`) having (`tt` > 10) order by `i`.`id` limit 1) AS "
         "`f` from `shop`.`orders` `o`",
         'WHERE ( "i"."order_id" = "o"."id" ) AND ( ( "o"."score" * 2 ) > 10 )'),
        # In the enclosing query's HAVING, which becomes a WHERE itself.
        ("select `o`.`id` AS `id`,(`o`.`score` * 2) AS `tt` from `shop`.`orders` `o` having "
         "exists(select 1 from `shop`.`items` `i` having (`tt` > 30))",
         'FROM "shop"."orders" AS "o" WHERE EXISTS( SELECT 1 FROM "shop"."items" AS "i" '
         'WHERE ( ( "o"."score" * 2 ) > 30 ) )'),
        # MySQL reads no alias of a query that holds the subquery in its WHERE: o's tt (100
        # for scores 50 and 150, else NULL), not o2's (100 for every order).
        ("select `o`.`score` AS `tt`,(select (`o2`.`id` * 100) AS `tt` from `shop`.`orders` `o2` "
         "where ((`o2`.`id` = 1) and exists(select 1 from `shop`.`items` `i` having (`tt` > 30)))) "
         "AS `k` from `shop`.`orders` `o`",
         'EXISTS( SELECT 1 FROM "shop"."items" AS "i" WHERE ( "o"."score" > 30 ) )'),
    ],
)
def test_a_views_subquery_having_reads_an_enclosing_alias_as_its_value(
    body: str, expected: str
) -> None:
    ddl, view = _view(body)
    assert expected in ddl, ddl
    assert view.auto_converted and not view.warnings, [w.message for w in view.warnings]


@pytest.mark.parametrize(
    ("body", "name"),
    [
        # count(0) written in would count the subquery's rows.
        ("select count(0) AS `n`,(select count(0) from `shop`.`items` `i` having (`n` > 1)) AS `c` "
         "from `shop`.`orders` `o`", "n"),
        # o.score written in would read items o.
        ("select `o`.`score` AS `tt`,(select count(0) from `shop`.`items` `o` having (`tt` > 30)) "
         "AS `c` from `shop`.`orders` `o`", "tt"),
    ],
)
def test_an_enclosing_alias_that_cannot_be_written_in_is_reported(body: str, name: str) -> None:
    ddl, view = _view(body)
    assert f'HAVING ( "{name}" >' in ddl, ddl
    assert [w.classification.value for w in view.warnings] == ["MANUAL"]
    assert view.warnings[0].message.startswith(
        f"MySQL reads {name} in a subquery's HAVING as the enclosing query's select-list alias"
    )


def test_the_statement_form_of_an_enclosing_alias_stays_manual() -> None:
    """Written, the enclosing query's FROM may have the name (MySQL reads that first) and the
    alias's columns are unqualified: named, as before."""
    _, result = _convert(
        "SELECT o.total * 2 AS tt, (SELECT COUNT(*) FROM payments p HAVING tt > 30) AS c "
        "FROM orders o"
    )
    assert result.classification.value == "MANUAL"
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]
