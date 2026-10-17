"""G108: the review of G101-G105 -- the shared HAVING inliner reads a name where MySQL 8.4
does. A bare GROUP BY name in a statement is a FROM column first (MySQL's warning 1052),
told by the Evaluation inventory; a subquery's HAVING name outside its select list is the
enclosing query's; an aggregate over an unqualified column is computed where that column
is read; and ANY_VALUE is no aggregate. Each case was run on MySQL 8.4 and PostgreSQL 17
over the same rows: a view as MySQL stores it and a Query Converter statement."""

from __future__ import annotations

import pytest
import sqlglot

from dsql_migrator.core.converter import table_type_index
from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef
from dsql_migrator.core.query_converter import QueryConverter

_TABLES = {
    "orders": ("id", "customer_id", "total", "status", "qty", "amount"),
    "payments": ("id", "order_id", "customer_id", "amount"),
    "customers": ("id", "name"),
    "statuses": ("code", "label"),
    "items": ("id", "order_id", "sku"),
    "t": ("id", "col1", "col2", "grp", "val"),
}


def _inventory(**drop: str) -> tuple:
    """The Evaluation inventory's column types (``shop.<table>``), without the columns
    ``drop`` names per table."""
    tables = [
        TableDef(
            name=f"shop.{name}",
            columns=[
                ColumnDef(name=column, mysql_type="int")
                for column in columns
                if column != drop.get(name)
            ],
            primary_key=[columns[0]],
        )
        for name, columns in _TABLES.items()
    ]
    return tuple(table_type_index(tables).values())


def _convert(sql: str, column_types: tuple = ()):
    result = QueryConverter(column_types=column_types).convert(sql)
    return result.converted_sql, result


def _clauses(converted: str) -> "tuple[bool, bool]":
    """(has a WHERE, has a HAVING) of the outermost query."""
    tree = sqlglot.parse_one(converted, read="postgres")
    return tree.args.get("where") is not None, tree.args.get("having") is not None


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body))
    return " ".join(view.target_ddl.split()), view


# --- G98-2-R1 / R2: a bare GROUP BY name a FROM table has is that column ------------------


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # MySQL: GROUP BY status and HAVING status are the FROM column o.status ([Open 4]);
        # inlined, Aurora DSQL also returned the 'old' group.
        ("SELECT (SELECT s.label FROM statuses s WHERE s.code = o.status) AS status, "
         "COUNT(*) AS n FROM orders o GROUP BY status HAVING status <> 'old'",
         "HAVING status <> 'old'"),
        ("SELECT (SELECT s.label FROM statuses s WHERE s.code = status) AS status, "
         "COUNT(*) AS n FROM orders o GROUP BY status HAVING status <> 'old'",
         "HAVING status <> 'old'"),
        ("SELECT (SELECT COUNT(*) FROM payments p WHERE p.customer_id = o.customer_id) "
         "AS customer_id, COUNT(*) AS n FROM orders o GROUP BY customer_id "
         "HAVING customer_id > 1", "HAVING customer_id > 1"),
        # Inlined: 'operator does not exist: character varying > integer'.
        ("SELECT (SELECT c.name FROM customers c WHERE c.id = o.customer_id) AS customer_id, "
         "COUNT(*) AS n FROM orders o GROUP BY customer_id HAVING customer_id > 11",
         "HAVING customer_id > 11"),
        # orders has amount / qty too (R2): MySQL groups and filters by o.amount / o.qty.
        ("SELECT (SELECT MAX(p.amount) FROM payments p) AS amount, COUNT(*) AS n FROM orders o "
         "GROUP BY amount HAVING amount > 20", "HAVING amount > 20"),
        ("SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.amount < o.amount) AS amount, "
         "COUNT(*) AS n FROM orders o GROUP BY amount HAVING amount > 10", "HAVING amount > 10"),
        ("SELECT (SELECT COUNT(*) FROM payments p WHERE p.amount < o.qty * 10) AS qty, "
         "COUNT(*) AS n FROM orders o GROUP BY qty HAVING qty > 1", "HAVING qty > 1"),
    ],
)
@pytest.mark.parametrize("inventory", [False, True])
def test_a_group_by_name_a_from_table_has_is_that_column(
    sql: str, having: str, inventory: bool
) -> None:
    converted, result = _convert(sql, _inventory() if inventory else ())
    assert converted.endswith(having), converted
    assert result.classification.value == "AUTO"


def test_a_group_by_alias_no_from_table_has_is_inlined_by_the_inventory() -> None:
    """orders without amount: MySQL groups by the alias ([15, 20, 100])."""
    sql = ("SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.order_id = o.id) AS amount, "
           "COUNT(*) AS n FROM orders o GROUP BY amount HAVING amount > 10")
    converted, _ = _convert(sql, _inventory(orders="amount"))
    assert converted.endswith(
        "HAVING (SELECT MAX(p.amount) FROM payments AS p WHERE p.order_id = o.id) > 10"
    ), converted
    # Without it, the name stays the FROM column it most likely is: a wrong guess fails on
    # Aurora DSQL ('column "amount" does not exist') instead of grouping other rows.
    converted, _ = _convert(sql)
    assert converted.endswith("HAVING amount > 10"), converted


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # A derived table's or a CTE's outputs need no inventory.
        ("SELECT (SELECT MAX(p.amount) FROM payments p) AS amount, COUNT(*) AS n FROM "
         "(SELECT o.id, o.amount FROM orders o) d GROUP BY amount HAVING amount > 20",
         "HAVING amount > 20"),
        ("WITH d AS (SELECT o.id, o.qty FROM orders o) SELECT (SELECT COUNT(*) FROM payments p "
         "WHERE p.amount < d.qty * 10) AS qty, COUNT(*) AS n FROM d GROUP BY qty "
         "HAVING qty > 1", "HAVING qty > 1"),
        ("SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.order_id = d.id) AS amount, "
         "COUNT(*) AS n FROM (SELECT o.id FROM orders o) d GROUP BY amount HAVING amount > 10",
         "HAVING (SELECT MAX(p.amount) FROM payments AS p WHERE p.order_id = d.id) > 10"),
    ],
)
def test_a_derived_tables_outputs_tell_the_group_by_name(sql: str, having: str) -> None:
    converted, _ = _convert(sql)
    assert converted.endswith(having), converted


# --- R1: a subquery's HAVING name outside its select list is the enclosing query's --------


@pytest.mark.parametrize(
    "sql",
    [
        # MySQL reads c.id ([a, b, c, d]); as a WHERE, Aurora DSQL read o.id ([b, c, d]).
        "SELECT c.name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o "
        "WHERE o.customer_id = c.id HAVING id > 2)",
        "SELECT c.id FROM customers c WHERE c.id IN (SELECT p.customer_id FROM payments p "
        "HAVING id > 12)",
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM payments p HAVING customer_id > 12)",
        # With GROUP BY, Aurora DSQL read p.customer_id (functionally dependent on p.id).
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT p.id FROM payments p GROUP BY p.id "
        "HAVING customer_id > 12)",
    ],
)
@pytest.mark.parametrize("inventory", [False, True])
def test_an_enclosing_querys_name_in_a_subquerys_having_is_manual(
    sql: str, inventory: bool
) -> None:
    converted, result = _convert(sql, _inventory() if inventory else ())
    assert "HAVING" in converted and converted.count("WHERE") == sql.count("WHERE"), converted
    assert result.classification.value == "MANUAL"
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]
    assert "from the enclosing query" in result.warnings[0].message


def test_an_enclosing_name_the_subquery_lacks_is_read_there_by_both_engines() -> None:
    """customers' name is in no source of the subquery: Aurora DSQL's WHERE reads it from the
    enclosing query as MySQL's HAVING does ([12, 13, 14]); kept a HAVING, the subquery was
    one group even without orders ([12, 13, 14, 15])."""
    sql = ("SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o "
           "WHERE o.customer_id = c.id HAVING name > 'a')")
    converted, result = _convert(sql, _inventory())
    assert converted.endswith("WHERE o.customer_id = c.id AND name > 'a')"), converted
    assert result.classification.value == "AUTO"
    # Unknown without the inventory: said, not guessed.
    converted, result = _convert(sql)
    assert converted.endswith("HAVING name > 'a')") and result.classification.value == "MANUAL"
    # A derived table's outputs tell it alone.
    converted, result = _convert(
        "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM (SELECT o.customer_id "
        "FROM orders o) d WHERE d.customer_id = c.id HAVING name > 'a')"
    )
    assert converted.endswith("WHERE d.customer_id = c.id AND name > 'a')"), converted
    assert result.classification.value == "AUTO"


@pytest.mark.parametrize(
    ("sql", "tail"),
    [
        # SELECT * lists the subquery's own columns: MySQL reads p.customer_id ([3, 6]).
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT * FROM payments p "
         "WHERE p.order_id = o.id HAVING customer_id > 12)",
         "WHERE p.order_id = o.id AND customer_id > 12)"),
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT p.* FROM payments p "
         "WHERE p.order_id = o.id HAVING customer_id > 12)",
         "WHERE p.order_id = o.id AND customer_id > 12)"),
        # A select-list column of the enclosing query is written as that column.
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT o.customer_id FROM payments p "
         "HAVING customer_id > 12)", "FROM payments AS p WHERE o.customer_id > 12)"),
    ],
)
def test_a_subquerys_having_name_its_select_list_holds_is_a_where(sql: str, tail: str) -> None:
    converted, result = _convert(sql)
    assert converted.endswith(tail), converted
    assert result.classification.value == "AUTO"


# --- G98-6-R1 / R3: an aggregate is computed where its columns are read -------------------


@pytest.mark.parametrize(
    ("sql", "inventory"),
    [
        # A FROM-less subquery has no columns: COUNT(total) is the outer query's ([7]); as a
        # WHERE, 'aggregate functions are not allowed in WHERE'.
        ("SELECT (SELECT COUNT(total)) AS c FROM orders HAVING c > 1", False),
        ("SELECT (SELECT COUNT(id)) AS c FROM orders HAVING c > 1", False),
        ("SELECT 1 AS one FROM orders HAVING (SELECT COUNT(total)) > 3", False),
        # payments / customers have no total: MySQL reads o.total ([100.00], [60.00]).
        ("SELECT (SELECT MAX(total) FROM payments p LIMIT 1) AS m FROM orders o "
         "HAVING m > 10", True),
        ("SELECT (SELECT MAX(total) FROM customers c WHERE c.id = 11) AS m FROM orders o "
         "HAVING m > 10", True),
        # A derived table's outputs need no inventory.
        ("SELECT (SELECT MAX(total) FROM (SELECT p.id FROM payments p) d LIMIT 1) AS m "
         "FROM orders o HAVING m > 10", False),
        # A column whose query cannot be told keeps the HAVING.
        ("SELECT (SELECT COUNT(x.v) FROM payments p) AS c FROM orders o HAVING c > 1", False),
    ],
)
def test_an_aggregate_over_the_outer_querys_unqualified_column_keeps_the_having(
    sql: str, inventory: bool
) -> None:
    converted, _ = _convert(sql, _inventory() if inventory else ())
    assert _clauses(converted) == (False, True), converted


@pytest.mark.parametrize("inventory", [False, True])
def test_a_subquerys_own_aggregate_stays_a_row_filter(inventory: bool) -> None:
    """MySQL reads amount from payments, the subquery's own table: a row filter ([2, 3, 4, 6])."""
    converted, result = _convert(
        "SELECT o.id, (SELECT SUM(amount) FROM payments WHERE order_id = o.id) AS paid "
        "FROM orders o HAVING paid > 10",
        _inventory() if inventory else (),
    )
    assert _clauses(converted) == (True, False), converted
    assert result.classification.value == "AUTO"


def test_an_aggregate_in_a_from_less_subquery_of_a_subquery_is_that_subquerys() -> None:
    """COUNT(amount) is the payments subquery's: one group per order ([2])."""
    converted, _ = _convert(
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM payments p "
        "WHERE p.order_id = o.id HAVING (SELECT COUNT(amount)) > 1)"
    )
    assert converted.endswith("HAVING (SELECT COUNT(amount)) > 1)"), converted


# --- R4: ANY_VALUE is no aggregate in MySQL ------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        # MySQL [4, 5]; PostgreSQL's any_value made it one group ([]).
        ("SELECT ANY_VALUE(id) AS a FROM t HAVING a > 3", "SELECT id AS a FROM t WHERE id > 3"),
        # MySQL returns each row; as an aggregate, one.
        ("SELECT ANY_VALUE(id) FROM t", "SELECT id FROM t"),
        ("SELECT (SELECT ANY_VALUE(o.id)) AS a FROM orders o",
         "SELECT (SELECT o.id) AS a FROM orders AS o"),
        ("SELECT DISTINCT ANY_VALUE(grp) AS g FROM t", "SELECT DISTINCT grp AS g FROM t"),
        # MySQL reads a name inside ANY_VALUE in HAVING as a HAVING name: the alias.
        ("SELECT id AS k FROM t HAVING ANY_VALUE(k) > 3", "SELECT id AS k FROM t WHERE id > 3"),
        ("SELECT ANY_VALUE(col1 + col2) * 2 AS x FROM t", "SELECT (col1 + col2) * 2 AS x FROM t"),
    ],
)
def test_any_value_where_nothing_groups_is_its_argument(sql: str, expected: str) -> None:
    converted, result = _convert(sql)
    assert converted == expected
    assert result.classification.value == "AUTO"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ANY_VALUE(grp) AS g, COUNT(*) AS n FROM t",
        "SELECT ANY_VALUE(id) AS a FROM t HAVING COUNT(*) > 1",
        "SELECT grp, ANY_VALUE(grp * 10) AS v FROM t GROUP BY grp HAVING grp > 1",
        "SELECT o.id, (SELECT ANY_VALUE(p.order_id) FROM payments p WHERE p.order_id = o.id "
        "GROUP BY p.order_id) AS x FROM orders o",
    ],
)
def test_any_value_beside_an_aggregate_or_a_group_by_stays(sql: str) -> None:
    converted, _ = _convert(sql)
    assert "ANY_VALUE(" in converted, converted


def test_a_views_any_value_where_nothing_groups_is_its_argument() -> None:
    ddl, view = _view(
        "select any_value(`o`.`id`) AS `a` from `shop`.`orders` `o` having (`a` > 2)"
    )
    assert 'SELECT "o"."id" AS "a" FROM "shop"."orders" AS "o" WHERE ( "o"."id" > 2 )' in ddl, ddl
    assert view.auto_converted
    ddl, _ = _view("select (select any_value(`o`.`id`)) AS `a` from `shop`.`orders` `o`")
    assert "ANY_VALUE" not in ddl, ddl


# --- An inlined operator keeps its precedence ----------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        # MySQL [6, 8, 10]; written bare, col1 + col2 * 2 > 10 returned [8, 10].
        ("SELECT col1 + col2 AS s FROM t HAVING s * 2 > 10",
         "SELECT col1 + col2 AS s FROM t WHERE (col1 + col2) * 2 > 10"),
        ("SELECT grp, SUM(val) + 1 AS s FROM t GROUP BY grp HAVING s * 2 > 10",
         "SELECT grp, SUM(val) + 1 AS s FROM t GROUP BY grp HAVING (SUM(val) + 1) * 2 > 10"),
        ("SELECT col1 - col2 AS s FROM t HAVING 10 - s > 7",
         "SELECT col1 - col2 AS s FROM t WHERE 10 - (col1 - col2) > 7"),
        # AND / OR are functions to sqlglot as well: MySQL [5]; bare, [4, 5].
        ("SELECT id, val > 30 OR grp = 1 AS f FROM t HAVING f AND id > 4",
         "SELECT id, val > 30 OR grp = 1 AS f FROM t WHERE (val > 30 OR grp = 1) AND id > 4"),
        # Arithmetic under a comparison, and EXISTS, read the same bare.
        ("SELECT id, val * 2 AS d FROM t HAVING d < 70",
         "SELECT id, val * 2 AS d FROM t WHERE val * 2 < 70"),
        ("SELECT o.id, EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id) AS e "
         "FROM orders o HAVING e AND o.id > 2",
         "SELECT o.id, EXISTS(SELECT 1 FROM payments AS p WHERE p.order_id = o.id) AS e "
         "FROM orders AS o WHERE EXISTS(SELECT 1 FROM payments AS p WHERE p.order_id = o.id) "
         "AND o.id > 2"),
    ],
)
def test_an_inlined_alias_keeps_its_precedence(sql: str, expected: str) -> None:
    converted, _ = _convert(sql)
    assert converted == expected
