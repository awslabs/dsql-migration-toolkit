"""G120: the final regression round on G112-G117 -- the HAVING inliner and the Query Converter.
Without the Evaluation inventory a GROUP BY alias that names a column of its own name and reads
another column of its query, an alias of an enclosing query's column in a subquery, and a guess in
a top-level CTE or derived table are written as 5d955cc wrote them, and reported where MySQL may
read the name either way; t.* beside another starred source names both; the alias analysis and a
long UNION's typing are linear. SQL_CACHE where MySQL 8 cannot read a column is MySQL 5.7's
modifier, a NATURAL JOIN label is checked against the column the inventory spells, and an UPDATE
SET value that reads an earlier assignment's column is reported. Each statement was run on MySQL
8.4 and PostgreSQL 17 over the same rows."""

from __future__ import annotations

import gc
import time

import pytest
import sqlglot

from dsql_migrator.core.converter import inline_having_aliases, table_type_index
from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.query_converter import QueryConverter, table_column_names

_TABLES = [
    TableDef(
        name=f"shop.{name}",
        columns=[ColumnDef(name=column, mysql_type="int") for column in columns],
        primary_key=[columns[0]],
    )
    for name, columns in {
        "customers": ("id", "name"),
        "orders": ("id", "customer_id", "total", "status", "created_at"),
        "items": ("id", "order_id", "sku"),
        "mc2": ("id", "CustomerId", "Amount"),
        "people": ("id", "CustomerId"),
    }.items()
]
_TYPES = tuple(table_type_index(_TABLES).values())


def _convert(sql: str, converter: "QueryConverter | None" = None):
    result = (converter or QueryConverter()).convert(sql)
    return result.converted_sql, [w.code for w in result.warnings], result


def _timed(function, *arguments) -> float:
    best = float("inf")
    gc.collect()
    gc.disable()  # a collection costs by the whole suite's heap, not by this input
    try:
        for _ in range(3):
            started = time.perf_counter()
            function(*arguments)
            best = min(best, time.perf_counter() - started)
    finally:
        gc.enable()
    return best


# --- having-rh-1 / having-fid-fd-key: an alias named like its table's key ------------------


@pytest.mark.parametrize(
    ("sql", "tail", "nested"),
    [
        # orders.id is the key: o.customer_id depends on it, so MySQL groups and filters by
        # o.id (warning 1052): 4 rows. Inlined (G114), Aurora DSQL tested the lookup: other rows.
        ("SELECT (SELECT MAX(p.id) FROM payments p WHERE p.customer_id = o.customer_id) AS id, "
         "COUNT(*) AS n FROM orders o GROUP BY id HAVING id > 3", "GROUP BY id HAVING id > 3", False),
        ("SELECT c.id FROM customers c WHERE EXISTS (SELECT (SELECT MAX(p.id) FROM payments p "
         "WHERE p.customer_id = o.customer_id) AS id FROM orders o WHERE o.customer_id = c.id "
         "GROUP BY id HAVING id > 3)", "GROUP BY id HAVING id > 3)", True),
        ("SELECT (SELECT c.id FROM customers c WHERE c.id = o.customer_id) AS id, COUNT(*) AS n "
         "FROM orders o GROUP BY id HAVING id > 3", "GROUP BY id HAVING id > 3", False),
        ("SELECT c0.id FROM customers c0 WHERE EXISTS (SELECT (SELECT c.id FROM customers c "
         "WHERE c.id = o.customer_id) AS id FROM orders o WHERE o.customer_id = c0.id GROUP BY id "
         "HAVING id > 3)", "GROUP BY id HAVING id > 3)", True),
        # items' key: MySQL 4 rows, inlined none.
        ("SELECT (SELECT c2.name FROM customers c2 WHERE c2.id = i.order_id) AS id, COUNT(*) AS k "
         "FROM items i GROUP BY id HAVING id IS NOT NULL", "GROUP BY id HAVING NOT id IS NULL", False),
        # Moved from test_g114: items has no amount, orders no label or name, so MySQL groups by
        # the alias ([1, 2]; []; [11, 13, 14]) -- which nothing tells without the inventory.
        ("SELECT o2.id FROM orders o2 WHERE EXISTS (SELECT (SELECT MAX(p.amount) FROM payments p "
         "WHERE p.order_id = i.order_id) AS amount FROM items i WHERE i.order_id = o2.id "
         "GROUP BY amount HAVING amount > 7)", "GROUP BY amount HAVING amount > 7)", True),
        ("SELECT s0.code, s0.label FROM statuses s0 WHERE EXISTS (SELECT (SELECT s.label "
         "FROM statuses s WHERE s.code = o.status) AS label FROM orders o WHERE o.customer_id = 12 "
         "GROUP BY label HAVING label = 'Paid')", "GROUP BY label HAVING label = 'Paid')", True),
        ("SELECT c.id FROM customers c WHERE EXISTS (SELECT (SELECT c2.name FROM customers c2 "
         "WHERE c2.id = o.customer_id) AS name, COUNT(*) AS n FROM orders o WHERE o.id = c.id - 8 "
         "GROUP BY name HAVING name <> 'a')", "GROUP BY name HAVING name <> 'a')", True),
        ("SELECT (SELECT MAX(p.amount) FROM payments p WHERE p.order_id = i.order_id) AS amount, "
         "COUNT(*) AS n FROM items i GROUP BY amount HAVING amount > 10",
         "GROUP BY amount HAVING amount > 10", False),
    ],
)
def test_an_alias_named_like_a_key_keeps_the_column_and_names_both_readings(
    sql: str, tail: str, nested: bool
) -> None:
    converted, codes, result = _convert(sql)
    assert converted.endswith(tail), converted
    assert codes == ["MYSQL_SEMANTICS"]
    message = result.warnings[0].message
    assert "when it is a key the alias's own columns depend on" in message
    assert "else by the select-list alias" in message
    assert ("from the enclosing query" in message) is nested, message
    # With the inventory it is told: AUTO.
    _converted, codes, _ = _convert(sql, QueryConverter(column_types=_TYPES))
    assert codes == []


# --- having-rh-2: an alias of an enclosing query's column in a grouped subquery ----------


@pytest.mark.parametrize(
    "sql",
    [
        # items has no status: MySQL reads the alias, o.status ([1, 4]); bare, Aurora DSQL reads
        # the enclosing o.status too. Qualified (G114): 'column i.status does not exist'.
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT o.status AS status FROM items i WHERE "
        "i.order_id = o.id GROUP BY status HAVING status = 'new')",
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT o.status AS status, COUNT(*) AS n FROM "
        "items i WHERE i.order_id = o.id GROUP BY status HAVING status = 'new')",
        "SELECT o.id, (SELECT o.status AS status FROM items i WHERE i.order_id = o.id GROUP BY "
        "status HAVING status <> 'paid' LIMIT 1) AS s FROM orders o",
        "SELECT o.id FROM orders o WHERE o.id IN (SELECT i.order_id FROM items i WHERE EXISTS "
        "(SELECT o.status AS status FROM payments p WHERE p.order_id = i.order_id GROUP BY status "
        "HAVING status = 'new'))",
        # Beside two sources it was MANUAL.
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT o.status AS status FROM items i JOIN "
        "payments p ON p.order_id = i.order_id WHERE i.order_id = o.id GROUP BY status HAVING "
        "status = 'new')",
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT (o.status) AS status FROM items i WHERE "
        "i.order_id = o.id GROUP BY status HAVING status = 'new')",
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT status AS status FROM items i WHERE "
        "i.order_id = o.id GROUP BY status HAVING status = 'new')",
    ],
)
def test_an_alias_that_is_an_enclosing_column_of_its_name_stays_bare(sql: str) -> None:
    """Bare, the name reads the subquery's own column of the name if it has one, as MySQL
    does, else that same enclosing column -- the alias's value."""
    converted, codes, _ = _convert(sql)
    assert codes == [] and "HAVING status" in converted, converted
    assert "HAVING i.status" not in converted and "HAVING p.status" not in converted


@pytest.mark.parametrize(
    ("sql", "having", "reading"),
    [
        # An expression of it: 5d955cc's bare name, MySQL's reading untold (items' status, or
        # COALESCE(o.status, 'none')).
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT COALESCE(status, 'none') AS status FROM "
         "items i WHERE i.order_id = o.id GROUP BY status HAVING status = 'new')",
         "HAVING status = 'new')", "it is left as written"),
        # An alias of another name: 5d955cc's inline (MySQL [2, 4, 5]); qualified (G114), 'column
        # i.dbl does not exist'.
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT o.total * 2 AS dbl FROM items i WHERE "
         "i.order_id = o.id GROUP BY dbl HAVING dbl > 30)",
         "HAVING o.total * 2 > 30)", "its expression is written in the HAVING"),
    ],
)
def test_an_alias_of_an_enclosing_expression_is_written_as_5d955cc_and_reported(
    sql: str, having: str, reading: str
) -> None:
    converted, codes, result = _convert(sql)
    assert converted.endswith(having), converted
    assert codes == ["MYSQL_SEMANTICS"] and reading in result.warnings[0].message


# --- having-rh-3: a top-level CTE or derived table has no enclosing query ----------------


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # MySQL groups and filters by o.total: 4 rows, as the bare name reads on Aurora DSQL.
        # It was MANUAL, saying Aurora DSQL reads the name from an enclosing query.
        ("WITH x AS (SELECT (SELECT MAX(p2.total) FROM orders p2) AS total, COUNT(*) AS n FROM "
         "orders o JOIN payments p ON p.order_id = o.id GROUP BY total HAVING total > 10) SELECT * "
         "FROM x", "GROUP BY total HAVING total > 10) SELECT * FROM x"),
        ("SELECT * FROM (SELECT (SELECT MAX(p2.total) FROM orders p2) AS total, COUNT(*) AS n FROM "
         "orders o JOIN payments p ON p.order_id = o.id GROUP BY total HAVING total > 10) d",
         "GROUP BY total HAVING total > 10) AS d"),
        # A guess no source has fails loudly, as at the top level ('column "zz" does not exist').
        ("SELECT * FROM (SELECT 'x' AS zz, COUNT(*) AS n FROM orders o JOIN payments p ON "
         "p.order_id = o.id GROUP BY zz HAVING zz <> 'old') d", "GROUP BY zz HAVING zz <> 'old') AS d"),
        ("WITH x AS (SELECT 'x' AS zz, COUNT(*) AS n FROM orders o GROUP BY zz HAVING zz <> 'old') "
         "SELECT * FROM x", "GROUP BY zz HAVING zz <> 'old') SELECT * FROM x"),
    ],
)
def test_a_top_level_cte_or_derived_table_guesses_as_the_top_level(sql: str, having: str) -> None:
    converted, codes, _ = _convert(sql)
    assert codes == [] and converted.endswith(having), converted


@pytest.mark.parametrize(
    ("sql", "having"),
    [
        # A derived table in a subquery and a LATERAL can read an enclosing query: the guess is
        # written with the one source, as in any subquery.
        ("SELECT c.id FROM customers c WHERE EXISTS (SELECT * FROM (SELECT 'x' AS zz, COUNT(*) AS "
         "n FROM orders o WHERE o.customer_id = c.id GROUP BY zz HAVING zz <> 'old') d)",
         "HAVING o.zz <> 'old'"),
        ("SELECT o.id, l.n FROM orders o, LATERAL (SELECT 'x' AS status, COUNT(*) AS n FROM "
         "payments p WHERE p.order_id = o.id GROUP BY status HAVING status <> 'old') l",
         "HAVING p.status <> 'old'"),
    ],
)
def test_a_derived_table_that_reads_an_enclosing_query_is_a_subquery(sql: str, having: str) -> None:
    converted, codes, _ = _convert(sql)
    assert codes == [] and having in converted, converted


# --- having-rh-4: t.* beside another starred source names every one ----------------------


@pytest.mark.parametrize(("stars", "named"), [("p.*, i.*", "p.* or i.*"), ("i.*, p.*", "i.* or p.*")])
def test_every_starred_source_is_named(stars: str, named: str) -> None:
    """Only items has sku: MySQL reads i.sku through i.* ([1, 2, 4]). The note named p.* alone,
    and both rewrites it suggested were wrong."""
    sql = (f"SELECT o.id FROM orders o WHERE EXISTS (SELECT {stars} FROM payments p JOIN items i "
           "ON i.order_id = p.order_id WHERE p.order_id = o.id HAVING sku = 'x')")
    _converted, codes, result = _convert(sql)
    assert codes == ["MYSQL_SEMANTICS"]
    message = result.warnings[0].message
    assert f"through {named}, whichever has a column sku" in message, message
    assert "p.sku" in message and "i.sku" in message
    # The inventory tells it: i.sku, AUTO.
    converted, codes, _ = _convert(sql, QueryConverter(column_types=_TYPES))
    assert codes == [] and "i.sku = 'x'" in converted, converted


# --- having-rh-5: the alias analysis is linear --------------------------------------------


@pytest.mark.parametrize(
    "alias",
    [
        # One aggregate over n columns in a nested query: each column walked up through it and
        # its aggregate re-read per column -- quadratic, and cubic for a long sum.
        "(SELECT MAX(GREATEST({columns})) FROM p LIMIT 1) AS s",
        "(SELECT MAX({sum}) FROM p LIMIT 1) AS s",
    ],
)
def test_the_alias_analysis_is_linear(alias: str) -> None:
    def statement(n: int) -> str:
        columns = ", ".join(f"o.c{i}" for i in range(n))
        total = " + ".join(f"o.c{i}" for i in range(n))
        return (f"SELECT {alias.format(columns=columns, sum=total)}, COUNT(*) AS k FROM t o "
                "GROUP BY s HAVING s > 0")

    def inline(sql: str) -> None:
        inline_having_aliases(sqlglot.parse_one(sql, read="mysql"), stored=False)

    small, large = statement(100), statement(800)
    _timed(inline, small)  # warm up
    assert _timed(inline, large) < _timed(inline, small) * 24  # linear: about 8


def test_many_group_by_aliases_are_read_in_linear_time() -> None:
    """The GROUP BY's other columns were listed again per alias (3,000 aliases: 5.7 s)."""

    def statement(n: int) -> str:
        aliases = ", ".join(f"o.c{i} + 1 AS a{i}" for i in range(n))
        return (f"SELECT {aliases}, COUNT(*) AS k FROM t o GROUP BY "
                f"{', '.join(f'a{i}' for i in range(n))} HAVING a0 > 0")

    def inline(sql: str) -> None:
        inline_having_aliases(sqlglot.parse_one(sql, read="mysql"), stored=False)

    small, large = statement(300), statement(2400)
    _timed(inline, small)
    assert _timed(inline, large) < _timed(inline, small) * 24  # linear: about 8


# --- G112V-Q-4: a long UNION is typed in linear time ----------------------------------------


def test_a_long_union_is_converted_in_linear_time() -> None:
    """Each table was looked up for a CTE by walking to the root, which a UNION nests its
    branches as deep as they are many: 5,000 branches took 9.5 s (bcb86bd 1.5 s)."""

    def union(n: int) -> str:
        return " UNION ALL ".join(f"SELECT id FROM orders WHERE id = {i}" for i in range(n))

    converter = QueryConverter()
    small, large = union(150), union(2400)
    _timed(converter.convert, small)
    assert _timed(converter.convert, large) < _timed(converter.convert, small) * 32  # about 16


# --- G112V-Q-1 / G112R-Q-1: SQL_CACHE where MySQL 8 reads no column ----------------------


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # MySQL 8.4 refuses each (ERROR 1064): MySQL 5.7's, where SQL_CACHE is its query-cache
        # modifier. Kept, sqlglot could not parse them (PARSE_ERROR, no SQL_CACHE note).
        ("SELECT SQL_CACHE * FROM orders", "SELECT * FROM orders"),
        ("SELECT SQL_CACHE o.id FROM orders o", "SELECT o.id FROM orders AS o"),
        ("select sql_cache count(*) as n from orders o", "SELECT COUNT(*) AS n FROM orders AS o"),
        ("SELECT SQL_CACHE DISTINCT id FROM orders", "SELECT DISTINCT id FROM orders"),
        ("SELECT SQL_CACHE HIGH_PRIORITY id FROM orders", "SELECT id FROM orders"),
        ("SELECT SQL_CACHE 1", "SELECT 1"),
        ("SELECT SQL_CACHE 'a' AS s", "SELECT 'a' AS s"),
        ("INSERT INTO t2 (a, b) SELECT SQL_CACHE id + 100, 1 FROM orders",
         "INSERT INTO t2 (a, b) SELECT id + 100, 1 FROM orders"),
        # The other statements of the input are converted with it.
        ("SELECT 1; SELECT SQL_CACHE * FROM orders", "SELECT 1; SELECT * FROM orders"),
    ],
)
def test_sql_cache_where_mysql_8_reads_no_column_is_mysql_57s_modifier(sql: str, converted: str) -> None:
    out, codes, result = _convert(sql)
    assert out == converted and codes == ["MYSQL_ONLY_SELECT"]
    message = result.warnings[0].message
    assert "MySQL 5.7's query-cache modifier here" in message and "left out" in message


def test_sql_cache_before_sql_calc_found_rows_keeps_the_found_rows_note() -> None:
    """MySQL 5.7's pagination idiom: both are left out, and FOUND_ROWS() is told its form."""
    out, codes, result = _convert("SELECT SQL_CACHE SQL_CALC_FOUND_ROWS * FROM orders LIMIT 10")
    assert out == "SELECT * FROM orders LIMIT 10" and codes == ["MYSQL_ONLY_SELECT"] * 2
    assert "COUNT(*)" in result.warnings[0].message


def test_sql_cache_mysql_8_reads_as_a_column_is_still_left_as_written() -> None:
    out, codes, result = _convert("SELECT sql_cache - 1 FROM modcols")
    assert out == "SELECT sql_cache - 1 FROM modcols" and codes == ["MYSQL_ONLY_SELECT"]
    assert "so it is left as written" in result.warnings[0].message
    # A statement that does not parse for another reason is still a PARSE_ERROR.
    assert _convert("SELECT SQL_CACHE * FROM")[1] == ["PARSE_ERROR"]


# --- G112V-Q-2 / G112R-Q-2: NATURAL JOIN labels against the inventory's spellings ----------


_SPELLED = QueryConverter(column_types=_TYPES, column_names=table_column_names(_TABLES))


@pytest.mark.parametrize(
    "sql",
    [
        # The common shape: customers' id is spelled id. Same rows on both engines; it was
        # MANUAL NATURAL_JOIN_CASE (5d955cc: AUTO).
        "SELECT c.name, t.n FROM customers c NATURAL JOIN (SELECT customer_id AS id, COUNT(*) AS n "
        "FROM orders GROUP BY customer_id) t",
        "SELECT d.n FROM (SELECT o.id AS id, o.total AS n FROM orders o) d NATURAL JOIN items i",
        # A label quoted as the schema spells the column (G109's control case): it matches.
        "SELECT a.n FROM (SELECT customer_id AS `CustomerId`, COUNT(*) AS n FROM orders GROUP BY "
        "customer_id) a NATURAL JOIN mc2",
        "WITH recent AS (SELECT customer_id AS `CustomerId`, MAX(created_at) AS last_order FROM "
        "orders GROUP BY customer_id) SELECT r.last_order FROM recent r NATURAL JOIN mc2 WHERE "
        "`CustomerId` > 10",
    ],
)
def test_a_natural_join_label_spelled_as_the_schema_is_auto(sql: str) -> None:
    assert _convert(sql, _SPELLED)[1] == []


@pytest.mark.parametrize(
    ("sql", "converter"),
    [
        # customerid / CustomerId beside mc2's CustomerId: MySQL 3 rows, Aurora DSQL 12.
        ("SELECT a.n FROM (SELECT customer_id AS customerid, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN mc2", _SPELLED),
        ("SELECT a.n FROM (SELECT customer_id AS CustomerId, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN mc2", _SPELLED),
        ("SELECT d.n FROM (SELECT o.id AS `Id`, o.total AS n FROM orders o) d NATURAL JOIN items i",
         _SPELLED),
        # A table the inventory lacks, a * over one, or no inventory: the label may match only by
        # case -- reported, as before.
        ("SELECT d.n FROM (SELECT o.id AS id, o.total AS n FROM orders o) d NATURAL JOIN other_t u",
         _SPELLED),
        ("SELECT a.n FROM (SELECT customer_id AS customerid, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN (SELECT * FROM mc2) b", _SPELLED),
        ("SELECT d.n FROM (SELECT o.id AS id, o.total AS n FROM orders o) d NATURAL JOIN items i",
         QueryConverter()),
    ],
)
def test_a_natural_join_label_matching_only_by_case_is_reported(sql: str, converter) -> None:
    _converted, codes, result = _convert(sql, converter)
    assert codes == ["NATURAL_JOIN_CASE"]
    message = result.warnings[0].message
    # The way out names the quoting: an unquoted CustomerId folds, and still does not match.
    assert "write the join with ON" in message
    assert 'quoted (AS "CustomerId"; unquoted, CustomerId folds to customerid)' in message


# --- F112-UPDATE-LTR: a SET value reading an earlier assignment's column -------------------


@pytest.mark.parametrize(
    ("sql", "shown"),
    [
        # MySQL adds the new paid (0): qty 3 for id 1; Aurora DSQL the old one (1): 4.
        ("UPDATE orders o SET o.paid = 0, o.qty = o.qty + o.paid WHERE o.id = 1", "qty = ... paid"),
        # MySQL 1|2|2, Aurora DSQL 1|2|1.
        ("UPDATE fx_lr SET a = a + 1, b = a", "b = ... a"),
        # A correlated subquery reads the new a on MySQL too (b 2 for a row whose a was 1).
        ("UPDATE fx_lr SET a = a + 1, b = (SELECT MAX(i.id) FROM items i WHERE i.id = fx_lr.a)",
         "b = ... a"),
    ],
)
def test_a_set_value_reading_an_earlier_assignment_is_reported(sql: str, shown: str) -> None:
    converted, codes, result = _convert(sql)
    assert codes == ["MYSQL_SEMANTICS"] and converted is not None
    message = result.warnings[0].message
    assert message.startswith(f"SET {shown}: ") and "left to right" in message, message


@pytest.mark.parametrize(
    "sql",
    [
        # Read before it is assigned: the old value on both engines.
        "UPDATE fx_lr SET b = a, a = a + 1",
        "UPDATE orders SET qty = qty + 1, score = score + 1 WHERE id = 1",
        # A subquery's own column of the name.
        "UPDATE fx_lr SET a = a + 1, b = (SELECT MAX(a) FROM other_t)",
    ],
)
def test_a_set_value_reading_no_earlier_assignment_is_auto(sql: str) -> None:
    assert _convert(sql)[1] == []
