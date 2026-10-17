"""G115: the review of G106-G111 -- the Query Converter writes a row of defaults before an ON
CONFLICT clause, leaves SQL_CACHE as written (a name on MySQL 8), reports a NATURAL JOIN whose
labels may match a table's columns only by case and keeps the result-label note for the rest,
reads a qualified star as naming no column and a starred derived table by the columns it
stars, tells a shadowed alias from a case-only clash, leaves out optimizer hints (reporting
SET_VAR), gives an UPDATE ... LIMIT key shift its two steps, reads a JOIN without ON as MySQL
nests it, and strips modifiers in linear time; a view leaves out MySQL's stored SELECT
modifiers and reports its PARTITION selection. Each statement was checked on MySQL 8.4 and
PostgreSQL 17."""

from __future__ import annotations

import gc
import time

import pytest

from dsql_migrator.core.converter import SchemaConverter, table_type_index
from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef
from dsql_migrator.core.query_converter import QueryConverter, _with_ignore, _without_dml_modifiers


def _convert(sql: str, converter: "QueryConverter | None" = None):
    result = (converter or QueryConverter()).convert(sql)
    return result.converted_sql, [w.code for w in result.warnings], result


def _table(name: str, *columns: str) -> TableDef:
    return TableDef(
        name=f"shop.{name}",
        columns=[ColumnDef(name=c, mysql_type="int", nullable=c != "id") for c in columns],
        primary_key=["id"],
    )


# The Evaluation inventory of the tables the statements read (its names lower-cased, as
# table_type_index keeps them).
_TYPED = QueryConverter(
    column_types=tuple(
        table_type_index(
            [
                _table("customers", "id", "name"),
                _table("orders", "id", "customer_id", "total", "status", "created_at"),
                _table("payments", "id", "order_id", "customer_id", "amount"),
                _table("mc2", "id", "CustomerId", "Amount"),
            ]
        ).values()
    )
)


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # 'INSERT INTO dflt  ON CONFLICT DO UPDATE SET v = 'y'DEFAULT VALUES': sqlglot put its
        # DEFAULT VALUES after the conflict clause, glued to it ('syntax error at or near
        # "ON"' even with the conflict target added). With (id) added it inserts MySQL's row.
        ("INSERT INTO dflt () VALUES () ON DUPLICATE KEY UPDATE v = 'y'",
         "INSERT INTO dflt DEFAULT VALUES ON CONFLICT DO UPDATE SET v = 'y'"),
        ("INSERT INTO dflt VALUES () ON DUPLICATE KEY UPDATE v = VALUES(v)",
         "INSERT INTO dflt DEFAULT VALUES ON CONFLICT DO UPDATE SET v = EXCLUDED.v"),
    ],
)
def test_a_row_of_defaults_goes_before_on_conflict(sql: str, converted: str) -> None:
    out, codes, _ = _convert(sql)
    assert out == converted and codes == ["ON_DUPLICATE_KEY_UPDATE"]


def test_a_row_of_defaults_whose_alias_the_update_reads_is_left_as_written() -> None:
    """AS new was dropped while new.v stayed. MySQL: (5, 'xy') on the second run; PostgreSQL's
    INSERT INTO dflt DEFAULT VALUES ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v || 'y' too."""
    converted, codes, result = _convert(
        "INSERT INTO dflt () VALUES () AS new ON DUPLICATE KEY UPDATE v = CONCAT(new.v, 'y')"
    )
    assert codes == ["MYSQL_ONLY_DML", "ON_DUPLICATE_KEY_UPDATE"]
    assert "VALUES ()" in converted and "AS new" in converted
    assert "DEFAULT VALUES ON CONFLICT" in result.warnings[0].message
    assert "EXCLUDED.col for each new.col" in result.warnings[0].message
    # A row alias nothing reads is no reason to keep VALUES ().
    assert _convert("INSERT INTO dflt () VALUES () AS new")[:2] == ("INSERT INTO dflt DEFAULT VALUES", [])


@pytest.mark.parametrize(
    ("sql", "converted", "reported"),
    [
        # MySQL 8 reads SQL_CACHE as a name (8.0.3 dropped the modifier): modcols(sql_cache,
        # x) = (10, 40) gives 9, 10, 50, 0 and 1 there; each was AUTO with -1, 40, 40, a
        # syntax error ('SELECT IS AS NULL') or a PARSE_ERROR.
        ("SELECT sql_cache - 1 FROM modcols", "SELECT sql_cache - 1 FROM modcols", True),
        ("SELECT sql_cache x FROM modcols", "SELECT sql_cache AS x FROM modcols", True),
        ("SELECT sql_cache + x FROM modcols", "SELECT sql_cache + x FROM modcols", True),
        ("SELECT sql_cache IS NULL FROM modcols", "SELECT sql_cache IS NULL FROM modcols", True),
        ("SELECT id FROM modcols WHERE x IN (SELECT sql_cache * 4 FROM modcols)",
         "SELECT id FROM modcols WHERE x IN (SELECT sql_cache * 4 FROM modcols)", True),
        ("SELECT sql_cache + 1 AS x FROM (SELECT 5 AS sql_cache) d",
         "SELECT sql_cache + 1 AS x FROM (SELECT 5 AS sql_cache) AS d", True),
        # MySQL 5.7's modifier: kept, and fails on Test ('column "sql_cache" does not exist'),
        # as it does on MySQL 8 ('Unknown column SQL_CACHE').
        ("SELECT SQL_CACHE id FROM orders", "SELECT SQL_CACHE AS id FROM orders", True),
        # Before a comma it is a name on MySQL 5.7 too; SQL_NO_CACHE stays a modifier (MySQL
        # 8.4 reads SELECT sql_no_cache - 1 as -1).
        ("SELECT SQL_CACHE, id FROM orders", "SELECT SQL_CACHE, id FROM orders", False),
        ("SELECT sql_no_cache - 1 FROM modcols", "SELECT -1 FROM modcols", False),
    ],
)
def test_sql_cache_is_left_as_written_and_reported(sql: str, converted: str, reported: bool) -> None:
    out, codes, result = _convert(sql)
    assert out == converted and ("MYSQL_ONLY_SELECT" in codes) is reported
    if reported:
        note = next(w for w in result.warnings if w.code == "MYSQL_ONLY_SELECT").message
        assert "MySQL 5.7's query-cache modifier" in note and "MySQL 8.0" in note


@pytest.mark.parametrize(
    ("sql", "typed"),
    [
        # MySQL matches customerid to mc2's CustomerId (3 rows); Aurora DSQL finds no common
        # column and pairs every row (12). AUTO, with the inventory too.
        ("SELECT a.n FROM (SELECT customer_id AS customerid, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN mc2", False),
        ("SELECT a.n FROM (SELECT customer_id AS customerid, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN mc2", True),
        # ... and a * of the table (2 rows for 12), with the inventory reading it.
        ("SELECT a.n FROM (SELECT customer_id AS customerid, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN (SELECT * FROM mc2) b", True),
        # It was MIXED_CASE_NAME 'CustomerId ... finds nothing after cut over'.
        ("SELECT a.n FROM (SELECT customer_id AS CustomerId, COUNT(*) AS n FROM orders GROUP BY "
         "customer_id) a NATURAL JOIN mc2", False),
        # A label quoted is no spelling of the schema's (MySQL matches `customerid` alike):
        # without the inventory nothing tells which case mc2's column is in. It was AUTO.
        ("WITH recent AS (SELECT customer_id AS `CustomerId`, MAX(created_at) AS last_order FROM "
         "orders GROUP BY customer_id) SELECT r.last_order FROM recent r NATURAL JOIN mc2 WHERE "
         "`CustomerId` > 10", False),
    ],
)
def test_a_natural_join_a_label_may_match_only_by_case_is_reported(sql: str, typed: bool) -> None:
    _converted, codes, result = _convert(sql, _TYPED if typed else None)
    assert "NATURAL_JOIN_CASE" in codes and "MIXED_CASE_NAME" not in codes
    assert "write the join with ON" in next(
        w.message for w in result.warnings if w.code == "NATURAL_JOIN_CASE"
    )


def test_a_natural_join_label_no_table_holds_keeps_its_result_label_note() -> None:
    """customers has no ordercount: both engines join on id alone. It was MIXED_CASE_NAME
    'orderCount ... finds nothing after cut over'; the label does come back folded."""
    _converted, codes, result = _convert(
        "SELECT name, orderCount FROM customers NATURAL JOIN (SELECT customer_id AS id, COUNT(*) "
        "AS orderCount FROM orders GROUP BY customer_id) t",
        _TYPED,
    )
    assert "MIXED_CASE_LABEL" in codes and "MIXED_CASE_NAME" not in codes


@pytest.mark.parametrize(
    "sql",
    [
        # mc2 holds neither did nor score: the same rows on both engines (MIXED_CASE_NAME).
        "SELECT d.did FROM (SELECT id AS did, 5 AS Score FROM orders WHERE id <= 2) d NATURAL JOIN mc2 m2",
        # orders holds no score: Score is d's alone, on both engines (MIXED_CASE_NAME).
        "SELECT d.did FROM (SELECT id AS did, 5 AS Score FROM orders WHERE id <= 2) d JOIN "
        "customers c ON c.id = d.did + 9 NATURAL JOIN orders o WHERE Score >= 1",
    ],
)
def test_with_the_inventory_a_natural_join_no_label_can_join_is_auto(sql: str) -> None:
    assert _convert(sql, _TYPED)[1] == []


def test_the_strict_bare_name_rule_holds_only_for_a_join_of_the_source_holding_it() -> None:
    """The NATURAL JOIN of two tables cannot make d's Score one column with theirs (MySQL
    refuses an ambiguous Score): only its result label folds. It was MIXED_CASE_NAME."""
    codes = _convert(
        "SELECT Score FROM (SELECT 5 AS Score) d JOIN (customers c NATURAL JOIN orders o) ON 1 = 1"
    )[1]
    assert codes == ["MIXED_CASE_LABEL"]


@pytest.mark.parametrize(
    ("sql", "typed"),
    [
        # The re-sort idiom: t.* names no column, and both engines expand the inner t (ids
        # 5, 6, 7 on each). It was SHADOWED_ALIAS ('column t.* does not exist').
        ("SELECT * FROM (SELECT t.* FROM orders t ORDER BY t.id DESC LIMIT 3) t ORDER BY id", True),
        ("SELECT o.* FROM (SELECT o.* FROM orders o ORDER BY o.created_at DESC LIMIT 3) o ORDER BY o.id", True),
        ("SELECT o.id FROM orders o WHERE EXISTS (SELECT o.* FROM payments o WHERE o.order_id = 1)", True),
        ("SELECT t.id FROM (SELECT t.* FROM (SELECT id, total FROM orders) t) t", False),
    ],
)
def test_a_qualified_star_under_a_reused_alias_stays_auto(sql: str, typed: bool) -> None:
    assert _convert(sql, _TYPED if typed else None)[1] == []


@pytest.mark.parametrize(
    "sql",
    [
        # MySQL reads O.amount from the inner `O` (payments has it: 1, 2, 3, 5, 7); Aurora
        # DSQL folds O to o and reaches the outer o, which lacks it. It was SHADOWED_ALIAS,
        # telling the reverse.
        "SELECT o.id FROM (SELECT id FROM orders) o WHERE EXISTS (SELECT 1 FROM payments `O` "
        "WHERE `O`.order_id = o.id AND O.amount > 5)",
        # MySQL reads O.id from the outer O because `o` is another alias, not for a column
        # the inner one lacks.
        "SELECT O.id FROM orders O WHERE EXISTS (SELECT 1 FROM (SELECT order_id FROM payments) "
        "`o` WHERE `o`.order_id = O.id)",
    ],
)
def test_a_reference_the_engines_read_from_two_aliases_is_a_case_only_clash(sql: str) -> None:
    assert _convert(sql)[1] == ["CASE_ONLY_ALIASES"]


def test_a_shadowed_alias_quotes_the_error_as_postgresql_names_it() -> None:
    """PostgreSQL: 'column o.id does not exist' (the unquoted O folded); it was 'O.id'."""
    _converted, codes, result = _convert(
        "SELECT O.id FROM orders O WHERE EXISTS (SELECT 1 FROM (SELECT order_id FROM payments) "
        "O WHERE O.order_id = O.id)"
    )
    assert codes == ["SHADOWED_ALIAS"]
    assert result.warnings[0].message.startswith("O.id: ")
    assert "('column o.id does not exist')" in result.warnings[0].message


@pytest.mark.parametrize(
    "sql",
    [
        # payments has no created_at: MySQL reads o.created_at from the outer orders (7
        # rows), Aurora DSQL stops at the inner o ('column o.created_at does not exist').
        # AUTO with the inventory too.
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM (SELECT * FROM payments) o WHERE "
        "o.order_id = o.id AND o.created_at IS NOT NULL)",
        "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM (SELECT p.* FROM payments p) o "
        "WHERE o.order_id = o.id AND o.created_at IS NOT NULL)",
        "WITH w AS (SELECT * FROM payments) SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM "
        "w o WHERE o.order_id = o.id AND o.created_at IS NOT NULL)",
    ],
)
def test_a_starred_inner_alias_is_read_by_the_columns_it_stars(sql: str) -> None:
    assert _convert(sql, _TYPED)[1] == ["SHADOWED_ALIAS"]


def test_a_select_modifier_mysql_stores_in_a_view_is_left_out() -> None:
    """MySQL stores each in the view's body; written for PostgreSQL the CREATE VIEW failed
    ('syntax error at or near "o"'), graded converted."""
    for modifiers in ("straight_join high_priority", "sql_small_result", "sql_big_result",
                      "sql_buffer_result"):
        view = SchemaConverter().convert_view(ViewDef(
            name="shop.vm",
            definition=f"select {modifiers} `o`.`id` AS `id` from `shop`.`orders` `o`",
        ))
        assert view.target_ddl == (
            'CREATE VIEW "shop"."vm" AS\nSELECT\n  "o"."id" AS "id"\nFROM "shop"."orders" AS "o"'
        )
        assert view.auto_converted and view.warnings == []


def test_a_view_reports_sql_calc_found_rows_and_its_partition_selection() -> None:
    found_rows = SchemaConverter().convert_view(ViewDef(
        name="shop.vm",
        definition="select sql_calc_found_rows `o`.`id` AS `id` from `shop`.`orders` `o` limit 2",
    ))
    assert "SQL_CALC_FOUND_ROWS" not in found_rows.target_ddl
    assert [w.classification.value for w in found_rows.warnings] == ["MANUAL"]
    assert "FOUND_ROWS()" in found_rows.warnings[0].message
    # MySQL reads only p0's rows (2), left out it reads all three: left as written.
    partition = SchemaConverter().convert_view(ViewDef(
        name="shop.vm",
        definition="select `p`.`id` AS `id` from `shop`.`parts` PARTITION (`p0`) `p`",
    ))
    assert 'PARTITION("p0")' in partition.target_ddl
    assert "no partitions" in partition.warnings[0].message


@pytest.mark.parametrize(
    ("sql", "converted"),
    [
        # Each was UNTRANSLATED_CALL MAX_EXECUTION_TIME() / BKA() / SEMIJOIN(): the hint's
        # content read as a call, though the conversion leaves the hint out.
        ("SELECT /*+ MAX_EXECUTION_TIME(1000) */ id FROM orders", "SELECT id FROM orders"),
        ("DELETE /*+ BKA(o) */ LOW_PRIORITY FROM orders WHERE id = 1", "DELETE FROM orders WHERE id = 1"),
        ("SELECT id FROM orders WHERE id IN (SELECT /*+ SEMIJOIN(FIRSTMATCH) */ order_id FROM payments)",
         "SELECT id FROM orders WHERE id IN (SELECT order_id FROM payments)"),
    ],
)
def test_an_optimizer_hint_is_left_out(sql: str, converted: str) -> None:
    assert _convert(sql)[:2] == (converted, [])


def test_set_var_is_reported() -> None:
    """It was UNTRANSLATED_CALL SET_VAR() and MIXED_CASE_NAME 'OFF ... quote each'; the setting
    it made is not applied on Aurora DSQL, whose foreign key check then runs."""
    converted, codes, result = _convert(
        "INSERT /*+ SET_VAR(foreign_key_checks=OFF) */ INTO customers (id, name) VALUES (20, 'z')"
    )
    assert converted == "INSERT INTO customers (id, name) VALUES (20, 'z')"
    assert codes == ["OPTIMIZER_HINT"]
    assert "SET_VAR(foreign_key_checks = OFF)" in result.warnings[0].message
    assert "does not apply the setting" in result.warnings[0].message


def test_an_update_limit_that_shifts_a_unique_key_is_told_the_two_steps() -> None:
    """MySQL: 1, 2, 3, 5, 6. The advised UPDATE seq SET pos = pos + 1 WHERE id IN (SELECT id
    ... ORDER BY pos DESC LIMIT 2) fails on PostgreSQL ('Key (pos)=(5) already exists'); the
    two steps over those rows give MySQL's."""
    _converted, codes, result = _convert(
        "UPDATE seq SET pos = pos + 1 WHERE pos >= 3 ORDER BY pos DESC LIMIT 2"
    )
    message = result.warnings[0].message
    assert codes == ["MYSQL_ONLY_DML"] and "IN (SELECT <primary key>" in message
    assert "SET pos = -(pos + 1) WHERE <primary key> IN (...)" in message
    assert "SET pos = -pos WHERE pos < 0" in message
    # DELETE ... LIMIT shifts no key.
    delete = _convert("DELETE FROM seq WHERE pos >= 3 ORDER BY pos DESC LIMIT 2")[2]
    assert "two steps" not in delete.warnings[0].message


@pytest.mark.parametrize(
    "sql",
    [
        # MySQL nests a JOIN without ON left, so the ON sees d (rows 1, 2, 3 / 1-7); Aurora
        # DSQL: 'column "Flag" does not exist'. The ON read it as a comma's: AUTO.
        "SELECT d.did FROM (SELECT *, id AS did, 1 AS Flag FROM mc) d JOIN customers c JOIN mc m "
        "ON m.id = `Flag` WHERE c.id = 10",
        "SELECT d.did FROM (SELECT o.id AS did, 1 AS Flag FROM orders o) d JOIN payments i JOIN "
        "people p ON p.id = `Flag` WHERE i.id = 1",
    ],
)
def test_an_on_after_a_join_without_on_sees_what_mysql_nests_before_it(sql: str) -> None:
    converted, codes, _ = _convert(sql)
    assert codes == ["QUOTE_MISMATCH"] and " CROSS JOIN " in converted


def test_a_bare_name_an_on_reads_through_a_join_without_on_is_its_own() -> None:
    """MySQL and Aurora DSQL both read d's Flag (4 rows each). It was MIXED_CASE_NAME."""
    assert _convert(
        "SELECT d.did FROM (SELECT 1 AS did, 1 AS Flag) d JOIN customers c JOIN mc m ON m.id = Flag"
    )[1] == []


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


@pytest.mark.parametrize("step", ["strip", "put back"])
def test_modifiers_are_stripped_and_ignore_put_back_in_linear_time(step: str) -> None:
    """Each removed modifier rebuilt the whole input, and each IGNORE the whole output: 12,000
    UPDATE IGNORE statements took 43 and 62 times 600's (20,000: 1.7 s and 2.7 s)."""

    def inputs(n: int) -> tuple:
        if step == "strip":
            return ("; ".join(f"UPDATE IGNORE orders SET status = 'x' WHERE id = {i}" for i in range(n)),)
        return ("; ".join(f"UPDATE orders SET status = 'x' WHERE id = {i}" for i in range(n)), list(range(n)))

    function = _without_dml_modifiers if step == "strip" else _with_ignore
    small, large = inputs(600), inputs(12000)
    _timed(function, *small)  # warm up
    assert _timed(function, *large) < _timed(function, *small) * 33  # linear: it is about 20
