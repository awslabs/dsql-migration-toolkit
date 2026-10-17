"""G101: the review of G97-G100 -- MySQL expressions bound and typed as MySQL binds them (a
db-qualified column, a CTE, UPDATE / DELETE, tables that differ only in case), the kinds of
more expressions, MySQL's exact division and double-precision ROUND / TRUNCATE, interval
amounts and date strings, joins with no ON, STR_TO_DATE in the session's time zone, and the
calls the rewrite writes itself told apart from the source's. Each form was run on MySQL 8.4
and PostgreSQL 17 over the same rows (scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import re

import pytest

from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="i", mysql_type="int"),
        ColumnDef(name="n", mysql_type="int"),
        ColumnDef(name="f", mysql_type="float"),
        ColumnDef(name="db", mysql_type="double"),
        ColumnDef(name="dec4", mysql_type="decimal(10,4)"),
        ColumnDef(name="dec2", mysql_type="decimal(10,2)"),
        ColumnDef(name="j", mysql_type="json"),
        ColumnDef(name="bl", mysql_type="blob"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="c", mysql_type="char(8)"),
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="dt", mysql_type="datetime"),
        ColumnDef(name="tm", mysql_type="time"),
    ],
    primary_key=["id"],
)
_ORDERS = TableDef(
    name="shop.orders",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="payload", mysql_type="varchar(40)"),
        ColumnDef(name="body", mysql_type="blob"),
        ColumnDef(name="d", mysql_type="date"),
    ],
    primary_key=["id"],
)
_TABLES = [_T, _ORDERS]


def _norm(sql: str) -> str:
    """SQL with its whitespace collapsed and none just inside a parenthesis (where the
    pretty printer puts it depends on the line's width)."""
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str, tables=_TABLES):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=tables)
    return _norm(view.target_ddl), view


def _select(select_list: str, *, where: str = ""):
    return _view(f"select {select_list} from `shop`.`t` `t`" + (f" where {where}" if where else ""))


def _query(sql: str, *, typed: bool = True, tables=_TABLES):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    types = tuple(table_type_index(tables).values()) if typed else ()
    result = QueryConverter(column_types=types).convert(sql)
    return _norm(result.converted_sql or ""), result


# --- Binding and typing ------------------------------------------------------------------


_A = TableDef(
    name="ra.orders",
    columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name="payload", mysql_type="varchar(40)"),
             ColumnDef(name="d", mysql_type="date")],
)
_B = TableDef(
    name="rb.orders",
    columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name="payload", mysql_type="blob"),
             ColumnDef(name="d", mysql_type="datetime")],
)


def test_a_db_qualified_column_is_typed_by_the_table_of_that_database() -> None:
    """G97P-1 / F5: rb.orders.payload (a BLOB) was typed as ra.orders.payload, the first
    table of that bare name -- hex instead of text, a DATETIME cut to a DATE."""
    ddl, _ = _view(
        "select cast(`rb`.`orders`.`payload` as char charset utf8mb4) AS `p`,"
        "(`rb`.`orders`.`d` + interval 1 day) AS `nd`,length(`ra`.`orders`.`payload`) AS `l` "
        "from (`ra`.`orders` join `rb`.`orders` on((`ra`.`orders`.`id` = `rb`.`orders`.`id`)))",
        tables=[_A, _B],
    )
    assert "CONVERT_FROM(\"rb\".\"orders\".\"payload\", 'UTF8')" in ddl
    assert "(\"rb\".\"orders\".\"d\" + INTERVAL '1 DAY') AS \"nd\"" in ddl and "AS DATE" not in ddl
    assert 'OCTET_LENGTH("ra"."orders"."payload")' in ddl


def test_an_inner_alias_of_another_databases_table_leaves_the_outer_reference() -> None:
    """G97P-9: MySQL binds ra.orders.id to the outer table (the inner alias is rb's); the
    db was dropped and PostgreSQL bound it to the inner alias -- counts of 0."""
    ddl, _ = _view(
        "select `ra`.`orders`.`id` AS `id`,(select count(0) from `rb`.`items` `orders` "
        "where (`orders`.`oid` = `ra`.`orders`.`id`)) AS `n` from `ra`.`orders`",
        tables=[_A, _B],
    )
    assert '"orders"."oid" = "ra"."orders"."id"' in ddl


def test_an_alias_of_a_view_still_loses_its_database() -> None:
    ddl, _ = _view("select `shop`.`items`.`id` AS `id` from `shop`.`v_base` `items`")
    assert '"items"."id"' in ddl and '"shop"."items"' not in ddl


def test_tables_that_differ_only_in_case_are_typed_apart() -> None:
    """G97P-12: `Docs` and `docs` shared one entry, the last one listed."""
    upper = TableDef(name="shop.Docs", columns=[ColumnDef(name="body", mysql_type="blob")])
    lower = TableDef(name="shop.docs", columns=[ColumnDef(name="body", mysql_type="varchar(20)")])
    for tables in ([upper, lower], [lower, upper]):
        ddl, _ = _view("select convert(`x`.`body` using utf8mb4) AS `b` from `shop`.`Docs` `x`", tables=tables)
        assert "CONVERT_FROM(" in ddl
        ddl, _ = _view("select convert(`x`.`body` using utf8mb4) AS `b` from `shop`.`docs` `x`", tables=tables)
        assert "CONVERT_FROM(" not in ddl


def test_a_name_in_another_case_finds_the_one_table_it_can_mean() -> None:
    from dsql_migrator.core.converter import table_type_index, view_column_types

    index = table_type_index([_ORDERS])
    # G107 (ES-23): a stored view keeps MySQL's case -- `ORDERS` is another table.
    assert view_column_types("select 1 from `shop`.`ORDERS`", index) == ()
    sql, _ = _query("SELECT CONVERT(body USING utf8mb4) FROM Orders")
    assert "CONVERT_FROM(body, 'UTF8')" in sql


def test_a_cte_named_like_an_inventory_table_is_not_that_table() -> None:
    """G97P-3: the CTE's columns took the table's types: CONVERT_FROM over its text, a
    DATETIME cut to a DATE."""
    sql, _ = _query(
        "WITH orders AS (SELECT id, CONVERT(body USING utf8mb4) AS body, payload AS d FROM orders) "
        "SELECT CAST(body AS CHAR), d + INTERVAL 1 DAY FROM orders"
    )
    # Inside its own body the name is the real table; after it, the CTE.
    assert "CAST(CONVERT_FROM(body, 'UTF8') AS TEXT) AS body" in sql
    assert "SELECT CAST(body AS TEXT), d + INTERVAL '1 DAY' FROM orders" in sql


def test_a_recursive_cte_is_its_own_name_in_its_body() -> None:
    sql, _ = _query(
        "WITH RECURSIVE orders AS (SELECT 1 AS id, 'x' AS body UNION ALL SELECT id + 1, body FROM orders "
        "WHERE id < 3) SELECT LENGTH(body) FROM orders"
    )
    assert "OCTET_LENGTH(body)" in sql and "CONVERT_FROM" not in sql


@pytest.mark.parametrize(
    ("sql", "emitted"),
    [
        ("DELETE FROM orders WHERE CAST(body AS CHAR) = 'hello'", "CAST(CONVERT_FROM(body, 'UTF8') AS TEXT) = 'hello'"),
        ("UPDATE orders SET payload = CONVERT(body USING utf8mb4) WHERE id = 2",
         "SET payload = CAST(CONVERT_FROM(body, 'UTF8') AS TEXT)"),
        ("UPDATE t SET dec2 = TRUNCATE(f, 2)", "(TRUNC(CAST(f AS DOUBLE PRECISION) * 100) / 100)"),
        ("INSERT INTO orders (id, body) VALUES (1, 'x') ON DUPLICATE KEY UPDATE payload = CONVERT(VALUES(body) USING utf8mb4)",
         "CAST(CONVERT_FROM(EXCLUDED.body, 'UTF8') AS TEXT)"),
    ],
)
def test_update_delete_and_upsert_are_typed_like_a_select(sql: str, emitted: str) -> None:
    """G97P-4: only a SELECT was typed -- the DELETE matched nothing, the UPDATE wrote hex."""
    converted, _result = _query(sql)
    assert emitted in converted, converted


# --- Kinds, LENGTH and BINARY of an unknown or json value -----------------------------


@pytest.mark.parametrize(
    ("call", "emitted"),
    [
        ("length(`t`.`j`)", 'OCTET_LENGTH(CAST("t"."j" AS TEXT))'),
        ("length(json_extract(`t`.`j`,'$.a'))", "OCTET_LENGTH(CAST(JSON_EXTRACT_PATH(\"t\".\"j\", 'a') AS TEXT))"),
        ("length(json_array(`t`.`i`))", 'OCTET_LENGTH(CAST(JSON_BUILD_ARRAY("t"."i") AS TEXT))'),
        ("length(count(0))", "OCTET_LENGTH(CAST(COUNT(0) AS TEXT))"),
        ("length(max(`t`.`id`))", 'OCTET_LENGTH(CAST(MAX("t"."id") AS TEXT))'),
        ("length(round(`t`.`dec2`,1))", 'OCTET_LENGTH(CAST(ROUND("t"."dec2", 1) AS TEXT))'),
        ("md5(cast(sum(`t`.`i`) as char charset binary))", "MD5(CONVERT_TO(CAST(SUM(\"t\".\"i\") AS TEXT), 'UTF8'))"),
    ],
)
def test_length_and_binary_of_a_json_or_computed_value_read_its_text(call: str, emitted: str) -> None:
    """G97P-2 / F2 / F3 / XS-1: OCTET_LENGTH(json) and CAST(bigint AS BYTEA) do not exist."""
    ddl, _ = _select(f"{call} AS `x`")
    assert emitted in ddl, ddl


def test_an_unknown_expression_is_made_text_and_a_bare_column_keeps_its_own_bytes() -> None:
    sql, _ = _query("SELECT LENGTH(i), LENGTH(i + 1), CAST(i AS BINARY), CAST(i + 1 AS BINARY) FROM x", typed=False)
    # G107: the text of any type (noted), where OCTET_LENGTH(integer) does not exist.
    assert "OCTET_LENGTH(i || ''), OCTET_LENGTH(CAST(i + 1 AS TEXT))" in sql
    assert "CAST(i AS BYTEA), CONVERT_TO(CAST(i + 1 AS TEXT), 'UTF8')" in sql


def test_binary_ordering_and_grouping_keys_are_the_value_itself() -> None:
    """F3: ORDER BY CAST(name AS BYTEA) failed on a backslash in the text."""
    sql, _ = _query("SELECT s FROM x ORDER BY BINARY s", typed=False)
    # G107 (R4): of unknown type, by the bytes of its text -- a number's digits too.
    assert "ORDER BY CONVERT_TO(CAST(s AS TEXT), 'UTF8')" in sql and "BYTEA" not in sql
    ddl, _ = _view("select count(0) AS `n` from `shop`.`t` `t` group by cast(`t`.`s` as char charset binary)")
    assert 'GROUP BY "t"."s"' in ddl
    # A number keeps the bytes of its text, as MySQL orders them.
    ddl, _ = _view("select `t`.`id` AS `id` from `shop`.`t` `t` order by cast(`t`.`i` as char charset binary)")
    assert "ORDER BY CONVERT_TO(CAST(\"t\".\"i\" AS TEXT), 'UTF8')" in ddl


def test_a_binary_n_compared_with_text_is_dropped_and_noted() -> None:
    """F4: CONVERT_TO kept for BINARY(n) compared bytea = varchar, which has no operator."""
    ddl, view = _select("`t`.`id` AS `id`", where="(`t`.`s` = cast('31/12/99' as char(8) charset binary))")
    assert "\"t\".\"s\" = '31/12/99'" in ddl and "CONVERT_TO" not in ddl
    assert any("BINARY(n)" in w.message for w in view.warnings)


@pytest.mark.parametrize(
    "call",
    ["json_array(now(2))", "json_array(utc_timestamp(6))", "json_array(max(`t`.`dt`))", "json_array(now(5))"],
)
def test_json_of_a_rewritten_clock_or_an_aggregate_is_mysqls_text(call: str) -> None:
    """G97P-10 / F8: the truncated clock, CURRENT_TIMESTAMP AT TIME ZONE 'UTC' and MAX() had
    no kind, so JSON got PostgreSQL's ISO 'T' form."""
    ddl, _ = _select(f"{call} AS `x`")
    assert "'YYYY-MM-DD HH24:MI:SS.US'" in ddl, ddl


def test_json_of_max_of_a_time_has_its_microseconds() -> None:
    ddl, _ = _select("json_object('tm',max(`t`.`tm`)) AS `x`")
    assert "'HH24:MI:SS.US'" in ddl


def test_a_date_beside_a_date_string_is_a_date() -> None:
    """G97P-11: COALESCE(d, '2024-06-01') + INTERVAL 1 DAY was a timestamp."""
    ddl, _ = _select(
        "(coalesce(`t`.`d`,'2024-06-01') + interval 1 day) AS `a`,"
        "(case when (`t`.`i` > 0) then `t`.`d` else '2024-06-01' end + interval 1 month) AS `b`"
    )
    assert ddl.count("AS DATE)") == 2, ddl


def test_json_of_a_datetime_beside_a_string_is_mysqls_string() -> None:
    ddl, _ = _select("json_array(coalesce(`t`.`dt`,'2024-06-01 00:00:00')) AS `x`")
    # G107 (P12): each value's own text -- the DATETIME's, the string as written.
    assert "COALESCE(CAST(\"t\".\"dt\" AS TEXT), '2024-06-01 00:00:00')" in ddl and ".US" not in ddl


# --- Numbers ----------------------------------------------------------------------------


# G106 (ES-8): MySQL's quotient is its 9 kept digits, truncated -- DIV(x * 10^9, y) * 10^-9,
# where this was the numeric division (20 digits, or 16 significant).
_QUOTIENT = '(DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001)'


def test_mysqls_exact_division_is_numeric_at_its_scale() -> None:
    """G97P-7: 10 / 3 was the double 3.3333333333333335 (MySQL 3.3333), TRUNCATE(a / b, 6)
    3.333333, LENGTH 18. (The view has an AVG, so MySQL materializes it: its outputs are
    shown at their scale -- G106.)"""
    ddl, _ = _select(
        "(`t`.`i` / 3) AS `a`,(`t`.`dec4` / 3) AS `b`,((`t`.`i` / `t`.`n`) * 100) AS `c`,"
        "truncate((`t`.`i` / 3),6) AS `e`,length((`t`.`i` / 3)) AS `g`,concat((`t`.`i` / 3),'%') AS `h`,"
        "avg(`t`.`i`) AS `k`,(`t`.`db` / 2) AS `m`"
    )
    assert f'ROUND(CAST({_QUOTIENT} AS DECIMAL), 4) AS "a"' in ddl
    assert 'ROUND((DIV("t"."dec4" * 1000000000, NULLIF(3, 0)) * 0.000000001), 8) AS "b"' in ddl
    # MySQL computes with the digits it does not show: 2 / 3 * 100 is 66.6667, not 66.67.
    assert 'ROUND(((DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF("t"."n", 0)) * 0.000000001) * 100), 4) AS "c"' in ddl
    assert f'TRUNC({_QUOTIENT}, 4) AS "e"' in ddl
    assert f'OCTET_LENGTH(CAST(ROUND(CAST({_QUOTIENT} AS DECIMAL), 4) AS TEXT)) AS "g"' in ddl
    assert f"CAST(ROUND(CAST({_QUOTIENT} AS DECIMAL), 4) AS TEXT) || '%'" in ddl  # G107: as text
    assert 'ROUND(TRUNC(AVG("t"."i"), 9), 4) AS "k"' in ddl  # G106: AVG keeps 9 digits too
    assert '(CAST("t"."db" AS DOUBLE PRECISION) / NULLIF(2, 0)) AS "m"' in ddl  # a double's is a double


def test_a_quotient_is_compared_and_inlined_with_every_digit() -> None:
    """MySQL's 10 / 3 = 3.3333 is false, and HAVING reads an alias's own value."""
    ddl, _ = _select("`t`.`id` AS `id`", where="((`t`.`i` / 3) = 3.3333)")
    assert f"WHERE ({_QUOTIENT} = 3.3333)" in ddl
    ddl, _ = _view(
        "select `t`.`n` AS `n`,(sum(`t`.`i`) / 3) AS `r` from `shop`.`t` `t` group by `t`.`n` having (`r` > 1.6666)"
    )
    assert 'HAVING ((DIV(CAST(SUM("t"."i") AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001) > 1.6666)' in ddl


@pytest.mark.parametrize(
    ("call", "emitted"),
    [
        ("round(`t`.`db`,2)", '(ROUND(CAST("t"."db" AS DOUBLE PRECISION) * 100) / 100)'),
        ("round(`t`.`f`,2)", '(ROUND(CAST("t"."f" AS DOUBLE PRECISION) * 100) / 100)'),
        ("round((`t`.`db` * `t`.`i`),1)", '(ROUND(CAST(("t"."db" * "t"."i") AS DOUBLE PRECISION) * 10) / 10)'),
        ("round(`t`.`db`,-(1))", '(ROUND(CAST("t"."db" AS DOUBLE PRECISION) / 10) * 10)'),
        ("round(0.125e0,2)", "(ROUND(CAST(0.125e0 AS DOUBLE PRECISION) * 100) / 100)"),
        # G106 (P3): MySQL sums a FLOAT in double precision -- the cast goes inside the SUM.
        ("truncate(sum(`t`.`f`),2)", '(TRUNC(CAST(SUM(CAST("t"."f" AS DOUBLE PRECISION)) AS DOUBLE PRECISION) * 100) / 100)'),
        ("truncate(coalesce(`t`.`f`,0),2)", '(TRUNC(CAST(COALESCE("t"."f", 0) AS DOUBLE PRECISION) * 100) / 100)'),
        ("truncate((`t`.`f` + `t`.`f`),2)", '(TRUNC(CAST(("t"."f" + "t"."f") AS DOUBLE PRECISION) * 100) / 100)'),
        ("truncate(abs(`t`.`f`),2)", '(TRUNC(CAST(ABS("t"."f") AS DOUBLE PRECISION) * 100) / 100)'),
        ("round(`t`.`i`,2)", 'SELECT "t"."i" AS "x"'),
        ("round(`t`.`dec2`,4)", 'ROUND("t"."dec2", 2)'),
    ],
)
def test_round_and_truncate_compute_as_mysql_does(call: str, emitted: str) -> None:
    """G97P-5 / G97P-6 / F7 / XS-2: round(double precision, integer) does not exist, and a
    REAL made numeric directly keeps 6 digits. MySQL rounds a FLOAT / DOUBLE in double
    precision, half to even on the binary product (ROUND(0.125e0, 2) is 0.12); an integer
    stays itself, and a decimal keeps its scale (ROUND(3.33, 4) is 3.33)."""
    ddl, _ = _select(f"{call} AS `x`")
    assert emitted in ddl, ddl


def test_round_of_an_unknown_value_is_in_numeric() -> None:
    sql, _ = _query("SELECT ROUND(x, 2), ROUND(paid / total * 100, 2) FROM o", typed=False)
    assert "ROUND(CAST(x AS DECIMAL), 2)" in sql
    # G106 (P1): a division of operands not typed is exact in MySQL as like as not, so it is
    # rounded in numeric, half away from zero (in double precision 1 / 8 rounded to 0.12).
    assert "ROUND(CAST(CAST(paid AS DOUBLE PRECISION) / NULLIF(total, 0) * 100 AS DECIMAL), 2)" in sql


@pytest.mark.parametrize(
    ("call", "rounded"),
    [
        ("cast(-0.4 as unsigned)", "ROUND(CAST(-0.4 AS DECIMAL))"),
        ("cast(2.5e0 as unsigned)", "CAST(ROUND(CAST(2.5e0 AS DOUBLE PRECISION)) AS DECIMAL)"),
        ("cast(`t`.`db` as unsigned)", 'CAST(ROUND(CAST("t"."db" AS DOUBLE PRECISION)) AS DECIMAL)'),
    ],
)
def test_unsigned_rounds_before_it_wraps(call: str, rounded: str) -> None:
    """F9: CAST(-0.4 AS UNSIGNED) is 0 in MySQL (the test read the value unrounded, which
    gave 2^64), and a DOUBLE rounds half to even (2.5e0 is 2)."""
    ddl, _ = _select(f"{call} AS `x`")
    assert f"CASE WHEN {rounded} < 0 THEN {rounded} + 18446744073709551616 ELSE {rounded} END" in ddl, ddl


# --- Dates ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("call", "emitted"),
    [
        ("(`t`.`d` + interval -(1) day)", "CAST(\"t\".\"d\" + INTERVAL '-1 DAY' AS DATE)"),
        ("(`t`.`d` + interval `t`.`n` day)", "CAST(\"t\".\"d\" + \"t\".\"n\" * INTERVAL '1 DAY' AS DATE)"),
        ("(`t`.`d` + interval (`t`.`i` + 1) day)", "CAST(\"t\".\"d\" + (\"t\".\"i\" + 1) * INTERVAL '1 DAY' AS DATE)"),
        ("(`t`.`d` + interval `t`.`dec2` day)", "CAST(\"t\".\"d\" + ROUND(\"t\".\"dec2\") * INTERVAL '1 DAY' AS DATE)"),
        ("(`t`.`d` + interval 1.5 day)", "CAST(\"t\".\"d\" + INTERVAL '2 DAY' AS DATE)"),
        ("(`t`.`d` + interval -2.5 day)", "CAST(\"t\".\"d\" + INTERVAL '-3 DAY' AS DATE)"),
        ("(`t`.`dt` + interval 2.5e0 hour)", "\"t\".\"dt\" + INTERVAL '2 HOUR'"),
        ("(`t`.`d` + interval '1.5' day)", "CAST(\"t\".\"d\" + INTERVAL '1 DAY' AS DATE)"),
        ("(`t`.`dt` + interval 1.5 second)", "\"t\".\"dt\" + INTERVAL '1.5 SECOND'"),
        ("(`t`.`dt` + interval `t`.`f` minute)", "\"t\".\"dt\" + ROUND(\"t\".\"f\") * INTERVAL '1 MINUTE'"),
    ],
)
def test_interval_amounts_are_written_as_mysql_counts_them(call: str, emitted: str) -> None:
    """F10: interval -(1) day (as MySQL stores a negative amount) became INTERVAL DAY, a column
    amount INTERVAL 'n DAY' -- both PostgreSQL syntax errors -- and MySQL counts a fraction
    in whole units (half away from zero, a DOUBLE half to even; a string by its integer)."""
    ddl, _ = _select(f"{call} AS `x`")
    assert emitted in ddl, ddl


def test_date_strings_are_cast_in_chained_arithmetic_and_with_two_digit_years() -> None:
    """F6: the outer node's copy held the inner literal uncast; '24-01-31' is out of range."""
    ddl, _ = _view(
        "select (('2024-01-01' + interval 1 month) - interval 1 day) AS `a`,('24-01-31' + interval 1 day) AS `b`,"
        "('70-01-31' + interval 1 day) AS `c`"
    )
    assert "CAST((CAST(CAST('2024-01-01' AS DATE) + INTERVAL '1 MONTH' AS DATE)) - INTERVAL '1 DAY' AS DATE)" in ddl
    assert "CAST('2024-01-31' AS DATE)" in ddl and "CAST('1970-01-31' AS DATE)" in ddl


def test_str_to_date_is_read_in_the_sessions_time_zone() -> None:
    """F1: the fixed-offset parse (v || ' +00', fmt || ' TZH') read input left over after the
    format as the zone: '2024-01-02 03:04:05' with '%Y-%m-%d %H:%i' came back 5 hours early."""
    sql, _result = _query("SELECT * FROM t WHERE dt >= STR_TO_DATE('2024-01-02 03:04:05', '%Y-%m-%d %H:%i')")
    assert "CAST(TO_TIMESTAMP(NULLIF('2024-01-02 03:04:05', ''), 'YY-MM-DD HH24:MI') AS TIMESTAMP)" in sql
    assert "TZH" not in sql and "AT TIME ZONE" not in sql
    _ddl, view = _select("str_to_date(`t`.`s`,'%Y-%m-%d %H:%i') AS `x`")
    message = " ".join(w.message for w in view.warnings)
    assert "left over" in message and "daylight saving" in message


# --- Joins, calls, notes ----------------------------------------------------------------


@pytest.mark.parametrize(
    ("joins", "emitted"),
    [
        ("((`shop`.`t` `t` join `shop`.`orders` `o` on((`o`.`id` = `t`.`id`))) join `shop`.`orders` `p`)",
         'CROSS JOIN "shop"."orders" AS "p"'),
        ("(`shop`.`t` `t` join lateral (select count(0) AS `c` from `shop`.`orders` `o` where (`o`.`id` = `t`.`id`)) `l`)",
         "CROSS JOIN LATERAL ("),
        ("(`shop`.`t` `t` straight_join `shop`.`orders` `o` on((`o`.`id` = `t`.`id`)))", 'JOIN "shop"."orders" AS "o" ON'),
        ("(`shop`.`t` `t` straight_join `shop`.`orders` `o`)", 'CROSS JOIN "shop"."orders" AS "o"'),
    ],
)
def test_a_join_with_no_on_in_mysqls_join_container_is_a_cross_join(joins: str, emitted: str) -> None:
    """G97P-8: sqlglot wrote it as a comma, which PostgreSQL refuses inside parentheses."""
    ddl, _ = _view(f"select `t`.`id` AS `id` from {joins}")
    assert emitted in ddl and "STRAIGHT_JOIN" not in ddl, ddl
    assert not re.search(r'\), "|", (LATERAL|")', ddl.split(" FROM ", 1)[1]), ddl


def test_a_top_level_comma_join_is_left() -> None:
    # G107 (ES-14): a JOIN without ON is MySQL's CROSS JOIN, which binds as JOIN does.
    ddl, _ = _view("select `t`.`id` AS `id` from `shop`.`t` `t` join `shop`.`orders` `o`")
    assert '"shop"."t" AS "t" CROSS JOIN "shop"."orders" AS "o"' in ddl
    ddl, _ = _view("select `t`.`id` AS `id` from `shop`.`t` `t`, `shop`.`orders` `o`")
    assert '"shop"."t" AS "t", "shop"."orders" AS "o"' in ddl


def test_nested_greatest_and_least_through_arithmetic_grow_linearly() -> None:
    """G97P-13: each level checked again through `+ 0` doubled the output (1.26 MB at 14)."""
    expr = "x0"
    for level in range(1, 15):
        expr = f"{'GREATEST' if level % 2 else 'LEAST'}(x{level}, {expr} + 0)"
    sql, _ = _query(f"SELECT {expr} FROM t", typed=False)
    assert len(sql) < 1000 and sql.count("CASE WHEN") == 1


def test_a_coalesce_ending_in_a_literal_is_never_null() -> None:
    sql, _ = _query("SELECT GREATEST(a, COALESCE(b, 0)) FROM t", typed=False)
    assert "CASE WHEN a IS NULL THEN NULL ELSE GREATEST(a, COALESCE(b, 0)) END" in sql


def test_mysqls_json_unquote_of_json_extract_is_written_in_a_view_too() -> None:
    """XS-4: MySQL stores ->> in a view as json_unquote(json_extract(...)), which the view left
    as json_unquote() (MANUAL, failing) where the Query Converter wrote ->>."""
    ddl, view = _select("json_unquote(json_extract(`t`.`j`,'$.b')) AS `b`")
    assert "JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'b')" in ddl and view.warnings == []
    # Over several paths JSON_EXTRACT returns an array: left, and reported (G107: by what it
    # is, a path Aurora DSQL cannot follow).
    _ddl, view = _select("json_unquote(json_extract(`t`.`j`,'$.a','$.b')) AS `b`")
    assert any("several paths" in w.message for w in view.warnings)
    _sql, result = _query("SELECT JSON_UNQUOTE(JSON_EXTRACT(j, '$.a', '$.b')) FROM t")
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]


@pytest.mark.parametrize("sql", ["SELECT CAST(c AS BINARY(16)) FROM t", "SELECT CONVERT(bl USING ucs2) FROM t"])
def test_notes_name_no_target_ddl_for_a_statement(sql: str) -> None:
    """XS-6: a Query Converter statement has no target DDL."""
    _sql, result = _query(sql)
    assert result.warnings and all("target DDL" not in w.message for w in result.warnings)


def test_a_user_function_named_like_a_call_the_rewrite_writes_is_reported() -> None:
    """XS-7: DATE_TRUNC etc. were exempted by name, the source's own (schema-qualified) call
    included -- graded AUTO, and the CREATE VIEW failed."""
    _ddl, view = _select("`shop`.`date_trunc`(`t`.`dt`,'month') AS `m`,`shop`.`octet_length`(`t`.`s`) AS `o`")
    message = " ".join(w.message for w in view.warnings)
    assert "date_trunc()" in message and "octet_length()" in message
    _sql, result = _query("SELECT to_char(dt, 'month'), date_trunc(dt, 'month'), to_date(s) FROM t")
    message = " ".join(w.message for w in result.warnings)
    assert all(f"{name}()" in message for name in ("to_char", "date_trunc", "to_date"))


def test_the_rewrites_own_calls_are_not_reported_even_copied() -> None:
    """GREATEST copies its arguments: a clock's DATE_TRUNC stays the rewrite's."""
    ddl, view = _select("greatest(now(),`t`.`dt`) AS `g`,dayname(`t`.`d`) AS `n`,json_array(`t`.`dt`) AS `j`")
    assert ddl.count("DATE_TRUNC('second', LOCALTIMESTAMP)") == 2 and view.warnings == []
