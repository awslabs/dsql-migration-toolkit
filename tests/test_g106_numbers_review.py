"""G106: the review of G101-G105, the numbers -- a view MySQL merges leaves its quotient to
the query that reads it, MySQL's quotient is its kept digits truncated, a division of
operands not typed is rounded in numeric, more operand shapes are exact, the shown scale
carries through subqueries, windows, NULLIF, CASE branches and UNION columns, a FLOAT is
summed in double precision, ROUND to places read at run time, a TINYINT(1) used as a
number, strings read as numbers, composite intervals, windows, GREATEST / LEAST nests and
chains of thousands of operators. Each form was run on MySQL 8.4 and PostgreSQL 17 over the
same rows (scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import re

import pytest
import sqlglot
from sqlglot import exp

from dsql_migrator.core.models import Classification, ColumnDef, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="i", mysql_type="int"),
        ColumnDef(name="n", mysql_type="int"),
        ColumnDef(name="f", mysql_type="float"),
        ColumnDef(name="db", mysql_type="double"),
        ColumnDef(name="dec4", mysql_type="decimal(10,4)"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="c", mysql_type="char(8)"),
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="dt", mysql_type="datetime"),
        ColumnDef(name="flag", mysql_type="tinyint(1)"),
    ],
    primary_key=["id"],
)
_ORDERS = TableDef(
    name="shop.orders",
    columns=[ColumnDef(name="id", mysql_type="int", nullable=False), ColumnDef(name="qty", mysql_type="int")],
    primary_key=["id"],
)
_TABLES = [_T, _ORDERS]
# MySQL's int / int as written: its 9 kept digits, truncated.
_IN = 'DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF("t"."n", 0)) * 0.000000001'


def _norm(sql: str) -> str:
    """SQL with its whitespace collapsed and none just inside a parenthesis."""
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=_TABLES)
    return _norm(view.target_ddl), view


def _select(select_list: str, *, where: str = "", group: bool = False):
    return _view(
        f"select {select_list} from `shop`.`t` `t`"
        + (f" where {where}" if where else "")
        + (" group by `t`.`id`" if group else "")
    )


def _query(sql: str, *, typed: bool = True, pretty: bool = False):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    types = tuple(table_type_index(_TABLES).values()) if typed else ()
    result = QueryConverter(column_types=types).convert(sql, pretty=pretty)
    return _norm(result.converted_sql or ""), result


# --- Where MySQL shows a quotient at its scale (R1 / ES-1, P5 / ES-6, ES-9) --------------


def test_a_view_mysql_merges_leaves_its_quotient_to_the_query_that_reads_it() -> None:
    """R1 / ES-1: MySQL merges a view with no aggregate, DISTINCT, GROUP BY, LIMIT or UNION
    into the query that reads it, which computes with every digit the quotient keeps (SUM
    over 1000 rows of 1 / 3 is 333.3333, v.q * 3 is 1.0000, v.q = 0.3333 false). Rounded at
    the view's output, each reader computed with 0.3333 instead."""
    ddl, view = _view("select `t`.`id` AS `id`,(`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t`")
    assert f'SELECT "t"."id" AS "id", ({_IN}) AS "q" FROM' in ddl and "ROUND" not in ddl
    assert view.auto_converted and view.warnings == []


@pytest.mark.parametrize(
    "body",
    [
        "select `t`.`id` AS `id`,(`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` group by `t`.`id`",
        "select distinct `t`.`id` AS `id`,(`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t`",
        "select `t`.`id` AS `id`,(`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` limit 5",
        "select `t`.`id` AS `id`,(`t`.`i` / `t`.`n`) AS `q`,row_number() OVER ()  AS `r` from `shop`.`t` `t`",
        "select (`t`.`i` / `t`.`n`) AS `q`,count(0) AS `c` from `shop`.`t` `t`",
        "select (`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` union all select 1 AS `1`",
        # A subquery output that reads the view's own row.
        "select (`t`.`i` / `t`.`n`) AS `q`,(select max(`o`.`qty`) from `shop`.`orders` `o` "
        "where (`o`.`id` = `t`.`id`)) AS `m` from `shop`.`t` `t`",
    ],
)
def test_a_view_mysql_materializes_shows_its_outputs_at_their_scale(body: str) -> None:
    """MySQL materializes these (EXPLAIN: DERIVED), storing each output at its scale."""
    ddl, _ = _view(body)
    assert f"ROUND(({_IN}), 4)" in ddl, ddl


def test_a_constant_subquery_output_keeps_a_view_mergeable() -> None:
    """MySQL 8.4 merges a view whose subquery output reads none of its row; a table-less
    view it materializes."""
    ddl, _ = _view(
        "select (`t`.`i` / `t`.`n`) AS `q`,(select max(`o`.`qty`) from `shop`.`orders` `o`) AS `m` "
        "from `shop`.`t` `t`"
    )
    assert "ROUND" not in ddl
    ddl, _ = _view("select (1 / 3) AS `q`")
    assert "ROUND(CAST((DIV(CAST(1 AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001) AS DECIMAL), 4)" in ddl


def test_a_statements_own_output_is_shown_at_its_scale() -> None:
    sql, _ = _query("SELECT i / n AS q FROM t")
    assert sql == "SELECT ROUND(DIV(CAST(i AS DECIMAL) * 1000000000, NULLIF(n, 0)) * 0.000000001, 4) AS q FROM t"


def test_a_derived_table_or_cte_mysql_materializes_stores_its_outputs() -> None:
    """P5: MySQL rounds what a materialized derived table or CTE holds (x.q * 3 is then
    0.9999); one it merges stays unrounded."""
    ddl, _ = _view(
        "select `x`.`q` AS `q` from (select (`t`.`i` / 3) AS `q` from `shop`.`t` `t` group by `t`.`i`) `x`"
    )
    assert '(SELECT ROUND(CAST((DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001) AS DECIMAL), 4) AS "q"' in ddl
    ddl, _ = _view("select `x`.`q` AS `q` from (select (`t`.`i` / 3) AS `q` from `shop`.`t` `t`) `x`")
    assert "ROUND" not in ddl
    sql, _ = _query("WITH c AS (SELECT DISTINCT i / n AS q FROM t) SELECT c.q FROM c")
    assert "SELECT DISTINCT ROUND(DIV(" in sql


def test_a_quotient_read_out_of_a_derived_table_is_shown_by_its_type() -> None:
    """P5: a derived table's or a CTE's column computed by a quotient is one MySQL merges
    into the outer query, which shows it -- and what it computes from it -- at its scale; a
    column passed through holds its own digits (G107 types both)."""
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    converted = QueryConverter(column_types=tuple(table_type_index(_TABLES).values())).convert(
        "SELECT x.q, x.q * 7 AS q7, x.dec4 FROM (SELECT t.i / 7 AS q, t.dec4 FROM shop.t t) x"
    ).converted_sql
    assert "ROUND(x.q, 4)" in converted and "ROUND(x.q * 7, 4) AS q7" in converted
    assert ", x.dec4 FROM" in converted  # a table's decimal column holds its own digits


def test_a_scalar_subquery_window_and_nullif_carry_the_scale() -> None:
    """ES-6 / P5: they kept every digit at the output."""
    ddl, _ = _view("select (select avg(`o`.`qty`) from `shop`.`orders` `o`) AS `a`")
    assert 'ROUND((SELECT TRUNC(AVG("o"."qty"), 9) FROM "shop"."orders" AS "o"), 4) AS "a"' in ddl
    ddl, _ = _select("nullif((`t`.`i` / `t`.`n`),0) AS `b`", group=True)
    assert f"ROUND(NULLIF(({_IN}), 0), 4)" in ddl
    ddl, _ = _select("avg(`t`.`i`) OVER ()  AS `c`")
    assert 'ROUND(TRUNC(AVG("t"."i") OVER (), 9), 4) AS "c"' in ddl


def test_a_union_column_is_shown_at_its_largest_scale_every_branch() -> None:
    """P5 / ES-6: MySQL stores each branch at the union column's type -- SELECT 1 / 3 UNION
    SELECT 1 is 0.3333 and 1.0000, and a scale-4 quotient beside a scale-8 one 3.33333333."""
    ddl, _ = _view("select (`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` union all select 1 AS `1`")
    assert 'UNION ALL SELECT ROUND(1, 4) AS "1"' in ddl
    ddl, _ = _view(
        "select (`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` union all select (`t`.`dec4` / 3) AS `q` "
        "from `shop`.`t` `t`"
    )
    assert f"ROUND(({_IN}), 8)" in ddl and 'ROUND((DIV("t"."dec4" * 1000000000, NULLIF(3, 0)) * 0.000000001), 8)' in ddl


def test_a_case_or_if_shows_each_branch_as_itself() -> None:
    """ES-9: MySQL shows the branch a CASE / IF takes as itself (CASE WHEN c THEN 1 / 3 ELSE
    0 END is 0, IF(0, 1 / 3, -5) is -5); the whole CASE was padded to 0.0000. Through a
    temporary table (DISTINCT, a UNION, a view MySQL materializes) every value is stored at
    the column's scale, and COALESCE / GREATEST pad as MySQL's do."""
    sql, _ = _query("SELECT CASE WHEN i > 0 THEN i / n ELSE 0 END AS c, IF(i > 0, i / n, i) AS d FROM t")
    shown = "ROUND(DIV(CAST(i AS DECIMAL) * 1000000000, NULLIF(n, 0)) * 0.000000001, 4)"
    assert f"CASE WHEN i > 0 THEN {shown} ELSE 0 END AS c" in sql and f"THEN {shown} ELSE i END AS d" in sql
    sql, _ = _query("SELECT DISTINCT CASE WHEN i > 0 THEN i / n ELSE 0 END AS c FROM t")
    assert "SELECT DISTINCT ROUND(CASE WHEN i > 0 THEN DIV(" in sql
    ddl, _ = _select("(case when (`t`.`i` > 0) then (`t`.`i` / `t`.`n`) else 0 end) AS `c`", group=True)
    assert "ROUND((CASE WHEN" in ddl
    sql, _ = _query("SELECT COALESCE(5, i / n) AS c FROM t")
    assert "ROUND(COALESCE(5, DIV(" in sql


def test_having_reads_a_case_alias_without_its_shown_rounding() -> None:
    sql, _ = _query("SELECT n, CASE WHEN n > 0 THEN SUM(i) / 3 ELSE 0 END AS r FROM t GROUP BY n HAVING r > 1.6666")
    assert (
        "HAVING CASE WHEN n > 0 THEN DIV(CAST(SUM(i) AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001 "
        "ELSE 0 END > 1.6666"
    ) in sql


# --- MySQL's exact division (ES-8, ES-3, P1 / P2) ------------------------------------------


@pytest.mark.parametrize(
    ("dividend", "divisor", "kept"),
    # Checked on MySQL 8.4: 2 / 3, 1.5 / 7, DECIMAL(10,4) / 7, 1 / 1.5 keep 9 digits;
    # 1.25 / 1.5, (1 / 3) / 7 and AVG of a DECIMAL(20,10) 18.
    [(0, 0, 9), (1, 0, 9), (4, 0, 9), (0, 1, 9), (2, 1, 18), (9, 0, 18), (10, 0, 18)],
)
def test_mysql_keeps_whole_words_of_quotient_digits(dividend: int, divisor: int, kept: int) -> None:
    from dsql_migrator.core.converter import _mysql_quotient_digits

    assert _mysql_quotient_digits(dividend, divisor) == kept


def test_mysqls_quotient_is_its_kept_digits_truncated() -> None:
    """ES-8: MySQL computes with 9 fraction digits of an int / int quotient, truncated --
    2 / 3 * 1000000 is 666666.6660 -- where PostgreSQL's numeric division kept 20 (or 16
    significant, rounded: 123456789012 / 7 lost its 9th). Written DIV(x * 10^9, y) * 10^-9,
    exactly; AVG truncated likewise."""
    ddl, _ = _select(
        "((`t`.`i` / 3) / 7) AS `q`,(`t`.`dec4` / 7) AS `d`,(avg(`t`.`dec4`) * 1000) AS `a`"
    )
    assert (
        'DIV((DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001) * 1000000000000000000, '
        "NULLIF(7, 0)) * 0.000000000000000001"
    ) in ddl
    assert 'DIV("t"."dec4" * 1000000000, NULLIF(7, 0)) * 0.000000001' in ddl
    assert 'ROUND((TRUNC(AVG("t"."dec4"), 9) * 1000), 8) AS "a"' in ddl
    # a / NULLIF(b, 0) is not wrapped twice.
    ddl, _ = _select("(`t`.`i` / nullif(`t`.`n`,0)) AS `q`")
    assert f"({_IN}) AS" in ddl


@pytest.mark.parametrize(
    "divisor",
    [
        "nullif(`t`.`n`,0)",
        "floor(`t`.`n`)",
        "length(`t`.`s`)",
        "(to_days(`t`.`d`) - to_days(`t`.`dt`))",
        "(select count(0) from `shop`.`orders` `o`)",
        "row_number() OVER ()",
    ],
)
def test_common_operand_shapes_divide_exactly(divisor: str) -> None:
    """ES-3: the safe-division idiom a / NULLIF(b, 0), FLOOR, LENGTH, DATEDIFF (stored as
    TO_DAYS - TO_DAYS), a scalar subquery and a window rank kept sqlglot's double, rounding
    ties to even. FLOOR of an integer is the integer (PostgreSQL's floor(int) is a double)."""
    ddl, _ = _select(f"(`t`.`i` / {divisor}) AS `q`")
    assert 'DIV(CAST("t"."i" AS DECIMAL) * 1000000000, ' in ddl and "DOUBLE PRECISION" not in ddl, ddl
    assert "FLOOR" not in ddl


def test_a_quotient_of_values_not_typed_rounds_truncates_and_wraps_in_numeric() -> None:
    """P1 / P2: an untyped division was taken for a double, so ROUND(paid / total, 2) of
    1 / 8 gave 0.12, TRUNCATE(29 / 100, 2) 0.28, CAST(-1 / 2 AS UNSIGNED) 0 and ROUND(5 / 2)
    2 (MySQL: 0.13, 0.29, 18446744073709551615, 3) -- exact in MySQL as like as not, they
    round in numeric. A double operand keeps the double path."""
    sql, _ = _query("SELECT ROUND(a / b, 2), TRUNCATE(a / b, 2), CAST(a / b AS UNSIGNED), ROUND(a / b) FROM o", typed=False)
    untyped = "CAST(CAST(a AS DOUBLE PRECISION) / NULLIF(b, 0) AS DECIMAL)"
    assert f"ROUND({untyped}, 2)" in sql and f"TRUNC({untyped}, 2)" in sql
    assert f"CASE WHEN ROUND({untyped}) < 0 THEN ROUND({untyped}) + 18446744073709551616" in sql
    assert f"ROUND({untyped}) FROM o" in sql
    ddl, _ = _view("select round((`v`.`id` / 8),2) AS `r` from `shop`.`v_base` `v`")
    assert 'ROUND(CAST((CAST("v"."id" AS DOUBLE PRECISION) / NULLIF(8, 0)) AS DECIMAL), 2)' in ddl
    ddl, _ = _select("round((`t`.`db` / 8),2) AS `r`")
    assert 'ROUND(CAST((CAST("t"."db" AS DOUBLE PRECISION) / NULLIF(8, 0)) AS DOUBLE PRECISION) * 100) / 100' in ddl


# --- Floats, booleans, places read at run time (P3 / ES-4, ES-12, ES-19) -------------------


def test_sum_of_a_float_is_added_in_double_precision() -> None:
    """P3 / P4 / ES-4: PostgreSQL's sum(real) adds in real -- 100000.25 + 23456.53 lost its
    cents, 1000 x 0.1 came to 99.99905 -- before the cast to double precision; MySQL sums a
    FLOAT in double."""
    ddl, _ = _select("sum(`t`.`f`) AS `s`,sum(distinct `t`.`f`) AS `d`,sum(`t`.`f`) OVER ()  AS `w`")
    assert 'SUM(CAST("t"."f" AS DOUBLE PRECISION)) AS "s"' in ddl
    assert 'SUM(DISTINCT CAST("t"."f" AS DOUBLE PRECISION)) AS "d"' in ddl
    assert 'SUM(CAST("t"."f" AS DOUBLE PRECISION)) OVER () AS "w"' in ddl


def test_round_of_a_float_to_places_read_at_run_time_is_in_double_precision() -> None:
    """ES-12: ROUND(dd = 2.5, n - n) was 3 and ROUND(0.125e0, n) 0.13 in numeric; MySQL's
    my_double_round gives 2 and 0.12 -- rint(x * 10^d) / 10^d, rint(x / 10^-d) * 10^-d."""
    ddl, _ = _select("round(`t`.`db`,`t`.`n`) AS `r`,truncate(`t`.`f`,(`t`.`n` - 1)) AS `t`")
    ten = "POWER(CAST(10 AS DOUBLE PRECISION), "
    assert (
        f'CASE WHEN "t"."n" >= 0 THEN ROUND(CAST("t"."db" AS DOUBLE PRECISION) * {ten}"t"."n")) / {ten}"t"."n") '
        f'ELSE ROUND(CAST("t"."db" AS DOUBLE PRECISION) / {ten}-"t"."n")) * {ten}-"t"."n") END AS "r"'
    ) in ddl
    assert f'TRUNC(CAST("t"."f" AS DOUBLE PRECISION) / {ten}-("t"."n" - 1))) * {ten}-("t"."n" - 1)) END AS "t"' in ddl
    assert "DECIMAL" not in ddl


def test_a_tinyint1_used_as_a_number_is_its_integer() -> None:
    """ES-19: a TINYINT(1) is a boolean on Aurora DSQL -- paid / 2, paid + 0, SUM(paid),
    MAX(paid), IFNULL(paid, 0) and paid = 1 failed at CREATE VIEW (no boolean + integer,
    no sum(boolean), no boolean = integer)."""
    ddl, view = _select(
        "(`t`.`flag` / 2) AS `h`,(`t`.`flag` + 0) AS `z`,ifnull(`t`.`flag`,0) AS `n`,sum(`t`.`flag`) AS `s`,"
        "max(`t`.`flag`) AS `m`,avg(`t`.`flag`) AS `a`",
        where="((`t`.`flag` = 1) and (`t`.`flag` in (0,1)) and (`t`.`flag` = true))",
    )
    flag = 'CAST("t"."flag" AS INT)'
    assert f"DIV(CAST({flag} AS DECIMAL) * 1000000000, NULLIF(2, 0))" in ddl
    assert f"({flag} + 0)" in ddl and f"COALESCE({flag}, 0)" in ddl
    assert f"SUM({flag})" in ddl and f"MAX({flag})" in ddl and f"TRUNC(AVG({flag}), 9)" in ddl
    assert f"({flag} = 1)" in ddl and f"({flag} IN (0, 1))" in ddl
    assert '("t"."flag" = TRUE)' in ddl  # a boolean beside a boolean is one
    assert view.warnings == []


# --- Strings read as numbers, intervals (ES-11, ES-22, ES-21) -------------------------------


def test_a_string_read_as_a_number_takes_its_leading_integer() -> None:
    """ES-11 / ES-22: a text interval amount was ROUND(text), which does not exist; CAST(s AS
    UNSIGNED) rounded '1.5' to 2 and kept '-2' negative. MySQL reads the leading integer
    ('1.5' is 1, 'AB' 0, '-2' wraps to 18446744073709551614); an INTERVAL ... SECOND reads
    the leading number ('1.5abc' is 1.5 seconds); NULL stays NULL."""
    ddl, _ = _select(
        "(`t`.`d` + interval `t`.`s` day) AS `a`,(`t`.`dt` + interval `t`.`c` second) AS `b`,cast(`t`.`s` as unsigned) AS `u`"
    )
    leading = 'CASE WHEN NOT "t"."s" IS NULL THEN COALESCE(CAST(SUBSTRING("t"."s", \'^[[:space:]]*[+-]?[0-9]+\') AS '
    assert f"CAST(\"t\".\"d\" + ({leading}BIGINT), 0) END) * INTERVAL '1 DAY' AS DATE)" in ddl
    assert "(?:[0-9]+[.]?[0-9]*|[.][0-9]+)(?:[eE][+-]?[0-9]+)?') AS DECIMAL), 0) END) * INTERVAL '1 SECOND'" in ddl
    assert f"CASE WHEN {leading}DECIMAL), 0) END < 0 THEN" in ddl
    assert "ROUND" not in ddl


@pytest.mark.parametrize(
    ("interval", "emitted"),
    [
        ("interval '1 2' day_hour", "\"t\".\"dt\" + INTERVAL '1 day 2 hour'"),
        ("interval '1:30' hour_minute", "INTERVAL '1 hour 30 minute'"),
        ("interval '1.5' minute_second", "INTERVAL '1 minute 5 second'"),
        ("interval '2:3' day_second", "INTERVAL '0 day 0 hour 2 minute 3 second'"),
        ("interval '-1 2' day_hour", "INTERVAL '-1 day -2 hour'"),
        ("interval '1.5' second_microsecond", "INTERVAL '1 second 500000 microsecond'"),
        ("interval '1.1234567' second_microsecond", "INTERVAL '1 second 1234567 microsecond'"),
        ("interval 1.5 day_hour", "INTERVAL '1 day 5 hour'"),
        ("interval '1-2' year_month", "INTERVAL '14 MONTH'"),
    ],
)
def test_a_composite_interval_is_written_as_its_parts(interval: str, emitted: str) -> None:
    """ES-21: INTERVAL '1 2 DAY_HOUR' was refused by PostgreSQL. MySQL reads any punctuation
    as a separator, the parts left out as the leftmost, a microsecond part of fewer than 6
    digits as a fraction (each checked on MySQL 8.4)."""
    ddl, view = _select(f"(`t`.`dt` + {interval}) AS `x`")
    assert emitted in ddl and view.warnings == [], ddl


def test_a_date_plus_years_and_months_stays_a_date_and_unreadable_amounts_are_reported() -> None:
    ddl, _ = _select("(`t`.`d` + interval '1-2' year_month) AS `a`,(`t`.`d` + interval '1 2' day_hour) AS `b`")
    assert "(CAST(\"t\".\"d\" + INTERVAL '14 MONTH' AS DATE)) AS \"a\"" in ddl
    assert "(\"t\".\"d\" + INTERVAL '1 day 2 hour') AS \"b\"" in ddl  # a DATETIME, as MySQL's
    _ddl, view = _select("(`t`.`dt` + interval '1 2 3' day_hour) AS `a`,(`t`.`dt` + interval `t`.`s` day_hour) AS `b`")
    messages = " ".join(w.message for w in view.warnings)
    assert "INTERVAL '1 2 3' DAY_HOUR" in messages and "read at run time" in messages
    assert all(w.classification is Classification.MANUAL for w in view.warnings)


def test_a_yyyymmdd_string_is_a_date_and_the_unit_is_the_word_after_the_amount() -> None:
    """ES-21: '20240131' + INTERVAL 1 DAY returned a timestamp; INTERVAL '3 days' DAY was
    parsed as INTERVAL '3 DAYS' AS day, a syntax error -- MySQL reads its leading integer."""
    sql, _ = _query("SELECT '20240131' + INTERVAL 1 DAY, d + INTERVAL '3 days' DAY, dt + INTERVAL ' 7abc' HOUR FROM t")
    assert "CAST(CAST('20240131' AS DATE) + INTERVAL '1 DAY' AS DATE)" in sql
    assert "CAST(d + INTERVAL '3 DAY' AS DATE)" in sql and "dt + INTERVAL '7 HOUR'" in sql
    assert " AS DAY" not in sql.upper().replace("AS DATE", "")


# --- Windows (found with ES-6) -----------------------------------------------------------


def test_a_window_keeps_its_arguments_and_result_at_their_scale() -> None:
    """MySQL keeps a window function's row values and its result in the window's buffer,
    each at its scale: SUM(a / b) OVER () adds 0.3333 (27.8572 where every digit gives
    27.8571), AVG(x) OVER (...) * 10^9 multiplies 1.3333; a constant argument is not kept
    there (SUM(1 / 3) OVER () rounds only its result)."""
    ddl, _ = _select(
        "sum((`t`.`i` / 3)) OVER (ORDER BY `t`.`id` )  AS `s`,(avg(`t`.`i`) OVER ()  * 1000000000) AS `a`,"
        "sum((1 / 3)) OVER ()  AS `k`"
    )
    assert 'SUM(ROUND(CAST((DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001) AS DECIMAL), 4)) OVER' in ddl
    assert '(ROUND(TRUNC(AVG("t"."i") OVER (), 9), 4) * 1000000000) AS "a"' in ddl
    assert "ROUND(CAST(SUM((DIV(CAST(1 AS DECIMAL) * 1000000000, NULLIF(3, 0)) * 0.000000001)) OVER () AS DECIMAL), 4)" in ddl


# --- GREATEST / LEAST nests (P10 / R6 / ES-10) ----------------------------------------------


@pytest.mark.parametrize(
    "shape", ["({e}) / 2", "ABS({e})", "ROUND({e}, 2)", "CAST({e} AS SIGNED)", "CONCAT({e}, '')"]
)
def test_a_nest_is_tested_once_through_any_null_strict_node(shape: str) -> None:
    """P10 / R6 / ES-10: through a division, ABS(), ROUND(), a cast or CONCAT each level was
    tested on its own and copied twice by the next -- 634,755 characters at depth 12 through
    '/ 1'. (sqlglot's own parser stops at about 21 nested calls.)"""
    expr = "x0"
    for level in range(1, 15):
        expr = f"{'GREATEST' if level % 2 else 'LEAST'}(x{level}, " + shape.format(e=expr) + ")"
    sql, result = _query(f"SELECT {expr} FROM t", typed=False)
    assert len(sql) < 2000 and sql.count("CASE WHEN") == 1 and result.classification is Classification.AUTO, len(sql)


def test_a_zero_divisor_inside_a_nest_makes_it_null() -> None:
    """MySQL's x / 0 is NULL, so GREATEST(x1, x0 / LEAST(x2, x3)) is NULL when the LEAST is
    0; the division (x0 / NULLIF(..., 0)) is NULL there too, and Aurora DSQL's GREATEST
    would skip it."""
    sql, _ = _query("SELECT GREATEST(x1, x0 / LEAST(x2, x3)) FROM t", typed=False)
    assert sql.count("CASE WHEN") == 1 and "LEAST(x2, x3) = 0" in sql and "x3 IS NULL" in sql


def test_a_nest_behind_ifnull_is_capped_and_reported() -> None:
    """A nest behind IFNULL (not NULL-strict) is its own, and the next level copies it into
    its NULL test: past three such levels the outer calls are left as Aurora DSQL's and the
    statement is MANUAL, instead of doubling the output per level."""
    expr = "x0"
    for level in range(1, 9):
        expr = f"GREATEST(x{level}, IFNULL({expr}, x{level}))"
    sql, result = _query(f"SELECT {expr} FROM t", typed=False)
    assert len(sql) < 2000 and result.classification is Classification.MANUAL, len(sql)
    assert any("GREATEST() / LEAST() is nested" in w.message for w in result.warnings)


# --- Chains of thousands of operators (R5) --------------------------------------------------


@pytest.mark.parametrize("terms", [1200, 3000])
def test_a_chain_of_thousands_of_operators_converts(terms: int) -> None:
    """R5: decimal_scale / kind_of recursed once per operator -- RecursionError from about
    985 terms, out of the Convert click. Deeper than the typing reads walk, a value is of
    unknown type."""
    chain = " + ".join(f"c{i % 50}" for i in range(terms))
    for sql in (f"SELECT {chain} AS x FROM t", f"SELECT id FROM t WHERE ({chain}) / 3 > 5"):
        _sql, result = _query(sql, pretty=True)
        assert result.classification is Classification.AUTO and result.converted_sql


def test_a_statement_nested_too_deeply_is_manual_not_an_error() -> None:
    """QueryConverter.convert never raises: a nest deeper than sqlglot's parser walks is
    flagged for manual review."""
    sql = "SELECT " + "ABS(" * 100 + "x" + ")" * 100 + " FROM t"
    _sql, result = _query(sql)
    assert result.classification is Classification.MANUAL and result.converted_sql is None
    assert "nested too deeply" in result.warnings[0].message
