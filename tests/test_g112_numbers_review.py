"""G112: the review of G106-G111, the numbers -- an ALGORITHM=TEMPTABLE view is
materialized, a CASE / IF a statement groups by is shown whole, an assignment's target is
the column itself, a TINYINT(1) or a comparison MySQL reads as a number is its integer, a
quotient key is reported, chains of operators are typed in a loop, AVG is MySQL's exact
division of its SUM by its COUNT, a UNION column with a FLOAT / DOUBLE branch is a DOUBLE,
FLOAT arithmetic is in double precision, more operands are typed, an interval written first
is read as MySQL reads it, MOD / DIV by 0 is NULL, % binds as / does and TIMESTAMP '...' is
a DATETIME. Each form was run on MySQL 8.4 and PostgreSQL 17 over the same rows
(scripts/mysql_view_differential.py, local)."""

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
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="dt", mysql_type="datetime"),
        ColumnDef(name="flag", mysql_type="tinyint(1)"),
        ColumnDef(name="big", mysql_type="bigint"),
        ColumnDef(name="v", mysql_type="decimal(20,10)"),
    ],
    primary_key=["id"],
)
# MySQL's int / int as written: its 9 kept digits, truncated.
_IN = 'DIV(CAST("t"."i" AS DECIMAL) * 1000000000, NULLIF("t"."n", 0)) * 0.000000001'
_QIN = "DIV(CAST(i AS DECIMAL) * 1000000000, NULLIF(n, 0)) * 0.000000001"
_KEY_NOTE = "A quotient (an AVG) is a GROUP BY, PARTITION BY, window ORDER BY or COUNT(DISTINCT"


def _norm(sql: str) -> str:
    """SQL with its whitespace collapsed and none just inside a parenthesis."""
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=[_T])
    return _norm(view.target_ddl), view


def _select(select_list: str, *, group: bool = False):
    return _view(f"select {select_list} from `shop`.`t` `t`" + (" group by `t`.`id`" if group else ""))


def _query(sql: str):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    result = QueryConverter(column_types=tuple(table_type_index([_T]).values())).convert(sql)
    return _norm(result.converted_sql or ""), result


def _messages(warnings) -> str:
    return " ".join(w.message for w in warnings)


# --- ALGORITHM=TEMPTABLE (G106V-F1 / V106B-2 / ES1-TEMPTABLE) ------------------------------


def test_a_temptable_view_is_materialized_whatever_its_body() -> None:
    """The introspector reads SHOW CREATE VIEW's text, which keeps the ALGORITHM: MySQL
    materializes an ALGORITHM=TEMPTABLE view, storing q at 0.3333 (SUM over 1000 rows of
    1 / 3 is 333.3000); the same body UNDEFINED or MERGE is merged, left unrounded."""
    body = (
        "DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `shop`.`v` AS select (`t`.`i` / `t`.`n`) AS `q` "
        "from `shop`.`t` `t`"
    )
    ddl, view = _view(f"CREATE ALGORITHM=TEMPTABLE {body}")
    assert f'SELECT ROUND(({_IN}), 4) AS "q" FROM' in ddl and view.auto_converted
    for algorithm in ("UNDEFINED", "MERGE"):
        ddl, _ = _view(f"CREATE ALGORITHM={algorithm} {body}")
        assert f'SELECT ({_IN}) AS "q" FROM' in ddl and "ROUND" not in ddl


# --- A CASE / IF the statement groups by (V106B-3) ------------------------------------------


@pytest.mark.parametrize("key", ["IF(i > 1, i / n, 0)", "1", "r"])
def test_a_case_the_statement_groups_by_is_shown_whole(key: str) -> None:
    """MySQL keeps a CASE / IF it groups by in the grouping's temporary table at the
    column's scale (0.0000); shown branch by branch, the select list no longer matched GROUP
    BY CASE ... on Aurora DSQL ('column "t.i" must appear in the GROUP BY clause')."""
    sql, _ = _query(f"SELECT IF(i > 1, i / n, 0) AS r, COUNT(*) AS c FROM t GROUP BY {key}")
    assert f"SELECT ROUND(CASE WHEN i > 1 THEN {_QIN} ELSE 0 END, 4) AS r" in sql


def test_a_case_beside_another_key_shows_its_branch() -> None:
    """Not a key -- or a key MySQL reads as the FROM column of that name -- MySQL shows the
    branch it takes as itself (0)."""
    shown = f"CASE WHEN i > 1 THEN ROUND({_QIN}, 4) ELSE 0 END"
    sql, _ = _query("SELECT id, IF(i > 1, i / n, 0) AS r FROM t GROUP BY id")
    assert f"SELECT id, {shown} AS r" in sql
    sql, _ = _query("SELECT IF(i > 1, i / n, 0) AS n FROM t GROUP BY n")  # n: t's column
    assert f"SELECT {shown} AS n" in sql


# --- Assignment targets (V106B-4 / ES19-SET-TARGET) -----------------------------------------


def test_an_assignment_target_is_never_cast() -> None:
    """SET flag = 1 became SET CAST(flag AS INT) = 1, a syntax error at a cast nobody wrote;
    a number assigned to a TINYINT(1) -- a boolean on Aurora DSQL -- is written as one, and
    a boolean assigned to a number as its integer."""
    sql, result = _query("UPDATE t SET flag = 1, i = flag, n = (i > 0) WHERE id = 2")
    assert sql == "UPDATE t SET flag = TRUE, i = CAST(flag AS INT), n = (CAST(i > 0 AS INT)) WHERE id = 2"
    # (G120) i = flag and n = (i > 0) read what the SET assigned before them: MySQL assigns
    # left to right (the new flag, the new i), Aurora DSQL reads the old values -- reported.
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]
    assert "left to right" in result.warnings[0].message
    sql, _ = _query("UPDATE t SET flag = 1 - flag, flag = IF(i > 0, 1, 0) WHERE id = 2")
    assert "SET flag = (1 - CAST(flag AS INT)) <> 0, flag = CASE WHEN i > 0 THEN 1 ELSE 0 END <> 0" in sql
    sql, _ = _query("INSERT INTO t (id, flag) VALUES (1, TRUE) ON DUPLICATE KEY UPDATE flag = 0")
    assert sql.endswith("ON CONFLICT DO UPDATE SET flag = FALSE")


# --- A TINYINT(1) or a comparison read as a number (G106V-F3 / ES19-IDIOMS) ------------------


def test_a_tinyint1_is_its_integer_wherever_mysql_reads_a_number() -> None:
    """A simple CASE over the flag, numeric casts, <=>, ROUND / FLOOR / SIGN / POWER and the
    bitwise operators failed at CREATE VIEW while graded AUTO (no boolean = integer, no cast
    of a boolean to numeric, no round(boolean), no boolean & integer)."""
    ddl, view = _select(
        "(case `t`.`flag` when 1 then 'y' else 'n' end) AS `a`,cast(`t`.`flag` as signed) AS `b`,"
        "cast(`t`.`flag` as decimal(5,2)) AS `e`,(`t`.`flag` <=> 1) AS `k`,round(`t`.`flag`,0) AS `r`,"
        "sign(`t`.`flag`) AS `g`,pow(`t`.`flag`,2) AS `p`,(`t`.`flag` & 1) AS `w`"
    )
    flag = 'CAST("t"."flag" AS INT)'
    assert f"(CASE {flag} WHEN 1 THEN 'y' ELSE 'n' END)" in ddl
    assert f"CAST({flag} AS BIGINT)" in ddl and f"CAST({flag} AS DECIMAL(5, 2))" in ddl
    assert f"({flag} IS NOT DISTINCT FROM 1)" in ddl and f'{flag} AS "r"' in ddl
    assert f"CAST(SIGN({flag}) AS INT)" in ddl and f"POWER({flag}, 2)" in ddl and f"({flag} & 1)" in ddl
    assert view.warnings == []


def test_a_comparison_read_as_a_number_is_its_integer() -> None:
    """MySQL's count-the-rows idiom: SUM(qty > 2) is 2 there, function sum(boolean) does not
    exist here; (a > 0) + 1 and (a > 0) / 2 had no operator either."""
    ddl, _ = _select("sum((`t`.`i` > 2)) AS `a`,sum((not(`t`.`flag`))) AS `c`,((`t`.`i` > 0) + 1) AS `e`")
    assert 'SUM((CAST("t"."i" > 2 AS INT))) AS "a"' in ddl
    assert 'SUM((CAST(NOT ("t"."flag") AS INT))) AS "c"' in ddl
    assert '((CAST("t"."i" > 0 AS INT)) + 1) AS "e"' in ddl


def test_a_boolean_stays_one_where_mysql_reads_a_condition() -> None:
    sql, _ = _query(
        "SELECT id, flag = (i > 0) AS b FROM t WHERE flag AND flag = TRUE AND flag IS TRUE AND IF(flag, 1, 0) = 1"
    )
    assert "SELECT id, flag = (i > 0) AS b FROM t WHERE flag AND flag = TRUE AND flag IS TRUE AND CASE WHEN flag" in sql
    assert "CAST(flag" not in sql


# --- A quotient as a key (G106V-F4) ---------------------------------------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT i / n AS q, COUNT(*) AS c FROM t GROUP BY i / n",
        "SELECT id, RANK() OVER (ORDER BY i / n) AS rk FROM t",
        "SELECT id, SUM(i) OVER (PARTITION BY dec4 / 3) AS s FROM t",
        "SELECT COUNT(DISTINCT i / n) AS c FROM t",
        "WITH c AS (SELECT id, i / n AS q FROM t) SELECT MAX(c.id) AS m FROM c GROUP BY c.q",
    ],
)
def test_a_quotient_key_is_reported(sql: str) -> None:
    """MySQL keys a quotient at its shown scale -- 1 / 3 and 3333 / 10000 are one group,
    rank and distinct value -- where Aurora DSQL keys every digit kept: graded AUTO, they
    returned other groups, ranks and counts."""
    _sql, result = _query(sql)
    assert result.classification is Classification.MANUAL and _KEY_NOTE in _messages(result.warnings)


@pytest.mark.parametrize("key", ["1", "q", "id"])
def test_a_key_on_the_shown_output_is_not_reported(key: str) -> None:
    """GROUP BY 1 or the alias keys on the output, shown at MySQL's scale on both."""
    sql, result = _query(f"SELECT i / n AS q, COUNT(*) AS c FROM t GROUP BY {key}")
    assert result.classification is Classification.AUTO and f"SELECT ROUND({_QIN}, 4) AS q" in sql


# --- Chains of operators (V106B-5) ----------------------------------------------------------


def test_a_long_chain_keeps_mysqls_exact_division() -> None:
    """Past the typing bound (200 nested reads) a chain's kind was unknown, so (i + i + ...)
    / 3 of 250 integers was a double, 83.33333333333333 where MySQL shows 83.3333; the chain
    is now read in a loop."""
    chain = " + ".join("i" for _ in range(250))
    sql, result = _query(f"SELECT ({chain}) / 3 AS x FROM t")
    assert sql.startswith("SELECT ROUND(CAST(DIV(CAST((i + i + ") and "NULLIF(3, 0)) * 0.000000001" in sql
    assert result.classification is Classification.AUTO and "DOUBLE PRECISION" not in sql


def test_a_value_nested_past_the_typing_bound_is_reported() -> None:
    """Nested deeper than the typing reads walk (here 250 unary minuses), what depends on
    the value's type is not written as MySQL computes it -- reported, not silently AUTO."""
    from dsql_migrator.core.converter import _column_type_resolver, _rewrite_mysql_calls, table_type_index

    value: exp.Expression = exp.column("i", table="t")
    for _ in range(250):
        value = exp.Neg(this=value)
    select = exp.select(exp.Div(this=value, expression=exp.Literal.number(3))).from_("shop.t AS t")
    _untranslated, _misformatted, issues = _rewrite_mysql_calls(
        select, _column_type_resolver(tuple(table_type_index([_T]).values()))
    )
    assert any("nested more than 200 levels deep" in text for text in issues)


# --- AVG (V106B-6 / ES8-AVG) ----------------------------------------------------------------


def test_avg_is_mysqls_exact_division_of_its_sum_by_its_count() -> None:
    """PostgreSQL's avg keeps 16 significant digits, rounded: AVG of 1700000000, 1700000000,
    1700000002 is 1700000000.66666667 there, MySQL's kept digits 1700000000.666666666."""
    sql, _ = _query("SELECT AVG(big) AS a, AVG(DISTINCT big) AS b, AVG(v) AS c FROM t")
    exact = "DIV(CAST(SUM({0}big) AS DECIMAL) * 1000000000, NULLIF(COUNT({0}big), 0)) * 0.000000001"
    assert f"ROUND(CAST({exact.format('')} AS DECIMAL), 4) AS a" in sql
    assert f"ROUND(CAST({exact.format('DISTINCT ')} AS DECIMAL), 4) AS b" in sql
    # DECIMAL(20,10): 18 digits kept, 14 shown.
    assert "ROUND(DIV(SUM(v) * 1000000000000000000, NULLIF(COUNT(v), 0)) * 0.000000000000000001, 14) AS c" in sql


def test_a_window_avg_divides_the_windows_sum_by_its_count() -> None:
    sql, _ = _query("SELECT id, AVG(big) OVER (ORDER BY id) * 1000 AS w FROM t")
    over = "OVER (ORDER BY id NULLS FIRST)"
    assert (
        f"ROUND(CAST(DIV(CAST(SUM(big) {over} AS DECIMAL) * 1000000000, NULLIF(COUNT(big) {over}, 0)) "
        "* 0.000000001 AS DECIMAL), 4) * 1000 AS w"
    ) in sql


# --- UNION columns (P5-UNION-DOUBLE) --------------------------------------------------------


def test_a_union_column_with_a_float_branch_is_a_double() -> None:
    """MySQL's quotient UNION a DOUBLE is a DOUBLE of the quotient's kept digits
    (7.142857142), and a FLOAT beside another number is read as its double
    (0.10000000149011612): rounded to 7.1429 and typed real here."""
    ddl, _ = _view("select (`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` union all select `t`.`db` AS `db` from `shop`.`t` `t`")
    assert f'SELECT ({_IN}) AS "q"' in ddl and "ROUND" not in ddl
    ddl, _ = _view("select (`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` union all select `t`.`f` AS `f` from `shop`.`t` `t`")
    assert 'UNION ALL SELECT CAST("t"."f" AS DOUBLE PRECISION) AS "f"' in ddl and "ROUND" not in ddl


def test_a_null_branch_keeps_a_union_column_at_its_scale() -> None:
    """SELECT 1 / 3 UNION ALL SELECT 1 UNION ALL SELECT NULL is 0.3333, 1.0000, NULL."""
    ddl, _ = _view("select (1 / 3) AS `q` union all select 1 AS `1` union all select NULL AS `NULL`")
    assert 'UNION ALL SELECT ROUND(1, 4) AS "1" UNION ALL SELECT NULL AS "NULL"' in ddl


def test_a_union_column_with_a_string_branch_is_not_rounded() -> None:
    """MySQL's string column holds the quotient's kept digits ('7.142857142')."""
    ddl, _ = _view("select (`t`.`i` / `t`.`n`) AS `q` from `shop`.`t` `t` union all select `t`.`s` AS `s` from `shop`.`t` `t`")
    assert "ROUND" not in ddl


# --- FLOAT arithmetic (ES4-FLOAT-EXPR) ------------------------------------------------------


def test_float_arithmetic_is_in_double_precision() -> None:
    """real * real is real in PostgreSQL: SUM(f * f) over 1000 rows of 0.1 was
    10.000000707805157 (MySQL 10.000000298023263) -- the cast came after the product."""
    ddl, _ = _select("sum((`t`.`f` * `t`.`f`)) AS `s`,avg((`t`.`f` * `t`.`f`)) AS `a`,sum((`t`.`f` * `t`.`i`)) AS `m`")
    double = 'CAST("t"."f" AS DOUBLE PRECISION)'
    assert f'SUM(({double} * {double})) AS "s"' in ddl and f'AVG(({double} * {double})) AS "a"' in ddl
    assert 'SUM(("t"."f" * "t"."i")) AS "m"' in ddl  # real * integer is double precision already


# --- Operands of exact division (ES3-OPERANDS) ----------------------------------------------


def test_window_values_and_integer_functions_divide_exactly() -> None:
    """LAG / FIRST_VALUE take their argument's kind and scale, SIGN / LOCATE / ASCII are
    integers: they divided as doubles (7.142857142857143 for MySQL's 7.1429). PostgreSQL's
    sign(integer) is a double precision, so SIGN is written as the integer it is."""
    ddl, view = _select(
        "(lag(`t`.`i`) OVER (ORDER BY `t`.`id` )  / 7) AS `a`,(sign(`t`.`i`) / 3) AS `b`,(locate('0',`t`.`s`) / 3) AS `c`,"
        "(ascii(`t`.`s`) / 7) AS `e`,(first_value(`t`.`dec4`) OVER (ORDER BY `t`.`id` )  / 7) AS `g`"
    )
    assert 'DIV(CAST(LAG("t"."i") OVER (ORDER BY "t"."id" NULLS FIRST) AS DECIMAL) * 1000000000' in ddl
    assert 'DIV(CAST(CAST(SIGN("t"."i") AS INT) AS DECIMAL) * 1000000000' in ddl
    assert "DIV(CAST(POSITION('0' IN \"t\".\"s\") AS DECIMAL)" in ddl and 'DIV(CAST(ASCII("t"."s") AS DECIMAL)' in ddl
    assert 'ROUND((DIV(FIRST_VALUE("t"."dec4") OVER (ORDER BY "t"."id" NULLS FIRST) * 1000000000, NULLIF(7, 0))' in ddl
    assert "DOUBLE PRECISION" not in ddl and view.warnings == []


# --- An interval written first (ES21-IVL-FIRST) ---------------------------------------------


def test_an_interval_before_the_date_is_read_as_mysql_reads_it() -> None:
    """INTERVAL '1 2' DAY_HOUR + dt kept sqlglot's INTERVAL '1 2 DAY_HOUR' (refused), INTERVAL
    n DAY + d became INTERVAL 'n DAY': the date first, as MySQL stores a view's."""
    sql, result = _query("SELECT INTERVAL '1 2' DAY_HOUR + dt AS a, INTERVAL n DAY + d AS b FROM t")
    assert sql == "SELECT dt + INTERVAL '1 day 2 hour' AS a, CAST(d + n * INTERVAL '1 DAY' AS DATE) AS b FROM t"
    assert result.classification is Classification.AUTO


# --- MOD / DIV (MOD-ZERO, MOD-PRECEDENCE) ---------------------------------------------------


def test_mod_and_div_by_zero_are_null_and_a_float_mod_is_reported() -> None:
    """MySQL's a % 0, MOD(a, 0) and a DIV 0 are NULL; Aurora DSQL raised division by zero.
    MOD of a DOUBLE is C's fmod, which Aurora DSQL has no operator for."""
    ddl, view = _select(
        "mod(`t`.`i`,`t`.`n`) AS `m`,(`t`.`i` % `t`.`n`) AS `p`,(`t`.`i` DIV `t`.`n`) AS `q`,(`t`.`i` % 3) AS `l`"
    )
    assert '"t"."i" % NULLIF("t"."n", 0) AS "m"' in ddl and '("t"."i" % NULLIF("t"."n", 0)) AS "p"' in ddl
    assert '(DIV("t"."i", NULLIF("t"."n", 0))) AS "q"' in ddl and '("t"."i" % 3) AS "l"' in ddl
    assert view.warnings == []
    _ddl, view = _select("(`t`.`db` % 2) AS `m`")
    assert "C's fmod" in _messages(view.warnings) and view.warnings[0].classification is Classification.MANUAL


def test_mod_binds_as_division_does() -> None:
    """sqlglot bound % as + and -: a % 3 / 7 was a % (3 / 7) -- computed and shown so -- and
    a + b % c (a + b) % c."""
    from dsql_migrator.core.converter import MYSQL_EXPRESSION_DIALECT

    tree = sqlglot.parse_one("SELECT a % 3 / 7, a + b % c", read=MYSQL_EXPRESSION_DIALECT)
    first, second = tree.expressions
    assert isinstance(first, exp.Div) and isinstance(first.this, exp.Mod)
    assert isinstance(second, exp.Add) and isinstance(second.expression, exp.Mod)
    sql, _ = _query("SELECT dec4 % 3 / 7 AS a FROM t")
    assert sql == "SELECT ROUND(DIV(dec4 % 3 * 1000000000, NULLIF(7, 0)) * 0.000000001, 8) AS a FROM t"


# --- TIMESTAMP literals (TIMESTAMP-LITERAL) -------------------------------------------------


def test_a_timestamp_literal_is_a_datetime() -> None:
    """MySQL's TIMESTAMP '...' is a DATETIME, written CAST('...' AS TIMESTAMPTZ) -- a zoned
    value shown with the session's offset. One with an offset is the session's wall clock."""
    sql, _ = _query("SELECT TIMESTAMP '2024-01-31 10:00:00' AS a, TIMESTAMP '2024-01-31 10:00:00+05:00' AS b FROM t")
    assert sql == (
        "SELECT CAST('2024-01-31 10:00:00' AS TIMESTAMP) AS a, "
        "CAST(CAST('2024-01-31 10:00:00+05:00' AS TIMESTAMPTZ) AS TIMESTAMP) AS b FROM t"
    )
    ddl, _ = _view("select `t`.`id` AS `id`,TIMESTAMP'2024-01-31 10:00:00' AS `a` from `shop`.`t` `t`")
    assert "CAST('2024-01-31 10:00:00' AS TIMESTAMP) AS \"a\"" in ddl


# --- Derived tables and CTEs, end to end (G106V-F2 / P5-DERIVED) ----------------------------


def test_a_derived_or_cte_quotient_is_shown_through_the_real_resolver() -> None:
    """Settled by G107's typing on the merged tree: a mergeable derived table's or CTE's
    quotient is shown at the statement's output, and what is computed from it -- named as
    MySQL names it (x.q is q)."""
    sql, result = _query("SELECT x.id, x.q, x.q * 3 AS q3 FROM (SELECT id, i / n AS q FROM t) x")
    assert sql.startswith("SELECT x.id, ROUND(x.q, 4) AS q, ROUND(x.q * 3, 4) AS q3 FROM") and result.warnings == []
    sql, _ = _query("WITH c AS (SELECT id, i / n AS q FROM t) SELECT c.id, MAX(c.q) * 3 AS m FROM c GROUP BY c.id")
    assert "SELECT c.id, ROUND(MAX(c.q) * 3, 4) AS m FROM c" in sql


def test_a_view_mysql_materializes_shows_a_derived_quotient() -> None:
    ddl, view = _view(
        "select `x`.`id` AS `id`,max(`x`.`q`) AS `m`,(max(`x`.`q`) * 7) AS `m7` from (select `o`.`id` AS `id`,"
        "(`o`.`i` / 7) AS `q` from `shop`.`t` `o`) `x` group by `x`.`id`"
    )
    assert 'ROUND(MAX("x"."q"), 4) AS "m", ROUND((MAX("x"."q") * 7), 4) AS "m7"' in ddl and view.warnings == []
