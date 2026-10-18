"""G118: the final regression round on G112-G117, the numbers -- a quantified comparison
(ANY / SOME / ALL) and an IN's subquery are sets, never values to cast; a named WINDOW
converts, and the Query Converter never raises; MySQL's IF() that is a simple CASE's ELSE
or operand keeps its condition; a UNION column of a FLOAT is a DOUBLE only where MySQL makes
it one; a number INSERTed into a TINYINT(1) is the boolean (into a TINYINT(1) UNSIGNED, a
smallint on Aurora DSQL, the number); an unqualified read of a
derived table's or a CTE's quotient is shown and keyed as a qualified one; % / DIV inside a
FLOAT SUM divides by NULLIF(b, 0); a CASE a WITH ROLLUP groups by is shown per row; SET col
= a = b assigns (a = b). Each form was run on MySQL 8.4 and PostgreSQL 17 over the same rows
(scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import re

import pytest

from dsql_migrator.core.models import Classification, ColumnDef, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="a", mysql_type="int"),
        ColumnDef(name="b", mysql_type="int"),
        ColumnDef(name="qty", mysql_type="int"),
        ColumnDef(name="g", mysql_type="int"),
        ColumnDef(name="f", mysql_type="float"),
        ColumnDef(name="f2", mysql_type="float"),
        ColumnDef(name="db", mysql_type="double"),
        ColumnDef(name="dec2", mysql_type="decimal(10,2)"),
        ColumnDef(name="paid", mysql_type="tinyint(1)"),
        ColumnDef(name="ti", mysql_type="tinyint"),
        ColumnDef(name="si", mysql_type="smallint"),
        ColumnDef(name="mi", mysql_type="mediumint"),
        ColumnDef(name="big", mysql_type="bigint"),
        ColumnDef(name="yr", mysql_type="year"),
        ColumnDef(name="iu", mysql_type="int unsigned"),
        ColumnDef(name="pu", mysql_type="tinyint(1) unsigned"),
    ],
    primary_key=["id"],
)
# MySQL's a / b as written: its 9 kept digits, truncated.
_Q = "DIV(CAST(a AS DECIMAL) * 1000000000, NULLIF(b, 0)) * 0.000000001"
_KEY_NOTE = "A quotient (an AVG) is a GROUP BY, PARTITION BY, window ORDER BY or COUNT(DISTINCT"


def _norm(sql: str) -> str:
    """SQL with its whitespace collapsed and none just inside a parenthesis."""
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=[_T])
    return _norm(view.target_ddl), view


def _query(sql: str):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    result = QueryConverter(column_types=tuple(table_type_index([_T]).values())).convert(sql)
    return _norm(result.converted_sql or ""), result


# --- ANY / SOME / ALL and an IN's subquery (F112-ANYALL / G112R-1) --------------------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        ("SELECT id FROM t WHERE qty = ANY (SELECT qty FROM t WHERE id < 3)", "qty = ANY (SELECT qty FROM t"),
        ("SELECT id FROM t WHERE qty > ALL (SELECT qty FROM t WHERE id IN (2, 3))", "qty > ALL (SELECT qty FROM t"),
        ("SELECT id FROM t WHERE a = SOME (SELECT qty FROM t)", "a = ANY(SELECT qty FROM t)"),
        ("SELECT id FROM t WHERE qty IN (SELECT paid FROM t)", "qty IN (SELECT paid FROM t)"),
    ],
)
def test_a_quantified_comparison_is_a_set_never_a_value(sql: str, written: str) -> None:
    """sqlglot's ANY / ALL are predicates, so the comparison-as-number pass cast them beside
    the number they quantify: qty > CAST(ALL (SELECT ...) AS INT), a syntax error graded
    AUTO (5d955cc wrote them as they were). An IN's subquery is the set it reads too
    (qty IN CAST((SELECT ...) AS INT)); one of booleans still fails loudly, as on 5d955cc."""
    out, result = _query(sql)
    assert written in out and "CAST(" not in out
    assert result.classification is Classification.AUTO


def test_a_quantified_comparison_read_as_a_number_is_still_its_integer() -> None:
    """The comparison itself is a value -- (qty > ALL (...)) + 1 -- and so is EXISTS."""
    out, _ = _query("SELECT id FROM t WHERE (qty > ALL (SELECT qty FROM t WHERE id = 2)) + 1 = 2")
    assert "(CAST(qty > ALL (SELECT qty FROM t WHERE id = 2) AS INT)) + 1 = 2" in out
    out, _ = _query("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM t AS u WHERE u.id = 1) + 0 = 1")
    assert "CAST(EXISTS(SELECT 1 FROM t AS u WHERE u.id = 1) AS INT) + 0 = 1" in out


def test_a_view_keeps_its_quantified_comparison() -> None:
    """MySQL stores >= ALL, > ANY and < ANY as written (= ANY as IN, <> ALL as NOT IN)."""
    ddl, view = _view(
        "select `t`.`id` AS `id` from `shop`.`t` `t` where (`t`.`qty` >= all (select `u`.`qty` from "
        "`shop`.`t` `u` where (`u`.`qty` is not null)))"
    )
    assert '("t"."qty" >= ALL (SELECT "u"."qty" FROM "shop"."t" AS "u"' in ddl
    assert view.auto_converted and view.warnings == []


# --- A named WINDOW (ES112-1 / G112R-9) -----------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        (
            "SELECT id, ROW_NUMBER() OVER w AS rn FROM t WINDOW w AS (ORDER BY id)",
            "SELECT id, ROW_NUMBER() OVER w AS rn FROM t WINDOW w AS (ORDER BY id NULLS FIRST)",
        ),
        (
            "SELECT id, SUM(qty) OVER w AS s FROM t WINDOW w AS (PARTITION BY g)",
            "SELECT id, SUM(qty) OVER w AS s FROM t WINDOW w AS (PARTITION BY g)",
        ),
        (
            "SELECT id, SUM(a / b) OVER w AS s FROM t WINDOW w AS (PARTITION BY g)",
            f"SELECT id, SUM(ROUND({_Q}, 4)) OVER w AS s FROM t WINDOW w AS (PARTITION BY g)",
        ),
        (
            "SELECT id, AVG(qty) OVER w AS m FROM t WINDOW w AS (PARTITION BY g ORDER BY id)",
            "NULLIF(COUNT(qty) OVER w, 0)) * 0.000000001 AS DECIMAL), 4) AS m FROM t WINDOW w AS (PARTITION BY g",
        ),
    ],
)
def test_a_named_window_converts(sql: str, written: str) -> None:
    """The window loops read the WINDOW clause's own definition -- a Window whose this is
    the name w -- as a function, and raised TypeError out of QueryConverter.convert (the
    Query Playground's Convert crashed). bcb86bd converted these."""
    out, result = _query(sql)
    assert written in out and result.classification is Classification.AUTO


def test_a_named_windows_quotient_key_is_reported() -> None:
    """The definition's PARTITION BY / ORDER BY are keys like an inline window's."""
    _out, result = _query("SELECT id, SUM(qty) OVER w AS s FROM t WINDOW w AS (PARTITION BY a / b)")
    assert result.classification is Classification.MANUAL
    assert _KEY_NOTE in " ".join(w.message for w in result.warnings)


def test_a_view_with_a_named_window_converts() -> None:
    """The same crash made such a view MANUAL ('could not be automatically converted')."""
    ddl, view = _view(
        "select `t`.`id` AS `id`,row_number() OVER `w` AS `rn` from `shop`.`t` `t` window `w` AS "
        "(ORDER BY `t`.`id` )"
    )
    assert 'ROW_NUMBER() OVER "w" AS "rn" FROM "shop"."t" AS "t" WINDOW "w" AS (ORDER BY "t"."id"' in ddl
    assert view.auto_converted and view.warnings == []


def test_a_window_over_a_call_by_name_converts() -> None:
    """JSON_ARRAYAGG(x) OVER (...) is an Anonymous call, whose this is its name: the same
    read raised TypeError (bcb86bd converted it)."""
    out, result = _query("SELECT JSON_ARRAYAGG(qty) OVER (PARTITION BY g) AS j FROM t")
    assert out == "SELECT JSON_AGG(qty) OVER (PARTITION BY g) AS j FROM t"
    assert result.classification is Classification.AUTO


def test_the_query_converter_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Any failure inside the conversion is a MANUAL result with no converted SQL, as a
    view's is -- never an exception the Query Playground's Convert would show raw."""
    from dsql_migrator.core import query_converter
    from dsql_migrator.core.query_converter import CODE_RENDER_ERROR, QueryConverter

    def broken(*_args, **_kwargs):
        raise TypeError("must be str, not type")

    monkeypatch.setattr(query_converter, "rewrite_mysql_expressions", broken)
    result = QueryConverter().convert("SELECT 1")
    assert result.classification is Classification.MANUAL and result.converted_sql is None
    assert [w.code for w in result.warnings] == [CODE_RENDER_ERROR]
    assert "TypeError" in result.warnings[0].message and result.original_sql == "SELECT 1"


# --- MySQL's IF() as a simple CASE's ELSE or operand (G112R-2) ------------------------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        (
            "SELECT id, CASE qty WHEN 3 THEN 'three' ELSE IF(qty > 2, 'big', 'small') END AS k FROM t",
            "CASE qty WHEN 3 THEN 'three' ELSE CASE WHEN qty > 2 THEN 'big' ELSE 'small' END END AS k",
        ),
        (
            "SELECT id, CASE qty WHEN 3 THEN 'three' ELSE IF(paid, 'paid', 'unpaid') END AS k FROM t",
            "CASE qty WHEN 3 THEN 'three' ELSE CASE WHEN paid THEN 'paid' ELSE 'unpaid' END END AS k",
        ),
        (
            "SELECT id, CASE IF(paid, 1, 0) WHEN 1 THEN 'y' ELSE 'n' END AS k FROM t",
            "CASE CASE WHEN paid THEN 1 ELSE 0 END WHEN 1 THEN 'y' ELSE 'n' END AS k",
        ),
    ],
)
def test_an_if_beside_a_simple_case_keeps_its_condition(sql: str, written: str) -> None:
    """MySQL's IF() is an If node too: as a simple CASE's ELSE or operand it was taken for a
    WHEN branch, its condition cast beside the CASE's operand -- CASE WHEN CAST(qty > 2 AS
    INT), which Aurora DSQL rejects ('argument of CASE/WHEN must be type boolean'), graded
    AUTO. 5d955cc kept it."""
    out, result = _query(sql)
    assert written in out and "CAST(" not in out and result.classification is Classification.AUTO


def test_a_simple_cases_when_value_and_an_ifs_branch_are_still_numbers() -> None:
    out, _ = _query("SELECT id, CASE qty WHEN paid THEN 'y' ELSE 'n' END AS k FROM t")
    assert "CASE qty WHEN CAST(paid AS INT) THEN 'y' ELSE 'n' END AS k" in out
    # The IF's own branches beside each other: paid beside 5 (once read beside the CASE's).
    out, _ = _query("SELECT id, CASE qty WHEN 3 THEN 9 ELSE IF(qty > 2, paid, 5) END AS k FROM t")
    assert "ELSE CASE WHEN qty > 2 THEN CAST(paid AS INT) ELSE 5 END END AS k" in out


# --- A UNION column of a FLOAT (F112-UNIONFLOAT / G112R-3 / ES112-8) ------------------------


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT f AS q FROM t UNION ALL SELECT 1",
        "SELECT f AS q FROM t UNION ALL SELECT big FROM t",
        "SELECT f AS q FROM t UNION ALL SELECT COUNT(*) FROM t",
        "SELECT f AS q FROM t UNION ALL SELECT ti FROM t",
        "SELECT si AS q FROM t UNION ALL SELECT f FROM t",
        "SELECT f AS q FROM t UNION ALL SELECT mi FROM t",
        "SELECT f AS q FROM t UNION ALL SELECT yr FROM t",
        "SELECT ti AS q FROM t UNION ALL SELECT f FROM t UNION ALL SELECT 1",
        "SELECT f AS q FROM t UNION ALL SELECT MAX(big) FROM t",
    ],
)
def test_a_union_column_mysql_keeps_a_float_is_left_real(sql: str) -> None:
    """MySQL 8.4 types FLOAT UNION a TINYINT / SMALLINT / MEDIUMINT / YEAR column a FLOAT,
    and so FLOAT UNION an integer literal, a BIGINT or COUNT(*) after it: cast to double
    precision, SELECT f ... UNION ALL SELECT 0 showed 0.10000000149011612 where MySQL (and
    5d955cc) shows 0.1."""
    out, result = _query(sql)
    assert "DOUBLE PRECISION" not in out and result.classification is Classification.AUTO


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        ("SELECT f AS q FROM t UNION ALL SELECT a FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
        ("SELECT a AS q FROM t UNION ALL SELECT f FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS f FROM t"),
        ("SELECT f AS q FROM t UNION ALL SELECT iu FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
        ("SELECT f AS q FROM t UNION ALL SELECT db FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
        ("SELECT f AS q FROM t UNION ALL SELECT dec2 FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
        ("SELECT f AS q FROM t UNION ALL SELECT a / b FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
        ("SELECT 1 AS q UNION ALL SELECT f FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS f FROM t"),
        ("SELECT big AS q FROM t UNION ALL SELECT f FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS f FROM t"),
        ("SELECT ti AS q FROM t UNION ALL SELECT 1 UNION ALL SELECT f FROM t", "SELECT CAST(f AS DOUBLE PRECISION)"),
        ("SELECT f AS q FROM t UNION ALL SELECT SUM(a) FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
        ("SELECT f AS q FROM t UNION ALL SELECT MAX(a) FROM t", "SELECT CAST(f AS DOUBLE PRECISION) AS q FROM t"),
    ],
)
def test_a_union_column_mysql_makes_a_double_casts_its_float(sql: str, written: str) -> None:
    """A DOUBLE, a DECIMAL (a quotient, MySQL's exact SUM) or an INT column (its MAX too)
    beside it, wherever they stand -- and an integer literal, a BIGINT or COUNT(*) before the
    FLOAT -- make MySQL's column a DOUBLE, which Aurora DSQL types real."""
    out, _ = _query(sql)
    assert written in out


def test_a_views_union_of_a_float_and_a_literal_follows_the_branch_order() -> None:
    ddl, _ = _view("select `t`.`f` AS `q` from `shop`.`t` `t` union all select 0 AS `0`")
    assert "DOUBLE PRECISION" not in ddl
    ddl, _ = _view("select 0 AS `q` union all select `t`.`f` AS `f` from `shop`.`t` `t`")
    assert 'SELECT CAST("t"."f" AS DOUBLE PRECISION) AS "f"' in ddl


# --- A number INSERTed into a TINYINT(1) (ES112-3) ------------------------------------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        (
            "INSERT INTO t (id, paid) VALUES (100, 1), (101, 0), (102, -1)",
            "INSERT INTO t (id, paid) VALUES (100, TRUE), (101, FALSE), (102, TRUE)",
        ),
        ("INSERT INTO t (paid, id) VALUES (1, 104)", "INSERT INTO t (paid, id) VALUES (TRUE, 104)"),
        ("INSERT INTO t (id, paid) VALUES (103, 2 - 2)", "INSERT INTO t (id, paid) VALUES (103, (2 - 2) <> 0)"),
        ("INSERT INTO t (id, paid) SELECT id + 300, 1 FROM t", "INSERT INTO t (id, paid) SELECT id + 300, TRUE FROM t"),
        (
            "INSERT INTO t (id, paid) SELECT 500, CASE WHEN a > 1 THEN 1 ELSE 0 END FROM t",
            "SELECT 500, CASE WHEN a > 1 THEN 1 ELSE 0 END <> 0 FROM t",
        ),
        ("INSERT INTO t (id, paid) SELECT id, qty AS paid FROM t", "SELECT id, qty <> 0 AS paid FROM t"),
        (
            "INSERT INTO t (id, paid) SELECT 700, 1 UNION ALL SELECT 701, 0",
            "SELECT 700, TRUE UNION ALL SELECT 701, FALSE",
        ),
        (
            "INSERT INTO t (id, paid) VALUES (1, 0) ON DUPLICATE KEY UPDATE paid = 1",
            "VALUES (1, FALSE) ON CONFLICT DO UPDATE SET paid = TRUE",
        ),
    ],
)
def test_a_number_inserted_into_a_tinyint1_is_the_boolean(sql: str, written: str) -> None:
    """The way a flag is most often written: INSERT ... VALUES (100, 1) failed on Aurora
    DSQL ('column "paid" is of type boolean but expression is of type integer') while graded
    AUTO, as INSERT ... SELECT did; PostgreSQL has no cast from an integer to a boolean. Each
    value is mapped to its column by the INSERT's list, as UPDATE SET paid = 1 is written."""
    out, _ = _query(sql)
    assert written in out


def test_an_insert_without_a_column_list_reads_the_tables_columns_in_order() -> None:
    out, _ = _query("INSERT INTO t VALUES (105, 1, 2, 3, 4, 0.5, 0.5, 1.5, 2.25, 1, 1, 1, 1, 1, 2024, 1, 1)")
    assert out == "INSERT INTO t VALUES (105, 1, 2, 3, 4, 0.5, 0.5, 1.5, 2.25, TRUE, 1, 1, 1, 1, 2024, 1, 1)"


def test_an_unsigned_tinyint1_is_a_number() -> None:
    """A TINYINT(1) UNSIGNED is a smallint on Aurora DSQL (Schema Conversion widens it), so
    a number stored in it stays one: SET pu = TRUE failed there ('column "pu" is of type
    smallint but expression is of type boolean')."""
    out, _ = _query("UPDATE t SET pu = 1, paid = 1 WHERE id = 2")
    assert out == "UPDATE t SET pu = 1, paid = TRUE WHERE id = 2"
    out, _ = _query("INSERT INTO t (id, pu, paid) VALUES (1, 1, 1)")
    assert out == "INSERT INTO t (id, pu, paid) VALUES (1, 1, TRUE)"


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t (id, paid) VALUES (900, TRUE), (901, 1 > 0), (902, NULL)",
        "INSERT INTO t (id, paid) SELECT id, paid FROM t",
        "INSERT INTO t (id, qty) VALUES (106, 1)",
        "INSERT INTO other (id, paid) VALUES (1, 1)",
    ],
)
def test_an_insert_of_a_boolean_or_into_another_column_is_left(sql: str) -> None:
    """A boolean into the flag, a number into a number, and a table the inventory cannot
    type stay as written."""
    out, _ = _query(sql)
    assert out == sql


# --- An unqualified read of a derived table's or a CTE's quotient (ES112-6) ----------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        ("WITH c AS (SELECT id, a / b AS q FROM t) SELECT id, q FROM c", "SELECT id, ROUND(q, 4) AS q FROM c"),
        ("WITH c AS (SELECT id, a / b AS q FROM t) SELECT SUM(q) AS s FROM c", "SELECT ROUND(SUM(q), 4) AS s FROM c"),
        ("SELECT q * 3 AS m FROM (SELECT a / b AS q FROM t) AS x", "SELECT ROUND(q * 3, 4) AS m FROM"),
    ],
)
def test_an_unqualified_derived_quotient_is_shown_at_its_scale(sql: str, written: str) -> None:
    """Only a qualified read (c.q, x.q) was told from a table's column: q showed every digit
    kept, 0.333333333 where MySQL shows 0.3333, and SUM(q) 4.499999999 for 4.5000."""
    out, result = _query(sql)
    assert written in out and result.classification is Classification.AUTO


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT q, COUNT(*) AS n FROM (SELECT a / b AS q FROM t) AS x GROUP BY q",
        "SELECT x.id, COUNT(*) OVER (PARTITION BY q) AS c FROM (SELECT id, a / b AS q FROM t) AS x",
    ],
)
def test_an_unqualified_derived_quotient_key_is_reported(sql: str) -> None:
    _out, result = _query(sql)
    assert result.classification is Classification.MANUAL
    assert _KEY_NOTE in " ".join(w.message for w in result.warnings)


def test_an_unqualified_derived_column_that_is_no_quotient_is_left() -> None:
    out, _ = _query("SELECT id, q FROM (SELECT id, dec2 AS q FROM t) AS x")
    assert out == "SELECT id, q FROM (SELECT id, dec2 AS q FROM t) AS x"


# --- % / DIV inside a FLOAT SUM (G112R-5) ---------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        ("SELECT SUM(IF(a % b = 0, f, f2)) AS s FROM t", "SUM(CAST(CASE WHEN a % NULLIF(b, 0) = 0 THEN f"),
        ("SELECT SUM(IF(a DIV b = 1, f, f2)) AS s FROM t", "SUM(CAST(CASE WHEN DIV(a, NULLIF(b, 0)) = 1 THEN f"),
        ("SELECT SUM(DISTINCT IF(MOD(a, b) = 0, f, f2)) AS s FROM t", "SUM(DISTINCT CAST(CASE WHEN a % NULLIF(b, 0)"),
    ],
)
def test_a_remainder_inside_a_float_sum_is_null_for_a_zero_divisor(sql: str, written: str) -> None:
    """The FLOAT SUM pass cast a copy of its argument, so the MOD / DIV pass after it -- over
    the nodes as walked -- guarded the detached original: division by zero on Aurora DSQL,
    where MySQL's remainder is NULL."""
    out, _ = _query(sql)
    assert written in out


# --- A CASE a WITH ROLLUP groups by (ES112-9) -----------------------------------------------


@pytest.mark.parametrize("key", ["1", "unit"])
def test_a_case_a_rollup_groups_by_is_shown_per_row(key: str) -> None:
    """MySQL computes GROUP BY ... WITH ROLLUP by sorting, each row's CASE as itself (0 --
    5d955cc showed it so); kept whole it was padded, 0.0000."""
    out, _ = _query(
        f"SELECT CASE WHEN b > 3 THEN a / b ELSE 0 END AS unit, COUNT(*) AS c FROM t GROUP BY {key} WITH ROLLUP"
    )
    assert out.startswith(f"SELECT CASE WHEN b > 3 THEN ROUND({_Q}, 4) ELSE 0 END AS unit")


def test_a_case_a_rollup_groups_by_as_itself_stays_whole() -> None:
    """GROUP BY the same CASE: Aurora DSQL matches the select list's item only with the
    whole of it, so it stays whole (per branch the statement failed: 'column "t.b" must
    appear in the GROUP BY clause'); without ROLLUP an ordinal key is kept whole as before."""
    case = f"CASE WHEN b > 3 THEN {_Q} ELSE 0 END"
    out, _ = _query(
        "SELECT CASE WHEN b > 3 THEN a / b ELSE 0 END AS unit, COUNT(*) AS c FROM t "
        "GROUP BY CASE WHEN b > 3 THEN a / b ELSE 0 END WITH ROLLUP"
    )
    assert out.startswith(f"SELECT ROUND({case}, 4) AS unit")
    out, _ = _query("SELECT CASE WHEN b > 3 THEN a / b ELSE 0 END AS unit, COUNT(*) AS c FROM t GROUP BY 1")
    assert out.startswith(f"SELECT ROUND({case}, 4) AS unit")


# --- SET col = a = b (ES112-4) --------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "written"),
    [
        ("UPDATE t SET paid = a = 1", "UPDATE t SET paid = a = 1"),
        ("UPDATE t SET paid = a <=> 2", "UPDATE t SET paid = a IS NOT DISTINCT FROM 2"),
        ("UPDATE t SET paid = a <> 1", "UPDATE t SET paid = a <> 1"),
        ("UPDATE t SET qty = a <=> 1", "UPDATE t SET qty = CAST(a IS NOT DISTINCT FROM 1 AS INT)"),
        ("UPDATE t SET qty = a = 1", "UPDATE t SET qty = CAST(a = 1 AS INT)"),
        ("UPDATE t SET qty = a = 1 = 0", "UPDATE t SET qty = CAST(CAST(a = 1 AS INT) = 0 AS INT)"),
        ("UPDATE t AS x SET x.paid = x.a = 1 WHERE x.id = 2", "UPDATE t AS x SET paid = x.a = 1 WHERE x.id = 2"),
        (
            "INSERT INTO t (id, qty) VALUES (1, 5) ON DUPLICATE KEY UPDATE qty = a = 1",
            "ON CONFLICT DO UPDATE SET qty = CAST(a = 1 AS INT)",
        ),
    ],
)
def test_set_col_eq_a_eq_b_assigns_the_comparison(sql: str, written: str) -> None:
    """sqlglot reads the equalities left to right, ((paid = a) = 1), so the target was an
    operand: SET CAST(paid AS INT) = a = TRUE, a syntax error graded AUTO, and SET qty = a =
    1 a boolean into an integer. MySQL assigns (a = 1)."""
    out, result = _query(sql)
    assert written in out and "CAST(paid" not in out and "CAST(qty" not in out
    assert result.classification is (Classification.MANUAL if "DUPLICATE" in sql else Classification.AUTO)


def test_set_items_mysql_reads_otherwise_are_left() -> None:
    out, _ = _query("UPDATE t SET paid = (a <=> 2), qty = a < 1, g = 2")
    assert out == "UPDATE t SET paid = (a IS NOT DISTINCT FROM 2), qty = CAST(a < 1 AS INT), g = 2"
