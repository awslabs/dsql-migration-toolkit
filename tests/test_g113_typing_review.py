"""G113: the review of G106-G111, typing -- a choice MySQL makes a string written as MySQL
reads it where it stands (a number in arithmetic, a date in a date function, text where it
is shown), a TIME beside a time string, a TIME beside a date as MySQL's DATETIME, ->> as
MySQL reads it (a JSON null in a date function, a grouped one, compared with text, numeric
functions, nested [0] steps, a document of unknown type), a JSON value in arithmetic, `col
-> '$.a'` inside a function's arguments, BINARY keys and comparisons by what they hold,
implicit text conversion for the rest of the string functions, a recursive CTE's column by
its first SELECT's type, GREATEST / LEAST of numbers and strings, a UNION of numbers and
strings, a db-qualified column never a derived table's, and the resolver's computed columns
typed once. Each form was run on MySQL 8.4 and PostgreSQL 17 over the same rows
(scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import re
import time

import pytest
import sqlglot

from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="n", mysql_type="int"),
        ColumnDef(name="price", mysql_type="decimal(10,2)"),
        ColumnDef(name="f", mysql_type="float"),
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="dt", mysql_type="datetime"),
        ColumnDef(name="ts", mysql_type="timestamp"),
        ColumnDef(name="tm", mysql_type="time"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="b", mysql_type="blob"),
        ColumnDef(name="flag", mysql_type="tinyint(1)"),
        ColumnDef(name="j", mysql_type="json"),
    ],
    primary_key=["id"],
)
_U = TableDef(
    name="shop.u",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="n", mysql_type="int"),
    ],
    primary_key=["id"],
)
_TABLES = [_T, _U]


def _norm(sql: str) -> str:
    """SQL with its whitespace collapsed and none just inside a parenthesis."""
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=_TABLES)
    return _norm(view.target_ddl), [w.message for w in view.warnings]


def _select(select_list: str, *, where: str = "", tail: str = ""):
    return _view(f"select {select_list} from `shop`.`t` `t`" + (f" where {where}" if where else "") + tail)


def _query(sql: str, *, typed: bool = True):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    types = tuple(table_type_index(_TABLES).values()) if typed else ()
    result = QueryConverter(column_types=types).convert(sql)
    return _norm(result.converted_sql or ""), result


# --- A choice MySQL makes a string, as MySQL reads it ------------------------------------


def test_a_number_beside_a_string_read_as_a_number_is_the_double_mysql_reads() -> None:
    """RH-1 / T2: IFNULL(n, '0') + 1 was COALESCE(CAST(n AS TEXT), '0') + 1 -- text + integer,
    sum(text), text > integer refused while graded AUTO (bcb86bd ran them). MySQL computes a
    string beside a number in double precision; '' and 'n/a' read as 0."""
    ddl, notes = _select(
        "(ifnull(`t`.`n`,'0') + 1) AS `a`,(ifnull(`t`.`price`,'') * 2) AS `b`,"
        "(coalesce(`t`.`n`,'-1.5') + 0) AS `c`",
        where="(ifnull(`t`.`n`,'0') > 1)",
    )
    assert '(COALESCE(CAST("t"."n" AS DOUBLE PRECISION), 0) + 1) AS "a"' in ddl
    assert '(COALESCE(CAST("t"."price" AS DOUBLE PRECISION), 0) * 2) AS "b"' in ddl
    assert 'COALESCE(CAST("t"."n" AS DOUBLE PRECISION), -1.5) + 0' in ddl
    assert 'WHERE (COALESCE(CAST("t"."n" AS DOUBLE PRECISION), 0) > 1)' in ddl and notes == []
    ddl, _ = _select("sum(ifnull(`t`.`n`,'0')) AS `s`,avg(if(`t`.`flag`,`t`.`price`,'0')) AS `a`")
    assert 'SUM(COALESCE(CAST("t"."n" AS DOUBLE PRECISION), 0))' in ddl
    assert 'AVG(CASE WHEN "t"."flag" THEN CAST("t"."price" AS DOUBLE PRECISION) ELSE 0 END)' in ddl
    sql, result = _query("UPDATE t SET n = IFNULL(n, '0') + 1 WHERE id = 1")
    assert sql == "UPDATE t SET n = COALESCE(CAST(n AS DOUBLE PRECISION), 0) + 1 WHERE id = 1"
    assert result.classification.value == "AUTO"


def test_a_number_beside_a_string_in_mod_a_simple_case_and_in_is_read_as_a_number() -> None:
    ddl, _ = _select(
        "(ifnull(`t`.`n`,'0') % 3) AS `m`,(case ifnull(`t`.`n`,'0') when 3 then 'x' else 'y' end) AS `c`,"
        "(ifnull(`t`.`n`,'0') in (3,12)) AS `i`,abs(ifnull(`t`.`n`,'-3')) AS `a`"
    )
    assert '(COALESCE(CAST("t"."n" AS DECIMAL), 0) % 3)' in ddl  # double precision % integer does not exist
    assert 'CASE COALESCE(CAST("t"."n" AS DOUBLE PRECISION), 0) WHEN 3' in ddl
    assert 'COALESCE(CAST("t"."n" AS DOUBLE PRECISION), 0) IN (3, 12)' in ddl
    assert 'ABS(COALESCE(CAST("t"."n" AS DOUBLE PRECISION), -3))' in ddl


def test_a_number_beside_a_string_ordered_or_in_json_stays_mysqls_string() -> None:
    """MySQL orders IFNULL(n, '0') as strings (12 before 3), takes their MAX as a string and
    writes it in JSON as a string: 5d955cc's text was right there. (G119: shown, beside a
    number string, it is PostgreSQL's number -- MySQL's text, and a number to the query
    that reads a derived table's or a view's column: test_g119_typing_review.)"""
    ddl, _ = _select(
        "json_array(ifnull(`t`.`n`,'0')) AS `j`,ifnull(`t`.`n`,'0') AS `k`",
        tail=" order by ifnull(`t`.`n`,'0')",
    )
    assert "JSON_BUILD_ARRAY(COALESCE(CAST(\"t\".\"n\" AS TEXT), '0'))" in ddl
    assert "COALESCE(\"t\".\"n\", '0') AS \"k\"" in ddl
    assert "ORDER BY COALESCE(CAST(\"t\".\"n\" AS TEXT), '0')" in ddl


def test_a_time_beside_a_time_string_is_a_time_where_it_is_read_as_one() -> None:
    """RH-2: HOUR(IFNULL(tm, '00:00:00')) was EXTRACT(HOUR FROM <text>) -- refused while AUTO.
    Shown, MySQL writes the string as given ('00:00')."""
    ddl, notes = _select("hour(ifnull(`t`.`tm`,'00:00:00')) AS `h`,coalesce(`t`.`tm`,'00:00') AS `x`")
    assert "EXTRACT(HOUR FROM COALESCE(\"t\".\"tm\", '00:00:00'))" in ddl
    assert "COALESCE(CAST(\"t\".\"tm\" AS TEXT), '00:00') AS \"x\"" in ddl and notes == []


def test_a_date_beside_a_string_is_mysqls_string_where_it_is_shown() -> None:
    """T5 / T6 / RH-7 / S9: COALESCE(dt, '2024-06-01') showed '2024-06-01 00:00:00' for the
    literal's rows; COALESCE(d, '... 10:00:00') every DATE row with ' 00:00:00', in JSON too."""
    ddl, notes = _select(
        "coalesce(`t`.`dt`,'2024-06-01') AS `a`,coalesce(`t`.`d`,'2024-06-01 10:00:00') AS `b`,"
        "json_array(ifnull(`t`.`d`,'2024-01-01 10:00:00')) AS `j`,concat('d=',coalesce(`t`.`dt`,'2024-06-01')) AS `c`"
    )
    assert "COALESCE(CAST(\"t\".\"dt\" AS TEXT), '2024-06-01') AS \"a\"" in ddl
    assert "COALESCE(CAST(\"t\".\"d\" AS TEXT), '2024-06-01 10:00:00') AS \"b\"" in ddl
    assert "JSON_BUILD_ARRAY(COALESCE(CAST(\"t\".\"d\" AS TEXT), '2024-01-01 10:00:00'))" in ddl
    assert "'d=' || COALESCE(CAST(\"t\".\"dt\" AS TEXT), '2024-06-01')" in ddl
    assert "TIMESTAMP" not in ddl and notes == []


def test_a_date_beside_a_string_stays_a_date_where_a_date_function_reads_it() -> None:
    ddl, notes = _select(
        "(coalesce(`t`.`d`,'20240601') + interval 1 day) AS `a`,year(ifnull(`t`.`d`,'')) AS `y`",
        where="(coalesce(`t`.`d`,'2024-01-01') > `t`.`dt`)",
    )
    assert "CAST(COALESCE(\"t\".\"d\", '20240601') + INTERVAL '1 DAY' AS DATE)" in ddl
    # '' is no date to MySQL: NULL in YEAR(), where CAST('' AS DATE) failed on every NULL row.
    assert 'EXTRACT(YEAR FROM CAST(COALESCE("t"."d", NULL) AS DATE))' in ddl
    assert "WHERE (COALESCE(\"t\".\"d\", '2024-01-01') > \"t\".\"dt\")" in ddl and notes == []
    # A DATE beside a date with a time in date arithmetic: MySQL's string follows each row.
    ddl, notes = _select("(coalesce(`t`.`d`,'2024-06-01 10:00:00') + interval 1 day) AS `a`")
    assert 'CAST("t"."d" AS TIMESTAMP)' in ddl
    assert any("whose form follows each row's value" in n for n in notes)


def test_a_date_beside_a_string_column_or_a_time_is_typed_as_mysql_types_it() -> None:
    """S9: COALESCE(d, s) -- 'COALESCE types date and character varying cannot be matched' --
    and COALESCE(d, tm), MySQL's DATETIME (the time on the current date), refused while AUTO."""
    ddl, notes = _select("coalesce(`t`.`d`,`t`.`s`) AS `a`,coalesce(`t`.`d`,`t`.`tm`) AS `b`")
    assert 'COALESCE(CAST("t"."d" AS TEXT), "t"."s") AS "a"' in ddl
    assert 'COALESCE("t"."d", CAST((CURRENT_DATE + "t"."tm") AS TIMESTAMP)) AS "b"' in ddl and notes == []


def test_a_datetime_beside_a_timestamp_made_text_carries_no_zone() -> None:
    """S12: COALESCE(dt, ts) is a timestamptz on Aurora DSQL, whose text carries '+00'."""
    ddl, _ = _select("concat(if(`t`.`flag`,`t`.`dt`,`t`.`ts`),'') AS `a`")
    assert "TO_CHAR(CASE WHEN \"t\".\"flag\" THEN \"t\".\"dt\" ELSE \"t\".\"ts\" END, 'YYYY-MM-DD HH24:MI:SS')" in ddl


def test_a_coalesce_of_a_decimal_and_an_integer_is_shown_at_the_decimals_scale() -> None:
    """T7(c) / S12: COALESCE(price, 0) showed '0' where MySQL shows '0.00' -- in JSON MySQL
    writes each value as itself, and an IF / CASE shows the value taken as it is."""
    ddl, _ = _select(
        "coalesce(`t`.`price`,0) AS `a`,ifnull(`t`.`price`,`t`.`n`) AS `b`,if(`t`.`flag`,`t`.`price`,0) AS `c`,"
        "json_array(coalesce(`t`.`price`,0)) AS `j`"
    )
    assert 'COALESCE("t"."price", ROUND(0, 2)) AS "a"' in ddl
    assert 'COALESCE("t"."price", ROUND("t"."n", 2)) AS "b"' in ddl
    assert 'CASE WHEN "t"."flag" THEN "t"."price" ELSE 0 END AS "c"' in ddl
    assert 'JSON_BUILD_ARRAY(COALESCE("t"."price", 0))' in ddl


# --- ->> and JSON values -------------------------------------------------------------------


def test_a_json_null_read_by_a_date_function_is_null() -> None:
    """T1: YEAR / DATE_FORMAT / STR_TO_DATE of a ->> reading a JSON null got the text 'null':
    'invalid input syntax for type date' on every SELECT (MySQL reads it as no date)."""
    ddl, _ = _select(
        "year(json_unquote(json_extract(`t`.`j`,'$.d'))) AS `y`,"
        "date_format(json_unquote(json_extract(`t`.`j`,'$.d')),'%Y-%m') AS `m`,"
        "str_to_date(json_unquote(json_extract(`t`.`j`,'$.d')),'%Y-%m-%d') AS `s`"
    )
    assert "'null'" not in ddl
    assert "EXTRACT(YEAR FROM CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'd') AS DATE))" in ddl
    assert "TO_DATE(NULLIF(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'd'), '')" in ddl


def test_a_grouped_json_text_is_written_as_its_group_by_key_wherever_it_is_read() -> None:
    """RH-10: the cast read the bare text, the GROUP BY key the 'null'-text form: 'column t.j
    must appear in the GROUP BY clause'."""
    ddl, _ = _view(
        "select cast(json_unquote(json_extract(`t`.`j`,'$.a')) as decimal(5,1)) AS `a` from `shop`.`t` `t` "
        "group by json_unquote(json_extract(`t`.`j`,'$.a'))"
    )
    key = (
        "COALESCE(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'a'), CASE WHEN NOT JSON_EXTRACT_PATH(\"t\".\"j\", 'a') "
        "IS NULL THEN 'null' END)"
    )
    assert f"CAST(NULLIF({key}, 'null') AS DECIMAL(5, 1))" in ddl and ddl.endswith(f"GROUP BY {key}")


def test_json_text_compared_with_text_of_unknown_type_is_not_flagged() -> None:
    """RH-8: compared with LEFT(...), an IN subquery or a ? placeholder it was MANUAL, its
    note saying the statement fails -- it works (PostgreSQL compares text with text)."""
    _ddl, notes = _select("`t`.`id` AS `id`", where="(json_unquote(json_extract(`t`.`j`,'$.q')) = left(`t`.`s`,1))")
    assert notes == []
    _sql, result = _query("SELECT id FROM t WHERE j ->> '$.q' = ? AND j ->> '$.k' IN (SELECT s FROM u)")
    assert result.classification.value == "AUTO"
    _ddl, notes = _select("`t`.`id` AS `id`", where="(json_unquote(json_extract(`t`.`j`,'$.d')) = `t`.`d`)")
    assert any("not text -- a date" in n for n in notes)  # a date: PostgreSQL has no text = date


def test_a_path_with_up_to_three_zero_steps_is_followed() -> None:
    """RH-9: '$[0][0]' was left as MySQL's JSON_EXTRACT (refused); each [0] is a type test."""
    ddl, notes = _select("json_unquote(json_extract(`t`.`j`,'$[0][0]')) AS `a`")
    # Four type tests for the text, four for its JSON-null test (the inner [0]'s thrice).
    assert "JSON_EXTRACT(" not in ddl and ddl.count("JSON_TYPEOF") == 8 and notes == []
    _ddl, notes = _select("json_unquote(json_extract(`t`.`j`,'$[0][0][0][0]')) AS `a`")
    assert any("more than three [0] steps" in n for n in notes)


def test_a_json_document_whose_type_cannot_be_told_is_cast_to_json() -> None:
    """T9 / S8: json_extract_path_text(text, unknown) does not exist -- a document read
    through another view, or untyped in the Query Converter, was graded AUTO."""
    ddl, notes = _view(
        "select `b`.`id` AS `id`,json_unquote(json_extract(`b`.`js`,'$.q')) AS `q` from `shop`.`v_base` `b`"
    )
    assert "JSON_EXTRACT_PATH_TEXT(CAST(\"b\".\"js\" AS JSON), 'q')" in ddl and notes == []
    sql, _ = _query("SELECT id, JSON_EXTRACT(doc, '$.a') FROM docs", typed=False)
    assert "JSON_EXTRACT_PATH(CAST(doc AS JSON), 'a')" in sql
    sql, _ = _query("SELECT id, JSON_EXTRACT(j, '$.a') FROM t")
    assert "JSON_EXTRACT_PATH(j, 'a')" in sql  # a json column as it is


def test_json_text_in_a_numeric_function_or_a_simple_case_is_read_as_a_number() -> None:
    """S6: ABS / CEIL of ->> text, a simple CASE with number WHENs -- refused while AUTO;
    MOD over a double precision did not exist."""
    ddl, notes = _select(
        "abs(json_unquote(json_extract(`t`.`j`,'$.q'))) AS `a`,"
        "ceiling(json_unquote(json_extract(`t`.`j`,'$.q'))) AS `c`,"
        "(case json_unquote(json_extract(`t`.`j`,'$.q')) when 50 then 'fifty' else 'other' end) AS `e`,"
        "(json_unquote(json_extract(`t`.`j`,'$.q')) % 5) AS `m`"
    )
    text = "JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'q')"
    assert f"ABS(CAST({text} AS DOUBLE PRECISION))" in ddl and f"CEIL(CAST({text} AS DOUBLE PRECISION))" in ddl
    assert f"CASE CAST({text} AS DOUBLE PRECISION) WHEN 50" in ddl and f"(CAST({text} AS DECIMAL) % 5)" in ddl
    assert any("as a number where it meets one" in n for n in notes)


def test_a_json_value_in_arithmetic_is_the_double_of_its_text_and_ordered_is_reported() -> None:
    """S7: json + integer, sum(json), CAST(json AS BIGINT), COALESCE(json, 'x') refused while
    AUTO; ORDER BY a json value is now reported."""
    ddl, notes = _select(
        "(json_extract(`t`.`j`,'$.q') + 1) AS `a`,cast(json_extract(`t`.`j`,'$.q') as signed) AS `c`,"
        "coalesce(json_extract(`t`.`j`,'$.a'),'x') AS `e`,ifnull(json_extract(`t`.`j`,'$.n'),0) AS `f`"
    )
    assert "(CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'q') AS DOUBLE PRECISION) + 1)" in ddl
    assert "CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'q') AS BIGINT)" in ddl
    assert "COALESCE(CAST(JSON_EXTRACT_PATH(\"t\".\"j\", 'a') AS TEXT), 'x')" in ddl
    assert "COALESCE(CAST(JSON_EXTRACT_PATH(\"t\".\"j\", 'n') AS TEXT), CAST(0 AS TEXT))" in ddl
    assert any("as a number where it meets one" in n for n in notes)
    ddl, _ = _select("sum(json_extract(`t`.`j`,'$.q')) AS `s`")
    assert "SUM(CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'q') AS DOUBLE PRECISION))" in ddl
    _ddl, notes = _select("`t`.`id` AS `id`", tail=" order by json_extract(`t`.`j`,'$.a')")
    assert any("compared, ordered, grouped or made distinct" in n for n in notes)


def test_a_json_arrow_inside_a_functions_arguments_is_json_extract() -> None:
    """S4: sqlglot read `j -> '$.a'` there as a lambda, written as PostgreSQL's j -> '$.a' --
    a lookup of the key '$.a', NULL on every row, graded AUTO."""
    sql, _ = _query("SELECT id, COALESCE(j -> '$.a', j -> '$.b'), CONCAT(j -> '$.b', '!'), LENGTH(j -> '$.a') FROM t")
    assert "COALESCE(JSON_EXTRACT_PATH(j, 'a'), JSON_EXTRACT_PATH(j, 'b'))" in sql
    assert "CAST(JSON_EXTRACT_PATH(j, 'b') AS TEXT) || '!'" in sql
    assert "'$." not in sql


# --- BINARY keys and comparisons -------------------------------------------------------------


def test_a_binary_grouping_key_is_the_value_itself_whatever_it_holds() -> None:
    """T3 / RH-4: GROUP BY CONVERT_TO(CAST(x AS TEXT)) left a select list that names x (or its
    BINARY) not computable from the key -- 'must appear in the GROUP BY clause'."""
    ddl, _ = _view(
        "select `v`.`s` AS `s`,count(0) AS `n` from `shop`.`v_base` `v` group by cast(`v`.`s` as char charset binary)"
    )
    assert ddl.endswith('GROUP BY "v"."s"')
    sql, result = _query("SELECT s, COUNT(*) FROM t GROUP BY BINARY s", typed=False)
    assert sql.endswith("GROUP BY s") and result.classification.value == "AUTO"
    ddl, _ = _select("count(0) AS `n`", tail=" group by cast(`t`.`n` as char charset binary)")
    assert ddl.endswith('GROUP BY "t"."n"')


def test_a_binary_ordering_key_over_an_output_name_is_the_expression_it_names() -> None:
    """RH-5 / S1: ORDER BY CONVERT_TO(CAST(x AS TEXT)) over an alias -- 'column x does not
    exist' -- and over a UNION's column -- 'invalid UNION/INTERSECT/EXCEPT ORDER BY clause'."""
    ddl, notes = _select("concat(`t`.`s`,'x') AS `k`", tail=" order by cast(`k` as char charset binary)")
    assert 'ORDER BY "k"' in ddl and notes == []
    ddl, notes = _select("(`t`.`n` * 2) AS `x`", tail=" order by cast(`x` as char charset binary) limit 3")
    assert "ORDER BY CONVERT_TO(CAST((\"t\".\"n\" * 2) AS TEXT), 'UTF8')" in ddl and notes == []
    sql, _ = _query("SELECT s AS k FROM u ORDER BY BINARY k")
    assert "ORDER BY k" in sql
    ddl, notes = _view(
        "select `t`.`s` AS `s` from `shop`.`t` `t` union select `u`.`s` AS `s` from `shop`.`u` `u` "
        "order by cast(`s` as char charset binary)"
    )
    assert 'ORDER BY "s"' in ddl and notes == []


@pytest.mark.parametrize(
    "body",
    [
        "select `t`.`n` AS `v` from `shop`.`t` `t` union select `u`.`n` AS `n` from `shop`.`u` `u` "
        "order by cast(`v` as char charset binary)",
        "select distinct `t`.`n` AS `n` from `shop`.`t` `t` order by cast(`t`.`n` as char charset binary) limit 3",
    ],
)
def test_a_numbers_binary_order_a_union_or_distinct_cannot_take_is_reported(body: str) -> None:
    """RH-6 / S2 / T4: under SELECT DISTINCT BINARY was dropped for a number -- ordered 3
    before 12 where MySQL orders 12 first, graded AUTO; a UNION's was refused."""
    ddl, notes = _view(body)
    assert "CONVERT_TO" not in ddl
    assert any("orders such a query only by what it selects" in n for n in notes)


def test_a_dates_binary_order_under_distinct_is_the_date_itself() -> None:
    ddl, notes = _view(
        "select distinct `t`.`d` AS `d` from `shop`.`t` `t` order by cast(`t`.`d` as char charset binary)"
    )
    assert 'ORDER BY "t"."d"' in ddl and notes == []


def test_binary_compared_compares_the_values_text() -> None:
    """S14: BINARY n = '010' matched 10 (the number compared), IN ('2.0') was refused."""
    ddl, _ = _select("`t`.`id` AS `id`", where="(cast(`t`.`n` as char charset binary) = '010')")
    assert "WHERE (CAST(\"t\".\"n\" AS TEXT) = '010')" in ddl
    ddl, _ = _select("`t`.`id` AS `id`", where="(cast(`t`.`n` as char charset binary) = 10)")
    assert 'WHERE ("t"."n" = 10)' in ddl  # beside a number, as numbers
    ddl, _ = _view(
        "select `v`.`id` AS `id` from `shop`.`v_base` `v` where (cast(`v`.`n` as char charset binary) = '010')"
    )
    # G119: a value of a type that cannot be told is compared as itself, as 5d955cc wrote it
    # (x || '' read a TIMESTAMP with its zone and a boolean as 'true', and lost the index).
    assert "WHERE (\"v\".\"n\" = '010')" in ddl


# --- Text where MySQL wants a string -------------------------------------------------------


def test_the_other_string_functions_read_a_number_or_a_date_as_its_text() -> None:
    """T7(a) / S12: btrim(integer), md5(integer), position(integer, ...), integer ~ ... refused."""
    ddl, notes = _select(
        "trim(`t`.`n`) AS `a`,md5(`t`.`n`) AS `b`,locate('5',`t`.`n`) AS `c`,reverse(`t`.`d`) AS `e`,"
        "ascii(`t`.`n`) AS `g`,repeat(`t`.`n`,2) AS `h`",
        where="(`t`.`n` regexp '^1')",
    )
    for emitted in (
        'TRIM(CAST("t"."n" AS TEXT))', 'MD5(CAST("t"."n" AS TEXT))', "POSITION('5' IN CAST(\"t\".\"n\" AS TEXT))",
        'REVERSE(CAST("t"."d" AS TEXT))', 'ASCII(CAST("t"."n" AS TEXT))', 'REPEAT(CAST("t"."n" AS TEXT), 2)',
        "(CAST(\"t\".\"n\" AS TEXT) ~ '^1')",
    ):
        assert emitted in ddl, emitted
    assert notes == []


def test_a_comparison_made_text_is_its_digit_and_bytes_beside_text_are_decoded() -> None:
    """S12: CONCAT(n > 5, '') was 'true' where MySQL writes 1; CONCAT(s, blob) the blob's hex
    (CONCAT(int, blob) failed in bcb86bd, silently hex in 5d955cc)."""
    ddl, notes = _select("concat((`t`.`n` > 5),'') AS `a`,concat(`t`.`s`,`t`.`b`) AS `b`")
    assert "CAST(CAST((\"t\".\"n\" > 5) AS INT) AS TEXT) || ''" in ddl
    assert "\"t\".\"s\" || CONVERT_FROM(\"t\".\"b\", 'UTF8')" in ddl
    assert any("decodes them here as UTF-8" in n for n in notes)


def test_a_float_made_text_is_noted() -> None:
    """T7(b): MySQL writes a FLOAT with 6 significant digits (1234.57), graded AUTO."""
    _ddl, notes = _select("concat(`t`.`f`,'') AS `a`")
    assert any("6 significant digits" in n for n in notes)
    _ddl, notes = _select("json_array(`t`.`f`) AS `j`")
    assert any("6 significant digits" in n for n in notes)


def test_a_column_of_unknown_type_where_mysql_wants_a_string_is_made_text_and_noted() -> None:
    """S13: lpad(integer, ...), integer || integer through another view were refused while
    AUTO; LENGTH(IFNULL(blob, '')) counted CAST(bytea AS TEXT)'s hex. Under LIKE a column of
    unknown type is CAST(x AS TEXT) (a text column's index still serves LIKE 'prefix%'), and
    only LENGTH is noted (every untyped LIKE, LPAD or CONCAT made the statement MANUAL)."""
    ddl, notes = _view(
        "select lpad(`v`.`n`,3,'0') AS `a`,concat(`v`.`n`,`v`.`n`) AS `b`,length(ifnull(`v`.`b`,'')) AS `c`,"
        "concat(`v`.`n`,'-') AS `e` from `shop`.`v_base` `v` where (`v`.`n` like '1%')"
    )
    assert "LPAD(\"v\".\"n\" || '', 3, '0')" in ddl and "\"v\".\"n\" || '' || \"v\".\"n\" || ''" in ddl
    assert "OCTET_LENGTH(COALESCE(\"v\".\"b\", '') || '')" in ddl and "(CAST(\"v\".\"n\" AS TEXT) LIKE '1%')" in ddl
    assert "\"v\".\"n\" || '-' AS \"e\"" in ddl  # beside text, int || text has an operator
    assert len([n for n in notes if "cannot be told" in n and "TINYINT(1)" in n]) == 1


def test_greatest_of_a_number_and_a_string_compares_them_as_strings() -> None:
    """S10: MySQL's GREATEST(10, '5') is '5' (checked on 8.4); Aurora DSQL's was 10."""
    ddl, _ = _select("greatest(`t`.`n`,'5') AS `a`,least(`t`.`price`,'10') AS `b`")
    assert "GREATEST(CAST(\"t\".\"n\" AS TEXT), '5')" in ddl and "LEAST(CAST(\"t\".\"price\" AS TEXT), '10')" in ddl


def test_a_union_of_numbers_or_dates_and_strings_is_text_in_every_branch() -> None:
    """S15: 'UNION types integer and character varying cannot be matched', 'N/A' read as a date."""
    ddl, _ = _view(
        "select `x`.`v` AS `v` from (select `t`.`n` AS `v` from `shop`.`t` `t` union all "
        "select `t`.`s` AS `s` from `shop`.`t` `t`) `x`"
    )
    assert 'SELECT CAST("t"."n" AS TEXT) AS "v" FROM' in ddl
    ddl, _ = _view("select `t`.`d` AS `v` from `shop`.`t` `t` union all select 'N/A' AS `N/A`")
    assert 'SELECT CAST("t"."d" AS TEXT) AS "v"' in ddl
    sql, _ = _query("SELECT n FROM t UNION ALL SELECT s FROM u")
    assert sql == "SELECT CAST(n AS TEXT) AS n FROM t UNION ALL SELECT s FROM u"


# --- Recursive CTEs, binding, derived tables ---------------------------------------------------


@pytest.mark.parametrize(
    ("anchor", "step", "emitted"),
    [
        ("`t`.`s` AS `p` from `shop`.`t` `t` where (`t`.`id` = 1)", "concat(`r`.`p`,'/b')",
         "CAST(\"r\".\"p\" || '/b' AS VARCHAR(40))"),
        ("cast(1.5 as decimal(10,2)) AS `p`", "(`r`.`p` * 2)", 'CAST(("r"."p" * 2) AS DECIMAL(10, 2))'),
        ("`t`.`price` AS `p` from `shop`.`t` `t` where (`t`.`id` = 1)", "(`r`.`p` * 3)",
         'CAST(("r"."p" * 3) AS DECIMAL(10, 2))'),
    ],
)
def test_a_recursive_ctes_column_is_cast_to_its_first_selects_type(anchor: str, step: str, emitted: str) -> None:
    """T8 / S5: PostgreSQL refused a VARCHAR(n) / NUMERIC(p,s) anchor beside the recursive
    term's plain type ('in non-recursive term but type character varying overall')."""
    ddl, notes = _view(
        f"with recursive `r` as (select 1 AS `n`,{anchor} union all select (`r`.`n` + 1) AS `n + 1`,"
        f"{step} AS `x` from `r` where (`r`.`n` < 3)) select `r`.`n` AS `n`,`r`.`p` AS `p` from `r`"
    )
    assert emitted in ddl and notes == []


def test_an_integer_anchor_beside_a_recursive_fraction_is_reported() -> None:
    _ddl, notes = _view(
        "with recursive `r` as (select 1 AS `n` union all select (`r`.`n` + 0.5) AS `n + 0.5` from `r` "
        "where (`r`.`n` < 3)) select `r`.`n` AS `n` from `r`"
    )
    assert any("an integer in its first SELECT and a fraction" in n for n in notes)


def test_a_db_qualified_column_never_binds_a_derived_table() -> None:
    """S11: `db`.`t`.`id` beside a derived table aliased t lost its database, and Aurora DSQL
    bound the derived table: 'column t.id does not exist'."""
    ddl, _ = _view(
        "select `shop`.`t`.`id` AS `id`,(select count(0) from (select 1 AS `x`) `t` where "
        "(`shop`.`t`.`id` > 1)) AS `n` from `shop`.`t`"
    )
    assert 'WHERE ("shop"."t"."id" > 1)' in ddl


def test_a_derived_tables_quotient_is_shown_at_mysqls_scale_where_made_text() -> None:
    """S3 (settled on 5d955cc): LENGTH / CONCAT of a derived table's quotient read at its
    scale (LENGTH 6 for 3.3333, not 18); JSON writes every digit kept, as MySQL does."""
    ddl, _ = _view(
        "select length(`x`.`q`) AS `l`,concat(`x`.`q`,'') AS `c`,json_array(`x`.`q`) AS `j` from "
        "(select `t`.`id` AS `id`,(`t`.`n` / 3) AS `q` from `shop`.`t` `t`) `x`"
    )
    assert 'OCTET_LENGTH(CAST(ROUND("x"."q", 4) AS TEXT))' in ddl and "CAST(ROUND(\"x\".\"q\", 4) AS TEXT) || ''" in ddl
    assert 'JSON_BUILD_ARRAY("x"."q")' in ddl


def test_the_resolver_types_each_computed_column_once() -> None:
    """RH-3: a derived table's computed column re-typed its whole body for every reader --
    2^depth reads for a nest whose columns each read two of the level below."""
    from dsql_migrator.core import converter as C

    body = "SELECT t.id, t.a AS c0, t.a AS c1 FROM t"
    for level in range(1, 17):
        x = f"x{level}"
        body = f"SELECT {x}.id, {x}.c0 + {x}.c1 AS c0, {x}.c1 + {x}.c0 AS c1 FROM ({body}) {x}"
    tree = sqlglot.parse_one(f"SELECT y.c0 FROM ({body}) y", read=C.MYSQL_EXPRESSION_DIALECT)
    resolver = C._column_type_resolver([("t", (("id", "int"), ("a", "int")))], bare_names=True)
    typed: list = []

    def typed_by(expression: sqlglot.exp.Expression):
        typed.append(expression)
        kinds = {resolver(column) for column in expression.find_all(sqlglot.exp.Column)}
        return kinds.pop() if len(kinds) == 1 else None

    resolver.typed_by = typed_by
    assert resolver(tree.expressions[0]) == "int"
    assert len(typed) <= 2 * 16  # each (select, position) once


@pytest.mark.parametrize("depth", [8, 12])
def test_layered_derived_tables_and_cte_chains_convert_in_linear_time(depth: int) -> None:
    """RH-3's shapes (as BI tools generate them): 5d955cc converts each in about 0.01 s."""
    from dsql_migrator.core.converter import SchemaConverter, table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    types = [("id", "int"), ("a", "int"), ("b", "decimal(10,2)"), ("c", "int"), ("d", "decimal(12,4)"), ("e", "int")]
    table = TableDef(name="shop.t", columns=[ColumnDef(name=c, mysql_type=k) for c, k in types], primary_key=["id"])
    columns = ",".join(f"`t`.`{c}` AS `c{k}`" for k, c in enumerate("abcde"))
    body = f"select `t`.`id` AS `id`,{columns} from `shop`.`t` `t`"
    for level in range(1, depth + 1):
        p = f"`x{level - 1}`"
        columns = ",".join(
            f"(ifnull({p}.`c{k}`,0) + ({p}.`c{(k + 1) % 5}` * {p}.`c{(k + 2) % 5}`)) AS `c{k}`" for k in range(5)
        )
        body = f"select {p}.`id` AS `id`,{columns} from ({body}) {p}"
    rounded = ",".join(f"round(`y`.`c{k}`,2) AS `r{k}`" for k in range(5))
    view = f"select `y`.`id` AS `id`,{rounded} from ({body}) `y`"
    ctes = ["c0 AS (SELECT t.id, t.a / t.c AS q FROM t)"] + [
        f"c{i} AS (SELECT c{i - 1}.id, c{i - 1}.q + c{i - 1}.q AS q FROM c{i - 1})" for i in range(1, depth + 1)
    ]
    last = f"c{depth}"
    chain = "WITH " + ", ".join(ctes) + f" SELECT {last}.id, ROUND({last}.q, 2), CONCAT({last}.q, '') FROM {last}"
    started = time.perf_counter()
    assert SchemaConverter().convert_view(ViewDef(name="shop.v", definition=view), tables=[table]).auto_converted
    assert QueryConverter(column_types=tuple(table_type_index([table]).values())).convert(chain).converted_sql
    assert time.perf_counter() - started < 5
