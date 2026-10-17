"""G107: the review of G101-G105 -- MySQL expressions typed through derived tables and CTEs
(a CTE named like the table it reads included), LENGTH and BINARY of a column whose type
cannot be told written for any type or reported, MySQL's ->> as MySQL reads it (a JSON
null, a number it meets, a path Aurora DSQL cannot follow), a date beside a string that
is no date as MySQL's string, implicit number-to-text conversion, joins with no ON at top
level, `db`.`alias`.`col` bound as MySQL binds it, and a stored view's tables found in the
case MySQL keeps. Each form was run on MySQL 8.4 and PostgreSQL 17 over the same rows
(scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import re
import time

import pytest
import sqlglot

from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="i", mysql_type="int"),
        ColumnDef(name="n", mysql_type="int"),
        ColumnDef(name="dec2", mysql_type="decimal(10,2)"),
        ColumnDef(name="j", mysql_type="json"),
        ColumnDef(name="bl", mysql_type="blob"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="c", mysql_type="char(8)"),
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="dt", mysql_type="datetime"),
        ColumnDef(name="dt6", mysql_type="datetime(6)"),
        ColumnDef(name="tm", mysql_type="time"),
        ColumnDef(name="paid", mysql_type="tinyint(1)"),
    ],
    primary_key=["id"],
)
_ITEMS = TableDef(
    name="shop.items",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="tid", mysql_type="int"),
        ColumnDef(name="code", mysql_type="blob"),
        ColumnDef(name="qty", mysql_type="int"),
    ],
    primary_key=["id"],
)
_TABLES = [_T, _ITEMS]


def _norm(sql: str) -> str:
    """SQL with its whitespace collapsed and none just inside a parenthesis."""
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str, tables=_TABLES):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=tables)
    return _norm(view.target_ddl), [w.message for w in view.warnings]


def _select(select_list: str, *, where: str = ""):
    return _view(f"select {select_list} from `shop`.`t` `t`" + (f" where {where}" if where else ""))


def _query(sql: str, *, typed: bool = True, tables=_TABLES):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    types = tuple(table_type_index(tables).values()) if typed else ()
    result = QueryConverter(column_types=types).convert(sql)
    return _norm(result.converted_sql or ""), result


# --- Typing through derived tables and CTEs ----------------------------------------------


def test_a_derived_tables_and_a_ctes_columns_take_the_type_of_what_they_select() -> None:
    """P6: LENGTH of a numeric column read through a derived table or a CTE was
    OCTET_LENGTH(numeric) -- refused at CREATE VIEW, graded AUTO."""
    ddl, notes = _view(
        "select length(`d`.`total`) AS `a`,length(`d`.`i`) AS `b` from (select sum(`o`.`dec2`) AS "
        "`total`,`o`.`i` AS `i` from `shop`.`t` `o`) `d`"
    )
    assert 'OCTET_LENGTH(CAST("d"."total" AS TEXT))' in ddl and 'OCTET_LENGTH(CAST("d"."i" AS TEXT))' in ddl
    assert notes == []
    ddl, notes = _view(
        "with `c` as (select `o`.`id` AS `id`,`o`.`dec2` AS `amount` from `shop`.`t` `o`) "
        "select `c`.`id` AS `id`,length(`c`.`amount`) AS `l` from `c`"
    )
    assert 'OCTET_LENGTH(CAST("c"."amount" AS TEXT))' in ddl and notes == []


def test_select_star_a_column_list_and_a_recursive_ctes_anchor_type_their_columns() -> None:
    sql, _ = _query("SELECT CONVERT(x.code USING utf8mb4) FROM (SELECT * FROM items) x")
    assert "CONVERT_FROM(x.code, 'UTF8')" in sql
    sql, _ = _query("SELECT LENGTH(x.k) FROM (SELECT id, code FROM items) AS x (n, k)")
    assert "OCTET_LENGTH(x.k)" in sql and "|| ''" not in sql
    sql, _ = _query(
        "WITH RECURSIVE r AS (SELECT 1 AS n, i.code AS b FROM items i UNION ALL "
        "SELECT n + 1, b FROM r WHERE n < 3) SELECT CONVERT(r.b USING utf8mb4) FROM r"
    )
    assert "CONVERT_FROM(r.b, 'UTF8')" in sql


def test_a_cte_named_like_the_table_it_reads_is_typed_by_its_body() -> None:
    """ES-5: the CTE's every column was untyped -- CONVERT over a BLOB returned its hex,
    graded AUTO (169c920 typed it)."""
    sql, result = _query(
        "WITH items AS (SELECT id, code FROM items WHERE qty > 0) "
        "SELECT items.id, CONVERT(items.code USING utf8mb4) FROM items"
    )
    assert "CAST(CONVERT_FROM(items.code, 'UTF8') AS TEXT)" in sql
    assert result.classification.value == "MANUAL"  # the decode note


def test_a_shadowing_cte_whose_body_does_not_tell_stays_untyped() -> None:
    """G97P-3's case: a literal is no column of the table it shadows."""
    sql, _ = _query(
        "WITH items AS (SELECT 1 AS id, CONVERT(code USING utf8mb4) AS code FROM items) "
        "SELECT CAST(items.code AS CHAR) FROM items"
    )
    assert "SELECT CAST(items.code AS TEXT) FROM items" in sql


def test_a_computed_column_is_typed_by_what_it_computes() -> None:
    from dsql_migrator.core import converter as C

    body = "select `d`.`r` AS `r` from (select (`t`.`i` / 3) AS `r`,now(6) AS `w` from `shop`.`t` `t`) `d`"
    tree = sqlglot.parse_one(body, read=C._MySqlViewDialect)
    resolver = C._column_type_resolver(C.view_column_types(body, C.table_type_index(_TABLES)))
    C._rewrite_mysql_calls(tree, resolver)
    column = next(c for c in tree.find_all(sqlglot.exp.Column) if c.table == "d")
    assert resolver(column) == "decimal(65,4)"


def test_typing_through_nested_derived_tables_is_linear() -> None:
    def took(depth: int) -> float:
        source = f"(select `t`.`s` AS `a` from `shop`.`t` `t`) `d{depth}`"
        for level in reversed(range(depth)):
            source = f"(select `d{level + 1}`.`a` AS `a` from {source}) `d{level}`"
        body = f"select length(`d0`.`a`) AS `l`,concat(`d0`.`a`,'x') AS `c` from {source}"
        started = time.perf_counter()
        ddl, notes = _view(body)
        assert 'OCTET_LENGTH("d0"."a")' in ddl and notes == []
        return time.perf_counter() - started

    took(10)  # warm up
    small, large = took(15), took(60)
    assert large < small * 10  # linear: 4x the depth


# --- LENGTH and BINARY of a column whose type cannot be told -----------------------------


def test_length_of_a_column_of_unknown_type_counts_its_text_whatever_it_holds() -> None:
    """P6 / P7 / P8 / ES-16: OCTET_LENGTH(x) of a number or a date read through another
    view, or in the Query Converter before Step 1, was refused while graded AUTO."""
    ddl, notes = _view("select `b`.`id` AS `id`,length(`b`.`amount`) AS `l` from `shop`.`v_base` `b`")
    assert "OCTET_LENGTH(\"b\".\"amount\" || '')" in ddl
    assert any("TINYINT(1)" in n and "cannot be told" in n for n in notes)
    sql, result = _query("SELECT id FROM t WHERE LENGTH(i) = 1 AND CHAR_LENGTH(s) > 0", typed=False)
    assert "OCTET_LENGTH(i || '') = 1" in sql and "LENGTH(s || '') > 0" in sql
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]


def test_char_length_counts_the_characters_of_the_values_text() -> None:
    """CHAR_LENGTH(int) was LENGTH(integer), which does not exist."""
    sql, result = _query("SELECT CHAR_LENGTH(i), CHAR_LENGTH(c), CHAR_LENGTH(bl), CHAR_LENGTH(d) FROM t")
    assert "LENGTH(CAST(i AS TEXT)), LENGTH(c), LENGTH(bl), LENGTH(CAST(d AS TEXT))" in sql
    assert result.warnings == []


def test_binary_of_a_column_of_unknown_type_is_noted() -> None:
    """P9 / ES-16: CAST(x AS BYTEA) fails for a number and for text with a backslash; it was
    graded AUTO."""
    sql, result = _query("SELECT BINARY tx, MD5(CAST(i AS BINARY)) FROM t", typed=False)
    assert "CAST(tx AS BYTEA), MD5(CAST(i AS BYTEA))" in sql
    assert result.classification.value == "MANUAL"
    assert any("CONVERT_TO(CAST(x AS TEXT), 'UTF8')" in w.message for w in result.warnings)
    _ddl, notes = _view("select cast(`b`.`amount` as char charset binary) AS `raw` from `shop`.`v_base` `b`")
    assert any("BINARY" in n and "cannot be told" in n for n in notes)


def test_an_ordering_key_of_unknown_type_is_ordered_by_the_bytes_of_its_text() -> None:
    """R4: BINARY was dropped for an unknown kind -- a number read through a view ordered
    numerically (3 before 12) where MySQL orders its digits (12 before 3)."""
    ddl, notes = _view("select `v`.`id` AS `id` from `shop`.`v_base` `v` order by cast(`v`.`m` as char charset binary)")
    assert "ORDER BY CONVERT_TO(CAST(\"v\".\"m\" AS TEXT), 'UTF8')" in ddl and notes == []
    ddl, _ = _select("`t`.`s` AS `s`", where="(`t`.`id` > 0) order by cast(`t`.`s` as char charset binary)")
    assert 'ORDER BY "t"."s"' in ddl  # a text's order is byte-wise already


@pytest.mark.parametrize(
    ("column", "binary", "ordered"),
    [
        ("s", True, "CONVERT_TO(\"t\".\"s\", 'UTF8')"),
        ("c", True, "CONVERT_TO(CAST(\"t\".\"c\" AS TEXT), 'UTF8')"),
        ("s", False, '"t"."s"'),
    ],
)
def test_a_select_distincts_binary_order_key_is_written_as_its_select_list_has_it(
    column: str, binary: bool, ordered: str
) -> None:
    """R3: BINARY was dropped from the ORDER BY but kept in the DISTINCT list: 'for SELECT
    DISTINCT, ORDER BY expressions must appear in select list'."""
    key = f"cast(`t`.`{column}` as char charset binary)"
    listed = key if binary else f"`t`.`{column}`"
    ddl, _ = _view(f"select distinct {listed} AS `x` from `shop`.`t` `t` order by {key}")
    assert f"SELECT DISTINCT {ordered} AS \"x\"" in ddl and f"ORDER BY {ordered} NULLS FIRST" in ddl
    sql, _ = _query("SELECT DISTINCT BINARY s AS x FROM u ORDER BY BINARY s", typed=False)
    assert "SELECT DISTINCT CAST(s AS BYTEA) AS x FROM u ORDER BY CAST(s AS BYTEA)" in sql


# --- ->> and JSON_EXTRACT ------------------------------------------------------------------


def test_the_text_of_a_json_null_is_null_as_mysql_writes_it() -> None:
    """P11 / R2 / ES-2: JSON_EXTRACT_PATH_TEXT of a JSON null is NULL, MySQL's ->> the text
    'null' -- `IS NULL` gained the rows holding an explicit null."""
    ddl, notes = _select("`t`.`id` AS `id`", where="(json_unquote(json_extract(`t`.`j`,'$.deleted_at')) is null)")
    assert (
        "COALESCE(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'deleted_at'), CASE WHEN NOT "
        "JSON_EXTRACT_PATH(\"t\".\"j\", 'deleted_at') IS NULL THEN 'null' END) IS NULL"
    ) in ddl
    assert notes == []


def test_an_index_step_follows_mysqls_path() -> None:
    """MySQL reads [0] of a value that is no array as the value itself, a member only in an
    object and an index only in an array (JSON_EXTRACT_PATH reads '1' as either)."""
    ddl, notes = _select(
        "json_unquote(json_extract(`t`.`j`,'$.a[0]')) AS `a`,json_extract(`t`.`j`,'$.b[2]') AS `b`,"
        "json_extract(`t`.`j`,'$.\"1\"') AS `c`"
    )
    assert "CASE WHEN JSON_TYPEOF(\"t\".\"j\" -> 'a') = 'array' THEN (\"t\".\"j\" -> 'a') -> 0 ELSE \"t\".\"j\" -> 'a' END" in ddl
    assert "(\"t\".\"j\" -> 'b' -> 2) AS \"b\"" in ddl and "(\"t\".\"j\" -> '1') AS \"c\"" in ddl
    assert notes == []


@pytest.mark.parametrize(
    "where",
    [
        "(json_unquote(json_extract(`t`.`j`,'$.qty')) = 5)",
        "(json_unquote(json_extract(`t`.`j`,'$.qty')) between 1 and 5)",
        "(json_unquote(json_extract(`t`.`j`,'$.qty')) in (3,4))",
        "((json_unquote(json_extract(`t`.`j`,'$.qty')) + 1) > 2)",
        "(json_unquote(json_extract(`t`.`j`,'$.qty')) = `t`.`i`)",
    ],
)
def test_the_text_of_a_json_value_meeting_a_number_is_read_as_mysql_reads_it(where: str) -> None:
    """R2 / ES-2 (a): text = integer, text + integer do not exist -- CREATE VIEW failed, graded
    AUTO. MySQL reads the text as a double."""
    ddl, notes = _select("`t`.`id` AS `id`", where=where)
    assert "CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'qty') AS DOUBLE PRECISION)" in ddl
    assert any("as a number" in n and "fails the cast" in n for n in notes)


def test_sum_and_round_of_json_text_are_computed_in_double_precision() -> None:
    """ES-2 (d): ROUND(doc->>'$.a', 1) showed numeric 5.0 where MySQL's double shows 5."""
    ddl, _ = _select("sum(json_unquote(json_extract(`t`.`j`,'$.a'))) AS `s`,round(json_unquote(json_extract(`t`.`j`,'$.a')),1) AS `r`")
    assert "SUM(CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'a') AS DOUBLE PRECISION))" in ddl
    assert "(ROUND(CAST(CAST(JSON_EXTRACT_PATH_TEXT(\"t\".\"j\", 'a') AS DOUBLE PRECISION) AS DOUBLE PRECISION) * 10) / 10)" in ddl


@pytest.mark.parametrize(
    "path",
    ["'$'", "'$[*]'", "'$**.a'", "'$[last]'", "'$[1 to 2]'", "concat('$.',`t`.`s`)"],
)
def test_a_path_aurora_dsql_cannot_follow_is_reported(path: str) -> None:
    """R2 / ES-2 (b): JSON_EXTRACT_PATH_TEXT(j) does not exist, and '$[last]', a range or a
    run-time path were passed as a key -- NULL on every row, graded AUTO."""
    ddl, notes = _select(f"json_unquote(json_extract(`t`.`j`,{path})) AS `x`")
    assert "JSON_UNQUOTE(" in ddl
    assert len([n for n in notes if "one fixed path" in n]) == 1 and not any("json_unquote()" in n for n in notes)
    sql, result = _query(f"SELECT t.j ->> {path.replace('`', '')} FROM t")
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"] and "JSON_UNQUOTE(" in sql


def test_several_paths_and_a_json_value_compared_with_a_number_are_reported() -> None:
    """ES-20: JSON_EXTRACT(j, '$.a', '$.b') returned only the first path's value, and json =
    integer does not exist -- both AUTO."""
    _ddl, notes = _select("json_extract(`t`.`j`,'$.a','$.b') AS `m`")
    assert any("several paths" in n for n in notes)
    _ddl, notes = _select("`t`.`id` AS `id`", where="(json_extract(`t`.`j`,'$.a') = 5)")
    assert any("no comparison of json values" in n for n in notes)
    sql, result = _query("SELECT JSON_UNQUOTE(JSON_EXTRACT(j, '$.a', '$.b')) FROM t")
    assert [w.code for w in result.warnings] == ["MYSQL_SEMANTICS"]


def test_a_json_document_held_in_text_is_read_as_json() -> None:
    """R2 (c): json_extract_path_text(character varying, unknown) does not exist."""
    ddl, _ = _select("json_unquote(json_extract(`t`.`s`,'$.a')) AS `a`,json_extract(`t`.`s`,'$.b') AS `b`")
    assert "JSON_EXTRACT_PATH_TEXT(CAST(\"t\".\"s\" AS JSON), 'a')" in ddl
    assert "JSON_EXTRACT_PATH(CAST(\"t\".\"s\" AS JSON), 'b') AS \"b\"" in ddl


# --- A date beside a string -----------------------------------------------------------------


def test_a_date_beside_a_string_that_is_no_date_is_mysqls_string() -> None:
    """ES-13: IFNULL(d, '') failed at CREATE VIEW ('invalid input syntax for type date')."""
    ddl, notes = _select(
        "ifnull(`t`.`d`,'') AS `a`,coalesce(`t`.`dt6`,'N/A') AS `b`,"
        "(case when (`t`.`i` > 0) then `t`.`tm` else 'none' end) AS `c`"
    )
    assert "COALESCE(CAST(\"t\".\"d\" AS TEXT), '') AS \"a\"" in ddl
    assert "COALESCE(TO_CHAR(\"t\".\"dt6\", 'YYYY-MM-DD HH24:MI:SS.FF6'), 'N/A') AS \"b\"" in ddl
    assert "THEN CAST(\"t\".\"tm\" AS TEXT) ELSE 'none' END" in ddl
    assert notes == []


def test_a_date_beside_a_datetime_string_keeps_the_time() -> None:
    """ES-13: COALESCE(d, '2024-06-01 10:00:00') lost the time, and + INTERVAL 1 DAY was
    then cast to a DATE."""
    ddl, _ = _select("(coalesce(`t`.`d`,'2024-06-01 10:00:00') + interval 1 day) AS `a`")
    assert "COALESCE(CAST(\"t\".\"d\" AS TIMESTAMP), '2024-06-01 10:00:00') + INTERVAL '1 DAY'" in ddl
    assert "AS DATE" not in ddl


def test_mysqls_compact_date_string_is_a_date() -> None:
    """P12: coalesce(d, '20240601') + interval 1 day was a timestamp ('2024-02-01 00:00:00')."""
    ddl, _ = _select("(coalesce(`t`.`d`,'20240601') + interval 1 day) AS `a`")
    assert "CAST(COALESCE(\"t\".\"d\", '20240601') + INTERVAL '1 DAY' AS DATE)" in ddl


def test_json_of_a_date_beside_a_date_string_is_mysqls_string_of_it() -> None:
    """P12 / ES-13: the DATETIME(6) branch lost its fraction, a date-only string gained
    ' 00:00:00', and GREATEST's NULL check hid its literal (so '.000000' was added)."""
    ddl, _ = _select(
        "json_array(coalesce(`t`.`dt6`,'2024-06-01 00:00:00'),ifnull(`t`.`dt`,'2024-06-01')) AS `a`,"
        "json_object('m',greatest(`t`.`dt`,'2024-03-01 00:00:00')) AS `b`"
    )
    assert "COALESCE(TO_CHAR(\"t\".\"dt6\", 'YYYY-MM-DD HH24:MI:SS.FF6'), '2024-06-01 00:00:00')" in ddl
    assert "COALESCE(CAST(\"t\".\"dt\" AS TEXT), '2024-06-01')" in ddl
    assert "'m': CAST(CASE WHEN" in ddl and ".US" not in ddl


# --- MySQL's implicit text conversion ------------------------------------------------------


@pytest.mark.parametrize(
    ("select_list", "emitted"),
    [
        ("concat(year(`t`.`d`),month(`t`.`d`)) AS `a`", "CAST(CAST(EXTRACT(YEAR FROM"),
        ("lpad(`t`.`id`,5,'0') AS `a`", "LPAD(CAST(\"t\".\"id\" AS TEXT), 5, '0')"),
        ("left(`t`.`i`,1) AS `a`", "LEFT(CAST(\"t\".\"i\" AS TEXT), 1)"),
        ("replace((`t`.`i` / 3),'.',',') AS `a`", "REPLACE(CAST(ROUND("),
        ("upper(`t`.`paid`) AS `a`", "UPPER(CAST(CAST(\"t\".\"paid\" AS INT) AS TEXT))"),
        ("concat('at ',`t`.`dt6`) AS `a`", "'at ' || TO_CHAR(\"t\".\"dt6\", 'YYYY-MM-DD HH24:MI:SS.FF6')"),
        ("if(`t`.`paid`,`t`.`i`,`t`.`s`) AS `a`", "THEN CAST(\"t\".\"i\" AS TEXT) ELSE \"t\".\"s\" END"),
        # A date beside a number is a string too ('COALESCE types date and integer cannot
        # be matched').
        ("ifnull(`t`.`d`,0) AS `a`", "COALESCE(CAST(\"t\".\"d\" AS TEXT), CAST(0 AS TEXT))"),
    ],
)
def test_a_number_or_date_where_mysql_wants_a_string_is_its_text(select_list: str, emitted: str) -> None:
    """ES-17: integer || integer, lpad(integer, ...), left(integer, ...) do not exist, a CASE of
    text and an integer cannot be matched -- all refused while graded AUTO."""
    ddl, _ = _select(select_list)
    assert emitted in ddl, ddl


def test_a_number_compared_with_like_is_its_text() -> None:
    ddl, _ = _select("`t`.`id` AS `id`", where="(`t`.`i` like '1%')")
    assert "CAST(\"t\".\"i\" AS TEXT) LIKE '1%'" in ddl


def test_a_recursive_ctes_widened_anchor_column_is_text() -> None:
    """ES-18: the anchor's VARCHAR(10) beside the recursive term's text: 'recursive query "r"
    column 2 has type character varying(10) in non-recursive term but type character varying
    overall'. G113 (T8 / S5): the recursive SELECT's value is cast to the anchor's type, as
    MySQL types each recursive row by it (the anchor's TEXT form lost the cut to 10)."""
    sql, _ = _query(
        "WITH RECURSIVE r AS (SELECT 1 AS n, CAST('a' AS CHAR(10)) AS p UNION ALL "
        "SELECT n + 1, CONCAT(p, '/b') FROM r WHERE n < 3) SELECT n, p FROM r"
    )
    assert "CAST('a' AS VARCHAR(10)) AS p UNION ALL SELECT n + 1, CAST(p || '/b' AS VARCHAR(10))" in sql


# --- Joins and binding --------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "emitted"),
    [
        ("SELECT t.id FROM t JOIN items JOIN shop.items i2 ON i2.tid = t.id", "FROM t CROSS JOIN items JOIN"),
        ("SELECT t.id FROM t INNER JOIN items WHERE t.id = 1", "FROM t CROSS JOIN items WHERE"),
        ("SELECT t.id FROM t CROSS JOIN items ON items.tid = t.id", "FROM t JOIN items ON items.tid = t.id"),
        ("SELECT t.id FROM t, items WHERE items.tid = t.id", "FROM t, items WHERE"),
    ],
)
def test_a_top_level_join_without_on_is_a_cross_join(sql: str, emitted: str) -> None:
    """ES-14: sqlglot wrote an ON-less JOIN as a comma (which binds looser, so the next ON
    could not see t), and kept INNER JOIN without ON and CROSS JOIN ... ON -- each refused."""
    converted, _ = _query(sql)
    assert emitted in converted, converted


def test_a_db_qualified_column_over_a_table_written_without_its_database_is_typed() -> None:
    """P13: SELECT shop.t.bl FROM t (the session's database) was untyped -- CAST(bytea AS
    TEXT), its hex."""
    sql, _ = _query("SELECT t.id, CAST(shop.t.bl AS CHAR) FROM t")
    assert "CAST(CONVERT_FROM(shop.t.bl, 'UTF8') AS TEXT)" in sql


def test_a_db_qualified_column_binds_past_an_aliased_table_without_it() -> None:
    """ES-15: MySQL binds `db`.`t`.`s` to the outer t when the inner alias t (items) has no
    s; the database was dropped and PostgreSQL bound it to the alias: 'column t.s does not
    exist'."""
    ddl, _ = _view(
        "select `shop`.`t`.`id` AS `id`,(select count(0) from `shop`.`items` `t` where "
        "((`t`.`tid` = `t`.`id`) and (`shop`.`t`.`s` is not null))) AS `n` from `shop`.`t`"
    )
    assert 'NOT "shop"."t"."s" IS NULL' in ddl
    # One the alias's table has is the alias's (a statement may write it so).
    sql, _ = _query("SELECT (SELECT COUNT(*) FROM shop.items t WHERE shop.t.qty > 0) FROM shop.t")
    assert "WHERE t.qty > 0" in sql


def test_a_view_names_its_table_in_the_case_mysql_keeps() -> None:
    """ES-23: with only `Docs` (a BLOB) in the inventory, a view over `docs` (a VARCHAR) was
    typed by `Docs` -- CONVERT_FROM over a varchar."""
    upper = TableDef(name="shop.Docs", columns=[ColumnDef(name="body", mysql_type="blob")])
    ddl, _ = _view("select convert(`x`.`body` using utf8mb4) AS `b` from `shop`.`docs` `x`", tables=[upper])
    assert "CONVERT_FROM(" not in ddl
    sql, _ = _query("SELECT CONVERT(body USING utf8mb4) FROM docs", tables=[upper])
    assert "CONVERT_FROM(body, 'UTF8')" in sql  # a statement may write another case


def test_evaluation_grades_a_view_over_a_view_by_the_conversions_notes() -> None:
    """P6: Evaluation reported nothing for LENGTH through another view (graded AUTO)."""
    from dsql_migrator.core.assessor import MySqlViewConversionRule

    inventory = SourceInventory(
        tables=_TABLES,
        views=[
            ViewDef(name="shop.v_base", definition="select `t`.`id` AS `id`,`t`.`i` AS `i` from `shop`.`t` `t`"),
            ViewDef(name="shop.v_len", definition="select length(`b`.`i`) AS `l` from `shop`.`v_base` `b`"),
        ],
    )
    findings = MySqlViewConversionRule().evaluate(inventory)
    assert [f.object.name for f in findings] == ["shop.v_len"]
