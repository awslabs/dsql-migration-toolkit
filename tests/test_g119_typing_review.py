"""G119: the final regression round on G112-G117, typing -- what 5d955cc (or bcb86bd) wrote
right and G112-G117 broke, restored: a comparison standing in JSON as true / false, a date
beside ISO strings of its sort a date, a UNION column beside strings PostgreSQL reads as its
values and GREATEST / LEAST beside a clamp string ('0') left as they were, a number beside
number strings a number where it is shown, BINARY and string functions over a value of
unknown type as the value itself, a no-digit string compared with a date as the zero date, a
single-database view's bare tables in its own schema, GROUP_CONCAT of a TINYINT(1) its
digit, nested [0] steps bounded, ORDER BY BINARY of a shadowed name by the column, a widened
COALESCE in JSON as its value, and an unqualified derived-table quotient at MySQL's scale.
Each form was run on MySQL 8.4 and PostgreSQL 17 over the same rows
(scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import inspect
import re
import time

from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="n", mysql_type="int"),
        ColumnDef(name="price", mysql_type="decimal(10,2)"),
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="dt", mysql_type="datetime"),
        ColumnDef(name="dt6", mysql_type="datetime(6)"),
        ColumnDef(name="ts", mysql_type="timestamp"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
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


# --- JSON ----------------------------------------------------------------------------------


def test_a_comparison_standing_in_json_is_true_or_false() -> None:
    """F1 / RG1 / ES-1: JSON_ARRAY(n > 1) was written [1] -- MySQL (and 5d955cc) write [true],
    as PostgreSQL writes a boolean -- graded AUTO. A TINYINT(1), a derived table's comparison
    column and a choice of comparisons stay MySQL's 1 / 0."""
    ddl, notes = _select(
        "json_array((`t`.`n` > 1),(`t`.`n` is null),(0 = `t`.`flag`),`t`.`flag`,(`t`.`flag` = 1)) AS `a`,"
        "json_object('k',(`t`.`n` = 50)) AS `o`,json_array((case when (`t`.`n` > 1) then (`t`.`n` > 2) end)) AS `c`"
    )
    assert (
        'JSON_BUILD_ARRAY(("t"."n" > 1), ("t"."n" IS NULL), (0 = CAST("t"."flag" AS INT)), CAST("t"."flag" AS INT), '
        '(CAST("t"."flag" AS INT) = 1))'
    ) in ddl
    assert "JSON_OBJECT('k': (\"t\".\"n\" = 50))" in ddl
    assert 'JSON_BUILD_ARRAY(CAST((CASE WHEN ("t"."n" > 1) THEN ("t"."n" > 2) END) AS INT))' in ddl and notes == []
    ddl, _ = _select("json_arrayagg((`t`.`n` > 1)) AS `a`")
    assert 'JSON_AGG(("t"."n" > 1))' in ddl
    ddl, _ = _view("select json_array(`x`.`c`) AS `j` from (select (`t`.`n` > 1) AS `c` from `shop`.`t` `t`) `x`")
    assert 'JSON_BUILD_ARRAY(CAST("x"."c" AS INT))' in ddl
    sql, result = _query("SELECT id, JSON_ARRAY(n > 1, n IS NULL) FROM t")
    assert sql == "SELECT id, JSON_BUILD_ARRAY(n > 1, n IS NULL) FROM t" and result.classification.value == "AUTO"


def test_json_of_a_nested_or_grouped_widened_coalesce() -> None:
    """ES-13 / RG13: an inner COALESCE kept its widening ROUND in JSON ([0.00], MySQL [0]),
    and JSON of a COALESCE its query groups by dropped the ROUND its GROUP BY key kept --
    'must appear in the GROUP BY clause'. MySQL writes a COALESCE's value itself through
    an IF / CASE / COALESCE too (checked on 8.4)."""
    ddl, _ = _select(
        "json_array(coalesce(coalesce(`t`.`price`,0),1),if(`t`.`flag`,coalesce(`t`.`price`,0),1)) AS `j`,"
        "coalesce(`t`.`price`,0) AS `a`"
    )
    assert 'JSON_BUILD_ARRAY(COALESCE(COALESCE("t"."price", 0), 1), CASE WHEN "t"."flag" THEN COALESCE("t"."price", 0)' in ddl
    assert 'COALESCE("t"."price", ROUND(0, 2)) AS "a"' in ddl  # shown at its widest scale still
    ddl, _ = _select("json_array(coalesce(`t`.`price`,0)) AS `j`,count(0) AS `c`", tail=" group by coalesce(`t`.`price`,0)")
    assert 'JSON_BUILD_ARRAY(COALESCE("t"."price", ROUND(0, 2)))' in ddl
    assert ddl.endswith('GROUP BY COALESCE("t"."price", ROUND(0, 2))')


def test_group_concat_of_a_tinyint_or_a_comparison_is_its_digit() -> None:
    """ES112-7: STRING_AGG(CAST(flag AS TEXT)) gave 'true,false' where MySQL gives '1,0'; CAST
    AS CHAR, CONCAT_WS and CHAR_LENGTH of a comparison already go through MySQL's text."""
    ddl, _ = _select(
        "group_concat(`t`.`flag` order by `t`.`id` ASC separator ',') AS `g`,group_concat((`t`.`n` > 2) separator ',') AS `h`,"
        "group_concat(distinct `t`.`flag` order by `t`.`flag` ASC separator ',') AS `k`"
    )
    assert "STRING_AGG(CAST(CAST(\"t\".\"flag\" AS INT) AS TEXT), ','" in ddl
    assert "STRING_AGG(CAST(CAST((\"t\".\"n\" > 2) AS INT) AS TEXT), ',')" in ddl
    assert 'ARRAY_AGG(DISTINCT CAST("t"."flag" AS INT) ORDER BY CAST("t"."flag" AS INT) ASC' in ddl
    ddl, _ = _select("cast((`t`.`n` > 2) as char charset utf8mb4) AS `c`,concat_ws(',',(`t`.`n` > 2)) AS `w`")
    assert 'CAST(CAST(("t"."n" > 2) AS INT) AS TEXT) AS "c"' in ddl
    assert "CONCAT_WS(',', CAST(CAST((\"t\".\"n\" > 2) AS INT) AS TEXT))" in ddl


def test_nested_zero_steps_are_counted_through_the_nest() -> None:
    """RG9: each [0] step holds its document three times and a nested call's document is the
    inner call's value: three levels of '$[0][0][0]' wrote about 1 MB of DDL (four: 30 MB, 89 s)."""
    call = "`t`.`j`"
    for _ in range(4):
        call = f"json_extract({call},'$[0][0][0]')"
    started = time.perf_counter()
    ddl, notes = _select(f"json_unquote({call}) AS `a`")
    assert time.perf_counter() - started < 5 and len(ddl) < 20000
    assert any("more than three [0] steps" in n for n in notes)
    ddl, notes = _select("json_unquote(json_extract(json_extract(`t`.`j`,'$[0]'),'$[0][0]')) AS `a`")
    assert "JSON_EXTRACT(" not in ddl and notes == []  # three in all: followed


# --- Choices MySQL makes a string ------------------------------------------------------------


def test_a_date_beside_iso_strings_of_its_sort_stays_a_date() -> None:
    """F2 / RG4: IFNULL(end_date, '9999-12-31') was text wherever it was shown, so a reader
    that computes with it as a date -- a derived table's, a CTE's, a view's, an INSERT ...
    SELECT into a DATE -- failed ('text + interval', 'text >= date'), graded AUTO. Its text
    is MySQL's string already (5d955cc). Text only where the texts differ."""
    ddl, notes = _select(
        "ifnull(`t`.`d`,'9999-12-31') AS `e`,coalesce(`t`.`dt`,'2024-06-01 00:00:00') AS `f`,"
        "json_array(ifnull(`t`.`d`,'9999-12-31')) AS `j`"
    )
    assert "COALESCE(\"t\".\"d\", '9999-12-31') AS \"e\"" in ddl
    assert "COALESCE(\"t\".\"dt\", '2024-06-01 00:00:00') AS \"f\"" in ddl
    assert "JSON_BUILD_ARRAY(COALESCE(CAST(\"t\".\"d\" AS TEXT), '9999-12-31'))" in ddl and notes == []
    ddl, _ = _view(
        "select (`x`.`c` + interval 1 day) AS `n` from (select coalesce(`t`.`d`,'2024-06-01') AS `c` "
        "from `shop`.`t` `t`) `x`"
    )
    assert "(SELECT COALESCE(\"t\".\"d\", '2024-06-01') AS \"c\"" in ddl
    sql, result = _query("INSERT INTO t (id, d) SELECT id + 100, COALESCE(d, '2024-06-01') FROM t")
    assert sql.endswith("COALESCE(d, '2024-06-01') FROM t") and result.classification.value == "AUTO"
    # The texts differ: a fraction, a TIMESTAMP's zone, a date beside a date and time, a
    # string that is no date to PostgreSQL (MySQL's zero date).
    ddl, _ = _select(
        "coalesce(`t`.`dt6`,'2024-06-01 00:00:00') AS `a`,coalesce(`t`.`ts`,'2024-06-01 00:00:00') AS `b`,"
        "coalesce(`t`.`dt`,'2024-06-01') AS `c`,ifnull(`t`.`d`,'31/12/9999') AS `e`,ifnull(`t`.`d`,'0000-00-00') AS `z`"
    )
    assert "COALESCE(TO_CHAR(\"t\".\"dt6\", 'YYYY-MM-DD HH24:MI:SS.FF6'), '2024-06-01 00:00:00')" in ddl
    assert "COALESCE(TO_CHAR(\"t\".\"ts\", 'YYYY-MM-DD HH24:MI:SS'), '2024-06-01 00:00:00')" in ddl
    assert "COALESCE(CAST(\"t\".\"dt\" AS TEXT), '2024-06-01')" in ddl
    assert "COALESCE(CAST(\"t\".\"d\" AS TEXT), '31/12/9999')" in ddl
    assert "COALESCE(CAST(\"t\".\"d\" AS TEXT), '0000-00-00')" in ddl


def test_a_no_digit_string_compared_with_a_date_is_the_zero_date() -> None:
    """ES-3: WHERE IFNULL(dt, '') < NOW() was COALESCE(dt, NULL) < ... -- MySQL compares the
    zero date, below every date, and keeps the NULL rows -- silently, graded AUTO. In a date
    function it is no date, NULL, as MySQL reads it."""
    ddl, notes = _select("`t`.`id` AS `id`", where="(ifnull(`t`.`dt`,'') < now())")
    assert "WHERE (COALESCE(\"t\".\"dt\", CAST('-infinity' AS TIMESTAMP)) < DATE_TRUNC('second', LOCALTIMESTAMP))" in ddl
    ddl, _ = _select("`t`.`id` AS `id`", where="(ifnull(`t`.`d`,'n/a') < curdate())")
    assert "WHERE (COALESCE(\"t\".\"d\", CAST('-infinity' AS DATE)) < CURRENT_DATE)" in ddl
    ddl, _ = _select("year(ifnull(`t`.`d`,'')) AS `y`")
    assert 'EXTRACT(YEAR FROM CAST(COALESCE("t"."d", NULL) AS DATE))' in ddl and notes == []
    sql, result = _query("SELECT id FROM t WHERE IFNULL(dt, '') < NOW()")
    assert "COALESCE(dt, CAST('-infinity' AS TIMESTAMP))" in sql and result.classification.value == "AUTO"


def test_a_number_beside_number_strings_is_a_number_where_it_is_shown() -> None:
    """F3 / ES-4: IFNULL(qty, '0') shown in a derived table, a CTE, a view or an INSERT ...
    SELECT was text, so SUM over it, x + 1 or the insert into an INT column failed, graded
    AUTO (bcb86bd ran them: PostgreSQL reads '0' as the number, whose text is MySQL's
    string). SUM(DISTINCT ...) reads its value as SUM does."""
    ddl, notes = _view(
        "select sum(`x`.`q`) AS `s` from (select ifnull(`t`.`n`,'0') AS `q`,coalesce(`t`.`price`,'0') AS `p` "
        "from `shop`.`t` `t`) `x`"
    )
    assert "COALESCE(\"t\".\"n\", '0') AS \"q\", COALESCE(\"t\".\"price\", '0') AS \"p\"" in ddl and notes == []
    sql, _ = _query("INSERT INTO t (id, n) SELECT id + 200, IFNULL(n, '0') FROM t")
    assert sql == "INSERT INTO t (id, n) SELECT id + 200, COALESCE(n, '0') FROM t"
    ddl, _ = _select("sum(distinct ifnull(`t`.`n`,'0')) AS `s`")
    assert 'SUM(DISTINCT COALESCE(CAST("t"."n" AS DOUBLE PRECISION), 0))' in ddl
    # MySQL's string is seen in the statement itself: text there, as before.
    ddl, _ = _select(
        "ifnull(`t`.`n`,'0') AS `k`,ifnull(`t`.`n`,'N/A') AS `m`,ifnull(`t`.`n`,'007') AS `z`", tail=" order by `k`"
    )
    assert "COALESCE(CAST(\"t\".\"n\" AS TEXT), '0') AS \"k\"" in ddl
    assert "COALESCE(CAST(\"t\".\"n\" AS TEXT), 'N/A') AS \"m\"" in ddl
    assert "COALESCE(CAST(\"t\".\"n\" AS TEXT), '007') AS \"z\"" in ddl  # PostgreSQL's 7 is not '007'
    # Grouped by, the key is written as the select list's item (PostgreSQL matches them):
    # a number where nothing orders by it, MySQL's string when the query orders by it --
    # by its alias, its position or (grouped or DISTINCT) the expression, as a view stores
    # ORDER BY 1 (the final round's verifiers: the key stayed a number beside a texted
    # ORDER BY copy, refused at CREATE VIEW, and ORDER BY k ordered 10 before 9).
    ddl, _ = _select("ifnull(`t`.`n`,'0') AS `k`,count(0) AS `c`", tail=" group by ifnull(`t`.`n`,'0')")
    assert "SELECT COALESCE(\"t\".\"n\", '0') AS \"k\"" in ddl and "GROUP BY COALESCE(\"t\".\"n\", '0')" in ddl
    for tail in (" group by ifnull(`t`.`n`,'0') order by `k`",
                 " group by ifnull(`t`.`n`,'0') order by ifnull(`t`.`n`,'0')"):
        ddl, _ = _select("ifnull(`t`.`n`,'0') AS `k`,count(0) AS `c`", tail=tail)
        texted = "COALESCE(CAST(\"t\".\"n\" AS TEXT), '0')"
        assert f"SELECT {texted} AS \"k\"" in ddl and f"GROUP BY {texted}" in ddl, ddl
    ddl, _ = _select("ifnull(`t`.`n`,'0') AS `k`", tail=" order by ifnull(`t`.`n`,'0')")
    assert "SELECT COALESCE(\"t\".\"n\", '0') AS \"k\"" in ddl  # ungrouped: its ORDER BY copy is the string
    # A quotient's string is MySQL's at its scale (3.3333), not PostgreSQL's kept digits.
    ddl, _ = _select("ifnull((`t`.`n` / 3),'0') AS `q`")
    assert "COALESCE(CAST(ROUND(" in ddl and "AS TEXT), '0') AS \"q\"" in ddl


def test_greatest_of_a_number_and_a_clamp_string_stays_a_number() -> None:
    """ES-5 / RG5: GREATEST(x, '0') -- the clamp to zero -- was text, refused in + 1, SUM,
    compared with a number or SET n = ..., graded AUTO; 5d955cc ran it. Compared as strings
    a number and a number string below 2 order as the numbers do (checked on 8.4); beside
    '5' MySQL's string order is not the numbers' (GREATEST(10, '5') is '5'): text, as G113."""
    ddl, notes = _select(
        "(greatest(`t`.`n`,'0') + 1) AS `a`,sum(least(`t`.`price`,'0.5')) AS `b`", where="(greatest(`t`.`n`,'1') > 3)"
    )
    assert "GREATEST(\"t\".\"n\", '0') END + 1" in ddl and "LEAST(\"t\".\"price\", '0.5') END)" in ddl
    assert "GREATEST(\"t\".\"n\", '1') END > 3" in ddl and "AS TEXT" not in ddl and notes == []
    sql, result = _query("UPDATE t SET n = GREATEST(n, '0') WHERE id IN (1, 2)")
    assert "GREATEST(n, '0')" in sql and result.classification.value == "AUTO"
    ddl, _ = _select("greatest(`t`.`n`,'5') AS `a`,greatest(`t`.`n`,'-1') AS `b`")
    assert "GREATEST(CAST(\"t\".\"n\" AS TEXT), '5')" in ddl and "GREATEST(CAST(\"t\".\"n\" AS TEXT), '-1')" in ddl


def test_a_union_beside_literals_postgresql_reads_as_its_values_stays_as_written() -> None:
    """RG3 / ES-2 / F4: SELECT n ... UNION SELECT '5' was text in every branch, so SUM over
    it, n IN (... UNION SELECT '12'), x.v < CURDATE() and INSERT ... SELECT ... UNION ALL
    SELECT 2, '2.5' failed, graded AUTO; 5d955cc ran them. A text column or 'N/A' is text."""
    ddl, _ = _view(
        "select sum(`x`.`v`) AS `s` from (select `t`.`n` AS `v` from `shop`.`t` `t` union all select '5' AS `5`) `x`"
    )
    assert "(SELECT \"t\".\"n\" AS \"v\" FROM \"shop\".\"t\" AS \"t\" UNION ALL SELECT '5' AS \"5\")" in ddl
    ddl, _ = _view("select `t`.`d` AS `v` from `shop`.`t` `t` union all select '2024-01-01' AS `2024-01-01`")
    assert "SELECT \"t\".\"d\" AS \"v\" FROM" in ddl
    sql, _ = _query("SELECT id FROM t WHERE n IN (SELECT n FROM u UNION SELECT '12')")
    assert "IN (SELECT n FROM u UNION SELECT '12')" in sql
    sql, _ = _query("INSERT INTO t (id, price) SELECT 1, 1.5 UNION ALL SELECT 2, '2.5'")
    assert sql == "INSERT INTO t (id, price) SELECT 1, 1.5 UNION ALL SELECT 2, '2.5'"
    ddl, _ = _view("select `t`.`d` AS `v` from `shop`.`t` `t` union all select 'N/A' AS `N/A`")
    assert 'SELECT CAST("t"."d" AS TEXT) AS "v"' in ddl
    sql, _ = _query("SELECT n FROM t UNION ALL SELECT s FROM u")
    assert sql == "SELECT CAST(n AS TEXT) AS n FROM t UNION ALL SELECT s FROM u"


# --- Values whose type cannot be told, BINARY --------------------------------------------------


def test_binary_compared_with_a_value_of_unknown_type_is_the_value_itself() -> None:
    """RG2 / ES-7: BINARY x = 'str' over another view's column, or untyped, was x || '' --
    a TIMESTAMP's text with its zone, a boolean's 'true', x || '' = <integer> refused, the
    index lost -- with no note. 5d955cc compared the value itself."""
    ddl, notes = _view(
        "select `v`.`id` AS `id` from `shop`.`v_base` `v` where ((cast(`v`.`ts` as char charset binary) = "
        "'2024-01-02 03:04:05') and (cast(`v`.`i` as char charset binary) = `v`.`n`))"
    )
    assert "(\"v\".\"ts\" = '2024-01-02 03:04:05') AND (\"v\".\"i\" = \"v\".\"n\")" in ddl and notes == []
    sql, result = _query("SELECT id FROM users WHERE BINARY username = ?", typed=False)
    assert sql == "SELECT id FROM users WHERE username = %s" and result.classification.value == "AUTO"
    sql, _ = _query("SELECT t.id FROM t JOIN items ON BINARY items.tid = t.id", typed=False)
    assert sql.endswith("ON items.tid = t.id")
    sql, _ = _query("SELECT id FROM t WHERE BINARY n = '010'")
    assert sql == "SELECT id FROM t WHERE CAST(n AS TEXT) = '010'"  # a number known: its text


def test_a_column_of_unknown_type_in_like_or_a_string_function_is_read_as_text_unnoted() -> None:
    """RG12 / ES-15: x || '' LIKE 'al%' made every untyped LIKE / UPPER / TRIM MANUAL and kept
    a text column's index from serving it. Not noted any more; under LIKE / REGEXP the column
    is CAST(x AS TEXT), which that index still serves, and inside a string function x || ''
    -- the final round's verifiers: left bare, LEFT(created_at, 7) and d LIKE '2024-01%' over
    another view's date failed on Aurora DSQL. UPPER / LOWER keep it as written. LENGTH is
    still noted."""
    ddl, notes = _view("select `v`.`id` AS `id` from `shop`.`v_base` `v` where (`v`.`s` like 'al%')")
    assert "WHERE (CAST(\"v\".\"s\" AS TEXT) LIKE 'al%')" in ddl and notes == []
    sql, result = _query(
        "SELECT id, UPPER(s), TRIM(s), LEFT(d, 7), CONCAT(a, b), LPAD(id, 6, '0') FROM t WHERE s LIKE 'al%'",
        typed=False,
    )
    assert sql == (
        "SELECT id, UPPER(s), TRIM(s || ''), LEFT(d || '', 7), a || '' || b || '', LPAD(id || '', 6, '0') "
        "FROM t WHERE CAST(s AS TEXT) LIKE 'al%'"
    ) and result.classification.value == "AUTO"
    sql, result = _query("SELECT LENGTH(s) FROM t", typed=False)
    assert sql == "SELECT OCTET_LENGTH(s || '') FROM t" and result.classification.value == "MANUAL"


def test_order_by_binary_of_a_name_a_column_and_an_output_name_orders_by_the_column() -> None:
    """ES-8 / RG7: MySQL reads a name inside BINARY as the FROM column first; the alias reading
    ordered SELECT id, n + 90 AS n ... ORDER BY BINARY n by the output, graded AUTO."""
    sql, _ = _query("SELECT id, n + 90 AS n FROM t WHERE n IS NOT NULL ORDER BY BINARY n")
    assert sql.endswith("ORDER BY CONVERT_TO(CAST(n AS TEXT), 'UTF8') NULLS FIRST")
    sql, _ = _query("SELECT n AS s, s AS k FROM u ORDER BY BINARY s")
    assert sql.endswith("ORDER BY CONVERT_TO(s, 'UTF8') NULLS FIRST")  # not the output s
    sql, _ = _query("SELECT n * 2 AS x FROM t ORDER BY BINARY x")
    assert sql.endswith("ORDER BY CONVERT_TO(CAST((n * 2) AS TEXT), 'UTF8') NULLS FIRST")  # an output name only
    sql, _ = _query("SELECT s AS k FROM u ORDER BY BINARY k")
    assert sql.endswith("ORDER BY k NULLS FIRST")
    sql, result = _query("SELECT DISTINCT s AS s FROM u ORDER BY BINARY s")  # the column under its own name
    assert sql.endswith("ORDER BY s NULLS FIRST") and result.classification.value == "AUTO"


def test_the_unused_distinct_operand_set_is_gone() -> None:
    """RG14: listed_operand was filled and never read once ordered_by_bytes took over the
    SELECT DISTINCT case, which still orders by what it selects."""
    from dsql_migrator.core import converter

    assert "listed_operand" not in inspect.getsource(converter._rewrite_mysql_calls)
    ddl, _ = _view("select distinct `t`.`s` AS `s` from `shop`.`t` `t` order by cast(`t`.`s` as char charset binary)")
    assert ddl.endswith('ORDER BY "t"."s" NULLS FIRST')


# --- Single-database mode, derived quotients --------------------------------------------------------


def test_a_show_create_body_with_bare_tables_reads_the_views_own_schema() -> None:
    """ES112-2: in single-database mode SHOW CREATE VIEW writes the view's tables bare; the
    view was converted untyped (a quotient a double, a TINYINT(1) not a number, the TEMPTABLE
    rounding lost) and read FROM "t" -- the search path's. A CTE's name stays its own."""
    from dsql_migrator.core.converter import table_type_index, view_column_types

    body = (
        "CREATE ALGORITHM=TEMPTABLE DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS select "
        "(`t`.`n` / `t`.`id`) AS `q`,(`t`.`flag` + 1) AS `p` from `t` group by `t`.`id`"
    )
    ddl, notes = _view(body)
    assert 'ROUND((DIV(CAST("t"."n" AS DECIMAL) * 1000000000, NULLIF("t"."id", 0)) * 0.000000001), 4) AS "q"' in ddl
    assert '(CAST("t"."flag" AS INT) + 1) AS "p" FROM "shop"."t" GROUP BY' in ddl and notes == []
    assert view_column_types(body, table_type_index(_TABLES), schema="shop")[0][0] == "shop.t"
    ddl, _ = _view(
        "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v` AS with `c` as (select "
        "`t`.`id` AS `id` from `t`) select `c`.`id` AS `id`,`u`.`s` AS `s` from (`c` join `u` on((`u`.`id` = `c`.`id`)))"
    )
    assert 'WITH "c" AS (SELECT "t"."id" AS "id" FROM "shop"."t")' in ddl and 'FROM ("c" JOIN "shop"."u"' in ddl


def test_an_unqualified_derived_or_cte_quotient_is_shown_at_mysqls_scale() -> None:
    """F7: in a Query Converter statement q of (SELECT n / 3 AS q ...) x was written with every
    digit (0.333333333, LENGTH 11) where x.q was rounded (0.3333), graded AUTO."""
    sql, result = _query("SELECT q, q * 3, LENGTH(q), CONCAT(q, '') FROM (SELECT n / 3 AS q FROM t) x")
    assert sql.startswith(
        "SELECT ROUND(q, 4) AS q, ROUND(q * 3, 4), OCTET_LENGTH(CAST(ROUND(q, 4) AS TEXT)), CAST(ROUND(q, 4) AS TEXT) || ''"
    )
    assert result.classification.value == "AUTO"
    sql, _ = _query("WITH c AS (SELECT n / 3 AS q FROM t) SELECT q FROM c")
    assert "SELECT ROUND(q, 4) AS q FROM c" in sql
