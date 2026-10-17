"""G97: the review of G95 -- MySQL expressions are typed through MySQL's stored joins and
by what an expression computes, a value of unknown type gets the form right for text and
bytes alike, the Query Converter is typed by the Evaluation inventory, and the clocks,
casts and date parsing match MySQL. Each form was run on MySQL 8.4 and PostgreSQL 17 over
the same rows (scripts/mysql_view_differential.py, local)."""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef

_ORDERS = TableDef(
    name="shop.orders",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="code", mysql_type="char(8)"),
        ColumnDef(name="created_at", mysql_type="datetime"),
        ColumnDef(name="paid", mysql_type="tinyint(1)"),
        ColumnDef(name="score", mysql_type="int"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="d", mysql_type="date"),
        ColumnDef(name="t", mysql_type="time"),
    ],
    primary_key=["id"],
)
_ITEMS = TableDef(
    name="shop.items",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="order_id", mysql_type="int"),
        ColumnDef(name="code", mysql_type="blob"),
        ColumnDef(name="fl", mysql_type="float"),
        ColumnDef(name="day_key", mysql_type="int"),
    ],
    primary_key=["id"],
)


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(
        ViewDef(name="shop.v", definition=body), tables=[_ORDERS, _ITEMS]
    )
    return " ".join(view.target_ddl.split()), view


def _query(sql: str, *, typed: bool = False):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    types = tuple(table_type_index([_ORDERS, _ITEMS]).values()) if typed else ()
    result = QueryConverter(column_types=types).convert(sql)
    return result.converted_sql, [w.code for w in result.warnings]


def test_a_joined_view_as_mysql_stores_it_is_typed() -> None:
    """MySQL stores JOIN ... ON as `from (a join b on(...))`: no column was typed, and a
    blob's LENGTH counted its hex text."""
    ddl, _ = _view(
        "select `i`.`id` AS `id`,length(`i`.`code`) AS `l`,convert(`i`.`code` using utf8mb4) AS `t`,"
        "concat_ws('-',`o`.`code`,`o`.`s`) AS `w`,json_array(`o`.`created_at`,`o`.`paid`) AS `j`,"
        "(`o`.`d` + interval 1 day) AS `n` from (`shop`.`orders` `o` join `shop`.`items` `i` "
        "on((`i`.`order_id` = `o`.`id`)))"
    )
    assert 'OCTET_LENGTH("i"."code")' in ddl
    assert "CONVERT_FROM(\"i\".\"code\", 'UTF8')" in ddl
    assert "CONCAT_WS('-', CAST(\"o\".\"code\" AS TEXT), \"o\".\"s\")" in ddl
    assert "TO_CHAR(\"o\".\"created_at\", 'YYYY-MM-DD HH24:MI:SS.US')" in ddl and 'CAST("o"."paid" AS INT)' in ddl
    assert "CAST(\"o\".\"d\" + INTERVAL '1 DAY' AS DATE)" in ddl


def test_a_value_of_unknown_type_gets_the_form_right_for_text_and_bytes() -> None:
    """Untyped, LENGTH(x) counted the hex text of a bytea, and BINARY x made bytes of it."""
    sql, codes = _query("SELECT LENGTH(body), CAST(body AS BINARY) FROM attachments")
    assert sql == "SELECT OCTET_LENGTH(body), CAST(body AS BYTEA) FROM attachments" and codes == []


def test_the_query_converter_is_typed_by_the_evaluation_inventory() -> None:
    sql, codes = _query(
        "SELECT LENGTH(i.code), CONVERT(i.code USING utf8mb4), LENGTH(o.code) "
        "FROM items i JOIN orders o ON o.id = i.order_id",
        typed=True,
    )
    assert "OCTET_LENGTH(i.code)" in sql and "CONVERT_FROM(i.code, 'UTF8')" in sql
    assert "OCTET_LENGTH(CAST(o.code AS TEXT))" in sql
    assert "MYSQL_SEMANTICS" in codes  # invalid bytes are an error, not NULL


def test_every_view_over_a_table_shares_its_type_entry() -> None:
    """Each view's cache key copied the table's whole column list: 5,000 views over two
    1,000-column tables kept 646 MB."""
    from dsql_migrator.core.converter import table_type_index, view_column_types

    index = table_type_index([_ORDERS, _ITEMS])
    first = view_column_types("select `o`.`id` from `shop`.`orders` `o`", index)
    second = view_column_types("select `o`.`s` from `shop`.`orders` `o`", index)
    assert first == second and first[0] is second[0] is index["shop.orders"]


def test_json_values_are_written_by_what_the_expression_computes() -> None:
    ddl, _ = _view(
        "select json_array(`o`.`t`,(`o`.`created_at` + interval 1 hour),cast(`o`.`d` as datetime),"
        "now(3)) AS `j` from `shop`.`orders` `o`"
    )
    assert "TO_CHAR(( CAST('2000-01-01' AS DATE) + \"o\".\"t\" ), 'HH24:MI:SS.US')" in ddl
    assert ddl.count("'YYYY-MM-DD HH24:MI:SS.US'") == 3


def test_a_column_read_through_an_aliased_view_is_written_alias_dot_column() -> None:
    """MySQL stores `db`.`alias`.`col`; PostgreSQL refused it, and the alias was typed as a
    table of that name."""
    ddl, view = _view(
        "select `shop`.`items`.`id` AS `id`,length(`shop`.`items`.`code`) AS `l` "
        "from `shop`.`v_base` `items`"
    )
    assert '"items"."id"' in ddl and '"shop"."items"."code"' not in ddl
    assert 'OCTET_LENGTH("items"."code")' in ddl  # untyped: no CONVERT_FROM of a char


@pytest.mark.parametrize(
    ("charset", "encoding"),
    [("latin1", "WIN1252"), ("euckr", "EUC_KR"), ("sjis", "SJIS"), ("utf8mb4", "UTF8")],
)
def test_bytes_are_decoded_in_their_charsets_encoding(charset: str, encoding: str) -> None:
    ddl, _ = _view(f"select convert(`i`.`code` using {charset}) AS `t` from `shop`.`items` `i`")
    assert f"CONVERT_FROM(\"i\".\"code\", '{encoding}')" in ddl


def test_a_charset_with_no_encoding_is_reported() -> None:
    _ddl, view = _view("select convert(`i`.`code` using armscii8) AS `t` from `shop`.`items` `i`")
    assert any("no encoding for" in w.message for w in view.warnings)


def test_nested_greatest_and_least_grow_linearly() -> None:
    body = "`o`.`score`"
    for level in range(12):
        body = f"{'greatest' if level % 2 else 'least'}({body},{level})"
    ddl, _ = _view(f"select {body} AS `x` from `shop`.`orders` `o`")
    assert len(ddl) < 2000 and ddl.count("IS NULL") == 1


def test_binary_casts_keep_bytes_where_bytes_are_compared() -> None:
    ddl, view = _view(
        "select `i`.`id` AS `id` from (`shop`.`orders` `o` join `shop`.`items` `i` "
        "on((`i`.`order_id` = `o`.`id`))) where (`i`.`code` = cast(`o`.`s` as char charset binary))"
    )
    assert "\"i\".\"code\" = CONVERT_TO(\"o\".\"s\", 'UTF8')" in ddl
    _ddl, view = _view("select cast(`o`.`s` as char(4) charset binary) AS `b` from `shop`.`orders` `o`")
    assert any("BINARY(n)" in w.message for w in view.warnings)


def test_a_qualifier_is_matched_exactly() -> None:
    """o (outer) was typed as the inner scope's O."""
    ddl, _ = _view(
        "select (select length(`o`.`code`) from `shop`.`items` `O` where (`O`.`order_id` = `o`.`id`) "
        "limit 1) AS `l` from `shop`.`orders` `o`"
    )
    assert 'OCTET_LENGTH(CAST("o"."code" AS TEXT))' in ddl


def test_a_char_length_read_earlier_is_not_a_later_converts() -> None:
    sql, _ = _query(
        "SELECT jt.n FROM t, JSON_TABLE(t.j, '$[*]' COLUMNS (n CHAR(3) PATH '$.n')) AS jt "
        "ORDER BY CONVERT(t.descr USING utf8mb4)"
    )
    assert "CAST(t.descr AS TEXT)" in sql and "VARCHAR(3)" not in sql


@pytest.mark.parametrize(
    ("mysql", "emitted"),
    [
        ("NOW()", "DATE_TRUNC('second', LOCALTIMESTAMP)"),
        ("LOCALTIME", "DATE_TRUNC('second', LOCALTIMESTAMP)"),
        ("LOCALTIMESTAMP(3)", "DATE_TRUNC('milliseconds', LOCALTIMESTAMP)"),
        ("NOW(6)", "LOCALTIMESTAMP"),
        ("CURTIME()", "CAST(DATE_TRUNC('second', LOCALTIMESTAMP) AS TIME)"),
        ("UTC_TIMESTAMP", "DATE_TRUNC('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"),
        ("UTC_DATE()", "CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)"),
    ],
)
def test_clocks_are_truncated_as_mysql_truncates_them(mysql: str, emitted: str) -> None:
    """LOCALTIMESTAMP(0) and casts to timestamp(p) ROUND -- half the time into the next
    second, at midnight into the next day; LOCALTIME is MySQL's NOW(), not a time of day."""
    sql, codes = _query(f"SELECT {mysql} AS x")
    assert sql == f"SELECT {emitted} AS x" and codes == []


def test_a_literal_str_to_date_is_not_reported() -> None:
    sql, codes = _query("SELECT * FROM t WHERE created_at >= STR_TO_DATE('2024-01-02', '%Y-%m-%d')")
    assert codes == [] and "TO_DATE(" in sql


@pytest.mark.parametrize(
    ("call", "emitted"),
    [
        ("(cast(`o`.`created_at` as date) + interval 7 day)",
         "CAST(CAST(\"o\".\"created_at\" AS DATE) + INTERVAL '7 DAY' AS DATE)"),
        ("(last_day(`o`.`d`) + interval 1 day)", "AS DATE) + INTERVAL '1 DAY' AS DATE)"),
        ("(curdate() + interval 1 day)", "CAST(CURRENT_DATE + INTERVAL '1 DAY' AS DATE)"),
        ("('2024-01-31' + interval 1 month)",
         "CAST(CAST('2024-01-31' AS DATE) + INTERVAL '1 MONTH' AS DATE)"),
        ("truncate(`i`.`fl`,2)", "TRUNC(CAST(CAST(\"i\".\"fl\" AS DOUBLE PRECISION) AS DECIMAL), 2)"),
        ("truncate(`i`.`id`,1)", "\"i\".\"id\" AS \"x\""),
        ("date_format(`o`.`t`,'%h:%i %p')", "TO_CHAR(CAST(( CURRENT_DATE + \"o\".\"t\" ) AS TIMESTAMP)"),
        ("str_to_date(`i`.`day_key`,'%Y%m%d')", "TO_DATE(NULLIF(CAST(\"i\".\"day_key\" AS TEXT), '')"),
        ("date_format(`i`.`day_key`,'%Y-%m')", "TO_DATE(CAST(\"i\".\"day_key\" AS TEXT), 'YYYYMMDD')"),
        ("str_to_date('0930','%k%i')", "TO_TIMESTAMP('0930', 'HH24\"\"MI')"),
        ("str_to_date('2024-01-15.5','%Y-%m-%d.%f')", "AS TIMESTAMP)"),
    ],
)
def test_dates_are_typed_and_parsed_as_mysql_does(call: str, emitted: str) -> None:
    ddl, _ = _view(f"select {call} AS `x` from (`shop`.`orders` `o` join `shop`.`items` `i` on((`i`.`order_id` = `o`.`id`)))")
    assert emitted in ddl, ddl


def test_a_format_chosen_at_run_time_is_a_datetime() -> None:
    ddl, _ = _view(
        "select str_to_date(`o`.`s`,if(`o`.`paid`,'%d/%m/%Y','%e/%c/%y')) AS `x` from `shop`.`orders` `o`"
    )
    assert "AT TIME ZONE 'UTC' AS TIMESTAMP)" in ddl and "TO_DATE(" not in ddl


def test_the_str_to_date_note_says_both_ways_it_differs() -> None:
    _ddl, view = _view("select str_to_date(`o`.`s`,'%d/%m/%Y') AS `x` from `shop`.`orders` `o`")
    message = " ".join(w.message for w in view.warnings)
    assert "raise an error" in message and "separators differ" in message


def test_json_unquote_is_reported_once() -> None:
    _sql, codes = _query("SELECT JSON_UNQUOTE(doc) FROM docs")
    assert codes == ["JSON_UNQUOTE_UNSUPPORTED"]


def test_a_views_having_names_its_alias_as_mysql_stores_it() -> None:
    """MySQL stores having (`total` > 1000); Aurora DSQL never reads an output name there."""
    ddl, _ = _view(
        "select `shop`.`orders`.`score` AS `score`,sum(`shop`.`orders`.`id`) AS `total` "
        "from `shop`.`orders` group by `shop`.`orders`.`score` having (`total` > 1000)"
    )
    assert 'HAVING ( SUM("shop"."orders"."id") > 1000 )' in ddl
