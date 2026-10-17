"""G95: the review of G87/G88 -- MySQL expression semantics, written once for a view and a
Query Converter statement alike, by the types of the columns they read where those are
known. Each expected form was checked against MySQL 8.4 and PostgreSQL 17 over the same
rows (a view MySQL stored, converted, and run on both)."""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef

_ORDERS = TableDef(
    name="shop.orders",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="code", mysql_type="char(8)"),
        ColumnDef(name="body", mysql_type="blob"),
        ColumnDef(name="created_at", mysql_type="datetime"),
        ColumnDef(name="paid", mysql_type="tinyint(1)"),
        ColumnDef(name="score", mysql_type="int"),
        ColumnDef(name="ratio", mysql_type="decimal(10,2)"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
        ColumnDef(name="tz", mysql_type="varchar(10)"),
        ColumnDef(name="d", mysql_type="date"),
    ],
    primary_key=["id"],
)


def _view(select_list: str, *, where: str = ""):
    """A MySQL view as MySQL stores it (backquoted, alias-qualified), converted."""
    from dsql_migrator.core.converter import SchemaConverter

    body = f"select {select_list} from `shop`.`orders` `o`" + (f" where {where}" if where else "")
    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=[_ORDERS])
    return " ".join(view.target_ddl.split()), view


def _query(sql: str):
    from dsql_migrator.core.query_converter import QueryConverter

    result = QueryConverter().convert(sql)
    return result.converted_sql, result


@pytest.mark.parametrize(
    ("mysql", "emitted"),
    [
        ("VARIANCE(x)", "VAR_POP(x)"),
        ("STDDEV(x)", "STDDEV_POP(x)"),
        ("VAR_SAMP(x)", "VAR_SAMP(x)"),
        ("CAST(id AS CHAR)", "CAST(id AS TEXT)"),
        ("CAST(id AS CHAR(4))", "CAST(id AS VARCHAR(4))"),
        ("GREATEST(a, b)", "CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE GREATEST(a, b) END"),
        ("CONCAT_WS(',', a, b)", "CONCAT_WS(',', a, b)"),
        ("JSON_ARRAY(1, NULL)", "JSON_BUILD_ARRAY(1, NULL)"),
        ("NOW()", "DATE_TRUNC('second', LOCALTIMESTAMP)"),
        ("CURRENT_TIMESTAMP", "DATE_TRUNC('second', LOCALTIMESTAMP)"),
        ("DATE_FORMAT(d, '%Y-%m-%d')", "TO_CHAR(CAST(d AS TIMESTAMP), 'YYYY-MM-DD')"),
    ],
)
def test_the_query_converter_writes_mysql_expressions_as_a_view_does(mysql: str, emitted: str) -> None:
    """The Query Converter read plain MySQL: VARIANCE as the sample one, CAST AS CHAR as
    char(1) (123 became '1'), GREATEST skipping NULLs -- all AUTO, beside a view's right
    forms."""
    sql, _result = _query(f"SELECT {mysql} FROM t")
    assert emitted in sql, sql


def test_a_nested_greatest_keeps_mysql_null_semantics() -> None:
    ddl, _ = _view("least(greatest(`o`.`score`,0),100) AS `clamped`")
    # G97: the whole nest's arguments checked once, by the outermost call.
    assert 'CASE WHEN "o"."score" IS NULL THEN NULL ELSE LEAST(GREATEST("o"."score", 0), 100) END' in ddl
    assert "OR 0 IS NULL" not in ddl and "100 IS NULL" not in ddl  # a literal is never NULL


def test_a_char_columns_padding_is_left_out_where_mysql_leaves_it_out() -> None:
    ddl, _ = _view(
        "length(`o`.`code`) AS `l`, length(`o`.`body`) AS `b`, "
        "group_concat(distinct `o`.`code` order by `o`.`code` ASC separator ',') AS `c`, "
        "concat_ws('-',`o`.`code`,`o`.`s`) AS `w`"
    )
    assert 'OCTET_LENGTH(CAST("o"."code" AS TEXT))' in ddl
    assert 'OCTET_LENGTH("o"."body")' in ddl  # its own bytes, not its hex text
    assert 'DISTINCT CAST("o"."code" AS TEXT) ORDER BY CAST("o"."code" AS TEXT)' in ddl
    assert "CONCAT_WS('-', CAST(\"o\".\"code\" AS TEXT), \"o\".\"s\")" in ddl


def test_each_char_cast_keeps_its_own_length() -> None:
    """Lengths were read in text order and paired in tree order: the wrong column was cut."""
    ddl, _ = _view(
        "concat(cast(`o`.`created_at` as char(10) charset utf8mb4),' #',`o`.`id`) AS `label`, "
        "convert(`o`.`s` using utf8mb4) AS `customer`, "
        "concat('Customer\\'s code: ',cast(`o`.`code` as char(4) charset utf8mb4)) AS `c`"
    )
    assert 'CAST("o"."created_at" AS VARCHAR(10))' in ddl
    assert 'CAST("o"."s" AS TEXT) AS "customer"' in ddl
    assert 'CAST(CAST("o"."code" AS TEXT) AS VARCHAR(4))' in ddl or 'CAST("o"."code" AS VARCHAR(4))' in ddl


def test_bytes_and_binary_casts() -> None:
    ddl, _ = _view(
        "convert(`o`.`body` using utf8mb4) AS `t`, cast(`o`.`body` as char(10) charset utf8mb4) AS `t10`, "
        "cast(`o`.`id` as char charset binary) AS `b`",
        where="(`o`.`s` like cast('Ab%' as char charset binary))",
    )
    assert "CAST(CONVERT_FROM(\"o\".\"body\", 'UTF8') AS TEXT)" in ddl
    assert "CAST(CONVERT_FROM(\"o\".\"body\", 'UTF8') AS VARCHAR(10))" in ddl
    assert "CONVERT_TO(CAST(\"o\".\"id\" AS TEXT), 'UTF8')" in ddl
    assert "LIKE 'Ab%'" in ddl  # byte-wise already on Aurora DSQL


def test_nested_calls_are_all_rewritten() -> None:
    ddl, view = _view(
        "json_arrayagg(json_array(`o`.`id`,`o`.`code`)) AS `j`, json_array(`o`.`id`,now(3)) AS `n`, "
        "date_format(str_to_date(`o`.`s`,'%d/%m/%Y'),'%Y-%m-%d') AS `f`"
    )
    assert "JSON_AGG(JSON_BUILD_ARRAY(" in ddl and "JSON_ARRAY(" not in ddl
    assert "DATE_TRUNC('milliseconds', LOCALTIMESTAMP)" in ddl and "NOW(" not in ddl
    assert "TO_DATE(NULLIF(\"o\".\"s\", ''), 'DD/MM/YY')" in ddl and "%" not in ddl
    assert view.auto_converted and all("json_array" not in w.message for w in view.warnings)


def test_json_values_are_written_as_mysql_writes_them() -> None:
    ddl, _ = _view("json_array(`o`.`id`,`o`.`created_at`,`o`.`paid`) AS `j`, json_object('k',`o`.`created_at`) AS `o`")
    assert "TO_CHAR(\"o\".\"created_at\", 'YYYY-MM-DD HH24:MI:SS.US')" in ddl
    assert 'CAST("o"."paid" AS INT)' in ddl


def test_unsigned_wraps_a_negative_number_as_mysql_does() -> None:
    ddl, _ = _view("cast((`o`.`score` - 60) as unsigned) AS `u`, cast(`o`.`ratio` as unsigned) AS `r`")
    assert "18446744073709551616" in ddl
    assert "ROUND(CAST(\"o\".\"ratio\" AS DECIMAL))" in ddl or "ROUND(CAST(\"o\".\"ratio\" AS NUMERIC))" in ddl


def test_a_distinct_group_concat_ordered_by_another_expression_is_explained() -> None:
    _ddl, view = _view("group_concat(distinct `o`.`s` order by `o`.`created_at` ASC separator ',') AS `x`")
    messages = [w.message for w in view.warnings]
    assert any("ORDER BY must be its argument" in m for m in messages)
    assert not any("does not translate" in m for m in messages)


@pytest.mark.parametrize(
    ("call", "emitted"),
    [
        ("str_to_date(`o`.`s`,'%H:%i')", "CAST(TO_TIMESTAMP(\"o\".\"s\", 'HH24:MI') AS TIME)"),
        ("str_to_date(`o`.`s`,'%d/%m/%Y')", "TO_DATE(NULLIF(\"o\".\"s\", ''), 'DD/MM/YY')"),
        ("str_to_date(`o`.`s`,'%Y-%m-%d %H:%i')",
         "CAST(TO_TIMESTAMP(NULLIF(\"o\".\"s\", '') || ' +00', 'YY-MM-DD HH24:MI TZH') AT TIME ZONE 'UTC' AS TIMESTAMP)"),
        ("str_to_date(`o`.`s`,'%Y%m%d')", "TO_DATE(NULLIF(\"o\".\"s\", ''), 'YYYY\"\"MM\"\"DD')"),
        # %% is a %, an unknown specifier its letter, adjacent patterns kept apart.
        ("date_format(`o`.`created_at`,'%Y%%i|%d%a|%A, %-d')", "'YYYY\"%i|\"DD\"\"Dy\"|A, -d\"'"),
        ("dayname(`o`.`s`)", "TO_CHAR(CAST(\"o\".\"s\" AS DATE), 'FMDay')"),
        ("utc_date()", "CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)"),
        ("utc_timestamp()", "DATE_TRUNC('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC')"),
        ("utc_time()", "CAST(DATE_TRUNC('second', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') AS TIME)"),
        ("(to_days(`o`.`d`) - to_days('2024-01-01'))",
         "CAST(\"o\".\"d\" AS DATE) - CAST('0001-01-01' AS DATE)"),
        ("week(`o`.`d`,3)", "CAST(EXTRACT(WEEK FROM CAST(\"o\".\"d\" AS DATE)) AS INT)"),
        ("truncate(`o`.`ratio`,1)", "TRUNC(\"o\".\"ratio\", 1)"),  # a decimal is truncated as is
        ("(`o`.`d` + interval 1 month)", "CAST(\"o\".\"d\" + INTERVAL '1 MONTH' AS DATE)"),
    ],
)
def test_dates_are_written_as_mysql_computes_them(call: str, emitted: str) -> None:
    ddl, _ = _view(f"{call} AS `x`")
    assert emitted in ddl, ddl


def test_a_zone_read_at_run_time_is_an_iso_offset_too() -> None:
    ddl, _ = _view(
        "convert_tz(`o`.`created_at`,'+00:00',`o`.`tz`) AS `a`, "
        "convert_tz(`o`.`created_at`,'+00:00',if(`o`.`paid`,'+10:00','+09:00')) AS `b`"
    )
    assert "TRANSLATE(\"o\".\"tz\", '+-', '-+')" in ddl
    assert ddl.count("~ '^[+-][0-9]{1,2}:[0-9]{2}$'") == 2


def test_formats_read_at_run_time_and_unmatched_values_are_reported() -> None:
    ddl, view = _view(
        "date_format(`o`.`d`,if(`o`.`paid`,'%M %e, %Y','%Y-%m-%d')) AS `a`, "
        "date_format(`o`.`d`,`o`.`s`) AS `b`, str_to_date(`o`.`s`,'%d/%m/%Y') AS `c`"
    )
    assert "'FMMonth FMDD, YYYY'" in ddl and "'YYYY-MM-DD'" in ddl
    messages = " ".join(w.message for w in view.warnings)
    assert "format read at run time" in messages and "raise an error" in messages
    # The IF's condition is no format.
    assert "CASE WHEN \"o\".\"paid\"" in ddl


def test_an_unmapped_from_unixtime_specifier_is_named_by_its_call() -> None:
    _ddl, view = _view("from_unixtime(`o`.`id`,'%X-%V') AS `x`")
    assert any("FROM_UNIXTIME (%X)" in w.message for w in view.warnings)


def test_the_source_pane_shows_what_mysql_computes() -> None:
    from dsql_migrator.ui.schema_conversion import render_source_view_ddl

    shown = render_source_view_ddl(ViewDef(
        name="shop.v",
        definition="select var_samp(`o`.`ratio`) AS `v`, cast(`o`.`code` as char(4) charset utf8mb4) AS `c` "
                   "from `shop`.`orders` `o`",
    ))
    assert "VAR_SAMP(" in shown and "VARIANCE(" not in shown
    assert "CHAR(4) CHARACTER SET utf8mb4" in shown


def test_evaluation_grades_a_view_by_the_same_column_types() -> None:
    """VIEW_NOT_CONVERTED converted each view without the tables, so a view over a binary
    column could be graded unlike Schema Conversion's conversion of it."""
    from dsql_migrator.core.assessor import MySqlViewConversionRule
    from dsql_migrator.core.converter import SchemaConverter

    body = "select str_to_date(`o`.`s`,'%d/%m/%Y') AS `c` from `shop`.`orders` `o`"
    inventory = SourceInventory(tables=[_ORDERS], views=[ViewDef(name="shop.v", definition=body)])
    findings = MySqlViewConversionRule().evaluate(inventory)
    converted = SchemaConverter().convert(inventory).views[0]
    assert [f.risk for f in findings] == [w.message for w in converted.warnings]
