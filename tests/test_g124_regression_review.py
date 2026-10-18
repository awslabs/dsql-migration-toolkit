"""G124: the medium regressions the final round's verifiers found in G118-G122 -- a quotient
a materialized CTE or derived table stores at its scale is no key to flag, a number beside
number strings is one decision per select item (its GROUP BY key and an ORDER BY copy of it
alike), and a column of unknown type inside a string function or under LIKE / REGEXP is read
as text. Each form was run on MySQL 8.4 and PostgreSQL 17 over the same rows
(scripts/mysql_view_differential.py, local) and on Aurora DSQL
(scripts/mysql_view_differential_dsql.py)."""

from __future__ import annotations

import re

from dsql_migrator.core.models import ColumnDef, TableDef, ViewDef

_T = TableDef(
    name="shop.t",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="n", mysql_type="int"),
        ColumnDef(name="price", mysql_type="decimal(10,2)"),
        ColumnDef(name="s", mysql_type="varchar(40)"),
    ],
    primary_key=["id"],
)


def _norm(sql: str) -> str:
    return re.sub(r"\s+\)", ")", re.sub(r"\(\s+", "(", " ".join(sql.split())))


def _view(body: str):
    from dsql_migrator.core.converter import SchemaConverter

    view = SchemaConverter().convert_view(ViewDef(name="shop.v", definition=body), tables=[_T])
    return _norm(view.target_ddl), [w.message for w in view.warnings]


def _query(sql: str):
    from dsql_migrator.core.converter import table_type_index
    from dsql_migrator.core.query_converter import QueryConverter

    result = QueryConverter(column_types=tuple(table_type_index([_T]).values())).convert(sql)
    return _norm(result.converted_sql or ""), result


def test_a_quotient_a_materialized_cte_or_derived_table_stores_is_no_key_to_flag() -> None:
    """RANK() OVER (ORDER BY m) over a CTE's AVG was MANUAL: the CTE groups, so MySQL stores
    m at its scale (decimal(14,6)) and so does the conversion -- both rank the stored value.
    A derived table MySQL merges still hands on every digit kept, and stays flagged."""
    cte = ("with `a` as (select `t`.`s` AS `s`,avg(`t`.`price`) AS `m` from `shop`.`t` `t` group by `t`.`s`) "
           "select `a`.`s` AS `s`,rank() OVER (ORDER BY `a`.`m` )  AS `r` from `a`")
    ddl, notes = _view(cte)
    assert "RANK() OVER (ORDER BY \"a\".\"m\" NULLS FIRST)" in ddl and notes == []
    ddl, notes = _view("select `x`.`m` AS `m`,count(0) AS `c` from (select `t`.`s` AS `s`,avg(`t`.`price`) AS `m` "
                       "from `shop`.`t` `t` group by `t`.`s`) `x` group by `x`.`m`")
    assert "GROUP BY \"x\".\"m\"" in ddl and notes == []
    sql, result = _query("WITH a AS (SELECT s, AVG(price) AS m FROM t GROUP BY s) SELECT s, RANK() OVER (ORDER BY m) FROM a")
    assert "RANK() OVER (ORDER BY m NULLS FIRST)" in sql and result.classification.value == "AUTO"
    _, notes = _view("select `x`.`q` AS `q`,count(0) AS `c` from (select `t`.`id` AS `id`,(`t`.`n` / 3) AS `q` "
                     "from `shop`.`t` `t`) `x` group by `x`.`q`")
    assert any("quotient" in note and "GROUP BY" in note for note in notes)


def test_a_number_beside_number_strings_is_one_decision_per_select_item() -> None:
    """GROUP BY 1 ORDER BY 1 (a view stores both as the expression) wrote the ORDER BY copy
    as MySQL's string and the select item and its key as the number: PostgreSQL refused the
    view (the ORDER BY copy is not grouped). Ordered by, the item, its key and the copy are
    MySQL's string, as MySQL orders it ('10' before '9'); unordered, all three the number."""
    texted = "COALESCE(CAST(\"t\".\"n\" AS TEXT), '0')"
    for tail in (" group by ifnull(`t`.`n`,'0') order by ifnull(`t`.`n`,'0')", " group by ifnull(`t`.`n`,'0') order by `k`"):
        ddl, _ = _view(f"select ifnull(`t`.`n`,'0') AS `k`,count(0) AS `c` from `shop`.`t` `t`{tail}")
        assert f"SELECT {texted} AS \"k\"" in ddl and f"GROUP BY {texted}" in ddl, ddl
        assert "ORDER BY \"k\"" in ddl or f"ORDER BY {texted}" in ddl, ddl
    ddl, _ = _view("select distinct ifnull(`t`.`n`,'0') AS `k` from `shop`.`t` `t` order by ifnull(`t`.`n`,'0')")
    assert f"SELECT DISTINCT {texted} AS \"k\"" in ddl and f"ORDER BY {texted}" in ddl
    sql, result = _query("SELECT IFNULL(n, '0') AS k, COUNT(*) FROM t GROUP BY 1 ORDER BY 1")
    assert sql.startswith("SELECT COALESCE(CAST(n AS TEXT), '0') AS k") and result.classification.value == "AUTO"
    sql, _ = _query("SELECT IFNULL(n, '0') AS k, COUNT(*) FROM t GROUP BY 1")
    assert sql == "SELECT COALESCE(n, '0') AS k, COUNT(*) FROM t GROUP BY 1"


def test_a_column_of_unknown_type_in_a_string_function_or_like_is_read_as_text() -> None:
    """LEFT(v.d, 7) and v.d LIKE '2024-01%' over another view's date failed on Aurora DSQL
    (no left(date, integer), no date ~~ text): inside a string function the column is
    x || '' (MySQL's text of a date, a number or text), under LIKE / REGEXP CAST(x AS TEXT),
    which a text column's index still serves. UPPER keeps it as written. None is noted."""
    ddl, notes = _view("select left(`v`.`d`,7) AS `m`,count(0) AS `c` from `shop`.`v_base` `v` "
                       "where (`v`.`d` like '2024-01%') group by left(`v`.`d`,7)")
    assert "SELECT LEFT(\"v\".\"d\" || '', 7) AS \"m\"" in ddl and "GROUP BY LEFT(\"v\".\"d\" || '', 7)" in ddl
    assert "WHERE (CAST(\"v\".\"d\" AS TEXT) LIKE '2024-01%')" in ddl and notes == []
    ddl, notes = _view("select upper(`v`.`s`) AS `u` from `shop`.`v_base` `v` where regexp_like(`v`.`s`,'^a')")
    assert "SELECT UPPER(\"v\".\"s\") AS \"u\"" in ddl and "WHERE CAST(\"v\".\"s\" AS TEXT) ~ '^a'" in ddl and notes == []
