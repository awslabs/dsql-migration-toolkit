"""G82: the review of G68-G70 -- what a MySQL view's conversion really does with its
function calls, the Evaluation name search, and a MySQL generated column's expression."""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)


def _view_ddl(body: str) -> "tuple[str, tuple, tuple]":
    from dsql_migrator.core.converter import _converted_mysql_view

    ddl, untranslated, misformatted = _converted_mysql_view("v", f"select {body} AS `x` from `t`")
    return " ".join(ddl.split()), untranslated, misformatted


@pytest.mark.parametrize(
    ("call", "emitted"),
    [
        ("year(d)", "CAST(EXTRACT(YEAR FROM CAST(d AS DATE)) AS INT)"),
        ("hour(d)", "CAST(EXTRACT(HOUR FROM d) AS INT)"),
        ("second(d)", "CAST(FLOOR(EXTRACT(SECOND FROM d)) AS INT)"),
        ("dayofweek(d)", "( CAST(EXTRACT(DOW FROM CAST(d AS DATE)) AS INT) + 1 )"),
        ("datediff(a, b)", "( CAST(a AS DATE) - CAST(b AS DATE) )"),
        ("time(d)", "CAST(d AS TIME)"),
        ("curtime()", "LOCALTIME"),
        ("variance(a)", "VAR_POP(a)"),
        ("stddev(a)", "STDDEV_POP(a)"),
        ("std(a)", "STDDEV_POP(a)"),
        ("cast(a as char)", "CAST(a AS TEXT)"),
        ("group_concat(a)", "STRING_AGG(CAST(a AS TEXT), ',')"),
        ("group_concat(a, b)", "STRING_AGG(CAST(a AS TEXT) || CAST(b AS TEXT), ',')"),
        ("group_concat(distinct a)", "STRING_AGG(DISTINCT CAST(a AS TEXT), ',')"),
        ("group_concat(a order by b)", "STRING_AGG(CAST(a AS TEXT), ',' ORDER BY b"),
        # A DISTINCT aggregate's ORDER BY must be its argument: as an array, any type.
        ("group_concat(distinct a order by a separator ';')",
         "ARRAY_TO_STRING(ARRAY_AGG(DISTINCT a ORDER BY a NULLS FIRST) FILTER(WHERE NOT a IS NULL), ';')"),
        # G88: what sqlglot reads alike, or writes with another meaning.
        ("var_samp(a)", "VAR_SAMP(a)"),
        ("stddev_samp(a)", "STDDEV_SAMP(a)"),
        ("concat_ws(' ', a, b)", "CONCAT_WS(' ', a, b)"),
        ("convert_tz(d, '+00:00', '+09:00')", "AT TIME ZONE INTERVAL '+00:00' AT TIME ZONE INTERVAL '+09:00'"),
        ("greatest(a, b)", "CASE WHEN a IS NULL OR b IS NULL THEN NULL ELSE GREATEST(a, b) END"),
        ("length(s)", "OCTET_LENGTH(s)"),
        ("cast(a as char charset utf8mb4)", "CAST(a AS TEXT)"),
        ("cast(a as char(10) charset utf8mb4)", "CAST(a AS VARCHAR(10))"),
        ("convert(a using utf8mb4)", "CAST(a AS TEXT)"),
        ("cast(a as char charset binary)", "CAST(a AS BYTEA)"),
        ("cast(a as unsigned)", "CAST(a AS DECIMAL(20))"),
        ("json_array(1, null)", "JSON_BUILD_ARRAY(1, NULL)"),
        ("json_arrayagg(a)", "JSON_AGG(a)"),
        ("weekofyear(d)", "CAST(EXTRACT(WEEK FROM CAST(d AS DATE)) AS INT)"),
        ("now(3)", "LOCALTIMESTAMP(3)"),
        ("dayname(d)", "TO_CHAR(d, 'FMDay')"),
        ("date_format(d, '%Y-%m-%dT%H:%i:%s')", "'YYYY-MM-DD\"T\"HH24:MI:SS'"),
        ("date_format(d, '%h:%i %p')", "'HH12:MI AM'"),
        ("date_format(d, 'Date: %Y')", "'\"Date: \"YYYY'"),
        ("str_to_date(s, '%d/%m/%Y %H:%i')", "TO_TIMESTAMP(s, 'DD/MM/YYYY HH24:MI')"),
        ("from_unixtime(a, '%Y-%m-%d')", "TO_CHAR(TO_TIMESTAMP(a), 'YYYY-MM-DD')"),
        ("now()", "NOW()"),
    ],
)
def test_a_mysql_call_is_written_as_postgresql_runs_it(call: str, emitted: str) -> None:
    """YEAR(), HOUR(), DATEDIFF() ... were emitted under names PostgreSQL lacks, STDDEV and
    VARIANCE as the SAMPLE forms (MySQL computes the population ones), CAST AS CHAR as
    char(1), GROUP_CONCAT of an integer as string_agg(int) -- all graded AUTO."""
    ddl, untranslated, misformatted = _view_ddl(call)
    assert emitted in ddl, ddl
    assert untranslated == () and misformatted == ()


@pytest.mark.parametrize(
    ("call", "named"),
    [
        ("week(d)", "WEEK"),
        ("substring_index(s, ',', 1)", "SUBSTRING_INDEX"),
        ("sha2(s, 256)", "SHA2"),
        ("json_keys(j)", "JSON_KEYS"),
        ("timestamp(d, t)", "TIMESTAMP"),
        ("find_in_set('a', s)", "find_in_set"),
        ("json_set(j, '$.a', 1)", "JSON_SET"),
        ("utc_time()", "UTC_TIME"),
        ("group_concat(distinct a order by b)", "GROUP_CONCAT"),
    ],
)
def test_a_call_postgresql_lacks_is_reported(call: str, named: str) -> None:
    _ddl, untranslated, _misformatted = _view_ddl(call)
    assert named in untranslated


def test_postgresql_builtins_are_not_reported_untranslated() -> None:
    """now() and octet_length() were graded MANUAL with advice to rewrite a working view."""
    for call in ("now()", "octet_length(s)", "regexp_substr(s, 'a')"):
        assert _view_ddl(call)[1] == (), call


def test_a_date_format_token_with_no_postgresql_pattern_is_reported() -> None:
    """'%U' (MySQL's Sunday weeks from 00) has no TO_CHAR pattern -- PostgreSQL's WW counts
    7-day blocks from Jan 1 -- and stayed graded AUTO (G88: every other token is mapped
    by the converter itself, %p to AM included)."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import SchemaConverter

    ddl, _untranslated, misformatted = _view_ddl("date_format(d, '%Y-%U')")
    assert misformatted == ("DATE_FORMAT (%U)",)
    assert """'YYYY"-%U"'""" in ddl  # visible in the DDL, never silently dropped
    view = ViewDef(name="shop.v_week", definition="select date_format(`d`, '%x-%u') AS `w` from `shop`.`o`")
    converted = SchemaConverter().convert_view(view)
    (note,) = converted.warnings
    assert "DATE_FORMAT (%u)" in note.message and "returns different text" in note.message
    inventory = SourceInventory(views=[view])
    (item,) = [i for i in CompatibilityAssessor().assess(inventory).items if i.object_name == "shop.v_week"]
    assert item.classification.value == "MANUAL"


def test_the_rule_search_never_matches_the_compatible_id_or_a_fragment() -> None:
    """'comp' listed every AUTO object (rule id COMPATIBLE)."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.ui.evaluation import filter_assessment_items

    tables = [
        TableDef(name="app.company", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)], primary_key=["id"]),
        TableDef(name="app.orders", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)], primary_key=["id"]),
        TableDef(name="app.blobs", columns=[ColumnDef(name="id", mysql_type="varbinary(16)", nullable=False)], primary_key=["id"]),
    ]
    items = CompatibilityAssessor().assess(SourceInventory(tables=tables)).items
    names = lambda needle: [i.object_name for i in filter_assessment_items(items, name=needle)]  # noqa: E731
    assert names("comp") == ["app.company"]
    assert names("bytea") == ["app.blobs"] and names("BYTEA_KEY") == ["app.blobs"]
    assert names("byt") == []


def test_a_mysql_8_generated_expression_is_shown_as_show_create_table_writes_it() -> None:
    """information_schema's extra escape layer was shown raw, the clause came after NOT
    NULL (MySQL's grammar rejects that order), and a garbled non-ASCII literal was offered
    for translation without the SHOW CREATE TABLE step."""
    from dsql_migrator.core.converter import (
        MYSQL_GENERATED_COLUMN_ADVICE,
        mysql_generated_column_advice,
    )
    from dsql_migrator.core.introspector import enrich_columns
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl
    from tests.test_introspector import _FakeConnection

    tables = [TableDef(
        name="users",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="doc", mysql_type="json"),
                 ColumnDef(name="email", mysql_type="varchar(255)", nullable=False),
                 ColumnDef(name="is_cafe", mysql_type="tinyint(1)")],
        primary_key=["id"],
    )]
    rows = [
        ("users", "id", None, "", "int", None, ""),
        ("users", "doc", None, "", "json", None, ""),
        ("users", "email", None, "STORED GENERATED", "varchar(255)", None,
         "json_unquote(json_extract(`doc`,_utf8mb4\\'$.email\\'))"),
        ("users", "is_cafe", None, "VIRTUAL GENERATED", "tinyint(1)", None,
         "(json_unquote(json_extract(`doc`,_utf8mb4\\'$.kind\\')) = _utf8mb4\\'cafÃ©\\')"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)
    email = tables[0].columns[2]
    assert email.mysql_generated_expression == "json_unquote(json_extract(`doc`,_utf8mb4'$.email'))"
    ddl = render_source_table_ddl(tables[0])
    assert (
        "`email` varchar(255) GENERATED ALWAYS AS "
        "(json_unquote(json_extract(`doc`,_utf8mb4'$.email'))) STORED NOT NULL"
    ) in ddl
    assert "non-ASCII expression" in ddl
    assert mysql_generated_column_advice(tables[0]) == MYSQL_GENERATED_COLUMN_ADVICE


def test_postgresql_source_views_are_untouched() -> None:
    """The rewrites are MySQL-only: a PostgreSQL view body is emitted verbatim."""
    from dsql_migrator.core.converter import SchemaConverter

    view = ViewDef(name="app.v", definition=" SELECT now() AS t;")
    converted = SchemaConverter(source_type=SourceType.POSTGRES).convert_view(view)
    assert "now()" in converted.target_ddl
