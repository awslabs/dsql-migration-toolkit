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
        # A DISTINCT aggregate's ORDER BY must be its argument: no cast there.
        ("group_concat(distinct a order by a separator ';')", "STRING_AGG(DISTINCT a, ';' ORDER BY a"),
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
        ("now(3)", "now"),
        ("find_in_set('a', s)", "find_in_set"),
    ],
)
def test_a_call_postgresql_lacks_is_reported(call: str, named: str) -> None:
    _ddl, untranslated, _misformatted = _view_ddl(call)
    assert named in untranslated


def test_postgresql_builtins_are_not_reported_untranslated() -> None:
    """now() and octet_length() were graded MANUAL with advice to rewrite a working view."""
    for call in ("now()", "octet_length(s)", "regexp_substr(s, 'a')"):
        assert _view_ddl(call)[1] == (), call


def test_a_date_format_token_with_no_postgresql_spelling_is_reported() -> None:
    """'%p' stayed in TO_CHAR's format: the view was created and returned '03:15 %p'."""
    from dsql_migrator.core.assessor import CompatibilityAssessor
    from dsql_migrator.core.converter import SchemaConverter

    _ddl, _untranslated, misformatted = _view_ddl("date_format(d, '%h:%i %p')")
    assert misformatted == ("DATE_FORMAT (%p)",)
    view = ViewDef(name="shop.v_clock", definition="select date_format(`d`, '%h:%i %p') AS `t` from `shop`.`o`")
    converted = SchemaConverter().convert_view(view)
    (note,) = converted.warnings
    assert "DATE_FORMAT (%p)" in note.message and "returns different text" in note.message
    inventory = SourceInventory(views=[view])
    (item,) = [i for i in CompatibilityAssessor().assess(inventory).items if i.object_name == "shop.v_clock"]
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
