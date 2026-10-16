# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL source text reaches the target VERBATIM: view bodies, column DEFAULTs and
STORED generated expressions.

All three are PostgreSQL's own deparse (``pg_get_viewdef`` / ``pg_get_expr``), so Aurora
DSQL -- which reads SQL with PostgreSQL 16's own parser -- needs no translation. They used
to be re-rendered by sqlglot, which moved a ``::text`` cast from a jsonb key onto the whole
``->`` (jsonb became text), turned ``date_part`` and ``CURRENT_TIME`` into syntax errors,
dropped a ``WITH ORDINALITY`` alias, inverted ``ROW(..) IS NOT NULL``, swapped the PG13
``"position"`` arguments and lost the time zone of a 3-argument ``date_trunc`` -- each
graded auto-converted with no warning. The corpus below is real ``pg_get_viewdef`` /
``pg_get_expr`` output (leading space, ``'k'::text`` labels, terminating semicolon), and
every body must come out byte for byte.
"""

import pytest

from dsql_migrator.core.converter import (
    SchemaConverter,
    parse_target_column_constraints,
    parse_target_column_types,
    parse_target_generated_columns,
    parse_target_identity_columns,
    parse_target_primary_key,
)
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)
from dsql_migrator.ui.schema_conversion import build_view_preview, render_source_view_ddl

_PG = SourceType.POSTGRES

# name -> pg_get_viewdef(oid) output, exactly as PostgreSQL returns it.
_VIEWDEF_CORPUS = {
    # jsonb operators: PostgreSQL binds `::` tighter than any operator, so the cast
    # belongs to the KEY. sqlglot bound it to the whole expression.
    "jsonb_arrow": (
        " SELECT o.id,\n"
        "    (o.payload -> 'items'::text) AS items\n"
        "   FROM app.orders o;"
    ),
    "jsonb_arrow_chain": (
        " SELECT o.id,\n"
        "    ((o.payload -> 'customer'::text) ->> 'email'::text) AS email\n"
        "   FROM app.orders o;"
    ),
    "jsonb_exists": (
        " SELECT a.id,\n"
        "    (a.doc ? 'vip'::text) AS is_vip\n"
        "   FROM app.accounts a\n"
        "  WHERE (a.doc ? 'active'::text);"
    ),
    "jsonb_path": (
        " SELECT (a.doc #>> '{a,b}'::text[]) AS ab,\n"
        "    (a.doc #> '{a}'::text[]) AS a\n"
        "   FROM app.accounts a;"
    ),
    "jsonb_equals": (
        " SELECT a.id\n"
        "   FROM app.accounts a\n"
        "  WHERE ((a.doc -> 'flag'::text) = 'true'::jsonb);"
    ),
    "jsonb_set_returning": (
        " SELECT o.id,\n"
        "    jsonb_array_elements((o.payload -> 'items'::text)) AS item\n"
        "   FROM app.orders o;"
    ),
    # Every EXTRACT stored before PostgreSQL 14 reads back as date_part.
    "date_part": (
        " SELECT u.id,\n"
        "    date_part('year'::text, u.created_at) AS y,\n"
        "    (date_part('epoch'::text, u.created_at))::bigint AS epoch\n"
        "   FROM app.users u;"
    ),
    "current_time": (
        " SELECT CURRENT_TIME AS now_t,\n"
        "    CURRENT_TIMESTAMP(0) AS now_ts;"
    ),
    "with_ordinality": (
        " SELECT u.tag,\n"
        "    u.n\n"
        "   FROM app.posts p,\n"
        "    LATERAL unnest(string_to_array(p.tags, ','::text)) WITH ORDINALITY u(tag, n);"
    ),
    "row_is_not_null": (
        " SELECT p.id\n"
        "   FROM app.pairs p\n"
        "  WHERE (ROW(p.a, p.b) IS NOT NULL);"
    ),
    "whole_row_is_not_null": (
        " SELECT c.id\n"
        "   FROM (app.customers c\n"
        "     LEFT JOIN app.orders o ON ((o.customer_id = c.id)))\n"
        "  WHERE (o.* IS NOT NULL);"
    ),
    # PostgreSQL 13 and earlier deparse POSITION / SUBSTRING / OVERLAPS as plain function
    # calls, and the text survives pg_upgrade and dump/restore.
    "pg13_position": (
        " SELECT u.id,\n"
        "    \"substring\"((u.email)::text, (\"position\"((u.email)::text, '@'::text) + 1))"
        " AS domain\n"
        "   FROM app.users u;"
    ),
    "pg13_overlaps": (
        " SELECT b.id\n"
        "   FROM app.bookings b\n"
        "  WHERE \"overlaps\"(b.starts_at, b.ends_at, "
        "'2024-01-01 00:00:00+00'::timestamp with time zone, "
        "'2024-02-01 00:00:00+00'::timestamp with time zone);"
    ),
    "date_trunc_time_zone": (
        " SELECT date_trunc('day'::text, o.created_at, 'Asia/Seoul'::text) AS local_day,\n"
        "    count(*) AS orders\n"
        "   FROM app.orders o\n"
        "  GROUP BY (date_trunc('day'::text, o.created_at, 'Asia/Seoul'::text));"
    ),
    # sqlglot cannot parse this one at all (`@?`); it is still PostgreSQL's own text, so it
    # must not be demoted to manual work.
    "jsonpath_exists": (
        " SELECT a.id,\n"
        "    (a.doc @? '$.a'::jsonpath) AS has_a\n"
        "   FROM app.accounts a;"
    ),
    "newline_literal": (
        " SELECT ((p.first_name || '\n'::text) || p.last_name) AS label\n"
        "   FROM app.people p;"
    ),
    "cte_window_filter": (
        " WITH totals AS (\n"
        "         SELECT o.customer_id,\n"
        "            sum(o.total) AS total\n"
        "           FROM app.orders o\n"
        "          GROUP BY o.customer_id\n"
        "        )\n"
        " SELECT t.customer_id,\n"
        "    t.total,\n"
        "    rank() OVER (ORDER BY t.total DESC) AS rnk,\n"
        "    count(*) FILTER (WHERE (t.total > (100)::numeric)) OVER () AS big\n"
        "   FROM totals t;"
    ),
}


@pytest.mark.parametrize("key", sorted(_VIEWDEF_CORPUS))
def test_a_pg_view_body_is_emitted_byte_for_byte(key: str) -> None:
    definition = _VIEWDEF_CORPUS[key]
    conversion = SchemaConverter(source_type=_PG).convert_view(
        ViewDef(name=f"app.v_{key}", definition=definition)
    )
    body = definition.strip().removesuffix(";")
    assert conversion.target_ddl == f'CREATE VIEW "app"."v_{key}" AS\n{body}'
    assert conversion.auto_converted is True
    assert conversion.warnings == []
    assert conversion.schema_ddls == ['CREATE SCHEMA IF NOT EXISTS "app"']


def test_the_spans_the_re_render_changed_now_survive() -> None:
    """Guard against a vacuous corpus: the old emitter changed each of these exact spans."""
    ddls = {
        key: SchemaConverter(source_type=_PG)
        .convert_view(ViewDef(name=f"app.v_{key}", definition=definition))
        .target_ddl
        for key, definition in _VIEWDEF_CORPUS.items()
    }
    assert "(o.payload -> 'items'::text) AS items" in ddls["jsonb_arrow"]
    assert "(a.doc ? 'vip'::text) AS is_vip" in ddls["jsonb_exists"]
    assert "(a.doc #>> '{a,b}'::text[]) AS ab" in ddls["jsonb_path"]
    assert "date_part('epoch'::text, u.created_at)" in ddls["date_part"]
    assert "CURRENT_TIME AS now_t" in ddls["current_time"]
    assert "CURRENT_TIMESTAMP(0) AS now_ts" in ddls["current_time"]
    assert "WITH ORDINALITY u(tag, n)" in ddls["with_ordinality"]
    assert "(ROW(p.a, p.b) IS NOT NULL)" in ddls["row_is_not_null"]
    assert '"position"((u.email)::text, \'@\'::text)' in ddls["pg13_position"]
    assert '"overlaps"(b.starts_at, b.ends_at' in ddls["pg13_overlaps"]
    assert "'Asia/Seoul'::text) AS local_day" in ddls["date_trunc_time_zone"]
    for ddl in ddls.values():
        assert "CAST(" not in ddl and "EXTRACT(" not in ddl


def test_a_pg_view_name_is_quoted_like_a_table() -> None:
    converter = SchemaConverter(source_type=_PG)
    mixed = converter.convert_view(ViewDef(name="MyApp.V_Orders", definition=" SELECT 1 AS one;"))
    assert mixed.target_ddl == 'CREATE VIEW "MyApp"."V_Orders" AS\nSELECT 1 AS one'
    assert mixed.schema_ddls == ['CREATE SCHEMA IF NOT EXISTS "MyApp"']
    bare = converter.convert_view(ViewDef(name='we"ird', definition=" SELECT 1 AS one;"))
    assert bare.target_ddl == 'CREATE VIEW "we""ird" AS\nSELECT 1 AS one'
    assert bare.schema_ddls == []


def test_a_pg_definition_with_its_own_header_keeps_everything_after_the_name() -> None:
    """pg_get_viewdef never returns a header; a hand-built inventory may. The column list
    and the CHECK OPTION are part of the view, so they stay; OR REPLACE / TEMP do not
    (the apply decides replacement, and Aurora DSQL has no temporary views)."""
    converter = SchemaConverter(source_type=_PG)
    listed = converter.convert_view(
        ViewDef(
            name="public.active",
            definition=(
                "CREATE OR REPLACE VIEW public.active (id) AS SELECT u.id FROM users u "
                "WHERE (u.name ~~* 'a%'::text) WITH CASCADED CHECK OPTION;"
            ),
        )
    )
    assert listed.target_ddl == (
        'CREATE VIEW "public"."active" (id) AS SELECT u.id FROM users u '
        "WHERE (u.name ~~* 'a%'::text) WITH CASCADED CHECK OPTION"
    )
    assert listed.auto_converted is True
    recursive = converter.convert_view(
        ViewDef(
            name="public.nums",
            definition=(
                "CREATE TEMP RECURSIVE VIEW nums (n) AS VALUES (1) "
                "UNION ALL SELECT n + 1 FROM nums WHERE n < 100"
            ),
        )
    )
    assert recursive.target_ddl == (
        'CREATE RECURSIVE VIEW "public"."nums" (n) AS VALUES (1) '
        "UNION ALL SELECT n + 1 FROM nums WHERE n < 100"
    )


def test_a_pg_header_that_is_not_a_plain_view_is_left_for_manual_work() -> None:
    conversion = SchemaConverter(source_type=_PG).convert_view(
        ViewDef(name="public.m", definition="CREATE MATERIALIZED VIEW public.m AS SELECT 1")
    )
    assert conversion.auto_converted is False
    assert conversion.target_ddl.startswith("-- Could not auto-convert view public.m")
    assert [w.classification for w in conversion.warnings] == [Classification.MANUAL]


def test_the_source_pane_shows_the_true_pg_text_and_agrees_with_the_target() -> None:
    """The source pane re-rendered the view through the same sqlglot round trip, so the
    diff showed identical mangled SQL on both sides and hid the change."""
    view = ViewDef(name="app.v_order_items", definition=_VIEWDEF_CORPUS["jsonb_arrow"])
    conversion = SchemaConverter(source_type=_PG).convert_view(view)
    source = render_source_view_ddl(view, source_type=_PG)
    assert source == (
        'CREATE VIEW "app"."v_order_items" AS\n'
        "SELECT o.id,\n"
        "    (o.payload -> 'items'::text) AS items\n"
        "   FROM app.orders o"
    )
    preview = build_view_preview(view, conversion, source_type=_PG)
    assert preview.source_ddl == preview.target_ddl == source
    headed = ViewDef(name="public.v", definition="CREATE VIEW public.v AS SELECT (1)::text AS a")
    assert render_source_view_ddl(headed, source_type=_PG) == headed.definition


def test_convert_carries_pg_views_verbatim_and_mysql_views_still_transpile() -> None:
    inventory = SourceInventory(
        views=[ViewDef(name="app.v_jsonb", definition=_VIEWDEF_CORPUS["jsonb_arrow"])]
    )
    (pg_view,) = SchemaConverter(source_type=_PG).convert(inventory).views
    assert "(o.payload -> 'items'::text) AS items" in pg_view.target_ddl
    mysql_view = SchemaConverter().convert_view(
        ViewDef(
            name="shop.active_customers",
            definition=(
                "select `c`.`id` AS `id`,`c`.`name` AS `name` from `shop`.`customers` `c` "
                "where (`c`.`status` = 'active')"
            ),
        )
    )
    # Unchanged MySQL output (pinned byte for byte).
    assert mysql_view.target_ddl == (
        'CREATE VIEW "shop"."active_customers" AS\n'
        "SELECT\n"
        '  "c"."id" AS "id",\n'
        '  "c"."name" AS "name"\n'
        'FROM "shop"."customers" AS "c"\n'
        "WHERE\n"
        "  (\n"
        "    \"c\".\"status\" = 'active'\n"
        "  )"
    )


# --------------------------------------------------------------------------------------
# Column DEFAULTs and STORED generated expressions
# --------------------------------------------------------------------------------------


def _pg_table(column: ColumnDef, *extra: ColumnDef) -> TableDef:
    return TableDef(
        name="app.t",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False), column, *extra],
        primary_key=["id"],
    )


@pytest.mark.parametrize(
    ("pg_type", "rendered_type", "default"),
    [
        # Re-rendered as CURRENT_TIME(), a syntax error: the whole CREATE TABLE failed.
        ("time without time zone", "TIME", "CURRENT_TIME"),
        ("time with time zone", "TIMETZ", "CURRENT_TIME"),
        # Re-rendered as CAST((EXTRACT(CAST('epoch' AS TEXT) FROM ...)) AS BIGINT), a
        # syntax error -- the stored form of any pre-PG14 EXTRACT default.
        ("bigint", "BIGINT", "(date_part('epoch'::text, now()))::bigint"),
        # Re-rendered without its time zone: on Aurora DSQL (UTC) another day boundary.
        (
            "timestamp with time zone",
            "TIMESTAMPTZ",
            "date_trunc('day'::text, now(), 'Asia/Seoul'::text)",
        ),
        # Re-rendered without its precision.
        ("timestamp with time zone", "TIMESTAMPTZ", "CURRENT_TIMESTAMP(0)"),
        ("text", "TEXT", "'line1\nline2'::text"),
        # A backslash must not be read as an escape by the splice.
        ("bytea", "BYTEA", "'\\x00ff'::bytea"),
    ],
)
def test_a_pg_default_is_carried_byte_for_byte(
    pg_type: str, rendered_type: str, default: str
) -> None:
    table = _pg_table(ColumnDef(name="c", mysql_type=pg_type, nullable=False, default=default))
    conversion = SchemaConverter(source_type=_PG).convert_table(table)
    assert conversion.target_ddl == (
        'CREATE TABLE "app"."t" (\n'
        '  "id" BIGINT NOT NULL,\n'
        f'  "c" {rendered_type} NOT NULL DEFAULT {default},\n'
        '  PRIMARY KEY ("id")\n'
        ")"
    )
    assert not [w for w in conversion.warnings if w.column_name == "c"]
    # The tool's own applied-DDL readers (what Full Load follows) still read the table.
    assert parse_target_primary_key(conversion.target_ddl) == ["id"]
    assert set(parse_target_column_types(conversion.target_ddl)) == {"id", "c"}
    not_null, read_default = parse_target_column_constraints(conversion.target_ddl)["c"]
    assert not_null is True and read_default is not None


def test_a_newline_in_a_generated_expression_is_not_re_indented() -> None:
    """Pretty-printing re-indented every line of the raw clause, so '<LF>' became
    '<LF>  ' -- a different value on every row."""
    expression = "((first_name || '\n'::text) || last_name)"
    table = _pg_table(
        ColumnDef(name="first_name", mysql_type="text"),
        ColumnDef(name="last_name", mysql_type="text"),
        ColumnDef(
            name="label",
            mysql_type="text",
            generated=True,
            generated_kind="STORED",
            generated_expression=expression,
        ),
    )
    ddl = SchemaConverter(source_type=_PG).convert_table(table).target_ddl
    assert f'  "label" TEXT GENERATED ALWAYS AS ({expression}) STORED,\n' in ddl
    assert "'\n  '::text" not in ddl
    assert parse_target_generated_columns(ddl) == {"label"}


def test_every_raw_fragment_survives_beside_the_rest_of_the_conversion() -> None:
    """The identity clause, the enum CHECK and the PRIMARY KEY are still sqlglot's; the
    verbatim defaults and the generated expression are spliced in among them."""
    table = TableDef(
        name="app.shifts",
        columns=[
            ColumnDef(
                name="id",
                mysql_type="bigint",
                nullable=False,
                default="nextval('app.shifts_id_seq'::regclass)",
            ),
            ColumnDef(
                name="status",
                mysql_type="app.shift_status",
                nullable=False,
                default="'open'::app.shift_status",
                type_kind="enum",
                enum_labels=["open", "closed"],
            ),
            ColumnDef(
                name="clock_in",
                mysql_type="time without time zone",
                nullable=False,
                default="CURRENT_TIME",
            ),
            ColumnDef(name="first_name", mysql_type="text"),
            ColumnDef(name="last_name", mysql_type="text"),
            ColumnDef(
                name="label",
                mysql_type="text",
                generated=True,
                generated_kind="STORED",
                generated_expression="((first_name || '\n'::text) || last_name)",
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
    )
    ddl = SchemaConverter(source_type=_PG).convert_table(table).target_ddl
    assert ddl == (
        'CREATE TABLE "app"."shifts" (\n'
        '  "id" BIGINT NOT NULL GENERATED BY DEFAULT AS IDENTITY (CACHE 65536),\n'
        "  \"status\" TEXT NOT NULL DEFAULT 'open' CHECK (\"status\" IN ('open', 'closed')),\n"
        '  "clock_in" TIME NOT NULL DEFAULT CURRENT_TIME,\n'
        '  "first_name" TEXT,\n'
        '  "last_name" TEXT,\n'
        "  \"label\" TEXT GENERATED ALWAYS AS (((first_name || '\n'::text) || last_name))"
        " STORED,\n"
        '  PRIMARY KEY ("id")\n'
        ")"
    )
    assert parse_target_identity_columns(ddl) == frozenset({"id"})
    assert parse_target_generated_columns(ddl) == {"label"}


def test_a_column_named_like_a_placeholder_does_not_receive_a_fragment() -> None:
    """The splice runs on the rendered text, so a name spelling a placeholder must not be
    replaced: the CREATE is then rendered on one line with the fragments in place."""
    table = _pg_table(
        ColumnDef(
            name="__dsql_raw_fragment_0__",
            mysql_type="time without time zone",
            default="CURRENT_TIME",
        ),
        ColumnDef(name="note", mysql_type="text", default="'a\nb'::text"),
    )
    ddl = SchemaConverter(source_type=_PG).convert_table(table).target_ddl
    assert ddl == (
        'CREATE TABLE "app"."t" ("id" BIGINT NOT NULL, '
        '"__dsql_raw_fragment_0__" TIME DEFAULT CURRENT_TIME, '
        "\"note\" TEXT DEFAULT 'a\nb'::text, PRIMARY KEY (\"id\"))"
    )


def test_the_default_gates_still_decide_what_is_carried() -> None:
    """Moving the defaults out of the parsed text must not carry what was dropped before."""
    table = _pg_table(
        ColumnDef(
            name="seq",
            mysql_type="bigint",
            nullable=False,
            default="nextval('app.t_seq_seq'::regclass)",
        ),
        ColumnDef(name="kind", mysql_type="text", default="'image'::app.media_type"),
        ColumnDef(name="odd", mysql_type="text", default="((("),
    )
    conversion = SchemaConverter(source_type=_PG).convert_table(table)
    assert "DEFAULT" not in conversion.target_ddl
    dropped = {w.column_name for w in conversion.warnings if "default" in w.message.lower()}
    assert dropped == {"seq", "kind", "odd"}


def test_every_view_header_the_converter_emits_is_one_the_applier_accepts() -> None:
    """A RECURSIVE header was graded auto-converted, then refused by the tool's own applier
    as not a CREATE TABLE/VIEW/INDEX statement."""
    from dsql_migrator.core.converter import pg_view_ddl
    from dsql_migrator.core.schema_applier import parse_create_object

    for definition in (
        " SELECT 1 AS n;",
        "CREATE VIEW nums AS SELECT 1",
        "CREATE OR REPLACE TEMP VIEW nums (n) AS SELECT 1",
        "CREATE RECURSIVE VIEW nums (n) AS VALUES (1) UNION ALL SELECT n + 1 FROM nums WHERE n < 9",
        "CREATE OR REPLACE TEMPORARY RECURSIVE VIEW nums (n) AS VALUES (1)",
        "CREATE VIEW nums WITH (security_barrier) AS SELECT 1",
    ):
        ddl = pg_view_ddl("public.Nums", definition)
        assert ddl is not None, definition
        assert parse_create_object(ddl) == ("public.Nums", "VIEW"), (definition, ddl)
