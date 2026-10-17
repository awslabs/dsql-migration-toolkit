"""G100: the review of G94 -- an edited view with a note above it reloads as Schema Apply
applied it, a block comment between two words keeps them two words, a non-recursive CTE
joins the namespace after its own body, the conversion chat adopts only DDL for its object,
and each view note opens its own AI chat."""

from __future__ import annotations

from dsql_migrator.core.models import ColumnDef, SourceInventory, TableDef, ViewDef


def _orders() -> TableDef:
    return TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"])


def test_a_commented_view_edit_reloads_as_schema_apply_ran_it() -> None:
    """applied_view_ddls returned the raw edit: the reload's pre-drop could not parse it,
    the DROP TABLE was refused (2BP01), and the view was reported missing."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.schema_conversion_apply import applied_view_ddls

    inventory = SourceInventory(
        tables=[_orders()],
        views=[ViewDef(name="shop.v_orders", definition="select `o`.`id` AS `id` from `shop`.`orders` `o`")],
    )
    result = SchemaConverter().convert(inventory)
    edited = '-- tuned for Aurora DSQL\nCREATE VIEW "shop"."v_orders" AS SELECT "o"."id" FROM "shop"."orders" AS "o";'
    ddl = applied_view_ddls(result, {"shop.v_orders": edited})["shop.v_orders"]
    assert ddl.startswith('CREATE VIEW "shop"."v_orders"')


def test_a_block_comment_between_two_words_is_whitespace() -> None:
    from dsql_migrator.ui.schema_conversion_apply import edited_statements

    (stmt,) = edited_statements(
        'CREATE TABLE "s"."t" ("id" BIGINT/*surrogate*/NOT NULL, "q" INTEGER DEFAULT 1/*one*/NOT NULL);'
    )
    assert "BIGINT NOT NULL" in stmt and "1 NOT NULL" in stmt


def test_a_non_recursive_cte_reads_the_real_table_in_its_own_body() -> None:
    from dsql_migrator.core.converter import _scan_sql_names

    own_body = "WITH orders AS (SELECT * FROM orders WHERE status = 'open') SELECT o.id FROM orders o"
    assert _scan_sql_names(own_body).relations == [(None, "orders")]
    later_sibling = "WITH a AS (SELECT * FROM b), b AS (SELECT 1 AS n) SELECT * FROM a"
    assert (None, "b") in _scan_sql_names(later_sibling).relations
    recursive = "WITH RECURSIVE t(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM t) SELECT * FROM t"
    assert _scan_sql_names(recursive).relations == []


def test_the_conversion_chat_adopts_only_ddl_for_its_object() -> None:
    """It used the Query Playground's SELECT-only extractor: a DDL reply had no button,
    and one with a verification SELECT stored that SELECT as the table's DDL."""
    from dsql_migrator.ui.schema_conversion import extract_ddl_from_reply

    ddl = 'CREATE TABLE "shop"."orders" ("id" BIGINT NOT NULL, PRIMARY KEY ("id"));\nCREATE INDEX ASYNC "ix" ON "shop"."orders" ("id");'
    reply = f"Here is the fix:\n```sql\n{ddl}\n```\nCheck it with:\n```sql\nSELECT count(*) FROM \"shop\".\"orders\";\n```"
    assert extract_ddl_from_reply(reply, "shop.orders") == ddl
    assert extract_ddl_from_reply(reply, "shop.items") is None
    assert extract_ddl_from_reply("```sql\nSELECT 1;\n```", "shop.orders") is None


def test_each_view_note_has_its_own_chat() -> None:
    from dsql_migrator.core.converter import ConversionWarning
    from dsql_migrator.core.models import Classification
    from dsql_migrator.ui.schema_conversion import _note_scope_key

    notes = [
        ConversionWarning(object_name="shop.v", classification=Classification.MANUAL, message=text)
        for text in ("The view calls FIND_IN_SET()...", "The view formats a date with %U...")
    ]
    assert len({_note_scope_key(n) for n in notes}) == 2
