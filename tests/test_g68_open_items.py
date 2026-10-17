"""G68: remaining audit items -- the 2 MiB row limit beside the 1 MiB value limit."""

from __future__ import annotations

from dsql_migrator.core.assessor import OVERSIZED_ROW_SENTENCE, CompatibilityAssessor
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef


def _lob_concern(table: TableDef, engine: SourceType):
    (item,) = CompatibilityAssessor(source_type=engine).assess(SourceInventory(tables=[table])).items
    return [c for c in item.concerns if c.rule_id == "OVERSIZED_LOB"]


def test_several_large_columns_name_the_row_limit_too() -> None:
    """Two 1 MiB values fit their columns but not Aurora DSQL's 2 MiB row, and the
    per-column size probe cannot see it."""
    two = TableDef(
        name="app.docs",
        columns=[ColumnDef(name="id", mysql_type="int", nullable=False),
                 ColumnDef(name="body", mysql_type="longtext"),
                 ColumnDef(name="raw", mysql_type="mediumblob")],
        primary_key=["id"],
    )
    (concern,) = _lob_concern(two, SourceType.MYSQL)
    assert OVERSIZED_ROW_SENTENCE.strip() in concern.risk
    one = two.model_copy(update={"columns": two.columns[:2]})
    (concern,) = _lob_concern(one, SourceType.MYSQL)
    assert "2 MiB row limit" not in concern.risk
    pg = TableDef(
        name="app.docs",
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="doc", mysql_type="jsonb"),
                 ColumnDef(name="raw", mysql_type="bytea")],
        primary_key=["id"],
    )
    (concern,) = [c for c in _lob_concern(pg, SourceType.POSTGRES)]
    assert "2 MiB row limit" in concern.risk


def test_a_mysql_generated_columns_expression_is_read_and_shown() -> None:
    """GENERATION_EXPRESSION was in the very query the tool runs, yet dropped: the finding
    told the operator to read it with SHOW CREATE TABLE, and the source pane said
    'expression not captured'."""
    from dsql_migrator.core.converter import (
        MYSQL_GENERATED_COLUMN_ADVICE_READ,
        mysql_generated_column_advice,
        mysql_generated_column_detail,
    )
    from dsql_migrator.core.introspector import enrich_columns
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl
    from tests.test_introspector import _FakeConnection

    tables = [TableDef(
        name="orders",
        columns=[ColumnDef(name="id", mysql_type="INT"), ColumnDef(name="price", mysql_type="INT"),
                 ColumnDef(name="qty", mysql_type="INT"), ColumnDef(name="total", mysql_type="INT")],
        primary_key=["id"],
    )]
    rows = [
        ("orders", "id", None, "auto_increment", "int", None, ""),
        ("orders", "price", None, "", "int", None, ""),
        ("orders", "qty", None, "", "int", None, ""),
        ("orders", "total", None, "STORED GENERATED", "int", None, "(`price` * `qty`)"),
    ]
    enrich_columns(_FakeConnection(rows), "app", tables)
    total = tables[0].columns[3]
    assert total.generated and total.generated_kind == "STORED"
    assert total.generated_expression == "(`price` * `qty`)"
    assert "total AS (`price` * `qty`)" in mysql_generated_column_detail(tables[0])
    assert mysql_generated_column_advice(tables[0]) == MYSQL_GENERATED_COLUMN_ADVICE_READ
    assert "SHOW CREATE TABLE" not in MYSQL_GENERATED_COLUMN_ADVICE_READ
    assert "GENERATED ALWAYS AS ((`price` * `qty`)) STORED" in render_source_table_ddl(tables[0])
    # A six-column row (an older reader / test double) still works: nothing captured.
    older = [TableDef(name="orders", columns=[ColumnDef(name="total", mysql_type="INT")])]
    enrich_columns(_FakeConnection([("orders", "total", None, "STORED GENERATED", "int", None)]), "app", older)
    assert older[0].columns[0].generated and older[0].columns[0].generated_expression is None


def test_a_mysql_view_is_graded_by_its_conversion_at_both_steps() -> None:
    """An 8-name regex flagged GROUP_CONCAT (translated to string_agg) and missed
    FIND_IN_SET (left as written, so the CREATE VIEW fails)."""
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.core.models import ViewDef

    inventory = SourceInventory(views=[
        ViewDef(name="shop.v_hits", definition="select find_in_set('a', `tags`) AS `hit` from `shop`.`t`"),
        ViewDef(name="shop.v_names", definition="select group_concat(`name` separator ',') AS `n` from `shop`.`t`"),
    ])
    by_view = {v.view: v for v in SchemaConverter().convert(inventory).views}
    (note,) = by_view["shop.v_hits"].warnings
    assert "find_in_set()" in note.message and by_view["shop.v_hits"].auto_converted
    assert by_view["shop.v_names"].warnings == []
    items = {i.object_name: i for i in CompatibilityAssessor().assess(inventory).items}
    assert items["shop.v_hits"].classification.value == "MANUAL"
    assert items["shop.v_hits"].risk == note.message
    assert items["shop.v_names"].classification.value == "AUTO"
