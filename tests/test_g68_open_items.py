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
