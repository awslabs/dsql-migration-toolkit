# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G167 (PG-5): a PostgreSQL 18 column whose NOT NULL was added NOT VALID.

``ALTER TABLE t ADD CONSTRAINT t_v_nn NOT NULL v NOT VALID`` (PostgreSQL 18) leaves the rows
that already hold NULL in place, unchecked, while PostgreSQL sets ``pg_attribute.attnotnull``
at once -- so reflection reported the column NOT NULL, Schema Conversion emitted
``"v" INT NOT NULL``, and Full Load quarantined every pre-existing NULL row (23502),
permanently: Aurora DSQL cannot drop or add a NOT NULL later, and nothing said why.

Now Evaluation reads such constraints (``contype 'n'``, not ``convalidated``), the column is
created without NOT NULL, and both steps say so (PG_NOT_NULL_NOT_VALID, MANUAL/LOSS). Live on
PostgreSQL 18.6 (throwaway local container): the catalog row below, a validated NOT VALID
constraint and a plain NOT NULL left alone; on 13.23 and 17.11 the read returns nothing.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    ConversionNoteKind,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.core.source_dialect import postgres as pg_dialect_module

_PG = SourceType.POSTGRES


class _Result:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self) -> list[dict]:
        return self._rows


class _Catalog:
    """Answers the NOT VALID NOT NULL read with the rows PostgreSQL 18.6 returned."""

    def __init__(self, rows: list[dict]) -> None:
        self.rows, self.executed = rows, []

    def execute(self, statement, params=None):  # noqa: ANN001, ANN201
        self.executed.append((str(statement), dict(params or {})))
        names = set((params or {}).get("names") or ())
        return _Result([r for r in self.rows if r["rel"] in names])


def _nnv() -> TableDef:
    """rev.nnv as reflection leaves it (bare name, inside the schema read)."""
    return TableDef(
        name="nnv",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="v", mysql_type="integer", nullable=False),
            ColumnDef(name="w", mysql_type="integer", nullable=False),
        ],
        primary_key=["id"],
    )


def test_the_read_marks_the_column_nullable() -> None:
    sql = pg_dialect_module._PG_UNVALIDATED_NOT_NULL_SQL
    assert "con.contype = 'n' AND NOT con.convalidated" in sql
    assert "a.attnum = con.conkey[1]" in sql and "c.relname = ANY(:names)" in sql
    table = _nnv()
    catalog = _Catalog([{"rel": "nnv", "col": "v"}, {"rel": "nnv", "col": "id"},
                        {"rel": "other", "col": "w"}])
    pg_dialect_module._pg_read_unvalidated_not_null(catalog, "rev", [table])
    columns = {c.name: (c.nullable, c.not_null_not_valid) for c in table.columns}
    # A key column keeps its NOT NULL (the key's), another table's row changes nothing here.
    assert columns == {"id": (False, False), "v": (True, True), "w": (False, False)}
    assert catalog.executed[0][1] == {"nsp": "rev", "names": ["nnv"]}
    empty = _Catalog([])
    pg_dialect_module._pg_read_unvalidated_not_null(empty, "rev", [])
    assert empty.executed == []


def _read_table() -> TableDef:
    table = _nnv()
    pg_dialect_module._pg_read_unvalidated_not_null(_Catalog([{"rel": "nnv", "col": "v"}]),
                                                    "rev", [table])
    return table.model_copy(update={"name": "rev.nnv"})


def test_the_column_is_created_without_not_null_and_noted() -> None:
    inventory = SourceInventory(tables=[_read_table()])
    (converted,) = SchemaConverter(source_type=_PG).convert(inventory).tables
    ddl = " ".join(converted.target_ddl.split())
    assert '"v" INT,' in ddl and '"w" INT NOT NULL' in ddl
    (note,) = [w for w in converted.warnings if "NOT VALID column(s)" in w.message]
    assert note.classification is Classification.MANUAL and note.kind is ConversionNoteKind.LOSS
    assert note.message.startswith(
        "NOT NULL ... NOT VALID column(s) are created without NOT NULL — v."
    )
    report = CompatibilityAssessor(source_type=_PG).assess(inventory)
    (item,) = report.items
    concern = {c.rule_id: c for c in item.concerns}["PG_NOT_NULL_NOT_VALID"]
    assert concern.classification is Classification.MANUAL
    assert concern.risk == note.message
    assert "ALTER TABLE ... VALIDATE CONSTRAINT on the source" in concern.recommendation


def test_the_flag_stays_out_of_the_payload_while_false_and_the_panel_shows_it() -> None:
    from dsql_migrator.ui.schema_conversion import render_source_table_ddl

    plain = ColumnDef(name="w", mysql_type="integer").model_dump()
    assert "not_null_not_valid" not in plain
    table = _read_table()
    payload = json.loads(table.model_dump_json())
    assert payload["columns"][1]["not_null_not_valid"] is True
    assert TableDef.model_validate(payload) == table
    rendered = render_source_table_ddl(table, source_type=_PG)
    assert '"v" integer NOT NULL NOT VALID' in rendered and '"w" integer NOT NULL' in rendered


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_HEAD = {
    "en": "**PostgreSQL 17/18 sources — four version-specific edges",
    "ko": "**PostgreSQL 17/18 소스 — 버전별 엣지 4가지",
    "ja": "**PostgreSQL 17/18 ソース — バージョン固有のエッジ 4 点",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_names_the_edge(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "06-limitations.md").read_text(encoding="utf-8").split()
    )
    bullet = text.split(_HEAD[language], 1)[1].split("## 6.3", 1)[0]
    for spelled in ("`NOT NULL … NOT VALID`", "`PG_NOT_NULL_NOT_VALID`",
                    "`ALTER TABLE … VALIDATE CONSTRAINT`", "**MANUAL/LOSS**"):
        assert spelled in bullet, (language, spelled)
