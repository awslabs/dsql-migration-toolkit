# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G189 (PGFIX-5): a PostgreSQL 18 PERIOD foreign key declared NOT ENFORCED.

The source never checks such a key (pg_constraint conenforced f, conperiod t; live on 18.6 in
the review), yet it was graded twice with contradictory advice: PG_FK_PERIOD (MANUAL, MEDIUM,
a LOSS) said to enforce it in the application, and PG_FK_NOT_ENFORCED said no action was
needed and offered to re-add it as ALTER TABLE ... ADD CONSTRAINT ... NOT VALID -- a plain
equality key, which refuses rows the source accepts (Aurora DSQL has no PERIOD).

Now only the NOT ENFORCED note names it, saying it cannot be created to be enforced either,
and the re-add advice leaves PERIOD keys out. Doubles only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import (
    ColumnDef,
    ConversionNoteKind,
    ForeignKeyDef,
    SourceInventory,
    SourceType,
    TableDef,
)

_PG = SourceType.POSTGRES
_PK = ColumnDef(name="id", mysql_type="integer", nullable=False)


def _inventory(*, enforced: bool) -> SourceInventory:
    rooms = TableDef(
        name="rev.rooms",
        columns=[_PK, ColumnDef(name="valid", mysql_type="tstzrange", nullable=False)],
        primary_key=["id", "valid"],
    )
    key = ForeignKeyDef(name="ne_room", columns=["room_id", "during"],
                        referenced_table="rev.rooms", referenced_columns=["id", "valid"],
                        period=True, enforced=enforced)
    bookings = TableDef(
        name="rev.ne_only",
        columns=[_PK, ColumnDef(name="room_id", mysql_type="integer"),
                 ColumnDef(name="during", mysql_type="tstzrange")],
        primary_key=["id"],
        foreign_keys=[key],
    )
    return SourceInventory(tables=[rooms, bookings])


def _concerns(inventory: SourceInventory) -> dict:
    report = CompatibilityAssessor(source_type=_PG).assess(inventory)
    (item,) = [i for i in report.items if i.object_name == "rev.ne_only"]
    return {c.rule_id: c for c in item.concerns}


def test_a_not_enforced_period_key_is_graded_once_as_not_enforced() -> None:
    concerns = _concerns(_inventory(enforced=False))
    assert "PG_FK_PERIOD" not in concerns
    concern = concerns["PG_FK_NOT_ENFORCED"]
    assert ("ne_room also declares PERIOD, which Aurora DSQL has no form of, so it cannot be "
            "created there to be enforced either.") in concern.risk
    assert ("To have Aurora DSQL enforce such a key after all (not one declared with PERIOD, "
            "which Aurora DSQL has no form of), add its ALTER TABLE") in concern.recommendation
    conversion = {
        t.table: t for t in SchemaConverter(source_type=_PG).convert(_inventory(enforced=False)).tables
    }["rev.ne_only"]
    assert conversion.foreign_key_ddls == []
    assert not [w for w in conversion.warnings if "PERIOD foreign key(s)" in w.message]
    (note,) = [w for w in conversion.warnings if "NOT ENFORCED" in w.message]
    assert note.kind is ConversionNoteKind.RECOMMENDATION


def test_an_enforced_period_key_is_still_a_loss() -> None:
    concerns = _concerns(_inventory(enforced=True))
    assert concerns["PG_FK_PERIOD"].risk.startswith("PERIOD foreign key(s) are NOT created")
    assert "PG_FK_NOT_ENFORCED" not in concerns


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_ROUTE = {
    "en": "to have Aurora DSQL enforce such a key (not one declared with `PERIOD`, which Aurora "
          "DSQL has no form of), add its",
    "ko": "Aurora DSQL이 그런 키를 강제하게 하려면(Aurora DSQL에 그런 형태가 없는 `PERIOD`로 선언한 키는 "
          "제외) 그 키의",
    "ja": "させるには(Aurora DSQL にその形がない `PERIOD` で宣言されたキーは除きます)、そのキーの",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_leaves_period_keys_out_of_the_re_add_route(language: str) -> None:
    text = " ".join(
        (_MANUAL / language / "02-evaluation-and-schema-conversion.md").read_text(encoding="utf-8").split()
    )
    assert _ROUTE[language] in text, language
