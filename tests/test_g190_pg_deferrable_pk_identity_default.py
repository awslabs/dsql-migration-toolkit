# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G190 (PGFIX-7): G168's DEFERRABLE-primary-key CDC sentence was unconditional.

A table whose PRIMARY KEY is DEFERRABLE has its UPDATEs and DELETEs refused once published
only while its REPLICA IDENTITY is DEFAULT: one already set REPLICA IDENTITY FULL, or USING
INDEX on a unique index that is not deferrable, publishes them (live on 17.11 in the review:
dk_full and dk_idx updated, dk_def refused; Step 3 FAILs only the DEFAULT one). Evaluation's
PG_DEFERRABLE_KEY told every such table that PostgreSQL refuses every UPDATE and DELETE and
that Step 3 fails it. The inventory does not read the identity, so the note, its advice and
the manual (2.1, 2.3, 6.1) now say it is the table left at REPLICA IDENTITY DEFAULT.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import _pg_deferrable_key_warning
from dsql_migrator.core.models import (
    ColumnDef,
    DeferrableKeyDef,
    SourceInventory,
    SourceType,
    TableDef,
)

_TABLE = TableDef(
    name="app.dk",
    columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)],
    primary_key=["id"],
    deferrable_keys=[DeferrableKeyDef(name="dk_pkey", definition="PRIMARY KEY (id) DEFERRABLE",
                                      primary=True, index="dk_pkey")],
)


def test_the_note_and_its_advice_name_the_table_left_at_replica_identity_default() -> None:
    note = _pg_deferrable_key_warning(_TABLE)
    assert note is not None
    assert ("a table left at REPLICA IDENTITY DEFAULT has every UPDATE and DELETE refused by "
            "PostgreSQL once it is in CDC's publication") in note.message
    assert "once the table is in CDC's publication PostgreSQL refuses every" not in note.message
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[_TABLE])
    )
    (item,) = report.items
    concern = {c.rule_id: c for c in item.concerns}["PG_DEFERRABLE_KEY"]
    assert ("If the primary key is DEFERRABLE, you replicate with CDC and the table's REPLICA "
            "IDENTITY is DEFAULT, re-create it") in concern.recommendation


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_qualifies_it_where_it_describes_the_key(language: str) -> None:
    def chapter(name: str) -> str:
        return " ".join((_MANUAL / language / name).read_text(encoding="utf-8").split())

    conversion = chapter("02-evaluation-and-schema-conversion.md")
    # 2.4 is chapter 2's findings reference (the rule catalogue 2.1 points to), so
    # Evaluation's text is 2.1 plus 2.4.
    evaluation = conversion.split("## 2.1", 1)[1].split("## 2.2", 1)[0] + conversion.split("## 2.4", 1)[1]
    limits = chapter("06-limitations.md").split("## 6.1", 1)[1].split("## 6.2", 1)[0]
    # Section 2.3 is the type mapping since 2026-10-25; the key is described in 2.1 and 6.1.
    for part, text in (("2.1", evaluation), ("6.1", limits)):
        assert any(
            "`REPLICA IDENTITY DEFAULT`" in after[:500]
            for after in text.split("`PG_DEFERRABLE_KEY`")[1:]
        ), (language, part)
