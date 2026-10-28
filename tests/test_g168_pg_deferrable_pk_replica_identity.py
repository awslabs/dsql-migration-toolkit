# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G168 (K26): a DEFERRABLE PostgreSQL primary key is no REPLICA IDENTITY, said at Evaluation.

PostgreSQL does not use a DEFERRABLE key for logical replication: once a table whose primary
key is DEFERRABLE is in a publication, the source refuses every UPDATE and DELETE on it
("cannot update table ... because it does not have a replica identity and publishes
updates", verified on PostgreSQL 16 and 17), and Step 3's REPLICA_IDENTITY check fails such
a table as required -- in Full load + CDC before the Full Load. Evaluation's PG_DEFERRABLE_KEY
(and the conversion note it shares) spoke only of the immediate uniqueness check, so an
operator planning a CDC migration learned of the source change it needs at Step 3. Now the
note says so for a primary key (never for a UNIQUE-only one), and so do its advice and the
manual (2.1, 2.3, 6.1).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import _pg_deferrable_key_warning
from dsql_migrator.core.models import (
    ColumnDef,
    DeferrableKeyDef,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
)

_SENTENCE = "With CDC (Full load + CDC or CDC only), a DEFERRABLE primary key is also no REPLICA IDENTITY"


def _table(*, primary: bool) -> TableDef:
    key = (
        DeferrableKeyDef(name="dk_pkey", definition="PRIMARY KEY (id) DEFERRABLE INITIALLY "
                         "DEFERRED", primary=True, initially_deferred=True, index="dk_pkey")
        if primary
        else DeferrableKeyDef(name="dk_code_key", definition="UNIQUE (code) DEFERRABLE",
                              index="dk_code_key")
    )
    return TableDef(
        name="app.dk",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="code", mysql_type="integer")],
        primary_key=["id"],
        indexes=[IndexDef(name="dk_code_key", columns=["code"], unique=True)],
        deferrable_keys=[key],
    )


def test_a_deferrable_primary_key_note_says_cdc_fails_the_table() -> None:
    note = _pg_deferrable_key_warning(_table(primary=True))
    assert note is not None and _SENTENCE in note.message
    assert "the CDC prerequisite check fails the table (in Full load + CDC, before the Full Load)" in note.message
    assert "re-created NOT DEFERRABLE or the table is set REPLICA IDENTITY FULL" in note.message
    unique_only = _pg_deferrable_key_warning(_table(primary=False))
    assert unique_only is not None and "REPLICA IDENTITY" not in unique_only.message


def test_evaluation_says_the_same() -> None:
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[_table(primary=True)])
    )
    (item,) = report.items
    concern = {c.rule_id: c for c in item.concerns}["PG_DEFERRABLE_KEY"]
    assert _SENTENCE in concern.risk
    assert "re-create it NOT DEFERRABLE on the source, or set the table REPLICA IDENTITY FULL" in (
        concern.recommendation
    )


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_it_where_it_describes_the_key(language: str) -> None:
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
            "`REPLICA IDENTITY`" in after[:700] and "`NOT DEFERRABLE`" in after[:700]
            for after in text.split("`PG_DEFERRABLE_KEY`")[1:]
        ), (language, part)
