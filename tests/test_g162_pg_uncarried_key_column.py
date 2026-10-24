# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G162 (K27): a PostgreSQL primary-key column of a type CDC cannot carry.

Debezium 2.7.4's TableSchemaBuilder builds a change record's KEY from the key columns it has
a schema for, so a key column of a type it leaves out of the change event (an isn type,
pgvector's vector, tsvector, ...) is left out of the key as well. The sink takes the key
columns from the record key, and DsqlDialect builds a DELETE's WHERE from whichever arrive:
with PRIMARY KEY (store, code isbn13) every source DELETE became
``DELETE ... WHERE "store" = ?`` and removed every target row of that store, silently, while
inserts and updates dead-lettered (executed against the shipped Debezium and sink jars in the
review). Step 3 graded the column like any other (WARN, not required) and both steps offered
routes that cannot apply to a key: exclude the column (refused for a key column), drop the
NOT NULL (a key keeps it), accept the gap.

Now a key column CDC cannot carry, for whatever reason, is a required FAIL of
COLUMN_REPLICABLE, like TABLE_REPLICABLE; the text names the key and what happens to it, and
only the routes that apply to a key are offered. Evaluation's CDC_UNCARRYABLE_COLUMN says the
same. The probe rows below are the shapes the real probe returned on PostgreSQL 17.11 with
the isn extension while this was written (throwaway local container); the tests use doubles.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.models import (
    ColumnDef,
    EffortLevel,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteStatus,
    SourceInventory,
    SourceType,
    TableDef,
)
from dsql_migrator.core.prerequisites_postgres import (
    UNCARRYABLE_COLUMN_ARRIVES_NULL,
    UNCARRYABLE_COLUMN_ARRIVES_WRONG,
    UNCARRYABLE_ROW_DEAD_LETTERS,
    PostgresCdcFacts,
    UncarryableCdcColumn,
    check_columns_replicable,
    check_postgres_cdc_prerequisites,
)
from dsql_migrator.ui.data_migration._models import prerequisite_block_reason

_ROOT = Path(__file__).resolve().parents[1]

# The routes that cannot apply to a key column.
_VALUE_ROUTES = (
    "exclude the column in Exclude tables & columns",
    "drop the NOT NULL",
    "Accepting it is also a valid choice",
)


def _dropped(name: str, declared: str) -> UncarryableCdcColumn:
    """A column the probe reports left out of the event (an extension's type), NOT NULL."""
    return UncarryableCdcColumn(
        column=name, declared_type=declared, reason=UNCARRYABLE_COLUMN_ARRIVES_NULL,
        nullable=False, in_event=False,
    )


def _facts(rows: dict) -> PostgresCdcFacts:
    names = list(rows)
    return PostgresCdcFacts(
        wal_level="logical", is_superuser=False, has_replication_role=True,
        max_replication_slots=10, used_replication_slots=1, max_wal_senders=10,
        used_wal_senders=1, is_in_recovery=False, max_slot_wal_keep_size_mb=-1,
        has_database_create=True, tables_not_owned=(),
        replica_identity={n: "d" for n in names}, unlogged={n: False for n in names},
        cdc_uncarryable_columns=rows,
    )


_ITEMS = TableDef(
    name="public.items",
    columns=[
        ColumnDef(name="store", mysql_type="integer", nullable=False),
        ColumnDef(name="code", mysql_type="isbn13", nullable=False),
        ColumnDef(name="qty", mysql_type="integer"),
    ],
    primary_key=["store", "code"],
)
_BOOKS = TableDef(
    name="public.books",
    columns=[
        ColumnDef(name="code", mysql_type="ean13", nullable=False),
        ColumnDef(name="title", mysql_type="text"),
    ],
    primary_key=["code"],
)
_PLAIN = TableDef(
    name="public.plain",
    columns=[
        ColumnDef(name="id", mysql_type="integer", nullable=False),
        ColumnDef(name="code", mysql_type="ean13"),
    ],
    primary_key=["id"],
)


def test_a_composite_key_column_cdc_leaves_out_is_a_required_fail_naming_the_over_delete() -> None:
    row = check_columns_replicable(
        _ITEMS, _facts({"public.items": (_dropped("code", "isbn13"),)})
    )
    assert row is not None and row.check_id is PrerequisiteCheckId.COLUMN_REPLICABLE
    assert row.status is PrerequisiteStatus.FAIL and row.required is True
    assert "`code` (isbn13) is part of the primary key" in row.detail
    assert "CDC applies this table's changes by `store` alone" in row.detail
    assert "a DELETE removes EVERY target row with the deleted row's `store`" in row.detail
    assert "CDC cannot replicate this table as it is." in row.detail
    # The value-column consequence (23502) is not the key's.
    assert "23502" not in row.detail
    for route in _VALUE_ROUTES:
        assert route not in row.remediation, route
    assert "separate Full load only migration" in row.remediation
    assert "change the key column's type on the source" in row.remediation
    assert "A key column cannot be excluded." in row.remediation


def test_a_one_column_key_cdc_leaves_out_carries_no_key_at_all() -> None:
    row = check_columns_replicable(
        _BOOKS, _facts({"public.books": (_dropped("code", "ean13"),)})
    )
    assert row is not None
    assert row.status is PrerequisiteStatus.FAIL and row.required is True
    assert "`code` (ean13) is the whole primary key" in row.detail
    assert "the changes carry no key: every insert and update is dead-lettered" in row.detail
    assert "REPLICA IDENTITY is FULL" in row.detail
    for route in _VALUE_ROUTES:
        assert route not in row.remediation, route


def test_a_key_column_that_arrives_wrong_or_dead_letters_is_held_too() -> None:
    table = TableDef(
        name="public.flags",
        columns=[
            ColumnDef(name="b", mysql_type="bit varying(8)", nullable=False),
            ColumnDef(name="t", mysql_type="time with time zone[]", nullable=False),
        ],
        primary_key=["b", "t"],
    )
    found = (
        UncarryableCdcColumn("b", "bit varying(8)", UNCARRYABLE_COLUMN_ARRIVES_WRONG, False),
        UncarryableCdcColumn("t", "time with time zone[]", UNCARRYABLE_ROW_DEAD_LETTERS, False),
    )
    row = check_columns_replicable(table, _facts({"public.flags": found}))
    assert row is not None
    assert row.status is PrerequisiteStatus.FAIL and row.required is True
    assert "`b` (bit varying(8)) is part of the primary key and arrives with a different value" in row.detail
    assert "lands as a second row beside it, and a DELETE misses it" in row.detail
    assert "`t` (time with time zone[]) is part of the primary key, and CDC cannot render it" in row.detail
    for route in _VALUE_ROUTES:
        assert route not in row.remediation, route


def test_a_value_column_of_the_same_type_keeps_its_warning_and_routes() -> None:
    """Only the key changed: a non-key column still gets today's WARN and choices."""
    nullable = UncarryableCdcColumn(
        "code", "ean13", UNCARRYABLE_COLUMN_ARRIVES_NULL, nullable=True, in_event=False
    )
    row = check_columns_replicable(_PLAIN, _facts({"public.plain": (nullable,)}))
    assert row is not None
    assert row.status is PrerequisiteStatus.WARN and row.required is False
    assert "primary key" not in row.detail
    assert "exclude the column in Exclude tables & columns" in row.remediation
    assert "Accepting it is also a valid choice" in row.remediation


def test_a_key_column_leads_and_a_value_column_follows() -> None:
    table = _ITEMS.model_copy(
        update={"columns": [*_ITEMS.columns, ColumnDef(name="emb", mysql_type="vector(3)")]}
    )
    found = (
        _dropped("code", "isbn13"),
        UncarryableCdcColumn("emb", "vector(3)", UNCARRYABLE_COLUMN_ARRIVES_NULL, True, False),
    )
    row = check_columns_replicable(table, _facts({"public.items": found}))
    assert row is not None and row.status is PrerequisiteStatus.FAIL
    assert row.detail.index("`code` (isbn13)") < row.detail.index("Separately, `emb` (vector(3))")
    assert "the divergence exists only on rows that arrive via CDC" in row.detail


def test_the_fail_holds_cdc_and_names_the_table() -> None:
    facts = _facts({
        "public.items": (_dropped("code", "isbn13"),),
        "public.plain": (UncarryableCdcColumn(
            "code", "ean13", UNCARRYABLE_COLUMN_ARRIVES_NULL, nullable=True, in_event=False),),
    })
    results = check_postgres_cdc_prerequisites(facts, [_ITEMS, _PLAIN])
    report = PrerequisiteReport.build(mode=MigrationMode.CDC, results=results)
    assert report.can_proceed is False
    reason = prerequisite_block_reason(report, mode=MigrationMode.CDC)
    assert "Every column's type can be carried by CDC (public.items)" in reason
    assert "public.plain" not in reason
    # Without the keyed table the value column alone never blocks.
    results = check_postgres_cdc_prerequisites(facts, [_PLAIN])
    assert PrerequisiteReport.build(mode=MigrationMode.CDC, results=results).can_proceed


def test_a_re_keyed_tables_record_key_column_counts_as_a_key() -> None:
    """A column the target key adds (Schema Conversion's composite key) is in every change's
    key as well; only tables in the connector's re-key map have one."""
    table = TableDef(
        name="public.orders",
        columns=[
            ColumnDef(name="id", mysql_type="integer", nullable=False),
            ColumnDef(name="code", mysql_type="upc", nullable=False),
        ],
        primary_key=["id"],
    )
    facts = _facts({"public.orders": (_dropped("code", "upc"),)})
    plain = check_columns_replicable(table, facts)
    assert plain is not None and plain.status is PrerequisiteStatus.WARN
    results = check_postgres_cdc_prerequisites(
        facts, [table], message_key_columns={"public.orders": ["code", "id"]}
    )
    (row,) = [r for r in results if r.check_id is PrerequisiteCheckId.COLUMN_REPLICABLE]
    assert row.status is PrerequisiteStatus.FAIL and row.required is True
    assert "CDC applies this table's changes by `id` alone" in row.detail


def test_a_column_that_stops_the_connector_is_described_once() -> None:
    """A NOT NULL bit(n)[] key column stops the source connector, which is its own FAIL and
    sentence; it is not described as a key column a second time."""
    from dsql_migrator.core.prerequisites_postgres import uncarried_key_columns

    stopper = UncarryableCdcColumn(
        "bits", "bit(3)[]", UNCARRYABLE_COLUMN_ARRIVES_NULL, nullable=False, in_event=True
    )
    assert uncarried_key_columns([stopper], ["bits"]) == []
    table = TableDef(
        name="public.b",
        columns=[ColumnDef(name="bits", mysql_type="bit(3)[]", nullable=False)],
        primary_key=["bits"],
    )
    row = check_columns_replicable(table, _facts({"public.b": (stopper,)}))
    assert row is not None and row.status is PrerequisiteStatus.FAIL and row.required
    assert "STOPS the PostgreSQL source connector" in row.detail
    assert "part of the primary key" not in row.detail


def _concern(table: TableDef):
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    (item,) = [i for i in report.items if i.object_name == table.name]
    return {c.rule_id: c for c in item.concerns}["CDC_UNCARRYABLE_COLUMN"]


def test_evaluation_names_the_key_as_step_3_does() -> None:
    concern = _concern(_ITEMS)
    assert concern.risk.startswith(
        "If you replicate with CDC: `code` (isbn13) is part of the primary key"
    )
    assert "a DELETE removes EVERY target row with the deleted row's `store`" in concern.risk
    for route in _VALUE_ROUTES:
        assert route not in concern.recommendation, route
    assert "separate Full load only migration" in concern.recommendation
    assert concern.effort is EffortLevel.MEDIUM
    one_column = _concern(_BOOKS)
    assert "is the whole primary key" in one_column.risk
    value = _concern(_PLAIN)
    assert "primary key" not in value.risk and value.effort is EffortLevel.SIMPLE
    assert "exclude the column in Exclude tables & columns" in value.recommendation


_KEY_SENTENCE = {
    "en": ("A primary-key column of such a type", "every target row that shares them",
           "**Full load + CDC**", "Full load only"),
    "ko": ("기본 키 컬럼은 더 나쁩니다", "타깃 행을 모두 지웁니다", "**Full load + CDC**",
           "Full load only"),
    "ja": ("主キー列はさらに深刻です", "ターゲットの行をすべて削除します", "**Full load + CDC**",
           "Full load only"),
}
_LIMIT_HEAD = {
    "en": "**PostgreSQL: CDC does not carry most extension types.**",
    "ko": "**PostgreSQL: CDC는 대부분의 확장 타입을 옮기지 못함.**",
    "ja": "**PostgreSQL: CDC はほとんどの拡張機能の型を運びません。**",
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_a_key_column_holds_the_table(language: str) -> None:
    text = " ".join(
        (_ROOT / "docs" / "manual" / language / "06-limitations.md")
        .read_text(encoding="utf-8").split()
    )
    bullet = text.split(_LIMIT_HEAD[language], 1)[1].split(" - **", 1)[0]
    for claim in _KEY_SENTENCE[language]:
        assert claim in bullet, (language, claim)
