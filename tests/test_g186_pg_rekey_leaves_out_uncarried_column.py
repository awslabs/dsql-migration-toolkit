# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G186 (PGFIX-6): a re-keyed PostgreSQL table whose target key leaves out the source
primary-key column CDC cannot carry.

G162 made a key column CDC cannot carry a required FAIL of COLUMN_REPLICABLE, judged against
``change_key_columns`` -- which always put the SOURCE primary key first. A table re-keyed in
Schema Conversion to a key that leaves a source primary-key column out (source PRIMARY KEY
(store, code isbn13), target PRIMARY KEY (store, sku)) is not sink-keyable
(``cdc.sink_keyable_rekeys``): Debezium keys its records on the target key
(``message.key.columns``), so ``code`` is a value column. It was still a required FAIL, which
also held its Full Load, saying CDC applies the changes "by `store`, `sku` alone" with "no
unique key" matching them -- but (store, sku) IS the target primary key.

Now such a key is the change's key alone, ``code`` gets the value-column WARN (here refused:
NOT NULL with no DEFAULT on the target), and the Exclude tables & columns route, which refuses
a source primary-key column, is not offered for it. A re-key that keeps every source
primary-key column (Schema Conversion's composite key) is graded as before. The tests use
doubles only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.models import (
    ColumnDef,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteStatus,
    TableDef,
)
from dsql_migrator.core.prerequisites_postgres import (
    UNCARRYABLE_COLUMN_ARRIVES_NULL,
    PostgresCdcFacts,
    UncarryableCdcColumn,
    change_key_columns,
    check_columns_replicable,
    check_postgres_cdc_prerequisites,
)

_ROOT = Path(__file__).resolve().parents[1]
_EXCLUDE_ROUTE = "exclude the column in Exclude tables & columns"

_ITEMS = TableDef(
    name="public.items",
    columns=[
        ColumnDef(name="store", mysql_type="integer", nullable=False),
        ColumnDef(name="code", mysql_type="isbn13", nullable=False),
        ColumnDef(name="sku", mysql_type="bigint", nullable=False),
        ColumnDef(name="emb", mysql_type="vector(3)"),
    ],
    primary_key=["store", "code"],
)
_CODE = UncarryableCdcColumn(
    "code", "isbn13", UNCARRYABLE_COLUMN_ARRIVES_NULL, nullable=False, in_event=False
)
_EMB = UncarryableCdcColumn(
    "emb", "vector(3)", UNCARRYABLE_COLUMN_ARRIVES_NULL, nullable=True, in_event=False
)


def _facts(*found: UncarryableCdcColumn, identity: str = "f") -> PostgresCdcFacts:
    return PostgresCdcFacts(
        wal_level="logical", is_superuser=False, has_replication_role=True,
        max_replication_slots=10, used_replication_slots=1, max_wal_senders=10,
        used_wal_senders=1, is_in_recovery=False, max_slot_wal_keep_size_mb=-1,
        has_database_create=True, tables_not_owned=(),
        replica_identity={"public.items": identity}, unlogged={"public.items": False},
        cdc_uncarryable_columns={"public.items": found},
    )


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        ((), ["store", "code"]),
        (["store", "sku"], ["store", "sku"]),  # leaves code out: keyed by it alone
        (["sku", "store"], ["sku", "store"]),
        (["sku", "store", "code"], ["store", "code", "sku"]),  # composite key: source PK too
        (["code", "store"], ["store", "code"]),  # a reorder
    ],
)
def test_the_change_key(key, expected) -> None:  # noqa: ANN001
    assert change_key_columns(_ITEMS, key) == expected


def test_a_source_key_column_the_target_key_leaves_out_is_a_value_column() -> None:
    row = check_columns_replicable(_ITEMS, _facts(_CODE), key_columns=["store", "sku"])
    assert row is not None and row.check_id is PrerequisiteCheckId.COLUMN_REPLICABLE
    assert row.status is PrerequisiteStatus.WARN and row.required is False
    assert "CDC applies this table's changes by" not in row.detail
    assert "no unique key matches" not in row.detail
    assert "part of the primary key" not in row.detail
    assert ("`code` (isbn13) is dropped from the change event but NOT NULL with no DEFAULT "
            "on the target") in row.detail
    assert "drop the NOT NULL from the target DDL in Schema Conversion" in row.remediation
    # Exclude tables & columns refuses a source primary-key column.
    assert _EXCLUDE_ROUTE not in row.remediation
    assert "A key column cannot be excluded." not in row.remediation


def test_the_exclusion_route_is_kept_for_the_other_columns_and_says_which_it_is_not() -> None:
    row = check_columns_replicable(
        _ITEMS, _facts(_CODE, _EMB), key_columns=["store", "sku"]
    )
    assert row is not None and row.status is PrerequisiteStatus.WARN
    assert _EXCLUDE_ROUTE in row.remediation
    assert row.remediation.endswith(
        "`code` (isbn13) is part of the source primary key, which cannot be excluded."
    )


def test_a_re_key_that_keeps_the_source_key_still_holds_the_table() -> None:
    row = check_columns_replicable(
        _ITEMS, _facts(_CODE), key_columns=["sku", "store", "code"]
    )
    assert row is not None
    assert row.status is PrerequisiteStatus.FAIL and row.required is True
    assert "`code` (isbn13) is part of the primary key" in row.detail
    # And so does the table that is not re-keyed.
    plain = check_columns_replicable(_ITEMS, _facts(_CODE))
    assert plain is not None and plain.status is PrerequisiteStatus.FAIL and plain.required


def test_the_re_keyed_table_no_longer_holds_cdc() -> None:
    results = check_postgres_cdc_prerequisites(
        _facts(_CODE), [_ITEMS], message_key_columns={"public.items": ["store", "sku"]}
    )
    (row,) = [r for r in results if r.check_id is PrerequisiteCheckId.COLUMN_REPLICABLE]
    assert row.status is PrerequisiteStatus.WARN and row.required is False
    assert PrerequisiteReport.build(mode=MigrationMode.CDC, results=results).can_proceed
    held = check_postgres_cdc_prerequisites(_facts(_CODE), [_ITEMS])
    assert not PrerequisiteReport.build(mode=MigrationMode.CDC, results=held).can_proceed


_LIMIT_HEAD = {
    "en": "**PostgreSQL: CDC does not carry most extension types.**",
    "ko": "**PostgreSQL: CDC는 대부분의 확장 타입을 옮기지 못함.**",
    "ja": "**PostgreSQL: CDC はほとんどの拡張機能の型を運びません。**",
}
_RE_KEY = {
    "en": ("you change in Schema Conversion to one that leaves the column out is not held",
           "CDC keys its changes by that key",
           "except that a source primary-key column cannot be excluded"),
    "ko": ("Conversion에서 그 컬럼을 뺀 기본 키로 바꾼 테이블은 막지 않습니다",
           "CDC는 그 테이블의 변경을 그 키로 적용하고",
           "다만 소스 기본 키 컬럼은 제외할 수 없습니다"),
    "ja": ("変えたテーブルは止めません", "CDC はそのテーブルの変更をそのキーで適用し",
           "ただし、ソースの主キー列は除外できません"),
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_says_a_re_key_that_leaves_the_column_out_is_not_held(language: str) -> None:
    text = " ".join(
        (_ROOT / "docs" / "manual" / language / "06-limitations.md")
        .read_text(encoding="utf-8").split()
    )
    bullet = text.split(_LIMIT_HEAD[language], 1)[1].split(" - **", 1)[0]
    for claim in _RE_KEY[language]:
        assert claim in bullet, (language, claim)
