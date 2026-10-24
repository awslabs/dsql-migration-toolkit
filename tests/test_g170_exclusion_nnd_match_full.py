# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G170 (CUX-4, CUX-5): excluding a column of a NULLS NOT DISTINCT index or a MATCH FULL key.

An excluded column is NULL on Aurora DSQL. G151 keeps a PostgreSQL ``UNIQUE ... NULLS NOT
DISTINCT`` and G152 a ``MATCH FULL`` foreign key, which turned two warnings false:

* W4 said "Uniqueness is not enforced for migrated rows (all NULL)", but NULLS NOT DISTINCT
  treats the NULLs as equal: excluding ``email`` of a (tenant, email) index makes every row
  after the first of a tenant collide (23505), or the index fail to build after a Drop &
  reload;
* W3 said "NULL passes the foreign key", but MATCH FULL refuses a partly NULL key: excluding
  ``code`` of a (vendor, code) key makes its validation fail on the migrated rows (Cut over
  holds Finish) and a later write leaving only ``code`` NULL refused (23503).

Now each has its own warning (and the omit-only note its NULLS NOT DISTINCT case), read from
the APPLIED statements -- an edit that removes the clause is honoured -- else the source.
Each consequence was reproduced on PostgreSQL 16/17 (throwaway local containers): building
such an index over two (7, NULL) rows fails, a second (7, NULL) insert is 23505, VALIDATE of
a MATCH FULL key over (1, NULL) fails and an insert of (1, NULL) is refused, while an all-NULL
key validates.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dsql_migrator.core.column_exclusion import (
    W3,
    W4,
    build_exclusion_context,
    column_exclusion_warnings,
    legacy_exclusion_notes,
)
from dsql_migrator.core.converter import SchemaConverter
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    IndexDef,
    SourceInventory,
    SourceType,
    TableDef,
)


def _users() -> TableDef:
    return TableDef(
        name="public.users",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False),
                 ColumnDef(name="tenant", mysql_type="integer", nullable=False),
                 ColumnDef(name="email", mysql_type="text"),
                 ColumnDef(name="code", mysql_type="text"),
                 ColumnDef(name="tag", mysql_type="text")],
        primary_key=["id"],
        indexes=[IndexDef(name="users_tenant_email_key", columns=["tenant", "email"], unique=True,
                          nulls_not_distinct=True),
                 IndexDef(name="users_tag_key", columns=["tag"], unique=True,
                          nulls_not_distinct=True),
                 IndexDef(name="users_code_key", columns=["code"], unique=True)],
    )


def _sku() -> TableDef:
    return TableDef(
        name="public.sku",
        columns=[ColumnDef(name="vendor", mysql_type="integer", nullable=False),
                 ColumnDef(name="code", mysql_type="integer", nullable=False)],
        primary_key=["vendor", "code"],
    )


def _line() -> TableDef:
    return TableDef(
        name="public.line",
        columns=[ColumnDef(name="id", mysql_type="integer", nullable=False)]
        + [ColumnDef(name=n, mysql_type="integer") for n in ("vendor", "code", "v2", "c2", "one")],
        primary_key=["id"],
        foreign_keys=[
            ForeignKeyDef(name="line_full", columns=["vendor", "code"], referenced_table="public.sku",
                          referenced_columns=["vendor", "code"], match_full=True),
            ForeignKeyDef(name="line_simple", columns=["v2", "c2"], referenced_table="public.sku",
                          referenced_columns=["vendor", "code"]),
            # A one-column key cannot be partly NULL: MATCH FULL changes nothing there.
            ForeignKeyDef(name="line_one", columns=["one"], referenced_table="public.users",
                          referenced_columns=["id"], match_full=True),
        ],
    )


def _inventory() -> SourceInventory:
    return SourceInventory(tables=[_users(), _sku(), _line()])


def _applied() -> dict:
    result = SchemaConverter(source_type=SourceType.POSTGRES).convert(_inventory())
    return {t.table: t for t in result.tables}


def _warnings(ctx, table: str, column: str, code: str) -> list[str]:  # noqa: ANN001
    return [f.message for f in column_exclusion_warnings(table, column, ctx) if f.code == code]


@pytest.mark.parametrize("applied", [True, False], ids=["applied", "source"])
def test_a_nulls_not_distinct_member_says_the_nulls_collide(applied: bool) -> None:
    ctx = build_exclusion_context(_inventory(), _applied() if applied else None, warnings=True)
    (shared,) = _warnings(ctx, "public.users", "email", W4)
    assert shared.startswith(
        "Its unique index users_tenant_email_key is NULLS NOT DISTINCT, which treats NULLs as "
        "equal: migrated rows that share tenant are rejected after the first (23505)"
    )
    assert "remove NULLS NOT DISTINCT from the index in the table's target script" in shared
    (alone,) = _warnings(ctx, "public.users", "tag", W4)
    assert "every migrated row after the first is rejected (23505)" in alone
    # A plain unique index keeps the old warning.
    assert _warnings(ctx, "public.users", "code", W4) == [
        "Uniqueness is not enforced for migrated rows (all NULL)."
    ]


def test_an_edited_index_without_the_clause_gets_the_plain_warning() -> None:
    applied = _applied()
    users = applied["public.users"]
    edited = users.model_copy(update={"index_ddls": [
        ddl.replace(" NULLS NOT DISTINCT", "") for ddl in users.index_ddls
    ]})
    ctx = build_exclusion_context(_inventory(), {**applied, "public.users": edited}, warnings=True)
    assert _warnings(ctx, "public.users", "email", W4) == [
        "Uniqueness is not enforced for migrated rows (all NULL)."
    ]


def test_the_omit_only_note_says_the_nulls_collide() -> None:
    ctx = build_exclusion_context(_inventory(), _applied(), warnings=True)
    (note,) = legacy_exclusion_notes("public.users", "email", ctx)
    assert note.code == "WLR"
    assert note.message.endswith(
        "Aurora DSQL stores NULL in it. Its unique index users_tenant_email_key is NULLS NOT "
        "DISTINCT, so rows that share tenant are rejected after the first (23505)."
    )
    (plain,) = legacy_exclusion_notes("public.users", "code", ctx)
    assert plain.code == "WL" and plain.message.endswith("Aurora DSQL stores NULL in it.")


@pytest.mark.parametrize("applied", [True, False], ids=["applied", "source"])
def test_a_match_full_key_column_says_the_key_fails(applied: bool) -> None:
    ctx = build_exclusion_context(_inventory(), _applied() if applied else None, warnings=True)
    (full,) = _warnings(ctx, "public.line", "code", W3)
    assert full.startswith(
        "Its foreign key to public.sku is MATCH FULL (vendor, code), which refuses a key that "
        "is partly NULL: unless every column of the key is excluded, the key's validation "
        "fails on the migrated rows (Cut over holds Finish on it)"
    )
    assert "refused (23503)" in full and "remove MATCH FULL (or the key)" in full
    assert _warnings(ctx, "public.line", "c2", W3) == [
        "Its relationship to public.sku is not migrated (NULL passes the foreign key)."
    ]
    assert _warnings(ctx, "public.line", "one", W3) == [
        "Its relationship to public.users is not migrated (NULL passes the foreign key)."
    ]


def test_an_edited_key_without_match_full_gets_the_plain_warning() -> None:
    applied = _applied()
    line = applied["public.line"]
    edited = line.model_copy(update={"foreign_key_ddls": [
        ddl.replace(" MATCH FULL", "") for ddl in line.foreign_key_ddls
    ]})
    ctx = build_exclusion_context(_inventory(), {**applied, "public.line": edited}, warnings=True)
    assert _warnings(ctx, "public.line", "code", W3) == [
        "Its relationship to public.sku is not migrated (NULL passes the foreign key)."
    ]


_MANUAL = Path(__file__).resolve().parents[1] / "docs" / "manual"
_ROWS = {
    "en": ("If the key is `MATCH FULL` over more than one column, a partly NULL key is refused",
           "unless the index is `NULLS NOT DISTINCT` (PostgreSQL 15+), which treats NULLs as equal"),
    "ko": ("그 키가 둘 이상의 컬럼에 걸친 `MATCH FULL`이면 대신 일부만 NULL인 키가 거부됩니다",
           "NULL을 같은 값으로 보는 `NULLS NOT DISTINCT`(PostgreSQL 15+)이면"),
    "ja": ("そのキーが 2 列以上にわたる `MATCH FULL` の場合は、代わりに一部だけが NULL のキーが拒否されます",
           "NULL を等しいものとして扱う `NULLS NOT DISTINCT`(PostgreSQL 15+)の場合は"),
}


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_chapter_13_says_so(language: str) -> None:
    text = (_MANUAL / language / "13-excluding-tables-and-columns.md").read_text(encoding="utf-8")
    for claim in _ROWS[language]:
        assert claim in text, (language, claim)
    assert "(23503)" in text and "(23505)" in text
