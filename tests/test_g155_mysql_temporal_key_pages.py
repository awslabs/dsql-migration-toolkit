# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G155: Validation pages a MySQL table keyed by one DATE / DATETIME / TIMESTAMP column
through a day the month does not have.

Each page of the source checksum selected ``MAX(page_pk)`` as the next cursor. MAX() of a
date holding a day the month does not have ('2020-02-31', stored under ALLOW_INVALID_DATES)
is NULL on MySQL 5.7, 8.0 and 8.4 (under a strict mode, a valid date before it), so the next
page, bound after None, started from the first row again: the pages ran until they read more
rows than COUNT(*) -- or, without a count, forever -- and the checksum fell back to one
whole-table statement each time. The page's last key is now the MAX of the text MySQL prints
for such a key, which sorts as the keys do, and a full page whose last key still comes back
NULL stops the pages at once.

Doubles only: no MySQL, no AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core import validator as V
from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.validation_sql import (
    build_mysql_page_checksum_first_sql,
    build_mysql_page_checksum_next_sql,
)


def _table(key_type: str) -> TableDef:
    return TableDef(
        name="app.t",
        columns=[ColumnDef(name="k", mysql_type=key_type, nullable=False),
                 ColumnDef(name="v", mysql_type="int")],
        primary_key=["k"],
    )


@pytest.mark.parametrize("key_type", ["date", "datetime", "datetime(3)", "timestamp(6)"])
def test_a_temporal_keys_page_ends_on_the_text_of_its_last_key(key_type: str) -> None:
    for sql in (build_mysql_page_checksum_first_sql(_table(key_type), "k"),
                build_mysql_page_checksum_next_sql(_table(key_type), "k")):
        assert sql.startswith("SELECT COALESCE(SUM(page_tok), 0), MAX(CAST(page_pk AS CHAR)), "
                              "COUNT(*) FROM (")
    # The cursor still compares the column itself, in its own order.
    assert "WHERE `k` > :last ORDER BY `k` LIMIT :page" in build_mysql_page_checksum_next_sql(
        _table(key_type), "k")


@pytest.mark.parametrize("key_type", ["int", "bigint unsigned", "varchar(20)", "time",
                                      "enum('b','a')"])
def test_any_other_key_keeps_its_max(key_type: str) -> None:
    sql = build_mysql_page_checksum_first_sql(_table(key_type), "k")
    assert sql.startswith("SELECT COALESCE(SUM(page_tok), 0), MAX(page_pk), COUNT(*) FROM (")


class _Result:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def __iter__(self):  # noqa: ANN204
        return iter(self._rows)

    def scalar(self) -> object:
        return self._rows[0][0] if self._rows else None


class _Source:
    """A MySQL source whose page statements return scripted ``(sub_sum, last_pk, count)``
    rows, and whose whole-table statement returns ``whole``."""

    def __init__(self, pages: list, whole: int) -> None:
        self.pages, self.whole, self.executed = list(pages), whole, []

    def execute(self, statement: object, parameters: object = None) -> _Result:
        sql = str(statement)
        self.executed.append((sql, dict(parameters or {})))
        if "ckpage" in sql:
            return _Result([self.pages.pop(0)] if self.pages else [])
        return _Result([(self.whole,)])


@pytest.mark.parametrize("expected_rows", [9, None])
def test_a_full_page_whose_last_key_is_null_stops_the_pages_at_once(expected_rows) -> None:  # noqa: ANN001
    """The first full page's boundary comes back NULL: no second page is read after None
    (which named the first row again, and without a count never ended); the one exact
    whole-table statement is read instead."""
    source = _Source([(10, "2020-01-01", 2), (20, None, 2), (30, "2020-03-01", 2)], whole=99)
    with pytest.raises(V.PagedReadError) as raised:
        V._source_checksum_keyset(source, _table("date"), "k", 2, expected_rows=expected_rows)
    assert raised.value.reason == "the last key of a full page could not be read"
    assert len(source.executed) == 2
    assert source.executed[1][1] == {"last": "2020-01-01", "page": 2}

    source = _Source([(10, "2020-01-01", 2), (20, None, 2), (30, "2020-03-01", 2)], whole=99)
    assert V._source_checksum(source, _table("date"), 2, expected_rows=expected_rows) == "99"
    pages = [sql for sql, _ in source.executed if "ckpage" in sql]
    assert len(pages) == 2 and source.executed[-1][0].startswith("SELECT COALESCE(SUM(")


def test_the_text_of_the_last_key_is_bound_back_as_it_came() -> None:
    """'2020-02-31' is the page's last key, as MySQL prints it: the next page is read after
    it, and the pages add up to the checksum without a fallback."""
    source = _Source([(10, "2020-02-28", 2), (20, "2020-02-31", 2), (5, "2020-03-01", 1)],
                     whole=-1)
    assert V._source_checksum(source, _table("date"), 2, expected_rows=5) == "35"
    binds = [params.get("last") for sql, params in source.executed if "ckpage" in sql]
    assert binds == [None, "2020-02-28", "2020-02-31"]
