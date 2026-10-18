# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G130: follow-ups to G125's ENUM / SET keyset pages (MySQL source).

1. A key LED by an ENUM is read member by member. G125's cursor, ``status IN (<later
   members>) OR (status = <m> AND <rest> > ...)``, is a covering index scan plus a filter on
   MySQL 8.4 / 9.4 when the PK holds every column of the table, so each page re-read every
   row before the cursor (300k rows: 4.2 rows touched per row read, one page 100,000 for
   5,000). Each page is now ``status = :member`` or ``status = :member AND <the rest of the
   key after the cursor>`` -- a ref / range -- the next declared member once a page comes
   back short, and only the last member ends the table.
2. A page that returns its own cursor's row stops the table (members equal under the
   column's collation made G125's reader loop forever); so does a walked page that returns
   another member's rows.
3. A SET key compares ``CAST(col AS UNSIGNED)`` -- ``col + 0`` is a DOUBLE: a SET of 54+
   members lost bits and skipped rows (Full Load and Validation).
4. The members come from the source's catalog (information_schema.COLUMNS.COLUMN_TYPE), read
   once when the table's stream starts, not from the Evaluation record: a member added or
   reordered since left rows unread, silently. A catalog that cannot be read exactly stops
   the table before any page.

No database: the MySQL model of test_g125_keyset_review evaluates the reader's own WHERE.
Live: MySQL 8.4.11 and 9.4.0.
"""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import Optional

import pytest

from dsql_migrator.core.exporter import ExportError, keyset_stream
from dsql_migrator.core.source_dialect import MySQLSourceDialect
from tests.test_g125_keyset_review import (
    _assert_every_row_once,
    _MySqlModel,
    _read,
    _Result,
    _table,
)

_STATUS = "enum('new','active','closed')"
_COVERING = _table("tickets", [("status", _STATUS), ("id", "int")], ["status", "id"])


def _where(sql: str) -> str:
    return sql.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]


def _walk_pages(source: _MySqlModel) -> list[tuple[str, object]]:
    """Every next page as (its WHERE, the member it binds)."""
    return [(_where(s), p.get("last_0")) for s, p in source.pages[1:]]


# ---------------------------------------------------------------------------
# 1. A key led by an ENUM is read member by member
# ---------------------------------------------------------------------------


def test_a_covering_key_led_by_an_enum_reads_each_member_by_equality() -> None:
    rows = (
        [{"status": "new", "id": i} for i in range(1, 8)]
        + [{"status": "active", "id": i} for i in range(1, 6)]
        + [{"status": "closed", "id": i} for i in range(1, 4)]
    )
    keys, source = _read(_COVERING, rows, batch_size=3)
    _assert_every_row_once(_COVERING, rows, keys)
    within = "`status` = :last_0 AND ((`id` > :last_1))"
    start = "`status` = :last_0"
    # The first page as before; then the rest of 'new' after the cursor, each later member
    # from its first row -- and the table ends after a short page on the LAST member only.
    assert _walk_pages(source) == [
        (within, "new"), (within, "new"),
        (start, "active"), (within, "active"),
        (start, "closed"), (within, "closed"),
    ]
    for sql, _params in source.pages:
        assert " IN (" not in sql and " OR " not in sql  # never "the members after it"
        assert "ORDER BY `shop`.`tickets`.`status`, `shop`.`tickets`.`id`" in sql


def test_a_short_page_ends_the_table_only_on_the_last_member() -> None:
    table = _table(
        "steps", [("status", "enum('new','pending','active','closed','gone')"), ("id", "int")],
        ["status", "id"],
    )
    # 'pending' and 'gone' (the last) hold no row; 'active' exactly one page.
    rows = (
        [{"status": "new", "id": i} for i in range(1, 6)]
        + [{"status": "active", "id": i} for i in range(1, 4)]
        + [{"status": "closed", "id": 9}]
    )
    keys, source = _read(table, rows, batch_size=3)
    _assert_every_row_once(table, rows, keys)
    starts = [member for where, member in _walk_pages(source) if where == "`status` = :last_0"]
    assert starts == ["pending", "active", "closed", "gone"]  # each later member, once


@pytest.mark.parametrize("batch_size", range(1, 8))
def test_a_two_enum_key_walks_the_leading_member_and_pages_the_rest_within_it(
    batch_size: int,
) -> None:
    table = _table(
        "work",
        [("s", _STATUS), ("p", "enum('high','low','mid')"), ("id", "int")],
        ["s", "p", "id"],
    )
    rows = [
        {"s": s, "p": p, "id": i}
        for s, n in (("new", 4), ("closed", 3))
        for p in ("high", "mid")
        for i in range(1, n + 1)
    ]
    keys, source = _read(table, rows, batch_size=batch_size)
    _assert_every_row_once(table, rows, keys)
    for where, _member in _walk_pages(source):
        assert where.startswith("`s` = :last_0")  # one member of the leading ENUM per page
        assert "`s` IN (" not in where


def test_a_key_of_two_enums_alone_moves_on_once_its_member_has_nothing_after_the_cursor() -> None:
    table = _table("pair", [("s", _STATUS), ("p", "enum('x','y')")], ["s", "p"])
    rows = [{"s": s, "p": p} for s in ("new", "active", "closed") for p in ("x", "y")]
    for batch_size in (1, 2, 3, 4):
        keys, _source = _read(table, rows, batch_size=batch_size)
        _assert_every_row_once(table, rows, keys)


@pytest.mark.parametrize("batch_size", [1, 2, 3, 5])
def test_every_enum_and_set_key_shape_reads_every_row_once(batch_size: int) -> None:
    shapes = [
        (_COVERING, [{"status": s, "id": i} for s in ("new", "active", "closed") for i in (1, 2, 3)]),
        (  # a one-column ENUM key: one row per member
            _table("lvl", [("lvl", "enum('z','y','x','w')"), ("note", "text")], ["lvl"]),
            [{"lvl": m, "note": "n"} for m in ("z", "y", "x", "w")],
        ),
        (  # the invalid-value '' (index 0, sorted first) ahead of every member
            _COVERING,
            [{"status": s, "id": i} for s in ("", "new", "closed") for i in (1, 2, 3)],
        ),
        (  # a declared '' member, not first
            _table("decl", [("k", "enum('b','','a')"), ("id", "int")], ["k", "id"]),
            [{"k": k, "id": i} for k in ("b", "", "a") for i in (1, 2)],
        ),
        (  # a trailing ENUM: the leading INT is the range, nothing walks
            _table("prefs", [("u", "int"), ("c", "enum('email','sms','push')")], ["u", "c"]),
            [{"u": u, "c": c} for u in (1, 2, 3) for c in ("email", "sms", "push")],
        ),
        (  # a leading SET: by its bitmask
            _table("lead_set", [("f", "set('r','w','a')"), ("id", "int")], ["f", "id"]),
            [{"f": f, "id": i} for f in ("", "r", "w", "r,w", "a", "r,a") for i in (1, 2)],
        ),
    ]
    for table, rows in shapes:
        keys, _source = _read(table, rows, batch_size=batch_size)
        _assert_every_row_once(table, rows, keys)


def test_the_walk_issues_the_same_pages_on_every_read() -> None:
    """Pages are deterministic units: a retry re-issues exactly the same statements."""
    rows = [{"status": s, "id": i} for s in ("new", "closed") for i in range(1, 6)]
    _keys, first = _read(_COVERING, rows, batch_size=2)
    _keys, again = _read(_COVERING, rows, batch_size=2)
    assert first.executed == again.executed


# ---------------------------------------------------------------------------
# 2. A page that re-reads its cursor, or another member's rows, stops the table
# ---------------------------------------------------------------------------

_CI = "enum('a','A','b')"  # 'a' and 'A' are equal under a case-insensitive collation


@pytest.mark.parametrize("batch_size", [1, 2, 3])
def test_members_equal_under_the_collation_stop_a_walked_key_instead_of_looping(
    batch_size: int,
) -> None:
    table = _table("ci", [("k", _CI), ("id", "int")], ["k", "id"])
    rows = [{"k": k, "id": i} for k in ("a", "A", "b") for i in (1, 2, 3)]
    source = _MySqlModel(table, rows, ci=True)
    stream = keyset_stream(source, table, batch_size=batch_size)
    with pytest.raises(ExportError, match="key column 'k' has members the source compares as equal"):
        list(itertools.islice(stream, 100))
    assert len(source.pages) < 10


@pytest.mark.parametrize("batch_size", [1, 2])
def test_a_page_that_returns_its_cursor_row_stops_the_table(batch_size: int) -> None:
    """A trailing ENUM's IN list resolves 'A' to 'a' under the collation: the next page
    starts with the cursor's own row (the G125 reader re-read it forever at batch 1, and
    lost the 'A' row at batch 2)."""
    table = _table("trail", [("u", "int"), ("k", _CI)], ["u", "k"])
    rows = [{"u": u, "k": k} for u in (1, 2, 3) for k in ("a", "A", "b")]
    source = _MySqlModel(table, rows, ci=True)
    stream = keyset_stream(source, table, batch_size=batch_size)
    with pytest.raises(ExportError, match="returned the row the page was read after") as raised:
        list(itertools.islice(stream, 100))
    assert "shop.trail" in str(raised.value) and "'a'" not in str(raised.value)
    assert len(source.pages) < 10


def test_a_full_page_ending_on_its_cursor_never_repeats() -> None:
    """Any engine: a cursor that does not move stops the read at the first repeat."""

    class _Stuck:
        executed = 0

        def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
            self.executed += 1
            return _Result([{"id": 1}, {"id": 2}])  # whatever the cursor, the same page

    table = _table("t", [("id", "int")], ["id"])
    source = _Stuck()
    with pytest.raises(ExportError, match="returned the row the page was read after"):
        list(itertools.islice(keyset_stream(source, table, batch_size=2), 50))
    assert source.executed == 2


# ---------------------------------------------------------------------------
# 3. A SET key is compared by its exact bitmask
# ---------------------------------------------------------------------------

_SET60 = "set(" + ",".join(f"'m{i}'" for i in range(60)) + ")"


@pytest.mark.parametrize("batch_size", [1, 2, 3, 4])
def test_a_set_key_of_60_members_pages_by_its_exact_bitmask(batch_size: int) -> None:
    table = _table("flags", [("id", "int"), ("f", _SET60)], ["id", "f"])
    combos = ["m0", "m53", "m0,m53", "m54", "m0,m54", "m59", "m0,m59", "m58,m59"]
    rows = [{"id": i, "f": f} for i in (1, 2) for f in combos]
    keys, source = _read(table, rows, batch_size=batch_size)
    _assert_every_row_once(table, rows, keys)
    terms = [(s, p) for s, p in source.pages if "_bits" in s]
    assert terms and all("CAST(`f` AS UNSIGNED) > :last_1_bits" in s for s, _p in terms)
    assert all(" + 0" not in s for s, _p in source.pages)
    bits = {p["last_1"]: p["last_1_bits"] for _s, p in terms}
    assert bits.get("m0,m59", (1 << 59) + 1) == (1 << 59) + 1  # exact past bit 53


def test_the_double_the_old_set_term_compared_skips_rows_past_bit_53() -> None:
    """Why CAST: ``f + 0`` (MySQL's DOUBLE) cannot tell bit 59 + bit 0 from bit 59."""
    table = _table("flags", [("id", "int"), ("f", _SET60)], ["id", "f"])
    source = _MySqlModel(table, [{"id": 1, "f": f} for f in ("m59", "m0,m59")])
    after = {"last_0": 1, "last_1_bits": 1 << 59}
    old = "((`id` > :last_0) OR (`id` = :last_0 AND `f` + 0 > :last_1_bits))"
    new = "((`id` > :last_0) OR (`id` = :last_0 AND CAST(`f` AS UNSIGNED) > :last_1_bits))"
    assert [r["f"] for r in source._rows if source._eval(old, r, after)] == []  # lost
    assert [r["f"] for r in source._rows if source._eval(new, r, after)] == ["m0,m59"]


def test_validation_pages_enum_and_set_keys_by_cast_as_unsigned() -> None:
    from dsql_migrator.core.validation_sql import (
        build_mysql_page_checksum_first_sql,
        build_mysql_page_checksum_next_sql,
    )

    for column_type in (_SET60, _STATUS):
        table = _table("one", [("k", column_type), ("note", "text")], ["k"])
        for sql in (
            build_mysql_page_checksum_first_sql(table, "k"),
            build_mysql_page_checksum_next_sql(table, "k"),
        ):
            assert "CAST(`k` AS UNSIGNED) AS page_pk" in sql and "`k` + 0" not in sql
        assert "WHERE CAST(`k` AS UNSIGNED) > :last" in build_mysql_page_checksum_next_sql(table, "k")


# ---------------------------------------------------------------------------
# 4. The members come from the source's catalog when the stream starts
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "live, new_member",
    [
        ("enum('closed','active','new')", None),  # reordered
        ("enum('new','pending','active','closed')", "pending"),  # inserted mid-list
        ("enum('new','active','closed','archived')", "archived"),  # appended
    ],
)
@pytest.mark.parametrize("batch_size", [1, 3, 7])
def test_members_changed_after_evaluation_are_read_from_the_live_catalog(
    live: str, new_member: Optional[str], batch_size: int
) -> None:
    members = ["new", "active", "closed"] + ([new_member] if new_member else [])
    rows = [{"status": s, "id": i} for s in members for i in range(1, 6)]
    # The TableDef is the Evaluation record (the old definition); the source holds ``live``.
    keys, source = _read(_COVERING, rows, batch_size=batch_size, live={"status": live})
    _assert_every_row_once(_COVERING, rows, keys)
    catalog = [(s, p) for s, p in source.executed if "information_schema.COLUMNS" in s]
    assert catalog == [(catalog[0][0], {"schema": "shop", "name": "tickets"})]
    assert source.executed[0] == catalog[0]  # once, before the first page


def test_a_trailing_enum_member_added_after_evaluation_is_read_too() -> None:
    table = _table("prefs", [("u", "int"), ("c", "enum('email','sms')")], ["u", "c"])
    rows = [{"u": u, "c": c} for u in (1, 2, 3) for c in ("email", "fax", "sms")]
    keys, _source = _read(table, rows, batch_size=2, live={"c": "enum('email','fax','sms')"})
    _assert_every_row_once(table, rows, keys)


def test_a_key_without_enum_or_set_reads_no_catalog() -> None:
    table = _table("plain", [("a", "int"), ("b", "varchar(10)")], ["a", "b"])
    rows = [{"a": a, "b": b} for a in (1, 2) for b in ("x", "y")]
    _keys, source = _read(table, rows, batch_size=1)
    assert all("information_schema" not in s for s, _p in source.executed)


class _Catalog:
    """A source whose catalog returns ``rows`` (or raises ``error``); no page may follow."""

    def __init__(self, rows: list[tuple[str, str]], error: Optional[Exception] = None) -> None:
        self.rows, self.error, self.executed = rows, error, []

    def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
        self.executed.append((str(statement), dict(parameters or {})))
        assert "information_schema.COLUMNS" in str(statement), "a page was read"
        if self.error is not None:
            raise self.error
        return _Result(self.rows)


@pytest.mark.parametrize(
    "catalog, message",
    [
        ([("id", "int")], "is not in information_schema.COLUMNS for the table"),
        ([("status", "enum('a','b\\0c')"), ("id", "int")], "could not be read exactly"),
        ([("status", "enum('a','a','b')"), ("id", "int")], "names the same member twice"),
    ],
)
def test_members_that_cannot_be_read_exactly_stop_the_table_before_any_page(
    catalog: list[tuple[str, str]], message: str
) -> None:
    source = _Catalog(catalog)
    with pytest.raises(ExportError, match=message) as raised:
        list(keyset_stream(source, _COVERING, batch_size=5))
    assert "key column 'status'" in str(raised.value)
    assert len(source.executed) == 1


def test_a_failed_catalog_read_fails_the_table_as_itself() -> None:
    """Not swallowed into a guess: a dropped connection stays the retryable error it is."""
    source = _Catalog([], error=ConnectionError("lost connection to the source"))
    with pytest.raises(ConnectionError):
        list(keyset_stream(source, _COVERING, batch_size=5))


def test_a_walked_value_the_catalog_does_not_list_stops_the_table() -> None:
    class _Bytes(_MySqlModel):
        """A binary-charset ENUM: the driver returns the label as bytes."""

        def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
            result = super().execute(statement, parameters)
            if " LIMIT " in str(statement):
                return _Result([{**r, "status": r["status"].encode()} for r in result.mappings()])
            return result

    rows = [{"status": "new", "id": i} for i in (1, 2, 3)]
    source = _Bytes(_COVERING, rows)
    with pytest.raises(ExportError, match="holds a value that is not one of the members"):
        list(keyset_stream(source, _COVERING, batch_size=2))


def test_the_catalog_names_match_as_mysql_does_and_an_unqualified_table_uses_the_database() -> None:
    dialect = MySQLSourceDialect()
    table = _table("t", [("Status", _STATUS), ("id", "int")], ["Status", "id"]).model_copy(
        update={"name": "tickets"}
    )
    source = _Catalog([("status", "enum('new','closed')"), ("ID", "int")])
    members = dialect.keyset_key_members(source, table)
    assert members == {"Status": ("enum", ("", "new", "closed"))}
    sql, params = source.executed[0]
    assert "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :name" in sql
    assert params == {"name": "tickets"}
    qualified = _Catalog([("status", _STATUS), ("id", "int")])
    dialect.keyset_key_members(qualified, _COVERING)
    assert "WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name" in qualified.executed[0][0]
    assert qualified.executed[0][1] == {"schema": "shop", "name": "tickets"}


def test_the_exactness_check_is_the_catalogs_own_quoting() -> None:
    from dsql_migrator.core.converter import mysql_enum_set_members
    from dsql_migrator.core.source_dialect.mysql import _column_type_sql

    # As information_schema.COLUMNS.COLUMN_TYPE writes them (MySQL 8.4 / 9.4).
    for column_type in (
        "enum('q''x','bs\\\\x','lf\\nx','cr\\rx','tab\tx','dq\"x','sp x','é,한')",
        "set('read','write')",
    ):
        kind, values = mysql_enum_set_members(column_type)
        assert _column_type_sql(kind, values) == column_type
    kind, values = mysql_enum_set_members("enum('nul\\0x')")
    assert _column_type_sql(kind, values) != "enum('nul\\0x')"  # sqlglot keeps "\0" as text


# ---------------------------------------------------------------------------
# 5. The manual says the same in every language
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("language", ["en", "ko", "ja"])
def test_the_manual_describes_the_member_walk_and_the_live_members(language: str) -> None:
    manual = Path(__file__).resolve().parents[1] / "docs" / "manual" / language / "03-full-load.md"
    section = manual.read_text(encoding="utf-8").split("## 3.2", 1)[1].split("## 3.3", 1)[0]
    assert "`col = <" in section  # a key led by an ENUM: one member per page
    assert "CAST(col AS UNSIGNED) > n" in section and "col + 0" not in section
    assert "information_schema.COLUMNS" in section  # the members, read live
    assert "NUL" in section  # and the loud stops
    assert "collation" in section or "照合順序" in section
