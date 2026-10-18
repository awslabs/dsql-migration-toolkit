# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G125: keyset pages that agree with the order the source sorts the key in.

FL-1: MySQL sorts an ENUM by its member index and a SET by its bitmask, but compared an
ENUM / SET key column's cursor LABEL as a string, so Full Load silently skipped and re-read
rows at page boundaries (PK (status ENUM('new','active','closed'), id) read only its 'new'
rows). The reader now pages an ENUM by an IN list of the members declared after the
cursor's and a SET by its bitmask; Validation's MySQL checksum pages by the stored number.
(G130, test_g130_keyset_walk_review: a key LED by an ENUM is read member by member, the
members come from the source's catalog, a SET compares ``CAST(col AS UNSIGNED)``.)

FL-2: on PostgreSQL the composite-key disjunction is no index condition (each page re-read
the key from its start, O(n^2)); a PostgreSQL composite key now pages by the row-value
comparison its primary key serves, MySQL keeps the disjunction.

No database: :class:`_MySqlModel` serves pages by evaluating the reader's own WHERE the way
MySQL does. Both shapes were also run on MySQL 8.4 / 9.4 and PostgreSQL 16 / 17.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Optional

import pytest

from dsql_migrator.core.exporter import ExportError, keyset_stream
from dsql_migrator.core.models import ColumnDef, TableDef
from dsql_migrator.core.source_dialect import MySQLSourceDialect, PostgresSourceDialect

# ---------------------------------------------------------------------------
# A source that evaluates the reader's WHERE with MySQL's ENUM / SET rules
# ---------------------------------------------------------------------------

_TOKEN = re.compile(
    r"\s*(`(?:[^`]|``)+`|:\w+|\d+|\bIN\b|\bAND\b|\bOR\b|\bCAST\b|\bAS\b|\bUNSIGNED\b"
    r"|>=|<=|[()=<>+,])"
)


class _Result:
    def __init__(self, rows: list) -> None:
        self._rows = rows

    def mappings(self) -> list:
        return self._rows

    def fetchall(self) -> list:
        return self._rows


class _MySqlModel:
    """Serves keyset pages as MySQL evaluates them.

    ORDER BY sorts an ENUM by its member index and a SET by its bitmask; ``col > 'label'``
    compares the LABEL (a string), ``col = 'label'`` / ``IN`` the member the label names;
    ``CAST(col AS UNSIGNED)`` is the stored number, ``col + 0`` its DOUBLE (read as a signed
    64-bit value, as MySQL does), and an integer compared with an ENUM / SET compares the
    number. ``live`` is the source's definition of a column NOW (default: the table's),
    which the catalog read (information_schema.COLUMNS) returns and the rows sort by.
    ``ci``: a label bound against an ENUM resolves, as MySQL's index lookup does, to the
    first member equal to it ignoring case. Records every statement.
    """

    def __init__(
        self,
        table: TableDef,
        rows: list[dict],
        live: Optional[dict[str, str]] = None,
        ci: bool = False,
    ) -> None:
        self._types = {c.name: c.mysql_type for c in table.columns} | dict(live or {})
        self._members: dict[str, tuple[str, tuple[str, ...]]] = {}
        for column, column_type in self._types.items():
            kind = column_type.split("(", 1)[0].strip().lower()
            if kind in ("enum", "set"):
                inner = column_type[column_type.index("(") + 1 : -1]
                labels = tuple(m.replace("''", "'") for m in re.findall(r"'((?:[^']|'')*)'", inner))
                self._members[column] = (kind, labels)
        self._ci = ci
        self._pk = list(table.primary_key)
        self._rows = sorted(rows, key=lambda r: tuple(self._order(c, r[c]) for c in self._pk))
        self.executed: list[tuple[str, dict]] = []

    def _number(self, column: str, label: str) -> int:
        kind, labels = self._members[column]
        if kind == "enum":
            return labels.index(label) + 1 if label in labels else 0
        return sum(1 << labels.index(part) for part in label.split(",") if part)

    def _double(self, column: str, label: str) -> float:
        """``col + 0``: MySQL's DOUBLE of the stored number read as a SIGNED 64-bit value."""
        number = self._number(column, label)
        return float(number - (1 << 64) if number >= 1 << 63 else number)

    def _resolved(self, column: str, label: str) -> int:
        """The member index an ENUM label bound in a predicate names (0: no member)."""
        _kind, labels = self._members[column]
        if label in labels and not self._ci:
            return labels.index(label) + 1
        folded = [m.lower() for m in labels]
        return folded.index(label.lower()) + 1 if label.lower() in folded else 0

    def _order(self, column: str, value: object) -> object:
        return self._number(column, value) if column in self._members else value  # type: ignore[arg-type]

    @property
    def pages(self) -> list[tuple[str, dict]]:
        """The keyset pages read (every statement but the catalog read)."""
        return [(s, p) for s, p in self.executed if " LIMIT " in s]

    def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
        sql = str(statement)
        params = dict(parameters or {})
        self.executed.append((sql, params))
        if "information_schema.COLUMNS" in sql:
            return _Result([(name, column_type) for name, column_type in self._types.items()])
        if " WHERE " in sql:
            where = sql.split(" WHERE ", 1)[1].split(" ORDER BY ", 1)[0]
            rows = [row for row in self._rows if self._eval(where, row, params)]
        else:
            rows = list(self._rows)
        return _Result(rows[: params["batch_size"]])

    def _eval(self, where: str, row: dict, params: dict) -> bool:
        tokens = _TOKEN.findall(where)
        assert "".join(tokens).replace(" ", "") == where.replace(" ", ""), where
        pos = 0

        def peek() -> Optional[str]:
            return tokens[pos] if pos < len(tokens) else None

        def take(expected: Optional[str] = None) -> str:
            nonlocal pos
            token = tokens[pos]
            assert expected is None or token == expected, (token, expected, where)
            pos += 1
            return token

        def operand() -> tuple:
            token = take()
            if token == "CAST":
                take("(")
                column = take()[1:-1].replace("``", "`")
                take("AS")
                take("UNSIGNED")
                take(")")
                return ("number", self._number(column, row[column]))
            if token.startswith("`"):
                column = token[1:-1].replace("``", "`")
                if peek() == "+":
                    take("+")
                    take("0")
                    return ("number", self._double(column, row[column]))
                return ("column", column, row[column])
            if token.startswith(":"):
                return ("value", params[token[1:]])
            return ("value", int(token))

        def values(left: tuple, right: tuple) -> tuple:
            # An integer beside an ENUM / SET column compares its stored number.
            def one(side: tuple, other: tuple) -> object:
                if side[0] == "column" and side[1] in self._members and isinstance(other[-1], int):
                    return self._number(side[1], side[2])
                return side[-1]

            return one(left, right), one(right, left)

        def equal(left: tuple, right: tuple) -> bool:
            # ``enum_col = 'label'`` / ``IN ('label')`` is an index lookup of the member the
            # label resolves to; any other ``=`` compares values.
            if left[0] == "column" and self._members.get(left[1], ("",))[0] == "enum":
                if isinstance(right[-1], str):
                    return self._number(left[1], left[2]) == self._resolved(left[1], right[-1])
            a, b = values(left, right)
            return a == b

        def predicate() -> bool:
            left = operand()
            op = take()
            if op == "IN":
                take("(")
                items = [operand()]
                while peek() == ",":
                    take(",")
                    items.append(operand())
                take(")")
                return any(equal(left, item) for item in items)
            right = operand()
            if op == "=":
                return equal(left, right)
            a, b = values(left, right)
            return {">": a > b, ">=": a >= b, "<": a < b}[op]  # type: ignore[operator]

        def atom() -> bool:
            if peek() == "(":
                take("(")
                result = disjunction()
                take(")")
                return result
            return predicate()

        def conjunction() -> bool:
            result = atom()
            while peek() == "AND":
                take("AND")
                rhs = atom()
                result = result and rhs
            return result

        def disjunction() -> bool:
            result = conjunction()
            while peek() == "OR":
                take("OR")
                rhs = conjunction()
                result = result or rhs
            return result

        result = disjunction()
        assert pos == len(tokens), where
        return result


def _table(name: str, columns: list[tuple[str, str]], pk: list[str]) -> TableDef:
    return TableDef(
        name=f"shop.{name}",
        columns=[ColumnDef(name=n, mysql_type=t, nullable=False) for n, t in columns],
        primary_key=pk,
    )


def _read(
    table: TableDef,
    rows: list[dict],
    batch_size: int = 2,
    *,
    live: Optional[dict[str, str]] = None,
    ci: bool = False,
    **kwargs,
) -> tuple[list[tuple], _MySqlModel]:
    source = _MySqlModel(table, rows, live=live, ci=ci)
    stream = keyset_stream(source, table, batch_size=batch_size, **kwargs)
    return [tuple(r[c] for c in table.primary_key) for r in stream], source


def _assert_every_row_once(table: TableDef, rows: list[dict], keys: list[tuple]) -> None:
    expected = sorted(tuple(r[c] for c in table.primary_key) for r in rows)
    assert not [k for k, n in Counter(keys).items() if n > 1], "a row was read twice"
    assert sorted(keys) == expected, f"read {len(set(keys))} of {len(expected)} rows"


_TICKETS = _table(
    "tickets", [("status", "enum('new','active','closed')"), ("id", "int")], ["status", "id"]
)
_PREFS = _table(
    "prefs", [("user_id", "int"), ("channel", "enum('email','sms','push')")], ["user_id", "channel"]
)
_GRANTS = _table("grants", [("id", "int"), ("f", "set('read','write','admin')")], ["id", "f"])


# ---------------------------------------------------------------------------
# FL-1: MySQL ENUM / SET key columns
# ---------------------------------------------------------------------------


def test_the_model_reproduces_the_string_compared_cursor_losing_rows() -> None:
    """The model is MySQL's: the old ``status > 'new'`` drops every 'active' / 'closed' row."""
    source = _MySqlModel(_TICKETS, [{"status": s, "id": 1} for s in ("new", "active", "closed")])
    where = "((`status` > :last_0) OR (`status` = :last_0 AND `id` > :last_1))"
    assert [r for r in source._rows if source._eval(where, r, {"last_0": "new", "last_1": 1})] == []
    assert [r["status"] for r in source._rows] == ["new", "active", "closed"]  # index order


def test_a_leading_enum_key_reads_every_row_once_in_member_order() -> None:
    rows = [{"status": s, "id": i} for s in ("new", "active", "closed") for i in range(1, 6)]
    keys, source = _read(_TICKETS, rows)
    _assert_every_row_once(_TICKETS, rows, keys)
    assert [k[0] for k in keys] == ["new"] * 5 + ["active"] * 5 + ["closed"] * 5
    # The ORDER BY stays native; how the pages walk the members: test_g130_keyset_walk_review.
    assert all(
        "ORDER BY `shop`.`tickets`.`status`, `shop`.`tickets`.`id`" in sql
        for sql, _params in source.pages
    )


def test_a_trailing_enum_key_binds_the_members_after_the_cursor_not_its_label() -> None:
    rows = [{"user_id": u, "channel": c} for u in range(1, 5) for c in ("email", "sms", "push")]
    keys, source = _read(_PREFS, rows)
    _assert_every_row_once(_PREFS, rows, keys)
    _, params = next((s, p) for s, p in source.executed if p.get("last_1") == "sms")
    assert params["last_1_in_0"] == "push" and "last_1_in_1" not in params
    assert all(not isinstance(p.get("last_1"), int) for _s, p in source.executed)  # label kept


def test_a_set_key_pages_by_its_bitmask() -> None:
    combos = ["", "read", "write", "read,write", "admin", "read,admin", "write,admin", "read,write,admin"]
    rows = [{"id": i, "f": f} for i in range(1, 4) for f in combos]
    keys, source = _read(_GRANTS, rows, batch_size=3)
    _assert_every_row_once(_GRANTS, rows, keys)
    sql, params = next((s, p) for s, p in source.executed if p.get("last_1") == "write")
    # By the exact BIGINT UNSIGNED bitmask (G130: ``f + 0`` is a DOUBLE).
    assert "(`id` = :last_0 AND CAST(`f` AS UNSIGNED) > :last_1_bits)" in sql
    assert params["last_1_bits"] == 2
    _, params = next((s, p) for s, p in source.executed if p.get("last_1") == "read,admin")
    assert params["last_1_bits"] == 5


def test_alphabetical_enum_and_varchar_keys_are_unchanged_controls() -> None:
    key = ["user_id", "channel"]
    alpha = _table("alpha", [("user_id", "int"), ("channel", "enum('email','push','sms')")], key)
    text = _table("text", [("user_id", "int"), ("channel", "varchar(10)")], key)
    for table in (alpha, text):
        rows = [{"user_id": u, "channel": c} for u in range(1, 4) for c in ("email", "sms", "push")]
        keys, _source = _read(table, rows)
        _assert_every_row_once(table, rows, keys)
    _keys, source = _read(text, rows)
    assert source.executed == source.pages  # no ENUM / SET in the key: no catalog read
    assert "((`user_id` > :last_0) OR (`user_id` = :last_0 AND `channel` > :last_1))" in source.pages[1][0]


def test_a_one_column_enum_key_reads_every_row_and_stops_after_its_last_member() -> None:
    table = _table("lvl", [("lvl", "enum('low','mid','high','max')")], ["lvl"])
    rows = [{"lvl": m} for m in ("low", "mid", "high", "max")]
    keys, source = _read(table, rows)
    _assert_every_row_once(table, rows, keys)
    # After the first page, member by member (one row each), and none after the last.
    assert [p.get("last") for _s, p in source.pages[1:]] == ["high", "max"]
    assert all(" WHERE `lvl` = :last ORDER BY" in s for s, _p in source.pages[1:])


def test_reader_shards_of_an_int_enum_key_cover_every_row_once() -> None:
    rows = [{"user_id": u, "channel": c} for u in range(1, 9) for c in ("email", "sms", "push")]
    keys: list[tuple] = []
    for lower, upper in ((None, 4), (4, 7), (7, None)):
        got, source = _read(_PREFS, rows, pk_lower=lower, pk_upper=upper)
        keys += got
    _assert_every_row_once(_PREFS, rows, keys)
    assert "`channel` IN (" in source.pages[1][0] and "`user_id` >= :pk_lower" in source.pages[1][0]


def test_mysql_invalid_value_sentinel_sorts_first_and_every_member_follows_it() -> None:
    """A non-strict insert stores '' (index 0) for an invalid ENUM value; it sorts first."""
    rows = [{"status": s, "id": 1} for s in ("", "new", "active", "closed")]
    keys, source = _read(_TICKETS, rows, batch_size=1)
    _assert_every_row_once(_TICKETS, rows, keys)
    # The rest of the '' group first, then each member from its first row on.
    assert [p["last_0"] for _s, p in source.pages[1:]] == [
        "", "new", "new", "active", "active", "closed", "closed",
    ]


def test_a_key_value_outside_the_members_fails_the_table_loudly() -> None:
    """A value the read cannot place in the key order is refused, naming the column (never
    the value). The members are the source's own, read when the stream starts (G130)."""
    dialect = MySQLSourceDialect()
    members = {"channel": ("enum", ("", "email", "sms", "push"))}
    with pytest.raises(ExportError, match="key column 'channel' holds an ENUM value") as raised:
        dialect.keyset_after_sql(_PREFS, (7, "fax"), members)
    assert "fax" not in str(raised.value) and "information_schema.COLUMNS" in str(raised.value)
    with pytest.raises(ExportError, match="key column 'f' holds a SET value"):
        dialect.keyset_after_sql(
            _GRANTS, (1, "read,audit"), {"f": ("set", ("read", "write", "admin"))}
        )


def test_enum_and_set_members_are_read_as_schema_conversion_reads_them() -> None:
    from dsql_migrator.core.converter import mysql_enum_set_members

    assert mysql_enum_set_members("enum('it''s','a,b','c\\\\d','')") == (
        "enum", ("it's", "a,b", "c\\d", ""),
    )
    assert mysql_enum_set_members("SET('read','write')") == ("set", ("read", "write"))
    assert mysql_enum_set_members("varchar(10)") is None
    assert mysql_enum_set_members("enumerate") is None
    with pytest.raises(ValueError):
        mysql_enum_set_members("enum('a'")


# ---------------------------------------------------------------------------
# FL-1, the other MySQL pager: Validation's checksum pages
# ---------------------------------------------------------------------------


def test_validation_checksum_pages_an_enum_or_set_key_by_its_stored_number() -> None:
    from dsql_migrator.core.validation_sql import (
        build_mysql_page_checksum_first_sql,
        build_mysql_page_checksum_next_sql,
    )

    for column_type in ("enum('low','mid','high')", "set('a','b')"):
        table = _table("lvl", [("lvl", column_type), ("note", "varchar(5)")], ["lvl"])
        first = build_mysql_page_checksum_first_sql(table, "lvl")
        nxt = build_mysql_page_checksum_next_sql(table, "lvl")
        # MAX() over the label would be the alphabetical last ('mid'), not the page's last.
        assert "SELECT CAST(`lvl` AS UNSIGNED) AS page_pk," in first
        assert "ORDER BY `lvl` LIMIT :page" in first
        assert "WHERE CAST(`lvl` AS UNSIGNED) > :last ORDER BY `lvl` LIMIT :page" in nxt
    plain = _table("lvl", [("lvl", "varchar(5)")], ["lvl"])
    assert "WHERE `lvl` > :last ORDER BY `lvl`" in build_mysql_page_checksum_next_sql(plain, "lvl")


# ---------------------------------------------------------------------------
# FL-2: a PostgreSQL composite key pages by the row-value comparison
# ---------------------------------------------------------------------------


class _PgSource:
    """Serves PostgreSQL keyset pages from the bound cursor (a row-value comparison)."""

    def __init__(self, rows: list[dict], pk: list[str]) -> None:
        self._pk = pk
        self._rows = sorted(rows, key=lambda r: tuple(r[c] for c in pk))
        self.executed: list[tuple[str, dict]] = []

    def execute(self, statement: object, parameters: Optional[dict] = None) -> _Result:
        sql, params = str(statement), dict(parameters or {})
        self.executed.append((sql, params))
        rows = self._rows
        if "last_0" in params:
            cursor = tuple(params[f"last_{i}"] for i in range(len(self._pk)))
            rows = [r for r in rows if tuple(r[c] for c in self._pk) > cursor]
        if "pk_lower" in params:
            rows = [r for r in rows if r[self._pk[0]] >= params["pk_lower"]]
        if "pk_upper" in params:
            rows = [r for r in rows if r[self._pk[0]] < params["pk_upper"]]
        return _Result(rows[: params["batch_size"]])


_PG_EVENTS = TableDef(
    name="public.events",
    columns=[
        ColumnDef(name="tenant_id", mysql_type="integer", nullable=False),
        ColumnDef(name="created_at", mysql_type="date", nullable=False),
        ColumnDef(name="payload", mysql_type="text"),
    ],
    primary_key=["tenant_id", "created_at"],
)


@pytest.mark.parametrize("bounds", [{}, {"pk_lower": 2, "pk_upper": 4}])
def test_a_postgres_composite_key_pages_by_the_row_value_comparison(bounds: dict) -> None:
    rows = [
        {"tenant_id": t, "created_at": f"2025-01-0{d}", "payload": "x"}
        for t in range(1, 6)
        for d in range(1, 4)
    ]
    source = _PgSource(rows, ["tenant_id", "created_at"])
    stream = keyset_stream(source, _PG_EVENTS, batch_size=2, dialect=PostgresSourceDialect(), **bounds)
    got = [(r["tenant_id"], r["created_at"]) for r in stream]
    lower, upper = bounds.get("pk_lower", 0), bounds.get("pk_upper", 99)
    assert got == sorted((r["tenant_id"], r["created_at"]) for r in rows if lower <= r["tenant_id"] < upper)
    next_pages = [s for s, p in source.executed if "last_0" in p]
    assert next_pages
    for sql in next_pages:
        # The btree's index condition, over the native (bare) columns -- never the
        # disjunction PostgreSQL only filters by -- with the shard band still ANDed on.
        assert 'WHERE ("tenant_id", "created_at") > (:last_0, :last_1)' in sql
        assert " OR " not in sql
        assert ('"tenant_id" >= :pk_lower AND "tenant_id" < :pk_upper' in sql) == bool(bounds)
        assert 'ORDER BY "public"."events"."tenant_id", "public"."events"."created_at"' in sql


def test_a_postgres_one_column_key_and_mysql_composite_keys_keep_their_cursor() -> None:
    pg = TableDef(name="public.t", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
    assert PostgresSourceDialect().keyset_after_sql(pg, (7,), {}) == ('"id" > :last', {"last": 7})
    composite = _table("events", [("tenant_id", "int"), ("id", "bigint")], ["tenant_id", "id"])
    sql, params = MySQLSourceDialect().keyset_after_sql(composite, (3, 9), {})
    # MySQL 5.7's range optimizer serves the disjunction, not the row-value form.
    assert sql == "((`tenant_id` > :last_0) OR (`tenant_id` = :last_0 AND `id` > :last_1))"
    assert params == {"last_0": 3, "last_1": 9}
