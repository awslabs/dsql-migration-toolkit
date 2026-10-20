# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G143: a composite key on Aurora DSQL is paged by a form its primary key serves as a
range, so each page reads only its own rows.

Live plan probes on Aurora DSQL: the row value ``(k0, k1) > (:0, :1)`` the pagers wrote
after a composite cursor is never an index condition there. It is a filter over a scan of
the primary key from its first row -- with literals, untyped or native parameters and the
generic plan alike -- so every page also read every row before it: 80,038 rows for a
5,000-row page in the middle of 150,000 and all 150,000 for the last one (44 -> 338 ms per
page over one walk); a late page of a large enough table would pass the 300 s transaction
age. An equality prefix with one strict range on the next column IS a start key, so the
next page is now one range per key prefix, deepest first, each in key order and cut at the
page size, and the first page-size rows of them in key order::

    (... WHERE k0 = :0 AND k1 > :1 ORDER BY k0, k1 LIMIT n)
    UNION ALL (... WHERE k0 > :0 ORDER BY k0, k1 LIMIT n) ORDER BY k0, k1 LIMIT n

which read 5,014-5,198 rows (~30 ms) per page wherever it started, and none for the final
empty page, for 2- and 3-column keys of high and low cardinality. Validation's count and
checksum pages, its orphan pages and the cut-over orphan count page this way, and so does
the listing of a failed foreign key's orphans. A PostgreSQL SOURCE keeps the row value: one
index range on PostgreSQL, where the ranges read more and one can be planned as a bitmap
scan of its whole prefix.

The doubles read the SQL the builders write -- each range by its own bounds over typed rows,
a bound's text parsed in the column's type as the server parses an untyped parameter -- and
hold it to the row value: the same rows in the same order for every cursor, over every key
shape the G138 tests use and numeric / date / boolean / text parts. No database is reached.
"""

from __future__ import annotations

import functools
import random
import re
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Callable

import pytest
from psycopg import sql

import dsql_migrator.core.validator as v
import dsql_migrator.ui.data_migration._fk_validation as fkv
import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.models import ForeignKeyDef, ValidationMode
from dsql_migrator.core.validation_sql import (
    build_orphan_listing_sql,
    build_pg_composite_orphan_page_sql,
    build_pg_composite_page_sql,
)
from tests.test_g138_composite_validation import (
    _PG_SOURCE,
    _SOURCE,
    _TARGET,
    MySqlDouble,
    PgDouble,
    _grants,
    _order_items,
    _PgSqla,
    _prefs,
    _rekeyed,
    _Table,
    _tickets,
    _tok,
    _validator,
)
from tests.test_g141_validation_followups import _f32, _f32_text

# ---------------------------------------------------------------------------
# Typed key parts, as Aurora DSQL reads them
# ---------------------------------------------------------------------------


class _TypedPg(PgDouble):
    """Aurora DSQL with numeric, date, boolean and real key parts, read as the server reads
    them: a key comes back as its ``::text`` (``'1.50'``, ``'2024-02-29'``, ``'true'``, a
    float4's shortest decimal) and a bound sent untyped is parsed in the column's own type."""

    def _parse(self, table: _Table, column: str, text: object) -> object:
        kind = table.types.get(column, "int")
        if kind.startswith("numeric"):
            return Decimal(str(text))
        if kind == "date":
            return date.fromisoformat(str(text))
        if kind == "boolean":
            return {"false": False, "true": True}[str(text)]
        if kind == "real":
            return _f32(float(str(text)))
        return super()._parse(table, column, text)

    def _out(self, table: _Table, column: str, value: object, as_text: bool) -> object:
        if as_text and isinstance(value, bool):
            return "true" if value else "false"
        if as_text and table.types.get(column) == "real":
            return _f32_text(value)  # type: ignore[arg-type]
        return super()._out(table, column, value, as_text)  # '1.50' keeps its scale


def _amounts() -> _Table:
    """(numeric, date): a numeric lead of few values, over a leap day and a month end."""
    rows = [
        {"amount": Decimal(a), "day": date(2024, 2, 26) + timedelta(days=d), "memo": f"{a}/{d}"}
        for a in ("-3.75", "0.50", "1.00", "1.25", "10.00") for d in range(6)
    ]
    return _Table("app.amounts", ("amount", "day"), rows,
                  types={"amount": "numeric(10,2)", "day": "date", "memo": "text"})


def _switches() -> _Table:
    """(boolean, text): two leading values; names in C order (code points), with case,
    accents, a quote, spaces, the empty string and the checksum's escape characters."""
    names = ["", " x", "B", "Z", "a", "a b", "o'q", "z", "~|", "Á", "É", "é"]
    pairs = [(on, name) for on in (True, False) for name in names]
    rows = [{"on": on, "name": name, "v": i} for i, (on, name) in enumerate(pairs)]
    return _Table("app.switches", ("on", "name"), rows,
                  types={"on": "boolean", "name": "text", "v": "int"})


def _events() -> _Table:
    """(date, text, numeric): three parts over a year end, numerics of mixed scales."""
    rows = [
        {"day": date(2023, 12, 30) + timedelta(days=d), "kind": kind, "seq": Decimal(seq),
         "v": f"{d}{kind}{seq}"}
        for d in range(4) for kind in ("B", "a", "b") for seq in ("-0.5", "1.5", "2.25", "10")
    ]
    return _Table("app.events", ("day", "kind", "seq"), rows,
                  types={"day": "date", "kind": "varchar(8)", "seq": "numeric", "v": "text"})


def _codes() -> _Table:
    """(int, varchar): G138's text key -- case and accents -- in C order."""
    rng = random.Random(143)
    letters = ["a", "B", "Á", "m", "b", "é", "Z", "z", "É", "n"]
    keys = sorted({(rng.randrange(3), rng.choice(letters) + str(rng.randrange(9)))
                   for _ in range(90)})
    rows = [{"tenant": t, "code": c, "v": i} for i, (t, c) in enumerate(keys)]
    return _Table("app.codes", ("tenant", "code"), rows,
                  types={"tenant": "int", "code": "varchar(40)", "v": "int"})


def _prices() -> _Table:
    """(real, int): a MySQL FLOAT on Aurora DSQL leading, float4 values whose shortest
    decimal is often below the stored value (G141): the equality range still finds them."""
    rows = [{"price": _f32(0.01 + 0.03 * k), "n": n, "v": k * 10 + n}
            for k in range(12) for n in range(3)]
    return _Table("app.prices", ("price", "n"), rows,
                  types={"price": "real", "n": "int", "v": "int"})


def _cells() -> _Table:
    """(int, int, int): low-cardinality leading parts (G138's re-keyed ``(c, a, b)``)."""
    rows = [{"a": a, "b": b, "c": c, "v": a * 100 + b * 10 + c}
            for a in (0, 1, 2) for b in (0, 1) for c in range(7)]
    return _Table("app.cells", ("a", "b", "c"), rows)


_SHAPES: "dict[str, Callable[[], _Table]]" = {
    "bigint-int": lambda: _order_items(23),
    "enum-int": lambda: _tickets(7),
    "int-enum": _prefs,
    "int-set": _grants,
    "int-text": _codes,
    "rekeyed-int-int": lambda: _Table("app.leads", ("lead", "id"), _rekeyed()[1].rows),
    "numeric-date": _amounts,
    "boolean-text": _switches,
    "real-int": _prices,
    "date-text-numeric": _events,
    "int-int-int": _cells,
}


def _in_order(table: _Table) -> list[dict]:
    return sorted(table.rows, key=lambda r: tuple(r[c] for c in table.key))


def _cursor(double: PgDouble, table: _Table, row: dict, name: str = "last") -> dict:
    """``row``'s key as a page hands it back: each part as text, named as it is bound."""
    return {f"{name}_{i}": double._out(table, c, row[c], True) for i, c in enumerate(table.key)}


def _after(double: PgDouble, table: _Table, cursor: dict, page: int) -> list[dict]:
    """The reference: the ``page`` rows after the cursor in key order, as the row value
    ``(k0, k1, ...) > (:0, :1, ...)`` names them."""
    bound = tuple(double._parse(table, c, text) for c, text in zip(table.key, cursor.values()))
    return [r for r in _in_order(table) if tuple(r[c] for c in table.key) > bound][:page]


# ---------------------------------------------------------------------------
# The statements
# ---------------------------------------------------------------------------

_CELLS_SELECT = '"a" AS "ck_k0", "b" AS "ck_k1", "c" AS "ck_k2"'
_CELLS_ORDER = 'ORDER BY "app"."cells"."a", "app"."cells"."b", "app"."cells"."c" LIMIT 50'


def _window(text: str) -> str:
    return text.split(" FROM (", 1)[1].rsplit(") ckpage", 1)[0]


def test_the_next_page_reads_one_range_per_key_prefix_deepest_first() -> None:
    table = _cells().tabledef()
    text = build_pg_composite_page_sql(table, ("a", "b", "c"), 50, checksum=False,
                                       first=False).as_string(None)
    assert _window(text) == (
        f'(SELECT {_CELLS_SELECT} FROM "app"."cells" WHERE "a" = %(last_0)s AND '
        f'"b" = %(last_1)s AND "c" > %(last_2)s {_CELLS_ORDER}) UNION ALL '
        f'(SELECT {_CELLS_SELECT} FROM "app"."cells" WHERE "a" = %(last_0)s AND '
        f'"b" > %(last_1)s {_CELLS_ORDER}) UNION ALL '
        f'(SELECT {_CELLS_SELECT} FROM "app"."cells" WHERE "a" > %(last_0)s {_CELLS_ORDER}) '
        'ORDER BY "ck_k0", "ck_k1", "ck_k2" LIMIT 50'
    )
    # The aggregate over the window is G138's, and nothing is a row value or a disjunction.
    assert text.startswith('SELECT 0, COUNT(*), (array_agg("ck_k0"::text ORDER BY "ck_k0", '
                           '"ck_k1", "ck_k2"))[COUNT(*)], ')
    assert ") > (" not in text and " OR " not in text
    # Each bound stays untyped: a cast to text has no bigint > text and no char(n) start key.
    assert not re.search(r"%\(last_\d+\)s::", text)


def test_the_first_page_and_a_postgresql_sources_page_are_unchanged() -> None:
    table = _cells().tabledef()
    key = ("a", "b", "c")
    first = build_pg_composite_page_sql(table, key, 50, checksum=False, first=True)
    assert _window(first.as_string(None)) == (
        f'SELECT {_CELLS_SELECT} FROM "app"."cells" {_CELLS_ORDER}')
    source = build_pg_composite_page_sql(table, key, 50, checksum=False, first=False,
                                         row_value=True)
    assert _window(source.as_string(None)) == (
        f'SELECT {_CELLS_SELECT} FROM "app"."cells" WHERE ("a", "b", "c") > '
        f'(%(last_0)s, %(last_1)s, %(last_2)s) {_CELLS_ORDER}')
    # The shim's FROM ONLY still finds the source page's one FROM.
    only = v._from_only(source, v._pg_table_identifier("app.cells")).as_string(None)
    assert only.count('FROM ONLY "app"."cells"') == 1


def test_a_two_column_page_reads_two_ranges_with_the_checksum_in_each() -> None:
    table = _order_items().tabledef()
    text = build_pg_composite_page_sql(table, ("order_id", "line_no"), 7, checksum=True,
                                       nonnull_columns=("note",), first=False).as_string(None)
    window = _window(text)
    first_range, last_range = window.split(" UNION ALL ")
    for part in (first_range, last_range):
        assert ' AS ck_tok, ("note" IS NOT NULL) AS "ck_nn0" FROM "app"."order_items" ' in part
        assert ('ORDER BY "app"."order_items"."order_id", "app"."order_items"."line_no" '
                'LIMIT 7)') in part
    assert 'WHERE "order_id" = %(last_0)s AND "line_no" > %(last_1)s ORDER BY' in first_range
    assert 'WHERE "order_id" > %(last_0)s ORDER BY' in last_range
    assert window.endswith(') ORDER BY "ck_k0", "ck_k1" LIMIT 7')


def test_the_orphan_page_and_the_listing_read_the_same_ranges() -> None:
    fk = ForeignKeyDef(name="fk_cell", columns=["b"], referenced_table="app.p",
                       referenced_columns=["id"])
    orphan = build_pg_composite_orphan_page_sql("app.cells", fk, ("a", "b", "c"), 50,
                                                first=False).as_string(None)
    cte = orphan.split("WITH pg AS MATERIALIZED (", 1)[1].split(") SELECT (SELECT COUNT(*)", 1)[0]
    select = _CELLS_SELECT + ', "b"'  # the foreign-key column under its own name too
    assert cte == (
        f'(SELECT {select} FROM "app"."cells" WHERE "a" = %(last_0)s AND "b" = %(last_1)s '
        f'AND "c" > %(last_2)s {_CELLS_ORDER}) UNION ALL '
        f'(SELECT {select} FROM "app"."cells" WHERE "a" = %(last_0)s AND "b" > %(last_1)s '
        f'{_CELLS_ORDER}) UNION ALL '
        f'(SELECT {select} FROM "app"."cells" WHERE "a" > %(last_0)s {_CELLS_ORDER}) '
        'ORDER BY "ck_k0", "ck_k1", "ck_k2" LIMIT 50'
    )
    listing = build_orphan_listing_sql(
        "app.cells", fk, ["a", "b", "c"], page_size=50,
        after=[sql.Placeholder(f"after_{i}") for i in range(3)],
    ).as_string(None)
    order = _CELLS_ORDER
    assert listing.startswith(
        'WITH pg AS MATERIALIZED ((SELECT "a", "b", "c" FROM "app"."cells" WHERE '
        f'"a" = %(after_0)s AND "b" = %(after_1)s AND "c" > %(after_2)s {order}) UNION ALL '
        f'(SELECT "a", "b", "c" FROM "app"."cells" WHERE "a" = %(after_0)s AND '
        f'"b" > %(after_1)s {order}) UNION ALL (SELECT "a", "b", "c" FROM "app"."cells" '
        f'WHERE "a" > %(after_0)s {order}) ORDER BY "a", "b", "c" LIMIT 50) SELECT ')
    # A one-column key is one range already: its listing window is G142's.
    single = build_orphan_listing_sql("app.cells", fk, ["a"], page_size=50,
                                      after=[sql.Placeholder("after_0")]).as_string(None)
    assert 'WHERE ("a") > (%(after_0)s) ORDER BY "app"."cells"."a" LIMIT 50)' in single
    assert "UNION" not in single


# ---------------------------------------------------------------------------
# The same rows in the same order as the row value, for every cursor
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("make", list(_SHAPES.values()), ids=list(_SHAPES))
@pytest.mark.parametrize("page", [1, 2, 3, 5, 8])
def test_the_ranges_read_the_rows_the_row_value_names(make, page) -> None:
    table = make()
    double = _TypedPg(table)
    key = list(table.key)
    ranges = build_pg_composite_page_sql(table.tabledef(), key, page, checksum=True,
                                         first=False).as_string(None)
    row_value = build_pg_composite_page_sql(table.tabledef(), key, page, checksum=True,
                                            first=False, row_value=True).as_string(None)
    # Every key as the cursor, against the table and against it with every other row gone
    # (a cursor whose row was deleted between two pages names a key no row holds).
    cursors = [_cursor(double, table, row) for row in _in_order(table)]
    for rows in (table.rows, _in_order(table)[::2], _in_order(table)[1::2]):
        held = _Table(table.name, table.key, rows, types=table.types)
        for cursor in cursors:
            expected = _after(double, held, cursor, page)
            assert double._window(held, key, ranges, cursor, page) == expected, cursor
            assert double._window(held, key, row_value, cursor, page) == expected, cursor


@pytest.mark.parametrize("make", list(_SHAPES.values()), ids=list(_SHAPES))
@pytest.mark.parametrize("page", [1, 3, 7, 1000])
@pytest.mark.parametrize("checksum", [True, False])
def test_a_walk_reads_every_row_once_with_the_row_values_cursors(make, page, checksum) -> None:
    table = make()
    target, source = _TypedPg(table), _TypedPg(table)
    walked = v._composite_pages_pg(target, table.tabledef(), table.key, page,
                                   checksum=checksum)
    assert walked.rows == len(table.rows)
    assert walked.checksum == (sum(_tok(r) for r in table.rows) if checksum else 0)
    # The row value (a PostgreSQL source's pages) takes the same cursors to the same pages.
    v._composite_pages_pg(source, table.tabledef(), table.key, page, checksum=checksum,
                          side="source")
    assert [params for _, params in target.executed] == [params for _, params in source.executed]
    assert target.page_sums == source.page_sums
    assert all(" UNION ALL " in s for s, params in target.executed if params)
    assert not any(" UNION ALL " in s for s, _ in source.executed)


# ---------------------------------------------------------------------------
# Orphans: Validation's pages, the cut-over count and the listing
# ---------------------------------------------------------------------------


def _orders_child() -> "tuple[_Table, _Table, ForeignKeyDef, int]":
    """A 3-column child whose ``order_id`` has no parent row for 1001 and 1004."""
    rows = [{"order_id": 1000 + i // 6, "line_no": (i // 2) % 3, "part": i % 2, "q": i}
            for i in range(42)]
    child = _Table("app.lines", ("order_id", "line_no", "part"), rows)
    orders = _Table("app.orders", ("id",), [{"id": i} for i in range(1000, 1007)
                                            if i not in (1001, 1004)])
    fk = ForeignKeyDef(name="fk_lines_order", columns=["order_id"],
                       referenced_table="app.orders", referenced_columns=["id"])
    return child, orders, fk, sum(r["order_id"] in (1001, 1004) for r in rows)


@pytest.mark.parametrize("page", [1, 2, 4, 5, 6, 41, 42, 100])
def test_the_orphan_pages_count_every_orphan_once(page) -> None:
    child, orders, fk, expected = _orders_child()
    double = PgDouble(child, orders)
    assert v.target_orphan_count_composite(double, child.name, fk, child.key, page) == expected
    pages = [(s, params) for s, params in double.executed if "WITH pg AS MATERIALIZED" in s]
    assert all(" UNION ALL " in s for s, params in pages if params)
    # Its windows are the count pages' windows: the same cursors, page after page.
    counted = PgDouble(child)
    v._composite_pages_pg(counted, child.tabledef(), child.key, page, checksum=False)
    assert [params for _, params in pages] == [params for _, params in counted.executed]


def test_the_cut_over_count_pages_a_three_column_child_by_the_ranges() -> None:
    child, orders, fk, expected = _orders_child()
    double = PgDouble(child, orders)
    assert engine._count_orphans(lambda: double, child.name, fk, 4) == expected
    pages = [s for s, params in double.executed if "WITH pg AS MATERIALIZED" in s and params]
    assert len(pages) == 10  # 42 rows: 10 more pages after the first, the last one short
    assert all('"order_id" = %(last_0)s AND "line_no" = %(last_1)s AND "part" > %(last_2)s'
               in s and ") > (" not in s for s in pages)


def test_the_listing_reads_the_page_where_the_count_found_the_first_orphan(monkeypatch) -> None:
    child, orders, fk, expected = _orders_child()
    double = PgDouble(child, orders)
    page = 4
    monkeypatch.setattr(engine, "_ORPHAN_PAGE_SIZE", page)
    monkeypatch.setattr(engine, "_count_orphans",
                        functools.partial(engine._count_orphans, page_size=page))
    count, template, runnable = fkv.explain_foreign_key_failure(lambda: double, child.name, fk)
    assert count == expected
    # The count's pages in key order: the first orphan (order 1001) is on the second, after
    # the first page's last key.
    ordered = _in_order(child)
    first = next(i for i, r in enumerate(ordered) if r["order_id"] == 1001) // page
    cursor = _cursor(double, child, ordered[first * page - 1], "after")
    assert (first, cursor) == (1, {"after_0": "1000", "after_1": "1", "after_2": "1"})
    assert ('WHERE "order_id" = %(after_0)s AND "line_no" = %(after_1)s AND "part" > '
            '%(after_2)s') in template and ") > (" not in template
    filled = template
    for name, value in cursor.items():
        filled = filled.replace(f"%({name})s", sql.Literal(value).as_string(None))
    assert runnable == filled
    # Read as Aurora DSQL reads it, the window is exactly that page, orphans included.
    window = double._window(child, list(child.key), template, cursor, page)
    assert window == ordered[first * page:(first + 1) * page]
    assert any(r["order_id"] == 1001 for r in window)


# ---------------------------------------------------------------------------
# End to end: Aurora DSQL by the ranges, a PostgreSQL source by the row value
# ---------------------------------------------------------------------------


def test_a_postgresql_source_keeps_the_row_value_and_aurora_dsql_reads_the_ranges() -> None:
    table = _order_items(23)
    table.types = {c: "integer" if c != "note" else "text" for c in table.columns}
    source, target = PgDouble(table), PgDouble(table)
    report = _validator(_PgSqla(source, inheritance_parent=True), target,
                        reconcile_page_size=5).validate(
        _PG_SOURCE, _TARGET, [table.tabledef()], ValidationMode.CHECKSUM)
    assert report.items[0].matched is True, report.items[0].error
    source_pages = [(s, params) for s, params in source.executed if "ck_k0" in s]
    target_pages = [(s, params) for s, params in target.executed if "ck_k0" in s]
    assert len(source_pages) == len(target_pages) == 5
    for statement, params in source_pages[1:]:
        assert 'WHERE ("order_id", "line_no") > (%(last_0)s, %(last_1)s)' in statement
        assert statement.count('FROM ONLY "app"."order_items"') == 1
    for statement, params in target_pages[1:]:
        assert 'WHERE "order_id" = %(last_0)s AND "line_no" > %(last_1)s' in statement
        assert ") > (" not in statement
    assert [p for _, p in source_pages] == [p for _, p in target_pages]


def test_a_three_column_key_matches_and_a_missing_row_or_a_changed_value_is_seen() -> None:
    def compare(target_table: _Table, mode: ValidationMode):
        return _validator(MySqlDouble(_cells()), PgDouble(target_table),
                          reconcile_page_size=4).validate(
            _SOURCE, _TARGET, [_cells().tabledef()], mode).items[0]

    for mode in (ValidationMode.ROW_COUNT, ValidationMode.CHECKSUM):
        item = compare(_cells(), mode)
        assert item.matched is True, item.error
    missing = _cells()
    missing.rows.pop(17)  # one row missing on Aurora DSQL, mid-walk
    item = compare(missing, ValidationMode.ROW_COUNT)
    assert (item.source_row_count, item.target_row_count, item.matched) == (42, 41, False)
    changed = _cells()
    changed.rows[30]["v"] = -1  # one non-key value differs
    item = compare(changed, ValidationMode.CHECKSUM)
    assert (item.row_count_match, item.checksum_match, item.matched) == (True, False, False)


# ---------------------------------------------------------------------------
# The manual
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("lang, named, stale", [
    ("en", "Aurora DSQL with one range of its primary key per key prefix",
     "a PostgreSQL source and Aurora DSQL with a row-value cursor"),
    ("ko", "Aurora DSQL은 키 접두사마다 기본 키의 범위 하나씩으로",
     "PostgreSQL 소스와 Aurora DSQL은 행 값 커서로"),
    ("ja", "Aurora DSQL はキーのプレフィックスごとに主キーの範囲 1 つずつで",
     "PostgreSQL ソースと Aurora DSQL は行値カーソルで"),
])
def test_the_manual_says_aurora_dsql_pages_a_composite_key_by_prefix_ranges(
    lang, named, stale
) -> None:
    root = Path(__file__).resolve().parents[1] / "docs" / "manual" / lang
    lines = (root / "05-validation.md").read_text(encoding="utf-8").splitlines()
    flat = " ".join(" ".join(line.removeprefix("> ") for line in lines).split())
    assert named in flat and stale not in flat
    assert "`k0 = … AND k1 > …`" in flat
    catalog = (root / "08-testing-and-verification.md").read_text(encoding="utf-8")
    assert ("test_g143_dsql_composite_prefix_ranges.py" in catalog) == (lang != "ko")
