# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""VAL-3: one single-column orphan keyset pager, shared by Validation and the Full Load.

Validation's orphan check (``validator._target_orphan_count``) and the Full Load's
foreign-key pre-gate (``_foreign_keys._count_orphans``) page a single-column-keyed child
the same way: ``build_pg_orphan_page_first_sql`` once, then ``build_pg_orphan_page_next_sql``
bound with ``{"last": <the previous page's last key, as text>}``, one cursor per page, a
liveness call before each, until a page returns fewer rows than the page size. The pre-gate
also replays a page that conflicts (40001) or hits an XX000 -- only that page, after a
rollback -- and tells ``on_orphans`` the cursor of the first page holding an orphan.

These pin, with a recording target double, the statements, their parameters, the page
boundaries and the per-page cursor for both callers, and that the two callers send the same
pages; then the shared pager's hooks. Doubles only: no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core import target_introspector
from dsql_migrator.core import validator as v
from dsql_migrator.core.models import ColumnDef, ForeignKeyDef, TableDef
from dsql_migrator.core.validation_sql import (
    build_pg_orphan_page_first_sql,
    build_pg_orphan_page_next_sql,
)
from dsql_migrator.ui.data_migration import _foreign_keys as fk_pass
from dsql_migrator.ui.data_migration import _full_load_engine as engine

CHILD = "app.child"
FK = ForeignKeyDef(name="fk_child_parent", columns=["pid"], referenced_table="app.parent",
                   referenced_columns=["id"])
_ONE_ARG = "<no parameters>"


class _Conflict(Exception):
    sqlstate = "40001"


class _Cur:
    def __init__(self, db):
        self.db = db
        self.row = None

    def execute(self, statement, *parameters):
        assert len(parameters) <= 1
        bound = parameters[0] if parameters else _ONE_ARG
        self.db.log.append(("execute", str(statement), bound))
        if self.db.fail_next:
            self.db.fail_next -= 1
            raise _Conflict("40001")
        self.row = self.db.next_row()

    def fetchone(self):
        self.db.log.append(("fetchone",))
        return self.row

    def close(self):
        self.db.log.append(("cursor.close",))


class _Db:
    """A target that answers each orphan page from ``pages`` in order."""

    def __init__(self, pages, *, fail_on_page=None):
        self.pages = list(pages)
        self.log: list = []
        self.served = 0
        self.fail_on_page = fail_on_page
        self.fail_next = 0

    def next_row(self):
        row = self.pages[self.served] if self.served < len(self.pages) else None
        self.served += 1
        return row

    def cursor(self):
        if self.fail_on_page is not None and self.served + 1 == self.fail_on_page:
            self.fail_on_page = None
            self.fail_next = 1
        self.log.append(("cursor",))
        return _Cur(self)

    def rollback(self):
        self.log.append(("rollback",))

    def close(self):
        self.log.append(("conn.close",))


def _first(page_size) -> str:
    return str(build_pg_orphan_page_first_sql(CHILD, FK, "id", page_size))


def _next(page_size) -> str:
    return str(build_pg_orphan_page_next_sql(CHILD, FK, "id", page_size))


def _table() -> TableDef:
    return TableDef(name=CHILD, columns=[ColumnDef(name="id", mysql_type="int"),
                                         ColumnDef(name="pid", mysql_type="int")],
                    primary_key=["id"])


def _page_log(page_size, *lasts):
    """The calls one cursor-per-page read sends: the first page, then one per cursor."""
    out = []
    for i, last in enumerate((None, *lasts)):
        sql = _first(page_size) if i == 0 else _next(page_size)
        out += [("cursor",), ("execute", sql, _ONE_ARG if last is None else {"last": last}),
                ("fetchone",), ("cursor.close",)]
    return out


# (orphans on the page, the page's last key as text, rows on the page)
PAGES = [(0, "2", 2), (1, "4", 2), (3, "6", 2), (0, "7", 1)]


def _validation(db, page_size=2, beats=None):
    return v._target_orphan_count(
        db, _table(), FK, page_size,
        on_page=None if beats is None else (lambda: beats.append(1)),
    )


def _pre_gate(db, monkeypatch, page_size=2, beats=None, told=None, key=("id",)):
    monkeypatch.setattr(target_introspector, "read_target_primary_key",
                        lambda _conn, _table: tuple(key))
    return engine._count_orphans(
        lambda: db, CHILD, FK, page_size,
        on_page=None if beats is None else (lambda: beats.append(1)),
        sleep=lambda _s: None,
        on_orphans=None if told is None else told.append,
    )


# ---------------------------------------------------------------------------
# Query / page parity, per caller
# ---------------------------------------------------------------------------


def test_validation_pages_the_child_by_keyset_one_cursor_per_page() -> None:
    db = _Db(PAGES)
    beats: list = []
    assert _validation(db, beats=beats) == 4
    assert db.log == _page_log(2, "2", "4", "6")
    assert len(beats) == 4


def test_the_full_load_pre_gate_pages_the_child_the_same_way(monkeypatch) -> None:
    db = _Db(PAGES)
    beats: list = []
    told: list = []
    assert _pre_gate(db, monkeypatch, beats=beats, told=told) == 4
    assert db.log == _page_log(2, "2", "4", "6") + [("conn.close",)]
    assert len(beats) == 4
    assert told == [("2",)]  # the cursor of the first page holding an orphan, once


def test_both_callers_send_the_same_pages(monkeypatch) -> None:
    for pages in (PAGES, [(5, "3", 3)], [(0, "9", 2), (0, "11", 2), (0, None, 0)], []):
        a, b = _Db(pages), _Db(pages)
        beats_a: list = []
        beats_b: list = []
        assert _validation(a, page_size=2, beats=beats_a) == _pre_gate(
            b, monkeypatch, page_size=2, beats=beats_b)
        assert a.log == b.log[:-1] and b.log[-1] == ("conn.close",)
        assert len(beats_a) == len(beats_b)


@pytest.mark.parametrize(
    ("pages", "total", "reads"),
    [
        ([], 0, 1),                                  # no row at all: stops
        ([(None, None, None)], 0, 1),                # an empty window (NULL count): stops
        ([(2, "5", 1)], 2, 1),                       # a short first page: stops
        ([(0, "2", 2), (0, "4", 2)], 0, 3),          # full pages, then no row
        ([(1, "2", 2), (None, "4", 2), (0, "5", 0)], 1, 3),  # a NULL sub-count is 0
    ],
)
def test_the_pages_stop_where_they_stopped(monkeypatch, pages, total, reads) -> None:
    a, b = _Db(pages), _Db(pages)
    assert _validation(a) == total
    assert _pre_gate(b, monkeypatch) == total
    assert sum(1 for e in a.log if e[0] == "execute") == reads
    assert sum(1 for e in b.log if e[0] == "execute") == reads


def test_the_page_size_is_the_callers_and_bounds_every_page(monkeypatch) -> None:
    a, b = _Db([(0, "7", 7), (0, "9", 3)]), _Db([(0, "7", 7), (0, "9", 3)])
    _validation(a, page_size=7)
    _pre_gate(b, monkeypatch, page_size=7)
    assert a.log == _page_log(7, "7") == b.log[:-1]
    assert "SQL(' LIMIT '), Literal(7)" in _first(7) and "Literal(7)" in _next(7)
    # The defaults the two callers page with today.
    assert engine._ORPHAN_PAGE_SIZE == 5000 == v._RECONCILE_PAGE_SIZE


def test_the_pre_gate_replays_only_the_page_that_conflicted(monkeypatch) -> None:
    db = _Db(PAGES, fail_on_page=3)
    assert _pre_gate(db, monkeypatch) == 4
    third = [("cursor",), ("execute", _next(2), {"last": "4"})]
    assert db.log == (
        _page_log(2, "2")  # pages 1 and 2, read once
        + third + [("cursor.close",), ("rollback",)]  # page 3 conflicts: rolled back
        + third + [("fetchone",), ("cursor.close",)]  # and only page 3 runs again
        + [("cursor",), ("execute", _next(2), {"last": "6"}), ("fetchone",),
           ("cursor.close",)]  # page 4, from page 3's last key
        + [("conn.close",)]
    )


def test_the_pre_gate_tells_a_first_page_orphan_as_no_cursor(monkeypatch) -> None:
    told: list = []
    _pre_gate(_Db([(1, "5", 1)]), monkeypatch, told=told)
    assert told == [None]
    told.clear()
    _pre_gate(_Db([(0, "2", 2), (0, "4", 2), (0, "5", 1)]), monkeypatch, told=told)
    assert told == []


# ---------------------------------------------------------------------------
# The one pager: its hooks, and that both callers go through it
# ---------------------------------------------------------------------------


def test_the_shared_pager_runs_each_page_through_page_row_when_given() -> None:
    calls: list = []
    rows = iter(PAGES)
    total = v.target_orphan_count_keyset(
        None, CHILD, FK, "id", 2,
        page_row=lambda statement, parameters: calls.append(
            (str(statement), parameters)) or next(rows),
    )
    assert total == 4
    assert calls == [(_first(2), None), (_next(2), {"last": "2"}), (_next(2), {"last": "4"}),
                     (_next(2), {"last": "6"})]


def test_the_shared_pager_tells_on_orphans_once_with_the_pages_cursor() -> None:
    told: list = []
    v.target_orphan_count_keyset(_Db(PAGES), CHILD, FK, "id", 2, on_orphans=told.append)
    assert told == [("2",)]  # page 2 held the first orphan; page 3's are not told again
    told.clear()
    v.target_orphan_count_keyset(_Db([(2, "2", 2), (1, "3", 1)]), CHILD, FK, "id", 2,
                                 on_orphans=told.append)
    assert told == [None]


def test_the_full_load_has_no_pager_of_its_own() -> None:
    import ast
    import inspect
    import textwrap

    # The pre-gate lives in the foreign-key pass (_foreign_keys), which the engine re-exports:
    # the Full Load is both modules.
    for module in (engine, fk_pass):
        assert not hasattr(module, "_count_orphans_keyset")
    tree = ast.parse(textwrap.dedent(inspect.getsource(fk_pass._count_orphans)))
    called = {n.func.id for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert {"target_orphan_count_keyset", "target_orphan_count_composite"} <= called
    for module in (engine, fk_pass):
        source = inspect.getsource(module)
        assert "build_pg_orphan_page_first_sql" not in source
        assert "build_pg_orphan_page_next_sql" not in source
