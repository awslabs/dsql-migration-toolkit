# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""DP-1: the Full Load's snapshot read session, set up and ended in one place.

``TableExporter.stream_converted_rows`` and the generator behind
``stream_converted_ranges`` open the same read session (AUTOCOMMIT + a streaming cursor,
the dialect's snapshot start, then -- for a shard of a shared PostgreSQL snapshot -- ``SET
TRANSACTION SNAPSHOT``, and the source-load governor) and end it the same way (COMMIT, then
the zero-date report). The PostgreSQL shared-snapshot anchor the Full Load engine holds open
for a sharded load opens and closes through ``TableExporter.open_shared_snapshot`` /
``close_shared_snapshot``.

These pin, with a recording source double, the exact statements in order, on one
connection, with their execution options, for both entry points on MySQL and PostgreSQL;
the failure and early-close paths (the snapshot ends once, before the range pagers close --
P2 -- and the error raised is the one the read raised); and the anchor's open / close pair,
including the engine's early release. Doubles only: no MySQL, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core import exporter as exporter_mod
from dsql_migrator.core.exporter import TableExporter
from dsql_migrator.core.models import ColumnDef, SourceConnectionConfig, SourceType, TableDef
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.ui.data_migration import _full_load_engine as engine

_TXN = {"stream_results": False}
_STREAM = {"isolation_level": "AUTOCOMMIT", "stream_results": True}
_MYSQL_START = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
_PG_START = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"
_SNAP = "00000003-0000001B-1"


# ---------------------------------------------------------------------------
# A recording source double
# ---------------------------------------------------------------------------


class _Boom(RuntimeError):
    pass


class _Mappings:
    def __init__(self, rows):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _Result:
    def __init__(self, rows=(), scalar=None):
        self._rows = list(rows)
        self._scalar = scalar

    def mappings(self):
        return _Mappings(self._rows)

    def scalar(self):
        return self._scalar

    def first(self):
        return ("Threads_running", "0")


class _Conn:
    """One source connection: every call it gets is appended to ``log`` in order, tagged
    with this connection's id so the tests can tell that one connection did it all."""

    def __init__(self, log, rows, *, fail_on=None, commit_fails=False, snapshot_id=_SNAP):
        self.log = log
        self.rows = sorted(rows, key=lambda r: r["id"])
        self.fail_on = fail_on  # a callable(sql) -> bool: raise _Boom on that statement
        self.commit_fails = commit_fails
        self.snapshot_id = snapshot_id

    # SQLAlchemy Connection: a context manager whose __enter__ returns itself.
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.log.append(("exit", id(self)))
        return False

    def execution_options(self, **kwargs):
        self.log.append(("options", id(self), dict(kwargs)))
        return self

    def close(self):
        self.log.append(("close", id(self)))

    def exec_driver_sql(self, sql):
        self.log.append(("driver_sql", id(self), sql))
        return _Result(scalar=0)

    def execute(self, statement, parameters=None, execution_options=None):
        sql = str(statement)
        upper = sql.strip().upper()
        params = dict(parameters) if parameters else None
        opts = dict(execution_options) if execution_options is not None else None
        self.log.append(("execute", id(self), sql, params, opts))
        if self.fail_on is not None and self.fail_on(sql):
            raise _Boom(sql)
        if upper.startswith("COMMIT"):
            if self.commit_fails:
                raise RuntimeError("commit failed")
            return _Result()
        if upper.startswith(("START TRANSACTION", "SET TRANSACTION")):
            return _Result()
        if "THREADS_RUNNING" in upper:
            return _Result()
        if "PG_EXPORT_SNAPSHOT" in upper:
            return _Result(scalar=self.snapshot_id)
        if "PG_CATALOG.PG_CLASS" in upper:
            return _Result(scalar=False)  # not an inheritance parent
        p = params or {}
        lo, hi, last = p.get("pk_lower"), p.get("pk_upper"), p.get("last")
        rows = [
            r for r in self.rows
            if (last is None or r["id"] > last)
            and (lo is None or r["id"] >= lo)
            and (hi is None or r["id"] < hi)
        ]
        return _Result(rows[: p.get("batch_size")])


class _Engine:
    def __init__(self, conn):
        self.conn = conn

    def connect(self):
        self.conn.log.append(("connect", id(self.conn)))
        return self.conn

    def dispose(self):
        self.conn.log.append(("dispose", id(self.conn)))


def _exporter(conn, **kwargs) -> TableExporter:
    return TableExporter(engine_factory=lambda _cfg: _Engine(conn), batch_size=2, **kwargs)


def _mysql_cfg() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="db.example.com", database="app")


def _pg_cfg() -> SourceConnectionConfig:
    return SourceConnectionConfig(
        source_type=SourceType.POSTGRES, host="pg.example.com", port=5432, database="app"
    )


def _table(name: str) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="int"), ColumnDef(name="placed", mysql_type="date")],
        primary_key=["id"],
    )


def _rows(n: int, placed="0000-00-00"):
    return [{"id": i, "placed": placed} for i in range(1, n + 1)]


def _shape(log, conn):
    """The log as ``(kind, sql head or kwargs, params, options)``; every entry on ``conn``."""
    out = []
    for entry in log:
        kind = entry[0]
        if kind in ("zero_dates",) or kind.startswith("pager"):
            out.append(entry)
            continue
        assert entry[1] == id(conn), f"{kind} ran on another connection"
        if kind == "execute":
            _k, _c, sql, params, opts = entry
            head = sql.strip()
            if head.upper().startswith("SELECT") and " FROM pg_catalog" not in head:
                # A page read: what it reads from (its WHERE / ORDER BY are keyset_pages').
                head = head.split(" WHERE ")[0].split(" ORDER BY ")[0]
            out.append(("execute", head, params, opts))
        elif kind == "options":
            out.append(("options", entry[2]))
        else:
            out.append((kind,))
    return out


def _page(sql_head: str, **params):
    return ("execute", sql_head, params, None)


_MYSQL_PAGE = "SELECT `id`, `placed` FROM `app`.`orders`"
_PG_PAGE = 'SELECT "id", CAST("placed" AS text) AS "placed" FROM "app"."orders"'
_PG_CATALOG = (
    "SELECT (c.relkind = 'r' AND c.relhassubclass) AS inheritance_parent "
    "FROM pg_catalog.pg_class c "
    "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = :schema AND c.relname = :name"
)


def _pg_catalog_read():
    return ("execute", _PG_CATALOG, {"schema": "app", "name": "orders"}, None)


def _page_head(dialect: str) -> str:
    return _MYSQL_PAGE if dialect == "mysql" else _PG_PAGE


# ---------------------------------------------------------------------------
# stream_converted_rows: statement order
# ---------------------------------------------------------------------------


def test_the_row_stream_on_mysql_opens_reads_and_ends_its_snapshot_in_this_order() -> None:
    log: list = []
    conn = _Conn(log, _rows(3))
    zero: list = []
    out = list(_exporter(conn).stream_converted_rows(
        _mysql_cfg(), _table("app.orders"),
        on_zero_dates=lambda found: log.append(("zero_dates", found)) or zero.append(found),
    ))
    assert [r["id"] for r in out] == [1, 2, 3]
    assert _shape(log, conn) == [
        ("connect",),
        ("options", _STREAM),
        ("execute", _MYSQL_START, None, _TXN),
        _page(_MYSQL_PAGE, batch_size=2),
        _page(_MYSQL_PAGE, last=2, batch_size=2),
        ("execute", "COMMIT", None, _TXN),
        ("zero_dates", (("placed", "zero", "NULL", 3),)),
        ("exit",),
        ("dispose",),
    ]


def test_the_row_stream_on_postgres_adopts_the_shared_snapshot_first_then_reads() -> None:
    log: list = []
    conn = _Conn(log, _rows(3, placed=None))
    out = list(_exporter(conn).stream_converted_rows(
        _pg_cfg(), _table("app.orders"), shared_snapshot_id=_SNAP,
    ))
    assert [r["id"] for r in out] == [1, 2, 3]
    assert _shape(log, conn) == [
        ("connect",),
        ("options", _STREAM),
        ("execute", _PG_START, None, _TXN),
        ("execute", f"SET TRANSACTION SNAPSHOT '{_SNAP}'", None, _TXN),
        _pg_catalog_read(),
        _page(_PG_PAGE, batch_size=2),
        _page(_PG_PAGE, last=2, batch_size=2),
        ("execute", "COMMIT", None, _TXN),
        ("exit",),
        ("dispose",),
    ]


def test_the_row_stream_without_a_shared_snapshot_sends_no_set_transaction() -> None:
    log: list = []
    conn = _Conn(log, _rows(1, placed=None))
    list(_exporter(conn).stream_converted_rows(_pg_cfg(), _table("app.orders")))
    shape = _shape(log, conn)
    assert shape[:4] == [
        ("connect",), ("options", _STREAM), ("execute", _PG_START, None, _TXN),
        _pg_catalog_read(),
    ]
    assert not any("SET TRANSACTION" in str(e) for e in shape)


def test_the_governor_reads_the_snapshot_connection_before_each_page() -> None:
    log: list = []
    conn = _Conn(log, _rows(3, placed=None))
    list(_exporter(conn, max_source_threads_running=50).stream_converted_rows(
        _mysql_cfg(), _table("app.orders"),
    ))
    kinds = [e[1] for e in _shape(log, conn) if e[0] == "execute"]
    assert kinds[0] == _MYSQL_START
    assert "Threads_running" in kinds[1]  # asked before the first page
    assert kinds[2].startswith(_MYSQL_PAGE)
    assert kinds[-1] == "COMMIT"


def test_a_failed_page_ends_the_snapshot_once_and_raises_the_read_error() -> None:
    log: list = []
    conn = _Conn(log, _rows(5), fail_on=lambda sql: ":last" in sql)
    zero: list = []
    stream = _exporter(conn).stream_converted_rows(
        _mysql_cfg(), _table("app.orders"), on_zero_dates=zero.append)
    with pytest.raises(_Boom):
        list(stream)
    shape = _shape(log, conn)
    assert shape[-4:] == [
        _page(_MYSQL_PAGE, last=2, batch_size=2),
        ("execute", "COMMIT", None, _TXN),
        ("exit",),
        ("dispose",),
    ]
    assert [e for e in shape if e[1:2] == ("COMMIT",)] == [("execute", "COMMIT", None, _TXN)]
    assert zero == []


def test_a_failed_commit_after_a_failed_page_never_masks_the_read_error() -> None:
    log: list = []
    conn = _Conn(log, _rows(5), fail_on=lambda sql: ":last" in sql, commit_fails=True)
    with pytest.raises(_Boom):
        list(_exporter(conn).stream_converted_rows(_mysql_cfg(), _table("app.orders")))
    assert _shape(log, conn)[-3:] == [
        ("execute", "COMMIT", None, _TXN), ("exit",), ("dispose",),
    ]


def test_a_failed_snapshot_start_sends_no_commit() -> None:
    log: list = []
    conn = _Conn(log, _rows(2), fail_on=lambda sql: sql.startswith("START"))
    with pytest.raises(_Boom):
        list(_exporter(conn).stream_converted_rows(_mysql_cfg(), _table("app.orders")))
    assert _shape(log, conn) == [
        ("connect",), ("options", _STREAM), ("execute", _MYSQL_START, None, _TXN),
        ("exit",), ("dispose",),
    ]


def test_a_failed_shared_snapshot_import_sends_no_commit() -> None:
    log: list = []
    conn = _Conn(log, _rows(2, placed=None), fail_on=lambda sql: sql.startswith("SET TRANSACTION"))
    with pytest.raises(_Boom):
        list(_exporter(conn).stream_converted_rows(
            _pg_cfg(), _table("app.orders"), shared_snapshot_id=_SNAP))
    assert [e[0] for e in _shape(log, conn)][-2:] == ["exit", "dispose"]
    assert not any(e[1:2] == ("COMMIT",) for e in _shape(log, conn))


def test_a_stream_closed_early_ends_the_snapshot_and_reports_nothing() -> None:
    log: list = []
    conn = _Conn(log, _rows(5))
    zero: list = []
    stream = _exporter(conn).stream_converted_rows(
        _mysql_cfg(), _table("app.orders"), on_zero_dates=zero.append)
    assert next(stream)["id"] == 1
    stream.close()
    assert _shape(log, conn)[-3:] == [
        ("execute", "COMMIT", None, _TXN), ("exit",), ("dispose",),
    ]
    assert zero == []


# ---------------------------------------------------------------------------
# stream_converted_ranges: statement order and the P2 drain rule
# ---------------------------------------------------------------------------


class _LoggedPager:
    """Wraps one range's ``keyset_pages`` so its ``close()`` shows in the log."""

    def __init__(self, inner, log, index):
        self._inner, self._log, self._index = inner, log, index

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._inner)

    def close(self):
        self._log.append(("pager.close", self._index))
        self._inner.close()


@pytest.fixture
def logged_pagers(monkeypatch):
    """Route the module-global ``keyset_pages`` through :class:`_LoggedPager`."""
    real = exporter_mod.keyset_pages
    state = {"log": None, "n": 0}

    def _wrapped(*args, **kwargs):
        index = state["n"]
        state["n"] += 1
        return _LoggedPager(real(*args, **kwargs), state["log"], index)

    monkeypatch.setattr(exporter_mod, "keyset_pages", _wrapped)

    def _bind(log):
        state["log"] = log
        state["n"] = 0

    return _bind


def _drain(stream):
    seen = []
    for index, page in stream:
        seen.append((index, [r["id"] for r in page]))
    return seen


@pytest.mark.parametrize("dialect", ["mysql", "pg"])
def test_the_range_stream_opens_reads_in_turn_and_ends_in_this_order(
        dialect, logged_pagers) -> None:
    log: list = []
    logged_pagers(log)
    conn = _Conn(log, _rows(6, placed="0000-00-00" if dialect == "mysql" else None))
    cfg = _mysql_cfg() if dialect == "mysql" else _pg_cfg()
    snap = None if dialect == "mysql" else _SNAP
    pages = _exporter(conn).stream_converted_ranges(
        cfg, _table("app.orders"), [(None, 4), (4, None)], page_rows=2,
        shared_snapshot_id=snap,
        on_zero_dates=lambda found: log.append(("zero_dates", found)),
    )
    assert _drain(pages) == [(0, [1, 2]), (1, [4, 5]), (0, [3]), (1, [6])]
    head = _page_head(dialect)
    opening = [("connect",), ("options", _STREAM)]
    if dialect == "mysql":
        opening.append(("execute", _MYSQL_START, None, _TXN))
    else:
        opening += [
            ("execute", _PG_START, None, _TXN),
            ("execute", f"SET TRANSACTION SNAPSHOT '{_SNAP}'", None, _TXN),
            _pg_catalog_read(),
        ]
    ending = [("pager.close", 0), ("pager.close", 1), ("execute", "COMMIT", None, _TXN)]
    if dialect == "mysql":
        ending.append(("zero_dates", (("placed", "zero", "NULL", 6),)))
    ending += [("exit",), ("dispose",)]
    shape = _shape(log, conn)
    assert shape[: len(opening)] == opening
    assert shape[-len(ending):] == ending
    reads = shape[len(opening): -len(ending)]
    assert [r[:2] for r in reads] == [("execute", head)] * len(reads)
    assert [r[2] for r in reads] == [
        {"batch_size": 2, "pk_upper": 4},
        {"batch_size": 2, "pk_lower": 4},
        {"last": 2, "batch_size": 2, "pk_upper": 4},
        {"last": 5, "batch_size": 2, "pk_lower": 4},
        # Each range's short page ended it: no further read of either.
    ]


def test_a_failed_range_read_ends_the_snapshot_before_the_pagers_close(logged_pagers) -> None:
    # P2: on any error (or a close before the end) the snapshot ends once FIRST, then the
    # pagers close -- the reverse of the success path, where the pagers close before COMMIT.
    log: list = []
    logged_pagers(log)
    conn = _Conn(log, _rows(6), fail_on=lambda sql: ":last" in sql)
    zero: list = []
    pages = _exporter(conn).stream_converted_ranges(
        _mysql_cfg(), _table("app.orders"), [(None, 4), (4, None)], page_rows=2,
        on_zero_dates=zero.append)
    with pytest.raises(_Boom):
        _drain(pages)
    shape = _shape(log, conn)
    assert shape[-6:] == [
        _page(_MYSQL_PAGE, last=2, batch_size=2, pk_upper=4),
        ("execute", "COMMIT", None, _TXN),
        ("pager.close", 0),
        ("pager.close", 1),
        ("exit",),
        ("dispose",),
    ]
    assert sum(1 for e in shape if e[1:2] == ("COMMIT",)) == 1
    assert zero == []


def test_a_range_stream_closed_early_ends_the_snapshot_then_closes_the_pagers(
        logged_pagers) -> None:
    log: list = []
    logged_pagers(log)
    conn = _Conn(log, _rows(6))
    zero: list = []
    pages = _exporter(conn).stream_converted_ranges(
        _mysql_cfg(), _table("app.orders"), [(None, 4), (4, None)], page_rows=2,
        on_zero_dates=zero.append)
    index, page = next(pages)
    assert index == 0 and [r["id"] for r in page] == [1, 2]
    pages.close()
    assert _shape(log, conn)[-5:] == [
        ("execute", "COMMIT", None, _TXN),
        ("pager.close", 0),
        ("pager.close", 1),
        ("exit",),
        ("dispose",),
    ]
    assert zero == []


def test_a_failed_snapshot_start_of_a_range_stream_sends_no_commit(logged_pagers) -> None:
    log: list = []
    logged_pagers(log)
    conn = _Conn(log, _rows(2), fail_on=lambda sql: sql.startswith("START"))
    with pytest.raises(_Boom):
        _drain(_exporter(conn).stream_converted_ranges(
            _mysql_cfg(), _table("app.orders"), [(None, None)], page_rows=2))
    assert _shape(log, conn) == [
        ("connect",), ("options", _STREAM), ("execute", _MYSQL_START, None, _TXN),
        ("exit",), ("dispose",),
    ]


def test_both_entry_points_open_and_end_the_session_alike(logged_pagers) -> None:
    # The two copies the review found, compared: what each sends before its first page
    # read and after its last one is the same (bar the pagers only the ranges have).
    def _run(kind):
        log: list = []
        logged_pagers(log)
        conn = _Conn(log, _rows(3, placed=None))
        exp = _exporter(conn)
        if kind == "rows":
            list(exp.stream_converted_rows(_pg_cfg(), _table("app.orders"),
                                           shared_snapshot_id=_SNAP))
        else:
            _drain(exp.stream_converted_ranges(_pg_cfg(), _table("app.orders"),
                                               [(None, None)], page_rows=2,
                                               shared_snapshot_id=_SNAP))
        shape = [e for e in _shape(log, conn) if not e[0].startswith("pager")]
        reads = [i for i, e in enumerate(shape) if e[0] == "execute"
                 and e[1].startswith(_PG_PAGE)]
        return shape[: reads[0]], shape[reads[-1] + 1:]

    assert _run("rows") == _run("ranges")


# ---------------------------------------------------------------------------
# The shared-snapshot anchor: an open / close pair
# ---------------------------------------------------------------------------


def _migrator(conn):
    class _Mig:
        _exporter = TableExporter(engine_factory=lambda _cfg: _Engine(conn))

        class _inputs:
            source_config = _pg_cfg()

    return _Mig()


def test_the_anchor_opens_a_repeatable_read_snapshot_and_exports_its_id() -> None:
    log: list = []
    conn = _Conn(log, [])
    eng, held, snapshot_id = engine._open_shared_snapshot(
        _migrator(conn), dialect_for(SourceType.POSTGRES))
    assert held is conn and snapshot_id == _SNAP and isinstance(eng, _Engine)
    assert _shape(log, conn) == [
        ("connect",),
        ("options", {"isolation_level": "AUTOCOMMIT"}),
        ("execute", _PG_START, None, _TXN),
        ("execute", "SELECT pg_export_snapshot()", None, None),
    ]


def test_the_anchor_closes_with_a_commit_then_the_connection_then_the_engine() -> None:
    log: list = []
    conn = _Conn(log, [])
    eng, held, _sid = engine._open_shared_snapshot(
        _migrator(conn), dialect_for(SourceType.POSTGRES))
    del log[:]
    engine._close_shared_snapshot(eng, held)
    assert _shape(log, conn) == [
        ("options", {"isolation_level": "AUTOCOMMIT"}),
        ("execute", "COMMIT", None, _TXN),
        ("close",),
        ("dispose",),
    ]


def test_an_anchor_released_early_is_not_closed_again() -> None:
    # The engine closes the anchor once the last shard reading it returns, sets its handles
    # to None, and its finally closes "(None, None)" again: that second close does nothing.
    log: list = []
    conn = _Conn(log, [])
    eng, held, _sid = engine._open_shared_snapshot(
        _migrator(conn), dialect_for(SourceType.POSTGRES))
    engine._close_shared_snapshot(eng, held)
    count = len(log)
    engine._close_shared_snapshot(None, None)
    assert len(log) == count
    # A connection already gone: the engine is left alone too (today's behavior).
    engine._close_shared_snapshot(eng, None)
    assert len(log) == count


def test_a_failed_anchor_commit_still_closes_the_connection_and_the_engine() -> None:
    log: list = []
    conn = _Conn(log, [], commit_fails=True)
    eng, held, _sid = engine._open_shared_snapshot(
        _migrator(conn), dialect_for(SourceType.POSTGRES))
    del log[:]
    engine._close_shared_snapshot(eng, held)  # the COMMIT failure is swallowed
    assert [e[0] for e in _shape(log, conn)] == ["options", "execute", "close", "dispose"]


def test_a_failed_anchor_connection_close_still_disposes_and_raises() -> None:
    log: list = []
    conn = _Conn(log, [])

    def _bad_close():
        log.append(("close", id(conn)))
        raise _Boom("close")

    eng, held, _sid = engine._open_shared_snapshot(
        _migrator(conn), dialect_for(SourceType.POSTGRES))
    conn.close = _bad_close
    del log[:]
    with pytest.raises(_Boom):
        engine._close_shared_snapshot(eng, held)
    assert [e[0] for e in _shape(log, conn)] == ["options", "execute", "close", "dispose"]


@pytest.mark.parametrize(
    ("fail_on", "snapshot_id", "error", "sent"),
    [
        (lambda sql: sql.startswith("START"), _SNAP, _Boom, 1),
        (lambda sql: "pg_export_snapshot" in sql, _SNAP, _Boom, 2),
        (None, "", RuntimeError, 2),  # an empty exported id
    ],
)
def test_a_failed_anchor_open_closes_the_connection_before_disposing(
        fail_on, snapshot_id, error, sent) -> None:
    log: list = []
    conn = _Conn(log, [], fail_on=fail_on, snapshot_id=snapshot_id)
    with pytest.raises(error):
        engine._open_shared_snapshot(_migrator(conn), dialect_for(SourceType.POSTGRES))
    shape = _shape(log, conn)
    assert shape[:2] == [("connect",), ("options", {"isolation_level": "AUTOCOMMIT"})]
    assert [e[0] for e in shape[2:]] == ["execute"] * sent + ["close", "dispose"]


def test_the_anchor_reads_the_dialect_it_is_given() -> None:
    # The engine passes the dialect it planned the shards with; the anchor uses it as given.
    log: list = []
    conn = _Conn(log, [])

    class _Dialect:
        snapshot_start_sql = "START TRANSACTION -- given"

        @staticmethod
        def export_snapshot_sql():
            return "SELECT pg_export_snapshot() -- given"

    engine._open_shared_snapshot(_migrator(conn), _Dialect())
    assert [e[1] for e in _shape(log, conn) if e[0] == "execute"] == [
        "START TRANSACTION -- given", "SELECT pg_export_snapshot() -- given",
    ]



# ---------------------------------------------------------------------------
# DP-1's shape: one opening, one ending, one anchor pair
# ---------------------------------------------------------------------------


def test_the_anchor_pair_is_the_exporters_and_the_engine_delegates_to_it() -> None:
    log: list = []
    conn = _Conn(log, [])
    exp = TableExporter(engine_factory=lambda _cfg: _Engine(conn))
    eng, held, snapshot_id = exp.open_shared_snapshot(_pg_cfg(), dialect_for(SourceType.POSTGRES))
    assert held is conn and snapshot_id == _SNAP
    TableExporter.close_shared_snapshot(eng, held)
    assert [e[0] for e in _shape(log, conn)] == [
        "connect", "options", "execute", "execute", "options", "execute", "close", "dispose",
    ]
    # The engine's names stay (tests and the parallel phase patch / call them) and only
    # delegate: no SQL of their own, no reach into the exporter's private engine factory.
    import inspect

    for name in ("_open_shared_snapshot", "_close_shared_snapshot"):
        source = inspect.getsource(getattr(engine, name))
        assert "_engine_factory" not in source and "execute(" not in source, name
        assert "_TXN_CONTROL_EXEC_OPTS" not in source, name


def test_both_entry_points_open_and_end_through_the_one_pair_of_helpers() -> None:
    import ast
    import inspect
    import textwrap

    for method in (TableExporter.stream_converted_rows, TableExporter._ranges_in_turn):
        tree = ast.parse(textwrap.dedent(inspect.getsource(method)))
        called = [
            node.func.id for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        ]
        assert called.count("_open_read_session") == 1, method.__name__
        assert called.count("_finish_read_session") == 1, method.__name__
        assert called.count("_end_snapshot_after_failure") == 1, method.__name__
        assert "SourceLoadGovernor" not in called, method.__name__
        names = {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        assert "snapshot_start_sql" not in names and "set_transaction_snapshot_sql" not in names
