# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""P2: one MySQL reader session reads a large table's key ranges in turn, from one snapshot.

One primary-key-ordered write stream into one Aurora DSQL table is capped (about 20K rows/s
live, whatever is in flight); the same table read in key ranges loaded 37-62K. A MySQL table
whose read must stay one snapshot (G137: Full load only, no binlog start point, or a split the
unique-key guard refuses) is now read in its K ranges by ONE reader session -- one START
TRANSACTION WITH CONSISTENT SNAPSHOT under the G136 REPEATABLE READ pin -- a page of each range
in turn, so its batches, and so its writes, alternate across the ranges while the table keeps
one source connection and one process. These pin:

* ``keyset_pages``: ``keyset_stream`` page by page, the same statements, rows, errors and
  DEBUG lines for a plain, a composite, an ENUM-walked and a FLOAT-reread key;
* ``TableExporter.stream_converted_ranges``: one snapshot and one COMMIT, the ranges in turn,
  each in key order, every row once, the governor and the cancel before every page, one
  value-fit guard and one zero-date tally, and no next page before the last one was read to
  its end -- PyMySQL would drain it silently (the source double here raises instead);
* the importer's ``range_pages``: batch ids and resume per range under any interleaving, one
  producer thread;
* ``migrate_table(read_ranges=)`` and the whole-table worker: the ranges, the in-flight cap,
  the unchanged checks, and a lost source connection that re-reads every range from a fresh
  session; the process path end to end (the planner's G137 cases: test_g137_mysql_reader_shards);
* memory: wide rows never pile up beyond the batches queued and in flight.

Doubles only: no MySQL, no Aurora DSQL, no AWS.
"""

from __future__ import annotations

import contextlib
import itertools
import logging
import threading
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest

import dsql_migrator.core.exporter as exporter_module
import dsql_migrator.ui.data_migration._full_load_engine as engine
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportError,
    BatchedImportOptions,
    BatchedImportResult,
    OnConflictMode,
    _SkipCounter,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.exporter import (
    ExportCancelled,
    ExportError,
    SourceLoadGovernor,
    TableExporter,
    ValueConversionError,
    keyset_pages,
    keyset_stream,
)
from dsql_migrator.core.load_stats import LoadStats
from dsql_migrator.core.mysql_zero_dates import ZERO as ZERO_DATE
from dsql_migrator.core.models import (
    ColumnDef,
    MigrationJob,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.ui.data_migration._full_load_engine import (
    BatchedTableMigrator,
    DataMigrationInputs,
)

from tests.test_full_load_overlay import _InlineExecutor
from tests.test_g125_keyset_review import _MySqlModel, _table as _shop_table
from tests.test_g130_keyset_walk_review import _COVERING
from tests.test_g137_mysql_reader_shards import _Handle, _wm
from tests.test_g144_float_key_full_load import _FloatSource, _prices, _tabledef
from tests.test_ui_data_migration import _lost_connection

_MY = SourceConnectionConfig(host="db", database="app")
_PG = SourceConnectionConfig(host="db", database="app", source_type=SourceType.POSTGRES)
_RANGES = [(None, 100), (100, 200), (200, None)]
EVENTS = "app.events"


def _events(*extra: ColumnDef, name: str = EVENTS) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="bigint", nullable=False),
                 ColumnDef(name="v", mysql_type="varchar(20)"), *extra],
        primary_key=["id"],
    )


def _rows(*bands: "tuple[int, int]") -> list[dict]:
    """Rows ``id`` lo..hi-1 per band, ``v`` naming the id."""
    return [{"id": i, "v": f"v{i}"} for lo, hi in bands for i in range(lo, hi)]


# Range 0 holds 7 rows, range 1 holds 3, range 2 holds 10.
_THREE = _rows((1, 8), (100, 103), (200, 210))


# ---------------------------------------------------------------------------
# A MySQL source session as PyMySQL's unbuffered cursor serves it
# ---------------------------------------------------------------------------


class _Result:
    """One page on the double: its rows come one at a time as they are read, and it is
    ``done`` only once read past its last row (PyMySQL's end-of-result packet)."""

    def __init__(self, rows: list, make_row, description) -> None:
        self._rows = rows
        self._make_row = make_row
        self.done = False
        self.cursor = SimpleNamespace(description=description)

    def mappings(self):
        return self._stream()

    def _stream(self):
        for row in self._rows:
            yield self._make_row(row)
        self.done = True


class _SourceSession:
    """A MySQL source session as the Full Load's server-side cursor sees it.

    Keyset pages over ``rows`` (a one-column integer key ``id``, honouring the range bound and
    the cursor), the snapshot statements, and Threads_running for the governor. Like PyMySQL
    it holds ONE open result, and a statement sent while the last page still has rows to read
    RAISES here -- PyMySQL would read those rows off the socket and drop them, with no error
    (``Connection._execute_command``: "Previous unbuffered result was left incomplete") --
    as does a statement from a second thread: one connection, one thread."""

    def __init__(self, rows, *, make_row=dict, description=None, threads_running=0) -> None:
        self.rows = sorted(rows, key=lambda r: r["id"])
        self.make_row = make_row
        self.description = description
        self.threads_running = threads_running
        self.executed: list[tuple[str, dict]] = []
        self.events: list[object] = []
        self.thread = None
        self._open = None

    def execution_options(self, **_options):
        return self

    def execute(self, statement, parameters=None, execution_options=None):
        ident = threading.get_ident()
        if self.thread is None:
            self.thread = ident
        if ident != self.thread:
            raise AssertionError("a second thread sent a statement on the source session")
        if self._open is not None and not self._open.done:
            raise AssertionError("Previous unbuffered result was left incomplete")
        sql = " ".join(str(statement).split())
        params = dict(parameters or {})
        self.executed.append((sql, params))
        if "Threads_running" in sql:
            return SimpleNamespace(first=lambda: ("Threads_running", str(self.threads_running)))
        if sql.startswith(("START TRANSACTION", "COMMIT", "SET TRANSACTION")):
            return SimpleNamespace()
        assert sql.startswith("SELECT "), sql
        self.events.append(("page", params.get("pk_lower"), params.get("pk_upper")))
        lo, hi, last = params.get("pk_lower"), params.get("pk_upper"), params.get("last")
        page = [
            r for r in self.rows
            if (lo is None or r["id"] >= lo) and (hi is None or r["id"] < hi)
            and (last is None or r["id"] > last)
        ][: params["batch_size"]]
        self._open = _Result(page, self.make_row, self.description)
        return self._open

    def statements(self, prefix: str) -> list[str]:
        return [sql for sql, _params in self.executed if sql.startswith(prefix)]

    @property
    def pages(self) -> list[tuple[str, dict]]:
        return [(s, p) for s, p in self.executed if s.startswith("SELECT ")]


class _Engine:
    def __init__(self, session: _SourceSession) -> None:
        self.session = session
        self.connects = 0
        self.disposed = 0

    def connect(self):
        self.connects += 1
        return contextlib.nullcontext(self.session)

    def dispose(self) -> None:
        self.disposed += 1


def _exporter(session: _SourceSession, **kwargs) -> "tuple[TableExporter, _Engine]":
    engine_ = _Engine(session)
    return TableExporter(engine_factory=lambda _conf: engine_, **kwargs), engine_


def _read_all(stream) -> list[tuple[int, list[int]]]:
    return [(index, [row["id"] for row in page]) for index, page in stream]


def _range_of(row_id: int) -> int:
    return 0 if row_id < 100 else 1 if row_id < 200 else 2


def test_the_source_double_refuses_what_pymysql_would_drain_silently() -> None:
    session = _SourceSession(_THREE)
    result = session.execute("SELECT `id` FROM t LIMIT :batch_size", {"batch_size": 5})
    rows = result.mappings()
    next(rows)                                     # one row read, four still to read
    with pytest.raises(AssertionError, match="left incomplete"):
        session.execute("COMMIT")
    list(rows)
    session.execute("COMMIT")                      # read to its end: the next one goes
    raised: list = []
    thread = threading.Thread(target=lambda: raised.append(
        pytest.raises(AssertionError, session.execute, "COMMIT")))
    thread.start()
    thread.join()
    assert "second thread" in str(raised[0].value)
    # Two keyset streams on one connection, a row of each in turn -- the naive way to read
    # ranges in turn -- is what PyMySQL drains: the double stops it at the second stream.
    table = _events()
    naive = _SourceSession(_THREE)
    a = keyset_stream(naive, table, batch_size=3, pk_upper=100)
    b = keyset_stream(naive, table, batch_size=3, pk_lower=100)
    next(a)
    with pytest.raises(AssertionError, match="left incomplete"):
        next(b)


# ---------------------------------------------------------------------------
# 1. keyset_pages is keyset_stream page by page
# ---------------------------------------------------------------------------


def _flattened(pages) -> list:
    return [row for page in pages for row in page]


_PLAIN = _shop_table("plain", [("id", "int"), ("v", "text")], ["id"])
_PAIR = _shop_table("pair", [("u", "int"), ("n", "int")], ["u", "n"])
_TRAILING = _shop_table(
    "prefs", [("user_id", "int"), ("channel", "enum('email','sms','push')")],
    ["user_id", "channel"],
)

_SHAPES = {
    "plain": (_PLAIN, [{"id": i, "v": "x"} for i in range(1, 12)], {}),
    "plain, a bounded range": (
        _PLAIN, [{"id": i, "v": "x"} for i in range(1, 30)], {"pk_lower": 4, "pk_upper": 21}),
    "composite": (_PAIR, [{"u": u, "n": n} for u in (1, 2, 3) for n in (5, 1, 9, 2)], {}),
    "enum walk": (
        _COVERING,
        [{"status": s, "id": i} for s, k in (("new", 7), ("active", 5), ("closed", 3))
         for i in range(1, k + 1)],
        {},
    ),
    "trailing enum": (
        _TRAILING,
        [{"user_id": u, "channel": c} for u in range(1, 5) for c in ("email", "sms", "push")],
        {},
    ),
}


@pytest.mark.parametrize("batch_size", [1, 2, 3, 5])
@pytest.mark.parametrize("shape", sorted(_SHAPES))
def test_keyset_stream_is_keyset_pages_flattened(shape, batch_size, caplog) -> None:
    table, rows, bounds = _SHAPES[shape]
    with caplog.at_level(logging.DEBUG, logger="dsql_migrator.core.exporter"):
        streamed_source = _MySqlModel(table, rows)
        streamed = list(keyset_stream(streamed_source, table, batch_size=batch_size, **bounds))
        streamed_lines = [r.getMessage() for r in caplog.records]
        caplog.clear()
        paged_source = _MySqlModel(table, rows)
        paged = _flattened(keyset_pages(paged_source, table, batch_size=batch_size, **bounds))
        paged_lines = [r.getMessage() for r in caplog.records]
    assert paged == streamed and streamed
    assert paged_source.executed == streamed_source.executed      # the same statements
    assert paged_lines == streamed_lines and streamed_lines         # the same DEBUG lines


@pytest.mark.parametrize("batch", [3, 7, 10, 50])
@pytest.mark.parametrize("key", [("f",), ("n", "f")])
def test_a_float_key_rereads_the_same_way_page_by_page(key, batch) -> None:
    from dsql_migrator.core.source_dialect import MySQLSourceDialect

    table = _tabledef("mysql", key)
    streamed_source = _FloatSource("mysql", key, _prices(key))
    streamed = [r["v"] for r in keyset_stream(
        streamed_source, table, batch_size=batch, dialect=MySQLSourceDialect())]
    paged_source = _FloatSource("mysql", key, _prices(key))
    paged = [r["v"] for r in _flattened(keyset_pages(
        paged_source, table, batch_size=batch, dialect=MySQLSourceDialect()))]
    assert paged == streamed and sorted(paged) == sorted(r["v"] for r in _prices(key))
    assert paged_source.executed == streamed_source.executed


def test_a_stopped_read_stops_page_by_page_with_the_same_error() -> None:
    # Members equal under the collation stop a walked key (G130), after the same statements.
    table = _shop_table("ci", [("k", "enum('a','A','b')"), ("id", "int")], ["k", "id"])
    rows = [{"k": k, "id": i} for k in ("a", "A", "b") for i in (1, 2, 3)]
    errors = []
    for read in (lambda s: list(itertools.islice(keyset_stream(s, table, batch_size=2), 100)),
                 lambda s: _flattened(keyset_pages(s, table, batch_size=2))):
        source = _MySqlModel(table, rows, ci=True)
        with pytest.raises(ExportError) as raised:
            read(source)
        errors.append((str(raised.value), source.executed))
    assert errors[0] == errors[1]


def test_keyset_pages_refuses_the_next_page_before_this_one_is_read() -> None:
    source = _MySqlModel(_PLAIN, [{"id": i, "v": "x"} for i in range(1, 12)])
    pages = keyset_pages(source, _PLAIN, batch_size=3)
    page = next(pages)
    next(page)                                   # one of its three rows
    with pytest.raises(RuntimeError, match="was not read to its end"):
        next(pages)
    assert len(source.pages) == 1                # the next page's statement was never sent
    # A page never read at all is no different.
    source = _MySqlModel(_PLAIN, [{"id": i, "v": "x"} for i in range(1, 12)])
    pages = keyset_pages(source, _PLAIN, batch_size=3)
    next(pages)
    with pytest.raises(RuntimeError, match="was not read to its end"):
        next(pages)
    # Read to its end, the next page is decided -- the short last page ends the read.
    source = _MySqlModel(_PLAIN, [{"id": i, "v": "x"} for i in range(1, 5)])
    pages = keyset_pages(source, _PLAIN, batch_size=3)
    assert [[r["id"] for r in page] for page in pages] == [[1, 2, 3], [4]]
    assert len(source.pages) == 2


# ---------------------------------------------------------------------------
# 2. TableExporter.stream_converted_ranges: one session, the ranges in turn
# ---------------------------------------------------------------------------


def test_one_session_reads_every_range_in_turn_from_one_snapshot() -> None:
    session = _SourceSession(_THREE)
    exporter, engine_ = _exporter(session)
    turns = _read_all(exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3))
    # A page of each live range in turn; a range leaves the turn once its read ends (range
    # 1's full page is followed by an empty one, as a keyset read learns it ended).
    assert turns == [
        (0, [1, 2, 3]), (1, [100, 101, 102]), (2, [200, 201, 202]),
        (0, [4, 5, 6]), (1, []), (2, [203, 204, 205]),
        (0, [7]), (2, [206, 207, 208]),
        (2, [209]),
    ]
    # Every row once, each range's rows in key order, and in the range it was read for.
    ids = [i for _index, page in turns for i in page]
    assert sorted(ids) == [r["id"] for r in _THREE] and len(set(ids)) == len(ids)
    for index in range(3):
        mine = [i for r, page in turns if r == index for i in page]
        assert mine == sorted(mine) and {_range_of(i) for i in mine} <= {index}
    # One session, one snapshot: the transaction's start first, its COMMIT last, once each.
    statements = [sql for sql, _params in session.executed]
    assert statements[0] == "START TRANSACTION WITH CONSISTENT SNAPSHOT"
    assert statements[-1] == "COMMIT"
    assert len(session.statements("START TRANSACTION")) == 1
    assert len(session.statements("COMMIT")) == 1
    assert engine_.connects == 1 and engine_.disposed == 1
    # Each page is bounded to its range on the leading key column.
    assert [(lo, hi) for _e, lo, hi in session.events][:3] == _RANGES


def test_the_pages_are_converted_as_one_reader_converts_them() -> None:
    table = _events(ColumnDef(name="flag", mysql_type="tinyint(1)"))
    rows = [dict(r, flag=r["id"] % 2) for r in _THREE]
    exporter, _engine = _exporter(_SourceSession(rows))
    got = {row["id"]: row["flag"] for _i, page in
           exporter.stream_converted_ranges(_MY, table, _RANGES, page_rows=4) for row in page}
    assert got == {r["id"]: bool(r["id"] % 2) for r in rows}          # 0/1 -> boolean


def test_asking_for_the_next_page_before_this_one_is_read_raises_and_ends_the_snapshot(
        monkeypatch) -> None:
    ends: list = []
    original = exporter_module._end_snapshot_after_failure
    monkeypatch.setattr(exporter_module, "_end_snapshot_after_failure",
                        lambda connection: ends.append(1) or original(connection))
    session = _SourceSession(_THREE)
    exporter, engine_ = _exporter(session)
    found: list = []
    stream = exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3,
                                              on_zero_dates=found.append)
    _index, page = next(stream)
    next(page)                                       # one row of three
    with pytest.raises(RuntimeError, match="was not read to its end"):
        next(stream)
    assert len(session.pages) == 1                   # no statement went out over the page
    assert ends == [1] and engine_.disposed == 1 and found == []


def test_a_close_ends_the_snapshot_once_and_reports_no_zero_dates(monkeypatch) -> None:
    ends: list = []
    original = exporter_module._end_snapshot_after_failure
    monkeypatch.setattr(exporter_module, "_end_snapshot_after_failure",
                        lambda connection: ends.append(1) or original(connection))
    table = _events(ColumnDef(name="d", mysql_type="date", nullable=True))
    session = _SourceSession([dict(r, d="0000-00-00") for r in _THREE])
    exporter, engine_ = _exporter(session)
    found: list = []
    stream = exporter.stream_converted_ranges(_MY, table, _RANGES, page_rows=3,
                                              on_zero_dates=found.append)
    for _turn in range(2):
        _index, page = next(stream)
        list(page)
    stream.close()
    assert ends == [1] and found == [] and engine_.disposed == 1
    assert session.statements("COMMIT") == ["COMMIT"]    # the page was read: it went out


def test_zero_dates_are_reported_once_after_the_commit_summed_over_the_ranges() -> None:
    table = _events(ColumnDef(name="d", mysql_type="date", nullable=True))
    rows = [dict(r, d="0000-00-00" if r["id"] % 2 else None) for r in _THREE]
    session = _SourceSession(rows)
    exporter, _engine = _exporter(session)
    reported: list = []

    def _on_zero_dates(entries) -> None:
        reported.append((entries, session.executed[-1][0]))

    turns = _read_all(exporter.stream_converted_ranges(
        _MY, table, _RANGES, page_rows=3, on_zero_dates=_on_zero_dates))
    assert sum(len(page) for _i, page in turns) == len(rows)
    ((entries, last_statement),) = reported
    assert last_statement == "COMMIT"                    # after the snapshot ended
    ((column, rule, _written_as, count),) = entries
    assert (column, rule, count) == ("d", ZERO_DATE, sum(1 for r in rows if r["id"] % 2))


def test_the_governor_and_the_cancel_are_asked_before_every_page(monkeypatch) -> None:
    session = _SourceSession(_THREE, threads_running=1)
    governors: set = set()
    original = SourceLoadGovernor.throttle

    def _throttle(self, should_cancel=None):
        governors.add(id(self))
        session.events.append("throttle")
        return original(self, should_cancel)

    monkeypatch.setattr(SourceLoadGovernor, "throttle", _throttle)
    exporter, _engine = _exporter(session, max_source_threads_running=50)
    polls: list = []

    def _should_cancel() -> bool:
        polls.append(len(session.pages))
        return False

    turns = _read_all(exporter.stream_converted_ranges(
        _MY, _events(), _RANGES, page_rows=3, should_cancel=_should_cancel))
    pages = [e for e in session.events if e != "throttle"]
    assert len(pages) == len(turns) == 9
    # Every page statement follows a throttle (between pages, never mid-page), from ONE
    # governor on the session's own connection.
    assert all(session.events[i - 1] == "throttle"
               for i, e in enumerate(session.events) if e != "throttle")
    assert session.events.count("throttle") == 9 and len(governors) == 1
    # The cancel is polled before each page (and again after its throttle).
    assert polls == [n for n in range(9) for _twice in (0, 1)]


def test_a_stop_between_pages_ends_the_read_before_the_next_page(monkeypatch) -> None:
    ends: list = []
    original = exporter_module._end_snapshot_after_failure
    monkeypatch.setattr(exporter_module, "_end_snapshot_after_failure",
                        lambda connection: ends.append(1) or original(connection))
    session = _SourceSession(_THREE)
    exporter, _engine = _exporter(session)
    stop = threading.Event()
    stream = exporter.stream_converted_ranges(
        _MY, _events(), _RANGES, page_rows=3, should_cancel=stop.is_set)
    for _turn in range(4):
        _index, page = next(stream)
        list(page)
    stop.set()
    with pytest.raises(ExportCancelled):
        next(stream)
    assert len(session.pages) == 4 and ends == [1]


def test_one_value_fit_guard_armed_once_checks_every_range(caplog) -> None:
    from dsql_migrator.core.converter import parse_target_value_specs

    table = TableDef(name="app.orders", columns=[
        ColumnDef(name="id", mysql_type="INT"), ColumnDef(name="amount", mysql_type="DECIMAL(12,4)")],
        primary_key=["id"])
    rows = [{"id": i, "amount": Decimal("1.2300")} for i in (1, 2, 101, 201)]
    rows.append({"id": 250, "amount": Decimal("1.2345")})     # range 2: would be rounded
    session = _SourceSession(rows, description=[
        ("id", 3, None, 11, 11, 0, False), ("amount", 246, None, 14, 14, 4, True)])
    exporter, _engine = _exporter(session)
    specs = parse_target_value_specs(
        'CREATE TABLE "orders" ("id" integer PRIMARY KEY, "amount" numeric(10,2))')
    stream = exporter.stream_converted_ranges(_MY, table, _RANGES, page_rows=2,
                                              target_specs=specs)
    loaded: list = []
    with caplog.at_level(logging.INFO, logger="dsql_migrator.core.exporter"):
        with pytest.raises(ValueConversionError, match="column 'amount'"):
            for _index, page in stream:
                for row in page:
                    loaded.append(row["id"])
    stream.close()
    assert caplog.text.count("Full Load value-fit check on") == 1   # armed once, for all
    assert loaded == [1, 2, 101, 201]


def test_a_shared_snapshot_is_adopted_right_after_the_start(monkeypatch) -> None:
    monkeypatch.setattr(exporter_module, "_reads_own_rows_only", lambda *_a: False)
    session = _SourceSession(_THREE)
    exporter, _engine = _exporter(session)
    table = TableDef(name="app.events", columns=[
        ColumnDef(name="id", mysql_type="bigint"), ColumnDef(name="v", mysql_type="text")],
        primary_key=["id"])
    turns = _read_all(exporter.stream_converted_ranges(
        _PG, table, _RANGES, page_rows=3, shared_snapshot_id="00000003-0000001B-1"))
    assert sum(len(page) for _i, page in turns) == len(_THREE)
    statements = [sql for sql, _params in session.executed]
    assert statements[:2] == ["START TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                              "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'"]
    assert len(session.statements("SET TRANSACTION")) == 1 and statements[-1] == "COMMIT"


def test_without_a_shared_snapshot_none_is_set() -> None:
    session = _SourceSession(_THREE)
    exporter, _engine = _exporter(session)
    _read_all(exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3))
    assert session.statements("SET TRANSACTION") == []


class _SenderSession(_SourceSession):
    """The session double, also recording who sends each statement (thread name, first word)
    -- before its own checks, whose AssertionError a best-effort COMMIT would swallow."""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.senders: list[tuple[str, str]] = []

    def execute(self, statement, parameters=None, execution_options=None):
        self.senders.append((threading.current_thread().name, str(statement).split()[0]))
        return super().execute(statement, parameters, execution_options)

    @property
    def threads(self) -> set:
        return {name for name, _word in self.senders}


def _hold_a_page(stream, *, rows_first=None):
    """A reader thread that takes the first page, reads ``rows_first`` of its rows (None: to
    its end), then waits for ``go`` before reading the rest of the stream. Returns (thread,
    go, the ids read)."""
    held, go = threading.Event(), threading.Event()
    read: list = []

    def _reader() -> None:
        _index, page = next(stream)
        read.extend(row["id"] for row in (
            page if rows_first is None else itertools.islice(page, rows_first)))
        held.set()
        go.wait(5)
        read.extend(row["id"] for row in page)
        for _index, rest in stream:
            read.extend(row["id"] for row in rest)

    thread = threading.Thread(target=_reader, name="reader")
    thread.start()
    assert held.wait(5)
    return thread, go, read


def test_a_close_from_another_thread_never_touches_the_session_a_page_is_read_on() -> None:
    # The engine's cleanup after a failed load can run while the read-ahead thread is still in
    # a page (its join is bounded): a COMMIT from there would drain that page under the reader
    # (PyMySQL), two threads on one connection. The close is refused instead, as a running
    # generator refuses one, and the reader reads on.
    session = _SenderSession(_THREE)
    exporter, engine_ = _exporter(session)
    stream = exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3)
    thread, go, read = _hold_a_page(stream, rows_first=1)
    with pytest.raises(ValueError, match="another thread is reading"):
        stream.close()
    go.set()
    thread.join(5)
    assert session.threads == {"reader"}                      # never the closing thread
    assert sorted(read) == [r["id"] for r in _THREE]          # the read went on, every row
    assert session.statements("COMMIT") == ["COMMIT"] and engine_.disposed == 1


def test_a_close_goes_through_once_no_page_is_held_or_from_the_reader_itself() -> None:
    session = _SenderSession(_THREE)
    exporter, engine_ = _exporter(session)
    stream = exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3)
    # The reader read its page to its end and asks for no other: nothing of the session is
    # in use, so another thread's close ends the snapshot.
    thread, go, read = _hold_a_page(stream)
    stream.close()
    # (The double refuses a second thread's statement as such; here it is sent in sequence.)
    assert session.senders[-1] == ("MainThread", "COMMIT") and engine_.disposed == 1
    go.set()
    thread.join(5)
    assert read == [1, 2, 3]                                  # the closed stream yields no more
    # From the reader's own thread a close goes through mid-page (it reads nothing meanwhile).
    session = _SenderSession(_THREE)
    exporter, engine_ = _exporter(session)
    stream = exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3)
    _index, page = next(stream)
    next(page)
    stream.close()
    # (PyMySQL drains the rest of the page before that COMMIT; the double refuses it as such.)
    assert session.senders[-1] == ("MainThread", "COMMIT") and engine_.disposed == 1


def test_a_close_while_the_reader_asks_for_a_page_is_refused(monkeypatch) -> None:
    # Asking for a page runs the governor and the page's SELECT on the session: a close from
    # another thread meanwhile is refused too (the governor here holds until released).
    session = _SenderSession(_THREE)
    exporter, _engine = _exporter(session, max_source_threads_running=50)
    inside, release = threading.Event(), threading.Event()

    def _throttle(self, should_cancel=None):
        inside.set()
        release.wait(5)

    monkeypatch.setattr(SourceLoadGovernor, "throttle", _throttle)
    stream = exporter.stream_converted_ranges(_MY, _events(), _RANGES, page_rows=3)
    turns: list = []
    thread = threading.Thread(target=lambda: turns.extend(_read_all(stream)), name="reader")
    thread.start()
    assert inside.wait(5)
    with pytest.raises(ValueError, match="another thread is reading"):
        stream.close()
    release.set()
    thread.join(5)
    assert sum(len(page) for _i, page in turns) == len(_THREE)
    assert session.threads == {"reader"}


# ---------------------------------------------------------------------------
# 3. The importer's range pages: per-range batches, one producer
# ---------------------------------------------------------------------------


class _Target:
    """An Aurora DSQL double for the real importer: every INSERT commits (after ``delay``).
    It keeps each batch's values (``batches``) unless ``keep_values`` is False, and each
    batch's parameter count (``sizes``)."""

    def __init__(self, delay: float = 0.0, *, keep_values: bool = True) -> None:
        self.delay = delay
        self.keep_values = keep_values
        self.batches: list[tuple] = []
        self.sizes: list[int] = []
        self.threads: set = set()
        self._lock = threading.Lock()

    def connect(self):
        target = self

        class _Cursor:
            rowcount = -1

            def execute(self, _statement, params=None):
                if target.delay:
                    time.sleep(target.delay)
                with target._lock:
                    if target.keep_values:
                        target.batches.append(tuple(params or ()))
                    target.sizes.append(len(params or ()))
                    target.threads.add(threading.current_thread().name)

            def close(self):
                return None

        class _Conn:
            def cursor(self):
                return _Cursor()

            def close(self):
                return None

        return _Conn()


def _importer(target: _Target, *, batch_size: int = 2, parallelism: int = 2) -> BatchedImporter:
    return BatchedImporter(
        BatchedImportOptions(batch_size=batch_size, parallelism=parallelism),
        connection_factory=target.connect, sleep=lambda _s: None, jitter=lambda: 0.0,
    )


_ID_ONLY = TableDef(name=EVENTS, columns=[ColumnDef(name="id", mysql_type="bigint")],
                    primary_key=["id"])
# Range r's pages of row ids: a page of 5 rows makes batches of 2, 2 and 1 -- no batch spans
# two pages, so none spans two ranges; an empty page (a keyset read's end) makes none.
_PAGES = {0: [[1, 2, 3, 4], [5, 6, 7, 8, 9], [10]],
          1: [[100, 101, 102]],
          2: [[200, 201], [202, 203, 204, 205], []]}
_EXPECTED = {
    f"{EVENTS}#s00-batch-000000": (0, 0, (1, 2)),
    f"{EVENTS}#s00-batch-000001": (0, 1, (3, 4)),
    f"{EVENTS}#s00-batch-000002": (0, 2, (5, 6)),
    f"{EVENTS}#s00-batch-000003": (0, 3, (7, 8)),
    f"{EVENTS}#s00-batch-000004": (0, 4, (9,)),
    f"{EVENTS}#s00-batch-000005": (0, 5, (10,)),
    f"{EVENTS}#s01-batch-000000": (1, 0, (100, 101)),
    f"{EVENTS}#s01-batch-000001": (1, 1, (102,)),
    f"{EVENTS}#s02-batch-000000": (2, 0, (200, 201)),
    f"{EVENTS}#s02-batch-000001": (2, 1, (202, 203)),
    f"{EVENTS}#s02-batch-000002": (2, 2, (204, 205)),
}


def _interleavings(pages: dict) -> list[list[tuple[int, list[int]]]]:
    """Every order of all the pages that keeps each range's own pages in order."""
    slots = [r for r, of_range in pages.items() for _page in of_range]
    orders = []
    for order in sorted(set(itertools.permutations(slots))):
        taken = {r: 0 for r in pages}
        turn = []
        for r in order:
            turn.append((r, pages[r][taken[r]]))
            taken[r] += 1
        orders.append(turn)
    return orders


def _as_pages(turn):
    return [(r, [{"id": i} for i in page]) for r, page in turn]


def test_batch_ids_are_per_range_under_any_interleaving() -> None:
    importer = _importer(_Target())
    orders = _interleavings(_PAGES)
    assert len(orders) == 140                     # 7! / (3! 1! 3!)
    for turn in orders:
        work = list(importer._iter_work_pages(
            _as_pages(turn), _ID_ONLY, ["id"], ["id"], {}, _SkipCounter(),
            OnConflictMode.NONE))
        got = {w.chunk_id: (w.shard_id, w.batch_index, tuple(r["id"] for r in w.rows))
               for w in work}
        assert len(got) == len(work) and got == _EXPECTED, turn


def test_a_resume_skips_per_range() -> None:
    importer = _importer(_Target())
    turn = _interleavings(_PAGES)[17]
    counter = _SkipCounter()
    work = list(importer._iter_work_pages(
        _as_pages(turn), _ID_ONLY, ["id"], ["id"], {"0": 2, "2": 0}, counter,
        OnConflictMode.SKIP_EXISTING))
    assert {w.chunk_id for w in work} == set(_EXPECTED) - {
        f"{EVENTS}#s00-batch-000000", f"{EVENTS}#s00-batch-000001",
        f"{EVENTS}#s00-batch-000002", f"{EVENTS}#s02-batch-000000"}
    assert counter.value == 4
    assert {w.on_conflict for w in work} == {OnConflictMode.SKIP_EXISTING}


def test_the_resume_point_is_per_range_whatever_the_interleaving() -> None:
    for turn in _interleavings(_PAGES)[::13]:
        target = _Target()
        job = MigrationJob(job_id="resume")
        result = _importer(target).import_rows(
            iter(()), _ID_ONLY, range_pages=iter(_as_pages(turn)), job=job,
            on_conflict=OnConflictMode.SKIP_EXISTING)
        assert result.rows_loaded == 19 and result.failures == 0
        assert job.resume_batch_watermark == {"0": 5, "1": 1, "2": 2}
        assert sorted(i for batch in target.batches for i in batch) == sorted(
            i for pages in _PAGES.values() for page in pages for i in page)


def test_one_producer_thread_reads_the_range_pages(monkeypatch) -> None:
    def _no_merge(*_a, **_k):
        raise AssertionError("one reader session's pages are never merged from threads")

    monkeypatch.setattr(BatchedImporter, "_prefetch_many", staticmethod(_no_merge))
    readers: list = []

    def _pages():
        for r, page in _as_pages(_interleavings(_PAGES)[0]):
            readers.append(threading.current_thread().name)
            yield r, page

    target = _Target()
    result = _importer(target).import_rows(iter(()), _ID_ONLY, range_pages=_pages())
    assert result.rows_loaded == 19 and set(readers) == {"fullload-prefetch"}
    # The prefetch seam off: the pages are read inline, on the caller's thread.
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_PREFETCH", "0")
    readers.clear()
    _importer(_Target()).import_rows(iter(()), _ID_ONLY, range_pages=_pages())
    assert set(readers) == {threading.current_thread().name}


def test_range_pages_and_shard_sources_do_not_combine() -> None:
    with pytest.raises(BatchedImportError, match="cannot be combined"):
        _importer(_Target()).import_rows(
            iter(()), _ID_ONLY, range_pages=iter(()), shard_sources=[iter([{"id": 1}])])


# ---------------------------------------------------------------------------
# 4. migrate_table(read_ranges=) and the whole-table worker
# ---------------------------------------------------------------------------


def _range_turns(rows: list, ranges: list, page_rows: int):
    """``rows`` in pages of ``page_rows`` per range, a page of each range in turn."""
    def _in(row, lo, hi):
        return (lo is None or row["id"] >= lo) and (hi is None or row["id"] < hi)

    per_range = []
    for lo, hi in ranges:
        mine = [r for r in rows if _in(r, lo, hi)]
        per_range.append([mine[i:i + page_rows] for i in range(0, len(mine), page_rows)] or [[]])
    for turn in itertools.zip_longest(*per_range):
        for index, page in enumerate(turn):
            if page is not None:
                yield index, iter([dict(r) for r in page])


class _RangesExporter:
    """The worker's exporter for a table read by one reader session: 3-row pages of ``rows``
    per range, in turn (whatever ``page_rows`` it is given, which it records with the rest
    of each call); ``lose_on`` (call number -> page number) raises a lost MySQL connection
    when that page is asked for."""

    def __init__(self, rows: list, *, lose_on=None) -> None:
        self.rows = rows
        self.lose_on = dict(lose_on or {})
        self.calls: list = []

    def stream_converted_ranges(self, conn, table, ranges, *, page_rows, **kwargs):
        self.calls.append(SimpleNamespace(table=table.name, columns=[c.name for c in table.columns],
                                          ranges=list(ranges), page_rows=page_rows, **kwargs))
        call = len(self.calls)

        def _pages():
            for number, turn in enumerate(_range_turns(self.rows, list(ranges), 3), start=1):
                if self.lose_on.get(call) == number:
                    raise _lost_connection(2013)
                yield turn

        return _pages()

    def stream_converted_rows(self, *_a, **_k):
        raise AssertionError("a table read in key ranges is read by one session, not this")


class _PagesImporter:
    """An importer double for range pages: each call's resume point when it started, the
    pages it read, and each page "committed" into the resume job as the real one does."""

    def __init__(self) -> None:
        self.calls: list = []
        self.caps: list = []

    def cap_parallelism(self, n: int) -> None:
        self.caps.append(n)

    def import_rows(self, rows, table, *, range_pages=None, shard_sources=None, job=None,
                    on_batch_loaded=None, **kwargs):
        assert list(rows) == [] and shard_sources is None and range_pages is not None
        call = SimpleNamespace(start=dict(job.resume_batch_watermark) if job else None,
                               pages=[], job=job, **kwargs)
        self.calls.append(call)
        for index, page in range_pages:
            ids = [row["id"] for row in page]
            call.pages.append((index, ids))
            if job is not None and ids:
                job.resume_batch_watermark = {**job.resume_batch_watermark,
                                              str(index): len(call.pages)}
            if on_batch_loaded is not None and ids:
                on_batch_loaded(len(ids), 0)
        loaded = sum(len(ids) for _i, ids in call.pages)
        return BatchedImportResult(rows_loaded=loaded, batches_completed=len(call.pages))


def _inputs(tables, *, replace=(), cdc=False, coexisting=False) -> DataMigrationInputs:
    return DataMigrationInputs(
        source_config=_MY, source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=list(tables)),
        is_cdc_migration=cdc, cdc_coexisting=coexisting, replace_tables=frozenset(replace),
    )


def _migrator(inputs, exporter, importer, **seams) -> BatchedTableMigrator:
    defaults = dict(
        table_recreator=lambda t: [], target_counter=lambda t: None,
        target_pk_reader=lambda t: list(t.primary_key),
        target_generated_reader=lambda t: set(),
    )
    defaults.update(seams)
    return BatchedTableMigrator(inputs, exporter=exporter, watermark_capturer=object(),
                                importer_factory=lambda _i: importer, **defaults)


def test_migrate_table_reads_its_ranges_through_one_session() -> None:
    table = _events()
    exporter, importer = _RangesExporter(_THREE), _PagesImporter()
    migrator = _migrator(_inputs([table]), exporter, importer)
    stats = LoadStats()
    progress: list = []
    result = migrator.migrate_table(table, read_ranges=_RANGES, load_stats=stats,
                                    on_rows=lambda loaded, skipped: progress.append(loaded))
    (call,) = exporter.calls
    assert call.ranges == _RANGES and call.columns == ["id", "v"]
    # Pages hold whole batches: 2 x 2,000 rows (Rows per batch) per page.
    assert call.page_rows == engine._range_page_rows(2000, 2) == 4000
    assert callable(call.on_zero_dates) and "target_specs" in vars(call)
    (load,) = importer.calls
    assert [index for index, _ids in load.pages] == [0, 1, 2, 0, 2, 0, 2, 2]
    assert result.rows_loaded == len(_THREE) and sum(progress) == len(_THREE)
    assert load.on_conflict is OnConflictMode.SKIP_EXISTING     # an append, as with one range
    # P3: the batches in flight of three key ranges (at most Batches per table).
    assert importer.caps == [engine._unit_in_flight(8, 3, None)] == [8]
    snapshot = stats.snapshot()
    assert snapshot["ranges"] == 3 and snapshot["in_flight_cap"] == 8


def test_a_page_holds_whole_batches_and_no_more_than_a_ranges_share_in_flight() -> None:
    f = engine._range_page_rows
    # Whole batches of the effective size, close to 5,000 rows (the specified examples).
    assert (f(2000, 2), f(3000, 2), f(2000, 100)) == (4000, 3000, 4585)
    assert f(1, 1) == 5000 and f(4999, 1) == 4999
    # At most a range's share of the batches in flight: 2 of 8 over 4 ranges, 1 of 8 over 8
    # ranges, and never below one batch.
    assert (f(2000, 2, 2), f(2000, 2, 1), f(2000, 2, 0)) == (4000, 2000, 2000)
    assert (f(2000, 100, 2), f(200, 2, 2), f(200, 2, 25)) == (1310, 400, 5000)


def test_the_batches_in_flight_span_every_range() -> None:
    # The reader turns to the next range after each page, so a window of the batches in flight
    # (8 here, over 4 ranges) holds batches of every range only when a page holds at most a
    # range's share of them; pages as large as 5,000 rows would put all 8 on one range.
    def _window_ranges(page_rows: int) -> int:
        rows = _rows((1, 81), (100, 180), (200, 280), (300, 380))      # 80 rows a range
        exporter, _engine = _exporter(_SourceSession(rows))
        importer = _importer(_Target(), batch_size=4, parallelism=8)
        pages = exporter.stream_converted_ranges(
            _MY, _ID_ONLY, [(None, 100), (100, 200), (200, 300), (300, None)],
            page_rows=page_rows)
        order = [w.shard_id for w in importer._iter_work_pages(
            pages, _ID_ONLY, ["id"], ["id"], {}, _SkipCounter(), OnConflictMode.NONE)]
        assert len(order) == 80
        return min(len(set(order[i:i + 8])) for i in range(0, 80 - 8 + 1))

    assert _window_ranges(engine._range_page_rows(4, 1, 8 // 4)) == 4
    assert _window_ranges(engine._range_page_rows(4, 1)) == 1      # 5,000 rows: a range a page


@pytest.mark.parametrize("ranges, cap, page_rows", [
    (8, None, 2000),          # 8 in flight over 8 ranges: one batch a page
    (4, None, 4000),          # 8 over 4 ranges: two (the default shape)
    (4, 16, 4000),            # 16 over 4 ranges: four, but 5,000 rows hold two batches
    (2, 1, 2000),             # fewer in flight than ranges: still one batch a page
])
def test_migrate_table_sizes_the_page_by_its_in_flight_share(ranges, cap, page_rows) -> None:
    table = _events()
    exporter, importer = _RangesExporter(_THREE), _PagesImporter()
    bounds = [(None, 1)] + [(i, i + 1) for i in range(1, ranges - 1)] + [(ranges - 1, None)]
    _migrator(_inputs([table]), exporter, importer).migrate_table(
        table, read_ranges=bounds, in_flight_cap=cap)
    assert exporter.calls[0].page_rows == page_rows


@pytest.mark.parametrize("ranges", [(), [(None, None)]])
def test_one_range_or_none_reads_as_before(ranges) -> None:
    from tests.test_ui_data_migration import _FakeExporter, _FakeImporter

    table = _events()
    exporter, importer = _FakeExporter({EVENTS: _THREE}), _FakeImporter()
    _migrator(_inputs([table]), exporter, importer).migrate_table(table, read_ranges=ranges)
    assert exporter.streamed == [EVENTS] and len(importer.received[0][1]) == len(_THREE)


def test_a_load_while_cdc_streams_keeps_its_own_readers() -> None:
    from tests.test_ui_data_migration import _FakeExporter, _FakeImporter

    table = _events()
    exporter, importer = _FakeExporter({EVENTS: _THREE}), _FakeImporter()
    _migrator(_inputs([table], coexisting=True), exporter, importer).migrate_table(
        table, read_ranges=_RANGES)
    assert exporter.streamed == [EVENTS]               # stream_converted_rows, not the turn


class _ClosedPages:
    """Range pages that record their close (the source session's release)."""

    def __init__(self, rows: list) -> None:
        self._turns = _range_turns(rows, _RANGES, 3)
        self.closed = 0

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._turns)

    def close(self) -> None:
        self.closed += 1


class _FailingPagesImporter(_PagesImporter):
    """Reads one page of the range pages, then fails with ``error``."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error

    def import_rows(self, rows, table, *, range_pages=None, **kwargs):
        _index, page = next(iter(range_pages))
        list(page)
        raise self.error


@pytest.mark.parametrize("error, raised", [
    (_lost_connection(2013), Exception),                 # a lost source connection: re-read
    (ExportCancelled(EVENTS), engine._FullLoadStopped),  # a Stop between pages
])
def test_a_failed_load_closes_its_range_pages_on_the_way_out(error, raised) -> None:
    # The source session is released as the failure leaves migrate_table (a caller then waits
    # out a failover before its re-read), not when the traceback is collected.
    table = _events()
    pages = _ClosedPages(_THREE)
    exporter = SimpleNamespace(stream_converted_ranges=lambda *_a, **_k: pages)
    migrator = _migrator(_inputs([table]), exporter, _FailingPagesImporter(error))
    with pytest.raises(raised):
        migrator.migrate_table(table, read_ranges=_RANGES)
    assert pages.closed == 1


def test_the_replace_count_check_and_the_promotion_are_unchanged() -> None:
    table = _events()
    # A Drop & reload whose target counts fewer rows than were loaded: silent loss, failed.
    exporter, importer = _RangesExporter(_THREE), _PagesImporter()
    short = _migrator(_inputs([table], replace=[EVENTS]), exporter, importer,
                      target_counter=lambda t: 3)
    with pytest.raises(RuntimeError, match="silent row loss detected"):
        short.migrate_table(table, read_ranges=_RANGES)
    assert importer.calls[0].on_conflict is OnConflictMode.NONE
    # A changed key on an empty target is promoted to a recreate, read in its ranges too.
    from dsql_migrator.core.converter import TableConversion

    recreated: list = []
    ddl = ('CREATE TABLE "app"."events" ("id" BIGINT NOT NULL, "v" VARCHAR(20), '
           'PRIMARY KEY ("v", "id"))')
    inputs = DataMigrationInputs(
        source_config=_MY, source_password=None,
        target_config=TargetConnectionConfig(cluster_endpoint="c.dsql.example",
                                             region="us-east-1"),
        inventory=SourceInventory(tables=[table]),
        table_conversions={EVENTS: TableConversion(table=EVENTS, target_ddl=ddl)},
    )
    exporter, importer = _RangesExporter(_THREE), _PagesImporter()
    counts = iter([0, len(_THREE)])                  # empty before; all of them after
    promoted = _migrator(inputs, exporter, importer, target_counter=lambda t: next(counts),
                         table_recreator=lambda t: recreated.append(t.name) or [])
    result = promoted.migrate_table(table, read_ranges=_RANGES)
    assert recreated == [EVENTS] and result.rows_loaded == len(_THREE)
    assert importer.calls[0].on_conflict is OnConflictMode.NONE
    assert importer.calls[0].key_columns == ["v", "id"]
    assert len(exporter.calls) == 1


class _Queue:
    def __init__(self) -> None:
        self.sent: list = []

    def put_nowait(self, message) -> None:
        self.sent.append(message)


def _run_worker(monkeypatch, exporter, importer, *, replace=(), pre_recreated=False,
                recreated=None):
    class _WorkerMigrator(BatchedTableMigrator):  # the real class, imported before any patch
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=exporter, watermark_capturer=object(),
                importer_factory=lambda _i: importer,
                table_recreator=lambda t: (recreated if recreated is not None else []).append(
                    t.name) or [],
                target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
            )

    queue = _Queue()
    monkeypatch.setattr(engine, "BatchedTableMigrator", _WorkerMigrator)
    monkeypatch.setattr(engine, "_worker_progress_queue", queue)
    monkeypatch.setattr(engine, "_worker_cancel_event", None)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS", "0")
    table = _events()
    result = engine._migrate_one_table_in_process(engine._TableWorkerArgs(
        job_id="j", table=table, inputs=_inputs([table], replace=replace),
        pre_recreated=pre_recreated, in_flight=8, read_ranges=tuple(_RANGES)))
    return result, queue


def test_a_lost_source_connection_re_reads_every_range_from_a_fresh_session(monkeypatch):
    # The read loses its source connection when its second page is asked for.
    exporter, importer = _RangesExporter(_THREE, lose_on={1: 2}), _PagesImporter()
    result, queue = _run_worker(monkeypatch, exporter, importer)
    assert result.status == "DONE" and result.rows_loaded == len(_THREE)
    # Two sessions, each reading every range: the re-read opened a new snapshot.
    assert [c.ranges for c in exporter.calls] == [_RANGES, _RANGES]
    assert [len(c.pages) for c in importer.calls] == [1, 8]
    # The first attempt "committed" range 0's first batch, but its ordinals belong to the
    # lost snapshot: the re-read starts from none (each range re-read from its start).
    assert [c.start for c in importer.calls] == [{}, {}]
    # One whole-table re-read, reported from a fresh snapshot -- never "range not re-read".
    rereads = [m for m in queue.sent if isinstance(m, tuple) and m[0] == engine._SOURCE_REREAD]
    assert len(rereads) == 1 and rereads[0][5] is None and rereads[0][6] is False
    assert result.load_stats["ranges"] == 3


def test_a_drop_and_reload_one_session_table_recreates_before_its_re_read(monkeypatch):
    exporter, importer = _RangesExporter(_THREE, lose_on={1: 2}), _PagesImporter()
    recreated: list = []
    result, _queue = _run_worker(monkeypatch, exporter, importer, replace=[EVENTS],
                                 pre_recreated=True, recreated=recreated)
    assert result.status == "DONE"
    # The parent recreated it before the first attempt; the re-read recreates it again, so
    # its plain INSERTs go into an empty table.
    assert recreated == [EVENTS]
    assert [c.on_conflict for c in importer.calls] == [OnConflictMode.NONE] * 2
    assert [c.job for c in importer.calls] == [None, None]    # no resume on a replace


# ---------------------------------------------------------------------------
# 5. The process path end to end: one unit, one session, the ranges in turn
# ---------------------------------------------------------------------------


def test_full_load_only_reads_an_explicitly_split_mysql_table_with_one_session(monkeypatch):
    sessions: list = []
    rows = _THREE

    class _PlannedExporter(TableExporter):
        """The real reader over the source double; the planning read answers the ranges."""

        def __init__(self) -> None:
            def _factory(_conf):
                sessions.append(_SourceSession(rows))
                return _Engine(sessions[-1])

            super().__init__(engine_factory=_factory)

        def plan_pk_shard_ranges(self, conn, table, shards, *, min_rows=0):
            assert shards == 4
            return list(_RANGES)

    target = _Target(delay=0.001)
    importers: list = []
    marks_while_loading: list = []

    def _importer_factory(_inputs):
        # The table's G137 record while its one session reads it: none (one snapshot).
        marks_while_loading.append([c.reader_shards for c in handle.job.chunks])
        importers.append(_importer(target, batch_size=2, parallelism=8))
        return importers[-1]

    class _RunMigrator(BatchedTableMigrator):
        def __init__(self, inputs, **_k):
            super().__init__(
                inputs, exporter=_PlannedExporter(), watermark_capturer=object(),
                importer_factory=_importer_factory, table_recreator=lambda t: [],
                target_counter=lambda t: None, target_pk_reader=lambda t: list(t.primary_key),
                target_generated_reader=lambda t: set(),
                target_unique_keys_reader=lambda t: pytest.fail("no unique-key guard runs"),
            )

    activity: list = []
    monkeypatch.setattr(engine, "BatchedTableMigrator", _RunMigrator)
    monkeypatch.setattr(engine, "ProcessPoolExecutor", _InlineExecutor)
    monkeypatch.setattr(engine, "_PROCESS_LAUNCH_STAGGER_SECONDS", 0)
    monkeypatch.setattr(engine, "log_activity", lambda cat, action, **kw: activity.append(
        (action, kw.get("target"), kw.get("detail"))))
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM", "2")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_READER_SHARDS", "4")
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SHARD_MIN_ROWS", "1000")
    _InlineExecutor.submitted = []
    table = _events()
    migrator = _RunMigrator(_inputs([table], cdc=False))
    handle = _Handle([EVENTS])
    counts = engine._migrate_tables_in_parallel(
        handle, "j", [table], migrator, ErrorLogStore(), watermark=_wm(), fresh_watermark=True)
    assert counts == (0, 0, 0)
    (chunk,) = handle.job.chunks
    assert chunk.status == "DONE" and chunk.rows_loaded == len(rows)
    assert chunk.reader_shards is None                      # one snapshot: no G137 record
    assert marks_while_loading == [[None]]
    assert _InlineExecutor.submitted == [EVENTS]            # one unit
    (session,) = sessions                                   # one source session
    assert len(session.statements("START TRANSACTION")) == 1
    assert [_range_of(p.get("pk_lower") or 0) for _s, p in session.pages] == [0, 1, 2]
    written = sorted(i for batch in target.batches for i in batch[::2])
    assert written == [r["id"] for r in rows]               # every row once
    assert ("reader shards", EVENTS, engine._SPLIT_ONE_SNAPSHOT.format(k=3)) in activity
    assert len(importers) == 1 and importers[0]._options.parallelism == 8


# ---------------------------------------------------------------------------
# 6. Memory: wide rows never pile up beyond the batches queued and in flight
# ---------------------------------------------------------------------------


class _Blob(bytes):
    """A 1 MiB value as the batcher sizes it (``len``) that is one byte in memory, counting
    the live ones."""

    live = 0
    peak = 0
    lock = threading.Lock()

    def __new__(cls):
        with cls.lock:
            cls.live += 1
            cls.peak = max(cls.peak, cls.live)
        return super().__new__(cls, b"x")

    def __len__(self) -> int:
        return 1 << 20

    def __del__(self) -> None:
        with _Blob.lock:
            _Blob.live -= 1


def test_wide_rows_never_pile_up_beyond_the_batches_queued_and_in_flight() -> None:
    _Blob.live = _Blob.peak = 0
    table = _events(ColumnDef(name="payload", mysql_type="longtext"))
    rows = _rows((1, 301), (100_000, 100_301), (200_000, 200_301))
    session = _SourceSession(rows, make_row=lambda r: {"id": r["id"], "v": r["v"],
                                                        "payload": _Blob()})
    exporter, _engine = _exporter(session)
    # The writes are the bottleneck, so the read-ahead queue fills; no value is kept.
    target = _Target(delay=0.002, keep_values=False)
    parallelism = 2
    importer = _importer(target, batch_size=2000, parallelism=parallelism)
    page_rows = 280                              # 40 batches of 7 wide rows per page
    result = importer.import_rows(iter(()), table, range_pages=exporter.stream_converted_ranges(
        _MY, table, [(None, 100_000), (100_000, 200_000), (200_000, None)],
        page_rows=page_rows))
    assert result.rows_loaded == len(rows) and result.failures == 0
    # 8 MiB per batch: 7 rows of 1 MiB (and their ids) to a batch.
    assert max(target.sizes) == 7 * 3
    # What may hold rows at once: the read-ahead queue, the batch its reader is putting and
    # the one it assembles, and the batches in flight (and the one being submitted) -- never
    # the page being drained, which the double hands over one row at a time. (On the real
    # stack SQLAlchemy's streaming result also buffers up to 1,000 of the page's rows, as for
    # every Full Load reader; that buffer is the same with one range or K.)
    depth = max(4, 2 * parallelism)
    bound = (depth + parallelism + 3) * 7
    assert _Blob.peak <= bound < page_rows, (_Blob.peak, bound)
    assert _Blob.live == 0
