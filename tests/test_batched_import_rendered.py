# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""R1: each bulk INSERT is rendered once to its final bytes and sent through psycopg's RawCursor.

No server anywhere. ``offline_connection`` is a real :class:`psycopg.Connection` over a libpq
connection whose connect attempt -- to a socket directory that does not exist -- has already
failed: enough for real cursors, the connection's adapters and libpq's own identifier
quoting, while nothing can ever be sent. What a cursor would send is read the way psycopg
builds it in ``execute``: a fresh ``Transformer`` of the cursor under the cursor's query class.

Pinned here:

* a RawCursor sends exactly the query bytes, parameter types, formats and adapted values a
  normal cursor made of the ``Composed`` statement, over a corpus of shapes and value types;
* a ``%`` in a table or column name (``pct%off``, ``a%sb``, ``x%%y``) loads as itself, on the
  INSERT and on the select-filter recovery's SELECT;
* a shape is rendered once and kept in bounded memory (the full-size slot plus a small LRU),
  and every retry of a batch sends the same bytes object;
* the loader's connections have psycopg's prepared statements off, while the shared connector
  leaves them on (Validation relies on them).
"""

from __future__ import annotations

import ipaddress
import threading
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from time import sleep

import psycopg
import pytest
from psycopg import adapt, pq, sql
from psycopg._queries import PostgresQuery, PostgresRawQuery
from psycopg.types.json import Json, Jsonb

from dsql_migrator.core import batched_import
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
    _ConnectionPool,
    _insert_cursor,
    _ParamSafeIdentifier,
    _RenderedInserts,
    build_insert_statement,
    render_insert_statement,
)
from dsql_migrator.core.models import ColumnDef, TableDef, TargetConnectionConfig
from dsql_migrator.core.occ import OCC_SQLSTATE
from dsql_migrator.core.target_connection import DsqlConnector
from tests.test_batched_import import (
    _FakeStore,
    _connection_factory,
    _no_sleep,
    _rows,
    _table,
    _zero_jitter,
)

_NOWHERE = b"host=/nonexistent-dsql-migrator-r1-test port=1"


@pytest.fixture()
def offline_connection():
    connection = psycopg.Connection(pq.PGconn.connect_start(_NOWHERE))
    try:
        yield connection
    finally:
        connection.close()


def _as_sent(cursor, query, params):
    """What ``cursor.execute(query, params)`` hands libpq: ``_start_query`` makes a fresh
    ``Transformer`` of the cursor and ``_convert_query`` runs the cursor's query class."""
    pgq = type(cursor)._query_cls(adapt.Transformer(cursor))
    pgq.convert(query, params)
    return pgq.query, list(pgq.params or ()), tuple(pgq.types), list(pgq.formats or ())


def _as_sent_without_connection(query_cls, query, params):
    pgq = query_cls(adapt.Transformer())
    pgq.convert(query, params)
    return pgq.query, list(pgq.params or ()), tuple(pgq.types), list(pgq.formats or ())


# Every kind of value the exporters hand the importer (MySQL's converter and PostgreSQL's
# pass-through: int, Decimal, float, str, bool, bytes, UUID, date, time, datetime, timedelta,
# lists for arrays, None), with the edges that change a parameter's type or format: integer
# size classes, NaN / infinity, aware and naive times, multi-byte and placeholder-like text.
_VALUES = (
    None, 0, 1, -1, 32767, 32768, -32769, 2**31 - 1, 2**31, 2**63 - 1, 2**63, -(2**63) - 1,
    10**30, True, False, 1.5, -0.0, float("inf"), float("nan"),
    Decimal("3.14"), Decimal("-0.000001"), Decimal("1E+30"), Decimal("NaN"),
    "", "plain", "ünï 日本語 🙂", "%s %(x)s %% $1 'q' \"d\" \\",
    b"", b"\x00\xffbytes", bytearray(b"ba"), memoryview(b"mv"),
    date(2026, 1, 2), time(3, 4, 5, 6), time(3, 4, tzinfo=timezone.utc),
    datetime(2026, 1, 2, 3, 4, 5, 123456), datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=9))),
    timedelta(days=-1, seconds=5, microseconds=7),
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
    [1, 2, None], [["a", "b"], ["c", None]], [],
    ipaddress.ip_address("10.0.0.1"), ipaddress.ip_network("10.0.0.0/8"),
    Jsonb({"a": [1, "x"]}), Json([1, 2]),
)


def _params(count: int, offset: int) -> list:
    return [_VALUES[(offset + index) % len(_VALUES)] for index in range(count)]


_TABLES = ("orders", "perf.order_items", 'od"d.ta ble', "스키마.テーブル", "Mixed.CaseName")
_COLUMN_SETS = (
    ("id",),
    ("id", "name"),
    ("id", "customer_id", "status", "total", "currency", "created_at", "updated_at", "note",
     "meta"),
    ("id", 'q"uote', "sp ace", "ünï", "back\\slash", "dot.ted", "select"),
    ("id", "ID", "CamelCase", "UPPER"),  # mixed case: quoted, so "id" and "ID" stay apart
)
_MODES = (
    (OnConflictMode.NONE, ("id",)),
    (OnConflictMode.DO_NOTHING, ("id",)),
    (OnConflictMode.DO_NOTHING, ()),
    (OnConflictMode.DO_UPDATE, ("id",)),
    (OnConflictMode.DO_UPDATE, None),  # every column a key: degrades to DO NOTHING
)
_ROW_COUNTS = (1, 2, 7)


def _shapes():
    for table in _TABLES:
        for columns in _COLUMN_SETS:
            for mode, keys in _MODES:
                for rows in _ROW_COUNTS:
                    yield table, list(columns), rows, mode, list(columns if keys is None else keys)


def test_the_cursor_classes_convert_as_modelled() -> None:
    # _as_sent models execute() through these: if psycopg moved them, the model would lie.
    assert psycopg.Cursor._query_cls is PostgresQuery
    assert psycopg.RawCursor._query_cls is PostgresRawQuery


def test_the_raw_path_sends_exactly_what_the_composed_path_sent(offline_connection) -> None:
    checked = 0
    for offset, (table, columns, rows, mode, keys) in enumerate(_shapes()):
        params = _params(rows * len(columns), offset)
        composed = build_insert_statement(table, columns, rows, mode, keys)
        rendered = render_insert_statement(table, columns, rows, mode, keys)
        raw_cursor = _insert_cursor(offline_connection)
        assert type(raw_cursor) is psycopg.RawCursor
        shape = (table, columns, rows, mode)
        # Through the connection: the names quoted by libpq, the connection's adapters.
        assert _as_sent(raw_cursor, rendered, params) == _as_sent(
            psycopg.Cursor(offline_connection), composed, params
        ), shape
        # And with no connection at all, where psycopg quotes names itself.
        assert _as_sent_without_connection(
            PostgresRawQuery, rendered, params
        ) == _as_sent_without_connection(PostgresQuery, composed, params), shape
        checked += 1
    assert checked == len(_TABLES) * len(_COLUMN_SETS) * len(_MODES) * len(_ROW_COUNTS)


@pytest.mark.parametrize("rows, width", [(2000, 9), (3000, 21)])
def test_full_size_batches_match_too_and_reuse_the_rendered_bytes(
    offline_connection, rows: int, width: int
) -> None:
    columns = ["id"] + [f"c{index}" for index in range(1, width)]
    params = _params(rows * width, 0)
    composed = build_insert_statement("perf.orders", columns, rows, OnConflictMode.NONE, ["id"])
    rendered = render_insert_statement("perf.orders", columns, rows, OnConflictMode.NONE, ["id"])
    raw_cursor = _insert_cursor(offline_connection)
    assert _as_sent(raw_cursor, rendered, params) == _as_sent(
        psycopg.Cursor(offline_connection), composed, params
    )
    assert rendered.as_bytes().count(b"$") == rows * width
    # What a RawCursor sends is the rendered bytes object itself: nothing is rebuilt.
    pgq = PostgresRawQuery(adapt.Transformer(raw_cursor))
    pgq.convert(rendered, params)
    assert pgq.query is rendered.as_bytes()


def test_a_normal_cursor_refuses_a_rendered_statement(offline_connection) -> None:
    # Run on the wrong cursor, a rendered statement fails loudly -- never a different one.
    rendered = render_insert_statement("s.t", ["id", "v"], 2, OnConflictMode.NONE, ["id"])
    with pytest.raises(psycopg.ProgrammingError, match="0 placeholders but 4 parameters"):
        _as_sent(psycopg.Cursor(offline_connection), rendered, [1, 2, 3, 4])


def test_render_rejects_what_build_rejects() -> None:
    with pytest.raises(ValueError):
        render_insert_statement("t", ["id"], 0, OnConflictMode.NONE, ["id"])
    with pytest.raises(ValueError):
        render_insert_statement("t", [], 1, OnConflictMode.NONE, [])


def test_only_a_psycopg_connection_gets_a_raw_cursor(offline_connection) -> None:
    assert type(_insert_cursor(offline_connection)) is psycopg.RawCursor

    class _Double:
        def cursor(self):
            return "the double's own cursor"

    assert _insert_cursor(_Double()) == "the double's own cursor"


# ---------------------------------------------------------------------------
# A '%' in a name
# ---------------------------------------------------------------------------


_PERCENT_NAMES = ("pct%off", "a%sb", "x%%y")


def test_a_percent_in_a_name_broke_the_composed_path(offline_connection) -> None:
    # Before R1: a normal cursor's %s scan read the quoted names too.
    cursor = psycopg.Cursor(offline_connection)
    for name in ("pct%off", "a%sb"):  # '%o' is no placeholder; '%s' is one too many
        with pytest.raises(psycopg.ProgrammingError):
            _as_sent(
                cursor,
                build_insert_statement("s.t", ["id", name], 2, OnConflictMode.NONE, ["id"]),
                [1, 2, 3, 4],
            )
    query = _as_sent(
        cursor, build_insert_statement("s.t", ["id", "x%%y"], 2, OnConflictMode.NONE, ["id"]),
        [1, 2, 3, 4],
    )[0]
    assert b'"x%y"' in query and b'"x%%y"' not in query  # silently another column


def _composed_path_without_its_scan_bug(cursor, table, columns, rows, mode, keys, params):
    """What the Composed path would send for these names if its %s scan left them alone:
    each '%' is swapped for a byte that neither the scan nor libpq's quoting treats
    specially (\\x01), the statement goes through ``cursor`` as before, and the '%' is
    swapped back -- today's statement, every name quoted as libpq quotes it."""
    def swap(name):
        return name.replace("%", "\x01")

    composed = build_insert_statement(
        swap(table), [swap(c) for c in columns], rows, mode, [swap(k) for k in keys]
    )
    query, *rest = _as_sent(cursor, composed, params)
    assert b"%" not in query  # so every '%' below is one a name put back
    return (query.replace(b"\x01", b"%"), *rest)


@pytest.mark.parametrize("name", _PERCENT_NAMES + ("%", "%(x)s", "100%", "%b%t"))
def test_a_percent_in_a_name_is_just_a_character_on_the_raw_path(
    offline_connection, name: str
) -> None:
    # Byte for byte, query + values + types + formats: the '%' as a schema, table, column
    # and conflict-key name, in every mode.
    shapes = (
        (f"{name}.t", ["id", "v"], ["id"]),
        (f"s.{name}", ["id", "v"], ["id"]),
        ("s.t", ["id", name], ["id"]),
        ("s.t", [name, "v"], [name]),
    )
    for table, columns, keys in shapes:
        for mode in (OnConflictMode.NONE, OnConflictMode.DO_NOTHING, OnConflictMode.DO_UPDATE):
            for rows in (1, 3):
                params = _params(rows * len(columns), rows)
                rendered = render_insert_statement(table, columns, rows, mode, keys)
                sent = _as_sent(_insert_cursor(offline_connection), rendered, params)
                assert sent == _composed_path_without_its_scan_bug(
                    psycopg.Cursor(offline_connection), table, columns, rows, mode, keys,
                    params,
                ), (table, columns, mode, rows)
                assert sql.Identifier(name).as_bytes(offline_connection) in sent[0]
                assert sent[0].count(b"$") == rows * len(columns)


def test_a_param_safe_identifier_reaches_the_server_as_itself() -> None:
    for name in _PERCENT_NAMES:
        statement = sql.SQL("SELECT {} FROM t WHERE k = {}").format(
            _ParamSafeIdentifier(name), sql.Placeholder()
        )
        sent = _as_sent_without_connection(PostgresQuery, statement, [1])[0]
        assert sent == b"SELECT " + sql.Identifier(name).as_bytes(None) + b" FROM t WHERE k = $1"
    # Its quoting is Identifier's own; only the '%' are doubled.
    assert _ParamSafeIdentifier("s", 'q"%').as_bytes(None) == b'"s"."q""%%"'


class _UniqueViolation(Exception):
    sqlstate = "23505"


class _WireTarget:
    """One table on a fake target, reached through cursors that convert each statement as
    psycopg's would before playing it: the RawCursor's query class for a rendered statement
    (what :func:`_insert_cursor` gives a real connection), a normal cursor's otherwise -- so
    a statement psycopg would refuse fails here too."""

    def __init__(self, columns, key) -> None:
        self.columns = list(columns)
        self.key_index = [self.columns.index(name) for name in key]
        self.rows: dict = {}
        self.sent: list = []  # (sent raw, query bytes), in order
        self.lock = threading.Lock()

    def connect(self) -> "_WireConnection":
        return _WireConnection(self)


class _WireConnection:
    def __init__(self, target: _WireTarget) -> None:
        self._target = target

    def cursor(self) -> "_WireCursor":
        return _WireCursor(self._target)

    def close(self) -> None:
        pass


class _WireCursor:
    def __init__(self, target: _WireTarget) -> None:
        self._target = target
        self.rowcount = -1
        self._result: list = []

    def execute(self, query, params=None) -> None:
        raw = isinstance(query, batched_import._RenderedStatement)
        pgq = (PostgresRawQuery if raw else PostgresQuery)(adapt.Transformer())
        pgq.convert(query, params)
        target, values = self._target, list(params or ())
        with target.lock:
            target.sent.append((raw, pgq.query))
            if pgq.query.startswith(b"SELECT"):
                width = len(target.key_index)
                keys = [tuple(values[i : i + width]) for i in range(0, len(values), width)]
                self._result = [key for key in keys if key in target.rows]
                return
            width = len(target.columns)
            rows = [tuple(values[i : i + width]) for i in range(0, len(values), width)]
            keys = [tuple(row[i] for i in target.key_index) for row in rows]
            if any(key in target.rows for key in keys):
                raise _UniqueViolation("duplicate key value violates unique constraint")
            target.rows.update(zip(keys, rows))
            self.rowcount = len(rows)

    def fetchall(self) -> list:
        return list(self._result)

    def close(self) -> None:
        pass


def test_a_table_and_columns_with_a_percent_load_end_to_end() -> None:
    columns, key = ["a%sb", "x%%y", "pct%off"], ["a%sb", "x%%y"]
    target = _WireTarget(columns, key)
    importer = BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=1),
        connection_factory=target.connect, occ_base_delay=0.0, sleep=_no_sleep,
        jitter=_zero_jitter,
    )
    table = TableDef(
        name="s.pct%off", columns=[ColumnDef(name=c, mysql_type="int") for c in columns],
        primary_key=key,
    )
    rows = [{"a%sb": i, "x%%y": i * 10, "pct%off": None} for i in range(5)]

    clean = importer.import_rows(iter(rows), table, on_conflict=OnConflictMode.NONE)
    assert (clean.rows_loaded, clean.failures) == (5, 0)
    assert [raw for raw, _query in target.sent] == [True, True, True]
    assert all(
        query.startswith(b'INSERT INTO "s"."pct%off" ("a%sb", "x%%y", "pct%off") VALUES ($1, ')
        for _raw, query in target.sent
    )

    # A reload over rows that are partly there: each optimistic INSERT hits 23505, the
    # select-filter's SELECT -- a normal cursor, with parameters -- reads the keys present,
    # and the missing row goes in. That SELECT failed psycopg's %s scan before.
    del target.rows[(3, 30)]
    target.sent.clear()
    again = importer.import_rows(iter(rows), table, on_conflict=OnConflictMode.SKIP_EXISTING)
    assert (again.rows_loaded, again.conflicts, again.failures) == (1, 4, 0)
    selects = [(raw, query) for raw, query in target.sent if query.startswith(b"SELECT")]
    assert len(selects) == 3 and not any(raw for raw, _query in selects)
    assert all(
        query.startswith(b'SELECT "a%sb", "x%%y" FROM "s"."pct%off" WHERE ("a%sb", "x%%y") IN')
        for _raw, query in selects
    )
    assert set(target.rows) == {(i, i * 10) for i in range(5)}


# ---------------------------------------------------------------------------
# Rendered once, bounded, and reused by every retry
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "mode",
    [OnConflictMode.NONE, OnConflictMode.DO_NOTHING, OnConflictMode.DO_UPDATE,
     OnConflictMode.SKIP_EXISTING],
)
def test_each_shape_is_rendered_once_and_every_retry_sends_the_same_bytes(mode) -> None:
    store = _FakeStore()
    store.insert_failures = [OCC_SQLSTATE, "XX000"]  # the first batch's first two attempts
    seen: list = []  # the statement object of every INSERT, in order
    base = _connection_factory(store, ["id", "name"], ["id"])

    def factory():
        connection = base()
        original = connection.handle_execute

        def handle_execute(query, params, cursor):
            seen.append(query)
            return original(query, params, cursor)

        connection.handle_execute = handle_execute
        return connection

    importer = BatchedImporter(
        BatchedImportOptions(batch_size=2, parallelism=1), connection_factory=factory,
        occ_base_delay=0.0, sleep=_no_sleep, jitter=_zero_jitter,
    )
    result = importer.import_rows(_rows(5), _table(), on_conflict=mode)

    assert (result.rows_loaded, result.failures) == (5, 0)
    # Three batches (2, 2 and 1 rows) and two retries: five INSERTs of two shapes.
    assert len(seen) == 5
    assert importer._inserts.rendered == 2
    assert seen[1] is seen[0] and seen[2] is seen[0]  # the retries resent the same object
    assert seen[3] is seen[0]  # so did the next full-size batch
    assert seen[4] is not seen[0]  # the partial last batch has its own
    assert all(statement.as_bytes() is statement.as_bytes() for statement in seen)


def test_the_cache_keeps_the_full_size_shape_and_bounds_the_rest() -> None:
    cache = _RenderedInserts(batch_size=4, lru_size=3)
    columns, keys, mode = ("id", "v"), ("id",), OnConflictMode.NONE
    full = cache.statement("s.t", columns, 4, mode, keys)
    partial = {rows: cache.statement("s.t", columns, rows, mode, keys) for rows in (1, 2, 3)}
    assert cache.rendered == 4
    # A fourth partial shape evicts the least recently used one, never the full-size shape.
    cache.statement("s.u", columns, 1, mode, keys)
    assert len(cache._lru) == 3
    assert cache.statement("s.t", columns, 4, mode, keys) is full
    assert cache.statement("s.t", columns, 3, mode, keys) is partial[3]
    assert cache.rendered == 5
    assert cache.statement("s.t", columns, 1, mode, keys) is not partial[1]  # rendered again
    assert cache.rendered == 6
    # However many partial shapes come, the LRU stays bounded and the full-size slot holds.
    for rows in (1, 2, 3):
        for table in ("a.x", "b.x", "c.x"):
            cache.statement(table, columns, rows, OnConflictMode.DO_NOTHING, keys)
    assert len(cache._lru) == 3
    assert cache.statement("s.t", columns, 4, mode, keys) is full
    # Another table's full-size shape takes the slot over.
    other = cache.statement("s.v", columns, 4, mode, keys)
    assert cache._full is not None and cache._full[1] is other


def test_full_size_is_the_batch_the_reader_cuts_for_the_table() -> None:
    # 30 columns: 65,535 // 30 = 2,184 rows per batch, not the configured 3,000.
    cache = _RenderedInserts(batch_size=3000)
    columns = tuple(f"c{index}" for index in range(30))
    statement = cache.statement("s.t", columns, 2184, OnConflictMode.NONE, ("c0",))
    assert cache._full is not None and cache._full[1] is statement
    assert not cache._lru


def test_writers_racing_for_a_new_shape_render_it_once(monkeypatch) -> None:
    render = batched_import.render_insert_statement

    def slow_render(*args, **kwargs):
        sleep(0.02)  # lets the other writers run into the same miss
        return render(*args, **kwargs)

    monkeypatch.setattr(batched_import, "render_insert_statement", slow_render)
    cache = _RenderedInserts(batch_size=500)
    barrier = threading.Barrier(8)
    got: list = []

    def writer() -> None:
        barrier.wait()
        got.append(cache.statement("s.t", ("id", "v"), 500, OnConflictMode.NONE, ("id",)))

    threads = [threading.Thread(target=writer) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert cache.rendered == 1
    assert len(got) == 8 and all(statement is got[0] for statement in got)


# ---------------------------------------------------------------------------
# Prepared statements: off on the loader's connections only
# ---------------------------------------------------------------------------


def test_the_loaders_connections_have_prepared_statements_off(offline_connection) -> None:
    replacement = psycopg.Connection(pq.PGconn.connect_start(_NOWHERE))
    try:
        assert offline_connection.prepare_threshold == 5  # psycopg's default
        opened = iter([offline_connection, replacement])
        pool = _ConnectionPool(lambda: next(opened), 1)
        with pytest.raises(RuntimeError):
            with pool.lease() as leased:
                assert leased is offline_connection
                assert leased.prepare_threshold is None
                raise RuntimeError("dropped while in use")
        with pool.lease() as leased:  # the reconnect gets the same treatment
            assert leased is replacement
            assert leased.prepare_threshold is None
    finally:
        replacement.close()


def test_the_shared_connector_keeps_psycopgs_default(offline_connection) -> None:
    # Validation reads through the same DsqlConnector and relies on prepared statements.
    connector = DsqlConnector(
        TargetConnectionConfig(cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"),
        token_generator=lambda host, region, expires: "token",
        connect_factory=lambda **kwargs: offline_connection,
        ipv4_resolver=lambda host: None,
    )
    assert connector.connect().prepare_threshold == 5
