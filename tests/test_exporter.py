# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the read-only PK keyset table exporter.

Covers (Requirement 5.1 / Property 1):

- keyset pagination issues ``pk > :last ORDER BY pk LIMIT :n`` and advances
  correctly across pages until exhaustion,
- streaming is lazy (pages are fetched on demand, not all at once),
- single- and composite-key keyset pagination advances correctly across pages,
  and a missing primary key raises a clear error,
- value conversion reuses the Schema Converter mapping (TINYINT(1) -> bool,
  DATETIME -> UTC, BLOB -> bytes),
- CSV output content (including bytea hex and boolean text) is correct,
- the source is read-only: only SELECT / transaction-control statements run,
- the engine path wires a consistent-snapshot autocommit streaming connection,
- the S3 sink uploads via an injected client (no real S3).
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone
from itertools import islice
from typing import Optional

import pytest

from dsql_migrator.core.exporter import (
    _quote_mysql_identifier,
    _quote_mysql_table,
    CsvRowWriter,
    ExportCancelled,
    ExportError,
    RowWriter,
    SourceLoadGovernor,
    TableExporter,
    UnsupportedPrimaryKeyError,
    ValueConversionError,
    ValueConverter,
    _read_threads_running,
    compute_pk_shard_ranges,
    export_rows,
    keyset_stream,
    shardable_leading_int_pk,
)
from dsql_migrator.core.introspector import is_write_or_ddl
from dsql_migrator.core.models import (
    ColumnDef,
    SourceConnectionConfig,
    SourceType,
    TableDef,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeMappings:
    """Mirrors SQLAlchemy's MappingResult: iterable + ``.first()``."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def __iter__(self):  # noqa: ANN204
        return iter(self._rows)

    def first(self):  # noqa: ANN201 - mirrors SQLAlchemy MappingResult.first()
        return self._rows[0] if self._rows else None


class _FakeResult:
    """Mirrors the slice of the SQLAlchemy result API the exporter uses."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def mappings(self):  # noqa: ANN201 - mirrors SQLAlchemy
        return _FakeMappings(self._rows)


class _StatusResult:
    """Mirrors a SHOW GLOBAL STATUS result: ``.first()`` -> (Variable_name, Value)."""

    def __init__(self, value: Optional[int]) -> None:
        self._value = value

    def first(self):  # noqa: ANN201 - mirrors SQLAlchemy Result.first()
        return None if self._value is None else ("Threads_running", str(self._value))


class _FakeConnection:
    """A fake connection that serves keyset pages from an in-memory dataset.

    It interprets the ``last`` / ``batch_size`` bind parameters exactly like a
    real keyset query would, records every executed statement (for read-only and
    SQL-shape assertions), and counts page queries (for laziness assertions).
    """

    def __init__(self, rows: list[dict], pk="id", threads_running=None) -> None:
        self._pk_cols = [pk] if isinstance(pk, str) else list(pk)
        self._rows = sorted(
            rows, key=lambda row: tuple(row[c] for c in self._pk_cols)
        )
        self._pk = self._pk_cols[0]
        self.executed: list[tuple[str, Optional[dict]]] = []
        self.page_queries = 0
        self.execution_options_seen: Optional[dict] = None
        # Scripted Threads_running values the source-load governor's SHOW GLOBAL
        # STATUS returns (consumed in order, last value repeats); None -> the metric
        # is unconfigured, so SHOW returns 0 (never throttles). SHOW GLOBAL STATUS is
        # NOT appended to ``executed`` so it doesn't shift the page-SQL-shape asserts.
        self._threads_running = (
            list(threads_running) if threads_running is not None else None
        )
        self._tr_index = 0
        self.status_reads = 0

    def execution_options(self, **kwargs):  # noqa: ANN201 - mirrors SQLAlchemy
        self.execution_options_seen = kwargs
        return self

    def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        upper = sql.strip().upper()
        if "THREADS_RUNNING" in upper:  # source-load governor's SHOW GLOBAL STATUS
            self.status_reads += 1
            if not self._threads_running:
                return _StatusResult(0)  # unconfigured -> never throttles
            value = self._threads_running[
                min(self._tr_index, len(self._threads_running) - 1)
            ]
            self._tr_index += 1
            return _StatusResult(value)
        self.executed.append((sql, parameters))
        if (
            upper.startswith("START TRANSACTION")
            or upper.startswith("COMMIT")
            or upper.startswith("SET TRANSACTION")  # incl. SET TRANSACTION SNAPSHOT
        ):
            return _FakeResult([])

        self.page_queries += 1
        params = parameters or {}
        limit = params.get("batch_size")
        # Honor the optional PK-range bound (reader sharding): [pk_lower, pk_upper).
        pk_lower = params.get("pk_lower")
        pk_upper = params.get("pk_upper")

        def _in_range(row) -> bool:  # noqa: ANN001
            v = row[self._pk]
            if pk_lower is not None and v < pk_lower:
                return False
            if pk_upper is not None and v >= pk_upper:
                return False
            return True

        if len(self._pk_cols) == 1:
            last = params.get("last")
            candidates = [
                row for row in self._rows
                if (last is None or row[self._pk] > last) and _in_range(row)
            ]
        else:
            if "last_0" in params:
                last_key = tuple(
                    params[f"last_{i}"] for i in range(len(self._pk_cols))
                )
                candidates = [
                    row
                    for row in self._rows
                    if tuple(row[c] for c in self._pk_cols) > last_key
                    and _in_range(row)  # leading-column shard band (composite too)
                ]
            else:
                candidates = [row for row in self._rows if _in_range(row)]
        page = candidates[:limit] if limit is not None else candidates
        return _FakeResult(page)


class _FakeEngine:
    """Minimal engine exposing ``connect()`` (context manager) and ``dispose()``."""

    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self.disposed = False

    def connect(self):  # noqa: ANN201 - mirrors SQLAlchemy
        connection = self._connection

        class _Ctx:
            def __enter__(self):  # noqa: ANN204
                return connection

            def __exit__(self, *_args):  # noqa: ANN204
                return False

        return _Ctx()

    def dispose(self) -> None:
        self.disposed = True


class _RecordingWriter(RowWriter):
    """Captures header and rows in order for streaming assertions."""

    def __init__(self) -> None:
        self.header: list[str] = []
        self.rows: list[dict] = []
        self.closed = False

    def write_header(self, columns: list[str]) -> None:
        self.header = list(columns)

    def write_row(self, row) -> None:  # noqa: ANN001
        self.rows.append(dict(row))

    def close(self) -> None:
        self.closed = True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_table() -> TableDef:
    return TableDef(
        name="customers",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="name", mysql_type="VARCHAR(100)"),
        ],
        primary_key=["id"],
    )


def _typed_table() -> TableDef:
    return TableDef(
        name="events",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="active", mysql_type="TINYINT(1)"),
            ColumnDef(name="created_at", mysql_type="DATETIME"),
            ColumnDef(name="payload", mysql_type="BLOB"),
        ],
        primary_key=["id"],
    )


def _source_config() -> SourceConnectionConfig:
    return SourceConnectionConfig(host="db.example.com", database="app")


# ---------------------------------------------------------------------------
# Keyset pagination
# ---------------------------------------------------------------------------


def test_keyset_stream_paginates_and_advances_until_exhausted() -> None:
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    connection = _FakeConnection(rows)

    yielded = list(keyset_stream(connection, _simple_table(), batch_size=2))

    assert [row["id"] for row in yielded] == [1, 2, 3, 4, 5]
    # 5 rows at 2 per page -> pages of 2, 2, 1 (the short page stops the loop).
    assert connection.page_queries == 3

    select_statements = [
        sql for sql, _ in connection.executed if sql.strip().upper().startswith("SELECT")
    ]
    # First page has no keyset predicate; later pages carry pk > :last.
    assert "WHERE" not in select_statements[0]
    assert all("ORDER BY" in sql and "LIMIT" in sql for sql in select_statements)
    assert all("`id` > :last" in sql for sql in select_statements[1:])

    # The advanced bind parameter equals the previous page's last primary key.
    page_params = [
        params for sql, params in connection.executed if "WHERE" in sql
    ]
    assert page_params[0]["last"] == 2
    assert page_params[1]["last"] == 4


def _interval_pk_table() -> TableDef:
    # An interval PRIMARY KEY: PostgresSourceDialect reads it via a same-name text
    # CAST (``CAST("dur" AS text) AS "dur"``), which is exactly the shape that made a
    # BARE ``ORDER BY "dur"`` resolve to the TEXT output alias (text order) while the
    # keyset WHERE boundary compared the native interval -> silent skip/dup.
    return TableDef(
        name="events",
        columns=[
            ColumnDef(name="dur", mysql_type="interval"),
            ColumnDef(name="label", mysql_type="text"),
        ],
        primary_key=["dur"],
    )


def test_keyset_order_by_is_native_table_qualified_for_text_cast_pk() -> None:
    # FIX 1: the keyset ORDER BY must reference the NATIVE input column (table-qualified
    # to the FROM item), NOT the same-name text-cast output alias, so it agrees with the
    # WHERE boundary. A bare ``ORDER BY "dur"`` would bind to the ``CAST(... AS text) AS
    # "dur"`` output (text order) while WHERE (``"dur" > :last``) compares native interval.
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    rows = [{"dur": i, "label": f"n{i}"} for i in range(1, 4)]
    connection = _FakeConnection(rows, pk="dur")

    list(keyset_stream(
        connection, _interval_pk_table(), batch_size=2, dialect=PostgresSourceDialect()
    ))

    selects = [
        sql for sql, _ in connection.executed if sql.strip().upper().startswith("SELECT")
    ]
    assert selects
    for sql in selects:
        # PK is read via the text cast (unchanged), ...
        assert 'CAST("dur" AS text) AS "dur"' in sql
        # ... but ORDER BY is table-qualified to the FROM item (native column), ...
        assert 'ORDER BY "events"."dur"' in sql
        # ... and the misleading BARE alias form is gone (the regression this locks).
        assert 'ORDER BY "dur"' not in sql
    # WHERE still compares the bare (native) column, matching the qualified ORDER BY.
    assert any('"dur" > :last' in sql for sql in selects)


def test_keyset_walk_over_interval_pk_returns_every_row_exactly_once() -> None:
    # FIX 1 (behavioral): a keyset walk over a text-cast (interval) PRIMARY KEY visits
    # every row EXACTLY once with no skip/dup across pages/resume. (The exact native-vs-
    # text ORDER BY divergence is proven live against PostgreSQL 16; here the fake serves
    # native-ordered pages, so this guards the streaming mechanics for a text-cast PK.)
    from datetime import timedelta

    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    # Durations whose native order differs from their text order.
    durations = [
        timedelta(minutes=1),
        timedelta(hours=25),
        timedelta(days=2),
        timedelta(days=10),
    ]
    rows = [{"dur": d, "label": f"n{i}"} for i, d in enumerate(durations)]
    connection = _FakeConnection(rows, pk="dur")

    yielded = list(keyset_stream(
        connection, _interval_pk_table(), batch_size=2, dialect=PostgresSourceDialect()
    ))

    got = [r["dur"] for r in yielded]
    assert got == sorted(durations)  # native order, ...
    assert len(got) == len(set(got)) == len(durations)  # ... every row exactly once.


def test_keyset_order_by_is_table_qualified_on_mysql_and_still_walks() -> None:
    # FIX 1: table-qualifying the ORDER BY is harmless on MySQL -- the emitted SQL uses
    # ``ORDER BY `customers`.`id``` (still the base column) and the walk is unchanged.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    connection = _FakeConnection(rows)

    yielded = list(keyset_stream(connection, _simple_table(), batch_size=2))

    assert [row["id"] for row in yielded] == [1, 2, 3, 4, 5]
    selects = [
        sql for sql, _ in connection.executed if sql.strip().upper().startswith("SELECT")
    ]
    assert all("ORDER BY `customers`.`id`" in sql for sql in selects)


def test_keyset_stream_logs_pk_range_per_page_at_debug(caplog) -> None:
    import logging

    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    connection = _FakeConnection(rows)
    with caplog.at_level(logging.DEBUG, logger="dsql_migrator.core.exporter"):
        list(keyset_stream(connection, _simple_table(), batch_size=2))

    page_logs = [r for r in caplog.records if "export keyset page" in r.getMessage()]
    # 5 rows at 2/page -> 3 pages -> exactly one DEBUG line per page (never per row).
    assert len(page_logs) == 3
    msgs = [r.getMessage() for r in page_logs]
    assert "range=[1..2]" in msgs[0] and "rows=2" in msgs[0]
    assert "range=[3..4]" in msgs[1]
    assert "range=[5..5]" in msgs[2] and "rows=1" in msgs[2]
    # PII guard: the per-page trace logs PK + counts only, never row VALUES.
    blob = " ".join(msgs)
    assert not any(f"n{i}" in blob for i in range(1, 6))


def test_keyset_stream_silent_when_debug_disabled(caplog) -> None:
    import logging

    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    connection = _FakeConnection(rows)
    with caplog.at_level(logging.INFO, logger="dsql_migrator.core.exporter"):
        list(keyset_stream(connection, _simple_table(), batch_size=2))
    # Off by default (INFO): no row-trace records, no cost on the hot path.
    assert [r for r in caplog.records if "export keyset page" in r.getMessage()] == []


def test_keyset_stream_is_lazy_and_does_not_fetch_all_pages() -> None:
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 7)]
    connection = _FakeConnection(rows)

    stream = keyset_stream(connection, _simple_table(), batch_size=2)
    first_three = list(islice(stream, 3))

    assert [row["id"] for row in first_three] == [1, 2, 3]
    # Producing 3 rows needs only the first two pages, never all six rows.
    assert connection.page_queries == 2


def test_keyset_stream_stops_when_cancelled_between_pages() -> None:
    # A cooperative stop must interrupt the row pull between pages, not only
    # between load batches: the stream raises ExportCancelled before the next
    # SELECT so a wedged/large table can be stopped promptly.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 7)]
    connection = _FakeConnection(rows)

    # Cancel only after the first page has been fetched.
    state = {"pages": 0}

    def should_cancel() -> bool:
        return state["pages"] >= 1

    out = []
    stream = keyset_stream(
        connection, _simple_table(), batch_size=2, should_cancel=should_cancel
    )
    with pytest.raises(ExportCancelled):
        for row in stream:
            out.append(row["id"])
            state["pages"] = connection.page_queries

    # The first page's rows were yielded; the second page was never fetched.
    assert out == [1, 2]
    assert connection.page_queries == 1


def test_keyset_stream_not_cancelled_runs_to_completion() -> None:
    # A should_cancel that never fires must not change the normal full read.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    connection = _FakeConnection(rows)
    out = list(
        keyset_stream(
            connection, _simple_table(), batch_size=2, should_cancel=lambda: False
        )
    )
    assert [r["id"] for r in out] == [1, 2, 3, 4, 5]


def test_keyset_stream_rejects_missing_primary_key() -> None:
    table = TableDef(
        name="no_pk",
        columns=[ColumnDef(name="value", mysql_type="INT")],
        primary_key=[],
    )
    with pytest.raises(UnsupportedPrimaryKeyError, match="no primary key"):
        list(keyset_stream(_FakeConnection([]), table))


# ---------------------------------------------------------------------------
# Source-load governor (opt-in proactive read throttle)
# ---------------------------------------------------------------------------


def test_read_threads_running_parses_and_fails_open() -> None:
    assert _read_threads_running(_FakeConnection([], threads_running=[42])) == 42
    # A NULL/absent status row -> None (fail-open), never raises.
    assert _read_threads_running(_FakeConnection([], threads_running=[None])) is None

    class _Boom:
        def execute(self, *_a, **_k):  # noqa: ANN002, ANN003, ANN201
            raise RuntimeError("status read failed")

    assert _read_threads_running(_Boom()) is None  # exception -> None (never raises)


def test_governor_disabled_is_a_noop() -> None:
    conn = _FakeConnection([], threads_running=[999])
    sleeps: list[float] = []
    gov = SourceLoadGovernor(conn, None, sleep=sleeps.append)
    assert gov.enabled is False
    gov.throttle()
    assert conn.status_reads == 0  # disabled -> never even reads the metric
    assert sleeps == []


def test_governor_under_ceiling_does_not_pause() -> None:
    conn = _FakeConnection([], threads_running=[5])
    sleeps: list[float] = []
    changes: list = []
    gov = SourceLoadGovernor(
        conn, 10, sleep=sleeps.append,
        on_state_change=lambda paused, running: changes.append((paused, running)),
    )
    gov.throttle()
    assert sleeps == [] and changes == []
    assert conn.status_reads == 1


def test_governor_pauses_over_ceiling_then_resumes() -> None:
    # Over the ceiling for two reads, then it recedes -> pause (sliced sleep) + resume.
    conn = _FakeConnection([], threads_running=[20, 20, 5])
    sleeps: list[float] = []
    changes: list = []
    gov = SourceLoadGovernor(
        conn, 10, sleep=sleeps.append, monotonic=lambda: 0.0, slice_seconds=1.0,
        on_state_change=lambda paused, running: changes.append((paused, running)),
    )
    gov.throttle()
    assert sleeps == [1.0, 1.0]                    # two slices while over the ceiling
    assert changes == [(True, 20), (False, 5)]     # one pause + one resume transition
    assert conn.status_reads == 3                  # re-reads each slice while paused


def test_governor_fail_open_never_pauses() -> None:
    # A NULL/failed status read must not stall the load (fail-open).
    conn = _FakeConnection([], threads_running=[None])
    sleeps: list[float] = []
    gov = SourceLoadGovernor(conn, 10, sleep=sleeps.append)
    gov.throttle()
    assert sleeps == []


def test_governor_pause_honors_should_cancel() -> None:
    # A Stop during a pause returns promptly so the caller can raise ExportCancelled.
    conn = _FakeConnection([], threads_running=[99])  # always over the ceiling
    sleeps: list[float] = []
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2  # False on the first check, True on the second

    gov = SourceLoadGovernor(conn, 10, sleep=sleeps.append, monotonic=lambda: 0.0)
    gov.throttle(should_cancel)
    assert len(sleeps) == 1  # one slice, then the cancel ended the wait (no hang)


def test_governor_caches_reading_within_ttl() -> None:
    # Two throttle() calls within the TTL read the metric ONCE (cached), so the extra
    # SHOW GLOBAL STATUS is negligible across many pages.
    conn = _FakeConnection([], threads_running=[5, 999])
    gov = SourceLoadGovernor(conn, 10, monotonic=lambda: 100.0, ttl_seconds=2.0)
    gov.throttle()
    gov.throttle()
    assert conn.status_reads == 1  # second call reused the cached value


def test_governor_uses_injected_metric_reader_not_a_hardcoded_query() -> None:
    # The governor reads its metric through the injected ``metric_reader`` (a PostgreSQL
    # source passes the dialect's pg_stat_activity reader), so it NEVER runs a hardcoded
    # MySQL ``SHOW GLOBAL STATUS`` on the connection. A connection whose execute() raises
    # proves the governor touches ONLY the reader -- the fix for a PG snapshot connection,
    # where a MySQL SHOW would be a syntax error that aborts the snapshot transaction.
    class _NoSqlConn:
        def execute(self, *_a, **_k):  # noqa: ANN002, ANN003, ANN202
            raise AssertionError("governor must not run SQL on the connection directly")

    seen = []

    def _reader(conn):  # noqa: ANN001, ANN202
        seen.append(conn)
        return 0  # at/below the ceiling -> no pause

    gov = SourceLoadGovernor(
        _NoSqlConn(), 5, sleep=lambda _s: None, metric_reader=_reader
    )
    gov.throttle()
    assert seen, "the injected metric_reader must be used"
    # Reaching here (no AssertionError) proves the governor never queried the connection.


def test_keyset_stream_throttles_before_each_page() -> None:
    # The governor is asked to throttle at the same between-pages point as the cancel
    # poll: exactly once before each page fetch.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 6)]
    conn = _FakeConnection(rows)

    class _SpyGovernor:
        def __init__(self) -> None:
            self.calls = 0

        def throttle(self, should_cancel=None) -> None:  # noqa: ANN001
            self.calls += 1

    spy = _SpyGovernor()
    out = list(keyset_stream(conn, _simple_table(), batch_size=2, governor=spy))
    assert [r["id"] for r in out] == [1, 2, 3, 4, 5]
    assert spy.calls == conn.page_queries  # throttled once before every page


def test_keyset_stream_cancel_after_throttle_raises() -> None:
    # If a Stop lands DURING the governor's pause, keyset_stream re-polls the cancel
    # after throttle returns and raises ExportCancelled (never fetches the page).
    conn = _FakeConnection([{"id": 1, "name": "n1"}])
    calls = {"n": 0}

    def should_cancel() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2  # False at the top poll, True right after throttle

    class _NoopGovernor:
        def throttle(self, should_cancel=None) -> None:  # noqa: ANN001
            return

    with pytest.raises(ExportCancelled):
        list(keyset_stream(
            conn, _simple_table(), batch_size=2,
            should_cancel=should_cancel, governor=_NoopGovernor(),
        ))
    assert conn.page_queries == 0  # cancelled before the first page was fetched


def test_show_global_status_passes_the_read_only_guard() -> None:
    # The governor's query MUST NOT be blocked by the source read-only guard, else it
    # would silently fail-open and never throttle.
    assert is_write_or_ddl("SHOW GLOBAL STATUS LIKE 'Threads_running'") is False


def test_table_exporter_wires_the_governor_when_ceiling_set() -> None:
    # A configured ceiling makes stream_converted_rows consult the source's
    # Threads_running between pages (governor wired end-to-end). Under the ceiling it
    # never pauses, so every row still streams.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 4)]
    conn = _FakeConnection(rows, threads_running=[5])  # under ceiling 10 -> no pause
    exporter = TableExporter(
        engine_factory=lambda _cfg: _FakeEngine(conn),
        max_source_threads_running=10,
    )
    out = list(exporter.stream_converted_rows(_source_config(), _simple_table()))
    assert [r["id"] for r in out] == [1, 2, 3]
    assert conn.status_reads >= 1  # the governor consulted the source


def test_table_exporter_no_governor_when_ceiling_unset() -> None:
    # Default (no ceiling) -> zero overhead: SHOW GLOBAL STATUS is never issued.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 4)]
    conn = _FakeConnection(rows, threads_running=[5])
    exporter = TableExporter(engine_factory=lambda _cfg: _FakeEngine(conn))
    out = list(exporter.stream_converted_rows(_source_config(), _simple_table()))
    assert [r["id"] for r in out] == [1, 2, 3]
    assert conn.status_reads == 0  # governor not built -> no status query at all


def test_table_exporter_no_governor_when_ceiling_zero() -> None:
    # 0 is the OFF sentinel -> no governor built, no status query (same as unset).
    rows = [{"id": 1, "name": "n1"}]
    conn = _FakeConnection(rows, threads_running=[5])
    exporter = TableExporter(
        engine_factory=lambda _cfg: _FakeEngine(conn),
        max_source_threads_running=0,
    )
    out = list(exporter.stream_converted_rows(_source_config(), _simple_table()))
    assert [r["id"] for r in out] == [1]
    assert conn.status_reads == 0


def _pg_source_config() -> SourceConnectionConfig:
    return SourceConnectionConfig(
        source_type=SourceType.POSTGRES, host="pg.example.com", port=5432, database="app"
    )


def test_stream_converted_rows_adopts_the_shared_snapshot() -> None:
    # A PG shard given shared_snapshot_id runs SET TRANSACTION SNAPSHOT as the FIRST statement
    # after the snapshot start (before any page read), so every shard reads one point-in-time
    # cut -- a consistent range-sharded read even for a REPLACE (no-CDC) load.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 4)]
    conn = _FakeConnection(rows)
    exporter = TableExporter(engine_factory=lambda _cfg: _FakeEngine(conn))
    out = list(exporter.stream_converted_rows(
        _pg_source_config(), _simple_table(), shared_snapshot_id="00000003-0000001B-1"
    ))
    assert [r["id"] for r in out] == [1, 2, 3]
    stmts = [s.strip().upper() for s, _ in conn.executed]
    assert any(
        "SET TRANSACTION SNAPSHOT '00000003-0000001B-1'" in s for s, _ in conn.executed
    )
    i_start = next(i for i, s in enumerate(stmts) if s.startswith("START TRANSACTION"))
    i_set = next(i for i, s in enumerate(stmts) if s.startswith("SET TRANSACTION SNAPSHOT"))
    i_read = next(i for i, s in enumerate(stmts) if s.startswith("SELECT"))
    assert i_start < i_set < i_read  # snapshot start -> adopt shared snapshot -> read


def test_stream_converted_rows_no_shared_snapshot_by_default() -> None:
    # Without shared_snapshot_id (default), NO SET TRANSACTION SNAPSHOT is emitted -- the
    # stream keeps its own snapshot (unchanged single-reader / MySQL behavior).
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 4)]
    conn = _FakeConnection(rows)
    exporter = TableExporter(engine_factory=lambda _cfg: _FakeEngine(conn))
    list(exporter.stream_converted_rows(_pg_source_config(), _simple_table()))
    assert not any("SNAPSHOT" in s.upper() for s, _ in conn.executed)


def test_stream_converted_rows_reports_throttle_transitions(monkeypatch) -> None:
    # stream_converted_rows forwards the governor's pause/resume to on_throttle (the
    # seam the Full Load engine wires to the progress caption). Patch the wall-clock
    # sleep so the pause doesn't actually wait.
    import dsql_migrator.core.exporter as exporter_mod

    monkeypatch.setattr(exporter_mod, "_wall_sleep", lambda _s: None)
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 3)]
    conn = _FakeConnection(rows, threads_running=[50, 1])  # over ceiling, then clear
    transitions: list = []
    exporter = TableExporter(
        engine_factory=lambda _cfg: _FakeEngine(conn),
        max_source_threads_running=10,
    )
    out = list(exporter.stream_converted_rows(
        _source_config(), _simple_table(),
        on_throttle=lambda paused, running: transitions.append((paused, running)),
    ))
    assert [r["id"] for r in out] == [1, 2]  # throttled, never dropped
    assert (True, 50) in transitions          # paused when over the ceiling
    assert any(not paused for paused, _ in transitions)  # and resumed when it cleared


# ---------------------------------------------------------------------------
# Reader range sharding
# ---------------------------------------------------------------------------


def test_shardable_leading_int_pk_accepts_single_and_composite_int_leading() -> None:
    # A single integer PK is shardable; the helper returns its column name.
    for mysql_type in ("INT", "BIGINT", "int(11)", "SMALLINT UNSIGNED", "MEDIUMINT"):
        table = TableDef(
            name="t",
            columns=[ColumnDef(name="id", mysql_type=mysql_type)],
            primary_key=["id"],
        )
        assert shardable_leading_int_pk(table) == "id", mysql_type
    # A COMPOSITE PK whose LEADING column is an integer shards on that leading column,
    # regardless of the trailing columns' types (they never enter the boundary math).
    composite = TableDef(
        name="c",
        columns=[
            ColumnDef(name="tenant_id", mysql_type="INT"),
            ColumnDef(name="created", mysql_type="DATETIME(6)"),
            ColumnDef(name="sku", mysql_type="VARCHAR(64)"),
        ],
        primary_key=["tenant_id", "created", "sku"],
    )
    assert shardable_leading_int_pk(composite) == "tenant_id"


def test_shardable_leading_int_pk_rejects_non_integer_leading_and_no_pk() -> None:
    # A composite PK whose LEADING column is NOT an integer -> single reader (the
    # leading string/decimal has no collation-free arithmetic split).
    non_int_leading = TableDef(
        name="c",
        columns=[
            ColumnDef(name="uid", mysql_type="VARCHAR(36)"),
            ColumnDef(name="seq", mysql_type="INT"),
        ],
        primary_key=["uid", "seq"],
    )
    string_pk = TableDef(
        name="s",
        columns=[ColumnDef(name="uid", mysql_type="VARCHAR(36)")],
        primary_key=["uid"],
    )
    decimal_pk = TableDef(
        name="d",
        columns=[ColumnDef(name="amount", mysql_type="DECIMAL(10,2)")],
        primary_key=["amount"],
    )
    no_pk = TableDef(name="n", columns=[ColumnDef(name="x", mysql_type="INT")])
    assert shardable_leading_int_pk(non_int_leading) is None
    assert shardable_leading_int_pk(string_pk) is None
    assert shardable_leading_int_pk(decimal_pk) is None
    assert shardable_leading_int_pk(no_pk) is None


def test_compute_pk_shard_ranges_splits_min_max_into_half_open_ranges() -> None:
    # MIN=1, MAX=1000, shards=4 -> 4 contiguous ranges; first lo=None, last hi=None
    # (open ends guarantee full coverage), interior boundaries evenly spaced.
    class _MinMaxConn:
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            assert "MIN(" in str(statement) and "MAX(" in str(statement)
            return _FakeResult([{"lo": 1, "hi": 1000}])

    ranges = compute_pk_shard_ranges(_MinMaxConn(), _simple_table(), 4)
    assert len(ranges) == 4
    assert ranges[0][0] is None            # open start
    assert ranges[-1][1] is None           # open end
    # contiguous: each range's hi == the next range's lo
    for (lo_a, hi_a), (lo_b, _hi_b) in zip(ranges, ranges[1:]):
        assert hi_a == lo_b
    # step = (1000-1+1)//4 = 250, so interior boundaries are 251, 501, 751
    assert ranges[0][1] == 251
    assert ranges[1] == (251, 501)
    assert ranges[2] == (501, 751)
    assert ranges[3][0] == 751


def test_compute_pk_shard_ranges_falls_back_for_small_or_empty_table() -> None:
    class _EmptyConn:
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            return _FakeResult([{"lo": None, "hi": None}])

    class _TinyConn:
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            return _FakeResult([{"lo": 1, "hi": 3}])  # span 3 <= shards 4

    assert compute_pk_shard_ranges(_EmptyConn(), _simple_table(), 4) == [(None, None)]
    assert compute_pk_shard_ranges(_TinyConn(), _simple_table(), 4) == [(None, None)]
    # shards<=1 short-circuits without querying.
    assert compute_pk_shard_ranges(_EmptyConn(), _simple_table(), 1) == [(None, None)]


# ---------------------------------------------------------------------------
# Identifier quoting -- the escaping that makes the interpolated SQL safe
# ---------------------------------------------------------------------------


def test_quoting_escapes_embedded_backticks_and_splits_qualified_names() -> None:
    # A table or column name CANNOT be a bind parameter (":col" would be a string
    # literal, not a column), so every statement here interpolates the name and the
    # quoting IS the protection. Static analysers flag those f-strings on sight, so
    # pin the escaping rather than leaving it to a reviewer's reading.
    assert _quote_mysql_identifier("id") == "`id`"
    assert _quote_mysql_identifier("a`b") == "`a``b`"      # backtick doubled, not dropped
    # Cluster-wide introspection yields "database.table"; each part is quoted
    # separately, else MySQL reads `db.tbl` as ONE name in the unset current database.
    assert _quote_mysql_table("db.tbl") == "`db`.`tbl`"
    assert _quote_mysql_table("plain") == "`plain`"
    # A dot inside an unqualified name is still one identifier, not a split point.
    assert _quote_mysql_table("db.tbl.extra") == "`db`.`tbl.extra`"


def test_shard_range_sql_keeps_a_hostile_identifier_inside_one_quoted_name() -> None:
    # The end-to-end property: even if a source object were named to look like a SQL
    # terminator, it reaches the statement only as a quoted identifier -- the backtick
    # is doubled, so it cannot close the quote and start a new clause. (Names come from
    # information_schema reflection, not free-text input; this is defence in depth.)
    hostile = TableDef(
        name="orders` WHERE 1=1 -- ",
        columns=[ColumnDef(name="id` DROP", mysql_type="INT")],
        primary_key=["id` DROP"],
    )
    seen: list[str] = []

    class _CapturingConn:
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            seen.append(str(statement))
            return _FakeResult([{"lo": 1, "hi": 1000}])

    compute_pk_shard_ranges(_CapturingConn(), hostile, 4)
    sql = seen[0]
    # The payload survives verbatim INSIDE the quoted identifier ...
    assert "`orders`` WHERE 1=1 -- `" in sql
    assert "`id`` DROP`" in sql
    # ... and never as an unescaped identifier terminator, which is what would let it
    # become a clause of its own. This is the assertion that fails if quoting is lost.
    assert "orders` WHERE" not in sql
    assert "id` DROP" not in sql


def _composite_table() -> TableDef:
    return TableDef(
        name="events",
        columns=[
            ColumnDef(name="tenant_id", mysql_type="INT"),
            ColumnDef(name="id", mysql_type="BIGINT"),
        ],
        primary_key=["tenant_id", "id"],
    )


def test_compute_pk_shard_ranges_shards_composite_when_leading_is_integer() -> None:
    # Composite (tenant_id INT, id BIGINT): MIN/MAX are read from the LEADING integer
    # column and split into K half-open ranges (first lo=None, last hi=None).
    class _MinMaxConn:
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            s = str(statement)
            assert "MIN(" in s and "`tenant_id`" in s  # LEADING column, not the trailing id
            return _FakeResult([{"lo": 1, "hi": 1000}])

    ranges = compute_pk_shard_ranges(_MinMaxConn(), _composite_table(), 4)
    assert len(ranges) == 4
    assert ranges[0][0] is None and ranges[-1][1] is None

    # A composite whose LEADING column is NON-integer still falls back to one reader.
    non_int_leading = TableDef(
        name="s",
        columns=[
            ColumnDef(name="uid", mysql_type="VARCHAR(36)"),
            ColumnDef(name="id", mysql_type="INT"),
        ],
        primary_key=["uid", "id"],
    )
    assert compute_pk_shard_ranges(_FakeConnection([]), non_int_leading, 4) == [
        (None, None)
    ]


def test_keyset_stream_honors_pk_range_bounds() -> None:
    # 10 rows, id 1..10; shard [4, 8) must yield exactly ids 4,5,6,7.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 11)]
    connection = _FakeConnection(rows)
    out = list(
        keyset_stream(
            connection, _simple_table(), batch_size=100, pk_lower=4, pk_upper=8
        )
    )
    assert [r["id"] for r in out] == [4, 5, 6, 7]


def test_keyset_stream_pk_range_open_ends_cover_head_and_tail() -> None:
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 11)]
    # First shard: (None, 4) -> ids < 4.  Last shard: (8, None) -> ids >= 8.
    head = list(keyset_stream(_FakeConnection(rows), _simple_table(),
                              batch_size=100, pk_lower=None, pk_upper=4))
    tail = list(keyset_stream(_FakeConnection(rows), _simple_table(),
                              batch_size=100, pk_lower=8, pk_upper=None))
    assert [r["id"] for r in head] == [1, 2, 3]
    assert [r["id"] for r in tail] == [8, 9, 10]


def test_pk_range_shards_partition_all_rows_without_overlap() -> None:
    # The union of all shards must equal the whole table, with no row in two shards.
    rows = [{"id": i, "name": f"n{i}"} for i in range(1, 21)]

    class _MinMaxConn(_FakeConnection):
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            if "MIN(" in str(statement):
                return _FakeResult([{"lo": 1, "hi": 20}])
            return super().execute(statement, parameters)

    ranges = compute_pk_shard_ranges(_MinMaxConn(rows), _simple_table(), 3)
    seen: list[int] = []
    for lo, hi in ranges:
        got = [
            r["id"]
            for r in keyset_stream(
                _FakeConnection(rows), _simple_table(),
                batch_size=100, pk_lower=lo, pk_upper=hi,
            )
        ]
        seen.extend(got)
    assert sorted(seen) == list(range(1, 21))   # full coverage
    assert len(seen) == len(set(seen))           # no overlap


def test_keyset_stream_composite_key_honors_leading_pk_range_bound() -> None:
    # Composite (tenant_id, id): a shard band on the LEADING column [2, 4) yields only
    # tenant_id 2 and 3, correctly paginated across pages, while the 5.7-safe
    # disjunction cursor still drives the within-shard ordering.
    rows = [{"tenant_id": t, "id": i} for t in range(1, 6) for i in range(1, 4)]
    connection = _FakeConnection(rows, pk=("tenant_id", "id"))
    out = list(
        keyset_stream(
            connection, _composite_table(),
            batch_size=2, pk_lower=2, pk_upper=4,  # tiny batch forces multi-page
        )
    )
    assert [(r["tenant_id"], r["id"]) for r in out] == [
        (2, 1), (2, 2), (2, 3), (3, 1), (3, 2), (3, 3),
    ]
    # SQL shape: the leading-column band AND the explicit disjunction cursor, and NOT a
    # row-value tuple comparison (which would lose the 5.7 PK-index range scan).
    next_page = next(
        s for s, _p in connection.executed if "SELECT" in s and ":last_0" in s
    )
    assert "`tenant_id` >= :pk_lower" in next_page
    assert "`tenant_id` < :pk_upper" in next_page
    assert "`tenant_id` > :last_0" in next_page       # disjunction term ...
    assert "(`tenant_id`, `id`)" not in next_page      # ... never the row-value form


def test_pk_range_shards_partition_composite_leading_int_without_overlap() -> None:
    # Composite (tenant_id, id): K shards banded on the LEADING column must union to the
    # whole table with no overlap, and every row of one tenant co-locates in one shard.
    rows = [{"tenant_id": t, "id": i} for t in range(1, 13) for i in range(1, 4)]

    class _MinMaxConn(_FakeConnection):
        def execute(self, statement, parameters=None, execution_options=None):  # noqa: ANN001, ANN201
            if "MIN(" in str(statement):
                return _FakeResult([{"lo": 1, "hi": 12}])
            return super().execute(statement, parameters)

    ranges = compute_pk_shard_ranges(
        _MinMaxConn(rows, pk=("tenant_id", "id")), _composite_table(), 3
    )
    assert len(ranges) == 3
    seen: list[tuple] = []
    shard_of_tenant: dict[int, int] = {}
    for shard_idx, (lo, hi) in enumerate(ranges):
        got = [
            (r["tenant_id"], r["id"])
            for r in keyset_stream(
                _FakeConnection(rows, pk=("tenant_id", "id")), _composite_table(),
                batch_size=100, pk_lower=lo, pk_upper=hi,
            )
        ]
        seen.extend(got)
        for tenant, _id in got:
            # every row of a given tenant lands in exactly one shard
            assert shard_of_tenant.setdefault(tenant, shard_idx) == shard_idx
    all_rows = [(r["tenant_id"], r["id"]) for r in rows]
    assert sorted(seen) == sorted(all_rows)  # full coverage
    assert len(seen) == len(set(seen))       # no overlap


def test_keyset_stream_supports_composite_primary_key() -> None:
    table = TableDef(
        name="composite",
        columns=[
            ColumnDef(name="tenant_id", mysql_type="INT"),
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="name", mysql_type="VARCHAR(10)"),
        ],
        primary_key=["tenant_id", "id"],
    )
    rows = [
        {"tenant_id": 1, "id": 2, "name": "a"},
        {"tenant_id": 1, "id": 5, "name": "b"},
        {"tenant_id": 2, "id": 1, "name": "c"},
    ]
    connection = _FakeConnection(rows, pk=["tenant_id", "id"])
    out = list(keyset_stream(connection, table, batch_size=2))
    # Lexicographic keyset order across the composite key, all rows once.
    assert [(r["tenant_id"], r["id"]) for r in out] == [(1, 2), (1, 5), (2, 1)]
    selects = [
        sql for sql, _ in connection.executed if sql.strip().upper().startswith("SELECT")
    ]
    assert "WHERE" not in selects[0]
    # Later pages use the index-friendly lexicographic keyset EXPANSION -- not the row-value
    # tuple form, which only uses the PK index on MySQL 8.0.14+ (a 5.7-compatible source would
    # full-scan per page).
    assert any(
        "`tenant_id` > :last_0" in sql
        and "`tenant_id` = :last_0 AND `id` > :last_1" in sql
        for sql in selects[1:]
    )
    assert not any("(`tenant_id`, `id`) >" in sql for sql in selects)  # no row-value form
    # The advanced bind params carry the previous page's last composite key.
    page_params = [params for sql, params in connection.executed if "WHERE" in sql]
    assert page_params[0]["last_0"] == 1 and page_params[0]["last_1"] == 5


def test_keyset_stream_rejects_non_positive_batch_size() -> None:
    with pytest.raises(ValueError, match="positive"):
        list(keyset_stream(_FakeConnection([]), _simple_table(), batch_size=0))


# ---------------------------------------------------------------------------
# Value conversion (reuses the Schema Converter mapping)
# ---------------------------------------------------------------------------


def test_value_converter_maps_tinyint_one_to_boolean() -> None:
    converter = ValueConverter(_typed_table())
    assert converter.convert_value("active", 1) is True
    assert converter.convert_value("active", 0) is False
    assert converter.convert_value("active", None) is None


def test_value_converter_rejects_tinyint_value_outside_0_1() -> None:
    # A TINYINT(1) column maps to DSQL boolean, but MySQL's (1) is display width:
    # the column can legally hold 2, -1, 127, ... bool(int(value)) would flatten
    # any non-zero to True and silently lose the magnitude, so the converter must
    # fail loudly (naming the column + value) rather than corrupt the row.
    converter = ValueConverter(_typed_table())
    for bad in (2, -1, 127):
        with pytest.raises(ValueConversionError) as excinfo:
            converter.convert_value("active", bad)
        message = str(excinfo.value)
        assert "active" in message
        assert str(bad) in message


def test_value_converter_target_type_override_loads_tinyint_as_smallint() -> None:
    # #1: when the APPLIED target type is smallint (user remapped TINYINT(1) in
    # Schema Conversion), value conversion follows the TARGET type -- a non-0/1
    # value loads as the integer instead of failing the boolean conversion.
    converter = ValueConverter(_typed_table(), target_types={"active": "smallint"})
    assert converter.convert_value("active", 2) == 2
    assert converter.convert_value("active", 0) == 0
    assert converter.convert_value("active", None) is None


def test_value_converter_override_does_not_affect_other_columns() -> None:
    # An override for one column leaves others source-derived: a TINYINT(1) with
    # no override still converts to boolean.
    converter = ValueConverter(_typed_table(), target_types={"id": "bigint"})
    assert converter.convert_value("active", 1) is True


def test_value_conversion_error_guides_schema_conversion_remap() -> None:
    # #2: the failure guides the user to remap the target type in Schema Conversion
    # (now effective), naming smallint/integer.
    converter = ValueConverter(_typed_table())
    with pytest.raises(ValueConversionError) as excinfo:
        converter.convert_value("active", 2)
    message = str(excinfo.value)
    assert "Schema Conversion" in message
    assert "smallint" in message


def test_value_converter_datetime_to_naive_utc() -> None:
    # MySQL DATETIME -> DSQL timestamp WITHOUT TIME ZONE. The value is normalized to
    # UTC and returned NAIVE (tzinfo dropped) so binding is independent of the DSQL
    # session TimeZone (a tz-aware value would be shifted to the session zone).
    converter = ValueConverter(_typed_table())

    naive = datetime(2024, 1, 2, 3, 4, 5)
    converted = converter.convert_value("created_at", naive)
    assert converted == datetime(2024, 1, 2, 3, 4, 5)
    assert converted.tzinfo is None

    aware = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone(_hours(9)))
    converted_aware = converter.convert_value("created_at", aware)
    assert converted_aware == datetime(2024, 1, 2, 3, 0, 0)
    assert converted_aware.tzinfo is None


def test_value_converter_timestamp_stays_tz_aware_utc() -> None:
    # MySQL TIMESTAMP -> DSQL timestamptz keeps the tz-aware UTC instant.
    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="ts", mysql_type="TIMESTAMP"),
        ],
        primary_key=["id"],
    )
    converter = ValueConverter(table)
    aware = datetime(2024, 1, 2, 12, 0, 0, tzinfo=timezone(_hours(9)))
    converted = converter.convert_value("ts", aware)
    assert converted == datetime(2024, 1, 2, 3, 0, 0, tzinfo=timezone.utc)
    assert converted.tzinfo is timezone.utc


def test_value_converter_keeps_blob_as_bytes() -> None:
    converter = ValueConverter(_typed_table())
    payload = b"\x00\x01\xff"
    assert converter.convert_value("payload", payload) == payload


def test_value_converter_decodes_bit_bytes_to_int() -> None:
    # MySQL BIT(n) maps to an integer target in DSQL (bit type unsupported); the
    # driver returns big-endian bytes which must be decoded to the unsigned int.
    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="flags", mysql_type="BIT(8)"),
        ],
        primary_key=["id"],
    )
    converter = ValueConverter(table)
    assert converter.convert_value("flags", b"\xdb") == 219
    assert converter.convert_value("flags", b"M") == 77
    assert converter.convert_value("flags", None) is None
    assert converter.convert_value("flags", 5) == 5  # already-int passthrough


def test_value_converter_time_timedelta_to_time() -> None:
    # MySQL TIME arrives as a timedelta from the driver; an in-range (0..24h) value
    # must become a datetime.time so it binds to a DSQL ``time`` column (a timedelta
    # would bind to interval and fail the row).
    from datetime import time as _time, timedelta as _td

    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="t_of_day", mysql_type="TIME"),
        ],
        primary_key=["id"],
    )
    converter = ValueConverter(table)
    # In-range still converts:
    assert converter.convert_value("t_of_day", _td(seconds=19479)) == _time(5, 24, 39)
    assert converter.convert_value("t_of_day", None) is None
    # Out of range (negative / >24h) now raises loudly instead of passing through.
    with pytest.raises(ValueConversionError):
        converter.convert_value("t_of_day", _td(hours=30))


def test_value_converter_rejects_out_of_range_time() -> None:
    # MySQL TIME's full range is -838:59:59..838:59:59; a value outside [0, 24h)
    # has no DSQL ``time`` representation. Passing the raw timedelta through would
    # silently bind to interval (or emit a non-time cell), corrupting the column,
    # so the converter must fail loudly (naming the column + value) -- the silent-
    # corruption sibling of the TINYINT(1)-out-of-range guard.
    from datetime import timedelta as _td

    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="t_of_day", mysql_type="TIME"),
        ],
        primary_key=["id"],
    )
    converter = ValueConverter(table)
    for bad in (_td(hours=30), _td(seconds=-1), _td(hours=838, minutes=59, seconds=59)):
        with pytest.raises(ValueConversionError) as excinfo:
            converter.convert_value("t_of_day", bad)
        message = str(excinfo.value)
        assert "t_of_day" in message
        assert "time" in message


def test_value_converter_time_boundary_values_convert() -> None:
    # The bound is [0, 24h): the lower edge (00:00:00) and the value just under 24h
    # (23:59:59.999999) both still convert to datetime.time, confirming the
    # ``< 86400`` bound and microsecond handling.
    from datetime import time as _time, timedelta as _td

    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="t_of_day", mysql_type="TIME"),
        ],
        primary_key=["id"],
    )
    converter = ValueConverter(table)
    assert converter.convert_value("t_of_day", _td(0)) == _time(0, 0, 0)
    assert converter.convert_value(
        "t_of_day", _td(hours=23, minutes=59, seconds=59, microseconds=999999)
    ) == _time(23, 59, 59, 999999)


def test_value_converter_passes_through_plain_types() -> None:
    converter = ValueConverter(_simple_table())
    assert converter.convert_value("id", 7) == 7
    assert converter.convert_value("name", "alice") == "alice"


def _hours(value: int):  # noqa: ANN202 - tiny helper for tz offsets
    from datetime import timedelta

    return timedelta(hours=value)


# ---------------------------------------------------------------------------
# export_rows: streaming, conversion, CSV output, read-only
# ---------------------------------------------------------------------------


def _typed_rows() -> list[dict]:
    return [
        {
            "id": 1,
            "active": 1,
            "created_at": datetime(2024, 1, 1, 0, 0, 0),
            "payload": b"\x01\x02",
        },
        {
            "id": 2,
            "active": 0,
            "created_at": datetime(2024, 6, 1, 12, 30, 0),
            "payload": b"\xff",
        },
    ]


def test_export_rows_writes_converted_csv_content() -> None:
    connection = _FakeConnection(_typed_rows())
    buffer = io.StringIO()
    writer = CsvRowWriter(buffer)

    count = export_rows(connection, _typed_table(), writer, batch_size=10)
    writer.close()

    assert count == 2
    parsed = list(csv.reader(io.StringIO(buffer.getvalue())))
    assert parsed[0] == ["id", "active", "created_at", "payload"]
    # created_at is DATETIME -> naive UTC (no +00:00 offset), session-tz-independent.
    assert parsed[1] == ["1", "true", "2024-01-01T00:00:00", "\\x0102"]
    assert parsed[2] == ["2", "false", "2024-06-01T12:30:00", "\\xff"]


def test_export_rows_streams_rows_in_primary_key_order() -> None:
    connection = _FakeConnection(_typed_rows())
    writer = _RecordingWriter()

    count = export_rows(connection, _typed_table(), writer, batch_size=1)

    assert count == 2
    assert writer.header == ["id", "active", "created_at", "payload"]
    assert [row["id"] for row in writer.rows] == [1, 2]
    # Converted values reached the writer (not raw MySQL values).
    assert writer.rows[0]["active"] is True
    # DATETIME -> naive UTC (no tzinfo), session-tz-independent for the timestamp col.
    assert writer.rows[0]["created_at"].tzinfo is None
    assert writer.rows[0]["created_at"] == datetime(2024, 1, 1, 0, 0, 0)


def test_export_rows_uses_consistent_snapshot_and_is_read_only() -> None:
    connection = _FakeConnection(_typed_rows())
    writer = _RecordingWriter()

    export_rows(connection, _typed_table(), writer, batch_size=10)

    statements = [sql for sql, _ in connection.executed]
    assert any(
        sql.strip().upper().startswith("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        for sql in statements
    )
    assert any(sql.strip().upper() == "COMMIT" for sql in statements)
    offending = [sql for sql in statements if is_write_or_ddl(sql)]
    assert offending == [], f"export issued write/DDL on the source: {offending}"


# ---------------------------------------------------------------------------
# TableExporter engine path
# ---------------------------------------------------------------------------


def test_table_exporter_uses_autocommit_streaming_snapshot() -> None:
    connection = _FakeConnection(_typed_rows())
    engine = _FakeEngine(connection)

    exporter = TableExporter(engine_factory=lambda _conn: engine, batch_size=10)
    writer = _RecordingWriter()

    count = exporter.export_table(_source_config(), _typed_table(), writer)

    assert count == 2
    assert connection.execution_options_seen == {
        "isolation_level": "AUTOCOMMIT",
        "stream_results": True,
    }
    assert engine.disposed is True
    offending = [sql for sql, _ in connection.executed if is_write_or_ddl(sql)]
    assert offending == []


def test_table_exporter_rejects_table_without_columns() -> None:
    connection = _FakeConnection([])
    engine = _FakeEngine(connection)
    exporter = TableExporter(engine_factory=lambda _conn: engine)
    table = TableDef(name="empty", columns=[], primary_key=["id"])

    with pytest.raises(ExportError, match="no columns"):
        exporter.export_table(_source_config(), table, _RecordingWriter())


def test_select_column_sql_wraps_spatial_in_st_asbinary() -> None:
    # Spatial columns are read as WKB bytes (-> bytea), aliased back to the name;
    # other columns are read as-is. This keeps Full Load bytes identical to the
    # WKB Debezium delivers for CDC. The logic now lives on the MySQL dialect.
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.core.source_dialect import dialect_for

    d = dialect_for(SourceType.MYSQL)
    geom = d.select_column_sql(ColumnDef(name="geom", mysql_type="point"))
    assert "ST_AsBinary" in geom
    assert geom.endswith("AS `geom`")

    plain = d.select_column_sql(ColumnDef(name="name", mysql_type="VARCHAR(100)"))
    assert "ST_AsBinary" not in plain
    assert plain == "`name`"


def test_keyset_stream_postgres_dialect_emits_pg_sql_not_mysql() -> None:
    # Regression (#5): the PG-source read composition (dialect quoting + select_column_sql)
    # inside keyset_stream is only unit-tested in isolation. A wiring regression that emits
    # MySQL backticks or drops the jsonb text-cast passes every isolated test but breaks
    # 100% of PostgreSQL Full Loads. Assert the composed SELECTs are PG-shaped.
    from dsql_migrator.core.source_dialect import PostgresSourceDialect

    rows = [{"id": i, "doc": "{}"} for i in range(1, 4)]
    connection = _FakeConnection(rows)
    table = TableDef(
        name="public.orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="doc", mysql_type="jsonb"),
        ],
        primary_key=["id"],
    )
    out = list(
        keyset_stream(connection, table, batch_size=2, dialect=PostgresSourceDialect())
    )
    assert [r["id"] for r in out] == [1, 2, 3]
    selects = [s for s, _ in connection.executed if s.strip().upper().startswith("SELECT")]
    assert selects, "no SELECT emitted"
    # PostgreSQL double-quoted identifiers, NEVER MySQL backticks.
    assert all("`" not in s for s in selects)
    assert any('"id"' in s for s in selects)
    # jsonb is read via CAST(... AS text) so a JSON `null` is preserved as text, not parsed.
    assert any('cast("doc" as text)' in s.lower() for s in selects)
    # Keyset predicate uses the PG-quoted PK.
    assert any('"id" > :last' in s for s in selects[1:])


def test_shardable_leading_int_pk_postgres_membership() -> None:
    # Tier-3 #13: PG reader sharding gates on shardable_leading_int_pk; a wrong
    # membership/parse would drop rows or fail range reads. Lock it for the PG dialect.
    from dsql_migrator.core.exporter import shardable_leading_int_pk
    from dsql_migrator.core.source_dialect import PostgresSourceDialect

    pg = PostgresSourceDialect()
    for t in ("integer", "bigint", "smallint", "bigserial"):
        table = TableDef(name="t", columns=[ColumnDef(name="id", mysql_type=t)], primary_key=["id"])
        assert shardable_leading_int_pk(table, pg) == "id", t
    comp = TableDef(
        name="t",
        columns=[ColumnDef(name="tenant_id", mysql_type="bigint"), ColumnDef(name="uid", mysql_type="uuid")],
        primary_key=["tenant_id", "uid"],
    )
    assert shardable_leading_int_pk(comp, pg) == "tenant_id"  # composite-leading int
    for t in ("uuid", "text", "numeric(10,0)", "timestamp with time zone"):
        table = TableDef(name="t", columns=[ColumnDef(name="id", mysql_type=t)], primary_key=["id"])
        assert shardable_leading_int_pk(table, pg) is None, t
