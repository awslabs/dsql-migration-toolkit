# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit and property tests for the export consistency-point (watermark) capturer.

Covers:
- Full capture: binlog file/position, ``gtid_executed``, ``server_uuid``, a UTC
  snapshot timestamp, and per-table row counts (Requirements 5.7, 5.8).
- Graceful degradation when binlog/GTID metadata is unavailable: those fields
  are ``None`` while the snapshot timestamp and row counts are still present.
- Capture occurs within a single consistent-snapshot transaction
  (``START TRANSACTION WITH CONSISTENT SNAPSHOT`` ... ``COMMIT``).
- Read-only guarantee: no write/DDL statement is issued (Property 1).
- ``Watermark`` round-trips through ``MigrationJob`` serialization (persistence
  hook for Requirement 5.7 / Property 11).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional, Union

from dsql_migrator.core.introspector import is_write_or_ddl
from dsql_migrator.core.models import MigrationJob, Watermark
from dsql_migrator.core.watermark import (
    COMMIT,
    SHOW_MASTER_STATUS,
    START_CONSISTENT_SNAPSHOT,
    WatermarkCapturer,
    capture_watermark,
    count_source_rows,
    estimate_source_rows,
    max_pk_source,
)

FIXED_NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fakes mirroring SQLAlchemy's result/connection API
# ---------------------------------------------------------------------------


class _FakeResult:
    """A result that mimics the slice of SQLAlchemy's API used by capture."""

    def __init__(
        self,
        *,
        mapping: Optional[dict] = None,
        scalar: object = None,
        rows: Optional[list] = None,
    ) -> None:
        self._mapping = mapping
        self._scalar = scalar
        self._rows = rows or []

    def mappings(self) -> "_FakeResult":
        return self

    def first(self) -> Optional[dict]:
        return self._mapping

    def scalar(self) -> object:
        return self._scalar

    def __iter__(self):  # noqa: ANN204 - iterate result rows
        return iter(self._rows)


class _FakeConnection:
    """A connection returning canned results for each capture statement.

    Each ``master_status``/``gtid``/``server_uuid`` value may be a value or an
    ``Exception`` instance to simulate a failing/restricted query.
    """

    def __init__(
        self,
        *,
        master_status: Union[dict, Exception, None] = None,
        gtid: Union[str, Exception, None] = None,
        server_uuid: Union[str, Exception, None] = None,
        counts: Optional[dict[str, int]] = None,
        current_db: str = "app",
    ) -> None:
        self._master_status = master_status
        self._gtid = gtid
        self._server_uuid = server_uuid
        self._counts = counts or {}
        self._current_db = current_db
        self.executed: list[str] = []
        self.execution_options_calls: list[dict] = []

    def execution_options(self, **kwargs: object) -> "_FakeConnection":
        self.execution_options_calls.append(kwargs)
        return self

    def execute(self, statement, _parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        self.executed.append(sql)
        upper = sql.upper()

        if "SHOW MASTER STATUS" in upper:
            if isinstance(self._master_status, Exception):
                raise self._master_status
            return _FakeResult(mapping=self._master_status)
        if "@@GLOBAL.GTID_EXECUTED" in upper:
            if isinstance(self._gtid, Exception):
                raise self._gtid
            return _FakeResult(scalar=self._gtid)
        if "@@GLOBAL.SERVER_UUID" in upper:
            if isinstance(self._server_uuid, Exception):
                raise self._server_uuid
            return _FakeResult(scalar=self._server_uuid)
        if "SELECT DATABASE()" in upper:
            return _FakeResult(scalar=self._current_db)
        if "INFORMATION_SCHEMA.TABLES" in upper:
            # Approximate row estimates: return (schema, table, rows) for each
            # known table under the current database.
            rows = [
                (self._current_db, table, count)
                for table, count in self._counts.items()
            ]
            return _FakeResult(rows=rows)
        if upper.startswith("SELECT COUNT(*)"):
            table = sql.split("`")[1]
            return _FakeResult(scalar=self._counts.get(table, 0))
        # START TRANSACTION / COMMIT and any other control statement.
        return _FakeResult()


class _FakeConnectionContext:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeConnection:
        return self._connection

    def __exit__(self, *exc: object) -> bool:
        return False


class _FakeEngine:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection
        self.disposed = False

    def connect(self) -> _FakeConnectionContext:
        return _FakeConnectionContext(self._connection)

    def dispose(self) -> None:
        self.disposed = True


# ---------------------------------------------------------------------------
# capture_watermark — full capture
# ---------------------------------------------------------------------------


def test_capture_collects_all_fields() -> None:
    connection = _FakeConnection(
        master_status={
            "File": "mysql-bin.000123",
            "Position": 45678,
            "Executed_Gtid_Set": "",
        },
        gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
        server_uuid="3E11FA47-71CA-11E1-9E33-C80AA9429562",
        counts={"orders": 5, "customers": 3},
    )

    watermark = capture_watermark(
        connection, ["orders", "customers"], now=lambda: FIXED_NOW
    )

    assert watermark.binlog_file == "mysql-bin.000123"
    assert watermark.binlog_position == 45678
    assert watermark.gtid_executed == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert watermark.server_uuid == "3E11FA47-71CA-11E1-9E33-C80AA9429562"
    assert watermark.snapshot_timestamp == FIXED_NOW
    assert watermark.table_row_counts == {"orders": 5, "customers": 3}
    # Counts come from information_schema estimates (no COUNT(*) scan).
    assert watermark.row_counts_approximate is True
    assert not any("COUNT(*)" in sql.upper() for sql in connection.executed)


def test_capture_snapshot_timestamp_is_utc() -> None:
    connection = _FakeConnection(counts={"orders": 0})
    watermark = capture_watermark(connection, ["orders"])

    timestamp = watermark.model_dump()["snapshot_timestamp"]
    assert timestamp.tzinfo is not None
    assert timestamp.utcoffset() == timezone.utc.utcoffset(None)


def test_capture_gtid_falls_back_to_master_status() -> None:
    """When ``@@GLOBAL.gtid_executed`` is empty, the master-status GTID is used."""
    connection = _FakeConnection(
        master_status={
            "File": "mysql-bin.000001",
            "Position": 100,
            "Executed_Gtid_Set": "uuid:1-9",
        },
        gtid=None,
        counts={"orders": 1},
    )

    watermark = capture_watermark(connection, ["orders"], now=lambda: FIXED_NOW)

    assert watermark.gtid_executed == "uuid:1-9"


# ---------------------------------------------------------------------------
# capture_watermark — graceful degradation
# ---------------------------------------------------------------------------


def test_capture_handles_missing_binlog_status_row() -> None:
    """No master-status row (e.g. binlog disabled): coords None, counts present."""
    connection = _FakeConnection(
        master_status=None,
        gtid=None,
        server_uuid="server-uuid-1",
        counts={"orders": 7},
    )

    watermark = capture_watermark(connection, ["orders"], now=lambda: FIXED_NOW)

    assert watermark.binlog_file is None
    assert watermark.binlog_position is None
    assert watermark.gtid_executed is None
    assert watermark.server_uuid == "server-uuid-1"
    assert watermark.snapshot_timestamp == FIXED_NOW
    assert watermark.table_row_counts == {"orders": 7}


def test_capture_handles_restricted_master_status_command() -> None:
    """``SHOW MASTER STATUS`` denied: capture still succeeds with what remains."""
    connection = _FakeConnection(
        master_status=RuntimeError("command denied to user"),
        gtid=RuntimeError("variable not available"),
        server_uuid="server-uuid-2",
        counts={"orders": 2, "customers": 4},
    )

    watermark = capture_watermark(
        connection, ["orders", "customers"], now=lambda: FIXED_NOW
    )

    assert watermark.binlog_file is None
    assert watermark.binlog_position is None
    assert watermark.gtid_executed is None
    assert watermark.server_uuid == "server-uuid-2"
    assert watermark.table_row_counts == {"orders": 2, "customers": 4}


# ---------------------------------------------------------------------------
# Consistent-snapshot transaction + read-only (Property 1 / 11)
# ---------------------------------------------------------------------------


def test_capture_runs_within_consistent_snapshot_transaction() -> None:
    connection = _FakeConnection(counts={"orders": 1})

    capture_watermark(connection, ["orders"], now=lambda: FIXED_NOW)

    assert START_CONSISTENT_SNAPSHOT in connection.executed
    assert COMMIT in connection.executed
    start_index = connection.executed.index(START_CONSISTENT_SNAPSHOT)
    commit_index = connection.executed.index(COMMIT)
    count_index = next(
        i
        for i, sql in enumerate(connection.executed)
        if "INFORMATION_SCHEMA.TABLES" in sql.upper()
    )
    # Coordinates and the (approximate) counts are read strictly between
    # snapshot start and commit.
    assert start_index < count_index < commit_index


def test_capture_tolerates_count_estimate_failure_and_commits() -> None:
    class _FailingEstimateConnection(_FakeConnection):
        def execute(self, statement, parameters=None):  # noqa: ANN001, ANN201
            sql = str(statement)
            if "INFORMATION_SCHEMA.TABLES" in sql.upper():
                self.executed.append(sql)
                raise RuntimeError("estimate query failed")
            return super().execute(statement, parameters)

    connection = _FailingEstimateConnection(counts={"orders": 1})

    # The row-count estimate is non-critical: a failure degrades to zeros instead
    # of aborting the capture, and the snapshot transaction is still committed.
    watermark = capture_watermark(connection, ["orders"], now=lambda: FIXED_NOW)

    assert COMMIT in connection.executed
    assert watermark.table_row_counts == {"orders": 0}
    assert watermark.row_counts_approximate is True


def test_capture_issues_no_write_or_ddl_statements() -> None:
    """Property 1: capture must never issue a write/DDL to the read-only source."""
    connection = _FakeConnection(
        master_status={
            "File": "mysql-bin.000001",
            "Position": 10,
            "Executed_Gtid_Set": "uuid:1",
        },
        gtid="uuid:1",
        server_uuid="server-uuid",
        counts={"orders": 1, "customers": 2},
    )

    capture_watermark(connection, ["orders", "customers"], now=lambda: FIXED_NOW)

    assert connection.executed  # statements were actually issued
    offending = [sql for sql in connection.executed if is_write_or_ddl(sql)]
    assert offending == [], f"capture issued write/DDL: {offending}"


def test_count_estimate_uses_bound_parameters_not_interpolation() -> None:
    """Schema/table names are bound parameters, never interpolated into SQL."""
    connection = _FakeConnection(counts={"weird`name": 0})

    capture_watermark(connection, ["weird`name"], now=lambda: FIXED_NOW)

    info_sql = next(
        sql
        for sql in connection.executed
        if "INFORMATION_SCHEMA.TABLES" in sql.upper()
    )
    # The raw name never appears in the SQL text -- it is a bound parameter.
    assert "weird`name" not in info_sql
    assert ":t0" in info_sql


# ---------------------------------------------------------------------------
# WatermarkCapturer — engine/clock injection
# ---------------------------------------------------------------------------


def test_capturer_uses_injected_engine_and_clock() -> None:
    connection = _FakeConnection(
        master_status={
            "File": "mysql-bin.000009",
            "Position": 999,
            "Executed_Gtid_Set": "",
        },
        gtid="uuid:1-3",
        server_uuid="server-uuid-9",
        counts={"orders": 11},
    )
    engine = _FakeEngine(connection)

    capturer = WatermarkCapturer(
        engine_factory=lambda _conn: engine, now=lambda: FIXED_NOW
    )
    from dsql_migrator.core.models import SourceConnectionConfig

    watermark = capturer.capture(
        SourceConnectionConfig(host="db.example.com", database="app"), ["orders"]
    )

    assert watermark.binlog_file == "mysql-bin.000009"
    assert watermark.binlog_position == 999
    assert watermark.gtid_executed == "uuid:1-3"
    assert watermark.table_row_counts == {"orders": 11}
    assert engine.disposed is True
    # The snapshot transaction is controlled explicitly via AUTOCOMMIT.
    assert {"isolation_level": "AUTOCOMMIT"} in connection.execution_options_calls
    assert SHOW_MASTER_STATUS in connection.executed


# ---------------------------------------------------------------------------
# Persistence hook — watermark round-trips through MigrationJob
# ---------------------------------------------------------------------------


def test_watermark_round_trips_through_migration_job() -> None:
    watermark = Watermark(
        binlog_file="mysql-bin.000123",
        binlog_position=45678,
        gtid_executed="uuid:1-5",
        server_uuid="server-uuid",
        snapshot_timestamp=FIXED_NOW,
        table_row_counts={"orders": 5, "customers": 3},
    )
    job = MigrationJob(job_id="job-1", watermark=watermark)

    restored = MigrationJob.model_validate(job.model_dump())

    assert restored == job
    assert restored.watermark == watermark


# ---------------------------------------------------------------------------
# count_source_rows / max_pk_source: per-table source counts + high-water PK
# ---------------------------------------------------------------------------


class _ScalarConn:
    """Minimal connection: maps a per-table scalar by matching the table token.

    ``values`` maps a table name (or its backtick-quoted form fragment) to the
    scalar the next ``execute(...).scalar()`` returns; a table listed in
    ``raises`` makes execute() raise (missing table / no access).
    """

    def __init__(self, values: dict, raises: Optional[set] = None) -> None:
        self._values = values
        self._raises = raises or set()
        self._pending: object = None

    def execute(self, statement, _parameters=None):  # noqa: ANN001, ANN201
        sql = str(statement)
        for name in self._raises:
            token = name.split(".")[-1]
            if token in sql:
                raise RuntimeError("relation does not exist")
        self._pending = None
        for name, val in self._values.items():
            token = name.split(".")[-1]
            if token in sql:
                self._pending = val
                break
        return _FakeResult(scalar=self._pending)


def test_count_source_rows_per_table_and_missing_is_none() -> None:
    conn = _ScalarConn({"cdc_demo.orders": 100, "cdc_demo.items": 0},
                       raises={"cdc_demo.gone"})
    counts = count_source_rows(conn, ["cdc_demo.orders", "cdc_demo.items", "cdc_demo.gone"])
    assert counts == {"cdc_demo.orders": 100, "cdc_demo.items": 0, "cdc_demo.gone": None}


def test_max_pk_source_returns_high_water_and_skips_no_pk() -> None:
    conn = _ScalarConn({"cdc_demo.orders": 12759})
    out = max_pk_source(conn, {"cdc_demo.orders": "id", "cdc_demo.nopk": ""})
    assert out["cdc_demo.orders"] == 12759
    assert out["cdc_demo.nopk"] is None  # empty pk -> skipped


def test_max_pk_source_non_integer_max_is_none() -> None:
    conn = _ScalarConn({"cdc_demo.orders": "not-an-int"})
    out = max_pk_source(conn, {"cdc_demo.orders": "id"})
    assert out["cdc_demo.orders"] is None


# ---------------------------------------------------------------------------
# estimate_source_rows: scan-free information_schema row estimate
# ---------------------------------------------------------------------------


def test_estimate_source_rows_uses_information_schema_not_count() -> None:
    # Reads the table_rows estimate via information_schema -- NO COUNT(*) scan
    # (so it stays cheap on a large-scale source).
    conn = _FakeConnection(counts={"orders": 12000, "items": 500}, current_db="cdc_demo")
    est = estimate_source_rows(conn, ["orders", "items"])
    assert est == {"orders": 12000, "items": 500}
    assert not any("COUNT(*)" in sql.upper() for sql in conn.executed)
    assert any("INFORMATION_SCHEMA.TABLES" in sql.upper() for sql in conn.executed)


def test_estimate_source_rows_missing_table_is_none() -> None:
    # A table not present in information_schema maps to None (unknown), distinct
    # from a genuine 0-row table.
    conn = _FakeConnection(counts={"orders": 12000}, current_db="cdc_demo")
    est = estimate_source_rows(conn, ["orders", "ghost"])
    assert est["orders"] == 12000
    assert est["ghost"] is None


def test_estimate_source_rows_empty_input() -> None:
    conn = _FakeConnection(counts={}, current_db="cdc_demo")
    assert estimate_source_rows(conn, []) == {}
