# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Capture a consistency point (watermark) at the start of a data export.

Before any rows are exported, the :class:`WatermarkCapturer` records the exact
point-in-time a snapshot reflects so the migration can be audited, a later CDC
catch-up can be resumed from it, and validation can compare source/target
as-of this point (Requirements 5.7, 5.8 / Property 11).

What is captured (within a single consistent-snapshot transaction so every
value is as-of one point):

- MySQL binlog coordinates (file + position) via ``SHOW MASTER STATUS``,
- the GTID set (``@@GLOBAL.gtid_executed``),
- the source ``@@GLOBAL.server_uuid``,
- a UTC snapshot timestamp, and
- per-table row counts.

Global consistency is obtained with ``START TRANSACTION WITH CONSISTENT
SNAPSHOT`` (InnoDB, REPEATABLE READ): a single reader sees all tables at one
coordinate. This is the accurate, slightly slower default described in the
design.

Two guarantees from the design are preserved:

- Read-only source (Property 1): every statement issued here is a read or
  transaction-control statement (``START TRANSACTION`` / ``COMMIT`` / ``SHOW``
  / ``SELECT``), and the same read-only guard from
  :mod:`dsql_migrator.core.introspector` is installed on the default engine so
  any accidental write/DDL is refused before reaching the database.
- Graceful degradation (Requirement 5.7): binlog/GTID metadata may be
  unavailable (binary logging disabled, or ``SHOW MASTER STATUS`` restricted on
  RDS/Aurora). The capturer records whatever is available, leaves the rest
  ``None``, and still produces a valid :class:`~dsql_migrator.core.models.Watermark`
  with the snapshot timestamp and row counts. Only :class:`ReadOnlySourceError`
  is never swallowed, so a Property 1 violation can never be masked.

The connection/engine is injectable (like
:class:`~dsql_migrator.core.introspector.SourceIntrospector`) so unit tests can
supply a fake connection returning canned ``SHOW MASTER STATUS`` /
``gtid_executed`` / ``COUNT(*)`` results without a real MySQL server.

Persistence note: the captured watermark is attached to the job record via
``MigrationJob.watermark`` (a first-class, serializable field). A dedicated
SQLite job-state store belongs to the Job Manager (task 11); this module only
produces the watermark and exposes it for persistence.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional, Protocol

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dsql_migrator.core.introspector import (
    ReadOnlySourceError,
    _default_engine_factory,
)
from dsql_migrator.core.models import SourceConnectionConfig, Watermark
from dsql_migrator.core.source_dialect import MySQLSourceDialect, SourceDialect

# Transaction-control and read statements issued during capture. None of these
# are writes or DDL, so they pass the read-only guard (Property 1).
START_CONSISTENT_SNAPSHOT = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
SHOW_MASTER_STATUS = "SHOW MASTER STATUS"
SHOW_BINARY_LOGS = "SHOW BINARY LOGS"
COMMIT = "COMMIT"


class _Connection(Protocol):
    """Minimal connection contract used by the capture helpers.

    Only ``execute`` is required, which keeps the capture logic easy to unit
    test with a lightweight fake connection that mirrors SQLAlchemy's result
    API (``mappings().first()`` and ``scalar()``).
    """

    def execute(self, statement: object, parameters: object = ...) -> object: ...


def _utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


# The MySQL source dialect: default for the exact-count / max-PK helpers below, whose
# identifier quoting now comes from the dialect (single source of truth) so a non-MySQL
# source quotes its own way. (The information_schema row-estimate query stays
# MySQL-specific for now; its PostgreSQL pg_class form is a later phase.)
_MYSQL_DIALECT = MySQLSourceDialect()


def _read_master_status(
    connection: _Connection,
) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """Read binlog file/position/GTID via ``SHOW MASTER STATUS`` (read-only).

    Returns ``(None, None, None)`` when the command is unavailable or returns no
    row (e.g. binary logging disabled or restricted privileges). A
    :class:`ReadOnlySourceError` is never suppressed.
    """
    try:
        result = connection.execute(text(SHOW_MASTER_STATUS))
        row = result.mappings().first()
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - binlog info is optional; degrade gracefully
        return None, None, None

    if not row:
        return None, None, None

    file_name = row.get("File")
    position = row.get("Position")
    gtid = row.get("Executed_Gtid_Set")
    return (
        file_name or None,
        int(position) if position is not None else None,
        gtid or None,
    )


def _read_global_variable(connection: _Connection, name: str) -> Optional[str]:
    """Read a global server variable as a string, or ``None`` if unavailable.

    ``name`` is an internal constant (never user input). A
    :class:`ReadOnlySourceError` is never suppressed.
    """
    try:
        result = connection.execute(text(f"SELECT @@GLOBAL.{name}"))
        value = result.scalar()
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - variable may be unavailable; degrade gracefully
        return None
    if value is None:
        return None
    text_value = str(value)
    return text_value or None


def _approximate_row_counts(
    connection: _Connection,
    tables: list[str],
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> dict[str, int]:
    """Return approximate per-table row counts (0 for missing) from the source.

    Thin wrapper over :meth:`SourceDialect.estimate_row_counts` (a single scan-free
    metadata query -- never a ``COUNT(*)`` scan -- so capturing the watermark adds
    negligible load even for very large tables) that maps its ``None`` "unknown"
    sentinel to ``0`` for the progress-baseline callers that want a numeric default.
    """
    return {
        name: (0 if estimate is None else estimate)
        for name, estimate in dialect.estimate_row_counts(connection, tables).items()
    }


def estimate_source_rows(
    connection: _Connection,
    tables: list[str],
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> dict[str, Optional[int]]:
    """Return APPROXIMATE per-table source row counts (no table scan, read-only).

    Delegates to :meth:`SourceDialect.estimate_row_counts`: one scan-free metadata
    query (MySQL ``information_schema.tables.table_rows``; PostgreSQL
    ``pg_class.reltuples``) -- never a ``COUNT(*)`` scan -- so it adds negligible load
    even for large-scale tables. This is the scalable default for the per-table
    consistency view: source-side scans on a live production source must be avoided. A
    table missing from the estimate maps to ``None`` (unknown), distinct from a genuine
    0. The estimate can drift from the exact count under heavy write churn -- it is a
    baseline, not an authoritative reconciliation (Validation, Step 4, does the exact
    comparison when one is explicitly needed).
    """
    return dialect.estimate_row_counts(connection, tables)


def _count_table_rows(
    connection: _Connection, table: str, dialect: "SourceDialect" = _MYSQL_DIALECT
) -> int:
    """Return exact ``COUNT(*)`` for ``table`` (full scan; used only when an
    exact count is explicitly required, not during watermark capture)."""
    statement = text(f"SELECT COUNT(*) FROM {dialect.quote_table(table)}")
    result = connection.execute(statement)
    value = result.scalar()
    return int(value) if value is not None else 0


def count_source_rows(
    connection: _Connection,
    tables: list[str],
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> dict[str, Optional[int]]:
    """Return an exact ``COUNT(*)`` per source table over one connection (read-only).

    Used by the per-table migration-status view to show the live source row count
    beside the target count so the operator can watch CDC converge. A table that
    errors (missing / no access) maps to ``None`` (unknown) rather than 0, so it is
    distinguishable from a genuinely empty table. Exact counts scan the table, so
    callers gate this behind an explicit user action (not an auto-poll).
    """
    counts: dict[str, Optional[int]] = {}
    for table in tables:
        try:
            counts[table] = _count_table_rows(connection, table, dialect)
        except ReadOnlySourceError:
            raise
        except Exception:  # noqa: BLE001 - missing table/error -> unknown
            counts[table] = None
    return counts


def max_pk_source(
    connection: _Connection,
    pk_by_table: dict[str, str],
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> dict[str, Optional[int]]:
    """Return ``MAX(pk)`` per source table for a single integer PK (read-only).

    ``pk_by_table`` maps a (possibly schema-qualified) table name to its single PK
    column. Used by the CDC consistency view to compare the source high-water PK
    against the target's: an equal max means the stream's leading edge is caught
    up even when row COUNTs differ. Returns ``None`` for a table with no single
    integer PK, or on any error.
    """
    out: dict[str, Optional[int]] = {}
    for table, pk in pk_by_table.items():
        if not pk:
            out[table] = None
            continue
        try:
            quoted = dialect.quote_table(table)
            col = dialect.quote_identifier(pk)
            result = connection.execute(text(f"SELECT MAX({col}) FROM {quoted}"))
            val = result.scalar()
            out[table] = int(val) if isinstance(val, int) else None
        except ReadOnlySourceError:
            raise
        except Exception:  # noqa: BLE001 - missing/non-integer/error -> unknown
            out[table] = None
    return out


def capture_watermark(
    connection: _Connection,
    tables: list[str],
    *,
    now: Optional[Callable[[], datetime]] = None,
) -> Watermark:
    """Capture a :class:`Watermark` on an open connection (read-only).

    All reads happen inside one ``START TRANSACTION WITH CONSISTENT SNAPSHOT``
    transaction so the binlog/GTID coordinates and the per-table row counts are
    all as-of a single point. The transaction is committed before returning.

    ``now`` is injectable to make the snapshot timestamp deterministic in tests;
    it defaults to the current UTC time.
    """
    clock = now or _utc_now

    connection.execute(text(START_CONSISTENT_SNAPSHOT))
    try:
        snapshot_timestamp = clock()
        binlog_file, binlog_position, gtid_from_master = _read_master_status(
            connection
        )
        gtid_executed = (
            _read_global_variable(connection, "gtid_executed") or gtid_from_master
        )
        server_uuid = _read_global_variable(connection, "server_uuid")
        # Approximate, scan-free row estimates (information_schema) to keep the
        # source load negligible during capture; validation re-counts exactly
        # when it needs precise numbers.
        table_row_counts = _approximate_row_counts(connection, tables)
    finally:
        connection.execute(text(COMMIT))

    return Watermark(
        binlog_file=binlog_file,
        binlog_position=binlog_position,
        gtid_executed=gtid_executed,
        server_uuid=server_uuid,
        snapshot_timestamp=snapshot_timestamp,
        table_row_counts=table_row_counts,
        row_counts_approximate=True,
    )


class WatermarkCapturer:
    """Captures an export consistency point from a read-only source connection."""

    def __init__(
        self,
        engine_factory: Optional[Callable[[SourceConnectionConfig], Engine]] = None,
        now: Optional[Callable[[], datetime]] = None,
    ) -> None:
        """Create a capturer.

        ``engine_factory`` builds a read-only-guarded SQLAlchemy engine for a
        connection config (the default reuses the introspector's MySQL factory,
        which installs the read-only guard). ``now`` overrides the clock used for
        the snapshot timestamp; both are injectable for testing.
        """
        self._engine_factory = engine_factory or _default_engine_factory
        self._now = now or _utc_now

    def capture(
        self, conn: SourceConnectionConfig, tables: list[str]
    ) -> Watermark:
        """Connect to the source and capture a watermark for ``tables``.

        Implements Requirements 5.7, 5.8 / Property 11. The connection runs in
        autocommit mode so the explicit ``START TRANSACTION WITH CONSISTENT
        SNAPSHOT`` / ``COMMIT`` statements control the snapshot transaction
        directly. All access is read-only (Property 1).
        """
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                snapshot_connection = connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                return capture_watermark(
                    snapshot_connection, tables, now=self._now
                )
        finally:
            engine.dispose()


def list_binary_logs(connection: _Connection) -> Optional[list[str]]:
    """Return the binlog file names the source still retains (read-only).

    ``SHOW BINARY LOGS`` lists only the logs that have **not** been purged. Returns
    ``None`` -- meaning "unknown", never "empty" -- when the statement is
    unavailable or the privilege is missing, so a caller can degrade to a warning
    instead of wrongly claiming the log is gone. A :class:`ReadOnlySourceError` is
    never suppressed.
    """
    try:
        result = connection.execute(text(SHOW_BINARY_LOGS))
        rows = result.mappings().all()
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - optional diagnostic; degrade to "unknown"
        return None
    names: list[str] = []
    for row in rows:
        name = row.get("Log_name")
        if name:
            names.append(str(name))
    return names


def binlog_resume_gap_reason(
    watermark_file: Optional[str], retained: Optional[list[str]]
) -> Optional[str]:
    """Why a gapless CDC resume from ``watermark_file`` is impossible, or ``None``.

    The Full Load watermark pins the binlog coordinate CDC must resume from, but the
    watermark is captured at Full Load **start** -- so a long load plus the ~15-20
    min infrastructure create plus the connector create all elapse before Debezium
    reads it. If the source purged that file in the meantime the gapless hand-off
    (Property 11) is impossible: rows changed between the snapshot point and the new
    log start are lost, and the only correct recovery is a re-snapshot.

    Today that surfaces only as an undiagnosed connector ``CREATE_FAILED`` roughly
    26 minutes into a billable create (MySQL error 1236, "could not find first log
    file"), so checking it up front turns a dead end into an actionable message.

    ``retained`` is ``None`` when the check could not run (statement unavailable /
    privilege missing) -- treated as "unknown", which never blocks. Pure.
    """
    if not watermark_file or retained is None or not retained:
        return None
    if watermark_file in retained:
        return None
    return (
        f"The Full Load watermark points at binary log '{watermark_file}', which the "
        f"source no longer retains (oldest kept: '{retained[0]}'). A gapless resume "
        "from the snapshot is no longer possible — changes made since then are not "
        "in the remaining logs. Re-run the Full Load to take a fresh snapshot (and "
        "raise the source's binlog retention first, e.g. "
        "CALL mysql.rds_set_configuration('binlog retention hours', 168)), or start "
        "CDC from a manual position and accept the gap."
    )


__all__ = [
    "WatermarkCapturer",
    "capture_watermark",
    "count_source_rows",
    "estimate_source_rows",
    "max_pk_source",
    "list_binary_logs",
    "binlog_resume_gap_reason",
    "START_CONSISTENT_SNAPSHOT",
    "SHOW_MASTER_STATUS",
    "SHOW_BINARY_LOGS",
    "COMMIT",
]
