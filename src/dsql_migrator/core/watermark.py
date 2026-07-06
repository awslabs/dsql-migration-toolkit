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

# Transaction-control and read statements issued during capture. None of these
# are writes or DDL, so they pass the read-only guard (Property 1).
START_CONSISTENT_SNAPSHOT = "START TRANSACTION WITH CONSISTENT SNAPSHOT"
SHOW_MASTER_STATUS = "SHOW MASTER STATUS"
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


def _quote_mysql_identifier(name: str) -> str:
    """Quote a MySQL identifier with backticks, escaping embedded backticks."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _quote_mysql_table(name: str) -> str:
    """Quote a possibly schema-qualified table name as ``\\`schema\\`.\\`table\\```.

    Cluster-wide introspection qualifies names as ``database.table``; quoting the
    whole string as one identifier yields ``\\`database.table\\``` which MySQL
    reads as one table in the (unset) current database ("1046, No database
    selected"). Split on the first dot so each part is quoted independently.
    """
    schema, separator, obj = name.partition(".")
    if separator and schema and obj:
        return f"{_quote_mysql_identifier(schema)}.{_quote_mysql_identifier(obj)}"
    return _quote_mysql_identifier(name)


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
    connection: _Connection, tables: list[str]
) -> dict[str, int]:
    """Return approximate per-table row counts from ``information_schema``.

    Reads the storage-engine row estimate (``information_schema.tables.
    table_rows``) in a single metadata query -- never a ``COUNT(*)`` table/index
    scan -- so capturing the watermark adds negligible load to the source even
    for very large tables (the scalable default; the estimate is good enough for
    a progress baseline). Tables absent from the estimate (or with a NULL
    estimate) default to 0. Names may be schema-qualified (``db.table``) or
    unqualified (resolved against the connection's current database).
    """
    if not tables:
        return {}
    current_db: Optional[str] = None
    try:
        current_db = connection.execute(text("SELECT DATABASE()")).scalar()
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - degrade gracefully
        current_db = None

    # Map each requested name to its (schema, table) lookup key and remember the
    # original name so the result is keyed exactly as the caller passed it.
    wanted: dict[tuple[str, str], str] = {}
    params: dict[str, str] = {}
    clauses: list[str] = []
    for index, name in enumerate(tables):
        schema, separator, obj = name.partition(".")
        if not (separator and schema and obj):
            schema, obj = (current_db or ""), name
        wanted[(schema, obj)] = name
        params[f"s{index}"] = schema
        params[f"t{index}"] = obj
        clauses.append(f"(table_schema = :s{index} AND table_name = :t{index})")

    counts: dict[str, int] = {name: 0 for name in tables}
    if not clauses:
        return counts
    query = text(
        "SELECT table_schema, table_name, table_rows "
        "FROM information_schema.tables "
        f"WHERE {' OR '.join(clauses)}"
    )
    try:
        result = connection.execute(query, params)
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - estimates are non-critical; degrade to 0
        return counts
    for row in result:
        key = (str(row[0]), str(row[1]))
        original = wanted.get(key)
        if original is not None and row[2] is not None:
            counts[original] = int(row[2])
    return counts


def estimate_source_rows(
    connection: _Connection, tables: list[str]
) -> dict[str, Optional[int]]:
    """Return APPROXIMATE per-table source row counts (no table scan, read-only).

    Reads the storage-engine row estimate from ``information_schema.tables.
    table_rows`` in a single metadata query -- never a ``COUNT(*)`` scan -- so it
    adds negligible load to the source even for large-scale tables. This is the
    scalable default for the per-table consistency view: source-side scans on a
    live production source must be avoided. A table missing from the estimate maps
    to ``None`` (unknown), distinct from a genuine 0. The estimate can drift from
    the exact count by a meaningful margin under heavy write churn -- it is a
    baseline, not an authoritative reconciliation (Validation, Step 4, does the
    exact comparison when one is explicitly needed).
    """
    if not tables:
        return {}
    current_db: Optional[str] = None
    try:
        current_db = connection.execute(text("SELECT DATABASE()")).scalar()
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - degrade gracefully
        current_db = None

    wanted: dict[tuple[str, str], str] = {}
    params: dict[str, str] = {}
    clauses: list[str] = []
    for index, name in enumerate(tables):
        schema, separator, obj = name.partition(".")
        if not (separator and schema and obj):
            schema, obj = (current_db or ""), name
        wanted[(schema, obj)] = name
        params[f"s{index}"] = schema
        params[f"t{index}"] = obj
        clauses.append(f"(table_schema = :s{index} AND table_name = :t{index})")

    out: dict[str, Optional[int]] = {name: None for name in tables}
    if not clauses:
        return out
    query = text(
        "SELECT table_schema, table_name, table_rows "
        "FROM information_schema.tables "
        f"WHERE {' OR '.join(clauses)}"
    )
    try:
        result = connection.execute(query, params)
    except ReadOnlySourceError:
        raise
    except Exception:  # noqa: BLE001 - estimates are non-critical; leave None
        return out
    for row in result:
        key = (str(row[0]), str(row[1]))
        original = wanted.get(key)
        if original is not None and row[2] is not None:
            out[original] = int(row[2])
    return out


def _count_table_rows(connection: _Connection, table: str) -> int:
    """Return exact ``COUNT(*)`` for ``table`` (full scan; used only when an
    exact count is explicitly required, not during watermark capture)."""
    statement = text(f"SELECT COUNT(*) FROM {_quote_mysql_table(table)}")
    result = connection.execute(statement)
    value = result.scalar()
    return int(value) if value is not None else 0


def count_source_rows(
    connection: _Connection, tables: list[str]
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
            counts[table] = _count_table_rows(connection, table)
        except ReadOnlySourceError:
            raise
        except Exception:  # noqa: BLE001 - missing table/error -> unknown
            counts[table] = None
    return counts


def max_pk_source(
    connection: _Connection, pk_by_table: dict[str, str]
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
            quoted = _quote_mysql_table(table)
            col = "`" + pk.replace("`", "``") + "`"
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


__all__ = [
    "WatermarkCapturer",
    "capture_watermark",
    "count_source_rows",
    "estimate_source_rows",
    "max_pk_source",
    "START_CONSISTENT_SNAPSHOT",
    "SHOW_MASTER_STATUS",
    "COMMIT",
]
