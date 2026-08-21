# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Export source table rows to files via read-only PK keyset streaming.

This module implements the export half of the data migrator (design.md section
"Data Migration Design" -> "Export", Requirement 5.1 / Property 1). It streams a
source MySQL table to an output file (CSV today; a pluggable :class:`RowWriter`
keeps other formats orthogonal) while guaranteeing the source is only ever read.

Pipeline for one table:

1. PK keyset streaming (not OFFSET): rows are read in ascending primary-key
   order with ``WHERE pk > :last ORDER BY pk LIMIT :batch_size`` (single key) or an
   explicit lexicographic disjunction ``(k0 > :last_0) OR (k0 = :last_0 AND k1 >
   :last_1) OR ...`` (composite key, PK-index-friendly on MySQL 5.7+, unlike the
   row-value tuple form), carrying the last-seen primary key forward. This avoids large
   ``OFFSET`` scans and yields a stable, resumable read order. Single- and
   composite-column primary keys are supported; only a missing primary key raises
   :class:`UnsupportedPrimaryKeyError` rather than being silently mishandled.
2. Read-only consistent snapshot (Property 1 / Requirement 5.1): the stream runs
   inside ``START TRANSACTION WITH CONSISTENT SNAPSHOT`` (InnoDB, REPEATABLE
   READ) -- the same snapshot semantics used by watermark capture (task 8.1) --
   and the read-only guard from :mod:`dsql_migrator.core.introspector` is
   installed on the default engine so any accidental write/DDL is refused before
   reaching the database. A server-side / streaming cursor (``stream_results``)
   is used so large tables are never fully materialized in memory.
3. Value conversion: cell values are converted with the **same** MySQL -> DSQL
   type mapping used by the Schema Converter (task 5,
   :func:`dsql_migrator.core.converter.map_mysql_type`). There is no second,
   divergent mapping table: the target DSQL type decides the value transform
   (``TINYINT(1)`` -> boolean ``0/1`` -> ``False/True``; ``DATETIME`` -> UTC
   ``timestamp``; ``BLOB`` -> ``bytea`` bytes; ``ENUM``/``SET`` -> text).
4. Streaming output: rows are written as they stream, never accumulated.

Output formats and sinks are orthogonal. The :class:`RowWriter` abstraction is
the format boundary; :class:`CsvRowWriter` implements CSV (stdlib ``csv``, no
extra dependency) targeting a text stream (e.g. a local file). The live import
path does not stage rows to a file at all -- it converts and applies them in
process via :meth:`TableExporter.stream_converted_rows` -- so CSV output is a
utility, not the migration hot path. A Parquet writer could be added as another
:class:`RowWriter` without touching the streaming/conversion core.

Dependencies (engine, output stream) are injectable -- mirroring
:class:`~dsql_migrator.core.introspector.SourceIntrospector` and the watermark
capturer -- so unit tests never touch a real MySQL.
"""

from __future__ import annotations

import csv
import logging
from abc import ABC, abstractmethod
from datetime import datetime, time, timedelta, timezone
from time import monotonic as _wall_monotonic, sleep as _wall_sleep
from typing import Callable, Iterator, Mapping, Optional, Protocol, TextIO

from sqlalchemy import text
from sqlalchemy.engine import Engine

from dsql_migrator.core.converter import map_mysql_type
from dsql_migrator.core.introspector import _default_engine_factory
from dsql_migrator.core.models import SourceConnectionConfig, TableDef
from dsql_migrator.core.source_dialect import (
    MySQLSourceDialect,
    SourceDialect,
    dialect_for,
)
from dsql_migrator.core.watermark import (
    COMMIT,
    estimate_source_rows,
)

# Dev row-trace logger (child of the ``dsql_migrator`` package logger, whose level
# is set from DSQL_MIGRATOR_LOG_LEVEL at app start). Per-page (never per-row) DEBUG
# lines record the PK range + count of each keyset page so a developer can trace
# exactly which rows Full Load read, in what order. Guarded by ``isEnabledFor`` so
# production (INFO) builds no strings and pays nothing. Logs PK values + counts
# only -- NEVER row values (Property 7); a natural-key PK is the operator's risk.
_LOGGER = logging.getLogger(__name__)

# Default rows fetched per keyset page. A page is bounded by this value, so a
# single page -- not the whole table -- is the upper bound on in-flight rows.
# Raised from 1000 to 5000: each MySQL round-trip is expensive (GIL-held PyMySQL
# network I/O), so larger pages amortize that cost across more rows. Memory stays
# bounded (one page in flight per reader shard).
DEFAULT_BATCH_SIZE = 5000


class ExportError(RuntimeError):
    """Base error for failures while exporting a source table."""


class ValueConversionError(ExportError):
    """Raised when a source cell cannot be safely converted to its target type.

    Surfaces a value that would otherwise be silently corrupted -- e.g. a
    ``TINYINT(1)`` column (mapped to DSQL ``boolean``) holding a value outside
    ``{0, 1}`` (MySQL's ``(1)`` is display width, not a value constraint, so a
    ``TINYINT(1)`` legally stores -128..127 / 0..255). ``bool(int(value))`` would
    flatten any non-zero magnitude to ``True`` and lose the original number, so we
    fail loudly (naming the column + value) instead of corrupting the row.
    """


class UnsupportedPrimaryKeyError(ExportError):
    """Raised when a table's primary key cannot drive keyset streaming.

    Keyset streaming requires a primary key (single- or composite-column) to
    define a deterministic, resumable read order. Only a MISSING primary key is
    unsupported; it is reported clearly instead of being silently mishandled.
    """


class _Connection(Protocol):
    """Minimal connection contract used by the streaming helper.

    Only ``execute`` is required, which keeps the streaming logic easy to unit
    test with a lightweight fake connection that mirrors SQLAlchemy's result API
    (``execute(...).mappings()`` yielding dict-like rows).
    """

    def execute(self, statement: object, parameters: object = ...) -> object: ...


# ---------------------------------------------------------------------------
# Value conversion (reuses the Schema Converter type mapping)
# ---------------------------------------------------------------------------


def _utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC (assume naive datetimes are already UTC).

    MySQL ``DATETIME`` has no time zone, and the converter maps it to a DSQL
    ``timestamp`` treated as UTC. A naive value therefore gets UTC attached; a
    tz-aware value is converted to UTC.
    """
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _target_kind(mysql_type: str) -> str:
    """Return the normalized DSQL target type name for a MySQL type string.

    Reuses :func:`dsql_migrator.core.converter.map_mysql_type` so value
    conversion stays consistent with schema conversion (no divergent mapping).
    Parameters and casing are stripped (e.g. ``numeric(20, 0)`` -> ``numeric``).
    Unparseable types degrade to a pass-through (their lower-cased text).
    """
    try:
        target_type, _ = map_mysql_type(mysql_type)
    except ValueError:
        return mysql_type.strip().lower()
    return target_type.split("(", 1)[0].strip().lower()


class ValueConverter:
    """Converts MySQL cell values to canonical values for the DSQL target type.

    The conversion is keyed off the DSQL type produced by the shared type mapping
    (Requirement 5.1, consistent with the Schema Converter). Canonical outputs:

    - ``boolean``: Python ``bool`` (``TINYINT(1)`` ``0/1`` -> ``False/True``),
    - ``bytea``: Python ``bytes`` (MySQL ``BLOB`` payload preserved),
    - ``timestamp``: tz-aware UTC :class:`datetime` (``DATETIME`` normalized),
    - everything else: the value unchanged.

    Serialization to a concrete file format (e.g. CSV text) is the
    :class:`RowWriter`'s responsibility, keeping type semantics and output format
    separate.
    """

    def __init__(
        self,
        table: TableDef,
        *,
        target_types: Optional[Mapping[str, str]] = None,
    ) -> None:
        """Precompute the per-column target type kinds for ``table``.

        ``target_types`` (optional) maps a column name to the *applied* target
        type (e.g. parsed from the converted/edited DDL via
        :func:`~dsql_migrator.core.converter.parse_target_column_types`). When a
        column is present there, the conversion follows that applied type instead
        of the source-derived mapping -- so a user remap in Schema Conversion
        (e.g. ``TINYINT(1)`` -> ``smallint`` instead of ``boolean``) takes effect
        on the Full Load value conversion. Columns absent from the override keep
        the source-derived kind.
        """
        overrides = target_types or {}
        self._kinds: dict[str, str] = {}
        for column in table.columns:
            if column.name in overrides:
                self._kinds[column.name] = (
                    overrides[column.name].split("(", 1)[0].strip().lower()
                )
            else:
                self._kinds[column.name] = _target_kind(column.mysql_type)
        # Source MySQL BIT(n) columns: the driver returns the value as big-endian
        # bytes, but BIT maps to an integer target in DSQL (the bit type is
        # unsupported), so those bytes must be decoded to an int at convert time.
        self._bit_columns: frozenset[str] = frozenset(
            column.name
            for column in table.columns
            if column.mysql_type.strip().lower().split("(", 1)[0] == "bit"
        )
        # Fast-path: columns whose target kind needs no conversion (int, varchar,
        # numeric, text, etc.). For a typical OLTP table 80-90% of columns fall
        # here. convert_row skips convert_value entirely for these, replacing 7+
        # Python ops per cell with a single frozenset lookup.
        _NEEDS_CONVERSION_KINDS = frozenset(
            {"boolean", "bytea", "timestamp", "timestamptz", "time"}
        )
        self._passthrough_columns: frozenset[str] = frozenset(
            name
            for name, kind in self._kinds.items()
            if kind not in _NEEDS_CONVERSION_KINDS and name not in self._bit_columns
        )

    def convert_value(self, column_name: str, value: object) -> object:
        """Convert a single cell value for ``column_name`` (``None`` passes through)."""
        if value is None:
            return None
        if column_name in self._bit_columns:
            # MySQL BIT(n) -> integer target. The driver returns big-endian bytes
            # (e.g. b'\xdb'); decode to the unsigned integer the bit pattern holds.
            # A driver that already returns an int (some configs) passes through.
            if isinstance(value, (bytes, bytearray, memoryview)):
                return int.from_bytes(bytes(value), byteorder="big")
            return value
        kind = self._kinds.get(column_name)
        if kind == "boolean":
            if isinstance(value, bool):
                return value
            # TINYINT(1) -> DSQL boolean. The (1) is display width, not a value
            # constraint, so the column can legally hold values outside {0, 1};
            # bool(int(value)) would flatten e.g. 2 or -1 to True and silently lose
            # the magnitude. Fail loudly (naming the column + value) so a wrong
            # boolean never lands instead of corrupting the row. A genuine 0/1
            # boolean column is unaffected.
            as_int = int(value)
            if as_int not in (0, 1):
                raise ValueConversionError(
                    f"column '{column_name}' maps to DSQL boolean but the source "
                    f"value {as_int!r} is outside {{0, 1}}; a TINYINT(1) storing "
                    "values beyond 0/1 cannot be a boolean without data loss. In "
                    "Schema Conversion, remap this column's target type to "
                    "smallint/integer (the converted DDL is editable), re-apply, "
                    "then retry this table -- the value then loads as an integer. "
                    "Alternatively, restrict the source values to 0/1."
                )
            return bool(as_int)
        if kind == "bytea":
            if isinstance(value, (bytes, bytearray, memoryview)):
                return bytes(value)
            return value
        if kind == "timestamp":
            # MySQL DATETIME -> DSQL ``timestamp`` WITHOUT TIME ZONE. Normalize to
            # UTC, then drop the tzinfo: binding a tz-AWARE datetime to a no-tz
            # column makes PostgreSQL/psycopg convert it to the session TimeZone and
            # discard the offset, so a non-UTC session would silently shift the
            # stored wall-clock by hours. A naive UTC value stores the intended
            # wall-clock regardless of the session's TimeZone.
            if isinstance(value, datetime):
                return _utc(value).replace(tzinfo=None)
            return value
        if kind == "timestamptz":
            # MySQL TIMESTAMP -> DSQL ``timestamptz``. A tz-aware UTC value stores
            # the correct instant independent of the session TimeZone.
            if isinstance(value, datetime):
                return _utc(value)
            return value
        if kind == "time":
            # MySQL TIME comes off the driver as a timedelta; psycopg binds a
            # timedelta to interval, not to a DSQL ``time`` column (type mismatch
            # that fails the row). Convert an in-range (0 <= t < 24h) value to a
            # datetime.time. MySQL TIME's full range is -838:59:59..838:59:59, and a
            # value outside [0, 24h) has NO ``time`` representation -- passing the
            # raw timedelta through would silently bind to interval (or emit a
            # non-time text cell), corrupting the column. Fail loudly (naming the
            # column + value) instead, matching the TINYINT(1)-out-of-range guard.
            if isinstance(value, timedelta):
                total = value.total_seconds()
                if 0 <= total < 86400:
                    secs = int(total)
                    micros = value.microseconds
                    return time(
                        hour=secs // 3600,
                        minute=(secs % 3600) // 60,
                        second=secs % 60,
                        microsecond=micros,
                    )
                raise ValueConversionError(
                    f"column '{column_name}' maps to DSQL time but the source "
                    f"value {value!r} is outside 00:00:00..23:59:59.999999; MySQL "
                    "TIME allows -838:59:59..838:59:59, which has no DSQL 'time' "
                    "representation and would corrupt the column. In Schema "
                    "Conversion, remap this column's target type to interval or "
                    "text (the converted DDL is editable), re-apply, then retry "
                    "this table. Alternatively, restrict the source TIME values to "
                    "the 0..24h range."
                )
            return value
        return value

    def convert_row(self, row: Mapping[str, object]) -> dict[str, object]:
        """Convert every known cell in ``row`` (unknown columns pass through).

        Fast path: columns in ``_passthrough_columns`` (the majority for a typical
        OLTP table — int, varchar, numeric, text) skip ``convert_value`` entirely.
        Only columns that actually need type translation (boolean, bytea, timestamp,
        timestamptz, time, bit) go through the full method.
        """
        passthrough = self._passthrough_columns
        return {
            name: (value if name in passthrough else self.convert_value(name, value))
            for name, value in row.items()
        }


# ---------------------------------------------------------------------------
# Output: pluggable row writers (format boundary)
# ---------------------------------------------------------------------------


def _csv_cell(value: object) -> str:
    """Serialize a converted value into a DSQL-loadable CSV text cell.

    ``None`` becomes an empty field, booleans become ``true``/``false``, bytes
    become PostgreSQL ``bytea`` hex (``\\x...``), and datetimes become ISO 8601.
    Everything else uses its string form; the ``csv`` module handles quoting.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "\\x" + bytes(value).hex()
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


class RowWriter(ABC):
    """A streaming, single-file row writer; subclasses implement a format.

    The exporter calls :meth:`write_header` once, then :meth:`write_row` per row
    as rows stream from the source, then :meth:`close`. This interface is the
    seam where additional formats (e.g. Parquet) or sinks plug in without
    changing the streaming/conversion core.
    """

    @abstractmethod
    def write_header(self, columns: list[str]) -> None:
        """Write the column header (column order for subsequent rows)."""

    @abstractmethod
    def write_row(self, row: Mapping[str, object]) -> None:
        """Write one already-converted row."""

    @abstractmethod
    def close(self) -> None:
        """Flush and finalize output (e.g. close the underlying file)."""


class CsvRowWriter(RowWriter):
    """Writes rows as CSV to a text stream (e.g. a local file).

    Rows are emitted in the header's column order. The writer optionally owns the
    underlying stream (closing it on :meth:`close`) and optionally runs a
    finalizer on close.
    """

    def __init__(
        self,
        stream: TextIO,
        *,
        owns_stream: bool = False,
        on_close: Optional[Callable[[], None]] = None,
    ) -> None:
        """Create a CSV writer over ``stream``.

        ``owns_stream`` closes ``stream`` on :meth:`close`; ``on_close`` runs an
        extra finalizer (after flushing, before closing the stream).
        """
        self._stream = stream
        self._writer = csv.writer(stream)
        self._owns_stream = owns_stream
        self._on_close = on_close
        self._columns: list[str] = []
        self._closed = False

    def write_header(self, columns: list[str]) -> None:
        self._columns = list(columns)
        self._writer.writerow(self._columns)

    def write_row(self, row: Mapping[str, object]) -> None:
        self._writer.writerow([_csv_cell(row.get(name)) for name in self._columns])

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._stream.flush()
        if self._on_close is not None:
            self._on_close()
        if self._owns_stream:
            self._stream.close()

    @classmethod
    def for_local_path(cls, path: str) -> "CsvRowWriter":
        """Create a CSV writer that writes to (and owns) a local file at ``path``."""
        stream = open(path, "w", newline="", encoding="utf-8")  # noqa: SIM115
        return cls(stream, owns_stream=True)


# ---------------------------------------------------------------------------
# Keyset streaming (read-only)
# ---------------------------------------------------------------------------


# The MySQL source dialect: the default for the source-reading helpers below
# (identifier quoting, integer-PK types). Threading a dialect through them keeps one
# source of truth for the source-engine SQL and lets a non-MySQL source supply its
# own quoting/types later without touching these call sites.
_MYSQL_DIALECT = MySQLSourceDialect()


def _quote_mysql_identifier(name: str) -> str:
    """Quote a MySQL identifier with backticks (delegates to the MySQL dialect)."""
    return _MYSQL_DIALECT.quote_identifier(name)


def _quote_mysql_table(name: str) -> str:
    """Quote a possibly schema-qualified MySQL table name (delegates to the dialect)."""
    return _MYSQL_DIALECT.quote_table(name)


def _primary_key_columns(table: TableDef) -> list[str]:
    """Return the primary-key columns (one or more), or raise for a missing key.

    Keyset export streams in ascending primary-key order. A single-column key uses
    ``pk > :last``; a composite key uses the explicit lexicographic disjunction
    ``(k0 > :last_0) OR (k0 = :last_0 AND k1 > :last_1) OR ...`` which uses the
    primary-key index on MySQL 5.7+ (the row-value tuple form does not), giving the
    same stable, resumable read order. Only a
    MISSING primary key is unsupported (no deterministic read order exists).
    """
    primary_key = list(table.primary_key)
    if not primary_key:
        raise UnsupportedPrimaryKeyError(
            f"table '{table.name}' has no primary key; keyset export requires a "
            "primary key to define a stable read order"
        )
    return primary_key


class ExportCancelled(ExportError):
    """Raised when a keyset stream is stopped early via its ``should_cancel`` hook.

    Lets a cooperative stop interrupt the row pull *between pages* (not only
    between load batches), so a Stop takes effect promptly even while rows are
    being read; the caller treats it like any other cooperative stop (the table
    is left incomplete and retryable, the re-load is idempotent).
    """


def shardable_leading_int_pk(
    table: TableDef, dialect: "SourceDialect" = _MYSQL_DIALECT
) -> Optional[str]:
    """Return the LEADING PK column name if it is an integer type, else None.

    Reader range sharding (splitting a big table into K disjoint ranges read
    concurrently) bands the LEADING primary-key column. It is safe whenever that
    leading column is an integer, for BOTH a single integer PK AND a COMPOSITE PK
    whose first column is an integer (e.g. ``(tenant_id, id)``): integer columns are
    collation-free, so MIN/MAX arithmetic yields interior boundaries strictly
    increasing in MySQL's numeric comparison order -- the exact order the reader's
    ``WHERE`` / ``ORDER BY`` use -- making the K ranges provably disjoint and
    covering. The band is on the leading value only, so all rows sharing a leading
    value co-locate in one shard (a composite key is never split across shards) and
    the within-shard keyset walk still uses the full composite cursor. A non-integer
    leading column returns None -> one reader (always correct, just not parallel).
    """
    pk = list(table.primary_key)
    if not pk:
        return None
    leading = pk[0]
    for column in table.columns:
        if column.name == leading:
            base = column.mysql_type.split("(")[0].strip().lower().split()[0]
            return leading if base in dialect.integer_pk_types else None
    return None


def compute_pk_shard_ranges(
    connection: _Connection,
    table: TableDef,
    shards: int,
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> list[tuple[Optional[int], Optional[int]]]:
    """Return ``shards`` half-open ``[lo, hi)`` ranges over the LEADING PK column.

    Reads ``MIN`` / ``MAX`` (index-only, read-only, Property 1) of the LEADING PK
    column -- when it is an integer (single or composite-leading) -- and splits
    ``[min, max]`` into ``shards`` contiguous slices.
    The first range's ``lo`` is ``None`` (open start) and the last range's ``hi``
    is ``None`` (open end) so the union is guaranteed to cover every row -- even
    rows inserted outside the sampled [min,max] between this call and the read
    (the read snapshot fixes the set, but the open ends are belt-and-suspenders).
    Falls back to a single ``(None, None)`` range (one reader, whole table) when
    the PK isn't a shardable integer, the table is empty, or ``shards <= 1``.

    The split is **key-domain-uniform** (``[MIN,MAX]`` divided into equal PK-value
    bands), which balances the shards well for a dense/monotonic key (e.g. AUTO_
    INCREMENT). For a sparse or clustered key the bands can hold uneven row counts
    (one hot shard, near-empty others) so the speedup is less than K -- it never
    hurts correctness (ranges stay disjoint + covering), only balance.
    """
    pk_col = shardable_leading_int_pk(table, dialect)
    if pk_col is None or shards <= 1:
        return [(None, None)]
    quoted_col = dialect.quote_identifier(pk_col)
    quoted_table = dialect.quote_table(table.name)
    row = connection.execute(
        text(f"SELECT MIN({quoted_col}) AS lo, MAX({quoted_col}) AS hi "
             f"FROM {quoted_table}")
    ).mappings().first()
    if not row or row["lo"] is None or row["hi"] is None:
        return [(None, None)]
    lo, hi = int(row["lo"]), int(row["hi"])
    span = hi - lo + 1  # inclusive count of the key domain
    if span <= shards:
        return [(None, None)]  # too small to bother splitting
    step = span // shards
    ranges: list[tuple[Optional[int], Optional[int]]] = []
    for i in range(shards):
        r_lo: Optional[int] = None if i == 0 else lo + i * step
        r_hi: Optional[int] = None if i == shards - 1 else lo + (i + 1) * step
        ranges.append((r_lo, r_hi))
    return ranges


# ---------------------------------------------------------------------------
# Source-load governor (opt-in proactive read throttle)
# ---------------------------------------------------------------------------

# Cache the source Threads_running reading between page polls so the extra status
# query is negligible even across many concurrent readers; while PAUSED, re-read
# each wait slice so the pause ends as soon as the metric recedes.
_GOVERNOR_STATUS_TTL_SECONDS = 2.0
# Sliced wait so a Stop is honored within one slice, not after a long pause.
_GOVERNOR_WAIT_SLICE_SECONDS = 1.0


def _read_threads_running(connection: _Connection) -> Optional[int]:
    """Read the source's global ``Threads_running``, or ``None`` on any failure.

    ``SHOW GLOBAL STATUS`` reads live server state (not the snapshot), is read-only
    (Property 1), and works inside the export's consistent-snapshot transaction. Any
    failure or malformed value returns ``None`` so a broken status read can NEVER
    stall the load (fail-open) -- the governor treats ``None`` as "don't throttle".
    """
    try:
        row = connection.execute(
            text("SHOW GLOBAL STATUS LIKE 'Threads_running'")
        ).first()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - best-effort; never fail the load on a status read
        return None
    if row is None:
        return None
    try:
        return int(row[1])
    except (TypeError, ValueError, IndexError):
        return None


class SourceLoadGovernor:
    """Opt-in proactive throttle that pauses Full Load reads on a loaded source.

    When ``max_threads_running`` is set, :func:`keyset_stream` calls
    :meth:`throttle` before fetching each page: while the source's global
    ``Threads_running`` exceeds the ceiling the read PAUSES (a sliced,
    Stop-responsive wait) and resumes when the metric recedes. Because the
    migration's own readers count toward ``Threads_running``, this effectively caps
    the source's active-query concurrency at ~the ceiling -- protecting a
    live-serving source (gh-ost ``--max-load`` style). It NEVER fails the load
    (pause-only) and a failed status read is fail-open (treated as "don't
    throttle"). With ``max_threads_running=None`` :meth:`throttle` is a no-op, so a
    load that does not opt in pays ZERO overhead.

    ``sleep`` / ``monotonic`` are injectable for tests; ``on_state_change`` (if
    given) is called once on each pause<->resume transition with
    ``(paused, threads_running)`` so a caller can surface the state (it is logged
    regardless).
    """

    def __init__(
        self,
        connection: _Connection,
        max_threads_running: Optional[int],
        *,
        sleep: Optional[Callable[[float], None]] = None,
        monotonic: Callable[[], float] = _wall_monotonic,
        ttl_seconds: float = _GOVERNOR_STATUS_TTL_SECONDS,
        slice_seconds: float = _GOVERNOR_WAIT_SLICE_SECONDS,
        on_state_change: Optional[Callable[[bool, Optional[int]], None]] = None,
    ) -> None:
        self._connection = connection
        # Normalize the ceiling: None / 0 / negative all mean OFF (0 is the config's
        # "off" sentinel), so the rest of the class only checks ``is None``.
        self._ceiling = (
            max_threads_running
            if max_threads_running and max_threads_running > 0
            else None
        )
        # None -> _wall_sleep resolved at call time (so a test can monkeypatch it on
        # the governor built internally by stream_converted_rows), like the validator's
        # reconnect proxy.
        self._sleep = sleep
        self._monotonic = monotonic
        self._ttl = ttl_seconds
        self._slice = slice_seconds
        self._on_state_change = on_state_change
        self._cached_value: Optional[int] = None
        self._cached_at: Optional[float] = None
        self._paused = False

    @property
    def enabled(self) -> bool:
        """True when a ceiling is set (otherwise :meth:`throttle` is a no-op)."""
        return self._ceiling is not None

    def _threads_running(self, *, fresh: bool) -> Optional[int]:
        now = self._monotonic()
        if (
            not fresh
            and self._cached_at is not None
            and (now - self._cached_at) < self._ttl
        ):
            return self._cached_value
        value = _read_threads_running(self._connection)
        self._cached_value = value
        self._cached_at = now
        return value

    def _set_paused(self, paused: bool, running: Optional[int]) -> None:
        if paused == self._paused:
            return
        self._paused = paused
        if paused:
            _LOGGER.warning(
                "Full Load paused: source Threads_running=%s exceeds the configured "
                "ceiling %s -- waiting for source load to recede.",
                running, self._ceiling,
            )
        else:
            _LOGGER.info(
                "Full Load resumed: source Threads_running=%s is at/below the "
                "ceiling %s.", running, self._ceiling,
            )
        if self._on_state_change is not None:
            self._on_state_change(paused, running)

    def throttle(self, should_cancel: Optional[Callable[[], bool]] = None) -> None:
        """Block (sliced) while the source is over the ceiling; no-op if disabled.

        Returns promptly when the metric is at/below the ceiling, when the ceiling
        is unset, on a status-read failure (fail-open), or when ``should_cancel``
        fires (the caller re-polls it and raises :class:`ExportCancelled`). Never
        raises -- throttling must never itself fail the load.
        """
        if self._ceiling is None:
            return
        fresh = False
        while True:
            running = self._threads_running(fresh=fresh)
            if running is None or running <= self._ceiling:
                self._set_paused(False, running)
                return
            self._set_paused(True, running)
            if should_cancel is not None and should_cancel():
                return  # caller re-polls should_cancel -> ExportCancelled
            (self._sleep or _wall_sleep)(self._slice)
            fresh = True  # re-read the metric each slice while paused


def keyset_stream(
    connection: _Connection,
    table: TableDef,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    should_cancel: Optional[Callable[[], bool]] = None,
    pk_lower: Optional[int] = None,
    pk_upper: Optional[int] = None,
    governor: Optional["SourceLoadGovernor"] = None,
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> Iterator[Mapping[str, object]]:
    """Yield ``table`` rows in ascending primary-key order via keyset pagination.

    Issues ``SELECT <cols> FROM <table> [WHERE pk > :last] ORDER BY pk LIMIT
    :batch_size`` repeatedly, advancing ``:last`` to the last row's primary key,
    until a short page signals exhaustion. Rows are yielded lazily one page at a
    time, so the whole table is never materialized (Requirement 5.1). Every
    statement is a ``SELECT`` (read-only, Property 1). Raises
    :class:`UnsupportedPrimaryKeyError` for missing/composite primary keys.

    ``should_cancel`` (optional) is polled before each page is fetched; when it
    returns ``True`` the stream raises :class:`ExportCancelled` instead of issuing
    the next ``SELECT``, so a cooperative stop interrupts the read promptly rather
    than only between load batches.

    ``pk_lower`` / ``pk_upper`` (optional) bound the read to the half-open range
    ``[pk_lower, pk_upper)`` on the **LEADING** PK column — the mechanism behind
    reader range sharding, where K readers each stream a disjoint slice of a large
    table concurrently. It applies to a single OR composite key (sharding sets it
    only for an INTEGER leading column): for a composite key the leading-column band
    ANDs with the disjunction cursor below, so every row sharing a leading value
    stays in one slice (a composite key is never split). The bound uses the PK index
    (leading column is the index prefix) and preserves ascending-PK order within the
    slice. ``pk_upper=None`` means "to the end" (the last shard); ``pk_lower=None``
    means "from the start" (the first shard).

    ``governor`` (optional) is the opt-in :class:`SourceLoadGovernor`; when set it is
    asked to :meth:`~SourceLoadGovernor.throttle` at the SAME pre-page poll point as
    ``should_cancel``, so a loaded source pauses the read between pages (never
    mid-page) and resumes when it recedes. ``None`` (the default) adds no overhead.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be a positive integer")

    pk_columns = _primary_key_columns(table)
    column_names = [column.name for column in table.columns]
    if not column_names:
        raise ExportError(f"table '{table.name}' has no columns to export")

    columns_sql = ", ".join(
        dialect.select_column_sql(column) for column in table.columns
    )
    table_sql = dialect.quote_table(table.name)
    order_by_sql = ", ".join(dialect.quote_identifier(c) for c in pk_columns)

    # Optional range bound on the LEADING PK column (reader sharding). Applies to a
    # single OR composite key: for a composite key it bands the leading (index-prefix)
    # column and ANDs with the disjunction cursor below, so every row sharing a leading
    # value stays in ONE shard (a composite key is never split across shards). Sharding
    # only sets these for an INTEGER leading column (shardable_leading_int_pk), whose
    # numeric-order boundaries keep the K ranges disjoint + covering.
    range_params: dict[str, object] = {}
    range_clauses: list[str] = []
    if pk_lower is not None or pk_upper is not None:
        pk_sql_bound = dialect.quote_identifier(pk_columns[0])
        if pk_lower is not None:
            range_clauses.append(f"{pk_sql_bound} >= :pk_lower")
            range_params["pk_lower"] = pk_lower
        if pk_upper is not None:
            range_clauses.append(f"{pk_sql_bound} < :pk_upper")
            range_params["pk_upper"] = pk_upper

    if len(pk_columns) == 1:
        # Single-column key: scalar ``pk > :last`` (one bind param ``last``).
        pk_sql = dialect.quote_identifier(pk_columns[0])
        where_sql = f"{pk_sql} > :last"
        last_param_names = ["last"]
    else:
        # Composite key: the EXPLICIT lexicographic keyset expansion
        #   (k0 > :last_0) OR (k0 = :last_0 AND k1 > :last_1) OR ...
        # rather than the row-value form ``(k0, k1, ...) > (:last_0, ...)``. Both are
        # lexicographically equivalent, but the row-value comparison only gets a PK index
        # range scan on MySQL 8.0.14+; on a 5.7-compatible source (RDS MySQL 5.7 / Aurora
        # MySQL 2 -- both supported) it degrades to a FULL TABLE SCAN per page, making the
        # whole keyset export O(n^2). The disjunction uses the PK index on every version. The
        # named params (:last_i) are REUSED across terms, so the bind set is unchanged (one
        # value per key column) and the param binding below is untouched.
        key_sql_cols = [dialect.quote_identifier(c) for c in pk_columns]
        last_param_names = [f"last_{i}" for i in range(len(pk_columns))]
        terms: list[str] = []
        for i in range(len(pk_columns)):
            prefix_eq = [
                f"{key_sql_cols[j]} = :{last_param_names[j]}" for j in range(i)
            ]
            strict_gt = f"{key_sql_cols[i]} > :{last_param_names[i]}"
            terms.append(" AND ".join([*prefix_eq, strict_gt]))
        where_sql = "(" + " OR ".join(f"({term})" for term in terms) + ")"

    # The first page has no keyset cursor yet, so its WHERE is only the range
    # bound (if any); later pages AND the cursor with the range bound.
    first_where = " AND ".join(range_clauses)
    next_where = " AND ".join([where_sql, *range_clauses])
    first_page_sql = (
        f"SELECT {columns_sql} FROM {table_sql}"
        f"{(' WHERE ' + first_where) if first_where else ''} "
        f"ORDER BY {order_by_sql} LIMIT :batch_size"
    )
    next_page_sql = (
        f"SELECT {columns_sql} FROM {table_sql} WHERE {next_where} "
        f"ORDER BY {order_by_sql} LIMIT :batch_size"
    )

    def _key_of(row: Mapping[str, object]) -> tuple:
        return tuple(row[c] for c in pk_columns)

    last_key: Optional[tuple] = None
    page_index = 0
    while True:
        # Poll the cooperative stop before fetching each page so a Stop interrupts
        # the pull between pages (not only between load batches). The partial read
        # is fine: the table is left incomplete and the idempotent re-load resumes.
        if should_cancel is not None and should_cancel():
            raise ExportCancelled(table.name)
        # Opt-in source-load throttle at the SAME between-pages point: pause while the
        # source is over its Threads_running ceiling (no-op when not configured). A
        # pause honors Stop within a slice, so re-poll the cancel after it returns.
        if governor is not None:
            governor.throttle(should_cancel)
            if should_cancel is not None and should_cancel():
                raise ExportCancelled(table.name)
        if last_key is None:
            statement = text(first_page_sql)
            params: dict[str, object] = {"batch_size": batch_size, **range_params}
        else:
            statement = text(next_page_sql)
            params = {"batch_size": batch_size, **range_params}
            for name, value in zip(last_param_names, last_key):
                params[name] = value

        result = connection.execute(statement, params)
        page_index += 1
        page_count = 0
        first_key: Optional[tuple] = None
        for row in result.mappings():  # type: ignore[attr-defined]
            page_count += 1
            key = _key_of(row)
            if first_key is None:
                first_key = key
            last_key = key
            yield row

        # One DEBUG line PER PAGE (never per row): PK range + count, so the read
        # order is observable at large scale without O(#rows) log volume. Guarded so
        # production (INFO) builds nothing. PK values + count only (Property 7).
        if _LOGGER.isEnabledFor(logging.DEBUG):
            single = len(pk_columns) == 1
            pk_repr = pk_columns[0] if single else pk_columns
            lo = first_key[0] if (single and first_key is not None) else first_key
            hi = last_key[0] if (single and last_key is not None) else last_key
            _LOGGER.debug(
                "export keyset page table=%s page=%d pk=%s range=[%s..%s] rows=%d",
                table.name, page_index, pk_repr, lo, hi, page_count,
            )

        if page_count < batch_size:
            return


# ---------------------------------------------------------------------------
# Exporter
# ---------------------------------------------------------------------------


class TableExporter:
    """Exports a source table to a file via read-only PK keyset streaming.

    The engine factory is injectable (like
    :class:`~dsql_migrator.core.introspector.SourceIntrospector`) so tests can
    supply a fake connection; the default reuses the introspector's MySQL factory
    which installs the read-only guard (Property 1).
    """

    def __init__(
        self,
        engine_factory: Optional[Callable[[SourceConnectionConfig], Engine]] = None,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_source_threads_running: Optional[int] = None,
    ) -> None:
        """Create an exporter with an optional engine factory and page size.

        ``max_source_threads_running`` (optional) opts the streaming read into the
        :class:`SourceLoadGovernor`: when set, :meth:`stream_converted_rows` pauses
        between pages while the source's ``Threads_running`` exceeds it. ``None``
        (the default) = no throttle, zero overhead.
        """
        self._engine_factory = engine_factory or _default_engine_factory
        self._batch_size = batch_size
        self._max_source_threads_running = max_source_threads_running

    def export_table(
        self,
        conn: SourceConnectionConfig,
        table: TableDef,
        writer: RowWriter,
        *,
        target_types: Optional[Mapping[str, str]] = None,
    ) -> int:
        """Stream ``table`` to ``writer`` from a consistent read-only snapshot.

        Opens the source, runs the keyset stream inside ``START TRANSACTION WITH
        CONSISTENT SNAPSHOT`` in autocommit mode (so the explicit transaction
        controls the snapshot, matching watermark capture), converts each row's
        values with :class:`ValueConverter`, and writes them as they stream
        (Requirement 5.1 / Property 1). The caller owns ``writer`` and is
        responsible for closing it. Returns the number of rows exported.
        """
        dialect = dialect_for(conn.source_type)
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                snapshot = connection.execution_options(
                    isolation_level="AUTOCOMMIT", stream_results=True
                )
                return export_rows(
                    snapshot,
                    table,
                    writer,
                    batch_size=self._batch_size,
                    target_types=target_types,
                    dialect=dialect,
                )
        finally:
            engine.dispose()

    def plan_pk_shard_ranges(
        self,
        conn: SourceConnectionConfig,
        table: TableDef,
        shards: int,
        *,
        min_rows: int = 0,
    ) -> list[tuple[Optional[int], Optional[int]]]:
        """Compute up to ``shards`` half-open PK ranges for ``table`` (read-only).

        Opens a short read-only connection and, for a single integer PK, delegates
        to :func:`compute_pk_shard_ranges` (one ``MIN/MAX`` query). Returns
        ``[(None, None)]`` — one reader over the whole table, i.e. the current
        behavior — whenever the table can't or shouldn't be sharded: ``shards <=
        1``, a composite/non-integer PK, an empty table, or (when ``min_rows`` > 0)
        a scan-free ``information_schema`` row estimate BELOW ``min_rows`` (small
        tables don't justify the extra connections/snapshots). Called once per
        table before the shard readers open their own snapshots.
        """
        dialect = dialect_for(conn.source_type)
        if shards <= 1 or shardable_leading_int_pk(table, dialect) is None:
            return [(None, None)]
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                ro = connection.execution_options(isolation_level="AUTOCOMMIT")
                if min_rows > 0:
                    est = estimate_source_rows(ro, [table.name], dialect).get(table.name)
                    if est is not None and est < min_rows:
                        return [(None, None)]
                return compute_pk_shard_ranges(ro, table, shards, dialect)
        finally:
            engine.dispose()

    def export_table_to_csv_path(
        self,
        conn: SourceConnectionConfig,
        table: TableDef,
        path: str,
    ) -> int:
        """Export ``table`` to a local CSV file at ``path`` (manages the writer)."""
        writer = CsvRowWriter.for_local_path(path)
        try:
            return self.export_table(conn, table, writer)
        finally:
            writer.close()

    def stream_converted_rows(
        self,
        conn: SourceConnectionConfig,
        table: TableDef,
        *,
        should_cancel: Optional[Callable[[], bool]] = None,
        target_types: Optional[Mapping[str, str]] = None,
        pk_lower: Optional[int] = None,
        pk_upper: Optional[int] = None,
        on_throttle: Optional[Callable[[bool, Optional[int]], None]] = None,
    ) -> "Iterator[Mapping[str, object]]":
        """Yield target-ready (converted) rows from a read-only consistent snapshot.

        The in-process importer pulls these straight into batched ``INSERT``s with
        no CSV/S3 staging. Mirrors :meth:`export_table` -- one ``START
        TRANSACTION WITH CONSISTENT SNAPSHOT`` keyset stream with a per-row
        :class:`ValueConverter` -- but is pull-based: rows are produced lazily so
        only one page is held at a time (Requirement 5.1), and the source is only
        ever read (Property 1). The snapshot transaction is held open for the
        table's read and committed when the generator is exhausted or closed.

        ``should_cancel`` (optional) is forwarded to :func:`keyset_stream`, which
        polls it before each page so a cooperative stop interrupts the read
        between pages (raising :class:`ExportCancelled`) instead of waiting for
        the next load batch.

        ``pk_lower`` / ``pk_upper`` (optional, single integer PK only) bound this
        stream to a half-open PK slice ``[pk_lower, pk_upper)`` -- one shard of a
        range-sharded read. Each shard opens its OWN snapshot connection here (the
        shards are disjoint, so their independently-timed snapshots never overlap a
        row); see :func:`compute_pk_shard_ranges`.
        """
        dialect = dialect_for(conn.source_type)
        converter = dialect.value_converter(table, target_types=target_types)
        engine = self._engine_factory(conn)
        try:
            with engine.connect() as connection:
                snapshot = connection.execution_options(
                    isolation_level="AUTOCOMMIT", stream_results=True
                )
                snapshot.execute(text(dialect.snapshot_start_sql))
                governor = (
                    SourceLoadGovernor(
                        snapshot,
                        self._max_source_threads_running,
                        on_state_change=on_throttle,
                    )
                    if self._max_source_threads_running
                    else None
                )
                try:
                    for raw in keyset_stream(
                        snapshot,
                        table,
                        batch_size=self._batch_size,
                        should_cancel=should_cancel,
                        pk_lower=pk_lower,
                        pk_upper=pk_upper,
                        governor=governor,
                        dialect=dialect,
                    ):
                        yield converter.convert_row(raw)
                finally:
                    snapshot.execute(text(COMMIT))
        finally:
            engine.dispose()


def export_rows(
    connection: _Connection,
    table: TableDef,
    writer: RowWriter,
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    target_types: Optional[Mapping[str, str]] = None,
    dialect: "SourceDialect" = _MYSQL_DIALECT,
) -> int:
    """Stream-convert-write ``table`` rows on an open connection (read-only).

    Wraps the keyset stream in a consistent-snapshot transaction, writes the
    header, then converts and writes each row as it arrives. Returns the row
    count. The transaction is committed even if writing fails; the caller owns
    the ``writer`` lifecycle. ``dialect`` (default MySQL) supplies the snapshot SQL,
    the per-row value converter, and the keyset quoting.
    """
    value_converter = dialect.value_converter(table, target_types=target_types)
    column_names = [column.name for column in table.columns]

    connection.execute(text(dialect.snapshot_start_sql))
    rows_exported = 0
    try:
        writer.write_header(column_names)
        for row in keyset_stream(
            connection, table, batch_size=batch_size, dialect=dialect
        ):
            writer.write_row(value_converter.convert_row(row))
            rows_exported += 1
    finally:
        connection.execute(text(COMMIT))
    return rows_exported


__all__ = [
    "DEFAULT_BATCH_SIZE",
    "ExportError",
    "ExportCancelled",
    "UnsupportedPrimaryKeyError",
    "ValueConverter",
    "RowWriter",
    "CsvRowWriter",
    "keyset_stream",
    "export_rows",
    "TableExporter",
    "SourceLoadGovernor",
]
