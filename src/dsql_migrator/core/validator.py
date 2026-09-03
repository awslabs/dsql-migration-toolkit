# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Consistency validation of migrated data (design.md "6. Validator").

The :class:`Validator` compares a migrated DSQL target against its MySQL source
and produces a :class:`~dsql_migrator.core.models.ValidationReport`
(Requirement 6). It implements:

- Per-table row-count comparison (Requirement 6.1).
- Sample/checksum-based data comparison (Requirement 6.2): in
  :attr:`~dsql_migrator.core.models.ValidationMode.CHECKSUM` mode an
  order-independent per-table checksum is computed on both sides and compared, so
  a reported match means the data itself is equal FOR EVERY COMPARED COLUMN. Note
  FLOAT/DOUBLE and JSON columns are NOT value-compared (no byte-identical
  cross-engine form -- see the checksum note below); each table lists them in
  ``checksum_excluded_columns`` so a match is not read as "every column verified".
- Optional orphan-record check (Requirement 6.3): Aurora DSQL now enforces foreign
  keys, and the tool re-creates them after the load (at cut over for a CDC
  migration). This check is the PRE-APPLY GATE for that step -- an enforced
  ``ADD CONSTRAINT`` fails if any child row has no matching parent -- and the
  integrity safety net when the user chose to strip foreign keys instead. It counts,
  for each preserved foreign-key rule
  (:class:`~dsql_migrator.core.models.ForeignKeyDef`), the child rows on the target
  whose (non-null) key has no matching parent row.
- Validation report including mismatches (Requirement 6.4).
- As-of-watermark validation with drift reporting (Requirement 6.5): when a
  :class:`~dsql_migrator.core.models.Watermark` is supplied, per-table source row
  counts are taken from the watermark snapshot (the consistency point) and the
  current source GTID is compared to the watermark's to report changes since the
  snapshot (Property 11).

Two correctness properties are central here:

- **Validation soundness (Property 9).** ``matched``/``is_match`` are computed so
  that a reported match can never hide an unequal row count or checksum: a table
  is ``matched`` only when its counts are equal and (in CHECKSUM mode) its
  checksums are equal, and the report ``is_match`` only when every table matched
  and no orphans were found. This is sound by construction
  (:meth:`~dsql_migrator.core.models.ValidationReport.build`).
- **Read-only source (Property 1).** Every source statement is a ``SELECT`` or
  transaction-control statement issued inside a single
  ``START TRANSACTION WITH CONSISTENT SNAPSHOT`` ... ``COMMIT`` (the same
  read-only-guarded engine used by introspection/export). The target is only
  ever read with ``SELECT`` as well.

Dependencies are injectable -- the source engine factory (like
:class:`~dsql_migrator.core.introspector.SourceIntrospector`) and the target
connection factory (like :mod:`dsql_migrator.core.batched_import`) -- so unit
tests never reach a real MySQL or DSQL cluster.

Cross-engine checksum note: the default MySQL and PostgreSQL checksum SQL
(:func:`build_mysql_checksum_sql` / :func:`build_pg_checksum_sql`) both reduce a
row to an order-independent sum of a 60-bit MD5 prefix, chosen to stay in a
positive range on both engines. Per-column text rendering is now engine-
NORMALIZED so equal data hashes equally on both sides: the NULL sentinel, bytea /
spatial WKB, BIT(n), boolean, and the temporal (timestamp / timestamptz / time)
and DECIMAL type-classes each render to the SAME canonical text on MySQL and
PostgreSQL (driven by the same converter classification the Full Load loader used
to STORE the value). The exceptions are FLOAT / DOUBLE and JSON: no byte-identical
cross-engine text form exists (a float has no exact shortest-round-trip decimal and
any fixed-precision rounding would degrade soundness for exact values; MySQL's
canonical JSON differs from the CDC sink's compact serialization), so those columns
are intentionally EXCLUDED from the checksum concatenation entirely
(:func:`_checksum_kind` returns ``"float"``/``"json"`` and both renderers return
``None`` for them). IMPORTANT: a difference confined to a NON-KEY float/double/json
value is therefore NOT detected by any mode -- the row count is unchanged by an
in-place value edit and reconciliation compares primary-key presence, not values. To
keep that honest rather than a silent blind spot, each :class:`TableValidationResult`
records the omitted columns in ``checksum_excluded_columns`` and the report/UI surface
them, so a CHECKSUM match is read as "every column EXCEPT these was value-compared",
not "every column verified". The checksum itself stays SOUND (a reported match always
means the two computed checksums were equal over the compared columns).
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable, Iterator, Mapping, Optional, Protocol

from psycopg import sql
from sqlalchemy import text
from sqlalchemy.engine import Engine

from dsql_migrator.core.introspector import _default_engine_factory
from dsql_migrator.core.models import (
    ColumnDef,
    DriftReport,
    ForeignKeyDef,
    OrphanFinding,
    ReconcileResult,
    RowDiffFinding,
    RowDiffKind,
    RowDiffSample,
    SourceConnectionConfig,
    SourceType,
    TableDef,
    TargetConnectionConfig,
    ValidationMode,
    ValidationReport,
    Watermark,
    TableValidationResult,
)
from dsql_migrator.core.target_connection import (
    DsqlConnector,
    is_transient_connection_error,
)
from dsql_migrator.core.source_dialect import SourceDialect, dialect_for
from dsql_migrator.core.validator_postgres import PgSourceConnection
from dsql_migrator.core.watermark import COMMIT, read_binlog_status_row

# The pure cross-engine SQL builders + PK-classification helpers were extracted to
# validation_sql.py for maintainability. Re-exported here so existing imports
# (`from dsql_migrator.core.validator import build_mysql_checksum_sql`, used by tests)
# and this module's own DB-access layer keep resolving unchanged.
from dsql_migrator.core.validation_sql import (  # noqa: F401
    _CHECKSUM_HEX_DIGITS,
    _INTEGER_BASE_TYPES,
    _NULL_SENTINEL,
    _checksum_kind,
    _decimal_scale,
    _mysql_checksum_expr,
    _mysql_concat_term,
    _pg_checksum_expr,
    _pg_concat_term,
    _pg_numeric_mask,
    _pg_table_identifier,
    _quote_mysql_identifier,
    _quote_mysql_table,
    build_mysql_checksum_sql,
    build_mysql_page_checksum_first_sql,
    build_mysql_page_checksum_next_sql,
    build_mysql_pk_first_page_sql,
    build_mysql_pk_next_page_sql,
    build_mysql_pk_token_sql,
    build_orphan_count_sql,
    build_pg_checksum_sql,
    build_pg_orphan_page_first_sql,
    build_pg_orphan_page_next_sql,
    build_pg_page_checksum_first_sql,
    build_pg_page_checksum_next_sql,
    build_pg_pk_first_page_sql,
    build_pg_pk_next_page_sql,
    build_pg_pk_token_sql,
    integer_pk_column,
    single_pk_column,
)


class _SourceConnection(Protocol):
    """Minimal SQLAlchemy-style source connection used by the read helpers."""

    def execute(self, statement: object, parameters: object = ...) -> object: ...


# A source engine factory builds a read-only-guarded SQLAlchemy engine for a
# connection config (default reuses the introspector's MySQL factory).
SourceEngineFactory = Callable[[SourceConnectionConfig], Engine]

# A target connection factory opens one psycopg-style DSQL connection for a
# target config. Injectable so tests never reach a real cluster.
TargetConnectionFactory = Callable[[TargetConnectionConfig], Any]


def _norm_pk(value: object) -> str:
    """Canonical primary-key string for cross-engine set comparison (int-safe).

    MySQL and psycopg can return a PK as ``int`` / ``Decimal`` / ``str`` depending
    on the column type; normalizing integral values to a plain base-10 string keeps
    a PK that is present on both sides from being spuriously classified as
    missing/extra purely due to representation (mirrors ``max_pk_source``).
    """
    try:
        return str(int(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return str(value)


# ---------------------------------------------------------------------------
# Source reads (SQLAlchemy, read-only)
# ---------------------------------------------------------------------------


def _source_count(connection: _SourceConnection, table_name: str) -> int:
    """Return ``COUNT(*)`` for a source table (read-only ``SELECT``)."""
    statement = text(
        f"SELECT COUNT(*) FROM {_quote_mysql_table(table_name)}"
    )
    value = connection.execute(statement).scalar()  # type: ignore[attr-defined]
    return int(value) if value is not None else 0


def _source_checksum(
    connection: _SourceConnection, table: TableDef, page_size: int
) -> str:
    """Return the source checksum for ``table`` as a string (read-only).

    For a single-column PK the checksum is accumulated over bounded keyset pages
    (:func:`_source_checksum_keyset`) so a billion-row source is never summed in one
    long-held read view; a composite/missing PK keeps the single whole-table scan
    (MySQL has no per-statement transaction limit, so the fallback is safe).
    """
    pk_column = single_pk_column(table)
    if pk_column is not None:
        return _source_checksum_keyset(connection, table, pk_column, page_size)
    value = connection.execute(text(build_mysql_checksum_sql(table))).scalar()  # type: ignore[attr-defined]
    return "0" if value is None else str(value)


def _source_checksum_keyset(
    connection: _SourceConnection, table: TableDef, pk_column: str, page_size: int
) -> str:
    """Accumulate the source checksum over bounded keyset pages (single-column PK).

    Each page sums the SAME per-row token as :func:`build_mysql_checksum_sql` over
    only ``page_size`` rows (``WHERE pk > :last ORDER BY pk LIMIT N``); the per-page
    sub-sums are folded into a Python integer. Because the token is per-row and SUM is
    order-independent, the accumulated total equals the whole-table checksum exactly,
    with every statement bounded and memory at one row. Reads only the PK + token.
    """
    first_sql = text(build_mysql_page_checksum_first_sql(table, pk_column))
    next_sql = text(build_mysql_page_checksum_next_sql(table, pk_column))
    total = 0
    last: object = None
    while True:
        if last is None:
            result = connection.execute(first_sql, {"page": page_size})  # type: ignore[attr-defined]
        else:
            result = connection.execute(next_sql, {"last": last, "page": page_size})  # type: ignore[attr-defined]
        row = None
        for candidate in result:  # single (sub_sum, last_pk, count) row
            row = candidate
            break
        if row is None:
            return str(total)
        sub_sum, last_pk, count = row[0], row[1], row[2]
        total += int(sub_sum) if sub_sum is not None else 0
        if count is None or int(count) < page_size:
            return str(total)
        last = last_pk


def _source_pk_tokens(
    connection: _SourceConnection, table: TableDef, pk_column: str, sample_size: int
) -> dict[str, str]:
    """Return up to ``sample_size`` ``{pk: token}`` entries for the source (read-only).

    Bounded by ``LIMIT :sample_size`` -- at most ``sample_size`` rows are read, in
    PK order, via the primary-key index. Both PK and token are stringified for
    cross-engine comparison against the target.
    """
    statement = text(build_mysql_pk_token_sql(table, pk_column))
    result = connection.execute(statement, {"sample_size": sample_size})  # type: ignore[attr-defined]
    return {_norm_pk(row[0]): str(row[1]) for row in result}


def _source_gtid(connection: _SourceConnection) -> Optional[str]:
    """Read the current source ``@@GLOBAL.gtid_executed`` for drift, or ``None``.

    A failure (e.g. GTID mode off or restricted) degrades gracefully to ``None``
    so validation still produces a report.
    """
    try:
        value = connection.execute(
            text("SELECT @@GLOBAL.gtid_executed")
        ).scalar()  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001 - drift info is optional; degrade gracefully
        return None
    if value is None:
        return None
    text_value = str(value)
    return text_value or None


def _source_binlog_position(
    connection: _SourceConnection,
) -> tuple[Optional[str], Optional[int]]:
    """Read the source's current binlog ``(file, position)``, or ``(None, None)``.

    The drift fallback for a source without GTID -- which is the normal case on RDS
    MySQL 8.0, where GTID cannot be enabled. Read-only (``SHOW BINARY LOG STATUS``
    on 8.2+/8.4, else ``SHOW MASTER STATUS``) and best-effort: any failure degrades
    to ``(None, None)`` so validation still produces a report, exactly like
    :func:`_source_gtid`.
    """
    row = read_binlog_status_row(connection)
    if not row:
        return None, None
    file_name = row.get("File") or None
    position = row.get("Position")
    try:
        return file_name, (int(position) if position is not None else None)
    except (TypeError, ValueError):  # non-numeric position: unusable for comparison
        return file_name, None


# ---------------------------------------------------------------------------
# Target reads (psycopg, read-only)
# ---------------------------------------------------------------------------


# Reconnect budget for the target read path. A DSQL connection is force-closed at
# the ~1h maximum connection duration, and a fresh reconnect can transiently hit
# DSQL's new-connection rate limit, so allow a few attempts with a short backoff.
_TARGET_RECONNECT_MAX_ATTEMPTS = 4
_TARGET_RECONNECT_BASE_DELAY_SECONDS = 0.5

_EXECUTE_SENTINEL = object()


class _ReconnectingTargetConnection:
    """Target-connection proxy that transparently reconnects on a transient
    (aged-out / dropped) DSQL connection and re-runs the in-flight statement.

    Validation's target reads -- count, checksum, keyset PK pages, orphan count --
    are read-only and idempotent, and the keyset pagers advance from the last PK, so
    a connection force-closed at DSQL's ~1h maximum connection duration (or dropped by
    a transient network / TLS event) partway through validating a large table is
    replaced with a fresh connection (which re-mints a short-lived IAM token) and only
    the failing statement is re-run -- the pager resumes from its last PK rather than
    rescanning. Without this, a >1h validation of a billion-row table (its keyset
    count + PK reconcile) permanently errors that table and blocks the cut-over gate;
    this is the same aged-connection class already hardened on the WRITE paths
    (``schema_applier._run_ddls_reconnecting`` and the batched loader's pool), which
    the read path lacked.

    Transparent: it quacks like a psycopg connection (``cursor()`` / ``close()``), so
    every ``_target_*`` read helper is unchanged. Only a connection-level transient
    error (SQLSTATE class ``08`` / no-SQLSTATE, via
    :func:`is_transient_connection_error`) triggers a reconnect; any real query /
    constraint error propagates unchanged. The underlying connection is opened EAGERLY
    so the open cost / an immediate connect failure surfaces at the same point as
    before this wrapper existed.
    """

    def __init__(
        self,
        factory: Callable[[], Any],
        *,
        max_attempts: int = _TARGET_RECONNECT_MAX_ATTEMPTS,
        base_delay: float = _TARGET_RECONNECT_BASE_DELAY_SECONDS,
        sleep: Optional[Callable[[float], None]] = None,
    ) -> None:
        self._factory = factory
        self._max_attempts = max(1, int(max_attempts))
        self._base_delay = max(0.0, float(base_delay))
        # None -> time.sleep, looked up dynamically at call time so tests can patch it.
        self._sleep = sleep
        self._connection: Any = factory()  # eager: preserve the pre-wrapper connect timing

    def _live(self) -> Any:
        if self._connection is None:
            self._connection = self._factory()
        return self._connection

    def _discard(self) -> None:
        if self._connection is not None:
            _safe_close(self._connection)
            self._connection = None

    def cursor(self) -> "_ReconnectingCursor":
        return _ReconnectingCursor(self)

    def close(self) -> None:
        self._discard()


class _ReconnectingCursor:
    """Cursor proxy for :class:`_ReconnectingTargetConnection`.

    ``execute`` runs on the owner's live connection; on a transient connection drop it
    discards the dead connection, reconnects (short bounded backoff), and re-runs the
    SAME statement on a fresh underlying cursor -- safe because every target read here
    is idempotent and the keyset pages carry their own ``WHERE pk > :last`` bound.
    ``fetchone`` / ``fetchall`` delegate to the live underlying cursor.
    """

    def __init__(self, owner: "_ReconnectingTargetConnection") -> None:
        self._owner = owner
        self._cursor: Any = None

    def execute(self, statement: Any, parameters: Any = _EXECUTE_SENTINEL) -> Any:
        attempt = 0
        while True:
            connection = self._owner._live()
            cursor = connection.cursor()
            try:
                if parameters is _EXECUTE_SENTINEL:
                    cursor.execute(statement)
                else:
                    cursor.execute(statement, parameters)
                self._cursor = cursor
                return cursor
            except Exception as exc:  # noqa: BLE001 - reconnect on a transient drop
                _safe_close(cursor)
                attempt += 1
                if (
                    is_transient_connection_error(exc)
                    and attempt < self._owner._max_attempts
                ):
                    # Discard the dead connection so the next _live() re-mints a fresh
                    # one (new IAM token), then replay the SAME statement.
                    self._owner._discard()
                    delay = self._owner._base_delay * attempt
                    if delay:
                        (self._owner._sleep or time.sleep)(delay)
                    continue
                raise

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> Any:
        return self._cursor.fetchall()

    def close(self) -> None:
        if self._cursor is not None:
            _safe_close(self._cursor)
            self._cursor = None


def _target_scalar(connection: Any, statement: Any) -> object:
    """Execute a read-only ``SELECT`` on the target and return the first column."""
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
        row = cursor.fetchone()
    finally:
        _safe_close(cursor)
    if row is None:
        return None
    return row[0]


def _target_count(connection: Any, table_name: str) -> int:
    """Return ``COUNT(*)`` for a target table (read-only ``SELECT``).

    A single unbounded scan: used ONLY as the fallback for a composite/missing PK
    (see :func:`_bounded_target_count`); on a very large such table it can still hit
    Aurora DSQL's 300s transaction limit -- a documented residual gap.
    """
    statement = sql.SQL("SELECT COUNT(*) FROM {table}").format(
        table=_pg_table_identifier(table_name)
    )
    value = _target_scalar(connection, statement)
    return int(value) if value is not None else 0


def _target_count_keyset(
    connection: Any, table: TableDef, pk_column: str, page_size: int
) -> int:
    """Exact target row count via BOUNDED keyset paging on a single-column PK.

    A single ``SELECT COUNT(*)`` scans the whole target in ONE transaction and, on a
    large table, exceeds Aurora DSQL's hard 300s transaction limit ("transaction age
    limit of 300s exceeded"), so a big table could otherwise never be validated. This
    pages the PK index (``WHERE pk > :last ORDER BY pk LIMIT N`` -- the same keyset
    stream reconciliation uses) and sums the per-page row counts, so every statement
    stays well under the limit and memory stays at one page. Reads only the PK column
    (Property 7); works for any orderable single-column PK (int/uuid/varchar/binary).
    """
    first_sql = build_pg_pk_first_page_sql(table, pk_column, page_size)
    next_sql = build_pg_pk_next_page_sql(table, pk_column, page_size)
    total = 0
    last: object = None
    while True:
        cursor = connection.cursor()
        try:
            if last is None:
                cursor.execute(first_sql)
            else:
                cursor.execute(next_sql, {"last": last})
            rows = cursor.fetchall()
        finally:
            _safe_close(cursor)
        total += len(rows)
        if len(rows) < page_size:
            return total
        last = rows[-1][0]


def _bounded_target_count(connection: Any, table: TableDef, page_size: int) -> int:
    """Target row count, bounded (keyset) for a single-column PK; ``COUNT(*)`` else.

    A composite/missing PK has no single keyset column, so it falls back to the
    unbounded ``COUNT(*)`` (which can still time out on a very large composite-PK
    table -- a residual gap not covered here).
    """
    pk_column = single_pk_column(table)
    if pk_column is not None:
        return _target_count_keyset(connection, table, pk_column, page_size)
    return _target_count(connection, table.name)


def _target_checksum(
    connection: Any, table: TableDef, page_size: int, source_is_postgres: bool = False
) -> str:
    """Return the target checksum for ``table`` as a string (read-only).

    For a single-column PK the checksum is accumulated over BOUNDED keyset pages
    (:func:`_target_checksum_keyset`) so a large DSQL table never sums in one
    transaction -- a single ``SELECT SUM(...)`` scan would exceed DSQL's hard 300s
    transaction limit and the table could never produce a checksum. A composite/missing
    PK falls back to the single-scan ``build_pg_checksum_sql`` (a documented residual,
    like :func:`_target_count`).

    ``source_is_postgres`` is forwarded to the PG checksum builders for the numeric-scale
    rule; it is ``True`` both when rendering the DSQL target of a PostgreSQL-source
    migration AND when this same function renders a PostgreSQL SOURCE (via
    :func:`_source_checksum_for`), so both ends of a PG migration agree.
    """
    pk_column = single_pk_column(table)
    if pk_column is not None:
        return _target_checksum_keyset(
            connection, table, pk_column, page_size, source_is_postgres
        )
    value = _target_scalar(
        connection, build_pg_checksum_sql(table, source_is_postgres)
    )
    return "0" if value is None else str(value)


def _target_checksum_keyset(
    connection: Any,
    table: TableDef,
    pk_column: str,
    page_size: int,
    source_is_postgres: bool = False,
) -> str:
    """Accumulate the target checksum over bounded keyset pages (single-column PK).

    The DSQL counterpart of :func:`_source_checksum_keyset`: each ``page_size``-row
    page sums the SAME per-row token as :func:`build_pg_checksum_sql` (so the
    accumulated total equals the whole-table checksum), keyset-advanced by
    ``MAX(page_pk)`` and terminated when a page returns fewer than ``page_size`` rows.
    Every statement stays well under DSQL's 300s limit; memory stays at one row. A
    connection force-closed at DSQL's ~1h limit is handled by the reconnecting proxy
    that wraps ``connection`` -- the failing page replays from its ``WHERE pk > :last``.
    """
    first_sql = build_pg_page_checksum_first_sql(
        table, pk_column, page_size, source_is_postgres
    )
    next_sql = build_pg_page_checksum_next_sql(
        table, pk_column, page_size, source_is_postgres
    )
    total = 0
    last: object = None
    while True:
        cursor = connection.cursor()
        try:
            if last is None:
                cursor.execute(first_sql)
            else:
                cursor.execute(next_sql, {"last": last})
            row = cursor.fetchone()
        finally:
            _safe_close(cursor)
        if row is None:
            return str(total)
        sub_sum, last_pk, count = row[0], row[1], row[2]
        total += int(sub_sum) if sub_sum is not None else 0
        if count is None or int(count) < page_size:
            return str(total)
        last = last_pk


def _target_pk_tokens(
    connection: Any,
    table: TableDef,
    pk_column: str,
    sample_size: int,
    source_is_postgres: bool = False,
) -> dict[str, str]:
    """Return up to ``sample_size`` ``{pk: token}`` entries for the target (read-only).

    Bounded by ``LIMIT`` -- at most ``sample_size`` rows are read, in PK order, via
    the primary-key index. PK and token stringified for cross-engine comparison.
    ``source_is_postgres`` is forwarded to the PG token builder for the numeric-scale rule.
    """
    cursor = connection.cursor()
    try:
        cursor.execute(
            build_pg_pk_token_sql(table, pk_column, sample_size, source_is_postgres)
        )
        rows = cursor.fetchall()
    finally:
        _safe_close(cursor)
    return {_norm_pk(row[0]): str(row[1]) for row in rows}


def _diff_pks(
    source_connection: _SourceConnection,
    target_connection: Any,
    table: TableDef,
    sample_size: int,
    source_dialect: SourceDialect,
) -> Optional[RowDiffSample]:
    """Sample which primary keys diverge between source and target for ``table``.

    Reads at most ``sample_size`` ``(pk, token)`` rows from each side in PK order
    (``ORDER BY pk LIMIT N`` -- bounded, no scan), then classifies by set
    difference: source-only -> ``MISSING_ON_TARGET``; target-only ->
    ``EXTRA_ON_TARGET``; present on both with differing tokens -> ``VALUE_MISMATCH``.
    Findings are capped at ``sample_size`` (``truncated`` flags the cap). Returns
    ``None`` for a composite/missing primary key (skipped, never scanned). Logs
    PK values + checksum tokens only -- never row values (Property 7); a natural-key
    PK is the operator's risk and the feature is dev-gated/default-off.
    """
    if len(table.primary_key) != 1:
        return None  # composite/missing PK: skip rather than fall back to a scan
    pk_column = table.primary_key[0]

    source_tokens = _source_pk_tokens_for(
        source_dialect, source_connection, table, pk_column, sample_size
    )
    target_tokens = _target_pk_tokens(
        target_connection, table, pk_column, sample_size,
        source_is_postgres=_source_is_postgres(source_dialect),
    )

    findings: list[RowDiffFinding] = []
    for pk in sorted(set(source_tokens) | set(target_tokens)):
        if len(findings) >= sample_size:
            break
        in_source = pk in source_tokens
        in_target = pk in target_tokens
        if in_source and not in_target:
            findings.append(RowDiffFinding(
                pk=pk, kind=RowDiffKind.MISSING_ON_TARGET,
                source_checksum=source_tokens[pk],
            ))
        elif in_target and not in_source:
            findings.append(RowDiffFinding(
                pk=pk, kind=RowDiffKind.EXTRA_ON_TARGET,
                target_checksum=target_tokens[pk],
            ))
        elif source_tokens[pk] != target_tokens[pk]:
            findings.append(RowDiffFinding(
                pk=pk, kind=RowDiffKind.VALUE_MISMATCH,
                source_checksum=source_tokens[pk], target_checksum=target_tokens[pk],
            ))

    # truncated: the sample windows were full on either side, so divergences may
    # exist beyond the first N PKs (the sample explains, it does not bound).
    truncated = (
        len(findings) >= sample_size
        or len(source_tokens) >= sample_size
        or len(target_tokens) >= sample_size
    )
    return RowDiffSample(
        pk_column=pk_column,
        sample_size=sample_size,
        truncated=truncated,
        findings=findings,
    )


# ---------------------------------------------------------------------------
# Full PK-set reconciliation (streaming keyset merge, bounded memory)
# ---------------------------------------------------------------------------
#
# Default rows fetched per keyset page during reconciliation. Bounded so memory
# stays at one page per side regardless of table size (large-scale design).
_RECONCILE_PAGE_SIZE = 5000

# How many diverging PKs to record per side in a ReconcileResult sample. The full
# divergence COUNT is always exact; only the listed example PKs are capped.
_RECONCILE_SAMPLE_CAP = 50


def _iter_source_pks(
    connection: _SourceConnection, table: TableDef, pk_column: str, page_size: int
) -> "Iterator[int]":
    """Yield every source PK in ascending order via bounded keyset pagination.

    Reads one ``page_size``-row page at a time (``WHERE pk > :last ORDER BY pk
    LIMIT N``), advancing the keyset, so the table is never materialized. Only
    the integer PK column is read (Property 7). Read-only.
    """
    first_sql = text(build_mysql_pk_first_page_sql(table, pk_column))
    next_sql = text(build_mysql_pk_next_page_sql(table, pk_column))
    last: object = None
    while True:
        if last is None:
            result = connection.execute(first_sql, {"page": page_size})
        else:
            result = connection.execute(next_sql, {"last": last, "page": page_size})
        count = 0
        for row in result:  # type: ignore[attr-defined]
            count += 1
            last = row[0]
            yield int(row[0])
        if count < page_size:
            return


def _iter_target_pks(
    connection: Any, table: TableDef, pk_column: str, page_size: int
) -> "Iterator[int]":
    """Yield every target PK in ascending order via bounded keyset pagination.

    Mirrors :func:`_iter_source_pks` on the psycopg target: one bounded page per
    round-trip, keyset-advanced, so a billion-row table never lands in RAM.
    """
    first_sql = build_pg_pk_first_page_sql(table, pk_column, page_size)
    next_sql = build_pg_pk_next_page_sql(table, pk_column, page_size)
    last: object = None
    while True:
        cursor = connection.cursor()
        try:
            if last is None:
                cursor.execute(first_sql)
            else:
                cursor.execute(next_sql, {"last": last})
            rows = cursor.fetchall()
        finally:
            _safe_close(cursor)
        for row in rows:
            last = row[0]
            yield int(row[0])
        if len(rows) < page_size:
            return


# ---------------------------------------------------------------------------
# Source-read dispatch (by source engine)
# ---------------------------------------------------------------------------
#
# MySQL source reads use build_mysql_* over the SQLAlchemy text() path. A PostgreSQL
# source reuses the SAME PG-16 readers as the DSQL target (build_pg_* + _target_* /
# _bounded_target_count) -- both ends are PostgreSQL, so one renderer serves both sides --
# via a PgSourceConnection shim that runs them through the guarded SQLAlchemy connection
# (read-only guard + snapshot preserved). The target read path is unchanged.


def _source_is_postgres(dialect: SourceDialect) -> bool:
    return dialect.source_type is SourceType.POSTGRES


def _source_row_count_live(
    dialect: SourceDialect,
    connection: _SourceConnection,
    table: TableDef,
    page_size: int,
) -> int:
    """Exact live source count: PG keyset-bounds it (single PK) like the target; MySQL
    (no per-txn limit) uses a plain ``COUNT(*)`` as before."""
    if _source_is_postgres(dialect):
        return _bounded_target_count(PgSourceConnection(connection), table, page_size)
    return _source_count(connection, table.name)


def _source_checksum_for(
    dialect: SourceDialect,
    connection: _SourceConnection,
    table: TableDef,
    page_size: int,
) -> str:
    if _source_is_postgres(dialect):
        return _target_checksum(
            PgSourceConnection(connection), table, page_size, source_is_postgres=True
        )
    return _source_checksum(connection, table, page_size)


def _iter_source_pks_for(
    dialect: SourceDialect,
    connection: _SourceConnection,
    table: TableDef,
    pk_column: str,
    page_size: int,
) -> "Iterator[int]":
    if _source_is_postgres(dialect):
        return _iter_target_pks(
            PgSourceConnection(connection), table, pk_column, page_size
        )
    return _iter_source_pks(connection, table, pk_column, page_size)


def _source_pk_tokens_for(
    dialect: SourceDialect,
    connection: _SourceConnection,
    table: TableDef,
    pk_column: str,
    sample_size: int,
) -> dict[str, str]:
    if _source_is_postgres(dialect):
        return _target_pk_tokens(
            PgSourceConnection(connection), table, pk_column, sample_size,
            source_is_postgres=True,
        )
    return _source_pk_tokens(connection, table, pk_column, sample_size)


# Poll the cancel check every this-many merge steps during reconciliation. Small
# enough that a cancel takes effect within a few thousand rows even on a huge
# table, large enough that the check adds no measurable overhead.
_RECONCILE_CANCEL_POLL_EVERY = 4096


def reconcile_pk_streams(
    pk_column: str,
    source_pks: "Iterator[int]",
    target_pks: "Iterator[int]",
    *,
    sample_cap: int = _RECONCILE_SAMPLE_CAP,
    should_cancel: Optional["CancelCheck"] = None,
) -> ReconcileResult:
    """Merge two ascending PK streams into a :class:`ReconcileResult` (pure).

    Both inputs MUST yield integer primary keys in ascending order (the keyset
    streams do). A classic sorted-merge walks both at once with O(1) extra memory
    beyond the bounded sample lists: a PK on the source but not the target is
    ``missing_on_target`` (a lost / not-yet-replicated row); a PK on the target
    but not the source is ``extra_on_target`` (a delete CDC has not applied). The
    divergence COUNTS are exact and unbounded-safe; only the example PK lists are
    capped at ``sample_cap`` (``sample_truncated`` flags the cap). The table is
    ``consistent`` only when both divergence counts are zero.

    ``should_cancel`` (polled every few thousand merged rows) makes a cancel
    responsive WITHIN a single large table: the reconciliation of one big table is
    the longest unit of work, so without this a cooperative stop would wait for
    the whole table. When it fires, :class:`ValidationCancelled` is raised.

    Kept pure (takes iterators, returns a model) so it is unit-testable without
    any database and so the same merge powers both engines.
    """
    cancel = should_cancel or (lambda: False)
    source_count = 0
    target_count = 0
    missing = 0  # source-only
    extra = 0  # target-only
    missing_sample: list[str] = []
    extra_sample: list[str] = []

    sentinel = object()
    s = next(source_pks, sentinel)
    t = next(target_pks, sentinel)
    steps = 0
    while s is not sentinel or t is not sentinel:
        steps += 1
        if steps % _RECONCILE_CANCEL_POLL_EVERY == 0 and cancel():
            raise ValidationCancelled(
                "Validation cancelled during record reconciliation."
            )
        if t is sentinel or (s is not sentinel and s < t):  # type: ignore[operator]
            source_count += 1
            missing += 1
            if len(missing_sample) < sample_cap:
                missing_sample.append(str(s))
            s = next(source_pks, sentinel)
        elif s is sentinel or (t is not sentinel and t < s):  # type: ignore[operator]
            target_count += 1
            extra += 1
            if len(extra_sample) < sample_cap:
                extra_sample.append(str(t))
            t = next(target_pks, sentinel)
        else:  # s == t: present on both, advance both
            source_count += 1
            target_count += 1
            s = next(source_pks, sentinel)
            t = next(target_pks, sentinel)

    return ReconcileResult(
        pk_column=pk_column,
        source_count=source_count,
        target_count=target_count,
        missing_on_target=missing,
        extra_on_target=extra,
        missing_sample=missing_sample,
        extra_sample=extra_sample,
        sample_truncated=missing > len(missing_sample) or extra > len(extra_sample),
        consistent=missing == 0 and extra == 0,
    )


def _target_orphan_count(
    connection: Any, table: TableDef, fk: ForeignKeyDef, page_size: int
) -> int:
    """Return the number of orphan child rows for ``fk`` on the target.

    For a single-column-PK child the count is accumulated over BOUNDED keyset pages
    (:func:`_target_orphan_count_keyset`) so a billion-row child never runs the orphan
    scan in one transaction -- a single unbounded ``COUNT(*) ... NOT EXISTS`` would exceed
    Aurora DSQL's hard 300s transaction limit, exactly the failure mode the count/checksum
    keyset pagers already avoid. A composite/missing PK falls back to the single scan
    (:func:`build_orphan_count_sql`), a documented residual like :func:`_target_count`.
    """
    pk_column = single_pk_column(table)
    if pk_column is not None:
        return _target_orphan_count_keyset(
            connection, table.name, fk, pk_column, page_size
        )
    value = _target_scalar(connection, build_orphan_count_sql(table.name, fk))
    return int(value) if value is not None else 0


def _target_orphan_count_keyset(
    connection: Any,
    child_table: str,
    fk: ForeignKeyDef,
    pk_column: str,
    page_size: int,
) -> int:
    """Accumulate the orphan count over bounded keyset pages (single-column PK).

    The orphan counterpart of :func:`_target_count_keyset` / :func:`_target_checksum_keyset`:
    each ``page_size``-row page reports ``(orphan_sub_count, last_pk, row_count)`` where
    the sub-count is a ``COUNT(*) FILTER`` over the orphan predicate but the page window
    itself is UNFILTERED, so ``last_pk`` (the window's max PK) advances the keyset over
    non-orphan rows too and no PK range is skipped. Sub-counts fold into a Python integer;
    the loop stops when a page returns fewer than ``page_size`` rows. Bounded per page, so
    it stays well under DSQL's 300s limit; a connection aged out at ~1h is handled by the
    reconnecting proxy wrapping ``connection`` (the page replays from ``WHERE pk > :last``).
    """
    first_sql = build_pg_orphan_page_first_sql(child_table, fk, pk_column, page_size)
    next_sql = build_pg_orphan_page_next_sql(child_table, fk, pk_column, page_size)
    total = 0
    last: object = None
    while True:
        cursor = connection.cursor()
        try:
            if last is None:
                cursor.execute(first_sql)
            else:
                cursor.execute(next_sql, {"last": last})
            row = cursor.fetchone()
        finally:
            _safe_close(cursor)
        if row is None:
            return total
        sub_count, last_pk, count = row[0], row[1], row[2]
        total += int(sub_count) if sub_count is not None else 0
        if count is None or int(count) < page_size:
            return total
        last = last_pk


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def _default_target_connection_factory(target: TargetConnectionConfig) -> Any:
    """Open a default autocommit/TLS/IAM DSQL connection via :class:`DsqlConnector`.

    The IAM token is generated and kept confidential by the connector
    (Property 7 / Requirement 5.4).
    """
    return DsqlConnector(target).connect()


# A cancel check polled between tables: returns True once a cooperative stop has
# been requested (e.g. the JobManager handle's ``cancelled`` flag).
CancelCheck = Callable[[], bool]

# A progress callback invoked BEFORE each table is compared, with the table name,
# its 1-based index, and the total table count -- so the UI can show "checking
# table i of N: <name>" while a long run streams. Best-effort: the validator
# ignores any exception it raises so reporting never breaks the comparison.
ProgressCallback = Callable[[str, int, int], None]


class ValidationCancelled(RuntimeError):
    """Raised when a validation run is cooperatively cancelled mid-way.

    Validation stops at the next table boundary (its consistent-snapshot
    transaction is still closed cleanly) and raises this instead of returning a
    partial :class:`~dsql_migrator.core.models.ValidationReport`, so a cancelled
    run is never mistaken for a completed comparison. The caller maps it to a
    CANCELLED job state, not a failure.
    """


class Validator:
    """Validates a migrated DSQL target against its MySQL source (Requirement 6).

    The source engine factory and target connection factory are injectable so
    unit tests never reach a real MySQL or DSQL cluster; the defaults use the
    read-only-guarded MySQL engine and an IAM-authenticated
    :class:`~dsql_migrator.core.target_connection.DsqlConnector` connection.
    """

    def __init__(
        self,
        *,
        source_engine_factory: Optional[SourceEngineFactory] = None,
        target_connection_factory: Optional[TargetConnectionFactory] = None,
        row_diff_sample_size: int = 0,
        reconcile_page_size: int = _RECONCILE_PAGE_SIZE,
    ) -> None:
        """Create a validator with optional injected source/target factories.

        ``row_diff_sample_size`` (default ``0`` == OFF) enables the dev-only
        row-level diff: for any table that does NOT match, up to this many
        diverging primary keys are sampled (``ORDER BY pk LIMIT N``) and attached
        to the result as a :class:`RowDiffSample`. It runs only for mismatched
        tables, only at validation time, never on the export/import hot path, and
        is bounded so a large table is never scanned. Logs PK + checksum tokens
        only -- never row values (Property 7).

        ``reconcile_page_size`` is the keyset page size used by the full PK-set
        reconciliation (Property 7: PK values only); kept injectable so a test can
        force multi-page streaming with a tiny table.
        """
        self._source_engine_factory = source_engine_factory or _default_engine_factory
        self._target_connection_factory = (
            target_connection_factory or _default_target_connection_factory
        )
        self._row_diff_sample_size = max(0, int(row_diff_sample_size))
        self._reconcile_page_size = max(1, int(reconcile_page_size))

    def validate(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        tables: list[TableDef],
        mode: ValidationMode = ValidationMode.ROW_COUNT,
        *,
        watermark: Optional[Watermark] = None,
        check_orphans: bool = False,
        reconcile: bool = False,
        should_cancel: Optional[CancelCheck] = None,
        on_progress: Optional[ProgressCallback] = None,
        max_workers: int = 1,
        deep_only_on_count_mismatch: bool = False,
        quarantined_by_table: Optional[Mapping[str, int]] = None,
    ) -> ValidationReport:
        """Compare ``tables`` between source and target and return a report.

        Per-table row counts are compared (Requirement 6.1); in
        :attr:`ValidationMode.CHECKSUM` mode an order-independent checksum is also
        compared (Requirement 6.2). When ``reconcile`` is set, every primary key
        is streamed from both sides and merged to find the EXACT missing/extra
        rows (the pre-cut-over "no mismatched records" check), bounded so a large
        table is never materialized. When ``check_orphans`` is set, each table's
        preserved foreign keys are checked for orphan child rows on the target
        (Requirement 6.3). When ``watermark`` is supplied, per-table source row
        counts are taken from the watermark snapshot and the current source GTID
        is compared to the watermark's to report drift (Requirement 6.5 /
        Property 11). All source access is read-only inside one consistent-
        snapshot transaction (Property 1); the target is only read.

        A failure comparing ONE table (e.g. it does not exist on the target, or a
        query errors) is isolated to that table's :class:`TableValidationResult`
        ``error`` and never aborts the whole run, so the report still surfaces
        every other table and the bad table is shown as a failed check ("table
        errors") instead of crashing Validation.

        ``should_cancel`` (when given) is polled BEFORE each table; once it returns
        ``True`` the run stops at that table boundary and raises
        :class:`ValidationCancelled` (the consistent-snapshot transaction and
        connections are still closed cleanly via ``finally``), so a cancelled run
        never returns a partial report mistaken for a completed comparison.

        ``max_workers`` bounds table-level parallelism (default ``1`` ==
        sequential, the historical behavior). With ``> 1`` the tables are compared
        concurrently across a bounded thread pool, EACH worker opening its OWN
        source connection + consistent-snapshot transaction and its own target
        connection (a DB connection/transaction is not shareable across threads).
        Per-table comparison is independent -- each table is internally consistent
        within its own snapshot -- so cross-table snapshot identity is not needed
        for a correct per-table verdict; this cuts a large multi-table run's wall
        clock from the sum of per-table scans toward the slowest single table.
        Drift (a whole-source signal) is still read once on the main thread. A
        cancel still stops promptly: queued tables are skipped and the pool drains.

        ``deep_only_on_count_mismatch`` (default ``False``) is a speed optimization
        for the common case where most tables already match: the cheap row counts
        always run, but the EXPENSIVE checks (CHECKSUM-mode checksum and full PK
        reconciliation) run only for a table whose counts DIFFER. A count-matched
        table then reports ``checksum_match``/``reconcile`` as ``None`` (not run --
        an honest "verified by count", never a false equality claim), so soundness
        holds: ``matched`` only reflects checks that actually ran. This skips the
        per-row scans on the tables that need them least, cutting wall clock sharply
        on a healthy migration while still deep-checking every table that looks off.
        """
        cancel = should_cancel or (lambda: False)
        workers = max(1, int(max_workers))
        if workers > 1 and len(tables) > 1:
            return _with_quarantine_counts(
                self._validate_parallel(
                    source, target, tables, mode,
                    watermark=watermark, check_orphans=check_orphans,
                    reconcile=reconcile, cancel=cancel, on_progress=on_progress,
                    max_workers=workers,
                    deep_only_on_count_mismatch=deep_only_on_count_mismatch,
                ),
                quarantined_by_table,
            )

        def _report_progress(table_name: str, index: int, total: int) -> None:
            if on_progress is None:
                return
            try:
                on_progress(table_name, index, total)
            except Exception:  # noqa: BLE001 - progress is advisory; never break a run
                pass

        source_dialect = dialect_for(source.source_type)
        source_engine = self._source_engine_factory(source)
        items: list[TableValidationResult] = []
        orphan_findings: list[OrphanFinding] = []
        current_gtid: Optional[str] = None
        try:
            with source_engine.connect() as raw_connection:
                source_connection = raw_connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                source_connection.execute(text(source_dialect.snapshot_start_sql))
                try:
                    # Drift coordinates are MySQL binlog/GTID concepts. On a PostgreSQL
                    # source these probes are SQL syntax errors that would ABORT the
                    # shared snapshot transaction (PostgreSQL fails the whole txn on any
                    # statement error), poisoning every subsequent table read -- so skip
                    # them for PG (these MySQL binlog/GTID drift coordinates do not exist
                    # for a PostgreSQL source, which uses LSN watermarks, so drift stays
                    # "undeterminable"). MySQL runs them as before on the same snapshot.
                    current_gtid: Optional[str] = None
                    current_binlog_file: Optional[str] = None
                    current_binlog_position: Optional[int] = None
                    if not _source_is_postgres(source_dialect):
                        current_gtid = _source_gtid(source_connection)
                        # Also capture file:pos -- the drift fallback when the source has
                        # no GTID (the normal case on RDS MySQL 8.0).
                        current_binlog_file, current_binlog_position = (
                            _source_binlog_position(source_connection)
                        )
                    target_connection = _ReconnectingTargetConnection(
                        lambda: self._target_connection_factory(target)
                    )
                    try:
                        total = len(tables)
                        for index, table in enumerate(tables, start=1):
                            # Cooperative stop at the table boundary: cleanly exit
                            # (COMMIT + dispose run in the finally blocks) and
                            # signal the cancellation rather than return a partial.
                            if cancel():
                                raise ValidationCancelled(
                                    "Validation cancelled before completing all "
                                    "tables."
                                )
                            _report_progress(table.name, index, total)
                            items.append(
                                self._validate_table(
                                    source_connection,
                                    target_connection,
                                    table,
                                    mode,
                                    watermark,
                                    reconcile,
                                    cancel,
                                    source_dialect,
                                    deep_only_on_count_mismatch=(
                                        deep_only_on_count_mismatch
                                    ),
                                )
                            )
                            if check_orphans:
                                orphan_findings.extend(
                                    self._check_orphans(target_connection, table)
                                )
                    finally:
                        _safe_close(target_connection)
                finally:
                    source_connection.execute(text(COMMIT))
        finally:
            source_engine.dispose()

        drift = _build_drift(
            watermark, current_gtid, current_binlog_file, current_binlog_position
        )
        snapshot_timestamp = (
            watermark.snapshot_timestamp if watermark is not None else None
        )
        return _with_quarantine_counts(
            ValidationReport.build(
                mode=mode,
                items=items,
                orphan_findings=orphan_findings,
                orphan_check_performed=check_orphans,
                drift=drift,
                snapshot_timestamp=snapshot_timestamp,
                reconcile_requested=reconcile,
            ),
            quarantined_by_table,
        )

    def _validate_parallel(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        tables: list[TableDef],
        mode: ValidationMode,
        *,
        watermark: Optional[Watermark],
        check_orphans: bool,
        reconcile: bool,
        cancel: CancelCheck,
        on_progress: Optional[ProgressCallback],
        max_workers: int,
        deep_only_on_count_mismatch: bool = False,
    ) -> ValidationReport:
        """Compare ``tables`` concurrently, one bounded worker per in-flight table.

        Each table is an independent unit (its own source snapshot + target
        connection), so they run across a :class:`ThreadPoolExecutor` capped at
        ``max_workers``. Results are reassembled in the ORIGINAL table order (the
        report must read deterministically regardless of completion order). Drift
        is a whole-source signal, so the current GTID is read once on its own
        short-lived snapshot here. A cooperative cancel skips not-yet-started
        tables; a per-table failure stays isolated to that table's result.
        """
        from concurrent.futures import ThreadPoolExecutor

        total = len(tables)
        # Slot results by original index so completion order never reorders the report.
        results: list[Optional[TableValidationResult]] = [None] * total
        orphans_by_index: list[list[OrphanFinding]] = [[] for _ in range(total)]
        # Progress is reported as tables COMPLETE (not start) under concurrency, so the
        # count rises monotonically; guard the shared counter + callback with a lock.
        progress_lock = threading.Lock()
        done_count = 0

        def _run_one(index: int, table: TableDef) -> None:
            nonlocal done_count
            if cancel():
                return
            item, orphans = self._validate_one_table_isolated(
                source, target, table, mode, watermark, reconcile, check_orphans,
                cancel, deep_only_on_count_mismatch=deep_only_on_count_mismatch,
            )
            results[index] = item
            orphans_by_index[index] = orphans
            if on_progress is not None:
                with progress_lock:
                    done_count += 1
                    try:
                        on_progress(table.name, done_count, total)
                    except Exception:  # noqa: BLE001 - advisory; never break a run
                        pass

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_run_one, index, table)
                for index, table in enumerate(tables)
            ]
            for future in futures:
                future.result()  # re-raise a worker exception (e.g. unexpected fatal)

        if cancel():
            raise ValidationCancelled(
                "Validation cancelled before completing all tables."
            )

        items = [r for r in results if r is not None]
        orphan_findings: list[OrphanFinding] = []
        for found in orphans_by_index:
            orphan_findings.extend(found)

        current_gtid, current_binlog_file, current_binlog_position = (
            self._read_source_position(source)
        )
        drift = _build_drift(
            watermark, current_gtid, current_binlog_file, current_binlog_position
        )
        snapshot_timestamp = (
            watermark.snapshot_timestamp if watermark is not None else None
        )
        return ValidationReport.build(
            mode=mode,
            items=items,
            orphan_findings=orphan_findings,
            orphan_check_performed=check_orphans,
            drift=drift,
            snapshot_timestamp=snapshot_timestamp,
            reconcile_requested=reconcile,
        )

    def _validate_one_table_isolated(
        self,
        source: SourceConnectionConfig,
        target: TargetConnectionConfig,
        table: TableDef,
        mode: ValidationMode,
        watermark: Optional[Watermark],
        reconcile: bool,
        check_orphans: bool,
        cancel: CancelCheck,
        deep_only_on_count_mismatch: bool = False,
    ) -> "tuple[TableValidationResult, list[OrphanFinding]]":
        """Compare ONE table on its own source snapshot + target connection.

        The self-contained unit the parallel path runs per worker: open a fresh
        source engine/connection in a consistent-snapshot transaction (so each
        worker is internally consistent and read-only -- Property 1), open a fresh
        target connection, compare the one table, and -- when requested -- its
        orphans, then close everything cleanly via ``finally``. A per-table failure
        is already isolated inside :meth:`_validate_table`.
        """
        source_dialect = dialect_for(source.source_type)
        source_engine = self._source_engine_factory(source)
        orphans: list[OrphanFinding] = []
        try:
            with source_engine.connect() as raw_connection:
                source_connection = raw_connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                source_connection.execute(text(source_dialect.snapshot_start_sql))
                try:
                    target_connection = _ReconnectingTargetConnection(
                        lambda: self._target_connection_factory(target)
                    )
                    try:
                        item = self._validate_table(
                            source_connection, target_connection, table,
                            mode, watermark, reconcile, cancel, source_dialect,
                            deep_only_on_count_mismatch=deep_only_on_count_mismatch,
                        )
                        if check_orphans:
                            orphans = list(
                                self._check_orphans(target_connection, table)
                            )
                    finally:
                        _safe_close(target_connection)
                finally:
                    source_connection.execute(text(COMMIT))
        finally:
            source_engine.dispose()
        return item, orphans

    def _read_source_position(
        self, source: SourceConnectionConfig
    ) -> tuple[Optional[str], Optional[str], Optional[int]]:
        """Read the source's current ``(gtid, binlog_file, binlog_position)`` once.

        Used by the parallel path, where no shared main-thread snapshot exists. Both
        coordinates are read on the SAME short-lived connection so they describe the
        same instant. The binlog pair is the drift fallback for a source without GTID
        -- the normal case on RDS MySQL 8.0, where a GTID-only comparison could never
        determine drift at all. Best-effort and read-only: any failure degrades to
        ``None`` values (drift becomes "undeterminable"), never aborting the run.
        """
        # Drift coordinates are MySQL binlog/GTID; a PostgreSQL source has neither (it
        # uses LSN watermarks), and the probes are MySQL-only SQL, so skip them entirely
        # (drift "undeterminable"). (Unlike the serial path this uses a throwaway
        # connection, so it wouldn't poison table reads -- but skipping avoids two doomed
        # queries.)
        if source.source_type is SourceType.POSTGRES:
            return None, None, None
        engine = self._source_engine_factory(source)
        try:
            with engine.connect() as raw_connection:
                connection = raw_connection.execution_options(
                    isolation_level="AUTOCOMMIT"
                )
                gtid = _source_gtid(connection)
                binlog_file, binlog_position = _source_binlog_position(connection)
                return gtid, binlog_file, binlog_position
        except Exception:  # noqa: BLE001 - drift is advisory
            return None, None, None
        finally:
            engine.dispose()

    def _validate_table(
        self,
        source_connection: _SourceConnection,
        target_connection: Any,
        table: TableDef,
        mode: ValidationMode,
        watermark: Optional[Watermark],
        reconcile: bool,
        should_cancel: CancelCheck,
        source_dialect: SourceDialect,
        deep_only_on_count_mismatch: bool = False,
    ) -> TableValidationResult:
        """Compare one table, isolating any failure to this table's result.

        Delegates to :meth:`_compare_table`; if comparing this one table raises
        (e.g. it is missing on the target, or a query errors), the exception is
        caught and surfaced as the result's ``error`` (a failed "table errors"
        check) so one bad table never aborts the whole validation run. The
        message is rendered log-safe (no row values / credentials, Property 7).
        A :class:`ValidationCancelled` is NOT a per-table failure -- it is a
        cooperative stop, so it propagates to abort the whole run cleanly.
        """
        try:
            return self._compare_table(
                source_connection,
                target_connection,
                table,
                mode,
                watermark,
                reconcile,
                should_cancel,
                source_dialect,
                deep_only_on_count_mismatch=deep_only_on_count_mismatch,
            )
        except ValidationCancelled:
            raise  # cooperative stop: abort the run, not a per-table error
        except Exception as exc:  # noqa: BLE001 - isolate per-table failure
            return TableValidationResult(
                table=table.name,
                source_row_count=0,
                target_row_count=0,
                row_count_match=False,
                matched=False,
                error=_safe_error_message(exc),
            )

    def _compare_table(
        self,
        source_connection: _SourceConnection,
        target_connection: Any,
        table: TableDef,
        mode: ValidationMode,
        watermark: Optional[Watermark],
        reconcile: bool,
        should_cancel: CancelCheck,
        source_dialect: SourceDialect,
        deep_only_on_count_mismatch: bool = False,
    ) -> TableValidationResult:
        """Compare one table's row count (and, in CHECKSUM mode, its checksum).

        The source row count comes from the watermark snapshot when available
        (as-of the consistency point, Requirement 6.5); otherwise it is read
        live. ``matched`` is computed soundly: equal counts, (in CHECKSUM mode)
        equal checksums, AND -- when reconciliation ran -- no missing/extra
        records (Property 9).

        When ``deep_only_on_count_mismatch`` is set, the expensive checksum and PK
        reconciliation are skipped for a table whose counts already AGREE (they
        leave ``checksum_match``/``reconcile`` as ``None`` -- not run, never a
        false equality); a count DISAGREEMENT still triggers the full deep checks
        so the exact diverging rows are found. Soundness is preserved: ``matched``
        only credits checks that actually ran.
        """
        source_row_count = self._source_row_count(
            source_connection, table, watermark, source_dialect,
            self._reconcile_page_size,
        )
        # The TARGET count must be BOUNDED on Aurora DSQL: a single COUNT(*) scans the
        # whole table in one transaction and, at scale, exceeds DSQL's hard 300s limit,
        # so a large table could never report MATCH. We prefer the exact count that
        # reconciliation ALREADY streams (keyset-paged) for an integer PK; otherwise we
        # keyset-page the count ourselves for any single-column PK; COUNT(*) is used only
        # for a composite/missing PK. In deep-only mode the count is needed UP FRONT to
        # decide whether to run the deep checks; otherwise it is deferred to after
        # reconcile so reconcile's count is reused with no second scan.
        target_row_count: Optional[int] = None
        row_count_match: Optional[bool] = None
        if deep_only_on_count_mismatch:
            target_row_count = _bounded_target_count(
                target_connection, table, self._reconcile_page_size
            )
            row_count_match = source_row_count == target_row_count

        # Fast path: when asked to deep-check only on a count mismatch, a table whose
        # counts agree skips the per-row checksum + reconciliation scans entirely.
        # The result honestly reports those as not-run (None), so a "match" here
        # means "verified by row count" -- exactly the ROW_COUNT contract.
        run_deep = not (deep_only_on_count_mismatch and row_count_match)

        source_checksum: Optional[str] = None
        target_checksum: Optional[str] = None
        checksum_match: Optional[bool] = None
        checksum_excluded_columns: list[str] = []
        if mode is ValidationMode.CHECKSUM and run_deep:
            source_checksum = _source_checksum_for(
                source_dialect, source_connection, table, self._reconcile_page_size
            )
            target_checksum = _target_checksum(
                target_connection, table, self._reconcile_page_size,
                source_is_postgres=_source_is_postgres(source_dialect),
            )
            checksum_match = source_checksum == target_checksum
            # Columns the checksum could not value-compare (no byte-identical
            # cross-engine text form): FLOAT/DOUBLE and JSON. Recorded so a MATCH is
            # surfaced as "every column EXCEPT these was value-compared" -- a non-key
            # value diff confined to such a column is invisible to every mode.
            checksum_excluded_columns = [
                column.name
                for column in table.columns
                if _checksum_kind(column) in ("float", "json")
            ]

        # Full PK-set reconciliation (the "no mismatched records" check): stream
        # every PK from both sides and merge. Only for single-column integer PKs
        # (well-defined cross-engine order); other tables fall back to
        # count/checksum (reconcile stays None). Bounded keyset paging -- never a
        # full materialization (Property 7: PK values only).
        reconcile_result: Optional[ReconcileResult] = None
        if reconcile and run_deep:
            pk_column = integer_pk_column(table)
            if pk_column is not None:
                reconcile_result = reconcile_pk_streams(
                    pk_column,
                    _iter_source_pks_for(
                        source_dialect, source_connection, table, pk_column,
                        self._reconcile_page_size,
                    ),
                    _iter_target_pks(
                        target_connection, table, pk_column, self._reconcile_page_size
                    ),
                    should_cancel=should_cancel,
                )

        # Default path: the target count was deferred so it could reuse reconcile's
        # exact keyset-streamed count (no second scan); when reconcile did not run
        # (non-integer PK, reconcile off, or CHECKSUM-only), keyset-page it directly --
        # a single unbounded COUNT(*) is used only for a composite/missing PK.
        if target_row_count is None:
            target_row_count = (
                reconcile_result.target_count
                if reconcile_result is not None
                else _bounded_target_count(
                    target_connection, table, self._reconcile_page_size
                )
            )
            row_count_match = source_row_count == target_row_count

        matched = (
            row_count_match
            and (checksum_match is not False)
            and (reconcile_result is None or reconcile_result.consistent)
        )

        # Dev-only diagnostic: for a table that did NOT match, sample which PKs
        # diverge. Bounded (LIMIT N), mismatched-tables-only, validation-time-only
        # -- never the hot path, never a full scan. Off when sample size is 0.
        row_diff_sample: Optional[RowDiffSample] = None
        if not matched and self._row_diff_sample_size > 0:
            row_diff_sample = _diff_pks(
                source_connection, target_connection, table,
                self._row_diff_sample_size, source_dialect,
            )

        return TableValidationResult(
            table=table.name,
            source_row_count=source_row_count,
            target_row_count=target_row_count,
            row_count_match=row_count_match,
            source_checksum=source_checksum,
            target_checksum=target_checksum,
            checksum_match=checksum_match,
            checksum_excluded_columns=checksum_excluded_columns,
            matched=matched,
            row_diff_sample=row_diff_sample,
            reconcile=reconcile_result,
            # Record whether record-level reconciliation COULD run for this table (a
            # single integer PK), independent of whether it actually did. When
            # reconciliation was requested but this is False for every table, a
            # row-count match must not be released as a clean, record-verified pass.
            reconcile_applicable=integer_pk_column(table) is not None,
            deep_checks_skipped=not run_deep,
        )

    @staticmethod
    def _source_row_count(
        source_connection: _SourceConnection,
        table: TableDef,
        watermark: Optional[Watermark],
        source_dialect: SourceDialect,
        page_size: int,
    ) -> int:
        """Return the as-of source row count for ``table``.

        Uses the watermark's recorded snapshot count only when it is exact; the
        Full Load watermark stores approximate (scan-free) estimates to spare the
        source, so validation re-counts the source live (exact) for a correct
        row-count comparison. The exact count therefore runs only at validation
        time, never during the migration itself, and is dispatched per source engine
        (MySQL ``COUNT(*)``; PostgreSQL keyset-bounded like the target).
        """
        if (
            watermark is not None
            and not watermark.row_counts_approximate
            and table.name in watermark.table_row_counts
        ):
            return watermark.table_row_counts[table.name]
        return _source_row_count_live(
            source_dialect, source_connection, table, page_size
        )

    def _check_orphans(
        self, target_connection: Any, table: TableDef
    ) -> list[OrphanFinding]:
        """Check each preserved foreign key on ``table`` for orphan child rows.

        Returns one :class:`OrphanFinding` per foreign key that has at least one
        orphan on the target (clean keys produce no finding). Each FK is counted over
        the SAME bounded keyset paging the count/checksum use (single-column-PK child),
        so a large child never runs the orphan scan past DSQL's 300s transaction limit.
        """
        findings: list[OrphanFinding] = []
        for fk in table.foreign_keys:
            orphan_count = _target_orphan_count(
                target_connection, table, fk, self._reconcile_page_size
            )
            if orphan_count > 0:
                findings.append(
                    OrphanFinding(
                        table=table.name,
                        foreign_key=fk.name,
                        referenced_table=fk.referenced_table,
                        orphan_count=orphan_count,
                    )
                )
        return findings


def format_binlog_coordinate(
    file_name: Optional[str], position: Optional[int]
) -> Optional[str]:
    """Render a binlog coordinate as ``file:position``, or ``None`` if incomplete.

    Both halves are required: a file without a position cannot be compared, and a
    position without a file is meaningless (positions restart per file).
    """
    if not file_name or position is None:
        return None
    return f"{file_name}:{position}"


def binlog_advanced(
    watermark_file: Optional[str],
    watermark_position: Optional[int],
    current_file: Optional[str],
    current_position: Optional[int],
) -> Optional[bool]:
    """Whether the source advanced, comparing binlog coordinates. Pure.

    Returns ``None`` when either coordinate is incomplete (undeterminable).

    Only EQUALITY is needed, not ordering, which keeps this correct without having to
    reason about rotation: the position restarts at the top of each new binlog file,
    so a later file can hold a SMALLER position and any "is it greater" comparison of
    the raw values would be wrong. So: same file -> the position must be identical to
    count as unchanged; different file -> the log rotated (or was reset), which only
    happens if the server kept writing. Either way an inequality means the source did
    not stand still, which is exactly the question being asked. A coordinate that
    moved backwards (restored/rebuilt source, ``RESET MASTER``) therefore also reads
    as drift -- correctly, since it is certainly not "unchanged since the snapshot".
    """
    if not watermark_file or watermark_position is None:
        return None
    if not current_file or current_position is None:
        return None
    if watermark_file == current_file:
        return current_position != watermark_position
    # Different files -> the log rotated (or was reset), which means writes happened.
    return True


def _with_quarantine_counts(
    report: ValidationReport, quarantined_by_table: Optional[Mapping[str, int]]
) -> ValidationReport:
    """Attach per-table permanently-dropped row counts to a finished report.

    Applied once at the end of :meth:`Validator.validate` (both the serial and parallel
    paths) rather than threaded through every comparison layer: the counts come from the
    migration job, not from comparing the databases, so they are metadata ABOUT the run
    rather than an input to it. Keeping them out of the comparison also keeps
    ``_compare_table`` a pure source-vs-target function.

    Deliberately does NOT touch ``matched`` or the report's own verdict: the rows really
    are missing from the target, so a table that dropped rows must keep failing. This
    only records WHY, so the UI can separate an expected gap from unexplained loss --
    the operator previously had to reconstruct that by hand from the error log.

    Returns the report unchanged when there are no counts to attach.
    """
    if not quarantined_by_table:
        return report
    updated = [
        item.model_copy(
            update={"rows_quarantined": quarantined_by_table.get(item.table, 0) or 0}
        )
        for item in report.items
    ]
    return report.model_copy(update={"items": updated})


def _build_drift(
    watermark: Optional[Watermark],
    current_gtid: Optional[str],
    current_binlog_file: Optional[str] = None,
    current_binlog_position: Optional[int] = None,
) -> Optional[DriftReport]:
    """Build a drift report from the watermark and the source's current position.

    Returns ``None`` when no watermark was supplied (drift is undefined without a
    consistency point). GTID is preferred when BOTH sides have one; otherwise this
    falls back to comparing binlog ``file:position`` (Requirement 6.5 / Property 11).

    The fallback is the normal path on the primary supported source: RDS MySQL 8.0
    cannot enable GTID, so a GTID-only comparison reported "could not be determined"
    on every single run -- the section could never say anything. The watermark already
    captures file:pos (``SHOW MASTER STATUS`` returns it alongside the GTID, and CDC
    already resumes from it), so the coordinate needed to answer the question was
    being collected and then ignored.
    """
    if watermark is None:
        return None
    watermark_gtid = watermark.gtid_executed
    watermark_binlog = format_binlog_coordinate(
        watermark.binlog_file, watermark.binlog_position
    )
    current_binlog = format_binlog_coordinate(
        current_binlog_file, current_binlog_position
    )

    if watermark_gtid is not None and current_gtid is not None:
        drifted = watermark_gtid != current_gtid
        return DriftReport(
            watermark_gtid=watermark_gtid,
            current_gtid=current_gtid,
            drifted=drifted,
            detail=(
                "Source advanced since the snapshot (GTID changed)."
                if drifted
                else "No source changes since the snapshot."
            ),
            basis="gtid",
            watermark_binlog=watermark_binlog,
            current_binlog=current_binlog,
        )

    advanced = binlog_advanced(
        watermark.binlog_file,
        watermark.binlog_position,
        current_binlog_file,
        current_binlog_position,
    )
    if advanced is not None:
        return DriftReport(
            watermark_gtid=watermark_gtid,
            current_gtid=current_gtid,
            drifted=advanced,
            detail=(
                "Source advanced since the snapshot (binlog position moved from "
                f"{watermark_binlog} to {current_binlog})."
                if advanced
                else "No source changes since the snapshot (binlog position "
                f"unchanged at {watermark_binlog})."
            ),
            basis="binlog",
            watermark_binlog=watermark_binlog,
            current_binlog=current_binlog,
        )

    return DriftReport(
        watermark_gtid=watermark_gtid,
        current_gtid=current_gtid,
        drifted=False,
        detail=(
            "Neither a GTID nor a binlog position was available on both sides, so "
            "drift since the snapshot could not be determined."
        ),
        basis="",
        watermark_binlog=watermark_binlog,
        current_binlog=current_binlog,
    )


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise
        pass


def _safe_error_message(exc: Exception) -> str:
    """Render a per-table comparison failure as a short, log-safe message.

    Keeps only the exception type and its message text (the first line), so a
    table whose comparison failed is explained ("relation does not exist",
    "connection reset") without leaking a multi-line driver dump or any row
    values / credentials into the report (Property 7).
    """
    text_value = str(exc).strip().splitlines()
    detail = text_value[0].strip() if text_value else ""
    name = type(exc).__name__
    return f"{name}: {detail}" if detail else name


# ---------------------------------------------------------------------------
# Report rendering / export
# ---------------------------------------------------------------------------


def _render_table_line(item: TableValidationResult) -> str:
    """Render one per-table validation result as a readable text line."""
    # A table that could not be compared at all is reported as an error line.
    if item.error is not None:
        return f"- {item.table}: ERROR -- {item.error} [ERROR]"

    verdict = "MATCH" if item.matched else "MISMATCH"
    parts = [
        f"- {item.table}: source={item.source_row_count} "
        f"target={item.target_row_count} "
        f"(row count {'match' if item.row_count_match else 'MISMATCH'})"
    ]
    if item.checksum_match is not None:
        parts.append(
            f"checksum {'match' if item.checksum_match else 'MISMATCH'}"
        )
    reconcile = item.reconcile
    if reconcile is not None:
        parts.append(
            f"records {'consistent' if reconcile.consistent else 'INCONSISTENT'} "
            f"(missing={reconcile.missing_on_target}, extra={reconcile.extra_on_target})"
        )
    parts.append(f"[{verdict}]")
    line = ", ".join(parts)

    # Name the diverging PKs from the full reconciliation (PK values only --
    # never row values, Property 7): missing on target (lost / not-yet-replicated
    # rows) and extra on target (a source delete CDC has not applied).
    if reconcile is not None and not reconcile.consistent:
        sub = [line]
        if reconcile.missing_sample:
            sub.append(f"    missing on target (pk={reconcile.pk_column}): "
                       f"{reconcile.missing_sample}")
        if reconcile.extra_sample:
            sub.append(f"    extra on target (pk={reconcile.pk_column}): "
                       f"{reconcile.extra_sample}")
        if reconcile.sample_truncated:
            sub.append("    (sample truncated -- more diverging PKs exist)")
        line = "\n".join(sub)

    # Dev row-diff sample (only present when enabled and the table mismatched):
    # name the diverging PKs (PK + checksum tokens only -- never row values).
    sample = item.row_diff_sample
    if sample is not None:
        sub = [line, f"  diff sample (pk={sample.pk_column}, top-{sample.sample_size}):"]
        if not sample.findings:
            sub.append("    (no divergence within the sampled PK window)")
        for f in sample.findings:
            toks = []
            if f.source_checksum is not None:
                toks.append(f"src={f.source_checksum}")
            if f.target_checksum is not None:
                toks.append(f"tgt={f.target_checksum}")
            tok_str = f" [{' '.join(toks)}]" if toks else ""
            sub.append(f"    {f.pk} {f.kind.value}{tok_str}")
        if sample.truncated:
            sub.append("    (sample truncated -- more divergences may exist beyond the window)")
        return "\n".join(sub)
    return line


def _render_drift_lines(drift: Optional[DriftReport]) -> list[str]:
    """Render the drift-since-watermark section as readable text lines."""
    if drift is None:
        return [
            "Drift since snapshot: not available "
            "(validation ran without a watermark)."
        ]
    return [
        "Drift since snapshot:",
        f"- Watermark GTID: {drift.watermark_gtid or 'unavailable'}",
        f"- Current source GTID: {drift.current_gtid or 'unavailable'}",
        f"- Drifted: {'yes' if drift.drifted else 'no'}",
        f"- {drift.detail}",
    ]


def render_text_report(report: ValidationReport) -> str:
    """Render a :class:`ValidationReport` as a human-readable text summary.

    Includes the overall match verdict, the as-of snapshot timestamp, the
    per-table comparison, any orphan findings, and the drift-since-watermark
    section (Requirements 6.4, 6.5).
    """
    lines = ["Validation Report", "=================", ""]
    lines.append(f"Mode: {report.mode.value}")
    lines.append(f"Overall: {'MATCH' if report.is_match else 'MISMATCH'}")
    as_of = (
        report.snapshot_timestamp.isoformat()
        if report.snapshot_timestamp is not None
        else "live source (no watermark)"
    )
    lines.append(f"As-of (snapshot): {as_of}")
    lines.append("")

    # Cut-over readiness: the three checks summarized up front.
    errored = [item for item in report.items if item.error is not None]
    reconciled = [item for item in report.items if item.reconcile is not None]
    inconsistent = [item for item in reconciled if not item.reconcile.consistent]  # type: ignore[union-attr]
    lines.append("Cut-over readiness:")
    # The data-match LABEL is mode-aware: ROW_COUNT mode never reads non-PK column
    # VALUES, so "Data identical" would overstate what was verified -- it is "Row counts
    # match". Only CHECKSUM mode value-compares, so only it earns "Data identical". (The
    # NiceGUI readiness panel already draws this distinction; the downloadable text report
    # used to print "Data identical" unconditionally.)
    is_checksum = report.mode is ValidationMode.CHECKSUM
    match_label = "Data identical" if is_checksum else "Row counts match"
    lines.append(
        f"- {match_label}: {'yes' if report.is_match else 'NO'} "
        f"({sum(1 for i in report.items if i.matched)}/{len(report.items)} tables matched)"
    )
    # Honesty caveat (Property 9): FLOAT/DOUBLE and JSON columns have no byte-identical
    # cross-engine form, so the CHECKSUM omits them -- a difference confined to such a
    # NON-KEY column is invisible to every mode. Surface them so "Data identical: yes" is
    # read as "every column EXCEPT these", not "every column verified".
    excluded_by_table = {
        item.table: item.checksum_excluded_columns
        for item in report.items
        if item.checksum_excluded_columns
    }
    if excluded_by_table:
        detail = "; ".join(
            f"{table} ({', '.join(cols)})" for table, cols in excluded_by_table.items()
        )
        lines.append(
            "- Columns NOT value-compared (FLOAT/DOUBLE/JSON -- no cross-engine form, "
            f"a non-key value diff there is undetected): {detail}"
        )
    # Tables that reconciliation was REQUESTED for but could not cover (composite /
    # non-integer PK, not errored, not fast-sweep-skipped): compared by count/checksum
    # only. Computed INLINE here -- core must not import the UI's ``reconcile_skipped_
    # tables`` helper (that would invert the ui->core dependency); this mirrors its logic.
    reconcile_requested = report.reconcile_requested or bool(reconciled)
    reconcile_skipped = (
        [
            item.table
            for item in report.items
            if item.reconcile is None
            and item.error is None
            and not item.deep_checks_skipped
        ]
        if reconcile_requested
        else []
    )
    compared_by = "count and checksum" if is_checksum else "row count only"
    if reconciled:
        total_missing = sum(i.reconcile.missing_on_target for i in reconciled)  # type: ignore[union-attr]
        total_extra = sum(i.reconcile.extra_on_target for i in reconciled)  # type: ignore[union-attr]
        lines.append(
            f"- No missing or extra records: {'yes' if not inconsistent else 'NO'} "
            f"({total_missing} missing on target, {total_extra} extra on target)"
        )
        # Footnote the composite/non-integer-PK tables that ran but were NOT reconciled,
        # so "no missing or extra records" is not read as covering every table.
        if reconcile_skipped:
            lines.append(
                f"    note: {len(reconcile_skipped)} table(s) not record-reconciled "
                f"(composite/non-integer primary key), compared by {compared_by}: "
                f"{', '.join(reconcile_skipped)}"
            )
    elif reconcile_requested:
        # Reconciliation was requested but NO table was eligible (every PK is
        # composite/non-integer). A row-count/checksum match does NOT prove the record
        # sets match, so this must not read as a clean pass.
        lines.append(
            "- No missing or extra records: NOT verified -- record-level "
            "reconciliation was requested but could not run for any table "
            "(no single integer primary key); tables compared by "
            f"{compared_by}: {', '.join(reconcile_skipped)}"
        )
    else:
        lines.append(
            "- No missing or extra records: not checked (reconciliation off)"
        )
    lines.append(
        f"- No table errors: {'yes' if not errored else 'NO'} "
        f"({len(errored)} table(s) errored)"
    )
    lines.append("")

    lines.append(f"Tables ({len(report.items)}):")
    if report.items:
        lines.extend(_render_table_line(item) for item in report.items)
    else:
        lines.append("- (no tables compared)")
    lines.append("")

    lines.append(
        f"Orphan check: {'performed' if report.orphan_check_performed else 'skipped'}"
    )
    if report.orphan_findings:
        lines.append(f"Orphan findings ({len(report.orphan_findings)}):")
        lines.extend(
            f"- {finding.table}.{finding.foreign_key} -> "
            f"{finding.referenced_table}: {finding.orphan_count} orphan(s)"
            for finding in report.orphan_findings
        )
    else:
        lines.append("Orphan findings: none")
    lines.append("")

    lines.extend(_render_drift_lines(report.drift))
    return "\n".join(lines)


def export_report(report: ValidationReport, fmt: str = "json") -> str:
    """Export a validation report as ``"json"`` or ``"text"``.

    JSON is produced from the Pydantic model (machine-readable, downloadable);
    text is a readable summary. Raises ``ValueError`` for unknown formats.
    """
    normalized = fmt.lower()
    if normalized == "json":
        return report.model_dump_json(indent=2)
    if normalized == "text":
        return render_text_report(report)
    raise ValueError(f"unsupported report format: {fmt!r} (use 'json' or 'text')")


__all__ = [
    "Validator",
    "ValidationCancelled",
    "CancelCheck",
    "SourceEngineFactory",
    "TargetConnectionFactory",
    "build_mysql_checksum_sql",
    "build_pg_checksum_sql",
    "build_mysql_pk_token_sql",
    "build_pg_pk_token_sql",
    "build_mysql_pk_first_page_sql",
    "build_mysql_pk_next_page_sql",
    "build_pg_pk_first_page_sql",
    "build_pg_pk_next_page_sql",
    "build_mysql_page_checksum_first_sql",
    "build_mysql_page_checksum_next_sql",
    "build_pg_page_checksum_first_sql",
    "build_pg_page_checksum_next_sql",
    "build_orphan_count_sql",
    "integer_pk_column",
    "reconcile_pk_streams",
    "render_text_report",
    "export_report",
]
