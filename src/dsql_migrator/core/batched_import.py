# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Fallback import path: built-in batched ``INSERT ... ON CONFLICT`` loader.

This module is the **in-process** import path of the data migrator (design.md
"Data Migration Design" -> "Import", Requirements 5.2/5.3/5.6). It is the sole
loader: rows stream straight from the exporter into batched
``INSERT ... ON CONFLICT`` statements over the tool's own boto3 IAM DSQL
connection, so Full Load needs no external binary and reuses the exact connection
path the rest of the tool already uses.

What it does, and the properties it satisfies:

1. **Batching with a hard cap (Property 2 -- transaction limits).** Incoming
   already-converted rows are split into batches. Each batch becomes ONE
   multi-row parameterized ``INSERT INTO <table> (cols) VALUES (...),(...),...
   ON CONFLICT [...] DO NOTHING|DO UPDATE`` executed in a single autocommit
   transaction. The default batch size is a few hundred rows
   (:data:`DEFAULT_BATCH_ROWS`) and :data:`MAX_BATCH_ROWS` (3,000) is a hard
   upper bound enforced by :class:`BatchedImportOptions` -- never exceeded, so a
   single write transaction always stays within the DSQL per-transaction row
   limit.

2. **Parameterized + safe SQL (Requirement 9.4).** Values are always bound via
   ``psycopg`` placeholders, never string-interpolated, and table/column
   identifiers are composed with ``psycopg.sql`` (``Identifier``/``SQL``), so a
   row value or column name can never break out into SQL.

3. **Idempotent loading (Property 3).** The on-conflict mode defaults to
   :attr:`OnConflictMode.DO_NOTHING`. Re-running a batch never creates duplicate
   rows because the ``INSERT`` carries ``ON CONFLICT``; ``DO_UPDATE`` is also
   idempotent for full-row upserts.

4. **OCC safety (Property 5).** Each batch's execution is wrapped in
   :func:`~dsql_migrator.core.occ.with_occ_retry`, so a ``SQLSTATE 40001``
   serialization failure is retried with backoff. The batch operation is
   idempotent, so a retry is safe; an unresolved conflict surfaces the original
   error and leaves no partial state (each batch is a single atomic statement).

5. **Bounded parallel connections (Property 2 -- parallelism).** Batches are
   loaded concurrently across a small, bounded pool of DSQL connections (default
   :data:`DEFAULT_PARALLELISM`). Connections are created lazily, capped at the
   configured parallelism, and reused across batches. The connection factory is
   injectable so unit tests never reach a real cluster; the default opens
   autocommit/TLS/IAM connections via
   :class:`~dsql_migrator.core.target_connection.DsqlConnector` (tokens stay
   confidential, Property 7).

6. **Resumability (Property 4 / Requirement 5.3).** Because the exporter
   (task 8.2) streams rows in keyset (primary-key) order, batch ``i`` always maps
   to the same deterministic primary-key range. Each batch is therefore a stable
   resumable unit keyed by :func:`batch_chunk_id`. When a
   :class:`~dsql_migrator.core.models.MigrationJob` is supplied, batches whose
   chunk id is already ``DONE`` are skipped, and the run converges to the same
   target state as an uninterrupted run.

7. **Post-load ``CREATE INDEX ASYNC`` (Property 2 -- DDL/DML separation).**
   Secondary indexes are built *after* all data batches succeed, as SEPARATE
   single-DDL transactions (one statement per ``execute``), never mixed with DML
   in one transaction. The index DDL statements are produced by the Schema
   Converter (task 5, ``TableConversion.index_ddls``) and are passed in here as
   input -- they are not regenerated. Each DDL is wrapped in
   :func:`~dsql_migrator.core.occ.with_occ_retry` for ``OC001`` (schema
   conflict) idempotency. ``CREATE INDEX ASYNC`` is non-blocking on DSQL: the
   statement returns quickly and the index is built in the background.

The row input is decoupled from any specific file reader: :meth:`import_rows`
accepts an iterable of already-converted rows (dict-like, keyed by column name --
exactly what the exporter's :class:`~dsql_migrator.core.exporter.ValueConverter`
produces) plus the :class:`~dsql_migrator.core.models.TableDef` for column order
and primary key. A structured :class:`BatchedImportResult` is returned.
"""

from __future__ import annotations

import logging
import os
import random
import re
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import Enum
from queue import Queue
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional

from psycopg import sql
from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import (
    ChunkState,
    MigrationJob,
    TableDef,
    TargetConnectionConfig,
)
from dsql_migrator.core.occ import (
    DEFAULT_BASE_DELAY_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    JitterFunc,
    SleepFunc,
    is_occ_conflict,
    with_occ_retry,
)
from dsql_migrator.core.target_connection import (
    DsqlConnector,
    TRANSIENT_CONN_SIGNATURES,
    is_transient_connection_error,
)

# Dev row-trace logger (child of the ``dsql_migrator`` package logger, level set
# from DSQL_MIGRATOR_LOG_LEVEL at app start). One DEBUG line PER BATCH (never per
# row) records the batch's chunk id, PK range, attempted/inserted/conflict counts
# and OCC retry count, so a developer can trace exactly which rows Full Load wrote.
# Guarded by ``isEnabledFor`` so production (INFO) builds nothing. Logs PK values +
# counts only -- NEVER row values (Property 7); a natural-key PK is the caller's risk.
_LOGGER = logging.getLogger(__name__)

# Default rows per batch. Matches the Aurora DSQL Loader's default (2000): a
# large multi-row INSERT that still stays under the DSQL per-transaction row cap
# (MAX_BATCH_ROWS=3000). The effective size is additionally clamped per table so
# batch_size x column_count never exceeds the DSQL 65,535-parameter statement
# limit (see _effective_batch_size).
DEFAULT_BATCH_ROWS = 2000

# DSQL caps a single statement at this many bind parameters; a multi-row INSERT
# uses batch_size x column_count parameters, so the batch is clamped to keep
# under this (mirrors the Loader's "too many arguments" guard).
MAX_STATEMENT_PARAMETERS = 65535

# Hard upper bound on rows per write transaction (Property 2). A batch larger
# than this is rejected by BatchedImportOptions rather than silently clamped.
MAX_BATCH_ROWS = 3000

# DSQL caps data modified in a single write transaction at 10 MiB (a single row
# can be up to 2 MiB). The row-count cap alone is not enough for wide rows, so a
# batch is ALSO bounded by this estimated payload-byte budget (set below 10 MiB
# to leave headroom for bind-parameter / wire encoding overhead). Wide-row tables
# then split into smaller batches automatically instead of having the whole
# transaction rejected for exceeding the byte limit.
MAX_BATCH_BYTES = 8 * 1024 * 1024

# Default number of concurrent in-flight INSERT batches (each on its own DSQL
# connection). Tuned up toward the Loader's parallel model for throughput while
# staying bounded so a single table does not exhaust DSQL connection/rate
# limits; combined with table-level parallelism upstream (so the total in-flight
# connections is table_parallelism x this).
DEFAULT_PARALLELISM = 8

# How many assembled batches a background reader may run AHEAD of the write pool.
# The source read is single-threaded (one keyset page in flight per table) and,
# without a buffer, the submit loop pulls the next batch off the reader inline --
# so page N+1's read never overlaps page N's writes and the write pool starves.
# A bounded prefetch queue lets a dedicated reader thread stay this many batches
# ahead, overlapping read and write, while the bound keeps in-flight memory
# capped (Property 2). Sized to comfortably feed a full write pool without letting
# the reader race unboundedly ahead: ~2x the parallelism, floor of a few.
def _prefetch_depth(parallelism: int) -> int:
    """Bounded prefetch queue depth for a write pool of ``parallelism`` workers."""
    return max(4, parallelism * 2)


# Measurement seam ONLY. Prefetch is ON by default (production behavior is
# unchanged); setting DSQL_MIGRATOR_FULL_LOAD_PREFETCH to a falsey value ("0",
# "false", "no", "off") makes the loader consume the source reader inline again --
# i.e. the exact pre-prefetch code path. This exists so a SINGLE deployed image can
# be A/B'd in-VPC (flag on vs off) instead of building two images from two commits;
# ECS RunTask can flip the env var per task. Nothing in the normal app reads or sets
# it, so it has no effect on a real migration.
def _prefetch_enabled() -> bool:
    """Whether the read-ahead prefetch queue is enabled (default True)."""
    raw = os.environ.get("DSQL_MIGRATOR_FULL_LOAD_PREFETCH")
    if raw is None:
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off", "")


# Connection-lost signatures for a drop that carries NO SQLSTATE. When the TLS
# socket is torn down MID-QUERY the server never sends an error code, so psycopg
# raises an OperationalError with ``sqlstate=None`` and only a libpq/OpenSSL
# The transient-connection classifier now lives in ``target_connection`` (the
# single DSQL connection layer) so EVERY connect path shares it -- the batched
# loader's pool leases here AND the per-table DROP+recreate connect in
# ``schema_applier``. Aliased to the historical private name so existing callers
# and tests keep working.
_is_transient_connection_error = is_transient_connection_error
_TRANSIENT_CONN_SIGNATURES = TRANSIENT_CONN_SIGNATURES


def _is_retryable_load_error(exc: BaseException) -> bool:
    """Retryable during a batch load: an OCC ``40001`` conflict OR a transient
    class-08 connection drop / expired token. A permanent data error (e.g. a
    type/constraint violation) is NOT retryable and surfaces as a batch failure.
    """
    return is_occ_conflict(exc) or _is_transient_connection_error(exc)


class OnConflictMode(str, Enum):
    """On-conflict behavior for idempotent loading (Property 3).

    Both modes are idempotent: ``DO_NOTHING`` (the safe default) keeps the
    existing target row on a primary-key conflict, and ``DO_UPDATE`` upserts the
    full row. Either way, re-loading a batch never creates duplicate rows because
    the load issues ``INSERT ... ON CONFLICT``.
    """

    DO_NOTHING = "do-nothing"
    DO_UPDATE = "do-update"
    # Plain INSERT with NO ``ON CONFLICT`` clause. Use for a clean/replace load
    # into a freshly-recreated (empty) target, where no conflict is possible:
    # Aurora DSQL's multi-row ``INSERT ... ON CONFLICT DO NOTHING`` was observed
    # to SILENTLY skip some non-conflicting rows while reporting them via the
    # cursor rowcount (a contiguous band dropped with no error/conflict). A plain
    # INSERT persists every row and surfaces any real conflict as an error, so it
    # is the correct, loss-free path when the target is known empty.
    NONE = "none"
    # Idempotent + DSQL-safe: per batch, SELECT which primary keys already exist
    # on the target, then plain-INSERT only the missing rows (NO ``ON CONFLICT``).
    # Avoids the DSQL multi-row ON CONFLICT silent-drop while staying idempotent
    # AND preserving any newer row a concurrently-running CDC sink already wrote
    # (an existing PK is left untouched). This is the safe Full Load path when the
    # target is NOT known-empty (e.g. CDC is already streaming into it). Supports
    # single- or composite-column keys.
    SKIP_EXISTING = "skip-existing"


def _effective_batch_size(batch_size: int, column_count: int) -> int:
    """Clamp ``batch_size`` so a batch stays under the DSQL parameter limit.

    A multi-row INSERT binds ``rows x columns`` parameters; DSQL rejects a
    statement with more than :data:`MAX_STATEMENT_PARAMETERS`. Returns the
    largest size <= ``batch_size`` that fits (at least 1).
    """
    if column_count <= 0:
        return batch_size
    return max(1, min(batch_size, MAX_STATEMENT_PARAMETERS // column_count))

# A connection factory opens one new DSQL connection (autocommit + TLS + IAM).
# Injectable so unit tests never reach a real cluster.
ConnectionFactory = Callable[[], Any]


class BatchedImportError(RuntimeError):
    """Base error for the built-in batched ``INSERT`` import path.

    Raised for configuration problems that prevent a load from starting (e.g.
    ``DO_UPDATE`` requested without any conflict-key columns, or a table with no
    columns). Per-batch runtime failures are not raised from here: they are
    captured into :class:`BatchedImportResult` as ``failures`` so the rest of the
    load can proceed and be resumed.
    """


@dataclass(frozen=True)
class _BatchWork:
    """One unit of work: the rows of a single batch plus how to load them."""

    chunk_id: str
    table_name: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    on_conflict: OnConflictMode
    rows: tuple[Mapping[str, object], ...]


@dataclass
class _BatchOutcome:
    """The result of attempting to load one batch."""

    chunk_id: str
    status: str
    rows_loaded: int = 0
    conflicts: int = 0
    error: Optional[str] = None


class BatchedImportOptions(BaseModel):
    """Configuration for one built-in batched import invocation.

    ``on_conflict`` defaults to the idempotent :attr:`OnConflictMode.DO_NOTHING`
    (Property 3). ``key_columns`` are the ``ON CONFLICT`` conflict-target
    columns; when empty they default to the table's primary key at import time.
    ``batch_size`` is validated to ``1 <= batch_size <= MAX_BATCH_ROWS`` so a
    single write transaction can never exceed the DSQL row limit (Property 2).
    ``parallelism`` bounds the number of concurrent DSQL connections.
    """

    model_config = ConfigDict(extra="forbid")

    on_conflict: OnConflictMode = Field(
        default=OnConflictMode.DO_NOTHING,
        description="Idempotent conflict handling; defaults to DO_NOTHING.",
    )
    key_columns: list[str] = Field(
        default_factory=list,
        description="ON CONFLICT target columns; defaults to the table PK.",
    )
    batch_size: int = Field(
        default=DEFAULT_BATCH_ROWS,
        ge=1,
        le=MAX_BATCH_ROWS,
        description="Rows per batch; hard-capped at MAX_BATCH_ROWS (Property 2).",
    )
    parallelism: int = Field(
        default=DEFAULT_PARALLELISM,
        ge=1,
        description="Maximum number of concurrent DSQL connections.",
    )


class QuarantineRecord(BaseModel):
    """One Full Load row that could not be loaded and was isolated (quarantined).

    Credential-free (Property 7): it carries only the affected table, the failed
    row's PRIMARY-KEY identity (so an operator can locate it -- never the non-key
    column values), and the failure reason. This is the Full Load analogue of the
    CDC sink's dead-letter quarantine: a single poison row is set aside (and
    surfaced in the error log) so it cannot block the rest of its batch.
    """

    model_config = ConfigDict(extra="forbid")

    table: str
    primary_key: str
    error_code: Optional[str] = None
    message: str


class BatchedImportResult(BaseModel):
    """Structured outcome of a built-in batched import.

    ``rows_loaded`` / ``conflicts`` count this run only (skipped, already-``DONE``
    batches are not re-counted), so on a resumed run they reflect just the work
    done now while the target converges to the same final state as an
    uninterrupted run (Property 4). ``failures`` is the number of batches that
    could not be loaded; ``indexes_created`` is the number of
    ``CREATE INDEX ASYNC`` statements issued after the data load.
    """

    model_config = ConfigDict(extra="forbid")

    rows_loaded: int = Field(default=0, ge=0)
    conflicts: int = Field(default=0, ge=0)
    batches_completed: int = Field(default=0, ge=0)
    batches_skipped: int = Field(default=0, ge=0)
    failures: int = Field(default=0, ge=0)
    first_error: Optional[str] = Field(
        default=None,
        description="Message of the first failed batch (for diagnosis), if any.",
    )
    cancelled: bool = Field(
        default=False,
        description=(
            "True when the load stopped early on a cooperative cancel before all "
            "batches ran, so the table is incomplete and should be retried."
        ),
    )
    indexes_created: int = Field(default=0, ge=0)
    index_failures: list[str] = Field(
        default_factory=list,
        description=(
            "One credential-free message per post-load ``CREATE INDEX ASYNC`` that "
            "could not be created. Kept SEPARATE from ``failures`` (which counts "
            "DATA batches) because a failed index does not cost a single row: the "
            "table's data is complete and usable, only an access path is missing. "
            "The most common cause is DSQL's 24-indexes-per-table limit, which the "
            "assessor now flags up front (TOO_MANY_INDEXES) -- but it is reported "
            "here too, since an index can also fail for other reasons."
        ),
    )
    quarantined: int = Field(
        default=0,
        ge=0,
        description=(
            "Number of rows isolated (quarantined) because they hit a permanent, "
            "non-retryable error; the rest of their batch still loaded."
        ),
    )
    quarantine_records: list[QuarantineRecord] = Field(default_factory=list)


def batch_chunk_id(
    table_name: str, index: int, shard_id: Optional[int] = None
) -> str:
    """Return the deterministic resumable chunk id for batch ``index``.

    Keyset-ordered export (task 8.2) makes batch ``index`` map to the same
    primary-key range on every run, so this id is a stable resumable unit
    (Property 4). Zero-padded so ids sort in batch order.

    ``shard_id`` (reader range sharding): when a large table is read by K
    concurrent shard readers, each shard restarts its batch ``index`` at 0, so the
    id must be namespaced by shard to stay unique. ``shard_id=None`` keeps the
    original single-reader id, so an unsharded table's resume state is byte-for-byte
    unchanged. A sharded run's ids look like ``table#s00-batch-000000``.

    NOTE on resume with sharding: a sharded ``chunk_id`` is stably mapped to a PK
    range only if the shard ranges are recomputed identically, i.e. the source
    ``MIN(pk)``/``MAX(pk)`` are unchanged between runs (K is fixed by config, but the
    range boundaries come from live MIN/MAX). The Full Load engine does NOT pass a
    ``job`` into ``import_rows`` (whole-table re-load under idempotent SKIP_EXISTING
    on resume), so batch-level ``done_ids`` skipping is not exercised for a sharded
    table today; a future caller that wires ``job=`` with ``shards>1`` must pin the
    ranges (persist first-run MIN/MAX) before relying on batch-level resume.
    """
    if shard_id is None:
        return f"{table_name}#batch-{index:06d}"
    return f"{table_name}#s{shard_id:02d}-batch-{index:06d}"


def _pg_table_identifier(name: str) -> sql.Identifier:
    """Return a psycopg identifier for a possibly schema-qualified table name.

    Splits ``schema.table`` so it composes to ``"schema"."table"`` rather than a
    single quoted ``"schema.table"`` identifier. Cluster-wide introspection
    qualifies names as ``database.table``; the converter creates a matching
    PostgreSQL schema on the target, so the load target must be qualified too.
    """
    schema, separator, obj = name.partition(".")
    if separator and schema and obj:
        return sql.Identifier(schema, obj)
    return sql.Identifier(name)


def build_insert_statement(
    table_name: str,
    columns: list[str],
    num_rows: int,
    on_conflict: OnConflictMode,
    key_columns: list[str],
) -> sql.Composed:
    """Build a multi-row parameterized ``INSERT ... ON CONFLICT`` statement.

    All values are emitted as ``psycopg`` placeholders (never interpolated), and
    every identifier is composed with :class:`psycopg.sql.Identifier`, so neither
    a row value nor a column/table name can break out into SQL (Requirement 9.4).
    The caller supplies a flat parameter sequence of ``num_rows * len(columns)``
    values in row-major, ``columns`` order.

    On-conflict handling (Property 3): ``DO_NOTHING`` emits
    ``ON CONFLICT [(keys)] DO NOTHING`` (a conflict target is added when
    ``key_columns`` is given). ``DO_UPDATE`` emits
    ``ON CONFLICT (keys) DO UPDATE SET <non-key> = EXCLUDED.<non-key>``; when
    every column is a key column (nothing to update) it degrades to
    ``DO NOTHING`` to stay idempotent.
    """
    if num_rows < 1:
        raise ValueError("num_rows must be a positive integer")
    if not columns:
        raise ValueError("columns must not be empty")

    column_identifiers = sql.SQL(", ").join(sql.Identifier(name) for name in columns)
    single_row = sql.SQL("({})").format(
        sql.SQL(", ").join(sql.Placeholder() for _ in columns)
    )
    all_rows = sql.SQL(", ").join(single_row for _ in range(num_rows))

    statement = sql.SQL("INSERT INTO {table} ({columns}) VALUES {rows} {conflict}").format(
        table=_pg_table_identifier(table_name),
        columns=column_identifiers,
        rows=all_rows,
        conflict=_on_conflict_clause(on_conflict, key_columns, columns),
    )
    return statement


def _on_conflict_clause(
    on_conflict: OnConflictMode, key_columns: list[str], columns: list[str]
) -> sql.Composable:
    """Build the ``ON CONFLICT`` clause for the configured idempotent mode."""
    if on_conflict is OnConflictMode.NONE:
        # Plain INSERT: no ON CONFLICT clause at all. Correct only when the target
        # is known empty (clean/replace load); a real conflict then surfaces as an
        # error instead of being silently dropped (DSQL ON CONFLICT loss guard).
        return sql.SQL("")
    if on_conflict is OnConflictMode.DO_UPDATE:
        target = sql.SQL(", ").join(sql.Identifier(name) for name in key_columns)
        update_columns = [name for name in columns if name not in set(key_columns)]
        if not update_columns:
            # Every column is part of the key: nothing to update, stay idempotent.
            return sql.SQL("ON CONFLICT ({target}) DO NOTHING").format(target=target)
        assignments = sql.SQL(", ").join(
            sql.SQL("{column} = EXCLUDED.{column}").format(column=sql.Identifier(name))
            for name in update_columns
        )
        return sql.SQL("ON CONFLICT ({target}) DO UPDATE SET {assignments}").format(
            target=target, assignments=assignments
        )

    # DO_NOTHING: add an explicit conflict target when key columns are known.
    if key_columns:
        target = sql.SQL(", ").join(sql.Identifier(name) for name in key_columns)
        return sql.SQL("ON CONFLICT ({target}) DO NOTHING").format(target=target)
    return sql.SQL("ON CONFLICT DO NOTHING")


class _ConnectionPool:
    """A bounded pool of lazily-created, reusable connections.

    At most ``size`` connections are ever created (Property 2 -- bounded
    parallelism). A leased connection is returned to the pool on context exit and
    reused by a later batch. :meth:`close_all` closes every connection created.
    """

    def __init__(self, factory: ConnectionFactory, size: int) -> None:
        """Seed ``size`` empty slots; connections are created on first lease."""
        self._factory = factory
        self._slots: "Queue[Optional[Any]]" = Queue()
        for _ in range(size):
            self._slots.put(None)
        self._created: list[Any] = []
        self._lock = threading.Lock()

    @contextmanager
    def lease(self) -> Iterator[Any]:
        """Lease a connection, creating it on first use, returning it on exit.

        If the leased connection raises while in use (a dropped connection, an
        expired IAM token, or any other error), it is closed and the slot is
        refilled with ``None`` so the next lease creates a FRESH connection (which
        re-mints a short-lived IAM token via the factory). This prevents one
        broken connection from cascading the same failure across every later
        batch that would otherwise reuse it, and lets a retry recover.
        """
        connection = self._slots.get()
        if connection is None:
            connection = self._factory()
            with self._lock:
                self._created.append(connection)
        ok = False
        try:
            yield connection
            ok = True
        finally:
            if ok:
                self._slots.put(connection)
            else:
                try:
                    connection.close()
                except Exception:  # noqa: BLE001 - best-effort discard
                    pass
                with self._lock:
                    if connection in self._created:
                        self._created.remove(connection)
                self._slots.put(None)

    def close_all(self) -> None:
        """Close every connection this pool created (best-effort)."""
        with self._lock:
            created = list(self._created)
            self._created.clear()
        for connection in created:
            _safe_close(connection)


class BatchedImporter:
    """Built-in batched ``INSERT ... ON CONFLICT`` importer (fallback path).

    The connection factory, OCC retry budget, and OCC sleep/jitter are injectable
    so unit tests run instantly and never reach a real DSQL cluster. Construct
    with :class:`BatchedImportOptions` plus either an explicit
    ``connection_factory`` or a ``target`` (used to build a
    :class:`~dsql_migrator.core.target_connection.DsqlConnector`-backed factory),
    then call :meth:`import_rows`.
    """

    def __init__(
        self,
        options: Optional[BatchedImportOptions] = None,
        *,
        target: Optional[TargetConnectionConfig] = None,
        connection_factory: Optional[ConnectionFactory] = None,
        occ_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        occ_base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
        sleep: SleepFunc = time.sleep,
        jitter: JitterFunc = random.random,
    ) -> None:
        """Create an importer.

        ``connection_factory`` opens one new autocommit/TLS DSQL connection per
        call; when omitted, ``target`` is required and a
        :class:`~dsql_migrator.core.target_connection.DsqlConnector`-backed
        factory is built (IAM tokens stay confidential, Property 7). ``sleep`` and
        ``jitter`` are forwarded to :func:`~dsql_migrator.core.occ.with_occ_retry`
        and are injectable so tests are deterministic and never sleep for real.
        """
        if connection_factory is None and target is None:
            raise BatchedImportError(
                "either connection_factory or target must be provided"
            )
        self._options = options or BatchedImportOptions()
        self._connection_factory = connection_factory or _default_connection_factory(
            target  # type: ignore[arg-type]  # guarded above
        )
        self._occ_max_attempts = occ_max_attempts
        self._occ_base_delay = occ_base_delay
        self._sleep = sleep
        self._jitter = jitter
        # Side-channel sink for poison rows isolated during a load (thread-safe so
        # parallel batch workers can append). Reset at the start of each
        # import_rows call so the result reflects only that call.
        self._quarantine: list[QuarantineRecord] = []
        self._quarantine_lock = threading.Lock()
        # Statement cache: identical (table, columns, num_rows, on_conflict,
        # key_columns) tuples reuse the same sql.Composed object. For a typical
        # large-table load ~99.99% of batches have the same shape, eliminating
        # ~40,000 object allocations per batch (GIL-held).
        self._statement_cache: dict[tuple, object] = {}

    def import_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        table: TableDef,
        *,
        index_ddls: Optional[list[str]] = None,
        job: Optional[MigrationJob] = None,
        on_batch_loaded: Optional[Callable[[int, int], None]] = None,
        should_cancel: Optional[Callable[[], bool]] = None,
        on_conflict: Optional[OnConflictMode] = None,
        shard_sources: Optional[
            "list[Iterable[Mapping[str, object]]]"
        ] = None,
        key_columns: Optional[list[str]] = None,
    ) -> BatchedImportResult:
        """Load ``rows`` into ``table`` in bounded-parallel idempotent batches.

        Splits ``rows`` (already converted, keyed by column name) into batches of
        at most ``options.batch_size`` rows, loads them concurrently across a
        bounded connection pool with per-batch OCC retry (Properties 2/3/5), then
        -- only if every batch succeeded -- issues the post-load
        ``CREATE INDEX ASYNC`` statements in ``index_ddls`` as separate single-DDL
        transactions (Property 2). When ``job`` is supplied, batches already
        ``DONE`` are skipped and each batch's chunk state/progress is updated, so
        a resumed run converges to the same target as an uninterrupted one
        (Property 4). ``on_batch_loaded``, when given, is called as each batch
        succeeds with ``(rows_inserted, rows_skipped)`` -- where ``rows_skipped``
        are rows that already existed on the target and were left unchanged by the
        idempotent ``ON CONFLICT`` load -- so a caller can surface live cumulative
        progress (counting skipped rows as progress) instead of only a final
        total (Requirement 8.3). A re-load that mostly skips already-present rows
        therefore still shows movement rather than appearing stuck at zero. It is
        invoked from the draining thread (never concurrently for one call).
        ``key_columns`` (optional) overrides the conflict/skip key with the TARGET
        table's actual primary key (e.g. a composite ``(leading, id)``); when
        omitted it falls back to ``options.key_columns`` then the source PK.
        Returns a structured :class:`BatchedImportResult`.
        """
        columns = [column.name for column in table.columns]
        if not columns:
            raise BatchedImportError(
                f"table '{table.name}' has no columns to import"
            )
        # Conflict-key resolution order: an explicit ``key_columns`` (the TARGET
        # table's actual primary key, e.g. a composite (leading, id) chosen in
        # Schema Conversion) wins, then options.key_columns, then the SOURCE table's
        # PK. Passing the target PK here is what makes ON CONFLICT / SKIP_EXISTING
        # match the constraint that actually exists on the recreated target -- see
        # the Full Load engine, which derives it from the applied target DDL.
        key_columns = (
            list(key_columns)
            if key_columns
            else (list(self._options.key_columns) or list(table.primary_key))
        )
        effective_on_conflict = on_conflict or self._options.on_conflict
        if effective_on_conflict is OnConflictMode.DO_UPDATE and not key_columns:
            raise BatchedImportError(
                "on_conflict=DO_UPDATE requires key_columns or a table primary key"
            )

        done_ids = _done_chunk_ids(job)
        skipped_ids: list[str] = []
        # Reader range sharding: when ``shard_sources`` holds K disjoint-PK-range
        # row streams, build K work iterators (each ``chunk_id``-namespaced by shard
        # and appending to ``skipped_ids`` under a lock). Otherwise the single
        # ``rows`` stream builds one unsharded work iterator -- byte-for-byte the
        # previous behavior (``shard_id=None`` keeps the original chunk ids).
        work_iters: list[Iterator[_BatchWork]]
        if shard_sources:
            skipped_lock = threading.Lock()
            work_iters = [
                self._iter_work(
                    shard_rows, table, columns, key_columns, done_ids, skipped_ids,
                    effective_on_conflict, shard_id=i, skipped_lock=skipped_lock,
                )
                for i, shard_rows in enumerate(shard_sources)
            ]
        else:
            work_iters = [
                self._iter_work(
                    rows, table, columns, key_columns, done_ids, skipped_ids,
                    effective_on_conflict,
                )
            ]

        pool = _ConnectionPool(self._connection_factory, self._options.parallelism)
        with self._quarantine_lock:
            self._quarantine = []
        try:
            outcomes, stopped_early = self._run_data_batches(
                work_iters, pool, on_batch_loaded, should_cancel
            )
            failures = sum(1 for outcome in outcomes if outcome.status == "FAILED")
            indexes_created = 0
            index_failures: list[str] = []
            # Skip post-load indexing when the table is incomplete (a stop) or
            # any batch failed -- indexing only makes sense for a full table.
            if failures == 0 and not stopped_early and index_ddls:
                indexes_created, index_failures = self._create_indexes(
                    pool, index_ddls
                )
        finally:
            pool.close_all()

        if job is not None:
            _apply_outcomes_to_job(job, outcomes, skipped_ids)

        result = _aggregate_result(
            outcomes, skipped_ids, indexes_created, cancelled=stopped_early
        )
        # Indexes that could not be created. Reported alongside a SUCCESSFUL data
        # load: the rows are all present, so this is a missing access path to fix
        # later, not a reason to fail (and re-run) the table.
        result.index_failures = list(index_failures)
        with self._quarantine_lock:
            quarantined = list(self._quarantine)
        result.quarantined = len(quarantined)
        result.quarantine_records = quarantined
        return result

    def _iter_work(
        self,
        rows: Iterable[Mapping[str, object]],
        table: TableDef,
        columns: list[str],
        key_columns: list[str],
        done_ids: set[str],
        skipped_ids: list[str],
        on_conflict: OnConflictMode,
        shard_id: Optional[int] = None,
        skipped_lock: Optional[threading.Lock] = None,
    ) -> Iterator[_BatchWork]:
        """Yield a work item per non-skipped batch (consumes rows lazily).

        Batches already ``DONE`` (per ``done_ids``) are recorded in ``skipped_ids``
        and their rows are dropped without being loaded -- ``ON CONFLICT`` makes
        re-loading safe, but skipping avoids needless work on resume (Property 4).

        ``shard_id`` namespaces the ``chunk_id`` for one reader shard (batch
        ``index`` restarts at 0 per shard). ``skipped_lock`` guards ``skipped_ids``
        when K shard producers append concurrently; ``None`` (single reader) needs
        no lock.
        """
        columns_tuple = tuple(columns)
        key_tuple = tuple(key_columns)
        batch_size = _effective_batch_size(self._options.batch_size, len(columns_tuple))
        for index, batch in enumerate(
            _iter_batches(rows, batch_size, MAX_BATCH_BYTES)
        ):
            chunk_id = batch_chunk_id(table.name, index, shard_id)
            if chunk_id in done_ids:
                if skipped_lock is not None:
                    with skipped_lock:
                        skipped_ids.append(chunk_id)
                else:
                    skipped_ids.append(chunk_id)
                continue
            yield _BatchWork(
                chunk_id=chunk_id,
                table_name=table.name,
                columns=columns_tuple,
                key_columns=key_tuple,
                on_conflict=on_conflict,
                rows=tuple(batch),
            )

    @staticmethod
    def _prefetch(
        work_iter: Iterator[_BatchWork], depth: int
    ) -> Iterator[_BatchWork]:
        """Read ``work_iter`` on a background thread, buffering up to ``depth`` items.

        The underlying iterator is a single-threaded source reader: pulling the
        next batch issues the next keyset page + per-row conversion. Consuming it
        inline on the submit thread serializes read and write. This wrapper runs a
        dedicated producer thread that keeps reading ahead into a bounded queue
        (maxsize=``depth``), so page N+1's read overlaps page N's writes while the
        write pool drains -- the queue's bound preserves the bounded-memory
        guarantee (Property 2): the reader blocks on ``put`` once ``depth`` batches
        are buffered.

        Yields items in order. A producer exception is re-raised on the consumer
        thread (after the already-buffered items are drained is NOT required -- we
        surface it promptly so the batch failure is reported the same as before).
        The producer thread is a daemon and is joined when the generator is closed
        (consumer stops early, e.g. on cancel), so no thread leaks.
        """
        # Sentinels distinguish clean end-of-stream from a producer error.
        _END = object()
        q: "Queue[object]" = Queue(maxsize=depth)
        error: list[BaseException] = []
        stop = threading.Event()

        def _produce() -> None:
            try:
                for item in work_iter:
                    if stop.is_set():
                        break
                    q.put(item)  # blocks when the buffer is full (backpressure)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the consumer
                error.append(exc)
            finally:
                q.put(_END)

        producer = threading.Thread(
            target=_produce, name="fullload-prefetch", daemon=True
        )
        producer.start()
        try:
            while True:
                item = q.get()
                if item is _END:
                    break
                yield item  # type: ignore[misc]
            if error:
                raise error[0]
        finally:
            # Consumer stopped (cancel / exhausted / exception): tell the producer
            # to stop and drain any buffered item so its put() unblocks, then join.
            stop.set()
            while producer.is_alive():
                try:
                    q.get_nowait()
                except Exception:  # noqa: BLE001 - empty; producer is exiting
                    break
            producer.join(timeout=1.0)

    @staticmethod
    def _prefetch_many(
        work_iters: "list[Iterator[_BatchWork]]", depth: int
    ) -> Iterator[_BatchWork]:
        """Merge K shard readers into one bounded queue feeding a single write pool.

        Reader range sharding: each of ``work_iters`` is one shard's work stream
        (a disjoint PK range, its own source snapshot connection). K daemon
        producer threads drain them CONCURRENTLY into one shared ``Queue(maxsize=
        depth)``, so K source reads overlap each other AND the single write pool --
        the whole point, since one reader tops out at one CPU core. The consumer
        (the submit loop) still pulls from ONE queue, so the write side, progress
        accounting, and OCC budget are unchanged from the single-reader path.

        Order is NOT preserved across shards (batches interleave as shards produce
        them), which is fine: each batch's ``chunk_id`` already encodes its shard +
        index, so resume/idempotency is per-batch, not positional. The bound still
        caps in-flight memory. Any shard's exception is re-raised on the consumer;
        all producer threads are stopped and joined when the generator closes.
        """
        if not work_iters:
            # No shards -> nothing to produce. Return an empty stream instead of
            # blocking forever (with zero producers, the "last producer posts _END"
            # rule would never fire). The caller only routes >1 shard here, so this
            # is a guard against a future miswire, not a normal path.
            return
        _END = object()
        q: "Queue[object]" = Queue(maxsize=depth)
        error: list[BaseException] = []
        stop = threading.Event()
        remaining = [len(work_iters)]
        remaining_lock = threading.Lock()

        def _produce(it: "Iterator[_BatchWork]") -> None:
            try:
                for item in it:
                    if stop.is_set():
                        break
                    q.put(item)
            except BaseException as exc:  # noqa: BLE001 - re-raised on the consumer
                error.append(exc)
                stop.set()  # a failed shard stops the others promptly
            finally:
                # The LAST producer to finish posts the single end-of-stream marker.
                with remaining_lock:
                    remaining[0] -= 1
                    last = remaining[0] == 0
                if last:
                    q.put(_END)

        producers = [
            threading.Thread(
                target=_produce, args=(it,),
                name=f"fullload-prefetch-s{i:02d}", daemon=True,
            )
            for i, it in enumerate(work_iters)
        ]
        for p in producers:
            p.start()
        try:
            while True:
                item = q.get()
                if item is _END:
                    break
                yield item  # type: ignore[misc]
            if error:
                raise error[0]
        finally:
            stop.set()
            # A producer blocked on q.put() into a full queue only exits once a slot
            # frees, so keep draining WHILE any producer is alive -- but bound it by
            # wall-clock so a producer genuinely wedged mid-page (its source socket
            # read hasn't timed out yet) can't hold us for minutes. Producers are
            # daemons: a still-hung one dies with the process / when its read times
            # out, matching the single-reader _prefetch (which also returns ~1s).
            deadline = time.monotonic() + 2.0
            while any(p.is_alive() for p in producers):
                if time.monotonic() >= deadline:
                    break
                try:
                    q.get(timeout=0.05)
                except Exception:  # noqa: BLE001 - empty within the window; re-check
                    pass
            for p in producers:
                p.join(timeout=1.0)

    def _run_data_batches(
        self,
        work_iters: "list[Iterator[_BatchWork]]",
        pool: _ConnectionPool,
        on_batch_loaded: Optional[Callable[[int, int], None]] = None,
        should_cancel: Optional[Callable[[], bool]] = None,
    ) -> tuple[list[_BatchOutcome], bool]:
        """Execute batches concurrently with at most ``parallelism`` in flight.

        Submissions are throttled so no more than ``parallelism`` batches are
        ever pending, which keeps both the in-flight memory and the number of
        live connections bounded (Property 2). As each batch resolves ``DONE``,
        ``on_batch_loaded`` (if given) is called with ``(rows_inserted,
        rows_skipped)`` so the caller can report live cumulative progress --
        including rows skipped as already-present, so a mostly-skipped re-load
        still shows movement instead of appearing stuck at zero. It runs only on
        this draining thread, so the callback never needs its own
        synchronization. When ``should_cancel`` returns ``True``, no further
        batches are submitted (the already in-flight ones are still drained), so a
        stop takes effect within one batch rather than waiting out a whole large
        table. Returns the batch outcomes and whether the load stopped early
        (left unsubmitted work).
        """
        outcomes: list[_BatchOutcome] = []
        parallelism = self._options.parallelism
        in_flight: dict[Future[tuple[int, int]], str] = {}
        stopped_early = False

        def drain_one() -> None:
            done, _ = wait(set(in_flight), return_when=FIRST_COMPLETED)
            for future in done:
                chunk_id = in_flight.pop(future)
                outcome = _resolve_outcome(future, chunk_id)
                outcomes.append(outcome)
                if (
                    on_batch_loaded is not None
                    and outcome.status == "DONE"
                    and (outcome.rows_loaded or outcome.conflicts)
                ):
                    on_batch_loaded(outcome.rows_loaded, outcome.conflicts)

        # Read source pages AHEAD of the write pool on a dedicated thread so page
        # N+1's read overlaps page N's writes (the single serial reader would
        # otherwise starve the pool). The prefetch queue is bounded, preserving the
        # bounded-memory guarantee; closing the generator (below / on cancel) stops
        # and joins the reader thread. Nothing else in the loop changes: `work` is
        # the same _BatchWork, just delivered pre-read.
        #
        # The prefetch wrapper is a no-op passthrough when disabled via the
        # measurement seam (_prefetch_enabled), so the loop below consumes the raw
        # `work_iter` inline -- the exact pre-prefetch behavior -- letting one image
        # be A/B'd. `.close()` in the finally is safe on a plain generator too.
        #
        # With K shard readers (reader range sharding), _prefetch_many runs K
        # producer threads into one bounded queue; with one reader it's the original
        # single-producer _prefetch. When prefetch is disabled, a single reader is
        # consumed inline (K shards still need the merge, so _prefetch_many is used
        # regardless of the seam -- the seam only reproduces the pre-prefetch,
        # pre-sharding single-reader path, which by definition has one work-iter).
        if len(work_iters) > 1:
            prefetched: Iterator[_BatchWork] = self._prefetch_many(
                work_iters, _prefetch_depth(parallelism)
            )
        elif _prefetch_enabled():
            prefetched = self._prefetch(work_iters[0], _prefetch_depth(parallelism))
        else:
            prefetched = work_iters[0]
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            try:
                for work in prefetched:
                    if should_cancel is not None and should_cancel():
                        # Stop pulling new batches; remaining rows stay unloaded so
                        # the table is reported incomplete (retryable, idempotent).
                        stopped_early = True
                        break
                    future = executor.submit(self._load_batch, work, pool)
                    in_flight[future] = work.chunk_id
                    if len(in_flight) >= parallelism:
                        drain_one()
            finally:
                # Stop + join the reader thread promptly on cancel/early-exit
                # (also runs on the normal exhausted path -- a no-op there).
                prefetched.close()
            while in_flight:
                drain_one()
        return outcomes, stopped_early

    def _load_batch(
        self, work: _BatchWork, pool: _ConnectionPool
    ) -> tuple[int, int]:
        """Load one batch, isolating poison rows on a permanent error.

        Tries the batch via :meth:`_execute_for_mode` (which retries OCC ``40001``
        and transient connection drops). If it still fails with a NON-retryable
        error (a permanent data/constraint error that a re-run cannot fix), the
        batch is binary-split and each half retried recursively, down to a single
        row: that row is the poison row -- it is quarantined (its PK + reason
        recorded, never its non-key values) and skipped, while every other row in
        the batch still loads. This is the Full Load analogue of the CDC sink's
        DLQ quarantine: one bad row never blocks ~thousands of good rows, and the
        loss is surfaced (in the error log) rather than silent.

        Returns ``(rows_inserted, conflicts)`` for the loaded rows; quarantined
        rows are accumulated on the importer's quarantine sink (read via
        :meth:`import_rows`'s result).
        """
        try:
            return self._execute_for_mode(work, pool)
        except Exception as exc:  # noqa: BLE001 - classify retryable vs poison
            if _is_retryable_load_error(exc):
                raise  # retryable budget exhausted -> a real batch failure
            # Only a genuine row-level DB error (one carrying a SQLSTATE) is a
            # candidate poison row. A structural/programming error (e.g. a missing
            # primary key -> BatchedImportError, with no sqlstate) is NOT a poison
            # row: propagate it so the batch fails clearly rather than quarantining
            # every row.
            if getattr(exc, "sqlstate", None) is None:
                raise
            if len(work.rows) <= 1:
                self._quarantine_one(work, exc)
                return 0, 0
            mid = len(work.rows) // 2
            left = replace(work, rows=work.rows[:mid])
            right = replace(work, rows=work.rows[mid:])
            left_inserted, left_conflicts = self._load_batch(left, pool)
            right_inserted, right_conflicts = self._load_batch(right, pool)
            return left_inserted + right_inserted, left_conflicts + right_conflicts

    def _quarantine_one(self, work: _BatchWork, exc: BaseException) -> None:
        """Record one poison row (PK + reason only) to the quarantine sink."""
        row = work.rows[0]
        if work.key_columns:
            primary_key = ", ".join(
                f"{column}={row.get(column)!r}" for column in work.key_columns
            )
        else:
            primary_key = "(no primary key)"
        record = QuarantineRecord(
            table=work.table_name,
            primary_key=primary_key,
            error_code=getattr(exc, "sqlstate", None),
            # _safe_error, not str(exc): the raw driver text carries the failing
            # row's column values in its DETAIL line (Property 7). The sqlstate is
            # kept separately in error_code for triage.
            message=_safe_error(exc),
        )
        with self._quarantine_lock:
            self._quarantine.append(record)

    def _execute_for_mode(
        self, work: _BatchWork, pool: _ConnectionPool
    ) -> tuple[int, int]:
        """Load one batch as a single multi-row ``INSERT`` with OCC retry.

        Returns ``(rows_inserted, conflicts)``. The execution is wrapped in
        :func:`~dsql_migrator.core.occ.with_occ_retry`; because the statement is a
        single idempotent ``INSERT ... ON CONFLICT``, retrying a ``40001`` is safe
        and never duplicates rows (Properties 3/5).
        """
        attempted = len(work.rows)
        # Count OCC attempts so the trace can report retries. The wrapper runs once
        # per attempt (each ``40001`` retry re-invokes it), so attempts-1 == retries.
        attempts = 0

        if work.on_conflict is OnConflictMode.SKIP_EXISTING:
            # DSQL-safe idempotent path: SELECT existing PKs, plain-INSERT the rest.
            def _counted_skip(pool_):
                nonlocal attempts
                attempts += 1
                return self._execute_skip_existing(pool_, work)

            retried = with_occ_retry(
                max_attempts=self._occ_max_attempts,
                base_delay=self._occ_base_delay,
                sleep=self._sleep,
                jitter=self._jitter,
                retryable=_is_retryable_load_error,
            )(_counted_skip)
            inserted, conflicts = retried(pool)
            if _LOGGER.isEnabledFor(logging.DEBUG):
                first_pk, last_pk = _batch_pk_range(work)
                _LOGGER.debug(
                    "import batch chunk=%s table=%s pk_range=[%s..%s] attempted=%d "
                    "inserted=%d skipped=%d on_conflict=%s occ_retries=%d",
                    work.chunk_id, work.table_name, first_pk, last_pk, attempted,
                    inserted, conflicts, work.on_conflict.value, max(0, attempts - 1),
                )
            return inserted, conflicts

        cache_key = (
            work.table_name, work.columns, len(work.rows),
            work.on_conflict, work.key_columns,
        )
        statement = self._statement_cache.get(cache_key)
        if statement is None:
            statement = build_insert_statement(
                work.table_name,
                list(work.columns),
                len(work.rows),
                work.on_conflict,
                list(work.key_columns),
            )
            self._statement_cache[cache_key] = statement
        params = _flatten_params(work.rows, work.columns)

        def _counted_execute(pool_, statement_, params_, attempted_):
            nonlocal attempts
            attempts += 1
            return self._execute_insert(pool_, statement_, params_, attempted_)

        retried = with_occ_retry(
            max_attempts=self._occ_max_attempts,
            base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
            retryable=_is_retryable_load_error,
        )(_counted_execute)
        inserted, conflicts = retried(pool, statement, params, attempted)

        # One DEBUG line PER BATCH (never per row): chunk id + PK range + counts +
        # OCC retries. Guarded so production (INFO) pays nothing; PK values from the
        # first/last row in hand (keyset order) + counts only -- never row values.
        if _LOGGER.isEnabledFor(logging.DEBUG):
            first_pk, last_pk = _batch_pk_range(work)
            _LOGGER.debug(
                "import batch chunk=%s table=%s pk_range=[%s..%s] attempted=%d "
                "inserted=%d conflicts=%d on_conflict=%s occ_retries=%d",
                work.chunk_id, work.table_name, first_pk, last_pk, attempted,
                inserted, conflicts, work.on_conflict.value, max(0, attempts - 1),
            )
        return inserted, conflicts

    def _execute_insert(
        self,
        pool: _ConnectionPool,
        statement: sql.Composed,
        params: list[object],
        attempted: int,
    ) -> tuple[int, int]:
        """Run one batch ``INSERT`` on a pooled connection (single transaction).

        Each attempt leases its own connection from the bounded pool, so a retry
        never holds a connection while waiting. ``rows_inserted`` is taken from
        the cursor ``rowcount`` (rows actually inserted under ``ON CONFLICT``);
        ``conflicts`` is the remainder that hit an existing row.
        """
        with pool.lease() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(statement, params)
                rowcount = getattr(cursor, "rowcount", -1)
            finally:
                _safe_close(cursor)
        inserted = rowcount if isinstance(rowcount, int) and rowcount >= 0 else attempted
        inserted = min(inserted, attempted)
        return inserted, attempted - inserted

    def _execute_skip_existing(
        self, pool: _ConnectionPool, work: _BatchWork
    ) -> tuple[int, int]:
        """Idempotent DSQL-safe load: insert missing PKs, skip ones already there.

        Optimistic-first: try a single plain ``INSERT`` of the whole batch (no
        pre-SELECT). When no key overlaps -- the dominant large-table initial load
        case -- this is one round-trip per batch. If DSQL raises a unique violation
        (``SQLSTATE 23505``) because a key already exists (a concurrent CDC sink
        insert or a re-run), it falls back to ``SELECT`` the batch's existing keys
        then plain-``INSERT`` only the missing rows. Either way it stays idempotent,
        never overwrites a newer row the CDC sink already wrote, avoids the DSQL
        multi-row ``ON CONFLICT`` silent row-drop entirely, and supports single- or
        composite-column keys. Returns ``(inserted, skipped)``.

        The SELECT-filter fallback retries on a 23505 in the gap between its SELECT
        and INSERT (re-deriving the existing set, which converges); exhausting the
        bounded retries surfaces an error -- never a silent loss.
        """
        unique_violation_sqlstate = "23505"
        key_columns = list(work.key_columns)
        if not key_columns:
            raise BatchedImportError(
                "on_conflict=SKIP_EXISTING requires a primary key "
                f"(table '{work.table_name}' has none)"
            )
        total = len(work.rows)
        if total == 0:
            return 0, 0

        def _key_of(row: Mapping[str, object]) -> tuple:
            return tuple(row[name] for name in key_columns)

        # Optimistic fast path: one plain INSERT of the whole batch, NO pre-SELECT.
        # On a no-overlap load -- the dominant large-scale initial CDC-coexisting case
        # -- this is a single round-trip per batch (as fast as a clean load). Only
        # when a key already exists (a concurrent CDC insert or a re-run) does DSQL
        # raise a unique violation (23505); we then fall through to the SELECT-
        # filter path, paying the extra read ONLY on real overlap, not every batch.
        full_insert = build_insert_statement(
            work.table_name, list(work.columns), total, OnConflictMode.NONE, key_columns
        )
        with pool.lease() as connection:
            cursor = connection.cursor()
            try:
                cursor.execute(full_insert, _flatten_params(work.rows, work.columns))
                rowcount = getattr(cursor, "rowcount", -1)
                inserted = (
                    rowcount if isinstance(rowcount, int) and rowcount >= 0 else total
                )
                return min(inserted, total), total - min(inserted, total)
            except Exception as exc:  # noqa: BLE001 - inspect sqlstate
                if getattr(exc, "sqlstate", None) != unique_violation_sqlstate:
                    raise
                # Overlap exists -> fall back to the SELECT-filter path below.
            finally:
                _safe_close(cursor)

        key_idents = sql.SQL(", ").join(sql.Identifier(name) for name in key_columns)
        row_placeholder = sql.SQL("({})").format(
            sql.SQL(", ").join(sql.Placeholder() for _ in key_columns)
        )
        select_sql = sql.SQL(
            "SELECT {keys} FROM {table} WHERE ({keys}) IN ({tuples})"
        ).format(
            keys=key_idents,
            table=_pg_table_identifier(work.table_name),
            tuples=sql.SQL(", ").join(row_placeholder for _ in work.rows),
        )
        select_params = [row[name] for row in work.rows for name in key_columns]
        max_attempts = max(1, self._occ_max_attempts)
        last_error: Optional[BaseException] = None
        for _attempt in range(max_attempts):
            with pool.lease() as connection:
                cursor = connection.cursor()
                try:
                    cursor.execute(select_sql, select_params)
                    existing = {tuple(row) for row in cursor.fetchall()}
                    missing = [
                        row for row in work.rows if _key_of(row) not in existing
                    ]
                    if not missing:
                        return 0, total
                    insert_sql = build_insert_statement(
                        work.table_name,
                        list(work.columns),
                        len(missing),
                        OnConflictMode.NONE,
                        key_columns,
                    )
                    try:
                        cursor.execute(
                            insert_sql, _flatten_params(missing, work.columns)
                        )
                    except Exception as exc:  # noqa: BLE001 - inspect sqlstate
                        if getattr(exc, "sqlstate", None) == unique_violation_sqlstate:
                            # A concurrent insert won the race for a missing key;
                            # re-derive existing and retry the remainder.
                            last_error = exc
                            continue
                        raise
                    rowcount = getattr(cursor, "rowcount", -1)
                    inserted = (
                        rowcount
                        if isinstance(rowcount, int) and rowcount >= 0
                        else len(missing)
                    )
                    inserted = min(inserted, len(missing))
                    return inserted, total - inserted
                finally:
                    _safe_close(cursor)
        raise BatchedImportError(
            f"SKIP_EXISTING load for table '{work.table_name}' exhausted "
            f"{max_attempts} attempts due to concurrent unique violations"
        ) from last_error

    def _create_indexes(
        self, pool: _ConnectionPool, index_ddls: list[str]
    ) -> "tuple[int, list[str]]":
        """Issue post-load ``CREATE INDEX ASYNC`` statements, one per transaction.

        Each DDL string (already produced and safely quoted by the Schema
        Converter, task 5) runs as its own single-statement autocommit
        transaction -- never mixed with DML (Property 2) -- and is wrapped in OCC
        retry for ``OC001`` schema-conflict idempotency (Property 5). DSQL builds
        the index asynchronously in the background, so each statement returns
        promptly.

        Returns ``(created, failures)``. A failing index is ISOLATED rather than
        raised: indexes are built AFTER every row is written, so letting one
        propagate marked the whole table FAILED even though its data was complete --
        which also blocked the Validation gate on a table that had nothing missing.
        Since an index is an access path, not data, each DDL is attempted
        independently (one bad index no longer stops the remaining ones) and the
        failures are returned for the caller to surface as a warning.
        """
        retried = with_occ_retry(
            max_attempts=self._occ_max_attempts,
            base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
        )(_execute_ddl)
        created = 0
        failures: list[str] = []
        with pool.lease() as connection:
            for ddl in index_ddls:
                try:
                    retried(connection, ddl)
                except Exception as exc:  # noqa: BLE001 - isolate per-index failure
                    # Log-safe: the DDL is tool-generated (no row values, no
                    # credentials) and the driver message is a schema-level error.
                    failures.append(f"{_index_name_of(ddl)}: {_safe_error(exc)}")
                    _LOGGER.warning(
                        "Post-load index creation failed (data is unaffected): %s",
                        failures[-1],
                    )
                else:
                    created += 1
        return created, failures


def _default_connection_factory(target: TargetConnectionConfig) -> ConnectionFactory:
    """Build a connection factory backed by :class:`DsqlConnector`.

    Each call opens a new autocommit/TLS connection authenticated with a
    short-lived IAM token; the token is generated and kept confidential by the
    connector (Property 7 / Requirement 5.4).
    """
    connector = DsqlConnector(target)
    return connector.connect


def _index_name_of(ddl: str) -> str:
    """Extract the index name from a ``CREATE [UNIQUE] INDEX ASYNC <name> ON ...``.

    Best-effort labelling for a failure message so the operator can identify WHICH
    index did not get created. Falls back to a truncated DDL when the shape is
    unexpected (the DDL is tool-generated, so it never carries row values).
    """
    match = re.search(
        r'CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:ASYNC\s+)?"?([^"\s(]+)"?',
        ddl,
        re.IGNORECASE,
    )
    return match.group(1) if match else ddl[:60]


def _safe_error(exc: BaseException) -> str:
    """Render an exception as a short, credential- and VALUE-free single line.

    Keeps only the exception type and the FIRST line of its message. A psycopg
    error's ``str(exc)`` preserves the server ``DETAIL:``/``Failing row contains
    (...)`` lines, which carry the offending row's COLUMN VALUES (e.g. a duplicate
    email, a token) -- writing those to the error log / activity log / CloudWatch
    would violate Property 7. The primary line ("duplicate key value violates
    unique constraint ...") is the actionable part and is value-free, so taking the
    first line alone both explains the failure and drops the value dump. Mirrors
    :func:`dsql_migrator.core.validator._safe_error_message` (kept local to avoid a
    cross-module import). Collapsing with ``" ".join(str(exc).split())`` was NOT
    enough -- it merely folds the DETAIL line onto the same line, values intact.
    """
    first_line = str(exc).strip().splitlines()[0].strip() if str(exc).strip() else ""
    if len(first_line) > 300:
        first_line = first_line[:297] + "..."
    name = type(exc).__name__
    return f"{name}: {first_line}" if first_line else name


def _execute_ddl(connection: Any, ddl: str) -> None:
    """Execute a single DDL statement on ``connection`` (one transaction)."""
    cursor = connection.cursor()
    try:
        cursor.execute(ddl)
    finally:
        _safe_close(cursor)


def _estimate_row_bytes(row: Mapping[str, object]) -> int:
    """Cheaply estimate a row's payload size in bytes (for the batch byte cap).

    A heuristic sum over the row's values -- ``bytes`` by length, ``str`` by
    character count, everything else by its ``str`` length, ``None`` ~1. It need
    not be exact: it only has to keep a batch comfortably under the DSQL 10 MiB
    per-write-transaction limit, for which ``MAX_BATCH_BYTES`` leaves headroom.
    """
    total = 0
    for value in row.values():
        if value is None:
            total += 1
        elif isinstance(value, (bytes, bytearray, memoryview)):
            total += len(value)
        elif isinstance(value, str):
            total += len(value)
        else:
            total += len(str(value))
    return total


def _iter_batches(
    rows: Iterable[Mapping[str, object]],
    batch_size: int,
    max_bytes: Optional[int] = None,
) -> Iterator[list[Mapping[str, object]]]:
    """Split ``rows`` into in-order lists bounded by row count AND payload bytes.

    A batch is flushed when it reaches ``batch_size`` rows OR, when ``max_bytes``
    is set, when adding the next row would push the estimated payload past that
    budget (a non-empty batch always keeps at least one row, since a single row
    cannot be split). The byte bound matters at scale: DSQL caps data modified in
    one write transaction at 10 MiB, so wide rows must split before the row-count
    cap is reached or the whole transaction is rejected. Rows are pulled lazily so
    only one batch is materialized at a time, keeping memory bounded regardless of
    table size.

    Byte estimation is O(cols) per row, but it MUST run for every row: a batch whose
    first row is small but whose later rows are large would otherwise blow past
    ``max_bytes`` unchecked. So ``batch_bytes`` is a true running sum (every row
    estimated once and added), and a batch flushes the moment adding the next row
    would exceed the budget. There is no first-row extrapolation shortcut -- an
    earlier version sampled only the first row and skipped the rest when the
    extrapolation was under budget, which silently let a size-skewed batch exceed
    the DSQL 10 MiB per-transaction limit (recovered only by a costly recursive
    split downstream). Memory stays bounded (one batch at a time); the estimate is a
    cheap arithmetic sum, not a re-serialization.
    """
    batch: list[Mapping[str, object]] = []
    batch_bytes = 0
    for row in rows:
        if max_bytes is not None:
            row_bytes = _estimate_row_bytes(row)
            # Flush BEFORE appending when this row would push a non-empty batch over
            # the budget. A single oversized row still forms its own batch (a row
            # cannot be split); the downstream loader splits/handles it.
            if batch and batch_bytes + row_bytes > max_bytes:
                yield batch
                batch = []
                batch_bytes = 0
            batch_bytes += row_bytes
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
            batch_bytes = 0
    if batch:
        yield batch


def _flatten_params(
    rows: tuple[Mapping[str, object], ...], columns: tuple[str, ...]
) -> list[object]:
    """Flatten ``rows`` into a row-major parameter list in ``columns`` order.

    Uses a list comprehension instead of an append loop: CPython compiles this
    to a single BUILD_LIST + LIST_EXTEND bytecode path that is ~40% faster for
    the nested-iteration pattern (avoids per-element LOAD_ATTR + CALL overhead).
    """
    return [row.get(col) for row in rows for col in columns]


def _batch_pk_range(work: "_BatchWork") -> tuple[object, object]:
    """Return ``(first_pk, last_pk)`` for a batch's single-column key, else (None, None).

    Rows arrive in keyset (ascending PK) order from the exporter, so the first and
    last row bound the batch's PK range. Used only for the dev DEBUG trace; reads
    the PK from rows already in hand (never re-queries the source) and only the key
    column value -- never other row values (Property 7). Composite/keyless batches
    have no single PK to range over, so the range is omitted (None, None) rather
    than raising.
    """
    if len(work.key_columns) != 1 or not work.rows:
        return (None, None)
    pk = work.key_columns[0]
    return (work.rows[0].get(pk), work.rows[-1].get(pk))


def _resolve_outcome(
    future: "Future[tuple[int, int]]", chunk_id: str
) -> _BatchOutcome:
    """Turn a completed batch future into a success/failure :class:`_BatchOutcome`.

    A failed batch is captured (not raised) so the rest of the load proceeds and
    can be resumed; each batch is a single atomic statement, so a failure leaves
    no partial state (Property 5).
    """
    try:
        inserted, conflicts = future.result()
    except Exception as exc:  # noqa: BLE001 - recorded as a per-batch failure
        # _safe_error, not str(exc): this text becomes result.first_error and flows
        # into the per-table failure message / error log / activity log, so it must
        # not carry the driver DETAIL line's row values (Property 7).
        return _BatchOutcome(chunk_id=chunk_id, status="FAILED", error=_safe_error(exc))
    return _BatchOutcome(
        chunk_id=chunk_id,
        status="DONE",
        rows_loaded=inserted,
        conflicts=conflicts,
    )


def _done_chunk_ids(job: Optional[MigrationJob]) -> set[str]:
    """Return the set of chunk ids already ``DONE`` in ``job`` (for skipping)."""
    if job is None:
        return set()
    return {chunk.chunk_id for chunk in job.chunks if chunk.status == "DONE"}


def _apply_outcomes_to_job(
    job: MigrationJob, outcomes: list[_BatchOutcome], skipped_ids: list[str]
) -> None:
    """Upsert this run's batch outcomes into ``job`` and recompute progress.

    Skipped (already-``DONE``) batches are left untouched. Each executed batch's
    chunk state is created or updated, ``attempts`` is incremented, and overall
    ``progress_pct``/``error_count`` are recomputed from the chunk states.
    """
    by_id = {chunk.chunk_id: chunk for chunk in job.chunks}
    for outcome in outcomes:
        chunk = by_id.get(outcome.chunk_id)
        if chunk is None:
            chunk = ChunkState(chunk_id=outcome.chunk_id)
            job.chunks.append(chunk)
            by_id[outcome.chunk_id] = chunk
        chunk.attempts += 1
        chunk.status = "DONE" if outcome.status == "DONE" else "FAILED"
        if outcome.status == "DONE":
            chunk.rows_loaded = outcome.rows_loaded
    _ = skipped_ids  # skipped chunks keep their existing DONE state untouched
    _recompute_job_progress(job)


def _recompute_job_progress(job: MigrationJob) -> None:
    """Recompute ``progress_pct`` and ``error_count`` from chunk states."""
    total = len(job.chunks)
    if total == 0:
        job.progress_pct = 0.0
        job.error_count = 0
        return
    done = sum(1 for chunk in job.chunks if chunk.status == "DONE")
    job.progress_pct = round(done / total * 100.0, 4)
    job.error_count = sum(1 for chunk in job.chunks if chunk.status == "FAILED")


def _aggregate_result(
    outcomes: list[_BatchOutcome],
    skipped_ids: list[str],
    indexes_created: int,
    *,
    cancelled: bool = False,
) -> BatchedImportResult:
    """Aggregate per-batch outcomes into a :class:`BatchedImportResult`."""
    completed = [outcome for outcome in outcomes if outcome.status == "DONE"]
    failures = sum(1 for outcome in outcomes if outcome.status == "FAILED")
    first_error = next(
        (
            outcome.error
            for outcome in outcomes
            if outcome.status == "FAILED" and outcome.error
        ),
        None,
    )
    return BatchedImportResult(
        rows_loaded=sum(outcome.rows_loaded for outcome in completed),
        conflicts=sum(outcome.conflicts for outcome in completed),
        batches_completed=len(completed),
        batches_skipped=len(skipped_ids),
        failures=failures,
        first_error=first_error,
        cancelled=cancelled,
        indexes_created=indexes_created,
    )


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise
        pass


__all__ = [
    "DEFAULT_BATCH_ROWS",
    "MAX_BATCH_ROWS",
    "DEFAULT_PARALLELISM",
    "ConnectionFactory",
    "OnConflictMode",
    "BatchedImportError",
    "BatchedImportOptions",
    "BatchedImportResult",
    "BatchedImporter",
    "batch_chunk_id",
    "build_insert_statement",
    "safe_error_message",
]


# Public alias for the value-free error renderer, so callers outside this module
# (e.g. the Full Load engine's per-table failure handler) sanitize driver
# exceptions the same way before logging them (Property 7).
safe_error_message = _safe_error
