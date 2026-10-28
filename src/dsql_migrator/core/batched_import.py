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
   row value or column name can never break out into SQL. Each INSERT shape is
   rendered ONCE to the exact bytes psycopg used to rebuild on every execute
   (``$1..$n`` placeholders, :func:`render_insert_statement`) and sent through
   psycopg's :class:`~psycopg.RawCursor`, so no batch -- and no retry -- renders it
   again, and a ``%`` in a table or column name is just a character.

3. **Idempotent loading (Property 3).** The on-conflict mode defaults to
   :attr:`OnConflictMode.DO_NOTHING`. Re-running a batch never creates duplicate
   rows because the ``INSERT`` carries ``ON CONFLICT``; ``DO_UPDATE`` is also
   idempotent for full-row upserts.

4. **OCC safety (Property 5).** Each batch's execution is wrapped in
   :func:`~dsql_migrator.core.occ.with_occ_retry`, so a ``SQLSTATE 40001``
   serialization failure is retried with backoff. The batch operation is
   idempotent, so a retry is safe; an unresolved conflict surfaces the original
   error and leaves no partial state (each batch is a single atomic statement).
   Aurora DSQL's temporary "server unavailable" (``SQLSTATE XX000``) shares that
   budget: it can mean the batch DID commit (the documented uncertain outcome), and
   a re-run is exact even then (every mode re-runs to the same rows; only the counts
   move from loaded to skipped), so it is retried like a ``40001`` and, once the
   budget runs out, fails the batch -- never a row (no quarantine).

5. **Bounded parallel connections (Property 2 -- parallelism).** Batches are
   loaded concurrently across a small, bounded pool of DSQL connections (default
   :data:`DEFAULT_PARALLELISM`). Connections are created lazily, capped at the
   configured parallelism, and reused across batches -- also after the server rejected
   only a statement (an OCC ``40001``, a data error), while one whose session may have
   ended is replaced by a fresh one (``_ConnectionPool.lease``). The connection factory is
   injectable so unit tests never reach a real cluster; the default opens
   autocommit/TLS/IAM connections via
   :class:`~dsql_migrator.core.target_connection.DsqlConnector` (tokens stay
   confidential, Property 7).

6. **Resumability (Property 4 / Requirement 5.3).** Because the exporter
   (task 8.2) streams rows in keyset (primary-key) order, batch ``i`` always maps
   to the same deterministic primary-key range. Each batch is therefore a stable
   resumable unit. When a :class:`~dsql_migrator.core.models.MigrationJob` is
   supplied, batches at or below the per-shard resume high-water
   (:attr:`~dsql_migrator.core.models.MigrationJob.resume_batch_watermark`, the
   highest contiguously-completed batch index) are skipped, and the run converges
   to the same target state as an uninterrupted run. The high-water is a compact
   O(shards) marker rather than a per-batch record, so resume state does not grow
   with the row count (a billion-row table's memory stays bounded).

7. **Post-load ``CREATE INDEX ASYNC`` (Property 2 -- DDL/DML separation).**
   Secondary indexes are built *after* all data batches succeed, as SEPARATE
   single-DDL transactions (one statement per ``execute``), never mixed with DML
   in one transaction. The index DDL statements are produced by the Schema
   Converter (task 5, ``TableConversion.index_ddls``) and are passed in here as
   input -- they are not regenerated. Each DDL is wrapped in
   :func:`~dsql_migrator.core.occ.with_occ_retry` for ``OC001`` (schema
   conflict) idempotency. ``CREATE INDEX ASYNC`` is non-blocking on DSQL: the
   statement returns quickly and the index is built in the background. An
   ``XX000`` is retried the same way; because that attempt's CREATE may have
   committed, a later "already exists" is reported with a note to check the
   target, never counted as created. The Full Load engine passes none to
   :meth:`BatchedImporter.import_rows`: a Drop & reload table's go on the empty table
   before any row loads (P4d), the rest after every table's data (P4a), each pass on one
   connection (:meth:`BatchedImporter.index_pass`), so no index DDL changes the schema
   under another table's batches (OC001). With secondary indexes in place during a load, a
   row a UNIQUE index other than the primary key rejects (23505) is a poison row, never a
   key overlap (:meth:`BatchedImporter._violates_secondary_unique`).

8. **Write-path counters.** Every batch, attempt, retry (by SQLSTATE, with Aurora DSQL's
   OC000 / OC001 told apart), 23505 recovery, connect, connection kept after an error and
   read-ahead queue wait is counted into a :class:`~dsql_migrator.core.load_stats.LoadStats`
   collector -- the importer's own, or the load unit's after
   :meth:`BatchedImporter.bind_load_stats` -- counts and timings only (Property 7), in
   bounded memory.

The row input is decoupled from any specific file reader: :meth:`import_rows`
accepts an iterable of already-converted rows (dict-like, keyed by column name --
exactly what the exporter's :class:`~dsql_migrator.core.exporter.ValueConverter`
produces) plus the :class:`~dsql_migrator.core.models.TableDef` for column order
and primary key. A structured :class:`BatchedImportResult` is returned.
"""

from __future__ import annotations

import hashlib
import logging
import os
import random
import re
import threading
import time
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, replace
from enum import Enum
from queue import Empty, Full, Queue
from typing import Any, Callable, Iterable, Iterator, Mapping, Optional, Sequence

import psycopg
from psycopg import sql
from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.load_stats import LoadStats
from dsql_migrator.core.models import (
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
    is_server_unavailable,
    is_transient_connection_error,
)
from dsql_migrator.core.target_introspector import read_primary_key_index_name

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
# How long one drain wait blocks before returning to the caller's cancel poll. Short
# enough that Stop feels immediate, long enough that a healthy batch is normally awaited
# in a single wait rather than spun on.
_DRAIN_POLL_SECONDS = 1.0

MAX_BATCH_BYTES = 8 * 1024 * 1024

# Default number of concurrent in-flight INSERT batches (each on its own DSQL
# connection). Tuned up toward the Loader's parallel model for throughput while
# staying bounded so a single table does not exhaust DSQL connection/rate
# limits. The Full Load lowers it per load unit (cap_parallelism): at most 4 per key
# range the unit reads, and a share of the run's budget (P3, the engine's
# _unit_in_flight), so a run's in-flight connections are no longer
# table_parallelism x this.
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


def _put_counted(q: "Queue[object]", item: object, stats: Optional[LoadStats]) -> None:
    """``q.put(item)``, telling ``stats`` how long the READER waited on a full queue.

    Only a put that had to wait is timed, so a reader that keeps up pays one
    ``put_nowait``. Waiting here means the writes are the bottleneck.
    """
    try:
        q.put_nowait(item)
    except Full:
        started = time.perf_counter()
        q.put(item)  # blocks until a writer frees a slot (backpressure)
        if stats is not None:
            stats.add_reader_blocked(time.perf_counter() - started)


def _get_counted(q: "Queue[object]", stats: Optional[LoadStats]) -> object:
    """``q.get()``, telling ``stats`` how long the WRITERS waited on an empty queue.

    The consumer only asks for a batch when a write slot is free, so waiting here means
    the source read is the bottleneck.
    """
    try:
        return q.get_nowait()
    except Empty:
        started = time.perf_counter()
        item = q.get()
        if stats is not None:
            stats.add_writer_wait(time.perf_counter() - started)
        return item


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
    """Retryable during a batch load: an OCC ``40001`` conflict, Aurora DSQL's temporary
    "server unavailable" (``XX000``), OR a transient class-08 connection drop / expired
    token. A permanent data error (e.g. a type/constraint violation) is NOT retryable and
    is isolated to its row. A retryable error that outlasts the budget fails the BATCH,
    never a row: it is raised, so the batch is FAILED (retryable), not split or quarantined.
    """
    return (
        is_occ_conflict(exc)
        or is_server_unavailable(exc)
        or _is_transient_connection_error(exc)
    )


# Appended to an index failure when an attempt to create the index answered Aurora DSQL's
# XX000: that attempt's CREATE may have committed. A later "already exists" (42P07) is NOT
# counted as created -- index names are per schema on Aurora DSQL and come from the source,
# where they are per table, so the index found could be another table's. Shared by the
# post-load index pass and Schema Conversion's bare CREATE INDEX (core/schema_applier).
UNCONFIRMED_INDEX_NOTE = (
    " (Aurora DSQL answered server unavailable (SQLSTATE XX000) to an attempt to create "
    "it, so it may have been created anyway: check whether it exists on this table "
    "before you add it)"
)
# The post-load pass's note when instead the connection dropped while an attempt's CREATE
# ran (G25): the statement may have committed before its reply was lost, the same
# uncertain outcome as an XX000.
UNCONFIRMED_INDEX_DROP_NOTE = (
    " (the connection dropped while an attempt to create it was running, so it may have "
    "been created anyway: check whether it exists on this table before you add it)"
)


def is_unconfirmed_index_failure(message: object) -> bool:
    """True when an index failure carries :data:`UNCONFIRMED_INDEX_NOTE` or
    :data:`UNCONFIRMED_INDEX_DROP_NOTE`: an attempt to create the index ended with no
    confirmed outcome (Aurora DSQL's XX000, or a connection dropped during the statement),
    so the index may exist after all. The Full Load reports such an index as "not
    confirmed" rather than missing, keyed on these markers instead of a copy of their
    text."""
    text = str(message or "")
    return UNCONFIRMED_INDEX_NOTE in text or UNCONFIRMED_INDEX_DROP_NOTE in text


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


class IndexTargetUnreachable(Exception):
    """Aurora DSQL did not answer one of a table's index DDLs within its retry budget.

    Raised only by the run-level index pass (:meth:`BatchedImporter.index_pass`), where a
    target that stopped answering would otherwise be recorded as every later table's
    missing index: the pass stops instead and leaves the table, and those after it, for
    Retry unfinished tables. ``created`` counts this table's DDLs submitted before it and
    ``failures`` holds the permanent failures recorded before it (index name and SQLSTATE
    message only, as :meth:`BatchedImporter.create_indexes` reports them).
    """

    def __init__(self, created: int, failures: "Sequence[str]") -> None:
        super().__init__(
            f"Aurora DSQL did not answer an index DDL ({created} submitted before it)"
        )
        self.created = int(created)
        self.failures = list(failures)


@dataclass(frozen=True)
class _BatchWork:
    """One unit of work: the rows of a single batch plus how to load them.

    ``shard_id`` / ``batch_index`` identify the batch's position for the compact
    resume high-water (:class:`_BatchResumeTracker`): the draining thread advances
    that shard's contiguous completed prefix by ``batch_index`` as the batch resolves.
    """

    chunk_id: str
    table_name: str
    columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    on_conflict: OnConflictMode
    rows: tuple[Mapping[str, object], ...]
    shard_id: Optional[int] = None
    batch_index: int = 0


@dataclass
class _BatchOutcome:
    """The result of attempting to load one batch."""

    chunk_id: str
    status: str
    rows_loaded: int = 0
    conflicts: int = 0
    error: Optional[str] = None


@dataclass
class _RunAggregate:
    """Running fold of per-batch outcomes for one ``import_rows`` call.

    Folding each outcome as it resolves means a whole-table load never retains one
    ``_BatchOutcome`` per batch (O(rows / batch_size) objects -- ~500k for a billion-row
    table). Every field the result needs is a foldable running total.
    """

    rows_loaded: int = 0
    conflicts: int = 0
    batches_completed: int = 0
    failures: int = 0
    first_error: Optional[str] = None

    def fold(self, outcome: "_BatchOutcome") -> None:
        if outcome.status == "DONE":
            self.batches_completed += 1
            self.rows_loaded += outcome.rows_loaded
            self.conflicts += outcome.conflicts
        else:  # FAILED
            self.failures += 1
            if self.first_error is None and outcome.error:
                self.first_error = outcome.error


def _shard_key(shard_id: Optional[int]) -> str:
    """String key for a shard reader (``""`` for the unsharded single reader).

    JSON object keys are strings, and this key is persisted on
    :attr:`MigrationJob.resume_batch_watermark`, so map ``shard_id`` to a stable
    string once here rather than at every call site.
    """
    return "" if shard_id is None else str(shard_id)


class _BatchResumeTracker:
    """Compact, memory-bounded per-shard resume high-water for one ``import_rows`` run.

    Replaces the O(rows / batch_size) per-batch ``ChunkState`` list a resumed
    (SKIP_EXISTING) load used to accumulate on its ephemeral resume job -- a
    billion-row table would otherwise retain hundreds of thousands of objects
    (contributing to the Fargate OOM the memory-pressure logger warns about). Because
    batches stream in keyset order per shard (``batch_chunk_id`` maps ``index`` to a
    stable PK range), a completed CONTIGUOUS prefix means every range up to that index
    is committed, so the resume point a same-process retry needs is just the highest
    contiguously-completed index per shard -- one int per shard.

    Batches complete concurrently (up to ``parallelism`` in flight), so completions
    are not strictly in index order. Out-of-order completions are held in an ``ahead``
    set that the frontier absorbs as it advances; its size is the completion-reorder
    distance -- typically ~``parallelism`` (a stuck low-index batch could enlarge it,
    but it holds small ints, not the per-batch model objects the old code retained, so
    it is far smaller in every case). On a FAILED batch the shard is SEALED (its
    frontier stops advancing and its ahead-set is dropped): a retry re-runs everything
    past the frontier idempotently anyway, so there is no reason to keep accumulating
    indices past a gap -- which HARD-bounds memory even when a batch fails midway
    through a huge shard. The durable resume state carried across attempts is the
    ``watermark`` dict: O(shards), independent of row count.

    The tracker is written only from the single draining thread (``record_*``) and
    read once at the end (:meth:`watermark`); it is NOT consulted for skipping (that
    uses the immutable prior-run watermark), so it needs no lock.
    """

    def __init__(self, initial: Optional[Mapping[str, int]] = None) -> None:
        # shard key -> highest contiguous completed batch index (seeded from the prior
        # attempt so this run's completions extend, rather than restart, the prefix).
        self._frontier: dict[str, int] = dict(initial or {})
        self._ahead: dict[str, set[int]] = {}
        self._sealed: set[str] = set()

    def record_done(self, shard_id: Optional[int], index: int) -> None:
        key = _shard_key(shard_id)
        if key in self._sealed:
            return
        frontier = self._frontier.get(key, -1)
        if index <= frontier:
            return  # already covered (e.g. a re-run of a skipped range)
        ahead = self._ahead.setdefault(key, set())
        ahead.add(index)
        while (frontier + 1) in ahead:
            ahead.discard(frontier + 1)
            frontier += 1
        self._frontier[key] = frontier

    def record_failed(self, shard_id: Optional[int]) -> None:
        key = _shard_key(shard_id)
        # A gap: freeze the frontier and drop the ahead-set so a shard that fails
        # midway cannot accumulate O(rows) done-ahead indices behind the gap.
        self._sealed.add(key)
        self._ahead.pop(key, None)

    def watermark(self) -> dict[str, int]:
        """The per-shard contiguous completed prefix to carry into the next attempt."""
        return dict(self._frontier)


class _SkipCounter:
    """Thread-safe count of batches skipped as already-committed on resume.

    Only the COUNT is ever needed (``BatchedImportResult.batches_skipped``), so a
    resume that skips most of a billion-row table keeps a single int rather than a
    per-batch id list. K shard readers increment concurrently, so it locks; the
    single-reader path passes no lock and never contends.
    """

    def __init__(self) -> None:
        self._n = 0
        self._lock = threading.Lock()

    def add(self, *, locked: bool) -> None:
        if locked:
            with self._lock:
                self._n += 1
        else:
            self._n += 1

    @property
    def value(self) -> int:
        return self._n


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

    NOTE on resume with sharding: batch-level resume skips by the per-shard
    high-water (:class:`_BatchResumeTracker`), keyed on ``batch index`` within a
    shard. That is stably mapped to a PK range only if the shard ranges are recomputed
    identically between attempts, i.e. the source ``MIN(pk)``/``MAX(pk)`` are unchanged
    (K is fixed by config, but the range boundaries come from live MIN/MAX). This holds
    for the loader's SAME-PROCESS source-drop retry -- the only place ``job=`` is wired
    -- because the source is read-only for the duration and the ephemeral resume job is
    never persisted (a cross-process "Retry failed tables" recreates the target and
    restarts the table from index 0, so a shifted MIN/MAX cannot cause a stale skip).
    """
    if shard_id is None:
        return f"{table_name}#batch-{index:06d}"
    return f"{table_name}#s{shard_id:02d}-batch-{index:06d}"


def _pg_table_identifier(
    name: str, identifier: "type[sql.Identifier]" = sql.Identifier
) -> sql.Identifier:
    """Return a psycopg identifier for a possibly schema-qualified table name.

    Splits ``schema.table`` so it composes to ``"schema"."table"`` rather than a
    single quoted ``"schema.table"`` identifier. Cluster-wide introspection
    qualifies names as ``database.table``; the converter creates a matching
    PostgreSQL schema on the target, so the load target must be qualified too.
    ``identifier`` is the class to build it with (:class:`_ParamSafeIdentifier` for a
    statement a normal cursor runs with parameters).
    """
    schema, separator, obj = name.partition(".")
    if separator and schema and obj:
        return identifier(schema, obj)
    return identifier(name)


class _ParamSafeIdentifier(sql.Identifier):
    """:class:`psycopg.sql.Identifier` for a statement a NORMAL cursor runs WITH parameters.

    Such a cursor scans the whole text for ``%s`` placeholders, quoted names included, and
    turns ``%%`` back into ``%``, so the same quoting with each ``%`` doubled reaches the
    server as the name itself. Without it a name such as ``pct%off`` failed the scan,
    ``a%sb`` became a placeholder and ``x%%y`` arrived as ``x%y``. Only for a statement
    executed with parameters: without them nothing is scanned and the doubling would stay.
    """

    def as_bytes(self, context: Any = None) -> bytes:
        return super().as_bytes(context).replace(b"%", b"%%")


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

    The loader itself sends this statement as :func:`render_insert_statement` renders
    it, once per shape.
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


class _RenderedStatement(sql.Composable):
    """A statement already rendered to the bytes the server receives (``$n`` placeholders).

    A :class:`~psycopg.sql.Composable`, so ``cursor.execute`` takes it as it took the
    ``Composed``, and :meth:`as_bytes` returns the same bytes object every time. Only a
    cursor that sends a query as it is may run it (:func:`_insert_cursor`): a normal cursor
    scans it for ``%s`` placeholders and refuses the parameters it finds no place for -- an
    error, never a different statement.
    """

    def __init__(self, query: bytes) -> None:
        super().__init__(query)

    def as_bytes(self, context: Any = None) -> bytes:
        return self._obj


def render_insert_statement(
    table_name: str,
    columns: list[str],
    num_rows: int,
    on_conflict: OnConflictMode,
    key_columns: list[str],
) -> _RenderedStatement:
    """:func:`build_insert_statement`'s statement, rendered once to the bytes the server receives.

    Byte for byte the query psycopg made of that ``Composed`` on every execute -- every name
    quoted by :class:`psycopg.sql.Identifier`, the same ``ON CONFLICT`` clause, each value's
    ``%s`` numbered ``$1..$n`` in row-major order -- made once per shape instead. psycopg
    caches no query over 4096 bytes or 50 parameters, so for a bulk INSERT it re-joined the
    ``Composed`` and re-scanned it for placeholders on every execute and every retry: about
    2 us per value, more than half of a table worker's CPU. The values are still adapted on
    every execute, as before.

    Rendered without a connection, where :class:`~psycopg.sql.Identifier` quotes as libpq's
    ``PQescapeIdentifier`` does for a UTF-8 client (Aurora DSQL's): double quotes around,
    each ``"`` doubled.

    Run it on :func:`_insert_cursor`'s cursor, which sends it as it is: the text is never
    scanned for ``%``, so ``pct%off`` and ``a%sb`` load instead of failing the scan, and
    ``x%%y`` stays ``x%%y`` instead of arriving as ``x%y``.
    """
    if num_rows < 1:
        raise ValueError("num_rows must be a positive integer")
    if not columns:
        raise ValueError("columns must not be empty")
    width = len(columns)
    head = sql.SQL("INSERT INTO {table} ({columns}) VALUES ").format(
        table=_pg_table_identifier(table_name),
        columns=sql.SQL(", ").join(sql.Identifier(name) for name in columns),
    ).as_bytes(None)
    marks = ["$%d" % number for number in range(1, num_rows * width + 1)]
    rows = ", ".join(
        "(" + ", ".join(marks[start : start + width]) + ")"
        for start in range(0, len(marks), width)
    ).encode("ascii")
    conflict = _on_conflict_clause(on_conflict, key_columns, columns).as_bytes(None)
    return _RenderedStatement(head + rows + b" " + conflict)


def _insert_cursor(connection: Any) -> Any:
    """The cursor a bulk INSERT runs on: psycopg's :class:`~psycopg.RawCursor` on a psycopg
    connection -- it sends a :class:`_RenderedStatement` as it is and adapts the values
    exactly as a normal cursor does -- and the connection's own ``cursor()`` on anything
    else (a unit test's double)."""
    if isinstance(connection, psycopg.Connection):
        return psycopg.RawCursor(connection)
    return connection.cursor()


def _disable_prepared_statements(connection: Any) -> None:
    """Switch off psycopg's automatic prepared statements on a connection the loader opened.

    psycopg keeps the last 100 (query, parameter types) keys of a connection and prepares one
    seen 5 times. A bulk INSERT's key is its whole text -- hundreds of KB -- and its types
    change with each batch's NULLs and integer sizes, so the keys rarely recur and mostly
    cost memory, 20-41 MiB per connection; ``prepare=False`` on ``execute`` still records
    them, ``prepare_threshold=None`` does not. A table whose types do repeat was prepared on
    each connection from its sixth batch on; it is now sent unprepared every time, with the
    same bytes and values.

    Set here, on the loader's own connections, not in
    :class:`~dsql_migrator.core.target_connection.DsqlConnector`: Validation reads through
    the same connector and relies on prepared statements for its keyset pages.
    """
    if isinstance(connection, psycopg.Connection):
        connection.prepare_threshold = None


# Shapes besides the full-size one that keep their rendered text: a reader's partial last
# batch, a byte-capped batch of wide rows, the halves of a split batch, a select-filter
# remainder. Small on purpose: an entry can be about 1 MB (65,535 parameters), and a miss
# only renders that shape again.
_RENDERED_INSERT_LRU_SIZE = 8


class _RenderedInserts:
    """Each bulk INSERT shape rendered once, in bounded memory.

    A shape is (table, columns, rows, mode, conflict keys). Every batch but a reader's last
    has the table's FULL-size shape (its effective batch size), which keeps a slot of its
    own that the others never evict; the rest share a small LRU. The text is several times
    the size of the ``Composed`` it replaces (which shared one row's placeholders across all
    the rows), hence the bound. The importer's writer threads share it: one lock, and a
    missing shape is rendered under it, so each is rendered once.
    """

    def __init__(self, batch_size: int, lru_size: int = _RENDERED_INSERT_LRU_SIZE) -> None:
        self._batch_size = batch_size
        self._lru_size = max(1, lru_size)
        self._full: Optional[tuple[tuple, _RenderedStatement]] = None
        self._lru: "OrderedDict[tuple, _RenderedStatement]" = OrderedDict()
        self._lock = threading.Lock()
        self.rendered = 0  # shapes rendered so far

    def statement(
        self,
        table_name: str,
        columns: tuple[str, ...],
        num_rows: int,
        on_conflict: OnConflictMode,
        key_columns: tuple[str, ...],
    ) -> _RenderedStatement:
        """The rendered INSERT of ``num_rows`` rows of this shape, rendered on a miss."""
        key = (table_name, columns, num_rows, on_conflict, key_columns)
        full = num_rows == _effective_batch_size(self._batch_size, len(columns))
        with self._lock:
            if full:
                if self._full is not None and self._full[0] == key:
                    return self._full[1]
            else:
                cached = self._lru.get(key)
                if cached is not None:
                    self._lru.move_to_end(key)
                    return cached
            statement = render_insert_statement(
                table_name, list(columns), num_rows, on_conflict, list(key_columns)
            )
            self.rendered += 1
            if full:
                self._full = (key, statement)
            else:
                self._lru[key] = statement
                if len(self._lru) > self._lru_size:
                    self._lru.popitem(last=False)
            return statement


# A pool slot whose connection was discarded after an error: its next lease opens a
# REPLACEMENT, which the load's counters report as a reconnect (a never-used slot is None).
_RECONNECT = object()

# SQLSTATE classes after which a loader connection is discarded even when it still looks
# usable: 08 connection exception, 53 insufficient resources, 57 operator intervention (an
# admin shutdown, a cancelled statement), 58 system error, and XX internal error (Aurora
# DSQL's XX000 "server unavailable" among them). Class 40 is kept for 40001 only.
_SESSION_ENDING_CLASSES = frozenset({"08", "53", "57", "58", "XX"})


def _session_survives(exc: BaseException, connection: Any) -> bool:
    """Whether ``connection`` can serve the next lease after ``exc`` was raised while it
    was in use: the server rejected only the statement, and the session is still idle.

    Loader connections are autocommit, so a rejected statement aborts only its own implicit
    transaction; R1 switched psycopg's prepared statements off on them, so no per-session
    statement cache can go stale; and every retry resends identical bytes. Kept: an OCC
    ``40001`` (Aurora DSQL's OC000 row conflict, OC001 schema change), a data or constraint
    error (``22xxx`` / ``23xxx`` -- the poison-row binary split leases once per failing
    half, so a bad row in a 2000-row batch cost 11-12 reconnects), ``54000``, ``42xxx``.
    Discarded: an error with no SQLSTATE (a dropped socket, a client-side error), one of
    :data:`_SESSION_ENDING_CLASSES`, any other class-40 code (``40003`` statement completion
    unknown), anything that is not an :class:`Exception`, and a connection that does not
    report itself open, unbroken and idle -- so a double without psycopg's ``closed`` /
    ``broken`` / ``info.transaction_status`` is discarded as before. Pure; never raises.
    """
    try:
        if not isinstance(exc, Exception):
            return False
        state = getattr(exc, "sqlstate", None)
        if not isinstance(state, str) or not state:
            return False
        code = state.upper()
        if code[:2] in _SESSION_ENDING_CLASSES or (code[:2] == "40" and code != "40001"):
            return False
        return bool(
            not getattr(connection, "closed", True)
            and not getattr(connection, "broken", False)
            and connection.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        )
    except Exception:  # noqa: BLE001 - a session it cannot read is not one to keep
        return False


class _ConnectionPool:
    """A bounded pool of lazily-created, reusable connections.

    At most ``size`` connections are ever created (Property 2 -- bounded
    parallelism). A leased connection is returned to the pool on context exit and
    reused by a later batch. :meth:`close_all` closes every connection created.
    ``stats`` (optional) counts the connections opened, the reconnects among them, the
    failed connects and the connections kept after an error (:meth:`lease`).
    """

    def __init__(
        self, factory: ConnectionFactory, size: int, stats: Optional[LoadStats] = None
    ) -> None:
        """Seed ``size`` empty slots; connections are created on first lease."""
        self._factory = factory
        self._slots: "Queue[Optional[Any]]" = Queue()
        for _ in range(size):
            self._slots.put(None)
        self._created: list[Any] = []
        self._lock = threading.Lock()
        self._stats = stats

    @contextmanager
    def lease(self) -> Iterator[Any]:
        """Lease a connection, creating it on first use, returning it on exit.

        If the leased connection raises while in use, it is KEPT when the server rejected
        only the statement and the session is still idle (:func:`_session_survives`: an OCC
        ``40001``, a data or constraint error), counted as a connection kept: a retry or the
        next batch reuses it instead of opening a new one (a TLS handshake and an IAM token),
        as every OCC retry and every failing half of a poison-row split used to. Anything
        else (a dropped connection, an expired IAM token, an error that may have ended the
        session, one with no SQLSTATE, a session that is not idle) closes it and refills the
        slot empty (:data:`_RECONNECT`), so the next lease creates a FRESH connection (which
        re-mints a short-lived IAM token via the factory). This prevents one broken
        connection from cascading the same failure across every later batch that would
        otherwise reuse it, and lets a retry recover. The error is re-raised either way.
        """
        connection = self._slots.get()
        if connection is None or connection is _RECONNECT:
            empty_slot = connection
            try:
                connection = self._factory()
            except BaseException:
                # Creating the connection failed (e.g. DSQL's new-connection rate limit under
                # a burst, or an IAM-token mint blip). The slot token was already pulled from
                # the queue above, and this is BEFORE the try below that would refill it, so
                # without putting an empty slot back the pool shrinks by one PERMANENTLY. A
                # connect failure is classified transient and retried, so repeated blips would
                # drain every slot until the next self._slots.get() blocks forever (no
                # timeout) -- deadlocking the whole load. Refill the slot, then re-raise so the
                # OCC retry can lease again and re-create a fresh connection.
                if self._stats is not None:
                    self._stats.record_connect_failure()
                self._slots.put(empty_slot)
                raise
            if self._stats is not None:
                self._stats.record_connect(reconnect=empty_slot is _RECONNECT)
            # Every connection the loader opens, a reconnect included: Full Load passes the
            # shared connector's connect, which keeps psycopg's default for Validation.
            _disable_prepared_statements(connection)
            with self._lock:
                self._created.append(connection)
        try:
            yield connection
        except BaseException as exc:
            if _session_survives(exc, connection):
                self._slots.put(connection)
                if self._stats is not None:
                    self._stats.record_connection_kept()
            else:
                try:
                    connection.close()
                except Exception:  # noqa: BLE001 - best-effort discard
                    pass
                with self._lock:
                    if connection in self._created:
                        self._created.remove(connection)
                self._slots.put(_RECONNECT)
            raise
        else:
            self._slots.put(connection)

    def close_all(self) -> None:
        """Close every connection this pool created (best-effort)."""
        with self._lock:
            created = list(self._created)
            self._created.clear()
        for connection in created:
            _safe_close(connection)


# Cap on quarantine records retained, and -- once exceeded -- the signal to STOP. A
# systematic data error (e.g. a whole column over DSQL's 1 MiB per-value limit) would
# otherwise binary-split every batch down to single rows (O(rows) transactions) and append a
# QuarantineRecord per row without bound. Beyond the cap the load fails loudly so the operator
# fixes the root cause instead of the tool silently quarantining a huge fraction of the table.
MAX_QUARANTINE_RECORDS = 1000
# Back-compat alias for the internal call sites and tests that already name it privately.
_MAX_QUARANTINE_RECORDS = MAX_QUARANTINE_RECORDS


# PostgreSQL ``bpchar`` (CHAR(n)) pads its OUTPUT to the declared length while a MySQL
# CHAR source value arrives TRIMMED (MySQL strips trailing blanks on retrieval). The server
# itself compares bpchar blank-insensitively, so ``WHERE (code) IN (%s)`` with the trimmed
# value DOES match the stored row -- but the Python-side set compare then saw
# ``'AB        '`` against ``'AB'`` and concluded the key was absent. Every already-present
# row was therefore re-inserted: an idempotent resume became a full re-write, and on the
# plain-INSERT path a duplicate-key failure. Normalising by the type the SELECT itself
# reports needs no new plumbing.
_BPCHAR_OID = 1042


def _key_parameter(value: object) -> object:
    """A key value as the SELECT-filter binds it: a whole ``Decimal`` as an ``int``.

    A numeric source key made a bigint identity (a PostgreSQL ``numeric(12,0)`` serial)
    comes off the driver as a Decimal, which psycopg binds as numeric, and
    ``"id" IN ($1, ...)`` then resolves to ``"id"::numeric = ...``: the bigint primary key
    cannot serve it, so every retried batch scanned the table. An int is compared as the
    column's own type (and widened, not cast column-side, for a numeric key)."""
    from decimal import Decimal

    if isinstance(value, Decimal) and value.is_finite() and value == value.to_integral_value():
        return int(value)
    return value


def _normalize_key(values: tuple, description: object) -> tuple:
    """Make a key tuple comparable across the wire's padding rules (see :data:`_BPCHAR_OID`)."""
    columns = tuple(description or ())
    if not columns or len(columns) != len(values):
        return values
    return tuple(
        value.rstrip(" ")
        if isinstance(value, str)
        and getattr(column, "type_code", None) == _BPCHAR_OID
        else value
        for value, column in zip(values, columns)
    )


def _quarantined_row_identity(work: "_BatchWork", row: Mapping[str, object]) -> tuple:
    """Identity of one quarantined row: ``(table, digest of its key values)`` -- of the whole
    row when the table has no key -- held in memory only, never logged.

    Used both to carry a prior attempt's drops across a resumed ``import_rows`` and to
    keep a re-read from recording the same drop twice; the message is deliberately excluded
    (the same row can fail with slightly different driver text and is still the same lost
    row). Not the record's logged primary key: that withholds a natural key's values
    (:func:`format_key_values`), so every quarantined row of a table keyed by a string or a
    date was one row -- the drop count read 1 whatever was dropped, and the quarantine cap
    never fired for such a table. A digest, so a dropped row's values (an oversized one, as
    often as not) are not held.
    """
    names = work.key_columns or work.columns
    values = tuple(_key_parameter(row.get(name)) for name in names)
    digest = hashlib.sha256(repr(values).encode("utf-8", "backslashreplace")).digest()
    return (work.table_name, digest)


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
        max_quarantine_records: Optional[int] = None,
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
        # The permanently-dropped-row safety cap, per importer. A SHARDED table builds one
        # importer PER SHARD in its own process, so the module constant alone let K shards
        # drop K x 1000 rows before the cap fired -- K times the intended bound, and a
        # shared counter is impossible across processes. The orchestrator therefore divides
        # the single budget among the shards; default keeps the whole budget.
        self._max_quarantine = max(1, int(max_quarantine_records or _MAX_QUARANTINE_RECORDS))
        self._sleep = sleep
        self._jitter = jitter
        # Side-channel sink for poison rows isolated during a load (thread-safe so
        # parallel batch workers can append). Reset at the start of each
        # import_rows call so the result reflects only that call.
        self._quarantine: list[QuarantineRecord] = []
        # Total poison rows seen this load (the retained list is capped at
        # _MAX_QUARANTINE_RECORDS; this counts every one so the result reports the true total).
        self._quarantine_total = 0
        self._quarantine_lock = threading.Lock()
        # Each bulk INSERT shape (table, columns, rows, mode, keys) rendered once to the
        # bytes the server receives, in bounded memory (_RenderedInserts). For a typical
        # large-table load ~99.99% of batches have the same shape.
        self._inserts = _RenderedInserts(self._options.batch_size)
        # The write path's counters (core.load_stats): counts and timings only. Its own
        # until a caller binds the collector of the unit it loads (bind_load_stats).
        self._stats = LoadStats()
        # P4d: {table: the stored name of its primary-key index, or None if unknown}, read
        # from the target catalog the first time a unique violation needs it.
        self._primary_key_indexes: dict[str, Optional[str]] = {}

    def bind_load_stats(self, stats: LoadStats) -> None:
        """Count into ``stats`` from now on: the collector of the load unit (a table, or one
        reader shard of it) this importer writes for, kept across the unit's attempts so a
        re-read adds to it. Counts and timings only -- see :mod:`~dsql_migrator.core.load_stats`.
        """
        self._stats = stats

    def cap_parallelism(self, n: int) -> None:
        """Keep at most ``n`` batches in flight from now on (never raises the configured
        ``parallelism``; at least 1).

        The pool size, :meth:`_run_data_batches`'s in-flight bound and the prefetch depth
        follow, since they read ``options.parallelism`` at each :meth:`import_rows` call.
        The Full Load caps each load unit's importer (P3): at most 4 per key range it reads
        and a share of the run's budget, as the engine's ``_unit_in_flight`` sizes it.
        """
        self._options = self._options.model_copy(
            update={"parallelism": max(1, min(self._options.parallelism, int(n)))}
        )

    @property
    def load_stats(self) -> LoadStats:
        """The collector this importer counts into: its own, or the bound one."""
        stats = getattr(self, "_stats", None)
        if stats is None:  # an instance built without __init__ (a test's bare importer)
            stats = self._stats = LoadStats()
        return stats

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
        range_pages: Optional[
            "Iterable[tuple[int, Iterable[Mapping[str, object]]]]"
        ] = None,
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
        ``range_pages`` (optional, P2; ``rows`` is then unused and ``shard_sources`` must be
        None) is ONE reader's pages of a table's key ranges in turn, ``(range index, the
        page's rows)`` (:meth:`~dsql_migrator.core.exporter.TableExporter.
        stream_converted_ranges`): each page is batched on its own and each range numbers its
        own batches (:meth:`_iter_work_pages`), and one producer thread reads them -- they
        come from one source connection.
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

        # Resume high-water from any prior attempt on this (ephemeral) job: shard key
        # -> highest contiguously-committed batch index. Immutable during the run --
        # it drives batch skipping AND seeds this run's tracker so completions extend
        # the prefix. Compact (one int per shard), unlike the old per-batch DONE set.
        skip_watermark = dict(job.resume_batch_watermark) if job is not None else {}
        skip_counter = _SkipCounter()
        # Reader range sharding: when ``shard_sources`` holds K disjoint-PK-range
        # row streams, build K work iterators (each ``chunk_id``-namespaced by shard
        # and counting skips under a lock). Otherwise the single ``rows`` stream builds
        # one unsharded work iterator -- byte-for-byte the previous behavior
        # (``shard_id=None`` keeps the original chunk ids).
        work_iters: list[Iterator[_BatchWork]]
        if range_pages is not None:
            # P2: one reader session's pages of K key ranges -> ONE work iterator, so the
            # single-producer prefetch reads them (never _prefetch_many: one connection).
            if shard_sources:
                raise BatchedImportError(
                    "range_pages and shard_sources cannot be combined: the range pages are "
                    "one reader's"
                )
            work_iters = [
                self._iter_work_pages(
                    range_pages, table, columns, key_columns, skip_watermark, skip_counter,
                    effective_on_conflict,
                )
            ]
        elif shard_sources:
            skipped_lock = threading.Lock()
            work_iters = [
                self._iter_work(
                    shard_rows, table, columns, key_columns, skip_watermark,
                    skip_counter, effective_on_conflict, shard_id=i,
                    skipped_lock=skipped_lock,
                )
                for i, shard_rows in enumerate(shard_sources)
            ]
        else:
            work_iters = [
                self._iter_work(
                    rows, table, columns, key_columns, skip_watermark, skip_counter,
                    effective_on_conflict,
                )
            ]

        pool = _ConnectionPool(
            self._connection_factory, self._options.parallelism, stats=self.load_stats
        )
        # A RESUMED call keeps what the previous attempt quarantined. Clearing the sink
        # unconditionally lost it: a source-drop retry that SKIPS the already-committed
        # batches never re-reads their rows, so rows permanently dropped during attempt 1
        # were absent from the surviving result -- the table then logged SUCCESS with
        # ``rows_quarantined=0``, no PK appeared in the error log, ``_finalize_run`` saw
        # nothing incomplete, and the Validation gate that should have held never fired.
        # Only reachable where the watermark survives a re-read (a shard reopening one
        # SHARED PostgreSQL snapshot; every other path now discards it), but there the loss
        # is total and silent. Deduped by the row's identity, so a re-read that re-quarantines
        # the same row cannot double-count it.
        resuming = bool(job is not None and getattr(job, "resume_batch_watermark", None))
        with self._quarantine_lock:
            carried = list(self._quarantine) if resuming else []
            self._quarantine = carried
            self._quarantine_total = len(carried) if resuming else 0
            # The dropped rows' identities carry with their records (_quarantined_row_identity).
            seen = getattr(self, "_quarantine_seen", None)
            self._quarantine_seen = set(seen or ()) if resuming else set()
        # OWN the resume high-water HERE (seeded from the prior attempt's watermark) rather
        # than inside _run_data_batches, so the completed contiguous prefix survives even
        # when the SOURCE READ raises mid-load: _run_data_batches records into THIS tracker,
        # and the finally below persists it onto the (ephemeral) job BEFORE the exception
        # propagates. When the tracker was owned inside _run_data_batches (returned only on a
        # clean return), a raising source read discarded the prefix and a same-process retry
        # re-WROTE every already-committed batch from batch 0 (needless OCC round-trips). The
        # source RE-READ is intentional and fails SAFE; only the redundant re-write is spared.
        resume_tracker = _BatchResumeTracker(skip_watermark)
        indexes_created = 0
        index_failures: list[str] = []
        try:
            # Outcomes are folded into `agg` as they resolve and the resume point is
            # tracked as a compact per-shard high-water -- no per-batch list is ever
            # retained, so a billion-row table's memory stays bounded whether or not a
            # (resume) job is present. The tracker is seeded from the prior watermark.
            # The data pass is timed apart from the index pass below, also when it raises
            # (a source drop): its time and window still count.
            load_wall_start, load_start = time.time(), time.perf_counter()
            try:
                agg, stopped_early = self._run_data_batches(
                    work_iters, pool, on_batch_loaded, should_cancel,
                    resume_tracker=resume_tracker,
                )
            finally:
                self.load_stats.add_phase("load", time.perf_counter() - load_start)
                self.load_stats.mark_load(load_wall_start, time.time())
            failures = agg.failures
            # Skip post-load indexing when the table is incomplete (a stop) or
            # any batch failed -- indexing only makes sense for a full table.
            if failures == 0 and not stopped_early and index_ddls:
                indexes_created, index_failures = self._create_indexes(
                    pool, index_ddls
                )
        finally:
            pool.close_all()
            # Persist the advanced resume high-water back onto the (ephemeral) job so a
            # same-process source-drop retry skips the keyset ranges already committed. In
            # the finally so it survives a raising source read (see the tracker comment).
            if job is not None:
                job.resume_batch_watermark = resume_tracker.watermark()

        result = _aggregate_result(
            agg, skip_counter.value, indexes_created, cancelled=stopped_early
        )
        # Indexes that could not be created. Reported alongside a SUCCESSFUL data
        # load: the rows are all present, so this is a missing access path to fix
        # later, not a reason to fail (and re-run) the table.
        result.index_failures = list(index_failures)
        with self._quarantine_lock:
            quarantined = list(self._quarantine)
            quarantined_total = self._quarantine_total
        # Report the TRUE total (the retained records are capped at _MAX_QUARANTINE_RECORDS).
        result.quarantined = quarantined_total
        result.quarantine_records = quarantined
        return result

    def _iter_work(
        self,
        rows: Iterable[Mapping[str, object]],
        table: TableDef,
        columns: list[str],
        key_columns: list[str],
        skip_watermark: Mapping[str, int],
        skip_counter: "_SkipCounter",
        on_conflict: OnConflictMode,
        shard_id: Optional[int] = None,
        skipped_lock: Optional[threading.Lock] = None,
    ) -> Iterator[_BatchWork]:
        """Yield a work item per non-skipped batch (consumes rows lazily).

        On resume, a batch at or below its shard's high-water in ``skip_watermark``
        was already committed by a prior attempt; its rows are dropped (counted in
        ``skip_counter``, not loaded). ``ON CONFLICT`` makes re-loading safe -- skipping
        just avoids needless work on resume (Property 4). Batches stream in keyset
        order, so ``index <= high-water`` is exactly "this PK range is already done".

        ``shard_id`` namespaces the ``chunk_id`` for one reader shard (batch ``index``
        restarts at 0 per shard). ``skipped_lock`` guards the shared skip counter when
        K shard producers run concurrently; ``None`` (single reader) needs no lock.
        """
        columns_tuple = tuple(columns)
        key_tuple = tuple(key_columns)
        high_water = skip_watermark.get(_shard_key(shard_id), -1)
        batch_size = _effective_batch_size(self._options.batch_size, len(columns_tuple))
        for index, batch in enumerate(
            _iter_batches(rows, batch_size, MAX_BATCH_BYTES)
        ):
            if index <= high_water:
                skip_counter.add(locked=skipped_lock is not None)
                continue
            yield _BatchWork(
                chunk_id=batch_chunk_id(table.name, index, shard_id),
                table_name=table.name,
                columns=columns_tuple,
                key_columns=key_tuple,
                on_conflict=on_conflict,
                rows=tuple(batch),
                shard_id=shard_id,
                batch_index=index,
            )

    def _iter_work_pages(
        self,
        range_pages: "Iterable[tuple[int, Iterable[Mapping[str, object]]]]",
        table: TableDef,
        columns: list[str],
        key_columns: list[str],
        skip_watermark: Mapping[str, int],
        skip_counter: "_SkipCounter",
        on_conflict: OnConflictMode,
    ) -> Iterator[_BatchWork]:
        """Yield a work item per non-skipped batch of ONE reader's key-range pages (P2).

        ``range_pages`` yields ``(range index, the page's rows)`` in the order the reader
        visits the ranges, a page of each in turn. Each page is batched on its own -- no batch
        spans two pages, so none spans two ranges -- and each range numbers its batches from
        0, as a reader shard does (:meth:`_iter_work`): batch *i* of range *r*
        (``batch_chunk_id(table, i, r)``, resume key ``str(r)``) is the same primary-key
        slice of one snapshot however the ranges' pages interleave, and a resume skips per
        range. Pulled lazily, one batch at a time; the rows of a page are read to its end
        before the next page is asked for. One producer, so the skip count takes no lock.
        """
        columns_tuple = tuple(columns)
        key_tuple = tuple(key_columns)
        batch_size = _effective_batch_size(self._options.batch_size, len(columns_tuple))
        # range index -> the ordinal of its next batch
        next_index: dict[int, int] = {}
        for shard_id, page in range_pages:
            high_water = skip_watermark.get(_shard_key(shard_id), -1)
            index = next_index.get(shard_id, 0)
            for batch in _iter_batches(page, batch_size, MAX_BATCH_BYTES):
                if index <= high_water:
                    skip_counter.add(locked=False)
                else:
                    yield _BatchWork(
                        chunk_id=batch_chunk_id(table.name, index, shard_id),
                        table_name=table.name,
                        columns=columns_tuple,
                        key_columns=key_tuple,
                        on_conflict=on_conflict,
                        rows=tuple(batch),
                        shard_id=shard_id,
                        batch_index=index,
                    )
                index += 1
            next_index[shard_id] = index

    @staticmethod
    def _prefetch(
        work_iter: Iterator[_BatchWork], depth: int, stats: Optional[LoadStats] = None
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

        ``stats`` (optional) is told how long the reader waited on the full queue (the
        writes are the bottleneck) and how long the consumer waited on an empty one (the
        read is).
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
                    _put_counted(q, item, stats)  # blocks when the buffer is full
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
                item = _get_counted(q, stats)
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
        work_iters: "list[Iterator[_BatchWork]]",
        depth: int,
        stats: Optional[LoadStats] = None,
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
        ``stats`` is told the queue waits, as in :meth:`_prefetch` (summed over the
        readers).
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
                    _put_counted(q, item, stats)
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
                item = _get_counted(q, stats)
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
        *,
        resume_tracker: "_BatchResumeTracker",
    ) -> "tuple[_RunAggregate, bool]":
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
        # Fold each outcome into a running aggregate as it resolves and advance the
        # caller-owned per-shard resume high-water (``resume_tracker``) -- NO per-batch
        # list is ever retained, so a billion-row load stays bounded regardless of whether
        # a (resume) job is present. The caller owns the tracker (seeded from the prior
        # attempt's watermark) so the completed prefix survives even if the source read
        # raises here; this method only RECORDS into it.
        agg = _RunAggregate()
        parallelism = self._options.parallelism
        # future -> (chunk_id, shard_id, batch_index): the shard/index feed the resume
        # high-water; only ~parallelism entries are ever live (bounded).
        in_flight: dict[Future[tuple[int, int]], tuple[str, Optional[int], int]] = {}
        stopped_early = False

        def drain_one() -> None:
            # BOUNDED wait. With no timeout, one batch whose DSQL response never arrives
            # (a blackholed socket -- the DSQL connection sets connect_timeout only, no
            # socket read timeout) blocked the cooperative-stop drain forever, and the
            # enclosing `with ThreadPoolExecutor(...)` then blocked on its own
            # shutdown(wait=True). Timing out lets the caller re-poll should_cancel and
            # come back, so Stop stays responsive while a healthy batch is still awaited.
            while True:
                done, _ = wait(
                    set(in_flight), timeout=_DRAIN_POLL_SECONDS,
                    return_when=FIRST_COMPLETED,
                )
                if done:
                    break
                # Nothing finished yet. Keep waiting -- returning here would let the
                # caller submit past `parallelism` and break the bounded-pool guarantee.
                # The timeout exists so this is a series of short waits rather than one
                # uninterruptible block; a batch that can never answer is bounded by the
                # DSQL connection's socket timeouts (see target_connection.py), not here.
            for future in done:
                chunk_id, shard_id, batch_index = in_flight.pop(future)
                outcome = _resolve_outcome(future, chunk_id)
                agg.fold(outcome)
                if outcome.status == "DONE":
                    resume_tracker.record_done(shard_id, batch_index)
                else:
                    resume_tracker.record_failed(shard_id)
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
                work_iters, _prefetch_depth(parallelism), stats=self.load_stats
            )
        elif _prefetch_enabled():
            prefetched = self._prefetch(
                work_iters[0], _prefetch_depth(parallelism), stats=self.load_stats
            )
        else:
            prefetched = work_iters[0]
        with ThreadPoolExecutor(max_workers=parallelism) as executor:
            try:
                try:
                    for work in prefetched:
                        if should_cancel is not None and should_cancel():
                            # Stop pulling new batches; remaining rows stay unloaded so
                            # the table is reported incomplete (retryable, idempotent).
                            stopped_early = True
                            break
                        future = executor.submit(self._load_batch_timed, work, pool)
                        in_flight[future] = (work.chunk_id, work.shard_id, work.batch_index)
                        if len(in_flight) >= parallelism:
                            drain_one()
                finally:
                    # Stop + join the reader thread promptly on cancel/early-exit
                    # (also runs on the normal exhausted path -- a no-op there).
                    prefetched.close()
                # Normal / cancel exit: drain the in-flight tail (surfaces a callback
                # error exactly as before).
                while in_flight:
                    drain_one()
            finally:
                # If the SOURCE READ (or the tail drain) raised above, the tail loop did
                # not run, yet the batches already submitted still COMMITTED. Drain those
                # completed futures into the caller's resume tracker anyway so the persisted
                # prefix is MAXIMAL and a retry does not re-write committed keyset ranges.
                # Best-effort only: never let a drained callback error mask the exception
                # currently unwinding (which the retry path classifies to decide re-read).
                while in_flight:
                    try:
                        drain_one()
                    except Exception:  # noqa: BLE001 - unwinding; keep the original error
                        break
        return agg, stopped_early

    def _load_batch_timed(
        self, work: _BatchWork, pool: _ConnectionPool
    ) -> tuple[int, int]:
        """:meth:`_load_batch`, counting the batch and its time from first attempt to
        outcome (retries and their backoff included) for the load's counters."""
        started = time.perf_counter()
        failed = True
        try:
            outcome = self._load_batch(work, pool)
            failed = False
            return outcome
        finally:
            self.load_stats.record_batch(time.perf_counter() - started, failed=failed)

    def _load_batch(
        self, work: _BatchWork, pool: _ConnectionPool
    ) -> tuple[int, int]:
        """Load one batch, isolating poison rows on a permanent error.

        Tries the batch via :meth:`_execute_for_mode` (which retries OCC ``40001``,
        Aurora DSQL's temporary ``XX000`` and transient connection drops; one that
        outlasts the budget is raised, so the BATCH fails -- retryable, never split). If
        it fails with a NON-retryable error (a permanent data/constraint error that a
        re-run cannot fix), the batch is binary-split and each half retried recursively,
        down to a single row: that row is the poison row -- it is quarantined (its PK +
        reason recorded, never its non-key values) and skipped, while every other row in
        the batch still loads. This is the Full Load analogue of the CDC sink's DLQ
        quarantine: one bad row never blocks ~thousands of good rows, and the loss is
        surfaced (in the error log) rather than silent.

        Returns ``(rows_inserted, conflicts)`` for the loaded rows; quarantined
        rows are accumulated on the importer's quarantine sink (read via
        :meth:`import_rows`'s result).
        """
        if self._quarantine_total >= self._max_quarantine:
            # A systematic data error has already quarantined the cap's worth of rows; stop
            # binary-splitting and quarantining unboundedly and fail loudly so the operator
            # fixes the root cause (see _MAX_QUARANTINE_RECORDS) instead of the tool silently
            # dropping a huge fraction of the table.
            raise BatchedImportError(
                f"table '{work.table_name}': quarantine cap ({self._max_quarantine}) "
                "reached -- too many rows failed with a permanent data error; aborting so the "
                "systematic cause is fixed rather than silently dropping more rows."
            )
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
        """Record one poison row (PK + reason only) to the quarantine sink.

        Every poison row bumps ``_quarantine_total`` so the result reports the true count, but
        the retained list is capped at ``_MAX_QUARANTINE_RECORDS`` (a systematic error must not
        grow it without bound -- see :meth:`_load_batch`, which stops the load once the cap is
        reached).
        """
        row = work.rows[0]
        if work.key_columns:
            # Column names always, values only for a surrogate key: a natural-key PK can
            # itself be the sensitive datum, and this string is written to the durable
            # error log, the activity log and its CloudWatch mirror (Property 7).
            primary_key = format_key_values(work.key_columns, row)
        else:
            primary_key = "(no primary key)"
        # _safe_error, not str(exc): the raw driver text carries the failing row's column
        # values in its DETAIL line (Property 7). The sqlstate is kept separately in
        # error_code for triage.
        message = _safe_error(exc)
        # P4d: a row a secondary UNIQUE index rejected (23505) names that index -- an
        # identifier, never a value -- when the primary message does not already.
        constraint = (
            violated_constraint_name(exc) if getattr(exc, "sqlstate", None) == "23505" else None
        )
        if constraint and f'"{constraint}"' not in message:
            message = f"{message} [unique index: {constraint}]"
        record = QuarantineRecord(
            table=work.table_name,
            primary_key=primary_key,
            error_code=getattr(exc, "sqlstate", None),
            message=message,
        )
        with self._quarantine_lock:
            # Dedup by the row's identity: a retry re-reads rows a previous attempt
            # already quarantined, and the cause is deterministic (a value over DSQL's
            # per-value cap fails identically every time), so counting it again would
            # inflate the drop total the operator has to reconcile.
            key = _quarantined_row_identity(work, row)
            seen = getattr(self, "_quarantine_seen", None)
            if seen is None:
                seen = self._quarantine_seen = set()
            if key in seen:
                return
            seen.add(key)
            self._quarantine_total += 1
            if len(self._quarantine) < self._max_quarantine:
                self._quarantine.append(record)

    def _execute_for_mode(
        self, work: _BatchWork, pool: _ConnectionPool
    ) -> tuple[int, int]:
        """Load one batch as a single multi-row ``INSERT`` with OCC retry.

        Returns ``(rows_inserted, conflicts)``. The execution is wrapped in
        :func:`~dsql_migrator.core.occ.with_occ_retry`; because the statement is a
        single idempotent ``INSERT ... ON CONFLICT``, retrying a ``40001`` is safe
        and never duplicates rows (Properties 3/5).

        Aurora DSQL's ``XX000`` is retried the same way, and its attempt MAY have
        committed (every connection is autocommit, and a multi-row ``INSERT`` is
        atomic), so each mode re-runs a batch that already landed exactly: ``DO_NOTHING``
        inserts nothing and counts every row as a conflict; ``DO_UPDATE`` writes the same
        values again; ``NONE`` gets ``23505`` and recovers through
        :meth:`_select_filter_insert`, which finds every key present; ``SKIP_EXISTING``'s
        optimistic ``INSERT`` gets ``23505`` and takes the same select-filter. The rows
        are exact; only the counts move from loaded to skipped.
        """
        attempted = len(work.rows)
        # Count OCC attempts so the trace can report retries. The wrapper runs once
        # per attempt (each ``40001`` retry re-invokes it), so attempts-1 == retries.
        attempts = 0

        def _count_failure(exc: BaseException) -> None:
            # A retryable failure aborted a transaction on the target; the wrapper retries it
            # unless this was the budget's last attempt. A permanent error is no retry: it
            # splits the batch (a poison row) or fails it.
            if _is_retryable_load_error(exc):
                self.load_stats.record_retry(exc, exhausted=attempts >= self._occ_max_attempts)

        if work.on_conflict is OnConflictMode.SKIP_EXISTING:
            # DSQL-safe idempotent path: SELECT existing PKs, plain-INSERT the rest. The
            # optimistic plain INSERT is fetched once, so every attempt sends the same bytes.
            optimistic_insert = self._inserts.statement(
                work.table_name, work.columns, attempted, OnConflictMode.NONE,
                work.key_columns,
            )

            def _counted_skip(pool_):
                nonlocal attempts
                attempts += 1
                try:
                    return self._execute_skip_existing(pool_, work, optimistic_insert)
                except Exception as exc:  # noqa: BLE001 - counted, then re-raised
                    _count_failure(exc)
                    raise

            retried = with_occ_retry(
                max_attempts=self._occ_max_attempts,
                base_delay=self._occ_base_delay,
                sleep=self._sleep,
                jitter=self._jitter,
                retryable=_is_retryable_load_error,
            )(_counted_skip)
            try:
                inserted, conflicts = retried(pool)
            finally:
                self.load_stats.record_statement(attempts)
            if _LOGGER.isEnabledFor(logging.DEBUG):
                first_pk, last_pk = _batch_pk_range(work)
                _LOGGER.debug(
                    "import batch chunk=%s table=%s pk_range=[%s..%s] attempted=%d "
                    "inserted=%d skipped=%d on_conflict=%s occ_retries=%d",
                    work.chunk_id, work.table_name, first_pk, last_pk, attempted,
                    inserted, conflicts, work.on_conflict.value, max(0, attempts - 1),
                )
            return inserted, conflicts

        # Fetched once for the batch: every attempt sends these same bytes.
        statement = self._inserts.statement(
            work.table_name, work.columns, attempted, work.on_conflict, work.key_columns
        )
        params = _flatten_params(work.rows, work.columns)

        def _counted_execute(pool_, statement_, params_, attempted_):
            nonlocal attempts
            attempts += 1
            try:
                return self._execute_insert(
                    pool_, statement_, params_, attempted_,
                    on_conflict=work.on_conflict,
                )
            except Exception as exc:  # noqa: BLE001 - inspect sqlstate
                # A NONE-mode plain INSERT (clean load into an empty target) can only hit a
                # unique violation (23505) on a RETRY after the batch already committed but the
                # commit ack was lost to a transient connection drop or to Aurora DSQL's XX000
                # at commit. Recover idempotently via SELECT-existing + insert-missing instead
                # of letting the 23505 reach _load_batch, which would binary-split and FALSELY
                # quarantine rows that are actually present (phantom data-loss report).
                # DO_NOTHING / DO_UPDATE carry an ON CONFLICT clause, so a 23505 is not
                # expected there and is left to surface.
                if (
                    work.on_conflict is OnConflictMode.NONE
                    and work.key_columns
                    and getattr(exc, "sqlstate", None) == "23505"
                ):
                    self.load_stats.record_unique_recovery()
                    try:
                        return self._select_filter_insert(pool_, work)
                    except Exception as recovery_exc:  # noqa: BLE001 - counted, re-raised
                        _count_failure(recovery_exc)
                        raise
                _count_failure(exc)
                raise

        retried = with_occ_retry(
            max_attempts=self._occ_max_attempts,
            base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
            retryable=_is_retryable_load_error,
        )(_counted_execute)
        try:
            inserted, conflicts = retried(pool, statement, params, attempted)
        finally:
            self.load_stats.record_statement(attempts)

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

    def _timed_execute(self, cursor: Any, statement: Any, params: Any) -> None:
        """``cursor.execute`` of a data INSERT, its wall time counted as one write round
        trip (the driver's adaptation of the values included), failed or not."""
        started = time.perf_counter()
        try:
            cursor.execute(statement, params)
        finally:
            self.load_stats.record_execute(time.perf_counter() - started)

    def _execute_insert(
        self,
        pool: _ConnectionPool,
        statement: sql.Composable,
        params: list[object],
        attempted: int,
        on_conflict: "OnConflictMode" = None,  # type: ignore[assignment]
    ) -> tuple[int, int]:
        """Run one batch ``INSERT`` on a pooled connection (single transaction).

        Each attempt leases its own connection from the bounded pool, so a retry
        never holds a connection while waiting. ``rows_inserted`` is taken from
        the cursor ``rowcount`` (rows actually inserted under ``ON CONFLICT``);
        ``conflicts`` is the remainder that hit an existing row. ``statement`` is a
        :func:`render_insert_statement` statement, run on :func:`_insert_cursor`'s cursor.
        """
        with pool.lease() as connection:
            cursor = _insert_cursor(connection)
            try:
                self._timed_execute(cursor, statement, params)
                rowcount = getattr(cursor, "rowcount", -1)
            finally:
                _safe_close(cursor)
        inserted = rowcount if isinstance(rowcount, int) and rowcount >= 0 else attempted
        inserted = min(inserted, attempted)
        # Under NONE there is no ON CONFLICT clause, so there is no such thing as a
        # "conflict": a shortfall means the server accepted the statement and stored FEWER
        # rows than were sent -- rows silently lost, which is precisely what the NONE mode
        # exists to make impossible. Folding it into ``conflicts`` reported those rows as
        # "already on target (skipped)", so the run looked complete. Fail the batch loudly
        # instead; the 23505 -> _select_filter_insert recovery above stays the ONLY
        # legitimate source of skipped rows in this mode.
        if on_conflict is OnConflictMode.NONE and inserted < attempted:
            raise BatchedImportError(
                f"plain INSERT stored {inserted} of {attempted} row(s) with no ON CONFLICT "
                "clause -- the missing rows were neither inserted nor rejected, so they "
                "would be silently absent from the target"
            )
        return inserted, attempted - inserted

    def _execute_skip_existing(
        self,
        pool: _ConnectionPool,
        work: _BatchWork,
        optimistic_insert: sql.Composable,
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

        ``optimistic_insert`` is the batch's rendered plain ``INSERT``, fetched once by the
        caller so every attempt sends the same bytes.

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

        # Optimistic fast path: one plain INSERT of the whole batch, NO pre-SELECT.
        # On a no-overlap load -- the dominant large-scale initial CDC-coexisting case
        # -- this is a single round-trip per batch (as fast as a clean load). Only
        # when a key already exists (a concurrent CDC insert or a re-run) does DSQL
        # raise a unique violation (23505); we then fall through to the SELECT-
        # filter path, paying the extra read ONLY on real overlap, not every batch.
        # The optimistic full-batch INSERT is rendered once per shape (every full-size batch
        # has the identical shape), exactly like the non-SKIP path. SKIP_EXISTING is the
        # primary append / resume / CDC-coexist load path.
        with pool.lease() as connection:
            cursor = _insert_cursor(connection)
            try:
                self._timed_execute(
                    cursor, optimistic_insert, _flatten_params(work.rows, work.columns)
                )
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

        self.load_stats.record_unique_recovery()
        return self._select_filter_insert(pool, work)

    def _select_filter_insert(
        self, pool: _ConnectionPool, work: _BatchWork
    ) -> tuple[int, int]:
        """SELECT the batch's already-present keys, then plain-INSERT only the missing rows.

        The idempotent recovery shared by SKIP_EXISTING (after its optimistic INSERT hits a
        23505 on real key overlap) and the NONE clean-load path (when a retry after a lost
        commit-ack re-runs an already-committed batch). Retries on a 23505 in the gap between
        the SELECT and the INSERT (a concurrent insert won a missing key), re-deriving the
        existing set until it converges; exhausting the bounded retries surfaces an error --
        never a silent loss. Returns ``(inserted, skipped)``.

        P4d: a 23505 on those rows that names a UNIQUE index other than the primary key
        (:meth:`_violates_secondary_unique`) is a data error, not a race: it is raised at
        once, and :meth:`_load_batch` isolates the row (binary split, quarantine). Every
        23505 still comes here first, so a re-run of a batch that landed (its keys all
        present) is skipped whichever index the server named.
        """
        unique_violation_sqlstate = "23505"
        key_columns = list(work.key_columns)
        total = len(work.rows)

        def _key_of(row: Mapping[str, object]) -> tuple:
            return tuple(row[name] for name in key_columns)

        # The SELECT runs WITH parameters on a normal cursor, whose %s scan reads the quoted
        # names too: hence _ParamSafeIdentifier (a key or table named ``pct%off`` failed it).
        key_idents = sql.SQL(", ").join(_ParamSafeIdentifier(name) for name in key_columns)
        row_placeholder = sql.SQL("({})").format(
            sql.SQL(", ").join(sql.Placeholder() for _ in key_columns)
        )
        select_sql = sql.SQL(
            "SELECT {keys} FROM {table} WHERE ({keys}) IN ({tuples})"
        ).format(
            keys=key_idents,
            table=_pg_table_identifier(work.table_name, _ParamSafeIdentifier),
            tuples=sql.SQL(", ").join(row_placeholder for _ in work.rows),
        )
        select_params = [_key_parameter(row[name]) for row in work.rows for name in key_columns]
        max_attempts = max(1, self._occ_max_attempts)
        last_error: Optional[BaseException] = None
        for _attempt in range(max_attempts):
            with pool.lease() as connection:
                cursor = connection.cursor()
                insert_cursor = None
                try:
                    cursor.execute(select_sql, select_params)
                    # getattr: a test double's cursor need not expose ``description``,
                    # and None simply means "no padding to normalise".
                    key_types = getattr(cursor, "description", None)
                    existing = {
                        _normalize_key(tuple(row), key_types)
                        for row in cursor.fetchall()
                    }
                    missing = [
                        row
                        for row in work.rows
                        if _normalize_key(_key_of(row), key_types) not in existing
                    ]
                    if not missing:
                        return 0, total
                    insert_sql = self._inserts.statement(
                        work.table_name, work.columns, len(missing), OnConflictMode.NONE,
                        work.key_columns,
                    )
                    insert_cursor = _insert_cursor(connection)
                    try:
                        self._timed_execute(
                            insert_cursor, insert_sql, _flatten_params(missing, work.columns)
                        )
                    except Exception as exc:  # noqa: BLE001 - inspect sqlstate
                        if getattr(exc, "sqlstate", None) == unique_violation_sqlstate:
                            # P4d: none of these rows' keys was there, so a UNIQUE index
                            # other than the primary key rejected one of them: a DATA error
                            # no re-read resolves. Raised, it reaches _load_batch, which
                            # splits the batch down to that row and quarantines it.
                            if self._violates_secondary_unique(connection, work, exc):
                                raise
                            # A concurrent insert won the race for a missing key;
                            # re-derive existing and retry the remainder.
                            self.load_stats.record_unique_recovery()
                            last_error = exc
                            continue
                        raise
                    rowcount = getattr(insert_cursor, "rowcount", -1)
                    inserted = (
                        rowcount
                        if isinstance(rowcount, int) and rowcount >= 0
                        else len(missing)
                    )
                    inserted = min(inserted, len(missing))
                    return inserted, total - inserted
                finally:
                    _safe_close(cursor)
                    if insert_cursor is not None:
                        _safe_close(insert_cursor)
        raise BatchedImportError(
            f"load for table '{work.table_name}' exhausted {max_attempts} attempts "
            "due to concurrent unique violations"
        ) from last_error

    def _violates_secondary_unique(
        self, connection: Any, work: _BatchWork, exc: BaseException
    ) -> bool:
        """Whether a unique violation of rows whose keys :meth:`_select_filter_insert` found
        absent names a UNIQUE index other than the table's primary key (P4d).

        Such a row collides with another row on a secondary unique index -- present since
        Step 2 created the table with its indexes, and since P4d before a Drop & reload's
        rows too -- which re-reading the keys never resolves: it is a data error. Decided by
        name, both known: the index the violation names (:func:`violated_constraint_name`)
        against the table's primary-key index, read once per table from the target catalog
        on ``connection`` (its session is idle: the rejected INSERT ended its own autocommit
        transaction). Either one unknown, or the key itself (a concurrent insert won it):
        ``False``, and the select-filter retries as before.
        """
        name = violated_constraint_name(exc)
        if not name:
            return False
        primary = self._primary_key_index(connection, work.table_name)
        return primary is not None and name != primary

    def _primary_key_index(self, connection: Any, table_name: str) -> Optional[str]:
        """The stored name of ``table_name``'s primary-key index, read from the target catalog
        the first time and kept (``None``: unknown -- also kept, so a catalog that cannot be
        read costs one attempt per table, not one per unique violation)."""
        cache = getattr(self, "_primary_key_indexes", None)
        if cache is None:  # an instance built without __init__ (a test's bare importer)
            cache = self._primary_key_indexes = {}
        if table_name not in cache:
            try:
                cache[table_name] = read_primary_key_index_name(connection, table_name)
            except Exception:  # noqa: BLE001 - unknown: the caller retries as before
                cache[table_name] = None
        return cache[table_name]

    def _create_indexes(
        self,
        pool: _ConnectionPool,
        index_ddls: list[str],
        *,
        on_job: "Optional[Callable[[str, Optional[str]], None]]" = None,
        unreachable_raises: bool = False,
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

        Aurora DSQL's ``XX000`` is retried within the same budget, as is a dropped
        connection. Either attempt may have created the index (G25: a connection that
        drops while the statement runs loses its reply, not necessarily its commit), and an
        index DDL is not idempotent, so a failure after one -- a later "already exists"
        (``42P07``), or an ``XX000`` / drop that never cleared -- carries
        :data:`UNCONFIRMED_INDEX_NOTE` (or :data:`UNCONFIRMED_INDEX_DROP_NOTE`) and is never
        counted as created (the name could be another table's index). Never ``IF NOT
        EXISTS``: that would silently skip a genuine name collision.

        ``unreachable_raises`` (the run-level index pass, :meth:`index_pass`): a DDL whose
        last attempt still failed with a retryable error -- Aurora DSQL did not answer (a
        dropped connection, ``XX000``) or stayed in conflict -- or never sent the statement
        (the connect failed: e.g. the AWS credentials that mint its IAM token expired during
        a long run) raises :class:`IndexTargetUnreachable` with what was submitted and
        recorded before it, instead of being recorded as a missing index: the caller leaves
        the table for a retry, which reads which of its indexes exist first.

        ``on_job(ddl, job_id)`` (P4b) is called once per DDL that was submitted, with the job
        id Aurora DSQL answered for its build (``CREATE INDEX ASYNC`` returns one row holding
        it, as ``ALTER TABLE ASYNC`` does) or ``None`` when it answered none. Never for a DDL
        that failed. A callback that raises is logged and never fails the DDL: the index was
        submitted either way.
        """
        # Lease a FRESH connection INSIDE each retried unit -- mirroring the data
        # path (``_execute_insert``, which leases per attempt) -- and pass the wider
        # ``retryable`` predicate so a transient/aged-out connection drop is retried
        # against a NEW connection. Two things went wrong when this loop instead held
        # ONE lease for every DDL and used the default (OCC-``40001``-only) retry
        # predicate: (1) a connection-level drop carries no ``40001`` SQLSTATE, so it
        # was never retried -- it propagated on the first attempt; and (2) the
        # per-DDL ``except`` below swallowed it and the loop kept reusing the SAME
        # (now dead) connection object, so once a pooled connection crossed DSQL's
        # ~60-min max connection duration mid-load, EVERY remaining index for the
        # table was reported failed even though the data was complete and a reconnect
        # would have built them. Leasing per DDL (a drop closes the connection and
        # refills the slot, so the next lease / retry mints a fresh one) plus
        # ``retryable=_is_retryable_load_error`` fixes both: a transient drop is
        # retried on a fresh connection, and no dead connection is reused across DDLs.
        # ``unconfirmed`` holds the note for an attempt whose CREATE ended with no confirmed
        # outcome -- it answered XX000, or (G25) the connection dropped while it ran: either
        # may have committed -- so the failure it may cause is reported with that note. Only
        # the statement counts: opening the connection, or a cursor on one the client
        # already knows is closed, ran no CREATE.
        def _run_ddl(ddl: str, unconfirmed: list, sent: list) -> Optional[str]:
            sent[0] = False
            with pool.lease() as connection:
                cursor = connection.cursor()
                try:
                    sent[0] = True
                    cursor.execute(ddl)
                except Exception as exc:  # noqa: BLE001 - noted, then re-raised to the retry
                    if _is_retryable_load_error(exc):
                        self.load_stats.record_index_retry(exc)
                    if not unconfirmed[0]:
                        unconfirmed[0] = (
                            UNCONFIRMED_INDEX_NOTE
                            if is_server_unavailable(exc)
                            else UNCONFIRMED_INDEX_DROP_NOTE
                            if _is_transient_connection_error(exc)
                            else ""
                        )
                    raise
                else:
                    # P4b: the build's job id it answered with (never raises: the CREATE
                    # ran, so nothing here may make the retry run it again).
                    return _answered_job_id(cursor)
                finally:
                    _safe_close(cursor)

        retried = with_occ_retry(
            max_attempts=self._occ_max_attempts,
            base_delay=self._occ_base_delay,
            sleep=self._sleep,
            jitter=self._jitter,
            retryable=_is_retryable_load_error,
        )(_run_ddl)
        created = 0
        failures: list[str] = []
        started = time.perf_counter()
        try:
            for ddl in index_ddls:
                unconfirmed = [""]
                # Whether the last attempt sent the statement at all: one that failed to
                # connect (its token, its handshake) or to open a cursor ran no CREATE.
                sent = [False]
                try:
                    job_id = retried(ddl, unconfirmed, sent)
                except Exception as exc:  # noqa: BLE001 - isolate per-index failure
                    if unreachable_raises and (
                        _is_retryable_load_error(exc) or not sent[0]
                    ):
                        raise IndexTargetUnreachable(created, failures) from exc
                    # Log-safe: the DDL is tool-generated (no row values, no
                    # credentials) and the driver message is a schema-level error.
                    # Tag a UNIQUE index: it is not only a missing access path, it is a
                    # missing CONSTRAINT -- the target now accepts duplicates the source
                    # forbids. The reporting layer needs to tell the two apart, and the DDL
                    # (tool-generated, no row values) is the only place that knows.
                    _unique = bool(re.match(r"\s*CREATE\s+UNIQUE\b", ddl, re.IGNORECASE))
                    failures.append(
                        f"{'UNIQUE ' if _unique else ''}{_index_name_of(ddl)}: "
                        f"{_safe_error(exc)}{unconfirmed[0]}"
                    )
                    _LOGGER.warning(
                        "Post-load index creation failed (data is unaffected): %s",
                        failures[-1],
                    )
                else:
                    created += 1
                    if on_job is not None:
                        try:
                            on_job(ddl, job_id)
                        except Exception:  # noqa: BLE001 - a record of the build, not the build
                            _LOGGER.warning(
                                "Could not record the build of a submitted index (it was "
                                "submitted): %s", _index_name_of(ddl), exc_info=True,
                            )
        finally:
            # The submissions only: CREATE INDEX ASYNC returns before the index is built.
            self.load_stats.add_phase("index", time.perf_counter() - started)
        return created, failures

    @contextmanager
    def index_pass(self) -> "Iterator[Callable[[Sequence[str]], tuple[int, list[str]]]]":
        """The run-level index pass (P4a): every table's ``CREATE INDEX ASYNC`` statements
        on ONE Aurora DSQL connection, one statement at a time.

        Yields ``create(ddls, on_job=None) -> (created, failures)``, :meth:`_create_indexes`
        over one one-connection pool reused for every call (a connection the server only
        rejected a statement on is kept, one that may have ended is replaced, as on the load
        path), so the pass opens one connection -- one TLS handshake and one IAM token --
        instead of one per table. A DDL Aurora DSQL did not answer within its retry budget
        raises :class:`IndexTargetUnreachable` rather than being recorded as a missing index.
        ``on_job`` is handed each submitted DDL's build job id (P4b). The connection is
        closed when the pass ends. Its connects are not counted: each table's submissions
        count into the collector bound at the call.
        """
        pool = _ConnectionPool(self._connection_factory, 1, stats=None)

        def create(
            index_ddls: "Sequence[str]",
            on_job: "Optional[Callable[[str, Optional[str]], None]]" = None,
        ) -> "tuple[int, list[str]]":
            ddls = list(index_ddls or ())
            if not ddls:
                return 0, []
            return self._create_indexes(pool, ddls, on_job=on_job, unreachable_raises=True)

        try:
            yield create
        finally:
            pool.close_all()

    def create_indexes(
        self,
        index_ddls: list[str],
        on_job: "Optional[Callable[[str, Optional[str]], None]]" = None,
    ) -> "tuple[int, list[str]]":
        """Public post-load index pass on a fresh bounded pool (``(created, failures)``).

        A regular :meth:`import_rows` given ``index_ddls`` runs the post-load ``CREATE
        INDEX ASYNC`` pass itself; the Full Load engine leaves a run's to its run-level
        index pass instead, which uses :meth:`index_pass` (one connection for the whole
        pass) and this method only for an importer without it. It opens its own bounded
        :class:`_ConnectionPool` and delegates to :meth:`_create_indexes`, so the ``CREATE
        INDEX ASYNC`` / OCC-retry / per-DDL isolation behavior is IDENTICAL to the
        ``import_rows`` path. A no-op (and no connection opened) when there are no index
        DDLs. ``on_job`` is handed each submitted DDL's build job id (P4b).
        """
        if not index_ddls:
            return 0, []
        pool = _ConnectionPool(
            self._connection_factory, self._options.parallelism, stats=self.load_stats
        )
        try:
            return self._create_indexes(pool, index_ddls, on_job=on_job)
        finally:
            pool.close_all()


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


# The index a unique violation's primary message names: PostgreSQL and Aurora DSQL write
# 'duplicate key value violates unique constraint "<name>"' (the values are in DETAIL).
_UNIQUE_CONSTRAINT_RE = re.compile(r'violates unique constraint "(.+)"')


def violated_constraint_name(exc: BaseException) -> Optional[str]:
    """The UNIQUE index (constraint) a unique violation names, or ``None`` (P4d).

    psycopg's diagnostic ``constraint_name`` when the server sent one, else the name the
    primary message quotes. An identifier, never a value: the DETAIL line, which holds the
    row's values, is never read. Never raises.
    """
    try:
        name = getattr(getattr(exc, "diag", None), "constraint_name", None)
    except Exception:  # noqa: BLE001 - a diagnostic that cannot be read names nothing
        name = None
    if name:
        return str(name)
    lines = str(exc).strip().splitlines()
    match = _UNIQUE_CONSTRAINT_RE.search(lines[0]) if lines else None
    return match.group(1) if match else None


def _answered_job_id(cursor: Any) -> Optional[str]:
    """The job id an async DDL answered with, or ``None`` (P4b).

    Aurora DSQL's ``CREATE [UNIQUE] INDEX ASYNC`` returns one row holding its build's
    ``job_id``, as ``ALTER TABLE ASYNC`` does (the ``schema_applier._submit_returning_job_id``
    pattern); a statement with no result set (PostgreSQL, a double), no row, or a ``NULL``
    answers ``None``. Never raises: the statement has already run, so a cursor that cannot
    say must not make the caller run it again.
    """
    try:
        if getattr(cursor, "description", None) is None:
            return None
        row = cursor.fetchone()
    except Exception:  # noqa: BLE001 - the CREATE ran; only its job id is unknown
        return None
    try:
        if not row or row[0] is None:
            return None
        return str(row[0])
    except Exception:  # noqa: BLE001 - an unreadable row: no job id
        return None


# Key-value types whose value is safe to show: a surrogate key. Everything else is a
# possible natural key (email, account number, national id) and is withheld.
_SAFE_KEY_TYPES = (int, bool)


def _safe_key_value(value: object) -> object:
    """A key value safe to log: surrogate keys as-is, anything else ``<withheld>``.

    The single-value form of :func:`format_key_values`, for the DEBUG keyset/batch range
    trace where there is no column name to pair it with.
    """
    import uuid

    if isinstance(value, bool) or isinstance(value, _SAFE_KEY_TYPES):
        return value
    if isinstance(value, uuid.UUID) or value is None:
        return value
    return "<withheld>"


def format_key_values(
    columns: "Sequence[str]", row: "Mapping[str, Any]", *, limit: int = 200
) -> str:
    """Render a row's key columns for an error/audit line, withholding natural-key VALUES.

    Column NAMES are always included -- they are schema, not data, and without them the
    record cannot be located at all. VALUES are shown only for a surrogate key (integer,
    bool, UUID); anything else renders ``col=<withheld>``, because a primary key can BE
    the sensitive datum (an email or account-number PK), and this string reaches the
    downloadable error log, the activity log and its CloudWatch mirror.

    This is the rule the CDC side already applies to a dead-lettered record's pk
    (:mod:`dsql_migrator.core.cdc_dlq`, and ``DsqlSinkTask`` in the sink connector); the
    loader interpolated every key column with ``!r`` instead, so a natural-key PK was
    written out verbatim -- a Property 7 violation, in the very function whose next line
    is careful to keep the driver's row-value dump out of the same record.
    """
    import uuid

    parts: list[str] = []
    for column in columns:
        value = row.get(column)
        if isinstance(value, bool) or isinstance(value, _SAFE_KEY_TYPES):
            parts.append(f"{column}={value}")
        elif isinstance(value, uuid.UUID):
            parts.append(f"{column}={value}")
        elif value is None:
            # NULL is not a value anyone can be identified by, and "col=None" is the
            # actionable fact for a key that should never be null.
            parts.append(f"{column}=None")
        else:
            parts.append(f"{column}=<withheld>")
    rendered = ", ".join(parts)
    if len(rendered) > limit:
        rendered = rendered[: limit - 3] + "..."
    return rendered


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


def _estimate_row_bytes(row: Mapping[str, object]) -> int:
    """Cheaply estimate a row's payload size in bytes (for the batch byte cap).

    A heuristic sum over the row's values -- ``bytes`` by length, ``str`` by its
    UTF-8 byte length, everything else by its ``str`` length, ``None`` ~1. It need
    not be exact: it only has to keep a batch comfortably under the DSQL 10 MiB
    per-write-transaction limit, for which ``MAX_BATCH_BYTES`` leaves headroom.

    A ``str`` is counted by its ENCODED (UTF-8) byte length, not its character
    count: DSQL's per-transaction cap is on WIRE bytes, and a multi-byte string
    (CJK is 3 bytes/char, most emoji 4) is far larger on the wire than its length.
    Counting characters under-estimated such a batch, so a CJK/emoji-heavy page
    could pass the row-count/byte gate here yet exceed 10 MiB on the wire and be
    rejected. This is the EXACT wire size (not a ``*4`` worst-case bound, which
    would needlessly over-split plain-ASCII batches).
    """
    total = 0
    for value in row.values():
        if value is None:
            total += 1
        elif isinstance(value, (bytes, bytearray, memoryview)):
            total += len(value)
        elif isinstance(value, str):
            total += len(value.encode("utf-8"))
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
    # WITHHELD for a natural key, exactly as a quarantine record's pk is. This trace used
    # to be reachable only via DSQL_MIGRATOR_LOG_LEVEL + a restart, so its docstring called
    # a natural-key PK "the operator's risk" -- but the Settings tab's level switch now
    # turns it on, which would put an email/account-number range one click away.
    return (
        _safe_key_value(work.rows[0].get(pk)),
        _safe_key_value(work.rows[-1].get(pk)),
    )


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


def _aggregate_result(
    agg: "_RunAggregate",
    batches_skipped: int,
    indexes_created: int,
    *,
    cancelled: bool = False,
) -> BatchedImportResult:
    """Build a :class:`BatchedImportResult` from the run's folded aggregate.

    The per-batch outcomes were folded into ``agg`` as they resolved (see _RunAggregate)
    and skipped batches counted in a single int -- no per-batch list is retained, so the
    result reads the same totals off the running fold at O(1) memory.
    """
    return BatchedImportResult(
        rows_loaded=agg.rows_loaded,
        conflicts=agg.conflicts,
        batches_completed=agg.batches_completed,
        batches_skipped=batches_skipped,
        failures=agg.failures,
        first_error=agg.first_error,
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
    "IndexTargetUnreachable",
    "UNCONFIRMED_INDEX_NOTE",
    "UNCONFIRMED_INDEX_DROP_NOTE",
    "batch_chunk_id",
    "build_insert_statement",
    "is_unconfirmed_index_failure",
    "render_insert_statement",
    "safe_error_message",
    "violated_constraint_name",
]


# Public alias for the value-free error renderer, so callers outside this module
# (e.g. the Full Load engine's per-table failure handler) sanitize driver
# exceptions the same way before logging them (Property 7).
safe_error_message = _safe_error
