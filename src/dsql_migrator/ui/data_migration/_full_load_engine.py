# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Full Load backend run engine for the Data Migration screen (NiceGUI-free).

The run inputs, the :class:`DataMigrator` seam, the run orchestration
(:func:`run_full_load` / :func:`run_data_migration` and the parallel per-table
runner), and the in-process reference migrator (:class:`BatchedTableMigrator`)
live here, split out of the screen module so they can be unit tested directly
without touching NiceGUI. The screen package re-exports every name from this
module, so the public import surface is unchanged.
"""

from __future__ import annotations

import logging
import multiprocessing
import multiprocessing.queues
import re
import threading
import time as _time
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    TimeoutError as FuturesTimeoutError,
    as_completed,
)
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Callable, Iterator, Mapping, NamedTuple, Optional, Protocol, Sequence, Union,
)

from dsql_migrator.config import SecretValue, load_config
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.exporter import ExportCancelled, TableExporter
from dsql_migrator.core.introspector import (
    is_source_transient_error,
    source_error_hint,
)
from dsql_migrator.core.job_manager import JobHandle
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
    safe_error_message,
)
from dsql_migrator.core.target_connection import (
    DsqlConnector,
    is_transient_connection_error,
    target_error_hint,
)
from dsql_migrator.core.converter import (
    SchemaConverter,
    SchemaConvertOptions,
    TableConversion,
    parse_target_column_types,
    parse_target_primary_key,
)
from dsql_migrator.core.schema_applier import (
    apply_foreign_key, recreate_table, validate_foreign_key,
)
from dsql_migrator.core.validation_sql import (
    build_orphan_count_sql,
    build_pg_orphan_page_first_sql,
    build_pg_orphan_page_next_sql,
    single_pk_column,
)
from dsql_migrator.core.models import (
    ChunkState,
    DataErrorRecord,
    ForeignKeyDef,
    MigrationJob,
    SourceConnectionConfig,
    SourceInventory,
    SourceType,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
    Watermark,
    apply_lob_exclusions,
)
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.watermark import WatermarkCapturer, estimate_source_rows
from dsql_migrator.ui.connect import make_source_engine_factory

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Run inputs and the Data Migrator seam (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DataMigrationInputs:
    """Everything needed to run a data migration for one session.

    ``inventory`` is the Step 1 (Evaluation) inventory, so the source is not
    re-introspected. ``on_conflict`` defaults to the idempotent
    :attr:`~dsql_migrator.core.batched_import.OnConflictMode.DO_NOTHING`
    (Property 3).
    """

    source_config: SourceConnectionConfig
    source_password: Optional[SecretValue]
    target_config: TargetConnectionConfig
    inventory: SourceInventory
    on_conflict: OnConflictMode = OnConflictMode.DO_NOTHING
    # Optional global AWS profile, threaded into the target DSQL connection so
    # the load shares the single credential context (Requirements 9.5/9.7).
    aws_profile: Optional[str] = None
    # Unused by the in-process streaming importer (which never materializes a
    # whole table to disk or S3); retained only as inert configuration carried
    # from AppConfig.
    staging_bucket: Optional[str] = None
    staging_prefix: str = "mysql-dsql-migrator/full-load"
    # Tables (qualified names) the user confirmed to load fresh: their target is
    # DROPped and recreated from converted DDL before loading (DSQL has no
    # TRUNCATE). Empty by default, so a normal load never drops anything.
    replace_tables: frozenset[str] = frozenset()
    # When True, CDC is (or will be) streaming into the target during this Full
    # Load, so the load must NOT DROP+recreate any table (that races the live
    # sink) and must be idempotent against rows the sink already wrote. The load
    # then uses ``SKIP_EXISTING`` (insert only missing primary keys, never
    # overwrite a newer CDC row, never use the DSQL-unsafe ON CONFLICT) and skips
    # all DROP/replace. Set for the Full load + CDC flow where connectors start
    # before the Full Load (gapless, no offset seeding needed).
    cdc_coexisting: bool = False
    # The CDC stack this Full Load will hand off to, for a PostgreSQL Full Load + CDC
    # run. When set (PostgreSQL source only), the watermark capture creates a logical
    # replication slot + publication ON THE SOURCE at the consistency point (named
    # deterministically from this stack name) so CDC resumes from the slot's LSN with no
    # gap; the slot pins WAL until CDC consumes it. Left None for a Full-Load-only run
    # (a slot with no consumer would pin WAL and fill the source disk) and always for a
    # MySQL source (which bridges via the binlog offset-seeder, not a slot). This is the
    # PostgreSQL gapless-handoff signal -- distinct from cdc_coexisting, which is MySQL's
    # CDC-live-during-load (SKIP_EXISTING) model that PostgreSQL deliberately does not use.
    cdc_stack_name: Optional[str] = None
    # True when this run belongs to a Full Load + CDC migration (ANY source), so the
    # post-load foreign-key pass MUST be deferred to cut over. cdc_coexisting and
    # cdc_stack_name only cover two of the three CDC sub-flows -- connectors-first
    # (MySQL SKIP_EXISTING, cdc_coexisting) and the PostgreSQL slot handoff
    # (cdc_stack_name). A MySQL Full-Load-FIRST -> binlog-watermark handoff (CDC started
    # AFTER the load, the "Automatic" start point) has NEITHER set at load end, so
    # without this flag the FK pass would run at load end and the later out-of-order sink
    # stream would dead-letter every child row whose parent has not arrived yet
    # (SQLSTATE 23503). Deriving this from migration_type covers all three sub-flows.
    is_cdc_migration: bool = False
    # Per-table APPLIED target conversion (schema/create/index DDL), keyed by
    # qualified table name, honoring the user's Schema Conversion edits. Single
    # source of truth for the target schema, used to (a) recreate a table on a
    # fresh/replace load from the *applied* DDL -- not a deterministic
    # re-derivation that would clobber a user remap -- and (b) drive Full Load
    # value conversion off the applied target column types (e.g. a TINYINT(1)
    # remapped to smallint loads as an integer, not a boolean). A table absent
    # here falls back to the deterministic conversion / source-derived types.
    table_conversions: Mapping[str, TableConversion] = field(default_factory=dict)
    # Converted target view DDLs (view name -> CREATE VIEW SQL), used ONLY on a
    # "drop & reload" (replace) run: a view that SELECTs from a replaced table
    # blocks that table's DROP ("other objects depend on it"). Before the replace
    # tables are recreated, the views that reference them are dropped; after the
    # load, those views are recreated -- so a clean reload succeeds without a blunt
    # DROP ... CASCADE and the user's views survive. Empty on an append run or when
    # nothing is being replaced.
    dependent_view_ddls: Mapping[str, str] = field(default_factory=dict)
    # Oversized-LOB columns the user opted to EXCLUDE from the migration, keyed by
    # qualified table name -> the set of column names to drop. The SAME selection
    # that feeds CDC's column.exclude.list (single source of truth, so Full Load
    # and CDC never disagree). Applied by dropping the column from the effective
    # TableDef before streaming: the exporter's SELECT list and the importer's
    # INSERT list both derive from ``table.columns``, so the column is read from
    # neither the source nor written to the target. A primary-key column is never
    # excluded (candidates exclude PKs; the engine also guards). Empty => nothing
    # is dropped (the default; oversized rows are quarantined per-row instead).
    excluded_lob_columns: Mapping[str, frozenset[str]] = field(default_factory=dict)


@dataclass(frozen=True)
class TableLoadResult:
    """Outcome of loading one table: rows newly inserted and rows skipped.

    ``rows_skipped`` are source rows that already existed on the target and were
    skipped by the idempotent ``INSERT ... ON CONFLICT DO NOTHING`` load. Keeping
    it distinct from ``rows_loaded`` lets the completeness check recognize a
    table as fully present (loaded + skipped == source) instead of falsely
    reporting a "row-count mismatch" when its rows pre-existed.
    """

    rows_loaded: int
    rows_skipped: int = 0
    # Rows isolated (quarantined) on a permanent error: the rest of the table
    # still loaded; each record (PK + reason, no values) is surfaced in the error
    # log so the loss is visible, never silent. Empty tuple for a clean load.
    rows_quarantined: int = 0
    quarantine_records: tuple = ()
    # Post-load ``CREATE INDEX ASYNC`` statements that failed (one message each).
    # Distinct from quarantined rows: the table's DATA is complete, so the table is
    # reported as loaded -- only an access path is missing. Surfaced as a warning so
    # the operator can add the index later (the common cause, DSQL's 24-index limit,
    # is flagged before loading by the TOO_MANY_INDEXES assessment rule).
    index_failures: tuple = ()


def _as_load_result(value: "Union[int, TableLoadResult]") -> TableLoadResult:
    """Normalize a ``migrate_table`` return to a :class:`TableLoadResult`.

    Accepts a bare row count (back-compat with fakes/older migrators that return
    an ``int``) or a full :class:`TableLoadResult`.
    """
    if isinstance(value, TableLoadResult):
        return value
    return TableLoadResult(rows_loaded=int(value))


class DataMigrator(Protocol):
    """The export/import seam driven by :func:`run_data_migration`.

    An implementation captures the export consistency point once
    (:meth:`capture_watermark`) and migrates one table at a time
    (:meth:`migrate_table`), returning the rows loaded (and optionally rows
    skipped) and raising on a per-table failure. The reference
    :class:`BatchedTableMigrator` wires the real Task 8 components; tests supply
    a fake.
    """

    def capture_watermark(self, tables: Sequence[TableDef]) -> Watermark:
        """Capture the export consistency point for the tables being migrated.

        Scoped to ``tables`` (the selection) so the snapshot counts cover only
        what is migrated -- never the whole source inventory, which would make a
        small selection wait on huge unrelated tables.
        """

    def migrate_table(
        self,
        table: TableDef,
        *,
        on_rows: Optional[Callable[[int, int], None]] = None,
        should_cancel: Optional[Callable[[], bool]] = None,
    ) -> "Union[int, TableLoadResult]":
        """Export then import ``table``; return rows loaded (raise on failure).

        ``on_rows``, when given, is called as each batch lands with
        ``(rows_inserted, rows_skipped)`` -- where ``rows_skipped`` already existed
        on the target and were left unchanged by the idempotent load -- so the
        caller can record live cumulative progress (counting skipped rows so a
        mostly-skipped re-load still shows movement) instead of only the final
        total (Requirement 8.3). ``should_cancel``, when given, is polled between
        batches; if it turns ``True`` the load stops early and the table is
        reported incomplete by raising :class:`_FullLoadStopped` so the caller can
        leave it for retry (the re-load is idempotent).
        """


# Builds a :class:`DataMigrator` bound to the run's inputs.
MigratorFactory = Callable[[DataMigrationInputs], DataMigrator]


def _sync_identity_sequences_after_load(
    inputs: "DataMigrationInputs",
    table_names: "Sequence[str]",
    *,
    sync: "Optional[Callable[..., dict]]" = None,
) -> dict:
    """Advance target identity sequences past the rows just loaded. Best-effort.

    Required, not cosmetic. The converter's ``IDENTITY_WITH_CACHE`` strategy emits
    ``GENERATED BY DEFAULT AS IDENTITY`` -- ``BY DEFAULT`` being what allows Full Load to
    write the source's own key values -- but an explicitly-supplied value does NOT advance
    the sequence. So after a load the sequence still sits at its start while those values
    are already taken, and the application's FIRST insert after cut-over dies on a
    duplicate key. Confirmed on a live ap-northeast-2 cluster.

    It is the worst failure shape available: counts and checksums MATCH, so Validation
    passes clean, and it only appears after cut-over -- once the source is frozen and
    rollback is no longer trivial. Hence repairing it here, at the point the tool knows
    both the loaded tables and the target, rather than leaving a runbook step to remember.

    Returns ``{table: restart_value_or_None}``. Never raises: a load that finished
    correctly must not be reported FAILED because of this follow-up, and a table with no
    identity column (the KEEP_INTEGER default) legitimately yields ``None``. The caller
    logs the outcome so an unrepaired sequence is never silent.

    ``sync`` is an injectable seam (tests pass a fake; production uses the real
    introspector over a DSQL connection).
    """
    if not table_names:
        return {}
    try:
        if sync is None:
            from dsql_migrator.core.target_introspector import (
                sync_identity_sequences as sync,
            )
        # Build the connection factory LAZILY, inside the callee, so an injected fake
        # never has to satisfy DsqlConnector's config validation just to be reached.
        def _factory():
            return DsqlConnector(
                inputs.target_config, aws_profile=inputs.aws_profile
            ).connect()

        return sync(list(table_names), connection_factory=_factory) or {}
    except Exception as exc:  # noqa: BLE001 - never fail a completed load on this
        log_activity(
            ActivityCategory.FULL_LOAD,
            "identity sequence sync failed",
            # FAILURE, not INFO: an unrepaired identity sequence is a post-cut-over
            # duplicate-key outage, so it must stand out in the audit trail even though
            # the load itself succeeded.
            status=ActivityStatus.FAILURE,
            detail=(
                f"Could not advance target identity sequences: "
                f"{safe_error_message(exc)}. If any target table uses an identity "
                "primary key, run ALTER TABLE <t> ALTER COLUMN <pk> RESTART WITH "
                "<max(pk)+1> before cut-over, or the application's first insert will "
                "hit a duplicate key."
            ),
        )
        return {}


# ---------------------------------------------------------------------------
# Run orchestration (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


def _seed_chunks(job: MigrationJob, table_names: Sequence[str]) -> None:
    """Initialize one ``PENDING`` chunk per table on ``job`` (start of a run)."""
    job.chunks = [
        ChunkState(chunk_id=name, status="PENDING") for name in table_names
    ]
    job.watermark = None
    _recompute_progress(job)


def _start_chunk(job: MigrationJob, chunk_id: str) -> None:
    """Mark ``chunk_id`` ``IN_PROGRESS``, count the attempt, and stamp the start."""
    chunk = _find_chunk(job, chunk_id)
    if chunk is not None:
        chunk.status = "IN_PROGRESS"
        chunk.attempts += 1
        # Reset timing so a retry's elapsed/ETA reflects this attempt only.
        chunk.started_at = datetime.now(timezone.utc)
        chunk.finished_at = None
    _recompute_progress(job)


def _complete_chunk(
    job: MigrationJob,
    chunk_id: str,
    rows_loaded: int,
    rows_skipped: int = 0,
    rows_quarantined: int = 0,
) -> None:
    """Mark ``chunk_id`` ``DONE``; record loaded/skipped/quarantined rows and finish.

    ``rows_quarantined`` are rows permanently DROPPED (a non-retryable per-row error),
    recorded on the chunk so the run-level completeness verdict can see the gap instead
    of inferring completeness from a row count the estimate's tolerance absorbs.
    """
    chunk = _find_chunk(job, chunk_id)
    if chunk is not None:
        chunk.status = "DONE"
        chunk.rows_loaded = rows_loaded
        chunk.rows_skipped = rows_skipped
        chunk.rows_quarantined = rows_quarantined
        chunk.finished_at = datetime.now(timezone.utc)
    _recompute_progress(job)


def _fail_chunk(job: MigrationJob, chunk_id: str) -> None:
    """Mark ``chunk_id`` ``FAILED`` (other tables continue)."""
    chunk = _find_chunk(job, chunk_id)
    if chunk is not None:
        chunk.status = "FAILED"
        chunk.finished_at = datetime.now(timezone.utc)
    _recompute_progress(job)


def _advance_chunk_rows(
    job: MigrationJob, chunk_id: str, delta_loaded: int, delta_skipped: int = 0
) -> None:
    """Add to an in-progress chunk's loaded/skipped row counts (live progress).

    Lets the UI show cumulative rows as batches land mid-table instead of only a
    final total (Requirement 8.3). ``delta_skipped`` carries rows that already
    existed on the target (idempotent skip), advanced separately so a re-load
    that mostly skips already-present rows still shows movement (per-table
    progress is rows-present = loaded + skipped) instead of appearing stuck at
    zero. Only applied while the chunk is ``IN_PROGRESS`` so a late callback
    cannot disturb a terminal state; :func:`_complete_chunk` later sets the exact
    final totals, so these are monotonic estimates that converge to the
    authoritative counts. Table-level ``progress_pct`` is terminal-state based and
    unaffected, so it is not recomputed here.
    """
    chunk = _find_chunk(job, chunk_id)
    if chunk is not None and chunk.status == "IN_PROGRESS":
        chunk.rows_loaded += delta_loaded
        chunk.rows_skipped += delta_skipped


def _start_chunk_if_pending(job: MigrationJob, chunk_id: str) -> None:
    """Start ``chunk_id`` only if it is still ``PENDING`` -- idempotent for the multiprocess
    'started' signal.

    In the multiprocess path each worker signals when it ACTUALLY begins its table; several
    shards of one table signal, and a straggler may signal after a sibling already started
    it. This must not re-stamp ``started_at`` (which would reset the table's elapsed/ETA),
    unlike :func:`_start_chunk`, which the single-process and retry paths call to deliberately
    re-stamp per attempt.
    """
    chunk = _find_chunk(job, chunk_id)
    if chunk is not None and chunk.status == "PENDING":
        _start_chunk(job, chunk_id)


def _start_then_advance(
    job: MigrationJob, chunk_id: str, delta_loaded: int, delta_skipped: int
) -> None:
    """Ensure the chunk is started (in case its 'started' signal was dropped) then add the
    live row deltas -- the drain applies both from one worker progress message."""
    _start_chunk_if_pending(job, chunk_id)
    _advance_chunk_rows(job, chunk_id, delta_loaded, delta_skipped)


def _find_chunk(job: MigrationJob, chunk_id: str) -> Optional[ChunkState]:
    """Return the chunk named ``chunk_id`` on ``job``, if present."""
    return next((chunk for chunk in job.chunks if chunk.chunk_id == chunk_id), None)


def _recompute_progress(job: MigrationJob) -> None:
    """Recompute ``progress_pct`` and ``error_count`` from chunk states.

    ``progress_pct`` is the fraction of tables that reached a terminal state
    (``DONE`` or ``FAILED``), so the bar advances as failures are recorded too;
    ``error_count`` is the number of failed tables.
    """
    total = len(job.chunks)
    if total == 0:
        job.progress_pct = 0.0
        job.error_count = 0
        return
    settled = sum(1 for chunk in job.chunks if chunk.status in ("DONE", "FAILED"))
    job.progress_pct = round(settled / total * 100.0, 4)
    job.error_count = sum(1 for chunk in job.chunks if chunk.status == "FAILED")


def _error_code(exc: BaseException) -> Optional[str]:
    """Return a SQLSTATE-like error code from ``exc`` when available.

    psycopg/DB-API errors expose ``sqlstate``; anything else yields ``None`` so
    the record still captures the message without an code.
    """
    code = getattr(exc, "sqlstate", None)
    return str(code) if code else None


def full_load_progress_caption(job: Optional[MigrationJob]) -> str:
    """Return a live caption describing what an in-progress Full Load is doing.

    Gives the user concrete, moving feedback instead of a static spinner: the
    watermark-capture phase, the table currently exporting/loading, and how many
    of the selected tables have settled. Pure/NiceGUI-agnostic for testing.
    """
    if job is None or not job.chunks:
        return "Starting Full Load…"
    if job.watermark is None:
        return "Capturing export watermark (consistent snapshot)…"
    total = len(job.chunks)
    done = sum(1 for chunk in job.chunks if chunk.status in ("DONE", "FAILED"))
    in_progress = {
        chunk.chunk_id for chunk in job.chunks if chunk.status == "IN_PROGRESS"
    }
    current = next(
        (chunk.chunk_id for chunk in job.chunks if chunk.status == "IN_PROGRESS"),
        None,
    )
    if current is not None:
        base = f"Exporting and loading {current} ({done}/{total} tables done)…"
    else:
        base = f"Exporting and loading tables ({done}/{total} done)…"
    # Source-load governor: if any still-in-progress table has a reader paused (source
    # Threads_running over the configured ceiling), say so -- so a deliberately throttled
    # read is never mistaken for a hang. Intersect with IN_PROGRESS so a stale count on a
    # settled table is never shown.
    throttled = sorted(
        name
        for name, count in job.throttled_tables.items()
        if count > 0 and name in in_progress
    )
    if throttled:
        plural = "s" if len(throttled) != 1 else ""
        base += (
            f" — paused on source load ({len(throttled)} table{plural}: source "
            "Threads_running over the configured ceiling)"
        )
    return base


def _set_table_throttled(job: MigrationJob, table_name: str, paused: bool) -> None:
    """Update the per-table paused-reader count from one throttle transition.

    A table has K shard readers, each of which pauses/resumes independently, so this
    keeps a count (not a flag): ``paused`` increments it, a resume decrements it, and it
    is removed at zero. ``job.throttled_tables[name] > 0`` therefore means "at least one
    reader of this table is currently paused by the source-load governor".
    """
    counts = job.throttled_tables
    if paused:
        counts[table_name] = counts.get(table_name, 0) + 1
    else:
        remaining = counts.get(table_name, 0) - 1
        if remaining > 0:
            counts[table_name] = remaining
        else:
            counts.pop(table_name, None)


# Default number of tables migrated concurrently during Full Load. Operator-tunable
# via DSQL_MIGRATOR_FULL_LOAD_TABLE_PARALLELISM (read at run time below). Combined
# with each table's internal batch parallelism, this bounds the total concurrent
# DSQL connections (table_parallelism x batch_parallelism), which must stay within
# the cluster connection quota.
FULL_LOAD_TABLE_PARALLELISM = 4

# Per-socket read/write timeout (seconds) for the Full Load source stream. Each
# keyset page is bounded (DEFAULT_BATCH_SIZE rows) and returns quickly, so this
# generous ceiling only trips on a genuine stall -- a server/network hang
# mid-read -- turning a hung table into a clean FAILED (retryable) instead of a
# job stuck in RUNNING forever. See SOURCE_READ_TIMEOUT_SECONDS in introspector.
FULL_LOAD_SOURCE_READ_TIMEOUT_SECONDS = 300

# Flush live per-table row progress to the job once this many rows accumulate
# since the last flush. Batch callbacks fire frequently on large tables, so this
# coalesces them into far fewer job updates (each persists the job snapshot)
# while still advancing the count visibly mid-table; the exact final total is set
# on completion regardless of this threshold.
PROGRESS_FLUSH_ROWS = 10_000

# Safety ceiling on concurrent SOURCE snapshot readers = table_parallelism x
# reader_shards. Each holds a long-lived source connection; this caps the product
# so a high table_parallelism x reader_shards can't exhaust the source MySQL's
# max_connections. Reader sharding is clamped down (never up) to honor it.
_MAX_SOURCE_READERS = 32


class _FullLoadStopped(Exception):
    """Raised when a table's load is stopped early by a cooperative cancel.

    Distinct from a real load error so the runner can mark the table for retry
    (the re-load is idempotent) without recording it as a data error.
    """


class FullLoadIncompleteError(RuntimeError):
    """Raised when a Full Load run ends with one or more tables failed.

    Per-table failures are isolated (the other tables still load and are
    recorded), but a run that did not load every selected table has produced
    INCOMPLETE target data. Raising once at the end -- after every table has been
    attempted -- marks the job/step ``FAILED`` (never ``DONE``), so an incomplete
    load can never be mistaken for success and the prerequisite gate keeps
    Validation/cut-over from proceeding on partial data. The user is pointed to
    the downloadable error log and "Retry failed tables". This is distinct from a
    cooperative stop (:class:`_FullLoadStopped`), which the manager records as
    ``CANCELLED``.
    """


def _fail_unfinished_chunks(job: MigrationJob) -> None:
    """Mark every non-``DONE`` chunk ``FAILED`` after a stop (so retry resumes).

    On a cooperative cancel, tables that never started (``PENDING``) or were
    interrupted are left unfinished. Converting them to ``FAILED`` lets the
    existing "Retry failed tables" path pick up exactly the remaining work while
    already-``DONE`` tables are carried forward (idempotent re-load).
    """
    for chunk in job.chunks:
        if chunk.status != "DONE":
            chunk.status = "FAILED"  # type: ignore[assignment]
    _recompute_progress(job)


class _TableLoadOutcome(Enum):
    """How a single table's Full Load attempt ended (run-completion classifier).

    ``QUARANTINED`` is distinct from ``FAILED``: the table loaded every loadable
    row but had to permanently drop one or more rows a hard DSQL limit rejects
    (e.g. a value over the ~1 MiB per-value cap) -- a real, non-retryable gap, but
    not a table failure. Keeping them separate lets the run offer an
    accept-and-continue override for a quarantine-only incompleteness while still
    blocking on retryable failures.
    """

    LOADED = "loaded"
    QUARANTINED = "quarantined"
    FAILED = "failed"
    STOPPED = "stopped"


class _RunCounts(NamedTuple):
    """Fresh per-run tallies of tables that did not fully load (this run only)."""

    real_failed: int
    quarantined: int


# ---------------------------------------------------------------------------
# Multiprocess worker infrastructure (Phase 1 — true multi-core Full Load)
# ---------------------------------------------------------------------------

# Stagger delay between process submissions to avoid DSQL IAM token/connection burst.
_PROCESS_LAUNCH_STAGGER_SECONDS = 0.15

# How long Stop Full Load waits for the worker processes to wind down cooperatively
# before it stops waiting on them. Cancellation is cooperative: a worker finishes its
# current batch, sees ``cancel_event`` and returns. That is normally seconds, so this
# is a generous backstop -- not a normal timeout. Its job is to guarantee that Stop
# ALWAYS terminates: without it a worker wedged anywhere (a hung socket read, a
# blocked queue put -- the observed deadlock) left the parent in ``as_completed``
# forever while the UI insisted it was "finishing the current batch".
_CANCEL_GRACE_SECONDS = 90.0

# Sentinel put onto progress_queue to signal drain thread to stop.
_PROGRESS_SENTINEL = None
# Marker for a worker's "I actually started this table" signal, sent as
# ``(_CHUNK_STARTED, table_name)``. The parent marks the chunk IN_PROGRESS when a bounded-pool
# worker BEGINS the table, not at submission time (when every table would otherwise show
# in-progress at once with inflated per-table elapsed/ETA).
_CHUNK_STARTED = "\x00chunk-started"
# Marker for a worker's source-load-governor throttle transition, sent as
# ``(_CHUNK_THROTTLED, table_name, paused: bool)``. The drain maintains a per-table
# paused-reader count on the job so the progress caption can show a "paused on source
# load" hint. Distinct 3-tuple sentinel so it is matched BEFORE the generic
# ``(table, delta_loaded, delta_skipped)`` progress tuple.
_CHUNK_THROTTLED = "\x00chunk-throttled"


@dataclass(frozen=True)
class _TableWorkerArgs:
    """Picklable input for one table's Full Load in a worker process."""

    job_id: str
    table: TableDef
    inputs: "DataMigrationInputs"
    # True when the parent already DROP+recreated this empty target in the serial
    # pre-pass; the worker then loads without re-running the DDL (no startup storm).
    pre_recreated: bool = False


@dataclass(frozen=True)
class _ShardWorkerArgs:
    """Picklable input for one PK-range shard of a single table (Phase 2).

    Each shard loads a disjoint PK slice of the same table into the same target
    using idempotent INSERT ON CONFLICT DO NOTHING. Multiple shards of the same
    table run concurrently in separate processes, each with its own MySQL snapshot
    + DSQL connection pool.
    """

    job_id: str
    table: TableDef
    inputs: "DataMigrationInputs"
    pk_lower: Optional[int]
    pk_upper: Optional[int]
    shard_index: int
    # A dialect-exported snapshot id all shards import so they share ONE point-in-time cut
    # (consistent sharded read of a REPLACE load without CDC). None => each shard uses its
    # own snapshot (MySQL, or the CDC-handoff path).
    shared_snapshot_id: Optional[str] = None


@dataclass(frozen=True)
class _TableWorkerResult:
    """Picklable result from a worker process (never raises past boundary)."""

    table_name: str
    status: str  # "DONE" | "FAILED" | "STOPPED"
    rows_loaded: int = 0
    rows_skipped: int = 0
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    quarantine_records: tuple = ()
    # Post-load index DDLs that failed. Carried back to the parent so a fully-loaded
    # table still reports its missing indexes (a warning), rather than the worker
    # either swallowing them or failing the table over an access path.
    index_failures: tuple = ()
    shard_index: int = -1


def _slim_worker_inputs(
    inputs: "DataMigrationInputs", table_name: str
) -> "DataMigrationInputs":
    """Return per-table inputs for a worker: drop the all-tables inventory + conversions.

    A worker migrates ONE table (passed separately as ``args.table``) and reads only this
    table's conversion via ``table_conversions.get(table.name)``; ``inventory`` is never read
    in the worker/migrator path. Pickling the full ``SourceInventory`` + every table's DDL
    into EACH of N worker submissions is O(tables^2) serialization/IPC (seconds-to-minutes of
    parent-side pickling at a few thousand tables). Slim both to this one table so a many-
    table migration is O(tables); the small fields (connection configs, flags, excluded-LOB
    map, view DDLs) are preserved unchanged.
    """
    conv = inputs.table_conversions.get(table_name)
    return replace(
        inputs,
        inventory=SourceInventory(),
        table_conversions=({table_name: conv} if conv is not None else {}),
    )


# Per-worker-process globals, set by _init_worker (ProcessPoolExecutor initializer).
_worker_progress_queue: Optional["multiprocessing.Queue[object]"] = None
_worker_cancel_event: Optional["multiprocessing.synchronize.Event"] = None


def _init_worker(
    progress_queue: "multiprocessing.Queue[object]",
    cancel_event: "multiprocessing.synchronize.Event",
) -> None:
    """Initializer for ProcessPoolExecutor workers — stores shared IPC objects.

    Queue and Event are NOT picklable so they can't be passed as submit() args.
    Instead they are inherited via the initializer (which receives them directly
    from the parent through process inheritance, not pickle).
    """
    global _worker_progress_queue, _worker_cancel_event  # noqa: PLW0603
    _worker_progress_queue = progress_queue
    _worker_cancel_event = cancel_event


def _report_progress(
    progress_queue: "Optional[multiprocessing.Queue[object]]", message: object
) -> None:
    """Send a progress message from a worker process WITHOUT ever blocking.

    A plain ``queue.put`` blocks once the pipe's buffer is full, and that is how Stop
    Full Load deadlocked: the drain thread stopped consuming, the workers filled the
    queue and parked in ``sem_wait`` inside ``put``, and there they could no longer
    reach the code that polls ``cancel_event``. The parent's ``as_completed`` then
    waited forever while the UI showed a reassuring "Stopping… finishing the current
    batch."

    Progress is pure telemetry: the counters are DELTAS that the next flush re-accrues,
    and the authoritative totals come from the worker's return value, so dropping a
    message costs at most a slightly stale progress bar. Correctness never depends on
    it -- unlike liveness, which did. So use ``put_nowait`` and swallow a full queue.
    """
    if progress_queue is None:
        return
    try:
        progress_queue.put_nowait(message)
    except Exception:  # noqa: BLE001 - queue.Full / closed pipe: telemetry only
        pass


def _retry_source_drops_in_process(
    work,
    *,
    cancelled,
    table_name: str,
    release=None,
    source_type: SourceType = SourceType.MYSQL,
):
    """Run ``work()`` in a child process, retrying a dropped SOURCE connection.

    The in-process twin of :func:`_migrate_table_with_source_retry` (see it for why a
    retry RE-READS the table instead of resuming the dead snapshot). It is separate
    because a child process has no ``JobHandle`` and cannot ``log_activity`` to the
    parent's log -- it signals only through its returned ``_TableWorkerResult`` -- so
    the retry state lives entirely in this call and the outcome is what the parent
    sees. ``cancelled`` is a zero-arg predicate over the shared cancel event.

    ``release`` (optional) closes the failed attempt's source row stream before the
    retry waits, so the dead connection is not pinned for the whole backoff
    (:func:`_release_source_stream`).

    Without this, the multiprocess load path (the default for a large migration)
    would still fail a table on an Aurora failover while the single-process path
    recovered -- the same run behaving differently depending on the worker mode.
    """
    config = load_config()
    attempts = max(1, int(config.full_load_source_retry_attempts))
    backoff = max(0.0, float(config.full_load_source_retry_backoff_seconds))

    for attempt in range(1, attempts + 1):
        cause = ""
        try:
            return work()
        except _FullLoadStopped:
            raise
        except ExportCancelled:
            raise
        except Exception as exc:  # noqa: BLE001 - classify transient vs permanent
            if attempt >= attempts or not is_source_transient_error(exc, source_type):
                raise
            if cancelled():
                raise
            # Keep only the TEXT of the failure, then leave the except block before
            # waiting -- see _wait_before_source_reread for why that matters.
            cause = f"{type(exc).__name__}: {exc}"
        _release_source_stream(release)
        if not _wait_before_source_reread(
            table_name=table_name, attempt=attempt, attempts=attempts,
            backoff=backoff, cause=cause, cancelled=cancelled,
        ):
            raise _FullLoadStopped()
    raise AssertionError("source retry loop exited without a result")


def _close_row_streams(rows, shard_sources) -> None:
    """Close source row generators so their MySQL connections are freed now.

    ``TableExporter.stream_converted_rows`` yields from inside a
    ``START TRANSACTION WITH CONSISTENT SNAPSHOT`` and disposes its engine in the
    generator's own ``finally``, so that cleanup runs only when the generator is
    exhausted, explicitly closed, or collected. On the failure path neither of the
    first two happens and the traceback keeps it referenced, so closing it here is
    what actually returns the connection. Best-effort and exception-safe: cleanup
    must never replace the failure that triggered it.
    """
    candidates = list(shard_sources or ())
    if rows is not None:
        candidates.append(rows)
    for candidate in candidates:
        close = getattr(candidate, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except Exception:  # noqa: BLE001 - cleanup must not mask the real error
            pass


def _release_source_stream(release) -> None:
    """Close an abandoned source row stream before a retry waits (best-effort).

    The row streams are GENERATORS whose source engine is disposed in their own
    ``finally`` (see ``TableExporter.stream_converted_rows``), so an abandoned one
    holds its MySQL connection open until it is closed or collected. Closing it
    explicitly releases the dead snapshot connection immediately instead of leaving
    it pinned for the whole backoff.
    """
    if release is None:
        return
    try:
        release()
    except Exception:  # noqa: BLE001 - cleanup must never mask the real failure
        pass


def _wait_before_source_reread(
    *,
    table_name: str,
    attempt: int,
    attempts: int,
    backoff: float,
    cause: str,
    cancelled,
) -> bool:
    """Wait out the failover before re-reading; ``False`` if the user stopped.

    Called from OUTSIDE the ``except`` block on purpose. Inside it, the live
    exception keeps its traceback alive, which keeps the failed attempt's frames
    alive, which keeps that attempt's source row generator (a frame local) alive --
    so the dead source connection would stay open for the entire backoff, and the
    retry would then open ANOTHER one. At 16 tables x 8 shards that doubles the
    source connection count at exactly the moment a just-promoted Aurora writer is
    most fragile, risking ``1040 Too many connections``. Returning here drops the
    traceback first, so the old connection is released before the wait.

    The wait itself is sliced so a user Stop is honored promptly rather than after
    the full delay.
    """
    delay = backoff * (2 ** (attempt - 1))
    _LOGGER.warning(
        "Full Load source connection lost for table %s (attempt %d/%d): %s -- "
        "re-reading from a fresh snapshot in %.0fs",
        table_name, attempt, attempts, cause, delay,
    )
    waited = 0.0
    while waited < delay:
        if cancelled():
            return False
        _time.sleep(min(1.0, delay - waited))
        waited += 1.0
    return not cancelled()


def _migrate_one_table_in_process(args: _TableWorkerArgs) -> _TableWorkerResult:
    """Run entirely inside a child process. Builds own MySQL+DSQL connections.

    Module-level function (required for pickle with spawn context). Reads the
    shared progress_queue and cancel_event from worker-global state (set by
    _init_worker). Catches all exceptions and returns a plain _TableWorkerResult;
    never raises past the top-level try so unpicklable exceptions cannot break
    the ProcessPool.
    """
    progress_queue = _worker_progress_queue
    cancel_event = _worker_cancel_event
    table = args.table
    name = table.name
    # Signal that this worker actually began the table, so the parent marks the chunk
    # IN_PROGRESS now (a bounded pool runs only a few at once) rather than at submission.
    _report_progress(progress_queue, (_CHUNK_STARTED, name))
    try:
        migrator = BatchedTableMigrator(args.inputs)
        pending_loaded = 0
        pending_skipped = 0

        def _on_rows(loaded: int, skipped: int = 0) -> None:
            nonlocal pending_loaded, pending_skipped
            pending_loaded += loaded
            pending_skipped += skipped
            if pending_loaded + pending_skipped >= PROGRESS_FLUSH_ROWS:
                flush_l, flush_s = pending_loaded, pending_skipped
                pending_loaded, pending_skipped = 0, 0
                _report_progress(progress_queue, (name, flush_l, flush_s))

        _is_cancelled = lambda: (  # noqa: E731 - shared by the load + its retry
            cancel_event is not None and cancel_event.is_set()
        )
        # ``pre_recreated`` says the parent already DROP+recreated this (empty) target,
        # which is what lets a replace load use a plain INSERT. That only holds for the
        # FIRST attempt: once a retry re-reads the table, rows from the failed attempt
        # are already on the target, so a plain INSERT would collide on their keys.
        # Clearing the flag on a retry makes this worker recreate the table itself, so
        # the re-read again loads into an empty target and stays duplicate-free.
        _attempt_state = {"first": True}
        # One resume job for this table, REUSED across the in-process source-drop retries so a
        # retry skips the keyset ranges the prior attempt already committed -- only the
        # SKIP_EXISTING/append path uses it (migrate_table guards the replace/NONE path, which
        # recreates the target on retry). In-process only: this job lives in the worker, so a
        # crash / "Retry failed tables" still restarts the table from the beginning.
        _resume_job = MigrationJob(job_id=f"fullload-resume:{name}")

        # Source-load governor state -> the parent's progress drain, so the caption can
        # show "paused on source load" (Property 7: table name only, never row data).
        def _on_throttle(paused: bool, _running: Optional[int]) -> None:
            _report_progress(progress_queue, (_CHUNK_THROTTLED, name, paused))

        def _load_table():
            pre = args.pre_recreated and _attempt_state["first"]
            _attempt_state["first"] = False
            return migrator.migrate_table(
                table,
                on_rows=_on_rows,
                should_cancel=_is_cancelled,
                pre_recreated=pre,
                resume_job=_resume_job,
                on_throttle=_on_throttle,
            )

        outcome = _as_load_result(
            _retry_source_drops_in_process(
                _load_table,
                cancelled=_is_cancelled,
                table_name=name,
                source_type=args.inputs.source_config.source_type,
            )
        )
        # Flush remaining progress.
        if (pending_loaded or pending_skipped) and progress_queue is not None:
            _report_progress(progress_queue, (name, pending_loaded, pending_skipped))
        quarantine_recs = tuple(
            {"primary_key": r.primary_key, "message": r.message, "error_code": getattr(r, "error_code", None)}
            for r in (getattr(outcome, "quarantine_records", ()) or ())
        )
        return _TableWorkerResult(
            table_name=name,
            status="DONE",
            rows_loaded=outcome.rows_loaded,
            rows_skipped=outcome.rows_skipped,
            quarantine_records=quarantine_recs,
            index_failures=tuple(getattr(outcome, "index_failures", ()) or ()),
        )
    except _FullLoadStopped:
        return _TableWorkerResult(table_name=name, status="STOPPED")
    except Exception as exc:  # noqa: BLE001
        return _TableWorkerResult(
            table_name=name,
            status="FAILED",
            # safe_error_message, not f"{exc}": a psycopg error's str() keeps the
            # server DETAIL/"Failing row contains (...)" line, i.e. the offending
            # row's COLUMN VALUES. This message is surfaced to the AI DBA via
            # list_failed_full_load_tables, so it must be value-free (Property 7).
            error_message=safe_error_message(exc),
            error_code=_error_code(exc),
        )


def _migrate_shard_in_process(args: _ShardWorkerArgs) -> _TableWorkerResult:
    """Load one PK-range shard of a table in its own process (Phase 2).

    Same pattern as _migrate_one_table_in_process but reads only the
    [pk_lower, pk_upper) slice. Each shard builds its own MySQL connection +
    DSQL connection pool so they run on independent cores.
    """
    progress_queue = _worker_progress_queue
    cancel_event = _worker_cancel_event
    table = args.table
    name = table.name
    # Signal that this worker actually began the table, so the parent marks the chunk
    # IN_PROGRESS now (a bounded pool runs only a few at once) rather than at submission.
    _report_progress(progress_queue, (_CHUNK_STARTED, name))
    try:
        migrator = BatchedTableMigrator(args.inputs)
        pending_loaded = 0
        pending_skipped = 0

        def _on_rows(loaded: int, skipped: int = 0) -> None:
            nonlocal pending_loaded, pending_skipped
            pending_loaded += loaded
            pending_skipped += skipped
            if pending_loaded + pending_skipped >= PROGRESS_FLUSH_ROWS:
                flush_l, flush_s = pending_loaded, pending_skipped
                pending_loaded, pending_skipped = 0, 0
                _report_progress(progress_queue, (name, flush_l, flush_s))

        applied = args.inputs.table_conversions.get(table.name)
        target_types = (
            parse_target_column_types(applied.target_ddl) if applied else None
        )
        _is_cancelled = lambda: (  # noqa: E731 - shared by the load + its retry
            cancel_event is not None and cancel_event.is_set()
        )
        # Use plain INSERT (NONE) when the table was just DROP+recreated in the
        # parent (empty target, no conflicts possible, faster). Use SKIP_EXISTING
        # for append/CDC-coexisting loads (existing data, idempotent).
        is_replace = (
            not args.inputs.cdc_coexisting
            and table.name in args.inputs.replace_tables
        )
        conflict_mode = OnConflictMode.NONE if is_replace else OnConflictMode.SKIP_EXISTING
        # The TARGET's primary key, from the applied DDL -- the same conflict key
        # ``migrate_table`` resolves. Sharding is chosen off the SOURCE PK (a single
        # integer ``id`` is shardable), so a table whose TARGET key is a composite
        # ``(leading, id)`` reaches this path too. Passing it is required: without it
        # the importer falls back to the source PK, and a SKIP_EXISTING filter keyed
        # on a column that is not unique on the target can match a different row and
        # wrongly skip a source row (silent loss). ``None`` when the applied DDL has
        # no parseable key, which keeps the source-PK fallback for the common case.
        shard_key_columns = (
            parse_target_primary_key(applied.target_ddl) if applied else []
        ) or None
        if shard_key_columns == list(table.primary_key):
            shard_key_columns = None  # unchanged key: keep the existing fallback
        importer = migrator._importer_factory(args.inputs)

        # First attempt uses the planned mode; any retry downgrades to SKIP_EXISTING
        # so re-reading a partially-written shard stays duplicate-free. ``_live_rows``
        # holds the CURRENT attempt's generator so a retry can close it (releasing the
        # dead source connection) before waiting out the failover.
        _shard_conflict_mode = [conflict_mode]
        _live_rows: list = [None]
        # One resume job for this shard, reused across the in-process source-drop retries so a
        # retry skips the keyset ranges this shard already committed. A shard NEVER recreates
        # its target (the parent pre-recreated it once, before submitting shards), so committed
        # rows persist across a retry -- skipping them is safe on both the NONE (first attempt)
        # and SKIP_EXISTING (retry) modes. In-process only: a full "Retry failed tables"
        # recreates the target and restarts the shard from the beginning.
        _resume_job = MigrationJob(
            job_id=f"fullload-resume:{name}#shard{args.shard_index}"
        )

        # Source-load governor state -> the parent's progress drain (table name only,
        # Property 7). Multiple shards of one table report independently; the drain
        # keeps a per-table paused-reader count.
        def _on_throttle(paused: bool, _running: Optional[int]) -> None:
            _report_progress(progress_queue, (_CHUNK_THROTTLED, name, paused))

        def _load_shard():
            # Stream + import together so a retry re-opens the SHARD's own snapshot
            # from its pk_lower: the generator is single-use, and re-reading the
            # shard's whole PK range keeps it consistent as of one point in time.
            rows = migrator._exporter.stream_converted_rows(
                args.inputs.source_config,
                table,
                should_cancel=_is_cancelled,
                target_types=target_types,
                pk_lower=args.pk_lower,
                pk_upper=args.pk_upper,
                on_throttle=_on_throttle,
                shared_snapshot_id=args.shared_snapshot_id,
            )
            _live_rows[0] = rows
            return importer.import_rows(
                rows,
                table,
                job=_resume_job,
                on_batch_loaded=_on_rows,
                should_cancel=_is_cancelled,
                on_conflict=_shard_conflict_mode[0],
                key_columns=shard_key_columns,
            )

        def _attempt():
            try:
                return _load_shard()
            finally:
                _shard_conflict_mode[0] = OnConflictMode.SKIP_EXISTING

        def _release_rows() -> None:
            rows, _live_rows[0] = _live_rows[0], None
            close = getattr(rows, "close", None)
            if callable(close):
                close()

        result = _retry_source_drops_in_process(
            _attempt, cancelled=_is_cancelled, table_name=name,
            release=_release_rows,
            source_type=args.inputs.source_config.source_type,
        )
        if (pending_loaded or pending_skipped) and progress_queue is not None:
            _report_progress(progress_queue, (name, pending_loaded, pending_skipped))
        if result.cancelled:
            return _TableWorkerResult(
                table_name=name, status="STOPPED", shard_index=args.shard_index
            )
        if result.failures:
            return _TableWorkerResult(
                table_name=name, status="FAILED", shard_index=args.shard_index,
                error_message=f"{result.failures} batch(es) failed",
                rows_loaded=result.rows_loaded, rows_skipped=result.conflicts,
            )
        quarantine_recs = tuple(
            {"primary_key": r.primary_key, "message": r.message, "error_code": getattr(r, "error_code", None)}
            for r in (getattr(result, "quarantine_records", ()) or ())
        )
        return _TableWorkerResult(
            table_name=name, status="DONE", shard_index=args.shard_index,
            rows_loaded=result.rows_loaded, rows_skipped=result.conflicts,
            quarantine_records=quarantine_recs,
            index_failures=tuple(getattr(result, "index_failures", ()) or ()),
        )
    except _FullLoadStopped:
        return _TableWorkerResult(table_name=name, status="STOPPED", shard_index=args.shard_index)
    except ExportCancelled:
        return _TableWorkerResult(table_name=name, status="STOPPED", shard_index=args.shard_index)
    except Exception as exc:  # noqa: BLE001
        return _TableWorkerResult(
            table_name=name, status="FAILED", shard_index=args.shard_index,
            # safe_error_message, not f"{exc}": drop the psycopg DETAIL/"Failing
            # row contains (...)" row-value line before it reaches the durable
            # result and, via list_failed_full_load_tables, the AI DBA (Property 7).
            error_message=safe_error_message(exc),
            error_code=_error_code(exc),
        )


# --- Container memory-pressure sampling (OOM diagnostics) -------------------
# Paths for the container's memory cgroup (v2 first, then v1). On ECS Fargate the
# task runs in a memory cgroup whose limit == the task's Memory -- the hard limit the
# kernel OOM-kills on. Reading it from the PARENT (where the drain thread runs) sees
# the WHOLE-cgroup total (every worker process), i.e. the exact number a kill trips.
_CGROUP_V2_CURRENT = "/sys/fs/cgroup/memory.current"
_CGROUP_V2_MAX = "/sys/fs/cgroup/memory.max"
_CGROUP_V1_CURRENT = "/sys/fs/cgroup/memory/memory.usage_in_bytes"
_CGROUP_V1_MAX = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
# A v1 "unlimited" limit is a huge page-aligned sentinel; treat anything this large as
# "no enforced limit" so we never compute a meaningless ~0% utilization.
_CGROUP_NO_LIMIT = 1 << 62
_MEM_SAMPLE_INTERVAL_SECONDS = 5.0        # sample at most this often (drain wakes ~3x/s)
_MEM_WARN_FRACTION = 0.80                 # WARNING once usage crosses this of the limit
_MEM_WARN_REARM_FRACTION = 0.70           # re-arm the WARNING after usage drops below this
_MEM_HIGHWATER_STEP_BYTES = 50 * 1024 * 1024  # only INFO-log a new high-water this much higher


def _read_int_file(path: str) -> Optional[int]:
    """Read a single integer from ``path``; ``None`` if absent/unreadable (dev/macOS)."""
    try:
        with open(path) as handle:
            return int(handle.read().strip())
    except Exception:  # noqa: BLE001 - file missing (non-Linux) or unreadable
        return None


def _read_cgroup_memory() -> Optional[tuple[int, Optional[int]]]:
    """Return ``(used_bytes, limit_bytes|None)`` for the container's memory cgroup.

    Reads cgroup v2 first, then v1. ``limit_bytes`` is ``None`` when there is no enforced
    limit (v2 ``"max"`` or a v1 huge sentinel). Returns ``None`` when no cgroup memory
    file is readable (e.g. macOS/dev), so the sampler simply no-ops off-Fargate. No new
    dependency -- just ``/sys`` reads.
    """
    used = _read_int_file(_CGROUP_V2_CURRENT)
    if used is not None:
        try:
            with open(_CGROUP_V2_MAX) as handle:
                text = handle.read().strip()
            raw = None if text == "max" else int(text)
        except Exception:  # noqa: BLE001
            raw = None
        limit = raw if (raw is not None and raw < _CGROUP_NO_LIMIT) else None
        return used, limit
    used = _read_int_file(_CGROUP_V1_CURRENT)
    if used is not None:
        raw = _read_int_file(_CGROUP_V1_MAX)
        limit = raw if (raw is not None and raw < _CGROUP_NO_LIMIT) else None
        return used, limit
    return None


class _MemoryPressureLogger:
    """Sample the container's whole-cgroup memory in the drain thread and log it.

    Why this exists: a Full Load's memory can climb toward the Fargate task limit (the
    read-ahead prefetch queue + in-flight write batches per worker, amplified by wide /
    oversized-LOB rows), and when it crosses the hard limit the kernel OOM-kills the task
    with NO app log -- the operator saw only a CloudWatch metric spike and an ELB
    "Request timed out". This leaves a trail instead: an INFO line at each new memory
    high-water (so the peak is always in the log) and a WARNING when usage crosses ~80%
    of the limit, tagging the tables currently loading, with the actionable remedies.

    Off-Fargate (no cgroup memory file) it is a silent no-op. Read-only ``/sys`` reads,
    no new dependency; self-throttled so a busy progress queue never spams the log.
    """

    def __init__(self, handle: JobHandle) -> None:
        self._handle = handle
        self._enabled = _read_cgroup_memory() is not None
        self._next_sample = 0.0
        self._high_water = 0
        self._warned = False

    def sample(self) -> None:
        """Read the cgroup usage once (throttled) and log a high-water / pressure line."""
        if not self._enabled:
            return
        now = _time.monotonic()
        if now < self._next_sample:
            return
        self._next_sample = now + _MEM_SAMPLE_INTERVAL_SECONDS
        reading = _read_cgroup_memory()
        if reading is None:
            return
        used, limit = reading
        used_mib = used / (1024 * 1024)
        # New high-water -> INFO, so the run's peak memory is always captured in the log.
        if used >= self._high_water + _MEM_HIGHWATER_STEP_BYTES:
            self._high_water = used
            pct = f" ({used / limit:.0%} of task limit)" if limit else ""
            _LOGGER.info(
                "Full Load memory high-water: %.0f MiB%s%s",
                used_mib, pct, self._loading_suffix(),
            )
        if not limit:
            return
        frac = used / limit
        # Crossed ~80% of the hard limit -> WARNING once (re-armed only after it recedes,
        # so a run hovering near the threshold does not flap the log).
        if not self._warned and frac >= _MEM_WARN_FRACTION:
            self._warned = True
            suffix = self._loading_suffix()
            _LOGGER.warning(
                "Full Load memory pressure: %.0f MiB (%.0f%% of the %.0f MiB task "
                "limit)%s -- approaching the Fargate hard limit; an OOM kill would stop "
                "the task with no further log. Reduce full_load_table_parallelism / "
                "full_load_batch_parallelism, exclude oversized-LOB columns, or redeploy "
                "the task with more memory.",
                used_mib, frac * 100, limit / (1024 * 1024), suffix,
            )
            # Also record it on the DURABLE activity log so it surfaces in the UI's
            # activity timeline / downloadable report and SURVIVES the task -- an OOM
            # kill (the very risk this warns about) tears down the app and its logs, so a
            # note only in the CloudWatch worker log or the in-memory job is easily lost.
            # No row values (Property 7). INFO because the run has NOT failed yet; the
            # detail carries the severity (ActivityStatus has no WARNING tier).
            log_activity(
                ActivityCategory.FULL_LOAD,
                "memory pressure",
                status=ActivityStatus.INFO,
                detail=(
                    f"container memory reached {frac * 100:.0f}% of the "
                    f"{limit / (1024 * 1024):.0f} MiB task limit ({used_mib:.0f} MiB)"
                    f"{suffix} — approaching the Fargate hard limit, beyond which an OOM "
                    "kill stops the task with no further log. Lower "
                    "full_load_table_parallelism / full_load_batch_parallelism, exclude "
                    "oversized-LOB columns, or redeploy the task with more memory."
                ),
            )
        elif self._warned and frac < _MEM_WARN_REARM_FRACTION:
            self._warned = False

    def _loading_suffix(self) -> str:
        """`` while loading: t1, t2`` for the tables currently IN_PROGRESS, or ``""``.

        Read under the manager lock via ``handle.update`` (the only thread-safe path to
        the live job), so the memory line names the likely culprit table(s).
        """
        names: list[str] = []
        try:
            self._handle.update(
                lambda job: names.extend(
                    chunk.chunk_id
                    for chunk in job.chunks
                    if chunk.status == "IN_PROGRESS"
                )
            )
        except Exception:  # noqa: BLE001 - sampling must never break the drain thread
            return ""
        return f" while loading: {', '.join(sorted(names))}" if names else ""


def _drain_progress_queue(
    progress_queue: "multiprocessing.Queue[object]",
    handle: JobHandle,
    cancel_event: "multiprocessing.synchronize.Event",
    stop_event: threading.Event,
) -> None:
    """Drain thread: reads progress from worker processes → handle.update().

    Also mirrors handle.cancelled → cancel_event so workers stop cooperatively, and
    samples the container's memory each wake so a run approaching the Fargate limit
    leaves a diagnostic trail before an OOM kill (which otherwise leaves no app log).
    Runs until ``stop_event`` is set AND the queue is empty.
    """
    mem_logger = _MemoryPressureLogger(handle)
    while not stop_event.is_set():
        mem_logger.sample()
        # Mirror cancellation into the multiprocessing Event.
        if handle.cancelled and not cancel_event.is_set():
            cancel_event.set()
        try:
            msg = progress_queue.get(timeout=0.3)
        except Exception:  # noqa: BLE001 - queue.Empty or other
            continue
        if msg is _PROGRESS_SENTINEL:
            break
        if isinstance(msg, tuple) and len(msg) == 2 and msg[0] == _CHUNK_STARTED:
            # A worker actually began its table -> mark IN_PROGRESS now (not at submission).
            # (== not is: the marker is pickled across the process boundary.)
            handle.update(lambda job, n=msg[1]: _start_chunk_if_pending(job, n))
            continue
        if isinstance(msg, tuple) and len(msg) == 3 and msg[0] == _CHUNK_THROTTLED:
            # Source-load governor paused/resumed a reader -> update the caption hint.
            handle.update(
                lambda job, n=msg[1], p=msg[2]: _set_table_throttled(job, n, p)
            )
            continue
        table_name, delta_loaded, delta_skipped = msg
        handle.update(
            lambda job, n=table_name, dl=delta_loaded, ds=delta_skipped: (
                _start_then_advance(job, n, dl, ds)
            )
        )
    # Final drain (anything left after stop).
    while True:
        try:
            msg = progress_queue.get_nowait()
        except Exception:  # noqa: BLE001
            break
        if msg is _PROGRESS_SENTINEL or msg is None:
            break
        if isinstance(msg, tuple) and len(msg) == 2 and msg[0] == _CHUNK_STARTED:
            handle.update(lambda job, n=msg[1]: _start_chunk_if_pending(job, n))
            continue
        if isinstance(msg, tuple) and len(msg) == 3 and msg[0] == _CHUNK_THROTTLED:
            handle.update(
                lambda job, n=msg[1], p=msg[2]: _set_table_throttled(job, n, p)
            )
            continue
        table_name, delta_loaded, delta_skipped = msg
        handle.update(
            lambda job, n=table_name, dl=delta_loaded, ds=delta_skipped: (
                _start_then_advance(job, n, dl, ds)
            )
        )


def _log_quarantined_row(name: str, primary_key: object, message: object,
                         error_code: object = None) -> None:
    """Record one permanently-dropped row on the DURABLE activity log.

    The per-row error log is in-memory, so after an app restart nothing said WHICH rows
    were lost -- only a count. A permanently dropped row is precisely what an audit trail
    is for: it is the one outcome the migration cannot recover by itself, and the operator
    needs the primary key to fix the source value. PK + reason only, never row values
    (Property 7).

    Shared by all three load paths (in-process, sharded worker, single-table worker) so a
    sharded table -- the large ones, least likely to be checked by hand -- cannot silently
    skip the audit entry.
    """
    log_activity(
        ActivityCategory.FULL_LOAD,
        "row quarantined",
        status=ActivityStatus.FAILURE,
        target=name,
        error_code=error_code,
        detail=(
            f"row pk[{primary_key}] PERMANENTLY DROPPED: {message}. The rest of the "
            "table loaded; fix the source value and reload this table to close the gap."
        ),
    )


def _record_index_failures(
    error_log: ErrorLogStore, job_id: str, table_name: str, failures
) -> None:
    """Log post-load index-creation failures for a table that DID load its data.

    Shared by the single-process and multiprocess paths so a missing index is
    reported identically either way. Deliberately does NOT fail the table: every row
    is present, so only an access path is absent -- failing here used to mark a
    fully-loaded table FAILED and block the Validation gate on data with nothing
    missing. Logged as INFO (not FAILURE) for the same reason.
    """
    entries = tuple(failures or ())
    if not entries:
        return
    for failure in entries:
        error_log.record(
            job_id,
            DataErrorRecord(
                table=table_name,
                chunk_id=table_name,
                error_code=None,
                message=(
                    f"index not created: {failure} — the table's DATA loaded "
                    "completely; add the index later or reduce the table's index "
                    "count (Aurora DSQL allows 24 per table, including the "
                    "primary key)"
                ),
                occurred_at=datetime.now(timezone.utc),
            ),
        )
    log_activity(
        ActivityCategory.FULL_LOAD,
        "load table",
        status=ActivityStatus.INFO,
        target=table_name,
        detail=(
            f"{len(entries)} index(es) could not be created; the table's data "
            "loaded completely (see the error log)"
        ),
    )


def _migrate_table_with_source_retry(
    migrator: "DataMigrator",
    table: TableDef,
    *,
    on_rows: Callable[..., None],
    handle: JobHandle,
) -> object:
    """Load ``table``, RE-READING it from a fresh snapshot if the source drops.

    An Aurora MySQL failover (writer promotion during patching, an instance
    replacement, an AZ event) closes every open connection, so a multi-hour Full
    Load will meet one. Without this, the table in flight simply failed and waited
    for a human to press Re-run.

    The retry deliberately re-reads the table FROM THE START on a new connection
    instead of resuming the dead read at its last primary key. Resuming would splice
    two different MySQL snapshots into one table (the pre-failover rows plus rows as
    of a minute later), leaving it consistent as of no single point in time -- and
    the gapless Full Load -> CDC handoff depends on each table being consistent as
    of the run's watermark, so a spliced table could leave a change the stream
    replays against rows that already moved past it. Re-reading keeps that
    guarantee; the idempotent load skips the rows already written, so the only cost
    is re-read I/O (and reader sharding shrinks even that, since each shard already
    holds its own snapshot).

    Only CONNECTION-level failures retry (:func:`is_source_transient_error`); a data
    or schema error would fail identically forever, so it propagates at once. A
    cooperative stop is honored between attempts and never retried.
    """
    config = load_config()
    attempts = max(1, int(config.full_load_source_retry_attempts))
    backoff = max(0.0, float(config.full_load_source_retry_backoff_seconds))
    name = table.name

    for attempt in range(1, attempts + 1):
        try:
            return migrator.migrate_table(
                table, on_rows=on_rows, should_cancel=lambda: handle.cancelled
            )
        except _FullLoadStopped:
            raise  # a user stop is not a failure to retry
        except Exception as exc:  # noqa: BLE001 - classify transient vs permanent
            # A fake migrator (tests) may not expose source_type; default to MySQL,
            # matching the doubles' simulated source.
            source_type = getattr(migrator, "source_type", SourceType.MYSQL)
            if attempt >= attempts or not is_source_transient_error(exc, source_type):
                raise
            if handle.cancelled:
                raise
            delay = backoff * (2 ** (attempt - 1))
            _LOGGER.warning(
                "Full Load source connection lost for table %s (attempt %d/%d): "
                "%s: %s -- re-reading from a fresh snapshot in %.0fs",
                name, attempt, attempts, type(exc).__name__, exc, delay,
            )
            log_activity(
                ActivityCategory.FULL_LOAD,
                "load table",
                status=ActivityStatus.INFO,
                target=name,
                detail=(
                    f"source connection lost (attempt {attempt}/{attempts}) — "
                    f"likely an Aurora failover; re-reading this table from a fresh "
                    f"snapshot in {delay:.0f}s. Already-written rows are skipped "
                    f"(idempotent), so no duplicates."
                ),
            )
            # Wait for the promoted writer to take over (DNS re-points within ~30-60s
            # on Aurora); reconnecting instantly would just fail again. Sleep in short
            # slices so a user Stop is honored promptly instead of after the full wait.
            waited = 0.0
            while waited < delay:
                if handle.cancelled:
                    raise
                _time.sleep(min(1.0, delay - waited))
                waited += 1.0
    # Unreachable: the loop either returns or raises.
    raise AssertionError("source retry loop exited without a result")


def _migrate_one_table(
    handle: JobHandle,
    job_id: str,
    table: TableDef,
    migrator: "DataMigrator",
    error_log: ErrorLogStore,
) -> _TableLoadOutcome:
    """Migrate a single table, recording per-table progress/failure on the job.

    Marks the table ``IN_PROGRESS``, migrates it while advancing a live
    cumulative row count as batches land, then ``DONE`` (with the exact rows) or
    ``FAILED`` (recording a credential-free error to ``error_log`` and logging
    the cause). A cooperative cancel is honored: a table not yet started is left
    untouched, and one stopped mid-load is marked ``FAILED`` (for retry) without
    recording a data error. A per-table failure is isolated so other tables
    continue. The job updates go through ``handle`` under the manager lock, so
    this is safe to call concurrently from multiple table workers.

    Returns ``True`` when the table did NOT fully load -- either it ended in a
    real load failure, OR it loaded but quarantined (permanently dropped) one or
    more rows (e.g. a value over DSQL's ~1 MiB per-value limit). Both leave the
    target with missing rows, so the caller reports the run as incomplete (the
    Validation gate then holds). Returns ``False`` on a clean full load or a
    cooperative stop (handled via cancellation, not as a data error). A
    quarantining table's chunk is still ``DONE`` with the rows that did load, so a
    re-run after fixing the source value idempotently reloads only the gap.
    """
    name = table.name
    # Honor a stop requested before this table started: leave it PENDING so the
    # post-run cleanup marks it FAILED for retry (never half-start a table).
    if handle.cancelled:
        return _TableLoadOutcome.STOPPED
    handle.update(lambda job, n=name: _start_chunk(job, n))

    # Coalesce frequent per-batch callbacks into occasional job updates so a
    # large table advances visibly without persisting on every batch. The
    # accumulators are touched only on the single draining thread of this table's
    # import, so they need no lock. ``pending_skipped`` is tracked separately so a
    # re-load that mostly SKIPS already-present rows still advances the live
    # progress (rows-present) instead of looking stuck at zero, while the per-table
    # "newly loaded" count stays distinct from the "already present" count.
    pending = [0]
    pending_skipped = [0]

    def _on_rows(loaded: int, skipped: int = 0) -> None:
        pending[0] += loaded
        pending_skipped[0] += skipped
        if pending[0] + pending_skipped[0] >= PROGRESS_FLUSH_ROWS:
            flush_loaded, pending[0] = pending[0], 0
            flush_skipped, pending_skipped[0] = pending_skipped[0], 0
            handle.update(
                lambda job, n=name, dl=flush_loaded, ds=flush_skipped: (
                    _advance_chunk_rows(job, n, dl, ds)
                )
            )

    try:
        outcome = _as_load_result(
            _migrate_table_with_source_retry(
                migrator, table, on_rows=_on_rows, handle=handle
            )
        )
    except _FullLoadStopped:
        # Stopped mid-table by the user: incomplete, so mark it FAILED for retry
        # (the re-load is idempotent). Not a data error -- nothing is logged.
        _LOGGER.info("Full Load stopped for table %s (will be retryable)", name)
        log_activity(
            ActivityCategory.FULL_LOAD,
            "load table",
            status=ActivityStatus.INFO,
            target=name,
            detail="stopped by user (retryable)",
        )
        handle.update(lambda job, n=name: _fail_chunk(job, n))
        return _TableLoadOutcome.STOPPED
    except Exception as exc:  # noqa: BLE001 - recorded as a per-table failure
        # safe_error_message, not f"{exc}": this text is written to the durable
        # error log + activity log (+ CloudWatch). A raw psycopg exception keeps its
        # DETAIL/"Failing row contains (...)" line, which carries the row's column
        # values -- a Property 7 leak. First-line-only drops the values while keeping
        # the actionable primary message. (A RuntimeError from the batch loop already
        # carries a pre-sanitized first_error; this also covers a raw driver
        # exception raised outside that loop, e.g. a DDL/connection failure.)
        message = safe_error_message(exc)
        # A dropped SOURCE connection (Aurora failover) is an EXPECTED event on a
        # multi-hour load, and the raw driver text ("(2013, 'Lost connection to MySQL
        # server during query')") tells the operator nothing about what to do. Append
        # the what-happened/what-next explanation so the error log, the activity log,
        # and the inline per-table message all explain it the same way. Only added
        # when the retries above were exhausted -- a recovered failover never gets here.
        # Prefer the source-side hint (dropped connection, too-many-connections); fall
        # back to the DSQL target-side hint (OCC exhaustion, per-table limit, constraint
        # / data rejection) so a target failure also explains what to do next, not just
        # the bare driver text.
        hint = (
            source_error_hint(
                exc, getattr(migrator, "source_type", SourceType.MYSQL)
            )
            or target_error_hint(exc)
        )
        if hint:
            message = f"{message} — {hint}"
        _LOGGER.warning("Full Load failed for table %s: %s", name, message)
        code = _error_code(exc)
        error_log.record(
            job_id,
            DataErrorRecord(
                table=name,
                chunk_id=name,
                error_code=code,
                message=message,
                occurred_at=datetime.now(timezone.utc),
            ),
        )
        log_activity(
            ActivityCategory.FULL_LOAD,
            "load table",
            status=ActivityStatus.FAILURE,
            target=name,
            error_code=code,
            detail=message,
            exc=exc,
        )
        handle.update(lambda job, n=name: _fail_chunk(job, n))
        return _TableLoadOutcome.FAILED
    else:
        # Surface any quarantined poison rows to the single error log (PK + reason
        # only) so an isolated bad row is visible, never a silent loss.
        for record in getattr(outcome, "quarantine_records", ()) or ():
            error_log.record(
                job_id,
                DataErrorRecord(
                    table=name,
                    chunk_id=name,
                    error_code=getattr(record, "error_code", None),
                    message=(
                        f"quarantined row pk[{record.primary_key}]: {record.message}"
                    ),
                    occurred_at=datetime.now(timezone.utc),
                ),
            )
            _log_quarantined_row(
                name, record.primary_key, record.message,
                getattr(record, "error_code", None),
            )
        # Indexes that could not be created. Recorded so the operator sees WHICH
        # index is missing, but deliberately NOT treated as a table failure: every
        # row loaded, so the table is complete -- only an access path is absent.
        # (Failing here used to mark a fully-loaded table FAILED and block the
        # Validation gate on data that had nothing missing.)
        _record_index_failures(
            error_log, job_id, name, getattr(outcome, "index_failures", ())
        )
        skipped_note = (
            f", {outcome.rows_skipped:,} already on target (skipped)"
            if outcome.rows_skipped
            else ""
        )
        quarantined = getattr(outcome, "rows_quarantined", 0) or 0
        quarantine_note = f", {quarantined:,} quarantined" if quarantined else ""
        # Surface any oversized-LOB column excluded from THIS table's load in the
        # per-table detail too (the run-level "column excluded" events are the audit
        # record; this is the at-a-glance echo on the table's own line).
        excluded_note = _lob_excluded_note(migrator, name)
        # Quarantined rows are permanently DROPPED from the target (e.g. a value
        # over DSQL's ~1 MiB per-value limit, or another non-retryable row error),
        # so the table did NOT fully load. Report it as a FAILURE -- not SUCCESS --
        # so the run-level verdict is loud (a partial table must never read as
        # "loaded"); the rows that did load are still committed and the chunk is
        # completed (idempotent), so a re-run after fixing the source value reloads
        # only the gap. ``_migrate_one_table`` returns True for a quarantining table
        # so the run raises FullLoadIncompleteError and the Validation gate holds.
        had_quarantine = quarantined > 0
        log_activity(
            ActivityCategory.FULL_LOAD,
            "load table",
            status=ActivityStatus.FAILURE if had_quarantine else ActivityStatus.SUCCESS,
            target=name,
            detail=(
                f"{outcome.rows_loaded:,} rows newly loaded"
                f"{skipped_note}{quarantine_note}{excluded_note}"
                + (
                    " -- quarantined rows were DROPPED (e.g. a value over DSQL's "
                    "~1 MiB per-value limit); see the error log and re-run after "
                    "fixing the source value"
                    if had_quarantine
                    else ""
                )
            ),
        )
        handle.update(
            lambda job, n=name, r=outcome.rows_loaded, s=outcome.rows_skipped,
            q=quarantined: (
                _complete_chunk(job, n, r, s, q)
            )
        )
        # A table that dropped rows is reported as an incomplete load even though
        # its chunk is DONE: the run-end check then raises (unless quarantine is
        # explicitly accepted) so the job/step is FAILED, never a silent success
        # with missing rows.
        return (
            _TableLoadOutcome.QUARANTINED
            if had_quarantine
            else _TableLoadOutcome.LOADED
        )


def _open_shared_snapshot(migrator, dialect):
    """Open an anchor REPEATABLE READ transaction on the source and export its snapshot id.

    Returns ``(engine, connection, snapshot_id)``. The transaction is held OPEN (via the
    returned engine+connection) for the whole sharded load so every shard worker can import
    the same point-in-time cut (``dialect.set_transaction_snapshot_sql``) -- a consistent
    range-sharded read even for a REPLACE (no-CDC) load. Caller MUST call
    :func:`_close_shared_snapshot` when the load finishes. PostgreSQL only.
    """
    from sqlalchemy import text

    from dsql_migrator.core.exporter import _TXN_CONTROL_EXEC_OPTS

    engine = migrator._exporter._engine_factory(migrator._inputs.source_config)
    conn = None
    try:
        conn = engine.connect()
        anchor = conn.execution_options(isolation_level="AUTOCOMMIT")
        anchor.execute(
            text(dialect.snapshot_start_sql), execution_options=_TXN_CONTROL_EXEC_OPTS
        )
        snapshot_id = anchor.execute(text(dialect.export_snapshot_sql())).scalar()
        if not snapshot_id:
            raise RuntimeError("source returned an empty exported-snapshot id")
        return engine, conn, snapshot_id
    except Exception:
        # Close the checked-out connection (else its open REPEATABLE READ txn lingers until
        # GC) BEFORE disposing the engine -- engine.dispose() does not close a leased conn.
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - best-effort on the failure path
                pass
        engine.dispose()
        raise


def _close_shared_snapshot(engine, conn) -> None:
    """COMMIT + close the anchor snapshot transaction opened by :func:`_open_shared_snapshot`."""
    if conn is None:
        return
    from sqlalchemy import text

    from dsql_migrator.core.exporter import _TXN_CONTROL_EXEC_OPTS, COMMIT

    try:
        conn.execution_options(isolation_level="AUTOCOMMIT").execute(
            text(COMMIT), execution_options=_TXN_CONTROL_EXEC_OPTS
        )
    except Exception:  # noqa: BLE001 - best-effort teardown of a read-only anchor txn
        pass
    finally:
        try:
            conn.close()
        finally:
            if engine is not None:
                engine.dispose()


def _migrate_tables_in_parallel(
    handle: JobHandle,
    job_id: str,
    tables: Sequence[TableDef],
    migrator: "DataMigrator",
    error_log: ErrorLogStore,
) -> _RunCounts:
    """Migrate ``tables`` concurrently (bounded), isolating per-table failures.

    Tables load in parallel up to :data:`FULL_LOAD_TABLE_PARALLELISM` workers for
    throughput; each worker calls :func:`_migrate_one_table`, which catches its
    own per-table errors, so one table's failure never aborts the others. If the
    job is cancelled, in-flight tables stop mid-load and not-yet-started ones are
    skipped; any remaining non-``DONE`` chunk is then marked ``FAILED`` so the
    "Retry failed tables" path resumes exactly the unfinished work.

    Returns fresh per-run :class:`_RunCounts` (``real_failed`` tables that errored
    vs ``quarantined`` tables that dropped rows), so the caller can block on a
    retryable failure yet offer an accept-and-continue override for a
    quarantine-only incompleteness (a cooperative stop counts as neither -- it is
    surfaced as ``CANCELLED``).
    """
    if not tables:
        return _RunCounts(0, 0)
    real_failed = 0
    quarantined = 0

    def _tally(outcome: _TableLoadOutcome) -> None:
        nonlocal real_failed, quarantined
        if outcome is _TableLoadOutcome.FAILED:
            real_failed += 1
        elif outcome is _TableLoadOutcome.QUARANTINED:
            quarantined += 1

    # Operator-tunable table-level parallelism (default FULL_LOAD_TABLE_PARALLELISM),
    # bounded by the table count.
    cfg = load_config()
    table_parallelism = cfg.full_load_table_parallelism

    # Process-parallel path: when the migrator is a BatchedTableMigrator (the
    # production path), use ProcessPoolExecutor so each table gets its own GIL
    # and its own CPU core. Fakes/test doubles fall through to ThreadPool.
    use_processes = isinstance(migrator, BatchedTableMigrator) and table_parallelism > 1

    if not use_processes:
        # Thread fallback (test doubles, or table_parallelism<=1).
        workers = min(table_parallelism, len(tables))
        if workers <= 1:
            for table in tables:
                _tally(_migrate_one_table(handle, job_id, table, migrator, error_log))
        else:
            with ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="fullload-table"
            ) as pool:
                futures = [
                    pool.submit(
                        _migrate_one_table, handle, job_id, table, migrator, error_log
                    )
                    for table in tables
                ]
                for future in as_completed(futures):
                    _tally(future.result())
    else:
        # Unified process-parallel path: small tables get 1 worker each, large
        # shardable tables get multiple shard workers. All submitted to ONE pool.
        from dsql_migrator.core.exporter import shardable_leading_int_pk

        # Plan work units: for each table, decide if it should be sharded.
        # Small/non-shardable tables get 1 worker each; an eligible large table gets
        # K shard workers.
        #
        # SHARDING SAFETY -- must mirror the single-process migrate_table invariant
        # (see the "NOT sharded on the REPLACE path" block below), or the production
        # multiprocess path silently reintroduces a torn read the single-process path
        # forbids. Each shard opens its OWN independently-timed CONSISTENT SNAPSHOT, so
        # a source written to DURING the load can land a cross-shard torn read (one row
        # of a multi-row source txn in shard A's snapshot, its sibling not yet in shard
        # B's). That is only provably safe when the load is idempotent AND a CDC stream
        # will reconcile post-snapshot writes -- i.e. cdc_coexisting. A REPLACE (clean
        # plain-INSERT, no CDC) or a non-CDC append has nothing to reconcile it, so such
        # a table must be read by a SINGLE reader (one snapshot = one point-in-time cut).
        # Resolve the SOURCE dialect so the shardability pre-gate uses the right
        # integer-PK set (the authoritative plan_pk_shard_ranges below already does);
        # without it this pre-gate ran under the module-default MySQL dialect.
        _shard_dialect = dialect_for(migrator._inputs.source_config.source_type)
        # Sharding a table's read is consistent when EITHER a CDC stream will reconcile the
        # shards' independently-timed snapshots (cdc_coexisting) OR the source dialect can
        # give every shard ONE shared point-in-time snapshot (supports_shared_snapshot --
        # PostgreSQL exported snapshots). The latter makes even a REPLACE (no-CDC) load shard
        # SAFELY on a live source, so it lifts the "single-reader for REPLACE" restriction.
        _shared_snapshot = _shard_dialect.supports_shared_snapshot
        _shardable_ok = bool(migrator._inputs.cdc_coexisting) or _shared_snapshot

        # Shard count is capped exactly like single-process: cfg.full_load_reader_shards
        # clamped so table_parallelism x shards stays under the source max_connections
        # ceiling. NOT the pool budget (remaining_slots) -- that ignored the off-switch
        # (reader_shards=1) and the source-connection guardrail.
        tp = max(1, cfg.full_load_table_parallelism)
        effective_reader_shards = max(
            1, min(cfg.full_load_reader_shards, _MAX_SOURCE_READERS // tp)
        )

        work_units: list[tuple] = []  # ("table", table) or ("shard", table, lo, hi, idx)
        for table in tables:
            table_is_replace = (
                not migrator._inputs.cdc_coexisting
                and table.name in migrator._inputs.replace_tables
            )
            shardable = (
                _shardable_ok
                and (not table_is_replace or _shared_snapshot)
                and shardable_leading_int_pk(table, _shard_dialect) is not None
                and effective_reader_shards > 1
            )
            if not shardable:
                work_units.append(("table", table))
                continue
            shard_ranges = migrator._exporter.plan_pk_shard_ranges(
                migrator._inputs.source_config,
                table,
                effective_reader_shards,
                min_rows=cfg.full_load_shard_min_rows,
            )
            if len(shard_ranges) > 1:
                for i, (lo, hi) in enumerate(shard_ranges):
                    work_units.append(("shard", table, lo, hi, i))
            else:
                work_units.append(("table", table))

        total_workers = min(table_parallelism, len(work_units))

        # Pre-pass: DROP+recreate EVERY replace table BEFORE spawning workers, and do
        # it SERIALLY. DSQL runs one DDL per transaction under optimistic concurrency;
        # if N table workers each recreated their own table at once on startup they
        # contend on the shared schema catalog and raise OC001 (40001, "schema has
        # been updated by another transaction"), which can exhaust the DDL retry
        # budget and fail a table before a single row loads. The DROP+CREATE is
        # metadata-only, so doing it here (sequential, ~one connect per table) removes
        # that startup DDL storm at the source. Workers then load into the already-
        # empty target WITHOUT re-running the DDL (they derive the same post-load
        # secondary-index DDLs from the applied conversion) -- the same way sharded
        # tables have always been pre-recreated here.
        table_recreator = migrator._table_recreator
        recreated_names: set[str] = set()
        # A REPLACE table recreated here is EMPTY with NO secondary indexes yet
        # (recreate_table builds only the base table; indexes are a post-load pass). The
        # single-process / unsharded paths run that pass inside import_rows, but the
        # multiprocess SHARDED path loads PK slices across separate processes with NO
        # index_ddls, so no import_rows call owns the whole table to index it. Capture each
        # recreated table's secondary-index DDLs here and build them ONCE in the parent after
        # all of that table's shards succeed (see the shard-success branch below). Without
        # this, a sharded REPLACE load silently created NO secondary indexes.
        replace_index_ddls: dict[str, list[str]] = {}
        for wu in work_units:
            tbl = wu[1]
            if tbl.name in recreated_names:
                continue
            is_replace = (
                not migrator._inputs.cdc_coexisting
                and tbl.name in migrator._inputs.replace_tables
            )
            if is_replace:
                replace_index_ddls[tbl.name] = table_recreator(tbl)
                recreated_names.add(tbl.name)

        ctx = multiprocessing.get_context("spawn")
        progress_queue: multiprocessing.Queue = ctx.Queue()
        cancel_event = ctx.Event()
        stop_drain = threading.Event()
        drain = threading.Thread(
            target=_drain_progress_queue,
            args=(progress_queue, handle, cancel_event, stop_drain),
            daemon=True, name="fullload-progress-drain",
        )
        drain.start()

        # Track shard results per table for aggregation.
        shard_results: dict[str, list[_TableWorkerResult]] = {}

        # For a shared-snapshot dialect (PostgreSQL) with sharded work, open ONE anchor
        # snapshot in the parent and export its id; every shard imports it so the whole
        # sharded read is a single consistent point-in-time cut. None for MySQL / no shards
        # -> shards use own snapshots. Opened INSIDE the try so a build failure still runs the
        # finally (drain-thread + queue teardown, _close_shared_snapshot).
        _anchor_engine = _anchor_conn = None
        shared_snapshot_id: Optional[str] = None

        try:
            if _shared_snapshot and any(wu[0] == "shard" for wu in work_units):
                _anchor_engine, _anchor_conn, shared_snapshot_id = _open_shared_snapshot(
                    migrator, _shard_dialect
                )
            with ProcessPoolExecutor(
                max_workers=total_workers,
                mp_context=ctx,
                initializer=_init_worker,
                initargs=(progress_queue, cancel_event),
            ) as pool:
                futures: dict = {}  # future -> ("table", name) or ("shard", name, idx)
                submission_idx = 0
                for wu in work_units:
                    if handle.cancelled:
                        break
                    if submission_idx > 0:
                        _time.sleep(_PROCESS_LAUNCH_STAGGER_SECONDS)
                    submission_idx += 1

                    if wu[0] == "table":
                        table = wu[1]
                        # The chunk is marked IN_PROGRESS when the WORKER signals it actually
                        # started (see _CHUNK_STARTED), not here at submission -- a bounded
                        # pool runs only a few tables at once, so marking every submitted
                        # table in-progress inflated the "in progress" count and per-table ETA.
                        args = _TableWorkerArgs(
                            job_id=job_id, table=table,
                            inputs=_slim_worker_inputs(migrator._inputs, table.name),
                            pre_recreated=table.name in recreated_names,
                        )
                        f = pool.submit(_migrate_one_table_in_process, args)
                        futures[f] = ("table", table.name)
                    else:  # "shard"
                        table, lo, hi, shard_idx = wu[1], wu[2], wu[3], wu[4]
                        # Start chunk only once per sharded table.
                        if table.name not in shard_results:
                            shard_results[table.name] = []
                            # Started when the first shard worker signals it began (see
                            # _CHUNK_STARTED), not here at submission.
                        shard_args = _ShardWorkerArgs(
                            job_id=job_id, table=table,
                            inputs=_slim_worker_inputs(migrator._inputs, table.name),
                            pk_lower=lo, pk_upper=hi, shard_index=shard_idx,
                            shared_snapshot_id=shared_snapshot_id,
                        )
                        f = pool.submit(_migrate_shard_in_process, shard_args)
                        futures[f] = ("shard", table.name, shard_idx)

                # Process results as they complete.
                # Bounded wait so Stop Full Load can never hang. Cancellation is
                # cooperative (a worker finishes its batch, sees cancel_event, returns),
                # but a wedged worker used to strand the parent in as_completed with no
                # timeout -- forever, while the UI showed "Stopping… finishing the
                # current batch". Waiting in slices lets us notice the grace period
                # expiring and stop waiting instead.
                _cancel_deadline: Optional[float] = None
                _pending = set(futures)
                while _pending:
                    if handle.cancelled and _cancel_deadline is None:
                        _cancel_deadline = _time.monotonic() + _CANCEL_GRACE_SECONDS
                    try:
                        done_iter = as_completed(_pending, timeout=1.0)
                        future = next(iter(done_iter))
                    except StopIteration:
                        break
                    except FuturesTimeoutError:
                        if (
                            _cancel_deadline is not None
                            and _time.monotonic() >= _cancel_deadline
                        ):
                            # Cooperative stop did not take. Abandon the wait; the
                            # `with ProcessPoolExecutor` exit terminates the workers,
                            # and the unfinished chunks are marked FAILED below (they
                            # are retryable -- the load is idempotent).
                            _LOGGER.warning(
                                "Full Load cancel: %d worker task(s) did not stop "
                                "within %.0fs; abandoning the wait and tearing down "
                                "the pool.",
                                len(_pending), _CANCEL_GRACE_SECONDS,
                            )
                            for _f in _pending:
                                _f.cancel()
                            break
                        continue
                    _pending.discard(future)
                    tag = futures[future]
                    result: _TableWorkerResult = future.result()
                    name = result.table_name

                    if tag[0] == "shard":
                        # Accumulate shard results; finalize when all shards done.
                        shard_results[name].append(result)
                        expected_shards = sum(
                            1 for wu in work_units
                            if wu[0] == "shard" and wu[1].name == name
                        )
                        if len(shard_results[name]) < expected_shards:
                            continue  # more shards pending for this table
                        # All shards for this table complete — aggregate.
                        total_loaded = sum(r.rows_loaded for r in shard_results[name])
                        total_skipped = sum(r.rows_skipped for r in shard_results[name])
                        all_quarantine = [
                            rec for r in shard_results[name]
                            for rec in r.quarantine_records
                        ]
                        any_failed = any(r.status == "FAILED" for r in shard_results[name])
                        any_stopped = any(r.status == "STOPPED" for r in shard_results[name])
                        if any_failed:
                            # Record EVERY shard's outcome (status + rows + message),
                            # not just failed shards that carried a message. A shard
                            # that failed without a message was previously invisible,
                            # leaving "one or more shards failed" with no diagnosable
                            # cause; logging each shard's status/rows makes the partial
                            # state and the failing shard(s) explicit.
                            for r in shard_results[name]:
                                if r.status == "FAILED":
                                    error_log.record(job_id, DataErrorRecord(
                                        table=name,
                                        chunk_id=f"{name} shard {r.shard_index}",
                                        error_code=r.error_code,
                                        message=(
                                            f"shard {r.shard_index} FAILED "
                                            f"(rows_loaded={r.rows_loaded}): "
                                            f"{r.error_message or '(no error message)'}"
                                        ),
                                        occurred_at=datetime.now(timezone.utc),
                                    ))
                            log_activity(ActivityCategory.FULL_LOAD, "load table",
                                status=ActivityStatus.FAILURE, target=name,
                                detail=f"one or more shards failed ({total_loaded:,} rows loaded)")
                            handle.update(lambda job, n=name: _fail_chunk(job, n))
                            _tally(_TableLoadOutcome.FAILED)
                        elif any_stopped:
                            handle.update(lambda job, n=name: _fail_chunk(job, n))
                            _tally(_TableLoadOutcome.STOPPED)
                        else:
                            for rec in all_quarantine:
                                error_log.record(job_id, DataErrorRecord(
                                    table=name, chunk_id=name,
                                    error_code=rec.get("error_code"),
                                    message=f"quarantined row pk[{rec.get('primary_key')}]: {rec.get('message')}",
                                    occurred_at=datetime.now(timezone.utc),
                                ))
                                _log_quarantined_row(
                                    name, rec.get("primary_key"), rec.get("message"),
                                    rec.get("error_code"),
                                )
                            _record_index_failures(
                                error_log, job_id, name,
                                [f for r in shard_results[name]
                                 for f in (r.index_failures or ())],
                            )
                            had_q = len(all_quarantine) > 0
                            log_activity(ActivityCategory.FULL_LOAD, "load table",
                                status=ActivityStatus.FAILURE if had_q else ActivityStatus.SUCCESS,
                                target=name,
                                detail=f"{total_loaded:,} rows loaded across {expected_shards} shards"
                                + _lob_excluded_note(migrator, name))
                            handle.update(lambda job, n=name, r=total_loaded, s=total_skipped,
                                q=len(all_quarantine):
                                _complete_chunk(job, n, r, s, q))
                            # SUCCESS branch ONLY (no shard FAILED/STOPPED): build the
                            # recreated REPLACE table's secondary indexes ONCE now that every
                            # shard finished. The shard workers loaded slices with no
                            # index_ddls, so this is where a sharded REPLACE table gets its
                            # indexes -- via BatchedImporter.create_indexes, IDENTICAL CREATE
                            # INDEX ASYNC / OCC / isolation behavior to the non-sharded path.
                            _shard_index_ddls = replace_index_ddls.get(name)
                            if _shard_index_ddls:
                                _idx_importer = migrator._importer_factory(migrator._inputs)
                                _idx_created, _idx_failures = _idx_importer.create_indexes(
                                    _shard_index_ddls
                                )
                                _record_index_failures(
                                    error_log, job_id, name, _idx_failures
                                )
                            _tally(_TableLoadOutcome.QUARANTINED if had_q else _TableLoadOutcome.LOADED)
                    else:
                        # Single-table worker result (same as before).
                        if result.status == "DONE":
                            had_quarantine = len(result.quarantine_records) > 0
                            for rec in result.quarantine_records:
                                error_log.record(job_id, DataErrorRecord(
                                    table=name, chunk_id=name,
                                    error_code=rec.get("error_code"),
                                    message=f"quarantined row pk[{rec.get('primary_key')}]: {rec.get('message')}",
                                    occurred_at=datetime.now(timezone.utc),
                                ))
                                _log_quarantined_row(
                                    name, rec.get("primary_key"), rec.get("message"),
                                    rec.get("error_code"),
                                )
                            _record_index_failures(
                                error_log, job_id, name, result.index_failures
                            )
                            log_activity(ActivityCategory.FULL_LOAD, "load table",
                                status=ActivityStatus.FAILURE if had_quarantine else ActivityStatus.SUCCESS,
                                target=name,
                                detail=f"{result.rows_loaded:,} rows newly loaded"
                                + _lob_excluded_note(migrator, name))
                            handle.update(lambda job, n=name, r=result.rows_loaded, s=result.rows_skipped,
                                q=len(result.quarantine_records):
                                _complete_chunk(job, n, r, s, q))
                            _tally(_TableLoadOutcome.QUARANTINED if had_quarantine else _TableLoadOutcome.LOADED)
                        elif result.status == "STOPPED":
                            log_activity(ActivityCategory.FULL_LOAD, "load table",
                                status=ActivityStatus.INFO, target=name,
                                detail="stopped by user (retryable)")
                            handle.update(lambda job, n=name: _fail_chunk(job, n))
                            _tally(_TableLoadOutcome.STOPPED)
                        else:  # FAILED
                            error_log.record(job_id, DataErrorRecord(
                                table=name, chunk_id=name,
                                error_code=result.error_code,
                                message=result.error_message or "unknown error",
                                occurred_at=datetime.now(timezone.utc),
                            ))
                            log_activity(ActivityCategory.FULL_LOAD, "load table",
                                status=ActivityStatus.FAILURE, target=name,
                                error_code=result.error_code,
                                detail=result.error_message or "unknown error")
                            handle.update(lambda job, n=name: _fail_chunk(job, n))
                            _tally(_TableLoadOutcome.FAILED)
        finally:
            # Close the shared-snapshot anchor (if any) once all shard readers are done.
            _close_shared_snapshot(_anchor_engine, _anchor_conn)
            stop_drain.set()
            # Non-blocking: this runs on the CLEANUP path, so a full queue must not
            # be able to wedge it. ``stop_drain`` already tells the drain to exit, so
            # the sentinel is only a fast-path wake-up -- losing it costs at most the
            # drain's 0.3 s poll interval.
            _report_progress(progress_queue, _PROGRESS_SENTINEL)
            drain.join(timeout=5)

    if handle.cancelled:
        handle.update(_fail_unfinished_chunks)
    return _RunCounts(real_failed, quarantined)


def _log_identity_sequence_sync(
    inputs: "Optional[DataMigrationInputs]",
    table_names: "Sequence[str]",
    *,
    sync: "Optional[Callable[..., dict]]" = None,
) -> dict:
    """Sync target identity sequences and RECORD the outcome in the activity log.

    The log entry is the point: an unrepaired identity sequence only fails after
    cut-over, so "we tried and here is what happened" has to be in the durable audit
    trail rather than inferred. Tables with no identity column are omitted (the
    KEEP_INTEGER default), so a schema without any produces no noise.
    """
    if inputs is None:
        return {}
    synced = _sync_identity_sequences_after_load(inputs, table_names, sync=sync)
    _log_identity_sequence_sync_outcome(synced)
    return synced


def _log_identity_sequence_sync_outcome(synced: "Mapping[str, object]") -> None:
    """Record the identity-sequence sync result in the activity log.

    Partitions the raw result: advanced (int) tables are logged as SUCCESS; FAILED
    (str) tables are logged as FAILURE so a swallowed RESTART WITH is never silent
    (audit finding D2). ``None`` no-ops are omitted.
    """
    from dsql_migrator.core.target_introspector import partition_identity_sync

    advanced, failed = partition_identity_sync(synced)
    if advanced:
        detail = ", ".join(
            f"{name} -> RESTART WITH {value}" for name, value in sorted(advanced.items())
        )
        log_activity(
            ActivityCategory.FULL_LOAD,
            "identity sequences synced",
            status=ActivityStatus.SUCCESS,
            detail=(
                f"{len(advanced)} identity primary key(s) advanced past the loaded "
                f"rows so the application's first insert after cut-over cannot collide: "
                f"{detail}"
            ),
        )
    if failed:
        fdetail = ", ".join(
            f"{name}: {reason}" for name, reason in sorted(failed.items())
        )
        log_activity(
            ActivityCategory.FULL_LOAD,
            "identity sequence sync failed",
            status=ActivityStatus.FAILURE,
            detail=(
                f"{len(failed)} identity sequence(s) could NOT be advanced past the "
                f"loaded rows; the application's first insert after cut-over may "
                f"collide — advance them manually before cut-over: {fdetail}"
            ),
        )


def sync_identity_sequences_for_tables(
    target_config: "TargetConnectionConfig",
    table_names: "Sequence[str]",
    *,
    aws_profile: "Optional[str]" = None,
    sync: "Optional[Callable[..., dict]]" = None,
) -> dict:
    """Sync target identity sequences off the CURRENT ``MAX(pk)`` and log the outcome.

    Config-based sibling of :func:`_log_identity_sequence_sync` (which needs a full
    :class:`DataMigrationInputs`). Used by the "Accept quarantined rows & continue"
    action: accepting the gap is the moment a quarantined load becomes COMPLETE, but it
    happens AFTER ``_finalize_run`` (which saw the run as incomplete and skipped the
    sync). Without this, an accept-after-load flow left the identity sequence at its
    start (``nextval`` = 1) and the app's first insert after cut-over collided with a
    migrated id (23505). Quarantined rows are permanently dropped, so ``MAX(pk)`` is
    final and syncing off it is correct. Never raises (best-effort follow-up).
    """
    if not table_names:
        return {}
    try:
        if sync is None:
            from dsql_migrator.core.target_introspector import (
                sync_identity_sequences as sync,
            )

        def _factory():
            return DsqlConnector(target_config, aws_profile=aws_profile).connect()

        synced = sync(list(table_names), connection_factory=_factory) or {}
    except Exception as exc:  # noqa: BLE001 - never fail the accept on this follow-up
        log_activity(
            ActivityCategory.FULL_LOAD,
            "identity sequence sync failed",
            status=ActivityStatus.FAILURE,
            detail=(
                f"Could not advance target identity sequences after accepting the gap: "
                f"{safe_error_message(exc)}. If any target table uses an identity "
                "primary key, run ALTER TABLE <t> ALTER COLUMN <pk> RESTART WITH "
                "<max(pk)+1> before cut-over, or re-run Validation to sync them."
            ),
        )
        return {}
    _log_identity_sequence_sync_outcome(synced)
    return synced


def _finalize_run(
    handle: JobHandle,
    job_id: str,
    table_names: Sequence[str],
    counts: _RunCounts,
    error_log: ErrorLogStore,
    *,
    accept_quarantined_rows: bool,
    inputs: "Optional[DataMigrationInputs]" = None,
    sync_sequences: "Optional[Callable[..., dict]]" = None,
) -> None:
    """Log the run outcome and raise :class:`FullLoadIncompleteError` unless complete.

    A clean run (or a cooperative cancel) logs and returns. An incomplete run
    raises so the job/step is ``FAILED`` -- EXCEPT when the ONLY incompleteness is
    quarantined rows (``real_failed == 0``) AND ``accept_quarantined_rows`` is set,
    in which case the run completes with an explicit "quarantine accepted" record:
    the permanently-dropped rows are an acknowledged gap (still surfaced in
    Validation), so CDC is no longer blocked. Any retryable real failure always
    raises, even with the flag set, so the override can never mask a recoverable
    failure.
    """
    incomplete = counts.real_failed + counts.quarantined
    if not incomplete or handle.cancelled:
        if not handle.cancelled:
            log_activity(
                ActivityCategory.FULL_LOAD,
                "run completed",
                status=ActivityStatus.SUCCESS,
                detail=f"{len(table_names)} table(s) loaded",
            )
            # Sync the identity sequence off the loaded MAX(pk). Safe here because the
            # load is COMPLETE: a PARTIAL load (a real failure leaves a gap that a later
            # retry fills) would set the sequence from an incomplete high-water mark and
            # the remaining rows could still collide, so that path is NOT synced. A
            # cancelled run is skipped for the same reason. (The accepted-quarantine
            # branch below is ALSO a completed load and syncs too -- quarantined rows are
            # permanently dropped, so MAX(pk) is final there as well.)
            _log_identity_sequence_sync(inputs, table_names, sync=sync_sequences)
        return
    total = len(table_names)
    # Quarantine records are the error-log rows whose message marks an isolated
    # dropped row (PK + reason), distinct from a table-level load failure.
    quarantined_rows = sum(
        1
        for record in error_log.records(job_id)
        if str(getattr(record, "message", "")).startswith("quarantined row pk[")
    )
    quarantine_only = counts.real_failed == 0 and counts.quarantined > 0
    if accept_quarantined_rows and quarantine_only:
        log_activity(
            ActivityCategory.FULL_LOAD,
            "run completed (quarantine accepted)",
            status=ActivityStatus.SUCCESS,
            detail=(
                f"{quarantined_rows} row(s) quarantined and ACCEPTED (permanently "
                "dropped, e.g. a value over DSQL's ~1 MiB per-value limit); the "
                f"target intentionally omits them. {counts.quarantined} table(s) "
                "completed with accepted gaps -- the gap is reported in Validation."
            ),
        )
        # This IS a completed load, so the identity sequence must be synced here too
        # -- exactly as on the clean path above. The clean-path comment warns against
        # syncing a PARTIAL load (a real failure leaves a gap that later retries fill,
        # so MAX(pk) is not yet final). Quarantined rows are the opposite: they are
        # PERMANENTLY dropped and never backfilled, so the current MAX(pk) IS final and
        # syncing off it is correct. Skipping it here left the sequence at its start
        # (nextval=1) after an accepted-gap load, so the app's first insert after
        # cut-over collided with a migrated id (duplicate key 23505) -- and only if the
        # operator happened to run Validation (v0.1.266 re-sync) was it repaired.
        _log_identity_sequence_sync(inputs, table_names, sync=sync_sequences)
        return
    # Name the affected tables and their reasons. "1 of 8 table(s) did not fully load"
    # is a count, not a diagnosis: reading it later tells you a run failed but not which
    # table or why, and the per-row detail lives only in the in-memory error log (gone
    # after a restart). Roll the per-table reasons into the durable entry, deduplicated
    # and bounded so a 500-table run cannot flood the rotated log.
    detail_parts = [
        f"{incomplete} of {total} table(s) did not fully load"
        + (f"; {quarantined_rows} row(s) quarantined" if quarantined_rows else "")
    ]
    reasons_by_table: dict[str, str] = {}
    # Full Load records only: CDC writes under this same job id (cdc_error_log_key), so
    # an unfiltered read could attribute a dead-lettered row's reason to a table in a
    # FULL_LOAD activity-log line. The quarantine count above is already safe (it keys
    # on the "quarantined row pk[" prefix, which only the Full Load writers emit).
    from dsql_migrator.ui.data_migration._cdc_status import full_load_error_records

    for record in full_load_error_records(error_log, job_id):
        table = str(getattr(record, "table", "") or "?")
        message = str(getattr(record, "message", "") or "").strip()
        if not message or table in reasons_by_table:
            continue  # first reason per table is the representative one
        reasons_by_table[table] = message[:200]
    if reasons_by_table:
        listed = "; ".join(
            f"{table}: {reason}"
            for table, reason in sorted(reasons_by_table.items())[:8]
        )
        more = max(0, len(reasons_by_table) - 8)
        detail_parts.append(
            f"reasons — {listed}" + (f" (+{more} more table(s))" if more else "")
        )
    log_activity(
        ActivityCategory.FULL_LOAD,
        "run incomplete",
        status=ActivityStatus.FAILURE,
        detail=". ".join(detail_parts),
    )
    if quarantine_only:
        guidance = (
            f"{quarantined_rows} row(s) were QUARANTINED (permanently dropped, "
            "e.g. a value over DSQL's ~1 MiB per-value limit) and are listed in "
            "the downloadable error log by primary key. Fix the offending source "
            "value(s) and re-run Full Load (the idempotent re-load fills only the "
            "gap), or choose 'Accept quarantined rows & continue' to proceed to "
            "CDC with the gap acknowledged; a plain retry cannot recover a "
            "permanently-rejected value."
        )
    elif quarantined_rows:
        guidance = (
            "some tables failed to load and "
            f"{quarantined_rows} row(s) were quarantined; review the downloadable "
            "error log, fix the offending source value(s) for quarantined rows, "
            "then use 'Retry failed tables'."
        )
    else:
        guidance = (
            "review the downloadable error log, then use 'Retry failed tables' to "
            "load the remaining tables before validating."
        )
    raise FullLoadIncompleteError(
        f"Full Load incomplete: {incomplete} of {total} table(s) did not fully "
        f"load. The target holds partial data -- {guidance}"
    )


def _predrop_dependent_views(migrator: DataMigrator) -> None:
    """Call the migrator's dependent-view pre-drop, if it supports one.

    A no-op for a fake/older migrator without the method (tests) or an append run
    (the method itself no-ops when nothing is being replaced). Any failure in this
    optional pre-pass is swallowed (logged) rather than allowed to abort the whole
    Full Load: if a view really still blocks a table's DROP, that surfaces as a
    normal per-table failure the user can act on -- it must never wipe the run's
    progress with an unhandled error.
    """
    hook = getattr(migrator, "predrop_dependent_views", None)
    if callable(hook):
        try:
            hook()
        except Exception:  # noqa: BLE001 - optional pre-pass; never fail the run
            _LOGGER.warning("Dependent-view pre-drop pass failed", exc_info=True)


def _recreate_dependent_views(migrator: DataMigrator) -> None:
    """Call the migrator's dependent-view recreate, if it supports one (else no-op).

    Best-effort post-pass: the tables/data are already loaded, so a failure here
    is logged and swallowed rather than failing an otherwise-successful run.
    """
    hook = getattr(migrator, "recreate_dependent_views", None)
    if callable(hook):
        try:
            hook()
        except Exception:  # noqa: BLE001 - optional post-pass; never fail the run
            _LOGGER.warning("Dependent-view recreate pass failed", exc_info=True)


def _apply_foreign_keys(migrator: DataMigrator) -> None:
    """Call the migrator's post-load foreign-key apply, if it supports one (else no-op).

    Best-effort run-level post-pass, mirroring :func:`_recreate_dependent_views`:
    Aurora DSQL enforces foreign keys, but they must be added AFTER the concurrent
    bulk load (which has no parent-before-child ordering). The data is already
    loaded, so a failure here is logged and swallowed rather than failing an
    otherwise-successful run; a foreign key that cannot be created (an orphan
    pre-gate hit, or an ALTER failure) is reported in the activity log for manual
    application, not treated as data loss.
    """
    hook = getattr(migrator, "apply_foreign_keys", None)
    if not callable(hook):
        return
    try:
        counts = hook()
    except Exception:  # noqa: BLE001 - optional post-pass; never fail the run
        _LOGGER.warning("Foreign-key apply pass failed", exc_info=True)
        return
    # Surface the post-load FK pass as a NAMED step in the activity log (the
    # migration's visible audit trail), mirroring the CDC cut-over "Apply foreign
    # keys" action, so a Full-Load-only run shows the foreign keys being (re)created
    # at load end instead of leaving the user wondering where they went. Emitted
    # only when the pass actually ran over >=1 FK: a load with no preserved FKs, or a
    # CDC run that defers them to cut over, returns (0, 0, 0) -> no noise. A hook that
    # returns nothing (older/test double) is tolerated (no summary line).
    if not isinstance(counts, tuple) or len(counts) != 3:
        return
    applied, skipped, failed = counts
    if applied or skipped or failed:
        log_activity(
            ActivityCategory.FULL_LOAD,
            "apply foreign keys",
            status=ActivityStatus.FAILURE if failed else ActivityStatus.SUCCESS,
            detail=(
                f"Post-load foreign-key pass: {applied} applied, "
                f"{skipped} skipped (orphaned rows), {failed} failed."
            ),
        )


def _log_excluded_lob_columns(
    inputs: "Optional[DataMigrationInputs]",
    scope: Optional[set[str]] = None,
) -> None:
    """Record one activity event per oversized-LOB column excluded from this run.

    Excluding a column is a deliberate decision to NOT migrate certain data -- the
    column is dropped from the load and arrives NULL on the target -- so it belongs in
    the activity log, which is the migration's audit trail (row-level quarantine is
    already logged; column-level exclusion should be too). Logged at ``INFO``: it is an
    expected, user-chosen omission, not a fault. Value-free (only names the column), so
    no Property-7 concern. ``scope`` (a retry's table subset) filters the events so a
    retry logs only the exclusions for the tables it actually re-ran; ``None`` = all.
    """
    if inputs is None:
        return
    for table_name in sorted(inputs.excluded_lob_columns):
        if scope is not None and table_name not in scope:
            continue
        for column in sorted(inputs.excluded_lob_columns[table_name]):
            log_activity(
                ActivityCategory.FULL_LOAD,
                "column excluded",
                status=ActivityStatus.INFO,
                target=f"{table_name}.{column}",
                detail=(
                    "oversized LOB column excluded from the load by user; the column "
                    "is left NULL on the target (its data is not migrated)"
                ),
            )


def _log_captured_watermark(
    watermark: "Optional[Watermark]",
    source_type: SourceType = SourceType.MYSQL,
) -> None:
    """Record the Full Load consistency point (watermark) in the activity log.

    The watermark pins the exact source position the snapshot reflects and is where a
    later CDC catch-up resumes -- the core of the gapless Full Load -> CDC handoff. It
    was persisted only on the in-memory job record, so the downloaded
    ``migration_activity.log`` (the artifact teams attach to a change ticket) carried no
    record of which source point-in-time the migration captured. It is now logged at
    ``INFO`` right after capture: the GTID when present, else the ``binlog_file:pos``,
    the snapshot UTC timestamp, and whether the per-table row counts are approximate
    ``information_schema`` estimates. Only a log POSITION and a timestamp -- never a row
    value -- so there is no Property-7 concern. ``None`` (legacy caller) is a no-op.
    """
    if watermark is None:
        return
    # Prefer the GTID (MySQL's portable resume coordinate); then binlog file:pos; then
    # the PostgreSQL WAL LSN (its resume coordinate); else word the absence for the source
    # engine (e.g. binary logging off on MySQL, or the LSN unreadable on PostgreSQL).
    if watermark.gtid_executed:
        coord = f"GTID {watermark.gtid_executed}"
    elif watermark.binlog_file:
        coord = f"binlog {watermark.binlog_file}:{watermark.binlog_position}"
    elif watermark.wal_lsn:
        coord = f"WAL LSN {watermark.wal_lsn}"
    elif source_type is SourceType.MYSQL:
        coord = "no binlog/GTID coordinate available (binary logging off or restricted)"
    else:
        engine = dialect_for(source_type).engine_display_name
        coord = (
            f"row-count baseline only (Full Load; the WAL/LSN handoff coordinate for a "
            f"gapless CDC catch-up could not be read for this {engine} source)"
        )
    ts = watermark.snapshot_timestamp.isoformat().replace("+00:00", "Z")
    approx = " (row counts are approximate estimates)" if watermark.row_counts_approximate else ""
    log_activity(
        ActivityCategory.FULL_LOAD,
        "watermark captured",
        status=ActivityStatus.INFO,
        detail=(
            f"consistency point for the gapless CDC handoff: {coord}; "
            f"snapshot={ts}; {len(watermark.table_row_counts)} table(s) counted{approx}"
        ),
    )


def _lob_excluded_note(migrator: "DataMigrator", table_name: str) -> str:
    """Return the per-table `` (N column(s) excluded: ...)`` suffix, or ``""``.

    The at-a-glance echo appended to a table's ``load table`` detail so the affected
    table's own line shows which oversized-LOB columns were dropped (the run-level
    ``column excluded`` events are the audit record). Reads ``migrator._inputs``
    defensively -- it is optional for legacy callers/tests. Shared by the single- and
    multi-process success sites so the note is identical on every path.
    """
    inputs = getattr(migrator, "_inputs", None)
    excluded = sorted(
        getattr(inputs, "excluded_lob_columns", {}).get(table_name, ()) if inputs else ()
    )
    if not excluded:
        return ""
    return f" ({len(excluded)} column(s) excluded: {', '.join(excluded)})"


def run_full_load(
    handle: JobHandle,
    tables: Sequence[TableDef],
    *,
    migrator: DataMigrator,
    error_log: ErrorLogStore,
    accept_quarantined_rows: bool = False,
    # Needed only to open a target connection for the post-load identity-sequence sync
    # (see _sync_identity_sequences_after_load). Optional so existing callers/tests that
    # do not exercise that path keep working -- but the UI DOES pass it, because an
    # unsynced identity sequence is a post-cut-over duplicate-key failure.
    inputs: "Optional[DataMigrationInputs]" = None,
) -> None:
    """Drive the Full Load export/import pipeline for the selected ``tables``.

    Seeds one chunk per selected table, captures the export watermark once and
    persists it on the job (Requirement 5.7 / Property 11), then migrates each
    table in turn, recording per-table ``IN_PROGRESS`` -> ``DONE``/``FAILED``
    progress through ``handle`` (Requirement 8.3). A per-table failure is
    isolated: the chunk is marked ``FAILED``, a credential-free
    :class:`~dsql_migrator.core.models.DataErrorRecord` is appended to
    ``error_log`` (keyed by the job id, Property 15), and the remaining tables
    continue. A watermark-capture failure is fatal and propagates so the job
    (and step) are marked ``FAILED`` (Requirement 5.7). After every selected
    table has been attempted, if ANY table failed the run raises
    :class:`FullLoadIncompleteError` so the job/step is ``FAILED`` (never
    ``DONE``): an incomplete load must not look successful, and the prerequisite
    gate then keeps Validation from running on partial data until the failed
    tables are retried.

    This is the selected-table-scoped successor to :func:`run_data_migration`
    (Property 16): the ``tables`` are the resolved selection from
    :class:`~dsql_migrator.core.table_selection.TableSelector`.
    """
    job_id = handle.job_id
    table_names = [table.name for table in tables]
    handle.update(lambda job: _seed_chunks(job, table_names))
    log_activity(
        ActivityCategory.FULL_LOAD,
        "run started",
        status=ActivityStatus.STARTED,
        detail=f"{len(table_names)} table(s) selected",
    )
    _log_excluded_lob_columns(inputs)

    watermark = migrator.capture_watermark(tables)
    handle.update(lambda job: setattr(job, "watermark", watermark))
    # inputs is Optional here; fall back to MySQL when absent (matches the default).
    _log_captured_watermark(
        watermark,
        getattr(getattr(inputs, "source_config", None), "source_type", SourceType.MYSQL),
    )

    # On a "drop & reload" run, drop views that depend on the replaced tables
    # BEFORE the per-table DROP+recreate (a view can span several tables loaded in
    # parallel, so this is a run-level pre-pass), then recreate them after.
    _predrop_dependent_views(migrator)
    counts = _migrate_tables_in_parallel(handle, job_id, tables, migrator, error_log)
    _recreate_dependent_views(migrator)
    _apply_foreign_keys(migrator)
    _finalize_run(
        handle,
        job_id,
        table_names,
        counts,
        error_log,
        accept_quarantined_rows=accept_quarantined_rows,
        inputs=inputs,
    )


def _seed_retry_chunks(
    job: MigrationJob,
    prior_chunks: Sequence[ChunkState],
    retry_names: set,
) -> None:
    """Seed a retry job from the prior run, resetting only the failed chunks.

    Tables that already succeeded keep their ``DONE`` state, loaded row count,
    and attempt count, so the retry job shows the whole picture; tables being
    retried are reset to ``PENDING`` (their attempt count is preserved so the
    retry increments it). This keeps a single unified view across the original
    run and the retry instead of fragmenting it into separate jobs.
    """
    chunks: list[ChunkState] = []
    for prior in prior_chunks:
        if prior.chunk_id in retry_names:
            chunks.append(
                ChunkState(
                    chunk_id=prior.chunk_id,
                    status="PENDING",
                    rows_loaded=0,
                    attempts=prior.attempts,
                )
            )
        else:
            chunks.append(prior.model_copy(deep=True))
    job.chunks = chunks
    _recompute_progress(job)


def run_full_load_retry(
    handle: JobHandle,
    prior_chunks: Sequence[ChunkState],
    tables_to_retry: Sequence[TableDef],
    *,
    migrator: DataMigrator,
    error_log: ErrorLogStore,
    watermark: Optional[Watermark] = None,
    accept_quarantined_rows: bool = False,
    # See run_full_load: needed for the post-load identity-sequence sync. A retry that
    # COMPLETES the load is exactly when the sync must run -- the first attempt's
    # finalize skipped it (the run was incomplete), so without this the sequence stays
    # unsynced on every recovered run.
    inputs: "Optional[DataMigrationInputs]" = None,
) -> None:
    """Re-run Full Load for only the previously failed ``tables_to_retry``.

    Carries the prior run's chunk states forward (already-succeeded tables stay
    ``DONE``) and reuses the original ``watermark`` (no new snapshot is taken),
    then migrates each retry table, recording ``IN_PROGRESS`` ->
    ``DONE``/``FAILED`` and appending any failure to the single ``error_log``
    (Property 15). A per-table failure is isolated so the remaining retries
    continue, exactly like :func:`run_full_load`.
    """
    job_id = handle.job_id
    retry_names = {table.name for table in tables_to_retry}
    handle.update(lambda job: _seed_retry_chunks(job, prior_chunks, retry_names))
    _log_excluded_lob_columns(inputs, scope=retry_names)
    if watermark is not None:
        handle.update(lambda job: setattr(job, "watermark", watermark))
        # A retry reuses the ORIGINAL watermark (no new snapshot); record which
        # consistency point it resumed against so the audit trail is complete.
        _log_captured_watermark(
            watermark,
            getattr(
                getattr(inputs, "source_config", None), "source_type", SourceType.MYSQL
            ),
        )

    # Same run-level view pre-drop / recreate as run_full_load, so a retry that
    # DROP+recreates a table whose view dependency blocked the first attempt now
    # succeeds instead of silently skip-loading over stale rows.
    _predrop_dependent_views(migrator)
    counts = _migrate_tables_in_parallel(
        handle, job_id, tables_to_retry, migrator, error_log
    )
    _recreate_dependent_views(migrator)
    _apply_foreign_keys(migrator)
    _finalize_run(
        handle,
        job_id,
        list(retry_names),
        counts,
        error_log,
        accept_quarantined_rows=accept_quarantined_rows,
        inputs=inputs,
    )


def run_data_migration(
    handle: JobHandle,
    tables: Sequence[TableDef],
    *,
    migrator: DataMigrator,
) -> None:
    """Run Full Load without surfacing per-table errors to an error log.

    Thin back-compat wrapper over :func:`run_full_load` (the primary entry point)
    that discards per-table error records into a throwaway in-memory log. New
    callers should use :func:`run_full_load` with the session's
    :class:`~dsql_migrator.core.error_log.ErrorLogStore` so failures are
    downloadable (Property 15).
    """
    run_full_load(handle, tables, migrator=migrator, error_log=ErrorLogStore())


def job_status_to_step_status(job_status: str) -> Optional[StepStatus]:
    """Map a :class:`JobManager` job status to the Data Migration step status.

    Returns ``DONE``/``FAILED`` for terminal job states and ``None`` while the
    job is still ``PENDING``/``RUNNING`` (the step stays ``IN_PROGRESS``). A
    ``CANCELLED`` (user-stopped) job maps to ``FAILED`` so the step shows as
    incomplete and the retry-remaining-tables affordance appears.
    """
    if job_status == "DONE":
        return StepStatus.DONE
    if job_status in ("FAILED", "CANCELLED"):
        return StepStatus.FAILED
    return None


def reconcile_full_load_step(
    saved_step: StepStatus, job_status: Optional[str]
) -> Optional[StepStatus]:
    """Return the step status a saved ``IN_PROGRESS`` should be corrected to.

    The live poll only advances the Full Load step while the job is RUNNING. If
    the job reached a terminal state without the poll running -- e.g. an app
    restart reconciled a hung/interrupted job to FAILED, or the stall watchdog
    reaped it -- the saved step would otherwise stay ``IN_PROGRESS`` forever and
    the screen would show "Full Load in progress…" with no terminal affordances.

    This is the render-time reconciliation: when ``saved_step`` is ``IN_PROGRESS``
    and ``job_status`` is terminal, it returns the corrected step
    (``DONE``/``FAILED``); otherwise it returns ``None`` (leave the step as-is).
    ``job_status`` is ``None`` when the job is unknown/absent, which also leaves
    the step unchanged (nothing to reconcile against).
    """
    if saved_step is not StepStatus.IN_PROGRESS:
        return None
    if job_status is None:
        return None
    return job_status_to_step_status(job_status)


def data_migration_step_after_cdc(
    status: StepStatus, *, cdc_streaming: bool
) -> Optional[StepStatus]:
    """Promote the Data Migration step to DONE once CDC is actually streaming.

    The step normally only reaches DONE via a finished Full Load. But a CDC-only
    plan (or a reconnected session with no local Full Load watermark) never runs
    Full Load, so the step would stay un-DONE and downstream gating (Validation)
    would stay locked ("Complete Data Migration first…") even though data is
    actively flowing to the target. When CDC is streaming, treat the step as DONE
    for gating.

    Returns the new status (``DONE``) to apply, or ``None`` to leave the step
    unchanged. Never downgrades a terminal ``DONE``/``FAILED`` (a finished Full
    Load or a real failure wins), and only promotes when CDC is live.
    """
    if not cdc_streaming:
        return None
    if status in (StepStatus.DONE, StepStatus.FAILED):
        return None
    return StepStatus.DONE


# ---------------------------------------------------------------------------
# In-process Data Migrator: read-only export stream -> batched INSERT load
# ---------------------------------------------------------------------------

# Builds the in-process batched importer for a run (injectable so tests skip
# real DSQL). Receives the run inputs so it can use the configured profile/region.
ImporterFactory = Callable[["DataMigrationInputs"], BatchedImporter]


def _default_importer_factory(inputs: "DataMigrationInputs") -> BatchedImporter:
    """Build the in-process batched importer for ``inputs``.

    Loads via the same DSQL connection path the tool uses everywhere else (boto3
    IAM token + psycopg through :class:`DsqlConnector` with the configured
    profile/region), so it needs no external binary and shares the tool's working
    credential context. The connector caches its short-lived token across batches
    and tables (Property 7).
    """
    connector = DsqlConnector(
        inputs.target_config, aws_profile=inputs.aws_profile
    )
    # Per-table batch parallelism and rows-per-batch are operator-tunable
    # (DSQL_MIGRATOR_FULL_LOAD_BATCH_PARALLELISM / _BATCH_ROWS) so a deployment can
    # trade throughput against OCC-collision rate and the cluster connection quota
    # without a code change; they fall back to the bounded defaults.
    cfg = load_config()
    options = BatchedImportOptions(
        on_conflict=inputs.on_conflict,
        parallelism=cfg.full_load_batch_parallelism,
        batch_size=cfg.full_load_batch_rows,
    )
    # occ_max_attempts is the per-batch retry budget for BOTH OCC conflicts and
    # transient connection failures; a large-scale load needs it patient enough to
    # ride out a connection storm at a high-parallelism transition (config key
    # DSQL_MIGRATOR_FULL_LOAD_OCC_MAX_ATTEMPTS).
    return BatchedImporter(
        options,
        connection_factory=connector.connect,
        occ_max_attempts=cfg.full_load_occ_max_attempts,
    )


# Drops and recreates one table's target from converted DDL, returning its
# secondary-index DDLs to (re)create after the data load. Injectable for tests.
TableRecreator = Callable[[TableDef], list[str]]


def _views_referencing(
    view_ddls: Mapping[str, str], tables: "frozenset[str]"
) -> list[str]:
    """Return the CREATE-VIEW DDLs whose SQL references any of ``tables``.

    A view that SELECTs from a table being DROP+recreated blocks that table's
    DROP. We select exactly the views that name one of the replace ``tables`` --
    matching either the qualified ``schema.table`` or the bare table name as a
    word in the view's DDL -- so an unrelated view is never needlessly dropped.
    Case-insensitive, word-boundary match to avoid matching a substring of a
    longer identifier. Deterministic order (by view name) for stable behavior.
    """
    if not view_ddls or not tables:
        return []
    wanted: list[str] = []
    for view_name in sorted(view_ddls):
        ddl = view_ddls[view_name]
        low = ddl.lower()
        for table in tables:
            bare = table.split(".")[-1].lower()
            qualified = table.lower()
            if _names_in(low, (qualified, bare)):
                wanted.append(ddl)
                break
    return wanted


def _names_in(haystack_lower: str, candidates: "Sequence[str]") -> bool:
    """True if any candidate appears in ``haystack_lower`` as a bounded token.

    Guards against a bare table name matching a substring of a longer identifier
    (e.g. ``orders`` inside ``orders_archive``) by requiring a non-identifier
    character (or string edge) on both sides.
    """
    import re

    for name in candidates:
        if not name:
            continue
        if re.search(rf"(?<![\w.]){re.escape(name)}(?![\w])", haystack_lower):
            return True
    return False


def _default_table_recreator(inputs: "DataMigrationInputs") -> TableRecreator:
    """Build the default DROP+recreate function for a run's target.

    Used for tables the user confirmed to load fresh (``inputs.replace_tables``):
    DROPs+recreates the empty target via
    :func:`~dsql_migrator.core.schema_applier.recreate_table` (DSQL has no
    TRUNCATE). The DDL comes from the run's APPLIED per-table conversion
    (``inputs.table_conversions``, honoring the user's Schema Conversion edits) so
    a fresh re-load preserves a custom-remapped schema instead of clobbering it
    with a deterministic re-derivation; a table not carried there falls back to the
    deterministic :class:`SchemaConverter`. Returns the table's secondary-index
    DDLs so the caller recreates them after loading.
    """

    def recreate(table: TableDef) -> list[str]:
        conversion = inputs.table_conversions.get(table.name)
        if conversion is None:
            # No applied conversion carried in (e.g. a table generated outside this
            # session): re-derive deterministically with the SOURCE engine's dialect
            # (else a PostgreSQL-source table would be re-derived as MySQL DDL).
            conversion = SchemaConverter(
                source_type=inputs.source_config.source_type
            ).convert_table(table, SchemaConvertOptions())
        connector = DsqlConnector(
            inputs.target_config, aws_profile=inputs.aws_profile
        )
        recreate_table(
            conversion.schema_ddls,
            conversion.target_ddl,
            connection_factory=connector.connect,
        )
        return list(conversion.index_ddls)

    return recreate


class BatchedTableMigrator:
    """In-process :class:`DataMigrator`: stream source rows and batch-load DSQL.

    Per table it streams a read-only consistent-snapshot of converted rows
    (:meth:`TableExporter.stream_converted_rows`, Property 1) straight into the
    in-process :class:`~dsql_migrator.core.batched_import.BatchedImporter`
    (bounded-parallel idempotent ``INSERT ... ON CONFLICT`` batches with OCC
    retry -- Properties 2/3/5). Nothing is staged to local disk or S3, and the
    load uses the same boto3 IAM connection the tool already uses elsewhere (no
    external binary). The export consistency point is captured once by
    :class:`~dsql_migrator.core.watermark.WatermarkCapturer` (Property 11). All
    seams are injectable so tests never touch a real MySQL/DSQL.
    """

    def __init__(
        self,
        inputs: "DataMigrationInputs",
        *,
        exporter: Optional[TableExporter] = None,
        watermark_capturer: Optional[WatermarkCapturer] = None,
        importer_factory: ImporterFactory = _default_importer_factory,
        table_recreator: Optional[TableRecreator] = None,
        target_counter: Optional[Callable[[TableDef], Optional[int]]] = None,
        target_pk_reader: Optional[
            Callable[[TableDef], Optional[list[str]]]
        ] = None,
    ) -> None:
        """Build a migrator bound to one run's ``inputs``."""
        self._inputs = inputs
        # The Full Load stream opts into a per-socket read timeout so a
        # connected-but-stalled source read (a large table's page that never
        # returns) fails the table -- making it retryable -- instead of hanging
        # the job in RUNNING forever. Each keyset page is bounded, so it returns
        # well within the timeout; only a genuine stall trips it.
        self._exporter = exporter or TableExporter(
            engine_factory=make_source_engine_factory(
                inputs.source_password,
                read_timeout_seconds=FULL_LOAD_SOURCE_READ_TIMEOUT_SECONDS,
            ),
            # Opt-in source-load throttle: when the operator sets a ceiling, every
            # reader (single + each shard, since both go through this exporter)
            # pauses between pages while the source's Threads_running exceeds it.
            # None (default) = no throttle. Read here so both the in-process and
            # per-shard-process paths (which all use this migrator's exporter) honor it.
            max_source_threads_running=load_config().full_load_max_source_threads_running,
        )
        self._watermark_capturer = watermark_capturer or WatermarkCapturer(
            engine_factory=make_source_engine_factory(inputs.source_password)
        )
        self._importer_factory = importer_factory
        self._table_recreator = table_recreator or _default_table_recreator(inputs)
        # Post-load completeness verifier for clean-replace loads (injectable for
        # tests). Defaults to a read-only target COUNT(*).
        self._target_counter = target_counter or self._default_count_target_rows
        # Reads the target's ACTUAL primary key (injectable for tests). Used only on
        # the append path when the applied conversion asks for a different key than
        # the source, to decide against the live target instead of assuming.
        self._target_pk_reader = target_pk_reader or self._default_target_pk

    @property
    def source_type(self) -> SourceType:
        """The source engine this migrator reads (drives engine-specific source-error
        classification / hints on the retry path)."""
        return self._inputs.source_config.source_type

    def capture_watermark(self, tables: Sequence[TableDef]) -> Watermark:
        """Capture the export consistency point for the selected ``tables``.

        Only the tables being migrated are counted within the snapshot, so a
        small selection is not blocked by snapshot counts over large, unrelated
        source tables (read-only).
        """
        table_names = [table.name for table in tables]
        source = self._inputs.source_config
        if source.source_type is not SourceType.MYSQL:
            # A binlog/GTID watermark is a MySQL CDC concept, and the MySQL WatermarkCapturer
            # runs MySQL-only SQL (START TRANSACTION WITH CONSISTENT SNAPSHOT / SHOW MASTER
            # STATUS) that FAILS on PostgreSQL. A non-MySQL source records its OWN resume
            # coordinate (PostgreSQL: the WAL LSN, via the dialect) plus the scan-free
            # row-count baseline (dialect-dispatched: pg_class.reltuples for PG).
            return self._capture_postgres_watermark(source, table_names)
        return self._watermark_capturer.capture(source, table_names)

    def _capture_postgres_watermark(
        self, source: SourceConnectionConfig, table_names: Sequence[str]
    ) -> Watermark:
        """Watermark for a non-MySQL (PostgreSQL) source: WAL LSN + row-count baseline.

        Records the dialect's CDC resume coordinate -- for PostgreSQL the WAL ``wal_lsn``
        (the gapless Full Load -> CDC handoff point, PG's analog of MySQL binlog:pos),
        captured BEFORE the per-table reader snapshots open so replaying from it is a
        superset -- plus the approximate per-table estimate (never a COUNT(*) scan) via
        the source dialect, so the progress baseline is preserved.

        Two paths by whether CDC will follow (``inputs.cdc_stack_name``):

        * **Full Load + CDC** (stack name set): create a logical replication slot +
          publication ON THE SOURCE at this consistency point (via the audited source-write
          path). The slot's returned consistent-point becomes ``wal_lsn`` and pins the
          source WAL until CDC consumes it; the slot/publication names are recorded so the
          connector resumes from them and teardown drops them. Slot creation is FATAL here
          -- a missing slot would silently lose every change made during the load.
        * **Full Load only** (no stack name): a best-effort ``pg_current_wal_lsn()`` READ
          (no slot -- a slot with no consumer would pin WAL and fill the source disk).
          None (e.g. insufficient privilege) still yields a valid, loadable watermark.
        """
        dialect = dialect_for(source.source_type)
        stack_name = self._inputs.cdc_stack_name
        wal_lsn: Optional[str] = None
        slot_name: Optional[str] = None
        publication_name: Optional[str] = None
        # Row estimates (and, Full-Load-only, the plain LSN read) go through the shared
        # read-only-guarded engine.
        engine = make_source_engine_factory(self._inputs.source_password)(source)
        try:
            with engine.connect() as connection:
                if stack_name is None:
                    wal_lsn = dialect.capture_resume_lsn(connection)
                estimates = estimate_source_rows(connection, list(table_names), dialect)
        finally:
            engine.dispose()
        # Full Load + CDC: create the slot+publication at this consistency point (before
        # any per-table reader snapshot opens -- capture_watermark runs strictly before
        # _migrate_tables_in_parallel), through the dedicated audited source-WRITE path
        # (NOT the read-only-guarded engine above).
        if stack_name is not None:
            handles = self._provision_pg_replication(source, stack_name, table_names)
            wal_lsn = handles.consistent_lsn
            slot_name = handles.slot_name
            publication_name = handles.publication_name
        return Watermark(
            snapshot_timestamp=datetime.now(timezone.utc),
            wal_lsn=wal_lsn,
            slot_name=slot_name,
            publication_name=publication_name,
            # None estimate (never-analyzed / missing) -> 0, matching the MySQL baseline.
            table_row_counts={n: (c or 0) for n, c in estimates.items()},
            row_counts_approximate=True,
        )

    def _provision_pg_replication(
        self,
        source: SourceConnectionConfig,
        stack_name: str,
        table_names: Sequence[str],
    ) -> "PgReplicationHandles":
        """Create the PostgreSQL CDC slot + publication on the source, audited.

        The ONLY source-write in the Full Load path. Uses the dedicated, non-read-only-
        guarded, AUTOCOMMIT PostgreSQL write engine (:mod:`dsql_migrator.core.cdc_pg_slot`);
        names the objects deterministically from ``stack_name`` so the connector param and
        teardown reference the same slot/publication. Each write is recorded in the audit
        trail (``log_activity``). Raises on failure -- the gapless handoff is impossible
        without the slot, so the Full Load must fail loudly rather than produce a
        decorative LSN.
        """
        from dsql_migrator.core import cdc_pg_slot

        slot = cdc_pg_slot.pg_slot_name(stack_name)
        publication = cdc_pg_slot.pg_publication_name(stack_name)

        def _audit(message: str) -> None:
            log_activity(
                ActivityCategory.FULL_LOAD,
                "CDC replication slot",
                status=ActivityStatus.INFO,
                detail=f"source ({stack_name}): {message}",
            )

        engine = cdc_pg_slot.build_pg_source_write_engine(
            source, self._inputs.source_password
        )
        try:
            with engine.connect() as connection:
                return cdc_pg_slot.provision_pg_replication(
                    connection,
                    slot_name=slot,
                    publication_name=publication,
                    tables=list(table_names),
                    on_log=_audit,
                )
        finally:
            engine.dispose()

    def migrate_table(
        self,
        table: TableDef,
        *,
        on_rows: Optional[Callable[[int, int], None]] = None,
        should_cancel: Optional[Callable[[], bool]] = None,
        pre_recreated: bool = False,
        resume_job: Optional[MigrationJob] = None,
        on_throttle: Optional[Callable[[bool, Optional[int]], None]] = None,
    ) -> TableLoadResult:
        """Stream ``table`` from the source and load it via batched INSERTs.

        Rows are produced lazily from a read-only consistent snapshot and loaded
        in bounded-parallel idempotent batches, so the whole table is never
        materialized (Requirement 5.1 / Property 2). ``on_rows`` (if given) is
        invoked as each batch lands with ``(rows_inserted, rows_skipped)``, so the
        caller can show live cumulative progress -- counting skipped (already
        present) rows so a re-load that mostly skips still advances -- rather than
        only the final total.
        ``should_cancel`` (if given) is polled between batches; when it turns
        ``True`` the load stops early and :class:`_FullLoadStopped` is raised so
        the caller marks the table for retry (the re-load is idempotent). Returns
        rows loaded; raises if any batch ultimately failed so the caller records
        a per-table failure with the underlying cause.
        """
        # Drop the user-excluded LOB columns up front so EVERY downstream use --
        # the keyset read, the INSERT column list, and shard planning -- sees the
        # same column-filtered view. The target still carries the column (it is
        # recreated from the applied DDL, which is untouched); the load simply never
        # writes to it, and the column takes its default / NULL. Name and PK are
        # preserved, so target-PK parsing and target lookups below are unaffected.
        # ``original_table`` keeps the full column set for target RECREATION only:
        # dropping the column from the load must not drop it from the target schema
        # (that would diverge from CDC and from the schema the user applied in Step
        # 2), so the recreator is always handed the unfiltered table.
        original_table = table
        table = apply_lob_exclusions(
            table, self._inputs.excluded_lob_columns.get(table.name)
        )
        applied = self._inputs.table_conversions.get(table.name)
        target_types = (
            parse_target_column_types(applied.target_ddl)
            if applied is not None
            else None
        )
        # The TARGET table's primary key, parsed from the APPLIED (possibly user- or
        # AI-edited) target DDL -- the single source of truth for the conflict key.
        # When the target PK differs from the source PK (e.g. a composite
        # (leading, id) chosen in Schema Conversion), Full Load MUST use the target
        # PK for ON CONFLICT / SKIP_EXISTING, or it would key on the wrong columns
        # (silent skip-wrong on append, or a 42P10/23505 hard failure). None => the
        # applied DDL had no parseable PK, so the loader falls back to the source PK
        # (unchanged behavior for the common target-PK == source-PK case).
        target_key_columns = (
            parse_target_primary_key(applied.target_ddl)
            if applied is not None
            else []
        ) or None
        # If the user confirmed loading this table fresh over existing data, DROP
        # and recreate its empty target first (DSQL has no TRUNCATE); the returned
        # secondary-index DDLs are (re)created after the load.
        index_ddls: Optional[list[str]] = None
        # When CDC is streaming into the target, a DROP+recreate would race the
        # live sink, so replace/DROP is disabled regardless of replace_tables.
        is_replace = (
            not self._inputs.cdc_coexisting
            and table.name in self._inputs.replace_tables
        )
        # A CHANGED target PK (e.g. the composite (leading, id) recommended to avoid
        # hot partitions) is a SCHEMA change, so it can only be honored by creating
        # the target from the applied DDL -- appending cannot retrofit a key onto an
        # existing table. When the target is EMPTY, recreating it is non-destructive
        # (there is nothing to lose), so promote this table to the replace path and
        # give the user the schema they actually chose.
        #
        # Without this the load "succeeded" into whatever shape the target happened to
        # have: an empty table still carrying the OLD single-column key accepted all
        # the rows, so the user believed they had the hot-partition remedy while the
        # table was in fact keyed the old way -- and now populated, so correcting it
        # required a destructive reload. Silently loading data in the wrong shape is
        # worse than either refusing or recreating.
        if (
            not is_replace
            and not self._inputs.cdc_coexisting
            and target_key_columns
            and target_key_columns != list(table.primary_key)
            and not pre_recreated
            and self._target_counter(table) == 0
        ):
            # ...unless the empty target ALREADY carries that key -- the normal state
            # right after "Apply all to target" in Step 2. Recreating it would DROP and
            # CREATE the identical table to "apply" a key it already has: a wasted DDL
            # round trip (DSQL permits one DDL per transaction) that the confirm dialog
            # has to announce as a recreate, which reads as a contradiction. Only an
            # EQUAL key skips the promotion; a key that cannot be read (None) is
            # unknown, not safe, so it still recreates -- matching the append path
            # below, which refuses rather than assumes when the catalog won't answer.
            if self._target_pk_reader(table) != target_key_columns:
                is_replace = True

        # Reader range sharding: for a LARGE single-integer-PK table, split the read
        # into K disjoint PK ranges streamed concurrently (each its own snapshot), so
        # the CPU-bound single keyset reader isn't the ceiling. plan_pk_shard_ranges
        # returns one (None, None) range -- i.e. the original single reader -- for
        # small tables, composite/non-integer PKs, or when sharding is off (K<=1).
        #
        # Sharded ONLY on the CDC-coexisting path. The K shards each open an
        # independently-timed CONSISTENT SNAPSHOT, so a source written to DURING the
        # load could land a cross-shard torn read (one row of a multi-row source txn
        # in shard A's snapshot, its sibling not yet in shard B's). That is only
        # provably safe when a CDC stream will reconcile any post-snapshot write:
        # SKIP_EXISTING makes the re-load idempotent (disjoint ranges never
        # double-load) and CDC backfills the torn sibling. Neither a REPLACE (plain
        # INSERT, no CDC) NOR a NON-CDC append has anything to reconcile it, so both
        # must use a SINGLE reader (one snapshot = one point-in-time cut). Requiring
        # cdc_coexisting -- not merely "not is_replace" -- is what closes the non-CDC
        # append torn-read hole; it matches the multiprocess planner's _shardable_ok.
        cfg = load_config()
        shard_ranges: list = [(None, None)]
        if not is_replace and self._inputs.cdc_coexisting:
            # Source-connection guardrail: total concurrent source snapshot readers
            # = table_parallelism x reader_shards. Clamp the effective shard count so
            # that product stays under a safe ceiling (each reader holds a long-lived
            # source connection; unbounded, table_parallelism(<=16) x shards(<=8) =
            # 128 could exhaust the source's max_connections). The write side is
            # unaffected (one write pool per table). See config full_load_reader_shards.
            max_src_readers = _MAX_SOURCE_READERS
            tp = max(1, cfg.full_load_table_parallelism)
            effective_shards = max(1, min(cfg.full_load_reader_shards,
                                          max_src_readers // tp))
            # Say so when the configured shard count was clamped. Silently loading
            # with fewer readers than asked for looks like the setting had no effect;
            # naming the ceiling (and that it protects the SOURCE's max_connections)
            # makes the trade-off visible and points at the knob that would help.
            if effective_shards < cfg.full_load_reader_shards:
                _LOGGER.info(
                    "Table %s: reader shards clamped %d -> %d (table parallelism %d "
                    "x shards must stay <= %d concurrent source readers to protect "
                    "the source's max_connections; lower table parallelism to allow "
                    "more shards per table)",
                    table.name, cfg.full_load_reader_shards, effective_shards, tp,
                    max_src_readers,
                )
            shard_ranges = self._exporter.plan_pk_shard_ranges(
                self._inputs.source_config, table, effective_shards,
                min_rows=cfg.full_load_shard_min_rows,
            )

        def _shard_stream(
            lo: "Optional[int]", hi: "Optional[int]"
        ) -> "Iterator[Mapping[str, object]]":
            return self._exporter.stream_converted_rows(
                self._inputs.source_config,
                table,
                should_cancel=should_cancel,
                target_types=target_types,
                pk_lower=lo,
                pk_upper=hi,
                on_throttle=on_throttle,
            )

        sharded = len(shard_ranges) > 1
        if sharded:
            shard_sources = [_shard_stream(lo, hi) for (lo, hi) in shard_ranges]
            rows: "Iterator[Mapping[str, object]]" = iter(())  # unused when sharded
        else:
            lo, hi = shard_ranges[0]
            shard_sources = None
            rows = _shard_stream(lo, hi)

        if is_replace:
            if pre_recreated:
                # The parent already DROP+recreated this empty target in the serial
                # pre-pass (avoiding a concurrent-DDL catalog storm at startup). Don't
                # re-run the DDL; derive the same secondary-index DDLs from the applied
                # conversion so they are still (re)created after the load.
                index_ddls = (
                    list(applied.index_ddls) if applied is not None else []
                )
            else:
                # Recreate from the UNFILTERED table so the target keeps the excluded
                # column (only its data is skipped, matching CDC); the fallback
                # deterministic conversion would otherwise omit a dropped column.
                index_ddls = self._table_recreator(original_table)
            # Clean load into a freshly-emptied target: plain INSERT (no ON
            # CONFLICT) -- DSQL never silently drops a non-conflicting row. The
            # target was just recreated from the applied DDL, so target_key_columns
            # (parsed from that same DDL) is exactly the new target's PK.
            load_on_conflict: Optional[OnConflictMode] = OnConflictMode.NONE
            load_key_columns = target_key_columns
        else:
            # Idempotent load into existing/CDC-fed data: insert only the missing
            # keys (never overwrite a newer CDC row, never use the DSQL-unsafe
            # multi-row ON CONFLICT that silently drops rows). Works for single- or
            # composite-column keys.
            load_on_conflict = OnConflictMode.SKIP_EXISTING
            # APPEND path does not recreate the target, so the key we load against
            # must match the key the LIVE TARGET actually has. When the applied
            # conversion asks for a different (e.g. composite ``(leading, id)``) PK
            # than the source, that has to be resolved against the target itself --
            # not assumed.
            #
            # This used to assume the target "still has its original key" and refuse
            # outright. That assumption is false on the primary path it blocked: the
            # user picks a Composite key in Schema Conversion, applies it (so the
            # target really does have ``(user_id, id)``), then runs the first Full
            # Load -- an APPEND, because append is the default and the derived
            # ``replace_tables`` is empty when the target holds no rows. The refusal
            # named "Drop & reload" as the fix, but that control only renders for
            # tables that already contain data, and ``replace_targets`` is derived
            # from that same set -- so on an empty target the remedy did not exist.
            #
            # An empty target normally does not reach here: it was promoted to the
            # replace path above, which recreates the table from the applied DDL so the
            # key is right by construction. Two cases still land here with a changed
            # key -- a target that HOLDS ROWS (a DROP would destroy data the user never
            # agreed to lose) and a CDC-coexisting load (a DROP would race the live
            # sink, so replace is forbidden however empty the target is). Both are
            # decided against the target's REAL key rather than an assumption:
            #   * it matches the applied DDL -> key the idempotent append on it;
            #   * it disagrees, or cannot be read -> refuse (unknown is not "safe"),
            #     naming the real key and the ways forward.
            source_pk = list(table.primary_key)
            load_key_columns = None  # default: the target's existing (source) PK
            if target_key_columns and target_key_columns != source_pk:
                actual = self._target_pk_reader(table)
                if actual is None:
                    raise RuntimeError(
                        f"Table '{table.name}' is configured with a changed "
                        f"primary key {tuple(target_key_columns)}, but the "
                        "target's actual primary key could not be read, so an "
                        "idempotent append cannot be keyed safely. Check the "
                        "target connection and retry this table."
                    )
                if actual != target_key_columns:
                    # Name the remedy that is actually available. While CDC streams
                    # into the target, "Drop & reload" is not one: recreating the
                    # table would race the live sink, so the schema has to be fixed
                    # before CDC starts.
                    remedy = (
                        "Stop CDC, apply the converted schema for this table in "
                        "Step 2 (Schema Conversion), then re-run the load — while "
                        "CDC is streaming the table cannot be recreated."
                        if self._inputs.cdc_coexisting
                        else (
                            "Apply the converted schema in Step 2 (Schema "
                            'Conversion), or choose "Drop & reload" for this table '
                            "to recreate it from the converted DDL (its existing "
                            "rows are permanently lost)."
                        )
                    )
                    raise RuntimeError(
                        f"Table '{table.name}' is configured with a changed primary "
                        f"key {tuple(target_key_columns)}, but the target table's "
                        f"primary key is {tuple(actual)} — a primary key cannot be "
                        f"changed by appending. {remedy}"
                    )
                load_key_columns = actual
        # Batch-level resume (Property 4): pass the resume job ONLY on the SKIP_EXISTING
        # (append / CDC-coexist) path, where the target is NOT recreated -- so batches a prior
        # attempt already committed are still present, and import_rows can SKIP those completed
        # keyset ranges on a retry (a source-drop re-read of a 99%-loaded table no longer
        # re-probes every already-loaded batch). NEVER on the replace/NONE path: that recreates
        # the empty target on retry, so the prior batches' rows are gone -- skipping them would
        # silently lose data.
        load_job = (
            resume_job if load_on_conflict is OnConflictMode.SKIP_EXISTING else None
        )
        importer = self._importer_factory(self._inputs)
        try:
            result = importer.import_rows(
                rows,
                table,
                index_ddls=index_ddls,
                job=load_job,
                on_batch_loaded=on_rows,
                should_cancel=should_cancel,
                on_conflict=load_on_conflict,
                shard_sources=shard_sources,
                key_columns=load_key_columns,
            )
        except ExportCancelled as exc:
            # A cooperative stop interrupted the source read between pages (the
            # importer was pulling rows). Treat it exactly like a batch-boundary
            # stop: incomplete + retryable, not a data error.
            _close_row_streams(rows, shard_sources)
            raise _FullLoadStopped(table.name) from exc
        except BaseException:
            # Any other failure (notably a dropped source connection) abandons these
            # row streams mid-read. They are GENERATORS that dispose their source
            # engine in their own ``finally``, so an abandoned one keeps its MySQL
            # connection open until it is closed or garbage-collected -- and the
            # raising frame keeps it referenced. Close them here so the connection is
            # released as the exception leaves, instead of staying pinned while a
            # caller waits out a failover and opens ANOTHER connection to retry.
            _close_row_streams(rows, shard_sources)
            raise
        if result.cancelled:
            raise _FullLoadStopped(table.name)
        if result.failures:
            detail = f": {result.first_error}" if result.first_error else ""
            raise RuntimeError(
                f"{result.failures} batch(es) failed loading "
                f"'{table.name}'{detail}"
            )
        # Post-load completeness check for a clean replace: the target was
        # recreated empty, so its actual row count MUST be at least the rows just
        # loaded. A shortfall means rows were silently lost on the target (the
        # importer/loader reported success but the rows did not persist) -- raise
        # so the table is marked FAILED and retried, never falsely reported DONE.
        if is_replace:
            target_count = self._target_counter(table)
            if target_count is not None and target_count < result.rows_loaded:
                raise RuntimeError(
                    f"Full Load incomplete for '{table.name}': target has "
                    f"{target_count} row(s) but {result.rows_loaded} were loaded "
                    "-- silent row loss detected on the target"
                )
        # ``conflicts`` are source rows that already existed on the target and
        # were skipped by ON CONFLICT DO NOTHING -- report them so completeness
        # treats the table as fully present (loaded + skipped == source) rather
        # than flagging a false mismatch when its rows pre-existed.
        return TableLoadResult(
            rows_loaded=result.rows_loaded,
            rows_skipped=result.conflicts,
            rows_quarantined=getattr(result, "quarantined", 0) or 0,
            quarantine_records=tuple(getattr(result, "quarantine_records", ()) or ()),
            index_failures=tuple(getattr(result, "index_failures", ()) or ()),
        )

    def _default_count_target_rows(self, table: TableDef) -> Optional[int]:
        """Return the target row count for ``table`` (None if it cannot be read).

        Read-only verification helper for the clean-replace completeness check.
        Returns ``None`` (verification skipped) when the count is unavailable, so
        an inability to verify never produces a false "row loss" failure.
        """
        from dsql_migrator.core.target_introspector import count_target_rows

        try:
            connector = DsqlConnector(
                self._inputs.target_config, aws_profile=self._inputs.aws_profile
            )
            counts = count_target_rows(
                [table.name], connection_factory=connector.connect
            )
            value = counts.get(table.name)
            return value if isinstance(value, int) else None
        except Exception:  # noqa: BLE001 - verification is best-effort, never fatal
            return None

    def _default_target_pk(self, table: TableDef) -> Optional[list[str]]:
        """Return the target table's real primary-key columns (None if unreadable).

        Read-only catalog probe, used on the append path to decide against the live
        target rather than assume its key. ``None`` means "could not determine", which
        the caller treats as unsafe -- never as agreement.
        """
        from dsql_migrator.core.target_introspector import target_primary_key_columns

        try:
            connector = DsqlConnector(
                self._inputs.target_config, aws_profile=self._inputs.aws_profile
            )
            return target_primary_key_columns(
                table.name, connection_factory=connector.connect
            )
        except Exception:  # noqa: BLE001 - unknown, decided by the caller
            return None

    def _dependent_view_ddls_for_replace(self) -> list[str]:
        """Converted view DDLs that reference a table being DROP+recreated.

        A view that SELECTs from a replaced table blocks that table's ``DROP``
        ("other objects depend on it"). Returns the CREATE-VIEW DDLs of exactly
        the views whose SQL names one of the replace tables, so the caller can drop
        them before recreating the tables and recreate them afterward. Empty on an
        append run, when CDC is coexisting (no DROP happens), or when no view
        references a replaced table.
        """
        if self._inputs.cdc_coexisting or not self._inputs.replace_tables:
            return []
        return _views_referencing(
            self._inputs.dependent_view_ddls, self._inputs.replace_tables
        )

    def predrop_dependent_views(self) -> None:
        """Drop views that depend on the replace tables (before recreating them).

        Run-level pre-pass for a "drop & reload" run: a view can depend on several
        tables loaded in parallel, so the drop must happen ONCE up front, not
        per-table. Idempotent (``DROP VIEW IF EXISTS``); recreated by
        :meth:`recreate_dependent_views` after the load. No-op on an append run.
        """
        ddls = self._dependent_view_ddls_for_replace()
        if not ddls:
            return
        from dsql_migrator.core.schema_applier import drop_object

        connect = self._view_connection_factory()
        for view_ddl in ddls:
            try:
                drop_object(view_ddl, connection_factory=connect)
            except Exception:  # noqa: BLE001 - best-effort; a real block surfaces on the table DROP
                _LOGGER.warning("Could not pre-drop dependent view", exc_info=True)

    def recreate_dependent_views(self) -> None:
        """Recreate the views dropped by :meth:`predrop_dependent_views`.

        Run-level post-pass: after the replace tables are reloaded, recreate the
        dependent views so the user's views survive a clean reload (they were only
        dropped to clear the table-DROP dependency). Uses the same idempotent
        DROP-then-CREATE as a table recreate (``recreate_table`` with no schema
        DDLs), so a re-run converges. No-op on an append run.
        """
        ddls = self._dependent_view_ddls_for_replace()
        if not ddls:
            return
        from dsql_migrator.core.schema_applier import recreate_table

        connect = self._view_connection_factory()
        for view_ddl in ddls:
            try:
                # DROP VIEW IF EXISTS + CREATE VIEW (no schema DDLs -- the view's
                # schema was already ensured when its tables were recreated).
                recreate_table([], view_ddl, connection_factory=connect)
            except Exception:  # noqa: BLE001 - best-effort; the tables/data are already loaded
                _LOGGER.warning("Could not recreate dependent view", exc_info=True)

    def apply_foreign_keys(self) -> tuple[int, int, int]:
        """Run-level POST-PASS: re-create preserved foreign keys after the data load.

        Returns ``(applied, skipped, failed)`` so the caller can surface the pass as
        a named step in the activity log; a deferred (CDC) or no-FK run returns
        ``(0, 0, 0)``.

        Aurora DSQL enforces foreign keys, but they must NOT exist during the load
        (the concurrent, sharded, multi-table bulk load has no parent-before-child
        ordering, so a child row can commit before its parent), so each preserved FK
        is applied HERE as its own single-DDL ``ADD CONSTRAINT`` (OC001 retry +
        reconnect, idempotent). Before adding a constraint an ORPHAN PRE-GATE counts
        child rows with no matching parent: an enforced ``ADD CONSTRAINT`` would fail
        on any orphan, so the FK is skipped with an actionable activity-log entry
        (table / FK / count) instead of an opaque ALTER error. A per-FK apply failure
        is logged and skipped, never failing the completed load.

        No-op for ANY CDC migration -- foreign keys are applied at cut over, after the
        stream drains, never while the sink streams out-of-order rows (it would
        dead-letter an FK violation, SQLSTATE 23503). ``is_cdc_migration`` (derived from
        ``migration_type == FULL_LOAD_AND_CDC``) is the reliable, source-agnostic marker
        and forces deferral for every CDC run. It exists because the two finer-grained
        signals each cover only one handoff model and MISS the MySQL Full-Load-first case:
          * ``cdc_coexisting`` -- MySQL's connectors-first model, where connectors stream
            DURING the load (SKIP_EXISTING). CDC is already live, so this is True mid-load.
            It is False for a MySQL Full-Load-FIRST -> binlog-watermark handoff (CDC started
            after the load), which is the common flow.
          * ``cdc_stack_name`` -- PostgreSQL's Full-Load-FIRST gapless handoff. Set only for
            a PostgreSQL Full Load + CDC run (None for a load-only run and always for
            MySQL). Without ``is_cdc_migration``, a MySQL Full-Load-first->CDC run had
            NEITHER signal set at load end, so the FK pass ran and the later stream
            dead-lettered (23503) -- the bug this flag fixes.
        Also a no-op when the user disabled foreign-key preservation
        (``foreign_key_ddls`` empty).
        """
        if (
            self._inputs.is_cdc_migration
            or self._inputs.cdc_coexisting
            or self._inputs.cdc_stack_name
        ):
            return (0, 0, 0)
        return apply_preserved_foreign_keys(
            self._inputs.table_conversions,
            self._view_connection_factory(),
            child_pk_columns=child_pk_columns_for(self._inputs.inventory),
        )

    def _view_connection_factory(self):
        """A fresh-DSQL-connection factory for the view pre-drop / recreate DDL."""
        connector = DsqlConnector(
            self._inputs.target_config, aws_profile=self._inputs.aws_profile
        )
        return connector.connect


def child_pk_columns_for(
    inventory: Optional[SourceInventory],
) -> dict[str, Optional[str]]:
    """Map each table name -> its single-column PK (or ``None``) from ``inventory``.

    Feeds :func:`apply_preserved_foreign_keys`' orphan pre-gate so a single-column-PK
    child is orphan-counted over bounded keyset pages. A composite/missing-PK table maps
    to ``None`` (single-scan fallback). Empty when there is no inventory.
    """
    if inventory is None:
        return {}
    return {table.name: single_pk_column(table) for table in inventory.tables}


def apply_preserved_foreign_keys(
    table_conversions: Mapping[str, TableConversion],
    connection_factory: Callable[[], Any],
    child_pk_columns: Optional[Mapping[str, Optional[str]]] = None,
) -> tuple[int, int, int]:
    """Re-create preserved foreign keys as post-load ``ADD CONSTRAINT``, orphan-gated.

    Shared by the Full Load run-level post-pass
    (:meth:`BatchedTableMigrator.apply_foreign_keys`) and the CDC **cut-over** action
    (which runs it once the stream has drained). Each preserved FK is applied as its
    own single-DDL ``ALTER TABLE ... ADD CONSTRAINT`` (OCC 40001 retry + reconnect,
    idempotent). An ORPHAN PRE-GATE counts child rows with no matching parent first:
    an enforced ``ADD CONSTRAINT`` would fail on any orphan, so that FK is skipped with
    an actionable activity-log entry (table / FK / count) instead of an opaque ALTER
    failure. Best-effort per FK: a failure is logged and skipped, never raised.

    ``child_pk_columns`` maps a child table name -> its single-column PK (or ``None``),
    so a large child's orphan pre-gate is counted over BOUNDED keyset pages rather than
    one unbounded scan that could exceed DSQL's ~300s transaction limit. Callers build it
    from the source inventory (:func:`child_pk_columns_for`); omitting it (or a table not
    in it) falls back to the single-scan count, so existing callers are unaffected.

    Returns ``(applied, skipped, failed)`` counts.
    """
    # Pair each rendered ADD-CONSTRAINT DDL with its FK metadata by CONSTRAINT NAME,
    # not by list position. The deterministic conversion builds both lists in
    # table.foreign_keys order (aligned), but an edited Schema-Conversion script
    # re-parses foreign_key_ddls from only the FK lines the user kept -- in their
    # edited order -- while preserved_foreign_keys stays the full deterministic list.
    # A positional zip would then pair a DDL with the WRONG FK's metadata, so the
    # orphan pre-gate would check the wrong columns and VALIDATE the wrong constraint.
    # Matching on the constraint name (the DDL's own key) keeps the pre-gate and the
    # VALIDATE target aligned to the constraint actually being added.
    pending: list[tuple[str, Optional[str], Optional[ForeignKeyDef], str]] = []
    for table_name, conv in table_conversions.items():
        by_name = {fk.name: fk for fk in conv.preserved_foreign_keys}
        for add_ddl in conv.foreign_key_ddls:
            constraint_name = _constraint_name_from_ddl(add_ddl)
            fk = by_name.get(constraint_name) if constraint_name is not None else None
            pending.append((table_name, constraint_name, fk, add_ddl))
    if not pending:
        return (0, 0, 0)

    probe = connection_factory()  # one read-only connection for the orphan pre-gate
    applied = skipped = failed = 0
    try:
        for table_name, constraint_name, fk, add_ddl in pending:
            target = f"{table_name}.{constraint_name or '?'}"
            # The child's single-column PK (when known) selects the bounded keyset-paged
            # orphan count; None (composite/missing PK, or no metadata supplied) uses the
            # single scan. Keeps the pre-gate from timing out on a very large child.
            pk_col = (child_pk_columns or {}).get(table_name)
            try:
                if fk is None:
                    # No metadata to key the orphan query on (an edited/renamed FK the
                    # re-parser could not match). Skip the pre-gate and let the ADD run:
                    # a NOT VALID add succeeds without scanning, and the best-effort
                    # VALIDATE below surfaces any pre-existing violation.
                    orphans = 0
                    _LOGGER.debug("No FK metadata for %s; skipping orphan pre-gate", target)
                else:
                    orphans = _count_orphans(probe, table_name, fk, pk_col)
            except Exception as exc:  # noqa: BLE001 - cannot verify -> do not risk a bad ADD
                if is_transient_connection_error(exc):
                    # The shared probe died mid-pass (class 08 / expired IAM token /
                    # DSQL session-max-duration). Reopen it once so a single drop does
                    # not cascade into a wall of false "apply manually" entries for the
                    # remaining FKs, then retry this FK's pre-check.
                    try:
                        probe.close()
                    except Exception:  # noqa: BLE001 - best-effort close of the dead probe
                        pass
                    try:
                        probe = connection_factory()
                        orphans = _count_orphans(probe, table_name, fk, pk_col)
                    except Exception:  # noqa: BLE001 - reconnect/re-check still failing
                        failed += 1
                        _LOGGER.warning(
                            "Orphan pre-check failed for FK %s (after reconnect)",
                            target, exc_info=True,
                        )
                        log_activity(
                            ActivityCategory.FULL_LOAD,
                            "foreign key not applied",
                            status=ActivityStatus.FAILURE,
                            target=target,
                            detail=(
                                "orphan pre-check failed; verify referential integrity "
                                "and apply this foreign key manually"
                            ),
                        )
                        continue
                else:
                    failed += 1
                    _LOGGER.warning("Orphan pre-check failed for FK %s", target, exc_info=True)
                    log_activity(
                        ActivityCategory.FULL_LOAD,
                        "foreign key not applied",
                        status=ActivityStatus.FAILURE,
                        target=target,
                        detail=(
                            "orphan pre-check failed; verify referential integrity and "
                            "apply this foreign key manually"
                        ),
                    )
                    continue
            if orphans:
                skipped += 1
                log_activity(
                    ActivityCategory.FULL_LOAD,
                    "foreign key not applied",
                    status=ActivityStatus.FAILURE,
                    target=target,
                    detail=(
                        f"{orphans} child row(s) reference a missing parent, so Aurora "
                        "DSQL cannot enforce this foreign key. Resolve the orphan rows "
                        "(often an un-replicated source cascade) and re-apply."
                    ),
                )
                continue
            try:
                # DSQL only accepts ADD CONSTRAINT ... NOT VALID (renders that way);
                # it enforces every NEW write immediately.
                apply_foreign_key(add_ddl, connection_factory=connection_factory)
                # Existing rows are consistent (the orphan pre-gate above was clean), so
                # mark the constraint validated via the async VALIDATE job. Uses the
                # DDL's own constraint name (not the metadata's) so it targets exactly
                # the constraint just added. Best-effort: if it fails the FK still
                # enforces all new writes.
                if constraint_name is not None:
                    try:
                        validate_foreign_key(
                            table_name, constraint_name,
                            connection_factory=connection_factory,
                        )
                    except Exception:  # noqa: BLE001 - async VALIDATE is best-effort
                        _LOGGER.warning(
                            "VALIDATE CONSTRAINT deferred for %s (FK still enforces new writes)",
                            target, exc_info=True,
                        )
                applied += 1
            except Exception:  # noqa: BLE001 - non-blocking: the data is already loaded
                failed += 1
                _LOGGER.warning("Could not apply FK %s", target, exc_info=True)
                log_activity(
                    ActivityCategory.FULL_LOAD,
                    "foreign key not applied",
                    status=ActivityStatus.FAILURE,
                    target=target,
                    detail="could not be created automatically; apply it manually",
                )
    finally:
        try:
            probe.close()
        except Exception:  # noqa: BLE001 - best-effort close
            pass
    log_activity(
        ActivityCategory.FULL_LOAD,
        "foreign keys applied",
        status=ActivityStatus.INFO,
        detail=(
            f"post-load foreign keys: {applied} applied, {skipped} skipped "
            f"(orphan rows), {failed} failed"
        ),
    )
    return (applied, skipped, failed)


# Matches ``ADD CONSTRAINT <name> FOREIGN KEY`` and captures <name>, either a
# double-quoted identifier (as rendered) or a bare word (if a user typed it unquoted).
_FK_CONSTRAINT_NAME_RE = re.compile(
    r'ADD\s+CONSTRAINT\s+("(?:[^"]|"")+"|[^\s("]+)\s+FOREIGN\s+KEY',
    re.IGNORECASE,
)


def _constraint_name_from_ddl(add_ddl: object) -> Optional[str]:
    """Return the (unquoted) constraint name from an ``ADD CONSTRAINT ... FOREIGN KEY`` DDL.

    The constraint name is the reliable key that pairs a rendered FK DDL with its
    metadata (for the orphan pre-gate) and with the ``VALIDATE CONSTRAINT`` target --
    more robust than positional pairing, which breaks when a user deletes/reorders FK
    lines in the edited Schema-Conversion script. Returns ``None`` if the statement is
    not a recognizable ADD-CONSTRAINT-FOREIGN-KEY (the caller then applies it without a
    pre-gate rather than mis-keying another FK's metadata).
    """
    match = _FK_CONSTRAINT_NAME_RE.search(str(add_ddl))
    if not match:
        return None
    token = match.group(1)
    if len(token) >= 2 and token.startswith('"') and token.endswith('"'):
        return token[1:-1].replace('""', '"')
    return token


# Rows scanned per keyset page in the orphan pre-gate's paged path. Bounded so a
# billion-row child never runs one orphan scan past DSQL's ~300s transaction limit.
_ORPHAN_PAGE_SIZE = 5000


def _count_orphans(
    connection: Any,
    child_table: str,
    fk: ForeignKeyDef,
    pk_column: Optional[str] = None,
    page_size: int = _ORPHAN_PAGE_SIZE,
) -> int:
    """Count child rows whose foreign key points to a missing parent (target).

    Read-only pre-gate for the post-load ``ADD CONSTRAINT``: an enforced foreign key
    cannot be created while orphan rows exist, so this turns an opaque ALTER failure
    into an actionable per-FK count. Reuses the Validation orphan queries
    (:mod:`~dsql_migrator.core.validation_sql`).

    For a single-column-PK child (``pk_column`` given) the count is accumulated over
    BOUNDED keyset pages so a very large child never runs the orphan scan in one
    transaction (a single ``COUNT(*) ... NOT EXISTS`` would exceed DSQL's ~300s
    transaction limit) -- exactly as the count/checksum keyset pagers do. When
    ``pk_column`` is ``None`` (composite/missing PK) it falls back to the single scan
    (:func:`~dsql_migrator.core.validation_sql.build_orphan_count_sql`).

    The count is wrapped in OCC retry: on Aurora DSQL even a read can raise a
    serialization failure (OC001/40001) when the target is being written concurrently
    (e.g. a live CDC sink at cut over), and a transient conflict must not be mistaken
    for "orphan pre-check failed" and skip an otherwise-applicable foreign key. On a
    conflict the whole (idempotent, read-only) count re-runs from the first page.
    """
    from dsql_migrator.core.occ import with_occ_retry

    def _run() -> int:
        # A prior OC001 leaves the connection's transaction aborted; clear it before the
        # retry so the re-run starts clean (a no-op / harmless on an autocommit conn).
        try:
            connection.rollback()
        except Exception:  # noqa: BLE001 - best-effort
            pass
        if pk_column is not None:
            return _count_orphans_keyset(
                connection, child_table, fk, pk_column, page_size
            )
        cursor = connection.cursor()
        try:
            cursor.execute(build_orphan_count_sql(child_table, fk))
            row = cursor.fetchone()
        finally:
            try:
                cursor.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass
        return int(row[0]) if row and row[0] is not None else 0

    return with_occ_retry()(_run)()


def _count_orphans_keyset(
    connection: Any,
    child_table: str,
    fk: ForeignKeyDef,
    pk_column: str,
    page_size: int,
) -> int:
    """Accumulate the orphan count over bounded keyset pages (single-column-PK child).

    Each page reports ``(orphan_sub_count, last_pk, row_count)`` over an UNFILTERED PK
    window (the orphan predicate is a ``COUNT(*) FILTER``), so ``last_pk`` advances the
    keyset over non-orphan rows too and no PK range is skipped. Sub-counts fold into a
    Python integer; the loop stops when a page returns fewer than ``page_size`` rows.
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
            try:
                cursor.close()
            except Exception:  # noqa: BLE001 - best-effort close
                pass
        if row is None:
            return total
        sub_count, last_pk, count = row[0], row[1], row[2]
        total += int(sub_count) if sub_count is not None else 0
        if count is None or int(count) < page_size:
            return total
        last = last_pk


def default_migrator_factory(inputs: DataMigrationInputs) -> DataMigrator:
    """Default :data:`MigratorFactory`: build an in-process migrator (no binary)."""
    return BatchedTableMigrator(inputs)
