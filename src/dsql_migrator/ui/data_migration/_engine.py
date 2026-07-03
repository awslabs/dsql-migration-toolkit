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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Mapping, NamedTuple, Optional, Protocol, Sequence, Union

from dsql_migrator.config import SecretValue, load_config
from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.exporter import ExportCancelled, TableExporter
from dsql_migrator.core.job_manager import JobHandle
from dsql_migrator.core.batched_import import (
    BatchedImporter,
    BatchedImportOptions,
    OnConflictMode,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.converter import (
    SchemaConverter,
    SchemaConvertOptions,
    TableConversion,
    parse_target_column_types,
)
from dsql_migrator.core.schema_applier import recreate_table
from dsql_migrator.core.models import (
    ChunkState,
    DataErrorRecord,
    MigrationJob,
    SourceConnectionConfig,
    SourceInventory,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
    Watermark,
)
from dsql_migrator.core.watermark import WatermarkCapturer
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
    job: MigrationJob, chunk_id: str, rows_loaded: int, rows_skipped: int = 0
) -> None:
    """Mark ``chunk_id`` ``DONE``; record loaded/skipped rows and stamp finish."""
    chunk = _find_chunk(job, chunk_id)
    if chunk is not None:
        chunk.status = "DONE"
        chunk.rows_loaded = rows_loaded
        chunk.rows_skipped = rows_skipped
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
    current = next(
        (chunk.chunk_id for chunk in job.chunks if chunk.status == "IN_PROGRESS"),
        None,
    )
    if current is not None:
        return f"Exporting and loading {current} ({done}/{total} tables done)…"
    return f"Exporting and loading tables ({done}/{total} done)…"


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
            migrator.migrate_table(
                table, on_rows=_on_rows, should_cancel=lambda: handle.cancelled
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
        message = f"{type(exc).__name__}: {exc}"
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
        skipped_note = (
            f", {outcome.rows_skipped:,} already on target (skipped)"
            if outcome.rows_skipped
            else ""
        )
        quarantined = getattr(outcome, "rows_quarantined", 0) or 0
        quarantine_note = f", {quarantined:,} quarantined" if quarantined else ""
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
                f"{skipped_note}{quarantine_note}"
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
            lambda job, n=name, r=outcome.rows_loaded, s=outcome.rows_skipped: (
                _complete_chunk(job, n, r, s)
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
    table_parallelism = load_config().full_load_table_parallelism
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
                # _migrate_one_table never raises (per-table errors are caught
                # inside); it returns how the table's load ended.
                _tally(future.result())

    if handle.cancelled:
        handle.update(_fail_unfinished_chunks)
    return _RunCounts(real_failed, quarantined)


def _finalize_run(
    handle: JobHandle,
    job_id: str,
    table_names: Sequence[str],
    counts: _RunCounts,
    error_log: ErrorLogStore,
    *,
    accept_quarantined_rows: bool,
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
        return
    log_activity(
        ActivityCategory.FULL_LOAD,
        "run incomplete",
        status=ActivityStatus.FAILURE,
        detail=(
            f"{incomplete} of {total} table(s) did not fully load"
            + (f"; {quarantined_rows} row(s) quarantined" if quarantined_rows else "")
        ),
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


def run_full_load(
    handle: JobHandle,
    tables: Sequence[TableDef],
    *,
    migrator: DataMigrator,
    error_log: ErrorLogStore,
    accept_quarantined_rows: bool = False,
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

    watermark = migrator.capture_watermark(tables)
    handle.update(lambda job: setattr(job, "watermark", watermark))

    # On a "drop & reload" run, drop views that depend on the replaced tables
    # BEFORE the per-table DROP+recreate (a view can span several tables loaded in
    # parallel, so this is a run-level pre-pass), then recreate them after.
    _predrop_dependent_views(migrator)
    counts = _migrate_tables_in_parallel(handle, job_id, tables, migrator, error_log)
    _recreate_dependent_views(migrator)
    _finalize_run(
        handle,
        job_id,
        table_names,
        counts,
        error_log,
        accept_quarantined_rows=accept_quarantined_rows,
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
    if watermark is not None:
        handle.update(lambda job: setattr(job, "watermark", watermark))

    # Same run-level view pre-drop / recreate as run_full_load, so a retry that
    # DROP+recreates a table whose view dependency blocked the first attempt now
    # succeeds instead of silently skip-loading over stale rows.
    _predrop_dependent_views(migrator)
    counts = _migrate_tables_in_parallel(
        handle, job_id, tables_to_retry, migrator, error_log
    )
    _recreate_dependent_views(migrator)
    _finalize_run(
        handle,
        job_id,
        list(retry_names),
        counts,
        error_log,
        accept_quarantined_rows=accept_quarantined_rows,
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
    return BatchedImporter(options, connection_factory=connector.connect)


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
            # session): re-derive deterministically.
            conversion = SchemaConverter().convert_table(table, SchemaConvertOptions())
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
            )
        )
        self._watermark_capturer = watermark_capturer or WatermarkCapturer(
            engine_factory=make_source_engine_factory(inputs.source_password)
        )
        self._importer_factory = importer_factory
        self._table_recreator = table_recreator or _default_table_recreator(inputs)
        # Post-load completeness verifier for clean-replace loads (injectable for
        # tests). Defaults to a read-only target COUNT(*).
        self._target_counter = target_counter or self._default_count_target_rows

    def capture_watermark(self, tables: Sequence[TableDef]) -> Watermark:
        """Capture the export consistency point for the selected ``tables``.

        Only the tables being migrated are counted within the snapshot, so a
        small selection is not blocked by snapshot counts over large, unrelated
        source tables (read-only).
        """
        table_names = [table.name for table in tables]
        return self._watermark_capturer.capture(
            self._inputs.source_config, table_names
        )

    def migrate_table(
        self,
        table: TableDef,
        *,
        on_rows: Optional[Callable[[int, int], None]] = None,
        should_cancel: Optional[Callable[[], bool]] = None,
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
        applied = self._inputs.table_conversions.get(table.name)
        target_types = (
            parse_target_column_types(applied.target_ddl)
            if applied is not None
            else None
        )
        rows = self._exporter.stream_converted_rows(
            self._inputs.source_config,
            table,
            should_cancel=should_cancel,
            target_types=target_types,
        )
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
        if is_replace:
            index_ddls = self._table_recreator(table)
        if is_replace:
            # Clean load into a freshly-emptied target: plain INSERT (no ON
            # CONFLICT) -- DSQL never silently drops a non-conflicting row.
            load_on_conflict: Optional[OnConflictMode] = OnConflictMode.NONE
        else:
            # Idempotent load into existing/CDC-fed data: insert only the missing
            # keys (never overwrite a newer CDC row, never use the DSQL-unsafe
            # multi-row ON CONFLICT that silently drops rows). Works for single- or
            # composite-column keys.
            load_on_conflict = OnConflictMode.SKIP_EXISTING
        importer = self._importer_factory(self._inputs)
        try:
            result = importer.import_rows(
                rows,
                table,
                index_ddls=index_ddls,
                on_batch_loaded=on_rows,
                should_cancel=should_cancel,
                on_conflict=load_on_conflict,
            )
        except ExportCancelled as exc:
            # A cooperative stop interrupted the source read between pages (the
            # importer was pulling rows). Treat it exactly like a batch-boundary
            # stop: incomplete + retryable, not a data error.
            raise _FullLoadStopped(table.name) from exc
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

    def _view_connection_factory(self):
        """A fresh-DSQL-connection factory for the view pre-drop / recreate DDL."""
        connector = DsqlConnector(
            self._inputs.target_config, aws_profile=self._inputs.aws_profile
        )
        return connector.connect


def default_migrator_factory(inputs: DataMigrationInputs) -> DataMigrator:
    """Default :data:`MigratorFactory`: build an in-process migrator (no binary)."""
    return BatchedTableMigrator(inputs)
