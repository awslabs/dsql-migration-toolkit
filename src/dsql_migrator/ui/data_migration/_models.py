# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Pure view-models, formatters, and enums for the Data Migration screen.

Progress aggregation, the unified Full Load status/table views, prerequisite
grouping, watermark formatting, and the CDC handling read-models live here,
split out of the screen module so they can be unit tested without NiceGUI. The
screen package re-exports every name from this module, so the public import
surface is unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Iterable, Mapping, Optional, Sequence

from dsql_migrator.core.assessor import (
    _OVERSIZED_LOB_BASES,
    _base_type,
    is_pg_oversized_lob_type,
    pg_oversized_lob_column_names,
)
from dsql_migrator.core.models import (
    ChunkState,
    ErrorLogSummary,
    LoadKind,
    LoadStatusView,
    MigrationJob,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
    SourceInventory,
    SourceType,
    TableStatusRow,
    Watermark,
)

# Text shown when a watermark field could not be captured (binary logging
# disabled, or SHOW MASTER STATUS restricted on RDS/Aurora). The export is still
# valid; only the optional coordinate is missing (Requirement 5.7).
_UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# Progress aggregation (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MigrationProgress:
    """A snapshot summary of a migration job's per-table progress (Req 8.3)."""

    total_tables: int
    done_tables: int
    failed_tables: int
    in_progress_tables: int
    pending_tables: int
    rows_loaded: int
    progress_pct: float


def summarize_progress(job: MigrationJob) -> MigrationProgress:
    """Summarize ``job``'s chunk states into table counts and rows loaded."""
    chunks = job.chunks
    return MigrationProgress(
        total_tables=len(chunks),
        done_tables=sum(1 for chunk in chunks if chunk.status == "DONE"),
        failed_tables=sum(1 for chunk in chunks if chunk.status == "FAILED"),
        in_progress_tables=sum(
            1 for chunk in chunks if chunk.status == "IN_PROGRESS"
        ),
        pending_tables=sum(1 for chunk in chunks if chunk.status == "PENDING"),
        rows_loaded=sum(chunk.rows_loaded for chunk in chunks),
        progress_pct=job.progress_pct,
    )


# ---------------------------------------------------------------------------
# Unified monitoring view: Full Load provider (NiceGUI-agnostic) -- Req 13.1
# ---------------------------------------------------------------------------


def build_full_load_status_view(
    job: MigrationJob, error_summary: Optional[ErrorLogSummary] = None
) -> LoadStatusView:
    """Map a Full Load job + error summary to the unified :class:`LoadStatusView`.

    This is the Full Load *provider* for the unified monitoring component
    (Req 13.1): it reuses :func:`summarize_progress` for the aggregates and the
    single :class:`ErrorLogSummary` for per-table error counts (Req 13.2/13.4),
    so Full Load and CDC render through one component without recomputation.
    """
    progress = summarize_progress(job)
    by_table = error_summary.errors_by_table if error_summary else {}
    rows = [
        TableStatusRow(
            table=chunk.chunk_id,
            state=chunk.status,
            rows_loaded=chunk.rows_loaded,
            errors=by_table.get(chunk.chunk_id, 0),
        )
        for chunk in job.chunks
    ]
    return LoadStatusView(
        kind=LoadKind.FULL_LOAD,
        tables=rows,
        progress_pct=progress.progress_pct,
        tables_done=progress.done_tables,
        tables_failed=progress.failed_tables,
        error_summary=error_summary,
    )


# Fraction by which a scan-free source ESTIMATE is assumed to be able to differ from
# the true count before a shortfall is treated as a real problem. The source figures
# on both the Full Load and CDC status views come from
# ``information_schema.TABLE_ROWS``, which InnoDB derives from index sampling: it is
# commonly several percent off (observed ~8.5% on a 3M-row table) in EITHER direction.
# A generous 20% keeps "rows missing"/"incomplete" for genuine data loss instead of
# firing on statistics noise; exact counts are compared with no tolerance at all.
# Exactness is Validation's (step 4) job -- it runs a real COUNT(*) + reconciliation.
_ESTIMATE_TOLERANCE = 0.20

# When a measured end-to-end replication lag means the stream's leading edge has NOT caught
# up. A live stream is always slightly behind, so this must not be near zero or every
# healthy table reads as behind. 30s matches the threshold the connector-status chip already
# uses to call a lag "elevated" (``lag_warn_seconds`` in :func:`cdc_connector_status`), so
# the per-table verdict and the pipeline chip agree instead of contradicting each other.
_LAG_BEHIND_MS = 30_000


@dataclass(frozen=True)
class FullLoadTableRow:
    """Live per-table Full Load detail for the progress table.

    ``expected_rows`` is the source snapshot count captured on the watermark
    (Property 11); comparing it to ``rows_loaded`` tells whether a finished
    table loaded completely. ``attempts`` surfaces retry activity.
    """

    table: str
    state: str
    rows_loaded: int
    expected_rows: Optional[int]
    attempts: int
    errors: int
    # Source rows that already existed on the target and were skipped by the
    # idempotent load (ON CONFLICT DO NOTHING). Counted toward completeness so a
    # table whose rows pre-existed is not falsely reported as a row-count
    # mismatch (loaded + skipped == source).
    rows_skipped: int = 0
    # Rows PERMANENTLY DROPPED for this table (a non-retryable per-row error, e.g. a
    # value over DSQL's ~1 MiB per-value limit). A real data gap -- unlike
    # ``rows_skipped``, these rows are NOT on the target. Held here so completeness
    # can distinguish a confirmed loss from the source ESTIMATE's sampling noise; the
    # estimate tolerance previously absorbed the shortfall and reported a run that
    # dropped rows as "loaded every source row".
    rows_quarantined: int = 0
    # The most recent failure reason for this table (cause), shown inline so the
    # user can see why it failed without downloading the log. None when no error.
    error_message: Optional[str] = None
    # Per-table wall-clock timing: start (when IN_PROGRESS) and finish (terminal),
    # used to show an ETA while loading and the total elapsed once finished.
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @property
    def rows_present(self) -> int:
        """Rows now present on the target for this table: newly loaded + skipped."""
        return self.rows_loaded + self.rows_skipped

    @property
    def progress_pct(self) -> Optional[float]:
        """Per-table load progress as a percent, or ``None`` when unknown.

        A table the loader has FINISHED is 100% by definition: the export streams
        the table by PK keyset to exhaustion, so ``DONE`` means every source row was
        read and written -- it does not depend on ``expected_rows`` agreeing. This
        matters because ``expected_rows`` is the watermark's scan-free
        ``information_schema`` ESTIMATE, which InnoDB derives from index sampling and
        can OVERCOUNT: dividing by it would leave a fully-loaded table stuck at e.g.
        "91%" and imply rows were lost.

        While the table is still loading, the estimate is the only baseline available,
        so it drives the in-flight percentage (capped at 100 for the progress bar);
        rows that already existed on the target (skipped) count as progress.
        """
        if self.state == "DONE":
            return 100.0
        if self.expected_rows is None or self.expected_rows <= 0:
            return None
        return min(100.0, round(self.rows_present / self.expected_rows * 100.0, 1))

    @property
    def complete(self) -> Optional[bool]:
        """Whether a finished table loaded everything the source held.

        ``None`` until the table is ``DONE``. Once ``DONE``, the loader ran the
        table's PK keyset stream to exhaustion, so completeness is established by
        that fact -- NOT by matching the watermark's ``expected_rows``, which is a
        scan-free ``information_schema`` ESTIMATE (InnoDB index sampling, routinely
        several percent off in EITHER direction). Comparing against it used to make
        a fully-loaded table report incomplete whenever the estimate happened to
        overcount.

        So this reports ``True`` for a ``DONE`` table unless the shortfall against
        the estimate is too large to be sampling error, in which case the estimate is
        worth heeding as a signal that something really did not load. The exact
        guarantee is Validation (step 4), which runs a real ``COUNT(*)`` and
        record-level reconciliation.
        """
        if self.state != "DONE":
            return None
        # A quarantined row is a CONFIRMED loss, not estimate noise: the loader saw the
        # row, could not write it, and dropped it permanently. It must never be absorbed
        # by the sampling tolerance below -- that is exactly how a run that dropped a row
        # still reported "loaded every source row" beside its own amber warning saying
        # the row was permanently dropped.
        if self.rows_quarantined > 0:
            return False
        if self.expected_rows is None or self.expected_rows <= 0:
            return True
        shortfall = self.expected_rows - self.rows_present
        if shortfall <= 0:
            return True
        return shortfall <= self.expected_rows * _ESTIMATE_TOLERANCE

    @property
    def expected_exceeded_pct(self) -> Optional[float]:
        """How far ``rows_present`` EXCEEDS the source estimate, as a percent.

        The progress bar caps at 100%, which hides the (common) case of loading more
        rows than the scan-free estimate predicted -- a normal undercount, not a
        problem. Surfacing it lets the UI explain the "target > source" arithmetic a
        reader sees in the Rows column. ``None`` when there is no estimate or the
        loaded count did not exceed it.
        """
        if self.expected_rows is None or self.expected_rows <= 0:
            return None
        excess = self.rows_present - self.expected_rows
        if excess <= 0:
            return None
        return round(excess / self.expected_rows * 100.0, 1)


def build_full_load_table_rows(
    job: MigrationJob,
    error_summary: Optional[ErrorLogSummary] = None,
    error_messages: Optional[dict[str, str]] = None,
) -> list[FullLoadTableRow]:
    """Build the live per-table Full Load rows from a job and its error summary.

    Reads each chunk's state/rows/attempts and pairs it with the source snapshot
    count from the job watermark (for the completeness check), the per-table
    error count, and the latest per-table error message (the failure cause) from
    the single error log. NiceGUI-agnostic for unit testing.
    """
    by_table = error_summary.errors_by_table if error_summary else {}
    messages = error_messages or {}
    expected = (
        job.watermark.table_row_counts if job.watermark is not None else {}
    )
    return [
        FullLoadTableRow(
            table=chunk.chunk_id,
            state=chunk.status,
            rows_loaded=chunk.rows_loaded,
            expected_rows=expected.get(chunk.chunk_id),
            attempts=chunk.attempts,
            errors=by_table.get(chunk.chunk_id, 0),
            rows_skipped=chunk.rows_skipped,
            rows_quarantined=getattr(chunk, "rows_quarantined", 0) or 0,
            error_message=messages.get(chunk.chunk_id),
            started_at=chunk.started_at,
            finished_at=chunk.finished_at,
        )
        for chunk in job.chunks
    ]


def quarantined_rows_by_table(job: Optional[MigrationJob]) -> dict[str, int]:
    """Return ``{table: rows permanently dropped}`` for a Full Load job, dropping zeros.

    Lets Validation ATTRIBUTE a target deficit to rows the migration is known to have
    dropped, instead of the operator cross-checking the Full Load error log by hand
    (which is what the manual used to instruct). Empty for a job that dropped nothing,
    or when there is no job -- e.g. a reconnected session, where the counts are not
    persisted; Validation then reports the deficit as unexplained, which is the honest
    answer rather than a guess.
    """
    if job is None:
        return {}
    counts: dict[str, int] = {}
    for chunk in job.chunks:
        dropped = getattr(chunk, "rows_quarantined", 0) or 0
        if dropped:
            counts[chunk.chunk_id] = dropped
    return counts


def failed_table_names(job: MigrationJob) -> list[str]:
    """Return the names of the tables whose Full Load chunk is ``FAILED``."""
    return [chunk.chunk_id for chunk in job.chunks if chunk.status == "FAILED"]


def unsettled_table_names(job: MigrationJob) -> list[str]:
    """Return the tables that did NOT finish -- ``FAILED`` **or** still ``PENDING``.

    A run can end terminally (``FAILED``/``CANCELLED``) with tables left
    ``PENDING`` rather than ``FAILED`` -- e.g. a fatal error (or a run-level
    pre-pass crash) that aborted the job *before* those tables were even
    attempted, or a cooperative stop that left queued tables untouched. Those
    tables are unfinished and must be retryable, but :func:`failed_table_names`
    (``FAILED`` only) misses them, which would strand them ``PENDING`` with no way
    to resume short of re-running everything. This is the recovery set for a
    terminated run: every chunk not ``DONE`` (idempotent to re-run; ``DONE``
    tables are kept).
    """
    return [chunk.chunk_id for chunk in job.chunks if chunk.status != "DONE"]


def untick_can_settle(job, names: "Optional[Sequence[str]]") -> bool:
    """Could unticking a selected table settle a FAILED Full Load step (G16)? True when a
    table of the current view (``names``; ``None`` = the whole record) did not finish or
    dropped rows. A run-level failure with every one of them loaded -- a post-load pass
    raised, an app restart or a Stop after the last table -- stays Failed whatever is
    unticked (:func:`~dsql_migrator.ui.data_migration._full_load_ui.full_load_step_for_scope`),
    so offering the untick there would be a dead end. Pure, O(tables)."""
    wanted = None if names is None else set(names)
    return any(
        chunk.status != "DONE" or (getattr(chunk, "rows_quarantined", 0) or 0) > 0
        for chunk in getattr(job, "chunks", None) or ()
        if wanted is None or chunk.chunk_id in wanted
    )


def recreated_unfinished(job: MigrationJob) -> list[str]:
    """Tables the tool DROP+recreated in this job lineage that did not finish.

    Such a table holds no rows (or a partial load) and its overlay must be re-run
    before it loads again, so a retry re-runs it as a recreate. De-duplicated, in
    recorded order; an entry with no chunk in this job is not unfinished work here.
    """
    status = {chunk.chunk_id: chunk.status for chunk in job.chunks}
    return [
        name
        for name in dict.fromkeys(getattr(job, "recreated_tables", None) or ())
        if name in status and status[name] != "DONE"
    ]


# ---------------------------------------------------------------------------
# G12: a Full load only table set that changed after the Full Load (NiceGUI-agnostic)
# G16: a Full load + CDC table set narrowed after its Full Load, before CDC starts
# G17: ... and grown, through a guarded Retry that CDC then captures from its start point
# ---------------------------------------------------------------------------
#
# The job record keeps its whole history (its watermark is extended only by a guarded Full
# load + CDC addition, ``Watermark.cdc_added_tables``); every surface that reads chunks
# reads a SCOPED VIEW of it instead -- the identical object whenever nothing changed, so
# CDC only, an untouched selection or an unchanged set render exactly as before.

SCOPE_NOTICE_HEADER = "Table set changed after the last Full Load"
SCOPE_ADDED_REASON = "Not loaded yet — selected after the last Full Load."
# G17: the per-table reasons of a Full load + CDC table selected after its Full Load.
SCOPE_ADDED_REASON_FL_CDC = (
    "Not loaded under the Full Load's start point yet — selected after the last Full Load. "
    "Its load first checks that CDC can capture it with no gap; if Aurora DSQL already "
    "holds rows in it, they are dropped and reloaded."
)
SCOPE_ADDED_REASON_PG_SUFFIX = " On PostgreSQL it is first added to CDC's publication on the source."
SCOPE_ADDED_QUARANTINED_REASON = (
    "Loaded with quarantined rows, so CDC does not capture it yet: a table added after the "
    "Full Load is captured only after a load with none. Fix the source values (or exclude "
    "the column with Exclude column & reload), then retry it, or untick it."
)
CDC_NOT_ADDABLE_REASON = (
    "Cannot be added by Retry — this Full Load recorded no start-point definition for it, "
    "so CDC could not decode its binary-log changes since then. Re-run all tables, or "
    "untick it."
)


def _names_text(names: "Sequence[str]") -> str:
    from dsql_migrator.ui.cdc_dlq_gate import bounded_table_names

    return bounded_table_names(list(names))


def cdc_not_addable_refusal(names: "Sequence[str]") -> str:
    """The Retry refusal for Full load + CDC tables a guarded load cannot add (G17, MySQL:
    no start-point definition recorded with the watermark)."""
    one = len(names) == 1
    it = "it" if one else "them"
    return (
        f"{_names_text(names)} cannot be added to CDC by Retry: this Full Load recorded no "
        f"start-point definition for {it}, so CDC could not decode "
        f"{'its' if one else 'their'} binary-log changes since then. Re-run all tables "
        "loads every selected table from a fresh start point, or untick "
        f"{it} in the table list."
    )


def cdc_covered_names(watermark) -> "Optional[frozenset[str]]":
    """The tables CDC may capture from a Full Load ``watermark``'s start point, or ``None``
    when it carries no per-table counts: the run's tables (its count keys) plus the tables a
    GUARDED Full load + CDC load added since (``cdc_added_tables``, G17). Validation's scope
    gap reads the run's ORIGINAL keys instead. Pure, O(tables)."""
    counts = getattr(watermark, "table_row_counts", None) if watermark is not None else None
    if not counts:
        return None
    added = getattr(watermark, "cdc_added_tables", None) or {}
    return frozenset(counts) | frozenset(added) if added else frozenset(counts)


def cdc_addition_has_start_point(watermark, name: str, engine) -> bool:
    """Can a table selected after this Full Load be added to CDC by a guarded Retry at all?
    (G17; render-time, no source I/O.)

    MySQL (and an unknown engine): only when the watermark recorded a comparable start-point
    definition for it (a baseline read with the original watermark): Debezium decodes its
    binary log from that start point with today's definition, so nothing else proves the
    two equal. PostgreSQL: always here -- its guarded load decides (the publication add and
    the wait). Pure."""
    if engine is SourceType.POSTGRES:
        return True
    from dsql_migrator.core.cdc_catalog_drift import start_point_comparable

    recorded = getattr(watermark, "table_fingerprints", None) or {}
    return start_point_comparable(recorded.get(name))


def _stored_selection(migration_state) -> "list[str]":
    selection = getattr(migration_state, "selection", None)
    return list(getattr(selection, "selected_tables", None) or ())


def cdc_capture_names(migration_state, watermark) -> "Optional[frozenset[str]]":
    """The tables CDC captures from a Full Load ``watermark`` (G16), or ``None`` when it
    carries no per-table counts (the selection / deployed-list fallbacks then apply).

    The watermark's covered tables (:func:`cdc_covered_names`: the set that run started
    with, plus any table a guarded Full load + CDC load added since, G17 -- each has a
    gapless start point there) narrowed to the STORED selection when that is touched: a
    table unticked after the load (Full load + CDC before CDC starts, or a Full load only set
    switched to a CDC type) is never captured, and a table selected since is captured only
    once its guarded load added it to the coverage. An untouched selection is the covered
    set (a default that drifted is no decision); a touched, EMPTY one captures nothing.
    Never resolves through ``TableSelector``, so it cannot raise. When nothing was narrowed
    (the selection holds every covered table) this is exactly the covered set, as before.
    Pure, O(tables).
    """
    covered = cdc_covered_names(watermark)
    if covered is None:
        return None
    if not getattr(migration_state, "selection_touched", False):
        return covered
    return covered & frozenset(_stored_selection(migration_state))


def keep_unlisted_covered(
    stored: "Sequence[str]", listed: "Sequence[str]", covered: "Iterable[str]"
) -> "list[str]":
    """The stored names a tree or popup write keeps although it cannot list them (G16):
    covered by the Full Load watermark (:func:`cdc_covered_names`, its guarded additions
    included, G17) but not in ``listed`` (the migratable universe the tree and the rules see
    -- e.g. a loaded table a stale target catalog no longer counts).
    A write stores ``(listed AND ticked)``, which would silently drop such a table, and CDC
    captures the covered tables still STORED. Stored order. Pure."""
    listed_set = set(listed or ())
    covered_set = set(covered or ())
    return [n for n in dict.fromkeys(stored or ()) if n in covered_set and n not in listed_set]


def narrowed_record_names(migration_state, job) -> "Optional[list[str]]":
    """The Full Load record's tables still selected, when a Full load + CDC set was narrowed
    after its load (G16); ``None`` when the views read the record as is.

    ``None`` unless the selection is touched, the record carries per-table watermark counts,
    and the STORED selection keeps some but not all of the record's tables (an emptied
    selection reads the record as is: nothing can be retried or captured then anyway). Names
    only record tables, in record order: the REMOVAL half of the Full load + CDC view, which
    outlives the start of CDC. The tables selected after the load (G17) are added by
    :func:`full_load_scope_names` only while CDC never started. A selection ticked back
    to every record table is ``None`` again (:func:`whole_record_selected`: the step is then
    judged on the record itself). Pure, O(tables)."""
    if job is None or not getattr(migration_state, "selection_touched", False):
        return None
    if cdc_covered_names(getattr(job, "watermark", None)) is None:
        return None
    stored = set(_stored_selection(migration_state))
    record = [c.chunk_id for c in getattr(job, "chunks", None) or ()]
    kept = [n for n in record if n in stored]
    if not kept or len(kept) == len(record):
        return None
    return kept


def whole_record_selected(migration_state, job) -> bool:
    """Does a touched Full load + CDC selection hold every table of the Full Load record
    (G16)? Nothing is narrowed then (:func:`narrowed_record_names` is ``None``), and a step
    an untick turned DONE must be judged on the record itself again -- a table ticked back
    whose load failed makes it Failed, as in Full load only (G12). Pure, O(tables)."""
    if getattr(migration_state, "migration_type", None) is not MigrationType.FULL_LOAD_AND_CDC:
        return False
    if job is None or not getattr(migration_state, "selection_touched", False):
        return False
    record = [c.chunk_id for c in getattr(job, "chunks", None) or ()]
    if not record:
        return False
    stored = set(_stored_selection(migration_state))
    return all(n in stored for n in record)


def capture_load_complete(job, captured: "Iterable[str]") -> bool:
    """Did every CAPTURED table load completely although the run as a whole did not? Pure.

    True only for a FAILED or CANCELLED record whose unfinished work lies entirely in tables
    CDC no longer captures (G16): every captured table has a DONE chunk with no quarantined
    row, and some chunk outside the capture is not DONE or quarantined rows. A run-level
    failure (every chunk DONE), any gap in a captured table, or an empty capture stays False
    -- the PostgreSQL slot is then still suppressed (a lossless re-snapshot, the safe side).
    """
    if job is None or getattr(job, "status", None) not in ("FAILED", "CANCELLED"):
        return False
    names = set(captured or ())
    if not names:
        return False
    by_name = {c.chunk_id: c for c in getattr(job, "chunks", None) or ()}

    def _complete(chunk) -> bool:
        return chunk.status == "DONE" and not (getattr(chunk, "rows_quarantined", 0) or 0)

    if any(n not in by_name or not _complete(by_name[n]) for n in names):
        return False
    return any(not _complete(c) for n, c in by_name.items() if n not in names)


# ``full_load_scope_names``'s default: compute the pending additions itself.
_PENDING_UNSET = object()


def full_load_scope_names(
    migration_state, job_manager, selected, *, job=None, pending=_PENDING_UNSET
) -> "Optional[list[str]]":
    """The selection the Full Load views follow, or ``None`` (they read the record as is).

    Full load only (G12): a list only with a TOUCHED, non-empty selection, while
    :func:`table_set_editable_after_load` holds. The touched rule keeps an untouched
    Schema Conversion default that drifted from being read as a change: the prerequisite
    run pins a touched set before any load, so a real migration always has one.

    Full load + CDC: the record's tables still in the STORED selection
    (:func:`narrowed_record_names`, G16) -- the removal half, NOT tied to the predicate, so
    once CDC starts (the set is then locked again) a table unticked before it never comes
    back as a red error or a Retry / Reload row -- followed, while CDC never started for the
    migration, by the tables selected since that the watermark does not cover yet (G17:
    ``_cdc_monitoring.fl_cdc_pending_additions`` over ``selected``, the resolved selection;
    ``pending`` passes them when the caller computed them), in selection order: each is
    unfinished until its guarded load extends the coverage. ``job`` is the Full Load record
    when the caller already holds it (a read is a deep copy of the record); ``None`` reads
    it here, and only for a touched selection.
    """
    mtype = getattr(migration_state, "migration_type", None)
    if mtype is MigrationType.FULL_LOAD_AND_CDC:
        if not getattr(migration_state, "selection_touched", False):
            return None
        if job is None:
            if job_manager is None:
                return None
            from dsql_migrator.ui.data_migration._cdc_status import _current_job

            job = _current_job(job_manager, getattr(migration_state, "job_id", None))
        if pending is _PENDING_UNSET:
            from dsql_migrator.ui.data_migration._cdc_monitoring import (
                fl_cdc_pending_additions,
            )

            pending = fl_cdc_pending_additions(
                migration_state, job_manager, selected, job=job
            )
        narrowed = narrowed_record_names(migration_state, job)
        if not pending:
            return narrowed
        stored = set(_stored_selection(migration_state))
        names = (
            narrowed
            if narrowed is not None
            else [c.chunk_id for c in getattr(job, "chunks", None) or () if c.chunk_id in stored]
        )
        listed = set(names)
        return list(names) + [n for n in dict.fromkeys(pending) if n not in listed]
    if mtype is not MigrationType.FULL_LOAD_ONLY:
        return None
    if not getattr(migration_state, "selection_touched", False):
        return None
    names = [n for n in dict.fromkeys(selected or ()) if n]
    if not names:
        return None
    from dsql_migrator.ui.data_migration._cdc_monitoring import table_set_editable_after_load

    if not table_set_editable_after_load(migration_state, job_manager):
        return None
    return names


def scope_full_load_job(job, names: "Optional[Sequence[str]]", *, unfinished=()):
    """The job record seen through the current selection ``names``. Pure; never mutates.

    Returns ``job`` ITSELF when ``names`` is None, there is no job, it is still running,
    or the selection equals the record's tables (and nothing is reset, below). Otherwise a
    copy whose chunks are the selected tables' chunks (record order) followed by a PENDING
    chunk for each selected table the record lacks (selection order), with ``progress_pct``
    / ``error_count`` recomputed as the engine does; job id, watermark, status and error
    lineage are kept. A name in ``unfinished`` (G17: a Full load + CDC table the watermark
    does not cover yet) whose record chunk is DONE -- a Full load only detour, a quarantined
    load, a load whose coverage write was lost -- is presented PENDING: only its status
    changes (attempts, rows and quarantined rows are kept), so the step reads Failed and
    Retry unfinished tables offers it.
    """
    if names is None or job is None:
        return job
    if getattr(job, "status", None) in ("PENDING", "RUNNING"):
        return job
    chunks_in = list(getattr(job, "chunks", None) or ())
    wanted = list(dict.fromkeys(names))
    keep = set(wanted)
    reset = {
        c.chunk_id for c in chunks_in
        if c.status == "DONE" and c.chunk_id in keep
    } & set(unfinished or ())
    if keep == {c.chunk_id for c in chunks_in} and not reset:
        return job
    chunks = []
    for chunk in chunks_in:
        if chunk.chunk_id not in keep:
            continue
        copy = chunk.model_copy(deep=True)
        if chunk.chunk_id in reset:
            copy.status = "PENDING"
        chunks.append(copy)
    have = {c.chunk_id for c in chunks}
    chunks.extend(ChunkState(chunk_id=n) for n in wanted if n not in have)
    total = len(chunks)
    settled = sum(1 for c in chunks if c.status in ("DONE", "FAILED"))
    return job.model_copy(update={
        "chunks": chunks,
        "progress_pct": round(settled / total * 100.0, 4) if total else 0.0,
        "error_count": sum(1 for c in chunks if c.status == "FAILED"),
    })


def scoped_full_load_view(migration_state, job_manager, selected, *, job):
    """THE scoped view of the Full Load record ``job`` every chunk-driven surface reads
    (G12, G16, G17): :func:`scope_full_load_job` over :func:`full_load_scope_names`, with the
    Full load + CDC tables the watermark does not cover yet presented unfinished. The
    pending additions are computed once, over the record in hand (no further job read).
    ``selected`` is the resolved selection. Pure apart from CDC-state job reads."""
    if job is None or getattr(job, "status", None) in ("PENDING", "RUNNING"):
        return job
    pending: "list[str]" = []
    if getattr(migration_state, "migration_type", None) is MigrationType.FULL_LOAD_AND_CDC:
        from dsql_migrator.ui.data_migration._cdc_monitoring import fl_cdc_pending_additions

        pending = fl_cdc_pending_additions(migration_state, job_manager, selected, job=job)
    return scope_full_load_job(
        job,
        full_load_scope_names(migration_state, job_manager, selected, job=job, pending=pending),
        unfinished=pending,
    )


def scope_change(job, scoped) -> "tuple[list[str], list[str]]":
    """``(added, removed)``: selected tables the record lacks, record tables not selected.
    Both empty when ``scoped`` is ``job`` (nothing changed)."""
    if scoped is job or job is None or scoped is None:
        return [], []
    record = [c.chunk_id for c in job.chunks]
    view = [c.chunk_id for c in scoped.chunks]
    # Sets built once (a render calls this a few times over every table).
    record_set, view_set = set(record), set(view)
    return ([n for n in view if n not in record_set],
            [n for n in record if n not in view_set])


def watermark_coverage_gap(
    job, selected: "Sequence[str]", migratable: "Sequence[str]" = ()
) -> "Optional[tuple[list[str], list[str]]]":
    """``(selected since, unselected since)`` against the tables the watermark covers.

    ``None`` without a job or per-table watermark counts (nothing to compare). The
    watermark covers the run's table set -- every run table gets a count key -- plus the
    tables a guarded Full load + CDC load added since (:func:`cdc_covered_names`, G17), and
    CDC captures the covered tables still selected (:func:`cdc_capture_names`, G16): a table
    selected since has no gapless start point there (a Full load only load never covers
    it), while one unselected since is simply not captured (the type-switch lock counts it
    only once CDC has started for the migration, whose set is fixed then). A covered table
    that is no longer migratable (left the target) is not "unselected since". Pure.
    """
    watermark = getattr(job, "watermark", None) if job is not None else None
    covered_names = cdc_covered_names(watermark)
    if covered_names is None:
        return None
    covered = set(covered_names)
    universe = set(migratable or ()) or covered
    chosen = set(selected or ())
    return sorted(chosen - covered), sorted((covered & universe) - chosen)


def coverage_lock_reason(
    gap: "tuple[Sequence[str], Sequence[str]]", *, started: bool = False
) -> str:
    """The migration-type lock text for a table set changed after the Full Load.

    Before CDC has started for the migration only tables selected since lock (the gap's
    ``selected since`` half): the watermark gives them no gapless start point, while a
    removal alone never locks (CDC captures the loaded tables still selected, G16). Once CDC
    has started (``started``) -- even if stopped and deleted since -- any change locks, both
    halves are named: a detour through Full load only must not change the set a started CDC
    captured."""
    if started:
        added, removed = gap
        parts = []
        if added:
            parts.append(f"selected since: {_names_text(added)}")
        if removed:
            parts.append(f"unselected since: {_names_text(removed)}")
        return (
            f"Locked — the table set changed after this Full Load ({'; '.join(parts)}), and "
            "CDC has already started for this migration: a CDC type cannot continue a set "
            "that differs from the one that run started with. To add CDC, re-run Full Load "
            "over the current set (it takes a fresh watermark), or restore the original "
            "table set."
        )
    added = list(gap[0])
    one = len(added) == 1
    return (
        f"Locked — the table set changed after this Full Load (selected since: "
        f"{_names_text(added)}), and its watermark covers only the tables that run started "
        f"with, so a CDC type has no gapless start point for {'it' if one else 'them'}. "
        "Unticking tables does not lock the type: CDC captures the loaded tables still "
        "selected. To add CDC, re-run Full Load over the current set (it takes a fresh "
        f"watermark), or untick {'it' if one else 'them'} again."
    )


COVERAGE_GAP_NOTICE_BODY = (
    "Tables were selected after this Full Load, so its watermark does not cover them and "
    "CDC cannot continue them from it. To add CDC, re-run Full Load over the current set "
    "first (it takes a fresh watermark), or untick them again; the migration type unlocks "
    "then."
)
# The same notice once CDC has started for the migration (any change to the set locks).
COVERAGE_GAP_NOTICE_BODY_STARTED = (
    "The table set changed after this Full Load, and CDC has already started for this "
    "migration: CDC cannot continue a set that differs from the one this run started with. "
    "To add CDC, re-run Full Load over the current set first (it takes a fresh watermark), "
    "or restore the original table set; the migration type unlocks then."
)
COVERAGE_GAP_NOTICE_PG_SUFFIX = (
    " On PostgreSQL, CDC then re-reads the tables either way (a Full load only run "
    "creates no replication slot)."
)


def added_since_run_reason(added: "Sequence[str]", since: str = "the last Full Load") -> str:
    """The run-guard text for tables added to the selection after ``since``: the last
    Full Load with no report to vouch for them (G12), or "the checks ran" for a report
    that predates them -- one wording, the pronoun following the count."""
    listed = ", ".join(added[:3]) + (" and more" if len(added) > 3 else "")
    many = len(added) > 1
    return (
        f"Re-run the prerequisite checks — {listed} {'were' if many else 'was'} added to "
        f"the selection after {since}, so {'they were' if many else 'it was'} never checked."
    )


def scope_notice(
    job,
    scoped,
    *,
    blocked_by: "Optional[str]" = None,
    cdc: bool = False,
    pending: "Optional[Sequence[str]]" = None,
    not_addable: "Sequence[str]" = (),
    pg: bool = False,
) -> "Optional[tuple[str, str, str]]":
    """``(tone, header, body)`` of the Full Load card's scope notice, or ``None`` when the
    view is the record. Warning while a selected table is not loaded yet -- the step then
    reads Failed, which the notice says, since nothing failed; info for a removal alone (a
    left-as-is table is expected, not a problem). ``blocked_by`` (the run guard, e.g. the
    prerequisite checks a newly selected table never had) is named FIRST: until it clears,
    Retry unfinished tables refuses the newly selected tables. ``cdc`` (Full load + CDC,
    G16): the removal sentence also says CDC does not capture the unselected tables.

    ``pending`` (Full load + CDC, G17: the selected tables the watermark does not cover
    yet, whether or not the record has a chunk for them) replaces the "added" half: they
    are loaded under the Full Load's start point by Retry unfinished tables (on PostgreSQL
    with a recorded slot -- ``pg`` -- after joining CDC's publication), and CDC captures
    them from it. ``not_addable`` names the ones Retry cannot add (MySQL, no start-point
    definition recorded): only Re-run all tables (or an untick) settles them, so the Retry
    sentence names only the rest -- and when none is left, the run guard (``blocked_by``)
    leads to Re-run all tables instead, which it holds too. Linear in the table count (a
    render calls this over every pending table)."""
    added, removed = scope_change(job, scoped)
    if pending is not None:
        added = list(dict.fromkeys(pending))
    if not added and not removed:
        return None
    sentences = []
    if added and pending is not None:
        one = len(added) == 1
        first = f" First: {blocked_by} Then" if blocked_by else ""
        # Sets built once: a set rebuilt per name made this quadratic in the pending count.
        added_set = set(added)
        blocked = [n for n in dict.fromkeys(not_addable or ()) if n in added_set]
        blocked_set = set(blocked)
        addable = [n for n in added if n not in blocked_set]
        lead = (
            f"{len(added)} table{'' if one else 's'} selected after the last Full Load "
            f"({_names_text(added)}) {'is' if one else 'are'} not loaded under its start "
            f"point yet, so this step reads Failed until {'it is' if one else 'they are'}."
        )
        if addable:
            one_a = len(addable) == 1
            it = "it" if one_a else "them"
            # Mixed: name the ones Retry loads (the next sentence names the others).
            what = _names_text(addable) if blocked else it
            sentences.append(
                f"{lead}{first} Retry unfinished tables below loads {what} and keeps every "
                "loaded table"
                + (f"; it first adds {it} to CDC's publication on the source" if pg else "")
                + f". CDC then captures {it} from that start point with the other tables; "
                "Start CDC, Validation and Cut over wait until then."
            )
        else:
            sentences.append(lead)
        if blocked:
            one_b = len(blocked) == 1
            it_b = "it" if one_b else "them"
            # Nothing Retry can add: the run guard leads to the one route left (it holds
            # Re-run all tables too).
            route = first if not addable else ""
            sentences.append(
                f"{_names_text(blocked)} cannot be added by Retry: this Full Load recorded "
                f"no start-point definition for {it_b}, so CDC could not decode "
                f"{'its' if one_b else 'their'} binary-log changes since then.{route} "
                f"Re-run all tables (a fresh start point over every selected table) loads "
                f"{it_b}, or untick {it_b}."
            )
    elif added:
        one = len(added) == 1
        first = f" First: {blocked_by} Then" if blocked_by else ""
        sentences.append(
            f"{len(added)} newly selected table{'' if one else 's'} ({_names_text(added)}) "
            f"{'is' if one else 'are'} not loaded yet, so this step reads Failed until "
            f"{'it is' if one else 'they are'} loaded.{first} Retry unfinished tables below loads "
            f"{'it' if one else 'them'} and keeps every loaded table; Validation and Cut "
            "over wait until then."
        )
    if removed:
        one = len(removed) == 1
        sentences.append(
            f"{len(removed)} table{'' if one else 's'} no longer selected "
            f"({_names_text(removed)}) {'is' if one else 'are'} left as is on Aurora DSQL "
            f"(not dropped): CDC does not capture {'it' if one else 'them'}, and this "
            f"migration no longer loads or validates {'it' if one else 'them'}."
            if cdc
            else f"{len(removed)} table{'' if one else 's'} no longer selected "
            f"({_names_text(removed)}) {'is' if one else 'are'} left as is on Aurora DSQL "
            f"(not dropped); this migration no longer loads or validates "
            f"{'it' if one else 'them'}."
        )
        status = {c.chunk_id: c for c in job.chunks}
        unfinished = [
            n for n in removed
            if n in status and (
                status[n].status != "DONE" or (getattr(status[n], "rows_quarantined", 0) or 0)
            )
        ]
        if unfinished:
            one_u = len(unfinished) == 1
            sentences.append(
                f"{_names_text(unfinished)} did not fully load in the last run; as "
                f"{'it is' if one_u else 'they are'} no longer selected, that no longer "
                "holds this step."
            )
    return ("warning" if added else "info", SCOPE_NOTICE_HEADER, " ".join(sentences))


@dataclass(frozen=True)
class RetryScope:
    """What a scoped retry submits: ``names`` (empty = nothing), the ``notice`` naming
    the tables it dropped (unselected; ``notice_tone`` warning when nothing is left), and a
    ``refusal`` (the prerequisite guard for newly selected tables) that submits nothing."""

    names: "tuple[str, ...]"
    notice: "Optional[str]" = None
    notice_tone: str = "info"
    refusal: "Optional[str]" = None
    new: "tuple[str, ...]" = ()


def retry_scope_block_reason(
    names: "Sequence[str]",
    scoped_ids: "Sequence[str]",
    record_ids: "Sequence[str]",
    guard: "Optional[Callable[[Sequence[str]], Optional[str]]]" = None,
) -> RetryScope:
    """Scope a retry request to the current selection (G12). Pure.

    A name outside ``scoped_ids`` (no longer selected) is dropped -- left as is on Aurora
    DSQL, never reloaded or dropped. A name outside ``record_ids`` (selected after the
    load) needs ``guard(new)`` to be ``None``: the prerequisite checks never saw it.
    """
    in_scope = set(scoped_ids)
    record = set(record_ids)
    wanted = list(dict.fromkeys(names))
    dropped = [n for n in wanted if n not in in_scope]
    kept = [n for n in wanted if n in in_scope]
    notice = None
    if dropped:
        one = len(dropped) == 1
        notice = (
            f"{_names_text(dropped)} {'is' if one else 'are'} no longer selected for this "
            f"migration, so {'it is' if one else 'they are'} left as is on Aurora DSQL and "
            f"not reloaded. Tick {'it' if one else 'them'} in the table list to load "
            f"{'it' if one else 'them'} again."
        )
    new = [n for n in kept if n not in record]
    refusal = None
    if new and guard is not None:
        reason = guard(new)
        if reason:
            others = [n for n in kept if n in record]
            refusal = reason + (
                f" Untick {_names_text(new)} in the retry list to retry the other tables now."
                if others else ""
            )
    return RetryScope(
        names=() if refusal else tuple(kept),
        notice=notice,
        notice_tone="warning" if dropped and not kept else "info",
        refusal=refusal,
        new=tuple(new),
    )


def format_duration(seconds: float) -> str:
    """Format a duration in seconds as a compact ``Hh Mm``/``Mm Ss``/``Ss`` string."""
    total = int(max(0, round(seconds)))
    if total < 60:
        return f"{total}s"
    minutes, secs = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def build_lag_chart_option(
    series: "Sequence[tuple[int, int]]",
) -> Optional[dict]:
    """Build the ECharts option for the live "Stream lag" chart from
    ``[(epoch_seconds, lag_ms), ...]``, or ``None`` when there is nothing meaningful
    to plot (fewer than 2 points -- a single dot is not a trend).

    A CloudWatch-style live time series: X is a **time** axis (real timestamps), Y is
    **lag in milliseconds**. Data is ``[[epoch_ms, lag_ms], ...]`` (ECharts' time axis
    expects epoch milliseconds). Pure / NiceGUI-free so it is unit-testable; the caller
    updates the chart IN PLACE (``chart.options`` + ``chart.update()``) each poll so
    the line extends without the whole element being recreated (no flicker).
    """
    points = sorted((int(ts), int(ms)) for ts, ms in (series or []))
    if len(points) < 2:
        return None
    data = [[ts * 1000, ms] for ts, ms in points]  # ECharts time axis: epoch ms
    return {
        "tooltip": {"trigger": "axis"},
        "grid": {"left": 64, "right": 16, "top": 24, "bottom": 28},
        "xAxis": {"type": "time"},
        "yAxis": {"type": "value", "name": "lag (ms)", "min": 0, "minInterval": 1},
        "series": [
            {
                "name": "Max stream lag (ms)",
                "type": "line",
                "smooth": True,
                "showSymbol": False,
                "areaStyle": {},
                "data": data,
            }
        ],
    }


def format_table_timing(row: "FullLoadTableRow", now: datetime) -> str:
    """Return the per-table time cell: ETA while loading, total elapsed when done.

    - Not started: ``"—"``.
    - Finished (DONE/FAILED): the total elapsed wall-clock time for the run.
    - In progress with a known source count and some rows loaded: an estimated
      time remaining (``"~1m 30s left"``), extrapolated from the elapsed time and
      the fraction loaded so far.
    - In progress otherwise (no count yet / no rows yet): the elapsed time so far
      (``"12s elapsed"``), since a remaining estimate is not yet meaningful.
    """
    if row.started_at is None:
        return "—"
    if row.state in ("DONE", "FAILED"):
        if row.finished_at is None:
            return "—"  # e.g. interrupted-then-restored: no reliable finish time
        return format_duration((row.finished_at - row.started_at).total_seconds())
    if row.state != "IN_PROGRESS":
        return "—"
    elapsed = (now - row.started_at).total_seconds()
    pct = row.progress_pct
    if pct is not None and pct > 0 and row.rows_loaded > 0 and elapsed > 0:
        total_estimate = elapsed / (pct / 100.0)
        remaining = max(0.0, total_estimate - elapsed)
        return f"~{format_duration(remaining)} left"
    return f"{format_duration(elapsed)} elapsed"


@dataclass(frozen=True)
class FullLoadCompleteness:
    """Whether a finished Full Load loaded every source row, for the verdict.

    ``settled`` is the number of tables in a terminal state; ``complete`` counts
    tables whose loaded rows matched the source snapshot count; ``mismatched``
    lists tables that finished ``DONE`` but with a row-count gap; ``unknown``
    counts finished tables with no source count to compare. ``all_complete`` is
    ``True`` only when every table is ``DONE`` and every comparable count matched.
    """

    total: int
    settled: int
    complete: int
    failed: int
    mismatched: list[str]
    unknown: int
    # Rows permanently DROPPED across the run, and the tables that dropped them. A
    # confirmed data gap, so it must NOT be presented as estimate noise however
    # approximate the baseline is.
    quarantined_rows: int = 0
    quarantined_tables: list[str] = field(default_factory=list)

    @property
    def all_complete(self) -> bool:
        return (
            self.total > 0
            and self.settled == self.total
            and self.failed == 0
            and not self.mismatched
            and self.unknown == 0
            and self.quarantined_rows == 0
        )


def full_load_completeness(rows: Sequence[FullLoadTableRow]) -> FullLoadCompleteness:
    """Summarize source-vs-loaded completeness across all Full Load tables.

    ``mismatched`` lists only tables whose shortfall against the source ESTIMATE is
    too large to be that estimate's sampling error (see
    :attr:`FullLoadTableRow.complete`), so a normal few-percent discrepancy no longer
    reports a finished table as mismatched. ``unknown`` still counts finished tables
    with no estimate at all to compare against.
    """
    total = len(rows)
    settled = sum(1 for r in rows if r.state in ("DONE", "FAILED"))
    failed = sum(1 for r in rows if r.state == "FAILED")
    complete = sum(1 for r in rows if r.complete is True)
    mismatched = [r.table for r in rows if r.complete is False]
    unknown = sum(
        1 for r in rows if r.state == "DONE" and r.expected_rows is None
    )
    quarantined_rows = sum(r.rows_quarantined for r in rows)
    quarantined_tables = [r.table for r in rows if r.rows_quarantined > 0]
    return FullLoadCompleteness(
        total=total,
        settled=settled,
        complete=complete,
        failed=failed,
        mismatched=mismatched,
        unknown=unknown,
        quarantined_rows=quarantined_rows,
        quarantined_tables=quarantined_tables,
    )


@dataclass(frozen=True)
class MigrationTableStatus:
    """Per-table consistency view across the whole migration (Full Load + CDC).

    The customer's question this answers is "did CDC replicate everything, is
    anything missing?". So it separates the one-shot Full Load contribution from
    the ongoing CDC contribution and surfaces what did NOT land:

    - ``full_load_rows``  -- rows the one-shot Full Load loaded for this table.
    - ``cdc_applied_net`` -- the NET rows CDC has applied since Full Load
      (``target_rows - full_load_rows``): inserts add, deletes subtract, so this
      is the net change the stream produced, NOT a raw event count (our sink
      applies Debezium c/u/r as one idempotent upsert and d as a delete, so exact
      per-op insert/update/delete counts are not recoverable downstream).
    - ``source_rows`` vs ``target_rows`` (live exact COUNT(*) on each side) and
      the ``delta`` / ``in_sync`` consistency verdict -- the authoritative
      "is the target caught up to the source right now?" signal.
    - ``dlq_count`` -- change events QUARANTINED to the DLQ for this table, i.e.
      changes that did NOT reach the target (the "missing" the customer worries
      about). ``None`` when no DLQ/error breakdown is available.

    All fields are plain data; the builder is pure for unit testing.
    """

    table: str
    full_load_state: str  # PENDING / IN_PROGRESS / DONE / FAILED / "" (not run)
    full_load_rows: Optional[int]  # rows the Full Load loaded for this table
    source_rows: Optional[int]  # source count (live exact, or snapshot estimate)
    target_rows: Optional[int]  # live target COUNT(*), None if unknown/absent
    source_estimate: bool = False  # True when source_rows is the snapshot estimate
    dlq_count: Optional[int] = None  # change events quarantined (not applied)
    # Stream high-water marks: MAX(single-int PK) on each side. Comparing them
    # tells whether CDC's leading edge has caught up independently of the row
    # COUNT, so a lagging stream is distinguishable from a mid-stream gap. None
    # when the table has no single integer PK or the value is unknown.
    source_max_pk: Optional[int] = None
    target_max_pk: Optional[int] = None
    # Per-op change counts the CDC SINK reports having applied since it started
    # streaming, from its ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied``
    # CloudWatch metrics. A DMS-style breakdown: ``{"inserts": N, "updates": N,
    # "deletes": N}``. Scan-free (needs NO ``COUNT(*)`` on either side) and — unlike
    # the old net-rows figure — makes UPDATE traffic visible, but a best-effort
    # MONITOR, not an exact figure: it is APPROXIMATE UNDER REPLAY. A transient
    # reconnect / consumer rebalance makes Kafka Connect redeliver already-committed
    # events and the sink re-counts them (apply stays idempotent, so only the counter
    # over-states, never the data). The authoritative source↔target reconciliation is
    # Validation (Step 4)'s exact ``COUNT(*)`` / checksum, which does NOT read these
    # counters, and the consistency verdict is gated on the DLQ count + exact row delta
    # + PK high-water mark, never on these. ``None`` when unavailable (sink not yet
    # emitting / older plugin without the metrics).
    cdc_applied_ops: "Optional[dict[str, int]]" = None
    # End-to-end replication lag in milliseconds from the sink's ``ReplicationLagMs``
    # CloudWatch metric (apply time minus the event's source commit time). Time-based
    # and PK-agnostic -- the accurate "Stream lag" signal, preferred over the MAX(pk)
    # leading-edge (``pk_gap``) fallback. ``None`` when unavailable (older plugin) or
    # the table is idle/caught up (no recent datapoint).
    replication_lag_ms: Optional[int] = None

    @property
    def pk_gap(self) -> Optional[int]:
        """How far the target's high-water PK trails the source's, or ``None``.

        ``source_max_pk - target_max_pk`` when both are known. 0 means the stream's
        leading edge is caught up (latest source rows have landed); > 0 means the
        stream is genuinely BEHIND (newest source rows not yet applied). ``None``
        when either side is unknown (no single integer PK / not fetched).
        """
        if self.source_max_pk is None or self.target_max_pk is None:
            return None
        return self.source_max_pk - self.target_max_pk

    @property
    def stream_caught_up(self) -> Optional[bool]:
        """True when the stream's leading edge has reached the source.

        Reads the TIME-based ``replication_lag_ms`` first and the high-water PK only as a
        fallback, which is the precedence that field's own docstring already declares ("the
        accurate Stream lag signal, preferred over the MAX(pk) leading-edge fallback") and
        that :attr:`consistency`'s docstring already promised ("combines ... the stream
        high-water (PK) / time-based lag signals"). It did neither: the property read
        ``pk_gap`` alone, so a table with a measured 12-minute lag was reported
        ``"consistent"`` -- a green badge in the same row as "12m 0s behind" -- and a table
        with no single integer PK had no leading-edge signal at all.

        ``None`` lag means "no recent datapoint", which is how an IDLE or caught-up table
        looks, so it must NOT be read as behind; the PK gap is consulted in that case. Only
        a lag at or past :data:`_LAG_BEHIND_MS` counts as behind -- a live stream is always
        a little behind, and calling that a problem would flag every healthy table.

        Independent of the row COUNT: True even if rows are missing mid-stream, as
        long as the leading edge has caught up. Lets the UI say "stream caught up, but N
        rows missing mid-stream" vs. "stream is behind".
        """
        lag = self.replication_lag_ms
        if lag is not None:
            return lag < _LAG_BEHIND_MS
        g = self.pk_gap
        return None if g is None else g <= 0

    @property
    def cdc_inserts(self) -> Optional[int]:
        """INSERTs CDC has applied since it started streaming, or ``None``."""
        if self.cdc_applied_ops is None:
            return None
        return int(self.cdc_applied_ops.get("inserts", 0))

    @property
    def cdc_updates(self) -> Optional[int]:
        """UPDATEs CDC has applied since it started streaming, or ``None``."""
        if self.cdc_applied_ops is None:
            return None
        return int(self.cdc_applied_ops.get("updates", 0))

    @property
    def cdc_deletes(self) -> Optional[int]:
        """DELETEs CDC has applied since it started streaming, or ``None``."""
        if self.cdc_applied_ops is None:
            return None
        return int(self.cdc_applied_ops.get("deletes", 0))

    @property
    def cdc_applied_net(self) -> Optional[int]:
        """Net rows CDC applied since Full Load (inserts minus deletes).

        Derived from the sink's per-op metrics (``cdc_applied_ops``) when present --
        scan-free, needs no ``COUNT(*)`` on either side. Falls back to
        ``target_rows - full_load_rows`` when those metrics are unavailable (sink not
        yet emitting / older plugin), which needs both the Full Load row count and a
        live target count. ``None`` when neither is known. Can be negative if the
        stream net-deleted rows. This is a net row delta, not a raw event count --
        the per-op ``cdc_inserts`` / ``cdc_updates`` / ``cdc_deletes`` are the counts.
        """
        if self.cdc_applied_ops is not None:
            return int(self.cdc_applied_ops.get("inserts", 0)) - int(
                self.cdc_applied_ops.get("deletes", 0)
            )
        if self.target_rows is None or self.full_load_rows is None:
            return None
        return self.target_rows - self.full_load_rows

    @property
    def delta(self) -> Optional[int]:
        """Rows the target is behind the source now (``source - target``), or None.

        NOTE: when :attr:`source_estimate` is set, ``source_rows`` is a scan-free
        ``information_schema`` estimate, so this delta carries that estimate's error
        (percent-level on a large table) and must NOT be read as an exact shortfall.
        :attr:`counts_comparable` says whether it can be; :attr:`consistency` already
        accounts for it.
        """
        if self.source_rows is None or self.target_rows is None:
            return None
        return self.source_rows - self.target_rows

    @property
    def in_sync(self) -> Optional[bool]:
        """True when target == source (caught up).

        ``None`` when either count is unknown OR the source figure is an ESTIMATE:
        an estimate cannot establish equality, so this reports "not determinable"
        rather than a false negative on statistics noise.
        """
        if not self.counts_comparable:
            return None
        d = self.delta
        return None if d is None else d == 0

    @property
    def counts_comparable(self) -> bool:
        """Whether source and target row counts may be compared for EQUALITY.

        Only when the source figure is an EXACT count. The CDC status view's source
        figure is normally a scan-free ``information_schema`` ESTIMATE (to spare a
        large production source), and InnoDB derives that from index sampling -- it
        routinely differs from the truth by several percent, and on a big table by
        ~10%. Subtracting an exact target ``COUNT(*)`` from an estimate therefore
        produces a meaningless delta, so no equality-based verdict ("counts match",
        "target exceeds source") may be drawn from it.
        """
        return not self.source_estimate

    @property
    def consistency(self) -> str:
        """A plain-language consistency verdict for this table.

        Combines the DLQ, the stream high-water (PK) / time-based lag signals, and --
        only when the source count is EXACT -- the row-count delta, so a stream that
        is genuinely lagging is told apart from one whose leading edge has caught up
        but is missing rows in the middle:

        - ``"quarantined"`` -- DLQ has events that never reached the target (data
          is missing); this wins over everything else.
        - ``"consistent"`` -- the stream's leading edge has caught up and nothing
          indicates missing rows. With an EXACT source count this means the counts
          are equal; with an ESTIMATE it means the lag signals are clean and the
          counts agree within the estimate's tolerance (an estimate cannot prove
          equality, so this is the honest reading of "nothing looks wrong").
        - ``"behind"`` -- the stream is still catching up (its high-water PK trails
          the source, or counts show the target short with no PK signal).
        - ``"gap"`` -- the leading edge HAS caught up, yet rows are missing
          mid-stream -- a real gap to investigate, not mere lag.
        - ``"unknown"`` -- nothing to judge on yet (no counts and no lag signal).

        Crucially, a source ESTIMATE never produces ``"gap"`` from a small
        difference, and never produces the old ``"ahead"`` verdict at all: a target
        that merely EXCEEDS an estimate is the normal case (the estimate undercounts),
        not an anomaly, and reporting it as one made most healthy tables look broken.
        """
        if self.dlq_count:
            return "quarantined"
        caught = self.stream_caught_up
        d = self.delta

        if self.counts_comparable:
            # Exact source count: the delta is authoritative.
            if d is None:
                return "unknown" if caught is None else (
                    "consistent" if caught else "behind"
                )
            if caught is False:
                # A MEASURED lag proves the leading edge has not caught up, so it outranks
                # equal counts. Equal counts do not mean current: an UPDATE-only workload
                # changes no count at all, and an insert/delete pair nets to zero -- so
                # returning "consistent" here put a green badge next to the row's own
                # "12m behind" reading and could invite a cut-over decision on stale data.
                return "behind"
            if d == 0:
                return "consistent"
            if d < 0:
                # Target exceeds an EXACT source count. Real but not a shortfall --
                # read it as the stream still settling rather than the alarming
                # "target ahead" (which fired constantly on estimates).
                return "behind"
            if caught is True:
                return "gap"
            return "behind"

        # Source figure is an ESTIMATE: it cannot prove equality, so lean on the
        # lag signals and only treat a LARGE shortfall as a gap.
        if d is not None and d > 0 and self._exceeds_estimate_tolerance(d):
            # Materially fewer rows than even a noisy estimate allows for.
            return "gap" if caught is True else "behind"
        if caught is None:
            # No PK/lag signal and no usable count comparison.
            return "unknown" if d is None else "consistent"
        return "consistent" if caught else "behind"

    def _exceeds_estimate_tolerance(self, shortfall: int) -> bool:
        """Whether ``shortfall`` is too large to be source-estimate noise."""
        if self.source_rows is None or self.source_rows <= 0:
            return False
        return shortfall > self.source_rows * _ESTIMATE_TOLERANCE


def per_table_counts_notice_body(*, counts_fetched: bool) -> str:
    """Body for the per-table table's info notice, keyed on whether counts were read.

    The Consistency verdict needs the source/target row counts and high-water PKs, and
    those come only from the explicit "Refresh source/target counts" action (a COUNT(*)
    / MAX(pk) that scans the source, so it is never auto-polled). Until it is pressed,
    every row's Consistency reads "refresh to check" -- which points at a button the
    user has to connect to on their own. So before the first refresh the notice names
    that link ("Refresh ... to fill the Consistency column"); afterwards it drops the
    prompt and just states the estimate caveat. The exact reconciliation still lives in
    Validation either way.
    """
    tail = (
        "The target counts are exact. For an authoritative row/checksum "
        "reconciliation, run Validation (Step 4)."
    )
    if not counts_fetched:
        return (
            "The Consistency column reads “refresh to check” until you press "
            "“Refresh source/target counts” below — that reads the source count from "
            "information_schema (table_rows), a scan-free estimate that adds no load "
            "even on a large-scale source. " + tail
        )
    return (
        "Refresh reads the source row counts from information_schema (table_rows) — a "
        "scan-free estimate, so it adds no load even on a large-scale source, but it "
        "can drift from the exact count under heavy writes. " + tail
    )


def build_migration_table_status(
    table_names: "Sequence[str]",
    *,
    full_load_job: "Optional[MigrationJob]" = None,
    target_counts: "Optional[dict[str, Optional[int]]]" = None,
    source_counts: "Optional[dict[str, Optional[int]]]" = None,
    dlq_counts: "Optional[dict[str, int]]" = None,
    source_max_pk: "Optional[dict[str, Optional[int]]]" = None,
    target_max_pk: "Optional[dict[str, Optional[int]]]" = None,
    applied_ops_metric: "Optional[dict[str, dict[str, int]]]" = None,
    replication_lag_ms: "Optional[dict[str, Optional[int]]]" = None,
    source_is_estimate: bool = True,
) -> list["MigrationTableStatus"]:
    """Assemble the per-table migration status for ``table_names`` (pure).

    Reads the Full Load state + loaded rows + source snapshot estimate from
    ``full_load_job`` (its chunks/watermark), then overlays the live
    ``target_counts`` (exact target ``COUNT(*)``) and, when available, exact
    ``source_counts`` (else the watermark estimate is used as ``source_rows`` and
    flagged ``source_estimate``). ``dlq_counts`` (per-table quarantined-event
    counts from the error log) surfaces changes that did NOT reach the target.
    ``applied_ops_metric`` (per-table ``{"inserts","updates","deletes"}`` counts from
    the sink's ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied`` CloudWatch
    metrics) drives the scan-free "Changes since Full Load" (I/U/D) figures when
    present, so those need no ``COUNT(*)`` on either side. NiceGUI-agnostic so it can
    be unit tested with plain dicts; the UI supplies the live counts from a read-only
    poll.
    """
    chunks_by_table: dict[str, ChunkState] = {}
    expected: dict[str, Optional[int]] = {}
    if full_load_job is not None:
        chunks_by_table = {c.chunk_id: c for c in full_load_job.chunks}
        if full_load_job.watermark is not None:
            expected = dict(full_load_job.watermark.table_row_counts)
    target_counts = target_counts or {}
    source_counts = source_counts or {}
    dlq_counts = dlq_counts or {}
    source_max_pk = source_max_pk or {}
    target_max_pk = target_max_pk or {}
    applied_ops_metric = applied_ops_metric or {}
    replication_lag_ms = replication_lag_ms or {}

    out: list[MigrationTableStatus] = []
    for name in table_names:
        chunk = chunks_by_table.get(name)
        fl_state = chunk.status if chunk is not None else ""
        fl_rows = chunk.rows_loaded if chunk is not None else None
        live_source = source_counts.get(name)
        if live_source is not None:
            # A live source figure was supplied. It is an estimate unless the
            # caller says it ran an exact count (source_is_estimate=False).
            src, est = live_source, source_is_estimate
        elif expected.get(name) is not None:
            # Fall back to the Full Load watermark snapshot count -- always an
            # approximate (scan-free) figure. Keyed on the VALUE, not just presence: a
            # table whose estimate is None is present in the watermark but has no figure,
            # and taking that branch would label the missing number an "estimate".
            src, est = expected[name], True
        else:
            src, est = None, False
        out.append(
            MigrationTableStatus(
                table=name,
                full_load_state=fl_state,
                full_load_rows=fl_rows,
                source_rows=src,
                target_rows=target_counts.get(name),
                source_estimate=est,
                dlq_count=dlq_counts.get(name),
                source_max_pk=source_max_pk.get(name),
                target_max_pk=target_max_pk.get(name),
                cdc_applied_ops=applied_ops_metric.get(name),
                replication_lag_ms=replication_lag_ms.get(name),
            )
        )
    return out


# Ordered load states for the status-distribution visualization.
_LOAD_STATE_ORDER: tuple[str, ...] = ("DONE", "IN_PROGRESS", "FAILED", "PENDING")


def summarize_table_states(
    rows: "Sequence[FullLoadTableRow]",
) -> dict[str, int]:
    """Count tables per load state for the status-distribution visualization.

    Always returns every state key (0 when absent) in a fixed order so the
    summary chips render consistently. O(tables), so it scales to large schemas.
    """
    counts = {state: 0 for state in _LOAD_STATE_ORDER}
    for row in rows:
        counts[row.state] = counts.get(row.state, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Prerequisite gating & error-log summary (NiceGUI-agnostic)
# ---------------------------------------------------------------------------


def prereq_report_covered_tables(report: Optional[PrerequisiteReport]) -> set[str]:
    """Return the table names a prerequisite report actually covered.

    Derived from the report itself rather than tracked as new state: the checker emits one
    ``TABLE_PRIMARY_KEY`` and one ``TARGET_SCHEMA_READY`` result per selected table, each
    carrying that table in ``target`` (``core/prerequisites.py``), so the covered set is
    already in the object. That matters because the reports are never persisted and nothing
    clears them -- a report outlives the selection it was run for, and recording the scope
    separately would add a second thing to keep in sync.

    Returns an empty set for ``None`` or a report with no per-table results (a mode whose
    checks are all table-independent), which callers must read as "scope unknown", not as
    "nothing was covered".
    """
    if report is None:
        return set()
    per_table = (
        PrerequisiteCheckId.TABLE_PRIMARY_KEY,
        PrerequisiteCheckId.TARGET_SCHEMA_READY,
    )
    return {
        result.target
        for result in report.results
        if result.check_id in per_table and result.target
    }


def schema_recreate_tables(
    table_names: Iterable[str],
    *,
    table_conversions,
    inventory: Optional[SourceInventory],
    tables_with_data: Iterable[str],
    target_keys: Optional[Mapping[str, Optional[list[str]]]] = None,
) -> list[str]:
    """Return the EMPTY target tables whose primary key the load will have to recreate.

    A changed target primary key (e.g. the composite ``(leading, id)`` chosen to avoid
    hot partitions) is a **schema** change: appending cannot retrofit a key onto an
    existing table, so the load recreates the target from the applied DDL. That is
    non-destructive only because the table is empty — which is exactly why it happens
    without asking — but the user must still be TOLD, since a target DDL they edited by
    hand after Schema Conversion is replaced.

    Scoped to tables NOT in ``tables_with_data``: a populated table is never silently
    recreated (it goes through the explicit Drop & reload choice instead), so it must not
    appear in this disclosure.

    ``target_keys`` maps table name -> the target's ACTUAL primary-key columns (as read
    by :func:`~dsql_migrator.core.target_introspector.target_primary_key_columns`), and
    is what keeps this honest. A user who applies the composite key in Step 2 has a
    target that ALREADY carries it, and recreating it would be a no-op DROP+CREATE
    announced as "recreated to apply the chosen primary key" -- a contradiction, plus a
    wasted DDL round trip (DSQL allows one DDL per transaction). When the real key
    already equals the applied key, the table is dropped from this list.

    The mapping stays OPTIONAL and the fallback is deliberately conservative: a missing
    entry, or an explicit ``None`` (the key could not be read), keeps the table listed.
    Unknown is not "safe" -- the engine promotes on the same source-vs-applied
    comparison, so under-reporting here would hide a recreate that still happens. Do NOT
    make this function read the target itself: it renders inside the confirm dialog, and
    the caller already probes the target once (alongside ``tables_with_data``) and
    passes the cached answer in.
    """
    from dsql_migrator.core.converter import parse_target_primary_key

    if inventory is None:
        return []
    source_pk = {table.name: list(table.primary_key) for table in inventory.tables}
    populated = set(tables_with_data)
    keys = target_keys or {}
    out: list[str] = []
    for name in table_names:
        if name in populated:
            continue
        applied = (table_conversions or {}).get(name)
        if applied is None:
            continue
        target_key = parse_target_primary_key(applied.target_ddl)
        if not target_key or target_key == source_pk.get(name, []):
            continue
        # The target already has exactly the key the conversion asks for, so there is
        # nothing for a recreate to apply. Only an EQUAL key clears it -- absent (not
        # probed) or None (unreadable) both stay listed.
        if name in keys and keys[name] == target_key:
            continue
        out.append(name)
    return sorted(out)


def prereq_scope_gap(
    report: Optional[PrerequisiteReport], selected: Iterable[str]
) -> list[str]:
    """Return selected tables the report never checked, sorted; empty when in scope.

    ASYMMETRIC ON PURPOSE. Removing a table from the selection leaves the report a
    superset -- everything still selected was checked and passed -- so that is not a gap and
    must not block. Adding one is different: it never saw ``TARGET_SCHEMA_READY``, and
    ``run_full_load`` raises ``FullLoadIncompleteError`` on any per-table failure, so an
    unchecked table can fail the whole job rather than just itself.

    Returns ``[]`` when the report is absent or carries no per-table results, so a
    table-independent report (or a reconnected session with no report at all) is left to the
    guards that already handle an absent report -- this helper only reports a gap it can
    actually prove.
    """
    covered = prereq_report_covered_tables(report)
    if not covered:
        return []
    return sorted(name for name in selected if name and name not in covered)


def prerequisite_block_reason(
    report: PrerequisiteReport, *, mode: Optional[MigrationMode] = None
) -> Optional[str]:
    """Return a run-guard disable reason when prerequisites block the mode.

    Returns ``None`` when ``report.can_proceed`` is ``True``; otherwise an
    English, user-facing reason naming the failed required checks, suitable for a
    disabled Run button's tooltip (Property 14). Mirrors the workflow
    ``run_guards`` contract used by other steps.

    ``mode`` names the ACTION the failure gates. A bare "before running" is ambiguous
    on this screen -- the Prerequisites panel sits above a Full Load, a billable CDC
    infrastructure deploy AND a Start CDC, so "running" could mean any of the three and
    the operator cannot tell which is held. Naming it also makes the CDC-only case
    honest: what the failure actually stops first is the ~5-minute billable deploy.
    """
    if report.can_proceed:
        return None
    failed = [
        result
        for result in report.results
        if result.required and result.status == PrerequisiteStatus.FAIL
    ]
    titles: list[str] = []
    for result in failed:
        title = result.title if result.target is None else f"{result.title} ({result.target})"
        if title not in titles:
            titles.append(title)
    joined = "; ".join(titles) if titles else "a required check"
    if mode is MigrationMode.CDC:
        gated = "before deploying the CDC infrastructure or starting CDC"
    elif mode is MigrationMode.FULL_LOAD:
        gated = "before running the Full Load"
    else:
        gated = "before running"
    return f"Resolve the failed prerequisite(s) {gated}: {joined}."


def resnapshot_resolvable_failures(
    report: Optional[PrerequisiteReport],
) -> list[PrerequisiteResult]:
    """Return the CDC failures a route CHOICE resolves, not a source-side fix.

    The PostgreSQL "publication + slot exist" check fails in two very different ways.
    A coverage gap (the publication exists but omits captured tables) needs someone to
    change the source. Plain ABSENCE does not: it means this load never had a gapless
    handoff to begin with, and the operator's way out is to pick one of two routes the
    tool can take. ``resolvable_by_resnapshot`` on the result is where the checker
    records that difference, so both the affordance and the run guard read it from the
    one place instead of re-deriving it.

    Used by the render guard as well: when a choice is pending, repeating the verdict as
    a third red line under the choices adds no information and inverts the reading order
    (solution, then "resolve it"). Pure.
    """
    if report is None:
        return []
    return [
        result
        for result in report.results
        if result.check_id is PrerequisiteCheckId.CDC_REPLICATION_OBJECTS
        and result.status is PrerequisiteStatus.FAIL
        and getattr(result, "resolvable_by_resnapshot", False)
    ]


class PrereqCategory(str, Enum):
    """User-facing grouping for prerequisite checks (Req 5.10, UX grouping).

    Checks are grouped so the operator reads results by concern -- can I reach
    the databases, is the source configured, is the schema/tables ready, is the
    streaming pipeline up -- instead of one long flat list.
    """

    CONNECTIVITY = "Connectivity & Access"
    SOURCE_CONFIG = "Source Configuration"
    SCHEMA_TABLES = "Schema & Tables"
    STREAMING = "Streaming Pipeline (CDC)"


# Which category each check belongs to. Connectivity = can we reach/authenticate
# to both ends; Source Configuration = source server settings/privileges; Schema &
# Tables = per-table readiness on source and target; Streaming = the optional CDC
# transport (MSK / MSK Connect).
#
# This must stay EXHAUSTIVE over ``PrerequisiteCheckId`` -- the lookup falls back to
# SCHEMA_TABLES, so a missing entry does not raise, it just files the check under the
# wrong heading. That is how the five PostgreSQL logical-replication checks ended up
# there: four of them are server settings/privileges, so "Source Configuration" rendered
# nearly empty on a PG CDC run while "Schema & Tables" was padded with server-level rows.
# ``test_prereq_category_map_covers_every_check_id`` pins the exhaustiveness.
_PREREQ_CATEGORY_BY_CHECK: dict[PrerequisiteCheckId, PrereqCategory] = {
    PrerequisiteCheckId.SOURCE_REACHABLE: PrereqCategory.CONNECTIVITY,
    PrerequisiteCheckId.TARGET_DSQL_REACHABLE: PrereqCategory.CONNECTIVITY,
    PrerequisiteCheckId.TARGET_IAM_AUTH: PrereqCategory.CONNECTIVITY,
    PrerequisiteCheckId.REPLICATION_GRANTS: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.SELECT_GRANT_SCOPE: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.BINLOG_ROW_FORMAT: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.BINLOG_RETENTION: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.GTID_MODE: PrereqCategory.SOURCE_CONFIG,
    # PostgreSQL logical-replication readiness: server settings and server privileges,
    # the same nature as the MySQL binlog/GTID rows above.
    PrerequisiteCheckId.WAL_LEVEL_LOGICAL: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.REPLICATION_ROLE: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.REPLICATION_SLOTS: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.SOURCE_IS_WRITER: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.SLOT_WAL_RETENTION: PrereqCategory.SOURCE_CONFIG,
    # Also a server-level privilege (CREATE on the database + table ownership), so it sits
    # with REPLICATION_ROLE rather than under Schema & Tables.
    PrerequisiteCheckId.PUBLICATION_PRIVILEGE: PrereqCategory.SOURCE_CONFIG,
    # Beside the privilege row on purpose, so the panel reads "can create" then
    # "does exist" as a pair -- they are different questions with different remedies.
    PrerequisiteCheckId.CDC_REPLICATION_OBJECTS: PrereqCategory.SOURCE_CONFIG,
    # Per-table, like REPLICA_IDENTITY below.
    PrerequisiteCheckId.TABLE_REPLICABLE: PrereqCategory.SCHEMA_TABLES,
    # Per-table and per-COLUMN (which of this table's column types CDC cannot carry), so it
    # belongs with the table rows and next to TABLE_REPLICABLE, not with the server settings.
    PrerequisiteCheckId.COLUMN_REPLICABLE: PrereqCategory.SCHEMA_TABLES,
    PrerequisiteCheckId.TABLE_PRIMARY_KEY: PrereqCategory.SCHEMA_TABLES,
    PrerequisiteCheckId.TARGET_SCHEMA_READY: PrereqCategory.SCHEMA_TABLES,
    # Per-table, on the target and the source respectively -- the fallback already put
    # these two here, but only by accident; stated so the map is exhaustive.
    PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE: PrereqCategory.SCHEMA_TABLES,
    # A per-table, per-column data fact, so it belongs beside the other table rows rather
    # than with the server settings.
    PrerequisiteCheckId.SOURCE_VALUE_SIZE: PrereqCategory.SCHEMA_TABLES,
    PrerequisiteCheckId.REPLICA_IDENTITY: PrereqCategory.SCHEMA_TABLES,
    PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY: PrereqCategory.SCHEMA_TABLES,
    PrerequisiteCheckId.MSK_AVAILABLE: PrereqCategory.STREAMING,
    PrerequisiteCheckId.MSK_CONNECT_AVAILABLE: PrereqCategory.STREAMING,
}

# Display order of the categories (connectivity first -- it gates everything).
_PREREQ_CATEGORY_ORDER: tuple[PrereqCategory, ...] = (
    PrereqCategory.CONNECTIVITY,
    PrereqCategory.SOURCE_CONFIG,
    PrereqCategory.SCHEMA_TABLES,
    PrereqCategory.STREAMING,
)


@dataclass(frozen=True)
class PrereqCategoryGroup:
    """One category's prerequisite results with a rolled-up status and summary.

    ``status`` is the category's worst-meaningful outcome (a required ``FAIL``
    makes the category blocking; otherwise a ``WARN`` shows it needs attention; a
    category whose checks are all ``SKIP`` -- e.g. CDC-only checks under Full Load
    -- rolls up to ``SKIP``). ``summary`` is a short per-status count line.
    """

    category: PrereqCategory
    results: list[PrerequisiteResult]
    status: PrerequisiteStatus
    summary: str


def _rollup_category_status(
    results: Sequence[PrerequisiteResult],
) -> PrerequisiteStatus:
    """Roll a category's per-check statuses into one headline status.

    Severity order: a required ``FAIL`` makes the category blocking (``FAIL``); a
    non-required ``FAIL`` or any ``WARN`` is advisory (``WARN``); an ``INFO``
    (optional recommendation / expected state) with nothing worse rolls up to
    ``INFO`` (calm, does not auto-expand); all-``SKIP`` is ``SKIP``; otherwise
    ``PASS``.
    """
    if not results:
        return PrerequisiteStatus.SKIP
    if all(r.status is PrerequisiteStatus.SKIP for r in results):
        return PrerequisiteStatus.SKIP
    if any(r.status is PrerequisiteStatus.FAIL and r.required for r in results):
        return PrerequisiteStatus.FAIL
    if any(
        r.status is PrerequisiteStatus.WARN
        or (r.status is PrerequisiteStatus.FAIL and not r.required)
        for r in results
    ):
        return PrerequisiteStatus.WARN
    # A not-checked SKIP (the source was unreachable) is never green: the section reads
    # calm INFO, as the unmeasured rows it replaces did. A mode SKIP beside passes is PASS.
    if any(
        r.status is PrerequisiteStatus.INFO
        or (r.status is PrerequisiteStatus.SKIP and getattr(r, "not_checked", False))
        for r in results
    ):
        return PrerequisiteStatus.INFO
    return PrerequisiteStatus.PASS


def _category_summary(results: Sequence[PrerequisiteResult]) -> str:
    """Return a short per-status count line for a category (e.g. ``1 failed · 2 passed``).

    A SKIP that stands in for a check NOT RUN because the source was unreachable
    (``not_checked``) counts as "not checked", never as "skipped": an all-SKIP Source
    Configuration group read "Not applicable for this mode" for checks that very much
    apply, they just could not be asked.
    """
    total = len(results)
    skipped = sum(1 for r in results if r.status is PrerequisiteStatus.SKIP)
    not_checked = sum(
        1
        for r in results
        if r.status is PrerequisiteStatus.SKIP and getattr(r, "not_checked", False)
    )
    if total and skipped == total and not not_checked:
        return "Not applicable for this mode"
    counts = (
        (PrerequisiteStatus.FAIL, "failed"),
        (PrerequisiteStatus.WARN, "warning"),
        (PrerequisiteStatus.INFO, "recommendation"),
        (PrerequisiteStatus.PASS, "passed"),
        (PrerequisiteStatus.SKIP, "skipped"),
    )
    # Statuses whose label pluralizes by adding 's' (e.g. 2 warnings, 2
    # recommendations); the rest read the same singular/plural (passed, failed).
    _pluralize = {"warning", "recommendation"}
    parts: list[str] = []
    for status, label in counts:
        n = sum(1 for r in results if r.status is status)
        if status is PrerequisiteStatus.SKIP:
            if not_checked:
                parts.append(f"{not_checked} not checked")
            n -= not_checked
        if not n:
            continue
        if label in _pluralize and n != 1:
            label = f"{label}s"
        parts.append(f"{n} {label}")
    return " · ".join(parts)


def group_prereq_results(
    results: Sequence[PrerequisiteResult],
) -> list[PrereqCategoryGroup]:
    """Group prerequisite results into ordered, user-facing categories (Req 5.10).

    Returns one :class:`PrereqCategoryGroup` per non-empty category in display
    order, each carrying its results, a rolled-up headline status, and a count
    summary. NiceGUI-agnostic so the grouping/rollup is unit-testable.
    """
    buckets: dict[PrereqCategory, list[PrerequisiteResult]] = {
        category: [] for category in _PREREQ_CATEGORY_ORDER
    }
    for result in results:
        category = _PREREQ_CATEGORY_BY_CHECK.get(
            result.check_id, PrereqCategory.SCHEMA_TABLES
        )
        buckets[category].append(result)
    groups: list[PrereqCategoryGroup] = []
    for category in _PREREQ_CATEGORY_ORDER:
        category_results = buckets[category]
        if not category_results:
            continue
        groups.append(
            PrereqCategoryGroup(
                category=category,
                results=category_results,
                status=_rollup_category_status(category_results),
                summary=_category_summary(category_results),
            )
        )
    return groups


def format_error_summary(summary: ErrorLogSummary) -> str:
    """Format an :class:`ErrorLogSummary` as a one-line, user-facing string.

    The count equals the rows in the downloadable log (Property 15).
    """
    if summary.total_errors == 0:
        return "No data errors recorded."
    table_count = len(summary.errors_by_table)
    table_word = "table" if table_count == 1 else "tables"
    error_word = "error" if summary.total_errors == 1 else "errors"
    return (
        f"{summary.total_errors} data {error_word} across {table_count} {table_word}."
    )


# ---------------------------------------------------------------------------
# Watermark formatting for display (NiceGUI-agnostic) -- Requirement 8.5
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WatermarkDisplay:
    """A watermark formatted for display (Requirement 8.5 / Property 11).

    Optional binlog/GTID coordinates degrade to ``"unavailable"`` so the UI can
    always render a valid watermark even when only the snapshot timestamp and
    row counts were captured (Requirement 5.7).
    """

    coordinate: str
    gtid: str
    server_uuid: str
    snapshot_timestamp: str
    summary: str
    # ``None`` for a table whose snapshot estimate is UNKNOWN (never-analyzed PostgreSQL
    # table); the panel renders it as an em dash, not as 0.
    table_row_counts: dict[str, Optional[int]]
    # PostgreSQL-source coordinates (empty for a MySQL source). A PG watermark has a
    # WAL LSN + replication slot + publication instead of binlog/GTID/server-uuid, so
    # the panel must show these rather than rendering the MySQL fields as "unavailable"
    # and hiding the LSN the loader actually captured.
    wal_lsn: str = _UNAVAILABLE
    slot_name: str = _UNAVAILABLE
    publication_name: str = _UNAVAILABLE
    is_postgres: bool = False


def format_binlog_coordinate(watermark: Watermark) -> str:
    """Format the binlog ``file:position`` coordinate, or ``"unavailable"``."""
    if watermark.binlog_file and watermark.binlog_position is not None:
        return f"{watermark.binlog_file}:{watermark.binlog_position}"
    if watermark.binlog_file:
        return watermark.binlog_file
    return _UNAVAILABLE


def format_watermark(
    watermark: Watermark, *, source_type: "Optional[SourceType]" = None
) -> WatermarkDisplay:
    """Format a :class:`Watermark` for display (Requirement 8.5 / Property 11).

    Source-agnostic: a MySQL watermark yields the binlog ``file:position`` (+ GTID)
    one-line summary; a PostgreSQL watermark (a WAL LSN + slot + publication, no
    binlog/GTID) yields a "WAL LSN <lsn>" summary and populates the PG fields, so the
    panel shows the coordinate the loader actually captured instead of rendering every
    MySQL field as "unavailable".

    ``source_type`` is the engine the caller KNOWS, and it wins. Without it the engine can
    only be inferred from the presence of a WAL LSN -- but the PG Full-Load-only LSN read
    is best-effort (``probe_scalar``, ``None`` without the privilege), so a PG run whose
    one LSN read came back empty rendered the MySQL binlog/GTID/server-UUID rows and
    reported an "unavailable binlog coordinate" for a source that has no binlog at all.
    The LSN fallback is kept so a caller that has no session (and any older caller) behaves
    as before.
    """
    snapshot_timestamp = watermark.snapshot_timestamp.isoformat()
    is_postgres = source_type is SourceType.POSTGRES or bool(watermark.wal_lsn)

    if is_postgres:
        coordinate = watermark.wal_lsn or _UNAVAILABLE
        coordinate_part = (
            f"WAL LSN {coordinate}"
            if coordinate != _UNAVAILABLE
            else "an unavailable WAL LSN"
        )
        summary = f"Exported as of {coordinate_part}, snapshot {snapshot_timestamp}"
        return WatermarkDisplay(
            coordinate=coordinate,
            gtid=_UNAVAILABLE,
            server_uuid=_UNAVAILABLE,
            snapshot_timestamp=snapshot_timestamp,
            summary=summary,
            table_row_counts=dict(watermark.table_row_counts),
            wal_lsn=coordinate,
            slot_name=watermark.slot_name or _UNAVAILABLE,
            publication_name=watermark.publication_name or _UNAVAILABLE,
            is_postgres=True,
        )

    coordinate = format_binlog_coordinate(watermark)
    coordinate_part = (
        coordinate
        if coordinate != _UNAVAILABLE
        else "an unavailable binlog coordinate"
    )
    gtid_part = f" (GTID {watermark.gtid_executed})" if watermark.gtid_executed else ""
    summary = (
        f"Exported as of {coordinate_part}{gtid_part}, "
        f"snapshot {snapshot_timestamp}"
    )

    return WatermarkDisplay(
        coordinate=coordinate,
        gtid=watermark.gtid_executed or _UNAVAILABLE,
        server_uuid=watermark.server_uuid or _UNAVAILABLE,
        snapshot_timestamp=snapshot_timestamp,
        summary=summary,
        table_row_counts=dict(watermark.table_row_counts),
    )


# ---------------------------------------------------------------------------
# CDC handling read-models (NiceGUI-agnostic) -- surface the CDC connector
# behavior in the CDC screen: oversized-LOB exclusion, DLQ/circuit-breaker
# reporting, connector health + lag, and the handling contract the pipeline
# guarantees. All pure: they take already-gathered facts and return display
# data, mirroring format_watermark / format_error_summary.
# ---------------------------------------------------------------------------

# Aurora DSQL rejects a single text/bytea/json value over 1 MiB; a row over the Kafka
# client limit kills the source task. The CDC screen reuses the
# evaluation OVERSIZED_LOB type set (_OVERSIZED_LOB_BASES) and base-type parser
# (_base_type) imported above, so the exclusion offer stays in lock-step with the
# evaluation rule instead of duplicating the type list.


@dataclass(frozen=True)
class LobExclusionCandidate:
    """One table's columns that CDC can optionally exclude at capture.

    ``columns`` are the column names flagged by the evaluation ``OVERSIZED_LOB``
    rule (MySQL ``mediumtext/longtext/mediumblob/longblob/json``) whose values can
    exceed the Aurora DSQL 1 MiB per-value limit. Excluding them (the column then
    migrates as NULL, and Debezium's ``column.exclude.list`` never captures it) is the
    safe handling for values that can also exceed the CDC message size limit --
    runtime isolation cannot catch those.
    """

    table: str
    columns: tuple[str, ...]


def lob_exclusion_candidates(
    inventory: Optional[SourceInventory],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> list[LobExclusionCandidate]:
    """Return the oversized-LOB columns per table, sorted by table name.

    Pure: derived directly from the source inventory's column types, so the CDC
    screen can offer exclusion without re-running evaluation. The classification is
    chosen by ``source_type`` -- MySQL ``mediumtext/longtext/mediumblob/longblob/json`` vs
    PostgreSQL's ``is_pg_oversized_lob_type`` -- because ``column.mysql_type`` holds the
    SOURCE engine's type name (the MySQL set matched against PG types finds at most the
    shared ``json`` spelling).
    Both come from :mod:`core.assessor`, so this offer, the Evaluation
    ``OVERSIZED_LOB`` rule and the Schema Conversion note can never disagree about which
    columns are at risk. Primary-key columns are never offered (a PK can't be dropped);
    returns ``[]`` when nothing qualifies.
    """
    if inventory is None:
        return []
    candidates: list[LobExclusionCandidate] = []
    for table in inventory.tables:
        pk = set(table.primary_key)
        if source_type is SourceType.POSTGRES:
            # Table-aware: a CHECK limiting the column to a finite literal set means it
            # cannot hold an oversized value, so it must not be OFFERED for exclusion
            # either -- excluding it would drop a column for nothing.
            at_risk = set(pg_oversized_lob_column_names(table))
        else:
            at_risk = {
                column.name
                for column in table.columns
                if _base_type(column.mysql_type) in _OVERSIZED_LOB_BASES
            }
        columns = tuple(
            column.name
            for column in table.columns
            if column.name in at_risk and column.name not in pk
        )
        if columns:
            candidates.append(
                LobExclusionCandidate(table=table.name, columns=columns)
            )
    candidates.sort(key=lambda c: c.table)
    return candidates


# Source LOB base types split by the DSQL type they convert to, so a quarantine reason
# that names the DSQL type can be narrowed back to the source columns that produced it.
# (MySQL converter: mediumblob/longblob -> bytea, mediumtext/longtext -> text, json -> json.)
# Every member of _OVERSIZED_LOB_BASES must sit in exactly one bucket (pinned by a test), or
# a quarantine naming its type pre-ticks nothing. A ``jsonb`` reason (a json column re-typed
# jsonb in Schema Conversion) contains ``json`` and so pre-ticks the source json column.
_LOB_BASES_BY_TARGET_TYPE: "dict[str, frozenset[str]]" = {
    "bytea": frozenset({"mediumblob", "longblob"}),
    "text": frozenset({"mediumtext", "longtext"}),
    "json": frozenset({"json"}),
}

# The PostgreSQL map. PG -> DSQL is the IDENTITY for these types, so the source spelling
# equals the target type named in the reason -- which is precisely why the MySQL map found
# nothing for a PG source and the dialog opened with nothing pre-ticked. Kept as a separate
# dict rather than merged into the one above: a MySQL source has its own ``text`` type, and
# although it can never reach this helper today (``_OVERSIZED_LOB_BASES`` deliberately
# omits plain ``text``/``blob`` as too small to exceed 1 MiB), merging would make that
# exclusion load-bearing for CORRECTNESS -- widen the MySQL set later and a MySQL ``text``
# column would start being pre-ticked from a ``text`` reason. ``json``/``jsonb`` map to
# themselves and appear in a reason under their own name.
_PG_LOB_BASES_BY_TARGET_TYPE: "dict[str, frozenset[str]]" = {
    "bytea": frozenset({"bytea"}),
    "text": frozenset(
        {"text", "character varying", "varchar", "character", "char", "bpchar"}
    ),
    "jsonb": frozenset({"jsonb"}),
    "json": frozenset({"json"}),
}


def preselect_lob_columns_for_reason(
    columns_with_types: "Sequence[tuple[str, str]]",
    reason_text: str,
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> tuple[str, ...]:
    """Return the LOB columns a quarantine reason points at, for pre-ticking a picker.

    The offending COLUMN is not recoverable from the quarantine record: it carries only
    ``table`` / ``pk`` / ``error_code`` / ``message``, and the message keeps just the first
    line of the driver error -- which names the DSQL TYPE, not the column
    (``datatype limit greater than 1048576 bytes not supported for bytea``).

    That type token is still a real signal, because the source->DSQL mapping is
    deterministic: for MySQL ``mediumblob``/``longblob`` become ``bytea``,
    ``mediumtext``/``longtext`` become ``text``, and ``json`` stays ``json``; for PostgreSQL
    the mapping is the identity.
    So a ``bytea`` reason narrows a table with both a blob and a text LOB column
    (``product_media`` has ``content`` and ``full_description``) down to the blob one.

    ``source_type`` picks the mapping. Without it a PostgreSQL source matched PG spellings
    against MySQL base names and pre-ticked NOTHING (today: only a json column, the one
    spelling the engines share) -- not a blocker (the dialog's documented else-branch
    explains the empty selection and the action refuses an empty pick), but one
    avoidable click on the recovery path.

    This only PRE-SELECTS; the caller must still show the choice, because excluding the
    wrong column silently NULLs it for the whole table. Returns ``()`` when the reason
    names no known type or nothing matches, which means "make the user choose".
    """
    lowered = (reason_text or "").lower()
    by_target = (
        _PG_LOB_BASES_BY_TARGET_TYPE
        if source_type is SourceType.POSTGRES
        else _LOB_BASES_BY_TARGET_TYPE
    )
    matched: list[str] = []
    # Longest target name first so a "jsonb" reason is not also matched by the "json"
    # entry, which would pre-tick a json column for a jsonb failure.
    for target_type in sorted(by_target, key=len, reverse=True):
        if target_type not in lowered:
            continue
        for column, mysql_type in columns_with_types:
            # The single source of truth decides whether this column can hold an oversized
            # value at all, so the pre-tick can never disagree with the candidate offer. The
            # bucket match alone strips the length modifier, so a bounded
            # `character varying(50)` fell into the `text` bucket -- pre-ticking a column
            # that CANNOT exceed 1 MiB, in a dialog whose confirm NULLs it for every row.
            # Unreachable today only because the candidate list already excludes bounded
            # types; keyed here so the two filters cannot drift apart, and so widening the
            # core set later does not silently reopen the empty-pre-tick symptom.
            if source_type is SourceType.POSTGRES and not is_pg_oversized_lob_type(
                mysql_type or ""
            ):
                continue
            base = str(mysql_type or "").split("(")[0].strip().lower()
            if base in by_target[target_type] and column not in matched:
                matched.append(column)
        break
    return tuple(matched)


def effective_target_ddls(inventory, conv_state, source_type) -> "Optional[dict[str, str]]":
    """The applied target DDL per table, from the SAME inputs the Full Load runner converts
    with (inventory + Schema Conversion edits and toggle). ``None`` when either is missing.

    The ONE text source the overlay records against and cut over compares with (a DDL
    digest only matches byte-identical text), so every caller passes
    :func:`session_source_type` -- a MySQL fallback would convert a restored PostgreSQL
    session's DDL differently. Called on job threads: the edits are copied first.
    """
    if inventory is None or not getattr(inventory, "tables", None) or conv_state is None:
        return None
    from dsql_migrator.core.converter import SchemaConverter
    from dsql_migrator.ui.schema_conversion import applied_table_conversions

    edited = dict(getattr(conv_state, "edited_target_ddls", None) or {})
    applied = applied_table_conversions(
        SchemaConverter(source_type=source_type).convert(inventory),
        edited,
        preserve_foreign_keys=getattr(conv_state, "preserve_foreign_keys", True),
    )
    return {name: conversion.target_ddl for name, conversion in applied.items()}


CUTOVER_RESTORE_BLOCK_TEXT = (
    "Cut over is restoring the excluded columns' DEFAULTs. Wait for it to finish, then "
    "start again."
)


def cutover_restore_block_reason(migration_state) -> "Optional[str]":
    """Why a job that (re)drops excluded columns' DEFAULTs must not start now, or ``None``.

    A Full Load, a reload or Start CDC re-applies the overlay (DROP DEFAULT) on the very
    columns a running cut-over restore is setting, and a DEFAULT set halfway through a
    load gives the remaining batches that DEFAULT instead of NULL. The callable is wired
    by the builder (None without a Validation store: never refuses).
    """
    fn = getattr(migration_state, "cutover_restore_in_flight", None)
    try:
        running = bool(fn()) if callable(fn) else False
    except Exception:  # noqa: BLE001 - an unreadable job state never blocks a start
        running = False
    return CUTOVER_RESTORE_BLOCK_TEXT if running else None


def session_source_type(session) -> SourceType:
    """The session's source engine, falling back to the RESTORED hint, then MySQL.

    A live ``source_config`` is the authority. But a restored snapshot that carried no
    connection coordinates leaves it ``None`` while still knowing the engine, which
    ``apply_session_snapshot`` records as ``restored_source_type`` -- so reading only
    ``source_config`` sent every PG-aware surface back to the MySQL default right after a
    restore: the pre-load LOB panel claimed "no oversized LOB columns" for a schema full of
    text/bytea, the quarantine dialog pre-ticked nothing, and the watermark panel labelled
    MySQL rows. ``ui/connect.py`` already consults that hint for the same reason; this makes
    one helper of it so the next PG-aware call site cannot forget the second half.
    """
    live = getattr(getattr(session, "source_config", None), "source_type", None)
    if live is not None:
        return live
    return getattr(session, "restored_source_type", None) or SourceType.MYSQL


def session_has_seeder_lambda(session) -> "Optional[bool]":
    """Does this session's cdc-stack contain the in-VPC offset-seeder Lambda? ``None`` with
    no session (the caller shows a range covering both). Read through
    :func:`session_source_type`, so a RESTORED PostgreSQL session (no live source_config)
    is not taken for MySQL. The seed mode only matters for MySQL (PostgreSQL is forced
    external), and reading the host config would be I/O on a render path: the default IS
    lambda, so the MySQL answer stays the conservative "expect the ENI wait"."""
    if session is None:
        return None
    from dsql_migrator.core.cdc import stack_has_seeder_lambda

    return stack_has_seeder_lambda(session_source_type(session), None)


def make_lob_candidates_for(inventory, session, migration_state=None):
    """Build the post-quarantine LOB picker's candidate lookup for one session.

    Module-level, not a closure inside the page builder, so it can be TESTED. It used to be
    nested, which is what let the engine-unaware call inside it ship: every test of the
    button injects a lambda for this callable, so no test ever ran the real body, and the
    only guard left was an AST assertion that the ``source_type`` keyword appears -- which a
    hard-coded ``SourceType.MYSQL`` would also satisfy. Closing over just ``inventory`` and
    ``session`` made hoisting it a two-argument factory.

    The picker needs the TYPES, not just the names: the quarantine reason names the DSQL
    type (``bytea``/``text``/``json``) rather than the column, and the source type is what
    maps back to it. :class:`LobExclusionCandidate` carries names only, so the types come
    from the inventory here.

    With ``migration_state`` the lookup also drops every column the exclusion rule refuses
    (a composite target key, a column a preserved foreign key references, a column Aurora
    DSQL computes), so neither recovery route offers an "Exclude & reload" that the rule
    would then refuse -- or, before the rule existed, accept and break the reload.
    """

    def lob_candidates_for(table_name: str):
        if inventory is None:
            return ()
        table = next((t for t in inventory.tables if t.name == table_name), None)
        if table is None:
            return ()
        allowed = {
            c.table: set(c.columns)
            # The engine MUST be passed: the parameter defaults to MySQL, whose type names
            # (mediumtext/longblob/...) match a PostgreSQL inventory's format_type spellings
            # only on the shared ``json`` -- never text/bytea/jsonb. Before MySQL json joined
            # that set the result was empty, and the caller's "no candidates -> do not
            # offer a dead button" early return hid the ONLY post-quarantine recovery
            # action for every PostgreSQL migration; without the engine it would still hide
            # it on a table with no json column, and elsewhere offer only the json column.
            for c in (
                lob_exclusion_candidates(
                    inventory, source_type=session_source_type(session)
                )
                or ()
            )
        }.get(table_name, set())
        if allowed and migration_state is not None:
            from dsql_migrator.core.column_exclusion import column_exclusion_verdict

            ctx = exclusion_context_for(
                migration_state,
                inventory,
                selected_tables=exclusion_scope(migration_state),
                focus_tables=[table_name],
            )
            allowed = {
                c for c in allowed
                if not column_exclusion_verdict(table_name, c, ctx).blocked
            }
        return tuple(
            (col.name, col.mysql_type) for col in table.columns if col.name in allowed
        )

    return lob_candidates_for


def scope_lob_candidates(
    candidates: Sequence[LobExclusionCandidate],
    *,
    selected_tables: Optional[Sequence[str]],
    stored_selection: Sequence[str],
) -> list[LobExclusionCandidate]:
    """Scope oversized-LOB candidates to the tables that will actually be migrated.

    ``selected_tables`` is the caller's RESOLVED effective selection (e.g. the Full
    Load screen's ``effective_migration_selection``): when provided, filter strictly
    to it -- so selecting a single schema lists only that schema's LOB columns even
    though ``stored_selection`` (``migration_state.selection.selected_tables``) is
    still the empty "= all" default until the picker is touched (the Schema-Conversion
    pick only pre-ticks the picker). When ``None``, fall back to ``stored_selection``
    (empty => all), the legacy behavior for callers that can't resolve the effective
    set. Pure, so the scoping is unit-testable without a live screen. Both name forms
    are qualified ``db.table`` (matches ``TableDef.name`` / ``TableSelection``).
    """
    if selected_tables is not None:
        scoped = set(selected_tables)
        return [c for c in candidates if c.table in scoped]
    stored = set(stored_selection)
    if not stored:
        return list(candidates)
    return [c for c in candidates if c.table in stored]


@dataclass(frozen=True)
class ExclusionInputs:
    """What every exclusion gate judges, resolved LIVE by the Data Migration screen.

    ``selected`` -- the tables the migration moves (the picker, else the Schema Conversion
    default); ``applied`` -- the applied conversion (edits honoured); ``live_columns`` --
    each existing target table's columns from the target catalog (``None`` when there is
    no current catalog).
    """

    selected: "list[str]"
    applied: "Mapping[str, object]"
    preserve_foreign_keys: bool
    live_columns: "Optional[Mapping[str, frozenset[str]]]" = None
    # The tree's universe (tables with a target to load into), in its order; the applied
    # view DDLs (W6, view findings); the conversion signature the applied set is keyed by.
    migratable: "list[str]" = field(default_factory=list)
    view_ddls: "Mapping[str, str]" = field(default_factory=dict)
    signature: tuple = ()


def live_target_columns_of(target_inventory) -> "Optional[dict[str, frozenset[str]]]":
    """``{schema.table: column names}`` of the target catalog's tables, or ``None``."""
    if target_inventory is None:
        return None
    return {
        f"{rel.schema_name}.{rel.name}": frozenset(c.name for c in rel.columns)
        for schema in getattr(target_inventory, "schemas", None) or ()
        for rel in getattr(schema, "tables", None) or ()
    }


def exclusion_context_for(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
    selected_tables: "Optional[Sequence[str]]" = None,
    focus_tables: "Optional[Iterable[str]]" = None,
    view_ddls: "Optional[Mapping[str, str]]" = None,
    warnings: bool = False,
):
    """The :class:`~dsql_migrator.core.column_exclusion.ExclusionContext` for this session.

    ``applied_conversions`` / ``preserve_foreign_keys`` default to what the last Data
    Migration render recorded (``DataMigrationState.set_applied_conversions``), so the CDC
    builders and Validation -- which do not hold the Schema Conversion state -- judge a
    column against the same applied target DDL as Full Load. Before any render (a restored
    session) that record is empty and the rule falls back to the source schema plus the
    stored ``message.key.columns`` map, which is itself derived from the applied DDL.
    """
    from dsql_migrator.core.column_exclusion import build_exclusion_context

    getter = getattr(migration_state, "applied_conversions", None)
    cached, cached_preserve = getter() if callable(getter) else ({}, True)
    mkc_getter = getattr(migration_state, "cdc_message_key_columns", None)
    return build_exclusion_context(
        inventory,
        cached if applied_conversions is None else applied_conversions,
        message_key_columns=mkc_getter() if callable(mkc_getter) else None,
        preserve_foreign_keys=(
            cached_preserve if preserve_foreign_keys is None else preserve_foreign_keys
        ),
        selected_tables=selected_tables,
        focus_tables=focus_tables,
        view_ddls=view_ddls,
        warnings=warnings,
    )


def effective_lob_exclusions(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
) -> "dict[str, frozenset[str]]":
    """The exclusion set EVERY path uses: the stored set minus key columns (B1/B2).

    One answer for Full Load, the CDC ``column.exclude.list`` and Validation, so the paths
    can no longer disagree. Before this, the stored set went to each path raw: Full Load
    silently skipped a source-PK entry (``apply_lob_exclusions``) while the CDC path sent
    it to Debezium, and a Schema Conversion composite key's leading column was not
    protected on either path (the importer reads every target-key column from each row).
    A valid set passes through unchanged, so every normal session behaves exactly as
    before. With no inventory there is nothing to judge against, and the stored set is
    returned as-is (the paths that need an inventory refuse to run without one).
    """
    stored = migration_state.lob_exclusions()
    if inventory is None or not getattr(inventory, "tables", None):
        return {table: frozenset(cols) for table, cols in stored.items() if cols}
    from dsql_migrator.core.column_exclusion import effective_exclusions

    ctx = exclusion_context_for(
        migration_state,
        inventory,
        applied_conversions=applied_conversions,
        preserve_foreign_keys=preserve_foreign_keys,
        focus_tables=stored.keys(),
    )
    return dict(effective_exclusions(stored, ctx).columns)


@dataclass(frozen=True)
class RestoredExclusionFacts:
    """What a pre-overlay snapshot said about its exclusions, kept until they are
    classified (legacy = data already moved under the old semantics)."""

    entries: "frozenset[tuple[str, str]]"
    job_id: Optional[str]
    connector_names: "tuple[str, ...]"
    cdc_started: bool
    stack_possible: bool
    # True once stage 1 (the job evidence) found nothing and only the CDC probe can
    # decide; stage 2 never runs before it.
    stage1_passed: bool = False


def _chunk_moved(chunk) -> bool:
    """Evidence a Full Load chunk wrote (or tried to write) rows: it left PENDING, was
    attempted, or counted loaded / skipped / quarantined rows."""
    return (
        getattr(chunk, "status", "PENDING") != "PENDING"
        or (getattr(chunk, "attempts", 0) or 0) > 0
        or (getattr(chunk, "rows_loaded", 0) or 0) > 0
        or (getattr(chunk, "rows_skipped", 0) or 0) > 0
        or (getattr(chunk, "rows_quarantined", 0) or 0) > 0
    )


def classify_restored_exclusions(migration_state, job_manager=None) -> "Optional[bool]":
    """Classify a restored pre-overlay session's exclusions ONLY on evidence of writes.

    ``True`` legacy (omit-only), ``False`` upgraded (overlay entries), ``None`` still
    pending (behaves as legacy). Idempotent; reads jobs only.

    Stage 1 (needs ``job_manager``): legacy when the Full Load job is missing (a pruned
    record is not proof of nothing), holds a chunk that left PENDING or loaded rows, or
    the snapshot names connectors or a CDC start. With none of that and no possible CDC
    stack, upgrade. Stage 2 (state only, once the CDC probe has landed): legacy when CDC
    started, connectors are known, a different Full Load job ran meanwhile, an offset is
    committed or the stack is running; upgrade when the stack is absent or infra-only
    with no committed offset; otherwise still pending. Infra inputs and an infra deploy
    are never evidence.
    """
    pending = getattr(migration_state, "exclusion_classification_pending", None)
    if not callable(pending) or not pending():
        return None
    facts = migration_state.restored_exclusion_facts()
    if facts is None:
        return None

    def _settle(legacy: bool) -> bool:
        migration_state.finalize_exclusion_classification(legacy=legacy)
        return legacy

    if job_manager is not None:
        if facts.job_id is not None:
            from dsql_migrator.core.job_manager import JobNotFoundError

            try:
                job = job_manager.get_status(facts.job_id)
            except JobNotFoundError:
                job = None
            if job is None:
                return _settle(True)
            if any(_chunk_moved(chunk) for chunk in getattr(job, "chunks", ()) or ()):
                return _settle(True)
            # The newest job can be all PENDING (a retry re-seeds its chunks, a run that
            # ended before its first table leaves them untouched) while an earlier run
            # of the lineage moved rows: look at every job the store knows for these
            # tables before calling it clean.
            tables = {table for table, _column in facts.entries}
            lister = getattr(job_manager, "list_jobs", None)
            for other in (lister() if callable(lister) else ()) or ():
                if any(
                    chunk.chunk_id in tables and _chunk_moved(chunk)
                    for chunk in getattr(other, "chunks", ()) or ()
                ):
                    return _settle(True)
        if facts.connector_names or facts.cdc_started:
            return _settle(True)
        if not facts.stack_possible:
            return _settle(False)
        if not facts.stage1_passed:
            from dataclasses import replace as _replace

            facts = _replace(facts, stage1_passed=True)
            migration_state.set_restored_exclusion_facts(facts)
    if not facts.stage1_passed:
        return None  # the job evidence has not been judged yet
    # Stage 2: what this session's own probe and runs have seen since the restore.
    if (
        getattr(migration_state, "cdc_ops_window_start", None) is not None
        or getattr(migration_state, "cdc_connector_names", None)
        or getattr(migration_state, "job_id", None) != facts.job_id
        or getattr(migration_state, "cdc_has_committed_offset", False)
    ):
        return _settle(True)
    if not getattr(migration_state, "cdc_stack_phase_checked", False):
        return None
    phase = getattr(migration_state, "cdc_stack_phase", None)
    if phase == "running":
        return _settle(True)
    if phase in ("absent", "infra"):
        return _settle(False)
    return None


@dataclass(frozen=True)
class ForcedReplaceView:
    """What the Start dialog learned about the column-exclusion overlay.

    ``forced``: table -> why it is dropped and reloaded (the NULL contract needs it);
    ``appended_warnings``: table -> why a table that would be forced is appended anyway
    (CDC is streaming into it); ``undecided``: tables whose target could not be read (never
    forced -- the engine then refuses them if needed); ``null_columns``: table -> the
    columns that load as NULL.
    """

    forced: "dict[str, str]"
    appended_warnings: "dict[str, str]"
    undecided: "tuple[str, ...]"
    null_columns: "dict[str, tuple[str, ...]]"


def exclusion_forced_replace(
    action_tables: "Sequence[str]",
    *,
    overlay_by_table: "Mapping[str, Iterable[str]]",
    excluded_by_table: "Mapping[str, Iterable[str]]",
    source_columns_for: "Callable[[str], Sequence[str]]",
    facts: "Mapping[str, object]",
    ledger_tables: "Mapping[str, object]",
    cdc_coexisting: bool,
    recreated_unfinished: "Iterable[str]",
    index_ddls_for: "Callable[[str], Sequence[str]]",
) -> ForcedReplaceView:
    """Which action tables the Start dialog must drop and reload, and why.

    Per table, in order: an overlay or recorded table whose target facts are unknown is
    UNDECIDED (never forced). Otherwise the one shared verdict
    (``core.overlay_ledger.append_verdict``, the engine's own check) decides whether an
    append would break the NULL contract; a table the tool recreated and did not finish
    (``recreated_unfinished``) is reloaded so its indexes are built. While CDC streams
    nothing is forced: each such table is appended with a warning. Live computed columns
    are omit-only and never force.
    """
    from dsql_migrator.core.overlay_ledger import append_verdict

    unfinished = set(recreated_unfinished or ())
    forced: dict[str, str] = {}
    warnings: dict[str, str] = {}
    undecided: list[str] = []
    null_columns: dict[str, tuple[str, ...]] = {}
    for table in action_tables:
        overlay = frozenset(overlay_by_table.get(table, ()) or ())
        record = ledger_tables.get(table)
        if not overlay and record is None and table not in unfinished:
            continue
        table_facts = facts.get(table)
        refusal = None
        if table in unfinished and not cdc_coexisting:
            # Reloading it needs no facts: it was recreated and not finished, so its rows
            # are the tool's own partial load and its indexes were never built.
            forced[table] = (
                "recreated by the last run and not finished; it is dropped and reloaded "
                "so its indexes are built"
            )
            if overlay:
                null_columns[table] = tuple(sorted(overlay))
            continue
        if overlay or record is not None:
            if table_facts is None:
                if cdc_coexisting and table in unfinished:
                    ddls = list(index_ddls_for(table) or ())
                    warnings[table] = (
                        f"{table} was recreated by the last run and not finished; while CDC "
                        "streams it is appended, so its indexes are not built: "
                        + ("; ".join(ddls) if ddls else "none recorded")
                    )
                undecided.append(table)
                if overlay:
                    null_columns[table] = tuple(sorted(overlay))
                continue
            live = sorted(
                c for c in overlay
                if (f := table_facts.columns.get(c)) is not None and not f.computed
            )
            computed = sorted(
                c for c in overlay
                if (f := table_facts.columns.get(c)) is not None and f.computed
            )
            if live:
                null_columns[table] = tuple(live)
            if getattr(table_facts, "exists", True):
                refusal, _reincluded = append_verdict(
                    record,
                    table_facts=table_facts,
                    overlay_live=live,
                    computed=computed,
                    excluded=tuple(excluded_by_table.get(table, ()) or ()),
                    source_columns=tuple(source_columns_for(table) or ()),
                )
        if cdc_coexisting:
            if refusal is not None:
                warnings[table] = (
                    f"{table} cannot be dropped while CDC streams: existing rows keep "
                    f"their values in {', '.join(refusal.columns)}; Validation will count "
                    "them."
                )
            elif table in unfinished:
                ddls = list(index_ddls_for(table) or ())
                warnings[table] = (
                    f"{table} was recreated by the last run and not finished; while CDC "
                    "streams it is appended, so its indexes are not built: "
                    + ("; ".join(ddls) if ddls else "none recorded")
                )
            continue
        if refusal is not None and refusal.kind == "reincluded":
            forced[table] = (
                f"{', '.join(refusal.columns)} hold NULL from an earlier exclusion set; it "
                "is dropped and reloaded so they get their source values"
            )
        elif refusal is not None:
            forced[table] = (
                "already holds rows with a different exclusion set (or none recorded); it "
                f"is dropped and reloaded so every row is NULL in {', '.join(refusal.columns)}"
            )
    return ForcedReplaceView(forced, warnings, tuple(undecided), null_columns)


def legacy_lob_exclusions(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
) -> "dict[str, frozenset[str]]":
    """The omit-only entries (legacy, and a still-pending classification) within the
    effective set, so a key column (B1/B2) is never among them."""
    effective = effective_lob_exclusions(
        migration_state, inventory, applied_conversions=applied_conversions,
        preserve_foreign_keys=preserve_foreign_keys,
    )
    getter = getattr(migration_state, "legacy_exclusions", None)
    legacy = getter() if callable(getter) else {}
    out = {t: frozenset(effective.get(t, frozenset()) & cols) for t, cols in legacy.items()}
    return {t: cols for t, cols in out.items() if cols}


def overlay_lob_exclusions(
    migration_state,
    inventory,
    *,
    applied_conversions: "Optional[Mapping[str, object]]" = None,
    preserve_foreign_keys: "Optional[bool]" = None,
) -> "dict[str, frozenset[str]]":
    """THE overlay set: the effective set minus legacy / pending entries minus the columns
    Aurora DSQL computes (B4 by the applied DDL). The CANDIDATE set: the run further skips
    columns absent live and live-computed ones.

    Contract: omitted = ``effective_lob_exclusions`` (unchanged); overlay = this; legacy =
    omitted - overlay - computed. A legacy-only table has no overlay column, so it is
    never forced to reload and appends with a warning.
    """
    effective = effective_lob_exclusions(
        migration_state, inventory, applied_conversions=applied_conversions,
        preserve_foreign_keys=preserve_foreign_keys,
    )
    if not effective:
        return {}
    getter = getattr(migration_state, "legacy_exclusions", None)
    legacy = getter() if callable(getter) else {}
    generated: "Mapping[str, frozenset[str]]" = {}
    if inventory is not None and getattr(inventory, "tables", None):
        generated = exclusion_context_for(
            migration_state, inventory, applied_conversions=applied_conversions,
            preserve_foreign_keys=preserve_foreign_keys, focus_tables=effective.keys(),
        ).generated
    out: dict[str, frozenset[str]] = {}
    for table, columns in effective.items():
        kept = frozenset(columns) - frozenset(legacy.get(table, ())) - frozenset(
            generated.get(table, ())
        )
        if kept:
            out[table] = kept
    return out


def stored_exclusion_issues(
    migration_state,
    inventory,
    *,
    committed: bool,
    selected_tables: "Optional[Sequence[str]]" = None,
):
    """Judge the STORED exclusions (see ``column_exclusion.check_stored_exclusions``).

    ``committed`` must be the exclusion card's own ``locked`` flag. Before it locks, a
    blocked or vanished entry is an ERROR that holds Start Full Load, Deploy CDC
    infrastructure and Start CDC -- and the card, being unlocked, offers the Remove that
    the gate names. Once it locks (data moved, or the set was handed to the CDC pipeline)
    the same entry is only a warning: blocking there would point at a Remove the frozen
    card cannot perform.

    ``selected_tables`` (the tables this migration moves) scopes the check: an entry on a
    source table that is NOT selected affects nothing and is ignored, while an entry on a
    table that is no longer in the source at all is still reported -- its table may have
    been renamed, and the exclusion the user asked for would silently lapse. ``None``
    falls back to the stored selection (pinned by the prerequisite checks, which every
    gated button requires first), and to every table when that is still empty -- so the
    card and the gate it serves judge the same scope and the gate can never name an
    entry the card does not list. Empty with no stored entries or no inventory.
    """
    stored = migration_state.lob_exclusions()
    if not stored or inventory is None or not getattr(inventory, "tables", None):
        return ()
    from dsql_migrator.core.column_exclusion import check_stored_exclusions

    if selected_tables is None:
        selected_tables = exclusion_scope(migration_state)
    if selected_tables is not None:
        known = {t.name for t in inventory.tables}
        selected = set(selected_tables)
        stored = {t: c for t, c in stored.items() if t in selected or t not in known}
        if not stored:
            return ()
    ctx = exclusion_context_for(
        migration_state,
        inventory,
        selected_tables=selected_tables,
        focus_tables=stored.keys(),
    )
    return check_stored_exclusions(stored, ctx, committed=committed)


def exclusion_scope(migration_state) -> "Optional[list[str]]":
    """The tables the exclusion rule treats as migrated when the caller has no resolved set.

    The selection the last Data Migration render RESOLVED (the same ``selected_names`` the
    Full Load card is given -- an explicit "unselect all" stays ``[]``); before any render,
    the stored selection, or ``None`` -- every table -- while that is still the empty
    "= all" default. One definition, so the card, every gate (Continue, Start Full Load,
    the header Run, Deploy, Attach) and the recovery actions judge the same tables.
    """
    getter = getattr(migration_state, "resolved_selection", None)
    resolved = getter() if callable(getter) else None
    if resolved is not None:
        return resolved
    selection = getattr(migration_state, "selection", None)
    return list(getattr(selection, "selected_tables", None) or ()) or None


# --- the exclusion popup's pure composition (core.exclusion_rules over live inputs) ----


def exclusion_rules_text_for(migration_state, inventory) -> str:
    """The saved rule text, else lines synthesized from the stored set (a session that
    never saved rules: legacy, card ticks)."""
    from dsql_migrator.core.exclusion_rules import synthesize_rule_text

    saved = migration_state.exclusion_rules_text()
    if saved is not None:
        return saved
    return synthesize_rule_text(migration_state.lob_exclusions(), migration_state.legacy_exclusions())


def popup_open_text(migration_state, inventory) -> "tuple[str, list[str]]":
    """What a fresh open shows -- the saved (or synthesized) rules plus the pending lines --
    and those pending lines (a Save removes exactly them)."""
    text = exclusion_rules_text_for(migration_state, inventory)
    pending = migration_state.pending_rules()
    if pending:
        text = "\n".join([text.rstrip("\n")] + pending) if text.strip() else "\n".join(pending)
    return text, pending


def popup_resolution(
    migration_state,
    inventory,
    text: str,
    *,
    inputs: "ExclusionInputs",
    source_type: SourceType,
    selection_lock_reason: "Optional[str]" = None,
    capture_tables: "Optional[Callable[[Sequence[str]], Sequence[object]]]" = None,
    memo_slot: str = "editor",
    convert_legacy: "Iterable[str]" = (),
    add_lock=None,
):
    """:func:`~dsql_migrator.core.exclusion_rules.resolve_exclusion_rules` over this
    session's live inputs: the ONE context (selected' + focus, warnings on), the resolved
    selection, the provenance and re-ticks, the legacy (omit-only) entries, the stored set
    for the diff, the applied DDL and live columns for 42703 and the target changes, and --
    when ``capture_tables(selected')`` is given (CDC types) -- the capture preview.

    ``add_lock`` (Full load + CDC after its Full Load, before CDC starts, G16: the table
    picker's per-table add-lock, ``_cdc_monitoring.TableSetAddLock``) turns a table line
    that would select a table the lock refuses into a per-table E3; ``None`` changes
    nothing.

    Memoized on the state (transient) by the text's digest and every input, the inventory,
    applied set and live columns held by IDENTITY (a reused ``id`` never serves a stale
    result), so the row and the popup re-resolve only when something changed. One entry
    per ``memo_slot`` (the row's saved rules, the editor's text), so typing in the popup
    does not evict the row's comparison. ``convert_legacy``: tables whose omit-only entries
    Change & reload converts (its ticked checkboxes) -- resolved as overlay entries, so
    their warnings and ALTERs are the ones the reload really runs.
    """
    import hashlib

    from dsql_migrator.core.exclusion_rules import resolve_exclusion_rules

    stored = migration_state.lob_exclusions()
    converted = set(convert_legacy or ())
    legacy = {t: c for t, c in migration_state.legacy_exclusions().items()
              if t not in converted}
    prev = migration_state.rule_excluded_tables()
    reticks = migration_state.rule_reticked_tables()
    mkc_getter = getattr(migration_state, "cdc_message_key_columns", None)
    mkc = mkc_getter() if callable(mkc_getter) else {}
    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    key = (
        hashlib.sha256((text or "").encode("utf-8")).hexdigest(),
        tuple(inputs.signature), tuple(inputs.selected), tuple(inputs.migratable),
        frozenset(prev), frozenset(reticks),
        tuple(sorted((t, tuple(c)) for t, c in (mkc or {}).items())),
        getattr(getattr(migration_state, "migration_type", None), "value", None),
        selection_lock_reason,
        frozenset((t, c) for t, cs in legacy.items() for c in cs),
        frozenset((t, c) for t, cs in stored.items() for c in cs),
        deployed if capture_tables is not None else None,
        capture_tables is not None,
        source_type,
        add_lock,
    )
    held = (inventory, inputs.applied, inputs.live_columns, inputs.view_ddls)
    memos = getattr(migration_state, "_exclusion_resolution_memo", None)
    if not isinstance(memos, dict):
        memos = {}
        migration_state._exclusion_resolution_memo = memos
    memo = memos.get(memo_slot)
    if memo and memo[0] == key and all(a is b for a, b in zip(memo[1], held)):
        return memo[2]
    tables = {t.name: t for t in (getattr(inventory, "tables", None) or ())}
    migratable = [tables[n] for n in inputs.migratable if n in tables]

    def factory(selected, focus):
        return exclusion_context_for(
            migration_state, inventory, applied_conversions=inputs.applied,
            preserve_foreign_keys=inputs.preserve_foreign_keys, selected_tables=selected,
            focus_tables=focus, view_ddls=inputs.view_ddls, warnings=True,
        )

    capture = None
    if capture_tables is not None:
        def capture(columns, selected):
            return capture_exclusions_for(
                columns, columns, inventory, list(capture_tables(selected)), deployed)

    reselect_lock = None
    if add_lock is not None:
        def reselect_lock(names):
            blocked = add_lock.blocked(names, tree=False)
            return (blocked, add_lock.refusal(blocked, tree=False)) if blocked else None

    resolution = resolve_exclusion_rules(
        text,
        migratable=migratable,
        source_type=source_type,
        effective_selection=inputs.selected,
        rule_excluded_prev=prev,
        selection_lock_reason=selection_lock_reason,
        retick_overrides=reticks,
        context_factory=factory,
        applied_conversions=inputs.applied,
        live_columns=inputs.live_columns,
        legacy=legacy,
        stored_columns=stored,
        capture=capture,
        reselect_lock=reselect_lock,
    )
    memos[memo_slot] = (key, held, resolution)
    return resolution


@dataclass(frozen=True)
class ExclusionDrift:
    """What re-resolving the SAVED rules would change now (after Refresh objects, a tree
    tick): shown as a chip and in the Start dialog, never applied."""

    added: "tuple[tuple[str, str], ...]"
    removed: "tuple[tuple[str, str], ...]"
    unselects: "tuple[str, ...]"
    reselects: "tuple[str, ...]"
    # Saved lines that no longer resolve (e.g. a dropped table), and a global error.
    error_lines: "tuple[int, ...]" = ()
    global_error: bool = False
    # Columns the rules exclude on a table re-ticked in the tree since the last Save.
    retick_columns: "tuple[tuple[str, str], ...]" = ()


def exclusion_drift(
    migration_state, resolution, *, migratable: "Optional[Iterable[str]]" = None
) -> "Optional[ExclusionDrift]":
    """The difference between ``resolution`` (of the saved rules) and the stored state, or
    ``None``. A rule-excluded table re-ticked in the tree is an override, not drift -- but
    the column rules that would apply to it are reported. Lines that no longer resolve are
    reported too, and never hide the column changes the rest of the rules make; a table
    change under the selection lock (E3) is not drift. A stored entry on a table unticked
    in the tree (migratable, out of scope) is not drift either: the rules still match it,
    the table is simply not migrated. One on a table gone from the inventory still is."""
    if resolution is None:
        return None
    stored = migration_state.lob_exclusions()
    scope = set(resolution.in_scope)
    universe = set(migratable) if migratable is not None else None
    now = {(t, c) for t, cs in resolution.columns.items() for c in cs}
    before = {(t, c) for t, cs in stored.items() for c in cs
              if t in scope or (universe is not None and t not in universe)}
    reticks = migration_state.rule_reticked_tables()
    locked = any(e.code == "E3" for es in resolution.errors.values() for e in es) or any(
        e.code == "E3" for e in resolution.global_errors)
    unselects = () if locked else tuple(
        t for t in resolution.tables_to_unselect if t not in reticks)
    reselects = () if locked else tuple(resolution.tables_to_reselect)
    error_lines = tuple(sorted(n for n, es in resolution.errors.items()
                               if any(e.code != "E3" for e in es)))
    drift = ExclusionDrift(
        tuple(sorted(now - before)), tuple(sorted(before - now)), unselects, reselects,
        error_lines, any(e.code != "E3" for e in resolution.global_errors),
        tuple(resolution.retick_columns),
    )
    if not (drift.added or drift.removed or drift.unselects or drift.reselects
            or drift.error_lines or drift.global_error or drift.retick_columns):
        return None
    return drift


def open_lob_candidates(candidates, excluded: "Mapping[str, Iterable[str]]"):
    """Oversized-LOB candidates minus the columns ``excluded`` already excludes. Works on
    :class:`LobExclusionCandidate` lists and on one table's ``(column, type)`` pairs
    (``excluded`` then maps that table); empty tables drop out."""
    if candidates and isinstance(candidates[0], LobExclusionCandidate):
        out = []
        for c in candidates:
            gone = set(excluded.get(c.table, ()) or ())
            kept = tuple(col for col in c.columns if col not in gone)
            if kept:
                out.append(LobExclusionCandidate(table=c.table, columns=kept))
        return out
    gone = set(excluded or ())
    return tuple(pair for pair in candidates or () if pair[0] not in gone)


# The migration-type heading's CSS marker and the ONE scroll script that brings it into
# view with a brief ring (the screen's button and the popup's [Change migration type]).
MIGRATION_TYPE_ANCHOR = "dm-migration-type-anchor"
SCROLL_TO_MIGRATION_TYPE_JS = (
    f"const el=document.querySelector('.{MIGRATION_TYPE_ANCHOR}');"
    "if(el){el.scrollIntoView({behavior:'smooth',block:'center'});"
    "el.classList.add('ring-2','ring-blue-400','rounded');"
    "setTimeout(()=>el.classList.remove("
    "'ring-2','ring-blue-400','rounded'),2000);}"
)


def overlay_change_summary(actions: "Iterable[str]") -> str:
    """"N x DROP NOT NULL (one-way), M x DROP DEFAULT (restored at Cut over)" for the
    overlay statements' actions -- ONE wording for the popup and both Start dialogs."""
    actions = list(actions)
    n_not_null = sum(1 for a in actions if a == "DROP NOT NULL")
    n_default = sum(1 for a in actions if a == "DROP DEFAULT")
    parts = []
    if n_not_null:
        parts.append(f"{n_not_null} x DROP NOT NULL (one-way)")
    if n_default:
        parts.append(f"{n_default} x DROP DEFAULT (restored at Cut over)")
    return ", ".join(parts)


def start_dialog_exclusion_notes(migration_state) -> "list[tuple[str, str]]":
    """The Start Full Load dialog's exclusion lines, ``(tone, text)``: the tables the RULES
    unselected (in the tree) are not loaded, and the row's drift -- never applied, never a
    gate."""
    notes: list[tuple[str, str]] = []
    getter = getattr(migration_state, "exclusion_inputs", None)
    inputs = getter() if callable(getter) else None
    provenance = migration_state.rule_excluded_tables()
    if inputs is not None:
        # Only the ones still unselected: never "not loaded" for a selected table.
        provenance = (provenance & set(inputs.migratable)) - set(inputs.selected)
    if provenance:
        n = len(provenance)
        notes.append(("info", f"{n} table{'s' if n != 1 else ''} unselected by your rules "
                      f"{'is' if n == 1 else 'are'} not loaded (left as is on Aurora DSQL)."))
    drift = migration_state.exclusion_drift_text()
    if drift:
        notes.append(("warning", f"{drift} in Exclude tables & columns."))
    return notes


def deploy_exclusion_disclosure(migration_state, inventory) -> "Optional[tuple[str, str]]":
    """The Deploy CDC confirm's one info line, ``(header, details)``, or ``None`` when
    nothing is excluded: what the stack is created with, and that changing it later needs
    Delete CDC infrastructure. Never a gate (the gates stay in the deploy block notice)."""
    stored = migration_state.lob_exclusions()
    columns = sum(len(c) for c in stored.values())
    getter = getattr(migration_state, "exclusion_inputs", None)
    inputs = getter() if callable(getter) else None
    provenance = migration_state.rule_excluded_tables()
    if inputs is not None:
        # Only the ones still unselected: never "not loaded" for a selected table.
        provenance = (provenance & set(inputs.migratable)) - set(inputs.selected)
    if not columns and not provenance:
        return None
    parts = []
    if provenance:
        parts.append(f"{len(provenance)} table{'s' if len(provenance) != 1 else ''}")
    if columns:
        parts.append(f"{columns} column{'s' if columns != 1 else ''}")
    header = (f"Excluded: {', '.join(parts)}. They are fixed once deployed: changing them "
              "later needs Delete CDC infrastructure.")
    names = ", ".join(f"{t}.{c}" for t in sorted(stored) for c in sorted(stored[t]))
    sent = ""
    if inventory is not None and getattr(inventory, "tables", None):
        wanted = set(inputs.selected) if inputs is not None else None
        captured = [t for t in inventory.tables if wanted is None or t.name in wanted]
        sent = cdc_capture_exclusions(migration_state, inventory, captured).sent
    plain = ",".join(f"{t}.{c}" for t in sorted(stored) for c in sorted(stored[t]))
    if names and sent == plain:
        # The capture list is the same names: say so once instead of listing them twice.
        return header, f"Columns (sent as ColumnExcludeList unchanged): {names}."
    details = (f"Columns: {names or '(none)'}. Sent as ColumnExcludeList: "
               f"{sent or '(nothing excluded at capture)'}")
    return header, details


QUARANTINE_RULE_COMMENT = "# from the Full Load quarantine"


@dataclass(frozen=True)
class QuarantineSuggestion:
    """What the "Exclude column & reload" route does with one quarantined table.

    ``outcome``: ``A`` (``lines`` to insert, one per column the reason's type names that
    the open rules leave open; ``kept`` maps a column an include keeps to its line),
    ``NOT_SAVED`` (only an unsaved or pending line -- ``not_saved_lines`` -- excludes it:
    Save), ``B`` (the reason names no open column: pick one; ``filter_table`` when section
    4 has rows for it) or ``C`` (the saved rules exclude every flagged column: the value
    is in another column)."""

    outcome: str
    lines: "tuple[str, ...]" = ()
    kept: "Mapping[str, tuple[int, ...]]" = field(default_factory=dict)
    not_saved_lines: "tuple[int, ...]" = ()
    filter_table: bool = False
    # Preselected columns whose name holds a line break: no rule can name them.
    unwritable: "tuple[str, ...]" = ()


def quarantine_rule_suggestion(
    table: str,
    reason_text: str,
    candidates: "Sequence[tuple[str, str]]",
    *,
    open_excluded: "Iterable[str]",
    saved_excluded: "Iterable[str]",
    include_keepers: "Mapping[str, tuple[int, ...]]" = (),
    open_excluded_by: "Mapping[str, tuple[int, ...]]" = (),
    source_type: SourceType = SourceType.MYSQL,
) -> QuarantineSuggestion:
    """Decide the quarantine route's outcome for ``table`` (first match wins), from its
    ``(column, type)`` LOB candidates. OPEN = minus what the OPEN rule text excludes (the
    editor, else the saved rules plus pending lines) -- it decides which lines to insert,
    so none duplicates one already there; SAVED = minus the effective (saved) set -- it
    decides the copy when nothing is inserted, so a column only an unsaved line excludes is
    never treated as fixed. ``include_keepers``/``open_excluded_by`` map a column to the
    include lines keeping it / the open lines excluding it."""
    from dsql_migrator.core.exclusion_rules import rule_name_writable, rule_part_token

    keepers = dict(include_keepers or {})
    by_line = dict(open_excluded_by or {})
    open_c = open_lob_candidates(candidates, open_excluded)
    saved_c = open_lob_candidates(candidates, saved_excluded)
    pre_open = preselect_lob_columns_for_reason(open_c, reason_text, source_type=source_type)
    if pre_open:
        schema, _sep, rest = table.partition(".")
        lines, kept, unwritable = [], {}, []
        for column in pre_open:
            if column in keepers:
                kept[column] = tuple(keepers[column])
                continue
            if not all(rule_name_writable(x) for x in (schema, rest, column)):
                unwritable.append(column)
                continue
            lines.append("exclude " + ".".join(rule_part_token(x) for x in (schema, rest, column))
                         + f"  {QUARANTINE_RULE_COMMENT}")
        return QuarantineSuggestion("A", tuple(lines), kept, unwritable=tuple(unwritable))
    pre_saved = preselect_lob_columns_for_reason(saved_c, reason_text, source_type=source_type)
    if pre_saved:
        lines = tuple(sorted({n for c in pre_saved for n in by_line.get(c, ())}))
        return QuarantineSuggestion("NOT_SAVED", not_saved_lines=lines)
    if saved_c:
        if open_c:
            return QuarantineSuggestion("B", filter_table=True)
        lines = tuple(sorted({n for c, _t in saved_c for n in by_line.get(c, ())}))
        return QuarantineSuggestion("B", not_saved_lines=lines)
    return QuarantineSuggestion("C")


@dataclass(frozen=True)
class ExclusionRowSummary:
    """The button row's summary under the tree: ``variant`` is ``nothing``, ``counts``,
    ``locked``, ``drift`` or ``blocking`` (a notice box, not a hint); the LOB line counts
    the type-based candidates the open rules leave open in the selected tables."""

    variant: str
    text: str
    lob_count: int = 0
    lob_text: "Optional[str]" = None
    lob_tone: str = "info"


def _count_text(tables: int, columns: int) -> str:
    parts = []
    if tables:
        parts.append(f"{tables} table{'s' if tables != 1 else ''}")
    if columns:
        parts.append(f"{columns} column{'s' if columns != 1 else ''}")
    return ", ".join(parts)


def exclusion_row_summary(
    migration_state,
    *,
    locked: bool,
    drift: "Optional[ExclusionDrift]",
    block_reason: "Optional[str]",
    blocking_count: int = 0,
    lob_candidates: "Sequence[LobExclusionCandidate]" = (),
    measured_over: "Iterable[str]" = (),
    migratable: "Optional[Iterable[str]]" = None,
) -> ExclusionRowSummary:
    """The row's variant and copy. "N tables" counts only the RULE-excluded tables (the
    tree counter stays the one selection number); ``lob_candidates`` are already scoped
    and minus the open exclusions; the LOB line turns warning only when a measured
    over-limit value (``measured_over``, (table, column) pairs) is still not excluded."""
    stored = migration_state.lob_exclusions()
    columns = sum(len(c) for c in stored.values())
    provenance = migration_state.rule_excluded_tables()
    # Provenance outlives a table leaving the tree (so it stays excluded if it returns);
    # the count is of the tables the tree shows.
    tables = len(provenance & set(migratable)) if migratable is not None else len(provenance)
    counts = _count_text(tables, columns)
    lob_count = sum(len(c.columns) for c in lob_candidates)
    lob_text = None
    lob_tone = "info"
    if lob_count:
        lob_text = (
            f"{lob_count} column{'s' if lob_count != 1 else ''} in the selected tables can "
            "exceed Aurora DSQL's value limit (type-based). Review them in Exclude tables & "
            "columns."
        )
        open_names = {(c.table, col) for c in lob_candidates for col in c.columns}
        found = len(open_names & set(measured_over))
        if found:
            # A measured offender, not only a type: say so, or the warning tone is the
            # only difference from the type-based line.
            lob_tone = "warning"
            mtype = getattr(migration_state, "migration_type", None)
            consequence = (
                "each such row is dead-lettered by CDC (kept out of Aurora DSQL)"
                if mtype is MigrationType.CDC_ONLY
                else "each such row is quarantined at load"
                + (" (or dead-lettered during CDC)"
                   if mtype is MigrationType.FULL_LOAD_AND_CDC else "")
            )
            # "(measured)", not "(Check sizes)": the size cache is filled by the popup's
            # Check sizes AND by the prerequisite run's SOURCE_VALUE_SIZE answers
            # (DataMigrationState.set_prereq_report), so a user who only ran the checks
            # was credited with a click they never made.
            lob_text = (
                f"{found} column{'s' if found != 1 else ''} in the selected tables "
                f"{'hold' if found != 1 else 'holds'} a value over Aurora DSQL's limit "
                f"(measured): {consequence}. Review them in Exclude tables & columns."
            )
    if block_reason:
        n = blocking_count or 1
        text = (f"{n} saved exclusion{'s' if n != 1 else ''} no longer appl"
                f"{'y' if n != 1 else 'ies'}. Starting is blocked until you review them.")
        return ExclusionRowSummary("blocking", text, lob_count, lob_text, lob_tone)
    if locked:
        if drift is not None and drift.retick_columns:
            # Full load only after a load (G12): a rule-unselected table ticked again in the
            # tree has none of the column rules it lost; a reload would load those columns.
            n = len(drift.retick_columns)
            names = ", ".join(f"{t}.{c}" for t, c in drift.retick_columns[:3])
            more = f" and {n - 3} more" if n > 3 else ""
            text = (f"Your rules exclude {names}{more} on tables ticked again after your last "
                    f"Save (not excluded now), so a reload would load "
                    f"{'it' if n == 1 else 'them'} - use Change & reload")
            return ExclusionRowSummary("drift", text, lob_count, lob_text, lob_tone)
        text = f"{counts} excluded - locked" if counts else "Nothing excluded - locked"
        return ExclusionRowSummary("locked", text, lob_count, lob_text, lob_tone)
    if drift is not None:
        bits = []
        if drift.error_lines or drift.global_error:
            where = ", ".join(f"L{n}" for n in drift.error_lines)
            bits.append(f"have lines that no longer resolve ({where})" if where
                        else "have an error")
        if drift.added:
            names = ", ".join(f"{t}.{c}" for t, c in drift.added[:3])
            more = f" and {len(drift.added) - 3} more" if len(drift.added) > 3 else ""
            n = len(drift.added)
            bits.append(f"match {n} more column{'s' if n != 1 else ''} ({names}{more})")
        if drift.removed:
            n = len(drift.removed)
            bits.append(f"no longer match {n} saved column{'s' if n != 1 else ''}")
        if drift.unselects or drift.reselects:
            bits.append("would change the table selection")
        if drift.retick_columns:
            names = ", ".join(f"{t}.{c}" for t, c in drift.retick_columns[:3])
            more = f" and {len(drift.retick_columns) - 3} more" if len(drift.retick_columns) > 3 else ""
            bits.append(f"exclude {names}{more} on tables ticked again after your last Save "
                        "(not excluded now)")
        text = "Your rules now " + "; ".join(bits) + " - review"
        return ExclusionRowSummary("drift", text, lob_count, lob_text, lob_tone)
    if counts:
        return ExclusionRowSummary("counts", f"Rules exclude {counts}", lob_count, lob_text,
                                   lob_tone)
    return ExclusionRowSummary("nothing", "Nothing excluded", lob_count, lob_text, lob_tone)


def exclusion_refusal(migration_state, inventory, table_name: str, columns) -> "Optional[str]":
    """Why the rule refuses to exclude ``columns`` of ``table_name``, or ``None``.

    The click-time check for the two recovery actions (Full Load quarantine "Exclude &
    reload", CDC dead-letter "Exclude at capture"), which write the exclusion directly and
    never pass a prerequisite check: a refused column (or one the source no longer has) is
    reported and NOTHING is written. With no inventory there is nothing to judge against.
    """
    if not columns or inventory is None or not getattr(inventory, "tables", None):
        return None
    from dsql_migrator.core.column_exclusion import check_stored_exclusions

    ctx = exclusion_context_for(
        migration_state,
        inventory,
        selected_tables=exclusion_scope(migration_state),
        focus_tables=[table_name],
    )
    issues = check_stored_exclusions({table_name: set(columns)}, ctx, committed=False)
    if not issues:
        return None
    return " ".join(i.message for i in issues) + " Nothing was changed."


def missing_column_block_reason(
    migration_state,
    inventory,
    *,
    selected_tables: "Optional[Sequence[str]]" = None,
    job_manager=None,
) -> "Optional[str]":
    """The gate text for source columns the load would fail on (42703), or ``None``.

    Judged over the tables this migration moves (``selected_tables``, else
    :func:`exclusion_scope`, else every table) against the applied conversion, minus the
    EFFECTIVE exclusions -- an excluded column is not written, so it is not a problem.
    Only a CERTAIN failure is reported: a column the applied DDL lacks is let through when
    the live target table (from the target catalog) still has it, because an append into
    that table succeeds -- as it did in 0.1.561 -- whatever an unapplied edit says. Not
    tied to the exclusion card's lock (the remedy is Schema Conversion, which a stack does
    not block); skipped while CDC streams (Schema Conversion Apply and Start Full Load are
    held then anyway). Unknown (no conversion yet) means no claim.
    """
    if inventory is None or not getattr(inventory, "tables", None):
        return None
    if job_manager is not None:
        from dsql_migrator.ui.data_migration._cdc_state import cdc_streaming_started

        if cdc_streaming_started(migration_state, job_manager):
            return None
    getter = getattr(migration_state, "applied_conversions", None)
    applied, _preserve = getter() if callable(getter) else ({}, True)
    if not applied:
        return None
    from dsql_migrator.core.column_exclusion import missing_target_columns

    scope = selected_tables if selected_tables is not None else exclusion_scope(migration_state)
    wanted = None if scope is None else set(scope)
    tables = [t for t in inventory.tables if wanted is None or t.name in wanted]
    missing = missing_target_columns(
        tables, applied, effective_lob_exclusions(migration_state, inventory)
    )
    live_getter = getattr(migration_state, "live_target_columns", None)
    live = live_getter() if callable(live_getter) else None
    if live:
        missing = {
            table: kept
            for table, cols in missing.items()
            if (kept := tuple(c for c in cols if c not in live.get(table, frozenset())))
        }
    if not missing:
        return None
    pairs = [f"{table}.{column}" for table, cols in missing.items() for column in cols]
    shown = ", ".join(pairs[:3]) + (f" and {len(pairs) - 3} more" if len(pairs) > 3 else "")
    many = len(pairs) > 1
    them = "them" if many else "it"
    mtype = getattr(getattr(migration_state, "migration_type", None), "value", "")
    consequence = (
        f"CDC would dead-letter every change to {them}"
        if mtype == "cdc_only"
        else f"the load would fail on {them} and CDC would dead-letter every change to {them}"
        if mtype == "full_load_and_cdc"
        else f"the load would fail on {them}"
    )
    return (
        f"{shown} {'are' if many else 'is'} not in your Aurora DSQL table"
        f"{'s' if len(missing) > 1 else ''}; {consequence} (42703, undefined column). "
        f"Add {them} back to the target DDL in Schema Conversion. "
        "Columns are never dropped from a migration automatically."
    )


def stored_exclusion_block_reason(
    issues, *, where: str = "in Exclude tables & columns (under the table list)"
) -> "Optional[str]":
    """The gate text for stored-entry ERRORs, or ``None`` when nothing blocks.

    "Above" holds on every gated button: the card renders under the table picker, above
    all sub-steps, for a type with a Full Load, and above Deploy inside the CDC sub-step
    for CDC only.
    """
    from dsql_migrator.core.column_exclusion import IssueSeverity

    errors = [i for i in issues if i.severity is IssueSeverity.ERROR]
    if not errors:
        return None
    shown = " ".join(i.message for i in errors[:3])  # each message ends with a period
    more = f" And {len(errors) - 3} more." if len(errors) > 3 else ""
    many = len(errors) > 1
    return (
        f"{len(errors)} saved column exclusion{'s' if many else ''} no longer "
        f"appl{'y' if many else 'ies'}: {shown}{more} Remove {'them' if many else 'it'} "
        f"{where} first."
    )


def cdc_excluded_columns(
    lob_exclusions: "Mapping[str, Sequence[str]]",
    inventory: object,
) -> "dict[str, list[str]]":
    """The columns CDC must not stream: the operator's LOB exclusions PLUS the columns
    Aurora DSQL computes itself.

    A preserved ``GENERATED ALWAYS AS (expr) STORED`` column is maintained by the TARGET, and
    DSQL REJECTS a write that supplies a value for it (SQLSTATE 428C9, a permanent error the
    sink dead-letters). So it must be in Debezium's ``column.exclude.list`` for the same
    reason Full Load omits it from the INSERT.

    In practice PostgreSQL 17 and earlier do not publish generated columns at all, so this is
    belt-and-braces there; it becomes load-bearing on PostgreSQL 18, whose publications can
    opt into publishing them (``publish_generated_columns``).

    Derived from the SOURCE inventory with the converter's own predicate, so it agrees with
    what Schema Conversion emits. Caveat worth knowing: if the operator EDITS the target DDL
    to delete the GENERATED clause, the target column becomes ordinary and would then want
    the stream -- re-run Schema Conversion's apply so the two agree.
    """
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    merged: dict[str, set[str]] = {
        table: set(columns) for table, columns in (lob_exclusions or {}).items()
    }
    for table in getattr(inventory, "tables", None) or ():
        preserved = [column.name for column in pg_preserved_generated_columns(table)]
        if preserved:
            merged.setdefault(table.name, set()).update(preserved)
    return {table: sorted(columns) for table, columns in merged.items() if columns}


def format_column_exclude_list(
    selected: dict[str, Sequence[str]],
) -> str:
    """Format selected exclusions as a Debezium ``column.exclude.list`` value.

    Maps ``{table: [col, ...]}`` to the comma-separated ``db.table.column`` form
    the connector template's ``ColumnExcludeList`` parameter expects, sorted for
    a stable, reviewable value. ``table`` keys are the fully-qualified
    ``db.table`` names; empty input yields ``""`` (exclude nothing).
    """
    entries: list[str] = []
    for table in sorted(selected):
        for column in sorted(selected[table]):
            entries.append(f"{table}.{column}")
    return ",".join(entries)


@dataclass(frozen=True)
class DeployedCaptureExclusions:
    """The operator exclusions an attached stack's ColumnExcludeList carries, PROVEN by a
    byte-for-byte round trip. ``form``: ``"exact"`` (only the 0.1.562+ formula reproduces
    it), ``"legacy"`` (only the 0.1.561 formula does) or ``"plain"`` (both do). The form
    says which VERSION deployed the stack -- never whether its entries were overlaid: a
    0.1.562+ session can hold omit-only (legacy) entries too."""

    columns: "dict[str, list[str]]"
    form: str


def deployed_capture_exclusions(
    migration_state, inventory
) -> "Optional[DeployedCaptureExclusions]":
    """Read the attached stack's ColumnExcludeList back into operator columns, or ``None``.

    * 0.1.562+ (exact) reading: each entry matched the way Debezium matches it against the
      stack's OWN captured tables (its TableIncludeList decided any ``(?-i)`` marker and
      the compact grouping), minus the generated columns the value itself carries, then
      re-formatted with exactly those generated columns.
    * 0.1.561 (legacy) reading, tried whenever that fails: the value was the raw stored set
      (any inventory table, captured or not) plus EVERY inventory table's generated columns,
      unescaped -- so each entry is a literal ``db.table.column`` of the inventory.

    Accepted only when the reading's own formula reproduces the deployed value exactly.
    ``None``: no value recorded, a stack table missing from the inventory, or a value
    neither formula reproduces.
    """
    from dsql_migrator.core.cdc import (
        format_column_exclude_value,
        infer_column_exclusions,
        split_column_exclude_value,
    )
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    if deployed is None:
        return None
    if deployed == "":
        return DeployedCaptureExclusions({}, "plain")
    tables = list(getattr(inventory, "tables", None) or ())
    by_name = {t.name: t for t in tables}
    generated_all = {
        t.name: {c.name for c in pg_preserved_generated_columns(t)} for t in tables
    }

    def _operator(found):
        return {
            t: sorted(set(cols) - generated_all.get(t, set()))
            for t, cols in found.items()
            if set(cols) - generated_all.get(t, set())
        }

    legacy_of = lambda operator: format_column_exclude_list(  # noqa: E731
        cdc_excluded_columns(operator, inventory)
    )
    names = list(getattr(migration_state, "cdc_reconciled_table_names", None) or ())
    if names and all(name in by_name for name in names):
        captured = [by_name[name] for name in names]
        found = infer_column_exclusions(deployed, captured)
        if found is not None:
            operator = _operator(found)
            # Re-formatted from what the VALUE matched (operator + the generated columns it
            # carries), so a generated column that appeared or went away since deploy does
            # not fail the reading.
            exact = format_column_exclude_value(found, captured)
            if deployed == exact:
                return DeployedCaptureExclusions(
                    operator, "plain" if deployed == legacy_of(operator) else "exact"
                )
    literal = {f"{t.name}.{c.name}": (t.name, c.name) for t in tables for c in t.columns}
    found: dict[str, set[str]] = {}
    for entry in split_column_exclude_value(deployed):
        if entry not in literal:
            return None
        table, column = literal[entry]
        found.setdefault(table, set()).add(column)
    # Re-formatted from the literal entries themselves (generated columns as the value
    # carries them), so a generated column that appeared since deploy does not fail it.
    if deployed == format_column_exclude_list({t: sorted(c) for t, c in found.items()}):
        return DeployedCaptureExclusions(_operator(found), "legacy")
    return None


def capture_exclusion_mismatch(
    migration_state, inventory, capture: "CdcCaptureExclusions", captured_tables
) -> "Optional[str]":
    """Why Start CDC must not send ``capture`` to the attached stack, or ``None``.

    Start re-sends ColumnExcludeList on every pass, so a session whose set differs from a
    stack that already streamed would silently change what CDC captures for rows it has
    been streaming under the other set (live: a fresh session sent ``""`` over a stack
    excluding 5 columns, so changes would have carried values the loaded rows hold as
    NULL). Compared STRUCTURALLY on the tables captured both now and by the stack, so a
    legitimate string change (0.1.561 -> exact form, a changed table set, a generated
    column that appeared) is not refused. Nothing to refuse when the bytes already match,
    the stack never streamed, or no stack value is known yet.
    """
    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    if deployed is None or capture.sent == deployed:
        return None
    phase = getattr(migration_state, "cdc_stack_phase", None)
    # "Streamed" = the stack was ever started: a committed offset, connectors detected, or a
    # phase only a started stack reaches (an update that failed rolling back is "unstable").
    if not (
        getattr(migration_state, "cdc_has_committed_offset", False)
        or getattr(migration_state, "cdc_connector_names", None)
        or phase in ("running", "partial", "unstable")
    ):
        return None
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    now_names = {getattr(t, "name", None) for t in captured_tables or ()}
    stack_names = set(getattr(migration_state, "cdc_reconciled_table_names", None) or ())
    shared = (now_names & stack_names) if stack_names else now_names
    generated = {
        t.name: {c.name for c in pg_preserved_generated_columns(t)}
        for t in captured_tables or ()
        if getattr(t, "name", None)
    }
    session = {
        t: sorted(set(cols) - generated.get(t, set()))
        for t, cols in capture.structured.items()
        if t in shared and set(cols) - generated.get(t, set())
    }
    stack = deployed_capture_exclusions(migration_state, inventory)
    name = getattr(migration_state, "cdc_stack_name", None) or "the CDC infrastructure"

    def _named(columns) -> str:
        flat = [f"{t}.{c}" for t, cols in sorted(columns.items()) for c in cols]
        if not flat:
            return "no columns"
        return ", ".join(flat[:6]) + (f" and {len(flat) - 6} more" if len(flat) > 6 else "")

    if stack is None:
        how = (
            "Delete the CDC infrastructure, change the rules in Exclude tables & columns, "
            "then deploy it again."
            if getattr(migration_state, "job_id", None) is None
            else "Delete the CDC infrastructure and deploy it again: it then captures with "
            "this session's set, the one its Full Load used (changing that set needs Start "
            "over)."
        )
        return (
            f"'{name}' has streamed with an excluded-column list that cannot be read "
            "against this session's inventory (a table it captures is missing here, or the "
            "list does not match these columns exactly), so Start CDC cannot tell whether "
            f"sending this session's set would change what CDC captures. {how}"
        )
    stack_cols = {t: cols for t, cols in stack.columns.items() if t in shared}
    if stack_cols == session:
        return None
    remedy = (
        "Delete the CDC infrastructure and deploy it again with this session's exclusions"
        + (
            ", or Start over and attach it again so the session takes the stack's exclusions."
            if getattr(migration_state, "job_id", None) is None
            else " (this session's Full Load already wrote its rows under this session's set)."
        )
    )
    return (
        f"'{name}' has streamed while excluding {_named(stack_cols)}, but this session "
        f"would exclude {_named(session)}. Start CDC sends the session's set, so changes "
        f"CDC streams from here on would not match the rows already streamed. {remedy}"
    )


@dataclass(frozen=True)
class CdcCaptureExclusions:
    """Everything the CDC surfaces need about ``column.exclude.list``, from ONE computation.

    ``structured`` is ``{db.table: [columns]}``: the EFFECTIVE exclusion set plus the
    STORED generated columns of the CAPTURED tables. ``value`` is its exact-match form
    (``core.cdc.format_column_exclude_value``) and ``legacy`` the exact 0.1.561 value for
    the same session (raw stored set, every inventory table's generated columns, the
    unescaped formatter). ``sent`` is what Start CDC sends against the stack recorded by
    the last discovery -- the preview shows it, so preview == sent. ``error`` (the value is
    over CloudFormation's 4,096-byte parameter; never for a value the stack already
    carries) blocks Deploy, Start CDC and -- for Full load + CDC -- Start Full Load, each
    with the remedy true in its state; ``inexact`` lists the names a stack KEPT on the
    legacy form does not match exactly.
    """

    structured: "dict[str, list[str]]"
    value: str
    legacy: str
    sent: str
    error: "Optional[str]" = None
    inexact: "tuple[str, ...]" = ()
    # Bytes the automatically added generated columns ALONE need: over the limit, fewer
    # operator exclusions cannot fix a size refusal.
    generated_bytes: int = 0
    # Excluded names Debezium cannot tell apart from another captured column.
    ambiguous: "Mapping[str, list[str]]" = field(default_factory=dict)


def cdc_capture_exclusions(
    migration_state, inventory, captured_tables: "Sequence[object]"
) -> CdcCaptureExclusions:
    """Build :class:`CdcCaptureExclusions` for the tables CDC captures (see there), from
    the session's stored set and the stack recorded by the last discovery."""
    return capture_exclusions_for(
        effective_lob_exclusions(migration_state, inventory),
        migration_state.lob_exclusions(),
        inventory,
        captured_tables,
        getattr(migration_state, "cdc_deployed_column_exclude_list", None),
    )


def capture_exclusions_for(
    effective: "Mapping[str, Iterable[str]]",
    raw_stored: "Mapping[str, Iterable[str]]",
    inventory,
    captured_tables: "Sequence[object]",
    deployed_value: "Optional[str]",
) -> CdcCaptureExclusions:
    """:func:`cdc_capture_exclusions` for ANY set -- pure, so the exclusion popup previews
    exactly what Start CDC would send after its Save: ``effective`` is the exclusion set
    every path reads, ``raw_stored`` the stored set the 0.1.561 legacy form is built from,
    ``deployed_value`` the stack's recorded ColumnExcludeList."""
    from dsql_migrator.core.cdc import (
        CDC_PARAMETER_BYTE_LIMIT,
        choose_column_exclude_value,
        column_exclude_ambiguous_entries,
        column_exclude_inexact_entries,
        format_column_exclude_value,
    )
    from dsql_migrator.core.converter import pg_preserved_generated_columns

    captured = list(captured_tables or ())
    captured_names = {t.name for t in captured}
    # Only the CAPTURED tables' exclusions: an entry on a table outside table.include.list
    # is inert, the card does not show it, and counting it made a size refusal whose
    # "exclude fewer" remedy could not be followed. (The legacy value below is untouched.)
    merged: dict[str, set[str]] = {
        table: set(cols)
        for table, cols in (effective or {}).items()
        if table in captured_names
    }
    generated: dict[str, list[str]] = {}
    for table in captured:
        preserved = [c.name for c in pg_preserved_generated_columns(table)]
        if preserved:
            merged.setdefault(table.name, set()).update(preserved)
            generated[table.name] = sorted(preserved)
    structured = {t: sorted(c) for t, c in sorted(merged.items()) if c}
    value = format_column_exclude_value(structured, captured)
    legacy_columns = cdc_excluded_columns(
        {t: set(c) for t, c in (raw_stored or {}).items()}, inventory)
    legacy = format_column_exclude_list(legacy_columns)
    deployed = deployed_value
    sent = choose_column_exclude_value(value, legacy, deployed)
    error: "Optional[str]" = None
    inexact: "tuple[str, ...]" = ()
    if deployed is not None and sent == deployed:
        # Exactly what the stack already carries: nothing new to refuse (0.1.561 accepted
        # it), but a legacy value may not match its names exactly -- say so.
        if sent == legacy:
            inexact = tuple(column_exclude_inexact_entries(legacy_columns, captured))
    else:
        # A comma in a name is carried escaped (\\,), so only the size can refuse. The
        # text states the FACT; each caller appends the remedy that is true in its state
        # (fewer exclusions while the card is editable, the lock's own remedy otherwise).
        size = len(sent.encode("utf-8"))
        if size > CDC_PARAMETER_BYTE_LIMIT:
            error = (
                f"The column exclusions need {size:,} bytes in the CDC capture "
                f"configuration, over CloudFormation's {CDC_PARAMETER_BYTE_LIMIT:,}-byte "
                "limit for one parameter."
            )
    generated_bytes = (
        len(format_column_exclude_value(generated, captured).encode("utf-8"))
        if error and generated
        else 0
    )
    ambiguous = column_exclude_ambiguous_entries(structured, captured)
    return CdcCaptureExclusions(
        structured, value, legacy, sent, error, inexact, generated_bytes, ambiguous
    )


def capture_ambiguity_warning(capture: "CdcCaptureExclusions") -> "Optional[str]":
    """The warning for excluded names Debezium cannot separate from another column."""
    if not capture.ambiguous:
        return None
    pairs = [f"{name} (also drops {', '.join(others)})" for name, others in capture.ambiguous.items()]
    return (
        "Debezium names these the same as another captured column, so excluding one "
        "also drops the other at capture: " + "; ".join(pairs[:3])
        + (f" and {len(pairs) - 3} more" if len(pairs) > 3 else "")
        + ". No escaping can tell them apart; rename one, or keep both excluded."
    )


# The remedy for a capture-size refusal while the exclusion card is still editable.
CAPTURE_SIZE_REMEDY_UNLOCKED = (
    "Narrow the rules in Exclude tables & columns (under the table list) first."
)


# DSQL caps a single value at 1 MiB. Used for display.
_DSQL_VALUE_LIMIT_MIB = 1


@dataclass(frozen=True)
class DlqHealth:
    """DLQ depth assessed against a circuit-breaker threshold (H6/H11).

    The spike confirmed record-level DLQ isolation (H5/H6) but no automatic
    circuit breaker (H11): a flood of poison rows quietly accumulates. This
    read-model classifies the current depth so the screen can warn before a
    systematic incompatibility falls silently behind.
    """

    depth: int
    threshold: int
    level: str  # "ok" | "warn" | "alarm"
    message: str


def assess_dlq_health(
    dlq_depth: Optional[int], *, threshold: int = 100
) -> Optional[DlqHealth]:
    """Classify DLQ depth as ok/warn/alarm against ``threshold`` (H11).

    Returns ``None`` when no depth is reported (no signal to show). At/over the
    threshold is ``alarm`` (systematic incompatibility likely -- review the
    conversion / consider pausing); over half is ``warn``; otherwise ``ok``
    (sporadic poison, isolated as designed). Pure and side-effect free.
    """
    if dlq_depth is None:
        return None
    depth = max(0, dlq_depth)
    if depth >= threshold:
        level = "alarm"
        message = (
            f"{depth} quarantined records (>= {threshold}). Likely a systematic "
            "incompatibility -- review the schema conversion and consider pausing "
            "the connector rather than falling further behind."
        )
    elif depth * 2 >= threshold:
        level = "warn"
        message = (
            f"{depth} quarantined records approaching the {threshold} alarm "
            "threshold. Check the error log for a recurring cause."
        )
    else:
        level = "ok"
        message = (
            f"{depth} quarantined records, isolated to the DLQ (the pipeline keeps "
            "running)."
            if depth
            else "No records quarantined."
        )
    return DlqHealth(
        depth=depth, threshold=threshold, level=level, message=message
    )


@dataclass(frozen=True)
class ConnectorHealthRow:
    """One connector's display health (state + optional lag) -- H1/H2/H9.

    ``tone`` is a coarse severity ("ok" | "warn" | "bad") the screen maps to a
    color/icon, derived from the managed connector state and the reported lag so
    token-refresh (H1), OCC contention (H2), and failure-recovery (H9) behavior
    is legible at a glance without recomputation (Req 13.5).
    """

    name: str
    state: str
    tone: str
    detail: str
    # User-friendly role label (e.g. "Source (-> Kafka)") derived from the
    # connector name; ``name`` keeps the raw connector id for reference/debug.
    label: str = ""


# A connector state Kafka Connect reports as healthy vs. terminal.
_HEALTHY_CONNECTOR_STATES = frozenset({"RUNNING"})
_BAD_CONNECTOR_STATES = frozenset({"FAILED"})


def connector_role_label(name: str) -> str:
    """Map a raw MSK Connect connector name to a user-friendly role label.

    The pipeline is source DB -> Debezium source -> Kafka/MSK -> custom DSQL sink,
    so a name containing "source"/"debezium" is the source and one containing
    "sink" is the sink. The explicit "source"/"sink" tokens are checked first
    because the source connector name also contains "dsql" (e.g.
    ``dsql-cdc-spike-debezium-source``), which would otherwise misclassify it.
    The label stays engine-neutral (the connector name follows the tool's cdc-stack
    ``dsql-cdc-*`` / legacy ``mysql-dsql-cdc-*`` convention regardless of the actual
    source engine, so it can't be used to name MySQL vs. PostgreSQL). Falls back to
    the raw name when the role can't be inferred, so an unexpected connector is never
    hidden.
    """
    lowered = name.lower()
    if "source" in lowered or "debezium" in lowered:
        return "Source (→ Kafka)"
    if "sink" in lowered or "dsql" in lowered:
        return "Sink (Kafka → Aurora DSQL)"
    return name


def connector_health_rows(
    connector_states: dict[str, str],
    *,
    lag_seconds: Optional[float] = None,
    lag_warn_seconds: float = 30.0,
    diagnoses: Optional[Mapping[str, str]] = None,
) -> list[ConnectorHealthRow]:
    """Map connector states (+ worst lag) to display health rows, name-sorted.

    Pure projection of the managed signals already on ``LoadStatusView``:
    ``RUNNING`` is ok, ``FAILED`` is bad, anything else (PROVISIONING/PAUSED/…)
    is warn. The worst-case ``lag_seconds`` is attached to running connectors and
    nudges the tone to ``warn`` past ``lag_warn_seconds`` so a silently growing
    lag is visible. Each row carries a user-friendly role ``label`` (Source/Sink)
    and rows are ordered Source-then-Sink (data-flow order), not by raw name.
    ``diagnoses`` (connector name -> short cause headline, matched from its worker log)
    turns a FAILED row's detail into "Stopped — <cause>." instead of the generic restart
    text; it is ignored for every other state. Returns ``[]`` when no connector states
    are reported.
    """
    # Order by data flow: Source (-> Kafka) before Sink (Kafka -> DSQL), then
    # by name for any extra/unknown connectors.
    def _flow_key(connector_name: str) -> tuple:
        label = connector_role_label(connector_name)
        rank = 0 if label.startswith("Source") else 1 if label.startswith("Sink") else 2
        return (rank, connector_name)

    rows: list[ConnectorHealthRow] = []
    for name in sorted(connector_states, key=_flow_key):
        state = connector_states[name]
        upper = state.upper()
        if upper in _BAD_CONNECTOR_STATES:
            cause = (diagnoses or {}).get(name)
            tone, detail = "bad", (
                f"Stopped — {cause}."
                if cause
                else "Stopped — a task is not running. Restart required."
            )
        elif upper not in _HEALTHY_CONNECTOR_STATES:
            tone, detail = "warn", "Starting up — not streaming yet."
        elif lag_seconds is not None and lag_seconds >= lag_warn_seconds:
            tone, detail = "warn", f"Streaming — {lag_seconds:.1f}s behind source (elevated)."
        else:
            tone = "ok"
            detail = (
                f"Streaming — {lag_seconds:.1f}s behind source."
                if lag_seconds is not None
                else "Streaming normally."
            )
        rows.append(
            ConnectorHealthRow(
                name=name,
                state=state,
                tone=tone,
                detail=detail,
                label=connector_role_label(name),
            )
        )
    return rows


@dataclass(frozen=True)
class CdcHandlingFact:
    """One row of the CDC handling contract (what's handled vs. what to watch).

    ``handled`` True = the pipeline guarantees this automatically (verified in
    testing); False = a caveat the user must account for. ``evidence`` cites the
    supporting rationale so the claim is traceable.
    """

    handled: bool
    title: str
    detail: str
    evidence: str


def cdc_handling_facts() -> list[CdcHandlingFact]:
    """Return the CDC handling contract surfaced to the user (spike H1-H13).

    A static, ordered summary of what CDC handles automatically (idempotent
    upsert, ≤3,000-row batching, type mapping, DELETE/tombstone, OCC retry, token
    refresh, at-least-once resume) versus what to watch (oversized LOBs, DDL not
    propagated, no automatic circuit breaker), so the user sets correct
    expectations before streaming. Pure/static -- the verified handling behavior.
    """
    return [
        CdcHandlingFact(
            handled=True,
            title="No duplicates, even after retries",
            detail=(
                "Each change is applied idempotently, so a retried or redelivered "
                "change still results in exactly one row per key."
            ),
            evidence="H4/H9",
        ),
        CdcHandlingFact(
            handled=True,
            title="Inserts, updates, and deletes all replicate",
            detail=(
                "Source inserts/updates are applied to the target and source "
                "deletes are removed from the target. A source TRUNCATE is not "
                "replicated; the CDC step flags it."
            ),
            evidence="H7",
        ),
        CdcHandlingFact(
            handled=True,
            title="Source types are converted for DSQL",
            detail=(
                "Column types (e.g. ENUM, JSON, date/time) are "
                "mapped to their DSQL-compatible target types automatically."
            ),
            evidence="H8",
        ),
        CdcHandlingFact(
            handled=True,
            title="Resilient to load and reconnects",
            detail=(
                "Write contention and routine credential/connection refreshes are "
                "handled automatically without interrupting the stream."
            ),
            evidence="H1/H2",
        ),
        CdcHandlingFact(
            handled=True,
            title="A bad row won't stall the pipeline",
            detail=(
                "A change the target permanently rejects is set aside (dead-letter "
                "queue) and the rest keep flowing -- one bad row never blocks others."
            ),
            evidence="H5/H6",
        ),
        CdcHandlingFact(
            handled=False,
            title="Schema changes (DDL) are NOT replicated",
            detail=(
                "An ALTER TABLE on the source is not applied to the target. Make "
                "the matching change on the target yourself: Fix target schema… "
                "for an added or renamed column, ALTER … DROP NOT NULL for a column "
                "that now allows NULL, and for a type or key change recreate the "
                "table with CDC stopped (Replace in Schema Conversion, then Drop & "
                "reload; with CDC only, correct it by hand or use Start over). "
                "Schema Conversion's Apply is blocked while CDC streams. Rows that "
                "no longer fit are set aside to the dead-letter queue, but some "
                "changes are written with no dead letter, so validate with CHECKSUM."
            ),
            evidence="§4",
        ),
        CdcHandlingFact(
            handled=False,
            title=f"Very large values (over {_DSQL_VALUE_LIMIT_MIB} MiB) must be excluded",
            detail=(
                "A value too large to stream cannot be recovered later. Exclude such "
                "columns in Exclude tables & columns (under the table list); CDC then "
                "never captures them."
            ),
            evidence="H13",
        ),
        CdcHandlingFact(
            handled=False,
            title="Set-aside rows keep accumulating — watch the DLQ",
            detail=(
                "If many changes are incompatible they pile up in the dead-letter "
                "queue without pausing the stream, so keep an eye on the DLQ count "
                "above."
            ),
            evidence="H11",
        ),
    ]


# ---------------------------------------------------------------------------
# Migration type (UI orchestration pattern) + its pure resolvers / metadata
# ---------------------------------------------------------------------------


class MigrationType(str, Enum):
    """The migration orchestration pattern the user selected (UI concept only).

    Distinct from :class:`~dsql_migrator.core.models.MigrationMode` (the backend
    two-value prerequisite/execution primitive): this is what the user picked in
    the Data Migration type selector and maps to one or two phases run in
    sequence with an automatic Full Load -> CDC handoff.
    """

    FULL_LOAD_ONLY = "full_load_only"
    CDC_ONLY = "cdc_only"
    FULL_LOAD_AND_CDC = "full_load_and_cdc"


# The migration types that include a CDC phase (as opposed to Full Load only).
_CDC_MIGRATION_TYPES: frozenset[MigrationType] = frozenset(
    {MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC}
)


def source_supports_cdc(source_type: SourceType) -> bool:
    """Return whether CDC is available for a source of ``source_type`` today.

    This is the SINGLE enablement point for CDC-by-engine. Both MySQL (Debezium
    MySQL -> MSK -> DSQL sink) and PostgreSQL (pgoutput publication + replication
    slot -> MSK -> DSQL sink) CDC are implemented and enabled. Deliberately an
    **allowlist** -- only CDC-capable engines are listed -- so any future source
    engine defaults to "no CDC" until it is explicitly enabled here, rather than
    silently offering a non-functional CDC deploy.
    """
    return source_type in (SourceType.MYSQL, SourceType.POSTGRES)


# Ordered sub-steps of the Data Migration flow (Prerequisites -> Full Load -> CDC).
_SUBSTEPS: tuple[str, ...] = ("prerequisites", "full_load", "cdc")


def prereq_mode_for_type(migration_type: MigrationType) -> MigrationMode:
    """Return the :class:`MigrationMode` to run prerequisite checks for.

    CDC's checks are a strict superset of Full Load's (they add binlog/GTID/MSK
    checks and stronger replication grants), so both ``CDC_ONLY`` and the
    combined type use ``MigrationMode.CDC`` -- no report merging is needed. Only
    ``FULL_LOAD_ONLY`` uses the lighter ``MigrationMode.FULL_LOAD``.
    """
    if migration_type is MigrationType.FULL_LOAD_ONLY:
        return MigrationMode.FULL_LOAD
    return MigrationMode.CDC


def substeps_for_type(migration_type: MigrationType) -> tuple[str, ...]:
    """Return the ordered stepper sub-steps for the given migration type.

    - ``FULL_LOAD_ONLY``    -> ``("prerequisites", "full_load")``
    - ``CDC_ONLY``          -> ``("prerequisites", "cdc")``
    - ``FULL_LOAD_AND_CDC`` -> ``("prerequisites", "full_load", "cdc")``
    """
    if migration_type is MigrationType.FULL_LOAD_ONLY:
        return ("prerequisites", "full_load")
    if migration_type is MigrationType.CDC_ONLY:
        return ("prerequisites", "cdc")
    return ("prerequisites", "full_load", "cdc")


def migration_status_label(
    migration_type: MigrationType, *, cdc_streaming: bool = False
) -> str:
    """Return what the Data Migration status badge is describing, e.g. "Full Load".

    A bare "DONE" is ambiguous the moment the type selector moves. After a finished
    Full Load the user can switch to CDC only, and the badge -- still backed by the
    same underlying step -- then reads as "CDC: DONE" when no CDC has run at all.
    Naming the phase the status belongs to keeps it honest without changing the value.

    ``cdc_streaming`` matters for the combined type: the step is promoted to DONE once
    CDC is genuinely live, so before that the status still describes the Full Load.
    """
    if migration_type is MigrationType.FULL_LOAD_ONLY:
        return "Full Load"
    if migration_type is MigrationType.CDC_ONLY:
        return "CDC"
    return "CDC" if cdc_streaming else "Full Load"


def migration_status_badge(
    migration_type: MigrationType,
    *,
    full_load_status,
    cdc_status,
    cdc_streaming: bool = False,
) -> tuple:
    """Return ``(label, status)`` for the Data Migration status badge.

    The label alone was not enough. The badge's status has always come from the single
    ``full_load`` workflow step that every migration type shares, so a CDC-only session
    labelled it "CDC" while showing a value that belonged to a Full Load -- and because
    the whole workflow is persisted and restored, a session that once ran a Full Load
    came back reading "CDC: DONE" without CDC ever having run. Naming a phase and then
    showing another phase's value is worse than the bare "DONE" this replaced.

    So for CDC only the status is read from the ``cdc`` workflow step instead, which is
    maintained independently (set to IN_PROGRESS when connectors are detected). That step
    never reaches DONE by design -- CDC is continuous replication with no completion, and
    it ends only through an explicit Stop/Delete -- so this badge moves between
    NOT_STARTED and IN_PROGRESS, which is what CDC actually does. Both directions are
    real: ``_sync_cdc_step_status`` tracks the step to whether the connectors currently
    exist, so a Stop CDC / infrastructure Delete drops it back to NOT_STARTED instead of
    leaving "CDC: IN_PROGRESS" pinned on a pipeline that no longer has any connectors.

    Full load only, and the combined type before CDC goes live, keep reading the
    ``full_load`` step: there the label and the value describe the same phase.

    Both statuses are passed in (not read off a session) to keep this pure, and the
    chosen one is returned as-is -- the caller renders its value AND picks its colour
    from the same object, so the text and the colour cannot disagree. This decides
    DISPLAY only: the ``full_load`` step remains the Validation gate, so changing what
    the badge shows must not change what is reachable.
    """
    label = migration_status_label(migration_type, cdc_streaming=cdc_streaming)
    if migration_type is MigrationType.CDC_ONLY:
        return label, cdc_status
    return label, full_load_status


def stale_error_notice(
    error: Optional[str],
    *,
    migration_type: MigrationType,
    error_migration_type: Optional[MigrationType],
    quarantine_accepted: bool = False,
) -> Optional[tuple[str, str, str]]:
    """Return ``(tone, header, body)`` for the failure notice, or None to hide it.

    The bug this exists for: the notice was rendered from ``migration_state.error``
    alone, and nothing cleared that on a type switch. So after a Full Load that
    quarantined rows, switching to CDC only left a red "Migration failed" banner on a
    screen whose own header said "Success" and whose status said DONE -- three verdicts
    at once, and the user cannot tell which is true.

    Deleting the message would be worse: it reports rows that are genuinely missing
    from the target, which is exactly what someone about to start CDC needs to know
    (CDC replicates ongoing changes; it does not backfill a Full Load gap). So when the
    error belongs to a DIFFERENT migration type than the one now selected, it is kept
    but demoted to a warning and re-framed as carried-over context rather than a live
    failure of the current selection.

    ``error_migration_type`` is the type that was selected when the error was recorded;
    ``None`` means unknown (an older session), which is treated as "same type" so the
    behaviour is unchanged rather than silently softened.

    ``quarantine_accepted`` hides the notice outright. "Accept quarantined rows &
    continue" is the operator RESOLVING this exact error: it marks Full Load DONE and the
    step then reports "Full Load complete -- with an accepted gap", which already names
    the dropped row count, the affected tables, and that Validation still reports the gap.
    Leaving the raw ``FullLoadIncompleteError`` above that as a red "Migration failed"
    contradicted the very decision the button records -- three verdicts on one screen
    (failed / complete-with-gap / DONE) -- and re-flagged as a problem something the
    operator had already dealt with. Nothing is lost by hiding it: the accepted-gap
    notice carries the same facts in the resolved framing, and the error log still lists
    every dropped row by primary key.
    """
    if not error:
        return None
    if quarantine_accepted:
        return None
    if error_migration_type is None or error_migration_type is migration_type:
        return ("error", "Migration failed", error)
    return (
        "warning",
        "Carried over from the previous Full Load",
        "This did not happen in the migration type now selected, but the target still "
        "reflects it -- CDC streams ongoing changes and will not backfill a Full Load "
        f"gap. Re-run Full Load to close it before relying on the target. {error}",
    )


def prereq_connection_block(session) -> Optional[tuple[str, str, str]]:
    """``(header, body, short)`` while a connection is unverified in this session, else None.

    The prerequisite checks used to run whenever a source and target CONFIG existed. A
    restored session has both (the restore re-creates them) but not the source password,
    which is kept in memory only (Property 7) -- so after a restart or redeploy the checks
    ran password-less, and every source check came back as a confident false verdict. An
    edited but untested connection is the same hazard the other way round: the session
    still holds the LAST TESTED values, so the checks would quietly test a connection the
    screen no longer shows.

    Keys on the per-side verified flags (a passed test in THIS session), the same signal
    as Validation, Reload, Check sizes and Change & reload. Not ``workflow_unlocked()``
    (a sticky latch the restore sets again) and not ``has_source()`` (a config exists).
    ``short`` is the disabled button's tooltip and the click-time refusal. Pure.
    """
    missing = [
        side
        for side, verified in (
            ("source", getattr(session, "source_verified", False)),
            ("target", getattr(session, "target_verified", False)),
        )
        if not verified
    ]
    if not missing:
        return None
    names = " and ".join(missing)
    plural = len(missing) > 1
    noun = "connections" if plural else "connection"
    header = "Reconnect first to run the prerequisite checks"
    # The memory-only reason is the source's alone: the target signs in with IAM tokens.
    why = (
        "The source password is kept in memory only, so an app restart or redeploy "
        "clears it. "
        if "source" in missing
        else ""
    )
    body = (
        f"The {names} {noun} {'are' if plural else 'is'} not verified right now. {why}"
        "Editing a connection, or a failed test, clears its verification. Test "
        f"{'them' if plural else 'it'} on the Connect step, then run the checks here."
    )
    short = f"Reconnect first: test the {names} {noun} on the Connect step."
    return header, body, short


def should_pin_cdc_substep(
    *,
    migration_type: MigrationType,
    has_connectors: bool,
    infra_prep_state: Optional[str] = None,
    infra_action_kind: Optional[str] = None,
    infra_action_running: bool = False,
) -> bool:
    """Return whether the view should be pinned to the CDC sub-step.

    The sub-step resolver falls back to ``prerequisites`` (or ``full_load``) whenever
    nothing explicitly stored ``"cdc"``, so any re-render can collapse the CDC section
    and snap the operator back to Prerequisites even though CDC is what they are
    working on. Pinning was originally gated on ``has_connectors`` alone, which left
    two gaps the operator actually hits -- in both, the CDC infrastructure card lives
    at the bottom of Prerequisites, so the work happens there and then the next action
    silently collapses out of view:

    * **Infrastructure ready, streaming not started.** No connectors exist yet, so the
      old gate did not fire: the "CDC infrastructure is ready" notice appeared under
      Prerequisites while Start CDC sat inside a collapsed CDC section.
    * **Infrastructure delete just submitted.** Also no connectors, so submitting the
      teardown bounced the view back to Prerequisites mid-operation.

    ``infra_action_kind`` is the in-flight CDC action (``"infra"`` / ``"delete"`` /
    ``"stop"`` / ``"start"``) and ``infra_action_running`` whether it is still
    PENDING/RUNNING; ``infra_prep_state`` is
    :func:`~dsql_migrator.ui.data_migration._cdc_ui.cdc_infra_prep_state`.

    Scoped to types that HAVE a CDC step, and the infra-readiness arm is scoped to
    ``CDC_ONLY``: for the combined type a finished Full Load deliberately keeps its
    results on screen and the operator advances with "Continue to CDC" (see
    :func:`resolve_active_substep_for_type`), so pinning on infra-ready would yank the
    snapshot's stats out of view -- exactly the regression that resolver avoids.
    """
    if "cdc" not in substeps_for_type(migration_type):
        return False
    if has_connectors:
        return True
    # An in-flight infra create/teardown IS the CDC work; don't bounce away from it.
    if infra_action_running and infra_action_kind in ("infra", "delete", "stop"):
        return True
    # Ready but not started: Start CDC is the next action, so show it.
    return migration_type is MigrationType.CDC_ONLY and infra_prep_state == "ready"


def resolve_active_substep_for_type(
    active: Optional[str],
    *,
    migration_type: MigrationType,
    has_job: bool,
    full_load_done: bool = False,
) -> str:
    """Resolve which sub-step the stepper should show for ``migration_type``.

    Honors an explicit ``active`` choice when it is valid for the type. When the
    combined type's Full Load has completed (``full_load_done``) without an
    explicit choice, the view STAYS on ``"full_load"`` so the operator can review
    the finished snapshot's stats (row counts, completeness, watermark) and then
    advance to CDC themselves via the "Continue to CDC" button -- it does NOT
    auto-advance to ``"cdc"`` (which would whisk the Full Load results out of
    view the instant it finishes). Otherwise defaults to the active load
    (``"full_load"`` when a job exists and the type has that step) or
    ``"prerequisites"``. ``full_load_done`` is retained for call-site/back-compat
    but no longer forces an auto-advance.
    """
    valid = substeps_for_type(migration_type)
    if active in valid:
        return active  # type: ignore[return-value]
    if full_load_done and "full_load" in valid:
        # Full Load finished: keep its results on screen (no auto-advance to CDC).
        return "full_load"
    return "full_load" if has_job and "full_load" in valid else "prerequisites"


def resolve_active_substep(active: Optional[str], *, has_job: bool) -> str:
    """Backward-compatible resolver over the full set of sub-steps.

    Retained for existing callers/tests; honors any of the known sub-steps
    (``_SUBSTEPS``) when given explicitly, else defaults to Full Load once a job
    exists and Prerequisites before that. New type-aware flows use
    :func:`resolve_active_substep_for_type`.
    """
    if active in _SUBSTEPS:
        return active  # type: ignore[return-value]
    return "full_load" if has_job else "prerequisites"


@dataclass(frozen=True)
class _MigrationTypeMeta:
    """Display metadata for a migration type (selector label + header blurb).

    ``when`` is a short "choose this when…" cue and ``requirements`` is a short
    upfront note of what the mode needs (e.g. CDC's MSK provisioning + source
    binlog settings), so the cost/prerequisites of choosing CDC are visible at
    decision time instead of only later in the prerequisite checks.
    """

    label: str
    icon: str
    blurb: str
    when: str = ""
    requirements: str = ""


# Selector label, header icon, one-line blurb, a "when to use" cue, and an upfront
# requirements note per migration type.
_MIGRATION_TYPE_META: dict[MigrationType, _MigrationTypeMeta] = {
    MigrationType.FULL_LOAD_ONLY: _MigrationTypeMeta(
        label="Full load only",
        icon="bolt",
        blurb=(
            "One-shot Full Load: pick tables, check Prerequisites, then run it. "
            "Progress and any data errors update automatically while the job runs."
        ),
        when="Choose for a one-time copy or a maintenance-window cutover.",
        requirements="No extra infrastructure.",
    ),
    MigrationType.CDC_ONLY: _MigrationTypeMeta(
        label="CDC only",
        icon="sync",
        blurb=(
            "Continuous CDC via the optional managed pipeline: check Prerequisites, "
            "then review the CDC setup. Stand-alone (no Full Load in this session) -- "
            "start from a prior watermark or an external start position."
        ),
        when="Choose to resume/attach streaming to an already-loaded target.",
        requirements=(
            "Needs a managed MSK pipeline and source change-data-capture enabled "
            "(MySQL binlog in ROW mode, or PostgreSQL logical replication)."
        ),
    ),
    MigrationType.FULL_LOAD_AND_CDC: _MigrationTypeMeta(
        label="Full load + CDC",
        icon="merge",
        blurb=(
            "Full Load then CDC, end to end: run the Full Load, and when it finishes "
            "the CDC step opens automatically -- seeded from the Full Load "
            "watermark for a gapless hand-off (no manual seed point)."
        ),
        when="Choose for near-zero-downtime cutover (snapshot, then stay in sync).",
        requirements=(
            "Needs a managed MSK pipeline and the source MySQL binlog enabled in "
            "ROW mode."
        ),
    ),
}


def migration_type_blurb(
    mt: MigrationType, source_type: SourceType = SourceType.MYSQL
) -> str:
    """Source-aware tile blurb, which ``workflow.py`` also re-renders as the step BANNER.

    The MySQL wording is the static baseline, so existing callers are unchanged. The
    PostgreSQL CDC-only blurb drops two clauses that are FALSE on that engine, and it is not
    a cosmetic difference: this string stands above every screen of the Data Migration step.

    * "start from a prior watermark" -- only when that watermark carries the SLOT a
      Full-load-+-CDC run created; a Full-load-only watermark's LSN is not a usable start
      point, because a slot cannot be created at a past position.
    * "or an external start position" -- there is none. ``_cdc_resume_signal`` DISCARDS the
      operator's override for PostgreSQL, because Debezium PostgreSQL resumes only from a
      slot's committed position and cannot be told to start from an arbitrary WAL LSN.

    Note what this deliberately does NOT do: disable the tile. CDC-only is valid and GAPLESS
    on PostgreSQL whenever a previous Full-load-+-CDC run recorded the slot -- and job state
    (including the watermark) is persisted and reloaded on startup, so a LATER session sees
    it. Disabling the tile would also remove the re-snapshot continuation, which is the only
    route left after a Full-load-only run. The fix is honesty, not unavailability.
    """
    meta = _MIGRATION_TYPE_META[mt]
    if source_type is not SourceType.POSTGRES:
        return meta.blurb
    if mt is MigrationType.CDC_ONLY:
        return (
            "Continuous CDC via the optional managed pipeline: check Prerequisites, "
            "then review the CDC setup. Stand-alone (no Full Load in this session) — "
            "resumes with no gap only if an earlier \"Full load + CDC\" run left its "
            "replication slot; otherwise it re-snapshots every selected table first."
        )
    if mt is MigrationType.FULL_LOAD_AND_CDC:
        return (
            meta.blurb
            + " The replication slot is created at the snapshot point BEFORE the load, so "
            "the handoff has no gap — and the CDC phase can be started later, not "
            "necessarily in this session."
        )
    return meta.blurb


def migration_type_when(
    mt: MigrationType, source_type: SourceType = SourceType.MYSQL
) -> str:
    """Source-aware "choose this when…" cue for a migration-type tile.

    The last engine-blind string on the card. MySQL keeps the static baseline in
    ``_MIGRATION_TYPE_META`` -- byte-identical, and true: a CDC-only start seeds the
    connector from the watermark's binlog coordinates, so it really does attach to an
    already-loaded target. On PostgreSQL that cue RECOMMENDED the tile for exactly the case
    it cannot serve: with no replication slot recorded, CDC-only does not attach, it
    re-snapshots every selected table through the streaming pipeline (much slower than the
    bulk loader, and it never removes rows deleted on the source since the load). The cue is
    the only string on the tile that makes a RECOMMENDATION, so it names the ALTERNATIVE --
    the blurb already describes the behaviour and the requirements note already lists the
    preconditions, and neither is restated here.

    Only CDC-only differs. "Full load only" and "Full load + CDC" read the same on both
    engines (their PostgreSQL consequences are carried by the tradeoff note and the blurb
    respectively), so they keep the baseline rather than gaining a cue that says nothing
    new. Pure.
    """
    meta = _MIGRATION_TYPE_META[mt]
    if mt is not MigrationType.CDC_ONLY or source_type is not SourceType.POSTGRES:
        return meta.when
    return (
        "Choose to resume a stream whose replication slot still exists; without one, "
        "\"Full load + CDC\" re-loads faster than this mode's re-snapshot."
    )


def migration_type_requirements(
    mt: MigrationType, source_type: SourceType = SourceType.MYSQL
) -> str:
    """Source-aware upfront requirements note for a migration-type tile.

    CDC needs the source's change stream enabled, and how that is enabled differs
    by engine: MySQL's binlog in ROW mode vs. PostgreSQL logical replication
    (``wal_level=logical`` + a ``pgoutput`` publication). Non-CDC types keep their
    static note (no such requirement). Defaults to the MySQL wording baked into
    ``_MIGRATION_TYPE_META`` when the engine is unknown, so existing MySQL callers
    are unchanged and only a PostgreSQL source gets the re-worded requirement --
    the same source-aware pattern used elsewhere in the CDC flow.
    """
    meta = _MIGRATION_TYPE_META[mt]
    if mt not in _CDC_MIGRATION_TYPES or source_type is not SourceType.POSTGRES:
        return meta.requirements
    base = (
        "Needs a managed MSK pipeline and the source's change stream enabled — "
        "PostgreSQL logical replication (wal_level=logical)."
    )
    # Split by TYPE, because on PostgreSQL the two modes differ in a way that decides
    # whether the handoff can be gapless at all: only the combined type creates the
    # publication and slot, and only BEFORE the load, when a slot can still be positioned
    # at the snapshot point. Saying "a pgoutput publication" to both implied the CDC-only
    # tile would arrange one. "MSK" must stay in the string -- the needs-infra icon keys
    # on it.
    if mt is MigrationType.CDC_ONLY:
        return (
            base
            + " The publication and replication slot must ALREADY exist on the source: "
            'this mode does not create them; only "Full load + CDC" does, at the snapshot '
            "point. Without them CDC can still start by re-snapshotting every selected "
            "table."
        )
    return (
        base
        + " The tool creates the publication and replication slot itself, at the Full "
        "Load snapshot point, so the handoff has no gap."
    )


def migration_type_tradeoff(
    mt: MigrationType, source_type: SourceType = SourceType.MYSQL
) -> str:
    """The consequence of picking this tile that is not visible from its own copy.

    Only "Full load only" on PostgreSQL has one, and it is decided HERE, irreversibly:
    nothing retains WAL during the load, and a logical replication slot cannot be
    created at -- or rewound to -- a past position (live-verified on PG 16 and 18:
    ``pg_create_logical_replication_slot`` takes no LSN, ``pg_replication_slot_advance``
    is forward-only). So the changes committed during and after the load are gone for
    replication purposes, and adding CDC later costs a second read of every table.

    Stated on the tile because that is where the decision is made. "Full load + CDC"
    carries the mirror-image note ("the CDC phase can be started later"), but an
    operator who reads only THIS tile -- the cheap, no-infrastructure one -- had no way
    to know it was the one choice that forfeits a gapless handoff for good. MySQL is
    unaffected: a CDC-only start seeds the connector from the watermark's binlog
    coordinates, so the handoff stays gapless as long as the binlogs are retained.

    Returns "" when there is nothing extra to say. Pure.
    """
    if mt is not MigrationType.FULL_LOAD_ONLY or source_type is not SourceType.POSTGRES:
        return ""
    return (
        "On PostgreSQL this gives up a gapless CDC handoff for good: nothing retains "
        "WAL during the load and a replication slot cannot be created at a past "
        "position, so adding CDC afterwards means re-snapshotting every table (or "
        'starting over as "Full load + CDC"). Pick one of those if you may want CDC '
        "later."
    )


# The Evaluation rule whose finding only matters once CDC is in scope: foreign keys
# with automatic referential actions (ON DELETE/UPDATE CASCADE|SET NULL) change child
# rows inside InnoDB, so those changes never reach the binary log and CDC cannot
# replicate them (MySQL bug #32506). See core/assessor.py.
_FK_CASCADE_CDC_RULE_ID = "FK_CASCADE_CDC_GAP"


def cdc_cascade_gap_tables(assessment: object, tables=None) -> list[str]:
    """Return the tables whose FK cascades CDC cannot replicate (sorted, de-duped).

    The deterministic assessment already detects this at Evaluation time, but the
    finding is CDC-specific -- and it was surfaced only in the Evaluation report,
    which the user reads BEFORE deciding whether CDC is in scope. Its own
    recommendation opens with "Before starting CDC", so it belongs next to the CDC
    choice too: silently orphaned child rows on the target are exactly the kind of
    divergence that is expensive to find later.

    Accepts any object exposing ``items`` (the real ``AssessmentReport`` or a test
    double) and degrades to ``[]`` when absent/unreadable, so a decorative surface can
    never break the screen. ``tables`` (the migrated selection) keeps only those: CDC
    captures nothing else; empty or ``None`` keeps every finding. Pure.
    """
    items = getattr(assessment, "items", None) or []
    names: list[str] = []
    for item in items:
        if getattr(item, "rule_id", None) != _FK_CASCADE_CDC_RULE_ID:
            continue
        name = getattr(item, "object_name", None)
        if name and name not in names:
            names.append(str(name))
    if tables:
        keep = set(tables)
        names = [n for n in names if n in keep]
    return sorted(names)


def cdc_prerequisite_block_header(
    report: Optional[PrerequisiteReport],
    *,
    cdc_checks_already_passed: bool = False,
) -> str:
    """The notice header that matches :func:`cdc_prerequisite_block_reason`'s verdict.

    "Run the CDC prerequisite checks first" is right when the checks have not run, and
    WRONG once they have and one of them failed -- which is the whole point of the new
    existence gate. Pure; mirrors the reason function's own branch order.
    """
    if report is None and not cdc_checks_already_passed:
        return "Run the CDC prerequisite checks first"
    return "A CDC prerequisite is failing"


# Required FAILs that must NOT block the CDC actions. Everything not listed blocks -- see the
# denylist rationale in cdc_prerequisite_block_reason.
#
# EXACTLY ONE entry, and the shortness is the point: an audit of all 23 check ids found that
# every state which legitimately should not stop CDC is ALREADY graded required=False
# (BINLOG_RETENTION, GTID_MODE, MSK_AVAILABLE, MSK_CONNECT_AVAILABLE, SELECT_GRANT_SCOPE, the
# REPLICATION_GRANTS role downgrade, SLOT_WAL_RETENTION, the REPLICATION_SLOTS full-pool
# states, CDC_REPLICATION_OBJECTS's SKIP/INFO/WARN, REPLICA_IDENTITY's non-PK-index WARN,
# REPLICA_IDENTITY_COVERS_KEY's unreadable-identity INFO). So the denylist cannot over-block
# by accident -- the required flag already carries that distinction.
#
# TARGET_SCHEMA_READY is the one exception, and it is deliberate on two grounds. Its
# remediation points OFF this screen ("Apply converted DDL for X in Step 2"), and the Deploy
# card sits at the bottom of Prerequisites precisely so the MSK create can overlap the Full
# Load -- which is what applies that DDL. Blocking here would forfeit the overlap to report a
# problem the Full Load guard already owns.
#
# TABLE_PRIMARY_KEY is deliberately NOT exempt, though an earlier draft of this list had it.
# Its own remediation names CDC, not the load ("CDC keying and idempotent upsert require
# it"), the sink throws "Cannot build upsert for table <t>: record has no key (pk) fields"
# and dead-letters every record for that table, and the app's own phase taxonomy already tags
# it "Full Load + CDC" -- so the panel was rendering "CDC: Blocked" for it while both buttons
# stayed enabled.
_CDC_NON_BLOCKING_FAILURES: frozenset = frozenset(
    {PrerequisiteCheckId.TARGET_SCHEMA_READY}
)


def cdc_prerequisite_block_reason(
    report: Optional[PrerequisiteReport],
    *,
    cdc_checks_already_passed: bool = False,
) -> Optional[str]:
    """Why the CDC lifecycle must not proceed, or ``None`` when it may.

    The CDC actions (Deploy infrastructure / Start CDC) previously had NO
    prerequisite gate of their own -- they were reachable only because the linear
    sub-step order (Prerequisites -> Full Load -> CDC) happened to put the checks
    first. That ordering was an implicit guarantee established by choosing the
    migration type early; once the type can change late (or the deploy is offered
    beside Prerequisites), the guarantee has to be explicit.

    Gates on the two things that make CDC *possible at all* on the source, both
    checked only in ``MigrationMode.CDC``:

    * the report exists (the CDC-mode checks have actually run), and
    * the source was reachable -- otherwise the checker ran none of the source checks,
      so the reason given is the reachability failure, not their not-checked rows, and
    * the engine's change-stream check passed -- ``BINLOG_ROW_FORMAT`` for MySQL
      (Debezium cannot read a ``STATEMENT``/``MIXED`` binlog, nor reconstruct rows
      without ``binlog_row_image=FULL``), or ``WAL_LEVEL_LOGICAL`` for PostgreSQL
      (pgoutput needs ``wal_level=logical``). A report carries only its own engine's
      check, so gating on the MySQL one alone blocked every PostgreSQL CDC deploy.
    * ``CDC_REPLICATION_OBJECTS`` did not FAIL -- CDC's publication is absent, omits a
      selected table, or narrows its publish list, any of which means streaming cannot
      work against this source at all (the Debezium task dies within seconds, or reports
      RUNNING while replicating nothing). Exactly the same GRADE as the change-stream
      check above, so it belongs beside it. The row's own remediation says "Fix this
      BEFORE deploying the CDC infrastructure -- MSK Serverless and both connectors are
      billed from creation", and until this gate existed the button it referred to was
      still enabled: the cluster was created, billing started, and Start CDC then refused,
      leaving a cluster nobody could use (~$1.4-2.0/hour).

    Without this, a source on the wrong binlog format surfaced only as an
    undiagnosed connector ``CREATE_FAILED`` ~26 min into a billable create; and
    fixing it needs an RDS parameter-group change plus a reboot, so it must be known
    before any infrastructure is paid for.

    Deliberately does NOT gate on ``report.can_proceed``: that also covers per-table
    ``TARGET_SCHEMA_READY`` / ``TABLE_PRIMARY_KEY`` failures, which the Full Load
    guard already owns and which do not make streaming impossible. Gating on
    ``CDC_REPLICATION_OBJECTS``'s FAIL specifically -- not on ``can_proceed`` -- keeps that
    distinction, and the row's GRADING already draws the line for us: a missing SLOT is a
    WARN (such a start re-snapshots, so it costs a re-read, not correctness), unread facts
    are INFO, and the whole row SKIPs in "Full load + CDC" where the objects are supposed
    to be absent. So this gate fires on the one state that makes streaming impossible and
    on nothing else -- verified by execution over all five states.

    ``cdc_checks_already_passed`` excuses an ABSENT report. The reports live in process
    memory only and are deliberately never persisted, so they vanish on an app restart.
    (Nothing clears them otherwise -- this docstring used to claim the Full Load does,
    which was never true; the start path only records the gated mode.) Without this the
    gate
    punished the normal Full-load-+-CDC flow: run the CDC prerequisites, let the load
    finish, and "Deploy CDC infrastructure" was blocked telling you to run checks you
    had already run. Callers pass the recorded gated mode (the load could only have
    STARTED once the CDC-superset checks passed), which is durable. A report that is
    present but failing still blocks -- that is a live signal. Pure/unit-testable.
    """
    if report is None:
        if cdc_checks_already_passed:
            return None
        return (
            # Engine-neutral on purpose. "the source binary log" is MySQL's mechanism, and
            # this is the branch a PostgreSQL operator continuing a Full-load-only run is
            # GUARANTEED to hit first (that run records prereq_gated_mode=FULL_LOAD, so the
            # checks never count as already passed) -- so the MySQL wording was the first
            # thing the PG journey said. The branch below had already been engine-split.
            "Run the CDC prerequisite checks first (Prerequisites step) — they verify the "
            "source's change stream is usable for streaming (MySQL: the binary log; "
            "PostgreSQL: wal_level=logical and the replication objects) before any "
            "billable infrastructure is created."
        )
    # An unreachable source comes FIRST. The checker then reports the change-stream check
    # as not checked (SKIP), and the stream branch below would read that as "not passed"
    # and send the operator to change a parameter group and reboot a source that may
    # already be configured correctly. The reachability row says what is actually wrong.
    reach = next(
        (
            r
            for r in report.results
            if r.check_id is PrerequisiteCheckId.SOURCE_REACHABLE
        ),
        None,
    )
    if reach is not None and reach.status is PrerequisiteStatus.FAIL:
        parts = [
            f"{reach.title} failed.",
            (reach.detail or "").strip(),
            (reach.remediation or "").strip(),
        ]
        return " ".join(p for p in parts if p)
    # The "CDC possible at all" source check differs by engine: MySQL needs the binary
    # log in ROW format with a FULL row image (BINLOG_ROW_FORMAT); PostgreSQL needs
    # wal_level=logical (WAL_LEVEL_LOGICAL). A report carries only its own engine's
    # check, so gate on whichever is present -- keying on BINLOG_ROW_FORMAT alone left
    # every PostgreSQL Full Load + CDC unable to deploy (its report has no binlog check,
    # so the gate always fired).
    stream = next(
        (
            r
            for r in report.results
            if r.check_id
            in (
                PrerequisiteCheckId.BINLOG_ROW_FORMAT,
                PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
            )
        ),
        None,
    )
    if stream is None or stream.status is not PrerequisiteStatus.PASS:
        detail = (stream.detail or "").strip() if stream is not None else ""
        suffix = f" {detail}" if detail else ""
        return (
            "CDC needs the source change stream enabled (MySQL: binary log in ROW "
            "format with a FULL row image; PostgreSQL: wal_level=logical) — that check "
            f"has not passed.{suffix} Fix it on the source (an RDS parameter-group "
            "change needs a reboot), then re-run the checks."
        )
    objects = next(
        (
            r
            for r in report.results
            if r.check_id is PrerequisiteCheckId.CDC_REPLICATION_OBJECTS
        ),
        None,
    )
    if objects is not None and objects.status is PrerequisiteStatus.FAIL:
        # Carry the row's OWN detail + remediation, so the disabled button's tooltip shows
        # the route (including where the re-snapshot option is) instead of sending the
        # operator back to the prerequisite table to find out.
        parts = [
            (objects.detail or "").strip(),
            (objects.remediation or "").strip(),
        ]
        return " ".join(p for p in parts if p) or (
            "CDC's publication and replication slot do not exist on the source."
        )
    # EVERY OTHER required FAIL, minus an explicit non-blocking list. This used to be a
    # third allowlist entry, and that shape failed twice: CDC_REPLICATION_OBJECTS had to be
    # retro-fitted in v0.1.509, and REPLICA_IDENTITY_COVERS_KEY (v0.1.516) was added to the
    # checker WITHOUT being added here -- so it diagnosed a silently-lost DELETE in perfect
    # detail while Deploy and Start stayed enabled, and a comment elsewhere claimed it was
    # "the authoritative gate". An allowlist fails SILENTLY (a new blocking check enforces
    # nothing); a denylist fails LOUDLY (an over-blocked button is reported in minutes). So
    # the default is now to block, and anything that must not block says so by name below.
    blocking = [
        r
        for r in report.results
        if r.required
        and r.status is PrerequisiteStatus.FAIL
        and r.check_id not in _CDC_NON_BLOCKING_FAILURES
    ]
    if blocking:
        first = blocking[0]
        where = f" ({first.target})" if first.target else ""
        parts = [
            f"{first.title}{where} failed.",
            (first.detail or "").strip(),
            (first.remediation or "").strip(),
        ]
        if len(blocking) > 1:
            parts.append(f"({len(blocking) - 1} more required check(s) also failed.)")
        return " ".join(p for p in parts if p)
    return None
