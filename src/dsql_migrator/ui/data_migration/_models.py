"""Pure view-models, formatters, and enums for the Data Migration screen.

Progress aggregation, the unified Full Load status/table views, prerequisite
grouping, watermark formatting, and the CDC handling read-models live here,
split out of the screen module so they can be unit tested without NiceGUI. The
screen package re-exports every name from this module, so the public import
surface is unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Sequence

from dsql_migrator.core.assessor import (
    _OVERSIZED_LOB_BASES,
    _base_type,
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

        With a known source count it is ``rows_present / expected`` (capped at
        100), so rows that already existed on the target (skipped) count as
        progress; without a count it is 100 for a finished table and ``None``
        while the table has not finished (the loader reports rows only on
        completion).
        """
        if self.expected_rows is None or self.expected_rows <= 0:
            return 100.0 if self.state == "DONE" else None
        return min(100.0, round(self.rows_present / self.expected_rows * 100.0, 1))

    @property
    def complete(self) -> Optional[bool]:
        """Whether a finished table now holds every source row.

        ``None`` until the table is ``DONE`` or when no source count is known to
        compare against; otherwise ``True`` iff every source row is present --
        newly loaded OR already on the target (skipped) -- i.e.
        ``rows_loaded + rows_skipped >= expected_rows``. Using rows-present (not
        just newly inserted) avoids a false mismatch for a table whose rows
        pre-existed on the target (the idempotent load skips them).
        """
        if self.state != "DONE" or self.expected_rows is None:
            return None
        return self.rows_present >= self.expected_rows


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
            error_message=messages.get(chunk.chunk_id),
            started_at=chunk.started_at,
            finished_at=chunk.finished_at,
        )
        for chunk in job.chunks
    ]


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

    @property
    def all_complete(self) -> bool:
        return (
            self.total > 0
            and self.settled == self.total
            and self.failed == 0
            and not self.mismatched
            and self.unknown == 0
        )


def full_load_completeness(rows: Sequence[FullLoadTableRow]) -> FullLoadCompleteness:
    """Summarize source-vs-loaded completeness across all Full Load tables."""
    total = len(rows)
    settled = sum(1 for r in rows if r.state in ("DONE", "FAILED"))
    failed = sum(1 for r in rows if r.state == "FAILED")
    complete = sum(1 for r in rows if r.complete is True)
    mismatched = [r.table for r in rows if r.complete is False]
    unknown = sum(
        1 for r in rows if r.state == "DONE" and r.expected_rows is None
    )
    return FullLoadCompleteness(
        total=total,
        settled=settled,
        complete=complete,
        failed=failed,
        mismatched=mismatched,
        unknown=unknown,
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
        """True when the target's high-water PK has reached the source's.

        Independent of the row COUNT: True even if rows are missing mid-stream, as
        long as the newest source row has landed. ``None`` when ``pk_gap`` is
        unknown. Lets the UI say "stream caught up, but N rows missing mid-stream"
        vs. "stream is N behind".
        """
        g = self.pk_gap
        return None if g is None else g <= 0

    @property
    def cdc_applied_net(self) -> Optional[int]:
        """Net rows CDC applied since Full Load (``target - full_load_rows``).

        ``None`` until both the Full Load row count and a live target count are
        known. Can be negative if the stream net-deleted rows. This is a net row
        delta, not a raw insert/update/delete event count.
        """
        if self.target_rows is None or self.full_load_rows is None:
            return None
        return self.target_rows - self.full_load_rows

    @property
    def delta(self) -> Optional[int]:
        """Rows the target is behind the source now (``source - target``), or None."""
        if self.source_rows is None or self.target_rows is None:
            return None
        return self.source_rows - self.target_rows

    @property
    def in_sync(self) -> Optional[bool]:
        """True when target == source (caught up); None when either is unknown."""
        d = self.delta
        return None if d is None else d == 0

    @property
    def consistency(self) -> str:
        """A plain-language consistency verdict for this table.

        Uses the row-count delta AND the stream high-water (PK) mark, so a stream
        that is genuinely lagging is told apart from one that has caught up its
        leading edge but is missing rows in the middle:

        - ``"quarantined"`` -- DLQ has events that never reached the target (data
          is missing); this wins over everything else.
        - ``"consistent"`` -- target row count equals source.
        - ``"behind"`` -- counts differ AND the stream high-water PK trails the
          source (newest source rows not yet applied -- CDC is catching up).
        - ``"gap"`` -- counts differ but the stream high-water PK has caught up
          (latest rows landed, yet rows are missing mid-stream -- a real gap to
          investigate, not mere lag).
        - ``"ahead"`` -- target exceeds source (unusual).
        - ``"unknown"`` -- counts not yet fetched.
        """
        if self.dlq_count:
            return "quarantined"
        d = self.delta
        if d is None:
            return "unknown"
        if d == 0:
            return "consistent"
        if d < 0:
            return "ahead"
        # Counts differ and target is short. Distinguish lag from a mid-stream gap
        # via the high-water PK: if the newest source row has NOT landed -> behind;
        # if it has (or PK comparison unavailable) -> a gap to investigate.
        caught = self.stream_caught_up
        if caught is False:
            return "behind"
        if caught is True:
            return "gap"
        return "behind"  # PK signal unavailable: default to the lag reading


def build_migration_table_status(
    table_names: "Sequence[str]",
    *,
    full_load_job: "Optional[MigrationJob]" = None,
    target_counts: "Optional[dict[str, Optional[int]]]" = None,
    source_counts: "Optional[dict[str, Optional[int]]]" = None,
    dlq_counts: "Optional[dict[str, int]]" = None,
    source_max_pk: "Optional[dict[str, Optional[int]]]" = None,
    target_max_pk: "Optional[dict[str, Optional[int]]]" = None,
    source_is_estimate: bool = True,
) -> list["MigrationTableStatus"]:
    """Assemble the per-table migration status for ``table_names`` (pure).

    Reads the Full Load state + loaded rows + source snapshot estimate from
    ``full_load_job`` (its chunks/watermark), then overlays the live
    ``target_counts`` (exact target ``COUNT(*)``) and, when available, exact
    ``source_counts`` (else the watermark estimate is used as ``source_rows`` and
    flagged ``source_estimate``). ``dlq_counts`` (per-table quarantined-event
    counts from the error log) surfaces changes that did NOT reach the target.
    NiceGUI-agnostic so it can be unit tested with plain dicts; the UI supplies
    the live counts from a read-only poll.
    """
    chunks_by_table: dict[str, ChunkState] = {}
    expected: dict[str, int] = {}
    if full_load_job is not None:
        chunks_by_table = {c.chunk_id: c for c in full_load_job.chunks}
        if full_load_job.watermark is not None:
            expected = dict(full_load_job.watermark.table_row_counts)
    target_counts = target_counts or {}
    source_counts = source_counts or {}
    dlq_counts = dlq_counts or {}
    source_max_pk = source_max_pk or {}
    target_max_pk = target_max_pk or {}

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
        elif name in expected:
            # Fall back to the Full Load watermark snapshot count -- always an
            # approximate (scan-free) figure.
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


def prerequisite_block_reason(report: PrerequisiteReport) -> Optional[str]:
    """Return a run-guard disable reason when prerequisites block the mode.

    Returns ``None`` when ``report.can_proceed`` is ``True``; otherwise an
    English, user-facing reason naming the failed required checks, suitable for a
    disabled Run button's tooltip (Property 14). Mirrors the workflow
    ``run_guards`` contract used by other steps.
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
    return f"Resolve the failed prerequisite(s) before running: {joined}."


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
# to both ends; Source Configuration = MySQL server settings/privileges; Schema &
# Tables = per-table readiness on source and target; Streaming = the optional CDC
# transport (MSK / MSK Connect).
_PREREQ_CATEGORY_BY_CHECK: dict[PrerequisiteCheckId, PrereqCategory] = {
    PrerequisiteCheckId.SOURCE_REACHABLE: PrereqCategory.CONNECTIVITY,
    PrerequisiteCheckId.TARGET_DSQL_REACHABLE: PrereqCategory.CONNECTIVITY,
    PrerequisiteCheckId.TARGET_IAM_AUTH: PrereqCategory.CONNECTIVITY,
    PrerequisiteCheckId.REPLICATION_GRANTS: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.BINLOG_ROW_FORMAT: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.GTID_MODE: PrereqCategory.SOURCE_CONFIG,
    PrerequisiteCheckId.TABLE_PRIMARY_KEY: PrereqCategory.SCHEMA_TABLES,
    PrerequisiteCheckId.TARGET_SCHEMA_READY: PrereqCategory.SCHEMA_TABLES,
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
    if any(r.status is PrerequisiteStatus.INFO for r in results):
        return PrerequisiteStatus.INFO
    return PrerequisiteStatus.PASS


def _category_summary(results: Sequence[PrerequisiteResult]) -> str:
    """Return a short per-status count line for a category (e.g. ``1 failed · 2 passed``)."""
    total = len(results)
    skipped = sum(1 for r in results if r.status is PrerequisiteStatus.SKIP)
    if total and skipped == total:
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
    table_row_counts: dict[str, int]


def format_binlog_coordinate(watermark: Watermark) -> str:
    """Format the binlog ``file:position`` coordinate, or ``"unavailable"``."""
    if watermark.binlog_file and watermark.binlog_position is not None:
        return f"{watermark.binlog_file}:{watermark.binlog_position}"
    if watermark.binlog_file:
        return watermark.binlog_file
    return _UNAVAILABLE


def format_watermark(watermark: Watermark) -> WatermarkDisplay:
    """Format a :class:`Watermark` for display (Requirement 8.5 / Property 11).

    Produces the design's one-line summary ("exported as of
    mysql-bin.000123:45678 (GTID ...), snapshot <ts>") plus the individual
    fields, so the user can see exactly which consistency point the data was
    exported as-of.
    """
    coordinate = format_binlog_coordinate(watermark)
    snapshot_timestamp = watermark.snapshot_timestamp.isoformat()

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
# CDC handling read-models (NiceGUI-agnostic) -- surface the connector-spike
# findings (cdc-handling-design.md / deploy/cdc-stack/SPIKE-RESULTS.md) in the
# CDC screen: oversized-LOB exclusion (H13), DLQ/circuit-breaker reporting
# (H5/H6/H11), connector health + lag (H1/H2/H9), and the handling contract the
# pipeline guarantees (H3/H4/H8). All pure: they take already-gathered facts and
# return display data, mirroring format_watermark / format_error_summary.
# ---------------------------------------------------------------------------

# Aurora DSQL rejects a single text/bytea value over 1 MiB; a row over the Kafka
# client limit kills the source task (spike H13). The CDC screen reuses the
# evaluation OVERSIZED_LOB type set (_OVERSIZED_LOB_BASES) and base-type parser
# (_base_type) imported above, so the exclusion offer stays in lock-step with the
# evaluation rule instead of duplicating the type list.


@dataclass(frozen=True)
class LobExclusionCandidate:
    """One table's columns that CDC can optionally exclude at capture (H13).

    ``columns`` are the column names flagged by the evaluation ``OVERSIZED_LOB``
    rule (MySQL ``mediumtext/longtext/mediumblob/longblob``) whose values can
    exceed the Aurora DSQL 1 MiB per-value limit. Excluding them at capture
    (Debezium ``column.exclude.list``) is the only safe handling for values that
    can also exceed the 8 MiB broker limit -- runtime isolation cannot catch
    those (cdc-handling-design.md §4-b).
    """

    table: str
    columns: tuple[str, ...]


def lob_exclusion_candidates(
    inventory: Optional[SourceInventory],
) -> list[LobExclusionCandidate]:
    """Return the oversized-LOB columns per table, sorted by table name.

    Pure: derived directly from the source inventory's column types (the same
    base-type set the evaluation ``OVERSIZED_LOB`` rule uses), so the CDC screen
    can offer exclusion without re-running evaluation. Primary-key columns are
    never offered (a PK can't be dropped); returns ``[]`` when nothing qualifies.
    """
    if inventory is None:
        return []
    candidates: list[LobExclusionCandidate] = []
    for table in inventory.tables:
        pk = set(table.primary_key)
        columns = tuple(
            column.name
            for column in table.columns
            if _base_type(column.mysql_type) in _OVERSIZED_LOB_BASES
            and column.name not in pk
        )
        if columns:
            candidates.append(
                LobExclusionCandidate(table=table.name, columns=columns)
            )
    candidates.sort(key=lambda c: c.table)
    return candidates


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


# DSQL caps a single value at 1 MiB and the MSK Serverless broker caps a message
# at 8 MiB; the Kafka *client* default is 1 MiB (spike H13). Used for display.
_DSQL_VALUE_LIMIT_MIB = 1
_BROKER_MESSAGE_LIMIT_MIB = 8


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
    # User-friendly role label (e.g. "Source (MySQL -> Kafka)") derived from the
    # connector name; ``name`` keeps the raw connector id for reference/debug.
    label: str = ""


# A connector state Kafka Connect reports as healthy vs. terminal.
_HEALTHY_CONNECTOR_STATES = frozenset({"RUNNING"})
_BAD_CONNECTOR_STATES = frozenset({"FAILED"})


def connector_role_label(name: str) -> str:
    """Map a raw MSK Connect connector name to a user-friendly role label.

    The pipeline is MySQL -> Debezium source -> Kafka/MSK -> custom DSQL sink, so
    a name containing "source"/"debezium" is the source and one containing
    "sink" is the sink. The explicit "source"/"sink" tokens are checked first
    because the source connector name also contains "dsql" (e.g.
    ``mysql-dsql-cdc-spike-debezium-source``), which would otherwise misclassify it.
    Falls back to the raw name when the role can't be inferred, so an unexpected
    connector is never hidden.
    """
    lowered = name.lower()
    if "source" in lowered or "debezium" in lowered:
        return "Source (MySQL → Kafka)"
    if "sink" in lowered or "dsql" in lowered:
        return "Sink (Kafka → Aurora DSQL)"
    return name


def connector_health_rows(
    connector_states: dict[str, str],
    *,
    lag_seconds: Optional[float] = None,
    lag_warn_seconds: float = 30.0,
) -> list[ConnectorHealthRow]:
    """Map connector states (+ worst lag) to display health rows, name-sorted.

    Pure projection of the managed signals already on ``LoadStatusView``:
    ``RUNNING`` is ok, ``FAILED`` is bad, anything else (PROVISIONING/PAUSED/…)
    is warn. The worst-case ``lag_seconds`` is attached to running connectors and
    nudges the tone to ``warn`` past ``lag_warn_seconds`` so a silently growing
    lag is visible. Each row carries a user-friendly role ``label`` (Source/Sink)
    and rows are ordered Source-then-Sink (data-flow order), not by raw name.
    Returns ``[]`` when no connector states are reported.
    """
    # Order by data flow: Source (MySQL->Kafka) before Sink (Kafka->DSQL), then
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
            tone, detail = "bad", "Stopped — a task is not running. Restart required."
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
    expectations before streaming. Pure/static -- the verified behavior from
    cdc-handling-design.md §5.
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
                "deletes are removed from the target."
            ),
            evidence="H7",
        ),
        CdcHandlingFact(
            handled=True,
            title="MySQL types are converted for DSQL",
            detail=(
                "Column types (e.g. ENUM, JSON, DATETIME, generated columns) are "
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
                "An ALTER TABLE on the source is not applied to the target. Apply "
                "schema changes through Schema Conversion separately; rows that no "
                "longer match are set aside to the dead-letter queue."
            ),
            evidence="§4",
        ),
        CdcHandlingFact(
            handled=False,
            title=f"Very large values (over {_DSQL_VALUE_LIMIT_MIB} MiB) must be excluded",
            detail=(
                "A value too large to stream cannot be recovered later. Exclude such "
                "oversized LOB/TEXT columns at capture (the panel below)."
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
            "One-shot Full Load: pick tables, check Prerequisites, then run the "
            "snapshot. Progress, the export watermark, and any data errors update "
            "automatically while the job runs."
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
            "start from a prior watermark or an external GTID."
        ),
        when="Choose to resume/attach streaming to an already-loaded target.",
        requirements=(
            "Needs a managed MSK pipeline and the source MySQL binlog enabled in "
            "ROW mode."
        ),
    ),
    MigrationType.FULL_LOAD_AND_CDC: _MigrationTypeMeta(
        label="Full load + CDC",
        icon="merge",
        blurb=(
            "Full Load then CDC, end to end: run the snapshot, and when it finishes "
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
