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
from typing import Iterable, Mapping, Optional, Sequence

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
        """True when the target's high-water PK has reached the source's.

        Independent of the row COUNT: True even if rows are missing mid-stream, as
        long as the newest source row has landed. ``None`` when ``pk_gap`` is
        unknown. Lets the UI say "stream caught up, but N rows missing mid-stream"
        vs. "stream is N behind".
        """
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


def lob_exclusion_scope_gap(
    checked: "Mapping[str, frozenset[str]]",
    current: "Mapping[str, frozenset[str]]",
) -> list[str]:
    """Return ``table.column`` columns EXCLUDED since the checks ran, sorted.

    ASYMMETRIC ON PURPOSE, mirroring :func:`prereq_scope_gap`. ``checked`` is the
    exclusion the last prerequisite report was validated against; ``current`` is the
    live selection. Only newly-EXCLUDED columns are a gap: excluding a column after
    the checks removes it from the load's column set, so a column that is ``NOT
    NULL``/no-default on the target could flip loadability from PASS to FAIL --
    which the stale report would not show, letting a doomed load start. UN-excluding
    a column (present in ``checked`` but not ``current``) only adds a column back to
    the load, which the checks already covered, so it is never a gap.

    Returns the offending fully-qualified columns (``table.column``); empty means the
    live exclusion is within what the report checked (identical, or a strict subset).
    Pure.
    """
    added: list[str] = []
    for table, cols in current.items():
        prior = checked.get(table, frozenset())
        for col in cols:
            if col not in prior:
                added.append(f"{table}.{col}")
    return sorted(added)


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
    PrerequisiteCheckId.BINLOG_RETENTION: PrereqCategory.SOURCE_CONFIG,
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


def format_watermark(watermark: Watermark) -> WatermarkDisplay:
    """Format a :class:`Watermark` for display (Requirement 8.5 / Property 11).

    Source-agnostic: a MySQL watermark yields the binlog ``file:position`` (+ GTID)
    one-line summary; a PostgreSQL watermark (a WAL LSN + slot + publication, no
    binlog/GTID) yields a "WAL LSN <lsn>" summary and populates the PG fields, so the
    panel shows the coordinate the loader actually captured instead of rendering every
    MySQL field as "unavailable".
    """
    snapshot_timestamp = watermark.snapshot_timestamp.isoformat()
    is_postgres = bool(watermark.wal_lsn)

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

# Aurora DSQL rejects a single text/bytea value over 1 MiB; a row over the Kafka
# client limit kills the source task. The CDC screen reuses the
# evaluation OVERSIZED_LOB type set (_OVERSIZED_LOB_BASES) and base-type parser
# (_base_type) imported above, so the exclusion offer stays in lock-step with the
# evaluation rule instead of duplicating the type list.


@dataclass(frozen=True)
class LobExclusionCandidate:
    """One table's columns that CDC can optionally exclude at capture.

    ``columns`` are the column names flagged by the evaluation ``OVERSIZED_LOB``
    rule (MySQL ``mediumtext/longtext/mediumblob/longblob``) whose values can
    exceed the Aurora DSQL 1 MiB per-value limit. Excluding them at capture
    (Debezium ``column.exclude.list``) is the only safe handling for values that
    can also exceed the 8 MiB broker limit -- runtime isolation cannot catch
    those.
    """

    table: str
    columns: tuple[str, ...]


# PostgreSQL base types whose values can exceed the DSQL 1 MiB per-value limit:
# unbounded ``text`` and ``bytea`` (a length-bounded varchar(n) with small n cannot,
# and json/jsonb are stored differently and are not hit by the text 1 MiB cap the same
# way). The MySQL set (_OVERSIZED_LOB_BASES) does not match PG type names, so a PG
# source would otherwise offer NO exclusions and the panel would falsely report none.
_PG_OVERSIZED_LOB_BASES = frozenset({"text", "bytea"})


def lob_exclusion_candidates(
    inventory: Optional[SourceInventory],
    *,
    source_type: SourceType = SourceType.MYSQL,
) -> list[LobExclusionCandidate]:
    """Return the oversized-LOB columns per table, sorted by table name.

    Pure: derived directly from the source inventory's column types, so the CDC
    screen can offer exclusion without re-running evaluation. The base-type set is
    chosen by ``source_type`` -- MySQL ``mediumtext/longtext/mediumblob/longblob`` vs
    PostgreSQL ``text/bytea`` -- because ``column.mysql_type`` holds the SOURCE engine's
    type name (matching a MySQL set against PG types found nothing). Primary-key columns
    are never offered (a PK can't be dropped); returns ``[]`` when nothing qualifies.
    """
    if inventory is None:
        return []
    bases = (
        _PG_OVERSIZED_LOB_BASES
        if source_type is SourceType.POSTGRES
        else _OVERSIZED_LOB_BASES
    )
    candidates: list[LobExclusionCandidate] = []
    for table in inventory.tables:
        pk = set(table.primary_key)
        columns = tuple(
            column.name
            for column in table.columns
            if _base_type(column.mysql_type) in bases
            and column.name not in pk
        )
        if columns:
            candidates.append(
                LobExclusionCandidate(table=table.name, columns=columns)
            )
    candidates.sort(key=lambda c: c.table)
    return candidates


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
    ``mysql-dsql-cdc-spike-debezium-source``), which would otherwise misclassify it.
    The label stays engine-neutral (the connector name follows the tool's
    ``mysql-dsql-*`` convention regardless of the actual source engine, so it can't
    be used to name MySQL vs. PostgreSQL). Falls back to the raw name when the role
    can't be inferred, so an unexpected connector is never hidden.
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
                "deletes are removed from the target."
            ),
            evidence="H7",
        ),
        CdcHandlingFact(
            handled=True,
            title="Source types are converted for DSQL",
            detail=(
                "Column types (e.g. ENUM, JSON, date/time, generated columns) are "
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
    return (
        "Needs a managed MSK pipeline and the source's change stream enabled — "
        "PostgreSQL logical replication (wal_level=logical + a pgoutput publication)."
    )


# The Evaluation rule whose finding only matters once CDC is in scope: foreign keys
# with automatic referential actions (ON DELETE/UPDATE CASCADE|SET NULL) change child
# rows inside InnoDB, so those changes never reach the binary log and CDC cannot
# replicate them (MySQL bug #32506). See core/assessor.py.
_FK_CASCADE_CDC_RULE_ID = "FK_CASCADE_CDC_GAP"


def cdc_cascade_gap_tables(assessment: object) -> list[str]:
    """Return the tables whose FK cascades CDC cannot replicate (sorted, de-duped).

    The deterministic assessment already detects this at Evaluation time, but the
    finding is CDC-specific -- and it was surfaced only in the Evaluation report,
    which the user reads BEFORE deciding whether CDC is in scope. Its own
    recommendation opens with "Before starting CDC", so it belongs next to the CDC
    choice too: silently orphaned child rows on the target are exactly the kind of
    divergence that is expensive to find later.

    Accepts any object exposing ``items`` (the real ``AssessmentReport`` or a test
    double) and degrades to ``[]`` when absent/unreadable, so a decorative surface can
    never break the screen. Pure.
    """
    items = getattr(assessment, "items", None) or []
    names: list[str] = []
    for item in items:
        if getattr(item, "rule_id", None) != _FK_CASCADE_CDC_RULE_ID:
            continue
        name = getattr(item, "object_name", None)
        if name and name not in names:
            names.append(str(name))
    return sorted(names)


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
    * the engine's change-stream check passed -- ``BINLOG_ROW_FORMAT`` for MySQL
      (Debezium cannot read a ``STATEMENT``/``MIXED`` binlog, nor reconstruct rows
      without ``binlog_row_image=FULL``), or ``WAL_LEVEL_LOGICAL`` for PostgreSQL
      (pgoutput needs ``wal_level=logical``). A report carries only its own engine's
      check, so gating on the MySQL one alone blocked every PostgreSQL CDC deploy.

    Without this, a source on the wrong binlog format surfaced only as an
    undiagnosed connector ``CREATE_FAILED`` ~26 min into a billable create; and
    fixing it needs an RDS parameter-group change plus a reboot, so it must be known
    before any infrastructure is paid for.

    Deliberately does NOT gate on ``report.can_proceed``: that also covers per-table
    ``TARGET_SCHEMA_READY`` / ``TABLE_PRIMARY_KEY`` failures, which the Full Load
    guard already owns and which do not make streaming impossible.

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
            "Run the CDC prerequisite checks first (Prerequisites step) — they "
            "verify the source binary log is usable for streaming before any "
            "billable infrastructure is created."
        )
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
    return None
