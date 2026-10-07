# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Control + read-only monitoring of already-deployed MSK Connect connectors.

The tool's UI does NOT deploy the cdc-stack (decision-change 8) -- the CDC data
plane (MSK Serverless + MSK Connect, Debezium source + custom DSQL sink) is
provisioned separately via CloudFormation. This module lets the control plane
*observe and stop* those already-deployed connectors:

  * read-only: ``list_connectors`` / ``describe_connector`` for connector state +
    task health, and CloudWatch ``MilliSecondsBehindSource`` for replication lag.
  * one guarded mutation: ``delete_connector`` (MSK Connect has no pause/resume
    API, so "stop" is a delete). The controller performs the delete with NO
    confirmation of its own -- the UI caller owns the type-the-name confirmation
    dialog. Keeping the guard in the UI keeps it reviewable and testable.

Mirrors :class:`~dsql_migrator.ui.prerequisite_probes.SessionMskProbe`: the shared
profile-aware :func:`build_session` builds boto3 clients lazily (so importing or
constructing this never reaches AWS), the ``session`` arg is a test-injection
seam, and every read method turns any access error into a falsy result so the UI
degrades gracefully instead of crashing. Property 1 (read-only except the one
guarded stop) and Property 7 (no credential value is read or logged) hold.
"""

from __future__ import annotations

import contextlib
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Sequence

from dsql_migrator.core.aws_session import BotoSessionLike, build_session
from dsql_migrator.core.cdc import CdcConnectorError, ConnectorState, ConnectorStatus
from dsql_migrator.core.cdc_dlq import parse_dlq_log_message
from dsql_migrator.core.cdc_value_conversion import (
    VALUE_CONVERSION_FILTER_PATTERN,
    CdcValueConversionFailure,
    parse_value_conversion_failure,
)


@dataclass
class ConnectorHealth:
    """CloudWatch task-health snapshot for one connector (latest datapoint).

    All fields are ``None`` when the metric has no recent datapoint or CloudWatch
    could not be read, so the UI shows "unknown" rather than a wrong zero.
    """

    running_tasks: Optional[int] = None
    errored_tasks: Optional[int] = None
    send_rate: Optional[float] = None
    poll_rate: Optional[float] = None

# MSK Connect connectorState -> our coarse ConnectorState. MSK Connect reports
# RUNNING/PAUSED/FAILED/CREATING/UPDATING/DELETING; we map the transient
# lifecycle states to the closest monitoring bucket.
_CONNECTOR_STATE_MAP = {
    "RUNNING": ConnectorState.RUNNING,
    "PAUSED": ConnectorState.PAUSED,
    "FAILED": ConnectorState.FAILED,
    "CREATING": ConnectorState.UNASSIGNED,
    "UPDATING": ConnectorState.UNASSIGNED,
    "DELETING": ConnectorState.PAUSED,
}

# CloudWatch namespace for MSK Connect connector health/throughput metrics.
# NOTE: MSK Connect does NOT publish a replication-lag metric to CloudWatch
# (Debezium's MilliSecondsBehindSource is a JMX metric, not auto-exported). The
# CloudWatch path here covers connector health (running/errored task counts,
# send rate); end-to-end replication lag is measured separately against the DSQL
# target (now - max applied timestamp), which is the most accurate signal and
# needs no cdc-stack change.
_MSK_CONNECT_NAMESPACE = "AWS/KafkaConnect"
_RUNNING_TASKS_METRIC = "RunningTaskCount"
_ERRORED_TASKS_METRIC = "ErroredTaskCount"
_SEND_RATE_METRIC = "SinkRecordSendRate"
# Source connector poll rate: change events read from the binlog per second.
# Together with the sink send rate it tells whether changes are still flowing
# (both ~0 after the source is quiesced = pipeline idle / caught up).
_SOURCE_POLL_RATE_METRIC = "SourceRecordPollRate"

# The custom DSQL sink's per-table applied-ops metrics (connectors/dsql-sink emits a
# per-offset-commit COUNT of inserts / updates / deletes, dimensioned Stack + Table).
# Summed over the window here to get a DMS-style change breakdown per table — a
# source-scan-free CDC signal that replaces COUNT(*)-ing the source in the live
# monitor, and (unlike the old NetRowsApplied) makes UPDATE traffic visible.
_CDC_METRIC_NAMESPACE = "MysqlDsqlMigrator/CDC"
_APPLIED_OP_METRICS = {  # result key -> CloudWatch metric name
    "inserts": "InsertsApplied",
    "updates": "UpdatesApplied",
    "deletes": "DeletesApplied",
}
# CloudWatch GetMetricData accepts at most 500 MetricDataQueries per request. The
# applied-ops read builds 3 queries per matched table, so a large-scale migration
# (>~166 tracked tables) would exceed this in one call -- batch to stay under it.
_GMD_MAX_QUERIES = 500
# End-to-end replication lag (ms) the sink emits per table = apply-wall-clock minus
# the event's source commit time (source.ts_ms). Time-based + PK-agnostic, so it is
# a far more accurate "how far behind is the target" signal than the UI's MAX(pk)
# leading-edge check. Read with Stat=Maximum (worst recent lag) over a short window.
_REPLICATION_LAG_METRIC = "ReplicationLagMs"
# ReplicationLagMs is EVENT-DRIVEN: the sink emits a datapoint only when it applies
# an event. When the source is quiesced and the pipeline drains, emission stops -- so
# the newest datapoint still inside the read window is the LAST applied event's lag,
# not the current state (nothing is in flight -> caught up). Treat a "current lag"
# datapoint older than this cutoff as absent, so a drained pipeline reads as caught up
# (lag drops to 0) instead of freezing at the last value until it ages out of the
# window. Sized above the metric's 1-min resolution + CloudWatch ingestion delay so an
# actively-streaming pipeline (which emits every offset-commit) is never falsely idled.
_LAG_FRESHNESS_SECONDS = 180

# TTL for the per-(stack, metric) Table-dimension discovery cache. Every CDC poll
# (~5 s) reads applied-ops (3 metrics) + per-table lag + the lag trend, each of which
# calls _list_metric_dimensions -- 5 ListMetrics-pagination passes per poll for data
# ("which tables emit this metric") that changes only when a NEW table starts
# emitting. Memoizing discovery collapses the 5 within a poll AND the re-discovery
# across polls to at most one pagination per metric per window, cutting CloudWatch
# ListMetrics calls ~6x at the 5 s poll rate (and the throttling risk with them). A
# table that newly starts emitting appears within this window; the live get_metric_data
# DATA reads are never cached (they must stay current).
_METRIC_DIM_CACHE_TTL_SECONDS = 30

# Initial DLQ look-back on the FIRST poll (before a cursor exists). CDC often runs
# headless / as a resumed stream and dead-letters rows for a while BEFORE an operator
# opens the UI and the monitor first reads the log; a 1 h look-back silently omitted
# every quarantine older than that (and once the cursor moves forward they can never be
# picked up). A wider first-read window captures a realistic late/headless attach's
# backlog; the oldest-first read + forward cursor then drain it over subsequent polls.
# Not unbounded: a per-call `limit` caps each read, and this is only the FIRST window
# (later polls advance from the cursor), so it does not re-scan history every poll.
_DLQ_INITIAL_LOOKBACK_SECONDS = 6 * 3600

# How far behind "now" the DLQ cursor is allowed to trail after a COMPLETE read. The cursor
# used to advance only to the newest event read, so a clean stream never got one: every poll
# re-scanned the whole first-read look-back, and a session left closed for longer than that
# window lost every dead letter older than it on the next read. After a read that reached the
# end, nothing older than this grace can still be on its way into the log group (CloudWatch
# ingestion is seconds), so the cursor may move up to ``now - grace`` -- never backwards and
# always past the newest event already read.
_DLQ_INGEST_GRACE_SECONDS = 600

# How long a page-capped read's continuation is resumed by the next read. FilterLogEvents can
# return EMPTY pages that still carry a nextToken while it scans (documented behaviour), so on
# a busy, clean worker log group a 20-page read can end with nothing matched and no position
# to move the cursor to -- and every retry re-scanned the same first pages for good. The next
# read therefore continues from the stored nextToken (same request, so the same scan). A
# CloudWatch token expires after 24 h; a continuation older than this restarts from the
# cursor instead (seen-id de-dup covers the overlap).
_DLQ_RESUME_MAX_AGE_SECONDS = 3600


@dataclass(frozen=True)
class DlqReadState:
    """What the LAST dead-letter read of one log group proved (see :meth:`dlq_read_state`).

    ``ok`` False: the read failed -- and returned ``[]``, indistinguishable from an empty
    queue -- with ``error_code`` a value-free class (AWS error code or exception class).
    ``truncated``: it stopped at its page cap (the next read continues where it stopped).
    ``history_start_ms``: where this controller's continuous coverage of the group begins
    (its first successful read's start time, or the seeded cursor for a controller rebuilt
    from a saved session), ``None`` until known. ``started_ms``: when the read that produced
    this state BEGAN -- for a read continued across calls, its first call -- so a reader
    that needs "a read started after X" never credits a continuation with a later start.
    """

    ok: bool
    error_code: Optional[str] = None
    truncated: bool = False
    history_start_ms: Optional[int] = None
    started_ms: Optional[int] = None


def _dlq_error_code(exc: BaseException) -> str:
    """A value-free class for a failed DLQ read: the AWS error code, else the exception class."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        code = (response.get("Error") or {}).get("Code")
        if code:
            return str(code)[:64]
    return type(exc).__name__[:64]


def _match_metric_tables(
    requested: Sequence[str], discovered: Sequence[str]
) -> dict[str, str]:
    """Map each requested table name to a published metric ``Table`` dimension value.

    The sink emits ``Table`` schema-qualified (``db.table``); the tool's names may be
    bare or qualified. An exact match wins; otherwise the bare table name (the last
    dotted segment) must match **exactly one** published value. Ambiguous bare matches
    (the same table name under two schemas) are skipped rather than risk attributing
    another schema's rows to the wrong table — that table just falls back to the
    COUNT-based figure. Pure, so it is unit-tested without AWS.
    """
    discovered_set = set(discovered)
    by_bare: dict[str, list[str]] = {}
    for value in discovered:
        by_bare.setdefault(value.rsplit(".", 1)[-1], []).append(value)
    matched: dict[str, str] = {}
    for name in requested:
        if name in discovered_set:  # exact (cluster mode / already qualified)
            matched[name] = name
            continue
        candidates = by_bare.get(name.rsplit(".", 1)[-1], [])
        if len(candidates) == 1:  # unambiguous bare match (single-db mode)
            matched[name] = candidates[0]
        # 0 candidates (not published yet) or >1 (ambiguous) -> skip -> COUNT fallback
    return matched


class MskConnectController:
    """Read-only status + guarded stop over already-deployed MSK Connect connectors."""

    def __init__(
        self,
        region: str,
        *,
        aws_profile: Optional[str] = None,
        session: Optional[BotoSessionLike] = None,
        monotonic: Optional[Callable[[], float]] = None,
    ) -> None:
        """Bind the controller to the MSK Connect region + global AWS profile.

        ``session`` is an injection seam for tests (a fake ``boto3.Session``);
        when omitted the shared profile-aware session is built lazily on first
        use so constructing the controller never reaches AWS. ``monotonic`` is the
        clock seam for the metric-dimension discovery cache (defaults to
        :func:`time.monotonic`); tests inject a fake to exercise TTL expiry.
        """
        self._region = region
        self._aws_profile = aws_profile
        self._session = session
        self._monotonic = monotonic or time.monotonic
        # Cursor + de-dup state for incremental DLQ log reads (see dlq_errors):
        # only events newer than the last seen are returned, and an eventId set
        # guards the timestamp boundary so a record is never surfaced twice.
        # Keyed BY LOG GROUP. A single cursor leaked across pipelines: attaching to
        # another CDC stack (the "Existing CDC infrastructure found" offer) reused the
        # previous stack's cursor, so the adopted pipeline's earlier dead-letters -- older
        # than that cursor but inside the first-read look-back -- were never surfaced and
        # its DLQ card read "0 quarantined" for a pipeline that had quarantined rows.
        self._dlq_cursor_ms: "dict[str, int]" = {}
        self._dlq_seen_ids: "dict[str, set[str]]" = {}
        # Whether a DLQ read's coverage was BOUNDED, and how. Both make the quarantine
        # count a LOWER bound rather than a total, and the number is what a cut-over
        # decision turns on -- so it must be reportable, not implicit.
        # {log_group: window_seconds} for a first read that had no cursor to resume from;
        # {log_group: bool} for a read that hit the page cap.
        self._dlq_first_read_window_seconds: "dict[str, int]" = {}
        self._dlq_read_truncated: "dict[str, bool]" = {}
        # Whether the last read of each group SUCCEEDED, and why not. dlq_errors returns []
        # on any error, so a missing logs:FilterLogEvents grant or a deleted log group looked
        # exactly like a clean queue -- and the cut-over gate turns on that difference.
        self._dlq_read_ok: "dict[str, bool]" = {}
        self._dlq_read_error: "dict[str, Optional[str]]" = {}
        # Where this controller's continuous coverage of each group begins (ms): the start of
        # its first successful read (or the seeded cursor). Set once per group.
        self._dlq_history_start_ms: "dict[str, int]" = {}
        # A page-capped read's continuation, per group: (startTime, nextToken, first-call
        # ms). The next read resumes it instead of re-scanning the same first pages (see
        # _DLQ_RESUME_MAX_AGE_SECONDS). In memory only: a token is opaque and short-lived.
        self._dlq_resume: "dict[str, tuple[int, str, int]]" = {}
        # When the read behind each group's read state began (ms; a continuation's first
        # call), see DlqReadState.started_ms.
        self._dlq_read_started_ms: "dict[str, int]" = {}
        # Serializes overlapping reads (the Data Migration poll and the cut-over check can
        # share one controller), so the cursor and seen-id set are never raced.
        self._dlq_lock = threading.Lock()
        # SEPARATE read position for the value-conversion read (see
        # value_conversion_failures). It reads the SAME log group with a different
        # pattern, and sharing the DLQ cursor above would let the far more frequent
        # conversion line advance it past a dead-letter line that had not been ingested
        # yet -- undercounting the quarantines a cut-over decision reads.
        self._conv_cursor_ms: "dict[str, int]" = {}
        # Short-TTL cache for _list_metric_dimensions, keyed on (stack, metric_name):
        # {key: (expiry_monotonic, dims)}. Collapses the 5 per-poll discovery passes
        # (and cross-poll re-discovery) so ListMetrics is not paged every 5 s for data
        # that changes only when a new table starts emitting. See the TTL constant.
        self._dim_cache: dict[tuple[str, str], tuple[float, list[str]]] = {}

    def seed_dlq_cursor(
        self, cursor_ms: Optional[dict] = None, seen_ids: Optional[dict] = None
    ) -> None:
        """Adopt a DLQ read cursor recorded elsewhere (the UI session).

        The cursor and seen-id set are what stop an already-surfaced dead letter from
        being read -- and durably re-audited -- a second time. They lived only on this
        instance, and the controller is rebuilt on any state reset, app restart or new
        browser session, so each rebuild re-read the blind look-back window and wrote
        every record to the activity log again. Seeding from session state makes a rebuild
        continue where the previous controller left off.
        """
        for group, value in (cursor_ms or {}).items():
            if value is not None:
                self._dlq_cursor_ms[group] = int(value)
        for group, ids in (seen_ids or {}).items():
            self._dlq_seen_ids.setdefault(group, set()).update(ids or ())

    def dlq_cursor_state(self) -> tuple:
        """Return ``(cursor_ms_by_group, seen_ids_by_group)`` for the caller to persist."""
        return (
            dict(self._dlq_cursor_ms),
            {g: set(ids) for g, ids in self._dlq_seen_ids.items()},
        )

    def dlq_coverage_bound(self, log_group: str) -> Optional[str]:
        """Why this group's quarantine count may be INCOMPLETE, or ``None`` if it isn't.

        Two bounds exist and both are silent by construction: a read with no cursor to
        resume from can only look back ``window_seconds`` (anything quarantined before
        that "can never be picked up"), and a read that hits the page cap stops early.
        Either way the DLQ depth is a lower bound -- so a cut-over reader who sees
        "0 quarantined" must be told which, instead of a number that looks like a total.

        Pure: reports what the last read recorded, no AWS call.
        """
        if self._dlq_read_truncated.get(log_group):
            return (
                "the dead-letter read hit its page cap, so more quarantined records "
                "exist than were counted"
            )
        window = self._dlq_first_read_window_seconds.get(log_group)
        if window:
            hours = window // 3600 or 1
            return (
                f"the dead-letter history was read from the last {hours}h only (no "
                "earlier read position to resume from), so any record quarantined "
                "before that is not counted"
            )
        return None

    def _dlq_state_dict(self, name: str) -> dict:
        """The named per-group bookkeeping dict, created on a ``__new__``-built instance."""
        value = getattr(self, name, None)
        if value is None:
            value = {}
            setattr(self, name, value)
        return value

    def dlq_read_state(self, log_group: str) -> Optional[DlqReadState]:
        """What the last dead-letter read of ``log_group`` proved, or ``None`` before any.

        Pure: reports what :meth:`dlq_errors` recorded, no AWS call. The cut-over gate reads
        it because the read itself returns ``[]`` on failure (pinned behaviour), which makes
        "I could not read the queue" indistinguishable from "the queue is empty".
        """
        ok_by_group = getattr(self, "_dlq_read_ok", None) or {}
        if log_group not in ok_by_group:
            return None
        ok = bool(ok_by_group[log_group])
        return DlqReadState(
            ok=ok,
            error_code=(getattr(self, "_dlq_read_error", None) or {}).get(log_group),
            truncated=ok
            and bool((getattr(self, "_dlq_read_truncated", None) or {}).get(log_group)),
            history_start_ms=(getattr(self, "_dlq_history_start_ms", None) or {}).get(
                log_group
            ),
            started_ms=(getattr(self, "_dlq_read_started_ms", None) or {}).get(log_group)
            if ok
            else None,
        )

    def _client(self, service_name: str) -> object:
        session = self._session or build_session(self._aws_profile)
        return session.client(service_name, region_name=self._region)

    def dlq_errors(
        self,
        log_group: str,
        *,
        now: Optional[datetime] = None,
        limit: int = 100,
        window_seconds: int = _DLQ_INITIAL_LOOKBACK_SECONDS,
        max_pages: int = 20,
    ) -> list[CdcConnectorError]:
        """Read NEW sink dead-letter (DLQ) events from the connector log group.

        MSK Connect publishes no DLQ-topic depth metric, but the custom DSQL sink
        logs every quarantined record to its CloudWatch worker log group
        (``/msk-connect/<stack>-cdc``) with the affected topic, Kafka offset and
        reason (``errors.log.include.messages=true``). This filters that group for
        dead-letter lines newer than the last read (a timestamp cursor + eventId
        set de-dup across polls) and parses each into a credential-free
        :class:`CdcConnectorError` (no row values, no SQL). Returns ``[]`` on any
        access error (fail-closed) so the monitor degrades gracefully.

        ``window_seconds`` is the FIRST-read look-back only (once a cursor exists,
        reads are incremental from it). It defaults to
        :data:`_DLQ_INITIAL_LOOKBACK_SECONDS` so a headless / late-attach run's earlier
        quarantines are captured rather than silently dropped -- see that constant.

        Intended to be called on the CDC status poll's worker thread (it does
        blocking network I/O); the caller records the returned errors into the
        single error log so the DLQ depth / per-table "Quarantined" surface
        reflects the real pipeline.

        Each read also records what it PROVED (:meth:`dlq_read_state`): whether it
        succeeded (and the value-free error class when not), whether it hit the page cap,
        and where this controller's continuous coverage of the group begins. After a read
        that reached the end, the cursor also moves up to ``now`` minus
        :data:`_DLQ_INGEST_GRACE_SECONDS` (never backwards, always past the newest event
        read), so a clean stream stops re-scanning the look-back window on every poll.
        A read that stops at its page cap stores its continuation, and the next read
        resumes it (see :data:`_DLQ_RESUME_MAX_AGE_SECONDS`), so repeated reads always make
        progress through a log group too large for one. Overlapping calls are serialized.
        """
        lock = getattr(self, "_dlq_lock", None) or contextlib.nullcontext()
        with lock:
            return self._dlq_errors_locked(
                log_group,
                now=now,
                limit=limit,
                window_seconds=window_seconds,
                max_pages=max_pages,
            )

    def _dlq_errors_locked(
        self,
        log_group: str,
        *,
        now: Optional[datetime],
        limit: int,
        window_seconds: int,
        max_pages: int,
    ) -> list[CdcConnectorError]:
        """The body of :meth:`dlq_errors`, run under the read lock."""
        now_dt = now or datetime.now(timezone.utc)
        now_ms = int(now_dt.timestamp() * 1000)
        cursor = self._dlq_cursor_ms.get(log_group)
        seen = self._dlq_seen_ids.setdefault(log_group, set())
        # A page-capped previous read left a continuation: resume THAT request (the token is
        # tied to its startTime) rather than re-scanning its first pages again.
        resumes = self._dlq_state_dict("_dlq_resume")
        resume = resumes.get(log_group)
        if resume is not None and now_ms - resume[2] > _DLQ_RESUME_MAX_AGE_SECONDS * 1000:
            resumes.pop(log_group, None)
            resume = None

        def _fresh_start() -> int:
            # Remember that THIS read had no cursor and therefore only looked back
            # ``window_seconds``. Anything quarantined before that is, in this module's own
            # words, an event that can never be picked up -- so the count it produces is a
            # LOWER BOUND, and a cut-over decision must be told that rather than shown a
            # number that looks complete.
            if cursor is None:
                self._dlq_first_read_window_seconds[log_group] = int(window_seconds)
            return cursor if cursor is not None else now_ms - window_seconds * 1000

        if resume is not None:
            start_ms, token, chain_started_ms = resume
        else:
            start_ms, token, chain_started_ms = _fresh_start(), None, now_ms

        def _pages(logs, token):
            # FOLLOW nextToken. A single page capped at ``limit`` silently dropped every
            # further dead letter in the window -- and the quarantine count is exactly the
            # number a cut-over decision turns on, so an undercount reads as a cleaner
            # stream than there was. Bounded by ``max_pages`` so a pathological log group
            # cannot turn an advisory poll into an unbounded scan; when the bound is hit we
            # say so (``dlq_read_truncated``) instead of returning a quiet partial count.
            events: list = []
            for _page in range(max(1, max_pages)):
                kwargs = {
                    "logGroupName": log_group,
                    "startTime": start_ms,
                    "filterPattern": (
                        '?"Quarantined record to DLQ" ?"Dropping unapplicable record"'
                    ),
                    "limit": limit,
                }
                if token:
                    kwargs["nextToken"] = token
                response = logs.filter_log_events(**kwargs)
                events.extend(list(response.get("events", []) or []))
                token = response.get("nextToken")
                if not token:
                    return events, None, False
            return events, token, bool(token)

        try:
            logs = self._client("logs")
            try:
                events, token, truncated = _pages(logs, token)
            except Exception:
                if resume is None:
                    raise
                # A continuation CloudWatch no longer accepts (expired, or the group was
                # recreated): drop it and read afresh from the cursor, once.
                resumes.pop(log_group, None)
                resume = None
                start_ms, chain_started_ms = _fresh_start(), now_ms
                events, token, truncated = _pages(logs, None)
        except Exception as exc:  # noqa: BLE001 - advisory monitoring read, never crash
            # Still [] (the monitor degrades gracefully), but the failure is RECORDED: a
            # failed read must not pass for an empty queue at cut over.
            self._dlq_state_dict("_dlq_read_ok")[log_group] = False
            self._dlq_state_dict("_dlq_read_error")[log_group] = _dlq_error_code(exc)
            return []
        if truncated:
            resumes[log_group] = (int(start_ms), str(token), int(chain_started_ms))
        else:
            resumes.pop(log_group, None)
        self._dlq_state_dict("_dlq_read_started_ms")[log_group] = int(chain_started_ms)
        self._dlq_read_truncated[log_group] = truncated
        self._dlq_state_dict("_dlq_read_ok")[log_group] = True
        self._dlq_state_dict("_dlq_read_error")[log_group] = None
        errors: list[CdcConnectorError] = []
        max_ts = cursor or 0
        for event in events:
            message = str(event.get("message", ""))
            timestamp = event.get("timestamp")
            event_id = str(
                event.get("eventId") or f"{timestamp}:{message[:48]}"
            )
            if event_id in seen:
                continue
            occurred_at = (
                datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
                if isinstance(timestamp, (int, float))
                else None
            )
            record = parse_dlq_log_message(message, occurred_at=occurred_at)
            seen.add(event_id)
            if record is not None:
                errors.append(record)
            if isinstance(timestamp, (int, float)) and timestamp > max_ts:
                max_ts = int(timestamp)
        if not truncated:
            # A complete read: nothing older than the ingest grace can still arrive, so the
            # cursor may trail "now" by that much even when no event was read. max() keeps it
            # from ever moving backwards (max_ts starts at the previous cursor) and past the
            # newest event already read. "Now" is when this read BEGAN (a continuation's
            # first call): a line ingested while it was continuing is read next time.
            self._dlq_cursor_ms[log_group] = max(
                max_ts + 1 if max_ts else 0,
                chain_started_ms - _DLQ_INGEST_GRACE_SECONDS * 1000,
            )
        elif max_ts > (cursor or 0):
            # Past the newest event read -- the restart point should the continuation be
            # dropped. Unmoved when this capped read matched nothing new.
            self._dlq_cursor_ms[log_group] = max_ts + 1
        # Coverage of this group starts at the first successful read that reached the end or
        # established a read position (a seeded controller's start IS its seeded cursor).
        if not truncated or self._dlq_cursor_ms.get(log_group) is not None:
            self._dlq_state_dict("_dlq_history_start_ms").setdefault(
                log_group, int(start_ms)
            )
        # Bound the de-dup set so a long-running session never grows it unbounded.
        if len(seen) > 5000:
            self._dlq_seen_ids[log_group] = set(list(seen)[-2000:])
        return errors

    def value_conversion_failures(
        self,
        log_group: str,
        *,
        now: Optional[datetime] = None,
        limit: int = 200,
        window_seconds: int = _DLQ_INITIAL_LOOKBACK_SECONDS,
    ) -> "list[CdcValueConversionFailure]":
        """Read SOURCE-connector value-conversion failures from the connector log group.

        A source connector that cannot convert one column's value logs the column at
        ERROR and delivers that field as NULL: the row LANDS, nothing is dead-lettered,
        and no count-based signal can see it (see
        :mod:`dsql_migrator.core.cdc_value_conversion`). This filters the same worker log
        group :meth:`dlq_errors` reads for those lines and returns the distinct
        ``(table, column, type)`` triples it saw, identifiers only. ``[]`` on any access
        error (fail-closed), exactly like :meth:`dlq_errors`.

        Deliberately NOT folded into :meth:`dlq_errors`, for three separate reasons:

        * it must never become a :class:`CdcConnectorError`, because those are what the
          DLQ depth counts -- these rows were APPLIED, so counting them would inflate
          the quarantine number a cut-over decision turns on;
        * it keeps its OWN cursor (``_conv_cursor_ms``), so neither read can move the
          other's read position past lines the other has not returned (the two line
          classes come from DIFFERENT connectors, hence different log streams with
          independent ingestion lag);
        * and it must not share the dead-letter read's page budget: a conversion failure
          is logged PER ROW while a quarantine is rare, so a flood would have consumed
          those pages and silently truncated the quarantine count -- in exactly the
          situation this signal exists to detect.

        ONE ``logs:FilterLogEvents`` call per invocation: the ``nextToken`` is
        deliberately NOT followed, because the result is a SET rather than a count, so
        walking one page forward per poll converges without an unbounded scan -- and a
        value that fails once fails on every row that carries it. ``limit`` is generous
        so a same-millisecond burst cannot be stepped over by the forward cursor.
        """
        now_dt = now or datetime.now(timezone.utc)
        now_ms = int(now_dt.timestamp() * 1000)
        cursor = self._conv_cursor_ms.get(log_group)
        start_ms = cursor if cursor is not None else now_ms - window_seconds * 1000
        try:
            logs = self._client("logs")
            response = logs.filter_log_events(
                logGroupName=log_group,
                startTime=start_ms,
                filterPattern=VALUE_CONVERSION_FILTER_PATTERN,
                limit=limit,
            )
        except Exception:  # noqa: BLE001 - advisory monitoring read, never crash
            return []
        found: "list[CdcValueConversionFailure]" = []
        max_ts = cursor or 0
        for event in list(response.get("events", []) or []):
            record = parse_value_conversion_failure(str(event.get("message", "")))
            # No eventId de-dup (unlike the DLQ read): a repeated triple is harmless
            # because the caller merges into a set, so the read is idempotent by
            # construction and needs no cross-session seeding.
            if record is not None and record not in found:
                found.append(record)
            timestamp = event.get("timestamp")
            if isinstance(timestamp, (int, float)) and timestamp > max_ts:
                max_ts = int(timestamp)
        if max_ts:
            self._conv_cursor_ms[log_group] = max_ts + 1
        return found

    def connector_log_tail(
        self,
        stack_name: str,
        connector_name: str,
        *,
        limit: int = 400,
        since: Optional[datetime] = None,
    ) -> str:
        """Recent worker-log lines of ``connector_name`` (``""`` on any error). Bounded.

        The same capped CloudWatch Logs reads the Start CDC failure path makes
        (:func:`~dsql_migrator.core.cdc_stack_deployer.read_connector_log_tail`), for the
        Pipeline-health diagnosis of a connector that stopped AFTER it started. Read it
        only for a stopped connector, throttled by the caller: it is two API calls, and a
        healthy connector's log names no failure. MySQL Start CDC also reads a failed
        connector create with it, limited to events written ``since`` that Start.
        """
        from dsql_migrator.core.cdc_stack_deployer import read_connector_log_tail

        try:
            logs = self._client("logs")
        except Exception:  # noqa: BLE001 - advisory monitoring read, never crash
            return ""
        if since is None:
            return read_connector_log_tail(logs, stack_name, connector_name, limit=limit)
        return read_connector_log_tail(
            logs, stack_name, connector_name, limit=limit, since=since
        )

    # -- read-only ----------------------------------------------------------

    def list_connectors(self) -> list[dict]:
        """Return the raw ``connectors`` list from ``kafkaconnect:ListConnectors``.

        Empty list on any access error (fail-closed). Callers that DECIDE something from
        emptiness must use :meth:`list_connectors_checked` instead -- see why there.
        """
        _ok, connectors = self.list_connectors_checked()
        return connectors

    def list_connectors_checked(self) -> "tuple[bool, list[dict]]":
        """Return ``(read_succeeded, connectors)``.

        ``list_connectors`` folds every error into ``[]``, so "there are no connectors" and
        "this task cannot call ``kafkaconnect:ListConnectors``" are the same value. That is
        fine for a read that only DISPLAYS what it found, and wrong for anything that
        concludes something from emptiness: with a missing permission the tool would report a
        live, streaming pipeline as absent -- and an operator who believes nothing is
        streaming may edit the CDC inputs or apply foreign keys, which makes the sink
        dead-letter out-of-order child rows (23503). Distinguishing the two is what lets a
        caller leave its state UNKNOWN instead of asserting either way.
        """
        try:
            client = self._client("kafkaconnect")
            response = client.list_connectors()
            return True, list(response.get("connectors", []) or [])
        except Exception:  # noqa: BLE001 - cannot see them != they do not exist
            import logging

            logging.getLogger(__name__).warning(
                "kafkaconnect:ListConnectors failed", exc_info=True
            )
            return False, []

    def describe_connector(self, connector_arn: str) -> Optional[dict]:
        """Return the raw ``describe_connector`` response, or ``None`` on error."""
        try:
            client = self._client("kafkaconnect")
            return client.describe_connector(connectorArn=connector_arn)
        except Exception:  # noqa: BLE001 - treated as "not describable"
            return None

    def connector_statuses(
        self, connector_names: Sequence[str]
    ) -> list[ConnectorStatus]:
        """Return :class:`ConnectorStatus` for the named connectors.

        Lists all connectors once, filters by name, and maps each
        ``connectorState`` to a coarse :class:`ConnectorState`. Unknown states map
        to ``UNASSIGNED``. Returns ``[]`` when ``connector_names`` is empty (zero
        AWS calls) or on any access error.
        """
        wanted = {name for name in connector_names if name}
        if not wanted:
            return []
        statuses: list[ConnectorStatus] = []
        for connector in self.list_connectors():
            name = connector.get("connectorName")
            if name not in wanted:
                continue
            raw_state = str(connector.get("connectorState", "")).upper()
            state = _CONNECTOR_STATE_MAP.get(raw_state, ConnectorState.UNASSIGNED)
            statuses.append(ConnectorStatus(name=name, state=state))
        return statuses

    def connector_health(
        self, connector_names: Sequence[str], *, window_seconds: int = 300
    ) -> dict[str, "ConnectorHealth"]:
        """Return CloudWatch task-health metrics per connector.

        Reads ``RunningTaskCount``, ``ErroredTaskCount`` and ``SinkRecordSendRate``
        (dimension ``ConnectorName``) over the trailing ``window_seconds`` and
        takes the most recent datapoint of each. These are the health/throughput
        signals MSK Connect actually publishes -- replication *lag* is measured
        separately against the DSQL target. Returns an empty :class:`ConnectorHealth`
        for every connector on any error or missing datapoint, so the UI renders
        "unknown" rather than crashing.

        ``now`` is the system clock (a live monitoring read, not a deterministic
        build step); tests inject a fake CloudWatch client with canned datapoints.
        """
        names = [name for name in connector_names if name]
        result: dict[str, ConnectorHealth] = {name: ConnectorHealth() for name in names}
        if not names:
            return result
        metrics = [
            (_RUNNING_TASKS_METRIC, "running", "Maximum"),
            (_ERRORED_TASKS_METRIC, "errored", "Maximum"),
            (_SEND_RATE_METRIC, "send_rate", "Average"),
            (_SOURCE_POLL_RATE_METRIC, "poll_rate", "Average"),
        ]
        try:
            client = self._client("cloudwatch")
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = []
            id_map: dict[str, tuple[str, str]] = {}
            for ni, name in enumerate(names):
                for mi, (metric, _field, stat) in enumerate(metrics):
                    qid = f"m{ni}_{mi}"
                    id_map[qid] = (name, metrics[mi][1])
                    queries.append(
                        {
                            "Id": qid,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": _MSK_CONNECT_NAMESPACE,
                                    "MetricName": metric,
                                    "Dimensions": [
                                        {"Name": "ConnectorName", "Value": name}
                                    ],
                                },
                                "Period": 60,
                                "Stat": stat,
                            },
                            "ReturnData": True,
                        }
                    )
            response = client.get_metric_data(
                MetricDataQueries=queries, StartTime=start, EndTime=now
            )
            for item in response.get("MetricDataResults", []):
                name_field = id_map.get(item.get("Id"))
                values = item.get("Values", [])
                if name_field is None or not values:
                    continue
                name, field = name_field
                # get_metric_data returns newest-first by default.
                latest = float(values[0])
                health = result[name]
                if field == "running":
                    health.running_tasks = int(latest)
                elif field == "errored":
                    health.errored_tasks = int(latest)
                elif field == "send_rate":
                    health.send_rate = latest
                elif field == "poll_rate":
                    health.poll_rate = latest
        except Exception:  # noqa: BLE001 - treated as "health unknown"
            return {name: ConnectorHealth() for name in names}
        return result

    def applied_ops_by_table(
        self,
        stack: str,
        tables: Sequence[str],
        *,
        window_seconds: int = 14 * 24 * 3600,
    ) -> dict[str, dict[str, int]]:
        """Per-table applied-ops breakdown CDC has applied, from the sink's
        ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied`` metrics.

        Returns ``{table: {"inserts": N, "updates": N, "deletes": N}}`` (a DMS-style
        change breakdown), each summed over the trailing ``window_seconds`` — a
        lightweight, **source-scan-free** per-table signal that replaces COUNT(*)-ing
        the source in the live monitor. Unlike the old net-rows metric it makes UPDATE
        traffic visible (net = inserts - deletes hid updates entirely).

        The sink emits the ``Table`` dimension **schema-qualified** (``db.table``),
        while the tool's names can be bare or qualified, so we ``ListMetrics`` to
        discover the published dimension values (UNION across the three op metrics —
        an update-only table has no ``InsertsApplied`` dimension) then match each
        requested name to one, working in either mode.

        Best-effort: returns ``{}`` on any error / no datapoint; approximate under
        replay (a monitor, not the exact reconciliation, which is Validation). Uses
        the live clock; tests inject a fake CloudWatch client.
        """
        names = [t for t in tables if t]
        if not stack or not names:
            return {}
        result: dict[str, dict[str, int]] = {}
        try:
            client = self._client("cloudwatch")
            # 1) Discover the Table dims the sink published, UNION across the three op
            #    metrics (a table with only updates has no InsertsApplied dimension).
            discovered: set[str] = set()
            for metric in _APPLIED_OP_METRICS.values():
                discovered |= set(
                    self._list_metric_dimensions(client, stack, metric) or []
                )
            if not discovered:
                return {}
            # 2) Map each requested table name to a published dimension value.
            by_dim = _match_metric_tables(names, sorted(discovered))
            if not by_dim:
                return {}
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = []
            id_map: dict[str, tuple[str, str]] = {}  # qid -> (table name, op key)
            i = 0
            for name, dim in by_dim.items():
                for op_key, metric in _APPLIED_OP_METRICS.items():
                    qid = f"o{i}"
                    i += 1
                    id_map[qid] = (name, op_key)
                    queries.append(
                        {
                            "Id": qid,
                            "MetricStat": {
                                "Metric": {
                                    "Namespace": _CDC_METRIC_NAMESPACE,
                                    "MetricName": metric,
                                    "Dimensions": [
                                        {"Name": "Stack", "Value": stack},
                                        {"Name": "Table", "Value": dim},
                                    ],
                                },
                                # Daily Sum buckets over the window; summed below =
                                # total ops applied (avoids an over-large single Period).
                                "Period": 86400,
                                "Stat": "Sum",
                            },
                            "ReturnData": True,
                        }
                    )
            # get_metric_data caps at 500 MetricDataQueries/request and we build 3 per
            # matched table, so at >~166 tables a single call would exceed the cap and
            # raise -- which the outer except would swallow, blanking the WHOLE monitor
            # (every table, not just the overflow). Batch into <=500-query requests and
            # merge, so the per-op monitor scales to a large table set. Each query id is
            # unique across batches, so merging by (table, op) never double-counts.
            for offset in range(0, len(queries), _GMD_MAX_QUERIES):
                batch = queries[offset : offset + _GMD_MAX_QUERIES]
                response = client.get_metric_data(
                    MetricDataQueries=batch, StartTime=start, EndTime=now
                )
                for item in response.get("MetricDataResults", []):
                    key = id_map.get(item.get("Id"))
                    values = item.get("Values", [])
                    if key is None or not values:
                        continue
                    name, op_key = key
                    bucket = result.setdefault(
                        name, {"inserts": 0, "updates": 0, "deletes": 0}
                    )
                    bucket[op_key] = int(round(sum(float(v) for v in values)))
        except Exception:  # noqa: BLE001 - best-effort monitor signal only
            return {}
        return result

    def replication_lag_by_table(
        self,
        stack: str,
        tables: Sequence[str],
        *,
        window_seconds: int = 15 * 60,
    ) -> dict[str, int]:
        """Per-table end-to-end replication lag in **milliseconds**, from the sink's
        ``ReplicationLagMs`` metric (apply-wall-clock minus the event's source commit
        time). Time-based and PK-agnostic — a far more accurate replication-lag signal
        than the UI's ``MAX(pk)`` leading-edge check.

        Discovery + name matching are identical to :meth:`applied_ops_by_table` (the
        sink emits the same schema-qualified ``Table`` dimension). Read with ``Stat=Maximum``
        (worst lag) at 1-minute resolution over the trailing ``window_seconds`` and
        return the **most recent** minute's value per table (current worst lag). Idle
        tables (no recent events) have no datapoint and are simply absent — the stream
        is caught up.

        Because the metric is event-driven (a datapoint is emitted only when the sink
        applies an event), a drained pipeline stops emitting: the newest datapoint left
        in the window is then the LAST applied event's lag, which is stale, not the
        current state. So a "most recent" datapoint older than ``_LAG_FRESHNESS_SECONDS``
        is treated as absent — the table reads as caught up (lag drops) once the source
        is quiesced, instead of freezing at the last value until it ages out of the
        window. Best-effort: ``{}`` on any error / no (fresh) datapoint.
        """
        names = [t for t in tables if t]
        if not stack or not names:
            return {}
        result: dict[str, int] = {}
        try:
            client = self._client("cloudwatch")
            discovered = self._list_metric_dimensions(
                client, stack, _REPLICATION_LAG_METRIC
            )
            if not discovered:
                return {}
            by_dim = _match_metric_tables(names, discovered)
            if not by_dim:
                return {}
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = []
            id_map: dict[str, str] = {}
            for i, (name, dim) in enumerate(by_dim.items()):
                qid = f"l{i}"
                id_map[qid] = name
                queries.append(
                    {
                        "Id": qid,
                        "MetricStat": {
                            "Metric": {
                                "Namespace": _CDC_METRIC_NAMESPACE,
                                "MetricName": _REPLICATION_LAG_METRIC,
                                "Dimensions": [
                                    {"Name": "Stack", "Value": stack},
                                    {"Name": "Table", "Value": dim},
                                ],
                            },
                            # 1-minute worst-lag buckets; the newest bucket is the
                            # current lag. Maximum (not Average) so a lag spike shows.
                            "Period": 60,
                            "Stat": "Maximum",
                        },
                        "ReturnData": True,
                    }
                )
            fresh_cutoff = now.timestamp() - _LAG_FRESHNESS_SECONDS
            # get_metric_data caps at 500 MetricDataQueries/request (one per matched
            # table here), so at >500 tables a single call would exceed the cap and
            # raise -- which the outer except would swallow, blanking the WHOLE lag
            # surface (every table, not just the overflow). Batch into <=500-query
            # requests and merge by id, mirroring applied_ops_by_table, so the lag
            # monitor scales to a large table set. TimestampDescending -> Values[0]
            # is the most recent minute.
            for offset in range(0, len(queries), _GMD_MAX_QUERIES):
                batch = queries[offset : offset + _GMD_MAX_QUERIES]
                response = client.get_metric_data(
                    MetricDataQueries=batch,
                    StartTime=start,
                    EndTime=now,
                    ScanBy="TimestampDescending",
                )
                for item in response.get("MetricDataResults", []):
                    name = id_map.get(item.get("Id"))
                    values = item.get("Values", [])
                    if name is None or not values:
                        continue
                    # Drop a stale "current lag": if the newest datapoint predates the
                    # freshness cutoff, the pipeline has drained (no recent applies) ->
                    # the table is caught up, so omit it rather than report the frozen
                    # last-event lag. Timestamps[0] aligns with Values[0]
                    # (TimestampDescending). When timestamps are absent (defensive /
                    # older shape), keep the value.
                    timestamps = item.get("Timestamps") or []
                    if timestamps:
                        newest = timestamps[0]
                        newest_ts = (
                            newest.timestamp() if hasattr(newest, "timestamp")
                            else float(newest)
                        )
                        if newest_ts < fresh_cutoff:
                            continue
                    result[name] = int(round(float(values[0])))
        except Exception:  # noqa: BLE001 - best-effort monitor signal only
            return {}
        return result

    def replication_lag_series(
        self,
        stack: str,
        tables: Sequence[str],
        *,
        window_seconds: int = 15 * 60,
    ) -> list[tuple[int, int]]:
        """Pipeline-wide replication-lag TIME SERIES for the trend chart.

        Same ``ReplicationLagMs`` metric / dimensions / read as
        :meth:`replication_lag_by_table`, but keeps the WHOLE trailing
        ``window_seconds`` (not just the newest 1-minute bucket) and collapses the
        per-table series into ONE worst-case line -- the MAX per-table lag per bucket
        -- answering "is the pipeline catching up or falling behind?" for the cutover
        decision. Returns ``[(epoch_seconds, max_lag_ms), ...]`` ascending by time;
        buckets with no datapoint on any table are simply absent (the stream is caught
        up there). The datapoints are already fetched by the per-table read but
        discarded; this keeps them. Best-effort: ``[]`` on any error / no data.
        """
        names = [t for t in tables if t]
        if not stack or not names:
            return []
        try:
            client = self._client("cloudwatch")
            discovered = self._list_metric_dimensions(
                client, stack, _REPLICATION_LAG_METRIC
            )
            if not discovered:
                return []
            by_dim = _match_metric_tables(names, discovered)
            if not by_dim:
                return []
            now = datetime.now(timezone.utc)
            start = datetime.fromtimestamp(
                now.timestamp() - window_seconds, tz=timezone.utc
            )
            queries = [
                {
                    "Id": f"l{i}",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": _CDC_METRIC_NAMESPACE,
                            "MetricName": _REPLICATION_LAG_METRIC,
                            "Dimensions": [
                                {"Name": "Stack", "Value": stack},
                                {"Name": "Table", "Value": dim},
                            ],
                        },
                        "Period": 60,
                        "Stat": "Maximum",
                    },
                    "ReturnData": True,
                }
                for i, (_name, dim) in enumerate(by_dim.items())
            ]
            # Collapse every table's per-minute series into one worst-case line:
            # MAX lag across tables per timestamp bucket. get_metric_data caps at 500
            # MetricDataQueries/request (one per matched table here), so at >500 tables
            # a single call would raise -- which the outer except would swallow,
            # blanking the WHOLE trend chart. Batch into <=500-query requests and
            # accumulate by_bucket across batches (MAX is associative, so merging
            # batches is correct), mirroring applied_ops_by_table.
            by_bucket: dict[int, float] = {}
            for offset in range(0, len(queries), _GMD_MAX_QUERIES):
                batch = queries[offset : offset + _GMD_MAX_QUERIES]
                response = client.get_metric_data(
                    MetricDataQueries=batch,
                    StartTime=start,
                    EndTime=now,
                    ScanBy="TimestampAscending",
                )
                for item in response.get("MetricDataResults", []):
                    timestamps = item.get("Timestamps", []) or []
                    values = item.get("Values", []) or []
                    for ts, val in zip(timestamps, values):
                        epoch = (
                            int(ts.timestamp()) if hasattr(ts, "timestamp") else int(ts)
                        )
                        fval = float(val)
                        if epoch not in by_bucket or fval > by_bucket[epoch]:
                            by_bucket[epoch] = fval
            return [(epoch, int(round(by_bucket[epoch]))) for epoch in sorted(by_bucket)]
        except Exception:  # noqa: BLE001 - best-effort monitor signal only
            return []

    def _list_metric_dimensions(
        self, client: object, stack: str, metric_name: str
    ) -> list[str]:
        """Discover the ``Table`` dimension values the sink published for ``stack``
        under ``metric_name`` (e.g. ``InsertsApplied`` or ``ReplicationLagMs``).

        Filters ``ListMetrics`` to that metric with this ``Stack`` dimension and returns
        each metric's ``Table`` value. Paginates over ``NextToken`` with a bounded loop
        (500 metrics/page) so a large schema is covered without an unbounded call chain.

        Memoized per ``(stack, metric_name)`` for ``_METRIC_DIM_CACHE_TTL_SECONDS`` so
        the 5 discovery passes a single CDC poll makes (3 op metrics + 2 lag readers),
        and the re-discovery every poll, collapse to one pagination per metric per
        window -- discovery ("which tables emit this metric") changes only when a new
        table starts emitting, and the live get_metric_data DATA reads are never cached.
        Only successful reads are cached; an error propagates (uncached) to the caller's
        best-effort handler, so a transient failure is retried on the next poll.
        """
        key = (stack, metric_name)
        now = self._monotonic()
        cached = self._dim_cache.get(key)
        if cached is not None and now < cached[0]:
            return list(cached[1])
        tables: list[str] = []
        token: Optional[str] = None
        for _ in range(20):  # 500 metrics/page -> up to 10k tables; a hard cap
            kwargs: dict = {
                "Namespace": _CDC_METRIC_NAMESPACE,
                "MetricName": metric_name,
                "Dimensions": [{"Name": "Stack", "Value": stack}],
            }
            if token:
                kwargs["NextToken"] = token
            resp = client.list_metrics(**kwargs)  # type: ignore[attr-defined]
            for metric in resp.get("Metrics", []):
                for dim in metric.get("Dimensions", []):
                    if dim.get("Name") == "Table" and dim.get("Value"):
                        tables.append(dim["Value"])
            token = resp.get("NextToken")
            if not token:
                break
        # Cache only a fully-read result (the loop above completed without raising).
        self._dim_cache[key] = (now + _METRIC_DIM_CACHE_TTL_SECONDS, list(tables))
        return tables

    # -- guarded mutation ---------------------------------------------------

    def delete_connector(self, connector_arn: str, current_version: str) -> bool:
        """Delete (stop) a connector. Returns ``True`` on success, ``False`` on error.

        DESTRUCTIVE and irreversible: MSK Connect has no pause/resume, so stopping
        a connector means deleting it. This method has NO confirmation of its own;
        the UI caller MUST gate it behind an explicit type-the-name dialog.
        """
        try:
            client = self._client("kafkaconnect")
            client.delete_connector(
                connectorArn=connector_arn, currentVersion=current_version
            )
            return True
        except Exception:  # noqa: BLE001 - surfaced to the caller as failure
            return False


def build_msk_connect_controller(
    region: str, *, aws_profile: Optional[str] = None
) -> MskConnectController:
    """Build a profile-aware controller for the given MSK Connect region."""
    return MskConnectController(region, aws_profile=aws_profile)


__all__ = [
    "ConnectorHealth",
    "MskConnectController",
    "build_msk_connect_controller",
]
