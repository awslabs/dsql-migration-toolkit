# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The CDC dead-letter gate on cut over (NiceGUI-agnostic, except the one scheduler).

A CDC dead letter is a change the stream set aside and NEVER retries: a later full-row
update of the same row heals it, but a row that is never touched again stays missing or
stale, and a lost DELETE leaves a permanent extra row. Validation's default ROW_COUNT mode
cannot see a missed UPDATE at all, and cut over used to read only the validation verdict --
so a CDC migration could reach "ready" with stale rows on Aurora DSQL.

This module decides, from state alone, whether cut over is held for that reason:

* **Per table.** Every table in the persisted dead-letter ledger
  (``DataMigrationState.cdc_dlq_ledger``) is RESOLVED only when its current report item is
  value-verified (no error, matched, ``checksum_match is True``, deep checks not skipped) AND
  its comparison STARTED more than :data:`CDC_DLQ_EVIDENCE_MARGIN_SECONDS` after the latest
  dead letter for it. The compare-start time is the app clock, stamped before the compare
  (conservative); the dead-letter time is the CloudWatch event time, hence the margin.
* **The queue itself** (CDC types only). The tool must hold one successful, complete read
  of the dead-letter log that STARTED after the latest validation finished, and the read's
  history must reach back to the CDC start (the Full Load watermark, or the CDC start for
  CDC only) -- or one full CHECKSUM run started after that history began must cover every
  table. ``dlq_errors`` returns ``[]`` on any error, so without this a missing permission
  read as "no dead letters". It fails closed, with "Check again" and a recorded waiver.

Way forward, never a dead end: a table that can be re-checked must be (a CHECKSUM can
always be started); a table Validation cannot prove (not in the report, or inside an
accepted gap) and a read problem can be waived -- logged, persisted, and scoped to the
counts seen, so a newer dead letter re-opens the check once the queue can be read. One
residual: a CHECKSUM that cannot FINISH (a very large table with a composite or missing
primary key is hashed in one statement, which can exceed Aurora DSQL's transaction limit)
leaves the table "not matched"; its exit is to leave it out on the Validation step, which
makes it waivable here (the Validation result says so). A waiver never overrides a
Validation mismatch: this gate is evaluated only after ``cutover_release_state`` released.

Inert -- no section, no timer, no log clause -- when no CDC evidence exists in the session,
so a Full-Load-only migration (and every pre-existing session without CDC) is unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

# A dead letter's time is the MSK Connect worker / CloudWatch clock, a compare start is this
# app's clock: a table resolves only when its compare started this long AFTER its last dead
# letter, so a skew of up to a minute can never flip a stale table to "resolved".
CDC_DLQ_EVIDENCE_MARGIN_SECONDS = 60
# At most one background dead-letter read per this many seconds per session, and only while
# the Validation or Cut over step renders. The single-flight claim's watchdog is twice this.
CDC_DLQ_CHECK_TTL_SECONDS = 60

REASON_NOT_VALIDATED = "not-validated"
REASON_ACCEPTED_GAP = "accepted-gap"
REASON_NOT_MATCHED = "not-matched"
REASON_ROW_COUNT_ONLY = "row-count-only"
REASON_COMPARE_TIME_UNKNOWN = "compare-time-unknown"
REASON_COMPARED_BEFORE = "compared-before"
# Never waivable: a CHECKSUM can always be started, so each of these has a way forward that
# proves it. (A CHECKSUM that cannot finish -- see the module docstring -- is left out on the
# Validation step, which turns it into "not-validated" below.)
MUST_RECHECK_REASONS = frozenset(
    {
        REASON_NOT_MATCHED,
        REASON_ROW_COUNT_ONLY,
        REASON_COMPARE_TIME_UNKNOWN,
        REASON_COMPARED_BEFORE,
    }
)
# Validation cannot prove these at all (a name the report does not use, a table left out,
# rows that can never match), so a recorded acknowledgement is the way forward.
WAIVABLE_REASONS = frozenset({REASON_NOT_VALIDATED, REASON_ACCEPTED_GAP})

READ_PENDING = "pending"
READ_FAILED = "failed"
READ_INCOMPLETE = "incomplete"
READ_PARTIAL_HISTORY = "partial-history"
READ_OK = "ok"
READ_PROBLEMS = frozenset({READ_FAILED, READ_INCOMPLETE, READ_PARTIAL_HISTORY})

_ISSUE_LINE_LIMIT = 20
_NAME_LIMIT = 6


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _from_ms(ms: Optional[int]) -> Optional[datetime]:
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


def value_verified(item: object) -> bool:
    """A report item that proves the table's VALUES, not just its row count. Pure."""
    return (
        getattr(item, "error", None) is None
        and bool(getattr(item, "matched", False))
        and getattr(item, "checksum_match", None) is True
        and not bool(getattr(item, "deep_checks_skipped", False))
    )


@dataclass(frozen=True)
class CdcDlqTableIssue:
    """One dead-lettered table the gate has not cleared, and why (first reason that applies)."""

    table: str
    count: int
    last_at: Optional[datetime]
    kinds: tuple[str, ...]
    reason: str
    compared_from: Optional[datetime] = None


@dataclass(frozen=True)
class CdcDlqGate:
    """The gate's verdict. ``CdcDlqGate()`` is the INERT gate (no CDC evidence)."""

    applies: bool = False
    read_required: bool = False
    read_state: Optional[str] = None
    last_read: Optional[object] = None
    log_group: Optional[str] = None
    history_from: Optional[datetime] = None
    cdc_start: Optional[datetime] = None
    issues: tuple[CdcDlqTableIssue, ...] = ()
    resolved: tuple[str, ...] = ()
    # Dead-letter count per resolved table, for the copy that names them.
    resolved_counts: Mapping[str, int] = field(default_factory=dict)
    unverifiable: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    ledger_total: int = 0
    ledger_tables: int = 0
    waived: bool = False
    migration_type: Optional[object] = None
    # From state alone (no read needed), for Validation: CDC is in use and the dead-letter
    # history cannot be shown to reach back to the CDC start (an attached pipeline, or a
    # count that starts after it), and no full CHECKSUM run after it has cleared that. Cut
    # over then holds as "partial-history" once its read lands, so Validation must not say
    # "Ready". Never changes ``blocks``/``held`` (Cut over decides those from its own read).
    history_unconfirmed: bool = False

    @property
    def must_recheck(self) -> tuple[CdcDlqTableIssue, ...]:
        return tuple(i for i in self.issues if i.reason in MUST_RECHECK_REASONS)

    @property
    def waivable_issues(self) -> tuple[CdcDlqTableIssue, ...]:
        return tuple(i for i in self.issues if i.reason in WAIVABLE_REASONS)

    @property
    def read_problem(self) -> Optional[str]:
        return self.read_state if self.read_state in READ_PROBLEMS else None

    @property
    def pending(self) -> bool:
        return self.read_state == READ_PENDING

    @property
    def blocks(self) -> bool:
        return self.applies and (
            bool(self.must_recheck)
            or (
                (bool(self.waivable_issues) or self.read_problem is not None)
                and not self.waived
            )
        )

    @property
    def held(self) -> bool:
        return self.blocks or self.pending

    @property
    def can_waive(self) -> bool:
        return (
            self.applies
            and not self.must_recheck
            and not self.pending
            and (bool(self.waivable_issues) or self.read_problem is not None)
            and not self.waived
        )

    @property
    def recheckable(self) -> tuple[str, ...]:
        return tuple(i.table for i in self.must_recheck)


def table_proof_reason(
    item: Optional[object],
    *,
    release: str,
    explained: bool,
    started: Optional[datetime],
    last_at: Optional[datetime],
) -> Optional[str]:
    """Why a table's current report item does NOT prove it after ``last_at``, or ``None``. Pure.

    The per-table ladder of this gate, shared with the REPLICA IDENTITY gate
    (``ui/cdc_replica_identity_gate.py``, where ``last_at`` is the identity's restore time).
    First reason that applies: not in the report; inside an accepted gap (``explained`` and
    ``release == "accepted"``); not a match; matched by row count only; compare time
    unknown; compared too early -- the comparison must have STARTED more than
    :data:`CDC_DLQ_EVIDENCE_MARGIN_SECONDS` after ``last_at``.
    """
    started = _aware(started)
    last_at = _aware(last_at)
    if item is None:
        return REASON_NOT_VALIDATED
    if release == "accepted" and explained:
        return REASON_ACCEPTED_GAP
    if getattr(item, "error", None) is not None or not getattr(item, "matched", False):
        return REASON_NOT_MATCHED
    if not value_verified(item):
        return REASON_ROW_COUNT_ONLY
    if started is None:
        return REASON_COMPARE_TIME_UNKNOWN
    margin = timedelta(seconds=CDC_DLQ_EVIDENCE_MARGIN_SECONDS)
    if last_at is not None and last_at >= started - margin:
        return REASON_COMPARED_BEFORE
    return None


def cdc_dlq_waiver_covers(
    waiver: object,
    *,
    waivable_counts: Mapping[str, int],
    read_problem: Optional[str],
) -> bool:
    """Whether a recorded waiver still covers the CURRENT state. Pure.

    Every table Validation cannot prove must be in the waiver at no more than the count seen
    then, and the current read problem (if any) must be the one waived. A newer dead letter,
    a new such table, or a different read problem re-opens the check.
    """
    if waiver is None:
        return False
    tables = dict(getattr(waiver, "tables", None) or {})
    for table, count in waivable_counts.items():
        if table not in tables or int(count) > int(tables[table]):
            return False
    if read_problem is not None and read_problem != getattr(waiver, "read_problem", None):
        return False
    return True


def cdc_dlq_cutover_gate(
    *,
    ledger: "Mapping[str, Any]",
    cdc_in_use: bool,
    cdc_evidence: bool,
    check_reads: bool,
    report: Optional[object],
    release: str,
    explained_tables: "Sequence[str]" = (),
    compared_run_start: Optional[datetime] = None,
    compared_overrides: "Optional[Mapping[str, datetime]]" = None,
    evidence_at: Optional[datetime] = None,
    log_group: Optional[str] = None,
    last_read: Optional[object] = None,
    history_from_ms: Optional[int] = None,
    cdc_start: Optional[datetime] = None,
    waiver: Optional[object] = None,
    migration_type: Optional[object] = None,
) -> CdcDlqGate:
    """Evaluate the gate from explicit inputs. Pure (see the module docstring for the rule)."""
    ledger = dict(ledger or {})
    applies = bool(ledger) or (cdc_in_use and cdc_evidence)
    if not applies:
        return CdcDlqGate(migration_type=migration_type)
    read_required = bool(cdc_in_use and check_reads)
    margin = timedelta(seconds=CDC_DLQ_EVIDENCE_MARGIN_SECONDS)
    overrides = {k: _aware(v) for k, v in (compared_overrides or {}).items() if v is not None}
    run_start = _aware(compared_run_start)

    def compared_from(table: str) -> Optional[datetime]:
        return overrides.get(table) or run_start

    items = {item.table: item for item in (getattr(report, "items", None) or ())}
    explained = set(explained_tables or ())
    issues: list[CdcDlqTableIssue] = []
    resolved: list[str] = []
    resolved_counts: dict[str, int] = {}
    unverifiable: dict[str, tuple[str, ...]] = {}
    total = 0
    for table in sorted(ledger):
        entry = ledger[table]
        count = int(getattr(entry, "count", 0) or 0)
        total += count
        last_at = _aware(getattr(entry, "last_at", None))
        kinds = tuple(getattr(entry, "kinds", None) or ())
        item = items.get(table)
        started = compared_from(table)
        reason = table_proof_reason(
            item,
            release=release,
            explained=table in explained,
            started=started,
            last_at=last_at,
        )
        if reason is None:
            resolved.append(table)
            resolved_counts[table] = count
            excluded = tuple(getattr(item, "checksum_excluded_columns", None) or ())
            if excluded:
                unverifiable[table] = excluded
            continue
        issues.append(
            CdcDlqTableIssue(
                table=table, count=count, last_at=last_at, kinds=kinds, reason=reason,
                compared_from=started,
            )
        )

    history_from = _from_ms(history_from_ms)
    start = _aware(cdc_start)

    def _history_cleared() -> bool:
        # One full CHECKSUM run, every comparison started after the history began, proves
        # the data whatever the uncounted records were.
        return (
            history_from is not None
            and bool(items)
            and all(
                value_verified(item)
                and compared_from(name) is not None
                and compared_from(name) > history_from + margin  # type: ignore[operator]
                for name, item in items.items()
            )
        )

    # The state-only view of "partial-history" (see CdcDlqGate.history_unconfirmed). A known
    # start with no history yet is the normal pending-read path: that read proves it.
    if not cdc_in_use:
        history_unconfirmed = False
    elif start is None:
        history_unconfirmed = not _history_cleared()
    elif history_from is None:
        history_unconfirmed = False
    else:
        history_unconfirmed = history_from > start and not _history_cleared()
    read_state: Optional[str] = None
    evidence = _aware(evidence_at)
    if read_required:
        started_at = _aware(getattr(last_read, "started_at", None))
        if (
            last_read is None
            or getattr(last_read, "log_group", None) != log_group
            or (evidence is not None and (started_at is None or started_at <= evidence))
        ):
            read_state = READ_PENDING
        elif not getattr(last_read, "ok", False):
            read_state = READ_FAILED
        elif not getattr(last_read, "complete", False):
            read_state = READ_INCOMPLETE
        else:
            proven = (
                history_from is not None and start is not None and history_from <= start
            )
            read_state = (
                READ_OK if proven or _history_cleared() else READ_PARTIAL_HISTORY
            )

    read_problem = read_state if read_state in READ_PROBLEMS else None
    waivable_counts = {i.table: i.count for i in issues if i.reason in WAIVABLE_REASONS}
    waived = bool(waivable_counts or read_problem) and cdc_dlq_waiver_covers(
        waiver, waivable_counts=waivable_counts, read_problem=read_problem
    )
    return CdcDlqGate(
        applies=True,
        read_required=read_required,
        read_state=read_state,
        last_read=last_read,
        log_group=log_group,
        history_from=history_from,
        cdc_start=start,
        issues=tuple(issues),
        resolved=tuple(resolved),
        resolved_counts=resolved_counts,
        unverifiable=unverifiable,
        ledger_total=total,
        ledger_tables=len(ledger),
        waived=waived,
        migration_type=migration_type,
        history_unconfirmed=history_unconfirmed,
    )


def _latest(*stamps: Optional[datetime]) -> Optional[datetime]:
    present = [_aware(s) for s in stamps if s is not None]
    return max(present) if present else None


def validation_evidence_at(validation_state: object) -> Optional[datetime]:
    """When the latest validation evidence was produced: the run's or a re-check's finish."""
    if validation_state is None:
        return None
    return _latest(
        getattr(validation_state, "completed_at", None),
        getattr(validation_state, "rechecked_at", None),
    )


def cdc_dlq_gate_for(
    migration_state: object,
    validation_state: object,
    *,
    cdc_in_use: bool,
    release: str,
    summary: Optional[object],
    check_reads: bool = True,
) -> CdcDlqGate:
    """The gate for this session's state. No I/O; ``migration_state`` None -> inert."""
    if migration_state is None:
        return CdcDlqGate()
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_log_group

    ledger = dict(getattr(migration_state, "cdc_dlq_ledger", None) or {})
    cdc_start = getattr(migration_state, "cdc_ops_window_start", None)
    evidence = cdc_start is not None or bool(
        getattr(migration_state, "cdc_connector_names", None) or []
    )
    stack = getattr(migration_state, "cdc_stack_name", None) or CDC_DEFAULT_STACK_NAME
    group = cdc_dlq_log_group(stack)
    try:
        migration_type = getattr(migration_state, "migration_type", None)
    except Exception:  # noqa: BLE001 - copy only
        migration_type = None
    compared = getattr(validation_state, "compared_run_start", None)
    overrides = getattr(validation_state, "compared_overrides", None) or {}
    return cdc_dlq_cutover_gate(
        ledger=ledger,
        cdc_in_use=cdc_in_use,
        cdc_evidence=evidence,
        check_reads=check_reads,
        report=getattr(validation_state, "result", None),
        release=release,
        explained_tables=tuple(getattr(summary, "quarantine_explained_tables", ()) or ()),
        compared_run_start=compared,
        compared_overrides=overrides,
        evidence_at=validation_evidence_at(validation_state),
        log_group=group,
        last_read=getattr(migration_state, "cdc_dlq_last_read", None),
        history_from_ms=(getattr(migration_state, "cdc_dlq_read_from_ms", None) or {}).get(
            group
        ),
        cdc_start=cdc_start,
        waiver=getattr(validation_state, "cutover_dlq_waiver", None),
        migration_type=migration_type,
    )


# ---------------------------------------------------------------------------
# The background read (never on render; bounded; single flight)
# ---------------------------------------------------------------------------


def cdc_dlq_check_due(
    last_read: Optional[object],
    evidence_at: Optional[datetime],
    now: datetime,
) -> bool:
    """Whether a fresh dead-letter read is needed: none yet, none since the latest
    validation evidence, or the last one is older than the TTL. Pure.

    A failed read is NOT retried before the TTL (no tight loop); "Check again" forces one.
    """
    if last_read is None:
        return True
    started = _aware(getattr(last_read, "started_at", None))
    if started is None:
        return True
    evidence = _aware(evidence_at)
    if evidence is not None and started <= evidence:
        return True
    return (_aware(now) - started).total_seconds() > CDC_DLQ_CHECK_TTL_SECONDS


def schedule_cdc_dlq_check(
    ui: object,
    session: object,
    migration_state: object,
    refresh: Callable[[], None],
    *,
    evidence_at: Optional[datetime],
    force: bool = False,
) -> None:
    """Arm ONE background dead-letter read when it is due (or ``force``d by Check again).

    The render never reads CloudWatch: this only arms a one-shot timer; the claim is taken
    inside the callback (a timer dropped before it fires never leaks it), the read runs on
    a worker thread, the result lands on the loop, and the screen refreshes once.

    Once the claim is taken the read and its landing are DETACHED from the timer
    (:func:`~dsql_migrator.ui.data_migration._cdc_status.land_detached`): any refresh
    deletes the timer and cancels its callback, and a read cancelled there had already
    consumed its records from the shared controller -- they never reached the ledger and
    no later read could see them. The claim is held until the result has landed, so a
    re-render meanwhile arms nothing (single flight; the TTL counts from the landed read).
    """
    if migration_state is None:
        return
    armed_with = getattr(migration_state, "cdc_dlq_last_read", None)
    if not force and not cdc_dlq_check_due(
        armed_with, evidence_at, datetime.now(timezone.utc)
    ):
        return
    if getattr(migration_state, "cdc_dlq_check_running", False):
        return

    async def _check() -> None:
        from nicegui import run

        from dsql_migrator.ui.data_migration._cdc_status import (
            apply_cdc_dlq_read,
            land_detached,
            read_cdc_dlq_once,
        )

        current = getattr(migration_state, "cdc_dlq_last_read", None)
        if force:
            if current is not armed_with:
                return  # a newer read already landed since the click
        elif not cdc_dlq_check_due(current, evidence_at, datetime.now(timezone.utc)):
            return  # an earlier timer's read already landed
        if not migration_state.begin_cdc_dlq_check():  # type: ignore[attr-defined]
            return

        async def _read_and_land() -> None:
            # Owns the claim until the result has LANDED (never released by a cancelled
            # timer while the read is still running).
            try:
                fetched = await run.io_bound(read_cdc_dlq_once, migration_state, session)
                # None only when the app is stopping: nothing was read to land.
                apply_cdc_dlq_read(migration_state, fetched)
            finally:
                migration_state.finish_cdc_dlq_check()  # type: ignore[attr-defined]
            try:
                refresh()
            except Exception:  # noqa: BLE001 - the page went away
                pass

        await land_detached(_read_and_land)

    ui.timer(0.05, _check, once=True)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Copy (value-free: table names, counts, error classes, times -- never row values)
# ---------------------------------------------------------------------------


def _fmt_minute(value: Optional[datetime]) -> str:
    return value.strftime("%Y-%m-%d %H:%M UTC") if value is not None else "an unknown time"


def _named(issues: "Sequence[CdcDlqTableIssue]", limit: int = _NAME_LIMIT) -> str:
    """``t (k), u (j) (+N more)`` for copy and log details, bounded."""
    shown = ", ".join(f"{i.table} ({i.count})" for i in issues[:limit])
    return shown + (f" (+{len(issues) - limit} more)" if len(issues) > limit else "")


def _is_type(migration_type: object, name: str) -> bool:
    return getattr(migration_type, "name", None) == name


def _history_gap_sentence(gate: CdcDlqGate, group: str) -> str:
    """Why the dead-letter history can't be confirmed (shared by Cut over and Validation)."""
    if gate.cdc_start is not None:
        return (
            f"The tool's dead-letter count for {group} starts at "
            f"{_fmt_minute(gate.history_from)}, but CDC has been running since "
            f"{_fmt_minute(gate.cdc_start)}, so records set aside before then are not "
            "counted (an earlier version of the tool kept them only in memory, or no "
            "read reached back that far)."
        )
    return (
        "The tool does not know when CDC started (the pipeline was attached while "
        "already running), so it cannot confirm that its reads of "
        f"{group} cover every record CDC set aside."
    )


def read_state_notice(gate: CdcDlqGate) -> Optional[tuple[str, str, str]]:
    """``(tone, header, body)`` for the queue-read state, or ``None`` when it is fine.

    The waiver this copy offers is conditional on what the gate will actually allow: while a
    table is left to re-check, "Continue without this check" is not drawn, so the copy says
    it comes after the re-check -- never an offer of a button that is not there.
    """
    group = gate.log_group or "the CDC log group"
    recheck_first = bool(gate.must_recheck)
    if gate.read_state == READ_PENDING:
        return (
            "info",
            "Checking the CDC dead-letter queue",
            "Cut over waits for one fresh read of the CDC dead-letter log in CloudWatch "
            "Logs, so any change CDC set aside since your last validation is counted. This "
            "takes a few seconds; "
            + (
                "this check updates when it finishes."
                if gate.issues
                else "the runbook appears when it finishes."
            ),
        )
    if gate.read_state == READ_FAILED:
        code = getattr(gate.last_read, "error_code", None) or "unknown error"
        if code == "ResourceNotFoundException":
            body = (
                f"The log group {group} does not exist — it is deleted together with the "
                "CDC infrastructure, so the records CDC set aside can no longer be read. "
                "Run Validation in CHECKSUM mode for every table, then continue without "
                "this check; that decision is recorded in the activity log."
            )
        elif code == "NoTargetConnection":
            body = (
                "This session has no Aurora DSQL target connection, so the tool does not "
                f"know which AWS Region to read {group} from. Test the target connection "
                "on the Connect step, then choose Check again."
            )
        else:
            body = (
                f"The last read of {group} in CloudWatch Logs failed ({code}). A failed read "
                "looks the same as an empty one, so the tool cannot tell whether CDC set "
                "changes aside since your last validation. Check that this app's AWS "
                f"identity is allowed logs:FilterLogEvents on that log group — the deployed "
                "task role covers CDC stacks named dsql-cdc-* or mysql-dsql-cdc-*; a local "
                "AWS profile needs the same permission — then choose Check again. "
                + (
                    "Once the tables below are re-checked, you can also continue without "
                    "this check if you have confirmed the stream another way; that "
                    "decision is recorded in the activity log."
                    if recheck_first
                    else "If you have confirmed the stream another way, you can continue "
                    "without this check; that decision is recorded in the activity log."
                )
            )
        return ("error", "Can't read the CDC dead-letter queue", body)
    if gate.read_state == READ_INCOMPLETE:
        return (
            "error",
            "The CDC dead-letter queue was only partly read",
            f"The last read of {group} stopped at its page limit, so more dead-lettered "
            "records may exist than the tool has counted. Choose Check again to keep "
            "reading: each read continues where the last one stopped, and this check "
            "clears once a read reaches the end. If a read never gets there, you can "
            + (
                "continue without this check once the tables below are re-checked; "
                if recheck_first
                else "continue without this check; "
            )
            + "that decision is recorded in the activity log.",
        )
    if gate.read_state == READ_PARTIAL_HISTORY:
        # Warning, not error: the way forward is one routine CHECKSUM run that clears this
        # by itself -- the same tier as the per-table re-check. An attached pipeline and a
        # session saved by an earlier version both land here as a matter of course.
        return (
            "warning",
            "Part of the CDC dead-letter history can't be confirmed",
            _history_gap_sentence(gate, group)
            + " Run Validation in CHECKSUM mode for every table: a CHECKSUM comparison "
            f"started after {_fmt_minute(gate.history_from)} proves the data whatever "
            "those records were, and this check then clears by itself. "
            + (
                "Or, once the tables below are re-checked, continue without this check; "
                if recheck_first
                else "Or continue without this check; "
            )
            + "that decision is recorded in the activity log.",
        )
    return None


def issue_notice(gate: CdcDlqGate) -> Optional[tuple[str, str, str]]:
    """``(tone, header, body)`` for the per-table issues, or ``None`` when there are none."""
    must = gate.must_recheck
    if must:
        n = len(must)
        return (
            "warning",
            f"Re-check {n} table(s) with CHECKSUM before cutting over",
            "CDC set aside changes for these tables that never reached Aurora DSQL, and it "
            "never retries them. A later update of the same row heals it, but a row that is "
            "never touched again stays missing or stale, and a lost DELETE leaves an extra "
            "row. A row count cannot see a missed UPDATE, so each table must match in a "
            "CHECKSUM comparison that started after its last dead-lettered record. On the "
            # Not the button's label: Validation draws ONE re-check for this gate's tables
            # and the REPLICA IDENTITY gate's, so its count can differ from n.
            "Validation step, re-check them with CHECKSUM (one button there re-checks every "
            "table that needs it); this check clears by itself once they match.",
        )
    waivable = gate.waivable_issues
    if waivable:
        reasons = {i.reason for i in waivable}
        # What can actually clear each kind: a table outside the report needs a RUN that
        # includes it (a per-table re-check can only revisit report rows); a table inside
        # the accepted gap can never match until its source rows are fixed and reloaded.
        remedies: list[str] = []
        if REASON_NOT_VALIDATED in reasons:
            remedies.append(
                "Include a table that is not in the report on the Validation step and "
                "re-run Validation (tables with dead-lettered changes are compared by "
                "CHECKSUM automatically)."
            )
        if REASON_ACCEPTED_GAP in reasons:
            remedies.append(
                "A table inside the gap you accepted cannot be cleared by a CHECKSUM "
                "unless its source rows are corrected and it is reloaded with "
                "'Drop & reload'."
            )
        return (
            "warning",
            f"{len(waivable)} table(s) with dead-lettered CDC changes have no CHECKSUM proof",
            "CDC set aside changes for these tables, but Validation cannot prove them: they "
            "are not in the validation report (left out on the Validation step, or the "
            "dead-letter log names a table the report does not use), or they are part of "
            "the gap you accepted. "
            + " ".join(remedies)
            + " Or continue without this check; that decision is recorded in the activity "
            "log.",
        )
    return None


_OVERSIZED_LABEL = "a value over Aurora DSQL's per-value limit"
# The drift kinds that mean the SOURCE schema changed (the other kinds are a missing target
# table, a key conflict, an unbindable value or a TRUNCATE -- not a schema change).
_SCHEMA_CHANGE_KINDS = frozenset({"add-column", "drop-column", "type-change"})
# core.cdc.SchemaDriftKind.SOURCE_TRUNCATE (kept a literal like the kinds above).
_SOURCE_TRUNCATE_KIND = "source-truncate"
# core.cdc.SchemaDriftKind.VALUE_DOES_NOT_FIT: the sink refused a value its Aurora DSQL column
# would change. Not a schema-change claim (a MySQL TINYINT(1) holds 2 with no DDL), so it is not
# in _SCHEMA_CHANGE_KINDS; it gets its own "What to do" line, because the remedy is a column
# type that holds the values, and Aurora DSQL can only give it one by recreating the table.
_VALUE_FIT_KIND = "value-does-not-fit"


def _kind_label(kind: str) -> str:
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_KIND

    if kind == OVERSIZED_VALUE_KIND:
        return _OVERSIZED_LABEL
    try:
        from dsql_migrator.ui.data_migration._cdc_monitoring import _DRIFT_LABELS
    except Exception:  # noqa: BLE001 - copy only
        return kind
    return _DRIFT_LABELS.get(kind, (kind, ""))[0]


def _reason_text(issue: CdcDlqTableIssue) -> str:
    if issue.reason == REASON_ROW_COUNT_ONLY:
        return "compared by row count only, which cannot see a lost UPDATE"
    if issue.reason == REASON_COMPARED_BEFORE:
        when = (
            issue.compared_from.strftime("%H:%M:%S UTC")
            if issue.compared_from is not None
            else "an unknown time"
        )
        return (
            f"its last comparison started at {when}, before (or within a minute of) "
            "this record"
        )
    if issue.reason == REASON_COMPARE_TIME_UNKNOWN:
        return "when it was last compared is unknown (restored from an earlier session)"
    if issue.reason == REASON_NOT_MATCHED:
        return "its validation result is not a match"
    if issue.reason == REASON_NOT_VALIDATED:
        return "not in the validation report"
    if issue.reason == REASON_ACCEPTED_GAP:
        return "part of the gap you accepted, so a CHECKSUM cannot match it"
    return issue.reason


def issue_lines(gate: CdcDlqGate) -> list[str]:
    """One bounded line per open table (at most 20), then an "and N more" tail."""
    lines: list[str] = []
    for issue in gate.issues[:_ISSUE_LINE_LIMIT]:
        when = (
            issue.last_at.strftime("%Y-%m-%d %H:%M:%S UTC")
            if issue.last_at is not None
            else "an unknown time"
        )
        cause = (
            " (" + ", ".join(_kind_label(k) for k in issue.kinds) + ")" if issue.kinds else ""
        )
        lines.append(
            f"{issue.table}: {issue.count} dead-lettered record(s), the latest at "
            f"{when}{cause} — {_reason_text(issue)}"
        )
    more = len(gate.issues) - _ISSUE_LINE_LIMIT
    if more > 0:
        lines.append(f"and {more} more table(s)")
    return lines


def way_forward_lines(gate: CdcDlqGate) -> list[str]:
    """The numbered "What to do" steps for the open tables, by migration type."""
    if not gate.issues:
        return []
    # The last step is what actually clears each kind of open table (one line per kind
    # present, never per table): a re-check revisits report rows only, so a table outside
    # the report needs a run that includes it, and an accepted-gap table cannot match.
    reasons = {i.reason for i in gate.issues}
    finals: list[str] = []
    if gate.must_recheck:
        finals.append(
            "Re-check these tables on the Validation step. Tables with dead-lettered "
            "changes are compared by CHECKSUM automatically, and this check clears by "
            "itself once each matches."
        )
    if REASON_NOT_VALIDATED in reasons:
        finals.append(
            "Include the tables that are not in the validation report on the Validation "
            "step and re-run Validation; they are compared by CHECKSUM automatically, and "
            "this check clears by itself once each matches."
        )
    if REASON_ACCEPTED_GAP in reasons:
        finals.append(
            "A table inside the gap you accepted cannot match until its source rows are "
            "corrected and it is reloaded; until then, continue without this check (that "
            "decision is recorded in the activity log)."
        )
    if _is_type(gate.migration_type, "FULL_LOAD_ONLY"):
        lines = [
            "These dead letters were recorded while CDC ran earlier in this session.",
            "Reload these tables on the Full Load step with 'Drop & reload'.",
            *finals,
        ]
    else:
        fix = (
            "Fix the cause on the Data Migration step: the dead-letter queue card shows "
            "each record's reason (after an app restart it lists only records read since "
            "then)."
        )
        truncate_repair: list[str] = []
        if _is_type(gate.migration_type, "CDC_ONLY"):
            repair = (
                "CDC only has no Full Load step to reload from: correct these rows on "
                "Aurora DSQL by hand (the dead-letter queue card lists each record's "
                "primary key and Kafka offset), and delete any row a lost DELETE left "
                "behind."
            )
            # A source TRUNCATE dead letter names no row (no key, no image), so the line
            # above gives no way to find what to remove. Stop CDC first: it freezes the
            # target, so the delete cannot race a newly replicated insert. Names only the
            # tables that carry the truncate kind -- this line tells the user to DELETE target
            # rows, so it must never name a table that was not truncated -- and the wording
            # holds for one table or several (and for repeated TRUNCATEs of one table).
            truncated = [i for i in gate.issues if _SOURCE_TRUNCATE_KIND in i.kinds]
            if truncated:
                truncate_repair.append(
                    "Source TRUNCATE was not replicated for "
                    f"{bounded_table_names([i.table for i in truncated])}: a TRUNCATE dead "
                    "letter names no row. Stop CDC, delete the rows the source no longer has "
                    "from Aurora DSQL (at most 3,000 rows per transaction), then Start CDC."
                )
        else:
            # The same order as the Fix-target-schema dialog's "in this order" runbook (and
            # the manual's 4.2): Evaluation first, while CDC keeps streaming, so the window
            # with CDC stopped stays short.
            repair = (
                "If rows are missing or stale: re-run Step 1 (Evaluation) if the source "
                "schema changed, Stop CDC, reload these tables on the Full Load step with "
                "'Drop & reload', then Start CDC and let it drain. An append reload cannot "
                "rewrite a stale row or remove an extra one."
            )
        lines = [fix, repair, *truncate_repair, *finals]
    from dsql_migrator.core.cdc_dlq import OVERSIZED_VALUE_KIND

    kinds = {k for i in gate.issues for k in i.kinds}
    if OVERSIZED_VALUE_KIND in kinds:
        lines.append(
            "A value over Aurora DSQL's per-value limit can never be stored: exclude that "
            "column (Exclude tables & columns on the Data Migration step) or shrink the "
            "source value before you reload."
        )
    drifted = [i for i in gate.issues if _SCHEMA_CHANGE_KINDS & set(i.kinds)]
    if drifted:
        names = ", ".join(i.table for i in drifted[:_NAME_LIMIT]) + (
            f" (+{len(drifted) - _NAME_LIMIT} more)" if len(drifted) > _NAME_LIMIT else ""
        )
        lines.append(
            f"Records for {names} show a source schema change: re-run Step 1 (Evaluation) "
            "and align the Aurora DSQL table before you reload."
        )
    unfit = [i for i in gate.issues if _VALUE_FIT_KIND in i.kinds]
    if unfit:
        names = ", ".join(i.table for i in unfit[:_NAME_LIMIT]) + (
            f" (+{len(unfit) - _NAME_LIMIT} more)" if len(unfit) > _NAME_LIMIT else ""
        )
        if _is_type(gate.migration_type, "CDC_ONLY"):
            lines.append(
                f"Records for {names} hold a value their Aurora DSQL column cannot store "
                "exactly: with CDC stopped, give that column a type that holds the values on "
                "Aurora DSQL by hand (Aurora DSQL cannot change a column's type in place), or "
                "use Start over."
            )
        else:
            lines.append(
                f"Records for {names} hold a value their Aurora DSQL column cannot store "
                "exactly: Aurora DSQL cannot change a column's type in place, so re-apply these "
                "tables in Schema Conversion with Replace and a type that holds the values "
                "(re-run Step 1 first if the source type changed) before you reload."
            )
    return lines


def waiver_reason_phrases(gate: CdcDlqGate) -> list[str]:
    """What a waiver gives up, in words (for the notice and the activity log)."""
    phrases: list[str] = []
    if gate.read_problem == READ_FAILED:
        phrases.append("a readable dead-letter queue")
    elif gate.read_problem == READ_INCOMPLETE:
        phrases.append("a complete read of the dead-letter queue")
    elif gate.read_problem == READ_PARTIAL_HISTORY:
        phrases.append(f"the dead-letter history before {_fmt_minute(gate.history_from)}")
    if gate.waivable_issues:
        names = ", ".join(i.table for i in gate.waivable_issues[:_NAME_LIMIT]) + (
            f" (+{len(gate.waivable_issues) - _NAME_LIMIT} more)"
            if len(gate.waivable_issues) > _NAME_LIMIT
            else ""
        )
        phrases.append(f"CHECKSUM proof for {names}")
    return phrases


def waived_notice(gate: CdcDlqGate) -> tuple[str, str]:
    """``(header, body)`` of the warning shown on a released, waived gate.

    What it promises depends on what was waived: a newer dead letter re-locks the step only
    if a read can SEE it, and while the queue cannot be read at all, none can.
    """
    if gate.read_problem == READ_FAILED:
        tail = (
            ". That decision is in the activity log. While the queue can't be read, the "
            "tool can't see a change CDC sets aside from now on either, so this step will "
            "not lock again for one. Check the stream another way right before you repoint "
            "your application."
        )
    else:
        tail = (
            ". That decision is in the activity log, and this step locks again if a later "
            "read finds another change CDC set aside."
        )
    return (
        "Cutting over without the full CDC dead-letter check",
        "You chose to continue without " + " and ".join(waiver_reason_phrases(gate)) + tail,
    )


def unverifiable_notice(gate: CdcDlqGate) -> Optional[tuple[str, str]]:
    """``(header, body)`` naming the columns CHECKSUM could not compare, or ``None``."""
    if not gate.unverifiable:
        return None
    entries = list(gate.unverifiable.items())
    shown = "; ".join(f"{t} ({', '.join(cols)})" for t, cols in entries[:_NAME_LIMIT])
    if len(entries) > _NAME_LIMIT:
        shown += f"; +{len(entries) - _NAME_LIMIT} more table(s)"
    return (
        "CHECKSUM could not compare every column of the dead-lettered tables",
        "These columns have no byte-identical form on both engines, so a dead-lettered "
        f"change confined to them would not show: {shown}. Check those values another way "
        "if they matter.",
    )


def waiver_log_detail(gate: CdcDlqGate) -> str:
    """The bounded, value-free activity-log detail of a waiver."""
    return (
        "operator chose to cut over without "
        + " and ".join(waiver_reason_phrases(gate))
        + f"; {gate.ledger_total} dead-lettered record(s) counted on {gate.ledger_tables} "
        "table(s) so far; "
        + (
            "the queue could not be read, so dead letters that arrive while it stays "
            "unreadable will not re-open the check"
            if gate.read_problem == READ_FAILED
            else "the check re-opens if another arrives"
        )
    )[:480]


def ack_log_clause(gate: CdcDlqGate) -> str:
    """The clause appended to "cut over acknowledged" ("" when the gate is inert)."""
    if not gate.applies:
        return ""
    if gate.waived:
        return "; CDC dead-letter check WAIVED (" + " and ".join(
            waiver_reason_phrases(gate)
        ) + ")"
    if gate.held:
        return "; CDC dead-letter check NOT cleared"
    if not gate.ledger_tables:
        return "; CDC dead letters: none recorded"
    return (
        f"; CDC dead letters: {gate.ledger_total} record(s) on {gate.ledger_tables} "
        "table(s), each cleared by a CHECKSUM match after its last one"
    )


def verdict_hold(gate: Optional[CdcDlqGate], *, total: int, as_of: str) -> Optional[tuple[str, str]]:
    """``(header, body)`` replacing a green "Ready for cut-over" while the gate holds tables
    (or while the dead-letter history can't be confirmed, which Cut over holds on too)."""
    if gate is None or not gate.applies:
        return None
    must = gate.must_recheck
    unvalidated = tuple(i for i in gate.issues if i.reason == REASON_NOT_VALIDATED)
    history = (
        " Cut over also waits for one CHECKSUM run of every table: "
        + _history_hold_reason(gate)
        if gate.history_unconfirmed
        else ""
    )
    if must:
        body = (
            f"All {total} table(s) match as-of {as_of}. But CDC dead-lettered changes for "
            f"{_named(must)} — changes the stream never applied to Aurora DSQL — "
            + _hold_cause({i.reason for i in must})
            + ". Cut over opens once each of these tables matches in a CHECKSUM comparison "
            "that started after its last dead-lettered record. Re-check them below; if a "
            "table then differs, this report shows how to repair it."
        )
        if unvalidated:
            body += (
                f" CDC also dead-lettered changes for {_named(unvalidated)}, which this "
                "report does not cover — include them and re-run, or acknowledge them on "
                "the Cut over step."
            )
        return (
            f"Tables match — cut over waits for a CHECKSUM re-check of {len(must)} table(s)",
            body + history,
        )
    if unvalidated:
        return (
            f"Tables match — {len(unvalidated)} table(s) with dead-lettered CDC changes are "
            "not in this report",
            f"All {total} table(s) match as-of {as_of}, but CDC dead-lettered changes for "
            f"{_named(unvalidated)}, which this report does not cover (left out of this "
            "validation, or a name that matches no migrated table). Include them and re-run, "
            "or acknowledge them on the Cut over step." + history,
        )
    if gate.history_unconfirmed:
        group = gate.log_group or "the CDC log group"
        return (
            "Tables match — cut over waits for one CHECKSUM run of every table",
            f"All {total} table(s) match as-of {as_of}. "
            + _history_gap_sentence(gate, group)
            + " A CHECKSUM comparison of every table proves the data whatever those records "
            "were: re-run Validation in CHECKSUM mode with Fast sweep off, and cut over then "
            "opens by itself. Or continue without this check on the Cut over step; that "
            "decision is recorded in the activity log.",
        )
    return None


def _hold_cause(reasons: "set[str]") -> str:
    """Why these tables are held, by what actually holds them (never blame a row count for
    a table that matched by CHECKSUM and simply got a dead letter later)."""
    if reasons == {REASON_ROW_COUNT_ONLY}:
        return "and a row-count match cannot reveal a missed UPDATE"
    if reasons == {REASON_COMPARED_BEFORE}:
        return (
            "and each of these tables' last CHECKSUM comparison started before (or within a "
            "minute of) its latest dead-lettered record, so it cannot prove that record was "
            "applied"
        )
    if reasons == {REASON_COMPARE_TIME_UNKNOWN}:
        return (
            "and when these tables were last compared was not kept across the restore, so "
            "no comparison can be shown to come after their dead-lettered records"
        )
    return (
        "and none of these tables has a CHECKSUM match that started after its last "
        "dead-lettered record"
    )


def history_options_hint(gate: Optional[CdcDlqGate]) -> str:
    """The Validation Options hint while the dead-letter history can't be confirmed ("" when
    it can): disclosed BEFORE a run, so the operator picks CHECKSUM once instead of running a
    row-count pass that Cut over then bounces."""
    if gate is None or not gate.applies or not gate.history_unconfirmed:
        return ""
    return (
        "Cut over also needs one CHECKSUM run of every table (with Fast sweep off), "
        "because " + _history_hold_reason(gate)
    )


def _history_hold_reason(gate: CdcDlqGate) -> str:
    if gate.cdc_start is None:
        return (
            "the pipeline was attached while already running, so the dead-letter history "
            "can't be confirmed."
        )
    return (
        f"the dead-letter history before {_fmt_minute(gate.history_from)} can't be "
        "confirmed."
    )


def verdict_resolved_suffix(gate: Optional[CdcDlqGate]) -> str:
    """The sentence the green verdict gains once every dead-lettered table is resolved."""
    if gate is None or not gate.applies or gate.issues or not gate.resolved:
        return ""
    names = ", ".join(
        f"{t} ({int(gate.resolved_counts.get(t, 0))})" for t in gate.resolved[:_NAME_LIMIT]
    ) + (f" (+{len(gate.resolved) - _NAME_LIMIT} more)" if len(gate.resolved) > _NAME_LIMIT else "")
    return (
        f" CDC dead-lettered changes for {names}; each has since matched in a CHECKSUM "
        "comparison started after its last dead-lettered record."
    )


def held_log_clause(gate: Optional[CdcDlqGate]) -> Optional[str]:
    """The "validation completed" clause replacing "ready for cut-over" while tables are held."""
    if gate is None or not gate.applies:
        return None
    held = tuple(
        i for i in gate.issues if i.reason in MUST_RECHECK_REASONS or i.reason == REASON_NOT_VALIDATED
    )
    history = (
        "the CDC dead-letter history can't be confirmed — one CHECKSUM run of every table "
        "clears it"
        if gate.history_unconfirmed
        else ""
    )
    if not held:
        return f"; cut over held: {history}" if history else None
    return (
        f"; cut over held: {len(held)} table(s) with CDC dead letters lack a CHECKSUM match "
        f"after their last one: {_named(held)}"
        + (f"; and {history}" if history else "")
    )


def bounded_table_names(names: "Sequence[str]", limit: int = _NAME_LIMIT) -> str:
    """Join table names for copy or a log detail, capped."""
    names = list(names)
    shown = ", ".join(names[:limit])
    return shown + (f" (+{len(names) - limit} more)" if len(names) > limit else "")


__all__ = [
    "CDC_DLQ_CHECK_TTL_SECONDS",
    "CDC_DLQ_EVIDENCE_MARGIN_SECONDS",
    "CdcDlqGate",
    "CdcDlqTableIssue",
    "cdc_dlq_check_due",
    "cdc_dlq_cutover_gate",
    "cdc_dlq_gate_for",
    "cdc_dlq_waiver_covers",
    "schedule_cdc_dlq_check",
    "table_proof_reason",
    "value_verified",
]
