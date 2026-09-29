# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Cut over puts back the DEFAULTs the column-exclusion overlay dropped (NiceGUI-free).

The overlay dropped each excluded column's DEFAULT (recorded first in the overlay
ledger) so the column loads NULL. At cut over the tool restores them -- automatically
only when it can show the recorded DEFAULT is still what the operator's Schema
Conversion DDL means, otherwise as a listed manual decision. ``SET DEFAULT`` affects new
rows only, so migrated rows stay NULL.

* :func:`classify_cutover_defaults` -- pure: status per column (pending / done /
  diverged / gone / unknown, from the LIVE catalog), auto (A)/(B), the manual reasons.
* :func:`read_cutover_defaults` -- the background cache fill (ledger x bulk facts).
* :func:`run_cutover_default_restore` -- the job body: the settle check, serial units,
  ONE ledger batch at the end.
* :func:`cutover_settle_probe` -- the read-only CDC settle check.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

from dsql_migrator.core.overlay_ledger import (
    LedgerDecision,
    LedgerRead,
    OverlayLedgerError,
    apply_restore_results,
    ddl_digest,
)
from dsql_migrator.core.schema_applier import (
    is_single_sql_expression,
    restore_default_statement,
)

# How long a filled cache is trusted before the next render refills it (there is no
# step-open hook, so this approximates "on step open").
CUTOVER_DEFAULTS_TTL_SECONDS = 60.0

NOT_EXCLUDED_REASON = (
    "DEFAULT {o} was dropped while this column was excluded; it is no longer excluded, "
    "so restoring it is your decision."
)
NOT_EXECUTABLE_REASON = (
    "DEFAULT {o} is not a single expression, so the tool does not run it; run the "
    "statement yourself."
)
MANUAL_REASONS = {
    1: (
        "The tool cannot rebuild your Schema Conversion DDL in this session, so it cannot "
        "check {o} against it."
    ),
    2: (
        "This table is not in your current Schema Conversion, so the tool cannot check {o} "
        "against a DDL."
    ),
    3: (
        "When {o} was recorded, the tool had no Schema Conversion DDL for this table, so it "
        "cannot check that DEFAULT against your current DDL."
    ),
    4: (
        "Your Schema Conversion DDL for this table changed after {o} was recorded, and the "
        "tool cannot read the new DDL, so it cannot tell whether this column's DEFAULT "
        "changed."
    ),
    5: (
        "Your Schema Conversion DDL for this table changed after {o} was recorded, and the "
        "tool could not read this column in the DDL of that time, so it cannot tell whether "
        "its DEFAULT changed."
    ),
    6: (
        "Your Schema Conversion DDL for this table no longer has column {c}, so the tool "
        "cannot check {o} against it."
    ),
    7: (
        "Your Schema Conversion DDL for this table changed after {o} was recorded. At that "
        "time it declared no DEFAULT for this column, so {o} did not come from it."
    ),
    8: (
        "Your Schema Conversion DDL no longer declares a DEFAULT for this column; {o} was "
        "recorded when it declared DEFAULT {x}."
    ),
    9: (
        "Your Schema Conversion DDL now declares DEFAULT {y} for this column; {o} was "
        "recorded when it declared DEFAULT {x}."
    ),
}


@dataclass(frozen=True)
class CutoverDefaultItem:
    """One recorded, dropped DEFAULT and what cut over does with it."""

    table: str
    column: str
    original_default: str
    # pending (no live DEFAULT) | done (live == original) | diverged | gone | unknown
    status: str
    live_default: Optional[str]
    auto: bool
    reason: Optional[str]
    executable: bool
    dropped_not_null: bool
    statement: str
    # The table itself is absent on the target (its record is deleted once confirmed).
    table_absent: bool = False


@dataclass(frozen=True)
class CutoverDefaultsSnapshot:
    """The cut-over DEFAULT status, read in the background (the render does no I/O)."""

    # loaded | ledger_unreadable | target_unreadable
    state: str
    items: "tuple[CutoverDefaultItem, ...]" = ()
    # "table.column" of every recorded column whose NOT NULL was dropped (one-way) and
    # that still exists: it stays nullable after cut over.
    nullable: "tuple[str, ...]" = ()
    checked_at: Optional[datetime] = None
    error: Optional[str] = None
    fill_key: Any = None
    generation: int = 0
    filled_monotonic: float = 0.0

    @property
    def auto_items(self) -> "tuple[CutoverDefaultItem, ...]":
        return tuple(i for i in self.items if i.auto)

    @property
    def manual_items(self) -> "tuple[CutoverDefaultItem, ...]":
        return tuple(i for i in self.items if i.status == "pending" and not i.auto)

    @property
    def pending(self) -> int:
        return sum(1 for i in self.items if i.status == "pending")

    @property
    def restored(self) -> int:
        return sum(1 for i in self.items if i.status == "done")

    @property
    def total(self) -> int:
        """The denominator of "restored: n of m": pending + done (diverged is listed as
        information, gone is owed nothing, unknown is counted as not restored)."""
        return self.pending + self.restored + self.unknown

    @property
    def unknown(self) -> int:
        return sum(1 for i in self.items if i.status == "unknown")

    @property
    def diverged_items(self) -> "tuple[CutoverDefaultItem, ...]":
        return tuple(i for i in self.items if i.status == "diverged")

    @property
    def gone_items(self) -> "tuple[CutoverDefaultItem, ...]":
        return tuple(i for i in self.items if i.status == "gone")

    @property
    def outstanding(self) -> int:
        """DEFAULTs still owed or not confirmed (pending + unknown)."""
        return self.pending + self.unknown


@dataclass(frozen=True)
class CutoverRestoreOutcome:
    """What the last restore job did (``refused`` when it did nothing)."""

    restored: "tuple[str, ...]" = ()
    already_done: "tuple[str, ...]" = ()
    diverged: "tuple[str, ...]" = ()
    gone: "tuple[str, ...]" = ()
    skipped: "tuple[str, ...]" = ()
    failed: "Mapping[str, str]" = field(default_factory=dict)
    # "table.column" -> the statement, for a hand run of a failed column.
    failed_statements: "Mapping[str, str]" = field(default_factory=dict)
    refused: Optional[str] = None
    # The refusal came from the CDC settle check: the drained attestation is cleared.
    refused_by_settle: bool = False
    ledger_warning: Optional[str] = None
    statements: "tuple[str, ...]" = ()


def _text(value: Optional[str]) -> str:
    return value if value else "(none)"


def _manual_reason(rec_col, table: str, column: str, current_ddls) -> str:
    """The first of reasons 1-9 that applies to a still-excluded pending column that
    failed both (A) and (B)."""
    from dsql_migrator.core.converter import parse_target_column_constraints

    o = rec_col.original_default
    if current_ddls is None:
        return MANUAL_REASONS[1].format(o=o)
    ddl = current_ddls.get(table)
    if ddl is None:
        return MANUAL_REASONS[2].format(o=o)
    if rec_col.recorded_ddl_digest is None:
        return MANUAL_REASONS[3].format(o=o)
    parsed = parse_target_column_constraints(ddl)
    if not parsed:
        return MANUAL_REASONS[4].format(o=o)
    if rec_col.recorded_ddl_state == "unreadable":
        return MANUAL_REASONS[5].format(o=o)
    if column not in parsed:
        return MANUAL_REASONS[6].format(o=o, c=column)
    if rec_col.recorded_ddl_state == "none":
        return MANUAL_REASONS[7].format(o=o)
    x = _text(rec_col.recorded_ddl_default)
    _not_null, now_default = parsed[column]
    if now_default is None:
        return MANUAL_REASONS[8].format(o=o, x=x)
    return MANUAL_REASONS[9].format(o=o, x=x, y=now_default)


def _auto_ok(rec_col, table: str, column: str, current_ddls) -> bool:
    """(A) the DDL text is unchanged since this column was recorded, or (B) it still
    declares the same DEFAULT for this column (DDL text against DDL text)."""
    if current_ddls is None or table not in current_ddls:
        return False
    ddl = current_ddls[table]
    if rec_col.recorded_ddl_digest is not None and rec_col.recorded_ddl_digest == ddl_digest(ddl):
        return True
    if rec_col.recorded_ddl_state == "declared":
        from dsql_migrator.core.converter import parse_target_column_constraints

        parsed = parse_target_column_constraints(ddl)
        if column in parsed:
            _not_null, now_default = parsed[column]
            return now_default is not None and now_default == rec_col.recorded_ddl_default
    return False


def classify_cutover_defaults(
    ledger_tables: "Mapping[str, Any]",
    facts: "Mapping[str, Any]",
    *,
    current_ddls: "Optional[Mapping[str, str]]",
    still_excluded: "Mapping[str, Sequence[str]]",
) -> "tuple[tuple[CutoverDefaultItem, ...], tuple[str, ...]]":
    """``(items, nullable)`` for the recorded columns. Pure (no I/O, no NiceGUI).

    Status comes from the LIVE facts: a table whose facts are unknown is ``unknown`` (never
    auto); an absent table or column is ``gone``; no live DEFAULT is ``pending``; the
    recorded original live is ``done``; any other DEFAULT is ``diverged`` (never
    overwritten). A pending column is restored automatically only while it is still
    excluded, its original is a single expression, and (A) or (B) holds; every other
    pending column is a manual decision with a one-line reason.
    """
    items: list[CutoverDefaultItem] = []
    nullable: list[str] = []
    for table in sorted(ledger_tables):
        record = ledger_tables[table]
        table_facts = facts.get(table)
        excluded = set(still_excluded.get(table, ()) or ())
        for entry in sorted(record.columns, key=lambda c: c.column):
            column = entry.column
            if table_facts is None:
                live, status, absent = None, "unknown", False
            elif not table_facts.exists:
                live, status, absent = None, "gone", True
            else:
                live, absent = table_facts.columns.get(column), False
                status = "gone" if live is None else None
            if entry.dropped_not_null and status not in ("gone",):
                nullable.append(f"{table}.{column}")
            if entry.original_default is None:
                continue
            o = entry.original_default
            live_default = getattr(live, "default_expr", None) if live is not None else None
            if status is None:
                if live_default is None:
                    status = "pending"
                elif live_default == o:
                    status = "done"
                else:
                    status = "diverged"
            executable = is_single_sql_expression(o)
            auto = False
            reason: Optional[str] = None
            if status == "pending":
                if column not in excluded:
                    reason = NOT_EXCLUDED_REASON.format(o=o)
                elif _auto_ok(entry, table, column, current_ddls):
                    auto = executable
                else:
                    reason = _manual_reason(entry, table, column, current_ddls)
                if not executable:
                    suffix = NOT_EXECUTABLE_REASON.format(o=o)
                    reason = f"{reason} {suffix}" if reason else suffix
            items.append(
                CutoverDefaultItem(
                    table=table,
                    column=column,
                    original_default=o,
                    status=status,
                    live_default=live_default,
                    auto=auto,
                    reason=reason,
                    executable=executable,
                    dropped_not_null=entry.dropped_not_null,
                    statement=restore_default_statement(table, column, o).as_string(None) + ";",
                    table_absent=absent,
                )
            )
    return tuple(items), tuple(nullable)


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip().splitlines()
    return text[0] if text else type(exc).__name__


def read_cutover_defaults(
    *,
    ledger,
    ledger_key: str,
    read_facts: "Callable[[Sequence[str], Mapping[str, Sequence[str]]], Mapping[str, Any]]",
    current_ddls: "Callable[[], Optional[Mapping[str, str]]]",
    still_excluded: "Callable[[], Mapping[str, Sequence[str]]]",
    on_record_found: "Optional[Callable[[], None]]" = None,
    fill_key: Any = None,
    generation: int = 0,
    now: "Optional[Callable[[], datetime]]" = None,
    monotonic: "Optional[Callable[[], float]]" = None,
) -> CutoverDefaultsSnapshot:
    """The cache fill: ONE ledger read, ONE bulk facts read, then the classification.

    Runs on a worker thread; never raises (a failure is an ``*_unreadable`` state).
    """
    import time as _time

    stamp = (now or (lambda: datetime.now(timezone.utc)))()
    mono = (monotonic or _time.monotonic)()

    def _snap(state, items=(), nullable=(), error=None):
        return CutoverDefaultsSnapshot(
            state=state, items=tuple(items), nullable=tuple(nullable), checked_at=stamp,
            error=error, fill_key=fill_key, generation=generation, filled_monotonic=mono,
        )

    try:
        try:
            read = ledger.read(ledger_key)
        except Exception as exc:  # noqa: BLE001 - the contract says never; unknown if it does
            read = LedgerRead("unreadable", {}, _first_line(exc))
        if read.status == "unreadable":
            return _snap("ledger_unreadable", error=read.error)
        tables = {t: r for t, r in (read.tables or {}).items() if r is not None}
        if not tables:
            return _snap("loaded")
        if on_record_found is not None:
            try:
                on_record_found()
            except Exception:  # noqa: BLE001 - a flag, never fails the fill
                pass
        columns = {t: [c.column for c in r.columns] for t, r in tables.items() if r.columns}
        if not columns:
            return _snap("loaded")
        try:
            facts = read_facts(sorted(columns), columns) or {}
        except Exception as exc:  # noqa: BLE001 - every table unknown
            return _snap("target_unreadable", error=_first_line(exc))
        if all(facts.get(t) is None for t in columns):
            return _snap("target_unreadable", error="the Aurora DSQL catalog could not be read")
        try:
            ddls = current_ddls()
        except Exception:  # noqa: BLE001 - no DDL: every pending column is manual (reason 1)
            ddls = None
        try:
            excluded = still_excluded() or {}
        except Exception:  # noqa: BLE001 - nothing counts as still excluded: nothing auto
            excluded = {}
        items, nullable = classify_cutover_defaults(
            {t: tables[t] for t in columns}, facts, current_ddls=ddls, still_excluded=excluded
        )
        return _snap("loaded", items, nullable)
    except Exception as exc:  # noqa: BLE001 - the fill never raises
        return _snap("target_unreadable", error=_first_line(exc))


def cutover_settle_probe(
    *, region: str, stack_name: str, aws_profile: Optional[str] = None,
    assume_role_arn: Optional[str] = None, deployer_factory: Optional[Callable[..., Any]] = None,
) -> Optional[str]:
    """Why the CDC connectors are not settled (a refusal reason), or ``None``. READ-ONLY.

    Refuses while the cdc-stack runs an operation (``*_IN_PROGRESS``) or either connector
    is CREATING / UPDATING; an absent stack, and connectors RUNNING, FAILED, DELETING or
    absent, pass. A read error RAISES (the caller refuses on it). A stack stuck in a
    failed state passes: its only remedy (Delete) ends CDC, and waiting never clears it.
    """
    from dsql_migrator.core.cdc import cdc_expected_connector_names

    if deployer_factory is None:
        from dsql_migrator.core.cdc_stack_deployer import build_cdc_stack_deployer

        deployer_factory = build_cdc_stack_deployer
    deployer = deployer_factory(region, aws_profile=aws_profile, assume_role_arn=assume_role_arn)
    stack = deployer.describe_stack_or_none(stack_name)
    if stack is None:
        return None
    status = str(getattr(stack, "stack_status", "") or "")
    if status.upper().endswith("_IN_PROGRESS"):
        return f"the cdc-stack is {status}"
    for name in cdc_expected_connector_names(stack_name):
        state = deployer.connector_state(name)
        if state and str(state).upper() in ("CREATING", "UPDATING"):
            return f"{name} is {state}"
    return None


SETTLE_REFUSAL = (
    "not restored: {why}. Wait until both CDC connectors are RUNNING and caught up, then "
    "tick again."
)
LEDGER_UNREADABLE_REFUSAL = "cannot read the record of dropped DEFAULTs; nothing was restored"
LEDGER_WRITE_WARNING = (
    "DEFAULTs restored on Aurora DSQL, but the record of dropped DEFAULTs could not be "
    "updated"
)


def run_cutover_default_restore(
    handle,
    *,
    items: "Sequence[CutoverDefaultItem]",
    ledger,
    ledger_key: str,
    restore: Callable[..., Any],
    connection_factory: Callable[[], Any],
    settle_probe: "Optional[Callable[[], Optional[str]]]" = None,
    now: "Optional[Callable[[], datetime]]" = None,
) -> CutoverRestoreOutcome:
    """The restore job body: settle check, then one unit per column STRICTLY serially,
    then ONE ledger batch (a locked read-modify-write against a fresh read)."""
    if settle_probe is not None:
        try:
            why = settle_probe()
        except Exception as exc:  # noqa: BLE001 - an unreadable CDC state refuses
            why = f"the CDC state could not be read ({_first_line(exc)})"
        if why:
            return CutoverRestoreOutcome(
                refused=SETTLE_REFUSAL.format(why=why), refused_by_settle=True
            )
    try:
        read = ledger.read(ledger_key)
    except Exception as exc:  # noqa: BLE001 - unknown is unreadable
        read = LedgerRead("unreadable", {}, _first_line(exc))
    if read.status == "unreadable":
        return CutoverRestoreOutcome(refused=LEDGER_UNREADABLE_REFUSAL)
    restored, done, diverged, gone, skipped, statements = [], [], [], [], [], []
    failed: dict[str, str] = {}
    failed_statements: dict[str, str] = {}
    results: dict[str, dict[str, tuple[str, Optional[str]]]] = {}
    absent_tables: set[str] = set()
    for item in sorted(items, key=lambda i: (i.table, i.column)):
        if getattr(handle, "cancelled", False):
            break
        name = f"{item.table}.{item.column}"
        record = (read.tables or {}).get(item.table)
        entry = record.column(item.column) if record is not None else None
        if entry is None or entry.original_default != item.original_default:
            skipped.append(name)  # changed since this screen was drawn
            continue
        try:
            outcome = restore(
                item.table, item.column, item.original_default,
                connection_factory=connection_factory,
            )
            status = getattr(outcome, "status", "failed")
            results.setdefault(item.table, {})[item.column] = (status, None)
            if status == "restored":
                restored.append(name)
                statements.append(item.statement)
            elif status == "done":
                done.append(name)
            elif status == "diverged":
                diverged.append(name)
            elif status == "gone":
                gone.append(name)
                if item.table_absent:
                    absent_tables.add(item.table)
        except Exception as exc:  # noqa: BLE001 - kept per column
            code = getattr(exc, "sqlstate", None)
            reason = f"{code + ': ' if code else ''}{_first_line(exc)}"
            failed[name] = reason
            failed_statements[name] = item.statement
            results.setdefault(item.table, {})[item.column] = ("failed", reason)
        finally:
            beat = getattr(handle, "heartbeat", None)
            if callable(beat):
                beat()
    ledger_warning = None
    stamp = (now or (lambda: datetime.now(timezone.utc)))()
    merges = {}
    for table, outcomes in results.items():
        def _merge(existing, table=table, outcomes=outcomes):
            if existing is None:
                return LedgerDecision("unchanged", None)
            after = apply_restore_results(existing, outcomes, stamp)
            if table in absent_tables and not after.columns:
                return LedgerDecision("delete", None)
            if after == existing:
                return LedgerDecision("unchanged", existing)
            return LedgerDecision("write", after)

        merges[table] = _merge
    if merges:
        try:
            ledger.merge_tables(ledger_key, merges)
        except OverlayLedgerError:
            ledger_warning = LEDGER_WRITE_WARNING
        except Exception:  # noqa: BLE001 - same honest warning; live status stays true
            ledger_warning = LEDGER_WRITE_WARNING
    return CutoverRestoreOutcome(
        restored=tuple(restored), already_done=tuple(done), diverged=tuple(diverged),
        gone=tuple(gone), skipped=tuple(skipped), failed=failed,
        failed_statements=failed_statements, ledger_warning=ledger_warning,
        statements=tuple(statements),
    )


__all__ = [
    "CUTOVER_DEFAULTS_TTL_SECONDS",
    "CutoverDefaultItem",
    "CutoverDefaultsSnapshot",
    "CutoverRestoreOutcome",
    "MANUAL_REASONS",
    "NOT_EXCLUDED_REASON",
    "classify_cutover_defaults",
    "cutover_settle_probe",
    "read_cutover_defaults",
    "run_cutover_default_restore",
]
