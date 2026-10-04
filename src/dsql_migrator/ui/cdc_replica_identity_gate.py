# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The runtime REPLICA IDENTITY re-check for re-keyed PostgreSQL tables, and its cut-over gate.

A table CDC RE-KEYS (Schema Conversion's composite key) has an Aurora DSQL primary key that
adds a column outside the source primary key. A DELETE carries that column only under
``REPLICA IDENTITY FULL`` -- on the table AND on every partition. Below FULL, Debezium fills the
absent NOT NULL key column with a placeholder (0, '' or its DEFAULT). Live-confirmed on
PostgreSQL 17 (2026-10-03): a DELETE keyed on the composite key reached the sink as
``{lead: 7, id: 2}`` -- 7 the column DEFAULT -- matched 0 rows on Aurora DSQL, and nothing was
dead-lettered. Since G15, CDC infrastructure with sink plugin v51 or later keys such a table on
its SOURCE primary key (SinkKeyColumns) and the sink deletes by that key once Aurora DSQL's
unique index on it is valid, so the delete lands. The hold below stays for what that does not
cover: the window before that index is valid (it is rebuilt after the load on a Drop & reload),
an UPDATE of the leading column (dead-lettered 23505, the old row kept), ``USING INDEX`` (a
placeholder in the source key itself) and infrastructure deployed by an earlier version. A lost
DELETE is never replayed.

The Start prerequisite (``REPLICA_IDENTITY_COVERS_KEY``) checks this once. This module keeps
checking while CDC runs, because ``ALTER TABLE ... REPLICA IDENTITY DEFAULT``, a new
partition (it starts at DEFAULT) or ``USING INDEX`` can drop it at any time:

* **The read.** One bounded catalog statement for all re-keyed tables
  (``cdc_pg_slot.read_rekeyed_replica_identity``) through the read-only source engine, at
  most every :data:`CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS` per session, single flight,
  never on the event loop. It needs the source password, which is kept in memory only.
* **The grade.** The prerequisite's own rule (``grade_rekeyed_replica_identity``), so a
  state that passes Start never raises a finding and the reverse.
* **The finding** (``CdcReplicaIdentityFinding``, persisted, cleared only by Start over):
  opened when a table is read below FULL; restored when it is read FULL again (or CDC stops
  re-keying it). One activity line per transition, never per poll.
* **The gate.** While a finding is open Cut over is held outright -- no CHECKSUM releases it,
  because deletes can keep being lost. Once restored, the table must match in a value-verified
  CHECKSUM that started more than a minute after the restore (G1's per-table rule,
  ``cdc_dlq_gate.table_proof_reason``). A table Validation cannot prove (left out, or in an
  accepted gap) can be waived, recorded. Cut over also waits for one fresh read after the
  latest validation. Fail-OPEN by design when the source cannot be read (no password after a
  restart, a failed read, a table not found) and no finding is open: Cut over then warns and
  the acknowledgement log says it was not re-checked. An open finding still holds, and its
  notices say why the re-read cannot clear it (no password, or the failed read).

Inert for MySQL, for tables that are not re-keyed (or only reorder the primary key) and for
Full load only without a finding: nothing is read, drawn or logged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, Optional, Sequence

# At most one source read per this many seconds per session (single flight across tabs); the
# claim's watchdog is twice this, so a hung read never pins "re-reading".
CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS = 120

PROBE_NO_SOURCE = "no-source-connection"
PROBE_READ_FAILED = "read-failed"

PROBE_STATE_UNAVAILABLE = "unavailable"
PROBE_STATE_PENDING = "pending"
PROBE_STATE_FAILED = "failed"
PROBE_STATE_OK = "ok"

REASON_WEAK = "weak"

_NAME_LIMIT = 6
_PARTITION_LIMIT = 5


def _aware(value: Optional[datetime]) -> Optional[datetime]:
    if value is not None and value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class CdcReplicaIdentityProbe:
    """One runtime read of the re-keyed tables' REPLICA IDENTITY (value-free).

    ``scope`` is what was asked ({table: composite key columns}); ``reads`` what the source
    returned ({table: cdc_pg_slot.RekeyedIdentityRead}) -- a scoped table absent from it was
    not found. A failed read has ``ok`` False, a ``problem`` and only the exception's CLASS
    name (never its message, which can carry a host or a value).
    """

    started_at: datetime
    finished_at: datetime
    ok: bool
    problem: Optional[str] = None
    error_class: Optional[str] = None
    scope: Mapping[str, tuple] = field(default_factory=dict)
    reads: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ReplicaIdentityTransition:
    """A finding changed state: ``opened`` / ``reopened`` / ``restored`` / ``not-rekeyed``."""

    kind: str
    table: str
    finding: object


# ---------------------------------------------------------------------------
# Scope, possibility, due-ness (pure)
# ---------------------------------------------------------------------------


def _is_postgres(session: object) -> bool:
    try:
        from dsql_migrator.core.models import SourceType
        from dsql_migrator.ui.data_migration._models import session_source_type

        return session is not None and session_source_type(session) is SourceType.POSTGRES
    except Exception:  # noqa: BLE001 - an unreadable session is not PostgreSQL
        return False


def has_source_connection(session: object) -> bool:
    """A source config AND its in-memory password (never persisted: none after a restart)."""
    return (
        getattr(session, "source_config", None) is not None
        and getattr(session, "source_password", None) is not None
    )


def cdc_evidence(migration_state: object) -> bool:
    """CDC ran in this session (the same evidence the dead-letter gate uses)."""
    return getattr(migration_state, "cdc_ops_window_start", None) is not None or bool(
        getattr(migration_state, "cdc_connector_names", None) or []
    )


def _findings(migration_state: object) -> "dict[str, Any]":
    return dict(getattr(migration_state, "cdc_replica_identity_findings", None) or {})


def _open(finding: object) -> bool:
    return getattr(finding, "restored_at", None) is None


def _selected(migration_state: object) -> "Optional[set[str]]":
    resolved = getattr(migration_state, "resolved_selection", None)
    try:
        names = resolved() if callable(resolved) else None
    except Exception:  # noqa: BLE001 - fall back to the stored selection
        names = None
    if names is None:
        selection = getattr(migration_state, "selection", None)
        names = list(getattr(selection, "selected_tables", None) or []) or None
    return None if names is None else {str(n) for n in names}


def _scope(migration_state: object, session: object, *, prune: bool) -> "dict[str, list[str]]":
    if migration_state is None:
        return {}
    findings = _findings(migration_state)
    scope: dict[str, list[str]] = {}
    if _is_postgres(session):
        deployed = getattr(migration_state, "cdc_deployed_message_key_columns", None)
        if deployed is not None:
            base = dict(deployed)
        else:
            keys_fn = getattr(migration_state, "cdc_message_key_columns", None)
            try:
                keys = dict(keys_fn()) if callable(keys_fn) else {}
            except Exception:  # noqa: BLE001 - no map, no scope
                keys = {}
            selected = _selected(migration_state)
            base = {t: k for t, k in keys.items() if selected is None or t in selected}
        scope = {str(t): [str(c) for c in k] for t, k in base.items() if k}
        # A pure REORDER of the source primary key adds nothing a DEFAULT before-image lacks
        # (the Start prerequisite grades it "nothing to grade"): left out up front whenever the
        # Step 1 inventory knows the table's primary key, so it is never read or named.
        pk_for = getattr(migration_state, "source_primary_key_for", None)
        if callable(pk_for):
            for table in list(scope):
                try:
                    pk = pk_for(table)
                except Exception:  # noqa: BLE001 - unknown: let the read grade it
                    pk = None
                if pk is not None and set(scope[table]) <= set(pk):
                    del scope[table]
        last = getattr(migration_state, "cdc_replica_identity_last_probe", None)
        if prune and last is not None and getattr(last, "ok", False):
            from dsql_migrator.core.cdc_pg_slot import grade_rekeyed_replica_identity

            reads = getattr(last, "reads", None) or {}
            asked = getattr(last, "scope", None) or {}
            for table in list(scope):
                read = reads.get(table)
                if (
                    read is not None
                    and tuple(asked.get(table, ())) == tuple(scope[table])
                    and grade_rekeyed_replica_identity(table, scope[table], read) is None
                    and not (table in findings and _open(findings[table]))
                ):
                    del scope[table]
    for table, finding in findings.items():
        if _open(finding):
            scope.setdefault(table, list(getattr(finding, "key_columns", None) or []))
    return {t: k for t, k in scope.items() if k}


def replica_identity_scope(migration_state: object, session: object) -> "dict[str, list[str]]":
    """The re-keyed tables the re-check is ABOUT: ``{table: composite key columns}``. No I/O.

    PostgreSQL only. The DEPLOYED stack's re-keyed tables when known (MessageKeyColumns and
    SinkKeyColumns, however each is routed), else this session's re-key map limited to the
    selection. A table the last successful read graded "not re-keyed" (its live primary key
    covers the key: a pure reorder) under the same key is left out. Every OPEN finding's table
    stays in scope (so it can be seen FULL again); a restored one is never re-added, so a
    de-scoped table cannot re-open. What the notices name and the gate grades.
    """
    return _scope(migration_state, session, prune=True)


def replica_identity_read_scope(
    migration_state: object, session: object
) -> "dict[str, list[str]]":
    """What one read asks the source about: :func:`replica_identity_scope` WITHOUT leaving out
    the tables graded "not re-keyed" -- the same single statement, one row each -- so the
    scope a read covers never shrinks and re-grows between reads (which would make every
    other read look uncovered and run twice per TTL)."""
    return _scope(migration_state, session, prune=False)


def replica_identity_probe_possible(migration_state: object, session: object) -> bool:
    """Whether the runtime read can run now: something to read, CDC evidence (or an open
    finding), and a source connection with its in-memory password."""
    if migration_state is None or not has_source_connection(session):
        return False
    if not (cdc_evidence(migration_state) or any(map(_open, _findings(migration_state).values()))):
        return False
    return bool(replica_identity_scope(migration_state, session))


def replica_identity_probe_due(
    last: Optional[object],
    scope: "Mapping[str, Sequence[str]]",
    evidence_at: Optional[datetime],
    now: datetime,
) -> bool:
    """Whether a fresh read is needed. Pure.

    None yet; a scoped table the last read did not ask about; a read that started before the
    latest validation evidence (Cut over wants one after it); or older than the TTL. A failed
    read is NOT retried before the TTL (no tight loop) -- "Check again" forces one.
    """
    if last is None:
        return True
    started = _aware(getattr(last, "started_at", None))
    if started is None:
        return True
    if not set(scope or {}) <= set(getattr(last, "scope", None) or {}):
        return True
    evidence = _aware(evidence_at)
    if evidence is not None and started <= evidence:
        return True
    return (_aware(now) - started).total_seconds() > CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS


def _pending_reason(
    last: Optional[object], scope: "Mapping[str, Sequence[str]]", evidence_at: Optional[datetime]
) -> bool:
    """Due for a reason Cut over waits on (never just the TTL)."""
    if last is None or _aware(getattr(last, "started_at", None)) is None:
        return True
    if not set(scope or {}) <= set(getattr(last, "scope", None) or {}):
        return True
    evidence = _aware(evidence_at)
    return evidence is not None and _aware(last.started_at) <= evidence  # type: ignore[attr-defined,operator]


# ---------------------------------------------------------------------------
# The fold (pure)
# ---------------------------------------------------------------------------


def fold_replica_identity_probe(
    findings: "Mapping[str, Any]", probe: Optional[object]
) -> "tuple[dict[str, Any], list[ReplicaIdentityTransition]]":
    """Fold one read into the findings. Pure; returns ``(findings, transitions)``.

    A failed read changes nothing. Per scoped table: weak opens a finding (or re-opens a
    restored one, with a new start time), or silently refreshes an open one's facts; FULL
    restores an open finding at the read's finish; "not re-keyed" (the live primary key now
    covers the key) closes it as ``not-rekeyed``; unknown (not found, unreadable) changes
    nothing.
    """
    current = dict(findings or {})
    if probe is None or not getattr(probe, "ok", False):
        return current, []
    from dsql_migrator.core.cdc_pg_slot import (
        REPLICA_IDENTITY_FULL,
        REPLICA_IDENTITY_WEAK,
        grade_rekeyed_replica_identity,
    )
    from dsql_migrator.core.models import CdcReplicaIdentityFinding

    started = _aware(getattr(probe, "started_at", None)) or _now()
    finished = _aware(getattr(probe, "finished_at", None)) or started
    reads = getattr(probe, "reads", None) or {}
    transitions: list[ReplicaIdentityTransition] = []
    for table, key in sorted((getattr(probe, "scope", None) or {}).items()):
        read = reads.get(table)
        grade = grade_rekeyed_replica_identity(table, list(key), read)
        existing = current.get(table)
        if grade == REPLICA_IDENTITY_WEAK:
            facts = {
                "key_columns": [str(c) for c in key],
                "primary_key": [str(c) for c in read.primary_key],
                "identity": str(read.identity or ""),
                "weak_partitions": int(read.weak_partitions or 0),
                "weak_partition_names": [str(n) for n in read.weak_partition_names][
                    :_PARTITION_LIMIT
                ],
            }
            if existing is None or not _open(existing):
                finding = CdcReplicaIdentityFinding(first_weak_at=started, **facts)
                current[table] = finding
                transitions.append(
                    ReplicaIdentityTransition(
                        kind="opened" if existing is None else "reopened",
                        table=table,
                        finding=finding,
                    )
                )
            else:
                changed = {k: v for k, v in facts.items() if getattr(existing, k) != v}
                if changed:
                    current[table] = existing.model_copy(update=changed)
        elif grade == REPLICA_IDENTITY_FULL or (grade is None and read is not None):
            if existing is not None and _open(existing):
                by = "full" if grade == REPLICA_IDENTITY_FULL else "not-rekeyed"
                finding = existing.model_copy(
                    update={"restored_at": finished, "restored_by": by}
                )
                current[table] = finding
                transitions.append(
                    ReplicaIdentityTransition(
                        kind="restored" if by == "full" else "not-rekeyed",
                        table=table,
                        finding=finding,
                    )
                )
    return current, transitions


# ---------------------------------------------------------------------------
# The read (never on render; bounded; single flight)
# ---------------------------------------------------------------------------


async def _probe_and_land(session: object, migration_state: object, scope: Mapping) -> None:
    """Read on a worker thread, land on the loop, log the transitions; owns the claim."""
    try:
        from nicegui import run

        from dsql_migrator.ui.data_migration._cdc_status import (
            read_cdc_replica_identity_once,
        )

        probe = await run.io_bound(read_cdc_replica_identity_once, session, dict(scope))
        if probe is not None:  # None only when the app is stopping
            transitions = migration_state.apply_cdc_replica_identity_probe(probe)  # type: ignore[attr-defined]
            log_replica_identity_transitions(transitions)
    finally:
        migration_state.finish_cdc_replica_identity_probe()  # type: ignore[attr-defined]


def start_replica_identity_probe(
    session: object, migration_state: object, *, force: bool = False
) -> bool:
    """From the CDC poll: start ONE background read when it is possible and due. Returns
    immediately (the read is spawned, never awaited), so the status landing is never
    delayed; the next poll tick renders the result. Returns whether a read was started."""
    if migration_state is None or not replica_identity_probe_possible(migration_state, session):
        return False
    scope = replica_identity_read_scope(migration_state, session)
    last = getattr(migration_state, "cdc_replica_identity_last_probe", None)
    if not force and not replica_identity_probe_due(last, scope, None, _now()):
        return False
    try:
        import asyncio

        asyncio.get_running_loop()
    except RuntimeError:
        return False
    if not migration_state.begin_cdc_replica_identity_probe():  # type: ignore[attr-defined]
        return False
    from dsql_migrator.ui.data_migration._cdc_status import spawn_detached

    spawn_detached(lambda: _probe_and_land(session, migration_state, scope))
    return True


async def probe_replica_identity_now(session: object, migration_state: object) -> bool:
    """"Check again" in the CDC monitor: one forced read, awaited (and still landed if the
    caller is cancelled). Returns whether a read ran."""
    if migration_state is None or not replica_identity_probe_possible(migration_state, session):
        return False
    scope = replica_identity_read_scope(migration_state, session)
    if not migration_state.begin_cdc_replica_identity_probe():  # type: ignore[attr-defined]
        return False
    from dsql_migrator.ui.data_migration._cdc_status import land_detached

    await land_detached(lambda: _probe_and_land(session, migration_state, scope))
    return True


def _view_signature(migration_state: object, evidence_at: Optional[datetime]) -> tuple:
    """What Validation / Cut over draw from the re-check, so a page is rebuilt only when it
    changed (a silent identical re-read every two minutes rebuilds nothing)."""
    last = getattr(migration_state, "cdc_replica_identity_last_probe", None)
    evidence = _aware(evidence_at)
    findings = tuple(
        sorted(
            (
                t,
                getattr(f, "first_weak_at", None),
                getattr(f, "restored_at", None),
                getattr(f, "identity", ""),
                getattr(f, "weak_partitions", 0),
            )
            for t, f in _findings(migration_state).items()
        )
    )
    probe = (
        None
        if last is None
        else (
            bool(getattr(last, "ok", False)),
            getattr(last, "problem", None),
            getattr(last, "error_class", None),
            tuple(sorted(getattr(last, "scope", None) or {})),
            tuple(sorted(getattr(last, "reads", None) or {})),
            evidence is None or (_aware(last.started_at) or evidence) > evidence,
        )
    )
    return (findings, probe)


# How long a page waits for a read ANOTHER tab (or the CDC poll) is running before it gives
# up without refreshing. The read itself is bounded by the dialect's connect_timeout plus its
# 10 s statement_timeout, so it lands well within this.
_WAIT_FOR_OTHER_READ_SECONDS = 30.0


async def _refresh_after_other_read(
    migration_state: object,
    evidence_at: Optional[datetime],
    before: tuple,
    refresh: Callable[[], None],
    *,
    always: bool,
) -> None:
    import asyncio

    waited = 0.0
    while getattr(migration_state, "cdc_replica_identity_probe_running", False):
        if waited >= _WAIT_FOR_OTHER_READ_SECONDS:
            return
        await asyncio.sleep(0.5)
        waited += 0.5
    if always or _view_signature(migration_state, evidence_at) != before:
        try:
            refresh()
        except Exception:  # noqa: BLE001 - the page went away
            pass


def schedule_replica_identity_probe(
    ui: object,
    session: object,
    migration_state: object,
    refresh: Callable[[], None],
    *,
    evidence_at: Optional[datetime],
    force: bool = False,
) -> None:
    """Validation / Cut over: arm ONE background read when it is due (or ``force``d by
    Check again). The render never reads the source: this arms a one-shot timer, the claim is
    taken inside it, the read and its landing are detached from the timer (a refresh cannot
    drop them), and ``refresh`` runs only when what the page draws changed (always after a
    forced read, which re-enables its button). When another tab or the CDC poll holds the
    claim, the timer waits for THAT read instead (bounded) and refreshes on a change -- and
    always when this page was drawn waiting on "Re-reading" (Cut over), because that other
    read may itself have started before the latest validation: the re-render then arms a
    read of its own. So a page waiting on "Re-reading" is never left behind."""
    if migration_state is None or not replica_identity_probe_possible(migration_state, session):
        return
    scope = replica_identity_read_scope(migration_state, session)
    armed_with = getattr(migration_state, "cdc_replica_identity_last_probe", None)
    if not force and not replica_identity_probe_due(armed_with, scope, evidence_at, _now()):
        return
    before = _view_signature(migration_state, evidence_at)
    # Whether the gate drew this page "pending" (the same scope and rule it uses): such a
    # page has no button and no periodic re-render of its own.
    drawn_pending = _pending_reason(
        armed_with, replica_identity_scope(migration_state, session), evidence_at
    )

    async def _check() -> None:
        from dsql_migrator.ui.data_migration._cdc_status import land_detached

        current = getattr(migration_state, "cdc_replica_identity_last_probe", None)
        if not replica_identity_probe_possible(migration_state, session):
            return
        scope_now = replica_identity_read_scope(migration_state, session)
        if force:
            if current is not armed_with:
                # A newer read already landed since the click: show it (re-enables Check
                # again) rather than read again.
                await _refresh_after_other_read(
                    migration_state, evidence_at, before, refresh, always=True
                )
                return
        elif not replica_identity_probe_due(current, scope_now, evidence_at, _now()):
            # Another read (an earlier timer, another tab, the CDC poll) already landed
            # since this page was drawn: show it if it changed what the page draws.
            await _refresh_after_other_read(
                migration_state, evidence_at, before, refresh, always=False
            )
            return
        if not migration_state.begin_cdc_replica_identity_probe():  # type: ignore[attr-defined]
            await _refresh_after_other_read(
                migration_state, evidence_at, before, refresh, always=force or drawn_pending
            )
            return

        async def _read_and_land() -> None:
            await _probe_and_land(session, migration_state, scope_now)
            if force or _view_signature(migration_state, evidence_at) != before:
                try:
                    refresh()
                except Exception:  # noqa: BLE001 - the page went away
                    pass

        await land_detached(_read_and_land)

    ui.timer(0.05, _check, once=True)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# The gate (pure)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ReplicaIdentityIssue:
    """A finding the gate has not cleared: ``weak`` (open) or a G1 proof reason."""

    table: str
    finding: object
    reason: str
    compared_from: Optional[datetime] = None


@dataclass(frozen=True)
class ReplicaIdentityGate:
    """The gate's verdict. ``ReplicaIdentityGate()`` is the INERT gate."""

    applies: bool = False
    weak: tuple[ReplicaIdentityIssue, ...] = ()
    issues: tuple[ReplicaIdentityIssue, ...] = ()
    resolved: tuple[str, ...] = ()
    probe_state: Optional[str] = None
    unread: tuple[str, ...] = ()
    scope: Mapping[str, tuple] = field(default_factory=dict)
    last_probe: Optional[object] = None
    waived: bool = False
    migration_type: Optional[object] = None
    has_source_connection: bool = False

    @property
    def must_recheck(self) -> tuple[ReplicaIdentityIssue, ...]:
        from dsql_migrator.ui.cdc_dlq_gate import MUST_RECHECK_REASONS

        return tuple(i for i in self.issues if i.reason in MUST_RECHECK_REASONS)

    @property
    def waivable(self) -> tuple[ReplicaIdentityIssue, ...]:
        from dsql_migrator.ui.cdc_dlq_gate import WAIVABLE_REASONS

        return tuple(i for i in self.issues if i.reason in WAIVABLE_REASONS)

    @property
    def pending(self) -> bool:
        return self.probe_state == PROBE_STATE_PENDING

    @property
    def blocks(self) -> bool:
        return self.applies and (
            bool(self.weak)
            or bool(self.must_recheck)
            or (bool(self.waivable) and not self.waived)
        )

    @property
    def held(self) -> bool:
        return self.blocks or (self.applies and self.pending)

    @property
    def can_waive(self) -> bool:
        return (
            self.applies
            and bool(self.waivable)
            and not self.weak
            and not self.must_recheck
            and not self.pending
            and not self.waived
        )

    @property
    def recheckable(self) -> tuple[str, ...]:
        """Tables a CHECKSUM re-check can clear. Weak tables never: deletes can keep being lost."""
        return tuple(i.table for i in self.must_recheck)

    @property
    def unchecked(self) -> bool:
        """Released without a usable re-read (fail-open; warned and logged)."""
        return self.probe_state in (PROBE_STATE_UNAVAILABLE, PROBE_STATE_FAILED) or bool(
            self.unread
        )


def replica_identity_waiver_covers(
    waiver: Optional[object], waivable: "Sequence[ReplicaIdentityIssue]"
) -> bool:
    """A recorded waiver covers only when EVERY waivable table maps to its CURRENT
    ``restored_at`` -- a later drop (a new finding, a new restore) re-opens the check."""
    if waiver is None or not waivable:
        return False
    tables = dict(getattr(waiver, "tables", None) or {})
    for issue in waivable:
        restored = _aware(getattr(issue.finding, "restored_at", None))
        waived_for = _aware(tables.get(issue.table))
        if restored is None or waived_for is None or waived_for != restored:
            return False
    return True


def cdc_replica_identity_cutover_gate(
    *,
    findings: "Mapping[str, Any]",
    scope: "Mapping[str, Sequence[str]]",
    cdc_in_use: bool,
    cdc_evidence: bool,
    is_pg: bool,
    check_probe: bool,
    report: Optional[object],
    release: str,
    explained_tables: "Sequence[str]" = (),
    compared_run_start: Optional[datetime] = None,
    compared_overrides: "Optional[Mapping[str, datetime]]" = None,
    evidence_at: Optional[datetime] = None,
    last_probe: Optional[object] = None,
    has_source_connection: bool = False,
    waiver: Optional[object] = None,
    migration_type: Optional[object] = None,
) -> ReplicaIdentityGate:
    """Evaluate the gate from explicit inputs. Pure (see the module docstring for the rule)."""
    from dsql_migrator.ui.cdc_dlq_gate import table_proof_reason

    findings = dict(findings or {})
    scope = {t: tuple(k) for t, k in dict(scope or {}).items()}
    applies = bool(findings) or (cdc_in_use and cdc_evidence and is_pg and bool(scope))
    if not applies:
        return ReplicaIdentityGate(migration_type=migration_type)
    overrides = {k: _aware(v) for k, v in (compared_overrides or {}).items() if v is not None}
    run_start = _aware(compared_run_start)
    items = {item.table: item for item in (getattr(report, "items", None) or ())}
    explained = set(explained_tables or ())
    weak: list[ReplicaIdentityIssue] = []
    issues: list[ReplicaIdentityIssue] = []
    resolved: list[str] = []
    for table in sorted(findings):
        finding = findings[table]
        if _open(finding):
            weak.append(ReplicaIdentityIssue(table=table, finding=finding, reason=REASON_WEAK))
            continue
        started = overrides.get(table) or run_start
        reason = table_proof_reason(
            items.get(table),
            release=release,
            explained=table in explained,
            started=started,
            last_at=_aware(getattr(finding, "restored_at", None)),
        )
        if reason is None:
            resolved.append(table)
        else:
            issues.append(
                ReplicaIdentityIssue(
                    table=table, finding=finding, reason=reason, compared_from=started
                )
            )
    probe_state: Optional[str] = None
    unread: tuple[str, ...] = ()
    # Exactly when replica_identity_probe_possible() can run a read (given a password), so a
    # "pending" hold always has a read that can clear it.
    if check_probe and is_pg and scope and ((cdc_in_use and cdc_evidence) or weak):
        if not has_source_connection:
            probe_state = PROBE_STATE_UNAVAILABLE
        elif _pending_reason(last_probe, scope, evidence_at):
            probe_state = PROBE_STATE_PENDING
        elif not getattr(last_probe, "ok", False):
            probe_state = PROBE_STATE_FAILED
        else:
            probe_state = PROBE_STATE_OK
            reads = getattr(last_probe, "reads", None) or {}
            unread = tuple(sorted(t for t in scope if t not in reads))
    waivable = [i for i in issues if i.reason in _waivable_reasons()]
    return ReplicaIdentityGate(
        applies=True,
        weak=tuple(weak),
        issues=tuple(issues),
        resolved=tuple(resolved),
        probe_state=probe_state,
        unread=unread,
        scope=scope,
        last_probe=last_probe,
        waived=replica_identity_waiver_covers(waiver, waivable),
        migration_type=migration_type,
        has_source_connection=has_source_connection,
    )


def _waivable_reasons() -> frozenset:
    from dsql_migrator.ui.cdc_dlq_gate import WAIVABLE_REASONS

    return WAIVABLE_REASONS


def replica_identity_gate_for(
    migration_state: object,
    validation_state: object,
    session: object,
    *,
    cdc_in_use: bool,
    release: str,
    summary: Optional[object],
    check_probe: bool = True,
) -> ReplicaIdentityGate:
    """The gate for this session's state. No I/O; ``migration_state`` None -> inert."""
    if migration_state is None:
        return ReplicaIdentityGate()
    from dsql_migrator.ui.cdc_dlq_gate import validation_evidence_at

    try:
        migration_type = getattr(migration_state, "migration_type", None)
    except Exception:  # noqa: BLE001 - copy only
        migration_type = None
    return cdc_replica_identity_cutover_gate(
        findings=_findings(migration_state),
        scope=replica_identity_scope(migration_state, session),
        cdc_in_use=cdc_in_use,
        cdc_evidence=cdc_evidence(migration_state),
        is_pg=_is_postgres(session),
        check_probe=check_probe,
        report=getattr(validation_state, "result", None),
        release=release,
        explained_tables=tuple(getattr(summary, "quarantine_explained_tables", ()) or ()),
        compared_run_start=getattr(validation_state, "compared_run_start", None),
        compared_overrides=getattr(validation_state, "compared_overrides", None) or {},
        evidence_at=validation_evidence_at(validation_state),
        last_probe=getattr(migration_state, "cdc_replica_identity_last_probe", None),
        has_source_connection=has_source_connection(session),
        waiver=getattr(validation_state, "cutover_replica_identity_waiver", None),
        migration_type=migration_type,
    )


def cdc_checksum_forced_tables(migration_state: object) -> "set[str]":
    """Tables Validation always compares by CHECKSUM: every dead-lettered table (G1), every
    table with a REPLICA IDENTITY finding (G2) and every table with a source-schema finding
    (G4, ``ui/cdc_catalog_drift_gate.py``). Only ever ADDS tables to a CHECKSUM pass."""
    return (
        set(getattr(migration_state, "cdc_dlq_ledger", None) or {})
        | set(_findings(migration_state))
        | set(getattr(migration_state, "cdc_catalog_drift_findings", None) or {})
    )


def replica_identity_proven_tables(
    findings: "Mapping[str, Any]", validation_state: object
) -> "set[str]":
    """Restored findings a value-verified CHECKSUM started after the restore (+ margin) has
    already proven -- the CDC monitor stops showing them. Pure."""
    from dsql_migrator.ui.cdc_dlq_gate import table_proof_reason

    if validation_state is None:
        return set()
    report = getattr(validation_state, "result", None)
    items = {i.table: i for i in (getattr(report, "items", None) or ())}
    run_start = _aware(getattr(validation_state, "compared_run_start", None))
    overrides = getattr(validation_state, "compared_overrides", None) or {}
    proven: set[str] = set()
    for table, finding in dict(findings or {}).items():
        if _open(finding):
            continue
        started = _aware(overrides.get(table)) or run_start
        if (
            table_proof_reason(
                items.get(table),
                release="released",
                explained=False,
                started=started,
                last_at=_aware(getattr(finding, "restored_at", None)),
            )
            is None
        ):
            proven.add(table)
    return proven


# ---------------------------------------------------------------------------
# Copy (value-free: table / partition names, identity codes, times, error classes)
# ---------------------------------------------------------------------------


_IDENTITY_LABELS = {
    "d": "REPLICA IDENTITY DEFAULT",
    "n": "REPLICA IDENTITY NOTHING",
    "i": "REPLICA IDENTITY USING INDEX",
    "f": "REPLICA IDENTITY FULL",
}


# Dated, like the dead-letter gate's times next to them on Cut over: a finding persists and CDC
# runs for days, so a bare "10:00 until 09:30" would read backwards across midnight.
def _fmt_hm(value: Optional[datetime]) -> str:
    value = _aware(value)
    return value.strftime("%Y-%m-%d %H:%M UTC") if value is not None else "an unknown time"


def _fmt_hms(value: Optional[datetime]) -> str:
    value = _aware(value)
    return value.strftime("%Y-%m-%d %H:%M:%S UTC") if value is not None else "an unknown time"


def _bounded(names: "Sequence[str]", limit: int = _NAME_LIMIT) -> str:
    names = list(names)
    shown = ", ".join(names[:limit])
    return shown + (f" (+{len(names) - limit} more)" if len(names) > limit else "")


def identity_label(finding: object) -> str:
    """How a finding's identity reads in copy (the table's own code, and its partitions)."""
    code = str(getattr(finding, "identity", "") or "")
    count = int(getattr(finding, "weak_partitions", 0) or 0)
    names = list(getattr(finding, "weak_partition_names", None) or [])[:_PARTITION_LIMIT]
    sample = ", ".join(names) + (
        f" (+{count - len(names)} more)" if count > len(names) else ""
    )
    if count and code == "f":
        return f"FULL on the table, but {count} partition(s) are not: {sample}"
    label = _IDENTITY_LABELS.get(code, "REPLICA IDENTITY unknown")
    if count:
        return f"{label}, and {count} partition(s) are not FULL: {sample}"
    return label


def _labelled(issues: "Sequence[ReplicaIdentityIssue]") -> str:
    shown = ", ".join(f"{i.table} ({identity_label(i.finding)})" for i in issues[:_NAME_LIMIT])
    more = len(issues) - _NAME_LIMIT
    return shown + (f" (+{more} more)" if more > 0 else "")


def _is_type(migration_type: object, name: str) -> bool:
    return getattr(migration_type, "name", None) == name


def repair_line(migration_type: object) -> str:
    """How to remove rows a lost DELETE left behind, by migration type."""
    if _is_type(migration_type, "CDC_ONLY"):
        return (
            "If Validation then reports a table as different, delete the rows the source no "
            "longer has on Aurora DSQL by hand (CDC only has no Full Load step to reload "
            "from), then re-check it."
        )
    return (
        "If Validation then reports a table as different, let CDC catch up (Stream lag: "
        "caught up), Stop CDC, reload the table on the Full Load step with 'Drop & reload', "
        "then Start CDC and re-check it. An append reload cannot remove a row a lost DELETE "
        "left behind."
    )


def wrong_row_sentence(issues: "Sequence[ReplicaIdentityIssue]") -> str:
    """Only when a key omits part of the source primary key: a lost DELETE can also hit a
    different row (one whose key really holds the placeholder value)."""
    names = [i.table for i in issues if getattr(i.finding, "key_omits_source_pk", False)]
    if not names:
        return ""
    return (
        f" The key of {_bounded(names)} does not contain the whole source primary key, so "
        "such a DELETE can also remove a different row."
    )


_NO_PASSWORD_SENTENCE = (
    " The source password is kept in memory only: reconnect the source on the Connect step "
    "so the tool can re-check."
)


def _failed_read_sentence(last_probe: Optional[object], *, check_again: bool = True) -> str:
    """While a finding is open, a FAILED re-read must be visible: otherwise "Check again"
    looks like it does nothing and the hold cannot be cleared (a rotated password, a changed
    security group). "" unless the last read failed -- the no-password case has its own
    sentence. Value-free: the error class only, never its message."""
    if (
        last_probe is None
        or getattr(last_probe, "ok", True)
        or getattr(last_probe, "problem", None) != PROBE_READ_FAILED
    ):
        return ""
    then = ", then choose Check again." if check_again else " so the tool can re-read it."
    return (
        f" The last re-read at {_fmt_hms(getattr(last_probe, 'started_at', None))} failed "
        f"({getattr(last_probe, 'error_class', None) or 'unknown error'}), so the tool cannot "
        "see the fix yet: if the source credentials or network access changed, reconnect the "
        f"source on the Connect step{then}"
    )


def _weak_issues(findings: "Mapping[str, Any]") -> list[ReplicaIdentityIssue]:
    return [
        ReplicaIdentityIssue(table=t, finding=f, reason=REASON_WEAK)
        for t, f in sorted(dict(findings or {}).items())
        if _open(f)
    ]


def monitor_weak_notice(
    findings: "Mapping[str, Any]",
    *,
    has_password: bool,
    last_probe: Optional[object] = None,
) -> Optional[tuple[str, str]]:
    """``(header, body)`` of the CDC monitor's ERROR notice, or ``None`` with none open. It
    also says why the re-read cannot clear it yet: no password, or the last read failed."""
    weak = _weak_issues(findings)
    if not weak:
        return None
    found = min(
        (_aware(getattr(i.finding, "first_weak_at", None)) for i in weak),
        key=lambda d: d or _now(),
    )
    body = (
        f"{_labelled(weak)}, found at {_fmt_hm(found)}. These tables have the composite "
        "primary key chosen in Schema Conversion, and a DELETE carries that key's added "
        "column only under REPLICA IDENTITY FULL. While it is below FULL, a DELETE can leave "
        "its row on Aurora DSQL with no error and no dead letter, and an UPDATE that changes "
        "a key column can be lost too. Run ALTER TABLE <table> REPLICA IDENTITY FULL "
        "on the source now; for a partitioned table, run it on each partition too (the "
        "parent does not propagate, and a new partition starts at DEFAULT). Then choose "
        "Check again. Deletes made before the fix are not replayed, so Cut over is held "
        "until REPLICA IDENTITY is FULL again and each table matches in a CHECKSUM "
        "comparison started after the fix."
    )
    body += wrong_row_sentence(weak)
    if not has_password:
        body += _NO_PASSWORD_SENTENCE
    else:
        body += _failed_read_sentence(last_probe)
    return (
        "Deletes on re-keyed tables can be lost — REPLICA IDENTITY is not FULL on the "
        "source",
        body,
    )


def _not_rekeyed(finding: object) -> bool:
    return getattr(finding, "restored_by", None) == "not-rekeyed"


def _window(finding: object) -> str:
    """The span a restored finding was below FULL, by how it closed: never "until" a FULL read
    that did not happen (closed because CDC stopped re-keying it, the identity can still be
    DEFAULT)."""
    first = _fmt_hm(getattr(finding, "first_weak_at", None))
    restored = _fmt_hm(getattr(finding, "restored_at", None))
    if _not_rekeyed(finding):
        return f"below FULL from at least {first}; CDC stopped re-keying it at {restored}"
    return f"below FULL from at least {first} until {restored}"


def _below_full_until(issues: "Sequence[ReplicaIdentityIssue]", *, also: bool = False) -> str:
    """"was below FULL until X" -- or, when CDC stopped re-keying one of them (its identity
    may still be below FULL), "was below FULL while CDC re-keyed them, until X"."""
    latest = _fmt_hm(_latest_restore(issues))
    was = "was also" if also else "was"
    if any(_not_rekeyed(i.finding) for i in issues):
        return f"{was} below FULL while CDC re-keyed them, until {latest}"
    return f"{was} below FULL until {latest}"


def _restored_phrase(issue: ReplicaIdentityIssue) -> str:
    return f"{issue.table} ({_window(issue.finding)})"


def _restored_list(issues: "Sequence[ReplicaIdentityIssue]") -> str:
    shown = ", ".join(_restored_phrase(i) for i in issues[:_NAME_LIMIT])
    more = len(issues) - _NAME_LIMIT
    return shown + (f" (+{more} more)" if more > 0 else "")


def _latest_restore(issues: "Sequence[ReplicaIdentityIssue]") -> Optional[datetime]:
    stamps = [_aware(getattr(i.finding, "restored_at", None)) for i in issues]
    stamps = [s for s in stamps if s is not None]
    return max(stamps) if stamps else None


def replica_identity_waived_tables(
    findings: "Mapping[str, Any]", waiver: Optional[object]
) -> "set[str]":
    """Restored findings the operator's Cut over waiver covers at its CURRENT restore (a later
    drop and restore makes a table wait again). Pure. Per table, so an approximation of the
    gate's all-or-nothing rule: a table that becomes waivable later still shows as waiting."""
    if waiver is None:
        return set()
    return {
        t
        for t, f in dict(findings or {}).items()
        if not _open(f)
        and replica_identity_waiver_covers(
            waiver, [ReplicaIdentityIssue(table=t, finding=f, reason="not-validated")]
        )
    }


def monitor_restored_notice(
    findings: "Mapping[str, Any]",
    proven: "set[str]",
    migration_type: object,
    waived: "Optional[set[str]]" = None,
) -> Optional[tuple[str, str]]:
    """``(header, body)`` of the CDC monitor's WARNING for restored, not-yet-proven tables.

    "FULL again" only when every listed table was read FULL; a table CDC stopped re-keying
    (its identity may still be DEFAULT) gets a header true for both. ``waived`` tables (the
    operator continued without this proof on Cut over) are not said to hold Cut over.
    """
    from dsql_migrator.ui.cdc_dlq_gate import CDC_DLQ_EVIDENCE_MARGIN_SECONDS

    restored = [
        ReplicaIdentityIssue(table=t, finding=f, reason="restored")
        for t, f in sorted(dict(findings or {}).items())
        if not _open(f) and t not in proven
    ]
    if not restored:
        return None
    waived = set(waived or ())
    waiting = [i for i in restored if i.table not in waived]
    skipped = [i.table for i in restored if i.table in waived]
    if any(_not_rekeyed(i.finding) for i in restored):
        header = (
            "REPLICA IDENTITY was below FULL on re-keyed tables — deletes made then may be "
            "missing"
        )
    else:
        header = "REPLICA IDENTITY is FULL again — deletes made before the fix may be missing"
    body = (
        f"{_restored_list(restored)}. Deletes made in that time may not have removed their "
        "rows on Aurora DSQL, and nothing replays them. Validation compares these tables by "
        "CHECKSUM automatically."
    )
    if waiting:
        latest = _latest_restore(waiting)
        # The exact threshold (seconds and date), never one earlier than the gate's real one.
        after = (
            latest + timedelta(seconds=CDC_DLQ_EVIDENCE_MARGIN_SECONDS)
            if latest is not None
            else None
        )
        whom = "each" if not skipped else f"each of {_bounded([i.table for i in waiting])}"
        body += (
            f" Cut over waits until {whom} matches in a comparison started after "
            f"{_fmt_hms(after)}."
        )
    if skipped:
        body += (
            " You chose on the Cut over step to continue without this proof for "
            f"{_bounded(skipped)}; that decision is in the activity log."
        )
    return (header, f"{body} {repair_line(migration_type)}")


def monitor_unchecked_notice(
    scope: "Mapping[str, Sequence[str]]", last_probe: Optional[object], *, has_password: bool
) -> Optional[tuple[str, str, str]]:
    """``(tone, header, body)`` of the CDC monitor's notice when the re-check cannot run:
    ``info`` without the source password (expected after a restart, it is kept in memory
    only), ``warning`` when the last read FAILED (unexpected: until it works again a drop
    below FULL goes unseen). ``None`` otherwise."""
    names = _bounded(sorted(scope))
    if not names:
        return None
    if not has_password:
        return (
            "info",
            "Can't re-check REPLICA IDENTITY on the source",
            "While CDC runs, the tool re-reads REPLICA IDENTITY on the re-keyed tables "
            f"({names}) every 2 minutes, because a DELETE on them can be lost silently if it "
            "drops below FULL. The source password is kept in memory only, so after an app "
            "restart the tool cannot connect: reconnect the source on the Connect step to "
            "resume the check.",
        )
    if last_probe is not None and not getattr(last_probe, "ok", True):
        return (
            "warning",
            "Can't re-check REPLICA IDENTITY on the source",
            f"The last re-read of REPLICA IDENTITY on the re-keyed tables ({names}) at "
            f"{_fmt_hms(getattr(last_probe, 'started_at', None))} failed "
            f"({getattr(last_probe, 'error_class', None) or 'unknown error'}); the tool tries "
            "again in 2 minutes. Keep these tables at REPLICA IDENTITY FULL on the source.",
        )
    return None


def cutover_weak_notice(gate: ReplicaIdentityGate) -> Optional[tuple[str, str]]:
    """``(header, body)`` of Cut over's ERROR while a finding is open."""
    if not gate.weak:
        return None
    body = (
        f"REPLICA IDENTITY on {_labelled(gate.weak)} is below FULL on the source, so their "
        "DELETEs (and UPDATEs that change the source primary key) can miss their rows on Aurora "
        "DSQL with no error and no dead letter, and an UPDATE of the added key column alone is "
        "dead-lettered and leaves the old row. Run ALTER TABLE <table> REPLICA IDENTITY FULL on "
        "the source (on each partition of a partitioned table), then choose Check again. Once "
        "the tool reads FULL, each table must match in a CHECKSUM comparison started more "
        "than a minute later; Validation compares them by CHECKSUM automatically."
    )
    if not gate.has_source_connection:
        body += _NO_PASSWORD_SENTENCE
    elif gate.probe_state == PROBE_STATE_FAILED:
        body += _failed_read_sentence(gate.last_probe)
    body += wrong_row_sentence(gate.weak)
    return (
        f"Cut over is held: deletes on {len(gate.weak)} re-keyed table(s) can be lost",
        body,
    )


def cutover_pending_notice(gate: ReplicaIdentityGate) -> Optional[tuple[str, str]]:
    if not gate.pending:
        return None
    return (
        "Re-reading REPLICA IDENTITY on the source",
        "Cut over waits for one fresh read of REPLICA IDENTITY on the re-keyed tables "
        f"({_bounded(sorted(gate.scope))}), so a change since your last validation is "
        "caught. This takes a few seconds.",
    )


_RI_REASON_TEXT = {
    "compared-before": "its last comparison started before that",
    "row-count-only": "it was compared by row count only",
    "not-matched": "it does not match",
    "compare-time-unknown": "its comparison time was lost in a restore",
}


def cutover_must_recheck_notice(gate: ReplicaIdentityGate) -> Optional[tuple[str, str]]:
    must = gate.must_recheck
    if not must:
        return None
    n = len(must)

    def _line(issue: ReplicaIdentityIssue) -> str:
        return (
            f"{issue.table}: {_window(issue.finding)} — "
            f"{_RI_REASON_TEXT.get(issue.reason, issue.reason)}"
        )

    shown = "; ".join(_line(i) for i in must[:_NAME_LIMIT]) + (
        f"; +{n - _NAME_LIMIT} more" if n > _NAME_LIMIT else ""
    )
    # The Validation button is ONE action for every table a CHECKSUM re-check clears (this
    # gate's and the dead-letter gate's), so its count is not quoted here.
    return (
        f"Re-check {n} re-keyed table(s) with CHECKSUM before cutting over",
        "REPLICA IDENTITY on the source was below FULL for a while, so DELETEs made then may "
        f"not have reached Aurora DSQL: {shown}. Each table must match in a CHECKSUM "
        "comparison started more than a minute after the fix. On the Validation step, "
        "re-check them with CHECKSUM (one button there re-checks every table that needs it); "
        f"this check clears by itself once they match. {repair_line(gate.migration_type)}",
    )


def cutover_waivable_notice(gate: ReplicaIdentityGate) -> Optional[tuple[str, str]]:
    """Restored tables Validation cannot prove: what clears each kind, and the waiver -- only
    offered outright when "Continue without this check" is actually drawn (G1's rule: never
    an offer of a button that is not there)."""
    from dsql_migrator.ui.cdc_dlq_gate import REASON_ACCEPTED_GAP, REASON_NOT_VALIDATED

    waivable = gate.waivable
    if not waivable or gate.waived:
        return None
    reasons = {i.reason for i in waivable}
    # A table outside the report needs a RUN that includes it; a table inside the accepted
    # gap can never match until its source rows are fixed and it is reloaded.
    remedies: list[str] = []
    if REASON_NOT_VALIDATED in reasons:
        remedies.append(
            "Include a table that is not in the report on the Validation step and re-run "
            "Validation (it is compared by CHECKSUM automatically)."
        )
    if REASON_ACCEPTED_GAP in reasons:
        remedies.append(
            "A table inside the gap you accepted cannot be cleared by a CHECKSUM unless its "
            "source rows are corrected and it is reloaded with 'Drop & reload'."
        )
    if gate.weak:
        offer = (
            "Once the tables above are back at REPLICA IDENTITY FULL and re-checked, you can "
            "also continue without this check"
        )
    elif gate.must_recheck:
        offer = "Once the tables above are re-checked, you can also continue without this check"
    elif gate.pending:
        offer = "Once the re-read above finishes, you can also continue without this check"
    else:
        offer = "Or continue without this check"
    return (
        f"{len(waivable)} re-keyed table(s) have no CHECKSUM proof since REPLICA IDENTITY "
        "was restored",
        f"{_bounded([i.table for i in waivable])} are not in the validation report (left out "
        "on the Validation step) or are part of the gap you accepted, so Validation cannot "
        "show that no delete was lost while their REPLICA IDENTITY was below FULL. "
        + " ".join(remedies)
        + f" {offer}; that decision is recorded in the activity log.",
    )


def cutover_waived_notice(gate: ReplicaIdentityGate) -> Optional[tuple[str, str]]:
    if not gate.waived:
        return None
    return (
        "Cutting over without CHECKSUM proof for re-keyed tables",
        f"You chose to continue without CHECKSUM proof for "
        f"{_bounded([i.table for i in gate.waivable])}, whose REPLICA IDENTITY was below "
        "FULL earlier. That decision is in the activity log; this step locks again if "
        "REPLICA IDENTITY drops below FULL again.",
    )


def cutover_unchecked_notice(gate: ReplicaIdentityGate) -> Optional[tuple[str, str]]:
    """The fail-open WARNING above the runbook when the re-read could not run or read all."""
    if not gate.applies or not gate.unchecked:
        return None
    names = _bounded(sorted(gate.scope))
    parts: list[str] = []
    if gate.probe_state == PROBE_STATE_UNAVAILABLE:
        parts.append(
            f"The tool re-reads REPLICA IDENTITY on the re-keyed tables ({names}) before cut "
            "over, because a DELETE on them can be lost silently while it is below FULL. The "
            "source password is kept in memory only and this session has none: reconnect "
            "the source on the Connect step to run the check."
        )
    elif gate.probe_state == PROBE_STATE_FAILED:
        last = gate.last_probe
        parts.append(
            f"The re-read of REPLICA IDENTITY on the re-keyed tables ({names}) at "
            f"{_fmt_hms(getattr(last, 'started_at', None))} failed "
            f"({getattr(last, 'error_class', None) or 'unknown error'}); choose Check again."
        )
    if gate.unread and gate.probe_state == PROBE_STATE_OK:
        parts.append(
            f"{_bounded(list(gate.unread))} were not found on the source, so their REPLICA "
            "IDENTITY could not be read."
        )
    if not parts:
        return None
    return (
        "Cut over could not re-read REPLICA IDENTITY on the source",
        " ".join(parts)
        + " Without this check, confirm on the source that these tables (and each of their "
        "partitions) are REPLICA IDENTITY FULL before you cut over.",
    )


def replica_identity_verdict_hold(
    gate: Optional[ReplicaIdentityGate], *, total: int, as_of: str
) -> Optional[tuple[str, str, str]]:
    """``(tone, header, body)`` replacing a green "Ready for cut-over" while the gate holds.

    Led by the most severe case (still below FULL, then a re-check, then not in the report);
    the body also names every other held table, so the "Re-check" button under it never
    re-checks a table no notice mentions.
    """
    if gate is None or not gate.applies:
        return None
    must = gate.must_recheck
    unvalidated = (
        () if gate.waived else tuple(i for i in gate.issues if i.reason == "not-validated")
    )
    also_unvalidated = (
        f" REPLICA IDENTITY on {_bounded([i.table for i in unvalidated])} was also below FULL "
        "while CDC ran, and this report does not cover them — include them and re-run, or, "
        "once the other tables are cleared, acknowledge them on the Cut over step."
        if unvalidated
        else ""
    )
    if gate.weak:
        body = (
            f"All {total} table(s) match as-of {as_of}, but REPLICA IDENTITY on "
            f"{_bounded([i.table for i in gate.weak])} is below FULL on the source, so their "
            "DELETEs can miss Aurora DSQL and this match may not last. Run ALTER TABLE … "
            "REPLICA IDENTITY FULL on the source (on each partition too); the tool re-reads "
            "it, and Cut over then needs a CHECKSUM match started after the fix."
        )
        if not gate.has_source_connection:
            body += _NO_PASSWORD_SENTENCE
        else:
            body += _failed_read_sentence(gate.last_probe, check_again=False)
        if must:
            body += (
                f" REPLICA IDENTITY on {_bounded([i.table for i in must])} "
                f"{_below_full_until(must, also=True)}, and none of these has a CHECKSUM "
                "match that started more than a minute after that: re-check them below."
            )
        return (
            "error",
            "Tables match — cut over is held: REPLICA IDENTITY on "
            f"{len(gate.weak)} re-keyed table(s) is below FULL",
            body + also_unvalidated,
        )
    if must:
        return (
            "warning",
            f"Tables match — cut over waits for a CHECKSUM re-check of {len(must)} re-keyed "
            "table(s)",
            f"All {total} table(s) match as-of {as_of}. But REPLICA IDENTITY on "
            f"{_bounded([i.table for i in must])} {_below_full_until(must)}, so a DELETE made "
            "before then may not have reached Aurora DSQL, and none of these tables has a "
            "CHECKSUM match that started more than a minute after that. Re-check them below; "
            "if a table then differs, this report shows how to repair it." + also_unvalidated,
        )
    if unvalidated:
        return (
            "warning",
            f"Tables match — {len(unvalidated)} re-keyed table(s) whose REPLICA IDENTITY was "
            "below FULL are not in this report",
            f"All {total} table(s) match as-of {as_of}, but REPLICA IDENTITY on "
            f"{_bounded([i.table for i in unvalidated])} was below FULL while CDC ran, and "
            "this report does not cover them. Include them and re-run, or acknowledge them "
            "on the Cut over step.",
        )
    return None


def replica_identity_options_hint(tables: "Sequence[str]") -> str:
    """The Validation Options hint naming finding tables the next run CHECKSUMs ("" if none)."""
    tables = list(tables)
    if not tables:
        return ""
    return (
        "REPLICA IDENTITY on the source was below FULL while CDC ran for "
        f"{len(tables)} re-keyed table(s) ({_bounded(tables)}): whatever mode you pick, "
        "this run also compares them by CHECKSUM."
    )


def replica_identity_start_log_clause(tables: "Sequence[str]") -> str:
    """The "validation started" clause for finding tables not already dead-lettered."""
    tables = list(tables)
    if not tables:
        return ""
    return (
        "; tables whose REPLICA IDENTITY dropped below FULL are also compared by CHECKSUM: "
        f"{_bounded(tables)}"
    )


def replica_identity_held_log_clause(gate: Optional[ReplicaIdentityGate]) -> Optional[str]:
    """The "validation completed" clause replacing "ready for cut-over" while held."""
    if gate is None or not gate.applies:
        return None
    if gate.weak:
        return (
            "; cut over held: REPLICA IDENTITY below FULL on "
            f"{_bounded([i.table for i in gate.weak])}"
        )
    held = tuple(
        i for i in gate.issues
        if i.reason in _must_recheck_reasons() or (i.reason == "not-validated" and not gate.waived)
    )
    if not held:
        return None
    return (
        f"; cut over held: {len(held)} re-keyed table(s) lack a CHECKSUM match after "
        f"REPLICA IDENTITY was restored: {_bounded([i.table for i in held])}"
    )


def _must_recheck_reasons() -> frozenset:
    from dsql_migrator.ui.cdc_dlq_gate import MUST_RECHECK_REASONS

    return MUST_RECHECK_REASONS


def replica_identity_ack_log_clause(gate: Optional[ReplicaIdentityGate]) -> str:
    """The clause appended to "cut over acknowledged" ("" when the gate is inert)."""
    if gate is None or not gate.applies:
        return ""
    if gate.waived:
        return (
            "; REPLICA IDENTITY check WAIVED (CHECKSUM proof for "
            f"{_bounded([i.table for i in gate.waivable])})"
        )
    if gate.unchecked:
        if gate.probe_state == PROBE_STATE_UNAVAILABLE:
            why = "source password not in memory"
        elif gate.probe_state == PROBE_STATE_FAILED:
            why = (
                "read failed: "
                + str(getattr(gate.last_probe, "error_class", None) or "unknown error")
            )
        else:
            why = f"not found: {_bounded(list(gate.unread))}"
        return f"; re-keyed tables' REPLICA IDENTITY: not re-checked ({why})"
    if gate.resolved:
        return (
            "; re-keyed tables' REPLICA IDENTITY: below FULL earlier on "
            f"{_bounded(list(gate.resolved))}, each cleared by a CHECKSUM match after the fix"
        )
    if gate.probe_state == PROBE_STATE_OK:
        return (
            "; re-keyed tables' REPLICA IDENTITY: FULL at "
            f"{_fmt_hms(getattr(gate.last_probe, 'started_at', None))}"
        )
    return "; re-keyed tables' REPLICA IDENTITY: not re-checked (no read)"


def replica_identity_waiver_log_detail(gate: ReplicaIdentityGate) -> str:
    return (
        "operator chose to cut over without CHECKSUM proof for "
        f"{_bounded([i.table for i in gate.waivable])}, whose REPLICA IDENTITY "
        f"{_below_full_until(gate.waivable)}; the check re-opens if it drops below FULL again"
    )[:480]


def _transition_line(transition: ReplicaIdentityTransition) -> tuple[str, str, str]:
    """``(status name, action, detail)`` for one transition. Value-free."""
    finding = transition.finding
    table = transition.table
    if transition.kind in ("opened", "reopened"):
        key = ", ".join(getattr(finding, "key_columns", None) or [])
        missing = ", ".join(getattr(finding, "missing", None) or []) or "a column"
        return (
            "FAILURE",
            "re-keyed table's REPLICA IDENTITY is below FULL: its deletes can be lost",
            f"{identity_label(finding)}; the composite key ({key}) adds {missing}, which a "
            "DELETE carries only under REPLICA IDENTITY FULL, so deletes on "
            f"{table} can miss Aurora DSQL with no error and no dead letter. Fix: ALTER TABLE "
            f"{table} REPLICA IDENTITY FULL on the source (each partition too). Cut over is "
            "held.",
        )
    restored = _aware(getattr(finding, "restored_at", None))
    after = _fmt_hms(restored + timedelta(seconds=60)) if restored is not None else "the fix"
    if transition.kind == "not-rekeyed":
        return (
            "WARNING",
            "table is no longer re-keyed by CDC: REPLICA IDENTITY finding closed",
            "REPLICA IDENTITY was below FULL from at least "
            f"{_fmt_hms(getattr(finding, 'first_weak_at', None))}; deletes made before "
            f"{_fmt_hms(restored)} may be missing on Aurora DSQL. Cut over waits for a "
            f"CHECKSUM match started after {after}.",
        )
    return (
        "WARNING",
        "re-keyed table's REPLICA IDENTITY is FULL again",
        f"below FULL from at least {_fmt_hms(getattr(finding, 'first_weak_at', None))} to "
        f"{_fmt_hms(restored)}; deletes in that window may be missing on Aurora DSQL. Cut "
        f"over waits for a CHECKSUM match started after {after}.",
    )


def log_replica_identity_transitions(
    transitions: "Sequence[ReplicaIdentityTransition]",
) -> None:
    """One durable activity line per transition (never per poll). Best-effort."""
    if not transitions:
        return
    try:
        from dsql_migrator.core.activity_log import (
            ActivityCategory,
            ActivityStatus,
            log_activity,
        )
    except Exception:  # pragma: no cover - logging must never break the poll
        return
    for transition in transitions:
        try:
            status, action, detail = _transition_line(transition)
            log_activity(
                ActivityCategory.CDC,
                action,
                status=getattr(ActivityStatus, status),
                target=transition.table,
                detail=detail[:600],
            )
        except Exception:  # noqa: BLE001 - one bad line must not drop the others
            continue


__all__ = [
    "CDC_REPLICA_IDENTITY_PROBE_TTL_SECONDS",
    "CdcReplicaIdentityProbe",
    "ReplicaIdentityGate",
    "ReplicaIdentityIssue",
    "ReplicaIdentityTransition",
    "cdc_checksum_forced_tables",
    "cdc_replica_identity_cutover_gate",
    "fold_replica_identity_probe",
    "log_replica_identity_transitions",
    "probe_replica_identity_now",
    "replica_identity_gate_for",
    "replica_identity_probe_due",
    "replica_identity_probe_possible",
    "replica_identity_proven_tables",
    "replica_identity_read_scope",
    "replica_identity_scope",
    "schedule_replica_identity_probe",
    "start_replica_identity_probe",
]
