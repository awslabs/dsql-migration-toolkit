# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The runtime source-schema re-check for captured tables while CDC runs, and its cut-over gate.

CDC replicates row data, not DDL. Most source changes the target cannot take are rejected
and dead-lettered (the dead-letter banner and the G1 gate see them), but several are applied
with no rejected row at all: a dropped nullable or DEFAULT column, a widened numeric or a
MySQL ``tinyint(1)`` widened to ``int`` (rounded / collapsed to true on Aurora DSQL), a
dropped or renamed table (the target freezes, or a same-name table streams into it), a
PostgreSQL column Debezium cannot convert or a generated column (never sent), a NULL-rule or
primary-key change. Nothing reports them. This module keeps checking, like the REPLICA
IDENTITY re-check (``ui/cdc_replica_identity_gate.py``) it mirrors:

* **The baseline** is what the migration was built from: the Step 1 inventory's definitions
  of the CAPTURED tables (the same chain Start CDC uses), plus the columns the operator added
  through Fix target schema, minus the columns CDC does not capture (exclusions, PostgreSQL
  STORED generated columns). It comes from a builder-registered handle; without it (no
  inventory: Step 1 is running or failed; no captured set yet) the check "cannot check" --
  never clean, and it never closes a finding.
* **The read.** ONE bounded catalog statement for all captured tables
  (``core.cdc_catalog_drift.read_live_catalog``) through the read-only source engine, at most
  every :data:`CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS` per session, single flight, never on the
  event loop. It needs the source password, kept in memory only. Only findings and per-table
  digests are kept, never the live catalog.
* **The finding** (``CdcCatalogDriftFinding``, persisted, cleared only by Start over): opened
  when a read shows a difference; resolved only by a SUCCESSFUL read that shows the live
  source equal to the CURRENT baseline (Step 1 re-run, a column added through Fix, or the
  source reverted) -- never on a timer. A Step 1 re-run that changes a matching table's
  definition records a resolved "definition-change" finding (values CDC applied before may
  differ); a table that left the inventory is closed as "not-in-inventory". One activity
  line per transition KIND per read, never per table or per poll.
* **The gate.** An open finding holds Cut over outright: no CHECKSUM releases it, because a
  CHECKSUM renders from the Step 1 inventory and can false-match a stale one (a widened
  ``tinyint(1)`` still compares as true/false). Once resolved, the table must match in a
  value-verified CHECKSUM that started more than a minute after ``resolved_at`` (G1's per-
  table rule). A table Validation cannot prove (left out, gone from the inventory, inside an
  accepted gap) can be waived, recorded. Cut over waits for one fresh read after the latest
  validation and after any baseline change. Fail-OPEN when the source cannot be checked (no
  password, no baseline, a failed read) and nothing is open: Cut over warns and the
  acknowledgement log says so. An open finding still holds.

Inert for Full load only and before CDC ran (unless a finding was recorded): nothing is read,
drawn or logged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Mapping, Optional, Sequence

from dsql_migrator.core.cdc_catalog_drift import (
    KIND_ADD_COLUMN,
    KIND_DEFINITION_CHANGE,
    KIND_DROP_COLUMN,
    KIND_KEY_CHANGE,
    KIND_NULLABILITY_CHANGE,
    KIND_ORDER,
    KIND_RENAME_COLUMN,
    KIND_SOURCE_TABLE_MISSING,
    KIND_TYPE_CHANGE,
    baseline_digest,
    diff_table,
    split_qualified,
)
from dsql_migrator.ui.cdc_replica_identity_gate import (
    PROBE_NO_SOURCE,
    PROBE_READ_FAILED,
    PROBE_STATE_FAILED,
    PROBE_STATE_OK,
    PROBE_STATE_PENDING,
    PROBE_STATE_UNAVAILABLE,
    _NO_PASSWORD_SENTENCE,
    _RI_REASON_TEXT,
    _aware,
    _bounded,
    _failed_read_sentence,
    _fmt_hm,
    _fmt_hms,
    _is_postgres,
    _is_type,
    _now,
    cdc_evidence,
    has_source_connection,
)

# At most one source read per this many seconds per session (single flight across tabs); the
# claim's watchdog is twice this, so a hung read never pins "re-reading".
CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS = 300

# 2+ captured tables and the read found NONE: the connection reads another database or user
# (a reconnect), not N dropped tables -- one read problem, no findings.
PROBE_NONE_VISIBLE = "none-visible"
PROBE_STATE_NO_BASELINE = "no-baseline"

REASON_DRIFTED = "drifted"

RESOLVED_MATCHED = "matched"
RESOLVED_REBASELINED = "rebaselined"
RESOLVED_NOT_IN_INVENTORY = "not-in-inventory"

_NAME_LIMIT = 6
_FACT_LIMIT = 3


@dataclass(frozen=True)
class CatalogBaseline:
    """What the migration was built from, as the builder-registered handle reports it.

    ``inventory`` is the Step 1 ``SourceInventory``; ``captured`` the inventory tables CDC
    captures (watermark, else selection, else the deployed TableIncludeList -- Start CDC's
    own chain); ``capture_names`` the raw names that chain matched (a captured table that has
    since left the inventory is still named there).
    """

    inventory: Any
    captured: tuple = ()
    capture_names: frozenset = frozenset()
    source_type: Any = None


@dataclass(frozen=True)
class CatalogDriftRequest:
    """One read's input, built on the loop from references only (no copy of the inventory).

    ``inventory`` is held so the token's ``id()`` stays unique while a probe refers to it.
    ``epoch`` is the migration the read was claimed for (Start over replaces it), so a read
    that lands after Start over is dropped."""

    tables: tuple = ()
    skip: Mapping[str, frozenset] = field(default_factory=dict)
    target_added: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    source_type: Any = None
    token: tuple = ()
    not_in_baseline: tuple = ()
    inventory: Any = field(default=None, compare=False, repr=False)
    epoch: Any = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class CdcCatalogDriftProbe:
    """One runtime read of the captured tables' source definitions, diffed (value-free).

    ``scope`` is the tables compared, ``drift`` the ones that differ (``CatalogDiff``),
    ``digests`` every compared table's baseline digest, ``not_in_baseline`` tables that are no
    longer in the Step 1 inventory. A failed read has ``ok`` False, a ``problem`` and only the
    exception's CLASS name (never its message, which can carry a host or a value).
    """

    started_at: datetime
    finished_at: datetime
    ok: bool
    problem: Optional[str] = None
    error_class: Optional[str] = None
    scope: tuple = ()
    not_in_baseline: tuple = ()
    drift: Mapping[str, Any] = field(default_factory=dict)
    digests: Mapping[str, str] = field(default_factory=dict)
    token: tuple = ()
    found: int = 0
    inventory: Any = field(default=None, compare=False, repr=False)


@dataclass(frozen=True)
class CatalogDriftTransition:
    """A finding changed state: ``opened`` / ``reopened`` / ``resolved`` / ``rebaselined`` /
    ``not-in-inventory``."""

    kind: str
    table: str
    finding: object


# ---------------------------------------------------------------------------
# Baseline, scope, possibility, due-ness (pure; the provider call is CPU only)
# ---------------------------------------------------------------------------


def _findings(migration_state: object) -> "dict[str, Any]":
    return dict(getattr(migration_state, "cdc_catalog_drift_findings", None) or {})


def _open(finding: object) -> bool:
    return getattr(finding, "resolved_at", None) is None


def handle_registered(migration_state: object) -> bool:
    """The builder registered the baseline provider (it is, in the real app)."""
    return callable(getattr(migration_state, "cdc_catalog_baseline", None))


def _raw_baseline(migration_state: object) -> Optional[CatalogBaseline]:
    handle = getattr(migration_state, "cdc_catalog_baseline", None)
    if not callable(handle):
        return None
    try:
        return handle()
    except Exception:  # noqa: BLE001 - no baseline is "cannot check", never clean
        return None


def _usable(baseline: Optional[object]) -> bool:
    # An inventory is what the comparison needs. An EMPTY captured set is still usable: an
    # open finding whose table left the inventory (the only captured table, dropped on
    # purpose, then Step 1 re-run) must still be closed; with nothing open and nothing
    # captured the scope is empty, which the gate reports as "cannot check".
    return baseline is not None and bool(
        getattr(getattr(baseline, "inventory", None), "tables", None)
    )


def catalog_drift_baseline(migration_state: object) -> Optional[CatalogBaseline]:
    """The baseline to compare with, or ``None`` -- "cannot check": no handle, or no Step 1
    inventory (running, or its last run failed). No captured table set yet leaves the
    scope empty (also "cannot check" for the gate)."""
    baseline = _raw_baseline(migration_state)
    return baseline if _usable(baseline) else None


def _inventory_names(baseline: Optional[object]) -> "Optional[set[str]]":
    tables = getattr(getattr(baseline, "inventory", None), "tables", None)
    if not tables:
        return None
    return {t.name for t in tables}


def _added_sig(migration_state: object) -> tuple:
    added = getattr(migration_state, "cdc_target_added_columns", None) or {}
    return tuple(
        sorted((str(t), tuple(sorted(dict(cols or {}).items()))) for t, cols in added.items())
    )


def catalog_drift_token(migration_state: object, baseline: Optional[object]) -> tuple:
    """What the comparison depends on besides the source: the inventory object (a Step 1
    re-run replaces it) and the Fix-added columns. A change makes a read due at once (and
    Cut over wait for it); safety never rests on it."""
    inventory = getattr(baseline, "inventory", None)
    return (id(inventory) if inventory is not None else None, _added_sig(migration_state))


def _scope_parts(
    migration_state: object, baseline: Optional[object]
) -> "tuple[dict[str, Any], tuple[str, ...]]":
    """``({table: TableDef} to read, tables no longer in the inventory)``. Qualified only.

    The captured tables, plus every OPEN finding's table still in the inventory (so it can be
    seen matching again). Gone from the inventory: an open finding's table, or a captured
    name the check compared before.
    """
    if baseline is None:
        return {}, ()
    findings = _findings(migration_state)
    open_tables = {t for t, f in findings.items() if _open(f)}
    by_name = {t.name: t for t in (getattr(baseline.inventory, "tables", None) or ())}
    tables = {
        t.name: t for t in (baseline.captured or ()) if split_qualified(t.name) is not None
    }
    for name in sorted(open_tables):
        if name in by_name and split_qualified(name) is not None:
            tables.setdefault(name, by_name[name])
    digests = getattr(migration_state, "cdc_catalog_drift_digests", None) or {}
    gone = {t for t in open_tables if t not in by_name} | (
        {str(n) for n in (baseline.capture_names or ()) if str(n) in digests} - set(by_name)
    )
    return tables, tuple(sorted(g for g in gone if split_qualified(g) is not None))


def catalog_drift_scope(
    migration_state: object, baseline: Optional[object] = None
) -> tuple[str, ...]:
    """The tables the re-check is ABOUT (captured, plus open findings). No I/O."""
    if baseline is None:
        baseline = catalog_drift_baseline(migration_state)
    tables, gone = _scope_parts(migration_state, baseline)
    return tuple(sorted(set(tables) | set(gone)))


def _possible(migration_state: object, session: object, baseline: Optional[object]) -> bool:
    if migration_state is None or baseline is None or not has_source_connection(session):
        return False
    if not (cdc_evidence(migration_state) or any(map(_open, _findings(migration_state).values()))):
        return False
    tables, gone = _scope_parts(migration_state, baseline)
    return bool(tables or gone)


def catalog_drift_probe_possible(migration_state: object, session: object) -> bool:
    """Whether the runtime read can run now: a baseline, CDC evidence (or an open finding),
    something to read and a source connection with its in-memory password."""
    if migration_state is None:
        return False
    return _possible(migration_state, session, catalog_drift_baseline(migration_state))


def _covered(last: object) -> set:
    return set(getattr(last, "scope", None) or ()) | set(
        getattr(last, "not_in_baseline", None) or ()
    )


def _pending_reason(
    last: Optional[object],
    scope: "Sequence[str]",
    token: Optional[tuple],
    evidence_at: Optional[datetime],
) -> bool:
    """Due for a reason Cut over waits on (never just the TTL): no read yet, a table the last
    read did not cover, a baseline change since it, or a read older than the latest
    validation."""
    started = _aware(getattr(last, "started_at", None)) if last is not None else None
    if started is None:
        return True
    if not set(scope or ()) <= _covered(last):
        return True
    if token is not None and tuple(getattr(last, "token", None) or ()) != tuple(token):
        return True
    evidence = _aware(evidence_at)
    return evidence is not None and started <= evidence


def catalog_drift_probe_due(
    last: Optional[object],
    scope: "Sequence[str]",
    token: Optional[tuple],
    evidence_at: Optional[datetime],
    now: datetime,
) -> bool:
    """Whether a fresh read is needed. Pure. :func:`_pending_reason`, or older than the TTL.
    A failed read is NOT retried before the TTL (no tight loop) unless the baseline changed
    -- "Check again" forces one."""
    if _pending_reason(last, scope, token, evidence_at):
        return True
    started = _aware(getattr(last, "started_at", None))
    return (_aware(now) - started).total_seconds() > CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS  # type: ignore[operator]


def catalog_drift_request(
    migration_state: object, baseline: CatalogBaseline
) -> CatalogDriftRequest:
    """Build one read's input ON THE LOOP (once per read, never per render): the tables, the
    concrete columns CDC does not capture (effective exclusions + PostgreSQL STORED generated
    columns, exactly ``CdcCaptureExclusions.structured``) and the Fix-added columns."""
    from dsql_migrator.core.converter import pg_preserved_generated_columns
    from dsql_migrator.ui.data_migration._models import effective_lob_exclusions

    tables, gone = _scope_parts(migration_state, baseline)
    try:
        effective = dict(effective_lob_exclusions(migration_state, baseline.inventory))
    except Exception:  # noqa: BLE001 - fall back to the stored set (never fewer skips)
        stored = getattr(migration_state, "lob_exclusions", None)
        try:
            effective = dict(stored()) if callable(stored) else {}
        except Exception:  # noqa: BLE001
            effective = {}
    skip: dict[str, frozenset] = {}
    for name, table in tables.items():
        names = {str(c) for c in (effective.get(name) or ())}
        names.update(c.name for c in pg_preserved_generated_columns(table))
        if names:
            skip[name] = frozenset(names)
    added_all = getattr(migration_state, "cdc_target_added_columns", None) or {}
    added = {
        str(t): {str(c): str(v) for c, v in dict(cols or {}).items()}
        for t, cols in added_all.items()
        if t in tables
    }
    return CatalogDriftRequest(
        tables=tuple(tables[n] for n in sorted(tables)),
        skip=skip,
        target_added=added,
        source_type=baseline.source_type,
        token=catalog_drift_token(migration_state, baseline),
        not_in_baseline=gone,
        inventory=baseline.inventory,
        # Taken right after the claim, on the loop (``_claim_and_build``): no Start over can
        # slip between the claim and this tag.
        epoch=getattr(migration_state, "_cdc_catalog_drift_epoch", None),
    )


def build_catalog_drift_probe(
    request: CatalogDriftRequest,
    live: "Mapping[str, Any]",
    *,
    started: datetime,
    finished: datetime,
) -> CdcCatalogDriftProbe:
    """Diff a successful read against the request's baseline (pure; on the worker).

    Two or more tables and none found is ONE read problem (``none-visible``: a reconnect to
    another database or user), never that many findings."""
    scope = tuple(sorted(t.name for t in request.tables))
    found = sum(1 for name in scope if name in live)
    common = dict(
        started_at=started, finished_at=finished, scope=scope,
        not_in_baseline=tuple(request.not_in_baseline), token=tuple(request.token),
        found=found, inventory=request.inventory,
    )
    if len(scope) >= 2 and found == 0:
        return CdcCatalogDriftProbe(ok=False, problem=PROBE_NONE_VISIBLE, **common)
    drift: dict[str, Any] = {}
    digests: dict[str, str] = {}
    for table in request.tables:
        skip = request.skip.get(table.name, frozenset())
        digests[table.name] = baseline_digest(
            table, skip=skip, source_type=request.source_type
        )
        diff = diff_table(
            table,
            live.get(table.name),
            skip=skip,
            target_added=request.target_added.get(table.name),
            source_type=request.source_type,
        )
        if diff is not None:
            drift[table.name] = diff
    return CdcCatalogDriftProbe(ok=True, drift=drift, digests=digests, **common)


# ---------------------------------------------------------------------------
# The fold (pure)
# ---------------------------------------------------------------------------


def fold_catalog_drift_probe(
    findings: "Mapping[str, Any]",
    digests: "Mapping[str, str]",
    probe: Optional[object],
) -> "tuple[dict[str, Any], dict[str, str], list[CatalogDriftTransition]]":
    """Fold one read into the findings and digests. Pure; ``(findings, digests, transitions)``.

    A failed read changes nothing. Per compared table: a difference opens a finding (or
    re-opens a resolved one, with a new start time) or silently refreshes an open one's
    facts; a match resolves an open finding ("matched"); a match whose BASELINE digest
    changed since the last read (a Step 1 re-run redefined it) resolves it as "rebaselined"
    -- or records a resolved "definition-change" finding when there was none. Per table no
    longer in the inventory: an open finding closes "not-in-inventory", and one the check
    compared before gets a resolved "source-table-missing" finding. The first read seeds the
    digests silently; digests of tables no longer compared are dropped.
    """
    from dsql_migrator.core.models import CdcCatalogDriftFinding

    current = dict(findings or {})
    seen = dict(digests or {})
    if probe is None or not getattr(probe, "ok", False):
        return current, seen, []
    started = _aware(getattr(probe, "started_at", None)) or _now()
    finished = _aware(getattr(probe, "finished_at", None)) or started
    drift = getattr(probe, "drift", None) or {}
    new_digests = getattr(probe, "digests", None) or {}
    scope = tuple(getattr(probe, "scope", None) or ())
    transitions: list[CatalogDriftTransition] = []
    for table in sorted(scope):
        old, new = seen.get(table), new_digests.get(table)
        rebaselined = old is not None and new is not None and old != new
        existing = current.get(table)
        diff = drift.get(table)
        if diff is not None:
            facts = {
                "kinds": list(diff.kinds),
                "changes": list(diff.changes),
                "more_changes": int(diff.more_changes),
                "fixable": bool(diff.fixable),
            }
            if existing is None or not _open(existing):
                finding = CdcCatalogDriftFinding(first_seen_at=started, **facts)
                current[table] = finding
                transitions.append(
                    CatalogDriftTransition(
                        kind="opened" if existing is None else "reopened",
                        table=table, finding=finding,
                    )
                )
            else:
                changed = {k: v for k, v in facts.items() if getattr(existing, k) != v}
                if changed:
                    current[table] = existing.model_copy(update=changed)
        elif existing is not None and _open(existing):
            finding = existing.model_copy(
                update={"resolved_at": finished, "resolved_by": RESOLVED_MATCHED}
            )
            current[table] = finding
            transitions.append(CatalogDriftTransition("resolved", table, finding))
        elif rebaselined:
            if existing is not None:
                finding = existing.model_copy(
                    update={"resolved_at": finished, "resolved_by": RESOLVED_REBASELINED}
                )
            else:
                finding = CdcCatalogDriftFinding(
                    kinds=[KIND_DEFINITION_CHANGE], first_seen_at=finished,
                    resolved_at=finished, resolved_by=RESOLVED_REBASELINED,
                )
            current[table] = finding
            transitions.append(CatalogDriftTransition("rebaselined", table, finding))
        if new is not None:
            seen[table] = new
    for table in sorted(getattr(probe, "not_in_baseline", None) or ()):
        existing = current.get(table)
        if existing is not None and _open(existing):
            finding = existing.model_copy(
                update={"resolved_at": finished, "resolved_by": RESOLVED_NOT_IN_INVENTORY}
            )
        elif existing is None and table in seen:
            finding = CdcCatalogDriftFinding(
                kinds=[KIND_SOURCE_TABLE_MISSING], first_seen_at=finished,
                resolved_at=finished, resolved_by=RESOLVED_NOT_IN_INVENTORY,
            )
        else:
            continue
        current[table] = finding
        transitions.append(CatalogDriftTransition("not-in-inventory", table, finding))
    seen = {t: d for t, d in seen.items() if t in scope}
    return current, seen, transitions


# ---------------------------------------------------------------------------
# The read (never on render; bounded; single flight)
# ---------------------------------------------------------------------------


async def _probe_and_land(session: object, migration_state: object, request: object) -> None:
    """Read on a worker thread, land on the loop, log the transitions; owns the claim.

    A read that started before Start over is dropped when it lands (the request's ``epoch``
    no longer names the migration), and its claim is not released over a newer one."""
    epoch = getattr(request, "epoch", None)
    try:
        from nicegui import run

        from dsql_migrator.ui.data_migration._cdc_status import read_cdc_catalog_drift_once

        probe = await run.io_bound(read_cdc_catalog_drift_once, session, request)
        if probe is not None:  # None only when the app is stopping
            transitions = migration_state.apply_cdc_catalog_drift_probe(probe, epoch)  # type: ignore[attr-defined]
            log_catalog_drift_transitions(transitions)
    finally:
        migration_state.finish_cdc_catalog_drift_probe(epoch)  # type: ignore[attr-defined]


def _claim_and_build(migration_state: object, baseline: CatalogBaseline) -> Optional[object]:
    """Take the single-flight claim and build the request (released if building fails)."""
    if not migration_state.begin_cdc_catalog_drift_probe():  # type: ignore[attr-defined]
        return None
    try:
        return catalog_drift_request(migration_state, baseline)
    except Exception:
        migration_state.finish_cdc_catalog_drift_probe()  # type: ignore[attr-defined]
        raise


def start_catalog_drift_probe(
    session: object, migration_state: object, *, force: bool = False
) -> bool:
    """From the CDC poll: start ONE background read when it is possible and due. Returns
    immediately (the read is spawned, never awaited), so the status landing is never
    delayed; the next poll tick renders the result. Returns whether a read was started."""
    if migration_state is None:
        return False
    baseline = catalog_drift_baseline(migration_state)
    if not _possible(migration_state, session, baseline):
        return False
    last = getattr(migration_state, "cdc_catalog_drift_last_probe", None)
    if not force and not catalog_drift_probe_due(
        last,
        catalog_drift_scope(migration_state, baseline),
        catalog_drift_token(migration_state, baseline),
        None,
        _now(),
    ):
        return False
    try:
        import asyncio

        asyncio.get_running_loop()
    except RuntimeError:
        return False
    request = _claim_and_build(migration_state, baseline)  # type: ignore[arg-type]
    if request is None:
        return False
    from dsql_migrator.ui.data_migration._cdc_status import spawn_detached

    try:
        spawn_detached(lambda: _probe_and_land(session, migration_state, request))
    except Exception:
        migration_state.finish_cdc_catalog_drift_probe()  # type: ignore[attr-defined]
        raise
    return True


async def probe_catalog_drift_now(session: object, migration_state: object) -> bool:
    """"Check again" in the CDC monitor: one forced read, awaited (and still landed if the
    caller is cancelled). Returns whether a read ran."""
    if migration_state is None:
        return False
    baseline = catalog_drift_baseline(migration_state)
    if not _possible(migration_state, session, baseline):
        return False
    request = _claim_and_build(migration_state, baseline)  # type: ignore[arg-type]
    if request is None:
        return False
    from dsql_migrator.ui.data_migration._cdc_status import land_detached

    await land_detached(lambda: _probe_and_land(session, migration_state, request))
    return True


def _view_signature(migration_state: object, evidence_at: Optional[datetime]) -> tuple:
    """What Validation / Cut over draw from the re-check, so a page is rebuilt only when it
    changed (a silent identical re-read every five minutes rebuilds nothing)."""
    last = getattr(migration_state, "cdc_catalog_drift_last_probe", None)
    evidence = _aware(evidence_at)
    findings = tuple(
        sorted(
            (
                t,
                getattr(f, "first_seen_at", None),
                getattr(f, "resolved_at", None),
                getattr(f, "resolved_by", None),
                tuple(getattr(f, "kinds", None) or ()),
                len(getattr(f, "changes", None) or ()),
                getattr(f, "more_changes", 0),
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
            tuple(getattr(last, "scope", None) or ()),
            tuple(sorted(getattr(last, "drift", None) or {})),
            tuple(getattr(last, "token", None) or ()),
            evidence is None or (_aware(last.started_at) or evidence) > evidence,
        )
    )
    return (findings, probe)


# How long a page waits for a read ANOTHER tab (or the CDC poll) is running before it gives
# up without refreshing. The read is bounded by the dialect's connect_timeout plus its 10 s
# statement / socket timeout (8 s server-side on MySQL), so it lands well within this.
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
    while getattr(migration_state, "cdc_catalog_drift_probe_running", False):
        if waited >= _WAIT_FOR_OTHER_READ_SECONDS:
            return
        await asyncio.sleep(0.5)
        waited += 0.5
    if always or _view_signature(migration_state, evidence_at) != before:
        try:
            refresh()
        except Exception:  # noqa: BLE001 - the page went away
            pass


def schedule_catalog_drift_probe(
    ui: object,
    session: object,
    migration_state: object,
    refresh: Callable[[], None],
    *,
    evidence_at: Optional[datetime],
    force: bool = False,
) -> None:
    """Validation / Cut over: arm ONE background read when it is due (or ``force``d by Check
    again). The same discipline as the REPLICA IDENTITY re-check's scheduler: the render
    never reads the source, the claim is taken inside a one-shot timer, the read and its
    landing are detached from the timer, ``refresh`` runs only when what the page draws
    changed (always after a forced read, or when the page was drawn waiting on
    "Re-reading"), and a read another tab holds is waited for (bounded)."""
    if migration_state is None:
        return
    baseline = catalog_drift_baseline(migration_state)
    if not _possible(migration_state, session, baseline):
        return
    scope = catalog_drift_scope(migration_state, baseline)
    token = catalog_drift_token(migration_state, baseline)
    armed_with = getattr(migration_state, "cdc_catalog_drift_last_probe", None)
    if not force and not catalog_drift_probe_due(armed_with, scope, token, evidence_at, _now()):
        return
    before = _view_signature(migration_state, evidence_at)
    drawn_pending = _pending_reason(armed_with, scope, token, evidence_at)

    async def _check() -> None:
        from dsql_migrator.ui.data_migration._cdc_status import land_detached

        current = getattr(migration_state, "cdc_catalog_drift_last_probe", None)
        baseline_now = catalog_drift_baseline(migration_state)
        if not _possible(migration_state, session, baseline_now):
            return
        if force:
            if current is not armed_with:
                # A newer read already landed since the click: show it (re-enables Check
                # again) rather than read again.
                await _refresh_after_other_read(
                    migration_state, evidence_at, before, refresh, always=True
                )
                return
        elif not catalog_drift_probe_due(
            current,
            catalog_drift_scope(migration_state, baseline_now),
            catalog_drift_token(migration_state, baseline_now),
            evidence_at,
            _now(),
        ):
            await _refresh_after_other_read(
                migration_state, evidence_at, before, refresh, always=False
            )
            return
        request = _claim_and_build(migration_state, baseline_now)  # type: ignore[arg-type]
        if request is None:
            await _refresh_after_other_read(
                migration_state, evidence_at, before, refresh, always=force or drawn_pending
            )
            return

        async def _read_and_land() -> None:
            await _probe_and_land(session, migration_state, request)
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
class CatalogDriftIssue:
    """A finding the gate has not cleared: ``drifted`` (open) or a G1 proof reason."""

    table: str
    finding: object
    reason: str
    compared_from: Optional[datetime] = None


@dataclass(frozen=True)
class CatalogDriftGate:
    """The gate's verdict. ``CatalogDriftGate()`` is the INERT gate."""

    applies: bool = False
    open: tuple = ()
    issues: tuple = ()
    resolved: tuple = ()
    probe_state: Optional[str] = None
    problem: Optional[str] = None
    scope: tuple = ()
    last_probe: Optional[object] = None
    waived: bool = False
    migration_type: Optional[object] = None
    has_source_connection: bool = False
    is_pg: bool = False

    @property
    def must_recheck(self) -> tuple:
        from dsql_migrator.ui.cdc_dlq_gate import MUST_RECHECK_REASONS

        return tuple(i for i in self.issues if i.reason in MUST_RECHECK_REASONS)

    @property
    def waivable(self) -> tuple:
        from dsql_migrator.ui.cdc_dlq_gate import WAIVABLE_REASONS

        return tuple(i for i in self.issues if i.reason in WAIVABLE_REASONS)

    @property
    def pending(self) -> bool:
        return self.probe_state == PROBE_STATE_PENDING

    @property
    def blocks(self) -> bool:
        return self.applies and (
            bool(self.open)
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
            and not self.open
            and not self.must_recheck
            and not self.pending
            and not self.waived
        )

    @property
    def recheckable(self) -> tuple:
        """Tables a CHECKSUM re-check can clear. Open ones never: the stale inventory can
        make a CHECKSUM false-match."""
        return tuple(i.table for i in self.must_recheck)

    @property
    def unchecked(self) -> bool:
        """Released without a usable re-read (fail-open; warned and logged)."""
        return self.probe_state in (
            PROBE_STATE_UNAVAILABLE, PROBE_STATE_NO_BASELINE, PROBE_STATE_FAILED
        )


def catalog_drift_waiver_covers(
    waiver: Optional[object], waivable: "Sequence[CatalogDriftIssue]"
) -> bool:
    """A recorded waiver covers only when EVERY waivable table maps to its CURRENT
    ``resolved_at`` -- a new change (a new finding, a new resolve) re-opens the check."""
    if waiver is None or not waivable:
        return False
    tables = dict(getattr(waiver, "tables", None) or {})
    for issue in waivable:
        resolved = _aware(getattr(issue.finding, "resolved_at", None))
        waived_for = _aware(tables.get(issue.table))
        if resolved is None or waived_for is None or waived_for != resolved:
            return False
    return True


def _gone(table: str, finding: object, inventory_names: "Optional[set[str]]") -> bool:
    """Closed because the table left the Step 1 inventory, and still out of it (or the
    inventory is unavailable): Validation cannot compare it."""
    return getattr(finding, "resolved_by", None) == RESOLVED_NOT_IN_INVENTORY and (
        inventory_names is None or table not in inventory_names
    )


def cdc_catalog_drift_cutover_gate(
    *,
    findings: "Mapping[str, Any]",
    scope: "Sequence[str]",
    cdc_in_use: bool,
    cdc_evidence: bool,
    registered: bool,
    baseline_known: bool,
    check_probe: bool,
    report: Optional[object],
    release: str,
    inventory_names: "Optional[set[str]]" = None,
    explained_tables: "Sequence[str]" = (),
    compared_run_start: Optional[datetime] = None,
    compared_overrides: "Optional[Mapping[str, datetime]]" = None,
    evidence_at: Optional[datetime] = None,
    last_probe: Optional[object] = None,
    token: Optional[tuple] = None,
    has_source_connection: bool = False,
    waiver: Optional[object] = None,
    migration_type: Optional[object] = None,
    is_pg: bool = False,
) -> CatalogDriftGate:
    """Evaluate the gate from explicit inputs. Pure (see the module docstring for the rule)."""
    from dsql_migrator.ui.cdc_dlq_gate import WAIVABLE_REASONS, table_proof_reason

    findings = dict(findings or {})
    scope = tuple(sorted(scope or ()))
    applies = bool(findings) or (cdc_in_use and cdc_evidence and registered)
    if not applies:
        return CatalogDriftGate(migration_type=migration_type)
    overrides = {k: _aware(v) for k, v in (compared_overrides or {}).items() if v is not None}
    run_start = _aware(compared_run_start)
    items = {item.table: item for item in (getattr(report, "items", None) or ())}
    explained = set(explained_tables or ())
    opened: list[CatalogDriftIssue] = []
    issues: list[CatalogDriftIssue] = []
    resolved: list[str] = []
    for table in sorted(findings):
        finding = findings[table]
        if _open(finding):
            opened.append(CatalogDriftIssue(table=table, finding=finding, reason=REASON_DRIFTED))
            continue
        started = overrides.get(table) or run_start
        reason = table_proof_reason(
            None if _gone(table, finding, inventory_names) else items.get(table),
            release=release,
            explained=table in explained,
            started=started,
            last_at=_aware(getattr(finding, "resolved_at", None)),
        )
        if reason is None:
            resolved.append(table)
        else:
            issues.append(
                CatalogDriftIssue(table=table, finding=finding, reason=reason, compared_from=started)
            )
    probe_state: Optional[str] = None
    problem: Optional[str] = None
    if check_probe and registered and ((cdc_in_use and cdc_evidence) or opened):
        if not has_source_connection:
            probe_state = PROBE_STATE_UNAVAILABLE
        elif not baseline_known or not scope:
            probe_state = PROBE_STATE_NO_BASELINE
        elif _pending_reason(last_probe, scope, token, evidence_at):
            probe_state = PROBE_STATE_PENDING
        elif not getattr(last_probe, "ok", False):
            probe_state = PROBE_STATE_FAILED
            problem = getattr(last_probe, "problem", None)
        else:
            probe_state = PROBE_STATE_OK
    waivable = [i for i in issues if i.reason in WAIVABLE_REASONS]
    return CatalogDriftGate(
        applies=True,
        open=tuple(opened),
        issues=tuple(issues),
        resolved=tuple(resolved),
        probe_state=probe_state,
        problem=problem,
        scope=scope,
        last_probe=last_probe,
        waived=catalog_drift_waiver_covers(waiver, waivable),
        migration_type=migration_type,
        has_source_connection=has_source_connection,
        is_pg=is_pg,
    )


def catalog_drift_gate_for(
    migration_state: object,
    validation_state: object,
    session: object,
    *,
    cdc_in_use: bool,
    release: str,
    summary: Optional[object],
    check_probe: bool = True,
) -> CatalogDriftGate:
    """The gate for this session's state. No I/O; ``migration_state`` None -> inert."""
    if migration_state is None:
        return CatalogDriftGate()
    from dsql_migrator.ui.cdc_dlq_gate import validation_evidence_at

    try:
        migration_type = getattr(migration_state, "migration_type", None)
    except Exception:  # noqa: BLE001 - copy only
        migration_type = None
    registered = handle_registered(migration_state)
    raw = _raw_baseline(migration_state) if registered else None
    baseline = raw if _usable(raw) else None
    findings = _findings(migration_state)
    scope = (
        catalog_drift_scope(migration_state, baseline)
        if baseline is not None
        else tuple(sorted(t for t, f in findings.items() if _open(f)))
    )
    return cdc_catalog_drift_cutover_gate(
        findings=findings,
        scope=scope,
        cdc_in_use=cdc_in_use,
        cdc_evidence=cdc_evidence(migration_state),
        registered=registered,
        baseline_known=baseline is not None,
        check_probe=check_probe,
        report=getattr(validation_state, "result", None),
        release=release,
        inventory_names=_inventory_names(raw),
        explained_tables=tuple(getattr(summary, "quarantine_explained_tables", ()) or ()),
        compared_run_start=getattr(validation_state, "compared_run_start", None),
        compared_overrides=getattr(validation_state, "compared_overrides", None) or {},
        evidence_at=validation_evidence_at(validation_state),
        last_probe=getattr(migration_state, "cdc_catalog_drift_last_probe", None),
        token=catalog_drift_token(migration_state, baseline) if baseline is not None else None,
        has_source_connection=has_source_connection(session),
        waiver=getattr(validation_state, "cutover_catalog_drift_waiver", None),
        migration_type=migration_type,
        is_pg=_is_postgres(session),
    )


def catalog_drift_proven_tables(
    findings: "Mapping[str, Any]",
    validation_state: object,
    inventory_names: "Optional[set[str]]" = None,
) -> "set[str]":
    """Resolved findings a value-verified CHECKSUM started after the resolve (+ margin) has
    already proven -- the CDC monitor stops showing them. A table gone from the inventory is
    never proven. Pure."""
    from dsql_migrator.ui.cdc_dlq_gate import table_proof_reason

    if validation_state is None:
        return set()
    report = getattr(validation_state, "result", None)
    items = {i.table: i for i in (getattr(report, "items", None) or ())}
    run_start = _aware(getattr(validation_state, "compared_run_start", None))
    overrides = getattr(validation_state, "compared_overrides", None) or {}
    proven: set[str] = set()
    for table, finding in dict(findings or {}).items():
        if _open(finding) or _gone(table, finding, inventory_names):
            continue
        started = _aware(overrides.get(table)) or run_start
        if (
            table_proof_reason(
                items.get(table),
                release="released",
                explained=False,
                started=started,
                last_at=_aware(getattr(finding, "resolved_at", None)),
            )
            is None
        ):
            proven.add(table)
    return proven


def catalog_drift_waived_tables(
    findings: "Mapping[str, Any]", waiver: Optional[object]
) -> "set[str]":
    """Resolved findings the operator's Cut over waiver covers at their CURRENT resolve."""
    if waiver is None:
        return set()
    return {
        t
        for t, f in dict(findings or {}).items()
        if not _open(f)
        and catalog_drift_waiver_covers(
            waiver, [CatalogDriftIssue(table=t, finding=f, reason="not-validated")]
        )
    }


def cdc_catalog_drift_forced_tables(migration_state: object) -> "set[str]":
    """Every table with a source-schema finding (Validation compares it by CHECKSUM)."""
    return set(_findings(migration_state))


# ---------------------------------------------------------------------------
# Copy (value-free: table / column names, catalog type text, times, error classes)
# ---------------------------------------------------------------------------

_LABELS = {
    KIND_SOURCE_TABLE_MISSING: "table not found on the source",
    KIND_KEY_CHANGE: "primary key changed at the source",
    KIND_TYPE_CHANGE: "column type changed at the source",
    KIND_DROP_COLUMN: "column dropped at the source",
    KIND_RENAME_COLUMN: "column renamed at the source",
    KIND_NULLABILITY_CHANGE: "NULL rule changed at the source",
    KIND_ADD_COLUMN: "column added at the source",
    KIND_DEFINITION_CHANGE: "definition changed in the Step 1 inventory",
}

_WHY_MISSING_MYSQL = (
    "it was dropped or renamed, or the tool's source user can no longer see it. CDC captures "
    "tables by the names it was deployed with, so nothing more reaches this Aurora DSQL "
    "table, and a table re-created under the same name streams into it as is"
)
_WHY_MISSING_PG = (
    "it was dropped, renamed or moved to another schema. After a rename, changes keep "
    "reaching this Aurora DSQL table only until the connector restarts, and are dropped "
    "after that"
)
_WHYS = {
    KIND_KEY_CHANGE: (
        "change records now carry a different key than this Aurora DSQL table's primary key, "
        "so writes are dead-lettered or miss their rows, and Aurora DSQL cannot change a "
        "primary key in place"
    ),
    KIND_TYPE_CHANGE: (
        "Aurora DSQL keeps the old type: a value it cannot hold is dead-lettered, and one it "
        "can is converted silently, for example rounded to the old scale, or stored as true "
        "for any non-zero value when a MySQL tinyint(1) became a wider integer"
    ),
    KIND_DROP_COLUMN: (
        "CDC no longer sends it, so Aurora DSQL stores NULL or the column DEFAULT in it for "
        "new rows and keeps the old value on updated rows (where it is NOT NULL with no "
        "DEFAULT, those rows are dead-lettered instead)"
    ),
    KIND_RENAME_COLUMN: (
        "CDC now sends the new name, which this Aurora DSQL table does not have, so those "
        "rows are dead-lettered and the old column is no longer filled"
    ),
    KIND_NULLABILITY_CHANGE: (
        "Aurora DSQL keeps the old rule: a NULL it rejects is dead-lettered, and a MySQL "
        "ALTER that filled existing NULLs wrote no change events for them"
    ),
    KIND_DEFINITION_CHANGE: (
        "Step 1 (Evaluation) now reads a different definition than the one CDC ran with, so "
        "values CDC applied before the re-run may not match"
    ),
}
_WHY_ADD_PLAIN = (
    "this Aurora DSQL table does not have it, so CDC rows that carry it are dead-lettered "
    "until it exists"
)
_WHY_ADD_PG_CLAUSE = (
    ", and a column of a type CDC cannot convert (for example interval[] or money[]) never "
    "arrives at all, even once the table has it"
)
# Fix target schema adds every column its plan finds and cannot tell a generated one apart,
# so a table that gained a generated column is never offered it (see diff_table's fixable).
_WHY_ADD_GENERATED = (
    "the source computes it (a generated column) and CDC may not send its values, so Fix "
    "target schema… is not offered for this table"
)


def kind_labels(finding: object) -> list[str]:
    kinds = set(getattr(finding, "kinds", None) or ())
    return [_LABELS[k] for k in KIND_ORDER if k in kinds]


def kind_whys(finding: object, *, is_pg: bool) -> list[str]:
    kinds = set(getattr(finding, "kinds", None) or ())
    changes = list(getattr(finding, "changes", None) or ())
    whys: list[str] = []
    for kind in KIND_ORDER:
        if kind not in kinds:
            continue
        if kind == KIND_SOURCE_TABLE_MISSING:
            whys.append(_WHY_MISSING_PG if is_pg else _WHY_MISSING_MYSQL)
        elif kind == KIND_ADD_COLUMN:
            adds = [c for c in changes if getattr(c, "kind", "") == KIND_ADD_COLUMN]
            # A kept change list can be capped: with no added column among the kept ones,
            # fall back to the plain reason.
            plain = any(not c.generated for c in adds) or not adds
            if plain:
                whys.append(_WHY_ADD_PLAIN + (_WHY_ADD_PG_CLAUSE if is_pg else ""))
            if any(c.generated for c in adds):
                whys.append(_WHY_ADD_GENERATED)
        else:
            whys.append(_WHYS[kind])
    return whys


def change_fact(change: object) -> str:
    """One change in words (names and catalog types only)."""
    kind = getattr(change, "kind", "")
    column = getattr(change, "column", None) or ""
    before = getattr(change, "before", None) or ""
    after = getattr(change, "after", None) or ""
    if kind == KIND_ADD_COLUMN:
        return f"added {column} (generated)" if change.generated else f"added {column} {after}".rstrip()  # type: ignore[attr-defined]
    if kind == KIND_DROP_COLUMN:
        return f"dropped {column}"
    if kind == KIND_RENAME_COLUMN:
        return f"{column} renamed to {after}"
    if kind == KIND_TYPE_CHANGE:
        return f"{column} was {before}, now {after}"
    if kind == KIND_NULLABILITY_CHANGE:
        return f"{column} was {before}, now {after}"
    if kind == KIND_KEY_CHANGE:
        # A dropped key (DROP PRIMARY KEY) or one added to a table that had none: "none",
        # never an empty "()".
        def _key(columns: str) -> str:
            return f"({columns})" if columns else "none"

        return f"primary key was {_key(before)}, now {_key(after)}"
    return kind


def _facts(finding: object) -> str:
    changes = list(getattr(finding, "changes", None) or ())
    shown = [change_fact(c) for c in changes[:_FACT_LIMIT]]
    more = max(len(changes) - _FACT_LIMIT, 0) + int(getattr(finding, "more_changes", 0) or 0)
    if more:
        shown.append(f"+{more} more change{'s' if more != 1 else ''}")
    return "; ".join(shown)


def finding_row(table: str, finding: object, *, is_pg: bool) -> str:
    """``{table}: {labels} ({facts}) — {whys}.`` for the CDC monitor."""
    facts = _facts(finding)
    labels = ", ".join(kind_labels(finding))
    whys = "; ".join(kind_whys(finding, is_pg=is_pg))
    return f"{table}: {labels}" + (f" ({facts})" if facts else "") + f" — {whys}."


def _labelled(issues: "Sequence[CatalogDriftIssue]") -> str:
    shown = ", ".join(
        f"{i.table} ({', '.join(kind_labels(i.finding))})" for i in issues[:_NAME_LIMIT]
    )
    more = len(issues) - _NAME_LIMIT
    return shown + (f" (+{more} more)" if more > 0 else "")


def _is_cdc_only(migration_type: object) -> bool:
    return _is_type(migration_type, "CDC_ONLY")


def monitor_runbook(migration_type: object) -> str:
    if _is_cdc_only(migration_type):
        return (
            "To realign a table once the source has its final definition: change the Aurora "
            "DSQL table by hand where it must follow the source (CDC only has no Full Load "
            "step to reload from) and correct the rows CDC wrote since the change — they are "
            "not replayed — then re-run Step 1 (Evaluation) so this check compares with the "
            "new definition. Or undo the change on the source."
        )
    return (
        "To realign a table once the source has its final definition: re-run Step 1 "
        "(Evaluation) — this check then compares with it — and where the Aurora DSQL table "
        "must change too, let CDC catch up (Stream lag: caught up), Stop CDC, reload the "
        "table on the Full Load step with 'Drop & reload', then Start CDC. Drop a removed or "
        "renamed column on Aurora DSQL first: Drop & reload does not destroy a target column "
        "the new definition no longer has. Run no other DDL on migrated tables while CDC is "
        "stopped. Or undo the change on the source."
    )


def repair_line(migration_type: object) -> str:
    """How to repair a table whose CHECKSUM then differs, by migration type."""
    if _is_cdc_only(migration_type):
        return (
            "If a table then differs, correct its rows on Aurora DSQL by hand (CDC only has "
            "no Full Load step to reload from), then re-check it."
        )
    return (
        "If a table then differs, let CDC catch up (Stream lag: caught up), Stop CDC, reload "
        "it on the Full Load step with 'Drop & reload', then Start CDC and re-check it."
    )


MONITOR_INTRO = (
    "The tool re-reads the definitions of the captured tables on the source every 5 minutes "
    "while this page is open, and compares them with the Step 1 (Evaluation) inventory this "
    "migration was built from. CDC does not replicate DDL, and a change like these can leave "
    "wrong values on Aurora DSQL with no error and no dead letter:"
)

MONITOR_HOLD_SENTENCE = (
    "Cut over is held until the source matches the Step 1 inventory again and each table "
    "then matches in a CHECKSUM comparison started after that; Validation compares these "
    "tables by CHECKSUM automatically. Choose Check again after a change."
)

_NO_BASELINE_SENTENCE = (
    " The re-check compares with the Step 1 (Evaluation) inventory, and there is none right "
    "now (Step 1 is running or its last run failed, or the captured table set is not known "
    "yet): it resumes when both are known."
)


def _none_visible_sentence(last_probe: Optional[object]) -> str:
    return (
        f" The last re-read at {_fmt_hms(getattr(last_probe, 'started_at', None))} found none "
        "of the captured tables on the source: if you reconnected with a different database "
        "or user, reconnect with the one this migration reads from, then choose Check again."
    )


def _problem_sentence(
    *, has_password: bool, baseline_known: bool, last_probe: Optional[object]
) -> str:
    if not has_password:
        return _NO_PASSWORD_SENTENCE
    if not baseline_known:
        return _NO_BASELINE_SENTENCE
    if (
        last_probe is not None
        and not getattr(last_probe, "ok", True)
        and getattr(last_probe, "problem", None) == PROBE_NONE_VISIBLE
    ):
        return _none_visible_sentence(last_probe)
    return _failed_read_sentence(last_probe)


@dataclass(frozen=True)
class MonitorOpenNotice:
    """The CDC monitor's ERROR box for open findings (drawn by ``_cdc_monitoring``)."""

    header: str
    intro: str
    rows: tuple  # ((table, text), ...), at most six
    more_tables: str
    runbook: str
    tail: str


def monitor_open_notice(
    findings: "Mapping[str, Any]",
    *,
    is_pg: bool,
    migration_type: object,
    has_password: bool,
    last_probe: Optional[object],
    baseline_known: bool,
    fix_tables: "set[str] | frozenset[str]" = frozenset(),
) -> Optional[MonitorOpenNotice]:
    """The open findings' notice, or ``None`` with none open. ``fix_tables`` are the rows the
    renderer draws a Fix target schema… button for (the sentence about it needs one)."""
    opened = sorted((t, f) for t, f in dict(findings or {}).items() if _open(f))
    if not opened:
        return None
    rows = tuple((t, finding_row(t, f, is_pg=is_pg)) for t, f in opened[:_NAME_LIMIT])
    more = len(opened) - _NAME_LIMIT
    kinds = {k for _t, f in opened for k in (getattr(f, "kinds", None) or ())}
    sentences: list[str] = []
    if KIND_SOURCE_TABLE_MISSING in kinds:
        sentences.append(
            "A table not found on the source cannot be realigned here: CDC captures a fixed "
            "table list, so a renamed table is not captured under its new name — rename it "
            "back on the source, or use Start over to migrate it under its new name. If you "
            "dropped it on purpose, re-run Step 1 (Evaluation): it then leaves this check, "
            "and Cut over asks you to acknowledge it."
        )
    if KIND_RENAME_COLUMN in kinds:
        sentences.append(
            "Do not use Fix target schema… for a renamed column: it adds an empty column and "
            "leaves the old one."
        )
    if KIND_KEY_CHANGE in kinds:
        if _is_cdc_only(migration_type):
            sentences.append(
                "Aurora DSQL cannot change a primary key in place: restore the original key "
                "on the source, or use Start over to migrate the table with its new key."
            )
        else:
            sentences.append(
                "Aurora DSQL cannot change a primary key in place: restore the original key "
                "on the source, or re-run Step 1 and reload the table with 'Drop & reload' as "
                "above, which recreates it with the new key."
            )
    if set(fix_tables or ()) & {t for t, _f in rows}:
        # Validation's stale-inventory check grades a column the Step 1 inventory lacks as
        # not compared, so the Fix alone never lets a CHECKSUM count.
        sentences.append(
            "For a plain added column, Fix target schema… adds it to Aurora DSQL now; then "
            "re-run Step 1 so Validation can compare it."
        )
    sentences.append(MONITOR_HOLD_SENTENCE)
    tail = " ".join(sentences) + _problem_sentence(
        has_password=has_password, baseline_known=baseline_known, last_probe=last_probe
    )
    return MonitorOpenNotice(
        header=(
            f"Source schema changed on {len(opened)} captured table(s) — CDC does not carry "
            "it to Aurora DSQL"
        ),
        intro=MONITOR_INTRO,
        rows=rows,
        more_tables=f"+{more} more tables" if more > 0 else "",
        runbook=monitor_runbook(migration_type),
        tail=tail,
    )


def _window(finding: object) -> str:
    """When a resolved finding's change was in effect, by how it was resolved."""
    first = _fmt_hm(getattr(finding, "first_seen_at", None))
    resolved = _fmt_hm(getattr(finding, "resolved_at", None))
    by = getattr(finding, "resolved_by", None)
    if by == RESOLVED_REBASELINED:
        return f"changed before {resolved}, found when Step 1 was re-run"
    if by == RESOLVED_NOT_IN_INVENTORY:
        return f"no longer in the Step 1 inventory since {resolved}"
    return f"changed from at least {first} until {resolved}"


def _windows(issues: "Sequence[CatalogDriftIssue]") -> str:
    shown = ", ".join(f"{i.table} ({_window(i.finding)})" for i in issues[:_NAME_LIMIT])
    more = len(issues) - _NAME_LIMIT
    return shown + (f" (+{more} more)" if more > 0 else "")


def _latest_resolve(issues: "Sequence[CatalogDriftIssue]") -> Optional[datetime]:
    stamps = [_aware(getattr(i.finding, "resolved_at", None)) for i in issues]
    stamps = [s for s in stamps if s is not None]
    return max(stamps) if stamps else None


def _threshold(issues: "Sequence[CatalogDriftIssue]") -> Optional[datetime]:
    from dsql_migrator.ui.cdc_dlq_gate import CDC_DLQ_EVIDENCE_MARGIN_SECONDS

    latest = _latest_resolve(issues)
    return (
        latest + timedelta(seconds=CDC_DLQ_EVIDENCE_MARGIN_SECONDS)
        if latest is not None
        else None
    )


def monitor_resolved_notice(
    findings: "Mapping[str, Any]",
    proven: "set[str]",
    migration_type: object,
    waived: "Optional[set[str]]" = None,
    inventory_names: "Optional[set[str]]" = None,
) -> Optional[tuple[str, str]]:
    """``(header, body)`` of the CDC monitor's WARNING for resolved, not-yet-proven tables.

    Only a table Validation can still compare gets the CHECKSUM wait and the repair line; a
    table gone from the Step 1 inventory gets the acknowledgement it needs instead, and a
    waived one the recorded decision."""
    resolved = [
        CatalogDriftIssue(table=t, finding=f, reason="resolved")
        for t, f in sorted(dict(findings or {}).items())
        if not _open(f) and t not in proven
    ]
    if not resolved:
        return None
    waived = set(waived or ())
    skipped = [i.table for i in resolved if i.table in waived]
    waiting = [i for i in resolved if i.table not in waived]
    gone = [i.table for i in waiting if _gone(i.table, i.finding, inventory_names)]
    comparable = [i for i in waiting if i.table not in gone]
    body = f"{_windows(resolved)}."
    if comparable:
        whom = (
            "each"
            if not (skipped or gone)
            else f"each of {_bounded([i.table for i in comparable])}"
        )
        body += (
            " Validation compares these tables by CHECKSUM automatically. Cut over waits "
            f"until {whom} matches in a comparison started after "
            f"{_fmt_hms(_threshold(comparable))}."
        )
    if gone:
        # The window above already says "no longer in the Step 1 inventory since ...".
        one = len(gone) == 1
        body += (
            f" {_bounded(gone)} {'was' if one else 'were'} dropped or renamed at the source, "
            f"so Validation cannot compare {'it' if one else 'them'}: if that was intended, "
            f"acknowledge {'it' if one else 'them'} on the Cut over step."
        )
    if skipped:
        body += (
            " You chose on the Cut over step to continue without this proof for "
            f"{_bounded(skipped)}; that decision is in the activity log."
        )
    if comparable:
        body += f" {repair_line(migration_type)}"
    return (
        "The source schema changed while CDC ran — values written meanwhile may differ on "
        "Aurora DSQL",
        body,
    )


def monitor_unchecked_notice(
    scope: "Sequence[str]", last_probe: Optional[object], *, has_password: bool
) -> Optional[tuple[str, str, str]]:
    """``(tone, header, body)`` when the re-check cannot run with nothing open: ``info``
    without the source password (expected after a restart), ``warning`` when the last read
    failed or found none of the tables. ``None`` otherwise."""
    names = sorted(scope or ())
    if not names:
        return None
    n = len(names)
    header = "Can't re-check the source schema"
    if not has_password:
        return (
            "info",
            header,
            f"While CDC runs, the tool re-reads the definitions of the {n} captured table(s) "
            "on the source every 5 minutes, because a change such as a dropped column, a "
            "changed type or a renamed table can reach Aurora DSQL with no error and no dead "
            "letter. The source password is kept in memory only, so after an app restart the "
            "tool cannot connect: reconnect the source on the Connect step to resume the "
            "check.",
        )
    if last_probe is None or getattr(last_probe, "ok", True):
        return None
    at = _fmt_hms(getattr(last_probe, "started_at", None))
    if getattr(last_probe, "problem", None) == PROBE_NONE_VISIBLE:
        return (
            "warning",
            header,
            f"The last re-read at {at} found none of the {n} captured tables on the source "
            f"({_bounded(names)}). If you reconnected with a different database or user, "
            "reconnect with the one this migration reads from; the tool tries again in 5 "
            "minutes.",
        )
    return (
        "warning",
        header,
        f"The last re-read of the source schema of the {n} captured table(s) at {at} failed "
        f"({getattr(last_probe, 'error_class', None) or 'unknown error'}); the tool tries "
        "again in 5 minutes. Avoid DDL on migrated tables until it works again.",
    )


# --- Cut over ---------------------------------------------------------------------------


def _gate_problem_sentence(gate: CatalogDriftGate) -> str:
    if not gate.has_source_connection:
        return _NO_PASSWORD_SENTENCE
    if gate.probe_state == PROBE_STATE_NO_BASELINE:
        return _NO_BASELINE_SENTENCE
    if gate.probe_state == PROBE_STATE_FAILED:
        if gate.problem == PROBE_NONE_VISIBLE:
            return _none_visible_sentence(gate.last_probe)
        return _failed_read_sentence(gate.last_probe)
    return ""


def cutover_open_notice(gate: CatalogDriftGate) -> Optional[tuple[str, str]]:
    """``(header, body)`` of Cut over's ERROR while a finding is open."""
    if not gate.open:
        return None
    body = (
        f"{_labelled(gate.open)}. CDC keeps writing these tables in the shape this migration "
        "was built from, so their values on Aurora DSQL can be wrong with no error and no "
        "dead letter — and a CHECKSUM can match while the Step 1 inventory still has the old "
        "shape. Realign each table or undo the change on the source (the CDC step lists what "
        "changed and how), then choose Check again. Once the tool reads a match, each table "
        "must match in a CHECKSUM comparison started more than a minute later; Validation "
        "compares them by CHECKSUM automatically."
    )
    return (
        f"Cut over is held: the source schema changed on {len(gate.open)} table(s)",
        body + _gate_problem_sentence(gate),
    )


def cutover_pending_notice(gate: CatalogDriftGate) -> Optional[tuple[str, str]]:
    if not gate.pending:
        return None
    return (
        "Re-reading the source schema",
        "Cut over waits for one fresh read of the definitions of the "
        f"{len(gate.scope)} captured table(s) on the source, so a change since your last "
        "validation or Step 1 run is caught. This takes a few seconds.",
    )


def cutover_must_recheck_notice(gate: CatalogDriftGate) -> Optional[tuple[str, str]]:
    must = gate.must_recheck
    if not must:
        return None
    n = len(must)

    def _line(issue: CatalogDriftIssue) -> str:
        return (
            f"{issue.table}: {_window(issue.finding)} — "
            f"{_RI_REASON_TEXT.get(issue.reason, issue.reason)}"
        )

    shown = "; ".join(_line(i) for i in must[:_NAME_LIMIT]) + (
        f"; +{n - _NAME_LIMIT} more" if n > _NAME_LIMIT else ""
    )
    return (
        f"Re-check {n} schema-changed table(s) with CHECKSUM before cutting over",
        "The source schema of these tables changed while CDC ran, so values CDC applied then "
        f"may be wrong: {shown}. Each table must match in a CHECKSUM comparison started more "
        "than a minute after the source matched the Step 1 inventory again. On the Validation "
        "step, re-check them with CHECKSUM (one button there re-checks every table that needs "
        "it); this check clears by itself once they match. "
        f"{repair_line(gate.migration_type)}",
    )


def cutover_waivable_notice(gate: CatalogDriftGate) -> Optional[tuple[str, str]]:
    """Resolved tables Validation cannot prove: what clears each kind, and the waiver -- only
    offered outright when "Continue without this check" is actually drawn."""
    from dsql_migrator.ui.cdc_dlq_gate import REASON_ACCEPTED_GAP, REASON_NOT_VALIDATED

    waivable = gate.waivable
    if not waivable or gate.waived:
        return None
    reasons = {i.reason for i in waivable}
    remedies: list[str] = []
    if REASON_NOT_VALIDATED in reasons:
        remedies.append(
            "Include a table that still exists on the source on the Validation step and "
            "re-run Validation (it is compared by CHECKSUM automatically); a table no longer "
            "at the source cannot be compared, and CDC does not capture it under any new name."
        )
    if REASON_ACCEPTED_GAP in reasons:
        remedies.append(
            "A table inside the gap you accepted cannot be cleared by a CHECKSUM unless its "
            "source rows are corrected and it is reloaded with 'Drop & reload'."
        )
    if gate.open:
        offer = (
            "Once the tables above match the Step 1 inventory again and are re-checked, you "
            "can also continue without this check"
        )
    elif gate.must_recheck:
        offer = "Once the tables above are re-checked, you can also continue without this check"
    elif gate.pending:
        offer = "Once the re-read above finishes, you can also continue without this check"
    else:
        offer = "Or continue without this check"
    one = len(waivable) == 1
    are = "is" if one else "are"
    return (
        f"{len(waivable)} table(s) whose source schema changed have no CHECKSUM proof",
        f"{_bounded([i.table for i in waivable])} {are} not in the validation report (left "
        "out on the Validation step, or no longer in the Step 1 inventory because "
        f"{'it was' if one else 'they were'} dropped or renamed at the source) or {are} part "
        "of the gap you accepted, so Validation cannot show that "
        f"{'its' if one else 'their'} values are right. "
        + " ".join(remedies)
        + f" {offer}; that decision is recorded in the activity log.",
    )


def cutover_waived_notice(gate: CatalogDriftGate) -> Optional[tuple[str, str]]:
    if not gate.waived:
        return None
    return (
        "Cutting over without CHECKSUM proof for tables whose source schema changed",
        "You chose to continue without CHECKSUM proof for "
        f"{_bounded([i.table for i in gate.waivable])}. That decision is in the activity "
        "log; this step locks again if their source schema changes again.",
    )


def cutover_unchecked_notice(gate: CatalogDriftGate) -> Optional[tuple[str, str]]:
    """The fail-open WARNING above the runbook when the re-read could not run."""
    if not gate.applies or not gate.unchecked:
        return None
    n = len(gate.scope)
    tables = f"the {n} captured table(s)" if n else "the captured tables"
    last = gate.last_probe
    if gate.probe_state == PROBE_STATE_UNAVAILABLE:
        part = (
            f"The tool re-reads the definitions of {tables} on the source before cut over, "
            "because a DDL change can make CDC store wrong values without rejecting a row. "
            "The source password is kept in memory only and this session has none: reconnect "
            "the source on the Connect step to run the check."
        )
    elif gate.probe_state == PROBE_STATE_NO_BASELINE:
        part = (
            "The tool compares the source schema with the Step 1 (Evaluation) inventory, and "
            "there is none right now (Step 1 is running or its last run failed) or the "
            "captured table set is not known yet."
        )
    elif gate.problem == PROBE_NONE_VISIBLE:
        part = (
            f"The re-read at {_fmt_hms(getattr(last, 'started_at', None))} found none of the "
            f"{n} captured tables on the source; if you reconnected with a different database "
            "or user, reconnect with the one this migration reads from, then choose Check "
            "again."
        )
    else:
        part = (
            f"The re-read of the source schema at {_fmt_hms(getattr(last, 'started_at', None))} "
            f"failed ({getattr(last, 'error_class', None) or 'unknown error'}); choose Check "
            "again."
        )
    return (
        "Cut over could not re-read the source schema",
        part
        + " Without this check, confirm that no DDL ran on the migrated tables while CDC ran "
        "before you cut over.",
    )


# --- Validation -------------------------------------------------------------------------


def catalog_drift_verdict_hold(
    gate: Optional[CatalogDriftGate], *, total: int, as_of: str
) -> Optional[tuple[str, str, str]]:
    """``(tone, header, body)`` replacing a green "Ready for cut-over" while the gate holds.
    Led by the most severe case; the body names every other held table, so the Re-check
    button under it never re-checks a table no notice mentions."""
    if gate is None or not gate.applies:
        return None
    must = gate.must_recheck
    unvalidated = (
        () if gate.waived else tuple(i for i in gate.issues if i.reason == "not-validated")
    )
    also_unvalidated = (
        f" The source schema of {_bounded([i.table for i in unvalidated])} also changed while "
        "CDC ran, and this report does not cover them — include them and re-run, or, once the "
        "other tables are cleared, acknowledge them on the Cut over step."
        if unvalidated
        else ""
    )
    if gate.open:
        body = (
            f"All {total} table(s) match as-of {as_of}, but the source schema of "
            f"{_bounded([i.table for i in gate.open])} no longer matches the Step 1 inventory "
            "this comparison used, so the match can be false (a TINYINT(1) widened to INT "
            "still compares as true or false). Realign each table or undo the change on the "
            "source (the CDC step lists what changed); the tool re-reads the source, and Cut "
            "over then needs a CHECKSUM match started after that."
        )
        if not gate.has_source_connection:
            body += _NO_PASSWORD_SENTENCE
        if must:
            body += (
                f" The source schema of {_bounded([i.table for i in must])} also changed "
                f"earlier, until {_fmt_hm(_latest_resolve(must))}, and none of these has a "
                "CHECKSUM match that started more than a minute after that: re-check them "
                "below."
            )
        return (
            "error",
            "Tables match — cut over is held: the source schema changed on "
            f"{len(gate.open)} table(s)",
            body + also_unvalidated,
        )
    if must:
        return (
            "warning",
            f"Tables match — cut over waits for a CHECKSUM re-check of {len(must)} table(s) "
            "whose source schema changed",
            f"All {total} table(s) match as-of {as_of}. But the source schema of "
            f"{_bounded([i.table for i in must])} changed while CDC ran, until "
            f"{_fmt_hm(_latest_resolve(must))}, and none of these tables has a CHECKSUM match "
            "that started more than a minute after that. Re-check them below; if a table then "
            "differs, this report shows how to repair it." + also_unvalidated,
        )
    if unvalidated:
        return (
            "warning",
            f"Tables match — {len(unvalidated)} table(s) whose source schema changed are not "
            "in this report",
            f"All {total} table(s) match as-of {as_of}, but the source schema of "
            f"{_bounded([i.table for i in unvalidated])} changed while CDC ran, and this "
            "report does not cover them. Include them and re-run, or acknowledge them on the "
            "Cut over step.",
        )
    return None


def catalog_drift_options_hint(tables: "Sequence[str]") -> str:
    """The Validation Options hint naming finding tables the next run CHECKSUMs ("" if none)."""
    tables = list(tables)
    if not tables:
        return ""
    return (
        f"The source schema of {len(tables)} table(s) changed while CDC ran "
        f"({_bounded(tables)}): whatever mode you pick, this run also compares them by "
        "CHECKSUM."
    )


def catalog_drift_start_log_clause(tables: "Sequence[str]") -> str:
    tables = list(tables)
    if not tables:
        return ""
    return (
        "; tables whose source schema changed are also compared by CHECKSUM: "
        f"{_bounded(tables)}"
    )


def catalog_drift_held_log_clause(gate: Optional[CatalogDriftGate]) -> Optional[str]:
    """The "validation completed" clause replacing "ready for cut-over" while held."""
    if gate is None or not gate.applies:
        return None
    if gate.open:
        return f"; cut over held: source schema changed on {_bounded([i.table for i in gate.open])}"
    from dsql_migrator.ui.cdc_dlq_gate import MUST_RECHECK_REASONS

    held = tuple(
        i for i in gate.issues
        if i.reason in MUST_RECHECK_REASONS or (i.reason == "not-validated" and not gate.waived)
    )
    if not held:
        return None
    return (
        f"; cut over held: {len(held)} table(s) lack a CHECKSUM match after their source "
        f"schema matched the Step 1 inventory again: {_bounded([i.table for i in held])}"
    )


def catalog_drift_ack_log_clause(gate: Optional[CatalogDriftGate]) -> str:
    """The clause appended to "cut over acknowledged" ("" when the gate is inert)."""
    if gate is None or not gate.applies:
        return ""
    if gate.waived:
        return (
            "; source schema check WAIVED (CHECKSUM proof for "
            f"{_bounded([i.table for i in gate.waivable])})"
        )
    if gate.unchecked:
        if gate.probe_state == PROBE_STATE_UNAVAILABLE:
            why = "source password not in memory"
        elif gate.probe_state == PROBE_STATE_NO_BASELINE:
            why = "no Step 1 inventory or captured table set"
        elif gate.problem == PROBE_NONE_VISIBLE:
            why = "none of the captured tables visible"
        else:
            why = "read failed: " + str(
                getattr(gate.last_probe, "error_class", None) or "unknown error"
            )
        return f"; source schema: not re-checked ({why})"
    if gate.resolved:
        return (
            f"; source schema: changed earlier on {_bounded(list(gate.resolved))}, each "
            "cleared by a CHECKSUM match after it matched the Step 1 inventory again"
        )
    if gate.probe_state == PROBE_STATE_OK:
        return (
            "; source schema: matched the Step 1 inventory at "
            f"{_fmt_hms(getattr(gate.last_probe, 'started_at', None))}"
        )
    return "; source schema: not re-checked (no read)"


def catalog_drift_waiver_log_detail(gate: CatalogDriftGate) -> str:
    return (
        "operator chose to cut over without CHECKSUM proof for "
        f"{_bounded([i.table for i in gate.waivable])}, whose source schema changed while CDC "
        f"ran (until {_fmt_hm(_latest_resolve(gate.waivable))}); the check re-opens if their "
        "source schema changes again"
    )[:480]


# --- Activity log -----------------------------------------------------------------------


def _transition_lines(
    transitions: "Sequence[CatalogDriftTransition]",
) -> "list[tuple[str, str, str, str]]":
    """``(status name, action, target, detail)`` per transition KIND -- one line for all the
    tables a read changed that way, never one per table. Value-free."""
    from dsql_migrator.ui.cdc_dlq_gate import CDC_DLQ_EVIDENCE_MARGIN_SECONDS

    groups: dict[str, list[CatalogDriftTransition]] = {}
    for t in transitions:
        key = "opened" if t.kind in ("opened", "reopened") else t.kind
        groups.setdefault(key, []).append(t)
    margin = timedelta(seconds=CDC_DLQ_EVIDENCE_MARGIN_SECONDS)
    lines: list[tuple[str, str, str, str]] = []
    for key in ("opened", "resolved", "rebaselined", "not-in-inventory"):
        group = sorted(groups.get(key, ()), key=lambda t: t.table)
        if not group:
            continue
        n = len(group)
        tables = [t.table for t in group]
        target = tables[0] if n == 1 else f"{n} tables"
        stamps = [_aware(getattr(t.finding, "resolved_at", None)) for t in group]
        stamps = [s for s in stamps if s is not None]
        after = _fmt_hms(max(stamps) + margin) if stamps else "the change"
        if key == "opened":
            parts = []
            for t in group[:_NAME_LIMIT]:
                facts = _facts(t.finding)
                parts.append(
                    f"{t.table}: {', '.join(kind_labels(t.finding))}"
                    + (f" ({facts})" if facts else "")
                )
            more = f"; +{n - _NAME_LIMIT} more tables" if n > _NAME_LIMIT else ""
            lines.append((
                "FAILURE",
                "source schema changed on a captured table: CDC does not carry it"
                if n == 1
                else f"source schema changed on {n} captured tables: CDC does not carry it",
                target,
                "; ".join(parts) + more + ". Cut over is held.",
            ))
        elif key == "resolved":
            firsts = [_aware(getattr(t.finding, "first_seen_at", None)) for t in group]
            firsts = [f for f in firsts if f is not None]
            lines.append((
                "WARNING",
                "source schema matches the Step 1 inventory again",
                target,
                f"{_bounded(tables)}: changed from at least "
                f"{_fmt_hms(min(firsts) if firsts else None)}; Cut over waits for a CHECKSUM "
                f"match started after {after}.",
            ))
        elif key == "rebaselined":
            lines.append((
                "WARNING",
                "Step 1 reads a changed definition for a captured table"
                if n == 1
                else f"Step 1 reads a changed definition for {n} captured tables",
                target,
                f"{_bounded(tables)}: the Step 1 re-run changed the definition this check "
                "compares with, so values CDC applied before it may differ; Cut over waits "
                f"for a CHECKSUM match started after {after}.",
            ))
        else:
            lines.append((
                "WARNING",
                "captured table left the Step 1 inventory: source schema finding closed"
                if n == 1
                else f"{n} captured tables left the Step 1 inventory",
                target,
                f"{_bounded(tables)}: no longer in the Step 1 inventory (dropped or renamed at "
                "the source); Validation cannot compare them, so Cut over asks you to "
                "acknowledge them.",
            ))
    return lines


def log_catalog_drift_transitions(transitions: "Sequence[CatalogDriftTransition]") -> None:
    """One durable activity line per transition kind per read (never per table or poll).
    Best-effort."""
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
    for status, action, target, detail in _transition_lines(transitions):
        try:
            log_activity(
                ActivityCategory.CDC,
                action,
                status=getattr(ActivityStatus, status),
                target=target,
                detail=detail[:600],
            )
        except Exception:  # noqa: BLE001 - one bad line must not drop the others
            continue


__all__ = [
    "CDC_CATALOG_DRIFT_PROBE_TTL_SECONDS",
    "CatalogBaseline",
    "CatalogDriftGate",
    "CatalogDriftIssue",
    "CatalogDriftRequest",
    "CatalogDriftTransition",
    "CdcCatalogDriftProbe",
    "MonitorOpenNotice",
    "PROBE_NONE_VISIBLE",
    "PROBE_NO_SOURCE",
    "PROBE_READ_FAILED",
    "PROBE_STATE_NO_BASELINE",
    "build_catalog_drift_probe",
    "catalog_drift_baseline",
    "catalog_drift_gate_for",
    "catalog_drift_probe_due",
    "catalog_drift_probe_possible",
    "catalog_drift_proven_tables",
    "catalog_drift_request",
    "catalog_drift_scope",
    "catalog_drift_waived_tables",
    "cdc_catalog_drift_cutover_gate",
    "fold_catalog_drift_probe",
    "log_catalog_drift_transitions",
    "probe_catalog_drift_now",
    "schedule_catalog_drift_probe",
    "start_catalog_drift_probe",
]
