# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The column-exclusion overlay at Start CDC (NiceGUI-free).

Before any connector is created or changed, the Start CDC job makes every captured
overlay column nullable with no DEFAULT on Aurora DSQL, so every row CDC writes holds NULL
there -- the same NULL contract the Full Load keeps. The dropped DEFAULTs are recorded
first in the overlay ledger (cut over restores them). This covers a CDC-only migration,
Retry CDC and a re-Start after Stop (a cut-over restore may have put a DEFAULT back), and
is a verified no-op after a Full Load that already overlaid the tables. Every failure
blocks the Start, changing no connector.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Mapping, NamedTuple, Optional, Sequence

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.overlay_ledger import (
    LedgerDecision,
    LedgerRead,
    OverlayLedgerError,
    OverlayLedgerView,
    append_verdict,
    plan_ledger_record,
)
from dsql_migrator.core.schema_applier import (
    apply_exclusion_overlay,
    build_overlay_statements,
)
from dsql_migrator.core.target_introspector import target_overlay_facts

_PREFIX = "Start CDC blocked: "


class StartCdcOverlayBlocked(RuntimeError):
    """The overlay could not be made safe, so Start CDC stops before any connector."""

    def __init__(self, reason: str, *, sqlstate: Optional[str] = None) -> None:
        super().__init__(_PREFIX + reason)
        self.sqlstate = sqlstate


def _first_line(exc: BaseException) -> str:
    text = str(exc).strip().splitlines()
    return text[0] if text else type(exc).__name__


def _live_overlay(table_facts, overlay) -> "tuple[dict, list]":
    live: dict[str, Any] = {}
    computed: list[str] = []
    for column in sorted(overlay):
        facts = table_facts.columns.get(column)
        if facts is None:
            continue
        if facts.computed:
            computed.append(column)
        else:
            live[column] = facts
    return live, computed


def _plan_detail(live) -> str:
    parts = []
    for column in sorted(live):
        facts = live[column]
        steps = []
        if facts.not_null:
            steps.append("DROP NOT NULL")
        if facts.default_expr is not None:
            steps.append(f"DROP DEFAULT (was {facts.default_expr})")
        if steps:
            parts.append(f"{column}: {', '.join(steps)}")
    return "; ".join(parts)


def apply_start_cdc_overlay(
    handle,
    *,
    overlay_by_table: "Mapping[str, Sequence[str]]",
    captured: "Sequence[str]",
    excluded_by_table: "Mapping[str, Sequence[str]]",
    source_columns_for: "Callable[[str], Sequence[str]]",
    ledger,
    ledger_key: Optional[str],
    ledger_in_use: bool,
    connection_factory: Optional[Callable[[], Any]],
    effective_target_ddls: "Callable[[], Optional[Mapping[str, str]]]",
    log: Callable[..., None],
    facts_reader: Callable[..., Any] = target_overlay_facts,
    applier: Callable[..., Any] = apply_exclusion_overlay,
) -> None:
    """Record, then apply, the overlay of the captured tables -- strictly serially.

    A session with no captured overlay column that never used the ledger makes no call
    at all. The ledger is read once; the target catalog once (the overlay columns plus
    every recorded set column); the record is written in at most ONE batch BEFORE any
    ALTER (a recorded-only table read empty gets its table-level reconcile); then one
    overlay unit per table, cancel-checked with a heartbeat. Raises
    :class:`StartCdcOverlayBlocked` on anything it cannot make safe.
    """
    captured_set = set(captured)
    overlay = {
        t: frozenset(c) for t, c in overlay_by_table.items() if t in captured_set and c
    }
    if not overlay and not ledger_in_use:
        return
    if ledger is None or ledger_key is None:
        if overlay:
            raise StartCdcOverlayBlocked(
                "no overlay ledger configured, so the DEFAULTs of the excluded columns "
                "could not be recorded. Nothing was changed."
            )
        return
    try:
        read = ledger.read(ledger_key)
    except Exception as exc:  # noqa: BLE001 - the contract says never; unknown if it does
        read = LedgerRead("unreadable", {}, f"{type(exc).__name__}: {exc}")
    if read.status == "unreadable":
        # Blocks whenever the session uses the record (as the Full Load does): a table
        # CDC now writes values into could otherwise keep a record saying it is NULL in
        # every row, and a later append would mix values into those rows.
        raise StartCdcOverlayBlocked(
            "the record of dropped DEFAULTs on Aurora DSQL could not be read, so the "
            "exclusion record of the captured tables cannot be kept accurate. Nothing was "
            "changed; retry Start CDC."
        )
    view = OverlayLedgerView(ledger, ledger_key, read)
    recorded = {t for t in captured_set if view.record(t) is not None}
    recorded_only = recorded - set(overlay)
    tables = sorted(set(overlay) | recorded)
    if not tables:
        return
    if connection_factory is None:
        raise StartCdcOverlayBlocked(
            "there is no Aurora DSQL connection to read the target catalog; nothing was "
            "changed."
        )
    columns = {
        t: sorted(set(overlay.get(t, ())) | set(getattr(view.record(t), "set_columns", ()) or ()))
        for t in tables
    }
    try:
        facts = facts_reader(tables, columns, connection_factory=connection_factory) or {}
    except Exception:  # noqa: BLE001 - every table unknown
        facts = {}
    unknown = [t for t in tables if facts.get(t) is None]
    if unknown:
        raise StartCdcOverlayBlocked(
            f"could not read the Aurora DSQL catalog of {', '.join(unknown)}; nothing was "
            "changed on them."
        )
    ddls_cache: dict = {}

    def _ddl(table: str) -> Optional[str]:
        if "value" not in ddls_cache:
            try:
                ddls_cache["value"] = effective_target_ddls() or {}
            except Exception:  # noqa: BLE001 - an unreadable DDL only makes the restore manual
                ddls_cache["value"] = {}
        return ddls_cache["value"].get(table)

    now = datetime.now(timezone.utc)
    merges: dict[str, Any] = {}
    units: list[tuple[str, dict]] = []
    for table in tables:
        table_facts = facts[table]
        if not table_facts.exists:
            continue
        live, computed = _live_overlay(table_facts, overlay.get(table, ()))
        for column in computed:
            log(
                "column excluded",
                status=ActivityStatus.INFO,
                detail=f"{table}.{column}: computed by Aurora DSQL: omitted and not altered",
            )
        record = view.record(table)
        has_rows = bool(table_facts.has_rows)
        refusal, reincluded = append_verdict(
            record, table_facts=table_facts, overlay_live=sorted(live), computed=computed,
            excluded=tuple(excluded_by_table.get(table, ()) or ()),
            source_columns=tuple(source_columns_for(table) or ()),
        )
        if refusal is not None and has_rows:
            # CDC coexistence is the one exemption: the stream never writes an excluded
            # column, so the rows already there keep what they hold (the record says so).
            log(
                "overlay append",
                status=ActivityStatus.WARNING,
                detail=(
                    f"{table}: {refusal.text()} -- streamed anyway (CDC never writes an "
                    "excluded column); rows already in the table keep what they hold in "
                    f"{', '.join(refusal.columns)}"
                ),
            )
        if table in recorded_only and has_rows and not reincluded:
            continue  # unchanged: the record stays as it is
        if record is None and not live:
            continue  # nothing live to record and no record: no ledger write at all
        needs_default = any(f.default_expr is not None for f in live.values())
        ddl = _ddl(table) if needs_default else None

        def _merge(existing, table=table, live=live, has_rows=has_rows, ddl=ddl,
                   reincluded=reincluded):
            decision = plan_ledger_record(
                existing, table=table, overlay_facts=live, has_rows=has_rows,
                just_recreated=False, effective_ddl=ddl, now=now, reincluded=reincluded,
            )
            if (
                decision.kind == "write"
                and existing is not None
                and decision.record.model_copy(update={"applied_at": existing.applied_at})
                == existing
            ):
                return LedgerDecision("unchanged", existing)
            return decision

        merges[table] = _merge
        if build_overlay_statements(table, live):
            units.append((table, live))
            log(
                "overlay planned",
                status=ActivityStatus.INFO,
                detail=f"{table}: {_plan_detail(live)}" + (
                    "; the dropped DEFAULTs are recorded first and restored at cut over"
                    if needs_default
                    else ""
                ),
            )
    if merges:
        try:
            view.merge(merges)
        except OverlayLedgerError as exc:
            names = ", ".join(sorted(merges))
            if units:
                raise StartCdcOverlayBlocked(
                    f"cannot record the DEFAULTs to be dropped on {names}; nothing was "
                    f"altered ({_first_line(exc)})."
                ) from exc
            raise StartCdcOverlayBlocked(
                f"cannot record the change to the exclusion record of {names}; nothing "
                f"was changed ({_first_line(exc)})."
            ) from exc
    done: list[str] = []
    for table, live in sorted(units):
        if getattr(handle, "cancelled", False):
            return
        after = view.record(table)
        expected = {
            column: (
                after.column(column).original_default
                if after is not None and after.column(column) is not None
                else None
            )
            for column in live
        }
        try:
            outcome = applier(
                table, live, expected_defaults=expected, connection_factory=connection_factory
            )
            log(
                "overlay applied",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{table}: nullable: {', '.join(getattr(outcome, 'dropped_not_null', ()) or ()) or 'none'}; "
                    f"DEFAULT dropped: {', '.join(sorted(getattr(outcome, 'dropped_defaults', {}) or {})) or 'none'}"
                ),
            )
            done.append(table)
        except Exception as exc:  # noqa: BLE001 - blocks the Start, no connector touched
            code = getattr(exc, "sqlstate", None)
            detail = (
                f"the change to {table} failed ({code + ': ' if code else ''}"
                f"{_first_line(exc)}). "
                + (f"{', '.join(done)} {'is' if len(done) == 1 else 'are'} already nullable; "
                   if done else "")
                + "the others were not changed. Retry Start CDC: it issues only what is "
                "still needed. No connector was created or changed."
            )
            log("overlay failed", status=ActivityStatus.FAILURE, detail=detail)
            raise StartCdcOverlayBlocked(detail, sqlstate=code) from exc
        finally:
            beat = getattr(handle, "heartbeat", None)
            if callable(beat):
                beat()


class OverlayFactsUnknown(RuntimeError):
    """The CDC Start dialog could not read the target catalog for the overlay columns."""


class CdcOverlayPreview(NamedTuple):
    """What the CDC-only Start dialog shows: the overlay statements Start CDC runs, and
    the captured overlay tables that already hold rows (CDC never writes an excluded
    column, so those rows keep what they hold there)."""

    statements: list
    tables_with_rows: "tuple[str, ...]" = ()


def probe_cdc_overlay_changes(
    overlay_by_table: "Mapping[str, Sequence[str]]",
    *,
    connection_factory: Optional[Callable[[], Any]],
    facts_reader: Callable[..., Any] = target_overlay_facts,
) -> CdcOverlayPreview:
    """The overlay Start CDC will apply, from ONE live catalog read. BLOCKING.

    Raises :class:`OverlayFactsUnknown` when the catalog cannot be read (the dialog then
    says so; the job reads again before any ALTER and blocks if it still cannot).
    """
    overlay = {t: sorted(c) for t, c in overlay_by_table.items() if c}
    if not overlay:
        return CdcOverlayPreview([])
    if connection_factory is None:
        raise OverlayFactsUnknown("no Aurora DSQL connection")
    facts = facts_reader(sorted(overlay), overlay, connection_factory=connection_factory) or {}
    statements = []
    with_rows = []
    for table in sorted(overlay):
        table_facts = facts.get(table)
        if table_facts is None:
            raise OverlayFactsUnknown(table)
        live, _computed = _live_overlay(table_facts, overlay[table])
        statements.extend(build_overlay_statements(table, live))
        if table_facts.exists and table_facts.has_rows:
            with_rows.append(table)
    return CdcOverlayPreview(statements, tuple(with_rows))


__all__ = [
    "CdcOverlayPreview",
    "OverlayFactsUnknown",
    "StartCdcOverlayBlocked",
    "apply_start_cdc_overlay",
    "probe_cdc_overlay_changes",
]
