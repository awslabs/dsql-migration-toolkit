# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Post-start CDC monitoring / DLQ / status render panels (extracted from ``_cdc_ui.py``).

The surfaces that are meaningful only once CDC is streaming: the per-table migration
status table, the live connector-health + stream-lag monitoring, the change-flow and
pipeline-health panels, the dead-letter-queue panel (+ its breakdown/records/download
sub-panels), the schema-drift banner and the CDC-handling panel, with the facts they show
(the AI-assistant facts). The pure state predicates live in ``_cdc_state``, the read-models
in ``_models`` / ``_cdc_status``, and the column-exclusion and table-set locks, which make
no NiceGUI call, in ``_table_set_lock``; the schema-drift copy and runbooks and the Fix
target schema (add-column) dialog live in ``_cdc_drift_fix``. This module re-exports every
name defined in those two, private ones included, so an import from here resolves
unchanged. A re-export does not carry a monkeypatch: a patch on ``_table_set_lock`` does not
reach this module's own read of ``exclusion_lock_state`` (in
:func:`_exclusion_state_facts`), which a test patches here, and a patch here does not reach
the reads ``_cdc_drift_fix`` makes (its runbooks, the dialog's ``render_notice``); the two
modules' docstrings say what a patch there reaches.

Its imports: at its top, ``_cdc_state`` / ``_models`` / ``_cdc_status`` /
``_table_set_lock`` / ``_cdc_drift_fix`` / ``_full_load_ui`` / ``core`` / ``ui.design`` and
the package's ``_LOGGER``; inside functions, more of those and also ``ui.ai_tools``,
``ui.cdc_catalog_drift_gate``, ``ui.cdc_dlq_gate``, ``ui.cdc_replica_identity_gate``,
``ui.connect`` and NiceGUI's ``run``. One-directional with ``_cdc_ui``: never imports from
it, since ``_cdc_ui`` imports the panels it renders from here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Mapping, Sequence
from typing import Optional

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.models import LoadStatusView, SourceType
from dsql_migrator.ui.data_migration._cdc_state import (
    _CDC_POLL_INTERVAL_SECONDS,
    _cdc_is_streaming,
    cdc_monitoring_visible,
    cdc_pipeline_live,
)
from dsql_migrator.ui.data_migration._models import (
    assess_dlq_health,
    build_lag_chart_option,
    build_migration_table_status,
    cdc_handling_facts,
    connector_health_rows,
    format_column_exclude_list,
    per_table_counts_notice_body,
    session_source_type,
)
from dsql_migrator.ui.data_migration._cdc_status import (
    _CDC_TONE_STYLE,
    _VALUE_FIT_COLUMN_LIMIT,
    _apply_cdc_status,
    _cdc_status_view,
    _current_job,
    _fetch_cdc_status,
    _fetch_migration_row_counts,
    _migration_status_tables,
    cdc_dlq_records,
    cdc_dlq_shown_message,
    cdc_dlq_summary,
    cdc_error_log_key,
    is_cdc_error_record,
)
# The column-exclusion and table-set locks moved to _table_set_lock.py (cdcui-5); every name
# defined there, private ones included, is re-exported here so an import from this module
# resolves unchanged. A re-export does not carry a monkeypatch: patch _table_set_lock for
# the calls made there, and this module for its own read (exclusion_lock_state in
# _exclusion_state_facts).
from dsql_migrator.ui.data_migration._table_set_lock import (  # noqa: F401
    _EXCLUSION_BAKED_PHASES,
    cdc_stack_being_deleted,
    cdc_delete_eta,
    cdc_stop_in_flight,
    cdc_operation_in_flight,
    _stop_then,
    capture_error_with_remedy,
    ExclusionLockState,
    exclusion_lock_state,
    fk_apply_running,
    RECONNECT_REASON,
    TABLE_SET_WAIT_LOAD,
    TABLE_SET_WAIT_FK,
    TABLE_SET_WAIT_RESTORE,
    TABLE_SET_WAIT_INFRA,
    TABLE_SET_WAIT_CDC_CHECK,
    TABLE_SET_WAIT_CDC_UNREAD,
    _FAILED_CREATE_STATUSES,
    table_set_failed_create_wait,
    table_set_delete_wait,
    _cdc_started_for_migration,
    _JOB_UNSET,
    _fl_cdc_narrowing_job,
    _fl_cdc_narrowing_open,
    table_set_editable_after_load,
    table_set_wait_reason,
    LOCK_TIP_RULE,
    LOCK_TIP_SPLIT,
    ADDED_TABLE_LOAD_ROUTE,
    TREE_HINT_FL_CDC,
    TREE_HINT_SPLIT,
    CDC_ONLY_NARROWING_SENTENCE,
    R4_FL_CDC_NARROWING,
    R1_FL_CDC_SENTENCE,
    CONTINUE_HINT_FL_CDC,
    _delete_reseed_note,
    _refuse_rule,
    _refuse_rule_unloaded,
    _bounded_names,
    _ADD_LOCK_REFUSALS,
    TableSetAddLock,
    table_set_add_lock,
    _uncovered,
    cdc_uncovered_selection,
    fl_cdc_pending_additions,
    cdc_addition_block_reason,
    change_and_reload_block_reason,
)
# The Fix target schema copy and dialog live in _cdc_drift_fix (cdcui-8); every name it
# defines is re-exported here, private ones included, so an import from here resolves
# unchanged.
from dsql_migrator.ui.data_migration._cdc_drift_fix import (  # noqa: F401
    _UNIQUE_CONFLICT_WHY_HELD,
    _UNIQUE_CONFLICT_WHY_REST,
    _UNIQUE_CONFLICT_WHY,
    _UNIQUE_CONFLICT_TAIL,
    _UNIQUE_CONFLICT_TAIL_CDC_ONLY,
    _UNBINDABLE_WHY,
    _UNBINDABLE_RECOVERY,
    _UNBINDABLE_RECOVERY_CDC_ONLY,
    _UNBINDABLE_REPAIR_CDC_ONLY,
    _DRIFT_LABELS,
    _VALUE_FIT_TIME_WHY,
    _value_fit_times_only,
    _VALUE_FIT_WHY_PG,
    _VALUE_FIT_SERVER_WHY,
    _VALUE_FIT_SERVER_PG,
    _VALUE_FIT_SERVER_ALSO,
    _SINK_VALUE_FIT_CODE,
    _DROP_COLUMN_NOT_SENT_WHY,
    _DROP_COLUMN_TOAST_PG,
    _CHECK_WHY_PG,
    _drift_why,
    _CORROBORATED_KINDS,
    _catalog_confirming_changes,
    _catalog_label,
    _TARGET_SIDE_DRIFT_KINDS,
    _CERTAIN_LOSS_DRIFT_KINDS,
    _NON_DDL_SOURCE_KINDS,
    _CHECKSUM_REPAIRED_DRIFT_KINDS,
    _FIX_NAME_LIMIT,
    _fix_names,
    _fix_is_cdc_only,
    _DLQ_KEY_VALUES,
    _DLQ_KEY_LISTED,
    _value_fit_runbook,
    _fix_engine_sentence,
    _fix_refusal_notice,
    _key_change_remedy,
    _truncate_runbook,
    _banner_rekeyed,
    _ddl_runbook,
    _value_fit_remap,
    _FIX_RENAME_EVIDENCE,
    _fix_rename_notice,
    _fix_held_notices,
    _fix_orphan_notice,
    _FIX_RESUMES,
    _FIX_ADD_STEPS,
    _FIX_ADD_RUNBOOK,
    _fix_still_sent,
    _fix_runbook,
    _FixInputs,
    _fix_inputs,
    _open_add_column_dialog,
)
from dsql_migrator.ui.data_migration._full_load_ui import _quarantine_detail_row
from dsql_migrator.ui.design import (
    EXPANSION_PANEL_CLASSES,
    FIT_TABLE_CLASS,
    definition_row,
    fit_table_css,
    inline_hint,
    notice_panel,
    render_notice,
)
from dsql_migrator.ui.data_migration import _LOGGER

# The two legend strings that make a CLAIM about cut-over safety, pulled out as constants so
# a test can hold the claim without driving the whole live-monitoring render (and so a future
# edit cannot quietly put a go/no-go back into a live metric).
#
# "safe to cut over" used to be attached to "caught up" here -- the only place the tool
# turned a live metric into a go/no-go -- and it is wrong in one reachable state: a table an
# initial snapshot has not REACHED yet has no lag datapoint at all, so it reads "caught up"
# while none of its changes have arrived. Cut over is gated on Validation's exact comparison,
# never on this column.
_STREAM_LAG_HELP = (
    "How far behind the target is, in time — the age of the newest source change not yet "
    "applied. “caught up” = no recent change is waiting; during an initial snapshot that "
    "can also mean the snapshot has not reached this table yet, so it is not a cut-over "
    "signal on its own. “N behind (PK)” is a fallback shown only when the time-based "
    "metric isn't available yet."
)
# The legend stated only one direction ("non-green means investigate"). The reciprocal is
# what a cut-over decision actually needs, and it is load-bearing on a start that
# re-snapshots: green cannot prove the snapshot has reached this table, because a table it
# has not reached yet has no lag datapoint and no missing-row signal either.
_CONSISTENCY_LEGEND_HELP = (
    "— any non-green badge means investigate. “consistent” means nothing looks wrong (the "
    "source count is an estimate, so it is not a proven exact match), and on a start that "
    "re-snapshots it does not prove the snapshot has reached this table; Validation "
    "(step 4) is the exact check, and cut over is gated on it."
)


def _source_estimate_header_tip(source_type) -> str:
    """The "Source rows (est.)" header tooltip, worded for the SOURCE ENGINE.

    The figure comes from a different catalog on each engine, refreshed by a different
    mechanism, and the tooltip used to describe only MySQL's ("information_schema ...
    InnoDB index sampling") -- wrong on a PostgreSQL source, where it is the planner's
    ``pg_class.reltuples``. Deliberately quotes no autovacuum/InnoDB thresholds: they
    are configurable and differ by platform (RDS/Aurora ship their own defaults), so a
    number here would be wrong on some sources.
    """
    exact_hint = "For an exact source-vs-target comparison, run Validation (step 4)."
    if source_type == SourceType.POSTGRES:
        return (
            "Approximate row count from PostgreSQL's planner statistics "
            "(pg_class.reltuples) — a scan-free ESTIMATE, so this view never runs a "
            "COUNT(*) full scan against your live source. It is only as current as the "
            "last ANALYZE, VACUUM (manual or autovacuum) or CREATE INDEX on the table, so "
            "rows written since then are not in it; a table with no statistics yet shows "
            "“not analyzed”. Hover a value for when it was last refreshed. " + exact_hint
        )
    return (
        "Approximate row count from the source's information_schema — a scan-free "
        "ESTIMATE, so this view never runs a COUNT(*) full scan against your live "
        "source. InnoDB derives it from index sampling, so it commonly differs from the "
        "true count by several percent (more on a large table) and often UNDERCOUNTS — "
        "a target that slightly exceeds it is normal, not data duplication. On MySQL "
        "8.0+ it is read from InnoDB, not from the copy MySQL caches for "
        "information_schema_stats_expiry (24 hours by default). ANALYZE TABLE refreshes "
        "it. " + exact_hint
    )


def _source_estimate_cell(
    estimate: "Optional[int]",
    *,
    exact: bool,
    stats_at: "Mapping[str, Optional[datetime]]",
    table: str,
) -> "tuple[str, str]":
    """``(label, tooltip)`` for one table's source-rows cell. Pure.

    ``stats_at`` is when each estimate was last refreshed (see
    ``SourceDialect.estimate_stats_at``). A table present with ``None`` has no recorded
    ANALYZE/VACUUM time covering its whole estimate; a table ABSENT is unknown (MySQL, or
    the read failed) and keeps the plain rendering -- nothing is invented for it.
    """
    known = table in stats_at
    refreshed = stats_at.get(table)
    if estimate is None:
        if known and refreshed is None:
            return (
                "not analyzed",
                "PostgreSQL has no statistics for this table yet (it has never been "
                "analyzed or vacuumed), so there is no row estimate. Autovacuum will "
                "analyze it once enough of its rows change. Validation (step 4) counts "
                "it exactly.",
            )
        return "—", ""
    label = f"{estimate:,}" + (" (exact)" if exact else "")
    if exact or not known:
        return label, ""
    if refreshed is None:
        # An estimate exists but no single refresh time covers it: a partition that was
        # never analyzed (its rows are missing from the summed figure), or a PostgreSQL 13
        # table that was never analyzed and reads 0. Say so rather than date it.
        return (
            label,
            "How current this estimate is, is unknown: no ANALYZE or VACUUM time covers "
            "all of it (for example a partition that has never been analyzed, whose rows "
            "are then missing from it). Validation (step 4) counts it exactly.",
        )
    return (
        label,
        "Planner estimate last refreshed by ANALYZE or VACUUM at "
        f"{refreshed.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC (for a "
        "partitioned table, its least recently refreshed partition) — rows written "
        "since then may not be in it.",
    )


def _render_migration_table_status(
    ui, migration_state, job_manager, session, *, inventory=None
) -> None:
    """Per-table consistency view: Full Load vs CDC, source-vs-target, DLQ.

    Answers the operator's real question -- "did CDC replicate everything, is
    anything missing?". It separates the one-shot Full Load row count from the
    changes CDC has applied since, shows the source-vs-target consistency verdict, and
    surfaces per-table quarantined (DLQ) events -- changes that did NOT reach the
    target. The per-op **Inserts / Updates / Deletes** columns are fed scan-free by the
    sink's own ``InsertsApplied`` / ``UpdatesApplied`` / ``DeletesApplied`` CloudWatch
    metrics (DMS-style cumulative counters, refreshed each CDC poll), so they need no
    COUNT(*). The source/target-count columns are an explicit "Refresh counts" action
    (not an auto-poll): the TARGET is an exact COUNT(*), while the SOURCE is a scan-free
    catalog ESTIMATE plus an index-only MAX(pk) -- never a COUNT(*) against the live
    production source (see _fetch_migration_row_counts).

    Rendered only once CDC has actually STARTED. It used to appear as soon as the CDC
    sub-step did -- i.e. during the ~15-20 min infrastructure create -- where every
    CDC-specific column (Stream lag, Quarantined, Inserts/Updates/Deletes, Consistency)
    is necessarily empty because no connector exists yet. That read as "CDC is running
    and replicating nothing", the opposite of the truth, and it padded the deploy screen
    with a table that could not answer its own question. Nothing is lost by waiting: the
    Full Load's own per-table table (Table / Status / Rows / Progress / Time / Attempts)
    stays on the Full Load step, so its results remain visible there.

    Matches ``_render_cdc_live_monitoring`` directly above it, which is likewise
    "meaningful only once streaming".
    """
    # cdc_monitoring_visible: appears the moment Start CDC is pressed (the connectors
    # take ~10-20 min to reach RUNNING and the operator wants the per-table view during
    # that ramp), and disappears again while a teardown is in flight -- replication
    # figures for a pipeline being dismantled are about to be meaningless.
    if not cdc_monitoring_visible(migration_state, job_manager):
        return
    table_names = _migration_status_tables(migration_state, job_manager)
    if not table_names:
        return

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("table_rows", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Per-table migration status").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold"
            )
            ui.space()  # type: ignore[attr-defined]
            fetched_at = getattr(migration_state, "row_counts_fetched_at", None)
            if fetched_at is not None:
                ui.label(  # type: ignore[attr-defined]
                    f"counts as of {fetched_at.strftime('%H:%M:%S')} UTC"
                ).classes("text-xs text-gray-400")

        # Persistent-table holder: the table ELEMENT + its header ⓘ tooltips are built
        # once; later polls swap ONLY the row data in place (see the early-return
        # below), so a tooltip is not torn down mid-hover by the ~5s poll.
        _status_tbl: dict = {"el": None}

        def _status_table() -> None:  # type: ignore[misc]
            job = _current_job(job_manager, migration_state.job_id)
            # Per-table quarantined (DLQ) counts: change events that did NOT reach the
            # target -- the "missing" the customer cares about for consistency.
            # CDC-sourced ONLY. The error log is keyed by the Full Load job id (both
            # phases write under it -- cdc_error_log_key), so an unfiltered summary put
            # Full Load quarantines in a column headed "Quarantined" on the CDC table,
            # AND disagreed with the DLQ card right below it: the card read "0
            # quarantined" while this column read 3 for the same session. Same key, same
            # filter, so the two now always agree.
            dlq_counts = dict(
                cdc_dlq_summary(
                    migration_state, cdc_error_log_key(migration_state)
                ).errors_by_table
            )
            rows_model = build_migration_table_status(
                table_names,
                full_load_job=job,
                target_counts=getattr(migration_state, "row_count_target", {}),
                source_counts=getattr(migration_state, "row_count_source", {}),
                dlq_counts=dlq_counts,
                source_max_pk=getattr(migration_state, "row_max_pk_source", {}),
                target_max_pk=getattr(migration_state, "row_max_pk_target", {}),
                applied_ops_metric=getattr(
                    migration_state, "cdc_applied_ops_by_table", {}
                ),
                replication_lag_ms=getattr(
                    migration_state, "cdc_replication_lag_by_table", {}
                ),
            )
            # Columns separate the one-shot Full Load contribution from the ongoing
            # CDC contribution, then show the source-vs-target consistency verdict
            # and anything quarantined (not applied).
            columns = [
                {"name": "table", "label": "Table", "field": "table", "align": "left"},
                {"name": "fl", "label": "Full Load", "field": "fl", "align": "left"},
                {"name": "fl_rows", "label": "Full Load rows", "field": "fl_rows"},
                # DMS-style per-op columns: cumulative INSERT/UPDATE/DELETE counts CDC
                # has applied since it started streaming (each its own column so the
                # counters read like DMS table statistics).
                {"name": "ins", "label": "Inserts", "field": "ins", "align": "right"},
                {"name": "upd", "label": "Updates", "field": "upd", "align": "right"},
                {"name": "del", "label": "Deletes", "field": "del", "align": "right"},
                # Header says "(est.)" once: the source figure here is the scan-free
                # information_schema estimate (an exact COUNT(*) on a large production
                # source is not run from this view), so the approximation belongs in
                # the column's name rather than only repeated in every cell.
                {"name": "source", "label": "Source rows (est.)", "field": "source"},
                {"name": "target", "label": "Target rows", "field": "target"},
                {"name": "stream", "label": "Stream lag", "field": "stream"},
                {"name": "dlq", "label": "Quarantined", "field": "dlq"},
                {
                    "name": "consistency",
                    "label": "Consistency",
                    "field": "consistency",
                    "align": "left",
                },
            ]

            def _fmt(n: "Optional[int]") -> str:
                return "—" if n is None else f"{n:,}"

            def _fmt_count(n: "Optional[int]") -> str:
                # Bare thousands-separated count for the I/U/D sub-cells (no sign:
                # each op count is non-negative). "0" when the metric is present but
                # that op hasn't happened; the has_ops flag drives the "—" empty case.
                return "0" if n is None else f"{n:,}"

            def _fmt_lag(ms: "Optional[int]") -> str:
                # Time-based replication lag (ms) -> human "behind" text. Sub-second
                # rounds to "caught up" (effectively real-time); else s / m·s / h·m.
                if ms is None:
                    return "—"
                if ms < 1000:
                    return "caught up"
                secs = ms / 1000.0
                if secs < 60:
                    return f"{secs:.1f}s behind"
                if secs < 3600:
                    return f"{int(secs // 60)}m {int(secs % 60)}s behind"
                return f"{int(secs // 3600)}h {int((secs % 3600) // 60)}m behind"

            # User-facing consistency label + the verdict key (drives the badge color).
            # NB: there is deliberately no "target ahead" verdict. The source figure
            # here is an ESTIMATE that typically UNDERCOUNTS, so a target exceeding it
            # is the normal case -- flagging it as an anomaly made most healthy tables
            # look broken (see MigrationTableStatus.consistency).
            _CONSISTENCY_LABEL = {
                "consistent": "consistent",
                "quarantined": "data quarantined",
                "behind": "replicating…",
                "gap": "rows missing",
                "unknown": "refresh to check",
            }
            _stats_at = dict(getattr(migration_state, "source_estimate_stats_at", {}) or {})
            table_rows = []
            for r in rows_model:
                # The header already says "(est.)" (the normal case), so only the
                # UNUSUAL case is marked per-cell: an EXACT source count, which is
                # worth flagging because it makes the row's verdict authoritative.
                source_label, source_tip = _source_estimate_cell(
                    r.source_rows,
                    exact=not r.source_estimate,
                    stats_at=_stats_at,
                    table=r.table,
                )
                # Stream lag: prefer the sink's TIME-based end-to-end lag
                # (ReplicationLagMs = apply time − source commit time) — accurate and
                # PK-agnostic. Fall back to the MAX(pk) leading-edge check (caught up /
                # N behind) only when the metric is unavailable (older plugin) or the
                # counts weren't refreshed. A metric present but idle (no recent
                # datapoint) means the stream drained → "caught up".
                if r.replication_lag_ms is not None:
                    stream = _fmt_lag(r.replication_lag_ms)
                elif r.stream_caught_up is True:
                    stream = "caught up"
                elif r.stream_caught_up is False:
                    stream = f"{r.pk_gap:,} behind (PK)"
                else:
                    stream = "—"
                verdict = r.consistency
                table_rows.append(
                    {
                        "table": r.table,
                        # Title-case label (Done/In progress/…) to match the Full Load
                        # stats table's status badge; fl_state stays raw for the color.
                        "fl": {
                            "DONE": "Done",
                            "IN_PROGRESS": "In progress",
                            "FAILED": "Failed",
                            "PENDING": "Pending",
                        }.get(r.full_load_state, r.full_load_state) or "—",
                        "fl_state": r.full_load_state or "",
                        "fl_rows": _fmt(r.full_load_rows),
                        # DMS-style per-op counters (one column each): cumulative
                        # inserts / updates / deletes the sink applied since streaming
                        # began (scan-free). has_ops is False when the metrics are
                        # unavailable (older plugin / not yet emitting) -> the cells
                        # show "—".
                        "has_ops": r.cdc_applied_ops is not None,
                        "ins": _fmt_count(r.cdc_inserts),
                        "upd": _fmt_count(r.cdc_updates),
                        "del": _fmt_count(r.cdc_deletes),
                        "source": source_label,
                        "source_tip": source_tip,
                        "target": _fmt(r.target_rows),
                        "stream": stream,
                        "dlq": _fmt(r.dlq_count) if r.dlq_count else "0",
                        "consistency": _CONSISTENCY_LABEL.get(verdict, verdict),
                        "verdict": verdict,
                    }
                )
            # Poll update path: once the table exists, swap ONLY its row data in
            # place (no element/slot teardown) so the header ⓘ tooltips stay open
            # while hovering. Everything below (element, body/header slots, timer)
            # runs ONCE, on the first build.
            existing = _status_tbl["el"]
            if existing is not None:
                existing.rows[:] = table_rows  # type: ignore[attr-defined]
                existing.update()  # type: ignore[attr-defined]
                return
            # `dense` + a high rows-per-page (no footer pager) + `flat` keep the
            # table compact and render every table inline, so it grows with content
            # instead of showing a bottom pagination bar / inner scroll. Short column
            # labels were not enough to avoid a HORIZONTAL scrollbar: eleven columns of
            # Quasar's `white-space: nowrap` cells pinned the table 36px wider than the
            # card, so FIT_TABLE_CLASS lets the headers and the long `schema.table`
            # names wrap instead (see ui/design.py for the measurements).
            fit_table_css(ui)
            table = ui.table(  # type: ignore[attr-defined]
                columns=columns,
                rows=table_rows,
                row_key="table",
                pagination={"rowsPerPage": 0},
            ).props("dense flat").classes(f"w-full {FIT_TABLE_CLASS}")
            _status_tbl["el"] = table
            # Color the Full Load state as a badge.
            table.add_slot(
                "body-cell-fl",
                r"""
                <q-td :props="props">
                  <q-badge v-if="props.row.fl_state"
                    :color="{'DONE':'positive','IN_PROGRESS':'primary','FAILED':'negative','PENDING':'grey'}[props.row.fl_state] || 'grey'"
                    :label="props.value" outline />
                  <span v-else>—</span>
                </q-td>
                """,
            )
            # DMS-style per-op counters, one column each: cumulative inserts (green) /
            # updates (sky) / deletes (red) CDC has applied. Just the running count in
            # the op's colour (no leading glyph), or "—" when the metrics aren't
            # available yet (older plugin / sink not emitting).
            def _op_cell(name: str, colour: str) -> None:
                table.add_slot(
                    f"body-cell-{name}",
                    r"""
                    <q-td :props="props" class="text-right">
                      <span v-if="props.row.has_ops" class="%s">{{ props.value }}</span>
                      <span v-else>—</span>
                    </q-td>
                    """
                    % colour,
                )

            _op_cell("ins", "text-green-700")
            _op_cell("upd", "text-sky-700")
            _op_cell("del", "text-red-700")
            # Source rows: the estimate plus, when known, WHEN it was taken (a hover
            # tooltip, so the column stays as narrow as the rest of this 11-column table).
            table.add_slot(
                "body-cell-source",
                r"""
                <q-td :props="props">
                  <span :class="props.value === 'not analyzed' ? 'text-grey-6' : ''">
                    {{ props.value }}
                    <q-tooltip v-if="props.row.source_tip" class="text-body2"
                      style="max-width:340px">{{ props.row.source_tip }}</q-tooltip>
                  </span>
                </q-td>
                """,
            )
            # Color the consistency verdict (green=consistent, red=quarantined/gap,
            # amber=behind, grey=unknown) so a problem is obvious at a glance.
            table.add_slot(
                "body-cell-consistency",
                r"""
                <q-td :props="props">
                  <q-badge
                    :color="{'consistent':'positive','quarantined':'negative','gap':'negative','behind':'warning','unknown':'grey'}[props.row.verdict] || 'grey'"
                    :label="props.value" outline />
                </q-td>
                """,
            )
            # In-context help: an ⓘ next to the three columns whose meaning isn't
            # obvious from the label, so the explanation is right where the eye is
            # (the legend below is the fuller reference). Quasar `header-cell-<name>`
            # slot renders the label + a hover tooltip.
            def _hdr_info(name: str, tip: str) -> None:
                table.add_slot(
                    f"header-cell-{name}",
                    r"""
                    <q-th :props="props">
                      {{ props.col.label }}
                      <q-icon name="info" size="14px" class="q-ml-xs text-grey-5"
                        style="cursor:help">
                        <q-tooltip class="text-body2" style="max-width:340px">"""
                    + tip
                    + r"""</q-tooltip>
                      </q-icon>
                    </q-th>
                    """,
                )

            _hdr_info("ins", "Cumulative inserts CDC has applied since it started streaming.")
            _hdr_info("upd", "Cumulative updates CDC has applied since it started streaming.")
            _hdr_info("del", "Cumulative deletes CDC has applied since it started streaming.")
            _hdr_info(
                "stream",
                "How far behind the target is, in time — the age of the newest "
                "source change not yet applied. “caught up” = the target is current "
                "(safe to cut over). “N behind (PK)” is a fallback shown only when "
                "the time-based metric isn't available yet.",
            )
            _hdr_info(
                "source",
                # session_source_type, not source_config: after a restore the live
                # config can be empty while the engine is still known.
                _source_estimate_header_tip(session_source_type(session)),
            )
            _hdr_info(
                "consistency",
                "Overall verdict for the table: “consistent” (green) = the stream has "
                "caught up and nothing indicates missing rows · “replicating…” "
                "(amber) = target still catching up · “rows missing” (red) = newest "
                "rows landed but some in between are missing · “data quarantined” "
                "(red) = changes failed and were set aside (DLQ). Because Source rows "
                "is an estimate, “consistent” means nothing looks wrong rather than a "
                "proven exact match — Validation (step 4) is the exact check. Any "
                "non-green = worth investigating.",
            )
            # Keep the per-op Inserts/Updates/Deletes columns (and lag/quarantined)
            # live while CDC streams. Created ONCE (this block only runs on the first
            # build); a REPEATING timer re-invokes _status_table, which early-returns
            # to an in-place row swap on this persistent table. Reads state only (NO
            # network / no COUNT), so it stays scan-free. A repeating timer is safe
            # here (unlike the old .refresh() path) precisely because it never
            # re-renders / tears down slots -- so it can't trigger the "parent slot
            # deleted" crash, and the header ⓘ tooltips survive each poll. Armed only
            # while CDC is active; idle/Full-Load-only leaves the table static.
            if _cdc_is_streaming(migration_state):
                ui.timer(_CDC_POLL_INTERVAL_SECONDS, _status_table)  # type: ignore[attr-defined]

        async def _refresh_counts() -> None:
            # The refresh reads the source estimate + MAX(pk) and the target's exact
            # COUNT(*)/MAX(pk); the target count can take seconds on a large table, so
            # give clear in-progress feedback:
            # the button is disabled and relabelled "Refreshing…" (its text stays
            # legible -- we do NOT use Quasar `loading`, which replaces the label
            # with a bare spinner), and an ongoing top notification carries the
            # spinner. A toast confirms when the counts land. Without this the click
            # looked like a no-op (only a few cells changed at the end).
            from nicegui import run

            refresh_btn.set_text("Refreshing…")
            refresh_btn.disable()
            # An "ongoing"-type ui.notify has timeout=0 and returns no handle, so it
            # never auto-dismisses -- it would linger forever. Use ui.notification,
            # which returns a handle we explicitly .dismiss() when the fetch ends.
            progress = ui.notification(  # type: ignore[attr-defined]
                "Counting source and target rows…",
                type="ongoing",
                position="top",
                spinner=True,
                timeout=None,
            )
            try:
                fetched = await run.io_bound(
                    _fetch_migration_row_counts,
                    migration_state,
                    session,
                    table_names,
                    inventory,
                )
                if fetched is not None:
                    (
                        source_counts,
                        target_counts,
                        source_max_pk,
                        target_max_pk,
                        at,
                        source_available,
                    ) = fetched
                    migration_state.set_row_counts(
                        source=source_counts,
                        target=target_counts,
                        source_max_pk=source_max_pk,
                        target_max_pk=target_max_pk,
                        fetched_at=at,
                    )
                    if source_available:
                        ui.notify("Source/target counts updated.", type="positive", position="top")  # type: ignore[attr-defined]
                    else:
                        # Target read OK, but the source couldn't be read -- almost
                        # always a restored session with no source password (creds
                        # are never persisted). Tell the user how to get source counts.
                        ui.notify(  # type: ignore[attr-defined]
                            "Target counts updated. Source counts need the source "
                            "connection — re-enter it on the Connect step to compare "
                            "source vs target.",
                            type="warning",
                            position="top",
                            multi_line=True,
                        )
                else:
                    ui.notify(  # type: ignore[attr-defined]
                        "Could not read the counts — check the source/target "
                        "connections.",
                        type="warning",
                        position="top",
                    )
            finally:
                # Always dismiss the ongoing notification so it does not linger.
                try:
                    progress.dismiss()  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001 - already gone / page rebuilt
                    pass
                # The page may have been rebuilt while awaiting (NiceGUI #3028);
                # only touch the button/table if they still exist.
                if not getattr(refresh_btn, "is_deleted", False):
                    refresh_btn.set_text("Refresh source/target counts")
                    refresh_btn.enable()
                    _status_table()  # in-place row update (table built once already)

        # Top: the one thing to know before reading the numbers -- the source side
        # is a scan-free estimate, so it adds no load on a large-scale source but is
        # approximate (Validation does the exact reconciliation).
        # Before the first refresh the Consistency column reads "refresh to check" on
        # every row; the notice body then names the button that fills it (the column
        # and the action are otherwise only linked by a coincidence of wording). After
        # a refresh it drops the prompt and states the estimate caveat.
        _counts_fetched = (
            getattr(migration_state, "row_counts_fetched_at", None) is not None
        )
        render_notice(
            ui,
            tone="info",
            header="Source rows are an estimate (no load on the source)",
            body=per_table_counts_notice_body(counts_fetched=_counts_fetched),
        )
        refresh_btn = ui.button(  # type: ignore[attr-defined]
            "Refresh source/target counts",
            on_click=_refresh_counts,
            icon="sync",
        ).props("color=primary outline size=sm")
        _status_table()
        # Below the table (reference help, not a status to act on): the column
        # legend as scannable definition rows in a quiet bordered panel. Each term
        # matches a table header, so the mapping is obvious; the Consistency row
        # renders the REAL badge chips (same Quasar colors as the table cells), so
        # the reader sees the actual green/amber/red instead of imagining it. The
        # per-column ⓘ tooltips above carry the same help in-context.
        with ui.column().classes(  # type: ignore[attr-defined]
            "gap-1 w-full mt-2 border border-gray-200 rounded-md bg-gray-50 p-3"
        ):
            with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                ui.icon("help_outline").classes("text-sky-600 text-base")  # type: ignore[attr-defined]
                ui.label("How to read this table").classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-gray-900"
                )
            definition_row(
                ui, "Full Load rows", "Rows the one-shot snapshot loaded."
            )
            definition_row(
                ui,
                "Inserts / Updates / Deletes",
                "Cumulative row changes CDC has applied since it started streaming — "
                "green = inserts, blue = updates, red = deletes (running totals, live "
                "from the sink, scan-free). “—” = the metrics aren't available yet.",
            )
            definition_row(
                ui,
                "Source / Target rows",
                "Source = scan-free information_schema ESTIMATE (never a COUNT(*) on "
                "your live source) · Target = exact count. The estimate comes from "
                "InnoDB index sampling and often undercounts by a few percent, so a "
                "target slightly above it is normal — the two numbers are not meant "
                "to match exactly. Run Validation (step 4) for the exact comparison.",
            )
            definition_row(ui, "Stream lag", _STREAM_LAG_HELP)
            # Consistency: the actual badge chips, colored exactly like the table's
            # body-cell-consistency slot (consistent→positive, behind→warning,
            # gap/quarantined→negative). Keep these labels/colors in sync with that
            # slot and _CONSISTENCY_LABEL above.
            _consistency_desc = definition_row(ui, "Consistency")
            with _consistency_desc:  # type: ignore[attr-defined]
                ui.badge("consistent").props("color=positive outline")  # type: ignore[attr-defined]
                ui.badge("replicating…").props("color=warning outline")  # type: ignore[attr-defined]
                ui.badge("rows missing").props("color=negative outline")  # type: ignore[attr-defined]
                ui.badge("data quarantined").props("color=negative outline")  # type: ignore[attr-defined]
                ui.label(_CONSISTENCY_LEGEND_HELP).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-600"
                )

def _announce_cdc_events(migration_state, status_view, ai_post_event) -> None:
    """Edge-triggered CDC activity events into the AI feed (once per transition).

    Announces: CDC streaming started, each classified dead-letter group (schema drift, a
    source TRUNCATE, a missing target table, ... -- per table+kind), the DLQ
    first growing past zero, and a confirmed sink stall -- the silent-data-loss signals
    the assistant should be aware of. Deduped per stream via ``migration_state
    .cdc_announced``, so Start over's in-place state reset clears the markers.
    Credential/row-free text only (Property 7); never raises into the poller."""
    if ai_post_event is None:
        return
    try:
        # Keyed by the pinned CDC key, and held ON THE MIGRATION STATE. The key alone was
        # wrong in both directions: a Full Load retry changed it, so every event was
        # re-announced to the AI feed (a stream that appears to start twice); and the
        # stack-derived fallback is IDENTICAL across migrations, so after Start over a
        # fresh CDC-only session inherited the old markers and announced NOTHING at all.
        #
        # Adding ``id(migration_state)`` to a module-level dict did NOT fix that second
        # half, because Start over does not rebuild the state object -- it calls
        # ``reset_in_place``, which re-runs ``__init__`` on the SAME instance precisely so
        # the builders' captured closures stay valid. The id is therefore unchanged and the
        # stale markers survived, which is the regression this keying was meant to prevent:
        # after a Start over a fresh CDC run posted NO "CDC streaming started", drift or DLQ
        # event at all. Storing the markers on the state instead makes ``__init__`` clear
        # them for free, and drops a process-lifetime module dict that also leaked one entry
        # per stream per session.
        announced = getattr(migration_state, "cdc_announced", None)
        if not isinstance(announced, dict):
            announced = {}
            migration_state.cdc_announced = announced
        seen = announced.setdefault(cdc_error_log_key(migration_state), set())
        if "started" not in seen:
            seen.add("started")
            ai_post_event(text="CDC streaming started", status="started")
        for group in getattr(status_view, "schema_drift", None) or []:
            marker = ("drift", group.table, group.kind)
            if marker not in seen:
                seen.add(marker)
                label = _DRIFT_LABELS.get(group.kind, (group.kind, ""))[0]
                # Worded by the kind's own label, never as a blanket "source schema change":
                # several kinds are not one (a source TRUNCATE, a missing TARGET table, a key
                # conflict), and this text grounds later AI turns, so calling them DDL would
                # point the assistant at the wrong fix.
                ai_post_event(
                    text=(
                        f"CDC: {group.table}: {label} — "
                        f"{group.count} record(s) dead-lettered"
                    ),
                    status="warning",
                )
        depth = int(getattr(status_view, "dlq_depth", 0) or 0)
        if depth > 0 and "dlq" not in seen:
            seen.add("dlq")
            ai_post_event(
                text=(
                    f"CDC: dead-letter queue growing — {depth} poison record(s) "
                    "quarantined"
                ),
                status="warning",
            )
        activity = getattr(migration_state, "cdc_activity", None)
        if bool(getattr(activity, "sink_stall_confirmed", False)) and "stall" not in seen:
            seen.add("stall")
            ai_post_event(
                text=(
                    "CDC: sink stalled — the source is producing but the sink is not "
                    "advancing"
                ),
                status="error",
            )
    except Exception:  # noqa: BLE001 - activity mirroring is best-effort
        pass


def _render_cdc_slot_health(ui, migration_state) -> None:
    """Render the PostgreSQL replication-slot WAL-pressure notice (PostgreSQL CDC only).

    Reads ``migration_state.cdc_slot_health`` (a :class:`SlotHealth`, populated by the
    read-only source read on the consistency-view refresh) and renders the classified
    notice: an invalidated slot -> error, WAL pressure / an inactive slot -> warning, a
    healthy slot -> success. Only a real slot renders a panel; ``None`` / no-slot (a
    MySQL source, or before the slot exists) shows nothing, so the panel is inherently
    PostgreSQL-only. Pure render (no I/O).
    """
    health = getattr(migration_state, "cdc_slot_health", None)
    if health is None or not getattr(health, "exists", False):
        return
    from dsql_migrator.core.cdc_pg_slot import SLOT_MISSING_HEADER, pg_cdc_route
    from dsql_migrator.core.cdc_postgres import classify_slot_health
    from dsql_migrator.ui.data_migration._models import MigrationType

    tone, headline, detail = classify_slot_health(health)
    if tone == "error":
        # An invalidated slot (G25): its way out is the route rule's, like the Start CDC block
        # for the same slot. This panel renders only once a Start was submitted, so the stack
        # counts as started: new CDC topics before any Full Load re-run -- its topics may hold
        # changes no sink applied, which its sink would apply over the reload -- and, with
        # CDC only, Start CDC on the new stack (no Full Load step).
        detail = (
            f"{detail} "
            + pg_cdc_route(
                SLOT_MISSING_HEADER,
                started=True,
                cdc_only=getattr(migration_state, "migration_type", None)
                is MigrationType.CDC_ONLY,
            )
        )
    # Say WHEN it was read. The slot is read only on the source/target counts refresh, yet
    # this renders every 5 s tick -- so without the time a snapshot read as live state (a
    # stopped-time "no consumer" kept showing after Start CDC while the real slot was
    # active). State invalidates the snapshot on a pipeline start/stop; the time covers the
    # rest (the slot's WAL figures move while the pipeline state does not).
    read_at = getattr(migration_state, "cdc_slot_health_at", None)
    if read_at is not None:
        detail = (
            f"{detail} (Slot read at {read_at.strftime('%H:%M:%S')} UTC — refresh the "
            "source/target counts to re-read it.)"
        )
    render_notice(ui, tone=tone, header=headline, body=detail)


def _render_cdc_replica_identity_notice(
    ui, migration_state, session, on_check_again=None
) -> None:
    """The runtime REPLICA IDENTITY re-check for re-keyed PostgreSQL tables (render only).

    ``error`` while a finding is open (deletes can be lost; Cut over is held), with
    "Check again" -- and, when the re-read cannot clear it, why (no source password, or the
    last read failed); ``warning`` for a table whose identity is FULL again (or that CDC no
    longer re-keys) but that no CHECKSUM started after the fix has proven yet; with nothing
    open, ``info`` when the re-check cannot run (no source password after a restart) and
    ``warning`` when its last read failed. Reads only state the CDC poll's throttled
    background read (``cdc_replica_identity_gate.start_replica_identity_probe``) landed -- no
    I/O here. Nothing at all for MySQL or a table set with no re-key.
    """
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        cdc_evidence,
        has_source_connection,
        monitor_restored_notice,
        monitor_unchecked_notice,
        monitor_weak_notice,
        replica_identity_proven_tables,
        replica_identity_scope,
        replica_identity_waived_tables,
    )

    findings = dict(getattr(migration_state, "cdc_replica_identity_findings", None) or {})
    scope = replica_identity_scope(migration_state, session)
    if not findings and not scope:
        return
    has_password = has_source_connection(session)
    last_probe = getattr(migration_state, "cdc_replica_identity_last_probe", None)
    weak = monitor_weak_notice(findings, has_password=has_password, last_probe=last_probe)
    if weak is not None:
        render_notice(ui, tone="error", header=weak[0], body=weak[1])
        if on_check_again is not None:
            with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                button = ui.button(  # type: ignore[attr-defined]
                    "Check again", icon="refresh", on_click=on_check_again
                ).props("flat no-caps color=primary")
                if not has_password or bool(
                    getattr(migration_state, "cdc_replica_identity_probe_running", False)
                ):
                    button.props("disable")
    provider = getattr(migration_state, "validation_state_provider", None)
    try:
        validation_state = provider() if callable(provider) else None
    except Exception:  # noqa: BLE001 - without it every restored finding still shows
        validation_state = None
    restored = monitor_restored_notice(
        findings,
        replica_identity_proven_tables(findings, validation_state),
        getattr(migration_state, "migration_type", None),
        waived=replica_identity_waived_tables(
            findings, getattr(validation_state, "cutover_replica_identity_waiver", None)
        ),
    )
    if restored is not None:
        render_notice(ui, tone="warning", header=restored[0], body=restored[1])
    # With a finding open the error above already says why the re-read cannot clear it.
    if weak is None and scope and cdc_evidence(migration_state):
        unchecked = monitor_unchecked_notice(scope, last_probe, has_password=has_password)
        if unchecked is not None:
            render_notice(ui, tone=unchecked[0], header=unchecked[1], body=unchecked[2])


# The source-schema kinds the dead-letter banner ALSO classifies (from the rejection's
# SQLSTATE and record shape), with the same names as the re-check's: a table both report the
# same way is listed only in that banner, which then carries the re-check row's repair for it
# (the key-change recreate remedy, the DROP NOT NULL step) and the re-check's label
# (_catalog_label). ``value-does-not-fit`` is deliberately NOT here: it is not a schema-change
# claim (a MySQL TINYINT(1) holding 2 needs no DDL), so it must never hide a re-check finding;
# a widened table shows both, and both hold Cut over. Nor is ``check-violation``: a MySQL ENUM
# that gained a value is a re-check "type-change", and that row shows the new definition the
# replacement CHECK needs -- both stay, deliberately (like a rename).
_DLQ_SHARED_SCHEMA_KINDS = frozenset(
    {"add-column", "drop-column", "type-change", "key-change", "nullability-change"}
)


def _listed_in_dlq_banner(finding, banner_kinds, null_columns=()) -> bool:
    """Whether the dead-letter banner already reports every kind of this open finding.

    ``null_columns`` are the columns the banner's NULL rows (nullability-change) name for
    this table: the columns a NULL has reached."""
    kinds = set(getattr(finding, "kinds", None) or ())
    if not kinds or not banner_kinds or not kinds <= set(banner_kinds):
        return False
    if "nullability-change" in kinds:
        # The banner's NULL row is a RELAXATION (a NULL sent) and names only the columns a
        # NULL has reached. A tightening dead-letters nothing, changes past the kept six
        # cannot be checked, and a relaxed column no NULL has reached yet is named only by
        # the re-check row: each keeps that row. Names compare exactly, so a mismatch shows
        # both rows (the safe side).
        named = set(null_columns or ())
        changes = [
            change
            for change in (getattr(finding, "changes", None) or ())
            if getattr(change, "kind", None) == "nullability-change"
        ]
        if (
            not changes
            or int(getattr(finding, "more_changes", 0) or 0) > 0
            or any(
                getattr(change, "after", None) != "NULL"
                or getattr(change, "column", None) not in named
                for change in changes
            )
        ):
            return False
    # An unfixable add-column finding keeps its row here: an added column the source
    # computes is invisible to the banner, and the shared plan also refuses a table whose
    # CDC key column (message.key.columns) is gone -- the banner's Fix then only shows that
    # refusal, while this row's runbook says how to handle a key column.
    return "add-column" not in kinds or bool(getattr(finding, "fixable", False))


def _render_cdc_catalog_drift_notice(
    ui, migration_state, session, view=None, *, on_check_again=None, on_fix=None
) -> None:
    """The runtime source-schema re-check for the captured tables (render only).

    ``error`` while a finding is open -- a source DDL change CDC does not carry, which no
    rejected row reveals (a dropped nullable column, a widened type, a missing table, ...):
    one row per table with what changed and why it matters, the runbook for the migration
    type, "Fix target schema…" for an added or renamed column (not where the dead-letter
    banner already offers it), and "Check again". A table whose change the dead-letter banner
    already reports the same way (a column added, dropped or retyped, a changed key, a NULL
    rule relaxed when the banner names every relaxed column; the banner is drawn whenever
    the queue's depth is known) is listed only there -- never twice, with two
    severities and two runbooks (an unfixable add -- all generated, or a key refusal -- keeps
    its row here); the finding still holds Cut over. ``warning`` for a table
    whose source matches the baseline again but that no CHECKSUM started after that has
    proven yet. With nothing open, ``info`` when the check cannot run (no source password
    after a restart) and ``warning`` when its last read failed or found none of the tables.
    Reads only what the CDC poll's throttled background read (``cdc_catalog_drift_gate``)
    landed -- the baseline provider is CPU only. Nothing at all without the builder's
    baseline handle.
    """
    from dsql_migrator.ui.cdc_catalog_drift_gate import (
        _inventory_names,
        _raw_baseline,
        _usable,
        catalog_drift_proven_tables,
        catalog_drift_scope,
        catalog_drift_waived_tables,
        handle_registered,
        monitor_open_notice,
        monitor_resolved_notice,
        monitor_unchecked_notice,
    )
    from dsql_migrator.ui.cdc_replica_identity_gate import (
        _is_postgres,
        cdc_evidence,
        has_source_connection,
    )

    findings = dict(getattr(migration_state, "cdc_catalog_drift_findings", None) or {})
    # ONE provider call per render (CPU only): the baseline, and the inventory's names.
    raw_baseline = _raw_baseline(migration_state) if handle_registered(migration_state) else None
    baseline = raw_baseline if _usable(raw_baseline) else None
    scope = catalog_drift_scope(migration_state, baseline) if baseline is not None else ()
    if not findings and not scope:
        return
    has_password = has_source_connection(session)
    last_probe = getattr(migration_state, "cdc_catalog_drift_last_probe", None)
    migration_type = getattr(migration_state, "migration_type", None)
    open_findings = {
        t: f for t, f in findings.items() if getattr(f, "resolved_at", None) is None
    }
    # A table whose change the dead-letter banner below already reports the same way is
    # listed only there (one severity, one runbook). Render only: the gate and Cut over
    # still see every open finding, and the "Can't re-check" notice below keys on them all.
    drift_groups = list(getattr(view, "schema_drift", None) or ())
    banner_kinds: dict = {}
    banner_null_columns: dict = {}
    if getattr(view, "dlq_depth", None) is not None:  # the dead-letter card is drawn
        for g in drift_groups:
            if getattr(g, "kind", "") in _DLQ_SHARED_SCHEMA_KINDS:
                table = getattr(g, "table", None)
                banner_kinds.setdefault(table, set()).add(g.kind)
                if g.kind == "nullability-change":
                    banner_null_columns.setdefault(table, set()).update(
                        getattr(g, "columns", None) or ()
                    )
    shown_open = {
        t: f for t, f in open_findings.items()
        if not _listed_in_dlq_banner(f, banner_kinds.get(t), banner_null_columns.get(t, ()))
    }
    if shown_open:
        # The dead-letter banner already offers Fix for its add-column groups: one button
        # per table, never two.
        dlq_adds = {
            getattr(g, "table", None)
            for g in drift_groups
            if getattr(g, "kind", "") == "add-column"
        }
        fix_tables = {
            t for t, f in shown_open.items()
            if getattr(f, "fixable", False)
            and session is not None
            and on_fix is not None
            and has_password
            and t not in dlq_adds
        }
        notice = monitor_open_notice(
            shown_open,
            is_pg=_is_postgres(session),
            migration_type=migration_type,
            has_password=has_password,
            last_probe=last_probe,
            baseline_known=baseline is not None,
            fix_tables=fix_tables,
        )
        with notice_panel(
            ui, tone="error", header=notice.header, icon="schema"  # type: ignore[union-attr]
        ):
            ui.label(notice.intro).classes("text-xs text-gray-800")  # type: ignore[attr-defined,union-attr]
            for table, text in notice.rows:  # type: ignore[union-attr]
                with ui.row().classes("w-full items-center gap-2"):  # type: ignore[attr-defined]
                    ui.label(text).classes("text-xs text-gray-800")  # type: ignore[attr-defined]
                    if table in fix_tables:
                        ui.button(  # type: ignore[attr-defined]
                            "Fix target schema…",
                            on_click=lambda _e=None, t=table: on_fix(t),
                        ).props("flat dense no-caps size=sm color=primary")
            if notice.more_tables:  # type: ignore[union-attr]
                ui.label(notice.more_tables).classes("text-xs text-gray-800")  # type: ignore[attr-defined,union-attr]
            ui.label(notice.runbook).classes("text-xs italic text-gray-600")  # type: ignore[attr-defined,union-attr]
            ui.label(notice.tail).classes("text-xs text-gray-800")  # type: ignore[attr-defined,union-attr]
            if on_check_again is not None:
                with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                    button = ui.button(  # type: ignore[attr-defined]
                        "Check again", icon="refresh", on_click=on_check_again
                    ).props("flat no-caps color=primary")
                    if (
                        not has_password
                        or baseline is None
                        or bool(getattr(migration_state, "cdc_catalog_drift_probe_running", False))
                    ):
                        button.props("disable")
    provider = getattr(migration_state, "validation_state_provider", None)
    try:
        validation_state = provider() if callable(provider) else None
    except Exception:  # noqa: BLE001 - without it every resolved finding still shows
        validation_state = None
    inventory_names = _inventory_names(raw_baseline)
    resolved = monitor_resolved_notice(
        findings,
        catalog_drift_proven_tables(findings, validation_state, inventory_names),
        migration_type,
        waived=catalog_drift_waived_tables(
            findings, getattr(validation_state, "cutover_catalog_drift_waiver", None)
        ),
        inventory_names=inventory_names,
    )
    if resolved is not None:
        render_notice(ui, tone="warning", header=resolved[0], body=resolved[1])
    # With a finding open the error above already says why the re-read cannot clear it; with
    # no baseline nothing is drawn here (Cut over warns).
    if not open_findings and scope and cdc_evidence(migration_state):
        unchecked = monitor_unchecked_notice(scope, last_probe, has_password=has_password)
        if unchecked is not None:
            render_notice(ui, tone=unchecked[0], header=unchecked[1], body=unchecked[2])


# How many named columns the value-conversion notice lists inline before it tails off
# with "+N more" -- the same treatment cdc_applied_rollup_detail gives a long per-table
# list, so a pathological schema cannot turn one notice into a wall of text.
_VALUE_CONVERSION_LIST_LIMIT = 12


def _render_cdc_value_conversion_notice(ui, migration_state) -> None:
    """Flag columns the SOURCE connector could not convert and delivered as NULL.

    Distinct from the dead-letter panel BY CONSTRUCTION, and it has to stay that way:
    the row was APPLIED, so nothing was quarantined, the DLQ depth does not (and must
    not) include it, and "0 quarantined" is therefore not evidence that this did not
    happen. Reads the sticky set the CDC poll merged onto the state (see
    :mod:`dsql_migrator.core.cdc_value_conversion`) and renders NOTHING when it is empty,
    so it is inert on a healthy stream and for a source that has never logged one -- the
    same render-only-when-actionable rule the schema-drift banner and the slot-health
    notice follow. ``error`` tone because a named column is silently wrong on the target
    for every row CDC delivered, and it stays wrong until acted on; that is the same
    calibration ``unbindable-value`` gets, not a "be aware". Pure render (no I/O).

    Publishes no COUNT on purpose: the number of affected ROWS is not knowable from these
    log lines, and a number that looks total but is a floor is worse than no number.
    """
    failures = list(
        getattr(migration_state, "cdc_value_conversion_failures", None) or []
    )
    if not failures:
        return
    shown = ", ".join(
        f"{failure.qualified_column} (type {failure.type_name})"
        for failure in failures[:_VALUE_CONVERSION_LIST_LIMIT]
    )
    more = len(failures) - _VALUE_CONVERSION_LIST_LIMIT
    if more > 0:
        shown = f"{shown} (+{more} more)"
    render_notice(
        ui,
        tone="error",
        header="Column values are arriving empty — the source connector cannot convert them",
        body=(
            "The source connector could not convert these columns and delivered them as "
            f"NULL, so the rows WERE applied with the column empty: {shown}. Nothing was "
            "dead-lettered, so the dead-letter panel below does not count these rows and "
            "no row count can see them — Validation (step 4) will report the table as a "
            "checksum MISMATCH, and it is the only surface that can. Which rows are "
            "affected is not recorded, and the problem is broader than the columns "
            "listed: any value the connector cannot convert fails the same way, and this "
            "list is only what it has logged so far. The change stream cannot carry these "
            "columns at all, so reloading the table only helps once the stream is stopped "
            "— at cut over, stop CDC, then reload the affected table(s) with "
            "'Drop & reload' (an append reload cannot repair them: it only inserts "
            "primary keys the target is missing, so a row already there keeps its empty "
            "column), then re-run Validation; if it still reports a mismatch, the value "
            "cannot be represented on the target as it stands and has to change at the "
            "source."
        ),
    )


def _render_cdc_live_monitoring(
    ui, migration_state, job_manager, session=None, cdc_ai_opener=None,
    ai_post_event=None,
    # The DLQ panel rendered INSIDE this section owns the in-app recovery for rows CDC
    # dropped over DSQL's per-value limit, and both hooks come from the caller (the
    # candidate lookup needs the inventory; the exclusion writes migration-wide state).
    # They were added to the caller and to the panel but NOT to this function in between,
    # so every render of the Data Migration step raised
    # "TypeError: _render_cdc_live_monitoring() got an unexpected keyword argument
    # 'lob_candidates_for'" and the whole step fell to its error boundary.
    lob_candidates_for=None,
    exclude_columns=None,
) -> None:
    """Live connector health + DLQ, polled read-only from MSK Connect.

    Mirrors the Full Load poll chain: a refreshable region arms a one-shot timer
    at the END of its render, and the poll re-renders + re-arms -- a
    self-perpetuating single-shot chain that avoids the "parent slot deleted"
    crash a repeating timer causes. Meaningful only once streaming, so it is
    placed after the start action.

    Hidden entirely until CDC has STARTED. Before that there is no pipeline to report
    on -- the whole section (the "Live status" header, the empty stream-lag chart, and
    the "appears once connectors are detected" placeholder) was just dead space on the
    deploy screen, with the header sitting borderless above nothing. Gated on
    :func:`cdc_streaming_started` (not ``cdc_pipeline_live``) so it appears the moment
    Start CDC is pressed and stays through the connectors' ~10-20 min ramp -- which is
    exactly when the operator wants to watch them come up. Matches the per-table table
    below, which uses the same gate. The dead-letter panel is rendered INSIDE this
    section, so it inherits the same visibility -- see :func:`cdc_monitoring_visible`,
    which also hides all of it while a teardown is in flight.
    """
    if not cdc_monitoring_visible(migration_state, job_manager):
        return
    ui.label("Live status").classes("text-sm font-semibold")  # type: ignore[attr-defined]

    # --- Live "Stream lag" chart -------------------------------------------------
    # Created ONCE here, OUTSIDE the 5s refreshable below, and updated IN PLACE via
    # chart.update() on each poll -- so the line extends continuously like a
    # CloudWatch graph instead of the whole echart being torn down and recreated
    # every 5s (which flickered). The card is hidden until there are >=2 points (a
    # single dot is not a trend). The rolling series behind it is a hybrid: seeded
    # from CloudWatch's 1-min history (survives reload) then extended each poll.
    lag = {"card": None, "chart": None, "empty": None, "stalled": None}
    with ui.card().classes("w-full") as _lag_card:  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-1.5 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("show_chart", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Stream lag").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            # The interpretation guidance (what flat/rising means, cut-over safety)
            # is genuinely useful but reads as clutter when always on -- move it to a
            # hover ⓘ, matching the per-table header tooltips. The chart title +
            # y-axis "lag (ms)" carry the basics on their own.
            ui.icon("info").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help"
            ).tooltip(
                "Worst end-to-end replication lag across tables, live (max lag in "
                "ms). Flat near zero = caught up (safe to cut over); a rising line "
                "means the pipeline is falling behind."
            )
        lag["chart"] = (  # type: ignore[assignment]
            ui.echart(  # type: ignore[attr-defined]
                {"xAxis": {"type": "time"}, "yAxis": {"type": "value"}, "series": []}
            )
            .classes("w-full")
            .style("height: 220px")
        )
        # Shown INSTEAD of the chart when there is no >=2-point trend but CDC is live:
        # the sink emits ReplicationLagMs only while applying events, so a drained /
        # caught-up pipeline has no recent datapoint to plot. Without this the whole
        # panel vanished after a session restore (the in-memory trend is not persisted
        # and a caught-up pipeline can't be re-seeded from CloudWatch), so the operator
        # saw NO stream-lag signal at all. Now the metric is always present when CDC is
        # running -- as a "caught up" line when there is nothing to trend.
        with ui.row().classes("items-center gap-1.5 no-wrap") as _lag_empty:  # type: ignore[attr-defined]
            # The sink emits ReplicationLagMs only WHILE APPLYING, so "no datapoint" is
            # the absence of evidence, not evidence of zero lag -- and the same input is
            # produced by a sink that has stopped applying entirely. Say what was observed.
            ui.icon("remove_circle_outline").classes("text-sky-600 text-base")  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "No replication-lag datapoint in the recent window "
                "— the sink reports lag only while it is applying."
            ).classes("text-sm text-gray-700")
        lag["empty"] = _lag_empty  # type: ignore[assignment]
        # The SAME "no datapoints" input means the opposite thing when the sink has
        # stalled: the sink emits ReplicationLagMs only while applying, so a DEAD sink
        # produces no datapoint and rendered the green "Caught up" line above -- a total
        # replication outage shown as the strongest possible all-clear. This row
        # replaces it in that case (see _update_lag_chart).
        with ui.row().classes("items-center gap-1.5 no-wrap") as _lag_stalled:  # type: ignore[attr-defined]
            ui.icon("error").classes("text-red-600 text-base")  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "No lag data because the sink is applying nothing — this is a stall, "
                "not being caught up."
            ).classes("text-sm font-semibold text-red-700")
        lag["stalled"] = _lag_stalled  # type: ignore[assignment]
    lag["card"] = _lag_card  # type: ignore[assignment]

    def _update_lag_chart() -> None:
        """Push the latest rolling series into the persistent echart IN PLACE and
        toggle the card's states: the trend chart (>=2 points), a "caught up" line (CDC
        live but no trend to plot -- e.g. right after a session restore of a drained
        pipeline), the STALLED line instead of that (no datapoints because the sink is
        applying nothing -- the same input, opposite meaning), or fully hidden (CDC not
        streaming)."""
        option = build_lag_chart_option(
            getattr(migration_state, "cdc_replication_lag_series", None) or []
        )
        card, chart, empty = lag["card"], lag["chart"], lag["empty"]
        stalled_row = lag.get("stalled")
        activity = getattr(migration_state, "cdc_activity", None)
        stalled = bool(getattr(activity, "sink_stall_confirmed", False))
        if card is None or chart is None:
            return
        if option is not None:
            chart.options.clear()  # type: ignore[attr-defined]
            chart.options.update(option)  # type: ignore[attr-defined]
            chart.update()  # type: ignore[attr-defined]
            chart.set_visibility(True)  # type: ignore[attr-defined]
            if empty is not None:
                empty.set_visibility(False)  # type: ignore[attr-defined]
            if stalled_row is not None:
                stalled_row.set_visibility(False)  # type: ignore[attr-defined]
            card.set_visibility(True)  # type: ignore[attr-defined]
        elif _cdc_is_streaming(migration_state):
            # No trend, and the pipeline is live. Which line to show depends on WHY
            # there is no datapoint: caught up (sink applied everything) or stalled
            # (sink applied nothing) -- never claim the former when it is the latter.
            chart.set_visibility(False)  # type: ignore[attr-defined]
            if empty is not None:
                empty.set_visibility(not stalled)  # type: ignore[attr-defined]
            if stalled_row is not None:
                stalled_row.set_visibility(stalled)  # type: ignore[attr-defined]
            card.set_visibility(True)  # type: ignore[attr-defined]
        else:
            card.set_visibility(False)  # type: ignore[attr-defined]

    # The DLQ record table is rebuilt by every ~5s poll (it lives inside _cdc_live, which
    # refresh() clears), so any pagination/sort/filter hardcoded at build time is restored on
    # the next tick. This holder lives OUTSIDE the refreshable so the operator's choices
    # survive the rebuild -- the same shape as the Full Load progress table's
    # ``_progress_page`` (_full_load_ui.py), where the identical bug was fixed once already.
    # If a later refactor moves it inside _cdc_live the bug returns silently, which is what
    # the source-guard test pins.
    _dlq_record_page = {
        "page": 1,
        "rowsPerPage": _DLQ_RECORD_PAGE_SIZE,
        "sortBy": "when",
        "descending": True,
        "filter": "",
    }

    @ui.refreshable
    def _cdc_live() -> None:  # type: ignore[misc]
        view = _cdc_status_view(migration_state, job_manager)
        if view is not None:
            # Mirror CDC transitions (started / drift / DLQ growing / sink stalled) into
            # the AI feed, once each, so the assistant is aware of silent-loss risks.
            _announce_cdc_events(migration_state, view, ai_post_event)
            # Stop causes only for the connectors wired NOW: the status view is not cleared
            # on Stop CDC, and a cause kept for the Start card must not resurface here.
            _wired = set(getattr(migration_state, "cdc_connector_names", None) or [])
            _stack = getattr(migration_state, "cdc_stack_name", None)
            _render_cdc_pipeline_health(
                ui, view, getattr(migration_state, "cdc_activity", None),
                diagnoses={
                    n: d
                    for n, d in (
                        getattr(migration_state, "cdc_connector_diagnoses", None) or {}
                    ).items()
                    if n in _wired
                },
                log_group=f"/msk-connect/{_stack}-cdc" if _stack else None,
            )
            # PostgreSQL CDC: warn about source WAL pressure (the logical slot pinning
            # WAL) before the source disk fills. No-op for a MySQL source (no slot).
            _render_cdc_slot_health(ui, migration_state)
            # PostgreSQL re-keyed tables: a REPLICA IDENTITY dropped below FULL can lose
            # DELETEs silently (no dead letter), so it is flagged here from the throttled
            # background re-check, never read on this render.
            _render_cdc_replica_identity_notice(
                ui, migration_state, session, on_check_again=_check_identity_again
            )
            # A source DDL change CDC does not carry and no rejected row reveals (a dropped
            # nullable column, a widened type, a renamed table, ...): flagged from the
            # throttled background re-read of the captured tables' definitions, never read
            # on this render. A sibling of the dead-letter card, like the notice above.
            _render_cdc_catalog_drift_notice(
                ui, migration_state, session, view,
                on_check_again=_check_schema_again, on_fix=_fix_catalog_drift,
            )
            # A SIBLING of the dead-letter card, never inside it: these rows landed, so
            # rendering them within a panel titled "poison records" would invite exactly
            # the reading this signal exists to prevent. Above the card so an error-level
            # integrity finding is not filed under a "0 quarantined" all-clear.
            _render_cdc_value_conversion_notice(ui, migration_state)
            _render_cdc_dlq_panel(
                ui,
                migration_state,
                job_manager,
                view,
                on_refresh=_poll_cdc,
                session=session,
                cdc_ai_opener=cdc_ai_opener,
                lob_candidates_for=lob_candidates_for,
                exclude_columns=exclude_columns,
                page_state=_dlq_record_page,
            )
        else:
            controller = getattr(migration_state, "cdc_controller", None)
            names = getattr(migration_state, "cdc_connector_names", [])
            if controller is None or not names:
                # Wrap the waiting message in an info notice, not a bare label: every
                # other section on this screen is a bordered card/notice, so a loose
                # grey line here read as an unstyled gap. This is a normal pre-CDC
                # state (connectors not yet detected), so it is info, not a warning.
                render_notice(
                    ui,
                    tone="info",
                    icon="hourglass_empty",
                    header="No live pipeline yet",
                    body=(
                        "Live connector health and replication lag appear here once "
                        "the cdc-stack connectors are detected."
                    ),
                )
        if _cdc_is_streaming(migration_state):
            ui.timer(_CDC_POLL_INTERVAL_SECONDS, _poll_cdc, once=True)  # type: ignore[attr-defined]

    async def _poll_cdc() -> None:
        # The MSK Connect + CloudWatch reads are blocking network I/O; run them on
        # a worker thread (run.io_bound) so they never block the NiceGUI event loop.
        # Blocking the loop here previously starved the WebSocket keep-alive and
        # made the browser drop the connection. The pure view-build + state write
        # happens back on the loop after the fetch returns.
        from nicegui import run

        from dsql_migrator.ui.data_migration._cdc_status import land_detached

        # Scope the scan-free applied-ops metric read (InsertsApplied / UpdatesApplied /
        # DeletesApplied, from which the panel derives net rows) to the migrated table set.
        tables = _migration_status_tables(migration_state, job_manager)

        async def _fetch_and_land() -> None:
            # Detached from this timer (see land_detached): the worker has already moved
            # the DLQ cursor past the dead letters it read, so a poll cancelled by a refresh
            # or a step change must still fold them -- the cut-over gate counts on it.
            try:
                fetched = await run.io_bound(_fetch_cdc_status, migration_state, tables)
            except Exception:  # noqa: BLE001 - keep the last good view on any error
                fetched = None
            if fetched is not None:
                _apply_cdc_status(migration_state, fetched)

        await land_detached(_fetch_and_land)
        # PostgreSQL re-keyed tables: start the throttled REPLICA IDENTITY re-read (at most
        # every 2 minutes per session, single flight). Spawned, never awaited, so it cannot
        # delay this poll; the next tick renders what it found. No-op for MySQL.
        try:
            from dsql_migrator.ui.cdc_replica_identity_gate import (
                start_replica_identity_probe,
            )

            start_replica_identity_probe(session, migration_state)
        except Exception:  # noqa: BLE001 - monitoring must never break the poll
            _LOGGER.warning("REPLICA IDENTITY re-check could not start", exc_info=True)
        # Every captured table: start the throttled source-schema re-read (at most every 5
        # minutes per session, single flight), spawned and never awaited like the one above.
        # Inert without the builder's baseline handle or CDC evidence.
        try:
            from dsql_migrator.ui.cdc_catalog_drift_gate import start_catalog_drift_probe

            start_catalog_drift_probe(session, migration_state)
        except Exception:  # noqa: BLE001 - monitoring must never break the poll
            _LOGGER.warning("source schema re-check could not start", exc_info=True)
        _update_lag_chart()  # persistent chart: update in place (no flicker)
        _cdc_live.refresh()  # connector health / change flow / DLQ (text) redraw

    async def _check_identity_again() -> None:
        # "Check again" on the REPLICA IDENTITY notice: one forced read (still landed if this
        # handler is cancelled by a re-render), then redraw.
        from dsql_migrator.ui.cdc_replica_identity_gate import probe_replica_identity_now

        await probe_replica_identity_now(session, migration_state)
        _cdc_live.refresh()

    async def _check_schema_again() -> None:
        # "Check again" on the source-schema notice: one forced read (still landed if this
        # handler is cancelled by a re-render), then redraw.
        from dsql_migrator.ui.cdc_catalog_drift_gate import probe_catalog_drift_now

        await probe_catalog_drift_now(session, migration_state)
        _cdc_live.refresh()

    def _after_catalog_fix() -> None:
        # The Fix dialog calls this SYNCHRONOUSLY after an apply: force one re-read (the
        # recorded column changes the baseline), then redraw.
        try:
            from dsql_migrator.ui.cdc_catalog_drift_gate import start_catalog_drift_probe

            start_catalog_drift_probe(session, migration_state, force=True)
        except Exception:  # noqa: BLE001 - the next poll reads it anyway
            _LOGGER.warning("source schema re-check could not start", exc_info=True)
        _cdc_live.refresh()

    def _fix_catalog_drift(table: str):
        # Fix target schema… for a column the source added or renamed (from the
        # source-schema notice): the ADDED columns are recorded as part of the re-check's
        # baseline (a renamed one stays reported until Step 1 is re-run).
        return _open_add_column_dialog(
            ui, session, table, on_refresh=_after_catalog_fix,
            on_applied=getattr(migration_state, "record_cdc_target_added_columns", None),
            migration_state=migration_state,
        )

    _update_lag_chart()  # initial state (hidden until >=2 points)
    _cdc_live()

def _render_cdc_pipeline_health(
    ui,
    status_view: LoadStatusView,
    activity: "Optional[CdcActivitySummary]",
    *,
    diagnoses: "Optional[Mapping[str, object]]" = None,
    log_group: Optional[str] = None,
) -> None:
    """Render the combined "Pipeline health" card: connector health + change flow.

    Connector state/lag and the change-flow throughput both answer "is the pipeline
    running and moving data right now?", so they share one card (a labelled
    connector-health group, then a change-flow group), rather than two adjacent
    cards the operator has to mentally join. The DLQ stays its own card (it is about
    data set aside, a different concern). Renders nothing when there is no connector
    health to show (pre-streaming).

    ``diagnoses`` (connector name -> ``ConnectorLogDiagnosis``, matched by the poll from
    the worker log) names a stopped connector's cause on its row, and its runbook in an
    error notice under the connectors: the row's detail is one truncated line, and a
    stopped connector blocks progress. ``log_group`` is where the full error is. Reads
    only what the poll already stored (no AWS call on render).
    """
    held = dict(diagnoses or {})
    rows = connector_health_rows(
        status_view.connector_states,
        lag_seconds=status_view.lag_seconds,
        diagnoses={
            name: getattr(d, "headline", "") for name, d in held.items()
            if getattr(d, "headline", "")
        },
    )
    if not rows:
        return
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("monitor_heart", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Pipeline health").classes("text-sm font-semibold")  # type: ignore[attr-defined]

        # --- Connectors -------------------------------------------------------
        ui.label("Connectors").classes(  # type: ignore[attr-defined]
            "text-xs font-semibold text-gray-500 uppercase tracking-wide"
        )
        if status_view.caught_up_to is not None:
            ui.label(  # type: ignore[attr-defined]
                f"Caught up to {status_view.caught_up_to.isoformat()}"
            ).classes("text-xs text-gray-500")
        # Compact one-line-per-connector: status icon + friendly role label (raw
        # connector id in a hover tooltip) + a colour-coded state BADGE (green
        # "Running", etc.) for at-a-glance health + a muted detail. Keeps the minimal
        # list but restores the visible state badge (plain "streaming normally" text
        # read as too subtle); outline + title-case to match the other status chips.
        _badge_color = {"ok": "positive", "warn": "warning", "bad": "negative"}
        for row in rows:
            _border, _bg, icon_color, icon = _CDC_TONE_STYLE.get(
                row.tone, _CDC_TONE_STYLE["warn"]
            )
            with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.icon(icon, color=icon_color).classes("text-sm shrink-0")  # type: ignore[attr-defined]
                ui.label(row.label or row.name).classes(  # type: ignore[attr-defined]
                    "text-sm text-gray-800 shrink-0"
                ).tooltip(row.name)
                if row.state:
                    ui.badge(  # type: ignore[attr-defined]
                        row.state.title(), color=_badge_color.get(row.tone, "grey")
                    ).props("outline")
                ui.label(row.detail).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500 truncate"
                )
        # What to do about a stopped connector whose cause is known: the row above can
        # only name it, so the runbook goes in an error notice (it blocks progress).
        for row in rows:
            diagnosis = held.get(row.name)
            if row.state.upper() != "FAILED" or diagnosis is None:
                continue
            body = str(getattr(diagnosis, "guidance", "") or "")
            if log_group:
                body += f" The full error is in the CloudWatch log group {log_group}."
            render_notice(
                ui,
                tone="error",
                header=f"{row.label or row.name} stopped: {diagnosis.headline}",
                body=body,
            )

        # --- Change flow ------------------------------------------------------
        if activity is not None:
            ui.separator().classes("my-1")  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                ui.label("Change flow").classes(  # type: ignore[attr-defined]
                    "text-xs font-semibold text-gray-500 uppercase tracking-wide"
                )
                # Both the "what/why" and the data-source note move to a hover ⓘ so
                # the change-flow block is just the state line + rate gauges.
                ui.icon("info").classes(  # type: ignore[attr-defined]
                    "text-gray-400 text-sm cursor-help"
                ).tooltip(
                    "Whether changes are still streaming from the source to the "
                    "target. Rates are CloudWatch averages over about the last few "
                    "minutes, and they bottom out at an idle FLOOR of 0.1 rec/s -- the "
                    "shipped 5-minute heartbeat keeps them there. So reaching idle is "
                    "NOT proof the pipeline has drained: roughly up to 6 changes a "
                    "minute, and a sink that has stopped applying while the source "
                    "still produces, both look the same inside that band. Quiesce the "
                    "source, then confirm with Validation rather than with this line."
                )
            _render_change_flow_status(ui, activity)

# The confirmed-stall notice. Two causes look the same on the rate gauges, and only the sink
# connector log tells them apart: an ejected sink consumer never recovers by itself (Stop +
# Start rejoins the group), while a sink whose writes keep failing transiently -- retried by
# the sink and redelivered by Connect, never dead-lettered -- resumes once the cause clears.
# Quotes the sink's own log prefix (DsqlSinkTask.transientRetryException) and splits the
# guidance by the SQLSTATE it carries: only XX000 is Aurora DSQL's own temporary condition
# (G22); 08xxx / 57xxx / null is the sink's path to the cluster (a security group, NAT or
# VPC endpoint change, or a deleted cluster: pgjdbc reports a failed connect as 08001);
# 28xxx / 53300 / 53400 is a connection Aurora DSQL refused to open, retried since sink
# plugin v54 (G126; dead-lettered row by row before); and a lasting 40001 is another writer
# on the same rows -- never the sink's own tasks, which never write the same row (each row's
# changes travel on one partition).
SINK_STALL_NOTICE_BODY = (
    "The source is producing changes but the sink has applied none, so the target is "
    "falling behind. The connector can still report RUNNING and no errored task while this "
    "is happening, so RUNNING is not evidence that replication works — compare target row "
    "counts. Then check the sink connector log. A repeating \"Commit of offsets timed out\" "
    "means the sink consumer was ejected from its group (it keeps its thread alive) and the "
    "gap will not close on its own: Stop CDC and Start CDC to rejoin the group. A repeating "
    "\"DSQL apply failed (transient sqlstate=\" means the sink keeps retrying writes that "
    "fail, and the SQLSTATE after it names why. XX000 (\"server unavailable\") is a "
    "temporary Aurora DSQL condition: the sink resumes on its own once Aurora DSQL answers; "
    "if it lasts, contact AWS Support. 08xxx, 57xxx or null means the sink cannot reach the "
    "cluster or keeps losing its connection: check that the cluster still exists, that the "
    "connector subnets still reach its endpoint on port 5432 (security group, NAT gateway "
    "or Aurora DSQL VPC endpoint and its DNS), and that the connector's IAM role is still "
    "allowed to connect; the sink resumes once it can connect. 28xxx, 53300 or 53400 means "
    "Aurora DSQL refused to open the sink's connection: 28xxx its IAM token (check that the "
    "connector's IAM role is still allowed to connect), 53300 the cluster's limit of open "
    "connections (close idle connections elsewhere, or raise the quota), 53400 its rate of "
    "new connections; the sink resumes once a connection opens. 40001 means write conflicts "
    "outlast the sink's retries: something else is writing the same rows on Aurora DSQL — "
    "stop it until cut over, and the sink resumes. In every case, do NOT cut over until the "
    "sink send rate recovers."
)


def _render_change_flow_status(ui, activity: "CdcActivitySummary") -> None:
    """The change-flow state line + CloudWatch throughput (inner block, no card).

    Pure information for the operator's cutover judgement — NOT a gate or a
    recommendation. Honest about "unknown": when a rate is unavailable it is shown
    as such, never as 0/idle.
    """
    def _fmt(rate: "Optional[float]") -> str:
        # Unit = change-event RECORDS per second (SourceRecordPollRate /
        # SinkRecordSendRate); spell "rec/s" so a bare "/s" is not ambiguous.
        return f"{rate:.2f} rec/s" if rate is not None else "unknown"

    with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        if activity.idle is True:
            # NOT green, and NOT "no changes". `idle` means both CloudWatch rates are at or
            # below _CDC_IDLE_RATE_THRESHOLD (0.1 rec/s) -- a floor the shipped 5-minute
            # heartbeat genuinely sits on, so it cannot be lowered. Inside that band a real
            # change stream and a fully stalled sink are INDISTINGUISHABLE: measured live, 6
            # change events in 72s gave source 0.06 rec/s and this line still rendered the
            # strongest possible all-clear directly above its own gauge showing that rate.
            # So the line states what was measured and what it cannot rule out, and the
            # colour is neutral -- an operator timing cut-over must not read "drained" here.
            ui.icon("pause_circle").classes("text-base text-sky-600")  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "At or below the idle floor "
                f"({_fmt(activity.source_poll_rate)} in, {_fmt(activity.sink_send_rate)} out)"
                " — up to ~6 changes/min is indistinguishable from drained"
            ).classes("text-sm text-gray-700")
        elif activity.sink_stall_confirmed:
            # Checked BEFORE the "streaming" branch: a stalled sink is not idle, so it
            # used to fall through to "Streaming — changes are flowing" — asserting the
            # pipeline was healthy off the SOURCE rate alone while nothing reached DSQL.
            ui.icon("error", color="negative").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Sink stalled — changes are NOT reaching DSQL").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-red-700"
            )
        elif activity.idle is False:
            ui.icon("sync", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Streaming — changes are flowing").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-700"
            )
        else:
            ui.icon("help_outline", color="grey").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Activity unknown").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
    if activity.sink_stall_confirmed:
        # The divergence is already on screen as two bars (source > 0, sink 0) -- what
        # was missing is reading it. Say what it means and what to do, per the project's
        # "what happened, what to do next" rule: the connector will still show RUNNING,
        # so the operator needs to be told not to trust that.
        render_notice(
            ui,
            tone="error",
            header="The sink is not applying changes",
            body=SINK_STALL_NOTICE_BODY,
        )
    # Visual rate gauges: source poll vs sink send on the SAME scale, so at a glance
    # you can see whether the sink is keeping up with the source (matched bars) or
    # falling behind (shorter sink bar). Unknown rates show "unknown" with no bar.
    sp, ss = activity.source_poll_rate, activity.sink_send_rate
    scale = max([r for r in (sp, ss) if r is not None] or [0.0])

    def _rate_bar(label: str, rate: "Optional[float]") -> None:
        # Compact fixed-width row: label + a FIXED-width bar (not flex-1, so it never
        # stretches to the card edge) + the value right after it. Indent with pl-6
        # (padding, inside the width) NOT ml-6 (margin, which added to a w-full row
        # pushed the trailing value outside the Pipeline health card). No w-full → the
        # row sizes to its content and stays within the card.
        with ui.row().classes("items-center gap-2 no-wrap pl-6"):  # type: ignore[attr-defined]
            ui.label(label).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-600 shrink-0"
            ).style("width: 76px")
            with (
                ui.element("div")  # type: ignore[attr-defined]
                .classes("relative h-2 rounded bg-gray-200 shrink-0")
                .style("width: 140px")
            ):
                if rate is not None and scale > 0:
                    pct = max(3.0, min(100.0, rate / scale * 100.0))
                    ui.element("div").classes(  # type: ignore[attr-defined]
                        "absolute inset-y-0 left-0 rounded bg-sky-500"
                    ).style(f"width: {pct:.1f}%")
            ui.label(_fmt(rate)).classes(  # type: ignore[attr-defined]
                "text-xs font-mono text-gray-700 shrink-0"
            )

    _rate_bar("Source poll", sp)
    _rate_bar("Sink send", ss)
    # Data-source/freshness note moved to the "Change flow" header ⓘ tooltip; the
    # gauges stand on their own here (no standing provenance caption).

# Health level (assess_dlq_health) -> notice tone + status badge (label, quasar
# color) for the DLQ panel, so the panel speaks the same severity language as the
# rest of the app (NOTICE_STYLE / status badges) instead of a bespoke palette:
# clean = success, sporadic poison = info, approaching threshold = warning,
# systematic = error.
_DLQ_LEVEL_TONE: dict[str, str] = {
    "ok": "success",
    "warn": "warning",
    "alarm": "error",
}

def _dlq_panel_tone(health, *, sink_stalled: bool = False) -> str:
    """Map a :class:`DlqHealth` to a notice tone.

    ``ok`` with a non-zero depth is downgraded to ``info`` (sporadic, isolated
    poison is an FYI, not a success), while a truly clean stream (depth 0) stays
    ``success``. Calibrated to the project's severity rules (info/warning/error).

    ``sink_stalled`` blocks the ``success`` upgrade: depth is counted from quarantine
    log lines, and a sink that has stopped applying never reaches a record to
    quarantine -- so depth 0 means "nothing was even attempted", not "nothing went
    wrong". Painting that green made the panel assert an all-clear during total data
    loss. Downgraded to ``info`` (the stall itself is reported, loudly, by the change-
    flow signal; this panel just must not contradict it).
    """
    if health.level == "ok":
        return "success" if health.depth == 0 and not sink_stalled else "info"
    return _DLQ_LEVEL_TONE.get(health.level, "info")


_CDC_DLQ_SEED = (
    "Triage these dead-lettered CDC records: what are the root causes (by SQLSTATE / "
    "table), and exactly how do I fix them and backfill the affected tables before "
    "cut over?"
)
_CDC_DRIFT_SEED = (
    "The source schema changed and CDC dead-lettered the new rows. For each affected "
    "table, tell me exactly what DDL to apply to the target and how to backfill the "
    "set-aside rows — and whether I must stop CDC first."
)
# The value-fit-only banner's seed: those rows need not come from any source DDL (a MySQL
# TINYINT(1) can hold 2), and Aurora DSQL has no ALTER COLUMN ... TYPE to "apply".
_CDC_VALUE_FIT_SEED = (
    "CDC set rows aside because a value does not fit its Aurora DSQL column (not necessarily "
    "a source schema change). For each affected table, tell me how to give the column a type "
    "that holds these values — Aurora DSQL has no ALTER COLUMN ... TYPE — and how to backfill "
    "the set-aside rows, and whether I must stop CDC first."
)
# The target-side banner's seed: Aurora DSQL refused the rows (a unique index, a missing
# target table, a value the sink cannot bind) -- no source DDL to "apply", though a 23505 can
# also come from a source that dropped or relaxed a UNIQUE index, hence "not necessarily".
_CDC_TARGET_SIDE_SEED = (
    "CDC dead-lettered rows that Aurora DSQL or the sink refused on the target (not "
    "necessarily a source schema change). For each affected table, tell me why the rows were "
    "refused and how to restore them on the target, and whether I must stop CDC first."
)
# A source TRUNCATE (extra rows on the target) is no schema change, and nothing is backfilled.
_CDC_TRUNCATE_SEED = (
    "CDC could not replicate a source TRUNCATE (not a schema change; Aurora DSQL has no "
    "TRUNCATE statement), so the target still holds rows the source no longer has. For each "
    "affected table, tell me how to remove those rows from the target, and whether I must "
    "stop CDC first."
)
_CDC_TRUNCATE_VALUE_FIT_SEED = (
    "CDC could not replicate a source TRUNCATE (not a schema change), and it set rows aside "
    "because a value does not fit its Aurora DSQL column (not necessarily a source schema "
    "change either). For each affected table, tell me how to remove the rows the source no "
    "longer has, how to give the column a type that holds these values — Aurora DSQL has no "
    "ALTER COLUMN ... TYPE — and backfill the set-aside rows, and whether I must stop CDC "
    "first."
)


def _drift_ai_case(drift) -> str:
    """Which AI seed, tooltip and facts lead a banner over ``drift`` gets. Pure.

    ``"ddl"`` while any group is a source DDL kind (or there is none); else the banner claims
    no schema change: ``"target-side"`` with any target-side kind, otherwise
    ``"value-fit"``, ``"truncate"`` or ``"truncate-value-fit"``. The banner computes it once
    from the SAME (open) groups it shows and passes those groups to :func:`_cdc_drift_facts`,
    so the seed, the tooltip and the facts can never disagree."""
    kinds = {getattr(g, "kind", "") for g in drift}
    if kinds == {"generated-column"}:
        return "generated"  # a target-side refusal with its own repair, no source change
    if not kinds or kinds - _NON_DDL_SOURCE_KINDS - _TARGET_SIDE_DRIFT_KINDS - {"generated-column"}:
        return "ddl"
    # A GENERATED column alongside other kinds is one more target-side refusal: never the
    # TRUNCATE / value-fit case, which would claim a source TRUNCATE that never happened.
    if kinds & (_TARGET_SIDE_DRIFT_KINDS | {"generated-column"}):
        return "target-side"
    if kinds == {"value-does-not-fit"}:
        return "value-fit"
    if kinds == {"source-truncate"}:
        return "truncate"
    return "truncate-value-fit"


_CDC_GENERATED_SEED = (
    "CDC dead-lettered every insert and update for a table because its Aurora DSQL column is "
    "GENERATED and the change records carry a value for it (SQLSTATE 428C9; not a source "
    "schema change). Tell me how to make the column ordinary (ALTER TABLE ... ALTER COLUMN "
    "... DROP EXPRESSION), how to restore the rows set aside, and whether I must stop CDC first."
)

_DRIFT_AI_SEED = {
    "generated": _CDC_GENERATED_SEED,
    "ddl": _CDC_DRIFT_SEED,
    "value-fit": _CDC_VALUE_FIT_SEED,
    "target-side": _CDC_TARGET_SIDE_SEED,
    "truncate": _CDC_TRUNCATE_SEED,
    "truncate-value-fit": _CDC_TRUNCATE_VALUE_FIT_SEED,
}
_DRIFT_AI_TOOLTIP = {
    "generated": (
        "Ask AI DBA how to make the GENERATED column ordinary and restore the set-aside rows."
    ),
    "ddl": "Ask AI DBA what DDL to apply and how to backfill the drifted rows.",
    "value-fit": (
        "Ask AI DBA how to give the column a type that holds these values and backfill the "
        "set-aside rows."
    ),
    "target-side": "Ask AI DBA why the target refused these rows and how to restore them.",
    "truncate": "Ask AI DBA how to remove the rows the source no longer has.",
    "truncate-value-fit": (
        "Ask AI DBA how to remove the rows the source no longer has and give the column a "
        "type that holds these values."
    ),
}
# The facts are "authoritative" to the model: each lead claims only what its banner claims.
_DRIFT_AI_LEAD = {
    "generated": (
        "CDC dead-lettered every insert and update: a GENERATED Aurora DSQL column was sent a "
        "value (SQLSTATE 428C9; not a source schema change):"
    ),
    "ddl": "Source schema drift detected (CDC does not replicate DDL):",
    "value-fit": (
        "CDC set rows aside: a value does not fit its Aurora DSQL column (not necessarily a "
        "source schema change; CDC does not replicate DDL):"
    ),
    "target-side": (
        "CDC dead-lettered rows the target refused (not necessarily a source schema change; "
        "the dead-lettered rows are never replayed):"
    ),
    "truncate": (
        "A source TRUNCATE was not replicated (not a schema change; Aurora DSQL has no "
        "TRUNCATE statement, so the target keeps the rows the source removed):"
    ),
    "truncate-value-fit": (
        "A source TRUNCATE was not replicated (not a schema change), and CDC set rows aside "
        "because a value does not fit its Aurora DSQL column (not necessarily a source schema "
        "change):"
    ),
}


def _cdc_dlq_facts(migration_state, log_key: str, job_manager=None) -> str:
    """Assemble a credential-free, row-free DLQ facts block for the AI DBA chat.

    Reuses the CDC-assist error summarizer over the CDC-sourced dead-letter records
    (depth + top tables + top SQLSTATEs + a capped sample), so the chat is grounded on
    the real poison records without any row values leaving the tool (Property 7). It also
    states the migration type, whether a Full Load ran, and the exclusion lock's own steps
    -- the runbook in the grounding depends on them (no Full Load: nothing to Change &
    reload, no watermark to re-seed from)."""
    from collections import Counter

    from dsql_migrator.core.cdc_assist import _summarize_errors
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_records

    records = list(cdc_dlq_records(migration_state, log_key))
    lines = [f"CDC dead-letter queue: {len(records)} poison record(s)."]
    by_table = Counter(r.table for r in records)
    if by_table:
        lines.append(
            "Top tables: "
            + ", ".join(f"{t} ({n})" for t, n in by_table.most_common(8))
        )
    by_code = Counter(
        str(getattr(r, "error_code", "") or "")
        for r in records
        if getattr(r, "error_code", None)
    )
    if by_code:
        lines.append(
            "Top SQLSTATEs: "
            + ", ".join(f"{c} ({n})" for c, n in by_code.most_common(5))
        )
    # The tables the CDC step calls verified (every one a CHECKSUM proves after its last dead
    # letter, whatever else holds it), with the open finding still holding any of them at Cut
    # over, so the chat never advises reloading them.
    try:
        from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_open_finding_holds

        verified = _cdc_dlq_proven(migration_state)
        held = cdc_dlq_open_finding_holds(migration_state, verified)
    except Exception:  # noqa: BLE001 - advisory facts only
        verified, held = {}, {}
    if verified:
        lines.append(_verified_facts_line(verified, held))
    activity = getattr(migration_state, "cdc_activity", None)
    if bool(getattr(activity, "sink_stall_confirmed", False)):
        lines.append(
            "Sink stalled: yes (a zero DLQ depth would be EXPECTED during a stall)."
        )
    lines.extend(_exclusion_state_facts(migration_state, job_manager))
    lines.append("Sample dead-lettered events:")
    # Each reason as the record list shows it (cdc_dlq_shown_message): the facts are
    # authoritative to the chat, so a source TRUNCATE's sink clause "reload this table on the
    # Full Load step" must not reach it -- on CDC only there is no such step.
    # Row-free (Property 7): the shown message still carries the record's pk, its SQL and,
    # from a pre-v29 sink, pgjdbc's "Failing row contains (...)" -- all of which went to
    # Bedrock here while the chat's DLQ tool stripped them.
    from types import SimpleNamespace

    from dsql_migrator.ui.ai_tools import ai_safe_dlq_message

    lines.append(_summarize_errors([
        SimpleNamespace(
            table=r.table, error_code=getattr(r, "error_code", None),
            message=ai_safe_dlq_message(cdc_dlq_shown_message(r)),
        )
        for r in records
    ]))
    return "\n".join(lines)


def _exclusion_state_facts(migration_state, job_manager) -> "list[str]":
    """The migration type, whether a Full Load ran, and -- when the exclusions are locked
    -- the lock's own steps, for the AI facts. Best effort: a failed lookup drops only
    these lines, never the chat."""
    from dsql_migrator.ui.data_migration._models import _MIGRATION_TYPE_META

    out: "list[str]" = []
    try:
        mtype = getattr(migration_state, "migration_type", None)
        meta = _MIGRATION_TYPE_META.get(mtype)
        if meta is not None:
            out.append(f"Migration type: {meta.label}.")
        ran = getattr(migration_state, "job_id", None) is not None
        out.append(f"Full Load run recorded in this session: {'yes' if ran else 'no'}.")
        if job_manager is not None:
            lock = exclusion_lock_state(migration_state, job_manager)
            if lock.locked and lock.reason:
                out.append("Exclusion lock (authoritative; give the operator THESE steps): "
                           + lock.reason)
    except Exception:  # noqa: BLE001 - advisory facts only
        return out
    return out


def _cdc_drift_facts(
    status_view: LoadStatusView,
    *,
    drift=None,
    verified: "Optional[Mapping[str, datetime]]" = None,
    held: "Optional[Mapping[str, Sequence[str]]]" = None,
) -> str:
    """Assemble a credential-free schema-drift facts block for the AI DBA chat.

    The model is told these facts are authoritative, so the lead claims only what the banner
    claims (:func:`_drift_ai_case`). ``drift`` (default: the view's) is the banner's OPEN
    groups -- the ones its seed was chosen from; a verified table's target-side and truncate
    groups are not among them -- and ``verified`` the tables verified by CHECKSUM
    (:func:`_cdc_dlq_proven`), named on one line so the model never advises reloading them
    again (or deleting a truncate's rows) for those, while any open group of theirs keeps its
    fix, and ``held`` the open finding still holding any of them at Cut over
    (:func:`~dsql_migrator.ui.cdc_dlq_gate.cdc_dlq_open_finding_holds`), as the banner's
    Verified notice names it."""
    if drift is None:
        drift = getattr(status_view, "schema_drift", None) or []
    lines = [_DRIFT_AI_LEAD[_drift_ai_case(drift)]]
    for group in drift:
        noun = "record" if group.count == 1 else "records"
        columns = list(getattr(group, "columns", None) or ())
        # A check-violation group names its CHECK constraint and a unique-conflict group its
        # unique index (a constraint name, not a column); every other kind names columns.
        what = (
            "constraint"
            if getattr(group, "kind", "") in ("check-violation", "unique-conflict")
            else "columns"
        )
        detail = f" ({what}: {', '.join(columns)})" if columns else ""
        codes = list(getattr(group, "codes", None) or ())
        coded = f" [codes: {', '.join(codes)}]" if codes else ""
        lines.append(
            f"- {group.table}: {group.kind}{detail}{coded} — {group.count} {noun} dead-lettered"
        )
    if any(getattr(g, "kind", "") == "unique-conflict" for g in drift):
        # The one source-side cause of a 23505 that nothing else tells the model: no DDL is
        # replicated, and the source schema re-check compares no index.
        lines.append(_UNIQUE_SOURCE_CAUSE_FACT)
    if verified:
        lines.append(_verified_facts_line(verified, held))
    return "\n".join(lines)


_UNIQUE_SOURCE_CAUSE_FACT = (
    "A unique-conflict (23505) can also mean the source dropped or relaxed a UNIQUE index that "
    "Aurora DSQL still has (CDC replicates no DDL, and the source schema re-check compares no "
    "index): then drop it on Aurora DSQL too."
)


# What a verified table's CHECKSUM proves, for the AI facts: its rows match the source, so a
# dead letter whose whole repair is putting the rows right (_CHECKSUM_REPAIRED_DRIFT_KINDS: a
# target-side one, or a source TRUNCATE's extra rows) needs no other; any other kind still
# listed for it keeps the fix its banner group shows (a CHECKSUM proves no schema fix),
# exactly as the banner's per-group rule shows it.
_VERIFIED_FACTS_LEAD = (
    "Verified by CHECKSUM, so Cut over's dead-letter check no longer holds them, and their "
    "unique-conflict, missing-table, unbindable-value or source-truncate dead letters need no "
    "repair (do not advise a reload or a delete for them; any other kind listed for them "
    "keeps its own fix): "
)


def _verified_facts_line(
    verified: "Mapping[str, datetime]",
    held: "Optional[Mapping[str, Sequence[str]]]" = None,
) -> str:
    """The AI facts' line naming the tables verified by CHECKSUM, then -- as the banner's
    Verified notice does -- the open finding that still holds any of them at Cut over
    (``held``; names and times only)."""
    from dsql_migrator.ui.cdc_dlq_gate import (
        cdc_dlq_still_held_sentence,
        cdc_dlq_verified_clauses,
    )

    line = _VERIFIED_FACTS_LEAD + cdc_dlq_verified_clauses(verified, limit=_FIX_NAME_LIMIT)
    still = cdc_dlq_still_held_sentence(held or {}, limit=_FIX_NAME_LIMIT, pointer=False)
    return f"{line}. {still}" if still else line


def _render_cdc_ai_button(ui, on_click, tooltip: str) -> None:
    """The shared 'Ask AI DBA' affordance for the CDC DLQ / drift panels."""
    ui.button("Ask AI DBA", on_click=on_click).props(  # type: ignore[attr-defined]
        "flat dense no-caps size=sm color=indigo-6 icon=auto_awesome"
    ).tooltip(tooltip)


def _cdc_dlq_proven(migration_state) -> "dict[str, object]":
    """The dead-lettered tables the cut-over dead-letter check no longer holds -- the ones the
    CDC step calls verified by CHECKSUM, whatever else holds them (``{}`` when unknown)."""
    if migration_state is None or not getattr(migration_state, "cdc_dlq_ledger", None):
        return {}
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_proven_tables

    provider = getattr(migration_state, "validation_state_provider", None)
    try:
        validation_state = provider() if callable(provider) else None
    except Exception:  # noqa: BLE001 - without it every table simply stays open
        validation_state = None
    try:
        return cdc_dlq_proven_tables(migration_state, validation_state)
    except Exception:  # noqa: BLE001 - a resolved line is a convenience, never a gate
        return {}


def _render_cdc_dlq_resolved(ui, groups, verified, held=None) -> None:
    """One success notice naming the tables of ``groups`` -- the target-side and truncate
    groups a CHECKSUM verified after their table's last dead letter (a reload, a later
    full-row change of each row, or a delete of the rows the source dropped repaired them) --
    with one sentence naming the open finding (``held``) that still holds any of them at Cut
    over."""
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_resolved_body

    tables = sorted({g.table for g in groups})
    render_notice(
        ui,
        tone="success",
        header="Verified by CHECKSUM",
        body=cdc_dlq_resolved_body(
            {t: verified[t] for t in tables},
            {t: checks for t, checks in (held or {}).items() if t in tables},
        ),
    )


def _render_cdc_schema_drift_banner(
    ui,
    status_view: LoadStatusView,
    session=None,
    on_refresh=None,
    cdc_ai_opener=None,
    saved_exclusions: "Optional[Mapping[str, object]]" = None,
    migration_type=None,
    on_applied=None,
    migration_state=None,
) -> None:
    """Flag source DDL the target has not caught up to (from the classified DLQ).

    CDC does not propagate DDL: when the source alters a table, the first row under
    the new schema is rejected by DSQL and dead-lettered. The control plane derives
    the drift kind from the quarantine SQLSTATE and record shape (see
    ``classify_schema_drift``); this band names the affected table(s) + what changed and
    points at the manual recovery, so a reader is not left to reverse-engineer a rising DLQ
    count. Each DDL kind present gets its own repair that Aurora DSQL supports
    (:func:`_ddl_runbook`), and a 23502 / 42P10 row takes the source schema re-check's label
    when an open finding confirms it (:func:`_catalog_label`, state only). Renders
    nothing when no drift was detected (the common case), so it is inert on a healthy
    stream. Every runbook follows ``migration_type`` (CDC only has no Full
    Load step to reload from). ``on_applied`` is handed to the Fix dialog, which reports
    the columns it added (the source-schema re-check's baseline records them), and so is
    ``migration_state`` (the dialog's key, exclusion and rename inputs; without it the dialog
    adds and renames nothing).
    """
    drift = getattr(status_view, "schema_drift", None) or []
    if not drift:
        return
    # A table a CHECKSUM match in a comparison started after its last dead letter proves (the
    # cut-over dead-letter check's own rule; a reload or a later full-row change repaired it)
    # has its TARGET-SIDE and TRUNCATE groups shown as verified, not as an open "Stop CDC and
    # reload" error or a "the target still holds the rows" warning: their whole repair is
    # putting the rows right, which the CHECKSUM proves done (_CHECKSUM_REPAIRED_DRIFT_KINDS).
    # Whatever else holds the table (G25): an OPEN REPLICA IDENTITY or source schema finding
    # holds Cut over on its own and has its own fix (a reload fixes neither), so the Verified
    # notice names it in one sentence (cdc_dlq_open_finding_holds) rather than sending the
    # table back to a reload. Its value-fit and DDL groups keep their own headers and runbooks
    # (a CHECKSUM proves no schema fix). Its dead-lettered records stay listed in the breakdown
    # below the banner. A newer dead letter moves the ledger's last time past that comparison
    # and reopens it.
    from dsql_migrator.ui.cdc_dlq_gate import cdc_dlq_open_finding_holds

    _verified = _cdc_dlq_proven(migration_state)
    _held = cdc_dlq_open_finding_holds(migration_state, _verified)
    _resolved = [
        g for g in drift
        if getattr(g, "kind", "") in _CHECKSUM_REPAIRED_DRIFT_KINDS and g.table in _verified
    ]
    if _resolved:
        drift = [g for g in drift if not any(g is r for r in _resolved)]
        if not drift:
            _render_cdc_dlq_resolved(ui, _resolved, _verified, _held)
            return
    # THREE cases, not two. The old two-way split put every target-side kind under the
    # "Target table missing — rows are being lost" header, which is false for 23505 (there a
    # row only CAN end up missing) and was about to be false for 07006 as well. Mixed drift
    # reports the most severe.
    _kinds = {getattr(g, "kind", "") for g in drift}
    _target_side = [
        g for g in drift if getattr(g, "kind", "") in _TARGET_SIDE_DRIFT_KINDS
    ]
    _certain_loss = bool(_kinds & _CERTAIN_LOSS_DRIFT_KINDS)
    # FOUR cases now: a source TRUNCATE is neither a DDL change nor a target-side loss, so it
    # needs its own header and its own runbook line. Only when it is the ONLY kind present --
    # mixed with a DDL change it keeps the (more severe) DDL header, the per-table line below
    # still states the truncate in full, and _ddl_runbook adds the TRUNCATE repair for it.
    _truncate_only = _kinds == {"source-truncate"}
    # FIVE: a value the sink refused because its column would change it is not a schema change
    # either (a MySQL TINYINT(1) can hold 2 with no DDL), so it too gets its own header and
    # runbook when it is the only kind. `warning`: real, but the stream keeps running and the
    # cut-over gate holds each such table until a CHECKSUM match. No Fix button.
    _value_fit_only = _kinds == {"value-does-not-fit"}
    # SIX: those two together (and nothing else) are still no DDL change, so the banner names
    # both and gives the TRUNCATE runbook plus the value-fit one -- never the generic DDL one.
    _no_ddl = _kinds <= _NON_DDL_SOURCE_KINDS
    from dsql_migrator.ui.cdc_replica_identity_gate import _is_postgres

    _is_pg = _is_postgres(session)
    # SEVEN: a 42P10 (the records' key matches no Aurora DSQL unique key) loses EVERY insert
    # and update for the table -- certain, ongoing loss, so `error` like 07006 -- but its table
    # exists, so it is not target-side and never gets the dropped-table notice below.
    _key_loss = "key-change" in _kinds
    # EIGHT: a 428C9 (a GENERATED target column was sent a value) loses every insert and
    # update for the table too, and is no source change at all.
    _generated_loss = "generated-column" in _kinds
    # `error` for certain ongoing loss AND for the target-side kinds generally: a row that
    # may have vanished is not "be aware" either. `warning` is left to a source DDL change,
    # which is recoverable by applying the DDL.
    _tone = "error" if _target_side or _key_loss or _generated_loss else "warning"
    if _certain_loss:
        _header = (
            "Rows are being lost — the sink cannot write this table"
            if "unbindable-value" in _kinds
            else "Target table missing — rows are being lost"
        )
    elif _key_loss:
        _header = "Rows are being lost — the key no longer matches the Aurora DSQL primary key"
    elif _generated_loss:
        _header = "Rows are being lost — a GENERATED Aurora DSQL column was sent a value"
    elif _target_side:
        _header = "Rows may be missing from the target"
    elif _truncate_only:
        # Not a schema change, so it must not be announced as one -- and the direction is the
        # reverse of every other kind (extra rows on the target, not missing ones). Stays
        # `warning` by the design system's calibration: real, but the stream keeps running and
        # the cut-over gate already blocks on Validation's verdict -- and also holds each
        # dead-lettered table until a CHECKSUM comparison started after its last record.
        _header = "Source TRUNCATE was not replicated"
    elif _value_fit_only:
        _header = "Rows set aside: a value does not fit its Aurora DSQL column"
    elif _no_ddl:
        _header = (
            "Source TRUNCATE was not replicated, and a value does not fit its Aurora DSQL "
            "column"
        )
    else:
        _header = "Source schema change detected"
    with notice_panel(ui, tone=_tone, header=_header, icon="schema") as (header_row, _panel):
        if cdc_ai_opener is not None:
            with header_row:
                ui.space()  # type: ignore[attr-defined]
                # A banner that claims no schema change gets a chat that does not either (the
                # seed becomes the operator's first message): the seed, the tooltip and the
                # facts all come from the open groups shown here, so they cannot disagree,
                # and the facts name the tables already verified (never "reload them again").
                _ai_case = _drift_ai_case(drift)
                _render_cdc_ai_button(
                    ui,
                    lambda: cdc_ai_opener(
                        "drift",
                        _cdc_drift_facts(
                            status_view, drift=drift, verified=_verified, held=_held
                        ),
                        _DRIFT_AI_SEED[_ai_case],
                    ),
                    _DRIFT_AI_TOOLTIP[_ai_case],
                )
        for group in drift:
            label = _DRIFT_LABELS.get(group.kind, (group.kind, ""))[0]
            # The source schema re-check's own label when an open finding confirms this
            # group ("column dropped at the source", ...); the why stays record-level.
            label = _catalog_label(group, migration_state) or label
            why = _drift_why(group, is_pg=_is_pg, cdc_only=_fix_is_cdc_only(migration_type))
            noun = "record" if group.count == 1 else "records"
            # The value-free names the group's reasons give: a value-fit "<column> <type>",
            # the 23502 column, the 23514 constraint (never a value).
            _columns = list(getattr(group, "columns", None) or ())
            # 23502 is ALSO what an excluded column produces when the target still
            # requires it (NOT NULL, no DEFAULT): capture drops it from the event, so the
            # sink sends no value. When the records name the column not sent, that name
            # decides (names compare exactly): an excluded one IS the cause, and one the
            # exclusions do not cover rules them out. With no name (or the names capped),
            # say so for a table with saved exclusions, rather than only blaming a source
            # DDL change that may never have happened.
            _excluded = sorted((saved_exclusions or {}).get(group.table) or ())
            if group.kind == "drop-column" and _excluded:
                _hit = [c for c in _columns if c in set(_excluded)]
                if _hit and len(_hit) == len(_columns):
                    why = (
                        "the change records do not carry this column: this table's saved "
                        "column exclusions keep it out, and the Aurora DSQL table still "
                        "requires it (NOT NULL, no DEFAULT), so DSQL rejected the rows "
                        "(SQLSTATE 23502)"
                    )
                elif _hit:
                    why = (
                        f"{why}; this table's saved column exclusions also keep out "
                        f"{', '.join(_hit)}"
                    )
                elif not _columns or len(_columns) >= _VALUE_FIT_COLUMN_LIMIT:
                    why = (
                        f"{why}; or, since this table has saved column exclusions "
                        f"({', '.join(_excluded)}), an excluded column the target still "
                        "requires (NOT NULL, no DEFAULT) — it is not sent, so the same "
                        "rejection follows"
                    )
            _joiner = ", "
            if group.kind == "key-change" and not _columns:
                # The confirming re-check finding's key facts ("primary key was (id), now
                # (code)"), so "restore the original key" says which key.
                from dsql_migrator.ui.cdc_catalog_drift_gate import change_fact

                _columns = [
                    change_fact(c)
                    for c in _catalog_confirming_changes(group, migration_state)[:_FIX_NAME_LIMIT]
                ]
                _joiner = "; "  # a key fact lists columns with commas itself
            _named = f" ({_joiner.join(_columns)})" if _columns else ""
            with ui.row().classes("w-full items-center gap-2"):  # type: ignore[attr-defined]
                ui.label(  # type: ignore[attr-defined]
                    f"{group.table}: {label}{_named} — {why}. {group.count} {noun} "
                    "dead-lettered."
                ).classes("text-xs text-gray-800")
                # An added or renamed column (42703) is the only drift we offer to fix:
                # the dialog ADDs a new column or RENAMEs a renamed one, after approval of
                # the rendered DDL (Property 6: never a silent schema mutation), and
                # refuses a key change or a column the exclusions keep out with what to
                # do instead. A DROP/type change can rewrite or destroy target data, so
                # those stay alert-only -- see the runbook line below. Hidden entirely
                # when no session is wired (e.g. unit tests).
                if group.kind == "add-column" and session is not None:
                    ui.button(  # type: ignore[attr-defined]
                        "Fix target schema…",
                        on_click=lambda _e=None, table=group.table: (
                            _open_add_column_dialog(
                                ui, session, table, on_refresh=on_refresh,
                                on_applied=on_applied, migration_state=migration_state,
                            )
                        ),
                    ).props("flat dense no-caps size=sm color=primary")
        # One shared runbook line: CDC cannot apply the DDL for you (Property 6). A TRUNCATE-only
        # banner gets its OWN line: there is no DDL to apply and no set-aside row to backfill --
        # the target has too MANY rows, and an append (upsert-only) reload cannot remove a row
        # the source no longer has, so the remedy must name Drop & reload specifically. It says
        # Stop CDC FIRST because the Full Load step runs no reload while CDC is live (G129;
        # nor does it offer Drop & reload then). CDC only has no Full Load step at all, so
        # its remedy is a manual delete on the target -- also with CDC stopped, which freezes
        # the target so the delete cannot race a newly replicated insert; the replay after
        # Start is idempotent (upserts and no-op deletes). Mixed with a DDL or target-side kind,
        # _ddl_runbook appends the same TRUNCATE repair (_truncate_runbook) naming the
        # truncated tables: no DDL lead removes extra rows.
        # A value-fit refusal has its OWN runbook: a column's type can only change by recreating
        # the table on Aurora DSQL (no ALTER COLUMN ... TYPE), so the generic "apply the matching
        # change (e.g. ALTER TABLE)" line would be impossible advice for it. Alone, it IS the
        # runbook; with only a TRUNCATE it is appended to the TRUNCATE line (no DDL kind, so no
        # generic line); mixed with a DDL or target-side kind, _ddl_runbook folds its tables
        # into the one recreate sentence it shares with a type change. Every DDL kind gets a
        # repair Aurora DSQL supports there (no ALTER COLUMN TYPE, no primary-key change, a
        # CHECK only NOT VALID), by migration type.
        _value_fit_tables = [
            g.table for g in drift if getattr(g, "kind", "") == "value-does-not-fit"
        ]
        if _value_fit_only:
            _runbook = _value_fit_runbook(
                migration_type, _value_fit_tables, is_pg=_is_pg, remap=_value_fit_remap(drift)
            )
        elif not _no_ddl:
            _runbook = _ddl_runbook(
                drift, migration_type, is_pg=_is_pg, migration_state=migration_state
            )
        else:
            _runbook = _truncate_runbook(migration_type)
            if _value_fit_tables:
                _runbook += " " + _value_fit_runbook(
                    migration_type, _value_fit_tables, is_pg=_is_pg,
                    remap=_value_fit_remap(drift),
                )
        if _runbook:
            ui.label(_runbook).classes("text-xs italic text-gray-600")  # type: ignore[attr-defined]
        if _target_side:
            # The recovery is different in kind: dead-lettered rows are NOT replayed and
            # their Kafka offsets are already committed, so nothing will re-deliver them
            # -- and rows applied BEFORE the table was dropped went with the table. Only
            # a re-read of the source restores them. CDC only has no Full Load step (and
            # no Drop & reload): its repair is by hand, or Start over.
            _names = ", ".join(sorted({g.table for g in _target_side}))
            if _fix_is_cdc_only(migration_type):
                _stop_header = "Stop CDC and repair these tables"
                _stop_body = (
                    f"Dead-lettered rows are never replayed and their stream position is "
                    f"already committed, so {_names} will not catch up on its own, and CDC "
                    "only has no Full Load step to reload from. Stop CDC, confirm the table "
                    "exists on the target (re-apply Schema Conversion if it does not), "
                    "correct the dead-lettered rows on Aurora DSQL by hand (at most 3,000 "
                    "rows per transaction), then Start CDC and run Validation (step 4): it "
                    "is what proves the gap is closed; this panel cannot. The dead-letter "
                    f"queue card {_DLQ_KEY_VALUES}: for a table whose key values it "
                    "withholds, or instead of the repair by hand, use Start over to migrate "
                    "the table again with a Full Load."
                    + (
                        " A table that was dropped on the target also lost every row "
                        "applied before the drop, which no dead letter lists: use Start "
                        "over for it."
                        if "missing-table" in _kinds
                        else ""
                    )
                )
            else:
                _stop_header = "Stop CDC and reload these tables"
                _stop_body = (
                    f"Dead-lettered rows are never replayed and their stream position is "
                    f"already committed, so {_names} will not catch up on its own — and "
                    "any rows applied before the table was dropped went with it. Stop "
                    "CDC, confirm the table exists on the target (re-apply Schema "
                    "Conversion if it does not), reload just that table on the Full Load "
                    "step with 'Drop & reload', then start CDC again. It has to be "
                    "'Drop & reload': an append reload only inserts primary keys the "
                    "target is missing, so a row that IS on the target but holds a stale "
                    "value (the 07006 and 23505 cases) is never rewritten. Validation "
                    "(step 4) is what proves the gap is closed; this panel cannot."
                )
            render_notice(ui, tone="error", header=_stop_header, body=_stop_body)
    if _resolved:
        _render_cdc_dlq_resolved(ui, _resolved, _verified, _held)


def cdc_oversized_quarantine_groups(
    records,
) -> "list[tuple[str, list[str], list[str], int]]":
    """Group the OVERSIZED_VALUE dead-letter records by table. Pure.

    Returns ``(table, primary_keys, columns, unnamed)`` per table in first-seen order, so the
    recovery card can name the rows that were dropped and pre-tick exactly the column to stop
    capturing. ``unnamed`` counts the rows (distinct primary keys; a record without one counts
    alone) whose record names no column -- Aurora DSQL's own 54000 names only the type, and the
    sink names the column only when the row shows which it is -- so the card never attributes
    another record's column to them and still lets the operator choose.
    Only the oversized class is grouped: it is the one quarantine with an in-app remedy, and
    offering that remedy for a duplicate-key or a drift rejection would be a lie.

    Empty list when nothing qualifies, so the card renders nothing at all -- the same
    render-only-when-actionable rule the schema-drift banner beside it follows.
    """
    from dsql_migrator.core.cdc_dlq import (
        OVERSIZED_VALUE_CODE,
        oversized_column_from_reason,
    )

    order: list[str] = []
    pks: dict[str, list[str]] = {}
    cols: dict[str, list[str]] = {}
    unnamed_pks: dict[str, set[str]] = {}
    unnamed: dict[str, int] = {}
    for record in records or []:
        if getattr(record, "error_code", None) != OVERSIZED_VALUE_CODE:
            continue
        table = getattr(record, "table", "") or ""
        if not table:
            continue
        if table not in pks:
            order.append(table)
            pks[table] = []
            cols[table] = []
            unnamed_pks[table] = set()
            unnamed[table] = 0
        pk = getattr(record, "pk", None)
        if pk and pk not in pks[table]:
            pks[table].append(pk)
        column = oversized_column_from_reason(getattr(record, "message", ""))
        if column:
            if column not in cols[table]:
                cols[table].append(column)
        elif not pk or pk not in unnamed_pks[table]:
            if pk:
                unnamed_pks[table].add(pk)
            unnamed[table] += 1
    return [(table, pks[table], cols[table], unnamed[table]) for table in order]


def _render_cdc_oversized_recovery(
    ui,
    migration_state,
    log_key: str,
    *,
    job_manager=None,
    lob_candidates_for=None,
    exclude_columns=None,
    session=None,
    records=None,
) -> None:
    """Offer the in-app recovery for rows CDC dropped over DSQL's 1 MiB per-value limit.

    The CDC dead-letter surface was read-only: it reported, per table, that rows had been
    dropped and never let the operator act, and the AI assistant, asked what to do, told
    them to hand-edit ``column.exclude.list`` on the connector (not doable from this UI,
    and not necessary: the tool owns that setting and re-sends it on every Start CDC).

    ``exclude_columns(table, columns, column_unknown=...)`` is the route into the exclusion
    popup: it records the rule for each named column as a PENDING line (it never changes what
    is excluded by itself) and opens the popup in the mode the lock gives -- read-only with the
    lock's own steps while a stack holds the set, editable (the line inserted) once it can
    change. ``column_unknown`` is set when a dropped row of the table names no column, so the
    popup still lets the operator choose it. So the button is enabled in every state.
    """
    if exclude_columns is None:
        # No in-app action wired (an older caller) -> render nothing rather than a dead card.
        return
    if records is None:
        from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_records

        records = cdc_dlq_records(migration_state, log_key)
    groups = cdc_oversized_quarantine_groups(records)
    if not groups:
        return

    def _action(table: str, columns: "list[str]", unnamed: int):
        def _render() -> None:
            candidates = list(lob_candidates_for(table)) if lob_candidates_for else []
            if not candidates and not columns:
                # Nothing excludable on this table (e.g. the only oversized column is part of
                # the primary key, which lob_exclusion_candidates deliberately withholds). A
                # dead button would read as a broken feature, so say why instead.
                inline_hint(
                    ui,
                    "No column on this table can be excluded at capture.",
                    tone="warning",
                )
                return
            button = ui.button(  # type: ignore[attr-defined]
                "Exclude column…",
                icon="filter_alt_off",
                on_click=lambda: exclude_columns(
                    table, list(columns), column_unknown=unnamed > 0
                ),
            )
            button.props("flat dense no-caps size=sm color=warning")
            button.tooltip(
                "Open Exclude tables & columns with a rule for the column that cannot fit; "
                "it is recorded and applied when the rules can change."
            )

        return _render

    for table, pks, columns, unnamed in groups:
        where = ""
        if columns:
            where = f" — column {', '.join(columns)}"
            if unnamed:
                # A row whose record names no column is never shown under another's column.
                where += (
                    f"; for {unnamed} row{'s' if unnamed != 1 else ''} the column could not "
                    "be determined"
                )
        _quarantine_detail_row(
            ui,
            table=table,
            primary_keys=pks,
            reasons=[
                "Dropped over DSQL's 1 MiB per-value limit"
                + where
                + ". This row cannot be stored as-is and is never retried."
            ],
            action=_action(table, columns, unnamed),
        )


def _render_cdc_oversized_recovery_without_monitoring(
    ui, migration_state, job_manager, *, lob_candidates_for=None, exclude_columns=None,
    session=None,
) -> None:
    """The CDC dead-letter "Exclude column..." recovery when live monitoring is hidden.

    The recovery lives in the dead-letter panel, which renders only while CDC is live --
    yet its action is allowed only once no CDC infrastructure exists, so inside that panel
    the button could never be enabled and the dialog never opened. The records it needs
    are stored locally (``cdc_dlq_records``), so after a Stop the rows stay visible here
    with the button disabled and its true reason (delete the infrastructure), and after
    the delete the button works.
    """
    if exclude_columns is None or cdc_monitoring_visible(migration_state, job_manager):
        return
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_records

    log_key = cdc_error_log_key(migration_state)
    records = cdc_dlq_records(migration_state, log_key)
    if not cdc_oversized_quarantine_groups(records):
        return
    render_notice(
        ui,
        tone="warning",
        header="Rows CDC dropped over Aurora DSQL's 1 MiB value limit",
        body=(
            "These rows were dead-lettered before CDC stopped. Excluding the column that "
            "cannot fit is recorded below and applied when the rules can change."
        ),
    )
    _render_cdc_oversized_recovery(
        ui,
        migration_state,
        log_key,
        job_manager=job_manager,
        lob_candidates_for=lob_candidates_for,
        exclude_columns=exclude_columns,
        session=session,
        records=records,
    )


def _render_cdc_dlq_panel(
    ui,
    migration_state,
    job_manager,
    status_view: LoadStatusView,
    on_refresh=None,
    session=None,
    cdc_ai_opener=None,
    lob_candidates_for=None,
    exclude_columns=None,
    page_state=None,
) -> None:
    """Render the dead-letter queue as one cohesive, AWS-console-style card.

    A tinted notice band (tone from DLQ health -- :func:`_dlq_panel_tone`) carries
    a leading status icon, the "Dead-letter queue (poison records)" title, a depth
    badge, an optional refresh control, and the health message; below it sit the
    per-table breakdown, the scrollable record table (visible even at high row
    counts via paging), and the download. ``on_refresh`` (optional) re-polls the
    sink's CloudWatch dead-letter log on demand instead of waiting for the ~5s
    auto-poll.
    """
    health = assess_dlq_health(status_view.dlq_depth)
    if health is None:
        return
    _activity = getattr(migration_state, "cdc_activity", None)
    _stalled = bool(getattr(_activity, "sink_stall_confirmed", False))
    tone = _dlq_panel_tone(health, sink_stalled=_stalled)
    with notice_panel(
        ui, tone=tone, header="Dead-letter queue (poison records)"
    ) as (header_row, _panel):
        # Header band: icon + title + depth badge (left), refresh (right).
        with header_row:
            _badge_color = (
                "negative"
                if tone == "error"
                else "warning"
                if tone == "warning"
                else "grey-6"
                if health.depth == 0
                else "primary"
            )
            _bound = getattr(migration_state, "cdc_dlq_coverage_bound", None)
            ui.badge(  # type: ignore[attr-defined]
                f"at least {health.depth} quarantined" if _bound
                else f"{health.depth} quarantined"
            ).props(f"color={_badge_color}")
            ui.space()  # type: ignore[attr-defined]
            # Ask AI DBA to triage the poison records (only when there is something to
            # triage): reuses the CDC-assist DLQ-triage grounding in a conversational,
            # tool-wired chat scoped to these dead-letter records.
            if cdc_ai_opener is not None and health.depth > 0:
                _dlq_key = cdc_error_log_key(migration_state)
                _render_cdc_ai_button(
                    ui,
                    lambda k=_dlq_key: cdc_ai_opener(
                        "dlq", _cdc_dlq_facts(migration_state, k, job_manager), _CDC_DLQ_SEED
                    ),
                    "Ask AI DBA to triage these poison records and how to fix them.",
                )
            if on_refresh is not None:
                ui.button(on_click=on_refresh).props(  # type: ignore[attr-defined]
                    "flat dense round size=sm icon=refresh"
                ).tooltip("Refresh dead-letter records from CloudWatch")
        ui.label(health.message).classes("text-xs text-gray-700")  # type: ignore[attr-defined]
        if _bound:
            # The count's own coverage. Sibling in intent to the stalled-sink caveat
            # below: a number that LOOKS like a total but is a floor is the reassurance
            # a cut-over decision must not be given silently.
            render_notice(
                ui,
                tone="warning",
                header="This count is a minimum, not a total",
                body=(
                    f"The dead-letter history could not be read in full — {_bound}. "
                    "Treat the count as a floor when deciding to cut over; the "
                    "connector's CloudWatch log group holds the complete record."
                ),
            )
        if _stalled and health.depth == 0:
            # "No records quarantined." is literally true but reads as reassurance, so
            # say why it proves nothing right now: a stalled sink never reaches a record
            # to quarantine, so a zero depth is the EXPECTED reading during a stall. A
            # status caveat like this must carry its severity in a design-system notice
            # box, not loose red text (the box + amber border + icon do the signalling).
            render_notice(
                ui,
                tone="warning",
                header="A zero count is expected while the sink is stalled",
                body=(
                    "A stalled sink never reaches a record to quarantine, so a zero "
                    "dead-letter count here is not evidence that nothing was lost."
                ),
            )
        _render_cdc_schema_drift_banner(
            ui, status_view, session=session, on_refresh=on_refresh,
            cdc_ai_opener=cdc_ai_opener,
            saved_exclusions=migration_state.lob_exclusions(),
            migration_type=getattr(migration_state, "migration_type", None),
            on_applied=getattr(migration_state, "record_cdc_target_added_columns", None),
            migration_state=migration_state,
        )
        _render_cdc_dlq_breakdown(
            ui, status_view, migration_type=getattr(migration_state, "migration_type", None)
        )
        # CDC commonly has no Full Load job_id this session, so key the record list /
        # download off the same stable CDC key the fold used (cdc_error_log_key) --
        # not _current_job, which would be None and hide everything.
        log_key = cdc_error_log_key(migration_state)
        # The in-app recovery for the one quarantine class that HAS one, rendered as a sibling
        # of the drift banner above and, like it, invisible when its subset is empty.
        _render_cdc_oversized_recovery(
            ui, migration_state, log_key,
            job_manager=job_manager,
            lob_candidates_for=lob_candidates_for,
            exclude_columns=exclude_columns,
            session=session,
        )
        _render_cdc_dlq_records(ui, migration_state, log_key, page_state=page_state)
        if health.depth > 0:
            _render_cdc_error_download(ui, migration_state, log_key)
        # Resolve the Full Load job here so the pointer can follow the retry lineage
        # (a retry runs under a NEW job id -- see full_load_error_records_for).
        from dsql_migrator.ui.data_migration._cdc_status import _current_job

        _fl_job = _current_job(job_manager, getattr(migration_state, "job_id", None))
        _render_full_load_quarantine_pointer(
            ui, migration_state, log_key, job=_fl_job
        )

def _render_full_load_quarantine_pointer(
    ui, migration_state, log_key: str, job=None
) -> None:
    """Note that the Full Load set rows aside too, and where to see them.

    The DLQ panel now counts CDC records only, which is correct -- but the Full Load's
    quarantines must not simply vanish from view: they are rows that never reached the
    target, and cut-over depends on knowing about them. So when this session's error log
    also holds Full Load records, say so in one neutral line that points at the Full
    Load section rather than folding them back into a DLQ count they do not belong to.

    Deliberately NOT a warning: the Full Load already reported these where they belong,
    and this line is a cross-reference, not a new problem.
    """
    if not log_key:
        return
    # Resolve the RETRY LINEAGE rather than reading the bare key: a Full Load retry runs
    # under a new job id, so a bare read saw only the retry's own records and this
    # cross-reference silently vanished for tables the retry did not re-run -- while the
    # Full Load section on the same page still showed them. The two halves of one screen
    # disagreed, on the screen read just before cut over.
    from dsql_migrator.ui.data_migration._cdc_status import (
        full_load_error_records_for,
    )

    try:
        if job is not None:
            full_load = list(full_load_error_records_for(migration_state.error_log, job))
        else:
            records = migration_state.error_log.records(log_key) or []
            full_load = [r for r in records if not is_cdc_error_record(r)]
    except Exception:  # noqa: BLE001 - advisory line; never break the panel
        return
    if not full_load:
        return
    noun = "row" if len(full_load) == 1 else "rows"
    ui.label(  # type: ignore[attr-defined]
        f"The Full Load also set {len(full_load)} {noun} aside. Those are separate from "
        "this dead-letter queue (the batch loader isolated them; they never entered the "
        "stream) — see the Full Load section above for the details."
    ).classes("text-xs text-gray-500")


def _render_cdc_dlq_breakdown(
    ui, status_view: LoadStatusView, *, migration_type=None
) -> None:
    """Show which tables produced DLQ/error records, so a poison source is found.

    Reads the per-table counts already on the view's
    :class:`~dsql_migrator.core.models.ErrorLogSummary` (no new query) and renders
    them as compact, always-visible per-table chips (table xN), sorted by count.
    Chips (not a collapsible sub-table) keep the at-a-glance "where is the poison
    coming from" answer visible without a click and survive the ~5s re-render
    without snapping shut. Nothing shown when there are no per-table counts (a
    clean stream or errors not yet attributed to a table). ``migration_type`` picks
    the repair: CDC only has no Full Load step to reload from.
    """
    summary = status_view.error_summary
    by_table = summary.errors_by_table if summary is not None else {}
    if not by_table:
        return
    ordered = sorted(by_table.items(), key=lambda kv: (-kv[1], kv[0]))
    with ui.row().classes("w-full items-center gap-1 flex-wrap"):  # type: ignore[attr-defined]
        ui.label("By table:").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
        for table, count in ordered:
            ui.badge(f"{table} ×{count}").props("color=grey-7 outline")  # type: ignore[attr-defined]
    # WHAT TO DO with a quarantine, and where the answer actually lives. The dead-letter
    # record says a row was dropped; only Validation says what the TARGET is missing or
    # holding as a result, per table and by primary key. The two are linked through the
    # per-table dead-letter ledger (ui/cdc_dlq_gate.py): Validation compares every
    # dead-lettered table by CHECKSUM, and Cut over stays locked until each has matched in a
    # CHECKSUM comparison started after its last record. (The Full-Load-only quarantine count
    # that feeds Validation's "known gap" acknowledgement is still not populated by CDC.)
    #
    # The blind spot is stated because it is wider than it looks: a quarantined UPDATE changes
    # no row count, so RECONCILE cannot see it -- and ROW_COUNT is the DEFAULT validation mode.
    # CHECKSUM does, except on the columns it cannot value-compare (json always; floating
    # point only from a MySQL source), which the result names per table.
    #
    # "Stop CDC first" is part of the RECOVERY, not a footnote. This card is only on screen
    # while the sink is streaming (that is what fills the DLQ), and in exactly that state the
    # Full Load step runs no reload at all (G129): Retry unfinished tables, Reload specific
    # tables and the per-table Reload are disabled, and their confirm dialog cannot be
    # confirmed, because a load beside the sink inserts only the rows missing on the target, so
    # a row the source deletes while its table is copied could be written back after the sink
    # applied that delete. So the old wording ("drop and reload that table") sent the operator
    # to look for an action the tool deliberately withholds: two screens of the same app
    # contradicting each other. Stating the precondition here is the fix; "a reload fixes
    # them" is also softened to "reload that table to backfill it", a repair made with CDC
    # stopped.
    #
    # CDC only has no Full Load step at all (G25), so there the repair is the drift banner's
    # own: by hand on Aurora DSQL with CDC stopped, or Start over -- never a reload.
    if _fix_is_cdc_only(migration_type):
        repair = (
            "Rows MISSING on the target, or holding a stale value, are a dropped insert or "
            "update: with CDC stopped, correct them on Aurora DSQL by hand (at most 3,000 rows "
            "per transaction), or migrate the table again with Start over. Rows EXTRA on the "
            "target are a dropped delete: delete them by hand, with CDC stopped too. The "
            f"record list below {_DLQ_KEY_VALUES}: for a table whose key values it withholds, "
            "use Start over."
        )
    else:
        repair = (
            "Rows MISSING "
            "on the target are a dropped insert (reload that table to backfill it); rows "
            "EXTRA on the target are a dropped delete (a reload cannot remove it — the table "
            "must be dropped and reloaded, or the row deleted by hand). Both repairs need "
            "Stop CDC first: while the sink is streaming, the Full Load step runs no reload, "
            "because a reload beside it could write back rows the sink deletes meanwhile."
        )
    render_notice(
        ui,
        tone="info",
        icon="fact_check",
        header="Confirm the effect of these drops in Validation",
        body=(
            "A dead-lettered record is a row the stream did NOT apply, and it is never "
            "retried; Validation is what shows the consequence on the target. "
            + repair
            + " A dropped UPDATE changes no row "
            "count, so the default row-count mode cannot see it: Validation therefore also "
            "compares these tables by CHECKSUM, and Cut over stays locked until each has "
            "matched in a CHECKSUM comparison that started after its last dead-lettered "
            "record. CHECKSUM cannot value-compare json columns (nor, from a MySQL source, "
            "floating-point ones); the result names them per table."
        ),
    )

# Cap the inline DLQ record list so a flood of poison rows never renders an
# unbounded table in the browser; the full set is always in the downloadable log.
_DLQ_RECORD_LIST_LIMIT = 200

# Rows shown per page in the record table; paging keeps a large DLQ readable
# (a fixed, scrollable viewport) instead of an ever-growing wall of rows.
_DLQ_RECORD_PAGE_SIZE = 10

def _render_cdc_dlq_records(ui, migration_state, log_key: str, page_state=None) -> None:
    """List the individual quarantined records (time, table, SQLSTATE, reason).

    Reads the per-record rows already in the single
    :class:`~dsql_migrator.core.error_log.ErrorLogStore` under ``log_key`` (the CDC
    error-log key -- the Full Load job id, or the stable per-stack fallback when no
    Full Load ran this session; the same rows the download renders, no new query),
    newest first, so the operator sees WHAT was dead-lettered, not just a count.
    Rendered as a paged, bounded-height table so a high record count stays readable
    (a fixed viewport with paging) rather than an unbounded wall of rows.
    Credential-free by construction (table / SQLSTATE / reason + SQL TEMPLATE with
    ``?`` placeholders, never row values -- Property 7). Nothing is shown when there
    are no records yet (clean stream).
    """
    # CDC-sourced records only: the log key is shared with the Full Load, so an
    # unfiltered read listed batch-loader quarantines under "Dead-letter queue".
    records = cdc_dlq_records(migration_state, log_key)
    if not records:
        return
    ordered = sorted(
        records,
        key=lambda r: r.occurred_at or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    shown = ordered[:_DLQ_RECORD_LIST_LIMIT]
    rows = [
        {
            "table": r.table,
            "code": r.error_code or "—",
            # Without a source TRUNCATE's sink "Recovery: reload ... on the Full Load step"
            # clause (cdc_dlq_shown_message): the drift banner on this same panel, fed by the
            # same records, gives the runbook for the run's migration type.
            "message": cdc_dlq_shown_message(r),
            "when": r.occurred_at.strftime("%Y-%m-%d %H:%M:%S")
            if r.occurred_at
            else "—",
        }
        for r in shown
    ]
    extra = len(ordered) - len(shown)
    caption = (
        f"Quarantined records ({len(ordered)}"
        + (f", showing latest {len(shown)}" if extra > 0 else "")
        + ")"
    )
    # Always-visible (NOT a ui.expansion): the CDC panel re-renders on every ~5s
    # poll, which would re-create an expansion in its default (closed) state and
    # snap a just-opened list shut. A paged ui.table keeps a fixed, scrollable
    # viewport so even hundreds of rows stay readable and visible.
    ui.label(caption).classes("text-xs font-medium text-gray-700 mt-1")  # type: ignore[attr-defined]
    # Seeded from a holder that lives OUTSIDE the ~5s poll refreshable, and written back on
    # change. Everything here used to be hardcoded with no ``on_pagination_change`` at all,
    # so the browser's choice never reached Python and the next tick restored it: raising
    # "Records per page" 10 -> 20 was undone within 5 seconds and the control looked broken.
    # Quasar sends the sort in the same event, so a click on Time / Table / SQLSTATE snapped
    # back for the identical reason. Same shape as the Full Load progress table's
    # ``_progress_page`` (_full_load_ui.py), where this was fixed once already.
    _per_page, _page = _DLQ_RECORD_PAGE_SIZE, 1
    _sort_by, _descending = "when", True
    _filter = ""
    if isinstance(page_state, dict):
        _per_page = int(page_state.get("rowsPerPage", _per_page))
        # rowsPerPage == 0 is Quasar's "All": one page, and no ceil-div by zero.
        _max_page = max(1, -(-len(rows) // _per_page)) if _per_page > 0 else 1
        # Clamp: the record list GROWS and can also be filtered, so a saved page 3 can
        # point past the end and would render an empty table with no way back.
        _page = min(max(1, int(page_state.get("page", 1))), _max_page)
        _sort_by = page_state.get("sortBy", _sort_by)
        _descending = bool(page_state.get("descending", _descending))
        _filter = str(page_state.get("filter") or "")

    def _on_pagination_change(event: object) -> None:
        if not isinstance(page_state, dict):
            return
        # Copy the four keys individually, never ``event.value`` wholesale: a stray
        # ``rowsNumber`` flips QTable into server-side pagination mode, which would leave
        # every page after the first empty.
        value = getattr(event, "value", None) or {}
        page_state["page"] = int(value.get("page", page_state.get("page", 1)))
        page_state["rowsPerPage"] = int(
            value.get("rowsPerPage", page_state.get("rowsPerPage", _DLQ_RECORD_PAGE_SIZE))
        )
        page_state["sortBy"] = value.get("sortBy", page_state.get("sortBy"))
        page_state["descending"] = bool(
            value.get("descending", page_state.get("descending"))
        )

    table = ui.table(  # type: ignore[attr-defined]
        columns=[
            {"name": "when", "label": "Time", "field": "when", "align": "left",
             "sortable": True},
            {"name": "table", "label": "Table", "field": "table", "align": "left",
             "sortable": True},
            {"name": "code", "label": "SQLSTATE", "field": "code", "align": "left",
             "sortable": True},
            {"name": "message", "label": "Reason", "field": "message", "align": "left"},
        ],
        rows=rows,
        row_key="message",
        pagination={"rowsPerPage": _per_page, "page": _page,
                    "sortBy": _sort_by, "descending": _descending},
        on_pagination_change=_on_pagination_change,
    ).classes("w-full text-xs bg-white rounded-md")
    # `wrap-cells` is what makes the Reason column show the WHOLE message. Without it Quasar
    # keeps every cell on one line and clips it, so the part of a dead-letter reason that
    # actually identifies the failure -- the driver text and the offending SQL -- was cut off
    # exactly where it started to be useful. The Full Load progress table already sets it.
    table.props('flat bordered dense wrap-cells')
    # A search box so a specific table/SQLSTATE/reason is findable in a large DLQ.
    with table.add_slot("top-left"):  # type: ignore[attr-defined]
        def _on_filter(event: object) -> None:
            # Ignore the bind-time None: NiceGUI's backward binding fires this handler once
            # with no value, which would write "" straight over the saved filter.
            value = getattr(event, "value", None)
            if isinstance(page_state, dict) and value is not None:
                page_state["filter"] = str(value)

        _filter_input = ui.input(  # type: ignore[attr-defined]
            placeholder="Filter records", on_change=_on_filter
        ).props("dense clearable borderless")
        _filter_input.bind_value(table, "filter")
        # Seed AFTER bind_value, not via `value=`: NiceGUI's backward binding (table ->
        # input) wins the initial synchronization, so a value passed to the constructor is
        # wiped at bind time. Both the table and this input are rebuilt every poll, so
        # without this a filter typed into a LIVE dead-letter queue -- the one situation the
        # box exists for -- was erased within 5 seconds.
        if _filter:
            _filter_input.value = _filter

def _render_cdc_error_download(ui, migration_state, log_key: str) -> None:
    """Offer the CDC-sourced error records as a download.

    Both the COUNT in the label and the file contents are filtered to CDC. The log key
    is shared with the Full Load, so this button used to say "Download CDC error log
    (3 errors)" and hand over three Full Load quarantines -- see is_cdc_error_record.
    """
    records = cdc_dlq_records(migration_state, log_key)
    if not records:
        return
    # A filesystem-safe slug for the filename (the CDC fallback key is "cdc:<stack>").
    safe = log_key.replace(":", "_").replace("/", "_")

    def _download_log() -> None:
        try:
            # Serialize the FILTERED records; render_log(log_key) would re-read the
            # whole key and put Full Load rows back into a file labelled CDC.
            # ONE JSON document, not NDJSON: NDJSON is not valid JSON, so the two things
            # an operator actually does with this file -- open it, or feed it to
            # json.load / jq -- both failed on the old form. Same records, same fields.
            payload = migration_state.error_log.render_json(
                records,
                source="cdc",
                stack=getattr(migration_state, "cdc_stack_name", "") or "",
            )
            ui.download.content(  # type: ignore[attr-defined]
                payload, f"cdc_error_log_{safe}.json", "application/json"
            )
        except Exception as exc:  # noqa: BLE001 - surface instead of silent
            _LOGGER.exception("Failed to render/download CDC error log")
            ui.notify(  # type: ignore[attr-defined]
                f"Could not generate the error log: {exc}", type="negative"
            )

    # Named the same way as the Full Load download: WHAT it is and HOW MUCH, not the file
    # format. Saying "CDC" also matters here -- both steps offer a download, and
    # "Download error log" alone gave no way to tell which one you were getting.
    noun = "error" if len(records) == 1 else "errors"
    ui.button(  # type: ignore[attr-defined]
        f"Download CDC error log ({len(records)} {noun})",
        on_click=_download_log,
        icon="download",
    ).props("outline dense no-caps").tooltip(  # type: ignore[attr-defined]
        "One line per error with the table, primary key, reason and timestamp — never "
        "row values. Saved as NDJSON (one JSON object per line), readable in any text "
        "editor."
    )


def _render_cdc_handling_panel(ui) -> None:
    """Render the CDC behavior & limits section: what's handled vs. what to watch.

    A collapsed section (reference material) split into two clearly-labeled groups
    -- "Handled automatically" (the guarantees) and "Limits to watch" (the caveats
    the operator must plan around, e.g. DDL is not replicated) -- so the two read
    distinctly instead of as one mixed list. Customer-facing copy only (the
    internal spike hypothesis codes on each fact are not shown).
    """
    facts = cdc_handling_facts()
    handled = [f for f in facts if f.handled]
    limits = [f for f in facts if not f.handled]

    def _fact_rows(group, *, icon, color):
        for fact in group:
            with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.icon(icon, color=color).classes("text-base")  # type: ignore[attr-defined]
                with ui.column().classes("gap-0 flex-1 min-w-0"):  # type: ignore[attr-defined]
                    ui.label(fact.title).classes("text-sm")  # type: ignore[attr-defined]
                    ui.label(fact.detail).classes("text-xs text-gray-500")  # type: ignore[attr-defined]

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        # Collapsed by DEFAULT: this is reference-only info (no action), and the full
        # text is long -- showing it expanded on every visit adds noise and can
        # confuse the operator. They open it when they want the contract details.
        with ui.expansion("CDC behavior & limits", icon="info").classes(  # type: ignore[attr-defined]
            f"w-full {EXPANSION_PANEL_CLASSES}"
        ):
            with ui.column().classes("w-full gap-3 mt-1"):  # type: ignore[attr-defined]
                if handled:
                    ui.label("Handled automatically").classes(  # type: ignore[attr-defined]
                        "text-xs font-semibold text-gray-500 uppercase tracking-wide"
                    )
                    _fact_rows(handled, icon="check_circle", color="positive")
                if limits:
                    ui.separator().classes("my-1")  # type: ignore[attr-defined]
                    ui.label("Limits to watch").classes(  # type: ignore[attr-defined]
                        "text-xs font-semibold text-gray-500 uppercase tracking-wide"
                    )
                    _fact_rows(limits, icon="warning", color="warning")
