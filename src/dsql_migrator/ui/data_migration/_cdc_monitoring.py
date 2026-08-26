# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Post-start CDC monitoring / DLQ / status render panels (extracted from ``_cdc_ui.py``).

The surfaces that are meaningful only once CDC is streaming: the per-table migration
status table, the live connector-health + stream-lag monitoring, the change-flow and
pipeline-health panels, the dead-letter-queue panel (+ its breakdown/records/download
sub-panels and the add-column dialog), the schema-drift banner, and the LOB-exclusion /
CDC-handling panels. Rendering only; the pure state predicates live in ``_cdc_state`` and
the read-models in ``_models`` / ``_status``. One-directional: imports from ``_cdc_state`` /
``_models`` / ``_status`` / ``core`` / ``ui.design`` (never back from ``_cdc_ui``); it is
re-exported from ``_cdc_ui`` so ``_cdc_ui.<name>`` and every consumer/test import resolve
unchanged.
"""

from __future__ import annotations

from datetime import datetime, timezone
from collections.abc import Sequence
from typing import Optional

from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.core.models import LoadStatusView, SourceInventory
from dsql_migrator.ui.data_migration._cdc_state import (
    _CDC_POLL_INTERVAL_SECONDS,
    _cdc_is_streaming,
    cdc_infra_deploy_in_flight,
    cdc_monitoring_visible,
    cdc_pipeline_live,
    cdc_streaming_started,
)
from dsql_migrator.ui.data_migration._models import (
    _BROKER_MESSAGE_LIMIT_MIB,
    _DSQL_VALUE_LIMIT_MIB,
    assess_dlq_health,
    build_lag_chart_option,
    build_migration_table_status,
    cdc_handling_facts,
    connector_health_rows,
    format_column_exclude_list,
    lob_exclusion_candidates,
    per_table_counts_notice_body,
    scope_lob_candidates,
)
from dsql_migrator.ui.data_migration._cdc_status import (
    _CDC_TONE_STYLE,
    _apply_cdc_status,
    _cdc_status_view,
    _current_job,
    _fetch_cdc_status,
    _fetch_migration_row_counts,
    _migration_status_tables,
    cdc_dlq_records,
    cdc_dlq_summary,
    cdc_error_log_key,
    is_cdc_error_record,
)
from dsql_migrator.ui.design import (
    EXPANSION_PANEL_CLASSES,
    NOTICE_STYLE,
    definition_row,
    inline_hint,
    render_notice,
    section_header,
)
from dsql_migrator.ui.data_migration import _LOGGER

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
    COUNT(*). The source/target-count columns still come from
    a direct COUNT(*) on each side, which scans the source, so those remain an
    explicit "Refresh counts" action (not an auto-poll).

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
            table_rows = []
            for r in rows_model:
                # The header already says "(est.)" (the normal case), so only the
                # UNUSUAL case is marked per-cell: an EXACT source count, which is
                # worth flagging because it makes the row's verdict authoritative.
                source_label = _fmt(r.source_rows)
                if not r.source_estimate and r.source_rows is not None:
                    source_label += " (exact)"
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
            # instead of showing a bottom pagination bar / inner scroll. The columns
            # carry short labels so the row fits the card width without a horizontal
            # scrollbar at the bottom.
            table = ui.table(  # type: ignore[attr-defined]
                columns=columns,
                rows=table_rows,
                row_key="table",
                pagination={"rowsPerPage": 0},
            ).props("dense flat").classes("w-full")
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
                "Approximate row count from the source's information_schema — a "
                "scan-free ESTIMATE, so this view never runs a COUNT(*) full scan "
                "against your live source. InnoDB derives it from index sampling, so "
                "it commonly differs from the true count by several percent (more on "
                "a large table) and often UNDERCOUNTS — a target that slightly "
                "exceeds it is normal, not data duplication. For an exact "
                "source-vs-target comparison, run Validation (step 4).",
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
            # The source/target counts are a direct COUNT(*)/MAX(pk) on each side and
            # can take seconds on a large source, so give clear in-progress feedback:
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
            definition_row(
                ui,
                "Stream lag",
                "How far behind the target is, in time — the age of the newest source "
                "change not yet applied. “caught up” = the target is current (safe to "
                "cut over); “N behind (PK)” is a fallback shown only when the "
                "time-based metric isn't available yet.",
            )
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
                ui.label(  # type: ignore[attr-defined]
                    "— any non-green badge means investigate. “consistent” means "
                    "nothing looks wrong (the source count is an estimate, so it is "
                    "not a proven exact match); Validation (step 4) is the exact check."
                ).classes("text-xs text-gray-600")

# CDC activity events already mirrored into the AI feed, keyed by the stable CDC log
# key, so the ~5s monitor poll announces each transition ONCE (not every tick). Resets
# on process restart (in-memory) -- a benign re-announce, never a data risk.
_CDC_ANNOUNCED: "dict[str, set]" = {}


def _announce_cdc_events(migration_state, status_view, ai_post_event) -> None:
    """Edge-triggered CDC activity events into the AI feed (once per transition).

    Announces: CDC streaming started, source schema drift (per table+kind), the DLQ
    first growing past zero, and a confirmed sink stall -- the silent-data-loss signals
    the assistant should be aware of. Deduped per stream via :data:`_CDC_ANNOUNCED`.
    Credential/row-free text only (Property 7); never raises into the poller."""
    if ai_post_event is None:
        return
    try:
        seen = _CDC_ANNOUNCED.setdefault(cdc_error_log_key(migration_state), set())
        if "started" not in seen:
            seen.add("started")
            ai_post_event(text="CDC streaming started", status="started")
        for group in getattr(status_view, "schema_drift", None) or []:
            marker = ("drift", group.table, group.kind)
            if marker not in seen:
                seen.add(marker)
                label = _DRIFT_LABELS.get(group.kind, (group.kind, ""))[0]
                ai_post_event(
                    text=(
                        f"CDC: source schema change on {group.table} ({label}) — "
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
    from dsql_migrator.core.cdc_postgres import classify_slot_health

    tone, headline, detail = classify_slot_health(health)
    render_notice(ui, tone=tone, header=headline, body=detail)


def _render_cdc_live_monitoring(
    ui, migration_state, job_manager, session=None, cdc_ai_opener=None,
    ai_post_event=None,
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
            ui.icon("check_circle").classes("text-green-600 text-base")  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "Caught up — no replication lag in the recent window."
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

    @ui.refreshable
    def _cdc_live() -> None:  # type: ignore[misc]
        view = _cdc_status_view(migration_state, job_manager)
        if view is not None:
            # Mirror CDC transitions (started / drift / DLQ growing / sink stalled) into
            # the AI feed, once each, so the assistant is aware of silent-loss risks.
            _announce_cdc_events(migration_state, view, ai_post_event)
            _render_cdc_pipeline_health(
                ui, view, getattr(migration_state, "cdc_activity", None)
            )
            # PostgreSQL CDC: warn about source WAL pressure (the logical slot pinning
            # WAL) before the source disk fills. No-op for a MySQL source (no slot).
            _render_cdc_slot_health(ui, migration_state)
            _render_cdc_dlq_panel(
                ui,
                migration_state,
                job_manager,
                view,
                on_refresh=_poll_cdc,
                session=session,
                cdc_ai_opener=cdc_ai_opener,
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

        # Scope the scan-free net-rows metric read to the migrated table set.
        tables = _migration_status_tables(migration_state, job_manager)
        try:
            fetched = await run.io_bound(_fetch_cdc_status, migration_state, tables)
        except Exception:  # noqa: BLE001 - keep the last good view on any error
            fetched = None
        if fetched is not None:
            _apply_cdc_status(migration_state, fetched)
        _update_lag_chart()  # persistent chart: update in place (no flicker)
        _cdc_live.refresh()  # connector health / change flow / DLQ (text) redraw

    _update_lag_chart()  # initial state (hidden until >=2 points)
    _cdc_live()

def _render_cdc_pipeline_health(
    ui,
    status_view: LoadStatusView,
    activity: "Optional[CdcActivitySummary]",
) -> None:
    """Render the combined "Pipeline health" card: connector health + change flow.

    Connector state/lag and the change-flow throughput both answer "is the pipeline
    running and moving data right now?", so they share one card (a labelled
    connector-health group, then a change-flow group), rather than two adjacent
    cards the operator has to mentally join. The DLQ stays its own card (it is about
    data set aside, a different concern). Renders nothing when there is no connector
    health to show (pre-streaming).
    """
    rows = connector_health_rows(
        status_view.connector_states, lag_seconds=status_view.lag_seconds
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
                    "target. When you quiesce the source for cutover, watch this "
                    "drop to idle — the pipeline has drained. Rates are from "
                    "CloudWatch (about the last few minutes)."
                )
            _render_change_flow_status(ui, activity)

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
            ui.icon("pause_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
            ui.label("No changes flowing — pipeline idle").classes(  # type: ignore[attr-defined]
                "text-sm text-gray-700"
            )
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
            body=(
                "The source is producing changes but the sink has applied none, so the "
                "target is falling behind and the gap will not close on its own. The "
                "connector can still report RUNNING and no errored task while this is "
                "happening (a sink consumer ejected from its group keeps its thread "
                "alive), so RUNNING is not evidence that replication works — compare "
                "target row counts. Check the sink connector log for repeating "
                '"Commit of offsets timed out"; if you see it, Stop CDC and Start CDC '
                "to rejoin the group, and do NOT cut over until the sink send rate "
                "recovers."
            ),
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

# Source-schema-drift kinds (core.cdc.SchemaDriftKind values) -> a human label and
# the reason DSQL rejected the row. Detection-only: CDC never propagates DDL, so a
# source ALTER leaves the target behind and its rows dead-letter. The recovery is
# always operator-driven (the tool never auto-alters the target -- Property 6).
_DRIFT_LABELS: dict[str, tuple[str, str]] = {
    "add-column": (
        "column added at the source",
        "the source added a column the target table does not have, so DSQL rejected "
        "the new-schema rows (SQLSTATE 42703)",
    ),
    "drop-column": (
        "column dropped at the source",
        "the source dropped a column the target still requires (NOT NULL), so DSQL "
        "rejected the rows (SQLSTATE 23502)",
    ),
    "type-change": (
        "column type changed at the source",
        "the source changed a column's type incompatibly, so DSQL rejected the rows "
        "(SQLSTATE 42804 / 22xxx)",
    ),
}


async def _open_add_column_dialog(ui, session, table: str, on_refresh=None) -> None:
    """Offer the operator the exact ADD COLUMN DDL the target is missing.

    The recovery for the dominant drift kind (source ADD COLUMN), split so nothing
    mutates the target without consent: this reads the source's and the target's
    column lists, renders one ``ALTER TABLE ... ADD COLUMN`` per missing column via
    the converter's own type mapping, shows them verbatim for approval, and only
    then applies them -- one DDL per transaction, as Aurora DSQL requires.

    Both reads and the apply are blocking I/O, so each is offloaded with
    ``run.io_bound``; the render path itself stays I/O-free.
    """
    from nicegui import run

    from dsql_migrator.core.cdc_schema_evolve import (
        apply_add_columns,
        plan_add_columns,
        read_source_columns,
        read_target_columns,
    )
    from dsql_migrator.core.target_connection import DsqlConnector
    from dsql_migrator.ui.connect import make_source_engine_factory

    source_config = getattr(session, "source_config", None)
    target_config = getattr(session, "target_config", None)
    if source_config is None or target_config is None:
        ui.notify(  # type: ignore[attr-defined]
            "Connect the source and target first (Step 1).", type="warning"
        )
        return

    connector = DsqlConnector(
        target_config, aws_profile=getattr(session, "aws_profile", None)
    )

    def _build_plan():
        engine = make_source_engine_factory(getattr(session, "source_password", None))(
            source_config
        )
        raw = engine.raw_connection()
        try:
            source_columns = read_source_columns(raw, table)
        finally:
            raw.close()
        if not source_columns:
            # An empty source read is NOT "the target is already current" -- the
            # drifting table demonstrably exists on the source (it is dead-lettering
            # rows). Reading zero columns means the name did not resolve (renamed /
            # dropped on the source, or a topic->table mapping mismatch), so report
            # that explicitly rather than let the empty diff read as "nothing to do".
            raise ValueError(
                f"could not read source columns for '{table}' -- the table may have "
                "been renamed or dropped on the source, or its CDC topic maps to a "
                "different name. Verify the source table before adding columns."
            )
        target = connector.connect()
        try:
            target_columns = read_target_columns(target, table)
        finally:
            target.close()
        return plan_add_columns(table, source_columns, target_columns)

    try:
        plan = await run.io_bound(_build_plan)
    except Exception as exc:  # noqa: BLE001 - surfaced, never crashes the monitor
        ui.notify(f"Could not read the schemas: {exc}", type="negative")  # type: ignore[attr-defined]
        return

    if plan.is_empty and not plan.skipped:
        # The target already matches: the DLQ rows are still set aside, so point at
        # the backfill rather than implying there is nothing left to do.
        ui.notify(  # type: ignore[attr-defined]
            f"{table}: the target already has every source column. Use per-table "
            "Reload to backfill the dead-lettered rows.",
            type="info",
        )
        return

    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 560px"):  # type: ignore[attr-defined]
        section_header(ui, icon="schema", title=f"Add missing columns to {table}")
        if plan.steps:
            ui.label(  # type: ignore[attr-defined]
                "These statements will run on the target, one per transaction. Each "
                "column is added NULLable with no default: existing rows read NULL "
                "until you backfill them, and new change events carry the real value."
            ).classes("text-xs text-gray-700")
            ui.code(plan.ddl_text, language="sql").classes("w-full text-xs")  # type: ignore[attr-defined]
            for step in plan.steps:
                if step.warning:
                    render_notice(
                        ui,
                        tone="warning",
                        header=f"{step.column} ({step.source_type})",
                        body=step.warning,
                    )
        for skipped in plan.skipped:
            render_notice(
                ui,
                tone="warning",
                header=f"{skipped.column} is not included",
                body=(
                    f"source type {skipped.source_type}: {skipped.reason}. Add this "
                    "column by hand if you need it."
                ),
            )
        render_notice(
            ui,
            tone="info",
            header="After applying",
            body=(
                "CDC resumes applying new changes for this table on its own. The rows "
                "already dead-lettered are NOT replayed -- use per-table Reload to "
                "backfill them."
            ),
        )

        async def _apply() -> None:
            dialog.close()  # type: ignore[attr-defined]
            try:
                outcomes = await run.io_bound(
                    apply_add_columns, plan, connector.connect
                )
            except Exception as exc:  # noqa: BLE001
                ui.notify(f"Apply failed: {exc}", type="negative")  # type: ignore[attr-defined]
                return
            applied = [o for o in outcomes if o.applied]
            failed = [o for o in outcomes if not o.applied]
            from dsql_migrator.core.activity_log import (
                ActivityCategory,
                ActivityStatus,
                log_activity,
            )

            for outcome in outcomes:
                log_activity(
                    ActivityCategory.CDC,
                    "add column to target (schema drift)",
                    status=(
                        ActivityStatus.SUCCESS if outcome.applied
                        else ActivityStatus.FAILURE
                    ),
                    target=f"{table}.{outcome.column}",
                    detail=outcome.error or outcome.ddl,
                )
            if failed:
                ui.notify(  # type: ignore[attr-defined]
                    f"Added {len(applied)} column(s); {failed[0].column} failed: "
                    f"{failed[0].error}",
                    type="negative",
                )
            else:
                ui.notify(  # type: ignore[attr-defined]
                    f"Added {len(applied)} column(s) to {table}. Use per-table Reload "
                    "to backfill the dead-lettered rows.",
                    type="positive",
                )
            if on_refresh is not None:
                on_refresh()

        with ui.row().classes("w-full justify-end gap-2"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat no-caps")  # type: ignore[attr-defined]
            if plan.steps:
                ui.button(  # type: ignore[attr-defined]
                    f"Apply {len(plan.steps)} statement(s)", on_click=_apply
                ).props("no-caps color=primary")
    dialog.open()  # type: ignore[attr-defined]


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


def _cdc_dlq_facts(migration_state, log_key: str) -> str:
    """Assemble a credential-free, row-free DLQ facts block for the AI DBA chat.

    Reuses the CDC-assist error summarizer over the CDC-sourced dead-letter records
    (depth + top tables + top SQLSTATEs + a capped sample), so the chat is grounded on
    the real poison records without any row values leaving the tool (Property 7)."""
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
    activity = getattr(migration_state, "cdc_activity", None)
    if bool(getattr(activity, "sink_stall_confirmed", False)):
        lines.append(
            "Sink stalled: yes (a zero DLQ depth would be EXPECTED during a stall)."
        )
    lines.append("Sample dead-lettered events:")
    lines.append(_summarize_errors(records))
    return "\n".join(lines)


def _cdc_drift_facts(status_view: LoadStatusView) -> str:
    """Assemble a credential-free schema-drift facts block for the AI DBA chat."""
    drift = getattr(status_view, "schema_drift", None) or []
    lines = ["Source schema drift detected (CDC does not replicate DDL):"]
    for group in drift:
        noun = "record" if group.count == 1 else "records"
        lines.append(
            f"- {group.table}: {group.kind} — {group.count} {noun} dead-lettered"
        )
    return "\n".join(lines)


def _render_cdc_ai_button(ui, on_click, tooltip: str) -> None:
    """The shared 'Ask AI DBA' affordance for the CDC DLQ / drift panels."""
    ui.button("Ask AI DBA", on_click=on_click).props(  # type: ignore[attr-defined]
        "flat dense no-caps size=sm color=indigo-6 icon=auto_awesome"
    ).tooltip(tooltip)


def _render_cdc_schema_drift_banner(
    ui, status_view: LoadStatusView, session=None, on_refresh=None, cdc_ai_opener=None
) -> None:
    """Flag source DDL the target has not caught up to (from the classified DLQ).

    CDC does not propagate DDL: when the source alters a table, the first row under
    the new schema is rejected by DSQL and dead-lettered. The control plane derives
    the drift kind from the quarantine SQLSTATE (see ``classify_schema_drift``); this
    band names the affected table(s) + what changed and points at the manual
    recovery, so a reader is not left to reverse-engineer a rising DLQ count. Renders
    nothing when no drift was detected (the common case), so it is inert on a healthy
    stream.
    """
    drift = getattr(status_view, "schema_drift", None) or []
    if not drift:
        return
    bg, border, icon_color, _icon = NOTICE_STYLE.get("warning", NOTICE_STYLE["info"])
    with ui.column().classes(  # type: ignore[attr-defined]
        f"w-full gap-1 rounded-md border {border} {bg} p-2"
    ):
        with ui.row().classes("w-full items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            ui.icon("schema").classes(f"{icon_color} text-lg")  # type: ignore[attr-defined]
            ui.label("Source schema change detected").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-900"
            )
            if cdc_ai_opener is not None:
                ui.space()  # type: ignore[attr-defined]
                _render_cdc_ai_button(
                    ui,
                    lambda: cdc_ai_opener(
                        "drift", _cdc_drift_facts(status_view), _CDC_DRIFT_SEED
                    ),
                    "Ask AI DBA what DDL to apply and how to backfill the drifted rows.",
                )
        for group in drift:
            label, why = _DRIFT_LABELS.get(
                group.kind, (group.kind, "the source schema changed")
            )
            noun = "record" if group.count == 1 else "records"
            with ui.row().classes("w-full items-center gap-2"):  # type: ignore[attr-defined]
                ui.label(  # type: ignore[attr-defined]
                    f"{group.table}: {label} — {why}. {group.count} {noun} dead-lettered."
                ).classes("text-xs text-gray-800")
                # ADD COLUMN is the only additive (non-destructive) drift, so it is
                # the only one we offer to fix. A DROP/type change can rewrite or
                # destroy target data, so those stay alert-only -- see the runbook
                # line below. The action still requires an explicit approval of the
                # rendered DDL (Property 6: never a silent schema mutation), and it
                # is hidden entirely when no session is wired (e.g. unit tests).
                if group.kind == "add-column" and session is not None:
                    ui.button(  # type: ignore[attr-defined]
                        "Fix target schema…",
                        on_click=lambda _e=None, table=group.table: (
                            _open_add_column_dialog(
                                ui, session, table, on_refresh=on_refresh
                            )
                        ),
                    ).props("flat dense no-caps size=sm color=primary")
        # One shared runbook line: CDC cannot apply the DDL for you (Property 6).
        ui.label(  # type: ignore[attr-defined]
            "CDC does not replicate DDL. Apply the matching change to the target "
            "schema (e.g. ALTER TABLE), then use per-table Reload to backfill the "
            "rows that were set aside. Stop CDC first if a column was dropped or "
            "retyped, since those may require recreating the table."
        ).classes("text-xs italic text-gray-600")


def _render_cdc_dlq_panel(
    ui,
    migration_state,
    job_manager,
    status_view: LoadStatusView,
    on_refresh=None,
    session=None,
    cdc_ai_opener=None,
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
    bg, border, icon_color, default_icon = NOTICE_STYLE.get(tone, NOTICE_STYLE["info"])
    with ui.column().classes(  # type: ignore[attr-defined]
        f"w-full gap-2 rounded-md border {border} {bg} p-3"
    ):
        # Header band: icon + title + depth badge (left), refresh (right).
        with ui.row().classes("w-full items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            ui.icon(default_icon).classes(f"{icon_color} text-lg")  # type: ignore[attr-defined]
            ui.label("Dead-letter queue (poison records)").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-900"
            )
            _badge_color = (
                "negative"
                if tone == "error"
                else "warning"
                if tone == "warning"
                else "grey-6"
                if health.depth == 0
                else "primary"
            )
            ui.badge(  # type: ignore[attr-defined]
                f"{health.depth} quarantined"
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
                        "dlq", _cdc_dlq_facts(migration_state, k), _CDC_DLQ_SEED
                    ),
                    "Ask AI DBA to triage these poison records and how to fix them.",
                )
            if on_refresh is not None:
                ui.button(on_click=on_refresh).props(  # type: ignore[attr-defined]
                    "flat dense round size=sm icon=refresh"
                ).tooltip("Refresh dead-letter records from CloudWatch")
        ui.label(health.message).classes("text-xs text-gray-700")  # type: ignore[attr-defined]
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
        )
        _render_cdc_dlq_breakdown(ui, status_view)
        # CDC commonly has no Full Load job_id this session, so key the record list /
        # download off the same stable CDC key the fold used (cdc_error_log_key) --
        # not _current_job, which would be None and hide everything.
        log_key = cdc_error_log_key(migration_state)
        _render_cdc_dlq_records(ui, migration_state, log_key)
        if health.depth > 0:
            _render_cdc_error_download(ui, migration_state, log_key)
        _render_full_load_quarantine_pointer(ui, migration_state, log_key)

def _render_full_load_quarantine_pointer(ui, migration_state, log_key: str) -> None:
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
    try:
        records = migration_state.error_log.records(log_key) or []
    except Exception:  # noqa: BLE001 - advisory line; never break the panel
        return
    full_load = [r for r in records if not is_cdc_error_record(r)]
    if not full_load:
        return
    noun = "row" if len(full_load) == 1 else "rows"
    ui.label(  # type: ignore[attr-defined]
        f"The Full Load also set {len(full_load)} {noun} aside. Those are separate from "
        "this dead-letter queue (the batch loader isolated them; they never entered the "
        "stream) — see the Full Load section above for the details."
    ).classes("text-xs text-gray-500")


def _render_cdc_dlq_breakdown(ui, status_view: LoadStatusView) -> None:
    """Show which tables produced DLQ/error records, so a poison source is found.

    Reads the per-table counts already on the view's
    :class:`~dsql_migrator.core.models.ErrorLogSummary` (no new query) and renders
    them as compact, always-visible per-table chips (table xN), sorted by count.
    Chips (not a collapsible sub-table) keep the at-a-glance "where is the poison
    coming from" answer visible without a click and survive the ~5s re-render
    without snapping shut. Nothing shown when there are no per-table counts (a
    clean stream or errors not yet attributed to a table).
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

# Cap the inline DLQ record list so a flood of poison rows never renders an
# unbounded table in the browser; the full set is always in the downloadable log.
_DLQ_RECORD_LIST_LIMIT = 200

# Rows shown per page in the record table; paging keeps a large DLQ readable
# (a fixed, scrollable viewport) instead of an ever-growing wall of rows.
_DLQ_RECORD_PAGE_SIZE = 10

def _render_cdc_dlq_records(ui, migration_state, log_key: str) -> None:
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
            "message": r.message,
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
        pagination={"rowsPerPage": _DLQ_RECORD_PAGE_SIZE, "sortBy": "when",
                    "descending": True},
    ).classes("w-full text-xs bg-white rounded-md")
    # A search box so a specific table/SQLSTATE/reason is findable in a large DLQ.
    table.props('flat bordered dense')
    with table.add_slot("top-left"):  # type: ignore[attr-defined]
        ui.input(placeholder="Filter records").props(  # type: ignore[attr-defined]
            "dense clearable borderless"
        ).bind_value(table, "filter")

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
            payload = migration_state.error_log.render_records(records)
            ui.download.content(  # type: ignore[attr-defined]
                payload, f"cdc_error_log_{safe}.ndjson", "application/x-ndjson"
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

def lob_exclusion_lock(
    migration_state, job_manager, *, full_load_committed: bool = False
) -> tuple[bool, Optional[str]]:
    """Return ``(locked, reason)`` for the LOB-exclusion tick boxes.

    The selection is not a live setting: it is baked into the cdc-stack's
    ``ColumnExcludeList`` parameter when the infrastructure is created, and handed to
    the source connector at Start CDC. So once either operation is under way -- or the
    connectors exist -- a further tick changes state that nothing will read, and the box
    silently lies about what CDC captures. The boxes previously had no lock at all: a
    tick registered and the state really changed, it just never reached the pipeline.

    Every locked case now NAMES its reason. An earlier version left the
    "infrastructure exists" case silent on the theory that the greyed-out boxes were
    self-explanatory -- but a stopped-CDC operator who sees a ticked-and-frozen box
    with no explanation cannot tell WHY it is closed or how to change it (they read
    it as a bug). So the reason is always given; severity is carried by the render
    TONE (neutral for these expected/no-action states, not an alarming warning), not
    by withholding the text.

    Why the exclusion is locked once infrastructure exists (not merely once CDC is
    streaming): the excluded-column set is captured into the pipeline, and once CDC
    has streamed even once its resume offset is committed on MSK (a Stop deletes only
    the connectors, not the offset). Changing the exclusion then would leave the rows
    already migrated/streamed under the old set inconsistent with rows processed
    after -- silent partial data. The safe way to change it is to delete the CDC
    infrastructure and redeploy with the new set (a fresh offset + re-run), which is
    the remedy named below -- matching how the table picker locks on the same phase.

    ``full_load_committed`` is the FIRST and strongest clause: a Full Load has already
    committed rows to the target under a specific exclusion set (a completed
    ``full_load_only`` run, whose ``WorkflowStep.FULL_LOAD`` status stays ``DONE`` and
    whose ``job_id`` survives even after the type is switched to ``cdc_only`` to add
    replication). Editing the exclusion THEN is silent split-brain in EITHER direction:
    UN-excluding a column CDC then captures for post-snapshot changes while the loaded
    rows stay NULL; ADDING one makes CDC drop updates to a column the load populated,
    going stale. This clause is NOT released by deleting the cdc-stack (the load really
    ran against this set), so the only re-scope is Start over -- exactly like
    ``selection_lock_reason``'s ``has_job or status is DONE`` clause, which governs the
    Full Load screen's copy of this card. Without it, switching to ``cdc_only`` after a
    load moved the card to this (pre-deploy, weakly-locked) home and left it editable.

    Pure apart from reading the job's status through ``job_manager``; no AWS I/O.
    """
    if full_load_committed:
        return True, (
            "Locked — a Full Load has already loaded data using this exclusion set, so "
            "the excluded columns are fixed for this migration (changing them now would "
            "leave the already-loaded rows inconsistent with what CDC captures — the "
            "un-excluded column would be NULL on loaded rows but populated on later "
            "changes, or vice versa). To migrate a different column set, use 'Start over'."
        )
    if cdc_streaming_started(migration_state, job_manager):
        return True, (
            "Locked — the excluded columns were handed to the source connector when "
            "CDC started. Stop CDC to change them."
        )
    # An infra create is in flight: the parameter went out with the stack, but the
    # operator was just here choosing, so say what happened.
    if cdc_infra_deploy_in_flight(migration_state, job_manager):
        return True, (
            "Locked while the CDC infrastructure is being created — the excluded "
            "columns are part of the stack's parameters and were submitted with it."
        )
    if getattr(migration_state, "cdc_stack_phase", None) in (
        "infra",
        "running",
        "provisioning",
        "partial",
    ):
        # Infrastructure is deployed (e.g. after a Stop CDC, which keeps the stack and
        # its committed offset). Explain the lock and name the only safe remedy --
        # changing the exclusion on a pipeline that has already streamed would diverge
        # already-migrated rows from later ones.
        return True, (
            "Locked — the CDC infrastructure is deployed for this table set, so the "
            "excluded columns are fixed for this pipeline (changing them after CDC has "
            "streamed would leave already-migrated rows inconsistent with later ones). "
            "To change them, delete the CDC infrastructure on the CDC step first, then "
            "redeploy."
        )
    return False, None


def _render_cdc_lob_exclusion_panel(
    ui,
    migration_state,
    inventory: Optional[SourceInventory],
    refresh,
    *,
    locked: bool,
    lock_reason: Optional[str] = None,
    migration_wide: bool = False,
    selected_tables: Optional[Sequence[str]] = None,
) -> None:
    """Render the explicit, opt-in oversized-LOB column exclusion (H13).

    Lists the columns the evaluation flagged as able to exceed the DSQL 1 MiB
    per-value limit and lets the user exclude them. The selection is
    migration-wide: a ticked column is dropped from BOTH the Full Load INSERT list
    and CDC capture (Debezium ``column.exclude.list``) -- one choice, so the two
    data paths never disagree across the gapless handoff. Excluding is the only
    safe handling for values that can also exceed the broker limit -- runtime
    isolation can't recover those. No silent loss: nothing is excluded unless the
    user ticks it, and (in the CDC context) the resulting list is shown verbatim.

    ``migration_wide`` switches the copy: when True the card is rendered on the
    Full Load screen (before the load), so it speaks of "this migration"; when
    False it renders inside the CDC sub-flow and speaks of CDC capture (keeping the
    familiar wording and the ``column.exclude.list`` preview there). The underlying
    selection is the same either way.

    When no oversized-LOB column qualifies (the common case), there is nothing to
    configure -- so instead of a full section the panel collapses to a single calm
    INFO notice (AWS-style), keeping the result discoverable without the visual
    weight of a settings card. The full opt-in card is shown only when there are
    actual candidates to exclude.
    """
    # Scope the candidates to the tables that will ACTUALLY be migrated. The caller
    # passes the resolved EFFECTIVE selection so picking one schema lists only that
    # schema's LOB columns, even though ``migration_state.selection`` is still the
    # empty "= all" default until the picker is touched (see scope_lob_candidates).
    candidates = scope_lob_candidates(
        lob_exclusion_candidates(inventory),
        selected_tables=selected_tables,
        stored_selection=migration_state.selection.selected_tables,
    )
    selection = migration_state.lob_exclusions()
    if not candidates:
        # Nothing to exclude -> a lightweight info notice, not a heavy settings card.
        render_notice(
            ui,
            tone="info",
            icon="data_object",
            header="No oversized LOB columns",
            body=(
                "No MySQL LOB/TEXT columns in the selected tables can exceed the "
                f"Aurora DSQL {_DSQL_VALUE_LIMIT_MIB} MiB value limit — nothing "
                "needs excluding from "
                + ("this migration." if migration_wide else "CDC capture.")
            ),
        )
        return
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            ui.icon("data_object", color="warning").classes("text-base")  # type: ignore[attr-defined]
            ui.label("Oversized LOB columns (optional exclusion)").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold"
            )
        if migration_wide:
            ui.label(  # type: ignore[attr-defined]
                "These MySQL LOB/TEXT columns can hold values over the Aurora DSQL "
                f"{_DSQL_VALUE_LIMIT_MIB} MiB limit. Ticking one drops it from this "
                "migration entirely — the Full Load never writes it and (if CDC is "
                "used) capture excludes it too, so the two stay in lockstep. Leave a "
                "column ticked-off to load it normally; any single value that then "
                "exceeds the limit is quarantined per-row instead. Nothing is "
                "excluded unless you tick it."
            ).classes("text-xs text-gray-500")
        else:
            ui.label(  # type: ignore[attr-defined]
                "These MySQL LOB/TEXT columns can hold values over the Aurora DSQL "
                f"{_DSQL_VALUE_LIMIT_MIB} MiB limit. A value over the "
                f"{_BROKER_MESSAGE_LIMIT_MIB} MiB broker limit can't be streamed at "
                "all, so exclude such columns here to keep CDC from stalling. "
                "Nothing is excluded unless you tick it."
            ).classes("text-xs text-gray-500")
        # Always explain the lock when one is in effect. These are expected,
        # no-action-needed states (a deployed/started CDC pipeline), so the reason
        # renders NEUTRAL, not as an amber warning -- severity calibration: a normal
        # state must not read as a problem, but it must still say why the boxes are
        # frozen and how to change them (a silent frozen box reads as a bug).
        if locked and lock_reason:
            inline_hint(ui, lock_reason, tone="neutral")  # type: ignore[attr-defined]
        for candidate in candidates:
            excluded = selection.get(candidate.table, set())
            for column in candidate.columns:
                def _toggle(
                    event, _table=candidate.table, _column=column
                ) -> None:
                    migration_state.set_lob_exclusion(
                        _table, _column, bool(event.value)
                    )
                    if callable(refresh):
                        refresh()

                _box = ui.checkbox(  # type: ignore[attr-defined]
                    f"{candidate.table}.{column}",
                    value=column in excluded,
                    on_change=None if locked else _toggle,
                ).props("dense")
                if locked:
                    # disable() greys the box AND stops the click, so the visual state
                    # and the actual behaviour agree -- greying alone would still let
                    # the tick through.
                    _box.disable()
        # The Debezium ``column.exclude.list`` preview is a CDC-connector detail, so
        # show it only in the CDC context. On the Full Load screen the same
        # selection just means "not written", so the raw connector value would be
        # noise there.
        if not migration_wide:
            exclude_value = format_column_exclude_list(
                {table: sorted(cols) for table, cols in selection.items()}
            )
            if exclude_value:
                with ui.row().classes("items-center gap-1 no-wrap flex-wrap"):  # type: ignore[attr-defined]
                    ui.label("column.exclude.list:").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-500"
                    )
                    ui.label(exclude_value).classes("text-xs font-mono")  # type: ignore[attr-defined]

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
