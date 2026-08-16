# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Full Load data-plane render helpers (extracted from ``data_migration/__init__``).

The Full Load step/progress/results rendering -- watermark, the load-status table,
the ~830-line ``_render_full_load_step``, the per-cell/tooltip formatters, the
quarantine helpers, the progress/completeness/error-log renderers -- plus the tiny
``format_selected_workloads`` headline. Mirrors the existing ``_cdc_ui.py`` split and
is re-exported at the bottom of the package ``__init__`` so ``dm.<name>`` and
``from ...data_migration import <name>`` resolve unchanged. One-directional: this
module imports from ``_engine`` / ``_models`` / ``_status`` / ``_cdc_ui`` and the
shared ``core`` + ``ui`` modules -- never back from the package ``__init__``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional, Sequence

from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import LoadKind, LoadStatusView, MigrationJob, StepStatus
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.target_introspector import target_primary_keys, tables_with_rows
from dsql_migrator.ui.design import inline_hint, render_notice
from dsql_migrator.ui.workflow import WorkflowStep, with_status
from dsql_migrator.ui.data_migration._engine import (
    full_load_progress_caption,
    job_status_to_step_status,
)
from dsql_migrator.ui.data_migration._models import (
    FullLoadCompleteness,
    FullLoadTableRow,
    _LOAD_STATE_ORDER,
    _UNAVAILABLE,
    build_full_load_table_rows,
    format_error_summary,
    format_table_timing,
    format_watermark,
    full_load_completeness,
    summarize_table_states,
    unsettled_table_names,
)
from dsql_migrator.ui.data_migration._status import (
    _current_job,
    full_load_error_records,
    full_load_error_summary,
    full_load_latest_messages,
)
from dsql_migrator.ui.data_migration._cdc_ui import cdc_streaming_started

_LOGGER = logging.getLogger(__name__)

# How often the live Full Load progress region polls the background job (seconds).
_POLL_INTERVAL_SECONDS = 1.5

# Cloudscape "Alert" renderer alias (single source of truth in ui.design).
_render_notice = render_notice

def _render_watermark(ui: object, job: MigrationJob) -> None:
    """Render the export watermark for ``job`` (Requirement 8.5 / Property 11).

    Compact by design: this is provenance the operator reads once (or copies into a
    runbook), not something to watch, so it must not occupy the height a 4-row
    two-column ``ui.table`` did -- complete with sortable "Field"/"Value" headers for
    four fixed rows. Each coordinate is now a labelled monospace line inside one bordered
    panel: the label identifies the field, the monospace value is scannable and
    copy-pasteable, and unavailable fields are muted so the eye skips them instead of
    reading "unavailable" as an error.
    """
    if job.watermark is None:
        ui.label("Export watermark").classes(  # type: ignore[attr-defined]
            "text-sm font-semibold text-gray-700"
        )
        ui.label(  # type: ignore[attr-defined]
            "The export consistency point is captured when the migration starts."
        ).classes("text-sm text-gray-500")
        return

    display = format_watermark(job.watermark)
    with ui.element("div").classes(  # type: ignore[attr-defined]
        "w-full rounded-md border border-gray-200 bg-gray-50 p-3"
    ):
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("bookmark").classes("text-gray-500 text-base")  # type: ignore[attr-defined]
            ui.label("Export watermark").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-gray-800"
            )
            # The summary IS the identifying line (snapshot time + coordinate), so it
            # sits on the header row rather than as a separate paragraph.
            ui.label(display.summary).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-500 truncate"
            )
        for label, value in (
            ("Snapshot (UTC)", display.snapshot_timestamp),
            ("Binlog file:pos", display.coordinate),
            ("GTID set", display.gtid),
            ("Server UUID", display.server_uuid),
        ):
            # An unavailable coordinate is normal (binary logging off, or SHOW MASTER
            # STATUS restricted on RDS/Aurora), so it is muted -- not styled like a
            # missing required value.
            unavailable = str(value).strip().lower() == _UNAVAILABLE
            with ui.row().classes("items-baseline gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                ui.label(label).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500 shrink-0 w-32"
                )
                ui.label(value).classes(  # type: ignore[attr-defined]
                    "text-xs font-mono break-all "
                    + ("text-gray-400 italic" if unavailable else "text-gray-800")
                )

        if display.table_row_counts:
            approximate = getattr(job.watermark, "row_counts_approximate", False)
            count = len(display.table_row_counts)
            heading = (
                f"Per-table snapshot rows ({count}, estimated)"
                if approximate
                else f"Per-table snapshot rows ({count})"
            )
            # INSIDE the watermark panel and styled like its other fields. It used to sit
            # outside as a full-width Quasar expansion wrapping a bordered ``ui.table``
            # with its own sortable headers -- a second visual container hanging below the
            # panel it belongs to, in a style nothing else on the screen uses. The counts
            # are one value per table, so labelled monospace rows (the same shape as the
            # coordinates above) read as more of this panel's detail rather than a
            # separate data grid.
            # Quasar's default expansion header is a grey full-bleed bar with a large
            # leading glyph -- a heavy band across an otherwise flat panel, in the one
            # place that should read as a quiet "more detail" affordance. Strip the fill
            # and the icon and size the header like the field labels above it, so opening
            # it feels like unfolding more of the same panel.
            with ui.expansion(heading).classes(  # type: ignore[attr-defined]
                "w-full"
            ).props(
                "dense dense-toggle expand-separator=false "
                "header-class='text-xs text-gray-500 px-0'"
            ):
                if approximate:
                    ui.label(  # type: ignore[attr-defined]
                        "Estimated from the source catalog (no COUNT(*) scan) to spare "
                        "the source; exact counts are verified in Validation."
                    ).classes("text-xs text-gray-400 pb-1")
                for table, rows_count in display.table_row_counts.items():
                    # SAME two-column shape as the coordinates above: a fixed-width label
                    # column then the value, so the table names and the counts each line
                    # up with the fields they sit under instead of forming a second,
                    # differently-aligned list.
                    with ui.row().classes(  # type: ignore[attr-defined]
                        "items-baseline gap-2 no-wrap w-full"
                    ):
                        ui.label(table).classes(  # type: ignore[attr-defined]
                            "text-xs text-gray-500 shrink-0 w-64 truncate"
                        )
                        # Monospace, matching the coordinate values -- a column of counts
                        # then lines up on the digits and is comparable at a glance.
                        ui.label(f"{rows_count:,}").classes(  # type: ignore[attr-defined]
                            "text-xs font-mono text-gray-800"
                        )


def _render_load_status(ui, view: LoadStatusView) -> None:
    """Render the unified load status (Full Load or CDC) -- Req 13.1.

    One component for both modes: Full Load shows the progress bar + terminal
    summary; CDC shows continuous lag / "caught up to" / DLQ depth / connector
    states. Both show the same per-table table (with an errors column sourced
    from the single error log).
    """
    heading = "Migration progress" if view.kind == LoadKind.FULL_LOAD else "CDC status"
    ui.label(heading).classes("text-lg font-semibold")

    if not view.tables and view.kind == LoadKind.FULL_LOAD:
        ui.label("No tables to migrate.").classes("text-sm text-gray-500")
        return

    if view.progress_pct is not None:
        ui.linear_progress(value=view.progress_pct / 100.0, show_value=False).props(
            "instant-feedback"
        ).classes("w-full")
        ui.label(
            f"Summary — {view.progress_pct:.1f}% complete, "
            f"{view.tables_done}/{len(view.tables)} tables done, "
            f"{view.tables_failed} failed"
        ).classes("text-sm text-gray-600")

    if view.kind == LoadKind.CDC:
        bits: list[str] = []
        if view.lag_seconds is not None:
            bits.append(f"lag {view.lag_seconds:.1f}s")
        if view.caught_up_to is not None:
            bits.append(f"caught up to {view.caught_up_to.isoformat()}")
        if view.dlq_depth is not None:
            bits.append(f"DLQ depth {view.dlq_depth}")
        if bits:
            ui.label("  ·  ".join(bits)).classes("text-sm text-gray-600")
        if view.connector_states:
            states = ", ".join(
                f"{name}={state}" for name, state in view.connector_states.items()
            )
            ui.label(f"Connectors: {states}").classes("text-xs text-gray-500")

    columns = [
        {"name": "table", "label": "Table", "field": "table", "align": "left"},
        {"name": "state", "label": "Status", "field": "state"},
        {"name": "rows_loaded", "label": "Rows loaded", "field": "rows_loaded"},
        {"name": "errors", "label": "Errors", "field": "errors"},
    ]
    rows = [
        {
            "table": row.table,
            "state": row.state,
            "rows_loaded": row.rows_loaded if row.rows_loaded is not None else "",
            "errors": row.errors,
        }
        for row in view.tables
    ]
    ui.table(columns=columns, rows=rows, row_key="table").classes("w-full")


def format_selected_workloads(names: Sequence[str]) -> str:
    """Return a short headline for the tables a Full Load will migrate."""
    count = len(names)
    if count == 0:
        return "No tables selected"
    noun = "table" if count == 1 else "tables"
    return f"{count} {noun} selected for Full Load"


def _render_full_load_step(
    ui,
    migration_state,
    job_manager,
    session,
    *,
    job,
    status,
    selected_names,
    guard_reason,
    start_full_load,
    retry_failed_load,
    reload_table,
    accept_quarantine_and_continue,
    stop_full_load,
    refresh,
    retry_tables=None,
    ai_error_opener=None,
    schema_recreate_candidates: Optional[Sequence[str]] = None,
) -> None:
    """Render the Full Load step: confirm the selected workloads, then run it.

    Re-surfaces the tables chosen in the object browser as an explicit
    confirmation (Usability-first) and gates the start on the Full Load
    prerequisite verdict. Starting opens a confirmation dialog listing exactly
    what will be migrated before the snapshot export/load begins. While a job
    runs (or after it finishes) the watermark, live per-table progress (rows and
    percent vs the source snapshot count), retry of only the failed tables, a
    source-vs-loaded completeness verdict, and the downloadable error log are
    shown here.

    ``schema_recreate_candidates`` names the tables whose target the load will DROP and
    recreate to apply a primary key that differs from the source (see
    :func:`schema_recreate_tables`). Passed in because resolving it needs the inventory
    and the applied conversions, which the caller owns; the confirm dialog discloses
    whichever of them the current action covers.
    """
    ui.label("Workloads to migrate").classes("text-sm font-semibold")
    ui.label(format_selected_workloads(selected_names)).classes(
        "text-sm text-gray-600"
    )
    if selected_names:
        with ui.row().classes("items-center gap-1 flex-wrap"):
            for name in selected_names:
                ui.badge(name).props("color=blue-grey-6 outline")
    # No explanatory caption here. Its three claims are each already stated somewhere the
    # reader is better served:
    #   * "ticked in the object browser above" -- the picker's own caption says where the
    #     selection came from, and the badges listing it sit directly above this line;
    #   * "as of one consistency watermark" -- the Export watermark panel shows the actual
    #     coordinate, not just the promise of one;
    #   * "the source is read only" -- the confirm dialog says it at the moment the user
    #     commits, which is where a reassurance about writes actually matters.
    # Restating all three as standing grey text taught nothing on the second read.

    # Explicit confirmation before the (target-writing) Full Load begins. Tables
    # that already hold data offer a Drop-vs-Append choice; CDC-live is a hard
    # warning. Both are computed fresh inside the dialog builder below (so the
    # periodic poll re-render can't tear a stale value into the dialog).

    def _open_confirm_dialog_now(
        *, action_tables, on_confirm, title, table_reasons=None
    ) -> None:
        """Build + open the Full-Load confirm dialog in the TOP-LEVEL client.

        Shared by Start, Re-run-all, Retry-failed and per-table Reload:
        ``action_tables`` is the set being (re)loaded (drives the summary/badges),
        ``on_confirm`` is the action to run when confirmed, and ``title`` is the
        dialog heading. Built in the client context (not the per-render content
        slot) and opened on demand, so the periodic progress-poll re-render does
        NOT tear it down a couple of seconds after it appears. The Drop-vs-Append
        choice (when the target already holds data) applies to whichever action
        this dialog wraps, so a retry/Reload honors the SAME choice as Start.

        When ``table_reasons`` (a ``{table: failure_reason}`` map) is given -- the
        Retry-failed case -- the dialog renders ``action_tables`` as a **checklist**
        (all pre-checked) with each table's failure reason, so the user can uncheck
        tables they aren't ready to retry (e.g. a not-yet-fixed source value) and
        retry only the rest. ``on_confirm`` is then called with the checked subset;
        otherwise it is called with no arguments.
        """
        from nicegui import context as _ctx

        client = _ctx.client
        tables_with_data_now = sorted(migration_state.tables_with_data)
        cdc_live_now = cdc_streaming_started(migration_state, job_manager)
        # Empty targets whose primary key the load will RECREATE from the applied DDL.
        # A changed key is a schema change, so appending cannot deliver it; recreating an
        # empty table destroys nothing, which is why the load does it without asking --
        # but it must still be disclosed here, because a target DDL the user edited by
        # hand after Schema Conversion is replaced. Never includes a populated table:
        # those go through the explicit Drop & reload choice below. Resolved by the
        # caller (which owns the inventory + applied conversions) and narrowed to the
        # tables THIS dialog is about; a live sink forbids recreating anything.
        #
        # ``schema_recreate_candidates`` MUST be a callable, resolved here -- after the
        # pre-dialog probe has read the targets' real primary keys -- because a target
        # that already carries the applied key needs no recreate, and announcing one
        # contradicts what the user just did in Schema Conversion. Evaluating it at
        # render time (as a plain list) can only compare the applied DDL against the
        # SOURCE key, which is exactly what produced the false disclosure.
        #
        # Rejected rather than tolerated: accepting a list too would let a caller
        # silently regress to render-time evaluation, and the dialog would still look
        # correct while disclosing a recreate that does not happen.
        if not callable(schema_recreate_candidates):
            raise TypeError(
                "schema_recreate_candidates must be a callable resolved after the "
                "target probe, not a pre-computed list -- a list can only have been "
                "built before any target primary key was read."
            )
        _candidates = schema_recreate_candidates() or ()
        recreate_now = (
            [] if cdc_live_now else [n for n in _candidates if n in set(action_tables)]
        )
        selectable = table_reasons is not None
        # Live set of checked tables (mutated by the per-row checkboxes). Starts as
        # the full action set (all pre-checked) -- the common "retry everything".
        checked: set = set(action_tables)

        def _build() -> None:
            with ui.dialog() as confirm_dialog, ui.card().classes("min-w-[360px]"):
                ui.label(title).classes("text-lg font-semibold")
                ui.label(
                    f"{format_selected_workloads(action_tables)}. The target "
                    "tables will receive the snapshot rows; the source is accessed "
                    "read only."
                ).classes("text-sm")
                if cdc_live_now:
                    render_notice(
                        ui,
                        tone="error",
                        header="CDC is currently streaming",
                        body=(
                            "Re-running Full Load now will collide with the live "
                            "pipeline -- the snapshot (and any DROP+recreate) writes "
                            "to tables the CDC sink is actively applying changes to, "
                            "which can drop streamed rows or create a gap/overlap "
                            "(CDC resumes from the ORIGINAL watermark, not this new "
                            "one). Stop CDC first (CDC step → Stop CDC), re-run the "
                            "Full Load, then start CDC again so it resumes from the "
                            "new snapshot."
                        ),
                    )
                # Retry-failed: a checklist (all pre-checked) with each table's
                # failure reason, so the user can uncheck tables not ready to retry
                # and retry only the rest. Other actions just show badges.
                if selectable and action_tables:
                    ui.label(
                        "Uncheck any table you're not ready to retry yet (e.g. a "
                        "source value you haven't fixed):"
                    ).classes("text-xs text-gray-500")
                    with ui.column().classes(
                        "w-full gap-1 max-h-60 overflow-auto"
                    ):
                        for name in action_tables:
                            reason = (table_reasons or {}).get(name, "")

                            def _toggle(e: object, n=name) -> None:
                                if bool(getattr(e, "value", False)):
                                    checked.add(n)
                                else:
                                    checked.discard(n)
                                _sync_confirm_enabled()

                            with ui.row().classes("items-start gap-2 no-wrap w-full"):
                                ui.checkbox(value=True, on_change=_toggle).props(
                                    "dense"
                                )
                                with ui.column().classes("gap-0 min-w-0"):
                                    ui.label(name).classes(
                                        "text-sm font-medium text-gray-900"
                                    )
                                    if reason:
                                        ui.label(reason).classes(
                                            "text-xs text-red-700 break-all"
                                        )
                elif action_tables:
                    with ui.row().classes("items-center gap-1 flex-wrap"):
                        for name in action_tables:
                            ui.badge(name).props("color=blue-grey-6 outline")
                # Disclose the empty targets whose schema will be recreated to deliver
                # the primary key chosen in Schema Conversion. No choice is offered --
                # the table is empty, so there is nothing to lose and nothing to decide,
                # and appending simply cannot apply a new key -- but doing it silently
                # would hide that a hand-edited target DDL is about to be replaced.
                if recreate_now:
                    listed = ", ".join(recreate_now[:6]) + (
                        f" +{len(recreate_now) - 6} more"
                        if len(recreate_now) > 6
                        else ""
                    )
                    _noun = "table" if len(recreate_now) == 1 else "tables"
                    _verb = "uses" if len(recreate_now) == 1 else "use"
                    render_notice(
                        ui,
                        tone="info",
                        header=(
                            f"{len(recreate_now)} empty {_noun} will be recreated to "
                            "apply the chosen primary key"
                        ),
                        body=(
                            f"{listed} {_verb} a primary key that differs from the "
                            "source (for example the composite key chosen to avoid hot "
                            "partitions). A primary key cannot be applied by appending, "
                            "so each of these tables is dropped and recreated from your "
                            "applied Schema Conversion before loading. They hold no rows "
                            "on the target, so no data is lost — but any manual change "
                            "made to the target table outside Schema Conversion is "
                            "replaced."
                        ),
                    )
                # When selected targets already hold data (and CDC is not live),
                # let the user choose the run-wide behavior: append (keep existing
                # rows, load only the missing ones -- the non-destructive default)
                # or drop & reload (DROP+recreate each first, for a clean load).
                if tables_with_data_now and not cdc_live_now:
                    with ui.card().classes(
                        "w-full bg-amber-50 border border-amber-200 gap-2"
                    ):
                        ui.label(
                            f"{len(tables_with_data_now)} selected table(s) already "
                            "contain data on the target. Choose how to load them:"
                        ).classes("text-sm text-gray-800")
                        with ui.row().classes("items-center gap-1 flex-wrap"):
                            for name in tables_with_data_now:
                                ui.badge(name).props("color=amber-8 outline")
                        reload_choice = ui.radio(
                            {
                                "append": "Append — keep existing rows, load only "
                                "the missing ones (idempotent; recommended)",
                                "drop": "Drop & reload — DROP and recreate these "
                                "tables first for a clean load (existing rows are "
                                "permanently lost; DSQL has no TRUNCATE)",
                            },
                            value=migration_state.reload_mode,
                            on_change=lambda e: migration_state.set_reload_mode(
                                str(getattr(e, "value", "append"))
                            ),
                        ).props("dense").classes("text-sm")
                        reload_choice.classes("w-full")
                        # Reassure the user that a Drop & reload recreates the target
                        # from the APPLIED schema conversion -- including any edits
                        # they made in Schema Conversion -- not a stale/original DDL.
                        ui.label(
                            "Drop & reload recreates each table from your applied "
                            "Schema Conversion (including any edits you made there), "
                            "then rebuilds its secondary indexes after loading."
                        ).classes("text-xs text-gray-500")

                def _confirm() -> None:
                    if selectable and not checked:
                        return  # guard: nothing selected (button is also disabled)
                    confirm_dialog.close()
                    # Retry-failed passes the checked subset (in the original order);
                    # every other action calls on_confirm with no arguments.
                    if selectable:
                        chosen = [n for n in action_tables if n in checked]
                        on_confirm(chosen)
                    else:
                        on_confirm()

                with ui.row().classes("justify-end w-full gap-2"):
                    ui.button("Cancel", on_click=confirm_dialog.close).props("flat")
                    if cdc_live_now:
                        confirm_label = "Re-run anyway (CDC is live)"
                    elif tables_with_data_now:
                        # The button label + color follow the current choice so a
                        # destructive drop reads as destructive.
                        drop_chosen = migration_state.reload_mode == "drop"
                        confirm_label = (
                            "Drop, recreate and load" if drop_chosen
                            else "Append and load"
                        )
                    elif recreate_now:
                        # Name the DDL step in the button too, so the action is visible
                        # without relying on the notice above being read. Stays
                        # primary-coloured: these targets are empty, so nothing is lost.
                        confirm_label = "Recreate and load"
                    else:
                        confirm_label = "Confirm and start"
                    confirm_color = (
                        "negative"
                        if (cdc_live_now or (tables_with_data_now
                                             and migration_state.reload_mode == "drop"))
                        else "primary"
                    )
                    start_btn = ui.button(confirm_label, on_click=_confirm).props(
                        f"color={confirm_color}"
                    )

                    def _sync_confirm_enabled() -> None:
                        # Disable confirm when the retry checklist has nothing ticked.
                        if selectable and not checked:
                            start_btn.disable()
                        else:
                            start_btn.enable()

                    _sync_confirm_enabled()
                    # The label/color depend on the radio; re-render them on change
                    # so switching Drop<->Append updates the button immediately.
                    def _sync_btn(e: object) -> None:
                        drop_now = str(getattr(e, "value", "append")) == "drop"
                        start_btn.set_text(
                            "Drop, recreate and load" if drop_now
                            else "Append and load"
                        )
                        start_btn.props(
                            f"color={'negative' if drop_now else 'primary'}"
                        )
                    if tables_with_data_now:
                        reload_choice.on_value_change(_sync_btn)
            # Open on the NEXT tick, not this one. The dialog element was just
            # created; opening it in the same update batches "create element" and
            # "value=True" into one client message, and Quasar's QDialog needs the
            # element registered first and then a SEPARATE false->true transition to
            # animate open -- so the first open is silently dropped and only a second
            # click (element already registered) shows it. A one-shot timer defers
            # the open to a fresh tick so the transition fires on the first click.
            # (Same ui.timer(once=True) deferral pattern already used in this file.)
            ui.timer(0.05, confirm_dialog.open, once=True)

        with client:  # type: ignore[attr-defined]
            _build()

    # Re-entrancy guard: the confirm handler runs a ~1-2s off-loop probe before
    # opening the dialog, so a double-click would open TWO dialogs. This flag drops
    # a second click while the first is still resolving; the clicked button is also
    # disabled + relabeled "Checking…" for a visible cue (restored on dialog open).
    _confirm_busy = {"value": False}

    async def _open_full_load_confirm(
        event: object = None,
        *,
        probe_tables=None,
        on_confirm=None,
        title="Start Full Load?",
        table_reasons=None,
    ) -> None:
        """Probe which target tables hold data, then open the confirm dialog.

        Shared by Start, Re-run-all, Retry-failed and per-table Reload. Runs the
        read-only non-empty probe off the event loop (over ``probe_tables`` -- the
        tables this action will touch, defaulting to the full selection for
        Start/Re-run), records the result (drives the Drop-vs-Append choice + the
        run's replace set), then opens the confirm dialog in the top-level client
        context. ``on_confirm`` is the action to run on confirm (defaults to
        ``start_full_load``); ``title`` is the dialog heading. ``table_reasons``
        (Retry-failed) turns the dialog into a per-table checklist and makes
        ``on_confirm`` receive the checked subset.
        """
        if _confirm_busy["value"]:
            return  # a probe is already in flight -> ignore the extra click
        _confirm_busy["value"] = True
        action_tables = (
            list(probe_tables) if probe_tables is not None else list(selected_names)
        )
        confirm_action = on_confirm if on_confirm is not None else start_full_load
        btn = getattr(event, "sender", None)
        original_text = getattr(btn, "text", None) if btn is not None else None
        # Preserve the button's own icon (play_arrow for Start, restart_alt for the
        # terminal Re-run, replay for Reload) so restore does not swap it wrongly.
        original_icon = None
        if btn is not None:
            original_icon = getattr(btn, "_props", {}).get("icon")
        if btn is not None:
            try:
                btn.disable()
                btn.set_text("Checking target…")
                btn.props("icon=hourglass_top")
                btn.update()
            except Exception:  # noqa: BLE001 - cue is best-effort
                pass
        # Yield to flush the button's busy-state to the client BEFORE the
        # potentially slow target probe. Without this, NiceGUI batches the prop
        # changes and the user sees no feedback until the await below returns.
        import asyncio
        await asyncio.sleep(0)
        try:
            # Probe which action tables already hold rows. Default the run-wide
            # choice to "append" (non-destructive); the confirm dialog lets the
            # user switch to "drop" for a clean reload.
            migration_state.set_tables_with_data(frozenset())
            migration_state.set_target_primary_keys({})
            migration_state.set_reload_mode("append")
            target_config = getattr(session, "target_config", None)
            if target_config is not None and action_tables:
                from nicegui import run

                def _probe() -> tuple[frozenset[str], dict]:
                    connector = DsqlConnector(
                        target_config, aws_profile=session.aws_profile
                    )
                    names = list(action_tables)
                    found = frozenset(
                        tables_with_rows(names, connection_factory=connector.connect)
                    )
                    # Read every target's REAL primary key on the same trip, over a
                    # SINGLE connection (target_primary_keys, not a per-table
                    # target_primary_key_columns loop): each DSQL connect mints an IAM
                    # token + does a cross-region TLS handshake, so the per-table loop
                    # cost N+1 handshakes and made this probe take several seconds
                    # before the confirm dialog could open. The dialog needs the key to
                    # avoid announcing a recreate for a table that already carries the
                    # applied key (the state right after "Apply all to target"), and
                    # doing it here keeps the render path I/O-free.
                    keys = target_primary_keys(
                        names, connection_factory=connector.connect
                    )
                    return found, keys

                try:
                    found, keys = await run.io_bound(_probe)
                    migration_state.set_tables_with_data(found)
                    migration_state.set_target_primary_keys(keys)
                except Exception:  # noqa: BLE001 - on probe failure, warn-less confirm
                    migration_state.set_tables_with_data(frozenset())
                    # Leave the key map EMPTY, not partially filled: an unprobed table
                    # is treated as unknown, so the disclosure stays conservative.
                    migration_state.set_target_primary_keys({})
            _open_confirm_dialog_now(
                action_tables=action_tables,
                on_confirm=confirm_action,
                title=title,
                table_reasons=table_reasons,
            )
        finally:
            _confirm_busy["value"] = False
            if btn is not None and not getattr(btn, "is_deleted", False):
                try:
                    if original_icon:
                        btn.props(f"icon={original_icon}")
                    if original_text is not None:
                        btn.set_text(original_text)
                    btn.enable()
                except Exception:  # noqa: BLE001
                    pass

    async def _open_reload_confirm(table_name: str, event: object = None) -> None:
        """Per-table Reload -> the same probe + Drop-vs-Append confirm dialog.

        Scopes the probe/choice to the single table being reloaded, so the user
        chooses append vs drop for THAT table (and a schema-edited target is
        recreated from the applied conversion on drop).
        """
        await _open_full_load_confirm(
            event,
            probe_tables=[table_name],
            on_confirm=lambda n=table_name: reload_table(n),
            title=f"Reload {table_name}?",
        )

    # The watermark, object browser, prerequisites, and buttons are STATIC: they
    # are rendered once per full render and must NOT be rebuilt by the 0.5s poll,
    # or user-expanded sections (snapshot row counts, prerequisite categories)
    # would keep collapsing. Only this live region -- the per-table progress,
    # caption, completeness verdict, and error-log summary -- is refreshed on each
    # poll tick; the region re-arms its own one-shot timer while the job runs.
    # The per-table progress table is rebuilt on every ~1.5s poll (it lives inside
    # the refreshable _live_detail). A freshly-built ui.table resets to page 1, so
    # without persisting the page a user browsing page 2+ is yanked back on each
    # tick. This holder lives in _render_full_load_step (NOT rebuilt by the poll --
    # only _live_detail is), so the chosen page survives the rebuild; the table
    # seeds its pagination from it and writes back via on_pagination_change.
    # It carries rowsPerPage too: without that, raising "Records per page" was undone
    # by the very next poll tick, so the setting looked broken.
    _progress_page = {"page": 1, "rowsPerPage": 10}

    @ui.refreshable
    def _live_detail() -> None:
        current = _current_job(job_manager, migration_state.job_id)
        if current is None:
            return
        running = current.status in ("PENDING", "RUNNING")
        if running:
            ui.label(full_load_progress_caption(current)).classes(
                "text-sm text-gray-500"
            )
        # Full Load records ONLY: CDC writes under this same job_id (cdc_error_log_key),
        # so an unfiltered read counts dead-lettered rows as Full Load failures.
        summary = full_load_error_summary(migration_state.error_log, current.job_id)
        rows = build_full_load_table_rows(
            current,
            summary,
            full_load_latest_messages(migration_state.error_log, current.job_id),
        )
        _render_full_load_progress(
            ui,
            current,
            rows,
            reload_table=reload_table,
            reload_confirm=_open_reload_confirm,
            quarantine_only=_incomplete_is_quarantine_only(
                current, migration_state.error_log
            ),
            # EVERY dropped row, not one per table: ``latest_messages()`` keeps only the
            # last message per table, so a table that dropped 3 rows listed exactly 1 --
            # and the count above it disagreed with the list below.
            quarantine_records=[
                (str(record.table), str(record.message))
                for record in full_load_error_records(
                    migration_state.error_log, current.job_id
                )
            ],
            ai_error_opener=ai_error_opener,
            page_state=_progress_page,
        )
        # The error-log download belongs with the DETAIL it serializes, not under the
        # accept button: sitting immediately below "Accept quarantined rows & continue" it
        # read as that decision's secondary option, when it is just a way to take the same
        # per-row information away with you.
        _render_error_log(ui, migration_state, current)
        # The completeness baseline (expected_rows) comes from the watermark's
        # per-table counts, which are scan-free information_schema ESTIMATES
        # (row_counts_approximate) -- not exact. So a "row-count mismatch" against
        # them is expected noise, not a failure, and must not read as a red alert.
        approximate = (
            getattr(current.watermark, "row_counts_approximate", False)
            if current.watermark is not None
            else False
        )
        _render_completeness_banner(
            ui,
            full_load_completeness(rows),
            approximate=approximate,
            quarantine_accepted=migration_state.accept_quarantined_rows,
        )
        # The action the banner just described, directly beneath its verdict.
        _render_accept_quarantine_action(
            ui,
            quarantine_only=_incomplete_is_quarantine_only(
                current, migration_state.error_log
            ),
            terminal=current.status in ("DONE", "FAILED", "CANCELLED"),
            quarantine_accepted=migration_state.accept_quarantined_rows,
            accept_quarantine_and_continue=accept_quarantine_and_continue,
        )
        if running:
            # Re-arm a single-shot poll: it fires once, refreshes only this live
            # region (not the whole page), and the refresh renders a fresh timer.
            # once=True avoids the "parent slot deleted" crash a repeating timer
            # hits when its slot is rebuilt.
            ui.timer(_POLL_INTERVAL_SECONDS, _poll_live, once=True)

    def _poll_live() -> None:
        """Poll the job: refresh only the live region while running; on a terminal
        state, finalize the step status and do one full refresh (to flip buttons
        and reveal retry)."""
        try:
            current = job_manager.get_status(migration_state.job_id)
        except JobNotFoundError:
            return
        mapped = job_status_to_step_status(current.status)
        if mapped is None:
            _live_detail.refresh()
            return
        if mapped is StepStatus.FAILED:
            migration_state.set_error(
                job_manager.get_error(migration_state.job_id)
                or "Data migration failed."
            )
        session.set_workflow(  # type: ignore[attr-defined]
            with_status(
                session.workflow,  # type: ignore[attr-defined]
                WorkflowStep.FULL_LOAD,
                mapped,
            )
        )
        # Combined mode: do NOT auto-advance to CDC when the Full Load completes.
        # The operator should be able to review the finished snapshot's stats
        # (per-table rows, completeness, watermark) before moving on, so the view
        # stays on the Full Load step; the gapless hand-off to CDC happens when
        # they click "Continue to CDC". refresh() flips the buttons (Re-run /
        # enable Continue) without changing the active sub-step.
        refresh()

    if status is StepStatus.IN_PROGRESS:
        stopping = False
        try:
            stopping = job_manager.is_cancel_requested(migration_state.job_id)
        except Exception:  # noqa: BLE001 - best-effort UI hint
            stopping = False
        with ui.row().classes("items-center gap-2"):
            ui.spinner(size="sm")
            # Say what is actually guaranteed. "finishing the current batch" read as a
            # promise the tool could not keep: a wedged worker once left this label up
            # indefinitely with nothing progressing, so it looked like normal shutdown
            # when the job was in fact stuck. The stop is COOPERATIVE (each worker
            # finishes its batch, then returns) and now has a bounded grace period, so
            # the honest wording is "waiting for the workers", not "almost done".
            ui.label(
                "Stopping… waiting for the in-flight batches to finish."
                if stopping
                else "Full Load in progress…"
            ).classes("text-sm text-gray-500")
            stop_btn = ui.button(
                "Stop Full Load", on_click=stop_full_load, icon="stop"
            ).props("color=negative outline")
            if stopping:
                stop_btn.disable()
                stop_btn.tooltip(
                    "Stop already requested — waiting for the in-flight batches. If a "
                    "worker does not respond, the run is torn down after a grace "
                    "period and the unfinished tables become retryable."
                )
            else:
                stop_btn.tooltip(
                    "Stop after the current batch. Loaded tables are kept; the "
                    "rest become retryable."
                )
    else:
        # After a finished run that left failed tables, the recovery actions
        # (Retry failed tables = recommended, Re-run all tables = secondary) are
        # grouped together in the terminal section below, next to the failure
        # reason -- so we do NOT also draw a standalone primary "Re-run" button
        # here, which would compete with the recommended Retry and read as the
        # main action. The standalone button is only for the pre-run Start and
        # for a clean finished run (DONE, no failures) where re-running all is the
        # natural single action.
        # A terminated run with ANY unfinished table (FAILED or still PENDING)
        # shows the recovery row (Retry unfinished / Re-run all) instead of a lone
        # "Re-run Full Load" -- so a crash that left tables PENDING still offers a
        # scoped retry, not just a full re-run.
        terminal_with_failures = (
            job is not None
            and status is not StepStatus.IN_PROGRESS
            and bool(unsettled_table_names(job))
        )
        if not terminal_with_failures:
            start_label = "Re-run Full Load" if job is not None else "Start Full Load"
            start_btn = ui.button(
                start_label, on_click=_open_full_load_confirm, icon="play_arrow"
            ).props("color=primary")
            if not selected_names:
                start_btn.disable()
                start_btn.tooltip(
                    "Select at least one table in the object browser above."
                )
            elif guard_reason:
                start_btn.disable()
                start_btn.tooltip(guard_reason)
            elif cdc_streaming_started(migration_state, job_manager):
                # CDC is live: starting/re-running Full Load would collide with the
                # running stream, so DISABLE (grey out) the button rather than let it
                # look clickable. The tooltip + hint say how to re-enable it (Stop CDC),
                # so it is a clear "not now", not a dead end.
                start_btn.disable()
                start_btn.tooltip(
                    "CDC is streaming -- starting Full Load would collide with the "
                    "live pipeline. Stop CDC first (CDC step → Stop CDC) to run it."
                )
                inline_hint(
                    ui,
                    "CDC is live, so Full Load is disabled. Stop CDC first "
                    "(CDC step → Stop CDC) to run it.",
                    tone="warning",
                )

    if job is not None:
        ui.separator()
        # Live FIRST: per-table progress, caption, completeness, error log. This is what
        # the operator is watching, so it leads -- the watermark used to sit above it and
        # pushed the progress table (and, on a finished run, the completeness verdict and
        # any quarantine detail) below a block of static reference data.
        _live_detail()
        # Static reference AFTER: the watermark is captured once at run start and never
        # changes, so it reads as provenance/audit detail rather than something to watch.
        # It stays OUTSIDE the refreshable region (only _live_detail re-renders on each
        # ~1.5s poll tick), so its snapshot-row-counts expansion is still never collapsed
        # by the poll -- the reason it was hoisted out in the first place holds either
        # way, because order within the parent does not affect what the poll rebuilds.
        _render_watermark(ui, job)

        # Terminal-only affordances (shown after the job finishes, on the full
        # refresh the poll triggers): the job-level failure reason and retry.
        if status is not StepStatus.IN_PROGRESS:
            job_error = None
            try:
                job_error = job_manager.get_error(job.job_id)
            except JobNotFoundError:
                job_error = None
            # Suppress the raw job error when the ONLY problem is quarantine: the
            # completeness banner above already gives the same facts and remedy in plain
            # language, so this box added a third telling of "3 rows were dropped" -- in
            # red, with an exception class name, contradicting the amber "the rest
            # loaded" framing. A red "Load failed" also overstates a run whose only
            # incompleteness is rows that can never load. Any REAL failure still shows
            # it: that is a retryable error whose exact text matters.
            _quarantine_only_error = _incomplete_is_quarantine_only(
                job, migration_state.error_log
            )
            if job_error and not _quarantine_only_error:
                render_notice(
                    ui,
                    tone="error",
                    header="Load failed",
                    body=job_error,
                )

            # Recovery set = every table that did NOT finish (FAILED *or* still
            # PENDING). A fatal/aborted run (e.g. a run-level crash before the big
            # tables were attempted) leaves them PENDING, not FAILED -- so keying
            # recovery off FAILED alone would strand them with only a full "Re-run"
            # as escape. ``unsettled`` resumes exactly the unfinished tables.
            failed = unsettled_table_names(job)
            if failed:
                # Recovery needs a live source AND target. After an app restart the
                # connections (and the in-memory source password) are NOT restored
                # (Property 7), so both recovery actions would silently no-op. Tell
                # the user exactly why and disable the buttons instead of letting a
                # click do nothing.
                missing: list[str] = []
                if not session.has_source():
                    missing.append("source")
                if not session.has_target():
                    missing.append("target")
                connections_missing = bool(missing)
                if connections_missing:
                    render_notice(
                        ui,
                        tone="warning",
                        header="Reconnect to retry",
                        body=(
                            f"The {' and '.join(missing)} connection is not active "
                            "(connections and credentials are not restored after a "
                            "restart for security). Re-open the Connect step, test "
                            f"the {' and '.join(missing)} connection, then return "
                            "here to retry the failed tables — the succeeded tables "
                            "are kept."
                        ),
                    )
                # Two recovery actions, grouped and ranked so the recommended one
                # is unambiguous: "Retry failed tables" (resume only the unfinished
                # work, keep succeeded tables) is the primary action on the right;
                # "Re-run all tables" (fresh snapshot over everything) is the
                # secondary/destructive-leaning option on the left. Putting them in
                # one row -- instead of a lone primary "Re-run" up top competing
                # with the retry down here -- makes the choice clear at a glance.
                done_count = sum(1 for c in job.chunks if c.status == "DONE")
                reconnect_tip = (
                    f"Reconnect the {' and '.join(missing)} connection first "
                    "(Connect step), then retry."
                )
                with ui.row().classes("items-center gap-2 w-full"):
                    rerun_btn = ui.button(
                        "Re-run all tables",
                        on_click=_open_full_load_confirm,
                        icon="restart_alt",
                    ).props("color=primary outline")
                    if connections_missing:
                        rerun_btn.disable()
                        rerun_btn.tooltip(reconnect_tip)
                    elif not selected_names:
                        rerun_btn.disable()
                        rerun_btn.tooltip(
                            "Select at least one table in the object browser above."
                        )
                    elif guard_reason:
                        rerun_btn.disable()
                        rerun_btn.tooltip(guard_reason)
                    else:
                        rerun_btn.tooltip(
                            "Re-runs ALL selected tables from a fresh snapshot (new "
                            "watermark). Use 'Retry failed tables' to resume only "
                            "the unfinished ones instead."
                        )
                    # Spacer pushes the recommended action to the right edge.
                    ui.space()

                    # Per-table reason (cause) for the retry checklist, so the user
                    # can see WHY each table is unfinished before deciding whether to
                    # retry it now. FAILED tables carry their error-log message; a
                    # still-PENDING table was never attempted (the run ended first),
                    # so it gets a plain "not yet loaded" note.
                    _failure_reasons = full_load_latest_messages(
                        migration_state.error_log, job.job_id
                    )
                    _pending_names = {
                        c.chunk_id for c in job.chunks if c.status == "PENDING"
                    }

                    def _reason_for(name: str) -> str:
                        msg = _failure_reasons.get(name, "")
                        if msg:
                            return msg
                        if name in _pending_names:
                            return "Not loaded yet — the previous run ended first."
                        return ""

                    async def _confirm_retry_failed(event: object = None) -> None:
                        # Route the retry through the same confirm dialog as Start,
                        # as a CHECKLIST (all pre-checked) of the UNFINISHED tables +
                        # their reasons. Probing/Drop-vs-Append and the retry are
                        # scoped to the CHECKED subset, so the user can retry only the
                        # tables they're ready for.
                        retry_fn = retry_tables or (lambda _names: retry_failed_load())
                        await _open_full_load_confirm(
                            event,
                            probe_tables=list(failed),
                            on_confirm=retry_fn,
                            title=f"Retry {len(failed)} unfinished table(s)?",
                            table_reasons={n: _reason_for(n) for n in failed},
                        )

                    retry_btn = ui.button(
                        f"Retry unfinished tables ({len(failed)})",
                        on_click=_confirm_retry_failed,
                        icon="replay",
                    ).props("color=primary")
                    if connections_missing:
                        retry_btn.disable()
                        retry_btn.tooltip(reconnect_tip)
                    else:
                        retry_btn.tooltip(
                            f"Recommended: re-run Full Load for only the "
                            f"{len(failed)} failed table(s); the {done_count} "
                            "succeeded table(s) are kept as-is (no re-load, no new "
                            "snapshot)."
                        )


# Quasar color names for each per-table Full Load state badge.
_LOAD_STATE_COLORS: dict[str, str] = {
    "PENDING": "grey",
    "IN_PROGRESS": "primary",
    "DONE": "positive",
    "FAILED": "negative",
}


def _quarantined_row_count(job: "MigrationJob", error_log: "ErrorLogStore") -> int:
    """Count permanently-quarantined rows for ``job``.

    Prefers the count recorded on the JOB's chunks, because that is what survives an app
    restart: the job store is durable while :class:`ErrorLogStore` is in-memory only.
    Reading the log alone made this return 0 for a restored session, which flipped
    :func:`_incomplete_is_quarantine_only` to ``False`` and hid the
    "Accept quarantined rows & continue" button -- the exact action the failure message
    tells the operator to use. That was a complete dead end: the run cannot be retried
    into success (a permanently-rejected value never loads) and the only escape left was
    Start over.

    Falls back to counting the log's ``quarantined row pk[...]`` entries so a job written
    by an older version (no per-chunk count) still works.
    """
    if job is None:
        return 0
    from_chunks = sum(
        getattr(chunk, "rows_quarantined", 0) or 0 for chunk in job.chunks
    )
    if from_chunks:
        return from_chunks
    return sum(
        1
        for record in error_log.records(job.job_id)
        if str(getattr(record, "message", "")).startswith("quarantined row pk[")
    )


def _incomplete_is_quarantine_only(
    job: "MigrationJob", error_log: "ErrorLogStore"
) -> bool:
    """True when a run's only incompleteness is quarantined rows (overridable).

    A quarantine table's chunk completes DONE (its loadable rows are committed),
    so there are NO unfinished chunks; the run is incomplete only because rows were
    permanently dropped. That is the one case the accept-and-continue override may
    unblock -- any UNFINISHED table (a FAILED chunk, or one still PENDING because
    the run ended before it loaded) is retryable work and must still block.
    """
    if job is None:
        return False
    if unsettled_table_names(job):  # retryable/unfinished work is present
        return False
    return _quarantined_row_count(job, error_log) > 0


def _format_progress_cell(row: "FullLoadTableRow") -> str:
    """Render a per-table progress cell (percent, ``loading...``, or a dash)."""
    pct = row.progress_pct
    if pct is not None:
        return f"{pct:.0f}%"
    if row.state == "IN_PROGRESS":
        return "loading..."
    return "—"


def _format_rows_on_target_cell(row: "FullLoadTableRow") -> str:
    """Render the "Rows on target" cell: total present, with a new/existing split.

    Shows the total rows now on the target for this table (``rows_present`` =
    newly inserted + already present). When some rows already existed and were
    skipped by the idempotent re-load, the total is followed by a plain-language
    breakdown -- ``"1,067,310  ·  455,319 new + 611,991 already there"`` -- so the
    operator understands a re-run that mostly skips is not "stuck at zero" but is
    re-using rows a prior run loaded. With no skips it is just the loaded count.
    Thousands separators make large tables readable.
    """
    if row.rows_skipped:
        return (
            f"{row.rows_present:,}  ·  {row.rows_loaded:,} new + "
            f"{row.rows_skipped:,} already there"
        )
    return f"{row.rows_loaded:,}"


def _abbrev_count(n: "Optional[int]") -> str:
    """Compact count for a dense table: 1,180,000 -> "1.18M", 33585832 -> "33.6M".

    Keeps small numbers exact (with thousands separators) and abbreviates large
    ones to 3 significant figures so the "Rows (target / source)" column stays a
    single narrow line regardless of scale. ``None`` -> an em dash.
    """
    if n is None:
        return "—"
    n = int(n)
    if n < 100_000:
        return f"{n:,}"
    for div, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if n >= div:
            val = n / div
            # 3 sig figs: 1.18M, 33.6M, 747K
            return f"{val:.2f}{suffix}" if val < 10 else f"{val:.1f}{suffix}"
    return f"{n:,}"


def _rows_target_source_cell(row: "FullLoadTableRow") -> str:
    """Compact "<on target> / <source>" for the merged Rows column.

    Both counts abbreviated (:func:`_abbrev_count`) so the cell is one short line;
    the exact figures + the new/already-there breakdown live in the cell tooltip
    (:func:`_rows_breakdown_tooltip`).
    """
    return f"{_abbrev_count(row.rows_present)} / {_abbrev_count(row.expected_rows)}"


def _quarantined_cell_tooltip(row: "FullLoadTableRow") -> str:
    """Hover text for a table's "N dropped" badge (empty when nothing was dropped).

    Says what happened, that the rest of the table DID load (a quarantining table is
    ``DONE``, not failed), and where to act -- so the badge is self-explanatory instead
    of sending the reader hunting for the panel below the table.
    """
    dropped = row.rows_quarantined
    if dropped <= 0:
        return ""
    noun = "row was" if dropped == 1 else "rows were"
    return (
        f"{dropped:,} {noun} permanently dropped — a value Aurora DSQL could not "
        "store (e.g. over its ~1 MiB per-value limit). The rest of this table loaded "
        "normally. See the quarantine panel below for each row's primary key and "
        "reason; fix the source value and Reload this table to close the gap."
    )


def _rows_breakdown_tooltip(row: "FullLoadTableRow") -> str:
    """Exact figures + new/already-there split for the Rows cell's hover tooltip.

    Also explains the (normal) case of holding MORE rows than the source estimate:
    the watermark count is scan-free ``information_schema`` sampling and often
    undercounts, so "target > source" here is an estimate artifact, not duplicated
    data. Without this the arithmetic looks like a bug.
    """
    parts = [f"{row.rows_present:,} on target"]
    if row.rows_skipped:
        parts.append(
            f"{row.rows_loaded:,} new + {row.rows_skipped:,} already there"
        )
    if row.expected_rows is not None:
        parts.append(f"{row.expected_rows:,} source rows (estimate)")
        exceeded = row.expected_exceeded_pct
        if exceeded is not None:
            parts.append(
                f"{exceeded:.1f}% above the estimate — normal (the scan-free "
                "estimate undercounts); Validation (step 4) counts exactly"
            )
    return " · ".join(parts)


def _format_attempts_cell(row: "FullLoadTableRow") -> str:
    """Attempts, with a plain-language error marker so no Errors column is needed.

    ``"1"`` clean, ``"1 · 3 errors"`` when the table logged errors ("3 err" was cryptic --
    it read like part of the retry count).

    Quarantined rows are deliberately NOT repeated here: the same row's Status cell
    already carries a "3 dropped" badge with the full explanation on hover, so saying it
    again one column over was the same fact twice in one table row. A quarantine also
    logs an error per dropped row, so those rows still surface here as an error count.
    """
    if row.errors:
        noun = "error" if row.errors == 1 else "errors"
        return f"{row.attempts} · {row.errors} {noun}"
    return str(row.attempts)


def _parse_quarantined_pk(message: str) -> "Optional[str]":
    """Extract the primary key from a ``quarantined row pk[...]: reason`` message.

    Returns ``None`` when the message is not in that shape, so a caller falls back to
    showing it verbatim rather than mangling an unexpected format.
    """
    prefix = "quarantined row pk["
    text = str(message or "")
    if not text.startswith(prefix):
        return None
    end = text.find("]", len(prefix))
    if end == -1:
        return None
    return text[len(prefix) : end] or None


def _quarantined_reason(message: str) -> str:
    """Return just the reason from a ``quarantined row pk[...]: reason`` message."""
    text = str(message or "")
    marker = "]: "
    index = text.find(marker)
    if text.startswith("quarantined row pk[") and index != -1:
        return text[index + len(marker) :].strip()
    return text.strip()


_QUARANTINE_PK_CHIP_LIMIT = 12


def _group_quarantine_entries(
    entries: "Sequence[tuple[str, str]]",
) -> "list[tuple[str, list[str], list[str]]]":
    """Group ``(table, message)`` entries into ``(table, primary_keys, reasons)``.

    One group per TABLE, in first-seen order, because the shared facts (the table and --
    almost always -- the reason) belong stated once, and because Reload acts on the whole
    table: a per-row card offered one identical Reload button per row.

    ``reasons`` is deduplicated while preserving order: a table usually drops rows for the
    same reason (one oversized column), so this collapses to a single line; when the
    reasons genuinely differ they are all kept rather than picking one. A row whose message
    carries no parseable primary key contributes only its reason, so an unexpected format
    still surfaces instead of vanishing.
    """
    grouped: "dict[str, tuple[list[str], list[str]]]" = {}
    for table, message in entries:
        pks, reasons = grouped.setdefault(table, ([], []))
        pk = _parse_quarantined_pk(message)
        if pk:
            pks.append(pk)
        reason = _quarantined_reason(message)
        if reason and reason not in reasons:
            reasons.append(reason)
    return [(table, pks, reasons) for table, (pks, reasons) in grouped.items()]


def _quarantine_detail_row(
    ui,
    *,
    table: str,
    primary_keys: "Sequence[str]" = (),
    reasons: "Sequence[str]" = (),
    action=None,
) -> None:
    """Render one table's dropped rows: the table, every primary key, and the reason(s).

    Replaces a run-on log line ("quarantined row pk[id=3]: datatype limit greater than
    1048576 bytes not supported for bytea") in which the table name sat in a badge above
    and the PK was buried mid-sentence. The facts a reader acts on -- WHICH table, WHICH
    rows, WHY -- are separately labelled, with the primary keys as monospace chips because
    they are the handles you search the source with.

    One card per table keeps this compact as the count grows: 12 chips instead of 12 cards
    each repeating the same table name, reason and Reload button. Beyond that the chips are
    truncated with a "+N more" marker -- the full list is always in the downloadable error
    log, so the screen does not need to be exhaustive.
    """
    shown = list(primary_keys[:_QUARANTINE_PK_CHIP_LIMIT])
    hidden = max(0, len(primary_keys) - len(shown))
    with ui.row().classes(
        "items-start gap-3 w-full no-wrap rounded-md border border-amber-200 "
        "bg-amber-50 p-3"
    ):
        ui.icon("report_problem").classes("text-amber-600 text-lg")
        with ui.column().classes("gap-1 flex-1 min-w-0"):
            with ui.row().classes("items-center gap-2 flex-wrap"):
                ui.label(table).classes("text-sm font-semibold text-gray-900")
                # Count first: with many chips the number is what you read, and it also
                # covers the truncated case where the chips alone under-report.
                if primary_keys:
                    noun = "row" if len(primary_keys) == 1 else "rows"
                    ui.badge(f"{len(primary_keys)} {noun} dropped").props("color=amber-8")
                else:
                    ui.badge("dropped").props("color=amber-8")
            if shown:
                with ui.row().classes("items-center gap-1 flex-wrap"):
                    for pk in shown:
                        ui.badge(pk).props("color=amber-8 outline").classes("font-mono")
                    if hidden:
                        ui.label(f"+{hidden} more").classes(
                            "text-xs text-gray-500"
                        ).tooltip(
                            "The full list of dropped primary keys is in the "
                            "downloadable Full Load error log."
                        )
            for reason in reasons:
                ui.label(reason).classes("text-xs text-gray-700 break-words")
        if action is not None:
            with ui.row().classes("items-center gap-1 no-wrap shrink-0"):
                action()


# Friendly labels for each load state, used by the status-distribution chips.
_LOAD_STATE_LABELS: dict[str, str] = {
    "DONE": "Done",
    "IN_PROGRESS": "In progress",
    "FAILED": "Failed",
    "PENDING": "Pending",
}


def _render_table_state_summary(ui, rows: "Sequence[FullLoadTableRow]") -> None:
    """Render colored chips with the table count in each load state (O(tables)).

    A quarantining table settles as ``DONE``, so the state chips alone read "Done: 8" on
    a run that permanently dropped rows -- the at-a-glance summary looked clean. An amber
    chip is appended whenever anything was dropped so the loss is visible in the same
    glance, before the reader scrolls to the table.
    """
    counts = summarize_table_states(rows)
    dropped_rows = sum(row.rows_quarantined for row in rows)
    dropped_tables = sum(1 for row in rows if row.rows_quarantined > 0)
    with ui.row().classes("items-center gap-2 flex-wrap"):
        for state in _LOAD_STATE_ORDER:
            count = counts.get(state, 0)
            if count == 0:
                continue
            color = _LOAD_STATE_COLORS.get(state, "grey")
            ui.badge(f"{_LOAD_STATE_LABELS[state]}: {count}").props(
                f"color={color}"
            ).classes("text-sm q-px-sm q-py-xs")
        if dropped_rows:
            row_noun = "row" if dropped_rows == 1 else "rows"
            table_noun = "table" if dropped_tables == 1 else "tables"
            verb = "could not be stored and was" if dropped_rows == 1 else (
                "could not be stored and were"
            )
            ui.badge(f"Dropped: {dropped_rows} {row_noun}").props(
                "color=amber-8 outline"
            ).classes("text-sm q-px-sm q-py-xs").tooltip(
                f"{dropped_rows} {row_noun} across {dropped_tables} {table_noun} "
                f"{verb} permanently dropped. Those tables are marked in the Status "
                "column; the rest of their rows loaded normally."
            )


def _render_full_load_progress(
    ui, job: MigrationJob, rows: "Sequence[FullLoadTableRow]",
    *,
    reload_table=None,
    reload_confirm=None,
    # The accept-the-gap action moved OUT of this function: it now renders after the
    # completeness banner (see _render_accept_quarantine_action), so the button that
    # carries out the banner's remedy sits directly beneath the verdict rather than
    # above it. ``quarantine_only`` is kept because the panel still gates the per-row
    # Reload on a settled, quarantine-only run.
    quarantine_only: bool = False,
    # EVERY quarantined row, as ``(table, message)`` in log order. The panel used to be
    # built from ``latest_messages()``, which keeps one message per TABLE (last write
    # wins) -- so a table that dropped 3 rows listed exactly 1, and the count above it
    # disagreed with the list below. The primary key is the actionable part of each
    # entry, so every row has to appear.
    quarantine_records: "Sequence[tuple[str, str]]" = (),
    ai_error_opener=None,
    page_state=None,
) -> None:
    """Render the overall progress, a status distribution, and a live per-table
    table with colored status badges and per-row progress bars."""
    total = len(rows)
    settled = sum(1 for row in rows if row.state in ("DONE", "FAILED"))
    pct = job.progress_pct or 0.0
    if total:
        ui.linear_progress(value=pct / 100.0, show_value=False).props(
            "instant-feedback"
        ).classes("w-full")
    ui.label(
        f"Overall — {pct:.1f}% complete, {settled}/{total} tables settled"
    ).classes("text-sm text-gray-600")

    # At-a-glance status distribution (scales to any table count).
    _render_table_state_summary(ui, rows)

    # Simplified 6-column layout (was 9): the exact row breakdown and source-row
    # figure moved into the "Rows" cell tooltip; Errors merged into Attempts; the
    # redundant "Complete" column dropped (Status + Progress already convey it).
    columns = [
        {"name": "table", "label": "Table", "field": "table", "align": "left"},
        {"name": "state", "label": "Status", "field": "state", "align": "left"},
        {
            # "(est.)" belongs in the header: the source side of this cell is the
            # watermark's scan-free information_schema estimate, so a reader scanning
            # the column must not read "3.01M / 2.77M" as the target holding 240k rows
            # MORE than the source (the estimate simply undercounted).
            "name": "rows",
            "label": "Rows (target / source est.)",
            "field": "rows",
            "align": "left",
        },
        {"name": "progress", "label": "Progress", "field": "progress", "align": "left"},
        {"name": "time", "label": "Time", "field": "time", "align": "left"},
        {"name": "attempts", "label": "Attempts", "field": "attempts", "align": "left"},
    ]
    now = datetime.now(timezone.utc)
    table_rows = [
        {
            "table": row.table,
            "state": row.state,
            "state_label": _LOAD_STATE_LABELS.get(row.state, row.state),
            "state_color": _LOAD_STATE_COLORS.get(row.state, "grey"),
            # Rows permanently dropped for this table. A quarantining table finishes
            # DONE, so its status badge was identical to a clean table's -- the only
            # hint was one amber panel below the whole table, which does not say WHICH
            # row it belongs to once the table is paginated. 0 renders no badge.
            "quarantined": row.rows_quarantined,
            "quarantined_tooltip": _quarantined_cell_tooltip(row),
            "rows": _rows_target_source_cell(row),
            "rows_tooltip": _rows_breakdown_tooltip(row),
            "progress": _format_progress_cell(row),
            "progress_value": (
                None if row.progress_pct is None else round(row.progress_pct / 100.0, 4)
            ),
            "time": format_table_timing(row, now),
            "attempts": _format_attempts_cell(row),
        }
        for row in rows
    ]
    # `wrap-cells` lets long cells (headers like "Time (ETA / total)") wrap to a
    # second line instead of forcing the row wider than the card, and `dense`
    # tightens padding -- together the 9 columns fit the card width so the table
    # never shows a bottom horizontal scrollbar.
    # Seed pagination from the persisted page (survives the ~1.5s poll rebuild) so
    # a user browsing page 2+ is not yanked back to page 1 on every tick. The page
    # is clamped to the current row count (a shrinking table can't leave you on a
    # now-empty page). ``on_pagination_change`` writes the user's page back.
    # BOTH the page and the rows-per-page must be persisted. The table is rebuilt on
    # every ~1.5s poll tick, so any pagination value that is hardcoded here is silently
    # restored on the next tick: picking a larger "Records per page" appeared to do
    # nothing (and the reverting select looked like the table was refreshing itself).
    _rows_per_page = 10
    _saved_page = 1
    if isinstance(page_state, dict):
        _rows_per_page = int(page_state.get("rowsPerPage", _rows_per_page))
        # rowsPerPage == 0 is Quasar's "All" option: everything on one page.
        _max_page = (
            max(1, -(-len(table_rows) // _rows_per_page))  # ceil-div
            if _rows_per_page > 0
            else 1
        )
        _saved_page = min(max(1, int(page_state.get("page", 1))), _max_page)

    def _on_pagination_change(event: object) -> None:
        if isinstance(page_state, dict):
            value = getattr(event, "value", None) or {}
            page_state["page"] = int(value.get("page", page_state.get("page", 1)))
            page_state["rowsPerPage"] = int(
                value.get("rowsPerPage", page_state.get("rowsPerPage", 10))
            )

    table = ui.table(
        columns=columns,
        rows=table_rows,
        row_key="table",
        pagination={"rowsPerPage": _rows_per_page, "page": _saved_page},
        on_pagination_change=_on_pagination_change,
    ).props("wrap-cells dense").classes("w-full")
    # Colored status badge per row (visualizes each table's load state), plus an amber
    # "N dropped" badge when rows were permanently quarantined. A quarantining table
    # finishes DONE, so without this it is indistinguishable from a clean one in the
    # Status column -- the amber panel below the table says a drop happened but not on
    # which row, and it scrolls out of view / the row can be on another page.
    table.add_slot(
        "body-cell-state",
        r"""
        <q-td :props="props">
          <div class="row items-center no-wrap" style="gap:4px">
            <q-badge :color="props.row.state_color" :label="props.row.state_label" />
            <q-badge v-if="props.row.quarantined > 0" color="amber-8" outline
                     class="items-center">
              <q-icon name="report_problem" size="12px" class="q-mr-xs" />
              {{ props.row.quarantined }} dropped
              <q-tooltip>{{ props.row.quarantined_tooltip }}</q-tooltip>
            </q-badge>
          </div>
        </q-td>
        """,
    )
    # Per-row progress bar (rows loaded vs source snapshot count) with a label;
    # falls back to the text cell ("loading..."/"—") when the percent is unknown.
    table.add_slot(
        "body-cell-progress",
        r"""
        <q-td :props="props">
          <div v-if="props.row.progress_value !== null"
               class="row items-center no-wrap" style="gap:8px; min-width:140px">
            <q-linear-progress :value="props.row.progress_value" size="10px"
                 color="primary" track-color="grey-3" rounded style="flex:1" />
            <span class="text-caption">{{ props.value }}</span>
          </div>
          <span v-else>{{ props.value }}</span>
        </q-td>
        """,
    )
    # Rows cell: the compact "target / source" value with the exact figures +
    # new/already-there breakdown on hover, so the detail is available without
    # widening the column or wrapping to a second line.
    table.add_slot(
        "body-cell-rows",
        r"""
        <q-td :props="props">
          <span>{{ props.value }}</span>
          <q-tooltip>{{ props.row.rows_tooltip }}</q-tooltip>
        </q-td>
        """,
    )
    # Header ⓘ on the Rows column (same idiom as the CDC status table): explain that
    # the source side is an estimate right where the eye is, so "target / source"
    # arithmetic that looks off is understood without hovering a cell.
    table.add_slot(
        "header-cell-rows",
        r"""
        <q-th :props="props">
          {{ props.col.label }}
          <q-icon name="info" size="14px" class="q-ml-xs text-grey-5"
            style="cursor:help">
            <q-tooltip class="text-body2" style="max-width:340px">
              Rows now on the target vs. the source row count recorded on the export
              watermark. The source figure is a scan-free information_schema
              ESTIMATE (InnoDB index sampling), so it is commonly off by a few
              percent and often UNDERCOUNTS — the target legitimately exceeding it is
              normal, not duplicated data. A finished table is 100% because the
              loader streamed it to exhaustion, not because the two numbers match.
              Validation (step 4) does the exact COUNT(*) comparison.
            </q-tooltip>
          </q-icon>
        </q-th>
        """,
    )

    # Surface the failure cause inline: list each failed table with its latest
    # error message so the user can diagnose without downloading the log. Always
    # shown (not collapsible) so the live poll re-render never hides it.
    failures = [row for row in rows if row.error_message]
    quar_prefix = "quarantined row pk["
    # An index that could not be created is recorded against the table like any other
    # error-log entry, but it is NOT a load failure: every row is present and only an
    # access path is missing. Split it out so it is reported as its own warning instead
    # of appearing among the failed tables (which would contradict the table's DONE
    # state and imply data was lost).
    index_prefix = "index not created: "
    index_only = [
        r for r in failures if str(r.error_message).startswith(index_prefix)
    ]
    real_failures = [
        r
        for r in failures
        if not str(r.error_message).startswith(quar_prefix)
        and not str(r.error_message).startswith(index_prefix)
    ]
    quarantined = [
        r for r in failures if str(r.error_message).startswith(quar_prefix)
    ]
    terminal = job.status in ("DONE", "FAILED", "CANCELLED")

    def _ai_btn(table_name: str, error_message: str) -> None:
        # Per-table AI Assist: opens the chat drawer to explain THIS failure's
        # cause + fix. Shown enabled only when AI Assist is on (an opener was
        # threaded in); otherwise a disabled, discoverable affordance points at
        # the Connect screen -- mirroring Schema Conversion / Validation.
        if ai_error_opener is not None:
            ui.button(
                "AI Assist",
                on_click=lambda n=table_name, e=error_message: ai_error_opener(n, e),
            ).props(
                "flat dense no-caps size=sm color=indigo-6 icon=auto_awesome"
            ).tooltip("Ask AI why this table failed and how to fix it.")
        else:
            ui.button("AI Assist").props(
                "flat dense no-caps size=sm color=grey icon=auto_awesome"
            ).props("disable").tooltip(
                "Enable AI Assist on the Connect screen to diagnose this "
                "failure with AI."
            )

    def _failure_row(*, table_name, message, tone, action=None) -> None:
        # One failure/quarantine entry with a STABLE layout that doesn't shift when
        # the error text is long: the table badge + wrapping message sit in a
        # flex-1 column on the left, and the fixed-width action (AI Assist, and for
        # quarantine a Reload) is pinned top-right -- so buttons never get pushed to
        # a second line by a long message (the old row-nowrap did).
        with ui.row().classes("items-start gap-2 w-full no-wrap"):
            with ui.column().classes("gap-0 flex-1 min-w-0"):
                # negative = a real failure, warning = quarantine (rows dropped),
                # info = data complete but an index is missing.
                _badge_color = {
                    "error": "negative",
                    "warning": "warning",
                    "info": "info",
                }.get(tone, "warning")
                ui.badge(table_name).props(f"color={_badge_color} outline")
                inline_hint(ui, message, tone=tone, classes="text-xs break-words")
            if action is not None:
                with ui.row().classes("items-center gap-1 no-wrap shrink-0"):
                    action()

    # Real, retryable table failures (red). Retry is driven by the single
    # "Retry unfinished tables" control below (a checklist), so no per-row Reload
    # here -- only per-table AI diagnosis, which is inherently per-table.
    if real_failures:
        with ui.column().classes("w-full gap-2"):
            render_notice(
                ui,
                tone="error",
                header=f"Failure details ({len(real_failures)})",
            )
            for row in real_failures:
                _failure_row(
                    table_name=row.table,
                    message=row.error_message,
                    tone="error",
                    action=(lambda r=row: _ai_btn(r.table, r.error_message or "")),
                )

    # Quarantined rows (amber): the table loaded -- these rows were permanently
    # dropped (e.g. a value over DSQL's ~1 MiB per-value limit), NOT a failure.
    # A quarantine table is DONE (not "unfinished"), so the retry checklist does
    # not cover it; a per-row Reload stays here for "I fixed the source value,
    # reload just this table".
    def _quar_reload(table_name: str):
        def _btn() -> None:
            if reload_confirm is not None and terminal:
                ui.button(
                    "Reload",
                    on_click=lambda e, n=table_name: reload_confirm(n, e),
                ).props(
                    "flat dense no-caps size=sm color=primary icon=replay"
                ).tooltip(
                    "Reload just this table (e.g. after fixing the source value)."
                )
            elif reload_table is not None and terminal:
                ui.button(
                    "Reload", on_click=lambda n=table_name: reload_table(n)
                ).props(
                    "flat dense no-caps size=sm color=primary icon=replay"
                ).tooltip(
                    "Reload just this table (e.g. after fixing the source value)."
                )
        return _btn

    if index_only:
        # Data-complete, index-missing. Deliberately its own INFO-toned block, apart
        # from failures and quarantine: nothing is missing from the target, so the
        # migration can proceed -- but the operator has to know an index they asked
        # for does not exist, since queries relying on it will be slow.
        with ui.column().classes("w-full gap-2"):
            render_notice(
                ui,
                tone="info",
                header=(
                    f"Indexes not created ({len(index_only)}) — the data loaded "
                    "completely"
                ),
                body=(
                    "Every row is on the target; only these secondary indexes are "
                    "missing, so no data was lost and you do not need to re-run the "
                    "load. Add them later, or reduce the table's index count — Aurora "
                    "DSQL allows 24 indexes per table, including the primary key. "
                    "Queries that depend on a missing index will be slower until it "
                    "exists."
                ),
            )
            for row in index_only:
                _failure_row(
                    table_name=f"{row.table} · Done — index missing",
                    message=row.error_message,
                    tone="info",
                )

    # Prefer the FULL per-row records: one entry per dropped row. Falls back to the
    # per-table view when the caller passed none (an older call site, or a restored
    # session whose in-memory log is gone) -- one entry per table is still better than
    # nothing, and the count above remains authoritative either way.
    quarantine_entries: "list[tuple[str, str]]" = [
        (table, message)
        for table, message in (quarantine_records or ())
        if str(message).startswith(quar_prefix)
    ] or [(r.table, str(r.error_message)) for r in quarantined]
    if quarantine_entries:
        # NO header notice here. The completeness banner below already states the verdict
        # ("N rows permanently dropped (table)") and the remedy, and the summary chip +
        # per-row Status badge state the count above -- a header repeating it made a
        # 3-run drop announced in four boxes on one screen. This section's job is the
        # per-row DETAIL (which row, why, and Reload), which nothing else provides.
        #
        # GROUPED BY TABLE, one card per table. A card per ROW repeated the table name and
        # the reason once per row, and -- worse -- showed a "Reload" button per row when
        # Reload is per-TABLE: three identical buttons doing the same thing, each looking
        # like it acted on its own row. Grouping states the shared facts once and lists the
        # primary keys as chips, which stays compact when a table drops many rows.
        with ui.column().classes("w-full gap-2"):
            for table_name, pks, reasons in _group_quarantine_entries(
                quarantine_entries
            ):
                _quarantine_detail_row(
                    ui,
                    table=table_name,
                    primary_keys=pks,
                    reasons=reasons,
                    action=_quar_reload(table_name),
                )

def _render_accept_quarantine_action(
    ui,
    *,
    quarantine_only: bool,
    terminal: bool,
    quarantine_accepted: bool,
    accept_quarantine_and_continue=None,
) -> None:
    """Render the accept-the-gap action (or its accepted state), or nothing.

    Rendered AFTER the completeness banner, not inside the quarantine panel: the banner
    states the verdict and names the remedy, so the button that carries out that remedy
    belongs directly beneath it. Sitting above the verdict, it asked the operator to
    decide before reading the conclusion they were deciding on.

    Only offered when quarantine is the ONLY incompleteness -- a retryable failure must be
    retried or reloaded first, never waved past.
    """
    if not (quarantine_only and terminal and accept_quarantine_and_continue is not None):
        return
    with ui.row().classes("items-center gap-2 w-full"):
        if quarantine_accepted:
            # A one-line confirmation, NOT a second success notice. Accepting flips the
            # completeness banner directly above to "Full Load complete — with an accepted
            # gap", which already states the count, the table, that the next step is
            # unblocked, and that Validation reports the gap. A full notice here repeated
            # all of that in a second green box -- two boxes, nearly the same words. What
            # the banner does NOT say is that the gap remains closable, so that is all
            # this line adds; the checkmark alone acknowledges the click.
            with ui.row().classes("items-center gap-1 no-wrap"):
                ui.icon("check_circle").classes("text-green-600 text-base")
                ui.label(
                    "Gap accepted — reloading a table after fixing its source value "
                    "still closes it."
                ).classes("text-xs text-gray-600")
        else:
            ui.button(
                "Accept quarantined rows & continue",
                on_click=accept_quarantine_and_continue,
                icon="check",
            ).props("unelevated no-caps color=warning")


def _render_completeness_banner(
    ui,
    completeness: "FullLoadCompleteness",
    *,
    approximate: bool = False,
    quarantine_accepted: bool = False,
) -> None:
    """Render the source-vs-loaded completeness verdict once the load settles.

    ``approximate`` says the per-table baseline (``expected_rows``) came from the
    watermark's scan-free ``information_schema`` ESTIMATES rather than exact
    counts. When the only discrepancies are row-count gaps (no actual FAILED
    table) and the baseline was approximate, a "mismatch" is expected noise --
    the estimate is off, or rows were inserted after the snapshot -- so the
    verdict is shown as a calm INFO note pointing at Validation for an exact
    check, NOT a red "finished with issues" alert. A genuinely FAILED table is
    always surfaced as a warning regardless of the baseline's precision.
    """
    if completeness.total == 0 or completeness.settled != completeness.total:
        return  # still running -- the verdict is only meaningful when settled
    if completeness.all_complete:
        _render_notice(
            ui,
            tone="success",
            header="Full Load complete",
            body=(
                f"All {completeness.total} tables loaded every source row "
                "(loaded rows match the source snapshot count)."
            ),
        )
        return

    # The gap was explicitly ACCEPTED and nothing else is wrong: the run is complete by
    # the operator's own decision, so keeping the amber "finished with issues" contradicted
    # the green "Gap accepted" notice directly above it. State the gap plainly, but do not
    # re-flag as a problem something the operator has already resolved -- while never
    # claiming every row loaded, because they did not.
    if (
        quarantine_accepted
        and completeness.failed == 0
        and completeness.quarantined_rows
    ):
        row_noun = "row" if completeness.quarantined_rows == 1 else "rows"
        _render_notice(
            ui,
            tone="success",
            header="Full Load complete — with an accepted gap",
            body=(
                f"{completeness.quarantined_rows} {row_noun} could not be stored and "
                f"were permanently dropped ({', '.join(completeness.quarantined_tables)}"
                "); you accepted that gap, so the next step is unblocked. Every other "
                "row loaded. Validation (Step 4) still reports the gap against the "
                "source."
            ),
        )
        return

    # An estimate-only discrepancy (no real failure, approximate baseline) is an
    # informational note, not a failure: the counts simply differ from the
    # scan-free estimate. Surface it calmly (AWS-style info box) and defer to
    # Validation for the exact truth.
    # Quarantined rows are a CONFIRMED loss, so they can never be the calm
    # "counts differ from the estimate" note however approximate the baseline is --
    # nothing about a scan-free estimate explains a row the loader dropped.
    estimate_only = (
        approximate
        and completeness.failed == 0
        and completeness.quarantined_rows == 0
    )
    if estimate_only:
        notes: list[str] = []
        if completeness.mismatched:
            notes.append(
                f"{len(completeness.mismatched)} table(s) differ from the estimate "
                f"({', '.join(completeness.mismatched)})"
            )
        if completeness.unknown:
            notes.append(
                f"{completeness.unknown} without an estimate to compare"
            )
        _render_notice(
            ui,
            tone="info",
            header="Full Load finished — counts differ from the pre-load estimate",
            body=(
                "The pre-load row counts are approximate (scan-free "
                "information_schema figures, not exact) and drift when rows change "
                "during the load: "
                + "; ".join(notes)
                + ". This is expected — run Validation (Step 4) for an exact "
                "row-count/checksum check to confirm."
            ),
        )
        return

    problems: list[str] = []
    if completeness.failed:
        problems.append(f"{completeness.failed} failed")
    # Lead with the dropped rows: it is the one certainty in this list, and stating it
    # as its own item stops it hiding inside a generic "row-count mismatch".
    if completeness.quarantined_rows:
        row_noun = "row" if completeness.quarantined_rows == 1 else "rows"
        problems.append(
            f"{completeness.quarantined_rows} {row_noun} permanently dropped "
            f"({', '.join(completeness.quarantined_tables)})"
        )
    # Don't double-report a table already named as quarantined: its shortfall IS the
    # dropped rows, so listing it again as a "mismatch" reads like a second problem.
    _mismatched_only = [
        name
        for name in completeness.mismatched
        if name not in set(completeness.quarantined_tables)
    ]
    if _mismatched_only:
        problems.append(
            f"{len(_mismatched_only)} row-count mismatch "
            f"({', '.join(_mismatched_only)})"
        )
    if completeness.unknown:
        problems.append(
            f"{completeness.unknown} without a source count to compare"
        )
    # Tailor the remedy to the problems actually present. "Retry the failed tables" is
    # dead-end advice when nothing FAILED -- a quarantining table is DONE, so it is not
    # in the retry set; the way to recover those rows is to fix the source value and
    # reload that table.
    if completeness.failed:
        remedy = (
            "Retry the failed tables, or run Validation (Step 4) for a full "
            "row-count/checksum check."
        )
    elif completeness.quarantined_rows:
        remedy = (
            "The dropped rows are listed above with their reason: fix the source "
            "value(s) and Reload that table to load them, or accept the gap to "
            "continue (Validation reports it)."
        )
    else:
        remedy = "Run Validation (Step 4) for a full row-count/checksum check."
    _render_notice(
        ui,
        tone="warning",
        header="Full Load finished with issues",
        body="; ".join(problems) + ". " + remedy,
    )


def _render_error_log(ui, migration_state, job: MigrationJob) -> None:
    """Render the data-error summary and a download button when errors exist.

    The summary count equals the rows in the downloadable log (Property 15); the
    button is hidden when there are no errors.
    """
    job_id = job.job_id
    # Full Load records only -- CDC shares this key, and its dead-lettered rows belong
    # to the CDC panel's own download (see full_load_error_records).
    records = full_load_error_records(migration_state.error_log, job_id)
    summary = full_load_error_summary(migration_state.error_log, job_id)
    # NO "Data errors" heading + count when there is nothing to download: with zero
    # errors it printed a section header over "No data errors recorded." -- a whole block
    # asserting an absence. And when there ARE errors, every one of them is already shown
    # above with its table, primary key and reason, so a heading restating the count was
    # the same fact a fourth time. What this section uniquely offers is the DOWNLOAD, so
    # it is now just that button (its label already names what it contains).
    if summary.total_errors > 0:
        def _download_log() -> None:
            try:
                # Serialize the FILTERED records; render_log(job_id) would re-read the
                # whole key and put CDC rows into a file labelled Full Load.
                payload = migration_state.error_log.render_records(records)
                ui.download.content(  # type: ignore[attr-defined]
                    payload, f"error_log_{job_id}.ndjson", "application/x-ndjson"
                )
            except Exception as exc:  # noqa: BLE001 - surface instead of silent
                _LOGGER.exception("Failed to render/download error log")
                ui.notify(  # type: ignore[attr-defined]
                    f"Could not generate the error log: {exc}", type="negative"
                )

        with ui.row().classes("items-center gap-2 w-full"):
            # Name it in the user's terms: WHAT it is ("Full Load error log") and HOW MUCH
            # ("3 errors"). "Download error log (NDJSON)" led with a file format nobody
            # asked about and never said which step's errors it held.
            noun = "error" if summary.total_errors == 1 else "errors"
            ui.button(
                f"Download Full Load error log ({summary.total_errors} {noun})",
                on_click=_download_log,
                icon="download",
            ).props("outline no-caps size=sm").tooltip(
                format_error_summary(summary)
                + " One line per error with the table, primary key, reason and "
                "timestamp — never row values. Saved as NDJSON (one JSON object per "
                "line), readable in any text editor."
            )
