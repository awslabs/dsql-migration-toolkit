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

import dataclasses
import logging
from datetime import datetime, timezone
from typing import Callable, Mapping, Optional, Sequence

from dsql_migrator.core.batched_import import is_unconfirmed_index_failure
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobNotFoundError
from dsql_migrator.core.models import (
    LoadKind,
    LoadStatusView,
    MigrationJob,
    SourceType,
    StepStatus,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.core.schema_applier import OverlayRefusedError, build_overlay_statements
from dsql_migrator.core.target_introspector import (
    target_overlay_facts,
    target_primary_keys,
    tables_present,
    tables_with_rows,
)
from dsql_migrator.ui.design import inline_hint, render_notice
from dsql_migrator.ui.workflow import WorkflowStep, with_status
from dsql_migrator.ui.data_migration._full_load_engine import (
    full_load_progress_caption,
    job_status_to_step_status,
    new_start_point_block,
    sync_identity_sequences_for_tables,
)
from dsql_migrator.ui.data_migration._models import (
    MigrationType,
    session_source_type,
    SCOPE_ADDED_REASON,
    SCOPE_ADDED_REASON_FL_CDC,
    SCOPE_ADDED_REASON_PG_SUFFIX,
    SCOPE_ADDED_QUARANTINED_REASON,
    CDC_NOT_ADDABLE_REASON,
    cdc_addition_has_start_point,
    cdc_not_addable_refusal,
    full_load_scope_names,
    scope_change,
    scope_full_load_job,
    scoped_full_load_view,
    scope_notice,
    FullLoadCompleteness,
    FullLoadTableRow,
    _CDC_MIGRATION_TYPES,
    _LOAD_STATE_ORDER,
    _UNAVAILABLE,
    build_full_load_table_rows,
    format_error_summary,
    format_table_timing,
    format_watermark,
    full_load_completeness,
    exclusion_forced_replace,
    summarize_table_states,
    unsettled_table_names,
)
from dsql_migrator.ui.data_migration._cdc_status import (
    _current_job,
    full_load_error_records,
    full_load_error_records_for,
    full_load_error_summary,
    full_load_error_summary_for,
    full_load_latest_messages,
    full_load_latest_messages_for,
)
from dsql_migrator.ui.data_migration._cdc_ui import cdc_streaming_started

_LOGGER = logging.getLogger(__name__)

# How often the live Full Load progress region polls the background job (seconds).
_POLL_INTERVAL_SECONDS = 1.5

# G17: why a Full load + CDC table selected after its Full Load is forced to Drop & reload
# (rendered as "{table}: {reason}."). True on both engines: the tool cannot tell how old the
# rows are -- older than the Full Load's start point (MySQL replays only the changes since
# then) or than the table's publication join (PostgreSQL streams only the changes since
# then) -- and an append keeps every row already there.
_ADDITION_FORCED_REASON = (
    "selected after the Full Load, and Aurora DSQL already holds rows in it that may predate "
    "the changes CDC captures for it (an append keeps every row already there), so it is "
    "dropped and reloaded"
)

# G25 (decision A): while CDC is live the step keeps its "Stop CDC first" gate. A Full Load
# that takes a new start point then still needs new CDC topics (new_start_point_block), so the
# copy names the whole route and never a resume "from the new snapshot": Stop CDC removes the
# sink without a drain, and its consumer group would re-apply older changes over the reload.
_CDC_LIVE_START_TOOLTIP = (
    "CDC is streaming -- starting Full Load would collide with the live pipeline. Stop CDC "
    "first (CDC step → Stop CDC), then Delete CDC infrastructure and deploy it again before a "
    "new Full Load."
)
_CDC_LIVE_START_HINT = (
    "CDC is live, so Full Load is disabled. Stop CDC first (CDC step → Stop CDC), then Delete "
    "CDC infrastructure and deploy it again before a new Full Load."
)
# The confirm dialog's "CDC is currently streaming" notice: why no Full Load runs then, then
# the way on -- by the action, since a Retry or a Reload keeps the original start point and
# converges once CDC is stopped. G129: a load while the sink streams inserts the rows missing
# on the target, so a row the source deletes while its table is copied, deleted by the sink
# before the copy wrote it (a delete that found nothing), is written back and stays.
_CDC_LIVE_COLLISION = (
    "A Full Load cannot run while CDC is streaming: the CDC sink applies changes to the same "
    "tables, and a row the source deletes while the load is copying its table can reach Aurora "
    "DSQL after CDC has applied that delete, and then stays there."
)
# G129: Retry unfinished tables, Reload specific tables and a per-table Reload keep the original
# start point, but they load rows too, so while CDC is live they are disabled with this reason.
_CDC_LIVE_RELOAD_TOOLTIP = (
    "CDC is streaming -- reloading now could write back rows CDC deletes meanwhile. Stop CDC "
    "first (CDC step → Stop CDC), reload, then Start CDC again."
)
_CDC_LIVE_RETRY_HINT = (
    "CDC is live, so Retry unfinished tables is disabled. Stop CDC first (CDC step → Stop CDC), "
    "retry, then Start CDC again: the retry keeps the original start point, so CDC continues "
    "from where it stopped, gapless only while the CDC topics still retain what the sink had "
    "not applied (their retention period on MSK)."
)
_CDC_LIVE_NEW_START_POINT = (
    "Stop CDC first (CDC step → Stop CDC). A new start point then needs new CDC topics: Delete "
    "CDC infrastructure and deploy it again, then Re-run Full Load choosing 'Drop & reload', "
    "then Start CDC."
)
# Rule D: Stop CDC removes the sink without a drain, so what it had not applied reaches the
# target on the restart only from the CDC topics.
_CDC_LIVE_KEEPS_START_POINT = (
    "Stop CDC first (CDC step → Stop CDC), run this, then Start CDC again: this run keeps the "
    "original start point, so CDC continues from where it stopped, gapless only while the CDC "
    "topics still retain what the sink had not applied (their retention period on MSK)."
)
# G132: the gate above also holds while this session's Start CDC, Stop CDC or Delete CDC
# infrastructure runs (cdc_streaming_started counts each, and the CDC step then shows only that
# job's progress), but "CDC is streaming -- Stop CDC first" is not what is happening then: the
# operation ends by itself. So the reason is a wait for it, worded per operation (as the type
# and table-set locks word theirs), and the action stays disabled. Keyed by
# cdc_operation_in_flight; a Retry or a Reload (the original start point) and a run that takes
# a new start point go on differently once it has finished.
_CDC_OP_WAIT_RELOAD = {
    "start": (
        "Start CDC is in progress -- wait for it to finish (its progress is on the CDC step). "
        "Once CDC streams, a reload needs Stop CDC first (Stop CDC, reload, then Start CDC "
        "again): beside the live sink it could write back rows CDC deletes meanwhile."
    ),
    "stop": (
        "Stop CDC is in progress -- wait for it to finish (its progress is on the CDC step), "
        "reload, then Start CDC again."
    ),
    "delete": (
        "Delete CDC infrastructure is in progress -- wait for it to finish (its progress is on "
        "the CDC step), then reload."
    ),
}
_CDC_OP_WAIT_START = {
    "start": (
        "Start CDC is in progress -- wait for it to finish (its progress is on the CDC step). "
        "Once CDC streams, a new Full Load needs Stop CDC first, then Delete CDC "
        "infrastructure and deploy it again."
    ),
    "stop": (
        "Stop CDC is in progress -- wait for it to finish (its progress is on the CDC step), "
        "then Delete CDC infrastructure and deploy it again before a new Full Load."
    ),
    "delete": (
        "Delete CDC infrastructure is in progress -- wait for it to finish (its progress is on "
        "the CDC step) before a new Full Load."
    ),
}
# The confirm dialog's notice header while one runs (its body is the wait above).
_CDC_OP_WAIT_HEADER = "Full Load waits for the CDC operation to finish"


def cdc_load_hold(
    migration_state, job_manager, *, keeps_watermark: bool
) -> "Optional[tuple[str, Optional[str]]]":
    """``(reason, operation)`` while CDC holds every Full Load (G129), else ``None``.

    ``reason`` is the one line a disabled action's tooltip, a refused click's toast and the
    screen's backstops give -- for a Retry or a Reload (``keeps_watermark``) or for a run that
    takes a new start point. ``operation`` is this session's CDC lifecycle job still running
    ("start", "stop" or "delete"), whose wait the reason then names (G132), or ``None`` while
    CDC streams (Stop CDC first). Pure apart from the job reads."""
    if not cdc_streaming_started(migration_state, job_manager):
        return None
    from dsql_migrator.ui.data_migration._cdc_monitoring import cdc_operation_in_flight

    operation = cdc_operation_in_flight(migration_state, job_manager)
    if operation is not None:
        waits = _CDC_OP_WAIT_RELOAD if keeps_watermark else _CDC_OP_WAIT_START
        return waits[operation], operation
    return (_CDC_LIVE_RELOAD_TOOLTIP if keeps_watermark else _CDC_LIVE_START_TOOLTIP), None


# G27 (rule E): in a CDC migration type, a run that takes a new start point (Start, Re-run Full
# Load, Re-run all tables) gives CDC its new watermark. Under the Append / Drop & reload choice,
# Append is then not called "recommended" and this line names Drop & reload. Append stays the
# default (never preselect the destructive option); Full load only and a Retry / Reload (the
# original watermark) keep the choice as it was.
_APPEND_NEW_CDC_START_POINT = (
    "CDC from this run's new start point needs 'Drop & reload': an Append keeps the rows "
    "already on the target as they were, and CDC from the new watermark never replays the "
    "changes made before it."
)


def _call_or(provider, default):
    """Resolve a provider (or a plain value) to a value, falling back to ``default``.

    Lets a live region re-read its inputs on every refresh instead of rendering whatever
    they were when the page was first built. A plain value is passed through so a caller --
    or a test -- can still supply one, and a provider that raises yields ``default`` rather
    than tearing down the region it was being rendered into.
    """
    if provider is None:
        return default
    if not callable(provider):
        return provider
    try:
        return provider()
    except Exception:  # noqa: BLE001 - a status read must never break the render
        return default

# Cloudscape "Alert" renderer alias (single source of truth in ui.design).
_render_notice = render_notice

def _render_watermark(ui: object, job: MigrationJob, source_type=None) -> None:
    """Render the export watermark for ``job`` (Requirement 8.5 / Property 11).

    ``source_type`` is the session's source engine, passed so the panel does not have to
    INFER it from the presence of a WAL LSN -- a PostgreSQL run whose best-effort LSN read
    came back empty otherwise rendered MySQL binlog/GTID rows (see
    :func:`_models.format_watermark`). Optional so an existing caller/test without a
    session keeps the old inference.

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

    display = format_watermark(job.watermark, source_type=source_type)
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
        # A PostgreSQL source has a WAL LSN + replication slot + publication (no
        # binlog/GTID/server-uuid), so show those rows instead of rendering the MySQL
        # fields as "unavailable" and hiding the LSN the loader actually captured.
        if display.is_postgres:
            field_rows = (
                ("Snapshot (UTC)", display.snapshot_timestamp),
                ("WAL LSN", display.wal_lsn),
                ("Replication slot", display.slot_name),
                ("Publication", display.publication_name),
            )
        else:
            field_rows = (
                ("Snapshot (UTC)", display.snapshot_timestamp),
                ("Binlog file:pos", display.coordinate),
                ("GTID set", display.gtid),
                ("Server UUID", display.server_uuid),
            )
        for label, value in field_rows:
            # An unavailable coordinate is normal (binary logging off / SHOW MASTER
            # STATUS restricted on RDS/Aurora for MySQL; a not-yet-created slot for PG),
            # so it is muted -- not styled like a missing required value.
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
                        # ``_abbrev_count`` renders an UNKNOWN estimate (None, e.g. a
                        # never-analyzed PostgreSQL table) as an em dash rather than 0.
                        ui.label(_abbrev_count(rows_count)).classes(  # type: ignore[attr-defined]
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


def full_load_confirm_summary(
    names: Sequence[str],
    engine,
    migration_type,
    *,
    keeps_watermark: bool,
    cdc_live: bool = False,
    publication_additions: "Sequence[str] | frozenset[str]" = (),
) -> str:
    """The Full Load confirm dialog's summary line for the tables this run loads. Pure.

    What the run writes on the source depends on the action (G25). On a PostgreSQL Full
    load + CDC, a run that takes a new snapshot (Start, Re-run all tables) provisions CDC's
    replication objects at it (provision_pg_replication) -- unless CDC is streaming
    (``cdc_live``), when it provisions nothing. It creates the publication or reuses one,
    adding any missing table and setting a reused one's publish_via_partition_root to true
    if it is false (_repair_reused_publication, before the slot); it REPLACES the
    replication slot (one left by an earlier run is dropped and a new one created -- such a
    run waits for new CDC topics on a stack that has streamed, new_start_point_block); and
    it sets REPLICA IDENTITY FULL on a table whose CDC key was
    changed. A Retry or Reload (``keeps_watermark``) runs under the original watermark and
    only reads the source, except that a table selected after the Full Load (G17,
    ``publication_additions``) joins CDC's publication before its copy, the guard first
    setting the publication's publish_via_partition_root to true if it is false -- the
    ticked ones are named, as a possibility: an earlier guarded attempt that joined it can
    leave it pending (the wait after the join timed out, or the copy failed or
    quarantined), and the guard writes nothing for a member. Each catalog write an action
    can make on the source is named, as the dialog is where the operator learns of them.
    Every other engine and type only reads the source.
    """
    if not names:
        # A tick-to-choose dialog before the first tick (its confirm stays disabled).
        return "No tables ticked yet: tick the tables to load below."
    pg_cdc = engine is SourceType.POSTGRES and migration_type is MigrationType.FULL_LOAD_AND_CDC
    additions = set(publication_additions or ())
    joining = [n for n in names if n in additions]
    if pg_cdc and not keeps_watermark and not cdc_live:
        source = (
            "the source's tables are only read, but the tool also creates the CDC "
            "publication on the source (or reuses one, adding any missing table and setting "
            "its publish_via_partition_root to true if it is false), replaces the "
            "replication slot (a slot left by an earlier run is dropped and a new one is "
            "created at this snapshot) and sets REPLICA IDENTITY FULL on a table whose CDC "
            "key was changed"
        )
    elif pg_cdc and keeps_watermark and joining:
        from dsql_migrator.ui.cdc_dlq_gate import bounded_table_names

        one = len(joining) == 1
        source = (
            f"the source is accessed read only, except that {bounded_table_names(joining)} "
            f"{'joins' if one else 'join'} CDC's publication on the source if "
            f"{'it is not a member' if one else 'they are not members'} yet, and the "
            "publication's publish_via_partition_root is set to true if it is false"
        )
    else:
        source = "the source is accessed read only"
    return (
        f"{format_selected_workloads(names)}. The target tables will receive the snapshot "
        f"rows; {source}."
    )


def with_data_choice_text(count: int, *, ticked_scope: bool) -> str:
    """The Append / Drop & reload card's lead for ``count`` tables holding data. Pure.

    In a tick-to-choose dialog (Reload specific tables, Retry) the count follows the ticks:
    it counted every loaded table, so one ticked table read as "4 selected table(s) already
    contain data" above a Drop & reload choice that drops only the ticked one."""
    noun = "table" if count == 1 else "tables"
    if ticked_scope and count == 0:
        return "No ticked table holds data on the target yet; this choice applies to any that does:"
    if ticked_scope:
        return (
            f"{count} ticked {noun} already contain{'s' if count == 1 else ''} data on the "
            "target. Choose how to load them:"
        )
    return f"{count} selected table(s) already contain data on the target. Choose how to load them:"


def _overlay_statement_facts(view, facts, name, *, from_ddl):
    """The facts one table's overlay statements are built from: the applied DDL for a
    table this run recreates (or when the target could not be read), else the live
    catalog. Columns Aurora DSQL computes are omit-only and left out."""
    overlay = sorted(view.overlay_by_table.get(name, ()))
    if from_ddl or facts is None or facts.get(name) is None:
        chosen = view.ddl_facts_for(name) or {}
    else:
        chosen = {c: facts[name].columns.get(c) for c in overlay}
    return {c: f for c, f in chosen.items() if c in overlay and f is not None and not f.computed}


def _render_overlay_blocks(
    ui, ov, *, action_tables, forced, recreate_now, radio_tables, cdc_live_now, dialog,
    on_retry, reload_mode, on_refreshable, scope=None,
) -> None:
    """The Start dialog's column-exclusion blocks (only what applies is drawn):
    the unreadable-record error with [Retry], the could-not-read-the-target warning, and --
    re-rendered with the reload choice and, in a checklist, with the ticked tables -- the
    NULL line, the exact ALTERs and the forced tables with their reasons. With no overlay
    (``ov`` empty) only a G17 addition can be forced, so only that notice is drawn. While CDC
    is live nothing is forced and nothing runs (G129: the dialog cannot be confirmed then), so
    the appends a load beside the sink would make are not described either (G132)."""
    view = ov.get("view")
    outcome = ov.get("outcome")
    facts = ov.get("facts")
    if ov.get("ledger_error"):
        render_notice(
            ui,
            tone="error",
            header="Could not read the record of excluded-column changes",
            body=(
                "The tool records the DEFAULTs it drops on this Aurora DSQL target, and "
                "which tables were loaded under which exclusion set. That record could "
                "not be read, so Start is disabled. Nothing was changed; retry."
            ),
        )
        if on_retry is not None:
            async def _retry() -> None:
                dialog.close()
                await on_retry()

            ui.button("Retry", on_click=_retry, icon="refresh").props("flat color=primary")
        return
    undecided = list(getattr(outcome, "undecided", ()) or ())
    if ov.get("probe_failed") or undecided:
        named = f" for {', '.join(undecided)}" if undecided else ""
        render_notice(
            ui,
            tone="warning",
            header="Could not read the Aurora DSQL target",
            body=(
                f"Whether a table needs Drop & reload could not be decided here{named}. A "
                "table that holds rows under another exclusion set fails \"needs Drop & "
                "reload\" when the load starts instead of being appended."
            ),
        )

    def _in_scope(table: str) -> bool:
        return scope is None or table in scope()

    def _blocks(mode: str = reload_mode) -> None:
        null_columns = dict(getattr(outcome, "null_columns", {}) or {}) if outcome else {}
        if not null_columns and view is not None:
            null_columns = {t: tuple(sorted(c)) for t, c in view.overlay_by_table.items() if c}
        null_columns = {t: c for t, c in null_columns.items() if _in_scope(t)}
        if null_columns:
            n_cols = sum(len(c) for c in null_columns.values())
            inline_hint(
                ui,
                f"{n_cols} column{'s' if n_cols != 1 else ''} in {len(null_columns)} "
                f"table{'s' if len(null_columns) != 1 else ''} will load as NULL",
                tone="neutral",
                classes="text-sm",
            )
            with ui.expansion("show columns").classes("w-full text-xs"):
                for table in sorted(null_columns):
                    for column in null_columns[table]:
                        ui.label(f"{table}.{column}").classes("text-xs font-mono")
        if view is not None:
            recreated = set(forced) | set(recreate_now) | (
                set(radio_tables) if mode == "drop" and not cdc_live_now else set()
            )
            statements = []
            for table in action_tables:
                if table not in view.overlay_by_table or not _in_scope(table):
                    continue
                chosen = _overlay_statement_facts(
                    view, facts, table,
                    from_ddl=table in recreated or bool(ov.get("probe_failed")),
                )
                try:
                    statements.extend(build_overlay_statements(table, chosen))
                except OverlayRefusedError:  # computed columns were filtered; defensive
                    continue
            if statements:
                from dsql_migrator.ui.data_migration._models import overlay_change_summary

                render_notice(
                    ui,
                    tone="warning",
                    header="Aurora DSQL changes before loading: "
                    + overlay_change_summary(st.action for st in statements),
                    body=(
                        "Each excluded column is made nullable with no DEFAULT before its "
                        "table's first row, so every row stores NULL there."
                        + (
                            " The target could not be read, so these follow the applied "
                            "Schema Conversion; only what the target still needs is run."
                            if ov.get("probe_failed")
                            else ""
                        )
                    ),
                )
                with ui.expansion("show statements").classes("w-full text-xs"):
                    for statement in statements:
                        # "(DSQL)": these run on the target, never on the source.
                        ui.label(f"(DSQL) {statement.text()}").classes(
                            "text-xs font-mono wrap-anywhere")
        shown = {t: r for t, r in forced.items() if _in_scope(t)}
        if shown:
            render_notice(
                ui,
                tone="error",
                header=(
                    f"{len(shown)} table{'s' if len(shown) != 1 else ''} will be dropped "
                    "and reloaded"
                ),
                body=" ".join(f"{table}: {reason}." for table, reason in shown.items()),
            )

    refreshable = ui.refreshable(_blocks)
    refreshable()
    on_refreshable(lambda mode: refreshable.refresh(mode))


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
    # Recovery for rows quarantined over DSQL's ~1 MiB per-value limit: the offending
    # column can only be known AFTER a load. "Exclude column & reload" opens the exclusion
    # popup routed by the lock at click time (exclusion_route(table, reason)); None -> not
    # offered. ``exclusion_route_block`` disables it with that reason.
    lob_candidates_for: Optional[Callable[[str], Sequence[tuple[str, str]]]] = None,
    exclusion_route: Optional[Callable[[str, str], None]] = None,
    exclusion_route_block: Optional[str] = None,
    # Foreign keys are applied by an EXPLICIT action now, not as part of the load. The
    # orphan pre-check is O(child rows) (measured 28.0 min serial / 7.9 min at 8-way for 15
    # FKs over 15.5M child rows) and it ran AFTER every row was already written, so it held
    # the end of the load for minutes. A CDC migration already applies them at cut over;
    # this makes the Full-Load-only path the same flow.
    apply_foreign_keys: Optional[Callable[[], None]] = None,
    # PROVIDERS, deliberately not values: the FK card renders inside the @ui.refreshable
    # live region below, so anything evaluated at page-render time is frozen for the life of
    # the page. Passing values meant a refresh re-rendered the card with the state from
    # BEFORE the button was clicked -- the spinner never advanced and never gave way to the
    # result, even with every constraint already on the target. Each is called on every
    # refresh instead. They stay Optional so a caller that needs none passes none.
    foreign_keys_pending: Optional[Callable[[], int]] = None,
    foreign_keys_applied: Optional[Callable[[], Optional[tuple]]] = None,
    foreign_keys_running: Optional[Callable[[], bool]] = None,
    foreign_keys_progress: Optional[Callable[[], Optional[tuple]]] = None,
    cancel_foreign_keys: Optional[Callable[[], None]] = None,
    # The column-exclusion overlay (see make_overlay_dialog_providers): a BLOCKING ledger
    # read of the connected target, and a cheap view of the overlay / excluded sets and the
    # applied DDL. None (library callers, test doubles) keeps the previous dialog.
    overlay_ledger_read: Optional[Callable[[], object]] = None,
    exclusion_overlay_view: Optional[Callable[[Sequence[str]], object]] = None,
    # The source floor for the identity sync a scoped set's DONE submits (see
    # submit_scope_identity_sync); called on that job's thread. None: no floor.
    identity_extras: Optional[Callable[[Sequence[str]], Mapping[str, object]]] = None,
    # Full load + CDC before any load: one CDC pipeline cannot capture every selected table, or
    # may not (pre_load_table_list_warning: its header and body). A warning above Start Full
    # Load, never a hold.
    pre_load_warning: Optional[tuple[str, str]] = None,
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
    # G12: in Full load only the table set can change after the load (G16/G17: in Full load
    # + CDC it can drop and add tables until CDC starts -- an added one stays unfinished
    # until its guarded load extends the watermark's coverage -- and the view is
    # removal-only after that). Every chunk-driven read below follows the CURRENT selection
    # through a scoped view of the record (the record itself whenever nothing changed); the
    # watermark and the error-log download keep the real record.
    _record_job = job

    def _scoped(record):
        if record is None or getattr(record, "status", None) in ("PENDING", "RUNNING"):
            # Never scoped (scope_full_load_job returns it as is): skip the predicate's job
            # read, which every running-poll tick would otherwise pay for nothing.
            return record
        return scoped_full_load_view(migration_state, job_manager, selected_names, job=record)

    job = _scoped(job)
    _added, _removed = scope_change(_record_job, job)
    # G17: the Full load + CDC tables the watermark does not cover yet (the "added" half
    # there, whatever the record holds for them), the ones a guarded Retry cannot add
    # (MySQL, no start-point definition recorded), and whether their load first joins
    # them to CDC's publication (PostgreSQL with the load's slot).
    _engine = session_source_type(session)
    _pending: "list[str]" = []
    _not_addable: "list[str]" = []
    _pg_publication = False
    if (
        getattr(migration_state, "migration_type", None) is MigrationType.FULL_LOAD_AND_CDC
        and _record_job is not None
        and getattr(_record_job, "status", None) not in ("PENDING", "RUNNING")
    ):
        from dsql_migrator.ui.data_migration._cdc_monitoring import fl_cdc_pending_additions

        _pending = fl_cdc_pending_additions(
            migration_state, job_manager, selected_names, job=_record_job
        )
        _added = list(_pending)
        _record_wm = getattr(_record_job, "watermark", None)
        _not_addable = [
            n for n in _pending
            if not cdc_addition_has_start_point(_record_wm, n, _engine)
        ]
        _pg_publication = _engine is SourceType.POSTGRES and bool(
            getattr(_record_wm, "slot_name", None)
        )
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

    # G25 (decision A): a Full Load that takes a new start point on a cdc-stack that has
    # streamed (or that is not read yet) waits for new CDC topics. Judged once per render for
    # the buttons below, and again at the click and at the confirm -- the CDC state can change
    # under a rendered button or an open dialog (the stack-phase probe runs off the loop).
    _start_point_block = new_start_point_block(migration_state, _engine)

    def _refuse_new_start_point(keeps_watermark: bool) -> bool:
        """At a click: refuse (with a toast of the refusal's one-line form) a run that takes a
        new start point while the gate holds. A Retry or a Reload is never refused here."""
        if keeps_watermark:
            return False
        block = new_start_point_block(migration_state, _engine)
        if block is None:
            return False
        ui.notify(block[2], type="warning", position="top", multi_line=True)
        return True

    def _refuse_cdc_live(keeps_watermark: bool) -> bool:
        """At a confirm: refuse (with a toast) any Full Load while CDC is live (G129) -- CDC can
        start while the dialog is open. A Retry or a Reload is refused too; while a CDC
        operation runs, the toast says to wait for it (G132)."""
        hold = cdc_load_hold(migration_state, job_manager, keeps_watermark=keeps_watermark)
        if hold is None:
            return False
        ui.notify(hold[0], type="warning", position="top", multi_line=True)
        return True

    def _open_confirm_dialog_now(
        *, action_tables, on_confirm, title, keeps_watermark, table_reasons=None,
        preselect=True, checklist_hint=None, overlay=None, on_retry=None,
        addition_forced=None, cdc_additions=frozenset(),
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

        ``preselect=False`` starts the checklist EMPTY (confirm disabled until
        something is ticked) -- the "reload just these tables" case, where the action
        set is every loaded table and pre-checking all of them would offer a
        full re-load behind a button that says the opposite. ``checklist_hint``
        replaces the retry-specific instruction line for that case.

        ``addition_forced`` (G17: ``{table: reason}``) names the Full load + CDC tables
        selected after the Full Load that the probe found holding rows: they are dropped and
        reloaded whatever the Append / Drop choice says (the rows may predate the changes
        CDC captures for the table, and an append keeps them), exactly like the overlay's
        forced tables.

        ``keeps_watermark`` (G25) is the action kind the summary's source-write sentence
        follows: False for Start / Re-run all tables (a new snapshot, which on PostgreSQL
        Full load + CDC provisions the publication and replaces the slot), True for a Retry
        or Reload (the original watermark: the source is only read, except that each ticked
        ``cdc_additions`` table joins CDC's publication when the watermark has the load's
        slot, its guard setting the publication's publish_via_partition_root to true first
        if it is false). In a CDC migration type it also words the Append / Drop & reload
        choice (G27): a new start point does not call Append recommended and names Drop &
        reload (:data:`_APPEND_NEW_CDC_START_POINT`); Append stays the default either way.
        """
        from nicegui import context as _ctx

        client = _ctx.client
        tables_with_data_now = sorted(migration_state.tables_with_data)
        # Why CDC holds this run (G129; G132: a CDC operation in progress is a wait), or None.
        cdc_hold_now = cdc_load_hold(
            migration_state, job_manager, keeps_watermark=keeps_watermark
        )
        cdc_live_now = cdc_hold_now is not None
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
        # The column-exclusion overlay (None -> the previous dialog, unchanged).
        _ov = overlay or {}
        _outcome = _ov.get("outcome")
        forced = dict(getattr(_outcome, "forced", {}) or {}) if not cdc_live_now else {}
        _addition_forced = dict(addition_forced or {}) if not cdc_live_now else {}
        forced.update(_addition_forced)
        ledger_error = bool(_ov.get("ledger_error"))
        # The populated tables the operator still CHOOSES for (the forced ones are not a
        # choice: the NULL contract needs them dropped and reloaded).
        radio_tables = [n for n in tables_with_data_now if n not in forced]
        # Selected tables that are genuinely MISSING on the target AND that this run
        # will NOT create -> an append would fail each (relation does not exist), and if
        # the user then starts CDC that table is a standing gap CDC never backfills. A
        # missing table IS created when: 'drop' mode (the pre-pass DROP IF EXISTS +
        # CREATE recreates every replace table) or it is a recreate candidate (empty /
        # key-diff, recreated from the applied DDL even in append). Neither runs while
        # CDC is live. ``target_tables_present`` is None when the probe could not read
        # the target -> stay conservative and do NOT warn (avoid a false "all missing").
        _present = migration_state.target_tables_present
        if _present is None:
            missing_uncreated: list[str] = []
        else:
            _will_create = (
                set(action_tables)
                if (migration_state.reload_mode == "drop" and not cdc_live_now)
                else set(recreate_now)
            ) | set(forced)
            missing_uncreated = [
                n for n in action_tables
                if n not in _present and n not in _will_create
            ]
        selectable = table_reasons is not None
        # Live set of checked tables (mutated by the per-row checkboxes). Retry starts
        # with the full action set (all pre-checked) -- the common "retry everything".
        # A scoped reload starts EMPTY: its action set is every loaded table, so
        # pre-checking would arm a full re-load from a button that says "specific".
        checked: set = set(action_tables) if preselect else set()
        # The overlay's changes section re-renders when the reload choice flips.
        _changes: dict = {}

        # Set BEFORE close() on confirm, so the close handler below can tell a confirm
        # (the run consumes the forced set) from Cancel / Esc / a backdrop click / Retry
        # (the forced set is dropped: it is re-derived at every probe).
        confirmed = {"v": False}

        def _build() -> None:
            # Wider than Quasar's 560px default: the overlay statements and the per-table
            # choices read on one line each.
            with ui.dialog() as confirm_dialog, ui.card().style(
                "width: min(820px, 94vw); max-width: 94vw"
            ):
                def _on_close(e: object) -> None:
                    if not bool(getattr(e, "value", True)) and not confirmed["v"]:
                        migration_state.clear_exclusion_forced_replace()

                confirm_dialog.on_value_change(_on_close)
                ui.label(title).classes("text-lg font-semibold")
                _migration_type = getattr(migration_state, "migration_type", None)

                def _ticked(names) -> list:
                    return [n for n in names if n in checked] if selectable else list(names)

                def _summary() -> str:
                    # A G17 addition joins the publication only when the watermark has the
                    # load's slot (_pg_publication); without one Start CDC re-snapshots it.
                    return full_load_confirm_summary(
                        _ticked(action_tables), _engine, _migration_type,
                        keeps_watermark=keeps_watermark, cdc_live=cdc_live_now,
                        publication_additions=cdc_additions if _pg_publication else (),
                    )

                summary_label = ui.label(_summary()).classes("text-sm")
                # Missing target tables an append can't create -> warn BEFORE the load
                # (rather than letting it fail per table, then risk a standing CDC gap).
                if missing_uncreated:
                    _m_noun = "table" if len(missing_uncreated) == 1 else "tables"
                    render_notice(
                        ui,
                        tone="warning",
                        header=(
                            f"{len(missing_uncreated)} selected {_m_noun} do not exist "
                            "on the target"
                        ),
                        body=(
                            f"{format_selected_workloads(missing_uncreated)} — not "
                            "found on Aurora DSQL (deleted, or Schema Conversion not "
                            "applied). Appending cannot create a table, so the load "
                            "would fail for "
                            + ("it" if len(missing_uncreated) == 1 else "each")
                            + " — and if you then start CDC, "
                            + ("that table" if len(missing_uncreated) == 1
                               else "those tables")
                            + " would be a standing gap CDC will not backfill. Apply "
                            "Schema Conversion for "
                            + ("it" if len(missing_uncreated) == 1 else "them")
                            + " first"
                            + (", or choose 'Drop & reload' below to (re)create "
                               + ("it" if len(missing_uncreated) == 1 else "them")
                               + " from your applied conversion."
                               if radio_tables and not cdc_live_now
                               else " (on the Schema Conversion step), then retry.")
                        ),
                    )
                if cdc_hold_now is not None and cdc_hold_now[1] is not None:
                    # G132: a Start CDC, Stop CDC or Delete CDC infrastructure in progress is
                    # an expected wait (it ends by itself), not "CDC is streaming".
                    render_notice(
                        ui, tone="info", header=_CDC_OP_WAIT_HEADER, body=cdc_hold_now[0]
                    )
                elif cdc_live_now:
                    render_notice(
                        ui,
                        tone="error",
                        header="CDC is currently streaming",
                        body=f"{_CDC_LIVE_COLLISION} " + (
                            _CDC_LIVE_KEEPS_START_POINT if keeps_watermark
                            else _CDC_LIVE_NEW_START_POINT
                        ),
                    )
                from dsql_migrator.ui.data_migration._models import (
                    start_dialog_exclusion_notes,
                )

                for _tone, _note in start_dialog_exclusion_notes(migration_state):
                    inline_hint(ui, _note, tone=_tone)
                # Without an overlay too when a G17 addition is forced: the overlay blocks then
                # draw only the forced-table notice -- the same one, re-rendered with the
                # checklist ticks, so an unticked table is never announced as dropped.
                if overlay is not None or _addition_forced:
                    _render_overlay_blocks(
                        ui, _ov, action_tables=action_tables, forced=forced,
                        recreate_now=recreate_now, radio_tables=radio_tables,
                        cdc_live_now=cdc_live_now, dialog=confirm_dialog,
                        on_retry=on_retry, reload_mode=migration_state.reload_mode,
                        on_refreshable=lambda fn: _changes.update({"refresh": fn}),
                        scope=(lambda: checked) if selectable else None,
                    )
                # Retry-failed: a checklist (all pre-checked) with each table's
                # failure reason, so the user can uncheck tables not ready to retry
                # and retry only the rest. Other actions just show badges.
                if selectable and action_tables:
                    ui.label(
                        checklist_hint
                        or "Uncheck any table you're not ready to retry yet (e.g. a "
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
                                _sync_label()

                            with ui.row().classes("items-start gap-2 no-wrap w-full"):
                                ui.checkbox(
                                    value=bool(preselect), on_change=_toggle
                                ).props("dense")
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
                if radio_tables and not cdc_live_now:
                    # G27 (rule E): a new start point in a CDC migration type is CDC's new
                    # watermark, so Append is not "recommended" there and a line names Drop &
                    # reload (_APPEND_NEW_CDC_START_POINT). The default stays Append.
                    _new_cdc_start_point = (
                        _migration_type in _CDC_MIGRATION_TYPES and not keeps_watermark
                    )
                    with ui.card().classes(
                        "w-full bg-amber-50 border border-amber-200 gap-2"
                    ):
                        with_data_label = ui.label(
                            with_data_choice_text(
                                len(_ticked(radio_tables)), ticked_scope=selectable
                            )
                        ).classes("text-sm text-gray-800")
                        # A tick-to-choose dialog names its tables in the checklist above;
                        # badges of every loaded table here would contradict the ticks.
                        if not selectable:
                            with ui.row().classes("items-center gap-1 flex-wrap"):
                                for name in radio_tables:
                                    ui.badge(name).props("color=amber-8 outline")
                        reload_choice = ui.radio(
                            {
                                "append": "Append — keep existing rows, load only "
                                "the missing ones (idempotent"
                                + (")" if _new_cdc_start_point else "; recommended)"),
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
                        if _new_cdc_start_point:
                            inline_hint(ui, _APPEND_NEW_CDC_START_POINT, tone="warning")
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
                    if ledger_error:
                        return  # guard: the record could not be read (button disabled)
                    if cdc_live_now:
                        return  # guard: CDC is streaming (button disabled; G129)
                    if _refuse_new_start_point(keeps_watermark) or _refuse_cdc_live(
                        keeps_watermark
                    ):
                        # G25: the CDC state changed while the dialog was open (a probe read
                        # a stack that has streamed; G129: or CDC started): closed as a
                        # Cancel, nothing runs.
                        confirm_dialog.close()
                        return
                    confirmed["v"] = True
                    # Hand the forced set and the disclosed overlay to the run being built.
                    migration_state.confirm_exclusion_forced_replace()
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
                    def _label_and_color(mode: str) -> "tuple[str, str]":
                        drop_chosen = mode == "drop"
                        if cdc_live_now:
                            # G129: nothing runs while CDC is live (disabled; the notice
                            # above says why), so no label offers a way past it.
                            return "Confirm and start", "primary"
                        forced_now = set(forced) & checked if selectable else set(forced)
                        if forced_now:
                            # The NULL contract drops these whatever the radio says, so
                            # the button counts every table this run will DROP.
                            drop_set = forced_now | (set(radio_tables) if drop_chosen else set())
                            if selectable:
                                drop_set &= checked
                            count = len(drop_set)
                            return (
                                f"Drop {count} table{'s' if count != 1 else ''}, "
                                "recreate and load",
                                "negative",
                            )
                        if radio_tables:
                            # The button label + color follow the current choice so a
                            # destructive drop reads as destructive.
                            return (
                                ("Drop, recreate and load", "negative") if drop_chosen
                                else ("Append and load", "primary")
                            )
                        if recreate_now:
                            # Name the DDL step in the button too, so the action is
                            # visible without relying on the notice above being read.
                            # Stays primary-coloured: these targets are empty.
                            return "Recreate and load", "primary"
                        return "Confirm and start", "primary"

                    confirm_label, confirm_color = _label_and_color(
                        migration_state.reload_mode
                    )
                    start_btn = ui.button(confirm_label, on_click=_confirm).props(
                        f"color={confirm_color}"
                    )

                    def _sync_label(e: object = None) -> None:
                        # One place for the label, colour and enabled state: the radio
                        # change and every checklist toggle call it.
                        mode = (
                            str(getattr(e, "value", "append"))
                            if e is not None and hasattr(e, "value")
                            else migration_state.reload_mode
                        )
                        text, color = _label_and_color(mode)
                        start_btn.set_text(text)
                        start_btn.props(f"color={color}")
                        if selectable:
                            # The summary and the with-data count follow the ticks.
                            summary_label.set_text(_summary())
                            if radio_tables and not cdc_live_now:
                                with_data_label.set_text(
                                    with_data_choice_text(
                                        len(_ticked(radio_tables)), ticked_scope=True
                                    )
                                )
                        # Disabled when the retry checklist has nothing ticked, the
                        # record of excluded-column changes could not be read, or CDC is
                        # streaming (G129).
                        if (selectable and not checked) or ledger_error or cdc_live_now:
                            start_btn.disable()
                        else:
                            start_btn.enable()
                        # The overlay blocks follow the reload choice AND the ticks.
                        if _changes.get("refresh") is not None:
                            _changes["refresh"](mode)

                    _sync_label()
                    # Guard must MATCH the radio's creation condition above
                    # (`tables_with_data_now and not cdc_live_now`). With CDC live the
                    # radio is never created -- nothing runs while a sink streams (G129) --
                    # so the looser `if tables_with_data_now` raised UnboundLocalError and
                    # the whole confirm dialog failed to open. Still reachable from a stale
                    # per-table Reload or "Retry unfinished tables" (rendered before CDC
                    # started; both are disabled while it is live) when the probed table
                    # already holds target rows.
                    if radio_tables and not cdc_live_now:
                        reload_choice.on_value_change(_sync_label)
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
        preselect=True,
        checklist_hint=None,
        cdc_additions=frozenset(),
        keeps_watermark=False,
    ) -> None:
        """Probe which target tables hold data, then open the confirm dialog.

        Shared by Start, Re-run-all, Retry-failed and per-table Reload. Runs the
        read-only non-empty probe off the event loop (over ``probe_tables`` -- the
        tables this action will touch, defaulting to the full selection for
        Start/Re-run), records the result (drives the Drop-vs-Append choice + the
        run's replace set), then opens the confirm dialog in the top-level client
        context. ``on_confirm`` is the action to run on confirm (defaults to
        ``start_full_load``); ``title`` is the dialog heading. ``table_reasons``
        (Retry-failed / scoped Reload) turns the dialog into a per-table checklist and
        makes ``on_confirm`` receive the checked subset; ``preselect=False`` starts that
        checklist empty and ``checklist_hint`` replaces its instruction line.
        ``cdc_additions`` (G17: the Full load + CDC tables the watermark does not cover yet,
        as the step rendered them) -- each the probe finds holding rows is forced to Drop &
        reload (an empty one appends).
        ``keeps_watermark`` (G25) is the action kind the dialog states the source writes
        for: False -- the default action, ``start_full_load``, takes a new snapshot (Start,
        Re-run all tables) -- and True for an ``on_confirm`` that runs the retry engine
        under the original watermark (Retry unfinished tables, a per-table Reload, Reload
        specific tables). A new snapshot is refused here, before any probe, while the
        new-start-point gate holds (G25) -- the page header's Run reaches this dialog too.
        """
        if _confirm_busy["value"]:
            return  # a probe is already in flight -> ignore the extra click
        if _refuse_new_start_point(keeps_watermark):
            refresh()  # the step then shows the disabled action and why
            return
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
            migration_state.clear_exclusion_forced_replace()
            target_config = getattr(session, "target_config", None)
            # The column-exclusion overlay: its own ledger read, off the loop and BEFORE the
            # target probe, so a record for this target marks the session as using the
            # ledger even when the probe then fails (the engine then checks it).
            view = (
                exclusion_overlay_view(action_tables)
                if exclusion_overlay_view is not None and action_tables
                else None
            )
            ledger_configured = False
            ledger_error = False
            ledger_tables: dict = {}
            if target_config is not None and action_tables and overlay_ledger_read is not None:
                from nicegui import run

                try:
                    ledger = await run.io_bound(overlay_ledger_read)
                    ledger_configured = ledger is not None
                except Exception:  # noqa: BLE001 - unknown is not "no record"
                    ledger, ledger_configured = None, True
                status = getattr(ledger, "status", None) if ledger is not None else (
                    "unknown" if ledger_configured else None
                )
                if ledger_configured:
                    ledger_error = status not in ("present", "absent")
                    if status == "present":
                        ledger_tables = dict(getattr(ledger, "tables", {}) or {})
                        if ledger_tables:
                            migration_state.mark_overlay_ledger_in_use()
            # An unreadable record only matters to a session that uses the ledger (the
            # engine refuses exactly those runs); any other session keeps its dialog.
            overlay_active = ledger_configured and view is not None and (
                bool(view.overlay_by_table) or bool(ledger_tables)
                or bool(view.recreated_unfinished)
                or (ledger_error and bool(getattr(migration_state, "overlay_ledger_in_use", False)))
            )
            read_tables = sorted(
                t for t in action_tables
                if overlay_active and not ledger_error
                and (t in view.overlay_by_table or t in ledger_tables)
            )
            facts: Optional[dict] = {}
            probe_failed = False
            if target_config is not None and action_tables:
                from nicegui import run

                def _probe() -> tuple[frozenset[str], dict, frozenset[str], dict]:
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
                    # Which of the selected tables EXIST on the target -- distinct from
                    # `found` (rows), so the dialog can warn when a table is genuinely
                    # missing (deleted, or Schema Conversion not applied) and an append
                    # would fail it. See tables_present.
                    present = frozenset(
                        tables_present(names, connection_factory=connector.connect)
                    )
                    # The overlay facts, ONE read over the overlay columns and every
                    # recorded set column -- only when an action table needs them.
                    overlay_facts = (
                        target_overlay_facts(
                            read_tables,
                            {
                                t: sorted(
                                    set(view.overlay_by_table.get(t, ()))
                                    | set(getattr(ledger_tables.get(t), "set_columns", ()) or ())
                                )
                                for t in read_tables
                            },
                            connection_factory=connector.connect,
                        )
                        if read_tables
                        else {}
                    )
                    return found, keys, present, overlay_facts

                try:
                    found, keys, present, facts = await run.io_bound(_probe)
                    migration_state.set_tables_with_data(found)
                    migration_state.set_target_primary_keys(keys)
                    migration_state.set_target_tables_present(present)
                except Exception:  # noqa: BLE001 - on probe failure, warn-less confirm
                    migration_state.set_tables_with_data(frozenset())
                    # Leave the key map EMPTY, not partially filled: an unprobed table
                    # is treated as unknown, so the disclosure stays conservative.
                    migration_state.set_target_primary_keys({})
                    # None (not empty): "could not read the target", so the dialog must
                    # NOT warn that every table is missing -- stay conservative.
                    migration_state.set_target_tables_present(None)
                    facts, probe_failed = None, True
            overlay_info = None
            if view is not None:
                # The overlay this dialog discloses; the run refuses to start with another.
                migration_state.set_overlay_disclosed(view.overlay_by_table)
            # G17: a table selected after a Full load + CDC Full Load loads only into an
            # EMPTY table: rows already there may predate the changes CDC captures for it
            # (the load inserts ON CONFLICT DO NOTHING, so they would stay), so it is
            # dropped and reloaded. An empty one appends (no needless DROP); the engine
            # re-checks before loading.
            addition_forced = {
                t: _ADDITION_FORCED_REASON
                for t in action_tables
                if t in (cdc_additions or ()) and t in migration_state.tables_with_data
            }
            if overlay_active:
                outcome = None
                if not ledger_error:
                    # With the probe failed every overlay / recorded table is undecided,
                    # but a table the tool recreated and did not finish is still forced
                    # (reloading it needs no facts).
                    outcome = exclusion_forced_replace(
                        action_tables,
                        overlay_by_table=view.overlay_by_table,
                        excluded_by_table=view.excluded_by_table,
                        source_columns_for=view.source_columns_for,
                        facts=(facts or {}) if not probe_failed else {},
                        ledger_tables=ledger_tables,
                        cdc_coexisting=cdc_streaming_started(migration_state, job_manager),
                        recreated_unfinished=view.recreated_unfinished,
                        index_ddls_for=view.index_ddls_for,
                    )
                    migration_state.set_exclusion_forced_replace(
                        {**dict(outcome.forced), **addition_forced}
                    )
                overlay_info = {
                    "outcome": outcome, "ledger_error": ledger_error,
                    "probe_failed": probe_failed, "facts": facts, "view": view,
                }
            elif addition_forced:
                migration_state.set_exclusion_forced_replace(addition_forced)

            async def _retry() -> None:
                await _open_full_load_confirm(
                    None, probe_tables=action_tables, on_confirm=confirm_action,
                    title=title, table_reasons=table_reasons, preselect=preselect,
                    checklist_hint=checklist_hint, cdc_additions=cdc_additions,
                    keeps_watermark=keeps_watermark,
                )

            _open_confirm_dialog_now(
                action_tables=action_tables,
                on_confirm=confirm_action,
                title=title,
                keeps_watermark=keeps_watermark,
                table_reasons=table_reasons,
                preselect=preselect,
                checklist_hint=checklist_hint,
                overlay=overlay_info,
                on_retry=_retry,
                addition_forced=addition_forced,
                cdc_additions=cdc_additions,
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

    # The page header's Run asked for the Start dialog (exclusions or ledger records are
    # in play, so the run must disclose its target changes first): open it once.
    if getattr(migration_state, "pending_full_load_confirm", False):
        migration_state.pending_full_load_confirm = False
        # Created in the CLIENT context, not this content slot: a refresh of the content
        # while the probe runs must not delete the timer and cancel the open.
        from nicegui import context as _pending_ctx

        with _pending_ctx.client:  # type: ignore[attr-defined]
            ui.timer(0.1, _open_full_load_confirm, once=True)

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
            cdc_additions=frozenset(_pending),
            keeps_watermark=True,  # reload_table runs the retry engine
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
        record = _current_job(job_manager, migration_state.job_id)
        if record is None:
            return
        current = _scoped(record)
        _tables = _view_tables(record, current)
        # A set for the per-record membership test below (the error log can hold a record
        # per quarantined row, across every table).
        _table_set = None if _tables is None else set(_tables)
        running = current.status in ("PENDING", "RUNNING")
        if running:
            ui.label(full_load_progress_caption(current)).classes(
                "text-sm text-gray-500"
            )
        # Full Load records ONLY: CDC writes under this same job_id (cdc_error_log_key),
        # so an unfiltered read counts dead-lettered rows as Full Load failures.
        # ..._for() resolves the retry lineage: a retry runs under a NEW job id, so
        # reading only that id blanks the reason for every table it did not re-run.
        summary = full_load_error_summary_for(migration_state.error_log, current)
        rows = build_full_load_table_rows(
            current,
            summary,
            full_load_latest_messages_for(migration_state.error_log, current),
        )
        # G129: no reload while CDC streams (the sink's deletes would be written back);
        # G132: while a CDC operation runs, the reason is the wait for it.
        _reload_hold = cdc_load_hold(migration_state, job_manager, keeps_watermark=True)
        _render_full_load_progress(
            ui,
            current,
            rows,
            reload_table=reload_table,
            reload_confirm=_open_reload_confirm,
            reload_block=_reload_hold[0] if _reload_hold is not None else None,
            lob_candidates_for=lob_candidates_for,
            exclusion_route=exclusion_route,
            exclusion_route_block=exclusion_route_block,
            cdc_uncaptured=frozenset(_pending),
            source_type=session_source_type(session),
            # Reload/Retry need a LIVE source+target (the source password is not restored
            # after a session restore, Property 7). connection_ready() reflects "verified
            # this session" -- unlike has_source(), which is True whenever the config is
            # merely present (now the case after the v0.1.414 source-endpoint restore).
            connections_ready=session.connection_ready(),
            quarantine_only=_incomplete_is_quarantine_only(
                current, migration_state.error_log, _tables
            ),
            # EVERY dropped row, not one per table: ``latest_messages()`` keeps only the
            # last message per table, so a table that dropped 3 rows listed exactly 1 --
            # and the count above it disagreed with the list below. Only the tables the
            # view shows: a table no longer selected is never offered a Reload.
            quarantine_records=[
                (str(rec.table), str(rec.message))
                for rec in full_load_error_records_for(
                    migration_state.error_log, current
                )
                if _table_set is None or str(rec.table) in _table_set
            ],
            ai_error_opener=ai_error_opener,
            page_state=_progress_page,
        )
        # The error-log download belongs with the DETAIL it serializes, not under the
        # accept button: sitting immediately below "Accept quarantined rows & continue" it
        # read as that decision's secondary option, when it is just a way to take the same
        # per-row information away with you.
        _render_error_log(ui, migration_state, record)
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
                current, migration_state.error_log, _tables
            ),
            terminal=current.status in ("DONE", "FAILED", "CANCELLED"),
            quarantine_accepted=migration_state.accept_quarantined_rows,
            accept_quarantine_and_continue=accept_quarantine_and_continue,
        )
        # Referential integrity is a SEPARATE, explicit step now -- and the load reports
        # complete without it, so this must state plainly that it is still outstanding.
        # Resolved HERE, per refresh, so the card tracks the FK job instead of replaying the
        # state the page was first built with.
        fk_running = bool(_call_or(foreign_keys_running, False))
        _render_foreign_key_action(
            ui,
            terminal=current.status in ("DONE", "FAILED", "CANCELLED"),
            pending=int(_call_or(foreign_keys_pending, 0) or 0),
            applied=_call_or(foreign_keys_applied, None),
            apply_foreign_keys=apply_foreign_keys,
            connections_ready=session.connection_ready(),
            running=fk_running,
            progress=_call_or(foreign_keys_progress, None),
            cancel_foreign_keys=cancel_foreign_keys,
            scope_changed=bool(getattr(migration_state, "fk_result_scope_changed", False)),
        )
        # The FK pass is its OWN job and runs entirely AFTER the load is terminal, so gating
        # the poll on the load alone left nothing to re-arm it: the card could not advance
        # its counter and could not replace the spinner with the result. Poll while EITHER is
        # live.
        if running or fk_running:
            # Re-arm a single-shot poll: it fires once, refreshes only this live
            # region (not the whole page), and the refresh renders a fresh timer.
            # once=True avoids the "parent slot deleted" crash a repeating timer
            # hits when its slot is rebuilt.
            ui.timer(_POLL_INTERVAL_SECONDS, _poll_live, once=True)

    def _poll_scoped(current) -> bool:
        """The terminal poll for a set changed after the load (Full load only, G12; Full
        load + CDC narrowed or grown before CDC starts, G16/G17): the CURRENT set decides
        (:func:`full_load_step_for_scope`), e.g. a DONE retry with a selected table still
        not loaded is not a DONE step, and a failed retry whose failures lie only in
        unticked tables is. Writes the step,
        refreshes and returns True; False (an unchanged set) leaves the poll's own mapping
        to run."""
        view = _scoped(current)
        if view is current:
            return False
        derived = full_load_step_for_scope(
            current,
            view,
            accepted=bool(getattr(migration_state, "accept_quarantined_rows", False)),
            error_log=migration_state.error_log,
        )
        if derived is None:
            return False
        if derived is StepStatus.DONE:
            migration_state.clear_outputs()  # any failure lies in unselected tables
            if current.status != "DONE":
                # This run's finalize skipped the identity sync (it was incomplete).
                submit_scope_identity_sync(
                    job_manager, session, view, extras=identity_extras
                )
        elif current.status != "DONE":
            migration_state.set_error(
                job_manager.get_error(migration_state.job_id) or "Data migration failed."
            )
        # A DONE job with a selected table not loaded yet records no error: nothing
        # failed, and the scope notice says what is left.
        session.set_workflow(  # type: ignore[attr-defined]
            with_status(
                session.workflow,  # type: ignore[attr-defined]
                WorkflowStep.FULL_LOAD,
                derived,
            )
        )
        refresh()
        return True

    def _poll_live() -> None:
        """Poll the job: refresh only the live region while running; on a terminal
        state, finalize the step status and do one full refresh (to flip buttons
        and reveal retry)."""
        try:
            current = job_manager.get_status(migration_state.job_id)
        except JobNotFoundError:
            # A one-shot chain has no next tick of its own, so a bare return killed the
            # poll for the rest of the session: the load card stayed "In progress" with a
            # spinner and the foreign-key card froze on its counter, with nothing left to
            # advance either. If the FK pass is still running, refresh the live region --
            # that render re-arms the timer. Otherwise this is terminal: reconcile the step
            # off what we have and do one full refresh (which correctly installs no timer).
            if bool(_call_or(foreign_keys_running, False)):
                _live_detail.refresh()
                return
            # G25 (decision A): with the record gone there is no Retry or Reload, so running
            # the load again takes a new start point -- refused on a Full load + CDC
            # migration whose CDC stack has streamed until it is deployed again.
            migration_state.set_error(
                "The Full Load job record was lost (the app restarted, or the job was "
                "reaped). Run the Full Load again (on a Full load + CDC migration whose CDC "
                "stack has streamed, Delete CDC infrastructure and deploy it again first: a "
                "new start point needs new CDC topics), or check Validation — the rows "
                "already written are still on the target."
            )
            session.set_workflow(  # type: ignore[attr-defined]
                with_status(
                    session.workflow,  # type: ignore[attr-defined]
                    WorkflowStep.DATA_MIGRATION,
                    StepStatus.FAILED,
                )
            )
            refresh()
            return
        mapped = job_status_to_step_status(current.status)
        if mapped is None:
            _live_detail.refresh()
            return
        # The load is terminal -- but the foreign-key action is a SEPARATE job that only
        # STARTS once it is, so ending the poll here left its card frozen on "0 of N done"
        # with a spinner that never gave way to the result. While that job runs, refresh
        # just the live region (which re-arms the next tick); the single full refresh below
        # then happens on the first tick after it settles.
        if bool(_call_or(foreign_keys_running, False)):
            _live_detail.refresh()
            return
        # An ACCEPTED quarantine gap makes this run complete, and the job's own status can
        # never say so: the job is terminally FAILED (its rows really were dropped) and
        # "Accept quarantined rows & continue" is an operator decision recorded on the
        # SESSION. This poll re-armed itself on every refresh and wrote the job status back
        # unconditionally, so the acceptance was undone within a tick: the panel showed
        # "Full Load complete -- with an accepted gap" while the sidebar said Data
        # Migration FAILED and Validation stayed locked -- a dead end, because re-running
        # drops the same rows again. Honour the decision here, exactly as the finaliser and
        # the panel already do. Still gated on quarantine-ONLY, so a real retryable failure
        # is never waved through by a stale flag.
        # G12/G16: a changed Full load only set, or a narrowed Full load + CDC set,
        # decides itself.
        if _poll_scoped(current):
            return
        if (
            mapped is StepStatus.FAILED
            and getattr(migration_state, "accept_quarantined_rows", False)
            and _incomplete_is_quarantine_only(current, migration_state.error_log)
        ):
            mapped = StepStatus.DONE
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

    # The scope notice (G12), right under the workloads it describes and above the Start /
    # recovery actions: which selected tables are not loaded yet, which are left as is.
    # A run guard (the prerequisite checks a newly selected table never had) is named first:
    # Retry unfinished tables refuses the newly selected tables until it clears.
    _notice = scope_notice(
        _record_job,
        job,
        blocked_by=guard_reason if _added else None,
        cdc=getattr(migration_state, "migration_type", None)
        is MigrationType.FULL_LOAD_AND_CDC,
        pending=_pending if _pending else None,
        not_addable=_not_addable,
        pg=_pg_publication,
    )
    if _notice is not None and status is not StepStatus.IN_PROGRESS:
        render_notice(ui, tone=_notice[0], header=_notice[1], body=_notice[2])
    # Before the load: the selection is more than one CDC pipeline captures, or may be (its
    # header says which). Not a hold -- the load may take it all and the set be narrowed
    # before CDC starts.
    if pre_load_warning and job is None and status is not StepStatus.IN_PROGRESS:
        render_notice(
            ui, tone="warning", header=pre_load_warning[0], body=pre_load_warning[1]
        )

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
            _start_hold = cdc_load_hold(migration_state, job_manager, keeps_watermark=False)
            if not selected_names:
                start_btn.disable()
                start_btn.tooltip(
                    "Select at least one table in the object browser above."
                )
            elif guard_reason:
                start_btn.disable()
                start_btn.tooltip(guard_reason)
            elif _start_hold is not None:
                # CDC is live: starting/re-running Full Load would collide with the
                # running stream, so DISABLE (grey out) the button rather than let it
                # look clickable. The tooltip + hint name the way on (Stop CDC, then new
                # CDC topics -- G25), so it is a clear "not now", not a dead end. G132:
                # while a Start CDC, Stop CDC or Delete runs, they say to wait for it (info:
                # an expected wait).
                start_btn.disable()
                start_btn.tooltip(_start_hold[0])
                if _start_hold[1] is None:
                    inline_hint(ui, _CDC_LIVE_START_HINT, tone="warning")
                else:
                    inline_hint(ui, _start_hold[0], tone="info")
            elif _start_point_block is not None:
                # G25: a new start point on a cdc-stack that has streamed (or not read yet).
                # An info notice: the stack's state is normal, the route is the operator's.
                start_btn.disable()
                start_btn.tooltip(_start_point_block[2])
                render_notice(
                    ui, tone="info", header=_start_point_block[0], body=_start_point_block[1]
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
        _render_watermark(
            ui,
            _record_job,
            session_source_type(session),
        )

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
                job, migration_state.error_log, _view_tables(_record_job, job)
            )
            # After a Full load only set change (G12) or a Full load + CDC narrowing (G16)
            # the job's error can belong to a table no longer selected: when the current
            # set is complete, no red box.
            _scope_clears_error = job is not _record_job and full_load_step_for_scope(
                _record_job,
                job,
                accepted=bool(getattr(migration_state, "accept_quarantined_rows", False)),
                error_log=migration_state.error_log,
            ) is StepStatus.DONE
            if job_error and not _quarantine_only_error and not _scope_clears_error:
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
                # Recovery needs a live, VERIFIED source AND target. After a session
                # restore the connections (and the in-memory source password) are NOT
                # restored (Property 7), so both recovery actions would just fail at
                # connect. Gate on the VERIFIED flags, not has_source()/has_target():
                # since v0.1.414 the restore re-hydrates the source config (so
                # has_source() is True) but leaves it UNVERIFIED with no password, which
                # made this gate stop firing. Verified == tested live this session.
                missing: list[str] = []
                if not session.source_verified:
                    missing.append("source")
                if not session.target_verified:
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
                _rerun_gated = False  # G25: disabled by the new-start-point gate
                # G129: why CDC holds a run that takes a new start point, and a Retry
                # (G132: worded as a wait while a CDC operation runs), or None.
                _rerun_hold = cdc_load_hold(migration_state, job_manager, keeps_watermark=False)
                _retry_hold = cdc_load_hold(migration_state, job_manager, keeps_watermark=True)
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
                    elif _start_point_block is not None:
                        # G25: a fresh snapshot is a new start point (the notice below the
                        # row says why); Retry beside it keeps the original one. While CDC
                        # is live, Stop CDC comes first, as on the Start button.
                        rerun_btn.disable()
                        rerun_btn.tooltip(
                            _rerun_hold[0] if _rerun_hold is not None
                            else _start_point_block[2]
                        )
                        _rerun_gated = True
                    elif _rerun_hold is not None:
                        # G129: a Start in flight on a stack that has not streamed yet passes
                        # the gate above; the CDC hold still comes first, as on the Start
                        # button.
                        rerun_btn.disable()
                        rerun_btn.tooltip(_rerun_hold[0])
                    else:
                        rerun_btn.tooltip(
                            "Re-runs ALL selected tables from a fresh snapshot (new "
                            "watermark). Use 'Retry unfinished tables' to resume only "
                            "the unfinished ones instead."
                        )
                    # Spacer pushes the recommended action to the right edge.
                    ui.space()

                    # Per-table reason (cause) for the retry checklist, so the user
                    # can see WHY each table is unfinished before deciding whether to
                    # retry it now. FAILED tables carry their error-log message; a
                    # still-PENDING table was never attempted (the run ended first),
                    # so it gets a plain "not yet loaded" note.
                    _failure_reasons = full_load_latest_messages_for(
                        migration_state.error_log, job
                    )
                    _pending_names = {
                        c.chunk_id for c in job.chunks if c.status == "PENDING"
                    }

                    _view_chunks = {c.chunk_id: c for c in job.chunks}
                    # Sets built once: _reason_for runs for every unfinished table, and a
                    # list scan per name was quadratic at a large table count.
                    _not_addable_set = set(_not_addable)
                    _pending_set = set(_pending)
                    _added_set = set(_added)

                    def _reason_for(name: str) -> str:
                        # G17: a Full load + CDC table the watermark does not cover yet --
                        # one Retry cannot add, or one whose load quarantined rows, says so
                        # before any older failure message.
                        if name in _not_addable_set:
                            return CDC_NOT_ADDABLE_REASON
                        if name in _pending_set and (
                            getattr(_view_chunks.get(name), "rows_quarantined", 0) or 0
                        ):
                            return SCOPE_ADDED_QUARANTINED_REASON
                        msg = _failure_reasons.get(name, "")
                        if msg:
                            return msg
                        if name in _added_set:
                            base = (
                                SCOPE_ADDED_REASON_FL_CDC
                                + (SCOPE_ADDED_REASON_PG_SUFFIX if _pg_publication else "")
                                if name in _pending_set
                                else SCOPE_ADDED_REASON
                            )
                            # With a run guard the Confirm refuses it: say so on its row.
                            return f"{base} {guard_reason}" if guard_reason else base
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
                            cdc_additions=frozenset(_pending),
                            keeps_watermark=True,  # the retry engine, original watermark
                        )

                    retry_btn = ui.button(
                        f"Retry unfinished tables ({len(failed)})",
                        on_click=_confirm_retry_failed,
                        icon="replay",
                    ).props("color=primary")
                    # Every unfinished table selected after the load: with a run guard the
                    # click is CERTAIN to be refused (the same guard runs at the click), so
                    # the button is disabled with it, as Re-run all tables is. The same for
                    # Full load + CDC tables a guarded Retry cannot add (G17): only Re-run
                    # all tables (a fresh start point) loads them.
                    _only_added = bool(_added) and set(failed) <= set(_added)
                    if connections_missing:
                        retry_btn.disable()
                        retry_btn.tooltip(reconnect_tip)
                    elif _retry_hold is not None:
                        # G129: a retry while the sink streams would write back the rows it
                        # deletes during the copy; the hint below the row names the route
                        # (G132: or the wait for the CDC operation in progress).
                        retry_btn.disable()
                        retry_btn.tooltip(_retry_hold[0])
                    elif _not_addable and set(failed) <= set(_not_addable):
                        retry_btn.disable()
                        retry_btn.tooltip(cdc_not_addable_refusal(_not_addable))
                    elif _only_added and guard_reason:
                        retry_btn.disable()
                        retry_btn.tooltip(guard_reason)
                    elif _added and guard_reason:
                        # Mixed: the click refuses the newly selected tables (with this
                        # reason); unticking them in the checklist retries the others.
                        retry_btn.tooltip(guard_reason)
                    elif _added:
                        retry_btn.tooltip(
                            f"Recommended: load only the {len(failed)} unfinished "
                            "table(s) (failed, not reached, or selected after the last "
                            f"run); the {done_count} loaded table(s) are kept as-is (no "
                            "re-load, no new snapshot)."
                        )
                    else:
                        retry_btn.tooltip(
                            f"Recommended: re-run Full Load for only the "
                            f"{len(failed)} failed table(s); the {done_count} "
                            "succeeded table(s) are kept as-is (no re-load, no new "
                            "snapshot)."
                        )
                if _retry_hold is not None and _retry_hold[1] is None:
                    inline_hint(ui, _CDC_LIVE_RETRY_HINT, tone="warning")
                elif _retry_hold is not None:
                    # G132: a Start CDC, Stop CDC or Delete in progress: an expected wait.
                    inline_hint(ui, _retry_hold[0], tone="info")
                if _rerun_gated:
                    render_notice(
                        ui, tone="info", header=_start_point_block[0],
                        body=_start_point_block[1],
                    )
            else:
                # Every table finished. Until now this state offered NO scoped action at
                # all: the per-table "Reload" button lives only on the quarantine card,
                # so a table that loaded CLEANLY but has to be re-loaded -- the normal
                # case after re-applying ONE table's schema on the Schema Conversion
                # step, which drops and recreates it EMPTY -- had no control. The table
                # picker is locked once a load has run (Full load only excepted, and Full
                # load + CDC until CDC first starts, G16/G17: there the set can change,
                # but re-ticking a loaded table does not reload it), so
                # the only escape was a full
                # "Re-run Full Load" over every table (a whole-source re-read) or Start
                # over (discarding Evaluation, the conversion edits and the CDC inputs).
                # Only SELECTED tables are offered (``job`` is the scoped view): a table
                # no longer selected is left as is on Aurora DSQL.
                #
                # The engine already supports the scoped run (the same retry path the
                # quarantine Reload uses, which keeps the ORIGINAL watermark so CDC still
                # converges over it), so this only adds the missing entry point:
                # the identical confirm checklist, starting EMPTY so nothing is reloaded
                # by accident.
                _loaded = sorted(
                    c.chunk_id for c in job.chunks if c.status == "DONE"
                )
                if _loaded and retry_tables is not None:
                    # getattr, not attribute access: this branch renders for ANY
                    # terminal job, including a UI double / a session-less render, and a
                    # missing flag must read as "not verified" (button disabled) rather
                    # than raising out of the whole step.
                    _rl_missing: list[str] = []
                    if not getattr(session, "source_verified", False):
                        _rl_missing.append("source")
                    if not getattr(session, "target_verified", False):
                        _rl_missing.append("target")

                    async def _confirm_scoped_reload(event: object = None) -> None:
                        await _open_full_load_confirm(
                            event,
                            probe_tables=list(_loaded),
                            on_confirm=retry_tables,
                            title="Reload selected tables?",
                            # A checklist needs the reasons map to render; every entry is
                            # blank here because there is no failure to explain -- the
                            # hint line below carries the meaning instead.
                            table_reasons={n: "" for n in _loaded},
                            preselect=False,
                            # G25 (rule D): on a stack that has streamed the next Start
                            # continues from where streaming stopped, and its sink's
                            # unapplied tail comes from the CDC topics.
                            checklist_hint=(
                                "Tick the tables to reload. Reloading re-reads those "
                                "tables from the source and leaves every other table "
                                "untouched; the run keeps its original watermark, so CDC "
                                "still converges over the reloaded tables (on a stack "
                                "that has streamed, the next Start continues from where "
                                "streaming stopped, gapless only while the CDC topics "
                                "still retain what the sink had not applied)."
                            ),
                            keeps_watermark=True,  # retry_tables, original watermark
                        )

                    _reload_hold = cdc_load_hold(
                        migration_state, job_manager, keeps_watermark=True
                    )
                    with ui.row().classes("items-center gap-2 w-full"):
                        _reload_btn = ui.button(
                            "Reload specific tables…",
                            on_click=_confirm_scoped_reload,
                            icon="replay",
                        ).props("color=primary outline")
                        if _rl_missing:
                            _reload_btn.disable()
                            _reload_btn.tooltip(
                                f"Reconnect the {' and '.join(_rl_missing)} connection "
                                "first (Connect step) — credentials are not restored "
                                "after a restart."
                            )
                        elif guard_reason:
                            _reload_btn.disable()
                            _reload_btn.tooltip(guard_reason)
                        elif _reload_hold is not None:
                            # Same rule as Retry (G129): a reload under a live sink writes
                            # back the rows the sink deletes while the table is copied (G132:
                            # while a CDC operation runs, the wait for it).
                            _reload_btn.disable()
                            _reload_btn.tooltip(_reload_hold[0])
                        else:
                            _reload_btn.tooltip(
                                "Re-load only the tables you pick (e.g. after "
                                "re-applying one table's schema, which recreates it "
                                "empty). Other tables are not touched."
                            )


# Quasar color names for each per-table Full Load state badge.
_LOAD_STATE_COLORS: dict[str, str] = {
    "PENDING": "grey",
    "IN_PROGRESS": "primary",
    "DONE": "positive",
    "FAILED": "negative",
}


def _quarantined_row_count(
    job: "MigrationJob", error_log: "ErrorLogStore", tables: "Optional[Sequence[str]]" = None
) -> int:
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
    by an older version (no per-chunk count) still works. ``tables`` (a scoped view of the
    record, G12) limits that fallback to those tables; ``None`` counts every record.
    """
    if job is None:
        return 0
    from_chunks = sum(
        getattr(chunk, "rows_quarantined", 0) or 0 for chunk in job.chunks
    )
    if from_chunks:
        return from_chunks
    wanted = None if tables is None else set(tables)
    return sum(
        1
        for record in error_log.records(job.job_id)
        if str(getattr(record, "message", "")).startswith("quarantined row pk[")
        and (wanted is None or str(getattr(record, "table", "")) in wanted)
    )


def _incomplete_is_quarantine_only(
    job: "MigrationJob", error_log: "ErrorLogStore", tables: "Optional[Sequence[str]]" = None
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
    return _quarantined_row_count(job, error_log, tables) > 0


def submit_scope_identity_sync(job_manager, session, view, *, extras=None) -> bool:
    """Submit the identity-sequence sync a failed or cancelled run skipped, once its step
    turns DONE because its unfinished work lies only in tables no longer selected (Full load
    only, G12; Full load + CDC before CDC starts, G16). Returns True when a job was
    submitted.

    ``_finalize_run`` syncs only a complete run, so -- exactly as for an accepted
    quarantine gap -- a GENERATED BY DEFAULT identity key would stay at its start value
    and the app's first insert after cut-over collide (23505). The same background sync
    (a target write, never on the render thread) over the LOADED tables of the current
    set: a table no longer selected is left as is. ``extras(names)`` -- the source floor
    every post-load sync takes (:func:`identity_sync_extras`) -- is read in the job.
    """
    target_config = getattr(session, "target_config", None)
    names = [c.chunk_id for c in getattr(view, "chunks", None) or () if c.status == "DONE"]
    if target_config is None or not names or job_manager is None:
        return False
    profile = getattr(session, "aws_profile", None)

    def _sync_after_scope(_handle) -> None:
        sync_identity_sequences_for_tables(
            target_config, names, aws_profile=profile,
            extras=extras(names) if extras is not None else None,
            heartbeat=getattr(_handle, "heartbeat", None),
        )

    job_manager.submit(_sync_after_scope)
    return True


def _view_tables(job, view) -> "Optional[list[str]]":
    """The table filter for a scoped view's error-log reads: ``None`` for the record."""
    if view is job or view is None:
        return None
    return [c.chunk_id for c in view.chunks]


def full_load_step_for_scope(
    job, scoped, *, accepted: bool, error_log
) -> "Optional[StepStatus]":
    """The Data Migration step status the CURRENT table set earns (G12, G16, G17). Pure.

    For an unchanged set (``scoped is job``) this is exactly the live poll's mapping: the
    job's status, with an accepted quarantine-only gap counting as DONE. After a set
    change in Full load only (or a Full load + CDC set changed after its load before CDC
    starts: tables dropped, G16, or added, G17 -- an added table the watermark does not
    cover yet is presented PENDING until its guarded load extends the coverage), the view
    decides instead: a selected table not loaded (unsettled) or an unaccepted quarantine in
    it is FAILED; a DONE job is DONE; a failed
    or cancelled job is DONE only when its unfinished work lies entirely in tables no
    longer selected -- a run-level failure or a cancel with every chunk loaded stays
    FAILED. ``None`` while the job runs.
    """
    if job is None or job.status in ("PENDING", "RUNNING"):
        return None
    mapped = job_status_to_step_status(job.status)
    if mapped is None:
        return None
    if (
        mapped is StepStatus.FAILED
        and accepted
        and _incomplete_is_quarantine_only(job, error_log)
    ):
        mapped = StepStatus.DONE
    if scoped is job or scoped is None:
        return mapped
    if unsettled_table_names(scoped):
        return StepStatus.FAILED
    if not accepted and _quarantined_row_count(
        scoped, error_log, _view_tables(job, scoped)
    ) > 0:
        return StepStatus.FAILED
    if job.status == "DONE":
        return StepStatus.DONE
    view = {c.chunk_id for c in scoped.chunks}
    if any(
        c.status != "DONE" or (getattr(c, "rows_quarantined", 0) or 0) > 0
        for c in job.chunks
        if c.chunk_id not in view
    ):
        return StepStatus.DONE
    return mapped


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


def _has_unique_index_failure(rows) -> bool:
    """True when any of ``rows``' index failures names a UNIQUE index.

    ``BatchedImporter._create_indexes`` prefixes such a failure with ``"UNIQUE "`` because
    it is the only place that still has the DDL. A unique index is a CONSTRAINT the source
    enforced, so its absence is a correctness gap on the target, not just a slower query.
    """
    for row in rows or ():
        message = str(getattr(row, "error_message", "") or "")
        if "UNIQUE " in message:
            return True
    return False


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
        f"{dropped:,} {noun} permanently dropped — Aurora DSQL rejected a value in "
        "them (e.g. one over its ~1 MiB per-column limit, or a type the target column "
        "cannot accept). The rest of this table loaded normally. See the quarantine "
        "panel below for each row's primary key and the target's own reason, then "
        "Reload this table once that is fixed, or use \"Exclude column & reload\" to "
        "migrate the table without that column."
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
    connections_ready: bool = True,
    # Why the per-table Reload cannot run now (G129: CDC is streaming), or None. The button is
    # then disabled with this tooltip, never hidden.
    reload_block: Optional[str] = None,
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
    # entry, so every row has to appear. The caller passes every Full Load record, so the
    # index notice reads its failed indexes from here too, one per index (G25).
    quarantine_records: "Sequence[tuple[str, str]]" = (),
    ai_error_opener=None,
    page_state=None,
    # "Exclude column & reload": the quarantine card's route into the exclusion popup
    # (routed by the lock at click time). None -> not offered.
    lob_candidates_for: Optional[Callable[[str], Sequence[tuple[str, str]]]] = None,
    exclusion_route: Optional[Callable[[str, str], None]] = None,
    exclusion_route_block: Optional[str] = None,
    # Full load + CDC (G17): the tables CDC does not capture yet (selected after the Full
    # Load). Change & reload never drops one -- its next load on this step applies the rule
    # -- so their "Exclude column & reload" tooltip says that instead.
    cdc_uncaptured: "frozenset[str]" = frozenset(),
    # The source engine, used ONLY to pre-tick the right column in the exclude dialog
    # (PG -> DSQL is the identity for these types, MySQL is not). Optional so a caller
    # without a session keeps the MySQL default.
    source_type: "Optional[SourceType]" = None,
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
              watermark. The source figure is a scan-free ESTIMATE from the source's
              catalog statistics, so it is commonly off by a few
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
    # state and imply data was lost). "index not confirmed" is an index Aurora DSQL answered
    # XX000 for, or whose connection dropped, while it was being created: it may exist after
    # all.
    index_prefix = ("index not created: ", "index not confirmed: ")
    real_failures = [
        r
        for r in failures
        if not str(r.error_message).startswith(quar_prefix)
        and not str(r.error_message).startswith(index_prefix)
    ]
    quarantined = [
        r for r in failures if str(r.error_message).startswith(quar_prefix)
    ]
    # One entry per failed INDEX (G25), not per table: a table records one failure per
    # CREATE that failed, in DDL order, and judged by its latest one an earlier UNIQUE index
    # vanished -- a missing (or unconfirmed) constraint followed by any other failed index
    # read "the data loaded completely". ``quarantine_records`` carries every record; a table
    # with none there (a caller that passes none) keeps its latest message. A table listed
    # among the failures is not "Done", so it gets no entry here.
    _failed_tables = {r.table for r in real_failures}
    _index_records: dict[str, list[str]] = {}
    for _record_table, _record_message in quarantine_records or ():
        if str(_record_message).startswith(index_prefix):
            _index_records.setdefault(str(_record_table), []).append(str(_record_message))
    index_only = [
        dataclasses.replace(row, error_message=message)
        for row in rows
        if row.table not in _failed_tables
        for message in _index_records.get(row.table)
        or ([row.error_message] if str(row.error_message).startswith(index_prefix) else [])
    ]
    terminal = job.status in ("DONE", "FAILED", "CANCELLED")

    def _ai_btn(
        table_name: str,
        error_message: str,
        *,
        topic: str = "failure",
        seed: "Optional[str]" = None,
        tooltip: "Optional[str]" = None,
    ) -> None:
        # Per-table AI Assist: opens the chat drawer to explain THIS failure (or, for a
        # QUARANTINE, why rows were dropped) + the options. Shown enabled only when AI
        # Assist is on (an opener was threaded in); otherwise a disabled, discoverable
        # affordance points at the Connect screen -- mirroring Schema Conversion.
        if ai_error_opener is not None:
            ui.button(
                "AI Assist",
                on_click=lambda n=table_name, e=error_message, t=topic, s=seed: (
                    ai_error_opener(n, e, topic=t, seed=s)
                ),
            ).props(
                "flat dense no-caps size=sm color=indigo-6 icon=auto_awesome"
            ).tooltip(tooltip or "Ask AI why this table failed and how to fix it.")
        else:
            ui.button("AI Assist").props(
                "flat dense no-caps size=sm color=grey icon=auto_awesome"
            ).props("disable").tooltip(
                "Enable AI Assist on the Connect screen to diagnose this "
                "with AI."
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
    def _quar_actions(table_name: str, reason_text: str):
        # Quarantine rows get BOTH an AI Assist (why were they dropped + options) and
        # the per-table Reload. The AI chat is scoped to the quarantine reason so it is
        # not the generic "table failed" question -- these rows loaded, specific values
        # were dropped, and the useful advice is json-vs-text / chunk / accept-the-gap.
        def _render() -> None:
            _ai_btn(
                table_name,
                reason_text,
                topic="quarantine",
                seed=(
                    f"Rows in {table_name} were quarantined (permanently dropped) "
                    f'during Full Load: "{reason_text}". Why did that happen, and what '
                    "are my options — store the value differently (e.g. json vs text), "
                    "chunk it, or accept the gap?"
                ),
                tooltip="Ask AI why these rows were dropped and what your options are.",
            )
            _quar_exclude_reload(table_name, reason_text)()
            _quar_reload(table_name)()

        return _render

    def _quar_exclude_reload(table_name: str, reason_text: str):
        """The bundled "exclude the oversized column and reload this table" affordance.

        Sits with Reload because it answers the same question ("these rows were dropped,
        now what?") -- but where Reload re-reads the SAME columns (useful only if the
        source value was shrunk), this drops the column that cannot fit and reloads, which
        is the only in-tool way forward when the value is legitimately over 1 MiB. Without
        it the sole escape was Start over, discarding Evaluation, Schema Conversion and
        the CDC inputs to change one checkbox.
        """
        def _btn() -> None:
            if exclusion_route is None or not terminal:
                return
            columns = tuple(lob_candidates_for(table_name)) if lob_candidates_for else ()
            if not columns:
                return  # nothing excludable on this table -> do not offer a dead button
            label = "Exclude column & reload"
            props = "flat dense no-caps size=sm color=warning icon=filter_alt_off"
            if exclusion_route_block:
                # Never hide it: a silently absent control reads as a missing feature,
                # and the reason is the actionable part.
                ui.button(label).props(props + " disable").tooltip(exclusion_route_block)
                return
            ui.button(
                label,
                on_click=lambda _e=None, n=table_name, r=reason_text: exclusion_route(n, r),
            ).props(props).tooltip(
                "Open Exclude tables & columns with a rule for the column that cannot fit. "
                "CDC does not capture this table yet, so its Change & reload does not drop "
                "it: Save stores the rule, and the table's next load on this step (Retry "
                "unfinished tables) applies it."
                if table_name in cdc_uncaptured
                else "Open Exclude tables & columns with a rule for the column that cannot "
                "fit; its Change & reload drops and reloads this table so no row keeps a "
                "value loaded under the old column set."
            )

        return _btn

    def _quar_reload(table_name: str):
        def _btn() -> None:
            # Reload re-reads from the source and writes to the target, so it needs a
            # LIVE, verified connection -- specifically the in-memory source password,
            # which is NOT restored after a session restore (Property 7). Post-restore
            # the config is re-hydrated (so has_source() is True) but unverified with no
            # password, so a click would just fail at connect. Disable it with a
            # reconnect hint until both connections are re-verified this session.
            if not connections_ready:
                ui.button("Reload").props(
                    "flat dense no-caps size=sm color=primary icon=replay disable"
                ).tooltip(
                    "Reconnect first: re-verify the source and target on the Connect "
                    "step (credentials are not restored after a restart), then Reload."
                )
                return
            if reload_block and terminal and (
                reload_confirm is not None or reload_table is not None
            ):
                # G129: CDC is streaming -- disabled with the reason, never hidden.
                ui.button("Reload").props(
                    "flat dense no-caps size=sm color=primary icon=replay disable"
                ).tooltip(reload_block)
                return
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
        #
        # A UNIQUE index that failed to create is NOT merely a missing access path: it is a
        # missing CONSTRAINT, so the target now accepts duplicates the source forbids. "No
        # data was lost and you do not need to re-run" was true of the data and false of the
        # constraint, and a COMPOSITE_KEY table's post-load pass can contain ONLY such an
        # index. Split the notice, and raise the tone when one is unique -- cutting over onto
        # a table that no longer enforces uniqueness is not an FYI.
        #
        # An index whose CREATE ended with no confirmed outcome -- Aurora DSQL answered
        # XX000, or (G25) the connection dropped while it ran -- may exist after all (most
        # likely its CREATE landed), so it is "not confirmed", not "missing": when every entry
        # is such an index the notice says so instead of claiming a missing constraint, and
        # asks for a check rather than a create (the tone stays: it may truly be missing).
        # The "NOT created / a constraint is missing" wording is picked from the indexes KNOWN
        # missing only (G25): picked from every entry, a not-confirmed UNIQUE index (which
        # most likely exists) made a mix with a missing plain index claim a missing
        # constraint. A mix names both groups -- also on one table, as every entry is one
        # index -- and each entry's badge says which of the two it is.
        unsure = [r for r in index_only if is_unconfirmed_index_failure(r.error_message)]
        missing = [r for r in index_only if not is_unconfirmed_index_failure(r.error_message)]
        unique_missing = _has_unique_index_failure(missing)
        unique_unsure = _has_unique_index_failure(unsure)
        count = len(index_only)
        # "(N)" counts the entries below, one per index. It follows "Unique index" only when
        # every one is unique; otherwise the unique ones are counted apart (G25): counted
        # together, one unique and three plain indexes read "Unique index NOT created (4)".
        n_unique = sum(1 for r in index_only if _has_unique_index_failure([r]))
        cause = (
            "Aurora DSQL answered server unavailable (SQLSTATE XX000), or the connection "
            "dropped,"
        )
        if not missing and unique_unsure:
            header = (
                f"Unique index not confirmed ({count}) — the data loaded; check that "
                f"{'it exists' if count == 1 else 'they exist'} before cutting over"
                if n_unique == count
                else f"Indexes not confirmed ({count}, {n_unique} unique) — the data loaded; "
                f"check that the unique {'index exists' if n_unique == 1 else 'indexes exist'} "
                "before cutting over"
            )
            body = (
                f"Every row is on the target, so no data was lost. {cause} while these "
                "indexes were being created, so they may exist after all. At least one is "
                "a UNIQUE index, which the source enforced as a constraint: if it is "
                "missing, duplicates can be written until it exists. Check the table's "
                "indexes on the target and create any that are missing BEFORE cutting over."
            )
        elif not missing:
            header = f"Indexes not confirmed ({count}) — the data loaded completely"
            body = (
                "Every row is on the target, so no data was lost and you do not need to "
                f"re-run the load. {cause} while these indexes were being created, so they "
                "may exist after all: check the table's indexes on the target and create "
                "only the ones that are missing. Queries that depend on a missing index "
                "will be slower until it exists."
            )
        elif unsure:
            groups = f"({len(missing)}), not confirmed ({len(unsure)})"
            # The unique ones among the not-confirmed are counted (G25): "the unique index" and
            # "one of them is a UNIQUE index" for two of them sent the operator to check one
            # constraint, so the other could be cut over missing.
            n_unique_unsure = sum(1 for r in unsure if _has_unique_index_failure([r]))
            if unique_missing:
                header = (
                    f"Indexes NOT created {groups} — the data loaded, but a constraint is "
                    "missing"
                )
            elif unique_unsure:
                header = (
                    f"Indexes not created {groups} — the data loaded; check that the unique "
                    f"{'index exists' if n_unique_unsure == 1 else 'indexes exist'} before "
                    "cutting over"
                )
            else:
                header = f"Indexes not created {groups} — the data loaded completely"
            unique_unsure_note = (
                "one of them is a UNIQUE index"
                if n_unique_unsure == 1
                else f"{n_unique_unsure} of them are UNIQUE indexes"
            )
            body = (
                "Every row is on the target, so no data was lost. "
                + (
                    "But at least one index that was not created is a UNIQUE index, which "
                    "the source enforced as a constraint and the target now does not — "
                    "duplicates can be written until it exists, so create it BEFORE "
                    "cutting over. "
                    if unique_missing
                    else "Add the indexes that were not created later, or reduce the "
                    "table's index count. "
                )
                + "Aurora DSQL allows 24 indexes per table, including the primary key. "
                f"The ones not confirmed may exist after all: {cause} while they were "
                "being created. Check the table's indexes on the target and create only "
                "the ones that are missing"
                + (
                    f" — {unique_unsure_note}, so check BEFORE cutting over."
                    if unique_unsure
                    else "."
                )
            )
        elif unique_missing:
            header = (
                f"Unique index NOT created ({count})"
                if n_unique == count
                else f"Indexes NOT created ({count}, {n_unique} unique)"
            ) + " — the data loaded, but a constraint is missing"
            body = (
                "Every row is on the target, so no data was lost. But at least one of "
                "these is a UNIQUE index, which the source enforced as a constraint "
                "and the target now does not — duplicates can be written until it "
                "exists, so create it BEFORE cutting over. Aurora DSQL allows 24 "
                "indexes per table, including the primary key."
            )
        else:
            header = f"Indexes not created ({count}) — the data loaded completely"
            body = (
                "Every row is on the target; only these secondary indexes are "
                "missing, so no data was lost and you do not need to re-run the "
                "load. Add them later, or reduce the table's index count — Aurora "
                "DSQL allows 24 indexes per table, including the primary key. "
                "Queries that depend on a missing index will be slower until it "
                "exists."
            )
        with ui.column().classes("w-full gap-2"):
            render_notice(
                ui,
                tone="warning" if unique_missing or unique_unsure else "info",
                header=header,
                body=body,
            )
            for row in index_only:
                state = (
                    "not confirmed"
                    if is_unconfirmed_index_failure(row.error_message)
                    else "missing"
                )
                _failure_row(
                    table_name=f"{row.table} · Done — index {state}",
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
    # LAST RESORT: key on the DURABLE per-chunk count when no message survives. A table
    # can carry a dropped-row count with no reason text at all -- a restored session has
    # lost the in-memory error log entirely (see _quarantined_row_count). Keying this
    # panel only on records meant the whole card vanished, taking the recovery action
    # ("Exclude column & reload") with it for exactly the table that still needed it, so
    # Start over was the only escape left. The count is authoritative either way.
    if not quarantine_entries:
        quarantine_entries = [
            (r.table, "the target rejected a value in this row (the reason is no "
                      "longer in this session's error log)")
            for r in rows
            if (getattr(r, "rows_quarantined", 0) or 0) > 0
        ]
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
                reason_text = (
                    "; ".join(reasons) if reasons
                    else ("the target rejected a value in this row (the reason is no "
                          "longer in this session's error log)")
                )
                _quarantine_detail_row(
                    ui,
                    table=table_name,
                    primary_keys=pks,
                    reasons=reasons,
                    action=_quar_actions(table_name, reason_text),
                )

def _render_foreign_key_action(
    ui,
    *,
    terminal: bool,
    pending: int,
    applied: Optional[tuple],
    apply_foreign_keys=None,
    connections_ready: bool = False,
    running: bool = False,
    progress: Optional[tuple] = None,
    cancel_foreign_keys=None,
    scope_changed: bool = False,
) -> None:
    """Render the explicit "Apply foreign keys" action, and its outstanding state.

    ``scope_changed`` (``fk_result_scope_changed``): the previous result was voided by a
    table-set change (Full load only, G12), so the outstanding notice says to apply again
    for the changed set rather than that the keys were never applied.

    Foreign keys used to be applied as the last act of the load. The orphan pre-check that
    gates each one is O(child rows) -- measured 28.0 min serial / 7.9 min at 8-way for 15
    foreign keys over 15.5M child rows -- and it ran AFTER every row was already written, so
    it held the load's completion for minutes with nothing left to load. It is an operator
    action now, which is also how a CDC migration has always applied them (at cut over), so
    both migration types follow one flow.

    The risk that buys is a FORGOTTEN click: an unapplied constraint is invisible, and DSQL
    cannot tell us later (a failed async VALIDATE is unobservable -- live-verified). So the
    outstanding state is a prominent notice naming the count, not a quiet button.

    WARNING, not error: this sits directly under a GREEN "Full Load complete", and a red card
    there reads as "my load failed" -- a workshop participant read it exactly that way. The
    load DID succeed; foreign keys are the next required step, which is what `warning` means
    in this design system (a real but non-blocking issue), while `error` means the step
    itself failed.
    """
    if running:
        done, total = (tuple(progress or ()) + (0, 0))[:2]
        render_notice(
            ui, tone="info",
            header=(
                f"Applying foreign keys — {done} of {total} done"
                if total else "Applying foreign keys…"
            ),
            body=(
                "Each one is orphan-checked first, which reads the whole child table, so "
                "this can take several minutes on a large schema. You can leave this page; "
                "it keeps running."
            ),
        )
        with ui.row().classes("items-center gap-2 w-full"):
            ui.spinner(size="sm")
            if total:
                ui.label(f"{done}/{total}").classes("text-xs text-gray-500")
            if cancel_foreign_keys is not None:
                ui.button(
                    "Stop applying", on_click=lambda: cancel_foreign_keys()
                ).props("flat color=negative icon=stop")
        return
    if applied is not None:
        got_applied, got_skipped, got_failed = (list(applied) + [0, 0, 0])[:3]
        # A finished pass does NOT mean every foreign key was dealt with. "Stop applying"
        # breaks the engine's loop, so the foreign keys it never reached are in no bucket at
        # all: stopping after 2 of 6 returns (2, 0, 0). Reporting only the buckets therefore
        # announced a GREEN "2 applied -- referential integrity is in place on the target"
        # while four constraints did not exist, and withdrew the Apply button; since nothing
        # clears the result, that screen was the end of the road. `pending` is the run's
        # TOTAL, so whatever the buckets do not account for is still outstanding. The same
        # test catches a pass that DIED: _apply_foreign_keys swallows the exception and
        # returns (0, 0, 0), which used to render as a green "0 applied".
        settled = got_applied + got_skipped + got_failed
        # The denominator must not fail OPEN. `pending` is an ADVISORY count: its provider
        # swallows any error and returns 0, and it is 0 by design for a CDC run -- and with a
        # 0 total, `outstanding` clamps to 0 and the card prints the very green "referential
        # integrity is in place" this branch exists to prevent (and would read "2 of 0
        # applied"). The pass itself publishes a total on `progress`, so fall back to that.
        progress_total = int((tuple(progress or ()) + (0, 0))[1] or 0)
        total = pending or progress_total
        # max() still clamps: with NEITHER total available nothing is invented.
        outstanding = max(0, total - settled)
        tone = "success" if not (got_skipped or got_failed or outstanding) else "warning"
        # Decided once, used by both the copy and the guard below, so the card cannot promise
        # an action it does not render.
        can_apply = bool(outstanding and terminal and apply_foreign_keys is not None)
        detail = []
        if got_skipped or got_failed:
            detail.append(
                f"{got_skipped} skipped (orphan rows), {got_failed} failed. "
                "See the activity log for the per-constraint detail."
            )
        if outstanding:
            detail.append(
                f"{outstanding} foreign key(s) were NOT reached — the pass was stopped or "
                "ended early, so referential integrity is NOT in place for them."
            )
            detail.append(
                # Say what resuming costs: the pass re-checks every FK, so it is not a cheap
                # "finish the last few" -- and this render is the only place that warning can
                # appear, because the outstanding-state notice is suppressed here.
                "Apply them below to finish; re-applying one that already exists is a harmless "
                "no-op, but the pass re-checks EVERY foreign key and each check reads the whole "
                "child table, so resuming costs about as much as the first run."
                if can_apply else
                "Apply them from this step once the load is settled and both connections are "
                "verified."
            )
        if not detail:
            detail.append("Referential integrity is in place on the target.")
        render_notice(
            ui, tone=tone,
            header=(
                f"Foreign keys: {got_applied} of {total} applied"
                if outstanding else f"Foreign keys: {got_applied} applied"
            ),
            body=" ".join(detail),
        )
        if not can_apply:
            return
        # Fall through to the action so a stopped pass can be resumed. Withdrawing it left
        # the operator who clicked Stop with no way back other than Start over.
    elif not (terminal and pending and apply_foreign_keys is not None):
        return
    if applied is None and scope_changed:
        # The result was voided by a table-set change (Full load only, G12): the keys it
        # applied still exist, so the never-applied copy below would misstate the target.
        render_notice(
            ui, tone="warning",
            header="Foreign keys: apply again for the changed table set",
            body=(
                "The table set changed after foreign keys were applied. Apply again to "
                "cover the current set: keys already in place are kept (re-applying one is "
                "a no-op), but the pass re-checks every key, and each check reads the whole "
                "child table."
            ),
        )
    elif applied is None:
        render_notice(
            ui, tone="warning",
            header=f"{pending} foreign key(s) not yet applied",
            body=(
                "The load itself succeeded. Referential integrity is the next required step "
                "and is NOT in place until you apply them. Each one is orphan-checked "
                "first, which "
                "reads the whole child table, so this can take several minutes on a large "
                "schema — it is a separate step so it no longer holds up the load."
            ),
        )
    with ui.row().classes("items-center gap-2 w-full"):
        btn = ui.button(
            "Apply foreign keys",
            on_click=lambda: apply_foreign_keys(),
        ).props("color=primary icon=link")
        if not connections_ready:
            btn.props("disable")
            btn.tooltip(
                "Verify the source and target connections first — applying a foreign key "
                "reads the child table and issues DDL on the target."
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
                "catalog-statistics estimates, not exact) and drift when rows change "
                "during the load: "
                + "; ".join(notes)
                + ". This is expected — run Validation (Step 4) for an exact "
                "row-count/checksum check to confirm."
            ),
        )
        return

    # "N without a source count to compare" is NOT a problem: it is the Rows column's
    # "--", i.e. the pre-load estimate was unavailable for those tables. Mixed into the
    # same sentence as permanently-dropped rows it read as a second fault ("are those 4
    # broken too?"), which is half of why this banner looked like noise. Disclosed on its
    # own line, in its own tone.
    unknown_note = (
        f"{completeness.unknown} table(s) had no source count to compare (the \u201c--\u201d "
        "in the Rows column). Validation (Step 4) does the exact comparison."
        if completeness.unknown
        else ""
    )

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
            "The dropped rows are listed above with the target's own reason: fix that "
            "(reduce an oversized source value, or re-apply this table's Schema "
            "Conversion for a type mismatch) and Reload that table, use \"Exclude "
            "column & reload\" to migrate it without that column, or accept the gap to "
            "continue (Validation reports it)."
        )
    else:
        remedy = "Run Validation (Step 4) for a full row-count/checksum check."
    # Don't aggregate when there is nothing to aggregate. With ONE affected table and no
    # other problem, this banner said only what the panel above it had already said -- the
    # same count, the same table name, then "the dropped rows are listed above" -- and
    # restated the three remedies in prose while the same three sit right there as BUTTONS
    # (AI Assist / Exclude column & reload / Reload), with "Accept quarantined rows &
    # continue" immediately below. Two boxes, one fact. So the verdict collapses into the
    # per-table panel, and the aggregate returns as soon as it has something to add: a
    # SECOND affected table, a failure, or a real mismatch.
    quarantine_only = bool(completeness.quarantined_rows) and not (
        completeness.failed or _mismatched_only
    )
    single_table = len(completeness.quarantined_tables) <= 1
    if quarantine_only and single_table:
        row_noun = "row" if completeness.quarantined_rows == 1 else "rows"
        _render_notice(
            ui,
            tone="warning",
            header=(
                f"Full Load did not load every row — {completeness.quarantined_rows} "
                f"{row_noun} permanently dropped"
            ),
            # Only what the panel above cannot say: the CONSEQUENCE of doing nothing.
            # No table name, no count repeated per remedy, no remedy prose.
            body=(
                "The rows, their reason and the actions to fix or accept them are on the "
                "table's panel. Until the gap is closed or accepted, Validation (Step 4) "
                "reports the shortfall and cut over stays blocked."
            ),
        )
    else:
        _render_notice(
            ui,
            tone="warning",
            header="Full Load finished with issues",
            body="; ".join(problems) + ". " + remedy,
        )
    # AFTER the verdict, never instead of it: a benign "no count to compare" line must not
    # be the only thing on screen when rows were actually dropped.
    if unknown_note:
        _render_notice(
            ui,
            tone="info",
            header="Some tables had no pre-load count to compare against",
            body=unknown_note,
        )


def _render_error_log(ui, migration_state, job: MigrationJob) -> None:
    """Render the data-error summary and a download button when errors exist.

    The summary count equals the rows in the downloadable log (Property 15); the
    button is hidden when there are no errors.
    """
    job_id = job.job_id
    # Full Load records only -- CDC shares this key, and its dead-lettered rows belong
    # to the CDC panel's own download (see full_load_error_records).
    records = full_load_error_records_for(migration_state.error_log, job)
    summary = full_load_error_summary_for(migration_state.error_log, job)
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
                # JSON, matching the CDC button beside it on this same step: two adjacent
                # downloads in two different formats is a gratuitous difference, and NDJSON
                # is not valid JSON so `json.load` fails on it.
                payload = migration_state.error_log.render_json(
                    records, source="full-load", stack=str(job_id)
                )
                ui.download.content(  # type: ignore[attr-defined]
                    payload, f"error_log_{job_id}.json", "application/json"
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
