# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The "Exclude tables & columns" popup and its button row under the table tree (P3).

ONE home for every migration type: the row sits in the object-browser panel right under
"Tables to migrate" and opens a rules-only popup. The rules are text
(``core.exclusion_rules``); what a Save stores is the RESOLVED set (the session's excluded
columns, the TableSelection and the provenance), through
``DataMigrationState.set_column_exclusions`` only.

The module takes ONE context object built by the screen (state, job manager, session, the
live inputs and the page refresh), so it never imports back from the package or the CDC
UI. ``ui`` is passed in, like every renderer here, so tests drive it with a double.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Optional, Sequence

from dsql_migrator.core.models import SourceType
from dsql_migrator.core.exclusion_rules import (
    LINE_NO_EFFECT,
    LINE_ONLY_BLOCKED,
    LINE_ONLY_UNSELECTED,
    split_rule_comment,
    synthesis_gaps,
)
from dsql_migrator.ui.data_migration._models import (
    SCROLL_TO_MIGRATION_TYPE_JS,
    capture_ambiguity_warning,
    capture_exclusion_mismatch,
    cdc_capture_exclusions,
    classify_restored_exclusions,
    exclusion_rules_text_for,
    exclusion_drift,
    exclusion_row_summary,
    lob_exclusion_candidates,
    open_lob_candidates,
    overlay_change_summary,
    popup_open_text,
    popup_resolution,
    scope_lob_candidates,
    session_source_type,
    stored_exclusion_block_reason,
    stored_exclusion_issues,
)
from dsql_migrator.ui.design import CODE_SURFACE_CLASSES, EXPANSION_PANEL_CLASSES, inline_hint, render_notice

TITLE = "Exclude tables & columns"
BUTTON_LABEL = "Exclude tables & columns..."
HELP_TEXT = (
    'One rule per line. "*" matches any characters within one name part. If an include and '
    "an exclude match the same item, include wins (unlike AWS DMS selection rules)."
)
HELP_TOOLTIP = (
    "exclude table shop.audit_*  -- leave tables out of this migration\n"
    "exclude shop.product_media.content  -- migrate a column as NULL\n"
    "exclude shop.*.password_hash  /  exclude *.*.ssn\n"
    "exclude type:longtext,longblob  -- by source data type\n"
    "include shop.pages.body  -- the exception: include wins\n"
    'Quote a name that has spaces, ".", "*" or "#": "my schema"."order.items".c\n'
    "# starts a comment."
)
# The editor grows with its lines, capped at 60vh (local: ui/design.py stays unchanged).
_EDITOR_CSS = (
    ".exclusion-rules-editor .cm-editor{max-height:60vh;min-height:6rem}"
    ".exclusion-rules-editor .cm-scroller{overflow:auto}"
)
_DEBOUNCE_SECONDS = 0.3
_SHOWN = 5
_PAGE_SIZE = 25
REVIEW_FILTERS = ("All", "Excluded", "Blocked", "Warning")
_STATUS = {"unselected": "Unselected", "excluded": "Excluded", "blocked": "Blocked",
           "kept": "Kept"}
HONESTY_FOOTER = (
    "Not highlighted, but can still exceed limits: MySQL JSON and spatial columns; "
    "PostgreSQL arrays, xml, tsvector and geometric types; large varchar(n)/char(n) holding "
    "multi-byte text. A row over 2 MiB is not checked. You can still exclude them with a rule."
)
QUARANTINE_CONSEQUENCE = (
    "If loaded, each such row is quarantined (kept out of Aurora DSQL); past 1,000 "
    "quarantined rows the table fails. Excluding the column keeps the rows."
)
# What the SOURCE type can hold -- always worded as a ceiling, never a measurement.
_TYPE_ALLOWS = {
    SourceType.MYSQL: {"mediumtext": "16 MiB", "mediumblob": "16 MiB", "longtext": "4 GiB",
                       "longblob": "4 GiB"},
    SourceType.POSTGRES: {"text": "about 1 GB", "bytea": "about 1 GB", "json": "about 1 GB",
                          "character varying": "about 1 GB", "varchar": "about 1 GB",
                          "jsonb": "about 256 MiB"},
}


@dataclass
class ExclusionDialogContext:
    """What the row and the popup need, resolved LIVE by the screen.

    ``inputs_fn`` is the screen's live :class:`ExclusionInputs` resolver; ``lock_reason_fn``
    the table picker's lock reason (E3); ``capture_tables_fn(selected')`` the tables CDC
    would capture (CDC types only, else ``None``); ``refresh`` the page refresh (it also
    persists the session).
    """

    migration_state: object
    job_manager: object
    session: object
    inventory_fn: Callable[[], object]
    inputs_fn: Callable[[], object]
    refresh: Callable[[], None]
    lock_reason_fn: Callable[[], Optional[str]] = lambda: None
    capture_tables_fn: Optional[Callable[[Sequence[str]], Sequence[object]]] = None
    # Why the migration type cannot change now (the type selector's own lock), or None.
    type_lock_reason_fn: Callable[[], Optional[str]] = lambda: None
    extra: dict = field(default_factory=dict)


def _lock_state(ctx: ExclusionDialogContext):
    from dsql_migrator.ui.data_migration._cdc_monitoring import exclusion_lock_state

    return exclusion_lock_state(ctx.migration_state, ctx.job_manager)


def _resolve(ctx: ExclusionDialogContext, inventory, inputs, text: str, *, slot: str):
    return popup_resolution(
        ctx.migration_state,
        inventory,
        text,
        inputs=inputs,
        source_type=session_source_type(ctx.session),
        selection_lock_reason=ctx.lock_reason_fn(),
        capture_tables=ctx.capture_tables_fn,
        memo_slot=slot,
    )


def _names(items: Sequence[str]) -> str:
    shown = ", ".join(items[:_SHOWN])
    return shown if len(items) <= _SHOWN else f"{shown} and {len(items) - _SHOWN} more"


# --- the button row ------------------------------------------------------------------


def render_exclusion_button_row(ui, ctx: ExclusionDialogContext):
    """The row under the tree: the button, the summary and the LOB line. A LOCAL
    refreshable (returned), so the tree's tick handlers refresh it without refresh_all;
    nothing is drawn when no table is migratable."""

    @ui.refreshable
    def row() -> None:
        state = ctx.migration_state
        inventory = ctx.inventory_fn()
        inputs = ctx.inputs_fn()
        if inventory is None or inputs is None or not getattr(inputs, "migratable", None):
            return
        lock = _lock_state(ctx)
        drift = None
        saved = state.exclusion_rules_text()
        if saved is not None and not lock.locked:
            drift = exclusion_drift(state, _resolve(ctx, inventory, inputs, saved, slot="row"))
        issues = stored_exclusion_issues(
            state, inventory, committed=lock.locked, selected_tables=inputs.selected
        )
        block = stored_exclusion_block_reason(issues, where=f"in {TITLE}")
        from dsql_migrator.core.column_exclusion import IssueSeverity

        errors = sum(1 for i in issues if i.severity is IssueSeverity.ERROR)
        lob = open_lob_candidates(
            scope_lob_candidates(
                lob_exclusion_candidates(inventory, source_type=session_source_type(ctx.session)),
                selected_tables=inputs.selected,
                stored_selection=state.selection.selected_tables,
            ),
            state.lob_exclusions(),
        )
        over = {k for k, (verdict, _at) in state.value_sizes().items() if verdict == "over"}
        summary = exclusion_row_summary(
            state, locked=lock.locked, drift=drift, block_reason=block,
            blocking_count=errors, lob_candidates=lob, measured_over=over,
            migratable=inputs.migratable,
        )
        with ui.row().classes("items-center gap-3 w-full"):
            ui.button(
                BUTTON_LABEL, on_click=lambda: open_exclusion_dialog(ui, ctx)
            ).props("outline no-caps color=primary icon=filter_alt")
            if summary.variant != "blocking":
                text = summary.text
                tone = "warning" if summary.variant == "drift" else "neutral"
                inline_hint(ui, text, tone=tone)
        if summary.variant == "blocking":
            render_notice(ui, tone="error", header=summary.text, body=block or "")
            ui.button("Review", on_click=lambda: open_exclusion_dialog(ui, ctx)).props(
                "flat dense no-caps color=negative"
            )
        if summary.lob_text:
            inline_hint(ui, summary.lob_text, tone=summary.lob_tone)

    row()
    return row


# --- the popup -------------------------------------------------------------------------


def _client():
    try:
        from nicegui import context as _ctx

        return _ctx.client
    except Exception:  # noqa: BLE001 - no client (a test double): build in place
        return None


def open_exclusion_dialog(ui, ctx: ExclusionDialogContext, *, mode: Optional[str] = None):
    """Open the popup: editable while the exclusions are not locked, else read-only with
    the lock's reason and remedy (``mode`` forces one). Built in the clicked client's
    top-level context and opened on the next tick (QDialog drops a same-batch open)."""
    client = _client()

    def _build() -> None:
        lock = _lock_state(ctx)
        chosen = mode or ("read_only" if lock.locked else "edit")
        if chosen == "edit":
            _build_edit_dialog(ui, ctx)
        else:
            _build_read_only_dialog(ui, ctx, lock)

    if client is not None:
        with client:
            _build()
    else:
        _build()


def _line_status_text(status, resolution) -> str:
    parts: list[str] = []
    if status.action == "exclude":
        if status.kind == "table":
            if status.unselects:
                parts.append(f"unselects {len(status.unselects)} table"
                             f"{'s' if len(status.unselects) != 1 else ''}")
            elif status.matched:
                parts.append(f"matches {status.matched} table{'s' if status.matched != 1 else ''}")
        else:
            for key, columns, tables in status.type_counts:
                from dsql_migrator.core.exclusion_rules import type_name_token

                parts.append(f"{type_name_token(key)}: {columns} column{'s' if columns != 1 else ''}"
                             f" in {tables} table{'s' if tables != 1 else ''}")
            if not status.type_counts:
                parts.append(f"matches {status.matched} column{'s' if status.matched != 1 else ''}")
        for include_line, count in status.kept_by:
            parts.append(f"{count} kept by include on L{include_line}")
        if status.blocked:
            parts.append(f"{status.blocked} blocked ({', '.join(status.blocked_labels)})")
    else:
        if status.reselects:
            parts.append(f"re-selects {len(status.reselects)} table"
                         f"{'s' if len(status.reselects) != 1 else ''}")
        elif status.keeps:
            parts.append(f"keeps {status.keeps}")
    if LINE_ONLY_UNSELECTED in status.codes:
        parts.append("only unselected tables")
    if LINE_ONLY_BLOCKED in status.codes:
        parts.append("all blocked")
    if LINE_NO_EFFECT in status.codes:
        parts.append("cancels nothing")
    return f"L{status.line}  " + " - ".join(parts)


def _replace_line(text: str, number: int, new: str) -> str:
    lines = text.split("\n")
    if 1 <= number <= len(lines):
        lines[number - 1] = new
    return "\n".join(lines)


def _append_line(text: str, new: str) -> str:
    return f"{text.rstrip(chr(10))}\n{new}" if text.strip() else new


def _build_edit_dialog(ui, ctx: ExclusionDialogContext) -> None:
    state = ctx.migration_state
    classify_restored_exclusions(state, ctx.job_manager)
    inventory = ctx.inventory_fn()
    inputs = ctx.inputs_fn()
    if inventory is None or inputs is None:
        ui.notify("Connect and run Evaluation first: there is nothing to exclude yet.",
                  type="warning")
        return
    text, opened_with = popup_open_text(state, inventory)
    synthesized = state.exclusion_rules_text() is None and bool(state.lob_exclusions())
    stored_at_open = state.lob_exclusions()
    token = state.exclusion_edit_token()
    held = {"text": text, "timer": None}
    view = state.exclusion_dialog_ui()
    view.clear()
    ui.add_css(_EDITOR_CSS)

    with ui.dialog().props("persistent") as dialog, ui.card().classes("gap-3").style(
        "width: min(980px, 94vw); max-width: 94vw"
    ):
        def _close() -> None:
            view.clear()
            dialog.close()

        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            ui.label(TITLE).classes("text-base font-semibold")
            ui.icon("help_outline").classes("text-gray-400 cursor-help").tooltip(HELP_TOOLTIP)
            ui.space()
            ui.button(on_click=_close).props(
                "flat dense round size=sm icon=close color=grey-7"
            ).tooltip("Close without saving")
        ui.label(HELP_TEXT).classes("text-xs text-gray-600")
        editor = ui.codemirror(
            text, language=None, theme="basicLight", line_wrapping=False,
            on_change=lambda e: _on_edit(getattr(e, "value", "")),
        ).classes("w-full exclusion-rules-editor")

        @ui.refreshable
        def preview() -> None:
            resolution = _resolve(ctx, inventory, inputs, held["text"], slot="editor")
            _render_errors(ui, resolution, held, editor, _run_preview_now)
            for number in sorted(resolution.line_status):
                status = resolution.line_status[number]
                inline_hint(ui, _line_status_text(status, resolution),
                            tone="warning" if status.codes else "neutral")
                for note in status.notes:
                    inline_hint(ui, f"      {note}", tone="neutral")
            _render_result(ui, resolution)
            _render_review_all(ui, resolution, view, _run_preview_now)
            _render_lob_section(ui, ctx, inventory, inputs, resolution, view, _add_rules,
                                _run_preview_now)
            _render_target_changes(ui, ctx, resolution, view, _run_preview_now)
            if ctx.capture_tables_fn is not None:
                _render_capture_section(ui, resolution, view, _run_preview_now)
            if synthesized:
                gaps = synthesis_gaps(resolution, stored_at_open, _migratable(inventory, inputs))
                if gaps:
                    render_notice(
                        ui, tone="warning",
                        header=f"{len(gaps)} saved exclusion{'s' if len(gaps) != 1 else ''} "
                        "are not reproduced by these rules",
                        body=" ".join(f"{g.table}.{g.column}: {g.reason}" for g in gaps[:_SHOWN])
                        + (f" And {len(gaps) - _SHOWN} more." if len(gaps) > _SHOWN else "")
                        + " Saving stores the rules as shown, without them.",
                    )
            if resolution.capture_warning:
                render_notice(ui, tone="warning", header="CDC capture list",
                              body=resolution.capture_warning)
            if resolution.diff is not None:
                for line in resolution.diff.lines:
                    inline_hint(ui, line, tone="neutral")
            save_button.set_enabled(resolution.can_save)

        def _run_preview_now() -> None:
            preview.refresh()

        def _add_rules(lines: Sequence[str]) -> None:
            text = held["text"]
            for line in lines:
                text = _append_line(text, line)
            held["text"] = text
            editor.set_value(text)
            preview.refresh()

        def _on_edit(value: str) -> None:
            held["text"] = value or ""
            timer = held.get("timer")
            if timer is not None:
                try:
                    timer.cancel()
                except Exception:  # noqa: BLE001 - an already-fired one-shot
                    pass
            held["timer"] = ui.timer(_DEBOUNCE_SECONDS, _run_preview_now, once=True)

        def _save() -> None:
            resolution = _resolve(ctx, inventory, inputs, held["text"], slot="editor")

            def _lock_check() -> Optional[str]:
                lock = _lock_state(ctx)
                if lock.locked:
                    return lock.reason
                if resolution.selected_after is not None:
                    return ctx.lock_reason_fn()
                return None

            reason = state.set_column_exclusions(
                resolution, text=held["text"], opened_with=opened_with, token=token,
                lock_check=_lock_check,
            )
            if reason:
                ui.notify(reason, type="warning", multi_line=True)
                return
            _close()
            ctx.refresh()

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=_close).props("flat no-caps color=grey-8")
            save_button = ui.button("Save", on_click=_save).props("no-caps color=primary")
        preview()
    ui.timer(0.05, dialog.open, once=True)


def _migratable(inventory, inputs):
    tables = {t.name: t for t in (getattr(inventory, "tables", None) or ())}
    return [tables[n] for n in inputs.migratable if n in tables]


def _render_errors(ui, resolution, held, editor, rerun) -> None:
    errors = [e for es in resolution.errors.values() for e in es]
    total = len(resolution.errors) + (1 if resolution.global_errors else 0)
    if not errors and not resolution.global_errors:
        return
    lines = held["text"].split("\n")
    with ui.column().classes("w-full gap-1 p-2 rounded-md border border-red-200 bg-red-50"):
        ui.label(f"{total} line{'s' if total != 1 else ''} need fixing. Save is disabled.").classes(
            "text-sm font-semibold text-red-700")
        for error in errors:
            quoted = lines[error.line - 1].strip() if 0 < error.line <= len(lines) else ""
            ui.label(f'L{error.line} "{quoted}"  {error.message}').classes("text-xs text-red-700")
            for suggestion in error.suggestions:
                ui.button(
                    suggestion.label,
                    on_click=lambda _e=None, n=error.line, t=suggestion.line_text: _apply(n, t),
                ).props("flat dense no-caps size=sm color=primary")
        for error in resolution.global_errors:
            ui.label(error.message).classes("text-xs text-red-700")
            for suggestion in error.suggestions:
                ui.button(
                    suggestion.label,
                    on_click=lambda _e=None, t=suggestion.line_text: _apply(0, t),
                ).props("flat dense no-caps size=sm color=primary")

    def _apply(number: int, line_text: str) -> None:
        held["text"] = (_replace_line(held["text"], number, line_text) if number
                        else _append_line(held["text"], line_text))
        editor.set_value(held["text"])
        rerun()


def _render_result(ui, resolution) -> None:
    unselected = [i.table for i in resolution.items if i.kind == "unselected"]
    excluded = [i for i in resolution.items if i.kind == "excluded"]
    blocked = [i for i in resolution.items if i.kind == "blocked"]
    warned = [i for i in excluded if i.warning]
    with ui.column().classes("w-full gap-1"):
        ui.label("Result").classes("text-sm font-semibold")
        if unselected:
            inline_hint(ui, f"Tables  {len(unselected)} unselected by rules ({_names(unselected)}). "
                        "Not loaded by this migration; left as is on Aurora DSQL.")
        overlaid = [i for i in excluded if not i.legacy]
        omit_only = len(excluded) - len(overlaid)
        tables = {i.table for i in overlaid}
        text = (f"Columns  {len(overlaid)} excluded in {len(tables)} table"
                f"{'s' if len(tables) != 1 else ''}, migrated as NULL."
                if overlaid else "Columns  none excluded.")
        if omit_only:
            text += f" {omit_only} more omit-only (excluded before v0.1.562, no target change)."
        inline_hint(ui, text)
        for item in blocked[:_SHOWN]:
            inline_hint(ui, f"(not excluded) {item.table}.{item.column} (L"
                        f"{', L'.join(str(n) for n in item.lines)})  {' '.join(item.why)}",
                        tone="warning")
        if len(blocked) > _SHOWN:
            inline_hint(ui, f"... and {len(blocked) - _SHOWN} more not excluded", tone="warning")
        for item in warned[:_SHOWN]:
            inline_hint(ui, f"(!) {item.table}.{item.column}  {' '.join(item.why)}",
                        tone="warning")
        if len(warned) > _SHOWN:
            inline_hint(ui, f"... and {len(warned) - _SHOWN} more warnings", tone="warning")


# --- the sections ----------------------------------------------------------------------


def _expansion(ui, title: str, view: dict, key: str, rerun):
    """A collapsed section whose content is drawn only while expanded (the open state
    lives in the view dict, so a re-render keeps it)."""
    def _toggle(event) -> None:
        view[key] = bool(getattr(event, "value", False))
        rerun()

    return ui.expansion(title, value=bool(view.get(key)), on_value_change=_toggle).classes(
        f"w-full {EXPANSION_PANEL_CLASSES}")


def review_rows(resolution, status_filter: str = "All") -> "list[dict]":
    """The ONE Review-all list (tables and columns, blocked included). Kept and
    Unselected rows have no filter of their own: they appear under All only."""
    rows = []
    for item in resolution.items:
        if status_filter == "Excluded" and item.kind != "excluded":
            continue
        if status_filter == "Blocked" and item.kind != "blocked":
            continue
        if status_filter == "Warning" and not item.warning:
            continue
        name = item.table if item.column is None else f"{item.table}.{item.column}"
        why = list(item.changes) + list(item.why)
        if item.kind == "excluded" and not item.changes:
            why.insert(0, "none (nullable)")
        rows.append({
            "item": name,
            "status": _STATUS[item.kind] + (" (!)" if item.warning else ""),
            "lines": ", ".join(f"L{n}" for n in item.lines),
            "why": "; ".join(why),
        })
    return rows


def _render_review_all(ui, resolution, view: dict, rerun) -> None:
    total = len(resolution.items)
    if not total:
        return
    with _expansion(ui, f"Review all {total} items", view, "review_open", rerun):
        if not view.get("review_open"):
            return
        status_filter = view.get("review_filter", "All")

        def _set_filter(event) -> None:
            view["review_filter"] = getattr(event, "value", "All") or "All"
            view["review_page"] = 1
            rerun()

        rows = review_rows(resolution, status_filter)
        pages = max(1, -(-len(rows) // _PAGE_SIZE))
        page = min(max(1, int(view.get("review_page", 1))), pages)
        view["review_page"] = page

        def _go(delta: int) -> None:
            view["review_page"] = min(max(1, page + delta), pages)
            rerun()

        with ui.row().classes("items-center gap-2 w-full"):
            ui.select(list(REVIEW_FILTERS), value=status_filter, label="Status",
                      on_change=_set_filter).props("dense outlined").classes("w-40")
            ui.space()
            ui.label(f"page {page} of {pages}").classes("text-xs text-gray-500")
            ui.button(on_click=lambda: _go(-1)).props("flat dense round size=sm icon=chevron_left")
            ui.button(on_click=lambda: _go(1)).props("flat dense round size=sm icon=chevron_right")
        ui.table(
            columns=[
                {"name": "item", "label": "Item", "field": "item", "align": "left"},
                {"name": "status", "label": "Status", "field": "status", "align": "left"},
                {"name": "lines", "label": "Lines", "field": "lines", "align": "left"},
                {"name": "why", "label": "Why / target change", "field": "why", "align": "left"},
            ],
            rows=rows[(page - 1) * _PAGE_SIZE: page * _PAGE_SIZE],
            row_key="item",
        ).props("dense flat wrap-cells").classes("w-full")


def lob_rows(ctx: ExclusionDialogContext, inventory, inputs, resolution) -> "list[dict]":
    """Section 4's rows: the type-based candidates of the tables in scope, minus what the
    OPEN rules exclude (never a primary key: the candidates never offer one)."""
    from dsql_migrator.core.converter import parse_target_column_types
    from dsql_migrator.core.exclusion_rules import rule_part_token
    from dsql_migrator.core.prerequisites import (
        _format_limit,
        dsql_value_limit_bytes,
        dsql_value_limit_is_compressed,
    )

    source_type = session_source_type(ctx.session)
    candidates = scope_lob_candidates(
        lob_exclusion_candidates(inventory, source_type=source_type),
        selected_tables=list(resolution.in_scope), stored_selection=(),
    )
    sizes = ctx.migration_state.value_sizes()
    tables = {t.name: t for t in getattr(inventory, "tables", None) or ()}
    rows = []
    for candidate in open_lob_candidates(candidates, resolution.columns):
        table = tables.get(candidate.table)
        ddl = getattr((inputs.applied or {}).get(candidate.table), "target_ddl", None) or ""
        target_types = parse_target_column_types(ddl) if ddl else {}
        for name in candidate.columns:
            column = next((c for c in table.columns if c.name == name), None) if table else None
            source = (getattr(column, "mysql_type", "") or "").strip()
            base = source.lower().split("(")[0].strip()
            target = target_types.get(name, "")
            limit_type = target or base
            limit = _format_limit(dsql_value_limit_bytes(limit_type))
            compressed = dsql_value_limit_is_compressed(limit_type) and limit_type != "bytea"
            collation = (getattr(column, "collation", None) or "").lower()
            charset = None
            if (source_type is SourceType.MYSQL and collation
                    and not collation.startswith("utf8mb4") and "text" in base):
                charset = collation.split("_")[0]
            cached = sizes.get(f"{candidate.table}.{name}")
            schema, _sep, rest = candidate.table.partition(".")
            rows.append({
                "table": candidate.table, "column": name, "source": source, "target": target,
                "allows": _TYPE_ALLOWS.get(source_type, {}).get(base),
                "limit": limit, "compressed": compressed, "charset": charset,
                "result": cached,
                "kept_by": resolution.kept_by.get((candidate.table, name), ()),
                "rule": "exclude " + ".".join(rule_part_token(x) for x in (schema, rest, name)),
            })
    return rows


def _size_label(result, charset: "Optional[str]") -> "tuple[str, str]":
    if result is None:
        return "Not checked", "neutral"
    verdict, at = result
    when = at.astimezone().strftime("%H:%M") if at is not None else ""
    if verdict == "unknown":
        return "Not determined: stopped after 15 s (or cancelled)", "neutral"
    if verdict == "over":
        return f"Over the limit found · {when}. {QUARANTINE_CONSEQUENCE}", "warning"
    caveat = (f" (measured in {charset}; Aurora DSQL stores UTF-8, which can be larger)"
              if charset else "")
    return f"No value over the limit · {when}{caveat}", "success"


def _render_lob_section(ui, ctx, inventory, inputs, resolution, view, add_rules, rerun) -> None:
    rows = lob_rows(ctx, inventory, inputs, resolution)
    over = [r for r in rows if r["result"] and r["result"][0] == "over" and not r["kept_by"]]
    title = f"Oversized-LOB candidates ({len(rows)})"
    if over:
        title += f" - {len(over)} over the limit found"
    with _expansion(ui, title, view, "lob_open", rerun):
        if not view.get("lob_open"):
            return
        ui.label("Type-based: these types can hold a value over Aurora DSQL's per-value limit. "
                 "Sizes are measured only if you ask.").classes("text-xs text-gray-600")
        table_filter = view.get("lob_table")
        shown_rows = [r for r in rows if not table_filter or r["table"] == table_filter]
        pages = max(1, -(-len(shown_rows) // _PAGE_SIZE))
        page = min(max(1, int(view.get("lob_page", 1))), pages)
        view["lob_page"] = page
        page_rows = shown_rows[(page - 1) * _PAGE_SIZE: page * _PAGE_SIZE]
        if pages > 1:
            def _go(delta: int) -> None:
                view["lob_page"] = min(max(1, page + delta), pages)
                rerun()

            with ui.row().classes("items-center gap-2 w-full"):
                ui.space()
                ui.label(f"page {page} of {pages}").classes("text-xs text-gray-500")
                ui.button(on_click=lambda: _go(-1)).props("flat dense round size=sm icon=chevron_left")
                ui.button(on_click=lambda: _go(1)).props("flat dense round size=sm icon=chevron_right")
        for row in page_rows:
            with ui.row().classes("items-center gap-2 w-full no-wrap"):
                arrow = f"  {row['source']} -> {row['target']}" if row["target"] else f"  {row['source']}"
                ui.label(f"{row['table']}.{row['column']}{arrow}").classes("text-xs font-mono")
                ui.space()
                if row["kept_by"]:
                    first = row["kept_by"][0]
                    ui.button(f"Go to L{first}", on_click=lambda _e=None, n=first: ui.notify(
                        f"L{n} includes it, so a rule here would do nothing: edit or delete L{n}.")
                    ).props("flat dense no-caps size=sm color=primary")
                else:
                    ui.button("+ add rule", on_click=lambda _e=None, r=row["rule"]: add_rules([r])
                              ).props("flat dense no-caps size=sm color=primary")
            parts = []
            if row["allows"]:
                parts.append(f"Type allows up to {row['allows']}.")
            parts.append(f"Limit {row['limit']}.")
            label, tone = _size_label(row["result"], row["charset"])
            hint = inline_hint(ui, " ".join(parts) + " " + label, tone=tone)
            if row["compressed"]:
                hint.tooltip("The limit applies to the compressed size; the size check "
                             "measures uncompressed bytes (an upper bound).")
        if over:
            n = len(over)
            ui.button(f"+ add rules for the {n} over the limit",
                      on_click=lambda: add_rules([r["rule"] for r in over])).props(
                "flat dense no-caps size=sm color=primary")
        _render_check_sizes(ui, ctx, [r for r in page_rows if not r["kept_by"]], rerun)
        inline_hint(ui, HONESTY_FOOTER, tone="neutral")


def check_sizes_cost(n: int) -> str:
    """The cost of Check sizes, stated before the click."""
    from dsql_migrator.core.prerequisites import VALUE_SIZE_PROBE_TIMEOUT_SECONDS

    worst = n * VALUE_SIZE_PROBE_TIMEOUT_SECONDS
    worst_text = f"about {worst / 60:.0f} min" if worst >= 90 else f"{worst:.0f} s"
    return (
        f"Reads the source: one bounded query per column for the {n} column"
        f"{'s' if n != 1 else ''} on this page, stopping at the first over-limit value; at "
        f"most {VALUE_SIZE_PROBE_TIMEOUT_SECONDS:.0f} s each (worst case {worst_text}). "
        "Cancel stops after the current column."
    )


def _render_check_sizes(ui, ctx: ExclusionDialogContext, rows, rerun) -> None:
    """[Check sizes] for the page's not-yet-excluded candidates (at most 25): single-flight,
    cancellable between columns, ONE source engine per pass disposed in a finally, the
    probe off the event loop, the labels refreshed between awaits (never from a worker)."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import RECONNECT_REASON

    state = ctx.migration_state
    rows = rows[:_PAGE_SIZE]
    if not rows:
        return
    running = state.size_check_running()
    ready = bool(getattr(ctx.session, "connection_ready", lambda: False)())
    with ui.row().classes("items-center gap-2 w-full"):
        button = ui.button("Check sizes", on_click=lambda: _check_sizes(ctx, rows, rerun)).props(
            "outline dense no-caps size=sm color=primary")
        if running:
            button.props("disable")
            ui.button("Cancel", on_click=state.cancel_size_check).props(
                "flat dense no-caps size=sm color=grey-8")
        elif not ready:
            button.props("disable")
            button.tooltip(RECONNECT_REASON)
        inline_hint(ui, check_sizes_cost(len(rows)))


async def _check_sizes(ctx: ExclusionDialogContext, rows, rerun) -> None:
    from nicegui import run

    from dsql_migrator.core.prerequisites import (
        VALUE_SIZE_PROBE_TIMEOUT_SECONDS,
        dsql_value_limit_bytes,
    )
    from dsql_migrator.ui.connect import make_source_engine_factory
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    state = ctx.migration_state
    session = ctx.session
    if not getattr(session, "connection_ready", lambda: False)():
        return
    if not state.begin_size_check():
        return  # single-flight: a pass is already running
    rerun()
    try:
        config = session.source_config
        probe = SessionSourceProbe(config, session.source_password)
        engine = await run.io_bound(make_source_engine_factory(session.source_password), config)
        try:
            by_table: dict[str, list] = {}
            for row in rows:
                by_table.setdefault(row["table"], []).append(row)
            for table, cols in by_table.items():
                if state.size_check_cancelled():
                    break
                names = [r["column"] for r in cols]
                answer = await run.io_bound(
                    probe.oversized_values, table, names,
                    limit_bytes=dsql_value_limit_bytes(""),
                    timeout_seconds=VALUE_SIZE_PROBE_TIMEOUT_SECONDS,
                    limits={r["column"]: dsql_value_limit_bytes(r["target"] or r["source"])
                            for r in cols},
                    column_types={r["column"]: r["source"] for r in cols},
                    engine=engine, should_cancel=state.size_check_cancelled,
                )
                state.record_value_sizes(table, answer or {}, asked=names)
                rerun()
        finally:
            await run.io_bound(engine.dispose)
    finally:
        state.end_size_check()
        rerun()


def _statement_action(text: str) -> str:
    return "DROP NOT NULL" if text.rstrip().endswith("DROP NOT NULL") else "DROP DEFAULT"


def _render_target_changes(ui, ctx, resolution, view, rerun) -> None:
    from dsql_migrator.ui.data_migration._models import MigrationType

    statements = [s for c in resolution.target_changes for s in c.statements]
    notes = [c.note for c in resolution.target_changes if c.note]
    restores = sum(len(c.restores) for c in resolution.target_changes)
    if not statements and not notes:
        return
    cdc_only = getattr(ctx.migration_state, "migration_type", None) is MigrationType.CDC_ONLY
    when = "run when CDC starts" if cdc_only else "run when the load starts"
    title = (f"Changes to Aurora DSQL ({len(statements)} statement"
             f"{'s' if len(statements) != 1 else ''}, {when})")
    with _expansion(ui, title, view, "changes_open", rerun):
        if not view.get("changes_open"):
            return
        summary = overlay_change_summary(_statement_action(s) for s in statements)
        if summary:
            inline_hint(ui, summary, tone="warning")
        with ui.column().classes(f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"):
            for statement in statements:
                ui.label(statement).classes("text-xs font-mono wrap-anywhere")
        if any(_statement_action(s) == "DROP NOT NULL" for s in statements):
            inline_hint(ui, "NOT NULL is dropped one way: Aurora DSQL cannot add NOT NULL back "
                        "in place (a later Drop & reload of the table restores it).",
                        tone="warning")
        if restores:
            inline_hint(ui, f"Cut over restores {restores} DEFAULT{'s' if restores != 1 else ''} "
                        "(new rows only).")
        for note in notes:
            inline_hint(ui, note, tone="neutral")


def _render_capture_section(ui, resolution, view, rerun) -> None:
    from dsql_migrator.core.cdc import CDC_PARAMETER_BYTE_LIMIT

    if resolution.cdc_value is None:
        return
    title = f"CDC capture list ({resolution.cdc_bytes:,} of {CDC_PARAMETER_BYTE_LIMIT:,} bytes)"
    with _expansion(ui, title, view, "capture_open", rerun):
        if not view.get("capture_open"):
            return
        with ui.column().classes(f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"):
            ui.label(resolution.cdc_value or "(nothing excluded at capture)").classes(
                "text-xs font-mono wrap-anywhere")
        ambiguity = (capture_ambiguity_warning(resolution.capture_preview)
                     if resolution.capture_preview is not None else None)
        if ambiguity:
            inline_hint(ui, ambiguity, tone="warning")


_LOCK_HEADERS = {
    0: "The CDC infrastructure is being deleted",
    1: "The CDC infrastructure is being created",
    2: "The CDC infrastructure holds these exclusions",
    3: "A Full Load already loaded data with these exclusions",
}


def _render_pending(ui, pending: Sequence[str]) -> None:
    for line in pending:
        rule, comment = split_rule_comment(line)
        source = f" ({comment.strip().lstrip('#').strip()})" if comment.strip() else ""
        inline_hint(ui, f'Pending: "{rule.strip()}"{source} will be added when the rules can '
                    "be edited again.")


def _change_type_button(ui, ctx: ExclusionDialogContext, close) -> None:
    """[Change migration type]: close the popup FIRST (a scroll behind a persistent dialog
    moves only the page under it), then the shared scroll on the next tick. It only
    navigates: the tool never switches the type itself."""
    reason = ctx.type_lock_reason_fn()

    def _go() -> None:
        close()
        ui.timer(0.05, lambda: ui.run_javascript(SCROLL_TO_MIGRATION_TYPE_JS), once=True)

    button = ui.button("Change migration type", icon="arrow_upward", on_click=_go).props(
        "outline no-caps color=primary")
    if reason:
        button.props("disable")
        button.tooltip(reason)


def _build_read_only_dialog(ui, ctx: ExclusionDialogContext, lock) -> None:
    """The locked popup (D4): the lock's own reason and remedy (exclusion_lock_state is
    the only source of that text), the saved rules (not editable), the pending lines kept
    pending, the stored count, the stack's capture list as deployed -- beside the session's
    when they differ -- and only the remedies true in this cell."""
    state = ctx.migration_state
    classify_restored_exclusions(state, ctx.job_manager)
    inventory = ctx.inventory_fn()
    text = exclusion_rules_text_for(state, inventory)
    pending = state.pending_rules()
    stored = state.lob_exclusions()
    ui.add_css(_EDITOR_CSS)
    with ui.dialog().props("persistent") as dialog, ui.card().classes("gap-3").style(
        "width: min(980px, 94vw); max-width: 94vw"
    ):
        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            ui.label(f"{TITLE} (read only)").classes("text-base font-semibold")
            ui.space()
            ui.button(on_click=dialog.close).props(
                "flat dense round size=sm icon=close color=grey-7"
            ).tooltip("Close")
        render_notice(ui, tone="info", header=_LOCK_HEADERS.get(lock.tier, "Locked"),
                      body=lock.reason or "")
        _render_pending(ui, pending)
        ui.codemirror(text, language=None, theme="basicLight", line_wrapping=False).classes(
            "w-full exclusion-rules-editor").props("disable")
        n = sum(len(c) for c in stored.values())
        tables = sum(1 for c in stored.values() if c)
        inline_hint(ui, f"Columns: {n} excluded in {tables} table{'s' if tables != 1 else ''}")
        _render_deployed_capture(ui, ctx, inventory)
        with ui.row().classes("w-full justify-end gap-2"):
            if lock.tier != 0 and lock.offers_change_type:
                _change_type_button(ui, ctx, dialog.close)
            ui.button("Close", on_click=dialog.close).props("flat no-caps color=grey-8")
    ui.timer(0.05, dialog.open, once=True)


def _render_deployed_capture(ui, ctx: ExclusionDialogContext, inventory) -> None:
    state = ctx.migration_state
    deployed = getattr(state, "cdc_deployed_column_exclude_list", None)
    if deployed is None:
        return
    with ui.expansion("CDC capture list (as deployed)").classes(
            f"w-full {EXPANSION_PANEL_CLASSES}"):
        with ui.column().classes(f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"):
            ui.label(deployed or "(nothing excluded at capture)").classes(
                "text-xs font-mono wrap-anywhere")
    inputs = ctx.inputs_fn()
    if ctx.capture_tables_fn is None or inventory is None or inputs is None:
        return
    captured = list(ctx.capture_tables_fn(inputs.selected))
    capture = cdc_capture_exclusions(state, inventory, captured)
    mismatch = capture_exclusion_mismatch(state, inventory, capture, captured)
    if mismatch:
        render_notice(
            ui, tone="warning", header="This session's exclusions differ from the stack's",
            body=f"{mismatch} This session: {capture.sent or '(none)'}. As deployed: "
            f"{deployed or '(none)'}.",
        )


__all__ = [
    "BUTTON_LABEL",
    "check_sizes_cost",
    "HONESTY_FOOTER",
    "REVIEW_FILTERS",
    "lob_rows",
    "review_rows",
    "ExclusionDialogContext",
    "TITLE",
    "open_exclusion_dialog",
    "render_exclusion_button_row",
]
