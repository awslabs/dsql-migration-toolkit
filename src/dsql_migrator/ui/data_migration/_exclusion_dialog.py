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

from dsql_migrator.core.exclusion_rules import (
    LINE_NO_EFFECT,
    LINE_ONLY_BLOCKED,
    LINE_ONLY_UNSELECTED,
    synthesis_gaps,
)
from dsql_migrator.ui.data_migration._models import (
    classify_restored_exclusions,
    exclusion_drift,
    exclusion_row_summary,
    lob_exclusion_candidates,
    open_lob_candidates,
    popup_open_text,
    popup_resolution,
    scope_lob_candidates,
    session_source_type,
    stored_exclusion_block_reason,
    stored_exclusion_issues,
)
from dsql_migrator.ui.design import inline_hint, render_notice

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
    ui.add_css(_EDITOR_CSS)

    with ui.dialog().props("persistent") as dialog, ui.card().classes("gap-3").style(
        "width: min(980px, 94vw); max-width: 94vw"
    ):
        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            ui.label(TITLE).classes("text-base font-semibold")
            ui.icon("help_outline").classes("text-gray-400 cursor-help").tooltip(HELP_TOOLTIP)
            ui.space()
            ui.button(on_click=dialog.close).props(
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
            dialog.close()
            ctx.refresh()

        with ui.row().classes("w-full justify-end gap-2"):
            ui.button("Cancel", on_click=dialog.close).props("flat no-caps color=grey-8")
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
        tables = {i.table for i in excluded}
        inline_hint(ui, f"Columns  {len(excluded)} excluded in {len(tables)} table"
                    f"{'s' if len(tables) != 1 else ''}, migrated as NULL."
                    if excluded else "Columns  none excluded.")
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


def _build_read_only_dialog(ui, ctx: ExclusionDialogContext, lock) -> None:
    """The locked popup: the rules as saved (disabled), the lock's own reason and remedy.
    The per-cell variants and their remedy buttons come with the read-only modes."""
    state = ctx.migration_state
    inventory = ctx.inventory_fn()
    text, _pending = popup_open_text(state, inventory)
    with ui.dialog().props("persistent") as dialog, ui.card().classes("gap-3").style(
        "width: min(980px, 94vw); max-width: 94vw"
    ):
        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            ui.label(TITLE).classes("text-base font-semibold")
            ui.space()
            ui.button(on_click=dialog.close).props(
                "flat dense round size=sm icon=close color=grey-7"
            ).tooltip("Close")
        render_notice(ui, tone="info",
                      header="The exclusions are fixed right now", body=lock.reason or "")
        ui.codemirror(text, language=None, theme="basicLight", line_wrapping=False).classes(
            "w-full exclusion-rules-editor").props("disable")
        with ui.row().classes("w-full justify-end"):
            ui.button("Close", on_click=dialog.close).props("flat no-caps color=grey-8")
    ui.timer(0.05, dialog.open, once=True)


__all__ = [
    "BUTTON_LABEL",
    "ExclusionDialogContext",
    "TITLE",
    "open_exclusion_dialog",
    "render_exclusion_button_row",
]
