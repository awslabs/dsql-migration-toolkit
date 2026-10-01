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
    effective_lob_exclusions,
    exclusion_rules_text_for,
    exclusion_drift,
    exclusion_row_summary,
    lob_exclusion_candidates,
    open_lob_candidates,
    overlay_change_summary,
    popup_open_text,
    popup_resolution,
    quarantine_rule_suggestion,
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
        state.set_exclusion_drift_text(summary.text if summary.variant == "drift" else None)
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


def _client(ui=None):
    """The clicked browser tab's client (via the ``ui`` passed in, so a test double, which
    has none, builds in place), or ``None``."""
    try:
        context = getattr(ui, "context", None) if ui is not None else None
        client = getattr(context, "client", None)
        return client if hasattr(client, "id") else None
    except Exception:  # noqa: BLE001 - no client: build in place
        return None


# One popup per browser tab: a second click (or a stale reopen) never stacks another
# persistent dialog, and each closed dialog is deleted after its close transition.
_OPEN_DIALOGS: dict = {}
_PENDING = object()


def _claim(client) -> bool:
    if client is None:
        return True
    if _OPEN_DIALOGS.get(client.id) is not None:
        return False
    _OPEN_DIALOGS[client.id] = _PENDING
    return True


def _release(client, dialog=None) -> None:
    if client is None:
        return
    if dialog is None or _OPEN_DIALOGS.get(client.id) in (dialog, _PENDING):
        _OPEN_DIALOGS.pop(client.id, None)


def _register(dialog, client) -> None:
    """Track ``dialog`` as this tab's popup and delete it once hidden."""
    if client is None or dialog is None:
        return
    _OPEN_DIALOGS[client.id] = dialog

    def _gone(*_a) -> None:
        _release(client, dialog)
        try:
            dialog.delete()
        except Exception:  # noqa: BLE001 - already gone with its client
            pass

    try:
        dialog.on("hide", _gone)
    except Exception:  # noqa: BLE001 - a double without events
        pass


_CSS_CLIENTS: set = set()


def _add_css_once(ui, client) -> None:
    key = getattr(client, "id", None)
    if key is not None and key in _CSS_CLIENTS:
        return
    ui.add_css(_EDITOR_CSS)
    if key is not None:
        _CSS_CLIENTS.add(key)


def open_exclusion_dialog(
    ui,
    ctx: ExclusionDialogContext,
    *,
    mode: Optional[str] = None,
    insert_lines: Sequence[str] = (),
    hint: "Optional[tuple[str, str]]" = None,
    lob_filter: Optional[str] = None,
):
    """Open the popup: editable while the exclusions are not locked, else read-only with
    the lock's reason and remedy (``mode`` forces one). Built in the clicked client's
    top-level context and opened on the next tick (QDialog drops a same-batch open); at
    most one per tab.

    A recovery route passes ``insert_lines`` (shown, not saved), a ``hint`` ``(tone,
    text)`` above the editor, and ``lob_filter`` (the LOB section opens filtered to that
    table)."""
    client = _client(ui)
    if not _claim(client):
        return

    def _build():
        lock = _lock_state(ctx)
        chosen = mode or ("read_only" if lock.locked else "edit")
        extra = {"insert_lines": tuple(insert_lines), "hint": hint, "lob_filter": lob_filter,
                 "client": client}
        if chosen == "edit":
            return _build_edit_dialog(ui, ctx, **extra)
        if chosen == "change_and_reload":
            return _build_reload_dialog(ui, ctx, **extra)
        return _build_read_only_dialog(ui, ctx, lock, hint=hint, client=client)

    try:
        if client is not None:
            with client:
                dialog = _build()
        else:
            dialog = _build()
    except BaseException:
        _release(client)
        raise
    if dialog is None:
        _release(client)
    else:
        _register(dialog, client)


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


def _with_inserted(text: str, lines: Sequence[str]) -> str:
    """``text`` plus each line not already there (by rule text, comment ignored)."""
    have = {" ".join(split_rule_comment(x)[0].split()) for x in text.split("\n")}
    for line in lines:
        if " ".join(split_rule_comment(line)[0].split()) not in have:
            text = _append_line(text, line)
    return text


def _apply_route_view(view: dict, lob_filter: Optional[str]) -> None:
    if lob_filter:
        view["lob_open"] = True
        view["lob_table"] = lob_filter


def _shown_matches(held: dict, tables: Sequence[str] = ()) -> bool:
    """Whether the click acts on what the dialog last SHOWED (the preview is debounced,
    so a click can land before it caught up with the text)."""
    shown = held.get("shown")
    return shown is not None and shown == (held["text"], tuple(tables))


def _cancel_timer(held: dict) -> None:
    timer = held.get("timer")
    if timer is not None:
        try:
            timer.cancel()
        except Exception:  # noqa: BLE001 - an already-fired one-shot
            pass
        held["timer"] = None


def _go_to_line_js(editor, n: int) -> str:
    """Select line ``n`` in the CodeMirror editor and bring it into view (clamped: the
    text may have shrunk since the line number was shown)."""
    return (
        f"const c=getElement({editor.id});const v=c&&c.editor;"
        f"if(v){{const n=Math.min({int(n)},v.state.doc.lines);const l=v.state.doc.line(n);"
        "v.dispatch({selection:{anchor:l.from,head:l.to},scrollIntoView:true});v.focus();"
        "c.$el.scrollIntoView({block:'nearest'});}"
    )


def _build_edit_dialog(ui, ctx: ExclusionDialogContext, *, insert_lines=(), hint=None,
                       lob_filter=None, client=None):
    state = ctx.migration_state
    classify_restored_exclusions(state, ctx.job_manager)
    inventory = ctx.inventory_fn()
    inputs = ctx.inputs_fn()
    if inventory is None or inputs is None:
        ui.notify("Connect and run Evaluation first: there is nothing to exclude yet.",
                  type="warning")
        return None
    text, opened_with = popup_open_text(state, inventory)
    text = _with_inserted(text, insert_lines)
    synthesized = state.exclusion_rules_text() is None and bool(state.lob_exclusions())
    stored_at_open = state.lob_exclusions()
    token = state.exclusion_edit_token()
    held = {"text": text, "timer": None, "shown": None, "action": None}
    view: dict = {}  # this dialog's own view state (another tab never resets it)
    _apply_route_view(view, lob_filter)
    _add_css_once(ui, client)

    with ui.dialog().props("persistent") as dialog, ui.card().classes("gap-3").style(
        "width: min(980px, 94vw); max-width: 94vw"
    ):
        def _close() -> None:
            _cancel_timer(held)
            state.clear_size_check_listener(_run_preview_now)
            dialog.close()

        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            ui.label(TITLE).classes("text-base font-semibold")
            ui.icon("help_outline").classes("text-gray-400 cursor-help").tooltip(HELP_TOOLTIP)
            ui.space()
            ui.button(on_click=_close).props(
                "flat dense round size=sm icon=close color=grey-7"
            ).tooltip("Close without saving")
        ui.label(HELP_TEXT).classes("text-xs text-gray-600")
        if hint:
            inline_hint(ui, hint[1], tone=hint[0])
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
                                _run_preview_now, _go_to_line)
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
            held["shown"] = (held["text"], ())

        def _run_preview_now() -> None:
            _cancel_timer(held)
            preview.refresh()

        def _go_to_line(n: int) -> None:
            ui.run_javascript(_go_to_line_js(editor, n))

        def _add_rules(lines: Sequence[str]) -> None:
            held["text"] = _with_inserted(held["text"], lines)
            editor.set_value(held["text"])
            _run_preview_now()

        def _on_edit(value: str) -> None:
            held["text"] = value or ""
            # Until the preview caught up, Save acts on nothing it has not shown.
            save_button.set_enabled(False)
            _cancel_timer(held)
            held["timer"] = ui.timer(_DEBOUNCE_SECONDS, _run_preview_now, once=True)

        def _save() -> None:
            if not _shown_matches(held):
                _run_preview_now()
                ui.notify("The rules changed since they were checked: review them and "
                          "click Save again.", type="warning")
                return
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
        state.set_size_check_listener(_run_preview_now)
        preview()
    ui.timer(0.05, dialog.open, once=True)
    return dialog


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
            shown = lines[error.line - 1] if 0 < error.line <= len(lines) else ""
            ui.label(f'L{error.line} "{shown.strip()}"  {error.message}').classes(
                "text-xs text-red-700")
            for suggestion in error.suggestions:
                ui.button(
                    suggestion.label,
                    on_click=lambda _e=None, n=error.line, t=suggestion.line_text, was=shown: (
                        _apply(n, t, was)),
                ).props("flat dense no-caps size=sm color=primary")
        for error in resolution.global_errors:
            ui.label(error.message).classes("text-xs text-red-700")
            for suggestion in error.suggestions:
                ui.button(
                    suggestion.label,
                    on_click=lambda _e=None, t=suggestion.line_text: _apply(0, t, None),
                ).props("flat dense no-caps size=sm color=primary")

    def _apply(number: int, line_text: str, was: Optional[str]) -> None:
        if number:
            current = held["text"].split("\n")
            if not (0 < number <= len(current)) or current[number - 1] != was:
                # The text moved since this fix was shown: never rewrite another line.
                rerun()
                ui.notify("The rules changed since this fix was shown: check it again.",
                          type="warning")
                return
            held["text"] = _replace_line(held["text"], number, line_text)
        else:
            held["text"] = _with_inserted(held["text"], [line_text])
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


def _lob_candidates_open(ctx: ExclusionDialogContext, inventory, resolution):
    """The type-based candidates of the tables in scope, minus what the OPEN rules exclude
    (never a primary key: the candidates never offer one)."""
    source_type = session_source_type(ctx.session)
    candidates = scope_lob_candidates(
        lob_exclusion_candidates(inventory, source_type=source_type),
        selected_tables=list(resolution.in_scope), stored_selection=(),
    )
    return open_lob_candidates(candidates, resolution.columns)


def lob_rows(ctx: ExclusionDialogContext, inventory, inputs, resolution) -> "list[dict]":
    """Section 4's rows (built only while the section is expanded): each open candidate
    with its source -> target type, ceiling, limit, charset caveat, cached size result,
    the include lines keeping it, and -- for a column the rule refuses (B1-B4) -- the
    reason instead of [+ add rule]."""
    from dsql_migrator.core.column_exclusion import column_exclusion_verdict
    from dsql_migrator.core.converter import parse_target_column_types
    from dsql_migrator.core.exclusion_rules import rule_part_token
    from dsql_migrator.core.prerequisites import (
        _format_limit,
        dsql_value_limit_bytes,
        dsql_value_limit_is_compressed,
    )
    from dsql_migrator.ui.data_migration._models import exclusion_context_for

    source_type = session_source_type(ctx.session)
    open_candidates = _lob_candidates_open(ctx, inventory, resolution)
    if not open_candidates:
        return []
    sizes = ctx.migration_state.value_sizes()
    tables = {t.name: t for t in getattr(inventory, "tables", None) or ()}
    verdict_ctx = exclusion_context_for(
        ctx.migration_state, inventory, applied_conversions=inputs.applied,
        preserve_foreign_keys=inputs.preserve_foreign_keys,
        selected_tables=list(resolution.in_scope),
        focus_tables=[c.table for c in open_candidates],
    )
    rows = []
    for candidate in open_candidates:
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
            verdict = column_exclusion_verdict(candidate.table, name, verdict_ctx)
            schema, _sep, rest = candidate.table.partition(".")
            rows.append({
                "table": candidate.table, "column": name, "source": source, "target": target,
                "allows": _TYPE_ALLOWS.get(source_type, {}).get(base),
                "limit": limit, "compressed": compressed, "charset": charset,
                "result": sizes.get((candidate.table, name)),
                "kept_by": resolution.kept_by.get((candidate.table, name), ()),
                "blocked": (" ".join(f.message for f in verdict.findings)
                            if verdict.blocked else None),
                "rule": "exclude " + ".".join(rule_part_token(x) for x in (schema, rest, name)),
            })
    return rows


def _size_label(result, charset: "Optional[str]") -> "tuple[str, str]":
    if result is None:
        return "Not checked", "neutral"
    from datetime import timezone

    verdict, at = result
    when = at.astimezone(timezone.utc).strftime("%H:%M UTC") if at is not None else ""
    if verdict == "unknown":
        return ("Not determined: the read failed, took longer than 15 s, or was cancelled",
                "neutral")
    if verdict == "over":
        return f"Over the limit found · {when}. {QUARANTINE_CONSEQUENCE}", "warning"
    caveat = (f" (measured in {charset}; Aurora DSQL stores UTF-8, which can be larger)"
              if charset else "")
    return f"No value over the limit · {when}{caveat}", "success"


def _render_lob_section(ui, ctx, inventory, inputs, resolution, view, add_rules, rerun,
                        go_to_line=None) -> None:
    # The title needs only the counts, so the rows (target types, verdicts) are built only
    # while the section is open.
    candidates = _lob_candidates_open(ctx, inventory, resolution)
    count = sum(len(c.columns) for c in candidates)
    sizes = ctx.migration_state.value_sizes()
    over_count = sum(1 for c in candidates for col in c.columns
                     if (sizes.get((c.table, col)) or ("",))[0] == "over")
    title = f"Oversized-LOB candidates ({count})"
    if over_count:
        title += f" - {over_count} over the limit found"
    with _expansion(ui, title, view, "lob_open", rerun):
        if not view.get("lob_open"):
            return
        rows = lob_rows(ctx, inventory, inputs, resolution)
        addable_over = [r for r in rows if r["result"] and r["result"][0] == "over"
                        and not r["kept_by"] and not r["blocked"]]
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
                if row["blocked"]:
                    pass  # the rule refuses it: its reason is shown below, no rule offered
                elif row["kept_by"]:
                    first = row["kept_by"][0]

                    def _goto(_e=None, n=first) -> None:
                        if go_to_line is not None:
                            go_to_line(n)
                        ui.notify(f"L{n} includes it, so a rule here would do nothing: edit or "
                                  f"delete L{n}.")

                    ui.button(f"Go to L{first}", on_click=_goto).props(
                        "flat dense no-caps size=sm color=primary")
                else:
                    ui.button("+ add rule", on_click=lambda _e=None, r=row["rule"]: add_rules([r])
                              ).props("flat dense no-caps size=sm color=primary")
            if row["blocked"]:
                inline_hint(ui, f"Cannot be excluded: {row['blocked']}", tone="neutral")
                continue
            parts = []
            if row["allows"]:
                parts.append(f"Type allows up to {row['allows']}.")
            parts.append(f"Limit {row['limit']}.")
            label, tone = _size_label(row["result"], row["charset"])
            if row["kept_by"] and tone == "warning":
                tone = "warning"
            hint = inline_hint(ui, " ".join(parts) + " " + label, tone=tone)
            if row["compressed"]:
                hint.tooltip("The limit applies to the compressed size; the size check "
                             "measures uncompressed bytes (an upper bound).")
        if addable_over:
            n = len(addable_over)
            ui.button(f"+ add rules for the {n} over the limit",
                      on_click=lambda: add_rules([r["rule"] for r in addable_over])).props(
                "flat dense no-caps size=sm color=primary")
        # Every not-yet-excluded candidate on the page -- an include-kept one is exactly a
        # column that WILL load -- except one the rule refuses.
        _render_check_sizes(ui, ctx, [r for r in page_rows if not r["blocked"]], rerun)
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
    probe off the event loop, the popup on screen refreshed between awaits (never from a
    worker)."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import RECONNECT_REASON

    state = ctx.migration_state
    rows = rows[:_PAGE_SIZE]
    if not rows:
        return
    running = state.size_check_running()
    ready = bool(getattr(ctx.session, "connection_ready", lambda: False)())

    def _cancel() -> None:
        state.cancel_size_check()
        rerun()

    with ui.row().classes("items-center gap-2 w-full"):
        button = ui.button(
            "Check sizes", on_click=lambda: _check_sizes(ctx, rows, rerun, notify=ui.notify)
        ).props(
            "outline dense no-caps size=sm color=primary")
        if running:
            button.props("disable")
            ui.button("Cancel", on_click=_cancel).props("flat dense no-caps size=sm color=grey-8")
        elif not ready:
            button.props("disable")
            button.tooltip(RECONNECT_REASON)
        inline_hint(ui, check_sizes_cost(len(rows)))


def _ping(engine) -> None:
    with engine.connect():
        pass


async def _check_sizes(ctx: ExclusionDialogContext, rows, rerun, notify=None) -> None:
    from nicegui import run

    from dsql_migrator.core.prerequisites import (
        VALUE_SIZE_PROBE_TIMEOUT_SECONDS,
        dsql_value_limit_bytes,
    )
    from dsql_migrator.ui.connect import make_source_engine_factory
    from dsql_migrator.ui.prerequisite_probes import SessionSourceProbe

    state = ctx.migration_state
    session = ctx.session

    def _refresh() -> None:
        # The popup on screen now (it may have been closed and reopened mid-pass), and the
        # row under the tree, whose LOB line follows a found offender.
        current = state.size_check_listener() or rerun
        current()
        row_refresh = ctx.extra.get("row_refresh")
        if row_refresh is not None:
            row_refresh()

    if not getattr(session, "connection_ready", lambda: False)():
        return
    if not state.begin_size_check():
        return  # single-flight: a pass is already running
    _refresh()
    try:
        config = session.source_config
        probe = SessionSourceProbe(config, session.source_password)
        engine = await run.io_bound(make_source_engine_factory(session.source_password), config)
        try:
            try:
                await run.io_bound(_ping, engine)
            except Exception:  # noqa: BLE001 - unreachable source: say so, measure nothing
                if notify is not None:
                    notify("Could not reach the source: reconnect on the Connect step, then "
                           "check sizes again.", type="warning")
                return
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
                _refresh()
        finally:
            await run.io_bound(engine.dispose)
    finally:
        state.end_size_check()
        _refresh()


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


def _build_read_only_dialog(ui, ctx: ExclusionDialogContext, lock, *, hint=None, client=None):
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
    _add_css_once(ui, client)
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
        if hint:
            inline_hint(ui, hint[1], tone=hint[0])
        _render_pending(ui, pending)
        ui.codemirror(text, language=None, theme="basicLight", line_wrapping=False).classes(
            "w-full exclusion-rules-editor").props("disable")
        n = sum(len(c) for c in stored.values())
        tables = sum(1 for c in stored.values() if c)
        inline_hint(ui, f"Columns: {n} excluded in {tables} table{'s' if tables != 1 else ''}")
        # While the stack is being deleted its set is going away: no remedy text then.
        _render_deployed_capture(ui, ctx, inventory, with_mismatch=lock.tier != 0)
        if lock.offers_change_and_reload:
            def _switch() -> None:
                _release(client, dialog)
                dialog.close()

            _change_and_reload_button(ui, ctx, _switch)
        with ui.row().classes("w-full justify-end gap-2"):
            if lock.tier != 0 and lock.offers_change_type:
                _change_type_button(ui, ctx, dialog.close)
            ui.button("Close", on_click=dialog.close).props("flat no-caps color=grey-8")
    ui.timer(0.05, dialog.open, once=True)
    return dialog


def _reload_gate(ctx: ExclusionDialogContext) -> Optional[str]:
    from dsql_migrator.ui.data_migration._cdc_monitoring import change_and_reload_block_reason

    ready = bool(getattr(ctx.session, "connection_ready", lambda: False)())
    return change_and_reload_block_reason(ctx.migration_state, ctx.job_manager,
                                          connections_ready=ready)


def _change_and_reload_button(ui, ctx: ExclusionDialogContext, close) -> None:
    """[Change & reload] (R4, a Full Load type, the job found): enabled only while the ONE
    gate allows it -- re-checked at the click, which never switches mode on a reason."""
    reason = _reload_gate(ctx)

    def _go() -> None:
        now = _reload_gate(ctx)
        if now:
            ui.notify(now, type="warning", multi_line=True)
            return
        close()
        open_exclusion_dialog(ui, ctx, mode="change_and_reload")

    with ui.row().classes("items-center gap-2 w-full"):
        button = ui.button("Change & reload", icon="sync", on_click=_go).props(
            "no-caps color=primary")
        if reason:
            button.props("disable")
    if reason:
        no_run = reason.startswith(("No Full Load run is recorded",
                                    "The record of the last Full Load run"))
        inline_hint(ui, reason, tone="warning" if no_run else "neutral")


def reload_tables(resolution, migratable: Sequence[str], convert: "Sequence[str]" = ()) -> list:
    """The tables a Change & reload drops and reloads: those in scope whose excluded
    columns change, plus the legacy tables ticked for conversion -- in tree order."""
    changed = set(resolution.diff.added) | set(resolution.diff.removed) if resolution.diff else set()
    wanted = (changed & set(resolution.in_scope)) | set(convert)
    return [t for t in migratable if t in wanted]


def _row_estimate(ctx: ExclusionDialogContext, table: str) -> Optional[int]:
    from dsql_migrator.ui.data_migration._cdc_status import _current_job

    job = _current_job(ctx.job_manager, getattr(ctx.migration_state, "job_id", None))
    counts = getattr(getattr(job, "watermark", None), "table_row_counts", None) or {}
    return counts.get(table)


def _build_reload_dialog(ui, ctx: ExclusionDialogContext, *, insert_lines=(), hint=None,
                         lob_filter=None, client=None):
    """CHANGE & RELOAD (R4): column rules editable, table lines E3 (the table set is fixed
    after a Full Load), and a negative confirm naming every table it DROPS and reloads --
    acting only on what it showed. A text-only change saves the text and nothing else."""
    from dsql_migrator.core.activity_log import ActivityCategory, ActivityStatus, log_activity

    state = ctx.migration_state
    classify_restored_exclusions(state, ctx.job_manager)
    inventory = ctx.inventory_fn()
    inputs = ctx.inputs_fn()
    if inventory is None or inputs is None:
        return None
    text, opened_with = popup_open_text(state, inventory)
    text = _with_inserted(text, insert_lines)
    token = state.exclusion_edit_token()
    held = {"text": text, "timer": None, "shown": None, "action": None}
    convert: set = set()
    view: dict = {}
    _apply_route_view(view, lob_filter)
    _add_css_once(ui, client)
    with ui.dialog().props("persistent") as dialog, ui.card().classes("gap-3").style(
        "width: min(980px, 94vw); max-width: 94vw"
    ):
        def _close() -> None:
            _cancel_timer(held)
            state.clear_size_check_listener(_run_preview_now)
            dialog.close()

        with ui.row().classes("items-center gap-2 w-full no-wrap"):
            ui.label("Change exclusions & reload").classes("text-base font-semibold")
            ui.space()
            ui.button(on_click=_close).props(
                "flat dense round size=sm icon=close color=grey-7"
            ).tooltip("Close without saving")
        ui.label("Column rules are editable. Tables are fixed after a Full Load (use Start "
                 "over to change them).").classes("text-xs text-gray-600")
        if hint:
            inline_hint(ui, hint[1], tone=hint[0])
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
            _render_lob_section(ui, ctx, inventory, inputs, resolution, view, _add_rules,
                                _run_preview_now, _go_to_line)
            _render_legacy_conversion(ui, resolution, convert, _run_preview_now)
            tables = reload_tables(resolution, inputs.migratable, sorted(convert))
            _render_drop_card(ui, ctx, resolution, tables)
            label = (f"Drop & reload {tables[0]}" if len(tables) == 1
                     else f"Drop & reload {len(tables)} tables" if tables else "Save")
            with ui.row().classes("w-full justify-end gap-2"):
                ui.button("Cancel", on_click=_close).props("flat no-caps color=grey-8")
                save = ui.button(label, on_click=_save).props(
                    "no-caps " + ("color=negative" if tables else "color=primary"))
                save.set_enabled(resolution.can_save)
            held["action"] = save
            held["shown"] = (held["text"], tuple(tables))

        def _run_preview_now() -> None:
            _cancel_timer(held)
            preview.refresh()

        def _go_to_line(n: int) -> None:
            ui.run_javascript(_go_to_line_js(editor, n))

        def _add_rules(lines: Sequence[str]) -> None:
            held["text"] = _with_inserted(held["text"], lines)
            editor.set_value(held["text"])
            _run_preview_now()

        def _on_edit(value: str) -> None:
            held["text"] = value or ""
            # The confirm names the tables it drops: until the preview caught up with this
            # text, there is nothing to confirm.
            if held.get("action") is not None:
                held["action"].set_enabled(False)
            _cancel_timer(held)
            held["timer"] = ui.timer(_DEBOUNCE_SECONDS, _run_preview_now, once=True)

        def _save() -> None:
            # FIRST the one gate, at the click: a reason writes nothing and keeps the text.
            reason = _reload_gate(ctx)
            if reason:
                ui.notify(reason, type="warning", multi_line=True)
                return
            resolution = _resolve(ctx, inventory, inputs, held["text"], slot="editor")
            tables = reload_tables(resolution, inputs.migratable, sorted(convert))
            # Act only on what the confirm SHOWED: a click that landed before the preview
            # caught up drops nothing.
            if not _shown_matches(held, tables):
                _run_preview_now()
                ui.notify("The rules changed since the tables to drop were listed: check the "
                          "list and click again.", type="warning")
                return
            # No await from here on: the store, the forced set, the confirm and the submit
            # happen in one step, so nothing can interleave.
            classify_restored_exclusions(state, ctx.job_manager)
            refused = state.set_column_exclusions(
                resolution, text=held["text"], opened_with=opened_with, token=token,
                convert_legacy=sorted(convert),
            )
            if refused:
                ui.notify(refused, type="warning", multi_line=True)
                return
            if not tables:
                _close()
                ctx.refresh()
                return
            state.set_exclusion_forced_replace({t: "Change & reload" for t in tables})
            state.set_overlay_disclosed(None)
            state.confirm_exclusion_forced_replace()
            _close()
            log_activity(
                ActivityCategory.FULL_LOAD, "excluded columns changed; reloading",
                status=ActivityStatus.INFO, target=", ".join(tables),
                detail=(resolution.diff.text if resolution.diff else "")
                + f"; dropping and reloading {len(tables)} table(s)",
            )
            run = ctx.extra.get("run_retry_for")
            if run is None:
                state.clear_exclusion_forced_replace()
                ui.notify("Open the Full Load step to reload the tables.", type="warning")
                return
            run(tables)
            if state.exclusion_forced_replace:
                # The retry refused before taking the forced set: never leave it armed.
                state.clear_exclusion_forced_replace()

        state.set_size_check_listener(_run_preview_now)
        preview()
    ui.timer(0.05, dialog.open, once=True)
    return dialog


def _render_legacy_conversion(ui, resolution, convert: set, rerun) -> None:
    """The opt-in conversion of omit-only (pre-0.1.562) entries: a ticked table joins the
    reload, after which its excluded columns are NULL like every other excluded column."""
    for table in sorted(resolution.retained_legacy):
        columns = ", ".join(sorted(resolution.retained_legacy[table]))

        def _toggle(event, t=table) -> None:
            if getattr(event, "value", False):
                convert.add(t)
            else:
                convert.discard(t)
            rerun()

        ui.checkbox(
            f"Reload {table} so its columns excluded before v0.1.562 ({columns}) are NULL "
            "(now they hold the target DEFAULT)",
            value=table in convert, on_change=_toggle,
        )


def _render_drop_card(ui, ctx: ExclusionDialogContext, resolution, tables) -> None:
    if not tables:
        inline_hint(ui, "No table's excluded columns change: Save stores the rules only.")
        return
    diff = resolution.diff
    with ui.column().classes("w-full gap-1 p-2 rounded-md border border-amber-200 bg-amber-50"):
        ui.label(f"{len(tables)} table{'s' if len(tables) != 1 else ''} will be DROPPED and "
                 "reloaded from the source").classes("text-sm font-semibold text-amber-800")
        for table in tables[:20]:
            added = (diff.added.get(table, ()) if diff else ())
            removed = (diff.removed.get(table, ()) if diff else ())
            parts = []
            if added:
                parts.append("+ " + ", ".join(added) + " (NULL)")
            if removed:
                parts.append("- " + ", ".join(removed))
            if table in resolution.retained_legacy and not parts:
                parts.append("excluded before v0.1.562 -> NULL")
            rows = _row_estimate(ctx, table)
            estimate = f"   {rows:,} rows re-read" if rows is not None else ""
            ui.label(f"{table}   {'; '.join(parts)}{estimate}").classes("text-xs font-mono")
        if len(tables) > 20:
            ui.label(f"and {len(tables) - 20} more").classes("text-xs")
        ui.label(
            "Their rows on Aurora DSQL are permanently deleted; each table is recreated from "
            "your Schema Conversion DDL, made nullable where needed, and reloaded. Other "
            "tables are not touched."
        ).classes("text-xs text-amber-800")


def _render_deployed_capture(ui, ctx: ExclusionDialogContext, inventory, *,
                             with_mismatch: bool = True) -> None:
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
    if not with_mismatch or ctx.capture_tables_fn is None or inventory is None or inputs is None:
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


def quarantine_route_block(ctx: ExclusionDialogContext) -> Optional[str]:
    """Why "Exclude column & reload" is disabled at render: only when it would open Change
    & reload and that gate refuses (an FK pass, a cut-over restore, not reconnected...).
    In tiers 0-2 it opens the read-only popup instead, so it is never disabled there."""
    lock = _lock_state(ctx)
    if lock.tier == 3 and lock.offers_change_and_reload:
        return _reload_gate(ctx)
    return None


def _lines_text(lines: Sequence[int]) -> str:
    return ", ".join(f"L{n}" for n in lines)


def open_exclusion_route(ui, ctx: ExclusionDialogContext, table: str, reason_text: str, *,
                         lob_candidates_for) -> None:
    """The quarantine "Exclude column & reload", routed by the lock at CLICK time:

    * Change & reload (R4, a Full Load type): the editor with outcome A's lines inserted
      (unsaved), or the NOT SAVED / B (section 4 filtered to the table) / C hint;
    * a lock above (tiers 0-2) or no Change & reload here: the read-only popup, with A's
      lines recorded as pending (deduped) and every other outcome recorded nowhere;
    * a click whose gate re-check refuses opens, stores and submits nothing: it says why
      and re-renders, so the button shows the current state.
    """
    state = ctx.migration_state
    inventory = ctx.inventory_fn()
    inputs = ctx.inputs_fn()
    if inventory is None or inputs is None:
        ui.notify("The source schema is not loaded yet.", type="warning")
        return
    lock = _lock_state(ctx)
    editable = lock.tier == 3 and lock.offers_change_and_reload
    if editable:
        reason = _reload_gate(ctx)
        if reason:
            ui.notify(reason, type="warning", multi_line=True)
            ctx.refresh()
            return
    text, _pending = popup_open_text(state, inventory)
    resolution = _resolve(ctx, inventory, inputs, text, slot="editor")
    keepers = {c: lines for (t, c), lines in resolution.kept_by.items() if t == table}
    by_line = {c: lines for (t, c), lines in resolution.excluded_by.items() if t == table}
    saved = effective_lob_exclusions(state, inventory).get(table, frozenset())
    candidates = tuple(lob_candidates_for(table) if lob_candidates_for else ())
    outcome = quarantine_rule_suggestion(
        table, reason_text, candidates,
        open_excluded=resolution.columns.get(table, frozenset()),
        saved_excluded=saved, include_keepers=keepers, open_excluded_by=by_line,
        source_type=session_source_type(ctx.session),
    )
    when = "after the Delete" if lock.tier in (0, 1, 2) else "once Change & reload is available"
    hint = _route_hint(outcome, table, text, editable, when)
    if editable or not lock.locked:
        open_exclusion_dialog(
            ui, ctx, mode="change_and_reload" if editable else "edit",
            insert_lines=outcome.lines if outcome.outcome == "A" else (), hint=hint,
            lob_filter=table if outcome.outcome == "B" and outcome.filter_table else None,
        )
        return
    if outcome.outcome == "A":
        for line in outcome.lines:
            state.add_pending_rule(line)
    open_exclusion_dialog(ui, ctx, mode="read_only", hint=hint)


def _route_hint(outcome, table: str, text: str, editable: bool, when: str):
    lines = text.split("\n")

    def _quote(numbers) -> str:
        return "; ".join(f'"{lines[n - 1].strip()}"' for n in numbers if 0 < n <= len(lines))

    if outcome.outcome == "A":
        notes = [f"{table}.{c} is kept by the include on {_lines_text(n)}: "
                 + ("edit or delete that line to exclude it." if editable
                    else f"{when}, change that line in Change & reload.")
                 for c, n in outcome.kept.items()]
        notes += [f"{table}.{c}: its name has a line break and cannot be written as a rule; "
                  "exclude the table, or edit the target DDL in Schema Conversion."
                  for c in outcome.unwritable]
        return ("neutral", " ".join(notes)) if notes else None
    if outcome.outcome == "NOT_SAVED":
        if editable:
            return ("neutral", f"Already excluded by {_lines_text(outcome.not_saved_lines)} "
                    f"(not saved yet) — Save to drop and reload {table}.")
        return ("neutral", f"{table}: already excluded by a line not saved yet "
                f"({_quote(outcome.not_saved_lines)}); {when}, Save it in Change & reload.")
    if outcome.outcome == "B":
        text_b = ("The database reported a type, not a column, so no rule was added: pick the "
                  "column that holds the oversized value with [+ add rule].")
        if not outcome.filter_table and outcome.not_saved_lines:
            text_b += (f" Every flagged column of {table} is already excluded by "
                       f"{_lines_text(outcome.not_saved_lines)} (not saved yet): Save to drop "
                       f"and reload {table}, or type a rule for a column this tool does not "
                       "flag.")
        if not editable:
            text_b += f" {when}, pick it in Change & reload."
        return ("warning", text_b)
    text_c = (f"Every column of {table} whose type this tool flags as oversized is already "
              "excluded by your saved rules, so the value the database rejected is in another "
              "column (for example JSON or spatial, which are not flagged). Type a rule for "
              f"that column: exclude {table}.<column>.")
    if not editable:
        text_c += f" {when}, type it in Change & reload."
    return ("warning", text_c)


DLQ_RULE_COMMENT = "# from the CDC dead-letter queue"


def open_dlq_route(ui, ctx: ExclusionDialogContext, table: str, columns: Sequence[str]) -> None:
    """The CDC dead-letter "Exclude column…": record the rule for each column the record
    names as a PENDING line (persisted; it never changes what is excluded by itself), then
    open the popup in the mode the lock gives: Change & reload when it can run, editable
    while nothing is locked (the line inserted), read-only otherwise -- its text is the
    lock's own steps. A record naming no column records nothing and opens the LOB section
    filtered to the table."""
    from dsql_migrator.core.exclusion_rules import rule_name_writable, rule_part_token

    state = ctx.migration_state
    schema, _sep, rest = table.partition(".")
    for column in columns:
        if all(rule_name_writable(x) for x in (schema, rest, column)):
            state.add_pending_rule(
                "exclude " + ".".join(rule_part_token(x) for x in (schema, rest, column))
                + f"  {DLQ_RULE_COMMENT}")
    lock = _lock_state(ctx)
    lob_filter = None if columns else table
    if lock.tier == 3 and lock.offers_change_and_reload and _reload_gate(ctx) is None:
        open_exclusion_dialog(ui, ctx, mode="change_and_reload", lob_filter=lob_filter)
    elif not lock.locked:
        open_exclusion_dialog(ui, ctx, mode="edit", lob_filter=lob_filter)
    else:
        open_exclusion_dialog(ui, ctx, mode="read_only")


__all__ = [
    "BUTTON_LABEL",
    "DLQ_RULE_COMMENT",
    "open_dlq_route",
    "open_exclusion_route",
    "quarantine_route_block",
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
