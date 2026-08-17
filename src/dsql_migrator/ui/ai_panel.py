# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""The persistent, app-wide AI assistant panel (Cloudscape-styled right drawer).

Unlike the per-screen chat drawer it replaced (a modal dialog that reset its
transcript on every open), this is ONE panel wired at the app shell
(:func:`~dsql_migrator.ui.workflow.build_workflow_sidebar`) that
persists across the whole migration journey. It can be opened/closed at any time
from the header toggle, and the conversation is preserved: the transcript, active
scope, and open/closed state live on the session (``SessionConnectionState.
ai_conversation``, the SOURCE OF TRUTH the panel renders FROM), so they survive
closing/reopening the panel, navigating between steps, and a browser refresh (the
session id is cookie-stable). Nothing here is durably persisted and it holds no
credentials or row data (Property 7).

Each screen deep-links into the panel with :meth:`AiPanelHandle.open_scope`
(instead of building its own drawer): it sets the active subject (labels + a
context chip), the streamer that answers, and an optional seed question / footer
action. Switching scope inserts a visible divider and starts a fresh grounding
window -- the streamer is only ever given the CURRENT scope's turns (so a reply is
grounded on the object at hand), while the full transcript stays visible as
scrollback. Deterministic-first: replies are advisory and grounded by the
caller-supplied system prompt (the core strategist).

Screens also mirror MAJOR migration actions into the panel via
:meth:`AiPanelHandle.post_event` (Start Full Load, Apply schema, Run validation,
Cut over, ...), so the AI window reflects what you do in the UI as a live activity
feed. These events are deterministic (no model call), rendered as distinct timeline
strips, and the most recent ones are folded into the next chat turn as grounding
context -- so the assistant is aware of what the operator just did. Credential-free
and row-data-free (Property 7).
"""

from __future__ import annotations

import inspect
import logging
import threading
from collections import deque
from typing import Callable, Optional

from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
from dsql_migrator.core.models import AiScope, MigrationContext
from dsql_migrator.ui.ai_chat_drawer import (
    MAX_CHAT_INPUT_CHARS,
    ChatStreamer,
    markdown_has_code_block,
    split_markdown_segments,
)
from dsql_migrator.ui.design import (
    render_activity_event,
    render_notice,
    render_segmented_bar,
)

_LOGGER = logging.getLogger(__name__)

# Width of the persistent side panel. Set wide (≈50% wider than the original 440px)
# so an AI conversation -- prose, tables, and multi-line code blocks -- reads
# comfortably without wrapping every few words; this is the width the old modal chat
# used too. It does push the main content further left, but readability of the
# assistant's answers won the trade-off (an explicit product choice).
_PANEL_WIDTH_PX = 660

# Activity-event styling lives in the design system (ACTIVITY_EVENT_STYLE /
# render_activity_event) so the feed shares the app's one Cloudscape palette; the
# ``status`` a caller passes to post_event IS the design-system tone key
# (started / running / success / info / warning / error).

# How many of the most RECENT activity events (session-wide) are folded into a chat
# turn as grounding context, so the assistant is aware of what the operator just did
# in the tool. Kept deliberately SMALL: only the last few actions are relevant to the
# current question, and the point is to bound the text SENT to the model (only useful
# info, nothing excessive). Events are short one-liners; the transcript is trimmed
# separately in the strategist.
_MAX_GROUNDED_EVENTS = 6


class AiPanelHandle:
    """The shell's handle to the persistent AI panel.

    Screens use :meth:`open_scope` to deep-link (a chat scope) and
    :meth:`post_event` to mirror a major action into the activity feed; the header
    toggle uses :meth:`set_visible` / :meth:`toggle`; gating uses :meth:`is_enabled`.
    """

    def __init__(
        self,
        *,
        open_scope: Callable[..., Callable[[str], None]],
        post_event: Callable[..., None],
        set_visible: Callable[[bool], None],
        toggle: Callable[[], None],
        is_enabled: Callable[[], bool],
        is_visible: Callable[[], bool],
    ) -> None:
        self.open_scope = open_scope
        self.post_event = post_event
        self.set_visible = set_visible
        self.toggle = toggle
        self.is_enabled = is_enabled
        self.is_visible = is_visible


def _panel_css(ui: object) -> None:
    """Register the chat bubble styling once (same tokens as the old drawer)."""
    ui.add_css(  # type: ignore[attr-defined]
        ".dsql-chat-md, .dsql-chat-md * { overflow-wrap: anywhere; "
        "word-break: break-word; max-width: 100%; }"
        ".dsql-chat-md pre, .dsql-chat-md code { white-space: pre-wrap; }"
        ".dsql-chat-md h1, .dsql-chat-md h2, .dsql-chat-md h3, "
        ".dsql-chat-md h4, .dsql-chat-md h5, .dsql-chat-md h6 { "
        "font-weight: 600; line-height: 1.3; margin: 0.5rem 0 0.25rem; }"
        ".dsql-chat-md h1 { font-size: 1.05rem; }"
        ".dsql-chat-md h2 { font-size: 1rem; }"
        ".dsql-chat-md h3, .dsql-chat-md h4, .dsql-chat-md h5, "
        ".dsql-chat-md h6 { font-size: 0.9rem; }"
        ".dsql-chat-user code { background: rgba(255,255,255,0.95); "
        "color: #1e293b; border-radius: 6px; padding: 1px 4px; }"
        ".dsql-chat-md pre { background: #f8fafc; border: 1px solid #e2e8f0; "
        "color: #1e293b; border-radius: 8px; padding: 8px 10px; margin: 4px 0; }"
    )


def build_ai_panel(
    ui: object,
    *,
    state: object,
    get_context: Optional[Callable[[], MigrationContext]] = None,
    general_streamer_factory: Optional[Callable[[], Optional[ChatStreamer]]] = None,
    on_change: Optional[Callable[[], None]] = None,
) -> AiPanelHandle:
    """Build the persistent AI panel once and return its :class:`AiPanelHandle`.

    ``state`` is the session's :class:`~dsql_migrator.ui.session.
    SessionConnectionState`; the panel reads/writes ``state.ai_conversation`` as its
    source of truth (so the transcript + open/closed state survive close/reopen,
    navigation, and refresh) and gates itself on ``state.ai_assist.enabled``.
    ``get_context`` (optional) returns the current :class:`MigrationContext` for the
    baseline context chip shown when no object scope is active.
    """
    conversation = state.ai_conversation  # type: ignore[attr-defined]

    # Live (in-memory, NOT persisted) state for the ACTIVE turn machinery: the
    # streamer + footer action are callables that cannot be serialized, so after a
    # browser refresh they are gone (the transcript still restores from the session,
    # and the composer stays disabled until a screen re-activates a scope). The
    # grounding window (`scope_start`) is the index in the transcript where the
    # current scope began -- the streamer is only given messages from there on.
    conv: dict[str, object] = {
        "busy": False,
        "streamer": None,
        "footer_label": None,
        "footer_action": None,
        "footer_visible": None,
        "scope_start": len(conversation.messages),
        # Count of activity events posted while the panel was closed -- shown as a
        # badge on the right-edge reopen tab so an action is noticed even with the
        # panel hidden. Reset to 0 when the panel is opened.
        "unseen": 0,
        # Set by the Stop button to end the in-flight streaming turn early (the
        # streaming tick reads it, keeps the partial reply, and re-enables the composer).
        "stop_requested": False,
        # Thread-safe queue of activity events awaiting render. post_event may be
        # called from a BACKGROUND job thread (e.g. an assessment/schema/validation
        # run finishing), and NiceGUI can only build elements safely on the event
        # loop -- so post_event only appends here (+ to the session), and a loop timer
        # (_drain_events) renders them. Mirrors how the streaming tick renders.
        "pending": deque(),
    }
    _scroll_state: dict[str, bool] = {"at_bottom": True}

    _panel_css(ui)
    drawer = ui.right_drawer(  # type: ignore[attr-defined]
        value=bool(conversation.visible), bordered=True, elevated=False
    ).classes("bg-gray-50 q-pa-none").props(f"width={_PANEL_WIDTH_PX} :breakpoint=0")

    with drawer:  # type: ignore[attr-defined]
        with ui.column().classes("full-height column no-wrap w-full"):  # type: ignore[attr-defined]
            with ui.row().classes(  # type: ignore[attr-defined]
                "items-center gap-2 no-wrap w-full q-px-md q-py-sm bg-white "
                "border-b border-gray-200"
            ):
                ui.icon("auto_awesome", color="indigo-6").classes("text-2xl")  # type: ignore[attr-defined]
                with ui.column().classes("col gap-0 min-w-0"):  # type: ignore[attr-defined]
                    title_label = ui.label("AI assistant").classes(  # type: ignore[attr-defined]
                        "text-base font-semibold leading-tight"
                    )
                    subtitle_label = ui.label("").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-500 ellipsis"
                    )
                # A right-collapsing chevron (not an X): the panel is only HIDDEN, not
                # closed/discarded -- the conversation stays and the right-edge tab (and
                # header button) bring it back. An X read as "end/discard this".
                ui.button(
                    icon="chevron_right", on_click=lambda: _set_visible(False)
                ).props("flat dense round").tooltip("Hide the AI assistant")  # type: ignore[attr-defined]
            # Context chip: what the assistant is grounded on right now.
            chip_label = ui.label("").classes(  # type: ignore[attr-defined]
                "text-xs text-indigo-700 bg-indigo-50 border-b border-indigo-100 "
                "q-px-md q-py-xs w-full ellipsis"
            )
            scroll = ui.scroll_area(  # type: ignore[attr-defined]
                on_scroll=lambda e: _scroll_state.__setitem__(
                    "at_bottom", (e.vertical_percentage or 0) >= 0.95
                )
            ).classes("col").style("width: 100%")
            with scroll:  # type: ignore[attr-defined]
                convo = ui.column().classes("w-full gap-3 q-pa-md")  # type: ignore[attr-defined]
            with ui.column().classes(  # type: ignore[attr-defined]
                "w-full q-px-md q-py-sm bg-white border-t border-gray-200 gap-1"
            ):
                # A roomy, multi-line composer (like a real chat window), not a
                # single-line field: a follow-up here is often a pasted DDL/SQL
                # snippet or a few sentences. `autogrow` starts it a few lines tall
                # (min-height -- Quasar forces rows=1 under autogrow, so the starting
                # height has to come from CSS) and GROWS it with what you type, capped
                # at max-height so a long paste scrolls inside the box (Quasar switches
                # the textarea to overflow:auto once content exceeds max-height) rather
                # than pushing the send button off-screen. `items-end` keeps the round
                # send button pinned to the bottom-right corner as the box grows.
                with ui.row().classes("items-end gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                    chat_input = (  # type: ignore[attr-defined]
                        ui.textarea(placeholder="Ask about this migration…")
                        .props(
                            f"outlined autogrow maxlength={MAX_CHAT_INPUT_CHARS} "
                            'input-style="min-height: 4.5rem; max-height: 35vh; '
                            'font-size: 0.875rem"'
                        )
                        .classes("col")
                    )
                    send_btn = ui.button(icon="send").props(  # type: ignore[attr-defined]
                        "round dense color=indigo-6"
                    ).tooltip("Send  (Shift+Enter for a new line)")
                    # Shown only WHILE a reply is streaming (see _apply_composer_state):
                    # stops the turn, keeping whatever text arrived so far.
                    stop_btn = ui.button(icon="stop").props(  # type: ignore[attr-defined]
                        "round dense color=grey-7"
                    ).tooltip("Stop generating")
                    stop_btn.set_visibility(False)  # type: ignore[attr-defined]
                with ui.row().classes(  # type: ignore[attr-defined]
                    "items-center justify-between no-wrap w-full gap-2"
                ):
                    composer_hint = ui.label("").classes("text-xs text-gray-400 col")  # type: ignore[attr-defined]
                    # A live character counter that appears only as you near the cap
                    # (Cloudscape style), turning red at the limit. Hidden otherwise so
                    # it does not clutter the composer.
                    char_counter = ui.label("").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-400 no-wrap"
                    )
                    char_counter.set_visibility(False)  # type: ignore[attr-defined]
                ui.label(  # type: ignore[attr-defined]
                    "AI suggestions are advisory. Replies are grounded on the current "
                    "step's deterministic facts."
                ).classes("text-xs text-gray-400")
                # The Bedrock model currently answering, shown persistently under the
                # composer as a labeled indigo CHIP (not faint gray text) so it is easy
                # to spot. The per-reply "Generated by model X" is retrospective; this
                # tells you up front which model you are talking to. Reads the live
                # AI-assist config on build and on every open.
                model_row = ui.row().classes(  # type: ignore[attr-defined]
                    "items-center gap-1.5 no-wrap self-start rounded "
                    "bg-indigo-50 border border-indigo-100 q-px-sm q-py-xs max-w-full"
                )
                with model_row:  # type: ignore[attr-defined]
                    ui.icon("smart_toy", color="indigo-6").classes("text-sm")  # type: ignore[attr-defined]
                    ui.label("Model").classes(  # type: ignore[attr-defined]
                        "text-xs font-semibold text-indigo-700 no-wrap"
                    )
                    model_value = ui.label("").classes(  # type: ignore[attr-defined]
                        "text-xs text-indigo-700 ellipsis"
                    )

    # Collapsed-state affordance: a small tab pinned to the RIGHT EDGE so that, when
    # the panel is closed, there is a VISIBLE way to reopen it (not only the header
    # button). Shown only while the panel is closed AND AI is enabled; hidden when the
    # panel is open (the drawer would cover it) or AI is off.
    reopen_tab = (
        ui.button("AI", icon="auto_awesome", on_click=lambda: _set_visible(True))  # type: ignore[attr-defined]
        .props("dense color=indigo-6 no-caps")
        .style(
            "position: fixed; right: 0; top: 50%; transform: translateY(-50%); "
            "z-index: 2000; border-top-right-radius: 0; border-bottom-right-radius: 0; "
            "min-width: 0; padding: 12px 6px; box-shadow: -2px 0 8px rgba(0,0,0,0.15);"
        )
        .tooltip("Open the AI assistant")
    )
    # Unseen-activity badge on the reopen tab: when the panel is closed and a major
    # action posts an event, this shows the count so the action is noticed.
    with reopen_tab:  # type: ignore[attr-defined]
        unseen_badge = ui.badge("").props("floating color=red rounded")  # type: ignore[attr-defined]
    unseen_badge.set_visibility(False)  # type: ignore[attr-defined]

    def _sync_reopen_tab() -> None:
        enabled = bool(getattr(state.ai_assist, "enabled", False))  # type: ignore[attr-defined]
        collapsed = (not conversation.visible) and enabled
        try:
            reopen_tab.set_visibility(collapsed)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        unseen = int(conv.get("unseen", 0) or 0)
        try:
            if collapsed and unseen > 0:
                unseen_badge.set_text(str(unseen) if unseen < 100 else "99+")  # type: ignore[attr-defined]
                unseen_badge.set_visibility(True)  # type: ignore[attr-defined]
            else:
                unseen_badge.set_visibility(False)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    # --- helpers ----------------------------------------------------------------

    def _baseline_chip() -> str:
        """The context chip when no object scope is active (where you are)."""
        if get_context is None:
            return ""
        try:
            ctx = get_context()
        except Exception:  # noqa: BLE001 - a bad context must never break the panel
            return ""
        bits = [b for b in (ctx.current_step, ctx.migration_type) if b]
        return "  ·  ".join(bits)

    def _ensure_general_scope() -> None:
        """Give the panel a GENERAL (whole-migration) streamer when it is opened with
        no specific object scope, so the composer is usable straight from the header
        toggle -- "ask anything about this migration".

        The general streamer is grounded on the current MigrationContext and carries
        the same domain guardrail as the per-object chats (migration-only, declines
        off-topic). It is reconstructable, so this also restores a usable composer for
        the general scope after a browser refresh (a screen-specific scope's live
        streamer is not reconstructable and stays disabled until re-deep-linked).
        """
        if general_streamer_factory is None:
            return
        active = conversation.active_scope
        if active is not None and active.scope_id != "general":
            return  # a specific screen scope is active -- never override it
        if active is not None and conv["streamer"] is not None:
            return  # the general scope is already live
        streamer = general_streamer_factory()
        if streamer is None:
            return
        conv["streamer"] = streamer
        if active is None:
            conversation.active_scope = AiScope(
                scope_id="general", title="AI assistant", chip=_baseline_chip()
            )
            conv["scope_start"] = len(conversation.messages)
        try:
            title_label.set_text("AI assistant")  # type: ignore[attr-defined]
            subtitle_label.set_text("")  # type: ignore[attr-defined]
            chip_label.set_text(_baseline_chip())  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _apply_composer_state() -> None:
        # No per-conversation turn CAP: the guardrail is domain-scoping (the system
        # prompt keeps replies to this migration and declines off-topic) + a bounded
        # CONTEXT (the strategist trims the transcript to a char budget per call, so
        # cost stays bounded no matter how long the chat runs) -- not an arbitrary
        # "10 questions" wall. The composer is enabled whenever a streamer is active
        # and no turn is in flight.
        enabled = (not conv["busy"]) and conv["streamer"] is not None
        for el in (chat_input, send_btn):
            try:
                el.set_enabled(enabled)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass
        # The Stop button replaces Send while a reply is streaming.
        try:
            stop_btn.set_visibility(bool(conv["busy"]))  # type: ignore[attr-defined]
            send_btn.set_visibility(not bool(conv["busy"]))  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        try:
            if conv["streamer"] is None:
                composer_hint.set_text(  # type: ignore[attr-defined]
                    "Open AI from a step (e.g. an object's AI Assist) to start or "
                    "continue a conversation."
                )
            else:
                composer_hint.set_text("")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _set_busy(busy: bool) -> None:
        conv["busy"] = busy
        _apply_composer_state()

    def _notify_change() -> None:
        # Persist the session (the callback is dirty-checked, so this is cheap) so the
        # transcript survives an unexpected app restart. ONLY call from the event loop
        # (turn completion / drain timer / visibility toggle) -- never a job thread.
        if on_change is None:
            return
        try:
            on_change()
        except Exception:  # noqa: BLE001 - persistence is best-effort, never break UI
            pass

    def _refresh_model_line() -> None:
        """Update the connected-model chip under the composer (hidden when AI is off)."""
        try:
            cfg = state.ai_assist  # type: ignore[attr-defined]
            model_id = str(getattr(cfg, "model_id", "") or "")
            if not bool(getattr(cfg, "enabled", False)) or not model_id:
                model_row.set_visibility(False)  # type: ignore[attr-defined]
                return
            region = str(getattr(cfg, "region", "") or "")
            model_value.set_text(  # type: ignore[attr-defined]
                f"{model_id}  ·  {region}" if region else model_id
            )
            model_row.set_visibility(True)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _update_counter() -> None:
        """Show a character counter as the composer nears the per-message cap."""
        try:
            n = len(str(getattr(chat_input, "value", "") or ""))
        except Exception:  # noqa: BLE001
            return
        try:
            if n >= int(MAX_CHAT_INPUT_CHARS * 0.8):
                char_counter.set_text(f"{n}/{MAX_CHAT_INPUT_CHARS}")  # type: ignore[attr-defined]
                char_counter.set_visibility(True)  # type: ignore[attr-defined]
                if n >= MAX_CHAT_INPUT_CHARS:
                    char_counter.classes(add="text-red-600", remove="text-gray-400")  # type: ignore[attr-defined]
                else:
                    char_counter.classes(add="text-gray-400", remove="text-red-600")  # type: ignore[attr-defined]
            else:
                char_counter.set_visibility(False)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _set_visible(visible: bool) -> None:
        # Opening with no object scope -> activate the general "ask anything about
        # this migration" streamer so the composer is immediately usable.
        if visible:
            _ensure_general_scope()
            conv["unseen"] = 0  # opening the panel clears the unseen-activity badge
            _refresh_model_line()  # the connected model may have changed since build
        conversation.visible = bool(visible)
        try:
            (drawer.show if visible else drawer.hide)()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            try:
                drawer.value = bool(visible)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass
        _sync_reopen_tab()
        _apply_composer_state()
        _notify_change()  # persist open/closed state (reopens where left after restart)

    def _autoscroll(force: bool = False) -> None:
        if not force and not _scroll_state.get("at_bottom", True):
            return
        try:
            scroll.scroll_to(percent=1.0, duration=0.15)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _divider(text: str) -> None:
        with convo:  # type: ignore[attr-defined]
            with ui.row().classes("items-center gap-2 w-full q-my-xs"):  # type: ignore[attr-defined]
                ui.element("div").classes("col border-t border-gray-200")  # type: ignore[attr-defined]
                ui.label(text).classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-400 no-wrap"
                )
                ui.element("div").classes("col border-t border-gray-200")  # type: ignore[attr-defined]

    def _render_assessment_event(text: str, data: dict) -> None:
        """A compact VISUAL for a completed assessment: a short headline + a
        proportional Automatic/Review/Unsupported bar + a count legend (no wall of
        text). ``text`` remains the plain grounding line the AI sees."""
        total = int(data.get("total", 0) or 0)
        conflicts = int(data.get("conflicts", 0) or 0)
        with convo:  # type: ignore[attr-defined]
            with ui.card().classes(  # type: ignore[attr-defined]
                "w-full bg-white border border-gray-200 rounded-md !shadow-none "
                "q-pa-sm gap-2"
            ):
                with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                    ui.icon("fact_check", color="indigo-6").classes("text-base")  # type: ignore[attr-defined]
                    ui.label(  # type: ignore[attr-defined]
                        f"Assessment complete — {total} object"
                        + ("" if total == 1 else "s")
                    ).classes("text-xs font-semibold text-gray-800")
                render_segmented_bar(
                    ui,
                    segments=[
                        ("Automatic", int(data.get("auto", 0) or 0), "ok"),
                        ("Review", int(data.get("manual", 0) or 0), "warning"),
                        ("Unsupported", int(data.get("unsupported", 0) or 0), "bad"),
                    ],
                )
                if conflicts:
                    ui.label(  # type: ignore[attr-defined]
                        f"{conflicts} object" + ("" if conflicts == 1 else "s")
                        + " already exist on the target"
                    ).classes("text-xs text-gray-500")

    def _event_entry(
        text: str, status: str = "info", kind: str = "", data: object = None
    ) -> None:
        """Render one activity event. A known ``kind`` with structured ``data`` gets a
        custom VISUAL (e.g. the assessment breakdown bar); everything else renders as
        the design system's tinted timeline chip, visually distinct from the bubbles."""
        if kind == "assessment" and isinstance(data, dict):
            _render_assessment_event(text, data)
            return
        with convo:  # type: ignore[attr-defined]
            render_activity_event(ui, text, tone=status)

    def _user_bubble(text: str) -> None:
        with convo:  # type: ignore[attr-defined]
            with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                has_code = "```" in text
                if has_code:
                    classes = (
                        "text-sm text-gray-800 bg-indigo-50 border border-indigo-100 "
                        "rounded-2xl rounded-tr-sm px-3 py-2 dsql-chat-md"
                    )
                    style = "max-width: 92%; overflow-wrap: anywhere"
                else:
                    classes = (
                        "text-sm text-white bg-indigo-600 rounded-2xl rounded-tr-sm "
                        "px-3 py-2 dsql-chat-md dsql-chat-user"
                    )
                    style = "max-width: 85%; overflow-wrap: anywhere"
                ui.markdown(text).classes(classes).style(style)  # type: ignore[attr-defined]

    def _assistant_bubble_final(markdown: str, *, model_id: str = "") -> None:
        """Render a FINISHED assistant reply (used by replay + on turn completion)."""
        with convo:  # type: ignore[attr-defined]
            # No leading avatar icon: the reply is a full-width card so the answer uses
            # the panel's whole width. The icon column only ate horizontal space and
            # narrowed every reply.
            with ui.column().classes("w-full gap-1 min-w-0"):  # type: ignore[attr-defined]
                bubble = ui.card().classes(  # type: ignore[attr-defined]
                    "w-full bg-white border border-gray-200 rounded-2xl "
                    "!shadow-none q-pa-md"
                )
                with bubble:  # type: ignore[attr-defined]
                    if markdown_has_code_block(markdown):
                        for kind, body, lang in split_markdown_segments(markdown):
                            if kind == "code":
                                ui.code(  # type: ignore[attr-defined]
                                    body.rstrip("\n"), language=lang or None
                                ).classes("w-full dsql-chat-md")
                            elif body.strip():
                                ui.markdown(body).classes(  # type: ignore[attr-defined]
                                    "text-sm w-full dsql-chat-md"
                                )
                    else:
                        ui.markdown(markdown).classes(  # type: ignore[attr-defined]
                            "text-sm w-full dsql-chat-md"
                        )
                if model_id:
                    ui.label(f"Generated by model {model_id}").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-400"
                    )

    def _replay() -> None:
        """Rebuild the transcript UI from the session (survives refresh/reconnect)."""
        for entry in conversation.messages:
            role = (entry or {}).get("role")
            text = (entry or {}).get("text", "")
            if role == "user":
                _user_bubble(text)
            elif role == "assistant":
                _assistant_bubble_final(text)
            elif role == "event":
                _event_entry(
                    text,
                    str((entry or {}).get("status", "info")),
                    kind=str((entry or {}).get("kind", "")),
                    data=(entry or {}).get("data"),
                )

    def _drain_events() -> None:
        """Render any events queued by ``post_event`` -- runs ON THE LOOP (a timer), so
        it is safe to build NiceGUI elements here even though post_event may have been
        called from a background job thread. Also bumps the unseen-badge when closed."""
        queue = conv.get("pending")
        if not queue:
            return
        rendered = False
        while True:
            try:
                text, status, kind, data = queue.popleft()  # type: ignore[union-attr]
            except IndexError:
                break
            try:
                _event_entry(text, status, kind=kind, data=data)
            except Exception:  # noqa: BLE001 - a bad event must not break the panel
                pass
            if not conversation.visible:
                conv["unseen"] = int(conv.get("unseen", 0) or 0) + 1
            rendered = True
        if rendered:
            _sync_reopen_tab()
            _autoscroll()
            _notify_change()  # persist the newly-recorded events (crash-durable)

    def _run_turn(user_text: str) -> None:
        text = (user_text or "").strip()
        streamer = conv["streamer"]
        if not text or conv["busy"] or streamer is None:
            return
        conv["stop_requested"] = False  # clear any stale flag from a prior turn
        _set_busy(True)
        conversation.messages.append({"role": "user", "text": text})
        # Ground the streamer on the CURRENT scope only: send the turns since this
        # scope began, not the whole cross-scope scrollback.
        scope_start = int(conv["scope_start"])  # type: ignore[arg-type]
        # The model only ever sees user/assistant TURNS within the current scope's
        # window (the streamer maps any other role to "user", which would corrupt the
        # alternation). Activity events are folded in SEPARATELY as grounding context,
        # from the WHOLE session (not just the scope) so a reply is aware of what the
        # operator just did anywhere in the tool.
        messages_snapshot = [
            dict(m)
            for m in conversation.messages[scope_start:]
            if (m or {}).get("role") in ("user", "assistant")
        ]
        recent_events = [
            str((m or {}).get("text", ""))
            for m in conversation.messages
            if (m or {}).get("role") == "event"
        ][-_MAX_GROUNDED_EVENTS:]
        if recent_events and messages_snapshot:
            ctx = (
                "(Context — recent actions the operator performed in the migration "
                "tool:\n" + "\n".join(f"- {e}" for e in recent_events) + "\n)"
            )
            messages_snapshot[-1] = {
                **messages_snapshot[-1],
                "text": f"{ctx}\n\n{messages_snapshot[-1].get('text', '')}",
            }
        footer_label = conv["footer_label"]
        footer_action = conv["footer_action"]
        footer_visible = conv["footer_visible"]
        _scroll_state["at_bottom"] = True
        _user_bubble(text)

        state_box: dict[str, object] = {"text": "", "done": False, "outcome": None}
        lock = threading.Lock()

        def on_delta(delta: str) -> None:
            with lock:
                state_box["text"] = str(state_box["text"]) + delta

        def worker() -> None:
            try:
                outcome = streamer(messages_snapshot, on_delta)  # type: ignore[misc]
            except Exception:  # noqa: BLE001 - never break the page
                _LOGGER.exception("AI panel turn failed")
                outcome = ObjectGuidanceOutcome(
                    available=False,
                    reason="UNAVAILABLE",
                    detail=(
                        "Generating a reply failed unexpectedly. Try again, or check "
                        "the Bedrock model/region on the Connect screen."
                    ),
                )
            with lock:
                state_box["outcome"] = outcome
                state_box["done"] = True

        with convo:  # type: ignore[attr-defined]
            # Full-width reply, no leading avatar icon (matches _assistant_bubble_final).
            with ui.column().classes("w-full gap-1 min-w-0"):  # type: ignore[attr-defined]
                bubble = ui.card().classes(  # type: ignore[attr-defined]
                    "w-full bg-white border border-gray-200 rounded-2xl "
                    "!shadow-none q-pa-md"
                )
                with bubble:  # type: ignore[attr-defined]
                    answer_md = ui.markdown("").classes(  # type: ignore[attr-defined]
                        "text-sm w-full dsql-chat-md"
                    )
                typing = ui.row().classes("items-center gap-2 pl-1")  # type: ignore[attr-defined]
                with typing:  # type: ignore[attr-defined]
                    ui.spinner(type="dots", size="md", color="indigo-6")  # type: ignore[attr-defined]
                    ui.label("AI is writing…").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
                actions = ui.row().classes("items-center gap-1")  # type: ignore[attr-defined]
                with actions:  # type: ignore[attr-defined]
                    meta = ui.label("").classes("text-xs text-gray-400")  # type: ignore[attr-defined]

        _autoscroll(force=True)
        threading.Thread(target=worker, daemon=True).start()

        timer_box: dict[str, object] = {}

        def tick() -> None:
            with lock:
                text_now = str(state_box["text"])
                done = bool(state_box["done"])
                outcome = state_box["outcome"]
            try:
                if not answer_md.is_deleted:  # type: ignore[attr-defined]
                    answer_md.set_content(text_now)  # type: ignore[attr-defined]
                _autoscroll()
            except Exception:  # noqa: BLE001 - panel may have been closed
                _stop = timer_box.get("timer")
                if _stop is not None:
                    _stop.active = False  # type: ignore[attr-defined]
                return
            if not done:
                if not bool(conv.get("stop_requested")):
                    return
                # Stop pressed mid-stream: end the turn now, keeping whatever text
                # arrived. (The background Bedrock stream drains and is discarded --
                # boto3 cannot be interrupted mid-iteration -- but the turn ends here.)
                _t = timer_box.get("timer")
                if _t is not None:
                    _t.active = False  # type: ignore[attr-defined]
                conv["stop_requested"] = False
                try:
                    typing.set_visibility(False)  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001
                    pass
                partial = text_now.strip()
                if not partial:
                    try:
                        answer_md.set_content("_(stopped)_")  # type: ignore[attr-defined]
                    except Exception:  # noqa: BLE001
                        pass
                # Persist the partial as the assistant turn so it survives refresh and
                # keeps the user/assistant alternation valid for the next turn.
                conversation.messages.append(
                    {"role": "assistant", "text": partial or "_(stopped)_"}
                )
                try:
                    meta.set_text("Stopped")  # type: ignore[attr-defined]
                except Exception:  # noqa: BLE001
                    pass
                _set_busy(False)
                _autoscroll()
                _notify_change()  # persist the stopped (partial) turn
                return
            _t = timer_box.get("timer")
            if _t is not None:
                _t.active = False  # type: ignore[attr-defined]
            try:
                typing.set_visibility(False)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass
            if isinstance(outcome, ObjectGuidanceOutcome) and not outcome.available:
                bubble.clear()  # type: ignore[attr-defined]
                with bubble:  # type: ignore[attr-defined]
                    render_notice(
                        ui, tone="warning", header="AI reply unavailable",
                        body=outcome.detail,
                    )
                meta.set_text("")  # type: ignore[attr-defined]
                # Roll back the user turn so a retry is not blocked / mis-grounded.
                msgs = conversation.messages
                if msgs and msgs[-1].get("role") == "user":
                    msgs.pop()
            elif isinstance(outcome, ObjectGuidanceOutcome) and outcome.available:
                if markdown_has_code_block(outcome.markdown):
                    answer_md.set_content("")  # type: ignore[attr-defined]
                    with bubble:  # type: ignore[attr-defined]
                        for kind, body, lang in split_markdown_segments(outcome.markdown):
                            if kind == "code":
                                ui.code(  # type: ignore[attr-defined]
                                    body.rstrip("\n"), language=lang or None
                                ).classes("w-full dsql-chat-md")
                            elif body.strip():
                                ui.markdown(body).classes(  # type: ignore[attr-defined]
                                    "text-sm w-full dsql-chat-md"
                                )
                else:
                    answer_md.set_content(outcome.markdown)  # type: ignore[attr-defined]
                meta.set_text(  # type: ignore[attr-defined]
                    f"Generated by model {outcome.model_id}" if outcome.model_id else ""
                )
                conversation.messages.append(
                    {"role": "assistant", "text": outcome.markdown}
                )
                show_footer = footer_action is not None and bool(footer_label)
                if show_footer and callable(footer_visible):
                    try:
                        show_footer = bool(footer_visible(outcome.markdown))
                    except Exception:  # noqa: BLE001
                        show_footer = False
                if show_footer:
                    answer = outcome.markdown

                    async def _do_footer(_e=None, _answer=answer) -> None:
                        maybe = footer_action(_answer)  # type: ignore[misc]
                        if inspect.isawaitable(maybe):
                            await maybe

                    with actions:  # type: ignore[attr-defined]
                        ui.button(  # type: ignore[attr-defined]
                            str(footer_label), on_click=_do_footer
                        ).props("flat dense no-caps color=indigo-6 icon=download_done")
            _set_busy(False)
            _autoscroll()
            _notify_change()  # persist the completed turn (crash-durable transcript)

        timer_box["timer"] = ui.timer(0.12, tick)  # type: ignore[attr-defined]

    def _send() -> None:
        text = (getattr(chat_input, "value", "") or "").strip()  # type: ignore[attr-defined]
        if not text or conv["busy"]:
            return
        try:
            chat_input.set_value("")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        _run_turn(text)

    def _stop_generation() -> None:
        # End the in-flight streaming turn (the tick loop finalizes the partial reply).
        if conv["busy"]:
            conv["stop_requested"] = True

    send_btn.on("click", lambda _e=None: _send())  # type: ignore[attr-defined]
    stop_btn.on("click", lambda _e=None: _stop_generation())  # type: ignore[attr-defined]
    # Enter sends; Shift+Enter inserts a newline (the multi-line textarea composer).
    # NiceGUI's modifier set has no `.exact`, so the plain-vs-shift distinction is made
    # client-side: this js_handler suppresses the browser's default newline and emits to
    # the server ONLY when Shift is not held -- Shift+Enter falls through to the default
    # and adds a line. (`emit` is in scope inside the handler; see nicegui.js.)
    chat_input.on(  # type: ignore[attr-defined]
        "keydown.enter",
        lambda _e=None: _send(),
        js_handler="(e) => { if (!e.shiftKey) { e.preventDefault(); emit(); } }",
    )
    # Keep the character counter in sync as the composer's value changes.
    chat_input.on("update:model-value", lambda _e=None: _update_counter())  # type: ignore[attr-defined]

    # --- public API -------------------------------------------------------------

    def post_event(
        *,
        text: str,
        status: str = "info",
        kind: str = "",
        data: object = None,
    ) -> None:
        """Record a deterministic ACTIVITY event and mirror it into the panel feed.

        Called by screens when a MAJOR migration action starts/finishes (Start Full
        Load, Apply schema, Run validation, Cut over, ...). It appends an ``event``
        entry to the transcript (the session source of truth, so it survives a
        refresh), renders it in the feed, and -- when the panel is closed -- bumps the
        reopen-tab unseen badge. It NEVER calls the model; the ``text`` is folded into a
        later chat turn as grounding context instead (see ``_run_turn``). A no-op when
        AI is disabled (the panel is inert then). The caller must pass credential-free,
        row-data-free text (Property 7); ``status`` is one of the design tones
        (started / success / info / warning / error).

        ``kind`` + ``data`` opt into a richer VISUAL for the event (e.g.
        ``kind="assessment"`` with a counts dict renders a breakdown bar instead of a
        plain chip). ``data`` must be a credential-free, row-data-free dict of numbers/
        statuses; ``text`` stays the plain fallback the AI is grounded on.
        """
        if not bool(getattr(state.ai_assist, "enabled", False)):  # type: ignore[attr-defined]
            return
        clean = str(text or "").strip()
        if not clean:
            return
        st = str(status or "info")
        entry: dict[str, object] = {"role": "event", "text": clean, "status": st}
        if kind:
            entry["kind"] = str(kind)
        if isinstance(data, dict):
            entry["data"] = dict(data)
        # Record on the session (source of truth) and QUEUE for the loop timer to
        # render. No UI is built here -- post_event is often called from a background
        # job thread (assessment/schema/validation runs), and NiceGUI elements must be
        # built on the event loop (see _drain_events). list.append + deque.append are
        # both thread-safe.
        conversation.messages.append(entry)
        conv["pending"].append(  # type: ignore[union-attr]
            (clean, st, str(kind), dict(data) if isinstance(data, dict) else None)
        )

    def open_scope(
        *,
        scope_id: str,
        title: str,
        subtitle: str = "",
        chip: str = "",
        streamer: ChatStreamer,
        seed_question: Optional[str] = None,
        footer_label: Optional[str] = None,
        footer_action: Optional[Callable[[str], None]] = None,
        footer_visible: Optional[Callable[[str], bool]] = None,
    ) -> Callable[[str], None]:
        """Deep-link the panel to a subject: set the streamer + labels, then open.

        The transcript is NEVER reset (session/info preserved). When ``scope_id``
        differs from the active scope, a divider is inserted, the grounding window
        advances (the streamer only sees turns from here on), and -- if
        ``seed_question`` is given -- that question is asked automatically. Re-opening
        the SAME scope just re-focuses the panel without re-asking. Returns a
        ``send(text)`` callable to drive a follow-up programmatically.
        """
        conv["streamer"] = streamer
        conv["footer_label"] = footer_label
        conv["footer_action"] = footer_action
        conv["footer_visible"] = footer_visible
        active = conversation.active_scope
        is_new_scope = active is None or active.scope_id != scope_id
        if is_new_scope:
            conversation.active_scope = AiScope(
                scope_id=scope_id, title=title, subtitle=subtitle, chip=chip
            )
            conv["scope_start"] = len(conversation.messages)
            if conversation.messages:
                _divider(f"Now: {title}" if title else "New topic")
        try:
            title_label.set_text(title or "AI assistant")  # type: ignore[attr-defined]
            subtitle_label.set_text(subtitle)  # type: ignore[attr-defined]
            chip_label.set_text(chip or _baseline_chip())  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        _set_visible(True)
        _set_busy(False)
        _autoscroll(force=True)
        if is_new_scope and seed_question:
            _run_turn(seed_question)
        return _run_turn

    def set_visible(visible: bool) -> None:
        _set_visible(visible)

    def toggle() -> None:
        _set_visible(not bool(conversation.visible))

    def is_enabled() -> bool:
        return bool(getattr(state.ai_assist, "enabled", False))  # type: ignore[attr-defined]

    def is_visible() -> bool:
        return bool(conversation.visible)

    # Restore the transcript + chip from the session (open/close, nav, refresh).
    _replay()
    try:
        active = conversation.active_scope
        if active is not None:
            title_label.set_text(active.title or "AI assistant")  # type: ignore[attr-defined]
            subtitle_label.set_text(active.subtitle)  # type: ignore[attr-defined]
            chip_label.set_text(active.chip or _baseline_chip())  # type: ignore[attr-defined]
        else:
            chip_label.set_text(_baseline_chip())  # type: ignore[attr-defined]
    except Exception:  # noqa: BLE001
        pass
    # If the panel is restored OPEN (e.g. after a browser refresh / app restart) with
    # no live streamer, activate the general "ask anything" scope now so the composer
    # is usable immediately -- otherwise the drawer shows (and replays the transcript +
    # renders new activity events) but the input stays disabled with no way to start a
    # chat. The general streamer is reconstructable; a specific screen scope's is not,
    # so a restored object scope still waits for a re-open from its screen.
    if bool(conversation.visible):
        _ensure_general_scope()
    _apply_composer_state()
    _sync_reopen_tab()
    _refresh_model_line()
    _autoscroll(force=True)
    # Loop timer that renders events posted by post_event (possibly from a background
    # job thread). Low frequency -- it just checks a deque and renders any new events.
    ui.timer(0.25, _drain_events)  # type: ignore[attr-defined]

    return AiPanelHandle(
        open_scope=open_scope,
        post_event=post_event,
        set_visible=set_visible,
        toggle=toggle,
        is_enabled=is_enabled,
        is_visible=is_visible,
    )


__all__ = ["AiPanelHandle", "build_ai_panel"]
