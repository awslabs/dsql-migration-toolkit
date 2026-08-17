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
"""

from __future__ import annotations

import inspect
import logging
import threading
from typing import Callable, Optional

from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
from dsql_migrator.core.models import AiScope, MigrationContext
from dsql_migrator.ui.ai_chat_drawer import (
    MAX_CHAT_INPUT_CHARS,
    ChatStreamer,
    markdown_has_code_block,
    split_markdown_segments,
)
from dsql_migrator.ui.design import render_notice

_LOGGER = logging.getLogger(__name__)

# Width of the persistent side panel. Narrower than the old 660px modal dialog
# (a modal could be wide; a panel that sits beside the content should not eat the
# whole page) while still leaving room for code blocks (which wrap internally).
_PANEL_WIDTH_PX = 440


class AiPanelHandle:
    """The shell's handle to the persistent AI panel.

    Screens use :meth:`open_scope` to deep-link; the header toggle uses
    :meth:`set_visible` / :meth:`toggle`; gating uses :meth:`is_enabled`.
    """

    def __init__(
        self,
        *,
        open_scope: Callable[..., Callable[[str], None]],
        set_visible: Callable[[bool], None],
        toggle: Callable[[], None],
        is_enabled: Callable[[], bool],
        is_visible: Callable[[], bool],
    ) -> None:
        self.open_scope = open_scope
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
                with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                    chat_input = (  # type: ignore[attr-defined]
                        ui.input(placeholder="Ask a follow-up…")
                        .props(f"dense outlined rounded maxlength={MAX_CHAT_INPUT_CHARS}")
                        .classes("col")
                    )
                    send_btn = ui.button(icon="send").props(  # type: ignore[attr-defined]
                        "round dense color=indigo-6"
                    ).tooltip("Send")
                composer_hint = ui.label("").classes("text-xs text-gray-400")  # type: ignore[attr-defined]
                ui.label(  # type: ignore[attr-defined]
                    "AI suggestions are advisory. Replies are grounded on the current "
                    "step's deterministic facts."
                ).classes("text-xs text-gray-400")

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

    def _sync_reopen_tab() -> None:
        enabled = bool(getattr(state.ai_assist, "enabled", False))  # type: ignore[attr-defined]
        try:
            reopen_tab.set_visibility((not conversation.visible) and enabled)  # type: ignore[attr-defined]
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

    def _set_visible(visible: bool) -> None:
        # Opening with no object scope -> activate the general "ask anything about
        # this migration" streamer so the composer is immediately usable.
        if visible:
            _ensure_general_scope()
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
            with ui.row().classes("items-start gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.icon("auto_awesome", color="indigo-6").classes("text-xl q-mt-xs")  # type: ignore[attr-defined]
                with ui.column().classes("col gap-1 min-w-0"):  # type: ignore[attr-defined]
                    bubble = ui.card().classes(  # type: ignore[attr-defined]
                        "w-full bg-white border border-gray-200 rounded-2xl "
                        "rounded-tl-sm !shadow-none q-pa-md"
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

    def _run_turn(user_text: str) -> None:
        text = (user_text or "").strip()
        streamer = conv["streamer"]
        if not text or conv["busy"] or streamer is None:
            return
        _set_busy(True)
        conversation.messages.append({"role": "user", "text": text})
        # Ground the streamer on the CURRENT scope only: send the turns since this
        # scope began, not the whole cross-scope scrollback.
        scope_start = int(conv["scope_start"])  # type: ignore[arg-type]
        messages_snapshot = [dict(m) for m in conversation.messages[scope_start:]]
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
            with ui.row().classes("items-start gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.icon("auto_awesome", color="indigo-6").classes("text-xl q-mt-xs")  # type: ignore[attr-defined]
                with ui.column().classes("col gap-1 min-w-0"):  # type: ignore[attr-defined]
                    bubble = ui.card().classes(  # type: ignore[attr-defined]
                        "w-full bg-white border border-gray-200 rounded-2xl "
                        "rounded-tl-sm !shadow-none q-pa-md"
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

    send_btn.on("click", lambda _e=None: _send())  # type: ignore[attr-defined]
    chat_input.on("keydown.enter", lambda _e=None: _send())  # type: ignore[attr-defined]

    # --- public API -------------------------------------------------------------

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
    _apply_composer_state()
    _sync_reopen_tab()
    _autoscroll(force=True)

    return AiPanelHandle(
        open_scope=open_scope,
        set_visible=set_visible,
        toggle=toggle,
        is_enabled=is_enabled,
        is_visible=is_visible,
    )


__all__ = ["AiPanelHandle", "build_ai_panel"]
