# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""A shared, reusable right slide-in AI chat drawer (Cloudscape-styled).

Both the Evaluation screen (per-object migration guidance) and the Schema
Conversion screen (per-object DDL conversion help) open the SAME chat drawer so
the AI-assistance experience is identical across the app: a fixed full-height
right panel, dismissed only by its X; a chat transcript whose assistant replies
stream in token-by-token; a composer for follow-up questions; a copy action; and
guardrails (a per-conversation turn limit and a per-message length cap). Topic
scoping and transcript length are enforced by the caller-supplied ``streamer``
(the core strategist builds the grounded system prompt and caps the transcript).

The drawer is intentionally screen-agnostic: :func:`build_chat_drawer` builds the
chrome once and returns an ``open_chat`` callable that each screen invokes with
its own title/subtitle, first question, streaming function, and an optional
footer action (e.g. Schema Conversion's "Use as target DDL", which pulls the
latest answer's SQL into the editable target). The ``streamer`` contract returns
any object exposing ``available`` / ``markdown`` / ``model_id`` / ``detail`` --
the core :class:`~dsql_migrator.core.assessment_strategist.ObjectGuidanceOutcome`
satisfies it -- so this module stays UI-only and testable in isolation.
"""

from __future__ import annotations

import inspect
import logging
import re
import threading
from typing import Callable, Optional

from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
from dsql_migrator.ui.design import render_notice

_LOGGER = logging.getLogger(__name__)

# Guardrails for the per-object AI chat. The conversation is intentionally
# bounded so it stays on-topic and cannot grow without limit: at most
# ``_MAX_CHAT_TURNS`` user turns per conversation (the first auto-question counts
# as one), each capped to ``_MAX_CHAT_INPUT_CHARS`` characters. (Topic scoping is
# enforced model-side via the caller's system prompt; the transcript sent to the
# model is separately length-capped in the core strategist.)
MAX_CHAT_TURNS = 10
MAX_CHAT_INPUT_CHARS = 1000

# A streaming chat turn: given the running transcript and a per-chunk callback,
# stream the reply and return an outcome (available/markdown/model_id/detail).
ChatStreamer = Callable[
    ["list[dict[str, str]]", Callable[[str], None]], ObjectGuidanceOutcome
]

# A COMPLETE fenced code block: optional language tag, newline, body, closing fence.
# Screen-agnostic (any language, not SQL) — the drawer stays generic. Only complete
# blocks match, so a truncated/unterminated trailing fence is left as prose.
_FENCE_RE = re.compile(r"```([\w+-]*)[ \t]*\r?\n(.*?)```", re.DOTALL)


def split_markdown_segments(md: str) -> "list[tuple[str, str, str]]":
    """Split markdown into ordered ("text"|"code", body, language) segments.

    Used to render a finished assistant reply so each fenced code block becomes its
    own component (with a per-block copy button) while surrounding prose stays
    markdown. Generic: matches any language tag (or none), never SQL-specific, so
    the shared drawer keeps no screen knowledge. An unterminated trailing fence is
    not matched and therefore falls through as text (no content is dropped). Pure
    and deterministic. Returns a single ("text", md, "") segment when there are no
    complete code blocks.
    """
    segments: list[tuple[str, str, str]] = []
    pos = 0
    for m in _FENCE_RE.finditer(md or ""):
        if m.start() > pos:
            segments.append(("text", md[pos : m.start()], ""))
        segments.append(("code", m.group(2), (m.group(1) or "").strip()))
        pos = m.end()
    tail = (md or "")[pos:]
    if tail or not segments:
        segments.append(("text", tail, ""))
    return segments


def markdown_has_code_block(md: str) -> bool:
    """True when ``md`` contains at least one COMPLETE fenced code block."""
    return _FENCE_RE.search(md or "") is not None


def chat_turns_remaining(
    messages: "list[dict[str, str]]", *, max_turns: int = MAX_CHAT_TURNS
) -> int:
    """Return how many more user turns the chat allows (0 when at the limit).

    Counts the ``user`` entries already in the transcript (the initial
    auto-question included) and subtracts from ``max_turns``, floored at 0. Pure
    and deterministic so the drawer's turn-limit gating is unit-testable.
    """
    used = sum(1 for entry in messages if (entry or {}).get("role") == "user")
    return max(0, max_turns - used)


def build_chat_drawer(ui: object) -> Callable[..., None]:
    """Build the shared chat drawer chrome once; return an ``open_chat`` opener.

    ``open_chat(*, title, subtitle, first_question, streamer, footer_label=None,
    footer_action=None, footer_visible=None)`` resets the transcript and starts a
    fresh conversation: it asks ``first_question`` automatically, then lets the user
    ask follow-ups via the composer (Enter or Send), each answered by ``streamer``
    with the full running transcript so the model keeps context. When
    ``footer_action`` is given, each assistant reply shows a ``footer_label`` button
    that calls ``footer_action(markdown)`` (e.g. to adopt the reply's SQL). An
    optional ``footer_visible(markdown) -> bool`` predicate gates that button per
    reply — the button is shown only when it returns True (e.g. hide "Test rewrite
    on target" when the reply proposes no runnable SQL). Returns a ``send(text)``
    callable to drive a follow-up turn programmatically.
    """
    # "Sticky bottom" state: while a reply streams we only auto-scroll if the user
    # is already near the bottom, so scrolling UP to read earlier text is not fought
    # by every stream tick. Updated by the scroll area's on_scroll below; starts
    # True so a fresh conversation follows the first reply.
    _scroll_state: dict[str, bool] = {"at_bottom": True}
    with ui.dialog().props("position=right persistent maximized=false") as dialog:  # type: ignore[attr-defined]
        # Wrap long content (code lines, unbroken identifiers) inside the chat
        # bubbles so the drawer never grows a horizontal/bottom scrollbar.
        ui.add_css(  # type: ignore[attr-defined]
            ".dsql-chat-md, .dsql-chat-md * { overflow-wrap: anywhere; "
            "word-break: break-word; max-width: 100%; }"
            ".dsql-chat-md pre, .dsql-chat-md code { white-space: pre-wrap; }"
            # Tame Markdown headings: the model sometimes emits '## The fix', which
            # the browser would render as a huge h1/h2 inside a small chat bubble.
            # Cap every heading to a compact, bold, chat-sized line (slightly larger
            # for h1>h2>h3) so a reply reads like chat, not a document, regardless of
            # what Markdown the model used. The base text stays the bubble's text-sm.
            ".dsql-chat-md h1, .dsql-chat-md h2, .dsql-chat-md h3, "
            ".dsql-chat-md h4, .dsql-chat-md h5, .dsql-chat-md h6 { "
            "font-weight: 600; line-height: 1.3; margin: 0.5rem 0 0.25rem; }"
            ".dsql-chat-md h1 { font-size: 1.05rem; }"
            ".dsql-chat-md h2 { font-size: 1rem; }"
            ".dsql-chat-md h3, .dsql-chat-md h4, .dsql-chat-md h5, "
            ".dsql-chat-md h6 { font-size: 0.9rem; }"
            # A short typed follow-up renders on a solid indigo bubble
            # (.dsql-chat-user); give its inline code a light panel with dark text so
            # it stays readable (default code styling would be white-on-indigo =
            # invisible). Rich user turns (with code blocks) use the soft light
            # bubble instead and get the neutral panel below.
            ".dsql-chat-user code { background: rgba(255,255,255,0.95); "
            "color: #1e293b; border-radius: 6px; padding: 1px 4px; }"
            # Code/plan blocks in any chat bubble: a calm neutral panel, subtle
            # border, comfortable padding — readable without shouting.
            ".dsql-chat-md pre { background: #f8fafc; border: 1px solid #e2e8f0; "
            "color: #1e293b; border-radius: 8px; padding: 8px 10px; margin: 4px 0; }"
        )
        with ui.card().classes(  # type: ignore[attr-defined]
            "full-height column no-wrap q-pa-none bg-gray-50"
        ).style("width: 660px; max-width: 96vw;"):
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
                ui.button(icon="close", on_click=dialog.close).props(  # type: ignore[attr-defined]
                    "flat dense round"
                ).tooltip("Close")
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
                    "AI suggestions are advisory. This chat stays scoped to this "
                    "object."
                ).classes("text-xs text-gray-400")

    # Current conversation (one object/topic at a time).
    conv: dict[str, object] = {
        "messages": [],
        "busy": False,
        "streamer": None,
        "footer_label": None,
        "footer_action": None,
        "footer_visible": None,
    }

    def _apply_composer_state() -> None:
        remaining = chat_turns_remaining(conv["messages"])  # type: ignore[arg-type]
        at_limit = remaining <= 0
        enabled = (not conv["busy"]) and (not at_limit) and conv["streamer"] is not None
        for el in (chat_input, send_btn):
            try:
                el.set_enabled(enabled)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                pass
        try:
            if at_limit:
                composer_hint.set_text(  # type: ignore[attr-defined]
                    "Conversation limit reached — close and reopen for a new chat."
                )
            elif conv["busy"]:
                composer_hint.set_text("")  # type: ignore[attr-defined]
            else:
                composer_hint.set_text(  # type: ignore[attr-defined]
                    f"{remaining} message(s) left in this conversation."
                )
        except Exception:  # noqa: BLE001
            pass

    def _set_busy(busy: bool) -> None:
        conv["busy"] = busy
        _apply_composer_state()

    def _autoscroll(force: bool = False) -> None:
        # Only follow the stream to the bottom when the user is already there
        # (sticky bottom) — unless ``force`` (a user action, e.g. sending a turn),
        # which should always reveal the newest message. A short smooth glide reads
        # more naturally than an instant jump on every stream tick.
        if not force and not _scroll_state.get("at_bottom", True):
            return
        try:
            scroll.scroll_to(percent=1.0, duration=0.15)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _user_bubble(text: str) -> None:
        with convo:  # type: ignore[attr-defined]
            with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                # Render as Markdown (not a plain label) so fenced ```sql / plan
                # code blocks show as readable monospace instead of literal backticks.
                # A short typed follow-up stays the compact solid-indigo pill; a rich
                # turn that carries code/plan blocks (e.g. the re-test result the
                # screen feeds back) uses a SOFT light bubble instead — a big solid
                # dark-indigo block wrapping a bright code panel reads heavy and
                # unnatural. Both are right-aligned so they still read as "from you".
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

    def _make_copy(state: dict, lock: "threading.Lock") -> Callable[[], object]:
        async def do_copy() -> None:
            with lock:
                text = str(state["text"])
                outcome = state["outcome"]
            payload = (
                outcome.markdown
                if isinstance(outcome, ObjectGuidanceOutcome) and outcome.available
                else text
            )
            if not payload.strip():
                ui.notify("Nothing to copy yet.", type="warning")  # type: ignore[attr-defined]
                return
            try:
                await ui.clipboard.write(payload)  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001
                ui.notify("Could not copy to clipboard.", type="negative")  # type: ignore[attr-defined]
                return
            ui.notify("Answer copied to clipboard", type="positive")  # type: ignore[attr-defined]

        return do_copy

    def _run_turn(user_text: str) -> None:
        text = (user_text or "").strip()
        streamer = conv["streamer"]
        if not text or conv["busy"] or streamer is None:
            return
        if chat_turns_remaining(conv["messages"]) <= 0:  # type: ignore[arg-type]
            _apply_composer_state()
            return
        _set_busy(True)
        conv["messages"].append({"role": "user", "text": text})  # type: ignore[attr-defined]
        messages_snapshot = [dict(m) for m in conv["messages"]]  # type: ignore[attr-defined]
        footer_label = conv["footer_label"]
        footer_action = conv["footer_action"]
        footer_visible = conv["footer_visible"]
        # Posting a turn is a user action — always reveal it and re-arm sticky
        # bottom so the incoming reply is followed until the user scrolls up.
        _scroll_state["at_bottom"] = True
        _user_bubble(text)

        state: dict[str, object] = {"text": "", "done": False, "outcome": None}
        lock = threading.Lock()

        def on_delta(delta: str) -> None:
            with lock:
                state["text"] = str(state["text"]) + delta

        def worker() -> None:
            try:
                outcome = streamer(messages_snapshot, on_delta)  # type: ignore[misc]
            except Exception:  # noqa: BLE001 - never break the page
                _LOGGER.exception("AI chat turn failed")
                outcome = ObjectGuidanceOutcome(
                    available=False,
                    reason="UNAVAILABLE",
                    detail=(
                        "Generating a reply failed unexpectedly. Try again, or "
                        "check the Bedrock model/region on the Connect screen."
                    ),
                )
            with lock:
                state["outcome"] = outcome
                state["done"] = True

        with convo:  # type: ignore[attr-defined]
            with ui.row().classes("items-start gap-2 w-full no-wrap"):  # type: ignore[attr-defined]
                ui.icon("auto_awesome", color="indigo-6").classes(  # type: ignore[attr-defined]
                    "text-xl q-mt-xs"
                )
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
                        ui.label("AI is writing…").classes(  # type: ignore[attr-defined]
                            "text-xs text-gray-500"
                        )
                    actions = ui.row().classes("items-center gap-1")  # type: ignore[attr-defined]
                    with actions:  # type: ignore[attr-defined]
                        ui.button(  # type: ignore[attr-defined]
                            icon="content_copy", on_click=_make_copy(state, lock)
                        ).props("flat dense round size=sm color=grey-7").tooltip(
                            "Copy answer"
                        )
                        meta = ui.label("").classes("text-xs text-gray-400")  # type: ignore[attr-defined]

        _autoscroll(force=True)  # reveal the just-posted turn + typing indicator
        threading.Thread(target=worker, daemon=True).start()

        timer_box: dict[str, object] = {}

        def tick() -> None:
            with lock:
                text_now = str(state["text"])
                done = bool(state["done"])
                outcome = state["outcome"]
            try:
                if not answer_md.is_deleted:  # type: ignore[attr-defined]
                    answer_md.set_content(text_now)  # type: ignore[attr-defined]
                _autoscroll()
            except Exception:  # noqa: BLE001 - drawer may have been closed
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
                        ui,
                        tone="warning",
                        header="AI reply unavailable",
                        body=outcome.detail,
                    )
                meta.set_text("")  # type: ignore[attr-defined]
                msgs = conv["messages"]  # type: ignore[attr-defined]
                if msgs and msgs[-1].get("role") == "user":  # type: ignore[attr-defined]
                    msgs.pop()  # type: ignore[attr-defined]
            elif isinstance(outcome, ObjectGuidanceOutcome) and outcome.available:
                # Finalize the reply. This done branch runs exactly once (the timer
                # is already deactivated above), so it is safe to build per-block
                # controls here — unlike the streaming ticks, which replace the whole
                # markdown subtree every ~120ms. When the reply contains a fenced code
                # block, re-render it as segments: prose stays ui.markdown, and each
                # code block becomes a ui.code, which ships its OWN copy button (so
                # the recommended query is one click to copy). Text-only replies keep
                # the simple single-markdown path (no churn).
                if markdown_has_code_block(outcome.markdown):
                    answer_md.set_content("")  # type: ignore[attr-defined]
                    with bubble:  # type: ignore[attr-defined]
                        for kind, body, lang in split_markdown_segments(
                            outcome.markdown
                        ):
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
                    f"Generated by model {outcome.model_id}"
                    if outcome.model_id
                    else ""
                )
                conv["messages"].append(  # type: ignore[attr-defined]
                    {"role": "assistant", "text": outcome.markdown}
                )
                # Optional screen-supplied action on the reply (e.g. adopt SQL, or
                # re-test a rewrite). Only shown when there IS an action AND — if the
                # screen supplied a ``footer_visible`` predicate — that predicate says
                # this particular reply is actionable. This lets the AI DBA hide
                # "Test rewrite on target" when a reply proposes no runnable SQL
                # (e.g. it concluded the query is already efficient).
                show_footer = footer_action is not None and bool(footer_label)
                if show_footer and callable(footer_visible):
                    try:
                        show_footer = bool(footer_visible(outcome.markdown))
                    except Exception:  # noqa: BLE001 - a bad predicate must not break the reply
                        show_footer = False
                if show_footer:
                    answer = outcome.markdown

                    async def _do_footer(_e=None, _answer=answer) -> None:
                        # footer_action may be sync (e.g. adopt SQL) or async (e.g.
                        # re-test the rewrite on the target, which does off-loop
                        # I/O); await it when it returns an awaitable.
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
        if not text or conv["busy"] or chat_turns_remaining(conv["messages"]) <= 0:  # type: ignore[arg-type]
            return
        try:
            chat_input.set_value("")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        _run_turn(text)

    send_btn.on("click", lambda _e=None: _send())  # type: ignore[attr-defined]
    chat_input.on("keydown.enter", lambda _e=None: _send())  # type: ignore[attr-defined]

    def open_chat(
        *,
        title: str,
        subtitle: str,
        first_question: str,
        streamer: ChatStreamer,
        footer_label: Optional[str] = None,
        footer_action: Optional[Callable[[str], None]] = None,
        footer_visible: Optional[Callable[[str], bool]] = None,
    ) -> Callable[[str], None]:
        conv["messages"] = []
        conv["busy"] = False
        conv["streamer"] = streamer
        conv["footer_label"] = footer_label
        conv["footer_action"] = footer_action
        conv["footer_visible"] = footer_visible
        title_label.set_text(title)  # type: ignore[attr-defined]
        subtitle_label.set_text(subtitle)  # type: ignore[attr-defined]
        convo.clear()  # type: ignore[attr-defined]
        try:
            chat_input.set_value("")  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        _set_busy(False)
        dialog.open()  # type: ignore[attr-defined]
        _run_turn(first_question)

        # Return a sender so the opening screen can drive a follow-up turn
        # programmatically (e.g. feed a re-test's before/after DPU numbers back so
        # the SAME assistant explains the improvement in-thread). It goes through
        # _run_turn like a typed message, so the reply is delivered BY THE AI and
        # stays within the turn limit; a no-op once the conversation is at its cap.
        def send_turn(text: str) -> None:
            _run_turn(text)

        return send_turn

    return open_chat


__all__ = [
    "MAX_CHAT_TURNS",
    "MAX_CHAT_INPUT_CHARS",
    "ChatStreamer",
    "chat_turns_remaining",
    "build_chat_drawer",
]
