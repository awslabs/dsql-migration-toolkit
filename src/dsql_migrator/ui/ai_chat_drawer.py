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

import logging
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
    footer_action=None)`` resets the transcript and starts a fresh conversation:
    it asks ``first_question`` automatically, then lets the user ask follow-ups
    via the composer (Enter or Send), each answered by ``streamer`` with the full
    running transcript so the model keeps context. When ``footer_action`` is
    given, each available assistant reply shows a ``footer_label`` button that
    calls ``footer_action(markdown)`` (e.g. to adopt the reply's SQL).
    """
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
            scroll = ui.scroll_area().classes("col").style("width: 100%")  # type: ignore[attr-defined]
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

    def _autoscroll() -> None:
        try:
            scroll.scroll_to(percent=1.0, duration=0.0)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass

    def _user_bubble(text: str) -> None:
        with convo:  # type: ignore[attr-defined]
            with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                ui.label(text).classes(  # type: ignore[attr-defined]
                    "text-sm text-white bg-indigo-600 rounded-2xl rounded-tr-sm "
                    "px-3 py-2"
                ).style("max-width: 85%; overflow-wrap: anywhere")

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

        _autoscroll()
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
                answer_md.set_content(outcome.markdown)  # type: ignore[attr-defined]
                meta.set_text(  # type: ignore[attr-defined]
                    f"Generated by model {outcome.model_id}"
                    if outcome.model_id
                    else ""
                )
                conv["messages"].append(  # type: ignore[attr-defined]
                    {"role": "assistant", "text": outcome.markdown}
                )
                # Optional screen-supplied action on the reply (e.g. adopt SQL).
                if footer_action is not None and footer_label:
                    answer = outcome.markdown

                    def _do_footer(_e=None, _answer=answer) -> None:
                        footer_action(_answer)  # type: ignore[misc]

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
    ) -> None:
        conv["messages"] = []
        conv["busy"] = False
        conv["streamer"] = streamer
        conv["footer_label"] = footer_label
        conv["footer_action"] = footer_action
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

    return open_chat


__all__ = [
    "MAX_CHAT_TURNS",
    "MAX_CHAT_INPUT_CHARS",
    "ChatStreamer",
    "chat_turns_remaining",
    "build_chat_drawer",
]
