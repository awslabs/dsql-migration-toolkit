# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Shared, screen-agnostic AI-chat helpers (streaming-reply rendering + guardrails).

The per-screen right slide-in drawer this module once built has been superseded by
the persistent, app-wide panel (:mod:`dsql_migrator.ui.ai_panel`), into which every
screen now deep-links. What remains here are the pure, UI-only helpers the panel
reuses -- fenced-code segmentation for a finished reply, the per-message-length
constant, and the ``ChatStreamer`` contract (a streamer returns any object exposing
``available`` / ``markdown`` / ``model_id`` / ``detail`` -- the core
:class:`~dsql_migrator.core.assessment_strategist.ObjectGuidanceOutcome` satisfies
it). Kept a leaf module (no NiceGUI import) so the helpers stay unit-testable.
"""

from __future__ import annotations

import re
from typing import Callable

from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome

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



__all__ = [
    "MAX_CHAT_TURNS",
    "MAX_CHAT_INPUT_CHARS",
    "ChatStreamer",
    "chat_turns_remaining",
    "split_markdown_segments",
    "markdown_has_code_block",
]
