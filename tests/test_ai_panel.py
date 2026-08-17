# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the persistent app-wide AI panel (ui/ai_panel.py).

The panel renders FROM the session's ``ai_conversation`` (the source of truth), so
the transcript + open/closed state survive closing/reopening the panel, navigating
between steps, and a browser refresh. These tests drive it with a minimal NiceGUI
double + a fake, synchronous ChatStreamer, and pump the streaming timer manually.
"""
from __future__ import annotations

import time
from typing import Optional

from dsql_migrator.core.assessment_strategist import ObjectGuidanceOutcome
from dsql_migrator.core.models import AiScope
from dsql_migrator.ui.ai_panel import build_ai_panel
from dsql_migrator.ui.session import SessionConnectionState


# ---------------------------------------------------------------------------
# Minimal NiceGUI double
# ---------------------------------------------------------------------------


class _El:
    """A fake NiceGUI element: chainable, context-managing, records text/content."""

    def __init__(self, ui: "_Ui", kind: str, text: str = "") -> None:
        self._ui = ui
        self.kind = kind
        self.text = text
        self.content = text if kind in ("markdown", "code") else ""
        self.value = ""
        self.enabled = True
        self.visible = True
        self.is_deleted = False
        if kind in ("markdown", "code"):
            ui.rendered.append((kind, text))

    # chainable no-ops
    def classes(self, *_a, **_k) -> "_El":
        return self

    def style(self, *_a, **_k) -> "_El":
        return self

    def props(self, *_a, **_k) -> "_El":
        return self

    def tooltip(self, *_a, **_k) -> "_El":
        return self

    def on(self, *_a, **_k) -> "_El":
        return self

    def on_click(self, *_a, **_k) -> "_El":
        return self

    # context manager (with ui.row(): ...)
    def __enter__(self) -> "_El":
        return self

    def __exit__(self, *_exc) -> bool:
        return False

    # mutators the panel calls
    def set_text(self, text: str) -> None:
        self.text = text

    def set_content(self, content: str) -> None:
        self.content = content
        self._ui.rendered.append(("set_content", content))

    def set_value(self, value: str) -> None:
        self.value = value

    def set_enabled(self, enabled: bool) -> None:
        self.enabled = enabled

    def set_visibility(self, visible: bool) -> None:
        self.visible = visible

    def clear(self) -> None:
        pass

    def scroll_to(self, *_a, **_k) -> None:
        pass

    # right_drawer show/hide
    def show(self) -> None:
        self._ui.drawer_visible = True

    def hide(self) -> None:
        self._ui.drawer_visible = False


class _Timer:
    def __init__(self, interval: float, cb) -> None:  # noqa: ANN001
        self.interval = interval
        self.cb = cb
        self.active = True


class _Ui:
    """A minimal NiceGUI stand-in recording what the panel builds."""

    def __init__(self) -> None:
        self.rendered: list[tuple[str, str]] = []  # (markdown|code|set_content, body)
        self.timers: list[_Timer] = []
        self.drawer_visible: Optional[bool] = None
        self.buttons: list[_El] = []

    def add_css(self, *_a, **_k) -> None:
        pass

    def notify(self, *_a, **_k) -> None:
        pass

    def _el(self, kind: str, text: str = "") -> _El:
        return _El(self, kind, text)

    def right_drawer(self, *_a, value: bool = False, **_k) -> _El:  # noqa: ANN001
        self.drawer_visible = bool(value)
        return self._el("right_drawer")

    def column(self, *_a, **_k) -> _El:
        return self._el("column")

    def row(self, *_a, **_k) -> _El:
        return self._el("row")

    def card(self, *_a, **_k) -> _El:
        return self._el("card")

    def scroll_area(self, *_a, **_k) -> _El:
        return self._el("scroll_area")

    def label(self, text: str = "", *_a, **_k) -> _El:
        return self._el("label", text)

    def icon(self, *_a, **_k) -> _El:
        return self._el("icon")

    def button(self, *a, **_k) -> _El:  # noqa: ANN002
        text = a[0] if a and isinstance(a[0], str) else ""
        el = _El(self, "button", text)
        self.buttons.append(el)
        return el

    def input(self, *_a, **_k) -> _El:
        return self._el("input")

    def markdown(self, text: str = "", *_a, **_k) -> _El:
        return self._el("markdown", text)

    def code(self, text: str = "", *_a, **_k) -> _El:
        return self._el("code", text)

    def spinner(self, *_a, **_k) -> _El:
        return self._el("spinner")

    def element(self, *_a, **_k) -> _El:
        return self._el("element")

    def timer(self, interval: float, cb) -> _Timer:  # noqa: ANN001
        t = _Timer(interval, cb)
        self.timers.append(t)
        return t


def _make_streamer(reply: str = "The answer.", record: Optional[list] = None):
    """A synchronous fake ChatStreamer: streams ``reply`` and returns it available."""

    def streamer(messages, on_delta):  # noqa: ANN001
        if record is not None:
            record.append([dict(m) for m in messages])
        on_delta(reply)
        return ObjectGuidanceOutcome(
            available=True, reason="OK", detail="", markdown=reply, model_id="fake-model"
        )

    return streamer


def _pump(ui: _Ui, *, max_iters: int = 200) -> None:
    """Drive the most recent streaming timer until its turn completes."""
    if not ui.timers:
        return
    timer = ui.timers[-1]
    for _ in range(max_iters):
        if not timer.active:
            return
        timer.cb()
        if not timer.active:
            return
        time.sleep(0.003)


def _enabled_state() -> SessionConnectionState:
    state = SessionConnectionState()
    state.ai_assist.enabled = True
    return state


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_is_enabled_reflects_ai_assist_toggle() -> None:
    state = SessionConnectionState()
    panel = build_ai_panel(_Ui(), state=state)
    assert panel.is_enabled() is False  # opt-in, default off
    state.ai_assist.enabled = True
    assert panel.is_enabled() is True


def test_visibility_persists_to_session() -> None:
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    assert state.ai_conversation.visible is False
    panel.set_visible(True)
    assert state.ai_conversation.visible is True and ui.drawer_visible is True
    panel.toggle()
    assert state.ai_conversation.visible is False and ui.drawer_visible is False


def test_open_scope_seeds_and_persists_the_conversation() -> None:
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    sent: list = []
    panel.open_scope(
        scope_id="eval:orders", title="orders", chip="Evaluation",
        streamer=_make_streamer("Use IDENTITY.", record=sent),
        seed_question="How do I convert orders?",
    )
    _pump(ui)
    msgs = state.ai_conversation.messages
    # Seed question + assistant reply are recorded on the SESSION (source of truth).
    assert [m["role"] for m in msgs] == ["user", "assistant"]
    assert msgs[0]["text"] == "How do I convert orders?"
    assert msgs[1]["text"] == "Use IDENTITY."
    assert state.ai_conversation.active_scope.scope_id == "eval:orders"
    assert state.ai_conversation.visible is True
    # The streamer saw the current scope's turn (the seed question).
    assert sent and sent[0][0]["text"] == "How do I convert orders?"


def test_scope_switch_preserves_transcript_and_reground_window() -> None:
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.open_scope(
        scope_id="eval:orders", title="orders",
        streamer=_make_streamer("A1"), seed_question="Q1",
    )
    _pump(ui)
    # Switch to a NEW scope: the prior turns REMAIN (session/info preserved), and a
    # divider is inserted.
    second_sent: list = []
    panel.open_scope(
        scope_id="valid:items", title="items",
        streamer=_make_streamer("A2", record=second_sent), seed_question="Q2",
    )
    _pump(ui)
    roles = [m["role"] for m in state.ai_conversation.messages]
    assert roles == ["user", "assistant", "user", "assistant"]  # nothing reset
    texts = [m["text"] for m in state.ai_conversation.messages]
    assert texts == ["Q1", "A1", "Q2", "A2"]
    assert state.ai_conversation.active_scope.scope_id == "valid:items"
    # Grounding window advanced: the second scope's streamer saw ONLY its own turn,
    # not the orders conversation.
    assert second_sent and [m["text"] for m in second_sent[0]] == ["Q2"]


def test_reopening_same_scope_does_not_reseed() -> None:
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.open_scope(
        scope_id="eval:orders", title="orders",
        streamer=_make_streamer("A1"), seed_question="Q1",
    )
    _pump(ui)
    # Re-open the SAME scope (e.g. the user clicked the button again): no new seed turn.
    panel.open_scope(
        scope_id="eval:orders", title="orders",
        streamer=_make_streamer("A1"), seed_question="Q1",
    )
    _pump(ui)
    assert [m["text"] for m in state.ai_conversation.messages] == ["Q1", "A1"]


def test_replay_restores_transcript_from_session() -> None:
    # A browser refresh rebuilds the panel; it must restore the transcript + scope
    # from the (cookie-stable) session rather than starting empty.
    state = _enabled_state()
    state.ai_conversation.messages.extend([
        {"role": "user", "text": "prior question"},
        {"role": "assistant", "text": "prior answer"},
    ])
    state.ai_conversation.active_scope = AiScope(scope_id="eval:orders", title="orders")
    state.ai_conversation.visible = True
    ui = _Ui()
    build_ai_panel(ui, state=state)
    bodies = [body for _kind, body in ui.rendered]
    assert "prior question" in bodies and "prior answer" in bodies
    assert ui.drawer_visible is True  # reopened where the user left it


def test_disabled_panel_reports_not_enabled_and_stays_hidden() -> None:
    state = SessionConnectionState()  # AI off
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    assert panel.is_enabled() is False
    assert state.ai_conversation.visible is False
    assert ui.drawer_visible is False


def _reopen_tab(ui: _Ui) -> _El:
    tabs = [b for b in ui.buttons if b.text == "AI"]
    assert len(tabs) == 1, "expected exactly one right-edge 'AI' reopen tab"
    return tabs[0]


def test_reopen_tab_shows_when_collapsed_and_hides_when_open() -> None:
    state = _enabled_state()  # AI on, panel closed by default
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    tab = _reopen_tab(ui)
    assert tab.visible is True  # collapsed + enabled -> the expand tab is shown
    panel.set_visible(True)
    assert tab.visible is False  # open -> tab hidden (drawer covers the edge)
    panel.set_visible(False)
    assert tab.visible is True  # collapsed again -> tab back


def test_reopen_tab_hidden_when_ai_disabled() -> None:
    state = SessionConnectionState()  # AI off
    ui = _Ui()
    build_ai_panel(ui, state=state)
    assert _reopen_tab(ui).visible is False  # no AI -> no floating reopen tab


def test_general_scope_activates_on_open_from_header() -> None:
    # Opened from the header with no object scope -> a general "ask anything about
    # this migration" streamer is wired so the composer is usable immediately.
    state = _enabled_state()
    ui = _Ui()
    calls = {"n": 0}

    def factory():  # noqa: ANN202
        calls["n"] += 1
        return _make_streamer("general reply")

    panel = build_ai_panel(ui, state=state, general_streamer_factory=factory)
    panel.set_visible(True)
    assert calls["n"] == 1
    assert state.ai_conversation.active_scope.scope_id == "general"


def test_object_scope_takes_priority_over_general() -> None:
    # A screen deep-link sets a SPECIFIC scope; the general factory is not used.
    state = _enabled_state()
    ui = _Ui()
    calls = {"n": 0}

    def factory():  # noqa: ANN202
        calls["n"] += 1
        return _make_streamer("general")

    panel = build_ai_panel(ui, state=state, general_streamer_factory=factory)
    panel.open_scope(
        scope_id="eval:orders", title="orders",
        streamer=_make_streamer("A"), seed_question="Q",
    )
    _pump(ui)
    assert state.ai_conversation.active_scope.scope_id == "eval:orders"
    assert calls["n"] == 0


def test_general_chat_system_is_migration_scoped_and_declines_off_topic() -> None:
    from dsql_migrator.core.assessment_strategist import build_general_chat_system

    sysp = build_general_chat_system(
        current_step="Validation", migration_type="full load + cdc"
    )
    low = sysp.lower()
    assert "aurora dsql" in low and "migrat" in low  # migration-domain scoped
    assert "off-topic" in low and "decline" in low   # declines unrelated questions
    assert "Validation" in sysp                       # grounded on the current step
