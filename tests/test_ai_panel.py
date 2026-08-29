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
        self.props_str = ""
        self.events: list[str] = []
        if kind in ("markdown", "code"):
            ui.rendered.append((kind, text))

    # chainable no-ops
    def classes(self, *_a, **_k) -> "_El":
        return self

    def style(self, *_a, **_k) -> "_El":
        return self

    def props(self, *a, **_k) -> "_El":  # noqa: ANN002
        if a and isinstance(a[0], str):
            self.props_str = a[0]
        return self

    def tooltip(self, *_a, **_k) -> "_El":
        return self

    def on(self, *a, **_k) -> "_El":  # noqa: ANN002
        if a and isinstance(a[0], str):
            self.events.append(a[0])
        return self

    def on_click(self, *_a, **_k) -> "_El":
        return self

    # context manager (with ui.row(): ...) -- tracks the slot stack so timers/
    # elements record which container owns them (mirrors NiceGUI's slot_stack), so a
    # test can model render_main.refresh() clearing a container + its timers.
    def __enter__(self) -> "_El":
        self._ui.slot_stack.append(self)
        return self

    def __exit__(self, *_exc) -> bool:
        if self._ui.slot_stack and self._ui.slot_stack[-1] is self:
            self._ui.slot_stack.pop()
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
        # Which container slot owns this timer (set by _Ui.timer). render_main.refresh()
        # deletes a container's descendants -- including timers created in its slot.
        self.parent = None
        self.is_deleted = False


class _Ui:
    """A minimal NiceGUI stand-in recording what the panel builds."""

    def __init__(self) -> None:
        self.rendered: list[tuple[str, str]] = []  # (markdown|code|set_content, body)
        self.timers: list[_Timer] = []
        self.drawer_visible: Optional[bool] = None
        self.buttons: list[_El] = []
        self.textareas: list[_El] = []
        self.labels: list[_El] = []
        self.badges: list[_El] = []
        self.slot_stack: list = []  # active container slots (for timer ownership)
        self.event_handlers: dict = {}  # ui.on(name, cb) -> {name: cb}
        self.drawer_el: Optional[_El] = None  # the right_drawer element

    def add_css(self, *_a, **_k) -> None:
        pass

    def add_body_html(self, *_a, **_k) -> None:
        pass

    def on(self, name=None, handler=None, *_a, **_k) -> None:  # noqa: ANN001
        # ui.on(event_name, handler, *, throttle=...) -- record so tests can invoke
        # the drawer-width / drag-resize handlers directly.
        if name is not None:
            self.event_handlers[name] = handler

    def notify(self, *_a, **_k) -> None:
        pass

    def _el(self, kind: str, text: str = "") -> _El:
        return _El(self, kind, text)

    def right_drawer(self, *_a, value: bool = False, **_k) -> _El:  # noqa: ANN001
        self.drawer_visible = bool(value)
        el = self._el("right_drawer")
        self.drawer_el = el
        return el

    def column(self, *_a, **_k) -> _El:
        return self._el("column")

    def row(self, *_a, **_k) -> _El:
        return self._el("row")

    def card(self, *_a, **_k) -> _El:
        return self._el("card")

    def scroll_area(self, *_a, **_k) -> _El:
        return self._el("scroll_area")

    def label(self, text: str = "", *_a, **_k) -> _El:
        el = self._el("label", text)
        self.labels.append(el)
        return el

    def icon(self, *_a, **_k) -> _El:
        return self._el("icon")

    def button(self, *a, **_k) -> _El:  # noqa: ANN002
        text = a[0] if a and isinstance(a[0], str) else ""
        el = _El(self, "button", text)
        el.icon = _k.get("icon", "")  # type: ignore[attr-defined]
        self.buttons.append(el)
        return el

    def input(self, *_a, **_k) -> _El:
        return self._el("input")

    def textarea(self, *_a, **_k) -> _El:
        el = self._el("textarea")
        self.textareas.append(el)
        return el

    def markdown(self, text: str = "", *_a, **_k) -> _El:
        return self._el("markdown", text)

    def code(self, text: str = "", *_a, **_k) -> _El:
        return self._el("code", text)

    def spinner(self, *_a, **_k) -> _El:
        return self._el("spinner")

    def badge(self, text: str = "", *_a, **_k) -> _El:
        el = self._el("badge", text)
        self.badges.append(el)
        return el

    def element(self, *_a, **_k) -> _El:
        return self._el("element")

    def timer(self, interval: float, cb) -> _Timer:  # noqa: ANN001
        t = _Timer(interval, cb)
        t.parent = self.slot_stack[-1] if self.slot_stack else None
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


class _Evt:
    """A minimal event double: the drawer-width handlers read ``event.args``."""

    def __init__(self, args: object) -> None:
        self.args = args


def _drawer_width(ui: _Ui) -> int:
    """Parse the ``width=<px>`` currently on the drawer element's props."""
    props = ui.drawer_el.props_str  # type: ignore[union-attr]
    for tok in props.split():
        if tok.startswith("width="):
            return int(tok.split("=", 1)[1])
    raise AssertionError(f"no width in drawer props: {props!r}")


def test_auto_width_is_30pct_of_viewport_clamped() -> None:
    state = _enabled_state()
    ui = _Ui()
    build_ai_panel(ui, state=state)
    fn = ui.event_handlers["ai_dba_drawer_width"]
    # Mid-size window: 30% of 1500 = 450, inside [360, 660].
    fn(_Evt(1500))
    assert _drawer_width(ui) == 450
    # Huge window: capped at the readable max (660), not 30% (=900).
    fn(_Evt(3000))
    assert _drawer_width(ui) == 660
    # Narrow window: floored at the readability minimum (360), not 30% (=240).
    fn(_Evt(800))
    assert _drawer_width(ui) == 360


def test_drag_resize_sets_and_remembers_manual_width() -> None:
    state = _enabled_state()
    ui = _Ui()
    build_ai_panel(ui, state=state)
    viewport = ui.event_handlers["ai_dba_drawer_width"]
    drag = ui.event_handlers["ai_dba_drawer_resize"]
    viewport(_Evt(1600))  # AUTO -> 480
    assert _drawer_width(ui) == 480
    # User drags the edge to 700 px (wider than the AUTO cap of 660 -- allowed).
    drag(_Evt(700))
    assert _drawer_width(ui) == 700
    # A later window resize must NOT clobber the width the user chose.
    viewport(_Evt(1400))
    assert _drawer_width(ui) == 700


def test_drag_resize_clamps_to_window_and_floor() -> None:
    state = _enabled_state()
    ui = _Ui()
    build_ai_panel(ui, state=state)
    viewport = ui.event_handlers["ai_dba_drawer_width"]
    drag = ui.event_handlers["ai_dba_drawer_resize"]
    viewport(_Evt(1000))
    # Dragged too wide: clamped so the Tool UI keeps its 200 px gap (1000 - 200).
    drag(_Evt(5000))
    assert _drawer_width(ui) == 800
    # Dragged too narrow: floored at the 360 px readability minimum.
    drag(_Evt(50))
    assert _drawer_width(ui) == 360


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


def test_start_over_resets_conversation_in_place_and_wipes_the_panel() -> None:
    # Start over must reset the AI DBA chat. The panel captures state.ai_conversation
    # by reference at build time and is NOT rebuilt on Start over, so clear() must reset
    # it IN PLACE (same object) rather than replace it -- otherwise the panel keeps
    # rendering (and appending to) the stale transcript. panel.reset() then wipes the
    # rendered bubbles.
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.open_scope(
        scope_id="eval:orders", title="orders",
        streamer=_make_streamer("A1"), seed_question="Q1",
    )
    _pump(ui)
    assert state.ai_conversation.messages  # transcript populated
    conv_obj = state.ai_conversation

    # Start over: clear() resets the conversation IN PLACE; the app then calls the
    # panel's reset() to wipe the rendered transcript.
    state.clear()
    panel.reset()

    # Same object (the panel's captured reference stays valid), now emptied.
    assert state.ai_conversation is conv_obj
    assert state.ai_conversation.messages == []
    assert state.ai_conversation.active_scope is None
    assert state.ai_conversation.visible is False


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
    tabs = [b for b in ui.buttons if b.text == "AI DBA"]
    assert len(tabs) == 1, "expected exactly one right-edge 'AI DBA' reopen tab"
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


def test_restored_open_panel_activates_general_streamer_so_composer_is_usable() -> None:
    # After a refresh/restart the panel is rebuilt with visible=True restored from the
    # session, but no live streamer -- the composer must not be left dead. Build should
    # activate the general scope so it is immediately usable (the bug: events rendered
    # but the chat input stayed disabled).
    state = _enabled_state()
    state.ai_conversation.visible = True  # restored open
    ui = _Ui()

    def factory():  # noqa: ANN202
        return _make_streamer("hi")

    build_ai_panel(ui, state=state, general_streamer_factory=factory)
    assert state.ai_conversation.active_scope is not None
    assert state.ai_conversation.active_scope.scope_id == "general"
    # composer inputs are enabled (a streamer is active, not busy)
    inputs = [t for t in ui.textareas]
    assert inputs and inputs[0].enabled is True


def test_composer_reenables_after_nav_clears_a_screen_scope() -> None:
    # Regression: a turn started from a step screen's "AI Assist" deep-link parents its
    # streaming tick timer on the screen's slot. Navigating (render_main.refresh() ->
    # container.clear()) used to delete + cancel that timer mid-turn, so tick() never
    # ran _set_busy(False) and the shared composer stayed disabled forever. The fix
    # anchors the tick timer on the persistent panel column so it survives navigation.
    import threading

    state = _enabled_state()
    ui = _Ui()
    release = threading.Event()

    def blocking_streamer(messages, on_delta):  # noqa: ANN001
        on_delta("partial ")
        release.wait(timeout=5)  # keep the turn in-flight across "navigation"
        return ObjectGuidanceOutcome(
            available=True, reason="OK", detail="",
            markdown="partial done", model_id="fake-model",
        )

    panel = build_ai_panel(ui, state=state)

    # A turn fired from a control rendered INSIDE render_main's refreshable container:
    # model that with a caller column entered as a context manager.
    screen = ui.column()
    with screen:
        panel.open_scope(
            scope_id="eval:orders", title="orders",
            streamer=blocking_streamer,
            seed_question="Explain the orders table.",
        )

    chat_input = ui.textareas[0]
    assert chat_input.enabled is False  # in-flight -> composer disabled

    # Navigation -> render_main.refresh(): clear the screen container, cancelling every
    # timer parented under it (mirrors Element.clear() -> Timer._handle_delete()).
    for t in ui.timers:
        if getattr(t, "parent", None) is screen:
            t.active = False
            t.is_deleted = True
    screen.is_deleted = True

    release.set()  # the turn completes AFTER navigation
    _pump(ui)

    # Post-fix: the tick timer lives under the persistent panel column, survives the
    # refresh, finalizes the turn -> composer usable again + assistant turn recorded.
    assert chat_input.enabled is True
    assert state.ai_conversation.messages[-1]["role"] == "assistant"


def test_restored_open_panel_with_dead_screen_scope_falls_back_to_general() -> None:
    # Regression: after a rebuild/restart the session restores an OPEN panel whose
    # active scope is a SCREEN deep-link (e.g. an object chat) -- but that scope's
    # streamer is a closure that can't be reconstructed, so conv["streamer"] is None.
    # The composer must NOT be left dead (the bug the user hit: navigate to a step, the
    # "Moved to ..." event shows, but the input is blocked). Build must re-home to the
    # general scope so the composer is immediately usable.
    from dsql_migrator.core.models import AiScope

    state = _enabled_state()
    state.ai_conversation.visible = True  # restored OPEN
    state.ai_conversation.active_scope = AiScope(
        scope_id="schema_conversion:orders", title="orders",
        subtitle="orders", chip="Schema conversion · orders",
    )
    # A prior (screen-scoped) exchange restored from the session.
    state.ai_conversation.messages.append(
        {"role": "user", "text": "How do I convert orders?"}
    )
    state.ai_conversation.messages.append({"role": "assistant", "text": "Drop the FK."})
    ui = _Ui()

    def factory():  # noqa: ANN202
        return _make_streamer("general reply")

    build_ai_panel(ui, state=state, general_streamer_factory=factory)
    # Stale screen scope re-homed to general (its streamer was unrecoverable)...
    assert state.ai_conversation.active_scope.scope_id == "general"
    # ...so the composer is enabled instead of dead.
    inputs = [t for t in ui.textareas]
    assert inputs and inputs[0].enabled is True


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


def test_composer_is_a_roomy_multiline_autogrow_textarea() -> None:
    # The composer must read like a chat window (multi-line, grows with content),
    # not a single-line field: a follow-up is often a pasted snippet or a few lines.
    state = _enabled_state()
    ui = _Ui()
    build_ai_panel(ui, state=state)
    assert len(ui.textareas) == 1, "the composer must be a textarea, not ui.input"
    props = ui.textareas[0].props_str
    assert "autogrow" in props            # grows with what you type
    assert "min-height" in props          # starts a few lines tall (autogrow forces rows=1)
    assert "max-height" in props          # capped so a long paste scrolls, not overflows
    # Enter-to-send is wired on the textarea (Shift+Enter falls through to a newline).
    assert any(ev == "keydown.enter" for ev in ui.textareas[0].events)


def _drain_events(ui: _Ui) -> None:
    # post_event only QUEUES (it may be called from a background job thread); a loop
    # timer built at panel-build time renders queued events. Drive it here.
    if ui.timers:
        ui.timers[0].cb()


def _run_progress(ui: _Ui) -> None:
    # Drive the panel's persistent progress poller (the 1.0s loop timer) once.
    for t in ui.timers:
        if abs(t.interval - 1.0) < 1e-6:
            t.cb()
            return


def test_disabling_ai_midsession_inerts_panel_and_blocks_model_calls() -> None:
    # COST SAFETY: turning AI Assist off on Connect mid-session must inert AI DBA -- the
    # composer goes dead and NO further turn reaches the model, even though a streamer
    # was set while it was on (the bug: the panel kept working -> unwanted AI charges).
    state = _enabled_state()
    ui = _Ui()
    calls = {"n": 0}

    def streamer(messages, on_delta):  # noqa: ANN001
        calls["n"] += 1
        on_delta("hi")
        return ObjectGuidanceOutcome(
            available=True, reason="OK", detail="", markdown="hi", model_id="m"
        )

    panel = build_ai_panel(ui, state=state)
    send = panel.open_scope(
        scope_id="eval:orders", title="orders",
        streamer=streamer, seed_question="Q",
    )
    _pump(ui)
    chat_input = ui.textareas[0]
    assert chat_input.enabled is True and calls["n"] == 1  # worked while AI was on

    state.ai_assist.enabled = False   # Connect toggles AI Assist OFF mid-session
    _drain_events(ui)                 # the loop timer reactively inerts the panel
    assert chat_input.enabled is False  # composer dead -> the user can't send
    send("please answer again")       # even a programmatic send...
    _pump(ui)
    assert calls["n"] == 1            # ...never reaches the model (no Bedrock cost)


def test_refresh_context_updates_step_chip_except_when_pinned_to_an_object() -> None:
    # Side-menu navigation updates the panel's baseline STEP chip (via refresh_context)
    # -- but a per-object scope keeps its own chip, so nav must not overwrite it.
    from dsql_migrator.core.models import MigrationContext

    step = {"name": "Evaluation"}

    def get_context() -> MigrationContext:
        return MigrationContext(
            current_step=step["name"], migration_type="", summary=""
        )

    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(
        ui, state=state, get_context=get_context,
        general_streamer_factory=lambda: _make_streamer(),
    )
    panel.set_visible(True)  # general scope -> chip = baseline ("Evaluation")
    assert any(el.text == "Evaluation" for el in ui.labels)

    step["name"] = "Schema Conversion"   # navigate
    panel.refresh_context()
    assert any(el.text == "Schema Conversion" for el in ui.labels)

    # Pinned to an object scope: refresh_context must NOT overwrite its chip.
    panel.open_scope(
        scope_id="eval:orders", title="orders",
        chip="Evaluation · orders", streamer=_make_streamer(),
    )
    step["name"] = "Data Migration"
    panel.refresh_context()
    assert not any(el.text == "Data Migration" for el in ui.labels)


def test_live_progress_card_monitors_and_finalizes_full_load() -> None:
    # The AI panel is a live Full Load monitor: one card updated in place by the
    # persistent poller (not one feed entry per table), finalizing to a summary.
    state = _enabled_state()
    state.ai_conversation.visible = True
    ui = _Ui()
    snap: dict = {"data": None}

    panel = build_ai_panel(ui, state=state, get_progress=lambda: snap["data"])
    # No Full Load job yet -> provider returns None -> no card.
    _run_progress(ui)
    assert not any("Full Load" in (el.text or "") for el in ui.labels)

    # Running: 2/5 tables done, 1 failed, with a throughput + ETA line.
    snap["data"] = {
        "label": "Full Load", "running": True, "total": 5, "done": 2,
        "failed": 1, "rows": 1000, "failed_objects": ["ecommerce.orders"],
        "rows_per_sec": 250, "eta": "~2m 30s left",
    }
    _run_progress(ui)
    texts = " ".join(el.text for el in ui.labels)
    assert "Full Load — 2/5 tables · 1 failed · 1,000 rows" in texts
    assert "Failed: ecommerce.orders" in texts
    assert "250 rows/s" in texts and "~2m 30s left" in texts  # throughput/ETA line

    # Terminal: the SAME card updates in place to the final summary ("complete").
    snap["data"] = {
        "label": "Full Load", "running": False, "total": 5, "done": 4,
        "failed": 1, "rows": 5000, "failed_objects": ["ecommerce.orders"],
    }
    _run_progress(ui)
    texts2 = " ".join(el.text for el in ui.labels)
    assert "Full Load complete — 4/5 tables · 1 failed · 5,000 rows" in texts2


def test_post_event_records_and_renders_when_enabled() -> None:
    # A major migration action is mirrored into the panel as a deterministic activity
    # event: recorded on the session (survives refresh) and shown in the feed.
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.post_event(text="Started Full Load for orders", status="started")
    msgs = state.ai_conversation.messages
    # Recorded on the session (source of truth) immediately, from any thread.
    assert msgs == [
        {"role": "event", "text": "Started Full Load for orders", "status": "started"}
    ]
    assert not any(el.text == "Started Full Load for orders" for el in ui.labels)
    # Rendered only once the loop timer drains the queue (safe UI-thread rendering).
    _drain_events(ui)
    assert any(el.text == "Started Full Load for orders" for el in ui.labels)


def test_conversion_activity_event_reads_as_preview_not_applied() -> None:
    # The Generate-DDL visual event must read as a PREVIEW (DDL generated to review),
    # NOT an apply/migration ("N objects converted" overstated it), and name the
    # DSQL-unconvertible kinds.
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.post_event(
        text="Schema conversion generated target DDL for 3 objects — a preview",
        status="success",
        kind="conversion",
        data={
            "converted": 3, "tables": 3, "views": 0,
            "triggers": 2, "routines": 1, "events": 0,
        },
    )
    _drain_events(ui)
    texts = " ".join(el.text for el in ui.labels)
    assert "DDL generated for 3 objects" in texts  # headline: generated, not "converted"
    assert "nothing is applied to Aurora DSQL until you Apply" in texts  # preview note
    assert "can't convert 2 trigger(s), 1 routine(s)" in texts  # unconvertible kinds


def test_apply_activity_event_renders_created_skipped_failed_summary() -> None:
    # When Apply finishes, the panel gets a VISUAL summary: a Created/Skipped/Failed
    # bar + a headline, and (on failure) the names of the objects that failed.
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.post_event(
        text="Applied schema to Aurora DSQL: 3 of 4 objects applied",
        status="error",
        kind="apply",
        data={
            "created": 2, "skipped": 1, "failed": 1, "total": 4,
            "failed_objects": ["geo"],
        },
    )
    _drain_events(ui)
    texts = " ".join(el.text for el in ui.labels)
    assert "Schema apply — 3 of 4 applied, 1 failed" in texts
    assert "Failed: geo" in texts  # names the failed object


def test_post_event_is_noop_when_ai_disabled() -> None:
    state = SessionConnectionState()  # AI off
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    panel.post_event(text="Started Full Load for orders", status="started")
    assert state.ai_conversation.messages == []  # nothing recorded on an inert panel


def _reopen_badge(ui: _Ui) -> _El:
    assert ui.badges, "expected the reopen-tab unseen badge to be built"
    return ui.badges[0]


def test_post_event_bumps_unseen_badge_when_closed_and_clears_on_open() -> None:
    state = _enabled_state()  # AI on, panel closed by default
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    badge = _reopen_badge(ui)
    assert badge.visible is False
    panel.post_event(text="Full Load complete: 8 tables", status="success")
    panel.post_event(text="Validation complete: 8/8 matched", status="success")
    _drain_events(ui)  # the loop timer renders queued events + bumps the unseen count
    assert badge.visible is True and badge.text == "2"  # unseen count while closed
    panel.set_visible(True)
    assert badge.visible is False  # opening the panel clears the badge


def test_recent_events_are_folded_into_the_next_chat_turn_as_context() -> None:
    # The assistant is made aware of what the operator did: recent activity events are
    # folded into the CURRENT question's text, but never sent as their own turns
    # (the streamer would mis-map a non-user/assistant role).
    state = _enabled_state()
    ui = _Ui()
    panel = build_ai_panel(ui, state=state)
    sent: list = []
    send = panel.open_scope(
        scope_id="general", title="AI assistant",
        streamer=_make_streamer("ok", record=sent),
    )
    panel.post_event(text="Started Full Load for orders", status="started")
    panel.post_event(text="Full Load complete: 1,240,000 rows", status="success")
    send("What should I check next?")
    _pump(ui)
    turn = sent[-1]  # the messages the streamer saw for this turn
    # Only user/assistant roles reach the model (events are not their own turns).
    assert all(m["role"] in ("user", "assistant") for m in turn)
    # The recent actions are folded into the last (current) user message as context.
    last_user = turn[-1]
    assert last_user["role"] == "user"
    assert "Started Full Load for orders" in last_user["text"]
    assert "Full Load complete: 1,240,000 rows" in last_user["text"]
    assert "What should I check next?" in last_user["text"]


def test_stop_button_is_built_and_hidden_until_streaming() -> None:
    # A "stop generating" affordance exists; it is hidden until a reply is streaming
    # (it replaces Send while busy — wired in _apply_composer_state / the tick loop).
    state = _enabled_state()
    ui = _Ui()
    build_ai_panel(ui, state=state)
    stop_btns = [b for b in ui.buttons if getattr(b, "icon", "") == "stop"]
    assert len(stop_btns) == 1, "expected a single Stop-generating button"
    assert stop_btns[0].visible is False  # hidden until a turn is in flight


def test_model_line_shows_the_connected_model_when_enabled() -> None:
    state = _enabled_state()
    state.ai_assist.model_id = "global.anthropic.claude-sonnet-5"
    ui = _Ui()
    build_ai_panel(ui, state=state)
    # A labeled chip: a "Model" caption + the model id as its value (easy to spot).
    assert any(el.text == "Model" for el in ui.labels), "expected a 'Model' label chip"
    assert any(
        el.text == "global.anthropic.claude-sonnet-5" for el in ui.labels
    ), "the connected Bedrock model id should be shown under the composer"


def test_general_chat_system_is_migration_scoped_and_declines_off_topic() -> None:
    from dsql_migrator.core.assessment_strategist import build_general_chat_system

    sysp = build_general_chat_system(
        current_step="Validation", migration_type="full load + cdc"
    )
    low = sysp.lower()
    assert "aurora dsql" in low and "migrat" in low  # migration-domain scoped
    assert "off-topic" in low and "decline" in low   # declines unrelated questions
    assert "Validation" in sysp                       # grounded on the current step
