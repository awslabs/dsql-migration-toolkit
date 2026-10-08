# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Connect's AI Assist card: the badge + status area follow the SESSION verdict (G18).

The verdict (``state.ai_verified`` / ``ai_unavailable_detail``) can be written while the
Connect view is open by something that is not on it -- an AI DBA reply latches a denial
(e.g. the model's AWS Marketplace agreement is missing), a later success clears it -- and
it is reset by any settings / profile change. These tests render the REAL
``build_connect_page`` against a permissive, tree-aware NiceGUI double and check that:

- a latched denial shows its fixed reason on (re)build, as an amber "AI Assist
  unavailable" notice (the tone of the badge, the journey chip and the AI DBA panel's
  reply notice) -- never as loose red text;
- while the view stays open, a light timer redraws the badge + status area when (and
  only when) the verdict or its latched reason changes, so a Verify's own message is
  never overwritten;
- a settings or profile change clears a stale status line;
- while the preflight runs, Verify shows a neutral "Verifying AI access..." line that
  neither the old message nor the timer paints over;
- a result for settings that changed while the check ran (model, region, AWS profile,
  the switch) is dropped instead of being recorded for settings that were never checked;
- Verify AI access latches a MODEL_AGREEMENT_REQUIRED result like ACCESS_DENIED and
  never shows the denial's message;
- a clean pass shows the honest "answered just now" copy with its first-call caveat;
- a follow-up in an AI DBA chat opened from this screen answers with the CURRENT Bedrock
  model and AWS profile, so it cannot latch a just-verified config "unavailable".

Offline only: the verify runner is injected (or wraps a fake Bedrock client).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import sys
import types
from typing import Any, Callable, Iterator, NamedTuple, Optional

import pytest

from dsql_migrator.core.models import AiAccessCheckResult
from dsql_migrator.ui import connect as connect_mod
from dsql_migrator.ui.ai_assist import (
    BEDROCK_MODEL_FALLBACKS,
    DEFAULT_BEDROCK_MODEL_ID,
    build_ai_assist_config,
)
from dsql_migrator.ui.design import INLINE_HINT_TEXT
from dsql_migrator.ui.session import SessionStore

# ---------------------------------------------------------------------------
# A permissive, tree-aware NiceGUI double
# ---------------------------------------------------------------------------


def _call_handler(handler: Callable, event: Any) -> Any:
    """Invoke a NiceGUI-style handler with the event only if it takes an argument."""
    try:
        takes_arg = bool(inspect.signature(handler).parameters)
    except (TypeError, ValueError):
        takes_arg = True
    return handler(event) if takes_arg else handler()


class _El:
    """A chainable element double: records text / classes / props / value / handlers.

    Any method the double does not model is a chainable no-op, so the page builds as in
    NiceGUI. ``with el:`` makes it the parent of the elements created inside, and
    ``clear()`` drops them, so a test can read what a container shows NOW. ``set_value``
    fires the value-change handlers ONLY when the value actually changes (NiceGUI's
    ``BindableProperty`` semantics), and an ``on_change`` constructor kwarg is registered
    as a value-change handler, as NiceGUI does.
    """

    def __init__(self, ui: "_Ui", kind: str, args: tuple, kwargs: dict) -> None:
        self._ui = ui
        self.kind = kind
        self.text = args[0] if args and isinstance(args[0], str) else ""
        self.label = kwargs.get("label") or ""
        self.value = kwargs.get("value")
        self.classes_str = ""
        self.props_str = ""
        self.is_deleted = False
        self.enabled = True
        self.children: list[_El] = []
        self.click: Optional[Callable] = kwargs.get("on_click")
        self.value_handlers: list[Callable] = []
        if kwargs.get("on_change") is not None:
            self.value_handlers.append(kwargs["on_change"])
        if kwargs.get("on_value_change") is not None:
            self.value_handlers.append(kwargs["on_value_change"])

    def __getattr__(self, _name: str) -> Callable[..., "_El"]:
        return lambda *_a, **_k: self

    def __enter__(self) -> "_El":
        self._ui.stack.append(self)
        return self

    def __exit__(self, *_exc: Any) -> bool:
        self._ui.stack.pop()
        return False

    def clear(self) -> "_El":
        self.children = []
        return self

    def classes(self, add: Optional[str] = None, *, replace: Optional[str] = None,
                **_k: Any) -> "_El":
        if replace is not None:
            self.classes_str = replace
        elif add:
            self.classes_str = f"{self.classes_str} {add}".strip()
        return self

    def props(self, add: Optional[str] = None, **_k: Any) -> "_El":
        if add:
            self.props_str = add
        return self

    def set_text(self, text: str) -> None:
        self.text = text

    def set_enabled(self, enabled: bool) -> None:
        self.enabled = bool(enabled)

    # G20: modelled (not chainable no-ops) so a test can see what a button shows after
    # ``run_busy`` restored it.
    def enable(self) -> None:
        self.enabled = True

    def disable(self) -> None:
        self.enabled = False

    def set_value(self, value: Any) -> "_El":
        previous = self.value
        if value == previous:
            return self
        self.value = value
        event = types.SimpleNamespace(sender=self, value=value, previous_value=previous)
        for handler in list(self.value_handlers):
            _call_handler(handler, event)
        return self

    def on_value_change(self, handler: Callable) -> "_El":
        self.value_handlers.append(handler)
        return self

    def on_click(self, handler: Callable) -> "_El":
        self.click = handler
        return self


def _walk(el: _El) -> Iterator[_El]:
    yield el
    for child in el.children:
        yield from _walk(child)


class _Timer:
    def __init__(self, interval: float, callback: Callable) -> None:
        self.interval = interval
        self.callback = callback


class _Status(NamedTuple):
    """What the AI status area shows right now."""

    kind: str  # "empty" | "line" (an inline hint) | "notice" (a render_notice box)
    header: str  # the notice's bold header ("" for a line)
    text: str  # the line's text, or the notice's body
    classes: str  # the line's classes, or the notice box's


class _Ui:
    """Records every element, timer and toast ``build_connect_page`` creates."""

    def __init__(self) -> None:
        self.els: list[_El] = []
        self.stack: list[_El] = []
        self.timers: list[_Timer] = []
        self.toasts: list[tuple[str, Optional[str]]] = []

    def __getattr__(self, kind: str) -> Callable[..., _El]:
        def factory(*args: Any, **kwargs: Any) -> _El:
            el = _El(self, kind, args, kwargs)
            self.els.append(el)
            if self.stack:
                self.stack[-1].children.append(el)
            return el

        return factory

    def notify(self, message: str, *_a: Any, type: Optional[str] = None,  # noqa: A002
               **_k: Any) -> None:
        self.toasts.append((message, type))

    def timer(self, interval: float, callback: Callable, *_a: Any, **_k: Any) -> _Timer:
        timer = _Timer(interval, callback)
        self.timers.append(timer)
        return timer

    def refreshable(self, fn: Callable) -> Callable:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return fn(*args, **kwargs)

        wrapper.refresh = wrapper  # type: ignore[attr-defined]
        return wrapper

    # -- lookups ---------------------------------------------------------------

    def _index(self, kind: str, text: str) -> int:
        return next(
            i for i, el in enumerate(self.els) if el.kind == kind and el.text == text
        )

    def ai_badge(self) -> _El:
        """The AI Assist section header's status badge."""
        title = self._index("label", "AI Assist")
        return next(el for el in self.els[title + 1:] if el.kind == "badge")

    def status_box(self) -> _El:
        """The AI status area: the first column after the Bedrock docs link."""
        link = self._index("link", "Bedrock model IDs (AWS docs)")
        return next(el for el in self.els[link + 1:] if el.kind == "column")

    def status(self) -> _Status:
        box = self.status_box()
        if not box.children:
            return _Status("empty", "", "", "")
        first = box.children[0]
        if first.kind == "row":  # render_notice's box: icon + header + body labels
            labels = [el for el in _walk(first) if el.kind == "label"]
            body = labels[1].text if len(labels) > 1 else ""
            return _Status("notice", labels[0].text, body, first.classes_str)
        return _Status("line", "", first.text, first.classes_str)

    def shown(self) -> tuple[str, str]:
        return (self.ai_badge().text, self.status().text)

    def model_select(self) -> _El:
        return next(
            el for el in self.els
            if el.kind == "select" and el.label.startswith("Model ID")
        )

    def region_input(self) -> _El:
        return next(
            el for el in self.els if el.kind == "input" and el.text.startswith("Region")
        )

    def profile_select(self) -> _El:
        return next(
            el for el in self.els if el.kind == "select" and el.label == "AWS profile"
        )

    def ai_switch(self) -> _El:
        return next(
            el for el in self.els
            if el.kind == "switch" and el.text == "Enable AI Assist"
        )

    def find_button(self, text: str) -> _El:
        """The most recently created button with this text (NOT the ``ui.button``
        factory the page itself calls)."""
        return [el for el in self.els if el.kind == "button" and el.text == text][-1]

    def tick(self) -> None:
        for timer in self.timers:
            timer.callback()

    def verify(self) -> None:
        button = self.find_button("Verify AI access")
        assert button.click is not None
        asyncio.run(button.click())


async def _io_bound(fn: Callable, *args: Any, **kwargs: Any) -> Any:
    return fn(*args, **kwargs)


def _render(
    setup: Callable[[Any], None],
    runner: Optional[Callable] = None,
    *,
    profiles: tuple[str, ...] = (),
    **page_kwargs: Any,
) -> tuple[_Ui, Any]:
    """Build the real Connect page against the double; return (ui, session state)."""
    ui = _Ui()
    fake = types.ModuleType("nicegui")
    fake.ui = ui  # type: ignore[attr-defined]
    fake.run = types.SimpleNamespace(  # type: ignore[attr-defined]
        io_bound=_io_bound, cpu_bound=_io_bound
    )
    store = SessionStore()
    state = store.get_or_create("s")
    setup(state)

    def _never_called(*_a: Any) -> AiAccessCheckResult:
        raise AssertionError("Verify AI access was not expected to run")

    saved = sys.modules.get("nicegui")
    sys.modules["nicegui"] = fake
    try:
        connect_mod.build_connect_page(
            store,
            "s",
            profile_lister=lambda: list(profiles),
            verify_runner=runner or _never_called,
            **page_kwargs,
        )
    finally:
        if saved is None:
            sys.modules.pop("nicegui", None)
        else:
            sys.modules["nicegui"] = saved
    return ui, state


def _ai_on(state: Any) -> None:
    state.set_ai_assist(build_ai_assist_config(enabled=True))


def _ok_runner(detail: str = "OK-COPY") -> Callable:
    return lambda config, _profile: AiAccessCheckResult(
        ok=True, reason="OK", detail=detail, model_id=config.model_id
    )


def _failing_runner(reason: str, detail: str) -> Callable:
    return lambda config, _profile: AiAccessCheckResult(
        ok=False,
        reason=reason,  # type: ignore[arg-type]
        detail=detail,
        model_id=config.model_id,
    )


def _assert_amber_notice(ui: _Ui) -> None:
    status = ui.status()
    assert (status.kind, status.header) == ("notice", "AI Assist unavailable")
    assert "bg-amber-50" in status.classes and "border-amber-200" in status.classes
    # Not blocking (the deterministic workflow is unaffected), so never the red an
    # "error" uses -- and never loose colored text for a verdict (design system).
    assert not any("text-red-700" in el.classes_str for el in _walk(ui.status_box()))


# ---------------------------------------------------------------------------
# The verdict on (re)build and while the view stays open
# ---------------------------------------------------------------------------


def test_a_latched_denial_shows_its_reason_on_build_as_an_amber_notice() -> None:
    # The badge said "Unavailable" with an EMPTY status line: no reason, no next step.
    def latched(state: Any) -> None:
        _ai_on(state)
        state.set_ai_verified(False, "FIXED-COPY")

    ui, _state = _render(latched)
    assert ui.shown() == ("Unavailable", "FIXED-COPY")
    _assert_amber_notice(ui)


def test_an_open_connect_view_follows_a_latch_and_a_clear_from_elsewhere() -> None:
    ui, state = _render(_ai_on, _ok_runner())
    ui.verify()
    assert ui.shown() == ("Verified", "OK-COPY")
    ui.tick()
    assert ui.shown() == ("Verified", "OK-COPY"), "an unchanged verdict must not redraw"

    state.set_ai_verified(False, "LATCHED-BY-PANEL")  # e.g. an AI DBA reply was denied
    ui.tick()
    assert ui.shown() == ("Unavailable", "LATCHED-BY-PANEL")
    _assert_amber_notice(ui)

    state.set_ai_verified(None)  # a later reply succeeded
    ui.tick()
    assert ui.shown() == ("Optional", "")
    assert ui.status().kind == "empty"


def test_an_open_connect_view_follows_a_reason_change_while_latched() -> None:
    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_DETAILS, _UNAVAILABLE_DETAILS

    iam = _ACCESS_CHECK_DETAILS["ACCESS_DENIED"]
    ui, state = _render(_ai_on, _failing_runner("ACCESS_DENIED", iam))
    ui.verify()
    assert ui.shown() == ("Unavailable", iam)
    # Still "unavailable" -- only the latched REASON changed (the operator fixed IAM or
    # re-authenticated, and the open AI DBA panel's retry was then denied for the
    # model's agreement). The status must follow it, not keep the old, wrong remedy.
    agreement = _UNAVAILABLE_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    state.set_ai_verified(False, agreement)
    ui.tick()
    assert ui.shown() == ("Unavailable", agreement)


def test_ai_off_wins_over_a_late_latch() -> None:
    ui, state = _render(lambda _state: None)  # AI Assist off
    state.set_ai_verified(False, "X")  # a reply in flight finished after AI was turned off
    ui.tick()
    assert ui.shown() == ("Optional", "")


# ---------------------------------------------------------------------------
# A settings / profile change clears a stale status line
# ---------------------------------------------------------------------------


def test_a_model_change_clears_a_stale_verified_status_line() -> None:
    ui, state = _render(_ai_on, _ok_runner())
    ui.verify()
    assert ui.shown() == ("Verified", "OK-COPY")
    ui.model_select().set_value("global.anthropic.claude-opus-5")
    assert state.ai_verified is None
    assert ui.shown() == ("Optional", "")


def test_a_settings_change_clears_a_throttle_note_whose_verdict_did_not_change() -> None:
    # THROTTLED leaves the verdict "unverified" before AND after the edit, so only a
    # FORCED redraw on the settings change clears the old note.
    ui, _state = _render(_ai_on, _failing_runner("THROTTLED", "THROTTLE-COPY"))
    ui.verify()
    assert ui.status().text == "THROTTLE-COPY"
    ui.tick()
    assert ui.status().text == "THROTTLE-COPY"
    ui.model_select().set_value("global.anthropic.claude-opus-5")
    assert ui.status().text == ""


def test_a_profile_switch_clears_a_verified_status_line() -> None:
    ui, state = _render(_ai_on, _ok_runner(), profiles=("dev",))
    ui.verify()
    assert ui.shown() == ("Verified", "OK-COPY")
    ui.profile_select().set_value("dev")
    assert state.aws_profile == "dev"
    assert state.ai_verified is None
    assert ui.shown() == ("Optional", "")


@pytest.mark.parametrize(
    ("reason", "answered_by"),
    [("THROTTLED", None), ("OK", BEDROCK_MODEL_FALLBACKS[0])],
    ids=["throttle-note", "fallback-pass-warning"],
)
def test_a_profile_switch_clears_a_note_whose_verdict_did_not_change(
    reason: str, answered_by: Any
) -> None:
    # Both leave the verdict "unverified" before AND after the switch, so only the FORCED
    # redraw clears the line -- the timer's change test never would, and the old
    # profile's "access verified, but using ..." claim would outlive its credentials.
    def runner(config: Any, _profile: Any) -> AiAccessCheckResult:
        return AiAccessCheckResult(
            ok=reason == "OK",
            reason=reason,  # type: ignore[arg-type]
            detail="OLD-PROFILE-NOTE",
            model_id=answered_by or config.model_id,
        )

    ui, state = _render(_ai_on, runner, profiles=("dev",))
    ui.verify()
    assert state.ai_verified is None
    assert ui.status().text == "OLD-PROFILE-NOTE"
    ui.profile_select().set_value("dev")
    assert state.ai_verified is None
    assert ui.shown() == ("Optional", "")
    ui.tick()
    assert ui.shown() == ("Optional", "")


# ---------------------------------------------------------------------------
# Verify AI access
# ---------------------------------------------------------------------------


def test_verify_shows_its_progress_line_while_the_preflight_runs() -> None:
    # The only in-flight cue (run_busy just disables the button). A retry while the
    # agreement is still propagating returns the SAME denial, so without this line the
    # old copy reads as the new result. Here a denial was latched elsewhere just before
    # the click (no tick yet) and the 1 s timer keeps ticking during the real preflight:
    # neither may paint over the progress line.
    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_DETAILS

    copy = _ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    holder: dict[str, _Ui] = {}
    in_flight: list[_Status] = []

    def runner(config: Any, _profile: Any) -> AiAccessCheckResult:
        holder["ui"].tick()
        in_flight.append(holder["ui"].status())
        return AiAccessCheckResult(
            ok=False, reason="MODEL_AGREEMENT_REQUIRED", detail=copy,
            model_id=config.model_id,
        )

    ui, state = _render(_ai_on, runner)
    holder["ui"] = ui
    state.set_ai_verified(False, copy)  # e.g. an AI DBA reply was just denied
    ui.verify()
    assert in_flight == [
        _Status(
            "line", "", "Verifying AI access...", f"text-sm {INLINE_HINT_TEXT['neutral']}"
        )
    ]
    assert ui.shown() == ("Unavailable", copy)


def test_verify_latches_a_model_agreement_denial_as_the_unavailable_notice() -> None:
    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_DETAILS

    copy = _ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    ui, state = _render(_ai_on, _failing_runner("MODEL_AGREEMENT_REQUIRED", copy))
    ui.verify()
    assert state.ai_verified is False
    assert state.ai_unavailable_detail == copy
    assert ui.shown() == ("Unavailable", copy)
    _assert_amber_notice(ui)
    assert ui.toasts[-1] == (copy, "negative")
    drawn = ui.status()
    ui.tick()
    assert ui.status() == drawn


def test_a_clean_pass_shows_the_honest_copy_with_its_caveat_in_green() -> None:
    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_DETAILS, _FIRST_CALL_CAVEAT

    ok_copy = _ACCESS_CHECK_DETAILS["OK"]
    ui, state = _render(_ai_on, _ok_runner(ok_copy))
    ui.verify()
    assert state.ai_verified is True
    assert ui.shown() == ("Verified", ok_copy)
    assert _FIRST_CALL_CAVEAT in ui.status().text
    assert ui.status().kind == "line"
    assert "text-green-700" in ui.status().classes
    assert ui.toasts[-1] == (ok_copy, "positive")


@pytest.mark.parametrize("start", ["VERIFIED", "LATCHED"])
def test_a_fallback_pass_warning_survives_the_adoption_and_the_timer(start: str) -> None:
    # The adoption's set_value fires persist_ai_settings (a forced redraw) mid-handler;
    # the Verify message must still be what is shown afterwards, and the timer must not
    # replace it while the verdict it was drawn for holds.
    fallback = BEDROCK_MODEL_FALLBACKS[0]

    def setup(state: Any) -> None:
        _ai_on(state)
        if start == "VERIFIED":
            state.set_ai_verified(True)
        else:
            state.set_ai_verified(False, "L")

    def runner(_config: Any, _profile: Any) -> AiAccessCheckResult:
        return AiAccessCheckResult(
            ok=True, reason="OK", detail="FALLBACK-WARNING", model_id=fallback
        )

    ui, state = _render(setup, runner)
    ui.verify()
    assert state.ai_assist.model_id == fallback
    assert state.ai_verified is None
    assert ui.status().text == "FALLBACK-WARNING"
    assert "text-amber-700" in ui.status().classes
    ui.tick()
    assert ui.status().text == "FALLBACK-WARNING"


_SETTINGS_CHANGED = (
    "AI settings changed during the check. Click Verify AI access to check the "
    "current settings."
)


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("model", "OK"),
        ("model", "MODEL_AGREEMENT_REQUIRED"),
        ("region", "OK"),
        ("profile", "OK"),
    ],
)
def test_a_result_for_settings_changed_during_the_check_is_dropped(
    change: str, reason: str
) -> None:
    # Only the Verify button is disabled while the preflight runs: the model, region and
    # AWS profile stay editable (and a second tab shares the session). Each change resets
    # the verdict; the old settings' result must not then be recorded for the NEW ones --
    # a green "Verified" for a model that was never called, or another model's denial.
    holder: dict[str, _Ui] = {}
    asked: list[tuple] = []

    def runner(config: Any, profile: Any) -> AiAccessCheckResult:
        asked.append((config.model_id, config.region, profile))
        ui = holder["ui"]
        if change == "model":
            ui.model_select().set_value("global.anthropic.claude-opus-5")
        elif change == "region":
            ui.region_input().set_value("eu-west-1")
        else:
            ui.profile_select().set_value("dev")
        return AiAccessCheckResult(
            ok=reason == "OK",
            reason=reason,  # type: ignore[arg-type]
            detail=f"{reason}-COPY",
            model_id=config.model_id,
        )

    ui, state = _render(_ai_on, runner, profiles=("dev",))
    holder["ui"] = ui
    ui.verify()
    assert asked == [(DEFAULT_BEDROCK_MODEL_ID, None, None)]
    assert state.ai_verified is None
    assert not any(message == f"{reason}-COPY" for message, _type in ui.toasts)
    assert ui.shown() == ("Optional", _SETTINGS_CHANGED)
    assert ui.status().kind == "line"
    ui.tick()
    assert ui.shown() == ("Optional", _SETTINGS_CHANGED)


def test_a_change_undone_during_the_check_keeps_its_result() -> None:
    # A -> B -> A while in flight: the result IS about the current settings.
    holder: dict[str, _Ui] = {}

    def runner(config: Any, _profile: Any) -> AiAccessCheckResult:
        holder["ui"].model_select().set_value("global.anthropic.claude-opus-5")
        holder["ui"].model_select().set_value(config.model_id)
        return AiAccessCheckResult(
            ok=True, reason="OK", detail="OK-COPY", model_id=config.model_id
        )

    ui, state = _render(_ai_on, runner)
    holder["ui"] = ui
    ui.verify()
    assert state.ai_assist.model_id == DEFAULT_BEDROCK_MODEL_ID
    assert state.ai_verified is True
    assert ui.shown() == ("Verified", "OK-COPY")


def test_switching_ai_off_during_the_check_records_nothing() -> None:
    holder: dict[str, _Ui] = {}

    def runner(config: Any, _profile: Any) -> AiAccessCheckResult:
        holder["ui"].ai_switch().set_value(False)
        return AiAccessCheckResult(
            ok=True, reason="OK", detail="OK-COPY", model_id=config.model_id
        )

    ui, state = _render(_ai_on, runner)
    holder["ui"] = ui
    ui.verify()
    assert state.ai_verified is None
    assert ui.toasts == []
    # Nothing to verify while AI is off, so no "verify again" note either.
    assert ui.shown() == ("Optional", "")


def test_a_real_agreement_denial_verifies_once_and_never_shows_the_message() -> None:
    from botocore.exceptions import ClientError

    from dsql_migrator.core.ai_assistant import _ACCESS_CHECK_DETAILS, AiConversionAssistant
    from dsql_migrator.ui.ai_assist import run_verify_ai_access

    sentinel = "arn:aws:sts::123456789012:assumed-role/WSOpsRole/g18-SENTINEL"
    account = "123456789012"  # survives a truncation that cuts the ARN short
    denial = ClientError(
        {
            "Error": {
                "Code": "AccessDeniedException",
                # The sentinel at BOTH ends: a truncated leak still carries it.
                "Message": (
                    f"{sentinel} Model access is denied due to IAM user or service "
                    "role is not authorized to perform the required AWS Marketplace "
                    f"actions (aws-marketplace:ViewSubscriptions) {sentinel}"
                ),
            }
        },
        "InvokeModel",
    )

    class _DenyingClient:
        def __init__(self) -> None:
            self.calls = 0

        def invoke_model(self, **_kwargs: Any) -> dict:
            self.calls += 1
            raise denial

    client = _DenyingClient()

    def runner(config: Any, profile: Any) -> AiAccessCheckResult:
        return run_verify_ai_access(
            config,
            profile,
            assistant_factory=lambda cfg, _p: AiConversionAssistant(cfg, client=client),
        )

    ui, state = _render(_ai_on, runner)
    ui.verify()
    copy = _ACCESS_CHECK_DETAILS["MODEL_AGREEMENT_REQUIRED"]
    assert client.calls == 1  # the preflight stops: no fallback probe
    assert ui.status().text == copy
    assert state.ai_unavailable_detail == copy
    rendered = [el.text for el in ui.els] + [message for message, _type in ui.toasts]
    assert not any(account in text for text in rendered)
    assert sentinel not in repr(state.ai_unavailable_detail)


# ---------------------------------------------------------------------------
# A chat opened from this screen answers with the CURRENT settings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("switch", ["model", "profile"])
def test_a_follow_up_in_an_open_connect_chat_uses_the_current_settings(
    switch: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The REAL "Ask AI to help fix this" site: its AI DBA scope stays open (the panel is
    # app-wide and keeps the scope's streamer), the first reply is denied and latches,
    # the operator switches to a model / AWS profile that works and Verify passes -- and
    # a follow-up in the still-open chat used to go to the OLD model / profile, whose
    # denial then latched the just-verified settings "unavailable" (with copy blaming
    # "the configured model"). The strategist is now resolved per send.
    from botocore.exceptions import ClientError

    import dsql_migrator.core.assessment_strategist as strategist_mod
    from dsql_migrator.core.ai_assistant import AiConversionAssistant
    from dsql_migrator.core.models import ConnectionResult
    from dsql_migrator.ui.ai_assist import ai_is_usable
    from dsql_migrator.ui.ai_panel import build_ai_panel
    from tests.test_ai_panel import _pump
    from tests.test_ai_panel import _Ui as _PanelUi

    model_a, model_b = DEFAULT_BEDROCK_MODEL_ID, BEDROCK_MODEL_FALLBACKS[0]
    calls: list[tuple[str, Optional[str]]] = []  # (modelId, aws_profile) per chat call

    class _Bedrock:
        """Model A's agreement is missing; the default credential chain has expired."""

        def __init__(self, profile: Optional[str]) -> None:
            self.profile = profile

        def _check(self, model_id: str, op: str) -> None:
            if switch == "model" and model_id == model_a:
                raise ClientError({"Error": {"Code": "AccessDeniedException", "Message": (
                    "not authorized to perform the required AWS Marketplace actions"
                )}}, op)
            if switch == "profile" and self.profile != "dev":
                raise ClientError({"Error": {"Code": "ExpiredTokenException",
                                             "Message": "expired"}}, op)

        def invoke_model(self, **kwargs: Any) -> dict:  # the preflight
            self._check(kwargs["modelId"], "InvokeModel")
            return {"body": json.dumps({"content": [{"type": "text", "text": "p"}]})}

        def invoke_model_with_response_stream(self, **kwargs: Any) -> dict:  # the chat
            calls.append((kwargs["modelId"], self.profile))
            self._check(kwargs["modelId"], "InvokeModelWithResponseStream")
            delta = {"type": "content_block_delta", "delta": {"text": "Check the SG."}}
            return {"body": [{"chunk": {"bytes": json.dumps(delta).encode()}}]}

    monkeypatch.setattr(
        strategist_mod,
        "build_bedrock_runtime_client",
        lambda _config, aws_profile=None: _Bedrock(aws_profile),
    )

    def setup(state: Any) -> None:
        _ai_on(state)
        state.set_target(
            connect_mod.build_target_config(
                cluster_endpoint="abcdefghijklmnopqrstuvwxyz.dsql.us-east-1.on.aws",
                region="us-east-1",
            )
        )

    panel_ui = _PanelUi()
    panel_box: dict[str, Any] = {}

    def open_ai_scope(**kwargs: Any) -> Any:  # what app.py's _open_ai_scope does
        panel_box["send"] = panel_box["panel"].open_scope(**kwargs)
        return panel_box["send"]

    ui, state = _render(
        setup,
        lambda config, profile: AiConversionAssistant(
            config, client=_Bedrock(profile)
        ).verify_access(),
        profiles=("dev",),
        target_tester=lambda _config, _profile: ConnectionResult(
            success=False, detail="connection refused"
        ),
        open_ai_scope=open_ai_scope,
    )
    panel_box["panel"] = build_ai_panel(panel_ui, state=state)

    asyncio.run(ui.find_button("Test target connection").click())
    ui.find_button("Ask AI to help fix this").click()
    _pump(panel_ui)
    assert state.ai_verified is False  # right for the settings that answered the seed
    ui.tick()
    assert ui.shown()[0] == "Unavailable"

    if switch == "model":
        ui.model_select().set_value(model_b)
    else:
        ui.profile_select().set_value("dev")
    ui.verify()
    assert state.ai_verified is True and ui.shown()[0] == "Verified"

    panel_box["send"]("Thanks -- what should I check next?")  # the composer's send
    _pump(panel_ui)
    ui.tick()

    if switch == "model":
        assert [model for model, _profile in calls] == [model_a, model_b]
    else:
        assert [profile for _model, profile in calls] == [None, "dev"]
    assert state.ai_verified is True
    assert ai_is_usable(state)
    assert ui.shown()[0] == "Verified"


# ---------------------------------------------------------------------------
# G20: the Verify button follows the AI Assist switch after a check
# ---------------------------------------------------------------------------


def test_g20_switching_ai_off_during_the_check_leaves_verify_disabled() -> None:
    # run_busy's restore re-enabled the button although AI Assist was off by then.
    holder: dict[str, _Ui] = {}

    def runner(config: Any, _profile: Any) -> AiAccessCheckResult:
        holder["ui"].ai_switch().set_value(False)
        return AiAccessCheckResult(
            ok=True, reason="OK", detail="OK-COPY", model_id=config.model_id
        )

    ui, _state = _render(_ai_on, runner)
    holder["ui"] = ui
    ui.verify()
    assert ui.find_button("Verify AI access").enabled is False
    assert ui.model_select().enabled is False


def test_g20_verify_stays_disabled_while_the_check_runs_and_follows_the_switch_after() -> None:
    holder: dict[str, _Ui] = {}
    seen: list[bool] = []

    def runner(config: Any, _profile: Any) -> AiAccessCheckResult:
        ui = holder["ui"]
        ui.ai_switch().set_value(False)
        ui.ai_switch().set_value(True)
        # Still running: a second click must not be possible.
        seen.append(ui.find_button("Verify AI access").enabled)
        return AiAccessCheckResult(
            ok=True, reason="OK", detail="OK-COPY", model_id=config.model_id
        )

    ui, _state = _render(_ai_on, runner)
    holder["ui"] = ui
    ui.verify()
    assert seen == [False]
    assert ui.find_button("Verify AI access").enabled is True


def test_g20_verify_is_enabled_again_after_a_check_with_ai_on() -> None:
    ui, _state = _render(_ai_on, _ok_runner())
    ui.verify()
    button = ui.find_button("Verify AI access")
    assert button.enabled is True
    # The check is over, so the switch alone drives the button again (the running flag
    # must be cleared, or AI Assist off and on leaves Verify disabled until a page rebuild).
    ui.ai_switch().set_value(False)
    assert button.enabled is False
    ui.ai_switch().set_value(True)
    assert button.enabled is True


def test_g20_the_connection_test_buttons_are_restored_as_before() -> None:
    def _no_io(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("no connection test may run against an empty form")

    ui, _state = _render(_ai_on, source_tester=_no_io, target_tester=_no_io)
    for label in ("Test source connection", "Test target connection"):
        button = ui.find_button(label)
        button.enabled = False
        asyncio.run(button.click())  # the empty form fails validation: no I/O
        assert button.enabled is True, label
