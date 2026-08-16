# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Start-over confirmation dialog + CDC-teardown banner UI (extracted from ``workflow.py``).

These render the destructive "Start over" confirmation dialog (with its CDC-infra
warning) and the CDC-teardown / lifecycle banner shown while a teardown is in flight.
They are a self-contained teardown-safety UI concern: every function takes the NiceGUI
``ui`` module (and its callbacks) as an explicit parameter, so this module has no NiceGUI
import and no back-dependency on ``workflow.py`` -- it is imported one-directionally by
``build_workflow_sidebar``. ``_start_over_cdc_warning`` and ``_cdc_teardown_banner_copy``
are pure (side-effect free) so they stay unit-testable in isolation.
"""

from __future__ import annotations

import re
from typing import Callable, Optional, Sequence

from dsql_migrator.ui.design import render_notice

def _start_over_cdc_warning(
    state: "object",
    cdc_stack_name: "Optional[str]" = None,
    *,
    cdc_confirmed_absent: bool = False,
) -> Optional[str]:
    """Return a caution when resetting would orphan deployed CDC infrastructure.

    Resetting clears only the tool's session/workbench, NOT any AWS resources. If
    the session shows signs of a deployed cdc-stack (entered infra inputs, or a
    non-default stack name), warn the operator to tear it down FIRST via the CDC
    step's Delete action -- otherwise MSK/NAT keep billing with no session pointing at
    them. Returns ``None`` when there is nothing at risk.

    ``cdc_confirmed_absent`` short-circuits to ``None``: when a fresh live probe has
    just confirmed that NO CDC resource exists (the caller's ``cdc_deployed`` is
    False after a successful probe -- e.g. the user just finished deleting the
    stack), there is nothing to orphan, so the "MSK/NAT keep billing" caution would
    be misleading. Only the session's stale CDC-plan/infra-inputs remain, and those
    are cleared by the reset itself.

    ``cdc_stack_name`` is the session's current cdc-stack name (from the migration
    state). A fresh session re-discovers a still-deployed stack ONLY at the default
    name, so when a non-default (custom) name was used -- e.g. a second/parallel
    migration via the CDC step's "Advanced — CDC stack name" field -- the reset
    would leave it orphaned with no in-tool pointer. In that case name the exact
    stack in the warning so the operator knows precisely what to delete (here or in
    the AWS console). ``None`` / the default name keeps the generic guidance.
    """
    if cdc_confirmed_absent:
        return None
    # Entered infra inputs are the real signal that infrastructure may exist, so they
    # alone are enough. Previously this ALSO required the migration type to still name
    # a CDC mode -- but the type is freely switchable, so a user who deployed MSK and
    # then flipped back to Full-load-only got NO warning and could silently orphan a
    # billing cluster. A known non-default stack name is likewise sufficient: a fresh
    # session only re-discovers the DEFAULT name, so that is the case most at risk.
    from dsql_migrator.core.cdc import CDC_DEFAULT_STACK_NAME

    infra_getter = getattr(state, "cdc_infra_inputs", None)
    has_infra = bool(infra_getter()) if callable(infra_getter) else False
    custom_stack = bool(cdc_stack_name) and cdc_stack_name != CDC_DEFAULT_STACK_NAME
    if not (has_infra or custom_stack):
        return None

    if custom_stack:
        return (
            "Heads up: if you deployed CDC infrastructure, resetting does NOT delete "
            f"it — MSK/NAT keep billing. This session uses a custom cdc-stack named "
            f"'{cdc_stack_name}', which a fresh session will NOT re-discover, so "
            "delete it FIRST with 'Delete CDC infrastructure' on the CDC step (or, if "
            f"you already reset, delete the '{cdc_stack_name}' stack in the AWS "
            "console), then start over."
        )
    return (
        "Heads up: if you deployed CDC infrastructure, resetting does NOT delete "
        "it — MSK/NAT keep billing. Tear it down first with 'Delete CDC "
        "infrastructure' on the CDC step, then start over."
    )


def _open_start_over_dialog(
    ui: object,
    state: "object",
    on_reset: Callable[[], None],
    select: Callable[[object], None],
    refresh_all: Callable[[], None],
    connect_view: object,
    *,
    cdc_deployed: bool = False,
    on_reset_cdc: Optional[Callable[[str], None]] = None,
    cdc_stack_name: Optional[str] = None,
    cdc_stack_names: Optional[Sequence[str]] = None,
    cdc_teardown_in_flight: bool = False,
    cdc_op_in_flight: Optional[str] = None,
) -> None:
    """Type-to-confirm dialog that clears the session and returns to Connect.

    Clears only the tool's per-session workbench (connections, workflow progress,
    selections, the chosen plan, CDC inputs) and the persisted snapshot -- never
    any AWS resource. Shows the CDC-orphan caution when relevant. ``cdc_stack_name``
    (the session's current stack name) lets the warning name a custom stack a fresh
    session would not re-discover.

    ``cdc_teardown_in_flight`` BLOCKS the reset: when a CDC stop/delete is currently
    running (freshly probed), resetting would fire a second background teardown and
    then wipe the session, hiding the in-flight delete. In that case the dialog only
    explains this and offers Close -- no RESET input, no enabled confirm button.

    ``cdc_op_in_flight`` (``"infra"``/``"start"``) instead only WARNS: a deploy/start
    is re-discoverable and shorter, so resetting stays allowed but the dialog notes
    the job keeps running in the background (no silent orphaning, no trapping).
    """
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 520px"):  # type: ignore[attr-defined]
        ui.label("Start over — reset this session").classes(  # type: ignore[attr-defined]
            "text-lg font-semibold text-red-700"
        )

        # Hard block: a CDC teardown (stop/delete) is mid-flight. Do NOT let the
        # user start over and race it (a second teardown + a session wipe that would
        # make the running delete invisible, and unre-discoverable for a custom
        # stack name). Explain and offer only Close.
        if cdc_teardown_in_flight:
            render_notice(
                ui,
                tone="warning",
                header="A CDC teardown is already running",
                body=(
                    "Your previous CDC stop/delete is still in progress (deleting "
                    "the stack can take ~15–25 min). Starting over now would launch "
                    "a second teardown and then clear this session, hiding the "
                    "one already running. Wait for it to finish (watch the Data "
                    "Migration step), then Start over."
                ),
            )
            with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
                ui.button("Close", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
            dialog.open()
            return

        # Suppress the orphan-billing caution when the fresh probe confirmed no CDC
        # is deployed (cdc_deployed is False here -- the tiles branch above handled
        # the deployed case). Otherwise a user who just deleted the stack would see
        # a misleading "MSK/NAT keep billing" warning about infra that is gone.
        warning = _start_over_cdc_warning(
            state, cdc_stack_name, cdc_confirmed_absent=not cdc_deployed
        )
        ui.label(  # type: ignore[attr-defined]
            "This clears your connections, migration plan, workflow progress, table "
            "selections and saved session state, returning you to a fresh Connect "
            "screen. It does NOT change or delete any AWS resource (your DSQL "
            "cluster and migrated data are untouched)."
        ).classes("text-sm text-gray-700")
        # A non-teardown CDC job (Deploy infra / Start CDC) is still running. Unlike a
        # stop/delete (hard-blocked above), a deploy/start is re-discoverable and must
        # not trap a user escaping a stuck run -- so warn, don't block: reset is still
        # allowed, but the operator is told the job keeps running in the background.
        if cdc_op_in_flight in ("infra", "start"):
            op_label = (
                "CDC infrastructure deploy"
                if cdc_op_in_flight == "infra"
                else "Start CDC"
            )
            render_notice(
                ui,
                tone="warning",
                header=f"A {op_label} is still running",
                body=(
                    "Resetting is allowed, but that operation keeps running in the "
                    "background after the reset — the Data Migration step will pick it "
                    "up again once you reconnect. Consider waiting for it to finish "
                    "before starting over."
                ),
            )
        cdc_choice = {"mode": "none"}
        if cdc_deployed and on_reset_cdc is not None:
            cdc_choice["mode"] = "stop"
            # NAME the stack, and do not imply this session deployed it. The pipeline may
            # equally have been left by an earlier session or be in use by another window
            # onto the same account (e.g. a local UI beside the deployed app) -- the stack
            # carries no owner tag, so the tool genuinely cannot tell. "A CDC pipeline is
            # currently deployed" read as "yours", which makes "Leave CDC untouched" feel
            # like the wrong answer even when it is the right one. Naming it lets the
            # operator recognise a pipeline something else is using and leave it alone
            # deliberately.
            # Resolve the stack name(s) the teardown will really act on. The caller
            # passes the full list; fall back to the single name for older callers.
            targets = [str(n) for n in (cdc_stack_names or []) if str(n).strip()]
            if not targets and cdc_stack_name:
                targets = [cdc_stack_name]
            plural = len(targets) > 1
            named = f" ({', '.join(targets)})" if targets else ""
            noun = "cdc-stacks" if plural else "cdc-stack"
            render_notice(
                ui,
                tone="warning",
                header=(
                    f"{len(targets)} CDC pipelines are running on this account — what "
                    "should happen to them?"
                    if plural
                    else "A CDC pipeline is running on this account — what should happen "
                    "to it?"
                ),
                body=(
                    f"Start over only wipes this tool's session, but the {noun}"
                    f"{named} keep{'' if plural else 's'} running on AWS (and billing). "
                    + (
                        "They may include the one this session deployed, plus others left "
                        "by an earlier session or in use by another window onto this "
                        "account"
                        if plural
                        else "It may be the one this session deployed, or one left by an "
                        "earlier session or in use by another window onto this account"
                    )
                    + " — the stack carries no owner, so the tool cannot tell. Leave "
                    + ("them" if plural else "it")
                    + " untouched if something else is using "
                    + ("them." if plural else "it.")
                ),
            )
            # NAME the stacks in the destructive tiles, not just in the notice above.
            # "Delete all CDC infrastructure" does not say WHAT it deletes, and the one
            # thing an operator needs in order to answer safely is which pipeline is
            # about to be torn down -- especially when the account holds one they must
            # not touch. The notice's name alone is easy to read as context for the
            # question rather than as the delete target.
            listed = ", ".join(targets)
            scope = f" ({listed})" if targets else ""
            cdc_tiles_def = {
                "stop": (
                    "Remove connectors, keep infrastructure",
                    f"Deletes only the MSK connectors on {listed or 'the cdc-stack'}; "
                    "MSK / VPC / IAM stay for a fast restart (idle billing continues). "
                    "Recommended.",
                ),
                "delete": (
                    (
                        f"Delete all CDC infrastructure{scope}"
                        if not plural
                        else f"Delete all {len(targets)} CDC stacks{scope}"
                    ),
                    "Tears down "
                    + ("every stack listed" if plural else "the whole stack")
                    + " — stops MSK / NAT billing, but it takes ~45 min to recreate "
                    "later.",
                ),
                "none": (
                    "Leave CDC untouched",
                    "Nothing on AWS changes — the right choice when another window (e.g. "
                    "the deployed app) is using "
                    + ("these pipelines" if plural else "this pipeline")
                    + ". Billing continues, and the migration type stays locked until "
                    "the connectors are removed.",
                ),
            }

            @ui.refreshable  # type: ignore[misc]
            def _cdc_tiles() -> None:
                for value, (title, desc) in cdc_tiles_def.items():
                    selected = cdc_choice["mode"] == value
                    border = (
                        "border-blue-500 bg-blue-50"
                        if selected
                        else "border-gray-300 bg-white"
                    )
                    card = ui.card().classes(  # type: ignore[attr-defined]
                        f"w-full p-3 rounded-lg border {border} cursor-pointer "
                        "hover:border-blue-400 transition-colors gap-1"
                    )
                    card.on(
                        "click",
                        lambda _e=None, v=value: (
                            cdc_choice.__setitem__("mode", v),
                            _cdc_tiles.refresh(),
                        ),
                    )
                    with card:
                        with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
                            ui.icon(  # type: ignore[attr-defined]
                                "radio_button_checked"
                                if selected
                                else "radio_button_unchecked",
                                color="primary" if selected else "grey-6",
                            ).classes("text-lg")
                            ui.label(title).classes(  # type: ignore[attr-defined]
                                "text-sm font-semibold"
                            )
                        ui.label(desc).classes(  # type: ignore[attr-defined]
                            "text-xs text-gray-600 leading-snug"
                        )

            with ui.column().classes("w-full gap-2 mt-1"):  # type: ignore[attr-defined]
                _cdc_tiles()
        elif warning:
            render_notice(
                ui,
                tone="warning",
                header="Resetting does not delete CDC infrastructure",
                body=warning,
            )
        ui.label("Type RESET to confirm:").classes("text-sm text-gray-700")  # type: ignore[attr-defined]
        # debounce=0 so the typed value syncs to the server immediately (the gate
        # below reads it on each keystroke; the default debounce would lag the
        # button-enable behind the last character).
        confirm_input = ui.input(placeholder="RESET").classes("w-full").props("debounce=0")  # type: ignore[attr-defined]
        reset_btn = ui.button("Start over", icon="restart_alt").props("color=negative")  # type: ignore[attr-defined]
        reset_btn.props("disable")

        def _typed(_e=None) -> str:
            # Prefer the live event payload (synced immediately) and fall back to
            # the element value, so the gate never lags a debounced round-trip.
            val = getattr(_e, "args", None) if _e is not None else None
            if not isinstance(val, str):
                val = confirm_input.value
            return (val or "").strip().upper()

        def _check(_e=None) -> None:
            if _typed(_e) == "RESET":
                reset_btn.props(remove="disable")
            else:
                reset_btn.props("disable")

        confirm_input.on("input", _check)
        confirm_input.on("keyup", _check)

        def _go() -> None:
            # The button is only enabled by _check once "RESET" was typed, so the
            # click itself is the confirmation -- no re-gate on a possibly-lagging
            # value read.
            dialog.close()
            # Tear down CDC per the user's choice BEFORE wiping the session, so the
            # teardown captures the (about-to-be-reset) stack/region config. "stop"
            # removes only the 2 connectors (keeps infra, fast restart); "delete"
            # tears the whole stack; "none" leaves CDC running.
            mode = cdc_choice.get("mode", "none") if on_reset_cdc is not None else "none"
            if mode in ("stop", "delete") and on_reset_cdc is not None:
                try:
                    on_reset_cdc(mode)
                except Exception:  # noqa: BLE001 - never block the reset
                    pass
            on_reset()
            select(connect_view)
            refresh_all()
            ui.notify(  # type: ignore[attr-defined]
                "Session reset — starting fresh.", type="positive", position="top"
            )

        reset_btn.on("click", _go)
        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
    dialog.open()


_TEARDOWN_BANNER_POLL_SECONDS = 10.0


def _cdc_teardown_banner_copy(
    info: "Optional[dict]",
) -> "Optional[tuple[str, str, str]]":
    """Map CDC-lifecycle banner info to ``(tone, header, body)``, or ``None`` when
    nothing is in flight. Pure/side-effect-free so the copy is unit-testable
    independent of the render+poll wrapper. ``info`` is ``{"state":
    "running"|"failed", "kind": "stop"|"delete"|"infra", "stack": <name>}``.
    """
    if not info:
        return None
    state = info.get("state", "running")
    kind = info.get("kind")
    stack = info.get("stack") or "the cdc-stack"
    # A FINISHED teardown, held until the operator closes it. Completion used to be a
    # ui.notify toast, which dies with the page -- so after a refresh there was nothing
    # separating "the 45-minute teardown finished" from "it never ran", for an operation
    # explicitly designed to be walked away from.
    if state == "done":
        stacks = [s for s in (info.get("stacks") or []) if s] or [stack]
        listed = ", ".join(stacks)
        if kind == "delete":
            noun = "stack" if len(stacks) == 1 else "stacks"
            return (
                "success",
                "CDC infrastructure deleted",
                f"Teardown of the cdc-{noun} ({listed}) finished — MSK / NAT billing has "
                "stopped. Deploying CDC again later takes ~45 min from scratch.",
            )
        return (
            "success",
            "CDC connectors removed",
            f"The CDC connectors on {listed} are gone and streaming has stopped. MSK, "
            "the VPC wiring and the plugins were kept, so Start CDC can restart quickly "
            "— and it resumes from where streaming left off.",
        )
    if kind == "infra" and state != "failed":
        # An infrastructure create is the one CDC operation the user is meant to walk
        # AWAY from: it takes ~15-20 min and is supposed to overlap the Full Load. So
        # it needs a cross-view banner too -- otherwise, having left the Data
        # Migration screen, the user has no way to tell it is still running and waits
        # on it instead of starting the snapshot.
        return (
            "info",
            "CDC infrastructure is deploying in the background",
            f"Provisioning '{stack}' (Amazon MSK, ~10–15 min). Nothing is streaming "
            "yet, so this does not block the Full Load — start it now and let the two "
            "run together. This banner clears itself when the deploy finishes.",
        )
    if state == "failed":
        return (
            "error",
            "CDC teardown failed — action needed",
            f"Tearing down '{stack}' did not complete (CloudFormation reported "
            "DELETE_FAILED). MSK / NAT may still be billing. Retry the cleanup, or "
            "dismiss this to finish in the AWS console.",
        )
    # Several stacks in one teardown: say which of how many, or the banner names a single
    # stack and reads as if it were the only one (it then appeared to finish early while
    # the rest were still deleting and still billing).
    index, total = info.get("index"), info.get("total")
    if index and total and total > 1:
        # "the rest follow" only while some actually do -- on the last stack it would be
        # wrong, and it is the one point where the operator is deciding whether to wait.
        progress = f" ({index} of {total}"
        progress += "; the rest follow)" if index < total else ", the last one)"
    else:
        progress = ""
    if kind == "delete":
        return (
            "info",
            "CDC infrastructure teardown in progress",
            f"Deleting '{stack}'{progress} in the background (~15–45 min). MSK / NAT "
            "keep billing until it completes. You can keep working — this banner "
            "reports the result when the teardown finishes.",
        )
    return (
        "info",
        "Removing CDC connectors",
        f"Removing the CDC connectors from '{stack}'{progress} in the background. This "
        "banner reports the result when it finishes.",
    )


def _render_cdc_teardown_banner(
    ui: object,
    banner_getter: "Optional[Callable[[], Optional[dict]]]",
    *,
    on_retry: "Optional[Callable[[], None]]" = None,
    on_dismiss: "Optional[Callable[[], None]]" = None,
    done_getter: "Optional[Callable[[], Optional[dict]]]" = None,
    on_done_dismiss: "Optional[Callable[[], None]]" = None,
) -> None:
    """Persistent, cross-view banner for a CDC teardown (stop/delete).

    Shown on EVERY view -- including the Connect screen a Start-over → delete lands
    on -- so the operator always knows a teardown is still in flight (and that MSK /
    NAT keep billing until it finishes). Without it, a Start-over-triggered delete
    runs invisibly and the user cannot tell whether the infrastructure is gone yet.

    Two states from ``banner_getter``:
    * ``running`` -- an info notice that self-polls via a one-shot timer chain (the
      app-wide idiom that avoids the "parent slot deleted" crash a repeating timer
      causes when the region is torn down) and removes itself once the getter reports
      the teardown settled.
    * ``failed`` -- an error notice with a one-click **Retry cleanup** (``on_retry``,
      which re-runs the teardown / ``recover_delete_failed``) and **Dismiss**
      (``on_dismiss``, stop tracking here). No self-poll -- it is terminal until the
      user acts, then a refresh re-reads the state.
    """
    if banner_getter is None and done_getter is None:
        return

    @ui.refreshable  # type: ignore[attr-defined,misc]
    def _banner() -> None:
        try:
            info = banner_getter() if banner_getter is not None else None
        except Exception:  # noqa: BLE001 - a banner must never break the page render
            info = None
        # An in-flight teardown wins: while one is running the completion notice from an
        # earlier one is stale. Only when nothing is in flight does the finished-but-
        # undismissed result show.
        if not info and done_getter is not None:
            try:
                done = done_getter()
            except Exception:  # noqa: BLE001 - never break the page render
                done = None
            if done:
                info = {**done, "state": "done"}
        copy = _cdc_teardown_banner_copy(info)
        if copy is None:
            return  # nothing in flight (or just settled) → render nothing, stop polling
        tone, header, body = copy
        state = (info or {}).get("state", "running")
        # A finished teardown is reported until the operator closes it -- never auto-hidden
        # and never self-polled. Auto-hiding is what left "finished" indistinguishable
        # from "never ran" after a refresh.
        if state == "done":
            with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                with ui.column().classes("flex-1 min-w-0"):  # type: ignore[attr-defined]
                    render_notice(ui, tone=tone, header=header, body=body)
                if on_done_dismiss is not None:

                    def _close(_e=None) -> None:
                        on_done_dismiss()
                        _banner.refresh()  # type: ignore[attr-defined]

                    ui.button(icon="close", on_click=_close).props(  # type: ignore[attr-defined]
                        "flat dense round size=sm color=grey-7"
                    ).tooltip("Dismiss")
            return
        # A running teardown/deploy is a LIVE operation that takes ~15-45 min, so mark
        # the notice busy: an animated spinner + "In progress" badge make it obvious the
        # work is still moving. With only a static icon the banner read as an inert
        # message, and the user could not tell whether it had stalled.
        render_notice(
            ui, tone=tone, header=header, body=body, busy=(state != "failed")
        )
        if state == "failed":
            # Actionable, terminal-until-acted: retry re-launches the teardown (the
            # getter then reports "running" again); dismiss clears the marker. No poll.
            with ui.row().classes("gap-2 mt-1"):  # type: ignore[attr-defined]
                if on_retry is not None:

                    def _retry(_e=None) -> None:
                        on_retry()
                        _banner.refresh()  # type: ignore[attr-defined]

                    ui.button(  # type: ignore[attr-defined]
                        "Retry cleanup", icon="restart_alt", on_click=_retry
                    ).props("color=primary")
                if on_dismiss is not None:

                    def _dismiss(_e=None) -> None:
                        on_dismiss()
                        _banner.refresh()  # type: ignore[attr-defined]

                    ui.button("Dismiss", on_click=_dismiss).props(  # type: ignore[attr-defined]
                        "flat color=grey"
                    )
            return
        # running → re-arm a single-shot poll so the banner disappears (or flips to
        # the failed state) once the job settles.
        ui.timer(_TEARDOWN_BANNER_POLL_SECONDS, _banner.refresh, once=True)  # type: ignore[attr-defined]

    _banner()


