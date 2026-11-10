# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Prerequisites panel render helpers (extracted from ``data_migration/__init__``).

The prerequisite-check panel Full Load, CDC and the combined Full load + CDC type share
(``_render_prerequisites_panel``: the intro, the Check button and the results), the results
it renders by category (``_render_prereq_results``, ``_render_prereq_category``,
``_render_prereq_table`` and the status colors and icons they use), the combined panel's
per-phase verdicts (``_CDC_ONLY_CHECK_IDS``, ``prereq_phase_tag``, ``PrereqPhaseVerdict``,
``prereq_phase_verdicts``, ``_render_prereq_phase_verdicts``), and the PostgreSQL
gapless-handoff choice the panel shows beside a failure a re-snapshot resolves
(``_render_gapless_choice``).

The package ``__init__`` re-exports every name defined here, private ones included, at its
bottom: for its screen code (the Prerequisites sub-step calls ``_render_prerequisites_panel``
as a module global) and for the readers that reach them as ``dm.<name>`` / ``from
...data_migration import <name>``. A re-export does not carry a monkeypatch: a patch on
``dm.<name>`` reaches the package's calls only, and the calls made here read this module's
names, so a test patches them here. One-directional: this module imports, at its top, from
``_models`` and the shared ``core`` + ``ui`` modules -- never from the package ``__init__``,
or from ``_cdc_ui``, which imports names back from it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Sequence

from dsql_migrator.core.activity_log import (
    ActivityCategory,
    ActivityStatus,
    log_activity,
)
from dsql_migrator.core.models import (
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
)
from dsql_migrator.ui.design import WRAP_CELLS_PROP, notice_container, render_notice
from dsql_migrator.ui.data_migration._models import (
    MigrationType,
    PrereqCategoryGroup,
    group_prereq_results,
    prereq_mode_for_type,
    prereq_scope_gap,
    resnapshot_resolvable_failures,
)


# Checks that only CDC requires (MySQL binlog/GTID, PostgreSQL WAL/replication, MSK).
# Everything else is common to Full Load and CDC. Used to tag each result row with the
# phase that needs it so the combined "Full load + CDC" panel shows it covers both (the
# CDC run is a superset of the Full Load checks -- see core.prerequisites). The
# PostgreSQL CDC-only ids (WAL_LEVEL_LOGICAL / REPLICATION_ROLE / REPLICATION_SLOTS /
# SOURCE_IS_WRITER / REPLICA_IDENTITY) MUST be listed too: omitting them made a PG
# source's CDC-only failures (e.g. wal_level=replica, the RDS default) read as
# "Full Load: Blocked" on the combined panel, though Full Load does not need them.
_CDC_ONLY_CHECK_IDS = frozenset(
    {
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.BINLOG_RETENTION,
        PrerequisiteCheckId.GTID_MODE,
        PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
        PrerequisiteCheckId.REPLICATION_ROLE,
        PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
        PrerequisiteCheckId.REPLICATION_SLOTS,
        PrerequisiteCheckId.SOURCE_IS_WRITER,
        PrerequisiteCheckId.TABLE_REPLICABLE,
        PrerequisiteCheckId.REPLICA_IDENTITY,
        # Emphatically CDC-only: Full Load reads every one of the column types this row
        # names as jsonb and loads them correctly, so tagging it "Full load + CDC" would
        # report the load as affected by a gap that exists only in the stream.
        PrerequisiteCheckId.COLUMN_REPLICABLE,
        # SLOT_WAL_RETENTION had drifted out of this set, so the CDC-handoff WAL check --
        # the PostgreSQL counterpart of BINLOG_RETENTION, which IS listed -- was tagged
        # "Full load + CDC" on the combined panel even though only CDC needs it.
        PrerequisiteCheckId.SLOT_WAL_RETENTION,
        # Same drift risk as SLOT_WAL_RETENTION above: omitting it would tag a CDC-only
        # finding "Full load + CDC" and make it read as "Full Load: Blocked".
        PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
        PrerequisiteCheckId.MSK_AVAILABLE,
        PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
    }
)


def prereq_phase_tag(check_id: PrerequisiteCheckId, *, combined: bool) -> str:
    """Return the phase tag for a prerequisite check row.

    In the combined "Full load + CDC" panel, a common check is tagged
    ``"Full Load + CDC"`` (it gates both phases) and a CDC-only check ``"CDC"``,
    so the single check run visibly covers both phases. Off the combined panel
    the tag is empty (the panel title already names the single phase).
    """
    if not combined:
        return ""
    return "CDC" if check_id in _CDC_ONLY_CHECK_IDS else "Full Load + CDC"


@dataclass(frozen=True)
class PrereqPhaseVerdict:
    """Per-phase pass/block verdict within the combined Full-load-+-CDC report.

    ``phase`` is "Full Load" or "CDC"; ``can_proceed`` is True when no *required*
    check that gates THAT phase failed. ``blocking_titles`` lists the failing
    required checks for the phase (for a precise message).
    """

    phase: str
    can_proceed: bool
    blocking_titles: tuple[str, ...]


def prereq_phase_verdicts(report: object) -> list[PrereqPhaseVerdict]:
    """Split a combined report's gating verdict into a Full Load and a CDC verdict.

    The combined panel runs CDC-mode checks (a superset of Full Load's), so a
    single "Blocked" headline can't tell the operator WHICH phase is blocked. This
    derives two verdicts from the same results: the **Full Load** phase is gated
    only by the common (non-CDC-only) checks, while the **CDC** phase is gated by
    every check. So a binlog/replication failure correctly reads as "Full Load can
    proceed, CDC is blocked" instead of implying the Full Load itself is blocked.
    Pure: no UI, no AWS. ``report`` is duck-typed (needs ``.results``).
    """
    results = list(getattr(report, "results", []) or [])

    def _blocking(for_full_load: bool) -> tuple[str, ...]:
        titles: list[str] = []
        for r in results:
            if not getattr(r, "required", False):
                continue
            if r.status is not PrerequisiteStatus.FAIL:
                continue
            # The Full Load phase ignores CDC-only checks; CDC considers all.
            if for_full_load and r.check_id in _CDC_ONLY_CHECK_IDS:
                continue
            # Named as the run guard names them: a per-table check by its table, once.
            target = getattr(r, "target", None)
            title = r.title if target is None else f"{r.title} ({target})"
            if title not in titles:
                titles.append(title)
        return tuple(titles)

    full_blocking = _blocking(for_full_load=True)
    cdc_blocking = _blocking(for_full_load=False)
    return [
        PrereqPhaseVerdict("Full Load", not full_blocking, full_blocking),
        PrereqPhaseVerdict("CDC", not cdc_blocking, cdc_blocking),
    ]


def _render_prerequisites_panel(
    ui,
    migration_state,
    run_checks,
    mode: MigrationMode = MigrationMode.FULL_LOAD,
    *,
    combined: bool = False,
    load_running: bool = False,
    refresh=None,
    connections_block: Optional[tuple[str, str, str]] = None,
) -> bool:
    """Render a mode's prerequisite check button and result panel (Req 5.10).

    Shared by Full Load, CDC, and the combined "Full load + CDC" type so every
    step presents an identical panel (same intro line, outline button, spinner,
    grouped results table); only the wording and the ``mode`` differ. A failed
    required check blocks that mode.

    ``combined`` is set for the Full-load-+-CDC type: the checks still run in CDC
    ``mode`` (a superset of the Full Load checks), but the wording says "Full
    Load and CDC" and each result row is tagged with the phase that needs it, so
    the single run visibly covers both phases (no separate Full Load panel).

    ``load_running`` is ``True`` while a Full Load is actively IN_PROGRESS. The
    checks are read-only and never touch the running job, so re-running them
    mid-load is harmless but pointless (the fresh result applies only to the NEXT
    run, not the in-flight one) and adds avoidable source read load. When set, the
    Check button is disabled with an explanatory notice, matching how the
    migration-type selector locks during a run.

    ``refresh`` re-renders the screen. Only the gapless-handoff choice below needs it (to
    move the operator onto the Full Load sub-step once its re-graded checks pass); every
    other action here re-renders via ``run_checks``. Optional so a test can render the
    panel without one.

    ``connections_block`` is :func:`prereq_connection_block`'s ``(header, body, short)``
    while a connection is unverified in this session (e.g. a restored session after a
    restart). The checks are then held: a warning notice says why and where to go, the
    Check button is disabled with ``short`` as its tooltip, and so are the two gapless
    route buttons (each re-runs the checks after changing the migration type or start
    mode, so it would record the choice and then be refused). ``run_checks`` re-checks
    at click time, so this is the visible half of the gate, not the only one.

    **Returns** whether the gapless-handoff route choice was rendered. The caller draws
    the run guard directly below this panel, and when a choice is pending that guard would
    be the FOURTH statement of one failure's severity -- sitting *under* the solution, so
    the screen reads "here is how to fix it" then "you must fix it". Returning the fact
    rather than letting the caller re-derive it keeps the two in lockstep by construction.
    """
    is_cdc = mode is MigrationMode.CDC
    if combined:
        intro = (
            "Run read-only checks before migrating. These cover both phases -- "
            "the Full Load and the CDC stream; a failed required check blocks the "
            "migration."
        )
        button_label = "Check Full load + CDC prerequisites"
        running_text = "Running Full load + CDC prerequisite checks… (read-only)"
    elif is_cdc:
        intro = (
            "Run read-only checks before streaming. A failed required check "
            "blocks CDC."
        )
        button_label = "Check CDC prerequisites"
        running_text = "Running CDC prerequisite checks… (read-only)"
    else:
        intro = (
            "Run read-only checks before loading. A failed required check "
            "blocks the Full Load."
        )
        button_label = "Check Full Load prerequisites"
        running_text = "Running Full Load prerequisite checks… (read-only)"
    ui.label(intro).classes("text-sm text-gray-500")
    running = migration_state.is_prereq_running(mode)
    report = migration_state.get_prereq_report(mode)
    # Visible text ABOVE the disabled button, not only its tooltip: the guidance has to be
    # readable without hovering. Amber, not red: nothing failed, and the remedy is one
    # re-test on the Connect step.
    # Not while an operation runs either: the button stays disabled for THAT reason (the
    # grey line below says so), and "then run the checks here" would be false meanwhile.
    if connections_block is not None and not running and not load_running:
        _conn_header, _conn_body, _ = connections_block
        render_notice(
            ui,
            tone="warning",
            header=_conn_header,
            body=_conn_body
            + (
                # Reports are never persisted, so a report beside this notice was run
                # before a connection was edited (or failed a re-test) in this session.
                " The results below were produced before that and may not reflect the "
                "connection as it is now."
                if report is not None
                else ""
            ),
        )
    with ui.row().classes("items-center gap-2 flex-wrap"):
        # Keep the label visible while running (just disabled) -- Quasar's
        # `loading` prop would swap the label for a spinner, leaving an empty
        # outlined button with a lone spinning ring. The progress spinner lives
        # in the status line below instead, so there is exactly one spinner.
        check_btn = ui.button(
            "Checking…" if running else button_label,
            on_click=lambda: run_checks(mode),
        ).props("outline")
        # Disable while the check itself runs, OR while a Full Load is in flight
        # (a mid-load re-run is inert w.r.t. the running job and only adds source
        # read load), OR while a connection is unverified in this session (the notice
        # above says why). The tooltip explains the latter two.
        if running:
            check_btn.props("disable")
        elif load_running:
            check_btn.props("disable").tooltip(
                "A migration operation is in progress — prerequisite checks are "
                "read-only and apply to the NEXT run, not the one in progress."
            )
        elif connections_block is not None:
            check_btn.props("disable").tooltip(connections_block[2])
    # While a load runs, spell out that re-checking here won't affect it (the
    # button is disabled above; this notice says why, so the disabled state isn't
    # a mystery).
    if load_running and not running:
        ui.label(
            "A migration operation is in progress. These checks apply to the "
            "next run — they don't affect the running operation."
        ).classes("text-xs text-gray-400")
    # Immediate feedback directly below the button while a check runs, so the
    # click is acknowledged at once instead of seeming unresponsive.
    if running:
        with ui.row().classes("items-center gap-2"):
            ui.spinner(size="sm")
            ui.label(running_text).classes("text-sm text-gray-600")
    if report is None:
        return False
    # A report can outlive the selection it covered (nothing clears it, and the
    # picker stays editable until a migration commits). If tables were ADDED since,
    # the verdict below is stale for them -- they were never checked -- and the run
    # guard blocks on exactly this. Surface it here too so the reason sits next to
    # the results the user is reading, not only on a disabled button elsewhere.
    added = prereq_scope_gap(report, migration_state.selection.selected_tables)
    if added:
        listed = ", ".join(added[:6]) + (
            f" +{len(added) - 6} more" if len(added) > 6 else ""
        )
        many = len(added) > 1
        render_notice(
            ui,
            tone="warning",
            header="These results don't cover your current selection",
            body=(
                f"{listed} {'were' if many else 'was'} added after these checks ran, so "
                f"{'they were' if many else 'it was'} never checked. Re-run the checks to "
                "cover the full selection before migrating."
            ),
        )
    _render_prereq_results(ui, mode, report, combined=combined)
    # The continuation affordance, BESIDE the failing row rather than in a dialog behind
    # the button this very failure disables. Without it a PostgreSQL "Full load only ->
    # CDC only" operator has no route at all: Deploy is gated on this FAIL, and the only
    # place that could record the re-snapshot decision was the Start CDC dialog, which
    # needs infrastructure that cannot be deployed.
    if mode is MigrationMode.CDC and resnapshot_resolvable_failures(report):
        _render_gapless_choice(
            ui,
            migration_state,
            run_checks,
            refresh=refresh,
            blocked_reason=(
                connections_block[2] if connections_block is not None else None
            ),
        )
        return True
    return False


def _render_gapless_choice(
    ui, migration_state, run_checks, *, refresh=None, blocked_reason=None
) -> None:
    """The PostgreSQL "Full load only -> CDC" fork: ONE situation, two routes, a button each.

    Shipped first (v0.1.510/511) as three stacked notices -- amber situation, blue
    recommendation, blue alternative -- plus a lone flat button. That layout said one
    problem's severity four times (Blocked badge, "1 failed" badge, amber notice, red
    guard line) with two blue panels wedged in between, and it left the ONLY clickable
    action on the route it explicitly does not recommend: visual pull and stated advice
    pointing opposite ways, with the recommended route reduced to "change the migration
    type above and run the Full Load again" -- instructions for work the tool can do
    itself. The codebase had already recorded this failure mode once, in
    ``stale_error_notice``: "three verdicts at once, and the user cannot tell which is
    true."

    So: one box states the situation, and the two routes sit inside it as peers, each with
    its own button and a one-line summary of what it leaves behind. Costs and mechanics go
    in a collapsed expansion, because the operator picking a route needs the difference
    (~11 lines of prose to reach "there are two options" was the complaint), not the full
    derivation up front.

    Both handlers RE-RUN the (read-only, seconds) prerequisite checks, so one re-graded
    report clears every gate at once instead of leaving a red FAIL beside an enabled
    primary button.

    ``blocked_reason`` (set while a connection is unverified) disables both buttons with
    it as the tooltip: each handler changes the migration type or start mode BEFORE its
    re-run, and that re-run would then be refused, so the choice would be recorded but
    not re-graded until the connections are re-tested.
    """
    # RECOMMENDED FIRST, and deliberately so -- against the usual "primary action on the
    # right" rule, which governs a form's action row, not a two-way fork where reading
    # order IS the recommendation. Both routes re-read every table, so the re-read is not
    # what distinguishes them; re-running as "Full load + CDC" (a) uses the tool's own bulk
    # loader rather than the streaming pipeline, (b) creates the slot BEFORE the load so
    # everything from the snapshot point on is streamed, and (c) can DROP each target table
    # first, which is the only way to clear rows deleted on the source since the last load.
    # The re-snapshot route leaves those rows behind forever. Same cost, strictly better
    # result -- so it leads, and it is the one that carries color=primary.
    async def _start_over() -> None:
        migration_state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
        # Clear any re-snapshot decision recorded earlier in this session. Without this, an
        # operator who clicked "Re-snapshot every table" first and then changed their mind
        # would get snapshot.mode=initial + publication.autocreate=filtered on the very run
        # whose entire point is to stream from the slot the tool creates at the snapshot
        # point -- silently paying for a second full read and discarding the gapless
        # handoff this route exists to restore.
        migration_state.set_cdc_start_mode("auto")
        log_activity(
            ActivityCategory.FULL_LOAD,
            "migration type selected",
            status=ActivityStatus.INFO,
            detail=(
                "migration type set to "
                f"{MigrationType.FULL_LOAD_AND_CDC.value} to restore a gapless "
                "CDC handoff"
            ),
        )
        _mode = prereq_mode_for_type(MigrationType.FULL_LOAD_AND_CDC)
        await run_checks(_mode)
        # Land on the Full Load screen -- the work this route needs next -- but ONLY when
        # the re-graded report says it can actually run. Navigating unconditionally would
        # drop the operator on a blocked Run button with the explanation left behind on the
        # panel they were moved off.
        _report = migration_state.get_prereq_report(_mode)
        if _report is not None and getattr(_report, "can_proceed", False):
            migration_state.close_prereq_review()
            migration_state.set_active_substep("full_load")
            if refresh is not None:
                refresh()

    async def _take_resnapshot() -> None:
        # ONE knob. "manual" forces snapshot.mode=initial, and
        # publication.autocreate.mode=filtered is DERIVED from that, so the connector's own
        # database user creates the publication this load never had (over exactly the
        # captured tables -- the tool itself still never writes to the source). This used to
        # be two independent values that both had to be set: v0.1.513 fixed this call site
        # by setting both, and the pair still broke from the start-position radio and from a
        # session restore (the second flag was never persisted), because discipline cannot
        # hold an invariant that the type system can.
        migration_state.set_cdc_start_mode("manual")
        log_activity(
            ActivityCategory.CDC,
            "CDC start mode selected",
            status=ActivityStatus.INFO,
            detail="re-snapshot every selected table (no gapless slot for this load)",
        )
        await run_checks(MigrationMode.CDC)

    def _route(
        *, label: str, icon: str, primary: bool, summary: str, detail: str, on_click
    ) -> None:
        with ui.column().classes("flex-1 min-w-0 gap-1"):
            btn = ui.button(label, icon=icon, on_click=on_click)  # type: ignore[attr-defined]
            btn.props("color=primary" if primary else "outline color=primary")
            if blocked_reason:
                btn.props("disable").tooltip(blocked_reason)
            # A fixed floor on the summary height keeps the two routes' expansions on the
            # same baseline: the summaries wrap to different line counts, and a ragged pair
            # reads as two unrelated blocks rather than two options being compared.
            ui.label(summary).classes(  # type: ignore[attr-defined]
                "text-xs text-gray-700 min-h-[2.25rem]"
            )
            # header-class strips Quasar's own header padding so the expansion's label lines
            # up with the summary above it instead of sitting indented from it.
            with ui.expansion("What this does, and what it costs").props(  # type: ignore[attr-defined]
                'dense header-class="q-px-none"'
            ).classes("w-full text-xs"):
                ui.label(detail).classes("text-xs text-gray-700")  # type: ignore[attr-defined]

    with notice_container(
        ui,
        tone="warning",
        icon="history_toggle_off",
        header="A gapless handoff is no longer possible for this load",
        body=(
            "Nothing was retaining WAL while the Full Load ran, and a replication slot "
            "cannot be created at a past position, so the changes committed since the "
            "load cannot be replayed. Both ways forward re-read the source tables. They "
            "differ in what they leave behind."
        ),
    ):
        with ui.row().classes("w-full gap-4 items-start flex-wrap"):  # type: ignore[attr-defined]
            _route(
                label='Start over as "Full load + CDC"',
                icon="replay",
                primary=True,
                summary=(
                    "Recommended — also clears rows deleted on the source since the load."
                ),
                detail=(
                    "Switches the migration type and re-runs the prerequisite checks for "
                    "it, then opens the Full Load. The tool creates the replication slot "
                    "at the new snapshot point BEFORE loading, so CDC resumes from it with "
                    "no gap — and you can start the CDC phase whenever you like, it does "
                    "not have to be now. Choose to DROP each table when the load asks: "
                    "that also clears any row deleted on the source since the first load, "
                    "which nothing else here can do. It reads the source again, but so "
                    "does the alternative, and it uses the bulk loader rather than the "
                    "streaming pipeline."
                ),
                on_click=_start_over,
            )
            _route(
                label="Re-snapshot every table",
                icon="restart_alt",
                primary=False,
                summary=(
                    "Keeps this Full Load — but a row deleted on the source since the "
                    "load stays on the target."
                ),
                detail=(
                    "Adds CDC without a second load pass: the connector creates its own "
                    "publication over exactly the captured tables, snapshots them, then "
                    "streams from the slot it made — there is no window between the "
                    "snapshot and the stream. Choose this if re-running the load is not "
                    "acceptable. Two costs: the source tables are read again through the "
                    "streaming pipeline, and a row DELETED between the load and the start "
                    "is in neither the snapshot nor the stream, so it stays on the target "
                    "(Validation reports it as an extra row — reload that table to clear "
                    "it)."
                ),
                on_click=_take_resnapshot,
            )


# Quasar color names for each prerequisite check status badge. INFO is a calm
# blue (an optional recommendation / expected state), distinct from the amber
# WARN (something is off).
_PREREQ_STATUS_COLORS: dict[PrerequisiteStatus, str] = {
    PrerequisiteStatus.PASS: "positive",
    PrerequisiteStatus.FAIL: "negative",
    PrerequisiteStatus.WARN: "warning",
    PrerequisiteStatus.INFO: "info",
    PrerequisiteStatus.SKIP: "grey",
}

# Material icon name for each prerequisite status, shown on the category header.
_PREREQ_STATUS_ICONS: dict[PrerequisiteStatus, str] = {
    PrerequisiteStatus.PASS: "check_circle",
    PrerequisiteStatus.FAIL: "error",
    PrerequisiteStatus.WARN: "warning",
    PrerequisiteStatus.INFO: "info",
    PrerequisiteStatus.SKIP: "remove_circle_outline",
}


def _render_prereq_results(
    ui, mode: MigrationMode, report: PrerequisiteReport, *, combined: bool = False
) -> None:
    """Render one mode's prerequisite results grouped by category (Req 5.10).

    Shows the gating verdict, then one collapsible section per category
    (Connectivity / Source Configuration / Schema & Tables / Streaming). A
    section that needs attention (a blocking failure or a warning) starts
    expanded; passing or not-applicable sections start collapsed to keep the
    panel scannable. When ``combined``, the verdict label names both phases and
    each row carries a phase tag.
    """
    if combined:
        label = "Full load + CDC"
    elif mode == MigrationMode.FULL_LOAD:
        label = "Full Load"
    else:
        label = "CDC"
    verdict = "Can proceed" if report.can_proceed else "Blocked"
    color = "positive" if report.can_proceed else "negative"
    with ui.row().classes("items-center gap-2"):
        ui.label(f"{label} prerequisites").classes("text-sm font-semibold")
        ui.badge(verdict).props(f"color={color}")
    # In the combined panel, also show a PER-PHASE verdict so a CDC-only failure
    # (e.g. binlog not ROW) reads as "Full Load can proceed, CDC blocked" instead
    # of a single ambiguous "Blocked" that looks like the Full Load itself failed.
    if combined:
        _render_prereq_phase_verdicts(ui, report)
    with ui.column().classes("w-full gap-1"):
        for group in group_prereq_results(report.results):
            _render_prereq_category(ui, group, combined=combined)


def _render_prereq_phase_verdicts(ui, report) -> None:
    """Render the Full Load / CDC per-phase verdict badges + a clarifying note."""
    verdicts = prereq_phase_verdicts(report)
    with ui.row().classes("items-center gap-3 flex-wrap"):
        for v in verdicts:
            color = "positive" if v.can_proceed else "negative"
            text = f"{v.phase}: {'Can proceed' if v.can_proceed else 'Blocked'}"
            ui.badge(text).props(f"color={color} outline")
    full = next((v for v in verdicts if v.phase == "Full Load"), None)
    cdc = next((v for v in verdicts if v.phase == "CDC"), None)
    # When only the CDC phase is blocked, spell out that the Full Load can still
    # run -- this is the common "binlog/replication not ready" case.
    if full is not None and cdc is not None and full.can_proceed and not cdc.can_proceed:
        # Said as the run guard holds it: a Full load + CDC run starts only once CDC can
        # follow it (the load's start point is the stream's), so 'the Full Load can run
        # now' was not so on this panel.
        blocking = list(cdc.blocking_titles)
        listed = "; ".join(blocking[:5]) + (
            f"; and {len(blocking) - 5} more" if len(blocking) > 5 else ""
        )
        render_notice(
            ui,
            tone="warning",
            header="CDC stream blocked",
            body="The Full Load itself needs none of these, but this Full load + CDC run "
            f"starts only once CDC can follow it ({listed}). Fix them to start it, or "
            "switch to Full load only to load now.",
        )


def _render_prereq_category(
    ui, group: PrereqCategoryGroup, *, combined: bool = False
) -> None:
    """Render one category as a collapsible section with a status header."""
    color = _PREREQ_STATUS_COLORS[group.status]
    icon = _PREREQ_STATUS_ICONS[group.status]
    # Draw attention to sections that need action; collapse the rest.
    expanded = group.status in (PrerequisiteStatus.FAIL, PrerequisiteStatus.WARN)
    with ui.expansion(value=expanded).classes("w-full").props(
        "expand-separator"
    ) as exp:
        with exp.add_slot("header"):
            with ui.row().classes("items-center gap-2 w-full no-wrap"):
                ui.icon(icon).props(f"color={color}")
                ui.label(group.category.value).classes("text-sm font-semibold")
                ui.badge(group.summary).props(f"color={color} outline")
        _render_prereq_table(ui, group.results, combined=combined)
        # NOTE: no cdc-stack deploy guide inside a category. MSK/MSK Connect are not
        # prerequisites: the cdc-stack (MSK) is deployed on this step -- offered at
        # the bottom of Prerequisites for Full load + CDC, otherwise on the CDC step --
        # and the connectors are created at Start CDC. So they report an
        # informational INFO here (expected, no action needed), never a blocking FAIL.


def _render_prereq_table(
    ui, results: Sequence[PrerequisiteResult], *, combined: bool = False
) -> None:
    """Render one category's checks as a compact results table.

    In the combined "Full load + CDC" panel an extra "Phase" column tags each
    check with the phase that needs it (Full Load + CDC vs CDC), so the single
    run visibly covers both phases.
    """
    columns = [
        {"name": "check", "label": "Check", "field": "check", "align": "left"},
        {"name": "status", "label": "Status", "field": "status"},
        {
            "name": "detail",
            "label": "Detail / remediation",
            "field": "detail",
            "align": "left",
        },
    ]
    if combined:
        # Insert the Phase tag column right after the check name.
        columns.insert(
            1, {"name": "phase", "label": "Phase", "field": "phase", "align": "left"}
        )
    rows = [
        {
            "check": result.title
            + (f" ({result.target})" if result.target else ""),
            "status": result.status.value,
            # BOTH, not "remediation or detail". ``detail`` carries the OBSERVED value and
            # ``remediation`` the instruction, and preferring remediation dropped the
            # measurement on every FAIL/WARN row -- so SLOT_WAL_RETENTION told the operator
            # to raise a cap whose current value the panel never showed, and a REPLICA
            # IDENTITY failure named no partition. The finding first, then what to do.
            "detail": " ".join(
                part for part in (result.detail, result.remediation) if part
            ),
            **(
                {"phase": prereq_phase_tag(result.check_id, combined=True)}
                if combined
                else {}
            ),
        }
        for result in results
    ]
    # WRAP the cells. "Detail / remediation" is by far the widest column -- a failed
    # PostgreSQL replication-objects check carries ~800 characters of detail plus
    # remediation -- and on one line it pinned the table ~3.5x its card, so the row was
    # clipped ("The connector does n...") and only reachable through a horizontal
    # scrollbar. The longest remediation is the one the operator most needs and was the
    # least visible. Costs row height, which the page can scroll; see WRAP_CELLS_PROP.
    ui.table(columns=columns, rows=rows, row_key="check").classes("w-full").props(
        WRAP_CELLS_PROP
    )
