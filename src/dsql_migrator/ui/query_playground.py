# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Query Playground screen (Optional tools): convert + test a MySQL query on DSQL.

A standalone, optional tool (not part of the four-step migration) where a user
pastes a real MySQL statement and sees:

1. how it converts to Aurora DSQL (PostgreSQL) -- the original and converted SQL
   side by side, plus any conversion warnings (lock anti-patterns, unconvertible
   idioms) -- via :class:`~dsql_migrator.core.query_converter.QueryConverter`, and
2. (optionally) whether the converted statement actually runs on the target,
   probed non-destructively by
   :func:`~dsql_migrator.core.query_playground.probe_statement`: a ``SELECT`` is
   ``EXPLAIN``-validated read-only, ``DDL`` is executed as a rolled-back dry run,
   and ``DML`` is never executed against the (production) target.

It is a *playground*: nothing it does mutates the source (it never touches the
source at all) or persists to the target. The conversion is pure and always
available; the "Test on target" action requires a verified target connection.

As with the workflow step screens, the run orchestration and presentation
helpers here are independent of NiceGUI so they can be unit tested directly; only
:func:`build_query_playground_screen` touches NiceGUI widgets.
"""

from __future__ import annotations

import re
import threading
from dataclasses import dataclass, field
from typing import Callable, Optional

from dsql_migrator.core.assessment_strategist import (
    build_query_chat_system,
    build_query_optimize_system,
)
from dsql_migrator.core.models import Classification, TargetConnectionConfig
from dsql_migrator.core.query_converter import (
    QueryConversionResult,
    QueryConverter,
    StatementKind,
)
from dsql_migrator.core.query_playground import (
    ExecutionProbe,
    ProbeOutcome,
    TargetConnectionFactory,
    probe_statement,
)
from dsql_migrator.core.target_connection import DsqlConnector
from dsql_migrator.ui.ai_chat_drawer import build_chat_drawer
from dsql_migrator.ui.design import badge_classes, render_notice, section_header
from dsql_migrator.ui.session import SessionStore

# ---------------------------------------------------------------------------
# Presentation helpers (NiceGUI-agnostic)
# ---------------------------------------------------------------------------

# Classification -> (notice tone, short label) for the conversion verdict.
_CLASSIFICATION_TONE: dict[Classification, tuple[str, str]] = {
    Classification.AUTO: ("success", "Auto-converted"),
    Classification.MANUAL: ("warning", "Manual review"),
    Classification.UNSUPPORTED: ("error", "Unsupported"),
}

# StatementKind -> (chip tone, label, what test-run does) for the kind banner.
_KIND_META: dict[StatementKind, tuple[str, str, str]] = {
    StatementKind.SELECT: (
        "active",
        "SELECT",
        "Testable read-only with EXPLAIN (plans the query; returns no data).",
    ),
    StatementKind.DDL: (
        "active",
        "DDL",
        "Testable as a dry run (executed inside a transaction that is rolled back; "
        "nothing is persisted).",
    ),
    StatementKind.DML: (
        "neutral",
        "DML",
        "Not test-run: the playground never executes INSERT/UPDATE/DELETE against "
        "the target.",
    ),
    StatementKind.OTHER: (
        "neutral",
        "Other",
        "Not test-run: only SELECT (EXPLAIN) and DDL (dry run) are probed.",
    ),
}


def classification_tone(classification: Classification) -> tuple[str, str]:
    """Return the ``(tone, label)`` notice styling for a conversion classification."""
    return _CLASSIFICATION_TONE.get(classification, ("info", classification.value))


def kind_meta(kind: StatementKind) -> tuple[str, str, str]:
    """Return the ``(badge_tone, label, test_run_explanation)`` for a statement kind."""
    return _KIND_META.get(kind, _KIND_META[StatementKind.OTHER])


def is_testable(result: QueryConversionResult) -> bool:
    """Return whether the converted statement can be probed on the target.

    Only a successfully converted ``SELECT`` (EXPLAIN) or ``DDL`` (dry run) is
    test-run; DML and OTHER are never executed, and a statement that did not
    convert has nothing to test.
    """
    if result.converted_sql is None:
        return False
    return result.statement_kind in (StatementKind.SELECT, StatementKind.DDL)


_SQL_FENCE_RE = re.compile(r"```(?:sql)?\s*\n?(.*?)```", re.IGNORECASE | re.DOTALL)
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)


def _strip_sql_comments(sql: str) -> str:
    """Remove SQL comments so a rewrite runs cleanly under an EXPLAIN wrapper.

    Drops ``/* ... */`` block comments and any ``-- ...`` line comment (to end of
    line). Critically, a leading ``--`` comment inside the reply would otherwise
    comment out the ``EXPLAIN`` we prepend (turning the whole probe into a
    comment) and cause a syntax error. Keeps string literals intact for the common
    case (comment markers inside quotes are rare in a rewrite and not worth a full
    tokenizer here).
    """
    sql = _BLOCK_COMMENT_RE.sub(" ", sql)
    lines = []
    for line in sql.splitlines():
        idx = line.find("--")
        lines.append(line[:idx] if idx != -1 else line)
    return "\n".join(lines)


def _last_testable_statement(sql: str) -> Optional[str]:
    """Return the last EXPLAIN-testable statement (SELECT/WITH) from ``sql``.

    An AI rewrite reply can contain MORE than the query — e.g. a suggested
    ``CREATE INDEX ASYNC ...;`` followed by the ``SELECT``. Passing the whole
    thing to ``EXPLAIN`` is a syntax error, and the playground can't run DDL here
    anyway. Split on statement boundaries (``;``) and return the LAST statement
    that starts with SELECT or WITH (a CTE) — that's the query to plan. Returns
    ``None`` when no such statement exists. Pure/testable.
    """
    # Split on semicolons (naive but sufficient: rewrites rarely embed ';' in a
    # string literal; if they do, the target simply rejects it and we surface it).
    parts = [p.strip() for p in sql.split(";")]
    for part in reversed(parts):
        if not part:
            continue
        head = part.lstrip("(").lstrip().upper()
        if head.startswith("SELECT") or head.startswith("WITH"):
            return part
    return None


def extract_sql_from_reply(markdown: str) -> Optional[str]:
    """Extract the runnable rewritten query from an AI reply, or ``None``.

    Pulls the LAST fenced ```sql block (the model's final proposal — it may show a
    'before' block first), strips SQL comments, and returns only the last
    EXPLAIN-testable statement (SELECT / WITH). This deliberately ISOLATES the
    exact query text so it runs cleanly under the ``EXPLAIN`` wrapper: it drops
    accompanying DDL (e.g. a suggested ``CREATE INDEX``, which the read-only
    playground can't run) and inline comments (a leading ``--`` would otherwise
    comment out the EXPLAIN and cause a syntax error). Returns ``None`` when no
    fenced block or no testable statement is present. Pure/testable.
    """
    if not markdown:
        return None
    blocks = _SQL_FENCE_RE.findall(markdown)
    for block in reversed(blocks):
        cleaned = _strip_sql_comments(block).strip()
        if not cleaned:
            continue
        stmt = _last_testable_statement(cleaned)
        if stmt:
            return stmt
    return None


def _pretty_sql(sql: str) -> str:
    """Pretty-print a SQL statement for readable display, best-effort.

    The re-test turn is shown verbatim in the chat drawer, so a one-line rewritten
    query reads much better multi-lined. Uses ``sqlglot`` (already a dependency)
    with the PostgreSQL dialect (DSQL is PG-wire). Falls back to the original text
    if parsing fails — never raises, since this is display polish, not correctness.
    """
    text = (sql or "").strip().rstrip(";")
    if not text:
        return text
    try:
        import sqlglot

        formatted = sqlglot.transpile(
            text, read="postgres", write="postgres", pretty=True
        )
        return formatted[0] if formatted else text
    except Exception:  # noqa: BLE001 - display only; keep the original on any error
        return text


def _sql_block(sql: str) -> str:
    """Wrap SQL in a fenced ```sql block (pretty-printed) for the chat markdown."""
    return f"```sql\n{_pretty_sql(sql)}\n```"


def _plan_block(plan: Optional[str]) -> str:
    """Wrap an EXPLAIN plan in a plain fenced block, trimmed for the chat."""
    return f"```\n{(plan or '').strip()[:2000]}\n```"


def _build_retest_turn(
    probe: ExecutionProbe,
    baseline_dpu: Optional[float],
    rewrite_sql: Optional[str] = None,
) -> str:
    """Compose the follow-up chat turn that reports a rewrite's re-test result.

    The operator clicked "Test rewrite on target": we re-ran the AI's proposed
    SQL on DSQL and captured a fresh probe. Rather than print raw numbers, we feed
    the deterministic outcome (did it run, its DPU, and the measured before/after
    delta vs the original's ``baseline_dpu``) back to the SAME assistant as a user
    turn, so the AI reports — in the drawer, in its own words — whether and by how
    much the rewrite actually improved. The tested query and the plan are rendered
    in fenced (pretty-printed) code blocks so the drawer bubble is readable.
    Pure/testable: builds only the text.
    """
    tested = f"\n\nThe query I actually ran:\n{_sql_block(rewrite_sql)}" if rewrite_sql else ""
    if probe.outcome is ProbeOutcome.FAILED:
        detail = probe.detail + (
            f" (SQLSTATE {probe.error_code})" if probe.error_code else ""
        )
        return (
            "I re-tested your rewritten query on the Aurora DSQL target and it was "
            f"REJECTED: {detail}{tested}\n\nExplain briefly why it failed and give a "
            "corrected rewrite that still returns the same results."
        )
    new_dpu = probe.dpu.total if probe.dpu is not None else None
    if new_dpu is None:
        return (
            "I re-tested your rewritten query on the target and it ran, but no DPU "
            "cost estimate was captured (the plan was EXPLAIN-only)."
            f"{tested}\n\nHere is its query plan:\n\n{_plan_block(probe.plan)}\n\n"
            "Briefly, does this plan confirm your rewrite is more efficient (scan "
            "type / filter layer), and is there anything more to improve?"
        )
    if baseline_dpu is None:
        return (
            "I re-tested your rewritten query on the target with EXPLAIN ANALYZE. "
            f"Its measured cost is {new_dpu:.5f} DPU total. The original query was "
            "not measured with ANALYZE, so I don't have a baseline. Based on this "
            "plan and DPU, is the rewrite efficient, and what else could improve it?"
            f"{tested}\n\nPlan:\n{_plan_block(probe.plan)}"
        )
    delta = baseline_dpu - new_dpu
    pct = (delta / baseline_dpu * 100.0) if baseline_dpu else 0.0
    direction = (
        "CHEAPER" if delta > 0 else "MORE EXPENSIVE" if delta < 0 else "about the same"
    )
    return (
        "I re-tested your rewritten query on the Aurora DSQL target with EXPLAIN "
        f"ANALYZE. Measured cost: original ≈ {baseline_dpu:.5f} DPU vs rewrite ≈ "
        f"{new_dpu:.5f} DPU — the rewrite is {direction} "
        f"({abs(delta):.5f} DPU, {abs(pct):.1f}%).{tested}\n\n"
        "Explain to me what this means: did your rewrite actually improve things, "
        "by how much, and why (point to the change in scan type / filter layer / "
        f"bytes moved)? If it did NOT improve, say so honestly and suggest the next "
        f"step. Rewrite's plan:\n{_plan_block(probe.plan)}"
    )


def probe_outcome_tone(outcome: ProbeOutcome) -> str:
    """Return the notice tone for a probe outcome (success/error/info)."""
    if outcome is ProbeOutcome.PASSED:
        return "success"
    if outcome is ProbeOutcome.FAILED:
        return "error"
    return "info"


# ---------------------------------------------------------------------------
# Per-session playground state
# ---------------------------------------------------------------------------


class PlaygroundState:
    """Per-session Query Playground inputs/outputs.

    ``sql`` is the editable input; ``result`` is the last conversion and ``probe``
    the last target test. ``probe`` is produced by a background worker (the test
    is a network round-trip) and read by the UI poller, so it is guarded by a lock
    for the cross-thread handoff (mirroring the step screens). AI assistance is
    handled by the shared right chat drawer (the same component the Evaluation /
    Schema Conversion screens use), which owns its own conversation state, so none
    is kept here.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.sql: str = ""
        self.result: Optional[QueryConversionResult] = None
        self._probe: Optional[ExecutionProbe] = None
        self._probing: bool = False
        # When True, a SELECT target test uses EXPLAIN ANALYZE (actually runs the
        # query for real timings/row counts) instead of plan-only EXPLAIN. Off by
        # default because ANALYZE executes the query; read/written on the UI thread.
        self.analyze: bool = False

    def set_result(self, result: QueryConversionResult) -> None:
        """Record the latest conversion and clear any stale probe verdict."""
        self.result = result
        with self._lock:
            self._probe = None

    def begin_probe(self) -> None:
        """Mark a target test as in flight (clears the previous verdict)."""
        with self._lock:
            self._probing = True
            self._probe = None

    def set_probe(self, probe: ExecutionProbe) -> None:
        """Record a finished target-test verdict."""
        with self._lock:
            self._probe = probe
            self._probing = False

    @property
    def probe(self) -> Optional[ExecutionProbe]:
        """Return the last target-test verdict, if any."""
        with self._lock:
            return self._probe

    @property
    def probing(self) -> bool:
        """Return whether a target test is currently in flight."""
        with self._lock:
            return self._probing

    def clear(self) -> None:
        """Discard the conversion + probe outputs (keep the typed SQL)."""
        with self._lock:
            self._probe = None
            self._probing = False
        self.result = None


@dataclass
class PlaygroundStore:
    """Process-memory map of session id to :class:`PlaygroundState`.

    Mirrors the other per-session stores: each UI session sees only its own
    playground state and nothing is persisted to disk (Property 7).
    """

    _states: dict[str, PlaygroundState] = field(default_factory=dict)

    def get_or_create(self, session_id: str) -> PlaygroundState:
        """Return the state for ``session_id``, creating an empty one if needed."""
        state = self._states.get(session_id)
        if state is None:
            state = PlaygroundState()
            self._states[session_id] = state
        return state

    def get(self, session_id: str) -> Optional[PlaygroundState]:
        """Return the state for ``session_id``, or ``None`` if absent."""
        return self._states.get(session_id)

    def reset_in_place(self, session_id: Optional[str]) -> None:
        """Reset the state WITHOUT replacing the object (no-op if absent)."""
        if session_id is None:
            return
        state = self._states.get(session_id)
        if state is not None:
            state.__init__()  # type: ignore[misc]  # re-run init on the same object


# ---------------------------------------------------------------------------
# NiceGUI screen
# ---------------------------------------------------------------------------

# How often the screen polls the background target-test (seconds).
_POLL_INTERVAL_SECONDS = 0.4

# A small starter so the empty playground is self-explanatory.
_PLACEHOLDER_SQL = (
    "SELECT * FROM orders WHERE id = 1\n"
    "-- try: INSERT ... ON DUPLICATE KEY UPDATE, SELECT ... FOR UPDATE, CREATE TABLE ..."
)


def _default_target_connection_factory(
    target: TargetConnectionConfig, aws_profile: Optional[str]
) -> TargetConnectionFactory:
    """Build the default IAM-authenticated DSQL connection factory for the probe."""

    def factory() -> object:
        return DsqlConnector(target, aws_profile=aws_profile).connect()

    return factory


def build_query_playground_screen(
    store: SessionStore,
    session_id: str,
    *,
    playground_store: PlaygroundStore,
    converter: Optional[QueryConverter] = None,
    target_connection_factory: Optional[
        Callable[[TargetConnectionConfig, Optional[str]], TargetConnectionFactory]
    ] = None,
) -> Callable[[Callable[[], None]], None]:
    """Build the Query Playground screen, returning its ``content_builder``.

    ``content_builder(refresh)`` renders the screen: a SQL input, a Convert
    action that shows the original/converted SQL and warnings, and a "Test on
    target" action that non-destructively probes the converted statement against
    the verified target DSQL (``EXPLAIN`` for SELECT, rolled-back dry run for DDL;
    DML is never executed). When AI assist is enabled (Connect screen), an "Ask
    AI to review / fix" action opens the SAME shared right chat drawer used by the
    Evaluation and Schema Conversion screens, grounded on the conversion and any
    target error; the chat is advisory and nothing is auto-applied (Property 13).
    The converter and target connection factory are injectable so the screen can
    be exercised without sqlglot edge cases or a real cluster in tests.
    """
    from nicegui import run, ui

    session = store.get_or_create(session_id)
    state = playground_store.get_or_create(session_id)
    query_converter = converter or QueryConverter()
    make_factory = target_connection_factory or _default_target_connection_factory

    def content(refresh: Callable[[], None]) -> None:
        # ``ui.code`` renders SQL inside a <pre> whose default ``white-space: pre``
        # (no wrap) lives on the inner element, so Tailwind classes on the wrapper
        # never reach it and a long converted line is clipped into a tiny box.
        # Force the SQL-card code blocks (and their inner <pre>/<code>) to wrap.
        ui.add_css(
            ".qp-sql, .qp-sql pre, .qp-sql code {"
            " white-space: pre-wrap !important;"
            " overflow-wrap: anywhere !important;"
            " word-break: break-word !important; }"
            # The query plan, by contrast, must NOT wrap — wrapped tree lines are
            # unreadable. Keep each plan line intact and scroll horizontally.
            " .qp-plan { overflow-x: auto !important; }"
            " .qp-plan pre, .qp-plan code {"
            " white-space: pre !important;"
            " overflow-wrap: normal !important;"
            " word-break: normal !important; }"
        )
        with ui.column().classes("w-full gap-3"):
            section_header(
                ui, icon="science", title="Query validation (playground)"
            )
            intro = (
                "Paste a MySQL statement to see how it converts to Aurora DSQL "
                "(PostgreSQL) and, for SELECT/DDL, test whether it runs on the "
                "target."
            )
            if session.ai_assist.enabled:
                intro += (
                    " AI Assist is on: open the AI chat to review the conversion, "
                    "ask follow-ups, or have it fix a statement the target rejected."
                )
            ui.label(intro).classes("text-sm text-gray-500")

            # Prominent, always-visible safety guarantee for customers worried that
            # this could change their data. Kept at the very top, before the input,
            # so the reassurance is the first thing seen (Usability / trust first).
            _render_safety_banner(ui)

            # Build the shared right chat drawer once (same component/look as the
            # Evaluation and Schema Conversion screens); None when AI is off.
            open_chat = build_chat_drawer(ui) if session.ai_assist.enabled else None

            sql_input = ui.textarea(
                label="MySQL SQL",
                placeholder=_PLACEHOLDER_SQL,
                value=state.sql,
                on_change=lambda e: setattr(state, "sql", e.value or ""),
            ).props(
                # A tall starting height so a multi-line query is fully visible;
                # autogrow still expands it further for longer statements.
                'outlined autogrow input-class=font-mono '
                'input-style="min-height: 14rem; font-size: 0.875rem"'
            ).classes("w-full")

            # Results render into their own refreshable region so Convert / Test /
            # Ask AI update just this area (no full-page rebuild / scroll reset),
            # giving a smooth flow from the input down to the results.
            # Set by Convert so the next results render scrolls itself into view;
            # cleared after one render so Test / Ask AI refreshes don't yank the
            # viewport back to the top.
            scroll_after_convert = {"pending": False}

            @ui.refreshable
            def render_results() -> None:
                result = state.result
                if result is None:
                    return
                with ui.column().classes("w-full gap-3") as results_box:
                    _render_conversion(ui, result)
                    _render_test_panel(result, on_test, render_results.refresh)
                    _render_ai_panel(result, on_ask_ai, on_optimize_ai)
                if scroll_after_convert["pending"]:
                    scroll_after_convert["pending"] = False
                    # Smoothly reveal the just-converted results beneath the editor,
                    # so pressing Convert flows naturally into the answer.
                    ui.run_javascript(
                        f"document.getElementById('c{results_box.id}')"
                        f"?.scrollIntoView({{behavior:'smooth',block:'start'}});"
                    )

            def on_convert() -> None:
                # Read the editor's LIVE value (not the possibly-stale state.sql,
                # whose on_change may not have synced before this click), then trim
                # leading/trailing whitespace and blank lines so a query pasted with
                # blank lines above/below it still converts.
                raw = getattr(sql_input, "value", None)
                if not isinstance(raw, str):
                    raw = state.sql or ""
                sql = raw.strip()
                if not sql:
                    ui.notify("Enter a MySQL statement to convert.", type="warning")
                    return
                # Reflect the trimmed query back into the editor + state so the
                # input matches exactly what was converted/tested.
                state.sql = sql
                try:
                    sql_input.set_value(sql)
                except Exception:  # noqa: BLE001 - best-effort UI sync
                    pass
                # Pretty-print the converted SQL (multi-line, indented) so a long
                # statement is readable; formatting only, never semantics.
                state.set_result(query_converter.convert(sql, pretty=True))
                scroll_after_convert["pending"] = True
                render_results.refresh()

            async def on_test() -> None:
                result = state.result
                if result is None or not is_testable(result):
                    return
                if not session.has_target():
                    ui.notify(
                        "Configure and verify the target connection first "
                        "(Connect screen).",
                        type="warning",
                    )
                    return
                target_config = session.target_config
                assert target_config is not None  # guaranteed by has_target()
                factory = make_factory(
                    target_config, getattr(session, "aws_profile", None)
                )
                converted = result.converted_sql
                kind = result.statement_kind
                analyze = state.analyze
                # Point the probe's search_path at the migrated schema (a MySQL DB
                # maps to a PG schema), so an UNQUALIFIED table reference in the
                # user's query (SELECT ... FROM orders) resolves to the migrated
                # table instead of failing with relation-does-not-exist under the
                # default public search_path -- mirroring how the query ran against
                # that MySQL database.
                source_config = getattr(session, "source_config", None)
                search_path = getattr(source_config, "database", None) or None
                state.begin_probe()
                render_results.refresh()
                # The probe is a network round-trip; run it off the event loop.
                probe = await run.io_bound(
                    lambda: probe_statement(
                        converted, kind, factory, analyze=analyze,
                        search_path=search_path,
                    )
                )
                state.set_probe(probe)
                render_results.refresh()

            def on_ask_ai() -> None:
                # Open the SAME shared right chat drawer the Evaluation / Schema
                # Conversion screens use, grounded on this query's conversion and
                # any target error captured by the probe (so the AI can fix the
                # real failure). Advisory only -- nothing is auto-applied.
                result = state.result
                if result is None or open_chat is None:
                    return
                from dsql_migrator.core.assessment_strategist import (
                    AssessmentStrategist,
                )

                probe = state.probe
                target_error = (
                    f"{probe.detail}"
                    + (f" (SQLSTATE {probe.error_code})" if probe.error_code else "")
                    if probe is not None and probe.outcome is ProbeOutcome.FAILED
                    else None
                )
                system = build_query_chat_system(
                    result.original_sql,
                    result.converted_sql or "",
                    target_error=target_error,
                )
                strategist = AssessmentStrategist(
                    session.ai_assist, aws_profile=getattr(session, "aws_profile", None)
                )
                first_question = (
                    "This converted query was rejected by Aurora DSQL. Why, and how "
                    "do I fix it so it runs?"
                    if target_error is not None
                    else "Review this MySQL → Aurora DSQL conversion: is it correct, "
                    "will it run on DSQL, and how would you improve it?"
                )
                open_chat(
                    title="AI query assistant",
                    subtitle="Query validation (playground)",
                    first_question=first_question,
                    streamer=lambda messages, on_delta: strategist.stream_chat(
                        system, messages, on_delta
                    ),
                )

            def on_optimize_ai() -> None:
                # Open the SAME right chat drawer, but grounded on making THIS
                # query efficient on Aurora DSQL: the model gets the converted SQL,
                # the DSQL efficiency rubric, and (when a Test captured them) the
                # real EXPLAIN plan + DPU. Under each reply a "Test rewrite on
                # target" action re-runs the probe on the model's proposed SQL and
                # feeds the before/after DPU back AS A NEW AI TURN, so the same
                # assistant reports how much it actually improved. Advisory only —
                # nothing is auto-applied; the user copies SQL back to run for real.
                result = state.result
                if result is None or open_chat is None:
                    return
                from dsql_migrator.core.assessment_strategist import (
                    AssessmentStrategist,
                )

                probe = state.probe
                baseline_dpu = (
                    probe.dpu.total
                    if probe is not None and probe.dpu is not None
                    else None
                )
                system = build_query_optimize_system(
                    result.original_sql,
                    result.converted_sql or "",
                    plan=probe.plan if probe is not None else None,
                    dpu_total=baseline_dpu,
                    analyzed=probe.analyzed if probe is not None else False,
                )
                strategist = AssessmentStrategist(
                    session.ai_assist, aws_profile=getattr(session, "aws_profile", None)
                )
                # send_turn (returned by open_chat) lets us drive a follow-up turn
                # so the re-test result is delivered BY THE AI, not as a raw panel.
                sender: dict[str, object] = {"send": None}

                async def _retest_rewrite(reply_markdown: str) -> None:
                    # Pull the model's proposed SQL out of the reply, re-run the
                    # read-only probe (EXPLAIN ANALYZE for a DPU number), then ask
                    # the SAME assistant to compare it to the baseline.
                    send = sender["send"]
                    if send is None:
                        return
                    rewrite = extract_sql_from_reply(reply_markdown)
                    if not rewrite:
                        ui.notify(
                            "No runnable SELECT found in the AI reply to test "
                            "(e.g. it only suggested an index or DDL). Ask the AI "
                            "for a rewritten SELECT in a ```sql block.",
                            type="warning",
                        )
                        return
                    if not session.has_target():
                        ui.notify(
                            "Verify the target connection first (Connect screen).",
                            type="warning",
                        )
                        return
                    target_config = session.target_config
                    assert target_config is not None
                    factory = make_factory(
                        target_config, getattr(session, "aws_profile", None)
                    )
                    ui.notify("Re-testing the rewrite on the target…", type="info")
                    # EXPLAIN ANALYZE so DSQL emits the DPU estimate to compare. The
                    # rewrite is treated as a SELECT probe (read-only / EXPLAIN).
                    rprobe = await run.io_bound(
                        lambda: probe_statement(
                            rewrite, StatementKind.SELECT, factory, analyze=True
                        )
                    )
                    send(  # type: ignore[operator]
                        _build_retest_turn(rprobe, baseline_dpu, rewrite_sql=rewrite)
                    )

                sender["send"] = open_chat(
                    title="AI DBA — query tuning",
                    subtitle="Query validation (playground) · Aurora DSQL efficiency",
                    first_question=(
                        "Rewrite this converted query to run more efficiently on "
                        "Aurora DSQL. Explain in detail what you changed and why it "
                        "is cheaper on DSQL (scan type / filter layer / fewer bytes "
                        "and DPU) — and keep the results identical."
                    ),
                    streamer=lambda messages, on_delta: strategist.stream_chat(
                        system, messages, on_delta
                    ),
                    footer_label="Test rewrite on target",
                    footer_action=_retest_rewrite,
                    # Only offer the re-test when the reply actually contains a
                    # runnable rewritten query — a reply that just concludes "this
                    # is already efficient" (no ```sql SELECT) gets no button.
                    footer_visible=lambda md: extract_sql_from_reply(md) is not None,
                )

            with ui.row().classes("items-center gap-2"):
                ui.button("Convert", icon="sync_alt", on_click=on_convert).props(
                    "color=primary"
                )

            render_results()

    def _render_test_panel(
        result: QueryConversionResult,
        on_test: Callable[[], object],
        refresh: Callable[[], None],
    ) -> None:
        """Render the 'Test on target' action + the probe verdict / progress."""
        testable = is_testable(result)
        has_target = session.has_target()
        is_select = result.statement_kind is StatementKind.SELECT
        with ui.row().classes("items-center gap-2 flex-wrap"):
            test_button = ui.button(
                "Test on target", icon="play_arrow", on_click=on_test
            ).props("color=primary outline")
            if not testable or not has_target or state.probing:
                test_button.disable()
            if not testable:
                test_button.tooltip(
                    "Only a converted SELECT (EXPLAIN) or DDL (dry run) can be "
                    "tested; DML is never executed."
                )
            elif not has_target:
                test_button.tooltip(
                    "Verify the target connection on the Connect screen first."
                )
            # ANALYZE only applies to a SELECT (it executes the query for real
            # stats); for DDL the dry-run already runs+rolls back, so hide it.
            if is_select:
                ui.switch(
                    "Run EXPLAIN ANALYZE (+ DPU cost)",
                    value=state.analyze,
                    on_change=lambda e: setattr(state, "analyze", bool(e.value)),
                ).props("dense").tooltip(
                    "EXPLAIN ANALYZE VERBOSE actually executes the (read-only) query "
                    "on the target to capture real timings, row counts, and Aurora "
                    "DSQL's per-statement DPU cost estimate. Off = plan-only EXPLAIN "
                    "(the query is not executed, no cost estimate)."
                )

        if state.probing:
            with ui.row().classes("items-center gap-2"):
                ui.spinner(size="sm")
                ui.label("Testing on the target...").classes(
                    "text-sm text-gray-500"
                )
            # Poll for the verdict, but only re-render ONCE the probe has finished.
            # ``refresh`` rebuilds the whole results region -- including the "Test on
            # target" button above and its tooltip -- and a q-tooltip is a CHILD of its
            # anchor, so an unconditional re-render every 0.4s destroyed the element the
            # pointer was over: the tooltip closed and only reopened on a fresh hover,
            # i.e. it flickered and could not be read. Nothing in this branch changes
            # between ticks (a spinner plus fixed text), so waiting for the state change
            # costs nothing and leaves the hovered tooltip alone.
            def _await_probe() -> None:
                if state.probing:
                    ui.timer(_POLL_INTERVAL_SECONDS, _await_probe, once=True)
                    return
                refresh()  # finished: now the verdict needs to be drawn

            ui.timer(_POLL_INTERVAL_SECONDS, _await_probe, once=True)
            return

        probe = state.probe
        if probe is not None:
            _render_probe(ui, probe)

    def _render_ai_panel(
        result: QueryConversionResult,
        on_ask_ai: Callable[[], object],
        on_optimize_ai: Callable[[], object],
    ) -> None:
        """Render the AI-assist entry points: buttons that open the chat drawer.

        Shown only when AI assist is enabled for the session (Connect screen);
        otherwise a one-line hint nudges toward enabling it on a hard case. The
        chat itself happens in the SAME shared right drawer the Evaluation and
        Schema Conversion screens use -- advisory only, nothing auto-applied
        (Property 13).

        Two actions: "Ask AI to review / fix" (correctness), and — for a converted
        SELECT — "Tune with AI DBA", which opens the AI-DBA tuning chat grounded on
        the real EXPLAIN plan / DPU (when a Test has run) and offers an in-drawer
        "Test rewrite on target" action that re-probes the AI's SQL and asks the AI
        to report the before/after DPU improvement.
        """
        if not session.ai_assist.enabled:
            _render_ai_disabled_hint(ui, result)
            return

        ui.separator()
        # A failed target test makes "fix" the headline; otherwise "review".
        probe = state.probe
        failed = probe is not None and probe.outcome is ProbeOutcome.FAILED
        ask_label = "Ask AI to fix" if failed else "Ask AI to review / explain"
        is_select = result.statement_kind is StatementKind.SELECT
        # The AI DBA tunes for efficiency, so it only makes sense once the query has
        # actually run on the target (a PASSED probe): the AI reasons from the real
        # EXPLAIN plan, and the re-test loop compares against a real baseline. Gate
        # the button on a successful Test on target for a converted SELECT.
        tested_ok = (
            is_select
            and result.converted_sql is not None
            and probe is not None
            and probe.outcome is ProbeOutcome.PASSED
        )
        # A baseline DPU (only from an EXPLAIN ANALYZE test) lets the button show
        # the current cost and enables the before/after comparison after a re-test.
        baseline_dpu = (
            probe.dpu.total
            if probe is not None and probe.dpu is not None
            else None
        )
        with ui.row().classes("items-center gap-2 flex-wrap"):
            ui.button(
                ask_label, icon="auto_awesome", on_click=on_ask_ai
            ).props("color=primary outline")
            if tested_ok:
                tune_label = "Tune with AI DBA"
                if baseline_dpu is not None:
                    tune_label += f"  (now ≈ {_fmt_dpu(baseline_dpu)} DPU)"
                ui.button(
                    tune_label, icon="tune", on_click=on_optimize_ai
                ).props("color=primary outline")
        # Footer hint adapts: nudge toward Testing first (so the tuner appears),
        # and toward ANALYZE (so a DPU baseline exists for the before/after proof).
        if tested_ok and baseline_dpu is None:
            hint = (
                "Tip: re-run Test with the EXPLAIN ANALYZE toggle on to capture a "
                "DPU baseline — then the AI DBA can prove how much its rewrite saves."
            )
        elif not tested_ok and is_select and result.converted_sql is not None:
            hint = (
                "Run Test on target first (ideally with EXPLAIN ANALYZE) to unlock "
                "\"Tune with AI DBA\" — it tunes from the real query plan and cost."
            )
        else:
            hint = (
                "Opens the AI chat (same as Evaluation / Schema Conversion); "
                "advisory only, nothing is auto-applied."
            )
        ui.label(hint).classes("text-xs text-gray-500")

    content.__name__ = "query_playground_content"
    return content


def _render_ai_disabled_hint(ui: object, result: QueryConversionResult) -> None:
    """When AI is off, nudge toward enabling it -- more pointedly on a hard case."""
    needs_help = result.classification is not Classification.AUTO
    if not needs_help:
        return
    render_notice(
        ui,
        tone="info",
        header="AI Assist can help here",
        body=(
            "This statement needs manual review. Enable AI Assist on the Connect "
            "screen to chat with the AI about a DSQL rewrite and an explanation."
        ),
    )


# The three safety guarantees, shown prominently so a worried customer sees them
# before doing anything. (icon, bold lead, explanation).
_SAFETY_POINTS: tuple[tuple[str, str, str], ...] = (
    (
        "block",
        "INSERT / UPDATE / DELETE are never executed",
        "Data-changing statements are converted and analyzed only — they are NOT "
        "run against the target. Your data cannot be modified here.",
    ),
    (
        "lock",
        "The source database is never touched",
        "The playground only reads the SQL you paste; it opens no connection to "
        "the source.",
    ),
    (
        "fact_check",
        "Tests on the target are non-destructive",
        "SELECT is checked with read-only EXPLAIN; DDL runs inside a transaction "
        "that is always rolled back, so nothing is persisted.",
    ),
)


def _render_safety_banner(ui: object) -> None:
    """Render the always-visible "your data is safe" guarantee panel.

    A prominent green, bordered Cloudscape-style panel with a shield header and
    three explicit guarantees -- the strongest being that INSERT/UPDATE/DELETE are
    never executed. Pinned above the input so a customer worried about data
    safety is reassured before they paste anything.
    """
    with ui.card().classes(  # type: ignore[attr-defined]
        "w-full !shadow-none border border-green-300 rounded-lg p-0 overflow-hidden "
        "bg-green-50"
    ):
        with ui.row().classes(  # type: ignore[attr-defined]
            "items-center gap-2 no-wrap w-full px-3 py-2 border-b border-green-200 "
            "bg-green-100"
        ):
            ui.icon("verified_user", color="green-7").classes("text-xl")  # type: ignore[attr-defined]
            ui.label("Safe by design — your data is never modified").classes(  # type: ignore[attr-defined]
                "text-sm font-semibold text-green-900"
            )
        with ui.column().classes("w-full gap-2 px-3 py-2"):  # type: ignore[attr-defined]
            for icon_name, lead, explanation in _SAFETY_POINTS:
                with ui.row().classes("items-start gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
                    ui.icon(icon_name, color="green-7").classes(  # type: ignore[attr-defined]
                        "text-base mt-0.5"
                    )
                    with ui.column().classes("gap-0 min-w-0 flex-1"):  # type: ignore[attr-defined]
                        ui.label(lead).classes(  # type: ignore[attr-defined]
                            "text-sm font-semibold text-green-900"
                        )
                        ui.label(explanation).classes(  # type: ignore[attr-defined]
                            "text-xs text-green-800"
                        )


def _render_conversion(ui: object, result: QueryConversionResult) -> None:
    """Render the conversion verdict, kind banner, side-by-side SQL, and warnings."""
    tone, label = classification_tone(result.classification)
    badge_tone, kind_label, kind_help = kind_meta(result.statement_kind)

    # Statement kind + what testing it does.
    with ui.row().classes("items-center gap-2 flex-wrap"):  # type: ignore[attr-defined]
        ui.label("Statement type:").classes("text-sm text-gray-500")  # type: ignore[attr-defined]
        ui.label(kind_label).classes(  # type: ignore[attr-defined]
            "text-xs leading-tight border rounded px-2 py-0.5 "
            + badge_classes(badge_tone)
        )
        ui.label(kind_help).classes("text-xs text-gray-500")  # type: ignore[attr-defined]

    # For a data-changing statement, reinforce the guarantee as a full notice
    # (not just the grey kind-help line) so a worried customer sees it clearly.
    if result.statement_kind is StatementKind.DML:
        render_notice(
            ui,
            tone="success",
            icon="block",
            header="This will NOT run on the target",
            body=(
                "INSERT / UPDATE / DELETE is converted and analyzed only. The "
                "playground never executes data-changing statements against the "
                "target, so your data cannot be modified here."
            ),
        )

    # Side-by-side original (MySQL) vs converted (DSQL).
    with ui.row().classes("w-full gap-3 no-wrap items-stretch"):  # type: ignore[attr-defined]
        _render_sql_card(ui, "Original (MySQL)", result.original_sql)
        _render_sql_card(
            ui,
            "Converted (Aurora DSQL)",
            result.converted_sql or "-- could not be converted; see warnings below",
        )

    # Conversion verdict notice.
    if result.classification is Classification.AUTO:
        render_notice(
            ui,
            tone="success",
            header="Converted cleanly",
            body="No conversion issues were found for this statement.",
        )
    else:
        render_notice(
            ui,
            tone=tone,
            header=label,
            body="Review the conversion notes below before using this statement.",
        )

    # Per-warning notices.
    for warning in result.warnings:
        w_tone, _ = classification_tone(warning.classification)
        render_notice(
            ui,
            tone=w_tone,
            header=warning.code.replace("_", " ").title(),
            body=warning.message,
        )


def _render_sql_card(ui: object, title: str, sql: str) -> None:
    """Render one titled, monospaced SQL card (half of the side-by-side pair)."""
    with ui.card().classes(  # type: ignore[attr-defined]
        "flex-1 min-w-0 !shadow-none border border-gray-200 rounded-lg p-0 "
        "overflow-hidden self-stretch"
    ):
        with ui.row().classes(  # type: ignore[attr-defined]
            "items-center gap-2 w-full px-3 py-2 border-b border-gray-200 bg-gray-50"
        ):
            ui.label(title).classes("text-xs font-semibold text-gray-700")  # type: ignore[attr-defined]
        ui.code(sql, language="sql").classes(  # type: ignore[attr-defined]
            "qp-sql w-full text-xs"
        )


def _render_probe(ui: object, probe: ExecutionProbe) -> None:
    """Render the target test-run verdict + (for SELECT) the captured query plan."""
    tone = probe_outcome_tone(probe.outcome)
    header = {
        ProbeOutcome.PASSED: "Runs on Aurora DSQL",
        ProbeOutcome.FAILED: "Aurora DSQL rejected the statement",
        ProbeOutcome.SKIPPED: "Not test-run",
    }.get(probe.outcome, "Test result")
    body = probe.detail
    if probe.error_code:
        body = f"{body} (SQLSTATE {probe.error_code})"
    render_notice(ui, tone=tone, header=header, body=body)

    # Aurora DSQL's per-statement DPU cost estimate (only from ANALYZE VERBOSE).
    if probe.dpu is not None:
        _render_dpu_card(ui, probe.dpu)

    # Show the captured EXPLAIN / EXPLAIN ANALYZE query plan, when present.
    if probe.plan:
        _render_plan_card(ui, probe)


def _fmt_dpu(value: float) -> str:
    """Format a DPU value with enough precision for small per-statement costs."""
    return f"{value:.5f}"


def _fmt_usd(value: float) -> str:
    """Format a small USD cost without losing tiny per-statement amounts."""
    if value <= 0:
        return "$0.00"
    if value < 0.01:
        # Per-statement costs are tiny; show enough significant digits to be useful.
        return f"${value:.8f}".rstrip("0").rstrip(".")
    return f"${value:,.4f}"


def _render_dpu_card(ui: object, dpu: "object") -> None:
    """Render Aurora DSQL's per-statement DPU cost estimate (simple, one line).

    A light bordered strip: the Total DPU is the headline (with the advisory USD
    cost beside it), the Compute / Read / Write split is a quiet secondary line,
    and a one-line footnote notes the cost is approximate. The DPU numbers are
    DSQL's own estimate (EXPLAIN ANALYZE VERBOSE); only the USD figure is
    tool-derived (Total × per-DPU price).
    """
    headline = f"{_fmt_dpu(dpu.total)} DPU"  # type: ignore[attr-defined]
    if dpu.estimated_cost_usd is not None:  # type: ignore[attr-defined]
        headline += f"  ·  ≈ {_fmt_usd(dpu.estimated_cost_usd)}"  # type: ignore[attr-defined]
    with ui.row().classes(  # type: ignore[attr-defined]
        "items-center gap-2 no-wrap w-full rounded-md border border-gray-200 "
        "bg-gray-50 px-3 py-2"
    ):
        ui.icon("savings", color="grey-6").classes("text-base shrink-0")  # type: ignore[attr-defined]
        with ui.column().classes("gap-0 min-w-0 flex-1"):  # type: ignore[attr-defined]
            with ui.row().classes("items-baseline gap-2 no-wrap"):  # type: ignore[attr-defined]
                ui.label("Estimated cost").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-500 shrink-0"
                )
                ui.label(headline).classes(  # type: ignore[attr-defined]
                    "text-sm font-semibold text-gray-800 font-mono"
                )
            ui.label(  # type: ignore[attr-defined]
                f"Compute {_fmt_dpu(dpu.compute)} · Read {_fmt_dpu(dpu.read)} · "  # type: ignore[attr-defined]
                f"Write {_fmt_dpu(dpu.write)} DPU — DSQL's own estimate; "  # type: ignore[attr-defined]
                "cost approximate (varies by Region)."
            ).classes("text-[11px] text-gray-400")


def _render_plan_card(ui: object, probe: ExecutionProbe) -> None:
    """Render the captured query plan in a titled, monospaced card."""
    title = (
        "Query plan (EXPLAIN ANALYZE — actually executed)"
        if probe.analyzed
        else "Query plan (EXPLAIN — planned, not executed)"
    )
    with ui.card().classes(  # type: ignore[attr-defined]
        "w-full min-w-0 !shadow-none border border-gray-200 rounded-lg p-0 "
        "overflow-hidden"
    ):
        with ui.row().classes(  # type: ignore[attr-defined]
            "items-center gap-2 w-full px-3 py-2 border-b border-gray-200 bg-gray-50"
        ):
            ui.icon("account_tree", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label(title).classes("text-xs font-semibold text-gray-700")  # type: ignore[attr-defined]
        ui.code(probe.plan, language="text").classes(  # type: ignore[attr-defined]
            "qp-plan w-full text-xs"
        )


__all__ = [
    "PlaygroundState",
    "PlaygroundStore",
    "classification_tone",
    "kind_meta",
    "is_testable",
    "probe_outcome_tone",
    "build_query_playground_screen",
]
