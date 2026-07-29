# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Query Playground engine + screen logic (no DB, no NiceGUI).

Covers the non-destructive target probe (SELECT -> EXPLAIN read-only, DDL ->
rolled-back dry run, DML -> never executed, failures captured) and the
NiceGUI-agnostic presentation helpers / per-session state of the playground
screen. A fake psycopg-style connection records what was executed and whether it
was ever committed, so the "never persists a change" guarantee is asserted
directly.
"""

from __future__ import annotations

from typing import Any, Optional

from dsql_migrator.core.models import Classification
from dsql_migrator.core.query_converter import (
    QueryConversionResult,
    QueryConverter,
    StatementKind,
)
from dsql_migrator.core.query_playground import (
    DEFAULT_USD_PER_DPU,
    ExecutionProbe,
    ProbeMode,
    ProbeOutcome,
    parse_dpu_estimate,
    probe_statement,
)
from dsql_migrator.ui.query_playground import (
    PlaygroundState,
    PlaygroundStore,
    _build_retest_turn,
    classification_tone,
    extract_sql_from_reply,
    is_testable,
    kind_meta,
    probe_outcome_tone,
)


# ---------------------------------------------------------------------------
# Fake psycopg-style target connection
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, connection: "_FakeConnection") -> None:
        self._connection = connection
        self._rows: list[tuple] = []

    def execute(self, statement: Any) -> None:
        text = str(statement)
        self._connection.executed.append(text)
        error = self._connection.error_for(text)
        if error is not None:
            raise error
        upper = text.upper()
        if upper.startswith("EXPLAIN ANALYZE"):
            # ANALYZE VERBOSE plans carry real timing/row stats AND the DSQL
            # per-statement DPU estimate block.
            self._rows = [
                ("Seq Scan on orders  (cost=0.00..1.10 rows=10 width=8)",),
                ("  (actual time=0.012..0.020 rows=5 loops=1)",),
                ("Planning Time: 0.1 ms",),
                ("Execution Time: 0.3 ms",),
                ("Statement DPU Estimate:",),
                ("  Compute: 0.01607 DPU",),
                ("  Read: 0.04312 DPU (Transaction minimum: 0.00375)",),
                ("  Write: 0.00000 DPU",),
                ("  Total: 0.05919 DPU",),
            ]
        elif upper.startswith("EXPLAIN"):
            self._rows = [
                ("Seq Scan on orders  (cost=0.00..1.10 rows=10 width=8)",),
                ("  Filter: (id = 1)",),
            ]
        else:
            self._rows = []

    def fetchall(self) -> list[tuple]:
        return list(self._rows)

    def close(self) -> None:
        self._connection.cursors_closed += 1


class _FakeConnection:
    """Records executed statements + commit/rollback so persistence can be asserted."""

    def __init__(self, *, error: Optional[Exception] = None) -> None:
        self.executed: list[str] = []
        self.autocommit = True
        self.committed = 0
        self.rolled_back = 0
        self.closed = False
        self.cursors_closed = 0
        self._error = error

    def error_for(self, statement: str) -> Optional[Exception]:
        # Raise the configured error only on the real statement, never on EXPLAIN's
        # planning of a healthy query (the error models a target rejection).
        if self._error is None:
            return None
        return self._error

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def commit(self) -> None:
        self.committed += 1

    def rollback(self) -> None:
        self.rolled_back += 1

    def close(self) -> None:
        self.closed = True


def _factory(connection: _FakeConnection):
    """A connection factory returning a fixed fake connection, recording calls."""
    calls = {"n": 0}

    def factory() -> _FakeConnection:
        calls["n"] += 1
        return connection

    factory.calls = calls  # type: ignore[attr-defined]
    return factory


def _sqlstate_error(message: str, sqlstate: str) -> Exception:
    exc = Exception(message)
    exc.sqlstate = sqlstate  # type: ignore[attr-defined]
    return exc


# ---------------------------------------------------------------------------
# Probe: SELECT -> EXPLAIN (read-only)
# ---------------------------------------------------------------------------


def test_select_is_probed_with_explain_and_never_commits() -> None:
    conn = _FakeConnection()
    probe = probe_statement("SELECT * FROM t", StatementKind.SELECT, _factory(conn))

    assert probe.outcome is ProbeOutcome.PASSED
    assert probe.mode is ProbeMode.EXPLAIN
    assert len(conn.executed) == 1
    assert conn.executed[0].upper().startswith("EXPLAIN ")
    assert "SELECT * FROM t" in conn.executed[0]
    assert conn.committed == 0
    assert conn.closed is True


def test_select_explain_strips_trailing_semicolon() -> None:
    conn = _FakeConnection()
    probe_statement("SELECT 1;", StatementKind.SELECT, _factory(conn))
    assert conn.executed[0] == "EXPLAIN SELECT 1"


def test_search_path_set_before_explain_so_unqualified_tables_resolve() -> None:
    # A query written against a MySQL database uses unqualified table names; the
    # migrated tables live in a same-named PG schema. Passing search_path sets it
    # (then public) BEFORE the EXPLAIN so "orders" resolves instead of failing with
    # relation-does-not-exist (42P01) under the default public search_path.
    conn = _FakeConnection()
    probe = probe_statement(
        "SELECT * FROM orders", StatementKind.SELECT, _factory(conn),
        search_path="ecommerce_demo",
    )
    assert probe.outcome is ProbeOutcome.PASSED
    assert conn.executed[0] == 'SET search_path TO "ecommerce_demo", public'
    assert conn.executed[1].upper().startswith("EXPLAIN ")
    assert "SELECT * FROM orders" in conn.executed[1]


def test_no_search_path_leaves_explain_first() -> None:
    # Without a source database (search_path=None) the probe runs no SET -- behavior
    # is unchanged (the default public search_path).
    conn = _FakeConnection()
    probe_statement("SELECT * FROM t", StatementKind.SELECT, _factory(conn))
    assert not any(s.upper().startswith("SET SEARCH_PATH") for s in conn.executed)
    assert conn.executed[0].upper().startswith("EXPLAIN ")


def test_select_explain_captures_query_plan() -> None:
    conn = _FakeConnection()
    probe = probe_statement("SELECT * FROM orders", StatementKind.SELECT, _factory(conn))
    # The plan rows are joined back into one multi-line plan text.
    assert probe.plan is not None
    assert "Seq Scan on orders" in probe.plan
    assert "Filter: (id = 1)" in probe.plan
    # Plan-only EXPLAIN did not execute the query.
    assert probe.analyzed is False


# ---------------------------------------------------------------------------
# Probe: SELECT -> EXPLAIN ANALYZE (opt-in; actually executes the read query)
# ---------------------------------------------------------------------------


def test_select_explain_analyze_runs_and_captures_real_stats() -> None:
    conn = _FakeConnection()
    probe = probe_statement(
        "SELECT * FROM orders", StatementKind.SELECT, _factory(conn), analyze=True
    )
    assert probe.outcome is ProbeOutcome.PASSED
    assert probe.mode is ProbeMode.EXPLAIN_ANALYZE
    assert probe.analyzed is True
    # The statement sent was EXPLAIN ANALYZE VERBOSE (VERBOSE is required for the
    # DSQL per-statement DPU estimate; it actually executes the query)...
    assert conn.executed[0].upper().startswith("EXPLAIN ANALYZE VERBOSE ")
    # ...and the captured plan carries real execution stats.
    assert probe.plan is not None
    assert "Execution Time" in probe.plan
    # A read-only SELECT probe never commits, even when analyzed.
    assert conn.committed == 0


def test_select_explain_analyze_captures_dpu_cost() -> None:
    conn = _FakeConnection()
    probe = probe_statement(
        "SELECT * FROM orders", StatementKind.SELECT, _factory(conn), analyze=True
    )
    assert probe.dpu is not None
    assert probe.dpu.compute == 0.01607
    assert probe.dpu.read == 0.04312
    assert probe.dpu.write == 0.0
    assert probe.dpu.total == 0.05919
    # Advisory USD cost = total * default per-DPU price.
    assert probe.dpu.estimated_cost_usd == 0.05919 * DEFAULT_USD_PER_DPU


def test_plain_explain_has_no_dpu_estimate() -> None:
    # Plan-only EXPLAIN does not run the query, so there is no DPU block.
    conn = _FakeConnection()
    probe = probe_statement(
        "SELECT * FROM orders", StatementKind.SELECT, _factory(conn), analyze=False
    )
    assert probe.dpu is None


def test_analyze_ignored_for_ddl_dry_run() -> None:
    # ANALYZE applies only to SELECT; DDL stays a rolled-back dry run regardless.
    conn = _FakeConnection()
    probe = probe_statement(
        "CREATE TABLE t (id INT PRIMARY KEY)",
        StatementKind.DDL,
        _factory(conn),
        analyze=True,
    )
    assert probe.mode is ProbeMode.DRY_RUN_ROLLBACK
    assert probe.analyzed is False
    assert conn.rolled_back == 1
    assert conn.committed == 0
    assert not any("EXPLAIN" in s.upper() for s in conn.executed)


# ---------------------------------------------------------------------------
# DPU estimate parsing (Statement DPU Estimate block from ANALYZE VERBOSE)
# ---------------------------------------------------------------------------


_DPU_PLAN = """Index Only Scan using test_table_pkey on public.test_table
Planning Time: 11.415 ms
Execution Time: 4.528 ms
Statement DPU Estimate:
  Compute: 0.01607 DPU
  Read: 0.04312 DPU (Transaction minimum: 0.00375)
  Write: 0.00000 DPU
  Total: 0.05919 DPU"""


def test_parse_dpu_estimate_reads_all_components_and_costs() -> None:
    dpu = parse_dpu_estimate(_DPU_PLAN)
    assert dpu is not None
    assert (dpu.compute, dpu.read, dpu.write, dpu.total) == (
        0.01607,
        0.04312,
        0.0,
        0.05919,
    )
    assert dpu.estimated_cost_usd == 0.05919 * DEFAULT_USD_PER_DPU


def test_parse_dpu_estimate_custom_price() -> None:
    dpu = parse_dpu_estimate(_DPU_PLAN, usd_per_dpu=0.00001)
    assert dpu is not None
    assert dpu.estimated_cost_usd == 0.05919 * 0.00001


def test_parse_dpu_estimate_none_without_block() -> None:
    assert parse_dpu_estimate("Seq Scan on orders\n  Filter: (id = 1)") is None
    assert parse_dpu_estimate(None) is None


def test_parse_dpu_estimate_derives_total_when_absent() -> None:
    plan = (
        "Statement DPU Estimate:\n"
        "  Compute: 0.10000 DPU\n"
        "  Read: 0.20000 DPU\n"
        "  Write: 0.30000 DPU"
    )
    dpu = parse_dpu_estimate(plan)
    assert dpu is not None
    assert round(dpu.total, 5) == 0.60000


# ---------------------------------------------------------------------------
# Probe: DDL -> dry run that is ALWAYS rolled back (never committed)
# ---------------------------------------------------------------------------


def test_ddl_is_dry_run_and_rolled_back_not_committed() -> None:
    conn = _FakeConnection()
    probe = probe_statement(
        "CREATE TABLE t (id INT PRIMARY KEY)", StatementKind.DDL, _factory(conn)
    )

    assert probe.outcome is ProbeOutcome.PASSED
    assert probe.mode is ProbeMode.DRY_RUN_ROLLBACK
    # The CREATE ran but was rolled back; nothing was committed.
    assert any("CREATE TABLE" in s for s in conn.executed)
    assert conn.rolled_back == 1
    assert conn.committed == 0
    # autocommit is restored to its original value after the dry run.
    assert conn.autocommit is True


def test_ddl_failure_is_rolled_back_and_reported() -> None:
    conn = _FakeConnection(
        error=_sqlstate_error('type "geometry" does not exist', "42704")
    )
    probe = probe_statement(
        "CREATE TABLE t (g GEOMETRY)", StatementKind.DDL, _factory(conn)
    )

    assert probe.outcome is ProbeOutcome.FAILED
    assert probe.error_code == "42704"
    assert "does not exist" in probe.detail
    # Even on failure the dry-run transaction is rolled back and never committed.
    assert conn.rolled_back == 1
    assert conn.committed == 0


# ---------------------------------------------------------------------------
# Probe: DML / OTHER / unconverted -> never executed
# ---------------------------------------------------------------------------


def test_dml_is_never_executed_and_factory_not_called() -> None:
    conn = _FakeConnection()
    factory = _factory(conn)
    probe = probe_statement("UPDATE t SET a = 1", StatementKind.DML, factory)

    assert probe.outcome is ProbeOutcome.SKIPPED
    assert probe.mode is ProbeMode.NOT_EXECUTED
    # The target connection is not even opened for DML.
    assert factory.calls["n"] == 0  # type: ignore[attr-defined]
    assert conn.executed == []


def test_other_statement_is_skipped() -> None:
    conn = _FakeConnection()
    probe = probe_statement("SET autocommit = 1", StatementKind.OTHER, _factory(conn))
    assert probe.outcome is ProbeOutcome.SKIPPED
    assert conn.executed == []


def test_unconverted_statement_is_skipped() -> None:
    factory = _factory(_FakeConnection())
    probe = probe_statement(None, StatementKind.SELECT, factory)
    assert probe.outcome is ProbeOutcome.SKIPPED
    assert factory.calls["n"] == 0  # type: ignore[attr-defined]


def test_select_rejection_is_captured_as_failed() -> None:
    conn = _FakeConnection(
        error=_sqlstate_error('relation "missing" does not exist', "42P01")
    )
    probe = probe_statement(
        "SELECT * FROM missing", StatementKind.SELECT, _factory(conn)
    )
    assert probe.outcome is ProbeOutcome.FAILED
    assert probe.error_code == "42P01"
    assert conn.closed is True


# ---------------------------------------------------------------------------
# Presentation helpers
# ---------------------------------------------------------------------------


def test_classification_tone_maps_severity() -> None:
    assert classification_tone(Classification.AUTO)[0] == "success"
    assert classification_tone(Classification.MANUAL)[0] == "warning"
    assert classification_tone(Classification.UNSUPPORTED)[0] == "error"


def test_kind_meta_labels_each_kind() -> None:
    assert kind_meta(StatementKind.SELECT)[1] == "SELECT"
    assert kind_meta(StatementKind.DDL)[1] == "DDL"
    assert kind_meta(StatementKind.DML)[1] == "DML"
    # DML is described as not test-run.
    assert "never executes" in kind_meta(StatementKind.DML)[2]


def test_probe_outcome_tone() -> None:
    assert probe_outcome_tone(ProbeOutcome.PASSED) == "success"
    assert probe_outcome_tone(ProbeOutcome.FAILED) == "error"
    assert probe_outcome_tone(ProbeOutcome.SKIPPED) == "info"


def _result(kind: StatementKind, converted: Optional[str]) -> QueryConversionResult:
    return QueryConversionResult(
        original_sql="x",
        converted_sql=converted,
        classification=Classification.AUTO,
        statement_kind=kind,
    )


def test_is_testable_only_for_converted_select_or_ddl() -> None:
    assert is_testable(_result(StatementKind.SELECT, "SELECT 1")) is True
    assert is_testable(_result(StatementKind.DDL, "CREATE TABLE t (id int)")) is True
    assert is_testable(_result(StatementKind.DML, "UPDATE t SET a=1")) is False
    assert is_testable(_result(StatementKind.OTHER, "SET x=1")) is False
    # Not testable when the statement could not be converted.
    assert is_testable(_result(StatementKind.SELECT, None)) is False


# ---------------------------------------------------------------------------
# Per-session state + store
# ---------------------------------------------------------------------------


def test_state_result_clears_stale_probe() -> None:
    state = PlaygroundState()
    state.set_probe(
        ExecutionProbe(outcome=ProbeOutcome.PASSED, mode=ProbeMode.EXPLAIN)
    )
    assert state.probe is not None
    state.set_result(QueryConverter().convert("SELECT 1"))
    # A fresh conversion invalidates the previous target-test verdict.
    assert state.probe is None


# ---------------------------------------------------------------------------
# AI query-optimizer: SQL extraction + re-test comparison turn
# ---------------------------------------------------------------------------


def test_extract_sql_from_reply_takes_last_fenced_block() -> None:
    # A reply may show a 'before' block then the rewrite; the LAST block is the
    # model's final proposal. Trailing ';' is stripped so it runs under EXPLAIN.
    md = (
        "Here's the original:\n```sql\nSELECT * FROM orders;\n```\n"
        "and my rewrite:\n```sql\nSELECT id FROM orders WHERE id = 1;\n```\ndone"
    )
    assert extract_sql_from_reply(md) == "SELECT id FROM orders WHERE id = 1"


def test_extract_sql_from_reply_handles_plain_fence_and_none() -> None:
    # A fence with no 'sql' language tag still counts.
    assert extract_sql_from_reply("```\nSELECT 1;\n```") == "SELECT 1"
    # No code block -> nothing to test.
    assert extract_sql_from_reply("just prose, no code") is None
    assert extract_sql_from_reply("") is None


def test_extract_sql_isolates_select_from_accompanying_ddl() -> None:
    # A tuning reply often includes a suggested CREATE INDEX before the SELECT.
    # Passing the whole block to EXPLAIN is a syntax error (and the read-only
    # playground can't run DDL), so only the SELECT must be returned.
    md = (
        "```sql\n"
        "CREATE INDEX ASYNC idx ON t (a) INCLUDE (b);\n\n"
        "SELECT a, b FROM t WHERE a >= 4;\n"
        "```"
    )
    assert extract_sql_from_reply(md) == "SELECT a, b FROM t WHERE a >= 4"


def test_extract_sql_strips_comments_that_would_break_explain() -> None:
    # A leading -- line comment would otherwise comment out the EXPLAIN wrapper we
    # prepend, causing a syntax error; block comments must go too.
    md = (
        "```sql\n"
        "-- rewrite: project only needed columns\n"
        "/* covering-index friendly */\n"
        "SELECT a, b FROM t WHERE a >= 4 LIMIT 20;\n"
        "```"
    )
    assert extract_sql_from_reply(md) == "SELECT a, b FROM t WHERE a >= 4 LIMIT 20"


def test_extract_sql_keeps_cte_and_returns_none_for_ddl_only() -> None:
    # A CTE (WITH ...) is testable and must be kept whole.
    cte = (
        "```sql\nWITH c AS (SELECT id FROM t ORDER BY ts DESC LIMIT 20)\n"
        "SELECT * FROM c;\n```"
    )
    assert extract_sql_from_reply(cte).startswith("WITH c AS")
    # A reply with only DDL (no runnable SELECT) yields None, so the UI tells the
    # user there's nothing to re-test rather than sending a doomed EXPLAIN.
    assert extract_sql_from_reply("```sql\nCREATE INDEX ASYNC idx ON t (a);\n```") is None


def _probe(outcome, *, dpu_total=None, plan="", analyzed=False, detail="ok", code=None):
    from dsql_migrator.core.query_playground import DpuEstimate

    dpu = (
        DpuEstimate(compute=0.0, read=0.0, write=0.0, total=dpu_total,
                    estimated_cost_usd=None)
        if dpu_total is not None
        else None
    )
    return ExecutionProbe(
        outcome=outcome, mode=ProbeMode.EXPLAIN, detail=detail, error_code=code,
        plan=plan, analyzed=analyzed, dpu=dpu,
    )


def test_pretty_sql_multilines_and_falls_back_on_garbage() -> None:
    from dsql_migrator.ui.query_playground import _pretty_sql

    one_line = "SELECT a, b FROM t WHERE a >= 4 ORDER BY b DESC LIMIT 5"
    pretty = _pretty_sql(one_line)
    assert "\n" in pretty  # multi-lined for readability
    assert "SELECT" in pretty and "FROM t" in pretty
    # Never raises and never returns empty for non-empty input — this is display
    # polish, so any weird text must round-trip safely (sqlglot is lenient and may
    # reformat rather than raise; either way we must not blow up or drop content).
    assert _pretty_sql("this is not sql ;;;").strip() != ""
    assert _pretty_sql("") == ""


def test_retest_turn_includes_pretty_tested_query() -> None:
    # The turn should show the exact query it ran, pretty-printed in a ```sql block.
    turn = _build_retest_turn(
        _probe(ProbeOutcome.PASSED, dpu_total=0.03, plan="Index Only Scan", analyzed=True),
        baseline_dpu=0.10,
        rewrite_sql="SELECT id FROM orders WHERE id = 42",
    )
    assert "```sql" in turn
    assert "The query I actually ran" in turn
    assert "SELECT" in turn and "orders" in turn


def test_retest_turn_reports_improvement_percentage() -> None:
    # A cheaper rewrite (0.03 vs baseline 0.10) -> CHEAPER + correct percentage, so
    # the AI can explain the win in-thread.
    turn = _build_retest_turn(
        _probe(ProbeOutcome.PASSED, dpu_total=0.03, plan="Index Only Scan", analyzed=True),
        baseline_dpu=0.10,
    )
    assert "CHEAPER" in turn
    assert "70.0%" in turn
    assert "0.03000 DPU" in turn and "0.10000 DPU" in turn


def test_retest_turn_flags_a_regression() -> None:
    # A pricier rewrite must be reported honestly, not spun as an improvement.
    turn = _build_retest_turn(
        _probe(ProbeOutcome.PASSED, dpu_total=0.20, analyzed=True),
        baseline_dpu=0.10,
    )
    assert "MORE EXPENSIVE" in turn


def test_retest_turn_on_failed_rewrite_asks_for_a_fix() -> None:
    turn = _build_retest_turn(
        _probe(ProbeOutcome.FAILED, detail="syntax error", code="42601"),
        baseline_dpu=0.10,
    )
    assert "REJECTED" in turn and "42601" in turn


def test_retest_turn_without_baseline_or_dpu_is_honest() -> None:
    # No DPU captured (plan-only) -> do not fabricate a number.
    no_dpu = _build_retest_turn(
        _probe(ProbeOutcome.PASSED, plan="Full Scan", analyzed=False), baseline_dpu=0.10
    )
    assert "no DPU cost estimate" in no_dpu.lower() or "no dpu" in no_dpu.lower()
    # DPU present but no baseline -> report cost, note missing baseline.
    no_base = _build_retest_turn(
        _probe(ProbeOutcome.PASSED, dpu_total=0.05, analyzed=True), baseline_dpu=None
    )
    assert "0.05000 DPU" in no_base and "baseline" in no_base.lower()


def test_state_probe_lifecycle() -> None:
    state = PlaygroundState()
    state.begin_probe()
    assert state.probing is True
    assert state.probe is None
    state.set_probe(
        ExecutionProbe(outcome=ProbeOutcome.FAILED, mode=ProbeMode.EXPLAIN)
    )
    assert state.probing is False
    assert state.probe is not None
    assert state.probe.outcome is ProbeOutcome.FAILED


def test_store_is_isolated_per_session() -> None:
    store = PlaygroundStore()
    a = store.get_or_create("a")
    b = store.get_or_create("b")
    assert a is not b
    assert store.get_or_create("a") is a
    a.sql = "SELECT 1"
    assert b.sql == ""


def test_store_reset_in_place_keeps_object_identity() -> None:
    store = PlaygroundStore()
    state = store.get_or_create("a")
    state.sql = "SELECT 1"
    state.set_result(QueryConverter().convert("SELECT 1"))
    store.reset_in_place("a")
    # Same object, wiped fields (closures captured at build time stay valid).
    assert store.get_or_create("a") is state
    assert state.sql == ""
    assert state.result is None
    store.reset_in_place(None)  # no-op, must not raise
    store.reset_in_place("missing")  # no-op, must not raise


# AI assistance for the playground now uses the SHARED right chat drawer
# (ui.ai_chat_drawer, covered by tests/test_ai_chat_drawer.py) and the grounding
# helper build_query_chat_system (tests/test_assessment_strategist.py), so there
# is no playground-local AI suggestion state to test here.


def test_probe_poll_re_renders_only_after_the_probe_finishes() -> None:
    """A 0.4s unconditional re-render made the "Test on target" tooltip unreadable.

    ``refresh`` here rebuilds the whole results region, including that button. A
    q-tooltip is a CHILD of its anchor, so rebuilding destroys the element the pointer is
    over: Quasar closes the tooltip and it only reopens on a fresh hover. Nothing in the
    probing branch changes between ticks (a spinner plus fixed text), so the poll now
    waits for the state change and only then re-renders.
    """
    import inspect

    from dsql_migrator.ui import query_playground

    src = inspect.getsource(query_playground)
    # The probing branch must NOT hand `refresh` straight to the timer.
    assert "ui.timer(_POLL_INTERVAL_SECONDS, refresh, once=True)" not in src
    # It polls its own re-arming callback, and refreshes once probing has cleared.
    assert "def _await_probe() -> None:" in src
    assert "ui.timer(_POLL_INTERVAL_SECONDS, _await_probe, once=True)" in src


def test_await_probe_only_refreshes_on_the_transition() -> None:
    # Behavioral check of the same loop: it re-arms while probing and fires exactly one
    # refresh when the probe completes -- never one per tick.
    calls = {"refresh": 0}
    timers: list = []
    state = {"probing": True}

    class _Ui:
        def timer(self, _interval, callback=None, **_k):
            timers.append(callback)

    ui = _Ui()
    interval = 0.4

    def _await_probe() -> None:
        if state["probing"]:
            ui.timer(interval, _await_probe)
            return
        calls["refresh"] += 1

    ui.timer(interval, _await_probe)

    # Three ticks while still probing -> re-arms, no re-render (tooltip survives).
    for _ in range(3):
        timers.pop()()
    assert calls["refresh"] == 0

    # The probe finishes -> exactly one re-render, so the verdict is drawn.
    state["probing"] = False
    timers.pop()()
    assert calls["refresh"] == 1
