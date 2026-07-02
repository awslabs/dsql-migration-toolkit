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
    classification_tone,
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
