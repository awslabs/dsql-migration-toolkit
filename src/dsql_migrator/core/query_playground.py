"""Non-destructive "does it run on DSQL?" probe for the Query Playground.

The Query Playground lets a user paste a MySQL statement, see how it converts to
Aurora DSQL (PostgreSQL) via :class:`~dsql_migrator.core.query_converter.QueryConverter`,
and optionally *test whether the converted statement would run* against the target
DSQL cluster. The target is treated as **production** (Safety: assume production
unless proven otherwise), so this probe NEVER persists a change:

- ``SELECT`` -- validated with ``EXPLAIN`` against the live schema. ``EXPLAIN``
  (without ``ANALYZE``) plans the query but does not execute it and returns no
  table data, so it is a read-only check that the statement is accepted and its
  referenced objects/columns exist.
- ``DDL`` (``CREATE`` / ``ALTER`` / ``DROP`` / ``TRUNCATE``) -- executed as a *dry
  run* inside a transaction that is ALWAYS rolled back. The statement proves it is
  accepted by DSQL (syntax, types, constraints) without anything being committed.
- ``DML`` (``INSERT`` / ``UPDATE`` / ``DELETE`` / ``REPLACE``) -- NEVER executed.
  Mutating production data, even inside a rolled-back transaction, is out of scope;
  the playground converts and analyzes DML but does not run it.
- ``OTHER`` / unconvertible -- not run.

Everything here is a single read-only or rolled-back probe; there is no code path
that commits. The connection factory is injectable so unit tests drive a fake
connection and never reach a real cluster, and the default builds an
IAM-authenticated DSQL connection via
:class:`~dsql_migrator.core.target_connection.DsqlConnector`.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Any, Callable, Optional

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import TargetConnectionConfig
from dsql_migrator.core.query_converter import StatementKind
from dsql_migrator.core.target_connection import DsqlConnector

# A target connection factory opens one psycopg-style DSQL connection. Injectable
# so tests never reach a real cluster (mirrors the validator's seam).
TargetConnectionFactory = Callable[[], Any]


class ProbeOutcome(str, Enum):
    """The result of test-running a converted statement against the target.

    - ``PASSED`` -- the read-only / dry-run probe completed with no error.
    - ``FAILED`` -- the target rejected the statement (syntax, missing relation,
      unsupported feature, lock-rule violation, ...); ``detail`` explains why.
    - ``SKIPPED`` -- the statement was not run by policy (it is DML, or an
      ``OTHER``/unconvertible statement), so executability was not tested.
    """

    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class ProbeMode(str, Enum):
    """How a statement was probed against the target."""

    EXPLAIN = "EXPLAIN"  # SELECT: planned read-only, no data returned
    EXPLAIN_ANALYZE = "EXPLAIN_ANALYZE"  # SELECT: actually executed for real stats
    DRY_RUN_ROLLBACK = "DRY_RUN_ROLLBACK"  # DDL: executed then rolled back
    NOT_EXECUTED = "NOT_EXECUTED"  # DML / OTHER: not sent to the target


class DpuEstimate(BaseModel):
    """The per-statement DPU cost estimate Aurora DSQL reports.

    Aurora DSQL extends ``EXPLAIN ANALYZE VERBOSE`` with a ``Statement DPU
    Estimate`` block (Compute / Read / Write / Total) -- this is DSQL's own
    measured estimate for the executed statement, not something the tool computes.
    ``estimated_cost_usd`` is the only tool-derived value: ``total × price`` for
    the configured per-DPU price (advisory; Region/time-dependent).
    """

    model_config = ConfigDict(extra="forbid")

    compute: float = Field(ge=0.0)
    read: float = Field(ge=0.0)
    write: float = Field(ge=0.0)
    total: float = Field(ge=0.0)
    estimated_cost_usd: Optional[float] = Field(default=None, ge=0.0)


class ExecutionProbe(BaseModel):
    """Outcome of a non-destructive test-run of one converted statement.

    ``mode`` records HOW it was probed (or that it was not executed), ``detail``
    is a short, log-safe English explanation, and ``error_code`` carries the
    PostgreSQL/DSQL ``SQLSTATE`` when the target rejected the statement (``None``
    otherwise). ``plan`` is the captured ``EXPLAIN`` (or ``EXPLAIN ANALYZE``)
    query plan text for a SELECT probe, and ``analyzed`` is ``True`` when the plan
    came from ``EXPLAIN ANALYZE`` (the query was actually executed for real
    timings/row counts) rather than a plan-only ``EXPLAIN``. ``dpu`` carries the
    parsed ``Statement DPU Estimate`` block when the analyze probe reported one.
    """

    model_config = ConfigDict(extra="forbid")

    outcome: ProbeOutcome
    mode: ProbeMode
    detail: str = ""
    error_code: Optional[str] = None
    plan: Optional[str] = None
    analyzed: bool = False
    dpu: Optional[DpuEstimate] = None


def _default_target_connection_factory(
    target: TargetConnectionConfig,
) -> TargetConnectionFactory:
    """Build the default IAM-authenticated DSQL connection factory for ``target``.

    Mirrors the validator's default: the IAM token is generated and kept
    confidential by :class:`DsqlConnector` (Property 7 / Requirement 5.4).
    """

    def factory() -> Any:
        return DsqlConnector(target).connect()

    return factory


def _strip_terminator(sql: str) -> str:
    """Return ``sql`` without a trailing semicolon/whitespace (for EXPLAIN prefixing)."""
    return sql.rstrip().rstrip(";").rstrip()


def _safe_close(closeable: Any) -> None:
    """Close a cursor/connection, swallowing any error during cleanup."""
    try:
        closeable.close()
    except Exception:  # noqa: BLE001 - cleanup must not raise
        pass


def _probe_detail(exc: Exception) -> tuple[str, Optional[str]]:
    """Render a target rejection as a (message, sqlstate) pair (log-safe).

    Keeps only the first line of the driver message so a multi-line dump never
    bloats the UI, and surfaces the PostgreSQL ``SQLSTATE`` when present so the
    user gets the specific error class.
    """
    sqlstate = getattr(exc, "sqlstate", None)
    lines = str(exc).strip().splitlines()
    message = lines[0].strip() if lines else exc.__class__.__name__
    return message or exc.__class__.__name__, sqlstate


def _format_plan(rows: Any) -> Optional[str]:
    """Join an ``EXPLAIN`` result set into one plan text block.

    PostgreSQL/DSQL ``EXPLAIN`` returns the plan as one column over several rows
    ("QUERY PLAN"); this concatenates those rows back into the familiar multi-line
    plan. Returns ``None`` when there were no rows so the UI can omit the panel.
    """
    lines: list[str] = []
    for row in rows or []:
        # Each row is a 1-tuple/sequence whose first column is the plan line.
        cell = row[0] if isinstance(row, (list, tuple)) and row else row
        lines.append("" if cell is None else str(cell))
    text = "\n".join(lines).strip()
    return text or None


# Default per-DPU price (USD) for the cost estimate. Aurora DSQL bills DPUs per
# million; us-east-1 is $8.00 / 1M DPU at the time of writing, so one DPU is
# 8.0/1_000_000. This is advisory and Region/time-dependent -- the UI labels it
# approximate and points to the pricing page; it can be overridden per call.
DEFAULT_USD_PER_DPU = 8.0 / 1_000_000

# Matches a "<label>: <number> DPU" line inside the "Statement DPU Estimate" block
# DSQL appends to EXPLAIN ANALYZE VERBOSE output (e.g. "  Compute: 0.01607 DPU").
_DPU_LINE_RE = re.compile(
    r"\b(compute|read|write|total)\b\s*:\s*([0-9]*\.?[0-9]+)\s*DPU",
    re.IGNORECASE,
)


def parse_dpu_estimate(
    plan: Optional[str], *, usd_per_dpu: float = DEFAULT_USD_PER_DPU
) -> Optional[DpuEstimate]:
    """Parse the ``Statement DPU Estimate`` block from an EXPLAIN VERBOSE plan.

    Aurora DSQL appends a per-statement estimate to ``EXPLAIN ANALYZE VERBOSE``
    output, e.g.::

        Statement DPU Estimate:
          Compute: 0.01607 DPU
          Read: 0.04312 DPU (Transaction minimum: 0.00375)
          Write: 0.00000 DPU
          Total: 0.05919 DPU

    This pulls out the Compute/Read/Write/Total values (tolerant of the trailing
    ``(Transaction minimum: ...)`` note) and attaches an advisory USD cost
    (``total × usd_per_dpu``). Returns ``None`` when the plan has no DPU block
    (e.g. a plan-only EXPLAIN, or a DSQL version that does not emit one), so the
    caller simply omits the cost panel. Pure/parsing-only -- never raises.
    """
    if not plan:
        return None
    values: dict[str, float] = {}
    for label, number in _DPU_LINE_RE.findall(plan):
        try:
            values[label.lower()] = float(number)
        except ValueError:  # pragma: no cover - regex guarantees a number
            continue
    if not {"compute", "read", "write"} <= values.keys():
        return None
    total = values.get("total")
    if total is None:
        total = values["compute"] + values["read"] + values["write"]
    cost = total * usd_per_dpu if usd_per_dpu and usd_per_dpu > 0 else None
    return DpuEstimate(
        compute=values["compute"],
        read=values["read"],
        write=values["write"],
        total=total,
        estimated_cost_usd=cost,
    )


def _run_explain(
    connection: Any,
    converted_sql: str,
    *,
    analyze: bool = False,
    usd_per_dpu: float = DEFAULT_USD_PER_DPU,
) -> ExecutionProbe:
    """Validate a SELECT with ``EXPLAIN`` and capture its query plan (+ DPU cost).

    Plan-only ``EXPLAIN`` (``analyze=False``) plans the query against the live
    schema without executing it (read-only, no data). ``EXPLAIN ANALYZE VERBOSE``
    (``analyze=True``) ACTUALLY executes the query to collect real timings/row
    counts AND makes Aurora DSQL append a per-statement ``Statement DPU Estimate``
    -- still a read for a SELECT, but it does run, so it is opt-in. The captured
    plan text and (when analyzed) the parsed DPU cost are returned on the probe.
    """
    # VERBOSE is required for DSQL to emit the Statement DPU Estimate block.
    keyword = "EXPLAIN ANALYZE VERBOSE" if analyze else "EXPLAIN"
    cursor = connection.cursor()
    try:
        cursor.execute(f"{keyword} {_strip_terminator(converted_sql)}")
        plan = _format_plan(cursor.fetchall())
    finally:
        _safe_close(cursor)
    dpu = parse_dpu_estimate(plan, usd_per_dpu=usd_per_dpu) if analyze else None
    detail = (
        "Executed with EXPLAIN ANALYZE VERBOSE against the live target: the query "
        "actually ran (read-only) and the plan below shows real timings, row "
        "counts, and Aurora DSQL's per-statement DPU cost estimate."
        if analyze
        else "Validated with EXPLAIN against the live target schema (read-only; "
        "the query was planned, not executed, and returned no data)."
    )
    return ExecutionProbe(
        outcome=ProbeOutcome.PASSED,
        mode=ProbeMode.EXPLAIN_ANALYZE if analyze else ProbeMode.EXPLAIN,
        detail=detail,
        plan=plan,
        analyzed=analyze,
        dpu=dpu,
    )


def _run_ddl_dry_run(connection: Any, converted_sql: str) -> ExecutionProbe:
    """Execute DDL inside a transaction that is ALWAYS rolled back (dry run).

    DSQL connections are autocommit; this temporarily turns autocommit off so the
    statement runs inside a transaction, then unconditionally rolls back so the
    object is never created/dropped for real. ``commit`` is never called on any
    path, so nothing this probe does can persist.
    """
    previous_autocommit = getattr(connection, "autocommit", True)
    cursor = connection.cursor()
    try:
        # Open an explicit transaction so the DDL does not auto-commit.
        try:
            connection.autocommit = False
        except Exception:  # noqa: BLE001 - some fakes have no settable attr
            pass
        try:
            cursor.execute(_strip_terminator(converted_sql))
        finally:
            # Roll back no matter what: a success must NOT persist, and a failure
            # must clear the aborted transaction.
            try:
                connection.rollback()
            except Exception:  # noqa: BLE001 - rollback best-effort
                pass
    finally:
        _safe_close(cursor)
        try:
            connection.autocommit = previous_autocommit
        except Exception:  # noqa: BLE001 - restore best-effort
            pass
    return ExecutionProbe(
        outcome=ProbeOutcome.PASSED,
        mode=ProbeMode.DRY_RUN_ROLLBACK,
        detail=(
            "Executed as a dry run inside a transaction that was rolled back; "
            "the statement is accepted by Aurora DSQL and nothing was persisted."
        ),
    )


# Per-kind reason shown when a statement is not test-run (SKIPPED).
_SKIP_DETAIL: dict[StatementKind, str] = {
    StatementKind.DML: (
        "INSERT/UPDATE/DELETE is not executed against the target: the playground "
        "never mutates production data. The conversion above is still validated."
    ),
    StatementKind.OTHER: (
        "This statement type is not test-run against the target (only SELECT is "
        "EXPLAINed and DDL is dry-run). Review the conversion above."
    ),
}


def probe_statement(
    converted_sql: Optional[str],
    statement_kind: StatementKind,
    connection_factory: TargetConnectionFactory,
    *,
    analyze: bool = False,
    usd_per_dpu: float = DEFAULT_USD_PER_DPU,
) -> ExecutionProbe:
    """Test-run one converted statement against the target, non-destructively.

    ``SELECT`` is validated with ``EXPLAIN`` and its query plan is captured;
    ``DDL`` is executed as a rolled-back dry run; ``DML`` and ``OTHER`` are never
    executed (``SKIPPED``). When ``analyze`` is set, a SELECT is probed with
    ``EXPLAIN ANALYZE VERBOSE`` instead -- which ACTUALLY executes the (read-only)
    query to collect real timings/row counts and Aurora DSQL's per-statement DPU
    cost estimate; it is opt-in because it runs the query. ``usd_per_dpu`` prices
    that estimate (advisory). A statement that could not be converted
    (``converted_sql is None``) is also skipped. The target connection is opened
    from ``connection_factory`` and always closed. A rejection by the target is
    captured as ``FAILED`` with the reason + ``SQLSTATE`` rather than raised, so
    the UI can render the verdict.
    """
    if converted_sql is None:
        return ExecutionProbe(
            outcome=ProbeOutcome.SKIPPED,
            mode=ProbeMode.NOT_EXECUTED,
            detail=(
                "There is no converted SQL to test (the statement could not be "
                "converted). Resolve the conversion warning first."
            ),
        )

    if statement_kind in (StatementKind.DML, StatementKind.OTHER):
        return ExecutionProbe(
            outcome=ProbeOutcome.SKIPPED,
            mode=ProbeMode.NOT_EXECUTED,
            detail=_SKIP_DETAIL[statement_kind],
        )

    # ANALYZE only applies to a SELECT (it executes the query for stats); DDL is
    # always a rolled-back dry run regardless.
    select_analyze = analyze and statement_kind is StatementKind.SELECT
    connection: Optional[Any] = None
    try:
        connection = connection_factory()
        if statement_kind is StatementKind.SELECT:
            return _run_explain(
                connection,
                converted_sql,
                analyze=select_analyze,
                usd_per_dpu=usd_per_dpu,
            )
        return _run_ddl_dry_run(connection, converted_sql)
    except Exception as exc:  # noqa: BLE001 - surfaced as a FAILED verdict
        message, sqlstate = _probe_detail(exc)
        if statement_kind is StatementKind.SELECT:
            mode = ProbeMode.EXPLAIN_ANALYZE if select_analyze else ProbeMode.EXPLAIN
        else:
            mode = ProbeMode.DRY_RUN_ROLLBACK
        return ExecutionProbe(
            outcome=ProbeOutcome.FAILED,
            mode=mode,
            detail=f"Aurora DSQL rejected the statement: {message}",
            error_code=sqlstate,
        )
    finally:
        if connection is not None:
            _safe_close(connection)


__all__ = [
    "TargetConnectionFactory",
    "ProbeOutcome",
    "ProbeMode",
    "DpuEstimate",
    "ExecutionProbe",
    "parse_dpu_estimate",
    "probe_statement",
    "DEFAULT_USD_PER_DPU",
]
