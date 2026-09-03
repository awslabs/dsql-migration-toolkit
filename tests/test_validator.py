# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the consistency validator (task 9).

Covers (Requirements 6.1-6.5, Properties 9 and 11):

- per-table row-count comparison (6.1) and the matching/mismatch verdicts,
- checksum-based data comparison (6.2) and that a deliberate data mismatch with
  equal row counts is still reported as a non-match (Property 9 soundness),
- optional orphan-record check on the target for preserved foreign keys (6.3),
- a validation report that includes the mismatched items (6.4),
- as-of-watermark validation: source row counts come from the watermark snapshot
  and source drift since the snapshot is reported via GTID comparison
  (6.5 / Property 11),
- read-only source access: only SELECT / transaction-control statements are
  issued (Property 1),
- safe identifier quoting in the generated checksum/orphan SQL (Requirement 9.4).

All tests use injected fakes mirroring the SQLAlchemy (source) and psycopg
(target) APIs; no real MySQL or DSQL connection is opened.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional

import pytest

from dsql_migrator.core.introspector import is_write_or_ddl
from dsql_migrator.core.models import (
    ColumnDef,
    ForeignKeyDef,
    SourceConnectionConfig,
    TableDef,
    TargetConnectionConfig,
    ValidationMode,
    Watermark,
)
from dsql_migrator.core.validation_sql import (
    _mysql_row_token,
    _pg_row_token,
)
from dsql_migrator.core.validator import (
    ValidationCancelled,
    Validator,
    _source_checksum,
    _source_checksum_keyset,
    _target_checksum,
    _target_checksum_keyset,
    build_mysql_checksum_sql,
    build_mysql_page_checksum_first_sql,
    build_mysql_page_checksum_next_sql,
    build_mysql_pk_token_sql,
    build_orphan_count_sql,
    build_pg_checksum_sql,
    build_pg_page_checksum_first_sql,
    build_pg_page_checksum_next_sql,
)

FIXED_NOW = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Fakes mirroring the SQLAlchemy (source) and psycopg (target) APIs
# ---------------------------------------------------------------------------


class _FakeScalarResult:
    """A result exposing the ``scalar()`` slice of SQLAlchemy's API."""

    def __init__(self, value: object = None) -> None:
        self._value = value

    def scalar(self) -> object:
        return self._value


class _FakeRowsResult:
    """A result that is iterable over ``(pk, token)`` rows (for the pk-token query)."""

    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _FakeSourceConnection:
    """A read-only-style source connection returning canned counts/checksums.

    Dispatches on statement text to mirror what the validator issues: a count, a
    checksum aggregate, the current GTID, or transaction control. Every executed
    statement is recorded for the read-only (Property 1) assertion.
    """

    def __init__(
        self,
        *,
        counts: Optional[dict[str, int]] = None,
        checksums: Optional[dict[str, str]] = None,
        pk_tokens: Optional[dict[str, list[tuple]]] = None,
        pk_sets: Optional[dict[str, list[int]]] = None,
        gtid: object = None,
    ) -> None:
        self._counts = counts or {}
        self._checksums = checksums or {}
        # {table: [(pk, token), ...]} for the row-diff pk-token SELECT.
        self._pk_tokens = pk_tokens or {}
        # {table: [pk, ...]} (ascending) for the full reconciliation keyset stream.
        self._pk_sets = {t: sorted(pks) for t, pks in (pk_sets or {}).items()}
        # Keep `counts` the single source of truth: a table given a count but no explicit
        # pk_set gets a synthetic ascending PK set of that size, so the bounded
        # keyset-paged count (which replaced the unbounded COUNT(*) for single-column-PK
        # tables) and reconciliation see the same total. Explicit pk_sets win (setdefault).
        for _t, _n in self._counts.items():
            self._pk_sets.setdefault(_t, list(range(1, int(_n) + 1)))
        self._gtid = gtid
        self.executed: list[str] = []
        self.execution_options_calls: list[dict] = []

    def execution_options(self, **kwargs: object) -> "_FakeSourceConnection":
        self.execution_options_calls.append(kwargs)
        return self

    def execute(self, statement: object, parameters: object = None) -> object:
        sql_text = str(statement)
        self.executed.append(sql_text)
        upper = sql_text.upper()
        params = parameters or {}

        if "@@GLOBAL.GTID_EXECUTED" in upper:
            if isinstance(self._gtid, Exception):
                raise self._gtid
            return _FakeScalarResult(self._gtid)
        # The paged checksum: "SELECT COALESCE(SUM(page_tok), 0), MAX(page_pk),
        # COUNT(*) FROM (... LIMIT :page) ckpage" -- returns ONE (sub_sum, last_pk,
        # count) row. Matched BEFORE every other branch: it contains MD5(/CONV(, AS
        # PAGE_PK/AS PAGE_TOK, and COUNT(*), which would otherwise misroute it. The
        # whole configured checksum lands on the first page (later pages contribute
        # 0), so the validator's per-page accumulation reproduces the total exactly.
        if "SUM(PAGE_TOK)" in upper:
            table = _last_backtick_table(sql_text)
            window = _keyset_page(
                self._pk_sets.get(table, []),
                last=params.get("last"),
                page=int(params.get("page", 0)),
            )
            last_pk = window[-1][0] if window else None
            sub_sum = (
                int(self._checksums.get(table, 0))
                if params.get("last") is None
                else 0
            )
            return _FakeRowsResult([(sub_sum, last_pk, len(window))])
        # The pk-token query also contains MD5(/CONV(, so it MUST be matched BEFORE
        # the checksum branch (it returns (pk, token) ROWS, not a scalar).
        if "AS TOK" in upper and "AS PK" in upper:
            rows = list(self._pk_tokens.get(_last_backtick_table(sql_text), []))
            limit = params.get("sample_size")
            if isinstance(limit, int):
                rows = rows[:limit]
            return _FakeRowsResult(rows)
        # The reconciliation keyset PK page: "SELECT `pk` AS pk ... ORDER BY ...
        # LIMIT :page" -- has AS PK but NOT AS TOK; returns (pk,) rows.
        if "AS PK" in upper and "LIMIT :PAGE" in upper:
            return _FakeRowsResult(
                _keyset_page(
                    self._pk_sets.get(_last_backtick_table(sql_text), []),
                    last=params.get("last"),
                    page=int(params.get("page", 0)),
                )
            )
        if "MD5(" in upper and "CONV(" in upper:
            return _FakeScalarResult(self._checksums.get(_last_backtick_table(sql_text)))
        if upper.startswith("SELECT COUNT(*)"):
            return _FakeScalarResult(self._counts.get(_last_backtick_table(sql_text), 0))
        # START TRANSACTION / COMMIT and any other control statement.
        return _FakeScalarResult(None)


class _FakeSourceConnectionContext:
    def __init__(self, connection: _FakeSourceConnection) -> None:
        self._connection = connection

    def __enter__(self) -> _FakeSourceConnection:
        return self._connection

    def __exit__(self, *exc: object) -> bool:
        return False


class _FakeSourceEngine:
    def __init__(self, connection: _FakeSourceConnection) -> None:
        self._connection = connection
        self.disposed = False

    def connect(self) -> _FakeSourceConnectionContext:
        return _FakeSourceConnectionContext(self._connection)

    def dispose(self) -> None:
        self.disposed = True


class _FakeTargetCursor:
    def __init__(self, connection: "_FakeTargetConnection") -> None:
        self._connection = connection
        self._result: object = None
        self._rows: Optional[list[tuple]] = None
        self.closed = False

    def execute(self, statement: Any, parameters: Any = None) -> None:
        self._result = self._connection.resolve(statement, parameters)
        # When the pk-token query ran, resolve() returns a row LIST; expose it via
        # fetchall() (the scalar path keeps using fetchone()).
        self._rows = self._result if isinstance(self._result, list) else None

    def fetchone(self) -> Optional[tuple]:
        if self._result is None or isinstance(self._result, list):
            return None
        # A multi-column aggregate (the paged checksum) is already a row tuple; a
        # scalar result is wrapped so callers can read it as row[0].
        if isinstance(self._result, tuple):
            return self._result
        return (self._result,)

    def fetchall(self) -> list[tuple]:
        return list(self._rows or [])

    def close(self) -> None:
        self.closed = True


class _FakeTargetConnection:
    """A psycopg-style target connection returning canned target values.

    ``counts``/``checksums`` are keyed by table name; ``orphans`` is keyed by the
    child table name. Statements arrive as ``psycopg.sql`` composables and are
    rendered to text for dispatch.
    """

    def __init__(
        self,
        *,
        counts: Optional[dict[str, int]] = None,
        checksums: Optional[dict[str, str]] = None,
        orphans: Optional[dict[str, int]] = None,
        pk_tokens: Optional[dict[str, list[tuple]]] = None,
        pk_sets: Optional[dict[str, list[int]]] = None,
        missing_tables: Optional[set[str]] = None,
    ) -> None:
        self._counts = counts or {}
        self._checksums = checksums or {}
        self._orphans = orphans or {}
        # {table: [(pk, token), ...]} for the row-diff pk-token SELECT.
        self._pk_tokens = pk_tokens or {}
        # {table: [pk, ...]} (ascending) for the full reconciliation keyset stream.
        self._pk_sets = {t: sorted(pks) for t, pks in (pk_sets or {}).items()}
        # Keep `counts` the single source of truth (see _FakeSourceConnection): a table
        # with a count but no explicit pk_set gets a synthetic ascending PK set of that
        # size so the bounded keyset target count matches COUNT(*). Explicit pk_sets win.
        for _t, _n in self._counts.items():
            self._pk_sets.setdefault(_t, list(range(1, int(_n) + 1)))
        # Tables that raise when read on the target (per-table error isolation).
        self._missing_tables = missing_tables or set()
        self.executed: list[str] = []
        self.closed = False

    def cursor(self) -> _FakeTargetCursor:
        return _FakeTargetCursor(self)

    def resolve(self, statement: Any, parameters: Any = None) -> object:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self.executed.append(text)
        params = parameters or {}
        table = _last_double_quoted_table(text)
        if table in self._missing_tables:
            raise RuntimeError(f'relation "{table}" does not exist')
        # The keyset-PAGED orphan count (single-column-PK child): returns ONE
        # (orphan_sub_count, last_pk, page_row_count) ROW via fetchone. Matched BEFORE the
        # single-scan orphan branch below (it also contains NOT EXISTS) and before the
        # keyset PK-page branch (it also has AS page_pk). The whole orphan count lands on
        # the FIRST page (later pages contribute 0), and the window is drawn from the
        # child's pk_set so the keyset advance/termination is exercised end-to-end.
        if "NOT EXISTS" in text and "FILTER" in text:
            # The child is the LAST ``FROM "..."`` (the parent appears first, inside the
            # NOT EXISTS), which ``_last_double_quoted_table`` already resolved as ``table``.
            child = table
            window = _keyset_page(
                self._pk_sets.get(child, []),
                last=params.get("last"),
                page=_limit_in(text),
            )
            last_pk = window[-1][0] if window else None
            sub_count = self._orphans.get(child, 0) if params.get("last") is None else 0
            return (sub_count, last_pk, len(window))
        if "NOT EXISTS" in text:
            child = _orphan_child_table(text)
            return self._orphans.get(child, 0)
        # The paged checksum: returns ONE (sub_sum, last_pk, count) ROW (via
        # fetchone). Matched FIRST because it also contains md5(/AS page_tok/COUNT(*),
        # which would otherwise misroute it. Whole checksum on the first page (later
        # pages 0) so the validator's per-page accumulation reproduces the total.
        if "SUM(page_tok)" in text:
            window = _keyset_page(
                self._pk_sets.get(table, []),
                last=params.get("last"),
                page=_limit_in(text),
            )
            last_pk = window[-1][0] if window else None
            sub_sum = (
                int(self._checksums.get(table, 0))
                if params.get("last") is None
                else 0
            )
            return (sub_sum, last_pk, len(window))
        # The pk-token query also contains md5(, so match it BEFORE the checksum
        # branch; it returns a row LIST (consumed via fetchall), not a scalar.
        if "AS tok" in text and "AS pk" in text:
            return list(self._pk_tokens.get(table, []))
        # The reconciliation keyset PK page: "SELECT ... AS pk ... LIMIT N" with no
        # md5/tok; returns a row LIST consumed via fetchall.
        if "AS pk" in text and "md5(" not in text:
            return _keyset_page(
                self._pk_sets.get(table, []),
                last=params.get("last"),
                page=_limit_in(text),
            )
        if "md5(" in text:
            return self._checksums.get(table)
        if "COUNT(*)" in text:
            return self._counts.get(table, 0)
        return None

    def close(self) -> None:
        self.closed = True


def _last_backtick_table(sql_text: str) -> str:
    matches = re.findall(r"FROM `([^`]+)`", sql_text)
    return matches[-1] if matches else ""


def _last_double_quoted_table(sql_text: str) -> str:
    matches = re.findall(r'FROM "([^"]+)"', sql_text)
    return matches[-1] if matches else ""


def _orphan_child_table(sql_text: str) -> str:
    match = re.search(r'FROM "([^"]+)" AS c', sql_text)
    return match.group(1) if match else ""


def _limit_in(sql_text: str) -> int:
    """Extract the literal ``LIMIT N`` bound from a rendered PG keyset page."""
    match = re.search(r"LIMIT (\d+)", sql_text)
    return int(match.group(1)) if match else 0


def _keyset_page(pks: list[int], *, last: object, page: int) -> list[tuple]:
    """Return one ascending keyset page of ``(pk,)`` rows after ``last``.

    Mirrors ``WHERE pk > :last ORDER BY pk LIMIT page`` over a sorted PK list, so
    the validator's page-by-page streaming is exercised end-to-end (multi-page
    when ``page`` is small).
    """
    start = 0 if last is None else _bisect_after(pks, int(last))
    return [(pk,) for pk in pks[start : start + page]]


def _bisect_after(pks: list[int], last: int) -> int:
    """Index of the first PK strictly greater than ``last`` (pks ascending)."""
    lo, hi = 0, len(pks)
    while lo < hi:
        mid = (lo + hi) // 2
        if pks[mid] <= last:
            lo = mid + 1
        else:
            hi = mid
    return lo


# ---------------------------------------------------------------------------
# Builders / fixtures
# ---------------------------------------------------------------------------


def _table(
    name: str = "orders",
    columns: tuple[str, ...] = ("id", "amount"),
    primary_key: tuple[str, ...] = ("id",),
    foreign_keys: Optional[list[ForeignKeyDef]] = None,
) -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name=c, mysql_type="int") for c in columns],
        primary_key=list(primary_key),
        foreign_keys=foreign_keys or [],
    )


def _typed_table(name: str = "t") -> TableDef:
    """A table exercising every cross-engine checksum render class (§A.6)."""
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="payload", mysql_type="BLOB"),
            ColumnDef(name="geo", mysql_type="POINT"),
            ColumnDef(name="flags", mysql_type="BIT(8)"),
            ColumnDef(name="active", mysql_type="TINYINT(1)"),
            ColumnDef(name="created_at", mysql_type="DATETIME(6)"),
            ColumnDef(name="ts", mysql_type="TIMESTAMP"),
            ColumnDef(name="t_of_day", mysql_type="TIME"),
            ColumnDef(name="amount", mysql_type="DECIMAL(10,4)"),
            ColumnDef(name="ratio", mysql_type="DOUBLE"),
        ],
        primary_key=["id"],
    )


def _source(connection: _FakeSourceConnection) -> Validator:
    """Build a Validator wired to a fixed source connection (target injected separately)."""
    engine = _FakeSourceEngine(connection)
    return Validator(source_engine_factory=lambda _conn: engine)


def _validator(
    source_connection: _FakeSourceConnection,
    target_connection: _FakeTargetConnection,
) -> Validator:
    engine = _FakeSourceEngine(source_connection)
    return Validator(
        source_engine_factory=lambda _conn: engine,
        target_connection_factory=lambda _target: target_connection,
    )


_SOURCE_CONFIG = SourceConnectionConfig(host="db.example.com", database="app")
_TARGET_CONFIG = TargetConnectionConfig(
    cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
)


# ---------------------------------------------------------------------------
# Row-count comparison (Requirement 6.1)
# ---------------------------------------------------------------------------


def test_matching_row_counts_report_overall_match() -> None:
    source = _FakeSourceConnection(counts={"orders": 5, "customers": 3})
    target = _FakeTargetConnection(counts={"orders": 5, "customers": 3})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders"), _table("customers")]
    )

    assert report.is_match is True
    assert {item.table for item in report.items} == {"orders", "customers"}
    assert all(item.matched for item in report.items)
    assert all(item.row_count_match for item in report.items)


def test_row_count_mismatch_is_reported() -> None:
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _FakeTargetConnection(counts={"orders": 4})  # one row missing on target

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]
    )

    assert report.is_match is False
    item = report.items[0]
    assert item.source_row_count == 5
    assert item.target_row_count == 4
    assert item.row_count_match is False
    assert item.matched is False


# ---------------------------------------------------------------------------
# Table-level parallel validation (max_workers > 1)
# ---------------------------------------------------------------------------


def _parallel_validator(
    counts: dict[str, int], target_counts: dict[str, int]
) -> "Validator":
    """A Validator whose factories mint a FRESH fake per call (per-worker conns).

    The parallel path opens one source snapshot + one target connection per table,
    so the factories must return new instances each call (a shared single
    connection would not exercise the per-worker isolation).
    """

    def _src_factory(_cfg):
        return _FakeSourceEngine(_FakeSourceConnection(counts=dict(counts)))

    def _tgt_factory(_cfg):
        return _FakeTargetConnection(counts=dict(target_counts))

    return Validator(
        source_engine_factory=_src_factory,
        target_connection_factory=_tgt_factory,
    )


def test_parallel_validation_matches_sequential_and_preserves_order() -> None:
    counts = {"orders": 5, "customers": 3, "items": 9}
    tables = [_table("orders"), _table("customers"), _table("items")]

    report = _parallel_validator(counts, counts).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, tables, max_workers=3
    )

    assert report.is_match is True
    # Results are reassembled in the ORIGINAL table order regardless of which
    # worker finished first.
    assert [item.table for item in report.items] == ["orders", "customers", "items"]
    assert all(item.matched for item in report.items)


# ---------------------------------------------------------------------------
# Target-connection reconnect on an aged-out / dropped DSQL connection.
#
# DSQL force-closes a connection at its ~1h maximum connection duration. A >1h
# validation of a billion-row table (keyset count + PK reconcile reuse ONE target
# connection) would otherwise error the table on the drop and block cut-over. The
# read path now transparently reconnects (new IAM token) and replays the in-flight
# statement -- the same aged-connection hardening the write paths already have.
# ---------------------------------------------------------------------------


def _transient_drop() -> Exception:
    """A no-SQLSTATE connection drop the way DSQL force-closes an aged connection."""
    return Exception("server closed the connection unexpectedly")


class _ScriptedTargetCursor:
    def __init__(self, conn: "_ScriptedTargetConn") -> None:
        self._conn = conn
        self.closed = False

    def execute(self, statement: Any, parameters: Any = None) -> None:
        self._conn.executes.append((statement, parameters))
        if self._conn.die:
            raise _transient_drop()

    def fetchone(self) -> Optional[tuple]:
        return (self._conn.value,)

    def fetchall(self) -> list[tuple]:
        return [(self._conn.value,)]

    def close(self) -> None:
        self.closed = True


class _ScriptedTargetConn:
    """A target connection that either dies transiently on every execute (``die``)
    or serves a canned scalar; records executes/close for assertions."""

    def __init__(self, *, die: bool, value: object = "ok") -> None:
        self.die = die
        self.value = value
        self.executes: list = []
        self.closed = False

    def cursor(self) -> _ScriptedTargetCursor:
        return _ScriptedTargetCursor(self)

    def close(self) -> None:
        self.closed = True


def test_reconnecting_target_replays_statement_on_a_transient_drop() -> None:
    from dsql_migrator.core.validator import _ReconnectingTargetConnection

    dying = _ScriptedTargetConn(die=True)
    healthy = _ScriptedTargetConn(die=False, value="ok")
    made: list = []

    def factory() -> _ScriptedTargetConn:
        conn = dying if not made else healthy
        made.append(conn)
        return conn

    proxy = _ReconnectingTargetConnection(factory, sleep=lambda _s: None)
    cursor = proxy.cursor()
    cursor.execute("SELECT 1", {"last": 42})

    assert cursor.fetchone() == ("ok",)  # served by the fresh (reconnected) connection
    assert dying.closed is True  # aged-out connection discarded
    assert len(made) == 2  # eager connect + exactly one reconnect
    # the SAME statement is replayed on the fresh connection (keyset resume-safe).
    assert healthy.executes == [("SELECT 1", {"last": 42})]


def test_reconnecting_target_propagates_a_non_transient_error() -> None:
    from dsql_migrator.core.validator import _ReconnectingTargetConnection

    class _PermCursor:
        def __init__(self, conn: "_PermConn") -> None:
            self._conn = conn

        def execute(self, statement: Any, parameters: Any = None) -> None:
            self._conn.executes += 1
            err = Exception("relation does not exist")
            err.sqlstate = "42P01"  # a real query error, NOT a connection drop
            raise err

        def close(self) -> None:
            pass

    class _PermConn:
        def __init__(self) -> None:
            self.executes = 0
            self.closed = False

        def cursor(self) -> "_PermCursor":
            return _PermCursor(self)

        def close(self) -> None:
            self.closed = True

    conns: list = []

    def factory() -> _PermConn:
        conn = _PermConn()
        conns.append(conn)
        return conn

    proxy = _ReconnectingTargetConnection(factory, sleep=lambda _s: None)
    with pytest.raises(Exception) as excinfo:
        proxy.cursor().execute("SELECT 1")

    assert getattr(excinfo.value, "sqlstate", None) == "42P01"
    assert len(conns) == 1  # a real query error must NOT reconnect


def test_reconnecting_target_gives_up_after_max_attempts() -> None:
    from dsql_migrator.core.validator import _ReconnectingTargetConnection

    made: list = []

    def factory() -> _ScriptedTargetConn:
        conn = _ScriptedTargetConn(die=True)
        made.append(conn)
        return conn

    proxy = _ReconnectingTargetConnection(
        factory, max_attempts=3, base_delay=0.0, sleep=lambda _s: None
    )
    with pytest.raises(Exception):
        proxy.cursor().execute("SELECT 1")

    # eager connect + 2 reconnects, then give up -> exactly max_attempts execute tries.
    assert len(made) == 3


def test_validation_recovers_when_the_target_connection_drops_mid_run(monkeypatch) -> None:
    # The aged-connection fix end-to-end: the target connection is force-closed
    # partway through validating a table; the read path reconnects and resumes, so
    # the table MATCHES rather than erroring (which would block the cut-over gate).
    import dsql_migrator.core.validator as validator_mod

    monkeypatch.setattr(validator_mod.time, "sleep", lambda _s: None)

    source = _FakeSourceConnection(counts={"orders": 5})
    healthy = _FakeTargetConnection(counts={"orders": 5})
    dying = _ScriptedTargetConn(die=True)
    calls: list = []

    def _tgt_factory(_cfg: Any):
        calls.append(1)
        return dying if len(calls) == 1 else healthy

    validator = Validator(
        source_engine_factory=lambda _conn: _FakeSourceEngine(source),
        target_connection_factory=_tgt_factory,
    )
    report = validator.validate(_SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")])

    assert report.is_match is True  # recovered -> matched, not errored
    item = report.items[0]
    assert item.matched is True
    assert item.target_row_count == 5
    assert dying.closed is True  # the dropped connection was discarded
    assert len(calls) >= 2  # reconnected at least once


def test_parallel_validation_reports_progress_monotonically() -> None:
    counts = {"orders": 1, "customers": 1, "items": 1}
    tables = [_table("orders"), _table("customers"), _table("items")]
    seen: list[tuple[str, int, int]] = []

    _parallel_validator(counts, counts).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, tables, max_workers=3,
        on_progress=lambda t, i, n: seen.append((t, i, n)),
    )

    # One callback per table; the index rises 1..N and the total is constant.
    assert len(seen) == 3
    assert [i for _t, i, _n in seen] == [1, 2, 3]
    assert {n for _t, _i, n in seen} == {3}


def test_parallel_validation_cancel_raises_and_skips() -> None:
    counts = {"orders": 1, "customers": 1, "items": 1}
    tables = [_table("orders"), _table("customers"), _table("items")]

    with pytest.raises(ValidationCancelled):
        _parallel_validator(counts, counts).validate(
            _SOURCE_CONFIG, _TARGET_CONFIG, tables, max_workers=2,
            should_cancel=lambda: True,  # cancelled from the start
        )


# ---------------------------------------------------------------------------
# Fast sweep: deep-check only on count mismatch (B)
# ---------------------------------------------------------------------------


def test_fast_sweep_skips_checksum_when_counts_match() -> None:
    # CHECKSUM mode + deep_only_on_count_mismatch: a count-matched table must NOT
    # run the checksum (reported None == not run), and still read as matched by
    # count -- never a false equality claim.
    source = _FakeSourceConnection(
        counts={"orders": 5}, checksums={"orders": "111"}
    )
    target = _FakeTargetConnection(
        counts={"orders": 5}, checksums={"orders": "999"}  # would mismatch if run
    )

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")],
        ValidationMode.CHECKSUM, deep_only_on_count_mismatch=True,
    )

    item = report.items[0]
    assert item.row_count_match is True
    assert item.checksum_match is None  # skipped (not run), not False
    assert item.source_checksum is None and item.target_checksum is None
    assert item.matched is True  # verified by count


def test_fast_sweep_runs_checksum_when_counts_differ() -> None:
    # A count MISMATCH still triggers the deep checksum so divergence is caught.
    source = _FakeSourceConnection(
        counts={"orders": 5}, checksums={"orders": "111"}
    )
    target = _FakeTargetConnection(
        counts={"orders": 4}, checksums={"orders": "999"}
    )

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")],
        ValidationMode.CHECKSUM, deep_only_on_count_mismatch=True,
    )

    item = report.items[0]
    assert item.row_count_match is False
    assert item.checksum_match is False  # deep check ran and caught the divergence
    assert item.matched is False


# ---------------------------------------------------------------------------
# Checksum comparison + soundness (Requirement 6.2 / Property 9)
# ---------------------------------------------------------------------------


def test_checksum_match_reports_overall_match() -> None:
    source = _FakeSourceConnection(counts={"orders": 2}, checksums={"orders": "987"})
    target = _FakeTargetConnection(counts={"orders": 2}, checksums={"orders": "987"})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )

    assert report.is_match is True
    item = report.items[0]
    assert item.checksum_match is True
    assert item.source_checksum == "987"
    assert item.target_checksum == "987"
    assert item.matched is True


def test_checksum_excluded_columns_are_recorded_and_surfaced() -> None:
    """M2: FLOAT/DOUBLE and JSON columns are omitted from the checksum (no byte-identical
    cross-engine form). The omission is recorded per table and surfaced in the report so a
    MATCH is not misread as 'every column verified' -- a non-key value diff confined to
    such a column is undetected by any mode."""
    from dsql_migrator.core.validator import render_text_report

    table = _table("metrics", columns=("id", "ratio", "meta"))
    table.columns[1].mysql_type = "double"
    table.columns[2].mysql_type = "json"
    source = _FakeSourceConnection(counts={"metrics": 2}, checksums={"metrics": "42"})
    target = _FakeTargetConnection(counts={"metrics": 2}, checksums={"metrics": "42"})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [table], ValidationMode.CHECKSUM
    )
    item = report.items[0]
    assert item.matched is True  # a false MATCH would be silent without the disclosure
    assert item.checksum_excluded_columns == ["ratio", "meta"]
    assert "not value-compared" in render_text_report(report).lower()

    # ROW_COUNT mode runs no checksum, so it records no exclusions.
    rc = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [table], ValidationMode.ROW_COUNT
    )
    assert rc.items[0].checksum_excluded_columns == []


def test_deliberate_data_mismatch_with_equal_counts_is_not_a_match() -> None:
    """Property 9: equal row counts but different checksums must NOT report match."""
    source = _FakeSourceConnection(counts={"orders": 3}, checksums={"orders": "111"})
    target = _FakeTargetConnection(counts={"orders": 3}, checksums={"orders": "222"})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )

    assert report.is_match is False
    item = report.items[0]
    assert item.row_count_match is True  # counts agree...
    assert item.checksum_match is False  # ...but the data does not
    assert item.matched is False


# ---------------------------------------------------------------------------
# Bounded page-checksum (CHECKSUM avoids DSQL's 300s single-scan limit)
# ---------------------------------------------------------------------------


def test_page_checksum_shares_the_whole_table_row_token() -> None:
    # The paged builder MUST sum the exact same per-row token as the whole-table
    # checksum (and the row-diff sample) -- a drift would silently mismatch.
    table = _typed_table("t")
    mysql_token = _mysql_row_token(table)
    assert f"COALESCE(SUM({mysql_token}), 0)" in build_mysql_checksum_sql(table)
    assert f"{mysql_token} AS page_tok" in build_mysql_page_checksum_first_sql(table, "id")
    assert f"{mysql_token} AS tok" in build_mysql_pk_token_sql(table, "id")

    pg_token = _pg_row_token(table).as_string(None)
    assert pg_token in build_pg_checksum_sql(table).as_string(None)
    assert pg_token in build_pg_page_checksum_first_sql(table, "id", 5000).as_string(None)


def _int_cols_table(n: int, name: str = "w") -> TableDef:
    return TableDef(
        name=name,
        columns=[ColumnDef(name=f"c{i}", mysql_type="int") for i in range(n)],
        primary_key=["c0"],
        foreign_keys=[],
    )


def test_row_token_stays_flat_within_the_arg_limit() -> None:
    # At exactly _CONCAT_MAX_TERMS columns the token is the ORIGINAL single flat
    # CONCAT_WS on BOTH engines -- no nesting, so narrow tables are byte-unchanged.
    from dsql_migrator.core.validation_sql import _CONCAT_MAX_TERMS

    table = _int_cols_table(_CONCAT_MAX_TERMS)
    assert _mysql_row_token(table).count("CONCAT_WS(") == 1
    assert _pg_row_token(table).as_string(None).count("concat_ws(") == 1


def test_row_token_nests_md5_for_wide_tables_over_the_pg_arg_limit() -> None:
    # A table wider than the PG/DSQL 100-argument limit must NEST MD5s per group so
    # no single concat_ws exceeds the limit -- identically on both engines, so equal
    # data still hashes equally. (255-column tables are supported by the assessor.)
    from dsql_migrator.core.validation_sql import _CONCAT_MAX_TERMS

    assert _CONCAT_MAX_TERMS < 100  # leaves room for the separator argument

    # One column past the limit already triggers nesting (2 groups -> 3 concat_ws).
    just_over = _int_cols_table(_CONCAT_MAX_TERMS + 1)
    assert _mysql_row_token(just_over).count("CONCAT_WS(") == 3
    assert _pg_row_token(just_over).as_string(None).count("concat_ws(") == 3

    # A realistic wide table (200 int columns -> 3 groups of <=96) + drift guard.
    n = 200
    wide = _int_cols_table(n)
    groups = (n + _CONCAT_MAX_TERMS - 1) // _CONCAT_MAX_TERMS
    my = _mysql_row_token(wide)
    pg = _pg_row_token(wide).as_string(None)
    assert my.count("CONCAT_WS(") == groups + 1  # `groups` inner + 1 outer
    assert pg.count("concat_ws(") == groups + 1  # PG nests identically
    assert groups <= _CONCAT_MAX_TERMS  # outer concat also stays under the limit
    # Every checksum builder still reduces over this exact wide token (no drift).
    assert f"COALESCE(SUM({my}), 0)" in build_mysql_checksum_sql(wide)
    assert f"{my} AS page_tok" in build_mysql_page_checksum_first_sql(wide, "c0")
    assert pg in build_pg_checksum_sql(wide).as_string(None)


def test_pg_page_checksum_sql_is_keyset_bounded_and_qualified() -> None:
    first = build_pg_page_checksum_first_sql(_table("orders"), "id", 500).as_string(None)
    nxt = build_pg_page_checksum_next_sql(_table("orders"), "id", 500).as_string(None)
    # The last-PK uses array_agg (not MAX) so a uuid PK -- which has no max() aggregate
    # in PostgreSQL/DSQL -- still keyset-advances (see build_pg_page_checksum_first_sql).
    for part in (
        "SUM(page_tok)",
        "(array_agg(page_pk ORDER BY page_pk))[COUNT(*)]",
        "COUNT(*)",
        "LIMIT 500",
    ):
        assert part in first and part in nxt
    assert "WHERE" not in first  # the first page carries no keyset predicate
    assert '"id" > %(last)s' in nxt  # subsequent pages are keyset+parameterized
    # schema-qualified name splits to "schema"."table", never one quoted identifier
    q = build_pg_page_checksum_first_sql(_table("app.orders"), "id", 10).as_string(None)
    assert '"app"."orders"' in q


def test_mysql_page_checksum_sql_is_keyset_bounded_and_qualified() -> None:
    first = build_mysql_page_checksum_first_sql(_table("orders"), "id")
    nxt = build_mysql_page_checksum_next_sql(_table("orders"), "id")
    for part in ("SUM(page_tok)", "MAX(page_pk)", "COUNT(*)", "LIMIT :page"):
        assert part in first and part in nxt
    assert "WHERE" not in first
    assert "WHERE `id` > :last" in nxt
    assert "`app`.`orders`" in build_mysql_page_checksum_first_sql(_table("app.orders"), "id")


class _PagedTargetCursor:
    def __init__(self, owner: "_PagedTargetConn") -> None:
        self._owner = owner
        self._row: Optional[tuple] = None

    def execute(self, statement: Any, parameters: Any = None) -> None:
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self._owner.executed.append((text, parameters))
        self._row = self._owner.pages.pop(0) if self._owner.pages else None

    def fetchone(self) -> Optional[tuple]:
        return self._row

    def close(self) -> None:
        pass


class _PagedTargetConn:
    """A target conn returning scripted ``(sub_sum, last_pk, count)`` rows per page."""

    def __init__(self, pages: list[tuple]) -> None:
        self.pages = list(pages)
        self.executed: list[tuple] = []

    def cursor(self) -> _PagedTargetCursor:
        return _PagedTargetCursor(self)


class _PagedSourceConn:
    """A source conn returning one scripted ``(sub_sum, last_pk, count)`` row per page."""

    def __init__(self, pages: list[tuple]) -> None:
        self.pages = list(pages)
        self.executed: list[tuple] = []

    def execute(self, statement: object, parameters: object = None) -> _FakeRowsResult:
        self.executed.append((str(statement), parameters or {}))
        row = self.pages.pop(0) if self.pages else None
        return _FakeRowsResult([row] if row is not None else [])


def test_target_checksum_keyset_accumulates_subsums_and_advances_keyset() -> None:
    # Three pages summing 100+200+50 = 350; the last page (count 2 < page 3) ends it.
    conn = _PagedTargetConn([(100, 10, 3), (200, 20, 3), (50, 25, 2)])
    total = _target_checksum_keyset(conn, _table("orders"), "id", 3)
    assert total == "350"
    # Genuinely paged: three statements, keyset-advanced by MAX(page_pk).
    assert len(conn.executed) == 3
    assert conn.executed[0][1] is None                 # first page: no keyset bound
    assert conn.executed[1][1] == {"last": 10}         # advanced by page-1 last_pk
    assert conn.executed[2][1] == {"last": 20}         # advanced by page-2 last_pk
    assert "WHERE" not in conn.executed[0][0]
    assert "WHERE" in conn.executed[1][0]


def test_source_checksum_keyset_accumulates_subsums_and_advances_keyset() -> None:
    conn = _PagedSourceConn([(100, 10, 3), (200, 20, 3), (50, 25, 2)])
    total = _source_checksum_keyset(conn, _table("orders"), "id", 3)
    assert total == "350"
    assert len(conn.executed) == 3
    assert conn.executed[0][1] == {"page": 3}
    assert conn.executed[1][1] == {"last": 10, "page": 3}
    assert conn.executed[2][1] == {"last": 20, "page": 3}


def test_page_checksum_empty_table_is_zero() -> None:
    # A COALESCE(SUM,0)=0 first page with count 0 terminates at "0" (matches the
    # whole-table empty-table checksum), and a truly empty result set does too.
    assert _target_checksum_keyset(_PagedTargetConn([(0, None, 0)]), _table("t"), "id", 3) == "0"
    assert _target_checksum_keyset(_PagedTargetConn([]), _table("t"), "id", 3) == "0"
    assert _source_checksum_keyset(_PagedSourceConn([(0, None, 0)]), _table("t"), "id", 3) == "0"


def test_checksum_composite_pk_falls_back_to_single_scan() -> None:
    # A composite/missing PK has no single keyset column, so both sides keep the
    # whole-table single scan (never a paged SUM(page_tok) statement).
    composite = _table("t", columns=("a", "b"), primary_key=("a", "b"))
    target = _FakeTargetConnection(counts={"t": 1}, checksums={"t": "42"})
    source = _FakeSourceConnection(counts={"t": 1}, checksums={"t": "42"})
    assert _target_checksum(target, composite, 3) == "42"
    assert _source_checksum(source, composite, 3) == "42"
    assert not any("SUM(page_tok)" in s for s in target.executed)
    assert not any("SUM(PAGE_TOK)" in s.upper() for s in source.executed)


def test_checksum_is_paged_end_to_end_over_multiple_pages() -> None:
    # A 5-row table with page size 2 pages the checksum (3 statements per side),
    # NEVER a single unbounded scan, and still matches when the data is equal.
    source = _FakeSourceConnection(counts={"orders": 5}, checksums={"orders": "777"})
    target = _FakeTargetConnection(counts={"orders": 5}, checksums={"orders": "777"})
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )
    item = report.items[0]
    assert item.checksum_match is True
    assert item.source_checksum == "777" and item.target_checksum == "777"
    # Paged, not a single whole-table SELECT SUM(...) scan (would be 1 statement).
    assert sum("SUM(page_tok)" in s for s in target.executed) == 3
    assert sum("SUM(PAGE_TOK)" in s.upper() for s in source.executed) == 3
    # The whole-table single-scan checksum SQL is NOT issued for a single-PK table.
    assert not any(build_pg_checksum_sql(_table("orders")).as_string(None) == s
                   for s in target.executed)


# ---------------------------------------------------------------------------
# Dev row-level diff sample (row_diff_sample_size > 0)
# ---------------------------------------------------------------------------


def _validator_diff(source, target, sample_size: int) -> Validator:
    engine = _FakeSourceEngine(source)
    return Validator(
        source_engine_factory=lambda _conn: engine,
        target_connection_factory=lambda _target: target,
        row_diff_sample_size=sample_size,
    )


def test_row_diff_classifies_value_mismatch_missing_and_extra() -> None:
    # Counts agree but checksums differ -> table mismatches -> the diff names PKs.
    # source has pk 1 (tok A), 2 (tok B2), 3 (tok C); target has 1 (tok A),
    # 2 (tok B_DIFF), 4 (tok D). So: 2=VALUE_MISMATCH, 3=MISSING_ON_TARGET,
    # 4=EXTRA_ON_TARGET; 1 matches and is not reported.
    source = _FakeSourceConnection(
        counts={"orders": 3}, checksums={"orders": "111"},
        pk_tokens={"orders": [("1", "A"), ("2", "B"), ("3", "C")]},
    )
    target = _FakeTargetConnection(
        counts={"orders": 3}, checksums={"orders": "222"},
        pk_tokens={"orders": [("1", "A"), ("2", "Bdiff"), ("4", "D")]},
    )
    report = _validator_diff(source, target, 100).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )
    sample = report.items[0].row_diff_sample
    assert sample is not None
    assert sample.pk_column == "id"
    kinds = {f.pk: f.kind.value for f in sample.findings}
    assert kinds == {
        "2": "VALUE_MISMATCH",
        "3": "MISSING_ON_TARGET",
        "4": "EXTRA_ON_TARGET",
    }
    # PII guard: only PK + checksum tokens are carried, never row VALUES.
    for f in sample.findings:
        assert set(f.model_dump()) == {"pk", "kind", "source_checksum", "target_checksum"}


def test_row_diff_off_by_default_issues_no_pk_token_query() -> None:
    # Default sample_size=0 -> no diff, and crucially NO pk-token SELECT issued
    # (hot-path-clean / default-off).
    source = _FakeSourceConnection(counts={"orders": 3}, checksums={"orders": "111"})
    target = _FakeTargetConnection(counts={"orders": 3}, checksums={"orders": "222"})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )
    assert report.items[0].row_diff_sample is None
    assert not any("AS tok" in s for s in source.executed)
    assert not any("AS tok" in s for s in target.executed)


def test_row_diff_skipped_for_matched_table() -> None:
    # A matched table never triggers a diff query even when the sample size > 0.
    source = _FakeSourceConnection(counts={"orders": 2}, checksums={"orders": "7"})
    target = _FakeTargetConnection(counts={"orders": 2}, checksums={"orders": "7"})
    report = _validator_diff(source, target, 50).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )
    assert report.items[0].matched is True
    assert report.items[0].row_diff_sample is None
    assert not any("AS tok" in s for s in source.executed)


def test_row_diff_query_is_bounded_by_limit_and_truncates() -> None:
    # Every pk-token statement carries a LIMIT; findings are capped at sample_size
    # and truncated=True when the window is full.
    source = _FakeSourceConnection(
        counts={"orders": 5}, checksums={"orders": "111"},
        pk_tokens={"orders": [(str(i), f"s{i}") for i in range(1, 6)]},
    )
    target = _FakeTargetConnection(
        counts={"orders": 5}, checksums={"orders": "222"},
        pk_tokens={"orders": [(str(i), f"t{i}") for i in range(1, 6)]},  # all differ
    )
    report = _validator_diff(source, target, 2).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )
    sample = report.items[0].row_diff_sample
    assert sample is not None
    assert len(sample.findings) == 2          # capped at sample_size
    assert sample.truncated is True
    assert any("LIMIT" in s.upper() for s in source.executed if "AS TOK" in s.upper())


def test_row_diff_skipped_for_composite_pk() -> None:
    # Composite PK -> diff returns None (skipped, never scanned).
    source = _FakeSourceConnection(counts={"t": 1}, checksums={"t": "1"})
    target = _FakeTargetConnection(counts={"t": 1}, checksums={"t": "2"})
    report = _validator_diff(source, target, 10).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG,
        [_table("t", columns=("a", "b"), primary_key=("a", "b"))],
        ValidationMode.CHECKSUM,
    )
    assert report.items[0].matched is False
    assert report.items[0].row_diff_sample is None


def test_validation_soundness_invariant_over_mixed_tables() -> None:
    """When is_match is True, every table's counts AND checksums are truly equal."""
    source = _FakeSourceConnection(
        counts={"orders": 2, "items": 9},
        checksums={"orders": "171", "items": "456"},
    )
    target = _FakeTargetConnection(
        counts={"orders": 2, "items": 9},
        checksums={"orders": "171", "items": "456"},
    )

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_table("orders"), _table("items")],
        ValidationMode.CHECKSUM,
    )

    assert report.is_match is True
    for item in report.items:
        assert item.source_row_count == item.target_row_count
        assert item.source_checksum == item.target_checksum


def test_row_count_mode_does_not_compute_checksums() -> None:
    source = _FakeSourceConnection(counts={"orders": 1})
    target = _FakeTargetConnection(counts={"orders": 1})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.ROW_COUNT
    )

    item = report.items[0]
    assert item.checksum_match is None
    assert item.source_checksum is None
    assert item.target_checksum is None
    assert item.matched is True


# ---------------------------------------------------------------------------
# Orphan-record check (Requirement 6.3)
# ---------------------------------------------------------------------------


def _orders_with_fk() -> TableDef:
    return _table(
        "orders",
        columns=("id", "customer_id"),
        primary_key=("id",),
        foreign_keys=[
            ForeignKeyDef(
                name="fk_orders_customer",
                columns=["customer_id"],
                referenced_table="customers",
                referenced_columns=["id"],
            )
        ],
    )


def test_orphan_records_are_detected_and_fail_the_match() -> None:
    source = _FakeSourceConnection(counts={"orders": 4})
    target = _FakeTargetConnection(counts={"orders": 4}, orphans={"orders": 2})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_orders_with_fk()],
        check_orphans=True,
    )

    assert report.orphan_check_performed is True
    assert len(report.orphan_findings) == 1
    finding = report.orphan_findings[0]
    assert finding.table == "orders"
    assert finding.foreign_key == "fk_orders_customer"
    assert finding.referenced_table == "customers"
    assert finding.orphan_count == 2
    # Row counts agree, but orphans make the overall result a non-match.
    assert report.items[0].matched is True
    assert report.is_match is False


def test_no_orphans_produces_clean_report() -> None:
    source = _FakeSourceConnection(counts={"orders": 4})
    target = _FakeTargetConnection(counts={"orders": 4}, orphans={"orders": 0})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_orders_with_fk()],
        check_orphans=True,
    )

    assert report.orphan_check_performed is True
    assert report.orphan_findings == []
    assert report.is_match is True


def test_orphans_not_checked_when_flag_is_off() -> None:
    source = _FakeSourceConnection(counts={"orders": 4})
    target = _FakeTargetConnection(counts={"orders": 4}, orphans={"orders": 5})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_orders_with_fk()]
    )

    assert report.orphan_check_performed is False
    assert report.orphan_findings == []
    assert report.is_match is True
    # No orphan query was issued against the target.
    assert all("NOT EXISTS" not in text for text in target.executed)


def test_orphan_count_single_int_pk_child_is_keyset_paged() -> None:
    # FIX 2: a single-column-integer-PK child is orphan-counted over BOUNDED keyset pages
    # (multi-page here via a tiny page size), not one unbounded scan -- and the accumulated
    # total equals the single-scan orphan count.
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _FakeTargetConnection(counts={"orders": 5}, orphans={"orders": 2})
    validator = Validator(
        source_engine_factory=lambda _c: _FakeSourceEngine(source),
        target_connection_factory=lambda _t: target,
        reconcile_page_size=2,  # 5 PKs / page 2 -> forces multiple orphan pages
    )
    report = validator.validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_orders_with_fk()], check_orphans=True
    )
    assert report.orphan_findings[0].orphan_count == 2  # same total as the single scan
    orphan_sql = [t for t in target.executed if "NOT EXISTS" in t]
    assert orphan_sql, "no orphan query ran"
    # The PAGED form: a keyset window folded into COUNT(*) FILTER + array_agg, NOT the
    # single unbounded "... AS c WHERE ..." scan.
    assert all("FILTER" in t and "array_agg" in t for t in orphan_sql)
    assert all(" AS c WHERE" not in t for t in orphan_sql)
    # Multi-page: page size 2 over 5 PKs -> 3 orphan pages ran.
    assert len(orphan_sql) >= 2


def test_orphan_count_composite_pk_child_uses_single_scan() -> None:
    # FIX 2: a composite-PK child has no single keyset column, so the orphan pre-gate
    # falls back to the single unbounded scan (exactly as count/checksum do).
    child = _table(
        "order_items",
        columns=("order_id", "line_no", "sku_id"),
        primary_key=("order_id", "line_no"),  # composite PK
        foreign_keys=[
            ForeignKeyDef(
                name="fk_sku",
                columns=["sku_id"],
                referenced_table="skus",
                referenced_columns=["id"],
            )
        ],
    )
    source = _FakeSourceConnection(counts={"order_items": 4})
    target = _FakeTargetConnection(counts={"order_items": 4}, orphans={"order_items": 1})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [child], check_orphans=True
    )
    assert report.orphan_findings[0].orphan_count == 1
    orphan_sql = [t for t in target.executed if "NOT EXISTS" in t]
    assert orphan_sql, "no orphan query ran"
    # The single-scan form: "... AS c WHERE ...", NOT the paged FILTER/array_agg.
    assert all("array_agg" not in t and "FILTER" not in t for t in orphan_sql)
    assert all(" AS c WHERE" in t for t in orphan_sql)


def test_orphan_page_sql_pages_the_window_unconditionally() -> None:
    # FIX 2 correctness: the paged orphan SQL selects the FULL PK window (so the keyset
    # boundary advances over NON-orphan rows too) and folds the orphan predicate into a
    # COUNT(*) FILTER -- if the WHERE dropped non-orphans, a page's max PK could skip a
    # PK range and under-count.
    from dsql_migrator.core.validation_sql import (
        build_pg_orphan_page_first_sql,
        build_pg_orphan_page_next_sql,
    )

    fk = ForeignKeyDef(
        name="fk", columns=["customer_id"],
        referenced_table="customers", referenced_columns=["id"],
    )
    first = build_pg_orphan_page_first_sql("orders", fk, "id", 5000).as_string(None)
    nxt = build_pg_orphan_page_next_sql("orders", fk, "id", 5000).as_string(None)
    # The orphan predicate is a FILTER on the count, not a WHERE on the window.
    assert "COUNT(*) FILTER (WHERE" in first
    assert 'pg."customer_id" IS NOT NULL' in first
    assert 'NOT EXISTS' in first and '"customers" AS p' in first
    # The window itself is unfiltered by the orphan predicate; the keyset advances by the
    # window's max PK via array_agg (works for any orderable PK type).
    assert '(array_agg(page_pk ORDER BY page_pk))[COUNT(*)]' in first
    assert 'FROM "orders" ORDER BY "id" LIMIT 5000' in first
    # The next page carries the keyset bound; the first page does not.
    assert '"id" > %(last)s' in nxt
    assert '"id" > ' not in first


# ---------------------------------------------------------------------------
# As-of-watermark validation + drift (Requirement 6.5 / Property 11)
# ---------------------------------------------------------------------------


def _watermark(*, gtid: Optional[str], counts: dict[str, int]) -> Watermark:
    return Watermark(
        gtid_executed=gtid,
        snapshot_timestamp=FIXED_NOW,
        table_row_counts=counts,
    )


def test_source_count_taken_from_watermark_snapshot() -> None:
    """As-of-watermark: the snapshot row count is compared, not a live re-count."""
    # The live source has drifted to 9 rows, but the snapshot recorded 5.
    source = _FakeSourceConnection(counts={"orders": 9}, gtid="uuid:1-9")
    target = _FakeTargetConnection(counts={"orders": 5})
    watermark = _watermark(gtid="uuid:1-5", counts={"orders": 5})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_table("orders")],
        watermark=watermark,
    )

    item = report.items[0]
    assert item.source_row_count == 5  # from the watermark snapshot, not live (9)
    assert item.target_row_count == 5
    assert item.matched is True
    assert report.is_match is True
    assert report.snapshot_timestamp == FIXED_NOW


def test_drift_since_snapshot_is_reported() -> None:
    source = _FakeSourceConnection(counts={"orders": 9}, gtid="uuid:1-9")
    target = _FakeTargetConnection(counts={"orders": 5})
    watermark = _watermark(gtid="uuid:1-5", counts={"orders": 5})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_table("orders")],
        watermark=watermark,
    )

    assert report.drift is not None
    assert report.drift.watermark_gtid == "uuid:1-5"
    assert report.drift.current_gtid == "uuid:1-9"
    assert report.drift.drifted is True


def test_no_drift_when_gtid_unchanged() -> None:
    source = _FakeSourceConnection(counts={"orders": 5}, gtid="uuid:1-5")
    target = _FakeTargetConnection(counts={"orders": 5})
    watermark = _watermark(gtid="uuid:1-5", counts={"orders": 5})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_table("orders")],
        watermark=watermark,
    )

    assert report.drift is not None
    assert report.drift.drifted is False


def test_drift_is_none_without_watermark() -> None:
    source = _FakeSourceConnection(counts={"orders": 1})
    target = _FakeTargetConnection(counts={"orders": 1})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]
    )

    assert report.drift is None
    assert report.snapshot_timestamp is None


def test_drift_undetermined_when_current_gtid_unavailable() -> None:
    source = _FakeSourceConnection(counts={"orders": 5}, gtid=None)
    target = _FakeTargetConnection(counts={"orders": 5})
    watermark = _watermark(gtid="uuid:1-5", counts={"orders": 5})

    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_table("orders")],
        watermark=watermark,
    )

    assert report.drift is not None
    assert report.drift.drifted is False
    assert "could not be determined" in report.drift.detail


# ---------------------------------------------------------------------------
# Read-only source (Property 1)
# ---------------------------------------------------------------------------


def test_source_access_is_read_only() -> None:
    source = _FakeSourceConnection(
        counts={"orders": 2}, checksums={"orders": "1"}, gtid="uuid:1"
    )
    target = _FakeTargetConnection(counts={"orders": 2}, checksums={"orders": "1"})

    _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM
    )

    assert source.executed  # statements were issued
    offending = [text for text in source.executed if is_write_or_ddl(text)]
    assert offending == [], f"validator issued write/DDL on source: {offending}"


def test_source_runs_within_consistent_snapshot_transaction() -> None:
    source = _FakeSourceConnection(counts={"orders": 1})
    target = _FakeTargetConnection(counts={"orders": 1})

    _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]
    )

    joined = " ".join(source.executed).upper()
    assert "START TRANSACTION WITH CONSISTENT SNAPSHOT" in joined
    assert "COMMIT" in joined
    assert {"isolation_level": "AUTOCOMMIT"} in source.execution_options_calls


def test_source_engine_is_disposed() -> None:
    source = _FakeSourceConnection(counts={"orders": 1})
    engine = _FakeSourceEngine(source)
    target = _FakeTargetConnection(counts={"orders": 1})
    validator = Validator(
        source_engine_factory=lambda _conn: engine,
        target_connection_factory=lambda _target: target,
    )

    validator.validate(_SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")])

    assert engine.disposed is True
    assert target.closed is True


# ---------------------------------------------------------------------------
# Generated SQL: safe identifier quoting (Requirement 9.4)
# ---------------------------------------------------------------------------


def test_mysql_checksum_sql_quotes_identifiers() -> None:
    table = _table("weird table", columns=("id", "full name"))
    statement = build_mysql_checksum_sql(table)
    assert "`weird table`" in statement
    assert "`full name`" in statement
    assert "MD5(" in statement


def test_pg_checksum_sql_quotes_identifiers() -> None:
    table = _table("weird table", columns=("id", "full name"))
    rendered = build_pg_checksum_sql(table).as_string(None)
    assert '"weird table"' in rendered
    assert '"full name"' in rendered
    assert "md5(" in rendered


def test_checksum_sql_qualifies_schema_table() -> None:
    # A schema-qualified table must be split into schema + table identifiers, not
    # quoted as a single "schema.table" name (regression: "No database selected"
    # / non-existent relation).
    table = _table("customers_sample.categories", columns=("id",))
    mysql_sql = build_mysql_checksum_sql(table)
    assert "`customers_sample`.`categories`" in mysql_sql
    assert "`customers_sample.categories`" not in mysql_sql
    pg_sql = build_pg_checksum_sql(table).as_string(None)
    assert '"customers_sample"."categories"' in pg_sql


def test_pk_token_sql_is_bounded_select_only() -> None:
    from dsql_migrator.core.validator import (
        build_mysql_pk_token_sql,
        build_pg_pk_token_sql,
    )

    table = _table("weird table", columns=("id", "full name"))
    mysql_sql = build_mysql_pk_token_sql(table, "id")
    # Read-only, identifier-quoted, ordered by PK, bounded by LIMIT (no COUNT/scan).
    assert mysql_sql.strip().upper().startswith("SELECT")
    assert not is_write_or_ddl(mysql_sql)
    assert "`weird table`" in mysql_sql and "`full name`" in mysql_sql
    assert "ORDER BY `id`" in mysql_sql and "LIMIT :sample_size" in mysql_sql
    assert "COUNT(" not in mysql_sql.upper()

    pg_sql = build_pg_pk_token_sql(table, "id", 100).as_string(None)
    assert pg_sql.strip().upper().startswith("SELECT")
    assert '"weird table"' in pg_sql and '"full name"' in pg_sql
    assert 'ORDER BY "id"' in pg_sql and "LIMIT 100" in pg_sql


def test_pk_token_sql_qualifies_schema_table() -> None:
    from dsql_migrator.core.validator import (
        build_mysql_pk_token_sql,
        build_pg_pk_token_sql,
    )

    table = _table("customers_sample.categories", columns=("id",))
    assert "`customers_sample`.`categories`" in build_mysql_pk_token_sql(table, "id")
    assert '"customers_sample"."categories"' in build_pg_pk_token_sql(
        table, "id", 10).as_string(None)


# ---------------------------------------------------------------------------
# Cross-engine checksum parity: engine-normalized per-column rendering (§A.6)
# ---------------------------------------------------------------------------


def _all_four_rendered(table: TableDef):
    """Render all four builders' SQL as strings for cross-builder assertions."""
    from dsql_migrator.core.validator import (
        build_mysql_pk_token_sql,
        build_pg_pk_token_sql,
    )

    return (
        build_mysql_checksum_sql(table),
        build_pg_checksum_sql(table).as_string(None),
        build_mysql_pk_token_sql(table, "id"),
        build_pg_pk_token_sql(table, "id", 10).as_string(None),
    )


def test_checksum_null_sentinel_is_backslash_free_and_identical() -> None:
    # The old '\0' sentinel parsed to a single NUL on MySQL but the two-char
    # string 0x5C30 on PG, so a NULL-bearing row hashed differently on each
    # engine. The sentinel '~N' is backslash-free and identical text on both
    # engines, and must appear (inside COALESCE) in ALL FOUR builders. It is also
    # UN-forgeable: the per-value escape turns every '~' into '~~', so no real value
    # can produce a lone '~N' (closing the old '<NULL>'-vs-literal collision).
    for rendered in _all_four_rendered(_typed_table()):
        assert "~N" in rendered
        assert "\\0" not in rendered
    # The separator escape ('~'->'~~', '|'->'~|') must be applied on BOTH engines so
    # a value containing '|' cannot shift a delimiter across a column boundary.
    mysql_sql, pg_sql, _t1, _t2 = _all_four_rendered(_typed_table())
    assert "REPLACE(REPLACE(" in mysql_sql and "'~', '~~'" in mysql_sql
    assert "replace(replace(" in pg_sql and "'~', '~~'" in pg_sql


def test_checksum_binary_columns_render_lower_hex() -> None:
    mysql_sql, pg_sql, mysql_tok, pg_tok = _all_four_rendered(_typed_table())
    # MySQL: plain BLOB -> LOWER(HEX(col)); spatial POINT -> LOWER(HEX(ST_AsBinary(col))).
    assert "LOWER(HEX(`payload`))" in mysql_sql
    assert "LOWER(HEX(ST_AsBinary(`geo`)))" in mysql_sql
    # PG: encode(col, 'hex') for both.
    assert "encode(\"payload\", 'hex')" in pg_sql
    assert "encode(\"geo\", 'hex')" in pg_sql
    # The old native-cast rendering for a binary column must be gone.
    assert "CAST(`payload` AS CHAR)" not in mysql_sql


def test_checksum_bit_column_renders_numeric() -> None:
    mysql_sql, pg_sql, _mysql_tok, _pg_tok = _all_four_rendered(_typed_table())
    assert "CAST(`flags` AS UNSIGNED)" in mysql_sql
    assert '"flags"::text' in pg_sql


def test_checksum_boolean_column_renders_words() -> None:
    mysql_sql, pg_sql, _mysql_tok, _pg_tok = _all_four_rendered(_typed_table())
    assert "WHEN `active` = 0 THEN 'false' ELSE 'true' END" in mysql_sql
    assert '"active"::text' in pg_sql


def test_checksum_boolean_null_routes_to_shared_sentinel() -> None:
    """A NULL boolean must render as SQL NULL on the MySQL side (-> the shared '~N'
    sentinel), matching PG's NULL boolean. Without the IS NULL guard, MySQL's
    `NULL = 0` is UNKNOWN and the CASE falls to ELSE -> 'true', which both
    false-mismatches a correctly-migrated NULL->NULL row and false-matches a
    source-NULL vs target-TRUE row (a soundness hole in CHECKSUM mode).
    """
    from dsql_migrator.core.validator import _mysql_checksum_expr

    expr = _mysql_checksum_expr(ColumnDef(name="active", mysql_type="TINYINT(1)"))
    assert "`active` IS NULL THEN NULL" in expr  # NULL -> NULL -> COALESCE sentinel
    assert "WHEN `active` = 0 THEN 'false' ELSE 'true' END" in expr


def test_checksum_kind_prefers_applied_target_type() -> None:
    """M1: when the APPLIED DSQL target type is known, the checksum render family comes
    from it (honoring a Schema-Conversion remap), not the default source-derived mapping.
    A TINYINT(1) the operator kept as smallint must render as a plain integer on both
    sides ('0'/'1'/'2'), not the default boolean ('true'/'false') that would false-mismatch
    every row. The source-based spatial/BIT cases still take precedence over the applied
    type (they need ST_AsBinary / CAST AS UNSIGNED regardless of the bytea/int target)."""
    from dsql_migrator.core.validator import _checksum_kind, _mysql_checksum_expr

    # Default (no applied type): TINYINT(1) -> boolean render.
    default_col = ColumnDef(name="active", mysql_type="TINYINT(1)")
    assert _checksum_kind(default_col) == "boolean"

    # Applied remap TINYINT(1) -> smallint: render as a plain integer, not boolean.
    remapped = ColumnDef(name="active", mysql_type="TINYINT(1)", target_type="smallint")
    assert _checksum_kind(remapped) == "plain"
    assert _mysql_checksum_expr(remapped) == "CAST(`active` AS CHAR)"

    # Source-based special cases still win over the applied type.
    assert _checksum_kind(ColumnDef(name="f", mysql_type="BIT(8)", target_type="smallint")) == "bit"
    assert _checksum_kind(ColumnDef(name="g", mysql_type="POINT", target_type="bytea")) == "binary"

    # An applied numeric / timestamptz classifies by the applied type.
    assert _checksum_kind(ColumnDef(name="n", mysql_type="int", target_type="numeric(20, 0)")) == "numeric"
    assert _checksum_kind(ColumnDef(name="t", mysql_type="datetime", target_type="timestamptz")) == "timestamptz"


def test_checksum_zerofill_int_renders_plain_numeric() -> None:
    """A MySQL INT ZEROFILL migrates to a plain integer, so the checksum must render its
    numeric value ('42'), not the zero-padded display form ('00042') that
    CAST(col AS CHAR) emits -- otherwise the source ('00042') false-mismatches the
    target integer ('42'). Arithmetic (col + 0) drops the display attribute.
    """
    from dsql_migrator.core.validator import _mysql_checksum_expr

    zf = ColumnDef(name="n", mysql_type="int(5) unsigned zerofill")
    assert _mysql_checksum_expr(zf) == "CAST(`n` + 0 AS CHAR)"
    # A plain (non-zerofill) integer is unaffected.
    assert _mysql_checksum_expr(ColumnDef(name="n", mysql_type="int")) == "CAST(`n` AS CHAR)"


def test_checksum_temporal_columns_fixed_fraction_no_zone() -> None:
    mysql_sql, pg_sql, _mysql_tok, _pg_tok = _all_four_rendered(_typed_table())
    # MySQL: fixed 6-digit fraction, no zone.
    assert "DATE_FORMAT(`created_at`, '%Y-%m-%d %H:%i:%s.%f')" in mysql_sql
    assert "DATE_FORMAT(`t_of_day`, '%H:%i:%s.%f')" in mysql_sql
    # PG: a plain timestamp (DATETIME -> created_at) renders DIRECTLY -- AT TIME ZONE
    # 'UTC' is NOT a no-op on `timestamp without time zone` (it converts through the
    # session TimeZone and shifts the wall-clock), so it must NOT be applied here.
    assert "to_char(\"created_at\", 'YYYY-MM-DD HH24:MI:SS.US')" in pg_sql
    assert "\"created_at\" AT TIME ZONE" not in pg_sql
    # timestamptz (TIMESTAMP -> ts) DOES use AT TIME ZONE 'UTC' to match the UTC instant.
    assert "to_char(\"ts\" AT TIME ZONE 'UTC'" in pg_sql
    assert "to_char(\"t_of_day\", 'HH24:MI:SS.US')" in pg_sql


def test_checksum_decimal_fixed_scale() -> None:
    mysql_sql, pg_sql, _mysql_tok, _pg_tok = _all_four_rendered(_typed_table())
    # Both sides pin the declared scale (4) so a stored-scale / trailing-zero
    # difference cannot diverge; MySQL CAST(... AS DECIMAL(65, 4)) and PG
    # round(col, 4) both emit e.g. 1.5000 / 0.0000 (verified against PG 16).
    assert "CAST(`amount` AS DECIMAL(65, 4))" in mysql_sql
    assert 'round("amount", 4)' in pg_sql
    assert "4" in pg_sql


def test_numeric_mask_covers_full_decimal65_range() -> None:
    # Regression: the mask integer run was 18 digits, but the MySQL side casts to
    # DECIMAL(65, scale) and BIGINT UNSIGNED is stored as numeric(20, 0). A value
    # like 18446744073709551615 (20 digits) overflowed the mask, making to_char
    # emit '#' padding instead of the digits -> a spurious checksum MISMATCH on
    # byte-identical data. The mask must span the full 65-digit integer range.
    from dsql_migrator.core.validator import _pg_numeric_mask

    mask = _pg_numeric_mask(0)
    integer_positions = sum(ch in "90" for ch in mask)
    assert integer_positions >= 65
    # BIGINT UNSIGNED max (20 digits) and DECIMAL(65,0) both fit.
    assert integer_positions >= len("18446744073709551615")

    scaled = _pg_numeric_mask(4)
    # Fixed 4-digit fraction preserved, integer run unchanged. The separator is a
    # LITERAL '.' (not the locale-aware 'D'), so the numeric facet is GUC-independent.
    assert scaled.endswith(".0000")
    assert "D" not in scaled  # no locale-aware decimal-point template
    assert sum(ch == "9" for ch in scaled.split(".")[0]) >= 64


def test_checksum_float_columns_excluded() -> None:
    # FLOAT/DOUBLE have no byte-identical cross-engine text form, so they are
    # omitted from the concatenation entirely on BOTH engines and in all four
    # builders (the DOUBLE column 'ratio' must never appear).
    for rendered in _all_four_rendered(_typed_table()):
        assert "ratio" not in rendered


def test_checksum_json_columns_excluded() -> None:
    # JSON has no byte-identical cross-engine text form: MySQL CAST(col AS CHAR)
    # emits a SPACED canonical form ({"k": "v"}), while a CDC-written row holds
    # Debezium's COMPACT serialization ({"k":"v"}) in the PG `json` column. Equal
    # data, different text -> a checksum false positive. So -- like FLOAT/DOUBLE --
    # JSON is omitted from the checksum on BOTH engines and in all four builders;
    # the 'meta' column must never appear (the int PK 'id' still anchors the concat).
    table = TableDef(
        name="j",
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="meta", mysql_type="JSON"),
        ],
        primary_key=["id"],
    )
    for rendered in _all_four_rendered(table):
        assert "meta" not in rendered
        assert "id" in rendered  # non-JSON PK still rendered, so the concat is valid


def test_pk_token_matches_checksum_per_column_terms() -> None:
    # Requirement 1: the PK-token per-row hash must reproduce the table-checksum
    # per-row hash, so the SAME per-column inner terms appear in both.
    mysql_sql, pg_sql, mysql_tok, pg_tok = _all_four_rendered(_typed_table())
    for term in (
        "LOWER(HEX(`payload`))",
        "LOWER(HEX(ST_AsBinary(`geo`)))",
        "CAST(`flags` AS UNSIGNED)",
        "WHEN `active` = 0 THEN 'false' ELSE 'true' END",
        "DATE_FORMAT(`created_at`, '%Y-%m-%d %H:%i:%s.%f')",
        "CAST(`amount` AS DECIMAL(65, 4))",
    ):
        assert term in mysql_sql and term in mysql_tok
    for term in (
        "encode(\"payload\", 'hex')",
        "\"active\"::text",
        "to_char(\"t_of_day\", 'HH24:MI:SS.US')",
        'round("amount", 4)',
    ):
        assert term in pg_sql and term in pg_tok


def test_all_float_table_renders_constant_term() -> None:
    from dsql_migrator.core.validator import (
        build_mysql_pk_token_sql,
        build_pg_pk_token_sql,
    )

    # An int PK + a single DOUBLE column: the DOUBLE is excluded, but the PK 'id'
    # (a plain int) is still rendered, so the concat has at least one argument.
    table = TableDef(
        name="floats",
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="ratio", mysql_type="DOUBLE"),
        ],
        primary_key=["id"],
    )
    for rendered in (
        build_mysql_checksum_sql(table),
        build_pg_checksum_sql(table).as_string(None),
        build_mysql_pk_token_sql(table, "id"),
        build_pg_pk_token_sql(table, "id", 10).as_string(None),
    ):
        assert rendered.strip().upper().startswith("SELECT")
        assert not is_write_or_ddl(rendered)
        assert "ratio" not in rendered
        # CONCAT_WS/concat_ws must have at least one argument (not zero args).
        assert "CONCAT_WS('|', )" not in rendered
        assert "concat_ws('|', )" not in rendered

    # A table with ONLY a float column (no non-float PK) falls back to the
    # sentinel constant so both engines still hash to the same valid SQL.
    only_float = TableDef(
        name="only_float",
        columns=[ColumnDef(name="ratio", mysql_type="DOUBLE")],
        primary_key=["ratio"],
    )
    mysql_only = build_mysql_checksum_sql(only_float)
    pg_only = build_pg_checksum_sql(only_float).as_string(None)
    assert "CONCAT_WS('|', '~N')" in mysql_only
    assert "concat_ws('|', '~N')" in pg_only


def test_orphan_count_sql_quotes_identifiers_and_joins_keys() -> None:
    fk = ForeignKeyDef(
        name="fk",
        columns=["customer_id"],
        referenced_table="customers",
        referenced_columns=["id"],
    )
    rendered = build_orphan_count_sql("orders", fk).as_string(None)
    assert '"orders" AS c' in rendered
    assert '"customers" AS p' in rendered
    assert 'c."customer_id" IS NOT NULL' in rendered
    assert 'p."id" = c."customer_id"' in rendered
    assert "NOT EXISTS" in rendered


def test_orphan_count_sql_splits_schema_qualified_names() -> None:
    # Regression: a schema-qualified name must render as "schema"."table", NOT one
    # quoted "schema.table" identifier (which is a relation that does not exist ->
    # UndefinedTable). Both child and parent are split.
    fk = ForeignKeyDef(
        name="fk",
        columns=["category_id"],
        referenced_table="customers_sample_new.categories",
        referenced_columns=["id"],
    )
    rendered = build_orphan_count_sql(
        "customers_sample_new.products", fk
    ).as_string(None)
    assert '"customers_sample_new"."products" AS c' in rendered
    assert '"customers_sample_new"."categories" AS p' in rendered
    # The buggy single-identifier form must NOT appear.
    assert '"customers_sample_new.products"' not in rendered
    assert '"customers_sample_new.categories"' not in rendered


def test_render_text_report_match_label_is_mode_aware() -> None:
    # FIX 3(1): the downloadable text report used to print "Data identical" in EVERY mode.
    # ROW_COUNT never reads non-PK column VALUES, so it must say "Row counts match"; only
    # CHECKSUM (which value-compares) earns "Data identical".
    from dsql_migrator.core.validator import render_text_report

    rc = _validator(
        _FakeSourceConnection(counts={"orders": 2}),
        _FakeTargetConnection(counts={"orders": 2}),
    ).validate(_SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.ROW_COUNT)
    rc_text = render_text_report(rc)
    assert "Row counts match: yes" in rc_text
    assert "Data identical" not in rc_text

    cs = _validator(
        _FakeSourceConnection(counts={"orders": 2}, checksums={"orders": "9"}),
        _FakeTargetConnection(counts={"orders": 2}, checksums={"orders": "9"}),
    ).validate(_SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], ValidationMode.CHECKSUM)
    cs_text = render_text_report(cs)
    assert "Data identical: yes" in cs_text
    assert "Row counts match" not in cs_text


def test_render_text_report_footnotes_reconcile_skipped_and_inapplicable() -> None:
    # FIX 3(2): footnote the composite/non-integer-PK tables that ran but were not
    # reconciled, and -- when reconciliation was requested yet NO table was eligible --
    # do not present "no missing or extra records" as a clean pass.
    from dsql_migrator.core.models import (
        ReconcileResult,
        TableValidationResult,
        ValidationReport,
    )
    from dsql_migrator.core.validator import render_text_report

    reconciled = TableValidationResult(
        table="orders", source_row_count=2, target_row_count=2,
        row_count_match=True, matched=True, reconcile_applicable=True,
        reconcile=ReconcileResult(
            pk_column="id", source_count=2, target_count=2, consistent=True
        ),
    )
    skipped = TableValidationResult(
        table="audit_log", source_row_count=1, target_row_count=1,
        row_count_match=True, matched=True, reconcile_applicable=False,
    )
    mixed = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT, items=[reconciled, skipped],
        reconcile_requested=True,
    )
    mixed_text = render_text_report(mixed)
    assert "No missing or extra records: yes" in mixed_text
    assert "not record-reconciled" in mixed_text
    assert "audit_log" in mixed_text

    # Requested but NO table eligible -> not a clean pass.
    none_eligible = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="t1", source_row_count=1, target_row_count=1,
                row_count_match=True, matched=True, reconcile_applicable=False,
            )
        ],
        reconcile_requested=True,
    )
    txt = render_text_report(none_eligible)
    assert "NOT verified" in txt
    assert "could not run for any table" in txt

    # Reconciliation genuinely off -> the plain "not checked (reconciliation off)" line.
    off = ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="t1", source_row_count=1, target_row_count=1,
                row_count_match=True, matched=True,
            )
        ],
    )
    assert "not checked (reconciliation off)" in render_text_report(off)


# ---------------------------------------------------------------------------
# Full PK-set reconciliation: pure merge (reconcile_pk_streams)
# ---------------------------------------------------------------------------


def test_reconcile_merge_finds_missing_and_extra() -> None:
    from dsql_migrator.core.validator import reconcile_pk_streams

    # source 1,2,3,5 ; target 1,3,4,5 -> missing 2 (source-only), extra 4 (target-only)
    result = reconcile_pk_streams("id", iter([1, 2, 3, 5]), iter([1, 3, 4, 5]))
    assert result.source_count == 4
    assert result.target_count == 4
    assert result.missing_on_target == 1
    assert result.extra_on_target == 1
    assert result.missing_sample == ["2"]
    assert result.extra_sample == ["4"]
    assert result.consistent is False


def test_reconcile_merge_clean_when_identical() -> None:
    from dsql_migrator.core.validator import reconcile_pk_streams

    result = reconcile_pk_streams("id", iter([1, 2, 3]), iter([1, 2, 3]))
    assert result.missing_on_target == 0
    assert result.extra_on_target == 0
    assert result.consistent is True
    assert result.sample_truncated is False


def test_reconcile_merge_caps_sample_but_counts_all() -> None:
    from dsql_migrator.core.validator import reconcile_pk_streams

    # 10 source-only PKs, sample capped at 3: count is exact, sample truncated.
    result = reconcile_pk_streams(
        "id", iter(range(1, 11)), iter([]), sample_cap=3
    )
    assert result.missing_on_target == 10
    assert result.missing_sample == ["1", "2", "3"]
    assert result.sample_truncated is True
    assert result.consistent is False


def test_reconcile_merge_cancels_mid_stream() -> None:
    # A cancel during reconciliation (the long inner loop) raises promptly instead
    # of draining the whole stream. Use a check that fires after a few rows.
    from dsql_migrator.core.validator import reconcile_pk_streams

    pulled = {"n": 0}

    def big_source():
        # A large source-only stream; count how far the merge consumes it.
        i = 0
        while True:
            i += 1
            pulled["n"] = i
            yield i

    # Poll fires True on the first check (every _RECONCILE_CANCEL_POLL_EVERY rows).
    with pytest.raises(ValidationCancelled):
        reconcile_pk_streams(
            "id", big_source(), iter([]), should_cancel=lambda: True
        )
    # It stopped early -- nowhere near exhausting an unbounded generator.
    assert pulled["n"] < 100_000


def test_reconcile_merge_not_cancelled_completes_normally() -> None:
    # A never-firing cancel behaves exactly like no cancel.
    from dsql_migrator.core.validator import reconcile_pk_streams

    result = reconcile_pk_streams(
        "id", iter([1, 2, 3]), iter([1, 2, 3]), should_cancel=lambda: False
    )
    assert result.consistent is True


def test_validate_cancels_during_a_single_large_table() -> None:
    # Cancel is requested while ONE big table is being reconciled (not at a table
    # boundary) -> the run still stops via the in-merge cancel poll.
    pks = list(range(1, 200_001))  # 200k rows: enough to cross the poll interval
    source = _FakeSourceConnection(
        counts={"orders": len(pks)}, pk_sets={"orders": pks}
    )
    target = _FakeTargetConnection(
        counts={"orders": len(pks)}, pk_sets={"orders": pks}
    )
    with pytest.raises(ValidationCancelled):
        _reconciling_validator(source, target, page_size=10_000).validate(
            _SOURCE_CONFIG,
            _TARGET_CONFIG,
            [_table("orders")],
            reconcile=True,
            should_cancel=lambda: True,
        )


# ---------------------------------------------------------------------------
# Integer-PK eligibility gate
# ---------------------------------------------------------------------------


def test_integer_pk_column_accepts_integer_types_and_modifiers() -> None:
    from dsql_migrator.core.validator import integer_pk_column

    table = TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="bigint(20) unsigned"),
            ColumnDef(name="note", mysql_type="varchar(20)"),
        ],
        primary_key=["id"],
    )
    assert integer_pk_column(table) == "id"


def test_integer_pk_column_rejects_non_integer_and_composite() -> None:
    from dsql_migrator.core.validator import integer_pk_column

    non_int = TableDef(
        name="t",
        columns=[ColumnDef(name="code", mysql_type="varchar(10)")],
        primary_key=["code"],
    )
    composite = TableDef(
        name="t",
        columns=[
            ColumnDef(name="a", mysql_type="int"),
            ColumnDef(name="b", mysql_type="int"),
        ],
        primary_key=["a", "b"],
    )
    assert integer_pk_column(non_int) is None
    assert integer_pk_column(composite) is None


# ---------------------------------------------------------------------------
# Reconciliation wired through Validator.validate (streaming, multi-page)
# ---------------------------------------------------------------------------


def _reconciling_validator(
    source_connection: _FakeSourceConnection,
    target_connection: _FakeTargetConnection,
    *,
    page_size: int = 2,
) -> Validator:
    """A validator with reconciliation streaming a tiny page so paging is exercised."""
    engine = _FakeSourceEngine(source_connection)
    return Validator(
        source_engine_factory=lambda _conn: engine,
        target_connection_factory=lambda _target: target_connection,
        reconcile_page_size=page_size,
    )


def test_reconcile_detects_missing_and_extra_over_multiple_pages() -> None:
    # source PKs 1..5 ; target PKs 1,2,4,5,6 -> missing 3 (lost), extra 6 (stale).
    source = _FakeSourceConnection(
        counts={"orders": 5}, pk_sets={"orders": [1, 2, 3, 4, 5]}
    )
    target = _FakeTargetConnection(
        counts={"orders": 5}, pk_sets={"orders": [1, 2, 4, 5, 6]}
    )
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=True
    )
    item = report.items[0]
    assert item.reconcile is not None
    assert item.reconcile.missing_on_target == 1
    assert item.reconcile.extra_on_target == 1
    assert item.reconcile.missing_sample == ["3"]
    assert item.reconcile.extra_sample == ["6"]
    assert item.matched is False
    assert report.is_match is False


def test_fast_sweep_skips_reconcile_when_counts_match() -> None:
    # Fast sweep trusts a count match: when counts are equal it does NOT stream PKs,
    # so reconcile is None (not run). This is the documented speed/coverage tradeoff
    # -- equal-count-but-different-PK divergence is only caught by a full run.
    source = _FakeSourceConnection(
        counts={"orders": 5}, pk_sets={"orders": [1, 2, 3, 4, 5]}
    )
    target = _FakeTargetConnection(
        counts={"orders": 5}, pk_sets={"orders": [1, 2, 4, 5, 6]}  # 3 missing, 6 extra
    )
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")],
        reconcile=True, deep_only_on_count_mismatch=True,
    )
    item = report.items[0]
    assert item.reconcile is None  # skipped because counts matched
    assert item.matched is True  # verified by count only (honest given fast sweep)


def test_reconcile_clean_table_matches() -> None:
    source = _FakeSourceConnection(
        counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]}
    )
    target = _FakeTargetConnection(
        counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]}
    )
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=True
    )
    item = report.items[0]
    assert item.reconcile is not None
    assert item.reconcile.consistent is True
    assert item.matched is True
    assert report.is_match is True


def test_reconcile_folds_into_match_even_when_counts_agree() -> None:
    # Equal counts (one missing balanced by one extra) but DIFFERENT rows: the
    # count check passes yet reconciliation must fail the table (Property 9).
    source = _FakeSourceConnection(
        counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]}
    )
    target = _FakeTargetConnection(
        counts={"orders": 3}, pk_sets={"orders": [1, 2, 9]}
    )
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=True
    )
    item = report.items[0]
    assert item.row_count_match is True  # counts agree...
    assert item.reconcile.missing_on_target == 1  # ...but row 3 is missing...
    assert item.reconcile.extra_on_target == 1  # ...and row 9 is extra
    assert item.matched is False
    assert report.is_match is False


def test_reconcile_skipped_for_non_integer_pk_falls_back_to_count() -> None:
    # A varchar PK is not eligible: reconcile stays None, the table is compared by
    # count only and no keyset PK page is issued.
    source = _FakeSourceConnection(counts={"t": 2})
    target = _FakeTargetConnection(counts={"t": 2})
    table = _table("t", columns=("code",), primary_key=("code",))
    table.columns[0].mysql_type = "varchar(10)"
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [table], reconcile=True
    )
    assert report.items[0].reconcile is None
    assert report.items[0].matched is True
    assert not any("AS PK" in s.upper() and "LIMIT :PAGE" in s.upper()
                   for s in source.executed)


def test_reconcile_off_issues_no_keyset_page() -> None:
    source = _FakeSourceConnection(counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]})
    target = _FakeTargetConnection(counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]})
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=False
    )
    assert report.items[0].reconcile is None
    # The SOURCE is never keyset-paged (its count comes from the watermark/COUNT(*)).
    # (The TARGET is keyset-counted now -- see the H1 tests below.)
    assert not any("LIMIT :PAGE" in s.upper() for s in source.executed)


def test_target_count_uses_bounded_keyset_not_count_star_for_single_pk() -> None:
    """H1: a single-column-PK table's target count is BOUNDED (keyset-paged), never a
    single COUNT(*) that would exceed DSQL's 300s limit at scale. Holds for a
    non-integer PK too (the flagged UUID/varchar case, which reconciliation skips)."""
    source = _FakeSourceConnection(counts={"orders": 3})
    target = _FakeTargetConnection(counts={"orders": 3})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")]
    )
    assert report.items[0].target_row_count == 3
    assert report.items[0].matched is True
    assert not any("COUNT(*)" in s for s in target.executed)

    src2 = _FakeSourceConnection(counts={"t": 4})
    tgt2 = _FakeTargetConnection(counts={"t": 4})
    vt = _table("t", columns=("code",), primary_key=("code",))
    vt.columns[0].mysql_type = "varchar(10)"
    rep2 = _validator(src2, tgt2).validate(_SOURCE_CONFIG, _TARGET_CONFIG, [vt])
    assert rep2.items[0].target_row_count == 4
    assert not any("COUNT(*)" in s for s in tgt2.executed)


def test_reconcile_reuses_streamed_count_no_count_star() -> None:
    """When reconciliation runs (integer PK) the target count is the exact total it
    already streamed keyset-paged -- no separate COUNT(*) scan is issued."""
    source = _FakeSourceConnection(counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]})
    target = _FakeTargetConnection(counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]})
    report = _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=True
    )
    assert report.items[0].matched is True
    assert report.items[0].target_row_count == 3
    assert not any("COUNT(*)" in s for s in target.executed)


def test_composite_pk_target_count_falls_back_to_count_star() -> None:
    """A composite/missing PK has no single keyset column, so the target count still
    uses COUNT(*) (a documented residual gap: it can time out on a huge composite-PK
    table). Single-column PKs -- the common shape -- are always keyset-bounded."""
    source = _FakeSourceConnection(counts={"t": 2})
    target = _FakeTargetConnection(counts={"t": 2})
    ct = _table("t", columns=("a", "b"), primary_key=("a", "b"))
    report = _validator(source, target).validate(_SOURCE_CONFIG, _TARGET_CONFIG, [ct])
    assert report.items[0].target_row_count == 2
    assert any("COUNT(*)" in s for s in target.executed)


def test_reconcile_pk_pages_are_read_only() -> None:
    source = _FakeSourceConnection(counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]})
    target = _FakeTargetConnection(counts={"orders": 3}, pk_sets={"orders": [1, 2, 3]})
    _reconciling_validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders")], reconcile=True
    )
    offending = [text for text in source.executed if is_write_or_ddl(text)]
    assert offending == []
    # The keyset stream really paged (page_size=2 over 3 rows -> >1 page).
    pages = [s for s in source.executed if "LIMIT :PAGE" in s.upper()]
    assert len(pages) >= 2


# ---------------------------------------------------------------------------
# Per-table error isolation (no table errors check)
# ---------------------------------------------------------------------------


def test_table_error_is_isolated_not_fatal() -> None:
    # "orders" errors on the target (e.g. relation missing) but "customers" still
    # validates -- the run completes and isolates the failure to the bad table.
    source = _FakeSourceConnection(counts={"orders": 5, "customers": 3})
    target = _FakeTargetConnection(
        counts={"customers": 3}, missing_tables={"orders"}
    )
    report = _validator(source, target).validate(
        _SOURCE_CONFIG, _TARGET_CONFIG, [_table("orders"), _table("customers")]
    )
    by_table = {item.table: item for item in report.items}
    assert by_table["orders"].error is not None
    assert "does not exist" in by_table["orders"].error
    assert by_table["orders"].matched is False
    assert by_table["customers"].matched is True
    # One bad table fails the overall verdict but never aborts the run.
    assert report.is_match is False
    assert len(report.items) == 2


# ---------------------------------------------------------------------------
# Cooperative cancellation (should_cancel)
# ---------------------------------------------------------------------------


def test_validate_raises_cancelled_before_any_table() -> None:
    # should_cancel True from the start -> stops before comparing any table and
    # raises ValidationCancelled (no partial report).
    source = _FakeSourceConnection(counts={"orders": 5, "customers": 3})
    target = _FakeTargetConnection(counts={"orders": 5, "customers": 3})
    with pytest.raises(ValidationCancelled):
        _validator(source, target).validate(
            _SOURCE_CONFIG,
            _TARGET_CONFIG,
            [_table("orders"), _table("customers")],
            should_cancel=lambda: True,
        )
    # The consistent-snapshot transaction was still closed cleanly (COMMIT) and
    # the engine disposed, even though we cancelled.
    joined = " ".join(source.executed).upper()
    assert "COMMIT" in joined


def test_validate_cancels_after_first_table_and_stops_early() -> None:
    # Cancel becomes true after the first table is compared -> the second table is
    # never compared (its COUNT is not issued).
    source = _FakeSourceConnection(counts={"orders": 5, "customers": 3})
    target = _FakeTargetConnection(counts={"orders": 5, "customers": 3})
    calls = {"n": 0}

    def should_cancel() -> bool:
        # False for the first table boundary, True for the second.
        used = calls["n"]
        calls["n"] += 1
        return used >= 1

    with pytest.raises(ValidationCancelled):
        _validator(source, target).validate(
            _SOURCE_CONFIG,
            _TARGET_CONFIG,
            [_table("orders"), _table("customers")],
            should_cancel=should_cancel,
        )
    # 'customers' was never reached -> its target COUNT was never issued.
    assert any("orders" in s for s in target.executed)
    assert not any("customers" in s for s in target.executed)


def test_validate_completes_when_never_cancelled() -> None:
    # A should_cancel that always returns False behaves exactly like no cancel.
    source = _FakeSourceConnection(counts={"orders": 5})
    target = _FakeTargetConnection(counts={"orders": 5})
    report = _validator(source, target).validate(
        _SOURCE_CONFIG,
        _TARGET_CONFIG,
        [_table("orders")],
        should_cancel=lambda: False,
    )
    assert report.is_match is True
    assert len(report.items) == 1


# ---------------------------------------------------------------------------
# Drift by binlog file:position -- the fallback for a source without GTID, which
# is the NORMAL case on RDS MySQL 8.0 (GTID cannot be enabled there).
# ---------------------------------------------------------------------------


def test_binlog_advanced_same_file_compares_position() -> None:
    from dsql_migrator.core.validator import binlog_advanced

    assert binlog_advanced("mysql-bin.000004", 1120, "mysql-bin.000004", 8450) is True
    assert binlog_advanced("mysql-bin.000004", 1120, "mysql-bin.000004", 1120) is False


def test_binlog_advanced_treats_a_rotated_file_as_advanced() -> None:
    """A new binlog file means the server kept writing, even at a SMALLER position.

    The position restarts near the top of each file, so any "is it greater" test on the
    raw numbers would call a rotated log unchanged -- or worse, went-backwards.
    """
    from dsql_migrator.core.validator import binlog_advanced

    assert binlog_advanced("mysql-bin.000004", 8450, "mysql-bin.000005", 154) is True


def test_binlog_advanced_counts_a_backwards_jump_as_changed() -> None:
    # A restored/rebuilt source or RESET MASTER can move the coordinate backwards. It
    # is certainly not "unchanged since the snapshot", so it must not report clean.
    from dsql_migrator.core.validator import binlog_advanced

    assert binlog_advanced("mysql-bin.000009", 500, "mysql-bin.000002", 120) is True
    assert binlog_advanced("mysql-bin.000004", 8450, "mysql-bin.000004", 1120) is True


def test_binlog_advanced_is_undeterminable_when_a_coordinate_is_incomplete() -> None:
    # Both halves are required: a position restarts per file, so one without the other
    # cannot be compared.
    from dsql_migrator.core.validator import binlog_advanced

    assert binlog_advanced(None, 1120, "mysql-bin.000004", 8450) is None
    assert binlog_advanced("mysql-bin.000004", None, "mysql-bin.000004", 8450) is None
    assert binlog_advanced("mysql-bin.000004", 1120, None, 8450) is None
    assert binlog_advanced("mysql-bin.000004", 1120, "mysql-bin.000004", None) is None


def test_format_binlog_coordinate() -> None:
    from dsql_migrator.core.validator import format_binlog_coordinate

    assert format_binlog_coordinate("mysql-bin.000004", 1120) == "mysql-bin.000004:1120"
    assert format_binlog_coordinate("mysql-bin.000004", 0) == "mysql-bin.000004:0"
    assert format_binlog_coordinate(None, 1120) is None
    assert format_binlog_coordinate("mysql-bin.000004", None) is None


def _coord_watermark(*, gtid=None, binlog_file=None, binlog_position=None):
    """A watermark carrying replication coordinates (own name: the module already has
    a ``_watermark`` for the row-count fixtures)."""
    from datetime import datetime, timezone

    from dsql_migrator.core.models import Watermark

    return Watermark(
        gtid_executed=gtid,
        binlog_file=binlog_file,
        binlog_position=binlog_position,
        snapshot_timestamp=datetime(2026, 7, 30, tzinfo=timezone.utc),
        table_row_counts={},
    )


def test_build_drift_falls_back_to_binlog_when_the_source_has_no_gtid() -> None:
    """The RDS MySQL 8.0 case: no GTID, so drift MUST come from file:pos.

    Previously drift was GTID-only, so on the primary supported source every run
    reported "could not be determined" and the whole section was dead -- even though
    the watermark already carried file:pos and CDC already resumes from it.
    """
    from dsql_migrator.core.validator import _build_drift

    drifted = _build_drift(
        _coord_watermark(binlog_file="mysql-bin.000004", binlog_position=1120),
        None,  # no GTID on either side
        "mysql-bin.000004",
        8450,
    )
    assert drifted is not None
    assert drifted.basis == "binlog"
    assert drifted.drifted is True
    assert drifted.watermark_binlog == "mysql-bin.000004:1120"
    assert drifted.current_binlog == "mysql-bin.000004:8450"
    assert "binlog position moved" in drifted.detail

    clean = _build_drift(
        _coord_watermark(binlog_file="mysql-bin.000004", binlog_position=1120),
        None,
        "mysql-bin.000004",
        1120,
    )
    assert clean is not None
    assert clean.basis == "binlog"
    assert clean.drifted is False
    assert "No source changes" in clean.detail


def test_build_drift_prefers_gtid_when_both_sides_have_one() -> None:
    # GTID is the stronger signal (a global set, not a per-file offset), so it wins;
    # the binlog pair is still recorded for the audit detail.
    from dsql_migrator.core.validator import _build_drift

    report = _build_drift(
        _coord_watermark(gtid="uuid:1-5", binlog_file="mysql-bin.000004", binlog_position=1120),
        "uuid:1-5",
        "mysql-bin.000009",  # would say "advanced" by binlog...
        77,
    )
    assert report is not None
    assert report.basis == "gtid"
    assert report.drifted is False  # ...but the GTID says unchanged, and GTID wins
    assert report.watermark_binlog == "mysql-bin.000004:1120"


def test_build_drift_undeterminable_only_when_neither_coordinate_works() -> None:
    from dsql_migrator.core.validator import _build_drift

    report = _build_drift(_coord_watermark(), None, None, None)
    assert report is not None
    assert report.basis == ""
    assert report.drifted is False
    assert "could not be determined" in report.detail
    # The message must name BOTH missing coordinates, not just the GTID -- blaming the
    # GTID alone is what made this look like a GTID-only feature.
    assert "binlog" in report.detail


def test_build_drift_is_none_without_a_watermark() -> None:
    # No consistency point -> drift is undefined (not "clean").
    from dsql_migrator.core.validator import _build_drift

    assert _build_drift(None, "uuid:1-5", "mysql-bin.000004", 1120) is None


# ---------------------------------------------------------------------------
# Source-read dispatch by engine (MySQL text() builders vs PG readers via the shim)
# ---------------------------------------------------------------------------


class _FakeDialect:
    def __init__(self, source_type: Any) -> None:
        self.source_type = source_type


def test_source_read_dispatch_routes_by_engine(monkeypatch) -> None:
    # A MySQL source uses the build_mysql_* helpers; a PostgreSQL source reuses the PG
    # readers (the same ones the DSQL target uses) wrapped in a PgSourceConnection shim.
    import dsql_migrator.core.validator as v
    from dsql_migrator.core.models import SourceType, TableDef, ColumnDef

    calls: list[tuple] = []
    # PgSourceConnection -> a marker so we can assert the PG path wraps the connection.
    monkeypatch.setattr(v, "PgSourceConnection", lambda c: ("shim", c))
    monkeypatch.setattr(v, "_source_checksum", lambda c, t, ps: calls.append(("my_ck", c)) or "my")
    # _target_checksum / _target_pk_tokens now take source_is_postgres -- the PG-source path
    # must pass True (so the DSQL renderer uses the PG numeric-scale rule on both ends).
    monkeypatch.setattr(
        v, "_target_checksum",
        lambda c, t, ps, source_is_postgres=False: calls.append(("pg_ck", c, source_is_postgres)) or "pg",
    )
    monkeypatch.setattr(v, "_source_count", lambda c, name: calls.append(("my_ct", c)) or 1)
    monkeypatch.setattr(v, "_bounded_target_count", lambda c, t, ps: calls.append(("pg_ct", c)) or 2)
    monkeypatch.setattr(v, "_source_pk_tokens", lambda c, t, pk, n: calls.append(("my_pk", c)) or {})
    monkeypatch.setattr(
        v, "_target_pk_tokens",
        lambda c, t, pk, n, source_is_postgres=False: calls.append(("pg_pk", c, source_is_postgres)) or {},
    )

    tbl = TableDef(name="t", columns=[ColumnDef(name="id", mysql_type="integer")], primary_key=["id"])
    pg = _FakeDialect(SourceType.POSTGRES)
    my = _FakeDialect(SourceType.MYSQL)

    # checksum
    assert v._source_checksum_for(pg, "C", tbl, 100) == "pg"
    assert calls[-1] == ("pg_ck", ("shim", "C"), True)  # PG path wraps in the shim + flags PG
    assert v._source_checksum_for(my, "C", tbl, 100) == "my"
    assert calls[-1] == ("my_ck", "C")  # MySQL path uses the raw connection

    # live count
    assert v._source_row_count_live(pg, "C", tbl, 100) == 2
    assert calls[-1] == ("pg_ct", ("shim", "C"))
    assert v._source_row_count_live(my, "C", tbl, 100) == 1
    assert calls[-1] == ("my_ct", "C")

    # pk tokens (used by _diff_pks)
    v._source_pk_tokens_for(pg, "C", tbl, "id", 50)
    assert calls[-1] == ("pg_pk", ("shim", "C"), True)
    v._source_pk_tokens_for(my, "C", tbl, "id", 50)
    assert calls[-1] == ("my_pk", "C")


def test_pg_checksum_timetz_is_offset_insensitive() -> None:
    # A PostgreSQL `timetz` stores its offset. The CDC sink writes it UTC-normalized
    # (Debezium's ZonedTime is always GMT) while Full Load preserves the source offset --
    # the same instant, different stored offset -- so an offset-sensitive ::text would
    # false-mismatch a CDC-written value. The checksum classifies timetz distinctly and
    # renders it shifted to UTC on BOTH engines so an equal instant matches regardless of
    # which write path produced it.
    from dsql_migrator.core.validator import _checksum_kind, build_pg_checksum_sql
    from dsql_migrator.core.models import ColumnDef, TableDef

    # All four format_type spellings classify as "timetz" -- including the precision-
    # qualified "time(6) with time zone", which a naive "(" split would collapse to "time".
    for spelling in ("time with time zone", "time(6) with time zone", "timetz", "timetz(3)"):
        assert (
            _checksum_kind(ColumnDef(name="tz", mysql_type=spelling, target_type=spelling))
            == "timetz"
        ), spelling
    # A plain `time` (no zone) is NOT timetz -- it keeps the offset-free render.
    assert _checksum_kind(ColumnDef(name="t", mysql_type="time", target_type="time")) == "time"

    table = TableDef(
        name="app.events",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", target_type="bigint"),
            ColumnDef(name="tz", mysql_type="time with time zone", target_type="time with time zone"),
            ColumnDef(name="t", mysql_type="time", target_type="time"),
        ],
        primary_key=["id"],
    )
    pg_sql = build_pg_checksum_sql(table).as_string(None)
    assert '("tz" AT TIME ZONE \'UTC\')::text' in pg_sql  # timetz: normalized to UTC
    assert "to_char(\"t\", 'HH24:MI:SS.US')" in pg_sql     # plain time unchanged
    assert '"t" AT TIME ZONE' not in pg_sql                # ...and not shifted


def test_pg_checksum_unconstrained_numeric_rounds_to_dsql_default_scale() -> None:
    # An unconstrained PG `numeric` (no declared scale) is stored by DSQL at its default
    # numeric(18,6), so the checksum rounds BOTH sides to scale 6 -- not scale 0 (the old
    # bug: dropped the whole fraction -> false MATCH) and not raw ::text (0.5 source vs
    # 0.500000 target -> false MISMATCH). A scale-bearing numeric(p,s) keeps its scale.
    # A PostgreSQL source sets column.target_type = the PG type, so _checksum_kind takes
    # the "numeric" branch (without target_type it would fall to the plain fallback).
    from dsql_migrator.core.validator import build_pg_checksum_sql
    from dsql_migrator.core.models import ColumnDef, TableDef

    table = TableDef(
        name="t",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", target_type="bigint"),
            ColumnDef(name="unconstrained", mysql_type="numeric", target_type="numeric"),
            ColumnDef(name="scaled", mysql_type="numeric(12,2)", target_type="numeric(12,2)"),
        ],
        primary_key=["id"],
    )
    # A PostgreSQL source passes source_is_postgres=True (both the DSQL target and the PG
    # source render with it), so a bare numeric compares at DSQL's default scale 6.
    rendered = build_pg_checksum_sql(table, source_is_postgres=True).as_string(None)
    assert 'round("unconstrained", 6)' in rendered          # DSQL default scale, not 0
    assert 'round("scaled", 2)' in rendered                 # declared scale kept
    # Without the PG-source flag (the DEFAULT -- e.g. the DSQL target of a MySQL-source
    # migration) the scale-6 rule must NOT fire: a bare numeric renders at its declared
    # scale 0, matching the MySQL source side. This is the B1 regression guard.
    rendered_mysql = build_pg_checksum_sql(table).as_string(None)
    assert 'round("unconstrained", 0)' in rendered_mysql
    assert 'round("unconstrained", 6)' not in rendered_mysql


def test_pg_checksum_bigint_unsigned_scale_matches_mysql_source() -> None:
    # B1 regression: for a MySQL source the DSQL TARGET side is rendered by
    # _pg_checksum_expr with source_is_postgres=False. A MySQL `bigint unsigned` -- the
    # paren-less COLUMN_TYPE on MySQL 8.0.19+ / 8.4 / Aurora MySQL 3 -- maps to
    # numeric(20,0), an INTEGER. Both sides must render at scale 0. The old code applied
    # the PostgreSQL unconstrained-numeric scale-6 default to ANY paren-less type, so the
    # target gained ".000000" while the MySQL source side stayed scale 0 -> every BIGINT
    # UNSIGNED row false-mismatched in CHECKSUM validation. (MySQL 5.7 spells it
    # "bigint(20) unsigned" -- parens -> already scale 0 -- which is why 5.7 never broke.)
    from dsql_migrator.core.validator import _mysql_checksum_expr, _pg_checksum_expr

    for spelling in ("bigint unsigned", "bigint(20) unsigned"):
        col = ColumnDef(name="amount", mysql_type=spelling)
        mysql_expr = _mysql_checksum_expr(col)
        pg_expr = _pg_checksum_expr(col, source_is_postgres=False).as_string(None)
        assert mysql_expr == "CAST(`amount` AS DECIMAL(65, 0))", spelling
        assert 'round("amount", 0)' in pg_expr, spelling
        # The target must NOT gain fractional digits (that was the false-mismatch).
        assert 'round("amount", 6)' not in pg_expr, spelling
        assert "D000000" not in pg_expr, spelling


def test_pg_source_connection_shim_renders_composed_and_binds_params() -> None:
    # Regression (#3): the PG-source validation adapter is on the critical path of EVERY PG
    # validation read but was only ever monkeypatched away. It must render a psycopg
    # Composed to text and thread params through exec_driver_sql -- and treat empty params
    # as no-params (the `if params:` guard).
    from types import SimpleNamespace
    from dsql_migrator.core.models import ColumnDef, TableDef
    from dsql_migrator.core.validation_sql import (
        build_pg_checksum_sql,
        build_pg_pk_next_page_sql,
    )
    from dsql_migrator.core.validator_postgres import PgSourceConnection

    class _Res:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class _SqlaConn:
        def __init__(self, rows):
            self.rows = rows
            self.calls: list = []
            # driver_connection=None so Composed.as_string(None) renders offline.
            self.connection = SimpleNamespace(driver_connection=None)

        def exec_driver_sql(self, sql, params=None):
            self.calls.append((sql, params))
            return _Res(self.rows)

    table = TableDef(
        name="app.orders",
        columns=[ColumnDef(name="id", mysql_type="bigint", target_type="bigint")],
        primary_key=["id"],
    )
    sqla = _SqlaConn([(10,), (11,)])
    cur = PgSourceConnection(sqla).cursor()

    # (1) Parameterized keyset page -> rendered %(last)s + params threaded through.
    cur.execute(build_pg_pk_next_page_sql(table, "id", 500), {"last": 10})
    text, params = sqla.calls[-1]
    assert '"id" > %(last)s' in text and "LIMIT 500" in text
    assert params == {"last": 10}
    assert cur.fetchall() == [(10,), (11,)]

    # (2) A no-param Composed (checksum) takes the no-params exec branch.
    sqla.calls.clear()
    cur.execute(build_pg_checksum_sql(table))
    assert sqla.calls[-1][1] is None
    assert cur.fetchone() == (10,)

    # (3) The `if params:` guard treats an empty dict as no-params too.
    sqla.calls.clear()
    cur.execute(build_pg_pk_next_page_sql(table, "id", 5), {})
    assert sqla.calls[-1][1] is None


def test_checksum_kind_jsonb_included_json_excluded() -> None:
    # Tier-3 #14: jsonb has a canonical text form -> classified "plain" (col::text,
    # INCLUDED in the checksum); json has no byte-identical cross-engine text form ->
    # "json" (EXCLUDED). Guards a "fix" that broadens kind=="json" to startswith("json")
    # and would silently drop jsonb from the checksum.
    from dsql_migrator.core.validator import (
        _checksum_kind, _mysql_checksum_expr, _pg_checksum_expr,
    )

    jb = ColumnDef(name="d", mysql_type="jsonb", target_type="jsonb")
    js = ColumnDef(name="d", mysql_type="json", target_type="json")
    assert _checksum_kind(jb) == "plain" and _checksum_kind(js) == "json"
    assert _pg_checksum_expr(jb) is not None and _pg_checksum_expr(js) is None
    assert _mysql_checksum_expr(jb) is not None and _mysql_checksum_expr(js) is None


def test_pg_source_keyset_count_through_shim_non_integer_and_composite() -> None:
    # Tier-3 #16: the PG-source live count keyset-pages a single (uuid/varchar) PK through
    # the shim (no COUNT(*)) and falls back to COUNT(*) for a composite PK.
    from types import SimpleNamespace
    from dsql_migrator.core.models import SourceType
    from dsql_migrator.core.validator import _source_row_count_live

    class _Res:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class _PagedSqla:
        def __init__(self, batches):
            self._b = list(batches)
            self.sqls: list = []
            self.connection = SimpleNamespace(driver_connection=None)

        def exec_driver_sql(self, sql, params=None):
            self.sqls.append(sql)
            return _Res(self._b.pop(0) if self._b else [])

    # uuid single PK, page 2 over 3 rows -> genuinely paged, no COUNT(*).
    ut = _table("t", columns=("id",), primary_key=("id",))
    ut.columns[0].mysql_type = "uuid"
    sqla = _PagedSqla([[("u1",), ("u2",)], [("u3",)]])
    assert _source_row_count_live(_FakeDialect(SourceType.POSTGRES), sqla, ut, 2) == 3
    assert len(sqla.sqls) == 2
    assert not any("COUNT(*)" in s for s in sqla.sqls)
    assert '"id" > %(last)s' in sqla.sqls[1]
    # composite PK -> COUNT(*) fallback through the shim.
    ct = _table("t", columns=("a", "b"), primary_key=("a", "b"))
    sqla2 = _PagedSqla([[(2,)]])
    assert _source_row_count_live(_FakeDialect(SourceType.POSTGRES), sqla2, ct, 2) == 2
    assert "COUNT(*)" in sqla2.sqls[0]
