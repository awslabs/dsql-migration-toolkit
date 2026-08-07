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
from dsql_migrator.core.validator import (
    ValidationCancelled,
    Validator,
    build_mysql_checksum_sql,
    build_orphan_count_sql,
    build_pg_checksum_sql,
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
        if "NOT EXISTS" in text:
            child = _orphan_child_table(text)
            return self._orphans.get(child, 0)
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
    source = _FakeSourceConnection(counts={"orders": 2}, checksums={"orders": "x"})
    target = _FakeTargetConnection(counts={"orders": 2}, checksums={"orders": "x"})
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
        checksums={"orders": "abc", "items": "def"},
    )
    target = _FakeTargetConnection(
        counts={"orders": 2, "items": 9},
        checksums={"orders": "abc", "items": "def"},
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
    assert "CASE WHEN `active` = 0 THEN 'false' ELSE 'true' END" in mysql_sql
    assert '"active"::text' in pg_sql


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
    # Fixed 4-digit fraction preserved, integer run unchanged.
    assert scaled.endswith("D0000")
    assert sum(ch == "9" for ch in scaled.split("D")[0]) >= 64


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
        "CASE WHEN `active` = 0 THEN 'false' ELSE 'true' END",
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
    assert not any("LIMIT :PAGE" in s.upper() for s in source.executed)


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
