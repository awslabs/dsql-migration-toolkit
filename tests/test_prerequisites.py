# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the prerequisite gate (Task 20).

Covers (Property 14 / Property 1 / Property 7 / Requirements 5.10, 5.11, 12.1):
- Each pure check function returns PASS/FAIL with credential-free remediation.
- Per-table checks (PK, target schema) produce one result per selected table.
- Full Load reports CDC-only checks as SKIP; CDC runs them.
- can_proceed is False iff a required check FAILs; WARN/SKIP do not block.
- Probes are read-only fakes (no real DB/MSK reached).
"""

from __future__ import annotations

from dsql_migrator.core.models import (
    ConnectionResult,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteCheckRequest,
    PrerequisiteStatus,
    TableDef,
)
from dsql_migrator.core.models import ColumnDef
from dsql_migrator.core.prerequisites import (
    PrerequisiteChecker,
    check_binlog_row_format,
    check_gtid_mode,
    check_replication_grants,
    check_table_primary_key,
    check_target_columns_loadable,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeSource:
    def __init__(
        self,
        *,
        reachable: bool = True,
        grants: list[str] | None = None,
        variables: dict[str, str] | None = None,
    ) -> None:
        self._reachable = reachable
        self._grants = grants if grants is not None else ["GRANT ALL PRIVILEGES ON *.*"]
        self._variables = variables or {
            "log_bin": "ON",
            "binlog_format": "ROW",
            "binlog_row_image": "FULL",
            "gtid_mode": "ON",
        }

    def reachable(self) -> ConnectionResult:
        return ConnectionResult(success=self._reachable)

    def grants(self) -> list[str]:
        return list(self._grants)

    def variables(self) -> dict[str, str]:
        return dict(self._variables)


class _FakeTarget:
    def __init__(
        self,
        *,
        reachable: bool = True,
        iam_ok: bool = True,
        existing: set[str] | None = None,
        required_without_default: dict[str, list[str]] | None = None,
    ) -> None:
        self._reachable = reachable
        self._iam_ok = iam_ok
        self._existing = existing if existing is not None else set()
        # Per-table value-required columns (NOT NULL, no default, non-identity).
        # Default empty so existing tests see a clean columns check.
        self._required_without_default = required_without_default or {}

    def reachable(self) -> bool:
        return self._reachable

    def iam_auth(self) -> ConnectionResult:
        return ConnectionResult(success=self._iam_ok)

    def relation_exists(self, qualified_name: str) -> bool:
        return qualified_name in self._existing

    def required_columns_without_default(self, qualified_name: str):
        return self._required_without_default.get(qualified_name, [])


class _FakeMsk:
    def __init__(self, *, cluster: bool = True, connect: bool = True) -> None:
        self._cluster = cluster
        self._connect = connect

    def cluster_available(self) -> bool:
        return self._cluster

    def connect_available(self) -> bool:
        return self._connect


def _table(name: str, *, pk: bool = True) -> TableDef:
    return TableDef(name=name, primary_key=["id"] if pk else [])


def _table_with_columns(name: str, columns: list[str]) -> TableDef:
    return TableDef(
        name=name,
        primary_key=["id"],
        columns=[ColumnDef(name=c, mysql_type="int") for c in columns],
    )


def test_columns_loadable_fails_only_for_a_required_column_absent_from_source() -> None:
    """The core rule: flag only columns the source cannot fill.

    A user added `added_notnull` to the target DDL in Schema Conversion. It is NOT
    NULL with no default and does not exist on the source, so Full Load -- which
    inserts only source columns -- would hit a not-null violation partway through.
    That must FAIL before the load; `id`/`name`, which the source supplies, must not.
    """
    table = _table_with_columns("ecommerce.orders", ["id", "name"])

    result = check_target_columns_loadable(
        table,
        # Target's value-required columns: id (from source, fine) + added_notnull
        # (target-only, unfillable).
        target_required_without_default=["id", "added_notnull"],
    )

    assert result.status is PrerequisiteStatus.FAIL
    assert result.required is True
    assert "added_notnull" in result.detail
    # A source-backed required column must never be named as the problem.
    assert "`id`" not in result.detail and "`name`" not in result.detail


def test_columns_loadable_passes_when_extra_columns_can_take_an_absent_value() -> None:
    # Nullable / defaulted / identity target-only columns are NOT value-required, so
    # they never reach this check's input -- the load fills them with NULL/default.
    # Only source-backed required columns remain, which are fine.
    table = _table_with_columns("t", ["id", "name"])

    result = check_target_columns_loadable(
        table, target_required_without_default=["id"]
    )

    assert result.status is PrerequisiteStatus.PASS


def test_columns_loadable_passes_when_target_is_unreadable() -> None:
    # None = target missing/unreadable. That is TARGET_SCHEMA_READY's failure to
    # report; this check must not double-fail on the same cause.
    table = _table_with_columns("t", ["id"])

    result = check_target_columns_loadable(table, target_required_without_default=None)

    assert result.status is PrerequisiteStatus.PASS


def test_columns_loadable_runs_per_selected_table_in_the_full_report() -> None:
    """End to end through the checker: the new check appears per table and gates.

    orders has a target-only NOT NULL column; users does not. The report must FAIL
    orders' TARGET_COLUMNS_LOADABLE and PASS users', and the overall report must not
    be proceed-able.
    """
    tables = [
        _table_with_columns("ecommerce.orders", ["id"]),
        _table_with_columns("ecommerce.users", ["id"]),
    ]
    target = _FakeTarget(
        existing={"ecommerce.orders", "ecommerce.users"},
        required_without_default={
            "ecommerce.orders": ["id", "added_notnull"],  # added_notnull unfillable
            "ecommerce.users": ["id"],  # source-backed only
        },
    )
    checker = PrerequisiteChecker(source_probe=_FakeSource(), target_probe=target)

    report = checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.FULL_LOAD,
            tables=[t.name for t in tables],
        ),
        tables=tables,
    )

    orders = _result(
        report, PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE, "ecommerce.orders"
    )
    users = _result(
        report, PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE, "ecommerce.users"
    )
    assert orders.status is PrerequisiteStatus.FAIL
    assert users.status is PrerequisiteStatus.PASS
    assert report.can_proceed is False


def _result(report, check_id: PrerequisiteCheckId, target: str | None = None):
    for r in report.results:
        if r.check_id == check_id and (target is None or r.target == target):
            return r
    raise AssertionError(f"missing result {check_id} target={target}")


# ---------------------------------------------------------------------------
# Pure check functions
# ---------------------------------------------------------------------------


def test_replication_grants_full_load_needs_only_select() -> None:
    ok = check_replication_grants(["GRANT SELECT ON db.* TO u"], MigrationMode.FULL_LOAD)
    assert ok.status == PrerequisiteStatus.PASS

    missing = check_replication_grants(
        ["GRANT INSERT ON db.* TO u"], MigrationMode.FULL_LOAD
    )
    assert missing.status == PrerequisiteStatus.FAIL
    assert "SELECT" in missing.detail


def test_replication_grants_cdc_needs_replication_privs() -> None:
    res = check_replication_grants(
        ["GRANT SELECT ON db.* TO u"], MigrationMode.CDC
    )
    assert res.status == PrerequisiteStatus.FAIL
    assert "REPLICATION CLIENT" in res.detail and "REPLICATION SLAVE" in res.detail


def test_table_primary_key_check_targets_the_table() -> None:
    res = check_table_primary_key(_table("app.t", pk=False))
    assert res.status == PrerequisiteStatus.FAIL
    assert res.target == "app.t"
    assert "app.t" in res.remediation


def test_binlog_and_gtid_checks() -> None:
    good = {"log_bin": "ON", "binlog_format": "ROW", "binlog_row_image": "FULL"}
    assert check_binlog_row_format(good).status == PrerequisiteStatus.PASS
    bad = {"log_bin": "ON", "binlog_format": "STATEMENT", "binlog_row_image": "FULL"}
    assert check_binlog_row_format(bad).status == PrerequisiteStatus.FAIL
    assert check_gtid_mode({"gtid_mode": "ON"}).status == PrerequisiteStatus.PASS
    # GTID is recommended, not required: off -> non-blocking INFO (an optional
    # recommendation, NOT a WARN that implies something is wrong; Property 14).
    gtid_off = check_gtid_mode({"gtid_mode": "OFF"})
    assert gtid_off.status == PrerequisiteStatus.INFO
    assert gtid_off.required is False


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def test_full_load_skips_cdc_checks_and_can_proceed() -> None:
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(),
        target_probe=_FakeTarget(existing={"app.orders"}),
    )
    request = PrerequisiteCheckRequest(
        mode=MigrationMode.FULL_LOAD, tables=["app.orders"]
    )
    report = checker.check(request, tables=[_table("app.orders")])

    assert report.can_proceed is True
    assert _result(report, PrerequisiteCheckId.BINLOG_ROW_FORMAT).status == (
        PrerequisiteStatus.SKIP
    )
    assert _result(report, PrerequisiteCheckId.GTID_MODE).status == (
        PrerequisiteStatus.SKIP
    )
    assert _result(report, PrerequisiteCheckId.MSK_AVAILABLE).status == (
        PrerequisiteStatus.SKIP
    )


def test_cdc_runs_all_checks_and_can_proceed_when_healthy() -> None:
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    request = PrerequisiteCheckRequest(mode=MigrationMode.CDC, tables=["app.orders"])
    report = checker.check(request, tables=[_table("app.orders")])

    assert report.can_proceed is True
    assert _result(report, PrerequisiteCheckId.BINLOG_ROW_FORMAT).status == (
        PrerequisiteStatus.PASS
    )
    assert _result(report, PrerequisiteCheckId.MSK_AVAILABLE).status == (
        PrerequisiteStatus.PASS
    )


def test_cdc_gtid_off_is_info_but_does_not_block() -> None:
    # GTID disabled on the source must not gate CDC: it is a non-blocking INFO
    # (optional recommendation) because CDC resumes from the binlog file:position
    # watermark (Property 14).
    source = _FakeSource(
        variables={
            "log_bin": "ON",
            "binlog_format": "ROW",
            "binlog_row_image": "FULL",
            "gtid_mode": "OFF",
        }
    )
    checker = PrerequisiteChecker(
        source_probe=source,
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    request = PrerequisiteCheckRequest(mode=MigrationMode.CDC, tables=["app.orders"])
    report = checker.check(request, tables=[_table("app.orders")])

    gtid = _result(report, PrerequisiteCheckId.GTID_MODE)
    assert gtid.status == PrerequisiteStatus.INFO
    assert gtid.required is False
    # Everything else healthy -> the INFO recommendation does not block.
    assert report.can_proceed is True


def test_required_failure_blocks_progression() -> None:
    # Target schema not applied for the selected table -> required FAIL.
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(),
        target_probe=_FakeTarget(existing=set()),
    )
    request = PrerequisiteCheckRequest(
        mode=MigrationMode.FULL_LOAD, tables=["app.orders"]
    )
    report = checker.check(request, tables=[_table("app.orders")])

    assert report.can_proceed is False
    schema_ready = _result(
        report, PrerequisiteCheckId.TARGET_SCHEMA_READY, target="app.orders"
    )
    assert schema_ready.status == PrerequisiteStatus.FAIL


def test_cdc_missing_msk_is_info_but_does_not_block() -> None:
    # New model: MSK is created by the cdc-stack deploy AFTER the CDC step, so an
    # absent MSK is an expected, no-action-needed INFO (not a WARN that implies a
    # problem, not a required FAIL). The user must still be able to reach the CDC
    # step (which produces the deploy config), so can_proceed stays True as long
    # as the real prerequisites (source/target) pass.
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=None,
    )
    request = PrerequisiteCheckRequest(mode=MigrationMode.CDC, tables=["app.orders"])
    report = checker.check(request, tables=[_table("app.orders")])

    assert report.can_proceed is True
    msk = _result(report, PrerequisiteCheckId.MSK_AVAILABLE)
    assert msk.status == PrerequisiteStatus.INFO
    assert msk.required is False
    assert _result(report, PrerequisiteCheckId.MSK_CONNECT_AVAILABLE).status == (
        PrerequisiteStatus.INFO
    )


def test_per_table_results_one_per_selected_table() -> None:
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(),
        target_probe=_FakeTarget(existing={"app.a", "app.b"}),
    )
    tables = [_table("app.a"), _table("app.b")]
    request = PrerequisiteCheckRequest(
        mode=MigrationMode.FULL_LOAD, tables=["app.a", "app.b"]
    )
    report = checker.check(request, tables=tables)

    pk_results = [
        r
        for r in report.results
        if r.check_id == PrerequisiteCheckId.TABLE_PRIMARY_KEY
    ]
    assert {r.target for r in pk_results} == {"app.a", "app.b"}


# ---------------------------------------------------------------------------
# Migration-wide LOB exclusion feeds the loadability gate
# ---------------------------------------------------------------------------


def _lob_table(name: str) -> TableDef:
    """A table with a PK, a normal column, and an oversized-LOB column."""
    return TableDef(
        name=name,
        primary_key=["id"],
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="name", mysql_type="varchar(100)"),
            ColumnDef(name="blob_doc", mysql_type="longtext"),
        ],
    )


def test_excluding_a_notnull_target_column_flips_loadable_to_fail() -> None:
    """Excluding a column the target REQUIRES turns a PASS into a FAIL.

    Without an exclusion the source supplies ``blob_doc``, so a NOT NULL/no-default
    target column of that name is filled and the gate PASSes. Once the user excludes
    ``blob_doc`` from the migration it is no longer in the load's column set, so it
    becomes unfillable -- the gate must FAIL before the load, not after every batch
    hits a not-null violation mid-load.
    """
    table = _lob_table("app.docs")
    target = _FakeTarget(
        existing={"app.docs"},
        required_without_default={"app.docs": ["id", "blob_doc"]},
    )
    checker = PrerequisiteChecker(source_probe=_FakeSource(), target_probe=target)
    request = PrerequisiteCheckRequest(
        mode=MigrationMode.FULL_LOAD, tables=["app.docs"]
    )

    # No exclusion: blob_doc is source-backed, so loadable PASSes.
    clean = checker.check(request, tables=[table])
    assert (
        _result(
            clean, PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE, "app.docs"
        ).status
        is PrerequisiteStatus.PASS
    )
    assert clean.can_proceed is True

    # Exclude blob_doc: now it cannot fill the required target column -> FAIL + block.
    excluded = checker.check(
        request,
        tables=[table],
        excluded_columns={"app.docs": ["blob_doc"]},
    )
    result = _result(
        excluded, PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE, "app.docs"
    )
    assert result.status is PrerequisiteStatus.FAIL
    assert "blob_doc" in result.detail
    assert excluded.can_proceed is False


def test_excluding_a_nullable_column_stays_loadable() -> None:
    # A nullable/defaulted target column is never in required_without_default, so
    # excluding its source counterpart leaves the gate PASSing -- the target takes
    # NULL/default for the column the load no longer writes.
    table = _lob_table("app.docs")
    target = _FakeTarget(
        existing={"app.docs"},
        required_without_default={"app.docs": ["id"]},
    )
    checker = PrerequisiteChecker(source_probe=_FakeSource(), target_probe=target)
    report = checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=["app.docs"]),
        tables=[table],
        excluded_columns={"app.docs": ["blob_doc"]},
    )
    assert (
        _result(
            report, PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE, "app.docs"
        ).status
        is PrerequisiteStatus.PASS
    )
    assert report.can_proceed is True


def test_exclusion_never_drops_a_pk_from_the_primary_key_check() -> None:
    # Even if a PK column is (wrongly) listed for exclusion, the filter preserves it,
    # so the PK check still sees a keyed table and PASSes -- the exclusion cannot
    # accidentally strip the key that anchors keyset streaming / ON CONFLICT.
    table = _lob_table("app.docs")
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(),
        target_probe=_FakeTarget(existing={"app.docs"}),
    )
    report = checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=["app.docs"]),
        tables=[table],
        excluded_columns={"app.docs": ["id", "blob_doc"]},
    )
    assert (
        _result(report, PrerequisiteCheckId.TABLE_PRIMARY_KEY, "app.docs").status
        is PrerequisiteStatus.PASS
    )
