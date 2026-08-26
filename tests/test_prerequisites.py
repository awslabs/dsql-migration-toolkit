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
    SourceType,
    TableDef,
)
from dsql_migrator.core.models import ColumnDef
from dsql_migrator.core.prerequisites import (
    PrerequisiteChecker,
    check_binlog_retention,
    check_binlog_row_format,
    check_gtid_mode,
    check_postgres_cdc_unsupported,
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
        cdc_facts=None,
    ) -> None:
        self._reachable = reachable
        self._grants = grants if grants is not None else ["GRANT ALL PRIVILEGES ON *.*"]
        self._variables = variables or {
            "log_bin": "ON",
            "binlog_format": "ROW",
            "binlog_row_image": "FULL",
            "gtid_mode": "ON",
            # ~30d self-managed retention so the default healthy source PASSes the
            # binlog-retention check too.
            "binlog_expire_logs_seconds": "2592000",
        }
        self._cdc_facts = cdc_facts

    def reachable(self) -> ConnectionResult:
        return ConnectionResult(success=self._reachable)

    def grants(self) -> list[str]:
        return list(self._grants)

    def variables(self) -> dict[str, str]:
        return dict(self._variables)

    def cdc_prerequisites(self, table_names):
        # None (default) -> the checker's PostgreSQL branch falls back to the
        # not-yet-supported INFO; a PostgresCdcFacts -> the real PG checks run.
        return self._cdc_facts


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


def test_binlog_retention_rds_value() -> None:
    # RDS retention hours: >= 24h passes, a short/unset ("0") value WARNs.
    ok = check_binlog_retention({"rds_binlog_retention_hours": "168"})
    assert ok.status == PrerequisiteStatus.PASS
    short = check_binlog_retention({"rds_binlog_retention_hours": "0"})
    assert short.status == PrerequisiteStatus.WARN
    assert short.required is False  # never a gating FAIL
    assert "168" in short.remediation  # points at the RDS fix


def test_binlog_retention_self_managed() -> None:
    # binlog_expire_logs_seconds: 30d passes; a short value WARNs; 0 = purge DISABLED
    # (binlogs kept) = PASS. expire_logs_days is the older fallback.
    assert (
        check_binlog_retention({"binlog_expire_logs_seconds": "2592000"}).status
        == PrerequisiteStatus.PASS
    )
    assert (
        check_binlog_retention({"binlog_expire_logs_seconds": "3600"}).status
        == PrerequisiteStatus.WARN
    )
    assert (
        check_binlog_retention({"binlog_expire_logs_seconds": "0"}).status
        == PrerequisiteStatus.PASS
    )
    assert (
        check_binlog_retention({"expire_logs_days": "7"}).status
        == PrerequisiteStatus.PASS
    )


def test_binlog_retention_unknown_is_non_blocking_info() -> None:
    # No retention signal at all -> advisory INFO, never a WARN/FAIL.
    res = check_binlog_retention({})
    assert res.status == PrerequisiteStatus.INFO
    assert res.required is False


def test_binlog_retention_rds_takes_precedence_over_variables() -> None:
    # An RDS source with retention unset ("0") is a risk even if the server variable
    # still reports a long value -- RDS governs retention, so RDS wins.
    res = check_binlog_retention(
        {"rds_binlog_retention_hours": "0", "binlog_expire_logs_seconds": "2592000"}
    )
    assert res.status == PrerequisiteStatus.WARN


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


# ---------------------------------------------------------------------------
# PostgreSQL source: CDC is not yet implemented, so the CDC-only checks must be
# engine-aware -- a PostgreSQL source must NOT run the MySQL binlog/GTID/grant
# checks (which would falsely FAIL) and must report an honest, non-blocking INFO.
# ---------------------------------------------------------------------------


def test_prereq_request_defaults_to_mysql_source() -> None:
    # Existing callers build the request without source_type; the default keeps
    # them on the MySQL checks.
    request = PrerequisiteCheckRequest(mode=MigrationMode.CDC)
    assert request.source_type is SourceType.MYSQL


def test_postgres_cdc_request_never_runs_mysql_variable_checks() -> None:
    """A PostgreSQL source asked for CDC must never call the MySQL `variables()` probe.

    The checker branches on the engine to the PostgreSQL readiness checks (fed by the
    dialect probe), and the MySQL binlog/GTID checks are SKIP. Crucially it never calls
    `variables()`, whose `SHOW GLOBAL VARIABLES` SQL would error on a PostgreSQL
    connection. (With healthy facts here, the PG checks pass; the facts-None blocking
    behavior is covered separately.)
    """

    class _NoVariablesSource(_FakeSource):
        def variables(self) -> dict[str, str]:  # pragma: no cover - must not run
            raise AssertionError(
                "variables() (MySQL SHOW GLOBAL VARIABLES) must not run for a "
                "PostgreSQL source"
            )

    checker = PrerequisiteChecker(
        source_probe=_NoVariablesSource(
            grants=["GRANT ALL PRIVILEGES ON db.* TO u"], cdc_facts=_pg_facts_ok()
        ),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    request = PrerequisiteCheckRequest(
        mode=MigrationMode.CDC,
        tables=["app.orders"],
        source_type=SourceType.POSTGRES,
    )
    report = checker.check(request, tables=[_table("app.orders")])

    # The MySQL-only checks are not applicable for this engine.
    assert _result(report, PrerequisiteCheckId.BINLOG_ROW_FORMAT).status is (
        PrerequisiteStatus.SKIP
    )
    assert _result(report, PrerequisiteCheckId.GTID_MODE).status is (
        PrerequisiteStatus.SKIP
    )
    # Healthy PG facts -> the real checks ran and pass.
    assert report.can_proceed is True


def test_postgres_source_never_demands_mysql_replication_grants() -> None:
    # Even in CDC mode, a PostgreSQL source is only asked for SELECT (its CDC
    # replication readiness is checked differently when PG CDC ships) -- never
    # MySQL's REPLICATION CLIENT/SLAVE, which a PG grant list would never contain.
    pg_select_only = check_replication_grants(
        ["GRANT SELECT ON db.* TO u"],
        MigrationMode.CDC,
        source_type=SourceType.POSTGRES,
    )
    assert pg_select_only.status is PrerequisiteStatus.PASS

    # A MySQL source in CDC mode still requires the replication privileges.
    mysql_missing = check_replication_grants(
        ["GRANT SELECT ON db.* TO u"],
        MigrationMode.CDC,
        source_type=SourceType.MYSQL,
    )
    assert mysql_missing.status is PrerequisiteStatus.FAIL
    assert "REPLICATION" in mysql_missing.detail.upper()


def test_check_postgres_cdc_unsupported_is_credential_free_info() -> None:
    result = check_postgres_cdc_unsupported()
    assert result.status is PrerequisiteStatus.INFO
    assert result.required is False
    assert "PostgreSQL" in result.title
    # Points the user at the supported path.
    assert "Full Load" in result.detail


# ---------------------------------------------------------------------------
# PostgreSQL CDC readiness: the real logical-replication checks (Phase C5),
# fed by PostgresCdcFacts from the dialect probe.
# ---------------------------------------------------------------------------


def _pg_facts_ok(**over):
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

    base = dict(
        wal_level="logical", is_superuser=False, has_replication_role=True,
        max_replication_slots=10, used_replication_slots=1, max_wal_senders=10,
        is_in_recovery=False, replica_identity={"app.orders": "d"},
    )
    base.update(over)
    return PostgresCdcFacts(**base)


def test_postgres_cdc_facts_run_the_real_readiness_checks() -> None:
    from dsql_migrator.core.models import PrerequisiteCheckId as Id

    checker = PrerequisiteChecker(
        source_probe=_FakeSource(cdc_facts=_pg_facts_ok()),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    report = checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.CDC, tables=["app.orders"],
            source_type=SourceType.POSTGRES,
        ),
        tables=[_table("app.orders")],
    )
    ids = {r.check_id for r in report.results}
    # The real PG checks ran (not the not-supported INFO fallback).
    assert Id.WAL_LEVEL_LOGICAL in ids
    assert Id.REPLICATION_ROLE in ids
    assert Id.SOURCE_IS_WRITER in ids
    assert Id.REPLICA_IDENTITY in ids
    assert Id.POSTGRES_CDC_UNSUPPORTED not in ids
    # MSK is engine-neutral -> still runs for PostgreSQL CDC.
    assert Id.MSK_AVAILABLE in ids
    # MySQL binlog/GTID are not applicable -> SKIP.
    assert _result(report, Id.BINLOG_ROW_FORMAT).status is PrerequisiteStatus.SKIP
    assert report.can_proceed is True


def test_postgres_cdc_unready_source_blocks() -> None:
    # wal_level!=logical, no replication role, a standby, REPLICA IDENTITY nothing:
    # every required PG check FAILs, so can_proceed is False.
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(
            cdc_facts=_pg_facts_ok(
                wal_level="replica", has_replication_role=False,
                is_in_recovery=True, replica_identity={"app.orders": "n"},
            )
        ),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    report = checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.CDC, tables=["app.orders"],
            source_type=SourceType.POSTGRES,
        ),
        tables=[_table("app.orders")],
    )
    assert report.can_proceed is False


def test_postgres_cdc_facts_none_blocks_the_run() -> None:
    # For a PostgreSQL source, None facts mean the readiness probe FAILED (unreachable /
    # insufficient privilege). CDC must NOT proceed against an unverified source -- a
    # required FAIL blocks it (not a benign INFO), so a slot/publication is never created
    # against a source whose logical-replication readiness is unknown.
    from dsql_migrator.core.models import PrerequisiteCheckId as Id

    checker = PrerequisiteChecker(
        source_probe=_FakeSource(cdc_facts=None),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    report = checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.CDC, tables=["app.orders"],
            source_type=SourceType.POSTGRES,
        ),
        tables=[_table("app.orders")],
    )
    blocker = _result(report, Id.WAL_LEVEL_LOGICAL)
    assert blocker.status is PrerequisiteStatus.FAIL
    assert blocker.required is True
    assert report.can_proceed is False  # unverified -> blocked


def test_pg_pure_checks_wal_level_role_writer_replica_identity() -> None:
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity,
        check_replication_role,
        check_source_is_writer,
        check_wal_level_logical,
    )

    # wal_level
    assert check_wal_level_logical(_pg_facts_ok()).status is PrerequisiteStatus.PASS
    assert check_wal_level_logical(
        _pg_facts_ok(wal_level="replica")
    ).status is PrerequisiteStatus.FAIL
    # unknown wal_level -> non-blocking INFO
    assert check_wal_level_logical(
        _pg_facts_ok(wal_level=None)
    ).status is PrerequisiteStatus.INFO
    # replication role: superuser OR membership; else FAIL
    assert check_replication_role(
        _pg_facts_ok(is_superuser=True, has_replication_role=False)
    ).status is PrerequisiteStatus.PASS
    assert check_replication_role(
        _pg_facts_ok(has_replication_role=False)
    ).status is PrerequisiteStatus.FAIL
    # writer vs standby
    assert check_source_is_writer(_pg_facts_ok()).status is PrerequisiteStatus.PASS
    assert check_source_is_writer(
        _pg_facts_ok(is_in_recovery=True)
    ).status is PrerequisiteStatus.FAIL
    # replica identity: 'd'/'f'/'i' usable, 'n' fails, unknown -> INFO
    t = _table("app.orders")
    assert check_replica_identity(
        t, _pg_facts_ok(replica_identity={"app.orders": "f"})
    ).status is PrerequisiteStatus.PASS
    assert check_replica_identity(
        t, _pg_facts_ok(replica_identity={"app.orders": "n"})
    ).status is PrerequisiteStatus.FAIL
    assert check_replica_identity(
        t, _pg_facts_ok(replica_identity={})
    ).status is PrerequisiteStatus.INFO


def test_pg_replica_identity_index_and_default_pass() -> None:
    # Regression: all three usable REPLICA IDENTITY codes must PASS -- 'd'
    # (default/PK), 'f' (full) and 'i' (index) -- while 'n' (nothing) FAILs. Existing
    # tests only pinned 'f', so 'd'/'i' could regress out of _USABLE_REPLICA_IDENTITY
    # unnoticed.
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity

    t = _table("app.orders")
    for code in ("d", "f", "i"):
        assert check_replica_identity(
            t, _pg_facts_ok(replica_identity={"app.orders": code})
        ).status is PrerequisiteStatus.PASS, code
    assert check_replica_identity(
        t, _pg_facts_ok(replica_identity={"app.orders": "n"})
    ).status is PrerequisiteStatus.FAIL


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


def test_check_replication_slot_headroom_states() -> None:
    # Regression + enhancement (#8): the headroom prereq WARNs when there is no room for a
    # new CDC slot OR walsender, INFOs on unknown counts (never a false FAIL), PASSes when
    # healthy. Non-blocking (required=False) either way.
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_replication_slot_headroom,
    )

    S = PrerequisiteStatus
    healthy = check_replication_slot_headroom(
        PostgresCdcFacts(max_replication_slots=10, used_replication_slots=2,
                         max_wal_senders=10, used_wal_senders=2)
    )
    assert healthy.status is S.PASS and healthy.required is False
    # Slots exhausted -> WARN.
    assert check_replication_slot_headroom(
        PostgresCdcFacts(max_replication_slots=5, used_replication_slots=5,
                         max_wal_senders=10, used_wal_senders=1)
    ).status is S.WARN
    # Degenerate max_wal_senders=0 -> WARN.
    assert check_replication_slot_headroom(
        PostgresCdcFacts(max_replication_slots=10, used_replication_slots=0,
                         max_wal_senders=0)
    ).status is S.WARN
    # NEW: walsender pool exhausted even with FREE slots -> WARN (was PASS before #8).
    walsender_full = check_replication_slot_headroom(
        PostgresCdcFacts(max_replication_slots=10, used_replication_slots=1,
                         max_wal_senders=5, used_wal_senders=5)
    )
    assert walsender_full.status is S.WARN and "5/5" in walsender_full.detail
    # Unknown counts -> INFO, never a blocking failure.
    info = check_replication_slot_headroom(PostgresCdcFacts(max_replication_slots=None))
    assert info.status is S.INFO and info.required is False
