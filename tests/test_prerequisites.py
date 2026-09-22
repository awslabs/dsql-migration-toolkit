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
    check_replication_grants,
    check_select_grant_scope,
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


def test_select_grant_scope_notice_only_when_scoped() -> None:
    # Global SELECT (*.*) or ALL PRIVILEGES on *.* -> no scope notice: introspection
    # can see every object.
    assert check_select_grant_scope(["GRANT SELECT ON *.* TO `u`@`%`"]) is None
    assert (
        check_select_grant_scope(["GRANT ALL PRIVILEGES ON *.* TO `u`@`%`"]) is None
    )
    # No SELECT at all -> REPLICATION_GRANTS owns the FAIL; no scope notice here.
    assert check_select_grant_scope(["GRANT INSERT ON `app`.* TO `u`@`%`"]) is None

    # A db-scoped SELECT -> a non-blocking WARN (objects outside the grant are invisible).
    scoped = check_select_grant_scope(["GRANT SELECT ON `app`.* TO `u`@`%`"])
    assert scoped is not None
    assert scoped.check_id is PrerequisiteCheckId.SELECT_GRANT_SCOPE
    assert scoped.status is PrerequisiteStatus.WARN
    assert scoped.required is False  # never blocks

    # A table-scoped SELECT is likewise scoped -> a notice.
    tbl = check_select_grant_scope(["GRANT SELECT ON `app`.`orders` TO `u`@`%`"])
    assert tbl is not None and tbl.status is PrerequisiteStatus.WARN

    # PostgreSQL source: introspection does not use MySQL SHOW statements, so no notice.
    assert (
        check_select_grant_scope(
            ["GRANT SELECT ON `app`.* TO `u`@`%`"], source_type=SourceType.POSTGRES
        )
        is None
    )


def test_replication_grants_role_granted_select_downgrades_to_warn() -> None:
    # SELECT is absent from the DIRECT grants but the user holds a role, whose privileges
    # plain SHOW GRANTS does not expand. The false "SELECT missing" must not hard-block.
    role_grants = [
        "GRANT USAGE ON *.* TO `mig`@`%`",
        "GRANT `app_read`@`%` TO `mig`@`%`",  # a role-membership line (no ON clause)
    ]
    res = check_replication_grants(role_grants, MigrationMode.FULL_LOAD)
    assert res.status is PrerequisiteStatus.WARN
    assert res.required is False  # non-blocking, so a role-privileged user can proceed

    # Without any role, the same missing SELECT stays a real, blocking FAIL.
    no_role = check_replication_grants(
        ["GRANT USAGE ON *.* TO `mig`@`%`"], MigrationMode.FULL_LOAD
    )
    assert no_role.status is PrerequisiteStatus.FAIL

    # CDC with the replication privileges ALSO missing is not downgraded (the missing
    # set is more than just SELECT), so it remains a blocking FAIL.
    cdc = check_replication_grants(
        ["GRANT `app_read`@`%` TO `mig`@`%`"], MigrationMode.CDC
    )
    assert cdc.status is PrerequisiteStatus.FAIL


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
    # A CONFIGURED zero BLOCKS (was WARN): nothing can be freed to make room, so the
    # WARN's remediation ("drop an unused slot") could not work, and since the Full Load
    # takes its snapshot through the same slot the run cannot even begin. Categorically
    # different from a full pool, which stays a non-blocking WARN above.
    for zero in (
        PostgresCdcFacts(max_replication_slots=10, used_replication_slots=0,
                         max_wal_senders=0),
        PostgresCdcFacts(max_replication_slots=0, used_replication_slots=0,
                         max_wal_senders=10),
    ):
        blocked = check_replication_slot_headroom(zero)
        assert blocked.status is S.FAIL, zero
        assert blocked.required is True, "a source that forbids every slot must block"
        assert "cannot be freed" in blocked.remediation.lower() or (
            "nothing can be freed" in blocked.remediation.lower()
        )
    # NEW: walsender pool exhausted even with FREE slots -> WARN (was PASS before #8).
    walsender_full = check_replication_slot_headroom(
        PostgresCdcFacts(max_replication_slots=10, used_replication_slots=1,
                         max_wal_senders=5, used_wal_senders=5)
    )
    assert walsender_full.status is S.WARN and "5/5" in walsender_full.detail
    # Unknown counts -> INFO, never a blocking failure.
    info = check_replication_slot_headroom(PostgresCdcFacts(max_replication_slots=None))
    assert info.status is S.INFO and info.required is False


def test_prereq_category_map_covers_every_check_id() -> None:
    """D-3: `group_prereq_results` falls back to SCHEMA_TABLES, so an unmapped id is filed
    under the wrong heading rather than raising. That is how the five PostgreSQL
    logical-replication checks landed there -- four of them are server settings/privileges,
    leaving 'Source Configuration' nearly empty on a PG CDC run while 'Schema & Tables' was
    padded with server-level rows. No test covered the map, so they slipped in silently."""
    from dsql_migrator.core.models import PrerequisiteCheckId
    from dsql_migrator.ui.data_migration._models import (
        _PREREQ_CATEGORY_BY_CHECK,
        PrereqCategory,
    )

    assert set(PrerequisiteCheckId) == set(_PREREQ_CATEGORY_BY_CHECK)
    # The server-level PostgreSQL checks belong with the MySQL binlog/GTID rows.
    for check in (
        PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
        PrerequisiteCheckId.REPLICATION_ROLE,
        PrerequisiteCheckId.REPLICATION_SLOTS,
        PrerequisiteCheckId.SOURCE_IS_WRITER,
        PrerequisiteCheckId.SLOT_WAL_RETENTION,
    ):
        assert _PREREQ_CATEGORY_BY_CHECK[check] is PrereqCategory.SOURCE_CONFIG, check
    # The per-table ones stay where the fallback happened to put them.
    for check in (
        PrerequisiteCheckId.REPLICA_IDENTITY,
        PrerequisiteCheckId.TARGET_COLUMNS_LOADABLE,
    ):
        assert _PREREQ_CATEGORY_BY_CHECK[check] is PrereqCategory.SCHEMA_TABLES, check


def test_slot_wal_retention_follows_binlog_retention_semantics() -> None:
    """D-1: the PostgreSQL analog of the binlog-retention risk. -1 (unlimited, the default)
    passes; a finite cap WARNs without blocking; unknown degrades to INFO."""
    from dsql_migrator.core.models import PrerequisiteCheckId, PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        check_slot_wal_retention,
    )

    unlimited = check_slot_wal_retention(
        PostgresCdcFacts(max_slot_wal_keep_size_mb=-1)
    )
    assert unlimited.check_id is PrerequisiteCheckId.SLOT_WAL_RETENTION
    assert unlimited.status is PrerequisiteStatus.PASS

    capped = check_slot_wal_retention(PostgresCdcFacts(max_slot_wal_keep_size_mb=2048))
    assert capped.status is PrerequisiteStatus.WARN
    # Never hard-blocks: a fast load + prompt Start CDC can fit inside the cap.
    assert capped.required is False
    assert "2048 MB" in capped.detail

    unknown = check_slot_wal_retention(PostgresCdcFacts())
    assert unknown.status is PrerequisiteStatus.INFO
    assert unknown.required is False


def test_pg_cdc_report_keeps_binlog_retention_visible_and_adds_the_wal_check() -> None:
    """D-1: BINLOG_RETENTION appeared as a SKIP in the WEAKER Full Load report but vanished
    entirely from the PG CDC report -- a check id present in the weaker mode and absent in
    the stronger one reads as an oversight. And its PostgreSQL equivalent (the slot's WAL
    can be discarded, invalidating it, which is the same silent Full-Load-to-CDC gap) was
    not checked at all."""
    from dsql_migrator.core.models import PrerequisiteCheckId as Id

    def _report(mode, facts=None):
        checker = PrerequisiteChecker(
            source_probe=_FakeSource(cdc_facts=facts or _pg_facts_ok()),
            target_probe=_FakeTarget(existing={"app.orders"}),
            msk_probe=_FakeMsk(),
        )
        return checker.check(
            PrerequisiteCheckRequest(
                mode=mode, tables=["app.orders"], source_type=SourceType.POSTGRES
            ),
            tables=[_table("app.orders")],
        )

    cdc = _report(MigrationMode.CDC)
    full = _report(MigrationMode.FULL_LOAD)
    # No longer missing from the stronger mode.
    assert _result(cdc, Id.BINLOG_RETENTION).status is PrerequisiteStatus.SKIP
    assert _result(full, Id.BINLOG_RETENTION).status is PrerequisiteStatus.SKIP
    # The PG equivalent RUNS only in CDC mode (a Full Load creates no slot) -- but it is
    # still LISTED in Full Load as a SKIP, for the very reason asserted just above: a
    # check id present in one mode and absent in the other reads as an oversight, and the
    # Full-Load SKIP list exists to preview what switching to CDC will require. (This
    # assertion used to demand the id be ABSENT from Full Load, which contradicted the
    # BINLOG_RETENTION rule two lines up and was how a PostgreSQL operator ended up with
    # a Full-Load preview listing only MySQL's binlog/GTID checks and none of their own.)
    assert _result(cdc, Id.SLOT_WAL_RETENTION).status is not PrerequisiteStatus.SKIP
    assert _result(full, Id.SLOT_WAL_RETENTION).status is PrerequisiteStatus.SKIP
    # A finite cap warns but must never block the run.
    capped = _report(MigrationMode.CDC, _pg_facts_ok(max_slot_wal_keep_size_mb=512))
    assert _result(capped, Id.SLOT_WAL_RETENTION).status is PrerequisiteStatus.WARN
    assert capped.can_proceed is True


def _prereq_report(mode, source_type, facts=None):
    """A healthy report for one (mode, engine) pair."""
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(cdc_facts=facts or _pg_facts_ok()),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    return checker.check(
        PrerequisiteCheckRequest(
            mode=mode, tables=["app.orders"], source_type=source_type
        ),
        tables=[_table("app.orders")],
    )


# The PG readiness checks an operator must satisfy before switching to CDC -- so the
# Full-Load-only preview has to list exactly these, not MySQL's binlog/GTID trio.
_PG_CDC_CHECK_IDS = (
    PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
    PrerequisiteCheckId.REPLICATION_ROLE,
    PrerequisiteCheckId.REPLICATION_SLOTS,
    PrerequisiteCheckId.SLOT_WAL_RETENTION,
    PrerequisiteCheckId.SOURCE_IS_WRITER,
    PrerequisiteCheckId.REPLICA_IDENTITY,
)


def test_full_load_only_previews_the_postgres_readiness_checks_for_a_pg_source() -> None:
    """The Full-Load SKIP list must preview THIS engine's CDC requirements.

    Leaving the CDC checks visible as SKIP in Full-Load-only mode exists to show what
    switching to CDC will additionally require. For a PostgreSQL source that preview
    listed MySQL's three binlog/GTID rows and NONE of the six PG checks -- so it
    previewed another engine's requirements while hiding the operator's own.
    """
    report = _prereq_report(MigrationMode.FULL_LOAD, SourceType.POSTGRES)

    for check_id in _PG_CDC_CHECK_IDS:
        result = _result(report, check_id)
        assert result.status is PrerequisiteStatus.SKIP, (
            f"{check_id.value} must be previewed as SKIP in Full-Load-only mode"
        )
        assert result.detail == "Not applicable for this mode.", (
            f"{check_id.value} DOES apply to PostgreSQL under CDC, so the reason is the "
            f"mode, not the engine: {result.detail!r}"
        )
    # Still non-blocking -- a preview must never gate a Full Load.
    assert report.can_proceed is True


def test_binlog_and_gtid_skips_blame_the_engine_not_the_mode_for_a_pg_source() -> None:
    """"Not applicable for this mode" implies "it will apply under CDC" -- false for PG.

    PostgreSQL has no binary log and no GTID in ANY mode, so the reason has to name the
    engine. Asserted in BOTH modes: the rows stay visible (a check id present in one mode
    and absent in the other reads as an oversight) but must never claim to be pending.
    """
    mysql_only = (
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.BINLOG_RETENTION,
        PrerequisiteCheckId.GTID_MODE,
    )
    for mode in (MigrationMode.FULL_LOAD, MigrationMode.CDC):
        report = _prereq_report(mode, SourceType.POSTGRES)
        for check_id in mysql_only:
            result = _result(report, check_id)
            assert result.status is PrerequisiteStatus.SKIP, (mode, check_id)
            assert result.detail == "Not applicable for this source engine.", (
                f"{mode.value}/{check_id.value} still blames the mode, telling a "
                f"PostgreSQL operator these apply under CDC: {result.detail!r}"
            )


def test_full_load_only_for_mysql_is_unchanged_and_lists_no_pg_checks() -> None:
    """Control: a MySQL source keeps the binlog/GTID preview and gains no PG rows."""
    report = _prereq_report(MigrationMode.FULL_LOAD, SourceType.MYSQL)
    present = {r.check_id for r in report.results}

    for check_id in (
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.BINLOG_RETENTION,
        PrerequisiteCheckId.GTID_MODE,
    ):
        result = _result(report, check_id)
        assert result.status is PrerequisiteStatus.SKIP
        # For MySQL these DO apply under CDC, so the mode is the right reason.
        assert result.detail == "Not applicable for this mode."
    assert not (set(_PG_CDC_CHECK_IDS) & present), (
        "a MySQL source must not be shown PostgreSQL logical-replication checks"
    )


def test_every_check_id_appears_in_both_modes_for_each_engine() -> None:
    """The invariant behind all of the above, stated once.

    A check id present in one mode but not the other reads as an oversight (this is the
    D-1 rule, generalised): whichever mode the operator looks at, the same rows appear --
    only their status/reason differs. Holds per ENGINE, which is what was broken: the PG
    Full-Load report was missing all six PG ids that its own CDC report had.
    """
    for source_type in (SourceType.MYSQL, SourceType.POSTGRES):
        full = {r.check_id for r in _prereq_report(
            MigrationMode.FULL_LOAD, source_type).results}
        cdc = {r.check_id for r in _prereq_report(
            MigrationMode.CDC, source_type).results}
        assert cdc - full == set(), (
            f"{source_type.value}: CDC checks invisible in the Full-Load preview: "
            f"{sorted(i.value for i in cdc - full)}"
        )
        assert full - cdc == set(), (
            f"{source_type.value}: Full-Load rows that vanish under CDC: "
            f"{sorted(i.value for i in full - cdc)}"
        )


# --- PostgreSQL CDC gate: the ten defects found in the 0.1.498 review ---------------
#
# Every scenario below was reproduced against a real PostgreSQL 16 before being pinned
# here; the fixtures use the exact catalog codes that server produces.


def _pg_facts_healthy(**over):
    """Facts for a healthy, fully-readable PostgreSQL source."""
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

    base = dict(
        wal_level="logical",
        is_superuser=False,
        has_replication_role=True,
        max_replication_slots=10,
        used_replication_slots=1,
        max_wal_senders=10,
        used_wal_senders=1,
        is_in_recovery=False,
        max_slot_wal_keep_size_mb=-1,
        has_database_create=True,
        tables_not_owned=(),
    )
    base.update(over)
    return PostgresCdcFacts(**base)


def test_replica_identity_index_must_still_have_its_backing_index() -> None:
    """relreplident stays 'i' after the identity index is dropped; the table is then dead.

    Live PG 16: `ALTER TABLE t REPLICA IDENTITY USING INDEX u; DROP INDEX u;` leaves
    relreplident='i', and an UPDATE once t is published fails with 'does not have a
    replica identity and publishes updates'. Grading 'i' by code alone called that PASS.
    """
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity

    table = _table("app.idxdrop")
    dropped = check_replica_identity(
        table,
        _pg_facts_healthy(
            replica_identity={"app.idxdrop": "i"},
            identity_index_valid={"app.idxdrop": False},
            identity_index_is_primary={"app.idxdrop": False},
        ),
    )
    assert dropped.status is PrerequisiteStatus.FAIL, (
        "a REPLICA IDENTITY index that no longer exists must not read as usable"
    )
    assert dropped.required is True
    assert "dropped" in dropped.detail

    intact = check_replica_identity(
        table,
        _pg_facts_healthy(
            replica_identity={"app.idxdrop": "i"},
            identity_index_valid={"app.idxdrop": True},
            identity_index_is_primary={"app.idxdrop": True},
        ),
    )
    assert intact.status is PrerequisiteStatus.PASS


def test_replica_identity_on_a_non_primary_key_index_warns_about_the_null_key() -> None:
    """A non-PK identity index publishes only its own columns, so the target's key is NULL."""
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity

    result = check_replica_identity(
        _table("app.idxnonpk"),
        _pg_facts_healthy(
            replica_identity={"app.idxnonpk": "i"},
            identity_index_valid={"app.idxnonpk": True},
            identity_index_is_primary={"app.idxnonpk": False},
        ),
    )
    assert result.status is PrerequisiteStatus.WARN, "silently PASSing hides a NULL key"
    assert result.required is False, "non-blocking: the load itself is unaffected"
    assert "primary key" in result.detail


def test_partitioned_parent_is_graded_on_its_partitions_identity() -> None:
    """PostgreSQL enforces the LEAF's identity, and ALTER on the parent does not reach it.

    Live PG 16: parent REPLICA IDENTITY FULL + one leaf NOTHING -> an UPDATE through the
    parent still errors, naming the LEAF. The migration selects only the parent (the
    children are dropped from the inventory), so reading the parent's own code called this
    PASS and the gate armed a source write outage.
    """
    from dsql_migrator.core.prerequisites_postgres import check_replica_identity

    bad_leaf = check_replica_identity(
        _table("app.part"),
        _pg_facts_healthy(
            replica_identity={"app.part": "f"},  # the PARENT looks perfect
            leaf_replica_identity={"app.part": {"app.part_2026": "n"}},
        ),
    )
    assert bad_leaf.status is PrerequisiteStatus.FAIL
    assert bad_leaf.required is True
    assert "app.part_2026" in bad_leaf.detail, "the operator must be told WHICH partition"
    assert "partition" in bad_leaf.remediation.lower()

    good_leaves = check_replica_identity(
        _table("app.part"),
        _pg_facts_healthy(
            replica_identity={"app.part": "d"},
            leaf_replica_identity={"app.part": {"app.part_2026": "d"}},
        ),
    )
    assert good_leaves.status is PrerequisiteStatus.PASS


def test_unlogged_table_is_caught_before_it_aborts_the_publication() -> None:
    """An UNLOGGED table is rejected from a publication, killing the WHOLE creation."""
    from dsql_migrator.core.prerequisites_postgres import check_table_replicable

    bad = check_table_replicable(
        _table("app.scratch"), _pg_facts_healthy(unlogged={"app.scratch": True})
    )
    assert bad.status is PrerequisiteStatus.FAIL and bad.required is True
    assert "UNLOGGED" in bad.detail
    assert "SET LOGGED" in bad.remediation

    ok = check_table_replicable(
        _table("app.orders"), _pg_facts_healthy(unlogged={"app.orders": False})
    )
    assert ok.status is PrerequisiteStatus.PASS


def test_publication_creation_privilege_is_checked_separately_from_the_slot() -> None:
    """CREATE on the database + ownership of every table -- neither is REPLICATION."""
    from dsql_migrator.core.prerequisites_postgres import check_publication_privilege

    no_create = check_publication_privilege(
        _pg_facts_healthy(has_database_create=False)
    )
    assert no_create.status is PrerequisiteStatus.FAIL and no_create.required is True
    assert "CREATE on the database" in no_create.detail

    not_owner = check_publication_privilege(
        _pg_facts_healthy(tables_not_owned=("app.orders", "app.users"))
    )
    assert not_owner.status is PrerequisiteStatus.FAIL
    assert "app.orders" in not_owner.detail, "name the tables, not just 'permission denied'"

    assert check_publication_privilege(_pg_facts_healthy()).status is (
        PrerequisiteStatus.PASS
    )
    # A superuser owns everything implicitly -- no false alarm.
    assert check_publication_privilege(
        _pg_facts_healthy(is_superuser=True, has_database_create=None)
    ).status is PrerequisiteStatus.PASS
    # Unreadable -> a non-blocking INFO, never a false FAIL.
    unknown = check_publication_privilege(_pg_facts_healthy(has_database_create=None))
    assert unknown.status is PrerequisiteStatus.INFO and unknown.required is False


def test_unreadable_facts_never_produce_a_confident_green_pass() -> None:
    """A REQUIRED check must not assert a catalog value the probe never read (fail-open).

    ``is_in_recovery`` defaulted to False, so an unreadable source got a green
    "Source accepts writes (pg_is_in_recovery=false)" -- on the single fact that decides
    whether a replication slot can exist.
    """
    from dsql_migrator.core.prerequisites_postgres import (
        check_replication_role,
        check_source_is_writer,
    )

    writer = check_source_is_writer(_pg_facts_healthy(is_in_recovery=None))
    assert writer.status is PrerequisiteStatus.INFO, "unknown must not read as PASS"
    assert writer.required is False
    assert "pg_is_in_recovery=false" not in (writer.detail or "")

    role = check_replication_role(
        _pg_facts_healthy(is_superuser=None, has_replication_role=None)
    )
    assert role.status is PrerequisiteStatus.INFO and role.required is False
    # ...but a real False is still a real FAIL.
    assert check_source_is_writer(
        _pg_facts_healthy(is_in_recovery=True)
    ).status is PrerequisiteStatus.FAIL


def test_a_probe_that_read_nothing_blocks_instead_of_proceeding() -> None:
    """All-unknown facts are unverified readiness, not "partially known".

    A PostgresCdcFacts full of Nones is not None, so every check degraded to a
    non-blocking INFO and the report PROCEEDED -- the blocking
    check_postgres_cdc_facts_unavailable could never engage. Live-reproduced: a report
    that had correctly blocked on a REPLICA IDENTITY of 'nothing' flipped to
    can_proceed=True with nothing verified.
    """
    from dsql_migrator.core.prerequisites_postgres import (
        PostgresCdcFacts,
        postgres_cdc_facts_are_unverified,
    )

    assert postgres_cdc_facts_are_unverified(PostgresCdcFacts()) is True
    # One readable critical fact is enough to be "partially known", not unverified.
    assert postgres_cdc_facts_are_unverified(
        PostgresCdcFacts(wal_level="logical")
    ) is False

    checker = PrerequisiteChecker(
        source_probe=_FakeSource(cdc_facts=PostgresCdcFacts()),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    report = checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.CDC,
            tables=["app.orders"],
            source_type=SourceType.POSTGRES,
        ),
        tables=[_table("app.orders")],
    )
    assert report.can_proceed is False, (
        "CDC must not start against a source whose readiness was never verified"
    )


def test_pg_cdc_aggregate_covers_every_table_with_both_per_table_checks() -> None:
    """Each selected table gets BOTH a replicability and a REPLICA IDENTITY row."""
    from dsql_migrator.core.prerequisites_postgres import (
        check_postgres_cdc_prerequisites,
    )

    tables = [_table("app.a"), _table("app.b")]
    results = check_postgres_cdc_prerequisites(
        _pg_facts_healthy(
            replica_identity={"app.a": "d", "app.b": "d"},
            unlogged={"app.a": False, "app.b": False},
        ),
        tables,
    )
    by_id: dict = {}
    for r in results:
        by_id.setdefault(r.check_id, []).append(r.target)
    assert sorted(by_id[PrerequisiteCheckId.REPLICA_IDENTITY]) == ["app.a", "app.b"]
    assert sorted(by_id[PrerequisiteCheckId.TABLE_REPLICABLE]) == ["app.a", "app.b"]
    # And every global check appears exactly once.
    for check_id in (
        PrerequisiteCheckId.WAL_LEVEL_LOGICAL,
        PrerequisiteCheckId.REPLICATION_ROLE,
        PrerequisiteCheckId.PUBLICATION_PRIVILEGE,
        PrerequisiteCheckId.REPLICATION_SLOTS,
        PrerequisiteCheckId.SLOT_WAL_RETENTION,
        PrerequisiteCheckId.SOURCE_IS_WRITER,
    ):
        assert len(by_id[check_id]) == 1, check_id


def test_the_checker_forwards_the_selected_tables_to_the_pg_probe() -> None:
    """Without the names the probe returns no per-table facts and every row becomes INFO.

    Nothing pinned this, so dropping the argument was a silent downgrade of the only
    checks protecting the source from a write-breaking publication.
    """
    seen: list = []

    class _RecordingSource(_FakeSource):
        def cdc_prerequisites(self, table_names):
            seen.append(list(table_names))
            return _pg_facts_healthy(
                replica_identity={n: "d" for n in table_names},
                unlogged={n: False for n in table_names},
            )

    checker = PrerequisiteChecker(
        source_probe=_RecordingSource(),
        target_probe=_FakeTarget(existing={"app.orders", "app.users"}),
        msk_probe=_FakeMsk(),
    )
    checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.CDC,
            tables=["app.orders", "app.users"],
            source_type=SourceType.POSTGRES,
        ),
        tables=[_table("app.orders"), _table("app.users")],
    )
    assert seen == [["app.orders", "app.users"]], (
        f"the probe was not given the selected tables: {seen}"
    )


def test_replica_identity_and_writer_are_blocking_checks() -> None:
    """Both must be required=True: they gate whether CDC can work at all.

    Flipping either to required=False used to pass the whole suite, letting an operator
    start CDC on a table whose UPDATE/DELETE the source will refuse.
    """
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity,
        check_source_is_writer,
    )

    bad_identity = check_replica_identity(
        _table("app.t"), _pg_facts_healthy(replica_identity={"app.t": "n"})
    )
    assert bad_identity.status is PrerequisiteStatus.FAIL
    assert bad_identity.required is True

    standby = check_source_is_writer(_pg_facts_healthy(is_in_recovery=True))
    assert standby.status is PrerequisiteStatus.FAIL
    assert standby.required is True

    # ...and the report they belong to must actually refuse to proceed.
    checker = PrerequisiteChecker(
        source_probe=_FakeSource(
            cdc_facts=_pg_facts_healthy(
                replica_identity={"app.orders": "n"}, unlogged={"app.orders": False}
            )
        ),
        target_probe=_FakeTarget(existing={"app.orders"}),
        msk_probe=_FakeMsk(),
    )
    report = checker.check(
        PrerequisiteCheckRequest(
            mode=MigrationMode.CDC,
            tables=["app.orders"],
            source_type=SourceType.POSTGRES,
        ),
        tables=[_table("app.orders")],
    )
    assert report.can_proceed is False


def test_a_failed_probe_statement_does_not_blank_the_later_facts() -> None:
    """One unreadable catalog must blank only ITSELF, not everything after it.

    All the probe's statements share one connection, so one implicit transaction: without
    a rollback the first failure left it ABORTED and every later statement failed with
    25P02 and silently became None. Live-measured on PostgreSQL 16 with pg_class revoked:
    3 of 9 facts survived without the rollback, 9 of 9 with it.

    The double below reproduces exactly that server behaviour -- once a statement has
    failed, every further statement raises until ``rollback()`` is called.
    """
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    class _PoisoningConnection:
        """Fails one designated statement, then mimics PostgreSQL's aborted transaction."""

        def __init__(self, fail_on: str) -> None:
            self._fail_on = fail_on
            self._aborted = False
            self.rollbacks = 0

        def rollback(self) -> None:
            self.rollbacks += 1
            self._aborted = False

        def execute(self, statement, params=None):
            sql = str(statement)
            if self._aborted:
                raise RuntimeError(
                    "current transaction is aborted, commands ignored until end of "
                    "transaction block"
                )
            if self._fail_on in sql:
                self._aborted = True
                raise RuntimeError("permission denied")
            return _FakeResult(sql)

    class _FakeResult:
        def __init__(self, sql: str) -> None:
            self._sql = sql

        def scalar(self):
            low = self._sql.lower()
            if "wal_level" in low:
                return "logical"
            if "is_superuser" in low:
                return "off"
            if "pg_is_in_recovery" in low:
                return False
            if "max_replication_slots" in low:
                return "10"
            if "max_wal_senders" in low:
                return "10"
            if "max_slot_wal_keep_size" in low:
                return "-1"
            if "has_database_privilege" in low:
                return True
            if "count(*)" in low:
                return 0
            return None

        def fetchall(self):
            return []

    # pg_class is read EARLY (the per-table facts), so a cascade would take out
    # everything after it: the slot capacity, the writer flag, the WAL cap, the
    # database-CREATE privilege.
    connection = _PoisoningConnection(fail_on="pg_class")
    facts = PostgresSourceDialect().probe_cdc_prerequisites(
        connection, ["app.orders"]
    )

    assert connection.rollbacks >= 1, "the probe never rolled back the aborted transaction"
    # The failing fact is unknown...
    assert dict(facts.replica_identity) == {}
    # ...and every later one still read.
    for name in (
        "max_replication_slots",
        "max_wal_senders",
        "is_in_recovery",
        "max_slot_wal_keep_size_mb",
        "has_database_create",
    ):
        assert getattr(facts, name) is not None, (
            f"{name} was blanked by an unrelated statement's failure (transaction "
            "poisoning is back)"
        )


def test_walsender_count_is_unknown_rather_than_a_false_zero() -> None:
    """A non-superuser outside pg_monitor cannot see other backends' backend_type.

    Live-verified on PostgreSQL 16: a least-privilege user sees its own row only, so the
    walsender count came back 0 and the exhaustion WARN could never fire -- dead in
    exactly the setup the tool recommends. Unknown must be reported as unknown.
    """
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    class _Connection:
        def __init__(self, *, monitor: bool) -> None:
            self._monitor = monitor

        def rollback(self) -> None:
            pass

        def execute(self, statement, params=None):
            return _Res(str(statement), self._monitor)

    class _Res:
        def __init__(self, sql, monitor) -> None:
            self._sql, self._monitor = sql, monitor

        def scalar(self):
            low = self._sql.lower()
            if "is_superuser" in low:
                return "off"
            if "pg_monitor" in low:
                return self._monitor
            if "backend_type" in low:
                return 7  # what a privileged connection would see
            if "wal_level" in low:
                return "logical"
            if "max_wal_senders" in low:
                return "10"
            return None

        def fetchall(self):
            return []

    blind = PostgresSourceDialect().probe_cdc_prerequisites(
        _Connection(monitor=False), []
    )
    assert blind.used_wal_senders is None, (
        "a masked pg_stat_activity must not be reported as 0 used walsenders"
    )
    sighted = PostgresSourceDialect().probe_cdc_prerequisites(
        _Connection(monitor=True), []
    )
    assert sighted.used_wal_senders == 7
