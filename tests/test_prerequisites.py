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

from dataclasses import fields

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

    def cdc_prerequisites(self, table_names, *, publication_name="", slot_name=""):
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

    # The MySQL-only checks are not LISTED for this engine at all. They used to appear as
    # SKIP rows to satisfy a mode-symmetry rule, but symmetry is about the two MODES of one
    # engine -- and for PostgreSQL they are now absent from both, so it holds. What was left
    # was another engine's requirements on a PostgreSQL operator's screen.
    present = {r.check_id for r in report.results}
    assert PrerequisiteCheckId.BINLOG_ROW_FORMAT not in present
    assert PrerequisiteCheckId.BINLOG_RETENTION not in present
    assert PrerequisiteCheckId.GTID_MODE not in present
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
    # MySQL binlog/GTID are not applicable to this engine, so they are not listed at all.
    assert Id.BINLOG_ROW_FORMAT not in ids
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
    """D-1, superseded: the PG report now omits BINLOG_RETENTION in BOTH modes.

    D-1 originally restored it as a SKIP in the PG CDC report because a check id present in
    the weaker mode and absent in the stronger one reads as an oversight. That symmetry rule
    is about the two MODES of one engine -- and it still holds, because the id is now absent
    from both PostgreSQL modes rather than present in both. Keeping it was showing a
    PostgreSQL operator another engine's requirement; the part of D-1 that still matters is
    its second half, which this test continues to pin: the PostgreSQL EQUIVALENT of the
    retention risk (the slot's WAL being discarded, the same silent Full-Load-to-CDC gap) is
    checked as SLOT_WAL_RETENTION.
    """
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
    # Absent from BOTH PostgreSQL modes -- symmetric, and no longer another engine's row.
    assert Id.BINLOG_RETENTION not in {r.check_id for r in cdc.results}
    assert Id.BINLOG_RETENTION not in {r.check_id for r in full.results}
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
    PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
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
        # The reason is always the MODE, never the engine -- these DO apply to PostgreSQL
        # under CDC. Two rows say more than that, deliberately: the ones that depend on
        # provisioning are the only place this report can still tell the operator that the
        # gapless choice expires when the Full Load starts.
        assert result.detail.startswith("Not applicable for this mode"), (
            f"{check_id.value} DOES apply to PostgreSQL under CDC, so the reason is the "
            f"mode, not the engine: {result.detail!r}"
        )
    provisioning_dependent = {
        PrerequisiteCheckId.CDC_REPLICATION_OBJECTS,
        PrerequisiteCheckId.SLOT_WAL_RETENTION,
    }
    for check_id in _PG_CDC_CHECK_IDS:
        detail = _result(report, check_id).detail
        if check_id in provisioning_dependent:
            # A Full-load-only run creates NO slot, so the preview must say so here --
            # after the load it is too late, and this row would otherwise imply the
            # requirement is merely deferred rather than forfeited.
            assert detail != "Not applicable for this mode.", check_id.value
            assert "Full load + CDC" in detail or "replication slot" in detail
        else:
            assert detail == "Not applicable for this mode.", check_id.value
    # Still non-blocking -- a preview must never gate a Full Load.
    assert report.can_proceed is True


def test_a_postgres_source_is_never_shown_mysql_binlog_or_gtid_rows() -> None:
    """Another engine's requirements have no place on a PostgreSQL operator's screen.

    These were first mislabelled ("Not applicable for this MODE", which reads as "they WILL
    apply under CDC" -- PostgreSQL has no binary log in any mode), then relabelled to name
    the engine, and are now omitted outright. The mode-symmetry rule that kept them is about
    the two MODES of one engine, and it still holds: absent from both PostgreSQL modes. The
    PostgreSQL counterpart of the retention risk is its own check (SLOT_WAL_RETENTION), so
    nothing is lost.
    """
    mysql_only = (
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.BINLOG_RETENTION,
        PrerequisiteCheckId.GTID_MODE,
    )
    for mode in (MigrationMode.FULL_LOAD, MigrationMode.CDC):
        present = {
            r.check_id for r in _prereq_report(mode, SourceType.POSTGRES).results
        }
        for check_id in mysql_only:
            assert check_id not in present, (
                f"{mode.value}: a PostgreSQL source is still shown {check_id.value}, "
                "a requirement that can never apply to it"
            )
        # ...and the operator's OWN checks are there instead.
        assert PrerequisiteCheckId.WAL_LEVEL_LOGICAL in present, mode
        assert PrerequisiteCheckId.SLOT_WAL_RETENTION in present, mode


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
        def cdc_prerequisites(self, table_names, *, publication_name="", slot_name=""):
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


# --- CDC_REPLICATION_OBJECTS: existence, not privilege -------------------------
# "Source user can create the CDC publication" PASSES while the publication is absent --
# a privilege, not an existence, check. That is how a green pre-flight came to be followed
# by a guaranteed deploy failure, and the two need different remedies (grant vs. run the
# load / re-snapshot).


def _pg_facts(**kw):
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

    base = dict(
        checked_publication_name="dsqlmig_pub_x",
        checked_slot_name="dsqlmig_x",
        publication_present=True,
        publication_publishes_all_dml=True,
        publication_tables=("app.orders", "app.items"),
        slot_usable=True,
        slot_present_any_database=True,
    )
    base.update(kw)
    return PostgresCdcFacts(**base)


def _tabs(*names):
    from dsql_migrator.core.models import ColumnDef, TableDef

    return [
        TableDef(
            name=n,
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        )
        for n in names
    ]


def test_the_existence_check_skips_in_the_mode_that_creates_the_objects() -> None:
    """THE non-regression assertion. "Full load + CDC" creates both at the snapshot point,
    so they are SUPPOSED to be absent beforehand -- this row must never read as a problem
    in the one mode that works, or it would disable the Run button and deadlock it."""
    from dsql_migrator.core.models import PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects

    # Absent objects AND provisioning -> SKIP, not FAIL.
    result = check_cdc_replication_objects(
        _pg_facts(publication_present=False, slot_usable=False, publication_tables=()),
        _tabs("app.orders"),
        provisions_replication=True,
    )
    assert result.status is PrerequisiteStatus.SKIP
    assert "Full Load creates" in result.detail
    # required stays True so the row keeps its weight, but SKIP never gates progression.
    assert result.required is True


def test_the_existence_check_grades_absence_coverage_and_dml() -> None:
    from dsql_migrator.core.models import PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects

    def verdict(**kw):
        return check_cdc_replication_objects(
            _pg_facts(**kw), _tabs("app.orders", "app.items"),
            provisions_replication=False,
        )

    # All present -> PASS.
    assert verdict().status is PrerequisiteStatus.PASS

    # Absent -> FAIL, and the remediation must say "before deploying" (the cost) and name
    # BOTH routes, without recommending a hand-created slot.
    absent = verdict(publication_present=False, publication_tables=())
    assert absent.status is PrerequisiteStatus.FAIL and absent.required is True
    assert "BEFORE deploying" in absent.remediation
    assert "Full load + CDC" in absent.remediation
    assert "re-snapshot" in absent.remediation
    assert "CURRENT WAL position" in absent.remediation

    # A publication that omits a selected table: RUNNING but replicating nothing for it.
    gap = verdict(publication_tables=("app.orders",))
    assert gap.status is PrerequisiteStatus.FAIL
    assert "app.items" in gap.detail

    # A narrowed publish list: the omitted change types never arrive, silently.
    narrowed = verdict(publication_publishes_all_dml=False)
    assert narrowed.status is PrerequisiteStatus.FAIL
    assert "INSERT/UPDATE/DELETE" in narrowed.detail

    # A missing SLOT is only a WARN: such a start re-snapshots, so it costs a re-read, not
    # correctness. Grading it FAIL would block the one route a Full-load-only operator has.
    no_slot = verdict(slot_usable=False, slot_present_any_database=False)
    assert no_slot.status is PrerequisiteStatus.WARN
    assert no_slot.required is False
    # Both branches that lead to a re-snapshot share one cost paragraph, so the route is
    # never described two different ways depending on which object happens to be missing.
    assert "Nothing is lost" in no_slot.remediation
    assert "read from the source a second time" in no_slot.remediation
    assert "DELETED on the source" in no_slot.remediation

    # Unread facts are UNKNOWN, never absent -- an INFO that cannot gate anything.
    unknown = verdict(publication_present=None)
    assert unknown.status is PrerequisiteStatus.INFO and unknown.required is False


def test_the_existence_check_is_gated_by_the_provisioners_own_predicate() -> None:
    """The gate must come from _pg_cdc_handoff_stack, not a re-derived migration type.

    That helper is the sole decider of whether the run provisions, so reusing it makes it
    structurally impossible for the gate and the provisioner to disagree -- and it covers
    cases a `migration_type is CDC_ONLY` test would miss (MySQL; CDC already streaming,
    where the objects DO exist and the check should pass).
    """
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm)
    idx = src.index("request = PrerequisiteCheckRequest(")
    window = src[max(0, idx - 900) : idx + 500]
    assert "_pg_cdc_handoff_stack(" in window
    assert "provisions_replication=_handoff_stack is not None" in window
    # ...and NOT re-derived from the migration type, which is the drift this avoids.
    assert "provisions_replication=migration_state.migration_type" not in src


def test_the_checker_derives_the_object_names_and_forwards_the_provisioning_flag() -> None:
    """End-to-end through the checker: the two wires the pure check cannot test itself.

    Without this, the checker could pass ``provisions_replication=True`` unconditionally
    (so the row always SKIPs and the gate is dead) or never derive the names (so the probe
    reads nothing and the row is always INFO) -- and every pure-function test would still
    be green.
    """
    from dsql_migrator.core import cdc_pg_slot
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

    seen: dict = {}

    class _Probe(_FakeSource):
        def cdc_prerequisites(self, table_names, *, publication_name="", slot_name=""):
            seen["pub"], seen["slot"] = publication_name, slot_name
            base = _pg_facts_ok()
            return PostgresCdcFacts(
                **{
                    **{f.name: getattr(base, f.name) for f in fields(base)},
                    "checked_publication_name": publication_name,
                    "checked_slot_name": slot_name,
                    "publication_present": False,
                    "publication_tables": (),
                }
            )

    def _run(**kw):
        checker = PrerequisiteChecker(
            source_probe=_Probe(cdc_facts=_pg_facts_ok()),
            target_probe=_FakeTarget(existing={"app.orders"}),
            msk_probe=_FakeMsk(),
        )
        return checker.check(
            PrerequisiteCheckRequest(
                mode=MigrationMode.CDC,
                tables=["app.orders"],
                source_type=SourceType.POSTGRES,
                **kw,
            ),
            tables=[_table("app.orders")],
        )

    report = _run(provisions_replication=False, cdc_stack_name="dsql-cdc-stack")
    # The names are DERIVED from the stack, the same way dispatch_source_config does, so
    # the check cannot be about different objects than the connector will use.
    assert seen["pub"] == cdc_pg_slot.pg_publication_name("dsql-cdc-stack")
    assert seen["slot"] == cdc_pg_slot.pg_slot_name("dsql-cdc-stack")
    row = _result(report, PrerequisiteCheckId.CDC_REPLICATION_OBJECTS)
    assert row.status is PrerequisiteStatus.FAIL
    assert report.can_proceed is False  # a required FAIL gates it

    # PAIRED: the SAME facts with provisioning ON must SKIP and never gate, or the mode
    # that works would deadlock. Note the names are NOT derived in that mode either.
    seen.clear()
    ok = _run(provisions_replication=True, cdc_stack_name="dsql-cdc-stack")
    skipped = _result(ok, PrerequisiteCheckId.CDC_REPLICATION_OBJECTS)
    assert skipped.status is PrerequisiteStatus.SKIP
    assert seen == {"pub": "", "slot": ""}


def test_the_dialect_reports_an_unread_existence_fact_as_unknown_not_absent() -> None:
    """Tri-state, not a defaulted bool -- the lesson already written into these facts.

    A defaulted ``False`` here would assert an ABSENCE nobody verified, and the existence
    check blocks on absence: an under-privileged or throttled catalog read would then
    refuse a deploy whose objects are actually fine. And the names must only be asked about
    when the caller supplied them, so a provisioning run reads nothing extra.
    """
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    class _Conn:
        """Answers everything as unreadable EXCEPT the plain settings reads."""

        def __init__(self):
            self.asked: list[str] = []

        def execute(self, statement, params=None):
            sql = str(statement)
            self.asked.append(sql)
            low = sql.lower()
            if "pg_publication" in low or "pg_replication_slots" in low:
                raise RuntimeError("permission denied")
            outer = self

            class _R:
                def scalar(self):
                    return "logical" if "wal_level" in low else None

                def fetchall(self):
                    return []

                def first(self):
                    return None

            return _R()

        def rollback(self):
            return None

    conn = _Conn()
    facts = PostgresSourceDialect().probe_cdc_prerequisites(
        conn, ["app.orders"], publication_name="p1", slot_name="s1"
    )
    # UNKNOWN (None), never False -- the check turns None into a non-blocking INFO.
    assert facts.publication_present is None
    assert facts.slot_usable is None
    assert facts.publication_publishes_all_dml is None
    # The names are still recorded, so the row can say WHICH objects it could not read.
    assert facts.checked_publication_name == "p1"
    assert facts.checked_slot_name == "s1"

    # PAIRED: with NO names supplied (a provisioning run) the catalogs are not asked at all.
    conn2 = _Conn()
    facts2 = PostgresSourceDialect().probe_cdc_prerequisites(conn2, ["app.orders"])
    assert facts2.checked_publication_name == ""
    assert not any(
        "pg_publication" in s.lower() and "pg_publication_tables" not in s.lower()
        for s in conn2.asked
    )


def test_accepting_the_resnapshot_regrades_only_what_it_repairs() -> None:
    """ONE re-graded report has to clear every downstream gate, or the operator is told how
    to unblock a button that stays disabled. And it must re-grade ONLY the absences a
    re-snapshot actually repairs -- Debezium's `filtered` autocreate does not ALTER an
    existing publication, so a narrow or incomplete one stays a FAIL."""
    from dsql_migrator.core.models import PrerequisiteStatus
    from dsql_migrator.core.prerequisites_postgres import check_cdc_replication_objects

    def row(*, resnap, **kw):
        base = dict(
            checked_publication_name="p", checked_slot_name="s",
            publication_present=True, publication_publishes_all_dml=True,
            publication_tables=("app.orders",),
            slot_usable=True, slot_present_any_database=True,
        )
        base.update(kw)
        return check_cdc_replication_objects(
            _pg_facts(**base), _tabs("app.orders"),
            provisions_replication=False, cdc_start_resnapshots=resnap,
        )

    absent = dict(publication_present=False, publication_tables=())
    # Without the decision: a blocking FAIL that ADVERTISES the remedy via the discriminator.
    blocked = row(resnap=False, **absent)
    assert blocked.status is PrerequisiteStatus.FAIL
    assert blocked.required is True
    assert blocked.resolvable_by_resnapshot is True
    # With it: non-blocking (required False), so can_proceed goes True and every gate
    # un-gates -- but WARN, not INFO. This row is the only surface on the critical path to
    # the Deploy button, so it is where the second read of the source has to be disclosed;
    # INFO collapsed it under a green badge and its remediation was the EMPTY STRING, which
    # is how a "Full load only" operator reached Start CDC still believing the stream would
    # simply continue from the load. Assert the cost is stated, not just the status.
    cleared = row(resnap=True, **absent)
    assert cleared.status is PrerequisiteStatus.WARN
    assert cleared.required is False
    assert "FRESH SNAPSHOT" in cleared.detail
    assert "read from the source a second time" in cleared.remediation
    assert "DELETED on the source" in cleared.remediation
    assert '"Full load + CDC"' in cleared.remediation

    # NOT re-graded, because a re-snapshot does not repair them:
    for kw in (dict(publication_tables=()), dict(publication_publishes_all_dml=False)):
        still = row(resnap=True, **kw)
        assert still.status is PrerequisiteStatus.FAIL, kw
        assert still.resolvable_by_resnapshot is False, kw


def test_the_checker_forwards_the_resnapshot_decision() -> None:
    """The flag is useless if the checker drops it, and every pure-function test would
    still be green."""
    from dataclasses import fields

    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteCheckId,
        PrerequisiteCheckRequest,
        PrerequisiteStatus,
        SourceType,
    )
    from dsql_migrator.core.prerequisites_postgres import PostgresCdcFacts

    class _Probe(_FakeSource):
        def cdc_prerequisites(self, table_names, *, publication_name="", slot_name=""):
            base = _pg_facts_ok()
            return PostgresCdcFacts(
                **{
                    **{f.name: getattr(base, f.name) for f in fields(base)},
                    "checked_publication_name": publication_name or "p",
                    "checked_slot_name": slot_name or "s",
                    "publication_present": False,
                    "publication_tables": (),
                }
            )

    def run(resnap):
        checker = PrerequisiteChecker(
            source_probe=_Probe(cdc_facts=_pg_facts_ok()),
            target_probe=_FakeTarget(existing={"app.orders"}),
            msk_probe=_FakeMsk(),
        )
        return checker.check(
            PrerequisiteCheckRequest(
                mode=MigrationMode.CDC, tables=["app.orders"],
                source_type=SourceType.POSTGRES, provisions_replication=False,
                cdc_stack_name="dsql-cdc-stack", cdc_start_resnapshots=resnap,
            ),
            tables=[_table("app.orders")],
        )

    blocked = run(False)
    assert (
        _result(blocked, PrerequisiteCheckId.CDC_REPLICATION_OBJECTS).status
        is PrerequisiteStatus.FAIL
    )
    assert blocked.can_proceed is False
    cleared = run(True)
    row = _result(cleared, PrerequisiteCheckId.CDC_REPLICATION_OBJECTS)
    # WARN, not INFO: the re-snapshot is a real (non-blocking) cost, and WARN is also what
    # auto-expands the section so the row is READ rather than collapsed under a green badge.
    assert row.status is PrerequisiteStatus.WARN
    assert row.required is False
    # THE payoff: the whole report stops gating, which is what un-gates Deploy and Start.
    # Informing must not become blocking -- that is the line this pair of asserts holds.
    assert cleared.can_proceed is True


def test_the_dialect_grades_an_invalidated_slot_unusable() -> None:
    """Both probes must agree. Without this the prerequisite PASSES a `lost` slot while the
    runtime probe blocks it -- so the operator is told they are ready and then refused."""
    from dsql_migrator.core.source_dialect.postgres import PostgresSourceDialect

    asked: list[str] = []

    class _Conn:
        def execute(self, statement, params=None):
            asked.append(str(statement))

            class _R:
                def scalar(self_inner):
                    return None

                def fetchall(self_inner):
                    return []

                def first(self_inner):
                    return None

            return _R()

        def rollback(self):
            return None

    PostgresSourceDialect().probe_cdc_prerequisites(
        _Conn(), ["app.orders"], publication_name="p", slot_name="s"
    )
    slot_usable_sql = [
        q for q in asked if "plugin = 'pgoutput'" in q and "current_database()" in q
    ]
    assert slot_usable_sql, "the slot-usable probe did not run"
    assert any("wal_status" in q for q in slot_usable_sql)
    assert any("coalesce" in q.lower() for q in slot_usable_sql)  # NULL-safe on PG<13


def test_a_rekeyed_table_on_replica_identity_default_blocks_cdc() -> None:
    """The reported silent DELETE loss, gated before any of it can happen.

    Live-observed on Aurora PostgreSQL 17.7 at v0.1.515: one deleted `orders` row stayed on
    the DSQL target -- no error, no DLQ entry, no log line -- while the child `order_items`
    rows deleted from the same transaction replicated correctly. The controlled contrast is
    the whole diagnosis: `orders` was the ONLY table with a re-keyed record key
    (`message.key.columns=ecommerce\\.orders:user_id,id`, the target PK chosen in Schema
    Conversion), and under REPLICA IDENTITY DEFAULT PostgreSQL limits a DELETE's before-image
    to the SOURCE primary key `(id)` -- so `user_id` arrives NULL, the sink renders
    `WHERE "user_id" = NULL AND "id" = ?`, and three-valued logic matches 0 rows.

    The existing REPLICA_IDENTITY check graded this table PASS ("REPLICA IDENTITY is set for
    change replication"), because it asks a different question and accepts 'd'.
    """
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity,
        check_replica_identity_covers_key,
    )

    table = TableDef(name="ecommerce.orders", primary_key=["id"])
    facts = _pg_facts_healthy(replica_identity={"ecommerce.orders": "d"})

    # The premise: the check that already existed says this source is fine.
    assert check_replica_identity(table, facts).status is PrerequisiteStatus.PASS

    blocked = check_replica_identity_covers_key(table, facts, ["user_id", "id"])
    assert blocked is not None
    assert blocked.status is PrerequisiteStatus.FAIL
    assert blocked.required is True, "a silently-lost DELETE must block the start"
    assert blocked.target == "ecommerce.orders"
    # It must name the missing column, the consequence, and that INSERT/UPDATE are fine --
    # otherwise the operator cannot tell this from the generic identity check above.
    assert "user_id" in blocked.detail
    assert "SILENTLY LOST" in blocked.detail
    assert "UPDATE are unaffected" in blocked.detail
    # The remediation is the one statement Debezium itself documents, and it must say that
    # DEFAULT is not enough -- the neighbouring check's remedy says "the default is enough".
    assert "REPLICA IDENTITY FULL" in blocked.remediation
    assert "DEFAULT is not enough" in blocked.remediation
    assert "ecommerce.orders" in blocked.remediation


def test_replica_identity_full_satisfies_a_rekeyed_table() -> None:
    """The verification point the reporter named: after the ALTER, the start is allowed."""
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity_covers_key,
    )

    table = TableDef(name="ecommerce.orders", primary_key=["id"])
    ok = check_replica_identity_covers_key(
        table,
        _pg_facts_healthy(replica_identity={"ecommerce.orders": "f"}),
        ["user_id", "id"],
    )
    assert ok is not None and ok.status is PrerequisiteStatus.PASS
    assert "user_id" in ok.detail


def test_the_key_coverage_check_stays_silent_when_there_is_nothing_to_grade() -> None:
    """It must not add a row per table to every PostgreSQL report.

    A table with no re-key (the default, "Keep source PK") and a re-key that merely REORDERS
    the existing primary-key columns both need nothing from the source: the before-image
    already carries those columns under DEFAULT.
    """
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity_covers_key,
    )

    facts = _pg_facts_healthy(replica_identity={"app.t": "d"})
    composite = TableDef(name="app.t", primary_key=["order_id", "id"])
    # No re-key at all -- this is what order_items was, and why its deletes worked.
    assert check_replica_identity_covers_key(composite, facts, []) is None
    assert check_replica_identity_covers_key(composite, facts, ()) is None
    # A pure REORDER of the source PK: every key column is still in the before-image.
    assert (
        check_replica_identity_covers_key(composite, facts, ["id", "order_id"]) is None
    )


def test_the_key_coverage_check_grades_partitions_not_the_parent() -> None:
    """ALTER TABLE on a partitioned PARENT does not reach existing partitions.

    PostgreSQL enforces and logs the LEAF's identity (live-verified elsewhere in this
    module), so the tool's own ALTER on the parent leaves every leaf losing deletes. A parent
    reading FULL is therefore not evidence: the leaves decide.
    """
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity_covers_key,
    )

    table = TableDef(name="app.events", primary_key=["id"])
    leaky = check_replica_identity_covers_key(
        table,
        _pg_facts_healthy(
            replica_identity={"app.events": "f"},  # the parent looks fine
            leaf_replica_identity={"app.events": {"app.events_p1": "d"}},
        ),
        ["tenant_id", "id"],
    )
    assert leaky is not None and leaky.status is PrerequisiteStatus.FAIL
    assert "app.events_p1" in leaky.detail
    assert "partition" in leaky.remediation.lower()
    assert "does not propagate" in leaky.remediation

    # Every leaf FULL -> nothing to report.
    fine = check_replica_identity_covers_key(
        table,
        _pg_facts_healthy(
            replica_identity={"app.events": "f"},
            leaf_replica_identity={"app.events": {"app.events_p1": "f"}},
        ),
        ["tenant_id", "id"],
    )
    assert fine is not None and fine.status is PrerequisiteStatus.PASS


def test_an_identity_index_is_not_accepted_for_a_rekeyed_table() -> None:
    """'i' publishes only that index's columns, so the added key column can still be NULL.

    Graded FAIL rather than probed for the index's column list: FULL is the remedy either
    way, and a WARN would let the silent loss ship (the neighbouring check WARNs, which is
    right for its question and wrong for this one).
    """
    from dsql_migrator.core.prerequisites_postgres import (
        check_replica_identity_covers_key,
    )

    table = TableDef(name="app.t", primary_key=["id"])
    for is_primary in (True, False):
        r = check_replica_identity_covers_key(
            table,
            _pg_facts_healthy(
                replica_identity={"app.t": "i"},
                identity_index_valid={"app.t": True},
                identity_index_is_primary={"app.t": is_primary},
            ),
            ["tenant_id", "id"],
        )
        assert r is not None and r.status is PrerequisiteStatus.FAIL, is_primary
        assert "REPLICA IDENTITY FULL" in r.remediation

    # An unreadable identity is unknown, not absent: INFO, never a block.
    unknown = check_replica_identity_covers_key(
        table, _pg_facts_healthy(replica_identity={}), ["tenant_id", "id"]
    )
    assert unknown is not None and unknown.status is PrerequisiteStatus.INFO
    assert unknown.required is False


def test_the_rekey_map_reaches_the_aggregate_report_and_blocks_it() -> None:
    """End to end through the aggregator: the report cannot proceed, and only for that table."""
    from dsql_migrator.core.models import PrerequisiteCheckId, PrerequisiteReport
    from dsql_migrator.core.prerequisites_postgres import (
        check_postgres_cdc_prerequisites,
    )

    tables = [
        TableDef(name="ecommerce.orders", primary_key=["id"]),
        TableDef(name="ecommerce.order_items", primary_key=["order_id", "id"]),
    ]
    facts = _pg_facts_healthy(
        replica_identity={"ecommerce.orders": "d", "ecommerce.order_items": "d"}
    )

    # Without the map nothing is graded -- this is the pre-fix behaviour, and it is what
    # makes the map (not a re-derivation inside the checker) the load-bearing input.
    silent = check_postgres_cdc_prerequisites(facts, tables)
    assert not [
        r
        for r in silent
        if r.check_id is PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY
    ]

    graded = check_postgres_cdc_prerequisites(
        facts, tables, message_key_columns={"ecommerce.orders": ["user_id", "id"]}
    )
    rows = [
        r
        for r in graded
        if r.check_id is PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY
    ]
    assert len(rows) == 1, "only the re-keyed table is graded"
    assert rows[0].target == "ecommerce.orders"
    assert rows[0].status is PrerequisiteStatus.FAIL
    assert PrerequisiteReport.build(MigrationMode.CDC, graded).can_proceed is False


def test_the_checker_forwards_the_rekey_map() -> None:
    """The map is useless if the checker drops it, and every pure-function test stays green.

    Proven necessary by mutation: replacing the forward with ``message_key_columns=None``
    left all eight direct tests of the check passing, because they call the aggregate
    function rather than going through PrerequisiteCheckRequest.
    """
    from dataclasses import fields, replace

    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteCheckId,
        PrerequisiteCheckRequest,
        PrerequisiteStatus,
        SourceType,
    )

    class _Probe(_FakeSource):
        def cdc_prerequisites(self, table_names, *, publication_name="", slot_name=""):
            base = _pg_facts_ok()
            return replace(
                base, replica_identity={"app.orders": "d"}
            ) if any(f.name == "replica_identity" for f in fields(base)) else base

    def run(keys):
        checker = PrerequisiteChecker(
            source_probe=_Probe(cdc_facts=_pg_facts_ok()),
            target_probe=_FakeTarget(existing={"app.orders"}),
            msk_probe=_FakeMsk(),
        )
        return checker.check(
            PrerequisiteCheckRequest(
                mode=MigrationMode.CDC, tables=["app.orders"],
                source_type=SourceType.POSTGRES,
                message_key_columns=keys,
            ),
            tables=[_table("app.orders")],
        )

    # No re-key -> the check is absent entirely (no noise for the common case).
    assert not [
        r
        for r in run({}).results
        if r.check_id is PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY
    ]

    # Re-keyed -> the row is there, it FAILs, and the whole report is blocked.
    report = run({"app.orders": ["user_id", "id"]})
    row = _result(report, PrerequisiteCheckId.REPLICA_IDENTITY_COVERS_KEY)
    assert row.status is PrerequisiteStatus.FAIL
    assert report.can_proceed is False


def test_the_request_accepts_the_rekey_map_at_all() -> None:
    """PrerequisiteCheckRequest is extra="forbid", so an undeclared field is a hard error."""
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteCheckRequest,
        SourceType,
    )

    req = PrerequisiteCheckRequest(
        mode=MigrationMode.CDC,
        tables=["app.orders"],
        source_type=SourceType.POSTGRES,
        message_key_columns={"app.orders": ["user_id", "id"]},
    )
    assert req.message_key_columns == {"app.orders": ["user_id", "id"]}
    # Default is empty, so every existing caller keeps its current behaviour.
    assert PrerequisiteCheckRequest(mode=MigrationMode.CDC).message_key_columns == {}


# ---------------------------------------------------------------------------
# "Check the largest value in each column" -- now the tool can
# ---------------------------------------------------------------------------


def _media_table(name: str = "ecommerce.product_media") -> TableDef:
    return TableDef(
        name=name,
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),
            ColumnDef(name="content", mysql_type="bytea"),
            ColumnDef(name="notes", mysql_type="text"),
        ],
        primary_key=["id"],
    )


def test_a_table_with_no_lob_column_adds_no_row() -> None:
    from dsql_migrator.core.prerequisites import check_source_value_size

    assert check_source_value_size(_media_table(), (), {}) is None


def test_an_oversized_value_is_reported_as_already_present() -> None:
    """The finding said "check the largest value in each column" and nothing could.

    So the one concrete fact behind an oversized-LOB warning -- will it actually bite? --
    was learned only when Full Load quarantined the row.
    """
    from dsql_migrator.core.prerequisites import check_source_value_size

    r = check_source_value_size(
        _media_table(), ("content", "notes"), {"content": True, "notes": False}
    )
    assert r is not None
    assert r.status is PrerequisiteStatus.WARN
    # NEVER blocking: an oversized value is the operator's decision, not a stop.
    assert r.required is False
    assert "`content`" in r.detail and "`notes`" not in r.detail, r.detail
    assert "CANNOT be stored" in r.detail, r.detail
    assert "reloading cannot fix it" in r.detail, r.detail
    # The remedies, including the one the tool itself offers.
    assert "exclude the column on the Data Migration step" in r.remediation
    assert "Amazon S3" in r.remediation
    # Honest about what was measured: an upper bound, because DSQL's limit is on the
    # COMPRESSED size for text/json.
    assert "uncompressed" in r.remediation, r.remediation


def test_no_oversized_value_passes_without_alarming() -> None:
    from dsql_migrator.core.prerequisites import check_source_value_size

    r = check_source_value_size(
        _media_table(), ("content",), {"content": False}
    )
    assert r is not None
    assert r.status is PrerequisiteStatus.PASS
    assert r.required is False
    assert "No value" in r.detail


def test_an_undetermined_probe_is_never_reported_as_a_pass() -> None:
    """An unverifiable ceiling shown as verified is the defect, not the fix.

    Proving the ABSENCE of an oversized value needs a full pass over the column, which is
    exactly the source scan this project avoids -- so the probe is allowed to time out, and
    a timeout must read as "not determined".
    """
    from dsql_migrator.core.prerequisites import check_source_value_size

    r = check_source_value_size(_media_table(), ("content",), None)
    assert r is not None
    assert r.status is PrerequisiteStatus.INFO, r.status
    assert r.required is False
    assert "Not determined" in r.detail, r.detail
    assert "timed out" in r.detail, r.detail
    # Gives the operator the query, so "check it yourself" is actionable.
    assert "octet_length" in r.remediation, r.remediation


def test_the_checker_asks_only_about_columns_the_caller_named() -> None:
    """The at-risk columns come from the ONE helper that drives the finding and the picker.

    Core cannot import it (it lives in the UI layer), so it is a parameter -- and omitting
    it must mean the question is simply not asked, never that it silently passed.
    """
    from dsql_migrator.core.models import PrerequisiteCheckId

    table = _media_table()
    asked: list[tuple] = []

    class _Probe(_FakeSource):
        def oversized_values(self, table_name, columns, *, limit_bytes, timeout_seconds):
            asked.append((table_name, tuple(columns), limit_bytes))
            return {"content": True}

    checker = PrerequisiteChecker(source_probe=_Probe(), target_probe=_FakeTarget())
    # Not supplied -> no size row at all.
    report = checker.check(PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=[table.name]), tables=[table])
    assert not [
        r for r in report.results
        if r.check_id is PrerequisiteCheckId.SOURCE_VALUE_SIZE
    ]
    assert asked == []

    # Supplied -> exactly those columns, with DSQL's documented limit.
    report = checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=[table.name]),
        tables=[table],
        lob_columns={table.name: ("content", "notes")},
    )
    rows = [
        r for r in report.results
        if r.check_id is PrerequisiteCheckId.SOURCE_VALUE_SIZE
    ]
    assert len(rows) == 1 and rows[0].status is PrerequisiteStatus.WARN
    assert asked == [(table.name, ("content", "notes"), 1024 * 1024)]


def test_an_already_excluded_column_is_not_asked_about() -> None:
    # Excluding the column IS the remedy this check recommends, so warning about its size
    # afterwards is noise.
    table = _media_table()
    asked: list[tuple] = []

    class _Probe(_FakeSource):
        def oversized_values(self, table_name, columns, *, limit_bytes, timeout_seconds):
            asked.append(tuple(columns))
            return {name: False for name in columns}

    checker = PrerequisiteChecker(source_probe=_Probe(), target_probe=_FakeTarget())
    checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=[table.name]),
        tables=[table],
        lob_columns={table.name: ("content", "notes")},
        excluded_columns={table.name: ["content"]},
    )
    assert asked == [("notes",)]


def test_a_probe_that_raises_degrades_to_not_determined() -> None:
    """Advisory: this must never break the gate, and must never fake a pass."""
    from dsql_migrator.core.models import PrerequisiteCheckId

    table = _media_table()

    class _Probe(_FakeSource):
        def oversized_values(self, *_a, **_k):
            raise RuntimeError("read timed out")

    checker = PrerequisiteChecker(source_probe=_Probe(), target_probe=_FakeTarget())
    report = checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=[table.name]), tables=[table], lob_columns={table.name: ("content",)}
    )
    row = next(
        r for r in report.results
        if r.check_id is PrerequisiteCheckId.SOURCE_VALUE_SIZE
    )
    assert row.status is PrerequisiteStatus.INFO
    # Advisory: it can never be the reason the gate closes (other stub-driven checks in
    # this synthetic report may fail; this row must not be among the blockers).
    assert row.required is False
    assert not [
        r for r in report.results
        if r.required and r.status is PrerequisiteStatus.FAIL
        and r.check_id is PrerequisiteCheckId.SOURCE_VALUE_SIZE
    ]


def test_a_probe_without_the_method_still_works() -> None:
    # Every probe written before this check (and every existing test fake) must keep
    # working -- the method is resolved through getattr for exactly that reason.
    from dsql_migrator.core.models import PrerequisiteCheckId

    table = _media_table()
    checker = PrerequisiteChecker(source_probe=_FakeSource(), target_probe=_FakeTarget())
    report = checker.check(
        PrerequisiteCheckRequest(mode=MigrationMode.FULL_LOAD, tables=[table.name]), tables=[table], lob_columns={table.name: ("content",)}
    )
    row = next(
        r for r in report.results
        if r.check_id is PrerequisiteCheckId.SOURCE_VALUE_SIZE
    )
    assert row.status is PrerequisiteStatus.INFO
    assert row.required is False


def test_the_value_size_ceiling_is_per_type_not_a_flat_1_mib() -> None:
    """A flat 1 MiB overstated an unbounded varchar's headroom 16-fold.

    Every figure here is from the Aurora DSQL "Supported data types" page AND re-verified
    live with incompressible values: varchar rejects 65536 bytes ("datatype limit greater
    than 65535 bytes not supported for varchar"), char(4096) rejects 4097 ("value too long
    for type character(4096)"), text and bytea reject 1048577.
    """
    from dsql_migrator.core.prerequisites import (
        DSQL_MAX_VALUE_BYTES,
        dsql_value_limit_bytes,
        dsql_value_limit_is_compressed,
    )

    assert DSQL_MAX_VALUE_BYTES == 1024 * 1024
    for column_type, expected in (
        ("text", 1024 * 1024),
        ("bytea", 1024 * 1024),
        ("json", 1024 * 1024),
        ("jsonb", 1024 * 1024),
        ("character varying", 65535),
        ("character varying(80)", 65535),
        ("varchar", 65535),
        ("character(10)", 4096),
        ("char", 4096),
        ("bpchar(5)", 4096),
        ("longtext", 1024 * 1024),
        ("longblob", 1024 * 1024),
        # An unknown type falls back to the database-wide per-column maximum, never lower
        # (reporting a limit BELOW the truth would invent a problem).
        ("inet", 1024 * 1024),
    ):
        assert dsql_value_limit_bytes(column_type) == expected, column_type

    # Compression is keyed on the TARGET type: DSQL compresses text/varchar/bpchar and
    # json/jsonb (outside a key) but NOT bytea, so a bytea finding must not offer headroom.
    for column_type in ("text", "character varying", "char(8)", "jsonb", "longtext"):
        assert dsql_value_limit_is_compressed(column_type) is True, column_type
    for column_type in ("bytea", "longblob", "blob"):
        assert dsql_value_limit_is_compressed(column_type) is False, column_type


def test_an_unmeasured_lob_column_is_never_reported_inside_a_pass() -> None:
    """A column the probe could not measure used to be counted as "did not exceed".

    That was reachable in normal PostgreSQL use -- ``octet_length(jsonb)`` does not exist,
    so the jsonb column's statement failed -- and produced a green "No value exceeds ..."
    naming a column nothing had measured.
    """
    from dsql_migrator.core.models import ColumnDef, TableDef
    from dsql_migrator.core.prerequisites import (
        PrerequisiteStatus,
        check_source_value_size,
    )

    table = TableDef(
        name="ecommerce.docs",
        columns=[
            ColumnDef(name="id", mysql_type="bigint", nullable=False),
            ColumnDef(name="notes", mysql_type="character varying"),
            ColumnDef(name="payload", mysql_type="jsonb"),
        ],
        primary_key=["id"],
    )
    columns = ["notes", "payload"]

    everything_measured = check_source_value_size(
        table, columns, {"notes": False, "payload": False}
    )
    assert everything_measured is not None
    assert everything_measured.status is PrerequisiteStatus.PASS
    # The PASS states each column's OWN ceiling, so it cannot be read as a flat 1 MiB.
    assert "`notes` (65,535 bytes)" in everything_measured.detail
    assert "`payload` (1 MiB)" in everything_measured.detail

    one_unmeasured = check_source_value_size(table, columns, {"notes": False})
    assert one_unmeasured is not None
    assert one_unmeasured.status is PrerequisiteStatus.INFO, one_unmeasured.status
    assert "Not determined for `payload` (1 MiB)" in one_unmeasured.detail
    # The remedy must work: octet_length has no jsonb overload, so it says to cast.
    assert "casting json/jsonb to text" in one_unmeasured.remediation

    # An actual offender still WARNs, and still discloses what went unmeasured.
    mixed = check_source_value_size(table, columns, {"notes": True})
    assert mixed is not None
    assert mixed.status is PrerequisiteStatus.WARN
    assert "`notes` (65,535 bytes) already holds" in mixed.detail
    assert "Not determined for `payload`" in mixed.detail
