# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the Schema Applier / DDL executor (task 15.2).

Covers (Requirements 10.4, 10.5, 10.6, 10.7, 10.8, Properties 12, 1, 2, 5):

- a new object is created and reported ``CREATED`` (Requirement 10.4, 10.7),
- an existing object under ``SKIP_IF_EXISTS`` is reported ``SKIPPED`` and the
  target is never written (Requirement 10.4),
- a destructive ``REPLACE`` is refused with ``FAILED`` unless explicitly
  confirmed, and performed (DROP then CREATE) when confirmed (Requirement 10.6),
- an OC001 (``SQLSTATE 40001``) schema conflict is retried idempotently and
  eventually succeeds (Requirement 10.5 / Property 5),
- a target error is captured as a ``FAILED`` result with a reason (Req 10.7),
- each DDL runs as its own single statement / transaction with DDL/DML
  separation (Property 2),
- ``preview`` pairs source/target DDL and reports existence (Requirement 10.2/10.3),
- the applier never accesses a source database -- it has no source handle and
  only ever writes to the injected target connection (Property 1 / Requirement
  10.8).

All tests inject a fake connection/cursor and a stub existence oracle, so no
live DSQL cluster (and no AWS call) is required.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from dsql_migrator.core.models import (
    ApplyMode,
    ApplyResult,
    ApplyStatus,
    DdlPreview,
    TargetConnectionConfig,
)
from dsql_migrator.core.occ import OCC_SQLSTATE
from dsql_migrator.core.schema_applier import (
    SchemaApplier,
    SchemaApplyError,
    parse_create_object,
    recreate_table,
)


# ---------------------------------------------------------------------------
# Fakes / helpers
# ---------------------------------------------------------------------------


class _FakeSerializationFailure(Exception):
    """A fake psycopg-like error exposing ``sqlstate`` (simulates 40001)."""

    def __init__(self, sqlstate: str = OCC_SQLSTATE) -> None:
        super().__init__("serialization failure")
        self.sqlstate = sqlstate


class _FakeCursor:
    """A cursor that records each executed statement on its connection."""

    def __init__(self, connection: "_FakeConnection") -> None:
        self._connection = connection
        self.closed = False

    def execute(self, statement: Any, params: Optional[list[object]] = None) -> None:
        self._connection.handle_execute(statement, params)

    def close(self) -> None:
        self.closed = True
        self._connection.closed_cursors += 1


class _FakeConnection:
    """A fake autocommit DSQL connection recording the DDL it executes.

    ``failures`` is a queue of SQLSTATEs to raise on the next execute(s), used to
    simulate OC001 conflicts. Each successful execute is rendered to text and
    appended to ``executed`` so tests can assert on statement content and order.
    """

    def __init__(self, *, failures: Optional[list[str]] = None) -> None:
        self.autocommit = True
        self.executed: list[str] = []
        self.closed = False
        self.closed_cursors = 0
        self._failures = list(failures or [])

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    def handle_execute(self, statement: Any, _params: Optional[list[object]]) -> None:
        if self._failures:
            raise _FakeSerializationFailure(sqlstate=self._failures.pop(0))
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self.executed.append(text)

    def close(self) -> None:
        self.closed = True


class _StubOracle:
    """A stub existence oracle standing in for a browsed TargetIntrospector."""

    def __init__(self, existing: Optional[set[str]] = None) -> None:
        self._existing = {name.lower() for name in (existing or set())}
        self.queried: list[str] = []

    def object_exists(self, name: str) -> bool:
        self.queried.append(name)
        return name.lower() in self._existing


class _SleepRecorder:
    """An injectable sleep function recording the delays it was asked for."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.delays.append(seconds)


def _no_sleep(_seconds: float) -> None:
    return None


def _zero_jitter() -> float:
    return 0.0


def _applier(
    *,
    existing: Optional[set[str]] = None,
    connection: Optional[_FakeConnection] = None,
    sleep=_no_sleep,
    occ_max_attempts: int = 5,
) -> tuple[SchemaApplier, _FakeConnection, _StubOracle]:
    connection = connection or _FakeConnection()
    oracle = _StubOracle(existing)
    applier = SchemaApplier(
        oracle,
        connection_factory=lambda: connection,
        occ_max_attempts=occ_max_attempts,
        occ_base_delay=0.0,
        sleep=sleep,
        jitter=_zero_jitter,
    )
    return applier, connection, oracle


_CREATE_ORDERS = 'CREATE TABLE "orders" ("id" uuid PRIMARY KEY, "total" numeric)'


# ---------------------------------------------------------------------------
# Object-identity parsing (derived from the DDL)
# ---------------------------------------------------------------------------


def test_parse_create_table_name_and_kind() -> None:
    assert parse_create_object(_CREATE_ORDERS) == ("orders", "TABLE")


def test_parse_create_table_schema_qualified_and_quoted() -> None:
    name, kind = parse_create_object('CREATE TABLE "app"."customers" ("id" uuid)')
    assert name == "app.customers"
    assert kind == "TABLE"


def test_parse_create_view_and_index_async() -> None:
    assert parse_create_object("CREATE VIEW v_active AS SELECT 1") == (
        "v_active",
        "VIEW",
    )
    # The DSQL-specific ASYNC index modifier is tolerated.
    assert parse_create_object('CREATE INDEX ASYNC "ix_total" ON "orders" ("total")') == (
        "ix_total",
        "INDEX",
    )
    assert parse_create_object(
        'CREATE UNIQUE INDEX ASYNC "ix_email" ON "users" ("email")'
    ) == ("ix_email", "INDEX")


def test_parse_create_schema() -> None:
    name, kind = parse_create_object('CREATE SCHEMA IF NOT EXISTS "app"')
    assert name == "app"
    assert kind == "SCHEMA"


def test_parse_rejects_non_create_statement() -> None:
    with pytest.raises(SchemaApplyError):
        parse_create_object("DROP TABLE orders")
    with pytest.raises(SchemaApplyError):
        parse_create_object("SELECT 1")


# ---------------------------------------------------------------------------
# apply — CREATED on a new object (Requirement 10.4, 10.7)
# ---------------------------------------------------------------------------


def test_apply_creates_new_object() -> None:
    applier, connection, oracle = _applier(existing=set())

    result = applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)

    assert isinstance(result, ApplyResult)
    assert result.object_name == "orders"
    assert result.status is ApplyStatus.CREATED
    # Exactly the CREATE statement was executed against the target.
    assert connection.executed == [_CREATE_ORDERS]
    # Existence was checked before applying (Requirement 10.3).
    assert oracle.queried == ["orders"]


def test_apply_creates_new_object_under_replace_without_confirmation() -> None:
    # REPLACE on a NON-existing object needs no confirmation: it is just a create.
    applier, connection, _ = _applier(existing=set())

    result = applier.apply(_CREATE_ORDERS, ApplyMode.REPLACE)

    assert result.status is ApplyStatus.CREATED
    assert connection.executed == [_CREATE_ORDERS]


def test_apply_create_schema_executes_without_existence_check() -> None:
    # CREATE SCHEMA is idempotent and not tracked by the existence oracle, so it
    # is executed directly and reported CREATED with no existence query.
    applier, connection, oracle = _applier(existing=set())

    result = applier.apply(
        'CREATE SCHEMA IF NOT EXISTS "app"', ApplyMode.SKIP_IF_EXISTS
    )

    assert result.status is ApplyStatus.CREATED
    assert result.object_name == "app"
    assert connection.executed == ['CREATE SCHEMA IF NOT EXISTS "app"']
    # No existence check is performed for a schema.
    assert oracle.queried == []


def test_apply_create_schema_self_heals_duplicate() -> None:
    # A 42P07 duplicate-object race on CREATE SCHEMA is absorbed as CREATED (the
    # schema is present), matching the table/view/index self-heal path -- not a
    # spurious FAILED.
    connection = _FakeConnection(failures=["42P07"])
    applier, _, _ = _applier(existing=set(), connection=connection)

    result = applier.apply(
        'CREATE SCHEMA IF NOT EXISTS "app"', ApplyMode.SKIP_IF_EXISTS
    )

    assert result.status is ApplyStatus.CREATED
    assert result.object_name == "app"


# ---------------------------------------------------------------------------
# apply — SKIPPED when the object exists (Requirement 10.4)
# ---------------------------------------------------------------------------


def test_apply_skips_existing_object_under_skip_if_exists() -> None:
    applier, connection, _ = _applier(existing={"orders"})

    result = applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)

    assert result.status is ApplyStatus.SKIPPED
    assert result.object_name == "orders"
    # The target was never written when skipping.
    assert connection.executed == []
    assert connection.closed is False  # no connection was even opened


def test_apply_skips_existing_index_under_skip_if_exists() -> None:
    """A CREATE INDEX whose index already exists is skipped, not failed.

    Regression: when the index is reported as existing on the target, the
    applier must not attempt the CREATE (which would fail with
    ``relation "..." already exists``) and instead report SKIPPED.
    """
    applier, connection, _ = _applier(existing={"idx_cat_parent"})

    result = applier.apply(
        'CREATE INDEX ASYNC "idx_cat_parent" ON "categories" ("parent_id")',
        ApplyMode.SKIP_IF_EXISTS,
    )

    assert result.status is ApplyStatus.SKIPPED
    assert result.object_name == "idx_cat_parent"
    assert connection.executed == []
    assert connection.closed is False


def test_apply_self_heals_duplicate_under_skip_if_exists() -> None:
    """A stale snapshot is self-healed: 42P07 'already exists' -> SKIPPED.

    The oracle reports the object absent, but the target raises ``42P07`` because
    it already exists (e.g. created by an earlier partial apply). Under
    SKIP_IF_EXISTS this is absorbed as SKIPPED so a re-apply converges.
    """
    connection = _FakeConnection(failures=["42P07"])
    applier, _, _ = _applier(existing=set(), connection=connection)

    result = applier.apply(
        'CREATE INDEX ASYNC "idx_cat_parent" ON "categories" ("parent_id")',
        ApplyMode.SKIP_IF_EXISTS,
    )

    assert result.status is ApplyStatus.SKIPPED
    assert result.object_name == "idx_cat_parent"


def test_apply_does_not_self_heal_duplicate_under_replace() -> None:
    """REPLACE does not absorb a 42P07: it did not drop first, so it FAILs."""
    connection = _FakeConnection(failures=["42P07"])
    applier, _, _ = _applier(existing=set(), connection=connection)

    result = applier.apply(
        'CREATE INDEX ASYNC "idx_cat_parent" ON "categories" ("parent_id")',
        ApplyMode.REPLACE,
        confirmed=True,
    )

    assert result.status is ApplyStatus.FAILED
    assert "Apply failed" in result.detail


# ---------------------------------------------------------------------------
# apply — destructive REPLACE confirmation gate (Requirement 10.6)
# ---------------------------------------------------------------------------


def test_replace_existing_is_refused_without_confirmation() -> None:
    applier, connection, _ = _applier(existing={"orders"})

    result = applier.apply(_CREATE_ORDERS, ApplyMode.REPLACE)

    assert result.status is ApplyStatus.FAILED
    assert "confirm" in result.detail.lower()
    # Nothing destructive was executed.
    assert connection.executed == []


def test_replace_existing_drops_then_creates_when_confirmed() -> None:
    applier, connection, _ = _applier(existing={"orders"})

    result = applier.apply(_CREATE_ORDERS, ApplyMode.REPLACE, confirmed=True)

    assert result.status is ApplyStatus.CREATED
    # DROP runs first, then the CREATE -- two separate single-DDL statements.
    assert len(connection.executed) == 2
    drop_statement, create_statement = connection.executed
    assert drop_statement == 'DROP TABLE IF EXISTS "orders"'
    assert create_statement == _CREATE_ORDERS


def test_replace_drop_uses_object_kind_for_a_view() -> None:
    applier, connection, _ = _applier(existing={"v_active"})

    applier.apply("CREATE VIEW v_active AS SELECT 1", ApplyMode.REPLACE, confirmed=True)

    assert connection.executed[0] == 'DROP VIEW IF EXISTS "v_active"'


def test_drop_issues_drop_if_exists_for_the_objects_kind() -> None:
    # The REPLACE pre-pass drops a dependent view (idempotent DROP ... IF EXISTS)
    # before the table it selects from is recreated; no existence check is needed.
    applier, connection, oracle = _applier()

    applier.drop("CREATE VIEW v_active AS SELECT 1")

    assert connection.executed == ['DROP VIEW IF EXISTS "v_active"']
    assert oracle.queried == []  # drop does not consult existence; IF EXISTS is enough
    assert connection.closed is True


# ---------------------------------------------------------------------------
# apply — OC001 idempotent retry (Requirement 10.5 / Property 5)
# ---------------------------------------------------------------------------


def test_oc001_conflict_is_retried_then_succeeds() -> None:
    connection = _FakeConnection(failures=[OCC_SQLSTATE])  # first execute conflicts
    sleeper = _SleepRecorder()
    applier, _, _ = _applier(
        existing=set(), connection=connection, sleep=sleeper, occ_max_attempts=5
    )

    result = applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)

    assert result.status is ApplyStatus.CREATED
    # One backoff happened before the retry; the retried CREATE then committed.
    assert len(sleeper.delays) == 1
    assert connection.executed == [_CREATE_ORDERS]


def test_exhausted_oc001_conflict_is_reported_as_failed() -> None:
    connection = _FakeConnection(failures=[OCC_SQLSTATE])
    # No retry budget: the single conflict fails the apply.
    applier, _, _ = _applier(
        existing=set(), connection=connection, occ_max_attempts=1
    )

    result = applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)

    assert result.status is ApplyStatus.FAILED
    assert result.object_name == "orders"
    assert connection.executed == []  # nothing committed


# ---------------------------------------------------------------------------
# apply — generic target failure captured as FAILED (Requirement 10.7)
# ---------------------------------------------------------------------------


class _RaisingConnection:
    """A connection whose cursor raises a non-OCC error on execute."""

    def __init__(self) -> None:
        self.closed = False

    def cursor(self) -> "_RaisingConnection":
        return self

    def execute(self, _statement: Any, _params: object = None) -> None:
        raise RuntimeError("relation already exists")

    def close(self) -> None:
        self.closed = True


def test_target_error_is_captured_as_failed_with_reason() -> None:
    oracle = _StubOracle(set())
    connection = _RaisingConnection()
    applier = SchemaApplier(oracle, connection_factory=lambda: connection)

    result = applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)

    assert result.status is ApplyStatus.FAILED
    assert "relation already exists" in result.detail
    # The connection was still closed despite the failure.
    assert connection.closed is True


# ---------------------------------------------------------------------------
# Property 2 — single-DDL transactions, DDL/DML separation
# ---------------------------------------------------------------------------


def test_each_executed_statement_is_a_single_ddl() -> None:
    applier, connection, _ = _applier(existing={"orders"})

    applier.apply(_CREATE_ORDERS, ApplyMode.REPLACE, confirmed=True)

    # No statement bundles multiple statements with a semicolon separator.
    for statement in connection.executed:
        assert statement.rstrip().rstrip(";").count(";") == 0
    # The DROP and the CREATE were issued as two distinct executes, not one.
    assert len(connection.executed) == 2


def test_connection_uses_autocommit() -> None:
    applier, connection, _ = _applier(existing=set())
    applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)
    # Each execute is its own transaction (DSQL autocommit / single-DDL rule).
    assert connection.autocommit is True


# ---------------------------------------------------------------------------
# preview — source vs. target DDL + existence (Requirement 10.2/10.3)
# ---------------------------------------------------------------------------


def test_preview_reports_pair_and_existence() -> None:
    applier, _, oracle = _applier(existing={"orders"})

    preview = applier.preview("CREATE TABLE `orders` (...)", _CREATE_ORDERS)

    assert isinstance(preview, DdlPreview)
    assert preview.object_name == "orders"
    assert preview.source_ddl == "CREATE TABLE `orders` (...)"
    assert preview.target_ddl == _CREATE_ORDERS
    assert preview.exists is True
    assert oracle.queried == ["orders"]


def test_preview_reports_absent_object() -> None:
    applier, _, _ = _applier(existing=set())
    preview = applier.preview("", _CREATE_ORDERS)
    assert preview.exists is False


# ---------------------------------------------------------------------------
# Property 1 / Requirement 10.8 — the applier never accesses a source
# ---------------------------------------------------------------------------


def test_applier_has_no_source_handle() -> None:
    """The applier only writes to the target; it holds no source connection."""
    applier, _, _ = _applier(existing=set())
    # No attribute on the applier references a source database.
    attribute_names = " ".join(vars(applier).keys()).lower()
    assert "source" not in attribute_names


def test_apply_only_touches_the_injected_target_connection() -> None:
    """Apply opens exactly the target connection and writes only DDL to it."""
    opened: list[_FakeConnection] = []

    def factory() -> _FakeConnection:
        connection = _FakeConnection()
        opened.append(connection)
        return connection

    oracle = _StubOracle(set())
    applier = SchemaApplier(oracle, connection_factory=factory)

    applier.apply(_CREATE_ORDERS, ApplyMode.SKIP_IF_EXISTS)

    # Exactly one (target) connection was opened, closed, and only DDL ran on it.
    assert len(opened) == 1
    assert opened[0].executed == [_CREATE_ORDERS]
    assert opened[0].closed is True


# ---------------------------------------------------------------------------
# Construction guards
# ---------------------------------------------------------------------------


def test_requires_connection_factory_or_target() -> None:
    with pytest.raises(SchemaApplyError):
        SchemaApplier(_StubOracle(set()))


def test_target_builds_a_default_connection_factory_without_connecting() -> None:
    target = TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )
    # Construction must succeed without opening a connection or reaching AWS.
    applier = SchemaApplier(_StubOracle(set()), target=target)
    assert isinstance(applier, SchemaApplier)


# ---------------------------------------------------------------------------
# recreate_table: DROP IF EXISTS + CREATE for the Full Load "replace" path
# ---------------------------------------------------------------------------


def test_recreate_table_ensures_schema_then_drops_and_creates() -> None:
    connection = _FakeConnection()

    recreate_table(
        ['CREATE SCHEMA IF NOT EXISTS "app"'],
        'CREATE TABLE "app"."orders" ("id" integer PRIMARY KEY)',
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )

    # Order matters: ensure schema, drop the existing table idempotently, then
    # recreate it empty. Each is its own statement (DDL separation).
    assert connection.executed == [
        'CREATE SCHEMA IF NOT EXISTS "app"',
        'DROP TABLE IF EXISTS "app"."orders"',
        'CREATE TABLE "app"."orders" ("id" integer PRIMARY KEY)',
    ]
    assert connection.closed is True


def test_recreate_table_retries_drop_on_occ_conflict() -> None:
    # First execute (the schema ensure) hits an OC001 and is retried; the load
    # still converges to the full DROP+CREATE sequence (idempotent retry).
    connection = _FakeConnection(failures=["40001"])

    recreate_table(
        [],
        'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )

    assert connection.executed == [
        'DROP TABLE IF EXISTS "t"',
        'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
    ]


class _TransientConnectError(Exception):
    """A psycopg-like connection error with NO sqlstate (server never answered).

    Mirrors ``ConnectionTimeout: connection timeout expired`` -- what DSQL's
    new-connection rate limit surfaces under a connect storm. ``sqlstate`` is
    ``None`` so it is classified transient by exception name, not by message.
    """

    sqlstate = None


def test_recreate_table_retries_the_connection_open_on_transient_failure() -> None:
    # Regression: the per-table DROP+recreate connect used to run OUTSIDE any retry,
    # so a transient ConnectionTimeout at the front-16 -> next-wave transition (many
    # workers opening fresh connections at once, tripping DSQL's new-connection rate
    # limit) failed the table with 0 rows loaded. The connect open is now retried.
    good = _FakeConnection()
    attempts = {"n": 0}

    def flaky_factory() -> _FakeConnection:
        attempts["n"] += 1
        if attempts["n"] <= 2:  # first two connects storm-fail, third succeeds
            raise _TransientConnectError("connection timeout expired")
        return good

    recreate_table(
        [],
        'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
        connection_factory=flaky_factory,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )

    assert attempts["n"] == 3  # two transient failures were retried, then succeeded
    assert good.executed == [
        'DROP TABLE IF EXISTS "t"',
        'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
    ]


def test_recreate_table_connect_gives_up_after_max_attempts() -> None:
    # A connect that never recovers still surfaces (bounded by the OCC budget) --
    # the retry is not infinite.
    attempts = {"n": 0}

    def always_fails() -> _FakeConnection:
        attempts["n"] += 1
        raise _TransientConnectError("connection timeout expired")

    with pytest.raises(_TransientConnectError):
        recreate_table(
            [],
            'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
            connection_factory=always_fails,
            occ_max_attempts=3,
            sleep=_no_sleep,
            jitter=_zero_jitter,
        )
    assert attempts["n"] == 3  # exactly the budget, then re-raised


class _SchemaLimitError(Exception):
    """A psycopg-like ``program_limit_exceeded`` (SQLSTATE 54000) for DSQL's
    10-schema cap: ``CREATE SCHEMA`` beyond the limit ("more than 10 schemas
    not allowed")."""

    def __init__(self, message: str = "more than 10 schemas not allowed",
                 sqlstate: str = "54000") -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class _SchemaLimitConnection(_FakeConnection):
    """A fake connection that raises the schema-limit error on ``CREATE SCHEMA``."""

    def handle_execute(self, statement: Any, _params) -> None:  # type: ignore[override]
        text = statement if isinstance(statement, str) else statement.as_string(None)
        if text.upper().startswith("CREATE SCHEMA"):
            raise _SchemaLimitError()
        self.executed.append(text)


def test_is_schema_limit_exceeded_detects_54000_with_schema_message() -> None:
    from dsql_migrator.core.schema_applier import _is_schema_limit_exceeded

    assert _is_schema_limit_exceeded(_SchemaLimitError()) is True
    # 54000 covers other program limits too; only schema-mentioning ones match.
    assert _is_schema_limit_exceeded(
        _SchemaLimitError(message="target lists can have at most 1664 entries")
    ) is False
    # Message fallback when the SQLSTATE was lost (wrapped/re-raised).
    assert _is_schema_limit_exceeded(
        _SchemaLimitError(message="ERROR: more than 10 schemas not allowed", sqlstate=None)
    ) is True
    assert _is_schema_limit_exceeded(RuntimeError("boom")) is False


def test_recreate_table_translates_schema_limit_to_actionable_error() -> None:
    # DSQL's hard 10-schema cap surfaces on CREATE SCHEMA as an opaque driver error;
    # it must become an actionable SchemaApplyError (not be retried -- it's a hard
    # limit) telling the user to free a schema.
    connection = _SchemaLimitConnection()

    with pytest.raises(SchemaApplyError) as exc_info:
        recreate_table(
            ['CREATE SCHEMA IF NOT EXISTS "app"'],
            'CREATE TABLE "app"."orders" ("id" integer PRIMARY KEY)',
            connection_factory=lambda: connection,
            occ_max_attempts=5,
            sleep=_no_sleep,
            jitter=_zero_jitter,
        )
    msg = str(exc_info.value)
    assert "10 schemas" in msg
    assert "DROP SCHEMA" in msg
    # The DROP/CREATE TABLE were never reached (failed on the schema ensure).
    assert connection.executed == []


def test_drop_object_emits_drop_if_exists() -> None:
    # Standalone, introspector-free drop used by the Full Load "drop & reload"
    # path to pre-drop a dependent view before recreating a table it references.
    from dsql_migrator.core.schema_applier import drop_object

    connection = _FakeConnection()
    drop_object(
        'CREATE VIEW "shop"."customer_order_summary" AS SELECT 1',
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )
    assert connection.executed == [
        'DROP VIEW IF EXISTS "shop"."customer_order_summary"'
    ]
    assert connection.closed is True


def test_drop_object_retries_on_occ_conflict() -> None:
    from dsql_migrator.core.schema_applier import drop_object

    connection = _FakeConnection(failures=["40001"])
    drop_object(
        'CREATE VIEW "v" AS SELECT 1',
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )
    assert connection.executed == ['DROP VIEW IF EXISTS "v"']


# ---------------------------------------------------------------------------
# Dependent-object DROP failure — actionable message instead of "use CASCADE"
# ---------------------------------------------------------------------------


_REAL_DEPENDENCY_ERROR = (
    "cannot drop table ecommerce_demo.countries because other objects depend on it\n"
    "DETAIL:  view ecommerce_demo.customer_order_summary depends on table "
    "ecommerce_demo.countries\n"
    "HINT:  Use DROP ... CASCADE to drop the dependent objects too."
)


class _DependencyError(Exception):
    """A psycopg-like ``dependent_objects_still_exist`` (SQLSTATE 2BP01)."""

    def __init__(self, message: str = _REAL_DEPENDENCY_ERROR,
                 sqlstate: str = "2BP01") -> None:
        super().__init__(message)
        self.sqlstate = sqlstate


class _DependencyConnection(_FakeConnection):
    """A fake connection that fails the DROP with a dependent-objects error."""

    def handle_execute(self, statement: Any, _params) -> None:  # type: ignore[override]
        text = statement if isinstance(statement, str) else statement.as_string(None)
        if text.upper().startswith("DROP "):
            raise _DependencyError()
        self.executed.append(text)


def test_is_dependent_objects_error_detects_2bp01_and_message_fallback() -> None:
    from dsql_migrator.core.schema_applier import _is_dependent_objects_error

    assert _is_dependent_objects_error(_DependencyError()) is True
    # Message fallback when the SQLSTATE was lost (wrapped/re-raised).
    assert _is_dependent_objects_error(
        _DependencyError(sqlstate=None)
    ) is True
    assert _is_dependent_objects_error(RuntimeError("boom")) is False


def test_dependent_objects_hint_names_the_blocking_view() -> None:
    """The message must name the view and say to SELECT it, not to CASCADE.

    The database's own HINT is "Use DROP ... CASCADE", which is wrong here: it would
    silently destroy a view this tool may not be able to recreate. The apply pre-drops
    every view IN THE SELECTION, so a blocking view simply was not selected.
    """
    from dsql_migrator.core.schema_applier import dependent_objects_hint

    msg = dependent_objects_hint(_REAL_DEPENDENCY_ERROR)
    assert "ecommerce_demo.customer_order_summary" in msg
    assert "object browser" in msg  # tells the user WHERE to act
    assert "re-run the apply" in msg
    # It must actively steer AWAY from the database's CASCADE hint.
    assert "Avoid DROP ... CASCADE" in msg
    # Singular grammar for a single blocker.
    assert "the view ecommerce_demo.customer_order_summary still depends on it" in msg


def test_dependent_objects_hint_handles_several_blockers_and_none_named() -> None:
    from dsql_migrator.core.schema_applier import dependent_objects_hint

    two = (
        _REAL_DEPENDENCY_ERROR
        + "\nDETAIL:  view ecommerce_demo.v2 depends on table ecommerce_demo.countries"
    )
    msg = dependent_objects_hint(two)
    assert "ecommerce_demo.customer_order_summary" in msg
    assert "ecommerce_demo.v2" in msg
    assert "still depend on it" in msg  # plural agreement
    assert "those views" in msg

    # No DETAIL line to parse -> still actionable, just unnamed.
    generic = dependent_objects_hint(
        "cannot drop table x because other objects depend on it"
    )
    assert "another object (usually a view)" in generic
    assert "object browser" in generic


def test_recreate_table_translates_dependency_failure() -> None:
    # The raw driver error (and its misleading CASCADE hint) must never reach the UI.
    connection = _DependencyConnection()

    with pytest.raises(SchemaApplyError) as exc_info:
        recreate_table(
            [],
            'CREATE TABLE "ecommerce_demo"."countries" ("id" integer PRIMARY KEY)',
            connection_factory=lambda: connection,
            occ_max_attempts=5,
            sleep=_no_sleep,
            jitter=_zero_jitter,
        )
    msg = str(exc_info.value)
    assert "ecommerce_demo.customer_order_summary" in msg
    assert "object browser" in msg
    # The CREATE was never reached (the DROP failed first).
    assert connection.executed == []


def test_dependency_failure_is_not_occ_retried() -> None:
    # A dependency is hard state, not a transient conflict: retrying only wastes time.
    attempts = {"n": 0}

    class _Counting(_DependencyConnection):
        def handle_execute(self, statement: Any, _params) -> None:  # type: ignore[override]
            text = (
                statement if isinstance(statement, str)
                else statement.as_string(None)
            )
            if text.upper().startswith("DROP "):
                attempts["n"] += 1
                raise _DependencyError()
            self.executed.append(text)

    with pytest.raises(SchemaApplyError):
        recreate_table(
            [],
            'CREATE TABLE "s"."t" ("id" integer PRIMARY KEY)',
            connection_factory=lambda: _Counting(),
            occ_max_attempts=5,
            sleep=_no_sleep,
            jitter=_zero_jitter,
        )
    assert attempts["n"] == 1  # tried exactly once


# ---------------------------------------------------------------------------
# Unquoted identifiers fold to lower case (DSQL/PostgreSQL CREATE folding)
# ---------------------------------------------------------------------------


def test_parse_folds_unquoted_identifier_but_preserves_quoted() -> None:
    # DSQL folds an unquoted CREATE identifier to lower case; a quoted one is verbatim.
    assert parse_create_object('CREATE TABLE Orders ("id" uuid)') == ("orders", "TABLE")
    assert parse_create_object('CREATE TABLE "Orders" ("id" uuid)') == ("Orders", "TABLE")
    name, _ = parse_create_object('CREATE TABLE MySchema."Orders" ("id" uuid)')
    assert name == "myschema.Orders"


def test_replace_folds_unquoted_mixed_case_identifier_for_the_drop() -> None:
    # Regression: a user-edited `CREATE TABLE Orders` creates the relation `orders` in DSQL,
    # so a confirmed REPLACE must drop `orders` -- not the case-exact `"Orders"`, whose
    # DROP ... IF EXISTS would silently no-op and leave the old table (or trip 42P07).
    applier, connection, oracle = _applier(existing={"orders"})

    result = applier.apply(
        'CREATE TABLE Orders ("id" uuid PRIMARY KEY)', ApplyMode.REPLACE, confirmed=True
    )

    assert result.status is ApplyStatus.CREATED
    assert oracle.queried == ["orders"]  # existence checked against the folded name
    assert connection.executed[0] == 'DROP TABLE IF EXISTS "orders"'  # DROP folds too


# ---------------------------------------------------------------------------
# REPLACE: committed DROP + failed CREATE is surfaced as data loss, not generic
# ---------------------------------------------------------------------------


class _CreateFailsConnection(_FakeConnection):
    """DROPs succeed; the CREATE raises a non-transient, non-OCC error."""

    def handle_execute(self, statement: Any, _params) -> None:  # type: ignore[override]
        text = statement if isinstance(statement, str) else statement.as_string(None)
        if text.upper().startswith("CREATE"):
            raise RuntimeError("syntax error at or near \"(\"")
        self.executed.append(text)


def test_replace_surfaces_dropped_but_not_recreated_when_create_fails() -> None:
    # DSQL commits the DROP immediately, so a non-transient CREATE failure after it leaves the
    # object gone. The result must say so explicitly (operator's signal to restore), not read
    # as a generic "apply failed" that looks retryable.
    oracle = _StubOracle({"orders"})
    connection = _CreateFailsConnection()
    applier = SchemaApplier(
        oracle,
        connection_factory=lambda: connection,
        occ_max_attempts=1,
        occ_base_delay=0.0,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )

    result = applier.apply(_CREATE_ORDERS, ApplyMode.REPLACE, confirmed=True)

    assert result.status is ApplyStatus.FAILED
    detail = result.detail.lower()
    assert "dropped" in detail and "recreated" in detail
    assert connection.executed == ['DROP TABLE IF EXISTS "orders"']  # the DROP did commit


# ---------------------------------------------------------------------------
# A transient failure DURING execute reconnects and replays the whole unit
# ---------------------------------------------------------------------------


class _ExecuteTransientOnceConnection(_FakeConnection):
    """Raises a transient (no-sqlstate) error on its FIRST execute -- a socket dropped
    mid-statement -- so the reconnect must open a fresh connection and replay."""

    def __init__(self) -> None:
        super().__init__()
        self._raised = False

    def handle_execute(self, statement: Any, _params) -> None:  # type: ignore[override]
        if not self._raised:
            self._raised = True
            raise _TransientConnectError("connection reset by peer")
        text = statement if isinstance(statement, str) else statement.as_string(None)
        self.executed.append(text)


def test_recreate_table_replays_the_unit_on_a_transient_execute_failure() -> None:
    # Regression: a transient failure was retried only while OPENING the connection, not
    # while executing. A socket dropped mid-DROP now reconnects and replays the whole
    # (idempotent) DROP+CREATE unit on a fresh connection.
    good = _FakeConnection()
    conns = iter([_ExecuteTransientOnceConnection(), good])

    recreate_table(
        [],
        'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
        connection_factory=lambda: next(conns),
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )

    assert good.executed == [
        'DROP TABLE IF EXISTS "t"',
        'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
    ]


# ---------------------------------------------------------------------------
# A permanently-unresolvable host fails fast (not retried the full OCC budget)
# ---------------------------------------------------------------------------


class OperationalError(Exception):
    """psycopg-like OperationalError (no SQLSTATE). The NAME makes
    is_transient_connection_error classify it transient, so only the message
    signature distinguishes a permanent host-resolution failure from a transient one."""

    sqlstate = None


def test_permanent_host_resolution_error_is_not_retryable() -> None:
    from dsql_migrator.core.schema_applier import (
        _is_permanent_connect_error,
        _is_retryable_connect_error,
    )

    perm = OperationalError('could not translate host name "typo-endpoint" to address')
    dns_blip = OperationalError("temporary failure in name resolution")
    reset = OperationalError("connection reset by peer")

    # Only the unambiguous NXDOMAIN-style failure is permanent...
    assert _is_permanent_connect_error(perm) is True
    assert _is_permanent_connect_error(dns_blip) is False  # EAI_AGAIN can recover
    assert _is_permanent_connect_error(reset) is False  # a rebooting cluster can recover

    # ...so it is excluded from the retryable set, while transient shapes stay retryable.
    assert _is_retryable_connect_error(perm) is False
    assert _is_retryable_connect_error(dns_blip) is True
    assert _is_retryable_connect_error(reset) is True


def test_recreate_table_fails_fast_on_a_permanently_unresolvable_host() -> None:
    # A typo'd / deleted DSQL endpoint never resolves, so retrying the connect the full OCC
    # budget only wastes time; it must fail after a single attempt.
    attempts = {"n": 0}

    def bad_host() -> _FakeConnection:
        attempts["n"] += 1
        raise OperationalError('could not translate host name "typo" to address')

    with pytest.raises(OperationalError):
        recreate_table(
            [],
            'CREATE TABLE "t" ("id" integer PRIMARY KEY)',
            connection_factory=bad_host,
            occ_max_attempts=5,
            sleep=_no_sleep,
            jitter=_zero_jitter,
        )
    assert attempts["n"] == 1  # permanent -> one attempt, not the budget of 5


# ---------------------------------------------------------------------------
# apply_foreign_key: post-load ADD CONSTRAINT (Aurora DSQL enforces FKs)
# ---------------------------------------------------------------------------

_FK_ADD = (
    'ALTER TABLE "orders" ADD CONSTRAINT "fk_user" '
    'FOREIGN KEY ("user_id") REFERENCES "users" ("id")'
)


def test_apply_foreign_key_runs_the_add_constraint() -> None:
    from dsql_migrator.core.schema_applier import apply_foreign_key

    connection = _FakeConnection()
    apply_foreign_key(
        _FK_ADD,
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )
    assert connection.executed == [_FK_ADD]


def test_apply_foreign_key_retries_on_occ_conflict() -> None:
    from dsql_migrator.core.schema_applier import apply_foreign_key

    # First execute raises OC001 (40001); the per-statement OCC retry re-runs it.
    connection = _FakeConnection(failures=[OCC_SQLSTATE])
    apply_foreign_key(
        _FK_ADD,
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )
    assert connection.executed == [_FK_ADD]


def test_apply_foreign_key_tolerates_duplicate_constraint() -> None:
    # A reconnect can replay an ADD that already committed; the duplicate_object
    # error (SQLSTATE 42710) must be treated as success, not re-raised.
    from dsql_migrator.core.schema_applier import apply_foreign_key

    connection = _FakeConnection(failures=["42710"])
    apply_foreign_key(  # must NOT raise
        _FK_ADD,
        connection_factory=lambda: connection,
        sleep=_no_sleep,
        jitter=_zero_jitter,
    )


def test_apply_foreign_key_reraises_a_genuine_failure() -> None:
    # A real, non-transient, non-duplicate error (e.g. 23503 FK violation) propagates,
    # so the caller's post-pass can report the foreign key as not applied.
    from dsql_migrator.core.schema_applier import apply_foreign_key

    connection = _FakeConnection(failures=["23503"])
    with pytest.raises(Exception):
        apply_foreign_key(
            _FK_ADD,
            connection_factory=lambda: connection,
            sleep=_no_sleep,
            jitter=_zero_jitter,
        )


def test_validate_foreign_key_runs_async_validate() -> None:
    # The NOT VALID FK's second step: ALTER TABLE ASYNC ... VALIDATE CONSTRAINT.
    from dsql_migrator.core.schema_applier import validate_foreign_key

    connection = _FakeConnection()
    validate_foreign_key(
        "orders", "fk_user",
        connection_factory=lambda: connection, sleep=_no_sleep, jitter=_zero_jitter,
    )
    assert len(connection.executed) == 1
    sqltext = connection.executed[0]
    assert "ALTER TABLE ASYNC" in sqltext
    assert "VALIDATE CONSTRAINT" in sqltext and '"fk_user"' in sqltext
