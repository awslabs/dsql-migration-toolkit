# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Tests for the opt-in ADD COLUMN drift recovery (core/cdc_schema_evolve).

The planner is pure, so these run with no MySQL/DSQL/AWS: the column lists are
passed in directly and the applier gets a fake connection factory.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.cdc_schema_evolve import (
    AddColumnPlan,
    apply_add_columns,
    plan_add_columns,
    read_source_columns,
    read_target_columns,
)


# --------------------------------------------------------------------------- #
# planner
# --------------------------------------------------------------------------- #
def test_plan_offers_only_the_columns_the_target_lacks() -> None:
    plan = plan_add_columns(
        "ecommerce.users",
        [("id", "bigint"), ("email", "varchar(255)"), ("nickname", "varchar(64)")],
        ["id", "email"],
    )
    assert [step.column for step in plan.steps] == ["nickname"]
    assert plan.skipped == ()
    assert not plan.is_empty


def test_plan_is_empty_when_target_already_matches() -> None:
    # The common case after a re-run: nothing to do, and the UI must say so
    # rather than offering an empty ALTER.
    plan = plan_add_columns(
        "ecommerce.users", [("id", "bigint"), ("email", "varchar(255)")], ["id", "email"]
    )
    assert plan.is_empty
    assert plan.ddl_text == ""


def test_plan_renders_nullable_add_column_ddl_with_quoted_identifiers() -> None:
    # Nullable + no default is deliberate: NOT NULL without a default is rejected
    # on a populated table, and a fabricated default would invent data for rows
    # CDC already applied. Identifiers are quoted so a reserved word or mixed case
    # cannot break (or inject into) the statement.
    plan = plan_add_columns(
        "ecommerce.product_media", [("order", "int")], []
    )
    (step,) = plan.steps
    # Assert the RENDERING (quoting + shape), not the mapping table itself -- the
    # MySQL->DSQL type mapping has its own tests in test_converter.
    assert step.ddl == (
        'ALTER TABLE "ecommerce"."product_media" '
        f'ADD COLUMN "order" {step.target_type}'
    )
    assert step.ddl.startswith('ALTER TABLE "ecommerce"."product_media" ADD COLUMN ')
    assert '"order"' in step.ddl, "a reserved-word column must stay quoted"
    assert "NOT NULL" not in step.ddl
    assert "DEFAULT" not in step.ddl


def test_plan_preserves_source_ordinal_order() -> None:
    # Multi-column drift should read like the source's own change history.
    plan = plan_add_columns(
        "app.t",
        [("a", "int"), ("b", "int"), ("c", "int"), ("d", "int")],
        ["b"],
    )
    assert [step.column for step in plan.steps] == ["a", "c", "d"]


def test_plan_target_comparison_is_case_insensitive() -> None:
    # The converter lower-cases identifiers when it creates the target table, so a
    # source `Full_Name` lives on the target as `full_name` and must NOT be
    # reported missing (that would emit a duplicate-column ALTER).
    plan = plan_add_columns(
        "app.t", [("Full_Name", "varchar(50)")], ["full_name"]
    )
    assert plan.is_empty


def test_plan_skips_unmappable_type_instead_of_guessing() -> None:
    # Never approximate a type: report it so the operator decides.
    plan = plan_add_columns(
        "app.t", [("ok", "int"), ("weird", "nonexistent_type_xyz(9)")], []
    )
    assert [step.column for step in plan.steps] == ["ok"]
    assert [skipped.column for skipped in plan.skipped] == ["weird"]
    assert "no Aurora DSQL mapping" in plan.skipped[0].reason


def test_plan_keeps_the_converter_warning_for_a_lossy_mapping() -> None:
    # A spatial type maps to bytea WITH a warning; the plan must carry it so the
    # approval dialog can show the caveat rather than presenting it as clean.
    plan = plan_add_columns("app.t", [("shape", "geometry")], [])
    (step,) = plan.steps
    assert step.target_type == "bytea"
    assert step.warning is not None and "no Aurora DSQL equivalent" in step.warning


def test_plan_ddl_text_is_one_statement_per_line() -> None:
    plan = plan_add_columns("app.t", [("a", "int"), ("b", "text")], [])
    assert plan.ddl_text.splitlines() == [step.ddl for step in plan.steps]
    assert len(plan.ddl_text.splitlines()) == 2


# --------------------------------------------------------------------------- #
# applier
# --------------------------------------------------------------------------- #
class _FakeCursor:
    def __init__(self, recorder, fail_on=None) -> None:
        self._recorder = recorder
        self._fail_on = fail_on

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None):
        if self._fail_on is not None and self._fail_on in sql:
            raise RuntimeError("boom: column already exists")
        self._recorder.append(sql)


class _FakeConnection:
    def __init__(self, recorder, fail_on=None) -> None:
        self._recorder = recorder
        self._fail_on = fail_on
        self.closed = False

    def cursor(self):
        return _FakeCursor(self._recorder, self._fail_on)

    def close(self) -> None:
        self.closed = True


def test_apply_runs_one_ddl_per_fresh_connection() -> None:
    # Aurora DSQL rejects two DDL statements in one transaction, so each ALTER
    # must get its own (autocommit) connection -- assert one connection per step.
    executed: list[str] = []
    opened: list[_FakeConnection] = []

    def factory():
        conn = _FakeConnection(executed)
        opened.append(conn)
        return conn

    plan = plan_add_columns("app.t", [("a", "int"), ("b", "text")], [])
    outcomes = apply_add_columns(plan, factory)
    assert [o.applied for o in outcomes] == [True, True]
    assert len(opened) == 2, "each DDL needs its own transaction/connection"
    assert len(executed) == 2
    assert all(conn.closed for conn in opened), "connections must not leak"


def test_apply_stops_at_the_first_failure_and_reports_it() -> None:
    # A partially-evolved table must be reported honestly; already-applied ALTERs
    # are durable (each committed on its own), so re-planning simply drops them.
    executed: list[str] = []

    def factory():
        return _FakeConnection(executed, fail_on='"b"')

    plan = plan_add_columns("app.t", [("a", "int"), ("b", "text"), ("c", "int")], [])
    outcomes = apply_add_columns(plan, factory)
    assert [o.column for o in outcomes] == ["a", "b"], "must stop after the failure"
    assert outcomes[0].applied is True
    assert outcomes[1].applied is False
    assert "already exists" in (outcomes[1].error or "")


def test_apply_reports_a_connection_failure_without_raising() -> None:
    def factory():
        raise RuntimeError("token expired")

    plan = plan_add_columns("app.t", [("a", "int")], [])
    outcomes = apply_add_columns(plan, factory)
    assert len(outcomes) == 1
    assert outcomes[0].applied is False
    assert "token expired" in (outcomes[0].error or "")


def test_apply_empty_plan_is_a_noop() -> None:
    def factory():  # pragma: no cover - must never be called
        raise AssertionError("an empty plan must not open a connection")

    assert apply_add_columns(AddColumnPlan(table="app.t"), factory) == ()


# --------------------------------------------------------------------------- #
# read helpers (query shape / read-only)
# --------------------------------------------------------------------------- #
class _RecordingCursor:
    def __init__(self, rows, calls) -> None:
        self._rows = rows
        self._calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False

    def execute(self, sql, params=None):
        self._calls.append((sql, params))

    def fetchall(self):
        return self._rows


class _RecordingConnection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.calls: list[tuple] = []

    def cursor(self):
        return _RecordingCursor(self.rows, self.calls)


def test_read_source_columns_uses_column_type_and_ordinal_order() -> None:
    # COLUMN_TYPE (not DATA_TYPE) is what the converter maps -- it keeps the
    # length/precision/unsigned detail the mapping depends on.
    conn = _RecordingConnection([("id", "bigint"), ("email", "varchar(255)")])
    assert read_source_columns(conn, "ecommerce.users") == [
        ("id", "bigint"),
        ("email", "varchar(255)"),
    ]
    sql, params = conn.calls[0]
    assert "COLUMN_TYPE" in sql
    assert "ORDER BY ORDINAL_POSITION" in sql
    assert params == ("ecommerce", "users")
    # Read-only against information_schema: never touches the user table.
    assert "information_schema.columns" in sql
    for forbidden in ("INSERT", "UPDATE", "DELETE", "ALTER"):
        assert forbidden not in sql.upper()


def test_read_target_columns_is_parameterized_and_read_only() -> None:
    conn = _RecordingConnection([("id",), ("email",)])
    assert read_target_columns(conn, "ecommerce.users") == ["id", "email"]
    sql, params = conn.calls[0]
    assert params == ("ecommerce", "users")
    assert "information_schema.columns" in sql
    for forbidden in ("INSERT", "UPDATE", "DELETE", "ALTER"):
        assert forbidden not in sql.upper()


@pytest.mark.parametrize("table", ["ecommerce.users", "users"])
def test_read_helpers_accept_qualified_or_bare_table(table: str) -> None:
    # A bare name yields an empty schema filter rather than crashing; the caller
    # always passes schema-qualified names, but this must not raise.
    conn = _RecordingConnection([])
    read_source_columns(conn, table)
    read_target_columns(conn, table)
    assert len(conn.calls) == 2
