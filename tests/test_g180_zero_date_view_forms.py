# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G180: the zero-date forms G156's view and query check left out.

G156 reports a view (and a Query Converter statement) that compares a date with a date MySQL
stores and Aurora DSQL has no value for, but only through =, <>, <, >, IN, BETWEEN and a
literal that IS a CASE's operand. The usual zero-date idioms stayed AUTO with no note and
their CREATE VIEW failed at apply: NULLIF(d, '0000-00-00') (22008), a simple CASE's WHEN
value (22008), GREATEST / LEAST (22008), MySQL's typed literals DATE '0000-00-00' and
TIMESTAMP '0000-00-00 00:00:00' (a cast PostgreSQL rejects, 22008) and the number 0, MySQL's
other spelling of the zero date compared with a date (42883, timestamp = integer).

The bodies below are what MySQL 8.0.46 prints for such views (SHOW CREATE VIEW, throwaway
container); PostgreSQL 17.11 (a stand-in for Aurora DSQL) refuses each converted CREATE VIEW
with the SQLSTATE named. Doubles only here.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessor import CompatibilityAssessor
from dsql_migrator.core.converter import SchemaConverter, table_type_index
from dsql_migrator.core.models import (
    Classification,
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
    ViewDef,
)
from dsql_migrator.core.query_converter import QueryConverter


def _users() -> TableDef:
    return TableDef(
        name="shop.users",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="deleted_at", mysql_type="datetime", nullable=False,
                      default="0000-00-00 00:00:00"),
            ColumnDef(name="born", mysql_type="date"),
            ColumnDef(name="qty", mysql_type="int"),
            ColumnDef(name="at", mysql_type="time"),
        ],
        primary_key=["id"],
    )


def _view(body: str) -> ViewDef:
    return ViewDef(
        name="shop.v",
        definition=(
            "CREATE ALGORITHM=UNDEFINED DEFINER=`a`@`%` SQL SECURITY DEFINER VIEW `shop`.`v` AS "
            + body
        ),
    )


_U = "`shop`.`users`"
_ZERO_STRING = "Aurora DSQL rejects that literal (SQLSTATE 22008)"
_ZERO_NUMBER = "A date is compared with 0, MySQL's number for the zero date"


@pytest.mark.parametrize(
    ("body", "said"),
    [
        (f"select nullif({_U}.`deleted_at`,'0000-00-00 00:00:00') AS `d` from {_U}", _ZERO_STRING),
        (f"select (case {_U}.`deleted_at` when '0000-00-00 00:00:00' then 'active' "
         f"else 'deleted' end) AS `s` from {_U}", _ZERO_STRING),
        (f"select {_U}.`id` AS `id` from {_U} where ({_U}.`born` = DATE'0000-00-00')",
         _ZERO_STRING),
        (f"select {_U}.`id` AS `id` from {_U} where "
         f"({_U}.`deleted_at` = TIMESTAMP'0000-00-00 00:00:00')", _ZERO_STRING),
        (f"select greatest({_U}.`born`,'0000-00-00') AS `g` from {_U}", _ZERO_STRING),
        (f"select least({_U}.`deleted_at`,'2024-02-30 00:00:00') AS `g` from {_U}", _ZERO_STRING),
        (f"select {_U}.`id` AS `id` from {_U} where ({_U}.`deleted_at` = 0)", _ZERO_NUMBER),
        (f"select {_U}.`id` AS `id` from {_U} where ({_U}.`deleted_at` > 0)", _ZERO_NUMBER),
        (f"select nullif({_U}.`born`,0) AS `d` from {_U}", _ZERO_NUMBER),
        (f"select (case {_U}.`deleted_at` when 0 then 'active' else 'deleted' end) AS `s` "
         f"from {_U}", _ZERO_NUMBER),
        (f"select DATE'0000-00-00' AS `z` from {_U}", "'0000-00-00' is read as a date"),
    ],
)
def test_each_zero_date_idiom_makes_the_view_manual(body: str, said: str) -> None:
    table = _users()
    converted = SchemaConverter().convert_view(_view(body), tables=[table])
    (warning,) = converted.warnings
    assert warning.classification is Classification.MANUAL and said in warning.message
    assert "NULL, the column's DEFAULT or 1970-01-01" in warning.message
    report = CompatibilityAssessor(source_type=SourceType.MYSQL).assess(
        SourceInventory(tables=[table], views=[_view(body)])
    )
    (item,) = [i for i in report.items if i.object_name == "shop.v"]
    assert item.classification is Classification.MANUAL
    assert [c.rule_id for c in item.concerns] == ["VIEW_NOT_CONVERTED"]


@pytest.mark.parametrize(
    "body",
    [
        f"select {_U}.`id` AS `id` from {_U} where ({_U}.`born` = DATE'2024-01-01')",
        f"select (case {_U}.`born` when '2024-01-01' then 1 else 0 end) AS `s` from {_U}",
        f"select nullif({_U}.`qty`,0) AS `q` from {_U}",
        f"select greatest({_U}.`qty`,0) AS `q` from {_U}",
        f"select (case {_U}.`qty` when 0 then 'none' else 'some' end) AS `s` from {_U}",
        f"select {_U}.`id` AS `id` from {_U} where ({_U}.`qty` > 0) limit 0",
        f"select {_U}.`id` AS `id` from {_U} where ({_U}.`at` = 0)",
    ],
)
def test_real_dates_and_numbers_beside_numbers_are_carried_as_before(body: str) -> None:
    converted = SchemaConverter().convert_view(_view(body), tables=[_users()])
    assert converted.warnings == [] and converted.auto_converted


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT NULLIF(deleted_at, '0000-00-00 00:00:00') FROM shop.users",
        "SELECT CASE deleted_at WHEN '0000-00-00 00:00:00' THEN 'active' ELSE 'deleted' END "
        "FROM shop.users",
        "SELECT id FROM shop.users WHERE born = DATE '0000-00-00'",
        "SELECT id FROM shop.users WHERE deleted_at = TIMESTAMP '0000-00-00 00:00:00'",
        "SELECT GREATEST(born, '0000-00-00') FROM shop.users",
        "SELECT LEAST(born, '0000-00-00') FROM shop.users",
        "SELECT id FROM shop.users WHERE deleted_at = 0",
        "UPDATE shop.users SET qty = 1 WHERE deleted_at = 0",
    ],
)
def test_the_query_converter_grades_the_same_statements_manual(statement: str) -> None:
    converter = QueryConverter(column_types=tuple(table_type_index([_users()]).values()))
    result = converter.convert(statement)
    assert result.classification is Classification.MANUAL
    assert any(
        "SQLSTATE 22008" in str(getattr(w, "message", w))
        or "SQLSTATE 42883" in str(getattr(w, "message", w))
        for w in result.warnings
    )
    plain = converter.convert("SELECT NULLIF(qty, 0) FROM shop.users")
    assert plain.classification is Classification.AUTO


def test_zero_dates_names_the_number_0_among_the_reads() -> None:
    from dsql_migrator.core.assessor import ZERO_DATES_RULE_ID

    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_users()], server_settings={"sql_mode": "NO_ENGINE_SUBSTITUTION"})
    )
    (item,) = [i for i in report.items if i.rule_id == ZERO_DATES_RULE_ID]
    assert "deleted_at = 0: MySQL reads the number 0 compared with a date" in item.recommendation
    assert "SQLSTATE 42883" in item.recommendation
