# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""G182: an init_connect sql_mode that is an expression of the session's own mode.

``SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode, 'ONLY_FULL_GROUP_BY', ''))`` is the
well-known way to relax GROUP BY for a legacy application, and ``CONCAT(@@sql_mode,
',ALLOW_INVALID_DATES')`` / ``REPLACE(@@SESSION.sql_mode, 'NO_ZERO_DATE', '')`` let it store
zero or invalid dates. G161 recorded such a value as written and could not read it: with the
default strict GLOBAL mode, Evaluation showed no SQL_MODE and no ZERO_DATES item, so the
application's loose GROUP BY queries failed on Aurora DSQL after cut over (42803) without a
word, and the dates it stored went unmentioned.

Now the dialect evaluates the usual forms from the GLOBAL value read with it, as a session gets
it, and records the mode they give; a form it cannot evaluate always gets both items, naming
the expression to check. The expected modes are what MySQL 8.0.46 gave a user without
CONNECTION_ADMIN or SUPER for each init_connect (SELECT @@SESSION.sql_mode, throwaway
container; MySQL orders the flags by bit, compared here as sets). Doubles only here.
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.assessor import SQL_MODE_RULE_ID, ZERO_DATES_RULE_ID, CompatibilityAssessor
from dsql_migrator.core.models import ColumnDef, SourceInventory, SourceType, TableDef
from dsql_migrator.core.source_dialect import dialect_for
from dsql_migrator.core.source_dialect.mysql import evaluate_sql_mode, init_connect_settings
from dsql_migrator.core.source_time_zone import init_connect_key

_MY = SourceType.MYSQL
_STRICT = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)
_TABLE = TableDef(
    name="app.orders",
    columns=[
        ColumnDef(name="id", mysql_type="int", nullable=False),
        ColumnDef(name="placed", mysql_type="date"),
    ],
    primary_key=["id"],
)


def _flags(mode: str) -> set:
    return {flag for flag in mode.split(",") if flag}


# What MySQL 8.0.46 gave the application's session for each init_connect (global: _STRICT).
_LIVE = [
    ("SET SESSION sql_mode = (SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''))",
     "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
     "NO_ENGINE_SUBSTITUTION"),
    ("SET sql_mode = CONCAT(@@sql_mode, ',ALLOW_INVALID_DATES')",
     "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,"
     "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"),
    ("SET sql_mode = REPLACE(@@SESSION.sql_mode, 'NO_ZERO_DATE', '')",
     "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
     "NO_ENGINE_SUBSTITUTION"),
    ("SET sql_mode = (select replace(@@global.sql_mode, 'STRICT_TRANS_TABLES', '') from dual)",
     "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
     "NO_ENGINE_SUBSTITUTION"),
    ("SET sql_mode = CONCAT_WS(',', @@sql_mode, 'ansi_quotes')",
     "ANSI_QUOTES,ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
     "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"),
    ("SET sql_mode = (SELECT TRIM(BOTH ',' FROM REPLACE(CONCAT(',',@@sql_mode,','),"
     "',ONLY_FULL_GROUP_BY,',',')))",
     "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
     "NO_ENGINE_SUBSTITUTION"),
    ("SET sql_mode='STRICT_ALL_TABLES'; SET sql_mode=CONCAT(@@sql_mode, ',ALLOW_INVALID_DATES')",
     "STRICT_ALL_TABLES,ALLOW_INVALID_DATES"),
]


@pytest.mark.parametrize(("init_connect", "session_mode"), _LIVE)
def test_the_usual_forms_give_the_mode_mysql_gives_the_session(
    init_connect: str, session_mode: str
) -> None:
    recorded = init_connect_settings(init_connect, global_sql_mode=_STRICT)["sql_mode"]
    assert _flags(recorded) == _flags(session_mode)


def test_what_cannot_be_evaluated_is_recorded_as_written() -> None:
    for expression in ("@app_mode", "(SELECT mode FROM cfg.modes)", "UPPER(@@sql_mode)"):
        assert evaluate_sql_mode(expression, _STRICT, _STRICT) is None
        assert init_connect_settings(
            f"SET sql_mode = {expression}", global_sql_mode=_STRICT
        ) == {"sql_mode": expression}
    # The session's mode is unknown after one: a later expression of it is too.
    assert init_connect_settings(
        "SET sql_mode = @x; SET sql_mode = CONCAT(@@sql_mode, ',A')", global_sql_mode=_STRICT
    ) == {"sql_mode": "CONCAT(@@sql_mode, ',A')"}
    # Without the global value nothing is evaluated (G161's reading, unchanged).
    assert init_connect_settings("SET sql_mode = CONCAT(@@sql_mode, ',NO_ZERO_DATE')") == {
        "sql_mode": "CONCAT(@@sql_mode, ',NO_ZERO_DATE')"
    }
    # DEFAULT is the global mode again for what follows.
    after_default = init_connect_settings(
        "SET sql_mode = 'A'; SET sql_mode = DEFAULT; SET sql_mode = CONCAT(@@sql_mode, ',B')",
        global_sql_mode="X",
    )
    assert after_default == {"sql_mode": "X,B"}


def test_the_read_records_the_mode_the_expression_gives() -> None:
    from tests.test_g148_source_server_settings import _VariablesConnection

    connection = _VariablesConnection({
        "sql_mode": _STRICT,
        "init_connect": _LIVE[0][0],
    })
    settings = dialect_for(_MY).server_settings(connection)
    assert _flags(settings[init_connect_key("sql_mode")]) == _flags(_LIVE[0][1])
    assert "init_connect" not in settings


def _items(settings: dict) -> dict:
    report = CompatibilityAssessor(source_type=_MY).assess(
        SourceInventory(tables=[_TABLE], server_settings=settings)
    )
    wanted = (SQL_MODE_RULE_ID, ZERO_DATES_RULE_ID)
    return {item.rule_id: item for item in report.items if item.rule_id in wanted}


def test_a_relaxed_group_by_is_graded_from_the_evaluated_mode() -> None:
    assigned = init_connect_settings(_LIVE[0][0], global_sql_mode=_STRICT)["sql_mode"]
    items = _items({"sql_mode": _STRICT, init_connect_key("sql_mode"): assigned})
    item = items[SQL_MODE_RULE_ID]
    assert f"init_connect sets sql_mode '{assigned}'" in item.risk
    assert "Without ONLY_FULL_GROUP_BY" in item.risk and "42803" in item.risk
    assert ZERO_DATES_RULE_ID not in items


@pytest.mark.parametrize("index", [1, 2])
def test_an_added_allow_invalid_dates_or_a_removed_no_zero_date_is_a_zero_dates_item(
    index: int,
) -> None:
    assigned = init_connect_settings(_LIVE[index][0], global_sql_mode=_STRICT)["sql_mode"]
    item = _items({"sql_mode": _STRICT, init_connect_key("sql_mode"): assigned})[
        ZERO_DATES_RULE_ID
    ]
    said = "ALLOW_INVALID_DATES" if index == 1 else "no NO_ZERO_DATE"
    assert said in item.risk and "app.orders.placed" in item.risk


@pytest.mark.parametrize("expression", ["@app_mode", "(SELECT mode FROM cfg.modes)"])
def test_an_unreadable_init_connect_mode_always_gets_both_items_naming_it(expression: str) -> None:
    items = _items({"sql_mode": _STRICT, init_connect_key("sql_mode"): expression})
    sql_mode = items[SQL_MODE_RULE_ID]
    assert f"sql_mode to {expression}, which the tool cannot read" in sql_mode.risk
    assert "SELECT @@SESSION.sql_mode" in sql_mode.risk
    assert "If that mode lacks ONLY_FULL_GROUP_BY" in sql_mode.risk
    assert "Run SELECT @@SESSION.sql_mode as the application's user" in sql_mode.recommendation
    zero = items[ZERO_DATES_RULE_ID]
    assert f"sql_mode to {expression}, which the tool cannot read" in zero.risk
    assert f"(the global sql_mode is '{_STRICT}', which lets none in)" in zero.risk
    assert "or with ALLOW_INVALID_DATES" in zero.risk and "CDC cannot carry" in zero.risk
    assert "and in its init_connect, which sets it after the global value" in zero.recommendation
    # A table with no date column gets no ZERO_DATES item, as before.
    report = CompatibilityAssessor(source_type=_MY).assess(SourceInventory(
        tables=[TableDef(name="app.t", columns=[ColumnDef(name="id", mysql_type="int")],
                         primary_key=["id"])],
        server_settings={"sql_mode": _STRICT, init_connect_key("sql_mode"): expression},
    ))
    assert ZERO_DATES_RULE_ID not in {item.rule_id for item in report.items}


def test_the_settings_caption_says_a_computed_mode_is_shown_as_it_gives() -> None:
    from dsql_migrator.ui.evaluation import source_settings_caption

    caption = source_settings_caption(_MY)
    assert "REPLACE(@@sql_mode, ...), as the mode it gives" in caption
    assert "name for you to check where the tool cannot read what it sets" in caption
