# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the query (DML) converter and FOR UPDATE anti-pattern checks.

Covers MySQL -> Aurora DSQL (PostgreSQL 16) transpilation
(``ON DUPLICATE KEY UPDATE`` -> ``ON CONFLICT``, ``LIMIT`` syntax, function
mapping), the ``SELECT ... FOR UPDATE`` lock anti-pattern detection, and the
manual-review flagging that keeps the original SQL when a statement cannot be
converted (Property 6 / Requirements 4.1, 4.2, 4.3, 4.4).
"""

from __future__ import annotations

import pytest

from dsql_migrator.core.models import Classification
from dsql_migrator.core.query_converter import (
    CODE_FOR_UPDATE_MULTI_TABLE,
    CODE_FOR_UPDATE_NO_EQUALITY,
    CODE_FOR_UPDATE_VERIFY_PK,
    CODE_JSON_UNQUOTE_UNSUPPORTED,
    CODE_ON_DUPLICATE_KEY_UPDATE,
    CODE_PARSE_ERROR,
    QueryConversionResult,
    QueryConverter,
    StatementKind,
    classify_sql,
)


def _convert(sql: str) -> QueryConversionResult:
    """Convert a single statement with a fresh converter."""
    return QueryConverter().convert(sql)


def _codes(result: QueryConversionResult) -> set[str]:
    """Return the set of warning codes attached to a result."""
    return {warning.code for warning in result.warnings}


# ---------------------------------------------------------------------------
# Transpilation (Requirement 4.1)
# ---------------------------------------------------------------------------


def test_clean_select_passes_through_as_auto() -> None:
    """A plain SELECT converts cleanly with no warnings (AUTO)."""
    result = _convert("SELECT id FROM t WHERE name = 'x'")
    assert result.classification is Classification.AUTO
    assert result.warnings == []
    assert result.converted_sql is not None
    assert "SELECT" in result.converted_sql.upper()


def test_original_and_converted_pair_are_both_returned() -> None:
    """Both the original and converted SQL are returned for comparison (4.4)."""
    sql = "SELECT id FROM t WHERE id = 1"
    result = _convert(sql)
    assert result.original_sql == sql
    assert result.converted_sql is not None
    assert result.converted_sql != ""


def test_limit_offset_syntax_is_converted() -> None:
    """MySQL ``LIMIT offset, count`` becomes ``LIMIT count OFFSET offset``."""
    result = _convert("SELECT * FROM t LIMIT 10, 5")
    assert result.converted_sql is not None
    converted = result.converted_sql.upper()
    assert "LIMIT 5" in converted
    assert "OFFSET 10" in converted


def test_mysql_only_function_is_mapped_to_pg_equivalent() -> None:
    """A MySQL-only function (IFNULL) maps to its PG equivalent (COALESCE)."""
    result = _convert("SELECT IFNULL(a, 0) FROM t")
    assert result.converted_sql is not None
    assert "COALESCE" in result.converted_sql.upper()
    assert "IFNULL" not in result.converted_sql.upper()


def test_on_duplicate_key_update_converted_to_on_conflict() -> None:
    """ON DUPLICATE KEY UPDATE becomes ON CONFLICT and is flagged for review."""
    result = _convert(
        "INSERT INTO t (id, n) VALUES (1, 'a') ON DUPLICATE KEY UPDATE n = VALUES(n)"
    )
    assert result.converted_sql is not None
    converted = result.converted_sql.upper()
    assert "ON CONFLICT" in converted
    assert "DO UPDATE" in converted
    assert "EXCLUDED" in converted
    assert "ON DUPLICATE KEY" not in converted
    # Conflict target columns cannot be inferred, so this needs manual review.
    assert CODE_ON_DUPLICATE_KEY_UPDATE in _codes(result)
    assert result.classification is Classification.MANUAL


# ---------------------------------------------------------------------------
# JSON_UNQUOTE(JSON_EXTRACT(...)) -> PostgreSQL/DSQL scalar text extraction
# ---------------------------------------------------------------------------


def test_json_unquote_extract_rewritten_to_scalar_text() -> None:
    """JSON_UNQUOTE(JSON_EXTRACT(col,'$.k')) -> JSON_EXTRACT_PATH_TEXT (runs on DSQL)."""
    result = _convert(
        "SELECT JSON_UNQUOTE(JSON_EXTRACT(`metadata`, '$.coupon')) AS c FROM `orders`"
    )
    assert result.converted_sql is not None
    converted = result.converted_sql.upper()
    # No bare JSON_UNQUOTE (which DSQL has no function for) survives...
    assert "JSON_UNQUOTE" not in converted
    # ...and the scalar/text extraction form is emitted instead.
    assert "JSON_EXTRACT_PATH_TEXT" in converted
    # Clean, automatic conversion -- no warnings.
    assert result.warnings == []
    assert result.classification is Classification.AUTO


def test_json_unquote_extract_nested_path() -> None:
    result = _convert("SELECT JSON_UNQUOTE(JSON_EXTRACT(c, '$.a.b')) FROM t")
    assert result.converted_sql is not None
    assert "JSON_EXTRACT_PATH_TEXT" in result.converted_sql.upper()
    assert "'a'" in result.converted_sql and "'b'" in result.converted_sql


def test_standalone_json_unquote_is_flagged_not_silently_emitted() -> None:
    """JSON_UNQUOTE without JSON_EXTRACT has no PG equivalent: left + flagged MANUAL."""
    result = _convert("SELECT JSON_UNQUOTE(c) FROM t")
    assert CODE_JSON_UNQUOTE_UNSUPPORTED in _codes(result)
    assert result.classification is Classification.MANUAL


# ---------------------------------------------------------------------------
# HAVING that references a SELECT-list alias (MySQL) -> inlined for PostgreSQL
# ---------------------------------------------------------------------------


def test_having_alias_is_inlined_for_postgres() -> None:
    """MySQL HAVING may reference an output alias; PG cannot -> inline the expr."""
    result = _convert(
        "SELECT customer_id, SUM(amount) AS total FROM orders "
        "GROUP BY customer_id HAVING total > 1000"
    )
    assert result.converted_sql is not None
    converted = result.converted_sql
    # The bare alias must NOT survive in HAVING (PG would reject it)...
    assert "HAVING\n  total" not in converted
    assert "HAVING total" not in converted
    # ...the underlying aggregate expression is written out instead.
    assert "SUM(" in converted.upper()
    assert "> 1000" in converted


def test_having_inlines_only_aliases_not_qualified_columns() -> None:
    """A qualified column in HAVING (real column) is left as-is; aliases inline."""
    result = _convert(
        "SELECT c.id, SUM(c.amount) AS net FROM c "
        "GROUP BY c.id HAVING net > 10 AND COUNT(c.x) > 2"
    )
    assert result.converted_sql is not None
    converted = result.converted_sql
    # alias 'net' inlined to its SUM expression; the COUNT(c.x) predicate stays.
    assert "net >" not in converted
    assert "SUM(c.amount) > 10" in converted
    assert "COUNT(c.x) > 2" in converted


def test_order_by_alias_is_preserved() -> None:
    """ORDER BY may use an output alias on both engines -- it must NOT be inlined."""
    result = _convert(
        "SELECT customer_id, SUM(amount) AS total FROM orders "
        "GROUP BY customer_id ORDER BY total DESC"
    )
    assert result.converted_sql is not None
    assert "ORDER BY" in result.converted_sql.upper()
    assert "total" in result.converted_sql  # alias kept in ORDER BY


# ---------------------------------------------------------------------------
# FOR UPDATE lock anti-pattern detection (Requirement 4.2)
# ---------------------------------------------------------------------------


def test_for_update_single_table_equality_emits_verify_warning() -> None:
    """Single table + equality cannot be proven a violation: verify warning."""
    result = _convert("SELECT * FROM t WHERE id = 1 FOR UPDATE")
    assert result.converted_sql is not None
    assert "FOR UPDATE" in result.converted_sql.upper()
    assert CODE_FOR_UPDATE_VERIFY_PK in _codes(result)
    assert result.classification is Classification.MANUAL


def test_for_update_multi_table_join_is_flagged_as_violation() -> None:
    """A FOR UPDATE across a join violates the single-table constraint."""
    result = _convert(
        "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id = 1 FOR UPDATE"
    )
    assert CODE_FOR_UPDATE_MULTI_TABLE in _codes(result)
    assert result.classification is Classification.MANUAL
    message = result.warnings[0].message.lower()
    assert "single table" in message


def test_for_update_comma_join_is_flagged_as_violation() -> None:
    """A comma-join FOR UPDATE is also a multi-table violation."""
    result = _convert("SELECT * FROM t, u WHERE t.id = 1 FOR UPDATE")
    assert CODE_FOR_UPDATE_MULTI_TABLE in _codes(result)


def test_for_update_range_predicate_is_flagged_as_violation() -> None:
    """A FOR UPDATE with only a range predicate is a multi-row lock violation."""
    result = _convert("SELECT * FROM t WHERE created > 5 FOR UPDATE")
    assert CODE_FOR_UPDATE_NO_EQUALITY in _codes(result)
    assert result.classification is Classification.MANUAL


def test_for_update_without_where_is_flagged_as_violation() -> None:
    """A FOR UPDATE with no WHERE locks every row: a violation."""
    result = _convert("SELECT * FROM t FOR UPDATE")
    assert CODE_FOR_UPDATE_NO_EQUALITY in _codes(result)


def test_for_update_or_predicate_is_flagged_as_violation() -> None:
    """A top-level OR can match multiple rows: not a simple equality lock."""
    result = _convert("SELECT * FROM t WHERE id = 1 OR id = 2 FOR UPDATE")
    assert CODE_FOR_UPDATE_NO_EQUALITY in _codes(result)


def test_select_without_for_update_has_no_lock_warning() -> None:
    """A SELECT without FOR UPDATE produces no lock warning."""
    result = _convert("SELECT * FROM t WHERE created > 5")
    assert result.classification is Classification.AUTO
    assert result.warnings == []


# ---------------------------------------------------------------------------
# Unsupported / unparseable SQL -> manual review (Requirement 4.3 / Property 6)
# ---------------------------------------------------------------------------


def test_unparseable_sql_is_flagged_manual_review_and_keeps_original() -> None:
    """Unparseable SQL is flagged for manual review with the original kept."""
    sql = "SELECT FROM WHERE (("
    result = _convert(sql)
    assert result.classification is Classification.MANUAL
    assert result.converted_sql is None
    assert result.original_sql == sql
    assert CODE_PARSE_ERROR in _codes(result)


def test_manual_review_never_silently_drops_input() -> None:
    """Property 6: a non-convertible statement is never silently dropped."""
    sql = "INSERT INTO"
    result = _convert(sql)
    assert result.converted_sql is None
    assert result.original_sql == sql
    assert result.warnings  # at least one warning explains why
    assert all(
        w.classification in {Classification.MANUAL, Classification.UNSUPPORTED}
        for w in result.warnings
    )


# ---------------------------------------------------------------------------
# Statement classification (drives what is safe to test-run)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM t WHERE id = 1", StatementKind.SELECT),
        ("WITH x AS (SELECT 1) SELECT * FROM x", StatementKind.SELECT),
        ("SELECT a FROM t UNION SELECT b FROM u", StatementKind.SELECT),
        ("INSERT INTO t (a) VALUES (1)", StatementKind.DML),
        ("INSERT INTO t SELECT * FROM u", StatementKind.DML),
        ("UPDATE t SET a = 1 WHERE id = 2", StatementKind.DML),
        ("DELETE FROM t WHERE id = 2", StatementKind.DML),
        ("REPLACE INTO t (a) VALUES (1)", StatementKind.DML),
        ("CREATE TABLE t (id INT PRIMARY KEY)", StatementKind.DDL),
        ("ALTER TABLE t ADD COLUMN c INT", StatementKind.DDL),
        ("DROP TABLE t", StatementKind.DDL),
        ("TRUNCATE TABLE t", StatementKind.DDL),
        ("SET autocommit = 1", StatementKind.OTHER),
        ("nonsense ((", StatementKind.OTHER),
    ],
)
def test_classify_sql(sql: str, expected: StatementKind) -> None:
    assert classify_sql(sql) is expected


def test_pretty_renders_multi_line_without_changing_default() -> None:
    sql = "SELECT a, b, c FROM t WHERE a = 1 AND b = 2 ORDER BY c"
    # Default stays single-line (back-compat for existing callers/tests).
    assert "\n" not in (_convert(sql).converted_sql or "")
    # pretty=True formats the converted SQL multi-line for readability.
    pretty = QueryConverter().convert(sql, pretty=True)
    assert pretty.converted_sql is not None
    assert "\n" in pretty.converted_sql
    assert "SELECT" in pretty.converted_sql.upper()


def test_conversion_result_carries_statement_kind() -> None:
    # The converter populates statement_kind so the playground can branch on it.
    assert _convert("SELECT 1").statement_kind is StatementKind.SELECT
    assert _convert("UPDATE t SET a = 1").statement_kind is StatementKind.DML
    assert (
        _convert("CREATE TABLE t (id INT PRIMARY KEY)").statement_kind
        is StatementKind.DDL
    )
