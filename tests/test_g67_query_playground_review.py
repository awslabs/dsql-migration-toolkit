"""G67: the review of G44-G55 -- Query Playground's mixed-case check flagged names the
statement defines itself, and DUAL."""

from __future__ import annotations

import pytest

from dsql_migrator.core.query_converter import QueryConverter


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT O.id FROM orders AS O WHERE O.status = 'paid'",
        "SELECT A.id, B.total FROM users A JOIN orders B ON B.user_id = A.id",
        "WITH Recent AS (SELECT id FROM orders) SELECT id FROM Recent",
        "SELECT id AS UserId FROM users ORDER BY UserId",
        "SELECT x.total FROM (SELECT amount AS total FROM payments) X",
        "SELECT 1 FROM DUAL",
    ],
)
def test_names_the_statement_defines_are_not_mixed_case_schema_names(sql: str) -> None:
    """An alias is folded the same where it is defined and where it is used, so the SQL
    runs unchanged -- yet it read MANUAL with advice to quote it, which breaks it."""
    result = QueryConverter().convert(sql)
    assert result.classification.value == "AUTO", [w.message for w in result.warnings]


def test_a_real_mixed_case_name_is_still_flagged() -> None:
    result = QueryConverter().convert("SELECT o.CustomerId FROM Orders AS o")
    (warning,) = [w for w in result.warnings if w.code == "MIXED_CASE_NAME"]
    assert "Orders, CustomerId" in warning.message


def test_from_dual_is_dropped_not_carried() -> None:
    """Aurora DSQL has no dual: the converted SQL failed with relation "dual" does not exist."""
    assert QueryConverter().convert("SELECT NOW() FROM dual WHERE 1 = 1").converted_sql == (
        "SELECT NOW() WHERE 1 = 1"
    )
    assert QueryConverter().convert("SELECT 1 FROM shop.dual").converted_sql == (
        "SELECT 1 FROM shop.dual"  # a real table of that name stays
    )
