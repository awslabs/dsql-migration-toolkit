# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""PostgreSQL-source assessment rules (``assessor_postgres``) -- Phase 1.

v1 is the source-neutral, target-DSQL STRUCTURAL rule set; the MySQL type/feature rules,
the MySQL-binlog CDC-cascade rule, and the MySQL-function view rule are excluded so they
never misfire on a PostgreSQL source.
"""

from dsql_migrator.core.assessor import CompatibilityAssessor, default_rules
from dsql_migrator.core.assessor_postgres import default_rules as pg_default_rules
from dsql_migrator.core.models import (
    ColumnDef,
    SourceInventory,
    SourceType,
    TableDef,
)

_EXPECTED_PG_RULE_IDS = {
    "FK_UNSUPPORTED",
    "CHECK_CONSTRAINT_DROPPED",
    "TRIGGER_UNSUPPORTED",
    "PROC_PLPGSQL",
    "EVENT_UNSUPPORTED",
    "NO_PRIMARY_KEY",
    "PARTITIONED_TABLE",
    "TOO_MANY_COLUMNS",
    "TOO_MANY_INDEXES",
    "TOO_MANY_KEY_COLUMNS",
}

_EXCLUDED_MYSQL_RULE_IDS = {
    "AUTO_INCREMENT",
    "CI_COLLATION",
    "SPATIAL_TYPE",
    "OVERSIZED_LOB",
    "NUMERIC_PRECISION",
    "ENUM_SET_TYPE",
    "TINYINT_BOOLEAN",
    "BIT_TYPE",
    "YEAR_TYPE",
    "GENERATED_COLUMN",
    "ON_UPDATE_TIMESTAMP",
    "UNSUPPORTED_INDEX_TYPE",
    "FK_CASCADE_CDC_GAP",  # MySQL-binlog framed
    "VIEW_UNSUPPORTED_SQL",  # MySQL app-query linter
}


def test_pg_default_rules_are_exactly_the_structural_source_neutral_set() -> None:
    ids = [r.rule_id for r in pg_default_rules()]
    assert set(ids) == _EXPECTED_PG_RULE_IDS
    assert len(ids) == len(set(ids))  # no duplicates
    for excluded in _EXCLUDED_MYSQL_RULE_IDS:
        assert excluded not in ids


def test_assessor_default_rules_postgres_delegates_to_the_pg_module() -> None:
    assert [type(r).__name__ for r in default_rules(SourceType.POSTGRES)] == [
        type(r).__name__ for r in pg_default_rules()
    ]


def test_compatibility_assessor_uses_pg_rules_for_a_pg_source() -> None:
    a = CompatibilityAssessor(source_type=SourceType.POSTGRES)
    ids = {r.rule_id for r in a._rules}  # white-box: the selected rule set
    assert ids == _EXPECTED_PG_RULE_IDS
    # A MySQL default assessor still uses the full MySQL set (unchanged).
    m = CompatibilityAssessor()
    assert "ENUM_SET_TYPE" in {r.rule_id for r in m._rules}


def test_pg_source_flags_missing_pk_but_not_a_mysql_enum_lookalike() -> None:
    # A PostgreSQL table with no PK must be flagged NO_PRIMARY_KEY; a column whose type
    # string resembles a MySQL ENUM must NOT be flagged (the ENUM rule is not run for PG).
    table = TableDef(
        name="shop.widgets",
        columns=[ColumnDef(name="kind", mysql_type="enum('a','b')")],
        primary_key=[],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    rule_ids = {item.rule_id for item in report.items}
    assert "NO_PRIMARY_KEY" in rule_ids
    assert "ENUM_SET_TYPE" not in rule_ids
