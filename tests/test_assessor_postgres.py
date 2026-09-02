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
    "PG_UNSUPPORTED_TYPE",  # PG-specific: DSQL-unsupported column types
    "PG_UNSUPPORTED_RELATION",  # PG-specific: materialized views / foreign tables
    "FK_PRESERVED",
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
    # A PostgreSQL table with no PK must be flagged NO_PRIMARY_KEY. A PostgreSQL enum
    # (DSQL-unsupported) IS flagged -- but by the PG rule PG_UNSUPPORTED_TYPE, NEVER by the
    # MySQL ENUM_SET_TYPE rule (which is not run for a PG source).
    table = TableDef(
        name="shop.widgets",
        columns=[ColumnDef(name="kind", mysql_type="mood")],  # a user-defined enum type
        primary_key=[],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[table])
    )
    # Collect the primary rule_id AND every concern's rule_id (an object aggregates all).
    rule_ids: set[str] = set()
    for item in report.items:
        rule_ids.add(item.rule_id)
        rule_ids.update(c.rule_id for c in (item.concerns or []))
    assert "NO_PRIMARY_KEY" in rule_ids
    assert "PG_UNSUPPORTED_TYPE" in rule_ids  # the enum is flagged by the PG type rule
    assert "ENUM_SET_TYPE" not in rule_ids  # ... never by the MySQL ENUM rule


def test_pg_unsupported_type_rule_flags_dsql_unsupported_columns() -> None:
    # Evaluation-time mirror of the Schema Conversion unsupported-type warning: only the
    # DSQL-unsupported columns are named, classified UNSUPPORTED, with a remodel target.
    from dsql_migrator.core.assessor_postgres import UnsupportedPostgresTypeRule
    from dsql_migrator.core.models import Classification, EffortLevel

    table = TableDef(
        name="app.events",
        columns=[
            ColumnDef(name="id", mysql_type="bigint"),     # supported
            ColumnDef(name="tags", mysql_type="text[]"),   # array -> unsupported
            ColumnDef(name="ip", mysql_type="inet"),       # network -> unsupported
            ColumnDef(name="note", mysql_type="text"),     # supported
        ],
        primary_key=["id"],
    )
    findings = UnsupportedPostgresTypeRule().evaluate(SourceInventory(tables=[table]))
    assert len(findings) == 1
    f = findings[0]
    assert f.rule_id == "PG_UNSUPPORTED_TYPE"
    assert f.classification is Classification.UNSUPPORTED
    assert f.effort is EffortLevel.MEDIUM
    assert "tags" in f.risk and "ip" in f.risk  # the unsupported columns are named
    assert "note" not in f.risk  # a supported column is not
    assert "jsonb" in f.recommendation  # names the faithful remodel targets


def test_pg_unsupported_type_rule_ignores_fully_supported_tables() -> None:
    from dsql_migrator.core.assessor_postgres import UnsupportedPostgresTypeRule

    table = TableDef(
        name="app.ok",
        columns=[
            ColumnDef(name="id", mysql_type="uuid"),
            ColumnDef(name="j", mysql_type="jsonb"),
            ColumnDef(name="t", mysql_type="timestamp with time zone"),
        ],
        primary_key=["id"],
    )
    assert UnsupportedPostgresTypeRule().evaluate(SourceInventory(tables=[table])) == []


def test_multi_schema_pg_source_is_not_flagged_as_multiple_databases() -> None:
    # A PostgreSQL source is schema-qualified (schema.table); its multiple schemas are
    # one database that DSQL supports and the tool migrates schema-qualified. The
    # MySQL-only MULTIPLE_DATABASES finding must NOT fire (it did, with MySQL
    # consolidation guidance, for essentially every multi-schema PG source).
    from dsql_migrator.core.assessor import default_inventory_rules

    ids = {rule.__name__ for rule in default_inventory_rules(SourceType.POSTGRES)}
    assert "check_multiple_source_databases" not in ids
    assert "check_table_count" in ids  # the table-count limit still applies

    inventory = SourceInventory(
        tables=[
            TableDef(name="public.users", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"]),
            TableDef(name="sales.orders", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"]),
        ]
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    rule_ids = {item.rule_id for item in report.items}
    assert "MULTIPLE_DATABASES" not in rule_ids
    # A MySQL source with the same two-schema shape DOES still get the finding.
    mysql_report = CompatibilityAssessor().assess(inventory)
    assert "MULTIPLE_DATABASES" in {item.rule_id for item in mysql_report.items}


def test_pg_evaluation_flags_collected_triggers_and_routines() -> None:
    # Fix #2: once enrich populates inventory.triggers/routines for a PG source, the PG
    # rule set (which wires TriggerRule/ProcedureRule) must flag them UNSUPPORTED -- they
    # were always empty before, so these rules never fired for PostgreSQL.
    from dsql_migrator.core.models import Classification, ObjectRef, ObjectType

    inventory = SourceInventory(
        tables=[
            TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
        ],
        triggers=[ObjectRef(name="shop.audit_ins", object_type=ObjectType.TRIGGER)],
        routines=[
            ObjectRef(name="shop.calc_total", object_type=ObjectType.FUNCTION),
            ObjectRef(name="shop.do_thing", object_type=ObjectType.PROCEDURE),
        ],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    by_name = {item.object_name: item for item in report.items}
    assert by_name["shop.audit_ins"].rule_id == "TRIGGER_UNSUPPORTED"
    assert by_name["shop.audit_ins"].classification is Classification.UNSUPPORTED
    assert by_name["shop.calc_total"].rule_id == "PROC_PLPGSQL"
    assert by_name["shop.calc_total"].classification is Classification.UNSUPPORTED
    assert by_name["shop.do_thing"].classification is Classification.UNSUPPORTED


def test_pg_evaluation_flags_materialized_views_and_foreign_tables() -> None:
    # Fix #4: matviews / foreign tables are carried as ViewDefs flagged unsupported_kind.
    # The PG rule set must flag them UNSUPPORTED (DSQL has neither) while leaving a plain
    # view unflagged (AUTO for the PG rule set, which has no view-content rule).
    from dsql_migrator.core.models import Classification, ViewDef

    inventory = SourceInventory(
        tables=[
            TableDef(name="shop.orders", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])
        ],
        views=[
            ViewDef(name="shop.daily_totals", unsupported_kind="materialized view"),
            ViewDef(name="shop.ext_orders", unsupported_kind="foreign table"),
            ViewDef(name="shop.plain", definition="SELECT 1"),
        ],
    )
    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(inventory)
    by_name = {item.object_name: item for item in report.items}
    assert by_name["shop.daily_totals"].rule_id == "PG_UNSUPPORTED_RELATION"
    assert by_name["shop.daily_totals"].classification is Classification.UNSUPPORTED
    assert by_name["shop.ext_orders"].rule_id == "PG_UNSUPPORTED_RELATION"
    assert by_name["shop.ext_orders"].classification is Classification.UNSUPPORTED
    assert by_name["shop.plain"].classification is Classification.AUTO


def test_html_report_title_reflects_the_postgres_source_engine() -> None:
    from dsql_migrator.core.assessor import render_html_report

    report = CompatibilityAssessor(source_type=SourceType.POSTGRES).assess(
        SourceInventory(tables=[TableDef(name="public.t", columns=[ColumnDef(name="id", mysql_type="bigint")], primary_key=["id"])])
    )
    pg_html = render_html_report(report, source_type=SourceType.POSTGRES)
    assert "PostgreSQL to Aurora DSQL Compatibility Assessment" in pg_html
    assert "MySQL to Aurora DSQL" not in pg_html
    # Default (MySQL) keeps the original title.
    assert "MySQL to Aurora DSQL Compatibility Assessment" in render_html_report(report)
