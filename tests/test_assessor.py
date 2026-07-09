# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the compatibility assessment rule engine.

Covers each rule (FK_UNSUPPORTED, TRIGGER_UNSUPPORTED, PROC_PLPGSQL,
AUTO_INCREMENT, NO_PRIMARY_KEY, CI_COLLATION, PARTITIONED_TABLE, SPATIAL_TYPE,
TINYINT_BOOLEAN, BIT_TYPE, YEAR_TYPE), the most-severe aggregation strategy,
report export, and Property 8 (assessment completeness: every object is
classified exactly once).

Requirements covered: 2.1, 2.2, 2.3, 2.4, 2.5.
"""

from __future__ import annotations

import json

from dsql_migrator.core.assessor import (
    CompatibilityAssessor,
    Rule,
    check_table_count,
    default_rules,
    export_report,
    render_text_report,
)
from dsql_migrator.core.models import (
    AssessmentReport,
    Classification,
    ColumnDef,
    EffortLevel,
    ForeignKeyDef,
    IndexDef,
    ObjectRef,
    ObjectType,
    SourceInventory,
    TableDef,
    ViewDef,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _assess(inventory: SourceInventory) -> AssessmentReport:
    return CompatibilityAssessor().assess(inventory)


def _item_for(report: AssessmentReport, object_name: str):
    return next(item for item in report.items if item.object_name == object_name)


def _table_with_pk(name: str, **kwargs) -> TableDef:
    """Build a table that, by default, triggers no rule (clean baseline)."""
    kwargs.setdefault("columns", [ColumnDef(name="id", mysql_type="INT")])
    kwargs.setdefault("primary_key", ["id"])
    return TableDef(name=name, **kwargs)


# ---------------------------------------------------------------------------
# Per-rule tests
# ---------------------------------------------------------------------------


def test_fk_unsupported_rule_classifies_table_manual() -> None:
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "orders",
                foreign_keys=[
                    ForeignKeyDef(
                        name="fk_customer",
                        columns=["customer_id"],
                        referenced_table="customers",
                        referenced_columns=["id"],
                    )
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "orders")
    assert item.rule_id == "FK_UNSUPPORTED"
    assert item.classification is Classification.MANUAL
    assert "foreign key" in item.risk.lower()
    assert item.effort is EffortLevel.SIMPLE


def test_trigger_unsupported_rule_classifies_trigger_unsupported() -> None:
    inventory = SourceInventory(
        triggers=[ObjectRef(name="trg_audit", object_type=ObjectType.TRIGGER)]
    )
    item = _item_for(_assess(inventory), "trg_audit")
    assert item.rule_id == "TRIGGER_UNSUPPORTED"
    assert item.classification is Classification.UNSUPPORTED


def test_proc_plpgsql_rule_classifies_routine_unsupported() -> None:
    inventory = SourceInventory(
        routines=[ObjectRef(name="sp_recalc", object_type=ObjectType.ROUTINE)]
    )
    item = _item_for(_assess(inventory), "sp_recalc")
    assert item.rule_id == "PROC_PLPGSQL"
    assert item.classification is Classification.UNSUPPORTED


def test_procedures_and_functions_are_categorized_separately() -> None:
    inventory = SourceInventory(
        routines=[
            ObjectRef(name="sp_recalc", object_type=ObjectType.PROCEDURE),
            ObjectRef(name="fn_total", object_type=ObjectType.FUNCTION),
        ]
    )
    report = _assess(inventory)
    proc = _item_for(report, "sp_recalc")
    func = _item_for(report, "fn_total")
    assert proc.kind == "PROCEDURE" and proc.classification is Classification.UNSUPPORTED
    assert func.kind == "FUNCTION" and func.classification is Classification.UNSUPPORTED
    assert "stored procedure" in proc.risk.lower()
    assert "function" in func.risk.lower()


def test_auto_increment_rule_classifies_table_manual() -> None:
    inventory = SourceInventory(
        tables=[_table_with_pk("users", auto_increment_column="id")]
    )
    item = _item_for(_assess(inventory), "users")
    assert item.rule_id == "AUTO_INCREMENT"
    assert item.classification is Classification.MANUAL
    assert "hot partition" in item.risk.lower()


def test_no_primary_key_rule_classifies_table_unsupported() -> None:
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="events",
                columns=[ColumnDef(name="payload", mysql_type="TEXT")],
                primary_key=[],
            )
        ]
    )
    item = _item_for(_assess(inventory), "events")
    assert item.rule_id == "NO_PRIMARY_KEY"
    assert item.classification is Classification.UNSUPPORTED


def test_ci_collation_rule_classifies_table_manual() -> None:
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "people",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(
                        name="name",
                        mysql_type="VARCHAR(100)",
                        collation="utf8mb4_general_ci",
                    ),
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "people")
    assert item.rule_id == "CI_COLLATION"
    assert item.classification is Classification.MANUAL
    assert "collation" in item.risk.lower()


def test_partitioned_table_rule_classifies_table_manual() -> None:
    inventory = SourceInventory(
        tables=[_table_with_pk("metrics", partitioned=True)]
    )
    item = _item_for(_assess(inventory), "metrics")
    assert item.rule_id == "PARTITIONED_TABLE"
    assert item.classification is Classification.MANUAL


def test_spatial_type_rule_classifies_table_manual_not_unsupported() -> None:
    # Spatial columns are auto-substituted to bytea (WKB preserved), so the table
    # migrates -- it is MANUAL (review whether bytea suffices), not UNSUPPORTED.
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "places",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="location", mysql_type="GEOMETRY"),
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "places")
    assert item.rule_id == "SPATIAL_TYPE"
    assert item.classification is Classification.MANUAL
    assert "bytea" in item.risk


def test_clean_table_is_classified_auto() -> None:
    inventory = SourceInventory(tables=[_table_with_pk("settings")])
    item = _item_for(_assess(inventory), "settings")
    assert item.classification is Classification.AUTO
    assert item.rule_id == "COMPATIBLE"
    # AUTO objects need no manual work, so they carry no effort estimate.
    assert item.effort is None


def test_medium_effort_for_spatial_type() -> None:
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "places",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="location", mysql_type="GEOMETRY"),
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "places")
    assert item.effort is EffortLevel.MEDIUM


def test_tinyint_one_flagged_manual_matching_converter() -> None:
    # Regression: the assessor used to return AUTO/COMPATIBLE for a table whose
    # only notable column was TINYINT(1)/BIT/YEAR, contradicting the converter's
    # MANUAL classification and the "no silent compatible" guarantee. TINYINT(1)
    # in particular is table-fatal at Full Load if a value is outside 0/1.
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "flags",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="active", mysql_type="TINYINT(1)"),
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "flags")
    assert item.rule_id == "TINYINT_BOOLEAN"
    assert item.classification is Classification.MANUAL


def test_wide_tinyint_is_not_flagged_as_boolean() -> None:
    # A wider TINYINT(n) is a normal small integer, not the boolean convention.
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "counts",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="n", mysql_type="TINYINT(4)"),
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "counts")
    assert item.classification is Classification.AUTO


def test_bit_and_year_flagged_manual() -> None:
    inventory = SourceInventory(
        tables=[
            _table_with_pk(
                "t_bit",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="flags", mysql_type="BIT(8)"),
                ],
            ),
            _table_with_pk(
                "t_year",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="yr", mysql_type="YEAR"),
                ],
            ),
        ]
    )
    report = _assess(inventory)
    bit_item = _item_for(report, "t_bit")
    year_item = _item_for(report, "t_year")
    assert bit_item.rule_id == "BIT_TYPE"
    assert bit_item.classification is Classification.MANUAL
    assert year_item.rule_id == "YEAR_TYPE"
    assert year_item.classification is Classification.MANUAL


def test_view_with_no_rules_is_classified_auto() -> None:
    inventory = SourceInventory(
        views=[ViewDef(name="active_orders", definition="SELECT 1")]
    )
    item = _item_for(_assess(inventory), "active_orders")
    assert item.classification is Classification.AUTO


# ---------------------------------------------------------------------------
# Aggregation: most severe classification wins
# ---------------------------------------------------------------------------


def test_most_severe_classification_wins_when_multiple_rules_match() -> None:
    # Table has a foreign key (MANUAL) AND no primary key (UNSUPPORTED).
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="line_items",
                columns=[ColumnDef(name="order_id", mysql_type="INT")],
                primary_key=[],
                foreign_keys=[
                    ForeignKeyDef(
                        name="fk_order",
                        columns=["order_id"],
                        referenced_table="orders",
                        referenced_columns=["id"],
                    )
                ],
            )
        ]
    )
    item = _item_for(_assess(inventory), "line_items")
    # UNSUPPORTED outranks MANUAL.
    assert item.classification is Classification.UNSUPPORTED
    assert item.rule_id == "NO_PRIMARY_KEY"
    # Findings from both rules are preserved in the combined recommendation.
    assert "application layer" in item.recommendation.lower()
    assert "primary key" in item.recommendation.lower()
    # The most demanding effort across matched rules wins: FK is SIMPLE, no-PK
    # is MEDIUM, so the item is MEDIUM.
    assert item.effort is EffortLevel.MEDIUM


# ---------------------------------------------------------------------------
# Difficulty summary
# ---------------------------------------------------------------------------


def test_report_summary_counts_objects_by_classification() -> None:
    inventory = SourceInventory(
        tables=[
            _table_with_pk("clean"),  # AUTO
            _table_with_pk("auto_inc", auto_increment_column="id"),  # MANUAL
            TableDef(name="no_pk", primary_key=[]),  # UNSUPPORTED
        ]
    )
    report = _assess(inventory)
    assert report.summary == {
        Classification.AUTO: 1,
        Classification.MANUAL: 1,
        Classification.UNSUPPORTED: 1,
    }


# ---------------------------------------------------------------------------
# Property 8 — assessment completeness
# ---------------------------------------------------------------------------


def _representative_inventory() -> SourceInventory:
    return SourceInventory(
        tables=[
            _table_with_pk("clean"),
            _table_with_pk("auto_inc", auto_increment_column="id"),
            TableDef(name="no_pk", primary_key=[]),
            _table_with_pk(
                "ci_table",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(
                        name="label",
                        mysql_type="VARCHAR(50)",
                        collation="utf8mb4_unicode_ci",
                    ),
                ],
            ),
            _table_with_pk("partitioned", partitioned=True),
            _table_with_pk(
                "spatial",
                columns=[
                    ColumnDef(name="id", mysql_type="INT"),
                    ColumnDef(name="shape", mysql_type="POLYGON"),
                ],
            ),
            _table_with_pk(
                "with_fk",
                foreign_keys=[
                    ForeignKeyDef(
                        name="fk_x",
                        columns=["x_id"],
                        referenced_table="x",
                        referenced_columns=["id"],
                    )
                ],
            ),
        ],
        views=[ViewDef(name="v_orders", definition="SELECT 1")],
        triggers=[ObjectRef(name="trg_audit", object_type=ObjectType.TRIGGER)],
        routines=[ObjectRef(name="sp_recalc", object_type=ObjectType.ROUTINE)],
    )


def test_property_8_every_object_classified_exactly_once() -> None:
    """Property 8: every inventory object has exactly one classification."""
    inventory = _representative_inventory()
    report = _assess(inventory)

    expected_names = (
        [table.name for table in inventory.tables]
        + [view.name for view in inventory.views]
        + [trigger.name for trigger in inventory.triggers]
        + [routine.name for routine in inventory.routines]
    )

    # Exactly one item per object: no object left unclassified, none duplicated.
    assert len(report.items) == len(expected_names)
    assert sorted(item.object_name for item in report.items) == sorted(expected_names)

    valid = set(Classification)
    assert all(item.classification in valid for item in report.items)

    # The summary accounts for every object.
    assert sum(report.summary.values()) == len(expected_names)


def test_property_8_holds_for_empty_inventory() -> None:
    report = _assess(SourceInventory())
    assert report.items == []
    assert report.summary == {
        Classification.AUTO: 0,
        Classification.MANUAL: 0,
        Classification.UNSUPPORTED: 0,
    }


def test_assessor_accepts_custom_extensible_rules() -> None:
    class AlwaysManualRule(Rule):
        rule_id = "ALWAYS_MANUAL"

        def evaluate(self, inventory):
            from dsql_migrator.core.assessor import Finding, KIND_TABLE, ObjectKey

            return [
                Finding(
                    object=ObjectKey(KIND_TABLE, table.name),
                    rule_id=self.rule_id,
                    classification=Classification.MANUAL,
                    risk="custom",
                    recommendation="custom",
                )
                for table in inventory.tables
            ]

    inventory = SourceInventory(tables=[_table_with_pk("t")])
    report = CompatibilityAssessor(rules=[AlwaysManualRule()]).assess(inventory)
    item = _item_for(report, "t")
    assert item.rule_id == "ALWAYS_MANUAL"
    assert item.classification is Classification.MANUAL


def test_default_rules_contains_all_documented_rule_ids() -> None:
    rule_ids = {rule.rule_id for rule in default_rules()}
    assert rule_ids == {
        "FK_UNSUPPORTED",
        "TRIGGER_UNSUPPORTED",
        "PROC_PLPGSQL",
        "EVENT_UNSUPPORTED",
        "AUTO_INCREMENT",
        "NO_PRIMARY_KEY",
        "CI_COLLATION",
        "PARTITIONED_TABLE",
        "SPATIAL_TYPE",
        "TOO_MANY_COLUMNS",
        "OVERSIZED_LOB",
        "NUMERIC_PRECISION",
        "ENUM_SET_TYPE",
        "TINYINT_BOOLEAN",
        "BIT_TYPE",
        "YEAR_TYPE",
        "VIEW_UNSUPPORTED_SQL",
        "GENERATED_COLUMN",
        "ON_UPDATE_TIMESTAMP",
        "UNSUPPORTED_INDEX_TYPE",
    }


def test_generated_column_rule_classifies_table_manual() -> None:
    table = _table_with_pk(
        "orders",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="total", mysql_type="DECIMAL(10,2)", generated=True),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "orders")
    assert item.rule_id == "GENERATED_COLUMN"
    assert item.classification is Classification.MANUAL
    assert "total" in item.risk


def test_on_update_timestamp_rule_classifies_table_manual() -> None:
    table = _table_with_pk(
        "orders",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(
                name="updated_at",
                mysql_type="TIMESTAMP",
                auto_update_timestamp=True,
            ),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "orders")
    assert item.rule_id == "ON_UPDATE_TIMESTAMP"
    assert item.classification is Classification.MANUAL


def test_unsupported_index_type_rule_flags_fulltext_spatial() -> None:
    table = _table_with_pk(
        "articles",
        indexes=[
            IndexDef(name="ft_body", columns=["body"], index_type="FULLTEXT"),
            IndexDef(name="ix_name", columns=["name"], index_type="BTREE"),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "articles")
    assert item.rule_id == "UNSUPPORTED_INDEX_TYPE"
    assert item.classification is Classification.UNSUPPORTED
    assert "ft_body" in item.risk and "ix_name" not in item.risk


def test_event_unsupported_rule_classifies_event_unsupported() -> None:
    inventory = SourceInventory(
        events=[ObjectRef(name="evt_nightly", object_type=ObjectType.EVENT)]
    )
    item = _item_for(_assess(inventory), "evt_nightly")
    assert item.rule_id == "EVENT_UNSUPPORTED"
    assert item.classification is Classification.UNSUPPORTED
    assert item.kind == "EVENT"


def test_too_many_columns_rule_classifies_table_unsupported() -> None:
    wide = _table_with_pk(
        "wide",
        columns=[ColumnDef(name=f"c{i}", mysql_type="INT") for i in range(256)],
        primary_key=["c0"],
    )
    item = _item_for(_assess(SourceInventory(tables=[wide])), "wide")
    assert item.rule_id == "TOO_MANY_COLUMNS"
    assert item.classification is Classification.UNSUPPORTED


def test_too_many_columns_rule_allows_table_at_the_limit() -> None:
    at_limit = _table_with_pk(
        "ok",
        columns=[ColumnDef(name=f"c{i}", mysql_type="INT") for i in range(255)],
        primary_key=["c0"],
    )
    item = _item_for(_assess(SourceInventory(tables=[at_limit])), "ok")
    assert item.classification is Classification.AUTO


def test_oversized_lob_rule_flags_large_text_blob_manual() -> None:
    table = _table_with_pk(
        "docs",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="body", mysql_type="LONGTEXT"),
            ColumnDef(name="blob", mysql_type="MEDIUMBLOB"),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "docs")
    assert item.rule_id == "OVERSIZED_LOB"
    assert item.classification is Classification.MANUAL
    assert "body" in item.risk and "blob" in item.risk


def test_oversized_lob_rule_ignores_small_text_blob() -> None:
    table = _table_with_pk(
        "notes",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="note", mysql_type="TEXT"),
            ColumnDef(name="thumb", mysql_type="BLOB"),
        ],
    )
    # TEXT/BLOB (<= 64 KiB) fit within the DSQL 1 MiB limit -> no finding.
    item = _item_for(_assess(SourceInventory(tables=[table])), "notes")
    assert item.classification is Classification.AUTO


def test_decimal_precision_rule_flags_over_38_unsupported() -> None:
    table = _table_with_pk(
        "money",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="amount", mysql_type="DECIMAL(40,2)"),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "money")
    assert item.rule_id == "NUMERIC_PRECISION"
    assert item.classification is Classification.UNSUPPORTED


def test_decimal_precision_rule_allows_within_limit() -> None:
    table = _table_with_pk(
        "money_ok",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="amount", mysql_type="DECIMAL(38,4)"),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "money_ok")
    assert item.classification is Classification.AUTO


def test_enum_set_rule_classifies_table_manual() -> None:
    table = _table_with_pk(
        "users",
        columns=[
            ColumnDef(name="id", mysql_type="INT"),
            ColumnDef(name="status", mysql_type="ENUM('a','b')"),
        ],
    )
    item = _item_for(_assess(SourceInventory(tables=[table])), "users")
    assert item.rule_id == "ENUM_SET_TYPE"
    assert item.classification is Classification.MANUAL
    assert "status" in item.risk


def test_view_rule_flags_unsupported_sql_manual() -> None:
    inventory = SourceInventory(
        views=[
            ViewDef(
                name="v_report",
                definition="SELECT GROUP_CONCAT(name) AS names FROM app.users",
            )
        ]
    )
    item = _item_for(_assess(inventory), "v_report")
    assert item.rule_id == "VIEW_UNSUPPORTED_SQL"
    assert item.classification is Classification.MANUAL
    assert item.kind == "VIEW"


def test_view_rule_leaves_clean_view_auto() -> None:
    inventory = SourceInventory(
        views=[ViewDef(name="v_ok", definition="SELECT id, name FROM app.users")]
    )
    item = _item_for(_assess(inventory), "v_ok")
    assert item.classification is Classification.AUTO


def test_inventory_rule_flags_multiple_source_databases() -> None:
    inventory = SourceInventory(
        tables=[_table_with_pk("db1.orders"), _table_with_pk("db2.users")]
    )
    item = _item_for(_assess(inventory), "2 source databases")
    assert item.rule_id == "MULTIPLE_DATABASES"
    assert item.classification is Classification.MANUAL
    assert item.kind == "DATABASE"


def test_inventory_rule_single_database_emits_no_finding() -> None:
    inventory = SourceInventory(
        tables=[_table_with_pk("db1.orders"), _table_with_pk("db1.users")]
    )
    report = _assess(inventory)
    assert not any(item.rule_id == "MULTIPLE_DATABASES" for item in report.items)


def test_check_table_count_flags_over_the_limit_unsupported() -> None:
    over = SourceInventory(
        tables=[_table_with_pk(f"t{i}") for i in range(1001)]
    )
    (item,) = check_table_count(over)
    assert item.rule_id == "TABLE_COUNT_LIMIT"
    assert item.classification is Classification.UNSUPPORTED
    assert item.kind == "DATABASE"
    # At/under the limit -> no finding.
    assert check_table_count(SourceInventory(tables=[_table_with_pk("t")])) == []


# ---------------------------------------------------------------------------
# Report export
# ---------------------------------------------------------------------------


def test_export_report_json_is_valid_and_round_trips() -> None:
    report = _assess(_representative_inventory())
    payload = export_report(report, "json")
    parsed = json.loads(payload)
    assert "items" in parsed and "summary" in parsed
    assert AssessmentReport.model_validate(parsed) == report


def test_export_report_text_contains_summary_and_items() -> None:
    report = _assess(_representative_inventory())
    text = export_report(report, "text")
    assert "Compatibility Assessment Report" in text
    assert "Difficulty summary" in text
    assert "Estimated manual effort" in text
    assert "no_pk" in text
    assert "[UNSUPPORTED]" in text


def test_export_report_rejects_unknown_format() -> None:
    report = _assess(SourceInventory())
    try:
        export_report(report, "xml")
    except ValueError as exc:
        assert "unsupported report format" in str(exc)
    else:  # pragma: no cover - the call must raise
        raise AssertionError("expected ValueError for unknown format")


def test_export_report_html_is_standalone_document() -> None:
    report = _assess(_representative_inventory())
    markup = export_report(report, "html")
    assert markup.startswith("<!DOCTYPE html>")
    assert "<title>MySQL to Aurora DSQL Compatibility Assessment</title>" in markup
    assert "Classification summary" in markup
    assert "Estimated manual effort" in markup
    # Objects and their kinds appear in the table.
    assert "no_pk" in markup
    assert "TABLE" in markup
    assert markup.rstrip().endswith("</html>")


def test_export_report_html_escapes_content() -> None:
    inventory = SourceInventory(
        tables=[TableDef(name="t<x>", primary_key=[])]
    )
    markup = export_report(_assess(inventory), "html")
    # A '<' in an object name must be escaped, never emitted raw.
    assert "t&lt;x&gt;" in markup


def test_export_report_html_includes_conversion_chart() -> None:
    report = _assess(_representative_inventory())
    markup = export_report(report, "html")
    # The HTML export embeds the conversion-statistics chart (self-contained,
    # no external scripts) with its legend.
    assert "Conversion statistics by object kind" in markup
    assert 'class="chart"' in markup
    assert "% manual" in markup
    assert "Auto-converted" in markup


def test_export_report_html_has_client_side_filters() -> None:
    report = _assess(_representative_inventory())
    markup = export_report(report, "html")
    # Filter controls for type/classification/effort + search, and the rows
    # carry the data attributes the inline filter script reads.
    assert 'id="f-kind"' in markup
    assert 'id="f-class"' in markup
    assert 'id="f-effort"' in markup
    assert 'id="f-search"' in markup
    assert 'id="assessed-objects"' in markup
    assert "data-classification=" in markup and "data-kind=" in markup
    # The filter is self-contained inline JS (no external dependency).
    assert "addEventListener" in markup


def test_render_html_report_includes_target_analysis() -> None:
    from dsql_migrator.core.assessor import render_html_report
    from dsql_migrator.core.models import TargetInventory

    report = _assess(_representative_inventory())
    markup = render_html_report(
        report, target=TargetInventory(), conflicts=["public.users"]
    )
    assert "Target analysis (Aurora DSQL)" in markup
    assert "schemas" in markup
    # A name conflict is surfaced in the export.
    assert "public.users" in markup
    assert "may conflict" in markup
    # Without target data the section is omitted.
    assert "Target analysis (Aurora DSQL)" not in render_html_report(report)


def test_render_html_report_includes_ai_assessment_when_provided() -> None:
    from dsql_migrator.core.assessor import render_html_report
    from dsql_migrator.core.models import AiAssessmentReport

    report = _assess(_representative_inventory())
    ai = AiAssessmentReport(
        strategy_summary="## Migration strategy\nDo X then Y.", model_id="m"
    )
    markup = render_html_report(report, ai_report=ai)
    assert "AI-led migration assessment" in markup
    assert "Do X then Y." in markup
    # Without an AI report, the AI section is omitted.
    assert "AI-led migration assessment" not in render_html_report(report)


def test_render_text_report_matches_export_text() -> None:
    report = _assess(_representative_inventory())
    assert render_text_report(report) == export_report(report, "text")

