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


def test_auto_increment_rule_reads_as_throughput_advice_not_a_failure() -> None:
    """An AUTO_INCREMENT key converts cleanly; changing it is a throughput choice.

    Schema Conversion already made this correction (``ConversionNoteKind.RECOMMENDATION``
    in ``core/converter.py``, v0.1.151) while this rule kept saying the key "causes hot
    partitions in Aurora DSQL" -- so the two screens contradicted each other about the
    same key, and advice was worded like a defect.
    """
    inventory = SourceInventory(
        tables=[_table_with_pk("users", auto_increment_column="id")]
    )
    item = _item_for(_assess(inventory), "users")
    assert item.rule_id == "AUTO_INCREMENT"
    assert item.classification is Classification.MANUAL
    text = item.risk.lower()
    # Leads with what is TRUE of the table, not with a consequence.
    assert "converts cleanly" in text
    assert "throughput" in text
    # The partitioning mechanism is still explained -- that is the actionable part.
    assert "primary-key order" in text
    # But it is no longer asserted as a failure the operator has to fix.
    assert "cause hot partitions" not in text
    assert "optional" in item.recommendation.lower()


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
        "FK_CASCADE_CDC_GAP",
        "TRIGGER_UNSUPPORTED",
        "PROC_PLPGSQL",
        "EVENT_UNSUPPORTED",
        "AUTO_INCREMENT",
        "NO_PRIMARY_KEY",
        "CI_COLLATION",
        "PARTITIONED_TABLE",
        "SPATIAL_TYPE",
        "TOO_MANY_COLUMNS",
        "TOO_MANY_INDEXES",
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


def test_export_report_html_includes_compatibility_chart() -> None:
    report = _assess(_representative_inventory())
    markup = export_report(report, "html")
    # The HTML export embeds the per-kind chart (self-contained, no external
    # scripts) with its legend. Split by CLASSIFICATION, matching the UI chart --
    # the export follows the screen so the two never disagree.
    assert "Compatibility by object kind" in markup
    assert 'class="chart"' in markup
    assert "% need attention" in markup
    for label in ("Auto-converted", "Review needed", "Unsupported"):
        assert label in markup
    # Effort words belong to the effort summary, not to this chart's legend.
    assert "Simple actions" not in markup


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



# ---------------------------------------------------------------------------
# FK referential actions: the CDC gap (MySQL bug #32506)
# ---------------------------------------------------------------------------


def _fk(name="fk_child", *, on_delete=None, on_update=None):
    from dsql_migrator.core.models import ForeignKeyDef

    return ForeignKeyDef(
        name=name,
        columns=["parent_id"],
        referenced_table="parent",
        referenced_columns=["id"],
        on_delete=on_delete,
        on_update=on_update,
    )


def _child_table(*fks):
    from dsql_migrator.core.models import ColumnDef, TableDef

    return TableDef(
        name="child",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="parent_id", mysql_type="int"),
        ],
        primary_key=["id"],
        foreign_keys=list(fks),
    )


def test_has_cascade_action_only_for_actions_that_write_child_rows() -> None:
    # RESTRICT/NO ACTION only REJECT the parent change, so they never produce an
    # unlogged child write -- they must NOT be flagged. CASCADE / SET NULL /
    # SET DEFAULT all change child rows inside InnoDB, so all three must be.
    assert _fk(on_delete="CASCADE").has_cascade_action is True
    assert _fk(on_update="CASCADE").has_cascade_action is True
    assert _fk(on_delete="SET NULL").has_cascade_action is True
    assert _fk(on_delete="SET DEFAULT").has_cascade_action is True
    assert _fk(on_delete="RESTRICT").has_cascade_action is False
    assert _fk(on_delete="NO ACTION").has_cascade_action is False
    assert _fk().has_cascade_action is False  # default (no action recorded)
    # Case- and separator-insensitive (drivers report "SET NULL" or "SET_NULL").
    assert _fk(on_delete="set null").has_cascade_action is True
    assert _fk(on_delete="SET_NULL").has_cascade_action is True


def test_cascade_fk_rule_flags_the_cdc_gap_with_the_action_named() -> None:
    from dsql_migrator.core.assessor import CascadeForeignKeyRule

    inventory = SourceInventory(tables=[_child_table(_fk(on_delete="CASCADE"))])
    (finding,) = CascadeForeignKeyRule().evaluate(inventory)
    assert finding.rule_id == "FK_CASCADE_CDC_GAP"
    # MANUAL, not UNSUPPORTED: the table migrates fine; the operator has to move the
    # cascade into the application (which DSQL requires anyway).
    assert finding.classification is Classification.MANUAL
    # The risk must name the concrete action, why CDC misses it, and that it is silent.
    assert "ON DELETE CASCADE" in finding.risk
    assert "binary log" in finding.risk
    assert "no error" in finding.risk.lower()
    assert "#32506" in finding.risk
    # The recommendation must give the fix AND the interim safety net.
    assert "EXPLICIT" in finding.recommendation
    assert "orphan" in finding.recommendation.lower()


def test_cascade_fk_rule_reports_both_actions_and_skips_safe_ones() -> None:
    from dsql_migrator.core.assessor import CascadeForeignKeyRule

    both = _fk("fk_both", on_delete="CASCADE", on_update="CASCADE")
    inventory = SourceInventory(tables=[_child_table(both)])
    (finding,) = CascadeForeignKeyRule().evaluate(inventory)
    assert "ON DELETE CASCADE" in finding.risk and "ON UPDATE CASCADE" in finding.risk

    # A RESTRICT-only FK produces NO finding (it cannot cause unlogged child writes).
    safe = SourceInventory(tables=[_child_table(_fk(on_delete="RESTRICT"))])
    assert CascadeForeignKeyRule().evaluate(safe) == []
    # And a table with no FKs at all is untouched.
    assert CascadeForeignKeyRule().evaluate(
        SourceInventory(tables=[_table_with_pk("plain")])
    ) == []


def test_cascade_fk_rule_is_separate_from_the_plain_fk_finding() -> None:
    # Both rules fire on a CASCADE FK: one for the dropped constraint, one for the
    # un-replicable action. They answer different questions and must not be merged.
    from dsql_migrator.core.assessor import CascadeForeignKeyRule, ForeignKeyRule

    inventory = SourceInventory(tables=[_child_table(_fk(on_delete="CASCADE"))])
    assert [f.rule_id for f in ForeignKeyRule().evaluate(inventory)] == ["FK_UNSUPPORTED"]
    assert [f.rule_id for f in CascadeForeignKeyRule().evaluate(inventory)] == [
        "FK_CASCADE_CDC_GAP"
    ]


def test_foreign_key_def_defaults_keep_actions_optional() -> None:
    # Older persisted inventories (and non-MySQL reflection) carry no actions; the
    # model must accept that and report no cascade rather than failing validation.
    from dsql_migrator.core.models import ForeignKeyDef

    fk = ForeignKeyDef(
        name="fk", columns=["a"], referenced_table="p", referenced_columns=["id"]
    )
    assert fk.on_delete is None and fk.on_update is None
    assert fk.has_cascade_action is False


# ---------------------------------------------------------------------------
# DSQL per-table index limit (24, PRIMARY KEY included)
# ---------------------------------------------------------------------------


def _table_with_indexes(n: int, name: str = "wide"):
    """A table with a PK and ``n`` single-column secondary indexes."""
    return TableDef(
        name=name,
        columns=[ColumnDef(name="id", mysql_type="INT", nullable=False)]
        + [ColumnDef(name=f"c{i}", mysql_type="INT") for i in range(1, n + 1)],
        primary_key=["id"],
        indexes=[IndexDef(name=f"ix_{i}", columns=[f"c{i}"]) for i in range(1, n + 1)],
    )


def test_too_many_indexes_budget_reserves_one_slot_for_the_primary_key() -> None:
    # Verified against a live DSQL cluster: the 24th CREATE INDEX on a table that
    # already had a PK failed, and pg_indexes then showed 24 rows INCLUDING the PK.
    # So the source's reflected indexes (which exclude the PK) may number at most 23.
    from dsql_migrator.core.assessor import TooManyIndexesRule

    rule = TooManyIndexesRule()
    assert rule.evaluate(SourceInventory(tables=[_table_with_indexes(22)])) == []
    assert rule.evaluate(SourceInventory(tables=[_table_with_indexes(23)])) == []  # exactly at the limit
    (finding,) = rule.evaluate(SourceInventory(tables=[_table_with_indexes(24)]))
    assert finding.rule_id == "TOO_MANY_INDEXES"


def test_too_many_indexes_explains_the_post_load_failure_timing() -> None:
    # The whole point of catching this at planning time: secondary indexes are built
    # by post-load CREATE INDEX ASYNC, so the limit is hit only AFTER Full Load has
    # written every row -- and a re-run cannot fix it.
    from dsql_migrator.core.assessor import TooManyIndexesRule

    (finding,) = TooManyIndexesRule().evaluate(
        SourceInventory(tables=[_table_with_indexes(30)])
    )
    assert finding.classification is Classification.MANUAL  # solvable: drop indexes
    assert finding.effort is EffortLevel.MEDIUM
    # Names the real counts on both sides of the limit.
    assert "30 secondary indexes" in finding.risk
    assert "31" in finding.risk and "24" in finding.risk
    assert "MySQL allows" in finding.risk
    # The error the user would otherwise hit, and WHEN.
    assert "54000" in finding.risk
    assert "after the data loads" in finding.risk.lower() or "already written" in finding.risk.lower()
    # Actionable: how many to remove, where to look, and that re-running won't help.
    assert "23 secondary indexes" in finding.recommendation
    assert "unused" in finding.recommendation.lower()
    assert "not transient" in finding.recommendation.lower()


def test_too_many_indexes_reported_through_the_full_assessment() -> None:
    # End-to-end through the default rule set (the rule must be registered).
    inventory = SourceInventory(tables=[_table_with_indexes(25, name="orders")])
    item = _item_for(_assess(inventory), "orders")
    assert "TOO_MANY_INDEXES" in item.rule_id or "indexes" in item.risk
    assert item.classification is Classification.MANUAL


def test_clean_table_index_count_is_not_flagged() -> None:
    from dsql_migrator.core.assessor import TooManyIndexesRule

    # No indexes at all, and a normal handful, are both fine.
    assert TooManyIndexesRule().evaluate(
        SourceInventory(tables=[_table_with_pk("plain")])
    ) == []
    assert TooManyIndexesRule().evaluate(
        SourceInventory(tables=[_table_with_indexes(5)])
    ) == []


# ---------------------------------------------------------------------------
# Per-concern reporting. An object matching several rules used to collapse into
# ONE semicolon-joined risk string and ONE joined recommendation string, so the
# Nth risk sat in one paragraph and its fix in another, unpaired.
# ---------------------------------------------------------------------------


def _multi_rule_table() -> TableDef:
    """A table that trips five independent rules at once -- the reported case."""
    return TableDef(
        name="orders",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="user_id", mysql_type="int", nullable=False),
            ColumnDef(
                name="status",
                mysql_type="enum('pending','shipped')",
                collation="utf8mb4_general_ci",
            ),
            ColumnDef(
                name="updated_at",
                mysql_type="datetime",
                auto_update_timestamp=True,
            ),
        ],
        primary_key=["id"],
        auto_increment_column="id",
        foreign_keys=[
            ForeignKeyDef(
                name="fk_orders_users",
                columns=["user_id"],
                referenced_table="users",
                referenced_columns=["id"],
            )
        ],
    )


def test_each_matched_rule_becomes_its_own_concern() -> None:
    """Five rules -> five concerns, each risk paired with its own recommendation."""
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_multi_rule_table()])
    )
    item = next(i for i in report.items if i.object_name == "orders")

    rule_ids = [c.rule_id for c in item.concerns]
    assert len(item.concerns) >= 5, rule_ids
    for expected in (
        "FK_UNSUPPORTED",
        "AUTO_INCREMENT",
        "CI_COLLATION",
        "ENUM_SET_TYPE",
        "ON_UPDATE_TIMESTAMP",
    ):
        assert expected in rule_ids, (expected, rule_ids)

    # Every concern carries BOTH halves, so no fix is orphaned from its risk.
    for concern in item.concerns:
        assert concern.risk, concern.rule_id
        assert concern.recommendation, concern.rule_id
        # ...and no concern is itself a joined multi-finding string.
        assert "; " not in concern.risk or concern.risk.count("; ") < 2


def test_concerns_are_ordered_most_severe_first() -> None:
    # The governing (most severe) classification must lead, so the worst problem is read
    # first rather than buried at position four.
    from dsql_migrator.core.models import Classification

    severity = {
        Classification.UNSUPPORTED: 2,
        Classification.MANUAL: 1,
        Classification.AUTO: 0,
    }
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_multi_rule_table()])
    )
    item = next(i for i in report.items if i.object_name == "orders")
    ranks = [severity[c.classification] for c in item.concerns]
    assert ranks == sorted(ranks, reverse=True), ranks
    # The item's own class is the governing one.
    assert item.classification is item.concerns[0].classification


def test_joined_strings_still_carry_every_finding() -> None:
    """The flat strings stay for back-compat and CSV-style exports.

    Concerns are the presentation surface, but nothing may DROP a finding from the
    joined text -- a downstream consumer reading only `risk` must still see all of it.
    """
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_multi_rule_table()])
    )
    item = next(i for i in report.items if i.object_name == "orders")
    for concern in item.concerns:
        assert concern.risk in item.risk
        assert concern.recommendation in item.recommendation


def test_auto_object_has_no_concerns_and_keeps_its_reassurance() -> None:
    # Nothing matched, so there is nothing to enumerate -- and the "no issues" line must
    # not disappear with the change.
    report = CompatibilityAssessor().assess(
        SourceInventory(
            tables=[
                TableDef(
                    name="users",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                )
            ]
        )
    )
    item = next(i for i in report.items if i.object_name == "users")
    assert item.concerns == []
    assert "No DSQL compatibility issues detected." in render_text_report(report)


def test_text_report_numbers_each_concern_with_its_fix() -> None:
    """The text export must enumerate, not concatenate."""
    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_multi_rule_table()])
    )
    text = render_text_report(report)
    assert "    1. [MANUAL] FK_UNSUPPORTED" in text
    assert "       Risk: Foreign key constraints" in text
    assert "       Fix:  Remove the foreign key" in text
    # The old single run-on Risk line is gone for a multi-rule object.
    assert "Aurora DSQL.; AUTO_INCREMENT column" not in text


def test_html_report_gives_each_concern_its_own_row() -> None:
    """One row per finding, so each risk sits physically beside its own fix.

    Listing the risks in one cell and the fixes in another still asked the reader to
    count list positions across two columns to pair them. A row per finding also lets
    each carry its own rule id, classification and effort -- the object-level row could
    only show the governing (worst) one.
    """
    from dsql_migrator.core.assessor import render_html_report

    report = CompatibilityAssessor().assess(
        SourceInventory(tables=[_multi_rule_table()])
    )
    item = next(i for i in report.items if i.object_name == "orders")
    markup = render_html_report(report)

    # The object cell spans its findings instead of being repeated.
    assert f'rowspan="{len(item.concerns)}"' in markup
    # Continuation rows are marked so the filter counter still counts OBJECTS.
    assert markup.count('data-concern="1"') == len(item.concerns) - 1
    # Every risk and its own fix are present as plain cells (no <ul> pairing game).
    # Compared against the ESCAPED text: the cells go through html.escape, so a risk
    # naming a column as 'id' arrives as &#x27;id&#x27; -- asserting on the raw string
    # would fail for a reason that has nothing to do with the layout.
    import html as _html

    for concern in item.concerns:
        assert f"<td>{_html.escape(concern.risk)}</td>" in markup, concern.rule_id
        assert (
            f"<td>{_html.escape(concern.recommendation)}</td>" in markup
        ), concern.rule_id
        assert f"<td>{concern.rule_id}</td>" in markup
    # Per-concern effort, not just the item's governing one.
    assert "<td>SIMPLE</td>" in markup and "<td>MEDIUM</td>" in markup


def test_html_filter_counts_objects_not_findings() -> None:
    # With a row per finding, a naive counter would report "9 of 9 shown" for one object
    # with five findings. The script must exclude the continuation rows.
    from dsql_migrator.core.assessor import render_html_report

    markup = render_html_report(
        CompatibilityAssessor().assess(SourceInventory(tables=[_multi_rule_table()]))
    )
    assert "tr[data-kind]:not([data-concern])" in markup
    assert "if(ok&&!r.dataset.concern)shown++" in markup


def test_classification_stats_by_kind_orders_by_total_count_desc() -> None:
    """The chart is a size ranking, so bars must step down in length.

    Ordering by trouble-share instead floated a single unsupported TRIGGER above a
    hundred tables -- a short bar sitting on top of long ones reads as a broken chart.
    Each bar already carries its own red segment and "% need attention" caption, so the
    severity signal does not depend on the row order.
    """
    from dsql_migrator.core.assessor import classification_stats_by_kind
    from dsql_migrator.core.models import AssessmentItem

    items = [
        AssessmentItem(
            object_name=f"t{i}",
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            kind="TABLE",
        )
        for i in range(5)
    ] + [
        AssessmentItem(
            object_name="v1",
            rule_id="VIEW_REVIEW",
            classification=Classification.MANUAL,
            kind="VIEW",
        ),
        AssessmentItem(
            object_name="v2",
            rule_id="VIEW_REVIEW",
            classification=Classification.MANUAL,
            kind="VIEW",
        ),
        # 100% UNSUPPORTED but only one object: it must NOT outrank the five tables.
        AssessmentItem(
            object_name="trg",
            rule_id="TRIGGER_UNSUPPORTED",
            classification=Classification.UNSUPPORTED,
            kind="TRIGGER",
        ),
    ]
    stats = classification_stats_by_kind(AssessmentReport.from_items(items))
    assert [kind for kind, _by_class, _total in stats] == ["TABLE", "VIEW", "TRIGGER"]
    assert [total for _kind, _by_class, total in stats] == [5, 2, 1]
    # Totals are non-increasing, which is the property that keeps the chart readable.
    totals = [total for _kind, _by_class, total in stats]
    assert totals == sorted(totals, reverse=True)


def test_classification_stats_by_kind_breaks_count_ties_by_name() -> None:
    # A stable order matters: the same report must not shuffle its bars between renders.
    from dsql_migrator.core.assessor import classification_stats_by_kind
    from dsql_migrator.core.models import AssessmentItem

    items = [
        AssessmentItem(
            object_name="z",
            rule_id="ROUTINE_UNSUPPORTED",
            classification=Classification.UNSUPPORTED,
            kind="PROCEDURE",
        ),
        AssessmentItem(
            object_name="a",
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            kind="FUNCTION",
        ),
    ]
    stats = classification_stats_by_kind(AssessmentReport.from_items(items))
    assert [kind for kind, _by_class, _total in stats] == ["FUNCTION", "PROCEDURE"]


def test_html_chart_lists_kinds_largest_first() -> None:
    # The export follows the UI, so its bar rows must come out in the same order.
    import re

    from dsql_migrator.core.assessor import render_html_report
    from dsql_migrator.core.models import AssessmentItem

    items = [
        AssessmentItem(
            object_name=f"t{i}",
            rule_id="COMPATIBLE",
            classification=Classification.AUTO,
            kind="TABLE",
        )
        for i in range(4)
    ] + [
        AssessmentItem(
            object_name="trg",
            rule_id="TRIGGER_UNSUPPORTED",
            classification=Classification.UNSUPPORTED,
            kind="TRIGGER",
        ),
    ]
    markup = render_html_report(AssessmentReport.from_items(items))
    labels = re.findall(r'<div class="bar-label">([^<]+)</div>', markup)
    assert labels == ["TABLE", "TRIGGER"]

def test_advisory_finding_does_not_inflate_the_effort_estimate() -> None:
    """Effort answers "how much work must I do", so optional advice must not raise it.

    A table needing only a foreign-key workaround (SIMPLE, under two hours) was reported
    as MEDIUM (two to six) purely because it ALSO carried the AUTO_INCREMENT throughput
    recommendation -- and since MySQL tables overwhelmingly have an AUTO_INCREMENT key,
    that inflated the estimate for the most common table shape there is. Measured on a
    real 7-table schema, this moved two tables from MEDIUM back to SIMPLE.
    """
    from dsql_migrator.core.models import ConversionNoteKind

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="orders",
                columns=[
                    ColumnDef(name="id", mysql_type="int", nullable=False),
                    ColumnDef(name="customer_id", mysql_type="int"),
                ],
                primary_key=["id"],
                auto_increment_column="id",
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
    by_rule = {c.rule_id: c for c in item.concerns}
    # The advisory finding is present and still carries its OWN effort, so the cost of
    # taking the advice remains visible on the finding itself.
    advisory = by_rule["AUTO_INCREMENT"]
    assert advisory.note_kind is ConversionNoteKind.RECOMMENDATION
    assert advisory.is_advisory
    assert advisory.effort is EffortLevel.MEDIUM
    # The real gap is SIMPLE, and that -- not the advice's MEDIUM -- governs the object.
    assert by_rule["FK_UNSUPPORTED"].note_kind is ConversionNoteKind.LOSS
    assert not by_rule["FK_UNSUPPORTED"].is_advisory
    assert item.effort is EffortLevel.SIMPLE


def test_an_object_whose_only_finding_is_advice_carries_no_effort() -> None:
    # Nothing is required of the operator, so the object must not borrow the advice's
    # estimate and appear in the effort summary as work to schedule.
    inventory = SourceInventory(
        tables=[_table_with_pk("users", auto_increment_column="id")]
    )
    report = _assess(inventory)
    item = _item_for(report, "users")
    assert [c.rule_id for c in item.concerns] == ["AUTO_INCREMENT"]
    assert all(c.is_advisory for c in item.concerns)
    assert item.effort is None
    assert report.effort_summary[EffortLevel.MEDIUM] == 0


def test_findings_default_to_loss_so_every_other_rule_is_unchanged() -> None:
    # LOSS is what every rule historically meant; only AUTO_INCREMENT opts out. A rule
    # that silently became advisory would quietly drop out of the effort estimate.
    inventory = SourceInventory(tables=[_multi_rule_table()])
    item = _item_for(_assess(inventory), "orders")
    advisory = {c.rule_id for c in item.concerns if c.is_advisory}
    assert advisory == {"AUTO_INCREMENT"}, advisory


def test_text_export_labels_an_advisory_finding_as_recommended() -> None:
    # The export must not call an optional throughput change a MANUAL risk; a reader
    # planning the migration needs to see which findings are required work.
    inventory = SourceInventory(tables=[_multi_rule_table()])
    text = render_text_report(_assess(inventory))
    assert "[RECOMMENDED] AUTO_INCREMENT" in text
    assert "effort if you take it: MEDIUM" in text
    # It is captioned Note, not Risk, and the gaps keep their Risk caption.
    assert "Note: The integer key" in text
    assert "Risk: Foreign key" in text


def test_html_export_marks_an_advisory_finding_outside_the_severity_ramp() -> None:
    from dsql_migrator.core.assessor import _HTML_ADVISORY_COLOR, render_html_report

    markup = render_html_report(
        CompatibilityAssessor().assess(SourceInventory(tables=[_multi_rule_table()]))
    )
    # The advisory row reads RECOMMENDED on the info-blue background rather than MANUAL
    # on amber, so the exported table draws the same distinction as the screen.
    assert f'<td style="background:{_HTML_ADVISORY_COLOR}">RECOMMENDED</td>' in markup
    assert "<td>MEDIUM (if taken)</td>" in markup
