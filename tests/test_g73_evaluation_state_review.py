"""G73: the review of G56-G67 -- a refreshed report of another source, the shell's
Re-run, the restored engine, a refresh that steps aside."""

from __future__ import annotations

from dsql_migrator.core.models import (
    AssessmentItem,
    AssessmentReport,
    Classification,
    SourceInventory,
    SourceType,
    TargetInventory,
)
from dsql_migrator.ui.evaluation import (
    EvaluationResult,
    EvaluationState,
    evaluation_run_guard_reason,
    refresh_stepped_aside_text,
    with_refreshed_catalogs,
)


def _probed_pg_result() -> EvaluationResult:
    from dsql_migrator.core.assessor_postgres import PG_CATALOG_CHECKED_RULE_ID

    checked = AssessmentItem(
        object_name="source catalog", kind="DATABASE", rule_id=PG_CATALOG_CHECKED_RULE_ID,
        classification=Classification.AUTO, risk="no table with row-level security",
        recommendation="",
    )
    return EvaluationResult(
        inventory=SourceInventory(tables=[]),
        assessment=AssessmentReport.from_items([checked]),
        target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[],
        source_type=SourceType.POSTGRES,
        source_label="PostgreSQL 16.4 at pg-a.example:5432/sales",
    )


def test_a_refresh_of_another_source_does_not_carry_the_first_sources_probes() -> None:
    """Re-stamped as database B, the report kept A's 'no row-level security' probe and read
    Ready for a database nobody probed."""
    from dsql_migrator.core.assessor_postgres import (
        PG_CATALOG_CHECKED_RULE_ID,
        PG_CATALOG_UNREAD_RULE_ID,
    )

    result = _probed_pg_result()
    other = with_refreshed_catalogs(
        result, inventory=SourceInventory(tables=[]), target_inventory=TargetInventory(schemas=[]),
        target_endpoint=None, source_label="PostgreSQL 16.4 at pg-b.example:5432/billing",
    )
    rules = {i.rule_id for i in other.assessment.items}
    assert PG_CATALOG_CHECKED_RULE_ID not in rules and PG_CATALOG_UNREAD_RULE_ID in rules
    same = with_refreshed_catalogs(
        result, inventory=SourceInventory(tables=[]), target_inventory=TargetInventory(schemas=[]),
        target_endpoint=None, source_label="PostgreSQL 16.4 at PG-A.example:5432/sales",
    )
    assert PG_CATALOG_CHECKED_RULE_ID in {i.rule_id for i in same.assessment.items}


def test_the_shells_rerun_is_guarded_on_tested_connections() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig, TargetConnectionConfig
    from dsql_migrator.ui.session import SessionStore

    session = SessionStore().get_or_create("s1")
    session.set_source(SourceConnectionConfig(host="db", database="shop"), None)
    session.set_target(TargetConnectionConfig(cluster_endpoint="c.dsql.example", region="us-east-1"))
    assert "not been tested" in (evaluation_run_guard_reason(session) or "")
    session.set_source_verified(True)
    session.set_target_verified(True)
    assert evaluation_run_guard_reason(session) is None


def test_a_refresh_that_steps_aside_says_why_or_nothing_after_start_over() -> None:
    state = EvaluationState()
    assert refresh_stepped_aside_text(state) is None  # reset: no result to speak of
    state.set_result(_probed_pg_result())
    assert "another refresh, or another tab" in refresh_stepped_aside_text(state)


def test_a_restored_report_of_another_engine_keeps_its_engine() -> None:
    """A MySQL report restored under a PostgreSQL connection was re-graded by PostgreSQL
    rules under its MySQL label."""
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.core.session_state_store import SessionSnapshot
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import (
        apply_session_snapshot,
        capture_session_snapshot,
    )

    session = SessionConnectionState()
    session.set_source(
        SourceConnectionConfig(source_type=SourceType.POSTGRES, host="pg-b.example", port=5432,
                               database="billing"),
        None,
    )
    eval_state = EvaluationState()
    eval_state.set_result(EvaluationResult(
        inventory=SourceInventory(tables=[]),
        assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[],
        source_type=SourceType.MYSQL,
        source_label="MySQL 8.0.39 at mysql-a.example:3306/shop",
    ))
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, SchemaConversionState(), DataMigrationState()
    )
    assert snapshot.evaluation_source_type == "mysql"
    restored = EvaluationState()
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(snapshot.model_dump_json()),
        SessionConnectionState(), restored, SchemaConversionState(), DataMigrationState(),
    )
    assert restored.result.source_type is SourceType.MYSQL
