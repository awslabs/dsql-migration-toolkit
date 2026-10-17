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


# --- G81: the review of G68-G78 (evaluation state) --------------------------------------


def _mysql_result(label: str = "MySQL 8.0.39 at mysql-a.example:3306/shop") -> EvaluationResult:
    return EvaluationResult(
        inventory=SourceInventory(tables=[]),
        assessment=AssessmentReport.from_items([]),
        target_inventory=TargetInventory(schemas=[]),
        target_conflicts=[],
        source_type=SourceType.MYSQL,
        source_label=label,
    )


def test_a_mysql_refresh_onto_another_source_says_its_privileges_were_not_checked() -> None:
    """A's listing probe was rightly not carried onto B, but nothing said B's was never run:
    the report read 100/Ready."""
    from dsql_migrator.core.assessor import SOURCE_VISIBILITY_RULE_ID

    other = with_refreshed_catalogs(
        _mysql_result(), inventory=SourceInventory(tables=[]),
        target_inventory=TargetInventory(schemas=[]), target_endpoint=None,
        source_label="MySQL 8.0.39 at mysql-b.example:3306/shop",
    )
    (item,) = [i for i in other.assessment.items if i.rule_id == SOURCE_VISIBILITY_RULE_ID]
    assert "was not checked for this source" in item.risk
    same = with_refreshed_catalogs(
        _mysql_result(), inventory=SourceInventory(tables=[]),
        target_inventory=TargetInventory(schemas=[]), target_endpoint=None,
        source_label="MySQL 8.0.39 at mysql-a.example:3306/shop",
    )
    assert SOURCE_VISIBILITY_RULE_ID not in {i.rule_id for i in same.assessment.items}


def test_an_ipv6_source_is_bracketed_and_an_unreadable_address_is_never_the_same() -> None:
    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.evaluation import describe_source, same_source_address, source_address

    a = describe_source(SourceConnectionConfig(
        source_type=SourceType.POSTGRES, host="fd00::a", port=5432, database="sales"
    ), "PostgreSQL")
    b = describe_source(SourceConnectionConfig(
        source_type=SourceType.POSTGRES, host="fd00::b", port=5432, database="billing"
    ), "PostgreSQL")
    assert a == "PostgreSQL at [fd00::a]:5432/sales"
    assert source_address(a) == "[fd00::a]:5432/sales"
    assert not same_source_address(source_address(a), source_address(b))
    assert same_source_address(source_address(a), source_address(a))
    assert not same_source_address("fd00::a:5432/sales", "fd00::b:5432/billing")


def test_an_engine_switch_at_the_same_address_keeps_the_assessed_engine() -> None:
    """A fixed local port-forward re-pointed from MySQL to PostgreSQL: the snapshot kept
    neither label nor engine, and the MySQL report was restored under PostgreSQL rules."""
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
        SourceConnectionConfig(source_type=SourceType.POSTGRES, host="127.0.0.1", port=15000,
                               database="app"),
        None,
    )
    eval_state = EvaluationState()
    eval_state.set_result(_mysql_result("MySQL 8.0.39 at 127.0.0.1:15000/app"))
    snapshot = capture_session_snapshot(
        "s1", session, eval_state, SchemaConversionState(), DataMigrationState()
    )
    assert snapshot.evaluation_source_type == "mysql"
    assert snapshot.evaluation_source_label == "MySQL 8.0.39 at 127.0.0.1:15000/app"
    restored = EvaluationState()
    apply_session_snapshot(
        SessionSnapshot.model_validate_json(snapshot.model_dump_json()),
        SessionConnectionState(), restored, SchemaConversionState(), DataMigrationState(),
    )
    assert restored.result.source_type is SourceType.MYSQL


def test_a_re_stamp_or_a_source_switch_changes_the_snapshot_signature() -> None:
    """A refresh that only re-stamped the label, and a Connect source switch, left the
    signature unchanged, so neither was saved and a restart restored the old label."""
    from dataclasses import replace

    from dsql_migrator.core.models import SourceConnectionConfig
    from dsql_migrator.ui.data_migration import DataMigrationState
    from dsql_migrator.ui.schema_conversion import SchemaConversionState
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.session_persistence import session_signature

    session = SessionConnectionState()
    eval_state = EvaluationState()
    eval_state.set_result(_mysql_result())
    conv, migration = SchemaConversionState(), DataMigrationState()
    before = session_signature(session, eval_state, conv, migration)
    eval_state.set_result(replace(eval_state.result, source_label="MySQL 8.0.39 at b.example:3306/shop"))
    restamped = session_signature(session, eval_state, conv, migration)
    assert restamped != before
    session.set_source(
        SourceConnectionConfig(source_type=SourceType.MYSQL, host="b.example", port=3306,
                               database="shop"),
        None,
    )
    assert session_signature(session, eval_state, conv, migration) != restamped
