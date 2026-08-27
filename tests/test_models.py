# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the shared data models.

Covers input validation (Requirement 9.4), serialization round-trips, the
assessment summary helper, and credential-safety of connection models
(Requirement 9.2 / Property 7).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dsql_migrator.config import SecretRef, SecretSource
from dsql_migrator.core.models import (
    AssessmentItem,
    AssessmentReport,
    ChunkState,
    Classification,
    ColumnDef,
    ForeignKeyDef,
    IndexDef,
    MigrationJob,
    ObjectRef,
    ObjectType,
    SourceConnectionConfig,
    SourceInventory,
    StepStatus,
    TableDef,
    TargetConnectionConfig,
    ViewDef,
    Watermark,
    WorkflowState,
    apply_lob_exclusions,
)


def test_source_connection_config_defaults_and_validation() -> None:
    config = SourceConnectionConfig(host="db.example.com", database="app")
    assert config.port == 3306
    assert config.username is None
    assert config.secret is None


def test_source_connection_config_rejects_invalid_port() -> None:
    with pytest.raises(ValidationError):
        SourceConnectionConfig(host="db", database="app", port=70000)


def test_source_connection_config_rejects_empty_host() -> None:
    with pytest.raises(ValidationError):
        SourceConnectionConfig(host="", database="app")


def test_source_connection_config_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        SourceConnectionConfig(host="db", database="app", password="leak")


def test_source_connection_config_uses_secret_reference_not_value() -> None:
    ref = SecretRef(source=SecretSource.SECRETS_MANAGER, locator="arn:aws:secret:db")
    config = SourceConnectionConfig(host="db", database="app", secret=ref)
    dumped = config.model_dump()
    assert dumped["secret"]["locator"] == "arn:aws:secret:db"
    assert "password" not in dumped


def test_target_connection_config_defaults() -> None:
    config = TargetConnectionConfig(cluster_endpoint="abc.dsql.us-east-1.on.aws", region="us-east-1")
    assert config.database == "postgres"
    assert config.username == "admin"


def test_table_def_round_trip_serialization() -> None:
    table = TableDef(
        name="orders",
        columns=[ColumnDef(name="id", mysql_type="INT"), ColumnDef(name="total", mysql_type="DECIMAL")],
        primary_key=["id"],
        indexes=[IndexDef(name="idx_total", columns=["total"], unique=False)],
        foreign_keys=[
            ForeignKeyDef(
                name="fk_customer",
                columns=["customer_id"],
                referenced_table="customers",
                referenced_columns=["id"],
            )
        ],
        auto_increment_column="id",
    )
    restored = TableDef.model_validate(table.model_dump())
    assert restored == table


def test_index_def_requires_at_least_one_column() -> None:
    with pytest.raises(ValidationError):
        IndexDef(name="idx_empty", columns=[])


def test_source_inventory_round_trip() -> None:
    inventory = SourceInventory(
        tables=[TableDef(name="users")],
        views=[ViewDef(name="active_users", definition="SELECT 1")],
        triggers=[ObjectRef(name="trg_audit", object_type=ObjectType.TRIGGER)],
        routines=[ObjectRef(name="sp_calc", object_type=ObjectType.ROUTINE)],
    )
    restored = SourceInventory.model_validate(inventory.model_dump())
    assert restored == inventory


def test_assessment_report_from_items_computes_complete_summary() -> None:
    items = [
        AssessmentItem(object_name="t1", rule_id="FK_PRESERVED", classification=Classification.MANUAL),
        AssessmentItem(object_name="t2", rule_id="NO_PRIMARY_KEY", classification=Classification.UNSUPPORTED),
        AssessmentItem(object_name="t3", rule_id="OK", classification=Classification.AUTO),
        AssessmentItem(object_name="t4", rule_id="FK_PRESERVED", classification=Classification.MANUAL),
    ]
    report = AssessmentReport.from_items(items)
    assert report.summary == {
        Classification.AUTO: 1,
        Classification.MANUAL: 2,
        Classification.UNSUPPORTED: 1,
    }
    assert len(report.items) == 4


def test_assessment_report_summary_present_for_all_classifications_when_empty() -> None:
    report = AssessmentReport.from_items([])
    assert report.summary == {
        Classification.AUTO: 0,
        Classification.MANUAL: 0,
        Classification.UNSUPPORTED: 0,
    }


def test_chunk_state_defaults_and_status_literal() -> None:
    chunk = ChunkState(chunk_id="orders:0")
    assert chunk.status == "PENDING"
    assert chunk.rows_loaded == 0
    with pytest.raises(ValidationError):
        ChunkState(chunk_id="orders:1", status="UNKNOWN")


def test_migration_job_progress_bounds() -> None:
    job = MigrationJob(job_id="job-1")
    assert job.status == "PENDING"
    assert job.watermark is None
    with pytest.raises(ValidationError):
        MigrationJob(job_id="job-2", progress_pct=150.0)


def test_watermark_defaults_allow_missing_binlog_metadata() -> None:
    watermark = Watermark(snapshot_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert watermark.binlog_file is None
    assert watermark.binlog_position is None
    assert watermark.gtid_executed is None
    assert watermark.server_uuid is None
    assert watermark.table_row_counts == {}


def test_watermark_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        Watermark(
            snapshot_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            unexpected="x",
        )


def test_watermark_rejects_negative_binlog_position() -> None:
    with pytest.raises(ValidationError):
        Watermark(
            snapshot_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
            binlog_position=-1,
        )


def test_watermark_round_trip_serialization() -> None:
    watermark = Watermark(
        binlog_file="mysql-bin.000123",
        binlog_position=45678,
        gtid_executed="uuid:1-5",
        server_uuid="server-uuid",
        snapshot_timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        table_row_counts={"orders": 5, "customers": 3},
    )
    restored = Watermark.model_validate(watermark.model_dump())
    assert restored == watermark


def test_workflow_state_defaults_to_not_started() -> None:
    state = WorkflowState()
    assert state.evaluation == StepStatus.NOT_STARTED
    assert state.schema_conversion == StepStatus.NOT_STARTED
    assert state.data_migration == StepStatus.NOT_STARTED
    assert state.validation == StepStatus.NOT_STARTED


# ---------------------------------------------------------------------------
# apply_lob_exclusions: the single migration-wide LOB-exclusion rule
# ---------------------------------------------------------------------------


def _lob_table() -> TableDef:
    return TableDef(
        name="app.docs",
        primary_key=["id"],
        columns=[
            ColumnDef(name="id", mysql_type="int"),
            ColumnDef(name="name", mysql_type="varchar(100)"),
            ColumnDef(name="blob_doc", mysql_type="longtext"),
        ],
    )


def test_apply_lob_exclusions_drops_only_the_named_columns() -> None:
    filtered = apply_lob_exclusions(_lob_table(), ["blob_doc"])
    assert [c.name for c in filtered.columns] == ["id", "name"]
    # Name and PK are preserved so downstream name-/PK-based lookups still resolve.
    assert filtered.name == "app.docs"
    assert filtered.primary_key == ["id"]


def test_apply_lob_exclusions_never_drops_a_pk_column() -> None:
    # A PK anchors keyset streaming + ON CONFLICT, so it is kept even if listed.
    filtered = apply_lob_exclusions(_lob_table(), ["id", "blob_doc"])
    assert [c.name for c in filtered.columns] == ["id", "name"]
    assert filtered.primary_key == ["id"]


def test_apply_lob_exclusions_returns_same_object_when_nothing_to_drop() -> None:
    table = _lob_table()
    # None, empty, and PK-only exclusions are all no-ops -> identity (no copy).
    assert apply_lob_exclusions(table, None) is table
    assert apply_lob_exclusions(table, []) is table
    assert apply_lob_exclusions(table, ["id"]) is table


def test_apply_lob_exclusions_ignores_unknown_column_names() -> None:
    # A stale/unknown name simply matches nothing; the table is unchanged.
    filtered = apply_lob_exclusions(_lob_table(), ["not_a_column", "blob_doc"])
    assert [c.name for c in filtered.columns] == ["id", "name"]
