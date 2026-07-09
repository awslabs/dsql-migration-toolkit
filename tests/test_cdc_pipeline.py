# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the CDC pipeline control-plane orchestrator (Task 23.1).

Covers (Requirement 12 / Property 11 / Property 15 / Property 1):
- build_source_config maps the selection to table.include.list, uses
  snapshot_mode=recovery (rebuild schema-history from the live DB, no row
  re-read), and seeds the start offset from the Full Load watermark (gapless
  handoff).
- build_sink_config produces per-table topics + PK-keyed idempotent upsert/delete
  + DLQ for the custom DSQL Sink Connector.
- status() relays injected read-only connector status (empty without a source).
- surface_errors() records one credential-free DataErrorRecord per connector/DLQ
  error into the single error log (no-op without a source).

All sources are injected fakes: no AWS/MSK is reached and the source DB is never
written (the orchestrator only builds configs and relays read-only signals).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dsql_migrator.core.cdc import (
    CdcConnectorError,
    CdcPipelineOrchestrator,
    CdcResumePoint,
    ConnectorState,
    ConnectorStatus,
    build_cdc_status_view,
    composite_cdc_excluded_key_columns,
    composite_key_columns_for_cdc,
    format_message_key_columns,
)
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import ErrorLogSummary, LoadKind, TableDef, Watermark


def _tables() -> list[TableDef]:
    return [
        TableDef(name="app.orders", primary_key=["id"]),
        TableDef(name="app.customers", primary_key=["id"]),
    ]


def _watermark() -> Watermark:
    return Watermark(
        binlog_file="mysql-bin.000123",
        binlog_position=45678,
        gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
        server_uuid="3E11FA47-71CA-11E1-9E33-C80AA9429562",
        snapshot_timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
    )


def test_build_source_config_seeds_offset_from_watermark() -> None:
    orch = CdcPipelineOrchestrator()
    config = orch.build_source_config("src", _tables(), _watermark())

    assert config.name == "src"
    assert config.table_include_list == ["app.orders", "app.customers"]
    # recovery: rebuild schema-history from the live DB (no row re-read) since the
    # offset is seeded -- schema_only would die "db history topic is missing".
    assert config.snapshot_mode == "recovery"
    # Gapless handoff (Property 11): start offset == the snapshot watermark.
    assert config.start_gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert config.start_binlog_file == "mysql-bin.000123"
    assert config.start_binlog_pos == 45678


def test_build_source_config_without_coordinates() -> None:
    orch = CdcPipelineOrchestrator()
    watermark = Watermark(
        snapshot_timestamp=datetime(2026, 5, 6, 7, 8, 9, tzinfo=timezone.utc)
    )
    config = orch.build_source_config("src", _tables(), watermark)
    assert config.start_gtid is None
    assert config.start_binlog_file is None
    assert config.start_binlog_pos is None


def test_build_source_config_column_exclude_list_default_empty() -> None:
    # H13: no exclusions unless the caller asks (no silent data loss).
    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark()
    )
    assert config.column_exclude_list == []


def test_build_source_config_passes_column_exclude_list() -> None:
    # H13: the opt-in oversized-LOB exclusion flows to column.exclude.list, which
    # maps to the cdc-stack ColumnExcludeList parameter.
    config = CdcPipelineOrchestrator().build_source_config(
        "src",
        _tables(),
        _watermark(),
        column_exclude_list=["app.orders.notes", "app.customers.avatar"],
    )
    assert config.column_exclude_list == [
        "app.orders.notes",
        "app.customers.avatar",
    ]


def test_build_source_config_resume_override_takes_precedence() -> None:
    # A manual start position overrides the watermark coordinates entirely.
    override = CdcResumePoint(
        gtid_executed="99999999-1111-2222-3333-444444444444:1-9",
    )
    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark(), resume_override=override
    )
    assert config.start_gtid == "99999999-1111-2222-3333-444444444444:1-9"
    # Watermark's binlog coords are NOT used when an override is supplied.
    assert config.start_binlog_file is None
    assert config.start_binlog_pos is None
    # Manual override path uses schema_only (no pre-existing schema-history topic).
    assert config.snapshot_mode == "schema_only"


def test_build_source_config_resume_override_binlog() -> None:
    override = CdcResumePoint(binlog_file="mysql-bin.000999", binlog_position=12345)
    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark(), resume_override=override
    )
    assert config.start_binlog_file == "mysql-bin.000999"
    assert config.start_binlog_pos == 12345
    assert config.start_gtid is None
    # Manual override -> schema_only (brand-new connector, no schema-history).
    assert config.snapshot_mode == "schema_only"


def test_build_source_config_none_override_uses_watermark() -> None:
    # Explicit None override falls back to the gapless watermark path.
    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark(), resume_override=None
    )
    assert config.start_gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert config.start_binlog_file == "mysql-bin.000123"


def test_build_sink_config_keys_and_dlq() -> None:
    orch = CdcPipelineOrchestrator()
    config = orch.build_sink_config("sink", _tables(), "app.dlq")

    assert config.name == "sink"
    assert config.topics == ["app.orders", "app.customers"]
    assert config.pk_mode == "record_key"
    assert config.insert_mode == "upsert"
    assert config.delete_enabled is True
    assert config.dlq_topic == "app.dlq"


def test_build_sink_config_rejects_empty_tables() -> None:
    # A Kafka Connect sink requires a non-empty topic list; an empty selection
    # would produce SinkTopics="" and be rejected by MSK Connect at
    # POST /connectors with an opaque HTTP 400 minutes into the deploy. Fail early,
    # with an actionable message, before any deploy is attempted. (Contrast the
    # SOURCE config, where an empty table list is valid = "all tables".)
    orch = CdcPipelineOrchestrator()
    with pytest.raises(ValueError, match="at least one table"):
        orch.build_sink_config("sink", [], "app.dlq")


def test_status_empty_without_source() -> None:
    assert CdcPipelineOrchestrator().status() == []


def test_status_relays_injected_statuses() -> None:
    statuses = [
        ConnectorStatus(name="src", state=ConnectorState.RUNNING, tasks_total=1),
        ConnectorStatus(
            name="sink",
            state=ConnectorState.RUNNING,
            tasks_total=4,
            tasks_failed=1,
            lag_seconds=12.5,
        ),
    ]
    orch = CdcPipelineOrchestrator(status_source=lambda: statuses)
    result = orch.status()
    assert [s.name for s in result] == ["src", "sink"]
    assert result[1].lag_seconds == 12.5
    assert result[1].tasks_failed == 1


def test_surface_errors_records_to_single_error_log() -> None:
    errors = [
        CdcConnectorError(
            table="app.orders",
            message="decimal precision exceeded",
            error_code="22003",
            occurred_at=datetime(2026, 3, 1, tzinfo=timezone.utc),
        ),
        CdcConnectorError(table="app.customers", message="sink task failed"),
    ]
    orch = CdcPipelineOrchestrator(
        error_source=lambda: errors,
        now=lambda: datetime(2026, 6, 1, tzinfo=timezone.utc),
    )
    error_log = ErrorLogStore()
    orch.surface_errors("job-cdc", error_log)

    summary = error_log.summary("job-cdc")
    assert summary.total_errors == 2
    assert summary.errors_by_table == {"app.orders": 1, "app.customers": 1}
    log = error_log.render_log("job-cdc").decode("utf-8")
    assert "decimal precision exceeded" in log
    # Missing occurred_at is stamped with the injected clock.
    assert "2026-06-01" in log


def test_surface_errors_noop_without_source() -> None:
    orch = CdcPipelineOrchestrator()
    error_log = ErrorLogStore()
    orch.surface_errors("job-cdc", error_log)
    assert error_log.summary("job-cdc").total_errors == 0


# ---------------------------------------------------------------------------
# CDC provider -> unified LoadStatusView (Task 24.4 / Req 13.1, 13.5)
# ---------------------------------------------------------------------------


def test_build_cdc_status_view_relays_managed_signals() -> None:
    statuses = [
        ConnectorStatus(name="src", state=ConnectorState.RUNNING, lag_seconds=2.0),
        ConnectorStatus(
            name="sink",
            state=ConnectorState.RUNNING,
            tasks_total=4,
            lag_seconds=9.5,
            caught_up_to=datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc),
        ),
    ]
    summary = ErrorLogSummary(
        total_errors=2, errors_by_table={"app.orders": 2}, log_available=True
    )
    view = build_cdc_status_view(statuses, summary, dlq_depth=2)

    assert view.kind == LoadKind.CDC
    assert view.connector_states == {"src": "RUNNING", "sink": "RUNNING"}
    assert view.lag_seconds == 9.5  # worst (max) lag
    assert view.caught_up_to == datetime(2026, 6, 1, 12, 0, tzinfo=timezone.utc)
    assert view.dlq_depth == 2
    assert view.error_summary is summary
    # Per-table rows come from the single error summary (connector-centric status).
    assert [(r.table, r.state, r.errors) for r in view.tables] == [
        ("app.orders", "STREAMING", 2)
    ]


def test_build_cdc_status_view_empty() -> None:
    view = build_cdc_status_view([])
    assert view.kind == LoadKind.CDC
    assert view.connector_states == {}
    assert view.lag_seconds is None
    assert view.caught_up_to is None
    assert view.tables == []


# ---------------------------------------------------------------------------
# Composite-PK CDC via Debezium source re-key (message.key.columns)
# ---------------------------------------------------------------------------


def _composite_conversions():
    return {
        "app.orders": TableConversion(
            table="app.orders",
            target_ddl=(
                'CREATE TABLE "app"."orders" ("id" bigint NOT NULL, '
                '"customer_id" bigint NOT NULL, PRIMARY KEY ("customer_id", "id"))'
            ),
        ),
        "app.customers": TableConversion(
            table="app.customers",
            target_ddl='CREATE TABLE "app"."customers" ("id" bigint NOT NULL, '
            'PRIMARY KEY ("id"))',
        ),
    }


def test_composite_key_columns_for_cdc_returns_target_key_for_changed_tables() -> None:
    tables = _tables()  # app.orders, app.customers -- both source PK [id]
    # Only the composite table maps to its (leading, id) target key; the unchanged
    # one is omitted (keeps the source-PK record key).
    assert composite_key_columns_for_cdc(tables, _composite_conversions()) == {
        "app.orders": ["customer_id", "id"],
    }


def test_composite_key_columns_for_cdc_ignores_unknown_or_missing() -> None:
    tables = _tables()
    # No conversion, and an unparseable target_ddl (parse -> []), both mean
    # "unknown" -> omitted (keyed on the source PK; today's behavior).
    conversions = {
        "app.customers": TableConversion(
            table="app.customers", target_ddl="-- not auto-converted"
        ),
    }
    assert composite_key_columns_for_cdc(tables, conversions) == {}


def test_format_message_key_columns_debezium_syntax_and_regex_escaping() -> None:
    # ';' between tables, ':' before columns, ',' between columns; dots escaped so
    # the table pattern matches literally.
    value = format_message_key_columns(
        {"app.orders": ["customer_id", "id"], "app.items": ["tenant_id", "id"]}
    )
    assert value == r"app\.items:tenant_id,id;app\.orders:customer_id,id"
    assert format_message_key_columns({}) == ""


def test_build_source_config_threads_message_key_columns() -> None:
    orch = CdcPipelineOrchestrator()
    config = orch.build_source_config(
        "src", _tables(), _watermark(),
        message_key_columns={"app.orders": ["customer_id", "id"]},
    )
    assert config.message_key_columns == {"app.orders": ["customer_id", "id"]}


def test_composite_cdc_excluded_key_columns_flags_excluded_key() -> None:
    mkc = {"app.orders": ["customer_id", "id"]}
    # A key column wrongly in the exclude list is flagged (Debezium can't build the
    # key from a column dropped at capture); an unrelated exclusion is fine.
    assert composite_cdc_excluded_key_columns(
        mkc, ["app.orders.customer_id", "app.orders.blob_col"]
    ) == ["app.orders.customer_id"]
    assert composite_cdc_excluded_key_columns(mkc, ["app.orders.blob_col"]) == []
