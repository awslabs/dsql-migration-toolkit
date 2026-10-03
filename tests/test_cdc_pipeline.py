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
    SchemaDriftKind,
    build_cdc_status_view,
    classify_schema_drift,
    composite_cdc_excluded_key_columns,
    composite_key_columns_for_cdc,
    format_message_key_columns,
)
from dsql_migrator.core.converter import TableConversion
from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import (
    ErrorLogSummary,
    LoadKind,
    SchemaDriftSummary,
    TableDef,
    Watermark,
)


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
    # No coordinates at all -> nothing to seed -> schema_only (starts cleanly).
    assert config.snapshot_mode == "schema_only"


def test_build_source_config_gtid_only_watermark_uses_schema_only() -> None:
    # A watermark with a GTID set but NO binlog file:pos cannot seed the
    # connect-offsets entry (the seeder is keyed on binlog file+pos and rejects a
    # GTID-only watermark). Reachable when SHOW MASTER STATUS is restricted
    # (REPLICATION CLIENT missing on RDS/Aurora) but @@GLOBAL.gtid_executed reads.
    # snapshot.mode MUST be schema_only, not recovery: recovery would resume from a
    # seeded offset that was never written -> task-start failure or a silent resume
    # from the live binlog (losing the Full-Load-window changes).
    orch = CdcPipelineOrchestrator()
    watermark = Watermark(
        gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
        server_uuid="3E11FA47-71CA-11E1-9E33-C80AA9429562",
        snapshot_timestamp=datetime(2026, 5, 6, 7, 8, 9, tzinfo=timezone.utc),
    )
    config = orch.build_source_config("src", _tables(), watermark)
    assert config.snapshot_mode == "schema_only"
    assert config.start_gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert config.start_binlog_file is None and config.start_binlog_pos is None


def test_build_source_config_binlog_file_without_position_uses_schema_only() -> None:
    # A watermark with a binlog file but no position also cannot seed an offset
    # (can_seed_offset requires BOTH), so it must not select recovery either.
    orch = CdcPipelineOrchestrator()
    watermark = Watermark(
        binlog_file="mysql-bin.000123",
        binlog_position=None,
        snapshot_timestamp=datetime(2026, 5, 6, 7, 8, 9, tzinfo=timezone.utc),
    )
    config = orch.build_source_config("src", _tables(), watermark)
    assert config.snapshot_mode == "schema_only"


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


def test_build_source_config_gtid_only_override_stays_schema_only() -> None:
    # A GTID-only manual start position overrides the watermark coordinates entirely,
    # and seeds NOTHING (both seeders key the offset on binlog file:pos) -- so the config
    # must not pretend otherwise: schema_only, no start coordinates at all.
    override = CdcResumePoint(
        gtid_executed="99999999-1111-2222-3333-444444444444:1-9",
    )
    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark(), resume_override=override
    )
    # Watermark's binlog coords are NOT used when an override is supplied.
    assert config.start_binlog_file is None
    assert config.start_binlog_pos is None
    # No GTID is reported for an override: the preview prints exactly what is seeded.
    assert config.start_gtid is None
    assert config.snapshot_mode == "schema_only"


def test_build_source_config_binlog_override_selects_recovery() -> None:
    # A Manual binlog file:position IS seeded into connect-offsets (cdc_seed_watermark),
    # so the connector takes the resume path and needs recovery -- schema_only with a
    # seeded offset dies at task start ("The db history topic is missing").
    override = CdcResumePoint(
        binlog_file="mysql-bin.000999",
        binlog_position=12345,
        gtid_executed="99999999-1111-2222-3333-444444444444:1-9",
    )
    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark(), resume_override=override
    )
    assert config.start_binlog_file == "mysql-bin.000999"
    assert config.start_binlog_pos == 12345
    # Only file:pos is seeded, so the override's GTID is not reported either.
    assert config.start_gtid is None
    assert config.snapshot_mode == "recovery"
    # Also with the sentinel (no Full Load) watermark a CDC-only Manual start uses.
    bare = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(),
        Watermark(snapshot_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc)),
        resume_override=override,
    )
    assert bare.snapshot_mode == "recovery"


def test_cdc_seed_watermark_precedence() -> None:
    from dsql_migrator.core.cdc import cdc_seed_watermark

    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    job = _watermark()
    # Automatic (no Manual point): the Full Load job watermark, unchanged -- even None.
    assert cdc_seed_watermark(job, None, now=now) == (job, "full_load")
    assert cdc_seed_watermark(None, None, now=now) == (None, "full_load")
    # A usable Manual point beats the job watermark; file:pos only, stamped `now`.
    manual = CdcResumePoint(
        binlog_file="mysql-bin.000042", binlog_position=4, gtid_executed="u:1-3"
    )
    for job_wm in (job, None):
        seed, origin = cdc_seed_watermark(job_wm, manual, now=now)
        assert origin == "manual"
        assert (seed.binlog_file, seed.binlog_position) == ("mysql-bin.000042", 4)
        assert seed.gtid_executed is None
        assert seed.snapshot_timestamp == now
    # A GTID set alone cannot seed: no fallback to the job watermark or to "now".
    gtid_only = CdcResumePoint(gtid_executed="u:1-3")
    assert cdc_seed_watermark(job, gtid_only, now=now) == (None, "manual_unseedable")
    # A file without a position is not a coordinate either.
    half = CdcResumePoint(binlog_file="mysql-bin.000042")
    assert cdc_seed_watermark(job, half, now=now) == (None, "manual_unseedable")


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
    # No drift by default -- inert on a healthy stream.
    assert view.schema_drift == []


# ---------------------------------------------------------------------------
# Source-schema-drift classification (detection-only; recovery stays manual)
# ---------------------------------------------------------------------------


def test_classify_schema_drift_maps_known_sqlstates() -> None:
    assert classify_schema_drift("42703") is SchemaDriftKind.ADD_COLUMN
    assert classify_schema_drift("23502") is SchemaDriftKind.DROP_COLUMN
    assert classify_schema_drift("42804") is SchemaDriftKind.TYPE_CHANGE


def test_classify_schema_drift_maps_a_missing_target_table() -> None:
    """42P01 / 3F000 are the one TARGET-side kind and must not stay anonymous.

    DSQL raises 42P01 when the table the sink writes to is gone (a Schema Conversion
    REPLACE, or an out-of-band DROP, under a live stream). The sink's isTransient covers
    only 40001 / 08* / 57* / a null state, so 42P01 is PERMANENT: every event for that
    table is dead-lettered with its offset committed and no retry. Unmapped, that -- the
    most destructive failure of the three -- rendered as an ordinary poison row with no
    banner at all, and under 50 records not even an amber DLQ badge.
    """
    assert classify_schema_drift("42P01") is SchemaDriftKind.MISSING_TABLE
    assert classify_schema_drift("3F000") is SchemaDriftKind.MISSING_TABLE
    # Same normalization as the others.
    assert classify_schema_drift(" 42p01 ") is SchemaDriftKind.MISSING_TABLE
    # And it reaches the model the banner reads.
    err = CdcConnectorError(table="db.orders", message="x", error_code="42P01")
    assert err.drift_kind is SchemaDriftKind.MISSING_TABLE


def test_classify_schema_drift_class_22_is_not_drift() -> None:
    # Class 22 (data exception): string-too-long / numeric-range / bad-datetime / bad
    # text are per-VALUE rejections ordinary bad data raises with NO source DDL change,
    # so they must NOT be surfaced as a "source changed a column's type" drift (which
    # fired that banner on every oversized/out-of-range poison row). They stay ordinary
    # quarantines. Only 42804 (datatype_mismatch) is the type-change signal.
    for code in ("22001", "22003", "22007", "22P02"):
        assert classify_schema_drift(code) is None


def test_classify_schema_drift_is_case_and_whitespace_insensitive() -> None:
    assert classify_schema_drift(" 42703 ") is SchemaDriftKind.ADD_COLUMN
    assert classify_schema_drift("  42804  ") is SchemaDriftKind.TYPE_CHANGE


def test_classify_schema_drift_returns_none_for_non_drift_codes() -> None:
    # An OCC conflict (40001), a size-limit rejection (54000), and no code at all are
    # ordinary poison rows, NOT schema drift -- must not be surfaced as a source DDL.
    assert classify_schema_drift("40001") is None
    assert classify_schema_drift("54000") is None
    assert classify_schema_drift(None) is None
    assert classify_schema_drift("") is None


def test_connector_error_drift_kind_is_derived_from_error_code() -> None:
    add = CdcConnectorError(table="orders", message="x", error_code="42703")
    assert add.drift_kind is SchemaDriftKind.ADD_COLUMN
    plain = CdcConnectorError(table="orders", message="x", error_code=None)
    assert plain.drift_kind is None


def test_build_cdc_status_view_carries_schema_drift() -> None:
    drift = [
        SchemaDriftSummary(table="orders", kind="add-column", count=3),
        SchemaDriftSummary(table="line_items", kind="type-change", count=1),
    ]
    view = build_cdc_status_view([], None, dlq_depth=4, schema_drift=drift)
    assert [(g.table, g.kind, g.count) for g in view.schema_drift] == [
        ("orders", "add-column", 3),
        ("line_items", "type-change", 1),
    ]


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


# ---------------------------------------------------------------------------
# A missing TARGET table is reported as its own, more severe condition
# ---------------------------------------------------------------------------


def test_missing_target_table_banner_does_not_call_it_a_source_change() -> None:
    """The banner must name the real condition, at the real severity.

    A dropped TARGET table is not a source DDL change, and it is not "be aware": rows are
    being permanently lost while it renders (dead-lettered with the offset committed, and
    never replayed). Reusing the source-drift header and the amber warning tone would
    have understated the only drift kind that is actively destroying data.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    class _Ui:
        def __init__(self):
            self.texts: list[str] = []
            self.classes_seen: list[str] = []

        def _el(self):
            ui = self

            class _El:
                def classes(self, *a, **k):
                    for value in a:
                        ui.classes_seen.append(str(value))
                    for value in k.values():
                        ui.classes_seen.append(str(value))
                    return self

                def props(self, *a, **k):
                    return self

                def tooltip(self, *a, **k):
                    return self

                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            return _El()

        def column(self, *a, **k):
            return self._el()

        def row(self, *a, **k):
            return self._el()

        def label(self, text="", *a, **k):
            self.texts.append(str(text))
            return self._el()

        def icon(self, name="", *a, **k):
            return self._el()

        def button(self, text="", *a, **k):
            self.texts.append(str(text))
            return self._el()

        def space(self, *a, **k):
            return self._el()

        def card(self, *a, **k):
            return self._el()

    view = SimpleNamespace(
        schema_drift=[
            SchemaDriftSummary(table="db.orders", kind="missing-table", count=7)
        ]
    )
    ui = _Ui()
    cm._render_cdc_schema_drift_banner(ui, view, session=None)
    blob = " ".join(ui.texts)

    assert "Target table missing" in blob
    assert "Source schema change detected" not in blob
    # The distinct recovery: the stream cannot heal it, so stop CDC and reload.
    assert "Stop CDC and reload these tables" in blob
    assert "db.orders" in blob
    # "Fix target schema…" is ADD COLUMN's ALTER -- useless for a missing table.
    assert "Fix target schema" not in blob
    # Error tone, not warning, on the BANNER's own surface. Assert the FIRST recorded
    # classes string -- the outer column -- not "any", because the nested stop-and-reload
    # render_notice also emits an error surface and would mask an amber banner.
    from dsql_migrator.ui.design import NOTICE_STYLE

    banner_surface = ui.classes_seen[0]
    assert NOTICE_STYLE["error"][1] in banner_surface, banner_surface
    assert NOTICE_STYLE["warning"][1] not in banner_surface, banner_surface


def test_source_drift_banner_keeps_its_warning_framing() -> None:
    # Regression guard for the split: an ordinary source ADD COLUMN must still read as a
    # source change at warning severity, and must NOT gain the stop-and-reload notice.
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    class _Ui:
        def __init__(self):
            self.texts: list[str] = []

        def _el(self):
            class _El:
                def classes(self, *a, **k):
                    return self

                def props(self, *a, **k):
                    return self

                def tooltip(self, *a, **k):
                    return self

                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

            return _El()

        def column(self, *a, **k):
            return self._el()

        def row(self, *a, **k):
            return self._el()

        def label(self, text="", *a, **k):
            self.texts.append(str(text))
            return self._el()

        def icon(self, name="", *a, **k):
            return self._el()

        def button(self, text="", *a, **k):
            self.texts.append(str(text))
            return self._el()

        def space(self, *a, **k):
            return self._el()

        def card(self, *a, **k):
            return self._el()

    view = SimpleNamespace(
        schema_drift=[
            SchemaDriftSummary(table="db.orders", kind="add-column", count=2)
        ]
    )
    ui = _Ui()
    cm._render_cdc_schema_drift_banner(ui, view, session=None)
    blob = " ".join(ui.texts)

    assert "Source schema change detected" in blob
    assert "Target table missing" not in blob
    assert "Stop CDC and reload these tables" not in blob


def test_07006_is_classified_so_a_total_row_loss_stops_being_invisible() -> None:
    """SQLSTATE 07006 = the sink cannot BIND a value, so every write for the table is lost.

    Live on Aurora PostgreSQL 17.7: a source ARRAY column. Schema Conversion substitutes
    `<t>[]` -> jsonb (DSQL has no array type) and the Full Load converts it correctly in SQL
    on the SOURCE, but the change stream delivers a java.util.ArrayList under a
    `SchemaBuilder.array(...)` schema with no logical name, so the sink passes it through and
    `ps.setObject` hands pgjdbc a type it cannot map. Permanent, so the record is
    dead-lettered and the offset commits past it.

    It was invisible three ways, which is why it needed classifying rather than just fixing:
    an UPDATE dead-letter changes NO row count (the table reads src=15 tgt=15 missing=0
    extra=0 while every value is stale), the record was unclassified so no banner named it,
    and under 50 records the DLQ badge stays quiet.
    """
    from dsql_migrator.core.cdc import SchemaDriftKind, classify_schema_drift

    assert classify_schema_drift("07006") is SchemaDriftKind.UNBINDABLE_VALUE
    # A per-VALUE data exception must still NOT be drift -- that distinction is the reason
    # the map is a map and not "anything that failed".
    assert classify_schema_drift("22001") is None


def test_the_unbindable_value_banner_states_certain_loss_and_the_real_remedy() -> None:
    """error tone, its own header, and a remedy the operator can actually act on.

    Calibration: this is CERTAIN, ONGOING, total loss for the table, so `error` -- the
    design system reserves `warning` for a real but non-blocking issue. It must also not
    claim a source DDL change (nothing changed on the source), must not borrow the
    missing-table header, and must say that COUNTS cannot see it, because an operator
    watching row counts has no other way to learn that.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm
    from dsql_migrator.ui.design import NOTICE_STYLE

    class _Ui:
        def __init__(self):
            self.texts: list[str] = []
            self.classes_seen: list[str] = []

        def _el(self):
            outer = self

            class _El:
                def classes(self, *a, **k):
                    for v in a:
                        outer.classes_seen.append(str(v))
                    for v in k.values():
                        outer.classes_seen.append(str(v))
                    return self

                def props(self, *a, **k):
                    return self

                def tooltip(self, *a, **k):
                    return self

                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

            return _El()

        def column(self, *a, **k):
            return self._el()

        def row(self, *a, **k):
            return self._el()

        def label(self, text="", *a, **k):
            self.texts.append(str(text))
            return self._el()

        def icon(self, name="", *a, **k):
            return self._el()

        def button(self, text="", *a, **k):
            self.texts.append(str(text))
            return self._el()

        def space(self, *a, **k):
            return self._el()

        def card(self, *a, **k):
            return self._el()

    ui = _Ui()
    cm._render_cdc_schema_drift_banner(
        ui,
        SimpleNamespace(schema_drift=[
            SchemaDriftSummary(table="ecommerce.products", kind="unbindable-value", count=5)
        ]),
        session=None,
    )
    blob = " ".join(ui.texts)
    assert "the sink cannot write this table" in blob
    # NOT a source change, and NOT the missing-table wording.
    assert "Source schema change detected" not in blob
    assert "Target table missing" not in blob
    # The three facts the operator cannot get anywhere else.
    assert "07006" in blob
    assert "Row COUNTS cannot see this" in blob
    assert "Validation (step 4)" in blob
    # The remedy must name 'Drop & reload': the rows ARE on the target carrying a stale
    # value, and an append reload never rewrites a primary key the target already has.
    assert "Drop & reload" in blob
    assert "an append reload cannot" in blob
    assert "ecommerce.products" in blob
    # error tone on the banner's OWN surface (the first classes string is the outer column).
    assert NOTICE_STYLE["error"][1] in ui.classes_seen[0], ui.classes_seen[0]
    assert NOTICE_STYLE["warning"][1] not in ui.classes_seen[0]


def test_a_unique_conflict_no_longer_claims_the_target_table_is_missing() -> None:
    """Pre-existing bug the three-way split fixes in passing.

    `unique-conflict` sat in the target-side set, so 23505 rendered under "Target table
    missing — rows are being lost" — false on both halves: the table is there, and a row
    only CAN end up missing. It keeps the error tone (a vanished row is not "be aware") but
    gets a header that is true.
    """
    from types import SimpleNamespace

    from dsql_migrator.core.models import SchemaDriftSummary
    from dsql_migrator.ui.data_migration import _cdc_monitoring as cm

    captured: list[str] = []

    class _Ui:
        def _el(self):
            class _El:
                def classes(self, *a, **k):
                    return self

                def props(self, *a, **k):
                    return self

                def tooltip(self, *a, **k):
                    return self

                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

            return _El()

        def __getattr__(self, _name):
            def _f(text="", *a, **k):
                if isinstance(text, str) and text:
                    captured.append(text)
                return self._el()
            return _f

    cm._render_cdc_schema_drift_banner(
        _Ui(),
        SimpleNamespace(schema_drift=[
            SchemaDriftSummary(table="app.orders", kind="unique-conflict", count=2)
        ]),
        session=None,
    )
    blob = " ".join(captured)
    assert "Rows may be missing from the target" in blob
    assert "Target table missing" not in blob
    assert "Source schema change detected" not in blob


# ---------------------------------------------------------------------------
# G5: ONE MySQL snapshot-mode rule (resume -> schema_only; new start point -> recovery)
# ---------------------------------------------------------------------------


def test_build_source_config_resume_of_a_stopped_stack_uses_schema_only() -> None:
    # A re-Start after Stop resumes the committed offset with the schema history intact:
    # recovery would rebuild that history from the tables AS THEY ARE NOW and read older
    # rows with it (12a-12c), schema_only replays the recorded history in binlog order.
    from dsql_migrator.core.cdc import MysqlStartFacts

    config = CdcPipelineOrchestrator().build_source_config(
        "src", _tables(), _watermark(), start_facts=MysqlStartFacts(resumes=True)
    )
    assert config.snapshot_mode == "schema_only"
    # Without facts the choice is exactly today's (a seedable watermark -> recovery).
    assert (
        CdcPipelineOrchestrator().build_source_config("src", _tables(), _watermark())
        .snapshot_mode
        == "recovery"
    )


@pytest.mark.parametrize(
    "can_seed, facts, expected",
    [
        # (a) connectors exist: their mode is kept, whatever else holds.
        (True, dict(live_mode="schema_only"), "schema_only"),
        (False, dict(live_mode="recovery"), "recovery"),
        (True, dict(live_mode="schema_only", resumes=True, history_missing=True), "schema_only"),
        (False, dict(live_mode="recovery", resumes=True), "recovery"),
        # (b) a resume with its history -> schema_only.
        (True, dict(resumes=True), "schema_only"),
        (False, dict(resumes=True), "schema_only"),
        # (c) a resume whose history is missing -> recovery, also with nothing to seed.
        (True, dict(resumes=True, history_missing=True), "recovery"),
        (False, dict(resumes=True, history_missing=True), "recovery"),
        # (d) a new seedable start point -> recovery; (e) nothing -> schema_only.
        (True, dict(), "recovery"),
        (False, dict(), "schema_only"),
        # An unknown live value is not a mode to keep.
        (True, dict(live_mode="initial"), "recovery"),
        # facts=None is today's rule.
        (True, None, "recovery"),
        (False, None, "schema_only"),
    ],
)
def test_mysql_snapshot_mode_truth_table(can_seed, facts, expected) -> None:
    from dsql_migrator.core.cdc import MysqlStartFacts, mysql_snapshot_mode

    f = None if facts is None else MysqlStartFacts(**facts)
    assert mysql_snapshot_mode(can_seed, f) == expected


def test_start_point_check_due_only_for_recovery_without_live_connectors() -> None:
    from dsql_migrator.core.cdc import MysqlStartFacts, start_point_check_due

    assert start_point_check_due("recovery", None) is True
    assert start_point_check_due("recovery", MysqlStartFacts()) is True
    assert start_point_check_due("recovery", MysqlStartFacts(resumes=True, history_missing=True))
    assert start_point_check_due("schema_only", MysqlStartFacts()) is False
    assert start_point_check_due("schema_only", None) is False
    # Retry CDC / a live re-Start keeps its mode and is not re-checked.
    assert start_point_check_due("recovery", MysqlStartFacts(live_mode="recovery")) is False


def test_cdc_seed_watermark_carries_the_manual_points_fingerprints() -> None:
    from dsql_migrator.core.cdc import cdc_seed_watermark

    now = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
    manual = CdcResumePoint(binlog_file="mysql-bin.000042", binlog_position=4)
    fps = {"app.orders": "1:2:0011aabb:0011223344556677"}
    seed, origin = cdc_seed_watermark(None, manual, now=now, override_fingerprints=fps)
    assert origin == "manual" and seed.table_fingerprints == fps
    # The job-watermark path returns the job watermark unchanged (its OWN fingerprints).
    job = _watermark().model_copy(update={"table_fingerprints": {"app.x": "v"}})
    assert cdc_seed_watermark(job, None, now=now, override_fingerprints=fps) == (
        job, "full_load"
    )
    # No fingerprints given -> none on the Manual seed.
    bare, _ = cdc_seed_watermark(None, manual, now=now)
    assert bare.table_fingerprints == {}
