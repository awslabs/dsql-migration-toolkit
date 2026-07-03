"""Unit tests for the Step 3 (Data Migration) screen's NiceGUI-agnostic logic.

These cover the parts of the Data Migration screen that do not touch NiceGUI:

- Run orchestration: seeding per-table chunks, capturing the export watermark
  once and persisting it on the job, recording per-table progress, isolating a
  per-table failure, and propagating a fatal watermark-capture failure
  (Requirements 8.2/8.3 / Property 11).
- Progress aggregation from chunk states (Requirement 8.3).
- Watermark formatting for display, including graceful degradation of optional
  binlog/GTID fields (Requirement 8.5 / Property 11).
- The reference BatchedTableMigrator wiring (export stream -> batched import)
  with fakes.
- Job-status -> step-status mapping and per-session state/store isolation.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.job_manager import JobManager
from dsql_migrator.core.batched_import import (
    BatchedImportResult,
    OnConflictMode,
)
from dsql_migrator.core.models import (
    ColumnDef,
    ErrorLogSummary,
    MigrationJob,
    MigrationMode,
    PrerequisiteCheckId,
    PrerequisiteReport,
    PrerequisiteResult,
    PrerequisiteStatus,
    SourceConnectionConfig,
    SourceInventory,
    StepStatus,
    TableDef,
    TableSelection,
    TargetConnectionConfig,
    TargetInventory,
    TargetObjectKind,
    TargetRelation,
    TargetSchemaNode,
    Watermark,
)
from dsql_migrator.ui.data_migration import (
    DataMigrationInputs,
    DataMigrationState,
    DataMigrationStore,
    BatchedTableMigrator,
    build_migration_table_tree,
    effective_migration_selection,
    format_binlog_coordinate,
    format_error_summary,
    format_watermark,
    full_load_run_guard_reason,
    generated_table_names,
    group_prereq_results,
    job_status_to_step_status,
    migratable_table_names,
    prerequisite_block_reason,
    run_data_migration,
    run_full_load,
    summarize_progress,
    target_existing_table_names,
)
from dsql_migrator.ui.data_migration import PrereqCategory
from dsql_migrator.ui.schema_conversion import TABLE_PREFIX


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------


def _tables() -> list[TableDef]:
    return [
        TableDef(
            name="orders",
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        ),
        TableDef(
            name="customers",
            columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
            primary_key=["id"],
        ),
    ]


def _inventory() -> SourceInventory:
    return SourceInventory(tables=_tables())


def _watermark() -> Watermark:
    return Watermark(
        binlog_file="mysql-bin.000123",
        binlog_position=45678,
        gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5",
        server_uuid="3E11FA47-71CA-11E1-9E33-C80AA9429562",
        snapshot_timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        table_row_counts={"orders": 10, "customers": 3},
    )


class _FakeMigrator:
    """A fake :class:`DataMigrator` returning canned rows / failures per table.

    ``rows_by_table`` maps a table name to the rows it loads; a name in
    ``fail_tables`` raises a per-table error. ``watermark_error`` makes the
    one-time watermark capture raise (a fatal error).
    """

    def __init__(
        self,
        *,
        watermark: Watermark | None = None,
        rows_by_table: dict[str, int] | None = None,
        fail_tables: tuple[str, ...] = (),
        quarantine_by_table: dict[str, int] | None = None,
        watermark_error: Exception | None = None,
    ) -> None:
        self._watermark = watermark or _watermark()
        self._rows_by_table = rows_by_table or {}
        self._fail_tables = set(fail_tables)
        # Map table -> number of rows it loads but QUARANTINES (permanently drops,
        # e.g. an oversized value). Returned as a TableLoadResult so run_full_load
        # exercises the dropped-row path.
        self._quarantine_by_table = quarantine_by_table or {}
        self._watermark_error = watermark_error
        self.captured = 0
        self.migrated: list[str] = []

    def capture_watermark(self, tables: object = None) -> Watermark:
        self.captured += 1
        if self._watermark_error is not None:
            raise self._watermark_error
        return self._watermark

    def migrate_table(
        self, table: TableDef, *, on_rows=None, should_cancel=None
    ):
        self.migrated.append(table.name)
        if table.name in self._fail_tables:
            raise RuntimeError(f"load failed for {table.name}")
        rows = self._rows_by_table.get(table.name, 0)
        if on_rows is not None and rows:
            on_rows(rows, 0)  # emit the table's rows as a single batch (no skips)
        quarantined = self._quarantine_by_table.get(table.name, 0)
        if quarantined:
            from dsql_migrator.core.batched_import import QuarantineRecord
            from dsql_migrator.ui.data_migration._engine import TableLoadResult

            return TableLoadResult(
                rows_loaded=rows,
                rows_quarantined=quarantined,
                quarantine_records=tuple(
                    QuarantineRecord(
                        table=table.name,
                        primary_key=f"id={i}",
                        error_code="54000",
                        message="value too large for the column",
                    )
                    for i in range(quarantined)
                ),
            )
        return rows


def _run_job(migrator: _FakeMigrator, tables: list[TableDef]) -> MigrationJob:
    """Run a migration to completion on a real JobManager and return the job."""
    manager = JobManager()
    job_id = manager.submit(
        lambda handle: run_data_migration(handle, tables, migrator=migrator)
    )
    assert manager.wait(job_id, timeout=5.0)
    return manager.get_status(job_id)


# ---------------------------------------------------------------------------
# Run orchestration (Requirements 8.2, 8.3 / Property 11)
# ---------------------------------------------------------------------------


def test_run_data_migration_captures_watermark_once_and_persists_it() -> None:
    migrator = _FakeMigrator(rows_by_table={"orders": 10, "customers": 3})
    job = _run_job(migrator, _tables())

    assert migrator.captured == 1
    assert job.watermark is not None
    assert job.watermark.binlog_file == "mysql-bin.000123"
    assert job.status == "DONE"


def test_run_data_migration_records_per_table_progress() -> None:
    migrator = _FakeMigrator(rows_by_table={"orders": 10, "customers": 3})
    job = _run_job(migrator, _tables())

    assert migrator.migrated == ["orders", "customers"]
    by_name = {chunk.chunk_id: chunk for chunk in job.chunks}
    assert by_name["orders"].status == "DONE"
    assert by_name["orders"].rows_loaded == 10
    assert by_name["customers"].rows_loaded == 3
    assert job.progress_pct == 100.0
    assert job.error_count == 0


def test_run_data_migration_isolates_per_table_failure() -> None:
    """A per-table failure is recorded and the remaining tables still migrate."""
    migrator = _FakeMigrator(
        rows_by_table={"customers": 3}, fail_tables=("orders",)
    )
    job = _run_job(migrator, _tables())

    by_name = {chunk.chunk_id: chunk for chunk in job.chunks}
    assert by_name["orders"].status == "FAILED"
    assert by_name["customers"].status == "DONE"
    assert migrator.migrated == ["orders", "customers"]
    assert job.error_count == 1
    # Isolation holds (other tables still migrate), but a run that did not load
    # every selected table is reported FAILED -- incomplete target data must
    # never look successful, so the job is not DONE. The watermark is still set.
    assert job.status == "FAILED"
    assert job.watermark is not None


def test_full_load_partial_failure_reports_incomplete_with_guidance() -> None:
    """A partial failure surfaces an actionable, credential-free error message."""
    from dsql_migrator.core.error_log import ErrorLogStore

    migrator = _FakeMigrator(
        rows_by_table={"customers": 3}, fail_tables=("orders",)
    )
    manager = JobManager()
    error_log = ErrorLogStore()
    job_id = manager.submit(
        lambda handle: run_full_load(
            handle, _tables(), migrator=migrator, error_log=error_log
        )
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)

    assert job.status == "FAILED"
    message = manager.get_error(job_id)
    assert message is not None
    assert "incomplete" in message.lower()
    assert "Retry failed tables" in message
    # The succeeded table is still DONE (carried forward for retry); the failed
    # one is recorded in the downloadable error log.
    by_name = {chunk.chunk_id: chunk for chunk in job.chunks}
    assert by_name["customers"].status == "DONE"
    assert "orders" in error_log.latest_messages(job_id)


def test_full_load_quarantined_rows_make_run_incomplete_not_success() -> None:
    """A table that loads but DROPS (quarantines) rows must fail the run loudly.

    Quarantined rows are permanently dropped from the target (e.g. a value over
    DSQL's ~1 MiB per-value limit), so the target is missing rows. The run must be
    FAILED (never a silent DONE/SUCCESS) so the Validation gate holds, and the
    error message must point at fixing the source value + re-running -- not a plain
    "Retry failed tables" (which cannot recover a permanently-rejected value).
    """
    from dsql_migrator.core.error_log import ErrorLogStore

    migrator = _FakeMigrator(
        rows_by_table={"orders": 10, "customers": 5},
        quarantine_by_table={"orders": 2},
    )
    manager = JobManager()
    error_log = ErrorLogStore()
    job_id = manager.submit(
        lambda handle: run_full_load(
            handle, _tables(), migrator=migrator, error_log=error_log
        )
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)

    # The run is FAILED even though both chunks completed -- dropped rows mean the
    # target is incomplete, so success would be a silent loss.
    assert job.status == "FAILED"
    message = manager.get_error(job_id)
    assert message is not None
    assert "quarantined" in message.lower()
    assert "re-run" in message.lower()
    # The chunk that dropped rows is still DONE with the rows that DID load (so a
    # re-run after fixing the source value idempotently fills only the gap).
    by_name = {chunk.chunk_id: chunk for chunk in job.chunks}
    assert by_name["orders"].status == "DONE"
    assert by_name["customers"].status == "DONE"
    # The dropped rows are listed in the downloadable error log by primary key.
    quarantine_msgs = [
        r.message
        for r in error_log.records(job_id)
        if r.message.startswith("quarantined row pk[")
    ]
    assert len(quarantine_msgs) == 2


def test_run_data_migration_watermark_failure_is_fatal() -> None:
    migrator = _FakeMigrator(watermark_error=RuntimeError("no REPLICATION CLIENT"))
    manager = JobManager()
    job_id = manager.submit(
        lambda handle: run_data_migration(handle, _tables(), migrator=migrator)
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)

    assert job.status == "FAILED"
    assert migrator.migrated == []  # no table migrated after a fatal capture
    error = manager.get_error(job_id)
    assert error is not None
    assert "no REPLICATION CLIENT" in error


def test_run_data_migration_seeds_one_chunk_per_table() -> None:
    migrator = _FakeMigrator()
    job = _run_job(migrator, _tables())
    assert [chunk.chunk_id for chunk in job.chunks] == ["orders", "customers"]


# ---------------------------------------------------------------------------
# Progress aggregation (Requirement 8.3)
# ---------------------------------------------------------------------------


def test_summarize_progress_counts_statuses_and_rows() -> None:
    migrator = _FakeMigrator(
        rows_by_table={"customers": 3}, fail_tables=("orders",)
    )
    job = _run_job(migrator, _tables())

    progress = summarize_progress(job)
    assert progress.total_tables == 2
    assert progress.done_tables == 1
    assert progress.failed_tables == 1
    assert progress.pending_tables == 0
    assert progress.in_progress_tables == 0
    assert progress.rows_loaded == 3
    assert progress.progress_pct == 100.0


def test_summarize_progress_empty_job() -> None:
    progress = summarize_progress(MigrationJob(job_id="j1"))
    assert progress.total_tables == 0
    assert progress.progress_pct == 0.0


# ---------------------------------------------------------------------------
# Watermark formatting (Requirement 8.5 / Property 11)
# ---------------------------------------------------------------------------


def test_format_watermark_full_coordinate_and_summary() -> None:
    display = format_watermark(_watermark())
    assert display.coordinate == "mysql-bin.000123:45678"
    assert display.gtid == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert display.server_uuid == "3E11FA47-71CA-11E1-9E33-C80AA9429562"
    assert display.snapshot_timestamp == "2026-01-02T03:04:05+00:00"
    assert "Exported as of mysql-bin.000123:45678" in display.summary
    assert "GTID 3E11FA47" in display.summary
    assert "snapshot 2026-01-02T03:04:05+00:00" in display.summary
    assert display.table_row_counts == {"orders": 10, "customers": 3}


def test_format_watermark_degrades_when_coordinates_unavailable() -> None:
    watermark = Watermark(
        snapshot_timestamp=datetime(2026, 5, 6, 7, 8, 9, tzinfo=timezone.utc)
    )
    display = format_watermark(watermark)
    assert display.coordinate == "unavailable"
    assert display.gtid == "unavailable"
    assert display.server_uuid == "unavailable"
    assert "an unavailable binlog coordinate" in display.summary
    assert "GTID" not in display.summary
    assert display.table_row_counts == {}


def test_format_binlog_coordinate_file_only() -> None:
    watermark = Watermark(
        binlog_file="mysql-bin.000999",
        snapshot_timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    assert format_binlog_coordinate(watermark) == "mysql-bin.000999"


# ---------------------------------------------------------------------------
# Reference BatchedTableMigrator (export stream -> batched import) with fakes
# ---------------------------------------------------------------------------


class _FakeExporter:
    """A fake exporter that yields canned converted rows and records the call."""

    def __init__(self, rows_by_table: dict[str, list[dict]] | None = None) -> None:
        self.streamed: list[str] = []
        self.target_types_by_table: dict[str, object] = {}
        self._rows_by_table = rows_by_table or {}

    def stream_converted_rows(
        self,
        conn: SourceConnectionConfig,
        table: TableDef,
        *,
        should_cancel=None,
        target_types=None,
    ) -> list[dict]:
        self.streamed.append(table.name)
        self.target_types_by_table[table.name] = target_types
        return self._rows_by_table.get(table.name, [{"id": 1}])


class _FakeWatermarkCapturer:
    """A fake watermark capturer returning a canned watermark."""

    def __init__(self, watermark: Watermark) -> None:
        self._watermark = watermark
        self.calls: list[list[str]] = []

    def capture(self, conn: SourceConnectionConfig, tables: list[str]) -> Watermark:
        self.calls.append(list(tables))
        return self._watermark


class _FakeImporter:
    """A fake in-process importer recording the rows and returning a result."""

    def __init__(
        self, *, rows: int = 0, failures: int = 0, first_error: str | None = None
    ) -> None:
        self.received: list[tuple[str, list[dict]]] = []
        self.index_ddls_by_table: dict[str, object] = {}
        self.on_conflict_by_table: dict[str, object] = {}
        self._rows = rows
        self._failures = failures
        self._first_error = first_error

    def import_rows(
        self, rows, table: TableDef, *, index_ddls=None, on_batch_loaded=None,
        should_cancel=None, on_conflict=None,
    ) -> BatchedImportResult:
        materialized = list(rows)
        self.index_ddls_by_table[table.name] = index_ddls
        self.on_conflict_by_table[table.name] = on_conflict
        self.received.append((table.name, materialized))
        loaded = self._rows if self._rows else len(materialized)
        if on_batch_loaded is not None and not self._failures and loaded:
            on_batch_loaded(loaded, 0)
        return BatchedImportResult(
            rows_loaded=0 if self._failures else loaded,
            failures=self._failures,
            first_error=self._first_error,
        )


def _inputs() -> DataMigrationInputs:
    return DataMigrationInputs(
        source_config=SourceConnectionConfig(host="db", database="app"),
        source_password=None,
        target_config=TargetConnectionConfig(
            cluster_endpoint="cluster.dsql.example", region="us-east-1"
        ),
        inventory=_inventory(),
    )


def test_batched_table_migrator_captures_watermark_for_selected_tables() -> None:
    capturer = _FakeWatermarkCapturer(_watermark())
    migrator = BatchedTableMigrator(
        _inputs(),  # inventory has orders + customers
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        watermark_capturer=capturer,  # type: ignore[arg-type]
        importer_factory=lambda _inputs: _FakeImporter(),  # type: ignore[arg-type,return-value]
    )
    # Only the selected subset is counted within the snapshot -- not the whole
    # inventory (so a small selection is not blocked by unrelated large tables).
    selected = [_inventory().tables[0]]  # orders only
    watermark = migrator.capture_watermark(selected)
    assert watermark.binlog_file == "mysql-bin.000123"
    assert capturer.calls == [["orders"]]


def test_batched_table_migrator_streams_then_loads() -> None:
    exporter = _FakeExporter(rows_by_table={"orders": [{"id": 1}, {"id": 2}]})
    importer = _FakeImporter()

    migrator = BatchedTableMigrator(
        _inputs(),
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
    )

    rows = migrator.migrate_table(_tables()[0])  # orders

    # The exporter streamed the table and the importer loaded the streamed rows;
    # nothing is staged to disk or S3 (the in-process streaming path).
    assert exporter.streamed == ["orders"]
    assert importer.received == [("orders", [{"id": 1}, {"id": 2}])]
    assert rows.rows_loaded == 2


def test_batched_table_migrator_recreates_replace_tables_and_passes_index_ddls() -> None:
    import dataclasses

    exporter = _FakeExporter()
    importer = _FakeImporter()
    recreated: list[str] = []

    def fake_recreator(table: TableDef) -> list[str]:
        recreated.append(table.name)
        return [f"CREATE INDEX ASYNC ix_{table.name}"]

    # "orders" is confirmed for fresh load (has rows); "customers" is not.
    inputs = dataclasses.replace(_inputs(), replace_tables=frozenset({"orders"}))
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
        table_recreator=fake_recreator,
        target_counter=lambda _t: None,  # skip the post-load count verification
    )

    migrator.migrate_table(_tables()[0])  # orders -> replace
    migrator.migrate_table(_tables()[1])  # customers -> plain load

    # Only the replace table is dropped+recreated, and its index DDLs flow to the
    # importer for post-load creation; the other table is loaded as-is.
    assert recreated == ["orders"]
    assert importer.index_ddls_by_table == {
        "orders": ["CREATE INDEX ASYNC ix_orders"],
        "customers": None,
    }
    # A clean/replace load uses a plain INSERT (OnConflictMode.NONE) so DSQL never
    # silently drops a non-conflicting row; a non-replace single-PK load uses the
    # DSQL-safe idempotent SKIP_EXISTING (insert only missing keys).
    assert importer.on_conflict_by_table["orders"] is OnConflictMode.NONE
    assert importer.on_conflict_by_table["customers"] is OnConflictMode.SKIP_EXISTING


def test_batched_table_migrator_cdc_coexisting_skips_drop_uses_skip_existing() -> None:
    """Full load + CDC: even a replace_tables entry is loaded idempotently with
    no DROP+recreate (which would race the live sink)."""
    import dataclasses

    exporter = _FakeExporter()
    importer = _FakeImporter()
    recreated: list[str] = []

    def fake_recreator(table: TableDef) -> list[str]:
        recreated.append(table.name)
        return [f"CREATE INDEX ASYNC ix_{table.name}"]

    # orders is in replace_tables, but cdc_coexisting overrides DROP.
    inputs = dataclasses.replace(
        _inputs(), replace_tables=frozenset({"orders"}), cdc_coexisting=True
    )
    recreated_for_shortfall: list[str] = []
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
        table_recreator=fake_recreator,
        target_counter=lambda _t: (_ for _ in ()).throw(
            AssertionError("no count verification when CDC co-runs")
        ),
    )

    migrator.migrate_table(_tables()[0])  # orders

    # No DROP/recreate (would race the live CDC sink), idempotent SKIP_EXISTING,
    # and no replace-only post-load count check.
    assert recreated == []
    assert importer.index_ddls_by_table["orders"] is None
    assert importer.on_conflict_by_table["orders"] is OnConflictMode.SKIP_EXISTING


def test_batched_table_migrator_raises_on_target_shortfall_for_replace() -> None:
    """A clean replace whose target ends up short of the loaded count fails.

    Guards the observed DSQL silent row-drop: the importer may report success
    while fewer rows actually persist. The post-load count verification turns
    that into a per-table failure instead of a false 'DONE'.
    """
    import dataclasses

    exporter = _FakeExporter(rows_by_table={"orders": [{"id": 1}, {"id": 2}]})
    importer = _FakeImporter(rows=2)  # importer claims 2 rows loaded
    inputs = dataclasses.replace(_inputs(), replace_tables=frozenset({"orders"}))
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda _t: [],
        target_counter=lambda _t: 1,  # but the target only has 1 -> silent loss
    )
    with pytest.raises(RuntimeError, match="silent row loss"):
        migrator.migrate_table(_tables()[0])


def test_batched_table_migrator_replace_passes_when_target_count_matches() -> None:
    """No false failure when the target count meets/exceeds the loaded count."""
    import dataclasses

    exporter = _FakeExporter(rows_by_table={"orders": [{"id": 1}, {"id": 2}]})
    importer = _FakeImporter(rows=2)
    inputs = dataclasses.replace(_inputs(), replace_tables=frozenset({"orders"}))
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda _t: [],
        target_counter=lambda _t: 2,  # target has all loaded rows -> OK
    )
    result = migrator.migrate_table(_tables()[0])
    assert result.rows_loaded == 2


def test_views_referencing_selects_only_dependent_views() -> None:
    from dsql_migrator.ui.data_migration._engine import _views_referencing

    view_ddls = {
        "shop.customer_order_summary": (
            "CREATE VIEW shop.customer_order_summary AS "
            "SELECT * FROM shop.orders JOIN shop.customers USING (id)"
        ),
        "shop.unrelated": "CREATE VIEW shop.unrelated AS SELECT * FROM shop.audit_log",
        # A view whose name contains 'orders' as a substring of a longer token must
        # NOT match when replacing 'orders'.
        "shop.orders_archive_v": (
            "CREATE VIEW shop.orders_archive_v AS SELECT * FROM shop.orders_archive"
        ),
    }
    got = _views_referencing(view_ddls, frozenset({"shop.orders"}))
    assert got == [view_ddls["shop.customer_order_summary"]]
    # Replacing a table no view references -> nothing to drop.
    assert _views_referencing(view_ddls, frozenset({"shop.nothing"})) == []
    # Empty inputs are safe.
    assert _views_referencing({}, frozenset({"shop.orders"})) == []
    assert _views_referencing(view_ddls, frozenset()) == []


def _patch_view_ddl_calls(monkeypatch, migrator):
    """Record the module-level drop_object / recreate_table calls the view pass
    makes, and stub the connection factory so nothing touches a real DSQL.

    Patches the REAL functions the migrator calls (not a fake applier), so this
    test exercises the actual construction path -- the gap that let a broken
    ``SchemaApplier(...)`` slip through before (it required ``introspector``)."""
    import dsql_migrator.ui.data_migration._engine as engine

    dropped: list[str] = []
    recreated: list[tuple] = []
    monkeypatch.setattr(
        engine.BatchedTableMigrator,
        "_view_connection_factory",
        lambda self: (lambda: object()),
    )
    monkeypatch.setattr(
        "dsql_migrator.core.schema_applier.drop_object",
        lambda ddl, *, connection_factory: dropped.append(ddl),
    )
    monkeypatch.setattr(
        "dsql_migrator.core.schema_applier.recreate_table",
        lambda schema_ddls, target_ddl, *, connection_factory: recreated.append(
            (schema_ddls, target_ddl)
        ),
    )
    return dropped, recreated


def _view_migrator(**overrides):
    import dataclasses

    return BatchedTableMigrator(
        dataclasses.replace(_inputs(), **overrides),
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: _FakeImporter(),  # type: ignore[arg-type,return-value]
        table_recreator=lambda _t: [],
        target_counter=lambda _t: None,
    )


def test_migrator_predrops_and_recreates_dependent_views_on_replace(monkeypatch) -> None:
    view_ddl = "CREATE VIEW shop.customer_order_summary AS SELECT * FROM orders"
    migrator = _view_migrator(
        replace_tables=frozenset({"orders"}),
        dependent_view_ddls={"shop.customer_order_summary": view_ddl},
    )
    dropped, recreated = _patch_view_ddl_calls(monkeypatch, migrator)

    migrator.predrop_dependent_views()
    migrator.recreate_dependent_views()

    # The view referencing the replaced 'orders' is dropped first (pre-pass), then
    # recreated (DROP+CREATE, no schema DDLs) after the load.
    assert dropped == [view_ddl]
    assert recreated == [([], view_ddl)]


def test_migrator_skips_view_pass_on_append_or_cdc(monkeypatch) -> None:
    view_ddl = "CREATE VIEW v AS SELECT * FROM orders"

    # Append run (no replace tables) -> no view drop/recreate even if views exist.
    append = _view_migrator(
        replace_tables=frozenset(), dependent_view_ddls={"v": view_ddl}
    )
    dropped, recreated = _patch_view_ddl_calls(monkeypatch, append)
    append.predrop_dependent_views()
    append.recreate_dependent_views()
    assert dropped == [] and recreated == []

    # CDC coexisting overrides DROP -> also no view pass.
    cdc = _view_migrator(
        replace_tables=frozenset({"orders"}),
        cdc_coexisting=True,
        dependent_view_ddls={"v": view_ddl},
    )
    dropped2, recreated2 = _patch_view_ddl_calls(monkeypatch, cdc)
    cdc.predrop_dependent_views()
    cdc.recreate_dependent_views()
    assert dropped2 == [] and recreated2 == []


def test_reload_mode_drives_derived_replace_targets() -> None:
    state = DataMigrationState()
    state.set_tables_with_data(frozenset({"shop.orders", "shop.customers"}))
    # Default is append -> nothing dropped.
    assert state.reload_mode == "append"
    assert state.replace_targets == frozenset()
    # Choosing drop -> the pre-existing tables become the replace set.
    state.set_reload_mode("drop")
    assert state.replace_targets == frozenset({"shop.orders", "shop.customers"})
    # Back-compat setter: a non-empty set implies drop; empty implies append.
    state.set_replace_targets(frozenset({"shop.orders"}))
    assert state.reload_mode == "drop"
    assert state.replace_targets == frozenset({"shop.orders"})
    state.set_replace_targets(frozenset())
    assert state.reload_mode == "append"
    assert state.replace_targets == frozenset()


def test_batched_table_migrator_raises_on_batch_failure() -> None:
    importer = _FakeImporter(failures=1, first_error="OC000: boom")
    migrator = BatchedTableMigrator(
        _inputs(),
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
    )

    # A failed batch surfaces as a per-table failure with the underlying cause,
    # so the run records it against the table (Property: failure isolation).
    with pytest.raises(RuntimeError, match="OC000: boom"):
        migrator.migrate_table(_tables()[0])


def test_batched_table_migrator_treats_export_cancel_as_cooperative_stop() -> None:
    # If the source stream is interrupted by a cooperative stop (ExportCancelled
    # raised between pages while the importer is pulling rows), the table must be
    # reported as a stop (_FullLoadStopped -> retryable), NOT a data failure.
    from dsql_migrator.core.exporter import ExportCancelled
    from dsql_migrator.ui.data_migration import _FullLoadStopped

    class _CancellingImporter:
        def import_rows(self, rows, table, **_kwargs):  # noqa: ANN001
            # Consuming the stream raises ExportCancelled (mirrors keyset_stream).
            raise ExportCancelled(table.name)

    migrator = BatchedTableMigrator(
        _inputs(),
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: _CancellingImporter(),  # type: ignore[arg-type,return-value]
    )

    with pytest.raises(_FullLoadStopped):
        migrator.migrate_table(_tables()[0])


# ---------------------------------------------------------------------------
# Job-status mapping + per-session state/store
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("job_status", "expected"),
    [
        ("DONE", StepStatus.DONE),
        ("FAILED", StepStatus.FAILED),
        ("PENDING", None),
        ("RUNNING", None),
    ],
)
def test_job_status_to_step_status(job_status: str, expected) -> None:
    assert job_status_to_step_status(job_status) is expected


def test_reconcile_full_load_step_corrects_stale_in_progress() -> None:
    # The reported bug: after a restart the job is terminal (FAILED) but the
    # saved step stayed IN_PROGRESS, so the screen showed "in progress" forever.
    # Reconciliation must correct it to the terminal step on render.
    from dsql_migrator.ui.data_migration import reconcile_full_load_step

    assert (
        reconcile_full_load_step(StepStatus.IN_PROGRESS, "FAILED")
        is StepStatus.FAILED
    )
    assert (
        reconcile_full_load_step(StepStatus.IN_PROGRESS, "CANCELLED")
        is StepStatus.FAILED
    )
    assert (
        reconcile_full_load_step(StepStatus.IN_PROGRESS, "DONE") is StepStatus.DONE
    )


def test_reconcile_full_load_step_leaves_running_and_nonprogress_alone() -> None:
    from dsql_migrator.ui.data_migration import reconcile_full_load_step

    # Job still running -> nothing to reconcile (stay IN_PROGRESS).
    assert reconcile_full_load_step(StepStatus.IN_PROGRESS, "RUNNING") is None
    assert reconcile_full_load_step(StepStatus.IN_PROGRESS, "PENDING") is None
    # No job known (e.g. job_id not restored) -> leave the step unchanged.
    assert reconcile_full_load_step(StepStatus.IN_PROGRESS, None) is None
    # A non-IN_PROGRESS saved step is never touched, even with a terminal job.
    assert reconcile_full_load_step(StepStatus.DONE, "FAILED") is None
    assert reconcile_full_load_step(StepStatus.NOT_STARTED, "FAILED") is None


def test_data_migration_step_after_cdc_promotes_when_streaming() -> None:
    from dsql_migrator.ui.data_migration import data_migration_step_after_cdc

    # CDC streaming + step not yet terminal -> promote to DONE so Validation opens
    # (covers the CDC-only / reconnected-no-watermark case that never runs Full Load).
    assert (
        data_migration_step_after_cdc(StepStatus.NOT_STARTED, cdc_streaming=True)
        is StepStatus.DONE
    )
    assert (
        data_migration_step_after_cdc(StepStatus.IN_PROGRESS, cdc_streaming=True)
        is StepStatus.DONE
    )


def test_data_migration_step_after_cdc_no_change_when_not_streaming_or_terminal() -> None:
    from dsql_migrator.ui.data_migration import data_migration_step_after_cdc

    # Not streaming -> never promote.
    assert data_migration_step_after_cdc(StepStatus.NOT_STARTED, cdc_streaming=False) is None
    # Already terminal -> never downgrade/override (a finished Full Load or a real
    # failure wins over the CDC promotion).
    assert data_migration_step_after_cdc(StepStatus.DONE, cdc_streaming=True) is None
    assert data_migration_step_after_cdc(StepStatus.FAILED, cdc_streaming=True) is None


def test_state_error_handoff_and_clear() -> None:
    state = DataMigrationState()
    assert state.error is None
    state.set_error("boom")
    assert state.error == "boom"
    state.clear_outputs()
    assert state.error is None


def test_store_is_isolated_per_session() -> None:
    store = DataMigrationStore()
    a = store.get_or_create("session-a")
    b = store.get_or_create("session-b")
    assert a is not b
    assert store.get_or_create("session-a") is a

    a.job_id = "job-1"
    assert b.job_id is None


def test_store_clear_removes_only_target_session() -> None:
    store = DataMigrationStore()
    store.get_or_create("session-a")
    store.get_or_create("session-b")

    store.clear("session-a")
    assert store.get("session-a") is None
    assert store.get("session-b") is not None


def test_reset_in_place_keeps_object_and_preserves_session_binding() -> None:
    # "Start over" resets DataMigrationState in place (the workflow builder captured
    # it in a closure). The reset must (a) keep the SAME instance, (b) wipe its
    # state, and (c) preserve the session binding set once at build time -- so
    # migration_type still reads through to the live session after the reset.
    from dsql_migrator.ui.data_migration import MigrationType
    from dsql_migrator.ui.session import SessionConnectionState

    store = DataMigrationStore()
    session = SessionConnectionState()
    state = store.get_or_create("s1")
    state.bind_session(session)
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    state.job_id = "job-xyz"

    store.reset_in_place("s1")

    assert store.get_or_create("s1") is state  # same instance
    assert state.job_id is None  # wiped
    # Binding preserved: setting the type still writes through to the session.
    assert state._session is session
    state.migration_type = MigrationType.CDC_ONLY
    assert session.migration_type is MigrationType.CDC_ONLY
    store.clear("missing")
    store.clear(None)


# ---------------------------------------------------------------------------
# run_full_load: per-table error recording to the error log (Property 15)
# ---------------------------------------------------------------------------


def _run_full_load_job(
    migrator: _FakeMigrator, tables: list[TableDef]
) -> tuple[MigrationJob, str, ErrorLogStore]:
    """Run run_full_load to completion; return (job, job_id, error_log)."""
    manager = JobManager()
    error_log = ErrorLogStore()
    job_id = manager.submit(
        lambda handle: run_full_load(
            handle, tables, migrator=migrator, error_log=error_log
        )
    )
    assert manager.wait(job_id, timeout=5.0)
    return manager.get_status(job_id), job_id, error_log


def test_run_full_load_records_per_table_failure_to_error_log() -> None:
    migrator = _FakeMigrator(
        rows_by_table={"customers": 3}, fail_tables=("orders",)
    )
    job, job_id, error_log = _run_full_load_job(migrator, _tables())

    # The failed table is isolated; the other still loads (same as before).
    by_name = {chunk.chunk_id: chunk for chunk in job.chunks}
    assert by_name["orders"].status == "FAILED"
    assert by_name["customers"].status == "DONE"
    # A run with any failed table is reported FAILED (incomplete target data).
    assert job.status == "FAILED"

    # The failure is now a downloadable error record keyed by the job id.
    summary = error_log.summary(job_id)
    assert summary.total_errors == 1
    assert summary.errors_by_table == {"orders": 1}
    assert summary.log_available is True
    log = error_log.render_log(job_id).decode("utf-8")
    assert '"table":"orders"' in log
    assert "load failed for orders" in log


def test_run_full_load_no_errors_leaves_empty_log() -> None:
    migrator = _FakeMigrator(rows_by_table={"orders": 10, "customers": 3})
    job, job_id, error_log = _run_full_load_job(migrator, _tables())

    assert job.status == "DONE"
    assert error_log.summary(job_id).total_errors == 0
    assert error_log.render_log(job_id) == b""


def test_run_full_load_watermark_failure_records_no_table_errors() -> None:
    migrator = _FakeMigrator(watermark_error=RuntimeError("no REPLICATION CLIENT"))
    manager = JobManager()
    error_log = ErrorLogStore()
    job_id = manager.submit(
        lambda handle: run_full_load(
            handle, _tables(), migrator=migrator, error_log=error_log
        )
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)

    assert job.status == "FAILED"
    # A fatal watermark failure aborts before any table runs: no per-table errors.
    assert error_log.summary(job_id).total_errors == 0


# ---------------------------------------------------------------------------
# Prerequisite run-guard reason (Property 14)
# ---------------------------------------------------------------------------


def _result(
    check_id: PrerequisiteCheckId,
    status: PrerequisiteStatus,
    *,
    required: bool = True,
    target: str | None = None,
    title: str = "Check",
) -> PrerequisiteResult:
    return PrerequisiteResult(
        check_id=check_id,
        title=title,
        status=status,
        required=required,
        target=target,
    )


def test_prerequisite_block_reason_none_when_can_proceed() -> None:
    report = PrerequisiteReport.build(
        MigrationMode.FULL_LOAD,
        [_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS)],
    )
    assert prerequisite_block_reason(report) is None


def test_prerequisite_block_reason_lists_failed_required_checks() -> None:
    report = PrerequisiteReport.build(
        MigrationMode.FULL_LOAD,
        [
            _result(
                PrerequisiteCheckId.TARGET_SCHEMA_READY,
                PrerequisiteStatus.FAIL,
                target="app.orders",
                title="Target schema is ready for the table",
            ),
            _result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
            # A non-required failure must NOT block.
            _result(
                PrerequisiteCheckId.GTID_MODE,
                PrerequisiteStatus.FAIL,
                required=False,
                title="GTID mode is enabled",
            ),
        ],
    )
    reason = prerequisite_block_reason(report)
    assert reason is not None
    assert "Target schema is ready for the table (app.orders)" in reason
    assert "GTID mode" not in reason  # non-required failure excluded


# ---------------------------------------------------------------------------
# Error-log summary formatting (Property 15)
# ---------------------------------------------------------------------------


def test_format_error_summary_zero_one_many() -> None:
    assert format_error_summary(ErrorLogSummary()) == "No data errors recorded."
    one = ErrorLogSummary(
        total_errors=1, errors_by_table={"t": 1}, log_available=True
    )
    assert format_error_summary(one) == "1 data error across 1 table."
    many = ErrorLogSummary(
        total_errors=3, errors_by_table={"a": 2, "b": 1}, log_available=True
    )
    assert format_error_summary(many) == "3 data errors across 2 tables."


# ---------------------------------------------------------------------------
# Full Load run-guard reason (Property 14)
# ---------------------------------------------------------------------------


def test_full_load_run_guard_blocks_without_inventory() -> None:
    state = DataMigrationState()
    assert "Evaluation" in (full_load_run_guard_reason(state, None) or "")
    empty = SourceInventory(tables=[])
    assert "Evaluation" in (full_load_run_guard_reason(state, empty) or "")


def test_cdc_start_override_none_by_default() -> None:
    # Default mode is "auto", so no manual override regardless of entered values.
    state = DataMigrationState()
    assert state.cdc_start_mode() == "auto"
    assert state.cdc_start_override() is None


def test_cdc_start_override_from_gtid() -> None:
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(gtid="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5")
    override = state.cdc_start_override()
    assert override is not None
    assert override.has_coordinates()
    assert override.gtid_executed == "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
    assert override.binlog_file is None


def test_cdc_start_override_from_binlog() -> None:
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000123", binlog_pos=45678)
    override = state.cdc_start_override()
    assert override is not None
    assert override.binlog_file == "mysql-bin.000123"
    assert override.binlog_position == 45678


def test_cdc_start_override_auto_mode_ignores_entered_values() -> None:
    # Switching back to auto drops the override even if values are still stored.
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(gtid="x:1")
    assert state.cdc_start_override() is not None
    state.set_cdc_start_mode("auto")
    assert state.cdc_start_override() is None


def test_cdc_start_override_blank_clears() -> None:
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(gtid="x:1")
    assert state.cdc_start_override() is not None
    state.set_cdc_start_position(gtid="   ")  # blank clears
    assert state.cdc_start_override() is None


def test_cdc_start_override_partial_binlog_is_none() -> None:
    # A binlog file with no position is not usable coordinates.
    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000123", binlog_pos=None)
    assert state.cdc_start_override() is None


class _FakeCdcController:
    """Minimal MskConnectController stand-in for the poller test."""

    def __init__(self, statuses, health):
        self._statuses = statuses
        self._health = health

    def connector_statuses(self, names):
        return self._statuses

    def connector_health(self, names):
        return self._health


def test_refresh_cdc_status_builds_view_from_controller() -> None:
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _refresh_cdc_status

    state = DataMigrationState()
    state.set_cdc_controller(
        _FakeCdcController(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=2, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])
    _refresh_cdc_status(state)
    view = state.cdc_status_view
    assert view is not None
    assert view.connector_states == {"sink": "RUNNING"}


def test_refresh_cdc_status_errored_tasks_marks_failed() -> None:
    # A connector MSK reports RUNNING but with errored tasks is degraded -> FAILED.
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _refresh_cdc_status

    state = DataMigrationState()
    state.set_cdc_controller(
        _FakeCdcController(
            statuses=[ConnectorStatus(name="src", state=ConnectorState.RUNNING)],
            health={"src": ConnectorHealth(running_tasks=0, errored_tasks=1)},
        )
    )
    state.set_cdc_connector_names(["src"])
    _refresh_cdc_status(state)
    assert state.cdc_status_view.connector_states == {"src": "FAILED"}


def test_refresh_cdc_status_no_controller_is_noop() -> None:
    from dsql_migrator.ui.data_migration import _refresh_cdc_status

    state = DataMigrationState()
    _refresh_cdc_status(state)  # no controller wired
    assert state.cdc_status_view is None


def test_ensure_cdc_controller_detects_deployed_connectors_for_start_over() -> None:
    # The Start-over dialog decides whether to show the stop/delete tiles from the
    # CACHED cdc_connector_names / cdc_stack_phase (what _cdc_deployed reads). Those
    # are populated by _ensure_cdc_controller's live probe. This is the contract the
    # Start-over fix relies on: when connectors for MY stack are deployed, a probe
    # (triggered on Start-over from any step, not only the CDC step) must flip the
    # cached state so cdc_deployed becomes True and the tiles appear.
    from dsql_migrator.core.cdc import cdc_expected_connector_names
    from dsql_migrator.ui.data_migration._status import _ensure_cdc_controller

    state = DataMigrationState()
    stack = state.cdc_stack_name  # default stack name
    src, sink = cdc_expected_connector_names(stack)

    class _Ctl:
        def list_connectors(self):
            # Both of MY connectors present and RUNNING on AWS.
            return [
                {"connectorName": src, "connectorState": "RUNNING"},
                {"connectorName": sink, "connectorState": "RUNNING"},
            ]

    # Pre-wire the controller (the pre-wired branch of _ensure_cdc_controller re-lists
    # connectors) and bypass the throttle exactly as the Start-over probe does.
    state.set_cdc_controller(_Ctl())
    state._cdc_discovery_monotonic = None

    class _Sess:
        target_config = None  # unused on the pre-wired path
        aws_profile = None

    _ensure_cdc_controller(state, _Sess())

    # cdc_deployed (app._cdc_deployed) reads exactly these -> now truthy -> tiles show.
    assert state.cdc_connector_names == [src, sink]
    assert state.cdc_connector_running_names == [src, sink]


def test_fetch_cdc_status_is_pure_read_then_apply_builds_view() -> None:
    # The live poller splits the blocking network read (_fetch_cdc_status, run on a
    # worker thread) from the in-memory view build (_apply_cdc_status, run on the
    # event loop). Fetch must NOT mutate state; apply must build the view from it.
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status, _fetch_cdc_status

    state = DataMigrationState()
    state.set_cdc_controller(
        _FakeCdcController(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=2, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])

    fetched = _fetch_cdc_status(state)
    assert fetched is not None
    # Pure read: no view written yet.
    assert state.cdc_status_view is None

    _apply_cdc_status(state, fetched)
    assert state.cdc_status_view is not None
    assert state.cdc_status_view.connector_states == {"sink": "RUNNING"}
    # DLQ depth is sourced from the error-log quarantine count (0 on a clean
    # stream, never None) so the DLQ panel always renders once streaming.
    assert state.cdc_status_view.dlq_depth == 0


def test_fetch_cdc_status_includes_dlq_errors_when_controller_exposes_reader() -> None:
    from dsql_migrator.core.cdc import (
        CdcConnectorError,
        ConnectorState,
        ConnectorStatus,
    )
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _fetch_cdc_status

    class _CtrlWithDlq(_FakeCdcController):
        def dlq_errors(self, log_group, **_kw):
            assert log_group.startswith("/msk-connect/")
            return [CdcConnectorError(table="orders", message="DLQ offset=1: boom")]

    state = DataMigrationState()
    state.set_cdc_controller(
        _CtrlWithDlq(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])

    fetched = _fetch_cdc_status(state)
    assert fetched is not None
    _statuses, _health, dlq_errors = fetched
    assert [e.table for e in dlq_errors] == ["orders"]


def test_apply_cdc_status_folds_dlq_errors_into_error_log_and_depth() -> None:
    # Regression: _apply_cdc_status must fold newly-read DLQ events into the
    # single error log so DLQ depth / per-table "Quarantined" surface the real
    # pipeline. This path only runs when there ARE quarantined records AND a
    # job_id, so a clean stream never exercises it -- which is exactly when a
    # wrong import name (CdcMonitor, since renamed to CdcPipelineOrchestrator)
    # would silently break the DLQ surface the moment a record is quarantined.
    from dsql_migrator.core.cdc import (
        CdcConnectorError,
        ConnectorState,
        ConnectorStatus,
    )
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    state = DataMigrationState()
    state.job_id = "job-cdc"
    statuses = [ConnectorStatus(name="sink", state=ConnectorState.RUNNING)]
    health = {"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)}
    dlq_errors = [
        CdcConnectorError(table="orders", message="DLQ offset=1: boom"),
        CdcConnectorError(table="orders", message="DLQ offset=2: boom"),
    ]

    _apply_cdc_status(state, (statuses, health, dlq_errors))

    view = state.cdc_status_view
    assert view is not None
    # The two quarantined records landed in the single error log and drive depth.
    assert view.dlq_depth == 2
    assert state.error_log.summary("job-cdc").errors_by_table == {"orders": 2}


def test_fetch_cdc_status_returns_none_without_controller() -> None:
    from dsql_migrator.ui.data_migration import _apply_cdc_status, _fetch_cdc_status

    state = DataMigrationState()
    assert _fetch_cdc_status(state) is None
    # apply(None) is a safe no-op.
    _apply_cdc_status(state, None)
    assert state.cdc_status_view is None


def test_fetch_cdc_status_swallows_controller_error() -> None:
    # A controller raising on the network read returns None (keep last good view),
    # rather than propagating into the poll.
    from dsql_migrator.ui.data_migration import _fetch_cdc_status

    class _BoomController:
        def connector_statuses(self, names):
            raise RuntimeError("MSK Connect unreachable")

        def connector_health(self, names):
            return {}

    state = DataMigrationState()
    state.set_cdc_controller(_BoomController())
    state.set_cdc_connector_names(["src"])
    assert _fetch_cdc_status(state) is None


# ---------------------------------------------------------------------------
# cdc_streaming_started -- locks the start point + object browser once CDC runs
# ---------------------------------------------------------------------------


def test_cdc_streaming_started_false_before_start() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    state = DataMigrationState()
    assert cdc_streaming_started(state, _StubJobManager({})) is False


def test_cdc_streaming_started_when_connectors_detected() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    state = DataMigrationState()
    state.set_cdc_controller(_FakeCdcController(statuses=[], health={}))
    state.set_cdc_connector_names(["mysql-dsql-cdc-stack-debezium-source"])
    assert cdc_streaming_started(state, _StubJobManager({}))


def test_cdc_streaming_started_when_phase_running() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    state = DataMigrationState()
    state.set_cdc_stack_phase("running")
    assert cdc_streaming_started(state, _StubJobManager({}))


def test_cdc_streaming_started_when_lifecycle_job_in_flight() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("start-1", kind="start")
    mgr = _StubJobManager({"start-1": _StubJob("RUNNING")})
    assert cdc_streaming_started(state, mgr)


def test_cdc_streaming_started_false_when_only_infra() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    # Infra deployed but no connectors and no job -> not streaming yet, start
    # point + table picker stay editable.
    state = DataMigrationState()
    state.set_cdc_stack_phase("infra")
    assert cdc_streaming_started(state, _StubJobManager({})) is False


def test_full_load_run_guard_requires_checks_first() -> None:
    state = DataMigrationState()
    reason = full_load_run_guard_reason(state, _inventory())
    assert reason is not None
    assert "prerequisite checks" in reason


def test_full_load_run_guard_passes_when_checks_pass() -> None:
    state = DataMigrationState()
    report = PrerequisiteReport.build(
        MigrationMode.FULL_LOAD,
        [_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS)],
    )
    state.set_prereq_report(MigrationMode.FULL_LOAD, report)
    assert full_load_run_guard_reason(state, _inventory()) is None


def test_full_load_run_guard_blocks_on_failed_required_check() -> None:
    state = DataMigrationState()
    report = PrerequisiteReport.build(
        MigrationMode.FULL_LOAD,
        [
            _result(
                PrerequisiteCheckId.TARGET_SCHEMA_READY,
                PrerequisiteStatus.FAIL,
                target="orders",
                title="Target schema is ready for the table",
            )
        ],
    )
    state.set_prereq_report(MigrationMode.FULL_LOAD, report)
    reason = full_load_run_guard_reason(state, _inventory())
    assert reason is not None
    assert "Target schema is ready for the table (orders)" in reason


def test_full_load_run_guard_allows_rerun_when_already_run_without_report() -> None:
    # Restored session: the prerequisite report lives only in process memory and
    # is NOT persisted, so after a reconnect get_prereq_report() is None even
    # though the Full Load already ran. A run can only have started once the
    # checks passed, so a finished/started Full Load (has_run) must not be
    # re-blocked on the absent report -- otherwise navigating Back to the
    # finished Full Load step shows a locked "Re-run Full Load" button.
    state = DataMigrationState()
    # No report set (mimics a fresh process after restore).
    assert full_load_run_guard_reason(state, _inventory()) is not None
    assert full_load_run_guard_reason(state, _inventory(), has_run=True) is None
    assert (
        full_load_run_guard_reason(
            state, _inventory(), prereq_mode=MigrationMode.CDC, has_run=True
        )
        is None
    )


def test_full_load_error_migration_context_describes_situation() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        full_load_error_migration_context,
    )

    state = DataMigrationState()
    state.migration_type = MigrationType.FULL_LOAD_AND_CDC
    state.set_replace_targets(frozenset({"customers_sample.countries"}))

    # A table that pre-existed on the target (DROP+recreate) in a CDC plan.
    ctx = full_load_error_migration_context(
        state, table_name="customers_sample.countries", cdc_live=False
    )
    assert "Full Load + CDC" in ctx
    assert "DROP+recreated" in ctx
    assert "CDC has not started" in ctx
    # A freshly-created table in a full-load-only plan: no CDC line, no DROP note.
    fl_only = DataMigrationState()
    fl_only.migration_type = MigrationType.FULL_LOAD_ONLY
    ctx2 = full_load_error_migration_context(
        fl_only, table_name="app.orders", cdc_live=False
    )
    assert "Full Load only" in ctx2
    assert "created fresh" in ctx2
    assert "CDC" not in ctx2.replace("no CDC", "")  # no CDC-streaming line
    # Context carries no credential/connection detail (Property 7).
    assert "password" not in ctx.lower() and "host" not in ctx.lower()


def test_prerequisites_section_expanded_stays_open_while_actionable() -> None:
    from dsql_migrator.ui.data_migration import prerequisites_section_expanded

    # Open when it is the active sub-step (the normal case).
    assert prerequisites_section_expanded(
        active_substep="prerequisites", running=False, done=False
    )
    # Reconnected session: active sub-step is a LATER step, but the checks are
    # running (just clicked "Check") -> must stay open so the spinner shows.
    assert prerequisites_section_expanded(
        active_substep="full_load", running=True, done=False
    )
    # Reconnected, not running yet, but still required (guard not cleared) ->
    # stay open so the user can act on it.
    assert prerequisites_section_expanded(
        active_substep="full_load", running=False, done=False
    )
    # Done and not the active step -> collapse (the flow has moved on).
    assert not prerequisites_section_expanded(
        active_substep="full_load", running=False, done=True
    )


def test_full_load_run_guard_reconnect_wording_when_checks_were_cleared() -> None:
    # Reconnected session: the report isn't persisted, but the persisted
    # active_substep proves the user had already advanced past Prerequisites
    # (the "Continue" gate is only reachable once checks passed) and hadn't yet
    # started the load (has_run stays False). The guard still blocks -- the
    # read-only checks must re-run after a reconnect -- but the message is worded
    # for the reconnect case, not the blunt first-run prompt.
    state = DataMigrationState()
    state.set_active_substep("full_load")
    reason = full_load_run_guard_reason(state, _inventory())
    assert reason is not None
    assert "Reconnected" in reason
    assert "read-only" in reason
    # A first-time user (no advanced substep, no run) still gets the plain prompt.
    fresh = DataMigrationState()
    plain = full_load_run_guard_reason(fresh, _inventory())
    assert plain is not None
    assert "Reconnected" not in plain
    assert "prerequisite checks" in plain


def test_full_load_run_guard_still_blocks_failed_check_even_after_run() -> None:
    # has_run only excuses an ABSENT report (restore). A report that is present
    # and failing is a live signal and must still block, even mid-migration.
    state = DataMigrationState()
    report = PrerequisiteReport.build(
        MigrationMode.FULL_LOAD,
        [
            _result(
                PrerequisiteCheckId.TARGET_SCHEMA_READY,
                PrerequisiteStatus.FAIL,
                target="orders",
                title="Target schema is ready for the table",
            )
        ],
    )
    state.set_prereq_report(MigrationMode.FULL_LOAD, report)
    assert full_load_run_guard_reason(state, _inventory(), has_run=True) is not None


# ---------------------------------------------------------------------------
# Unified Full Load status view (Req 13.1) -- provider mapping
# ---------------------------------------------------------------------------


def test_build_full_load_status_view_maps_job_and_errors() -> None:
    from dsql_migrator.core.models import LoadKind
    from dsql_migrator.ui.data_migration import build_full_load_status_view

    migrator = _FakeMigrator(
        rows_by_table={"customers": 3}, fail_tables=("orders",)
    )
    job, job_id, error_log = _run_full_load_job(migrator, _tables())

    view = build_full_load_status_view(job, error_log.summary(job_id))

    assert view.kind == LoadKind.FULL_LOAD
    assert view.progress_pct == 100.0
    assert view.tables_done == 1
    assert view.tables_failed == 1
    by_table = {row.table: row for row in view.tables}
    assert by_table["orders"].state == "FAILED"
    assert by_table["orders"].errors == 1
    assert by_table["customers"].state == "DONE"
    assert by_table["customers"].rows_loaded == 3
    assert by_table["customers"].errors == 0
    assert view.error_summary is not None
    assert view.error_summary.total_errors == 1


# ---------------------------------------------------------------------------
# Hierarchical table picker scoped to generated tables (Req 5.9)
# ---------------------------------------------------------------------------


def _qualified_inventory() -> SourceInventory:
    """A two-schema inventory to exercise schema grouping in the picker."""
    return SourceInventory(
        tables=[
            TableDef(
                name="app.orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
            TableDef(
                name="app.customers",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
            TableDef(
                name="audit.events",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
        ]
    )


def test_generated_table_names_maps_node_ids_in_inventory_order() -> None:
    inventory = _inventory()  # orders, customers (inventory order)
    node_ids = [f"{TABLE_PREFIX}customers", f"{TABLE_PREFIX}orders"]
    # Result follows inventory order, not node-id order.
    assert generated_table_names(inventory, node_ids) == ["orders", "customers"]


def test_generated_table_names_empty_when_nothing_generated() -> None:
    assert generated_table_names(_inventory(), None) == []
    assert generated_table_names(_inventory(), []) == []


def test_generated_table_names_ignores_unknown_and_non_table_nodes() -> None:
    inventory = _inventory()
    node_ids = [
        f"{TABLE_PREFIX}orders",
        f"{TABLE_PREFIX}ghost",  # not in inventory
        "category:tables:source",  # category node, not a table leaf
    ]
    assert generated_table_names(inventory, node_ids) == ["orders"]


def test_effective_selection_defaults_to_all_generated_until_touched() -> None:
    generated = ["orders", "customers"]
    # Untouched: every generated table is pre-selected (the Object browser default).
    assert effective_migration_selection(
        generated, TableSelection(), touched=False
    ) == ["orders", "customers"]


def test_effective_selection_intersects_with_generated_when_touched() -> None:
    generated = ["orders", "customers"]
    selection = TableSelection(selected_tables=["customers", "stale"])
    # Touched: only chosen names that are still generated, in generated order.
    assert effective_migration_selection(
        generated, selection, touched=True
    ) == ["customers"]


def test_effective_selection_explicit_empty_stays_empty_when_touched() -> None:
    generated = ["orders", "customers"]
    assert effective_migration_selection(
        generated, TableSelection(selected_tables=[]), touched=True
    ) == []


def test_effective_selection_default_pre_checks_only_target_existing() -> None:
    # Both tables are migratable, but only "orders" exists on the target DSQL,
    # so only "orders" is pre-checked out of the box (the rest stay available).
    migratable = ["orders", "customers"]
    assert effective_migration_selection(
        migratable, TableSelection(), touched=False, default=["orders"]
    ) == ["orders"]


def test_effective_selection_default_intersects_with_migratable() -> None:
    migratable = ["orders", "customers"]
    # A default name not in the migratable universe is ignored.
    assert effective_migration_selection(
        migratable, TableSelection(), touched=False, default=["ghost", "customers"]
    ) == ["customers"]


def test_effective_selection_touched_ignores_default() -> None:
    migratable = ["orders", "customers"]
    # Once the user has touched the picker, the default no longer applies.
    assert effective_migration_selection(
        migratable,
        TableSelection(selected_tables=["customers"]),
        touched=True,
        default=["orders"],
    ) == ["customers"]


def test_build_migration_table_tree_groups_by_schema_tables_only() -> None:
    inventory = _qualified_inventory()
    generated = ["app.orders", "app.customers", "audit.events"]
    tree = build_migration_table_tree(inventory, generated)

    # One node per schema, in first-seen order.
    assert [n["id"] for n in tree] == ["schema:app", "schema:audit"]
    app_categories = tree[0]["children"]
    # Tables-only: a single "Tables" category, no Views/Triggers/Routines.
    assert len(app_categories) == 1
    assert app_categories[0]["id"] == "category:tables:app"
    assert app_categories[0]["label"] == "Tables (2)"
    leaf_ids = [leaf["id"] for leaf in app_categories[0]["children"]]
    assert leaf_ids == [
        f"{TABLE_PREFIX}app.orders",
        f"{TABLE_PREFIX}app.customers",
    ]


def test_build_migration_table_tree_omits_non_migratable_tables() -> None:
    inventory = _inventory()  # orders, customers
    # Only "orders" has generated DDL; "customers" has no target table and must
    # be omitted entirely (not listed disabled).
    tree = build_migration_table_tree(inventory, ["orders"])
    leaves = tree[0]["children"][0]["children"]
    leaf_ids = [leaf["id"] for leaf in leaves]

    assert leaf_ids == [f"{TABLE_PREFIX}orders"]
    assert tree[0]["children"][0]["label"] == "Tables (1)"
    assert all("no target table" not in leaf["label"] for leaf in leaves)


def test_build_migration_table_tree_omits_schema_with_no_migratable() -> None:
    # A schema with no migratable tables (e.g. tpcds, absent from the target)
    # does not appear in the picker at all -- so no parent ever renders an
    # indeterminate dash for a non-selectable schema.
    inventory = _qualified_inventory()  # schemas: app (orders, customers), audit
    tree = build_migration_table_tree(inventory, ["app.orders"])
    schema_ids = [n["id"] for n in tree]

    # Only "app" (has a migratable table) is listed; "audit" is omitted.
    assert schema_ids == ["schema:app"]
    app_leaves = tree[0]["children"][0]["children"]
    assert [leaf["id"] for leaf in app_leaves] == [f"{TABLE_PREFIX}app.orders"]


def test_build_migration_table_tree_leaves_carry_pk_indicator_metadata() -> None:
    # Each table leaf carries has_pk (whether the table has a primary key) and a
    # "header": "table" hook so the renderer's header-table slot can show a PK
    # indicator. Non-leaf nodes (schema / "Tables (N)") do not carry these.
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="orders",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
            TableDef(
                name="audit_log",
                columns=[ColumnDef(name="msg", mysql_type="text", nullable=True)],
                primary_key=[],  # no PK
            ),
        ]
    )
    tree = build_migration_table_tree(inventory, ["orders", "audit_log"])
    leaves = tree[0]["children"][0]["children"]
    by_id = {leaf["id"]: leaf for leaf in leaves}
    assert by_id[f"{TABLE_PREFIX}orders"]["has_pk"] is True
    assert by_id[f"{TABLE_PREFIX}audit_log"]["has_pk"] is False
    assert all(leaf["header"] == "table" for leaf in leaves)
    # The schema and category nodes are not table leaves -> no PK hook on them.
    assert "header" not in tree[0]
    assert "header" not in tree[0]["children"][0]


# ---------------------------------------------------------------------------
# Migratable set includes tables already present on the target (Schema
# Conversion may have been applied beforehand) -- not only this session's
# generated DDL.
# ---------------------------------------------------------------------------


def _target_inventory(*table_names: str) -> TargetInventory:
    """Build a target catalog whose ``public`` schema has the given tables."""
    return TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="public",
                tables=[
                    TargetRelation(
                        schema_name="public",
                        name=name,
                        kind=TargetObjectKind.TABLE,
                    )
                    for name in table_names
                ],
            )
        ]
    )


def test_target_existing_table_names_matches_case_insensitive_unqualified() -> None:
    inventory = _inventory()  # orders, customers (unqualified)
    target = _target_inventory("ORDERS")  # case-insensitive, unqualified
    assert target_existing_table_names(inventory, target) == ["orders"]


def test_target_existing_table_names_empty_without_target() -> None:
    assert target_existing_table_names(_inventory(), None) == []


def test_prereq_running_flag_is_per_mode_and_transient() -> None:
    # Drives the immediate "checking..." feedback: independent per mode and
    # cleared once the run finishes.
    state = DataMigrationState()
    assert state.is_prereq_running(MigrationMode.FULL_LOAD) is False

    state.set_prereq_running(MigrationMode.FULL_LOAD)
    assert state.is_prereq_running(MigrationMode.FULL_LOAD) is True
    assert state.is_prereq_running(MigrationMode.CDC) is False  # independent

    state.clear_prereq_running(MigrationMode.FULL_LOAD)
    assert state.is_prereq_running(MigrationMode.FULL_LOAD) is False


def test_target_existing_table_names_qualified_does_not_cross_schema() -> None:
    # A qualified source table matches the target only within its own schema.
    # Here schema_a.items has no counterpart (no schema_a on the target), while
    # schema_b.items does -- so only the same-schema table is flagged as
    # existing (regression: an unqualified name match previously flagged both).
    inventory = SourceInventory(
        tables=[
            TableDef(
                name="schema_a.items",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
            TableDef(
                name="schema_b.items",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            ),
        ]
    )
    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="schema_b",
                tables=[
                    TargetRelation(
                        schema_name="schema_b",
                        name="items",
                        kind=TargetObjectKind.TABLE,
                    )
                ],
            )
        ]
    )
    # Only the same-schema match qualifies; schema_a.items is excluded.
    assert target_existing_table_names(inventory, target) == ["schema_b.items"]


def test_target_existing_table_names_ignores_target_views() -> None:
    inventory = _inventory()
    target = TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="public",
                views=[
                    TargetRelation(
                        schema_name="public",
                        name="orders",
                        kind=TargetObjectKind.VIEW,
                    )
                ],
            )
        ]
    )
    # A target *view* named like a source table is not a load target.
    assert target_existing_table_names(inventory, target) == []


def test_migratable_unions_generated_and_target_existing() -> None:
    inventory = _inventory()  # orders, customers
    # Nothing generated this session, but "customers" already exists on target.
    result = migratable_table_names(
        inventory, generated_node_ids=None, target=_target_inventory("customers")
    )
    assert result == ["customers"]


def test_migratable_allows_proceeding_without_current_session_conversion() -> None:
    """The whole point: a pre-applied target schema makes tables migratable."""
    inventory = _inventory()
    # Both tables already exist on the target (Schema Conversion run earlier);
    # generated_node_ids is None because this session never ran Step 2.
    result = migratable_table_names(
        inventory,
        generated_node_ids=None,
        target=_target_inventory("orders", "customers"),
    )
    assert result == ["orders", "customers"]


def test_migratable_dedups_and_preserves_inventory_order() -> None:
    inventory = _inventory()  # orders, customers
    # "orders" generated this session AND present on target -> appears once.
    result = migratable_table_names(
        inventory,
        generated_node_ids=[f"{TABLE_PREFIX}orders"],
        target=_target_inventory("orders", "customers"),
    )
    assert result == ["orders", "customers"]


def test_migratable_empty_when_neither_generated_nor_on_target() -> None:
    assert migratable_table_names(_inventory(), None, None) == []
    assert migratable_table_names(_inventory(), [], _target_inventory()) == []


# ---------------------------------------------------------------------------
# Prerequisite results grouped by user-facing category (Req 5.10 / UX grouping)
# ---------------------------------------------------------------------------


def test_group_prereq_results_buckets_checks_into_ordered_categories() -> None:
    results = [
        _result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.TARGET_DSQL_REACHABLE, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.TARGET_IAM_AUTH, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.REPLICATION_GRANTS, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.TABLE_PRIMARY_KEY, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.TARGET_SCHEMA_READY, PrerequisiteStatus.PASS),
    ]
    groups = group_prereq_results(results)

    # Categories appear in display order, connectivity first.
    assert [g.category for g in groups] == [
        PrereqCategory.CONNECTIVITY,
        PrereqCategory.SOURCE_CONFIG,
        PrereqCategory.SCHEMA_TABLES,
    ]
    connectivity = groups[0]
    assert {r.check_id for r in connectivity.results} == {
        PrerequisiteCheckId.SOURCE_REACHABLE,
        PrerequisiteCheckId.TARGET_DSQL_REACHABLE,
        PrerequisiteCheckId.TARGET_IAM_AUTH,
    }


def test_group_prereq_results_omits_empty_categories() -> None:
    groups = group_prereq_results(
        [_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS)]
    )
    assert [g.category for g in groups] == [PrereqCategory.CONNECTIVITY]


def test_group_rollup_blocks_on_required_failure() -> None:
    results = [
        _result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.TARGET_IAM_AUTH, PrerequisiteStatus.FAIL),
    ]
    (connectivity,) = group_prereq_results(results)
    assert connectivity.status is PrerequisiteStatus.FAIL
    assert connectivity.summary == "1 failed · 1 passed"


def test_group_rollup_warns_on_warning_only() -> None:
    results = [
        _result(
            PrerequisiteCheckId.GTID_MODE,
            PrerequisiteStatus.WARN,
            required=False,
        ),
        _result(PrerequisiteCheckId.REPLICATION_GRANTS, PrerequisiteStatus.PASS),
    ]
    (source_config,) = group_prereq_results(results)
    assert source_config.status is PrerequisiteStatus.WARN
    assert source_config.summary == "1 warning · 1 passed"


def test_group_rollup_info_is_calmer_than_warn() -> None:
    # An INFO-only category (e.g. GTID off = optional recommendation) rolls up to
    # INFO, not WARN -- so it is not treated as a problem and (per the renderer)
    # does not auto-expand. The summary labels it a "recommendation".
    results = [
        _result(
            PrerequisiteCheckId.GTID_MODE,
            PrerequisiteStatus.INFO,
            required=False,
        ),
        _result(PrerequisiteCheckId.REPLICATION_GRANTS, PrerequisiteStatus.PASS),
    ]
    (source_config,) = group_prereq_results(results)
    assert source_config.status is PrerequisiteStatus.INFO
    assert source_config.summary == "1 recommendation · 1 passed"


def test_rollup_warn_outranks_info() -> None:
    # Severity ordering within a category: a real WARN dominates an INFO.
    from dsql_migrator.ui.data_migration import _rollup_category_status

    results = [
        _result(PrerequisiteCheckId.GTID_MODE, PrerequisiteStatus.INFO, required=False),
        _result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.WARN, required=False),
    ]
    assert _rollup_category_status(results) is PrerequisiteStatus.WARN


def test_rollup_info_only_is_info() -> None:
    from dsql_migrator.ui.data_migration import _rollup_category_status

    results = [
        _result(PrerequisiteCheckId.GTID_MODE, PrerequisiteStatus.INFO, required=False),
        _result(PrerequisiteCheckId.REPLICATION_GRANTS, PrerequisiteStatus.PASS),
    ]
    assert _rollup_category_status(results) is PrerequisiteStatus.INFO


def test_group_rollup_skip_when_all_skipped_is_not_applicable() -> None:
    results = [
        _result(PrerequisiteCheckId.MSK_AVAILABLE, PrerequisiteStatus.SKIP),
        _result(PrerequisiteCheckId.MSK_CONNECT_AVAILABLE, PrerequisiteStatus.SKIP),
    ]
    (streaming,) = group_prereq_results(results)
    assert streaming.category is PrereqCategory.STREAMING
    assert streaming.status is PrerequisiteStatus.SKIP
    assert streaming.summary == "Not applicable for this mode"


def test_group_rollup_passes_when_all_pass() -> None:
    results = [
        _result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
        _result(PrerequisiteCheckId.TARGET_DSQL_REACHABLE, PrerequisiteStatus.PASS),
    ]
    (connectivity,) = group_prereq_results(results)
    assert connectivity.status is PrerequisiteStatus.PASS
    assert connectivity.summary == "2 passed"


# ---------------------------------------------------------------------------
# Data Migration sub-step flow (Prerequisites -> Full Load -> CDC) helpers
# ---------------------------------------------------------------------------


def test_resolve_active_substep_defaults_to_prerequisites_without_job() -> None:
    from dsql_migrator.ui.data_migration import resolve_active_substep

    assert resolve_active_substep(None, has_job=False) == "prerequisites"


def test_resolve_active_substep_defaults_to_full_load_with_job() -> None:
    from dsql_migrator.ui.data_migration import resolve_active_substep

    # Once a Full Load job exists, default to Full Load so progress is in front.
    assert resolve_active_substep(None, has_job=True) == "full_load"


def test_resolve_active_substep_honors_explicit_choice() -> None:
    from dsql_migrator.ui.data_migration import resolve_active_substep

    assert resolve_active_substep("cdc", has_job=True) == "cdc"
    assert resolve_active_substep("prerequisites", has_job=True) == "prerequisites"


def test_resolve_active_substep_ignores_unknown_value() -> None:
    from dsql_migrator.ui.data_migration import resolve_active_substep

    assert resolve_active_substep("bogus", has_job=False) == "prerequisites"


# ---------------------------------------------------------------------------
# Migration-type helpers (Full load only / CDC only / Full load + CDC)
# ---------------------------------------------------------------------------


def test_prereq_mode_for_type() -> None:
    from dsql_migrator.ui.data_migration import MigrationType, prereq_mode_for_type

    # CDC's checks are a superset of Full Load's, so both CDC_ONLY and the
    # combined type check in CDC mode; only Full-load-only uses FULL_LOAD.
    assert prereq_mode_for_type(MigrationType.FULL_LOAD_ONLY) is MigrationMode.FULL_LOAD
    assert prereq_mode_for_type(MigrationType.CDC_ONLY) is MigrationMode.CDC
    assert (
        prereq_mode_for_type(MigrationType.FULL_LOAD_AND_CDC) is MigrationMode.CDC
    )


def test_substeps_for_type() -> None:
    from dsql_migrator.ui.data_migration import MigrationType, substeps_for_type

    assert substeps_for_type(MigrationType.FULL_LOAD_ONLY) == (
        "prerequisites",
        "full_load",
    )
    assert substeps_for_type(MigrationType.CDC_ONLY) == ("prerequisites", "cdc")
    assert substeps_for_type(MigrationType.FULL_LOAD_AND_CDC) == (
        "prerequisites",
        "full_load",
        "cdc",
    )


def test_migration_type_tiles_surface_cdc_requirements_upfront() -> None:
    from dsql_migrator.ui.data_migration import _MIGRATION_TYPE_META, MigrationType

    # The CDC modes must disclose the MSK pipeline + the source binlog requirement
    # at decision time (not only later in prerequisites). The requirement is ROW-mode
    # binlog (GTID is optional, not stated as required), kept concise (no time note).
    for mt in (MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC):
        meta = _MIGRATION_TYPE_META[mt]
        assert "MSK" in meta.requirements
        assert "binlog" in meta.requirements.lower()
        assert "ROW" in meta.requirements
        # GTID is optional for CDC -> must NOT be presented as a requirement here.
        assert "gtid" not in meta.requirements.lower()
        assert meta.when  # a "choose this when…" cue is present

    # Full Load only has no extra infrastructure and says so.
    full = _MIGRATION_TYPE_META[MigrationType.FULL_LOAD_ONLY]
    assert "MSK" not in full.requirements
    assert "near-zero-downtime" in _MIGRATION_TYPE_META[
        MigrationType.FULL_LOAD_AND_CDC
    ].when


# ---------------------------------------------------------------------------
# migration_type_locked -- the type is frozen once a migration has started
# ---------------------------------------------------------------------------


class _StubJob:
    def __init__(self, status: str) -> None:
        self.status = status


class _StubJobManager:
    """Returns a canned job per id, or raises JobNotFoundError for unknown ids."""

    def __init__(self, jobs: dict[str, _StubJob]) -> None:
        self._jobs = jobs

    def get_status(self, job_id: str) -> _StubJob:
        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id not in self._jobs:
            raise JobNotFoundError(job_id)
        return self._jobs[job_id]


def test_migration_type_unlocked_before_any_migration_starts() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    locked = migration_type_locked(
        state, _StubJobManager({}), status=StepStatus.NOT_STARTED
    )
    assert locked is False


def test_migration_type_locked_while_step_in_progress() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    assert migration_type_locked(
        state, _StubJobManager({}), status=StepStatus.IN_PROGRESS
    )


def test_migration_type_unlocked_while_full_load_job_running() -> None:
    # A running Full Load no longer locks the type: the type is a planning choice,
    # the running job is unaffected by switching the view, and the user may run
    # Full Load and CDC as separate passes. Only CDC streaming commits the type.
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    state.job_id = "fl-1"
    mgr = _StubJobManager({"fl-1": _StubJob("RUNNING")})
    assert migration_type_locked(state, mgr, status=StepStatus.NOT_STARTED) is False


def test_migration_type_unlocked_while_cdc_infra_deploying() -> None:
    # Deploying (or having deployed) CDC infrastructure does NOT lock the type:
    # idle infra is a billable trade-off the user owns; they can still switch.
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("PENDING")})
    assert migration_type_locked(state, mgr, status=StepStatus.NOT_STARTED) is False


def test_migration_type_locked_when_cdc_streaming() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    # A STREAMING cdc-stack (CDC started) commits the CDC mode -> locked.
    state = DataMigrationState()
    state.set_cdc_stack_phase("running")
    assert migration_type_locked(
        state, _StubJobManager({}), status=StepStatus.NOT_STARTED
    )


def test_migration_type_unlocked_when_jobs_finished_and_no_stack() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    # A finished Full Load job (DONE) with no streaming CDC does not lock --
    # the user may still re-choose the type.
    state = DataMigrationState()
    state.job_id = "fl-done"
    mgr = _StubJobManager({"fl-done": _StubJob("DONE")})
    assert migration_type_locked(state, mgr, status=StepStatus.NOT_STARTED) is False


def test_migration_type_locked_after_restore_when_cdc_streaming_started() -> None:
    # CDC connector names survive a restore; their presence means CDC streaming
    # was started, so the type must stay LOCKED (the live pipeline is committed).
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    state.set_cdc_connector_names(["mysql-dsql-cdc-stack-debezium-source", "dsql-sink"])
    assert migration_type_locked(
        state, _StubJobManager({}), status=StepStatus.NOT_STARTED
    )


def test_migration_type_unlocked_after_restore_when_only_schema_progressed() -> None:
    # Evaluation / Schema Conversion are mode-agnostic and a deployed-but-idle
    # cdc-stack does not lock: a restored session that has not STARTED CDC stays
    # editable.
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked
    from dsql_migrator.ui.session import SessionConnectionState
    from dsql_migrator.ui.workflow import StepStatus as WStatus
    from dsql_migrator.ui.workflow import WorkflowStep, with_status

    session = SessionConnectionState()
    session.set_workflow(
        with_status(session.workflow, WorkflowStep.EVALUATION, WStatus.DONE)
    )
    session.set_workflow(
        with_status(session.workflow, WorkflowStep.SCHEMA_CONVERSION, WStatus.DONE)
    )
    state = DataMigrationState()
    state.bind_session(session)
    assert migration_type_locked(
        state, _StubJobManager({}), status=StepStatus.NOT_STARTED
    ) is False


def test_migration_type_unlocked_after_restore_fresh_default_session() -> None:
    # A restored session with NO progress and the default stack name / no infra is
    # NOT committed work -> the type stays editable (don't lock a fresh session).
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked
    from dsql_migrator.ui.session import SessionConnectionState

    session = SessionConnectionState()  # all steps NOT_STARTED
    state = DataMigrationState()
    state.bind_session(session)
    assert migration_type_locked(
        state, _StubJobManager({}), status=StepStatus.NOT_STARTED
    ) is False


def test_resolve_active_substep_for_type_defaults_prereqs_without_job() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        resolve_active_substep_for_type,
    )

    for mt in MigrationType:
        assert (
            resolve_active_substep_for_type(None, migration_type=mt, has_job=False)
            == "prerequisites"
        )


def test_resolve_active_substep_for_type_defaults_full_load_with_job() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        resolve_active_substep_for_type,
    )

    for mt in (MigrationType.FULL_LOAD_ONLY, MigrationType.FULL_LOAD_AND_CDC):
        assert (
            resolve_active_substep_for_type(None, migration_type=mt, has_job=True)
            == "full_load"
        )


def test_resolve_active_substep_for_type_cdc_only_has_no_full_load() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        resolve_active_substep_for_type,
    )

    # CDC_ONLY has no "full_load" step, so even with a job it stays on prereqs
    # until the user advances.
    assert (
        resolve_active_substep_for_type(
            None, migration_type=MigrationType.CDC_ONLY, has_job=True
        )
        == "prerequisites"
    )


def test_resolve_active_substep_for_type_stays_on_full_load_when_done() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        resolve_active_substep_for_type,
    )

    # Combined mode: Full Load done -> the view STAYS on full_load (no auto-advance)
    # so the operator can review the snapshot stats before clicking "Continue to
    # CDC" (which sets active_substep="cdc" explicitly).
    assert (
        resolve_active_substep_for_type(
            None,
            migration_type=MigrationType.FULL_LOAD_AND_CDC,
            has_job=True,
            full_load_done=True,
        )
        == "full_load"
    )


def test_resolve_active_substep_for_type_no_auto_advance_until_done() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        resolve_active_substep_for_type,
    )

    # Still running: stay on the active Full Load, do not jump to the CDC step.
    assert (
        resolve_active_substep_for_type(
            None,
            migration_type=MigrationType.FULL_LOAD_AND_CDC,
            has_job=True,
            full_load_done=False,
        )
        == "full_load"
    )


def test_resolve_active_substep_for_type_honors_explicit_when_valid() -> None:
    from dsql_migrator.ui.data_migration import (
        MigrationType,
        resolve_active_substep_for_type,
    )

    # Explicit "cdc" is honored in combined mode (e.g. user clicked Back/Forward).
    assert (
        resolve_active_substep_for_type(
            "cdc", migration_type=MigrationType.FULL_LOAD_AND_CDC, has_job=True
        )
        == "cdc"
    )
    # An explicit value not valid for the type is ignored (falls back to default).
    assert (
        resolve_active_substep_for_type(
            "cdc", migration_type=MigrationType.FULL_LOAD_ONLY, has_job=False
        )
        == "prerequisites"
    )


def test_full_load_run_guard_reason_uses_prereq_mode() -> None:
    from dsql_migrator.ui.data_migration import (
        DataMigrationState,
        full_load_run_guard_reason,
    )

    state = DataMigrationState()
    inventory = _inventory()
    passing = PrerequisiteReport.build(
        MigrationMode.CDC,
        [_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS)],
    )
    # A CDC report alone does not unblock the default (FULL_LOAD) guard...
    state.set_prereq_report(MigrationMode.CDC, passing)
    assert full_load_run_guard_reason(state, inventory) is not None
    # ...but it does when the guard is asked about the CDC mode (combined/CDC-only).
    assert (
        full_load_run_guard_reason(
            state, inventory, prereq_mode=MigrationMode.CDC
        )
        is None
    )


def test_state_migration_type_default_and_setter() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()
    assert state.migration_type is MigrationType.FULL_LOAD_ONLY
    state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    assert state.migration_type is MigrationType.FULL_LOAD_AND_CDC


# ---------------------------------------------------------------------------
# _render_cdc_decision -- the Migration Plan's Include-CDC? Yes/No control
# ---------------------------------------------------------------------------


class _TileUi:
    """UI double that captures each tile's click handler (element.on) in order.

    The CDC-decision tiles register their selection via ``tile.on("click", ...)``,
    so the element records its own handler; ``click_handlers`` ends up ordered as
    [No-tile, Yes-tile], matching the render order in _render_cdc_decision.
    """

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.click_handlers: list = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def on(self, event, handler, *_a, **_k):
            if event == "click":
                self._ui.click_handlers.append(handler)
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _record(self, text):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self)

    def label(self, text="", *_a, **_k):
        return self._record(text)

    def icon(self, *_a, **_k):
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def card(self, *_a, **_k):
        return self._El(self)


def _render_cdc_decision_double(state, *, status, locked=None):
    from dsql_migrator.ui.data_migration import _render_cdc_decision

    ui = _TileUi()
    calls = {"refresh": 0}

    def _refresh():
        calls["refresh"] += 1

    _render_cdc_decision(ui, state, status=status, refresh=_refresh, locked=locked)
    return ui, calls


def test_cdc_decision_yes_sets_full_load_and_cdc() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()  # defaults to FULL_LOAD_ONLY
    ui, calls = _render_cdc_decision_double(state, status=StepStatus.NOT_STARTED)
    # Order is [No, Yes]; click "Yes".
    ui.click_handlers[1]()
    assert state.migration_type is MigrationType.FULL_LOAD_AND_CDC
    assert calls["refresh"] == 1


def test_cdc_decision_no_sets_full_load_only() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()
    state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    ui, calls = _render_cdc_decision_double(state, status=StepStatus.NOT_STARTED)
    # Click "No".
    ui.click_handlers[0]()
    assert state.migration_type is MigrationType.FULL_LOAD_ONLY
    assert calls["refresh"] == 1


def test_cdc_decision_yes_keeps_cdc_only_variant() -> None:
    # Yes must not clobber a CDC-only choice into FULL_LOAD_AND_CDC: the CDC-only
    # variant already "includes CDC", so re-selecting Yes is a no-op.
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()
    state.set_migration_type(MigrationType.CDC_ONLY)
    ui, calls = _render_cdc_decision_double(state, status=StepStatus.NOT_STARTED)
    ui.click_handlers[1]()  # Yes
    assert state.migration_type is MigrationType.CDC_ONLY
    assert calls["refresh"] == 0  # unchanged -> no re-render


def test_cdc_decision_locked_ignores_clicks() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()  # FULL_LOAD_ONLY
    ui, calls = _render_cdc_decision_double(
        state, status=StepStatus.NOT_STARTED, locked=True
    )
    ui.click_handlers[1]()  # try to switch to Yes while locked
    assert state.migration_type is MigrationType.FULL_LOAD_ONLY
    assert calls["refresh"] == 0


def test_prereq_phase_tag_combined_marks_common_vs_cdc_only() -> None:
    from dsql_migrator.ui.data_migration import prereq_phase_tag

    # Common checks gate both phases; CDC-only checks (binlog/GTID/MSK) gate CDC.
    common = [
        PrerequisiteCheckId.SOURCE_REACHABLE,
        PrerequisiteCheckId.REPLICATION_GRANTS,
        PrerequisiteCheckId.TABLE_PRIMARY_KEY,
        PrerequisiteCheckId.TARGET_DSQL_REACHABLE,
        PrerequisiteCheckId.TARGET_IAM_AUTH,
        PrerequisiteCheckId.TARGET_SCHEMA_READY,
    ]
    cdc_only = [
        PrerequisiteCheckId.BINLOG_ROW_FORMAT,
        PrerequisiteCheckId.GTID_MODE,
        PrerequisiteCheckId.MSK_AVAILABLE,
        PrerequisiteCheckId.MSK_CONNECT_AVAILABLE,
    ]
    for check_id in common:
        assert prereq_phase_tag(check_id, combined=True) == "Full Load + CDC"
    for check_id in cdc_only:
        assert prereq_phase_tag(check_id, combined=True) == "CDC"


def test_prereq_phase_tag_empty_when_not_combined() -> None:
    from dsql_migrator.ui.data_migration import prereq_phase_tag

    # A single-phase panel names its phase in the title; no per-row tag needed.
    for check_id in PrerequisiteCheckId:
        assert prereq_phase_tag(check_id, combined=False) == ""


def _prereq_result(check_id, status, *, required=True):
    from dsql_migrator.core.models import PrerequisiteResult, PrerequisiteStatus

    return PrerequisiteResult(
        check_id=check_id,
        title=check_id.value,
        status=status,
        required=required,
        detail="",
        remediation="",
    )


def test_prereq_phase_verdicts_cdc_only_failure_does_not_block_full_load() -> None:
    # A binlog (CDC-only) FAIL must block CDC but NOT Full Load -- the headline fix.
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteReport,
        PrerequisiteStatus,
    )
    from dsql_migrator.ui.data_migration import prereq_phase_verdicts

    results = [
        _prereq_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
        _prereq_result(PrerequisiteCheckId.REPLICATION_GRANTS, PrerequisiteStatus.PASS),
        _prereq_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.FAIL),
    ]
    report = PrerequisiteReport.build(MigrationMode.CDC, results)
    verdicts = {v.phase: v for v in prereq_phase_verdicts(report)}
    assert verdicts["Full Load"].can_proceed is True
    assert verdicts["Full Load"].blocking_titles == ()
    assert verdicts["CDC"].can_proceed is False
    assert PrerequisiteCheckId.BINLOG_ROW_FORMAT.value in verdicts["CDC"].blocking_titles


def test_prereq_phase_verdicts_common_failure_blocks_both() -> None:
    # A common (Full Load + CDC) FAIL blocks both phases.
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteReport,
        PrerequisiteStatus,
    )
    from dsql_migrator.ui.data_migration import prereq_phase_verdicts

    results = [
        _prereq_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.FAIL),
        _prereq_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.PASS),
    ]
    report = PrerequisiteReport.build(MigrationMode.CDC, results)
    verdicts = {v.phase: v for v in prereq_phase_verdicts(report)}
    assert verdicts["Full Load"].can_proceed is False
    assert verdicts["CDC"].can_proceed is False


def test_prereq_phase_verdicts_all_pass_both_proceed() -> None:
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteReport,
        PrerequisiteStatus,
    )
    from dsql_migrator.ui.data_migration import prereq_phase_verdicts

    results = [
        _prereq_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
        _prereq_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.PASS),
    ]
    report = PrerequisiteReport.build(MigrationMode.CDC, results)
    verdicts = {v.phase: v for v in prereq_phase_verdicts(report)}
    assert verdicts["Full Load"].can_proceed is True
    assert verdicts["CDC"].can_proceed is True


def test_prereq_phase_verdicts_msk_warn_does_not_block_cdc() -> None:
    # MSK is a non-required advisory (WARN); it must not block the CDC verdict.
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteReport,
        PrerequisiteStatus,
    )
    from dsql_migrator.ui.data_migration import prereq_phase_verdicts

    results = [
        _prereq_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS),
        _prereq_result(
            PrerequisiteCheckId.MSK_AVAILABLE, PrerequisiteStatus.WARN, required=False
        ),
    ]
    report = PrerequisiteReport.build(MigrationMode.CDC, results)
    verdicts = {v.phase: v for v in prereq_phase_verdicts(report)}
    assert verdicts["CDC"].can_proceed is True


def test_format_selected_workloads_zero_one_many() -> None:
    from dsql_migrator.ui.data_migration import format_selected_workloads

    assert format_selected_workloads([]) == "No tables selected"
    assert format_selected_workloads(["orders"]) == "1 table selected for Full Load"
    assert (
        format_selected_workloads(["orders", "customers"])
        == "2 tables selected for Full Load"
    )


# ---------------------------------------------------------------------------
# Full Load per-table progress, completeness, and retry-only-failed
# ---------------------------------------------------------------------------


def _full_load_job(chunks, *, counts) -> MigrationJob:
    from dsql_migrator.core.models import ChunkState

    return MigrationJob(
        job_id="job-fl",
        chunks=[ChunkState(**c) for c in chunks],
        watermark=Watermark(
            snapshot_timestamp=datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
            table_row_counts=counts,
        ),
    )


def test_advance_chunk_rows_accumulates_only_while_in_progress() -> None:
    from dsql_migrator.ui.data_migration import _advance_chunk_rows

    job = _full_load_job(
        [
            {"chunk_id": "a", "status": "IN_PROGRESS", "rows_loaded": 100,
             "attempts": 1},
            {"chunk_id": "b", "status": "PENDING", "attempts": 0},
            {"chunk_id": "c", "status": "DONE", "rows_loaded": 50, "attempts": 1},
        ],
        counts={"a": 1000},
    )

    _advance_chunk_rows(job, "a", 25)  # in progress -> live count advances
    _advance_chunk_rows(job, "b", 25)  # not started -> ignored
    _advance_chunk_rows(job, "c", 25)  # terminal DONE -> ignored
    _advance_chunk_rows(job, "missing", 25)  # unknown chunk -> no-op

    by_id = {chunk.chunk_id: chunk for chunk in job.chunks}
    assert by_id["a"].rows_loaded == 125  # only the in-progress chunk accumulates
    assert by_id["b"].rows_loaded == 0
    assert by_id["c"].rows_loaded == 50


def test_advance_chunk_rows_accumulates_skipped_for_live_progress() -> None:
    # A re-load mostly SKIPS already-present rows: the skipped delta must advance
    # the live count too (rows-present = loaded + skipped), so the table does not
    # look stuck at zero while it streams past rows a prior run already loaded.
    from dsql_migrator.ui.data_migration import _advance_chunk_rows

    job = _full_load_job(
        [{"chunk_id": "a", "status": "IN_PROGRESS", "rows_loaded": 0, "attempts": 2}],
        counts={"a": 1000},
    )

    _advance_chunk_rows(job, "a", 0, 300)  # 300 skipped (already present), 0 new
    _advance_chunk_rows(job, "a", 40, 60)  # then 40 new + 60 skipped

    chunk = job.chunks[0]
    assert chunk.rows_loaded == 40    # newly inserted rows
    assert chunk.rows_skipped == 360  # already-present rows, advanced live


def test_fail_unfinished_chunks_marks_non_done_failed_for_retry() -> None:
    from dsql_migrator.ui.data_migration import _fail_unfinished_chunks

    job = _full_load_job(
        [
            {"chunk_id": "a", "status": "DONE", "rows_loaded": 10, "attempts": 1},
            {"chunk_id": "b", "status": "PENDING", "attempts": 0},
            {"chunk_id": "c", "status": "IN_PROGRESS", "rows_loaded": 3,
             "attempts": 1},
        ],
        counts={"a": 10},
    )

    _fail_unfinished_chunks(job)

    # DONE tables are carried forward; everything unfinished becomes FAILED so the
    # "Retry failed tables" path resumes exactly the remaining work.
    by_id = {chunk.chunk_id: chunk.status for chunk in job.chunks}
    assert by_id == {"a": "DONE", "b": "FAILED", "c": "FAILED"}


def test_run_full_load_stopped_table_is_failed_without_error_record() -> None:
    from dsql_migrator.ui.data_migration import _FullLoadStopped

    class _StoppingMigrator:
        def capture_watermark(self, tables):  # noqa: ANN001, ANN202
            return _watermark()

        def migrate_table(self, table, *, on_rows=None, should_cancel=None):  # noqa: ANN001, ANN202
            if table.name == "orders":
                raise _FullLoadStopped("orders")
            return 3

    job, job_id, error_log = _run_full_load_job(_StoppingMigrator(), _tables())

    by_name = {chunk.chunk_id: chunk.status for chunk in job.chunks}
    assert by_name["orders"] == "FAILED"  # stopped mid-table -> retryable
    assert by_name["customers"] == "DONE"
    # A user stop is not a data error, so nothing is written to the error log.
    assert error_log.summary(job_id).total_errors == 0


def test_full_load_progress_caption_reflects_phase_and_current_table() -> None:
    from dsql_migrator.ui.data_migration import full_load_progress_caption
    from dsql_migrator.core.models import ChunkState

    # No job yet -> starting.
    assert full_load_progress_caption(None) == "Starting Full Load…"

    # Job seeded but no watermark yet -> capture phase.
    capturing = MigrationJob(
        job_id="j",
        chunks=[ChunkState(chunk_id="orders", status="PENDING")],
    )
    assert "watermark" in full_load_progress_caption(capturing).lower()

    # Watermark captured + a table in progress -> names the current table + N/total.
    running = _full_load_job(
        [
            {"chunk_id": "orders", "status": "DONE", "rows_loaded": 5, "attempts": 1},
            {"chunk_id": "items", "status": "IN_PROGRESS", "attempts": 1},
            {"chunk_id": "customers", "status": "PENDING"},
        ],
        counts={"orders": 5, "items": 9, "customers": 2},
    )
    caption = full_load_progress_caption(running)
    assert "items" in caption
    assert "1/3" in caption


def test_build_full_load_table_rows_pairs_loaded_with_source_count() -> None:
    from dsql_migrator.ui.data_migration import build_full_load_table_rows

    job = _full_load_job(
        [
            {"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1},
            {"chunk_id": "items", "status": "DONE", "rows_loaded": 40, "attempts": 1},
            {"chunk_id": "customers", "status": "FAILED", "attempts": 2},
        ],
        counts={"orders": 100, "items": 50, "customers": 10},
    )
    rows = build_full_load_table_rows(
        job,
        ErrorLogSummary(total_errors=1, errors_by_table={"customers": 1}),
        {"customers": "OperationalError: (1046, 'No database selected')"},
    )
    by = {r.table: r for r in rows}

    # The failure cause is carried on the failed row for inline display.
    assert by["customers"].error_message == (
        "OperationalError: (1046, 'No database selected')"
    )
    assert by["orders"].error_message is None

    assert by["orders"].progress_pct == 100.0
    assert by["orders"].complete is True
    assert by["items"].progress_pct == 80.0
    assert by["items"].complete is False  # 40 of 50 loaded -> incomplete
    assert by["customers"].complete is None  # not DONE -> unknown
    assert by["customers"].errors == 1
    assert by["customers"].attempts == 2


def test_build_migration_table_status_combines_full_load_and_live_counts() -> None:
    from dsql_migrator.ui.data_migration import build_migration_table_status

    job = _full_load_job(
        [
            {"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1},
            {"chunk_id": "items", "status": "DONE", "rows_loaded": 40, "attempts": 1},
        ],
        counts={"orders": 100, "items": 50},
    )
    rows = build_migration_table_status(
        ["orders", "items"],
        full_load_job=job,
        # Live counts: orders caught up; items still 8 behind (CDC replicating).
        source_counts={"orders": 100, "items": 50},
        target_counts={"orders": 100, "items": 42},
        # Exact-count path (source_is_estimate=False marks them as not estimates).
        source_is_estimate=False,
    )
    by = {r.table: r for r in rows}

    assert by["orders"].full_load_state == "DONE"
    assert by["orders"].full_load_rows == 100
    assert by["orders"].source_rows == 100 and by["orders"].target_rows == 100
    assert by["orders"].delta == 0 and by["orders"].in_sync is True
    assert by["orders"].source_estimate is False  # exact count, not the estimate

    assert by["items"].delta == 8  # 50 source - 42 target
    assert by["items"].in_sync is False


def test_migration_table_status_marks_source_estimate_by_default() -> None:
    # The default fetch supplies an information_schema estimate (scan-free), so a
    # live source figure is flagged as an estimate unless source_is_estimate=False.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"], source_counts={"orders": 1000}, target_counts={"orders": 1000}
    )
    assert row.source_rows == 1000
    assert row.source_estimate is True  # default = estimate


def test_build_migration_table_status_falls_back_to_snapshot_estimate() -> None:
    # With no live source count, source_rows uses the watermark snapshot estimate
    # and is flagged so the UI can mark it "(est.)".
    from dsql_migrator.ui.data_migration import build_migration_table_status

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1}],
        counts={"orders": 100},
    )
    (row,) = build_migration_table_status(
        ["orders"], full_load_job=job, target_counts={"orders": 90}
    )
    assert row.source_rows == 100  # from the watermark estimate
    assert row.source_estimate is True
    assert row.target_rows == 90
    assert row.delta == 10


def test_build_migration_table_status_unknown_target_is_none_not_zero() -> None:
    # A target table not yet created/counted is None (unknown), distinct from empty.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"], target_counts={"orders": None}, source_counts={"orders": 5}
    )
    assert row.full_load_state == ""  # no job -> not run
    assert row.target_rows is None
    assert row.delta is None and row.in_sync is None


def test_migration_table_status_separates_full_load_from_cdc_net() -> None:
    # CDC applied (net) = target - Full Load rows: the rows the stream changed
    # after the snapshot, shown separately from the one-shot Full Load count.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1}],
        counts={"orders": 100},
    )
    # Full Load loaded 100; target now 137 and source 137 -> CDC net +37, consistent.
    (row,) = build_migration_table_status(
        ["orders"], full_load_job=job,
        source_counts={"orders": 137}, target_counts={"orders": 137},
    )
    assert row.full_load_rows == 100
    assert row.cdc_applied_net == 37  # 137 target - 100 Full Load
    assert row.consistency == "consistent"

    # A net-negative case (stream deleted rows): target 90 < Full Load 100.
    (neg,) = build_migration_table_status(
        ["orders"], full_load_job=job,
        source_counts={"orders": 90}, target_counts={"orders": 90},
    )
    assert neg.cdc_applied_net == -10
    assert neg.consistency == "consistent"


def test_migration_table_status_quarantined_means_data_missing() -> None:
    # DLQ count > 0 -> consistency is "quarantined" even if counts happen to match,
    # because quarantined change events never reached the target.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"],
        source_counts={"orders": 100}, target_counts={"orders": 100},
        dlq_counts={"orders": 3},
    )
    assert row.dlq_count == 3
    assert row.consistency == "quarantined"  # missing data wins over a count match


def test_migration_table_status_consistency_verdicts() -> None:
    from dsql_migrator.ui.data_migration import build_migration_table_status

    def verdict(src, tgt, dlq=None):
        (r,) = build_migration_table_status(
            ["t"], source_counts={"t": src}, target_counts={"t": tgt},
            dlq_counts={"t": dlq} if dlq else None,
        )
        return r.consistency

    assert verdict(100, 100) == "consistent"
    assert verdict(100, 80) == "behind"      # target trails source (replicating)
    assert verdict(80, 100) == "ahead"       # unusual: target exceeds source
    assert verdict(100, None) == "unknown"   # not yet counted
    assert verdict(100, 100, dlq=5) == "quarantined"


def test_cdc_applied_net_none_until_both_known() -> None:
    from dsql_migrator.ui.data_migration import build_migration_table_status

    # No Full Load job -> full_load_rows None -> cdc_applied_net None.
    (row,) = build_migration_table_status(["t"], target_counts={"t": 50})
    assert row.cdc_applied_net is None


def test_stream_lag_distinguishes_behind_from_mid_stream_gap() -> None:
    # The real-world case observed live: target has FEWER rows than source, but the
    # newest row (high-water PK) HAS landed -> not "behind", it's a mid-stream gap.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (gap,) = build_migration_table_status(
        ["orders"],
        source_counts={"orders": 12759}, target_counts={"orders": 8780},
        source_max_pk={"orders": 12759}, target_max_pk={"orders": 12759},
    )
    assert gap.pk_gap == 0
    assert gap.stream_caught_up is True
    assert gap.consistency == "gap"  # newest landed, but 3979 rows missing -> gap

    # Genuinely behind: newest source rows have NOT reached the target yet.
    (behind,) = build_migration_table_status(
        ["orders"],
        source_counts={"orders": 12759}, target_counts={"orders": 12000},
        source_max_pk={"orders": 12759}, target_max_pk={"orders": 12000},
    )
    assert behind.pk_gap == 759
    assert behind.stream_caught_up is False
    assert behind.consistency == "behind"  # stream lagging, expected during catch-up


def test_stream_caught_up_unknown_without_pk_marks() -> None:
    # Without high-water PK marks, stream_caught_up is None and a short target
    # falls back to "behind" (the conservative lag reading).
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"], source_counts={"orders": 100}, target_counts={"orders": 80}
    )
    assert row.pk_gap is None and row.stream_caught_up is None
    assert row.consistency == "behind"


def test_stream_caught_up_consistent_when_counts_match() -> None:
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"],
        source_counts={"orders": 100}, target_counts={"orders": 100},
        source_max_pk={"orders": 100}, target_max_pk={"orders": 100},
    )
    assert row.consistency == "consistent"
    assert row.stream_caught_up is True


def test_full_load_table_row_progress_unknown_while_in_progress() -> None:
    from dsql_migrator.ui.data_migration import build_full_load_table_rows

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "IN_PROGRESS", "attempts": 1}],
        counts={"orders": 100},
    )
    (row,) = build_full_load_table_rows(job)
    # No source count was loaded yet and the table is mid-load -> percent unknown.
    assert row.progress_pct == 0.0  # 0 loaded of 100 known
    assert row.complete is None


def test_format_duration_compact_units() -> None:
    from dsql_migrator.ui.data_migration import format_duration

    assert format_duration(0) == "0s"
    assert format_duration(45) == "45s"
    assert format_duration(125) == "2m 5s"
    assert format_duration(3725) == "1h 2m"


def test_format_eta_hint_buckets() -> None:
    from dsql_migrator.ui.data_migration import _format_eta_hint

    # Sub-10s stages are not worth a hint.
    assert _format_eta_hint(0) == ""
    assert _format_eta_hint(5) == ""
    # Seconds below a minute show "~Ns"; a minute or more rounds to "~N min".
    assert _format_eta_hint(45) == "~45s"
    assert _format_eta_hint(60) == "~1 min"
    assert _format_eta_hint(180) == "~3 min"


def test_ascii_log_replaces_fancy_punctuation() -> None:
    # The deploy log renders in a monospace block whose font may lack glyphs for
    # em-dash / ellipsis; they must be down-converted to ASCII to avoid tofu boxes.
    from dsql_migrator.ui.data_migration import _ascii_log

    assert _ascii_log("Discovering cdc-stack 'x'…") == "Discovering cdc-stack 'x'..."
    assert _ascii_log("stack UPDATE_IN_PROGRESS — User Initiated") == (
        "stack UPDATE_IN_PROGRESS - User Initiated"
    )
    # Plain ASCII passes through unchanged (and is not copied needlessly).
    assert _ascii_log("Fetched MSK bootstrap brokers.") == (
        "Fetched MSK bootstrap brokers."
    )


def test_cdc_stage_eta_keys_cover_start_stages() -> None:
    # Every Start-CDC stage id should have an ETA entry so no running stage is
    # missing its estimate (the label maps and ETA maps must stay in sync).
    from dsql_migrator.core.cdc_deployer import CDC_START_STAGES
    from dsql_migrator.ui.data_migration import _CDC_STAGE_ETA_SECONDS

    start_etas = _CDC_STAGE_ETA_SECONDS["start"]
    for chunk_id, _label in CDC_START_STAGES:
        assert chunk_id in start_etas, f"missing ETA for stage {chunk_id}"


def test_format_table_timing_eta_elapsed_and_total() -> None:
    from datetime import datetime, timedelta, timezone

    from dsql_migrator.ui.data_migration import (
        FullLoadTableRow,
        format_table_timing,
    )

    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    now = start + timedelta(seconds=30)

    # Not started yet -> placeholder.
    pending = FullLoadTableRow(
        table="t", state="PENDING", rows_loaded=0, expected_rows=100,
        attempts=0, errors=0,
    )
    assert format_table_timing(pending, now) == "—"

    # In progress, 25% done in 30s -> ~90s remaining.
    running = FullLoadTableRow(
        table="t", state="IN_PROGRESS", rows_loaded=25, expected_rows=100,
        attempts=1, errors=0, started_at=start,
    )
    assert format_table_timing(running, now) == "~1m 30s left"

    # In progress but no source count yet -> show elapsed instead of an ETA.
    running_unknown = FullLoadTableRow(
        table="t", state="IN_PROGRESS", rows_loaded=0, expected_rows=None,
        attempts=1, errors=0, started_at=start,
    )
    assert format_table_timing(running_unknown, now) == "30s elapsed"

    # Finished -> total elapsed wall-clock time.
    done = FullLoadTableRow(
        table="t", state="DONE", rows_loaded=100, expected_rows=100,
        attempts=1, errors=0, started_at=start,
        finished_at=start + timedelta(seconds=125),
    )
    assert format_table_timing(done, now) == "2m 5s"

    # Restored/interrupted (terminal but no finish time) -> unknown.
    interrupted = FullLoadTableRow(
        table="t", state="FAILED", rows_loaded=10, expected_rows=100,
        attempts=1, errors=0, started_at=start, finished_at=None,
    )
    assert format_table_timing(interrupted, now) == "—"


def test_format_rows_on_target_cell_plain_when_nothing_skipped() -> None:
    from dsql_migrator.ui.data_migration import (
        FullLoadTableRow,
        _format_rows_on_target_cell,
    )

    row = FullLoadTableRow(
        table="t", state="DONE", rows_loaded=1234567, expected_rows=1234567,
        attempts=1, errors=0,
    )
    # No skips: just the loaded count, with thousands separators for readability.
    assert _format_rows_on_target_cell(row) == "1,234,567"


def test_format_rows_on_target_cell_breaks_down_new_vs_existing() -> None:
    from dsql_migrator.ui.data_migration import (
        FullLoadTableRow,
        _format_rows_on_target_cell,
    )

    # A re-load that inserted 455,319 new rows over 611,991 that already existed.
    row = FullLoadTableRow(
        table="t", state="DONE", rows_loaded=455319, expected_rows=1067310,
        attempts=2, errors=0, rows_skipped=611991,
    )
    cell = _format_rows_on_target_cell(row)
    # Total present first, then a plain-language new/existing breakdown -- no bare
    # "(+N already present)" jargon. Thousands-separated.
    assert cell == "1,067,310  ·  455,319 new + 611,991 already there"


def test_failed_table_names_lists_only_failed_chunks() -> None:
    from dsql_migrator.ui.data_migration import failed_table_names

    job = _full_load_job(
        [
            {"chunk_id": "orders", "status": "DONE", "rows_loaded": 1, "attempts": 1},
            {"chunk_id": "customers", "status": "FAILED", "attempts": 1},
            {"chunk_id": "items", "status": "FAILED", "attempts": 1},
        ],
        counts={},
    )
    assert failed_table_names(job) == ["customers", "items"]


def test_unsettled_table_names_includes_pending_and_failed() -> None:
    # A fatal/aborted run can leave tables PENDING (never attempted) rather than
    # FAILED. Recovery must resume those too -- unsettled = every non-DONE chunk --
    # or a crash before the big tables loaded would strand them with no scoped retry.
    from dsql_migrator.ui.data_migration import (
        failed_table_names,
        unsettled_table_names,
    )

    job = _full_load_job(
        [
            {"chunk_id": "categories", "status": "DONE", "rows_loaded": 1, "attempts": 1},
            {"chunk_id": "orders", "status": "PENDING", "attempts": 3},
            {"chunk_id": "payments", "status": "PENDING", "attempts": 3},
            {"chunk_id": "reviews", "status": "FAILED", "attempts": 1},
        ],
        counts={},
    )
    # failed_table_names sees only the FAILED chunk (the pre-fix recovery gap)...
    assert failed_table_names(job) == ["reviews"]
    # ...unsettled_table_names resumes every unfinished table, PENDING included.
    assert unsettled_table_names(job) == ["orders", "payments", "reviews"]
    # An all-DONE job has nothing unsettled.
    done = _full_load_job(
        [{"chunk_id": "t", "status": "DONE", "rows_loaded": 1, "attempts": 1}],
        counts={},
    )
    assert unsettled_table_names(done) == []


def test_full_load_completeness_all_complete() -> None:
    from dsql_migrator.ui.data_migration import (
        build_full_load_table_rows,
        full_load_completeness,
    )

    job = _full_load_job(
        [
            {"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1},
            {"chunk_id": "items", "status": "DONE", "rows_loaded": 50, "attempts": 1},
        ],
        counts={"orders": 100, "items": 50},
    )
    completeness = full_load_completeness(build_full_load_table_rows(job))
    assert completeness.all_complete is True
    assert completeness.complete == 2
    assert completeness.mismatched == []


def test_full_load_completeness_flags_mismatch_and_failure() -> None:
    from dsql_migrator.ui.data_migration import (
        build_full_load_table_rows,
        full_load_completeness,
    )

    job = _full_load_job(
        [
            {"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1},
            {"chunk_id": "items", "status": "DONE", "rows_loaded": 40, "attempts": 1},
            {"chunk_id": "customers", "status": "FAILED", "attempts": 1},
        ],
        counts={"orders": 100, "items": 50, "customers": 10},
    )
    completeness = full_load_completeness(build_full_load_table_rows(job))
    assert completeness.all_complete is False
    assert completeness.failed == 1
    assert completeness.mismatched == ["items"]


def test_full_load_completeness_skipped_rows_count_as_present() -> None:
    """A DONE table with 0 newly-loaded rows but all rows already present (skipped
    by ON CONFLICT) is complete, not a false row-count mismatch."""
    from dsql_migrator.ui.data_migration import (
        build_full_load_table_rows,
        full_load_completeness,
    )

    job = _full_load_job(
        [
            {
                "chunk_id": "countries",
                "status": "DONE",
                "rows_loaded": 0,
                "rows_skipped": 50,
                "attempts": 1,
            },
        ],
        counts={"countries": 50},
    )
    rows = build_full_load_table_rows(job)
    assert rows[0].rows_loaded == 0
    assert rows[0].rows_present == 50
    assert rows[0].complete is True
    completeness = full_load_completeness(rows)
    assert completeness.all_complete is True
    assert completeness.mismatched == []


def test_run_full_load_retry_keeps_succeeded_and_reruns_failed() -> None:
    from dsql_migrator.core.models import ChunkState
    from dsql_migrator.ui.data_migration import failed_table_names, run_full_load_retry

    prior_chunks = [
        ChunkState(chunk_id="orders", status="DONE", rows_loaded=10, attempts=1),
        ChunkState(chunk_id="customers", status="FAILED", rows_loaded=0, attempts=1),
    ]
    # On retry, "customers" now loads successfully.
    migrator = _FakeMigrator(rows_by_table={"customers": 3})
    error_log = ErrorLogStore()
    manager = JobManager()
    retry_tables = [t for t in _tables() if t.name == "customers"]

    job_id = manager.submit(
        lambda h: run_full_load_retry(
            h,
            prior_chunks,
            retry_tables,
            migrator=migrator,
            error_log=error_log,
            watermark=_watermark(),
        )
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)
    by = {c.chunk_id: c for c in job.chunks}

    # Succeeded table carried forward untouched; failed table re-run and migrated.
    assert by["orders"].status == "DONE" and by["orders"].rows_loaded == 10
    assert by["customers"].status == "DONE" and by["customers"].rows_loaded == 3
    assert by["customers"].attempts == 2  # prior attempt + this retry
    assert failed_table_names(job) == []
    assert job.watermark is not None  # carried over, not recaptured
    # Only the failed table was migrated on retry.
    assert migrator.migrated == ["customers"]


def test_run_full_load_retry_isolates_still_failing_table() -> None:
    from dsql_migrator.core.models import ChunkState
    from dsql_migrator.ui.data_migration import failed_table_names, run_full_load_retry

    prior_chunks = [
        ChunkState(chunk_id="orders", status="DONE", rows_loaded=10, attempts=1),
        ChunkState(chunk_id="customers", status="FAILED", rows_loaded=0, attempts=1),
    ]
    migrator = _FakeMigrator(fail_tables=("customers",))  # still fails
    error_log = ErrorLogStore()
    manager = JobManager()
    retry_tables = [t for t in _tables() if t.name == "customers"]

    job_id = manager.submit(
        lambda h: run_full_load_retry(
            h, prior_chunks, retry_tables, migrator=migrator, error_log=error_log
        )
    )
    assert manager.wait(job_id, timeout=5.0)
    job = manager.get_status(job_id)
    by = {c.chunk_id: c for c in job.chunks}

    assert by["orders"].status == "DONE"  # untouched
    assert by["customers"].status == "FAILED"
    assert failed_table_names(job) == ["customers"]
    # The failure was recorded to the single error log for the retry job.
    assert error_log.summary(job_id).total_errors == 1


def test_summarize_table_states_counts_each_state_with_fixed_keys() -> None:
    from dsql_migrator.ui.data_migration import (
        build_full_load_table_rows,
        summarize_table_states,
    )

    job = _full_load_job(
        [
            {"chunk_id": "a", "status": "DONE", "rows_loaded": 10, "attempts": 1},
            {"chunk_id": "b", "status": "DONE", "rows_loaded": 5, "attempts": 1},
            {"chunk_id": "c", "status": "FAILED", "attempts": 1},
            {"chunk_id": "d", "status": "IN_PROGRESS", "attempts": 1},
        ],
        counts={"a": 10, "b": 5},
    )
    states = summarize_table_states(build_full_load_table_rows(job))
    # Every state key is present (0 when absent) for consistent chips.
    assert states == {"DONE": 2, "IN_PROGRESS": 1, "FAILED": 1, "PENDING": 0}


# ---------------------------------------------------------------------------
# _filter_mine — scope region connectors to THIS cdc-stack's two connectors
# ---------------------------------------------------------------------------


def test_filter_mine_matches_only_my_stack() -> None:
    from dsql_migrator.ui.data_migration import _filter_mine

    mine = [
        {"connectorName": "mysql-dsql-cdc-stack-debezium-source"},
        {"connectorName": "mysql-dsql-cdc-stack-dsql-sink"},
    ]
    assert _filter_mine(mine, "mysql-dsql-cdc-stack") == [
        "mysql-dsql-cdc-stack-debezium-source",
        "mysql-dsql-cdc-stack-dsql-sink",
    ]


def test_filter_mine_ignores_unrelated_connectors() -> None:
    from dsql_migrator.ui.data_migration import _filter_mine

    # A different stack's connectors (e.g. a throwaway spike) are NOT mine.
    spike = [
        {"connectorName": "mysql-dsql-cdc-spike-debezium-source"},
        {"connectorName": "mysql-dsql-cdc-spike-dsql-sink-v6"},
    ]
    assert _filter_mine(spike, "mysql-dsql-cdc-stack") == []


def test_filter_mine_mixed_returns_only_mine() -> None:
    from dsql_migrator.ui.data_migration import _filter_mine

    raw = [
        {"connectorName": "mysql-dsql-cdc-spike-debezium-source"},
        {"connectorName": "mysql-dsql-cdc-stack-dsql-sink"},
        {"connectorName": "some-other-thing"},
    ]
    assert _filter_mine(raw, "mysql-dsql-cdc-stack") == ["mysql-dsql-cdc-stack-dsql-sink"]


def test_filter_mine_empty_region() -> None:
    from dsql_migrator.ui.data_migration import _filter_mine

    assert _filter_mine([], "mysql-dsql-cdc-stack") == []


def test_filter_mine_partial_source_only() -> None:
    from dsql_migrator.ui.data_migration import _filter_mine

    # Valid transitional state: source deployed, sink not yet.
    raw = [{"connectorName": "mysql-dsql-cdc-stack-debezium-source"}]
    assert _filter_mine(raw, "mysql-dsql-cdc-stack") == ["mysql-dsql-cdc-stack-debezium-source"]


# --- classify_cdc_card_phase: full vs partial vs probed ----------------------


def test_classify_cdc_card_phase_both_connectors_is_running() -> None:
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    phase = classify_cdc_card_phase(
        ["mysql-dsql-cdc-stack-debezium-source", "mysql-dsql-cdc-stack-dsql-sink"],
        "mysql-dsql-cdc-stack",
        None,
    )
    assert phase == "running"


def test_classify_cdc_card_phase_source_only_is_partial() -> None:
    # The post-rollback state: the sink failed to create (e.g. MSK partition quota
    # exhausted) and only the source survived. That is NOT streaming -- changes are
    # captured to Kafka but never written to DSQL -- so it must classify as partial,
    # not "running" (which would wrongly show "Streaming" + a plain "Stop CDC").
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    phase = classify_cdc_card_phase(
        ["mysql-dsql-cdc-stack-debezium-source"], "mysql-dsql-cdc-stack", None
    )
    assert phase == "partial"


def test_classify_cdc_card_phase_sink_only_is_partial() -> None:
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    phase = classify_cdc_card_phase(
        ["mysql-dsql-cdc-stack-dsql-sink"], "mysql-dsql-cdc-stack", None
    )
    assert phase == "partial"


def test_classify_cdc_card_phase_no_connectors_uses_probed_phase() -> None:
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    # With no detected connectors, the probed CloudFormation phase wins.
    assert classify_cdc_card_phase([], "mysql-dsql-cdc-stack", "infra") == "infra"
    assert classify_cdc_card_phase([], "mysql-dsql-cdc-stack", "unstable") == "unstable"
    assert classify_cdc_card_phase(None, "mysql-dsql-cdc-stack", None) is None


def test_classify_cdc_card_phase_provisioning_when_not_all_running() -> None:
    # Both connectors exist but the sink is still CREATING on MSK: must classify as
    # "provisioning" (not "running"), so the card does not mislabel it "Streaming".
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    both = ["mysql-dsql-cdc-stack-debezium-source", "mysql-dsql-cdc-stack-dsql-sink"]
    phase = classify_cdc_card_phase(
        both,
        "mysql-dsql-cdc-stack",
        None,
        running_names=["mysql-dsql-cdc-stack-debezium-source"],  # only source is RUNNING
    )
    assert phase == "provisioning"


def test_classify_cdc_card_phase_running_when_all_running() -> None:
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    both = ["mysql-dsql-cdc-stack-debezium-source", "mysql-dsql-cdc-stack-dsql-sink"]
    phase = classify_cdc_card_phase(
        both, "mysql-dsql-cdc-stack", None, running_names=both
    )
    assert phase == "running"


def test_cdc_deploy_connection_blocker_cases() -> None:
    # The deploy dialog must tell the user up front when source/target connections
    # are not ready (e.g. after a restart) and disable Deploy — instead of failing
    # mid-submit with "test the source connection first".
    from dsql_migrator.ui.data_migration import cdc_deploy_connection_blocker

    class _Sess:
        def __init__(self, *, src, tgt, pw, secret_id=None):
            self._src, self._tgt, self.source_password = src, tgt, pw
            self.source_secret_id = secret_id

        def has_source(self):
            return self._src

        def has_target(self):
            return self._tgt

    # No session at all.
    assert cdc_deploy_connection_blocker(None) is not None

    # Target missing -> blocked (mentions target).
    msg = cdc_deploy_connection_blocker(_Sess(src=True, tgt=False, pw=object()))
    assert msg is not None and "target" in msg.lower()

    # Source missing -> blocked (mentions source).
    msg = cdc_deploy_connection_blocker(_Sess(src=False, tgt=True, pw=object()))
    assert msg is not None and "source" in msg.lower()

    # Source connected but in-memory password gone (post-restart) -> blocked.
    msg = cdc_deploy_connection_blocker(_Sess(src=True, tgt=True, pw=None))
    assert msg is not None and "password" in msg.lower()

    # Password-auth source fully ready -> not blocked.
    assert cdc_deploy_connection_blocker(
        _Sess(src=True, tgt=True, pw=object())
    ) is None

    # Secrets-Manager-auth source needs no in-memory password -> only target matters.
    assert cdc_deploy_connection_blocker(
        _Sess(src=True, tgt=True, pw=None, secret_id="arn:...:secret:x")
    ) is None


def test_cdc_live_running_names_excludes_task_failed_connector() -> None:
    # The reported bug: source connectorState=RUNNING but its TASK died -> the live
    # status view reports it FAILED. The lifecycle card must NOT count it as running
    # (else it shows "Streaming" while Pipeline health shows the source FAILED).
    from dsql_migrator.ui.data_migration import (
        cdc_live_running_names,
        classify_cdc_card_phase,
    )

    src, sink = ("mysql-dsql-cdc-stack-debezium-source", "mysql-dsql-cdc-stack-dsql-sink")
    discovery_running = [src, sink]  # both RUNNING at connector-resource level
    live_states = {src: "FAILED", sink: "RUNNING"}  # source task dead

    running = cdc_live_running_names(discovery_running, live_states)
    assert running == [sink]  # source excluded -> only sink is genuinely streaming

    # Feeding that into the phase classifier yields "provisioning" (both present,
    # not all running) -- so the badge is NOT "Streaming".
    phase = classify_cdc_card_phase(
        [src, sink], "mysql-dsql-cdc-stack", None, running_names=running
    )
    assert phase != "running"


def test_classify_cdc_card_phase_failed_connector_is_partial_not_provisioning() -> None:
    # A connector whose TASK died (live status FAILED) needs a RESTART, not more
    # waiting -- it must classify as "partial" (recovery), NEVER "provisioning"
    # (which tells the user to wait). FAILED takes precedence over the
    # running/provisioning split.
    from dsql_migrator.ui.data_migration import classify_cdc_card_phase

    both = ["mysql-dsql-cdc-stack-debezium-source", "mysql-dsql-cdc-stack-dsql-sink"]
    src = "mysql-dsql-cdc-stack-debezium-source"
    # source FAILED, sink running -> partial (not provisioning).
    phase = classify_cdc_card_phase(
        both, "mysql-dsql-cdc-stack", None,
        running_names=["mysql-dsql-cdc-stack-dsql-sink"],
        failed_names=[src],
    )
    assert phase == "partial"


def test_cdc_live_running_names_falls_back_when_no_live_states() -> None:
    from dsql_migrator.ui.data_migration import cdc_live_running_names

    both = ["mysql-dsql-cdc-stack-debezium-source", "mysql-dsql-cdc-stack-dsql-sink"]
    # No poll yet (None / empty) -> use the discovery set as-is.
    assert cdc_live_running_names(both, None) == both
    assert cdc_live_running_names(both, {}) == both
    # All live-RUNNING -> unchanged.
    assert cdc_live_running_names(both, {n: "RUNNING" for n in both}) == both


def test_cdc_deploy_card_superseded_hides_stale_interrupted_stages() -> None:
    # The reported bug: a Start CDC job reconciled to FAILED by an app restart
    # showed stale stages (Sink connector deploying = red FAILED, later steps
    # PENDING) even though the connectors actually reached RUNNING. When live
    # discovery has a definitive phase, the stale stage card must be suppressed.
    from dsql_migrator.core.job_manager import INTERRUPTED_BY_RESTART_MESSAGE
    from dsql_migrator.ui.data_migration import cdc_deploy_card_superseded

    interrupt = INTERRUPTED_BY_RESTART_MESSAGE
    # Interrupted-by-restart + live discovery says running/provisioning/partial
    # -> suppress the stale card (discovery is the truth).
    assert cdc_deploy_card_superseded(True, False, interrupt, "running") is True
    assert cdc_deploy_card_superseded(True, False, interrupt, "provisioning") is True
    assert cdc_deploy_card_superseded(True, False, interrupt, "partial") is True

    # Still in-flight -> keep the live stage card (it's the real progress).
    assert cdc_deploy_card_superseded(True, True, interrupt, "running") is False
    # A genuine in-job failure (not a restart) -> keep the failure card.
    assert cdc_deploy_card_superseded(True, False, "OC000: boom", "running") is False
    # No discovery verdict yet (e.g. infra/None) -> keep the card.
    assert cdc_deploy_card_superseded(True, False, interrupt, "infra") is False
    assert cdc_deploy_card_superseded(True, False, interrupt, None) is False
    # No deploy job at all.
    assert cdc_deploy_card_superseded(False, False, None, "running") is False


def test_running_mine_filters_to_running_connectors_only() -> None:
    from dsql_migrator.ui.data_migration._status import _running_mine

    raw = [
        {"connectorName": "mysql-dsql-cdc-stack-debezium-source", "connectorState": "RUNNING"},
        {"connectorName": "mysql-dsql-cdc-stack-dsql-sink", "connectorState": "CREATING"},
        {"connectorName": "unrelated-connector", "connectorState": "RUNNING"},
    ]
    # Only MY connectors that are RUNNING, in data-flow order (source before sink).
    assert _running_mine(raw, "mysql-dsql-cdc-stack") == ["mysql-dsql-cdc-stack-debezium-source"]


# --- _diagnose_for_dialog: the deploy-dialog network preview line ------------


def test_diagnose_for_dialog_manual_override_skips_diagnosis() -> None:
    from dsql_migrator.ui.data_migration import _diagnose_for_dialog

    class _MS:
        def cdc_infra_inputs(self):
            return {"vpc_id": "vpc-1", "connector_subnet_ids": "subnet-a,subnet-b"}

    class _Sess:
        aws_profile = None
        target_config = type("T", (), {"region": "us-east-1"})()

    msg, kind, routed = _diagnose_for_dialog(_MS(), _Sess())
    # A provided subnet override needs no live diagnosis.
    assert kind == "discovered"
    assert "provided" in msg
    assert routed == ""


def test_diagnose_for_dialog_no_session_returns_empty() -> None:
    from dsql_migrator.ui.data_migration import _diagnose_for_dialog

    class _MS:
        def cdc_infra_inputs(self):
            return {"vpc_id": "vpc-1"}

    assert _diagnose_for_dialog(_MS(), None) == ("", "", "")


def test_diagnose_for_dialog_missing_vpc_or_region_returns_empty() -> None:
    from dsql_migrator.ui.data_migration import _diagnose_for_dialog

    class _MS:
        def cdc_infra_inputs(self):
            return {}  # no vpc_id

    class _Sess:
        aws_profile = None
        target_config = type("T", (), {"region": "us-east-1"})()

    assert _diagnose_for_dialog(_MS(), _Sess()) == ("", "", "")


# --- CDC-deploy event-loop offload contract (Fargate: one asyncio loop) ------
# These guard the fix that moved the blocking AWS round-trips off the NiceGUI
# event loop so a CDC deploy click can't freeze every connected browser session.


def test_cdc_deploy_handlers_are_coroutine_functions() -> None:
    # _open_cdc_infra_dialog and _start_cdc_infra_deploy must stay async so their
    # AWS round-trips run via run.io_bound; if either reverts to a plain def it
    # would block the shared event loop again.
    import inspect as _inspect

    from dsql_migrator.ui.data_migration import (
        _open_cdc_infra_dialog,
        _start_cdc_infra_deploy,
    )

    assert _inspect.iscoroutinefunction(_open_cdc_infra_dialog)
    assert _inspect.iscoroutinefunction(_start_cdc_infra_deploy)


def test_migration_plan_awaits_async_cdc_helpers() -> None:
    # The Migration plan screen reuses the async CDC helpers and MUST await them.
    # Calling _open_cdc_infra_dialog / _start_cdc_infra_deploy without await leaves
    # an un-awaited coroutine (RuntimeWarning) -> the dialog never opens and the
    # "Deploy CDC infrastructure" button does nothing. Guard: every call to those
    # helpers in migration_plan.py is the operand of an `await`.
    import ast
    import pathlib

    import dsql_migrator.ui.migration_plan as mp

    targets = {"_open_cdc_infra_dialog", "_start_cdc_infra_deploy"}
    tree = ast.parse(pathlib.Path(mp.__file__).read_text(encoding="utf-8"))

    awaited = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call)
    }
    unawaited = [
        (node.func.id, node.lineno)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in targets
        and id(node) not in awaited
    ]
    assert not unawaited, f"async CDC helper(s) called without await: {unawaited}"


def test_cdc_infra_prefill_skips_arn_lookup_when_disabled(monkeypatch) -> None:
    # Render passes lookup_arn=False so it never makes the DSQL GetCluster call on
    # the event loop. Assert no client is built when the flag is off, and that it
    # IS attempted when on (the submit path) -- so the gate actually gates.
    import dsql_migrator.core.dsql_metadata as dsql_metadata
    from dsql_migrator.ui.data_migration import _cdc_infra_prefill

    calls = {"n": 0}

    def _fake_build_client(profile, region):  # pragma: no cover - should not run when off
        calls["n"] += 1
        raise AssertionError("GetCluster client built despite lookup_arn=False")

    monkeypatch.setattr(dsql_metadata, "build_dsql_client", _fake_build_client)

    class _MS:
        def cdc_infra_inputs(self):
            return {}

    class _Sess:
        aws_profile = None
        source_config = type("S", (), {"host": "src.example"})()
        target_config = type("T", (), {"cluster_endpoint": "abc.dsql.us-east-1.on.aws", "region": "us-east-1"})()

    # lookup_arn=False -> no client built, host still prefilled.
    values = _cdc_infra_prefill(_MS(), _Sess(), lookup_arn=False)
    assert calls["n"] == 0
    assert values.get("source_db_hostname") == "src.example"
    assert not values.get("dsql_cluster_arn")


# --- _resolve_cdc_source_secret: reuse SM secret vs. auto-create from creds ---


def _secret_session(*, secret_id=None, username=None, password=None):
    """Build a minimal session-like object for the secret-resolution helper."""
    from dsql_migrator.config import SecretValue

    cfg = type("Cfg", (), {"username": username})()
    return type(
        "Sess",
        (),
        {
            "source_secret_id": secret_id,
            "source_config": cfg,
            "source_password": SecretValue(password) if password is not None else None,
        },
    )()


def test_resolve_cdc_secret_reuses_sm_arn_without_creating(monkeypatch) -> None:
    from dsql_migrator.ui import data_migration as dm

    # SM auth -> reuse the ARN; ensure_source_secret must NOT be called.
    def _boom(*_a, **_kw):  # pragma: no cover - asserts it is never reached
        raise AssertionError("ensure_source_secret should not run for SM auth")

    monkeypatch.setattr("dsql_migrator.core.secrets.ensure_source_secret", _boom)

    arn = "arn:aws:secretsmanager:us-east-1:111122223333:secret:my/src-AbCdEf"
    out = dm._resolve_cdc_source_secret(
        _secret_session(secret_id=arn),
        stack_name="mysql-dsql-cdc-stack",
        aws_profile=None,
        region="us-east-1",
    )
    assert out.ok is True
    assert out.arn == arn
    assert out.name == "my/src"  # extract_secret_name strips the random suffix


def test_resolve_cdc_secret_autocreates_for_password_auth(monkeypatch) -> None:
    from dsql_migrator.ui import data_migration as dm

    calls: dict = {}

    def _fake_ensure(*, stack_name, username, password, aws_profile, region, kms_key_id=None):
        calls.update(
            stack_name=stack_name, username=username,
            password=password, aws_profile=aws_profile, region=region,
            kms_key_id=kms_key_id,
        )
        return "arn:aws:secretsmanager:us-east-1:111122223333:secret:mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source-XyZ"

    monkeypatch.setattr(
        "dsql_migrator.core.secrets.ensure_source_secret", _fake_ensure
    )

    out = dm._resolve_cdc_source_secret(
        _secret_session(username="appuser", password="s3cr3t"),
        stack_name="mysql-dsql-cdc-stack",
        aws_profile="prof",
        region="us-east-1",
        kms_key_id="alias/my-cmk",
    )
    assert out.ok is True
    assert out.arn.endswith("source-XyZ")
    # The name passed to cdc-stack is the deterministic colon-free name.
    assert out.name == "mysql-dsql-migrator/cdc/mysql-dsql-cdc-stack/source"
    # The in-memory password was revealed and forwarded to ensure_source_secret.
    assert calls["username"] == "appuser"
    assert calls["password"] == "s3cr3t"
    assert calls["stack_name"] == "mysql-dsql-cdc-stack"
    # The optional CMK is forwarded through.
    assert calls["kms_key_id"] == "alias/my-cmk"


def test_resolve_cdc_secret_warns_when_no_credentials() -> None:
    from dsql_migrator.ui import data_migration as dm

    # Password auth but the source was never tested (no password) -> warning, no create.
    out = dm._resolve_cdc_source_secret(
        _secret_session(username="appuser", password=None),
        stack_name="s",
        aws_profile=None,
        region="us-east-1",
    )
    assert out.ok is False
    assert out.error_type == "warning"
    assert "source connection" in out.error.lower()


def test_resolve_cdc_secret_wraps_provision_error_without_leaking(monkeypatch) -> None:
    from dsql_migrator.ui import data_migration as dm
    from dsql_migrator.core.secrets import SecretProvisionError

    def _fail(**_kw):
        # A credential-free provision error (the helper must surface it verbatim,
        # and must never include the password).
        raise SecretProvisionError("Could not create the source secret 's': denied")

    monkeypatch.setattr(
        "dsql_migrator.core.secrets.ensure_source_secret", _fail
    )

    out = dm._resolve_cdc_source_secret(
        _secret_session(username="u", password="leaky-password"),
        stack_name="s",
        aws_profile=None,
        region="us-east-1",
    )
    assert out.ok is False
    assert out.error_type == "negative"
    assert "leaky-password" not in out.error


# --- _is_inflight_stack_status: terminal-stuck vs. busy guidance -------------


def test_inflight_status_true_only_for_in_progress() -> None:
    from dsql_migrator.ui.data_migration import _is_inflight_stack_status

    # Live operations -> the UI should say "wait".
    for s in (
        "CREATE_IN_PROGRESS",
        "UPDATE_IN_PROGRESS",
        "DELETE_IN_PROGRESS",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "ROLLBACK_IN_PROGRESS",
    ):
        assert _is_inflight_stack_status(s) is True, s


def test_inflight_status_false_for_terminal_stuck_states() -> None:
    from dsql_migrator.ui.data_migration import _is_inflight_stack_status

    # Terminal failed/rolled-back states -> the UI should say "delete then retry",
    # never "wait" (they never clear on their own).
    for s in (
        "ROLLBACK_FAILED",
        "ROLLBACK_COMPLETE",
        "UPDATE_ROLLBACK_FAILED",
        "UPDATE_ROLLBACK_COMPLETE",
        "DELETE_FAILED",
        "CREATE_FAILED",
        None,
        "",
    ):
        assert _is_inflight_stack_status(s) is False, s


def test_cdc_unstable_message_delete_in_progress_is_reassuring() -> None:
    # While the cdc-stack is being torn down the card must say it's BEING DELETED
    # (with the ~15-25 min expectation), not a vague "Busy" / "needs cleanup".
    from dsql_migrator.ui.data_migration import cdc_unstable_message

    badge, tone, header, body = cdc_unstable_message("DELETE_IN_PROGRESS")
    assert badge == "Deleting…"
    assert tone == "info"  # reassuring, not a warning — deletion is expected
    assert "being deleted" in header.lower()
    assert "15" in body and "billing stops" in body.lower()


def test_cdc_unstable_message_other_in_progress_says_wait() -> None:
    from dsql_migrator.ui.data_migration import cdc_unstable_message

    badge, tone, header, body = cdc_unstable_message("UPDATE_IN_PROGRESS")
    assert badge == "Busy"
    assert tone == "warning"
    assert "wait" in body.lower()
    assert "UPDATE_IN_PROGRESS" in body


def test_cdc_unstable_message_terminal_stuck_says_delete_then_retry() -> None:
    from dsql_migrator.ui.data_migration import cdc_unstable_message

    for status in ("ROLLBACK_COMPLETE", "DELETE_FAILED", None):
        badge, tone, header, body = cdc_unstable_message(status)
        assert badge == "Busy"
        assert tone == "warning"
        assert "cleanup" in header.lower()
        assert "Delete CDC infrastructure" in body


def test_cdc_infra_form_stack_name_field_uses_fixed_prefix_prop() -> None:
    """The stack-name field renders the mandatory prefix via Quasar's `prefix` prop
    (inside the field, baseline-aligned) and prefills only the suffix — so the label
    and typed text can't misalign and a bare word can't escape the mysql-dsql-cdc-*
    family."""
    from dsql_migrator.ui.data_migration import _render_cdc_infra_form

    props_seen: list[str] = []
    input_values: list[str] = []

    class _El:
        def classes(self, *_a, **_k):
            return self

        def props(self, *a, **_k):
            if a and isinstance(a[0], str):
                props_seen.append(a[0])
            return self

        def on(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *_e):
            return False

    class _Ui:
        def input(self, *_a, value=None, **_k):
            if value is not None:
                input_values.append(value)
            return _El()

        def label(self, *_a, **_k):
            return _El()

        def row(self, *_a, **_k):
            return _El()

        def column(self, *_a, **_k):
            return _El()

        def expansion(self, *_a, **_k):
            return _El()

    class _MS:
        cdc_stack_name = "mysql-dsql-cdc-stack"

        def cdc_infra_inputs(self):
            return {}

        def set_cdc_infra_inputs(self, _v):
            pass

    _render_cdc_infra_form(_Ui(), _MS(), session=None)
    # The fixed prefix is applied as a Quasar prop, not a floating separate label.
    assert any('prefix="mysql-dsql-cdc-"' in p for p in props_seen)
    # The field prefills the SUFFIX only ("stack"), never the whole name.
    assert "stack" in input_values
    assert "mysql-dsql-cdc-stack" not in input_values


# --- _render_cdc_least_privilege_note: dedicated-CDC-user guidance -----------


class _RecordingUi:
    """A tiny NiceGUI stand-in that records label/code text and is context-safe.

    ui.expansion/row/card return a context manager; ui.label/code/icon/button
    return a chainable element. All emitted text is collected in ``self.texts``.
    """

    def __init__(self) -> None:
        self.texts: list[str] = []

    class _El:
        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _record(self, text):
        if text is not None:
            self.texts.append(str(text))
        return self._El()

    def expansion(self, *_a, **_k):
        return self._El()

    def label(self, text="", *_a, **_k):
        return self._record(text)

    def code(self, text="", *_a, **_k):
        return self._record(text)

    def icon(self, *_a, **_k):
        return self._El()

    def button(self, *_a, **_k):
        return self._El()

    def row(self, *_a, **_k):
        return self._El()

    def column(self, *_a, **_k):
        return self._El()

    def card(self, *_a, **_k):
        return self._El()


def test_deploy_log_lines_show_utc_timezone() -> None:
    # Deploy-log timestamps are UTC (the driver stamps datetime.now(timezone.utc));
    # the rendered line must say so, matching the downloaded log / CloudWatch.
    from datetime import datetime, timezone

    from dsql_migrator.ui.data_migration import _render_deploy_log

    ui = _RecordingUi()
    lines = [
        (datetime(2026, 7, 3, 5, 12, 3, tzinfo=timezone.utc), "Stack deletion submitted."),
    ]
    _render_deploy_log(ui, lines, {"open": True})
    body = "\n".join(ui.texts)
    assert "05:12:03 UTC - Stack deletion submitted." in body


def _completeness(
    *, total, settled, complete, failed, mismatched, unknown=0
):
    from dsql_migrator.ui.data_migration import FullLoadCompleteness

    return FullLoadCompleteness(
        total=total,
        settled=settled,
        complete=complete,
        failed=failed,
        mismatched=list(mismatched),
        unknown=unknown,
    )


def test_completeness_banner_approximate_mismatch_is_info_not_warning() -> None:
    # Estimate-only discrepancy (no FAILED table, approximate baseline) -> calm
    # INFO note that defers to Validation, NOT a red "finished with issues" alert.
    from dsql_migrator.ui.data_migration import _render_completeness_banner

    ui = _RecordingUi()
    comp = _completeness(
        total=1, settled=1, complete=0, failed=0, mismatched=["cdc_demo.orders"]
    )
    _render_completeness_banner(ui, comp, approximate=True)
    blob = "\n".join(ui.texts)
    assert "finished with issues" not in blob
    assert "differ from the estimate" in blob
    assert "Validation" in blob


def test_completeness_banner_exact_mismatch_is_warning() -> None:
    # When the baseline is exact (not approximate), a row-count mismatch is a real
    # issue and stays a warning.
    from dsql_migrator.ui.data_migration import _render_completeness_banner

    ui = _RecordingUi()
    comp = _completeness(
        total=1, settled=1, complete=0, failed=0, mismatched=["cdc_demo.orders"]
    )
    _render_completeness_banner(ui, comp, approximate=False)
    blob = "\n".join(ui.texts)
    assert "finished with issues" in blob
    assert "row-count mismatch" in blob


def test_completeness_banner_failed_table_always_warns_even_if_approximate() -> None:
    # A genuinely FAILED table is a real failure regardless of baseline precision.
    from dsql_migrator.ui.data_migration import _render_completeness_banner

    ui = _RecordingUi()
    comp = _completeness(
        total=2, settled=2, complete=1, failed=1, mismatched=[]
    )
    _render_completeness_banner(ui, comp, approximate=True)
    blob = "\n".join(ui.texts)
    assert "finished with issues" in blob
    assert "1 failed" in blob


def test_completeness_banner_all_complete_is_success() -> None:
    from dsql_migrator.ui.data_migration import _render_completeness_banner

    ui = _RecordingUi()
    comp = _completeness(
        total=2, settled=2, complete=2, failed=0, mismatched=[]
    )
    _render_completeness_banner(ui, comp, approximate=True)
    blob = "\n".join(ui.texts)
    assert "Full Load complete" in blob


def test_least_privilege_note_shown_for_password_auth() -> None:
    from dsql_migrator.ui.data_migration import _render_cdc_least_privilege_note

    ui = _RecordingUi()
    sess = type("S", (), {"source_secret_id": None})()
    _render_cdc_least_privilege_note(ui, session=sess)
    blob = "\n".join(ui.texts)
    # The recommendation + a grant snippet scoped to Debezium's needs.
    assert "least-privilege" in blob.lower() or "dedicated" in blob.lower()
    assert "REPLICATION SLAVE" in blob
    assert "LOCK TABLES" in blob
    assert "CREATE USER" in blob


def test_least_privilege_note_hidden_for_sm_auth() -> None:
    from dsql_migrator.ui.data_migration import _render_cdc_least_privilege_note

    ui = _RecordingUi()
    sess = type("S", (), {"source_secret_id": "arn:aws:secretsmanager:...:secret:x"})()
    _render_cdc_least_privilege_note(ui, session=sess)
    # SM-auth source manages its own credential -> no nudge, nothing rendered.
    assert ui.texts == []


# --- DataMigrationState.set_cdc_stack_name: validated, multi-DB names --------


def test_set_cdc_stack_name_accepts_family_name() -> None:
    from dsql_migrator.ui.data_migration import (
        CDC_DEFAULT_STACK_NAME,
        DataMigrationState,
    )

    ms = DataMigrationState()
    assert ms.cdc_stack_name == CDC_DEFAULT_STACK_NAME
    assert ms.set_cdc_stack_name("mysql-dsql-cdc-orders") is True
    assert ms.cdc_stack_name == "mysql-dsql-cdc-orders"
    # Whitespace is trimmed.
    assert ms.set_cdc_stack_name("  mysql-dsql-cdc-billing  ") is True
    assert ms.cdc_stack_name == "mysql-dsql-cdc-billing"


def test_set_cdc_stack_name_rejects_out_of_family_and_keeps_current() -> None:
    from dsql_migrator.ui.data_migration import DataMigrationState

    ms = DataMigrationState()
    assert ms.set_cdc_stack_name("mysql-dsql-cdc-orders") is True
    # A name outside the mysql-dsql-cdc-* family is rejected and the prior name kept,
    # so the tool never deploys resources the deploy role cannot manage.
    assert ms.set_cdc_stack_name("my-own-stack") is False
    assert ms.cdc_stack_name == "mysql-dsql-cdc-orders"
    assert ms.set_cdc_stack_name("mysql-dsql-cdc-bad_name") is False
    assert ms.cdc_stack_name == "mysql-dsql-cdc-orders"


def test_filter_mine_isolates_two_concurrent_stacks() -> None:
    from dsql_migrator.ui.data_migration import _filter_mine

    # Two cdc-stacks' connectors coexist in the region; each stack sees only its own.
    raw = [
        {"connectorName": "mysql-dsql-cdc-orders-debezium-source"},
        {"connectorName": "mysql-dsql-cdc-orders-dsql-sink"},
        {"connectorName": "mysql-dsql-cdc-billing-debezium-source"},
        {"connectorName": "mysql-dsql-cdc-billing-dsql-sink"},
    ]
    assert _filter_mine(raw, "mysql-dsql-cdc-orders") == [
        "mysql-dsql-cdc-orders-debezium-source",
        "mysql-dsql-cdc-orders-dsql-sink",
    ]
    assert _filter_mine(raw, "mysql-dsql-cdc-billing") == [
        "mysql-dsql-cdc-billing-debezium-source",
        "mysql-dsql-cdc-billing-dsql-sink",
    ]


def test_migration_type_lock_reason_in_progress_is_owned() -> None:
    from dsql_migrator.ui.data_migration import migration_type_lock_reason
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    reason = migration_type_lock_reason(state, status=StepStatus.IN_PROGRESS)
    assert reason is not None and "in progress" in reason


def test_migration_type_lock_reason_discovered_connectors() -> None:
    from dsql_migrator.ui.data_migration import (
        migration_type_lock_reason,
        migration_type_locked,
    )
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    state.set_cdc_connector_names(["mysql-dsql-cdc-stack-dsql-sink"])
    reason = migration_type_lock_reason(state, status=StepStatus.NOT_STARTED)
    assert reason is not None and "previous run" in reason
    assert migration_type_locked(state, None, status=StepStatus.NOT_STARTED) is True


def test_migration_type_lock_reason_none_when_idle() -> None:
    from dsql_migrator.ui.data_migration import (
        migration_type_lock_reason,
        migration_type_locked,
    )
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    assert migration_type_lock_reason(state, status=StepStatus.NOT_STARTED) is None
    assert migration_type_locked(state, None, status=StepStatus.NOT_STARTED) is False


def test_apply_cdc_status_logs_dlq_events_to_activity_log() -> None:
    # Each NEW DLQ event must also land in the durable activity-log file as a CDC
    # FAILURE line (auditable outside the live UI), credential-free. The activity
    # logger sets propagate=False once a handler is configured, so attach a capture
    # handler directly to it rather than relying on caplog (root-attached).
    import logging

    from dsql_migrator.core.activity_log import ACTIVITY_LOGGER_NAME
    from dsql_migrator.core.cdc import (
        CdcConnectorError,
        ConnectorState,
        ConnectorStatus,
    )
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    state = DataMigrationState()
    state.job_id = "job-cdc"
    statuses = [ConnectorStatus(name="sink", state=ConnectorState.RUNNING)]
    health = {"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)}
    dlq_errors = [
        CdcConnectorError(
            table="products",
            message="DLQ offset=21: column \"_dlq_probe\" does not exist",
            error_code="42703",
        )
    ]

    captured: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            captured.append(record)

    logger = logging.getLogger(ACTIVITY_LOGGER_NAME)
    handler = _Capture()
    prev_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    try:
        _apply_cdc_status(state, (statuses, health, dlq_errors))
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prev_level)

    lines = [
        r.getMessage()
        for r in captured
        if "quarantine record to DLQ" in r.getMessage()
    ]
    assert len(lines) == 1, lines
    assert "products" in lines[0]
    assert "42703" in lines[0]
    # The single error log + DLQ depth still update too.
    assert state.cdc_status_view.dlq_depth == 1


def test_apply_cdc_status_folds_dlq_without_full_load_job_id() -> None:
    # Regression: CDC commonly runs with NO Full Load job_id this session
    # (CDC-only / resumed stream). DLQ events must STILL fold + drive depth via the
    # stable per-stack fallback key (cdc_error_log_key); keying off "" silently
    # dropped every quarantined record so the UI panel showed nothing.
    from dsql_migrator.core.cdc import (
        CdcConnectorError,
        ConnectorState,
        ConnectorStatus,
    )
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status, cdc_error_log_key

    state = DataMigrationState()
    assert state.job_id is None  # CDC-only: no Full Load job this session
    key = cdc_error_log_key(state)
    assert key.startswith("cdc:")  # stable per-stack fallback, not ""

    statuses = [ConnectorStatus(name="sink", state=ConnectorState.RUNNING)]
    health = {"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)}
    dlq_errors = [
        CdcConnectorError(table="products", message="DLQ offset=22: boom", error_code=None)
    ]
    _apply_cdc_status(state, (statuses, health, dlq_errors))

    # Folded under the fallback key -> depth reflects it, and the records are
    # retrievable under the SAME key the panel reads.
    assert state.cdc_status_view.dlq_depth == 1
    assert len(state.error_log.records(key)) == 1


# --- DLQ panel: refresh icon + quarantined-record list -----------------------


class _DlqUi:
    """NiceGUI stand-in that records labels, table rows, and button click handlers."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.table_rows: list[dict] = []
        self.click_handlers: list = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

        def bind_value(self, *_a, **_k):
            return self

        def add_slot(self, *_a, **_k):
            return self._ui._El(self._ui)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _rec(self, text):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self)

    def label(self, text="", *_a, **_k):
        return self._rec(text)

    def icon(self, *_a, **_k):
        return self._El(self)

    def badge(self, text="", *_a, **_k):
        return self._rec(text)

    def space(self, *_a, **_k):
        return self._El(self)

    def input(self, *_a, **_k):
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def expansion(self, text="", *_a, **_k):
        return self._rec(text)

    def button(self, *_a, on_click=None, **_k):
        if on_click is not None:
            self.click_handlers.append(on_click)
        return self._El(self)

    def table(self, *_a, rows=None, **_k):
        if rows:
            self.table_rows.extend(rows)
        return self._El(self)

    class _Download:
        def content(self, *_a, **_k):
            return None

    download = _Download()


def _cdc_view_with_dlq(depth: int):
    from dsql_migrator.core.cdc import build_cdc_status_view
    from dsql_migrator.core.models import ErrorLogSummary

    # A clean stream (depth 0) has an EMPTY per-table breakdown; only a non-zero
    # depth attributes errors to a table -- mirror that so the fixture is realistic.
    by_table = {"orders": depth} if depth else {}
    summary = ErrorLogSummary(total_errors=depth, errors_by_table=by_table)
    return build_cdc_status_view([], summary, dlq_depth=depth)


class _JobJM:
    """Job manager double whose get_status returns one MigrationJob for any id."""

    def __init__(self, job):
        self._job = job

    def get_status(self, _job_id):
        return self._job


def test_render_cdc_dlq_panel_lists_records_and_wires_refresh() -> None:
    from dsql_migrator.core.models import DataErrorRecord, MigrationJob
    from dsql_migrator.ui.data_migration import _render_cdc_dlq_panel

    state = DataMigrationState()
    state.job_id = "job-cdc"
    state.error_log.record(
        "job-cdc",
        DataErrorRecord(
            table="orders",
            error_code="42703",
            message="DLQ offset=7: undefined_column",
            occurred_at=datetime(2026, 6, 27, 1, 2, 3, tzinfo=timezone.utc),
        ),
    )

    _JM = lambda: _JobJM(MigrationJob(job_id="job-cdc"))  # noqa: E731

    clicked = {"n": 0}

    def _on_refresh():
        clicked["n"] += 1

    ui = _DlqUi()
    _render_cdc_dlq_panel(
        ui, state, _JM(), _cdc_view_with_dlq(1), on_refresh=_on_refresh
    )

    # The panel renders its header and the individual quarantined record (table,
    # reason) -- not just a count -- so the operator sees WHAT was dead-lettered.
    assert any("Dead-letter queue" in t for t in ui.texts)
    assert any(r.get("table") == "orders" for r in ui.table_rows)
    assert any("undefined_column" in str(r.get("message", "")) for r in ui.table_rows)
    # The refresh icon is wired to the supplied callback.
    assert ui.click_handlers, "expected a refresh button with an on_click handler"
    for handler in ui.click_handlers:
        handler()
    assert clicked["n"] >= 1


def test_render_cdc_dlq_panel_no_refresh_button_without_callback() -> None:
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_cdc_dlq_panel

    state = DataMigrationState()
    state.job_id = "job-cdc"
    ui = _DlqUi()
    # depth 0 -> clean stream: panel still renders, no records, no refresh wired.
    _render_cdc_dlq_panel(
        ui,
        state,
        _JobJM(MigrationJob(job_id="job-cdc")),
        _cdc_view_with_dlq(0),
        on_refresh=None,
    )
    assert any("Dead-letter queue" in t for t in ui.texts)
    assert not ui.click_handlers
    assert not ui.table_rows


def test_migrate_table_passes_applied_target_types_to_exporter() -> None:
    # #1: migrate_table derives value-conversion target types from the run's
    # APPLIED per-table conversion and passes them to the exporter, so a remapped
    # column (e.g. TINYINT(1) -> smallint) converts to the applied target type.
    import dataclasses

    from dsql_migrator.core.converter import TableConversion

    exporter = _FakeExporter(rows_by_table={"orders": [{"id": 1}]})
    importer = _FakeImporter()
    applied = TableConversion(
        table="orders",
        target_ddl='CREATE TABLE "orders" ("id" uuid PRIMARY KEY, "active" smallint)',
    )
    inputs = dataclasses.replace(_inputs(), table_conversions={"orders": applied})
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: importer,  # type: ignore[arg-type,return-value]
    )

    migrator.migrate_table(_tables()[0])  # orders

    assert exporter.target_types_by_table["orders"] == {
        "id": "uuid",
        "active": "smallint",
    }


def test_default_table_recreator_uses_applied_conversion(monkeypatch) -> None:
    # #3: a fresh/replace load recreates the target from the APPLIED (edited)
    # conversion -- preserving a user-remapped schema -- not a deterministic
    # re-derivation that would clobber it.
    import dataclasses

    from dsql_migrator.core.converter import TableConversion
    from dsql_migrator.ui.data_migration import _engine

    captured: dict[str, object] = {}

    def fake_recreate_table(schema_ddls, target_ddl, *, connection_factory):
        captured["schema_ddls"] = list(schema_ddls)
        captured["target_ddl"] = target_ddl

    class _FakeConnector:
        def __init__(self, *args, **kwargs) -> None:
            pass

        def connect(self):  # pragma: no cover - not invoked by the fake
            return None

    monkeypatch.setattr(_engine, "recreate_table", fake_recreate_table)
    monkeypatch.setattr(_engine, "DsqlConnector", _FakeConnector)

    applied = TableConversion(
        table="orders",
        target_ddl='CREATE TABLE "orders" ("id" uuid PRIMARY KEY, "active" smallint)',
        schema_ddls=['CREATE SCHEMA IF NOT EXISTS "app"'],
        index_ddls=['CREATE INDEX ASYNC ix ON "orders" ("active")'],
    )
    inputs = dataclasses.replace(_inputs(), table_conversions={"orders": applied})

    recreator = _engine._default_table_recreator(inputs)
    index_ddls = recreator(_tables()[0])  # orders

    assert captured["target_ddl"] == applied.target_ddl
    assert captured["schema_ddls"] == applied.schema_ddls
    assert index_ddls == applied.index_ddls


# --- accept-quarantined-rows override (_finalize_run) ----------------------


class _FinalizeHandleStub:
    """Minimal JobHandle stand-in: _finalize_run only reads ``cancelled``."""

    cancelled = False


def _record_quarantine(log: ErrorLogStore, job_id: str, n: int = 1) -> None:
    from dsql_migrator.core.models import DataErrorRecord

    for i in range(n):
        log.record(
            job_id,
            DataErrorRecord(
                table="t",
                chunk_id="t",
                error_code=None,
                message=f"quarantined row pk[id={i}]: datatype limit greater than "
                "1048576 bytes not supported for text",
                occurred_at=datetime.now(timezone.utc),
            ),
        )


def _record_failure(log: ErrorLogStore, job_id: str) -> None:
    from dsql_migrator.core.models import DataErrorRecord

    log.record(
        job_id,
        DataErrorRecord(
            table="t",
            chunk_id="t",
            error_code="08006",
            message="OperationalError: connection reset",
            occurred_at=datetime.now(timezone.utc),
        ),
    )


def test_finalize_run_quarantine_only_accepted_completes() -> None:
    from dsql_migrator.ui.data_migration._engine import _finalize_run, _RunCounts

    log = ErrorLogStore()
    _record_quarantine(log, "j1", 1)
    # accept=True + quarantine-only => completes (no raise), unblocking CDC.
    _finalize_run(
        _FinalizeHandleStub(),
        "j1",
        ["t"],
        _RunCounts(real_failed=0, quarantined=1),
        log,
        accept_quarantined_rows=True,
    )


def test_finalize_run_quarantine_only_not_accepted_raises() -> None:
    from dsql_migrator.ui.data_migration._engine import (
        FullLoadIncompleteError,
        _finalize_run,
        _RunCounts,
    )

    log = ErrorLogStore()
    _record_quarantine(log, "j2", 1)
    with pytest.raises(FullLoadIncompleteError):
        _finalize_run(
            _FinalizeHandleStub(),
            "j2",
            ["t"],
            _RunCounts(real_failed=0, quarantined=1),
            log,
            accept_quarantined_rows=False,
        )


def test_finalize_run_real_failure_still_raises_even_if_accepted() -> None:
    from dsql_migrator.ui.data_migration._engine import (
        FullLoadIncompleteError,
        _finalize_run,
        _RunCounts,
    )

    log = ErrorLogStore()
    _record_failure(log, "j3")
    # A retryable real failure must NOT be bypassed by the override.
    with pytest.raises(FullLoadIncompleteError):
        _finalize_run(
            _FinalizeHandleStub(),
            "j3",
            ["t"],
            _RunCounts(real_failed=1, quarantined=0),
            log,
            accept_quarantined_rows=True,
        )


def test_finalize_run_mixed_failure_and_quarantine_still_raises_when_accepted() -> None:
    from dsql_migrator.ui.data_migration._engine import (
        FullLoadIncompleteError,
        _finalize_run,
        _RunCounts,
    )

    log = ErrorLogStore()
    _record_quarantine(log, "j4", 1)
    _record_failure(log, "j4")
    with pytest.raises(FullLoadIncompleteError):
        _finalize_run(
            _FinalizeHandleStub(),
            "j4",
            ["a", "b"],
            _RunCounts(real_failed=1, quarantined=1),
            log,
            accept_quarantined_rows=True,
        )


def test_finalize_run_clean_completes() -> None:
    from dsql_migrator.ui.data_migration._engine import _finalize_run, _RunCounts

    # Nothing failed/quarantined => completes regardless of the flag.
    _finalize_run(
        _FinalizeHandleStub(),
        "j5",
        ["t"],
        _RunCounts(real_failed=0, quarantined=0),
        ErrorLogStore(),
        accept_quarantined_rows=False,
    )


# --- CDC connector state-transition logging --------------------------------


def test_log_cdc_connector_transitions_logs_changes_only(monkeypatch) -> None:  # noqa: ANN001
    from dsql_migrator.ui import data_migration as dm

    events: list = []
    monkeypatch.setattr(
        dm,
        "_log_cdc_event",
        lambda action, **kw: events.append((action, kw.get("status"))),
    )

    class _View:
        def __init__(self, states):
            self.connector_states = states

    holder = {"states": {"src": "RUNNING", "sink": "PROVISIONING"}}
    monkeypatch.setattr(dm, "_cdc_status_view", lambda ms, jm: _View(holder["states"]))

    class _MS:
        pass

    ms = _MS()
    # First pass: src RUNNING logged; sink PROVISIONING (intermediate) not logged.
    dm._log_cdc_connector_transitions(ms, object())
    assert events == [("connector src running", dm.ActivityStatus.SUCCESS)]

    # Same states again: nothing new (de-duped on the last-seen state).
    dm._log_cdc_connector_transitions(ms, object())
    assert len(events) == 1

    # sink transitions to FAILED -> a single FAILURE event; src unchanged (no re-log).
    holder["states"] = {"src": "RUNNING", "sink": "FAILED"}
    dm._log_cdc_connector_transitions(ms, object())
    assert events[-1] == ("connector sink failed", dm.ActivityStatus.FAILURE)
    assert len(events) == 2
