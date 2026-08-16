# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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
    MigrationType,
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
            from dsql_migrator.ui.data_migration._full_load_engine import TableLoadResult

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
        # Tests default to no sharding (one reader), matching an unsharded table.
        self.shard_ranges_by_table: dict[str, list] = {}
        # The column names of the table each stream_converted_rows call received, so a
        # test can assert the LOB exclusion filtered the SELECT column set.
        self.stream_columns_by_table: dict[str, list[str]] = {}

    def plan_pk_shard_ranges(
        self, conn, table: TableDef, shards: int, *, min_rows: int = 0
    ) -> list:
        # Default: a single (None, None) range = one reader (current behavior). A
        # test can inject multiple ranges via shard_ranges_by_table to exercise
        # sharding.
        return self.shard_ranges_by_table.get(table.name, [(None, None)])

    def stream_converted_rows(
        self,
        conn: SourceConnectionConfig,
        table: TableDef,
        *,
        should_cancel=None,
        target_types=None,
        pk_lower=None,
        pk_upper=None,
    ) -> list[dict]:
        self.streamed.append(table.name)
        self.target_types_by_table[table.name] = target_types
        self.stream_columns_by_table[table.name] = [c.name for c in table.columns]
        # When sharded, serve only the rows whose id falls in [pk_lower, pk_upper)
        # so a K-shard read reconstructs exactly the table (no overlap, no gap).
        rows = self._rows_by_table.get(table.name, [{"id": 1}])
        if pk_lower is not None or pk_upper is not None:
            rows = [
                r for r in rows
                if (pk_lower is None or r["id"] >= pk_lower)
                and (pk_upper is None or r["id"] < pk_upper)
            ]
        return rows


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
        self.key_columns_by_table: dict[str, object] = {}
        # The resume job each import_rows call received (None on the replace/NONE path,
        # the shared job on the SKIP_EXISTING/append path) -- see batch-level resume.
        self.job_by_table: dict[str, object] = {}
        # The column names of the TableDef each import_rows call received, so a test
        # can assert the LOB exclusion filtered the INSERT column list too.
        self.import_columns_by_table: dict[str, list[str]] = {}
        self._rows = rows
        self._failures = failures
        self._first_error = first_error

    def import_rows(
        self, rows, table: TableDef, *, index_ddls=None, job=None, on_batch_loaded=None,
        should_cancel=None, on_conflict=None, shard_sources=None, key_columns=None,
    ) -> BatchedImportResult:
        # When sharded, the engine passes K row streams via shard_sources (and
        # `rows` is an empty sentinel); otherwise the single `rows` stream is used.
        if shard_sources is not None:
            materialized = [row for src in shard_sources for row in src]
        else:
            materialized = list(rows)
        self.index_ddls_by_table[table.name] = index_ddls
        self.on_conflict_by_table[table.name] = on_conflict
        self.key_columns_by_table[table.name] = key_columns
        self.job_by_table[table.name] = job
        self.import_columns_by_table[table.name] = [c.name for c in table.columns]
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


def test_slim_worker_inputs_keeps_only_this_tables_conversion_and_empty_inventory() -> None:
    # A worker migrates ONE table; pickling the full inventory + every table's conversion into
    # each of N worker submissions is O(N^2). The slimmed inputs must carry only this table's
    # conversion and an empty inventory, while preserving the small fields.
    import dataclasses

    from dsql_migrator.core.models import SourceInventory
    from dsql_migrator.ui.data_migration._full_load_engine import _slim_worker_inputs

    full = dataclasses.replace(
        _inputs(),  # inventory has multiple tables (orders + customers)
        table_conversions={"orders": "conv-orders", "customers": "conv-customers"},
    )
    assert len(full.inventory.tables) >= 2

    slim = _slim_worker_inputs(full, "orders")

    assert dict(slim.table_conversions) == {"orders": "conv-orders"}  # only this table
    assert isinstance(slim.inventory, SourceInventory) and slim.inventory.tables == []
    # the small fields are preserved unchanged
    assert slim.source_config is full.source_config
    assert slim.target_config is full.target_config


def test_start_chunk_if_pending_is_idempotent_for_worker_started_signals() -> None:
    # The multiprocess path marks a chunk IN_PROGRESS when a worker signals it actually began
    # (not at submission). Each shard of a table signals, and a straggler may signal after a
    # sibling already started it -- the later signals must NOT re-stamp started_at or
    # re-count the attempt (which would reset the table's elapsed/ETA).
    from dsql_migrator.core.models import ChunkState
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _find_chunk,
        _start_chunk_if_pending,
    )

    job = MigrationJob(
        job_id="j1", chunks=[ChunkState(chunk_id="orders", status="PENDING")]
    )

    _start_chunk_if_pending(job, "orders")
    chunk = _find_chunk(job, "orders")
    assert chunk.status == "IN_PROGRESS" and chunk.attempts == 1
    started_at = chunk.started_at
    assert started_at is not None

    _start_chunk_if_pending(job, "orders")  # a second shard's signal: must be a no-op
    assert chunk.attempts == 1
    assert chunk.started_at == started_at
    assert chunk.status == "IN_PROGRESS"


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


def test_migrate_table_excludes_lob_columns_from_read_and_write() -> None:
    # A migration-wide LOB exclusion drops the column from BOTH the source SELECT
    # (exporter) and the target INSERT (importer), because both derive their column
    # list from the effective TableDef -- but never the primary key, and the row
    # count is unaffected.
    import dataclasses

    table = TableDef(
        name="docs",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="title", mysql_type="varchar(200)"),
            ColumnDef(name="blob_doc", mysql_type="longtext"),
        ],
        primary_key=["id"],
    )
    exporter = _FakeExporter(rows_by_table={"docs": [{"id": 1}, {"id": 2}]})
    importer = _FakeImporter()
    inputs = dataclasses.replace(
        _inputs(),
        inventory=SourceInventory(tables=[table]),
        # Exclude the LOB column AND (defensively) the PK; the PK must survive.
        excluded_lob_columns={"docs": frozenset({"blob_doc", "id"})},
    )

    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
    )

    result = migrator.migrate_table(table)

    # Read side: the excluded LOB column is gone; the PK is kept (never dropped).
    assert exporter.stream_columns_by_table["docs"] == ["id", "title"]
    # Write side: the same filtered column set reaches the importer's INSERT list.
    assert importer.import_columns_by_table["docs"] == ["id", "title"]
    # Rows still flow -- exclusion drops a column, not rows.
    assert result.rows_loaded == 2


def test_migrate_table_keeps_all_columns_when_nothing_excluded() -> None:
    # Control: with no exclusion the full column set flows through unchanged, so the
    # exclusion path cannot silently drop columns on a normal load.
    table = TableDef(
        name="docs",
        columns=[
            ColumnDef(name="id", mysql_type="int", nullable=False),
            ColumnDef(name="title", mysql_type="varchar(200)"),
            ColumnDef(name="blob_doc", mysql_type="longtext"),
        ],
        primary_key=["id"],
    )
    exporter = _FakeExporter(rows_by_table={"docs": [{"id": 1}]})
    importer = _FakeImporter()
    import dataclasses

    inputs = dataclasses.replace(
        _inputs(), inventory=SourceInventory(tables=[table])
    )
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
    )

    migrator.migrate_table(table)

    assert exporter.stream_columns_by_table["docs"] == ["id", "title", "blob_doc"]
    assert importer.import_columns_by_table["docs"] == ["id", "title", "blob_doc"]


def test_batched_table_migrator_shards_the_read_into_k_streams() -> None:
    # When plan_pk_shard_ranges returns K ranges, migrate_table opens K shard
    # streams and passes them to the importer as shard_sources -- together
    # reconstructing the whole table (disjoint ranges, no overlap or gap).
    import dataclasses

    exporter = _FakeExporter(
        rows_by_table={"orders": [{"id": i} for i in range(1, 7)]}
    )
    # 3 shards: (None,3) -> 1,2 ; (3,5) -> 3,4 ; (5,None) -> 5,6
    exporter.shard_ranges_by_table["orders"] = [(None, 3), (3, 5), (5, None)]
    importer = _FakeImporter()

    # Sharding is only safe on the CDC-coexisting path (a CDC stream reconciles any
    # post-snapshot write across the independently-timed per-shard snapshots).
    inputs = dataclasses.replace(_inputs(), cdc_coexisting=True)
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
    )

    result = migrator.migrate_table(_tables()[0])  # orders

    # Three shard streams were opened (one stream_converted_rows call per shard).
    assert exporter.streamed == ["orders", "orders", "orders"]
    # The importer received every row across shards, exactly once.
    loaded_table, loaded_rows = importer.received[0]
    assert loaded_table == "orders"
    assert sorted(r["id"] for r in loaded_rows) == [1, 2, 3, 4, 5, 6]
    assert result.rows_loaded == 6


def test_non_cdc_append_is_never_sharded_even_when_ranges_available() -> None:
    # A NON-CDC append (existing data, no CDC to reconcile) must NOT shard: like the
    # replace path, K independently-timed snapshots could torn-read a concurrently
    # written source with nothing to backfill it. Only cdc_coexisting is safe to shard.
    exporter = _FakeExporter(
        rows_by_table={"orders": [{"id": i} for i in range(1, 7)]}
    )
    exporter.shard_ranges_by_table["orders"] = [(None, 3), (3, 5), (5, None)]
    importer = _FakeImporter()

    migrator = BatchedTableMigrator(
        _inputs(),  # cdc_coexisting=False, no replace -> a plain append
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
    )

    migrator.migrate_table(_tables()[0])  # orders (append, no CDC)

    # Exactly ONE stream opened (single snapshot), not three shards.
    assert exporter.streamed == ["orders"]


def test_replace_path_is_never_sharded_even_when_ranges_available() -> None:
    # A clean replace load (plain INSERT, no CDC) must NOT shard: K independently
    # timed snapshots could produce a cross-shard torn read of a concurrently
    # written source with nothing to reconcile it. Even though the fake would
    # offer 3 shard ranges for "orders", the replace path opens ONE stream.
    import dataclasses

    exporter = _FakeExporter(
        rows_by_table={"orders": [{"id": i} for i in range(1, 7)]}
    )
    exporter.shard_ranges_by_table["orders"] = [(None, 3), (3, 5), (5, None)]
    importer = _FakeImporter()
    inputs = dataclasses.replace(_inputs(), replace_tables=frozenset({"orders"}))
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda t: [],
    )

    migrator.migrate_table(_tables()[0])  # orders (replace)

    # Exactly ONE stream opened (single snapshot), not three shards.
    assert exporter.streamed == ["orders"]
    # Loaded as a plain replace (NONE), single source.
    assert importer.on_conflict_by_table["orders"] == OnConflictMode.NONE


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


def test_pre_recreated_replace_table_skips_ddl_but_keeps_replace_semantics() -> None:
    # BUG-B: when the parent already DROP+recreated the table in the serial pre-pass
    # (pre_recreated=True), the worker must NOT re-run the DDL -- re-running it
    # concurrently across workers is exactly the startup catalog storm (OC001). The
    # load still uses the replace path (plain INSERT NONE) and the post-load index
    # DDLs are derived from the applied conversion instead of from a recreate call.
    import dataclasses

    from dsql_migrator.core.converter import TableConversion

    exporter = _FakeExporter()
    importer = _FakeImporter()
    recreated: list[str] = []

    applied = TableConversion(
        table="orders",
        target_ddl='CREATE TABLE "orders" ("id" bigint PRIMARY KEY)',
        index_ddls=["CREATE INDEX ASYNC ix_orders_x"],
    )
    inputs = dataclasses.replace(
        _inputs(),
        replace_tables=frozenset({"orders"}),
        table_conversions={"orders": applied},
    )
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _inputs: importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda t: recreated.append(t.name) or ["SHOULD NOT BE USED"],
        target_counter=lambda _t: None,
    )

    migrator.migrate_table(_tables()[0], pre_recreated=True)  # orders (replace)

    # The recreator was NOT called (no per-worker DDL storm)...
    assert recreated == []
    # ...but the load is still a clean replace (plain INSERT), and the index DDLs
    # come from the applied conversion, not from the (skipped) recreate.
    assert importer.on_conflict_by_table["orders"] is OnConflictMode.NONE
    assert importer.index_ddls_by_table["orders"] == ["CREATE INDEX ASYNC ix_orders_x"]


def test_migrate_shard_in_process_maps_rows_skipped_from_conflicts(monkeypatch) -> None:
    # Regression: the shard worker referenced `result.rows_skipped`, which does NOT
    # exist on BatchedImportResult (it exposes `conflicts`). Every shard of a sharded
    # single-table load therefore raised AttributeError at its return, was caught and
    # returned FAILED with rows_loaded=0 -- so a single large table failed even though
    # all rows loaded (only the sharded path hit this; the unsharded table path maps
    # rows_skipped=conflicts correctly). Assert the shard worker returns DONE and maps
    # rows_skipped from conflicts.
    import dataclasses

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    table = _tables()[0]  # orders
    inputs = dataclasses.replace(_inputs(), replace_tables=frozenset({table.name}))

    class _ConflictImporter:
        def import_rows(self, rows, _table, *, on_batch_loaded=None,
                        should_cancel=None, on_conflict=None, **_kw):
            list(rows)  # drain the shard's row stream
            if on_batch_loaded is not None:
                on_batch_loaded(5, 3)
            return BatchedImportResult(rows_loaded=5, conflicts=3, failures=0)

    def _fake_migrator(inp):
        return BatchedTableMigrator(
            inp,
            exporter=_FakeExporter(),
            watermark_capturer=_FakeWatermarkCapturer(_watermark()),
            importer_factory=lambda _i: _ConflictImporter(),
            target_counter=lambda _t: None,
        )

    monkeypatch.setattr(_engine, "BatchedTableMigrator", _fake_migrator)

    args = _engine._ShardWorkerArgs(
        job_id="j", table=table, inputs=inputs,
        pk_lower=None, pk_upper=None, shard_index=0,
    )
    result = _engine._migrate_shard_in_process(args)

    assert result.status == "DONE"  # did NOT crash on result.rows_skipped
    assert result.rows_loaded == 5
    assert result.rows_skipped == 3  # mapped from BatchedImportResult.conflicts


def _recreate_inventory():
    """orders (source PK id) + customers (source PK id), both migratable."""
    return _inventory()


def test_schema_recreate_tables_lists_only_empty_tables_with_a_changed_key() -> None:
    """Drives the confirm dialog's disclosure: which EMPTY targets will be dropped and
    recreated to apply a primary key that differs from the source.

    The recreate happens without asking (an empty table has nothing to lose, and a key
    cannot be applied by appending), so the dialog has to SAY it -- a target DDL the user
    edited by hand after Schema Conversion is replaced.
    """
    from dsql_migrator.core.converter import TableConversion
    from dsql_migrator.ui.data_migration import schema_recreate_tables

    conversions = {
        "orders": _composite_applied(),  # target PK (customer_id, id) != source (id)
        "customers": TableConversion(
            table="customers",
            target_ddl='CREATE TABLE "customers" ("id" bigint NOT NULL, '
            'PRIMARY KEY ("id"))',  # unchanged
        ),
    }
    names = ["orders", "customers"]

    assert schema_recreate_tables(
        names,
        table_conversions=conversions,
        inventory=_recreate_inventory(),
        tables_with_data=[],
    ) == ["orders"]


def test_schema_recreate_tables_excludes_a_target_that_already_has_the_key() -> None:
    """The workshop complaint: after "Apply all to target" the empty target ALREADY
    carries the composite key, yet the confirm dialog announced "1 empty table will be
    recreated to apply the chosen primary key" -- claiming a recreate is needed to apply
    a key that is already applied. With the target's real key known and equal, there is
    nothing to apply, so the table must not be disclosed (and the engine skips the
    DROP+CREATE to match).
    """
    from dsql_migrator.ui.data_migration import schema_recreate_tables

    assert (
        schema_recreate_tables(
            ["orders"],
            table_conversions={"orders": _composite_applied()},
            inventory=_recreate_inventory(),
            tables_with_data=[],
            target_keys={"orders": ["customer_id", "id"]},
        )
        == []
    )


def test_schema_recreate_tables_keeps_a_target_whose_key_is_stale_or_unknown() -> None:
    """The conservative half of the same rule. The engine promotes on the source-vs-
    applied comparison, so anything this function drops is a recreate the user is never
    told about -- only a definitely-EQUAL key may clear it.
    """
    from dsql_migrator.ui.data_migration import schema_recreate_tables

    def listed(target_keys):
        return schema_recreate_tables(
            ["orders"],
            table_conversions={"orders": _composite_applied()},
            inventory=_recreate_inventory(),
            tables_with_data=[],
            target_keys=target_keys,
        )

    # Target still on the OLD key -> the recreate really happens, so disclose it.
    assert listed({"orders": ["id"]}) == ["orders"]
    # Key could not be read: unknown is NOT "safe" -- the engine recreates, so say so.
    assert listed({"orders": None}) == ["orders"]
    # Never probed (table absent from the mapping) -> same conservative answer.
    assert listed({}) == ["orders"]
    assert listed(None) == ["orders"]
    # Right columns, WRONG order: (id, customer_id) is a different key -- the leading
    # column is the entire point of the composite strategy -- so it is not a match.
    assert listed({"orders": ["id", "customer_id"]}) == ["orders"]


def test_schema_recreate_tables_excludes_a_populated_table() -> None:
    # A populated table is NEVER silently recreated -- it goes through the explicit
    # "Drop & reload" choice -- so it must not appear in this disclosure, or the dialog
    # would claim a destructive action the load will not take.
    from dsql_migrator.ui.data_migration import schema_recreate_tables

    assert schema_recreate_tables(
        ["orders"],
        table_conversions={"orders": _composite_applied()},
        inventory=_recreate_inventory(),
        tables_with_data=["orders"],
    ) == []


def test_schema_recreate_tables_is_silent_without_a_conversion_or_inventory() -> None:
    from dsql_migrator.ui.data_migration import schema_recreate_tables

    # No applied conversion for the table -> nothing is known to change.
    assert schema_recreate_tables(
        ["orders"],
        table_conversions={},
        inventory=_recreate_inventory(),
        tables_with_data=[],
    ) == []
    # No inventory -> the source key is unknown, so claim nothing.
    assert schema_recreate_tables(
        ["orders"],
        table_conversions={"orders": _composite_applied()},
        inventory=None,
        tables_with_data=[],
    ) == []


class _ConfirmDialogUi:
    """Records label/badge/radio text and button labels, and captures on_click handlers
    so a test can actually OPEN the Full Load confirm dialog and read what it renders."""

    def __init__(self) -> None:
        self.texts: list[str] = []
        self.buttons: list[str] = []
        self.opened = 0
        self.handlers: list[tuple[str, object]] = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def open(self):
            self._ui.opened += 1
            return self

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def _record(self, text):
        if text:
            self.texts.append(str(text))
        return self._El(self)

    def label(self, text="", *_a, **_k):
        return self._record(text)

    def badge(self, text="", *_a, **_k):
        return self._record(text)

    def button(self, text="", *_a, on_click=None, **_k):
        if text:
            self.buttons.append(str(text))
        if on_click is not None:
            self.handlers.append((str(text), on_click))
        return self._El(self)

    def radio(self, options=None, *_a, **_k):
        if isinstance(options, dict):
            self.texts.extend(str(v) for v in options.values())
        return self._El(self)

    def notify(self, *_a, **_k):
        return None

    def timer(self, _interval, callback=None, *_a, **_k):
        # The dialog is opened via ui.timer(delay, dialog.open, once=True) so the
        # element registers before the false->true transition. Fire the one-shot
        # callback immediately here so the test still observes the open.
        if callable(callback):
            callback()
        return _ConfirmDialogUi._El(self)

    def __getattr__(self, _name):
        return lambda *_a, **_k: _ConfirmDialogUi._El(self)


class _StubConnector:
    """Stands in for DsqlConnector: the probe only needs ``.connect`` to exist."""

    def connect(self):  # pragma: no cover - never called; introspectors are faked
        raise AssertionError("the probe's introspector calls must be patched")


class _StubSession:
    """A session with a target_config, so the pre-dialog probe actually runs."""

    target_config = object()
    aws_profile = None


def _open_full_load_confirm_dialog(
    monkeypatch,
    *,
    recreate_candidates,
    migration_state=None,
    session=None,
    selected_names=("ecommerce.orders",),
):
    """Render the Full Load step, click Start, and return the UI double.

    Drives the REAL dialog builder (not a source-text check): NiceGUI's ``context.client``
    is a read-only property on a Context instance, and the pre-dialog target probe runs
    via ``run.io_bound`` -- both are patched so the dialog builds in-process with no AWS
    or database access.

    ``session`` defaults to None, which has no ``target_config`` and so skips the probe
    entirely -- pass :class:`_StubSession` (with the introspectors patched) to exercise it.
    """
    import asyncio

    import nicegui.context as ctxobj
    import nicegui.run as nrun

    from dsql_migrator.ui import data_migration as dm

    class _Client:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(
        type(ctxobj), "client", property(lambda _self: _Client()), raising=False
    )

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)

    monkeypatch.setattr(nrun, "io_bound", _io_bound)

    ui = _ConfirmDialogUi()
    noop = lambda *_a, **_k: None  # noqa: E731

    class _NoJobs:
        def get_status(self, job_id):
            from dsql_migrator.core.job_manager import JobNotFoundError

            raise JobNotFoundError(job_id)

    dm._render_full_load_step(
        ui,
        migration_state if migration_state is not None else DataMigrationState(),
        _NoJobs(),
        session,  # None -> no target_config -> the probe is skipped entirely
        job=None,
        status=StepStatus.NOT_STARTED,
        selected_names=list(selected_names),
        guard_reason=None,
        start_full_load=noop,
        retry_failed_load=noop,
        reload_table=noop,
        accept_quarantine_and_continue=noop,
        stop_full_load=noop,
        refresh=noop,
        # The step passes a THUNK (resolved after the probe); accept a plain list here
        # for the tests that only care about what the dialog renders.
        schema_recreate_candidates=(
            recreate_candidates
            if callable(recreate_candidates)
            else (lambda: recreate_candidates)
        ),
    )
    start = [h for (label, h) in ui.handlers if "Start" in label]
    assert start, f"no Start handler captured; buttons={ui.buttons}"
    asyncio.run(start[0]())
    return ui


def test_confirm_dialog_discloses_the_schema_recreate_before_starting(monkeypatch) -> None:
    """Opening the dialog must SAY that an empty target will be dropped and recreated.

    The recreate is decided in the engine, after this dialog, so the dialog used to show
    only "Confirm and start" -- the user began a run that replaced a target table's DDL
    with no mention of it. Nothing is lost (the table is empty and the DDL was approved in
    Schema Conversion), but a hand-edit made outside Schema Conversion is replaced.
    """
    ui = _open_full_load_confirm_dialog(
        monkeypatch, recreate_candidates=["ecommerce.orders"]
    )

    assert ui.opened == 1
    body = " ".join(ui.texts)
    assert "will be recreated to apply the chosen primary key" in body
    assert "ecommerce.orders" in body
    assert "no data is lost" in body  # says why it is safe, not just that it happens
    # The button names the DDL step, so the action is visible without reading the notice.
    assert "Recreate and load" in ui.buttons
    assert "Confirm and start" not in ui.buttons


def test_confirm_dialog_is_unchanged_when_nothing_will_be_recreated(monkeypatch) -> None:
    # The ordinary load must not gain a scary notice or a renamed button.
    ui = _open_full_load_confirm_dialog(monkeypatch, recreate_candidates=[])

    assert ui.opened == 1
    assert "will be recreated to apply" not in " ".join(ui.texts)
    assert "Confirm and start" in ui.buttons
    assert "Recreate and load" not in ui.buttons


def test_full_load_step_passes_recreate_candidates_into_the_confirm_dialog() -> None:
    """The disclosure must reach the dialog through a PARAMETER.

    It was first written to read ``conv_state``/``inventory`` directly inside the dialog
    closure -- names that are not in that scope -- so every Start click would have raised
    NameError. Nothing caught it because no test opens the dialog, hence this structural
    check that the value is threaded in and closed over.
    """
    from dsql_migrator.ui import data_migration as dm

    code = dm._render_full_load_step.__code__
    dialog = next(
        c for c in code.co_consts
        if getattr(c, "co_name", None) == "_open_confirm_dialog_now"
    )
    build = next(
        c for c in dialog.co_consts if getattr(c, "co_name", None) == "_build"
    )
    # The dialog closes over the caller-supplied list...
    assert "schema_recreate_candidates" in dialog.co_freevars
    # ...and the rendering body closes over the narrowed result.
    assert "recreate_now" in build.co_freevars
    # The stale, unresolvable names must NOT come back.
    dialog_names = set(dialog.co_names) | set(dialog.co_freevars) | set(dialog.co_varnames)
    assert "conv_state" not in dialog_names
    assert "inventory" not in dialog_names


def test_recreate_disclosure_is_resolved_after_the_probe_not_at_render_time(
    monkeypatch,
) -> None:
    """The ordering bug that made the first fix useless.

    ``schema_recreate_candidates`` used to be a LIST, evaluated while the step rendered
    -- before any target had been read -- so it could only compare the applied DDL to the
    SOURCE key and always claimed a recreate. The probe that learns each target's REAL
    key runs later, when Start is clicked. Passing a THUNK instead moves the decision
    after the probe, which is the whole point: a target that already carries the applied
    key must not be announced as needing a recreate.

    Asserted behaviourally: the dialog must not call the thunk until the probe has run.
    """
    calls: list[str] = []

    def _candidates():
        calls.append("resolved")
        return []

    ui = _open_full_load_confirm_dialog(monkeypatch, recreate_candidates=_candidates)

    assert ui.opened == 1
    assert calls == ["resolved"], (
        "the dialog must resolve schema_recreate_candidates exactly once, when it opens "
        "(after the probe) -- not at render time, when no target key is known yet"
    )


def test_full_load_step_feeds_the_probed_target_keys_into_the_disclosure() -> None:
    """The thunk is only useful if it actually consults the probed keys.

    The thunk is built inside ``build_data_migration_screen``'s ``content`` closure,
    which needs a session, inventory and converter to drive -- so this checks the call
    site's AST instead: ``schema_recreate_tables`` must be invoked with a ``target_keys``
    keyword whose value reads ``migration_state.target_primary_keys``. Structural, but
    pinned to the parse tree rather than to a source substring, so reformatting or a
    reworded comment cannot satisfy it.

    Without this the step can pass ``target_keys=None`` (or drop the argument) while all
    the other plumbing stays in place, and the dialog silently reverts to "everything
    with a changed key is recreated" -- the workshop bug.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "schema_recreate_tables"
    ]
    assert calls, "build_data_migration_screen no longer calls schema_recreate_tables"

    for call in calls:
        kwargs = {kw.arg: kw.value for kw in call.keywords}
        assert "target_keys" in kwargs, (
            "the schema_recreate_tables call must pass target_keys, or the disclosure "
            "cannot tell an already-applied key from a stale one"
        )
        assert (
            ast.unparse(kwargs["target_keys"])
            == "migration_state.target_primary_keys"
        ), (
            "target_keys must be the probe's cache on the migration state; got "
            f"{ast.unparse(kwargs['target_keys'])!r}"
        )

    lambdas = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Lambda)
        and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "schema_recreate_tables"
            for c in ast.walk(node)
        )
    ]
    assert lambdas, (
        "schema_recreate_tables must be wrapped in a lambda so it is resolved after the "
        "dialog's target probe, not while the step renders"
    )


def test_pre_dialog_probe_caches_each_targets_real_primary_key(monkeypatch) -> None:
    """The probe that counts rows must ALSO read each target's real primary key.

    Everything downstream is fed from that one cache, so if the probe stops filling it
    the map is empty, every table falls back to "unknown", and the disclosure reverts to
    announcing a recreate for tables that need none -- with the thunk and the
    ``target_keys`` argument both still wired up, so no other test notices.

    Runs the real probe body with the connector and both introspector calls faked, and
    asserts the keys land on the migration state.
    """
    # The probe body lives in _render_full_load_step, which now resides in the
    # _full_load_ui module, so its DsqlConnector / tables_with_rows / target_primary_keys
    # lookups resolve in that module's namespace -- patch it there.
    from dsql_migrator.ui.data_migration import _full_load_ui as fl

    monkeypatch.setattr(fl, "DsqlConnector", lambda *a, **k: _StubConnector())
    monkeypatch.setattr(fl, "tables_with_rows", lambda names, **k: ["ecommerce.other"])
    real_keys = {
        "ecommerce.orders": ["customer_id", "id"],
        "ecommerce.other": ["id"],
    }
    # The probe now reads every target's key in ONE bulk call (target_primary_keys),
    # not a per-table target_primary_key_columns loop.
    monkeypatch.setattr(
        fl,
        "target_primary_keys",
        lambda names, **k: {name: real_keys.get(name) for name in names},
    )

    state = DataMigrationState()
    _open_full_load_confirm_dialog(
        monkeypatch,
        recreate_candidates=lambda: [],
        migration_state=state,
        session=_StubSession(),
        selected_names=["ecommerce.orders", "ecommerce.other"],
    )

    assert state.target_primary_keys == real_keys, (
        "the pre-dialog probe must cache every probed target's real primary key; got "
        f"{state.target_primary_keys!r}"
    )
    assert state.tables_with_data == frozenset({"ecommerce.other"})


def test_migration_state_distinguishes_unreadable_from_unprobed_target_keys() -> None:
    # The conservative fallback rests on this: None ("probed, unreadable") and absent
    # ("never probed") must both survive the round trip, and the getter must not hand
    # out the internal dict for a caller to mutate.
    state = DataMigrationState()
    assert state.target_primary_keys == {}

    state.set_target_primary_keys({"a": ["x", "id"], "b": None})
    assert state.target_primary_keys == {"a": ["x", "id"], "b": None}
    assert "c" not in state.target_primary_keys  # unprobed stays absent

    state.target_primary_keys["a"] = ["mutated"]
    assert state.target_primary_keys["a"] == ["x", "id"]


def test_shard_worker_keys_on_the_target_composite_pk(monkeypatch) -> None:
    """A sharded append must key its skip-filter on the TARGET's key, not the source's.

    Sharding is chosen off the SOURCE PK (a single integer ``id`` is shardable), so a
    table whose TARGET key is a composite ``(customer_id, id)`` reaches this path too --
    and the shard worker used to pass no ``key_columns`` at all. The importer then fell
    back to the source PK, so a SKIP_EXISTING filter ran ``WHERE (id) IN (...)`` against
    a target keyed ``(customer_id, id)``, where ``id`` alone is not unique: the filter
    can match a DIFFERENT row and wrongly skip a source row (silent loss). Only large
    tables shard, so this hit exactly the loads least likely to be row-counted by hand.
    """
    import dataclasses

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    table = _tables()[0]  # orders, source PK (id)
    inputs = dataclasses.replace(
        _inputs(),
        replace_tables=frozenset(),  # append path
        table_conversions={"orders": _composite_applied()},
    )
    seen: dict[str, object] = {}

    class _KeyRecordingImporter:
        def import_rows(self, rows, _table, *, on_batch_loaded=None,
                        should_cancel=None, on_conflict=None, key_columns=None, **_kw):
            list(rows)
            seen["key_columns"] = key_columns
            seen["on_conflict"] = on_conflict
            return BatchedImportResult(rows_loaded=1, failures=0)

    monkeypatch.setattr(
        _engine,
        "BatchedTableMigrator",
        lambda inp: BatchedTableMigrator(
            inp,
            exporter=_FakeExporter(),
            watermark_capturer=_FakeWatermarkCapturer(_watermark()),
            importer_factory=lambda _i: _KeyRecordingImporter(),
            target_counter=lambda _t: None,
        ),
    )

    result = _engine._migrate_shard_in_process(
        _engine._ShardWorkerArgs(
            job_id="j", table=table, inputs=inputs,
            pk_lower=None, pk_upper=None, shard_index=0,
        )
    )

    assert result.status == "DONE"
    assert seen["on_conflict"] is OnConflictMode.SKIP_EXISTING
    assert seen["key_columns"] == ["customer_id", "id"]


def test_shard_worker_leaves_an_unchanged_pk_to_the_source_fallback(monkeypatch) -> None:
    # When the target key equals the source key there is nothing to override, so the
    # worker passes None and the importer keeps its existing source-PK fallback.
    import dataclasses

    from dsql_migrator.core.converter import TableConversion
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    table = _tables()[0]
    plain = TableConversion(
        table="orders",
        target_ddl='CREATE TABLE "orders" ("id" bigint NOT NULL, PRIMARY KEY ("id"))',
    )
    inputs = dataclasses.replace(
        _inputs(), replace_tables=frozenset(), table_conversions={"orders": plain}
    )
    seen: dict[str, object] = {}

    class _KeyRecordingImporter:
        def import_rows(self, rows, _table, *, on_batch_loaded=None,
                        should_cancel=None, on_conflict=None, key_columns=None, **_kw):
            list(rows)
            seen["key_columns"] = key_columns
            return BatchedImportResult(rows_loaded=1, failures=0)

    monkeypatch.setattr(
        _engine,
        "BatchedTableMigrator",
        lambda inp: BatchedTableMigrator(
            inp,
            exporter=_FakeExporter(),
            watermark_capturer=_FakeWatermarkCapturer(_watermark()),
            importer_factory=lambda _i: _KeyRecordingImporter(),
            target_counter=lambda _t: None,
        ),
    )

    _engine._migrate_shard_in_process(
        _engine._ShardWorkerArgs(
            job_id="j", table=table, inputs=inputs,
            pk_lower=None, pk_upper=None, shard_index=0,
        )
    )

    assert seen["key_columns"] is None


def test_replace_load_passes_target_composite_pk_to_importer() -> None:
    # Phase 0: on a fresh (replace) load, migrate_table parses the PK out of the
    # APPLIED target DDL and passes it to the importer as key_columns, so a
    # composite target key drives ON CONFLICT even though the source PK is (id).
    import dataclasses

    from dsql_migrator.core.converter import TableConversion

    exporter = _FakeExporter()
    importer = _FakeImporter()
    applied = TableConversion(
        table="orders",
        target_ddl=(
            'CREATE TABLE "orders" ("id" bigint NOT NULL, '
            '"customer_id" bigint NOT NULL, PRIMARY KEY ("customer_id", "id"))'
        ),
    )
    inputs = dataclasses.replace(
        _inputs(),
        replace_tables=frozenset({"orders"}),
        table_conversions={"orders": applied},
    )
    migrator = BatchedTableMigrator(
        inputs,
        exporter=exporter,  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda _t: [],
        target_counter=lambda _t: None,
    )

    migrator.migrate_table(_tables()[0])  # orders -> replace

    assert importer.key_columns_by_table["orders"] == ["customer_id", "id"]


def _composite_applied():
    """An applied conversion whose target PK is (customer_id, id) -- the Composite
    key strategy the tool recommends -- against a source PK of (id)."""
    from dsql_migrator.core.converter import TableConversion

    return TableConversion(
        table="orders",
        target_ddl=(
            'CREATE TABLE "orders" ("id" bigint NOT NULL, '
            '"customer_id" bigint NOT NULL, PRIMARY KEY ("customer_id", "id"))'
        ),
    )


def _append_migrator(
    importer, *, target_rows, target_pk, recreated=None, **input_overrides
):
    """A migrator on the APPEND path with the composite conversion applied and both
    target probes stubbed (``target_rows`` = COUNT(*) before the load, ``target_pk`` =
    the target's real key). ``recreated`` (a list) records DROP+recreate calls.

    The counter is stateful on purpose: a replace load probes it BEFORE loading (is the
    target empty?) and again AFTER (completeness check), so a constant 0 would trip the
    post-load "silent row loss" guard and mask what the test means to assert.
    """
    import dataclasses

    state = {"loaded": False}
    calls = recreated if recreated is not None else []
    # _FakeExporter yields one canned row per table by default.
    loaded_rows = 1

    def _counter(_table):
        return loaded_rows if state["loaded"] else target_rows

    def _recreate(table):
        calls.append(table.name)
        state["loaded"] = True
        return ["CREATE INDEX ASYNC ix_orders ON orders (id)"]

    inputs = dataclasses.replace(
        _inputs(),
        replace_tables=input_overrides.pop("replace_tables", frozenset()),
        table_conversions={"orders": _composite_applied()},
        **input_overrides,
    )
    return BatchedTableMigrator(
        inputs,
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: importer,  # type: ignore[arg-type,return-value]
        table_recreator=_recreate,
        target_counter=_counter,
        target_pk_reader=lambda _t: target_pk,
    )


def test_empty_target_with_a_changed_pk_is_recreated_from_the_applied_ddl() -> None:
    """The workshop path: Composite key applied in Schema Conversion, then the FIRST
    Full Load -- an APPEND, because append is the default and the derived
    replace_tables is empty when the target holds no rows.

    A changed primary key is a SCHEMA change, so appending can never deliver it. When
    the target is EMPTY, recreating it is non-destructive, so the table is promoted to
    the replace path and created from the applied DDL -- the user actually gets the key
    they chose. The alternative (appending into whatever shape the target happens to
    have) silently loaded 500 rows under the OLD single-column key while the user
    believed they had the hot-partition remedy, and left the table populated so
    correcting it needed a destructive reload.
    """
    importer = _FakeImporter()
    recreated: list[str] = []
    migrator = _append_migrator(
        importer, target_rows=0, target_pk=None, recreated=recreated
    )

    migrator.migrate_table(_tables()[0])

    assert recreated == ["orders"]  # schema created from the applied DDL
    assert importer.key_columns_by_table["orders"] == ["customer_id", "id"]
    # A freshly created target cannot conflict, so it loads with a plain INSERT.
    assert importer.on_conflict_by_table["orders"] is OnConflictMode.NONE
    # ...and the post-load secondary indexes still come from the same conversion.
    assert importer.index_ddls_by_table["orders"]


def test_empty_target_still_on_the_old_key_is_recreated_not_appended_into() -> None:
    """The case that made "just append into an empty target" wrong: the target exists
    and is empty, but still carries the ORIGINAL single-column key (e.g. the composite
    choice was made after the table was created, and never applied).

    Appending would succeed and report success while keying the data the old way. The
    load must recreate the schema instead, so the chosen key is real."""
    importer = _FakeImporter()
    recreated: list[str] = []
    migrator = _append_migrator(
        importer, target_rows=0, target_pk=["id"], recreated=recreated
    )

    migrator.migrate_table(_tables()[0])

    assert recreated == ["orders"]
    assert importer.key_columns_by_table["orders"] == ["customer_id", "id"]


def test_empty_target_already_carrying_the_new_key_is_not_recreated() -> None:
    """The ACTUAL workshop path, and the gap the two tests above left open: the user
    picks the composite key in Step 2 AND clicks "Apply all to target", so the empty
    target already HAS ("customer_id", "id") before the first Full Load.

    Promoting here would DROP and CREATE an identical table to "apply" a key it already
    carries -- a wasted DDL round trip (DSQL allows one DDL per transaction), disclosed
    to the user as "1 empty table will be recreated to apply the chosen primary key",
    which reads as a contradiction against the table they just created. The load must
    append instead, keyed on the real composite key.
    """
    importer = _FakeImporter()
    recreated: list[str] = []
    migrator = _append_migrator(
        importer,
        target_rows=0,
        target_pk=["customer_id", "id"],  # already the applied key
        recreated=recreated,
    )

    migrator.migrate_table(_tables()[0])

    assert recreated == []  # nothing to apply -> no DROP+CREATE
    # Still keyed on the composite key, so the append stays idempotent on the real PK.
    assert importer.key_columns_by_table["orders"] == ["customer_id", "id"]
    # An existing (empty) table CAN conflict on a retry, so the append keeps ON CONFLICT
    # -- unlike the recreate path, which uses a plain INSERT into a fresh table.
    assert importer.on_conflict_by_table["orders"] is not OnConflictMode.NONE


def test_cdc_coexisting_empty_target_appends_when_the_key_already_matches() -> None:
    """Full-load-+-CDC cannot recreate anything (a DROP would race the live sink), so
    the promotion above must not apply. With the target's real key already matching the
    applied DDL, the idempotent append is correct and proceeds keyed on it."""
    importer = _FakeImporter()
    recreated: list[str] = []
    migrator = _append_migrator(
        importer,
        target_rows=0,
        target_pk=["customer_id", "id"],
        recreated=recreated,
        replace_tables=frozenset({"orders"}),  # ignored while CDC coexists
        cdc_coexisting=True,
    )

    migrator.migrate_table(_tables()[0])

    assert recreated == []  # never recreated under a live sink
    assert importer.key_columns_by_table["orders"] == ["customer_id", "id"]
    assert importer.on_conflict_by_table["orders"] is OnConflictMode.SKIP_EXISTING


def test_cdc_coexisting_key_mismatch_refuses_without_offering_drop_reload() -> None:
    # While CDC streams, "Drop & reload" is not an available remedy -- recreating the
    # table would race the live sink -- so the message must not suggest it.
    importer = _FakeImporter()
    migrator = _append_migrator(
        importer,
        target_rows=0,
        target_pk=["id"],
        replace_tables=frozenset({"orders"}),
        cdc_coexisting=True,
    )

    with pytest.raises(RuntimeError) as excinfo:
        migrator.migrate_table(_tables()[0])

    message = str(excinfo.value)
    assert "Stop CDC" in message
    assert "Drop & reload" not in message


def test_append_into_populated_target_uses_the_targets_real_composite_key() -> None:
    # Target already holds rows AND genuinely has the composite key (e.g. a resumed
    # load). Keying on it is correct and idempotent -- no reason to refuse.
    importer = _FakeImporter()
    migrator = _append_migrator(
        importer, target_rows=500, target_pk=["customer_id", "id"]
    )

    migrator.migrate_table(_tables()[0])

    assert importer.key_columns_by_table["orders"] == ["customer_id", "id"]


def test_append_is_refused_when_the_target_still_has_the_old_key() -> None:
    # The case the guard SHOULD catch: the target holds rows under the original (id)
    # key while the applied conversion asks for (customer_id, id). Keying the append
    # on the new columns could skip-wrong, so refuse -- and point at applying the
    # schema, not only at a Drop & reload.
    importer = _FakeImporter()
    migrator = _append_migrator(importer, target_rows=500, target_pk=["id"])

    with pytest.raises(RuntimeError, match="primary key is"):
        migrator.migrate_table(_tables()[0])


def test_append_is_refused_when_the_targets_key_cannot_be_read() -> None:
    # "Unknown" must never be treated as agreement: a populated target whose real key
    # cannot be read is not provably safe, so it refuses rather than guessing.
    importer = _FakeImporter()
    migrator = _append_migrator(importer, target_rows=500, target_pk=None)

    with pytest.raises(RuntimeError, match="could not be read"):
        migrator.migrate_table(_tables()[0])


def test_unchanged_target_pk_never_probes_the_target_on_append() -> None:
    """The common case (target PK == source PK) must stay probe-free -- the catalog
    read exists only to resolve a CHANGED key, and adding a per-table round-trip to
    every append would tax the large-scale path for nothing."""
    import dataclasses

    from dsql_migrator.core.converter import TableConversion

    def _must_not_probe(_table):
        raise AssertionError("target must not be probed when the PK is unchanged")

    plain = TableConversion(
        table="orders",
        target_ddl=(
            'CREATE TABLE "orders" ("id" bigint NOT NULL, '
            '"customer_id" bigint, PRIMARY KEY ("id"))'
        ),
    )
    importer = _FakeImporter()
    migrator = BatchedTableMigrator(
        dataclasses.replace(
            _inputs(), replace_tables=frozenset(), table_conversions={"orders": plain}
        ),
        exporter=_FakeExporter(),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda _t: [],
        target_counter=_must_not_probe,
        target_pk_reader=_must_not_probe,
    )

    migrator.migrate_table(_tables()[0])

    # Append keeps key_columns=None so the importer falls back to the source PK.
    assert importer.key_columns_by_table["orders"] is None


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


def test_migrate_table_wires_resume_job_only_on_the_append_path() -> None:
    # Batch-level resume (Property 4) is wired ONLY where it is safe: the SKIP_EXISTING
    # (append/CDC-coexist) path, where the target is not recreated so a prior attempt's
    # committed batches persist and can be skipped on a retry. On the replace/NONE path a
    # retry recreates the empty target, so skipping "done" batches would silently lose data --
    # the resume job must be withheld (None) there.
    import dataclasses

    resume = MigrationJob(job_id="r")

    # APPEND path (orders not in replace_tables) -> SKIP_EXISTING, resume job passed through.
    append_importer = _FakeImporter(rows=1)
    append_migrator = BatchedTableMigrator(
        _inputs(),
        exporter=_FakeExporter(rows_by_table={"orders": [{"id": 1}]}),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: append_importer,  # type: ignore[arg-type,return-value]
    )
    append_migrator.migrate_table(_tables()[0], resume_job=resume)
    assert append_importer.on_conflict_by_table["orders"] == OnConflictMode.SKIP_EXISTING
    assert append_importer.job_by_table["orders"] is resume

    # REPLACE path -> NONE mode, resume job WITHHELD (None) so a recreate-on-retry cannot lose
    # rows by skipping batches whose data was just dropped.
    replace_importer = _FakeImporter(rows=1)
    replace_inputs = dataclasses.replace(_inputs(), replace_tables=frozenset({"orders"}))
    replace_migrator = BatchedTableMigrator(
        replace_inputs,
        exporter=_FakeExporter(rows_by_table={"orders": [{"id": 1}]}),  # type: ignore[arg-type]
        watermark_capturer=_FakeWatermarkCapturer(_watermark()),  # type: ignore[arg-type]
        importer_factory=lambda _i: replace_importer,  # type: ignore[arg-type,return-value]
        table_recreator=lambda _t: [],
        target_counter=lambda _t: 1,
    )
    replace_migrator.migrate_table(_tables()[0], resume_job=resume)
    assert replace_importer.on_conflict_by_table["orders"] == OnConflictMode.NONE
    assert replace_importer.job_by_table["orders"] is None


def test_views_referencing_selects_only_dependent_views() -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import _views_referencing

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
    import dsql_migrator.ui.data_migration._full_load_engine as engine

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
# Durable CDC-teardown marker + Start-over race-guard predicate
# (persistent "teardown in progress" banner + block re-reset while in flight)
# ---------------------------------------------------------------------------


def test_set_and_clear_cdc_teardown_marker() -> None:
    # The durable marker drives BOTH the persistent teardown banner and the
    # Start-over race-guard; it must round-trip and clear cleanly.
    state = DataMigrationState()
    assert state.cdc_teardown_job_id is None
    assert state.cdc_teardown_ctx == {}
    state.set_cdc_teardown(
        "job-del",
        kind="delete",
        stack="mysql-dsql-cdc-x",
        ctx={"region": "ap-northeast-2", "cleanup_secret": True},
    )
    assert state.cdc_teardown_job_id == "job-del"
    assert state.cdc_teardown_kind == "delete"
    assert state.cdc_teardown_stack == "mysql-dsql-cdc-x"
    assert state.cdc_teardown_ctx["region"] == "ap-northeast-2"  # retry context kept
    state.clear_cdc_teardown()  # job settled / dismissed
    assert state.cdc_teardown_job_id is None
    assert state.cdc_teardown_kind is None
    assert state.cdc_teardown_stack is None
    assert state.cdc_teardown_ctx == {}
    # job_id=None also drops the kind/stack/ctx (no orphaned metadata).
    state.set_cdc_teardown("j", kind="stop", stack="s", ctx={"region": "r"})
    state.set_cdc_teardown(None)
    assert state.cdc_teardown_kind is None and state.cdc_teardown_stack is None
    assert state.cdc_teardown_ctx == {}


def test_reset_in_place_preserves_cdc_teardown_marker() -> None:
    # Concern #1's crux: Start over → delete submits the teardown, THEN wipes the
    # session. reset_in_place must PRESERVE the teardown marker (so the persistent
    # banner + guard keep working) while wiping everything else.
    store = DataMigrationStore()
    state = store.get_or_create("s1")
    state.job_id = "job-xyz"
    state.set_cdc_teardown(
        "teardown-1",
        kind="delete",
        stack="mysql-dsql-cdc-seoul",
        ctx={"region": "ap-northeast-2", "cleanup_secret": True},
    )

    store.reset_in_place("s1")

    assert store.get_or_create("s1") is state  # same instance
    assert state.job_id is None  # everything else wiped
    # ...but the in-flight teardown marker + its retry context survive the reset (so
    # the persistent banner + one-click Retry cleanup keep working post-reset).
    assert state.cdc_teardown_job_id == "teardown-1"
    assert state.cdc_teardown_kind == "delete"
    assert state.cdc_teardown_stack == "mysql-dsql-cdc-seoul"
    assert state.cdc_teardown_ctx == {"region": "ap-northeast-2", "cleanup_secret": True}


def test_reset_in_place_without_teardown_leaves_marker_clear() -> None:
    # No teardown in flight → reset leaves the marker empty (no spurious banner).
    store = DataMigrationStore()
    state = store.get_or_create("s2")
    store.reset_in_place("s2")
    assert state.cdc_teardown_job_id is None
    assert state.cdc_teardown_kind is None
    assert state.cdc_teardown_stack is None


class _MultiJobJM:
    """Job-manager double mapping job_id -> status; unknown ids raise
    JobNotFoundError (matching JobManager.get_status)."""

    def __init__(self, statuses) -> None:
        self._s = dict(statuses)

    def get_status(self, job_id):
        from types import SimpleNamespace

        from dsql_migrator.core.job_manager import JobNotFoundError

        if job_id not in self._s:
            raise JobNotFoundError(job_id)
        return SimpleNamespace(status=self._s[job_id])


def test_cdc_teardown_in_flight_true_via_durable_marker_closes_race() -> None:
    # Concern #2: right after Start over → delete, the local deploy pointer is wiped
    # and the stack has NOT yet flipped to DELETE_IN_PROGRESS -- yet the durable
    # teardown marker (RUNNING) must still block a second Start over (race closed).
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_in_flight

    jm = _MultiJobJM({"td-1": "RUNNING"})
    assert (
        cdc_teardown_in_flight(
            jm,
            teardown_job_id="td-1",
            deploy_job_id=None,  # wiped by the reset
            action_kind=None,  # wiped by the reset
            stack_status="CREATE_COMPLETE",  # not yet DELETE_IN_PROGRESS
        )
        is True
    )


def test_cdc_teardown_in_flight_true_via_local_job_or_stack_status() -> None:
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_in_flight

    # (a) local stop/delete job still running.
    jm = _MultiJobJM({"dep-1": "RUNNING"})
    assert (
        cdc_teardown_in_flight(
            jm,
            teardown_job_id=None,
            deploy_job_id="dep-1",
            action_kind="delete",
            stack_status=None,
        )
        is True
    )
    # (b) freshly-probed stack is mid-operation.
    assert (
        cdc_teardown_in_flight(
            _MultiJobJM({}),
            teardown_job_id=None,
            deploy_job_id=None,
            action_kind=None,
            stack_status="DELETE_IN_PROGRESS",
        )
        is True
    )


def test_cdc_teardown_in_flight_false_for_deploy_start_and_settled_states() -> None:
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_in_flight

    # A running Deploy/Start (kind infra/start) is NOT a teardown → not blocked
    # (re-discoverable; must not trap a user escaping a stuck run).
    assert (
        cdc_teardown_in_flight(
            _MultiJobJM({"dep-1": "RUNNING"}),
            teardown_job_id=None,
            deploy_job_id="dep-1",
            action_kind="infra",
            stack_status=None,
        )
        is False
    )
    # A settled marker (DONE) does not block.
    assert (
        cdc_teardown_in_flight(
            _MultiJobJM({"td-1": "DONE"}),
            teardown_job_id="td-1",
            deploy_job_id=None,
            action_kind=None,
            stack_status=None,
        )
        is False
    )
    # A stuck/terminal stack (DELETE_FAILED / ROLLBACK_COMPLETE) is NOT over-blocked
    # -- the user should still be able to Start over and delete it.
    assert (
        cdc_teardown_in_flight(
            _MultiJobJM({}),
            teardown_job_id=None,
            deploy_job_id=None,
            action_kind=None,
            stack_status="DELETE_FAILED",
        )
        is False
    )


def test_cdc_teardown_in_flight_ignores_deploy_start_stack_status() -> None:
    # Blocker fix: a running Deploy (CREATE_IN_PROGRESS) or Start CDC
    # (UPDATE_IN_PROGRESS) drives the SAME stack through a live status but is NOT a
    # teardown -- it must NOT hard-block Start over (it only warns via
    # cdc_op_in_flight). Only DELETE_IN_PROGRESS counts as a stack-level teardown.
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_in_flight

    for kind, status in (
        ("start", "UPDATE_IN_PROGRESS"),
        ("infra", "CREATE_IN_PROGRESS"),
    ):
        assert (
            cdc_teardown_in_flight(
                _MultiJobJM({"dep-1": "RUNNING"}),
                teardown_job_id=None,
                deploy_job_id="dep-1",
                action_kind=kind,
                stack_status=status,
            )
            is False
        ), f"{kind}/{status} must not hard-block Start over"
    # ...but an unambiguous DELETE_IN_PROGRESS still blocks even with no local job
    # (e.g. a teardown probed cross-session / after a lost job pointer).
    assert (
        cdc_teardown_in_flight(
            _MultiJobJM({}),
            teardown_job_id=None,
            deploy_job_id=None,
            action_kind=None,
            stack_status="DELETE_IN_PROGRESS",
        )
        is True
    )


def test_should_replace_teardown_marker_ownership() -> None:
    # Single-slot marker must not be clobbered by a DIFFERENT still-running teardown
    # (rare two-tab race): keep the first, longer-lived one so the banner/guard don't
    # switch to a shorter job and prematurely clear tracking of the still-running one.
    from dsql_migrator.ui.data_migration._cdc_status import (
        should_replace_teardown_marker,
    )

    jm = _MultiJobJM({"old": "RUNNING", "settled": "DONE"})
    assert should_replace_teardown_marker(jm, None, "new") is True  # no current
    assert should_replace_teardown_marker(jm, "old", "old") is True  # same job
    assert should_replace_teardown_marker(jm, "old", "new") is False  # different+running
    assert should_replace_teardown_marker(jm, "settled", "new") is True  # settled
    assert should_replace_teardown_marker(jm, "ghost", "new") is True  # unknown/lost


def test_cdc_teardown_banner_state_tracks_job_status() -> None:
    # running while PENDING/RUNNING; failed on FAILED/CANCELLED (actionable banner);
    # None when settled-ok/unknown (caller clears the marker + hides the banner).
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_banner_state

    assert cdc_teardown_banner_state(_MultiJobJM({}), None) is None  # no marker
    assert cdc_teardown_banner_state(_MultiJobJM({"j": "PENDING"}), "j") == "running"
    assert cdc_teardown_banner_state(_MultiJobJM({"j": "RUNNING"}), "j") == "running"
    assert cdc_teardown_banner_state(_MultiJobJM({"j": "FAILED"}), "j") == "failed"
    assert cdc_teardown_banner_state(_MultiJobJM({"j": "CANCELLED"}), "j") == "failed"
    assert cdc_teardown_banner_state(_MultiJobJM({"j": "DONE"}), "j") is None  # ok
    assert cdc_teardown_banner_state(_MultiJobJM({}), "ghost") is None  # lost job


def test_teardown_stack_confirmed_gone_only_on_definitive_absence() -> None:
    # Self-heal for the stale "CDC teardown failed" banner: clears ONLY when
    # CloudFormation definitively reports the stack does-not-exist (describe returns
    # None). A present stack (any state, incl. DELETE_FAILED), a raising/errored read,
    # or a missing deployer/name must NOT clear it (never hide a still-billing failure).
    from dsql_migrator.ui.data_migration._cdc_status import teardown_stack_confirmed_gone

    class _Gone:
        def describe_stack_or_none(self, name):
            return None  # definitive does-not-exist

    class _Present:
        def describe_stack_or_none(self, name):
            return object()  # a discovery -> stack still there (e.g. DELETE_FAILED)

    class _Raises:
        def describe_stack_or_none(self, name):
            raise RuntimeError("throttled / access denied / ambiguous")

    assert teardown_stack_confirmed_gone(_Gone(), "mysql-dsql-cdc-stack") is True
    assert teardown_stack_confirmed_gone(_Present(), "mysql-dsql-cdc-stack") is False
    assert teardown_stack_confirmed_gone(_Raises(), "mysql-dsql-cdc-stack") is False
    assert teardown_stack_confirmed_gone(_Gone(), "") is False  # no stack name
    assert teardown_stack_confirmed_gone(_Gone(), None) is False
    assert teardown_stack_confirmed_gone(None, "mysql-dsql-cdc-stack") is False


def test_cdc_step_delete_and_stop_handlers_set_teardown_marker(monkeypatch) -> None:
    # The CDC-step Delete / Stop buttons must ALSO set the durable marker (not just
    # the Start-over path), so the persistent banner survives navigating away from
    # the CDC step. Exercise the real handlers with stubs (no AWS): job_manager.submit
    # returns an id WITHOUT running the work closure, and the deployer/logger are
    # stubbed. (These handlers had zero coverage before.)
    from types import SimpleNamespace

    import dsql_migrator.core.cdc_deployer as _dep
    import dsql_migrator.ui.data_migration._cdc_ui as _cdcui

    monkeypatch.setattr(_dep, "build_cdc_stack_deployer", lambda *a, **k: object())
    monkeypatch.setattr(_cdcui, "_log_cdc_event", lambda *a, **k: None)

    class _SubmitOnlyJM:
        def __init__(self) -> None:
            self.n = 0

        def submit(self, _work):  # never runs work → no AWS call
            self.n += 1
            return f"job-{self.n}"

    class _Ui:
        def notify(self, *_a, **_k):
            return None

    session = SimpleNamespace(
        target_config=SimpleNamespace(region="us-east-1"),
        aws_profile=None,
        source_secret_id=None,
    )

    state = DataMigrationState()
    _cdcui._start_cdc_delete(_Ui(), state, _SubmitOnlyJM(), lambda: None, session=session)
    assert state.cdc_teardown_job_id == "job-1"
    assert state.cdc_teardown_kind == "delete"
    assert state.cdc_teardown_stack == state.cdc_stack_name
    # Retry context captured so a one-click retry works even post-reset.
    assert state.cdc_teardown_ctx["region"] == "us-east-1"
    assert state.cdc_teardown_ctx["cleanup_secret"] is True  # no SM secret → tool cleans up


def test_start_cdc_deploy_defers_blocking_setup_to_the_job_body(monkeypatch) -> None:
    # The Start-CDC confirm handler runs on the NiceGUI event loop, and on Fargate ONE
    # asyncio loop serves every browser session -- so any blocking call there freezes
    # them all. All the blocking Start-CDC setup (the deploy-role AssumeRole in the
    # deployer build, the STS account lookup, the ~50 KiB template read, the config
    # load) must run INSIDE the submitted job body (worker thread), not on the loop.
    from types import SimpleNamespace

    import dsql_migrator.core.cdc_deployer as _dep
    import dsql_migrator.ui.data_migration._cdc_ui as _cdcui

    calls = {"deployer": 0, "template": 0, "run": 0}

    class _Deployer:
        template_s3_bucket = ""

        def _client(self, _svc):
            return SimpleNamespace(
                get_caller_identity=lambda: {"Account": "111122223333"}
            )

    def _build(*_a, **_k):
        calls["deployer"] += 1
        return _Deployer()

    def _tmpl():
        calls["template"] += 1
        return "TEMPLATE-BODY"

    def _run(*_a, **_k):
        calls["run"] += 1

    monkeypatch.setattr(_dep, "build_cdc_stack_deployer", _build)
    monkeypatch.setattr(_dep, "run_cdc_start", _run)
    monkeypatch.setattr(_cdcui, "_read_cdc_template_body", _tmpl)
    monkeypatch.setattr(_cdcui, "_log_cdc_event", lambda *a, **k: None)

    class _SubmitOnlyJM:
        def __init__(self) -> None:
            self.work = None

        def submit(self, work):
            self.work = work
            return "job-1"

    class _Ui:
        def notify(self, *_a, **_k):
            return None

    session = SimpleNamespace(
        target_config=SimpleNamespace(
            region="us-east-1",
            cluster_endpoint="ep.dsql.amazonaws.com",
            database="postgres",
            username="admin",
        ),
        aws_profile=None,
        source_password=None,
        source_config=None,
        source_secret_id=None,
    )
    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["orders"]))

    jm = _SubmitOnlyJM()
    _cdcui._start_cdc_deploy(
        _Ui(), state, jm, lambda: None, inventory=_inventory(), session=session
    )

    # Submitted a job, but NOTHING blocking ran on the event loop.
    assert jm.work is not None
    assert calls == {"deployer": 0, "template": 0, "run": 0}

    # Running the job body (worker thread) is where the blocking setup + deploy happen.
    jm.work(SimpleNamespace())
    assert calls == {"deployer": 1, "template": 1, "run": 1}

    state2 = DataMigrationState()
    _cdcui._start_cdc_stop(_Ui(), state2, _SubmitOnlyJM(), lambda: None, session=session)
    assert state2.cdc_teardown_job_id == "job-1"
    assert state2.cdc_teardown_kind == "stop"
    assert state2.cdc_teardown_stack == state2.cdc_stack_name
    assert state2.cdc_teardown_ctx["region"] == "us-east-1"
    assert state2.cdc_teardown_ctx["cleanup_secret"] is False  # stop never cleans the secret


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
    from dsql_migrator.ui.data_migration._cdc_status import _ensure_cdc_controller

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


def test_adopt_cdc_stack_points_session_and_forces_rediscovery() -> None:
    # Adopting an existing stack must (a) set the stack name, (b) force a fresh
    # discovery (reset throttle + clear cached phase) so the screen re-reads the live
    # state, and (c) clear the "other stacks" list. It must not mutate AWS.
    state = DataMigrationState()
    state.set_cdc_other_stacks([("mysql-dsql-cdc-seoul-test", "UPDATE_COMPLETE")])
    state.set_cdc_stack_phase("absent", status=None)
    state._cdc_discovery_monotonic = 123.0

    assert state.adopt_cdc_stack("mysql-dsql-cdc-seoul-test") is True
    assert state.cdc_stack_name == "mysql-dsql-cdc-seoul-test"
    assert state._cdc_discovery_monotonic is None    # throttle reset -> next render re-probes
    assert state.cdc_stack_phase is None             # cached phase cleared
    assert state.cdc_stack_phase_checked is False
    assert state.cdc_other_stacks == []              # others cleared after adopting one

    # A name outside the mysql-dsql-cdc-* family is rejected; the name is unchanged.
    assert state.adopt_cdc_stack("not-a-cdc-stack") is False
    assert state.cdc_stack_name == "mysql-dsql-cdc-seoul-test"


def test_probe_cdc_stack_phase_populates_other_stacks(monkeypatch) -> None:
    # The render-time probe surfaces OTHER mysql-dsql-cdc-* stacks (name != mine) so
    # the card can offer to adopt an existing pipeline instead of deploying a duplicate.
    import dsql_migrator.core.cdc_deployer as deployer_mod
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    state = DataMigrationState()  # default stack name
    mine = state.cdc_stack_name

    class _FakeDeployer:
        def describe_stack_or_none(self, name):
            return None  # my stack not deployed under the current (default) name

        def list_cdc_stacks(self):
            return [
                (mine, "CREATE_COMPLETE"),  # mine -> filtered out
                ("mysql-dsql-cdc-seoul-test", "UPDATE_COMPLETE"),
            ]

    monkeypatch.setattr(
        deployer_mod, "build_cdc_stack_deployer", lambda *a, **k: _FakeDeployer()
    )

    class _Sess:
        class target_config:
            region = "ap-northeast-2"

        aws_profile = None

    _probe_cdc_stack_phase(state, _Sess())
    assert state.cdc_other_stacks == [("mysql-dsql-cdc-seoul-test", "UPDATE_COMPLETE")]


def test_cdc_reconciled_table_names_setter_trims_and_adopt_clears() -> None:
    # The reconciled set is trimmed / blank-dropped, and re-adopting a stack clears
    # it (it belonged to the previously-targeted stack; the fresh probe repopulates).
    state = DataMigrationState()
    assert state.cdc_reconciled_table_names == []
    state.set_cdc_reconciled_table_names(["a.x", " a.y ", "", "  "])
    assert state.cdc_reconciled_table_names == ["a.x", "a.y"]
    assert state.adopt_cdc_stack("mysql-dsql-cdc-seoul-test") is True
    assert state.cdc_reconciled_table_names == []


def test_cdc_tables_for_config_falls_back_to_reconciled_stack_tables() -> None:
    # Adopted / out-of-band pipeline: no watermark, no in-session selection, but the
    # live stack's TableIncludeList (reconciled onto the state) names the replicated
    # tables -> resolve them from inventory by .name, so the CDC config/guard/per-table
    # status reflect the running pipeline instead of showing "no tables selected".
    from dsql_migrator.ui.data_migration._cdc_ui import _cdc_tables_for_config

    inv = _inventory()  # tables: orders, customers
    state = DataMigrationState()
    # No watermark, no selection, no reconciled set -> the "select a table" state.
    assert _cdc_tables_for_config(state, inv, None) == []
    # Reconcile from the stack's TableIncludeList (entries are each table's .name).
    state.set_cdc_reconciled_table_names(["orders"])
    resolved = _cdc_tables_for_config(state, inv, None)
    assert [t.name for t in resolved] == ["orders"]


def test_probe_cdc_stack_phase_reconciles_table_include_list(monkeypatch) -> None:
    # The render-time probe reflects the live stack's TableIncludeList onto the state
    # so an adopted pipeline resolves its tables (each entry is a table's .name).
    import dsql_migrator.core.cdc_deployer as deployer_mod
    from dsql_migrator.ui.data_migration._cdc_status import _probe_cdc_stack_phase

    state = DataMigrationState()

    class _Disc:
        stack_status = "UPDATE_COMPLETE"
        is_stable = True
        current_parameters = {
            "TableIncludeList": "ecommerce_demo.orders,ecommerce_demo.customers",
        }

    class _FakeDeployer:
        def describe_stack_or_none(self, name):
            return _Disc()

        def list_cdc_stacks(self):
            return []

    monkeypatch.setattr(
        deployer_mod, "build_cdc_stack_deployer", lambda *a, **k: _FakeDeployer()
    )

    class _Sess:
        class target_config:
            region = "ap-northeast-2"

        aws_profile = None

    _probe_cdc_stack_phase(state, _Sess())
    assert state.cdc_reconciled_table_names == [
        "ecommerce_demo.orders",
        "ecommerce_demo.customers",
    ]


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
    _statuses, _health, dlq_errors, _net, _lag, _series = fetched
    assert [e.table for e in dlq_errors] == ["orders"]


def test_fetch_cdc_status_reads_applied_ops_when_controller_exposes_reader() -> None:
    # When the controller exposes applied_ops_by_table and the caller passes the
    # migrated table set, _fetch scopes the scan-free metric read to those tables
    # and _apply stores it on state (drives the "Changes since Full Load" I/U/D cell).
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status, _fetch_cdc_status

    seen: dict = {}

    class _CtrlWithOps(_FakeCdcController):
        def applied_ops_by_table(self, stack, tables, **_kw):
            seen["stack"] = stack
            seen["tables"] = list(tables)
            return {
                "orders": {"inserts": 5, "updates": 3, "deletes": 1},
                "customers": {"inserts": 2, "updates": 0, "deletes": 4},
            }

    state = DataMigrationState()
    state.set_cdc_stack_name("mysql-dsql-cdc-seoul-test")
    state.set_cdc_controller(
        _CtrlWithOps(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])

    fetched = _fetch_cdc_status(state, ["orders", "customers"])
    assert fetched is not None
    assert seen == {
        "stack": "mysql-dsql-cdc-seoul-test",
        "tables": ["orders", "customers"],
    }
    _apply_cdc_status(state, fetched)
    assert state.cdc_applied_ops_by_table == {
        "orders": {"inserts": 5, "updates": 3, "deletes": 1},
        "customers": {"inserts": 2, "updates": 0, "deletes": 4},
    }


def test_fetch_cdc_status_reads_replication_lag_when_controller_exposes_reader() -> None:
    # _fetch scopes the ReplicationLagMs read to the migrated table set and _apply
    # stores it on state (drives the time-based "Stream lag" column).
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status, _fetch_cdc_status

    seen: dict = {}

    class _CtrlWithLag(_FakeCdcController):
        def replication_lag_by_table(self, stack, tables, **_kw):
            seen["stack"] = stack
            seen["tables"] = list(tables)
            return {"orders": 8500, "customers": 200}

    state = DataMigrationState()
    state.set_cdc_stack_name("mysql-dsql-cdc-seoul-test")
    state.set_cdc_controller(
        _CtrlWithLag(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])

    fetched = _fetch_cdc_status(state, ["orders", "customers"])
    assert fetched is not None
    assert seen == {"stack": "mysql-dsql-cdc-seoul-test", "tables": ["orders", "customers"]}
    _apply_cdc_status(state, fetched)
    assert state.cdc_replication_lag_by_table == {"orders": 8500, "customers": 200}


def test_fetch_cdc_status_records_lag_sample() -> None:
    # _fetch reads the per-table lag + the CloudWatch series; _apply APPENDS one live
    # sample to the rolling buffer (current worst-across-tables lag = the latest point).
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status, _fetch_cdc_status

    class _CtrlWithLag(_FakeCdcController):
        def replication_lag_by_table(self, stack, tables, **_kw):
            return {"orders": 8000, "customers": 3000}

        def replication_lag_series(self, stack, tables, **_kw):
            return [(1000, 5000)]  # ancient seed → trimmed; the live append survives

    state = DataMigrationState()
    state.set_cdc_stack_name("mysql-dsql-cdc-seoul-test")
    state.set_cdc_controller(
        _CtrlWithLag(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])

    fetched = _fetch_cdc_status(state, ["orders", "customers"])
    assert fetched is not None
    _apply_cdc_status(state, fetched)
    # The current MAX lag across tables (8000) is appended as the latest live sample.
    assert state.cdc_replication_lag_series
    assert state.cdc_replication_lag_series[-1][1] == 8000


def test_build_lag_chart_option_line_and_none() -> None:
    from dsql_migrator.ui.data_migration._models import build_lag_chart_option

    assert build_lag_chart_option([]) is None
    assert build_lag_chart_option([(1000, 500)]) is None  # 1 point is not a trend
    opt = build_lag_chart_option([(1000, 500), (1060, 12000), (1120, 0)])
    assert opt["series"][0]["type"] == "line"
    assert opt["xAxis"]["type"] == "time"  # CloudWatch-style time axis
    assert "ms" in opt["yAxis"]["name"]  # y = lag in ms
    # data is [[epoch_ms, lag_ms], ...] (ECharts time axis wants epoch milliseconds).
    assert opt["series"][0]["data"] == [[1000000, 500], [1060000, 12000], [1120000, 0]]
    # Unordered input is sorted by time before plotting.
    opt2 = build_lag_chart_option([(1120, 0), (1000, 500)])
    assert opt2["series"][0]["data"][0][0] == 1000000


def test_record_cdc_lag_sample_seeds_appends_and_trims() -> None:
    # The rolling buffer behind the live chart: seed once from CloudWatch history,
    # append each poll's current lag, coalesce same-second samples, stay bounded.
    state = DataMigrationState()
    now = 1_000_000
    # First sample (empty buffer) → seed from history THEN append the live point.
    state.record_cdc_lag_sample(
        current_ms=500, now_epoch=now, seed_series=[(now - 120, 8000), (now - 60, 3000)]
    )
    assert state.cdc_replication_lag_series == [
        (now - 120, 8000),
        (now - 60, 3000),
        (now, 500),
    ]
    # Next poll: no re-seed (buffer non-empty), just append.
    state.record_cdc_lag_sample(current_ms=200, now_epoch=now + 5, seed_series=[(0, 9)])
    assert state.cdc_replication_lag_series[-1] == (now + 5, 200)
    assert (now - 120, 8000) in state.cdc_replication_lag_series  # still within window
    # Same-second sample coalesces (no duplicate epoch).
    state.record_cdc_lag_sample(current_ms=222, now_epoch=now + 5)
    assert state.cdc_replication_lag_series[-1] == (now + 5, 222)
    assert sum(1 for t, _ in state.cdc_replication_lag_series if t == now + 5) == 1
    # A far-future poll trims everything older than the window.
    state.record_cdc_lag_sample(current_ms=0, now_epoch=now + 10_000, window_seconds=900)
    assert all(t >= now + 10_000 - 900 for t, _ in state.cdc_replication_lag_series)
    # current_ms=None → skip (nothing appended).
    before = list(state.cdc_replication_lag_series)
    state.record_cdc_lag_sample(current_ms=None, now_epoch=now + 10_005)
    assert state.cdc_replication_lag_series == before


def test_migration_status_tables_falls_back_to_reconciled_without_job() -> None:
    # CDC often runs WITHOUT a Full Load job in this session (reconnected to a running
    # pipeline). Without a job, the per-table set (which also scopes the lag/net-rows
    # metric reads + the live chart) must fall back to the reconciled CDC tables, or
    # the whole per-table view + metrics render empty while the pipeline is streaming.
    from types import SimpleNamespace

    from dsql_migrator.core.job_manager import JobNotFoundError
    from dsql_migrator.ui.data_migration import _migration_status_tables

    class _NoJobJM:
        def get_status(self, job_id):
            raise JobNotFoundError(job_id)

    state = DataMigrationState()
    state.job_id = None
    assert _migration_status_tables(state, _NoJobJM()) == []  # no job, no reconciled
    state.set_cdc_reconciled_table_names(
        ["ecommerce_demo.customers", "ecommerce_demo.orders"]
    )
    assert _migration_status_tables(state, _NoJobJM()) == [
        "ecommerce_demo.customers",
        "ecommerce_demo.orders",
    ]

    # A Full Load job still wins (authoritative set).
    class _JobJM2:
        def get_status(self, _job_id):
            return SimpleNamespace(
                chunks=[SimpleNamespace(chunk_id="t1"), SimpleNamespace(chunk_id="t2")]
            )

    state.job_id = "fl-1"
    assert _migration_status_tables(state, _JobJM2()) == ["t1", "t2"]


def test_fetch_cdc_status_skips_applied_ops_without_tables() -> None:
    # No table set (non-UI caller / before Full Load) -> the metric read is not
    # attempted, so a source scan is never triggered for it.
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _fetch_cdc_status

    called = {"n": 0}

    class _CtrlWithOps(_FakeCdcController):
        def applied_ops_by_table(self, stack, tables, **_kw):
            called["n"] += 1
            return {}

    state = DataMigrationState()
    state.set_cdc_stack_name("stk")
    state.set_cdc_controller(
        _CtrlWithOps(
            statuses=[ConnectorStatus(name="sink", state=ConnectorState.RUNNING)],
            health={"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)},
        )
    )
    state.set_cdc_connector_names(["sink"])

    _fetch_cdc_status(state)  # tables omitted
    assert called["n"] == 0


def test_cdc_is_streaming_gates_poll_and_table_refresh() -> None:
    # Single gate for arming the ~5s CDC poll timers (live-status region AND the
    # per-table net-rows table). Both a controller AND connector names are needed;
    # missing either -> static (manual refresh only), so an idle/Full-Load-only
    # page never arms a timer that would re-render forever.
    from dsql_migrator.ui.data_migration import _cdc_is_streaming

    state = DataMigrationState()
    assert _cdc_is_streaming(state) is False  # neither wired

    state.set_cdc_controller(object())
    assert _cdc_is_streaming(state) is False  # controller but no connectors

    state.set_cdc_connector_names(["sink"])
    assert _cdc_is_streaming(state) is True  # both -> streaming

    state.set_cdc_connector_names([])
    assert _cdc_is_streaming(state) is False  # connectors dropped -> static again


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


def test_apply_cdc_status_surfaces_schema_drift_from_sqlstate() -> None:
    # A source ADD COLUMN / TYPE CHANGE shows up as quarantines carrying the
    # telltale SQLSTATE; _apply_cdc_status must classify them into the view's
    # schema_drift groups (per table + kind) so the monitor can flag source DDL,
    # while an ordinary poison row (no drift code) is NOT surfaced as drift.
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
        CdcConnectorError(table="orders", message="add col", error_code="42703"),
        CdcConnectorError(table="orders", message="add col", error_code="42703"),
        # A class-22 value error (string too long) is an ordinary poison row, NOT a
        # source type change -> counts toward DLQ depth but is not grouped as drift.
        CdcConnectorError(table="line_items", message="bad value", error_code="22001"),
        # An ordinary poison row (oversized value): no SQLSTATE -> not drift.
        CdcConnectorError(table="media", message="too big", error_code=None),
    ]

    _apply_cdc_status(state, (statuses, health, dlq_errors))

    view = state.cdc_status_view
    assert view is not None
    # All four count toward DLQ depth, but only the two ADD COLUMN rows are drift; the
    # 22001 value error and the no-SQLSTATE size rejection are ordinary poison rows.
    assert view.dlq_depth == 4
    groups = {(g.table, g.kind): g.count for g in view.schema_drift}
    assert groups == {("orders", "add-column"): 2}
    # The non-drift poison rows are absent from the drift groups.
    assert not any(g.table in {"media", "line_items"} for g in view.schema_drift)


def test_apply_cdc_status_no_drift_on_clean_stream() -> None:
    # No quarantines -> empty schema_drift (banner stays inert).
    from dsql_migrator.core.cdc import ConnectorState, ConnectorStatus
    from dsql_migrator.core.msk_connect_controller import ConnectorHealth
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    state = DataMigrationState()
    state.job_id = "job-cdc"
    statuses = [ConnectorStatus(name="sink", state=ConnectorState.RUNNING)]
    health = {"sink": ConnectorHealth(running_tasks=1, errored_tasks=0)}

    _apply_cdc_status(state, (statuses, health, []))

    view = state.cdc_status_view
    assert view is not None
    assert view.schema_drift == []


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


# ---------------------------------------------------------------------------
# cdc_pipeline_live -- the NARROW "data is actually flowing" signal, distinct
# from cdc_streaming_started's "inputs are committed, stop editing" latch. Only
# this drives the Data Migration "Success" badge / step DONE promotion.
# ---------------------------------------------------------------------------


def test_cdc_pipeline_live_true_only_when_connectors_or_phase_running() -> None:
    from dsql_migrator.ui.data_migration import cdc_pipeline_live

    connectors = DataMigrationState()
    connectors.set_cdc_controller(_FakeCdcController(statuses=[], health={}))
    connectors.set_cdc_connector_names(["mysql-dsql-cdc-stack-debezium-source"])
    assert cdc_pipeline_live(connectors) is True

    running = DataMigrationState()
    running.set_cdc_stack_phase("running")
    assert cdc_pipeline_live(running) is True


def test_cdc_pipeline_live_false_while_a_start_job_is_still_in_flight() -> None:
    # The bug this fixes: an in-flight connector START job made the Data Migration
    # step (and its "Success" badge on both the stepper header and the in-screen
    # chip) flip to DONE while the connectors were still coming up on MSK Connect
    # (~10-20 min) and no row had reached the target. cdc_streaming_started latches
    # then on purpose (inputs are committed), but the pipeline is NOT live yet.
    from dsql_migrator.ui.data_migration import (
        cdc_pipeline_live,
        cdc_streaming_started,
    )

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("start-1", kind="start")
    mgr = _StubJobManager({"start-1": _StubJob("RUNNING")})

    assert cdc_streaming_started(state, mgr) is True  # inputs latch immediately
    assert cdc_pipeline_live(state) is False  # ...but nothing is streaming yet


def test_cdc_pipeline_live_false_before_start_and_for_infra() -> None:
    from dsql_migrator.ui.data_migration import cdc_pipeline_live

    assert cdc_pipeline_live(DataMigrationState()) is False

    infra = DataMigrationState()
    infra.set_cdc_stack_phase("infra")
    assert cdc_pipeline_live(infra) is False


def test_data_migration_step_promotes_only_when_pipeline_live_not_on_start() -> None:
    """End-to-end: the step DONE promotion must follow cdc_pipeline_live.

    Ties the two pieces together -- a RUNNING start job must NOT promote the step
    (badge stays In progress), but detected connectors must.
    """
    from dsql_migrator.ui.data_migration import (
        cdc_pipeline_live,
        data_migration_step_after_cdc,
    )

    starting = DataMigrationState()
    starting.set_cdc_deploy_job_id("start-1", kind="start")
    assert (
        data_migration_step_after_cdc(
            StepStatus.IN_PROGRESS, cdc_streaming=cdc_pipeline_live(starting)
        )
        is None  # not promoted while the start job is still in flight
    )

    live = DataMigrationState()
    live.set_cdc_controller(_FakeCdcController(statuses=[], health={}))
    live.set_cdc_connector_names(["mysql-dsql-cdc-stack-debezium-source"])
    assert (
        data_migration_step_after_cdc(
            StepStatus.IN_PROGRESS, cdc_streaming=cdc_pipeline_live(live)
        )
        is StepStatus.DONE
    )


def test_cdc_streaming_started_false_when_only_infra() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    # Infra deployed but no connectors and no job -> not streaming yet, start
    # point + table picker stay editable.
    state = DataMigrationState()
    state.set_cdc_stack_phase("infra")
    assert cdc_streaming_started(state, _StubJobManager({})) is False


def test_cdc_streaming_started_false_while_infra_deploy_in_flight() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    # An in-flight kind="infra" job creates MSK/networking but NO connectors (the
    # template gates both on HasBootstrapServers, blank on the infra pass), so
    # nothing streams for the ~15-20 min it runs. Counting it as "streaming" made
    # the deploy masquerade as a live pipeline -- promoting Data Migration to DONE,
    # disabling Start Full Load, freezing the picker, and turning "Drop & reload"
    # into an append. It must also NOT block the deploy overlapping the Full Load.
    for status in ("PENDING", "RUNNING"):
        state = DataMigrationState()
        state.set_cdc_deploy_job_id("infra-1", kind="infra")
        mgr = _StubJobManager({"infra-1": _StubJob(status)})
        assert cdc_streaming_started(state, mgr) is False, status


def test_cdc_streaming_started_true_when_infra_job_but_connectors_detected() -> None:
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    # The kind="infra" exclusion must not mask a genuinely live pipeline: detected
    # connectors (or phase "running") still win, so the CDC-live safety gates hold
    # even if a stale infra marker is left on the state.
    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    state.set_cdc_controller(_FakeCdcController(statuses=[], health={}))
    state.set_cdc_connector_names(["mysql-dsql-cdc-stack-debezium-source"])
    mgr = _StubJobManager({"infra-1": _StubJob("RUNNING")})
    assert cdc_streaming_started(state, mgr) is True

    running_phase = DataMigrationState()
    running_phase.set_cdc_deploy_job_id("infra-2", kind="infra")
    running_phase.set_cdc_stack_phase("running")
    assert (
        cdc_streaming_started(
            running_phase, _StubJobManager({"infra-2": _StubJob("RUNNING")})
        )
        is True
    )


def test_infra_deploy_does_not_promote_data_migration_to_done() -> None:
    """An in-flight infra deploy must leave the Data Migration step untouched.

    ``data_migration_step_after_cdc`` promotes the step to DONE (which unlocks
    Validation) and never downgrades it, so a bogus promotion during the ~15-20 min
    MSK create persisted -- Validation opened with zero rows loaded. The promotion
    is driven purely by ``cdc_streaming_started``, so excluding kind="infra" there
    is what keeps the step at NOT_STARTED.
    """
    from dsql_migrator.ui.data_migration import cdc_streaming_started
    from dsql_migrator.ui.data_migration._full_load_engine import data_migration_step_after_cdc
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("RUNNING")})

    streaming = cdc_streaming_started(state, mgr)
    assert streaming is False
    assert (
        data_migration_step_after_cdc(
            StepStatus.NOT_STARTED, cdc_streaming=streaming
        )
        is None
    )


def test_infra_deploy_keeps_drop_and_reload_dropping() -> None:
    """``cdc_coexisting`` must stay False during an infra deploy.

    It feeds ``is_replace``: when True the loader falls back to idempotent
    SKIP_EXISTING (no DROP) because a DROP would race a live sink. During an infra
    create there is no sink, so a "Drop & reload" run must still drop -- otherwise
    the re-load silently appends over stale rows ("0 new + N already there").
    """
    from dsql_migrator.ui.data_migration import cdc_streaming_started

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("RUNNING")})
    assert cdc_streaming_started(state, mgr) is False


def test_infra_create_stack_status_is_not_a_live_migration_operation() -> None:
    """``CREATE_IN_PROGRESS`` must not disable the prerequisite Check button.

    The generic ``_is_inflight_stack_status`` is True for every ``*_IN_PROGRESS``,
    which kept the Check button dead for the whole ~15-20 min CFN create -- a
    SECOND path independent of ``cdc_streaming_started``. Only the infra create uses
    ``create_stack``; Start/Stop CDC go through ``update_stack``, so
    ``UPDATE_IN_PROGRESS`` must still count as a live operation.
    """
    from dsql_migrator.ui.data_migration import (
        _is_inflight_stack_status,
        is_infra_create_stack_status,
    )

    assert is_infra_create_stack_status("CREATE_IN_PROGRESS") is True
    assert is_infra_create_stack_status("create_in_progress") is True
    for other in (
        "UPDATE_IN_PROGRESS",
        "DELETE_IN_PROGRESS",
        "UPDATE_ROLLBACK_IN_PROGRESS",
        "CREATE_COMPLETE",
        None,
        "",
    ):
        assert is_infra_create_stack_status(other) is False, other

    # The composite the Prerequisites panel uses: in-flight AND not an infra create.
    def _live_operation(status: str | None) -> bool:
        return bool(_is_inflight_stack_status(status)) and not is_infra_create_stack_status(
            status
        )

    assert _live_operation("CREATE_IN_PROGRESS") is False
    assert _live_operation("UPDATE_IN_PROGRESS") is True
    assert _live_operation("DELETE_IN_PROGRESS") is True


def test_infra_deploy_does_not_block_schema_conversion_apply() -> None:
    """Schema Conversion's Apply must stay available during an infra deploy.

    ``cdc_active_check`` (wired in app.py to ``cdc_streaming_started``) blocks Apply
    because a live sink is writing the target tables. During an infra create no sink
    exists, so blocking would trap the user: Data Migration is prerequisite-locked
    behind Schema Conversion, so they could not reach the CDC step to stop anything.
    """
    from dsql_migrator.ui.data_migration import cdc_streaming_started
    from dsql_migrator.ui.schema_conversion import _cdc_apply_is_blocked

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("RUNNING")})

    assert _cdc_apply_is_blocked(lambda: cdc_streaming_started(state, mgr)) is False

    # A genuinely live pipeline still blocks (no regression of the safety gate).
    live = DataMigrationState()
    live.set_cdc_stack_phase("running")
    assert (
        _cdc_apply_is_blocked(
            lambda: cdc_streaming_started(live, _StubJobManager({}))
        )
        is True
    )


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
    # An UNKNOWN gated mode (an older snapshot, written before the field existed)
    # keeps the lenient behavior for every mode, so a reconnect is never hard-blocked.
    assert state.prereq_gated_mode is None
    assert (
        full_load_run_guard_reason(
            state, _inventory(), prereq_mode=MigrationMode.CDC, has_run=True
        )
        is None
    )


def test_full_load_run_guard_scopes_the_has_run_excuse_to_the_gated_mode() -> None:
    # The reversible "add CDC after a Full-load-only run" path: the completed run
    # passed only the FULL_LOAD checks, so switching the type to a CDC mode must NOT
    # inherit that pass -- the CDC-only checks (binlog ROW/FULL, replication grants)
    # never ran. Previously has_run=True excused the absent CDC report outright, so
    # Prerequisites collapsed as "done" and the CDC sub-step opened with the binary
    # log format unverified.
    state = DataMigrationState()
    state.set_prereq_gated_mode(MigrationMode.FULL_LOAD)

    # Same mode as the one that gated the run -> still excused (re-run stays open).
    assert (
        full_load_run_guard_reason(
            state, _inventory(), prereq_mode=MigrationMode.FULL_LOAD, has_run=True
        )
        is None
    )
    # A DIFFERENT mode -> the checks genuinely never ran, so it blocks.
    reason = full_load_run_guard_reason(
        state, _inventory(), prereq_mode=MigrationMode.CDC, has_run=True
    )
    assert reason is not None
    assert "adding CDC" in reason

    # Running the CDC checks clears it (a present, passing report always wins).
    state.set_prereq_report(
        MigrationMode.CDC,
        PrerequisiteReport.build(
            MigrationMode.CDC,
            [_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.PASS)],
        ),
    )
    assert (
        full_load_run_guard_reason(
            state, _inventory(), prereq_mode=MigrationMode.CDC, has_run=True
        )
        is None
    )


def test_full_load_run_guard_gated_mode_cdc_covers_a_later_full_load_rerun() -> None:
    # The reverse direction must stay open: a combined (Full load + CDC) run is
    # gated by the CDC superset, which already covers every FULL_LOAD check, so a
    # later Full-load-only re-run is not re-blocked... except that the recorded mode
    # differs, so it asks for a (cheap, read-only) re-check rather than silently
    # trusting a superset it can no longer see. Assert the behavior explicitly so
    # the trade-off is deliberate rather than accidental.
    state = DataMigrationState()
    state.set_prereq_gated_mode(MigrationMode.CDC)
    reason = full_load_run_guard_reason(
        state, _inventory(), prereq_mode=MigrationMode.FULL_LOAD, has_run=True
    )
    assert reason is not None  # asks for the checks; never silently proceeds


def test_cdc_prerequisite_gate_requires_a_cdc_report() -> None:
    # The CDC lifecycle actions (Deploy infra / Start CDC) previously had NO
    # prerequisite gate of their own -- they were safe only because the linear
    # sub-step order happened to put the checks first. With the type changeable late,
    # the gate has to be explicit.
    from dsql_migrator.ui.data_migration import cdc_prerequisite_block_reason

    reason = cdc_prerequisite_block_reason(None)
    assert reason is not None
    assert "CDC prerequisite checks" in reason


def test_cdc_prerequisite_gate_blocks_on_failed_binlog_format() -> None:
    from dsql_migrator.ui.data_migration import cdc_prerequisite_block_reason

    failing = PrerequisiteReport.build(
        MigrationMode.CDC,
        [
            _result(
                PrerequisiteCheckId.BINLOG_ROW_FORMAT,
                PrerequisiteStatus.FAIL,
                title="Binary log uses ROW format with full row image",
            )
        ],
    )
    reason = cdc_prerequisite_block_reason(failing)
    assert reason is not None
    assert "ROW format" in reason
    # A report that ran the checks and passed the binlog one clears the gate.
    passing = PrerequisiteReport.build(
        MigrationMode.CDC,
        [_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.PASS)],
    )
    assert cdc_prerequisite_block_reason(passing) is None


def test_cdc_prerequisite_gate_ignores_unrelated_required_failures() -> None:
    # Deliberately NOT gated on report.can_proceed: a per-table TARGET_SCHEMA_READY
    # failure is the Full Load guard's business and does not make streaming
    # impossible, so it must not also block the CDC lifecycle (that would double-
    # report one problem in two places).
    from dsql_migrator.ui.data_migration import cdc_prerequisite_block_reason

    report = PrerequisiteReport.build(
        MigrationMode.CDC,
        [
            _result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.PASS),
            _result(
                PrerequisiteCheckId.TARGET_SCHEMA_READY,
                PrerequisiteStatus.FAIL,
                target="orders",
            ),
        ],
    )
    assert report.can_proceed is False
    assert cdc_prerequisite_block_reason(report) is None


def test_cdc_prerequisite_gate_blocks_when_binlog_check_is_skipped() -> None:
    # A FULL_LOAD-mode run reports the CDC checks as SKIP. If such a report were
    # ever consulted for the CDC gate, "skipped" must not read as "passed".
    from dsql_migrator.ui.data_migration import cdc_prerequisite_block_reason

    skipped = PrerequisiteReport.build(
        MigrationMode.FULL_LOAD,
        [_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.SKIP)],
    )
    assert cdc_prerequisite_block_reason(skipped) is not None


def test_binlog_resume_gap_detects_a_purged_watermark_log() -> None:
    # The watermark is captured at Full Load START, so a long load + the ~15-20 min
    # infra create + the connector create all elapse before Debezium reads it. If the
    # source purged that file the gapless hand-off is impossible -- today that only
    # surfaces as an undiagnosed connector CREATE_FAILED ~26 min into a billable
    # create (MySQL 1236).
    from dsql_migrator.core.watermark import binlog_resume_gap_reason

    reason = binlog_resume_gap_reason(
        "mysql-bin.000010", ["mysql-bin.000042", "mysql-bin.000043"]
    )
    assert reason is not None
    assert "mysql-bin.000010" in reason
    assert "mysql-bin.000042" in reason  # names the oldest log still kept
    assert "binlog retention hours" in reason  # actionable remediation

    # Still retained -> no warning.
    assert (
        binlog_resume_gap_reason(
            "mysql-bin.000042", ["mysql-bin.000042", "mysql-bin.000043"]
        )
        is None
    )


def test_binlog_resume_gap_never_blocks_on_unknown() -> None:
    # "Unknown" must never be reported as a gap: no watermark file, or a source where
    # SHOW BINARY LOGS is unavailable / the privilege is missing (list_binary_logs
    # returns None, distinct from an empty list).
    from dsql_migrator.core.watermark import binlog_resume_gap_reason

    assert binlog_resume_gap_reason(None, ["mysql-bin.000042"]) is None
    assert binlog_resume_gap_reason("", ["mysql-bin.000042"]) is None
    assert binlog_resume_gap_reason("mysql-bin.000010", None) is None
    assert binlog_resume_gap_reason("mysql-bin.000010", []) is None


def test_list_binary_logs_returns_none_when_unavailable() -> None:
    # Distinguishing None ("unknown") from [] matters: treating a failed probe as an
    # empty log list would wrongly claim every watermark had been purged.
    from dsql_migrator.core.watermark import list_binary_logs

    class _Boom:
        def execute(self, statement, parameters=None):
            raise RuntimeError("SHOW BINARY LOGS denied")

    assert list_binary_logs(_Boom()) is None

    class _Rows:
        def execute(self, statement, parameters=None):
            class _R:
                def mappings(self):
                    class _M:
                        def all(self):
                            return [
                                {"Log_name": "mysql-bin.000042", "File_size": 1},
                                {"Log_name": "mysql-bin.000043", "File_size": 2},
                            ]

                    return _M()

            return _R()

    assert list_binary_logs(_Rows()) == ["mysql-bin.000042", "mysql-bin.000043"]


def test_cdc_start_dialog_is_a_coroutine_and_awaited() -> None:
    # _open_cdc_start_dialog became async (the binlog pre-flight runs via
    # run.io_bound). Every call must be awaited, or the dialog silently never opens
    # and "Start CDC" looks dead. Mirrors the infra-dialog guard.
    import ast
    import inspect as _inspect
    import pathlib

    from dsql_migrator.ui.data_migration import _cdc_ui

    assert _inspect.iscoroutinefunction(_cdc_ui._open_cdc_start_dialog)

    tree = ast.parse(pathlib.Path(_cdc_ui.__file__).read_text(encoding="utf-8"))
    awaited = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Await)
    }
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_open_cdc_start_dialog"
    ]
    assert calls, "expected at least one _open_cdc_start_dialog call site"
    for call in calls:
        assert id(call) in awaited, "_open_cdc_start_dialog call is not awaited"


def test_cdc_infra_prep_state_classifies_the_deploy_situation() -> None:
    """The Prerequisites-step CDC section must render the right thing per situation."""
    from dsql_migrator.ui.data_migration import cdc_infra_prep_state

    # Not probed yet -> "unknown": render NOTHING. Showing a fresh-deploy form before
    # the account-wide discovery has reported risks a duplicate (billable) MSK cluster.
    fresh = DataMigrationState()
    assert fresh.cdc_stack_phase_checked is False
    assert cdc_infra_prep_state(fresh, _StubJobManager({})) == "unknown"

    # Probed, nothing found -> offer the deploy.
    empty = DataMigrationState()
    empty.set_cdc_stack_phase(None)  # sets cdc_stack_phase_checked
    assert cdc_infra_prep_state(empty, _StubJobManager({})) == "deploy"

    # Probed, another stack exists under a different name -> offer to attach.
    other = DataMigrationState()
    other.set_cdc_stack_phase(None)
    other.set_cdc_other_stacks([("mysql-dsql-cdc-old", "CREATE_COMPLETE")])
    assert cdc_infra_prep_state(other, _StubJobManager({})) == "adopt"

    # Already deployed (any live phase) -> nothing to provision here.
    for phase in ("infra", "running", "unstable", "provisioning", "partial"):
        ready = DataMigrationState()
        ready.set_cdc_stack_phase(phase)
        assert cdc_infra_prep_state(ready, _StubJobManager({})) == "ready", phase

    # An infra create in flight -> show live progress only.
    deploying = DataMigrationState()
    deploying.set_cdc_stack_phase(None)
    deploying.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("RUNNING")})
    assert cdc_infra_prep_state(deploying, mgr) == "deploying"


def test_cdc_infra_prep_state_ignores_a_connector_level_job() -> None:
    # Only an "infra" job means "provisioning here"; a start/stop job belongs to the
    # CDC sub-step's own lifecycle card and must not hijack this section.
    from dsql_migrator.ui.data_migration import cdc_infra_prep_state

    state = DataMigrationState()
    state.set_cdc_stack_phase("infra")
    state.set_cdc_deploy_job_id("start-1", kind="start")
    mgr = _StubJobManager({"start-1": _StubJob("RUNNING")})
    assert cdc_infra_prep_state(state, mgr) == "ready"


def test_cdc_infra_prep_section_is_rendered_from_the_prerequisites_substep() -> None:
    """Pin WHERE the deploy affordance lives: inside ``_prereq_body``.

    The point of moving it out of the deep CDC sub-step is that the ~15-20 min MSK
    create must be reachable BEFORE the Full Load starts, so it can overlap. If the
    call drifted into the CDC body the overlap would be lost again (for the combined
    type the CDC section is only reached after the load completes).
    """
    import ast
    import pathlib

    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(pathlib.Path(dm.__file__).read_text(encoding="utf-8"))
    bodies = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name in ("_prereq_body", "_cdc_body", "_full_load_body")
    }
    assert "_prereq_body" in bodies

    def _calls(fn):
        return {
            node.func.id
            for node in ast.walk(fn)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }

    assert "_render_cdc_infra_prep_section" in _calls(bodies["_prereq_body"])
    for other in ("_cdc_body", "_full_load_body"):
        if other in bodies:
            assert "_render_cdc_infra_prep_section" not in _calls(bodies[other])


def test_prerequisites_placement_is_skipped_for_cdc_only() -> None:
    """The Prerequisites call must be gated OFF for CDC only.

    CDC only renders the card inside the CDC step instead (there is no Full Load to
    overlap), so leaving the Prerequisites call unconditional would show a billable
    deploy form twice in the same session.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    guards = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.If)
        and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "_render_cdc_infra_prep_section"
            for c in ast.walk(node)
        )
    ]
    assert guards, "the Prerequisites render must sit behind a guard"
    condition = ast.unparse(guards[0].test)
    assert "has_cdc" in condition
    assert "CDC_ONLY" in condition, (
        f"the guard must exclude CDC only, got: {condition}"
    )


def test_cdc_step_does_not_add_a_second_infra_prep_card() -> None:
    """The CDC step must NOT call _render_cdc_infra_prep_section.

    Its lifecycle card (_render_cdc_start_action) already renders the same BYO-VPC
    deploy form -- or the adopt choice -- whenever the stack is absent, so adding a
    prep-section call there put the identical form on screen twice (observed in the UI).
    Provisioning inside the CDC step belongs to the lifecycle card alone.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    tree = ast.parse(inspect.getsource(_cdc_ui._render_cdc_step).strip())
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "_render_cdc_infra_prep_section" not in called, (
        "the CDC step already provisions via _render_cdc_start_action; calling the "
        "prep section here duplicates a billable deploy form"
    )
    # The lifecycle card, which owns provisioning inside this step, is still rendered.
    assert "_render_cdc_start_action" in called


def test_lifecycle_card_owns_provisioning_when_no_stack_exists() -> None:
    """Why the CDC step needs no separate prep card: the lifecycle card covers it.

    Pins that _render_cdc_start_action still reaches the deploy form and the adopt
    choice. If that ever moved out, CDC only would have no way to provision at all --
    its Prerequisites entry point is deliberately suppressed.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    tree = ast.parse(inspect.getsource(_cdc_ui._render_cdc_start_action).strip())
    called = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "_render_cdc_infra_deploy_action" in called
    assert "_render_cdc_adopt_or_deploy_choice" in called


def test_discovery_fingerprint_tracks_every_field_discovery_writes() -> None:
    """Change detection is only sound if the fingerprint covers all mutated fields.

    A field left out would make a real change look like "nothing happened", and the
    refresh that reveals it (e.g. the duplicate-MSK adopt guard) would be skipped.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_discovery_fingerprint

    class _S:
        pass

    state = _S()
    base = cdc_discovery_fingerprint(state)
    # Each mutation must move the fingerprint.
    for attr, value in (
        ("cdc_controller", object()),
        ("cdc_connector_names", ["src"]),
        ("cdc_connector_running_names", ["src"]),
        ("cdc_stack_phase", "running"),
        ("cdc_other_stacks", [("other-stack", "CREATE_COMPLETE")]),
        ("cdc_stack_phase_checked", True),
    ):
        fresh = _S()
        setattr(fresh, attr, value)
        assert cdc_discovery_fingerprint(fresh) != base, f"{attr} must be tracked"


def test_discovery_fingerprint_is_stable_when_nothing_changed() -> None:
    # The whole point: a re-probe that finds the same stack/connectors must compare
    # equal, so no rebuild happens and an in-flight click survives.
    from dsql_migrator.ui.data_migration._cdc_status import cdc_discovery_fingerprint

    class _S:
        cdc_connector_names = ["mysql-source", "mysql-sink"]
        cdc_connector_running_names = ["mysql-source"]
        cdc_stack_phase = "running"
        cdc_other_stacks: list = []
        cdc_stack_phase_checked = True

    assert cdc_discovery_fingerprint(_S()) == cdc_discovery_fingerprint(_S())


def test_discovery_fingerprint_compares_a_rebuilt_controller_as_unchanged() -> None:
    # The controller object is rebuilt on every probe, so comparing identity would
    # always look changed and defeat the skip entirely. Presence is what matters.
    from dsql_migrator.ui.data_migration._cdc_status import cdc_discovery_fingerprint

    class _S:
        pass

    a, b = _S(), _S()
    a.cdc_controller = object()
    b.cdc_controller = object()  # different instance, same meaning
    assert cdc_discovery_fingerprint(a) == cdc_discovery_fingerprint(b)


def test_discovery_refresh_is_conditional_on_a_real_change() -> None:
    """The reported symptom: Start / Re-run Full Load needing a second click.

    Discovery fires ~0.05s after the screen renders and used to call the full
    ``refresh()`` unconditionally when it finished, rebuilding every widget -- so a
    click in that window went to a destroyed element and was dropped. The refresh must
    now sit behind a fingerprint comparison.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    discover = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_discover_cdc"
    )
    refresh_calls = [
        node
        for node in ast.walk(discover)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "refresh"
    ]
    assert refresh_calls, "discovery must still refresh when something changed"
    # Every refresh has to be inside a conditional -- an unguarded one at the function
    # body's top level is exactly the bug.
    top_level = [
        stmt
        for stmt in discover.body
        if isinstance(stmt, ast.Expr)
        and isinstance(stmt.value, ast.Call)
        and isinstance(stmt.value.func, ast.Name)
        and stmt.value.func.id == "refresh"
    ]
    assert not top_level, (
        "refresh() must not run unconditionally after discovery -- that rebuilds the "
        "Start Full Load button and swallows an in-flight click"
    )
    guards = [
        node
        for node in ast.walk(discover)
        if isinstance(node, ast.If)
        and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "refresh"
            for c in ast.walk(node)
        )
    ]
    assert guards, "the refresh must be guarded"
    assert "cdc_discovery_fingerprint" in ast.unparse(guards[0].test), (
        "the guard must compare the discovery fingerprint"
    )


def test_cdc_discovery_is_armed_before_the_substeps_render() -> None:
    """The account probe must be armed before any sub-step body renders.

    ``cdc_infra_prep_state`` returns "unknown" until the probe reports, and the
    sub-steps render in order (Prerequisites -> Full Load -> CDC). The discovery timer
    used to be armed inside the CDC sub-step block -- i.e. AFTER Prerequisites had
    already rendered -- so on the first pass the new deploy section would be
    suppressed, or (worse, if it were not gated) offered without the duplicate-MSK
    guard populated.
    """
    import ast
    import pathlib

    import dsql_migrator.ui.data_migration as dm

    src = pathlib.Path(dm.__file__).read_text(encoding="utf-8")
    tree = ast.parse(src)
    arm_lines = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_discover_cdc"
    ]
    assert len(arm_lines) == 1, "expected exactly one _discover_cdc definition"
    substep_call_lines = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_substep"
    ]
    assert substep_call_lines, "expected _substep call sites"
    assert arm_lines[0] < min(substep_call_lines), (
        "the CDC discovery must be armed before the first _substep renders"
    )


def test_cdc_cascade_gap_tables_extracts_the_cdc_specific_finding() -> None:
    # The assessment detects FK cascades CDC cannot replicate, but that finding only
    # lived in the Evaluation report -- read BEFORE the user knew whether CDC was in
    # scope. Surface it where CDC is chosen.
    from dsql_migrator.ui.data_migration import cdc_cascade_gap_tables

    class _Item:
        def __init__(self, rule_id, object_name):
            self.rule_id = rule_id
            self.object_name = object_name

    class _Report:
        def __init__(self, items):
            self.items = items

    report = _Report(
        [
            _Item("FK_CASCADE_CDC_GAP", "shop.order_items"),
            _Item("FK_NOT_SUPPORTED", "shop.orders"),
            _Item("FK_CASCADE_CDC_GAP", "shop.addresses"),
            _Item("FK_CASCADE_CDC_GAP", "shop.order_items"),  # duplicate
        ]
    )
    assert cdc_cascade_gap_tables(report) == ["shop.addresses", "shop.order_items"]

    # Degrades quietly: no assessment yet, or an object without items.
    assert cdc_cascade_gap_tables(None) == []
    assert cdc_cascade_gap_tables(object()) == []
    assert cdc_cascade_gap_tables(_Report([])) == []


def test_prerequisite_checks_pin_the_confirmed_table_selection() -> None:
    """Running the checks must record the exact table set they covered.

    The picker locks the moment a report exists, so that set IS the migration scope --
    but it was only implied by the default when the user never touched the picker,
    leaving ``selection`` empty. Anything reading the selection then resolved to "no
    tables", which is what made a CDC deploy fired right after the checks (before any
    watermark exists) produce an empty TableIncludeList and a uniform partition plan.
    Guard the contract structurally: run_checks calls set_selection with the resolved
    names.
    """
    import ast
    import pathlib

    import dsql_migrator.ui.data_migration as dm

    tree = ast.parse(pathlib.Path(dm.__file__).read_text(encoding="utf-8"))
    checks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "run_checks"
    ]
    assert len(checks) == 1, "expected exactly one run_checks"
    setters = [
        node
        for node in ast.walk(checks[0])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "set_selection"
    ]
    assert setters, "run_checks must pin the checked table set via set_selection"


def test_cdc_tables_for_config_uses_the_pinned_selection_without_a_watermark() -> None:
    # The deploy can now fire from the Prerequisites step, i.e. BEFORE any Full Load
    # watermark exists. The connector's table set then comes from the pinned selection;
    # without it the config would say "all selected" (empty), and the topic partition
    # plan would fall back to a uniform default.
    from dsql_migrator.core.models import TableSelection
    from dsql_migrator.ui.data_migration import _cdc_tables_for_config

    state = DataMigrationState()
    inv = _inventory()
    # No watermark, untouched picker -> nothing resolvable (the old behavior).
    assert _cdc_tables_for_config(state, inv, None) == []

    # After the checks pin the confirmed set, it resolves.
    names = [t.name for t in inv.tables]
    state.set_selection(TableSelection(selected_tables=names))
    resolved = [t.name for t in _cdc_tables_for_config(state, inv, None)]
    assert resolved == names


def test_infra_stage_eta_includes_bucket_and_plugin_upload() -> None:
    # These two stages exist in CDC_INFRA_STAGES but had no estimate, so the total ETA
    # under-reported the wait -- the user's only signal during a ~15-20 min deploy that
    # is now a foreground action.
    from dsql_migrator.core.cdc_deployer import CDC_INFRA_STAGES
    from dsql_migrator.ui.data_migration._cdc_status import _CDC_STAGE_ETA_SECONDS

    etas = _CDC_STAGE_ETA_SECONDS["infra"]
    assert etas.get("ensure_bucket", 0) > 0
    assert etas.get("upload_plugins", 0) > 0
    # Every declared infra stage now carries an estimate (no silent gaps).
    for key, _label in CDC_INFRA_STAGES:
        assert etas.get(key, 0) > 0, key


def test_sidebar_run_guard_derives_prereq_mode_from_the_migration_type() -> None:
    """The sidebar Run guard must not hardcode ``MigrationMode.FULL_LOAD``.

    ``full_load_run_guard_reason`` defaults ``prereq_mode`` to FULL_LOAD, so calling
    it without the argument made the sidebar Run button look enabled for a CDC type
    whose (superset) checks had never run -- contradicting the in-content guard on the
    same screen. ``data_migration_run_guard`` is a closure inside ``build_page``, so
    the contract is pinned structurally: its call passes ``prereq_mode``.
    """
    import ast
    import pathlib

    import dsql_migrator.ui.app as app_mod

    tree = ast.parse(pathlib.Path(app_mod.__file__).read_text(encoding="utf-8"))
    guards = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
        and node.name == "data_migration_run_guard"
    ]
    assert len(guards) == 1, "expected exactly one data_migration_run_guard"
    calls = [
        node
        for node in ast.walk(guards[0])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "full_load_run_guard_reason"
    ]
    assert calls, "expected the guard to call full_load_run_guard_reason"
    for call in calls:
        kwargs = {kw.arg for kw in call.keywords}
        assert "prereq_mode" in kwargs, (
            "the sidebar guard must pass prereq_mode (derived from the migration "
            "type), not fall back to the FULL_LOAD default"
        )


def test_probe_binlog_resume_gap_skips_a_manual_start_point() -> None:
    # A manual start position overrides the watermark, so the watermark's log being
    # purged is not what CDC will resume from -- warning about it would be wrong.
    from dsql_migrator.ui.data_migration._cdc_ui import _probe_binlog_resume_gap

    state = DataMigrationState()
    state.set_cdc_start_mode("manual")
    state.set_cdc_start_position(binlog_file="mysql-bin.000099", binlog_pos=4)
    assert state.cdc_start_override() is not None

    # A job whose watermark points at a long-purged log. The manual override wins, so
    # the probe must return None -- and must do so WITHOUT touching the source (the
    # session double below would raise if it were used).
    class _ExplodingSession:
        @property
        def source_config(self):  # pragma: no cover - must never be reached
            raise AssertionError("manual start point must skip the source probe")

    job = MigrationJob(job_id="j1")
    job.watermark = _watermark()
    state.job_id = "j1"
    assert (
        _probe_binlog_resume_gap(
            state, _StubJobManager({"j1": job}), _ExplodingSession()
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


def test_cdc_only_pins_the_cdc_substep_once_infrastructure_is_ready() -> None:
    """Reported gap 1: "CDC infrastructure is ready" appeared under Prerequisites
    while Start CDC sat inside a COLLAPSED CDC section.

    No connectors exist yet at that point, so the original connectors-only pin did not
    fire and the resolver fell back to "prerequisites" -- collapsing the very section
    holding the operator's next action.
    """
    from dsql_migrator.ui.data_migration import MigrationType, should_pin_cdc_substep

    assert (
        should_pin_cdc_substep(
            migration_type=MigrationType.CDC_ONLY,
            has_connectors=False,
            infra_prep_state="ready",
        )
        is True
    )


def test_pins_the_cdc_substep_while_an_infra_create_or_teardown_is_in_flight() -> None:
    """Reported gap 2: submitting "Delete CDC infrastructure" bounced the view back to
    a collapsed Prerequisites mid-operation.

    A teardown (or create) is CDC work with no connectors, so the connectors-only pin
    missed it. Applies to the combined type too -- the operator is acting on CDC
    infrastructure either way.
    """
    from dsql_migrator.ui.data_migration import MigrationType, should_pin_cdc_substep

    for kind in ("infra", "delete", "stop"):
        for mtype in (MigrationType.CDC_ONLY, MigrationType.FULL_LOAD_AND_CDC):
            assert (
                should_pin_cdc_substep(
                    migration_type=mtype,
                    has_connectors=False,
                    infra_action_kind=kind,
                    infra_action_running=True,
                )
                is True
            ), f"{mtype} / {kind} must pin"
    # A FINISHED action must not pin -- otherwise the view is stuck on CDC forever.
    assert (
        should_pin_cdc_substep(
            migration_type=MigrationType.CDC_ONLY,
            has_connectors=False,
            infra_action_kind="delete",
            infra_action_running=False,
        )
        is False
    )


def test_combined_type_is_not_pinned_to_cdc_merely_because_infra_is_ready() -> None:
    """The regression guard on the widened pin.

    For Full load + CDC, a finished Full Load deliberately keeps its results on screen
    and the operator advances via "Continue to CDC". Pinning on infra-ready would yank
    the snapshot's row counts and watermark out of view -- the exact behaviour
    resolve_active_substep_for_type was written to avoid.
    """
    from dsql_migrator.ui.data_migration import MigrationType, should_pin_cdc_substep

    assert (
        should_pin_cdc_substep(
            migration_type=MigrationType.FULL_LOAD_AND_CDC,
            has_connectors=False,
            infra_prep_state="ready",
        )
        is False
    )
    # Connectors existing still pins for the combined type (unchanged behaviour).
    assert (
        should_pin_cdc_substep(
            migration_type=MigrationType.FULL_LOAD_AND_CDC, has_connectors=True
        )
        is True
    )


def test_full_load_only_is_never_pinned_to_a_cdc_substep_it_does_not_have() -> None:
    # Guard against pinning a sub-step that is not in the type's stepper at all.
    from dsql_migrator.ui.data_migration import MigrationType, should_pin_cdc_substep

    assert (
        should_pin_cdc_substep(
            migration_type=MigrationType.FULL_LOAD_ONLY,
            has_connectors=True,
            infra_prep_state="ready",
            infra_action_kind="delete",
            infra_action_running=True,
        )
        is False
    )


def test_render_path_pins_the_cdc_substep_via_the_shared_helper() -> None:
    """The fix only reaches users if the screen consults the helper.

    Asserted on the parse tree so a reworded comment cannot satisfy it, and pinned to
    the keywords -- passing only has_connectors would silently restore the old
    narrower behaviour with all the plumbing still in place.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "should_pin_cdc_substep"
    ]
    assert calls, "the screen must decide the CDC pin via should_pin_cdc_substep"
    kwargs = {kw.arg for kw in calls[0].keywords}
    for required in (
        "migration_type",
        "has_connectors",
        "infra_prep_state",
        "infra_action_kind",
        "infra_action_running",
    ):
        assert required in kwargs, f"{required} must be passed, or the gap reopens"


def test_ready_notice_does_not_reference_a_full_load_that_cdc_only_lacks() -> None:
    """CDC only has no Full Load, so "start streaming ... after the Full Load" names a
    step that does not exist -- it reads as an unmet prerequisite.

    Checks the source of the ready branch carries both wordings, keyed on the
    migration type, since rendering it needs the whole Prerequisites sub-step.
    """
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui._render_cdc_infra_prep_section)
    assert "Start CDC on the CDC step below" in src, (
        "the CDC-only branch must point at Start CDC, not at a Full Load"
    )
    assert "CDC_ONLY" in src, "the wording must be keyed on the migration type"


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


def test_status_badge_names_the_phase_its_status_belongs_to() -> None:
    """A bare "DONE" lies once the type selector moves.

    The badge is backed by one underlying step for every migration type, so after a
    finished Full Load a switch to CDC only left it reading as though CDC had completed
    when none had run. Naming the phase keeps the value honest.
    """
    from dsql_migrator.ui.data_migration import MigrationType, migration_status_label

    assert migration_status_label(MigrationType.FULL_LOAD_ONLY) == "Full Load"
    assert migration_status_label(MigrationType.CDC_ONLY) == "CDC"
    # Combined: the step is promoted to DONE only once CDC is genuinely live, so before
    # that the status is still describing the Full Load.
    assert migration_status_label(MigrationType.FULL_LOAD_AND_CDC) == "Full Load"
    assert (
        migration_status_label(MigrationType.FULL_LOAD_AND_CDC, cdc_streaming=True)
        == "CDC"
    )
    # CDC-only never describes the Full Load, streaming or not.
    assert (
        migration_status_label(MigrationType.CDC_ONLY, cdc_streaming=True) == "CDC"
    )


def test_cdc_only_badge_reads_the_cdc_step_not_the_shared_full_load_step() -> None:
    """The reported bug: "CDC: DONE" in a restored session where CDC never ran.

    The badge's status came from the single `full_load` workflow step every type shares,
    and the whole workflow is persisted -- so a session that had once completed a Full
    Load came back labelled "CDC" with that Full Load's DONE. Naming one phase while
    showing another's value is worse than the bare "DONE" the label replaced.
    """
    from dsql_migrator.ui.data_migration import MigrationType, migration_status_badge
    from dsql_migrator.ui.workflow import StepStatus

    label, status = migration_status_badge(
        MigrationType.CDC_ONLY,
        full_load_status=StepStatus.DONE,  # restored from an earlier Full Load
        cdc_status=StepStatus.NOT_STARTED,  # CDC genuinely never ran
    )

    assert label == "CDC"
    assert status is StepStatus.NOT_STARTED, (
        "CDC only must show the cdc step's status, not the restored full_load DONE"
    )


def test_cdc_only_badge_follows_the_cdc_step_once_streaming() -> None:
    # The other half: when CDC is actually live the badge must say so, or the fix would
    # just pin it to NOT_STARTED forever.
    from dsql_migrator.ui.data_migration import MigrationType, migration_status_badge
    from dsql_migrator.ui.workflow import StepStatus

    label, status = migration_status_badge(
        MigrationType.CDC_ONLY,
        full_load_status=StepStatus.DONE,
        cdc_status=StepStatus.IN_PROGRESS,
        cdc_streaming=True,
    )

    assert (label, status) == ("CDC", StepStatus.IN_PROGRESS)


def test_other_types_keep_reading_the_full_load_step() -> None:
    """Regression guard: only CDC only changes source.

    For Full load only, and for the combined type before CDC goes live, the label and
    the value describe the same phase -- so they must keep reading `full_load`. Reading
    the cdc step there would show NOT_STARTED for a finished Full Load.
    """
    from dsql_migrator.ui.data_migration import MigrationType, migration_status_badge
    from dsql_migrator.ui.workflow import StepStatus

    for mtype in (MigrationType.FULL_LOAD_ONLY, MigrationType.FULL_LOAD_AND_CDC):
        label, status = migration_status_badge(
            mtype,
            full_load_status=StepStatus.DONE,
            cdc_status=StepStatus.NOT_STARTED,
        )
        assert status is StepStatus.DONE, f"{mtype} must read the full_load step"
        assert label == "Full Load"


def test_badge_returns_the_status_object_so_text_and_colour_cannot_disagree() -> None:
    # The caller renders `.value` and indexes _STATUS_COLORS with the SAME object. If
    # this returned a bare string the colour would have to be derived separately and
    # could drift (green "NOT STARTED").
    from dsql_migrator.ui.data_migration import (
        _STATUS_COLORS,
        MigrationType,
        migration_status_badge,
    )
    from dsql_migrator.ui.workflow import StepStatus

    _, status = migration_status_badge(
        MigrationType.CDC_ONLY,
        full_load_status=StepStatus.DONE,
        cdc_status=StepStatus.NOT_STARTED,
    )
    assert status in _STATUS_COLORS, "the returned status must key the colour map"
    assert _STATUS_COLORS[status] != _STATUS_COLORS[StepStatus.DONE]


def test_render_path_feeds_the_badge_both_workflow_steps() -> None:
    """The fix only reaches users if the screen passes the cdc step in.

    Pinned on the parse tree: passing only full_load_status would silently restore the
    old behaviour with the helper in place.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "migration_status_badge"
    ]
    assert calls, "the screen must build the badge via migration_status_badge"
    kwargs = {kw.arg: ast.unparse(kw.value) for kw in calls[0].keywords}
    assert "full_load_status" in kwargs and "cdc_status" in kwargs
    assert "WorkflowStep.CDC" in kwargs["cdc_status"], (
        f"cdc_status must come from the cdc workflow step, got {kwargs['cdc_status']}"
    )


def test_stale_error_is_demoted_not_deleted_after_a_type_switch() -> None:
    """The reported bug: a red "Migration failed" banner beside a "Success" header.

    A Full Load that quarantined rows recorded an error, nothing cleared it on a type
    switch, and the CDC-only screen then showed three verdicts at once -- header
    "Success", status DONE, banner "Migration failed". Dropping the message would be
    worse than leaving it: it reports rows genuinely missing from the target, and CDC
    streams ongoing changes without backfilling a Full Load gap. So it is kept and
    demoted.
    """
    from dsql_migrator.ui.data_migration import MigrationType, stale_error_notice

    err = "FullLoadIncompleteError: 1 of 7 table(s) did not fully load."

    # Same type -> a live failure, unchanged.
    assert stale_error_notice(
        err,
        migration_type=MigrationType.FULL_LOAD_ONLY,
        error_migration_type=MigrationType.FULL_LOAD_ONLY,
    ) == ("error", "Migration failed", err)

    # Switched to CDC only -> demoted to a warning, re-framed, message still carried.
    tone, header, body = stale_error_notice(
        err,
        migration_type=MigrationType.CDC_ONLY,
        error_migration_type=MigrationType.FULL_LOAD_ONLY,
    )
    assert tone == "warning", "a carried-over error must not keep the error tone"
    assert "Migration failed" not in header
    assert err in body, "the original detail must survive -- the gap it reports is real"
    assert "not backfill" in body, "must say CDC will not close a Full Load gap"

    # No error at all -> no notice.
    assert (
        stale_error_notice(
            None,
            migration_type=MigrationType.CDC_ONLY,
            error_migration_type=MigrationType.FULL_LOAD_ONLY,
        )
        is None
    )
    assert (
        stale_error_notice(
            "",
            migration_type=MigrationType.CDC_ONLY,
            error_migration_type=None,
        )
        is None
    )

    # Unknown provenance (an older session) must NOT be softened -- silently demoting a
    # failure we cannot attribute would hide a real one.
    assert stale_error_notice(
        err,
        migration_type=MigrationType.CDC_ONLY,
        error_migration_type=None,
    ) == ("error", "Migration failed", err)


def test_set_error_stamps_the_migration_type_and_clear_resets_it() -> None:
    # The demotion above is only possible if provenance is recorded, so pin the stamp.
    from dsql_migrator.ui.data_migration import DataMigrationState, MigrationType

    state = DataMigrationState()
    assert state.error_migration_type is None

    state.set_migration_type(MigrationType.FULL_LOAD_ONLY)
    state.set_error("boom")
    assert state.error == "boom"
    assert state.error_migration_type is MigrationType.FULL_LOAD_ONLY

    # Switching type must NOT rewrite the stamp -- that is what makes it provenance.
    state.set_migration_type(MigrationType.CDC_ONLY)
    assert state.error_migration_type is MigrationType.FULL_LOAD_ONLY

    # A re-run clears both, so the next failure is attributed afresh.
    state.clear_outputs()
    assert state.error is None
    assert state.error_migration_type is None


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


def test_type_selector_records_a_choice_even_for_the_current_value() -> None:
    """Clicking the already-selected tile must still record an explicit choice.

    The type has a DEFAULT (Full load only), and the journey header hides its
    migration-type banner until the user has actually chosen — so clicking that tile
    is precisely how a user confirms the default. The selector used to bail out on
    "no change", which left that user with no banner at all. Confirming must not
    disturb the screen, so the sub-step reset stays scoped to a real change.
    """
    from dsql_migrator.ui.data_migration import (
        DataMigrationState,
        MigrationType,
        _render_migration_type_selector,
    )
    from dsql_migrator.ui.session import SessionConnectionState

    session = SessionConnectionState()
    state = DataMigrationState()
    state.bind_session(session)
    state.set_active_substep("full_load")
    assert session.migration_type_chosen() is False
    assert state.migration_type is MigrationType.FULL_LOAD_ONLY  # the default

    ui = _RecordingUi()
    clicks: list = []
    # Capture each tile's click handler in render order (Full load only first).
    orig_card = ui.card

    def _card(*a, **k):
        el = orig_card(*a, **k)
        el.on = lambda _evt, handler, *_a, **_k: (clicks.append(handler), el)[1]
        return el

    ui.card = _card
    _render_migration_type_selector(
        ui, state, status=StepStatus.NOT_STARTED, refresh=lambda: None, locked=False
    )
    assert clicks, "expected a click handler per tile"

    clicks[0]()  # re-select the tile that is already active
    assert session.migration_type_chosen() is True   # the choice IS recorded
    assert state.migration_type is MigrationType.FULL_LOAD_ONLY  # value unchanged
    assert state.active_substep == "full_load"  # not reset (no real change)


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


def test_migration_type_locked_while_cdc_infra_is_being_created() -> None:
    """An in-flight cdc-stack CREATE must lock the type.

    It used to be excluded because the create streams nothing. But it is a ~15-20 min
    run provisioning a BILLABLE MSK cluster, and its progress view and "Delete CDC
    infrastructure" control both live on the CDC sub-step -- which switching to Full
    load only removes, leaving the cluster building with nothing on screen.
    """
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("PENDING")})
    assert migration_type_locked(state, mgr, status=StepStatus.NOT_STARTED) is True


def test_migration_type_unlocks_once_the_infra_job_finishes() -> None:
    """The lock is scoped to the RUN, not to the infrastructure existing.

    A finished create with no connectors leaves idle infra -- a billable trade-off the
    user owns and can still change their mind about (the CDC step is reachable and offers
    Delete). Locking forever would strand them.
    """
    from dsql_migrator.ui.data_migration import DataMigrationState, migration_type_locked

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("DONE")})
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
    # A DONE table is 100% because the loader streamed it to exhaustion -- NOT because
    # rows_present matches the source ESTIMATE (which can over- or undercount).
    assert by["items"].progress_pct == 100.0
    # 40 of an ESTIMATED 50 is a 20% shortfall == exactly the sampling tolerance, so
    # it is not escalated to "incomplete" (Validation does the exact check).
    assert by["items"].complete is True
    assert by["customers"].complete is None  # not DONE -> unknown
    assert by["customers"].errors == 1
    assert by["customers"].attempts == 2


def test_done_table_is_complete_even_when_the_estimate_overcounts() -> None:
    # The regression this guards: expected_rows is the watermark's scan-free
    # information_schema ESTIMATE (InnoDB index sampling). When it OVERCOUNTS, a
    # fully-loaded table used to report e.g. "91%" and complete=False, implying rows
    # were lost. A DONE table streamed its PK keyset to exhaustion, so it is 100%.
    from dsql_migrator.ui.data_migration import build_full_load_table_rows

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "DONE", "rows_loaded": 1_000_000,
          "attempts": 1}],
        counts={"orders": 1_100_000},  # estimate 10% ABOVE the truth
    )
    (row,) = build_full_load_table_rows(job)
    assert row.progress_pct == 100.0
    assert row.complete is True


def test_done_table_still_reports_gross_shortfall_as_incomplete() -> None:
    # The tolerance must not hide real loss: a shortfall far beyond any sampling
    # error still reports incomplete, so a genuinely truncated load is surfaced.
    from dsql_migrator.ui.data_migration import (
        build_full_load_table_rows,
        full_load_completeness,
    )

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1}],
        counts={"orders": 1_000_000},  # loaded 0.01% of the table
    )
    rows = build_full_load_table_rows(job)
    assert rows[0].complete is False
    assert full_load_completeness(rows).mismatched == ["orders"]


def test_rows_exceeding_the_estimate_is_surfaced_as_normal() -> None:
    # Loading MORE rows than the scan-free estimate predicted is the common case
    # (the estimate undercounts). The progress bar caps at 100%, so the excess is
    # reported separately and the tooltip explains it is not duplicated data.
    from dsql_migrator.ui.data_migration import (
        _rows_breakdown_tooltip,
        build_full_load_table_rows,
    )

    job = _full_load_job(
        [{"chunk_id": "order_items", "status": "DONE", "rows_loaded": 3_010_557,
          "attempts": 1}],
        counts={"order_items": 2_774_078},  # the real observed estimate
    )
    (row,) = build_full_load_table_rows(job)
    assert row.complete is True
    assert row.expected_exceeded_pct == 8.5
    tip = _rows_breakdown_tooltip(row)
    assert "source rows (estimate)" in tip
    assert "8.5% above the estimate" in tip and "normal" in tip

    # No excess -> nothing extra claimed.
    job2 = _full_load_job(
        [{"chunk_id": "t", "status": "DONE", "rows_loaded": 100, "attempts": 1}],
        counts={"t": 100},
    )
    (exact,) = build_full_load_table_rows(job2)
    assert exact.expected_exceeded_pct is None
    assert "above the estimate" not in _rows_breakdown_tooltip(exact)


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


def test_migration_table_status_carries_replication_lag_ms() -> None:
    # The per-table time-based replication lag (ReplicationLagMs) is threaded onto the
    # row so the "Stream lag" column can show it (preferred over the MAX(pk) fallback).
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"],
        source_max_pk={"orders": 1000}, target_max_pk={"orders": 900},  # PK gap present
        replication_lag_ms={"orders": 8500},
    )
    assert row.replication_lag_ms == 8500
    assert row.pk_gap == 100  # PK fallback still computed, but the metric is preferred

    # Absent from the metric map -> None (column falls back to the PK leading edge).
    (row2,) = build_migration_table_status(
        ["orders"], replication_lag_ms={"customers": 1},
    )
    assert row2.replication_lag_ms is None


def test_migration_table_status_consistency_verdicts() -> None:
    from dsql_migrator.ui.data_migration import build_migration_table_status

    def verdict(src, tgt, dlq=None):
        # An EXACT source count (source_is_estimate=False): the delta is
        # authoritative, so equality-based verdicts are allowed.
        (r,) = build_migration_table_status(
            ["t"], source_counts={"t": src}, target_counts={"t": tgt},
            dlq_counts={"t": dlq} if dlq else None,
            source_is_estimate=False,
        )
        return r.consistency

    assert verdict(100, 100) == "consistent"
    assert verdict(100, 80) == "behind"      # target trails source (replicating)
    # A target exceeding even an EXACT source count reads as the stream still
    # settling -- never the old alarming "ahead" verdict, which fired constantly on
    # estimates (where an undercount makes target > source the NORMAL case).
    assert verdict(80, 100) == "behind"
    assert verdict(100, None) == "unknown"   # not yet counted
    assert verdict(100, 100, dlq=5) == "quarantined"


def test_estimate_source_never_claims_exact_equality_or_target_ahead() -> None:
    # The CDC status view's source figure is a scan-free information_schema ESTIMATE
    # (InnoDB index sampling), which routinely UNDERCOUNTS by several percent. It
    # must never drive an equality verdict: a target slightly above it is the normal
    # healthy case, not an anomaly. This is the live regression that made 8 of 11
    # healthy tables show a red/amber "target ahead" badge.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    def row(src, tgt, **kw):
        (r,) = build_migration_table_status(
            ["t"], source_counts={"t": src}, target_counts={"t": tgt}, **kw
        )
        return r

    # Estimate is the DEFAULT for this builder.
    est = row(2_774_078, 3_010_557, source_max_pk={"t": 5}, target_max_pk={"t": 5})
    assert est.source_estimate is True
    assert est.counts_comparable is False
    # Target exceeds the estimate by ~8% (real observed InnoDB sampling error) and
    # the stream has caught up -> nothing is wrong.
    assert est.consistency == "consistent"
    # in_sync is NOT determinable from an estimate (never a false negative).
    assert est.in_sync is None
    # An exact count of the same shape IS comparable.
    exact = row(2_774_078, 3_010_557, source_is_estimate=False)
    assert exact.counts_comparable is True and exact.in_sync is False


def test_estimate_tolerates_sampling_noise_but_still_reports_gross_loss() -> None:
    # A small shortfall against an ESTIMATE is statistics noise, not missing rows.
    # A GROSS shortfall (well beyond any sampling error) is still reported, so real
    # data loss is not hidden by the tolerance.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    def verdict(src, tgt, caught=True):
        pk = {"t": 100} if caught else {"t": 100}
        tgt_pk = {"t": 100} if caught else {"t": 50}
        (r,) = build_migration_table_status(
            ["t"], source_counts={"t": src}, target_counts={"t": tgt},
            source_max_pk=pk, target_max_pk=tgt_pk,
        )
        return r.consistency

    # 5% short of the estimate, leading edge caught up -> noise, not a gap.
    assert verdict(1_000_000, 950_000) == "consistent"
    # Half the table missing -> a real gap, reported despite the tolerance.
    assert verdict(1_000_000, 500_000) == "gap"
    # Grossly short AND the leading edge trails -> still catching up.
    assert verdict(1_000_000, 500_000, caught=False) == "behind"


def test_estimate_verdict_still_defers_to_dlq_and_lag_signals() -> None:
    # The trustworthy signals are unaffected by the estimate: quarantined data wins
    # over everything, and a trailing high-water PK still reads as replicating.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (quarantined,) = build_migration_table_status(
        ["t"],
        source_counts={"t": 100}, target_counts={"t": 100},
        dlq_counts={"t": 2},
    )
    assert quarantined.consistency == "quarantined"

    (behind,) = build_migration_table_status(
        ["t"],
        source_counts={"t": 1000}, target_counts={"t": 1000},
        source_max_pk={"t": 1000}, target_max_pk={"t": 400},
    )
    # Counts agree (within the estimate) but the newest rows have NOT landed.
    assert behind.stream_caught_up is False
    assert behind.consistency == "behind"


def test_cdc_applied_net_none_until_both_known() -> None:
    from dsql_migrator.ui.data_migration import build_migration_table_status

    # No Full Load job -> full_load_rows None -> cdc_applied_net None.
    (row,) = build_migration_table_status(["t"], target_counts={"t": 50})
    assert row.cdc_applied_net is None


def test_applied_ops_metric_surfaces_per_op_and_wins_net_over_count() -> None:
    # The scan-free per-op metrics (Inserts/Updates/Deletes) surface I/U/D directly
    # AND derive net (inserts - deletes), winning over target-minus-Full Load so the
    # column needs no COUNT(*) once the sink is emitting them.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1}],
        counts={"orders": 100},
    )
    # Metric: +5 inserts / 3 updates / 0 deletes -> net +5; target COUNT (137) would
    # imply +37, but the metric wins. Updates are now visible (the old net hid them).
    (row,) = build_migration_table_status(
        ["orders"], full_load_job=job,
        target_counts={"orders": 137},
        applied_ops_metric={"orders": {"inserts": 5, "updates": 3, "deletes": 0}},
    )
    assert row.cdc_applied_ops == {"inserts": 5, "updates": 3, "deletes": 0}
    assert (row.cdc_inserts, row.cdc_updates, row.cdc_deletes) == (5, 3, 0)
    assert row.cdc_applied_net == 5  # inserts - deletes, not 137 - 100

    # Net can be negative when deletes outweigh inserts.
    (neg,) = build_migration_table_status(
        ["orders"], full_load_job=job,
        applied_ops_metric={"orders": {"inserts": 2, "updates": 1, "deletes": 5}},
    )
    assert neg.cdc_applied_net == -3
    assert (neg.cdc_inserts, neg.cdc_updates, neg.cdc_deletes) == (2, 1, 5)


def test_applied_ops_metric_update_only_table_visible() -> None:
    # A table with ONLY updates surfaces the update count (net 0) rather than looking
    # idle -- the whole point of the per-op split over the old net-rows figure.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (row,) = build_migration_table_status(
        ["orders"],
        applied_ops_metric={"orders": {"inserts": 0, "updates": 12, "deletes": 0}},
    )
    assert (row.cdc_inserts, row.cdc_updates, row.cdc_deletes) == (0, 12, 0)
    assert row.cdc_applied_net == 0


def test_cdc_applied_net_falls_back_when_metric_absent() -> None:
    # No metric datapoint for the table -> per-op is None and net falls back to
    # target - Full Load.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    job = _full_load_job(
        [{"chunk_id": "orders", "status": "DONE", "rows_loaded": 100, "attempts": 1}],
        counts={"orders": 100},
    )
    (row,) = build_migration_table_status(
        ["orders"], full_load_job=job,
        target_counts={"orders": 137},
        applied_ops_metric={"customers": {"inserts": 9}},  # different table
    )
    assert row.cdc_applied_ops is None
    assert (row.cdc_inserts, row.cdc_updates, row.cdc_deletes) == (None, None, None)
    assert row.cdc_applied_net == 37  # 137 target - 100 Full Load


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
    # Without high-water PK marks, stream_caught_up is None. With an EXACT source
    # count a short target still falls back to "behind" (the conservative lag
    # reading); with an ESTIMATE a 20%-of-estimate shortfall is within sampling
    # tolerance, so it is not escalated.
    from dsql_migrator.ui.data_migration import build_migration_table_status

    (exact,) = build_migration_table_status(
        ["orders"], source_counts={"orders": 100}, target_counts={"orders": 80},
        source_is_estimate=False,
    )
    assert exact.pk_gap is None and exact.stream_caught_up is None
    assert exact.consistency == "behind"

    (est,) = build_migration_table_status(
        ["orders"], source_counts={"orders": 100}, target_counts={"orders": 80}
    )
    assert est.pk_gap is None and est.stream_caught_up is None
    assert est.consistency == "consistent"  # within the estimate's tolerance

    # A gross shortfall with no PK signal is still surfaced as replicating/behind.
    (gross,) = build_migration_table_status(
        ["orders"], source_counts={"orders": 100}, target_counts={"orders": 10}
    )
    assert gross.consistency == "behind"


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


def test_abbrev_count_keeps_small_exact_and_abbreviates_large() -> None:
    from dsql_migrator.ui.data_migration import _abbrev_count

    assert _abbrev_count(None) == "—"
    assert _abbrev_count(300) == "300"
    assert _abbrev_count(40) == "40"
    assert _abbrev_count(74747) == "74,747"           # < 100K stays exact
    assert _abbrev_count(747_476) == "747.5K"
    assert _abbrev_count(1_180_000) == "1.18M"        # < 10 -> 2 decimals
    assert _abbrev_count(33_585_832) == "33.6M"       # >= 10 -> 1 decimal
    assert _abbrev_count(2_000_000_000) == "2.00B"


def test_rows_target_source_cell_and_attempts_cell() -> None:
    from dsql_migrator.ui.data_migration import (
        FullLoadTableRow,
        _format_attempts_cell,
        _rows_breakdown_tooltip,
        _rows_target_source_cell,
    )

    row = FullLoadTableRow(
        table="orders", state="IN_PROGRESS", rows_loaded=1_180_000,
        expected_rows=33_585_832, attempts=6, errors=0,
    )
    # Merged, abbreviated "<on target> / <source>".
    assert _rows_target_source_cell(row) == "1.18M / 33.6M"
    # Exact figures live in the tooltip.
    tip = _rows_breakdown_tooltip(row)
    assert "1,180,000 on target" in tip and "33,585,832 source rows" in tip
    # Attempts alone when no errors; with a plain-language marker otherwise. "1 err"
    # read like a retry count and never hinted that the number could mean rows the
    # target will never hold.
    assert _format_attempts_cell(row) == "6"
    row_err = FullLoadTableRow(
        table="t", state="FAILED", rows_loaded=0, expected_rows=10,
        attempts=5, errors=1,
    )
    assert _format_attempts_cell(row_err) == "5 · 1 error"
    row_errs = FullLoadTableRow(
        table="t", state="FAILED", rows_loaded=0, expected_rows=10,
        attempts=5, errors=3,
    )
    assert _format_attempts_cell(row_errs) == "5 · 3 errors"
    # Quarantine is NOT repeated here: the same row's Status cell already carries a
    # "3 dropped" badge, so naming it again one column over was the same fact twice in
    # one table row. The error count still surfaces the rows.
    row_dropped = FullLoadTableRow(
        table="t", state="DONE", rows_loaded=12, expected_rows=15,
        attempts=1, errors=3, rows_quarantined=3,
    )
    assert _format_attempts_cell(row_dropped) == "1 · 3 errors"
    assert "dropped" not in _format_attempts_cell(row_dropped)


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
            # A GROSS shortfall (5 of an estimated 50) -- far beyond sampling error, so
            # it is still reported as mismatched. (A few-percent gap would not be: the
            # source figure is an estimate, not an exact count.)
            {"chunk_id": "items", "status": "DONE", "rows_loaded": 5, "attempts": 1},
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
    from dsql_migrator.ui.data_migration._cdc_status import _running_mine

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


def test_cdc_ui_awaits_async_cdc_helpers() -> None:
    # The async CDC helpers MUST be awaited at every call site. Calling
    # _open_cdc_infra_dialog / _start_cdc_infra_deploy without await leaves an
    # un-awaited coroutine (RuntimeWarning) -> the dialog never opens and the
    # "Deploy CDC infrastructure" button does nothing. (This guard originally
    # covered migration_plan.py, which had exactly that bug; that screen is retired,
    # so it now guards the module that owns the helpers and their call sites.)
    import ast
    import pathlib

    from dsql_migrator.ui.data_migration import _cdc_ui as mp

    targets = {"_open_cdc_infra_dialog", "_start_cdc_infra_deploy"}
    tree = ast.parse(pathlib.Path(mp.__file__).read_text(encoding="utf-8"))

    awaited = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Await) and isinstance(node.value, ast.Call)
    }
    # A call inside a `lambda` is a deferred callback, not an immediate invocation:
    # the coroutine is created only when the callee invokes it, and the dialog awaits
    # it there (`result = on_confirm(); if inspect.isawaitable(result): await result`).
    # Requiring `await` inside a lambda is impossible anyway (a lambda cannot be
    # async), so exempt those and only flag genuinely-immediate un-awaited calls.
    deferred = {
        id(node)
        for lam in ast.walk(tree)
        if isinstance(lam, ast.Lambda)
        for node in ast.walk(lam)
        if isinstance(node, ast.Call)
    }
    unawaited = [
        (node.func.id, node.lineno)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in targets
        and id(node) not in awaited
        and id(node) not in deferred
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

        def on_value_change(self, *_a, **_k):
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
        # Hover-only text, recorded APART from `texts`: several design rules turn on the
        # difference (guidance must not live only in a tooltip), so conflating the two
        # would make a hover-only regression indistinguishable from visible copy.
        self.tooltips: list[str] = []
        # Rendered tick boxes, kept as objects (not just their text) so a test can ask
        # whether one is actually enabled and what handler it carries.
        self.checkboxes: list = []
        # Rendered buttons, so a test can drive a click (``.on_click``) instead of only
        # asserting the label text.
        self.buttons: list = []
        # Rendered ui.table() payloads ({"rows": [...], "columns": [...]}).
        self.tables: list = []

    class _El:
        # Class-level default: subclasses below (_Btn, _Input, ...) define their own
        # __init__ without calling super(), so an instance attribute would be missing on
        # them and every .tooltip() call would raise AttributeError. A class attribute is
        # always resolvable, so a subclass that does not wire up the recorder simply
        # drops tooltips instead of breaking the render.
        _ui = None

        def __init__(self, ui=None):
            self._ui = ui

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def tooltip(self, text="", *_a, **_k):
            if text and self._ui is not None:
                self._ui.tooltips.append(str(text))
            return self

        def style(self, *_a, **_k):
            return self

        def on(self, *_a, **_k):
            return self

        def on_value_change(self, *_a, **_k):
            return self

        def bind_value(self, *_a, **_k):
            return self

        def enable(self, *_a, **_k):
            return self

        def disable(self, *_a, **_k):
            return self

        def set_enabled(self, *_a, **_k):
            return self

        def set_visibility(self, *_a, **_k):
            return self

        def set_text(self, *_a, **_k):
            return self

        def add_slot(self, *_a, **_k):
            return self

        def open(self, *_a, **_k):
            return self

        def close(self, *_a, **_k):
            # Dialogs wire Cancel/confirm to `dialog.close`, so the element the double
            # returns for ui.dialog() must expose it or the whole body fails to build.
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def _record(self, text):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self)

    def expansion(self, *_a, **_k):
        return self._El(self)

    def label(self, text="", *_a, **_k):
        return self._record(text)

    def code(self, text="", *_a, **_k):
        return self._record(text)

    def icon(self, *_a, **_k):
        return self._El(self)

    def button(self, text="", *_a, on_click=None, **_k):
        # A button's LABEL is user-visible copy, so record it like any other text --
        # it was previously dropped, which made the action a card offers unassertable.
        # The handler is kept so a test can drive the click.
        if text:
            self.texts.append(str(text))
        el = self._El(self)
        el.on_click = on_click
        self.buttons.append(el)
        return el

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def card(self, *_a, **_k):
        return self._El(self)

    # --- extras needed by the CDC infra-prep section -----------------------
    def dialog(self, *_a, **_k):
        # The confirm dialogs (Stop CDC / Delete infra) build their body inside
        # `with ui.dialog()`, so a double without this cannot render -- and their COPY is
        # exactly where the operator forms expectations, so it needs asserting.
        return self._El(self)

    def separator(self, *_a, **_k):
        return self._El(self)

    def space(self, *_a, **_k):
        return self._El(self)

    def badge(self, text="", *_a, **_k):
        return self._record(text)

    def spinner(self, *_a, **_k):
        return self._El(self)

    def timer(self, *_a, **_k):
        return self._El(self)

    def notify(self, *_a, **_k):
        return None

    def echart(self, *_a, **_k):
        return self._El(self)

    def element(self, *_a, **_k):
        return self._El(self)

    def input(self, label="", *_a, **_k):
        return self._record(label)

    def select(self, *_a, **_k):
        return self._El(self)

    class _Checkbox(_El):
        """A checkbox that remembers whether it can actually be ticked.

        Both halves of "locked" have to be observable from the rendered widget: a
        greyed box that still fires its handler is the bug this records. ``enabled``
        follows disable()/enable(), and ``on_change`` keeps whatever handler was
        wired (``None`` when suppressed).
        """

        def __init__(self, ui=None, *, label="", on_change=None):
            super().__init__(ui)
            self.label = label
            self.on_change = on_change
            self.enabled = True

        def disable(self, *_a, **_k):
            self.enabled = False
            return self

        def enable(self, *_a, **_k):
            self.enabled = True
            return self

    def table(self, *_a, rows=None, columns=None, **_k):
        # Record the ROWS, not just that a table was drawn: the DLQ record table is
        # where a "filtered count over an unfiltered list" regression would show up.
        self.tables.append({"rows": list(rows or []), "columns": list(columns or [])})
        for row in rows or []:
            for value in row.values():
                if value is not None:
                    self.texts.append(str(value))
        return self._El(self)

    def checkbox(self, text="", *_a, on_change=None, **_k):
        if text is not None:
            self.texts.append(str(text))
        box = self._Checkbox(self, label=str(text), on_change=on_change)
        self.checkboxes.append(box)
        return box

    def refreshable(self, fn):
        # NiceGUI's @ui.refreshable returns a wrapper that renders when CALLED (not at
        # decoration time) and carries a .refresh(). Mirror that: decorating must not
        # invoke the body, or closures defined after the @-line are still unbound.
        fn.refresh = lambda *_a, **_k: None
        return fn


def test_cdc_infra_prep_section_renders_the_overlap_guidance() -> None:
    """The deploy section must tell the user the create can overlap the Full Load.

    That is the whole reason it sits on the Prerequisites sub-step instead of the deep
    CDC one, so the copy has to say it -- otherwise the user still waits ~15-20 min
    before starting the snapshot.
    """
    from dsql_migrator.ui.data_migration import _render_cdc_infra_prep_section

    ui = _RecordingUi()
    state = DataMigrationState()
    state.set_cdc_stack_phase(None)  # probed, nothing found -> the deploy branch
    _render_cdc_infra_prep_section(
        ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
    )
    joined = " ".join(ui.texts)
    assert "CDC streaming infrastructure" in joined
    assert "Not deployed" in joined
    assert "WHILE your Full Load" in joined
    assert "10-15 minutes" in joined
    # Says it is skippable (the CDC step remains a valid place to deploy later).
    assert "deploy later from the CDC step" in joined


def test_cdc_infra_prep_section_deploying_tells_user_to_start_the_load() -> None:
    from dsql_migrator.ui.data_migration import _render_cdc_infra_prep_section

    ui = _RecordingUi()
    state = DataMigrationState()
    state.set_cdc_stack_phase(None)
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    # A real MigrationJob: the live-progress card reads job.chunks/created_at, so the
    # minimal _StubJob is not enough here.
    job = MigrationJob(job_id="infra-1")
    job.status = "RUNNING"
    mgr = _StubJobManager({"infra-1": job})
    _render_cdc_infra_prep_section(
        ui, state, mgr, lambda: None, inventory=None, session=None
    )
    joined = " ".join(ui.texts)
    assert "Deploying" in joined
    assert "start your Full Load now" in joined
    assert "does not hold up the snapshot" in joined


def test_cdc_infra_prep_section_renders_nothing_before_the_probe_reports() -> None:
    # Rendering a fresh-deploy form before the account-wide discovery has reported
    # would drop the duplicate-MSK guard, so the section stays silent.
    from dsql_migrator.ui.data_migration import _render_cdc_infra_prep_section

    ui = _RecordingUi()
    _render_cdc_infra_prep_section(
        ui,
        DataMigrationState(),
        _StubJobManager({}),
        lambda: None,
        inventory=None,
        session=None,
    )
    assert ui.texts == []


def test_cdc_infra_prep_section_ready_points_at_the_cdc_step() -> None:
    from dsql_migrator.ui.data_migration import _render_cdc_infra_prep_section

    ui = _RecordingUi()
    state = DataMigrationState()
    state.set_cdc_stack_name("mysql-dsql-cdc-orders")
    state.set_cdc_stack_phase("infra")
    _render_cdc_infra_prep_section(
        ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
    )
    joined = " ".join(ui.texts)
    assert "already deployed" in joined
    assert "mysql-dsql-cdc-orders" in joined
    assert "Ready" in joined


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


class _ExpansionCapturingUi(_RecordingUi):
    """Records the ui.expansion(value=, on_value_change=) so a test can assert the
    remembered open-state is applied and user toggles are written back."""

    def __init__(self) -> None:
        super().__init__()
        self.expansion_value = None
        self.on_value_change = None

    def expansion(self, *_a, value=None, on_value_change=None, **_k):
        self.expansion_value = value
        self.on_value_change = on_value_change
        return self._El()


def test_deploy_log_expansion_opens_to_remembered_state_and_writes_back() -> None:
    # The expansion must open to the caller-remembered state (not the default
    # collapsed) and write user toggles back into that same dict, so the CDC
    # panel's 5s poll rebuild does not snap an opened log shut.
    from datetime import datetime, timezone

    from dsql_migrator.ui.data_migration import _render_deploy_log

    lines = [(datetime(2026, 7, 3, 5, 12, 3, tzinfo=timezone.utc), "line")]
    log_state = {"open": True}
    ui = _ExpansionCapturingUi()
    _render_deploy_log(ui, lines, log_state)
    assert ui.expansion_value is True  # opened to remembered state

    class _Ev:  # on_value_change receives an event carrying the new value
        value = False

    ui.on_value_change(_Ev())
    assert log_state["open"] is False  # user collapse persisted to the dict


def test_cdc_deploy_log_ui_state_persists_on_migration_state() -> None:
    # The deploy-log open/closed flag lives on the session-scoped migration state
    # (a stable, mutable dict) -- NOT a local of the render function -- so a full
    # CDC-panel re-render cannot recreate it and reset the log to collapsed.
    state = DataMigrationState()
    assert state.cdc_deploy_log_ui_state == {"open": False}
    ref = state.cdc_deploy_log_ui_state
    ref["open"] = True
    assert state.cdc_deploy_log_ui_state["open"] is True  # same object, mutation sticks


def test_cdc_existing_infra_banner_surfaces_adoptable_stacks() -> None:
    # Plan-level surfacing: when CDC infra already exists in the account (under a name
    # this reset session does not target), the banner names it so the user can attach
    # from the plan instead of navigating to the deep CDC substep. Nothing when none.
    from dsql_migrator.ui.data_migration import _render_cdc_existing_infra_banner

    state = DataMigrationState()
    ui_empty = _RecordingUi()
    _render_cdc_existing_infra_banner(ui_empty, state, lambda: None)
    assert ui_empty.texts == []  # no other stacks -> renders nothing

    state.set_cdc_other_stacks([("mysql-dsql-cdc-seoul-test", "UPDATE_COMPLETE")])
    ui_found = _RecordingUi()
    _render_cdc_existing_infra_banner(ui_found, state, lambda: None)
    body = "\n".join(ui_found.texts)
    assert "Existing CDC infrastructure found" in body
    assert "mysql-dsql-cdc-seoul-test" in body


def test_cdc_infra_prep_prefers_attach_over_a_duplicate_deploy(monkeypatch) -> None:
    # The duplicate-MSK guard, re-targeted from the retired Migration plan screen to
    # the Prerequisites-step section that replaced it: when an existing CDC pipeline
    # was discovered under a DIFFERENT stack name, the section must surface the ADOPT
    # choice -- never the fresh "deploy CDC infrastructure" VPC form, which would risk
    # a second, billable Amazon MSK cluster.
    from dsql_migrator.ui.data_migration import _cdc_ui, MigrationType

    state = DataMigrationState()
    state.set_migration_type(MigrationType.FULL_LOAD_AND_CDC)
    state.set_cdc_stack_phase(None)  # default-named stack not deployed (marks probed)
    state.set_cdc_other_stacks([("mysql-dsql-cdc-seoul-test", "UPDATE_COMPLETE")])

    calls = {"adopt": 0, "deploy": 0}
    monkeypatch.setattr(
        _cdc_ui, "_render_cdc_adopt_or_deploy_choice",
        lambda *a, **k: calls.__setitem__("adopt", calls["adopt"] + 1),
    )
    monkeypatch.setattr(
        _cdc_ui, "_render_cdc_infra_deploy_action",
        lambda *a, **k: calls.__setitem__("deploy", calls["deploy"] + 1),
    )

    _cdc_ui._render_cdc_infra_prep_section(
        _RecordingUi(), state, _StubJobManager({}), lambda: None,
        inventory=None, session=object(),
    )
    assert calls["adopt"] == 1   # adopt choice surfaced
    assert calls["deploy"] == 0  # fresh-deploy form NOT shown


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


class _StartJobManager:
    """A job manager whose single job is PENDING/RUNNING."""

    def __init__(self, status: str = "RUNNING") -> None:
        self._status = status

    def get_status(self, job_id):
        class _J:
            pass

        j = _J()
        j.status = self._status
        return j


def test_migration_type_locks_while_a_connector_start_is_in_flight() -> None:
    """The reported bug: the type was switchable while Start CDC was already running.

    The existing two reasons both miss it -- the connectors do not exist yet (so
    cdc_connector_names is empty and the phase is not "running"), and on a CDC-only
    plan the full_load step is not IN_PROGRESS. The user could switch to Full load +
    CDC and watch it lock a moment later, once the connectors appeared. The start point
    and table set are committed the instant Start is pressed, so the choice must freeze
    then.
    """
    from dsql_migrator.ui.data_migration import (
        migration_type_lock_reason,
        migration_type_locked,
    )
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    state.set_cdc_stack_phase("infra")  # stack exists, no connectors yet
    state.set_cdc_deploy_job_id("job-1", kind="start")
    jm = _StartJobManager()

    reason = migration_type_lock_reason(
        state, status=StepStatus.NOT_STARTED, job_manager=jm
    )
    assert reason is not None and "starting" in reason
    assert migration_type_locked(state, jm, status=StepStatus.NOT_STARTED) is True


def test_infra_create_lock_names_the_cost_and_the_remedy() -> None:
    """The message must say WHY (billable MSK) and WHERE the controls are.

    A dead, silently-disabled tile reads as a bug -- and the remedy is not obvious here,
    because the progress and teardown controls are on a different sub-step.
    """
    from dsql_migrator.ui.data_migration import migration_type_lock_reason
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-1", kind="infra")

    reason = migration_type_lock_reason(
        state, status=StepStatus.NOT_STARTED, job_manager=_StartJobManager()
    )
    assert reason is not None
    assert "billable" in reason
    assert "Delete CDC infrastructure" in reason


def test_infra_create_does_not_count_as_streaming_started() -> None:
    """The two predicates must stay distinct.

    ``cdc_streaming_started`` answers "are CDC's inputs committed / is anything
    streaming?" and must keep EXCLUDING an infra job -- it gates things like promoting
    the step to DONE, which an infra create must never do (that once unlocked Validation
    with zero rows loaded). The new type lock asks a different question.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_streaming_started

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-1", kind="infra")

    assert cdc_streaming_started(state, _StartJobManager()) is False


def test_migration_type_lock_needs_the_job_manager_to_see_a_start() -> None:
    # Without job_manager the in-flight start is invisible -- which is exactly why the
    # render path must pass it. Pinning this keeps the omission from looking harmless.
    from dsql_migrator.ui.data_migration import migration_type_lock_reason
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    state.set_cdc_stack_phase("infra")
    state.set_cdc_deploy_job_id("job-1", kind="start")

    assert (
        migration_type_lock_reason(state, status=StepStatus.NOT_STARTED) is None
    ), "state alone cannot see the start job"
    assert (
        migration_type_lock_reason(
            state, status=StepStatus.NOT_STARTED, job_manager=_StartJobManager()
        )
        is not None
    )


def test_type_selector_gets_the_lock_reason_from_the_same_evaluation() -> None:
    """The disabled state and its explanation must come from one call.

    The selector used to recompute the reason itself, without the caller's
    job_manager -- so with the fix above the tiles would lock while showing no reason
    at all (a dead control that looks like a bug). Pinned on the parse tree.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_render_migration_type_selector"
    ]
    assert calls, "the screen must render the type selector"
    kwargs = {kw.arg: ast.unparse(kw.value) for kw in calls[0].keywords}
    assert "lock_reason" in kwargs, "the reason must be passed in, not recomputed"
    # Both derive from the same computed value, so they cannot disagree.
    assert kwargs["locked"].startswith("_type_lock_reason")
    assert kwargs["lock_reason"] == "_type_lock_reason"

    reason_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "migration_type_lock_reason"
    ]
    assert reason_calls, "the screen must compute the lock reason"
    assert "job_manager" in {kw.arg for kw in reason_calls[0].keywords}, (
        "job_manager must be passed, or an in-flight connector start is invisible"
    )


def test_migration_type_lock_reason_none_when_idle() -> None:
    from dsql_migrator.ui.data_migration import (
        migration_type_lock_reason,
        migration_type_locked,
    )
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    assert migration_type_lock_reason(state, status=StepStatus.NOT_STARTED) is None
    assert migration_type_locked(state, None, status=StepStatus.NOT_STARTED) is False


# ---------------------------------------------------------------------------
# Table-picker lock: selection_lock_reason (the pure "why is it frozen" source)
# and the rendered picker (tooltip carries the reason; editable while a report is
# just a preview). See selection_lock_reason's docstring for the design.
# ---------------------------------------------------------------------------


def _fl_report(*names: str) -> PrerequisiteReport:
    """A passing FULL_LOAD report that COVERED exactly ``names`` (one
    TARGET_SCHEMA_READY per table, matching what core/prerequisites.py emits)."""
    results = [_result(PrerequisiteCheckId.SOURCE_REACHABLE, PrerequisiteStatus.PASS)]
    for name in names:
        results.append(
            _result(
                PrerequisiteCheckId.TABLE_PRIMARY_KEY,
                PrerequisiteStatus.PASS,
                target=name,
                title="Table has a primary key",
            )
        )
        results.append(
            _result(
                PrerequisiteCheckId.TARGET_SCHEMA_READY,
                PrerequisiteStatus.PASS,
                target=name,
                title="Target schema is ready for the table",
            )
        )
    return PrerequisiteReport.build(MigrationMode.FULL_LOAD, results)


def test_selection_editable_with_a_report_but_no_committed_migration() -> None:
    # The whole point of the rewrite: a prerequisite report is a PREVIEW, not a
    # commit. The picker must stay editable while only a report exists -- locking on
    # it (the old behavior) froze the scope before any migration began, and its own
    # "re-run the checks to change it" remedy was a dead end (a re-run re-pins the
    # SAME set). Late edits are instead caught by the run guard's scope check.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = DataMigrationState()
    state.set_prereq_report(MigrationMode.FULL_LOAD, _fl_report("orders", "customers"))
    assert (
        selection_lock_reason(
            state,
            _StubJobManager({}),
            status=StepStatus.NOT_STARTED,
            migration_type=MigrationType.FULL_LOAD_ONLY,
            has_job=False,
        )
        is None
    )


def test_selection_locks_once_a_full_load_job_exists() -> None:
    # A running/finished Full Load exported against this exact set, so it is frozen.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    reason = selection_lock_reason(
        DataMigrationState(),
        _StubJobManager({}),
        status=StepStatus.IN_PROGRESS,
        migration_type=MigrationType.FULL_LOAD_ONLY,
        has_job=True,
    )
    assert reason is not None
    assert "Full Load" in reason
    assert "Start over" in reason  # the actual remedy, not "re-run the checks"


def test_selection_locks_when_step_is_done_even_if_the_job_record_is_pruned() -> None:
    # The job record can be pruned while the workflow's Full Load step stays DONE
    # across a session restore. The lock must survive that (has_job=False), keyed off
    # the DONE status -- otherwise a reconnect after a finished load re-opens the
    # picker over an already-loaded table set.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    reason = selection_lock_reason(
        DataMigrationState(),
        _StubJobManager({}),
        status=StepStatus.DONE,
        migration_type=MigrationType.FULL_LOAD_ONLY,
        has_job=False,
    )
    assert reason is not None
    assert "Full Load" in reason


def test_selection_locks_while_cdc_is_streaming() -> None:
    # The live source connector's table list is fixed; ticking cannot add/remove a
    # streamed table. The remedy is to stop CDC, not Start over.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = DataMigrationState()
    state.set_cdc_stack_phase("running")
    reason = selection_lock_reason(
        state,
        _StubJobManager({}),
        status=StepStatus.NOT_STARTED,
        migration_type=MigrationType.CDC_ONLY,
        has_job=False,
    )
    assert reason is not None
    assert "CDC is running" in reason
    assert "stop CDC" in reason


def test_selection_locks_once_cdc_infra_is_deployed_because_partitions_are_immutable() -> None:
    # THE irreversible-partition-plan lock. Kafka topic partitions are baked when the
    # topic is created at infra deploy; a table added afterwards streams on a single
    # partition, permanently. The deploy button sits on the Prerequisites step so the
    # ~15-20 min MSK create can overlap the Full Load, and cdc_streaming_started
    # deliberately excludes the in-flight infra job -- so WITHOUT this clause that
    # entire window is unguarded. Do not "simplify" this lock away.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = DataMigrationState()
    state.set_cdc_stack_name("mysql-dsql-cdc-orders")
    state.set_cdc_stack_phase("infra")  # deployed, not yet streaming
    reason = selection_lock_reason(
        state,
        _StubJobManager({}),
        status=StepStatus.NOT_STARTED,
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
        has_job=False,
    )
    assert reason is not None
    assert "partition" in reason
    assert "delete the cdc infrastructure" in reason.lower()


def test_selection_locks_while_cdc_infra_is_still_deploying() -> None:
    # The lock has to hold DURING the ~15-20 min create too, not only after it lands
    # -- that overlap window is exactly when the user is tempted to keep editing.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    mgr = _StubJobManager({"infra-1": _StubJob("RUNNING")})
    reason = selection_lock_reason(
        state,
        mgr,
        status=StepStatus.NOT_STARTED,
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
        has_job=False,
    )
    assert reason is not None
    assert "partition" in reason  # the infra-clause message, not the cdc-live one


def test_cdc_infra_lock_is_scoped_to_cdc_bearing_types() -> None:
    # A Full-load-only run must not be frozen by a CDC stack that merely exists in the
    # account (e.g. left by another migration): its topic-partition plan is irrelevant
    # to a load that will never stream.
    from dsql_migrator.ui.data_migration import selection_lock_reason

    state = DataMigrationState()
    state.set_cdc_stack_name("mysql-dsql-cdc-orders")
    state.set_cdc_stack_phase("infra")
    assert (
        selection_lock_reason(
            state,
            _StubJobManager({}),
            status=StepStatus.NOT_STARTED,
            migration_type=MigrationType.FULL_LOAD_ONLY,
            has_job=False,
        )
        is None
    )


class _PickerUi(_RecordingUi):
    """_RecordingUi that also captures every tooltip string and button label, so a
    test can assert the lock icon's tooltip and the editable-only controls."""

    def __init__(self) -> None:
        super().__init__()
        self.tooltips: list[str] = []
        self.buttons: list[str] = []

    class _El(_RecordingUi._El):
        def __init__(self, ui):
            self._ui = ui

        def tooltip(self, text="", *_a, **_k):
            if text:
                self._ui.tooltips.append(str(text))
            return self

        def __getattr__(self, _name):
            # Any other element method (tree.expand/tick, bind_value_to, add_slot,
            # on, ...) is a chainable no-op.
            return lambda *_a, **_k: self

    def _record(self, text):
        if text is not None:
            self.texts.append(str(text))
        return self._El(self)

    def icon(self, *_a, **_k):
        return self._El(self)

    def button(self, text="", *_a, **_k):
        if text:
            self.buttons.append(str(text))
        return self._El(self)

    def row(self, *_a, **_k):
        return self._El(self)

    def column(self, *_a, **_k):
        return self._El(self)

    def card(self, *_a, **_k):
        return self._El(self)

    def scroll_area(self, *_a, **_k):
        return self._El(self)

    def tree(self, *_a, **_k):
        return self._El(self)

    def input(self, label="", *_a, **_k):
        return self._El(self)


def test_render_table_selection_editable_shows_controls_and_no_lock_icon() -> None:
    from dsql_migrator.ui.data_migration import _render_table_selection

    ui = _PickerUi()
    _render_table_selection(
        ui,
        _inventory(),
        DataMigrationState(),
        ["orders", "customers"],
        target_existing=["orders"],
        on_refresh=lambda: None,
        locked=False,
        lock_reason=None,
    )
    # Editable: the filter/bulk controls render and no lock tooltip appears.
    assert "Select all" in ui.buttons
    assert "Unselect all" in ui.buttons
    assert not any("Locked" in t for t in ui.tooltips)


def test_render_table_selection_locked_shows_the_reason_on_the_tooltip() -> None:
    # The rendered lock tooltip must carry the caller's per-cause reason verbatim, not
    # the old hardcoded "re-run the checks" string (which was wrong for CDC locks).
    from dsql_migrator.ui.data_migration import _render_table_selection

    ui = _PickerUi()
    _render_table_selection(
        ui,
        _inventory(),
        DataMigrationState(),
        ["orders", "customers"],
        target_existing=["orders"],
        locked=True,
        lock_reason="Locked — CDC is running and its source connector streams a fixed set.",
        locked_selection=None,
    )
    assert (
        "Locked — CDC is running and its source connector streams a fixed set."
        in ui.tooltips
    )
    # Locked hides the editable-only controls.
    assert "Select all" not in ui.buttons
    assert not any("re-run the checks" in t.lower() for t in ui.tooltips)


def test_prereq_scope_gap_ignores_removed_tables_but_flags_added_ones() -> None:
    # Asymmetric on purpose (see prereq_scope_gap): removing a table leaves the report
    # a superset -> no gap; adding one was never checked -> a gap that must block.
    from dsql_migrator.ui.data_migration._models import prereq_scope_gap

    report = _fl_report("orders", "customers")
    # Removed one -> still fully covered.
    assert prereq_scope_gap(report, ["orders"]) == []
    # Same set -> no gap.
    assert prereq_scope_gap(report, ["orders", "customers"]) == []
    # Added one -> the new table is the gap.
    assert prereq_scope_gap(report, ["orders", "customers", "app.audit"]) == [
        "app.audit"
    ]
    # No report / a table-independent report -> "unknown", left to the absent-report
    # guards; never a false gap.
    assert prereq_scope_gap(None, ["orders"]) == []


def test_run_guard_blocks_on_a_table_added_after_the_checks_but_not_on_a_removal() -> None:
    # End-to-end through the run guard: an ADDED table (never saw TARGET_SCHEMA_READY,
    # and run_full_load fails the whole job on any per-table failure) blocks; a
    # REMOVED table does not (still a superset).
    state = DataMigrationState()
    state.set_prereq_report(MigrationMode.FULL_LOAD, _fl_report("orders", "customers"))

    # Removed 'customers' -> still runnable.
    state.set_selection(TableSelection(selected_tables=["orders"]))
    assert full_load_run_guard_reason(state, _inventory()) is None

    # Added 'app.audit' -> blocked, and the message names the unchecked table.
    state.set_selection(
        TableSelection(selected_tables=["orders", "customers", "app.audit"])
    )
    reason = full_load_run_guard_reason(state, _inventory())
    assert reason is not None
    assert "app.audit" in reason
    assert "never checked" in reason


def test_lob_exclusion_scope_gap_flags_newly_excluded_but_not_unexcluded() -> None:
    # Asymmetric, mirroring prereq_scope_gap: excluding a column AFTER the checks is a
    # gap (it could flip loadability to FAIL unseen); un-excluding one only adds a
    # checked column back, so it is not a gap.
    from dsql_migrator.ui.data_migration._models import lob_exclusion_scope_gap

    checked = {"orders": frozenset({"notes"})}
    # Same selection -> no gap.
    assert lob_exclusion_scope_gap(checked, {"orders": frozenset({"notes"})}) == []
    # Un-excluded 'notes' -> not a gap (column added back to the checked load).
    assert lob_exclusion_scope_gap(checked, {}) == []
    # Newly excluded 'blob' (a different table too) -> both are gaps, sorted+qualified.
    assert lob_exclusion_scope_gap(
        checked, {"orders": frozenset({"notes", "blob"}), "docs": frozenset({"body"})}
    ) == ["docs.body", "orders.blob"]


def test_run_guard_blocks_when_a_column_is_excluded_after_the_checks() -> None:
    # End-to-end: the checks ran with no exclusion (report PASSes); excluding a column
    # afterwards must block the run and name the column, because the reduced column set
    # was never re-verified against the target's requirements. Un-excluding is fine.
    state = DataMigrationState()
    state.set_selection(TableSelection(selected_tables=["orders"]))
    # Checks ran against NO exclusion.
    state.set_prereq_report(MigrationMode.FULL_LOAD, _fl_report("orders"))
    assert full_load_run_guard_reason(state, _inventory()) is None

    # Exclude a column after the checks -> blocked, message names it.
    state.set_lob_exclusion("orders", "big_blob", True)
    reason = full_load_run_guard_reason(state, _inventory())
    assert reason is not None
    assert "orders.big_blob" in reason
    assert "excluded after the checks" in reason

    # Re-running the checks with the exclusion in place clears the gap.
    state.set_prereq_report(MigrationMode.FULL_LOAD, _fl_report("orders"))
    assert full_load_run_guard_reason(state, _inventory()) is None

    # Removing the exclusion after that is not a gap either (column added back).
    state.set_lob_exclusion("orders", "big_blob", False)
    assert full_load_run_guard_reason(state, _inventory()) is None


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

    def button(self, text="", *_a, on_click=None, **_k):
        # A button's LABEL is user-visible copy, so record it like any other text --
        # dropping it made the action a panel offers unassertable (same reason the
        # card double records it).
        if text:
            self.texts.append(str(text))
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


def _cdc_view_with_drift(groups):
    from dsql_migrator.core.cdc import build_cdc_status_view
    from dsql_migrator.core.models import ErrorLogSummary, SchemaDriftSummary

    total = sum(c for _, _, c in groups)
    summary = ErrorLogSummary(
        total_errors=total, errors_by_table={t: c for t, _, c in groups}
    )
    drift = [SchemaDriftSummary(table=t, kind=k, count=c) for t, k, c in groups]
    return build_cdc_status_view([], summary, dlq_depth=total, schema_drift=drift)


def test_render_cdc_dlq_panel_shows_schema_drift_banner() -> None:
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_cdc_dlq_panel

    state = DataMigrationState()
    state.job_id = "job-cdc"
    ui = _DlqUi()
    _render_cdc_dlq_panel(
        ui,
        state,
        _JobJM(MigrationJob(job_id="job-cdc")),
        _cdc_view_with_drift([("orders", "add-column", 3)]),
        on_refresh=None,
    )
    # The drift band names the change, the affected table, and the manual runbook
    # (CDC does not replicate DDL) so the operator is not left reverse-engineering it.
    assert any("Source schema change detected" in t for t in ui.texts)
    assert any("orders" in t and "column added" in t for t in ui.texts)
    assert any("does not replicate DDL" in t for t in ui.texts)


def test_render_cdc_dlq_panel_no_drift_banner_when_none() -> None:
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_cdc_dlq_panel

    state = DataMigrationState()
    state.job_id = "job-cdc"
    ui = _DlqUi()
    # depth 1 but no classified drift -> DLQ panel renders, drift band does NOT.
    _render_cdc_dlq_panel(
        ui,
        state,
        _JobJM(MigrationJob(job_id="job-cdc")),
        _cdc_view_with_dlq(1),
        on_refresh=None,
    )
    assert any("Dead-letter queue" in t for t in ui.texts)
    assert not any("Source schema change detected" in t for t in ui.texts)


def _drift_session():
    """A minimal session double: the fix action only needs the two configs present."""

    class _S:
        source_config = object()
        target_config = object()
        source_password = None
        aws_profile = None

    return _S()


def test_drift_banner_offers_the_add_column_fix_only_when_a_session_is_wired() -> None:
    # The opt-in ADD COLUMN recovery needs the source + target connections, which
    # live on the session. With no session (e.g. a bare render) the banner must
    # still show the drift + manual runbook, just without the action.
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_cdc_dlq_panel

    def _render(session):
        ui = _DlqUi()
        state = DataMigrationState()
        state.job_id = "job-cdc"
        _render_cdc_dlq_panel(
            ui,
            state,
            _JobJM(MigrationJob(job_id="job-cdc")),
            _cdc_view_with_drift([("orders", "add-column", 3)]),
            on_refresh=None,
            session=session,
        )
        return ui

    without = _render(None)
    assert any("Source schema change detected" in t for t in without.texts)
    assert not any("Fix target schema" in t for t in without.texts)

    with_session = _render(_drift_session())
    assert any("Fix target schema" in t for t in with_session.texts)


def test_drift_banner_offers_no_fix_for_drop_or_type_change() -> None:
    # ADD COLUMN is the only ADDITIVE drift, so it is the only one we offer to
    # repair. A source DROP or an incompatible type change can rewrite or destroy
    # target data, so those stay alert-only (the runbook tells the operator to stop
    # CDC first) -- offering a one-click fix there would be unsafe.
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_cdc_dlq_panel

    for kind in ("drop-column", "type-change"):
        ui = _DlqUi()
        state = DataMigrationState()
        state.job_id = "job-cdc"
        _render_cdc_dlq_panel(
            ui,
            state,
            _JobJM(MigrationJob(job_id="job-cdc")),
            _cdc_view_with_drift([("orders", kind, 2)]),
            on_refresh=None,
            session=_drift_session(),
        )
        assert any("Source schema change detected" in t for t in ui.texts), kind
        assert not any("Fix target schema" in t for t in ui.texts), kind


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
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

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
    from dsql_migrator.ui.data_migration._full_load_engine import _finalize_run, _RunCounts

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
    from dsql_migrator.ui.data_migration._full_load_engine import (
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
    from dsql_migrator.ui.data_migration._full_load_engine import (
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
    from dsql_migrator.ui.data_migration._full_load_engine import (
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
    from dsql_migrator.ui.data_migration._full_load_engine import _finalize_run, _RunCounts

    # Nothing failed/quarantined => completes regardless of the flag.
    _finalize_run(
        _FinalizeHandleStub(),
        "j5",
        ["t"],
        _RunCounts(real_failed=0, quarantined=0),
        ErrorLogStore(),
        accept_quarantined_rows=False,
    )


# --- identity-sequence sync gate on _finalize_run --------------------------
# The sequence sync must run on EVERY completed load (clean OR accepted-quarantine),
# because quarantined rows are permanently dropped so MAX(pk) is final; it must NOT
# run on a partial/failed load, whose retries could still fill the gap.


class _SyncSpy:
    """Records the (table_names) each sync call was handed; returns a canned result."""

    def __init__(self, result=None):
        self.calls: list = []
        self._result = result or {}

    def __call__(self, table_names, *, connection_factory):
        self.calls.append(list(table_names))
        return dict(self._result)


def _sentinel_inputs():
    # _log_identity_sequence_sync only needs a non-None ``inputs``; with an injected
    # ``sync`` the DsqlConnector factory closure is never invoked, so no real config
    # is required. A bare object stands in for DataMigrationInputs here.
    return object()


def test_finalize_run_accepted_quarantine_syncs_identity_sequence() -> None:
    # THE BUG: an accepted-quarantine run is a COMPLETED load, so its identity
    # sequence must be advanced past MAX(pk) -- previously this branch returned without
    # syncing, leaving the sequence at nextval=1 and colliding after cut-over.
    from dsql_migrator.ui.data_migration._full_load_engine import _finalize_run, _RunCounts

    log = ErrorLogStore()
    _record_quarantine(log, "jq", 1)
    spy = _SyncSpy(result={"order_items": 1504})
    _finalize_run(
        _FinalizeHandleStub(),
        "jq",
        ["order_items"],
        _RunCounts(real_failed=0, quarantined=1),
        log,
        accept_quarantined_rows=True,
        inputs=_sentinel_inputs(),
        sync_sequences=spy,
    )
    assert spy.calls == [["order_items"]]


def test_finalize_run_clean_still_syncs_identity_sequence() -> None:
    # Regression guard: the clean path must keep syncing (no change).
    from dsql_migrator.ui.data_migration._full_load_engine import _finalize_run, _RunCounts

    spy = _SyncSpy()
    _finalize_run(
        _FinalizeHandleStub(),
        "jc",
        ["t"],
        _RunCounts(real_failed=0, quarantined=0),
        ErrorLogStore(),
        accept_quarantined_rows=False,
        inputs=_sentinel_inputs(),
        sync_sequences=spy,
    )
    assert spy.calls == [["t"]]


def test_finalize_run_real_failure_does_not_sync_identity_sequence() -> None:
    # A partial/failed load must NOT sync: a later retry can still add rows, so MAX(pk)
    # is not yet final and syncing off it could let the remaining rows collide.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        FullLoadIncompleteError,
        _finalize_run,
        _RunCounts,
    )

    log = ErrorLogStore()
    _record_failure(log, "jf")
    spy = _SyncSpy()
    with pytest.raises(FullLoadIncompleteError):
        _finalize_run(
            _FinalizeHandleStub(),
            "jf",
            ["t"],
            _RunCounts(real_failed=1, quarantined=0),
            log,
            accept_quarantined_rows=True,
            inputs=_sentinel_inputs(),
            sync_sequences=spy,
        )
    assert spy.calls == []


def test_finalize_run_quarantine_not_accepted_does_not_sync() -> None:
    # Not accepted => the run RAISES (incomplete), so nothing is "completed" to sync.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        FullLoadIncompleteError,
        _finalize_run,
        _RunCounts,
    )

    log = ErrorLogStore()
    _record_quarantine(log, "jn", 1)
    spy = _SyncSpy()
    with pytest.raises(FullLoadIncompleteError):
        _finalize_run(
            _FinalizeHandleStub(),
            "jn",
            ["t"],
            _RunCounts(real_failed=0, quarantined=1),
            log,
            accept_quarantined_rows=False,
            inputs=_sentinel_inputs(),
            sync_sequences=spy,
        )
    assert spy.calls == []


# --- sync_identity_sequences_for_tables (the accept-after-load entrypoint) --
# The accept action happens AFTER _finalize_run, so a quarantined load that is only
# accepted later never hit the load-time sync. This config-based helper is what the
# "Accept quarantined rows & continue" button calls to close that gap.


def test_sync_identity_sequences_for_tables_syncs_and_filters_non_identity() -> None:
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        sync_identity_sequences_for_tables,
    )

    target = TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )
    spy = _SyncSpy(result={"order_items": 1504, "product_media": None})
    out = sync_identity_sequences_for_tables(
        target, ["order_items", "product_media"], sync=spy
    )
    # Handed the tables; the connector factory (closure) is never invoked with a fake sync.
    assert spy.calls == [["order_items", "product_media"]]
    # The raw result is returned (the log-outcome helper drops the None internally).
    assert out == {"order_items": 1504, "product_media": None}


def test_sync_identity_sequences_for_tables_never_raises_and_no_tables_is_noop() -> None:
    from dsql_migrator.core.models import TargetConnectionConfig
    from dsql_migrator.ui.data_migration._full_load_engine import (
        sync_identity_sequences_for_tables,
    )

    target = TargetConnectionConfig(
        cluster_endpoint="c.dsql.us-east-1.on.aws", region="us-east-1"
    )

    # No tables -> the sync is NOT invoked at all (no needless connection), empty result.
    no_tables_spy = _SyncSpy()
    assert sync_identity_sequences_for_tables(target, [], sync=no_tables_spy) == {}
    assert no_tables_spy.calls == []

    # A sync that raises is swallowed (best-effort follow-up), returns empty.
    def _raise(names, *, connection_factory):
        raise RuntimeError("connection refused")

    assert sync_identity_sequences_for_tables(target, ["t"], sync=_raise) == {}


def test_accept_quarantine_syncs_identity_over_migration_scope(monkeypatch) -> None:  # noqa: ANN001
    """Clicking "Accept quarantined rows & continue" must trigger the identity sync.

    This is the reported gap: the load ran with accept=False (nothing accepted yet), so
    _finalize_run skipped the sync; then the user accepted, which marked the step DONE
    but never synced -- leaving nextval=1. The accept handler now submits a background
    sync over the migration scope, keyed off the CURRENT target MAX(pk).
    """
    from dsql_migrator.ui import data_migration as dm

    calls: list = []
    monkeypatch.setattr(
        dm,
        "sync_identity_sequences_for_tables",
        lambda target_config, table_names, **kw: calls.append(list(table_names)),
    )
    # The handler is a closure; assert the wiring at the source level (the double
    # swallows NiceGUI, so a full render is not exercised here) -- the executable
    # behaviour of the sync itself is covered by the two tests above.
    import inspect

    src = inspect.getsource(dm.build_data_migration_screen)
    assert "sync_identity_sequences_for_tables(" in src
    assert "def _sync_after_accept" in src
    # It runs as a background job (target write, not on the click thread) and is gated
    # on having a target + a non-empty migration scope.
    assert "job_manager.submit(_sync_after_accept)" in src
    assert "if target_config is not None and selected_names:" in src


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


def test_apply_cdc_status_merges_applied_ops_and_never_wipes_on_empty() -> None:
    # Cumulative I/U/D counters must not flicker ("appears then disappears"): a
    # non-empty read MERGES into the last-known map (per table), and an EMPTY read
    # (flaky poll / metrics momentarily unavailable) KEEPS the prior values instead
    # of blanking the columns. A partial read (some tables missing) also keeps the
    # absent tables via merge.
    from dsql_migrator.ui.data_migration import _apply_cdc_status

    state = DataMigrationState()
    state.set_cdc_stack_name("stk")

    def fetched(ops):
        # (statuses, health, dlq_errors, applied_ops, lag_ms, lag_series)
        return ([], {}, [], ops, {}, [])

    # First non-empty read populates.
    _apply_cdc_status(state, fetched({"orders": {"inserts": 5, "updates": 2, "deletes": 1}}))
    assert state.cdc_applied_ops_by_table["orders"] == {"inserts": 5, "updates": 2, "deletes": 1}

    # An EMPTY read must NOT wipe -> values persist (kills the flicker).
    _apply_cdc_status(state, fetched({}))
    assert state.cdc_applied_ops_by_table["orders"] == {"inserts": 5, "updates": 2, "deletes": 1}

    # A later read updates orders AND adds customers (merge keeps both).
    _apply_cdc_status(state, fetched({
        "orders": {"inserts": 9, "updates": 4, "deletes": 1},
        "customers": {"inserts": 3, "updates": 0, "deletes": 0},
    }))
    assert state.cdc_applied_ops_by_table["orders"]["inserts"] == 9
    assert state.cdc_applied_ops_by_table["customers"]["inserts"] == 3

    # A partial read that omits customers keeps it via merge; orders still advances.
    _apply_cdc_status(state, fetched({"orders": {"inserts": 10, "updates": 4, "deletes": 1}}))
    assert state.cdc_applied_ops_by_table["orders"]["inserts"] == 10
    assert "customers" in state.cdc_applied_ops_by_table  # not dropped by the partial read


# ---------------------------------------------------------------------------
# Source connection drop (Aurora failover) during Full Load: automatic re-read
# ---------------------------------------------------------------------------


class _RetryHandle:
    """JobHandle stand-in exposing only what the source-retry path reads."""

    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled


def _lost_connection(code: int = 2013) -> Exception:
    class OperationalError(Exception):
        pass

    return OperationalError(code, "Lost connection to MySQL server during query")


class _FlakySourceMigrator:
    """Fails the first ``fail_times`` loads with a source drop, then succeeds.

    Records each attempt's ``pre_recreated`` so a test can assert a retry does NOT
    keep claiming the target is freshly-emptied.
    """

    def __init__(self, *, fail_times: int, error: Exception | None = None) -> None:
        self._fail_times = fail_times
        self._error = error or _lost_connection()
        self.attempts = 0
        self.pre_recreated_seen: list[bool] = []

    def migrate_table(self, table, *, on_rows=None, should_cancel=None,
                      pre_recreated=False):
        self.attempts += 1
        self.pre_recreated_seen.append(pre_recreated)
        if self.attempts <= self._fail_times:
            raise self._error
        from dsql_migrator.ui.data_migration._full_load_engine import TableLoadResult

        return TableLoadResult(rows_loaded=7)


def _no_backoff(monkeypatch) -> None:
    """Make the retry backoff zero so the tests don't actually sleep."""
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    monkeypatch.setattr(engine._time, "sleep", lambda _s: None)


def test_source_drop_retries_the_table_and_succeeds(monkeypatch) -> None:
    # An Aurora failover mid-load must be recovered automatically: the table is
    # re-read from a fresh snapshot instead of failing and waiting for a human.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _migrate_table_with_source_retry,
    )

    _no_backoff(monkeypatch)
    migrator = _FlakySourceMigrator(fail_times=1)
    result = _migrate_table_with_source_retry(
        migrator, _tables()[0], on_rows=lambda *_a, **_k: None,
        handle=_RetryHandle(),
    )
    assert migrator.attempts == 2  # failed once, then re-read successfully
    assert result.rows_loaded == 7


def test_source_drop_gives_up_after_the_configured_attempts(monkeypatch) -> None:
    # The budget is bounded: a source that never comes back surfaces the real error
    # (so the table is FAILED and retryable by hand) instead of looping forever.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _migrate_table_with_source_retry,
    )

    _no_backoff(monkeypatch)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS", "3")
    migrator = _FlakySourceMigrator(fail_times=99)
    with pytest.raises(Exception) as excinfo:
        _migrate_table_with_source_retry(
            migrator, _tables()[0], on_rows=lambda *_a, **_k: None,
            handle=_RetryHandle(),
        )
    assert "Lost connection" in str(excinfo.value)
    assert migrator.attempts == 3  # exactly the configured budget


def test_data_error_is_not_retried(monkeypatch) -> None:
    # A schema/data error fails identically forever, so retrying only adds delay.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _migrate_table_with_source_retry,
    )

    _no_backoff(monkeypatch)

    class _Unknown(Exception):
        pass

    migrator = _FlakySourceMigrator(
        fail_times=99, error=_Unknown(1054, "Unknown column 'x' in 'field list'")
    )
    with pytest.raises(Exception):
        _migrate_table_with_source_retry(
            migrator, _tables()[0], on_rows=lambda *_a, **_k: None,
            handle=_RetryHandle(),
        )
    assert migrator.attempts == 1  # no retry


def test_user_stop_is_not_retried(monkeypatch) -> None:
    # A cooperative stop is not a failure: it must propagate immediately.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _FullLoadStopped,
        _migrate_table_with_source_retry,
    )

    _no_backoff(monkeypatch)

    class _Stopping:
        def __init__(self):
            self.attempts = 0

        def migrate_table(self, table, **_kw):
            self.attempts += 1
            raise _FullLoadStopped()

    migrator = _Stopping()
    with pytest.raises(_FullLoadStopped):
        _migrate_table_with_source_retry(
            migrator, _tables()[0], on_rows=lambda *_a, **_k: None,
            handle=_RetryHandle(),
        )
    assert migrator.attempts == 1


def test_retry_aborts_promptly_when_the_user_stops_mid_backoff(monkeypatch) -> None:
    # A Stop during the failover wait must not be ignored for the whole backoff.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _migrate_table_with_source_retry,
    )
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    handle = _RetryHandle()
    # The user presses Stop while the retry is sleeping.
    monkeypatch.setattr(
        engine._time, "sleep", lambda _s: setattr(handle, "cancelled", True)
    )
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS", "60")
    migrator = _FlakySourceMigrator(fail_times=99)
    with pytest.raises(Exception):
        _migrate_table_with_source_retry(
            migrator, _tables()[0], on_rows=lambda *_a, **_k: None, handle=handle,
        )
    assert migrator.attempts == 1  # aborted during the wait, never re-read


def test_retry_disabled_by_config_fails_immediately(monkeypatch) -> None:
    # attempts=1 means "no retry" -- the documented way to keep the old behavior.
    from dsql_migrator.ui.data_migration._full_load_engine import (
        _migrate_table_with_source_retry,
    )

    _no_backoff(monkeypatch)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS", "1")
    migrator = _FlakySourceMigrator(fail_times=99)
    with pytest.raises(Exception):
        _migrate_table_with_source_retry(
            migrator, _tables()[0], on_rows=lambda *_a, **_k: None,
            handle=_RetryHandle(),
        )
    assert migrator.attempts == 1


def test_in_process_retry_drops_pre_recreated_on_a_retry(monkeypatch) -> None:
    # ``pre_recreated`` says the parent already emptied the target, which is what
    # licenses a plain INSERT. After a failed attempt has written rows, that is no
    # longer true -- so a retry must NOT keep the flag, or the re-read would collide
    # with its own rows. (Regression guard for the multiprocess load path.)
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    _no_backoff(monkeypatch)
    migrator = _FlakySourceMigrator(fail_times=1)
    state = {"first": True}

    def _load():
        pre = True and state["first"]
        state["first"] = False
        return migrator.migrate_table(
            _tables()[0], on_rows=None, should_cancel=lambda: False,
            pre_recreated=pre,
        )

    engine._retry_source_drops_in_process(
        _load, cancelled=lambda: False, table_name="orders"
    )
    assert migrator.pre_recreated_seen == [True, False]


def test_in_process_retry_classifies_like_the_single_process_path(monkeypatch) -> None:
    # The multiprocess path is the default for a large migration, so it must recover
    # from the same failover the single-process path does -- and likewise refuse to
    # retry a data error (a run must not behave differently per worker mode).
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    _no_backoff(monkeypatch)
    calls = {"n": 0}

    def _flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise _lost_connection(2006)
        return "ok"

    assert engine._retry_source_drops_in_process(
        _flaky, cancelled=lambda: False, table_name="t"
    ) == "ok"
    assert calls["n"] == 2

    permanent = {"n": 0}

    def _permanent():
        permanent["n"] += 1
        raise ValueError("bad column type")

    with pytest.raises(ValueError):
        engine._retry_source_drops_in_process(
            _permanent, cancelled=lambda: False, table_name="t"
        )
    assert permanent["n"] == 1


def test_failed_table_error_message_explains_a_dropped_source(monkeypatch) -> None:
    # Part A: once the retries are exhausted, the recorded per-table error carries the
    # operator explanation, not just the raw driver text.
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration._full_load_engine import _migrate_one_table

    _no_backoff(monkeypatch)
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_ATTEMPTS", "1")

    class _Handle:
        cancelled = False

        def update(self, _fn):
            pass

    error_log = ErrorLogStore()
    outcome = _migrate_one_table(
        _Handle(), "job-1", _tables()[0],
        _FlakySourceMigrator(fail_times=99), error_log,
    )
    assert outcome.name == "FAILED"
    (record,) = error_log.records("job-1")
    assert "Lost connection" in record.message  # the real cause is kept
    assert "failover" in record.message.lower()  # plus what it means
    assert "idempotent" in record.message.lower()  # plus why re-running is safe


# ---------------------------------------------------------------------------
# Connection management: an abandoned source stream must not stay open
# ---------------------------------------------------------------------------


class _TrackingStream:
    """A generator-like source stream that records when it is closed.

    Mirrors ``stream_converted_rows``: it holds a "connection" until closed. Used to
    prove the retry path releases the DEAD stream before waiting out a failover
    instead of pinning it (and then opening a second one).
    """

    def __init__(self, log: list, name: str) -> None:
        self._log = log
        self._name = name
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        raise _lost_connection()

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._log.append(self._name)


def test_retry_releases_the_dead_source_stream_before_waiting(monkeypatch) -> None:
    # The leak this guards: the row streams are generators whose source engine is
    # disposed in their own `finally`, so an abandoned one holds its MySQL connection
    # until closed/collected -- and the raising frame keeps it referenced. If the
    # retry waited with it still open, a 16x8 fan-out would DOUBLE the source
    # connection count exactly when a just-promoted Aurora writer is most fragile.
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    closed: list[str] = []
    sleeps: list[float] = []
    # Record the order of (close, sleep) so the release is proven to precede the wait.
    order: list[str] = []
    monkeypatch.setattr(
        engine._time, "sleep", lambda s: (sleeps.append(s), order.append("sleep"))
    )
    monkeypatch.setenv("DSQL_MIGRATOR_FULL_LOAD_SOURCE_RETRY_BACKOFF_SECONDS", "3")

    streams: list[_TrackingStream] = []
    calls = {"n": 0}

    def _work():
        calls["n"] += 1
        stream = _TrackingStream(closed, f"stream{calls['n']}")
        streams.append(stream)
        if calls["n"] == 1:
            raise _lost_connection()
        return "ok"

    def _release():
        for s in streams:
            if not s.closed:
                s.close()
                order.append("close")

    assert engine._retry_source_drops_in_process(
        _work, cancelled=lambda: False, table_name="orders", release=_release
    ) == "ok"
    # The failed attempt's stream was closed...
    assert closed == ["stream1"]
    # ...and closed BEFORE the backoff wait began (not after it).
    assert order[0] == "close"
    assert "sleep" in order
    assert sleeps  # the wait did happen


def test_migrate_table_closes_row_streams_on_failure() -> None:
    # Every caller benefits: migrate_table closes the streams it created when the
    # load raises, so the source connection is released as the exception leaves.
    import dsql_migrator.ui.data_migration._full_load_engine as engine

    closed: list[str] = []
    rows = _TrackingStream(closed, "rows")
    shards = [_TrackingStream(closed, "shard0"), _TrackingStream(closed, "shard1")]

    engine._close_row_streams(rows, shards)
    assert sorted(closed) == ["rows", "shard0", "shard1"]

    # Idempotent + exception-safe: a stream whose close() raises must not mask the
    # original failure, and a None/closeless value is ignored.
    class _Angry:
        def close(self):
            raise RuntimeError("close failed")

    engine._close_row_streams(_Angry(), [None, object()])  # must not raise
    engine._close_row_streams(None, None)  # must not raise


def test_too_many_connections_is_transient_with_its_own_advice() -> None:
    # A failover makes every reader reconnect at once, so the source can hit its
    # connection cap. That IS worth retrying (slots drain as readers finish), but the
    # operator advice differs: reduce concurrency / raise the limit, not "just wait".
    from dsql_migrator.core.introspector import (
        is_source_transient_error,
        source_error_hint,
    )

    class OperationalError(Exception):
        pass

    too_many = OperationalError(1040, "Too many connections")
    assert is_source_transient_error(too_many) is True
    hint = source_error_hint(too_many)
    assert hint is not None
    assert "connection limit" in hint.lower()
    assert "TABLE_PARALLELISM" in hint  # names the knob that actually helps
    assert "max_connections" in hint
    # It must NOT be the generic failover text (that advice would not fix this).
    assert "failover" not in hint.lower()

    # The per-user variant classifies the same way.
    assert is_source_transient_error(OperationalError(1203, "User has exceeded")) is True

    # A failover still gets the failover hint, not this one.
    failover_hint = source_error_hint(_lost_connection())
    assert failover_hint is not None and "failover" in failover_hint.lower()


# ---------------------------------------------------------------------------
# CDC lifecycle actions log their OUTCOME (not just "started")
# ---------------------------------------------------------------------------


class _RecordingHandle:
    def __init__(self, cancelled: bool = False) -> None:
        self.cancelled = cancelled


def _capture_cdc_events(monkeypatch) -> list:
    """Patch the CDC activity-log anchor and return the captured event list."""
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    events: list = []

    def _fake(action, *, detail=None, status=None):
        events.append((action, getattr(status, "value", status), detail))

    monkeypatch.setattr(cdc_ui, "_log_cdc_event", _fake)
    return events


def test_cdc_lifecycle_logs_success_with_elapsed(monkeypatch) -> None:
    # The gap this closes: the submit site logs only STARTED, so the audit trail
    # could not answer "did the Stop before cut-over succeed, and when?".
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    events = _capture_cdc_events(monkeypatch)
    ran: list = []
    wrapped = cdc_ui._logged_cdc_lifecycle(
        "stop CDC connectors", detail="stack s1", work=lambda h: ran.append(h)
    )
    handle = _RecordingHandle()
    wrapped(handle)

    assert ran == [handle]  # the real work still runs, unchanged
    (action, status, detail) = events[-1]
    assert action == "stop CDC connectors"
    assert status == "success"
    assert "stack s1" in detail and "completed in" in detail


def test_cdc_lifecycle_logs_failure_and_reraises(monkeypatch) -> None:
    # A failed Start/Stop must be visible in the log AND still fail the job (the
    # JobManager marks FAILED off the raised exception).
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    events = _capture_cdc_events(monkeypatch)

    def _boom(_handle):
        raise RuntimeError("CFN update failed")

    wrapped = cdc_ui._logged_cdc_lifecycle(
        "start CDC connectors", detail="stack s1", work=_boom
    )
    with pytest.raises(RuntimeError, match="CFN update failed"):
        wrapped(_RecordingHandle())

    (action, status, detail) = events[-1]
    assert action == "start CDC connectors"
    assert status == "failure"
    assert "failed after" in detail
    assert "RuntimeError" in detail and "CFN update failed" in detail


def test_cdc_lifecycle_logs_cancel_as_info(monkeypatch) -> None:
    # run_cdc_* RETURNS normally when cancelled, so the handle -- not an exception --
    # is what distinguishes "stopped early" from "finished". A cancel is neither a
    # success nor a failure.
    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui

    events = _capture_cdc_events(monkeypatch)
    wrapped = cdc_ui._logged_cdc_lifecycle(
        "deploy CDC infrastructure", detail="stack s1", work=lambda _h: None
    )
    wrapped(_RecordingHandle(cancelled=True))

    (action, status, detail) = events[-1]
    assert status == "info"
    assert "cancelled after" in detail


def test_cdc_lifecycle_outcome_is_logged_from_the_job_thread(monkeypatch) -> None:
    # The whole point: the outcome must be recorded by the JOB, not by the UI poller
    # (which only runs while the operator is looking at the CDC screen -- so a 20-30
    # minute action completing while they are elsewhere was previously never logged).
    import threading

    import dsql_migrator.ui.data_migration._cdc_ui as cdc_ui
    from dsql_migrator.core.job_manager import JobManager

    events = _capture_cdc_events(monkeypatch)
    seen_threads: list = []

    def _work(_handle):
        seen_threads.append(threading.current_thread().name)

    manager = JobManager()
    job_id = manager.submit(
        cdc_ui._logged_cdc_lifecycle(
            "start CDC connectors", detail="stack s1", work=_work
        )
    )
    assert manager.wait(job_id, timeout=5.0) is True
    assert manager.get_status(job_id).status == "DONE"

    # Logged without any UI render/poll happening.
    assert [e[1] for e in events] == ["success"]
    # And it ran off the main thread (i.e. on the job worker).
    assert seen_threads and seen_threads[0] != threading.main_thread().name


# ---------------------------------------------------------------------------
# Index-creation failures are reported without failing the table
# ---------------------------------------------------------------------------


def test_index_failures_are_logged_as_info_not_a_table_failure() -> None:
    # A missing index must not read as data loss: the entry goes to the error log so
    # the operator knows WHICH index is absent, but the table stays loaded.
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration._full_load_engine import _record_index_failures

    log = ErrorLogStore()
    _record_index_failures(
        log, "job-1", "orders",
        ["ix_total: more than 24 indexes per table are not allowed"],
    )
    (record,) = log.records("job-1")
    assert record.table == "orders"
    assert "index not created" in record.message
    assert "ix_total" in record.message
    # It must say the data is fine and how to resolve it.
    assert "DATA loaded" in record.message
    assert "24 per table" in record.message
    # No error code: this is not a row-level data error.
    assert record.error_code is None


def test_no_index_failures_logs_nothing() -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration._full_load_engine import _record_index_failures

    log = ErrorLogStore()
    _record_index_failures(log, "job-1", "orders", [])
    _record_index_failures(log, "job-1", "orders", None)
    assert log.records("job-1") == []


def test_table_load_result_carries_index_failures() -> None:
    from dsql_migrator.ui.data_migration._full_load_engine import TableLoadResult

    r = TableLoadResult(rows_loaded=10, index_failures=("ix_a: boom",))
    assert r.index_failures == ("ix_a: boom",)
    # Default is empty, so an ordinary load reports nothing.
    assert TableLoadResult(rows_loaded=10).index_failures == ()


def test_worker_result_carries_index_failures_for_the_parent() -> None:
    # The multiprocess path must report missing indexes too, or a run would behave
    # differently depending on the worker mode.
    from dsql_migrator.ui.data_migration._full_load_engine import _TableWorkerResult

    r = _TableWorkerResult(
        table_name="orders", status="DONE", index_failures=("ix_a: boom",)
    )
    assert r.index_failures == ("ix_a: boom",)
    assert _TableWorkerResult(table_name="t", status="DONE").index_failures == ()


# ---------------------------------------------------------------------------
# Discovered cdc-stacks — a failed/deleting stack must not be offered for attach
# ---------------------------------------------------------------------------


def test_split_attachable_stacks_excludes_failed_and_deleting() -> None:
    """A DELETE_FAILED stack must never be offered as "Attach to <stack>".

    Its resources are partly gone, so attaching yields a dead session — and the
    inviting Attach button hid the fact that actually matters: a teardown did not
    finish, so the leftover Amazon MSK / NAT may still be BILLING. (Observed for real:
    mysql-dsql-cdc-stack-0727 sat in DELETE_FAILED with an ACTIVE MSK Serverless
    cluster while the UI offered to attach to it.)
    """
    from dsql_migrator.ui.data_migration import split_attachable_stacks

    stacks = [
        ("mysql-dsql-cdc-good", "CREATE_COMPLETE"),
        ("mysql-dsql-cdc-stack-0727", "DELETE_FAILED"),
        ("mysql-dsql-cdc-rolled", "ROLLBACK_COMPLETE"),
        ("mysql-dsql-cdc-going", "DELETE_IN_PROGRESS"),
        ("mysql-dsql-cdc-updated", "UPDATE_COMPLETE"),
    ]
    attachable, needs_cleanup = split_attachable_stacks(stacks)
    assert [n for n, _ in attachable] == [
        "mysql-dsql-cdc-good",
        "mysql-dsql-cdc-updated",
    ]
    assert [n for n, _ in needs_cleanup] == [
        "mysql-dsql-cdc-stack-0727",
        "mysql-dsql-cdc-rolled",
        "mysql-dsql-cdc-going",
    ]
    assert split_attachable_stacks([]) == ([], [])


def test_split_attachable_stacks_is_case_insensitive() -> None:
    from dsql_migrator.ui.data_migration import split_attachable_stacks

    _ok, bad = split_attachable_stacks([("s", "delete_failed")])
    assert [n for n, _ in bad] == ["s"]


def test_stack_status_needs_cleanup_keeps_the_banner_alive() -> None:
    """The JOB finishing is not the same as the STACK being gone.

    A delete that ends in DELETE_FAILED (or a job record lost to an app restart)
    previously cleared the teardown marker, so the cross-view banner went silent while
    the leftover MSK / NAT kept billing with nothing in the UI saying so.
    """
    from dsql_migrator.ui.data_migration._cdc_status import stack_status_needs_cleanup

    assert stack_status_needs_cleanup("DELETE_FAILED") is True
    assert stack_status_needs_cleanup("ROLLBACK_COMPLETE") is True
    assert stack_status_needs_cleanup("UPDATE_ROLLBACK_FAILED") is True
    # A healthy or absent stack must NOT pin an alarming banner.
    assert stack_status_needs_cleanup("CREATE_COMPLETE") is False
    assert stack_status_needs_cleanup("UPDATE_COMPLETE") is False
    assert stack_status_needs_cleanup(None) is False
    assert stack_status_needs_cleanup("") is False


def test_attach_banner_separates_cleanup_from_attachable(monkeypatch) -> None:
    # The two situations need different treatment: cleanup is an ERROR (money is being
    # spent), attach is a WARNING (a choice). A DELETE_FAILED stack must get NO button.
    from dsql_migrator.ui.data_migration import (
        _render_cdc_existing_infra_banner,
        DataMigrationState,
    )

    state = DataMigrationState()
    state.set_cdc_other_stacks(
        [
            ("mysql-dsql-cdc-stack-0727", "DELETE_FAILED"),
            ("mysql-dsql-cdc-good", "CREATE_COMPLETE"),
        ]
    )

    ui = _RecordingUi()
    buttons: list[str] = []
    orig_button = ui.button

    def _button(text="", *a, **k):
        buttons.append(str(text))
        return orig_button(text, *a, **k)

    ui.button = _button
    _render_cdc_existing_infra_banner(ui, state, lambda: None)

    joined = " ".join(ui.texts)
    # The stuck stack is reported as needing cleanup, and names the billing risk.
    assert "needs cleanup" in joined
    assert "mysql-dsql-cdc-stack-0727" in joined
    assert "billing" in joined.lower()
    # Attach is offered ONLY for the healthy stack.
    assert any("Attach to mysql-dsql-cdc-good" in b for b in buttons)
    assert not any("0727" in b for b in buttons), (
        "a DELETE_FAILED stack must not get an Attach button"
    )


# ---------------------------------------------------------------------------
# Full Load progress table — pagination survives the ~1.5s poll rebuild
# ---------------------------------------------------------------------------


class _TableUi:
    """Double capturing ui.table's pagination + on_pagination_change handler."""

    def __init__(self):
        self.pagination = None
        self.on_pagination_change = None
        self.texts: list[str] = []

    class _El:
        def __init__(self, rec):
            self._rec = rec

        def classes(self, *_a, **_k):
            return self

        def props(self, *_a, **_k):
            return self

        def tooltip(self, *_a, **_k):
            return self

        def add_slot(self, *_a, **_k):
            return self

        def on(self, *_a, **_k):
            return self

        def on_value_change(self, *_a, **_k):
            return self

        def set_enabled(self, *_a, **_k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    def table(self, *_a, pagination=None, on_pagination_change=None, **_k):
        self.pagination = pagination
        self.on_pagination_change = on_pagination_change
        return self._El(self)

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._El(self)

    def __getattr__(self, _name):
        # Any other ui.* call is a no-op chainable element.
        return lambda *a, **k: _TableUi._El(self)


def _status_rows(n: int):
    """n FullLoadTableRows, enough to exercise multi-page pagination."""
    from dsql_migrator.ui.data_migration import FullLoadTableRow

    return [
        FullLoadTableRow(
            table=f"db.t{i}",
            state="DONE",
            rows_loaded=10,
            expected_rows=10,
            attempts=1,
            errors=0,
        )
        for i in range(n)
    ]


def test_progress_table_holder_persists_rows_per_page() -> None:
    """The poll holder must carry rowsPerPage, not just the page.

    The per-table progress table is rebuilt on every ~1.5s poll tick, so a pagination
    value that is hardcoded at build time is silently restored on the next tick: raising
    "Records per page" appeared to do nothing, and the reverting select made the table
    look like it was refreshing itself.
    """
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm._render_full_load_step)
    assert '_progress_page = {"page": 1, "rowsPerPage": 10}' in src, (
        "the poll-surviving holder must seed rowsPerPage too"
    )


def test_progress_table_writes_back_both_pagination_fields() -> None:
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm._render_full_load_progress)
    # Seeds from the holder (not a hardcoded constant)...
    assert 'page_state.get("rowsPerPage"' in src
    # ...and writes the user's choice back, or the next tick would undo it.
    assert 'page_state["rowsPerPage"]' in src


def test_progress_table_pagination_round_trips_through_the_holder() -> None:
    """End-to-end: pick 50 rows/page, then rebuild -> the choice is still 50."""
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_full_load_progress

    job = MigrationJob(job_id="j1")
    job.progress_pct = 50.0
    rows = _status_rows(25)
    holder = {"page": 1, "rowsPerPage": 10}

    ui = _TableUi()
    _render_full_load_progress(ui, job, rows, page_state=holder)
    assert ui.pagination == {"rowsPerPage": 10, "page": 1}

    # The user raises Records per page to 50 (Quasar fires the change event).
    ui.on_pagination_change(type("E", (), {"value": {"page": 1, "rowsPerPage": 50}})())
    assert holder["rowsPerPage"] == 50

    # The next poll tick rebuilds the table: the choice must survive.
    ui2 = _TableUi()
    _render_full_load_progress(ui2, job, rows, page_state=holder)
    assert ui2.pagination["rowsPerPage"] == 50, (
        "rows-per-page was reset by the rebuild -- the setting looks broken"
    )


def test_progress_table_supports_the_all_option_and_clamps_the_page() -> None:
    # rowsPerPage == 0 is Quasar's "All": every row on one page, so the page must not
    # be computed from a ceil-div by zero. And a shrinking table must clamp the page
    # rather than leave the user on a now-empty one.
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_full_load_progress

    job = MigrationJob(job_id="j1")
    rows = _status_rows(25)

    all_holder = {"page": 1, "rowsPerPage": 0}
    ui = _TableUi()
    _render_full_load_progress(ui, job, rows, page_state=all_holder)
    assert ui.pagination == {"rowsPerPage": 0, "page": 1}

    deep = {"page": 3, "rowsPerPage": 10}
    ui2 = _TableUi()
    _render_full_load_progress(ui2, job, rows, page_state=deep)
    assert ui2.pagination["page"] == 3  # 25 rows / 10 -> page 3 exists
    ui3 = _TableUi()
    _render_full_load_progress(ui3, job, _status_rows(5), page_state=deep)
    assert ui3.pagination["page"] == 1  # only 1 page left -> clamped


# ---------------------------------------------------------------------------
# Stop Full Load — must always terminate (observed deadlock regression)
# ---------------------------------------------------------------------------


def test_report_progress_never_blocks_on_a_full_queue() -> None:
    """THE deadlock fix: a worker must never park inside a progress put.

    Observed live: Stop Full Load sat on "Stopping… finishing the current batch."
    forever. The drain thread stopped consuming, the workers filled the queue and
    blocked in sem_wait inside ``queue.put`` -- and there they could no longer reach the
    code that polls ``cancel_event``, so cancellation could never be seen. Progress is
    telemetry (deltas re-accrued by the next flush; authoritative totals come from the
    worker's return value), so dropping a message is harmless. Blocking was not.
    """
    from dsql_migrator.ui.data_migration._full_load_engine import _report_progress

    class _FullQueue:
        def __init__(self):
            self.attempts = 0

        def put_nowait(self, _msg):
            self.attempts += 1
            import queue

            raise queue.Full

        def put(self, *_a, **_k):  # pragma: no cover - must never be called
            raise AssertionError("must not use the blocking put()")

    q = _FullQueue()
    _report_progress(q, ("db.t", 100, 0))  # must return, not raise, not block
    assert q.attempts == 1

    # A closed/broken pipe is equally survivable.
    class _Broken:
        def put_nowait(self, _msg):
            raise OSError("handle is closed")

    _report_progress(_Broken(), ("db.t", 1, 0))
    # And a missing queue is a no-op (single-process path).
    _report_progress(None, ("db.t", 1, 0))


def test_worker_progress_paths_use_the_nonblocking_helper() -> None:
    # Every worker-side send must go through _report_progress; a bare
    # progress_queue.put() anywhere in a worker re-introduces the deadlock.
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    src = inspect.getsource(_engine)
    assert "progress_queue.put(" not in src, (
        "a blocking progress_queue.put() would re-introduce the Stop deadlock; "
        "use _report_progress()"
    )
    # The helper itself uses put_nowait.
    assert "put_nowait" in inspect.getsource(_engine._report_progress)


def test_parallel_load_bounds_the_cancel_wait() -> None:
    """Stop must terminate even if a worker never responds.

    The parent waited in ``as_completed(futures)`` with no timeout, so a worker wedged
    anywhere (hung socket, blocked queue put) stranded the whole job in RUNNING while
    the UI claimed it was finishing a batch. The wait is now sliced so an expiring
    grace period ends it.
    """
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    src = inspect.getsource(_engine._migrate_tables_in_parallel)
    # Scope to the PROCESS path (the one that hung). The thread fallback above it is
    # for test doubles / parallelism<=1: it shares the process, uses no IPC queue, and
    # its workers poll cancellation directly in the batch loop, so the queue deadlock
    # cannot arise there.
    process_path = src[src.index("# Unified process-parallel path"):]
    assert "as_completed(futures)" not in process_path, (
        "as_completed(futures) with no timeout is the unbounded wait that hung Stop"
    )
    assert "timeout=" in process_path and "FuturesTimeoutError" in process_path
    # A cancel starts a bounded grace period, after which the wait is abandoned.
    assert "_CANCEL_GRACE_SECONDS" in src
    assert "_cancel_deadline" in src
    assert _engine._CANCEL_GRACE_SECONDS > 0


def test_cleanup_sentinel_is_also_nonblocking() -> None:
    # The sentinel is sent on the FINALLY path. A blocking put there could wedge the
    # very cleanup that is supposed to unwind the job.
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    src = inspect.getsource(_engine._migrate_tables_in_parallel)
    assert "_report_progress(progress_queue, _PROGRESS_SENTINEL)" in src


def test_multiprocess_planner_shards_only_cdc_coexisting_and_honors_reader_shards() -> None:
    # The production multiprocess planner must mirror the single-process sharding
    # invariant (audit D1): shard ONLY when cdc_coexisting (never a REPLACE or a
    # non-CDC append -> torn read with nothing to reconcile), and cap the shard count
    # by cfg.full_load_reader_shards clamped to the source-connection ceiling -- NOT
    # the pool budget (remaining_slots), which ignored the off-switch and the ceiling.
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    src = inspect.getsource(_engine._migrate_tables_in_parallel)
    process_path = src[src.index("# Unified process-parallel path"):]
    # Gated on cdc_coexisting (the only safe sharding condition).
    assert "_shardable_ok = bool(migrator._inputs.cdc_coexisting)" in process_path
    assert "not table_is_replace" in process_path
    # Shard count comes from the clamped reader-shards budget, not the pool slots.
    assert "effective_reader_shards" in process_path
    assert "_MAX_SOURCE_READERS // tp" in process_path
    # The old unsafe allocation must be gone from the CODE (the word may survive only
    # in a comment explaining why): no assignment or arithmetic on remaining_slots.
    assert "remaining_slots =" not in process_path
    assert "remaining_slots //" not in process_path
    assert "table_parallelism - non_shardable_count" not in process_path


# ---------------------------------------------------------------------------
# CDC prerequisite gate — must not punish the normal Full-load-+-CDC flow
# ---------------------------------------------------------------------------


def _passing_cdc_report():
    return PrerequisiteReport.build(
        MigrationMode.CDC,
        [_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.PASS)],
    )


def _failing_cdc_report():
    return PrerequisiteReport.build(
        MigrationMode.CDC,
        [_result(PrerequisiteCheckId.BINLOG_ROW_FORMAT, PrerequisiteStatus.FAIL)],
    )


def _gate(state):
    """Mirror the UI's call: report + the durable gated-mode escape hatch."""
    from dsql_migrator.ui.data_migration import cdc_prerequisite_block_reason

    return cdc_prerequisite_block_reason(
        state.get_prereq_report(MigrationMode.CDC),
        cdc_checks_already_passed=(
            getattr(state, "prereq_gated_mode", None) is MigrationMode.CDC
        ),
    )


def test_cdc_gate_allows_deploy_after_a_finished_full_load_and_cdc_run() -> None:
    """The reported bug: run the CDC checks, finish the Full Load, deploy is blocked.

    The prerequisite reports live in process memory and are deliberately never
    persisted -- and the Full Load clears them when it starts. So a finished
    Full-load-+-CDC run legitimately has no report, and the gate told the user to run
    checks they had just run. The run could only have STARTED once the CDC-superset
    checks passed, and that IS recorded durably (prereq_gated_mode).
    """
    state = DataMigrationState()
    state.set_prereq_gated_mode(MigrationMode.CDC)  # what starting the load records
    assert state.get_prereq_report(MigrationMode.CDC) is None  # report is gone
    assert _gate(state) is None, "deploy must not be blocked after the load ran"


def test_cdc_gate_still_blocks_a_session_that_never_checked() -> None:
    state = DataMigrationState()
    reason = _gate(state)
    assert reason is not None
    assert "CDC prerequisite checks" in reason


def test_cdc_gate_does_not_accept_a_full_load_only_pass() -> None:
    # A Full-load-only run passed only the FULL_LOAD checks -- binlog ROW/FULL was
    # never verified, so it must not excuse the CDC gate.
    state = DataMigrationState()
    state.set_prereq_gated_mode(MigrationMode.FULL_LOAD)
    assert _gate(state) is not None


def test_cdc_gate_prefers_a_present_failing_report_over_the_escape_hatch() -> None:
    # A failing report is a LIVE signal: even with the gated mode recorded, a source
    # whose binlog is not ROW/FULL can never stream, so deploy must stay blocked.
    state = DataMigrationState()
    state.set_prereq_gated_mode(MigrationMode.CDC)
    state.set_prereq_report(MigrationMode.CDC, _failing_cdc_report())
    reason = _gate(state)
    assert reason is not None
    assert "ROW format" in reason


def test_cdc_gate_allows_a_freshly_passing_report() -> None:
    state = DataMigrationState()
    state.set_prereq_report(MigrationMode.CDC, _passing_cdc_report())
    assert _gate(state) is None


def test_cdc_gate_call_sites_pass_the_escape_hatch() -> None:
    # Both CDC lifecycle gates (Deploy infrastructure, Start CDC) must use it, or the
    # bug returns on whichever one was missed.
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui)
    assert src.count("cdc_checks_already_passed=") == 2, (
        "both the deploy and start gates must pass cdc_checks_already_passed"
    )


class _GateUi(_RecordingUi):
    """A ``_RecordingUi`` that also tracks button enabled-state and input handlers.

    The VPC-ID gate works IN PLACE (``set_enabled`` / ``set_text`` on elements created
    earlier in the render, the way ``ui/connect.py`` gates its Next button) rather than
    by re-rendering, so a test has to observe those calls and be able to fire an input's
    saved handler -- neither of which the plain recorder exposes.
    """

    def __init__(self) -> None:
        super().__init__()
        self.buttons: list = []
        self.inputs: list = []
        self.hints: list = []

    class _Btn(_RecordingUi._El):
        def __init__(self, label: str) -> None:
            self.label = label
            self.enabled = True

        def props(self, value="", *_a, **_k):
            if "disable" in str(value):
                self.enabled = False
            return self

        def set_enabled(self, value, *_a, **_k):
            self.enabled = bool(value)
            return self

    class _Input(_RecordingUi._El):
        def __init__(self, label: str, value: str) -> None:
            self.label = label
            self.value = value
            self.handlers: dict = {}

        def on(self, event, handler=None, *_a, **_k):
            if handler is not None:
                self.handlers[event] = handler
            return self

        def on_value_change(self, handler=None, *_a, **_k):
            # Real NiceGUI fires this on every value change -- typing AND paste -- with a
            # ValueChangeEventArguments; the double just keys it like any other event.
            if handler is not None:
                self.handlers["value_change"] = handler
            return self

        def enter(self, text: str, *, event: str = "value_change"):
            """Simulate the user entering ``text``, then the resulting save."""
            self.value = text
            self.handlers[event](None)
            return self

    class _Hint(_RecordingUi._El):
        def __init__(self) -> None:
            self.text = ""

        def set_text(self, text="", *_a, **_k):
            self.text = str(text)
            return self

    def button(self, label="", *_a, **_k):
        element = self._Btn(str(label))
        self.buttons.append(element)
        return element

    def input(self, label="", *_a, value="", **_k):
        element = self._Input(str(label), str(value or ""))
        self.inputs.append(element)
        return element

    def label(self, text="", *_a, **_k):
        # inline_hint() renders through ui.label and is then driven by set_text, so hand
        # back a text-tracking element while still recording the initial text.
        self.texts.append(str(text))
        element = self._Hint()
        self.hints.append(element)
        return element

    def deploy_button(self):
        return next(b for b in self.buttons if "Deploy CDC infrastructure" in b.label)

    def vpc_field(self):
        return next(i for i in self.inputs if "VPC ID" in i.label)

    def gate_hint(self) -> str:
        return " ".join(h.text for h in self.hints if h.text)


def _cdc_state_ready_for_deploy():
    """A state whose CDC prerequisite checks have passed (so only the VPC ID can gate)."""
    from dsql_migrator.core.models import (
        MigrationMode,
        PrerequisiteCheckId,
        PrerequisiteReport,
        PrerequisiteResult,
        PrerequisiteStatus,
    )

    state = DataMigrationState()
    state.set_prereq_report(
        MigrationMode.CDC,
        PrerequisiteReport.build(
            MigrationMode.CDC,
            [
                PrerequisiteResult(
                    check_id=PrerequisiteCheckId.BINLOG_ROW_FORMAT,
                    title="Binlog row format",
                    status=PrerequisiteStatus.PASS,
                    required=True,
                    detail="ROW",
                )
            ],
        ),
    )
    return state


def _render_deploy_action(state):
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _GateUi()
    _cdc_ui._render_cdc_infra_deploy_action(
        ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
    )
    return ui


def test_deploy_cdc_infra_is_disabled_until_a_vpc_id_is_entered() -> None:
    """VpcId is the one deploy input the tool cannot infer, so it must gate the button.

    It was validated only in the submit path: the button looked ready, clicking it opened
    the confirmation dialog (which runs a VPC network diagnosis and a cost estimate), and
    only the final Deploy answered with an "Enter your VPC ID." toast. The requirement
    has to be stated BEFORE the click.
    """
    ui = _render_deploy_action(_cdc_state_ready_for_deploy())
    assert ui.deploy_button().enabled is False
    # ...and it says what is missing rather than just going dead.
    assert "VPC ID" in ui.gate_hint()

    state = _cdc_state_ready_for_deploy()
    state.set_cdc_infra_inputs({"vpc_id": "vpc-0123456789abcdef0"})
    ready = _render_deploy_action(state)
    assert ready.deploy_button().enabled is True
    assert ready.gate_hint() == ""  # nothing missing -> no leftover hint


def test_whitespace_only_vpc_id_still_counts_as_missing() -> None:
    # A field focused and left holding only spaces is not a VPC ID. The submit-path check
    # strips, so the gate must strip identically -- otherwise the button would invite a
    # click that the dialog then rejects.
    state = _cdc_state_ready_for_deploy()
    state.set_cdc_infra_inputs({"vpc_id": "   "})
    assert _render_deploy_action(state).deploy_button().enabled is False


def test_entering_a_vpc_id_enables_deploy_without_rebuilding_the_form() -> None:
    """The gate updates in place, so the first Deploy click is not swallowed.

    Re-rendering the form from its own input handler would recreate the field being typed
    in (losing focus) and could swap the button out from under the click.
    """
    ui = _render_deploy_action(_cdc_state_ready_for_deploy())
    button = ui.deploy_button()
    assert button.enabled is False

    inputs_before = list(ui.inputs)
    ui.vpc_field().enter("vpc-0123456789abcdef0")

    assert button.enabled is True  # the SAME button object, not a replacement
    assert ui.inputs == inputs_before  # nothing was re-created under the cursor
    assert ui.gate_hint() == ""

    # Clearing it re-locks the button instead of leaving a stale enabled state.
    ui.vpc_field().enter("")
    assert button.enabled is False
    assert "VPC ID" in ui.gate_hint()


def test_vpc_field_gates_on_value_change_and_on_blur() -> None:
    # Blur alone is not enough: the next move after entering the ID is to click Deploy,
    # and a click on a still-disabled button is swallowed, so the user would have to click
    # twice. value_change (as the Connect step uses) also covers a paste, which fires no
    # keystroke. Either event alone must open the gate.
    ui = _render_deploy_action(_cdc_state_ready_for_deploy())
    assert set(ui.vpc_field().handlers) >= {"blur", "value_change"}

    ui.vpc_field().enter("vpc-0123456789abcdef0", event="blur")
    assert ui.deploy_button().enabled is True

    ui2 = _render_deploy_action(_cdc_state_ready_for_deploy())
    ui2.vpc_field().enter("vpc-0123456789abcdef0", event="value_change")
    assert ui2.deploy_button().enabled is True


def test_prerequisite_block_takes_precedence_over_the_vpc_id_hint() -> None:
    # Both unmet: name the prerequisite checks only. They come first in the flow, and two
    # blocking reasons at once read as two separate problems.
    ui = _render_deploy_action(DataMigrationState())
    assert ui.deploy_button().enabled is False
    body = "\n".join(ui.texts)
    assert "Run the CDC prerequisite checks first" in body
    assert "to enable the deploy" not in body


# ---------------------------------------------------------------------------
# Default table selection: this session's Schema Conversion choice must win
# ---------------------------------------------------------------------------


def _dm_target_inventory(*names: str):
    from dsql_migrator.core.models import (
        TargetInventory,
        TargetObjectKind,
        TargetRelation,
        TargetSchemaNode,
    )

    return TargetInventory(
        schemas=[
            TargetSchemaNode(
                name="source",
                tables=[
                    TargetRelation(
                        schema_name="source", name=n, kind=TargetObjectKind.TABLE
                    )
                    for n in names
                ],
            )
        ]
    )


def test_default_selection_prefers_this_sessions_schema_conversion_choice() -> None:
    """Ticking 3 tables in Step 2 must not arrive in Step 3 with everything ticked.

    Reported from a real session: the picker showed ALL tables checked. The default was
    "every table that exists on the target", so a target still carrying tables from
    earlier runs silently re-selected them all and discarded the deliberate Step 2
    choice -- defaulting to migrating MORE than asked, the wrong direction for a
    long-running load.
    """
    from dsql_migrator.ui.data_migration import default_migration_selection

    target = _dm_target_inventory("orders", "customers")  # both already on target
    generated = [f"{TABLE_PREFIX}orders"]  # ...but only `orders` was picked here

    assert default_migration_selection(_inventory(), generated, target) == ["orders"]


def test_default_selection_falls_back_to_target_tables_when_nothing_generated() -> None:
    # The reconnect / applied-out-of-band case: with no Step 2 selection in THIS session
    # the choice is genuinely unknown, and an empty default would leave the user staring
    # at zero ticked tables. Falling back to the target set keeps that flow working.
    from dsql_migrator.ui.data_migration import default_migration_selection

    target = _dm_target_inventory("orders", "customers")

    assert default_migration_selection(_inventory(), None, target) == [
        "orders",
        "customers",
    ]
    assert default_migration_selection(_inventory(), [], target) == [
        "orders",
        "customers",
    ]


class _PickerTickUi(_RecordingUi):
    """Captures the ids passed to tree.tick() plus the rendered caption text."""

    def __init__(self) -> None:
        super().__init__()
        self.ticked: list[str] = []

    class _El(_RecordingUi._El):
        # _RecordingUi._El whitelists methods, so any NiceGUI call it does not list
        # (bind_value_to, expand, ...) raises AttributeError mid-render. The picker
        # uses several, and the point of these tests is the ticked set -- not which
        # chainable calls the double happens to enumerate.
        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def scroll_area(self, *_a, **_k):
        return self._El()

    def input(self, *_a, **_k):
        return self._El()

    def tree(self, *_a, **_k):
        outer = self

        class _Tree(_RecordingUi._El):
            def tick(self, ids=None):
                if ids is not None:
                    outer.ticked = list(ids)
                return self

            def __getattr__(self, _name):
                return lambda *_a, **_k: self

        return _Tree()


def _render_picker(generated, target):
    from dsql_migrator.ui.data_migration import (
        default_migration_selection,
        migratable_table_names,
        target_existing_table_names,
        _render_table_selection,
    )

    inventory = _inventory()
    ui = _PickerTickUi()
    _render_table_selection(
        ui,
        inventory,
        DataMigrationState(),
        migratable_table_names(inventory, generated, target),
        target_existing=target_existing_table_names(inventory, target),
        default_selection=default_migration_selection(inventory, generated, target),
    )
    return ui


def test_picker_ticks_only_the_schema_conversion_selection() -> None:
    target = _dm_target_inventory("orders", "customers")
    ui = _render_picker([f"{TABLE_PREFIX}orders"], target)

    assert ui.ticked == [f"{TABLE_PREFIX}orders"]
    caption = next(t for t in ui.texts if t.startswith("Pre-selected"))
    assert "1 of 2" in caption
    assert "selected in Schema Conversion" in caption


def test_picker_caption_says_target_when_that_is_where_the_default_came_from() -> None:
    # The fallback must NOT claim a Schema Conversion choice the user never made in this
    # session -- the origin is derived from the sets differing, not from the parameter
    # merely being supplied.
    target = _dm_target_inventory("orders", "customers")
    ui = _render_picker(None, target)

    assert len(ui.ticked) == 2
    caption = next(t for t in ui.texts if t.startswith("Pre-selected"))
    assert "already on the target" in caption
    assert "Schema Conversion" not in caption


def test_default_selection_uses_the_ticked_set_when_nothing_was_generated() -> None:
    """The restored-session case the first cut of this fix missed.

    Reported after v0.1.198: the picker STILL over-ticked, and "Start over" fixed it --
    the tell that it was a restored session. ``generated_node_ids`` is only set by
    pressing "Generate DDL for selected", so a session that applied without it (or
    pressed Clear afterwards) restores with that field empty while ``ticked_node_ids``
    -- also persisted -- still holds the real Step 2 selection. Falling straight through
    to the target-existing set then re-ticked every table the target happened to carry.

    Schema Conversion's own apply already resolves its scope as "generated else ticked"
    (``_selected_apply_names``); this mirrors it so the two steps agree.
    """
    from dsql_migrator.ui.data_migration import default_migration_selection

    target = _dm_target_inventory("orders", "customers")  # both already on target
    ticked = [f"{TABLE_PREFIX}orders"]  # ...but only `orders` was ticked in Step 2

    assert default_migration_selection(_inventory(), None, target, ticked) == ["orders"]
    # An explicit empty generated list (Clear was pressed) must behave the same.
    assert default_migration_selection(_inventory(), [], target, ticked) == ["orders"]


def test_default_selection_prefers_generated_over_ticked_when_both_exist() -> None:
    # Generated is the COMMITTED, reviewed scope, so it wins over a tick set the user
    # may have changed since generating.
    from dsql_migrator.ui.data_migration import default_migration_selection

    target = _dm_target_inventory("orders", "customers")

    assert default_migration_selection(
        _inventory(),
        [f"{TABLE_PREFIX}customers"],  # generated
        target,
        [f"{TABLE_PREFIX}orders"],  # ticked since
    ) == ["customers"]


def test_default_selection_falls_back_only_when_neither_scope_is_known() -> None:
    # A reconnect into a session that never ticked anything: the Step 2 choice is
    # genuinely unknown, so the target set keeps the flow usable rather than showing
    # zero ticked tables with no explanation.
    from dsql_migrator.ui.data_migration import default_migration_selection

    target = _dm_target_inventory("orders", "customers")

    assert default_migration_selection(_inventory(), None, target, None) == [
        "orders",
        "customers",
    ]
    assert default_migration_selection(_inventory(), [], target, []) == [
        "orders",
        "customers",
    ]


def test_all_default_selection_call_sites_pass_the_ticked_scope() -> None:
    """Every call site must thread ``ticked_node_ids``, not just the generated ids.

    The helper can be perfectly correct and the UI still over-tick if a call site omits
    the argument -- which is exactly how this shipped broken once: the pure function was
    fixed and tested, the picker still selected every table, and only "Start over"
    appeared to help (because a fresh session repopulates the generated ids). Assert the
    wiring, since a unit test of the helper alone cannot see it.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm.build_data_migration_screen))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "default_migration_selection"
    ]
    assert calls, "expected the screen to resolve the default selection"
    for call in calls:
        passed = [
            ast.unparse(arg) for arg in call.args
        ] + [f"{kw.arg}={ast.unparse(kw.value)}" for kw in call.keywords]
        joined = " ".join(passed)
        assert "ticked_node_ids" in joined, (
            "a default_migration_selection() call omits the ticked scope, so a restored "
            f"session would re-tick every target table: {joined}"
        )


def test_prereq_nav_row_left_aligns_the_guard_message_and_right_aligns_the_button() -> None:
    """The Prerequisites nav row must not right-align the guard sentence.

    Reported from a real session: after adding a table post-check, "Re-run the
    prerequisite checks — ecommerce_demo.categories was added…" appeared right-aligned.
    The row is `justify-end` because it normally holds only the primary "Continue"
    button (design system: primary actions sit right), and the guard message that
    REPLACES that button inherited the alignment -- ragging a full sentence against the
    right edge, away from the content it explains.

    Asserted on the row's classes rather than a source substring, so it fails if the
    alignment is hardcoded back.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    tree = ast.parse(src)

    # Find the conditional that picks the nav row's justification.
    picks = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.IfExp)
        and "justify-end" in ast.unparse(node)
        and "justify-start" in ast.unparse(node)
    ]
    assert picks, (
        "the Prerequisites nav row must choose its justification: justify-end for the "
        "primary button, justify-start for the guard sentence"
    )
    expr = ast.unparse(picks[0])
    # justify-end only when there is NO guard reason (i.e. the button is shown).
    assert "guard_reason is None" in expr
    assert expr.index("justify-end") < expr.index("justify-start"), (
        f"alignment is inverted: {expr}"
    )


# ---------------------------------------------------------------------------
# Quarantined rows must never be reported as a complete load
# ---------------------------------------------------------------------------


def _screenshot_rows(quarantined: int):
    """The reported run: product_media loaded 12 of an estimated 15 with 1 row dropped."""
    from dsql_migrator.ui.data_migration import FullLoadTableRow

    return [
        FullLoadTableRow(table="ecommerce.categories", state="DONE", rows_loaded=5,
                         expected_rows=5, attempts=1, errors=0),
        FullLoadTableRow(table="ecommerce.orders", state="DONE", rows_loaded=500,
                         expected_rows=500, attempts=1, errors=0),
        FullLoadTableRow(table="ecommerce.product_media", state="DONE", rows_loaded=12,
                         expected_rows=15, attempts=1, errors=3,
                         rows_quarantined=quarantined),
        FullLoadTableRow(table="ecommerce_demo.categories", state="DONE",
                         rows_loaded=630, expected_rows=629, attempts=1, errors=0),
    ]


def test_a_quarantined_row_makes_the_table_incomplete() -> None:
    """A dropped row is a CONFIRMED loss and must not be absorbed by the estimate
    tolerance.

    Reported from a real run: the screen showed an amber "Quarantined rows (1) — these
    rows were permanently dropped" box directly above a green "All 8 tables loaded every
    source row". `complete` compared loaded-vs-estimate only, and the 20% sampling
    tolerance swallowed the 3-row shortfall on a 15-row table, so the verdict contradicted
    the warning beside it.
    """
    from dsql_migrator.ui.data_migration import FullLoadTableRow

    dropped = FullLoadTableRow(
        table="t", state="DONE", rows_loaded=12, expected_rows=15, attempts=1,
        errors=3, rows_quarantined=1,
    )
    assert dropped.complete is False

    # Without the drop, the same shortfall stays within the estimate tolerance.
    noise = FullLoadTableRow(
        table="t", state="DONE", rows_loaded=12, expected_rows=15, attempts=1, errors=0
    )
    assert noise.complete is True


def test_completeness_reports_dropped_rows_and_is_not_all_complete() -> None:
    from dsql_migrator.ui.data_migration import full_load_completeness

    c = full_load_completeness(_screenshot_rows(1))
    assert c.all_complete is False
    assert c.quarantined_rows == 1
    assert c.quarantined_tables == ["ecommerce.product_media"]

    # The identical run WITHOUT a drop is still a clean completion.
    clean = full_load_completeness(_screenshot_rows(0))
    assert clean.all_complete is True
    assert clean.quarantined_rows == 0


class _BannerUi:
    """Collects the completeness banner's rendered header/body text."""

    def __init__(self) -> None:
        self.texts: list[str] = []

    class _El:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def label(self, text="", *_a, **_k):
        if text:
            self.texts.append(str(text))
        return self._El()

    def __getattr__(self, _name):
        return lambda *_a, **_k: _BannerUi._El()


def _banner_text(rows, *, approximate=True) -> str:
    from dsql_migrator.ui.data_migration import (
        full_load_completeness,
        _render_completeness_banner,
    )

    ui = _BannerUi()
    _render_completeness_banner(ui, full_load_completeness(rows), approximate=approximate)
    return " ".join(ui.texts)


def test_banner_never_claims_every_row_loaded_when_rows_were_dropped() -> None:
    body = _banner_text(_screenshot_rows(1))

    assert "loaded every source row" not in body
    assert "1 row permanently dropped" in body
    assert "ecommerce.product_media" in body


def test_banner_does_not_soften_dropped_rows_as_estimate_noise() -> None:
    # The approximate baseline routes count differences to a calm "counts differ from the
    # pre-load estimate" INFO note. A dropped row is not estimate drift, so it must not
    # be filed there -- nothing about a scan-free estimate explains a row the loader
    # could not write.
    body = _banner_text(_screenshot_rows(1), approximate=True)

    assert "counts differ from the pre-load estimate" not in body
    assert "This is expected" not in body
    assert "finished with issues" in body


def test_banner_remedy_matches_the_problem() -> None:
    # A quarantining table is DONE, so it is NOT in the retry set: telling the user to
    # "retry the failed tables" when nothing failed is a dead end.
    from dsql_migrator.ui.data_migration import FullLoadTableRow

    only_dropped = _banner_text(_screenshot_rows(1))
    assert "Retry the failed tables" not in only_dropped
    assert "Reload that table" in only_dropped

    with_failure = _banner_text(
        _screenshot_rows(1)
        + [
            FullLoadTableRow(table="x", state="FAILED", rows_loaded=0,
                             expected_rows=10, attempts=2, errors=1)
        ]
    )
    assert "Retry the failed tables" in with_failure


def test_a_quarantined_table_is_not_double_reported_as_a_mismatch() -> None:
    # The table's shortfall IS the dropped rows, so naming it twice would read as two
    # separate problems.
    body = _banner_text(_screenshot_rows(1))
    assert body.count("ecommerce.product_media") == 1
    assert "row-count mismatch" not in body


def test_quarantine_blocks_completion_even_with_no_estimate_to_compare() -> None:
    """A drop must sink the verdict on its own, not via a row-count comparison.

    Covers the case where the count check cannot help: ``expected_rows`` is ``None``
    (no source estimate), which previously short-circuited straight to
    ``complete = True``. The quarantine guard now runs FIRST, so the loss is caught
    without any baseline to compare against.

    (Note on layering: because that guard forces ``complete = False``, the table also
    lands in ``mismatched`` -- so ``all_complete``'s own ``quarantined_rows == 0``
    clause is defence-in-depth rather than separately reachable today. It is kept so a
    future change to ``complete`` cannot silently restore the green verdict.)
    """
    from dsql_migrator.ui.data_migration import (
        FullLoadTableRow,
        full_load_completeness,
    )

    no_estimate = [
        FullLoadTableRow(table="t", state="DONE", rows_loaded=10, expected_rows=None,
                         attempts=1, errors=1, rows_quarantined=2)
    ]
    row = no_estimate[0]
    assert row.complete is False  # would have been True before the guard

    c = full_load_completeness(no_estimate)
    assert c.quarantined_rows == 2
    assert c.all_complete is False

    body = _banner_text(no_estimate)
    assert "loaded every source row" not in body
    assert "2 rows permanently dropped" in body


def test_quarantine_count_reaches_the_row_from_the_job_chunk() -> None:
    """The engine records the drop on the chunk; the view-model must carry it through.

    A mutation dropping `rows_quarantined=` from `build_full_load_table_rows` survived --
    every completeness test built rows by hand, so nothing covered the engine -> UI hop
    where the count is actually read. That is the whole path the reported bug travelled.
    """
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import build_full_load_table_rows

    job = MigrationJob(job_id="j1")
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=1, attempts=1),
        ChunkState(chunk_id="ecommerce.orders", status="DONE", rows_loaded=500,
                   attempts=1),
    ]

    rows = {r.table: r for r in build_full_load_table_rows(job)}

    assert rows["ecommerce.product_media"].rows_quarantined == 1
    assert rows["ecommerce.orders"].rows_quarantined == 0
    # ...and that is enough to make the run incomplete.
    assert rows["ecommerce.product_media"].complete is False


# ---------------------------------------------------------------------------
# Marking the tables that dropped rows (badge in Status + summary chip)
# ---------------------------------------------------------------------------


class _SummaryChipUi:
    """Records badge labels and their tooltips for the state-summary row."""

    def __init__(self) -> None:
        self.badges: list[str] = []
        self.tooltips: list[str] = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def tooltip(self, text="", *_a, **_k):
            if text:
                self._ui.tooltips.append(str(text))
            return self

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def badge(self, text="", *_a, **_k):
        if text:
            self.badges.append(str(text))
        return self._El(self)

    def __getattr__(self, _name):
        return lambda *_a, **_k: _SummaryChipUi._El(self)


def _summary_chips(rows):
    from dsql_migrator.ui.data_migration import _render_table_state_summary

    ui = _SummaryChipUi()
    _render_table_state_summary(ui, rows)
    return ui


def test_state_summary_chips_flag_dropped_rows() -> None:
    """"Done: 8" alone made a run that permanently dropped rows look clean.

    A quarantining table settles as DONE, so the at-a-glance chips were identical to a
    flawless run's -- the reported screenshot showed exactly that. An amber chip now
    appears whenever anything was dropped.
    """
    ui = _summary_chips(_screenshot_rows(1))

    assert any(b.startswith("Done:") for b in ui.badges)
    assert "Dropped: 1 row" in ui.badges
    tip = " ".join(ui.tooltips)
    assert "permanently dropped" in tip
    assert "the rest of their rows loaded normally" in tip


def test_state_summary_chips_are_unchanged_on_a_clean_run() -> None:
    ui = _summary_chips(_screenshot_rows(0))

    assert any(b.startswith("Done:") for b in ui.badges)
    assert not any(b.startswith("Dropped:") for b in ui.badges)


def test_state_summary_chip_pluralizes_rows_and_tables() -> None:
    from dsql_migrator.ui.data_migration import FullLoadTableRow

    many = [
        FullLoadTableRow(table="a", state="DONE", rows_loaded=1, expected_rows=5,
                         attempts=1, errors=1, rows_quarantined=2),
        FullLoadTableRow(table="b", state="DONE", rows_loaded=1, expected_rows=5,
                         attempts=1, errors=1, rows_quarantined=3),
    ]
    ui = _summary_chips(many)

    assert "Dropped: 5 rows" in ui.badges
    assert "across 2 tables" in " ".join(ui.tooltips)


def test_progress_row_carries_the_dropped_badge_data() -> None:
    """The Status cell needs per-row data, because the amber panel below the table does
    not say WHICH row dropped rows -- and that row may be on another page."""
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import (
        _quarantined_cell_tooltip,
        build_full_load_table_rows,
    )

    job = MigrationJob(job_id="j1")
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=1, attempts=1),
        ChunkState(chunk_id="ecommerce.orders", status="DONE", rows_loaded=500,
                   attempts=1),
    ]
    rows = {r.table: r for r in build_full_load_table_rows(job)}

    dropped = _quarantined_cell_tooltip(rows["ecommerce.product_media"])
    assert "1 row was permanently dropped" in dropped
    assert "rest of this table loaded normally" in dropped
    assert "Reload this table" in dropped
    # No badge (and no tooltip) for a clean table.
    assert _quarantined_cell_tooltip(rows["ecommerce.orders"]) == ""


def test_status_cell_slot_renders_the_dropped_badge_from_row_data() -> None:
    # The badge lives in a Quasar slot template, so a wrong row key renders nothing at
    # runtime with the suite still green. Pin the template's contract: it must be
    # conditional on the count and read only keys the row mapping supplies.
    import inspect
    import re

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm._render_full_load_progress)
    start = src.index('"body-cell-state"')
    template = src[start : start + 800]

    assert 'v-if="props.row.quarantined > 0"' in template, (
        "the dropped badge must be conditional, not always rendered"
    )
    assert "props.row.quarantined_tooltip" in template
    # Every row key the template reads must be produced by the row mapping above.
    for key in set(re.findall(r"props\.row\.(\w+)", template)):
        assert f'"{key}":' in src, f"slot reads props.row.{key} but no row supplies it"


# ---------------------------------------------------------------------------
# "Accept quarantined rows & continue" must survive a restart
# ---------------------------------------------------------------------------


def _quarantine_job(*, dropped: int, status: str = "DONE"):
    from dsql_migrator.core.models import ChunkState, MigrationJob

    job = MigrationJob(job_id="j1")
    job.status = "FAILED"  # FullLoadIncompleteError
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status=status, rows_loaded=12,
                   rows_quarantined=dropped, attempts=1),
        ChunkState(chunk_id="ecommerce.orders", status="DONE", rows_loaded=500,
                   attempts=1),
    ]
    return job


def _log_with_quarantine_rows(job_id: str, pks) -> "ErrorLogStore":
    from datetime import datetime, timezone

    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.models import DataErrorRecord

    log = ErrorLogStore()
    for pk in pks:
        log.record(
            job_id,
            DataErrorRecord(
                table="ecommerce.product_media", chunk_id="ecommerce.product_media",
                error_code="22001",
                message=f"quarantined row pk[id={pk}]: datatype limit exceeded",
                occurred_at=datetime.now(timezone.utc),
            ),
        )
    return log


def test_accept_quarantine_stays_available_after_a_restart() -> None:
    """The escape hatch must not vanish with the in-memory error log.

    Reported from a real run: Full Load ended with FullLoadIncompleteError, whose message
    tells the operator to use "Accept quarantined rows & continue" — and in a RESTORED
    session that button was gone. The quarantine-only gate counted rows in
    ``ErrorLogStore``, which is in-memory, so a restart made the count 0 and the gate
    False. A complete dead end: the run cannot be retried into success (a
    permanently-rejected value never loads), so the only escape left was Start over.

    The job store IS durable, so the per-chunk count is the signal that survives.
    """
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration import (
        _incomplete_is_quarantine_only,
        _quarantined_row_count,
    )

    job = _quarantine_job(dropped=3)
    fresh_log = ErrorLogStore()  # a new process: the records are gone

    assert not fresh_log.records("j1")
    assert _quarantined_row_count(job, fresh_log) == 3
    assert _incomplete_is_quarantine_only(job, fresh_log) is True


def test_quarantine_count_is_not_double_counted_within_a_session() -> None:
    # Same session: the chunk count and the log entries describe the SAME rows, so the
    # count must be 3, not 6.
    from dsql_migrator.ui.data_migration import _quarantined_row_count

    job = _quarantine_job(dropped=3)
    log = _log_with_quarantine_rows("j1", (3, 7, 9))

    assert _quarantined_row_count(job, log) == 3


def test_quarantine_count_falls_back_to_the_error_log_for_an_older_job() -> None:
    # A job written before the per-chunk count existed has rows_quarantined == 0; the
    # log-scanning fallback keeps the escape hatch working for it.
    from dsql_migrator.ui.data_migration import (
        _incomplete_is_quarantine_only,
        _quarantined_row_count,
    )

    job = _quarantine_job(dropped=0)
    log = _log_with_quarantine_rows("j1", (3, 7))

    assert _quarantined_row_count(job, log) == 2
    assert _incomplete_is_quarantine_only(job, log) is True


def test_accept_quarantine_is_withheld_when_there_is_retryable_work() -> None:
    # An UNFINISHED table is retryable work and must still block: the override exists
    # only for rows that can never load, not to wave past a table that simply failed.
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration import _incomplete_is_quarantine_only

    job = _quarantine_job(dropped=3, status="FAILED")

    assert _incomplete_is_quarantine_only(job, ErrorLogStore()) is False


def test_accept_quarantine_is_withheld_when_nothing_was_dropped() -> None:
    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.ui.data_migration import _incomplete_is_quarantine_only

    job = _quarantine_job(dropped=0)

    assert _incomplete_is_quarantine_only(job, ErrorLogStore()) is False


# ---------------------------------------------------------------------------
# Failures must be diagnosable from the DURABLE activity log
# ---------------------------------------------------------------------------


def test_each_quarantined_row_is_recorded_on_the_activity_log(monkeypatch) -> None:
    """A permanently dropped row must leave a durable trace, not only an in-memory one.

    The per-row detail (primary key + reason) went ONLY to ``ErrorLogStore``, which is
    in-memory: after a restart nothing said WHICH rows were lost, just a count. A row the
    migration can never recover on its own is exactly what an audit trail is for.
    """
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append({"action": action, **kw}),
    )

    _engine._log_quarantined_row(
        "ecommerce.product_media", "id=3",
        "datatype limit greater than 1048576 bytes not supported for bytea", "22001",
    )

    (entry,) = captured
    assert entry["action"] == "row quarantined"
    assert entry["target"] == "ecommerce.product_media"
    assert entry["error_code"] == "22001"
    assert "pk[id=3]" in entry["detail"]  # the PK is what you need to fix the source
    assert "PERMANENTLY DROPPED" in entry["detail"]
    assert "1048576" in entry["detail"]  # the actual reason, not a generic label
    assert "rest of the table loaded" in entry["detail"]  # not a whole-table failure


def test_every_load_path_logs_quarantined_rows(monkeypatch) -> None:
    # Three load paths record quarantine (in-process, sharded worker, single-table
    # worker). A sharded table is a LARGE one -- least likely to be checked by hand --
    # so a path that skipped the audit entry would hide the worst cases.
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    src = inspect.getsource(_engine)
    # One definition + one call per load path.
    assert src.count("_log_quarantined_row(") == 4, (
        "expected the helper plus a call in each of the three load paths"
    )


def test_run_incomplete_names_the_tables_and_reasons(monkeypatch) -> None:
    """"1 of 8 table(s) did not fully load" is a count, not a diagnosis.

    Read weeks later it says a run failed but not which table or why -- and the per-table
    reasons lived only in the in-memory error log.
    """
    from datetime import datetime, timezone

    from dsql_migrator.core.error_log import ErrorLogStore
    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append({"action": action, **kw}),
    )

    log = ErrorLogStore()
    log.record("j1", DataErrorRecord(
        table="ecommerce.product_media", chunk_id="x", error_code="22001",
        message="quarantined row pk[id=3]: datatype limit greater than 1048576 bytes",
        occurred_at=datetime.now(timezone.utc)))

    class _Handle:
        cancelled = False

    with pytest.raises(Exception):
        _engine._finalize_run(
            _Handle(), "j1", ["t"] * 8,
            _engine._RunCounts(real_failed=0, quarantined=1), log,
            accept_quarantined_rows=False,
        )

    entry = next(e for e in captured if e["action"] == "run incomplete")
    assert "1 of 8 table(s) did not fully load" in entry["detail"]
    assert "ecommerce.product_media" in entry["detail"]  # WHICH table
    assert "1048576" in entry["detail"]  # WHY


# ---------------------------------------------------------------------------
# Oversized-LOB column exclusion is recorded on the activity log (audit trail)
# ---------------------------------------------------------------------------


def test_excluded_lob_columns_are_logged_one_event_per_column(monkeypatch) -> None:
    """Excluding a column is an intentional data omission -- it must be auditable.

    The exclusion dropped the column from the load but wrote NOTHING to the activity
    log, so a reviewer reading the downloaded log could not tell a column arrived NULL
    on purpose. Row-level quarantine is already logged; column-level exclusion now is
    too. Value-free (names only the column), logged at INFO (an expected, user choice).
    """
    import dataclasses

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append({"action": action, **kw}),
    )

    inputs = dataclasses.replace(_inputs(), excluded_lob_columns={
        "ecommerce.product_media": frozenset({"content", "thumbnail"}),
        "ecommerce.orders": frozenset({"notes"}),
    })
    _engine._log_excluded_lob_columns(inputs)

    events = [e for e in captured if e["action"] == "column excluded"]
    # One event per column, across all tables (2 + 1).
    assert len(events) == 3
    targets = {e["target"] for e in events}
    assert targets == {
        "ecommerce.product_media.content",
        "ecommerce.product_media.thumbnail",
        "ecommerce.orders.notes",
    }
    one = events[0]
    assert one["status"].value == "info"  # expected, user-chosen -- not a fault
    assert "left NULL on the target" in one["detail"]
    assert "not migrated" in one["detail"]


def test_excluded_lob_columns_retry_logs_only_scoped_tables(monkeypatch) -> None:
    # A retry re-runs a subset of tables, so it should log exclusions ONLY for the
    # tables it actually re-ran -- not re-announce exclusions for untouched tables.
    import dataclasses

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append({"action": action, **kw}),
    )

    inputs = dataclasses.replace(_inputs(), excluded_lob_columns={
        "ecommerce.product_media": frozenset({"content"}),
        "ecommerce.orders": frozenset({"notes"}),
    })
    _engine._log_excluded_lob_columns(inputs, scope={"ecommerce.product_media"})

    targets = {e["target"] for e in captured if e["action"] == "column excluded"}
    assert targets == {"ecommerce.product_media.content"}  # orders.notes NOT logged


def test_log_excluded_lob_columns_noop_without_inputs_or_exclusions(monkeypatch) -> None:
    # Legacy callers pass no inputs; a run with no exclusions logs nothing.
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[str] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append(action),
    )

    _engine._log_excluded_lob_columns(None)  # legacy caller: no inputs
    _engine._log_excluded_lob_columns(_inputs())  # default inputs: no exclusions
    assert captured == []


def test_lob_excluded_note_echoes_columns_on_the_table_line() -> None:
    """The per-table ``load table`` detail echoes which columns were excluded."""
    import dataclasses

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    class _M:
        _inputs = dataclasses.replace(_inputs(), excluded_lob_columns={
            "ecommerce.product_media": frozenset({"content"})
        })

    note = _engine._lob_excluded_note(_M(), "ecommerce.product_media")
    assert note == " (1 column(s) excluded: content)"
    # A table with no exclusion gets no note; a migrator without _inputs is safe.
    assert _engine._lob_excluded_note(_M(), "ecommerce.orders") == ""
    assert _engine._lob_excluded_note(object(), "ecommerce.product_media") == ""


# ---------------------------------------------------------------------------
# The Full Load watermark (CDC-handoff consistency point) is recorded in the log
# ---------------------------------------------------------------------------


def test_captured_watermark_is_logged_prefers_gtid(monkeypatch) -> None:
    """The watermark is the gapless-handoff consistency point -- it belongs in the log.

    It was persisted only on the in-memory job record, so the downloaded
    migration_activity.log had no record of which source point-in-time the migration
    captured (the coordinate a later CDC catch-up resumes from). It is now logged at
    INFO right after capture, preferring the GTID as the resume coordinate.
    """
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append({"action": action, **kw}),
    )

    _engine._log_captured_watermark(_watermark())

    (entry,) = [e for e in captured if e["action"] == "watermark captured"]
    assert entry["status"].value == "info"
    assert "GTID 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5" in entry["detail"]
    assert "2026-01-02T03:04:05Z" in entry["detail"]  # snapshot UTC timestamp
    assert "2 table(s) counted" in entry["detail"]
    # A resume coordinate + timestamp, never a row value (Property 7).
    assert "handoff" in entry["detail"]


def test_captured_watermark_falls_back_to_binlog_and_flags_approx(monkeypatch) -> None:
    from dsql_migrator.core.models import Watermark
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append({"action": action, **kw}),
    )

    wm = Watermark(
        binlog_file="mysql-bin.000999",
        binlog_position=1234,
        snapshot_timestamp=datetime(2026, 3, 4, 5, 6, 7, tzinfo=timezone.utc),
        table_row_counts={"orders": 7},
        row_counts_approximate=True,
    )
    _engine._log_captured_watermark(wm)

    (entry,) = captured
    assert "binlog mysql-bin.000999:1234" in entry["detail"]  # no GTID -> binlog:pos
    assert "approximate estimates" in entry["detail"]


def test_captured_watermark_none_is_noop(monkeypatch) -> None:
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append(action),
    )
    _engine._log_captured_watermark(None)  # legacy caller / retry without a watermark
    assert captured == []


def test_run_full_load_logs_the_captured_watermark(monkeypatch) -> None:
    # End-to-end: a real run emits a "watermark captured" event after "run started".
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    captured: list[str] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: captured.append(action),
    )

    migrator = _FakeMigrator(rows_by_table={"orders": 10, "customers": 3})
    _run_full_load_job(migrator, _tables())

    assert "run started" in captured
    assert "watermark captured" in captured
    # It comes after "run started" (captured once, right after the snapshot).
    assert captured.index("watermark captured") > captured.index("run started")


def test_migration_type_selection_is_logged_on_a_real_change_only() -> None:
    # The migration-type choice shapes the whole journey (Full Load only / CDC only /
    # both) and is logged when it CHANGES -- but not re-logged when a refresh re-confirms
    # the current tile (that would spam the log).
    import inspect

    from dsql_migrator.ui import data_migration as _dm

    src = inspect.getsource(_dm)
    assert '"migration type selected"' in src
    # The log call sits inside the `if changed:` block (only a real change logs).
    idx_changed = src.index("if changed:")
    idx_log = src.index('"migration type selected"')
    idx_refresh = src.index("refresh()", idx_changed)
    assert idx_changed < idx_log < idx_refresh  # logged within the changed branch


def test_cdc_connector_failure_detail_localizes_the_fault() -> None:
    """A bare "connector X failed" cannot be troubleshot: it names no cause.

    The reason lived only in CloudWatch and the in-memory error summary. The durable
    entry now carries the peer connectors' states (which side of the pipeline broke), the
    DLQ depth, the per-table error counts, and where to find the stack trace.
    """
    from dsql_migrator.core.models import ErrorLogSummary, LoadStatusView
    from dsql_migrator.ui.data_migration import _connector_failure_detail

    view = LoadStatusView(
        kind="CDC",
        connector_states={"cdc-source": "RUNNING", "cdc-sink": "FAILED"},
        dlq_depth=4,
        error_summary=ErrorLogSummary(
            total_errors=4, errors_by_table={"product_media": 3, "orders": 1},
            log_available=True,
        ),
    )

    detail = _connector_failure_detail(object(), view, "cdc-sink")

    # The source still running localizes the fault to the sink side.
    assert "cdc-source=RUNNING" in detail
    assert "cdc-sink" not in detail.split("other connectors:")[1].split(";")[0]
    assert "4 record(s) in the DLQ" in detail
    assert "product_media=3" in detail
    assert "CloudWatch" in detail


def test_cdc_connector_failure_detail_degrades_without_diagnostics() -> None:
    # A poll may fire before CloudWatch/DLQ data exists. The entry must still point
    # somewhere useful rather than being dropped or claiming false detail.
    from dsql_migrator.core.models import LoadStatusView
    from dsql_migrator.ui.data_migration import _connector_failure_detail

    detail = _connector_failure_detail(
        object(), LoadStatusView(kind="CDC", connector_states={"cdc-sink": "FAILED"}),
        "cdc-sink",
    )

    assert "CloudWatch" in detail
    assert "DLQ" not in detail  # never invents a depth it did not read


def test_connector_failure_transition_actually_logs_the_detail() -> None:
    # The helper can be perfect and the log still bare if the transition site passes
    # detail=None -- a mutation doing exactly that survived every other test here,
    # because they all called the helper directly. Assert the wiring.
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm._log_cdc_connector_transitions))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_connector_failure_detail"
    ]
    assert calls, "the FAILED transition must build a troubleshooting detail"
    # ...and that value must reach the log call as `detail=`.
    logged = [
        kw
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_log_cdc_event"
        for kw in node.keywords
        if kw.arg == "detail"
    ]
    assert logged, "_log_cdc_event must receive the detail"
    assert any(ast.unparse(kw.value) == "detail" for kw in logged), (
        "the built detail must be passed through, not dropped"
    )


def test_accept_quarantine_button_has_no_duplicate_caption() -> None:
    """The button must not repeat guidance the completeness banner already gives.

    Both sat on the same screen saying the same thing: the caption beside
    "Accept quarantined rows & continue" ("Fix the source value(s) and Reload to load
    them, or accept the gap to proceed to CDC ...") duplicated the banner's own remedy
    ("fix the source value(s) and Reload that table ... or accept the gap to continue
    (Validation reports it)"). The banner keeps it -- it is the one place that states the
    verdict AND the remedy together.
    """
    import inspect

    from dsql_migrator.ui import data_migration as dm

    render_src = inspect.getsource(dm._render_accept_quarantine_action)
    assert "Accept quarantined rows & continue" in render_src  # the button stays
    assert "Reload to load them" not in render_src, (
        "the duplicate caption beside the accept button is back"
    )

    # ...and the guidance still exists exactly once, in the banner.
    banner_src = inspect.getsource(dm._render_completeness_banner)
    assert "Reload that table" in banner_src
    assert "accept the gap" in banner_src


# ---------------------------------------------------------------------------
# Presenting a dropped row: which table, which row, why
# ---------------------------------------------------------------------------


_QUAR_MSG = (
    "quarantined row pk[id=3]: datatype limit greater than 1048576 bytes "
    "not supported for bytea"
)


class _DetailRowUi:
    """Records labels/badges/icons emitted by the quarantine detail row."""

    def __init__(self) -> None:
        self.labels: list[str] = []
        self.badges: list[str] = []
        self.icons: list[str] = []

    class _El:
        def __init__(self, ui):
            self._ui = ui

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def label(self, text="", *_a, **_k):
        if text:
            self.labels.append(str(text))
        return self._El(self)

    def badge(self, text="", *_a, **_k):
        if text:
            self.badges.append(str(text))
        return self._El(self)

    def icon(self, text="", *_a, **_k):
        if text:
            self.icons.append(str(text))
        return self._El(self)

    def __getattr__(self, _name):
        return lambda *_a, **_k: _DetailRowUi._El(self)


def test_quarantine_message_is_split_into_table_pk_and_reason() -> None:
    """The raw one-liner buried the two facts the operator acts on.

    It rendered as a single run-on line -- "quarantined row pk[id=3]: datatype limit
    greater than 1048576 bytes not supported for bytea" -- with the table name in a
    separate badge above and the primary key mid-sentence. The PK is the actionable
    handle (it is what you search the source with), so it now gets its own chip.
    """
    from dsql_migrator.ui.data_migration import (
        _quarantine_detail_row,
        _quarantined_reason,
    )

    ui = _DetailRowUi()

    _quarantine_detail_row(
        ui,
        table="ecommerce.product_media",
        primary_keys=["id=3"],
        reasons=[_quarantined_reason(_QUAR_MSG)],
    )

    assert "ecommerce.product_media" in ui.labels  # WHICH table, given prominence
    assert "id=3" in ui.badges  # WHICH row, as its own chip
    assert "1 row dropped" in ui.badges  # the count leads (it covers truncation too)
    # WHY -- the technical reason, without the redundant "quarantined row pk[...]" stem.
    reason = next(l for l in ui.labels if "1048576" in l)
    assert not reason.startswith("quarantined row pk[")
    assert "not supported for bytea" in reason


def test_quarantine_message_parsing_falls_back_for_unexpected_text() -> None:
    # An unparseable message must be shown verbatim rather than mangled or dropped.
    from dsql_migrator.ui.data_migration import (
        _parse_quarantined_pk,
        _quarantined_reason,
    )

    assert _parse_quarantined_pk(_QUAR_MSG) == "id=3"
    assert _quarantined_reason(_QUAR_MSG).startswith("datatype limit")

    assert _parse_quarantined_pk("some other failure") is None
    assert _quarantined_reason("some other failure") == "some other failure"
    # Malformed (no closing bracket) -> no PK claimed.
    assert _parse_quarantined_pk("quarantined row pk[id=3 oops") is None


def test_quarantine_section_has_no_duplicate_header() -> None:
    """The quarantine section shows per-row DETAIL only -- no count header.

    A 3-row drop was announced in four boxes on one screen: the summary chip, the row's
    Status badge, this section's header, and the completeness banner. The header was the
    redundant one (it also once said "Quarantined rows (1)" for 3 rows, because the list
    it measured holds one entry per TABLE). The banner owns the verdict + remedy; this
    section owns what nothing else provides: which row, why, and Reload.
    """
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import (
        _render_full_load_progress,
        build_full_load_table_rows,
    )

    job = MigrationJob(job_id="j1")
    job.status = "FAILED"
    job.progress_pct = 100.0
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=3, attempts=1)
    ]
    rows = build_full_load_table_rows(
        job, None, {"ecommerce.product_media": _QUAR_MSG}
    )

    ui = _DetailRowUi()
    _render_full_load_progress(
        ui, job, rows, reload_table=lambda _n: None,
        quarantine_only=True,
    )

    joined = " ".join(ui.labels + ui.badges)
    # No header notice -- neither the old table-count wording nor a row-count restatement.
    assert "Quarantined rows (1)" not in joined
    assert "permanently dropped across" not in joined
    # The per-row detail (the section's actual job) is still there.
    assert "ecommerce.product_media" in joined
    assert "id=3" in joined
    # The verdict is NOT this function's job at all -- it belongs to the completeness
    # banner, which is a sibling call. Assert it states it exactly once, so removing the
    # header did not remove the count from the screen entirely.
    from dsql_migrator.ui.data_migration import (
        _render_completeness_banner,
        full_load_completeness,
    )

    banner = _BannerUi()
    _render_completeness_banner(banner, full_load_completeness(rows), approximate=False)
    stated = [t for t in banner.texts if "3 rows permanently dropped" in t]
    assert len(stated) == 1, stated


# ---------------------------------------------------------------------------
# Export watermark: compact provenance, not a sortable 4-row table
# ---------------------------------------------------------------------------


class _WatermarkUi:
    """Records labels/icons and whether a ui.table was used for the fixed fields."""

    def __init__(self) -> None:
        self.labels: list[str] = []
        self.icons: list[str] = []
        self.tables = 0
        self.expansions: list[str] = []

    class _El:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def __getattr__(self, _name):
            return lambda *_a, **_k: self

    def label(self, text="", *_a, **_k):
        if text:
            self.labels.append(str(text))
        return self._El()

    def icon(self, text="", *_a, **_k):
        if text:
            self.icons.append(str(text))
        return self._El()

    def table(self, *_a, **_k):
        self.tables += 1
        return self._El()

    def expansion(self, text="", *_a, **_k):
        if text:
            self.expansions.append(str(text))
        return self._El()

    def __getattr__(self, _name):
        return lambda *_a, **_k: _WatermarkUi._El()


def _watermark_job(*, gtid=None, counts=None):
    from datetime import datetime, timezone

    from dsql_migrator.core.models import MigrationJob, Watermark

    job = MigrationJob(job_id="j1")
    job.watermark = Watermark(
        binlog_file="mysql-bin.000123", binlog_position=45678,
        gtid_executed=gtid,
        server_uuid="3E11FA47-71CA-11E1-9E33-C80AA9429562",
        snapshot_timestamp=datetime(2026, 8, 1, 2, 30, 15, tzinfo=timezone.utc),
        table_row_counts=counts or {},
    )
    return job


def test_watermark_renders_labelled_fields_not_a_four_row_table() -> None:
    """The watermark is provenance read once, so it must be compact.

    It used a 4-row two-column ``ui.table`` -- complete with sortable "Field"/"Value"
    headers for four fixed rows -- which took the height of a data grid to show four
    values. Each field is now a labelled monospace line, so the label identifies it and
    the value stays scannable and copy-pasteable.
    """
    from dsql_migrator.ui.data_migration import _render_watermark

    ui = _WatermarkUi()
    _render_watermark(ui, _watermark_job())

    # No table for the fixed fields (the row-counts expansion is separate; absent here).
    assert ui.tables == 0
    # Every coordinate is present with its own label.
    for label in ("Snapshot (UTC)", "Binlog file:pos", "GTID set", "Server UUID"):
        assert label in ui.labels
    assert "mysql-bin.000123:45678" in ui.labels
    assert "3E11FA47-71CA-11E1-9E33-C80AA9429562" in ui.labels
    # A leading icon + heading identify the section at a glance.
    assert "bookmark" in ui.icons
    assert "Export watermark" in ui.labels


def test_watermark_row_counts_match_the_panel_style() -> None:
    """The per-table counts belong to the watermark panel, styled like its other fields.

    They used to hang below it as a full-width expansion wrapping a bordered ``ui.table``
    with its own sortable headers -- a second visual container in a style nothing else on
    the screen uses. They are one value per table, so labelled monospace rows (the same
    shape as the coordinates above) read as more of this panel's detail.
    """
    from dsql_migrator.ui.data_migration import _render_watermark

    ui = _WatermarkUi()
    _render_watermark(ui, _watermark_job(counts={"orders": 500, "items": 1500}))

    # No nested data grid at all now.
    assert ui.tables == 0
    # Still collapsed behind an expansion (it can be long), with the count in the label.
    assert any("Per-table snapshot rows (2" in e for e in ui.expansions)
    # Each table is a labelled row with a thousands-separated count.
    assert "orders" in ui.labels
    assert "1,500" in ui.labels


def test_watermark_without_a_capture_explains_itself() -> None:
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_watermark

    ui = _WatermarkUi()
    _render_watermark(ui, MigrationJob(job_id="j1"))

    assert "Export watermark" in ui.labels
    assert any("captured when the migration starts" in l for l in ui.labels)
    assert ui.tables == 0


def test_watermark_renders_after_the_progress_table() -> None:
    """Provenance belongs below the live detail, not above it.

    The watermark used to sit between the separator and the progress table, pushing the
    per-table progress -- and on a finished run the completeness verdict and quarantine
    detail -- below a block of static reference data. It must still stay OUTSIDE the
    refreshable region, or the ~1.5s poll would collapse its row-counts expansion.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm._render_full_load_step)
    tree = ast.parse(src)

    # Compare the CALL sites, by line number. A naive src.index("_live_detail()") matches
    # the `def _live_detail()` definition first, so it passed no matter which order the
    # calls were in -- verified by swapping them.
    def _call_line(name: str) -> int:
        lines = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
            # Exclude the call inside the nested poll handler (_live_detail.refresh()
            # is an Attribute, not a Name, so only real invocations match here).
        ]
        assert lines, f"no call to {name}()"
        return max(lines)  # the top-level render call is the last one

    assert _call_line("_live_detail") < _call_line("_render_watermark"), (
        "the watermark must render AFTER the live progress detail"
    )

    live = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_live_detail"
    )
    assert "_render_watermark" not in ast.unparse(live), (
        "the watermark must not be inside the polled region"
    )


# ---------------------------------------------------------------------------
# Accepting the gap must be VISIBLE on the screen that offered it
# ---------------------------------------------------------------------------


def test_accepting_the_gap_replaces_the_button_with_confirmation() -> None:
    """The click worked but looked dead: nothing on this screen changed.

    Accepting sets the workflow step to DONE, unblocks Validation, and writes an activity
    entry -- but no render path read ``accept_quarantined_rows`` (it is only consumed when
    a NEW load runs), so the panel and its button re-rendered identically. A button that
    invites a second, equally invisible click is worse than none: the state is replaced by
    a success notice that also says where to go next.
    """
    from dsql_migrator.ui.data_migration import _render_accept_quarantine_action

    def _render(accepted: bool):
        ui = _DetailRowUi()
        ui.buttons: list[str] = []
        _orig_button = ui.button

        def _button(text="", *a, **k):
            if text:
                ui.buttons.append(str(text))
            return _orig_button(text, *a, **k)

        ui.button = _button
        _render_accept_quarantine_action(
            ui,
            quarantine_only=True,
            terminal=True,
            quarantine_accepted=accepted,
            accept_quarantine_and_continue=lambda: None,
        )
        return ui

    before = _render(False)
    assert any("Accept quarantined rows" in b for b in before.buttons)
    assert not any("Gap accepted" in l for l in before.labels)

    after = _render(True)
    # The button is gone -- no invitation to re-click an action already taken.
    assert not any("Accept quarantined rows" in b for b in after.buttons)
    # ...replaced by a ONE-LINE confirmation, not a second success notice: the
    # completeness banner directly above already states the count, the table, that the
    # next step is unblocked and that Validation reports the gap. Two green boxes saying
    # nearly the same words is what this replaced.
    confirmation = next(l for l in after.labels if "Gap accepted" in l)
    assert "still closes it" in confirmation  # the one fact the banner does not carry
    assert "Full Load marked complete" not in confirmation  # the banner's job
    assert not any("acknowledged gap, so the next step is unblocked" in l
                   for l in after.labels)
    assert "check_circle" in after.icons  # the click is still acknowledged visually


def test_completeness_banner_stops_calling_an_accepted_gap_an_issue() -> None:
    """Green "Gap accepted" sat directly above amber "Full Load finished with issues".

    The operator had just resolved that gap by explicit decision, so re-flagging it as a
    problem contradicted the confirmation one box away. The banner now reports it as
    complete-with-a-gap -- without ever claiming every row loaded, because they did not.
    """
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import (
        _render_completeness_banner,
        build_full_load_table_rows,
        full_load_completeness,
    )

    job = MigrationJob(job_id="j1")
    job.status = "FAILED"
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=3, attempts=1)
    ]
    completeness = full_load_completeness(build_full_load_table_rows(job, None, {}))

    unaccepted = _BannerUi()
    _render_completeness_banner(unaccepted, completeness, quarantine_accepted=False)
    assert any("finished with issues" in t for t in unaccepted.texts)

    accepted = _BannerUi()
    _render_completeness_banner(accepted, completeness, quarantine_accepted=True)
    body = " ".join(accepted.texts)
    assert "finished with issues" not in body
    assert "Full Load complete — with an accepted gap" in body
    assert "3 rows could not be stored" in body
    assert "you accepted that gap" in body
    # Must never claim a clean load.
    assert "loaded every source row" not in body
    # Validation still owns the gap.
    assert "Validation" in body


def test_an_unaccepted_or_failed_run_never_reads_as_complete() -> None:
    # Guard the precondition: the softened verdict applies ONLY to an accepted,
    # quarantine-only run. A real failure must keep the warning even if the flag is set,
    # or accepting a gap would paper over a retryable failure.
    from dsql_migrator.ui.data_migration import FullLoadTableRow, full_load_completeness
    from dsql_migrator.ui.data_migration import _render_completeness_banner

    with_failure = full_load_completeness([
        FullLoadTableRow(table="a", state="DONE", rows_loaded=12, expected_rows=15,
                         attempts=1, errors=3, rows_quarantined=3),
        FullLoadTableRow(table="b", state="FAILED", rows_loaded=0, expected_rows=10,
                         attempts=2, errors=1),
    ])

    ui = _BannerUi()
    _render_completeness_banner(ui, with_failure, quarantine_accepted=True)
    body = " ".join(ui.texts)
    assert "finished with issues" in body
    assert "accepted gap" not in body


def test_accept_flag_is_threaded_from_state_into_both_renders() -> None:
    """The flag must reach BOTH the panel and the banner from session state.

    A mutation deleting `quarantine_accepted=migration_state.accept_quarantined_rows`
    from the render call passed every other test here -- they all pass the flag directly,
    so the WIRING was untested. That is the same class of gap that shipped the
    restored-session table-selection bug and the CDC connector detail=None bug earlier in
    this series, so assert it structurally.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm._render_full_load_step)
    tree = ast.parse(src)

    wired = {
        node.func.id: {
            kw.arg: ast.unparse(kw.value) for kw in node.keywords
        }
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id
        in ("_render_accept_quarantine_action", "_render_completeness_banner")
    }

    for fn in ("_render_accept_quarantine_action", "_render_completeness_banner"):
        assert fn in wired, f"{fn} is not called from _render_full_load_step"
        assert wired[fn].get("quarantine_accepted") == (
            "migration_state.accept_quarantined_rows"
        ), f"{fn} must receive the accepted flag from session state, got {wired[fn]}"


def test_accept_action_renders_after_the_completeness_verdict() -> None:
    """The action must follow the verdict it acts on.

    The accept button lived inside the quarantine panel, which renders BEFORE the
    completeness banner -- so the operator was asked to decide before reading the
    conclusion they were deciding on. It is now its own call, placed after the banner.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm._render_full_load_step))

    def _line(name: str) -> int:
        lines = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
        ]
        assert lines, f"no call to {name}()"
        return max(lines)

    assert _line("_render_completeness_banner") < _line(
        "_render_accept_quarantine_action"
    ), "the accept action must render after the verdict it carries out"

    # ...and it must no longer be buried in the progress panel.
    assert "Accept quarantined rows" not in inspect.getsource(
        dm._render_full_load_progress
    )


def test_error_log_download_is_named_for_the_user_not_the_file_format() -> None:
    """"Download error log (NDJSON)" led with a format nobody asked about.

    It also never said WHICH step's errors it held, though both Full Load and CDC offer
    one. The label now names the step and the count; the format moves to the tooltip.
    """
    from datetime import datetime, timezone

    from dsql_migrator.core.models import DataErrorRecord, MigrationJob
    from dsql_migrator.ui.data_migration import _render_error_log

    state = DataMigrationState()
    for pk in (3, 7, 9):
        state.error_log.record("j1", DataErrorRecord(
            table="ecommerce.product_media", chunk_id="x",
            message=f"quarantined row pk[id={pk}]: too big",
            occurred_at=datetime.now(timezone.utc)))

    ui = _DetailRowUi()
    ui.buttons: list[str] = []
    _orig = ui.button

    def _button(text="", *a, **k):
        if text:
            ui.buttons.append(str(text))
        return _orig(text, *a, **k)

    ui.button = _button
    _render_error_log(ui, state, MigrationJob(job_id="j1"))

    (label,) = ui.buttons
    assert label == "Download Full Load error log (3 errors)"
    assert "NDJSON" not in label  # the format belongs in the tooltip
    # The redundant "Data errors" heading + count line is gone -- every error is already
    # listed above with its table, primary key and reason.
    assert not any("Data errors" in l for l in ui.labels)
    assert not any("data errors across" in l for l in ui.labels)


def test_error_log_download_is_hidden_with_no_errors() -> None:
    # With zero errors the section used to print a heading over "No data errors
    # recorded." -- a whole block asserting an absence.
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_error_log

    ui = _DetailRowUi()
    ui.buttons: list[str] = []
    ui.button = lambda text="", *a, **k: ui.buttons.append(str(text)) or ui._El(ui)

    _render_error_log(ui, DataMigrationState(), MigrationJob(job_id="j1"))

    assert ui.buttons == []
    assert ui.labels == []


def test_every_dropped_row_is_listed_not_one_per_table() -> None:
    """3 dropped rows must produce 3 entries, not 1.

    Reported: the count said "3 rows permanently dropped" but the list below showed only
    one. The panel was built from ``latest_messages()``, which keeps ONE message per table
    (last write wins), so a table that dropped N rows listed exactly one -- and the two
    numbers on the same screen disagreed. The primary key is the actionable part of each
    entry (it is what you search the source with), so every row has to appear.
    """
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import (
        _render_full_load_progress,
        build_full_load_table_rows,
    )

    job = MigrationJob(job_id="j1")
    job.status = "FAILED"
    job.progress_pct = 100.0
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=3, attempts=1)
    ]
    records = [
        (
            "ecommerce.product_media",
            f"quarantined row pk[id={pk}]: datatype limit greater than 1048576 bytes",
        )
        for pk in (3, 7, 9)
    ]
    rows = build_full_load_table_rows(
        job, None, {"ecommerce.product_media": records[-1][1]}
    )

    ui = _DetailRowUi()
    _render_full_load_progress(
        ui, job, rows, reload_table=lambda _n: None, quarantine_only=True,
        quarantine_records=records,
    )

    shown = [b for b in ui.badges if b.startswith("id=")]
    assert shown == ["id=3", "id=7", "id=9"], shown
    # GROUPED into one card per table: the table name and the shared reason are stated
    # ONCE, not repeated per row. A card per row also meant three identical "Reload"
    # buttons, since Reload acts on the whole table.
    assert ui.labels.count("ecommerce.product_media") == 1
    assert len([l for l in ui.labels if "1048576" in l]) == 1
    assert "3 rows dropped" in ui.badges


def test_quarantine_detail_falls_back_to_per_table_without_records() -> None:
    # A caller that passes no records (older call site, or a restored session whose
    # in-memory log is gone) still gets the per-table view -- one entry beats none, and
    # the count above stays authoritative.
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import (
        _render_full_load_progress,
        build_full_load_table_rows,
    )

    job = MigrationJob(job_id="j1")
    job.status = "FAILED"
    job.progress_pct = 100.0
    job.chunks = [
        ChunkState(chunk_id="ecommerce.product_media", status="DONE", rows_loaded=12,
                   rows_quarantined=3, attempts=1)
    ]
    rows = build_full_load_table_rows(
        job, None, {"ecommerce.product_media": _QUAR_MSG}
    )

    ui = _DetailRowUi()
    _render_full_load_progress(
        ui, job, rows, reload_table=lambda _n: None, quarantine_only=True,
    )

    assert [b for b in ui.badges if b.startswith("id=")] == ["id=3"]


def test_error_log_download_sits_with_the_detail_not_under_the_accept_button() -> None:
    # Immediately below "Accept quarantined rows & continue" it read as that decision's
    # secondary option, when it just takes the same per-row information away with you.
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    tree = ast.parse(inspect.getsource(dm._render_full_load_step))

    def _line(name: str) -> int:
        lines = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
        ]
        assert lines, f"no call to {name}()"
        return max(lines)

    assert _line("_render_error_log") < _line("_render_accept_quarantine_action"), (
        "the download must not sit directly under the accept decision"
    )


def test_watermark_counts_share_the_panel_font_and_two_column_shape() -> None:
    """The counts must look like the coordinates above them, not a separate list.

    Quasar's default expansion header is a grey full-bleed bar with a large leading glyph
    -- a heavy band across an otherwise flat panel. And the count rows used a
    flex-1/right-aligned layout while the coordinates used a fixed label column, so the
    two groups aligned differently. Both now use the same label-then-monospace-value
    shape; only the label WIDTH differs, because a table name needs more room than
    "GTID set".
    """
    from dsql_migrator.ui.data_migration import _render_watermark

    class _StyleUi(_WatermarkUi):
        def __init__(self) -> None:
            super().__init__()
            self.styled: list[tuple[str, str]] = []
            self.props: list[str] = []

        def label(self, text="", *_a, **_k):
            outer = self
            recorded = str(text) if text else ""
            if recorded:
                outer.labels.append(recorded)

            class _L:
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

                def classes(self, *a, **_k):
                    if a and recorded:
                        outer.styled.append((recorded, a[0]))
                    return self

                def __getattr__(self, _n):
                    return lambda *_a, **_k: self

            return _L()

        def expansion(self, text="", *_a, **_k):
            if text:
                self.expansions.append(str(text))
            outer = self

            class _E:
                def __enter__(self):
                    return self

                def __exit__(self, *exc):
                    return False

                def classes(self, *_a, **_k):
                    return self

                def props(self, *a, **_k):
                    if a:
                        outer.props.append(a[0])
                    return self

                def __getattr__(self, _n):
                    return lambda *_a, **_k: self

            return _E()

    ui = _StyleUi()
    _render_watermark(ui, _watermark_job(counts={"ecommerce.order_items": 1500}))

    styles = dict(ui.styled)
    # Values -- coordinate and count alike -- are monospace at the same size.
    assert "font-mono" in styles["mysql-bin.000123:45678"]
    assert "font-mono" in styles["1,500"]
    assert "text-xs" in styles["1,500"]
    # Labels -- field name and table name alike -- are the same muted size, in a fixed
    # column (the width differs: a table name needs more room than "GTID set").
    for label in ("Binlog file:pos", "ecommerce.order_items"):
        assert "text-xs text-gray-500" in styles[label]
        assert "shrink-0" in styles[label]
    # The heavy default header is stripped: no leading glyph, no grey fill.
    assert "numbers" not in ui.icons
    assert any("header-class" in p for p in ui.props)


# ---------------------------------------------------------------------------
# Grouping dropped rows by table (compact, and one Reload per table)
# ---------------------------------------------------------------------------


def _quar_entries(table: str, pks, reason: str = "datatype limit exceeded"):
    return [(table, f"quarantined row pk[id={pk}]: {reason}") for pk in pks]


def test_grouping_collapses_a_table_to_one_group() -> None:
    from dsql_migrator.ui.data_migration import _group_quarantine_entries

    grouped = _group_quarantine_entries(_quar_entries("t", (1, 2, 3)))

    assert len(grouped) == 1
    table, pks, reasons = grouped[0]
    assert table == "t"
    assert pks == ["id=1", "id=2", "id=3"]
    # A table usually drops rows for the SAME reason (one oversized column), so the
    # reason collapses to a single line instead of being repeated per row.
    assert reasons == ["datatype limit exceeded"]


def test_grouping_keeps_genuinely_different_reasons() -> None:
    # Deduplication must not hide a second, different cause -- that would mislead about
    # what needs fixing.
    from dsql_migrator.ui.data_migration import _group_quarantine_entries

    entries = _quar_entries("t", (1,), "too big") + _quar_entries("t", (2,), "bad type")
    (_table, pks, reasons) = _group_quarantine_entries(entries)[0]

    assert pks == ["id=1", "id=2"]
    assert reasons == ["too big", "bad type"]


def test_grouping_preserves_table_order_and_separates_tables() -> None:
    from dsql_migrator.ui.data_migration import _group_quarantine_entries

    grouped = _group_quarantine_entries(
        _quar_entries("b", (1,)) + _quar_entries("a", (2,)) + _quar_entries("b", (3,))
    )

    assert [t for t, _p, _r in grouped] == ["b", "a"]  # first-seen order
    assert grouped[0][1] == ["id=1", "id=3"]  # b's rows merged


def test_grouping_keeps_a_row_whose_message_has_no_parseable_pk() -> None:
    # An unexpected message format must still surface its reason rather than vanish.
    from dsql_migrator.ui.data_migration import _group_quarantine_entries

    (_table, pks, reasons) = _group_quarantine_entries([("t", "some other failure")])[0]

    assert pks == []
    assert reasons == ["some other failure"]


def test_grouped_card_offers_one_reload_per_table_not_per_row() -> None:
    """Reload acts on the whole TABLE, so a card per row offered N identical buttons.

    Reported from the screen: three cards, each repeating the table name and the same
    reason, each with its own "Reload" that did exactly the same thing -- while looking
    like it acted on that row alone.
    """
    from dsql_migrator.ui.data_migration import (
        _group_quarantine_entries,
        _quarantine_detail_row,
    )

    ui = _DetailRowUi()
    ui.buttons: list[str] = []
    _orig = ui.button

    def _button(text="", *a, **k):
        if text:
            ui.buttons.append(str(text))
        return _orig(text, *a, **k)

    ui.button = _button
    for table, pks, reasons in _group_quarantine_entries(_quar_entries("t", (1, 2, 3))):
        _quarantine_detail_row(
            ui, table=table, primary_keys=pks, reasons=reasons,
            action=lambda: ui.button("Reload"),
        )

    assert ui.buttons == ["Reload"]  # ONE, not three
    assert ui.labels.count("t") == 1
    assert "3 rows dropped" in ui.badges


def test_grouped_card_truncates_a_long_primary_key_list() -> None:
    """Many dropped rows must not produce an unbounded wall of chips.

    The count badge still reports the true total, and the full list is always in the
    downloadable error log -- so truncating the chips loses nothing.
    """
    from dsql_migrator.ui.data_migration import (
        _QUARANTINE_PK_CHIP_LIMIT,
        _group_quarantine_entries,
        _quarantine_detail_row,
    )

    total = _QUARANTINE_PK_CHIP_LIMIT + 8
    ui = _DetailRowUi()
    for table, pks, reasons in _group_quarantine_entries(
        _quar_entries("t", range(1, total + 1))
    ):
        _quarantine_detail_row(ui, table=table, primary_keys=pks, reasons=reasons)

    chips = [b for b in ui.badges if b.startswith("id=")]
    assert len(chips) == _QUARANTINE_PK_CHIP_LIMIT
    assert f"+{total - _QUARANTINE_PK_CHIP_LIMIT} more" in ui.labels
    # The badge reports the REAL total, so the truncation cannot under-report.
    assert f"{total} rows dropped" in ui.badges


# ---------------------------------------------------------------------------
# Start over must see a cdc-stack under a name this session does not target
# ---------------------------------------------------------------------------


def test_start_over_offers_teardown_for_a_stack_under_another_name() -> None:
    """Start over said "no CDC" while Data Migration offered to ATTACH to a real stack.

    Reported from a live session: Start over showed no CDC-teardown option, then moving to
    the CDC step offered "Existing CDC infrastructure found —
    mysql-dsql-cdc-stack-0729-new". Confirmed in the account: that stack exists
    (UPDATE_COMPLETE) under a name the session does not target. Both of Start over's
    signals (``cdc_stack_phase``, ``cdc_connector_names``) are scoped to
    ``cdc_stack_name``, so a stack from an earlier session or with a custom suffix was
    invisible to it -- and the silent prompt is the one that would have stopped the
    MSK / NAT billing.

    Asserted on ``app.py``'s source because the callbacks are per-session closures: the
    contract is that the offer consults ``cdc_other_stacks``, not just the session's own
    name.
    """
    import ast
    import inspect

    from dsql_migrator.ui import app as app_module

    src = inspect.getsource(app_module)
    tree = ast.parse(src)
    deployed = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_cdc_deployed"
    )
    # Strip the docstring: it NAMES cdc_other_stacks while explaining the bug, so
    # matching the whole function passed even with the check deleted (verified by
    # mutation). Only executable statements count.
    statements = [
        node
        for node in deployed.body
        if not (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant))
    ]
    body = "\n".join(ast.unparse(node) for node in statements)

    assert "cdc_other_stacks" in body, (
        "Start over must offer teardown for a discovered stack under another name"
    )
    # The original two signals must remain -- this ADDS a case, it does not replace them.
    assert "cdc_connector_names" in body
    assert "cdc_stack_phase" in body


def test_start_over_teardown_acts_on_every_stack_it_offered() -> None:
    """The offer, the tile listing, and the teardown must resolve the SAME stacks.

    The previous resolver returned a SINGLE name and adopted a discovered stack only when
    there was exactly one. With two or more it fell back to this session's own name --
    which, in that branch, is precisely the name that does NOT exist (the branch is
    reached only when the probe found no stack under it). So Start over offered "Delete
    all CDC infrastructure", the delete found nothing, and the operator was left paying
    for MSK / NAT behind a success toast.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_stack_names

    # The regression case: two discovered stacks, none under this session's own name.
    two = cdc_teardown_stack_names(
        own_stack_name="mysql-dsql-cdc-stack",
        stack_phase=None,
        connector_names=[],
        other_stacks=[("cdc-a", "CREATE_COMPLETE"), ("cdc-b", "CREATE_COMPLETE")],
    )
    assert two == ["cdc-a", "cdc-b"], "both discovered stacks must be torn down"
    assert "mysql-dsql-cdc-stack" not in two, (
        "must not target a name the probe did not find"
    )

    # This session's own stack is included only when it is genuinely live, and comes
    # first (the durable teardown marker/banner follows it).
    assert cdc_teardown_stack_names(
        own_stack_name="own", stack_phase="infra", connector_names=[],
        other_stacks=[("cdc-a", "CREATE_COMPLETE")],
    ) == ["own", "cdc-a"]
    assert cdc_teardown_stack_names(
        own_stack_name="own", stack_phase=None, connector_names=["own-debezium-source"],
        other_stacks=[],
    ) == ["own"]
    # Nothing deployed -> nothing to tear down (so no offer, and no no-op delete).
    assert cdc_teardown_stack_names(
        own_stack_name="own", stack_phase="absent", connector_names=[], other_stacks=[],
    ) == []
    # A discovered stack that happens to match the own name is not listed twice.
    assert cdc_teardown_stack_names(
        own_stack_name="own", stack_phase="running", connector_names=[],
        other_stacks=[("own", "CREATE_COMPLETE")],
    ) == ["own"]


def test_cdc_teardown_plan_covers_every_stack_and_cleans_the_secret_once() -> None:
    """Behavioral cover for the two survivors of the structural guard above.

    Two distinct mistakes, both invisible to a grep-style assertion:
    * tearing down only the FIRST stack -- the operator confirmed a named list, so the
      others would keep billing behind a success toast (the very bug being fixed);
    * repeating the shared source-secret cleanup per stack -- it is created out-of-band
      so CloudFormation cannot own it, and re-scheduling an already-scheduled secret
      delete fails for every extra stack.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_teardown_plan

    plan = cdc_teardown_plan(["cdc-a", "cdc-b", "cdc-c"], cleanup_secret=True)
    # EVERY stack is torn down, in the offered order.
    assert [name for name, _ in plan] == ["cdc-a", "cdc-b", "cdc-c"]
    # The shared secret is cleaned exactly once, with the first stack.
    assert [cleanup for _, cleanup in plan] == [True, False, False]

    # cleanup_secret=False (e.g. the source used Secrets Manager) never turns it on.
    assert cdc_teardown_plan(["cdc-a", "cdc-b"], cleanup_secret=False) == [
        ("cdc-a", False),
        ("cdc-b", False),
    ]
    # Blank/whitespace names are dropped rather than submitted as a delete of "".
    assert cdc_teardown_plan(["", "  ", "cdc-a"], cleanup_secret=True) == [
        ("cdc-a", True)
    ]
    assert cdc_teardown_plan([], cleanup_secret=True) == []


def test_start_over_offer_and_teardown_share_one_resolver() -> None:
    """Structural guard: both paths must call the shared resolver.

    The offer (``_cdc_deployed``) and the teardown are in different functions, so a change
    to one can silently desynchronise them -- which is how the no-op delete arose. The
    teardown must also iterate ALL names, since the dialog now lists them by name and the
    operator confirmed that list.
    """
    import ast
    import inspect

    from dsql_migrator.ui import app as app_module

    tree = ast.parse(inspect.getsource(app_module))
    by_name = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }

    resolver = ast.unparse(by_name["_cdc_teardown_stack_names"])
    # It reads all three signals the offer reads, and delegates to the pure helper.
    for signal in ("cdc_stack_phase", "cdc_connector_names", "cdc_other_stacks"):
        assert signal in resolver, signal
    assert "cdc_teardown_stack_names(" in resolver

    teardown = ast.unparse(by_name["_cdc_teardown_on_reset"])
    assert "_cdc_teardown_stack_names()" in teardown, (
        "the teardown must resolve stacks the same way the offer did"
    )
    # It must hand the WHOLE resolved list to the plan builder. This lives inside
    # build_page's closure, so it cannot be called directly -- and the failure mode is
    # silent (extra stacks keep billing behind a success toast), so assert on the call
    # shape: cdc_teardown_plan(stack_names, ...) with stack_names passed unsliced.
    plan_calls = [
        node
        for node in ast.walk(by_name["_cdc_teardown_on_reset"])
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "cdc_teardown_plan"
    ]
    assert len(plan_calls) == 1, "expected exactly one cdc_teardown_plan call"
    first_arg = plan_calls[0].args[0]
    assert isinstance(first_arg, ast.Name) and first_arg.id == "stack_names", (
        "the teardown must pass every resolved stack to the plan, unsliced "
        f"(got {ast.unparse(first_arg)})"
    )
    assert "getattr(migration_state, 'cdc_stack_name', None)" not in teardown, (
        "the teardown must not fall back to the session's own name directly"
    )


# ---------------------------------------------------------------------------
# Attach must be withheld when the pipeline does not cover this session's tables
# ---------------------------------------------------------------------------


def test_attach_scope_mismatch_is_asymmetric() -> None:
    """Only a LOADED table the pipeline ignores is a gap.

    Attaching promotes Data Migration to DONE and unlocks Validation, so it is sound only
    when the pipeline covers what this session loaded. A pipeline that is BROADER is fine:
    it may serve another table set in parallel and nothing this session owns is left
    uncovered.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_attach_scope_mismatch

    streamed = ["ecommerce_demo.orders", "ecommerce_demo.products"]

    # The reported situation: the live pipeline streams a different SCHEMA entirely.
    assert cdc_attach_scope_mismatch(
        streamed, ["ecommerce.orders", "ecommerce.users"]
    ) == ["ecommerce.orders", "ecommerce.users"]
    # Exact cover -> no gap.
    assert cdc_attach_scope_mismatch(streamed, streamed) == []
    # Pipeline broader than the selection -> NOT a gap (asymmetric on purpose).
    assert cdc_attach_scope_mismatch(streamed, ["ecommerce_demo.orders"]) == []
    # Case-insensitive, matching how table.include.list is written.
    assert cdc_attach_scope_mismatch(["ECOMMERCE.ORDERS"], ["ecommerce.orders"]) == []


def test_attach_scope_mismatch_never_blocks_on_unknowns() -> None:
    # An un-probed candidate has no TableIncludeList, and a session with no confirmed
    # selection has nothing to compare -- neither may be reported as a mismatch, or a
    # readable-but-unprobed stack would become permanently unattachable.
    from dsql_migrator.ui.data_migration._cdc_status import cdc_attach_scope_mismatch

    assert cdc_attach_scope_mismatch([], ["ecommerce.orders"]) == []
    assert cdc_attach_scope_mismatch(["ecommerce.orders"], []) == []
    assert cdc_attach_scope_mismatch([], []) == []


def _infra_banner(*, streamed, selected):
    from dsql_migrator.core.models import TableSelection
    from dsql_migrator.ui.data_migration import _render_cdc_existing_infra_banner

    state = DataMigrationState()
    state.set_cdc_other_stacks([("mysql-dsql-cdc-stack-0729-new", "UPDATE_COMPLETE")])
    state.set_cdc_other_stack_tables({"mysql-dsql-cdc-stack-0729-new": list(streamed)})
    state.set_selection(TableSelection(selected_tables=list(selected)))

    ui = _DetailRowUi()
    ui.buttons: list[str] = []
    _orig = ui.button

    def _button(text="", *a, **k):
        if text:
            ui.buttons.append(str(text))
        return _orig(text, *a, **k)

    ui.button = _button
    _render_cdc_existing_infra_banner(ui, state, lambda: None)
    return ui


def test_attach_is_withheld_for_a_pipeline_streaming_other_tables() -> None:
    """Reported: a live pipeline on the account streamed a completely different table set.

    Verified against the real stack: ``mysql-dsql-cdc-stack-0729-new`` replicates 11
    ``ecommerce_demo.*`` tables while the session had just loaded ``ecommerce.*``.
    Attaching would have marked the migration complete and unlocked cut-over while every
    loaded table had NO CDC -- losing each source change after the watermark.
    """
    ui = _infra_banner(
        streamed=["ecommerce_demo.orders", "ecommerce_demo.products"],
        selected=["ecommerce.orders", "ecommerce.users"],
    )

    assert not any("Attach to" in b for b in ui.buttons), ui.buttons
    body = " ".join(ui.labels)
    assert "streams a different set of tables" in body
    assert "not safe to attach" in body
    # Names what is uncovered, and why attaching would be wrong.
    assert "ecommerce.orders" in body
    assert "no ongoing changes" in body
    # Still tells the operator the idle infrastructure is costing money.
    assert "billing" in body


def test_attach_is_offered_when_the_pipeline_covers_the_selection() -> None:
    ui = _infra_banner(
        streamed=["ecommerce.orders", "ecommerce.users"],
        selected=["ecommerce.orders"],
    )

    assert "Attach to mysql-dsql-cdc-stack-0729-new" in ui.buttons
    body = " ".join(ui.labels)
    assert "Existing CDC infrastructure found" in body
    assert "not safe to attach" not in body


def test_attach_is_offered_when_the_candidate_tables_are_unknown() -> None:
    # A candidate whose parameters could not be read must stay attachable: the duplicate
    # -MSK warning is the whole point of the banner, and blocking on an unprobed stack
    # would push the operator toward deploying a second costly cluster.
    ui = _infra_banner(streamed=[], selected=["ecommerce.orders"])

    assert "Attach to mysql-dsql-cdc-stack-0729-new" in ui.buttons
    assert "not safe to attach" not in " ".join(ui.labels)


# ---------------------------------------------------------------------------
# "Automatic — gapless from Full Load" must only be claimed when it is TRUE
# ---------------------------------------------------------------------------


def _wm(*, binlog=True, gtid=True):
    from datetime import datetime, timezone

    from dsql_migrator.core.models import Watermark

    return Watermark(
        binlog_file="mysql-bin.000123" if binlog else None,
        binlog_position=45678 if binlog else None,
        gtid_executed="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5" if gtid else None,
        snapshot_timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
    )


def test_can_seed_offset_requires_the_binlog_position() -> None:
    """A GTID set alone cannot give a gapless start.

    The handoff works by writing the Full Load's position into MSK's connect-offsets
    topic, and that record is keyed on binlog file + pos: the in-VPC seeder REJECTS a
    watermark without them, and ``build_watermark_params`` returns all-empty values so the
    template skips the seeder and the connector starts from the CURRENT binlog. GTID is
    optional reinforcement the seeder adds when present, not a substitute.
    """
    from dsql_migrator.core.cdc import CdcResumePoint

    both = CdcResumePoint.from_watermark(_wm())
    assert both.can_seed_offset() is True

    gtid_only = CdcResumePoint.from_watermark(_wm(binlog=False))
    # The BROAD test still passes -- which is exactly how the UI came to promise gapless.
    assert gtid_only.has_coordinates() is True
    assert gtid_only.can_seed_offset() is False

    binlog_only = CdcResumePoint.from_watermark(_wm(gtid=False))
    assert binlog_only.can_seed_offset() is True

    nothing = CdcResumePoint.from_watermark(_wm(binlog=False, gtid=False))
    assert nothing.can_seed_offset() is False


def test_gapless_claim_matches_what_the_seeder_would_actually_do() -> None:
    """The UI's Automatic-availability must equal whether the seeder gets deployed.

    This is the invariant the bug broke: a GTID-only watermark showed "Automatic — gapless
    from Full Load (recommended)" and Ready, while the seeder was skipped entirely and
    every change made during the Full Load was lost.
    """
    from dsql_migrator.core.cdc import CdcResumePoint, build_watermark_params

    for binlog in (True, False):
        for gtid in (True, False):
            watermark = _wm(binlog=binlog, gtid=gtid)
            ui_says_gapless = CdcResumePoint.from_watermark(
                watermark
            ).can_seed_offset()
            params = dict(build_watermark_params(watermark))
            seeder_deployed = bool(params["WatermarkBinlogFile"])
            assert ui_says_gapless == seeder_deployed, (
                f"binlog={binlog} gtid={gtid}: UI says gapless={ui_says_gapless} "
                f"but seeder deployed={seeder_deployed}"
            )


def _start_card(watermark):
    from dsql_migrator.core.cdc import CdcResumePoint
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_start_point_card

    resume = CdcResumePoint.from_watermark(watermark) if watermark else None
    usable = resume is not None and resume.can_seed_offset()
    gtid_only = (
        resume is not None and not resume.can_seed_offset()
        and bool(resume.gtid_executed)
    )

    ui = _DetailRowUi()
    ui.radios: list[dict] = []
    _orig_getattr = None

    def _radio(options=None, *_a, **_k):
        if isinstance(options, dict):
            ui.radios.append(options)
        return ui._El(ui)

    ui.radio = _radio
    _render_cdc_start_point_card(
        ui,
        DataMigrationState(),
        lambda: None,
        wm_resume=resume,
        wm_usable=usable,
        effective_resume=resume if usable else None,
        mode="auto",
        locked=False,
        wm_gtid_only=gtid_only,
    )
    return ui


def test_gtid_only_watermark_does_not_claim_gapless() -> None:
    ui = _start_card(_wm(binlog=False))

    labels = " ".join(v for options in ui.radios for v in options.values())
    assert "gapless from Full Load" not in labels
    assert "has no binlog position" in labels
    # It must NOT claim the Full Load never ran -- there IS a watermark.
    assert "needs a Full Load watermark" not in labels

    body = " ".join(ui.labels)
    assert "cannot give a gapless start" in body
    assert "CURRENT" in body  # says where streaming WOULD start
    assert "REPLICATION CLIENT" in body  # names the actual fix
    assert "re-run the Full Load" in body


def test_usable_watermark_still_claims_gapless() -> None:
    ui = _start_card(_wm())

    labels = " ".join(v for options in ui.radios for v in options.values())
    assert "gapless from Full Load (recommended)" in labels
    assert "has no binlog position" not in labels
    assert "cannot give a gapless start" not in " ".join(ui.labels)


def test_absent_watermark_keeps_the_generic_wording() -> None:
    # No watermark at all is a different situation (no Full Load in this session), so the
    # GTID-specific guidance must not appear.
    ui = _start_card(None)

    labels = " ".join(v for options in ui.radios for v in options.values())
    assert "needs a Full Load watermark" in labels
    assert "REPLICATION CLIENT" not in " ".join(ui.labels)


# ---------------------------------------------------------------------------
# CDC step: when attach is wrong, DEPLOY must look like the way forward
# ---------------------------------------------------------------------------


class _CdcPanelUi(_DetailRowUi):
    """Adds expansion capture (title/icon/open-state) and button labels."""

    def __init__(self) -> None:
        super().__init__()
        self.buttons: list[str] = []
        self.expansions: list[tuple[str, object, object]] = []

    def button(self, text="", *_a, **_k):
        if text:
            self.buttons.append(str(text))
        return self._El(self)

    def expansion(self, text="", *_a, **kwargs):
        self.expansions.append((str(text), kwargs.get("icon"), kwargs.get("value")))
        return self._El(self)


def _cdc_prep_panel(*, streamed, selected):
    from dsql_migrator.core.models import TableSelection
    from dsql_migrator.ui.data_migration._cdc_ui import _render_cdc_infra_prep_section

    state = DataMigrationState()
    state.set_cdc_stack_phase(None)  # no stack of our own -> the adopt branch
    state.set_cdc_other_stacks([("mysql-dsql-cdc-stack-0729-new", "UPDATE_COMPLETE")])
    state.set_cdc_other_stack_tables(
        {"mysql-dsql-cdc-stack-0729-new": list(streamed)}
    )
    state.set_selection(TableSelection(selected_tables=list(selected)))

    ui = _CdcPanelUi()
    _render_cdc_infra_prep_section(
        ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
    )
    return ui


def _deploy_expansion(ui):
    return next(e for e in ui.expansions if "Deploy" in e[0])


def test_cdc_step_hides_attach_and_leads_with_deploy_on_a_scope_mismatch() -> None:
    """When attaching is wrong, deploying is the ONLY way forward — so it must lead.

    Reported (v0.1.210): the panel showed a prominent blue "Attach to
    mysql-dsql-cdc-stack-0729-new" the operator must NOT press, while the correct action
    sat collapsed behind a warning-triangle expansion labelled "Deploy a separate CDC
    pipeline instead" — so the right step looked like the risky one and was hidden.
    """
    ui = _cdc_prep_panel(
        streamed=["ecommerce_demo.orders"],
        selected=["ecommerce.orders", "ecommerce.users"],
    )

    # The attach button is gone, replaced by the reason.
    assert not any("Attach to" in b for b in ui.buttons), ui.buttons
    body = " ".join(ui.labels)
    assert "streams a different set of tables" in body
    assert "not safe to attach" in body

    title, icon, opened = _deploy_expansion(ui)
    assert title == "Deploy a CDC pipeline for this table set"
    assert "instead" not in title  # it is not an alternative; it is the path
    assert icon == "rocket_launch"  # no warning glyph on the correct action
    assert opened is True  # and it is not hidden behind a fold


def test_cdc_step_keeps_deploy_de_emphasised_when_attach_is_valid() -> None:
    # A second MSK cluster is expensive and rarely intended, so when attaching IS
    # appropriate the deploy path stays collapsed and flagged.
    ui = _cdc_prep_panel(
        streamed=["ecommerce.orders", "ecommerce.users"], selected=["ecommerce.orders"]
    )

    assert "Attach to mysql-dsql-cdc-stack-0729-new" in ui.buttons
    title, icon, opened = _deploy_expansion(ui)
    assert title == "Deploy a separate CDC pipeline instead"
    assert icon == "warning"
    assert not opened


def test_cdc_step_mismatch_notice_points_at_the_deploy_and_the_cost() -> None:
    ui = _cdc_prep_panel(
        streamed=["ecommerce_demo.orders"], selected=["ecommerce.orders"]
    )

    body = " ".join(ui.labels)
    # Says what to do...
    assert "Deploy a pipeline for this table set below" in body
    # ...and that the useless stack is still costing money.
    assert "keeps billing" in body


def test_cdc_step_gates_automatic_on_can_seed_offset_not_has_coordinates() -> None:
    """The CDC step must gate Automatic on what the SEEDER needs.

    A mutation swapping ``can_seed_offset()`` back to ``has_coordinates()`` passed every
    other test here, because they call the card directly with a pre-computed flag -- the
    WIRING was untested. That is the same class of gap as the earlier restored-session and
    detail=None bugs, and here it silently restores the exact defect: a GTID-only watermark
    would again claim "gapless from Full Load" while the seeder was skipped.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    tree = ast.parse(inspect.getsource(_cdc_ui._render_cdc_source_config_card))
    assigns = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id in ("wm_usable", "wm_gtid_only")
            for t in node.targets
        )
    ]
    joined = " ".join(assigns)
    assert "can_seed_offset()" in joined, (
        "Automatic availability must key off can_seed_offset(), not has_coordinates()"
    )
    assert "wm_usable = wm_resume is not None and wm_resume.has_coordinates()" not in (
        joined
    )
    # And the GTID-only case must be derived, so the card can explain the real cause.
    assert "gtid_executed" in joined


# ---------------------------------------------------------------------------
# Sink MCU knob: config -> CFN parameter wiring
# ---------------------------------------------------------------------------


def test_every_cdc_param_build_passes_the_configured_sink_mcu() -> None:
    """Each param-builder call site must pass ``sink_mcu_count``.

    The builders DEFAULT this argument, so a forgotten call site is silently wrong
    rather than a TypeError: the Settings knob would appear to work while that path
    kept deploying the template default. The three sites matter for different reasons
    -- the infra create records the value on a fresh stack, Start CDC is what actually
    creates/updates the sink connector, and the read-only params preview must not
    advertise a value the deploy then contradicts. This is the "state exists but was
    never wired into the render/deploy" gap, so it is asserted structurally.
    """
    import ast
    import pathlib

    from dsql_migrator.ui.data_migration import _cdc_ui

    tree = ast.parse(pathlib.Path(_cdc_ui.__file__).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in ("build_cdc_stack_params", "build_cdc_infra_params")
    ]
    # Two Start-CDC-path builds (preview + deploy) and one infra create.
    assert len(calls) == 3, f"expected 3 param builds, found {len(calls)}"
    for call in calls:
        kwargs = {kw.arg for kw in call.keywords if kw.arg}
        assert "sink_mcu_count" in kwargs, (
            f"{call.func.id} at line {call.lineno} does not pass sink_mcu_count"
        )


def test_sink_mcu_is_read_fresh_per_deploy_not_captured_once() -> None:
    """A change in Settings must reach the NEXT Start CDC without a restart.

    ``_sink_mcu_count`` calls ``load_config()`` per invocation because that is what
    re-reads the environment ``set_tuning_value`` writes to. Reading the config once
    at import/render would pin the value for the process lifetime, so the knob would
    appear to save and then do nothing until a restart.
    """
    import os

    from dsql_migrator.config import ENV_PREFIX
    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT
    from dsql_migrator.ui.data_migration._cdc_ui import _sink_mcu_count

    key = f"{ENV_PREFIX}CDC_SINK_MCU_COUNT"
    previous = os.environ.get(key)
    try:
        os.environ.pop(key, None)
        assert _sink_mcu_count() == CDC_DEFAULT_SINK_MCU_COUNT
        # A later change is observed by the very next call -- no restart, no re-import.
        os.environ[key] = "8"
        assert _sink_mcu_count() == 8
        os.environ[key] = "2"
        assert _sink_mcu_count() == 2
    finally:
        if previous is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = previous


def test_sink_mcu_falls_back_instead_of_blocking_a_deploy() -> None:
    """An unreadable config must not break Start CDC.

    Raising here would abort the deploy over a settings problem; returning some other
    number would silently resize the connector. The template-matching default is the
    only safe answer.
    """
    import dsql_migrator.config as cfg
    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT
    from dsql_migrator.ui.data_migration._cdc_ui import _sink_mcu_count

    original = cfg.load_config

    def _boom(*_a, **_k):
        raise RuntimeError("config unreadable")

    cfg.load_config = _boom  # type: ignore[assignment]
    try:
        assert _sink_mcu_count() == CDC_DEFAULT_SINK_MCU_COUNT
    finally:
        cfg.load_config = original  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Start CDC notice: the table set is already locked, so the copy must not tell
# the user to go pick tables -- and must name the remedy that actually works.
# ---------------------------------------------------------------------------


def test_start_cdc_button_only_renders_when_the_picker_is_already_locked() -> None:
    """Establishes the premise behind the notice copy: the set is ALWAYS frozen here.

    ``_render_cdc_start_button`` is called only for card phase ``"infra"``, which needs a
    probed ``cdc_stack_phase == "infra"``; that makes ``cdc_infra_prep_state`` return
    ``"ready"``, which is exactly what ``selection_lock_reason``'s CDC clause locks on.
    The clause is scoped to types with a ``"cdc"`` sub-step -- and those are the only
    types that can reach this button. So "pick all your tables before you start" was
    advice the operator could not act on: the checkboxes were disabled while the tip
    pointed at them.
    """
    from dsql_migrator.ui.data_migration import selection_lock_reason
    from dsql_migrator.ui.data_migration._cdc_ui import (
        cdc_infra_prep_state,
        classify_cdc_card_phase,
    )
    from dsql_migrator.ui.data_migration._models import MigrationType, substeps_for_type
    from dsql_migrator.ui.workflow import StepStatus

    state = DataMigrationState()
    state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
    jm = _StubJobManager({})

    assert (
        classify_cdc_card_phase(
            [], state.cdc_stack_name, state.cdc_stack_phase,
            running_names=[], failed_names=[],
        )
        == "infra"
    )
    assert cdc_infra_prep_state(state, jm) == "ready"

    cdc_types = [t for t in MigrationType if "cdc" in substeps_for_type(t)]
    assert cdc_types
    for mtype in cdc_types:
        assert selection_lock_reason(
            state, jm, status=StepStatus.NOT_STARTED,
            migration_type=mtype, has_job=False,
        ), f"picker unexpectedly unlocked for {mtype}"


def test_full_load_committed_tracks_the_lock_clause_that_start_over_owns() -> None:
    """Which remedy the notice may offer depends on WHICH lock applies.

    ``selection_lock_reason``'s Full-Load clause takes precedence and -- unlike the CDC
    clause -- deleting the cdc-stack does NOT release it, because the export really did
    run against this set. Verified directly below, since this is the fact that makes the
    two remedies non-interchangeable.
    """
    from dsql_migrator.ui.data_migration import selection_lock_reason
    from dsql_migrator.ui.data_migration._cdc_ui import _full_load_committed
    from dsql_migrator.ui.data_migration._models import MigrationType
    from dsql_migrator.ui.workflow import StepStatus

    jm = _StubJobManager({})

    # Deleting the CDC infra (phase back to absent) does NOT unlock a session whose
    # Full Load already finished -- so "delete the infrastructure to re-scope" would be
    # a ~45 min teardown that changes nothing about the picker.
    torn_down = DataMigrationState()
    assert selection_lock_reason(
        torn_down, jm, status=StepStatus.DONE,
        migration_type=MigrationType.FULL_LOAD_AND_CDC, has_job=False,
    ), "a finished Full Load must stay locked after a CDC teardown"

    # ... whereas a CDC-only session IS released by that teardown.
    assert selection_lock_reason(
        torn_down, jm, status=StepStatus.NOT_STARTED,
        migration_type=MigrationType.CDC_ONLY, has_job=False,
    ) is None

    # The helper the notice branches on agrees, and never claims a Full Load for a
    # migration type that has no Full Load step.
    class _Job:
        pass

    for mtype in MigrationType:
        state = DataMigrationState()
        state.migration_type = mtype
        assert _full_load_committed(None, state) is False
        expected = mtype is not MigrationType.CDC_ONLY
        assert _full_load_committed(_Job(), state) is expected, mtype


def test_start_cdc_shows_the_table_set_plainly_with_the_why_on_hover() -> None:
    """On the FIRST start this is not a warning -- it is the "which tables?" answer.

    It used to be a second full-width blue notice directly under "Ready to start CDC",
    giving a normal happy-path state two equal-weight boxes and burying the one line the
    operator scans for (WHICH tables stream) inside a paragraph about MSK partition
    accounting. The table set must stay VISIBLE (it is the verifiable fact, and hiding it
    would be the hover-only anti-pattern); the immutability rationale is background needed
    at most once, so it moves to an info tooltip.

    The remedy in that tooltip still differs by situation, because the two locks are not
    interchangeable: after a Full Load only Start over re-scopes.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._models import MigrationType

    class _Job:
        status = "DONE"
        watermark = None

    class _Table:
        def __init__(self, name):
            self.name = name

    class _Inventory:
        def __init__(self, tables):
            self.tables = tables

    def _render(*, job, mtype, inventory=None):
        ui = _RecordingUi()
        state = DataMigrationState()
        state.migration_type = mtype
        state.set_cdc_stack_phase("infra", status="CREATE_COMPLETE")
        state.job_id = "job-1" if job is not None else None
        jm = _StubJobManager({"job-1": job} if job is not None else {})
        _cdc_ui._render_cdc_start_button(
            ui, state, jm, lambda: None, inventory=inventory, session=None
        )
        return ui

    # A real watermark + inventory, so the table line actually resolves (with None the
    # branch renders a generic fallback and the invariant below would be vacuous).
    from datetime import datetime, timezone

    from dsql_migrator.core.models import Watermark

    covered = {f"ecommerce.t{i}": 100 for i in range(8)}
    inventory = _Inventory([_Table(name) for name in covered])

    class _LoadedJob:
        status = "DONE"
        # Built inline: a class body cannot read the enclosing function's locals.
        watermark = Watermark(
            binlog_file="mysql-bin.000042",
            binlog_position=120,
            snapshot_timestamp=datetime(2026, 8, 1, tzinfo=timezone.utc),
            table_row_counts={f"ecommerce.t{i}": 100 for i in range(8)},
        )

    # --- Full Load + CDC, load finished -> Start over is the only real exit ---
    after = _render(
        job=_LoadedJob(), mtype=MigrationType.FULL_LOAD_AND_CDC, inventory=inventory
    )
    visible = " ".join(after.texts)
    hover = " ".join(after.tooltips)

    # No second notice competing with "Ready to start CDC".
    assert "Ready to start CDC" in visible
    assert "This table set is now fixed" not in visible
    # THE key invariant: the table set stays VISIBLE. It is the one line the operator
    # scans for and the only verifiable claim here, so it must never move to hover.
    assert "Will stream 8 tables" in visible
    assert "ecommerce.t0" in visible
    assert "Will stream" not in hover
    # The rationale is available, but on hover -- not as a paragraph.
    assert "This set is fixed" in hover
    assert "partitions" in hover
    # After a Full Load: Start over, and it says WHY the set is what it is.
    assert "Start over" in hover
    assert "gapless" in hover
    assert "delete the CDC infrastructure" not in hover

    # --- CDC-only, no Full Load -> delete + redeploy genuinely re-scopes ---
    cdc_only = _render(job=None, mtype=MigrationType.CDC_ONLY)
    cdc_hover = " ".join(cdc_only.tooltips)
    assert "delete the CDC infrastructure" in cdc_hover
    assert "Start over" not in cdc_hover

    # Neither variant tells the operator to go pick tables (the picker is locked).
    for blob in (visible, hover, " ".join(cdc_only.texts), cdc_hover):
        assert "Pick all your tables" not in blob
        assert "up front" not in blob




# ---------------------------------------------------------------------------
# CDC restart: a stopped-but-previously-streamed pipeline needs NO watermark
# ---------------------------------------------------------------------------


def test_cdc_committed_offset_signal_distinguishes_stopped_from_fresh() -> None:
    """``DeploySink=true`` + blank bootstrap == "streamed, then stopped".

    Unambiguous because of how the three writes differ: the infra create pins
    ``DeploySink="false"``, only Start CDC sets it ``"true"``, and Stop overrides ONLY
    ``MskBootstrapServers`` (everything else rides through as ``UsePreviousValue``). So
    that combination is unreachable by a fresh infra-only deploy.

    It matters because the resume position lives in the source connector's offsets topic,
    which is pinned to a FIXED name and therefore survives a Stop -- so a restart needs no
    watermark at all.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_has_committed_offset

    # Fresh infra: nothing has streamed, so a start point IS required.
    assert cdc_has_committed_offset(
        {"MskBootstrapServers": "", "DeploySink": "false"}
    ) is False
    # Currently streaming: not a resume situation (the Start button is not shown).
    assert cdc_has_committed_offset(
        {"MskBootstrapServers": "b-1:9098", "DeploySink": "true"}
    ) is False
    # THE case: stopped after a real start -> resume offset exists.
    assert cdc_has_committed_offset(
        {"MskBootstrapServers": "", "DeploySink": "true"}
    ) is True
    # A whitespace-only bootstrap is still "blank" (CFN round-trips empty strings).
    assert cdc_has_committed_offset(
        {"MskBootstrapServers": "   ", "DeploySink": "true"}
    ) is True
    assert cdc_has_committed_offset(
        {"MskBootstrapServers": "", "DeploySink": "True"}
    ) is True
    # Unprobed / unreadable -> False, i.e. fall back to REQUIRING a start point rather
    # than claiming a resume point that may not exist.
    assert cdc_has_committed_offset(None) is False
    assert cdc_has_committed_offset({}) is False


def test_probe_records_the_committed_offset_signal_on_state() -> None:
    """The probe must WRITE the signal, or every reader stays False.

    This is the "state exists but is never populated" gap: each render test passes the
    flag in directly, so a probe that computed it and dropped it would leave the real UI
    permanently on the first-start path -- Start CDC dead after a Stop -- with a fully
    green suite. Asserted through the real probe, from a fake describe.
    """
    from dsql_migrator.ui.data_migration import _cdc_status as _status

    class _Discovery:
        def __init__(self, params):
            self.stack_status = "UPDATE_COMPLETE"
            self.current_parameters = params
            self.is_stable = True

    class _Deployer:
        def __init__(self, params):
            self._params = params

        def describe_stack_or_none(self, _name):
            return _Discovery(self._params)

        def list_cdc_stacks(self):
            return []

    class _Target:
        region = "us-east-1"

    class _Session:
        target_config = _Target()
        aws_profile = None

    def _probe(params):
        state = DataMigrationState()
        import dsql_migrator.core.cdc_deployer as deployer_mod

        original = deployer_mod.build_cdc_stack_deployer
        deployer_mod.build_cdc_stack_deployer = lambda *_a, **_k: _Deployer(params)
        try:
            _status._probe_cdc_stack_phase(state, _Session())
        finally:
            deployer_mod.build_cdc_stack_deployer = original
        return state

    # Stopped after a start -> the signal must land on the state.
    stopped = _probe({"MskBootstrapServers": "", "DeploySink": "true"})
    assert stopped.cdc_has_committed_offset is True
    assert stopped.cdc_stack_phase == "infra"  # same describe drove both

    # Fresh infra -> stays False, so the start-point guard still applies.
    fresh = _probe({"MskBootstrapServers": "", "DeploySink": "false"})
    assert fresh.cdc_has_committed_offset is False

    # Default before any probe: False (never claim a resume point that may not exist).
    assert DataMigrationState().cdc_has_committed_offset is False


def test_start_cdc_enabled_on_restart_without_any_watermark() -> None:
    """The reported bug: Start CDC was dead after a Stop with no Full Load job.

    The watermark is read off the Full Load JOB record, so after an app restart (job
    record pruned) or in a CDC-only session there is none -- and Start CDC went disabled
    with "Set the CDC start point above first", even though the connector's committed
    offset would have resumed streaming perfectly. That pushed the operator toward
    re-entering binlog coordinates by hand, or re-running the whole Full Load, to recover
    something nothing had lost.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    def _render(*, committed: bool):
        ui = _RecordingUi()
        state = DataMigrationState()
        state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
        state.set_cdc_has_committed_offset(committed)
        # No job at all -> no watermark, the situation that used to dead-end.
        state.job_id = None
        _cdc_ui._render_cdc_start_button(
            ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
        )
        return ui

    resumed = _render(committed=True)
    blob = " ".join(resumed.texts)
    # Enabled, and framed as a RESUME rather than a first start.
    assert "Set the CDC start point above first." not in blob
    assert "Ready to resume CDC" in blob
    assert "continues from exactly where streaming stopped" in blob
    # It must state the two things the operator needs: no gap and no re-load.
    assert "no gap" in blob
    assert "no re-load" in blob

    # Without a committed offset (a genuinely fresh stack) it stays the FIRST-start copy.
    # That distinction is what keeps the real guard intact: a fresh stack with no
    # watermark must not start, because the connector would begin at the source's CURRENT
    # binlog and silently lose every change made during the Full Load. (Which of the two
    # blocking hints shows -- prerequisites or start point -- depends on the prereq
    # report; the invariant asserted here is that it is not framed as a resume.)
    fresh = _render(committed=False)
    fresh_blob = " ".join(fresh.texts)
    assert "Ready to start CDC" in fresh_blob
    assert "Ready to resume CDC" not in fresh_blob
    assert "continues from exactly where streaming stopped" not in fresh_blob


def test_start_cdc_button_is_actually_enabled_on_restart() -> None:
    """The BUTTON state, not just the copy -- this is the defect itself.

    Asserting on wording alone would pass even if the button stayed disabled, which was
    the whole bug: Start CDC was dead after a Stop whenever the Full Load job record was
    gone (app restart) or never existed (CDC-only session).
    """
    from dsql_migrator.core.models import MigrationMode
    from dsql_migrator.ui.data_migration import _cdc_ui

    class _El:
        def __init__(self, ui, label=""):
            self._ui = ui
            self._label = label

        def props(self, value="", *_a, **_k):
            if self._label == "Start CDC" and "disable" in str(value):
                self._ui.start_disabled = True
            return self

        def __getattr__(self, _name):
            # Everything else (classes/tooltip/style/on/open/close/...) is a chainable
            # no-op; only `props` on the Start button carries signal.
            return lambda *_a, **_k: self

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    class _Ui:
        def __init__(self):
            self.texts: list[str] = []
            self.start_disabled = False

        def button(self, text="", *_a, **_k):
            self.texts.append(str(text))
            return _El(self, str(text))

        def label(self, text="", *_a, **_k):
            self.texts.append(str(text))
            return _El(self)

        def badge(self, text="", *_a, **_k):
            self.texts.append(str(text))
            return _El(self)

        def notify(self, *_a, **_k):
            return None

        def refreshable(self, fn):
            def _w(*a, **k):
                return fn(*a, **k)

            _w.refresh = lambda *a, **k: None
            return _w

        def __getattr__(self, _name):
            return lambda *_a, **_k: _El(self)

    def _render(*, committed):
        ui = _Ui()
        state = DataMigrationState()
        state.set_cdc_stack_phase("infra", status="UPDATE_COMPLETE")
        state.set_cdc_has_committed_offset(committed)
        state.job_id = None  # no Full Load job -> no watermark
        # Prereqs already passed, so the ONLY thing that could disable the button is the
        # start-point gate under test.
        state.set_prereq_gated_mode(MigrationMode.CDC)
        _cdc_ui._render_cdc_start_button(
            ui, state, _StubJobManager({}), lambda: None, inventory=None, session=None
        )
        return ui

    resumed = _render(committed=True)
    assert resumed.start_disabled is False, "Start CDC must be pressable on a restart"
    assert "Set the CDC start point above first." not in " ".join(resumed.texts)

    # The guard still holds for a genuinely fresh stack: starting there with no watermark
    # would begin at the source's CURRENT binlog and silently lose the Full Load window.
    fresh = _render(committed=False)
    assert fresh.start_disabled is True
    assert "Set the CDC start point above first." in " ".join(fresh.texts)


def test_start_point_card_reports_the_resume_instead_of_demanding_a_watermark() -> None:
    """The card must not contradict the button beneath it.

    On a restart it used to badge "Action needed" and offer "Automatic — needs a Full Load
    watermark (unavailable)" while Start CDC was enabled and would have worked. Both are
    wrong on a resume: no watermark is needed, and there is no start point left to choose
    (the position is in the offsets topic, which this card cannot set).
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _RecordingUi()
    _cdc_ui._render_cdc_start_point_card(
        ui,
        DataMigrationState(),
        lambda: None,
        wm_resume=None,
        wm_usable=False,
        effective_resume=None,
        mode="auto",
        locked=False,
        session=None,
        wm_gtid_only=False,
        resumes_from_offset=True,
    )
    blob = " ".join(ui.texts)
    assert "Resuming from the last streamed position" in blob
    assert "Ready" in ui.texts
    assert "Action needed" not in ui.texts
    # The now-irrelevant choice is not offered at all.
    assert "needs a Full Load watermark" not in blob
    assert "Manual — enter a GTID or binlog position" not in blob


def test_stop_dialog_promises_the_position_survives() -> None:
    """The operator forms the expectation HERE, before pressing Stop.

    "MSK ... are kept, so you can restart with Start CDC" said the infrastructure
    survives but never that the stream POSITION does -- and the reasonable guess (that
    deleting the connectors loses it) is wrong and expensive.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _RecordingUi()
    state = DataMigrationState()
    _cdc_ui._open_cdc_stop_dialog(ui, state, lambda: None)
    blob = " ".join(ui.texts)
    assert "the recorded stream position" in blob
    assert "continues from exactly where streaming stopped" in blob
    assert "no Full Load or start point needed again" in blob
    assert "stop and restart as often as you like" in blob


# ---------------------------------------------------------------------------
# Identity sequences must be advanced past the loaded rows (post-cut-over safety)
# ---------------------------------------------------------------------------


def test_identity_sequence_sync_runs_only_after_a_complete_load() -> None:
    """MAX(pk) is what the sequence has to clear, so a PARTIAL load must not sync it.

    The converter's IDENTITY_WITH_CACHE strategy emits GENERATED BY DEFAULT AS IDENTITY,
    and an explicitly-supplied value (which is how Full Load writes the source's keys) does
    NOT advance the sequence -- so without this the application's first insert after
    cut-over dies on a duplicate key. Verified live on ap-northeast-2.

    But syncing an INCOMPLETE load would set the sequence from a partial high-water mark,
    and the rows still to come could then collide -- so it is gated on completion.
    """
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    calls: list = []

    def _fake_sync(names, **_kw):
        calls.append(list(names))
        return {n: 101 for n in names}

    class _Handle:
        cancelled = False
        job_id = "job-x"

        def update(self, _fn):
            return None

    class _Inputs:
        target_config = object()
        aws_profile = None

    def _finalize(*, failed, quarantined, cancelled=False, accept=False):
        calls.clear()
        handle = _Handle()
        handle.cancelled = cancelled
        counts = _engine._RunCounts(
            real_failed=failed, quarantined=quarantined
        )
        try:
            _engine._finalize_run(
                handle,
                "job-x",
                ["ecommerce.orders"],
                counts,
                _engine.ErrorLogStore(),
                accept_quarantined_rows=accept,
                inputs=_Inputs(),
                sync_sequences=_fake_sync,
            )
        except Exception:
            pass  # an incomplete run raises; the assertion is about `calls`
        return list(calls)

    # Clean run -> sync.
    assert _finalize(failed=0, quarantined=0) == [["ecommerce.orders"]]
    # A real failure -> the load is incomplete, so MAX(pk) is not the final high-water
    # mark; syncing now could still leave a collision for the rows yet to load.
    assert _finalize(failed=1, quarantined=0) == []
    # Cancelled -> same reasoning.
    assert _finalize(failed=0, quarantined=0, cancelled=True) == []


def test_identity_sequence_sync_never_fails_a_completed_load() -> None:
    """A load that finished correctly must not be reported FAILED over this follow-up."""
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    class _Inputs:
        target_config = object()
        aws_profile = None

    def _boom(_names, **_kw):
        raise RuntimeError("DSQL unreachable")

    # Swallowed, and reported as {} so the caller logs nothing misleading.
    assert (
        _engine._sync_identity_sequences_after_load(
            _Inputs(), ["ecommerce.orders"], sync=_boom
        )
        == {}
    )
    # No tables -> no connection attempt at all.
    assert _engine._sync_identity_sequences_after_load(_Inputs(), [], sync=_boom) == {}


def test_identity_sequence_sync_is_wired_into_both_load_paths() -> None:
    """``inputs`` must reach _finalize_run from BOTH the initial load and the retry.

    A retry that COMPLETES the load is exactly when the sync must run -- the first
    attempt's finalize skipped it because the run was incomplete. Missing the retry path
    would leave every recovered migration with an unsynced sequence, and the failure only
    appears after cut-over. This is the "state exists but was never wired" gap, so it is
    asserted structurally.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    tree = ast.parse(inspect.getsource(_engine))
    by_name = {
        n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)
    }
    for fn in ("run_full_load", "run_full_load_retry"):
        body = ast.unparse(by_name[fn])
        assert "inputs=inputs" in body, f"{fn} does not forward inputs to _finalize_run"
    # And the finalize only syncs on the success branch.
    finalize = ast.unparse(by_name["_finalize_run"])
    assert "_log_identity_sequence_sync(" in finalize


# ---------------------------------------------------------------------------
# LOB exclusion lock + undetermined CDC state (restart recovery)
# ---------------------------------------------------------------------------


class _LockJobManager:
    """Job manager whose single job has a fixed status, or is missing."""

    def __init__(self, status=None) -> None:
        self._status = status

    def get_status(self, job_id):
        if self._status is None:
            from dsql_migrator.core.job_manager import JobNotFoundError

            raise JobNotFoundError(job_id)

        class _J:
            pass

        j = _J()
        j.status = self._status
        return j


def test_lob_exclusion_is_editable_before_anything_is_committed() -> None:
    # Nothing deployed and nothing running -> the operator can still choose.
    from dsql_migrator.ui.data_migration._cdc_ui import lob_exclusion_lock

    state = DataMigrationState()

    assert lob_exclusion_lock(state, _LockJobManager()) == (False, None)


def test_lob_exclusion_locks_while_infrastructure_is_being_created() -> None:
    """The reported gap: the tick boxes stayed live during an infra create.

    ColumnExcludeList is a create-time stack parameter, so it was submitted with the
    stack -- a later tick changed state nothing would read, and the box silently lied
    about what CDC captures. This case DOES explain itself: the operator was just
    here choosing, so the transition needs naming.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import lob_exclusion_lock

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-1", kind="infra")

    locked, reason = lob_exclusion_lock(state, _LockJobManager("RUNNING"))
    assert locked
    assert reason is not None and "being created" in reason


def test_lob_exclusion_locks_and_explains_once_infrastructure_exists() -> None:
    """Deployed infrastructure locks the choice AND explains why + the remedy.

    The exclusion is fixed for the pipeline once the stack exists (e.g. after a Stop
    CDC, which keeps the stack and its committed offset), so the boxes must not be
    tickable. An earlier version left this silent on the theory the greyed boxes were
    self-explanatory, but a stopped-CDC operator reads a frozen box with no reason as
    a bug. So the lock now names its reason and the remedy (delete + redeploy). The
    reason is rendered NEUTRAL (not warning) by the panel -- severity calibration --
    but the text itself must be present.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import lob_exclusion_lock

    for phase in ("infra", "provisioning", "partial"):
        state = DataMigrationState()
        state.set_cdc_stack_phase(phase)
        locked, reason = lob_exclusion_lock(state, _LockJobManager())
        assert locked, f"phase {phase} must lock the exclusion"
        assert reason is not None and "delete the CDC infrastructure" in reason, (
            f"phase {phase} must explain the lock and name the delete+redeploy remedy"
        )


def test_lob_exclusion_lock_names_cdc_start_when_streaming_started() -> None:
    # The remedy differs: with connectors up the fix is Stop CDC, not deleting the
    # infrastructure, so the message must say so.
    from dsql_migrator.ui.data_migration._cdc_ui import lob_exclusion_lock

    state = DataMigrationState()
    state.set_cdc_connector_names(["mysql-source"])
    state.set_cdc_stack_phase("running")

    locked, reason = lob_exclusion_lock(state, _LockJobManager())
    assert locked
    assert reason is not None and "Stop CDC" in reason


def test_lob_exclusion_locks_once_a_full_load_committed_under_the_set() -> None:
    """The full_load_only -> cdc_only split-brain gap: a completed Full Load's exclusion
    must not be editable after switching to cdc_only to add replication.

    A full_load_only run excludes a column and loads (rows land NULL). The tool then
    invites the user to switch to cdc_only and stream onto the already-loaded target. In
    that state nothing is deployed/streaming, so the CDC-step LOB card was editable --
    un-excluding the column would make CDC populate it for post-snapshot changes while
    loaded rows stay NULL (silent split-brain). The lock now fires on the committed load
    (via full_load_committed), NOT released by deleting the stack; remedy is Start over.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import lob_exclusion_lock

    # Nothing deployed, nothing streaming -- the pre-deploy window that used to be open.
    state = DataMigrationState()
    locked, reason = lob_exclusion_lock(
        state, _LockJobManager(), full_load_committed=True
    )
    assert locked
    assert reason is not None
    assert "Full Load has already loaded data" in reason
    assert "Start over" in reason  # the correct re-scope remedy (not delete+redeploy)

    # Without the committed-load signal the same pre-deploy state stays editable
    # (a genuine fresh cdc_only run has nothing loaded yet).
    assert lob_exclusion_lock(state, _LockJobManager()) == (False, None)


def _render_lob_panel(*, locked: bool, lock_reason=None):
    """Render the LOB panel with one exclusion candidate and return the UI double."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="docs",
                columns=[
                    ColumnDef(name="id", mysql_type="int", nullable=False),
                    ColumnDef(name="payload", mysql_type="longtext"),
                ],
                primary_key=["id"],
            )
        ]
    )
    ui = _RecordingUi()
    _cdc_ui._render_cdc_lob_exclusion_panel(
        ui,
        DataMigrationState(),
        inventory,
        lambda: None,
        locked=locked,
        lock_reason=lock_reason,
    )
    return ui


def _render_lob_panel_migration_wide(*, state=None):
    """Render the panel in migration-wide (Full Load) mode with one LOB candidate."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="docs",
                columns=[
                    ColumnDef(name="id", mysql_type="int", nullable=False),
                    ColumnDef(name="payload", mysql_type="longtext"),
                ],
                primary_key=["id"],
            )
        ]
    )
    ui = _RecordingUi()
    _cdc_ui._render_cdc_lob_exclusion_panel(
        ui,
        state or DataMigrationState(),
        inventory,
        lambda: None,
        locked=False,
        lock_reason=None,
        migration_wide=True,
    )
    return ui


def test_lob_panel_migration_wide_wording_and_no_connector_preview() -> None:
    # On the Full Load screen the card speaks of "this migration" (not CDC capture)
    # and omits the Debezium column.exclude.list preview (a connector-only detail).
    ui = _render_lob_panel_migration_wide()
    joined = " ".join(ui.texts)

    assert ui.checkboxes, "candidates must still render tick boxes"
    assert "this migration" in joined
    assert "column.exclude.list" not in joined


def test_lob_panel_migration_wide_no_candidates_says_this_migration() -> None:
    # The "nothing to exclude" info notice also uses the migration-wide wording.
    from dsql_migrator.ui.data_migration import _cdc_ui

    inventory = SourceInventory(
        tables=[
            TableDef(
                name="plain",
                columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                primary_key=["id"],
            )
        ]
    )
    ui = _RecordingUi()
    _cdc_ui._render_cdc_lob_exclusion_panel(
        ui,
        DataMigrationState(),
        inventory,
        lambda: None,
        locked=False,
        lock_reason=None,
        migration_wide=True,
    )
    joined = " ".join(ui.texts)
    assert "this migration" in joined
    assert "CDC capture" not in joined


def test_lob_panel_boxes_are_live_when_not_locked() -> None:
    # The control point of the two tests below: unlocked really is tickable, so a
    # "always disabled" regression cannot pass by accident.
    ui = _render_lob_panel(locked=False)

    assert ui.checkboxes, "the panel must render a tick box for the candidate"
    assert all(b.enabled for b in ui.checkboxes)
    assert all(b.on_change is not None for b in ui.checkboxes)


def test_lob_panel_disables_the_boxes_and_drops_the_handler_when_locked() -> None:
    """Greying alone is not enough -- the click must not register either.

    Before this the boxes were fully live: the state changed and was then discarded.
    Asserted on the RENDERED widgets, so a cosmetic-only "grey but still clickable"
    version fails.
    """
    ui = _render_lob_panel(locked=True, lock_reason="Locked — CDC started.")

    assert ui.checkboxes, "the panel must still render the tick boxes when locked"
    assert not any(b.enabled for b in ui.checkboxes), "locked boxes must be disabled"
    assert all(b.on_change is None for b in ui.checkboxes), (
        "on_change must be dropped while locked, not just greyed"
    )


def test_lob_panel_shows_the_lock_reason_when_one_is_given() -> None:
    """A transient lock must actually say why, in visible copy (not a tooltip).

    The two transient cases (infra create in flight, CDC started) are the ones where
    the operator may be mid-decision and needs the remedy named, so the reason has to
    reach the screen rather than being computed and dropped.
    """
    ui = _render_lob_panel(
        locked=True,
        lock_reason="Locked — the excluded columns were handed to the connector.",
    )

    joined = " ".join(ui.texts)
    assert "handed to the connector" in joined, (
        "the lock reason must be rendered as visible text"
    )


def test_lob_panel_shows_no_lock_line_when_the_lock_has_no_reason() -> None:
    """A silent lock renders no warning text -- the greyed boxes carry it.

    Deployed CDC infrastructure is the normal state, and a warning line there read as
    though something had gone wrong. The boxes must still be frozen.
    """
    ui = _render_lob_panel(locked=True, lock_reason=None)

    assert not any(b.enabled for b in ui.checkboxes), "a silent lock still locks"
    joined = " ".join(ui.texts)
    assert "Locked" not in joined
    assert "Delete the CDC infrastructure" not in joined


def test_cdc_state_undetermined_distinguishes_unprobed_from_absent() -> None:
    """The restart-recovery bug: a blank CDC pipeline card.

    The card lumped "not yet probed" in with "absent" and offered a deploy form -- for
    infrastructure that may already be streaming. The probe needs a target region and
    returns silently without one, and a restored session's connections are untrusted
    until re-verified, so after a restart the phase is None with a live pipeline.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_state_is_undetermined

    unprobed = DataMigrationState()
    assert cdc_state_is_undetermined(unprobed) is True, "not looked yet -> unknown"

    # The probe reporting "absent" sets the checked flag, which is a real answer.
    probed_absent = DataMigrationState()
    probed_absent.set_cdc_stack_phase(None)
    assert cdc_state_is_undetermined(probed_absent) is False

    known = DataMigrationState()
    known.set_cdc_stack_phase("running")
    assert cdc_state_is_undetermined(known) is False


def test_cdc_card_shows_the_unknown_notice_instead_of_a_deploy_form() -> None:
    """The unknown branch must precede the absent branch, or it is unreachable.

    Offering the deploy form for an unprobed session risks a duplicate, billable MSK
    cluster for a pipeline that already exists.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui._render_cdc_start_action)
    tree = ast.parse(src.strip())
    lines = {
        node.func.id: node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert "cdc_state_is_undetermined" in lines, (
        "the card must distinguish unknown from absent"
    )
    assert "_render_cdc_state_unknown_notice" in lines
    # The unknown check gates a branch that comes before the deploy action.
    assert lines["cdc_state_is_undetermined"] < lines["_render_cdc_infra_deploy_action"]


def test_unknown_notice_says_the_pipeline_is_unaffected_and_names_the_remedy() -> None:
    """Asserted on the RENDERED text, not the source.

    The operator must not re-run Start CDC on a live pipeline, and must know the one
    action that recovers the display. Checking the rendered body also survives the
    string being re-wrapped across source lines.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    captured: list = []

    class _El:
        def classes(self, *a, **k):
            return self

        def props(self, *a, **k):
            return self

        def style(self, *a, **k):
            return self

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    class _Ui:
        def label(self, text="", *a, **k):
            captured.append(str(text))
            return _El()

        def __getattr__(self, _name):
            def _any(*a, **k):
                for arg in a:
                    if isinstance(arg, str):
                        captured.append(arg)
                for arg in k.values():
                    if isinstance(arg, str):
                        captured.append(arg)
                return _El()

            return _any

    _cdc_ui._render_cdc_state_unknown_notice(_Ui())
    body = " ".join(captured)

    assert "keeps streaming" in body, "must say the running pipeline is unaffected"
    assert "Re-verify the target connection" in body, "must name the remedy"
    assert "do not start cdc again" in body.lower()


def test_redeploy_prompt_gates_the_form_only_after_a_teardown() -> None:
    """A finished delete must ASK before the deploy form reappears.

    A CDC delete takes ~20 min and removes a billable MSK cluster; showing the ~20-line
    BYO-VPC form the instant it lands reads as though the tool were about to rebuild
    what the operator just paid to remove. A first-ever deploy is NOT gated -- there the
    form is the next step.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import (
        cdc_redeploy_needs_confirmation,
    )

    fresh = DataMigrationState()
    assert not cdc_redeploy_needs_confirmation(fresh), (
        "a first-ever deploy must not be gated behind an extra click"
    )

    after_infra = DataMigrationState()
    after_infra.set_cdc_deploy_job_id("job-1", kind="infra")
    assert not cdc_redeploy_needs_confirmation(after_infra), (
        "only a teardown gates the form, not any CDC lifecycle action"
    )

    after_delete = DataMigrationState()
    after_delete.set_cdc_deploy_job_id("job-2", kind="delete")
    assert cdc_redeploy_needs_confirmation(after_delete)


def test_redeploy_confirmation_latches_so_the_form_survives_refreshes() -> None:
    # The card re-renders on a timer, so a non-latching answer would bounce the
    # operator back to the prompt mid-typing.
    from dsql_migrator.ui.data_migration._cdc_ui import (
        cdc_redeploy_needs_confirmation,
    )

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-2", kind="delete")
    state.set_cdc_redeploy_confirmed(True)

    assert not cdc_redeploy_needs_confirmation(state)


def test_a_second_teardown_prompts_again() -> None:
    """deploy -> delete -> delete must not reuse the first "yes".

    Otherwise the second delete lands straight on the deploy form, which is the exact
    behaviour this gate exists to prevent.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import (
        cdc_redeploy_needs_confirmation,
    )

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-2", kind="delete")
    state.set_cdc_redeploy_confirmed(True)  # first prompt answered
    assert not cdc_redeploy_needs_confirmation(state)

    # A fresh delete is submitted; the submit path clears the latch.
    state.set_cdc_deploy_job_id("job-3", kind="delete")
    state.set_cdc_redeploy_confirmed(False)
    assert cdc_redeploy_needs_confirmation(state)


def test_delete_submit_clears_a_previously_confirmed_redeploy() -> None:
    """Pin the clear at the SUBMIT site, not just in the test above.

    The predicate cannot see a stale latch on its own -- whoever submits the delete has
    to reset it, so assert the source of truth for that ordering.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui)
    tree = ast.parse(src)
    # Find the statement that submits a delete job and the reset that must follow it.
    set_delete_line = None
    reset_line = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        rendered = ast.unparse(node)
        if "set_cdc_deploy_job_id" in rendered and "delete" in rendered:
            set_delete_line = node.lineno
        if "set_cdc_redeploy_confirmed(False)" in rendered:
            reset_line = node.lineno
    assert set_delete_line is not None, "the delete submit site must exist"
    assert reset_line is not None, (
        "submitting a delete must clear any latched redeploy confirmation"
    )
    assert reset_line > set_delete_line, (
        "the reset must follow the delete submit so the new teardown re-prompts"
    )


def test_redeploy_prompt_leads_with_the_deletion_outcome() -> None:
    """The operator waited ~20 min for this: say it's gone and no longer billing.

    Rendered output, so a version that only shows a bare button fails.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _RecordingUi()
    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-2", kind="delete")
    _cdc_ui._render_cdc_redeploy_prompt(ui, state, lambda: None)

    joined = " ".join(ui.texts)
    assert "CDC infrastructure deleted" in joined
    assert "no longer" in joined, "say the cluster stopped costing money"
    # Redeploy is offered, but as an explicit opt-in that states the cost.
    assert "Redeploy CDC infrastructure" in joined
    assert "10-15 minutes" in joined and "billable" in joined
    # And it must NOT dump the BYO-VPC form here.
    assert "Provide your VPC" not in joined


def _render_start_action_after_delete(*, confirmed: bool):
    """Render the whole CDC lifecycle card in the "absent, just deleted" state."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _RecordingUi()
    state = DataMigrationState()
    # The probe has reported and found nothing -> the real "absent" branch (not the
    # undetermined one), which is where the deploy form used to appear immediately.
    state.set_cdc_stack_phase(None)
    state.set_cdc_deploy_job_id("job-del", kind="delete")
    if confirmed:
        state.set_cdc_redeploy_confirmed(True)
    _cdc_ui._render_cdc_start_action(
        ui,
        state,
        _StubJobManager({}),  # the delete job is finished/unknown -> not in flight
        lambda: None,
        inventory=None,
        session=None,
    )
    return ui


def test_lifecycle_card_asks_before_reoffering_the_deploy_form_after_a_delete() -> None:
    """Wiring test: the absent branch must route through the redeploy prompt.

    The predicate and the prompt can both be correct while the card never calls them --
    which is exactly the state the CDC step was in. Renders the real card.
    """
    joined = " ".join(_render_start_action_after_delete(confirmed=False).texts)

    assert "CDC infrastructure deleted" in joined, (
        "the card must confirm the teardown instead of jumping to a deploy form"
    )
    assert "Provide your VPC" not in joined, (
        "the BYO-VPC deploy form must wait behind the explicit redeploy prompt"
    )


def test_lifecycle_card_shows_the_deploy_form_once_redeploy_is_confirmed() -> None:
    # The control: saying yes must actually get the operator to the form, or the gate
    # would be a dead end.
    joined = " ".join(_render_start_action_after_delete(confirmed=True).texts)

    assert "Provide your VPC" in joined, (
        "confirming redeploy must reveal the infrastructure form"
    )


class _WorkflowSess:
    """Session double that holds a real WorkflowState so status writes are observable."""

    target_config = None
    aws_profile = None

    def __init__(self, cdc_status=None):
        from dsql_migrator.core.models import WorkflowState
        from dsql_migrator.ui.workflow import WorkflowStep, with_status

        self.workflow = WorkflowState()
        if cdc_status is not None:
            self.workflow = with_status(self.workflow, WorkflowStep.CDC, cdc_status)

    def set_workflow(self, workflow) -> None:
        self.workflow = workflow


def _discover_with_connectors(names, *, cdc_status):
    """Run the pre-wired discovery branch with ``names`` present on AWS."""
    from dsql_migrator.ui.data_migration._cdc_status import _ensure_cdc_controller

    state = DataMigrationState()

    class _Ctl:
        def list_connectors(self):
            return [{"connectorName": n, "connectorState": "RUNNING"} for n in names]

    state.set_cdc_controller(_Ctl())
    state._cdc_discovery_monotonic = None
    sess = _WorkflowSess(cdc_status)
    _ensure_cdc_controller(state, sess)
    return sess, state


def test_cdc_step_drops_back_to_not_started_when_the_connectors_are_gone() -> None:
    """A Stop CDC / infrastructure Delete must move the badge off IN_PROGRESS.

    Promotion was one-way: detected connectors set NOT_STARTED -> IN_PROGRESS and
    nothing moved it back, so after a teardown the Data Migration badge kept reading
    "CDC: IN_PROGRESS" for a pipeline with no connectors -- and since the workflow is
    persisted, the stale value returned on every restore.
    """
    from dsql_migrator.ui.workflow import WorkflowStep, get_status

    # Was streaming; AWS now reports no connectors of mine (post Stop/Delete).
    sess, state = _discover_with_connectors([], cdc_status=StepStatus.IN_PROGRESS)

    assert state.cdc_connector_names == []
    assert get_status(sess.workflow, WorkflowStep.CDC) is StepStatus.NOT_STARTED


def test_cdc_step_is_promoted_while_connectors_exist() -> None:
    # The control for the test above: detection must still promote, or the downgrade
    # would just be a badge that never lights up.
    from dsql_migrator.core.cdc import cdc_expected_connector_names
    from dsql_migrator.ui.workflow import WorkflowStep, get_status

    src, sink = cdc_expected_connector_names(DataMigrationState().cdc_stack_name)
    sess, state = _discover_with_connectors(
        [src, sink], cdc_status=StepStatus.NOT_STARTED
    )

    assert state.cdc_connector_names == [src, sink]
    assert get_status(sess.workflow, WorkflowStep.CDC) is StepStatus.IN_PROGRESS


def test_cdc_step_downgrade_does_not_clobber_a_recorded_failure() -> None:
    """A deliberate FAILED must survive a routine discovery pass.

    The downgrade exists to undo this function's OWN promotion, not to overwrite a
    terminal status some other path recorded -- otherwise a failed CDC start would be
    quietly relabelled "not started" on the next render.
    """
    from dsql_migrator.ui.workflow import WorkflowStep, get_status

    sess, _ = _discover_with_connectors([], cdc_status=StepStatus.FAILED)

    assert get_status(sess.workflow, WorkflowStep.CDC) is StepStatus.FAILED


def test_cdc_only_badge_follows_the_step_back_down_after_a_teardown() -> None:
    """End-to-end on the badge itself: CDC only must not read IN_PROGRESS post-Stop.

    The badge is what the user actually sees, so pin the pairing of label and value
    rather than only the underlying step.
    """
    from dsql_migrator.ui.data_migration._models import migration_status_badge

    label, status = migration_status_badge(
        MigrationType.CDC_ONLY,
        # A Full Load ran earlier in this session and is DONE; that must not leak in.
        full_load_status=StepStatus.DONE,
        cdc_status=StepStatus.NOT_STARTED,
        cdc_streaming=False,
    )

    assert label == "CDC"
    assert status is StepStatus.NOT_STARTED


def test_cdc_step_drops_back_on_a_freshly_built_controller_too() -> None:
    """The downgrade must also happen on the path that BUILDS the controller.

    A restored session has no controller yet, so its first render takes the
    build-then-list branch -- exactly when a stale persisted "CDC: IN_PROGRESS" is on
    screen. Covering only the pre-wired branch left that case broken.
    """
    from dsql_migrator.ui.data_migration import _cdc_status as _status
    from dsql_migrator.ui.workflow import WorkflowStep, get_status

    class _Ctl:
        def list_connectors(self):
            return []  # nothing of mine on AWS (post Stop/Delete)

    class _Target:
        region = "ap-northeast-2"

    state = DataMigrationState()
    state._cdc_discovery_monotonic = None
    sess = _WorkflowSess(StepStatus.IN_PROGRESS)
    sess.target_config = _Target()

    import dsql_migrator.core.msk_connect_controller as _mcc

    original = _mcc.build_msk_connect_controller
    _mcc.build_msk_connect_controller = lambda *_a, **_k: _Ctl()
    try:
        _status._ensure_cdc_controller(state, sess)
    finally:
        _mcc.build_msk_connect_controller = original

    assert state.cdc_controller is not None, "the controller must still be cached"
    assert get_status(sess.workflow, WorkflowStep.CDC) is StepStatus.NOT_STARTED


def _mixed_error_log(state, *, full_load: int = 3, cdc: int = 0):
    """Seed the session error log the way the workshop session did.

    Full Load quarantines carry ``chunk_id`` (the table name); CDC's ``surface_errors``
    never sets it. Returns the shared log key both sources use.
    """
    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration._cdc_status import cdc_error_log_key

    key = cdc_error_log_key(state)
    for i in range(full_load):
        state.error_log.record(
            key,
            DataErrorRecord(
                table="ecommerce.product_media",
                chunk_id="ecommerce.product_media",
                error_code="54000",
                message=f"quarantined row pk[id={i + 1}]: datatype limit greater than "
                "1048576 bytes not supported",
                occurred_at=datetime(2026, 8, 4, 10, 14, 3, tzinfo=timezone.utc),
            ),
        )
    for i in range(cdc):
        state.error_log.record(
            key,
            DataErrorRecord(
                table="ecommerce.orders",
                error_code="54000",
                message=f"dead-lettered record {i + 1}",
                occurred_at=datetime(2026, 8, 4, 21, 6, 0, tzinfo=timezone.utc),
            ),
        )
    return key


def test_full_load_quarantines_are_not_counted_as_dead_letter_records() -> None:
    """The workshop defect: Full Load rows showed up as "3 quarantined" in the DLQ.

    Both sources share ONE error-log key (it IS the Full Load job id whenever one ran),
    so the DLQ card counted batch-loader quarantines. Full Load has no DLQ, and a user
    who had just excluded the oversized column read the non-zero count as "the exclusion
    failed" -- when a zero CDC count is the proof that it worked.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_summary

    state = DataMigrationState()
    state.job_id = "job-fullload-1"  # a Full Load ran this session
    key = _mixed_error_log(state, full_load=3, cdc=0)

    # The raw log still holds all three (nothing is lost)...
    assert len(state.error_log.records(key)) == 3
    # ...but the DLQ view reports none of them.
    summary = cdc_dlq_summary(state, key)
    assert summary.total_errors == 0
    assert summary.errors_by_table == {}


def test_real_dead_letter_records_are_still_counted() -> None:
    # The control: filtering must not hide genuine CDC quarantines, which is the whole
    # purpose of the panel.
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_summary

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=2)

    summary = cdc_dlq_summary(state, key)
    assert summary.total_errors == 2
    assert summary.errors_by_table == {"ecommerce.orders": 2}


def test_dlq_record_list_shows_only_cdc_rows() -> None:
    """The rows beneath the count must agree with it.

    A filtered count over an unfiltered list would be worse than the original bug: the
    badge would say 0 while three rows sat underneath it.
    """
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_records

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=1)

    records = cdc_dlq_records(state, key)
    assert [r.table for r in records] == ["ecommerce.orders"]


def test_cdc_dlq_records_memoizes_on_append_only_count() -> None:
    """The CDC poll + re-render call this 4-5x per tick; the expensive copy+filter of
    the whole (uncapped) error log must run only when a NEW record arrived, not on
    every call with an unchanged append-only count."""
    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_records

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=2, cdc=2)

    calls = {"records": 0}
    real_records = state.error_log.records

    def _counting(job_id):
        calls["records"] += 1
        return real_records(job_id)

    state.error_log.records = _counting  # type: ignore[assignment]

    first = cdc_dlq_records(state, key)
    # Repeated calls with an unchanged count reuse the cached filtered view -- no
    # re-copy, no re-filter, same object handed back.
    for _ in range(4):
        assert cdc_dlq_records(state, key) is first
    assert calls["records"] == 1  # the expensive read ran exactly once

    # A new record changes the append-only count -> recompute exactly once more.
    state.error_log.record(
        key,
        DataErrorRecord(
            table="ecommerce.orders",
            message="x",
            occurred_at=datetime(2026, 8, 4, 10, 15, 0, tzinfo=timezone.utc),
        ),
    )
    updated = cdc_dlq_records(state, key)
    assert calls["records"] == 2
    assert len(updated) == len(first) + 1


def test_cdc_error_download_label_and_payload_exclude_full_load_rows() -> None:
    """"Download CDC error log (3 errors)" handed over three Full Load quarantines."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=1)

    ui = _RecordingUi()
    _cdc_ui._render_cdc_error_download(ui, state, key)

    joined = " ".join(ui.texts)
    assert "Download CDC error log (1 error)" in joined, (
        f"label must count CDC records only; got {joined!r}"
    )
    # And the bytes the button would produce carry no Full Load row.
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_records

    payload = state.error_log.render_records(cdc_dlq_records(state, key)).decode()
    assert "product_media" not in payload
    assert payload.count("\n") == 1


def test_download_is_not_offered_when_only_full_load_rows_exist() -> None:
    # Nothing was dead-lettered, so there is no CDC error log to download.
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=0)

    ui = _RecordingUi()
    _cdc_ui._render_cdc_error_download(ui, state, key)

    assert not any("Download CDC error log" in t for t in ui.texts)


def test_dlq_panel_points_at_the_full_load_for_its_own_quarantines() -> None:
    """Filtering must not make the Full Load's rows vanish from view.

    They are rows that never reached the target, so cut-over depends on knowing about
    them -- the panel cross-references them instead of silently dropping them.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=0)

    ui = _RecordingUi()
    _cdc_ui._render_full_load_quarantine_pointer(ui, state, key)

    joined = " ".join(ui.texts)
    assert "Full Load also set 3 rows aside" in joined
    assert "never entered the stream" in joined


def test_no_full_load_pointer_when_every_record_is_cdc() -> None:
    # Don't mention a Full Load that set nothing aside.
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    key = _mixed_error_log(state, full_load=0, cdc=2)

    ui = _RecordingUi()
    _cdc_ui._render_full_load_quarantine_pointer(ui, state, key)

    assert not any("Full Load also set" in t for t in ui.texts)


def test_dlq_record_table_rows_come_from_the_filtered_set() -> None:
    """Renders the real record table: the rows on screen must be CDC-only.

    A filtered count over an unfiltered list is worse than the original bug (badge says
    0, three rows sit underneath). ``cdc_dlq_records`` covers the helper; this covers
    what ``_render_cdc_dlq_records`` actually puts in the table.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=1)

    ui = _RecordingUi()
    _cdc_ui._render_cdc_dlq_records(ui, state, key)

    joined = " ".join(ui.texts)
    assert "product_media" not in joined, (
        "the Full Load's quarantined table must not appear in the DLQ record table"
    )
    assert "Quarantined records (1)" in joined, (
        f"the list heading must count the filtered rows; got {joined!r}"
    )


def test_dlq_record_table_is_empty_when_only_full_load_rows_exist() -> None:
    # Nothing was dead-lettered -> no record table at all (not a table of Full Load rows).
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=0)

    ui = _RecordingUi()
    _cdc_ui._render_cdc_dlq_records(ui, state, key)

    assert not any("Quarantined records" in t for t in ui.texts)
    assert not any("product_media" in t for t in ui.texts)


def test_cdc_download_payload_is_driven_by_the_buttons_own_handler() -> None:
    """Click the button and inspect the bytes it actually emits.

    Asserting a separately-recomputed payload let a "render_log(log_key)" regression
    survive: the label said 1 error while the file carried all four rows.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=1)

    downloaded: dict = {}

    class _Dl:
        @staticmethod
        def content(payload, filename, mime):
            downloaded["payload"] = payload
            downloaded["filename"] = filename

    ui = _RecordingUi()
    ui.download = _Dl()
    _cdc_ui._render_cdc_error_download(ui, state, key)

    handlers = [b.on_click for b in ui.buttons if b.on_click is not None]
    assert handlers, "the download button must be wired"
    handlers[0]()

    text = downloaded["payload"].decode()
    assert "product_media" not in text, "the CDC log must not contain Full Load rows"
    assert text.count("\n") == 1, f"expected exactly one CDC record; got {text!r}"


def test_full_load_pointer_is_rendered_by_the_dlq_panel() -> None:
    """Wiring: the panel itself must emit the cross-reference.

    The helper can be correct while nothing calls it -- which would silently drop the
    Full Load's quarantines from the screen entirely.
    """
    from dsql_migrator.core.cdc import (
        ConnectorState,
        ConnectorStatus,
        build_cdc_status_view,
    )
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_status import cdc_dlq_summary

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=0)

    view = build_cdc_status_view(
        [ConnectorStatus(name="src", state=ConnectorState.RUNNING)],
        cdc_dlq_summary(state, key),
        dlq_depth=0,
    )
    ui = _RecordingUi()
    _cdc_ui._render_cdc_dlq_panel(ui, state, _StubJobManager({}), view)

    joined = " ".join(ui.texts)
    # The DLQ itself is clean...
    assert "0 quarantined" in joined
    assert "No records quarantined" in joined
    # ...and the Full Load's rows are still accounted for.
    assert "Full Load also set 3 rows aside" in joined


def test_full_load_error_log_excludes_dead_lettered_cdc_rows() -> None:
    """The mirror defect: CDC rows counted as Full Load failures.

    CDC records under the Full Load's job_id whenever one ran, so an unfiltered read
    made "Download Full Load error log (5 errors)" out of 3 Full Load quarantines and 2
    dead-lettered rows -- reading at cut-over as "the Full Load lost 5 rows".
    """
    from dsql_migrator.ui.data_migration._cdc_status import full_load_error_summary

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=2)

    summary = full_load_error_summary(state.error_log, key)
    assert summary.total_errors == 3
    assert summary.errors_by_table == {"ecommerce.product_media": 3}


def test_the_two_screens_partition_the_error_log_exactly() -> None:
    """Full Load + CDC must add up to the whole log -- nothing lost, nothing double-counted.

    This is the property that makes filtering both directions correct rather than just
    moving the miscount around.
    """
    from dsql_migrator.ui.data_migration._cdc_status import (
        cdc_dlq_summary,
        full_load_error_summary,
    )

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=2)

    whole = state.error_log.summary(key).total_errors
    full_load = full_load_error_summary(state.error_log, key).total_errors
    cdc = cdc_dlq_summary(state, key).total_errors

    assert (full_load, cdc) == (3, 2)
    assert full_load + cdc == whole == 5


def test_full_load_download_label_and_payload_exclude_cdc_rows() -> None:
    """Click the real button: label counts, and bytes contain, Full Load rows only."""
    from dsql_migrator.core.models import MigrationJob
    from dsql_migrator.ui.data_migration import _render_error_log

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=2)

    downloaded: dict = {}

    class _Dl:
        @staticmethod
        def content(payload, filename, mime):
            downloaded["payload"] = payload

    ui = _RecordingUi()
    ui.download = _Dl()
    _render_error_log(ui, state, MigrationJob(job_id=key))

    joined = " ".join(ui.texts)
    assert "Download Full Load error log (3 errors)" in joined, (
        f"label must count Full Load rows only; got {joined!r}"
    )

    handlers = [b.on_click for b in ui.buttons if b.on_click is not None]
    assert handlers, "the download button must be wired"
    handlers[0]()
    text = downloaded["payload"].decode()
    assert "ecommerce.orders" not in text, "a Full Load log must not carry CDC rows"
    assert text.count("\n") == 3


def test_full_load_latest_messages_ignores_cdc_records() -> None:
    """A dead-lettered row must not supply the "why" for a table the Full Load loaded.

    ``latest_messages`` keeps the last message per table, so an unfiltered read let a
    CDC record become a Full Load table's displayed failure reason.
    """
    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration._cdc_status import full_load_latest_messages

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=1, cdc=0)
    # A CDC record for the SAME table, recorded later than the Full Load's.
    state.error_log.record(
        key,
        DataErrorRecord(
            table="ecommerce.product_media",
            error_code="54000",
            message="dead-lettered by the sink",
            occurred_at=datetime(2026, 8, 4, 21, 6, 0, tzinfo=timezone.utc),
        ),
    )

    messages = full_load_latest_messages(state.error_log, key)
    assert "dead-lettered" not in messages.get("ecommerce.product_media", "")
    assert "quarantined row pk[" in messages["ecommerce.product_media"]


def _render_cdc_per_table(state, *, job_id="job-fullload-1"):
    """Render the CDC step's per-table status table and return the UI double."""
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _RecordingUi()

    class _Sess:
        target_config = None
        aws_profile = None

    # The table only renders once CDC has STARTED (it is a CDC-status view), so put
    # the session in that state -- detected connectors are the narrow signal.
    state.set_cdc_stack_phase("running")
    # The table set comes from the Full Load job's chunk ids
    # (_migration_status_tables), so the job needs a chunk per listed table.
    job = MigrationJob(
        job_id=job_id,
        status="DONE",
        chunks=[ChunkState(chunk_id="ecommerce.product_media", status="DONE")],
    )
    _cdc_ui._render_migration_table_status(
        ui,
        state,
        _StubJobManager({job_id: job}),
        _Sess(),
        inventory=SourceInventory(
            tables=[
                TableDef(
                    name="ecommerce.product_media",
                    columns=[ColumnDef(name="id", mysql_type="int", nullable=False)],
                    primary_key=["id"],
                ),
            ]
        ),
    )
    return ui


def _quarantined_cells(ui) -> list:
    """The values rendered in the per-table table's "Quarantined" column."""
    return [
        row.get("dlq")
        for payload in ui.tables
        for row in payload["rows"]
        if "dlq" in row
    ]


def test_per_table_quarantined_column_excludes_full_load_rows() -> None:
    """The CDC table's "Quarantined" column must not count Full Load quarantines.

    Worse than the original defect: the DLQ card below it already reported "0
    quarantined" after v0.1.241, so the same screen showed two contradictory numbers
    for one session (card 0, column 3).
    """
    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    _mixed_error_log(state, full_load=3, cdc=0)

    cells = _quarantined_cells(_render_cdc_per_table(state))
    assert cells, "the per-table table must render a Quarantined cell"
    assert all(str(c) == "0" for c in cells), (
        f"Full Load quarantines must not appear in the CDC Quarantined column; got {cells}"
    )


def test_per_table_quarantined_column_still_counts_real_dlq_rows() -> None:
    # The control: genuine dead-lettered rows must still be reported per table, or the
    # column would be useless.
    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    _mixed_error_log(state, full_load=3, cdc=0)
    # Two dead-lettered rows for the table the table lists (no chunk_id -> CDC).
    from dsql_migrator.core.models import DataErrorRecord
    from dsql_migrator.ui.data_migration._cdc_status import cdc_error_log_key

    for i in range(2):
        state.error_log.record(
            cdc_error_log_key(state),
            DataErrorRecord(
                table="ecommerce.product_media",
                error_code="54000",
                message=f"dead-lettered {i + 1}",
                occurred_at=datetime(2026, 8, 4, 21, 6, 0, tzinfo=timezone.utc),
            ),
        )

    cells = _quarantined_cells(_render_cdc_per_table(state))
    assert any(str(c) == "2" for c in cells), (
        f"real DLQ rows must still be counted per table; got {cells}"
    )


def test_per_table_column_and_dlq_card_agree() -> None:
    """One screen, one number: the column and the card must never disagree.

    They read the same key, so the invariant is that both go through the same filter.
    """
    from dsql_migrator.ui.data_migration._cdc_status import (
        cdc_dlq_summary,
        cdc_error_log_key,
    )

    from dsql_migrator.core.models import DataErrorRecord

    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    key = _mixed_error_log(state, full_load=3, cdc=0)
    # A dead-lettered row for the table the per-table view actually lists, so the
    # column and the card are summing over the same table set.
    state.error_log.record(
        key,
        DataErrorRecord(
            table="ecommerce.product_media",
            error_code="54000",
            message="dead-lettered by the sink",
            occurred_at=datetime(2026, 8, 4, 21, 6, 0, tzinfo=timezone.utc),
        ),
    )

    card_total = cdc_dlq_summary(state, cdc_error_log_key(state)).total_errors
    column_total = sum(
        int(c)
        for c in _quarantined_cells(_render_cdc_per_table(state))
        if str(c).isdigit()
    )
    assert column_total == card_total == 1


def test_scroll_to_migration_type_button_targets_the_selector_anchor() -> None:
    """The jump link must scroll to the migration-type heading and mark it.

    Without the highlight the user lands at the top of a long page and still has to work
    out WHICH control the notice meant.
    """
    from dsql_migrator.ui.data_migration import (
        MIGRATION_TYPE_ANCHOR,
        _scroll_to_migration_type_button,
    )

    ui = _RecordingUi()
    scripts: list[str] = []
    ui.run_javascript = lambda code: scripts.append(code)

    _scroll_to_migration_type_button(ui)

    assert any("Change migration type" in t for t in ui.texts), (
        "the action must be labelled, not a bare icon"
    )
    handlers = [b.on_click for b in ui.buttons if b.on_click is not None]
    assert handlers, "the jump link must be wired"
    handlers[0]()

    assert scripts, "clicking must run the scroll script"
    code = scripts[0]
    assert f".{MIGRATION_TYPE_ANCHOR}" in code, (
        "the script must target the same anchor class the selector renders"
    )
    assert "scrollIntoView" in code
    # The highlight must be ADDED and then cleaned up -- a permanent ring would leave
    # the heading looking selected for the rest of the session.
    assert "classList.add('ring-2'" in code, (
        "the landed-on control must be highlighted, or the user still has to work out "
        "which control the notice meant"
    )
    assert "classList.remove(" in code and "setTimeout" in code, (
        "the highlight must be temporary"
    )


def test_migration_type_selector_renders_the_scroll_anchor() -> None:
    """The other half of the pair: the anchor must actually exist in the DOM.

    A correct script plus a missing anchor is a link that silently does nothing, so pin
    that the selector emits the class the script queries.
    """
    from dsql_migrator.ui.data_migration import (
        MIGRATION_TYPE_ANCHOR,
        _render_migration_type_selector,
    )

    classes_seen: list[str] = []

    class _ClassCapturingUi(_RecordingUi):
        class _El(_RecordingUi._El):
            def classes(self, value="", *_a, **_k):
                classes_seen.append(str(value))
                return self

    ui = _ClassCapturingUi()
    _render_migration_type_selector(
        ui,
        DataMigrationState(),
        status=StepStatus.DONE,
        refresh=lambda: None,
    )

    assert any(MIGRATION_TYPE_ANCHOR in c for c in classes_seen), (
        f"the selector must carry the {MIGRATION_TYPE_ANCHOR} scroll anchor"
    )


def test_full_load_only_cdc_notice_offers_the_jump_link() -> None:
    """Wiring: the "want CDC next?" notice must render the jump link with it.

    The anchor and the link can both be correct while nothing puts the link on screen --
    which leaves the notice telling the user to change a setting it never helps them
    reach. Pinned on source order (the notice is rendered deep inside the sub-step
    closure, so it cannot be called directly) by asserting the call follows the notice
    that motivates it.
    """
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    tree = ast.parse(src.strip())
    notice_line = None
    link_line = None
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        rendered = ast.unparse(node)
        if "want continuous" in rendered and "render_notice" in rendered:
            notice_line = node.lineno
        if "_scroll_to_migration_type_button" in rendered:
            link_line = node.lineno
    assert notice_line is not None, "the post-Full-Load CDC notice must exist"
    assert link_line is not None, (
        "the notice must be accompanied by the migration-type jump link"
    )
    assert link_line > notice_line, (
        "the jump link belongs directly after the notice that references the setting"
    )


def test_cdc_notice_body_points_at_the_link_not_a_direction() -> None:
    """"change the migration type above" made the user hunt for an off-screen control.

    With a jump link present the copy should send them to the link instead of naming a
    direction that may be several screens away.
    """
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    assert "Use the link below to jump to that " in src
    assert "change the migration type above to " not in src, (
        "the copy should not tell the user to scroll up and find it themselves"
    )


_QUARANTINE_ERROR = (
    "FullLoadIncompleteError: Full Load incomplete: 1 of 7 table(s) did not fully "
    "load. The target holds partial data -- 3 row(s) were QUARANTINED ..."
)


def test_accepting_the_quarantine_clears_the_migration_failed_banner() -> None:
    """Accepting the gap RESOLVES this error, so the red banner must go.

    Reported: after "Accept quarantined rows & continue" the step said "Full Load
    complete — with an accepted gap" and the status said DONE, while the banner above
    still said "Migration failed" — three verdicts on one screen, and the button's own
    decision contradicted.
    """
    from dsql_migrator.ui.data_migration._models import stale_error_notice

    assert (
        stale_error_notice(
            _QUARANTINE_ERROR,
            migration_type=MigrationType.FULL_LOAD_AND_CDC,
            error_migration_type=MigrationType.FULL_LOAD_AND_CDC,
            quarantine_accepted=True,
        )
        is None
    )


def test_the_banner_still_shows_before_the_gap_is_accepted() -> None:
    # The control: until the operator accepts, this IS a live failure and must be loud.
    from dsql_migrator.ui.data_migration._models import stale_error_notice

    notice = stale_error_notice(
        _QUARANTINE_ERROR,
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
        error_migration_type=MigrationType.FULL_LOAD_AND_CDC,
        quarantine_accepted=False,
    )
    assert notice is not None
    assert notice[0] == "error" and notice[1] == "Migration failed"


def test_acceptance_hides_the_banner_even_across_a_type_switch() -> None:
    """Accepted takes precedence over the carried-over demotion.

    A resolved error is not "context from another type" either -- it is done. Without
    this the user would switch to CDC only and meet the amber "Carried over from the
    previous Full Load" for a gap they had already acknowledged.
    """
    from dsql_migrator.ui.data_migration._models import stale_error_notice

    assert (
        stale_error_notice(
            _QUARANTINE_ERROR,
            migration_type=MigrationType.CDC_ONLY,
            error_migration_type=MigrationType.FULL_LOAD_AND_CDC,
            quarantine_accepted=True,
        )
        is None
    )


def test_acceptance_defaults_to_false_so_the_banner_is_never_hidden_by_accident() -> None:
    # Callers that predate the flag must keep the loud banner.
    from dsql_migrator.ui.data_migration._models import stale_error_notice

    notice = stale_error_notice(
        _QUARANTINE_ERROR,
        migration_type=MigrationType.FULL_LOAD_AND_CDC,
        error_migration_type=MigrationType.FULL_LOAD_AND_CDC,
    )
    assert notice is not None and notice[0] == "error"


def test_screen_passes_the_acceptance_flag_to_the_banner() -> None:
    """Wiring: the predicate is useless if the screen never tells it about acceptance."""
    import ast
    import inspect

    from dsql_migrator.ui import data_migration as dm

    src = inspect.getsource(dm.build_data_migration_screen)
    tree = ast.parse(src.strip())
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and "stale_error_notice" in ast.unparse(node.func)
        ):
            kwargs = {kw.arg: ast.unparse(kw.value) for kw in node.keywords}
            assert "quarantine_accepted" in kwargs, (
                "the screen must pass the acceptance state to the banner"
            )
            assert "accept_quarantined_rows" in kwargs["quarantine_accepted"]
            return
    raise AssertionError("stale_error_notice call not found in the screen")


def _render_per_table_with(state, *, job_id="job-fullload-1", cdc_jobs=None):
    """Render the per-table status view against ``state`` and return the UI double.

    ``cdc_jobs`` registers extra jobs (e.g. an in-flight connector start) so the
    started-CDC predicate can see them through the job manager.
    """
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import _cdc_ui

    class _Sess:
        target_config = None
        aws_profile = None

    ui = _RecordingUi()
    jobs = {
        job_id: MigrationJob(
            job_id=job_id,
            status="DONE",
            chunks=[ChunkState(chunk_id="ecommerce.product_media", status="DONE")],
        )
    }
    jobs.update(cdc_jobs or {})
    _cdc_ui._render_migration_table_status(
        ui, state, _StubJobManager(jobs), _Sess(), inventory=None
    )
    return ui


def test_per_table_status_is_hidden_while_cdc_infra_is_still_deploying() -> None:
    """The table must not appear during the ~15-20 min infrastructure create.

    Every CDC column (Stream lag, Quarantined, I/U/D, Consistency) is necessarily empty
    then -- no connector exists yet -- so it read as "CDC is running and replicating
    nothing", the opposite of the truth.
    """
    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    state.set_cdc_deploy_job_id("infra-1", kind="infra")
    state.set_cdc_stack_phase("provisioning")

    ui = _render_per_table_with(state)
    assert not any("Per-table migration status" in t for t in ui.texts)


def test_per_table_status_is_hidden_before_cdc_starts_even_with_infra_ready() -> None:
    # Deployed-but-not-started is the other pre-CDC state: the stack exists, no
    # connectors do, so the CDC columns are still empty.
    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    state.set_cdc_stack_phase("infra")

    ui = _render_per_table_with(state)
    assert not any("Per-table migration status" in t for t in ui.texts)


def test_per_table_status_appears_once_cdc_starts() -> None:
    # The control: it must show for a live pipeline, which is its whole purpose.
    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    state.set_cdc_stack_phase("running")

    ui = _render_per_table_with(state)
    assert any("Per-table migration status" in t for t in ui.texts)


def test_per_table_status_appears_during_the_connector_ramp() -> None:
    """Visible from the moment Start CDC is pressed, not only once RUNNING.

    Connectors take ~10-20 min to come up and the operator wants the per-table view
    during that ramp -- hence cdc_streaming_started, not cdc_pipeline_live.
    """
    state = DataMigrationState()
    state.job_id = "job-fullload-1"
    state.set_cdc_deploy_job_id("start-1", kind="start")  # start job in flight

    ui = _render_per_table_with(state, cdc_jobs={"start-1": _StubJob("RUNNING")})
    assert any("Per-table migration status" in t for t in ui.texts)


def _render_live_monitoring(state, *, cdc_jobs=None):
    """Render the CDC live-status section and return the UI double."""
    from dsql_migrator.ui.data_migration import _cdc_ui

    ui = _RecordingUi()
    _cdc_ui._render_cdc_live_monitoring(ui, state, _StubJobManager(cdc_jobs or {}))
    return ui


def test_live_status_is_hidden_before_cdc_starts() -> None:
    """The whole "Live status" section must be gone until CDC has started.

    Before that there is no pipeline to report on, and the borderless "Live status"
    header sat above an empty chart and a grey placeholder -- dead space on the deploy
    screen (and inconsistent with every other bordered section).
    """
    state = DataMigrationState()
    # Infra deployed but CDC not started: connectors do not exist yet.
    state.set_cdc_stack_phase("infra")

    ui = _render_live_monitoring(state)
    assert not any("Live status" in t for t in ui.texts), (
        "the Live status section must not render before CDC starts"
    )


def test_live_status_appears_once_cdc_starts() -> None:
    # The control: a live pipeline must show the section, which is its purpose.
    state = DataMigrationState()
    state.set_cdc_stack_phase("running")

    ui = _render_live_monitoring(state)
    assert any("Live status" in t for t in ui.texts)


def test_live_status_appears_during_the_connector_ramp() -> None:
    """From the moment Start CDC is pressed, not only once connectors are RUNNING.

    Connectors take ~10-20 min to come up and the operator wants to watch them -- hence
    cdc_streaming_started, not cdc_pipeline_live.
    """
    state = DataMigrationState()
    state.set_cdc_deploy_job_id("start-1", kind="start")

    ui = _render_live_monitoring(state, cdc_jobs={"start-1": _StubJob("RUNNING")})
    assert any("Live status" in t for t in ui.texts)


def test_ramp_placeholder_is_a_bordered_notice_not_a_bare_label() -> None:
    """During the ramp (started, no connectors yet) the waiting message must be boxed.

    Once the section is shown its placeholder is the only remaining pre-connector state,
    and a loose grey line there was the original styling complaint. It is a normal
    waiting state, so info (not warning).
    """
    state = DataMigrationState()
    state.set_cdc_deploy_job_id("start-1", kind="start")

    classes_seen: list = []

    class _ClassUi(_RecordingUi):
        class _El(_RecordingUi._El):
            def classes(self, value="", *_a, **_k):
                classes_seen.append(str(value))
                return self

        def row(self, *_a, **_k):
            return self._El(self)

    ui = _ClassUi()
    from dsql_migrator.ui.data_migration import _cdc_ui

    _cdc_ui._render_cdc_live_monitoring(
        ui, state, _StubJobManager({"start-1": _StubJob("RUNNING")})
    )
    joined = " ".join(ui.texts)
    assert "appear here once" in joined, "the waiting message must still be shown"
    # The waiting message must be inside a BORDERED notice, not a loose label. render_notice
    # puts the border on a row; assert a bordered/rounded container was emitted.
    assert any("rounded-md border" in c for c in classes_seen), (
        "the placeholder must be wrapped in a bordered notice, not a bare label"
    )
    assert "No live pipeline yet" in joined


def test_counts_notice_prompts_the_refresh_before_the_first_read() -> None:
    """Before counts are read, the notice must link Consistency to the Refresh button.

    Every row's Consistency shows "refresh to check" until the counts are fetched, and
    that only means something if the user connects it to the button that fills it.
    """
    from dsql_migrator.ui.data_migration._models import per_table_counts_notice_body

    body = per_table_counts_notice_body(counts_fetched=False)
    assert "Consistency" in body
    assert "Refresh source/target counts" in body
    assert "Validation (Step 4)" in body, "the exact check must still be named"


def test_counts_notice_drops_the_prompt_once_counts_are_read() -> None:
    # After a refresh the prompt is noise; state the estimate caveat instead.
    from dsql_migrator.ui.data_migration._models import per_table_counts_notice_body

    body = per_table_counts_notice_body(counts_fetched=True)
    assert "refresh to check" not in body.lower()
    assert "Press" not in body and "press" not in body
    assert "estimate" in body
    assert "Validation (Step 4)" in body


def test_per_table_notice_reflects_whether_counts_were_fetched() -> None:
    """Wiring: the screen must key the notice body on row_counts_fetched_at.

    The helper is useless if the screen always passes the same value, so pin that the
    call reads the fetched-at state.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui._render_migration_table_status)
    tree = ast.parse(src.strip())
    found = False
    arg_expr = None
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "per_table_counts_notice_body"
        ):
            kwargs = {kw.arg: ast.unparse(kw.value) for kw in node.keywords}
            assert "counts_fetched" in kwargs
            arg_expr = kwargs["counts_fetched"]
            found = True
    assert found, "the notice body must be built from per_table_counts_notice_body"
    # The arg may be an intermediate local; resolve it to its assignment and require
    # THAT expression to read the fetched-at state, so a hard-coded True/False (or a
    # local bound to a literal) cannot pass just because the caption elsewhere also
    # reads row_counts_fetched_at.
    if "row_counts_fetched_at" not in arg_expr:
        assign = None
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign) and any(
                isinstance(tgt, ast.Name) and tgt.id == arg_expr
                for tgt in node.targets
            ):
                assign = ast.unparse(node.value)
        assert assign is not None, (
            f"counts_fetched arg {arg_expr!r} must be a local assigned in the function"
        )
        assert "row_counts_fetched_at" in assign, (
            "counts_fetched must be computed from row_counts_fetched_at, not a literal; "
            f"got {arg_expr} = {assign}"
        )


def _render_deploy_stages_for(kind, *, running_stage="stack_delete"):
    """Render the CDC stage-progress card for ``kind`` mid-run and return the double."""
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import _cdc_ui
    from dsql_migrator.ui.data_migration._cdc_status import _CDC_STAGE_LABELS

    stage_ids = list(_CDC_STAGE_LABELS[kind].keys())
    chunks = []
    for sid in stage_ids:
        status = (
            "IN_PROGRESS" if sid == running_stage
            else "DONE" if stage_ids.index(sid) < stage_ids.index(running_stage)
            else "PENDING"
        )
        chunks.append(ChunkState(chunk_id=sid, status=status))
    job = MigrationJob(job_id="j", status="RUNNING", chunks=chunks)
    ui = _RecordingUi()
    _cdc_ui._render_deploy_stages(ui, job, kind=kind)
    return ui


def test_delete_progress_shows_an_upper_bound_not_a_countdown() -> None:
    """CDC delete waits on unpredictable ENI reclamation, so a precise ETA overshoots.

    Reported: "est. ~5 min remaining" while it actually took far longer. Show an honest
    upper bound instead of a countdown that reads as a stuck UI.
    """
    ui = _render_deploy_stages_for("delete")
    joined = " ".join(ui.texts)
    assert "up to ~20 min" in joined, "delete must show an upper-bound wait"
    assert "remaining" not in joined, (
        "delete must not show a precise 'est. N remaining' countdown"
    )


def test_delete_stages_show_no_per_stage_eta_hint() -> None:
    # The dominant stage (stack_delete, a 5-min estimate) is the unpredictable one; a
    # "~5 min" hint on it is exactly the misleading number. Run with an EARLIER stage
    # in progress so stack_delete is a PENDING stage whose ETA hint would otherwise
    # render, and assert it does not.
    ui = _render_deploy_stages_for("delete", running_stage="submit_delete")
    joined = " ".join(ui.texts)
    assert "~5 min" not in joined, "the pending stack_delete stage must show no ETA"
    assert "/ ~" not in joined, "the running delete stage must not append an ETA"


def test_delete_running_stage_shows_no_eta_suffix() -> None:
    # Even the IN-PROGRESS delete stage (with a started_at, so the elapsed path runs)
    # must not append "/ ~N min": that suffix is the misleading estimate.
    from datetime import datetime, timezone

    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import _cdc_ui

    job = MigrationJob(
        job_id="j",
        status="RUNNING",
        chunks=[
            ChunkState(chunk_id="discover_stack", status="DONE"),
            ChunkState(chunk_id="submit_delete", status="DONE"),
            ChunkState(
                chunk_id="stack_delete",
                status="IN_PROGRESS",
                started_at=datetime(2026, 8, 5, 0, 0, 0, tzinfo=timezone.utc),
            ),
            ChunkState(chunk_id="cleanup_secret", status="PENDING"),
            ChunkState(chunk_id="deleted", status="PENDING"),
        ],
    )
    ui = _RecordingUi()
    _cdc_ui._render_deploy_stages(ui, job, kind="delete")
    joined = " ".join(ui.texts)
    assert "elapsed" in joined, "the running stage must still show live elapsed time"
    assert "/ ~" not in joined, "but not an ETA suffix on a delete"


def test_start_progress_still_shows_the_estimated_remaining() -> None:
    """The control: non-delete operations keep the summed ETA countdown.

    Connector creation has a stable ~10-20 min estimate worth showing, so the change
    must be scoped to delete only.
    """
    ui = _render_deploy_stages_for("start", running_stage="stack_connectors")
    joined = " ".join(ui.texts)
    assert "remaining" in joined and "est." in joined
    assert "up to ~20 min" not in joined


class _TeardownJobManager:
    """Job manager returning one canned status, or raising for an unknown id."""

    def __init__(self, status=None):
        self._status = status

    def get_status(self, job_id):
        from dsql_migrator.core.job_manager import JobNotFoundError

        if self._status is None:
            raise JobNotFoundError(job_id)

        class _J:
            pass

        j = _J()
        j.status = self._status
        return j


def test_delete_in_flight_replaces_the_streaming_badge() -> None:
    """The reported defect: "Delete CDC infrastructure" left a green "Streaming".

    CloudFormation does not remove the connectors instantly, so discovery keeps
    reporting both as RUNNING and the card phase stays "running" -- while the card body
    already says "Deleting infrastructure". Two contradictory verdicts, and the
    reassuring one was wrong.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_teardown_badge

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-del", kind="delete")

    assert cdc_teardown_badge(state, _TeardownJobManager("RUNNING")) == (
        "Deleting…",
        "primary",
    )


def test_stop_in_flight_is_named_distinctly_from_delete() -> None:
    # Stop CDC (connectors only, MSK kept) and Delete (everything) leave very
    # different systems behind, so the badge must not conflate them.
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_teardown_badge

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-stop", kind="stop")

    assert cdc_teardown_badge(state, _TeardownJobManager("RUNNING")) == (
        "Stopping…",
        "primary",
    )


def test_teardown_badge_clears_once_the_job_finishes() -> None:
    """Scoped to the RUN. Once the job ends, the live phase must speak again.

    Otherwise the card would be stuck on "Deleting…" forever after a teardown, instead
    of falling through to "Not deployed".
    """
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_teardown_badge

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-del", kind="delete")

    assert cdc_teardown_badge(state, _TeardownJobManager("DONE")) is None


def test_non_teardown_jobs_do_not_claim_the_badge() -> None:
    # Start / infra have their own badges ("Provisioning…", "Working…"); a teardown
    # check that swallowed them would be a regression in the other direction.
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_teardown_badge

    for kind in ("start", "infra"):
        state = DataMigrationState()
        state.set_cdc_deploy_job_id("job-1", kind=kind)
        assert cdc_teardown_badge(state, _TeardownJobManager("RUNNING")) is None, (
            f"{kind} must not be reported as a teardown"
        )


def test_teardown_badge_is_checked_before_the_running_phase() -> None:
    """Wiring: the ORDER is the bug. The teardown must outrank phase == "running".

    The helper can be correct while the badge chain still tests "running" first --
    which is exactly the state the card was in.
    """
    import ast
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    src = inspect.getsource(_cdc_ui._render_cdc_start_action)
    tree = ast.parse(src.strip())
    # Locate the badge tuple assignment and read its conditional chain in order.
    chain = None
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Tuple)
            and any(
                isinstance(e, ast.Name) and e.id == "badge_text" for e in t.elts
            )
            for t in node.targets
        ):
            chain = ast.unparse(node.value)
    assert chain is not None, "the badge chain assignment must exist"
    teardown_at = chain.find("_teardown")
    streaming_at = chain.find("'Streaming'")
    assert teardown_at != -1, "the badge chain must consult the teardown state"
    assert streaming_at != -1, "the Streaming badge must still exist"
    assert teardown_at < streaming_at, (
        "the teardown check must come BEFORE phase == 'running', or a live delete "
        "keeps showing a green Streaming badge"
    )


def _live_cdc_state(*, kind=None):
    """State for a streaming pipeline, optionally with a lifecycle job in flight."""
    from dsql_migrator.core.cdc import cdc_expected_connector_names

    state = DataMigrationState()
    src, sink = cdc_expected_connector_names(state.cdc_stack_name)
    state.set_cdc_connector_names([src, sink])
    state.set_cdc_controller(object())
    if kind is not None:
        state.set_cdc_deploy_job_id("job-1", kind=kind)
    return state


def test_monitoring_hidden_while_the_infrastructure_is_being_deleted() -> None:
    """Reported: Live status / per-table / DLQ stayed up during a delete.

    A teardown does not remove the connectors instantly, so discovery keeps reporting
    them and cdc_streaming_started stays true for the whole ~20 min delete -- leaving a
    live stream-lag chart and per-table replication figures on screen for a pipeline
    being dismantled.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_monitoring_visible

    assert (
        cdc_monitoring_visible(
            _live_cdc_state(kind="delete"), _TeardownJobManager("RUNNING")
        )
        is False
    )


def test_monitoring_hidden_while_stopping_cdc() -> None:
    # Stop CDC removes the connectors too, so the live views are equally moot.
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_monitoring_visible

    assert (
        cdc_monitoring_visible(
            _live_cdc_state(kind="stop"), _TeardownJobManager("RUNNING")
        )
        is False
    )


def test_monitoring_visible_for_a_streaming_pipeline() -> None:
    # The control: with no teardown in flight the views must show, which is their point.
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_monitoring_visible

    assert cdc_monitoring_visible(_live_cdc_state(), _TeardownJobManager()) is True


def test_monitoring_still_visible_during_the_connector_ramp() -> None:
    """A Start CDC in flight must NOT hide the views -- that is when they matter most.

    Scoping the hide to teardowns only is what keeps the ramp observable.
    """
    from dsql_migrator.ui.data_migration._cdc_ui import cdc_monitoring_visible

    state = DataMigrationState()
    state.set_cdc_deploy_job_id("job-start", kind="start")

    assert cdc_monitoring_visible(state, _TeardownJobManager("RUNNING")) is True


def test_live_status_section_hidden_during_a_delete_including_its_dlq_panel() -> None:
    """Renders the real section: header, chart and the nested DLQ panel all go.

    The DLQ panel lives INSIDE the Live status section, so it must vanish with its
    parent rather than needing its own gate.
    """
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _live_cdc_state(kind="delete")
    ui = _RecordingUi()
    _cdc_ui._render_cdc_live_monitoring(ui, state, _TeardownJobManager("RUNNING"))

    joined = " ".join(ui.texts)
    assert "Live status" not in joined
    assert "Stream lag" not in joined
    assert "Dead-letter queue" not in joined, (
        "the DLQ panel must disappear with the Live status section it sits inside"
    )


def test_per_table_status_hidden_during_a_delete() -> None:
    from dsql_migrator.core.models import ChunkState, MigrationJob
    from dsql_migrator.ui.data_migration import _cdc_ui

    state = _live_cdc_state()
    state.job_id = "fullload-1"
    # Distinct ids: the Full Load job supplies the table set, while the CDC lifecycle
    # job is the in-flight teardown.
    state.set_cdc_deploy_job_id("cdc-del", kind="delete")

    class _Sess:
        target_config = None
        aws_profile = None

    class _JM:
        def get_status(self, job_id):
            from dsql_migrator.core.job_manager import JobNotFoundError

            if job_id == "fullload-1":
                return MigrationJob(
                    job_id="fullload-1",
                    status="DONE",
                    chunks=[ChunkState(chunk_id="ecommerce.orders", status="DONE")],
                )
            if job_id == "cdc-del":
                class _J:
                    status = "RUNNING"

                return _J()
            raise JobNotFoundError(job_id)

    ui = _RecordingUi()
    _cdc_ui._render_migration_table_status(
        ui, state, _JM(), _Sess(), inventory=None
    )
    assert not any("Per-table migration status" in t for t in ui.texts)


def test_both_monitoring_views_share_one_visibility_gate() -> None:
    """Wiring: a divergent gate would leave the screen self-contradictory.

    Pins that neither call site reverted to the bare cdc_streaming_started check.
    """
    import inspect

    from dsql_migrator.ui.data_migration import _cdc_ui

    for fn in (
        _cdc_ui._render_cdc_live_monitoring,
        _cdc_ui._render_migration_table_status,
    ):
        src = inspect.getsource(fn)
        assert "if not cdc_monitoring_visible(" in src, (
            f"{fn.__name__} must gate on the shared cdc_monitoring_visible predicate"
        )
        assert "if not cdc_streaming_started(" not in src, (
            f"{fn.__name__} must not bypass the shared gate with the raw predicate"
        )


# ---------------------------------------------------------------------------
# Container memory-pressure logging (OOM diagnostics; the user's feedback)
# ---------------------------------------------------------------------------


class _MemHandle:
    """JobHandle double whose update() exposes a live job with IN_PROGRESS chunks."""

    def __init__(self, in_progress: list[str]) -> None:
        from dsql_migrator.core.models import ChunkState, MigrationJob

        self.job_id = "mem-job"
        self._job = MigrationJob(
            job_id="mem-job",
            chunks=[ChunkState(chunk_id=n, status="IN_PROGRESS") for n in in_progress],
        )

    def update(self, fn):
        return fn(self._job)


def test_memory_pressure_logger_noop_off_fargate(monkeypatch, caplog) -> None:
    # No cgroup memory file (macOS/dev) -> disabled, samples nothing, never logs.
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    monkeypatch.setattr(_engine, "_read_cgroup_memory", lambda: None)
    logger = _engine._MemoryPressureLogger(_MemHandle([]))
    assert logger._enabled is False
    with caplog.at_level("INFO", logger="dsql_migrator.ui.data_migration._full_load_engine"):
        logger.sample()
        logger.sample()
    assert not [r for r in caplog.records if "memory" in r.getMessage().lower()]


def test_memory_pressure_logger_high_water_info_and_80pct_warning(monkeypatch, caplog) -> None:
    # A climbing usage logs a new high-water at INFO, then a single WARNING once it
    # crosses 80% of the limit, tagging the currently-loading table.
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    limit = 1024 * 1024 * 1024  # 1 GiB task limit
    readings = iter([
        (270 * 1024 * 1024, limit),   # consumed by __init__'s enable probe
        (270 * 1024 * 1024, limit),   # 270 MiB — first high-water (INFO)
        (270 * 1024 * 1024, limit),   # same — no new high-water, below 80%
        (900 * 1024 * 1024, limit),   # 900 MiB ≈ 88% — new high-water INFO + WARNING
    ])
    monkeypatch.setattr(_engine, "_read_cgroup_memory", lambda: next(readings))
    # Capture the DURABLE activity-log event (surfaces in the UI timeline / download).
    activity: list[dict] = []
    monkeypatch.setattr(
        _engine, "log_activity",
        lambda category, action, **kw: activity.append({"action": action, **kw}),
    )
    logger = _engine._MemoryPressureLogger(_MemHandle(["ecommerce.product_media"]))
    assert logger._enabled is True
    # Defeat the 5 s sample throttle so each sample() actually reads (set AFTER __init__
    # so the probe above doesn't consume a tick).
    ticks = iter([0.0, 1000.0, 2000.0, 3000.0])
    monkeypatch.setattr(_engine._time, "monotonic", lambda: next(ticks))
    with caplog.at_level("INFO", logger="dsql_migrator.ui.data_migration._full_load_engine"):
        logger.sample()  # 270 -> INFO high-water
        logger.sample()  # 270 -> nothing
        logger.sample()  # 900 -> INFO high-water + WARNING

    infos = [r for r in caplog.records if r.levelname == "INFO" and "high-water" in r.getMessage()]
    warns = [r for r in caplog.records if r.levelname == "WARNING" and "memory pressure" in r.getMessage()]
    assert len(infos) == 2                     # 270 and 900, not the middle repeat
    assert len(warns) == 1                     # crossed 80% exactly once
    w = warns[0].getMessage()
    assert "ecommerce.product_media" in w      # names the culprit table
    assert "88%" in w or "87%" in w            # 900/1024 ≈ 88%
    assert "task limit" in w

    # The durable activity-log event fires exactly once, at INFO, naming the table.
    mem_events = [e for e in activity if e["action"] == "memory pressure"]
    assert len(mem_events) == 1
    assert mem_events[0]["status"].value == "info"
    assert "ecommerce.product_media" in mem_events[0]["detail"]
    assert "task limit" in mem_events[0]["detail"]


def test_memory_pressure_warning_rearms_only_after_receding(monkeypatch, caplog) -> None:
    # The WARNING fires once, does not repeat while still high, and re-arms after usage
    # drops below the re-arm threshold (so a run hovering near the limit doesn't flap).
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    limit = 1000 * 1024 * 1024
    readings = iter([
        (500 * 1024 * 1024, limit),   # consumed by __init__'s enable probe
        (850 * 1024 * 1024, limit),   # 85% -> WARNING
        (860 * 1024 * 1024, limit),   # still high -> no repeat
        (600 * 1024 * 1024, limit),   # 60% < 70% re-arm -> silent, re-arms
        (900 * 1024 * 1024, limit),   # 90% -> WARNING again
    ])
    monkeypatch.setattr(_engine, "_read_cgroup_memory", lambda: next(readings))
    logger = _engine._MemoryPressureLogger(_MemHandle([]))
    # AFTER __init__ so the probe doesn't consume a tick.
    ticks = iter([0.0, 100.0, 200.0, 300.0, 400.0])
    monkeypatch.setattr(_engine._time, "monotonic", lambda: next(ticks))
    with caplog.at_level("WARNING", logger="dsql_migrator.ui.data_migration._full_load_engine"):
        for _ in range(4):
            logger.sample()

    warns = [r for r in caplog.records if "memory pressure" in r.getMessage()]
    assert len(warns) == 2  # once at 85%, silent at 86% and 60%, again at 90%


def test_read_cgroup_memory_prefers_v2_and_handles_max(monkeypatch, tmp_path) -> None:
    # v2 "max" (unlimited) -> limit None; a numeric max -> that limit.
    from dsql_migrator.ui.data_migration import _full_load_engine as _engine

    cur = tmp_path / "memory.current"
    mx = tmp_path / "memory.max"
    cur.write_text("123456\n")
    monkeypatch.setattr(_engine, "_CGROUP_V2_CURRENT", str(cur))
    monkeypatch.setattr(_engine, "_CGROUP_V2_MAX", str(mx))

    mx.write_text("max\n")
    assert _engine._read_cgroup_memory() == (123456, None)

    mx.write_text(str(2 * 1024 * 1024 * 1024) + "\n")
    used, limit = _engine._read_cgroup_memory()
    assert used == 123456 and limit == 2 * 1024 * 1024 * 1024
