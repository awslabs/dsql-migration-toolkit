# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the post-migration statistics report (Task 24.3 / Req 13.3).

Covers combining existing outputs (no recomputation -- Req 13.4): rows from the
job, source/target/matched from validation, errors from the single error
summary, and object outcomes from apply results; plus JSON/NDJSON/CSV rendering.
"""

from __future__ import annotations

import csv
import io
import json

from dsql_migrator.core.migration_stats import (
    MigrationStatsBuilder,
    MigrationStatsReport,
)
from dsql_migrator.core.models import (
    ApplyResult,
    ApplyStatus,
    ChunkState,
    ErrorLogSummary,
    MigrationJob,
    TableValidationResult,
    ValidationMode,
    ValidationReport,
)


def _job() -> MigrationJob:
    return MigrationJob(
        job_id="j1",
        chunks=[
            ChunkState(chunk_id="orders", status="DONE", rows_loaded=10),
            ChunkState(chunk_id="customers", status="FAILED", rows_loaded=0),
        ],
    )


def _validation() -> ValidationReport:
    return ValidationReport.build(
        mode=ValidationMode.ROW_COUNT,
        items=[
            TableValidationResult(
                table="orders",
                source_row_count=10,
                target_row_count=10,
                row_count_match=True,
                matched=True,
            ),
            TableValidationResult(
                table="customers",
                source_row_count=3,
                target_row_count=0,
                row_count_match=False,
                matched=False,
            ),
        ],
    )


def test_build_combines_all_sources() -> None:
    report = MigrationStatsBuilder().build(
        _job(),
        validation_report=_validation(),
        error_summary=ErrorLogSummary(
            total_errors=2, errors_by_table={"customers": 2}, log_available=True
        ),
        apply_results=[
            ApplyResult(object_name="orders", status=ApplyStatus.CREATED),
            ApplyResult(object_name="v_recent", status=ApplyStatus.SKIPPED),
        ],
    )

    by_table = {row.table: row for row in report.tables}
    assert by_table["orders"].rows_loaded == 10
    assert by_table["orders"].source_rows == 10
    assert by_table["orders"].target_rows == 10
    assert by_table["orders"].matched is True
    assert by_table["orders"].errors == 0
    assert by_table["customers"].matched is False
    assert by_table["customers"].errors == 2

    statuses = {obj.object_name: obj.apply_status for obj in report.objects}
    assert statuses == {"orders": ApplyStatus.CREATED, "v_recent": ApplyStatus.SKIPPED}


def test_build_without_validation_leaves_source_target_none() -> None:
    report = MigrationStatsBuilder().build(_job())
    orders = next(row for row in report.tables if row.table == "orders")
    assert orders.source_rows is None
    assert orders.target_rows is None
    assert orders.matched is None
    assert orders.rows_loaded == 10
    assert report.objects == []


def test_render_csv_has_header_and_one_row_per_table() -> None:
    report = MigrationStatsBuilder().build(_job(), validation_report=_validation())
    out = report.render("csv").decode("utf-8")
    rows = list(csv.reader(io.StringIO(out)))
    assert rows[0] == [
        "table",
        "rows_loaded",
        "source_rows",
        "target_rows",
        "matched",
        "errors",
    ]
    assert len(rows) == 3  # header + 2 tables
    orders_row = next(r for r in rows[1:] if r[0] == "orders")
    assert orders_row[1] == "10"
    assert orders_row[4] == "True"


def test_render_ndjson_tags_record_type() -> None:
    report = MigrationStatsBuilder().build(
        _job(),
        apply_results=[ApplyResult(object_name="orders", status=ApplyStatus.CREATED)],
    )
    lines = report.render("ndjson").decode("utf-8").splitlines()
    records = [json.loads(line) for line in lines]
    types = [r["record_type"] for r in records]
    assert types.count("table") == 2
    assert types.count("object") == 1
    obj = next(r for r in records if r["record_type"] == "object")
    assert obj["apply_status"] == "CREATED"


def test_render_json_round_trips() -> None:
    report = MigrationStatsBuilder().build(_job(), validation_report=_validation())
    data = json.loads(report.render("json").decode("utf-8"))
    assert {row["table"] for row in data["tables"]} == {"orders", "customers"}
    # Reparse through the model to confirm the JSON is a valid report.
    assert MigrationStatsReport.model_validate(data).tables[0].table in {
        "orders",
        "customers",
    }
