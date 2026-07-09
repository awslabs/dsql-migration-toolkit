# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Unit tests for the job-scoped error log store (Task 21).

Covers (Property 15 / Property 7 / Requirements 8.3, 8.4, 8.9):
- record/summary/render round-trip; counts match across summary and rendered log.
- Empty job has log_available=False and renders to empty bytes.
- NDJSON renders one record per line; CSV has the English header + one row each.
- Per-table counts are computed correctly; jobs are isolated.
"""

from __future__ import annotations

from datetime import datetime, timezone

from dsql_migrator.core.error_log import ErrorLogStore
from dsql_migrator.core.models import DataErrorRecord


def _rec(table: str, message: str = "boom", pk: str | None = None) -> DataErrorRecord:
    return DataErrorRecord(
        table=table,
        pk=pk,
        message=message,
        occurred_at=datetime(2026, 5, 1, 3, 12, 44, tzinfo=timezone.utc),
    )


def test_summary_counts_match_records() -> None:
    store = ErrorLogStore()
    store.record("job1", _rec("app.orders"))
    store.record("job1", _rec("app.orders"))
    store.record("job1", _rec("app.users"))

    summary = store.summary("job1")
    assert summary.total_errors == 3
    assert summary.errors_by_table == {"app.orders": 2, "app.users": 1}
    assert summary.log_available is True
    # Completeness (Property 15): summary count == sum of per-table counts.
    assert summary.total_errors == sum(summary.errors_by_table.values())


def test_latest_messages_returns_most_recent_per_table() -> None:
    store = ErrorLogStore()
    store.record("j", _rec("app.orders", message="first"))
    store.record("j", _rec("app.orders", message="second"))
    store.record("j", _rec("app.users", message="boom"))

    messages = store.latest_messages("j")
    assert messages == {"app.orders": "second", "app.users": "boom"}
    assert store.latest_messages("absent") == {}


def test_empty_job_has_no_log() -> None:
    store = ErrorLogStore()
    summary = store.summary("nope")
    assert summary.total_errors == 0
    assert summary.errors_by_table == {}
    assert summary.log_available is False
    assert store.render_log("nope") == b""
    assert store.render_log("nope", "csv") == b"table,pk,chunk_id,error_code,message,occurred_at\n"


def test_ndjson_renders_one_record_per_line() -> None:
    store = ErrorLogStore()
    store.record("j", _rec("app.orders", pk="12345"))
    store.record("j", _rec("app.users"))

    out = store.render_log("j").decode("utf-8")
    lines = out.splitlines()
    assert len(lines) == 2
    assert '"table":"app.orders"' in lines[0]
    assert '"pk":"12345"' in lines[0]
    # Row count equals the summary total (Property 15).
    assert len(lines) == store.summary("j").total_errors


def test_csv_has_header_and_rows() -> None:
    store = ErrorLogStore()
    store.record("j", _rec("app.orders", message="invalid input syntax", pk="7"))

    out = store.render_log("j", "csv").decode("utf-8")
    rows = out.splitlines()
    assert rows[0] == "table,pk,chunk_id,error_code,message,occurred_at"
    assert rows[1].startswith("app.orders,7,,,invalid input syntax,2026-05-01T03:12:44")


def test_csv_quotes_messages_with_commas() -> None:
    store = ErrorLogStore()
    store.record("j", _rec("app.orders", message="a, b, c"))
    out = store.render_log("j", "csv").decode("utf-8")
    # The comma-containing message must be quoted so columns are not split.
    assert '"a, b, c"' in out


def test_jobs_are_isolated() -> None:
    store = ErrorLogStore()
    store.record("a", _rec("t1"))
    store.record("b", _rec("t2"))
    assert store.summary("a").total_errors == 1
    assert store.summary("b").total_errors == 1
    assert store.summary("a").errors_by_table == {"t1": 1}
