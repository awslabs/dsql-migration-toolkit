# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Job-scoped data-error capture and downloadable error logs.

During a load (Full Load or CDC), per-table/per-row data errors are captured so
the user can download a structured log from the web UI instead of reading raw
stack traces (Usability-first, Requirements 8.3/8.4). :class:`ErrorLogStore`
collects :class:`~dsql_migrator.core.models.DataErrorRecord` entries per job,
reports a :class:`~dsql_migrator.core.models.ErrorLogSummary` for the UI, and
serializes a downloadable NDJSON/CSV artifact.

Storage reuses the job-state backend (in-memory by default; a SQLite/DSQL-schema
backend can be injected for resumability) so **no new service** is required
(no-bloat). The default backend is in-memory and append-only.

Confidentiality (Property 7): the store only serializes the fields of
:class:`DataErrorRecord`, whose ``message`` is English and credential-free by
contract; row values and secret columns are never stored or rendered in
plaintext.

Completeness (Property 15): every captured error is one record, so the UI
summary count equals the number of rows in the rendered log.
"""

from __future__ import annotations

import csv
import io
import threading
from typing import Literal, Protocol

from dsql_migrator.core.models import DataErrorRecord, ErrorLogSummary

# CSV column order for the downloadable artifact (English headers).
_CSV_FIELDS = ("table", "pk", "chunk_id", "error_code", "message", "occurred_at")


class ErrorLogBackend(Protocol):
    """Append-only storage for captured error records, keyed by job id.

    Implementations must be safe to call from multiple threads. The default is
    in-memory; a SQLite or target-DSQL-schema backend can implement the same
    surface for resumability without changing :class:`ErrorLogStore`.
    """

    def append(self, job_id: str, record: DataErrorRecord) -> None:
        """Append one error record for ``job_id``."""

    def list(self, job_id: str) -> list[DataErrorRecord]:
        """Return all records for ``job_id`` in insertion order."""


class InMemoryErrorLogBackend:
    """Thread-safe, append-only in-memory :class:`ErrorLogBackend` (default)."""

    def __init__(self) -> None:
        self._by_job: dict[str, list[DataErrorRecord]] = {}
        self._lock = threading.Lock()

    def append(self, job_id: str, record: DataErrorRecord) -> None:
        """Append ``record`` under ``job_id`` (thread-safe)."""
        with self._lock:
            self._by_job.setdefault(job_id, []).append(record)

    def list(self, job_id: str) -> list[DataErrorRecord]:
        """Return a copy of the records for ``job_id`` in insertion order."""
        with self._lock:
            return list(self._by_job.get(job_id, []))


class ErrorLogStore:
    """Captures data errors per job and renders downloadable error logs."""

    def __init__(self, backend: ErrorLogBackend | None = None) -> None:
        """Create a store over ``backend`` (default: in-memory, append-only)."""
        self._backend: ErrorLogBackend = backend or InMemoryErrorLogBackend()

    def record(self, job_id: str, error: DataErrorRecord) -> None:
        """Append one error record for ``job_id`` (append-only, thread-safe)."""
        self._backend.append(job_id, error)

    def records(self, job_id: str) -> list[DataErrorRecord]:
        """Return all error records for ``job_id`` in insertion order (a copy).

        Lets the UI list the individual quarantined/failed rows (table, reason,
        code, time) inline -- the same rows :meth:`render_log` serializes -- so a
        reader sees WHAT was set aside, not only a count. Records are English and
        credential-free (no row values/secrets -- Property 7).
        """
        return self._backend.list(job_id)

    def summary(self, job_id: str) -> ErrorLogSummary:
        """Return total + per-table counts and whether a log artifact exists.

        ``total_errors`` equals the sum of ``errors_by_table`` and the number of
        rows a rendered log would contain (Property 15). ``log_available`` is
        ``True`` only when there is at least one error.
        """
        records = self._backend.list(job_id)
        errors_by_table: dict[str, int] = {}
        for record in records:
            errors_by_table[record.table] = errors_by_table.get(record.table, 0) + 1
        return ErrorLogSummary(
            total_errors=len(records),
            errors_by_table=errors_by_table,
            log_available=bool(records),
        )

    def latest_messages(self, job_id: str) -> dict[str, str]:
        """Return the most recent error message per table for ``job_id``.

        Lets the UI surface *why* each table failed inline (the cause), rather
        than only a count. Messages are English and credential-free (Property 7).
        """
        messages: dict[str, str] = {}
        for record in self._backend.list(job_id):
            messages[record.table] = record.message
        return messages

    def render_log(
        self, job_id: str, fmt: Literal["ndjson", "csv"] = "ndjson"
    ) -> bytes:
        """Serialize all records for ``job_id`` as downloadable bytes (UTF-8).

        - ``ndjson`` (default): one :class:`DataErrorRecord` JSON object per line.
        - ``csv``: header ``table,pk,chunk_id,error_code,message,occurred_at``
          followed by one row per record (RFC-4180 quoting).

        Fields are English and credential-free; no row values/secrets are emitted
        (Property 7). The row count equals ``summary().total_errors``
        (Property 15).
        """
        records = self._backend.list(job_id)
        if fmt == "csv":
            return self._render_csv(records)
        return self._render_ndjson(records)

    @staticmethod
    def _render_ndjson(records: list[DataErrorRecord]) -> bytes:
        lines = [record.model_dump_json() for record in records]
        text = "\n".join(lines)
        if text:
            text += "\n"
        return text.encode("utf-8")

    @staticmethod
    def _render_csv(records: list[DataErrorRecord]) -> bytes:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(_CSV_FIELDS)
        for record in records:
            writer.writerow(
                [
                    record.table,
                    record.pk or "",
                    record.chunk_id or "",
                    record.error_code or "",
                    record.message,
                    record.occurred_at.isoformat(),
                ]
            )
        return buffer.getvalue().encode("utf-8")


__all__ = [
    "ErrorLogBackend",
    "InMemoryErrorLogBackend",
    "ErrorLogStore",
]
