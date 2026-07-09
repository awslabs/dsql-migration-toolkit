# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Durable persistence for migration job state (resumability, Property 4).

The :class:`~dsql_migrator.core.job_manager.JobManager` keeps live job state in
process memory, which is lost when the single-task web app restarts (crash, ECS
task replacement, deploy). To let an interrupted Full Load resume after a
reconnect, the manager can be given a :class:`JobStore`: it persists a snapshot
of each :class:`~dsql_migrator.core.models.MigrationJob` (status, per-chunk
``ChunkState`` with rows/attempts, and the export watermark) on every state
change and reloads them on startup.

Only the **non-secret** job snapshot is stored (Property 7): credentials never
reach this layer. The stored watermark + per-chunk completion is exactly the
checkpoint a resume needs -- already-``DONE`` chunks are skipped and only the
unfinished ones are re-run (idempotent ``INSERT ... ON CONFLICT``, Property 3).

The default :class:`SqliteJobStore` uses the local SQLite file at the configured
``job_state_path`` (single-task default). An :class:`InMemoryJobStore` is
provided for tests so they never touch disk. The store interface is intentionally
small (save / load_all / delete) so a durable backend (DynamoDB/RDS) can replace
SQLite for multi-task deployments without touching the JobManager.
"""

from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol

from dsql_migrator.core.models import MigrationJob


@dataclass(frozen=True)
class PersistedJob:
    """A reloaded job snapshot plus its (already-redacted) failure message."""

    job: MigrationJob
    error: Optional[str] = None


class JobStore(Protocol):
    """Durable store for :class:`MigrationJob` snapshots (resumability)."""

    def save(self, job: MigrationJob, error: Optional[str]) -> None:
        """Persist (upsert) ``job`` and its optional failure message."""

    def load_all(self) -> list[PersistedJob]:
        """Return every persisted job snapshot (oldest update first)."""

    def delete(self, job_id: str) -> None:
        """Remove a job's persisted snapshot, if present."""

    def prune_terminal(self, keep_most_recent: int) -> list[str]:
        """Delete all but the ``keep_most_recent`` newest ``DONE`` jobs.

        Never deletes a non-terminal (resumable/active) job. Returns the ids of
        the snapshots that were deleted so the caller can drop them from memory.
        """


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class InMemoryJobStore:
    """A non-durable :class:`JobStore` for tests (no disk, no SQLite)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, PersistedJob] = {}

    def save(self, job: MigrationJob, error: Optional[str]) -> None:
        with self._lock:
            # Pop+set so the most recently saved job is last (recency order),
            # which prune relies on to keep the newest terminal jobs.
            self._jobs.pop(job.job_id, None)
            # Deep-copy so later mutations of the live job do not leak in.
            self._jobs[job.job_id] = PersistedJob(
                job=job.model_copy(deep=True), error=error
            )

    def load_all(self) -> list[PersistedJob]:
        with self._lock:
            return [
                PersistedJob(job=item.job.model_copy(deep=True), error=item.error)
                for item in self._jobs.values()
            ]

    def delete(self, job_id: str) -> None:
        with self._lock:
            self._jobs.pop(job_id, None)

    def prune_terminal(self, keep_most_recent: int) -> list[str]:
        with self._lock:
            done = [
                jid
                for jid, item in self._jobs.items()
                if item.job.status == "DONE"
            ]
            to_delete = done if keep_most_recent <= 0 else done[:-keep_most_recent]
            for jid in to_delete:
                self._jobs.pop(jid, None)
        return list(to_delete)


class SqliteJobStore:
    """A SQLite-backed :class:`JobStore` at a local file (single-task default).

    Each job is one row keyed by ``job_id``; the snapshot is the job's JSON
    (``MigrationJob.model_dump_json``) so the schema is forward-compatible with
    model changes. All access is serialized through one connection guarded by a
    lock, so concurrent worker/UI threads never corrupt the file or race.
    """

    def __init__(self, path: str) -> None:
        """Open (creating if needed) the job-state database at ``path``."""
        self._lock = threading.Lock()
        # check_same_thread=False: the connection is shared across the worker and
        # UI threads, but every use is serialized by ``self._lock``.
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS jobs ("
            "job_id TEXT PRIMARY KEY, "
            "payload TEXT NOT NULL, "
            "error TEXT, "
            "status TEXT, "
            "updated_at TEXT NOT NULL)"
        )
        # Add the status column to a database created before retention existed.
        try:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN status TEXT")
        except sqlite3.OperationalError:
            pass  # column already exists
        self._conn.commit()

    def save(self, job: MigrationJob, error: Optional[str]) -> None:
        payload = job.model_dump_json()
        with self._lock:
            self._conn.execute(
                "INSERT INTO jobs (job_id, payload, error, status, updated_at) "
                "VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(job_id) DO UPDATE SET "
                "payload = excluded.payload, error = excluded.error, "
                "status = excluded.status, updated_at = excluded.updated_at",
                (job.job_id, payload, error, str(job.status), _now_iso()),
            )
            self._conn.commit()

    def load_all(self) -> list[PersistedJob]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT payload, error FROM jobs ORDER BY updated_at"
            ).fetchall()
        return [
            PersistedJob(job=MigrationJob.model_validate_json(payload), error=error)
            for payload, error in rows
        ]

    def delete(self, job_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
            self._conn.commit()

    def prune_terminal(self, keep_most_recent: int) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT job_id FROM jobs WHERE status = 'DONE' "
                "ORDER BY updated_at DESC"
            ).fetchall()
            keep = max(keep_most_recent, 0)
            to_delete = [row[0] for row in rows[keep:]]
            for job_id in to_delete:
                self._conn.execute("DELETE FROM jobs WHERE job_id = ?", (job_id,))
            self._conn.commit()
        return to_delete

    def close(self) -> None:
        """Close the underlying connection (mainly for tests/teardown)."""
        with self._lock:
            self._conn.close()


__all__ = [
    "PersistedJob",
    "JobStore",
    "InMemoryJobStore",
    "SqliteJobStore",
]
