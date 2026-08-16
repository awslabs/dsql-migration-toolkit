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

import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol

from dsql_migrator.core.models import MigrationJob

_LOGGER = logging.getLogger(__name__)


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


class S3JobStore:
    """An S3-backed :class:`JobStore`: one JSON object per job under ``jobs/``.

    Durable across a Fargate **task replacement** (unlike :class:`SqliteJobStore`,
    whose file lives on the task's EPHEMERAL ``/tmp`` and is lost on redeploy), so an
    interrupted Full Load resumes after a deploy and the per-table migration monitor
    survives a redeploy. Reuses the tool's managed plugin bucket (deterministic
    per-account/region name, auto-provisioned) exactly like ``S3SessionStateStore``
    -- no extra customer setup, and the task role's existing S3 grant on the bucket
    ``/*`` already covers the ``jobs/`` prefix.

    **Write coalescing (scale).** The Full Load drain persists on every progress
    tick (``rows_loaded``), which is cheap for local SQLite but would be a PUT storm
    on S3 for a large / billion-row table. Only chunk/job STATUS TRANSITIONS matter
    for resume: a non-``DONE`` chunk is re-run whole (idempotent), sub-chunk progress
    is display-only, and an interrupted in-flight chunk is reconciled to ``FAILED``
    on reload anyway. So a PUT is issued immediately whenever the job's status
    *signature* (``job.status`` + each chunk's status) changes, and pure-progress
    updates between transitions are throttled to at most one PUT per
    ``min_write_interval_seconds``. This bounds PUTs to ~the number of status
    transitions regardless of row count.

    Best-effort by design: any S3 / permission error is logged and never raised, so
    a persistence failure degrades resume without breaking the live migration UI.
    Concurrency: boto3 clients are thread-safe; the coalescing bookkeeping and the
    lazy client build / one-time bucket ensure are guarded by ``_lock``.
    """

    def __init__(
        self,
        bucket: str,
        *,
        region: Optional[str] = None,
        aws_profile: Optional[str] = None,
        prefix: str = "jobs/",
        s3_client: object = None,
        min_write_interval_seconds: float = 5.0,
        clock=None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._aws_profile = aws_profile
        self._prefix = prefix if prefix.endswith("/") else prefix + "/"
        self._lock = threading.Lock()
        self._client = s3_client  # injected in tests; else built lazily
        self._ensured = False
        self._min_interval = float(min_write_interval_seconds)
        self._clock = clock or time.monotonic
        # Coalescing bookkeeping (guarded by _lock): the last-persisted status
        # signature and the monotonic time of the last PUT, per job id.
        self._last_sig: dict[str, tuple] = {}
        self._last_put: dict[str, float] = {}

    # -- internals --------------------------------------------------------- #
    def _s3(self):
        """Return the boto3 S3 client, building it lazily (benign build race)."""
        client = self._client
        if client is None:
            # Build through the shared session factory so this S3 client shares the
            # one credential context (profile-or-default chain) as every other AWS
            # client in the tool, instead of re-implementing that selection here.
            from dsql_migrator.core.aws_session import build_session

            client = build_session(self._aws_profile).client(
                "s3", region_name=self._region
            )
            self._client = client
        return client

    def _key(self, job_id: str) -> str:
        return f"{self._prefix}{job_id}.json"

    def _job_id_from_key(self, key: str) -> Optional[str]:
        if key.startswith(self._prefix) and key.endswith(".json"):
            return key[len(self._prefix) : -len(".json")]
        return None

    def _ensure_bucket(self) -> None:
        """Create the bucket if absent (idempotent). Caller must hold ``_lock``."""
        if self._ensured:
            return
        client = self._s3()
        try:
            client.head_bucket(Bucket=self._bucket)
            self._ensured = True
            return
        except Exception:  # noqa: BLE001 - not found / no access; try to create
            pass
        try:
            if self._region and self._region != "us-east-1":
                client.create_bucket(
                    Bucket=self._bucket,
                    CreateBucketConfiguration={"LocationConstraint": self._region},
                )
            else:
                client.create_bucket(Bucket=self._bucket)
        except Exception as exc:  # noqa: BLE001
            if "BucketAlreadyOwnedByYou" not in str(exc):
                raise
        self._ensured = True

    @staticmethod
    def _status_signature(job: MigrationJob) -> tuple:
        """The state that matters for resume: job status + each chunk's status.

        Excludes ``rows_loaded`` and timestamps so a pure-progress update does not
        change the signature (and so is subject to throttling), while any status
        transition does (and forces an immediate durable PUT).
        """
        return (
            str(job.status),
            tuple((c.chunk_id, str(c.status)) for c in job.chunks),
        )

    # -- JobStore protocol ------------------------------------------------- #
    def save(self, job: MigrationJob, error: Optional[str]) -> None:
        try:
            sig = self._status_signature(job)
            with self._lock:
                changed = self._last_sig.get(job.job_id) != sig
                last = self._last_put.get(job.job_id)
                now = self._clock()
                due = last is None or (now - last) >= self._min_interval
                if not changed and not due:
                    return  # only progress changed and still inside the throttle window
            # Store the job JSON plus its (already-redacted) error in one object;
            # status is duplicated into object metadata so prune reads it via a
            # cheap head_object without downloading + parsing every snapshot.
            body = json.dumps({"job": job.model_dump_json(), "error": error})
            with self._lock:
                self._ensure_bucket()
            self._s3().put_object(
                Bucket=self._bucket,
                Key=self._key(job.job_id),
                Body=body.encode("utf-8"),
                ContentType="application/json",
                Metadata={"status": str(job.status)},
            )
            # Only mark persisted AFTER a successful PUT, so a failed write is retried
            # on the next save (the signature stays "changed" / the window stays due).
            with self._lock:
                self._last_sig[job.job_id] = sig
                self._last_put[job.job_id] = self._clock()
        except Exception:  # noqa: BLE001 - best-effort; must never break the UI
            _LOGGER.warning(
                "Could not persist job %s to s3://%s (resume may not work after a "
                "restart)",
                job.job_id,
                self._bucket,
                exc_info=True,
            )

    def load_all(self) -> list[PersistedJob]:
        out: list[PersistedJob] = []
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        try:
            with self._lock:
                self._ensure_bucket()
            client = self._s3()
            entries: list[tuple[str, datetime]] = []
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
                for obj in page.get("Contents", []):
                    if self._job_id_from_key(obj["Key"]) is not None:
                        entries.append((obj["Key"], obj.get("LastModified") or epoch))
            entries.sort(key=lambda kv: kv[1])  # oldest update first (protocol)
        except Exception:  # noqa: BLE001 - no store yet / transient; nothing to load
            _LOGGER.warning(
                "Could not list jobs in s3://%s", self._bucket, exc_info=True
            )
            return out
        for key, _ in entries:
            try:
                obj = self._s3().get_object(Bucket=self._bucket, Key=key)
                data = json.loads(obj["Body"].read().decode("utf-8"))
                job = MigrationJob.model_validate_json(data["job"])
                out.append(PersistedJob(job=job, error=data.get("error")))
            except Exception:  # noqa: BLE001 - skip a corrupt / incompatible snapshot
                _LOGGER.warning("Ignoring unreadable job snapshot %s", key)
        return out

    def delete(self, job_id: str) -> None:
        try:
            self._s3().delete_object(Bucket=self._bucket, Key=self._key(job_id))
        except Exception:  # noqa: BLE001 - best-effort
            _LOGGER.warning(
                "Could not delete job %s from s3://%s",
                job_id,
                self._bucket,
                exc_info=True,
            )
        with self._lock:
            self._last_sig.pop(job_id, None)
            self._last_put.pop(job_id, None)

    def prune_terminal(self, keep_most_recent: int) -> list[str]:
        epoch = datetime.min.replace(tzinfo=timezone.utc)
        try:
            with self._lock:
                self._ensure_bucket()
            client = self._s3()
            done: list[tuple[str, str, datetime]] = []  # (job_id, key, LastModified)
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    job_id = self._job_id_from_key(key)
                    if job_id is None:
                        continue
                    # Read status from object metadata (cheap head) -- never delete a
                    # non-terminal (resumable) job, and skip a full download/parse.
                    try:
                        head = client.head_object(Bucket=self._bucket, Key=key)
                    except Exception:  # noqa: BLE001 - skip on transient head failure
                        continue
                    if (head.get("Metadata") or {}).get("status") == "DONE":
                        done.append((job_id, key, obj.get("LastModified") or epoch))
            done.sort(key=lambda t: t[2], reverse=True)  # newest first
            keep = max(keep_most_recent, 0)
            deleted: list[str] = []
            for job_id, key, _ in done[keep:]:
                client.delete_object(Bucket=self._bucket, Key=key)
                with self._lock:
                    self._last_sig.pop(job_id, None)
                    self._last_put.pop(job_id, None)
                deleted.append(job_id)
            return deleted
        except Exception:  # noqa: BLE001 - best-effort; skip pruning on error
            _LOGGER.warning(
                "Could not prune jobs in s3://%s", self._bucket, exc_info=True
            )
            return []


__all__ = [
    "PersistedJob",
    "JobStore",
    "InMemoryJobStore",
    "SqliteJobStore",
    "S3JobStore",
]
