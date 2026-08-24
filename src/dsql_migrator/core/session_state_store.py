# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Durable per-session workbench state for reconnect/resume (Property 4).

The web UI keeps each session's progress in process memory (the workflow step
statuses, the Step-1 evaluation result, the Step-2 generated objects, and the
Data Migration job linkage). That memory is lost when the single-task app
restarts, so a reconnecting browser would otherwise start over even though the
durable :class:`~dsql_migrator.core.job_store.JobStore` (step A) still holds the
job. This module persists a **non-secret** snapshot of that session state keyed
by the stable browser ``session_id`` and reloads it on reconnect, so the user
returns to the same step with the same evaluation/selection and re-attaches to
their (possibly interrupted) Full Load job.

Only serializable, non-secret state is stored (Property 7): source credentials
are never captured here -- the user re-enters them on the Connect screen to
resume a run. The advisory AI assessment is intentionally dropped (it is
regenerable); the deterministic inventory/assessment/target catalog are kept so
the downstream screens are fully functional after a restore.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol

_LOGGER = logging.getLogger(__name__)

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import (
    AiConversation,
    AssessmentReport,
    SourceInventory,
    TableSelection,
    TargetInventory,
    ValidationReport,
    WorkflowState,
)


class SessionSnapshot(BaseModel):
    """A persisted, non-secret snapshot of one UI session's workbench state."""

    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    workflow: WorkflowState = Field(default_factory=WorkflowState)
    # Step 1 (Evaluation) deterministic result -- the keystone the downstream
    # screens depend on. Persisted so a restore does not require re-introspection.
    inventory: Optional[SourceInventory] = None
    assessment: Optional[AssessmentReport] = None
    target_inventory: Optional[TargetInventory] = None
    target_conflicts: list[str] = Field(default_factory=list)
    # Step 2 (Schema Conversion): object-leaf node ids whose DDL was generated.
    generated_node_ids: Optional[list[str]] = None
    # Step 2 (Schema Conversion): object-leaf node ids currently ticked in the
    # source object browser. Persisted separately from generated_node_ids so a
    # restored session restores the user's checkbox selection (not only the
    # already-generated set), matching what they had selected before a restart.
    ticked_node_ids: Optional[list[str]] = None
    # Step 2 (Schema Conversion): per-object EDITED target DDL (qualified object
    # name -> the user's customized CREATE DDL). Persisted so a Full Load re-run
    # after a reconnect/restart recreates a table with the customized schema (e.g.
    # a TINYINT(1)->smallint remap) instead of reverting to the deterministic
    # conversion and clobbering the applied target schema.
    edited_target_ddls: dict[str, str] = Field(default_factory=dict)
    # Step 3 (Data Migration) job linkage + table selection.
    migration_job_id: Optional[str] = None
    migration_selection: TableSelection = Field(default_factory=TableSelection)
    migration_selection_touched: bool = False
    migration_active_substep: Optional[str] = None
    # The prerequisite mode ("FULL_LOAD" / "CDC") that gated the most recent
    # started Full Load. The reports themselves are deliberately not persisted, so
    # the run-guard excuses an absent report once a run exists; this scopes that
    # excuse to the mode that actually cleared the gate, so switching the type to
    # add CDC afterwards does not inherit a Full-load-only pass. Optional/string
    # for compatibility: older snapshots lack it and restore as None, which keeps
    # the previous lenient behavior (never hard-blocks a reconnect).
    migration_prereq_gated_mode: Optional[str] = None
    # Selected migration type ("full_load_only" / "cdc_only" /
    # "full_load_and_cdc"). Optional/string for forward+backward compatibility:
    # older snapshots lack it and restore as the Full-load-only default.
    migration_type: Optional[str] = None
    # Whether the user EXPLICITLY chose that type (vs. still sitting on the
    # default). ``migration_type`` always has a value, so this is the only way to
    # tell the two apart; the journey header hides its migration-type banner until
    # a real choice exists. Older snapshots lack it and restore as False, which is
    # the safe direction (the banner reappears as soon as the type is re-picked).
    migration_type_chosen: bool = False
    # CDC pipeline state (Phase 5). All optional with defaults so snapshots
    # written before CDC persistence restore cleanly. The manual start-position
    # override (GTID / binlog file:position) and the oversized-LOB exclusions are
    # operator choices that must survive a restart; the connector names link the
    # session back to the deployed pipeline for live monitoring.
    # "auto" (gapless from watermark) or "manual" (explicit coordinates).
    cdc_start_mode: Optional[str] = None
    cdc_start_gtid: Optional[str] = None
    cdc_start_binlog_file: Optional[str] = None
    cdc_start_binlog_pos: Optional[int] = None
    # LOB exclusions flattened to "table:column" strings (stable, sorted).
    cdc_lob_exclusions: list[str] = Field(default_factory=list)
    cdc_connector_names: list[str] = Field(default_factory=list)
    # The in-flight (or just-finished) CDC lifecycle job (deploy-infra / start /
    # stop / delete) and which operation it is, so a reconnecting session can keep
    # showing that job's ordered stages instead of dropping them on a restart. The
    # job itself is reconciled by the JobManager on restore (a RUNNING one becomes
    # FAILED); persisting the id+kind lets the CDC card still render its stage
    # breakdown and terminal message. Optional/defaulted so older snapshots
    # restore cleanly.
    cdc_deploy_job_id: Optional[str] = None
    cdc_action_kind: Optional[str] = None
    # CDC infrastructure identity + inputs so a reconnecting session knows which
    # cdc-stack it owns and the VpcId/subnet inputs it deployed with. With these
    # restored, re-probing AWS (describe_stacks) recovers the live phase (Infra
    # ready / Streaming) instead of showing a blank Deploy form. Optional/defaulted
    # so older snapshots restore cleanly.
    cdc_stack_name: Optional[str] = None
    cdc_infra_inputs: dict[str, str] = Field(default_factory=dict)
    # Non-secret target (Aurora DSQL) connection so a reconnecting session can
    # re-probe the cdc-stack phase WITHOUT the user re-entering the target first
    # (DSQL auth is IAM-token based, so endpoint + region are enough to describe).
    # Source is intentionally NOT persisted (it carries a password / secret ref).
    # The sticky workflow-unlock latch is persisted so restored steps stay
    # navigable. ``target_verified`` is deliberately NOT persisted: the user still
    # re-tests on Connect to confirm live access, but the workbench resumes.
    target_endpoint: Optional[str] = None
    target_region: Optional[str] = None
    target_database: Optional[str] = None
    target_username: Optional[str] = None
    # The source ENGINE kind ("mysql" / "postgres") -- NOT the source connection (that
    # carries a secret and stays unpersisted, above). Kept so a snapshot restore can
    # pre-select the right engine on the Connect screen instead of always defaulting to
    # MySQL; a PostgreSQL operator resuming a restored workbench would otherwise land on
    # MySQL + port 3306 and have to re-pick. Optional/None on older snapshots -> the
    # picker falls back to the MySQL default (unchanged for every MySQL session).
    source_type: Optional[str] = None
    # AI Assist preference (non-secret: a toggle + Bedrock model id/region, never a
    # credential). Persisted so a reconnecting session keeps the user's choice
    # instead of resetting the toggle to off on every restart. Optional/defaulted
    # so older snapshots restore as AI-off (the safe opt-in default).
    ai_assist_enabled: bool = False
    ai_assist_model_id: Optional[str] = None
    ai_assist_region: Optional[str] = None
    # The persistent AI-assistant transcript (messages + active scope + open/closed),
    # so the conversation survives an app restart / crash -- not just a browser
    # refresh. Credential-free and row-data-free (Property 7); "Start over" deletes the
    # whole snapshot, so an intentional reset still clears it. None on older snapshots.
    ai_conversation: Optional[AiConversation] = None
    workflow_unlocked: bool = False
    # The workflow view the user was last looking at ("connect" or a WorkflowStep
    # value) so a reconnect reopens the same step instead of resetting to Connect.
    active_view: Optional[str] = None
    # Step 4 (Validation) last report + when it finished, so a reconnect reopens
    # the same result page instead of resetting to "Re-run" (mirrors the Evaluation
    # result restore). Credential-free by construction (counts / PK summaries /
    # SQLSTATE only -- Property 7). ``None`` until a validation has completed; the
    # restore surfaces a "restored as-of <time>; re-validate if the source changed"
    # note since a stored verdict can go stale as the source advances.
    validation_report: Optional[ValidationReport] = None
    validation_completed_at: Optional[datetime] = None
    # Tables in ``validation_report`` whose result came from a later per-table
    # re-check, and when that re-check finished. Persisted so the reconnected report
    # can still state that those rows are NEWER than the rest of the run -- without
    # them a merged report would silently read as one uniform comparison. Empty /
    # ``None`` for a report that was never partially re-checked (the usual case).
    validation_rechecked_tables: list[str] = Field(default_factory=list)
    validation_rechecked_at: Optional[datetime] = None


class SessionStateStore(Protocol):
    """Durable store for :class:`SessionSnapshot` keyed by session id."""

    def save(self, snapshot: SessionSnapshot) -> None:
        """Persist (upsert) ``snapshot`` for its session."""

    def load(self, session_id: str) -> Optional[SessionSnapshot]:
        """Return the persisted snapshot for ``session_id``, or ``None``."""

    def delete(self, session_id: str) -> None:
        """Remove a session's persisted snapshot, if present."""

    def prune(self, keep_most_recent: int) -> list[str]:
        """Delete all but the ``keep_most_recent`` most recently saved sessions.

        Returns the ids of the snapshots that were deleted.
        """


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class InMemorySessionStateStore:
    """A non-durable :class:`SessionStateStore` for tests (no disk)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshots: dict[str, str] = {}

    def save(self, snapshot: SessionSnapshot) -> None:
        with self._lock:
            # Pop+set so the newest save is last (recency order for prune).
            self._snapshots.pop(snapshot.session_id, None)
            self._snapshots[snapshot.session_id] = snapshot.model_dump_json()

    def load(self, session_id: str) -> Optional[SessionSnapshot]:
        with self._lock:
            payload = self._snapshots.get(session_id)
        return SessionSnapshot.model_validate_json(payload) if payload else None

    def delete(self, session_id: str) -> None:
        with self._lock:
            self._snapshots.pop(session_id, None)

    def prune(self, keep_most_recent: int) -> list[str]:
        with self._lock:
            ids = list(self._snapshots.keys())
            to_delete = ids if keep_most_recent <= 0 else ids[:-keep_most_recent]
            for session_id in to_delete:
                self._snapshots.pop(session_id, None)
        return list(to_delete)


class SqliteSessionStateStore:
    """A SQLite-backed :class:`SessionStateStore` at a local file."""

    def __init__(self, path: str) -> None:
        """Open (creating if needed) the session-state database at ``path``."""
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions ("
            "session_id TEXT PRIMARY KEY, "
            "payload TEXT NOT NULL, "
            "updated_at TEXT NOT NULL)"
        )
        self._conn.commit()

    def save(self, snapshot: SessionSnapshot) -> None:
        payload = snapshot.model_dump_json()
        with self._lock:
            self._conn.execute(
                "INSERT INTO sessions (session_id, payload, updated_at) "
                "VALUES (?, ?, ?) "
                "ON CONFLICT(session_id) DO UPDATE SET "
                "payload = excluded.payload, updated_at = excluded.updated_at",
                (snapshot.session_id, payload, _now_iso()),
            )
            self._conn.commit()

    def load(self, session_id: str) -> Optional[SessionSnapshot]:
        """Return the persisted snapshot, or ``None`` when absent/unreadable.

        A snapshot that no longer validates must DEGRADE, not crash. ``SessionSnapshot``
        (and ``WorkflowState``) are ``extra="forbid"``, so a payload written by a newer
        build -- or one naming a field that has since been removed -- raises here; with
        the exception propagating, the whole page build failed and the user was locked
        out of the tool rather than merely losing the restored progress. Mirrors the S3
        store, which already warns and returns ``None``.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        if not row:
            return None
        try:
            return SessionSnapshot.model_validate_json(row[0])
        except Exception:  # noqa: BLE001 - unreadable snapshot: start fresh, don't crash
            _LOGGER.warning(
                "Could not parse persisted session %s; ignoring it", session_id,
                exc_info=True,
            )
            return None

    def delete(self, session_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM sessions WHERE session_id = ?", (session_id,)
            )
            self._conn.commit()

    def prune(self, keep_most_recent: int) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT session_id FROM sessions ORDER BY updated_at DESC"
            ).fetchall()
            keep = max(keep_most_recent, 0)
            to_delete = [row[0] for row in rows[keep:]]
            for session_id in to_delete:
                self._conn.execute(
                    "DELETE FROM sessions WHERE session_id = ?", (session_id,)
                )
            self._conn.commit()
        return to_delete

    def close(self) -> None:
        """Close the underlying connection (mainly for tests/teardown)."""
        with self._lock:
            self._conn.close()


class S3SessionStateStore:
    """An S3-backed :class:`SessionStateStore`: one JSON object per session.

    Durable across a Fargate **task replacement** (unlike :class:`SqliteSessionStateStore`,
    whose file lives on the task's EPHEMERAL disk and is lost on redeploy/restart),
    so a reconnecting browser resumes its workbench even after a new image is
    deployed. Stores only the non-secret :class:`SessionSnapshot` (Property 7 --
    source credentials are never in it). Reuses the tool's managed plugin bucket
    (deterministic per-account/region name, auto-provisioned), so it adds NO
    customer setup -- the same "the tool provisions its own S3" convenience the
    plugin/staging paths already use.

    Best-effort by design: a transient S3 / permission error is logged and never
    raised to the caller, so persistence degrades to "resume may not work" instead
    of breaking the live migration UI (the in-process session still functions).
    Concurrency: the control plane runs as a single task and boto3 clients are
    thread-safe, so only the lazy client build + one-time bucket ensure are guarded.
    """

    def __init__(
        self,
        bucket: str,
        *,
        region: Optional[str] = None,
        aws_profile: Optional[str] = None,
        prefix: str = "sessions/",
        s3_client: object = None,
    ) -> None:
        self._bucket = bucket
        self._region = region
        self._aws_profile = aws_profile
        self._prefix = prefix if prefix.endswith("/") else prefix + "/"
        self._lock = threading.Lock()
        self._client = s3_client  # injected in tests; else built lazily
        self._ensured = False

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

    def _key(self, session_id: str) -> str:
        return f"{self._prefix}{session_id}.json"

    def _session_id_from_key(self, key: str) -> Optional[str]:
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
    def _is_absent(exc: Exception) -> bool:
        """True when an S3 error means the object/bucket simply does not exist."""
        code = ""
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            code = str(response.get("Error", {}).get("Code", ""))
        blob = f"{code} {exc}"
        return any(
            marker in blob
            for marker in ("NoSuchKey", "NoSuchBucket", "404", "Not Found")
        )

    # -- SessionStateStore protocol --------------------------------------- #
    def save(self, snapshot: SessionSnapshot) -> None:
        try:
            # Serialize INSIDE the guard: SessionSnapshot has no validate_assignment,
            # so a caller that set a field post-construction could (in theory) make
            # model_dump_json raise -- and this store's contract is that save() never
            # raises to the caller (persistence is best-effort; the UI must not break).
            payload = snapshot.model_dump_json()
            with self._lock:
                self._ensure_bucket()
            self._s3().put_object(
                Bucket=self._bucket,
                Key=self._key(snapshot.session_id),
                Body=payload.encode("utf-8"),
                ContentType="application/json",
            )
        except Exception:  # noqa: BLE001 - best-effort; must never break the UI
            _LOGGER.warning(
                "Could not persist session %s to s3://%s (resume may not work "
                "after a restart)",
                snapshot.session_id,
                self._bucket,
                exc_info=True,
            )

    def load(self, session_id: str) -> Optional[SessionSnapshot]:
        try:
            obj = self._s3().get_object(Bucket=self._bucket, Key=self._key(session_id))
            payload = obj["Body"].read().decode("utf-8")
        except Exception as exc:  # noqa: BLE001
            if not self._is_absent(exc):
                _LOGGER.warning(
                    "Could not read session %s from s3://%s",
                    session_id,
                    self._bucket,
                    exc_info=True,
                )
            return None
        try:
            return SessionSnapshot.model_validate_json(payload)
        except Exception:  # noqa: BLE001 - corrupt / incompatible snapshot
            _LOGGER.warning("Ignoring unreadable session snapshot %s", session_id)
            return None

    def delete(self, session_id: str) -> None:
        try:
            self._s3().delete_object(Bucket=self._bucket, Key=self._key(session_id))
        except Exception:  # noqa: BLE001 - best-effort
            _LOGGER.warning(
                "Could not delete session %s from s3://%s",
                session_id,
                self._bucket,
                exc_info=True,
            )

    def prune(self, keep_most_recent: int) -> list[str]:
        try:
            with self._lock:
                self._ensure_bucket()
            client = self._s3()
            objects: list[tuple[str, datetime]] = []
            epoch = datetime.min.replace(tzinfo=timezone.utc)
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
                for obj in page.get("Contents", []):
                    objects.append((obj["Key"], obj.get("LastModified") or epoch))
            # Newest first; keep the most-recent N, delete the older remainder.
            objects.sort(key=lambda kv: kv[1], reverse=True)
            keep = max(keep_most_recent, 0)
            deleted: list[str] = []
            for key, _ in objects[keep:]:
                client.delete_object(Bucket=self._bucket, Key=key)
                session_id = self._session_id_from_key(key)
                if session_id is not None:
                    deleted.append(session_id)
            return deleted
        except Exception:  # noqa: BLE001 - best-effort; skip pruning on error
            _LOGGER.warning(
                "Could not prune sessions in s3://%s", self._bucket, exc_info=True
            )
            return []


__all__ = [
    "SessionSnapshot",
    "SessionStateStore",
    "InMemorySessionStateStore",
    "SqliteSessionStateStore",
    "S3SessionStateStore",
]
