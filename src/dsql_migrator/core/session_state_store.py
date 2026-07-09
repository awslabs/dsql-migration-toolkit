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

import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Protocol

from pydantic import BaseModel, ConfigDict, Field

from dsql_migrator.core.models import (
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
    # Selected migration type ("full_load_only" / "cdc_only" /
    # "full_load_and_cdc"). Optional/string for forward+backward compatibility:
    # older snapshots lack it and restore as the Full-load-only default.
    migration_type: Optional[str] = None
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
    # AI Assist preference (non-secret: a toggle + Bedrock model id/region, never a
    # credential). Persisted so a reconnecting session keeps the user's choice
    # instead of resetting the toggle to off on every restart. Optional/defaulted
    # so older snapshots restore as AI-off (the safe opt-in default).
    ai_assist_enabled: bool = False
    ai_assist_model_id: Optional[str] = None
    ai_assist_region: Optional[str] = None
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
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
        return SessionSnapshot.model_validate_json(row[0]) if row else None

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


__all__ = [
    "SessionSnapshot",
    "SessionStateStore",
    "InMemorySessionStateStore",
    "SqliteSessionStateStore",
]
