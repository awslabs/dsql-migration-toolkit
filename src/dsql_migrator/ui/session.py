# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Per-session, in-memory connection state for the Connect screen.

Credentials entered in the Connect screen are confidential (Property 7 /
Requirement 9.2): they are held only in process memory, scoped to a single UI
session, and are never persisted to disk or written into logs, reports, or job
state. Source passwords are wrapped in
:class:`~dsql_migrator.config.SecretValue` so they cannot leak through
reprs/logs; DSQL targets authenticate with short-lived IAM tokens and therefore
store no password at all.

This module is intentionally free of any NiceGUI dependency so the session
isolation and confidentiality behavior can be unit tested in isolation. The UI
layer (``connect.py``) maps each browser session to one
:class:`SessionConnectionState` via :class:`SessionStore`.
"""

from __future__ import annotations

from typing import Optional

from dsql_migrator.config import SecretValue
from dsql_migrator.core.models import (
    AiAssistConfig,
    AiConversation,
    SourceConnectionConfig,
    TargetConnectionConfig,
    WorkflowState,
)


class SessionConnectionState:
    """In-memory source/target connection and workflow state for one UI session.

    Holds the non-secret :class:`SourceConnectionConfig` /
    :class:`TargetConnectionConfig` models plus the source password as a
    :class:`SecretValue`, and the :class:`WorkflowState` tracking the status of
    the four top-level migration steps. Nothing on this object is serialized, and
    its ``repr``/``str`` never include the password, so it is safe to log.
    """

    __slots__ = (
        "source_config",
        "target_config",
        "_source_password",
        "source_verified",
        "target_verified",
        "source_server_version",
        "source_mysql_version",
        "source_aurora_version",
        "source_instance_class",
        "target_cluster_name",
        "schema_conversion_skipped",
        "_workflow_unlocked",
        "workflow",
        "ai_assist",
        "ai_conversation",
        "aws_profile",
        "active_view",
        "_migration_type",
        "_migration_type_chosen",
        "_cdc_infra_inputs",
        "source_secret_id",
    )

    def __init__(self) -> None:
        self.source_config: Optional[SourceConnectionConfig] = None
        self.target_config: Optional[TargetConnectionConfig] = None
        self._source_password: Optional[SecretValue] = None
        # Whether the most recent source/target connection TEST succeeded. These
        # gate the Connect screen's own Next button: both must be True to
        # continue. A failed or not-yet-run test leaves the corresponding flag
        # False, and editing the connection details invalidates it until
        # re-tested.
        self.source_verified: bool = False
        self.target_verified: bool = False
        # Source server version (e.g. Aurora MySQL version) captured read-only on
        # a successful source test; shown on the overview diagram. Non-secret.
        self.source_server_version: Optional[str] = None
        # Community MySQL engine version (e.g. 8.0.42) behind an Aurora build.
        self.source_mysql_version: Optional[str] = None
        # Aurora MySQL engine version (e.g. 3.07.1) from @@aurora_version;
        # present only for Aurora MySQL sources. Non-secret.
        self.source_aurora_version: Optional[str] = None
        # Source RDS/Aurora instance class (e.g. db.r6g.large) looked up via the
        # RDS API on a successful source test; best effort, shown on the diagram.
        self.source_instance_class: Optional[str] = None
        # DSQL cluster "Name" tag (e.g. "prod-orders") looked up via the DSQL
        # control plane on a successful target test; falls back to the cluster id
        # on the diagram when absent. Best effort, non-secret.
        self.target_cluster_name: Optional[str] = None
        # True when the user chose "Skip conversion & continue to Data Migration"
        # (the target schema was prepared out of band); surfaced in the sidebar.
        self.schema_conversion_skipped: bool = False
        # Sticky latch: set once BOTH connections have been verified at least
        # once this session, and only reset by clear(). The workflow steps gate
        # on this (not on the live verified flags) so that re-locking the
        # Connect screen's Next button -- e.g. when editing a field invalidates a
        # prior test -- does not eject the user from the workflow they already
        # entered (Requirement 8.6: steps are navigable once unlocked).
        self._workflow_unlocked: bool = False
        # Per-session workflow step status; starts with every step NOT_STARTED.
        self.workflow: WorkflowState = WorkflowState()
        # Per-session AI-assist settings. Disabled by default (opt-in): until the
        # user enables it the workflow runs the deterministic-only path
        # (Requirements 11.1, 11.2).
        self.ai_assist: AiAssistConfig = AiAssistConfig()
        # Persistent AI-assistant transcript + panel state for this session. The
        # panel renders FROM this object, so the conversation survives closing/
        # reopening the panel, navigating between steps, and a browser refresh (the
        # session id is cookie-stable). In-memory only; credential-free (Property 7).
        # Cleared by clear() (Start over) only.
        self.ai_conversation: AiConversation = AiConversation()
        # Optional single global AWS named profile applied to ALL AWS clients
        # (DSQL token gen, Secrets Manager, Bedrock-runtime). Only the non-secret
        # profile NAME is held; None means the standard AWS credential chain is
        # used (Requirements 9.5, 9.6, 9.8 / Property 7).
        self.aws_profile: Optional[str] = None
        # The workflow view the user is currently looking at ("connect" or a
        # WorkflowStep value), so a browser refresh restores the same location
        # instead of resetting to the Connect screen. In-memory per session.
        self.active_view: Optional[str] = None
        # The migration pattern the user chose on the Data Migration step
        # (Full load / CDC / Full+CDC). Promoted to the session (was previously
        # only on DataMigrationState) so it is chosen ONCE, early, right after
        # Connect, and readable from every step. Stored as the enum's string
        # value to keep this module free of a ``data_migration`` import (which
        # imports this module -- a cycle). Defaults to full-load-only.
        self._migration_type: str = "full_load_only"
        # Whether the user EXPLICITLY chose the type (vs. sitting on the default).
        # The journey header hides the migration-type banner until this is True, so
        # the steps before the choice do not present the default as a decision.
        self._migration_type_chosen: bool = False
        # BYO-VPC infrastructure inputs entered on the Data Migration step (its
        # Prerequisites or CDC sub-step): VpcId, subnets, plugin S3 keys, source host/secret,
        # DsqlClusterArn, etc. Filled values only; pre-seeded from the target/
        # source config where known. Transient/session-only.
        self._cdc_infra_inputs: dict[str, str] = {}
        # When the source was connected via AWS Secrets Manager auth, the secret's
        # ARN/name entered on Connect. ``source_config.secret`` is NOT populated by
        # build_source_config, so this is the only place the secret id survives the
        # Connect screen -- the CDC deploy reads it to auto-fill SourceSecretArn/Name
        # (Property 7: only the non-secret reference is stored, never the credential).
        self.source_secret_id: Optional[str] = None

    def set_source(
        self,
        config: SourceConnectionConfig,
        password: Optional[SecretValue],
    ) -> None:
        """Record the source config and its in-memory password.

        ``password`` is kept only as a :class:`SecretValue`; the plaintext is
        never stored on the (log-safe) :class:`SourceConnectionConfig`.
        """
        self.source_config = config
        self._source_password = password

    @property
    def source_password(self) -> Optional[SecretValue]:
        """Return the in-memory source password (masked in logs), if any."""
        return self._source_password

    def set_target(self, config: TargetConnectionConfig) -> None:
        """Record the target config (DSQL uses IAM tokens, so no password)."""
        self.target_config = config

    def set_source_verified(self, verified: bool) -> None:
        """Record whether the latest source connection test succeeded."""
        self.source_verified = verified
        if not verified:
            # A failed test or an edited connection invalidates the captured
            # version so the diagram never shows stale source metadata.
            self.source_server_version = None
            self.source_mysql_version = None
            self.source_aurora_version = None
            self.source_instance_class = None
        self._refresh_workflow_unlock()

    def set_target_verified(self, verified: bool) -> None:
        """Record whether the latest target connection test succeeded."""
        self.target_verified = verified
        if not verified:
            # Invalidate captured target metadata so the diagram never shows a
            # stale cluster name after an edit or a failed test.
            self.target_cluster_name = None
        self._refresh_workflow_unlock()

    def set_target_cluster_name(self, name: Optional[str]) -> None:
        """Record the DSQL cluster 'Name' tag looked up on a successful test."""
        self.target_cluster_name = name

    def set_schema_conversion_skipped(self, skipped: bool) -> None:
        """Record whether Schema Conversion was skipped (target prepared OOB)."""
        self.schema_conversion_skipped = skipped

    def set_source_version(
        self,
        version: Optional[str],
        mysql_version: Optional[str] = None,
        aurora_version: Optional[str] = None,
    ) -> None:
        """Record the source server, community MySQL, and Aurora versions."""
        self.source_server_version = version
        self.source_mysql_version = mysql_version
        self.source_aurora_version = aurora_version

    def set_source_instance_class(self, instance_class: Optional[str]) -> None:
        """Record the source RDS instance class looked up on a successful test."""
        self.source_instance_class = instance_class

    def _refresh_workflow_unlock(self) -> None:
        """Latch the workflow as unlocked once both connections are verified.

        The latch is one-way for the lifetime of the session: it never flips
        back to locked when a connection is later invalidated (only ``clear()``
        resets it), so transient re-verification on the Connect screen does not
        re-lock the workflow the user has already entered.
        """
        if self.source_verified and self.target_verified:
            self._workflow_unlocked = True

    def connection_ready(self) -> bool:
        """Return ``True`` only when both source and target tests have passed.

        This is the live gate for the Connect screen's own Next button: editing a
        verified connection re-locks it until re-tested. Workflow-step navigation
        uses the sticky :meth:`workflow_unlocked` latch instead.
        """
        return self.source_verified and self.target_verified

    def workflow_unlocked(self) -> bool:
        """Return whether the workflow steps are unlocked for this session.

        Becomes ``True`` once both connections have been verified at least once
        and stays ``True`` until :meth:`clear`, so navigating between workflow
        steps is never re-blocked by transient connection invalidation.
        """
        return self._workflow_unlocked

    def restore_workflow_unlock(self, unlocked: bool) -> None:
        """Restore the unlock latch from a persisted snapshot (reconnect/resume).

        On reconnect the per-test ``verified`` flags are not persisted (the user
        re-tests on Connect), so the normal both-verified latch would re-lock a
        workflow the user had already entered before the browser closed. This
        restores the latch directly so the resumed session stays navigable.
        """
        if unlocked:
            self._workflow_unlocked = True

    def set_workflow(self, workflow: WorkflowState) -> None:
        """Replace the per-session workflow step status."""
        self.workflow = workflow

    def set_active_view(self, view: Optional[str]) -> None:
        """Record the workflow view the user is on (for refresh restoration)."""
        self.active_view = view

    @property
    def migration_type(self) -> "MigrationType":  # noqa: F821 - lazy-imported type
        """Return the chosen migration pattern as a ``MigrationType`` enum.

        Resolved lazily from the stored string value to avoid importing
        ``data_migration`` (which imports this module) at module load.
        """
        from dsql_migrator.ui.data_migration import MigrationType

        try:
            return MigrationType(self._migration_type)
        except ValueError:
            return MigrationType.FULL_LOAD_ONLY

    def set_migration_type(self, migration_type: object) -> None:
        """Record the chosen migration pattern (accepts a ``MigrationType`` or str).

        Stored as the enum's string value so this module stays import-cycle free.
        Also latches :attr:`migration_type_chosen`: the type has a default
        (full-load-only), so without this flag nothing could tell "the user picked
        Full load only" apart from "the user has not decided yet".
        """
        value = getattr(migration_type, "value", migration_type)
        self._migration_type = str(value)
        self._migration_type_chosen = True

    def set_migration_type_chosen(self, chosen: bool) -> None:
        """Set the explicit-choice latch directly (snapshot restore only).

        :meth:`set_migration_type` latches it to ``True``, which is right for a real
        user action but wrong for a restore: replaying a persisted type would make a
        session that never chose look as if it had. The restore path re-asserts the
        persisted value through this setter.
        """
        self._migration_type_chosen = bool(chosen)

    def migration_type_chosen(self) -> bool:
        """Whether the user has EXPLICITLY chosen a migration type yet.

        ``migration_type`` always answers (it defaults to full-load-only), so this is
        the only way to know the answer is real. Used to keep the journey header's
        migration-type banner hidden on the steps that come BEFORE the choice
        (Evaluation, Schema Conversion) -- otherwise the default was presented as a
        settled decision the user never made. Set by :meth:`set_migration_type` and
        restored from a session snapshot.
        """
        return self._migration_type_chosen

    def set_cdc_infra_inputs(self, inputs: dict[str, str]) -> None:
        """Replace the BYO-VPC infrastructure inputs entered for the CDC deploy."""
        self._cdc_infra_inputs = dict(inputs)

    def cdc_infra_inputs(self) -> dict[str, str]:
        """Return a copy of the entered BYO-VPC infrastructure inputs."""
        return dict(self._cdc_infra_inputs)

    def set_source_secret_id(self, secret_id: Optional[str]) -> None:
        """Record the source-credentials Secrets Manager ARN/name (or None).

        Set on a successful source connection test that used Secrets Manager auth;
        the CDC deploy reads it to auto-fill the SourceSecretArn/Name parameters so
        the user never re-enters them. Only the non-secret reference is stored.
        """
        self.source_secret_id = (secret_id or "").strip() or None

    def set_ai_assist(self, config: AiAssistConfig) -> None:
        """Replace the per-session AI-assist settings (opt-in, default off)."""
        self.ai_assist = config

    def set_aws_profile(self, profile: Optional[str]) -> None:
        """Record the optional global AWS profile name (or None for env chain).

        Only the non-secret profile name is stored; no credential value is ever
        kept here, so it is safe to persist/log (Property 7 / Requirement 9.8).
        """
        self.aws_profile = profile

    def has_source(self) -> bool:
        """Return ``True`` once a source connection has been configured."""
        return self.source_config is not None

    def has_target(self) -> bool:
        """Return ``True`` once a target connection has been configured."""
        return self.target_config is not None

    def clear(self) -> None:
        """Discard all connection state and credentials for this session."""
        self.source_config = None
        self.target_config = None
        self._source_password = None
        self.source_verified = False
        self.target_verified = False
        self.source_server_version = None
        self.source_mysql_version = None
        self.source_aurora_version = None
        self.source_instance_class = None
        self.target_cluster_name = None
        self.schema_conversion_skipped = False
        self._workflow_unlocked = False
        self.workflow = WorkflowState()
        self.ai_assist = AiAssistConfig()
        # Start over discards the AI transcript too: a fresh journey starts a fresh
        # conversation (a stale prior chat would be confusing).
        self.ai_conversation = AiConversation()
        self.aws_profile = None
        self.active_view = None
        self._migration_type = "full_load_only"
        self._migration_type_chosen = False
        self._cdc_infra_inputs = {}
        self.source_secret_id = None

    def __repr__(self) -> str:
        return (
            "SessionConnectionState("
            f"source={'set' if self.source_config is not None else 'unset'}, "
            f"target={'set' if self.target_config is not None else 'unset'})"
        )


class SessionStore:
    """Process-memory map of session id to :class:`SessionConnectionState`.

    Each UI session sees only its own state (session isolation): credentials
    entered in one session are never visible to another. The backing store is a
    plain in-memory dictionary, so nothing is written to disk; clearing a
    session or ending the process discards its credentials entirely.
    """

    def __init__(self) -> None:
        self._states: dict[str, SessionConnectionState] = {}

    def get_or_create(self, session_id: str) -> SessionConnectionState:
        """Return the state for ``session_id``, creating an empty one if needed."""
        state = self._states.get(session_id)
        if state is None:
            state = SessionConnectionState()
            self._states[session_id] = state
        return state

    def get(self, session_id: str) -> Optional[SessionConnectionState]:
        """Return the state for ``session_id``, or ``None`` if absent."""
        return self._states.get(session_id)

    def clear(self, session_id: Optional[str]) -> None:
        """Remove and wipe the state for ``session_id`` (no-op if absent)."""
        if session_id is None:
            return
        state = self._states.pop(session_id, None)
        if state is not None:
            state.clear()

    def reset_in_place(self, session_id: Optional[str]) -> None:
        """Wipe the session's state WITHOUT replacing the object instance.

        "Start over" must reset every field, but the workflow page captured the
        live ``SessionConnectionState`` object in its nav/select closures at build
        time. Popping + recreating (``clear``) would orphan that captured object:
        re-verifying the connections would update a NEW instance while the nav
        guard still reads the old one (it never re-latches, so steps stay locked).
        Resetting the SAME object in place keeps every closure pointing at the
        object that re-verification updates. No-op if absent (a later
        ``get_or_create`` then makes a fresh one, which is fine).
        """
        if session_id is None:
            return
        state = self._states.get(session_id)
        if state is not None:
            state.clear()

    def active_session_count(self) -> int:
        """Return the number of sessions currently held in memory."""
        return len(self._states)


__all__ = ["SessionConnectionState", "SessionStore"]
