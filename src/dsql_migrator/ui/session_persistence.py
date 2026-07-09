# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Capture/restore a UI session's workbench state for reconnect/resume.

These pure helpers translate between the in-memory per-session state objects
(:class:`~dsql_migrator.ui.session.SessionConnectionState`,
:class:`~dsql_migrator.ui.evaluation.EvaluationState`,
:class:`~dsql_migrator.ui.schema_conversion.SchemaConversionState`,
:class:`~dsql_migrator.ui.data_migration.DataMigrationState`) and the durable,
non-secret :class:`~dsql_migrator.core.session_state_store.SessionSnapshot`.

They are NiceGUI-agnostic so the capture/apply/dirty-check logic is unit
testable. The :func:`session_signature` lets the persistence hook skip writes
when only background job *progress* changed (that is persisted separately by the
:class:`~dsql_migrator.core.job_store.JobStore`), so a large inventory is never
re-serialized on every UI poll (large-scale: avoid O(rows) repeated work).
"""

from __future__ import annotations

from typing import Optional

from dsql_migrator.core.session_state_store import SessionSnapshot
from dsql_migrator.ui.workflow import WorkflowStep


def _flatten_lob_exclusions(exclusions: dict) -> list[str]:
    """Flatten ``{table: {col, ...}}`` to sorted ``"table:column"`` strings."""
    flat = [
        f"{table}:{column}"
        for table, columns in exclusions.items()
        for column in columns
    ]
    return sorted(flat)


def _restore_lob_exclusions(migration_state: object, flat: list) -> None:
    """Restore ``"table:column"`` strings onto the state's LOB exclusions."""
    for entry in flat or []:
        table, sep, column = entry.partition(":")
        if sep and table and column:
            migration_state.set_cdc_lob_exclusion(table, column, True)  # type: ignore[attr-defined]


def capture_session_snapshot(
    session_id: str,
    session: object,
    eval_state: object,
    conv_state: object,
    migration_state: object,
    validation_state: object = None,
) -> SessionSnapshot:
    """Build a non-secret :class:`SessionSnapshot` from the live session state."""
    snapshot = SessionSnapshot(
        session_id=session_id,
        workflow=session.workflow.model_copy(deep=True),  # type: ignore[attr-defined]
        generated_node_ids=(
            list(conv_state.generated_node_ids)  # type: ignore[attr-defined]
            if conv_state.generated_node_ids is not None  # type: ignore[attr-defined]
            else None
        ),
        ticked_node_ids=(
            list(conv_state.ticked_node_ids)  # type: ignore[attr-defined]
            if conv_state.ticked_node_ids  # type: ignore[attr-defined]
            else None
        ),
        edited_target_ddls=dict(conv_state.edited_target_ddls),  # type: ignore[attr-defined]
        migration_job_id=migration_state.job_id,  # type: ignore[attr-defined]
        migration_selection=migration_state.selection.model_copy(deep=True),  # type: ignore[attr-defined]
        migration_selection_touched=migration_state.selection_touched,  # type: ignore[attr-defined]
        migration_active_substep=migration_state.active_substep,  # type: ignore[attr-defined]
        migration_type=migration_state.migration_type.value,  # type: ignore[attr-defined]
        cdc_start_mode=migration_state.cdc_start_mode(),  # type: ignore[attr-defined]
        cdc_start_gtid=migration_state._cdc_start_gtid,  # type: ignore[attr-defined]
        cdc_start_binlog_file=migration_state._cdc_start_binlog_file,  # type: ignore[attr-defined]
        cdc_start_binlog_pos=migration_state._cdc_start_binlog_pos,  # type: ignore[attr-defined]
        cdc_lob_exclusions=_flatten_lob_exclusions(
            migration_state.cdc_lob_exclusions()  # type: ignore[attr-defined]
        ),
        cdc_connector_names=list(
            getattr(migration_state, "cdc_connector_names", []) or []
        ),
        cdc_deploy_job_id=getattr(migration_state, "cdc_deploy_job_id", None),
        cdc_action_kind=getattr(migration_state, "cdc_action_kind", None),
        cdc_stack_name=getattr(migration_state, "cdc_stack_name", None),
        cdc_infra_inputs=dict(migration_state.cdc_infra_inputs()),  # type: ignore[attr-defined]
        ai_assist_enabled=bool(getattr(session.ai_assist, "enabled", False)),  # type: ignore[attr-defined]
        ai_assist_model_id=getattr(session.ai_assist, "model_id", None),  # type: ignore[attr-defined]
        ai_assist_region=getattr(session.ai_assist, "region", None),  # type: ignore[attr-defined]
        workflow_unlocked=bool(session.workflow_unlocked()),  # type: ignore[attr-defined]
        active_view=getattr(session, "active_view", None),
    )
    # Non-secret target connection (DSQL = IAM auth) so a reconnect can re-probe.
    target = getattr(session, "target_config", None)
    if target is not None:
        snapshot.target_endpoint = getattr(target, "cluster_endpoint", None)
        snapshot.target_region = getattr(target, "region", None)
        snapshot.target_database = getattr(target, "database", None)
        snapshot.target_username = getattr(target, "username", None)
    result = eval_state.result  # type: ignore[attr-defined]
    if result is not None:
        snapshot.inventory = result.inventory
        snapshot.assessment = result.assessment
        snapshot.target_inventory = result.target_inventory
        snapshot.target_conflicts = list(result.target_conflicts)
    # Step 4 (Validation): persist the last completed report so a reconnect reopens
    # the result page (credential-free; mirrors the Evaluation result restore).
    if validation_state is not None:
        v_report = getattr(validation_state, "result", None)
        if v_report is not None:
            snapshot.validation_report = v_report
            snapshot.validation_completed_at = getattr(
                validation_state, "completed_at", None
            )
    return snapshot


def apply_session_snapshot(
    snapshot: SessionSnapshot,
    session: object,
    eval_state: object,
    conv_state: object,
    migration_state: object,
    validation_state: object = None,
) -> None:
    """Restore a :class:`SessionSnapshot` onto the live (fresh) session state.

    The Evaluation result is rebuilt only when its deterministic parts are all
    present; the advisory AI assessment is left ``None`` (regenerable). The last
    Validation report is re-hydrated (flagged restored, with its completion time)
    so the result page reopens. Source credentials are not restored -- the user
    re-enters them on Connect to resume.
    """
    from dsql_migrator.ui.data_migration import MigrationType
    from dsql_migrator.ui.evaluation import EvaluationResult

    session.set_workflow(snapshot.workflow.model_copy(deep=True))  # type: ignore[attr-defined]
    # Step 4 (Validation): re-hydrate the last report so a reconnect reopens the
    # result page instead of resetting to "Re-run". Marked restored so the UI can
    # note it is as-of the saved time (re-validate if the source has since changed).
    if validation_state is not None and snapshot.validation_report is not None:
        restore = getattr(validation_state, "restore", None)
        if callable(restore):
            restore(
                snapshot.validation_report.model_copy(deep=True),
                snapshot.validation_completed_at,
            )

    # Reopen the last-viewed step on reconnect (app.py applies a back-compat
    # redirect for the retired standalone "cdc" view afterwards).
    if snapshot.active_view and hasattr(session, "set_active_view"):
        session.set_active_view(snapshot.active_view)  # type: ignore[attr-defined]

    if (
        snapshot.inventory is not None
        and snapshot.assessment is not None
        and snapshot.target_inventory is not None
    ):
        eval_state.set_result(  # type: ignore[attr-defined]
            EvaluationResult(
                inventory=snapshot.inventory,
                assessment=snapshot.assessment,
                target_inventory=snapshot.target_inventory,
                target_conflicts=list(snapshot.target_conflicts),
            )
        )

    if snapshot.generated_node_ids is not None:
        conv_state.generated_node_ids = list(snapshot.generated_node_ids)  # type: ignore[attr-defined]
    if snapshot.ticked_node_ids is not None:
        conv_state.ticked_node_ids = list(snapshot.ticked_node_ids)  # type: ignore[attr-defined]
    if snapshot.edited_target_ddls:
        # Restore the customized target DDLs so a re-run recreates tables with the
        # user's schema (e.g. a smallint remap), not the deterministic conversion.
        conv_state.edited_target_ddls = dict(snapshot.edited_target_ddls)  # type: ignore[attr-defined]

    migration_state.job_id = snapshot.migration_job_id  # type: ignore[attr-defined]
    migration_state.selection = snapshot.migration_selection.model_copy(deep=True)  # type: ignore[attr-defined]
    migration_state.selection_touched = snapshot.migration_selection_touched  # type: ignore[attr-defined]
    migration_state.active_substep = snapshot.migration_active_substep  # type: ignore[attr-defined]
    # Bind the session so the migration_type restore writes through to it (the
    # session is the authoritative store now that the mode is chosen early).
    if hasattr(migration_state, "bind_session"):
        migration_state.bind_session(session)  # type: ignore[attr-defined]
    # Restore the migration type; older snapshots (or an unrecognized value)
    # default to Full-load-only.
    try:
        restored_type = (
            MigrationType(snapshot.migration_type)
            if snapshot.migration_type is not None
            else MigrationType.FULL_LOAD_ONLY
        )
    except ValueError:
        restored_type = MigrationType.FULL_LOAD_ONLY
    migration_state.migration_type = restored_type  # type: ignore[attr-defined]
    # Also set directly on the session in case no migration_state is bound.
    if hasattr(session, "set_migration_type"):
        session.set_migration_type(restored_type)  # type: ignore[attr-defined]

    # Restore the CDC operator choices (start mode + manual position, LOB
    # exclusions, tracked connector names). All optional -- older snapshots leave
    # them unset and default to Automatic.
    migration_state.set_cdc_start_position(  # type: ignore[attr-defined]
        gtid=snapshot.cdc_start_gtid,
        binlog_file=snapshot.cdc_start_binlog_file,
        binlog_pos=snapshot.cdc_start_binlog_pos,
    )
    migration_state.set_cdc_start_mode(  # type: ignore[attr-defined]
        snapshot.cdc_start_mode or "auto"
    )
    _restore_lob_exclusions(migration_state, snapshot.cdc_lob_exclusions)
    if snapshot.cdc_connector_names:
        migration_state.set_cdc_connector_names(snapshot.cdc_connector_names)  # type: ignore[attr-defined]

    # Restore the in-flight/just-finished CDC lifecycle job link so the CDC card
    # keeps showing that operation's ordered stages (and terminal message) after a
    # restart, instead of dropping them. The JobManager has already reconciled the
    # job itself (a RUNNING one became FAILED on restore); this only re-points the
    # UI at it. Guarded by hasattr so a session double without the setter is fine.
    if snapshot.cdc_deploy_job_id and hasattr(
        migration_state, "set_cdc_deploy_job_id"
    ):
        migration_state.set_cdc_deploy_job_id(  # type: ignore[attr-defined]
            snapshot.cdc_deploy_job_id, kind=snapshot.cdc_action_kind
        )

    # Restore the CDC infrastructure identity + inputs so the lifecycle card knows
    # which cdc-stack this session owns and the VpcId/subnet it deployed with. On
    # the next render (after the user re-verifies the target connection), the
    # read-only AWS probe recovers the live phase (Infra ready / Streaming).
    if snapshot.cdc_stack_name and hasattr(migration_state, "set_cdc_stack_name"):
        migration_state.set_cdc_stack_name(snapshot.cdc_stack_name)  # type: ignore[attr-defined]
    if snapshot.cdc_infra_inputs:
        migration_state.set_cdc_infra_inputs(dict(snapshot.cdc_infra_inputs))  # type: ignore[attr-defined]

    # Restore the non-secret target connection so the CDC card can re-probe the
    # live cdc-stack phase on reconnect (DSQL is IAM-token auth -- endpoint +
    # region suffice to describe). Source is not restored (it carries a secret);
    # the user re-tests connections on Connect, but the workbench resumes. The
    # unlock latch is restored too so already-entered steps stay navigable.
    if snapshot.target_endpoint and snapshot.target_region and hasattr(
        session, "set_target"
    ):
        from dsql_migrator.core.models import TargetConnectionConfig

        try:
            session.set_target(  # type: ignore[attr-defined]
                TargetConnectionConfig(
                    cluster_endpoint=snapshot.target_endpoint,
                    region=snapshot.target_region,
                    database=snapshot.target_database or "postgres",
                    username=snapshot.target_username or "admin",
                )
            )
        except Exception:  # noqa: BLE001 - a malformed persisted target is non-fatal
            pass
    if getattr(snapshot, "workflow_unlocked", False) and hasattr(
        session, "restore_workflow_unlock"
    ):
        session.restore_workflow_unlock(True)  # type: ignore[attr-defined]

    # Restore the AI Assist preference (toggle + Bedrock model/region) so a
    # reconnecting session keeps it on instead of resetting to off. Non-secret, so
    # it is safe to persist/restore (the credential comes from the AWS profile /
    # env chain at call time, never from here). The user still re-tests the source/
    # target connections on Connect, but the AI toggle should not flip back off.
    if getattr(snapshot, "ai_assist_enabled", False) and hasattr(
        session, "set_ai_assist"
    ):
        from dsql_migrator.ui.ai_assist import build_ai_assist_config

        session.set_ai_assist(  # type: ignore[attr-defined]
            build_ai_assist_config(
                enabled=True,
                model_id=snapshot.ai_assist_model_id,
                region=snapshot.ai_assist_region,
            )
        )


def session_is_fresh(
    session: object, eval_state: object, migration_state: object
) -> bool:
    """Return whether the in-memory session looks uninitialized (safe to restore).

    A restore must not clobber a live in-process session on a mere page reload.
    The session is "fresh" only when no step has started, no evaluation result
    exists, and no migration job is linked -- i.e. a brand-new process.
    """
    workflow = session.workflow  # type: ignore[attr-defined]
    any_started = any(
        getattr(workflow, step.value) != "NOT_STARTED" for step in WorkflowStep
    )
    return (
        not any_started
        and eval_state.result is None  # type: ignore[attr-defined]
        and migration_state.job_id is None  # type: ignore[attr-defined]
    )


def session_signature(
    session: object,
    eval_state: object,
    conv_state: object,
    migration_state: object,
    validation_state: object = None,
) -> tuple:
    """Return a cheap signature of the persistable linkage (no serialization).

    Changes only when the workflow status, evaluation-result presence/shape,
    generated objects, or migration linkage/selection change -- NOT when a
    background job's row counts advance (those are persisted by the JobStore).
    Used to skip redundant snapshot writes so a large inventory is not
    re-serialized on every poll.
    """
    workflow = session.workflow  # type: ignore[attr-defined]
    workflow_sig = tuple(getattr(workflow, step.value) for step in WorkflowStep)
    result = eval_state.result  # type: ignore[attr-defined]
    if result is None:
        eval_sig: tuple = (False,)
    else:
        eval_sig = (
            True,
            len(result.inventory.tables),
            len(result.inventory.views),
            sum(len(s.tables) for s in result.target_inventory.schemas),
        )
    generated = conv_state.generated_node_ids  # type: ignore[attr-defined]
    generated_sig = tuple(generated) if generated is not None else None
    selection = tuple(migration_state.selection.selected_tables)  # type: ignore[attr-defined]
    lob_sig = tuple(
        _flatten_lob_exclusions(migration_state.cdc_lob_exclusions())  # type: ignore[attr-defined]
    )
    target = getattr(session, "target_config", None)  # type: ignore[attr-defined]
    target_sig = (
        getattr(target, "cluster_endpoint", None),
        getattr(target, "region", None),
    )
    infra = migration_state.cdc_infra_inputs()  # type: ignore[attr-defined]
    infra_sig = tuple(sorted(infra.items()))
    return (
        workflow_sig,
        eval_sig,
        generated_sig,
        migration_state.job_id,  # type: ignore[attr-defined]
        selection,
        migration_state.selection_touched,  # type: ignore[attr-defined]
        migration_state.active_substep,  # type: ignore[attr-defined]
        migration_state.migration_type.value,  # type: ignore[attr-defined]
        migration_state.cdc_start_mode(),  # type: ignore[attr-defined]
        migration_state._cdc_start_gtid,  # type: ignore[attr-defined]
        migration_state._cdc_start_binlog_file,  # type: ignore[attr-defined]
        migration_state._cdc_start_binlog_pos,  # type: ignore[attr-defined]
        lob_sig,
        # Reconnect-relevant fields so a change to any of them triggers a save.
        getattr(session, "active_view", None),  # type: ignore[attr-defined]
        getattr(session, "workflow_unlocked", lambda: False)(),  # type: ignore[attr-defined]
        getattr(migration_state, "cdc_stack_name", None),  # type: ignore[attr-defined]
        infra_sig,
        target_sig,
        # CDC lifecycle-job link, so starting/finishing a CDC operation triggers a
        # save and the deploy-stage view survives a reconnect.
        getattr(migration_state, "cdc_deploy_job_id", None),  # type: ignore[attr-defined]
        getattr(migration_state, "cdc_action_kind", None),  # type: ignore[attr-defined]
        # AI Assist preference, so toggling it (or changing the model/region)
        # triggers a save and the choice survives a reconnect.
        bool(getattr(getattr(session, "ai_assist", None), "enabled", False)),  # type: ignore[attr-defined]
        getattr(getattr(session, "ai_assist", None), "model_id", None),  # type: ignore[attr-defined]
        getattr(getattr(session, "ai_assist", None), "region", None),  # type: ignore[attr-defined]
        # Validation result presence + completion time, so a finished validation
        # triggers a snapshot save (and a re-run that replaces it does too). Cheap:
        # a bool + timestamp, never the report itself.
        _validation_sig(validation_state),
    )


def _validation_sig(validation_state: object) -> tuple:
    """Cheap signature of the persistable validation result (presence + finish time)."""
    if validation_state is None:
        return (False,)
    report = getattr(validation_state, "result", None)
    if report is None:
        return (False,)
    completed = getattr(validation_state, "completed_at", None)
    return (True, completed.isoformat() if completed is not None else None)


__all__ = [
    "capture_session_snapshot",
    "apply_session_snapshot",
    "session_is_fresh",
    "session_signature",
]
