# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""CDC (Change Data Capture) UI for the Data Migration screen.

Extracted verbatim from ``data_migration/__init__.py`` -- a pure code move with
no behavior change. Every function and constant here previously lived in that
module and is re-exported from the package ``__init__`` (at the bottom of that
file), so ``dm.<name>`` attribute access and all existing monkeypatch targets keep
resolving exactly as before. This module owns the optional CDC data-plane UI:
source/start-point cards, infra + start/stop/delete dialogs and their control-plane
actions, the deploy/live-status panels, DLQ + pipeline-health monitoring, and the
shared live migration-table status grid. The Full Load screen, the activity-log
anchor (``_log_cdc_event``) and its connector-transition logger, and the
migration-type lock helper stay in ``__init__``.
"""

from __future__ import annotations

import logging

import inspect

from dataclasses import (
    dataclass,
)
from datetime import (
    datetime,
    timezone,
)
from typing import (
    Callable,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
)

from dsql_migrator.core.cdc import (
    CDC_TABLE_LIST_REMEDY,
    CDC_DEFAULT_DLQ_TOPIC,
    CDC_DEFAULT_STACK_NAME,
    CDC_DEFAULT_TOPIC_PREFIX,
    CDC_PLACEHOLDER_PREFIX,
    CDC_STACK_NAME_PREFIX,
    CdcPipelineOrchestrator,
    CdcResumePoint,
    MskSeedAdmission,
    build_cdc_stack_name,
    cdc_expected_connector_names,
    cdc_seed_watermark,
    mysql_snapshot_mode,
    start_point_check_due,
    composite_cdc_excluded_key_columns,
    split_column_exclude_value,
    cdc_stack_name_suffix,
    cdc_stack_params_to_json,
)
from dsql_migrator.core.cdc_coords import (
    gtid_mode_is_on,
    gtid_set_summary,
    normalize_gtid_set,
    parse_binlog_coordinate,
    validate_gtid,
)
from dsql_migrator.core.cdc_kafka_prep import offset_already_at_or_past
from dsql_migrator.core.job_manager import (
    JobNotFoundError,
    is_interrupted_by_restart,
)
from dsql_migrator.core.cdc import (
    cdc_teardown_estimate,
    cdc_teardown_reason,
    stack_has_seeder_lambda,
)
from dsql_migrator.core.cdc_postgres import (
    dispatch_cdc_infra_params,
    dispatch_cdc_stack_params,
    dispatch_source_config,
    pg_publication_autocreate_mode,
    pg_snapshot_mode,
)
from dsql_migrator.core.models import (
    LoadStatusView,
    MigrationMode,
    SourceInventory,
    SourceType,
    StepStatus,
    Watermark,
)
from dsql_migrator.core.table_selection import (
    TableSelector,
)
from dsql_migrator.ui.data_migration._models import (
    MigrationType,
    capture_load_complete,
    cdc_capture_names,
    classify_restored_exclusions,
    cutover_restore_block_reason,
    _DSQL_VALUE_LIMIT_MIB,
    assess_dlq_health,
    build_lag_chart_option,
    build_migration_table_status,
    cdc_handling_facts,
    cdc_prerequisite_block_header,
    cdc_prerequisite_block_reason,
    connector_health_rows,
    connector_role_label,
    cdc_excluded_columns,
    effective_lob_exclusions,
    missing_column_block_reason,
    capture_exclusion_mismatch,
    cdc_capture_exclusions,
    CAPTURE_SIZE_REMEDY_UNLOCKED,
    CDC_TABLE_LIST_HEADER,
    CDC_TABLE_LIST_WORST_CASE_HEADER,
    capture_ambiguity_warning,
    cdc_partition_map_worst_case,
    cdc_pre_load_table_list_routes,
    cdc_table_list_refusal,
    stored_exclusion_block_reason,
    stored_exclusion_issues,
    format_column_exclude_list,
    format_duration,
    lob_exclusion_candidates,
    per_table_counts_notice_body,
    SPLIT_READ_HEADER,
    mysql_reads_may_split,
    split_later_point_qualifier,
    split_read_refusal,
    split_read_tables,
    split_reload_route,
    split_route_view,
    table_list_hold_remedy,
)
from dsql_migrator.core.cdc_stack_deployer import is_stable_stack_status
from dsql_migrator.ui.data_migration._cdc_status import (
    _CDC_ACTION_NOUN,
    _CDC_ACTION_TERMINAL,
    _CDC_ACTION_TITLE,
    _CDC_DEPLOY_STAGE_STYLE,
    _CDC_STAGE_ETA_SECONDS,
    _CDC_STAGE_LABELS,
    _CDC_TONE_STYLE,
    _apply_cdc_status,
    _ascii_log,
    cdc_applied_rollup_detail,
    cdc_applied_totals,
    _cdc_status_view,
    _current_job,
    _deploy_total_duration,
    _fetch_cdc_status,
    _fetch_migration_row_counts,
    _format_eta_hint,
    _is_inflight_stack_status,
    cdc_attach_scope_mismatch,
    cdc_stack_has_streamed,
    cdc_start_rolled_back,
    cdc_started_before_load,
    drop_reload_condition_due,
    split_attachable_stacks,
    split_cleanup_by_progress,
    _migration_status_tables,
    _read_cdc_template_body,
    cdc_dlq_records,
    cdc_dlq_summary,
    cdc_error_log_key,
    is_cdc_error_record,
    should_replace_teardown_marker,
)
from dsql_migrator.ui.design import (
    EXPANSION_PANEL_CLASSES,
    NOTICE_STYLE,
    definition_row,
    inline_hint,
    notice_container,
    render_notice,
    section_header,
)

# These four names live in the package ``__init__``: the activity-log anchor
# ``_log_cdc_event`` (a monkeypatch target for the connector-transition logger that
# stays there), the ``migration_type_lock_reason`` helper, and the ``_LOGGER`` /
# ``_render_notice`` module constants. ``__init__`` imports THIS module for
# re-export, so the import below is only safe because ``__init__`` performs that
# re-export at the very bottom -- after these four names are already bound. The
# moved functions reference them as module globals, so they must be real imports
# here (a module-level ``__getattr__`` would not satisfy a function's LOAD_GLOBAL).
from dsql_migrator.core.activity_log import ActivityStatus
from dsql_migrator.ui.data_migration import (
    _log_cdc_event,
    migration_type_lock_reason,
    _LOGGER,
    _render_notice,
)

# The pure CDC state/phase predicates were extracted to _cdc_state.py so both this
# module and _cdc_monitoring.py can import them without a cycle. Re-exported here so
# _cdc_ui.<predicate> and every consumer/test import resolve unchanged.
from dsql_migrator.ui.data_migration._cdc_state import (  # noqa: E402,F401
    _CDC_POLL_INTERVAL_SECONDS,
    _cdc_is_streaming,
    cdc_infra_deploy_in_flight,
    cdc_monitoring_visible,
    cdc_delete_in_flight,
    cdc_evidence_unverified,
    cdc_pipeline_live,
    cdc_streaming_started,
    cdc_teardown_badge,
)


def _cdc_source_type(session) -> SourceType:
    """The session's source engine (default MySQL), for CDC config dispatch."""
    return getattr(
        getattr(session, "source_config", None), "source_type", SourceType.MYSQL
    )


def _cdc_source_database(session) -> str:
    """The session's source database name (PostgreSQL captures a single database)."""
    return getattr(getattr(session, "source_config", None), "database", "") or ""


def _session_source_credentials(session) -> Optional[tuple[str, str]]:
    """The session's own source username/password, for the teardown's slot drop.

    Property 7 is unchanged: these live in process memory only and are handed straight to a
    source connection -- never logged, never written to job state. They exist because the
    operator typed them to connect, which makes them the one credential path a teardown can
    rely on without an IAM grant. Reading the tool-managed CDC secret instead needs
    ``secretsmanager:GetSecretValue``, which the app's own identity does not have, and that
    is why a real teardown left a WAL-pinning replication slot behind.

    Returns ``None`` when there is no usable password (a Secrets-Manager-auth source, or a
    restored session whose password was not re-entered), so the caller falls back to the
    secrets path as before.
    """
    if session is None:
        return None
    config = getattr(session, "source_config", None)
    password = getattr(session, "source_password", None)
    if password is None:
        return None
    # ``session.source_password`` is a :class:`~dsql_migrator.config.SecretValue`, whose whole
    # point is that it does NOT behave like a str -- it exposes only ``reveal()`` so a
    # credential cannot be interpolated into a log line or an f-string by accident (Property
    # 7). Treating it as one here raised `'SecretValue' object has no attribute 'strip'` and
    # FAILED THE WHOLE TEARDOWN: "Delete infrastructure failed … you may need to delete the
    # stack from the CloudFormation console", leaving the cdc-stack up AND the replication
    # slot pinning source WAL -- the exact outcome this function was added to prevent.
    #
    # ``reveal()`` is also what the return value needs: ``run_cdc_delete`` hands the tuple to
    # ``build_pg_source_write_engine``, and it checks ``source_credentials[1].strip()`` before
    # using it, so returning the wrapper would only move the same AttributeError downstream.
    revealed = password.reveal() if hasattr(password, "reveal") else str(password)
    if not (revealed or "").strip():
        return None
    return (getattr(config, "username", "") or "", revealed)


def _cdc_stack_has_seeder_lambda(migration_state) -> Optional[bool]:
    """Does THIS session's cdc-stack contain the in-VPC offset-seeder Lambda?

    It is the only slow resource in a teardown (measured 18m30s of ENI reclamation, against
    MSK Serverless's 93s), so it is what decides the estimate the operator is shown. Read off
    the bound session's source engine rather than plumbed through every render layer:
    ``DataMigrationState`` already holds the session (``bind_session``), and a PostgreSQL
    stack is always created without the Lambda. Returns ``None`` when the engine cannot be
    determined, so the caller shows a range covering both cases instead of guessing.
    """
    from dsql_migrator.ui.data_migration._models import session_has_seeder_lambda

    return session_has_seeder_lambda(getattr(migration_state, "_session", None))


def _cdc_start_seed_mode(source_type: SourceType, host_seed_mode: str) -> str:
    """Engine-aware CDC-Start SeedMode -- ONE source of truth shared with the infra build.

    A PostgreSQL cdc-stack is ALWAYS created ``SeedMode=External`` (no in-VPC offset-seeder
    Lambda; it resumes from a logical replication slot, and ``build_pg_cdc_infra_params``
    hard-codes ``seed_mode="external"``). The External path does its Kafka prep -- creating
    the pinned offset / DLQ / per-table data topics + seeding the offset -- **in-process at
    Start** (``run_cdc_start``'s ``if seed_mode == "external"`` branch). So Start MUST also
    use ``"external"`` for a PostgreSQL source, derived from the SOURCE ENGINE and NOT from
    the host's ``cdc_seed_mode``: on a host whose ``cdc_seed_mode`` is the default (not
    external) that branch would be skipped, the topics never created, and PG CDC never
    streams. MySQL keeps deriving from the host config (the in-VPC EC2 host sets
    ``external``; Fargate/local -> ``lambda``), so MySQL behavior is unchanged.
    """
    if source_type is SourceType.POSTGRES:
        return "external"
    return host_seed_mode


def _cdc_resume_signal(migration_state, session):
    """Resolve the engine-appropriate CDC start signal for the config builders.

    Returns ``(resume_override, force_initial_snapshot)`` so every ``dispatch_source_config``
    call site stays consistent:

    - **MySQL** -- a Manual binlog ``file:position`` (with the GTID set executed there,
      which Debezium positions by on a ``gtid_mode=ON`` source) is what Start seeds into
      ``connect-offsets`` (:func:`cdc_seed_watermark`); pass it as ``resume_override``
      only when it can seed that offset (``can_seed_offset``). A GTID set alone cannot --
      both seeders key the offset on file+pos -- so it is not an override (and the card
      refuses it as a start point). ``force_initial_snapshot`` is always ``False`` (not a
      MySQL concept).
    - **PostgreSQL** -- Debezium resumes only from the replication slot and cannot start
      from an arbitrary WAL LSN, so Manual means "re-snapshot from scratch" (``initial``):
      there is no override value, just ``force_initial_snapshot = (mode == "manual")``.
      Automatic uses the gapless slot (``never``).
    """
    mode = migration_state.cdc_start_mode()
    if _cdc_source_type(session) is SourceType.POSTGRES:
        return None, mode == "manual"
    override = migration_state.cdc_start_override()
    usable = mode == "manual" and override is not None and override.can_seed_offset()
    return (override if usable else None), False


def _cdc_start_seed(migration_state, watermark, source_type):
    """The ``(seed, origin)`` a Start CDC offers the offset seeder. Pure (state reads only).

    MySQL: :func:`cdc_seed_watermark` over the Full Load job ``watermark`` and the Manual
    start point, so a Manual binlog file:position is really seeded. PostgreSQL keeps the job
    watermark exactly (it resumes from its slot; Manual is a re-snapshot there). Every
    surface that says where a Start begins -- the card, the Start button, the Start job, its
    activity line, the purge pre-flight -- reads this, so they cannot disagree.
    """
    if source_type is SourceType.POSTGRES:
        return watermark, "full_load"
    _fps = getattr(migration_state, "cdc_start_fingerprints", None)
    return cdc_seed_watermark(
        watermark,
        migration_state.cdc_start_override(),
        now=datetime.now(timezone.utc),
        # The Manual point's start-point fingerprints (recorded by Fetch current position
        # for exactly this point), so Start CDC can check the captured tables against it.
        override_fingerprints=_fps() if callable(_fps) else None,
    )


def _seed_can_seed(seed) -> bool:
    """Whether the seed Start offers can seed the offset (binlog file + position). Pure.

    The same test ``build_source_config`` applies to its resume point: the seed IS that
    point (the Manual override's coordinates, or the job watermark's).
    """
    return bool(getattr(seed, "binlog_file", None)) and getattr(
        seed, "binlog_position", None
    ) is not None


def mysql_start_facts(migration_state, seed, source_type=SourceType.MYSQL):
    """The cached :class:`~dsql_migrator.core.cdc.MysqlStartFacts` for a Start of ``seed``.

    Read off the phase probe's cache (committed offset, deployed watermark, live
    SnapshotMode) and the history-missing flag, so the connector-config preview and Start
    CDC decide ``snapshot.mode`` from the SAME inputs. ``None`` for PostgreSQL. Pure.
    """
    if source_type is SourceType.POSTGRES:
        return None
    from dsql_migrator.ui.data_migration._cdc_status import mysql_start_facts_from

    return mysql_start_facts_from(
        seed,
        has_committed_offset=bool(getattr(migration_state, "cdc_has_committed_offset", False)),
        deployed_watermark=getattr(migration_state, "cdc_deployed_watermark", None),
        live_mode=getattr(migration_state, "cdc_deployed_snapshot_mode", None),
        history_missing=bool(getattr(migration_state, "cdc_history_missing", False)),
    )


# The Start CDC worker's refusals (the GTID start check, G19/G20, and the start-point check)
# carry a notice of up to ~1,800 characters, and the JobManager keeps the first 300 of a job
# error, so every remedy was cut away. The job error is a short pointer; the full notice is
# kept on the migration state, tied to its job (it never outlives it, nor survives Start
# over, which clears the job id), and the CDC card renders it under the failed Start. The
# activity log gets the full notice too: on its own line, or on consecutive lines when it
# is longer than the log's 2,000-character line cap (the fix comes last, and a cut line
# would lose it -- G25). A restart loses the notice (process memory) and, on the default
# Fargate task, the activity log too (the task's own /tmp file; its CloudWatch mirror is off
# by default), while the job store and the session snapshot bring the failed Start and its
# pointer back. So the pointer names neither: every Start CDC and Retry CDC runs the same
# checks again, and the CDC step shows the cause and the fix then (G25).
_START_REFUSAL_NOTICE_ATTR = "cdc_start_refusal_notice"
START_REFUSAL_ACTION = "start blocked: cause and fix"


def _activity_lines(text: str) -> "list[str]":
    """``text`` as activity-log details that each fit the log's one-line cap: one line when
    it fits, else consecutive ``(i/N)`` parts split between words. Pure."""
    from dsql_migrator.core.activity_log import _MAX_DETAIL_CHARS

    words = text.split()
    if len(" ".join(words)) <= _MAX_DETAIL_CHARS:
        return [" ".join(words)]
    cap = _MAX_DETAIL_CHARS - len("(99/99) ")
    lines: list[str] = []
    for word in words:
        if lines and len(lines[-1]) + 1 + len(word) <= cap:
            lines[-1] += f" {word}"
        else:
            lines.append(word)
    return [f"({i}/{len(lines)}) {line}" for i, line in enumerate(lines, 1)]


def _start_refusal(
    migration_state, notice, job_id, *, first_line: Optional[str] = None
) -> RuntimeError:
    """The job error for a worker refusal, after recording its full ``notice``
    (``(tone, icon, header, body)``) for job ``job_id``'s CDC card and the activity log.
    Nothing was changed.

    Its FIRST line is the short pointer the job keeps: the JobManager and the activity log
    keep only a first line (300 / 2,000 characters). The full refusal follows on the next
    line for a caller that runs the worker in-process; no surface shows that part.
    ``first_line``: a refusal whose one-line form carries its route on its own (G25, A2),
    kept as that first line instead of the pointer -- under 300 characters with "Start CDC
    blocked: "."""
    if notice is None:
        return RuntimeError("Start CDC blocked. Nothing was changed.")
    tone, icon, header, body = notice
    setattr(migration_state, _START_REFUSAL_NOTICE_ATTR, (job_id, tone, icon, header, body))
    for line in _activity_lines(f"{header}. {body}"):
        _log_cdc_event(START_REFUSAL_ACTION, status=ActivityStatus.FAILURE, detail=line)
    first = (
        f"Start CDC blocked: {first_line}"
        if first_line
        else f"Start CDC blocked: {header}. Nothing was changed; the CDC step shows the cause "
        "and the fix whenever Start CDC (or Retry CDC) runs its checks."
    )
    return RuntimeError(f"{first}\n{header}. {body}")


def _start_refusal_notice(migration_state, job_id) -> "Optional[tuple]":
    """The refusal notice recorded for ``job_id`` (``(tone, icon, header, body)``), or None."""
    recorded = getattr(migration_state, _START_REFUSAL_NOTICE_ATTR, None)
    if not recorded or job_id is None or recorded[0] != job_id:
        return None
    return tuple(recorded[1:])


def _logged_cdc_lifecycle(action: str, *, detail: str, work, residue=None):
    """Wrap a CDC lifecycle job body so its OUTCOME reaches the activity log.

    The four lifecycle actions (deploy infra / start / stop / delete) each take
    minutes to tens of minutes, and the ``_log_cdc_event`` at their submit site only
    records that they were STARTED. Nothing logged their result, so the audit trail
    could not answer "did the Stop before cut-over actually succeed, and when?" --
    the question that matters most at the moment an operator is deciding to cut over.
    Connector-state transitions were the only proxy, and those are written by the UI
    POLLER, so they are missed entirely whenever the operator navigates away while a
    20-30 minute action runs.

    Wrapping the job body fixes both: this runs on the background job thread, so the
    outcome is recorded regardless of what the UI is showing. The wrapped ``work``
    keeps its own signature/behavior, and the ``core`` deployer functions are
    untouched -- ``core`` deliberately has no activity-log dependency, so the logging
    stays in this UI layer (mirroring how Full Load logs from ``_engine``).

    Records SUCCESS with the elapsed time, FAILURE with the elapsed time + the error
    (re-raising so the JobManager still marks the job FAILED), or INFO for a
    cooperative cancel -- ``run_cdc_*`` returns normally when cancelled, so the
    handle is what distinguishes "stopped early" from "finished".

    Known gap: if the PROCESS dies mid-action nothing runs here, so that job is left
    with only its STARTED line (the JobManager reconciles it to FAILED on restart,
    but does not log an activity event).
    """
    import time as _time

    def wrapped(handle) -> None:
        started = _time.monotonic()

        def _elapsed() -> str:
            return format_duration(max(0.0, _time.monotonic() - started))

        try:
            work(handle)
        except Exception as exc:  # noqa: BLE001 - log the outcome, then re-raise
            _log_cdc_event(
                action,
                status=ActivityStatus.FAILURE,
                detail=(
                    f"{detail} — failed after {_elapsed()}: "
                    f"{type(exc).__name__}: {exc}"
                ),
                exc=exc,
            )
            raise
        if bool(getattr(handle, "cancelled", False)):
            _log_cdc_event(
                action,
                status=ActivityStatus.INFO,
                detail=f"{detail} — cancelled after {_elapsed()}",
            )
            return
        # A teardown that finished its own work can still have LEFT SOMETHING on the
        # customer's source -- a logical replication slot that keeps pinning WAL until
        # the source disk fills. That cleanup is deliberately non-fatal (the stack is
        # already gone, so it must not fail the job), which meant the durable audit
        # line read "delete CDC infrastructure (success)" while the hazard survived and
        # its only record sat in the in-memory deploy log the next action wipes.
        # WARNING, not FAILURE: the action did complete; something needs doing next.
        _left = tuple(residue() or ()) if residue is not None else ()
        if _left:
            _log_cdc_event(
                action,
                status=ActivityStatus.WARNING,
                detail=(
                    f"{detail} — completed in {_elapsed()}, but LEFT BEHIND on the "
                    f"source: {'; '.join(_left)}"
                ),
            )
            return
        _log_cdc_event(
            action,
            status=ActivityStatus.SUCCESS,
            detail=f"{detail} — completed in {_elapsed()}",
        )

    return wrapped


def _render_cdc_step(
    ui,
    migration_state,
    job_manager,
    refresh,
    *,
    inventory: Optional[SourceInventory] = None,
    migration_type: "MigrationType" = MigrationType.CDC_ONLY,
    run_checks=None,
    session: object = None,
    cdc_ai_opener=None,
    ai_post_event=None,
    lob_candidates_for=None,
    exclude_columns=None,
) -> None:
    """Render the CDC step in user-journey order: decide -> prepare -> start ->
    monitor -> reference.

    The order follows how an operator actually thinks about CDC, not the code's
    convenience: (1) where it runs, (2) DECIDE the start point, (3) tune what is
    captured (oversized LOBs), (4) the explicit "start streaming" next action,
    (5) live monitoring once running (Pipeline health + per-table status + DLQ),
    (6) collapsed reference (CDC behavior & limits). Data parity and change-flow
    idle -- the cutover signals -- are shown by those panels and the Validation
    step, so there is no separate cutover panel.
    ``run_checks`` is accepted for signature parity; the prerequisites step owns it.
    """
    # 1. Where CDC runs (orientation banner) -- only BEFORE the cdc-stack is
    #    deployed; once it exists the flow is self-evident, so hide it to cut noise.
    _render_cdc_runs_on_banner(
        ui, phase=getattr(migration_state, "cdc_stack_phase", None)
    )

    # NOTE: provisioning is NOT rendered here. The lifecycle card in step 4
    # (_render_cdc_start_action) already owns it: on an absent stack it renders the
    # same BYO-VPC deploy form (or the adopt choice), so adding a second call here
    # showed the identical form twice on one screen. The Prerequisites copy is the
    # *extra* entry point -- offered there only so the ~5 min MSK create can
    # overlap a Full Load -- and it is suppressed for CDC only, which has no Full Load
    # to overlap. See _render_cdc_infra_prep_section.

    # 2. DECIDE: the CDC start point (Automatic/Manual). This is the central
    #    decision, so it comes first -- the source/sink connector config it
    #    produces is rendered (collapsed) inside this card.
    _render_cdc_source_config_card(
        ui,
        migration_state,
        job_manager,
        refresh,
        inventory=inventory,
        migration_type=migration_type,
        session=session,
    )

    # 3. PREPARE: the column exclusions feed column.exclude.list. They are edited in
    #    "Exclude tables & columns" under the table list (one home for every type); the
    #    CDC step only says what CDC receives.
    _render_cdc_exclusion_summary(ui, migration_state, job_manager, inventory)

    # 4. START: actually deploy the connectors (cloudformation update_stack) and
    #    show step-by-step progress.
    _render_cdc_start_action(
        ui, migration_state, job_manager, refresh,
        inventory=inventory, session=session,
    )

    # 5. MONITOR: live connector health + DLQ, meaningful only once streaming.
    # session is threaded through so the drift banner can offer the opt-in
    # ADD COLUMN fix (it needs the source + target connections).
    _render_cdc_live_monitoring(
        ui, migration_state, job_manager, session=session,
        cdc_ai_opener=cdc_ai_opener, ai_post_event=ai_post_event,
        # The in-app recovery for rows CDC dropped over DSQL's per-value limit. Threaded from
        # the caller because the candidate lookup needs the INVENTORY and the exclusion writes
        # migration-wide state; the panel itself has neither.
        lob_candidates_for=lob_candidates_for,
        exclude_columns=exclude_columns,
    )
    # 5a. The dead-letter "Exclude column..." recovery once monitoring is hidden (after
    #     Stop / Delete) -- the only state in which its action is allowed.
    _render_cdc_oversized_recovery_without_monitoring(
        ui, migration_state, job_manager,
        lob_candidates_for=lob_candidates_for,
        exclude_columns=exclude_columns,
        session=session,
    )

    # 5b. PER-TABLE: Full Load outcome + live source/target row counts per selected
    #     table, so the operator can see Full Load completion and CDC replication
    #     converge table by table (MSK has no per-table metric; this counts directly).
    _render_migration_table_status(
        ui, migration_state, job_manager, session, inventory=inventory
    )

    # 6. REFERENCE: what CDC handles vs. what to watch (collapsed, educational).
    #    (A separate "Cutover readiness" panel was removed: its signals are already
    #    shown -- data parity in the per-table Consistency column / Validation step,
    #    and change-flow idle in the Pipeline health card -- so it only duplicated.)
    _render_cdc_handling_panel(ui)


def _render_cdc_source_config_card(
    ui,
    migration_state,
    job_manager,
    refresh,
    *,
    inventory: Optional[SourceInventory] = None,
    migration_type: "MigrationType" = MigrationType.CDC_ONLY,
    session: object = None,
) -> None:
    """Render the Debezium source config, seeded from the watermark OR a manual
    start position.

    Builds :class:`~dsql_migrator.core.cdc.DebeziumSourceConfig` via
    :class:`CdcPipelineOrchestrator` (no AWS calls). The start offset is taken
    from the Full Load watermark when present (gapless -- Property 11), otherwise
    from a manual GTID / binlog file:position the operator enters here. The
    user's opt-in oversized-LOB exclusions flow into ``column.exclude.list``.
    Always shows the manual-entry form so a custom offset can override or stand
    in for a missing watermark; ``refresh`` re-renders the config preview when an
    override is applied.
    """
    job = _current_job(job_manager, migration_state.job_id)
    watermark = _cdc_watermark(job, migration_state)
    source_type = _cdc_source_type(session)
    is_pg = source_type is SourceType.POSTGRES
    override = migration_state.cdc_start_override()
    mode = migration_state.cdc_start_mode()
    # The watermark resume (gapless) is the Automatic option's source.
    wm_resume = (
        CdcResumePoint.from_watermark(watermark) if watermark is not None else None
    )
    if is_pg:
        # PostgreSQL Automatic = gapless resume from the logical replication slot created
        # at the Full Load consistency point. BOTH the WAL LSN and the recorded SLOT NAME
        # are required, and the slot is the load-bearing half: only a Full-Load-+-CDC run
        # writes it, and it is the tool's only record that a slot exists AT that LSN. The
        # comment here already CLAIMED "the WAL LSN + slot name" while the predicate tested
        # the LSN alone -- so a Full-load-only watermark rendered "Automatic -- gapless
        # from the replication slot (recommended)", a positive Ready badge and a green
        # "Start point set" line, promising gaplessness with no slot in existence. Same bug
        # shape as the MySQL GTID-only post-mortem 11 lines below. A slot cannot be created
        # at a past LSN, so without the record the only honest option is Manual.
        # Manual = re-snapshot from scratch (snapshot.mode=initial), which needs no
        # coordinate and is ALWAYS a resolvable start point -- Debezium PG resumes only from
        # the slot and cannot start from an arbitrary WAL LSN, so there is no binlog offset
        # to seed and the MySQL can_seed_offset()/gtid-only tests do not apply.
        wm_usable = (
            wm_resume is not None
            and wm_resume.can_resume_from_lsn()
            and bool(getattr(watermark, "slot_name", None))
        )
        wm_gtid_only = False
        effective_resume: Optional[CdcResumePoint] = (
            wm_resume if (mode == "auto" and wm_usable) else None
        )
    else:
        # Gate on what the SEEDER actually needs (binlog file:pos), not on the broad
        # "has any coordinate" test. A GTID-only watermark passed has_coordinates(), so the
        # card claimed "Automatic -- gapless from Full Load (recommended)" and showed Ready --
        # while build_watermark_params returned all-empty values, the template skipped the
        # seeder, and the connector started from the CURRENT binlog, losing every change made
        # during the load. The failure was silent until Validation, or after cut over.
        #
        # Reachable, not theoretical: the coordinates come from SEPARATE queries that degrade
        # independently -- SHOW MASTER STATUS needs REPLICATION CLIENT (commonly restricted on
        # RDS/Aurora) while @@GLOBAL.gtid_executed is a plain global read.
        wm_usable = wm_resume is not None and wm_resume.can_seed_offset()
        # A GTID set with NO file:pos: there IS a watermark and it looks usable, but it cannot
        # seed the offset. Called out separately so the reason is explicit instead of the
        # generic "no watermark" wording, which would be wrong here.
        wm_gtid_only = (
            wm_resume is not None
            and not wm_resume.can_seed_offset()
            and bool(wm_resume.gtid_executed)
        )
        # Effective start position: in manual mode an entered override wins; in auto
        # mode the watermark is used. A Manual point must be SEEDABLE (a binlog
        # file:position): Start seeds exactly that (cdc_seed_watermark), and a GTID set
        # alone cannot set where the pipeline starts -- so it is not a start point.
        if mode == "manual" and override is not None and override.can_seed_offset():
            effective_resume = override
        elif mode == "auto" and wm_usable:
            effective_resume = wm_resume
        else:
            effective_resume = None

    # PRIMARY card: the CDC start point. This is the central decision of the CDC
    # step, so it is a top-level card with an explicit Automatic/Manual choice --
    # not buried inside the raw connector config (which is now a collapsed
    # "advanced" expansion below).
    # Once CDC is streaming (or a lifecycle job is in flight) the start point is
    # already seeded into connect-offsets and cannot change the live pipeline, so
    # the radio + manual inputs are locked (read-only) to avoid misleading edits.
    started = cdc_streaming_started(migration_state, job_manager)
    # What Start will offer the seeder (a Manual binlog point beats the job watermark).
    seed, _seed_origin = _cdc_start_seed(migration_state, watermark, source_type)
    # A stopped-but-previously-streamed pipeline resumes from its own committed offset, so
    # no start point needs choosing. Without this the card contradicted the
    # button beneath it: "Action needed" / "needs a Full Load watermark" while Start CDC
    # was enabled and would have worked.
    resumes_from_offset = cdc_start_resumes(migration_state, seed, source_type)
    _render_cdc_start_point_card(
        ui,
        migration_state,
        refresh,
        wm_resume=wm_resume,
        wm_usable=wm_usable,
        effective_resume=effective_resume,
        mode=mode,
        locked=started,
        session=session,
        source_type=source_type,
        wm_gtid_only=wm_gtid_only,
        resumes_from_offset=resumes_from_offset,
        # G25 (A2): the start point Start CDC would refuse on a stack that has streamed --
        # this one, and the Automatic one (the Full Load watermark).
        newer=newer_start_point_refusal(
            migration_state, seed, source_type, origin=_seed_origin, job_watermark=watermark
        ),
        auto_refused=newer_start_point_refusal(migration_state, watermark, source_type)
        is not None,
        # The CAUSE of a re-snapshot, derived from the RAW job (the card only sees the
        # watermark, whose slot has already been suppressed for an unfinished load, so it
        # cannot tell "never provisioned" from "not honoured yet" on its own).
        resnapshot_reason=pg_resnapshot_reason(
            job, source_type=source_type, start_mode=mode, migration_state=migration_state
        ),
        # Evaluated when Fetch current position is clicked: the tables a Start captures
        # then, whose definitions are recorded with the fetched position.
        captured_names=lambda: [
            t.name for t in _cdc_tables_for_config(migration_state, inventory, watermark)
        ],
        pg_slot=_pg_start_slot(migration_state, watermark) if is_pg else None,
        # G27: this Full Load is not the migration's first copy, so Automatic is gapless only
        # if it did not append to rows already on the target -- and whether a Start CDC from
        # an earlier start point preceded it, which its notice then says first.
        recopied=drop_reload_condition_due(migration_state, job),
        recopied_after_start=cdc_started_before_load(migration_state, job),
        split_refusal=split_start_point_refusal(
            migration_state, job, seed, source_type, origin=_seed_origin,
            resumes=resumes_from_offset, job_manager=job_manager,
        ),
        split_waiting=bool(_captured_split_tables(migration_state, job)),
    )

    # On a restart the connector config below is not what decides the start position (the
    # committed offset is), so an absent effective_resume must not hide the rest of the
    # card -- it is exactly the state a resume renders in. A PostgreSQL re-snapshot is
    # likewise a resolvable start with no coordinate, so it must not early-return either --
    # and that holds for AUTOMATIC as much as for Manual. Testing Manual alone hid the
    # connector-config preview on the one route whose config is the surprise: the params
    # file inside it prints `PgSnapshotMode: initial` / `PgPublicationAutocreateMode:
    # filtered`, which is the re-snapshot in the tool's own machine-readable words. MySQL
    # with a usable watermark has an effective_resume and so always showed it; this closes
    # a gap that made the PG path look like a different app.
    if (
        effective_resume is None
        and not resumes_from_offset
        and not (is_pg and (mode == "manual" or not wm_usable))
    ):
        return

    # Build the connector config (pure -- no AWS calls). Restrict the table list
    # to what the watermark covered when inventory + watermark exist; otherwise
    # fall back to the user's confirmed selection (manual seed).
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    # ONE computation for the preview, Start and Deploy (cdc_capture_exclusions): the
    # EFFECTIVE set (key columns dropped) plus the STORED generated columns of the captured
    # tables -- DSQL rejects a write to one (428C9) and the sink dead-letters it.
    _capture = cdc_capture_exclusions(migration_state, inventory, tables_for_config)
    # The preview shows what Start CDC will SEND (a stack on the 0.1.561 form keeps it).
    exclude_list = split_column_exclude_value(_capture.sent) or None
    _resume_override, _force_initial_snapshot = _cdc_resume_signal(migration_state, session)

    # A CDC sink needs at least one table: with none selected the sink topic list
    # would be empty and the deploy would later fail at connector create (opaque
    # HTTP 400). Surface that here as a calm, actionable notice instead of building
    # a config preview from an empty selection (build_sink_config would raise).
    if not tables_for_config:
        # G17: only tables still to load under the Full Load's start point are selected --
        # the remedy is their load (or an untick). Said once, here: the Start card below
        # stays silent on an empty capture (its Will stream line and disabled Start say it).
        _pending_cfg = (
            cdc_start_uncovered(migration_state, job_manager, job)
            if cdc_capture_names(migration_state, watermark) == frozenset()
            else []
        )
        if _pending_cfg:
            render_notice(
                ui,
                tone="warning",
                header=START_UNCOVERED_HEADER,
                body=start_uncovered_body(
                    _pending_cfg, getattr(migration_state, "migration_type", None)
                ),
            )
            return
        render_notice(
            ui,
            tone="warning",
            header="Select at least one table before starting CDC",
            body=(
                # G16: after a Full Load only its snapshot's tables can be captured -- the
                # remedy is the table list, not Schema Conversion.
                "None of the tables the Full Load loaded is selected, so there is nothing "
                "for the CDC sink to write. Tick at least one of them in the table list, "
                "then return here to start CDC."
                if cdc_capture_names(migration_state, watermark) == frozenset()
                else "No tables are selected for replication yet, so there is nothing "
                "for the CDC sink to write. Choose the tables to migrate (Schema "
                "Conversion / the table picker), then return here to start CDC."
            ),
        )
        return

    # MySQL: built from the SEED, the same one Start sends (see _start_cdc_deploy), so
    # the preview cannot claim a start point the Start would not seed -- and from the same
    # stack facts, so it prints the snapshot.mode Start sends (mysql_start_facts).
    _config_wm = watermark if is_pg else seed
    config = dispatch_source_config(
        _cdc_source_type(session),
        tables_for_config,
        _config_wm if _config_wm is not None else _sentinel_watermark(),
        database=_cdc_source_database(session),
        stack_name=getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME),
        column_exclude_list=exclude_list,
        resume_override=_resume_override,
        force_initial_snapshot=_force_initial_snapshot,
        start_facts=mysql_start_facts(migration_state, seed, source_type),
    )

    # The sink config (topics, keying, DLQ). DLQ name is the cdc-stack default --
    # NOT derived from a connector that may not exist yet (pre-deploy). Engine-neutral
    # (the DSQL sink is the same for both source engines); the deployed connector is
    # named ``<stack>-dsql-sink``, so the preview uses "dsql-sink" (not "mysql-sink",
    # which was wrong for BOTH engines and misleading on a PostgreSQL source).
    sink = CdcPipelineOrchestrator().build_sink_config(
        "dsql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC
    )

    # Build the deployable cdc-stack parameter set: tool-known values filled,
    # customer-environment values as <FILL_ME> placeholders.
    target = getattr(session, "target_config", None)
    params = dispatch_cdc_stack_params(
        config,
        sink,
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME),
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        # Same value the deploy would send, so this preview cannot show a
        # SinkMcuCount the actual Start CDC then contradicts.
        sink_mcu_count=_sink_mcu_count(),
    )

    # SECONDARY (collapsed): connector configuration to hand to the cdc-stack.
    # Human-readable summary + the actual deployable parameter file. Advanced
    # output, so progressive disclosure keeps the start-point decision primary.
    source_lines = [
        f"snapshot.mode = {config.snapshot_mode}",
        f"table.include.list = {', '.join(config.table_include_list) or '(all selected)'}",
    ]
    # MySQL start point (GTID / binlog file+pos) -- a PostgreSQL source config has
    # neither attribute, so read them defensively (accessing config.start_gtid on a
    # PostgresSourceConfig raised AttributeError and crashed the CDC step render).
    start_gtid = getattr(config, "start_gtid", None)
    if start_gtid:
        # Debezium positions by this set only on a gtid_mode=ON source (file:pos otherwise).
        source_lines.append(f"gtid (start, used when gtid_mode=ON) = {start_gtid}")
    start_binlog_file = getattr(config, "start_binlog_file", None)
    if start_binlog_file:
        source_lines.append(
            f"binlog (start) = {start_binlog_file}:{getattr(config, 'start_binlog_pos', '')}"
        )
    # PostgreSQL start point: the logical-replication slot + publication (Debezium
    # pgoutput resumes from the slot's LSN); shown only when present (PG source).
    slot_name = getattr(config, "slot_name", None)
    if slot_name:
        source_lines.append(f"slot.name = {slot_name}")
    publication_name = getattr(config, "publication_name", None)
    if publication_name:
        source_lines.append(f"publication.name = {publication_name}")
    if config.column_exclude_list:
        source_lines.append(
            f"column.exclude.list = {', '.join(config.column_exclude_list)}"
        )
    sink_lines = [
        f"name = {sink.name}",
        f"topics (prefixed) = {dict(params.filled).get('SinkTopics') or '(all selected)'}",
        f"pk.mode = {sink.pk_mode}",
        f"insert.mode = {sink.insert_mode}",
        f"delete.enabled = {str(sink.delete_enabled).lower()}",
        f"errors.deadletterqueue.topic.name = {sink.dlq_topic}",
    ]
    with ui.expansion(  # type: ignore[attr-defined]
        "Connector configuration (hand to cdc-stack)", icon="settings"
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
        ui.label("Debezium source connector").classes(  # type: ignore[attr-defined]
            "text-xs font-semibold text-gray-600"
        )
        ui.code("\n".join(source_lines)).classes("w-full text-xs")  # type: ignore[attr-defined]
        ui.label("Custom DSQL sink connector").classes(  # type: ignore[attr-defined]
            "text-xs font-semibold text-gray-600 mt-2"
        )
        ui.code("\n".join(sink_lines)).classes("w-full text-xs")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Poison rows are quarantined to the dead-letter queue, not dropped."
        ).classes("text-xs text-gray-500")
        _render_cdc_params_file(ui, params)

def _render_cdc_params_file(ui, params) -> None:
    """Render the deployable cdc-stack parameter file (JSON) + a copy button.

    Tool-known values are filled; customer-environment values carry
    ``<FILL_ME:`` placeholders that MUST be replaced before deploying. A bold
    warning makes that unmistakable. The CDC start position is intentionally not
    a parameter (it is seeded via the connect-offsets topic), so a note explains
    that separately.
    """
    params_json = cdc_stack_params_to_json(params)
    n_placeholder = len(params.placeholders)
    with ui.expansion(  # type: ignore[attr-defined]
        "cdc-stack parameter file (JSON)", icon="description"
    ).classes(f"w-full mt-2 {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
        inline_hint(  # type: ignore[attr-defined]
            ui,
            f"Replace every value starting with {CDC_PLACEHOLDER_PREFIX} "
            f"({n_placeholder} customer-environment value(s)) before deploying — "
            "CloudFormation will reject or fail on an unfilled placeholder.",
            tone="warning",
            classes="text-xs font-semibold",
        )
        ui.code(params_json, language="json").classes("w-full text-xs")  # type: ignore[attr-defined]

        def _copy() -> None:
            try:
                ui.clipboard.write(params_json)  # type: ignore[attr-defined]
                ui.notify("Parameter file copied.", type="positive", position="top")  # type: ignore[attr-defined]
            except Exception:  # noqa: BLE001 - clipboard may be unavailable
                ui.notify(  # type: ignore[attr-defined]
                    "Copy the parameter file from the block above.", type="info",
                    position="top",
                )

        ui.button("Copy parameter file", on_click=_copy).props(  # type: ignore[attr-defined]
            "size=sm outline icon=content_copy"
        )
        ui.label(  # type: ignore[attr-defined]
            "Note: the CDC start position (the source's replication coordinate — "
            "MySQL binlog/GTID or PostgreSQL WAL LSN) is NOT a parameter here — it is "
            "seeded via the connect-offsets topic after deploy. See the start point "
            "shown above."
        ).classes("text-xs text-gray-500")

def _sentinel_watermark() -> "Watermark":
    """A placeholder watermark for the manual-override path (coords unused).

    ``build_source_config`` requires a ``Watermark`` argument, but when a manual
    ``resume_override`` is supplied its coordinates are ignored. Pass a minimal
    valid watermark stamped with the current time so the type contract holds.
    """
    return Watermark(snapshot_timestamp=datetime.now(timezone.utc))

def _render_cdc_exclusion_summary(ui, migration_state, job_manager, inventory) -> None:
    """The CDC step's read-only line: how much is excluded, and the ColumnExcludeList CDC
    receives. While the connectors run, or when Start CDC would refuse the session's set
    for a stack that streamed (``capture_exclusion_mismatch``), that is the DEPLOYED value
    -- the session's set is shown beside it, with the refusal. Otherwise it is what Start
    CDC sends (preview == sent; the chooser already keeps a deployed value it must keep),
    labelled "as deployed" when the stack carries exactly that. Nothing is drawn when
    nothing is excluded or captured."""
    from dsql_migrator.core.cdc import CDC_PARAMETER_BYTE_LIMIT
    from dsql_migrator.ui.design import CODE_SURFACE_CLASSES, inline_hint

    if inventory is None:
        return
    stored = effective_lob_exclusions(migration_state, inventory)
    columns = sum(len(c) for c in stored.values())
    getter = getattr(migration_state, "exclusion_inputs", None)
    inputs = getter() if callable(getter) else None
    tables = migration_state.rule_excluded_tables()
    if inputs is not None:
        tables = tables & set(inputs.migratable)
    deployed = getattr(migration_state, "cdc_deployed_column_exclude_list", None)
    captured = _cdc_tables_for_config(
        migration_state, inventory,
        _cdc_watermark(
            _current_job(job_manager, getattr(migration_state, "job_id", None)),
            migration_state,
        ),
    )
    capture = cdc_capture_exclusions(migration_state, inventory, captured)
    session_value = capture.sent
    mismatch = capture_exclusion_mismatch(migration_state, inventory, capture, captured)
    live = getattr(migration_state, "cdc_stack_phase", None) in ("running", "partial")
    if deployed is not None and (mismatch or live or session_value == deployed):
        value, origin = deployed, "as deployed"
    else:
        value, origin = session_value, "Start CDC sends"
    if not columns and not tables and not value and not deployed:
        return
    parts = []
    if tables:
        parts.append(f"{len(tables)} table{'s' if len(tables) != 1 else ''}")
    parts.append(f"{columns} column{'s' if columns != 1 else ''}")
    inline_hint(ui, f"Excluded: {', '.join(parts)} (change them in Exclude tables & columns "
                "above the steps).")
    size = len((value or "").encode("utf-8"))

    def _toggle(event) -> None:
        migration_state.cdc_receives_open = bool(getattr(event, "value", False))

    with ui.expansion(  # type: ignore[attr-defined]
        f"What CDC receives ({size:,} of {CDC_PARAMETER_BYTE_LIMIT:,} bytes; {origin})",
        value=bool(getattr(migration_state, "cdc_receives_open", False)),
        on_value_change=_toggle,
    ).classes("w-full text-xs"):
        with ui.column().classes(f"w-full gap-0 p-2 {CODE_SURFACE_CLASSES}"):  # type: ignore[attr-defined]
            ui.label(value or "(nothing excluded at capture)").classes(  # type: ignore[attr-defined]
                "text-xs font-mono wrap-anywhere")
        if mismatch:
            inline_hint(ui, f"This session would exclude {session_value or '(nothing)'}; "
                        f"Start CDC refuses to send it. {mismatch}", tone="warning")
        elif deployed is not None and session_value != deployed:
            if origin == "as deployed":
                inline_hint(ui, "The next Start CDC sends: "
                            f"{session_value or '(nothing excluded at capture)'}.")
            else:
                inline_hint(ui, f"As deployed: {deployed or '(nothing excluded at capture)'}. "
                            "Start CDC sends the value above.")


def _cdc_tables_for_config(
    migration_state, inventory: Optional[SourceInventory], watermark
) -> list:
    """Resolve which tables the CDC source config should include.

    THE capture set of every CDC surface (infra deploy params and partition plan, Start
    CDC, its dialog's checks, the PostgreSQL objects gate, the FK probe, "Will stream", the
    config preview, the monitor's streamed set). Prefers the watermark's covered tables
    still selected (:func:`cdc_capture_names`: the snapshot's set plus the tables a guarded
    Full load + CDC load added since -- G17 --, narrowed by a Full load + CDC untick after
    its Full Load -- G16 -- and never widened by the selection alone, since only covered
    tables have a gapless start) when both inventory and watermark are present, in inventory
    order; otherwise falls back to the user's confirmed table selection (the manual-seed
    case where no Full Load ran); and finally, for an ADOPTED / out-of-band pipeline (this
    session ran neither Full Load nor Start CDC -- e.g. after a reset + "Attach to
    <stack>"), to the table set reconciled from the live stack's ``TableIncludeList``.
    Returns an empty list only when none are available (config shows "all selected") --
    or when every covered table was unticked (nothing is captured then).
    """
    if inventory is None:
        return []
    # Never a table whose stored name another table or view shares: CDC would write both
    # into one relation (a session whose Full Load ran on an earlier version may still
    # have such a pair covered by its watermark and pinned in its selection).
    from dsql_migrator.core.converter import relation_clash_names

    clash = relation_clash_names(inventory)
    names = cdc_capture_names(migration_state, watermark)
    if names is not None:
        return [t for t in inventory.tables if t.name in names and t.name not in clash]
    selection = migration_state.selection
    if selection is not None and selection.selected_tables:
        return [
            t for t in TableSelector().resolve(inventory, selection) if t.name not in clash
        ]
    # Adopted / out-of-band pipeline: the session has no watermark or selection,
    # but the live stack's TableIncludeList tells us which tables are replicating.
    # Each include entry is a table's ``.name`` (build_source_config uses
    # ``[table.name for table in tables]``), so match on ``.name`` -- identical to
    # the watermark path above.
    reconciled = set(getattr(migration_state, "cdc_reconciled_table_names", []) or [])
    if reconciled:
        return [t for t in inventory.tables if t.name in reconciled and t.name not in clash]
    return []


def _cdc_row_counts_from_watermark(watermark, tables_for_config):
    """Per-table row estimates from a Full Load watermark, scoped to the capture.

    Returns ``{table_name: rows}`` (positive counts only) for the tables actually
    being captured, or ``None`` when the watermark carries no counts. These are the
    scan-free ``information_schema`` estimates captured at snapshot time -- exactly
    the RELATIVE size signal the partition planner needs -- plus, for a table a guarded
    Full load + CDC load added after it (G17), the rows that load wrote. Pure.
    """
    counts = getattr(watermark, "table_row_counts", None) if watermark else None
    if not counts:
        return None
    names = {t.name for t in tables_for_config}
    scoped = {n: int(c) for n, c in counts.items() if n in names and c}
    for name, rows in (getattr(watermark, "cdc_added_tables", None) or {}).items():
        if name in names and rows and name not in scoped:
            scoped[name] = int(rows)
    return scoped or None


def _estimate_cdc_table_rows(session, table_names):
    """BLOCKING, read-only scan-free per-table row estimates from the source.

    Runs on a worker thread (caller uses ``run.io_bound``). Used to size CDC topic
    partitions proportionally to table size when no Full Load watermark is present
    (e.g. CDC infra deployed early). Best-effort: returns ``None`` when the source
    cannot be read (no connection/password after a restore) or on any error, so the
    deploy simply falls back to the uniform partition default. Mirrors the
    information_schema estimate the migration-status view already uses.
    """
    if not table_names:
        return None
    try:
        from dsql_migrator.core.watermark import estimate_source_rows
        from dsql_migrator.ui.connect import make_source_engine_factory

        from dsql_migrator.core.source_dialect import dialect_for

        source_config = getattr(session, "source_config", None)
        has_password = getattr(session, "source_password", None) is not None
        if source_config is None or not session.has_source() or not has_password:
            return None
        # Source-engine dialect (PG uses pg_class.reltuples, not MySQL information_schema)
        # so a PostgreSQL source's per-table sizing estimate does not raise + fall back to
        # the uniform partition default. Mirrors _full_load_engine's capture path.
        dialect = dialect_for(source_config.source_type)
        engine = make_source_engine_factory(session.source_password)(source_config)
        with engine.connect() as connection:
            estimates = estimate_source_rows(connection, list(table_names), dialect)
        scoped = {n: int(c) for n, c in estimates.items() if c}
        return scoped or None
    except Exception:  # noqa: BLE001 - optional sizing signal; uniform fallback
        return None


def _mysql_rolled_back_continues(fallback: str) -> str:
    """How a MySQL Start continues after a Start CloudFormation rolled back (G25), as a clause:
    the offset seeder keeps a position that Start's source connector recorded at or past the
    start point, and what it streamed is only in the CDC topics -- the PostgreSQL wording
    (:func:`_pg_resume_continues`). ``fallback``: where it starts when nothing was recorded."""
    return (
        "continues from the position the rolled-back Start recorded, if it recorded one, else "
        f"from {fallback}; any changes that Start streamed are applied from the CDC topics, so "
        "this is gapless only while the topics still retain them"
    )


def _render_manual_vs_watermark_notice(
    ui, manual_point, wm_resume, *, rolled_back: bool = False, recopied: bool = False
) -> None:
    """A Manual binlog point REPLACES the gapless Full Load watermark as the start offset:
    say which changes that loses (after it) or re-applies (before it). Nothing when equal.
    ``rolled_back``: this stack's last Start CloudFormation rolled back, so Automatic is
    gapless only while the CDC topics retain what that Start streamed (G25). ``recopied``:
    the Full Load is not this migration's first copy, so Automatic is gapless only if it did
    not append to rows already on the target (G27)."""
    m = {"file": manual_point.binlog_file, "pos": int(manual_point.binlog_position)}
    w = {"file": wm_resume.binlog_file, "pos": int(wm_resume.binlog_position)}
    if m == w:
        return
    m_txt = f"{m['file']}:{m['pos']}"
    w_txt = f"{w['file']}:{w['pos']}"
    automatic = (
        f"Automatic {_mysql_rolled_back_continues(f'the snapshot end ({w_txt})')}."
        if rolled_back
        else "Automatic starts at the snapshot's end, which is gapless only if that Full Load "
        "did not append to rows already on the target."
        if recopied
        else None
    )
    if offset_already_at_or_past(m, w):
        render_notice(
            ui,
            tone="warning",
            header="Your manual point skips part of the stream",
            body=(
                f"The Full Load snapshot ends at {w_txt}; streaming from your manual point "
                f"{m_txt} means the changes between them are NOT replicated. "
                + (
                    f"Choose Automatic instead: it {automatic[len('Automatic '):]}"
                    if automatic
                    else "Choose Automatic for the gapless handoff."
                )
            ),
        )
    else:
        render_notice(
            ui,
            tone="info",
            header="Your manual point is before the Full Load snapshot",
            body=(
                f"Streaming from {m_txt} re-reads the changes up to the snapshot's end "
                f"({w_txt}): they are re-applied on top of the loaded rows (idempotent "
                "writes, so the result converges). "
                + (automatic or "Automatic starts exactly at the snapshot's end.")
            ),
        )


def _pg_start_slot(migration_state, watermark) -> str:
    """The replication slot a PostgreSQL Start gives its connector: the one the Full Load
    recorded, else the stack's own -- the same fallback ``dispatch_source_config`` uses."""
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name

    return getattr(watermark, "slot_name", None) or pg_slot_name(
        getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    )


# The CDC topics' condition on a resume (G25), both engines: Stop CDC removes the source and
# the sink in parallel with no drain, so the changes the sink had not applied reach the target
# on the next Start only from the CDC topics -- re-applied from the sink's last commit, which is
# idempotent -- and only while the topics still retain them. PostgreSQL also names the route
# past that period (pg_cdc_route, in _pg_resume_continues and the Stop dialog).
_MYSQL_TOPICS_CONDITION = (
    "Changes the sink had not applied when CDC stopped are applied from the CDC topics, so "
    "this is gapless only while the topics still retain them (their retention period on MSK)."
)
# The same condition said before the Stop, in the Stop CDC dialog.
_STOP_TOPICS_CONDITION = (
    "Changes the sink has not applied when CDC stops are applied from the CDC topics on the "
    "next Start, so a restart is gapless only while the topics still retain them"
)


def _pg_resume_continues(
    slot: str,
    *,
    snapshot_never: bool,
    rolled_back: bool = False,
    cdc_only: bool = False,
    stopped: bool = False,
) -> str:
    """How a PostgreSQL Start continues on a stack whose connectors were started before
    (:func:`cdc_start_resumes`), for the Start CDC and start-point cards. Pure.

    Worded like the start line (:func:`_cdc_start_detail`, G25): the stack records that its
    connectors were started, not that its source connector streamed. ``snapshot_never``
    (``snapshot.mode=never``) never snapshots; ``initial`` continues without a snapshot from a
    stored position whose initial snapshot completed, and snapshots again when none was
    stored or its snapshot had not completed (Debezium 2.7.4). The slot is the one this
    stack's connectors used: a Full Load that takes a new start point waits for new topics on
    a stack that has streamed (the Full Load step's gate,
    :func:`~dsql_migrator.ui.data_migration._cdc_status.cdc_stack_has_streamed`), so no
    resume after one is described here.

    ``rolled_back`` (:func:`_pg_start_rolled_back`): its last Start's update rolled back; the
    Start continues from the position that Start recorded, if it recorded one. ``stopped``
    (with ``rolled_back``): CDC had been stopped before that Start -- the rollback restored the
    stopped parameters (``cdc_has_committed_offset``) -- so the changes the sink had not applied
    at the Stop are in the topics too, and the retention bound counts from the Stop, not from
    that Start. Either way the changes no sink applied -- those that Start streamed, or those
    the sink had not applied when Stop CDC removed it, with no drain -- reach the target only
    from the CDC topics: gapless only while the topics still retain them. Past that, the route
    is new topics (:func:`~dsql_migrator.core.cdc_pg_slot.pg_cdc_route`; ``cdc_only`` picks its
    last step: no Full Load to re-run)."""
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route

    fresh = pg_cdc_route(None, started=True, cdc_only=cdc_only)
    if rolled_back:
        if snapshot_never:
            how = (
                "Start CDC creates them again and continues from the position the "
                "rolled-back Start recorded, if it recorded one, else from replication slot "
                f"{slot}'s confirmed position, with no snapshot (snapshot.mode=never)."
            )
        else:
            how = (
                "Start CDC creates them again and continues from the position the "
                "rolled-back Start recorded, if it recorded one (or from where an earlier "
                "run stopped streaming). If that Start (or the earlier run) had not completed "
                "its initial snapshot, or nothing was recorded, Debezium snapshots every "
                f"selected table first, then streams from replication slot {slot}."
            )
        if stopped:
            # Stopped, then a Start rolled back (G25): the sink's tail from before the Stop is
            # the oldest in the topics, so it is what expires first.
            return (
                "CloudFormation rolled back this stack's last Start, which removed its "
                "connectors; CDC had been stopped before that Start, and stopping removed the "
                f"connectors without draining them. {how} The changes the sink had not applied "
                "when CDC stopped, and any that Start streamed, are applied from the CDC "
                "topics, so this is gapless only while the topics still retain them: once "
                "their retention period on MSK has passed since CDC was stopped, a Start here "
                f"would skip them, so instead {fresh}."
            )
        return (
            "CloudFormation rolled back this stack's last Start, which removed its "
            f"connectors. {how} Any changes that Start streamed are applied from the CDC "
            "topics, so this is gapless only while the topics still retain them: once their "
            "retention period on MSK has passed since that Start, a Start here would skip "
            f"them, so instead {fresh}."
        )
    lead = (
        "This stack's connectors were started before, and stopping CDC removed only the "
        f"connectors (without draining them), not replication slot {slot}."
    )
    if snapshot_never:
        how = (
            "Start CDC re-creates them and continues from that slot with no snapshot "
            "(snapshot.mode=never): from where streaming stopped, or from the Full Load's "
            "start point if its source connector never streamed."
        )
    else:
        how = (
            "Start CDC re-creates them and continues from that slot. If its source connector "
            "streamed, it picks up where streaming stopped. If it never "
            "completed its initial snapshot (its task failed, or CDC was stopped, before then), "
            "Debezium snapshots every selected table first, then streams: a full re-read of the "
            "source, which takes a while on a large one."
        )
    return (
        f"{lead} {how} Changes the sink had not applied when CDC stopped are applied from the "
        "CDC topics, so this is gapless only while the topics still retain them: once their "
        f"retention period on MSK has passed, a Start here would skip them, so instead {fresh}."
    )


def _render_cdc_start_point_card(
    ui,
    migration_state,
    refresh,
    *,
    wm_resume,
    wm_usable: bool,
    effective_resume,
    mode: str,
    locked: bool = False,
    session: object = None,
    source_type: SourceType = SourceType.MYSQL,
    # A watermark that HAS a GTID set but no binlog file:position -- present, yet unable
    # to seed the CDC start offset. Distinguished from "no watermark" so the card can name
    # the actual cause and its fix (the REPLICATION CLIENT grant) instead of implying the
    # Full Load never ran.
    wm_gtid_only: bool = False,
    # This stack streamed before, so its resume offset is already committed and no start
    # point needs choosing -- the card reports the resume instead of demanding a coordinate
    # it does not need.
    resumes_from_offset: bool = False,
    # MySQL (G25, A2): ``(notice, line)`` when Start CDC refuses the start point this card
    # resolves -- it would move a stack that has streamed forward (newer_start_point_refusal)
    # -- and whether it refuses the Automatic one (the Full Load watermark) too.
    newer: Optional[tuple] = None,
    auto_refused: bool = False,
    # Why a PostgreSQL start re-snapshots (see :func:`pg_resnapshot_reason`), or None when
    # it does not. Passed in because the cause is only visible on the RAW job.
    resnapshot_reason: Optional[str] = None,
    # MySQL Manual: the names of the tables a Start would capture, called when Fetch
    # current position is clicked so their definitions are recorded with the position.
    captured_names: Optional[Callable[[], Sequence[str]]] = None,
    # PostgreSQL: the replication slot a Start gives the connector (:func:`_pg_start_slot`);
    # None names the stack's own.
    pg_slot: Optional[str] = None,
    # G27 (:func:`~dsql_migrator.ui.data_migration._cdc_status.drop_reload_condition_due`):
    # the Full Load is not this migration's first copy, so the Automatic start from its
    # watermark is gapless only if it did not append to rows already on the target.
    recopied: bool = False,
    # ...and Start CDC was submitted from an earlier start point before it ran
    # (:func:`~dsql_migrator.ui.data_migration._cdc_status.cdc_started_before_load`), which
    # the notice then says first (``reload_condition``'s ``started``).
    recopied_after_start: bool = False,
    # G137 (:func:`split_start_point_refusal`): Start CDC refuses the Manual point this card
    # resolves -- later than the Full Load's, while CDC captures a table read in parts.
    split_refusal: Optional[str] = None,
    # ...and such a table is captured (:func:`_captured_split_tables`): CDC only's re-run goes
    # through Full load + CDC, since Full load only refuses the switch then.
    split_waiting: bool = False,
) -> None:
    """Render the PRIMARY 'CDC start point' card with an Automatic/Manual choice.

    This is the central decision of the CDC step -- where streaming begins -- so
    it is a top-level card (not buried in the raw connector config). An AWS
    console-style radio chooses between Automatic (gapless from the Full Load
    watermark) and Manual (an explicit GTID / binlog position); the manual inputs
    appear only when Manual is selected (a MySQL resume offers none: a later start point on
    a stack that has streamed needs new CDC topics, G25). A status line confirms the
    resolved start point. Validation is advisory (orange hint, never blocks): MSK
    Connect is the final authority at connector start.

    ``locked`` (CDC already started) makes the radio + manual inputs read-only:
    the start position is already committed to connect-offsets, so changing it
    here cannot affect the live pipeline.

    For a **PostgreSQL** source the choice is Automatic (gapless from the replication
    slot created at Full Load) vs Manual (re-snapshot from scratch, ``snapshot.mode=
    initial``). Debezium PG resumes only from the slot and cannot be given an arbitrary
    WAL LSN, so Manual is a re-snapshot -- there is no coordinate to enter and no
    GTID/binlog inputs are shown.
    """
    is_pg = source_type is SourceType.POSTGRES
    # The operator's Manual entry (MySQL): what the notices below judge. Start seeds its
    # binlog file:position with its GTID set (cdc_seed_watermark), and on a gtid_mode=ON
    # source Debezium positions by that set; a GTID set alone seeds nothing (the offset is
    # keyed on the position).
    manual_point = (
        migration_state.cdc_start_override()
        if (not is_pg and mode == "manual")
        else None
    )
    manual_seedable = manual_point is not None and manual_point.can_seed_offset()

    def _render_gtid_only_notice() -> None:
        render_notice(
            ui,
            tone="warning",
            header="Enter a binlog file:position",
            body=(
                "A GTID set alone cannot set where this pipeline starts: the start offset "
                "is keyed on the binlog file:position, so enter it too (Fetch current "
                "position fills both)."
            ),
        )

    # A PostgreSQL re-snapshot is a RESOLVED start with no coordinate, so it is "ready" even
    # though effective_resume is None -- for Automatic just as much as for Manual. Testing
    # Manual alone put an amber "Action needed" badge directly above an ENABLED blue
    # "Start CDC" (the button's own readiness test passes on the LSN), telling the operator
    # something was broken when the only thing missing was a slot they can no longer get.
    # What Automatic does here is disclosed by the notice below, not by a false alarm.
    ready = newer is None and (
        resumes_from_offset
        or effective_resume is not None
        or (is_pg and (mode == "manual" or not wm_usable))
    )
    # MySQL: the stack's last Start CloudFormation rolled back (G25) -- its source connector
    # may have recorded a position the seeder keeps, and streamed into the CDC topics.
    mysql_rolled_back = (
        not is_pg and not resumes_from_offset and cdc_start_rolled_back(migration_state)
    )
    # The stack's connectors were started before (G25): Start CDC continues from what it
    # recorded, so the card's "where it begins" is not the Full Load snapshot's end.
    streamed = resumes_from_offset or cdc_stack_has_streamed(migration_state)
    from dsql_migrator.ui.cdc_start_point_gate import (
        RELOAD_CONDITION_HEADER,
        RELOAD_CONDITION_TOOLTIP,
        reload_condition,
    )

    # G27: this Full Load is not the migration's first copy -- the condition its watermark's
    # Automatic start carries (MySQL: the radio below; PostgreSQL: the slot start).
    reload_notice = (
        (RELOAD_CONDITION_HEADER, reload_condition(
            getattr(migration_state, "migration_type", None), postgres=is_pg,
            started=recopied_after_start, split=split_waiting,
        ))
        if recopied and wm_usable
        else None
    )
    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon(  # type: ignore[attr-defined]
                "play_circle",
                color="primary" if ready else "grey",
            ).classes("text-xl")
            ui.label("CDC start point").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            # "What is this?" moved to a hover ⓘ instead of a standing paragraph.
            # The unconditional "Automatic resumes exactly where the Full Load snapshot
            # ended" was the tool asserting the operator's assumption as fact, and it was
            # the ONLY explanatory text attached to Automatic anywhere on this card -- so on
            # a PostgreSQL load that recorded no slot it taught the wrong model at exactly
            # the moment the operator was forming one.
            ui.icon("info").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help"
            ).tooltip(
                "Where change streaming begins. This stack has streamed, so Start CDC "
                "continues from its recorded stream position"
                + ("." if locked else " (see the notice below).")
                if streamed
                else RELOAD_CONDITION_TOOLTIP
                if reload_notice is not None
                else "Where change streaming begins. Automatic resumes exactly where the "
                "Full Load snapshot ended (no gap, no overlap)."
                if wm_usable or not is_pg
                else "Where change streaming begins. This load left no replication slot "
                "to resume from, so streaming begins with a fresh snapshot of every "
                "selected table -- nothing is lost, but the source is read again."
            )
            ui.space()  # type: ignore[attr-defined]
            if locked:
                # The locked reason + how to change it rides on the badge tooltip, so
                # no separate standing "CDC has started — locked…" line is needed.
                # A Stop keeps the committed position and Start never rewinds it, so
                # "stop CDC first" alone promised an edit that cannot move it back.
                ui.badge("Locked", color="grey").props("outline").tooltip(  # type: ignore[attr-defined]
                    "CDC has started — the start point is locked. After a Stop, Start CDC "
                    "continues from the committed position; a different start point "
                    "(earlier or later) needs Delete CDC infrastructure and deploy it again "
                    "first."
                )
            elif ready:
                ui.badge("Ready", color="positive").props("outline")  # type: ignore[attr-defined]
            else:
                ui.badge("Action needed", color="warning").props("outline")  # type: ignore[attr-defined]

        # Resuming: no start point NEEDS choosing. The position lives in the connector's
        # offsets topic, not in anything this card can set, so the Automatic/Manual radio
        # would imply the operator must pick one -- and "Automatic — needs a Full Load
        # watermark (unavailable)" would be actively wrong, since no watermark is needed.
        # State the fact. No Manual point is offered: a start point past the committed one
        # needs new CDC topics on a stack that has streamed (G25, A2 -- Start CDC refuses it).
        # PostgreSQL: a stack whose last Start CloudFormation rolled back continues the same
        # way (G25) -- from the position that Start recorded, if it recorded one -- so it
        # takes this branch too: neither a promised snapshot nor an unconditional gapless
        # start below applies to it.
        pg_rolled_back = is_pg and _pg_start_rolled_back(migration_state)
        if (resumes_from_offset or pg_rolled_back) and not locked:
            # MySQL: the last start found the schema history missing, so this one rebuilds
            # it (recovery) after the start-point check -- "nothing re-applied, no choice"
            # would describe a start that is not the one about to run.
            history_missing = not is_pg and bool(
                getattr(migration_state, "cdc_history_missing", False)
            )
            if is_pg:
                # PostgreSQL: the stack records that its connectors were started, not that
                # its source connector streamed, so the card states both outcomes as the
                # start line does (G25). On PostgreSQL an effective resume point exists
                # exactly when the Start sends snapshot.mode=never (pg_snapshot_mode).
                _never = effective_resume is not None
                render_notice(
                    ui,
                    tone="success" if (_never and not pg_rolled_back) else "info",
                    icon="restart_alt",
                    header=(
                        "Starting again after a rolled-back Start"
                        if pg_rolled_back
                        else "Resuming from this stack's replication slot"
                    ),
                    body=(
                        _pg_resume_continues(
                            pg_slot or _pg_start_slot(migration_state, None),
                            snapshot_never=_never,
                            rolled_back=pg_rolled_back,
                            cdc_only=getattr(migration_state, "migration_type", None)
                            is MigrationType.CDC_ONLY,
                            # Stopped before a Start that rolled back: the retention bound
                            # counts from the Stop (G25).
                            stopped=bool(
                                getattr(migration_state, "cdc_has_committed_offset", False)
                            ),
                        )
                        + " No start point or Full Load watermark is needed."
                    ),
                )
            elif history_missing:
                from dsql_migrator.ui.cdc_start_point_gate import new_topics_route

                render_notice(
                    ui,
                    tone="info",
                    icon="restart_alt",
                    header="Resuming from the last streamed position",
                    body=(
                        "This pipeline has streamed before, so streaming continues from "
                        "its committed position on MSK. Its last start found Debezium's "
                        "schema history missing, so this Start rebuilds it from the source "
                        "tables as they are now (snapshot.mode=recovery), after the "
                        "start-point check. If that check names a changed table, a start "
                        "point after the change needs new CDC topics: "
                        + new_topics_route(getattr(migration_state, "migration_type", None))
                        + f". {_MYSQL_TOPICS_CONDITION}"
                    ),
                )
            else:
                render_notice(
                    ui,
                    tone="success",
                    icon="restart_alt",
                    header="Resuming from the last streamed position",
                    body=(
                        # G25: the sink re-applies from its last commit, and what it had not
                        # applied at the Stop comes only from the CDC topics.
                        "This pipeline has streamed before, so its resume position is "
                        "already committed on MSK (stopping CDC deleted the connectors, "
                        "not the position). Streaming continues from there — no start "
                        "point needed and no Full Load watermark required. "
                        f"{_MYSQL_TOPICS_CONDITION}"
                    ),
                )
            # A Manual point is still OFFERED to the seeder, which never rewinds the
            # committed offset -- so one at/before where this stack streamed from is
            # silently not applied. Say so rather than let the entry look honoured.
            if manual_seedable:
                render_notice(
                    ui,
                    tone="warning",
                    header="Your manual start point is not applied",
                    body=(
                        f"Your manual point {manual_point.binlog_file}:"
                        f"{manual_point.binlog_position} is earlier than where this "
                        "stack already streamed and is not applied. Start never rewinds "
                        "a committed position; to replay from it, delete the CDC "
                        "infrastructure and deploy again."
                    ),
                )
            elif manual_point is not None:
                _render_gtid_only_notice()
            # No Manual start point is offered here (G25, A2): one past where this stack
            # streamed needs new CDC topics, and Start CDC refuses it on this stack.
            return

        if is_pg:
            # PostgreSQL: Automatic = gapless from the slot; Manual = re-snapshot (initial).
            # Without a slot Automatic is NOT "(unavailable)" -- it stays selected, it works,
            # and it produces the same snapshot.mode=initial Manual does. Labelling the
            # PRE-SELECTED default unavailable was the card's most direct contradiction of
            # itself: it implied the operator had to act while the enabled Start button
            # implied they did not. Name what Automatic will DO instead.
            # PostgreSQL has NO start-point choice to offer, so it gets no radio. The old
            # "Manual — re-snapshot from scratch (initial)" option was a trap, not a choice:
            # executed over the whole state space, it changes the outcome in exactly ONE
            # state family and is a no-op in every other. With no watermark, and with a
            # Full-load-only watermark, BOTH modes yield snapshot.mode=initial; a
            # Full-load-+-CDC job that is not DONE has its slot suppressed, so both yield
            # initial there too. Only a DONE load that recorded a slot differs -- and there
            # Manual throws away the gapless resume to re-copy every row through the
            # STREAMING pipeline (~19-26K rows/s against the bulk loader's ~103K, measured in
            # docs/manual/en/12-performance-test-results.md) while being unable to remove
            # rows deleted since the load, because a snapshot only reports rows that still
            # exist. Re-running "Full load + CDC" beats it on every axis, which is what the
            # disclosure now says. The re-snapshot CAPABILITY stays (the publication-missing
            # escape buttons still record it); it is just not a start point to pick.
            pass
        else:
            if wm_usable and auto_refused:
                # G25 (A2): this stack has streamed, and the watermark is past where it last
                # started -- Start CDC refuses it (the notice below says why and the route).
                auto_label = "Automatic — from the Full Load watermark"
            elif wm_usable and (mysql_rolled_back or reload_notice is not None):
                # G25: gapless only while the CDC topics retain what the rolled-back Start
                # streamed (the notice below). G27: or only if a Full Load that is not this
                # migration's first copy did not append (the notice below).
                auto_label = "Automatic — from the Full Load watermark (recommended)"
            elif wm_usable:
                auto_label = "Automatic — gapless from Full Load (recommended)"
            elif wm_gtid_only:
                # Do NOT say "needs a Full Load watermark": there IS one. It simply lacks the
                # binlog file:position the offset seed is keyed on, so the honest label names
                # what is missing rather than implying the load never ran.
                auto_label = (
                    "Automatic — unavailable (the Full Load watermark has no binlog "
                    "position)"
                )
            else:
                auto_label = "Automatic — needs a Full Load watermark (unavailable)"
            manual_label = "Manual — enter a binlog file:position"

        def _on_mode(value: str) -> None:
            migration_state.set_cdc_start_mode(value)
            refresh()

        if not is_pg:
            # MySQL keeps its radio: there Manual supplies a REAL coordinate (a binlog
            # file:position), consumed as `resume_override` in `_cdc_resume_signal` and
            # seeded by Start (cdc_seed_watermark), and it is the documented cure for a
            # GTID-only watermark that auto cannot seed from. Both options stay
            # SELECTABLE even when no usable watermark exists -- the
            # steering is the label ("...(unavailable)") plus the notice below, not a disabled
            # option. (A previous comment claimed the option was disabled; no such code
            # existed, and a Quasar radio built from a plain dict has no per-option disable.)
            # When CDC has started the whole choice is disabled (read-only).
            radio = ui.radio(  # type: ignore[attr-defined]
                {"auto": auto_label, "manual": manual_label},
                value=mode,
                on_change=lambda e: _on_mode(e.value),
            ).props("inline=false")
            if locked:
                # Read-only AND clearly greyed: a bare Quasar `disable` dims too subtly
                # to read as "locked", so mute the whole choice (opacity + not-allowed
                # cursor) to match the "Locked" badge.
                radio.props("disable").classes(
                    "opacity-50 pointer-events-none cursor-not-allowed"
                )
            if newer is not None and not locked:
                # G25 (A2): Start CDC refuses this start point -- the stack has streamed,
                # and the point is past where it last started.
                render_notice(
                    ui, tone=newer[0][0], icon=newer[0][1], header=newer[0][2],
                    body=newer[0][3],
                )
            elif mysql_rolled_back and mode == "auto" and wm_usable and not locked:
                # G25: a retry after a Start CloudFormation rolled back -- gapless only while
                # the CDC topics retain what that Start streamed.
                from dsql_migrator.ui.cdc_start_point_gate import new_topics_route

                render_notice(
                    ui,
                    tone="info",
                    icon="restart_alt",
                    header="Starting again after a rolled-back Start",
                    body=(
                        "CloudFormation rolled back this stack's last Start, which removed "
                        "its connectors. Start CDC creates them again and "
                        f"{_mysql_rolled_back_continues('the Full Load watermark')} (their "
                        "retention period on MSK). Past that, a new start point needs new "
                        "CDC topics: "
                        + new_topics_route(getattr(migration_state, "migration_type", None))
                        + "."
                    ),
                )
            elif reload_notice is not None and mode == "auto" and not locked:
                # G27: the watermark's Full Load is not this migration's first copy.
                render_notice(
                    ui, tone="warning", header=reload_notice[0], body=reload_notice[1]
                )
        # PostgreSQL: keyed on the EFFECTIVE snapshot mode, not on a radio value. With the
        # radio gone `mode` no longer describes what will happen, and `not wm_usable` never
        # did -- it missed the state where a slot IS recorded and an escape button set
        # "manual" (a state session restore can reload), where the card claimed a gapless
        # start while the connector was configured `initial`. ``resnapshot_reason`` comes from
        # the RAW job and is None exactly when the start is gapless, so it is the predicate.
        _pg_resnap = is_pg and resnapshot_reason is not None
        if _pg_resnap and not locked:
            # G27 repair: a resume from this load's slot -- the way back the notice names --
            # is gapless only if a Full Load that is not this migration's first copy did not
            # append (``recopied``, not ``reload_notice``: an unfinished load's watermark has
            # its slot suppressed, so it is not ``wm_usable``).
            _notice = pg_resnapshot_notice(
                resnapshot_reason, recopied=recopied,
                migration_type=getattr(migration_state, "migration_type", None),
            )
            if _notice is not None:
                render_notice(ui, tone="warning", header=_notice[0], body=_notice[1])
            if resnapshot_reason == "manual":
                # The way back the notice names (G25): PostgreSQL has no start-point radio, so
                # a re-snapshot the "Re-snapshot every table instead" remedy recorded is set
                # back to Automatic HERE -- the Start then resumes from this Full Load's slot
                # with no snapshot, and the Start CDC dialog runs that Start's own checks.
                with ui.row().classes("w-full justify-end"):  # type: ignore[attr-defined]
                    ui.button(  # type: ignore[attr-defined]
                        PG_RESUME_FROM_SLOT_ACTION,
                        icon="restart_alt",
                        on_click=lambda: _on_mode("auto"),
                    ).props("color=primary outline no-caps")
        # G27: the gapless start from this Full Load's slot, when that Full Load is not this
        # migration's first copy.
        _pg_reload_condition = (
            is_pg and reload_notice is not None and not _pg_resnap
            and effective_resume is not None
        )
        if _pg_reload_condition and not locked:
            render_notice(ui, tone="warning", header=reload_notice[0], body=reload_notice[1])
        if not is_pg and not wm_usable and mode == "auto" and not locked:
            if wm_gtid_only:
                # Name the cause AND the fix. This is the one case where the operator can
                # get a real gapless handoff by changing something on the source, so a
                # generic "no watermark" message would send them to Manual with a GTID
                # that cannot seed the offset either -- still not gapless. The re-run takes a
                # NEW copy, so it names the destructive choice (rule E, G27 repair): an Append
                # keeps the first load's rows as they were, and CDC from the new watermark
                # never replays the changes made before it. CDC only has no Full Load step.
                from dsql_migrator.ui.cdc_start_point_gate import (
                    RERUN_FULL_LOAD_DROP_AND_RELOAD,
                    drop_and_reload_rerun,
                )

                _cdc_only_note = drop_and_reload_rerun(
                    getattr(migration_state, "migration_type", None)
                )[1]
                render_notice(
                    ui,
                    tone="warning",
                    header="This Full Load's watermark cannot give a gapless start",
                    body=(
                        "The watermark recorded a GTID set but no binlog "
                        "file:position, and the CDC start offset is keyed on that "
                        "position — so streaming would begin from the source's CURRENT "
                        "binlog and skip every change made during the Full Load. "
                        "SHOW MASTER STATUS is what supplies it; on RDS/Aurora it needs "
                        "the REPLICATION CLIENT grant. Grant it to the source user and "
                        f"{RERUN_FULL_LOAD_DROP_AND_RELOAD} for a gapless "
                        f"handoff{_cdc_only_note}, or enter a binlog position manually if "
                        "you know the coordinate the load started from."
                        + (
                            # G25: a new start point on this stack needs new CDC topics.
                            " This stack's last Start rolled back, so either way needs new "
                            "CDC topics first: Delete CDC infrastructure and deploy it "
                            f"again, then {RERUN_FULL_LOAD_DROP_AND_RELOAD} or enter the "
                            "position, then Start CDC."
                            if mysql_rolled_back
                            else ""
                        )
                    ),
                )
            else:
                # Steer to manual: auto is not usable here.
                inline_hint(  # type: ignore[attr-defined]
                    ui,
                    "No usable Full Load watermark in this session "
                    "(run a Full Load first, or choose Manual).",
                    tone="warning",
                )

        if mode == "manual" and not is_pg:
            if manual_point is not None and not manual_seedable and not locked:
                _render_gtid_only_notice()
            elif split_refusal and not locked and newer is None:
                # G137: Start CDC refuses this later point while it captures a table the Full
                # Load read in parts -- an error, not the gap warning below.
                render_notice(ui, tone="error", header=SPLIT_READ_HEADER, body=split_refusal)
            elif (
                manual_seedable
                and not locked
                # A point past where a streamed stack last started is refused above (G25,
                # A2); one at or before it is a resume (the card's resume branch).
                and newer is None
                and wm_usable
                and wm_resume is not None
                and wm_resume.can_seed_offset()
            ):
                _render_manual_vs_watermark_notice(
                    ui, manual_point, wm_resume, rolled_back=mysql_rolled_back,
                    recopied=reload_notice is not None,
                )
            _render_cdc_manual_inputs(
                ui, migration_state, refresh, locked=locked, session=session,
                captured_names=captured_names,
            )
        elif wm_usable and wm_resume is not None:
            if is_pg:
                _render_cdc_pg_start_summary(ui, wm_resume.wal_lsn)
            else:
                _render_cdc_start_summary(
                    ui, wm_resume.gtid_executed, wm_resume.binlog_file,
                    wm_resume.binlog_position,
                )

        # Resolved start-point confirmation (the single source of truth).
        if is_pg:
            # PG has no MySQL-style coordinate: confirm the resolved mode instead. Keyed on
            # the same `_pg_resnap` the disclosure above uses, so the two cannot disagree --
            # keying this on `mode == "manual"` printed the gapless line for a slot-bearing
            # watermark that an escape button had switched to a re-snapshot.
            pg_confirm = None
            if _pg_resnap:
                pg_confirm = "Start point set — re-snapshot every table (snapshot.mode=initial)"
            elif effective_resume is not None:
                lsn = effective_resume.wal_lsn or "(recorded)"
                # G27: no "gapless" while the notice above states the reload condition.
                pg_confirm = (
                    f"Start point set — from the replication slot (WAL LSN {lsn})"
                    if _pg_reload_condition
                    else f"Start point set — gapless from the replication slot (WAL LSN {lsn})"
                )
            if pg_confirm is not None:
                with ui.row().classes("items-center gap-2 no-wrap mt-1"):  # type: ignore[attr-defined]
                    ui.icon("check_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
                    ui.label(pg_confirm).classes("text-xs text-gray-700 font-mono")  # type: ignore[attr-defined]
        elif effective_resume is not None and newer is None and not split_refusal:
            with ui.row().classes("items-center gap-2 no-wrap mt-1"):  # type: ignore[attr-defined]
                ui.icon("check_circle", color="positive").classes("text-base")  # type: ignore[attr-defined]
                # The binlog coordinate is what the offset is keyed on, so the confirmation
                # names IT, plus the GTID set seeded with it (what Debezium starts after on
                # a gtid_mode=ON source). A GTID set alone seeds nothing.
                _gtids = normalize_gtid_set(effective_resume.gtid_executed)
                coord = (
                    f"binlog {effective_resume.binlog_file}:"
                    f"{effective_resume.binlog_position}"
                    + (f" · GTID set {gtid_set_summary(_gtids)}" if _gtids else "")
                    if effective_resume.can_seed_offset()
                    else f"GTID {effective_resume.gtid_executed}"
                )
                ui.label(f"Start point set — {coord}").classes(  # type: ignore[attr-defined]
                    "text-xs text-gray-700 font-mono"
                )

def _render_cdc_pg_start_summary(ui, wal_lsn) -> None:
    """Show the resolved WAL LSN for the PostgreSQL Automatic (gapless slot) choice."""
    with ui.row().classes("items-center gap-1 no-wrap mt-1"):  # type: ignore[attr-defined]
        ui.label("WAL LSN:").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
        ui.label(str(wal_lsn or "(none)")).classes("text-xs font-mono")  # type: ignore[attr-defined]


def _render_cdc_start_summary(ui, gtid, binlog_file, binlog_pos) -> None:
    """Show the resolved coordinates for the Automatic (watermark) choice."""
    with ui.row().classes("items-center gap-x-6 gap-y-1 flex-wrap mt-1"):  # type: ignore[attr-defined]
        for label_text, value in (
            ("GTID", gtid or "(none)"),
            (
                "Binlog",
                f"{binlog_file}:{binlog_pos}" if binlog_file else "(none)",
            ),
        ):
            with ui.row().classes("items-center gap-1 no-wrap"):  # type: ignore[attr-defined]
                ui.label(f"{label_text}:").classes("text-xs text-gray-500")  # type: ignore[attr-defined]
                ui.label(str(value)).classes("text-xs font-mono")  # type: ignore[attr-defined]

def _fetch_start_position(
    session, names
) -> "tuple[Optional[Mapping], Optional[dict], Optional[str]]":
    """Read the source's current binlog position, the captured tables' start-point
    fingerprints and the source's ``gtid_mode`` (BLOCKING; callers run it via
    ``run.io_bound``; MySQL).

    ONE read-only engine and ONE connection: the catalog statement (``read_live_catalog``,
    bounded like the CDC catalog-drift read) first, THEN ``SHOW BINARY LOG STATUS`` -- so a
    schema change racing the fetch can only make Start CDC refuse this point, never pass a
    position the recorded definitions do not describe -- then ``@@GLOBAL.gtid_mode``.
    The status row carries the GTID set executed at that position (``Executed_Gtid_Set``,
    read with it), which is what Debezium starts after on a ``gtid_mode=ON`` source.
    Returns ``(row, fingerprints, gtid_mode)``; ``fingerprints`` is ``None`` when the
    catalog read failed and ``gtid_mode`` when it could not be read (the position is still
    returned). A failure of the position read itself raises, as before. The engine is
    disposed in every case.
    """
    from dsql_migrator.core.cdc_catalog_drift import (
        read_live_catalog,
        start_point_fingerprints,
    )
    from dsql_migrator.core.introspector import ReadOnlySourceError
    from dsql_migrator.core.watermark import read_binlog_status_row, read_gtid_mode
    from dsql_migrator.ui.connect import make_source_engine_factory
    from dsql_migrator.ui.data_migration._cdc_status import (
        _CATALOG_DRIFT_READ_TIMEOUT_SECONDS,
    )

    engine = make_source_engine_factory(
        getattr(session, "source_password", None),
        read_timeout_seconds=_CATALOG_DRIFT_READ_TIMEOUT_SECONDS,
    )(getattr(session, "source_config", None))
    try:
        with engine.connect() as conn:
            wanted = sorted({str(n) for n in (names or ()) if n})
            fingerprints: Optional[dict] = None
            try:
                fingerprints = start_point_fingerprints(
                    read_live_catalog(conn, wanted, source_type=SourceType.MYSQL)
                ) if wanted else {}
            except ReadOnlySourceError:
                raise
            except Exception:  # noqa: BLE001 - optional record; the position still counts
                fingerprints = None
            row = read_binlog_status_row(conn)
            gtid_mode = read_gtid_mode(conn) if row else None
    finally:
        engine.dispose()
    return row, fingerprints, gtid_mode


def _render_cdc_manual_inputs(
    ui, migration_state, refresh, *, locked: bool = False, session: object = None,
    captured_names: Optional[Callable[[], Sequence[str]]] = None,
    select_manual: bool = False,
) -> None:
    """Render the Manual start-position inputs (GTID / binlog file:pos) + Apply.

    Shown when the Manual radio is selected, and (``select_manual``) inside the resume
    card's "Manual" disclosure, where "Use this start point" also selects Manual so a
    later point is what Start offers. Advisory validation here: a bad-looking GTID set
    shows a warning but is still stored, and Start CDC refuses it (on every gtid_mode:
    Debezium writes the set into every offset it commits and cannot read a malformed one
    back). ``locked`` (CDC already started) renders the inputs + button read-only.

    When a ``session`` with a live source connection is available, a "Fetch from
    source" button queries ``SHOW MASTER STATUS`` and populates the fields
    automatically — no manual copy-paste needed. It also records the captured tables'
    (``captured_names()``) start-point fingerprints with that position, and -- only while
    the source's ``gtid_mode`` is ON -- the GTID set executed at it (the same status row),
    which is what Debezium starts after on such a source; a set read while the mode is not
    ON is never filled in. "Use this start point" keeps a fetched (or already stored) GTID
    set only with the position it was read at, and the fingerprints only for exactly that
    (position, GTID set) pair -- the fetched pair, or, re-confirming the stored point
    unchanged after a re-render or a session restore, the stored one -- so a typed or
    edited point never carries a set or definitions read for another one.
    """
    gtid_error = {"msg": None}  # mutable cell so the handler can show a hint
    # The last fetch: the (file, pos) it returned, the fingerprints read with it, and the
    # GTID set read with it (normalized; None when the source's gtid_mode was not ON).
    fetched: dict = {"coord": None, "fps": None, "gtid": None}

    def _apply() -> None:
        gtid_raw = (gtid_input.value or "").strip()
        binlog_raw = (binlog_input.value or "").strip()
        gtid_norm = normalize_gtid_set(gtid_raw) or None
        gtid_error["msg"] = validate_gtid(gtid_raw) if gtid_raw else None
        binlog_file = None
        binlog_pos = None
        binlog_unparsed = False
        if binlog_raw:
            parsed = parse_binlog_coordinate(binlog_raw)
            if parsed is not None:
                binlog_file, binlog_pos = parsed
            else:
                binlog_unparsed = True
        _coord = (binlog_file, binlog_pos) if binlog_file is not None else None
        _stored_coord = (
            getattr(migration_state, "_cdc_start_binlog_file", None),
            getattr(migration_state, "_cdc_start_binlog_pos", None),
        )
        _stored_gtid = getattr(migration_state, "_cdc_start_gtid", None)
        # A GTID set read at one position is not the set at another: a fetched (or stored)
        # set left unchanged beside an EDITED position is dropped, not stored -- on a
        # gtid_mode=ON source Debezium would start after it, so a set read later than the
        # new position skips changes and one read earlier re-reads them. That includes no
        # position at all (left empty, or mistyped): a set saved alone is bound to nothing,
        # so a position typed on a later render would silently carry it. A set the operator
        # typed is theirs to assert (Start CDC refuses one that is not a GTID set, and on a
        # gtid_mode=ON source checks it against what the source executed and purged).
        stale_at = None
        if gtid_norm is not None:
            if fetched["gtid"] == gtid_norm and fetched["coord"] not in (None, _coord):
                stale_at = fetched["coord"]
            elif (
                _stored_gtid == gtid_norm
                and _stored_coord[0] is not None
                and _stored_coord[1] is not None
                and _stored_coord != _coord
            ):
                stale_at = _stored_coord
        if stale_at is not None:
            gtid_norm = None
            gtid_input.set_value("")
        # Fingerprints only for the very (position, GTID set) they were read with: this
        # card's fetch, or -- re-confirming the STORED point unchanged -- the ones already
        # recorded for it. The card is rebuilt on every refresh ("Use this start point"
        # itself refreshes) and after a session restore, so the fetch cell is empty then;
        # without the fallback a second click on the same point wiped them and the
        # start-point check dropped to a warning. Any other point carries none: on a
        # gtid_mode=ON source the GTID set IS the start point, so editing only the set
        # drops them too.
        if (
            _coord is not None
            and fetched["coord"] == _coord
            and fetched["gtid"] == gtid_norm
            and fetched["fps"] is not None
        ):
            _bound = fetched["fps"]
        elif _coord is not None and _coord == _stored_coord and gtid_norm == _stored_gtid:
            _bound = migration_state.cdc_start_fingerprints()
        else:
            _bound = None
        migration_state.set_cdc_start_position(
            gtid=gtid_norm,
            binlog_file=binlog_file,
            binlog_pos=binlog_pos,
            fingerprints=_bound,
        )
        # Inside the resume card's disclosure the radio is not shown: using a seedable
        # point there IS choosing Manual (a later point than the committed offset).
        if select_manual and binlog_file is not None:
            migration_state.set_cdc_start_mode("manual")
        if not gtid_raw and not binlog_raw:
            ui.notify(  # type: ignore[attr-defined]
                "Enter a binlog file:position first.",
                type="warning", position="top",
            )
        elif binlog_unparsed:
            ui.notify(  # type: ignore[attr-defined]
                "Binlog must be 'file:position' (e.g. mysql-bin.000123:45678)."
                + (
                    f" The GTID set shown was cleared: it was read at {stale_at[0]}:"
                    f"{stale_at[1]} and is kept only with that position."
                    if stale_at is not None
                    else ""
                ),
                type="negative", position="top",
            )
        elif binlog_file is None and stale_at is not None:
            ui.notify(  # type: ignore[attr-defined]
                f"The GTID set shown was read at {stale_at[0]}:{stale_at[1]}, so it is not "
                "saved without that position. Enter a binlog file:position (on a source with "
                "gtid_mode=ON, with the GTID set executed at it), or Fetch current position "
                "again.",
                type="warning", position="top",
            )
        elif binlog_file is None:
            # GTID only: stored, but it cannot set the start point (see the card notice).
            ui.notify(  # type: ignore[attr-defined]
                "A GTID set alone cannot set the start point — enter its binlog "
                "file:position too (Fetch current position fills both).",
                type="warning", position="top",
            )
        elif stale_at is not None:
            ui.notify(  # type: ignore[attr-defined]
                "Start point saved without a GTID set: the GTID set shown was read at "
                f"{stale_at[0]}:{stale_at[1]}, not at {binlog_file}:{binlog_pos}. On a "
                "source with gtid_mode=ON, enter the GTID set executed at "
                f"{binlog_file}:{binlog_pos}, or Fetch current position again.",
                type="warning", position="top",
            )
        elif gtid_error["msg"]:
            # Start CDC refuses it on every gtid_mode (Debezium would write it into every
            # offset it commits and fail on it at the next task start), so say so now.
            ui.notify(  # type: ignore[attr-defined]
                "Start point saved, but its GTID set is not a valid GTID set (<server "
                "UUID>:<interval>, comma-separated), so Start CDC refuses it — correct it, "
                "or clear it if the source's gtid_mode is not ON.",
                type="warning", position="top",
            )
        else:
            # The binlog coordinate is what the offset is keyed on, so it is what is
            # confirmed -- with the GTID set seeded beside it, when there is one.
            ui.notify(  # type: ignore[attr-defined]
                f"Start point saved (not streaming yet) — {binlog_file}:{binlog_pos}"
                + (" with its GTID set" if gtid_norm else ""),
                type="positive", position="top",
            )
        refresh()

    async def _fetch_from_source() -> None:
        """Query the source binlog status and fill the input fields."""
        from nicegui import run

        source_config = getattr(session, "source_config", None)
        source_password = getattr(session, "source_password", None)
        if source_config is None or source_password is None:
            ui.notify(  # type: ignore[attr-defined]
                "Source connection not available — connect to the source first.",
                type="warning", position="top",
            )
            return

        fetch_btn.disable()
        fetch_btn.set_text("Fetching…")
        try:
            _names = list(captured_names() if callable(captured_names) else ())
        except Exception:  # noqa: BLE001 - no scope: record nothing rather than fail
            _names = []

        try:
            row, fps, gtid_mode = await run.io_bound(_fetch_start_position, session, _names)
        except Exception as exc:  # noqa: BLE001
            ui.notify(  # type: ignore[attr-defined]
                f"Failed to fetch: {exc}",
                type="negative", position="top",
            )
            fetch_btn.set_text("Fetch current position")
            fetch_btn.enable()
            return

        fetch_btn.set_text("Fetch current position")
        fetch_btn.enable()

        if not row:
            ui.notify(  # type: ignore[attr-defined]
                "No binary log position returned — binary logging may be "
                "disabled.",
                type="warning", position="top",
            )
            return

        # The GTID set executed at this position (same status row) -- only while the source
        # runs gtid_mode=ON, where Debezium starts after it. A set read under any other
        # mode is never offered: if the mode is switched to ON later, such a set could make
        # MySQL skip the changes written without a GTID, and Start CDC refuses the point.
        gtid_on = gtid_mode_is_on(gtid_mode)
        gtid_val = normalize_gtid_set(row.get("Executed_Gtid_Set")) if gtid_on else ""
        binlog_file_val = row.get("File") or ""
        binlog_pos_val = row.get("Position")
        binlog_coord = (
            f"{binlog_file_val}:{binlog_pos_val}"
            if binlog_file_val and binlog_pos_val is not None
            else ""
        )

        gtid_input.set_value(gtid_val)
        binlog_input.set_value(binlog_coord)
        try:
            _coord = (
                (str(binlog_file_val), int(binlog_pos_val)) if binlog_coord else None
            )
        except (TypeError, ValueError):
            _coord = None
        fetched["coord"] = _coord
        fetched["fps"] = fps
        fetched["gtid"] = gtid_val or None
        if fps is None:
            # The point is NOT applied yet: say what to do next, as the success path does.
            ui.notify(  # type: ignore[attr-defined]
                "Fetched the current position, but the captured tables' definitions could "
                "not be recorded with it, so Start CDC cannot check them against this "
                "start point. Fetch again to record them, or click 'Use this start point' "
                "to use it without that check.",
                type="warning", position="top",
            )
            return
        if gtid_on:
            _fetched_msg = (
                "Fetched the current position and GTID set from the source (gtid_mode=ON: "
                "CDC starts after this GTID set) — click 'Use this start point' to confirm."
            )
        elif gtid_mode:
            _fetched_msg = (
                "Fetched the current position from the source — its gtid_mode is "
                f"{str(gtid_mode).strip().upper()}, so CDC starts from this binlog "
                "file:position. Click 'Use this start point' to confirm."
            )
        else:
            _fetched_msg = (
                "Fetched current position from source — click 'Use this start point' "
                "to confirm."
            )
        ui.notify(  # type: ignore[attr-defined]
            _fetched_msg,
            type="positive", position="top",
        )

    # Not "GTID (preferred)": the start offset is keyed on the binlog file:position, so a
    # GTID set alone cannot set where streaming begins (cdc_seed_watermark) -- but on a
    # gtid_mode=ON source Debezium starts after the GTID set seeded with it.
    ui.label(  # type: ignore[attr-defined]
        "Enter the binlog file:position streaming starts from. On a source with "
        "gtid_mode=ON, CDC starts after the GTID set executed at that position instead, "
        "so enter that too — Fetch current position fills both. A GTID set alone cannot "
        "set the start point."
    ).classes("text-xs text-gray-500 mt-1")
    gtid_input = ui.input(  # type: ignore[attr-defined]
        "GTID set",
        value=migration_state._cdc_start_gtid or "",
        placeholder="3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100",
    ).classes("w-full text-xs")
    binlog_default = ""
    if migration_state._cdc_start_binlog_file and migration_state._cdc_start_binlog_pos is not None:
        binlog_default = (
            f"{migration_state._cdc_start_binlog_file}:"
            f"{migration_state._cdc_start_binlog_pos}"
        )
    binlog_input = ui.input(  # type: ignore[attr-defined]
        "Binlog file:position",
        value=binlog_default,
        placeholder="mysql-bin.000123:45678",
    ).classes("w-full text-xs")
    if locked:
        # CDC started: the coordinate is already committed -- show it read-only AND
        # visibly greyed so it reads as locked (matches the radio + "Locked" badge).
        # The committed coordinate stays readable in the "Start point set — …" line.
        for _inp in (gtid_input, binlog_input):
            _inp.props("readonly").classes("opacity-50 pointer-events-none")

    if gtid_error["msg"]:
        inline_hint(ui, gtid_error["msg"], tone="warning")  # type: ignore[attr-defined]

    # "Use this start point" (not "Apply"/"Start") — this only records the
    # coordinate into the connector config; it does NOT begin streaming. Actual
    # streaming starts when the config is deployed to the cdc-stack. Disabled once
    # CDC has started (the start point is already seeded and cannot change).
    with ui.row().classes("items-center gap-2 mt-1"):  # type: ignore[attr-defined]
        # Fetch from source: auto-populates from SHOW MASTER STATUS.
        can_fetch = (
            session is not None
            and getattr(session, "source_config", None) is not None
            and getattr(session, "source_password", None) is not None
        )
        if can_fetch and not locked:
            fetch_btn = ui.button(  # type: ignore[attr-defined]
                "Fetch current position",
                on_click=_fetch_from_source,
                icon="download",
            ).props("size=sm flat color=primary")
        else:
            fetch_btn = None  # noqa: F841 — ref needed for the async handler above

        apply_btn = ui.button("Use this start point", on_click=_apply).props(  # type: ignore[attr-defined]
            "size=sm color=primary"
        )
        if locked:
            apply_btn.props("disable")

def _render_cdc_runs_on_banner(ui, phase=None) -> None:
    """Orientation banner: where CDC runs (source -> MSK -> DSQL sink).

    Shown only BEFORE the cdc-stack is deployed (phase ``"absent"``); once it exists
    the architecture is self-evident, so the banner is hidden to reduce noise. Also
    hidden for an unknown (``None``) phase, so it never flashes on a reconnect to an
    already-running pipeline (before the phase probe resolves).
    """
    if phase != "absent":
        return
    with ui.row().classes(  # type: ignore[attr-defined]
        "w-full items-start gap-2 p-3 rounded-lg border border-blue-200 "
        "bg-blue-50 no-wrap"
    ):
        ui.icon("hub", color="primary").classes("text-xl")  # type: ignore[attr-defined]
        with ui.column().classes("gap-0"):  # type: ignore[attr-defined]
            ui.label(  # type: ignore[attr-defined]
                "Runs on the optional cdc-stack: Debezium → MSK → custom DSQL "
                "sink."
            ).classes("text-sm text-gray-700")
            ui.label(  # type: ignore[attr-defined]
                "The control plane configures and monitors CDC; the cdc-stack "
                "(deployed separately) does the streaming."
            ).classes("text-xs text-gray-500")

def classify_cdc_card_phase(
    detected_names,
    stack_name: str,
    probed_phase: Optional[str],
    running_names=None,
    failed_names=None,
) -> Optional[str]:
    """Resolve the CDC lifecycle card's phase from detected connectors + probe.

    Connector detection is authoritative when it finds connectors; otherwise the
    probed CloudFormation phase (``absent`` / ``infra`` / ``unstable``) is used.

    Both expected connectors (source AND sink) present:

    - any of them ``FAILED`` (per ``failed_names`` -- task stopped/errored, needs a
      restart) -> ``"partial"`` (NOT streaming, recovery required) -- this is
      checked first so a dead task is never mistaken for "still provisioning";
    - else all ``RUNNING`` (per ``running_names``) -> ``"running"`` (streaming);
    - else some still coming up (``CREATING``/``UPDATING``) -> ``"provisioning"`` --
      MSK takes ~10-20 min to bring a connector up, so this avoids the misleading
      "Streaming" label while a sink is still being created.

    A non-empty but incomplete set is ``"partial"`` -- the post-failed-Start /
    post-rollback state where (typically) only the source survived: changes are
    captured into Kafka but never written to DSQL, so it is NOT streaming.

    ``running_names`` (optional) is the subset of detected connectors that are
    genuinely RUNNING; ``failed_names`` (optional) is the subset the live status
    view reports FAILED (task dead). When both are omitted the legacy behavior
    holds -- both present is treated as ``"running"``. NiceGUI-agnostic so the
    classification is unit-testable. Pure.
    """
    names = set(detected_names or [])
    if not names:
        return probed_phase
    expected = set(cdc_expected_connector_names(stack_name))
    if not expected.issubset(names):
        return "partial"
    # Both expected connectors exist. A FAILED connector (task stopped/errored)
    # needs a restart, NOT more waiting -- treat it as partial (recovery), never
    # provisioning. Checked before the running/provisioning split below.
    if failed_names and (set(failed_names) & expected):
        return "partial"
    # If we know which are RUNNING, require ALL of them to be RUNNING for
    # "streaming"; otherwise it is still provisioning (coming up).
    if running_names is not None and not expected.issubset(set(running_names)):
        return "provisioning"
    return "running"


def cdc_dead_task_names(expected, names, failed_names) -> list:
    """The expected connectors whose TASK stopped while every expected one exists. Pure.

    In ``expected``'s order (the card passes source first); ``[]`` when an expected
    connector is missing or none stopped. This is the dead-task clause of
    :func:`classify_cdc_card_phase`'s ``"partial"``: the lifecycle notice and its Danger
    zone both key on it, so they never disagree about "missing" versus "stopped".
    """
    expected = list(expected or [])
    if not expected or not set(expected).issubset(set(names or [])):
        return []
    stopped = set(failed_names or [])
    return [n for n in expected if n in stopped]


def _cdc_start_detail(
    stack_name: str,
    watermark: object,
    *,
    mode: Optional[str],
    source_type: object,
    force_snapshot: bool = False,
    exclusions: Optional[Mapping[str, object]] = None,
    resumes: bool = False,
    seed_origin: str = "full_load",
    snapshot_mode: Optional[str] = None,
    rolled_back: bool = False,
    recopied: bool = False,
) -> str:
    """The audit detail for a CDC start: where the stream resumes FROM, and how.

    ``snapshot_mode`` (MySQL) is the Debezium ``snapshot.mode`` the Start sends: it decides
    whether the schema history is rebuilt from the current tables (``recovery``) or the
    recorded one is replayed (``schema_only``).

    ``watermark`` is what the Start offers the seeder and ``seed_origin`` where it came
    from (:func:`_cdc_start_seed`): a ``"manual"`` one is named as the operator's Manual
    start point, not as a Full Load handoff -- by its binlog file:position plus the GTID set
    seeded with it (capped like every other line naming a set), since a Manual point records
    no ``gtid_mode`` for ``watermark_coordinate`` to judge.

    States a Full Load watermark's coordinate through the same precedence the Full Load
    watermark line uses
    (:func:`~dsql_migrator.ui.data_migration._full_load_engine.watermark_coordinate`),
    so the two lines in one log agree rather than spelling it two ways. With no watermark a
    MySQL stream starts from the source's CURRENT position, which means anything written
    between an earlier snapshot and now is NOT replicated -- the one case a reader must be
    able to spot, so it is said explicitly rather than implied by an absent coordinate. A
    PostgreSQL one has no such gap: on a stack whose connectors were started before
    (``resumes``) it continues from the stack's slot -- checked first by the Start's
    publication/slot gate -- or, with ``snapshot.mode=initial`` and a source connector that
    never streamed, snapshots first; without ``resumes`` it snapshots first.

    ``rolled_back`` (PostgreSQL, :func:`_pg_start_rolled_back`, G25): the stack's last Start
    was rolled back by CloudFormation. Its source connector may have recorded a position, so
    the line promises no snapshot either way: it continues from the position the rolled-back
    Start recorded, if it recorded one, and with ``initial`` snapshots first when that Start
    (or the earlier run) had not completed its initial snapshot, or nothing was recorded
    (Debezium 2.7.4); that Start's changes come from the CDC topics -- gapless only while the
    topics retain them. On MySQL (G25) a Full Load watermark's start after such a Start is
    worded the same way: the offset seeder keeps a position that Start recorded at or past the
    watermark, and what it streamed is only in the CDC topics -- so no unconditional "gapless".

    ``recopied`` (G27, :func:`~dsql_migrator.ui.data_migration._cdc_status.
    drop_reload_condition_due`): the Full Load is not this migration's first copy, so a start
    from its watermark is gapless only if it did not append to rows already on the target.

    Log positions, a mode, and column COUNTS only -- never a row value (Property 7).
    """
    from dsql_migrator.core.cdc_pg_slot import pg_slot_name
    from dsql_migrator.ui.cdc_start_point_gate import RELOAD_CONDITION_LINE
    from dsql_migrator.ui.data_migration._full_load_engine import watermark_coordinate

    # ``resumes`` on PostgreSQL means only that this stack's connectors were started and then
    # stopped: whether its source connector ever STREAMED (recorded a position) the stack does
    # not record. snapshot.mode=never streams from the slot either way. initial (a forced --
    # Manual -- or a slotless start) skips the snapshot once its source connector has streamed
    # (live-checked on the shipped Debezium, 2026-10-09), and takes it when that connector
    # never did -- its task failed, or was stopped, before its snapshot completed (live:
    # "Previous snapshot was cancelled before completion; a new snapshot will be taken"). So
    # the line names the slot and states both outcomes, never one the tool cannot know. The
    # Start's gate (_pg_objects_gate_block) checks that slot for every such start.
    pg_resumes = resumes and source_type is SourceType.POSTGRES
    # A Start whose update CloudFormation rolled back (G25): whether that Start's source
    # connector recorded a position the stack does not record, and the changes it streamed
    # are only in the CDC topics -- so the line promises no snapshot and states the bound.
    pg_rolled_back = rolled_back and source_type is SourceType.POSTGRES
    # The snapshot.mode this Start sends (the config's own, else its mirror: never needs the
    # slot the Full Load recorded and no forced snapshot).
    pg_never = (
        snapshot_mode == "never"
        if snapshot_mode
        else bool(getattr(watermark, "slot_name", None)) and not force_snapshot
    )
    parts = [f"stack {stack_name}"]
    if resumes and source_type is not SourceType.POSTGRES:
        # See cdc_start_resumes: nothing new is seeded, so the committed offset decides.
        parts.append(
            "start point: the committed stream position on MSK (this stack streamed "
            "before; it continues where streaming stopped)"
        )
    elif pg_rolled_back:
        _slot = getattr(watermark, "slot_name", None) or pg_slot_name(stack_name)
        parts.append(
            f"start point: replication slot {_slot} (this stack's last Start was rolled back "
            "by CloudFormation; "
            + (
                "snapshot.mode=never, so no snapshot runs and the connector continues from the "
                "position the rolled-back Start recorded, if it recorded one, else from the "
                "slot's confirmed position"
                if pg_never and getattr(watermark, "slot_name", None)
                # initial: Debezium continues without a snapshot only from a stored position
                # whose initial snapshot completed (2.7.4), so nothing more is promised.
                else "the connector continues from the position the rolled-back Start "
                "recorded, if it recorded one, or from where an earlier run stopped "
                "streaming; if that Start (or the earlier run) had not completed its initial "
                "snapshot, or nothing was recorded, Debezium snapshots every selected table "
                "first, then streams from the slot"
            )
            + "; any changes that Start streamed are applied from the CDC topics, gapless "
            "only while the topics still retain them)"
        )
    elif pg_resumes and pg_never and getattr(watermark, "slot_name", None):
        # A PostgreSQL restart streams from the slot's confirmed position, not from the Full
        # Load LSN: naming the watermark as the start put a position the stream had long
        # passed into the permanent record (live PG E2E, 2026-10-09). The slot stands where
        # streaming stopped -- or at this watermark when the connector never streamed from
        # it. Nothing on the stack records which, so the line states both. (A Full Load that
        # takes a new start point waits for new topics on a stack that has streamed -- the
        # Full Load step's gate, cdc_stack_has_streamed -- so no slot it re-created is named.)
        parts.append(
            f"start point: replication slot {watermark.slot_name} (this stack's connectors "
            "were started before; snapshot.mode=never, so no snapshot runs and the connector "
            "continues from the slot's confirmed position -- where streaming stopped, or the "
            f"Full Load watermark {watermark_coordinate(watermark, source_type)} if the "
            "connector never streamed from it)"
        )
    elif pg_resumes:
        # snapshot.mode=initial: the slot the Full Load recorded, else the stack's own
        # (pg_slot_name) -- the same name dispatch_source_config gives the connector.
        parts.append(
            "start point: replication slot "
            f"{getattr(watermark, 'slot_name', None) or pg_slot_name(stack_name)} (this "
            "stack's connectors were started before: if its source connector streamed, no "
            "snapshot runs and the connector continues from the slot's confirmed position, "
            "where streaming stopped; if it never streamed -- its task failed, or was "
            "stopped, before its snapshot completed -- Debezium snapshots every selected "
            "table first, then streams from the slot)"
        )
    elif watermark is not None and seed_origin == "manual":
        # Named as the card, the deploy log and the "CDC start position" line name it: the
        # binlog file:position the offset is keyed on, plus the GTID set seeded with it --
        # Debezium starts after that set only on a gtid_mode=ON source (at the position on
        # any other mode), and a Manual point records no mode for watermark_coordinate.
        _gtids = normalize_gtid_set(getattr(watermark, "gtid_executed", None))
        parts.append(
            f"start point binlog {getattr(watermark, 'binlog_file', None)}:"
            f"{getattr(watermark, 'binlog_position', None)}"
            + (f" with GTID set {gtid_set_summary(_gtids)}" if _gtids else "")
            + " (Manual start point; changes from this position on are replicated)"
        )
    elif watermark is None and seed_origin == "manual_unseedable":
        parts.append(
            "start point: the source's CURRENT position — the Manual start point has no "
            "binlog file:position to seed, so any change written before this moment is "
            "NOT replicated"
        )
    elif watermark is None and source_type is SourceType.POSTGRES:
        # No Full Load watermark and no committed offset (CDC only's first start, or a stack
        # deployed again since): snapshot.mode=initial snapshots every selected table, then
        # streams from the stack's own slot (pg_slot_name). No gap either, so the
        # CURRENT-position sentence is MySQL's only.
        parts.append(
            "start point: a snapshot — there is no Full Load watermark, so the "
            "connector snapshots every selected table, then streams from its own "
            f"replication slot {pg_slot_name(stack_name)}"
        )
    elif watermark is not None:
        # "gapless" is a claim about the SLOT, not about merely HAVING a watermark. A
        # PostgreSQL Full-load-only watermark carries a WAL LSN and no slot, so nothing can
        # resume from it and the connector re-snapshots instead -- certifying that start as
        # gapless in the PERMANENT record was worse than saying nothing, because Step 4 will
        # show rows the source no longer has and this line is the only durable artifact a
        # reader has to explain them. Mirrors ``pg_snapshot_mode``: PostgreSQL re-snapshots
        # when no slot was recorded OR a full snapshot is forced -- with no committed offset
        # (one with an offset continues from it: named above).
        pg_resnapshots = source_type is SourceType.POSTGRES and (
            not getattr(watermark, "slot_name", None) or force_snapshot
        )
        parts.append(
            f"start point {watermark_coordinate(watermark, source_type)} "
            + (
                "(re-snapshot: every selected table is read again before streaming, so "
                "rows DELETED on the source since the Full Load remain on the target)"
                if pg_resnapshots
                # MySQL after a Start CloudFormation rolled back (G25).
                else "(this stack's last Start was rolled back by CloudFormation: the "
                f"connector {_mysql_rolled_back_continues('this start point')})"
                if rolled_back and source_type is not SourceType.POSTGRES
                # G27: a Full Load that is not this migration's first copy.
                else RELOAD_CONDITION_LINE
                if recopied
                else "(gapless from the Full Load watermark)"
            )
        )
    else:
        parts.append(
            "start point: the source's CURRENT position — there is no Full Load "
            "watermark, so any change written before this moment is NOT replicated"
        )
    parts.append(f"mode {mode or 'auto'}")
    if snapshot_mode and source_type is not SourceType.POSTGRES:
        parts.append(f"snapshot.mode {snapshot_mode}")
    if force_snapshot:
        parts.append(
            # Worded like the start point above: the stack does not record whether its
            # source connector streamed, which is what decides it.
            "Manual's full snapshot runs only if nothing was recorded on this stack, or the "
            "run that recorded its position had not completed its initial snapshot"
            if pg_rolled_back
            else "Manual's full snapshot runs only if this stack's source connector never streamed"
            if pg_resumes
            else "a FULL initial snapshot is forced for this start"
        )
    excluded = sum(len(cols or ()) for cols in (exclusions or {}).values())
    if excluded:
        parts.append(f"{excluded} column(s) excluded from replication (Exclude tables & columns)")
    return "; ".join(parts)


def cdc_unstable_message(
    status: Optional[str], *, has_seeder_lambda: Optional[bool] = None
) -> tuple[str, str, str, str]:
    """Message for the ``unstable`` CDC card, keyed on the raw stack status.

    Returns ``(badge_text, tone, header, body)``. Three cases:

    - **DELETE_IN_PROGRESS** — a live teardown: the infra is being removed, so this
      is REASSURING (info), names the ~15-25 min native ENI-detach wait, and says
      billing stops on completion. Badge ``"Deleting…"``.
    - **any other ``*_IN_PROGRESS``** — some other live operation: wait for it
      (warning). Badge ``"Busy"``.
    - **terminal stuck** (ROLLBACK_*/DELETE_FAILED/…) — will not clear on its own;
      delete then redeploy (warning). Badge ``"Busy"``.

    Pure / NiceGUI-agnostic so the messaging is unit-testable.
    """
    raw = (status or "busy")
    upper = raw.upper()
    if upper == "DELETE_IN_PROGRESS":
        return (
            "Deleting…",
            "info",
            "CDC infrastructure is being deleted",
            f"The cdc-stack is being torn down (this takes "
            f"{cdc_teardown_estimate(has_seeder_lambda=has_seeder_lambda)} — "
            f"{cdc_teardown_reason(has_seeder_lambda=has_seeder_lambda)}). MSK / NAT "
            "billing stops once it completes. This view refreshes automatically; no "
            "action is needed.",
        )
    if _is_inflight_stack_status(raw):
        return (
            "Busy",
            "warning",
            "cdc-stack is busy",
            f"The cdc-stack is '{raw}'. Wait for the current operation to finish "
            "(progress refreshes), then the next action appears.",
        )
    return (
        "Busy",
        "warning",
        "cdc-stack needs cleanup",
        f"The cdc-stack is stuck in '{raw}' from a failed operation — it will not "
        "clear on its own. Use Delete CDC infrastructure below, then deploy again.",
    )

def cdc_live_running_names(discovery_running, connector_states) -> list:
    """Narrow the discovery "running" set to connectors LIVE-reported RUNNING.

    ``discovery_running`` is the set of connectors whose MSK ``connectorState`` was
    RUNNING at the last stack/connector discovery -- but a connector can report
    ``connectorState=RUNNING`` while its TASK has stopped/errored (not actually
    streaming). The live status view (``connector_states``, built from CloudWatch
    task health) flips such a connector to a non-RUNNING state. Intersecting the
    two yields the connectors that are genuinely streaming, so the lifecycle card's
    phase matches the Pipeline-health rows (a task-dead source → "Incomplete", not
    a misleading "Streaming").

    When no live states are available yet (no poll has run), the discovery set is
    used as-is (best signal we have). Pure.
    """
    running = [n for n in (discovery_running or [])]
    if not connector_states:
        return running
    return [
        n for n in running
        if str(connector_states.get(n, "")).upper() == "RUNNING"
    ]

def cdc_deploy_card_superseded(
    has_deploy_job: bool, deploying: bool, deploy_job_error, phase
) -> bool:
    """Whether a finished deploy job's stage card should be SUPPRESSED.

    A deploy job the JobManager reconciled to FAILED only because the app
    restarted mid-run carries stale stages (e.g. ``stack_sink=FAILED`` with later
    steps still PENDING) -- a red mark + "not done yet" rows that contradict the
    real, AWS-derived verdict. When live connector discovery already has a
    definitive phase (``running`` / ``provisioning`` / ``partial``), that is the
    truth, so the stale interrupted-job stage card is hidden and discovery drives
    the display. Only applies to a restart-interrupted job that is no longer
    in-flight; a genuinely-failed job or one still running keeps its card. Pure.
    """
    return (
        has_deploy_job
        and not deploying
        and is_interrupted_by_restart(deploy_job_error)
        and phase in ("running", "provisioning", "partial")
    )


def cdc_state_is_undetermined(migration_state) -> bool:
    """True when the CDC state is UNKNOWN rather than known-absent.

    The lifecycle card treats a ``None`` phase as "absent / not yet probed" and offers
    the deploy form. Those are not the same thing. The AWS phase probe needs a target
    region (``session.target_config``) and returns silently without one -- and a
    restored session deliberately does not trust its old connections, so after an app
    restart the probe has not run and the phase is ``None`` while a real pipeline may be
    streaming. The card then showed a fresh-deploy form for infrastructure that already
    exists, with nothing on screen saying the state was simply unknown. (Observed: the
    app was restarted during Start CDC; both connectors reached RUNNING on AWS, but the
    CDC pipeline card came back blank.)

    ``cdc_stack_phase_checked`` is the discriminator: the probe sets it whenever it
    reports, including when it reports "absent". So an unset flag means "we have not
    looked", which is what this reports -- distinct from "we looked and there is
    nothing".

    Pure (reads already-populated state; no AWS I/O), so it is safe during render.
    """
    if getattr(migration_state, "cdc_stack_phase_checked", False):
        return False  # the probe reported -- absent is then a real answer
    return getattr(migration_state, "cdc_stack_phase", None) is None


def cdc_redeploy_needs_confirmation(migration_state) -> bool:
    """True when the deploy form should wait behind an explicit "redeploy?" prompt.

    Only after a teardown IN THIS SESSION (``cdc_action_kind == "delete"``, which
    outlives the finished job) and only until the operator says yes. A CDC delete takes
    ~20 min and removes a billable MSK cluster; the moment it lands, the answer the
    operator wants is "it's gone", not a ~20-line BYO-VPC form implying the tool is
    about to rebuild what they just removed.

    A first-ever deploy is deliberately NOT gated: there the form IS the next step, and
    an extra click to reach it would be pure friction.

    Pure (reads already-populated state); safe during render.
    """
    if getattr(migration_state, "cdc_redeploy_confirmed", False):
        return False
    return getattr(migration_state, "cdc_action_kind", None) == "delete"


def _render_cdc_redeploy_prompt(ui, migration_state, refresh) -> None:
    """Confirm the teardown, then offer redeploy as a choice rather than a form.

    Leads with the outcome the operator was waiting for (the infrastructure is gone and
    is no longer billing), and makes rebuilding an explicit opt-in that states the real
    cost of saying yes (~5 min, billable). Answering yes latches
    ``cdc_redeploy_confirmed`` so the form stays open across refreshes, and makes the CDC
    sub-step -- where this prompt and the deploy form are -- the active one (G27): a section
    opened in the browser is not the active sub-step, so the refresh used to open Full Load.
    """
    render_notice(
        ui,
        tone="success",
        icon="task_alt",
        header="CDC infrastructure deleted",
        body=(
            "The cdc-stack and its MSK Serverless cluster are gone, so they no longer "
            "incur charges. Nothing else in this migration was affected — the target "
            "data and the Full Load results are untouched."
        ),
    )

    def _confirm() -> None:
        migration_state.set_cdc_redeploy_confirmed(True)
        migration_state.set_active_substep("cdc")
        if callable(refresh):
            refresh()

    with ui.row().classes("items-center gap-2 no-wrap flex-wrap"):  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "Deploy CDC infrastructure again?"
        ).classes("text-xs text-gray-600")
        ui.button(  # type: ignore[attr-defined]
            "Redeploy CDC infrastructure", on_click=_confirm
        ).props("outline size=sm").classes("normal-case")
    ui.label(  # type: ignore[attr-defined]
        "Rebuilding creates a new MSK Serverless cluster and takes ~5 minutes; "
        "it is billable. Leave this alone if you are done with CDC."
    ).classes("text-xs text-gray-500")


def _render_cdc_state_unknown_notice(
    ui, migration_state=None, refresh=None
) -> None:
    """Say the CDC state is unknown, WHY, and how to recover it.

    Without this the operator gets a deploy form for a pipeline that may already be
    running -- there is no way to tell from the screen that the tool simply has not
    looked yet. Two distinct causes, named so the user acts on the right one:

    * **probe FAILED** (``cdc_probe_error`` set) -- the read-only ``describe_stacks``
      check threw (bad/expired creds, missing ``cloudformation:DescribeStacks``, wrong
      region, throttle). Re-verifying the target will NOT help; the AWS access has to be
      fixed. Show the real error so it is actionable.
    * **probe NOT RUN** (no error) -- a restored session's connections are untrusted
      until re-verified, so the probe has not run yet. Re-verify the target on Connect.

    Either way a "Re-check CDC state" button re-runs the probe immediately (it clears
    the discovery throttle + refreshes), so recovery does not require navigating away.
    """
    probe_error = getattr(migration_state, "cdc_probe_error", None)
    if probe_error:
        body = (
            "The read-only AWS check that reads the live CDC state FAILED, so the tool "
            "cannot tell whether a pipeline exists. Re-verifying the target will not "
            f"help — fix the AWS access first. Reported error: {probe_error}. Check the "
            "credentials/permissions (cloudformation:DescribeStacks) and the target "
            "region, then re-check. Any pipeline you already started keeps streaming."
        )
    else:
        body = (
            "This session was restored, so its connections are not trusted until "
            "re-verified and the read-only AWS check that reads the live CDC state has "
            "not run. Any pipeline you already started is unaffected and keeps "
            "streaming. Re-verify the target connection on the Connect step to recover "
            "the real state here — do not start CDC again until it shows."
        )
    render_notice(ui, tone="warning", header="CDC state not determined yet", body=body)

    # A direct re-check so the user is not stuck: clear the discovery throttle so the
    # next render re-probes immediately, then refresh. (The discovery timer is armed on
    # render and gated by ``_cdc_discovery_monotonic``.)
    if migration_state is not None and callable(refresh):
        def _recheck(_e=None) -> None:
            try:
                migration_state._cdc_discovery_monotonic = None
            except Exception:  # noqa: BLE001 - best effort
                pass
            refresh()

        ui.button(  # type: ignore[attr-defined]
            "Re-check CDC state", icon="refresh", on_click=_recheck
        ).props("outline size=sm color=primary").classes("mt-2")


def _render_cdc_start_action(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Render the CDC lifecycle card: Deploy infra → Start → Stop → Delete.

    The UI owns the whole cdc-stack lifecycle as CloudFormation operations. The
    card branches on the cached stack phase (probed by ``_ensure_cdc_controller``
    / ``_probe_cdc_stack_phase``):

    * **absent** — no stack yet → show the BYO-VPC infrastructure form + a
      "Deploy CDC infrastructure" button (create_stack, ~5 min).
    * **infra** — stack up, no connectors → "Start CDC" (after the start point is
      set): a two-pass update that creates the connectors.
    * **running** — connectors deployed → "Stop CDC" (delete connectors only).
    * **unstable** — an operation is mid-flight or rolled back → guidance.

    A guarded "Delete CDC infrastructure" action is always offered (type-to-
    confirm) for full teardown / rollback recovery. While any lifecycle job runs,
    its ordered stages + stack-event log stream in via ``_render_cdc_deploy_live``.
    """
    deploy_job = _current_job(
        job_manager, getattr(migration_state, "cdc_deploy_job_id", None)
    )
    deploying = deploy_job is not None and deploy_job.status in ("PENDING", "RUNNING")

    controller = getattr(migration_state, "cdc_controller", None)
    names = getattr(migration_state, "cdc_connector_names", []) or []
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    expected = set(cdc_expected_connector_names(stack_name))
    detected_names = names if controller is not None else []
    # "Running" must mean a connector whose TASK is actually streaming, not merely
    # that the connector resource exists. The live status view folds CloudWatch
    # task health into per-connector state (a connector MSK reports RUNNING but
    # whose task stopped/errored becomes FAILED), so intersect the discovery
    # running-set with the status view's RUNNING connectors. This keeps the top
    # card's phase in lockstep with the Pipeline-health rows -- so a source whose
    # task died shows "Incomplete", not a misleading "Streaming".
    live_states = (
        getattr(
            _cdc_status_view(migration_state, job_manager), "connector_states", None
        )
        if controller is not None
        else None
    )
    running_names = cdc_live_running_names(
        getattr(migration_state, "cdc_connector_running_names", []) or [],
        live_states,
    ) if controller is not None else []
    # Connectors the live status view reports FAILED (task stopped/errored). A
    # FAILED connector needs a RESTART, not more waiting -- so it must classify as
    # "partial" (recovery), never "provisioning". Without this a dead source task
    # is mislabeled "Provisioning… (streaming begins once both reach RUNNING)",
    # telling the user to wait when the fix is to restart.
    failed_names = (
        [n for n, s in (live_states or {}).items() if str(s).upper() == "FAILED"]
    )
    phase = classify_cdc_card_phase(
        detected_names,
        stack_name,
        getattr(migration_state, "cdc_stack_phase", None),
        running_names=running_names,
        failed_names=failed_names,
    )

    with ui.card().classes("w-full"):  # type: ignore[attr-defined]
        with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
            # For the unstable phase the badge/notice depend on the raw stack status
            # (a live DELETE reads as "Deleting…", not a vague "Busy"). Derive both
            # from the single pure helper so badge and notice never diverge.
            _unstable_badge, _unstable_tone, _unstable_header, _unstable_body = (
                cdc_unstable_message(
                    getattr(migration_state, "cdc_stack_phase_status", None)
                )
            )
            _unstable_badge_color = "primary" if _unstable_tone == "info" else "warning"
            # A teardown in flight outranks the connector phase: CloudFormation has not
            # removed the connectors yet, so discovery still reports "running" and the
            # badge read a green "Streaming" beside a body saying "Deleting
            # infrastructure". See cdc_teardown_badge.
            _teardown = cdc_teardown_badge(migration_state, job_manager)
            badge_text, badge_color, icon_color = (
                (_teardown[0], _teardown[1], _teardown[1]) if _teardown is not None
                else ("Streaming", "positive", "positive") if phase == "running"
                else ("Provisioning…", "primary", "primary") if phase == "provisioning"
                else ("Working…", "primary", "primary") if deploying
                else ("Incomplete", "warning", "warning") if phase == "partial"
                else ("Infra ready", "primary", "primary") if phase == "infra"
                else (_unstable_badge, _unstable_badge_color, _unstable_badge_color)
                if phase == "unstable"
                else ("Not deployed", "grey", "grey")
            )
            ui.icon("rocket_launch", color=icon_color).classes("text-xl")  # type: ignore[attr-defined]
            ui.label("CDC pipeline").classes("text-sm font-semibold")  # type: ignore[attr-defined]
            ui.space()  # type: ignore[attr-defined]
            ui.badge(badge_text, color=badge_color).props("outline")  # type: ignore[attr-defined]

        # Live progress for whichever lifecycle job is in flight (or just done).
        # EXCEPT: a deploy job that the JobManager reconciled to FAILED only
        # because the app restarted mid-run is NOT the truth -- its stale stages
        # (e.g. stack_sink=FAILED) would show a red mark and "not done yet" steps
        # even though AWS finished. When live connector discovery already has a
        # definitive verdict (Streaming / Provisioning / Incomplete), let THAT
        # drive the card and suppress the stale interrupted job's stage card.
        deploy_job_error = None
        if deploy_job is not None:
            try:
                deploy_job_error = job_manager.get_error(
                    migration_state.cdc_deploy_job_id
                )
            except JobNotFoundError:
                deploy_job_error = None
        interrupted_superseded = cdc_deploy_card_superseded(
            deploy_job is not None, deploying, deploy_job_error, phase
        )
        if deploy_job is not None and not interrupted_superseded:
            _render_cdc_deploy_live(ui, migration_state, job_manager, refresh)
            if deploying:
                return  # all action buttons hidden while an operation runs

        if phase == "provisioning":
            # Both connectors exist but at least one is still CREATING/UPDATING on
            # MSK (~10-20 min). Show a clear "still coming up" notice instead of a
            # misleading "Streaming"/Stop affordance, and re-poll so the card flips
            # to "running" on its own once the sink reaches RUNNING.
            still = [n for n in names if n not in set(running_names)]
            render_notice(
                ui,
                tone="info",
                header="CDC pipeline provisioning",
                body=(
                    "The connectors are deploying on MSK Connect (this takes about "
                    "~5 minutes). "
                    + (
                        f"Still coming up: {', '.join(still)}. "
                        if still
                        else ""
                    )
                    + "Streaming begins once both connectors reach RUNNING; this "
                    "view refreshes automatically."
                ),
            )
            ui.timer(  # type: ignore[attr-defined]
                _CDC_POLL_INTERVAL_SECONDS, refresh, once=True
            )
        elif phase == "running":
            _render_cdc_running_actions(
                ui, migration_state, job_manager, refresh, session=session
            )
        elif phase == "partial":
            _render_cdc_partial_actions(
                ui, migration_state, job_manager, refresh,
                # Ordered (source first), so the notice names stopped connectors in
                # data-flow order.
                names=names, expected=cdc_expected_connector_names(stack_name),
                inventory=inventory, session=session,
                failed=failed_names,
            )
        elif phase == "infra":
            _render_cdc_start_button(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            )
        elif phase == "unstable":
            render_notice(
                ui, tone=_unstable_tone, header=_unstable_header, body=_unstable_body
            )
            # A live operation (DELETE / other *_IN_PROGRESS) clears on its own, so
            # keep polling to flip the card when it settles; a terminal stuck state
            # will not clear, so no auto-poll (the user must act).
            if _is_inflight_stack_status(
                getattr(migration_state, "cdc_stack_phase_status", None)
            ):
                ui.timer(_CDC_POLL_INTERVAL_SECONDS, refresh, once=True)  # type: ignore[attr-defined]
        elif cdc_state_is_undetermined(migration_state):
            # NOT the same as absent: the probe has not run (a restored session's
            # connections are untrusted until re-verified), so offering a deploy form
            # here would invite a duplicate, billable MSK cluster for a pipeline that
            # may already be streaming.
            _render_cdc_state_unknown_notice(ui, migration_state, refresh)
        else:  # absent -- the probe reported, and there really is nothing
            # Account-scoped discovery: if CDC infra already exists under a name this
            # (reset) session does not target, offer to ADOPT it rather than deploy a
            # duplicate (a second, costly MSK cluster). Adoption re-reads the live
            # state from AWS, so a running pipeline lands on its monitoring view.
            other_stacks = getattr(migration_state, "cdc_other_stacks", []) or []
            if other_stacks:
                _render_cdc_adopt_or_deploy_choice(
                    ui, migration_state, job_manager, refresh, other_stacks,
                    inventory=inventory, session=session,
                )
            elif cdc_redeploy_needs_confirmation(migration_state):
                # Straight after a teardown, confirm the deletion and ASK before
                # showing the deploy form again (see the predicate's docstring).
                _render_cdc_redeploy_prompt(ui, migration_state, refresh)
            else:
                _render_cdc_infra_deploy_action(
                    ui, migration_state, job_manager, refresh,
                    inventory=inventory, session=session,
                )

        # A stable stack only: an unstable one (rolling back, being deleted) has its own
        # guidance above.
        if phase in ("provisioning", "running", "partial", "infra"):
            _render_cdc_plugin_advisory(ui, migration_state)

        # Full teardown (guarded) once a stack might exist. When a connector is up
        # (running OR partial) the inline primary action is Stop CDC (connectors
        # only, keeps MSK/VPC) -- a full Delete would tear down the whole pipeline,
        # so it is NOT shown inline beside Stop; it is tucked behind a collapsed
        # "Danger zone" so it stays reachable for an intentional teardown but can't
        # be hit by mistake beside Stop. For infra / unstable phases (no connector
        # to stop) Delete IS the recovery action, so it stays inline.
        if phase in ("running", "partial"):
            if phase == "running":
                danger_header = "Stop CDC first for a normal pause"
                danger_body = (
                    "Deleting tears down the whole cdc-stack (MSK, VPC wiring, "
                    "plugins, connectors). To pause streaming, use Stop CDC above "
                    "instead — it keeps the infrastructure so you can restart "
                    "quickly. Delete only for a full teardown."
                )
            elif cdc_dead_task_names(
                cdc_expected_connector_names(stack_name), names, failed_names
            ):
                # Every connector exists but a task stopped: "Delete if Start keeps
                # failing" would send a stop that repeats by design (an unparseable DDL,
                # a schema-history replay) to a slow, billable teardown that returns to
                # the same stop. The partition quota is the one cause Delete does fix.
                danger_header = "Delete does not restart a stopped connector"
                danger_body = (
                    "Deleting tears down the whole cdc-stack (MSK, VPC wiring, plugins, "
                    "connectors) and does not fix why a connector stopped: after you "
                    "deploy again, Start CDC reads the source again, so the connector "
                    "stops the same way until its cause is dealt with. Follow the steps "
                    "above, and on Pipeline health when it names the cause, instead. "
                    "Delete only for a full teardown, when the MSK partition quota is "
                    "exhausted (a redeploy reclaims it), or when the steps above call for a "
                    "new start point: on a stack that has streamed, that needs new CDC "
                    "topics (Delete, deploy again, Re-run Full Load with Drop & reload, then "
                    "Start CDC)."
                )
            else:  # partial: a connector is missing
                danger_header = "Delete only if Stop + Start can't recover"
                danger_body = (
                    "Try Stop CDC above, then Start CDC again first. Delete only if "
                    "Start keeps failing (e.g. the MSK partition quota is exhausted) "
                    "— it tears down the whole cdc-stack (MSK, VPC wiring, plugins, "
                    "connectors) and a redeploy reclaims the quota from a clean slate."
                )
            with ui.expansion("Danger zone — delete all CDC infrastructure").props(
                "dense"
            ).classes(f"w-full mt-2 text-red-700 {EXPANSION_PANEL_CLASSES}"):
                render_notice(ui, tone="warning", header=danger_header, body=danger_body)
                _render_cdc_delete_action(
                    ui, migration_state, job_manager, refresh, session=session
                )
        elif phase in ("infra", "unstable"):
            _render_cdc_delete_action(ui, migration_state, job_manager, refresh, session=session)


def _render_cdc_plugin_advisory(ui, migration_state) -> None:
    """Warn while the deployed cdc-stack runs an older sink plugin than this version bundles.

    Start CDC never re-registers the plugins, so such a stack keeps its sink -- and every
    defect fixed since -- until Delete + Deploy CDC infrastructure. The version is the one
    the stack-phase probe read (no AWS call here); unknown says nothing.
    """
    from dsql_migrator.core.cdc import plugin_version_is_older
    from dsql_migrator.core.s3_provision import PLUGIN_VERSION

    deployed = getattr(migration_state, "cdc_deployed_plugin_version", None)
    if not plugin_version_is_older(deployed, PLUGIN_VERSION):
        return
    render_notice(
        ui,
        tone="warning",
        header="This CDC infrastructure runs an older sink plugin",
        body=(
            f"It was deployed with sink plugin {deployed}, and this version of the tool "
            f"bundles {PLUGIN_VERSION}. Start CDC keeps the plugin a stack was deployed "
            f"with, so the sink fixes made since {deployed} do not reach this pipeline "
            "until you Delete CDC infrastructure and deploy it again, which registers "
            f"{PLUGIN_VERSION}. The Delete confirmation says what deleting also removes."
        ),
    )


def _render_cdc_partial_actions(
    ui, migration_state, job_manager, refresh, *, names, expected,
    inventory=None, session=None, failed=None,
) -> None:
    """Actions for a PARTIAL pipeline: some but not all connectors are present -- or all
    are, but a connector's task stopped (``failed``).

    This is the post-failed-Start / post-rollback state — e.g. the source
    survived but the sink connector failed to create (commonly the MSK Serverless
    partition quota being exhausted) and CloudFormation rolled it back. With only
    one connector, changes are captured into Kafka but never written to DSQL (if
    the sink is missing) or nothing is captured at all (if the source is missing),
    so this is NOT "streaming". Be honest about it and steer the user to recover.

    Two recovery actions, ranked:

    - **Retry CDC** (primary): re-run Start CDC. ``run_cdc_start`` is config-aware
      idempotent, so a source that is already RUNNING is kept (no binlog re-read,
      no wasted MSK churn) and only the MISSING connector is (re)created. The
      events the source already streamed sit buffered in the Kafka topics, so the
      new sink consumes them and catches DSQL up -- gapless only while the topics still
      retain them (G25). This is the fast, non-destructive path for a sink that failed
      to create.
    - **Clean up leftover connector** (secondary): remove the surviving connector
      (same backend path as Stop CDC) to return to a clean infra-only state --
      useful when Retry keeps failing (e.g. MSK partition quota) and a from-scratch
      Start (or Delete + redeploy via the Danger zone) is wanted.

    When every expected connector EXISTS but one's task stopped (a FAILED connector -- an
    unparseable DDL statement or a schema-history replay failure, say), neither action
    fits: there is nothing missing to re-create, and Retry CDC does not restart a
    connector that exists (``run_cdc_start`` skips both when MSK reports them RUNNING, and
    an unchanged update re-creates nothing). That case says so and offers the normal Stop
    CDC; the cause, when the worker log matched one, is on the Pipeline-health card below.
    """
    present = set(names)
    missing = [n for n in expected if n not in present]
    stopped = cdc_dead_task_names(expected, names, failed)
    if stopped:
        stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
        many = len(stopped) > 1
        failed_label = " and ".join(connector_role_label(n) for n in stopped)
        held = getattr(migration_state, "cdc_connector_diagnoses", None) or {}
        known = any(n in held for n in stopped)
        # error, not warning: a dead task blocks streaming and cut over -- the same tone as
        # the Pipeline-health notice and row for the same stop.
        render_notice(
            ui,
            tone="error",
            header=(
                "CDC is not streaming — both connectors stopped"
                if many
                else "CDC is not streaming — a connector stopped"
            ),
            body=(
                f"Both connectors exist, but the {failed_label} "
                + ("tasks have" if many else "task has")
                + " stopped, so changes are not reaching Aurora DSQL. Retry CDC leaves "
                "connectors that exist as they are, so it cannot restart "
                + ("them. " if many else "this one. ")
                + (
                    "Pipeline health below names the cause and what to do."
                    if known
                    else "No known cause has been matched in "
                    + ("their worker logs" if many else "its worker log")
                    + " yet; the "
                    f"CloudWatch log group /msk-connect/{stack_name}-cdc has the error. "
                    + (
                        # PostgreSQL (G25, like the resume card): the stack does not record
                        # whether its source connector streamed -- a task that stopped in its
                        # initial snapshot never did -- so the card shown once CDC is stopped
                        # says where the Start continues from, not this one.
                        "Once the cause is fixed, Stop CDC; the CDC step then says where "
                        "Start CDC continues from."
                        if _cdc_source_type(session) is SourceType.POSTGRES
                        else "Once the cause is fixed, Stop CDC, then Start CDC: it continues "
                        "from the recorded stream position."
                    )
                )
            ),
        )
        _render_cdc_running_actions(
            ui, migration_state, job_manager, refresh, session=session
        )
        return
    missing_label = ", ".join(connector_role_label(n) for n in missing) or "a connector"
    present_label = ", ".join(connector_role_label(n) for n in sorted(present))
    render_notice(
        ui,
        tone="warning",
        header="CDC pipeline is incomplete — not streaming",
        body=(
            f"Only {present_label} is present; {missing_label} is missing (a failed "
            "Start or a rolled-back deploy). Until both the source and sink "
            "connectors are running, changes are not being written to DSQL. Retry "
            "CDC to re-create the missing connector (the running one is kept). If "
            "Retry keeps failing (e.g. the MSK partition quota), clean up the "
            "leftover connector, or use the Danger zone below to delete the "
            "infrastructure and redeploy."
        ),
    )

    def _cleanup() -> None:
        _open_cdc_stop_dialog(
            ui, migration_state,
            lambda: _start_cdc_stop(ui, migration_state, job_manager, refresh, session=session),
            partial=True,
            session=session,
        )

    def _retry() -> None:
        # Re-run Start CDC: idempotent, so the RUNNING connector is left alone and
        # the missing one is re-created (the source's buffered events let the new
        # sink catch DSQL up, while the topics retain them).
        _start_cdc_deploy(
            ui, migration_state, job_manager, refresh,
            inventory=inventory, session=session,
        )

    with ui.row().classes("items-center gap-2 w-full"):  # type: ignore[attr-defined]
        ui.button(  # type: ignore[attr-defined]
            "Clean up leftover connector", on_click=_cleanup,
            icon="cleaning_services",
        ).props("color=grey-7 outline")
        ui.space()  # type: ignore[attr-defined]
        retry_btn = ui.button(  # type: ignore[attr-defined]
            "Retry CDC", on_click=_retry, icon="replay"
        ).props("color=primary")
        retry_btn.tooltip(
            f"Re-create the missing connector ({missing_label}); the running "
            "connector is kept. The new sink catches up from the changes buffered in the "
            "CDC topics: gapless only while the topics still retain them (their retention "
            "period on MSK)."
        )

# G16/G17 (Full load + CDC after its Full Load, before CDC first starts): the CDC step's
# copy while the table list can still change the set.
START_TOOLTIP_NARROWING = (
    "Until you start CDC, you can still change the table list: CDC does not capture a table "
    "you untick (it is left as is on Aurora DSQL), and a table you tick is loaded on the "
    "Full Load step first. Once CDC starts this set is fixed: each table's Kafka topic "
    "partitions were sized when the infrastructure was created and cannot be changed, and "
    "MSK does not reclaim that capacity.\n\n"
)
START_TOOLTIP_NARROWING_TAIL = (
    "It is drawn from the Full Load snapshot and the tables loaded under its start point "
    "since, which is what makes the handoff gapless."
)
START_TOOLTIP_NARROWING_TAIL_RESNAPSHOT = (
    "It is drawn from the Full Load snapshot and the tables loaded under its start point "
    "since."
)
START_UNCOVERED_HEADER = "Load the tables selected after the Full Load first"
# G129: Start CDC waits while a Full Load runs (a Retry or a Reload too). The load inserts only
# the rows missing on the target, so a row the source deletes while its table is copied --
# deleted by the sink before the copy wrote it -- would be written back.
START_LOAD_RUNNING_HEADER = "Start CDC waits for the Full Load to finish"
START_LOAD_RUNNING_BODY = (
    "A Full Load is running. Start CDC once it has finished: until then the CDC sink would "
    "apply changes to tables the load is still copying, and a row the source deletes while its "
    "table is copied could reach Aurora DSQL after CDC applied that delete, and then stay there."
)
DEPLOY_PENDING_EMPTY = (
    "Load the tables selected after the Full Load before deploying the CDC infrastructure: "
    "none of the selected tables is loaded under the Full Load's start point yet, and the "
    "infrastructure is sized for the tables CDC captures when it is created."
)


def full_load_running(job) -> bool:
    """Is this migration's Full Load job -- a Start, a Re-run, a Retry or a Reload -- still
    pending or running? (G129: Start CDC waits for it.) Pure."""
    return getattr(job, "status", None) in ("PENDING", "RUNNING")


def _them(names) -> "tuple[str, bool]":
    """``(bounded names, one)`` for the G17 copy."""
    from dsql_migrator.ui.cdc_dlq_gate import bounded_table_names

    return bounded_table_names(list(names)), len(names) == 1


def start_uncovered_body(names, migration_type) -> str:
    """C-START: why Start CDC waits while selected tables are not covered yet (G17)."""
    listed, one = _them(names)
    it = "it" if one else "them"
    if migration_type is MigrationType.CDC_ONLY:
        return (
            f"{listed} {'is' if one else 'are'} selected but {'was' if one else 'were'} not "
            f"loaded by this migration's Full Load, so CDC would not capture {it}. Switch the "
            f"migration type to Full load + CDC and load {it} there, or untick {it} in its "
            "table list."
        )
    return (
        f"{listed} {'is' if one else 'are'} selected but not loaded under the Full Load's "
        f"start point yet, so CDC would not capture {it}. Load {it} on the Full Load step "
        f"(Retry unfinished tables, or Re-run all tables where that step says Retry cannot "
        f"add {it}), or untick {it} in the table list, then start CDC."
    )


def will_stream_pending(names, migration_type) -> str:
    """C-WILL: the capture is empty because the selected tables are still to load (G17).

    CDC only has no Full Load step, so it names the route :func:`start_uncovered_body`
    names: the type switch. Neither promises Retry unfinished tables, which cannot add a
    MySQL table the Full Load recorded no start-point definition for."""
    listed, one = _them(names)
    it = "it" if one else "them"
    if migration_type is MigrationType.CDC_ONLY:
        return (
            f"Will stream no tables yet: {listed} {'was' if one else 'were'} not loaded by "
            "this migration's Full Load (switch the migration type to Full load + CDC and "
            f"load {it} there)."
        )
    return (
        f"Will stream no tables yet: {listed} {'is' if one else 'are'} not loaded under the "
        f"Full Load's start point yet (load {it} on the Full Load step: Retry unfinished "
        f"tables, or Re-run all tables where that step says Retry cannot add {it})."
    )


def cdc_added_notice_body(names, *, mysql: bool) -> str:
    """C-ADDED: what capturing a table added after the Full Load costs (G17; info).

    MySQL replays the table's binary log from the Full Load's start point; PostgreSQL's
    slot decodes the table only from when it joined CDC's publication (just before its
    copy) -- the copy covers the time before that. Either way the newer copy converges."""
    listed, one = _them(names)
    it = "it" if one else "them"
    captured = (
        f"{'is' if one else 'are'} captured from its start point like the other tables: CDC "
        f"replays every change made to {it} since then over the newer copy, which converges."
        if mysql
        else f"{'is' if one else 'are'} captured from the same replication slot as the "
        f"other tables: CDC streams every change made to {it} since "
        f"{'it' if one else 'they'} joined CDC's publication (at the latest just before "
        f"{'its' if one else 'their'} copy) over that copy, which converges."
    )
    return (
        f"{listed} {'was' if one else 'were'} loaded after the Full Load and {captured} "
        "A change that briefly conflicts on a unique index with a row already "
        "copied can be dead-lettered; Cut over then waits for a CHECKSUM validation. If the "
        f"CDC infrastructure was deployed before {'it was' if one else 'they were'} loaded, "
        f"{'its Kafka topic gets' if one else 'their Kafka topics get'} the infrastructure's "
        "default partition count."
        + (
            " The binary log must still hold every change since the Full Load's start point."
            if mysql
            else ""
        )
    )


def deploy_pending_hint(names) -> str:
    """C-DEPLOY: a deploy now does not size the tables still to load (G17; info)."""
    listed, one = _them(names)
    it = "it" if one else "them"
    return (
        f"{listed} {'is' if one else 'are'} not loaded yet, so this deploy does not size "
        f"{'its Kafka topic' if one else 'their Kafka topics'}: Start CDC creates {it} later "
        f"with the infrastructure's default partition count. Load {it} on the Full Load step "
        f"first (Retry unfinished tables, or Re-run all tables where that step says Retry "
        f"cannot add {it}) to size {it} too."
    )


def _gate_selection(migration_state) -> "list[str]":
    """The resolved table selection (stored and migratable), else the stored one."""
    getter = getattr(migration_state, "resolved_selection", None)
    selected = getter() if callable(getter) else None
    if selected is None:
        selected = list(
            getattr(getattr(migration_state, "selection", None), "selected_tables", None) or ()
        )
    return list(selected)


def cdc_start_uncovered(migration_state, job_manager, job) -> "list[str]":
    """The selected tables Start CDC waits for (G17, :func:`cdc_uncovered_selection` over
    the resolved selection -- else the stored one -- and the record ``job`` in hand)."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import cdc_uncovered_selection

    return cdc_uncovered_selection(
        migration_state, job_manager, _gate_selection(migration_state), job=job
    )


def cdc_deploy_pending(migration_state, job_manager, job) -> "list[str]":
    """The Full load + CDC tables a Deploy now would not size (G17: still to load under the
    Full Load's start point, :func:`fl_cdc_pending_additions`)."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import fl_cdc_pending_additions

    return fl_cdc_pending_additions(
        migration_state, job_manager, _gate_selection(migration_state), job=job
    )
WILL_STREAM_NONE = (
    "Will stream no tables: none of the tables in the Full Load's snapshot is selected. "
    "Tick at least one in the table list."
)
# The same state in CDC only, whose table list is locked (it has no Full Load step to scope),
# so the generic remedy above would be a dead end. Reached by unticking every table in Full
# load only and then switching the type: a removal no longer locks that switch (G16).
WILL_STREAM_NONE_LOCKED_LIST = (
    "Will stream no tables: none of the tables in the Full Load's snapshot is selected, and "
    "CDC only cannot change the table set. To pick them again, switch the migration type to "
    "Full load + CDC and tick them in the table list there (a table the Full Load did not "
    "load is loaded there first), then switch back here or stay in Full load + CDC."
)


def will_stream_none(migration_state, job_manager) -> str:
    """The empty-capture line/tooltip whose remedy is reachable from THIS state (G16): the
    table list while it can still change the set, else the route that can."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import table_set_editable_after_load

    return (
        WILL_STREAM_NONE
        if table_set_editable_after_load(migration_state, job_manager)
        else WILL_STREAM_NONE_LOCKED_LIST
    )
DEPLOY_EMPTY_CAPTURE = (
    "Select at least one table in the table list before deploying the CDC infrastructure: "
    "none of the tables in the Full Load's snapshot is selected, and the infrastructure is "
    "sized for the tables selected when it is created."
)


def _render_cdc_start_button(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """The 'Start CDC' button shown when infra is deployed but no connectors run."""
    job = _current_job(job_manager, migration_state.job_id)
    watermark = _cdc_watermark(job, migration_state)
    override = migration_state.cdc_start_override()
    wm_resume = (
        CdcResumePoint.from_watermark(watermark) if watermark is not None else None
    )
    # A RESTART needs no start point. When this stack has streamed before, its resume
    # offset is already committed to the source connector's offsets topic -- which is
    # pinned to a fixed name and survives a Stop (a Stop only deletes the connectors) --
    # and the seeder skips re-seeding an offset that is at/past the watermark. So
    # streaming resumes exactly where it stopped -- unless a newer point (a later Manual
    # one) moves it forward; cdc_start_resumes tells the two apart. (A Full Load that takes
    # a new start point waits for new topics on such a stack: cdc_stack_has_streamed, G25.)
    #
    # Gating `ready` on a watermark alone was wrong here, and wrong in the case that
    # matters most: the watermark is read off the Full Load JOB record, so after an app
    # restart (job record gone) or in a CDC-only session there is none -- and Start CDC
    # went disabled with "Set the CDC start point above first" even though the pipeline
    # could resume perfectly. The operator was pushed toward re-entering coordinates by
    # hand, or worse, re-running the Full Load, to recover something the connector had
    # not lost.
    _seed, _seed_origin = _cdc_start_seed(
        migration_state, watermark, _cdc_source_type(session)
    )
    resumes_from_offset = cdc_start_resumes(
        migration_state, _seed, _cdc_source_type(session)
    )
    # G25 (A2): a MySQL Start that would move a stack that has streamed forward, past where it
    # last started (a detour through Full load only, a later Manual point) -- refused.
    _newer = newer_start_point_refusal(
        migration_state, _seed, _cdc_source_type(session), origin=_seed_origin,
        job_watermark=watermark,
    )
    # G137: a Manual point later than the Full Load's, while CDC captures a table it read in
    # parts -- refused.
    _split_refusal = split_start_point_refusal(
        migration_state, job, _seed, _cdc_source_type(session), origin=_seed_origin,
        resumes=resumes_from_offset, job_manager=job_manager,
    )
    # Why the last run's connector stopped, kept across Stop CDC (the runbook's first step)
    # until the next Start: "Ready to resume" alone would invite the Start that stops the
    # same way. A past event the operator can act on, not a block -> warning.
    _stack = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    _last_causes = getattr(migration_state, "cdc_connector_diagnoses", None) or {}
    for _diagnosis in _last_causes.values():
        render_notice(
            ui,
            tone="warning",
            header=f"The last CDC run stopped: {_diagnosis.headline}",
            body=(
                f"{_diagnosis.guidance} The full error is in the CloudWatch log group "
                f"/msk-connect/{_stack}-cdc."
            ),
        )
    # A PostgreSQL start that RE-SNAPSHOTS needs no prior start point: it is a resolved
    # start on its own (snapshot.mode=initial, the connector creates its own slot). Keyed on
    # that FACT, not on the radio value it used to read (`is_pg and mode == "manual"`).
    #
    # That old clause made the radio load-bearing for a state it has no business deciding:
    # a PostgreSQL CDC-only start, or any PG session whose job record was lost to a restart,
    # was disabled under Automatic with "Set the CDC start point above first" and enabled
    # under Manual -- while BOTH modes produce the identical `initial`/`filtered` config. So
    # it was a FALSE block whose only cure was an undiscoverable radio click, and the card
    # one screen up already badged the same state "Ready". Keying on the effective mode makes
    # PG ready by construction (pg_snapshot_mode returns `never` only with LSN+slot, which
    # the can_resume_from_lsn disjunct already covers, and `initial` otherwise), which is
    # what lets the PG radio go away entirely.
    is_pg = _cdc_source_type(session) is SourceType.POSTGRES
    mode = migration_state.cdc_start_mode()
    # Whether THIS start re-snapshots. Two claims below are false when it does: that Start
    # CDC "begins streaming" in "a few minutes", and that the table set "is what makes the
    # handoff gapless". Computed here so both read the same predicate.
    _resnapshots = pg_start_resnapshots(
        watermark, source_type=_cdc_source_type(session), start_mode=mode
    )
    # A MySQL Manual start is ready only on a SEEDABLE point (binlog file:position) -- the
    # offset Start seeds is keyed on it (cdc_seed_watermark). A GTID set alone cannot seed
    # it, and in Manual the job watermark is NOT a fallback, so neither may enable Start
    # there. (Whether a gtid_mode=ON source can start from the point is Start's GTID check.)
    manual_mysql = not is_pg and mode == "manual"
    ready = (
        resumes_from_offset
        or (override is not None and override.can_seed_offset())
        # The AUTOMATIC gapless handoff resumes from the Full Load watermark. MySQL seeds
        # the connect-offset from a binlog file:position (can_seed_offset); PostgreSQL
        # resumes from the logical-replication slot's WAL LSN (can_resume_from_lsn). Accept
        # EITHER so a PostgreSQL Full Load watermark (a WAL LSN, no binlog/GTID) enables
        # Start CDC. Gate MySQL on can_seed_offset() -- NOT has_coordinates() -- because a
        # GTID-only watermark (SHOW MASTER STATUS needs REPLICATION CLIENT, commonly
        # restricted on RDS, while @@GLOBAL.gtid_executed is a plain read) has no file:pos
        # to seed: auto mode would then silently resume from the CURRENT binlog and lose
        # every change made during the load. Such a watermark must steer the operator to
        # Manual (or grant REPLICATION CLIENT), so it must NOT enable auto Start.
        or (
            not manual_mysql
            and wm_resume is not None
            and (wm_resume.can_seed_offset() or wm_resume.can_resume_from_lsn())
        )
        or (is_pg and _resnapshots)
    )
    # G16: every table of the Full Load's snapshot unticked -- nothing would stream, so no
    # "Ready to ..." claim: the WILL_STREAM_NONE line below and the disabled Start say so.
    _empty_capture = cdc_capture_names(migration_state, watermark) == frozenset()
    # G17: a selected table the watermark does not cover yet (selected after the Full Load,
    # not loaded under its start point): Start waits for it -- CDC would not capture it, and
    # once CDC starts the set is fixed.
    _uncovered = cdc_start_uncovered(migration_state, job_manager, job)
    _migration_type = getattr(migration_state, "migration_type", None)
    # G129: a Full Load (a Retry or a Reload too) is still copying: Start waits for it.
    _load_running = full_load_running(job)
    # A restart is a materially different operation from a first start -- it resumes an
    # existing position rather than establishing one -- so it must not be described with
    # the first-start copy. Saying only "begins streaming" left the operator to guess
    # whether stopping had cost them their place (and the answer, "no", is the one thing
    # they need before pressing this).
    if _load_running:
        # A normal state, not a failure: Start becomes available when the load ends.
        render_notice(
            ui, tone="info", header=START_LOAD_RUNNING_HEADER, body=START_LOAD_RUNNING_BODY
        )
    elif _empty_capture:
        # Nothing would stream (G16; G17: or only tables still to load are selected): the
        # source config card above says why, once -- the Will stream line below and the
        # disabled Start say it here.
        pass
    elif _uncovered:
        # A captured table beside one still to load: this card is the only place that says
        # Start waits for it.
        render_notice(
            ui,
            tone="warning",
            header=START_UNCOVERED_HEADER,
            body=start_uncovered_body(_uncovered, _migration_type),
        )
    elif _newer is not None:
        # G25 (A2): no "Ready to start CDC ... begins streaming" for a start Start CDC refuses.
        render_notice(
            ui, tone=_newer[0][0], icon=_newer[0][1], header=_newer[0][2], body=_newer[0][3]
        )
    elif _split_refusal:
        # G137: the same for a start that would leave a table read in parts torn.
        render_notice(ui, tone="error", header=SPLIT_READ_HEADER, body=_split_refusal)
    elif is_pg and (resumes_from_offset or _pg_start_rolled_back(migration_state)):
        # PostgreSQL: worded like the start line (G25) -- the stack records that its
        # connectors were started, not that its source connector streamed. A Start whose
        # update CloudFormation rolled back is one too: its source connector may have
        # recorded a position Debezium continues from, so no snapshot is promised there.
        _rolled_back = _pg_start_rolled_back(migration_state)
        _continues = _pg_resume_continues(
            _pg_start_slot(migration_state, watermark),
            snapshot_never=not _resnapshots,
            rolled_back=_rolled_back,
            cdc_only=_migration_type is MigrationType.CDC_ONLY,
            # Stopped before a Start that rolled back: the retention bound counts from the
            # Stop (G25).
            stopped=bool(getattr(migration_state, "cdc_has_committed_offset", False)),
        )
        render_notice(
            ui,
            tone="info",
            icon="restart_alt",
            header="Ready to start CDC again" if _rolled_back else "Ready to resume CDC",
            body=(
                f"{_continues} No start point needs re-entering. The connectors take a few "
                "minutes; progress appears below."
                if not _last_causes
                else f"{_continues} The last run stopped for the cause above: follow its "
                "steps before Start CDC — for some causes a Start from the recorded position "
                "stops the same way."
            ),
        )
    elif resumes_from_offset:
        render_notice(
            ui,
            tone="info",
            icon="restart_alt",
            header="Ready to resume CDC",
            body=(
                # G25: no unconditional "no gap" -- Stop CDC removed the sink without a
                # drain, so what it had not applied comes only from the CDC topics.
                "This pipeline streamed before and its resume position is still recorded "
                "on MSK — stopping CDC removed only the connectors, not that position. "
                "Start CDC re-creates them and continues from exactly where streaming "
                "stopped, with no re-load and no need to re-enter a start point. "
                f"{_MYSQL_TOPICS_CONDITION} It takes a few minutes; progress appears below."
                if not _last_causes
                # A held stop cause qualifies the promise: for the replay causes (an
                # unparseable DDL, a schema-history replay) a Start from the recorded
                # position stops the same way, and the notice above says what to do. Not
                # "streamed before" either: a stop in the schema snapshot never streamed.
                else "This pipeline's resume position is still recorded on MSK — stopping "
                "CDC removed only the connectors, not that position. The last run stopped "
                "for the cause above: follow its steps before Start CDC — for some causes "
                "a Start from the recorded position stops the same way. "
                f"{_MYSQL_TOPICS_CONDITION}"
            ),
        )
    elif not is_pg and cdc_start_rolled_back(migration_state) and not _resnapshots:
        # MySQL (G25): a retry after a Start CloudFormation rolled back -- worded like
        # PostgreSQL's: gapless only while the CDC topics retain what that Start streamed.
        from dsql_migrator.ui.cdc_start_point_gate import new_topics_route

        render_notice(
            ui,
            tone="info",
            icon="restart_alt",
            header="Ready to start CDC again",
            body=(
                "CloudFormation rolled back this stack's last Start, which removed its "
                "connectors. Start CDC creates them again and "
                f"{_mysql_rolled_back_continues('the start point above')} (their retention "
                "period on MSK). Past that, a new start point needs new CDC topics: "
                + new_topics_route(_migration_type)
                + (
                    ". The connectors take a few minutes; progress appears below."
                    if not _last_causes
                    else ". The last run stopped for the cause above: follow its steps "
                    "before Start CDC — for some causes a Start from the recorded position "
                    "stops the same way."
                )
            ),
        )
    else:
        render_notice(
            ui,
            tone="info",
            icon="rocket_launch",
            header="Ready to start CDC",
            body=(
                "Infrastructure is deployed. Start CDC creates the connectors for your "
                "selected tables (source first, then sink) and begins streaming. It "
                "takes a few minutes; progress appears below."
                if not _resnapshots
                # "begins streaming ... a few minutes" is the sentence that seals the
                # assumption: it omits the snapshot phase entirely, so an operator on the
                # re-snapshot route expects incremental changes within minutes and instead
                # gets a full re-copy of the dataset. The connectors really do appear in
                # minutes -- what follows them does not.
                else "Infrastructure is deployed. Start CDC creates the connectors for "
                "your selected tables (source first, then sink) in a few minutes — but "
                "this start has nothing to resume from, so the source connector then "
                "takes a FRESH SNAPSHOT of every selected table before any change "
                "streams. Progress appears below."
            ),
        )

    # Show WHICH tables will stream right here, so the "pick your tables first"
    # advice is verifiable at a glance instead of asking the user to scroll up.
    cdc_tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    table_names = [t.name for t in cdc_tables]
    # G17: captured tables a guarded load added after the Full Load, before CDC ever started
    # -- what capturing them from the original start point costs, said once (info).
    _added_after = set(getattr(watermark, "cdc_added_tables", None) or ())
    _added_captured = [n for n in table_names if n in _added_after]
    # Not on a start that re-snapshots every table (said in "Ready to start CDC" above):
    # nothing is replayed over a copy there, which is what this notice is about.
    if _added_captured and not _resnapshots:
        from dsql_migrator.ui.data_migration._cdc_monitoring import _cdc_started_for_migration

        if not _cdc_started_for_migration(migration_state, job_manager):
            render_notice(
                ui,
                tone="info",
                header="Tables added after the Full Load",
                body=cdc_added_notice_body(
                    _added_captured,
                    mysql=_cdc_source_type(session) is not SourceType.POSTGRES,
                ),
            )
    if table_names:
        preview = ", ".join(table_names[:6]) + (
            f" +{len(table_names) - 6} more" if len(table_names) > 6 else ""
        )
        noun = "table" if len(table_names) == 1 else "tables"
        selection_line = f"Will stream {len(table_names)} {noun}: {preview}"
    elif _empty_capture:
        # G16: every table of the Full Load's snapshot was unticked -- nothing streams (G17:
        # or only tables still to load under its start point are selected).
        selection_line = (
            will_stream_pending(_uncovered, _migration_type)
            if _uncovered
            else will_stream_none(migration_state, job_manager)
        )
    else:
        selection_line = (
            "Will stream all tables covered by the Full Load snapshot / your "
            "current selection."
        )

    # State the CONFIRMED table set and the remedy that ACTUALLY works -- do NOT advise
    # "pick all your tables up front". By the time this button renders the set is
    # frozen against ADDITIONS: the button only appears for card phase "infra", which
    # requires a probed cdc_stack_phase of "infra", which makes cdc_infra_prep_state()
    # "ready", which is exactly what selection_lock_reason's CDC clause locks the picker
    # on (the CDC-bearing types are the only ones with a "cdc" sub-step). Telling the
    # operator to choose everything up front was advice they could not act on -- the
    # checkboxes were disabled while the tip pointed at them. The one exception is Full
    # load + CDC after its Full Load, before CDC first starts (G16, G17): the picker can still
    # change the set there (a ticked table the Full Load did not load is loaded under its
    # start point first), so the tooltip says so.
    #
    # The remedy depends on WHICH lock applies, and the two are not interchangeable:
    #   * Full Load already ran -> its clause wins and is NOT released by deleting the
    #     CDC stack (the export ran against this set; that fact is permanent). Once CDC has
    #     started, only Start over clears it for an added table. Saying "delete the CDC
    #     infrastructure to re-scope" here would send the operator through a ~45 min
    #     teardown that leaves the picker exactly as locked as before.
    #   * CDC-only (no Full Load) -> the CDC clause is the only one, and deleting the
    #     infrastructure genuinely does release it.
    # So mirror selection_lock_reason's own precedence rather than assuming one exit.
    # (Full load only lifts the Full Load clause when no CDC stack exists, G12 -- but it
    # has no CDC sub-step, so this card never renders for it.)
    full_load_ran = _full_load_committed(job, migration_state)
    # G16/G17: a Full load + CDC set that may still change (CDC never started for it).
    narrowing = (
        getattr(migration_state, "migration_type", None) is MigrationType.FULL_LOAD_AND_CDC
        and table_set_editable_after_load(migration_state, job_manager)
    )
    started_before = bool(getattr(migration_state, "cdc_connector_names", None))
    if started_before:
        # A REAL caution: repeated start/stop has already begun consuming MSK's
        # non-reclaimed partition capacity, and enough cycles wedge the cluster into a
        # delete-and-redeploy. Worth its own amber box.
        render_notice(
            ui,
            tone="warning",
            icon="warning",
            header="Re-starting CDC uses more MSK capacity each time",
            body=(
                f"{selection_line} Each restart re-creates connectors, and MSK's "
                "limited capacity isn't freed up between runs — so repeated "
                "start/stop cycles can eventually require deleting and redeploying "
                "the CDC infrastructure. Check the set above is the one you want "
                "before starting again."
            ),
        )
    else:
        # On the FIRST start this is not a warning at all -- it is the "which tables?"
        # answer plus a fact about why it is fixed. It used to be a second full-width
        # blue box directly under "Ready to start CDC", which gave a normal happy-path
        # state two equal-weight notices and buried the one line the operator actually
        # scans for (WHICH tables will stream) inside a paragraph about MSK partition
        # accounting. So: the table set is plain text (the verifiable fact, kept visible
        # -- it must not become hover-only), and the immutability rationale moves to an
        # info glyph beside it, since it is background the operator needs at most once.
        with ui.row().classes("items-center gap-1.5 no-wrap w-full"):  # type: ignore[attr-defined]
            ui.icon("playlist_add_check", color="primary").classes("text-base")  # type: ignore[attr-defined]
            ui.label(selection_line).classes("text-sm text-gray-700")  # type: ignore[attr-defined]
            ui.icon("info_outline").classes(  # type: ignore[attr-defined]
                "text-gray-400 text-sm cursor-help shrink-0"
            ).tooltip(
                (
                    START_TOOLTIP_NARROWING
                    + (
                        # The gapless tail only where the handoff IS gapless: not on a
                        # re-snapshot, and not (G27) after a Full Load that may not be this
                        # migration's first copy -- the card states the condition there.
                        START_TOOLTIP_NARROWING_TAIL
                        if not _resnapshots
                        and not drop_reload_condition_due(migration_state, job)
                        else START_TOOLTIP_NARROWING_TAIL_RESNAPSHOT
                    )
                )
                if narrowing
                else "This set is fixed. Each table's Kafka topic partitions were sized when "
                "the infrastructure was created and cannot be changed, and MSK does not "
                "reclaim that capacity.\n\n"
                + (
                    # Only assert gaplessness when this start actually IS gapless. Gated on
                    # full_load_ran alone, this told the operator in the tool's own voice
                    # that the handoff was gapless on the one route that re-snapshots -- and
                    # (G27) after a Full Load that is not this migration's first copy.
                    "It is drawn from the Full Load snapshot, which is what makes the "
                    "handoff gapless — streaming a different set means a fresh migration, "
                    "via 'Start over' (top right)."
                    if full_load_ran
                    and not _resnapshots
                    and not drop_reload_condition_due(migration_state, job)
                    else "It is drawn from the Full Load snapshot — streaming a different "
                    "set means a fresh migration, via 'Start over' (top right)."
                    if full_load_ran
                    else "To stream a different set, delete the CDC infrastructure below "
                    "and deploy it again for the tables you want."
                )
            )

    # _open_cdc_start_dialog is async (it runs the read-only binlog-retention
    # pre-flight via run.io_bound), so it MUST be awaited -- otherwise the dialog
    # never opens and "Start CDC" appears to do nothing.
    async def _confirm() -> None:
        await _open_cdc_start_dialog(
            ui, migration_state,
            lambda: _start_cdc_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
            job_manager=job_manager,
            inventory=inventory,
            refresh_after_drop=refresh,
        )

    # Explicit CDC-prerequisite gate, independent of the sub-step ordering: a
    # source whose binary log is not ROW/FULL can never be streamed, and creating
    # connectors against it burns ~26 min of billable create before failing with an
    # undiagnosed error. Checked here as well as before the infra deploy, because a
    # session can reach Start CDC on already-deployed (or adopted) infrastructure
    # without having passed through the deploy action.
    prereq_block = cdc_prerequisite_block_reason(
        migration_state.get_prereq_report(MigrationMode.CDC),
        cdc_checks_already_passed=(
            getattr(migration_state, "prereq_gated_mode", None) is MigrationMode.CDC
        ),
    )
    if prereq_block:
        render_notice(
            ui,
            tone="warning",
            header=cdc_prerequisite_block_header(
                migration_state.get_prereq_report(MigrationMode.CDC),
                cdc_checks_already_passed=(
                    getattr(migration_state, "prereq_gated_mode", None)
                    is MigrationMode.CDC
                ),
            ),
            body=prereq_block,
        )

    # The capture exclusions Start would send: refused when they cannot be carried (a
    # comma in a name, over CloudFormation's 4,096-byte parameter) -- say so before the
    # click -- and flagged when a stack kept on the 0.1.561 form never matched some names.
    _captured_now = _cdc_tables_for_config(migration_state, inventory, watermark)
    _capture = cdc_capture_exclusions(migration_state, inventory, _captured_now)
    _capture_error = (
        capture_error_with_remedy(
            migration_state, job_manager, _capture,
            table_lock=_table_lock(migration_state, job_manager, session),
        )
        if _capture.error
        else capture_exclusion_mismatch(migration_state, inventory, _capture, _captured_now)
    )
    # The table lists Start sends (TableIncludeList, SinkTopics) must each fit one stack
    # parameter too -- also said before the click.
    _tables_refusal = cdc_table_list_refusal([t.name for t in _captured_now], deploy=False)
    if _tables_refusal:
        _tables_refusal = _table_list_hold_remedy(
            _tables_refusal, migration_state, job_manager,
            _table_lock(migration_state, job_manager, session),
        )
    # A source column the target certainly lacks: the sink would dead-letter every change
    # to it (42703). Checked HERE as well as at Deploy, because Attach (which moves no data)
    # does not check it and the DDL can change between Deploy and Start. Not tied to any
    # lock: its remedy is Schema Conversion; it is skipped once CDC streams.
    _missing = missing_column_block_reason(
        migration_state, inventory, job_manager=job_manager
    )
    if _missing and not prereq_block and not _capture_error and not _tables_refusal:
        render_notice(
            ui,
            tone="error",
            header="A source column is missing from the Aurora DSQL table",
            body=_missing,
        )
    if _capture_error and not prereq_block:
        render_notice(
            ui,
            tone="error",
            header="The excluded columns cannot be sent to CDC",
            body=_capture_error,
        )
    elif _tables_refusal and not prereq_block:
        render_notice(ui, tone="error", header=CDC_TABLE_LIST_HEADER, body=_tables_refusal)
    _ambiguity = capture_ambiguity_warning(_capture)
    if _ambiguity:
        render_notice(ui, tone="warning", header="CDC capture needs attention", body=_ambiguity)
    if _capture.inexact:
        _names = ", ".join(_capture.inexact[:5]) + (
            f" and {len(_capture.inexact) - 5} more" if len(_capture.inexact) > 5 else ""
        )
        render_notice(
            ui,
            tone="warning",
            header="The deployed CDC stack may not exclude these columns exactly",
            body=(
                f"{_names}: the stack was deployed by a version that sent each exclusion "
                "as an unescaped pattern, and a regex character or a same-name column "
                "differing only in case makes it match nothing or more than that "
                "column. It is kept as deployed so a running pipeline is not changed. "
                "To match exactly, delete the CDC infrastructure and deploy it again."
            ),
        )

    start_btn = ui.button(  # type: ignore[attr-defined]
        "Start CDC", on_click=_confirm, icon="play_arrow"
    ).props("color=primary")
    if prereq_block:
        start_btn.props("disable")
        start_btn.tooltip(prereq_block)
    elif _load_running:
        # G129: the notice above says why; Start comes back when the load ends.
        start_btn.props("disable")
        start_btn.tooltip(START_LOAD_RUNNING_BODY)
    elif _capture_error:
        start_btn.props("disable")
        start_btn.tooltip(_capture_error)
    elif _tables_refusal:
        start_btn.props("disable")
        start_btn.tooltip(_tables_refusal)
    elif _missing:
        start_btn.props("disable")
        start_btn.tooltip(_missing)
    elif _uncovered:
        # G17: a selected table not loaded under the Full Load's start point yet.
        start_btn.props("disable")
        start_btn.tooltip(start_uncovered_body(_uncovered, _migration_type))
    elif _empty_capture:
        # Said before the click: Start would refuse an empty capture only after the dialog's
        # probes, in a notice the closing dialog takes with it.
        start_btn.props("disable")
        start_btn.tooltip(will_stream_none(migration_state, job_manager))
    elif _newer is not None:
        # G25 (A2): the notice above says why, and the route.
        start_btn.props("disable")
        start_btn.tooltip(_newer[1])
    elif _split_refusal:
        # G137: the notice above says why, and the route.
        start_btn.props("disable")
        start_btn.tooltip(_split_refusal)
    elif not ready:
        start_btn.props("disable")
        ui.label(  # type: ignore[attr-defined]
            "Set the CDC start point above first."
        ).classes("text-xs text-gray-500")

def _render_cdc_running_actions(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """The 'Stop CDC' action shown while connectors are running."""
    # No always-on blurb here: that the pipeline is streaming is already clear from
    # the live status right below, and the "Stop removes only the connectors, infra
    # is kept" impact is spelled out in the confirmation dialog (and the button
    # tooltip). Keeping it as static text just clutters the running view.
    def _confirm() -> None:
        _open_cdc_stop_dialog(
            ui, migration_state,
            lambda: _start_cdc_stop(ui, migration_state, job_manager, refresh, session=session),
            session=session,
        )

    ui.button(  # type: ignore[attr-defined]
        "Stop CDC", on_click=_confirm, icon="stop_circle"
    ).props("color=amber outline").tooltip(
        "Removes only the connectors — MSK, the VPC wiring and the plugins are "
        "kept, so you can Start CDC again quickly."
    )

def _cdc_prep_ready_badge(migration_state) -> tuple[str, str]:
    """Badge for the prep section's ``ready`` state, honouring the raw stack status.

    ``ready`` folds every non-stable status, so a flat ("Ready", "positive") badge
    labelled a deleting or failed stack green. Derives the badge from the same pure
    helper the CDC card uses, so the two surfaces cannot disagree again.
    """
    status = getattr(migration_state, "cdc_stack_phase_status", None)
    if status and not is_stable_stack_status(status):
        badge_text, _tone, _header, _body = cdc_unstable_message(status)
        return (badge_text, "warning")
    return ("Ready", "positive")


def cdc_infra_prep_state(migration_state, job_manager) -> str:
    """Classify the CDC-infrastructure situation for the Prerequisites-step section.

    Returns one of:

    * ``"deploying"`` -- an infra create is in flight (show live progress only).
    * ``"ready"``     -- a cdc-stack already exists (deployed earlier / adopted), so
      there is nothing to deploy here; Start CDC happens on the CDC sub-step.
    * ``"adopt"``     -- CDC infrastructure exists under a name this session does not
      target, so offer to attach instead of paying for a second MSK cluster.
    * ``"deploy"``    -- nothing exists yet: offer the BYO-VPC form + deploy.
    * ``"unknown"``   -- the account probe has not reported yet; render nothing rather
      than briefly showing a deploy form that could duplicate an existing pipeline.

    Pure (reads already-populated state; no AWS I/O) so it is safe during render.
    """
    if cdc_infra_deploy_in_flight(migration_state, job_manager):
        return "deploying"
    phase = getattr(migration_state, "cdc_stack_phase", None)
    if phase in ("infra", "running", "unstable", "provisioning", "partial"):
        return "ready"
    # Gate on the probe having actually reported: ``cdc_other_stacks`` is only
    # meaningful once the account-wide discovery ran. Showing a fresh-deploy form
    # before then risks a duplicate (billable) MSK cluster.
    if not getattr(migration_state, "cdc_stack_phase_checked", False):
        return "unknown"
    if getattr(migration_state, "cdc_other_stacks", None):
        return "adopt"
    return "deploy"


def _render_cdc_infra_prep_section(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """An EXTRA CDC-infrastructure entry point, at the bottom of Prerequisites.

    Not the only way to provision: the CDC step's lifecycle card
    (:func:`_render_cdc_start_action`) already renders the same BYO-VPC deploy form (or
    the adopt choice) whenever the stack is absent. This one exists purely so the
    ~5 min MSK create can be started EARLY and OVERLAP the Full Load, instead of
    waiting until the operator reaches the CDC step after the load. It also needs a real
    table set (the connector's ``TableIncludeList`` and the topic partition plan), and
    Prerequisites is the first point where both hold: running the checks pins and locks
    the confirmed selection, and that sub-step still precedes Full Load. Beside the
    migration-type tiles the picker is typically untouched, so the table set would
    resolve to "none".

    **Rendered only for types that HAVE a Full Load to overlap.** For CDC only the
    caller suppresses it: with no Full Load there is nothing to overlap, so it would be
    a second copy of the CDC step's own form on the same screen. Do not add a call to
    this from the CDC step -- that duplicates a billable deploy form.

    Scoped to the first-deploy affordance only: Start CDC, monitoring, Stop and Delete
    stay on the CDC sub-step, which remains reachable with no infrastructure deployed.
    """
    prep = cdc_infra_prep_state(migration_state, job_manager)
    if prep == "unknown":
        return

    ui.separator()  # type: ignore[attr-defined]
    section_header(
        ui,
        icon="cloud_upload",
        title="CDC streaming infrastructure",
        badge=(
            ("Deploying…", "primary") if prep == "deploying"
            else _cdc_prep_ready_badge(migration_state) if prep == "ready"
            else ("Not deployed", "grey")
        ),
    )

    if prep == "deploying":
        render_notice(
            ui,
            tone="info",
            busy=True,  # ~5 min live operation: spinner + "In progress" badge
            header="Deploying in the background — start your Full Load now",
            body=(
                "Amazon MSK takes ~5 minutes to provision. Nothing is streaming "
                "yet, so this does not hold up the snapshot: continue to Full Load "
                "and let the two run together. You can leave this screen; progress "
                "is kept and shown when you return."
            ),
        )
        _render_cdc_deploy_live(ui, migration_state, job_manager, refresh)
        return

    if prep == "ready":
        stack = getattr(migration_state, "cdc_stack_name", "the cdc-stack")
        # "ready" is a fold: EVERY non-stable CloudFormation status lands here, because
        # only CREATE_COMPLETE / UPDATE_COMPLETE / UPDATE_ROLLBACK_COMPLETE /
        # IMPORT_COMPLETE are in _STABLE_STACK_STATES. So a stack that is being torn down,
        # or parked in ROLLBACK_COMPLETE / CREATE_FAILED / DELETE_FAILED, was announced in
        # a green success box as "already deployed, so there is nothing to provision" --
        # while the CDC card one sub-step below, reading the SAME state, correctly said
        # "CDC infrastructure is being deleted". One screen contradicting itself.
        #
        # The fold itself is right (hiding the deploy affordance is correct: CreateStack
        # against a same-named deleting stack raises AlreadyExistsException), so only the
        # badge / tone / copy change, from the raw status through the same pure helper the
        # CDC card uses. No new action here -- Delete already lives one sub-step below.
        _raw_status = getattr(migration_state, "cdc_stack_phase_status", None)
        if _raw_status and not is_stable_stack_status(_raw_status):
            _badge, _tone, _header, _body = cdc_unstable_message(_raw_status)
            render_notice(ui, tone=_tone, header=_header, body=_body)
            # Re-poll while the status can still change on its own, so the section does
            # not sit on a stale in-flight message until something else refreshes. A
            # terminal status (DELETE_FAILED, ROLLBACK_COMPLETE) needs an operator
            # action, so arming a timer for it would poll forever for nothing.
            if _is_inflight_stack_status(_raw_status) and refresh is not None:
                ui.timer(  # type: ignore[attr-defined]
                    _CDC_POLL_INTERVAL_SECONDS, refresh, once=True
                )
            return
        # "after the Full Load" is false for CDC only -- there is no Full Load in that
        # plan, and the operator's next action is Start CDC on the (now-expanded) CDC
        # step. Naming a step that does not exist reads as a missing prerequisite.
        from dsql_migrator.ui.data_migration._models import MigrationType

        _cdc_only = (
            getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY
        )
        _next_step = (
            "You start streaming with Start CDC on the CDC step below."
            if _cdc_only
            else "You start streaming on the CDC step after the Full Load."
        )
        render_notice(
            ui,
            tone="success",
            header="CDC infrastructure is ready",
            body=(
                f"'{stack}' is already deployed, so there is nothing to provision "
                f"here. {_next_step}"
            ),
        )
        return

    if prep == "adopt":
        _render_cdc_adopt_or_deploy_choice(
            ui,
            migration_state,
            job_manager,
            refresh,
            getattr(migration_state, "cdc_other_stacks", []) or [],
            inventory=inventory,
            session=session,
        )
        return

    render_notice(
        ui,
        tone="info",
        icon="schedule",
        header="Deploy now so it is ready when the Full Load finishes",
        body=(
            "CDC needs Amazon MSK, which takes ~5 minutes to provision and bills "
            "while it exists. Deploying it here lets it run WHILE your Full Load "
            "does, instead of waiting afterwards — the snapshot is unaffected, and "
            "no data streams until you explicitly start CDC. You can also skip this "
            "and deploy later from the CDC step."
        ),
    )
    _render_cdc_infra_deploy_action(
        ui, migration_state, job_manager, refresh,
        inventory=inventory, session=session,
    )


def _render_cdc_attach_row(
    ui,
    migration_state,
    job_manager,
    refresh,
    in_scope,
    *,
    inventory=None,
    session=None,
    where: str = "in Exclude tables & columns (under the table list)",
) -> None:
    """The "Attach to <stack>" buttons -- ONE implementation for every surface offering them.

    Attaching locks the exclusion card (the stack now holds the capture list), so a saved
    exclusion that no longer applies must be removed FIRST: same gate as Deploy, shown on
    the disabled button and re-checked at click time in case the state moved since this
    render. Neither the capture-size nor the 42703 check applies here: attaching moves no
    data and changes no DDL. What Start then sends is the SESSION's set: a session with
    none takes the stack's excluded columns after the next probe
    (``adopt_deployed_exclusions_if_safe``), and Start refuses a set that differs from a
    stack that streamed (``capture_exclusion_mismatch``). ``where`` names where the card with the Remove is from THIS
    surface. The plan-level banner and the CDC step's adopt panel both call this, so they
    cannot drift apart.
    """
    _attach_block = _stored_exclusion_deploy_block(
        migration_state, job_manager, inventory, session,
        include_capture=False, include_missing=False, where=where,
    )
    if _attach_block:
        render_notice(
            ui,
            tone="error",
            header="Remove the saved column exclusions that no longer apply",
            body=_attach_block,
        )
    with ui.row().classes("items-center gap-2 flex-wrap w-full"):  # type: ignore[attr-defined]
        for name, _status in in_scope:
            def _adopt(_name=name) -> None:
                _now = _stored_exclusion_deploy_block(
                    migration_state, job_manager, inventory, session,
                    include_capture=False, include_missing=False, where=where,
                )
                if _now:
                    ui.notify(_now, type="warning", position="top")
                    return
                if migration_state.adopt_cdc_stack(_name):
                    refresh()
            _attach_btn = ui.button(  # type: ignore[attr-defined]
                f"Attach to {name}", on_click=_adopt, icon="link"
            ).props("color=primary")
            if _attach_block:
                _attach_btn.props("disable")
                _attach_btn.tooltip(_attach_block)


def _render_cdc_adopt_or_deploy_choice(
    ui, migration_state, job_manager, refresh, other_stacks, *,
    inventory=None, session=None,
) -> None:
    """CDC infra exists under a name this (reset) session does not target: offer to
    ADOPT it instead of deploying a duplicate.

    Deploying a second cdc-stack means a second, billable Amazon MSK cluster, so the
    adopt path is primary. Adoption is read/attach-only -- it re-reads the live state
    from AWS (running / provisioning / infra), so a running pipeline lands straight on
    its monitoring view; starting fresh remains the explicit Stop/Delete path, never a
    side effect of adopting. A separate fresh deploy stays reachable but de-emphasized.
    """
    # Failed / rolled-back / deleting stacks are NOT adoptable: their resources are
    # partly gone, so attaching yields a dead session -- and the urgent fact ("a
    # teardown did not finish, MSK/NAT may still be billing") was hidden behind an
    # inviting "Attach to <stack> (DELETE_FAILED)" button.
    attachable, needs_cleanup = split_attachable_stacks(other_stacks)

    if needs_cleanup:
        # A delete that is simply STILL RUNNING is not a failed one. Both are equally
        # un-attachable, so they share the bucket -- but the failure copy told the
        # operator "a previous teardown did not finish" about a teardown progressing
        # normally, and recommended 'Retain resources', which would abandon the very MSK /
        # NAT resources the delete is about to remove (the opposite of stopping the cost).
        _in_flight, _terminal = split_cleanup_by_progress(needs_cleanup)
        if _in_flight:
            _busy = ", ".join(f"{name} ({status})" for name, status in _in_flight)
            _has_seeder = _cdc_stack_has_seeder_lambda(migration_state)
            render_notice(
                ui,
                tone="warning",
                header="CDC infrastructure is still being removed",
                body=(
                    f"{_busy}. This is in progress, not stuck: a cdc-stack delete takes "
                    f"{cdc_teardown_estimate(has_seeder_lambda=_has_seeder)} because "
                    f"{cdc_teardown_reason(has_seeder_lambda=_has_seeder)}. It cannot be "
                    "attached to while it runs, and MSK / NAT billing stops when it "
                    "completes. No action is needed — you can keep working on the earlier "
                    "steps meanwhile."
                ),
            )
        if _terminal:
            stuck = ", ".join(f"{name} ({status})" for name, status in _terminal)
            render_notice(
                ui,
                tone="error",
                header=(
                    "Leftover CDC infrastructure needs cleanup — it may still be billing"
                ),
                body=(
                    f"{stuck}. A previous teardown did not finish, so this stack cannot "
                    "be used or attached to (it is partly deleted) — but its Amazon MSK / "
                    "NAT resources may still be incurring cost. Finish the delete first: "
                    "use 'Delete CDC infrastructure' below, or delete the stack in the "
                    "CloudFormation console (a DELETE_FAILED stack usually needs 'Retain "
                    "resources' on whatever is stuck)."
                ),
            )

    # Split the candidates by whether they actually cover THIS session's tables. Attaching
    # promotes Data Migration to DONE and unlocks Validation, so a pipeline streaming a
    # different table set would report the migration complete while every loaded table
    # received no ongoing changes at all. (The same guard as the plan-level banner; this
    # panel is a separate render path and had none.)
    loaded_tables = list(migration_state.selection.selected_tables)
    tables_by_stack = getattr(migration_state, "cdc_other_stack_tables", None) or {}
    in_scope: "list[tuple[str, str]]" = []
    out_of_scope: "list[tuple[str, str, list[str]]]" = []
    for name, status in attachable:
        missing = cdc_attach_scope_mismatch(
            tables_by_stack.get(name, ()), loaded_tables
        )
        (out_of_scope.append((name, status, missing)) if missing
         else in_scope.append((name, status)))

    for name, _status, missing in out_of_scope:
        listed = ", ".join(missing[:6]) + (
            f" +{len(missing) - 6} more" if len(missing) > 6 else ""
        )
        noun = "table" if len(missing) == 1 else "tables"
        render_notice(
            ui,
            tone="warning",
            header=f"{name} streams a different set of tables — not safe to attach",
            body=(
                f"It does not replicate {len(missing)} {noun} this session loaded "
                f"({listed}), so attaching would mark the migration complete while those "
                "tables received no ongoing changes. Deploy a pipeline for this table set "
                "below. That existing stack keeps billing meanwhile — delete it below if "
                "it is no longer needed."
            ),
        )

    if in_scope:
        listed = ", ".join(f"{name} ({status})" for name, status in in_scope)
        render_notice(
            ui,
            tone="warning",
            header="Existing CDC infrastructure found",
            body=(
                f"This account already has CDC infrastructure: {listed}. Deploying a "
                "new one would create a SEPARATE Amazon MSK cluster (ongoing cost). "
                "Attach to the existing pipeline instead — the tool re-reads its live "
                "state from AWS (if its connectors are already running, you go "
                "straight to monitoring)."
            ),
        )
        _render_cdc_attach_row(
            ui, migration_state, job_manager, refresh, in_scope,
            inventory=inventory, session=session,
        )

    # Present the deploy path according to whether attaching is actually an option.
    #
    # When every candidate is out of scope, deploying is not the "risky alternative" -- it
    # is the ONLY correct way forward. Keeping it collapsed behind a warning-triangle
    # expansion labelled "instead" made the right action look like the dangerous one, and
    # hid it: the operator saw a prominent blue Attach button they must not press and a
    # folded warning they should. So it renders EXPANDED, titled as the way forward, with
    # no warning glyph.
    #
    # When a candidate IS attachable, a second MSK cluster is expensive and rarely
    # intended, so the deploy stays collapsed and de-emphasised as before.
    deploy_is_the_way = bool(out_of_scope) and not in_scope
    with ui.expansion(  # type: ignore[attr-defined]
        "Deploy a CDC pipeline for this table set"
        if deploy_is_the_way
        else "Deploy a separate CDC pipeline instead",
        icon="rocket_launch" if deploy_is_the_way else "warning",
        value=deploy_is_the_way,
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}"):
        _render_cdc_infra_deploy_action(
            ui, migration_state, job_manager, refresh,
            inventory=inventory, session=session,
        )


def _render_cdc_infra_deploy_action(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """The BYO-VPC infrastructure form + 'Deploy CDC infrastructure' button."""
    ui.label(  # type: ignore[attr-defined]
        "No cdc-stack is deployed yet. Provide your VPC and the plugin/source "
        "details below, then deploy the infrastructure (MSK Serverless, the "
        "connector networking, plugins and IAM role). This takes ~5 minutes "
        "and creates billable AWS resources; connectors are created later by "
        "Start CDC."
    ).classes("text-xs text-gray-600")

    _render_cdc_least_privilege_note(ui, session=session)
    # The form renders BEFORE the button but drives its enabled state, so it flips the
    # button in place (via this holder) instead of triggering a re-render, which would
    # recreate the very field being typed in. Same approach the Connect step uses to gate
    # its Next button from live input (``ui/connect.py``: ``update_next_state``).
    _gate: dict = {"button": None, "hint": None}

    def _sync_gate() -> None:
        """Enable Deploy only once the VPC ID is filled in; else say what is missing."""
        button, hint = _gate["button"], _gate["hint"]
        if button is None or hint is None:
            return  # a prerequisite check blocks first, so there is no gate to sync
        if getattr(button, "is_deleted", False) or getattr(hint, "is_deleted", False):
            return  # the panel was rebuilt while an input handler still held these
        missing = not (migration_state.cdc_infra_inputs().get("vpc_id") or "").strip()
        button.set_enabled(not missing)
        hint.set_text(
            "Enter your VPC ID above to enable the deploy." if missing else ""
        )

    _flush_form = _render_cdc_infra_form(
        ui, migration_state, session=session, on_vpc_change=_sync_gate
    )

    async def _confirm() -> None:
        # G27: the dialog names the stack and diagnoses the subnets from the saved inputs,
        # and a click does not always blur the field typed in last -- so save the fields
        # first. A rejected suffix opens no dialog: its notice says why, the field reverted.
        if not _flush_form():
            return
        _job = _current_job(job_manager, getattr(migration_state, "job_id", None))
        _needs_objects = pg_objects_required_before_deploy(migration_state, _job)
        await _open_cdc_infra_dialog(
            ui, migration_state,
            lambda: _start_cdc_infra_deploy(
                ui, migration_state, job_manager, refresh,
                inventory=inventory, session=session,
            ),
            session=session,
            pg_objects_required=_needs_objects,
            # The gate inside needs the same inputs the connector config gets: the captured
            # table set (from the inventory + watermark) and the re-snapshot intent.
            inventory=inventory,
            job=_job,
        )

    # Explicit CDC-prerequisite gate. MSK is billable and takes ~5 min to
    # create, and a source whose binlog is not ROW/FULL can never stream -- fixing
    # it needs a parameter-group change plus a reboot on RDS. So verify that BEFORE
    # any infrastructure is paid for, rather than discovering it as an undiagnosed
    # connector failure later.
    _prereq_block = cdc_prerequisite_block_reason(
        migration_state.get_prereq_report(MigrationMode.CDC),
        # The reports are never persisted, so a reconnected Full-load-+-CDC run
        # legitimately has none. (Nothing CLEARS them either -- an earlier version of
        # this comment said the Full Load does, which was never true: the start path
        # only records prereq_gated_mode. A report therefore outlives the selection it
        # covered, which is why the run guard also checks its scope.) The run could
        # only have STARTED once the CDC-superset checks passed, and THAT is recorded
        # durably -- use it instead of re-demanding the checks.
        cdc_checks_already_passed=(
            getattr(migration_state, "prereq_gated_mode", None) is MigrationMode.CDC
        ),
    )
    deploy_btn = ui.button(  # type: ignore[attr-defined]
        "Deploy CDC infrastructure", on_click=_confirm, icon="cloud_upload"
    ).props("color=primary")
    if _prereq_block:
        deploy_btn.props("disable")
        deploy_btn.tooltip(_prereq_block)
        render_notice(
            ui,
            tone="warning",
            # The header must not say "run the checks first" once they HAVE run and one
            # failed -- which is exactly the CDC_REPLICATION_OBJECTS case.
            header=cdc_prerequisite_block_header(
                migration_state.get_prereq_report(MigrationMode.CDC),
                cdc_checks_already_passed=(
                    getattr(migration_state, "prereq_gated_mode", None)
                    is MigrationMode.CDC
                ),
            ),
            body=_prereq_block,
        )
        return
    # A saved column exclusion that no longer applies would be baked into the stack's
    # ColumnExcludeList and lock the card, so this is its last chance to be removed. (Also
    # held here: a capture list or a per-table list too long for one stack parameter.)
    _deploy_capture_now = _deploy_capture(migration_state, job_manager, inventory)
    _exclusion_notice = _deploy_block_notice(
        migration_state, job_manager, inventory, session, capture=_deploy_capture_now
    )
    if _exclusion_notice:
        deploy_btn.props("disable")
        deploy_btn.tooltip(_exclusion_notice[1])
        render_notice(
            ui, tone="error", header=_exclusion_notice[0], body=_exclusion_notice[1]
        )
        return
    # G16: every table of the Full Load's snapshot unticked -- an infrastructure sized for
    # no table. Said before the click (the click-time guard in _start_cdc_infra_deploy stays
    # the backstop); returning before the VPC gate keeps _sync_gate from enabling it. G17:
    # or only tables still to load under its start point are selected -- load them first.
    _deploy_job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    _deploy_pending = cdc_deploy_pending(migration_state, job_manager, _deploy_job)
    if cdc_capture_names(
        migration_state, _cdc_watermark(_deploy_job, migration_state)
    ) == frozenset():
        _empty_text = DEPLOY_PENDING_EMPTY if _deploy_pending else DEPLOY_EMPTY_CAPTURE
        deploy_btn.props("disable")
        deploy_btn.tooltip(_empty_text)
        inline_hint(ui, _empty_text, tone="warning")
        return
    if _deploy_pending:
        # Not blocking: the deploy sizes the tables CDC captures; these are captured once
        # loaded, on the default partition count.
        inline_hint(ui, deploy_pending_hint(_deploy_pending), tone="info")
    # Not blocking: excluded names Debezium cannot tell apart from another column.
    _ambiguity = capture_ambiguity_warning(_deploy_capture_now)
    if _ambiguity:
        render_notice(ui, tone="warning", header="CDC capture needs attention", body=_ambiguity)
    # Not blocking: with no row estimates recorded, only the click knows the partition plan
    # (it reads the source's first, and refuses before anything is created).
    _plan_warning = _deploy_partition_map_warning(migration_state, job_manager, inventory)
    if _plan_warning:
        render_notice(
            ui, tone="warning", header=CDC_TABLE_LIST_WORST_CASE_HEADER, body=_plan_warning
        )

    # VpcId is the one input the operator may still have to CHOOSE (subnets/NAT, the plugin
    # bucket, the DSQL cluster ARN, the source host and its secret are all resolved at deploy
    # time). For an RDS/Aurora source it is now PREFILLED from the source's own DBSubnetGroup
    # -- the same DescribeDBInstances response the tool already reads for the source security
    # group, so no extra API call and no extra IAM -- and the field stays editable, because
    # the CDC pipeline may legitimately run in another VPC reached by peering / Transit
    # Gateway / PrivateLink. It is still REQUIRED: a non-RDS host (self-managed PostgreSQL on
    # EC2), a cross-account endpoint or a missing rds:DescribeDBInstances leaves it blank.
    # It was validated only in the SUBMIT path: the button looked ready, the click
    # opened the confirmation dialog (which runs a network diagnosis and a cost
    # estimate), and only the final Deploy answered with an "Enter your VPC ID." toast.
    # State the requirement before the click instead.
    deploy_btn.tooltip(
        "Deploy the CDC infrastructure (MSK Serverless, networking, plugins, IAM)."
    )
    _gate["button"] = deploy_btn
    _gate["hint"] = inline_hint(ui, "", tone="info")
    _sync_gate()

def _deploy_block_notice(
    migration_state,
    job_manager,
    inventory,
    session,
    *,
    include_capture: bool = True,
    include_missing: bool = True,
    where: str = "in Exclude tables & columns (under the table list)",
    capture=None,
):
    """``(header, body)`` of what holds Deploy CDC infrastructure (or Attach), or ``None``.

    In order, each with its own header:

    * a saved exclusion that no longer applies -- judged with the LOB card's own lock
      (``exclusion_card_locked``) over the resolved tables, so an ERROR here always has an
      unlocked card offering its Remove (``where`` says where that card is from the button:
      above it on the CDC step, below the plan-level banner, on the CDC step for CDC only);
    * unless ``include_missing`` is False, a source column the target certainly lacks
      (42703) -- not tied to the lock, its remedy is Schema Conversion;
    * unless ``include_capture`` is False, capture exclusions over CloudFormation's
      parameter limit, with the remedy true for the cause and the state, then a per-table
      list the deploy sends over that limit (:func:`cdc_table_list_refusal`), its partition
      plan judged on the row estimates the Full Load recorded. With none recorded the deploy
      reads the source's at the click, so only the two lists every deploy sends are held
      here; the plan's worst case is a warning (:func:`_deploy_partition_map_warning`). Its
      remedy: :func:`_deploy_table_list_refused`.

    Attach passes ``include_missing=False, include_capture=False``: attaching moves no data
    and changes no DDL; it only locks the card, so only the saved-entry check applies.
    Start CDC carries no saved-entry gate: it is offered only once a stack exists, where
    the card is locked.
    """
    from dsql_migrator.ui.data_migration import exclusion_card_locked

    locked = exclusion_card_locked(migration_state, job_manager, session)
    stored = stored_exclusion_block_reason(
        stored_exclusion_issues(migration_state, inventory, committed=locked), where=where
    )
    if stored:
        return "Remove the saved column exclusions that no longer apply", stored
    if include_missing:
        missing = missing_column_block_reason(
            migration_state, inventory, job_manager=job_manager
        )
        if missing:
            return "A source column is missing from the Aurora DSQL table", missing
    if not include_capture:
        return None
    if capture is None:
        capture = _deploy_capture(migration_state, job_manager, inventory)
    if capture.error:
        return (
            "The excluded columns do not fit the CDC capture configuration",
            capture_error_with_remedy(
                migration_state, job_manager, capture,
                table_lock=_table_lock(migration_state, job_manager, session),
            ),
        )
    names, counts = _deploy_table_names_and_counts(migration_state, job_manager, inventory)
    refusal = cdc_table_list_refusal(names, deploy=True, row_counts_by_table=counts)
    if refusal:
        return (
            CDC_TABLE_LIST_HEADER,
            _deploy_table_list_refused(refusal, migration_state, job_manager, session),
        )
    return None


def _deploy_table_names_and_counts(migration_state, job_manager, inventory):
    """The tables a deploy captures now, and the row estimates its partition plan is sized
    from when the Full Load recorded them (``None`` when it did not: the deploy then reads
    the source's at the click)."""
    _job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    _wm = _cdc_watermark(_job, migration_state)
    captured = _cdc_tables_for_config(migration_state, inventory, _wm)
    return [t.name for t in captured], _cdc_row_counts_from_watermark(_wm, captured)


def _deploy_partition_map_warning(migration_state, job_manager, inventory):
    """The body of a warning that the deploy's partition plan may not fit, or ``None``.

    Only while no row estimates are recorded (:func:`_deploy_table_names_and_counts`): the
    plan then depends on the source's estimates, which the click reads before anything is
    created, judging the plan exactly -- so the button is not held, and the worst case is
    said here (:func:`cdc_partition_map_worst_case`). Recorded estimates are judged exactly
    by :func:`_deploy_block_notice`.
    """
    names, counts = _deploy_table_names_and_counts(migration_state, job_manager, inventory)
    worst = None if counts else cdc_partition_map_worst_case(names)
    if worst is None:
        return None
    return (
        f"{worst} No row estimates are recorded for them yet, so Deploy CDC infrastructure "
        "reads the source's when you click it, and refuses before it creates anything if "
        "its plan does not fit."
    )


def _deploy_capture(migration_state, job_manager, inventory):
    """The capture exclusions a Deploy / Start would send now."""
    _job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    return cdc_capture_exclusions(
        migration_state,
        inventory,
        _cdc_tables_for_config(
            migration_state, inventory, _cdc_watermark(_job, migration_state)
        ),
    )


def _table_lock(migration_state, job_manager, session):
    """The table picker's lock reason (late import: __init__ imports this module)."""
    from dsql_migrator.ui.data_migration import table_set_lock_reason

    return table_set_lock_reason(migration_state, job_manager, session)


def _deploy_table_list_refused(refusal: str, migration_state, job_manager, session) -> str:
    """A Deploy CDC infrastructure table-list refusal with its remedy. Before a Full load + CDC
    load, with CDC never started for this migration (the early deploy on Prerequisites), the
    routes of the Full Load card's warning (:func:`cdc_pre_load_table_list_routes`) in place of
    the refusal's own remedy, which they repeat: the load may still take every table, and the
    tables CDC does not need be unticked after it, before CDC starts (G16) -- the deploy itself
    stays refused. Otherwise the table picker's after it (:func:`_table_list_hold_remedy`)."""
    from dsql_migrator.ui.data_migration._cdc_monitoring import _cdc_started_for_migration

    lock = _table_lock(migration_state, job_manager, session)
    if (
        getattr(migration_state, "migration_type", None) is MigrationType.FULL_LOAD_AND_CDC
        and getattr(migration_state, "job_id", None) is None
        and not _cdc_started_for_migration(migration_state, job_manager)
    ):
        fact = refusal.removesuffix(CDC_TABLE_LIST_REMEDY).rstrip()
        from dsql_migrator.ui.data_migration._models import session_source_type

        return f"{fact} " + cdc_pre_load_table_list_routes(
            lock, split=mysql_reads_may_split(session_source_type(session))
        )
    return _table_list_hold_remedy(refusal, migration_state, job_manager, lock)


def _table_list_hold_remedy(refusal: str, migration_state, job_manager, lock) -> str:
    """A table-list refusal after the load with the table picker's remedy (``lock`` is its
    lock reason), as Deploy, Start CDC and Retry CDC say it. While CDC captures a table the
    Full Load read in parts that still waits for CDC's replay (G137), that table is named as
    one that cannot be deselected and Full load only is not offered
    (:func:`~dsql_migrator.ui.data_migration._models.table_list_hold_remedy`)."""
    job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    _split = _captured_split_tables(migration_state, job)
    return table_list_hold_remedy(
        refusal, lock, _split,
        split_route_view(migration_state, job_manager, job) if _split else job,
        cdc_only=_split_cdc_only(migration_state),
    )


def _table_set_open(migration_state, job_manager, session) -> bool:
    """Can the table picker change the set right now (no lock reason)? (G25) So a block names
    unticking a table only when the UI can carry it out. A lock that cannot be read counts as
    locked. Pure apart from job reads."""
    try:
        return _table_lock(migration_state, job_manager, session) is None
    except Exception:  # noqa: BLE001 - unknown: do not offer an untick that may be locked
        return False


def _stored_exclusion_deploy_block(migration_state, job_manager, inventory, session, **kwargs):
    """The body of :func:`_deploy_block_notice`, or ``None`` (the text-only form)."""
    notice = _deploy_block_notice(migration_state, job_manager, inventory, session, **kwargs)
    return None if notice is None else notice[1]


def _render_cdc_least_privilege_note(ui, *, session=None) -> None:
    """Recommend a dedicated least-privilege CDC source user before deploy.

    When the source was connected with a username/password, those exact
    credentials are stored in Secrets Manager and injected into the live Debezium
    connector. If the operator connected as root/admin, that over-privileged
    credential becomes long-lived. This collapsible note recommends a dedicated
    CDC user with only the grants Debezium needs and shows a copyable snippet
    (engine-appropriate: a MySQL user or a PostgreSQL role).
    Shown only for password auth (an SM-auth source already chose its own secret).
    """
    if getattr(session, "source_secret_id", None):
        return  # SM auth -- the customer manages their own credential.
    is_pg = _cdc_source_type(session) is SourceType.POSTGRES
    with ui.expansion(  # type: ignore[attr-defined]
        "Recommended: use a dedicated least-privilege CDC user",
        icon="security",
        value=False,
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}").props("expand-separator"):
        if is_pg:
            ui.label(  # type: ignore[attr-defined]
                "The username/password you connected with will be stored in AWS "
                "Secrets Manager and used by the CDC connector. Avoid using a "
                "superuser account: create a dedicated PostgreSQL role granted "
                "only what Debezium needs, reconnect on the Connect step as that "
                "role, then deploy. This limits blast radius if the secret is "
                "ever exposed."
            ).classes("text-xs text-gray-600")
            ui.code(  # type: ignore[attr-defined]
                "CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD '<strong-password>';\n"
                "GRANT USAGE ON SCHEMA public TO debezium;\n"
                "GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;\n"
                "CREATE PUBLICATION dbz_publication FOR TABLE <schema>.<table>;",
                language="sql",
            ).classes("w-full text-xs")
            ui.label(  # type: ignore[attr-defined]
                "REPLICATION lets Debezium read the WAL; SELECT reads table data "
                "for the initial snapshot. Debezium also needs the publication "
                "above (CREATE PUBLICATION FOR TABLE) and logical decoding enabled "
                "on the server: set wal_level=logical (on RDS/Aurora, set the "
                "rds.logical_replication=1 parameter and reboot). Scope the grant "
                "to specific schemas if your policy requires it."
            ).classes("text-xs text-gray-500")
        else:
            ui.label(  # type: ignore[attr-defined]
                "The username/password you connected with will be stored in AWS "
                "Secrets Manager and used by the CDC connector. Avoid using an "
                "admin/root account: create a dedicated MySQL user granted only what "
                "Debezium needs, reconnect on the Connect step as that user, then "
                "deploy. This limits blast radius if the secret is ever exposed."
            ).classes("text-xs text-gray-600")
            ui.code(  # type: ignore[attr-defined]
                "CREATE USER 'dsql_cdc'@'%' IDENTIFIED BY '<strong-password>';\n"
                "GRANT SELECT, RELOAD, SHOW DATABASES,\n"
                "      REPLICATION SLAVE, REPLICATION CLIENT,\n"
                "      LOCK TABLES\n"
                "  ON *.* TO 'dsql_cdc'@'%';\n"
                "FLUSH PRIVILEGES;",
                language="sql",
            ).classes("w-full text-xs")
            ui.label(  # type: ignore[attr-defined]
                "RELOAD + LOCK TABLES are used only for the consistent initial "
                "snapshot; SELECT reads table data; REPLICATION SLAVE/CLIENT read the "
                "binlog. Scope the grant to specific schemas instead of *.* if your "
                "policy requires it."
            ).classes("text-xs text-gray-500")

def _render_cdc_delete_action(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """A guarded 'Delete CDC infrastructure' (full stack teardown) action."""
    with ui.expansion("Delete CDC infrastructure", icon="delete_forever").classes(  # type: ignore[attr-defined]
        f"w-full {EXPANSION_PANEL_CLASSES}"
    ):
        inline_hint(  # type: ignore[attr-defined]
            ui,
            "Deletes the entire cdc-stack — MSK, VPC wiring, plugins, IAM role and "
            "any connectors. Use this when you are completely done with CDC, or to "
            "recover from a failed creation. This cannot be undone.",
            tone="neutral",
        )

        def _confirm() -> None:
            _open_cdc_delete_dialog(
                ui, migration_state,
                lambda: _start_cdc_delete(ui, migration_state, job_manager, refresh, session=session),
                session=session,
                job=_current_job(job_manager, getattr(migration_state, "job_id", None)),
            )

        ui.button(  # type: ignore[attr-defined]
            "Delete CDC infrastructure", on_click=_confirm, icon="delete_forever"
        ).props("color=negative outline")

# (label, state-key, placeholder/help, required?) for the Deploy-infra form.
# The form asks for ONLY the VPC id; from it the tool diagnoses egress and either
# reuses existing NAT subnets, has the stack create its own NAT, or (blocked) asks
# for the subnet override below. Everything else is auto-discovered/derived, and for an
# RDS/Aurora source the VPC id itself is PREFILLED from the source's DBSubnetGroup (shown
# with its provenance, and editable -- the pipeline may belong in another VPC). It stays in
# the form because the derivation is best-effort: a non-RDS host, a cross-account endpoint
# or a missing rds:DescribeDBInstances leaves it for the operator to supply.
# DsqlClusterArn + source host are auto-prefilled silently.
_CDC_INFRA_FIELDS: tuple[tuple[str, str, str, bool], ...] = (
    ("VPC ID", "vpc_id", "vpc-0123456789abcdef0", True),
    (
        "Advanced — connector subnet IDs (leave blank to auto-configure)",
        "connector_subnet_ids",
        "subnet-aaa,subnet-bbb",
        False,
    ),
)

def _cdc_infra_prefill(
    migration_state, session, *, lookup_arn: bool = True
) -> dict[str, str]:
    """Seed the infra form from prior input + the known source/target config.

    Previously-entered values win; otherwise we fill what the tool already knows:
    the source host from the source config, and the DSQL cluster ARN looked up
    (best effort, read-only) from the target endpoint. The VPC id is the only
    thing the customer must type; subnets, plugin bucket/keys, and (for SM auth)
    the secret are resolved at deploy time, not prefilled here.

    ``lookup_arn`` gates the one network call here (DSQL ``GetCluster`` for the
    cluster ARN). The ARN is not a displayed field -- it's only needed at submit
    -- so render passes ``lookup_arn=False`` to stay off the event loop, and the
    submit path resolves it (offloaded via ``run.io_bound``) where a brief wait
    is acceptable. This keeps a render of the CDC step from blocking every
    connected browser session on a control-plane round-trip (Fargate: one loop).
    """
    values = migration_state.cdc_infra_inputs()
    source = getattr(session, "source_config", None) if session else None
    if source is not None and not values.get("source_db_hostname"):
        host = getattr(source, "host", "")
        if host:
            values["source_db_hostname"] = host
    # DsqlClusterArn is NOT derivable from the endpoint hostname; fetch it once
    # (read-only GetCluster) and cache it so the form never asks for it.
    target = getattr(session, "target_config", None) if session else None
    if lookup_arn and target is not None and not values.get("dsql_cluster_arn"):
        endpoint = getattr(target, "cluster_endpoint", "")
        region = getattr(target, "region", None)
        aws_profile = getattr(session, "aws_profile", None)
        if endpoint:
            try:
                from dsql_migrator.core.dsql_metadata import (
                    build_dsql_client,
                    fetch_dsql_cluster_arn,
                )

                client = build_dsql_client(aws_profile, region)
                arn = fetch_dsql_cluster_arn(client, endpoint)
                if arn:
                    values["dsql_cluster_arn"] = arn
            except Exception:  # noqa: BLE001 - fall back to manual entry
                pass
    return values

def _render_cdc_infra_form(
    ui, migration_state, *, session=None, on_vpc_change=None
) -> Callable[[], bool]:
    """Render the minimal infra inputs (just VpcId + an advanced subnet override).

    Everything else is resolved at deploy time: subnets/NAT from the VPC, the
    plugin bucket/keys (uploaded), the DSQL cluster ARN + source host (prefilled),
    and the source-credentials secret (reused from the Connect Secrets-Manager
    reference, or created by the tool from the username/password used on Connect).

    Returns ``flush`` (G27): it saves each field whose value changed since this form last
    saved it, as its blur would, and returns False when the stack-name suffix was rejected
    (the field reverts and the charset notice shows). The Deploy click calls it before the
    confirm dialog is built: a click does not always blur the field typed in last, and the
    dialog then named the stack the field had before (live: "dsql-cdc-stack" after "e2e26"
    was typed). Only changed fields: Prerequisites and the CDC step can both show this form,
    and one copy's Deploy must not write its unedited values over what the other saved.
    """
    # Render must not block the event loop on a control-plane round-trip (one
    # asyncio loop serves every browser session on Fargate), so skip the DSQL
    # GetCluster ARN lookup here -- it's not a displayed field and the submit
    # path resolves it off-loop just before deploy.
    values = _cdc_infra_prefill(migration_state, session, lookup_arn=False)
    # Persist the prefill so a deploy submitted without edits still has the host.
    migration_state.set_cdc_infra_inputs(values)

    # For the flush the Deploy click runs (see the docstring): each field's save, and the value
    # this form last saved in each field (or rendered it with).
    saves: list = []
    applied: dict = {}
    with ui.expansion("Infrastructure inputs", icon="lan", value=True).classes(  # type: ignore[attr-defined]
        f"w-full {EXPANSION_PANEL_CLASSES}"
    ):
        ui.label(  # type: ignore[attr-defined]
            "Enter your VPC ID. Subnets, the plugin S3 bucket, the DSQL cluster "
            "ARN, the source host, and the source-credentials secret are all "
            "configured automatically."
        ).classes("text-xs text-gray-500")
        for label, key, placeholder, required in _CDC_INFRA_FIELDS:
            field = ui.input(  # type: ignore[attr-defined]
                label=label + (" *" if required else ""),
                value=values.get(key, ""),
                placeholder=placeholder,
            ).classes("w-full text-sm")

            # The connector must reach the source database privately, so the
            # source's own VPC (or one with private connectivity to it) is the
            # safe default.
            if key == "vpc_id":
                # Show the PROVENANCE of a derived value. Prefilling silently and
                # prefilling visibly are different things: the operator has to be able to
                # see that this came from the source and that changing it is expected (the
                # pipeline may belong in another VPC, reached by peering / Transit Gateway /
                # PrivateLink). Absent when nothing could be derived.
                _provenance = getattr(migration_state, "cdc_vpc_provenance", None)
                if _provenance and (values.get(key) or "").strip():
                    ui.label(  # type: ignore[attr-defined]
                        f"Derived from {_provenance}. Change it if the CDC pipeline "
                        "should run elsewhere."
                    ).classes("w-full text-xs text-gray-500")
                else:
                    ui.label(  # type: ignore[attr-defined]
                        "Recommended: the same VPC as your source database (or one with "
                        "private connectivity to it) — the connector must reach the "
                        "source privately."
                    ).classes("w-full text-xs text-gray-500")
                    # WHY it is not prefilled, when the tool tried and could not. Amber
                    # rather than gray: an AccessDenied here is a real, fixable gap in the
                    # deployment, and leaving it unsaid is what made the prefill look like
                    # a feature that had disappeared.
                    _note = getattr(
                        migration_state, "cdc_vpc_derivation_note", None
                    )
                    if _note:
                        ui.label(_note).classes(  # type: ignore[attr-defined]
                            "w-full text-xs text-amber-700"
                        )

            def _save(_e, k=key, f=field) -> None:
                current = migration_state.cdc_infra_inputs()
                current[k] = applied[k] = (f.value or "").strip()
                migration_state.set_cdc_infra_inputs(current)
                # VpcId gates the Deploy button, so re-check it as the value changes.
                if k == "vpc_id" and on_vpc_change is not None:
                    on_vpc_change()

            field.on("blur", _save)
            applied[key] = (values.get(key, "") or "").strip()
            saves.append((key, field, _save))
            # Also gate as the value changes, not only on blur: the user's next move
            # after entering the VPC ID is to click Deploy, and a click on a still-
            # disabled button is swallowed. on_value_change (as the Connect step uses)
            # covers a paste too, which fires no keystroke.
            if key == "vpc_id":
                field.on_value_change(_save)

        # Advanced: the cdc-stack name. The mandatory canonical "dsql-cdc-" prefix is
        # rendered INSIDE the field via Quasar's built-in `prefix` prop (baseline-
        # aligned with the typed text, like a "$" before an amount), and the user
        # types only the SUFFIX (e.g. "orders" -> dsql-cdc-orders) to run a
        # SECOND migration's CDC alongside an existing one. Editing only the suffix
        # makes it impossible to leave the cdc-stack family the deploy role
        # authorizes, so a bare "abcde" becomes the valid "dsql-cdc-abcde". (New
        # stacks always take the canonical prefix; a legacy "mysql-dsql-cdc-*" name is
        # still accepted for existing deployments -- see cdc_stack_name_is_valid.)
        name_field = ui.input(  # type: ignore[attr-defined]
            label="Advanced — CDC stack name (one per source DB)",
            value=cdc_stack_name_suffix(
                getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
            ),
            placeholder="stack",
        ).props(f'prefix="{CDC_STACK_NAME_PREFIX}"').classes("w-full text-sm")
        ui.label(  # type: ignore[attr-defined]
            "Full stack name = the fixed prefix + your suffix "
            "(e.g. dsql-cdc-orders). One stack per source DB."
        ).classes("w-full text-xs text-gray-500")

        def _current_suffix() -> str:
            return cdc_stack_name_suffix(
                getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
            )

        applied["stack_name"] = _current_suffix()

        def _save_stack_name(_e, f=name_field) -> bool:
            """Apply the typed suffix; False when it was rejected (reverted + notice)."""
            suffix = (f.value or "").strip()
            if not suffix:
                # Empty -> keep the current name; reflect its suffix back in the field.
                f.value = applied["stack_name"] = _current_suffix()
                return True
            full = build_cdc_stack_name(suffix)
            if full is not None and migration_state.set_cdc_stack_name(full):
                # normalize what's shown
                f.value = applied["stack_name"] = cdc_stack_name_suffix(full)
                return True
            # Reject: revert to the current suffix and explain the (charset) rule --
            # the prefix is already guaranteed, so only the suffix charset can fail.
            f.value = applied["stack_name"] = _current_suffix()
            ui.notify(  # type: ignore[attr-defined]
                "CDC stack name suffix may use only letters, digits and hyphens "
                "(e.g. 'orders' -> dsql-cdc-orders).",
                type="warning", position="top",
            )
            return False

        name_field.on("blur", _save_stack_name)

    def _flush() -> bool:
        for key, field, save in saves:
            if (field.value or "").strip() != applied[key]:
                save(None)
        if (name_field.value or "").strip() == applied["stack_name"]:
            return True
        return _save_stack_name(None)

    return _flush

def _render_cdc_cost_estimate(ui, *, includes_nat: bool, extra: str = "") -> None:
    """Show a ballpark hourly cost line in the deploy dialog (not a quote).

    ``extra`` appends the re-snapshot disclosure, because "what will this cost" and "how long
    will it run" are ONE decision and were two stacked boxes -- the second silently
    qualifying the first. Its presence also raises the tone: a time-based hourly figure is an
    ``info``, but "this start also re-reads the whole source" is a real, non-blocking cost
    the operator can still avoid by cancelling.
    """
    from dsql_migrator.core.cdc import estimate_cdc_hourly_cost

    est = estimate_cdc_hourly_cost(includes_nat=includes_nat)
    body = (
        f"~${est.hourly_low_usd:.2f}–${est.hourly_high_usd:.2f}/hour while deployed "
        f"({'incl.' if includes_nat else 'no'} NAT gateway). {est.caveat}"
    )
    _render_notice(
        ui,
        tone="warning" if extra else "info",
        icon="payments",
        header="Cost and how long it runs" if extra else "Estimated cost",
        body=f"{body} {extra}".strip(),
    )

def cdc_deploy_connection_blocker(session) -> Optional[str]:
    """Return why the source/target connections are not ready for a CDC deploy.

    The infra deploy needs a live TARGET (DSQL ARN is derived from it) and, for a
    username/password source, the in-memory source credentials (to create the CDC
    source secret -- the connector cannot read an in-memory password). These are
    NOT restored after an app restart (Property 7), so without this pre-check the
    user clicks Deploy, fills the dialog, hits Deploy again, and only THEN gets a
    "test the source connection first" error. Surfacing it on the dialog (with the
    Deploy button disabled) tells them up front to reconnect before starting.

    Returns ``None`` when ready; otherwise a short actionable reason. A Secrets-
    Manager-auth source needs no in-memory password (the connector reads the
    customer's secret), so only the target is required there. Pure/read-only.
    """
    if session is None:
        return "No session — open the Connect step and connect the source and target."
    if not getattr(session, "has_target", lambda: False)():
        return (
            "The target (Aurora DSQL) connection is not active. Open the Connect "
            "step and test the target connection, then return here to deploy."
        )
    # A Secrets-Manager-auth source supplies its own secret; no in-memory password.
    if getattr(session, "source_secret_id", None):
        return None
    if not getattr(session, "has_source", lambda: False)():
        return (
            "The source connection is not active. Open the Connect step "
            "and test the source connection, then return here to deploy."
        )
    if getattr(session, "source_password", None) is None:
        return (
            "Re-enter the source credentials: the in-memory password is not kept "
            "after a restart (for security), and the CDC source secret is created "
            "from it. Test the source connection on the Connect step, then deploy."
        )
    return None

async def _open_cdc_infra_dialog(
    ui, migration_state, on_confirm, *, session=None, pg_objects_required=False,
    inventory=None, job=None,
) -> None:
    """Confirm dialog before the (~5 min, billable) infrastructure create.

    When ``session`` is given and no manual subnet override is set, runs the
    read-only VPC network diagnosis and shows its outcome (reuse existing subnets,
    create a NAT gateway with the hourly-cost notice, or blocked) so the user
    consents with the network plan + cost in front of them. A pre-flight
    connection check (:func:`cdc_deploy_connection_blocker`) also runs first: if
    the source/target connections are not ready (e.g. after a restart), the dialog
    says so and disables Deploy, so the user reconnects BEFORE starting rather than
    hitting a failure mid-submit.

    The network diagnosis makes EC2 ``Describe*`` calls, so it runs off the event
    loop (``run.io_bound``) -- otherwise opening this dialog would freeze every
    browser session on Fargate (one asyncio loop) for the round-trip.
    """
    from nicegui import run

    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    conn_blocker = cdc_deploy_connection_blocker(session)
    net_message, net_kind, routed_warning = await run.io_bound(
        _diagnose_for_dialog, migration_state, session
    )
    # Checked here, not after Deploy: a VpcId that cannot reach the source fails only once
    # the MSK cluster exists, ~20 minutes in, and leaves a stack to tear down.
    # `or ("", "")`: nicegui's run.io_bound returns None once the app is stopping, and
    # unpacking that would raise out of the dialog.
    source_vpc_warning, source_inbound_hint = await run.io_bound(
        _source_vpc_warning_for_dialog, migration_state, session
    ) or ("", "")
    # Checked here, not after Deploy: a PostgreSQL cdc-stack is always created
    # SeedMode=External and is seeded BY THIS APP over MSK 9098, so a deployment that
    # cannot reach MSK -- or that the new cluster would not admit -- fails only at
    # Start CDC, after a billable MSK Serverless cluster already exists.
    # `or MskSeedAdmission()`: nicegui's run.io_bound returns None once the app is
    # stopping, and an attribute access on that would raise out of the dialog.
    seed_admission = (
        await run.io_bound(_msk_seed_admission, migration_state, session)
        or MskSeedAdmission()
    )
    # The COST gate: refuse BEFORE the ~5-minute billable MSK Serverless create when the
    # source's replication objects make Start CDC unusable. NOT "absent publication/slot" --
    # since v0.1.507 an absent publication under ``snapshot.mode=initial`` derives
    # ``publication.autocreate.mode=filtered`` and the connector's own DB user creates it, so
    # absent+absent returns NO block (executed, in the CDC-only state too). What still blocks
    # is an object that EXISTS and cannot be used -- a publication that omits a captured table
    # or narrows its publish list, a name-colliding non-logical slot -- or an absent slot a
    # ``never``-mode start would resume from. ASYMMETRIC by
    # migration type on purpose: in a Full-load-+-CDC run the objects legitimately do not
    # exist yet (the Full Load creates them), and deploying the infra first is the flow this
    # very card recommends -- so that case stays silent. It cannot strand anyone: the
    # Start-side gate RE-PROBES rather than remembering this verdict, so provisioning
    # between the two steps simply turns the block off.
    pg_infra_block: Optional[tuple] = None
    # ``pg_objects_required`` is computed by the caller (which holds job_manager) and is
    # deliberately NOT just "the type is CDC only": switching the type to Full-load-+-CDC
    # after a FINISHED Full-load-only run re-grades the existence row to SKIP, so the
    # prerequisite gate un-gates Deploy while the objects do not exist. Keying on the
    # WATERMARK instead is precise about WHEN TO PROBE: a fresh combined run has no
    # watermark (so the recommended deploy-before-load flow is untouched), a combined run
    # whose load provisioned carries a slot_name (untouched), and a load that finished
    # WITHOUT a slot is the state worth spending a source read on. It decides only whether
    # the probe RUNS, never the verdict: in that slotless state the effective mode is
    # ``initial``, so absent objects do NOT block (executed) -- the probe bites only on an
    # object that exists and cannot be used.
    if pg_objects_required:
        try:
            # Through the shared gate, so the operator's RECORDED re-snapshot decision is
            # honoured here exactly as it is at Start. Passing `[], None` with no
            # force_initial (v0.1.509) made this dialog re-block the route the tool had just
            # recommended, with a notice that is false for it, and sent the operator back to
            # "start over as Full load + CDC" -- the dead end v0.1.510 existed to remove.
            pg_infra_block = await run.io_bound(
                _pg_objects_gate_block,
                migration_state, session, inventory, _cdc_watermark(job, migration_state),
            )
        except Exception:  # noqa: BLE001 - unreadable is not absent; never block on it
            pg_infra_block = None
    # The target-foreign-key precondition, checked HERE as well as at Start. It BLOCKS at
    # Start, and it existed only there -- so a session whose preceding "Full load only" run
    # applied and validated the FKs (which that run does, by design) paid for the MSK
    # Serverless cluster first and was refused afterwards, leaving the cluster billing while
    # idle. Same probe, same copy, lower severity: at Deploy it is a sequencing warning, not
    # a block, because it is never a dead end (this migration's own FKs have a one-click
    # Remove in the Start dialog and cut over re-creates them).
    fk_preview = None
    if not conn_blocker:
        try:
            _fk_probed = await run.io_bound(
                _probe_cdc_blocking_foreign_keys,
                migration_state, session, inventory, _cdc_watermark(job, migration_state),
            )
        except Exception:  # noqa: BLE001 - advisory here; Start is the blocking gate
            _fk_probed = None
        if _fk_probed is not None and _fk_probed.blocking:
            fk_preview = _fk_probed
    # Wider than Quasar's 560px default: the blockers, cost, exclusions and capture list
    # it shows read better on fewer lines.
    with ui.dialog() as dialog, ui.card().classes("gap-2").style(  # type: ignore[attr-defined]
        "width: min(820px, 94vw); max-width: 94vw"
    ):
        ui.label("Deploy CDC infrastructure").classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            f"This creates the cdc-stack '{stack_name}' (CloudFormation): an MSK "
            "Serverless cluster, connector networking in your VPC, the plugins and "
            "an IAM role. It takes about 5 minutes and creates billable AWS "
            "resources. No connectors are created yet — run Start CDC afterwards."
        ).classes("text-sm text-gray-700")
        # BLOCKERS FIRST, and as ONE box. These four are the only conditions that grey out
        # Deploy, and they used to render in source order -- so the two that fire last sat
        # BELOW an info-toned cost estimate, i.e. the explanation for a disabled button was
        # the last thing on a scrolling dialog. Each keeps its own consequence sentence
        # verbatim, including the "would create a billable MSK Serverless cluster" clauses
        # the raw blocker strings do NOT carry.
        _blockers: list[tuple[str, str]] = []
        if conn_blocker:
            _blockers.append(("Not connected", conn_blocker))
        if net_kind == "blocked" and net_message:
            _blockers.append(("Network", net_message))
        if seed_admission.blocker:
            _blockers.append((
                "MSK access",
                seed_admission.blocker
                + " Deploying now would create a billable MSK Serverless cluster that "
                "Start CDC could never use.",
            ))
        if pg_infra_block is not None:
            _blockers.append((
                "Source replication objects",
                pg_infra_block[1]
                + " Fix this first, or start the migration over as "
                '"Full load + CDC", which creates both at the snapshot point.',
            ))
        if len(_blockers) == 1:
            _render_notice(
                ui, tone="error", icon="block",
                header=f"Deploy is blocked — {_blockers[0][0].lower()}",
                body=_blockers[0][1],
            )
        elif _blockers:
            with notice_container(
                ui, tone="error", icon="block", header="Deploy is blocked"
            ):
                for _term, _body in _blockers:
                    definition_row(ui, _term, _body)
        # PRICE AND DURATION, as ONE box. The hourly estimate and the re-snapshot disclosure
        # are the same decision -- what this deploy will cost and how long it runs -- and
        # they were two stacked boxes where the second silently qualified the first.
        #
        # The re-snapshot half is gated on the AFTER-A-LOAD causes only. A fresh
        # "Full load + CDC" deploy (the recommended flow: deploy, then load) has no watermark
        # yet and WILL be gapless, so alarming it would be a false warning on the happy path;
        # the same reasoning excludes a stand-alone CDC-only start, where nothing was read
        # here to re-read.
        _rs_reason = pg_resnapshot_reason(
            job,
            source_type=_cdc_source_type(session),
            start_mode=migration_state.cdc_start_mode(),
            migration_state=migration_state,
        )
        _rs_notice = (
            # G27 repair: the resume it names is conditional after a Full Load that is not
            # this migration's first copy.
            pg_resnapshot_notice(
                _rs_reason, brief=True,
                recopied=drop_reload_condition_due(migration_state, job),
            )
            if _rs_reason in _PG_RESNAPSHOT_AFTER_LOAD
            else None
        )
        _rs_body = ""
        if _rs_notice is not None:
            _rs_body = _rs_notice[1]
            # Size signal from the Full Load's own scan-free estimates, so the operator
            # weighs a real dataset rather than the word "every". Omitted silently when
            # the watermark carries no counts -- an invented number would be worse.
            _rs_counts = _cdc_row_counts_from_watermark(
                _cdc_watermark(job, migration_state),
                _cdc_tables_for_config(
                    migration_state, inventory, _cdc_watermark(job, migration_state)
                ),
            )
            if _rs_counts:
                _rs_body += (
                    f" That is about {sum(_rs_counts.values()):,} rows across "
                    f"{len(_rs_counts)} table(s), by the Full Load's own estimate."
                )
        # NAT base is only incurred when the stack creates its own NAT ("create");
        # reused existing subnets ("discovered") have no new NAT charge.
        _render_cdc_cost_estimate(
            ui, includes_nat=(net_kind == "create"), extra=_rs_body
        )
        # ONE box for the only two items that are the OPERATOR'S OWN WORK. Both cost the
        # same way if skipped -- the connectors are created and billed and then never stream
        # -- so they belong together, above the "what gets created" detail rather than
        # interleaved with it. The FK entry drops its mechanism paragraph ("the sink applies
        # change records across several tasks with no parent-before-child ordering...") which
        # explains WHY rather than what to do; the instruction and the consequence stay.
        _todo: list[tuple[str, str]] = []
        if fk_preview is not None:
            _todo.append(("Foreign keys on the target", _cdc_fk_deploy_body(fk_preview)))
        if source_inbound_hint:
            _todo.append(("Source database inbound rule", source_inbound_hint))
        if _todo:
            # `warning` only when a FK really is blocking: the inbound rule is very often
            # already satisfied and the tool cannot tell without reading a customer
            # resource, so on its own it stays a calm `info`.
            _todo_tone = "warning" if fk_preview is not None else "info"
            if len(_todo) == 1:
                _render_notice(
                    ui, tone=_todo_tone, icon="checklist",
                    header=f"Before the connectors can run — {_todo[0][0].lower()}",
                    body=_todo[0][1],
                )
            else:
                with notice_container(
                    ui, tone=_todo_tone, icon="checklist",
                    header="Before the connectors can run",
                ):
                    for _term, _body in _todo:
                        definition_row(ui, _term, _body)
        # ONE box for what the stack will CREATE or TOUCH in the account. These were four
        # separate notices saying four halves of the same thing, and none of them asks the
        # operator to do anything except read.
        _creates: list[tuple[str, str]] = []
        if net_message and net_kind != "blocked":
            _creates.append(("Networking", net_message))
        if routed_warning:
            _creates.append(("Subnet overlap", routed_warning))
        if source_vpc_warning:
            # Not a blocker -- peering / TGW / PrivateLink are legitimate -- but it is the
            # shape of a typo, so it keeps its own row and the box takes the warning tone.
            _creates.append(("This is not the source's VPC", source_vpc_warning))
        if seed_admission.warning:
            _creates.append((
                seed_admission.header or "MSK ingress was not registered",
                seed_admission.warning,
            ))
        elif seed_admission.note:
            _creates.append(("MSK access for the CDC seed", seed_admission.note))
        if _creates:
            _creates_tone = (
                "warning"
                if (routed_warning or source_vpc_warning or seed_admission.warning)
                else "info"
            )
            if len(_creates) == 1:
                _render_notice(
                    ui, tone=_creates_tone, icon="lan",
                    header=_creates[0][0], body=_creates[0][1],
                )
            else:
                with notice_container(
                    ui, tone=_creates_tone, icon="lan",
                    header="What this deploy creates in your VPC",
                ):
                    for _term, _body in _creates:
                        definition_row(ui, _term, _body)

        async def _go() -> None:
            dialog.close()
            # on_confirm is the async _start_cdc_infra_deploy (it offloads its AWS
            # round-trips); await it so failures surface and it isn't a no-op.
            result = on_confirm()
            if inspect.isawaitable(result):
                await result

        from dsql_migrator.ui.data_migration._models import deploy_exclusion_disclosure

        _disclosure = deploy_exclusion_disclosure(migration_state, inventory)
        if _disclosure is not None:
            _render_notice(ui, tone="info", header=_disclosure[0], body="")
            with ui.expansion("show").classes("w-full text-xs"):  # type: ignore[attr-defined]
                ui.label(_disclosure[1]).classes("text-xs font-mono wrap-anywhere")  # type: ignore[attr-defined]

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
            deploy_btn = ui.button("Deploy", on_click=_go).props("color=primary")  # type: ignore[attr-defined]
            if conn_blocker:
                # Not connected -> the deploy would fail mid-submit; block it and
                # point the user back to Connect.
                deploy_btn.props("disable")
                deploy_btn.tooltip(conn_blocker)
            elif net_kind == "blocked":
                # Cannot auto-resolve egress; block the deploy until fixed/overridden.
                deploy_btn.props("disable")
            elif seed_admission.blocker:
                # Would create a billable MSK cluster this app could never seed.
                deploy_btn.props("disable")
                deploy_btn.tooltip(seed_admission.blocker)
            elif pg_infra_block is not None:
                # CDC-only, and the source has nothing to stream from.
                deploy_btn.props("disable")
                deploy_btn.tooltip(pg_infra_block[1])
    dialog.open()

def derive_cdc_vpc_from_source(migration_state, session) -> bool:
    """Prefill the CDC VpcId from the source DB's own DBSubnetGroup. True when it changed.

    The value comes from the SAME ``DescribeDBInstances`` response the tool already reads for
    the source security group, so there is no extra API call and no extra IAM -- only more
    fields off a response it has. Best effort, exactly like the rest of ``rds_metadata``: a
    non-RDS host (self-managed PostgreSQL on EC2), a cross-account endpoint or a missing
    ``rds:DescribeDBInstances`` leaves the field blank for the operator to fill, which is the
    flow this feature had before it existed.

    Only fills an EMPTY field -- an operator's own value is never overwritten -- and records
    the provenance so the prefill is visible rather than a silent substitution. Blocking I/O:
    callers MUST run it off the event loop.
    """
    if session is None:
        return False
    host = getattr(getattr(session, "source_config", None), "host", None)
    if not host:
        return False
    fields = migration_state.cdc_infra_inputs()
    if (fields.get("vpc_id") or "").strip():
        return False  # the operator (or an earlier derivation) already supplied one
    # WHY it produced nothing, for the field's hint. Swallowing the reason is what made
    # this feature look ABSENT rather than unpermitted: on a managed deployment the read is
    # AccessDenied (the task role had no rds:DescribeDBInstances), the exception was
    # discarded, and the field simply stayed empty -- indistinguishable from "the tool never
    # tried". It works from a laptop whose credentials happen to have RDS access, which is
    # how it looked like a regression rather than a missing grant.
    migration_state.cdc_vpc_derivation_note = None
    try:
        from dsql_migrator.core.rds_metadata import (
            build_rds_client,
            fetch_source_network,
            parse_rds_region,
        )

        client = build_rds_client(
            getattr(session, "aws_profile", None), parse_rds_region(host)
        )
        info = fetch_source_network(client, host)
    except Exception as exc:  # noqa: BLE001 - best effort; the manual field remains
        text = f"{type(exc).__name__}: {exc}"
        if "AccessDenied" in text or "not authorized" in text:
            migration_state.cdc_vpc_derivation_note = (
                "This deployment cannot read the source database's network placement: its "
                "task role is missing rds:DescribeDBInstances (and, for an Aurora cluster "
                "endpoint, rds:DescribeDBClusters). Update the app stack to a template "
                "that grants them, or enter the VPC ID yourself."
            )
        else:
            migration_state.cdc_vpc_derivation_note = (
                "The source database's network placement could not be read "
                f"({type(exc).__name__}), so the VPC could not be derived — enter it "
                "yourself."
            )
        return False
    if info is None or not info.vpc_id:
        migration_state.cdc_vpc_derivation_note = (
            "The source endpoint does not resolve to an RDS/Aurora instance in this "
            "account and region (a self-managed PostgreSQL on EC2, or a cross-account "
            "endpoint), so there is no DBSubnetGroup to derive the VPC from — enter it "
            "yourself."
        )
        return False
    fields["vpc_id"] = info.vpc_id
    # The advanced subnet override gets the same treatment: the source's own subnets are
    # the overwhelmingly common answer, and leaving it blank still auto-configures.
    if not (fields.get("connector_subnet_ids") or "").strip() and info.subnet_ids:
        fields["connector_subnet_ids"] = ",".join(info.subnet_ids)
    migration_state.set_cdc_infra_inputs(fields)
    where = info.db_identifier or "the source database"
    group = f" (subnet group {info.db_subnet_group})" if info.db_subnet_group else ""
    migration_state.cdc_vpc_provenance = f"the source {where}{group}"
    return True


def _source_vpc_warning_for_dialog(migration_state, session) -> tuple[str, str]:
    """``(vpc_mismatch_warning, source_inbound_hint)`` for the deploy dialog; "" when unknown.

    Returns BOTH because they come from the SAME ``DescribeDBInstances`` response this
    already fetches and then discarded everything but the VPC id from -- so stating the
    inbound rule the operator must open on their own database costs no extra API call and no
    extra IAM. See :func:`~dsql_migrator.core.rds_metadata.source_inbound_rule_hint` for why
    the tool states that rule instead of opening or probing it.

    Checked BEFORE Deploy because a wrong VpcId is otherwise only discovered after roughly
    twenty minutes of MSK deployment, leaving a failed stack to clean up. Read-only, and
    silent whenever the answer is unknown -- a non-RDS host, a cross-account endpoint or a
    missing ``rds:DescribeDBInstances`` yields no warning rather than a false one. Blocking
    I/O: the caller runs it via ``run.io_bound`` alongside the network diagnosis.
    """
    if session is None:
        return "", ""
    source = getattr(session, "source_config", None)
    host = getattr(source, "host", None)
    if not host:
        return "", ""
    vpc_id = (migration_state.cdc_infra_inputs().get("vpc_id") or "").strip()
    if not vpc_id:
        return "", ""
    try:
        from dsql_migrator.core.rds_metadata import (
            build_rds_client,
            fetch_source_network,
            parse_rds_region,
            source_inbound_rule_hint,
            source_vpc_mismatch_warning,
        )

        client = build_rds_client(
            getattr(session, "aws_profile", None), parse_rds_region(host)
        )
        info = fetch_source_network(client, host)
        return (
            source_vpc_mismatch_warning(vpc_id, info) or "",
            source_inbound_rule_hint(info, port=getattr(source, "port", None)) or "",
        )
    except Exception:  # noqa: BLE001 - best effort; the dialog works without it
        return "", ""


def _msk_seed_admission(migration_state, session, *, vpc_id=None):
    """External-seed admission for the deploy dialog AND the create call (blocking).

    ONE decision used in two places, so the dialog cannot say "clear" while the
    submitted ``HostSubnetCidr`` is empty (or vice versa). Keyed on the existing
    :func:`_cdc_start_seed_mode` so a Lambda-seeded deploy (MySQL on Fargate/local)
    returns an all-empty verdict and behaves exactly as before -- no notice, no
    discovery, no AWS call, an empty ``HostSubnetCidr`` and therefore no ingress rule.
    An explicitly configured ``DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR`` short-circuits
    inside :func:`msk_seed_admission` before any discovery, which is what keeps the
    in-VPC EC2 host byte-identical.

    ``vpc_id`` overrides the state's own value: the submit path has already resolved
    ``fields`` (which may carry a derived VpcId the state has not been written back
    from), and the network the cdc-stack admits must be resolved against the VPC the
    create call is ACTUALLY about, not a possibly-staler copy.

    Blocking: the callers run it via ``run.io_bound``, like the network diagnosis
    beside it.
    """
    from dsql_migrator.config import load_config as _load_config
    from dsql_migrator.core.cdc import msk_seed_admission

    cfg = _load_config()
    seed_mode = _cdc_start_seed_mode(_cdc_source_type(session), cfg.cdc_seed_mode)
    if seed_mode != "external":
        return MskSeedAdmission()
    if vpc_id is None:
        vpc_id = migration_state.cdc_infra_inputs().get("vpc_id") or ""
    vpc_id = vpc_id.strip()
    lookup = None
    if not cfg.cdc_host_subnet_cidr and vpc_id:
        from dsql_migrator.core.ec2_metadata import (
            HostLookup,
            build_ec2_client,
            discover_host_network,
        )

        try:
            target = getattr(session, "target_config", None)
            lookup = discover_host_network(
                build_ec2_client(
                    getattr(session, "aws_profile", None),
                    getattr(target, "region", None) if target else None,
                ),
                vpc_id,
            )
        except Exception as exc:  # noqa: BLE001 - never break the deploy path
            # Distinguished, not collapsed: a failed build_ec2_client or a denied
            # describe used to render the SAME message as "no interface found", and
            # left no log line either -- three causes, one indistinguishable verdict.
            first = (str(exc).splitlines() or [""])[0][:160]
            _LOGGER.warning(
                "MSK seed host discovery failed for %s (%s: %s)",
                vpc_id,
                type(exc).__name__,
                first,
            )
            lookup = HostLookup(
                reason="lookup-failed", detail=f"{type(exc).__name__}: {first}"
            )
    return msk_seed_admission(
        seed_mode=seed_mode,
        cdc_seed_mode=cfg.cdc_seed_mode,
        cdc_msk_access=getattr(cfg, "cdc_msk_access", False),
        configured_host_cidr=cfg.cdc_host_subnet_cidr,
        vpc_id=vpc_id,
        lookup=lookup,
    )


def _diagnose_for_dialog(migration_state, session):
    """Return (message, kind, routed_warning) for the deploy dialog, best-effort.

    kind ∈ {"discovered","create","blocked",""}. ``routed_warning`` is a non-empty
    caution only when the VPC routes off-VPC (TGW/peering/VPN) and the stack will
    auto-carve subnets ("create" mode), else "". Empty message+kind when a manual
    subnet override is set (no diagnosis needed) or context is missing. Read-only.
    """
    if session is None:
        return "", "", ""
    fields = migration_state.cdc_infra_inputs()
    if (fields.get("connector_subnet_ids") or "").strip():
        return "using the connector subnets you provided.", "discovered", ""
    vpc_id = (fields.get("vpc_id") or "").strip()
    target = getattr(session, "target_config", None)
    region = getattr(target, "region", None) if target else None
    if not vpc_id or not region:
        return "", "", ""
    try:
        from dsql_migrator.core.ec2_metadata import (
            build_ec2_client,
            diagnose_cdc_network,
        )

        ec2 = build_ec2_client(getattr(session, "aws_profile", None), region)
        diagnosis = diagnose_cdc_network(ec2, vpc_id)
    except Exception:  # noqa: BLE001 - dialog still works without the preview
        return "", "", ""
    return diagnosis.reason, diagnosis.mode, (diagnosis.routed_cidr_warning or "")


def _captured_split_tables(migration_state, job) -> "list[str]":
    """The tables CDC captures (the watermark's covered tables still selected) that the Full
    Load record ``job`` marks as read in parts, each from its own snapshot, while their copy
    still waits for CDC's replay (G137, ``_models.split_read_tables``: until a CHECKSUM match
    or a recreate clears the mark -- a submitted Start CDC, or a stream, does not), in record
    order. Pure."""
    names = split_read_tables(job)
    if not names:
        return []
    captured = cdc_capture_names(migration_state, _cdc_watermark(job, migration_state))
    return [n for n in names if captured is None or n in captured]


def _split_cdc_only(migration_state) -> bool:
    """Is this CDC only, whose route to a reload goes through Full load + CDC (G137)?"""
    return getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY


def _split_later_qualifier(migration_state, job_manager, job) -> str:
    """What a start-point or GTID remedy naming Fetch current position says after it while CDC
    captures a table the Full Load read in parts (G137,
    :func:`~dsql_migrator.ui.data_migration._models.split_later_point_qualifier`, its route
    over the view the Full Load step renders), else "". Pure apart from the CDC-state job
    reads."""
    names = _captured_split_tables(migration_state, job)
    if not names:
        return ""
    return split_later_point_qualifier(
        names, split_route_view(migration_state, job_manager, job),
        cdc_only=_split_cdc_only(migration_state),
    )


def split_start_point_refusal(
    migration_state,
    job,
    seed,
    source_type,
    *,
    origin: str,
    resumes: bool = False,
    job_manager=None,
) -> Optional[str]:
    """Why Start CDC must refuse ``seed`` (G137), or ``None``.

    A MySQL Manual start point -- one entered, or Fetch current position's -- later than the
    Full Load's start point, while CDC captures a table that load marks as read in parts
    (:func:`_captured_split_tables`: until a CHECKSUM match or a recreate clears the mark;
    a Start CDC submitted before, which may never have streamed, does not): only a replay
    from a point no later than every read makes that copy consistent. Automatic, or a Manual
    point at or before the Full Load's start point, is that replay and is never refused, and
    a start that resumes a committed offset (a stopped stream) seeds nothing (``resumes``).
    The refusal names the tables, the no-cost way (Automatic, or a point at or before the
    Full Load's) and the reload route over the view the Full Load step renders
    (:func:`~dsql_migrator.ui.data_migration._models.split_read_refusal`). Pure apart from
    the CDC-state job reads.
    """
    if source_type is SourceType.POSTGRES or origin != "manual" or resumes:
        return None
    names = _captured_split_tables(migration_state, job)
    if not names:
        return None
    wm = getattr(job, "watermark", None)
    point_file = getattr(seed, "binlog_file", None)
    point_pos = getattr(seed, "binlog_position", None)
    wm_file, wm_pos = getattr(wm, "binlog_file", None), getattr(wm, "binlog_position", None)
    if (
        point_file
        and point_pos is not None
        and wm_file
        and wm_pos is not None
        and offset_already_at_or_past(
            {"file": wm_file, "pos": int(wm_pos)}, {"file": point_file, "pos": int(point_pos)}
        )
    ):
        return None
    point = (
        f"{point_file}:{point_pos}" if point_file and point_pos is not None else "this point"
    )
    return split_read_refusal(
        names,
        operation=(
            f"Starting CDC from {point}, later than the Full Load's start point "
            f"({wm_file}:{wm_pos}),"
        ),
        then="start CDC from that point",
        job=split_route_view(migration_state, job_manager, job),
        cdc_only=_split_cdc_only(migration_state),
        alternative=(
            "Set the CDC start point back to Automatic (the Full Load's start point), or "
            f"enter a Manual point at or before {wm_file}:{wm_pos}: CDC then replays every "
            "read, with no reload."
        ),
    )


def _probe_binlog_resume_gap(migration_state, job_manager, session) -> Optional[str]:
    """Read-only: is the start point's binary log still on the source? (blocking)

    Runs one ``SHOW BINARY LOGS`` against the source and compares it with the binlog
    file Start will SEED (:func:`_cdc_start_seed`): the Full Load watermark's, or a
    Manual start point's. Returns an actionable reason when that log has been purged,
    else ``None`` -- including whenever the answer is unknown (nothing to seed, no
    source password after a restart, or the statement/privilege is unavailable), so
    this never blocks on uncertainty. Blocking I/O: callers MUST run it via
    ``run.io_bound``.
    """
    job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    seed, origin = _cdc_start_seed(
        migration_state, _cdc_watermark(job, migration_state), _cdc_source_type(session)
    )
    watermark_file = getattr(seed, "binlog_file", None)
    if not watermark_file:
        return None
    # A real RESUME starts at the committed offset (the seeder does not rewind to the seed),
    # so the seed's log being purged says nothing about this Start.
    if cdc_start_resumes(migration_state, seed, _cdc_source_type(session)):
        return None
    source_config = getattr(session, "source_config", None)
    if source_config is None:
        return None
    try:
        from dsql_migrator.core.watermark import (
            binlog_resume_gap_reason,
            list_binary_logs,
        )
        from dsql_migrator.ui.connect import make_source_engine_factory

        engine = make_source_engine_factory(
            getattr(session, "source_password", None)
        )(source_config)
        try:
            with engine.connect() as connection:
                retained = list_binary_logs(connection)
        finally:
            engine.dispose()
    except Exception:  # noqa: BLE001 - advisory pre-flight; unknown never blocks
        return None
    # G137: the captured tables the Full Load read in parts -- no manual route for them before
    # their reload, which the reason names by the control the Full Load step shows.
    _split = _captured_split_tables(migration_state, job)
    reason = binlog_resume_gap_reason(
        watermark_file, retained, manual=(origin == "manual"),
        split_tables=_split,
        split_route=(
            split_reload_route(
                _split, split_route_view(migration_state, job_manager, job),
                cdc_only=_split_cdc_only(migration_state),
            )
            if _split
            else ""
        ),
    )
    if reason and origin != "manual":
        # CDC only has no Full Load step: the remedy's re-run needs the type switch the other
        # Drop & reload routes name (G27; empty for Full load + CDC) -- to Full load + CDC
        # while a table read in parts waits for CDC's replay (G137).
        from dsql_migrator.ui.cdc_start_point_gate import drop_and_reload_rerun

        _note = drop_and_reload_rerun(
            getattr(migration_state, "migration_type", None), split=bool(_split)
        )[1]
        if _note:
            reason = f"{reason}{_note.rstrip()}."
    if reason and cdc_start_rolled_back(migration_state):
        # G25: either remedy takes a new start point, which this stack needs new CDC topics
        # for -- that Start may have streamed into them.
        from dsql_migrator.ui.cdc_gtid_gate import ROLLED_BACK_NEW_POINT

        reason = f"{reason} {ROLLED_BACK_NEW_POINT}"
    return reason


def pg_objects_required_before_deploy(migration_state, job) -> bool:
    """Should the source's replication objects be PROBED before the (billable) deploy?

    Pure, so the decision is testable without a dialog. Deliberately NOT just "the type is
    CDC only": switching the type to Full-load-+-CDC after a FINISHED Full-load-only run
    re-grades the existence prerequisite to SKIP -- its detail says this run's Full Load
    creates them -- so the prerequisite gate un-gates Deploy while the objects do not exist.

    Keying on the WATERMARK is precise:

    * a FRESH combined run has no watermark -> False, so the recommended
      deploy-the-infra-before-the-load flow is untouched;
    * ANY run whose load has reached its consistency point -> True, whichever tile is
      selected now. A recorded ``slot_name`` used to return False on the assumption that a
      slot this run created is still there -- but a teardown DROPS it and an over-retained
      one is INVALIDATED (``wal_status='lost'``), and that is the worst state to discover
      late: ``snapshot.mode=never`` does not snapshot, so the connector would create a
      replacement slot at the CURRENT WAL and skip every change since the load, silently.
      Probing costs one read; not probing costs the ~5-minute billable MSK Serverless create
      before the same refusal lands at Start.

    True selects the PROBE, not a verdict. Since v0.1.507 an absent publication under
    ``snapshot.mode=initial`` derives ``publication.autocreate.mode=filtered``, so
    :func:`_pg_objects_gate_block` returns None for absent publication + absent slot in every
    state this returns True for -- including the slotless-watermark case above (executed). The
    probe bites only on an object that EXISTS and cannot be used, or an absent slot a
    ``never``-mode start would resume from.

    RESIDUAL, deliberately NOT gated: a combined-tile session with NO watermark (the load has
    not started) is not probed, so it can pay for MSK Serverless and only afterwards meet a
    stale publication of the same derived name from a prior run. Extending the trigger there
    was evaluated by execution and rejected: it prevents nothing in the absence case
    (absent+absent does not block anyway), a publication MISSING a requested table is REFUSED
    by the load's own ``create_publication`` with the right remedy (one that covers the set is
    reused: the connector's table.include.list filters the rest, G16), a
    same-named stale slot is dropped by ``provision_pg_replication`` (so blocking would
    pre-empt a decision the load owns), and a block would carry this probe's Start-time
    wording plus the dialog's 'start over as "Full load + CDC"' remedy -- meaningless when
    that tile is already selected, i.e. the v0.1.509 dead end again. Collisions need a REUSED
    cdc-stack name, so every first-run state is unaffected. (One real gap lives elsewhere and
    must be fixed there: ``create_publication`` REUSES a publication that covers the table
    set without grading its publish list, so an INSERT-only one left by a prior run is
    adopted silently.)
    """
    if getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY:
        return True
    watermark = _cdc_watermark(job, migration_state)
    return watermark is not None


def _cdc_watermark(job, migration_state=None):
    """The watermark a CDC start may resume from; the slot is suppressed on an UNFINISHED load.

    The watermark is attached BEFORE the first table loads, so a FAILED or partial run leaves
    one byte-identical to a successful run's -- same WAL LSN, same slot name. Nothing in the
    CDC-start path reads the job's status, so honouring that slot selects
    ``snapshot.mode=never`` and the never-loaded tables get NO baseline and NO backfill: their
    rows are permanently absent from the target, silently. Switching the type to "CDC only"
    even removes the "retry the failed tables first" hint, because that substep disappears.

    Dropping ONLY ``slot_name`` routes such a job to ``snapshot.mode=initial`` -- re-snapshot
    every captured table -- which is lossless. It SELF-HEALS: a retry carries the ORIGINAL
    watermark forward, so once the job reaches DONE the slot is honoured again and the gapless
    resume comes back on its own. It also fails in the safe direction: a load that completed
    but whose post-load pass raised costs a re-read, never correctness.

    "Unfinished" is judged over what CDC CAPTURES when ``migration_state`` is given (G16):
    a Full load + CDC set narrowed after a failed or cancelled run keeps the slot when every
    captured table loaded completely and the unfinished work lies only in tables no longer
    selected (:func:`capture_load_complete`) -- those are never captured, so nothing of
    theirs needs a baseline. A run-level failure, or any gap in a captured table, still
    suppresses it. ``None`` (no state) is the record-wide rule above.
    """
    wm = getattr(job, "watermark", None) if job is not None else None
    if wm is None:
        return None
    if getattr(job, "status", None) != "DONE" and getattr(wm, "slot_name", None):
        if migration_state is not None and capture_load_complete(
            job, cdc_capture_names(migration_state, wm) or ()
        ):
            return wm
        return wm.model_copy(update={"slot_name": None})
    return wm


def seed_refused_by_an_earlier_offer(
    migration_state, seed, source_type, *, streamed: Optional[bool] = None
) -> Optional[str]:
    """Why a MySQL Start must refuse ``seed``: it is EARLIER than a seed an earlier Start
    already offered on this stack, which never streamed (a Start that failed after seeding).
    The seeder never rewinds, so that seed stays and this one would silently not apply.
    ``None`` otherwise (and always once the stack streamed: cdc_start_resumes decides).

    The way on it names is one Start CDC takes (G25): "or later" only on a stack that has not
    streamed -- after a Start CloudFormation rolled back, or with that Start's connectors in
    place, a later point is refused too (:func:`newer_start_point_refusal`), so only that
    offset, or new CDC topics, are named. ``streamed``: whether it has, as the Start CDC
    worker's own describe shows it (G26 repair); ``None`` -- the stack-phase probe's cache
    (:func:`cdc_stack_has_streamed`)."""
    if source_type is SourceType.POSTGRES:
        return None
    if getattr(migration_state, "cdc_has_committed_offset", False):
        return None
    offered = getattr(migration_state, "cdc_offered_seed", None)
    f = getattr(seed, "binlog_file", None)
    pos = getattr(seed, "binlog_position", None)
    if not offered or not f or pos is None:
        return None
    if offset_already_at_or_past({"file": f, "pos": int(pos)}, {"file": offered[0], "pos": int(offered[1])}):
        return None
    if streamed is None:
        streamed = cdc_stack_has_streamed(migration_state)
    way_on = (
        "Start from that offset, or delete the CDC infrastructure and deploy it again to "
        "start from another point."
        if streamed
        else "Start from that offset or later, or delete the CDC infrastructure and deploy "
        "again to start earlier."
    )
    return (
        f"An earlier Start already offered the start offset {offered[0]}:{offered[1]} to "
        "this stack's seeder and did not complete; that offset stays seeded and the seeder "
        f"never rewinds, so starting from {f}:{int(pos)} would silently not apply -- the "
        f"changes in between would be skipped. {way_on} No connectors were changed."
    )


def newer_start_point_refusal(
    migration_state,
    seed,
    source_type,
    *,
    origin: str = "full_load",
    job_watermark=None,
    stack_params=None,
    stack_status: Optional[str] = None,
) -> "Optional[tuple[tuple[str, str, str, str], str]]":
    """``(notice, line)`` when Start CDC must refuse this MySQL Start of ``seed``: it would move
    a stack that has streamed FORWARD, past the start point it last started from (G25, decision
    A2) -- else ``None`` (always for PostgreSQL: see
    :func:`~dsql_migrator.ui.data_migration._cdc_status.newer_start_point`). ``notice`` is
    ``(tone, icon, header, body)``; ``line`` its one-line form (the disabled Start's tooltip,
    and the Start CDC job's error after "Start CDC blocked: ").

    The backstop of the Full Load step's new-start-point gate, which a detour gets around (the
    migration type switched to Full load only for a Re-run, then back, or to CDC only). Judged
    on ``stack_params`` / ``stack_status`` -- the worker's fresh describe -- when given, else
    on the stack-phase probe's cache. ``job_watermark`` is what Automatic would seed: a Manual
    ``seed`` refused while Automatic would continue the stream says so. Pure."""
    from dsql_migrator.ui.cdc_start_point_gate import (
        newer_point_line,
        newer_point_notice,
        point_label,
    )
    from dsql_migrator.ui.data_migration._cdc_status import (
        cdc_deployed_watermark,
        cdc_has_committed_offset,
        cdc_start_rolled_back,
        newer_start_point,
    )

    if source_type is SourceType.POSTGRES:
        return None
    named = bool(getattr(migration_state, "cdc_connector_names", None))
    if stack_params is not None:
        facts = {
            "has_committed_offset": cdc_has_committed_offset(stack_params),
            "connectors_exist": named
            or bool(str(stack_params.get("MskBootstrapServers", "") or "").strip()),
            "rolled_back": (
                str(stack_status).strip().upper().startswith("UPDATE_ROLLBACK")
                if stack_status is not None
                else cdc_start_rolled_back(migration_state)
            ),
            "deployed_watermark": cdc_deployed_watermark(stack_params),
        }
    else:
        facts = {
            "has_committed_offset": bool(
                getattr(migration_state, "cdc_has_committed_offset", False)
            ),
            "connectors_exist": named
            or getattr(migration_state, "cdc_stack_phase", None) == "running",
            "rolled_back": cdc_start_rolled_back(migration_state),
            "deployed_watermark": getattr(migration_state, "cdc_deployed_watermark", None),
        }
    offered = getattr(migration_state, "cdc_offered_seed", None)

    def _judge(candidate):
        return newer_start_point(
            candidate, offered_seed=offered, source_type=source_type, **facts
        )

    newer = _judge(seed)
    if newer is None:
        return None
    migration_type = getattr(migration_state, "migration_type", None)
    # A Manual point refused where Automatic would continue this stream.
    automatic_resumes = origin == "manual" and _judge(job_watermark) is None
    if newer.rolled_back and newer.last is None:
        # A first Start CloudFormation rolled back, whose offered start offset this session
        # does not hold and the stack's events did not give -- read for a rollback status by
        # the stack-phase probe, and by the Start CDC worker for its own describe (G26): no
        # start point can be checked against it (G25).
        return _offer_unknown_refusal(
            point_label(seed, origin), migration_type, automatic_resumes=automatic_resumes,
            origin=origin,
        )
    last = f"binlog {newer.last[0]}:{newer.last[1]}" if newer.last else None
    notice = newer_point_notice(
        point=point_label(seed, origin),
        last=last,
        rolled_back=newer.rolled_back,
        migration_type=migration_type,
        automatic_resumes=automatic_resumes,
        origin=origin,
    )
    return notice, newer_point_line(migration_type, origin)


def _offer_unknown_refusal(
    point: str, migration_type, *, automatic_resumes: bool = False, origin: str = "full_load"
) -> "tuple[tuple[str, str, str, str], str]":
    """A2's ``(notice, line)`` (:func:`newer_start_point_refusal`) on a MySQL stack whose first
    Start CloudFormation rolled back, when the start offset that Start offered is not known
    (G25). The rollback restored the stack's parameters. This session records the offer of a
    Start it makes (in memory: none after an app restart, nor for a Start made outside this
    session) and an offer read back from the stack's events (G26), which the stack-phase
    probe reads for a rollback status and the Start CDC worker for its own describe -- so
    this is shown only after such a read did not give it: the events cannot be read, the
    Start seeded in the app (SeedMode=External), or the seeder's properties are malformed.
    The body names the possible reasons this session holds no record, and asserts none (G26
    repair). Every start point is refused then, that same one included (so the header does
    not call it newer): none can be shown to be at or before that offer, while that Start's
    source connector may have streamed past it into the CDC topics. ``origin``: the refused
    start point's -- with CDC only, a route from the Full Load's watermark names the Drop &
    reload condition (G26, ``cdc_start_point_gate.reloads_first``). Pure."""
    from dsql_migrator.ui.cdc_start_point_gate import (
        OFFER_UNKNOWN_HEADER,
        RELOAD_FIRST,
        new_topics_route,
        offer_unknown_line,
        reloads_first,
    )

    body = (
        "CloudFormation rolled back this stack's last Start, and the start offset that Start "
        "offered is no longer known: the rollback restored the stack's parameters, this "
        "session holds no record of that offset (the app may have restarted since that "
        "Start, or that Start may have been made outside this session), and it could not be "
        f"read from the stack's events. So {point} cannot "
        "be shown to be at or before it, while that Start's source connector may have "
        "streamed past it into this stack's CDC topics before the rollback removed it. Start "
        "CDC does not move such a stack's position forward: its sink resumes from its last "
        "commit, so a later start point would have it write those older changes over anything "
        "loaded or corrected since, while the source would skip every change between where it "
        "stopped and the new point — silently. To start CDC, use "
        f"{new_topics_route(migration_type)}."
    )
    if reloads_first(migration_type, origin):
        body += f" {RELOAD_FIRST}"
    if automatic_resumes:
        body += " To continue this stream instead, set the CDC start point back to Automatic."
    return (
        ("error", "fast_forward", OFFER_UNKNOWN_HEADER, body + " No connectors were changed."),
        offer_unknown_line(migration_type, origin),
    )


def cdc_start_resumes(migration_state, watermark, source_type) -> bool:
    """Will Start CDC RESUME the stack's committed offset, rather than seed ``watermark``? Pure.

    ``watermark`` is the seed Start offers the offset seeder (:func:`_cdc_start_seed`: the
    job watermark, or a Manual binlog point). The seeder moves the committed offset FORWARD
    to it unless the offset is already at/past it (no-clobber). A stopped stack's offset is
    at/past the watermark it was last seeded from (``cdc_deployed_watermark``), so the seed
    is skipped -- a resume -- when it is at or before that one, judged by the seeders' own
    comparison (:func:`~dsql_migrator.core.cdc_kafka_prep.offset_already_at_or_past`:
    numeric binlog sequence, then position). A newer seed, or one judged against an unknown
    deployed watermark, is a seeded start. PostgreSQL is unchanged. The truth table is
    :func:`~dsql_migrator.ui.data_migration._cdc_status.start_resumes_committed_offset`,
    shared with the worker thread's re-check against a fresh describe.
    """
    from dsql_migrator.ui.data_migration._cdc_status import start_resumes_committed_offset

    return start_resumes_committed_offset(
        watermark,
        has_committed_offset=bool(getattr(migration_state, "cdc_has_committed_offset", False)),
        deployed_watermark=getattr(migration_state, "cdc_deployed_watermark", None),
        source_type=source_type,
    )


def _reload_condition_notice(migration_state, job, session) -> Optional[tuple]:
    """``(header, body)`` of the reload condition (G27,
    :func:`~dsql_migrator.ui.cdc_start_point_gate.reload_condition`) for a Start CDC that begins at
    the Full Load's watermark -- MySQL: it seeds that watermark and resumes no committed offset;
    PostgreSQL: it streams from that Full Load's slot on a stack never started -- when that Full
    Load is not this migration's first copy (:func:`drop_reload_condition_due`); else None. The
    start-point card states it from its own state; the Start CDC dialog uses this. Its CDC only
    re-run goes through Full load + CDC while a captured table read in parts waits for CDC's
    replay (G137). Pure apart from the CDC-state job reads."""
    if not drop_reload_condition_due(migration_state, job):
        return None
    source_type = _cdc_source_type(session)
    watermark = _cdc_watermark(job, migration_state)
    if watermark is None:
        return None
    if source_type is SourceType.POSTGRES:
        if (
            not getattr(watermark, "slot_name", None)
            or migration_state.cdc_start_mode() == "manual"
            or _pg_stack_started(migration_state)
        ):
            return None
    else:
        seed, origin = _cdc_start_seed(migration_state, watermark, source_type)
        if (
            origin != "full_load"
            or not _seed_can_seed(seed)
            or cdc_start_resumes(migration_state, seed, source_type)
        ):
            return None
    from dsql_migrator.ui.cdc_start_point_gate import RELOAD_CONDITION_HEADER, reload_condition

    return RELOAD_CONDITION_HEADER, reload_condition(
        getattr(migration_state, "migration_type", None),
        postgres=source_type is SourceType.POSTGRES,
        started=cdc_started_before_load(migration_state, job),
        split=bool(_captured_split_tables(migration_state, job)),
    )


def _mysql_start_context(migration_state, job_manager, inventory, session):
    """``(seed, origin, facts, snapshot_mode, tables)`` of the MySQL Start CDC would submit.

    Computed through the SAME functions Start uses (:func:`_cdc_start_seed`,
    :func:`mysql_start_facts`, :func:`~dsql_migrator.core.cdc.mysql_snapshot_mode`,
    :func:`_cdc_tables_for_config`), so the Start dialog checks exactly the start the job
    would run. Pure (state reads only).
    """
    job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    watermark = _cdc_watermark(job, migration_state)
    source_type = _cdc_source_type(session)
    seed, origin = _cdc_start_seed(migration_state, watermark, source_type)
    facts = mysql_start_facts(migration_state, seed, source_type)
    mode = mysql_snapshot_mode(_seed_can_seed(seed), facts)
    tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    return seed, origin, facts, mode, tables


def _probe_cdc_start_point(migration_state, session, *, seed, mode, facts, tables):
    """The MySQL start-point check for one Start (BLOCKING; run it via ``run.io_bound``).

    ``None`` when no check is due: PostgreSQL, a ``schema_only`` start (a resume replays
    the recorded history; nothing seeded reads the current schema), or a stack whose
    connectors exist (``start_point_check_due``). Otherwise the captured ``tables`` are
    compared with the seed's recorded fingerprints by ONE bounded catalog read
    (:func:`~dsql_migrator.ui.data_migration._cdc_status.read_cdc_start_point_once`, which
    never raises). A history-missing resume with no seed has no fingerprint to compare.
    """
    if _cdc_source_type(session) is SourceType.POSTGRES:
        return None
    if not start_point_check_due(mode, facts):
        return None
    from dsql_migrator.ui.data_migration._cdc_status import read_cdc_start_point_once

    recorded = dict(getattr(seed, "table_fingerprints", None) or {}) if seed is not None else {}
    names = [getattr(t, "name", str(t)) for t in (tables or ())]
    return read_cdc_start_point_once(session, names, recorded)


class StackStartRecord(NamedTuple):
    """What a stack records about its last Start (see :func:`_stack_start_record`)."""

    point: Optional[tuple]
    gtids: Optional[str]
    gtid_mode: Optional[str]
    committed: Optional[bool]


def _stack_start_record(migration_state, stack_params) -> StackStartRecord:
    """What the stack records about its last Start, from ``stack_params`` (a fresh describe
    on the worker) or else the phase probe's cache. Pure.

    ``point`` / ``gtids``: the start offset that Start offered (its Watermark* parameters,
    carried forward by Stop) -- ``(file, pos)`` or ``None``; ``gtids`` is ``""`` for an offer
    with no GTID set and ``None`` when nothing is recorded or known. ``gtid_mode``: the
    source gtid_mode its last MySQL Start positioned under (StartGtidMode, G20), ``None``
    for no record. ``committed``: the stack has streamed before (a committed offset), or
    ``None`` when there is neither a state nor a describe to say.
    """
    if stack_params is not None:
        from dsql_migrator.ui.data_migration._cdc_status import (
            cdc_deployed_gtid_mode,
            cdc_deployed_watermark,
            cdc_deployed_watermark_gtids,
            cdc_has_committed_offset,
        )

        return StackStartRecord(
            cdc_deployed_watermark(stack_params),
            cdc_deployed_watermark_gtids(stack_params),
            cdc_deployed_gtid_mode(stack_params),
            cdc_has_committed_offset(stack_params),
        )
    if migration_state is None:
        return StackStartRecord(None, None, None, None)
    deployed = getattr(migration_state, "cdc_deployed_watermark", None)
    gtids = getattr(migration_state, "cdc_deployed_watermark_gtids", None)
    return StackStartRecord(
        tuple(deployed) if deployed else None,
        gtids if deployed else None,
        getattr(migration_state, "cdc_deployed_gtid_mode", None) or None,
        bool(getattr(migration_state, "cdc_has_committed_offset", False)),
    )


def _seed_is_point(seed, point) -> bool:
    """Is ``seed``'s binlog file:position exactly ``point`` (a deployed ``(file, pos)``)?"""
    if not point or not _seed_can_seed(seed):
        return False
    try:
        return (
            str(getattr(seed, "binlog_file", "")) == str(point[0]).strip()
            and int(getattr(seed, "binlog_position")) == int(str(point[1]).strip())
        )
    except (TypeError, ValueError):
        return False


def _probe_cdc_gtid_start(
    migration_state, session, *, seed, facts, origin=None, stack_params=None
):
    """The MySQL GTID start check for one Start (BLOCKING; run it via ``run.io_bound``).

    ``None`` when no check is due: PostgreSQL, a stack whose connectors exist, or a start
    that neither seeds a binlog position nor resumes (:func:`gtid_start_check_due`).
    Otherwise ONE bounded read of the source's ``gtid_mode`` -- plus, for a seeded start
    whose GTID set Debezium will position by, whether the source can stream from that set
    (:func:`~dsql_migrator.ui.data_migration._cdc_status.read_source_gtid_state_once`,
    which never raises) -- judged by
    :func:`~dsql_migrator.ui.cdc_gtid_gate.mysql_gtid_start_verdict`, with what the stack
    records about its last Start (:func:`_stack_start_record`; ``stack_params``: the
    worker's fresh describe, else the phase probe's cache): the gtid_mode it positioned
    under (G20), the start offset it was offered, and whether it ever streamed -- a stack
    that never did is on its first start, which must read the mode to record it.
    ``origin`` is the seed's (:func:`_cdc_start_seed`): a ``"manual"`` GTID set that is not
    a GTID set is refused without reading anything.
    """
    if _cdc_source_type(session) is SourceType.POSTGRES:
        return None
    from dsql_migrator.ui.cdc_gtid_gate import (
        gtid_start_check_due,
        mysql_gtid_start_verdict,
    )

    if not gtid_start_check_due(seed, facts):
        return None
    from dsql_migrator.ui.data_migration._cdc_status import read_source_gtid_state_once

    seedable = _seed_can_seed(seed)
    resumes = bool(getattr(facts, "resumes", False))
    seed_gtids = normalize_gtid_set(getattr(seed, "gtid_executed", None))
    # The mode recorded with the start point (a Full Load watermark of this version).
    captured = getattr(seed, "gtid_mode", None) or None
    if (
        origin == "manual"
        and seedable
        and not resumes
        and seed_gtids
        and validate_gtid(seed_gtids) is not None
    ):
        # A typed set that is not a GTID set: refused on every mode, nothing to read. (A
        # Full Load watermark's set is MySQL's own output.)
        return mysql_gtid_start_verdict(
            None, seed_gtids=seed_gtids, seed_gtid_mode=captured, seedable=seedable,
            resumes=resumes, seed_set_malformed=True,
        )
    record = _stack_start_record(migration_state, stack_params)
    stack_point = record.point
    # G20: a stack that never streamed is on its first start (its gtid_mode record must be
    # written by a start that read the mode); unknown (no state, no describe) is not.
    first_start = record.committed is False and not resumes
    # The set is checked against the source only for a seeded start that will position by
    # it: a resume continues from what Debezium committed, and a point recorded while the
    # mode was not ON is refused whatever the set says.
    subset_set = (
        seed_gtids
        if (
            not resumes
            and seedable
            and (captured is None or gtid_mode_is_on(captured))
            and (seed_gtids or gtid_mode_is_on(captured))
        )
        else None
    )
    state = read_source_gtid_state_once(session, subset_set)
    return mysql_gtid_start_verdict(
        state,
        seed_gtids=seed_gtids,
        seed_gtid_mode=captured,
        seedable=seedable,
        resumes=resumes,
        stack_gtids=record.gtids,
        stack_point=(
            f"binlog {stack_point[0]}:{stack_point[1]}" if stack_point else None
        ),
        seed_is_stack_point=_seed_is_point(seed, stack_point),
        stack_gtid_mode=record.gtid_mode,
        first_start=first_start,
    )


def pg_start_resnapshots(watermark, *, source_type, start_mode: str) -> bool:
    """Will this PostgreSQL start RE-READ every captured table? Pure.

    ONE rule for every surface that has to disclose the second read, taken off the same
    :func:`pg_snapshot_mode` the connector config and the Deploy/Start gates already call --
    the mirror lives in one place, for exactly the reason spelled out in
    :func:`_pg_objects_gate_block`.

    Note this is deliberately True when ``watermark is None``: a start with no watermark in
    this session also runs ``snapshot.mode=initial``, and keying the disclosure on the
    in-memory object would go SILENT in the one state the job record is gone (an app
    restart) -- the inverse of the bug this exists to fix. WHY the re-read happens is a
    separate question with four different answers; ask :func:`pg_resnapshot_reason`, which
    reads the RAW job because this watermark has already had its slot suppressed.
    """
    if source_type is not SourceType.POSTGRES:
        return False
    return (
        pg_snapshot_mode(watermark, force_initial_snapshot=(start_mode == "manual"))
        == "initial"
    )


def pg_resnapshot_reason(
    job, *, source_type, start_mode: str, migration_state=None
) -> Optional[str]:
    """WHY a PostgreSQL start re-snapshots: the cause decides the REMEDY. Pure.

    ``None`` when the start is gapless. Otherwise one of:

    * ``"unfinished_load"`` -- the slot EXISTS on the source but :func:`_cdc_watermark`
      suppressed it because the load did not reach ``DONE``. Remedy: retry the failed
      tables; the slot is honoured again on its own once the job is DONE. Telling this
      operator "no slot was recorded" would push a full re-load where a retry sufficed.
    * ``"manual"`` -- a gapless resume WAS available and the operator chose a clean
      re-copy. Remedy: switch the start point back to Automatic. Not a defect, and not
      something to blame on a missing slot.
    * ``"no_slot"`` -- a finished load that never provisioned one, i.e. "Full load only".
      Remedy: start over as "Full load + CDC"; a slot cannot be created at a past LSN.
    * ``"no_watermark"`` -- no Full Load record in this session at all (CDC-only, or the
      job record was lost). Nothing was read here, so "read AGAIN" must not be claimed.

    Checked against the RAW job on purpose: ``_cdc_watermark`` strips ``slot_name`` for an
    unfinished load, so the stripped watermark cannot tell those two causes apart. With
    ``migration_state`` the load is "unfinished" by the same rule ``_cdc_watermark`` uses
    with it (G16: unfinished work only in tables CDC no longer captures keeps the slot), so
    the two never disagree; pass it wherever ``_cdc_watermark`` gets it.
    """
    if source_type is not SourceType.POSTGRES:
        return None
    raw = getattr(job, "watermark", None) if job is not None else None
    if raw is None:
        return "no_watermark"
    slot_recorded = bool(getattr(raw, "slot_name", None))
    if not slot_recorded:
        return "no_slot"
    if getattr(job, "status", None) != "DONE" and not (
        migration_state is not None
        and capture_load_complete(job, cdc_capture_names(migration_state, raw) or ())
    ):
        return "unfinished_load"
    return "manual" if start_mode == "manual" else None


# Per-CAUSE wording for a re-snapshotting PostgreSQL start: (header, cause, remedy).
# Kept as data, in one place, because the same disclosure has to appear on several surfaces
# and the REMEDY is different for every cause -- a single "no slot was recorded" message
# would send an operator whose load merely FAILED into a full re-load when a retry was
# enough, and would blame a missing slot for a re-copy the operator deliberately chose.
_PG_RESNAPSHOT_CAUSE: dict[str, tuple[str, str, str]] = {
    "no_slot": (
        "This CDC start will re-read every selected table",
        "A gapless resume needs the replication slot the tool creates at the Full Load "
        'consistency point, and a "Full load only" run does not create one. A logical '
        "replication slot cannot be created at, or rewound to, a past position, so that "
        "option cannot be recovered for this load.",
        'To stream without re-reading, start over as "Full load + CDC", which creates the '
        "slot at the snapshot point before the load.",
    ),
    "unfinished_load": (
        "This Full Load did not finish, so its replication slot is not used",
        "The slot does exist on the source, but tables that never loaded have no baseline "
        "-- resuming from it would leave their rows permanently absent, with no error. "
        "Re-snapshotting every selected table is the lossless route.",
        "Retry the failed tables first: once the load reaches DONE the slot is honoured "
        "again and the gapless resume comes back on its own, with no re-read.",
    ),
    "manual": (
        "Manual re-snapshots instead of resuming from this load's slot",
        "A gapless resume IS available here -- this Full Load created the replication "
        "slot. Manual takes a fresh copy instead, which is a legitimate choice but reads "
        "the source again.",
        # The control is on the CDC start point card (PG_RESUME_FROM_SLOT_ACTION, G25):
        # PostgreSQL has no start-point radio.
        "Switch the start point back to Automatic to resume from the slot with no re-read: "
        "use \"Resume from the Full Load's slot instead\" on the CDC start point card.",
    ),
    "no_watermark": (
        "This CDC start will snapshot every selected table first",
        "There is no Full Load watermark in this session to resume from, so the connector "
        "creates its own publication and slot and snapshots every selected table before "
        "it streams any change.",
        'If a Full Load did run in this migration, its job record is gone from this '
        'session; only a "Full load + CDC" run records a slot to resume from.',
    ),
}

# Whether a Full Load in THIS migration wrote rows to the target. Decides two sentences:
# "read a SECOND time" and the deleted-row residue are claims about a preceding load, and
# both would be false for a stand-alone CDC-only start.
_PG_RESNAPSHOT_AFTER_LOAD = {"no_slot", "unfinished_load", "manual"}


# The same disclosure, compressed to what a SPEND decision needs. Used only in the Deploy
# dialog, which authorises a billable cluster and where this notice competes with three other
# boxes; the full version stays on the start-point card and in the prerequisite row, where the
# operator is reading to UNDERSTAND rather than to decide. Dropped here: the slot mechanism
# (why a past position cannot be rewound to) and the "nothing is lost" reassurance -- both are
# verbatim one screen up. Kept: what happens, what it costs, the residue, and the way out.
_PG_RESNAPSHOT_BRIEF: dict[str, str] = {
    "no_slot": (
        "This load recorded no replication slot, so every selected table is read from the "
        "source again and streamed into DSQL before any change flows — expect it to take "
        "LONGER than the Full Load did, and rows DELETED on the source since then stay on "
        'the target. To avoid the re-read, cancel and start over as "Full load + CDC".'
    ),
    "unfinished_load": (
        "This Full Load did not finish, so its replication slot is not used: every selected "
        "table is re-snapshotted before any change flows. Retry the failed tables first — "
        "the gapless resume comes back on its own once the load reaches DONE."
    ),
    "manual": (
        "This start re-snapshots instead of resuming from the slot this load created, so "
        "every selected table is read again. Switch the start point back to Automatic to "
        "resume with no re-read (\"Resume from the Full Load's slot instead\" on the CDC start "
        "point card)."
    ),
    "no_watermark": (
        "There is no Full Load watermark to resume from, so every selected table is "
        "snapshotted through the streaming pipeline before any change flows — expect that to "
        "take a while on a large source."
    ),
}

# G27 repair: the two causes whose way back is a resume from this load's slot, when that Full
# Load is not this migration's first copy (``_cdc_status.drop_reload_condition_due``): the
# resume is gapless only if it did not append to rows already on the target, as the card's
# info tooltip says. ``(cause, remedy, brief)``, None keeping the default: the remedy names the
# Drop & reload re-run (rule E; ``{rerun}``/``{note}``:
# ``cdc_start_point_gate.drop_and_reload_rerun``), the brief only the condition. The default
# "manual" brief claims no gapless resume.
_PG_RESNAPSHOT_RECOPIED: dict[str, tuple[Optional[str], str, Optional[str]]] = {
    "unfinished_load": (
        None,
        "Retry the failed tables first: once the load reaches DONE the slot is honoured "
        "again and the resume from it comes back on its own, with no re-read -- gapless only "
        "if this Full Load did not append to rows already on the target. If it did, {rerun} "
        "instead{note}.",
        "This Full Load did not finish, so its replication slot is not used: every selected "
        "table is re-snapshotted before any change flows. Retry the failed tables first — "
        "the resume from the slot comes back on its own once the load reaches DONE, gapless "
        "only if this Full Load did not append to rows already on the target.",
    ),
    "manual": (
        "A resume from this load's slot is available here -- this Full Load created the "
        "replication slot -- but it is gapless only if that Full Load did not append to "
        "rows already on the target. Manual takes a fresh copy instead, which is a "
        "legitimate choice but reads the source again.",
        _PG_RESNAPSHOT_CAUSE["manual"][2]
        + " If that Full Load appended, {rerun} before Start CDC{note}.",
        None,
    ),
}


def pg_resnapshot_notice(
    reason: Optional[str], *, brief: bool = False, recopied: bool = False,
    migration_type: object = None,
) -> Optional[tuple[str, str]]:
    """``(header, body)`` disclosing a re-snapshotting PostgreSQL start, or ``None``. Pure.

    All the copy for this route in ONE place, so the Deploy dialog, the start-point card and
    the Start dialog cannot tell an operator three different stories -- and so a test can
    pin the wording without driving three renders.

    No wall-clock estimate is given on purpose. The re-snapshot runs through the STREAMING
    pipeline (Debezium -> MSK -> the sink), not the bulk loader, and the repo's own
    measurements put those an order of magnitude apart, so "about as long as the Full Load"
    would be a number the tool cannot support. State the direction, not a figure.

    ``recopied`` (G27 repair, ``_cdc_status.drop_reload_condition_due``): the Full Load is not
    this migration's first copy, so the resume from its slot that the ``manual`` and
    ``unfinished_load`` causes offer as the way back is worded as gapless only if it did not
    append (:data:`_PG_RESNAPSHOT_RECOPIED`), the re-run named for ``migration_type``.
    """
    entry = _PG_RESNAPSHOT_CAUSE.get(reason or "")
    if entry is None:
        return None
    header, cause, remedy = entry
    recopied_entry = _PG_RESNAPSHOT_RECOPIED.get(reason or "") if recopied else None
    if brief:
        return header, (
            (recopied_entry[2] if recopied_entry is not None else None)
            or _PG_RESNAPSHOT_BRIEF[reason or ""]
        )
    if recopied_entry is not None:
        from dsql_migrator.ui.cdc_start_point_gate import drop_and_reload_rerun

        rerun, note = drop_and_reload_rerun(migration_type, postgres=True)
        if reason == "unfinished_load":
            # An unfinished load's Full Load step offers 'Retry unfinished tables' and 'Re-run
            # all tables', not 'Re-run Full Load': name the button the operator can see there.
            rerun = rerun.replace("Re-run Full Load", "Re-run all tables")
        cause = recopied_entry[0] or cause
        remedy = recopied_entry[1].format(rerun=rerun, note=note)
    after_load = reason in _PG_RESNAPSHOT_AFTER_LOAD
    lossless = (
        "Nothing is lost: there is no window between the snapshot and the stream, and the "
        "target apply is idempotent."
    )
    if after_load:
        cost = (
            "The cost is that every selected table is read from the source a second time "
            "and every row crosses MSK into DSQL again -- through the streaming pipeline "
            "rather than the bulk loader, so expect it to take LONGER than the Full Load "
            "did. Rows DELETED on the source since the Full Load also stay on the target, "
            "because a fresh snapshot only reports the rows that still exist."
        )
    else:
        cost = (
            "The cost is that the whole selection crosses MSK into DSQL through the "
            "streaming pipeline before any change is streamed, so expect the initial "
            "snapshot to take a while on a large source."
        )
    return header, " ".join((cause, lossless, cost, remedy))


def _pg_start_rolled_back(migration_state) -> bool:
    """Did CloudFormation roll back this cdc-stack's last update -- a Start's? (G25) Its
    status as the stack-phase probe last read it (the same describe as the phase): ANY
    ``UPDATE_ROLLBACK_*`` -- complete, still in progress, or failed (an unstable status keeps
    the tool's unstable-stack handling; only the started rule reads it here). Only a Start
    updates an infra stack (Stop changes nothing on one; on a stack with connectors the
    update was a Stop's or a Retry's, started either way), and the rollback restores the
    parameters it had before, so ``DeploySink`` and ``MskBootstrapServers`` no longer say so.
    That Start's source connector may have streamed, and committed an offset, before the
    rollback deleted it: Debezium continues from that offset, so a re-snapshot would be
    skipped. The CDC topics outlive the rollback and may hold changes it streamed that no
    sink applied: the next sink applies them first, so a retry is gapless only while the
    topics still retain them, and a Full Load re-run on this stack would have them applied
    over the reloaded tables (the route rule, :func:`~dsql_migrator.core.cdc_pg_slot.pg_cdc_route`,
    names new topics instead). The engine-neutral reading lives in
    :func:`~dsql_migrator.ui.data_migration._cdc_status.cdc_start_rolled_back`. Pure."""
    from dsql_migrator.ui.data_migration._cdc_status import cdc_start_rolled_back

    return cdc_start_rolled_back(migration_state)


def _pg_stack_started(migration_state) -> bool:
    """Were this cdc-stack's connectors started before? (G25) Read from its CloudFormation
    parameters and status as the stack-phase probe last described them (one describe):
    started, then stopped
    (:func:`~dsql_migrator.ui.data_migration._cdc_status.cdc_has_committed_offset`:
    ``DeploySink="true"`` with a blank ``MskBootstrapServers``), connectors in place
    (``MskBootstrapServers`` set, the ``running`` phase, where Retry CDC re-creates a missing
    one; or this stack's connectors detected on MSK Connect, whatever the stack status -- a
    rollback that failed or is still running leaves them), or a Start whose update
    CloudFormation rolled back (:func:`_pg_start_rolled_back`: any rollback status). The
    PostgreSQL slot-position check judges only a stack for which none of these holds: its
    FIRST Start; and the route rule (:func:`~dsql_migrator.core.cdc_pg_slot.pg_cdc_route`)
    gives a stack for which any holds new topics first -- its topics may hold changes no sink
    applied -- and never the re-snapshot remedy. Nothing is kept beside what the stack
    records, so a Start a check refused, one that failed before it updated the stack, an app
    restart and Start over all leave the next Start judged the same way. Residual, like a slot
    re-created after this stack streamed (each migration needs its own CDC stack name): another
    migration re-creates the slot after this stack's first Start rolled back. The same answer,
    for either engine, is :func:`~dsql_migrator.ui.data_migration._cdc_status.cdc_stack_has_streamed`
    -- what the Full Load step gates a new start point on. Pure (state reads only)."""
    from dsql_migrator.ui.data_migration._cdc_status import cdc_stack_has_streamed

    return cdc_stack_has_streamed(migration_state)


# The Start CDC dialog's notice, and the activity line of the dialog and of the worker, when
# the publication/slot pre-flight could not read the source at all (G25): that does not
# block, but it is never silent either.
PG_CHECKS_NOT_RUN_HEADER = "CDC's publication and replication slot could not be checked"
PG_CHECKS_NOT_RUN_ACTION = "PostgreSQL publication and slot checks not run"
# The Start CDC dialog's lead for the refusal of the Start its re-snapshot remedy would
# unlock, shown instead of that remedy (G25); the refusal's own body follows.
PG_RESNAPSHOT_REFUSED_LEAD = (
    "Re-snapshot every table is not offered here: a Start that re-snapshots is refused too."
)
# The Start CDC dialog's notice, and its activity line, when only the second read -- the
# checks of the Start the re-snapshot remedy would unlock -- could not read the source (G25):
# the publication and slot checks before it ran, and the block they produced stands.
PG_RESNAPSHOT_NOT_CHECKED_HEADER = "The re-snapshot route could not be checked"
PG_RESNAPSHOT_NOT_CHECKED_ACTION = "PostgreSQL re-snapshot route check not run"


def _pg_checks_not_run_reason(session, exc: BaseException) -> str:
    """Why the PostgreSQL publication/slot pre-flight could not read the source: a short,
    value-free phrase (an exception class name at most, never its message: it can carry a
    host or a user name -- Property 7). Pure."""
    if getattr(session, "source_password", None) is None:
        if getattr(session, "source_secret_id", None):
            return (
                "this session holds no source credentials: the Secrets Manager secret is "
                "read when the source connection is tested, and an app restart clears it"
            )
        return "this session holds no source password: an app restart clears it"
    return f"the connection failed: {type(exc).__name__[:64]}"


def pg_checks_not_run_body(reason: str) -> str:
    """The Start CDC dialog's notice body for a pre-flight that could not read the source
    (:data:`PG_CHECKS_NOT_RUN_HEADER`). Pure."""
    return (
        f"Start CDC could not read the PostgreSQL source ({reason}), so it did not check "
        "CDC's publication and replication slot, nor which captured tables have inheritance "
        "child tables. Start CDC is not blocked on that, but what those checks catch would go "
        "unnoticed: a replication slot another run with the same CDC stack name re-created "
        "(the changes in between would be skipped), or a snapshot that copies inheritance "
        "child tables' rows into their parent. Test the source connection on the Connect "
        "step, then open Start CDC again so they run."
    )


def pg_resnapshot_not_checked_body(reason: str) -> str:
    """The Start CDC dialog's notice body when only the re-snapshot route's own read could not
    reach the source (:data:`PG_RESNAPSHOT_NOT_CHECKED_HEADER`, G25). Pure."""
    return (
        "Start CDC checked CDC's publication and replication slot, but could not read the "
        f"PostgreSQL source again ({reason}) to check the Start that \"Re-snapshot every table "
        "instead\" unlocks (snapshot.mode=initial): above all which captured tables have "
        "inheritance child tables, whose rows that snapshot would copy into their parent. The "
        "remedy is still offered; the Start CDC job runs those checks before it creates any "
        "connector, and goes ahead without them if it cannot read the source either."
    )


def _pg_block_route(
    migration_state, block: Optional[str], *, resnapshot: Optional[str] = None
) -> str:
    """The way out of PostgreSQL block ``block`` for this migration's CDC stack: the route
    rule's (:func:`~dsql_migrator.core.cdc_pg_slot.pg_cdc_route`, G25), fed from what the stack
    records (:func:`_pg_stack_started`) and the migration type -- the one derivation the Start
    CDC dialog, its withheld re-snapshot notice and the Start CDC job all use, so no surface
    names a route of its own. ``resnapshot``: the re-snapshot remedy's standing on a stack never
    started (``None``: not judged here). Pure (state reads only)."""
    from dsql_migrator.core.cdc_pg_slot import pg_cdc_route

    return pg_cdc_route(
        block,
        started=_pg_stack_started(migration_state),
        cdc_only=getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY,
        resnapshot=resnapshot,
    )


def _with_route(body: str, route: str) -> str:
    """``body`` followed by ``route``, when there is one. Pure."""
    return f"{body} {route}" if route else body


def _probe_pg_resume_slot(
    migration_state, session, watermark, *, on_unread: Optional[Callable[[str], None]] = None
) -> Optional[tuple]:
    """Read-only: is the slot a resumed PostgreSQL Start continues from usable? (blocking)

    For a Start the publication/slot probe does not check the slot for -- ``snapshot.mode=
    initial`` on a stack whose connectors were started before (:func:`_pg_objects_gate_block`).
    The slot is the one the connector is given (the SAME fallback ``dispatch_source_config``
    uses). Returns the block's ``(header, body)`` when the source has no usable slot of that
    name, else None -- None too when the catalog cannot be read, the probe's own rule
    (:func:`_probe_pg_replication_objects`: no block, and ``on_unread`` is told why). A
    same-named slot that exists but is unusable is that probe's block, which fires whatever
    the mode.

    Blocking I/O: callers MUST run it via ``run.io_bound``.
    """
    source_config = getattr(session, "source_config", None)
    if source_config is None:
        return None
    stack = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    try:
        from dsql_migrator.core import cdc_pg_slot as _pg
        from dsql_migrator.ui.connect import make_source_engine_factory

        slot = getattr(watermark, "slot_name", None) or _pg.pg_slot_name(stack)
        pub = getattr(watermark, "publication_name", None) or _pg.pg_publication_name(stack)
        engine = make_source_engine_factory(getattr(session, "source_password", None))(
            source_config
        )
        try:
            with engine.connect() as connection:
                objects = _pg.read_pg_replication_objects(
                    connection, publication_name=pub, slot_name=slot
                )
        finally:
            engine.dispose()
    except Exception as exc:  # noqa: BLE001 - unreadable is NOT absent: no block, but say so
        if on_unread is not None:
            on_unread(_pg_checks_not_run_reason(session, exc))
        return None
    if objects.slot_usable:
        return None
    # Worded like the start line and the CDC card (G25): the stack records that its
    # connectors were started, not that its source connector streamed -- and a slot dropped
    # since it streamed looks exactly like one a task that failed first never created. So
    # the block states both outcomes and stays closed for both.
    return (
        _pg.SLOT_MISSING_HEADER,
        f"The source has no usable logical pgoutput slot named '{slot}' in this database. If "
        "this stack's source connector streamed, Start CDC continues the position it "
        "recorded in that slot, and a slot cannot be created at a past position: the "
        "connector would create one at the CURRENT WAL and silently skip every change "
        "committed since streaming stopped. If it never streamed (its task failed, or CDC "
        "was stopped, before its snapshot completed, for example before it created the "
        "slot), the Start would snapshot every selected table first, with no gap. The stack "
        "does not record which, so Start CDC is blocked either way.",
    )


# The inheritance block's way back to the Full Load's slot (G25), when this Start snapshots only
# because "Re-snapshot every table instead" was taken (a PostgreSQL Manual start mode) and the
# Full Load recorded a slot: the CDC start point card's control sets the mode back to Automatic.
PG_RESUME_FROM_SLOT_ACTION = "Resume from the Full Load's slot instead"
PG_RESUME_FROM_SLOT_HINT = (
    "This Start snapshots only because \"Re-snapshot every table instead\" was chosen, and "
    "this migration's Full Load recorded a slot to resume from: use \""
    + PG_RESUME_FROM_SLOT_ACTION
    + "\" on the CDC start point card, then Start CDC again."
)


def _pg_objects_gate_block(
    migration_state,
    session,
    inventory,
    watermark,
    *,
    on_unread: Optional[Callable[[str], None]] = None,
    force_initial: Optional[bool] = None,
    untick: bool = True,
) -> Optional[tuple]:
    """The publication/slot gate, run with the SAME inputs the connector config will get.

    THE single entry point for every surface that gates on those objects, because deriving
    the probe's three inputs per call site is how this defect keeps coming back:

    * v0.1.507 fixed the Start path so a recorded re-snapshot decision stops the block.
    * v0.1.509 added the same gate to the Deploy dialog and passed ``[], None`` with no
      ``force_initial`` -- so the operator who took the re-snapshot route was re-blocked one
      screen later by a notice that is FALSE for that route ("the connector is configured
      with publication.autocreate.mode=disabled"), told to "start the migration over as
      'Full load + CDC'", and the continuation v0.1.510 built was a dead end again.
    * The Start DIALOG meanwhile graded coverage against every table in the inventory rather
      than the captured set, which can block on a publication that covers everything this
      migration actually replicates.

    ``pg_replication_objects_blocker``'s contract is to MIRROR ``build_pg_source_config``'s
    two decisions exactly; that is only checkable if the mirror lives in ONE place. So the
    tables come from :func:`_cdc_tables_for_config`, the resume signal from
    :func:`_cdc_resume_signal`, and the two graded decisions from
    :func:`pg_snapshot_mode` / :func:`pg_publication_autocreate_mode` -- the very functions
    the config builder itself calls. Re-deriving "will the connector autocreate?" from the
    start mode instead (which is what this did) made the gate agree with the config only by
    coincidence: it read ``manual`` as "autocreates", while the config read a SEPARATE flag
    that the start-position radio and session restore never set. The gate then waved through
    the one combination it exists to stop.

    The slot-position check judges only the stack's FIRST Start (G25): a stack whose
    CloudFormation record shows no Start of its connectors (:func:`_pg_stack_started`: none
    stopped, in place or rolled back). Stateless, so nothing a refused Start did, nor a
    restart, changes the verdict.

    ``force_initial`` judges the Start the dialog's "Re-snapshot every table instead" would
    unlock (``True``: Manual, ``snapshot.mode=initial``) instead of the one the start mode
    sends now, so that remedy is offered only when its own route's checks pass (G25).

    ``untick`` (G25): the table set can still change, so the inheritance block also names
    unticking the parent -- the Start CDC dialog passes the table picker's own lock, the Start
    CDC job ``False`` (a submitted Start fixes the set). When this Start snapshots only because
    the re-snapshot remedy was taken over a slot the Full Load recorded, that block names the
    way back to it (:data:`PG_RESUME_FROM_SLOT_HINT`), so a recorded Manual mode never loops it.

    When the source cannot be read at all, nothing blocks -- a missing publication makes the
    connector die loudly -- but the checks that catch a SILENT loss did not run either, so
    ``on_unread`` is told why (G25): the Start dialog shows a warning and the worker writes an
    activity line. Never silent.

    A Start on a stack that was started and then stopped (:func:`cdc_start_resumes`: its
    offset is committed) continues from its slot WHATEVER ``snapshot.mode`` it sends: once
    the source connector has streamed, Debezium skips ``initial``'s snapshot too
    (live-checked on the shipped Debezium 2.7.4, 2026-10-09), so a missing slot is re-created
    at the CURRENT WAL and every change since streaming stopped is skipped. The probe checks
    the slot only for ``never``, so :func:`_probe_pg_resume_slot` checks it for the other
    modes there. Every block on a stack whose connectors were started before
    (:func:`_pg_stack_started`: stopped since, in place, or created by a Start CloudFormation
    rolled back, any rollback status) names the route rule's one route, new CDC topics first
    (:func:`_pg_block_route`, G25): its topics may hold changes no sink applied -- Stop CDC and
    a rollback remove the connectors without draining them -- which its sink would apply over a
    Full Load re-run on them, and "Re-snapshot every table instead" would not snapshot there.
    The inheritance block, whose own fix runs a Full Load, is told to get new topics first, and
    so is the publish list's block after its own fix; a failed inheritance read keeps its own
    fix alone. A stack never
    started gets its route where the re-snapshot remedy's standing is known: the Start CDC
    dialog and the Start CDC job add it. A rolled-back stack's slot is not read for the other
    modes: that Start's source connector may have died before it created the slot, and an
    absent slot is then no gap (the Start snapshots). Residual,
    documented beside the same-stack-name one: that connector streamed and committed an
    offset, and the slot was dropped since -- the Start would continue from the offset on a
    slot created at the current WAL.

    Blocking source I/O: callers MUST run it via ``run.io_bound``.
    """
    tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    resume_override, mode_forces_initial = _cdc_resume_signal(migration_state, session)
    snapshot_mode = pg_snapshot_mode(
        watermark,
        resume_override=resume_override,
        force_initial_snapshot=(
            mode_forces_initial if force_initial is None else force_initial
        ),
    )
    # Why the source could not be read, at most once: a second read of a source the first
    # could not reach is not tried.
    unread: list[str] = []
    verdict = _probe_pg_replication_objects(
        migration_state,
        session,
        tables,
        watermark,
        snapshot_mode=snapshot_mode,
        on_unread=unread.append,
        # Passed only for a locked table set: the probe's default is the editable one.
        **({} if untick else {"untick": False}),
    )
    resumes = _cdc_source_type(session) is SourceType.POSTGRES and cdc_start_resumes(
        migration_state, watermark, SourceType.POSTGRES
    )
    if resumes and verdict is None and snapshot_mode != "never" and not unread:
        verdict = _probe_pg_resume_slot(
            migration_state, session, watermark, on_unread=unread.append
        )
    if unread and on_unread is not None:
        on_unread(unread[0])
    if (
        verdict is not None
        and _cdc_source_type(session) is SourceType.POSTGRES
        and _pg_stack_started(migration_state)
    ):
        # A stack started before: the route rule's one route, new topics first (G25).
        route = _pg_block_route(migration_state, verdict[0])
        verdict = (verdict[0], _with_route(verdict[1], route))
    elif (
        verdict is not None
        and force_initial is None
        and mode_forces_initial
        and getattr(watermark, "slot_name", None)
    ):
        from dsql_migrator.core.cdc_pg_slot import INHERITANCE_SNAPSHOT_HEADER

        if verdict[0] == INHERITANCE_SNAPSHOT_HEADER:
            # A stack never started whose Start snapshots only because the re-snapshot remedy
            # was taken, over a slot its Full Load recorded (G25): its way back is the Full
            # Load's slot -- the Start the block itself names -- through the card's control.
            verdict = (verdict[0], f"{verdict[1]} {PG_RESUME_FROM_SLOT_HINT}")
    return verdict


def _probe_pg_replication_objects(
    migration_state,
    session,
    tables,
    watermark,
    *,
    snapshot_mode: str = "never",
    on_unread: Optional[Callable[[str], None]] = None,
    untick: bool = True,
) -> Optional[tuple]:
    """Read-only: do CDC's publication + slot exist on the PostgreSQL source? (blocking)

    ``untick``: the table set can still change (the blocker's inheritance block names
    unticking the parent only then, G25).

    Returns the blocker's ``(header, body)`` or None. Fails CLOSED on "the catalog says
    absent/incomplete" and OPEN on "I could not ask" -- unreadable is not evidence of
    absence, and a missing publication makes the connector die LOUDLY anyway. But the slot
    position and the inheritance read guard against SILENT losses, so an open failure is
    never silent: ``on_unread`` is told why the source could not be read
    (:func:`_pg_checks_not_run_reason`, G25).

    The object names come from the SAME fallback ``dispatch_source_config`` uses (recorded
    on the watermark, else derived from the stack name), so the probe can never check a
    different object than the connector will. ``resumes_from_slot`` mirrors
    ``build_pg_source_config``'s invariant exactly -- the recorded slot, not the LSN -- so
    the check and the config agree by construction.

    Blocking I/O: callers MUST run it via ``run.io_bound`` (NEVER on the NiceGUI loop --
    one asyncio loop serves every browser session on Fargate).
    """
    if _cdc_source_type(session) is not SourceType.POSTGRES:
        return None
    source_config = getattr(session, "source_config", None)
    if source_config is None:
        return None
    stack = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    names = [getattr(t, "name", str(t)) for t in (tables or [])]
    # A Start that snapshots reads each captured table without ONLY, so the read also asks
    # which of them have inheritance child tables (G25).
    snapshots = snapshot_mode == "initial"
    try:
        from dsql_migrator.core import cdc_pg_slot as _pg
        from dsql_migrator.ui.connect import make_source_engine_factory

        pub = (
            getattr(watermark, "publication_name", None)
            or _pg.pg_publication_name(stack)
        )
        slot = getattr(watermark, "slot_name", None) or _pg.pg_slot_name(stack)
        engine = make_source_engine_factory(getattr(session, "source_password", None))(
            source_config
        )
        try:
            with engine.connect() as connection:
                objects = _pg.read_pg_replication_objects(
                    connection, publication_name=pub, slot_name=slot
                )
                if snapshots:
                    objects = _pg.with_inheritance_children(connection, objects, names)
        finally:
            engine.dispose()
    except Exception as exc:  # noqa: BLE001 - unreadable is NOT absent: no block, but say so
        if on_unread is not None:
            on_unread(_pg_checks_not_run_reason(session, exc))
        return None
    # MIRROR build_pg_source_config's two decisions exactly, or this refuses a
    # configuration that would have worked -- or, worse, passes one that cannot. Both are
    # read off the EFFECTIVE snapshot mode, through the same pure function the config uses:
    # ``initial`` means the connector does not resume from the slot and DOES create its own
    # publication, so neither absence blocks; ``never`` means it resumes from the
    # provisioned slot and may not touch the publication, so both absences block.
    autocreates = (
        pg_publication_autocreate_mode(snapshot_mode) == "filtered"
    )
    return _pg.pg_replication_objects_blocker(
        objects,
        names,
        resumes_from_slot=(
            bool(getattr(watermark, "slot_name", None)) and snapshot_mode == "never"
        ),
        connector_autocreates_publication=autocreates,
        # G25: on the stack's FIRST Start the slot must still be at the watermark's LSN
        # (another run with the same stack name may have re-created it). Judged by what the
        # stack records (_pg_stack_started), never by anything this process kept.
        start_lsn=getattr(watermark, "wal_lsn", None),
        stack_started=_pg_stack_started(migration_state),
        snapshots=snapshots,
        untick=untick,
    )


async def _open_cdc_start_dialog(
    ui, migration_state, on_confirm, *, session=None, job_manager=None, inventory=None,
    refresh_after_drop=None,
) -> None:
    """Confirm dialog before the (billable, partition-quota-using) Start.

    Two read-only pre-flight checks run first, so a doomed Start is caught before it
    consumes ~26 min of billable connector create and MSK partition quota:

    * :func:`cdc_deploy_connection_blocker` -- Start CDC builds the source
      connector's credentials secret from the in-memory source password (not
      restored after a restart) and needs a live target. Missing either is a hard
      block: the user reconnects first.
    * :func:`_probe_binlog_resume_gap` -- the binary log of the start point Start seeds
      (the Full Load watermark's, or a Manual one's) must still exist on the source, or
      streaming cannot begin there. This one is
      a **warning**, not a block: starting with a gap can be a deliberate choice,
      and the check degrades to silence whenever the answer is unknown.

    The binlog probe is blocking source I/O, so it runs via ``run.io_bound`` -- which
    is why this helper is async and MUST be awaited (an un-awaited coroutine would
    silently never open the dialog).
    """
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    conn_blocker = cdc_deploy_connection_blocker(session)
    overlay_changes: object = None
    binlog_gap: Optional[str] = None
    fk_block = None
    pg_objects_block: Optional[tuple] = None
    # The same gate for the Start the block's re-snapshot remedy would unlock (G25).
    pg_resnapshot_block: Optional[tuple] = None
    # Why the PostgreSQL publication/slot gate could not read the source, if it could not.
    pg_unread: list[str] = []
    # ...and why the re-snapshot route's own read could not (G25): the gate's checks ran then.
    pg_resnapshot_unread: list[str] = []
    # A PostgreSQL stack whose connectors were started before (stopped since, in place, or a
    # Start that CloudFormation rolled back, G25) continues from its slot whatever the Start
    # sends, so the re-snapshot remedy would NOT snapshot there: Start would go ahead and skip
    # every change since streaming stopped. Its block names the route rule's one route itself
    # (the gate's same test, _pg_stack_started), and no remedy is offered here.
    _pg_resumes = _cdc_source_type(session) is SourceType.POSTGRES and _pg_stack_started(
        migration_state
    )
    # The MySQL start-point check (cdc_start_point_gate): its verdict and notice.
    sp_verdict = None
    sp_notice: Optional[tuple] = None
    # The MySQL GTID start check (cdc_gtid_gate): its verdict and notice.
    gtid_verdict = None
    gtid_notice: Optional[tuple] = None
    inventory_tables = list(getattr(inventory, "tables", None) or [])
    # G17: a selected table not loaded under the Full Load's start point yet holds Start
    # (re-read here: another tab may have ticked one since the card rendered). No I/O; the
    # probes below are skipped then -- this Start cannot run anyway.
    _uncovered = (
        cdc_start_uncovered(
            migration_state, job_manager, _current_job(job_manager, migration_state.job_id)
        )
        if job_manager is not None
        else []
    )
    # G25 (A2): a MySQL Start that would move a stack that has streamed FORWARD, past where it
    # last started -- refused (no I/O; the job judges it again against a fresh describe). The
    # start-position probes below are then skipped: no start from this point can run here.
    newer: Optional[tuple] = None
    # The stack's last Start CloudFormation rolled back (G25): a refusal's remedy that takes a
    # new start point runs on a new stack.
    _rolled_back = cdc_start_rolled_back(migration_state)
    if job_manager is not None and _cdc_source_type(session) is not SourceType.POSTGRES:
        try:
            _a2_wm = _cdc_watermark(
                _current_job(job_manager, migration_state.job_id), migration_state
            )
            _a2_seed, _a2_origin = _cdc_start_seed(
                migration_state, _a2_wm, _cdc_source_type(session)
            )
            newer = newer_start_point_refusal(
                migration_state, _a2_seed, _cdc_source_type(session), origin=_a2_origin,
                job_watermark=_a2_wm,
            )
        except Exception:  # noqa: BLE001 - unknown here, like the probes; the job re-checks
            newer = None
    # G27: this Start begins at the watermark of a Full Load that is not this migration's first
    # copy -- the start-point card's condition, said again where Start is pressed (no I/O).
    reload_notice: Optional[tuple] = None
    if job_manager is not None and not _uncovered and newer is None:
        try:
            reload_notice = _reload_condition_notice(
                migration_state, _current_job(job_manager, migration_state.job_id), session
            )
        except Exception:  # noqa: BLE001 - advisory copy; never block the dialog
            reload_notice = None
    if job_manager is not None and not _uncovered:
        from nicegui import run

        try:
            binlog_gap = (
                None
                if newer is not None
                else await run.io_bound(
                    _probe_binlog_resume_gap, migration_state, job_manager, session
                )
            )
        except Exception:  # noqa: BLE001 - advisory only; never block the dialog
            binlog_gap = None
        # Enforced foreign keys on the target are a HARD block (unlike the binlog gap):
        # the sink dead-letters an out-of-order child row permanently and silently, so
        # "unknown" must block too. Blocking DSQL I/O -> run.io_bound, never on the loop.
        if not conn_blocker:
            _job = _current_job(job_manager, migration_state.job_id)
            _wm = _cdc_watermark(_job, migration_state)
            try:
                probed = await run.io_bound(
                    _probe_cdc_blocking_foreign_keys,
                    migration_state, session, inventory, _wm,
                )
            except Exception:  # noqa: BLE001 - probe itself failed -> cannot prove clean
                probed = None
                _log_cdc_event(
                    "foreign-key precondition unknown",
                    status=ActivityStatus.INFO,
                    detail=(
                        "could not read the target's foreign keys before Start CDC; "
                        "the job re-checks before creating any connector"
                    ),
                )
            if probed is not None and probed.blocking:
                fk_block = probed
            # MySQL: a recovery-mode start rebuilds Debezium's schema history from the
            # tables as they are NOW and reads OLDER binlog rows with it, so a captured
            # table changed since the start point breaks the stream -- loudly or SILENTLY.
            # Checked against the start point's fingerprint with one bounded catalog read;
            # a resume (schema_only) or a stack with connectors is not checked. Blocking
            # source I/O -> run.io_bound; the job re-checks on its worker thread.
            if _cdc_source_type(session) is not SourceType.POSTGRES and newer is None:
                # G19: on a gtid_mode=ON source Debezium starts after the GTID set seeded
                # with the start point, so a start point it cannot start exactly at is
                # refused here too (one bounded read; the job re-checks on its worker).
                try:
                    _g_seed, _g_origin, _g_facts = _mysql_start_context(
                        migration_state, job_manager, inventory, session
                    )[:3]
                    gtid_verdict = await run.io_bound(
                        _probe_cdc_gtid_start, migration_state, session,
                        seed=_g_seed, facts=_g_facts, origin=_g_origin,
                    )
                except Exception:  # noqa: BLE001 - unknown here; the job re-checks
                    gtid_verdict = None
                    _log_cdc_event(
                        "CDC GTID start-point check unknown",
                        status=ActivityStatus.INFO,
                        detail=(
                            "could not read the source's gtid_mode before Start CDC; the "
                            "job re-checks before creating any connector"
                        ),
                    )
                if gtid_verdict is not None and gtid_verdict.blocks:
                    from dsql_migrator.ui.cdc_gtid_gate import (
                        gtid_notice_point,
                        gtid_start_notice,
                    )

                    _g_label, _g_coord = gtid_notice_point(
                        gtid_verdict, seed=_g_seed, origin=_g_origin
                    )
                    gtid_notice = gtid_start_notice(
                        gtid_verdict,
                        point_label=_g_label,
                        coord=_g_coord,
                        origin=_g_origin,
                        migration_type=getattr(migration_state, "migration_type", None),
                        rolled_back=_rolled_back,
                        # G137: a fetched point is refused while a table read in parts is
                        # captured.
                        split_qualifier=_split_later_qualifier(
                            migration_state, job_manager,
                            _current_job(job_manager, migration_state.job_id),
                        ),
                    )
                try:
                    _sp_seed, _sp_origin, _sp_facts, _sp_mode, _sp_tables = (
                        _mysql_start_context(migration_state, job_manager, inventory, session)
                    )
                    sp_verdict = await run.io_bound(
                        _probe_cdc_start_point, migration_state, session,
                        seed=_sp_seed, mode=_sp_mode, facts=_sp_facts, tables=_sp_tables,
                    )
                except Exception:  # noqa: BLE001 - unknown here; the job re-checks
                    sp_verdict = None
                    _log_cdc_event(
                        "CDC start-point check unknown",
                        status=ActivityStatus.INFO,
                        detail=(
                            "could not check the captured tables against the CDC start "
                            "point before Start CDC; the job re-checks before creating any "
                            "connector"
                        ),
                    )
                if sp_verdict is not None:
                    from dsql_migrator.ui.cdc_start_point_gate import (
                        point_coord,
                        point_label,
                        start_point_notice,
                    )

                    sp_notice = start_point_notice(
                        sp_verdict,
                        point_label=point_label(_sp_seed, _sp_origin),
                        coord=point_coord(_sp_seed),
                        migration_type=getattr(migration_state, "migration_type", None),
                        # A checked resume (history missing) reads from the stack's own
                        # position, not from the start point it is compared with.
                        resumes=bool(getattr(_sp_facts, "resumes", False)),
                        # G25: a start point after the change needs new CDC topics on a
                        # stack whose connectors were started.
                        streamed=_rolled_back or cdc_stack_has_streamed(migration_state),
                        split_qualifier=_split_later_qualifier(
                            migration_state, job_manager,
                            _current_job(job_manager, migration_state.job_id),
                        ),
                    )
            # CDC only: list the overlay ALTERs Start CDC runs before any connector (one
            # live read). Full load + CDC reads nothing here: after the load they are
            # live, and the job verifies anyway. Never gates Start.
            if getattr(migration_state, "migration_type", None) is MigrationType.CDC_ONLY and inventory is not None:
                _captured = _cdc_tables_for_config(migration_state, inventory, _wm)
                if _captured and _has_captured_overlay(migration_state, inventory, _captured):
                    try:
                        overlay_changes = await run.io_bound(
                            _probe_cdc_overlay_changes,
                            migration_state, session, inventory, _captured,
                        )
                    except Exception:  # noqa: BLE001 - unknown: the job re-reads and blocks
                        overlay_changes = _OVERLAY_UNREAD
        # Whether the inheritance block may name unticking the parent: only while the table
        # picker can still change the set (G25).
        _pg_untick = _cdc_source_type(session) is SourceType.POSTGRES and _table_set_open(
            migration_state, job_manager, session
        )
        # The PostgreSQL publication/slot precondition. Checked HERE, before the operator
        # pays: starting without it deploys both connectors and the Debezium task dies on
        # "Publication autocreation is disabled" -- after the cdc-stack update. Blocking
        # source I/O -> run.io_bound, never on the loop. A source it cannot read does not
        # block, and is never silent either (G25): ``pg_unread`` gets why, shown below.
        try:
            _job2 = _current_job(job_manager, migration_state.job_id)
            _pg_wm = _cdc_watermark(_job2, migration_state)
            pg_objects_block = await run.io_bound(
                _pg_objects_gate_block,
                migration_state, session, inventory, _pg_wm,
                on_unread=pg_unread.append, untick=_pg_untick,
            )
        except Exception:  # noqa: BLE001 - unreadable is not absent; never block on it
            pg_objects_block = None
        # The block's "Re-snapshot every table instead" unlocks a Start that sends
        # snapshot.mode=initial, whose route has checks of its own -- the inheritance read
        # first, which a never-mode gate skips (G25). So that Start is judged too, before the
        # remedy is offered: its refusal is shown instead (the job is still the backstop).
        # Its read keeps its own reason: the block above came from checks that DID run.
        if pg_objects_block is not None and not _pg_resumes:
            from dsql_migrator.core.cdc_pg_slot import BLOCKS_WITH_THEIR_OWN_FIX

            if pg_objects_block[0] not in BLOCKS_WITH_THEIR_OWN_FIX:
                try:
                    pg_resnapshot_block = await run.io_bound(
                        _pg_objects_gate_block,
                        migration_state, session, inventory, _pg_wm,
                        on_unread=pg_resnapshot_unread.append, force_initial=True,
                        untick=_pg_untick,
                    )
                except Exception:  # noqa: BLE001 - unknown, like the gate; the job re-checks
                    pg_resnapshot_block = None
        if pg_unread and not conn_blocker:
            _log_cdc_event(
                PG_CHECKS_NOT_RUN_ACTION,
                status=ActivityStatus.WARNING,
                detail=(
                    f"could not read the PostgreSQL source before Start CDC ({pg_unread[0]}), "
                    "so its publication, replication slot and inheritance checks did not "
                    "run; the Start CDC job tries them again before it creates any "
                    "connector, and goes ahead without them if it cannot read the source "
                    "either"
                ),
            )
        elif pg_resnapshot_unread and not conn_blocker:
            _log_cdc_event(
                PG_RESNAPSHOT_NOT_CHECKED_ACTION,
                status=ActivityStatus.WARNING,
                detail=(
                    "could not read the PostgreSQL source a second time before Start CDC "
                    f"({pg_resnapshot_unread[0]}), so the checks of the Start that "
                    "'Re-snapshot every table instead' unlocks (its inheritance read) did not "
                    "run; the publication and replication slot checks ran. The Start CDC job "
                    "runs them before it creates any connector, and goes ahead without them if "
                    "it cannot read the source either"
                ),
            )
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 460px"):  # type: ignore[attr-defined]
        ui.label("Start CDC — create connectors").classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            f"This updates the cdc-stack '{stack_name}' to create the source and "
            "sink connectors for your selected tables and begin streaming. The MSK "
            "cluster, plugins and IAM role are reused (not changed). Each connector "
            "uses MSK Serverless partition quota, so avoid unnecessary retries."
        ).classes("text-sm text-gray-700")
        if conn_blocker:
            _render_notice(
                ui,
                tone="error",
                icon="link_off",
                header="Reconnect before starting CDC",
                body=conn_blocker,
            )
        if _uncovered:
            _render_notice(
                ui,
                tone="warning",
                header=START_UNCOVERED_HEADER,
                body=start_uncovered_body(
                    _uncovered, getattr(migration_state, "migration_type", None)
                ),
            )
        if binlog_gap:
            _render_notice(
                ui,
                tone="warning",
                icon="history_toggle_off",
                # "start point", not "snapshot": the probed log may be a Manual one.
                header="The start point's binary log has been purged",
                body=binlog_gap,
            )
        if newer is not None:
            _render_notice(
                ui, tone=newer[0][0], icon=newer[0][1], header=newer[0][2], body=newer[0][3]
            )
        elif reload_notice is not None:
            # G27: not a block -- the tool cannot tell whether that Full Load appended.
            _render_notice(ui, tone="warning", header=reload_notice[0], body=reload_notice[1])
        _gtid_blocks = bool(getattr(gtid_verdict, "blocks", False))
        if gtid_notice is not None:
            _render_notice(
                ui, tone=gtid_notice[0], icon=gtid_notice[1], header=gtid_notice[2],
                body=gtid_notice[3],
            )
        _sp_blocks = bool(getattr(sp_verdict, "blocks", False))
        if sp_notice is not None:
            _render_notice(
                ui, tone=sp_notice[0], icon=sp_notice[1], header=sp_notice[2],
                body=sp_notice[3],
            )
        _render_cdc_overlay_changes(ui, overlay_changes)
        if pg_unread and not conn_blocker:
            # The publication/slot gate could not read the source (G25): Start stays enabled
            # -- a missing publication or slot stops the connector loudly -- but the checks
            # against a silent loss did not run, and the operator sees which, and why.
            _render_notice(
                ui,
                tone="warning",
                icon="sync_problem",
                header=PG_CHECKS_NOT_RUN_HEADER,
                body=pg_checks_not_run_body(pg_unread[0]),
            )
        elif pg_resnapshot_unread and not conn_blocker:
            # Only the re-snapshot route's own read failed (G25): the block below came from
            # checks that ran, and its remedy stays offered, unchecked -- said as exactly that.
            _render_notice(
                ui,
                tone="warning",
                icon="sync_problem",
                header=PG_RESNAPSHOT_NOT_CHECKED_HEADER,
                body=pg_resnapshot_not_checked_body(pg_resnapshot_unread[0]),
            )
        # The PostgreSQL publication/slot block, with its remedy in the SAME dialog (same
        # shape as the FK slot below): a block whose only advice is "re-run the whole Full
        # Load" is a dead end, and the pre-flight results just computed would be thrown
        # away by closing. There is deliberately NO "accept the gap and stream from now"
        # option: the window starts at Full Load START, so it spans the entire load plus
        # all think time, the tool cannot say how many rows or which tables are affected,
        # and Validation's default ROW_COUNT mode would then certify the result clean --
        # an acknowledgement nobody can evaluate is a click-through, not consent.
        _pg_state = {"resolved": False}
        if pg_objects_block is not None:
            pg_slot_area = ui.column().classes("w-full gap-2")  # type: ignore[attr-defined]
            from dsql_migrator.core.cdc_pg_slot import (
                BLOCKS_WITH_THEIR_OWN_FIX,
                RESNAPSHOT_REFUSED,
            )

            # No remedy on a stack started before (_pg_resumes: the gate named its route), nor
            # for a block that names its own fix: a re-snapshot repairs neither
            # (cdc_pg_slot.BLOCKS_WITH_THEIR_OWN_FIX, G25).
            _pg_judged = (
                not _pg_resumes and pg_objects_block[0] not in BLOCKS_WITH_THEIR_OWN_FIX
            )
            _pg_block_body = pg_objects_block[1]
            if not _pg_resumes and not _pg_judged:
                # A block that names its own fix, on a stack never started (G25): the route
                # rule's way out after it -- a Full Load re-run after a narrowed publish list
                # is set back; nothing for a failed inheritance read, nor for the inheritance
                # block of this Start, whose own fix stands alone.
                _pg_block_body = _with_route(
                    _pg_block_body, _pg_block_route(migration_state, pg_objects_block[0])
                )
            _pg_refusal: Optional[tuple] = None
            if _pg_judged and pg_resnapshot_block is not None:
                # The re-snapshot's own Start is refused too (G25), so it is not offered, and
                # the route rule's way out follows: after the block itself when refused for the
                # same reason (a publication that misses a table, say), else after the refusal,
                # shown in a notice of its own.
                if pg_resnapshot_block[0] == pg_objects_block[0]:
                    _pg_block_body = _with_route(
                        _pg_block_body,
                        _pg_block_route(
                            migration_state, pg_objects_block[0], resnapshot=RESNAPSHOT_REFUSED
                        ),
                    )
                else:
                    _pg_refusal = (
                        pg_resnapshot_block[0],
                        _with_route(
                            f"{PG_RESNAPSHOT_REFUSED_LEAD} {pg_resnapshot_block[1]}",
                            _pg_block_route(
                                migration_state,
                                pg_resnapshot_block[0],
                                resnapshot=RESNAPSHOT_REFUSED,
                            ),
                        ),
                    )

            def _render_pg_block() -> None:
                pg_slot_area.clear()
                with pg_slot_area:
                    _render_notice(
                        ui,
                        tone="error",
                        icon="sync_problem",
                        header=pg_objects_block[0],
                        body=_pg_block_body,
                    )
                    if not _pg_judged:
                        return
                    if pg_resnapshot_block is not None:
                        if _pg_refusal is not None:
                            _render_notice(
                                ui,
                                tone="error",
                                icon="restart_alt",
                                header=_pg_refusal[0],
                                body=_pg_refusal[1],
                            )
                        return
                    _render_notice(
                        ui,
                        tone="info",
                        icon="restart_alt",
                        header="Re-snapshot instead — gapless, no second Full Load",
                        body=(
                            "Debezium can create its own replication slot, snapshot every "
                            "selected table and then stream from that slot. There is no "
                            "window between the snapshot and the stream, so nothing is "
                            "lost, and the target load is idempotent (INSERT ... ON "
                            "CONFLICT), so re-reading a row is safe. The cost is reading "
                            "the source tables again. If the publication is also missing, "
                            "the CONNECTOR's database user creates one over exactly the "
                            "captured tables — the tool itself never writes to your source."
                        ),
                    )

                    def _take_resnapshot() -> None:
                        # "manual" is the WHOLE decision now: it forces
                        # snapshot.mode=initial, from which publication.autocreate.mode
                        # =filtered is derived, so the connector's own DB user creates the
                        # publication. There is no second flag to forget. That Start's own
                        # checks passed when the dialog opened (pg_resnapshot_block), so the
                        # notice below may say it is unlocked; the job checks it again.
                        migration_state.set_cdc_start_mode("manual")
                        _pg_state["resolved"] = True
                        pg_slot_area.clear()
                        with pg_slot_area:
                            _render_notice(
                                ui,
                                tone="success",
                                icon="restart_alt",
                                header="Will re-snapshot every selected table",
                                body=(
                                    "CDC will snapshot the selected tables and then stream "
                                    "from the slot it creates, so the handoff has no gap. "
                                    "Start CDC is unlocked."
                                ),
                            )
                        start_btn.props(remove="disable")
                        start_btn.tooltip("")

                    ui.button(  # type: ignore[attr-defined]
                        "Re-snapshot every table instead",
                        icon="restart_alt",
                        on_click=_take_resnapshot,
                    ).props("color=primary")

        # Tracks whether the removal already happened, so Cancel can still refresh the card
        # behind the dialog (the drop is real even if the operator then backs out).
        _fk_state = {"cleared": False}
        if fk_block is not None:
            # The block notice and the Remove button live in a container that can be
            # REPLACED in place: the removal used to close the dialog and tell the operator
            # to "reopen Start CDC", which made a two-click job a four-click one and threw
            # away the pre-flight results (binlog probe, connection check) that had just
            # been computed. Now the same dialog reports the removal and unlocks Start.
            fk_slot = ui.column().classes("w-full gap-2")  # type: ignore[attr-defined]

            def _render_fk_block() -> None:
                fk_slot.clear()
                with fk_slot:
                    _render_notice(
                        ui,
                        tone="error",
                        icon="link",
                        header=_cdc_fk_block_reason(fk_block)[1],
                        body=_cdc_fk_block_body(fk_block),
                    )
                    if not fk_block.ours:
                        return

                    async def _remove_fks() -> None:
                        from nicegui import run

                        remove_btn.disable()
                        remove_btn.set_text("Removing…")
                        dropped, failed = await run.io_bound(
                            _drop_cdc_blocking_foreign_keys, fk_block.ours, session
                        )
                        if failed:
                            # Still blocked: an enforced FK that survived would make the
                            # stream dead-letter out-of-order child rows (23503), so Start
                            # must stay locked. Re-render the block with the outcome rather
                            # than closing -- the operator needs to see WHICH state they are
                            # in, and the remaining constraints are still listed above.
                            _fk_state["cleared"] = dropped > 0
                            fk_slot.clear()
                            with fk_slot:
                                _render_notice(
                                    ui,
                                    tone="error",
                                    icon="link",
                                    header=(
                                        f"{failed} foreign key(s) could not be removed — "
                                        "CDC is still blocked"
                                    ),
                                    body=(
                                        f"{dropped} removed, {failed} failed. The activity "
                                        "log names each one. Drop the remaining "
                                        "constraint(s) and reopen Start CDC; starting now "
                                        "would make the stream dead-letter out-of-order "
                                        "child rows (23503)."
                                    ),
                                )
                            return
                        # Cleared: report it HERE and unlock Start, so the operator carries
                        # straight on instead of re-opening the dialog and re-running the
                        # pre-flight checks.
                        _fk_state["cleared"] = True
                        # A start-point refusal (G5 or the GTID check) still stands after the
                        # FKs are gone, so the notice claims "ready" only when Start is
                        # really unlocked.
                        _unlocks = (
                            not conn_blocker
                            and not _sp_blocks
                            and not _gtid_blocks
                            and newer is None
                        )
                        fk_slot.clear()
                        with fk_slot:
                            _render_notice(
                                ui,
                                tone="success",
                                icon="link_off",
                                header=(
                                    f"{dropped} foreign key(s) removed — ready to start"
                                    if _unlocks
                                    else f"{dropped} foreign key(s) removed"
                                ),
                                body=(
                                    "The target no longer enforces the foreign keys this "
                                    "migration created, so the stream cannot dead-letter "
                                    "out-of-order child rows. Cut over re-creates them with "
                                    '"Apply foreign keys" once the stream has drained.'
                                    + (
                                        ""
                                        if _unlocks
                                        or not (_sp_blocks or _gtid_blocks or newer)
                                        else " Start CDC stays blocked by the start-point "
                                        "notice above."
                                    )
                                ),
                            )
                        if _unlocks:
                            start_btn.enable()
                            start_btn.props(remove="disable")
                            start_btn.tooltip("")

                    remove_btn = ui.button(  # type: ignore[attr-defined]
                        "Remove foreign keys", on_click=_remove_fks, icon="link_off"
                    ).props("color=negative")
                    remove_btn.tooltip(
                        "Drops exactly the "
                        f"{len(fk_block.ours)} foreign key(s) this migration created; the "
                        "cut-over 'Apply foreign keys' step re-creates them."
                    )

            _render_fk_block()

        def _go() -> None:
            start_btn.disable()
            start_btn.set_text("Submitting…")
            ui.notify(  # type: ignore[attr-defined]
                "Submitting Start CDC…", type="info", position="top",
            )
            dialog.close()
            on_confirm()

        def _dismiss() -> None:
            dialog.close()
            # The removal is REAL even if the operator then backs out, so the card behind
            # the dialog must stop showing the FK block. The close used to happen inside the
            # removal, which refreshed implicitly; now that the dialog stays open, the
            # refresh has to hang off the dismissal instead.
            if _fk_state["cleared"] and refresh_after_drop is not None:
                refresh_after_drop()

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=_dismiss).props("flat")  # type: ignore[attr-defined]
            start_btn = ui.button("Start CDC", on_click=_go).props("color=primary")  # type: ignore[attr-defined]
            if conn_blocker:
                start_btn.props("disable")
                start_btn.tooltip(conn_blocker)
            elif _uncovered:
                start_btn.props("disable")
                start_btn.tooltip(start_uncovered_body(
                    _uncovered, getattr(migration_state, "migration_type", None)))
            elif newer is not None:
                # Hard block (G25, A2): the sink would apply older changes over newer rows.
                start_btn.props("disable")
                start_btn.tooltip(newer[1])
            elif fk_block is not None:
                # Hard block: an enforced FK makes the stream discard child rows
                # silently, so Start stays unavailable until the FKs are gone.
                start_btn.props("disable")
                start_btn.tooltip(
                    "Enforced foreign keys on the target would dead-letter "
                    "out-of-order child rows (23503). Remove them first."
                    if (fk_block.ours or fk_block.foreign)
                    else "The target's foreign keys could not be read. CDC stays "
                    "blocked while this is unknown — re-test the target connection "
                    "and retry."
                )
            elif _gtid_blocks:
                # Hard block: on a gtid_mode=ON source this start point would make Debezium
                # re-read the retained binary log, or skip changes, or fail on every Start.
                from dsql_migrator.ui.cdc_gtid_gate import gtid_start_tooltip

                start_btn.props("disable")
                start_btn.tooltip(gtid_start_tooltip(gtid_verdict) or "")
            elif _sp_blocks:
                # Hard block: a recovery-mode start against a changed table replays the
                # same failure on every Start, or writes wrong values silently.
                from dsql_migrator.ui.cdc_start_point_gate import start_point_tooltip

                start_btn.props("disable")
                start_btn.tooltip(start_point_tooltip(sp_verdict) or "")
            elif pg_objects_block is not None and not _pg_state["resolved"]:
                start_btn.props("disable")
                start_btn.tooltip(pg_objects_block[1])
        # Rendered LAST because the remedy re-enables start_btn, which must exist first.
        if pg_objects_block is not None:
            _render_pg_block()
    dialog.open()

def _open_cdc_stop_dialog(
    ui, migration_state, on_confirm, *, partial: bool = False, session=None
) -> None:
    """Confirm dialog before removing the CDC connectors.

    The same backend action (delete the connectors) serves two situations, so the
    wording adapts: while streaming it is a "Stop CDC" (pause); after a failed
    Start that left only one connector (``partial=True``) nothing is streaming, so
    it is framed as cleaning up the leftover connector, not stopping a pipeline.
    """
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    if partial:
        title = "Clean up leftover connector"
        body = (
            f"This updates the cdc-stack '{stack_name}' to remove the leftover CDC "
            "connector from the failed Start (nothing is streaming yet). MSK, the "
            "VPC wiring and the plugins are kept, so you can Start CDC again "
            "afterwards. MSK Connect has no pause, so cleanup means deleting the "
            "connector."
        )
        confirm_label = "Clean up connector"
    else:
        title = "Stop CDC — remove connectors"
        # Spell out that the POSITION survives, not just the infrastructure. "You can
        # restart with Start CDC" left the operator to guess whether stopping cost them
        # their place in the binlog -- and the reasonable guess (that it does, since the
        # connectors are deleted) is wrong. That guess is expensive: it invites re-entering
        # coordinates by hand, or re-running the Full Load, to recover something the
        # connector never lost.
        _is_pg = _cdc_source_type(session) is SourceType.POSTGRES
        # The resume is gapless only while the CDC topics retain what the sink had not
        # applied (G25): Stop deletes the source and the sink in parallel, with no drain, and
        # the sink resumes from its last commit -- both engines. PostgreSQL also names the
        # route rule's way out past that period, as the resume cards do.
        if _is_pg:
            from dsql_migrator.core.cdc_pg_slot import pg_cdc_route

            _topics = (
                f"{_STOP_TOPICS_CONDITION}: once their retention period on MSK has passed, a "
                "Start would skip them, so instead "
                + pg_cdc_route(
                    None,
                    started=True,
                    cdc_only=getattr(migration_state, "migration_type", None)
                    is MigrationType.CDC_ONLY,
                )
                + "."
            )
        else:
            _topics = f"{_STOP_TOPICS_CONDITION} (their retention period on MSK)."
        body = (
            f"This updates the cdc-stack '{stack_name}' to delete the two CDC "
            "connectors and stop streaming. MSK Connect has no pause, so stopping means "
            "deleting the connectors — but MSK, the VPC wiring and the plugins are kept, "
            "and so is the recorded stream position. Start CDC re-creates the connectors "
            + (
                # PostgreSQL (G25, like the start line): a source connector stopped before
                # its initial snapshot completed has not streamed, and the next Start
                # (snapshot.mode=initial) takes that snapshot again.
                "and a source connector that has streamed continues from where streaming "
                "stopped, with no Full Load or start point needed again. One still "
                "taking its initial snapshot has not streamed yet: stopping it now cancels "
                "that snapshot, and the next Start snapshots every selected table again "
                "first."
                if _is_pg
                else "and continues from exactly where streaming stopped, with no Full Load "
                "or start point needed again."
            )
            + f" {_topics}"
            + " Restart as often as you like — but do not LEAVE it stopped."
        )
        if _is_pg:
            # WHY the position survives is also why the source keeps paying for it: the
            # connector config sets slot.drop.on.stop=false, so the replication slot stays
            # behind with no consumer attached -- and an inactive slot still pins every WAL
            # segment written from its restart_lsn forward. At PostgreSQL's DEFAULT
            # max_slot_wal_keep_size = -1 (measured on a live Aurora PostgreSQL source)
            # there is NO server-side reclaim, so that retention is unbounded until the
            # slot is dropped, which only Delete CDC infrastructure does. The old wording
            # ("You can stop and restart as often as you like") said nothing about any of
            # it, and the one place the tool warned about slot health was unreachable in
            # exactly this state -- it renders only while CDC is streaming.
            body += (
                " On PostgreSQL the recorded position IS a replication slot, and the slot "
                "is deliberately kept — so while CDC is stopped the source retains every "
                "WAL segment the slot still needs. PostgreSQL does not reclaim it on its "
                "own at the default max_slot_wal_keep_size, so source disk keeps growing "
                "for as long as you stay stopped. Restarting drains it; if you are done, "
                "use Delete CDC infrastructure, which drops the slot."
            )
            # pgoutput carries no DDL, so the backlog is decoded against the CURRENT
            # catalog when CDC continues: a key / NULL-rule change made while stopped can
            # be applied to the earlier changes too (case 12f; jar-verified, not
            # live-verified -- hence "may" and the CHECKSUM advice).
            body += (
                " A schema change (ALTER TABLE) to a captured table while CDC is stopped "
                "is not replayed in order: when CDC continues, the changes made before it "
                "may be applied with the table's current key and NULL rule. Make the same "
                "follow-up change on Aurora DSQL as for a live change, and validate the "
                "table with CHECKSUM afterwards."
            )
        else:
            # MySQL: a restart RESUMES with snapshot.mode=schema_only, replaying Debezium's
            # recorded schema history in binlog order -- so a DDL made while stopped is
            # applied in order like a live one (never as if it had always been there), and
            # needs the same follow-up as one.
            body += (
                " On MySQL a restart replays Debezium's recorded schema history, so a "
                "schema change (ALTER TABLE) to a captured table while CDC is stopped is "
                "applied in order when it continues, exactly as if it had kept streaming: "
                "it needs the same follow-up on Aurora DSQL, and the CDC step reports it as "
                "schema drift."
            )
        # A connector that stopped for a known cause: "no start point needed again" is
        # not true for every cause (a stop the recorded position replays), so say so.
        _held = getattr(migration_state, "cdc_connector_diagnoses", None) or {}
        _wired = list(getattr(migration_state, "cdc_connector_names", None) or [])
        _cause = next(((n, _held[n]) for n in _wired if n in _held), None)
        if _cause is not None:
            body += (
                f" {connector_role_label(_cause[0])} stopped: {_cause[1].headline}. "
                "Follow the steps the CDC step shows for it before Start CDC — for some "
                "causes a Start from the recorded position stops the same way."
            )
        confirm_label = "Stop CDC"
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 460px"):  # type: ignore[attr-defined]
        ui.label(title).classes("text-lg font-semibold")  # type: ignore[attr-defined]
        ui.label(body).classes("text-sm text-gray-700")  # type: ignore[attr-defined]

        def _go() -> None:
            dialog.close()
            on_confirm()

        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
            ui.button(confirm_label, on_click=_go).props("color=amber-8")  # type: ignore[attr-defined]
    dialog.open()

def _pg_delete_disclosure(migration_state, job) -> str:
    """What Delete CDC infrastructure does to a PostgreSQL source, for its confirm dialog (G27),
    as ``cdc_deployer.run_cdc_delete`` does it: once the stack is deleted (its connectors with
    it, so the slot is inactive), ``_drop_pg_source_replication`` drops the stack's replication
    slot -- its deployed ``PgSlotName``, which the stack-phase probe reads, else the name derived
    from the stack name -- then its publication, each only if it exists; a drop that fails
    leaves the teardown complete and logs the statements to run by hand. The publication is
    named when its name is known: the derived one beside the derived slot, or the one the Full
    Load (``job``) recorded beside its slot -- the two are always created together.

    What a later Start then needs, on a new stack (never started): when that slot is the one
    the Full Load recorded, the route of a missing slot (``pg_cdc_route``); when the stack
    streamed and no Full Load recorded a slot, a snapshot of every selected table, which its
    Start takes on its own. Otherwise nothing is said: no slot of this migration's start point
    is dropped, or the Full Load's is another one, which the teardown leaves. Pure."""
    from dsql_migrator.core.cdc_pg_slot import (
        SLOT_MISSING_HEADER,
        pg_cdc_route,
        pg_publication_name,
        pg_slot_name,
    )

    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    derived = pg_slot_name(stack_name)
    slot = (getattr(migration_state, "cdc_deployed_slot_name", None) or "").strip() or derived
    recorded = getattr(job, "watermark", None) if job is not None else None
    load_slot = getattr(recorded, "slot_name", None)
    loads_slot = load_slot == slot
    if slot == derived:
        publication = pg_publication_name(stack_name)
    else:
        publication = getattr(recorded, "publication_name", None) if loads_slot else None
    named = f" '{publication}'" if publication else ""
    text = (
        "On PostgreSQL, once the stack is deleted, the teardown also drops this stack's "
        f"replication slot '{slot}' and its publication{named} on the source (each only if it "
        "exists), which releases the WAL the slot retains. If it cannot drop them, the teardown "
        "still completes and its log gives the statements to run on the source."
    )
    if loads_slot:
        return (
            f"{text} A slot cannot be created again at a past position, so after you deploy "
            "again, Start CDC cannot resume from the Full Load's slot. "
            + pg_cdc_route(
                SLOT_MISSING_HEADER,
                started=False,
                cdc_only=getattr(migration_state, "migration_type", None)
                is MigrationType.CDC_ONLY,
            )
        )
    if not load_slot and _pg_stack_started(migration_state):
        return (
            f"{text} A slot cannot be created again at a past position, so after you deploy "
            "again, Start CDC on the new stack snapshots every selected table before it streams "
            "(a row deleted on the source in between stays on the target; Validation reports it "
            "as an EXTRA row)."
        )
    return text


def _open_cdc_delete_dialog(
    ui, migration_state, on_confirm, *, session=None, job=None
) -> None:
    """Type-to-confirm dialog before deleting the whole cdc-stack. ``job``: the Full Load
    record, whose slot a PostgreSQL teardown may drop (:func:`_pg_delete_disclosure`)."""
    stack_name = getattr(migration_state, "cdc_stack_name", CDC_DEFAULT_STACK_NAME)
    # Was a tool-managed source secret created (password auth), or is the source
    # secret the customer's own Secrets Manager reference (SM auth)?
    uses_sm_auth = bool(getattr(session, "source_secret_id", None))
    with ui.dialog() as dialog, ui.card().classes("gap-2").style("min-width: 480px"):  # type: ignore[attr-defined]
        ui.label("Delete CDC infrastructure").classes("text-lg font-semibold text-red-700")  # type: ignore[attr-defined]
        ui.label(  # type: ignore[attr-defined]
            "This deletes the entire cdc-stack — MSK, the VPC wiring, plugins, IAM "
            "role and any connectors. This cannot be undone."
        ).classes("text-sm text-gray-700")
        if _cdc_source_type(session) is not SourceType.POSTGRES:
            # The offsets AND the schema-history topics live on MSK, so a redeployed stack
            # starts again from the original start point with snapshot.mode=recovery --
            # which the start-point check refuses if a captured table changed since (it
            # only warns for a start point with no recorded fingerprints).
            ui.label(  # type: ignore[attr-defined]
                "On MySQL this also deletes the recorded stream position and Debezium's "
                "schema history: after you deploy again, Start CDC reads the binary log "
                "again from the Full Load watermark (or your Manual start point). It "
                "refuses if a captured table's columns changed since then, but only warns "
                "if that start point has no recorded table definitions (one recorded by an "
                "earlier version of the tool or typed by hand)."
            ).classes("text-xs text-gray-500")
        else:
            # G27: the teardown drops the slot and publication on the source, which this
            # dialog never said (live: both were gone after the delete).
            ui.label(  # type: ignore[attr-defined]
                _pg_delete_disclosure(migration_state, job)
            ).classes("text-xs text-gray-500")
        # Show the exact stack name in a copyable mono box so the operator can see
        # (and copy) precisely what to type -- relying on the input placeholder
        # alone is a trap (it looks pre-filled but is empty).
        ui.label("Stack name to confirm:").classes("text-xs text-gray-500 mt-1")  # type: ignore[attr-defined]
        ui.code(stack_name).classes("w-full text-sm")  # type: ignore[attr-defined]
        # Disclose what happens to the source-credentials secret (Property 7: the
        # operator must know whether their DB credentials are removed or retained).
        if uses_sm_auth:
            ui.label(  # type: ignore[attr-defined]
                "Your Secrets Manager source secret is left untouched (the tool "
                "never created it)."
            ).classes("text-xs text-gray-500")
        else:
            from dsql_migrator.core.secrets import cdc_source_secret_name

            secret_name = cdc_source_secret_name(stack_name)
            ui.label(  # type: ignore[attr-defined]
                f"The source-credentials secret the tool created for CDC "
                f"('{secret_name}') will also be scheduled for deletion with a "
                "7-day recovery window, so your database credentials do not linger "
                "in Secrets Manager."
                + (
                    # G27 repair: as cdc_deployer.run_cdc_delete does it -- a slot it could
                    # not drop keeps the secret, the stored credentials that can drop it.
                    " If the replication slot cannot be dropped, the teardown keeps the "
                    "secret instead: it holds the credentials needed to drop the slot, so "
                    "delete it yourself once the slot is gone."
                    if _cdc_source_type(session) is SourceType.POSTGRES
                    else ""
                )
            ).classes("text-xs text-amber-700")
        # Create the widgets first, then wire the live check -- so the handler can
        # safely reference all three even if on_change fires immediately.
        confirm_input = ui.input(  # type: ignore[attr-defined]
            label="Type the stack name above to enable Delete",
            placeholder=stack_name,
        ).classes("w-full")
        mismatch_hint = inline_hint(ui, "", tone="warning")  # type: ignore[attr-defined]
        del_btn = ui.button("Delete", icon="delete_forever").props("color=negative")  # type: ignore[attr-defined]
        del_btn.disable()

        # Gate the Delete button on an exact match, with a live near-miss hint.
        # NiceGUI's on_value_change delivers the current value to Python (the raw
        # DOM "input" event does not), so the gate reacts as the user types.
        def _check(_e=None) -> None:
            typed = (confirm_input.value or "").strip()
            if typed == stack_name:
                del_btn.enable()
                mismatch_hint.set_text("")
            else:
                del_btn.disable()
                mismatch_hint.set_text(
                    f"Doesn't match '{stack_name}' yet." if typed else ""
                )

        confirm_input.on_value_change(_check)

        def _go() -> None:
            if (confirm_input.value or "").strip() != stack_name:
                return
            dialog.close()
            on_confirm()

        del_btn.on("click", _go)
        with ui.row().classes("justify-end gap-2 w-full"):  # type: ignore[attr-defined]
            ui.button("Cancel", on_click=dialog.close).props("flat")  # type: ignore[attr-defined]
    dialog.open()

def _full_load_committed(job, migration_state) -> bool:
    """True when a Full Load has run for this table set (so its lock clause wins).

    Mirrors ``selection_lock_reason``'s FIRST clause (``has_job or status is DONE``),
    which takes precedence over the CDC-infrastructure clause -- and, unlike it, is NOT
    released by deleting the cdc-stack. The distinction decides which remedy the CDC
    notices may offer: telling an operator who has already loaded to "delete the CDC
    infrastructure to re-scope" would cost a ~45 min teardown and leave the picker just
    as locked, because the export really did run against this set (only Start over
    clears that).

    ``job`` is the current Full Load job (the caller already resolved it) -- its mere
    existence is the lock's ``has_job``. A migration type without a ``"full_load"``
    sub-step can never trip that clause, so CDC-only is excluded outright rather than
    inferred. Deliberately conservative: when unsure this returns False, which offers
    the cheaper (delete-and-redeploy) remedy -- wrong-but-recoverable, versus sending
    someone to Start over who did not need it.

    Full load only lifts that clause when no CDC stack exists (G12), but it has no CDC
    sub-step, so none of the notices this feeds ever renders for it. Full load + CDC lifts
    it until CDC first starts (G16, G17: a table may be dropped, or added and loaded under
    the Full Load's start point); the notices that read this say so while that holds, and
    once CDC has started adding a table is a Start over again, so the remedy this chooses
    still holds there.
    """
    from dsql_migrator.ui.data_migration._models import substeps_for_type

    try:
        if "full_load" not in substeps_for_type(migration_state.migration_type):
            return False
    except Exception:  # noqa: BLE001 - unknown type: fall through to the job check
        pass
    return job is not None


def _sink_mcu_count() -> int:
    """The operator's configured sink MCU count, read FRESH at deploy time.

    Read here rather than captured at import/render so a change made in Settings ->
    Performance -> CDC is picked up by the very next Start CDC without a restart
    (``load_config`` re-reads the environment, which is where ``set_tuning_value``
    writes). Falls back to the template-matching default if the config cannot be
    read, so a config problem can never block a deploy or silently resize a
    connector.
    """
    from dsql_migrator.core.cdc import CDC_DEFAULT_SINK_MCU_COUNT

    try:
        from dsql_migrator.config import load_config

        return int(load_config().cdc_sink_mcu_count)
    except Exception:  # noqa: BLE001 - never block a deploy on a config read
        return CDC_DEFAULT_SINK_MCU_COUNT


# The CDC Start dialog could not read the target for the overlay statements.
_OVERLAY_UNREAD = object()


def _has_captured_overlay(migration_state, inventory, captured) -> bool:
    from dsql_migrator.ui.data_migration._models import overlay_lob_exclusions

    wanted = {t.name for t in captured}
    return any(t in wanted for t in overlay_lob_exclusions(migration_state, inventory))


def _render_cdc_overlay_changes(ui, overlay_changes) -> None:
    """The CDC-only Start dialog's overlay notice: the exact ALTERs in run order, or that
    the target could not be read (the job then reads it itself), and which tables keep
    the values they already hold. Nothing when nothing is excluded."""
    if overlay_changes is None:
        return
    if overlay_changes is _OVERLAY_UNREAD:
        _render_notice(
            ui,
            tone="warning",
            header="Could not read the Aurora DSQL target",
            body=(
                "The changes to the excluded columns cannot be listed here. Start CDC "
                "reads the target itself before creating any connector and stops, "
                "changing nothing, if it cannot make them."
            ),
        )
        return
    statements = list(getattr(overlay_changes, "statements", ()) or ())
    with_rows = tuple(getattr(overlay_changes, "tables_with_rows", ()) or ())
    kept = (
        f"Rows already in {', '.join(with_rows)} keep what they hold in the excluded "
        "columns: CDC never writes them."
        if with_rows
        else ""
    )
    if not statements:
        if kept:
            _render_notice(
                ui, tone="info", header="Excluded columns on Aurora DSQL", body=kept
            )
        return
    from dsql_migrator.ui.data_migration._models import overlay_change_summary

    _render_notice(
        ui,
        tone="warning",
        icon="table_chart",
        header="Aurora DSQL changes when CDC starts: "
        + overlay_change_summary(st.action for st in statements),
        body=(
            "They run before any connector is created, so every row CDC inserts holds NULL "
            "in the excluded columns. Aurora DSQL cannot add NOT NULL back in place."
            + (f" {kept}" if kept else "")
        ),
    )
    with ui.expansion("show statements").classes("w-full text-xs"):  # type: ignore[attr-defined]
        for statement in statements:
            # "(DSQL)": these run on the target, never on the source.
            ui.label(f"(DSQL) {statement.text()}").classes("text-xs font-mono wrap-anywhere")  # type: ignore[attr-defined]


def _note_cdc_start(migration_state) -> None:
    """Tell the cut-over area that CDC starts (or tried to): a drained attestation made
    before is stale. Best effort; absent without a Validation store."""
    callback = getattr(migration_state, "on_cdc_start", None)
    if callable(callback):
        try:
            callback()
        except Exception:  # noqa: BLE001 - never fail a Start on a UI notification
            logging.getLogger(__name__).warning("on_cdc_start notification failed", exc_info=True)


def _start_cdc_overlay_plan(migration_state, inventory, tables_for_config, session, target) -> dict:
    """The Start CDC overlay's inputs, computed on the loop with no I/O."""
    from dsql_migrator.core.overlay_ledger import overlay_ledger_key
    from dsql_migrator.ui.data_migration._models import (
        effective_lob_exclusions,
        overlay_lob_exclusions,
    )

    captured = [t.name for t in tables_for_config]
    wanted = set(captured)
    overlay = {
        t: c for t, c in overlay_lob_exclusions(migration_state, inventory).items() if t in wanted
    }
    excluded = {
        t: c for t, c in effective_lob_exclusions(migration_state, inventory).items() if t in wanted
    }
    sources = {t.name: [c.name for c in t.columns] for t in tables_for_config}
    ddls = getattr(migration_state, "effective_target_ddls", None)
    ledger = getattr(migration_state, "overlay_ledger", None)
    ledger_key = None
    if ledger is not None and target is not None:
        try:
            ledger_key = overlay_ledger_key(target)
        except Exception:  # noqa: BLE001 - no key -> the overlay blocks if it needs one
            ledger_key = None
    return {
        "overlay_by_table": overlay,
        "captured": captured,
        "excluded_by_table": excluded,
        "source_columns_for": lambda name: sources.get(name, []),
        "ledger": ledger,
        "ledger_key": ledger_key,
        "ledger_in_use": bool(getattr(migration_state, "overlay_ledger_in_use", False)),
        # Lazy: the connector is built on the job thread, only if the overlay needs it --
        # and bound to the SAME target the ledger key and the sink parameters name, so a
        # target changed on Connect meanwhile can never be the one ALTERed.
        "connection_factory": (
            (lambda: _dsql_connect_for(target, getattr(session, "aws_profile", None))())
            if target is not None
            else None
        ),
        "effective_target_ddls": (lambda: ddls(inventory)) if callable(ddls) else (lambda: None),
        "log": _log_cdc_event,
    }


def _dsql_connect_for(target, aws_profile):
    """``DsqlConnector(target).connect`` for one fixed target (built when called)."""
    from dsql_migrator.core.target_connection import DsqlConnector

    return DsqlConnector(target, aws_profile=aws_profile).connect


def _same_target(a, b) -> bool:
    """The same Aurora DSQL cluster and database (the identity a Start CDC acts on)."""
    if a is None or b is None:
        return a is b
    return all(
        getattr(a, field, None) == getattr(b, field, None)
        for field in ("cluster_endpoint", "database", "region")
    )


def _apply_start_cdc_overlay(handle, **plan) -> None:
    from dsql_migrator.ui.data_migration._cdc_overlay import apply_start_cdc_overlay

    apply_start_cdc_overlay(handle, **plan)


def _probe_cdc_overlay_changes(migration_state, session, inventory, tables_for_config):
    """The overlay statements a CDC-only Start will run (one live read). BLOCKING."""
    from dsql_migrator.ui.data_migration._cdc_overlay import probe_cdc_overlay_changes
    from dsql_migrator.ui.data_migration._models import overlay_lob_exclusions

    wanted = {t.name for t in tables_for_config}
    overlay = {
        t: c for t, c in overlay_lob_exclusions(migration_state, inventory).items() if t in wanted
    }
    return probe_cdc_overlay_changes(
        overlay, connection_factory=_cdc_fk_connection_factory(session)
    )


def _cdc_fk_connection_factory(session):
    """Return a fresh-DSQL-connection factory for the target, or ``None``.

    Same seam the cut-over FK apply uses (``DsqlConnector(...).connect``), so the
    precondition reads and the drop run against exactly the target that will be
    streamed to.
    """
    target = getattr(session, "target_config", None)
    if target is None:
        return None
    from dsql_migrator.core.target_connection import DsqlConnector

    return DsqlConnector(
        target, aws_profile=getattr(session, "aws_profile", None)
    ).connect


def cdc_foreign_keys_in_stream_scope(preserved, streamed, inventory):
    """The owned FKs the stream can trip: those on a streamed table, and every key of a
    child table whose foreign key references a streamed table. Pure.

    ``preserved`` (what this migration's conversion renders) covers the whole evaluated
    inventory, so a selection of 5 tables was blocked -- with Start CDC disabled -- by the
    keys of unrelated tables elsewhere on the target that no streamed row can reach (live:
    ``ecommerce_demo`` FKs blocked a ``migration_schema`` fixture stream). Kept at TABLE
    level from the source inventory's keys, never per constraint name, so a renamed
    constraint is never missed. Fail-closed: a table the inventory does not describe is
    kept, and without an inventory nothing is narrowed.
    """
    inventory_tables = getattr(inventory, "tables", None)
    if not inventory_tables:
        return dict(preserved)
    streamed_set = set(streamed)
    keep = set(streamed_set)
    for table in inventory_tables:
        for foreign_key in getattr(table, "foreign_keys", None) or ():
            if getattr(foreign_key, "referenced_table", None) in streamed_set:
                keep.add(table.name)
                break
    known = {table.name for table in inventory_tables}
    return {
        table: names for table, names in preserved.items() if table in keep or table not in known
    }


# The target side of a :func:`_probe_cdc_generated_mismatch` entry for a table the target
# does not have: CDC would dead-letter every change to it.
_TARGET_TABLE_MISSING = "missing"


def _probe_cdc_generated_mismatch(migration_state, session, inventory, watermark):
    """``[(table, CDC leaves out, the target computes)]`` for each streamed table whose
    GENERATED columns the capture (read from the applied Schema Conversion DDL) and the
    LIVE target disagree on -- the target side ``None`` when its catalog could not be read,
    :data:`_TARGET_TABLE_MISSING` when the table is not there. BLOCKING I/O, one connection
    for every captured table.

    The capture set follows the DDL in Schema Conversion, but a GENERATED edit that never
    reached the target (an edit after the apply, a cancelled Replace, a target built
    outside the tool) leaves an ordinary column there: CDC would stop writing it -- inserts
    NULL or DEFAULT, updates stale -- with nothing dead-lettered, while Full Load's append
    path, which reads the live catalog, wrote it. The other way round (the target computes
    a column CDC sends) every change is refused (428C9). The target's columns are mapped to
    the source's as the capture maps them (exact, then a unique case-fold); a column only
    the target has is no concern of CDC's, which never sends it.

    Only tables with a generated column on the source or in Schema Conversion are read (a
    target that computes a source GENERATED column Schema Conversion made ordinary
    included): no connection at all for a schema without one. A target built outside the
    tool that computes some other column still dead-letters visibly, 428C9, which the
    dead-letter banner names with its fix."""
    from dsql_migrator.core.converter import parse_target_generated_columns
    from dsql_migrator.core.target_introspector import target_generated_columns_of
    from dsql_migrator.ui.data_migration._models import (
        applied_conversions_of,
        target_generated_capture_columns,
    )

    applied = applied_conversions_of(migration_state)

    def _generated_anywhere(table) -> bool:
        ddl = getattr((applied or {}).get(table.name), "target_ddl", None)
        return (
            any(getattr(c, "generated", False) for c in table.columns)
            or bool(target_generated_capture_columns(table, applied))
            or bool(ddl and parse_target_generated_columns(ddl))
        )

    tables = [
        t for t in (_cdc_tables_for_config(migration_state, inventory, watermark) or ())
        if _generated_anywhere(t)
    ]
    if not tables:
        return []
    try:
        connect = _cdc_fk_connection_factory(session)
    except Exception:  # noqa: BLE001 - no usable target: unknown, never agreeing
        connect = None
    if connect is None:
        return []  # no target to read: Start CDC's connection checks stop it first
    read = target_generated_columns_of([t.name for t in tables], connection_factory=connect)
    # A PostgreSQL publication never carries a column generated on the source, so a start
    # that only STREAMS (from the gapless slot: snapshot.mode=never) never sends one. An
    # initial snapshot does: it SELECTs every captured column, so a target that computes
    # one Schema Conversion keeps ordinary refuses each snapshot row (428C9) -- as the
    # Start config decides it (pg_snapshot_mode), and unknown counts as a snapshot.
    pg_streams_only = False
    if _cdc_source_type(session) is SourceType.POSTGRES:
        from dsql_migrator.core.cdc_postgres import pg_snapshot_mode

        try:
            resume_override, force_initial = _cdc_resume_signal(migration_state, session)
            pg_streams_only = (
                pg_snapshot_mode(
                    watermark,
                    resume_override=resume_override,
                    force_initial_snapshot=force_initial,
                )
                == "never"
            )
        except Exception:  # noqa: BLE001 - an unknown start mode is taken as a snapshot
            pg_streams_only = False
    mismatched: list = []
    for table in tables:
        expected = set(target_generated_capture_columns(table, applied))
        if table.name in read.unreadable:
            # Unknown is never taken as agreeing where CDC leaves a column out; a table
            # with nothing generated in Schema Conversion is not held on an unread catalog.
            if expected:
                mismatched.append((table.name, sorted(expected), None))
            continue
        if table.name in read.missing:
            mismatched.append((table.name, sorted(expected), _TARGET_TABLE_MISSING))
            continue
        exact = {c.name for c in table.columns}
        folded: dict[str, list[str]] = {}
        for column in table.columns:
            folded.setdefault(column.name.lower(), []).append(column.name)
        on_target = read.names.get(table.name, set())
        live: set[str] = set()
        for name in read.columns[table.name]:
            if name in exact:
                live.add(name)
            elif len(folded.get(name.lower(), ())) == 1:
                source = folded[name.lower()][0]
                # Not when the target has that exact name as a column of its own: a
                # target-only "total" beside the source's "Total" is another column.
                if source not in on_target:
                    live.add(source)
        live -= set(getattr(table, "primary_key", None) or ())
        if pg_streams_only:
            # Streamed only, a column generated on the source is never sent: the target
            # computing it is no 428C9, and is what keeps it current (an ordinary one there
            # would be left stale).
            live -= {c.name for c in table.columns if getattr(c, "generated", False)} - expected
        if live != expected:
            mismatched.append((table.name, sorted(expected), sorted(live)))
    return mismatched


def _cdc_generated_mismatch_notice(
    mismatched,
    *,
    is_pg: bool = False,
    cdc_only: bool = False,
    source_generated: "Optional[set]" = None,
) -> tuple:
    """The Start CDC refusal for :func:`_probe_cdc_generated_mismatch`. ``cdc_only``: the
    migration has no Full Load to reload with; ``source_generated``: the
    ``(table, column)`` pairs GENERATED on the source, which a PostgreSQL publication
    never carries."""
    unreadable = [t for t, _e, live in mismatched if live is None]
    missing = [t for t, _e, live in mismatched if live == _TARGET_TABLE_MISSING]
    differing = [
        (t, e, live) for t, e, live in mismatched
        if live is not None and live != _TARGET_TABLE_MISSING
    ]

    def _listed(names) -> str:
        return ", ".join(names[:5]) + (f" and {len(names) - 5} more" if len(names) > 5 else "")

    # The tables Start CDC could not check, said apart from a real difference: an unread
    # catalog wants the connection checked, a missing table an apply -- never a Replace.
    unchecked = []
    if missing:
        them = "it" if len(missing) == 1 else "them"
        unchecked.append(
            f"{_listed(missing)} {'does' if len(missing) == 1 else 'do'} not exist on the "
            f"target, so CDC would dead-letter every change to {them}: apply {them} from "
            "Schema Conversion"
            + (
                # A CDC-only migration's set is fixed once its infrastructure exists, and
                # the rows already loaded by other means have to be loaded again.
                f", then load {them} again the way "
                f"{'it was' if len(missing) == 1 else 'they were'} first loaded."
                if cdc_only
                else f", then reload {them} with the Full Load step's per-table 'Drop & "
                "reload' (the rows the Full Load wrote went with the table)."
            )
        )
    if unreadable:
        unchecked.append(
            f"Start CDC could not read the columns of {_listed(unreadable)} on the target, so "
            "it cannot tell whether CDC would write a column the target computes: check the "
            "target connection and Start CDC again."
        )
    lines = []
    for table, expected, live in differing[:5]:
        left_out = [c for c in expected if c not in live]
        sent = [c for c in live if c not in expected]
        parts = []
        if left_out:
            parts.append(
                f"{', '.join(left_out)} GENERATED in Schema Conversion but ordinary on the "
                "target (CDC would stop writing it)"
            )
        snapshot_sent = [
            c for c in sent if is_pg and (table, c) in (source_generated or set())
        ]
        sent = [c for c in sent if c not in snapshot_sent]
        if sent:
            parts.append(
                f"{', '.join(sent)} GENERATED on the target but not in Schema Conversion "
                "(every change would be refused, 428C9)"
            )
        if snapshot_sent:
            parts.append(
                f"{', '.join(snapshot_sent)} GENERATED on the target but not in Schema "
                "Conversion (this start's initial snapshot sends it, and every snapshot row "
                "would be refused, 428C9)"
            )
        lines.append(f"{table}: " + "; ".join(parts))
    if len(differing) > 5:
        lines.append(f"and {len(differing) - 5} more")
    if not differing:
        return (
            "error",
            "functions",
            "The target's generated columns could not be verified"
            if unreadable
            else "A captured table does not exist on the target"
            if len(missing) == 1
            else "Captured tables do not exist on the target",
            " ".join(unchecked) + " Nothing was changed.",
        )
    # Editing the DDL back to ordinary helps unless the column is generated on a
    # PostgreSQL SOURCE, whose publication never carries it.
    pinned = sorted(
        f"{t}.{c}"
        for t, expected, live in differing
        for c in expected
        if c not in live and is_pg and (t, c) in (source_generated or set())
    )
    # The target computes it, Schema Conversion does not, and it is generated on a
    # PostgreSQL SOURCE: dropping the expression would leave it stale under CDC.
    computed_pinned = sorted(
        f"{t}.{c}"
        for t, expected, live in differing
        for c in live
        if c not in expected and is_pg and (t, c) in (source_generated or set())
    )
    recreate = (
        "re-create the table from that DDL (a Replace in Schema Conversion empties it) and "
        "load its rows again the way they were first loaded"
        if cdc_only
        else "re-create the table from that DDL and reload it with Full Load's per-table "
        "'Drop & reload' (a Replace in Schema Conversion would empty the loaded table)"
    )
    remedy = (
        f" For a column Schema Conversion makes GENERATED: {recreate}, or edit the DDL back "
        "to ordinary if CDC should write it"
        + (
            f" -- not for {_listed(pinned)}, generated on the PostgreSQL source, which "
            "never publishes a generated column: CDC would leave it stale."
            if pinned
            else "."
        )
        + " For a column only the target computes: make the Schema Conversion DDL match it "
        "(GENERATED ALWAYS AS (...) STORED), or make the target column ordinary (ALTER "
        "TABLE ... ALTER COLUMN ... DROP EXPRESSION)"
        + (
            f" -- not for {_listed(computed_pinned)}, generated on the PostgreSQL source: "
            "CDC never streams it, so an ordinary column would be left stale."
            if computed_pinned
            else "."
        )
    )
    return (
        "error",
        "functions",
        "The target's generated columns differ from Schema Conversion",
        "CDC leaves out the columns the target computes, read from the Schema Conversion "
        "DDL, but the target does not match it: " + "; ".join(lines) + "." + remedy
        + "".join(" " + sentence for sentence in unchecked)
        + " Nothing was changed.",
    )


def _probe_cdc_blocking_foreign_keys(migration_state, session, inventory, watermark):
    """Read the target's enforced FKs on the to-be-streamed tables. BLOCKING I/O.

    An enforced foreign key cannot coexist with a live CDC stream: the sink applies
    change records across several tasks with NO parent-before-child ordering, so a child
    row can arrive before its parent, be rejected with SQLSTATE 23503, and be
    dead-lettered PERMANENTLY (the sink treats 23503 as a poison row -- no retry -- while
    offsets advance and the task survives, so the loss is silent). A preceding
    ``Full load only`` run leaves FKs applied AND validated, which is exactly how a
    session that later switches to CDC arrives here.

    Returns a :class:`CdcBlockingForeignKeys`, or ``None`` when there is nothing to check
    (no tables resolved yet -- the caller's own empty-selection guard covers that).

    Opens a DSQL connection, so callers MUST run it off the NiceGUI event loop
    (``run.io_bound`` in the dialog, or the job's worker thread).
    """
    from dsql_migrator.core.cdc import cdc_blocking_foreign_keys
    from dsql_migrator.core.target_introspector import target_foreign_keys

    preserved = migration_state.cdc_preserved_foreign_keys()
    tables = _cdc_tables_for_config(migration_state, inventory, watermark)
    streamed = [t.name for t in tables if getattr(t, "name", None)]
    if not streamed:
        return None
    preserved = cdc_foreign_keys_in_stream_scope(preserved, streamed, inventory)
    # Check every table this migration gives foreign keys to, not just the streamed set.
    # An FK on a NON-streamed child still blocks: a streamed DELETE on the parent is
    # rejected (23503) because the static child rows still reference it, and that DELETE
    # is then dead-lettered permanently. The extra tables cost one more catalog query each
    # on the SAME connection, and keying by child table keeps ownership matching valid.
    table_names = sorted(set(streamed) | set(preserved))
    connect = _cdc_fk_connection_factory(session)
    if connect is None:
        # No target configured: report every table UNKNOWN rather than "clean", so the
        # gate fails closed (the connection blocker will also fire in the dialog).
        return cdc_blocking_foreign_keys({name: None for name in table_names}, preserved)
    return cdc_blocking_foreign_keys(
        target_foreign_keys(table_names, connection_factory=connect), preserved
    )


def _drop_cdc_blocking_foreign_keys(pairs, session) -> tuple[int, int]:
    """Drop the given ``(table, constraint)`` FKs so CDC can stream. BLOCKING I/O.

    Only ever called with the ``ours`` half of :func:`cdc_blocking_foreign_keys` -- the
    constraints THIS migration's conversion renders, which cut over re-creates with the
    shared idempotent apply pass. Each drop is its own single autocommit DDL with OCC
    retry (DSQL allows one DDL per transaction) and is logged individually under the CDC
    category, so the audit trail names every constraint removed.

    Returns ``(dropped, failed)``.
    """
    from dsql_migrator.core.schema_applier import drop_foreign_key

    connect = _cdc_fk_connection_factory(session)
    if connect is None:
        return (0, len(list(pairs)))
    dropped = failed = 0
    for table_name, constraint_name in pairs:
        target = f"{table_name}.{constraint_name}"
        try:
            drop_foreign_key(table_name, constraint_name, connection_factory=connect)
            dropped += 1
            _log_cdc_event(
                "foreign key removed for CDC",
                status=ActivityStatus.SUCCESS,
                detail=(
                    f"{target} dropped so the stream cannot dead-letter out-of-order "
                    "child rows (23503); re-created by the cut-over 'Apply foreign "
                    "keys' step"
                ),
            )
        except Exception:  # noqa: BLE001 - report per FK; never abort the whole pass
            failed += 1
            _log_cdc_event(
                "foreign key not removed",
                status=ActivityStatus.FAILURE,
                detail=(
                    f"{target} could not be dropped; remove it manually before "
                    "starting CDC or the stream will dead-letter child rows (23503)"
                ),
            )
    return (dropped, failed)


def _cdc_fk_block_reason(blocking) -> tuple[str, str]:
    """Return the (clause, header) naming what ACTUALLY blocked. Pure.

    ``blocking`` is true for an unreadable catalog too (fail closed), and that is the
    likeliest case in practice -- a transient DSQL connect blip. Claiming "the target
    still has enforced foreign keys" then sends the operator to ``pg_constraint`` to find
    nothing, instead of to the connection test, which is the real remedy.
    """
    if blocking.ours or blocking.foreign:
        return (
            "the target still has enforced foreign keys",
            "Remove the target's foreign keys before streaming",
        )
    return (
        "the target's foreign keys could not be verified",
        "Could not verify the target's foreign keys",
    )


def _cdc_fk_deploy_body(blocking) -> str:
    """The Deploy CDC infrastructure dialog's foreign-key to-do. Pure.

    The dialog comes BEFORE the Start CDC dialog, where the only Remove control lives, and
    the Start CDC button exists only once the infrastructure is deployed. So for this
    migration's own keys it names that control as the next step. It used to say "Reopen
    Start CDC ... so clear them before you deploy", which the tool cannot do: there is no
    Start CDC dialog before the deployment, and the one-click Remove right after it leaves
    MSK idle for no meaningful time. Keys the tool did not create are the operator's to drop,
    and that can be done before the deployment, so only they keep the billing advice.
    """
    body = _cdc_fk_block_body(blocking, can_remove_here=False, why=False, before_start=True)
    if blocking.foreign:
        body += (
            " Start CDC will not stream while they exist, and MSK Serverless bills from "
            "creation, so drop them before you deploy."
        )
    return body


def _cdc_fk_block_body(
    blocking,
    *,
    can_remove_here: bool = True,
    why: bool = True,
    before_start: bool = False,
    bounded: bool = False,
) -> str:
    """Compose the CDC-start FK precondition message. Pure.

    ``can_remove_here`` is False for the consumers that render no Remove control (the job
    error and the activity log), so the copy does not point at a button that is not there.
    ``before_start`` (with ``can_remove_here=False``) is the Deploy dialog, which comes
    BEFORE the Start CDC dialog that holds the control: it names that control as the next
    step instead of "Reopen Start CDC" (see :func:`_cdc_fk_deploy_body`).

    ``why=False`` drops the closing mechanism paragraph (out-of-order child rows,
    SQLSTATE 23503, silent dead-lettering). It is the right explanation at the point CDC is
    REFUSED, but in the Deploy dialog it is 40 words of rationale competing with three other
    boxes for an operator who has not been refused anything yet -- there the instruction and
    its cost are the whole message.

    ``bounded`` names at most six keys / tables per list, then ``(+N more)``: the worker's
    refusal, whose notice is also the durable activity record -- an unbounded list pushed
    each fix past the log's line cap (G25). The Start CDC dialog keeps every name.
    """
    from dsql_migrator.ui.cdc_dlq_gate import bounded_table_names

    def _names(items) -> str:
        return bounded_table_names(items) if bounded else ", ".join(items)

    parts: list[str] = []
    if blocking.ours:
        if can_remove_here:
            removal = "Remove them below"
        elif before_start:
            removal = (
                "Start CDC will not stream until they are gone: after this deployment, use "
                "'Remove foreign keys' in the Start CDC dialog (it drops exactly these)"
            )
        else:
            removal = "Reopen Start CDC and use 'Remove foreign keys'"
        parts.append(
            "These foreign keys are enforced on the target and were created by this "
            "migration: "
            + _names([f"{t}.{c}" for t, c in blocking.ours])
            + f". {removal} — cut over re-creates them once the stream has drained."
        )
    if blocking.foreign:
        parts.append(
            "These enforced foreign keys are NOT accounted for by this migration, so "
            "the tool will not remove them (it could not put them back): "
            + _names([f"{t}.{c}" for t, c in blocking.foreign])
            + ". Drop them yourself before starting CDC."
        )
    if blocking.unknown_tables:
        parts.append(
            "The target's constraints could not be read for: "
            + _names(list(blocking.unknown_tables))
            + ". Re-test the target connection and retry — CDC is blocked while this "
            "is unknown, because an undetected foreign key silently discards rows."
        )
    if why:
        parts.append(
            "Why: the sink applies change records across several tasks with no "
            "parent-before-child ordering, so a child row can arrive first, be rejected "
            "with SQLSTATE 23503, and be dead-lettered permanently — the task keeps "
            "running and offsets advance, so the loss is silent."
        )
    return " ".join(parts)


def _cdc_target_region(ui, session):
    """Return (target_config, region) or notify + return (None, None) if missing."""
    target = getattr(session, "target_config", None)
    region = getattr(target, "region", None) if target else None
    if not region:
        ui.notify(  # type: ignore[attr-defined]
            "Configure the target connection first.", type="warning", position="top"
        )
        return None, None
    return target, region

def _start_cdc_deploy(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Build connector params and submit the two-pass Start CDC as a background job."""
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_start

    # A running cut-over DEFAULT restore: Start CDC would drop those DEFAULTs again.
    _restore_block = cutover_restore_block_reason(migration_state)
    if _restore_block:
        render_notice(
            ui, tone="error", header="Start CDC is waiting for cut over", body=_restore_block
        )
        return
    # Settle a restored pre-overlay session's exclusions before any set is read.
    classify_restored_exclusions(migration_state, job_manager)
    job = _current_job(job_manager, migration_state.job_id)
    # G129 backstop (the Start button says it first; Retry CDC comes here directly): a Full
    # Load still copying is refused before any parameter is built or job submitted.
    if full_load_running(job):
        render_notice(
            ui, tone="error", header=START_LOAD_RUNNING_HEADER, body=START_LOAD_RUNNING_BODY
        )
        return
    # G17 backstop (the Start button and its dialog say it first; Retry CDC comes here
    # directly): a selected table not loaded under the Full Load's start point yet. Refused
    # before any parameter is built or job submitted -- CDC would not capture it, and once
    # CDC starts the set is fixed.
    _uncovered = cdc_start_uncovered(migration_state, job_manager, job)
    if _uncovered:
        render_notice(
            ui,
            tone="error",
            header=START_UNCOVERED_HEADER,
            body=start_uncovered_body(
                _uncovered, getattr(migration_state, "migration_type", None)
            ),
        )
        return
    # The JOB watermark: it decides the table set, the FK probe, the PostgreSQL gates and
    # the ops window. What the seeder is OFFERED is the seed below (MySQL: a Manual binlog
    # point beats it -- the card and the preview already show that point as the start).
    watermark = _cdc_watermark(job, migration_state)
    seed, seed_origin = _cdc_start_seed(migration_state, watermark, _cdc_source_type(session))
    _seed_refusal = seed_refused_by_an_earlier_offer(
        migration_state, seed, _cdc_source_type(session)
    )
    if _seed_refusal:
        render_notice(
            ui, tone="error", header="This start point cannot be applied", body=_seed_refusal
        )
        return
    # G25 (A2): a stack that has streamed never moves forward to a newer start point (Retry
    # CDC comes here directly; the worker judges it again against a fresh describe).
    _newer = newer_start_point_refusal(
        migration_state, seed, _cdc_source_type(session), origin=seed_origin,
        job_watermark=watermark,
    )
    if _newer is not None:
        render_notice(
            ui, tone="error", icon=_newer[0][1], header=_newer[0][2], body=_newer[0][3]
        )
        return
    # G137 backstop (the card and the Start button say it first; Retry CDC comes here
    # directly): a Manual point later than the Full Load's while a table read in parts is
    # captured.
    _split_refusal = split_start_point_refusal(
        migration_state, job, seed, _cdc_source_type(session), origin=seed_origin,
        resumes=cdc_start_resumes(migration_state, seed, _cdc_source_type(session)),
        job_manager=job_manager,
    )
    if _split_refusal:
        render_notice(ui, tone="error", header=SPLIT_READ_HEADER, body=_split_refusal)
        return
    # ...and what the worker's GTID / start-point refusals add after a Fetch current position
    # remedy while such a table is captured.
    _split_qualifier = _split_later_qualifier(migration_state, job_manager, job)
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    # ONE computation for the preview, Start and Deploy (cdc_capture_exclusions): the
    # EFFECTIVE set (key columns dropped) plus the STORED generated columns of the captured
    # tables -- DSQL rejects a write to one (428C9) and the sink dead-letters it.
    _capture = cdc_capture_exclusions(migration_state, inventory, tables_for_config)
    # The exact-match form; Start CDC re-chooses against the LIVE stack's value
    # (core.cdc.apply_column_exclude_chooser), Deploy has no stack to keep.
    exclude_list = split_column_exclude_value(_capture.value) or None
    # Retry CDC skips the dialog, so the Start section's refusal is repeated here: a set
    # that differs from what a streamed stack excludes would change capture mid-stream.
    _exclusion_mismatch = capture_exclusion_mismatch(
        migration_state, inventory, _capture, tables_for_config
    )
    if _exclusion_mismatch:
        render_notice(
            ui,
            tone="error",
            header="The excluded columns cannot be sent to CDC",
            body=_exclusion_mismatch,
        )
        return
    _resume_override, _force_initial_snapshot = _cdc_resume_signal(migration_state, session)
    mode = migration_state.cdc_start_mode()
    # Guard: a CDC sink requires at least one table (empty -> SinkTopics="" ->
    # connector create fails later with an opaque HTTP 400, ~minutes into a
    # billable deploy). Stop here with an actionable message before submitting.
    if not tables_for_config:
        render_notice(
            ui,
            tone="warning",
            header="Select at least one table before starting CDC",
            body=(
                "No tables are selected for replication, so the CDC sink would have "
                "no topics to write. Choose the tables to migrate, then start CDC."
            ),
        )
        return
    # Composite-PK re-key: scope the stored key map to the tables actually being
    # replicated, then gate the ONE precondition -- a composite key column must not
    # be dropped at capture (column.exclude.list), or CDC can't build the composite key
    # (Debezium's record key, or the sink's from the row image on a v51+ stack).
    selected_names = {t.name for t in tables_for_config}
    message_key_columns = {
        table: cols
        for table, cols in migration_state.cdc_message_key_columns().items()
        if table in selected_names
    }
    if _capture.error:
        render_notice(
            ui,
            tone="error",
            header="The excluded columns cannot be sent to CDC",
            body=capture_error_with_remedy(
                migration_state, job_manager, _capture,
                table_lock=_table_lock(migration_state, job_manager, session),
            ),
        )
        return
    # Retry CDC skips the dialog and the Start button: the table lists this Start sends
    # must each fit one stack parameter, refused before any parameter is built.
    _tables_refusal = cdc_table_list_refusal(
        [t.name for t in tables_for_config], deploy=False
    )
    if _tables_refusal:
        render_notice(
            ui,
            tone="error",
            header=CDC_TABLE_LIST_HEADER,
            body=_table_list_hold_remedy(
                _tables_refusal, migration_state, job_manager,
                _table_lock(migration_state, job_manager, session),
            ),
        )
        return
    _missing = missing_column_block_reason(
        migration_state, inventory, job_manager=job_manager
    )
    if _missing:
        render_notice(
            ui,
            tone="error",
            header="A source column is missing from the Aurora DSQL table",
            body=_missing,
        )
        return
    # Judged on the STRUCTURED set: the formatted entries are escaped / case-flagged /
    # grouped, so comparing strings against them would miss an excluded key column.
    bad_key_cols = composite_cdc_excluded_key_columns(
        message_key_columns,
        [f"{t}.{c}" for t, cols in _capture.structured.items() for c in cols],
    )
    # A KEPT 0.1.561 value (the stack was deployed with it) can still exclude a key column
    # the exact form drops -- 0.1.561 refused to Start with it, and so does this. Its
    # remedy is not "remove it from the selection" (the card is locked while the stack
    # exists): a new stack gets a capture list without key columns.
    if not bad_key_cols and _capture.sent == _capture.legacy != _capture.value:
        _legacy_bad = composite_cdc_excluded_key_columns(
            message_key_columns, _capture.legacy.split(",")
        )
        if _legacy_bad:
            render_notice(
                ui,
                tone="error",
                header="The deployed capture list excludes a composite key column",
                body=(
                    "The deployed CDC stack keeps the capture list it was deployed with, "
                    "which excludes these composite primary-key columns, so Debezium "
                    "cannot build the record key: " + ", ".join(_legacy_bad) + ". Delete "
                    "the CDC infrastructure on the CDC step, then deploy it again and "
                    "Start CDC: a new stack gets a capture list without key columns."
                ),
            )
            return
    if bad_key_cols:
        render_notice(
            ui,
            tone="error",
            header="Composite key column is excluded from capture",
            body=(
                "These composite primary-key columns are in the column exclude list, "
                "so CDC cannot build the composite primary key of their changes: "
                + ", ".join(bad_key_cols)
                + ". Remove them from the LOB-exclusion selection before starting CDC "
                "(a key column is small and safe to capture)."
            ),
        )
        return
    # FIX 1 (PostgreSQL): a COMPOSITE_KEY re-key prepends a non-PK "leading" column to the
    # target primary key. Under REPLICA IDENTITY DEFAULT the UPDATE/DELETE before-image
    # carries only the source PK, so the DELETE does not carry the leading column: Debezium
    # fills it with a placeholder (live-confirmed on PostgreSQL 17: the column DEFAULT), so
    # the sink's NULL-key guard cannot fire and a delete keyed on the composite key misses
    # its row silently. Since G15 a v51+ sink deletes by the source key once the unique index
    # on it is valid, but not before, not on older infrastructure, and not for an UPDATE of
    # the leading column -- so FULL stays required. The
    # tool sets REPLICA IDENTITY FULL on exactly those tables during Full Load provisioning
    # (cdc_pg_slot); surface the requirement here so a CDC-only start (no Full Load in this
    # session) knows it must be set on the source first, and that it must STAY FULL: while
    # CDC runs the monitor re-checks it (ui/cdc_replica_identity_gate.py). MySQL is immune
    # (its binlog before-image is the full row), so this is PostgreSQL-only.
    if _cdc_source_type(session) is SourceType.POSTGRES:
        from dsql_migrator.core import cdc_pg_slot as _pg_slot

        # The publication/slot precondition used to be WARNED about here and decided from
        # the watermark. Both halves were wrong. The watermark is not the record of
        # provisioning it claimed to be -- it dies with an app restart and SURVIVES a
        # Delete-infra that DROPS both objects -- and the advice "or create them on the
        # source with the same names" is, at CDC-start time, the defect itself: a slot
        # created now starts at the CURRENT WAL, so resuming from it silently skips every
        # change since the load. The check is now a source PROBE that BLOCKS, in the Start
        # dialog (with a one-click re-snapshot remedy) and again on the worker thread just
        # before anything is created -- see _probe_pg_replication_objects and work() below.
        _rekeyed_full = _pg_slot.rekeyed_tables_needing_full_identity(
            message_key_columns,
            {t.name: list(t.primary_key) for t in tables_for_config},
        )
        if _rekeyed_full:
            # WARNING, not info, and it no longer says the tool has handled it. The old copy
            # read "The tool sets REPLICA IDENTITY FULL on them during Full Load" -- true
            # ONLY when the Full Load provisions the slot, which a Full-load-ONLY run does
            # not do. An operator who had in fact run Full Load therefore concluded it was
            # handled, started CDC, and lost every delete on the re-keyed table silently
            # (live-observed on Aurora PostgreSQL 17.7).
            #
            # The authoritative gate is the REPLICA_IDENTITY_COVERS_KEY prerequisite, which
            # reads the source's actual relreplident and BLOCKS; this notice only points at
            # it. That claim was FALSE when first written (v0.1.516): the check existed and
            # graded correctly, but cdc_prerequisite_block_reason gated on an ALLOWLIST of
            # three check ids that did not include it, so Deploy and Start stayed enabled
            # beside a red FAIL. The gate is a denylist as of v0.1.517, which is what makes
            # this comment true -- do not re-assert it from a check's existence alone.
            render_notice(
                ui,
                tone="warning",
                header="Re-keyed tables need REPLICA IDENTITY FULL on the source",
                body=(
                    "These tables have a composite (re-keyed) primary key: "
                    + ", ".join(_rekeyed_full) + ". A DELETE, or an UPDATE that changes the "
                    "source primary key, must carry the added leading key column, or it can "
                    "miss its row on Aurora DSQL with no error; an UPDATE of the leading "
                    "column alone can be dead-lettered and leave the old row. The tool sets "
                    "REPLICA IDENTITY FULL during Full Load ONLY when that run also "
                    "provisions the replication slot — a Full-load-only run does not, and "
                    "neither does a CDC-only start. Run the CDC prerequisite checks: they "
                    "read the source's actual REPLICA IDENTITY and block the start with the "
                    "exact ALTER TABLE to run. Keep it FULL for the whole run: while CDC "
                    "runs the tool re-checks it and holds Cut over if it drops."
                ),
            )
    # FIX 4: SeedMode is engine-aware -- the SINGLE source of truth shared with the infra
    # build. A PostgreSQL stack is always created SeedMode=External, so Start must run the
    # in-process External Kafka prep regardless of the host's cdc_seed_mode; MySQL keeps
    # deriving from the host config (unchanged).
    from dsql_migrator.config import load_config as _load_config

    _host_cfg = _load_config()
    seed_mode = _cdc_start_seed_mode(_cdc_source_type(session), _host_cfg.cdc_seed_mode)
    # The External prep runs IN-PROCESS over the MSK IAM bootstrap, so a deployment
    # without 9098 egress + data-plane kafka-cluster IAM cannot do it at all. This used
    # to WARN ("this host does not appear to be in-VPC") and proceed, which was both
    # wrong on Fargate -- the task IS in the cdc-stack VPC; it is simply not admitted
    # and holds no data-plane IAM -- and useless, because the seed then died a minute
    # later with a raw KafkaTimeoutError. Block instead: nothing has been submitted yet.
    # Keyed on the DEPLOYMENT's declared capability, NOT on cdc_seed_mode: Fargate must
    # keep cdc_seed_mode="lambda" so MySQL keeps using the cdc-stack's in-VPC seeder
    # Lambda, so the old condition fired even on a correctly equipped app stack. Whether
    # the DEPLOYED cdc-stack admits this host is a separate, fact-based check in
    # cdc_deployer._run_external_seed.
    if seed_mode == "external":
        from dsql_migrator.core.cdc import msk_seed_capability_blocker

        _seed_blocker = msk_seed_capability_blocker(
            cdc_seed_mode=_host_cfg.cdc_seed_mode,
            cdc_msk_access=getattr(_host_cfg, "cdc_msk_access", False),
        )
        if _seed_blocker:
            render_notice(
                ui,
                tone="error",
                icon="vpn_lock",
                header="This deployment cannot start PostgreSQL CDC",
                body=(
                    _seed_blocker
                    + " No connectors were created and the cdc-stack is unchanged."
                ),
            )
            return
    # MySQL: the connector config is built from the SEED and the cached stack facts, so
    # snapshot.mode (mysql_snapshot_mode: recovery for a NEW start offset, schema_only for
    # a resume) and the start coordinate agree with what is seeded by construction -- and
    # with the preview. PostgreSQL: the job watermark exactly as before (no facts).
    _config_wm = watermark if _cdc_source_type(session) is SourceType.POSTGRES else seed
    _start_facts = mysql_start_facts(migration_state, seed, _cdc_source_type(session))
    source_config = dispatch_source_config(
        _cdc_source_type(session),
        tables_for_config,
        _config_wm if _config_wm is not None else _sentinel_watermark(),
        database=_cdc_source_database(session),
        stack_name=migration_state.cdc_stack_name,
        column_exclude_list=exclude_list,
        resume_override=_resume_override,
        force_initial_snapshot=_force_initial_snapshot,
        message_key_columns=message_key_columns,
        # publication.autocreate.mode is NOT passed: it is derived from the snapshot mode
        # (pg_publication_autocreate_mode). It used to come from a separate piece of UI
        # state that the start-position radio and session restore never set, which is how
        # snapshot.mode=initial + autocreate=disabled -- guaranteed connector death, after
        # both connectors are billed -- became reachable.
        start_facts=_start_facts,
    )
    _sent_snapshot_mode = getattr(source_config, "snapshot_mode", None)
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC
    )
    target, region = _cdc_target_region(ui, session)
    if region is None:
        return
    params = dispatch_cdc_stack_params(
        source_config, sink_config,
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=migration_state.cdc_stack_name,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        sink_mcu_count=_sink_mcu_count(),
    )
    params = params.model_copy(update={"column_exclude_legacy": _capture.legacy})
    # The stack value the exclusion-mismatch refusal judged: the job refuses if the LIVE
    # stack carries another one by the time it runs (another session started it).
    params = params.model_copy(
        update={
            "column_exclude_seen": migration_state.cdc_deployed_column_exclude_list,
            "column_exclude_judged": True,
        }
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name
    aws_profile = getattr(session, "aws_profile", None)
    assume_role_arn = getattr(migration_state, "cdc_deploy_role_arn", None)
    # The column-exclusion overlay, prepared on the loop (no I/O): the captured tables'
    # overlay columns (legacy entries are omit-only), the ledger handle and its key, the
    # effective target DDL provider (called on the job thread only), and a connection
    # factory for the target.
    _overlay_plan = _start_cdc_overlay_plan(
        migration_state, inventory, tables_for_config, session, target
    )
    _is_pg_start = _cdc_source_type(session) is SourceType.POSTGRES

    def _check_mysql_start_on_worker(pre, pre_params, job_id=None, deployer=None) -> Optional[str]:
        """MySQL, on the worker thread before anything changes: the snapshot mode decided
        on the loop must still be right for the stack as it is NOW, the start point must
        be one a gtid_mode=ON source can start from (the GTID start check), and a
        recovery start must pass the start-point check (Retry CDC skips the dialog; a
        dialog verdict can be stale). Raises RuntimeError to refuse; nothing has been
        changed yet. A check's refusal raises a short job error and records its full
        notice for job ``job_id``'s CDC card (:func:`_start_refusal`). Returns the
        gtid_mode this Start records on the stack (G20:
        :func:`~dsql_migrator.ui.cdc_gtid_gate.gtid_mode_to_record`), or None when the
        check did not run or could not read the mode. ``deployer``: reads a rolled-back
        Start's offer from the stack's events when THIS describe shows the rollback and this
        session holds no offer (G26 repair)."""
        from dsql_migrator.ui.cdc_gtid_gate import (
            gtid_mode_to_record,
            gtid_notice_point,
            gtid_start_activity,
            gtid_start_notice,
        )
        from dsql_migrator.ui.cdc_start_point_gate import (
            point_coord,
            point_label,
            start_point_activity,
            start_point_notice,
        )
        from dsql_migrator.ui.data_migration._cdc_status import (
            _rolled_back_offer,
            cdc_deployed_gtid_mode,
            cdc_deployed_watermark,
            cdc_deployed_watermark_gtids,
            cdc_has_committed_offset,
            cdc_live_snapshot_mode,
            mysql_start_facts_from_params,
        )

        facts = _start_facts
        if pre is not None:
            fresh = mysql_start_facts_from_params(
                seed, pre_params,
                history_missing=bool(getattr(migration_state, "cdc_history_missing", False)),
            )
            if mysql_snapshot_mode(_seed_can_seed(seed), fresh) != _sent_snapshot_mode:
                # The stack changed since this page read it: a Start that just failed and
                # left its connectors, or another session or the console starting,
                # stopping or redeploying it. Refresh the cached facts from THIS describe
                # so the next click decides from the stack as it is, then refuse.
                migration_state.set_cdc_has_committed_offset(cdc_has_committed_offset(pre_params))
                migration_state.set_cdc_deployed_watermark(
                    cdc_deployed_watermark(pre_params),
                    gtids=cdc_deployed_watermark_gtids(pre_params),
                )
                _set_gtid_mode = getattr(migration_state, "set_cdc_deployed_gtid_mode", None)
                if callable(_set_gtid_mode):
                    _set_gtid_mode(cdc_deployed_gtid_mode(pre_params))
                _set_mode = getattr(migration_state, "set_cdc_deployed_snapshot_mode", None)
                if callable(_set_mode):
                    _set_mode(cdc_live_snapshot_mode(pre_params))
                _r1 = (
                    "Start CDC blocked: the CDC stack changed after this page last read it "
                    "(a Start that just failed, another session or the console started, "
                    "stopped or redeployed it), so this start would run Debezium in the "
                    "wrong snapshot mode. Nothing was changed; Start CDC again."
                )
                _log_cdc_event(
                    "start blocked: CDC stack changed", status=ActivityStatus.FAILURE,
                    detail=_r1,
                )
                raise RuntimeError(_r1)
            facts = fresh
        # G25 (A2): a stack that has streamed never moves forward to a newer start point --
        # judged on THIS describe when there is one (another session or the console may have
        # started or stopped the stack since the dialog read it), before anything reads.
        _pre_status = getattr(pre, "stack_status", None) if pre is not None else None
        # G26 repair: THIS describe can show a rollback the page's cache never saw (another
        # session's first Start rolled back after this page's last probe), so this session
        # holds no offer for it. Read that Start's offer from the stack's events here, as the
        # probe does (read-only, only for a rollback status; unknown -> nothing recorded,
        # fail closed), so the checks below judge the retry instead of refusing it unread.
        if getattr(migration_state, "cdc_offered_seed", None) is None:
            _offer = _rolled_back_offer(deployer, stack_name, _pre_status)
            if _offer:
                from types import SimpleNamespace

                migration_state.note_cdc_offered_seed(
                    SimpleNamespace(binlog_file=_offer[0], binlog_position=_offer[1])
                )
        _newer = newer_start_point_refusal(
            migration_state, seed, SourceType.MYSQL, origin=seed_origin,
            job_watermark=watermark,
            stack_params=pre_params if pre is not None else None,
            stack_status=_pre_status,
        )
        if _newer is not None:
            raise _start_refusal(migration_state, _newer[0], job_id, first_line=_newer[1])
        # A stack whose last Start CloudFormation rolled back (G25): the checks' remedies that
        # take a new start point run on a new stack.
        _rolled_back = (
            str(_pre_status).strip().upper().startswith("UPDATE_ROLLBACK")
            if _pre_status is not None
            else _pg_start_rolled_back(migration_state)
        )
        # ...and the click's earlier-offer check, again on the offer as known NOW (just read
        # above, or recorded since the click): A2 does not refuse an earlier seed, and the
        # seeder never rewinds, so it would silently not apply (G26 repair). Its way on is
        # judged on THIS describe too: after a rolled-back Start a later point is refused.
        _earlier = seed_refused_by_an_earlier_offer(
            migration_state, seed, SourceType.MYSQL,
            streamed=_rolled_back or cdc_stack_has_streamed(migration_state),
        )
        if _earlier:
            raise _start_refusal(
                migration_state,
                ("error", "", "This start point cannot be applied", _earlier),
                job_id,
            )
        # G19: on a gtid_mode=ON source Debezium starts after the GTID set seeded with the
        # start point (not at its file:position), so refuse a start point it cannot start
        # exactly at -- before the start-point check (which would otherwise also read). A
        # resume is judged by the stack's own record of its last start offset, from THIS
        # describe when there is one.
        _gtid_verdict = _probe_cdc_gtid_start(
            migration_state, session, seed=seed, facts=facts, origin=seed_origin,
            stack_params=pre_params if pre is not None else None,
        )
        # G20: the mode a passing check read is what this Start records on the stack.
        _gtid_record = gtid_mode_to_record(_gtid_verdict)
        if _gtid_verdict is not None:
            _gtid_label, _coord = gtid_notice_point(
                _gtid_verdict, seed=seed, origin=seed_origin
            )
            _gtid_seed_set = normalize_gtid_set(getattr(seed, "gtid_executed", None))
            _gtid_activity = gtid_start_activity(
                _gtid_verdict, coord=_coord, seed_gtids=_gtid_seed_set
            )
            if _gtid_verdict.blocks:
                _gtid_notice = gtid_start_notice(
                    _gtid_verdict, point_label=_gtid_label, coord=_coord,
                    origin=seed_origin,
                    migration_type=getattr(migration_state, "migration_type", None),
                    rolled_back=_rolled_back,
                    split_qualifier=_split_qualifier,
                )
                if _gtid_activity is not None:
                    _log_cdc_event(
                        _gtid_activity[0], status=_gtid_activity[1], detail=_gtid_activity[2]
                    )
                raise _start_refusal(migration_state, _gtid_notice, job_id)
            if _gtid_activity is not None:
                # The durable record of how CDC positioned (the deploy log is per-action).
                migration_state.append_cdc_deploy_log(
                    datetime.now(timezone.utc), _gtid_activity[2]
                )
                _log_cdc_event(
                    _gtid_activity[0], status=_gtid_activity[1], detail=_gtid_activity[2]
                )
        verdict = _probe_cdc_start_point(
            migration_state, session, seed=seed, mode=_sent_snapshot_mode, facts=facts,
            tables=tables_for_config,
        )
        if verdict is None:
            return _gtid_record
        _label = point_label(seed, seed_origin)
        _activity = start_point_activity(verdict, point_label=_label)
        if verdict.blocks:
            _notice = start_point_notice(
                verdict, point_label=_label, coord=point_coord(seed),
                migration_type=getattr(migration_state, "migration_type", None),
                resumes=bool(getattr(facts, "resumes", False)),
                # G25: a start point after the change needs new CDC topics on a stack whose
                # connectors were started (a resume, or a retry after a rolled-back Start).
                streamed=_rolled_back or cdc_stack_has_streamed(migration_state),
                split_qualifier=_split_qualifier,
            )
            if _activity is not None:
                _log_cdc_event(_activity[0], status=_activity[1], detail=_activity[2])
            raise _start_refusal(migration_state, _notice, job_id)
        if _activity is not None:
            _log_cdc_event(_activity[0], status=_activity[1], detail=_activity[2])
        return _gtid_record

    def work(handle) -> None:
        try:
            _work(handle)
        finally:
            # The cut-over restore attestation is stale once CDC starts (or tries to).
            _note_cdc_start(migration_state)

    def _work(handle) -> None:
        # Everything blocking (the deploy-role AssumeRole in the deployer build, the STS
        # account lookup, the ~50 KiB template read, the config load) runs HERE on the
        # job's worker thread -- NEVER on the NiceGUI event loop. On Fargate one asyncio
        # loop serves every browser session, so a blocking call on it freezes them all;
        # the sibling _start_cdc_infra_deploy offloads for exactly this reason.
        # The target this Start was prepared for (ledger key, sink parameters, overlay)
        # must still be the session's: a target changed on Connect after the click would
        # otherwise be checked and ALTERed while the connectors stream to the old one.
        if not _same_target(getattr(session, "target_config", None), target):
            raise RuntimeError(
                "Start CDC blocked: the Aurora DSQL target changed after Start CDC was "
                "clicked. Nothing was changed; start CDC again."
            )
        # BACKSTOP for the foreign-key precondition, re-checked on the worker thread
        # right before anything is created. The Start dialog already blocks on this, but
        # "Retry CDC" (_render_cdc_partial_actions) calls this job with NO dialog, and a
        # cached dialog verdict can be stale. An enforced FK makes the sink dead-letter
        # out-of-order child rows PERMANENTLY and SILENTLY (23503 is not retriable), so
        # refuse rather than stream into data loss. Fails closed: an unreadable catalog
        # counts as blocking.
        fk_state = _probe_cdc_blocking_foreign_keys(
            migration_state, session, inventory, watermark
        )
        if fk_state is not None and fk_state.blocking:
            # Through _start_refusal like the other worker refusals (G25): the job keeps
            # 300 characters, and the remedy follows the foreign-key list, so three or more
            # keys cut it off. The CDC card shows the full notice; its activity line
            # replaces the one this refusal wrote before. Its lists are bounded, so each
            # fix fits the activity log's line too (the Start CDC dialog lists every key).
            _clause = _cdc_fk_block_reason(fk_state)[0]
            raise _start_refusal(
                migration_state,
                (
                    "error",
                    "link",
                    _clause[:1].upper() + _clause[1:],
                    _cdc_fk_block_body(fk_state, can_remove_here=False, bounded=True),
                ),
                getattr(handle, "job_id", None),
            )
        # The GENERATED columns the capture leaves out (from the applied DDL) must be the
        # ones the live target computes (G46): fails closed on an unreadable table.
        _generated_mismatch = _probe_cdc_generated_mismatch(
            migration_state, session, inventory, watermark
        )
        if _generated_mismatch:
            raise _start_refusal(
                migration_state,
                _cdc_generated_mismatch_notice(
                    _generated_mismatch,
                    is_pg=_is_pg_start,
                    cdc_only=getattr(migration_state, "migration_type", None)
                    is MigrationType.CDC_ONLY,
                    source_generated={
                        (t.name, c.name)
                        for t in (getattr(inventory, "tables", None) or ())
                        for c in t.columns
                        if getattr(c, "generated", False)
                    },
                ),
                getattr(handle, "job_id", None),
            )
        # BACKSTOP for the PostgreSQL publication/slot precondition, on the worker thread
        # right before anything is created. The Start dialog already blocks on this, but
        # "Retry CDC" (_render_cdc_partial_actions) calls this job with NO dialog, and a
        # cached dialog verdict can be stale. It must live HERE and not in
        # _start_cdc_deploy's body: that runs ON the NiceGUI event loop and this probe is
        # blocking source I/O. Fails OPEN on an unreadable source (the probe returns
        # None) -- unlike the FK gate, because a missing publication kills the connector
        # LOUDLY whereas an enforced FK loses rows silently -- but never silently: the
        # activity log records that the checks did not run, and why (G25).
        def _pg_checks_not_run(reason: str) -> None:
            _log_cdc_event(
                PG_CHECKS_NOT_RUN_ACTION,
                status=ActivityStatus.WARNING,
                detail=(
                    f"could not read the PostgreSQL source ({reason}), so the publication, "
                    "replication slot and inheritance checks did not run; this Start CDC "
                    "went ahead without them"
                ),
            )

        _pg_block = _pg_objects_gate_block(
            migration_state, session, inventory, watermark, on_unread=_pg_checks_not_run,
            # A submitted Start fixes the table set (G25): no untick to name.
            untick=False,
        )
        if _pg_block is not None:
            _pg_body = _pg_block[1]
            if not _pg_stack_started(migration_state):
                # A stack never started: the route rule's way out, the re-snapshot remedy's
                # standing unknown here -- the Start CDC dialog judges it (G25). A stack started
                # before already has its route from the gate.
                _pg_body = _with_route(_pg_body, _pg_block_route(migration_state, _pg_block[0]))
            # Through _start_refusal like the other worker refusals: the job keeps 300
            # characters, which cut the fix off (G25), so the CDC card shows the notice.
            raise _start_refusal(
                migration_state,
                ("error", "sync_problem", _pg_block[0], _pg_body),
                getattr(handle, "job_id", None),
            )
        deployer = build_cdc_stack_deployer(
            region, aws_profile=aws_profile, assume_role_arn=assume_role_arn
        )
        # The exclusion-set staleness checks BEFORE anything changes (the overlay below
        # ALTERs the target and writes the ledger): judged against a fresh describe here,
        # and again by run_cdc_start against its own.
        from dsql_migrator.core.cdc import apply_column_exclude_chooser, column_exclude_seen_refusal

        _describe = getattr(deployer, "describe_stack_or_none", None)
        _pre = _describe(stack_name) if callable(_describe) else None
        _pre_params = getattr(_pre, "current_parameters", None)
        _seen_refusal = column_exclude_seen_refusal(params, _pre_params)
        if _seen_refusal:
            _log_cdc_event(
                "start blocked: excluded columns changed", status=ActivityStatus.FAILURE,
                detail=_seen_refusal,
            )
            raise RuntimeError(f"Start CDC blocked: {_seen_refusal}")
        # MySQL: the gtid_mode the GTID start check read (G20), recorded on the stack by the
        # pass that creates the connectors; None when the check did not run or could not
        # read it (the stack then keeps its record).
        _start_gtid_mode: Optional[str] = None
        if not _is_pg_start:
            _start_gtid_mode = _check_mysql_start_on_worker(
                _pre, _pre_params, job_id=getattr(handle, "job_id", None), deployer=deployer
            )
        # The column-exclusion overlay: every captured excluded column nullable with no
        # DEFAULT (recorded first) BEFORE any connector is created or changed, so every row
        # CDC writes holds NULL there. Covers Retry CDC and a re-Start after Stop; a no-op
        # after a Full Load that already overlaid. Blocks the Start on any failure.
        _apply_start_cdc_overlay(handle, **_overlay_plan)
        if getattr(handle, "cancelled", False):
            return
        # The cdc-stack template exceeds CFn's 51,200-byte inline limit, so the deployer
        # stages it in S3 via TemplateURL. Derive the bucket name from the deterministic
        # naming convention (same bucket the infra deploy created).
        from dsql_migrator.core.s3_provision import plugin_bucket_name as _pbucket
        try:
            _acct = deployer._client("sts").get_caller_identity()["Account"]  # type: ignore[attr-defined]
            deployer.template_s3_bucket = _pbucket(_acct, region)
        except Exception:  # noqa: BLE001 — best-effort; a later step fails with a clear message
            pass
        template_body = _read_cdc_template_body()
        # SeedMode is derived ENGINE-AWARE above (FIX 4): a PostgreSQL source forces
        # "external" (its stack is always SeedMode=External) so the in-process Kafka prep
        # runs; MySQL keeps the host config ("Host is the mode": the in-VPC EC2 host sets
        # DSQL_MIGRATOR_CDC_SEED_MODE=external, Fargate/local leave it -> "lambda").
        # Recorded BEFORE the Start: if it fails after the seeder wrote this offset, a later
        # Start must not offer an earlier one (seed_refused_by_an_earlier_offer).
        migration_state.note_cdc_offered_seed(seed)
        from dsql_migrator.core.cdc_deployer import (
            CdcDeployError,
            history_missing_after_failure,
        )

        # MySQL: a failed connector create is diagnosed from its worker log, read with the
        # APP's identity (TaskRole / instance role, which may read the connector log
        # group) before the deployer's -- on Fargate and EC2 the deployer acts as
        # CdcDeployRole, which cannot, and the Start then showed only the CloudFormation
        # status. PostgreSQL keeps today's exact call.
        _log_kwargs: dict = {}
        if not _is_pg_start:
            from dsql_migrator.core.msk_connect_controller import (
                build_msk_connect_controller,
            )

            _log_kwargs["log_tail"] = build_msk_connect_controller(
                region, aws_profile=aws_profile
            ).connector_log_tail
            # G20: MySQL only, like log_tail -- a PostgreSQL Start's call is unchanged.
            _log_kwargs["start_gtid_mode"] = _start_gtid_mode
        try:
            run_cdc_start(
                handle,
                stack_name=stack_name,
                params=params,
                deployer=deployer,
                on_log=migration_state.append_cdc_deploy_log,
                # The start offset offered to the seeder (_cdc_start_seed): the Full Load
                # watermark (gapless, Property 11) or the Manual binlog point. With no
                # seedable point there is no seed and the source connector starts from the
                # current binlog (or its committed offset on a restart).
                watermark=seed,
                seed_origin=seed_origin,
                template_body=template_body,
                seed_mode=seed_mode,
                # PostgreSQL only: lets the Start repair a publication DEBEZIUM created,
                # which lands at publish_via_partition_root=false and therefore makes the
                # connector silently DISCARD every change to a partitioned table. Optional
                # by design -- absent credentials (never persisted, Property 7) downgrade to
                # a warning rather than blocking a Start that works for every
                # non-partitioned table.
                pg_source_config=(
                    getattr(session, "source_config", None)
                    if _cdc_source_type(session) is SourceType.POSTGRES
                    else None
                ),
                pg_source_password=getattr(session, "source_password", None),
                **_log_kwargs,
            )
        except CdcDeployError as exc:
            # A schema_only RESUME that found this stack's schema history missing or
            # incomplete would fail the same way on every Start: flag it so the next one
            # rebuilds the history (recovery, after the start-point check). So would a
            # recovery Start whose schema snapshot stopped on a table definition Debezium
            # cannot parse: the offset is seeded, the history is not.
            if not _is_pg_start and history_missing_after_failure(
                _sent_snapshot_mode, str(exc)
            ):
                _note = getattr(migration_state, "note_cdc_history_missing", None)
                if callable(_note):
                    _note(True)
            raise
        # This Start just sent this value: it IS the stack's value now, so a Retry before
        # the next probe is judged against it (not the pre-Start one it replaced).
        _sent = dict(apply_column_exclude_chooser(params, _pre_params).filled).get("ColumnExcludeList")
        if _sent is not None:
            migration_state.set_cdc_deployed_column_exclude_list(_sent)
        if not _is_pg_start:
            # Likewise the snapshot mode the connectors now run with, and the history is
            # evidently there (or was just rebuilt).
            _set_mode = getattr(migration_state, "set_cdc_deployed_snapshot_mode", None)
            if callable(_set_mode):
                _set_mode(_sent_snapshot_mode)
            # ...and the gtid_mode this Start recorded on the stack (G20).
            if _start_gtid_mode:
                _set_gtid_mode = getattr(migration_state, "set_cdc_deployed_gtid_mode", None)
                if callable(_set_gtid_mode):
                    _set_gtid_mode(_start_gtid_mode)
            _note = getattr(migration_state, "note_cdc_history_missing", None)
            if callable(_note):
                _note(False)

    _action = "start CDC connectors"
    # The RESUME POINT is the fact this line exists for: it is what makes the Full
    # Load -> CDC handoff gapless, and it cannot be reconstructed afterwards (the
    # connector's own offsets are consumed and the deploy log is per-action and
    # ephemeral). The detail was the stack name alone, so a downloaded activity log
    # recorded that CDC was started but not FROM WHERE.
    _detail = _cdc_start_detail(
        stack_name,
        seed,
        mode=mode,
        source_type=_cdc_source_type(session),
        force_snapshot=_force_initial_snapshot,
        resumes=cdc_start_resumes(migration_state, seed, _cdc_source_type(session)),
        # PostgreSQL: a last Start CloudFormation rolled back promises no snapshot (G25).
        rolled_back=_pg_start_rolled_back(migration_state),
        seed_origin=seed_origin,
        # The operator's (effective) exclusions only -- "oversized LOB" in the detail; the
        # generated columns the capture also drops are not the operator's choice.
        exclusions=effective_lob_exclusions(migration_state, inventory),
        snapshot_mode=_sent_snapshot_mode,
        # G27: judged BEFORE this Start moves the ops window to this watermark (below).
        recopied=drop_reload_condition_due(migration_state, job),
    )
    if _overlay_plan["overlay_by_table"]:
        _mark = getattr(migration_state, "mark_overlay_ledger_in_use", None)
        if callable(_mark):
            _mark()
    _note_cdc_start(migration_state)
    # Each Start or Retry is a new failure episode: a cause read for the connectors it
    # replaces must not label the ones it creates (their names repeat).
    _clear_causes = getattr(migration_state, "clear_cdc_connector_diagnoses", None)
    if callable(_clear_causes):
        _clear_causes()
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="start")
    # Count per-table applied-ops (I/U/D) only from THIS migration's Full Load
    # watermark onward, so the monitor shows post-Full-Load CDC events -- not stale
    # ops from prior CDC runs still inside the metric's trailing window. No watermark
    # (CDC-only / manual) -> now (count from this CDC start).
    migration_state.set_cdc_ops_window_start(
        watermark.snapshot_timestamp if watermark is not None else None
    )
    _log_cdc_event(_action, detail=_detail)
    ui.notify("Start CDC submitted — watch the progress below.", type="positive", position="top")  # type: ignore[attr-defined]
    refresh()

@dataclass(frozen=True)
class _CdcSourceSecret:
    """Outcome of resolving the CDC source-credentials secret for a deploy.

    On success ``ok`` is True and ``arn``/``name`` carry the secret coordinates.
    On failure ``ok`` is False and ``error``/``error_type`` carry a credential-free
    notify message (the caller surfaces it). The plaintext password never appears
    on this object or in ``error`` (Property 7).
    """

    ok: bool
    arn: str = ""
    name: str = ""
    error: str = ""
    error_type: str = "warning"

def _resolve_cdc_source_secret(
    session, *, stack_name: str, aws_profile, region, kms_key_id=None
) -> _CdcSourceSecret:
    """Resolve the source-credentials secret the CDC connector reads from.

    - Source connected with Secrets Manager auth -> reuse that secret's ARN/name.
    - Source connected with username/password -> create (or upsert) a tool-managed
      secret from the in-memory credentials so the user never re-enters them. When
      ``kms_key_id`` is set, the created secret is encrypted with that customer-
      managed key instead of the default aws/secretsmanager key.

    Pure of any UI: returns a :class:`_CdcSourceSecret` the caller turns into a
    notify + return on failure.
    """
    source_secret_id = getattr(session, "source_secret_id", None)
    if source_secret_id:
        from dsql_migrator.core.s3_provision import extract_secret_name

        return _CdcSourceSecret(
            ok=True,
            arn=source_secret_id,
            name=extract_secret_name(source_secret_id),
        )

    pw = getattr(session, "source_password", None)
    src_cfg = getattr(session, "source_config", None)
    username = (getattr(src_cfg, "username", None) or "").strip()
    if pw is None or not username:
        return _CdcSourceSecret(
            ok=False,
            error=(
                "Test the source connection first (username/password) so the tool "
                "can create the CDC source secret."
            ),
            error_type="warning",
        )
    try:
        from dsql_migrator.core.secrets import (
            cdc_source_secret_name,
            ensure_source_secret,
        )

        arn = ensure_source_secret(
            stack_name=stack_name,
            username=username,
            password=pw.reveal(),
            aws_profile=aws_profile,
            region=region,
            kms_key_id=kms_key_id,
        )
        return _CdcSourceSecret(
            ok=True, arn=arn, name=cdc_source_secret_name(stack_name)
        )
    except Exception as exc:  # noqa: BLE001
        return _CdcSourceSecret(
            ok=False,
            error=f"Could not create the CDC source secret: {exc}",
            error_type="negative",
        )

async def _start_cdc_infra_deploy(
    ui, migration_state, job_manager, refresh, *, inventory=None, session=None
) -> None:
    """Validate the BYO-VPC inputs and submit create_stack as a background job.

    The pre-submit resolution makes several blocking AWS round-trips (DSQL
    ``GetCluster`` for the cluster ARN, EC2 ``Describe*`` for the network
    diagnosis, and a Secrets Manager read/create for the source credentials).
    On Fargate a single asyncio loop serves every browser session, so these run
    off the loop via ``run.io_bound`` -- otherwise clicking Deploy would freeze
    the UI for all connected users for the duration of the round-trips.
    """
    from nicegui import run
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_infra_deploy

    # G16: every table of the Full Load's snapshot unticked -- an infrastructure sized for
    # no table. Refused before any AWS call (a watermark with counts and nothing captured;
    # the pre-load deploy with no watermark is unchanged). G17: the same when only tables
    # still to load under the Full Load's start point are selected.
    _deploy_job = _current_job(job_manager, getattr(migration_state, "job_id", None))
    _deploy_wm = _cdc_watermark(_deploy_job, migration_state)
    if cdc_capture_names(migration_state, _deploy_wm) == frozenset():
        ui.notify(  # type: ignore[attr-defined]
            DEPLOY_PENDING_EMPTY
            if cdc_deploy_pending(migration_state, job_manager, _deploy_job)
            else DEPLOY_EMPTY_CAPTURE,
            type="warning",
            position="top",
        )
        return
    # A per-table list too long for one stack parameter (the deploy button says so first):
    # refused before any AWS call, so nothing is created or uploaded. The partition plan is
    # sized from the Full Load watermark's row estimates, else from the source's, read HERE
    # (read-only, scan-free) instead of after the AWS calls below, so the plan is judged
    # exactly as it is sent -- the same counts go to the parameter builder.
    _captured = _cdc_tables_for_config(migration_state, inventory, _deploy_wm)
    _captured_names = [t.name for t in _captured]
    _deploy_row_counts = None
    _tables_refusal = cdc_table_list_refusal(_captured_names, deploy=False)
    if not _tables_refusal:
        _deploy_row_counts = _cdc_row_counts_from_watermark(_deploy_wm, _captured) or (
            await run.io_bound(_estimate_cdc_table_rows, session, _captured_names)
        )
        _tables_refusal = cdc_table_list_refusal(
            _captured_names, deploy=True, row_counts_by_table=_deploy_row_counts
        )
    if _tables_refusal:
        ui.notify(  # type: ignore[attr-defined]
            _deploy_table_list_refused(_tables_refusal, migration_state, job_manager, session),
            type="negative",
            position="top",
        )
        return

    target, region = _cdc_target_region(ui, session)
    if region is None:
        return
    aws_profile = getattr(session, "aws_profile", None)

    # Resolve the cluster ARN off the loop (render skipped this GetCluster).
    fields = await run.io_bound(_cdc_infra_prefill, migration_state, session)

    # VpcId is the one always-required field.
    if not (fields.get("vpc_id") or "").strip():
        ui.notify(  # type: ignore[attr-defined]
            "Enter your VPC ID.", type="warning", position="top"
        )
        return

    # DsqlClusterArn is auto-derived (GetCluster) in the prefill; if that lookup
    # failed (e.g. missing dsql:GetCluster permission) we cannot proceed.
    if not (fields.get("dsql_cluster_arn") or "").strip():
        ui.notify(  # type: ignore[attr-defined]
            "Could not resolve the DSQL cluster ARN automatically. Check the "
            "target connection and dsql:GetCluster permission, then retry.",
            type="negative", position="top",
        )
        return

    # HostSubnetCidr: the network the cdc-stack admits on MSK 9098 so a
    # SeedMode=External (PostgreSQL, or the in-VPC EC2 host) Start can seed in-process.
    # Re-resolved here rather than trusting the dialog's verdict -- the dialog may have
    # been open for minutes and its EC2 describe can since have been throttled or
    # denied, and deploying with an empty value would create a cluster nothing admits.
    # Checked FIRST, before the network diagnosis and the Secrets Manager upsert, so a
    # refusal leaves nothing behind. Explicit DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR wins
    # inside msk_seed_admission (so the EC2 host is byte-identical); a Lambda-seeded
    # deploy resolves to "" -> no ingress rule, unchanged.
    _admission = (
        await run.io_bound(
            _msk_seed_admission, migration_state, session, vpc_id=fields["vpc_id"]
        )
        or MskSeedAdmission()
    )
    if _admission.blocker:
        # "negative", matching the dialog's error tone for the same string: this is a
        # refusal, and the deploy does not happen.
        ui.notify(_admission.blocker, type="negative", position="top")  # type: ignore[attr-defined]
        return

    # --- Click-time network resolution (read-only, off the event loop) ----------
    # (a) An explicit subnet override (Advanced) is used as-is. Otherwise diagnose
    #     the VPC: reuse existing NAT-egress subnets (discovered), let the stack
    #     create its own NAT (create), or fail with guidance (blocked).
    connector_subnet_ids = (fields.get("connector_subnet_ids") or "").strip()
    nat_public_subnet_id = ""
    private_subnet_cidr_a = ""
    private_subnet_cidr_b = ""
    private_subnet_az_a = ""
    private_subnet_az_b = ""
    if connector_subnet_ids:
        # User-supplied subnets: verify they have NAT egress. MSK Connect assigns
        # private IPs only, so IGW-only subnets cannot reach Secrets Manager or
        # any HTTPS AWS endpoint, causing a silent 10-minute deploy failure.
        def _verify_manual_subnets():
            from dsql_migrator.core.ec2_metadata import (
                build_ec2_client,
                verify_subnet_egress,
            )
            ec2 = build_ec2_client(aws_profile, region)
            return verify_subnet_egress(ec2, connector_subnet_ids.split(","))

        try:
            egress_ok, egress_reason = await run.io_bound(_verify_manual_subnets)
        except Exception:  # noqa: BLE001 — best-effort; don't block on EC2 errors
            egress_ok = True  # assume OK if we can't verify
            egress_reason = ""
        if not egress_ok:
            ui.notify(  # type: ignore[attr-defined]
                egress_reason, type="negative", position="top",
            )
            return

    if not connector_subnet_ids:
        def _diagnose():
            from dsql_migrator.core.ec2_metadata import (
                build_ec2_client,
                diagnose_cdc_network,
            )

            ec2 = build_ec2_client(aws_profile, region)
            return diagnose_cdc_network(ec2, fields["vpc_id"])

        try:
            diagnosis = await run.io_bound(_diagnose)
        except Exception as exc:  # noqa: BLE001
            ui.notify(  # type: ignore[attr-defined]
                f"Could not inspect the VPC network: {exc}",
                type="negative", position="top",
            )
            return
        if diagnosis.mode == "blocked":
            ui.notify(  # type: ignore[attr-defined]
                diagnosis.reason, type="warning", position="top"
            )
            return
        if diagnosis.mode == "discovered":
            connector_subnet_ids = diagnosis.connector_subnet_ids or ""
            # Double-check: verify the discovered subnets still have NAT egress
            # at this moment (they may have lost it if the owning stack was
            # deleted between diagnosis and now — race condition).
            def _verify_discovered():
                from dsql_migrator.core.ec2_metadata import (
                    build_ec2_client,
                    verify_subnet_egress,
                )
                ec2 = build_ec2_client(aws_profile, region)
                return verify_subnet_egress(ec2, connector_subnet_ids.split(","))

            try:
                disc_ok, disc_reason = await run.io_bound(_verify_discovered)
            except Exception:  # noqa: BLE001
                disc_ok = True
                disc_reason = ""
            if not disc_ok:
                ui.notify(  # type: ignore[attr-defined]
                    disc_reason, type="negative", position="top",
                )
                return
        else:  # "create" — the stack will make its own subnets + NAT
            nat_public_subnet_id = diagnosis.nat_public_subnet_id or ""
            private_subnet_cidr_a = diagnosis.private_subnet_cidrs[0]
            private_subnet_cidr_b = diagnosis.private_subnet_cidrs[1]
            private_subnet_az_a = diagnosis.availability_zones[0]
            private_subnet_az_b = diagnosis.availability_zones[1]

    # (b) source-credentials secret. The CDC connector can only read source
    #     credentials from Secrets Manager (never an in-memory password), so:
    #     - SM auth on Connect -> reuse that secret's ARN/name.
    #     - username/password auth -> create (or upsert) a tool-managed secret
    #       from the in-memory credentials, so the user never re-enters them.
    #     This does a Secrets Manager round-trip, so it runs off the loop too.
    secret = await run.io_bound(
        _resolve_cdc_source_secret,
        session,
        stack_name=migration_state.cdc_stack_name,
        aws_profile=aws_profile,
        region=region,
        kms_key_id=getattr(migration_state, "cdc_secret_kms_key_id", None),
    )
    if not secret.ok:
        ui.notify(  # type: ignore[attr-defined]
            secret.error, type=secret.error_type, position="top"
        )
        return
    source_secret_arn = secret.arn
    source_secret_name = secret.name

    template_body = _read_cdc_template_body()
    if template_body is None:
        ui.notify(  # type: ignore[attr-defined]
            "Could not read the cdc-stack template (deploy/cdc-stack/cdc-stack.yaml).",
            type="negative", position="top",
        )
        return

    job = _current_job(job_manager, migration_state.job_id)
    watermark = _cdc_watermark(job, migration_state)
    override = migration_state.cdc_start_override()
    tables_for_config = _cdc_tables_for_config(migration_state, inventory, watermark)
    # ONE computation for the preview, Start and Deploy (cdc_capture_exclusions): the
    # EFFECTIVE set (key columns dropped) plus the STORED generated columns of the captured
    # tables -- DSQL rejects a write to one (428C9) and the sink dead-letters it.
    _capture = cdc_capture_exclusions(migration_state, inventory, tables_for_config)
    # The exact-match form; Start CDC re-chooses against the LIVE stack's value
    # (core.cdc.apply_column_exclude_chooser), Deploy has no stack to keep.
    exclude_list = split_column_exclude_value(_capture.value) or None
    if _capture.error:
        ui.notify(
            capture_error_with_remedy(
                migration_state, job_manager, _capture,
                table_lock=_table_lock(migration_state, job_manager, session),
            ),
            type="negative",
            position="top",
        )
        return
    _resume_override, _force_initial_snapshot = _cdc_resume_signal(migration_state, session)
    mode = migration_state.cdc_start_mode()
    source_config = dispatch_source_config(
        _cdc_source_type(session),
        tables_for_config,
        watermark if watermark is not None else _sentinel_watermark(),
        database=_cdc_source_database(session),
        stack_name=migration_state.cdc_stack_name,
        column_exclude_list=exclude_list,
        resume_override=_resume_override,
        force_initial_snapshot=_force_initial_snapshot,
    )
    # Infra-only deploy (DeploySink=false, no connector yet): an empty table
    # selection is allowed here -- SinkTopics is populated later at Start CDC.
    sink_config = CdcPipelineOrchestrator().build_sink_config(
        "mysql-sink", tables_for_config, CDC_DEFAULT_DLQ_TOPIC, allow_empty=True
    )
    # Size-proportional topic partitions (skewed-workload fix): weight Kafka
    # partitions toward the largest tables so a hot table is not serialized on a
    # single sink task. Partition counts are fixed at topic-creation (which happens
    # at Start CDC, but the source connector reads these persisted params), so they
    # must be decided here at create. Prefer the Full Load watermark's scan-free
    # estimates; if absent (infra deployed before Full Load), fresh information_schema
    # estimates, read off the loop. Best-effort -> None -> uniform. Both were read at the
    # top of this click, where the table lists they size were judged: the same counts here.
    row_counts_by_table = _deploy_row_counts
    # (c) source-DB security group. Scope the connector's egress-to-source rule to
    #     the source DB's own SG so the stack does NOT fall back to an open
    #     0.0.0.0/0 egress on the source port. Best effort + off the loop: if the
    #     user supplied one it wins; otherwise look it up from RDS (read-only). A
    #     non-RDS host / missing rds:DescribeDBInstances just leaves it empty (the
    #     stack then uses the documented 0.0.0.0/0 fallback, as before).
    source_db_security_group_id = (fields.get("source_db_security_group_id") or "").strip()
    if not source_db_security_group_id:
        # source_db_hostname was prefilled from the source config by _cdc_infra_prefill.
        source_host = fields.get("source_db_hostname", "")

        def _lookup_source_sg():
            from dsql_migrator.core.rds_metadata import (
                build_rds_client,
                fetch_source_security_group_id,
                parse_rds_region,
            )

            sg_region = parse_rds_region(source_host)
            if sg_region is None:
                return None
            client = build_rds_client(aws_profile, sg_region)
            return fetch_source_security_group_id(client, source_host)

        if source_host:
            try:
                source_db_security_group_id = (
                    await run.io_bound(_lookup_source_sg)
                ) or ""
            except Exception:  # noqa: BLE001 - optional; fall back to open egress
                source_db_security_group_id = ""

    # Plugin bucket + keys are left EMPTY here; the deploy job ensures the managed
    # bucket, uploads the bundled artifacts, and patches these in before create.
    # "Host is the mode": the in-VPC EC2 host sets DSQL_MIGRATOR_CDC_SEED_MODE=external
    # (and DSQL_MIGRATOR_CDC_HOST_SUBNET_CIDR to its own subnet) so the cdc-stack is
    # CREATED as SeedMode=External (no in-VPC seeder Lambda) admitting the host on
    # 9098. Fargate/local leave both unset -> Lambda mode, no ingress (unchanged).
    from dsql_migrator.config import load_config as _load_config

    _cfg = _load_config()
    params = dispatch_cdc_infra_params(
        source_config, sink_config,
        vpc_id=fields["vpc_id"],
        connector_subnet_ids=connector_subnet_ids,
        nat_public_subnet_id=nat_public_subnet_id,
        private_subnet_cidr_a=private_subnet_cidr_a,
        private_subnet_cidr_b=private_subnet_cidr_b,
        private_subnet_az_a=private_subnet_az_a,
        private_subnet_az_b=private_subnet_az_b,
        source_db_security_group_id=source_db_security_group_id,
        plugin_bucket_arn="",
        debezium_plugin_s3_key="",
        dsql_sink_plugin_s3_key="",
        source_db_hostname=fields.get("source_db_hostname", ""),
        source_db_port=int(getattr(
            getattr(session, "source_config", None), "port", 3306
        ) or 3306),
        source_secret_arn=source_secret_arn,
        source_secret_name=source_secret_name,
        dsql_cluster_arn=fields["dsql_cluster_arn"],
        target_endpoint=getattr(target, "cluster_endpoint", "") if target else "",
        target_database=getattr(target, "database", "postgres") if target else "postgres",
        target_username=getattr(target, "username", "admin") if target else "admin",
        stack_name=migration_state.cdc_stack_name,
        topic_prefix=CDC_DEFAULT_TOPIC_PREFIX,
        row_counts_by_table=row_counts_by_table,
        sink_mcu_count=_sink_mcu_count(),
        seed_mode=_cfg.cdc_seed_mode,
        host_subnet_cidr=_admission.cidr,
    )
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=aws_profile,
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name

    def work(handle) -> None:
        run_cdc_infra_deploy(
            handle,
            stack_name=stack_name,
            template_body=template_body,
            params=params,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
            region=region,
            aws_profile=aws_profile,
        )

    _action = "deploy CDC infrastructure"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="infra")
    _log_cdc_event(_action, detail=_detail)
    ui.notify("Infrastructure deploy started (~5 min).", type="positive", position="top")  # type: ignore[attr-defined]
    refresh()

def _start_cdc_stop(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """Submit Stop CDC (blank MskBootstrapServers → delete connectors) as a job."""
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_stop

    _, region = _cdc_target_region(ui, session)
    if region is None:
        return
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=getattr(session, "aws_profile", None),
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name

    def work(handle) -> None:
        run_cdc_stop(
            handle,
            stack_name=stack_name,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
        )
        # The connectors are gone, so there is no live snapshot mode for the next Start to
        # keep (core.cdc.mysql_snapshot_mode); the next probe re-reads the rest.
        _set_mode = getattr(migration_state, "set_cdc_deployed_snapshot_mode", None)
        if callable(_set_mode):
            _set_mode(None)

    # CLOSE the run's data-path record before the connectors go away. Stop is the last
    # moment the applied-ops metrics mean anything (the connectors, and their CloudWatch
    # dimensions, are about to be deleted), and this is the line a cut-over reviewer reads
    # to see what the stream actually moved. The periodic roll-up is throttled, so without
    # this the final interval -- often the one just before cut over -- would be missing.
    _stop_totals = cdc_applied_totals(
        getattr(migration_state, "cdc_applied_ops_by_table", None)
    )
    if _stop_totals != (0, 0, 0):
        _log_cdc_event(
            "CDC applied rows (final)",
            status=ActivityStatus.INFO,
            detail=(
                cdc_applied_rollup_detail(
                    getattr(migration_state, "cdc_applied_ops_by_table", None)
                )
                # Honest about freshness: these come from the last completed poll, not a
                # drain confirmation, so they are a floor at the moment Stop was clicked.
                + " — as of the last status poll before Stop"
            ),
        )

    _action = "stop CDC connectors"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(_action, detail=_detail, work=work)
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="stop")
    # Durable marker → the persistent cross-view "teardown in progress" banner (so
    # navigating away from the CDC step doesn't hide the running stop). Ownership
    # guard: don't clobber a DIFFERENT teardown still running (rare two-tab race).
    if should_replace_teardown_marker(
        job_manager, migration_state.cdc_teardown_job_id, job_id
    ):
        migration_state.set_cdc_teardown(
            job_id,
            kind="stop",
            stack=stack_name,
            ctx={
                "region": region,
                "role_arn": getattr(migration_state, "cdc_deploy_role_arn", None),
                "profile": getattr(session, "aws_profile", None),
                "cleanup_secret": False,
            },
        )
    _log_cdc_event(_action, detail=_detail)
    ui.notify("Stop CDC submitted — removing connectors.", type="positive", position="top")  # type: ignore[attr-defined]
    refresh()

def _start_cdc_delete(
    ui, migration_state, job_manager, refresh, *, session=None
) -> None:
    """Submit Delete CDC infrastructure (full stack delete) as a job."""
    from dsql_migrator.core.cdc_deployer import build_cdc_stack_deployer, run_cdc_delete

    _, region = _cdc_target_region(ui, session)
    if region is None:
        return
    aws_profile = getattr(session, "aws_profile", None)
    deployer = build_cdc_stack_deployer(
        region,
        aws_profile=aws_profile,
        assume_role_arn=getattr(migration_state, "cdc_deploy_role_arn", None),
    )
    migration_state.clear_cdc_deploy_log()
    stack_name = migration_state.cdc_stack_name
    # Only the tool-managed (password-auth) secret should be cleaned up; if the
    # source used Secrets Manager auth, the tool never created one and must NOT
    # delete the customer's secret. delete_source_secret targets only the
    # deterministic mysql-dsql-migrator/cdc/<stack>/source name, but gating here keeps
    # the customer's own SM-auth secret entirely out of scope.
    cleanup_secret = not getattr(session, "source_secret_id", None)

    # What the teardown could not remove from the customer's source. Collected here and
    # read by _logged_cdc_lifecycle so the DURABLE audit line carries it -- the deploy
    # log this also goes to is in-memory and the next lifecycle action wipes it.
    _residue: list[str] = []

    def work(handle) -> None:
        run_cdc_delete(
            handle,
            stack_name=stack_name,
            deployer=deployer,
            on_log=migration_state.append_cdc_deploy_log,
            region=region,
            aws_profile=aws_profile,
            cleanup_source_secret=cleanup_secret,
            # The credentials this process already holds, so dropping the PostgreSQL
            # replication slot does not depend on secretsmanager:GetSecretValue (which the
            # app's identity is not granted for the tool-managed CDC secret).
            source_credentials=_session_source_credentials(session),
            on_residue=_residue.append,
            # Destroying the slot/publication on the customer's source is as auditable
            # as creating them (which Full Load already logs via its own _audit hook);
            # without this, teardown's source writes lived only in the in-memory deploy
            # log. Value-free -- object names and outcomes, no row data.
            on_source_write=lambda message: _log_cdc_event(
                "CDC source replication teardown",
                status=(
                    ActivityStatus.WARNING
                    if message.startswith(("WARNING", "FAILED"))
                    else ActivityStatus.INFO
                ),
                detail=f"source ({stack_name}): {message}",
            ),
        )

    _action = "delete CDC infrastructure"
    _detail = f"stack {stack_name}"
    job_id = job_manager.submit(
        _logged_cdc_lifecycle(
            _action, detail=_detail, work=work, residue=lambda: tuple(_residue)
        )
    )
    migration_state.set_cdc_deploy_job_id(job_id, kind="delete")
    # Clear any latched redeploy answer: this teardown must prompt again when it lands,
    # otherwise a deploy -> delete -> delete sequence silently reuses the earlier "yes"
    # and drops the operator straight back onto the deploy form.
    migration_state.set_cdc_redeploy_confirmed(False)
    # Durable marker → the persistent cross-view "teardown in progress" banner (the
    # delete runs ~15–45 min; the banner keeps it visible on every step, not just
    # the CDC card the user may navigate away from). Ownership guard: don't clobber a
    # DIFFERENT teardown still running (rare two-tab race).
    if should_replace_teardown_marker(
        job_manager, migration_state.cdc_teardown_job_id, job_id
    ):
        migration_state.set_cdc_teardown(
            job_id,
            kind="delete",
            stack=stack_name,
            ctx={
                "region": region,
                "role_arn": getattr(migration_state, "cdc_deploy_role_arn", None),
                "profile": aws_profile,
                "cleanup_secret": cleanup_secret,
            },
        )
    _log_cdc_event(_action, detail=_detail)
    ui.notify("Delete CDC infrastructure submitted.", type="warning", position="top")  # type: ignore[attr-defined]
    refresh()

def _render_cdc_deploy_live(ui, migration_state, job_manager, refresh) -> None:
    """Render the active lifecycle job's stages + event log; poll while running.

    The displayed stage labels + terminal messages adapt to which operation
    (``cdc_action_kind``) is running. When the job finishes it re-probes the
    stack phase and triggers a full ``refresh`` so the card flips to the next
    action (e.g. infra-deploy DONE → Start button appears).
    """
    # The deploy log expansion is rebuilt on every 5s poll -- both by the inner
    # refreshable AND by the OUTER CDC panel poll (``_poll_cdc``), which re-invokes
    # this whole function. A local dict would be recreated on that outer rebuild and
    # snap an opened log shut every few seconds, so anchor the open/closed state on
    # the session-scoped migration state (survives every level of re-render).
    log_state = migration_state.cdc_deploy_log_ui_state

    @ui.refreshable
    def _deploy_live() -> None:  # type: ignore[misc]
        job = _current_job(job_manager, migration_state.cdc_deploy_job_id)
        if job is None:
            return
        kind = getattr(migration_state, "cdc_action_kind", None) or "start"
        # "Refresh now" forces an immediate poll of the same live region (the 5s
        # timer keeps running too); reuses _poll_deploy so a finished job also
        # advances the card, identical to the automatic tick.
        _render_deploy_stages(
            ui, job, kind, on_refresh=_poll_deploy,
            has_seeder_lambda=_cdc_stack_has_seeder_lambda(migration_state),
        )
        _render_deploy_log(ui, migration_state.get_cdc_deploy_log(), log_state)
        if job.status in ("PENDING", "RUNNING"):
            ui.timer(_CDC_POLL_INTERVAL_SECONDS, _poll_deploy, once=True)  # type: ignore[attr-defined]
            return
        done_msg, fail_msg = _CDC_ACTION_TERMINAL.get(
            kind, _CDC_ACTION_TERMINAL["start"]
        )
        done_noun, fail_noun = _CDC_ACTION_NOUN.get(
            kind, _CDC_ACTION_NOUN["start"]
        )
        total = _deploy_total_duration(job)
        if job.status == "DONE":
            render_notice(
                ui,
                tone="success",
                header=f"{done_noun} complete" + (f" — took {total}" if total else ""),
                body=done_msg,
            )
        elif job.status == "FAILED":
            # A job reconciled to FAILED purely because the app restarted mid-run is
            # NOT a real failure: the AWS/CloudFormation work (e.g. a connector
            # still CREATING) usually kept going. Show a calm "re-checking live
            # state" notice (not a red "failed"), and let the connector discovery /
            # stack-phase probe -- which runs on every CDC-step render -- surface the
            # true state (Provisioning / Streaming / Incomplete) instead. The whole
            # card refreshes off that, so this interrupted job-card stops driving the
            # verdict.
            job_error = None
            try:
                job_error = job_manager.get_error(migration_state.cdc_deploy_job_id)
            except JobNotFoundError:
                job_error = None
            if is_interrupted_by_restart(job_error):
                render_notice(
                    ui,
                    tone="info",
                    icon="autorenew",
                    header="Re-checking CDC status after a restart",
                    body=(
                        "The app restarted while this operation was running. The "
                        "work continues on AWS — re-reading the live connector and "
                        "stack state now; the pipeline status below updates "
                        "automatically."
                    ),
                )
            else:
                # ``job_error`` is the REAL reason -- the CloudFormation failure text the
                # deployer already extracted -- and it was read just above only to ask
                # whether a restart interrupted the job, then discarded in favour of the
                # generic per-kind sentence. So an operator whose stack failed for a
                # nameable reason (a quota, a subnet without egress, an IAM gap) was shown
                # boilerplate recommending a second Delete. Lead with the generic sentence
                # (it carries the next action) and append what AWS actually said.
                render_notice(
                    ui,
                    tone="error",
                    header=f"{fail_noun} failed"
                    + (f" (after {total})" if total else ""),
                    body=(
                        f"{fail_msg} Reported cause: {job_error}"
                        if job_error
                        else fail_msg
                    ),
                )
                # A Start the worker's own checks refused: the job error is only a pointer
                # (it is capped), so the refusal's full cause and fix are shown here.
                _refusal = _start_refusal_notice(
                    migration_state, migration_state.cdc_deploy_job_id
                )
                if _refusal is not None:
                    _tone, _icon, _header, _body = _refusal
                    render_notice(ui, tone=_tone, icon=_icon, header=_header, body=_body)

    def _poll_deploy() -> None:
        job = _current_job(job_manager, migration_state.cdc_deploy_job_id)
        # When the operation finishes, re-probe the stack so the lifecycle card advances
        # to the next action; otherwise just refresh the live region.
        #
        # ``job is None`` (how _current_job reports a lost job record) must take the
        # refresh() branch too: _deploy_live early-returns on a missing job, so refreshing
        # only the live region emptied the card and left it blank forever. A full refresh
        # re-probes the stack, so a lost job resolves from live AWS state instead.
        if job is None or job.status not in ("PENDING", "RUNNING"):
            refresh()
        else:
            _deploy_live.refresh()

    _deploy_live()

def _render_deploy_stages(
    ui, job, kind: str = "start", on_refresh=None, *, has_seeder_lambda=None
) -> None:
    """Render each stage (job chunk) of the running operation as an icon row.

    Each not-yet-finished stage shows a rough ETA hint (``~3 min``) so the user
    knows what to expect; the in-progress stage also shows live elapsed time. A
    total estimate for the whole operation is shown under the title.

    While the operation runs, the title row also shows an "Auto-refreshing"
    caption and (when ``on_refresh`` is given) a manual Refresh button on the
    right: connector creation takes 10-20 min, so without a visible live signal a
    user can mistake the long-running stage for a frozen UI. The button lets them
    force an immediate poll; the caption reassures them it updates on its own.
    """
    labels = _CDC_STAGE_LABELS.get(kind, _CDC_STAGE_LABELS["start"])
    # Delete shows no per-stage ETA: its dominant stage (stack_delete) waits on
    # unpredictable ENI reclamation, so a "~5 min" hint on it is misleading. The
    # upper-bound line under the title carries the expectation instead. An empty map
    # makes every _format_eta_hint(...) below return "" for delete.
    etas = {} if kind == "delete" else _CDC_STAGE_ETA_SECONDS.get(kind, {})
    now = datetime.now(timezone.utc)
    running = job.status in ("PENDING", "RUNNING")
    with ui.row().classes("items-center gap-2 no-wrap w-full"):  # type: ignore[attr-defined]
        ui.label(_CDC_ACTION_TITLE.get(kind, "Progress")).classes(  # type: ignore[attr-defined]
            "text-sm font-semibold"
        )
        # Whole-operation hint. Delete is special: its wall-clock is dominated by
        # AWS reclaiming the in-VPC seeder Lambda's ENIs before the MSK cluster can go,
        # which is unpredictable and has been measured at ~20+ min against the old
        # ~5 min estimate. A precise-looking "est. ~5 min remaining" that overshoots by
        # 4x reads as a stuck UI, so for a delete show an honest UPPER BOUND ("up to
        # ~20 min") instead of a countdown. Other operations keep the summed ETA.
        if running:
            if kind == "delete":
                ui.label(  # type: ignore[attr-defined]
                    "can take "
                    + cdc_teardown_estimate(has_seeder_lambda=has_seeder_lambda)
                ).classes("text-xs text-gray-400")
            else:
                remaining_total = sum(
                    etas.get(c.chunk_id, 0) for c in job.chunks if c.status != "DONE"
                )
                total_hint = _format_eta_hint(remaining_total)
                if total_hint:
                    ui.label(f"est. {total_hint} remaining").classes(  # type: ignore[attr-defined]
                        "text-xs text-gray-400"
                    )
        if running and on_refresh is not None:
            # Manual refresh button only (the header's spinning icon + deploying
            # badge already signals the operation is live; a redundant spinner +
            # "Auto-refreshing…" text was just visual clutter).
            ui.space()  # type: ignore[attr-defined]
            ui.button(on_click=on_refresh).props(  # type: ignore[attr-defined]
                "flat dense round size=sm icon=refresh"
            ).tooltip("Refresh now")
    for chunk in job.chunks:
        # When the job itself has ended (FAILED/DONE), any stage still marked
        # IN_PROGRESS was interrupted — show it as FAILED so the spinner stops
        # and the user sees a definitive state, not a stale hourglass.
        effective_status = chunk.status
        if not running and chunk.status == "IN_PROGRESS":
            effective_status = "FAILED"
        icon, color = _CDC_DEPLOY_STAGE_STYLE.get(effective_status, _CDC_DEPLOY_STAGE_STYLE["PENDING"])
        label = labels.get(chunk.chunk_id, chunk.chunk_id)
        in_progress = effective_status == "IN_PROGRESS"
        # Emphasize the running stage: a live animated hourglass spinner (instead of
        # the static icon) plus a bold, pulsing primary label, so the eye is drawn
        # to exactly which step is happening now.
        with ui.row().classes("items-center gap-2 no-wrap"):  # type: ignore[attr-defined]
            if in_progress:
                ui.spinner("hourglass", size="sm", color="primary")  # type: ignore[attr-defined]
            else:
                ui.icon(icon, color=color).classes("text-base")  # type: ignore[attr-defined]
            label_classes = (
                "text-sm font-semibold text-primary animate-pulse"
                if in_progress
                else "text-xs text-gray-700"
            )
            ui.label(label).classes(label_classes)  # type: ignore[attr-defined]
            # The running stage shows live elapsed; pending stages show their ETA.
            if in_progress and chunk.started_at is not None:
                elapsed = (now - chunk.started_at).total_seconds()
                eta = etas.get(chunk.chunk_id, 0)
                suffix = f" / {_format_eta_hint(eta)}" if _format_eta_hint(eta) else ""
                ui.label(  # type: ignore[attr-defined]
                    f"{format_duration(max(0.0, elapsed))} elapsed{suffix}"
                ).classes("text-xs text-primary font-medium")
            elif chunk.status == "PENDING":
                hint = _format_eta_hint(etas.get(chunk.chunk_id, 0))
                if hint:
                    ui.label(hint).classes("text-xs text-gray-400")  # type: ignore[attr-defined]

def _render_deploy_log(ui, log_lines, log_state=None) -> None:
    """Render the timestamped deploy log lines, newest last (logging style).

    ``log_state`` is an optional ``{"open": bool}`` dict owned by the caller (it
    must outlive this render). The expansion opens to that remembered state and
    writes its toggles back, so the live poll's 5s rebuild does not collapse a log
    the user opened.
    """
    if not log_lines:
        return
    if log_state is None:
        log_state = {"open": False}

    def _remember(e) -> None:
        log_state["open"] = bool(e.value)

    with ui.expansion(  # type: ignore[attr-defined]
        f"Deploy log ({len(log_lines)} lines)",
        icon="terminal",
        value=log_state["open"],
        on_value_change=_remember,
    ).classes(f"w-full {EXPANSION_PANEL_CLASSES}"):
        # ASCII-only separator ("-"), and sanitize each message, so the monospace
        # ui.code font never renders a missing-glyph box (tofu) for punctuation
        # like the em-dash / ellipsis some deploy messages contained.
        # Timestamps are UTC (the deploy driver stamps datetime.now(timezone.utc)).
        # Show the zone explicitly so a line reads unambiguously and matches the
        # downloaded activity log / CloudWatch / CloudFormation events (all UTC).
        text = "\n".join(
            f"{ts.strftime('%H:%M:%S')} UTC - {_ascii_log(msg)}" for ts, msg in log_lines
        )
        ui.code(text).classes("w-full text-xs")  # type: ignore[attr-defined]



# The post-start CDC monitoring / DLQ / status render panels were extracted to
# _cdc_monitoring.py for maintainability (after the _cdc_state predicate split, so the
# panels import those predicates from _cdc_state, not back from here -- no cycle).
# Re-exported so _cdc_ui.<name>, _render_cdc_step's calls, and every consumer/test import
# resolve unchanged.
from dsql_migrator.ui.data_migration._cdc_monitoring import (  # noqa: E402,F401
    _DLQ_LEVEL_TONE,
    _DLQ_RECORD_LIST_LIMIT,
    _DLQ_RECORD_PAGE_SIZE,
    _DRIFT_LABELS,
    _dlq_panel_tone,
    _open_add_column_dialog,
    _render_cdc_dlq_breakdown,
    _render_cdc_dlq_panel,
    _render_cdc_dlq_records,
    _render_cdc_error_download,
    _render_cdc_handling_panel,
    _render_cdc_live_monitoring,
    _render_cdc_pipeline_health,
    _render_cdc_schema_drift_banner,
    _render_change_flow_status,
    _render_full_load_quarantine_pointer,
    _render_migration_table_status,
    exclusion_lock_state,
    change_and_reload_block_reason,
    fk_apply_running,
    table_set_editable_after_load,
    table_set_wait_reason,
    TABLE_SET_WAIT_LOAD,
    TABLE_SET_WAIT_FK,
    TABLE_SET_WAIT_RESTORE,
    TABLE_SET_WAIT_INFRA,
    TABLE_SET_WAIT_CDC_CHECK,
    TABLE_SET_WAIT_CDC_UNREAD,
    table_set_failed_create_wait,
    table_set_delete_wait,
    TableSetAddLock,
    table_set_add_lock,
    cdc_uncovered_selection,
    fl_cdc_pending_additions,
    cdc_addition_block_reason,
    LOCK_TIP_RULE,
    LOCK_TIP_SPLIT,
    TREE_HINT_FL_CDC,
    TREE_HINT_SPLIT,
    CDC_ONLY_NARROWING_SENTENCE,
    R4_FL_CDC_NARROWING,
    R1_FL_CDC_SENTENCE,
    CONTINUE_HINT_FL_CDC,
    capture_error_with_remedy,
    cdc_stack_being_deleted,
    cdc_delete_eta,
    _render_cdc_oversized_recovery_without_monitoring,
)
